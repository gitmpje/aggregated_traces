# %%
import gzip
import json
import os

from boto3 import Session
from botocore.exceptions import ClientError
from datetime import datetime, UTC
from pandas import DataFrame, concat, Index, read_csv
from pathlib import Path

from utils import parse_scenario_string

n_runs = 10
combined_report_file = "output/forward/combined_report_DoE_revision.csv"

session = Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN,
    region_name="eu-west-1",
)


def get_entity_record_number(df: DataFrame, entity: str) -> int:
    try:
        return Index(df["entity_target"]).get_loc(entity) + 1
    except KeyError:
        return None


with open("forward/scenarios.txt") as f:
    scenarios = f.read().split("\n")

# %% Retrieve results from S3
s3_client = session.client("s3")

for scenario_name in scenarios:
    scenario_dict = parse_scenario_string(scenario_name)
    merge_after_steps = scenario_dict["merge_after_steps"]
    split_after_steps = scenario_dict["split_after_steps"]
    number_of_devices = scenario_dict["number_of_devices"]
    n_resources_factor = scenario_dict["n_resources_factor"]

    result_files = Path(f"output/{scenario_name}/")
    if not result_files.exists():
        os.mkdir(result_files)

    for i in range(n_runs):
        object_key = os.path.join(
            "research", "trace-forward-experiments", scenario_name, f"run_{i}.json.gz"
        ).replace("\\", "/")
        file_name = os.path.join("output", scenario_name, f"run_{i}.json.gz")

        # if Path(file_name).exists():
        #     print(f"{file_name} already exists")
        #     continue

        try:
            last_modified = s3_client.head_object(
                Bucket="574885398813-spill-data", Key=object_key
            )["LastModified"]
            if last_modified < datetime(2026, 1, 1, tzinfo=UTC):
                print(f"{object_key} is too old, skipping")
                continue
            s3_client.download_file(
                Bucket="574885398813-spill-data", Key=object_key, Filename=file_name
            )
        except ClientError as exception:
            print(f"Failed retrieving: {object_key}: {exception}")


# %% Compute number of products to recall
def compute_n_recall(probability_dicts, packing_unit_size, threshold) -> DataFrame:
    results = []
    for event_data_file, probability_dict in probability_dicts.items():
        df_trace = DataFrame(probability_dict)

        # Aggregate to get probabilities for target entities
        df_trace_grouped = df_trace.groupby(["entity_source", "entity_target"]).agg(
            {
                "probability": "sum",
                "n_merges": "sum",
                "split_merge_kl": "mean",
                "n_production_steps_aggregations": list,
                "devices_quality": list,
            }
        )
        df_trace_grouped.reset_index(inplace=True)

        # Remove lists with NaN value
        df_trace_grouped["n_production_steps_aggregations"] = df_trace_grouped[
            "n_production_steps_aggregations"
        ].apply(lambda x: [] if any(not isinstance(i, list) for i in x) else x)

        entities_target = df_trace_grouped["entity_target"].unique()
        n_recall_all = len(entities_target) * packing_unit_size

        # Compute number of devices above threshold
        entities_target_threshold = df_trace_grouped[
            df_trace_grouped["probability"] > threshold
        ]["entity_target"].unique()
        n_recall_threshold = len(entities_target_threshold) * packing_unit_size

        if n_recall_threshold:
            devices_quality_selected = [
                q
                for r in df_trace_grouped[
                    df_trace_grouped["entity_target"].isin(entities_target_threshold)
                ]["devices_quality"]
                .apply(lambda x: [i for l in x for i in l])
                .values
                for q in r
            ]
            n_true_positive = sum([q < 1 for q in devices_quality_selected])
            n_false_positive = len(devices_quality_selected) - n_true_positive
            devices_quality_other = [
                q
                for r in df_trace_grouped[
                    ~df_trace_grouped["entity_target"].isin(entities_target_threshold)
                ]["devices_quality"]
                .apply(lambda x: [i for l in x for i in l])
                .values
                for q in r
            ]
            n_false_negative = sum([q < 1 for q in devices_quality_other])
            n_true_negative = len(devices_quality_other) - n_false_negative
        else:
            n_false_positive = float("nan")
            n_false_negative = float("nan")
            n_true_positive = float("nan")
            n_true_negative = float("nan")

        results.append(
            {
                "file": event_data_file,
                "probabilities": df_trace_grouped.to_dict(orient="records"),
                "n_recall_all": n_recall_all,
                "n_recall_threshold": n_recall_threshold,
                "n_true_positive": n_true_positive,
                "n_true_negative": n_true_negative,
                "n_false_positive": n_false_positive,
                "n_false_negative": n_false_negative,
                "n_simple_paths": df_trace["n_simple_paths"].iloc[0],
                "trace_graph_selected-n_nodes": df_trace[
                    "trace_graph_selected-n_nodes"
                ].iloc[0],
                "trace_graph_selected-n_edges": df_trace[
                    "trace_graph_selected-n_edges"
                ].iloc[0],
                "out_degrees": df_trace["out_degrees"].iloc[0],
                "path_lengths": df_trace["path_lengths"].iloc[0],
                "density": df_trace["density"].iloc[0],
            }
        )

    return DataFrame(results)


# processed_scenarios = read_csv(combined_report_file)["scenario_name"].unique()
processed_scenarios = []

threshold = 0.9

scenario_dict = {}
for scenario_name in scenarios:
    # Skip scenarios that are already processed
    if scenario_name in processed_scenarios:
        continue

    file_path = scenario_name + ".json"
    modification_time = os.path.getmtime(file_path)
    if modification_time < datetime(2026, 1, 1, tzinfo=UTC).timestamp():
        print(f"{file_path} is too old, skipping")
        continue

    try:
        with open(file_path) as f:
            config = json.load(f)
    except FileNotFoundError:
        print("Skip: ", scenario_name)
        continue

    print(scenario_name)

    packing_unit_size = config["packing_unit_size"]

    probability_dicts = {}
    for result_file in Path(f"output/{scenario_name}/").glob("*.json.gz"):
        with gzip.open(result_file) as f:
            probability_dicts[result_file] = json.load(f)

    scenario_dict[scenario_name] = compute_n_recall(
        probability_dicts, packing_unit_size, threshold
    )

print(len(scenario_dict))

df_combined = DataFrame()
# df_combined = read_csv(combined_report_file.replace(".csv", f"-threshold={threshold}.csv"))
# df_combined["probabilities"] = df_combined["probabilities"].apply(json.loads)

for k, v in scenario_dict.items():
    v["scenario_name"] = k
    df_combined = concat([df_combined, v])

df_combined["n_recall_diff"] = (
    df_combined["n_recall_all"] - df_combined["n_recall_threshold"]
)

df_combined["probabilities"] = df_combined["probabilities"].apply(json.dumps)
df_combined.to_csv(combined_report_file, index=False)
df_combined.to_csv(
    combined_report_file.replace(".csv", f"-threshold={threshold}.csv"), index=False
)

print(df_combined[["n_recall_diff", "n_false_positive", "n_false_negative"]].mean())
print((df_combined["n_false_positive"]/(df_combined["n_false_positive"]+df_combined["n_true_negative"])).mean())
print((df_combined["n_false_negative"]/(df_combined["n_true_positive"]+df_combined["n_false_negative"])).mean())
