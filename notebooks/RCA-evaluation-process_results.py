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
combined_report_file = "output/backward/combined_report_DoE_revision.csv"

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


with open("backward/scenarios.txt") as f:
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
            "research", "trace-backward-experiments", scenario_name, f"run_{i}.json.gz"
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


# %% Compute steps to find root cause (entity)
def compute_n_steps(probability_dicts, root_cause_entity) -> DataFrame:
    group_key = "product_model"

    results = []
    for event_data_file, probability_dict in probability_dicts.items():
        df_trace = DataFrame(probability_dict)

        for group_value in df_trace[group_key].unique():
            # Aggregate to get probabilities for target entities
            df_trace_grouped = (
                df_trace[df_trace[group_key] == group_value]
                .groupby(["entity_source", "entity_target"])
                .agg(
                    {
                        "probability": "sum",
                        "n_merges": "sum",
                        "split_merge_kl": "mean",
                        "n_production_steps_aggregations": list,
                    }
                )
            )
            df_trace_grouped.reset_index(inplace=True)

            # Remove lists with NaN value
            df_trace_grouped["n_production_steps_aggregations"] = df_trace_grouped[
                "n_production_steps_aggregations"
            ].apply(lambda x: [] if any(not isinstance(i, list) for i in x) else x)

            entities_source = df_trace_grouped["entity_source"].unique()
            for entity_source in entities_source:
                df_selected = df_trace_grouped[
                    df_trace_grouped["entity_source"] == entity_source
                ]

                # Shuffle/sample DataFrame for random order of inspection
                n_steps_random = get_entity_record_number(
                    df_selected.sample(frac=1), root_cause_entity
                )

                # Sort values for informed order of inspection
                n_steps_sorted = get_entity_record_number(
                    df_selected.sort_values("probability", ascending=False),
                    root_cause_entity,
                )

                results.append(
                    {
                        "file": event_data_file,
                        group_key: group_value,
                        "probabilities": df_selected.to_dict(orient="records"),
                        "n_steps_random": n_steps_random,
                        "n_steps_sorted": n_steps_sorted,
                        "n_simple_paths": df_trace[df_trace[group_key] == group_value][
                            "n_simple_paths"
                        ].iloc[0],
                        "trace_graph_selected-n_nodes": df_trace[
                            df_trace[group_key] == group_value
                        ]["trace_graph_selected-n_nodes"].iloc[0],
                        "trace_graph_selected-n_edges": df_trace[
                            df_trace[group_key] == group_value
                        ]["trace_graph_selected-n_edges"].iloc[0],
                        "out_degrees": df_trace[df_trace[group_key] == group_value][
                            "out_degrees"
                        ].iloc[0],
                        "path_lengths": df_trace[df_trace[group_key] == group_value][
                            "path_lengths"
                        ].iloc[0],
                        "density": df_trace[df_trace[group_key] == group_value][
                            "density"
                        ].iloc[0],
                    }
                )

    return DataFrame(results)


processed_scenarios = read_csv(combined_report_file, usecols=["scenario_name"])[
    "scenario_name"
].unique()
# processed_scenarios = []

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

    lower_yield_resources = [
        resource["id"]
        for resource in config["production_resources"]
        if resource["process_yield"] < 1
    ]
    if len(lower_yield_resources) > 1:
        print("Multiple resources with yield < 1")

    root_cause_entity = (
        f"http://example.org/id/ekg/aggregated_traces/{lower_yield_resources[0]}"
    )

    probability_dicts = {}
    for result_file in Path(f"output/{scenario_name}/").glob("*.json.gz"):
        with gzip.open(result_file) as f:
            probability_dicts[result_file] = json.load(f)

    scenario_dict[scenario_name] = compute_n_steps(probability_dicts, root_cause_entity)

print(len(scenario_dict))

# %%

df_combined = DataFrame()
# df_combined = read_csv(combined_report_file)
# df_combined["probabilities"] = df_combined["probabilities"].apply(json.loads)

for k, v in scenario_dict.items():
    v["scenario_name"] = k
    df_combined = concat([df_combined, v])

df_combined["n_steps_diff"] = (
    df_combined["n_steps_random"] - df_combined["n_steps_sorted"]
)

df_combined["probabilities"] = df_combined["probabilities"].apply(json.dumps)
df_combined.to_csv(combined_report_file.replace(".csv", "-2.csv"), index=False)

# %%
with open(combined_report_file, "a") as fout:
    for file in [
        combined_report_file.replace(".csv", "-1.csv"),
        combined_report_file.replace(".csv", "-2.csv"),
        combined_report_file.replace(".csv", "-3.csv"),
    ]:
        with open(file) as f:
            fout.write(f.read())
