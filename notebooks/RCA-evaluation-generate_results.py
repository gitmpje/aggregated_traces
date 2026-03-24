# %%
import json

from boto3 import Session
from pathlib import Path

from utils import generate_scenario, parse_scenario_string

n_runs = 10
basepath = Path("output/simulation")

session = Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN,
    region_name="eu-west-1",
)
lambda_client = session.client("lambda")


with open("backward/scenarios.txt") as f:
    scenarios = f.read().split("\n")

# %% ### Invoke Lambda function

# Define the quality threshold
quality_threshold = 0.9

for scenario_name in scenarios:
    scenario_dict = parse_scenario_string(scenario_name)
    merge_after_steps = scenario_dict["merge_after_steps"]
    split_after_steps = scenario_dict["split_after_steps"]
    number_of_devices = scenario_dict["number_of_devices"]
    n_resources_factor = scenario_dict["n_resources_factor"]

    split_configs = [
        {"after_step": step, "number_of_split_lots": 2} for step in split_after_steps
    ]

    event = {
        "quality_threshold": 0.9,
        "scenario_name": scenario_name,
        "scenario": generate_scenario(
            output_file=scenario_name + ".json",
            n_lots=32,
            n_devices=number_of_devices,
            steps_resources={
                "WT": (n_resources_factor * 2, 1),
                "DB": (n_resources_factor * 2, 2),
                "WB": (n_resources_factor * 4, 20),
                "Marking": (n_resources_factor * 4, 1),
                "FT": (n_resources_factor * 2, 1),
            },
            root_cause_resource="WB1",
            # required_material: Dict[str, str] = None,
            merge_after_steps=list(merge_after_steps),
            # n_lots_merge: int = 2,
            split_configs=split_configs,
        ),
    }

    count = 0
    for i in range(n_runs):
        event["run_number"] = i
        lambda_response = lambda_client.invoke(
            FunctionName="research-trace-backward-experiments",
            InvocationType="Event",  # RequestResponse
            # LogType="Tail",
            Payload=json.dumps(event).encode(),
        )

        if lambda_response["ResponseMetadata"]["HTTPStatusCode"] == 202:
            count += 1

    print(f"{scenario_name}: {count} invocations")
