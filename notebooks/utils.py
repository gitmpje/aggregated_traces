import json

from collections import defaultdict
from pandas import notna
from typing import Dict, List, Tuple

VARIABLES_MAPPING = {
    "count_merge": "$|S_{merge}|$",
    "count_split": "$|S_{split}|$",
    "split_before_merge": "$f_{split-merge}$",
    "number_of_devices_25-75": "$D_{25-75}$",
    "number_of_devices_10-90": "$D_{10-90}$",
    "number_of_devices_25-125": "$D_{25-125}$",
    "number_of_devices_10-170": "$D_{10-170}$",
    "n_resources_factor_2": "$r_f=2$",
    "n_resources_factor_4": "$r_f=4$",
    "n_resources_factor_8": "$r_f=8$",
    "n_resources_factor_16": "$r_f=16$",
    "n_resources_factor": r"$r_f$",
    "number_of_devices": r"$D$",
    "density": r"$\rho(G_{trace})$",
    "avg_out_degree": r"$\frac{\sum_{n \in N_{trace}} deg^+(n)}{|N_{trace}|}$",
    "max_out_degree": r"$\max(deg^+(n) | n \in N_{trace})$",
    "avg_path_length": r"$\frac{\sum_{p \in Paths} |p|}{|Paths|}$",
    "max_path_length": r"$\max(\{|p| : p \in Paths\})$",
    "n_simple_paths": r"$|Paths|$",
    "n_steps_diff": r"$\delta_{bw}$",
    "n_recall_diff": r"$\delta_{fw}$",
    "top_1_hit_random": r"Top-1 hit rate random",
    "top_1_hit_sorted": r"Top-1 hit rate sorted",
}


def generate_scenario(
    output_file: str,
    n_lots: int,
    n_devices: List[int],
    steps_resources: Dict[str, Tuple[int]],
    root_cause_resource: str,
    required_material: Dict[str, str] = None,
    merge_after_steps: List[str] = [],
    # n_lots_merge: int = 2,
    split_configs: List[dict] = [],
) -> str:
    # Select lots for merge after a certain step
    merge_configs = [{"after_step": step} for step in merge_after_steps]
    merge_configs_lots = defaultdict(list)
    for config in merge_configs:
        # for i in [i for i in range(0, n_lots, n_lots_merge)]:
        for i in range(n_lots):
            merge_configs_lots[f"Lot{i}"].append(config)

    production_lots = []
    for i in range(n_lots):
        lot_id = f"Lot{i}"
        production_lot = {
            "id": lot_id,
            "steps": list(steps_resources.keys()),
            "merge": merge_configs_lots.get(lot_id, []),
            "split": split_configs,
            "n_devices": n_devices[i],
        }

        if required_material:
            production_lot["required_material"] = required_material

        production_lots.append(production_lot)

    production_resources = []
    for step, (n, mean_duration) in steps_resources.items():
        production_resources.extend(
            [
                {
                    "id": f"{step}{i}",
                    "step": step,
                    "mean_move": 0.5,
                    "mean_duration": mean_duration,
                    "mean_breakdown": 5,
                    "mean_repair": 10,
                    "process_yield": 0.5 if root_cause_resource == f"{step}{i}" else 1,
                }
                for i in range(n)
            ]
        )

    simulation_config = {
        "production_lots": production_lots,
        "production_resources": production_resources,
        "material_lot_size": 100,
        "packing_unit_size": 50,
    }

    # with lock:
    with open(output_file, "w") as f:
        json.dump(simulation_config, f, indent=2)

    return json.dumps(simulation_config)


def parse_scenario_string(s: str):
    """
    Parse key=value pairs from a scenario string.
    Special rules:
      - merge_after_steps and split_after_steps become lists split on '-'
      - all other values stay as strings
    """
    result = {}

    number_of_devices_values = {
        "50": [50] * 32,
        "25-75": [25, 75] * 16,
        "10-90": [10, 90] * 16,
        "125-25": [25, 25, 25, 125] * 8,
        "25-125": [25, 25, 25, 125] * 8,
        "10-170": [10, 10, 10, 170] * 8,
    }

    # The part after the last directory slash contains the parameters
    params = s.split("/")[-1]

    # Split on '+' because parameters are joined with '+'
    for part in params.split("+"):
        if "=" not in part:
            continue  # safety

        key, value = part.split("=", 1)

        # Special handling for list-based keys
        if key in ("merge_after_steps", "split_after_steps"):
            result[key] = value.split("-")
        elif key == "number_of_devices":
            result[key] = number_of_devices_values[value]
        else:
            result[key] = int(value)

    return result


def parse_scenario_string_categories(s: str):
    """
    Parse key=value pairs from a scenario string.
    Special rules:
      - merge_after_steps and split_after_steps become lists split on '-'
      - all other values stay as strings
    """
    result = {}

    # The part after the last directory slash contains the parameters
    params = s.split("/")[-1]

    # Split on '+' because parameters are joined with '+'
    for part in params.split("+"):
        if "=" not in part:
            continue  # safety

        key, value = part.split("=", 1)

        result[key] = value

    return result


def combine_steps(row):
    steps_order = ["WT", "DB", "WB", "Marking", "FT"]
    merge_steps = (
        row["merge_after_steps"].split("-") if notna(row["merge_after_steps"]) else []
    )
    split_steps = (
        row["split_after_steps"].split("-") if notna(row["split_after_steps"]) else []
    )

    combined = []
    for step in steps_order:
        if step in merge_steps:
            combined.append(f"{step}-merge")
        elif step in split_steps:
            combined.append(f"{step}-split")
        else:
            combined.append(f"{step}")

    return "-".join(combined)


def merge_split_order(row, first):
    after = "split" if first == "merge" else "merge"
    row = row.split("-")

    # Collect all indices where first/after appear
    idx_first = [i for i, v in enumerate(row) if v == first]
    idx_after = [i for i, v in enumerate(row) if v == after]

    # If either is missing, order condition can't be met
    if not idx_first or not idx_after:
        return False

    # Return True if ANY first occurs before ANY after
    return any(i < j for i in idx_first for j in idx_after)
