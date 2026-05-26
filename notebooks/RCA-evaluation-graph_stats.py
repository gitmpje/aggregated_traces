import json

from pandas import Categorical, read_csv

from utils import parse_scenario_string_categories

FACTORS = [
    "merge_after_steps",
    "split_after_steps",
    "number_of_devices",
    "n_resources_factor",
]
CUSTOM_FACTOR_ORDER = {
    "number_of_devices": ["50", "25-75", "10-90", "25-125", "10-170"],
    "n_resources_factor": ["1", "2", "4", "8", "16"],
}

SELECTED_STATS = ["count", "mean", "std", "min", "max"]

METRICS = [
    ("trace_graph_selected-n_nodes", int),
    ("trace_graph_selected-n_edges", int),
    ("density", float),
    ("avg_out_degree", float),
    ("max_out_degree", int),
    ("n_simple_paths", int),
    ("avg_path_length", float),
    ("max_path_length", int),
    ("unique_entity_targets", int),
]

LATEX_MAP = {
    "trace_graph_selected-n_nodes": r"$|N_{trace}|$",
    "trace_graph_selected-n_edges": r"$|R_{trace}|$",
    "merge_after_steps": r"$S_{merge}$",
    "split_after_steps": r"$S_{split}$",
    "n_resources_factor": r"$r_f$",
    "number_of_devices": r"$D$",
    "density": r"$\rho(G_{trace})$",
    "avg_out_degree": r"$\frac{\sum_{n \in N_{trace}} deg^+(n)}{|N_{trace}|}$",
    "max_out_degree": r"$\max(deg^+(n) | n \in N_{trace})$",
    "avg_path_length": r"$\frac{\sum_{p \in Paths} |p|}{|Paths|}$",
    "max_path_length": r"$\max(\{|p| : p \in Paths\})$",
    "n_simple_paths": r"$|Paths|$",
    "unique_entity_targets": r"$|N_{target}|$",
}

report_name = "combined_report_DoE_revision"
df_combined = read_csv(
    f"output/backward/{report_name}.csv",
    usecols=[
        "scenario_name",
        "trace_graph_selected-n_nodes",
        "trace_graph_selected-n_edges",
        "density",
        "n_simple_paths",
        "out_degrees",
        "path_lengths",
        "probabilities",
    ],
)

output_path = "output/backward/suppl-DoE_backward-graph_statistics.tex"

print("Loaded DataFrame")

# Graph metrics
df_combined["out_degrees"] = df_combined["out_degrees"].apply(lambda x: json.loads(x))
df_combined["avg_out_degree"] = df_combined["out_degrees"].apply(
    lambda x: sum(x) / len(x)
)
df_combined["max_out_degree"] = df_combined["out_degrees"].apply(lambda x: max(x))

df_combined["path_lengths"] = df_combined["path_lengths"].apply(lambda x: json.loads(x))
df_combined["avg_path_length"] = df_combined["path_lengths"].apply(
    lambda x: sum(x) / len(x)
)
df_combined["max_path_length"] = df_combined["path_lengths"].apply(lambda x: max(x))


def _count_unique_entity_targets(probabilities_json):
    parsed = json.loads(probabilities_json)
    if isinstance(parsed, dict):
        parsed = [parsed]
    if not isinstance(parsed, list):
        return 0

    unique_targets = {
        entry.get("entity_target")
        for entry in parsed
        if isinstance(entry, dict) and "entity_target" in entry
    }
    unique_targets.discard(None)
    return len(unique_targets)


df_combined["unique_entity_targets"] = df_combined["probabilities"].apply(
    _count_unique_entity_targets
)

print("Computed graph metrics")

for factor in FACTORS:
    df_combined[factor] = df_combined["scenario_name"].apply(
        lambda x: parse_scenario_string_categories(x)[factor]
    )
df_combined["n_resources_factor"] = (
    df_combined["n_resources_factor"].astype(int).astype(str)
)
df_combined["number_of_devices"] = df_combined["number_of_devices"].replace(
    "125-25", "25-125"
)


def make_table(factor, metric, datatype):

    df = df_combined.groupby(factor)[metric].describe().reset_index()

    # -- precision control: density gets 5 decimals --
    if metric == "density":
        df = df.round(5)
    else:
        df = df.round(2)

    # -- enforce typing: count must be integer --
    df["count"] = df["count"].astype(int)

    # -- enforce custom factor order if provided --
    if factor in CUSTOM_FACTOR_ORDER:
        ordered = CUSTOM_FACTOR_ORDER[factor]
        df[factor] = Categorical(df[factor], categories=ordered, ordered=True)
        df = df.sort_values(factor)
    else:
        df = df.sort_values(factor)

    # Keep only the desired columns
    df = df[[factor] + SELECTED_STATS]

    # Replace with LaTeX names
    df.columns = [LATEX_MAP.get(c, c) for c in df.columns]

    # -----------------------------------------
    # Construct LaTeX table
    # -----------------------------------------
    latex = ""
    latex += "\\resizebox{\\linewidth}{!}{%\n"
    latex += "\\begin{tabular}{%s}\n" % ("c" * len(df.columns))
    latex += "\\toprule\n"
    latex += " & ".join(df.columns) + " \\\\\n"
    latex += "\\midrule\n"

    for _, row in df.iterrows():
        latex += " & ".join(str(v) for v in row.values) + " \\\\\n"

    latex += "\\bottomrule\n"
    latex += "\\end{tabular}%\n"
    latex += "}\n"

    return latex


with open(output_path, "w") as f:
    f.write(r"""\documentclass[a4paper,11pt]{article}
\usepackage{amsmath, amssymb, booktabs, geometry}
\usepackage{siunitx}
\usepackage{graphicx}
\usepackage{float}

\geometry{margin=1in}

\sisetup{
    table-number-alignment = center,
    round-mode=places,
    round-precision=2
}

\renewcommand{\thetable}{S\arabic{table}}

\begin{document}

\title{Supplementary Material: Graph Characteristics}
\author{}
\date{}
\maketitle

This supplementary document contains tables of descriptive statistics for graph characteristics across experimental factors.

Notation:
\begin{itemize}
    \item $deg^+(n)$: out-degree of node $n$
    \item $\rho(G)$: density of graph $G$
    \item $N_{trace}$: set of nodes (events) selected for tracing
    \item $R_{trace}$: set of edges for given type of trace
    \item $G_{trace} = (N_{trace}, R_{trace})$: graph with the nodes and edges selected for tracing
    \item $Paths$: set of simple paths extracted by Algorithm 1.
    \item $N_{target} = \{n | n \in Entity \wedge type_n=type_{target}\}$: set of target entities
\end{itemize}
""")

    # ------------------------------------------------------
    # MAIN CONTENT
    # ------------------------------------------------------
    for metric, datatype in METRICS:
        metric_label = LATEX_MAP.get(metric, metric)
        f.write("\n\\section*{" + metric_label + "}\n\n")

        # pair tables 2-by-2
        for i in range(0, len(FACTORS), 2):
            left_factor = FACTORS[i]
            right_factor = FACTORS[i + 1] if i + 1 < len(FACTORS) else None

            # ---------------------------
            # LEFT TABLE
            # ---------------------------

            left_table = make_table(left_factor, metric, datatype)

            # ---------------------------
            # BEGIN ROW: two minipages
            # ---------------------------
            f.write("\\begin{table}[H]\n\\centering\n")

            # Left table
            f.write("\\begin{minipage}[t]{0.48\\textwidth}\n")
            f.write(
                f"\\caption{{Descriptive statistics of {metric_label} for {LATEX_MAP.get(left_factor, factor)}}}\n"
            )
            f.write("\\centering\n")
            # f.write("\\textbf{Grouped by " + LATEX_MAP[left_factor] + "}\\\\[4pt]\n")
            f.write(left_table)
            f.write("\n\\end{minipage}\n")

            # Right table
            if right_factor is not None:
                f.write("\\hfill\n")
                f.write("\\begin{minipage}[t]{0.48\\textwidth}\n")
                f.write(
                    f"\\caption{{Descriptive statistics of {metric_label} for {LATEX_MAP.get(right_factor, factor)}}}\n"
                )
                f.write("\\centering\n")
                # f.write(
                #     "\\textbf{Grouped by " + LATEX_MAP[right_factor] + "}\\\\[4pt]\n"
                # )
                f.write(make_table(right_factor, metric, datatype))
                f.write("\n\\end{minipage}\n")

            f.write("\n\\end{table}\n\n")

    f.write(r"\end{document}")

# ------------------------------------------------------
# GLOBAL SUMMARY (independent of factors)
# ------------------------------------------------------

global_output = "output/backward/suppl-DoE_backward-graph_summary_global.tex"

with open(global_output, "w") as g:
    g.write(r"""
\begin{table}[H]
\centering
\caption{Global Summary of Graph Characteristics (Independent of Factors)}
\resizebox{\linewidth}{!}{
\begin{tabular}{lccccc}
\toprule
Metric & Count & Mean & Std & Min & Max \\
\midrule
""")

    # Loop over all metrics and compute global summary
    for metric, datatype in METRICS:
        series = df_combined[metric]

        count = series.count()
        mean = float(series.mean())
        std = float(series.std())
        min_v = float(series.min())
        max_v = float(series.max())

        metric_label = LATEX_MAP.get(metric, metric)

        g.write(
            f"{metric_label} & {count} & {mean:.2f} & {std:.2f} & {min_v:.2f} & {max_v:.2f} \\\\\n"
        )

    g.write(r"""\bottomrule
\end{tabular}
}
\end{table}
""")
