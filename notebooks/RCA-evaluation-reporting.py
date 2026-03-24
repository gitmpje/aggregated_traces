# %%
import json
import numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm

import pandas as pd

from sklearn.preprocessing import StandardScaler

from utils import (
    VARIABLES_MAPPING,
    combine_steps,
    merge_split_order,
    parse_scenario_string_categories,
)

factors = [
    "merge_after_steps",
    "split_after_steps",
    "number_of_devices",
    "n_resources_factor",
]

# %%
report_name = "combined_report_DoE_revision"
df_combined_selected = pd.read_csv(
    f"output/backward/{report_name}.csv",
    usecols=[
        "scenario_name",
        "n_steps_random",
        "n_steps_sorted",
        "n_steps_diff",
        "trace_graph_selected-n_nodes",
        "trace_graph_selected-n_edges",
        "density",
        "n_simple_paths",
        "out_degrees",
        "path_lengths",
    ],
)

for factor in factors:
    df_combined_selected[factor] = df_combined_selected["scenario_name"].apply(
        lambda x: parse_scenario_string_categories(x)[factor]
    )

df_combined_selected["merge_split_steps"] = df_combined_selected.apply(
    combine_steps, axis=1
)
df_combined_selected["merge_before_split"] = df_combined_selected[
    "merge_split_steps"
].apply(merge_split_order, first="merge")
df_combined_selected["split_before_merge"] = df_combined_selected[
    "merge_split_steps"
].apply(merge_split_order, first="split")

df_combined_selected["count_merge"] = df_combined_selected[
    "merge_split_steps"
].str.count("merge")
df_combined_selected["count_split"] = df_combined_selected[
    "merge_split_steps"
].str.count("split")

df_combined_selected["split_before_merge"] = df_combined_selected[
    "split_before_merge"
].astype(int)
df_combined_selected["number_of_devices"] = df_combined_selected[
    "number_of_devices"
].replace("125-25", "25-125")

# %%
top_k_rate_random = []
max_k_random = round(df_combined_selected["n_steps_random"].max())
for k in range(1, max_k_random):
    top_k_rate_random.append(
        (df_combined_selected["n_steps_random"].dropna() <= k).mean()
    )

top_k_rate_sorted = []
max_k_sorted = round(df_combined_selected["n_steps_sorted"].max())
for k in range(1, max_k_sorted):
    top_k_rate_sorted.append(
        (df_combined_selected["n_steps_sorted"].dropna() <= k).mean()
    )

plt.plot(range(1, max_k_random), top_k_rate_random, marker="x", label="random")
plt.plot(range(1, max_k_sorted), top_k_rate_sorted, marker="o", label="sorted")
plt.legend()
plt.ylabel("Top-$k$ hit rate")
plt.xlabel("$k$")
plt.xticks(range(1, max(max_k_random, max_k_sorted)))

plt.tight_layout()
plt.savefig("output/backward/figures/top_k_hit_rate.eps")

# %%
y_variable = "n_steps_diff"  # "kl_divergence"

variables = [
    "count_merge",
    "count_split",
    "split_before_merge",
    "number_of_devices_25-75",
    "number_of_devices_10-90",
    "number_of_devices_25-125",
    "number_of_devices_10-170",
    "n_resources_factor_2",
    "n_resources_factor_4",
    "n_resources_factor_8",
    "n_resources_factor_16",
]

# Prepare the data: drop rows with missing n_steps_diff or any factor
factors_reg = [
    "split_before_merge",
    # "kl_divergence",
]
normalize_factors = [
    "count_merge",
    "count_split",
]
dummy_factors = [
    "number_of_devices",
    "n_resources_factor",
]
df_reg = df_combined_selected.dropna(
    subset=factors_reg + dummy_factors + [y_variable]
).copy()

# Encode categorical variables
df_reg["split_before_merge"] = df_reg["split_before_merge"].astype(int)
df_reg["number_of_devices"] = df_reg["number_of_devices"].astype(str)
df_reg["n_resources_factor"] = df_reg["n_resources_factor"].astype(str)

# Normalize factors
scaler = StandardScaler()
for factor in normalize_factors:
    df_reg[factor] = scaler.fit_transform(df_reg[[factor]])

# Create dummy variables for categorical factors
dummy_factors = ["number_of_devices", "n_resources_factor"]
df_reg = pd.get_dummies(df_reg, columns=dummy_factors, drop_first=False, dtype=int)
df_reg.drop(
    ["number_of_devices_50", "n_resources_factor_1"],
    axis=1,
    inplace=True,
    errors="ignore",
)

dummy_columns = [c for c in df_reg.columns if any(f in c for f in dummy_factors)]

# Define X and y
X = df_reg[dummy_columns]
X = pd.concat([df_reg[factors_reg + normalize_factors], X], axis=1)
y = df_reg[y_variable]

# Add constant for intercept
X = sm.add_constant(X)

# Fit the model
model = sm.OLS(y, X).fit()

# Print the summary
print(model.summary())

# Get coefficients, confidence intervals, and p-values
coefs = model.params
conf = model.conf_int()
conf.columns = ["lower", "upper"]
pvalues = model.pvalues

# Prepare DataFrame for plotting
coef_df = coefs.to_frame("coef").join(conf)
coef_df = coef_df.reset_index().rename(columns={"index": "variable"})
coef_df["pvalue"] = coef_df["variable"].map(pvalues)
coef_df.set_index("variable", inplace=True)

# Filter variables with p-value <= 0.005
significant_vars = [v for v in variables if coef_df.loc[v, "pvalue"] <= 0.005]

# Plot coefficients with confidence intervals for significant variables only
plt.figure(figsize=(8, 5))
plt.errorbar(
    significant_vars,
    coef_df.loc[significant_vars, "coef"],
    yerr=[
        coef_df.loc[significant_vars, "coef"] - coef_df.loc[significant_vars, "lower"],
        coef_df.loc[significant_vars, "upper"] - coef_df.loc[significant_vars, "coef"],
    ],
    fmt="o",
    capsize=5,
    color="b",
)
plt.axhline(0, color="grey", linestyle="--")

xtick_labels = [VARIABLES_MAPPING[l.get_text()] for l in plt.xticks()[1]]
plt.xticks(ticks=plt.xticks()[0], labels=xtick_labels, rotation=45, ha="right")

plt.ylabel("Coefficient")
plt.title("MLR Coefficients with 95% Confidence Intervals (p ≤ 0.005)")
plt.tight_layout()

plt.savefig(
    "output/backward/figures/backward-mlr-ols-coefficients-significant.svg",
    bbox_inches="tight",
)
plt.savefig(
    "output/backward/figures/backward-mlr-ols-coefficients-significant.eps",
    bbox_inches="tight",
)

# %% Graph metrics
df_combined_selected["out_degrees"] = df_combined_selected["out_degrees"].apply(
    lambda x: json.loads(x)
)
df_combined_selected["avg_out_degree"] = df_combined_selected["out_degrees"].apply(
    lambda x: sum(x) / len(x)
)
df_combined_selected["max_out_degree"] = df_combined_selected["out_degrees"].apply(
    lambda x: max(x)
)

df_combined_selected["path_lengths"] = df_combined_selected["path_lengths"].apply(
    lambda x: json.loads(x)
)
df_combined_selected["avg_path_length"] = df_combined_selected["path_lengths"].apply(
    lambda x: sum(x) / len(x)
)
df_combined_selected["max_path_length"] = df_combined_selected["path_lengths"].apply(
    lambda x: max(x)
)

# %% Define columns to include in visualization
x_cols = [
    "number_of_devices",
    "n_resources_factor",
    "count_merge",
    "count_split",
    "split_before_merge",
    # "avg_out_degree",
    # "max_out_degree",
    # "avg_path_length",
    # "max_path_length",
    # "trace_graph_selected-n_nodes",
    # "trace_graph_selected-n_edges",
    # "density",
    # "n_simple_paths",
]
y_cols = [
    # "avg_out_degree",
    # "max_out_degree",
    # "avg_path_length",
    # "max_path_length",
    # "trace_graph_selected-n_nodes",
    # "trace_graph_selected-n_edges",
    # "density",
    # "n_simple_paths",
    "n_steps_diff",
    # "top_1_hit_sorted",
]

custom_order = {
    "number_of_devices": ["50", "25-75", "10-90", "25-125", "10-170"],
    "n_resources_factor": ["1", "2", "4", "8", "16"],
}

VARIABLES_MAPPING.update({"avg_out_degree": "Average out-degree"})

df_combined_selected["top_1_hit_random"] = (
    df_combined_selected["n_steps_random"].dropna() <= 1
)
df_combined_selected["top_1_hit_sorted"] = (
    df_combined_selected["n_steps_sorted"].dropna() <= 1
)

# %% Scatter plots

# Create subplot grid: rows = y_cols, cols = x_cols
fig, axes = plt.subplots(
    len(y_cols), len(x_cols), figsize=(5 * len(x_cols), 4 * len(y_cols)), squeeze=False
)

for i, y in enumerate(y_cols):
    for j, x in enumerate(x_cols):
        ax = axes[i, j]

        # Scatter plot
        ax.scatter(
            df_combined_selected[x],
            df_combined_selected[y],
            color="blue",
            alpha=0.7,
            edgecolors="k",
        )

        # ---- Add regression line ----
        X = df_combined_selected[x].values
        Y = df_combined_selected[y].values

        # Fit regression (degree 1 = linear)
        slope, intercept = np.polyfit(X, Y, 1)
        reg_x = np.linspace(X.min(), X.max(), 200)
        reg_y = slope * reg_x + intercept

        # Plot regression line
        ax.plot(reg_x, reg_y, color="red", linewidth=2, label="Regression line")
        ax.legend()

        # Labels and title
        ax.set_xlabel(VARIABLES_MAPPING[x])
        ax.set_ylabel(VARIABLES_MAPPING[y])
        ax.set_title(f"{VARIABLES_MAPPING[y]} vs {VARIABLES_MAPPING[x]}")
        ax.grid(True, linestyle="--", alpha=0.5)

plt.tight_layout()

# %% Boxplots
# Create subplot grid: rows = y_cols, cols = x_cols
fig, axes = plt.subplots(
    len(y_cols), len(x_cols), figsize=(5 * len(x_cols), 4 * len(y_cols)), squeeze=False
)

for i, y in enumerate(y_cols):
    for j, x in enumerate(x_cols):
        ax = axes[i, j]

        try:
            # Apply custom ordering if available
            if x in custom_order:
                ordered_categories = custom_order[x]
                df_plot = df_combined_selected.copy()
                df_plot[x] = pd.Categorical(
                    df_plot[x], categories=ordered_categories, ordered=True
                )
            else:
                df_plot = df_combined_selected

            df_plot.dropna(subset=[y]).boxplot(
                column=y, by=x, showfliers=True, ax=ax, grid=False
            )

            # ax.set_title(f"{VARIABLES_MAPPING[y]} by {VARIABLES_MAPPING[x]}")
            ax.set_title("")
            ax.set_xlabel(VARIABLES_MAPPING[x])
            ax.set_ylabel(VARIABLES_MAPPING[y])

        except Exception as e:
            ax.text(0.5, 0.5, f"Error: {e}", ha="center", va="center")

        # Remove pandas auto-title
        plt.suptitle("")

plt.tight_layout()
# plt.savefig("output/backward/figures/boxplot-parameters-avg_out_degree.eps")


# %% Mean + CI plots
def bootstrap_ci(data, n_boot=500, ci=95):
    """Memory-safe bootstrap CI for the mean."""
    # Avoid huge (n_boot, N) arrays
    boot_means = []
    n = len(data)

    for _ in range(n_boot):
        sample_idx = np.random.randint(0, n, n)
        boot_means.append(data[sample_idx].mean())

    lower = np.percentile(boot_means, (100 - ci) / 2)
    upper = np.percentile(boot_means, 100 - (100 - ci) / 2)
    return lower, upper


fig, axes = plt.subplots(
    len(y_cols), len(x_cols), figsize=(3 * len(x_cols), 5 * len(y_cols)), squeeze=False
)

# Store per-row ymin/ymax
row_min = {y: np.inf for y in y_cols}
row_max = {y: -np.inf for y in y_cols}

# Also store the computed (means, err, categories) so we don't recompute CI
plot_data = {(i, j): {} for i in range(len(y_cols)) for j in range(len(x_cols))}

# Compute means + CI and track row-wise min/max
for i, y in enumerate(y_cols):
    for j, x in enumerate(x_cols):
        ax = axes[i, j]

        # Apply category order if supplied
        if x in custom_order:
            ordered = custom_order[x]
            df_plot = df_combined_selected.copy()
            df_plot[x] = pd.Categorical(df_plot[x], categories=ordered, ordered=True)
            categories = ordered
        else:
            df_plot = df_combined_selected
            categories = sorted(df_plot[x].unique())

        means = []
        err_low = []
        err_high = []

        # Compute per-category means + CIs (cheap)
        for cat in categories:
            subset = df_plot[df_plot[x] == cat][y].dropna().values

            if len(subset) > 1:
                m = subset.mean()
                ci_lo, ci_hi = bootstrap_ci(subset)
            else:
                m = np.nan
                ci_lo = ci_hi = np.nan

            means.append(m)
            err_low.append(m - ci_lo)
            err_high.append(ci_hi - m)

        means = np.array(means, dtype=float)
        err_low = np.array(err_low, dtype=float)
        err_high = np.array(err_high, dtype=float)

        # Store for second pass (avoid recomputing!)
        plot_data[(i, j)] = dict(
            categories=categories, means=means, err=np.vstack([err_low, err_high])
        )

        # Track row min/max *ignoring NaNs*
        finite_low = np.nanmin(means - err_low)
        finite_high = np.nanmax(means + err_high)

        row_min[y] = min(row_min[y], finite_low)
        row_max[y] = max(row_max[y], finite_high)

        # Plot (first pass)
        ax.errorbar(
            categories,
            means,
            yerr=np.vstack([err_low, err_high]),
            fmt="o",
            capsize=4,
            color="black",
        )
        ax.set_xticks(np.arange(len(categories)))
        ax.set_xticklabels(categories, ha="right")
        ax.set_xlim(-0.5, len(categories) - 0.5)

        if j == 0:
            ax.set_ylabel(VARIABLES_MAPPING[y])
        ax.set_xlabel(VARIABLES_MAPPING[x])
        ax.set_title("")

        if j > 0:
            ax.set_ylabel("")  # remove label
            ax.tick_params(labelleft=False)  # hide tick labels
            # ax.spines["left"].set_visible(False)  # hide left spine


# Apply row-wise shared y-limits
for i, y in enumerate(y_cols):
    ymin = row_min[y]
    ymax = row_max[y]
    pad = 0.05 * (ymax - ymin)

    for ax in axes[i, :]:
        ax.set_ylim(ymin - pad, ymax + pad)

plt.suptitle("")
plt.tight_layout()
# plt.savefig("output/backward/figures/ci-parameters-n_steps_diff.eps")


# %% Effect‑size Plot (Cohen’s d Between Adjacent Classes)
def cohens_d(a, b):
    # Handle empty or single-point sets
    if len(a) < 2 or len(b) < 2:
        return np.nan
    pooled_sd = np.sqrt(((a.std(ddof=1) ** 2) + (b.std(ddof=1) ** 2)) / 2)
    if pooled_sd == 0:
        return 0
    return (a.mean() - b.mean()) / pooled_sd


fig, axes = plt.subplots(
    len(y_cols), len(x_cols), figsize=(5 * len(x_cols), 4 * len(y_cols)), squeeze=False
)

for i, y in enumerate(y_cols):
    for j, x in enumerate(x_cols):
        ax = axes[i, j]

        try:
            # Ordering
            if x in custom_order:
                categories = custom_order[x]
                df_plot = df_combined_selected.copy()
                df_plot[x] = pd.Categorical(
                    df_plot[x], categories=categories, ordered=True
                )
            else:
                df_plot = df_combined_selected
                categories = sorted(df_plot[x].unique())

            # Compute Cohen's d for adjacent pairs
            d_values = []
            labels = []

            for k in range(len(categories) - 1):
                c1, c2 = categories[k], categories[k + 1]
                a = df_plot[df_plot[x] == c1][y].dropna()
                b = df_plot[df_plot[x] == c2][y].dropna()
                d = cohens_d(a, b)
                d_values.append(d)
                labels.append(f"{c1} vs {c2}")

            ax.bar(range(len(d_values)), d_values)
            ax.set_xticks(range(len(d_values)))
            ax.set_xticklabels(labels, rotation=30, ha="right")
            ax.set_ylabel(f"Effect size (Cohen's d)")
            ax.set_xlabel(VARIABLES_MAPPING[x])
            ax.axhline(0, color="black", linewidth=1)
            ax.set_title("")

        except Exception as e:
            ax.text(0.5, 0.5, f"Error: {e}", ha="center", va="center")

plt.suptitle("")
plt.tight_layout()
