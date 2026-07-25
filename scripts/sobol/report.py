#!/usr/bin/env python3
"""
Generate visualizations and summary report from Sobol analysis.

Usage:
    python report.py --problem problem.json --analysis analysis.json --results results.csv --output output_dir/
"""

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from common import (
    CATEGORIES,
    get_param_bounds,
    get_param_categories,
    get_param_names,
    load_problem,
    validate_csv_columns,
)

# Category colors
CATEGORY_COLORS = {
    "signal-structural": "#4ECDC4",  # Teal
    "signal-threshold": "#FFE66D",   # Yellow
    "execution": "#FF6B6B",          # Coral
}


def setup_style():
    """Set up matplotlib style for dark background."""
    plt.style.use("dark_background")
    plt.rcParams["figure.dpi"] = 150
    plt.rcParams["savefig.dpi"] = 150
    plt.rcParams["font.size"] = 10
    plt.rcParams["axes.titlesize"] = 12
    plt.rcParams["axes.labelsize"] = 10


def load_analysis(path: str) -> dict:
    """Load analysis JSON file."""
    path = Path(path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Analysis file not found: {path}")

    with open(path) as f:
        return json.load(f)


def load_results(path: str, problem: dict) -> pd.DataFrame:
    """Load results CSV file."""
    path = Path(path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Results file not found: {path}")

    df = pd.read_csv(path)
    param_names = get_param_names(problem)
    metric_names = problem["metrics"]
    validate_csv_columns(df, param_names + metric_names, str(path))

    return df


def plot_sobol_indices(
    analysis: dict,
    problem: dict,
    metric: str,
    output_path: Path,
) -> None:
    """
    Create horizontal bar chart of Sobol indices.

    S1 shown as filled bar, ST shown as total bar (outline extends beyond S1).
    Colored by parameter category.
    """
    param_names = get_param_names(problem)
    param_categories = get_param_categories(problem)

    s1 = analysis["sobol_indices"][metric]["S1"]
    st = analysis["sobol_indices"][metric]["ST"]
    s1_conf = analysis["sobol_indices"][metric]["S1_conf"]
    st_conf = analysis["sobol_indices"][metric]["ST_conf"]

    # Sort by ST descending
    sorted_params = sorted(param_names, key=lambda p: st[p], reverse=True)

    fig, ax = plt.subplots(figsize=(12, 6))

    y_pos = np.arange(len(sorted_params))

    # Draw ST bars (total effect, lighter/outline)
    for i, param in enumerate(sorted_params):
        color = CATEGORY_COLORS[param_categories[param]]
        ax.barh(i, st[param], color=color, alpha=0.3, edgecolor=color, linewidth=2)

    # Draw S1 bars (first-order effect, filled)
    for i, param in enumerate(sorted_params):
        color = CATEGORY_COLORS[param_categories[param]]
        ax.barh(i, s1[param], color=color, alpha=0.9)

    # Error bars for ST
    st_vals = [st[p] for p in sorted_params]
    st_conf_vals = [st_conf[p] for p in sorted_params]
    ax.errorbar(st_vals, y_pos, xerr=st_conf_vals, fmt="none", color="white", alpha=0.5, capsize=3)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(sorted_params)
    ax.set_xlabel("Sensitivity Index")
    ax.set_title(f"Sobol Indices for {metric}\n(filled=S1/first-order, outline=ST/total)")
    ax.set_xlim(0, max(max(st.values()) * 1.2, 0.1))

    # Add legend for categories
    handles = [
        plt.Rectangle((0, 0), 1, 1, color=CATEGORY_COLORS[cat], alpha=0.9)
        for cat in CATEGORIES
    ]
    ax.legend(handles, CATEGORIES, loc="lower right")

    ax.invert_yaxis()  # Highest at top
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


def plot_group_contributions(
    analysis: dict,
    metric: str,
    output_path: Path,
) -> None:
    """Create pie chart of category S1 totals."""
    group_indices = analysis["group_indices"][metric]

    # Filter out zero or negative values
    values = []
    labels = []
    colors = []
    for cat in CATEGORIES:
        val = group_indices.get(cat, 0)
        if val > 0.001:
            values.append(val)
            labels.append(cat)
            colors.append(CATEGORY_COLORS[cat])

    if not values:
        print(f"WARNING: No positive group contributions for metric '{metric}'")
        return

    fig, ax = plt.subplots(figsize=(10, 8))

    wedges, texts, autotexts = ax.pie(
        values,
        labels=labels,
        colors=colors,
        autopct="%1.1f%%",
        startangle=90,
        textprops={"color": "white"},
    )

    ax.set_title(f"Category Contributions to {metric}")

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


def plot_heatmap(
    df: pd.DataFrame,
    param1: str,
    param2: str,
    metric: str,
    problem: dict,
    output_path: Path,
    bins: int = 20,
) -> None:
    """
    Create 2D heatmap of metric values for two parameters.
    """
    param_bounds = get_param_bounds(problem)
    bounds1 = param_bounds[param1]
    bounds2 = param_bounds[param2]

    # Create bins
    x_edges = np.linspace(bounds1[0], bounds1[1], bins + 1)
    y_edges = np.linspace(bounds2[0], bounds2[1], bins + 1)

    # Compute bin indices
    x_idx = np.digitize(df[param1], x_edges) - 1
    y_idx = np.digitize(df[param2], y_edges) - 1

    # Clip to valid range
    x_idx = np.clip(x_idx, 0, bins - 1)
    y_idx = np.clip(y_idx, 0, bins - 1)

    # Compute mean metric value in each bin
    heatmap = np.full((bins, bins), np.nan)
    for i in range(bins):
        for j in range(bins):
            mask = (x_idx == i) & (y_idx == j)
            if mask.any():
                heatmap[j, i] = df.loc[mask, metric].mean()

    fig, ax = plt.subplots(figsize=(10, 8))

    # Use extent to set axis labels correctly
    extent = [bounds1[0], bounds1[1], bounds2[0], bounds2[1]]
    im = ax.imshow(
        heatmap,
        origin="lower",
        aspect="auto",
        extent=extent,
        cmap="viridis",
    )

    ax.set_xlabel(param1)
    ax.set_ylabel(param2)
    ax.set_title(f"{metric}: {param1} vs {param2}")

    plt.colorbar(im, ax=ax, label=metric)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


def plot_sweep(
    df: pd.DataFrame,
    param: str,
    metric: str,
    problem: dict,
    output_path: Path,
    bins: int = 30,
) -> None:
    """
    Create 1D sweep plot showing mean ± std of metric vs parameter.
    """
    param_bounds = get_param_bounds(problem)
    bounds = param_bounds[param]

    # Create bins
    edges = np.linspace(bounds[0], bounds[1], bins + 1)
    centers = (edges[:-1] + edges[1:]) / 2

    # Compute bin indices
    idx = np.digitize(df[param], edges) - 1
    idx = np.clip(idx, 0, bins - 1)

    # Compute mean and std in each bin
    means = np.zeros(bins)
    stds = np.zeros(bins)
    for i in range(bins):
        mask = idx == i
        if mask.any():
            means[i] = df.loc[mask, metric].mean()
            stds[i] = df.loc[mask, metric].std()
        else:
            means[i] = np.nan
            stds[i] = np.nan

    fig, ax = plt.subplots(figsize=(10, 6))

    ax.fill_between(centers, means - stds, means + stds, alpha=0.3, color="#4ECDC4")
    ax.plot(centers, means, color="#4ECDC4", linewidth=2)

    ax.set_xlabel(param)
    ax.set_ylabel(metric)
    ax.set_title(f"{metric} vs {param}")
    ax.set_xlim(bounds[0], bounds[1])

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


def plot_interaction_matrix(
    analysis: dict,
    metric: str,
    output_path: Path,
) -> None:
    """
    Create 3x3 heatmap of cross-group interactions.
    """
    cross_group = analysis.get("cross_group_interactions", {}).get(metric, {})

    if not cross_group:
        print(f"WARNING: No cross-group interactions for metric '{metric}'")
        return

    # Build matrix
    matrix = np.zeros((len(CATEGORIES), len(CATEGORIES)))
    for i, cat1 in enumerate(CATEGORIES):
        for j, cat2 in enumerate(CATEGORIES):
            if i <= j:
                key = f"{cat1}:{cat2}"
                matrix[i, j] = cross_group.get(key, 0)
                matrix[j, i] = matrix[i, j]  # Symmetric

    fig, ax = plt.subplots(figsize=(8, 8))

    im = ax.imshow(matrix, cmap="YlOrRd")

    ax.set_xticks(range(len(CATEGORIES)))
    ax.set_yticks(range(len(CATEGORIES)))
    ax.set_xticklabels(CATEGORIES, rotation=45, ha="right")
    ax.set_yticklabels(CATEGORIES)

    # Add value annotations
    for i in range(len(CATEGORIES)):
        for j in range(len(CATEGORIES)):
            ax.text(j, i, f"{matrix[i, j]:.3f}", ha="center", va="center", color="black")

    ax.set_title(f"Cross-Group Interactions for {metric}")
    plt.colorbar(im, ax=ax, label="S2 Sum")

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


def determine_verdict(diagnostics: dict) -> str:
    """
    Determine overall verdict based on diagnostics.

    Returns: "PASS", "REVIEW", or "FAIL"
    """
    flags = [
        diagnostics.get("execution_dominates", False),
        diagnostics.get("high_cross_group_interaction", False),
        diagnostics.get("low_additivity", False),
        diagnostics.get("parameter_at_bound", False),
    ]

    flag_count = sum(flags)

    if flag_count == 0:
        return "PASS"
    elif flag_count <= 2:
        return "REVIEW"
    else:
        return "FAIL"


def generate_summary(
    analysis: dict,
    problem: dict,
    output_path: Path,
) -> None:
    """Generate text summary report."""
    metrics = problem["metrics"]
    primary_metric = metrics[0]
    diagnostics = analysis["diagnostics"]

    verdict = determine_verdict(diagnostics)

    lines = []
    lines.append("=" * 60)
    lines.append("SOBOL SENSITIVITY ANALYSIS SUMMARY")
    lines.append("=" * 60)
    lines.append("")

    # Verdict
    lines.append(f"VERDICT: {verdict}")
    lines.append("")

    # Parameter summary
    param_names = get_param_names(problem)
    lines.append(f"Parameters analyzed: {len(param_names)}")
    lines.append(f"Metrics: {', '.join(metrics)}")
    lines.append("")

    # Top influential parameters
    lines.append("-" * 40)
    lines.append(f"TOP INFLUENTIAL PARAMETERS ({primary_metric})")
    lines.append("-" * 40)

    s1 = analysis["sobol_indices"][primary_metric]["S1"]
    st = analysis["sobol_indices"][primary_metric]["ST"]
    sorted_params = sorted(param_names, key=lambda p: st[p], reverse=True)

    for i, param in enumerate(sorted_params[:5], 1):
        lines.append(f"  {i}. {param}: S1={s1[param]:.3f}, ST={st[param]:.3f}")

    lines.append("")

    # Group contributions
    lines.append("-" * 40)
    lines.append(f"CATEGORY CONTRIBUTIONS ({primary_metric})")
    lines.append("-" * 40)

    group_indices = analysis["group_indices"][primary_metric]
    total = sum(group_indices.values())
    for cat in CATEGORIES:
        val = group_indices.get(cat, 0)
        pct = val / total * 100 if total > 0 else 0
        lines.append(f"  {cat}: {val:.3f} ({pct:.1f}%)")

    lines.append("")

    # Diagnostics
    lines.append("-" * 40)
    lines.append("DIAGNOSTICS")
    lines.append("-" * 40)

    diag_descriptions = {
        "execution_dominates": "Execution category dominates (>50% of S1)",
        "high_cross_group_interaction": "High cross-group interactions (S2 sum > 0.3)",
        "low_additivity": "Low additivity (ST sum > 1.5, strong interactions)",
        "parameter_at_bound": "Parameters clustering at bounds",
    }

    for key, desc in diag_descriptions.items():
        status = "YES" if diagnostics.get(key, False) else "NO"
        lines.append(f"  [{status}] {desc}")

    if diagnostics.get("params_at_bounds"):
        lines.append("")
        lines.append("  Parameters at bounds:")
        for param, bound in diagnostics["params_at_bounds"].items():
            lines.append(f"    - {param}: {bound} bound")

    lines.append("")
    lines.append("=" * 60)

    # Write file
    with open(output_path, "w") as f:
        f.write("\n".join(lines))


def main():
    parser = argparse.ArgumentParser(
        description="Generate visualizations and summary from Sobol analysis"
    )
    parser.add_argument(
        "--problem",
        required=True,
        help="Path to problem definition JSON file",
    )
    parser.add_argument(
        "--analysis",
        required=True,
        help="Path to analysis JSON file",
    )
    parser.add_argument(
        "--results",
        required=True,
        help="Path to results CSV file",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to output directory",
    )

    args = parser.parse_args()

    # Set up style
    setup_style()

    # Load inputs
    problem_path = Path(args.problem).resolve()
    print(f"Loading problem: {problem_path}")
    problem = load_problem(problem_path)

    analysis_path = Path(args.analysis).resolve()
    print(f"Loading analysis: {analysis_path}")
    analysis = load_analysis(analysis_path)

    results_path = Path(args.results).resolve()
    print(f"Loading results: {results_path}")
    df = load_results(results_path, problem)

    # Create output directory
    output_dir = Path(args.output).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {output_dir}")

    # Use primary metric for all plots
    metrics = problem["metrics"]
    primary_metric = metrics[0]
    param_names = get_param_names(problem)

    # Generate plots
    print("\nGenerating plots...")

    # 1. Sobol indices bar chart
    print("  - sobol_indices.png")
    plot_sobol_indices(analysis, problem, primary_metric, output_dir / "sobol_indices.png")

    # 2. Group contributions pie chart
    print("  - group_contributions.png")
    plot_group_contributions(analysis, primary_metric, output_dir / "group_contributions.png")

    # 3. Heatmaps for top 3 parameters
    st = analysis["sobol_indices"][primary_metric]["ST"]
    top_params = sorted(param_names, key=lambda p: st[p], reverse=True)[:3]

    for i in range(len(top_params)):
        for j in range(i + 1, len(top_params)):
            p1, p2 = top_params[i], top_params[j]
            filename = f"heatmap_{p1}_vs_{p2}.png"
            print(f"  - {filename}")
            plot_heatmap(df, p1, p2, primary_metric, problem, output_dir / filename)

    # 4. Sweeps for top 5 parameters
    top_5_params = sorted(param_names, key=lambda p: st[p], reverse=True)[:5]
    for param in top_5_params:
        filename = f"sweep_{param}.png"
        print(f"  - {filename}")
        plot_sweep(df, param, primary_metric, problem, output_dir / filename)

    # 5. Interaction matrix
    print("  - interaction_matrix.png")
    plot_interaction_matrix(analysis, primary_metric, output_dir / "interaction_matrix.png")

    # 6. Summary text
    print("  - summary.txt")
    generate_summary(analysis, problem, output_dir / "summary.txt")

    print(f"\nDone! Generated {6 + len(top_params) * (len(top_params) - 1) // 2 + len(top_5_params)} files in {output_dir}")


if __name__ == "__main__":
    main()
