#!/usr/bin/env python3
"""
Compute Sobol sensitivity indices from simulation results.

Usage:
    python analyze.py --problem problem.json --results results.csv --output analysis.json
"""

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from SALib.analyze import sobol

from common import (
    CATEGORIES,
    build_salib_problem,
    get_param_bounds,
    get_param_categories,
    get_param_names,
    load_problem,
    validate_csv_columns,
)


def load_results(path: str, problem: dict) -> pd.DataFrame:
    """
    Load and validate results CSV.

    Args:
        path: Path to results CSV
        problem: Problem definition

    Returns:
        DataFrame with parameter and metric columns
    """
    path = Path(path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Results file not found: {path}")

    df = pd.read_csv(path)

    # Validate columns
    param_names = get_param_names(problem)
    metric_names = problem["metrics"]
    required_cols = param_names + metric_names
    validate_csv_columns(df, required_cols, str(path))

    return df


def check_missing_rows(df: pd.DataFrame, expected_count: int) -> None:
    """
    Check for missing rows and warn/error accordingly.

    Args:
        df: Results DataFrame
        expected_count: Expected number of rows from samples

    Raises:
        ValueError: If more than 5% of rows are missing
    """
    actual_count = len(df)
    missing_count = expected_count - actual_count

    if missing_count <= 0:
        return

    missing_pct = missing_count / expected_count * 100

    if missing_pct >= 5:
        raise ValueError(
            f"Too many missing rows: {missing_count} ({missing_pct:.1f}%) missing. "
            f"Expected {expected_count}, got {actual_count}. "
            f"Maximum allowed is 5%."
        )
    elif missing_count > 0:
        print(f"WARNING: {missing_count} rows ({missing_pct:.1f}%) missing from results. "
              f"Analysis will proceed but results may be less accurate.")


def compute_sobol_indices(problem: dict, df: pd.DataFrame) -> dict:
    """
    Compute Sobol indices for each metric.

    Args:
        problem: Problem definition
        df: Results DataFrame

    Returns:
        Dict mapping metric name to Sobol indices
    """
    salib_problem = build_salib_problem(problem)
    param_names = get_param_names(problem)
    metrics = problem["metrics"]

    results = {}
    for metric in metrics:
        Y = df[metric].values

        # Handle NaN values by replacing with mean
        if np.isnan(Y).any():
            nan_count = np.isnan(Y).sum()
            print(f"WARNING: {nan_count} NaN values in metric '{metric}', replacing with mean")
            Y = np.where(np.isnan(Y), np.nanmean(Y), Y)

        # Compute Sobol indices
        Si = sobol.analyze(salib_problem, Y, calc_second_order=True, print_to_console=False)

        results[metric] = {
            "S1": {name: float(Si["S1"][i]) for i, name in enumerate(param_names)},
            "S1_conf": {name: float(Si["S1_conf"][i]) for i, name in enumerate(param_names)},
            "ST": {name: float(Si["ST"][i]) for i, name in enumerate(param_names)},
            "ST_conf": {name: float(Si["ST_conf"][i]) for i, name in enumerate(param_names)},
        }

        # Add second-order indices if available
        if "S2" in Si:
            s2_dict = {}
            for i, name_i in enumerate(param_names):
                for j, name_j in enumerate(param_names):
                    if i < j:
                        key = f"{name_i}:{name_j}"
                        s2_dict[key] = float(Si["S2"][i, j])
            results[metric]["S2"] = s2_dict

    return results


def compute_group_indices(sobol_results: dict, problem: dict) -> dict:
    """
    Compute group-level indices by summing S1 within categories.

    Args:
        sobol_results: Sobol indices by metric
        problem: Problem definition

    Returns:
        Dict mapping metric -> category -> S1 sum
    """
    param_categories = get_param_categories(problem)
    group_indices = {}

    for metric, indices in sobol_results.items():
        s1 = indices["S1"]
        group_indices[metric] = {}

        for category in CATEGORIES:
            # Sum S1 for all params in this category
            category_sum = sum(
                s1[name] for name, cat in param_categories.items() if cat == category
            )
            group_indices[metric][category] = category_sum

    return group_indices


def compute_interaction_shares(sobol_results: dict) -> dict:
    """
    Compute interaction shares: (ST - S1) / ST for each parameter.

    This measures how much of a parameter's total effect comes from interactions.

    Args:
        sobol_results: Sobol indices by metric

    Returns:
        Dict mapping metric -> param -> interaction share
    """
    interaction_shares = {}

    for metric, indices in sobol_results.items():
        s1 = indices["S1"]
        st = indices["ST"]
        interaction_shares[metric] = {}

        for name in s1:
            st_val = st[name]
            s1_val = s1[name]
            if st_val > 0.001:  # Avoid division by zero/small numbers
                share = (st_val - s1_val) / st_val
            else:
                share = 0.0
            interaction_shares[metric][name] = share

    return interaction_shares


def compute_cross_group_interactions(sobol_results: dict, problem: dict) -> dict:
    """
    Compute cross-group interaction sums from S2 indices.

    Args:
        sobol_results: Sobol indices by metric
        problem: Problem definition

    Returns:
        Dict mapping metric -> (cat1, cat2) -> interaction sum
    """
    param_categories = get_param_categories(problem)
    cross_group = {}

    for metric, indices in sobol_results.items():
        if "S2" not in indices:
            continue

        s2 = indices["S2"]
        cross_group[metric] = {}

        # Initialize all category pairs
        for i, cat1 in enumerate(CATEGORIES):
            for j, cat2 in enumerate(CATEGORIES):
                if i <= j:
                    key = f"{cat1}:{cat2}"
                    cross_group[metric][key] = 0.0

        # Sum S2 by category pairs
        for pair, value in s2.items():
            name1, name2 = pair.split(":")
            cat1 = param_categories[name1]
            cat2 = param_categories[name2]

            # Ensure consistent ordering
            if CATEGORIES.index(cat1) > CATEGORIES.index(cat2):
                cat1, cat2 = cat2, cat1

            key = f"{cat1}:{cat2}"
            cross_group[metric][key] += value

    return cross_group


def detect_params_at_bounds(df: pd.DataFrame, problem: dict, threshold: float = 0.05) -> dict:
    """
    Detect parameters where optimal values cluster near bounds.

    Args:
        df: Results DataFrame
        problem: Problem definition
        threshold: Fraction of range to consider "at bound"

    Returns:
        Dict mapping param -> bound type ("lower", "upper", or None)
    """
    param_names = get_param_names(problem)
    param_bounds = get_param_bounds(problem)
    metrics = problem["metrics"]

    # Use first metric for detection (usually the primary objective)
    primary_metric = metrics[0]

    # Find top 10% of results
    top_pct = df.nlargest(int(len(df) * 0.1), primary_metric)

    at_bounds = {}
    for name in param_names:
        low, high = param_bounds[name]
        range_val = high - low
        lower_threshold = low + threshold * range_val
        upper_threshold = high - threshold * range_val

        values = top_pct[name]
        at_lower = (values <= lower_threshold).mean()
        at_upper = (values >= upper_threshold).mean()

        if at_lower > 0.5:
            at_bounds[name] = "lower"
        elif at_upper > 0.5:
            at_bounds[name] = "upper"
        else:
            at_bounds[name] = None

    return at_bounds


def generate_diagnostics(
    sobol_results: dict,
    group_indices: dict,
    interaction_shares: dict,
    cross_group: dict,
    params_at_bounds: dict,
    problem: dict,
) -> dict:
    """
    Generate diagnostic flags for the analysis.

    Args:
        sobol_results: Sobol indices by metric
        group_indices: Group-level indices
        interaction_shares: Interaction shares
        cross_group: Cross-group interactions
        params_at_bounds: Parameters at bounds
        problem: Problem definition

    Returns:
        Dict of diagnostic flags
    """
    metrics = problem["metrics"]
    primary_metric = metrics[0]

    diagnostics = {}

    # execution_dominates: execution category has >50% of total S1
    group = group_indices[primary_metric]
    total_s1 = sum(group.values())
    if total_s1 > 0:
        exec_share = group.get("execution", 0) / total_s1
        diagnostics["execution_dominates"] = exec_share > 0.5
    else:
        diagnostics["execution_dominates"] = False

    # high_cross_group_interaction: cross-group S2 sum > 0.3
    if primary_metric in cross_group:
        cg = cross_group[primary_metric]
        # Sum only cross-group (different categories)
        cross_sum = sum(v for k, v in cg.items() if k.split(":")[0] != k.split(":")[1])
        diagnostics["high_cross_group_interaction"] = cross_sum > 0.3
    else:
        diagnostics["high_cross_group_interaction"] = False

    # low_additivity: sum of ST > 1.5 (indicates strong interactions)
    st = sobol_results[primary_metric]["ST"]
    st_sum = sum(st.values())
    diagnostics["low_additivity"] = st_sum > 1.5

    # parameter_at_bound: any parameter has values clustering at bounds
    diagnostics["parameter_at_bound"] = any(v is not None for v in params_at_bounds.values())
    diagnostics["params_at_bounds"] = {k: v for k, v in params_at_bounds.items() if v is not None}

    return diagnostics


def main():
    parser = argparse.ArgumentParser(
        description="Compute Sobol sensitivity indices from simulation results"
    )
    parser.add_argument(
        "--problem",
        required=True,
        help="Path to problem definition JSON file",
    )
    parser.add_argument(
        "--results",
        required=True,
        help="Path to results CSV file",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to output analysis JSON file",
    )

    args = parser.parse_args()

    # Load inputs
    problem_path = Path(args.problem).resolve()
    print(f"Loading problem: {problem_path}")
    problem = load_problem(problem_path)

    results_path = Path(args.results).resolve()
    print(f"Loading results: {results_path}")
    df = load_results(results_path, problem)

    # Check for missing rows
    num_params = len(problem["parameters"])
    expected_samples = 1024 * (2 * num_params + 2)
    check_missing_rows(df, expected_samples)

    # Compute Sobol indices
    print("Computing Sobol indices...")
    sobol_results = compute_sobol_indices(problem, df)

    # Compute group-level indices
    print("Computing group-level indices...")
    group_indices = compute_group_indices(sobol_results, problem)

    # Compute interaction shares
    print("Computing interaction shares...")
    interaction_shares = compute_interaction_shares(sobol_results)

    # Compute cross-group interactions
    print("Computing cross-group interactions...")
    cross_group = compute_cross_group_interactions(sobol_results, problem)

    # Detect parameters at bounds
    print("Detecting parameters at bounds...")
    params_at_bounds = detect_params_at_bounds(df, problem)

    # Generate diagnostics
    print("Generating diagnostics...")
    diagnostics = generate_diagnostics(
        sobol_results, group_indices, interaction_shares, cross_group, params_at_bounds, problem
    )

    # Build output
    output = {
        "sobol_indices": sobol_results,
        "group_indices": group_indices,
        "interaction_shares": interaction_shares,
        "cross_group_interactions": cross_group,
        "diagnostics": diagnostics,
    }

    # Write output
    output_path = Path(args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nAnalysis written to: {output_path}")

    # Print summary
    print("\nDiagnostics summary:")
    for key, value in diagnostics.items():
        if key != "params_at_bounds":
            print(f"  {key}: {value}")
    if diagnostics.get("params_at_bounds"):
        print(f"  params_at_bounds: {diagnostics['params_at_bounds']}")


if __name__ == "__main__":
    main()
