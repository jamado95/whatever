#!/usr/bin/env python3
"""
Generate Saltelli sample matrix for Sobol sensitivity analysis.

Usage:
    python generate.py --problem problem.json --output samples.csv [--dry-run]
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from SALib.sample import saltelli

from common import (
    build_salib_problem,
    get_param_names,
    get_param_types,
    load_problem,
)


def generate_samples(problem: dict, calc_second_order: bool = True) -> np.ndarray:
    """
    Generate Saltelli samples for Sobol analysis.

    Args:
        problem: Problem definition dict
        calc_second_order: Whether to compute second-order indices

    Returns:
        Sample matrix of shape (N, num_params)
    """
    salib_problem = build_salib_problem(problem)
    # N=1024 is standard for Sobol analysis (power of 2)
    # Total samples = N * (2D + 2) for second-order, N * (D + 2) for first-order
    samples = saltelli.sample(salib_problem, 1024, calc_second_order=calc_second_order)
    return samples


def round_integer_params(samples: np.ndarray, problem: dict) -> np.ndarray:
    """
    Round integer-typed parameters to nearest integer.

    Args:
        samples: Sample matrix
        problem: Problem definition

    Returns:
        Sample matrix with integer params rounded
    """
    param_types = get_param_types(problem)
    param_names = get_param_names(problem)

    samples = samples.copy()
    for i, name in enumerate(param_names):
        if param_types[name] == "int":
            samples[:, i] = np.round(samples[:, i])

    return samples


def samples_to_dataframe(samples: np.ndarray, problem: dict) -> pd.DataFrame:
    """
    Convert sample matrix to DataFrame with parameter names.

    Args:
        samples: Sample matrix
        problem: Problem definition

    Returns:
        DataFrame with columns for each parameter
    """
    param_names = get_param_names(problem)
    return pd.DataFrame(samples, columns=param_names)


def print_summary(problem: dict, num_samples: int, dry_run: bool) -> None:
    """Print summary of sample generation."""
    num_params = len(problem["parameters"])
    metrics = problem["metrics"]

    print(f"Problem summary:")
    print(f"  Parameters: {num_params}")
    print(f"  Metrics: {len(metrics)} ({', '.join(metrics)})")
    print()
    print(f"Sample generation:")
    print(f"  Total samples: {num_samples}")
    print(f"  Formula: N * (2D + 2) = 1024 * (2*{num_params} + 2) = {1024 * (2 * num_params + 2)}")
    print()

    # Estimated times (rough heuristics)
    print("Estimated evaluation times (assuming 1 sec/sample):")
    print(f"  Sequential: ~{num_samples / 60:.1f} minutes")
    print(f"  Parallel (8 workers): ~{num_samples / 60 / 8:.1f} minutes")
    print()

    if dry_run:
        print("DRY RUN - No files written")


def main():
    parser = argparse.ArgumentParser(
        description="Generate Saltelli samples for Sobol sensitivity analysis"
    )
    parser.add_argument(
        "--problem",
        required=True,
        help="Path to problem definition JSON file",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to output samples CSV file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print summary without writing files",
    )

    args = parser.parse_args()

    # Load problem definition
    problem_path = Path(args.problem).resolve()
    print(f"Loading problem: {problem_path}")
    problem = load_problem(problem_path)

    # Generate samples
    print("Generating Saltelli samples...")
    samples = generate_samples(problem)
    samples = round_integer_params(samples, problem)
    df = samples_to_dataframe(samples, problem)

    # Print summary
    print()
    print_summary(problem, len(df), args.dry_run)

    # Write output
    if not args.dry_run:
        output_path = Path(args.output).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        print(f"Samples written to: {output_path}")


if __name__ == "__main__":
    main()
