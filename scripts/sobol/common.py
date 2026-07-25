"""
Shared utilities for Sobol sensitivity analysis.
"""

import json
from pathlib import Path

import pandas as pd

# Parameter categories for grouping
CATEGORIES = ["signal-structural", "signal-threshold", "execution"]


def load_problem(path: str) -> dict:
    """
    Load and validate problem definition from JSON file.

    Expected format:
    {
        "parameters": [
            {
                "name": "param_name",
                "bounds": [min, max],
                "type": "float" | "int",
                "category": "signal-structural" | "signal-threshold" | "execution"
            },
            ...
        ],
        "metrics": ["metric1", "metric2", ...]
    }
    """
    path = Path(path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Problem file not found: {path}")

    with open(path) as f:
        problem = json.load(f)

    # Validate structure
    if "parameters" not in problem:
        raise ValueError(f"Problem file missing 'parameters' key: {path}")
    if "metrics" not in problem:
        raise ValueError(f"Problem file missing 'metrics' key: {path}")

    if not problem["parameters"]:
        raise ValueError(f"Problem file has empty 'parameters' list: {path}")
    if not problem["metrics"]:
        raise ValueError(f"Problem file has empty 'metrics' list: {path}")

    # Validate each parameter
    for i, param in enumerate(problem["parameters"]):
        if "name" not in param:
            raise ValueError(f"Parameter {i} missing 'name': {path}")
        if "bounds" not in param:
            raise ValueError(f"Parameter '{param['name']}' missing 'bounds': {path}")
        if len(param["bounds"]) != 2:
            raise ValueError(f"Parameter '{param['name']}' bounds must have 2 values: {path}")
        if param["bounds"][0] >= param["bounds"][1]:
            raise ValueError(f"Parameter '{param['name']}' has invalid bounds (min >= max): {path}")

        # Default type to float if not specified
        if "type" not in param:
            param["type"] = "float"
        if param["type"] not in ("float", "int"):
            raise ValueError(f"Parameter '{param['name']}' has invalid type '{param['type']}': {path}")

        # Default category if not specified
        if "category" not in param:
            param["category"] = "signal-structural"
        if param["category"] not in CATEGORIES:
            raise ValueError(
                f"Parameter '{param['name']}' has invalid category '{param['category']}'. "
                f"Must be one of: {CATEGORIES}"
            )

    return problem


def build_salib_problem(problem: dict) -> dict:
    """
    Convert problem definition to SALib problem dict format.

    Returns:
        {
            "num_vars": int,
            "names": [str, ...],
            "bounds": [[min, max], ...],
            "groups": [str, ...]  # Optional, for grouped analysis
        }
    """
    params = problem["parameters"]
    return {
        "num_vars": len(params),
        "names": [p["name"] for p in params],
        "bounds": [p["bounds"] for p in params],
        "groups": [p["category"] for p in params],
    }


def validate_csv_columns(df: pd.DataFrame, required_cols: list[str], file_path: str) -> None:
    """
    Verify that required columns exist in DataFrame.

    Args:
        df: DataFrame to validate
        required_cols: List of column names that must exist
        file_path: Path to file (for error messages)

    Raises:
        ValueError: If any required columns are missing
    """
    missing = set(required_cols) - set(df.columns)
    if missing:
        raise ValueError(
            f"Missing required columns in {file_path}: {sorted(missing)}\n"
            f"Available columns: {sorted(df.columns)}"
        )


def get_param_names(problem: dict) -> list[str]:
    """Get list of parameter names from problem definition."""
    return [p["name"] for p in problem["parameters"]]


def get_param_types(problem: dict) -> dict[str, str]:
    """Get mapping of parameter name to type."""
    return {p["name"]: p["type"] for p in problem["parameters"]}


def get_param_categories(problem: dict) -> dict[str, str]:
    """Get mapping of parameter name to category."""
    return {p["name"]: p["category"] for p in problem["parameters"]}


def get_param_bounds(problem: dict) -> dict[str, tuple[float, float]]:
    """Get mapping of parameter name to bounds."""
    return {p["name"]: tuple(p["bounds"]) for p in problem["parameters"]}
