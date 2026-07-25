# Sobol Sensitivity Analysis Phase — Implementation Spec

## Purpose

Build a standalone Python script that implements the parameter sensitivity analysis phase of a trading strategy backtesting pipeline. This script is one component of a larger system where a Go backtesting engine runs strategies and this Python script handles sampling, orchestration, and analysis.

## Overview

The script has three modes of operation, run as subcommands:

1. **`generate`** — Produce a Saltelli sample matrix (CSV of parameter vectors for the Go engine to evaluate)
2. **`analyze`** — Consume the Go engine's output CSV and compute Sobol indices
3. **`report`** — Generate visualizations and a summary report from the analysis results

These are separate subcommands because the Go backtest runs happen between `generate` and `analyze`. The user runs `generate`, passes the output to Go, collects results, then runs `analyze` and `report`.

## Dependencies

- Python 3.10+
- SALib (for Saltelli sampling and Sobol analysis)
- pandas
- matplotlib
- numpy

Use a `requirements.txt`:
```
SALib>=1.4
pandas>=2.0
matplotlib>=3.7
numpy>=1.24
```

## Input: Problem Definition File

The script reads a JSON file that defines the parameter space. This file is the single source of truth for parameter names, bounds, types, and categories.

Example `problem.json`:
```json
{
  "strategy_name": "BuyTheDip",
  "base_sample_size": 2048,
  "parameters": [
    {
      "name": "trend_lookback",
      "bounds": [5, 50],
      "type": "integer",
      "category": "signal-structural"
    },
    {
      "name": "consecutive_closes",
      "bounds": [2, 8],
      "type": "integer",
      "category": "signal-structural"
    },
    {
      "name": "pullback_pct",
      "bounds": [0.01, 0.10],
      "type": "continuous",
      "category": "signal-threshold"
    },
    {
      "name": "maturity_score_cutoff",
      "bounds": [0.3, 0.9],
      "type": "continuous",
      "category": "signal-threshold"
    },
    {
      "name": "entry_offset_pct",
      "bounds": [0.0, 0.02],
      "type": "continuous",
      "category": "execution"
    },
    {
      "name": "stop_loss_pct",
      "bounds": [0.01, 0.10],
      "type": "continuous",
      "category": "execution"
    },
    {
      "name": "take_profit_pct",
      "bounds": [0.02, 0.20],
      "type": "continuous",
      "category": "execution"
    }
  ],
  "output_metrics": ["sharpe", "cagr", "max_drawdown", "trade_count", "win_rate", "total_pnl"]
}
```

Fields:
- `strategy_name`: identifier, used in output filenames
- `base_sample_size`: the N parameter for Saltelli sampling. Total runs will be N(2k+2) where k is the number of parameters
- `parameters[].name`: parameter name, must match column names in Go engine output
- `parameters[].bounds`: [lower, upper] inclusive range
- `parameters[].type`: `"integer"` or `"continuous"`. Integer parameters are rounded after Saltelli sampling
- `parameters[].category`: one of `"signal-structural"`, `"signal-threshold"`, `"execution"`. Used for group-level analysis
- `output_metrics`: list of metric column names the Go engine will produce. First metric in the list is the primary metric used for Sobol analysis (typically `sharpe`)

## Subcommand 1: `generate`

```
python sobol.py generate --problem problem.json --output samples.csv
```

Behavior:
1. Read `problem.json`
2. Construct SALib problem dict from the parameters (names and bounds)
3. Call `saltelli.sample(problem, N=base_sample_size, calc_second_order=True)`
4. For each parameter with `type: "integer"`, round the corresponding column to nearest integer
5. Write the resulting matrix to `samples.csv`

Output CSV format — one header row, one row per parameter vector:
```
trend_lookback,consecutive_closes,pullback_pct,maturity_score_cutoff,entry_offset_pct,stop_loss_pct,take_profit_pct
25,4,0.05,0.6,0.01,0.05,0.10
10,3,0.03,0.7,0.005,0.03,0.08
...
```

Also print to stdout:
- Total number of samples generated
- Expected number of backtest runs
- Estimated time at various run durations (e.g., "At 100ms per run: ~55 minutes")

## Go Engine Interface (not built by this script — context only)

The Go engine reads `samples.csv`, runs one backtest per row, and produces `results.csv`. Each row in `results.csv` contains the full input parameter vector plus the computed output metrics:

```
trend_lookback,consecutive_closes,pullback_pct,maturity_score_cutoff,entry_offset_pct,stop_loss_pct,take_profit_pct,sharpe,cagr,max_drawdown,trade_count,win_rate,total_pnl
25,4,0.05,0.6,0.01,0.05,0.10,1.23,0.15,-0.08,142,0.58,12500.50
10,3,0.03,0.7,0.005,0.03,0.08,0.87,0.09,-0.12,203,0.52,8200.30
...
```

Rows may be missing (if a run failed/timed out). The script must handle this — see analyze subcommand.

## Subcommand 2: `analyze`

```
python sobol.py analyze --problem problem.json --results results.csv --output analysis.json
```

Behavior:

1. Read `problem.json` and `results.csv`
2. Validate that all parameter columns from `problem.json` exist in `results.csv`
3. Validate that all `output_metrics` columns exist in `results.csv`
4. Handle missing rows: if `results.csv` has fewer rows than the expected Saltelli matrix size, print a warning with the count of missing rows. If more than 5% of rows are missing, abort with an error — Sobol analysis becomes unreliable with significant gaps
5. For each metric in `output_metrics`, compute Sobol indices using `sobol.analyze()`
6. Compute group-level indices by summing first-order indices within each category (`signal-structural`, `signal-threshold`, `execution`)
7. For each parameter, compute the interaction share: `(S_total - S_first) / S_total` — the fraction of that parameter's influence coming through interactions
8. Compute cross-group interaction indicators: for each pair of categories, sum the second-order indices of all parameter pairs that cross those category boundaries

Write `analysis.json` with the following structure:
```json
{
  "strategy_name": "BuyTheDip",
  "sample_count": 32768,
  "results_count": 32650,
  "missing_rows": 118,
  "primary_metric": "sharpe",
  "parameters": {
    "trend_lookback": {
      "category": "signal-structural",
      "S1": 0.32,
      "S1_conf": 0.04,
      "ST": 0.41,
      "ST_conf": 0.03,
      "interaction_share": 0.22
    }
  },
  "group_analysis": {
    "signal-structural": {
      "total_S1": 0.45,
      "total_ST": 0.58
    },
    "signal-threshold": {
      "total_S1": 0.30,
      "total_ST": 0.38
    },
    "execution": {
      "total_S1": 0.15,
      "total_ST": 0.25
    }
  },
  "cross_group_interactions": {
    "signal-structural × signal-threshold": 0.08,
    "signal-structural × execution": 0.12,
    "signal-threshold × execution": 0.05
  },
  "additivity": 0.90,
  "all_metrics": {
    "sharpe": { "...same structure as above..." },
    "cagr": { "...same structure..." },
    "max_drawdown": { "...same structure..." }
  },
  "diagnostics": {
    "warnings": [],
    "flags": []
  }
}
```

Diagnostic flags to check and include in `diagnostics.flags`:
- `"execution_dominates"` — if execution category total_S1 > signal categories combined total_S1
- `"high_cross_group_interaction"` — if any cross-group interaction sum > 0.15
- `"low_additivity"` — if sum of all first-order indices < 0.5 (interactions dominate the system)
- `"parameter_at_bound"` — if the best-performing region (top 10% of primary metric) has any parameter's median within 10% of its bound range from either edge

## Subcommand 3: `report`

```
python sobol.py report --problem problem.json --analysis analysis.json --results results.csv --output report_dir/
```

Generate the following files in `report_dir/`:

### 3a. Sobol Index Bar Chart (`sobol_indices.png`)

Horizontal bar chart with one bar per parameter. Each bar shows S1 (filled) and ST (total bar length, with the interaction portion in a lighter shade). Color-code bars by category: blue for signal-structural, green for signal-threshold, orange for execution. Include confidence intervals as error bars. Sort by ST descending.

### 3b. Group-Level Pie Chart (`group_contributions.png`)

Pie chart showing the three category groups' share of total first-order variance. Simple, three slices, same color scheme as 3a.

### 3c. 2D Heatmaps (`heatmap_{param1}_vs_{param2}.png`)

For the top 3 parameters by ST: generate all pairwise 2D heatmaps of the primary metric (sharpe). Bin the parameter ranges into 20×20 grids and compute the mean metric value per bin from the results data. Use a diverging colormap centered on zero (or median). Include the heatmap colorbar.

### 3d. 1D Parameter Sweeps (`sweep_{param}.png`)

For the top 5 parameters by ST: generate 1D sweep plots. Bin the parameter into 30 bins, compute mean and standard deviation of the primary metric per bin. Plot mean as a line with ±1 std as shaded region. Mark the overall best-performing region.

### 3e. Cross-Group Interaction Matrix (`interaction_matrix.png`)

3×3 heatmap of cross-group interaction sums. Rows and columns are the three categories. Diagonal cells show within-group interaction totals. Annotate cells with values.

### 3f. Summary Text Report (`summary.txt`)

Plain text report containing:
- Strategy name and run metadata (sample count, missing rows)
- Top 3 parameters by first-order index with values
- Group-level variance breakdown (one line per group)
- Cross-group interaction summary
- Additivity score
- All diagnostic flags with explanations
- Pass/fail verdict based on flags:
  - PASS: no flags raised
  - REVIEW: `low_additivity` or `parameter_at_bound` flags present
  - FAIL: `execution_dominates` or `high_cross_group_interaction` flags present

## Script Structure

Three separate scripts, one per mode:

```
sobol/
├── generate.py      # Saltelli sample matrix generation
├── analyze.py       # Sobol index computation
├── report.py        # Visualization and summary output
├── common.py        # Shared utilities: load problem.json, validation, constants
└── requirements.txt
```

Each script is independently executable:
```
python sobol/generate.py --problem problem.json --output samples.csv
python sobol/analyze.py --problem problem.json --results results.csv --output analysis.json
python sobol/report.py --problem problem.json --analysis analysis.json --results results.csv --output report_dir/
```

Each script uses argparse for its own arguments. `common.py` contains shared helpers: loading and validating `problem.json`, building the SALib problem dict, column validation against CSVs, and the category constants (`signal-structural`, `signal-threshold`, `execution`).

No classes needed — keep it functional. Each script reads files, processes, writes files.

## Error Handling

- If `problem.json` is malformed or missing required fields, print a clear error message identifying the missing field and exit 1
- If `results.csv` columns don't match `problem.json`, print which columns are missing and exit 1
- If SALib raises convergence warnings, capture them and include in `diagnostics.warnings`
- All file paths should be resolved to absolute paths for clear error messages

## Testing

Include a `--dry-run` flag on the `generate` subcommand that prints the problem dict and expected sample count without writing the CSV. This lets the user verify the problem definition before committing to a large sampling run.

## Notes for Implementation

- Use `SALib.sample.saltelli.sample` for generation
- Use `SALib.analyze.sobol.analyze` for analysis
- For second-order indices, pass `calc_second_order=True` to both sample and analyze
- The integer rounding after Saltelli sampling is a standard practice — document it with a comment in the code
- Matplotlib figures should use a dark background theme consistent with the project's existing plot_market.py (plotly_dark equivalent: use `plt.style.use('dark_background')`)
- All plots should be saved at 150 DPI, sized for readability (minimum 10×8 inches for heatmaps, 12×6 for bar charts)
