# Sobol Sensitivity Analysis Tool

Parameter sensitivity analysis for trading strategies using Sobol indices. Identifies which parameters most influence strategy performance and detects problematic interaction patterns.

## Installation

Dependencies are managed by [uv](https://docs.astral.sh/uv/) from the repo root
`pyproject.toml` (`sobol` dependency group). There is no install step — `uv run`
provisions the interpreter and syncs the environment on first use.

Run every command below from the **repo root**, not from this directory.

## Quick Start

```bash
# 1. Generate sample matrix
uv run scripts/sobol/generate.py --problem problem.json --output samples.csv

# 2. Run your strategy on each sample row, output results.csv with metric columns

# 3. Compute Sobol indices
uv run scripts/sobol/analyze.py --problem problem.json --results results.csv --output analysis.json

# 4. Generate visualizations and summary
uv run scripts/sobol/report.py --problem problem.json --analysis analysis.json --results results.csv --output report/
```

## Problem Definition

Create a `problem.json` file defining parameters and metrics:

```json
{
  "parameters": [
    {
      "name": "lookback_window",
      "bounds": [10, 100],
      "type": "int",
      "category": "signal-structural"
    },
    {
      "name": "entry_threshold",
      "bounds": [0.01, 0.1],
      "type": "float",
      "category": "signal-threshold"
    },
    {
      "name": "stop_loss_pct",
      "bounds": [0.005, 0.05],
      "type": "float",
      "category": "execution"
    }
  ],
  "metrics": ["sharpe_ratio", "max_drawdown", "win_rate"]
}
```

### Parameter Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Parameter identifier (used in CSV columns) |
| `bounds` | Yes | `[min, max]` range for sampling |
| `type` | No | `"float"` (default) or `"int"` |
| `category` | No | `"signal-structural"`, `"signal-threshold"`, or `"execution"` |

### Categories

- **signal-structural**: Core signal logic (lookback windows, MA periods)
- **signal-threshold**: Entry/exit thresholds, filter levels
- **execution**: Position sizing, stop-loss, take-profit

## Scripts

### generate.py

Generates Saltelli sample matrix for Sobol analysis.

```bash
uv run scripts/sobol/generate.py --problem problem.json --output samples.csv [--dry-run]
```

| Argument | Description |
|----------|-------------|
| `--problem` | Path to problem definition JSON |
| `--output` | Path to output CSV file |
| `--dry-run` | Print summary without writing files |

**Output:** CSV with one row per sample, columns for each parameter.

Sample count formula: `N * (2D + 2)` where N=1024 and D=number of parameters.

### analyze.py

Computes Sobol sensitivity indices from simulation results.

```bash
uv run scripts/sobol/analyze.py --problem problem.json --results results.csv --output analysis.json
```

| Argument | Description |
|----------|-------------|
| `--problem` | Path to problem definition JSON |
| `--results` | Path to results CSV (samples + metrics) |
| `--output` | Path to output analysis JSON |

**Output:** JSON containing:
- `sobol_indices`: S1 (first-order), ST (total), S2 (second-order) per metric
- `group_indices`: Category-level S1 sums
- `interaction_shares`: `(ST - S1) / ST` per parameter
- `cross_group_interactions`: S2 sums between categories
- `diagnostics`: Warning flags

### report.py

Generates visualizations and text summary.

```bash
uv run scripts/sobol/report.py --problem problem.json --analysis analysis.json --results results.csv --output report/
```

| Argument | Description |
|----------|-------------|
| `--problem` | Path to problem definition JSON |
| `--analysis` | Path to analysis JSON from analyze.py |
| `--results` | Path to results CSV |
| `--output` | Output directory for plots and summary |

**Output files:**
- `sobol_indices.png` - Horizontal bar chart (S1 filled, ST outline)
- `group_contributions.png` - Pie chart of category contributions
- `heatmap_{p1}_vs_{p2}.png` - 2D heatmaps for top 3 parameters
- `sweep_{param}.png` - 1D sweeps for top 5 parameters
- `interaction_matrix.png` - Cross-group interaction heatmap
- `summary.txt` - Text report with verdict

## Interpreting Results

### Sobol Indices

- **S1 (First-order)**: Direct effect of parameter alone
- **ST (Total)**: Effect including all interactions
- **S2 (Second-order)**: Pairwise interaction effects

If `ST >> S1`, the parameter has strong interactions with others.

### Diagnostic Flags

| Flag | Meaning | Action |
|------|---------|--------|
| `execution_dominates` | Execution params >50% of S1 | Signal may be weak; review entry logic |
| `high_cross_group_interaction` | Cross-group S2 >0.3 | Parameters are coupled; tune together |
| `low_additivity` | Sum of ST >1.5 | Strong interactions; grid search may miss optima |
| `parameter_at_bound` | Optimal values at bounds | Expand parameter range |

### Verdict

- **PASS**: No flags triggered
- **REVIEW**: 1-2 flags triggered
- **FAIL**: 3+ flags triggered

## Example Workflow

```bash
# Check sample count before generating
uv run scripts/sobol/generate.py --problem problem.json --output samples.csv --dry-run

# Generate samples
uv run scripts/sobol/generate.py --problem problem.json --output samples.csv

# Run backtests (example using parallel)
cat samples.csv | parallel --header : --colsep ',' \
  './backtest --window {lookback_window} --threshold {entry_threshold}' \
  > results.csv

# Analyze
uv run scripts/sobol/analyze.py --problem problem.json --results results.csv --output analysis.json

# Generate report
uv run scripts/sobol/report.py --problem problem.json --analysis analysis.json \
  --results results.csv --output report/

# View summary
cat report/summary.txt
```

## Notes

- Results CSV must contain all parameter columns plus metric columns
- Missing rows: warning if <5%, error if >=5%
- NaN metric values are replaced with column mean
- First metric in problem.json is used as primary for rankings/plots
