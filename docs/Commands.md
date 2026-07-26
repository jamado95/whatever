# Commands

Every command runs from the **repo root** — `cmd/main.go` loads `config.json` relative to the
current directory, and the `uv` project lives at the root.

Go tooling is driven by the `Makefile`. Python tooling is managed by [uv](https://docs.astral.sh/uv/)
from `pyproject.toml` / `uv.lock`; `uv run` provisions the interpreter and syncs `.venv` on demand,
so there is no Python setup step.

⚠️ marks commands that are currently broken — see [Known broken commands](#known-broken-commands).

---

## Core Engine

### Build & run

| Command | Description |
|---|---|
| `make build` | Compile the engine to `./whatever`. |
| `make run [CONFIG=]` | Run the engine from source (`CONFIG` defaults to `config.json`). |
| `make validate [CONFIG=]` | Validate a config and exit without running anything. |
| `./whatever [flags]` | Run a previously built binary. |

Engine type, provider, features, symbol and limits are all set in the config file; the two flags
below only choose *which* file and *whether to run it*. Ctrl-C shuts down cleanly. As checked in,
`make run` executes the `data_logger` engine: `binance_csv` → feature chain → console log + JSONL
export to `data/exports/`.

| Flag | Description |
|---|---|
| `--config <path>` | Config file to load. Default `config.json`. |
| `--validate-only` | Construct every enabled component, report any config errors, then exit without running the engine. |

```bash
# Run against an alternative config
make run CONFIG=configs/btc-1h.json

# Check a config without connecting to a data source or writing exports
make validate
./whatever --config configs/generated-042.json --validate-only
```

### Config validation

`config.json` is validated strictly. Unknown keys, wrong value types and out-of-range enum values
are startup errors naming the offending path, rather than being silently ignored:

```
config.json: feature "ema" variant 0: json: unknown field "smoothingg"
config.json: engine "data_logger": invalid ticker type "fixedinterval", want one of [realtime fixed]
config.json: failed to parse config: invalid timeframe "1hr", want one of [1m 5m 15m 1h 1d]
```

A single run reports every independent problem it finds, not just the first. Components disabled
with `"disabled": true` are not validated. Because no component performs I/O when constructed,
`--validate-only` is a genuine dry run — nothing is fetched and no files are written.

One limitation: if a component the engine depends on fails to build, the engine itself is not
constructed, so errors inside its `options` block only surface once the earlier error is fixed.

### Checks

| Command | Description |
|---|---|
| `make test` | Run the Go test suite — covers `internal/config`, `internal/engine` and the providers; the remaining packages report "no test files". |
| `make fmt` | Format all Go sources with `gofmt -w`. |
| `make tidy` | Prune and sync `go.mod` / `go.sum`. |
| `make lint` | ⚠️ Run `golangci-lint` (v2 config in `.golangci.yml`). |
| `make lint-clean` | ⚠️ Clear the lint cache, then lint. |
| `make check` | ⚠️ Run `fmt` → `tidy` → `test` → `lint` in sequence. |
| `go build ./...` | Compile every package without producing a binary. |
| `go vet ./...` | Static analysis — the working substitute while `golangci-lint` is unavailable. |

### Data acquisition

Feeds the data layer: `binance-klines` produces the CSV archives that the `binance_csv` provider
reads; the market-info commands are for looking up symbols before configuring a run.

| Command | Description |
|---|---|
| `make binance-klines SYMBOL= INTERVAL= START= END= [OUT=]` | Download Binance kline CSVs (`OUT` defaults to `./data`). |
| `make binance-pairs [QUOTE=] [SEARCH=] [JSON=1]` | List available trading pairs. |
| `make binance-ticker SYMBOL= [JSON=1]` | Fetch 24h stats for one symbol. |
| `make binance-tickers [QUOTE=] [JSON=1]` | Fetch 24h stats for all symbols. |

```bash
# Download January 2024 hourly BTCUSDT klines
make binance-klines SYMBOL=BTCUSDT INTERVAL=1h START=2024-01-01 END=2024-01-31 OUT=./data

# Market info
make binance-pairs QUOTE=USDT
make binance-pairs SEARCH=BTC
make binance-ticker SYMBOL=BTCUSDT
make binance-tickers QUOTE=USDT JSON=1
```

---

## Visualisation

The engine writes the JSONL export itself (via the `exporter` configured in `config.json`);
`scripts/plot_market_data.py` renders it to a standalone interactive HTML file, written next to the
input with an `.html` suffix.

### Python environment

| Command | Description |
|---|---|
| `make py-sync` | Materialise `.venv` from `uv.lock` — optional, since `uv run` does it automatically. |
| `make py-lock` | Re-resolve dependencies and update `uv.lock` after editing `pyproject.toml`. |
| `make clean-venv` | Delete `.venv`; it is rebuilt on the next `uv run`. |

### Plotting

| Command | Description |
|---|---|
| `make plot FILE= [START=] [END=] [INDICATORS=]` | Render a JSONL export to an interactive HTML chart. |
| `uv run scripts/plot_market_data.py <file> [flags]` | Same, but exposes `--window` and `--rangeslider`, which the make target does not forward. |
| `make quick-plot` | Plot the most recent export in `data/exports/` with no arguments. |

```bash
# Plot a full export
make plot FILE=data/exports/BTCUSDT_1h_data_logger_1704070799999_1707667199999.jsonl

# Restrict the date range and pick specific indicator columns
make plot FILE=data/exports/BTCUSDT_1h_data_logger_1704070799999_1707667199999.jsonl \
  START=2024-01-01 END=2024-01-31 INDICATORS=ema_20,vwap_20

# Direct invocation, with the initial candle window and range slider
uv run scripts/plot_market_data.py data/exports/BTCUSDT_1h_data_logger_1704070799999_1707667199999.jsonl \
  --window 500 --rangeslider
```

---

## Backtest

**There are no engine-side backtest commands yet.** `internal/engine/backtesting.go` contains a
single comment line, so there is nothing to register, configure or run. The methodology this engine
is meant to implement is in [Backtesting.md](Backtesting.md) and `research/Backtesting.md`; the
design gaps blocking it are tracked in [discussions/engine.md](discussions/engine.md).

The Sobol parameter-sensitivity tooling under `scripts/sobol/` does run today, but it is **not wired
to anything**: step 2 below requires a `results.csv` of per-parameter-set performance metrics that no
component currently produces. Until the backtesting engine exists, that file has to be generated by
hand.

| Command | Description |
|---|---|
| `uv run scripts/sobol/generate.py --problem <json> --output <csv> [--dry-run]` | Generate a Saltelli sample matrix from a parameter definition. |
| `uv run scripts/sobol/analyze.py --problem <json> --results <csv> --output <json>` | Compute Sobol indices from backtest results. |
| `uv run scripts/sobol/report.py --problem <json> --analysis <json> --results <csv> --output <dir>` | Render sensitivity plots and a text verdict. |

```bash
# 1. Check sample count before committing to a run (N * (2D + 2) samples)
uv run scripts/sobol/generate.py --problem problem.json --output samples.csv --dry-run

# 2. Generate the sample matrix
uv run scripts/sobol/generate.py --problem problem.json --output samples.csv

# 3. Run a backtest per sample row and write results.csv — NO COMMAND EXISTS FOR THIS YET

# 4. Compute Sobol indices
uv run scripts/sobol/analyze.py --problem problem.json --results results.csv --output analysis.json

# 5. Generate plots and summary
uv run scripts/sobol/report.py --problem problem.json --analysis analysis.json \
  --results results.csv --output report/
```

See [scripts/sobol/README.md](../scripts/sobol/README.md) for the `problem.json` schema and how to
read the diagnostics.

---

## Known broken commands

| Command | Problem | Workaround |
|---|---|---|
| `make lint`, `make lint-clean`, `make check` | `golangci-lint` is not installed. | Install it, or use `go vet ./...`. |
