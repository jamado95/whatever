# CSV data quality check

## Context

The `data/` folder holds 104 Binance kline CSVs that feed the `binance_csv` provider. Nothing checks them for missing candles or missing values today; `docs/discussions/data-layer.md` lists gap and integrity handling as open robustness work. This adds a Python CLI that reads one CSV and prints a completeness and continuity report to the terminal. It is wired into the Makefile and `docs/Commands.md` the same way `plot_market_data.py` is.

What the data looks like:
- Headerless, 12 columns: `open_time,open,high,low,close,volume,close_time,quote_volume,trades,taker_buy_base,taker_buy_quote,ignore` (see `parseCsvRecordToCandle` in `internal/domains/provider/binance_csv.go`).
- Filename `{SYMBOL}-{INTERVAL}-{YYYY-MM}.csv`, but one file can span many months (e.g. `SOLUSDT-1h-2024-01.csv` is all of 2024).
- The 16 files from 2025 use **µs** timestamps; the older ones use ms. Intervals present: 1m, 5m, 1h, 1d.

Decisions (agreed with user): the script goes at `scripts/csv_quality.py`; no automated tests; continuity is measured from the first row to the last; the µs finding is recorded in `data-layer.md`. Plan structure: a single doc.

## Changes

### 1. `scripts/csv_quality.py` (new, stdlib only)

Header docstring and `argparse`/`main()` returning an exit code, both as in `scripts/plot_market_data.py`. No new dependencies, so `pyproject.toml` and `uv.lock` don't change.

CLI: `csv_quality.py <csv_path> [--interval 1h]`

Functions:
- `parse_args()`
- `interval_ms(token)`: turns `30s`/`1m`/`5m`/`1h`/`1d`/`1w` into ms. `1M` (calendar month) is not supported, so continuity is skipped with a note.
- `detect_interval(path, timestamps)`: the `--interval` value if given, else the token parsed from the filename (`-(\d+[smhdwM])-`), else the most common positive gap between timestamps. The report says which source it used.
- `normalise_ts(value)`: values ≥ 1e14 count as µs and are divided by 1000. This is done per row, so a file that crosses into 2025 still works. The report lists the units it saw.
- `load_rows(path)`: `csv.reader`. If the first row's `open_time` isn't an integer, treat the row as a header and skip it (Binance's newer archives can include one, per data-layer.md gap #6). Blank lines are ignored.
- `check_completeness(rows)`: for each of the 12 columns, a cell counts as missing if it is absent (short row), empty or whitespace, or doesn't parse as a number (including `nan`). Returns:
  - missing count and % per column
  - rows with any missing cell (count and %)
  - total missing cells (count and %)
  - rows whose column count isn't 12 (count and %)
- `check_continuity(timestamps, step)`: uses the parsed `open_time`s; rows where it can't be parsed are left out here and already counted under completeness.
  - `expected = (last − first) / step + 1`
  - `present` = unique timestamps aligned to the step, i.e. `(t − first) % step == 0`
  - `missing = expected − present`
  - **continuity score = present / expected × 100**
  - Also reported: number of gaps, largest gap, duplicates (`count − unique`), out-of-order rows (in file order) and misaligned timestamps. These are listed alongside the score but don't change it.
  - Gaps: consecutive unique aligned timestamps `a, b` with `b − a > step` form one gap of `(b − a)/step − 1` missing candles, running from `a+step` to `b−step`.
  - Fewer than two timestamps: score N/A.
- `print_report(...)`: plain-text sections, each with absolute counts and percentages. The header always prints the **start and end dates of the analysed period**: the first and last `open_time` in the file, in UTC, plus the span between them. These are the same bounds `expected` is computed from. If no `open_time` parses, both are printed as `N/A`.

```
CSV data quality: data/BTCUSDT/BTCUSDT-1h-2024-01.csv
  Interval   1h (filename)
  Start      2024-01-01 00:00:00 UTC
  End        2024-12-31 23:00:00 UTC
  Span       365d 23h
  Rows       8784   header: none   timestamp unit: ms

Completeness
  Rows with missing data   0 / 8784     0.00%
  Missing cells            0 / 105408   0.00%
  Malformed rows (≠12 col) 0 / 8784     0.00%
  column            missing        %
  open_time               0    0.00%
  ...

Continuity
  Expected candles   8784
  Present            8784
  Missing            0          0.00%
  Gaps               0          largest: —
  Duplicates         0
  Out of order       0
  Misaligned         0
  Continuity score   100.00%

  Largest gaps (top 10)
  2024-03-01 10:00 → 2024-03-01 12:00   3 candles
```

Exit code: 0 once a report is printed, whatever the quality; 1 if the file is missing, unreadable or has no data rows.

### 2. `Makefile`

Add a `csv-quality` target in the Python section, using the same pattern as `plot`: check that `FILE` is set and exists, then `$(PYTHON) scripts/csv_quality.py $(FILE)`, with `--interval $(INTERVAL)` added only when `INTERVAL` is set. Add it to `.PHONY` and put a usage comment above it.

### 3. `docs/Commands.md`

Add a `## Data quality` section between Core Engine and Visualisation. It gets a table (`make csv-quality FILE= [INTERVAL=]` and `uv run scripts/csv_quality.py <file> [--interval]`), a short explanation of how missing data and the continuity score are defined, and examples.

### 4. `docs/discussions/data-layer.md`

Add item 7 under "Known gaps and defects": Binance spot archives from 2025-01 onwards use µs `OpenTime`/`CloseTime` (e.g. `data/BTCUSDC/BTCUSDC-5m-2025-01.csv`). `parseCsvRecordToCandle` passes them through unscaled, so for those files `Candle` timestamps are 1000× larger than the ms contract. Not handled yet. No engine code changes.

### 5. Store the plan

After it has been executed, save this plan as `docs/plans/csv-data-quality.md`. There's no discussion doc for this topic, so nothing links to it.

`docs/Architecture.md` and `cmd/scripts/README.md` stay as they are: `/scripts` is already listed as the place for helpers, and the README only covers `cmd/scripts`.

## Verification

- `make csv-quality FILE=data/SOLUSDT-1h-2024-01.csv`: expect 8784 rows, 0 missing, 100.00% (already checked with awk: no gaps), Start 2024-01-01 00:00:00 UTC and End 2024-12-31 23:00:00 UTC.
- `make csv-quality FILE=data/BTCUSDC/BTCUSDC-5m-2025-01.csv`: a µs file; expect timestamp unit µs, a 2025 date range and a sensible score.
- `make csv-quality FILE=data/BTCUSDC/BTCUSDC-1d-2018-01.csv`: a short file; expect Start 2018-12-15 00:00:00 UTC and End 2018-12-31 00:00:00 UTC, even though the filename says 2018-01.
- Run the script once over all 104 files (`find data -name '*.csv' -exec uv run scripts/csv_quality.py {} \;`) to make sure nothing crashes and to see if any real gaps turn up.
- Edit copies in the scratchpad (delete rows, blank cells, duplicate a row, add a header row, cut a row short) and check the counts and score come out right.
- `make csv-quality` with no `FILE` and with a missing file: expect the usage or error message and a non-zero exit.
- Commit message: one line, no description, no co-author trailer (as CLAUDE.md requires).

## Implementation notes

- `load_rows` + `check_completeness` were merged into a single-pass `scan_file`, which keeps only the open_times in memory instead of every row (1m archives hold ~525k rows).
- First run over `data/`: no missing cells in any file, and 36 files score below 100%. 2019–2023 files have exchange-downtime gaps (99.67–99.99%); `BTCUSDT`/`ETHUSDT` 2018 1h/5m files also have off-grid candles after the February 2018 outage (98.81–99.02%).
