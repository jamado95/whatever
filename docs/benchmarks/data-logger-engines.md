# `data_logger` vs `data_logger_v2` — replay benchmark

Run 2026-10-25 on `main` at `6546e3f` (after #2, data layer v2 iteration 1).

## Question

How does `data_logger_v2` (data layer v2: per-stream lanes, readiness, `FrameBuffer`, feature
bridge) compare with `data_logger` (`FeatureChain` per stream → `OrderedFanIn` → `SynchronizerV2`)
on the same replay workload, with and without per-frame logging?

## Setup

| | |
|---|---|
| Machine | Intel Core Ultra 9 275HX, 24 logical cores, 15 GB RAM |
| OS | WSL2, kernel 6.18.33.2-microsoft-standard-WSL2 |
| Go | 1.26.5 linux/amd64 |
| Data | BTCUSDT, all of 2024: 105,408 5m, 8,784 1h and 366 1d bars (`data/BTCUSDT/BTCUSDT-{5m,1h,1d}-2024-01.csv`) |

**Workload**, identical for both engines:

- Three direct subscriptions — BTCUSDT 5m, 1h and 1d, one `binance_csv` provider each. No
  derivation: `data_logger` rejects them.
- Features from `configs/data_logger.json`: `engulfing_candle`, `vwap_20`, `vwap_50`, `ema_20`,
  `ema_50` — lookback 100 candles.
- The `jsonl` exporter; no `ticker` block (unpaced) and no `limit`, so each run covers the full year.

**Method:**

- One binary from `go build ./cmd`, both engines selected by config.
- Five runs per engine, alternating between engines, timed with
  `/usr/bin/time -f "%e %U %S %M"` (wall, user, sys, peak RSS); stdout and stderr to `/dev/null`.
- **Logging off:** there is no switch, so a second binary was built from a throwaway worktree with
  `DefaultLoggerConfig`'s level changed from `debug` to `disabled`. Every production logger is built
  from `DefaultLoggerConfig`; the binary printed 0 bytes.
- **Profiles:** one run per engine with logging off, from the same worktree with
  `pprof.StartCPUProfile` added to `main`.

## Results

Medians of five runs; range is wall time.

| Engine | Logging | Wall | Range | User | Sys | CPU | Peak RSS |
|---|---|---|---|---|---|---|---|
| `data_logger` | on | 3.50 s | 3.49–3.68 | 5.70 s | 1.63 s | 7.33 s | 15.6 MB |
| `data_logger` | off | **2.05 s** | 2.03–2.11 | 3.87 s | 1.17 s | 5.04 s | 15.9 MB |
| `data_logger_v2` | on | 3.06 s | 3.04–3.09 | 3.75 s | 0.64 s | 4.39 s | 16.1 MB |
| `data_logger_v2` | off | 2.13 s | 2.11–2.14 | 2.65 s | 0.57 s | **3.22 s** | 15.5 MB |

| | `data_logger` | `data_logger_v2` |
|---|---|---|
| Frames | 105,121 | 105,408 |
| Records exported | 114,248 | 114,558 |
| Throughput, logging on | ~30k frames/s | ~34k frames/s |
| Throughput, logging off | ~51k frames/s | ~49k frames/s |
| Logging cost (wall) | ~1.45 s | ~0.93 s |

- **Logging on:** v2 is ~13% faster in wall time and uses ~40% less CPU.
- **Logging off:** `data_logger` is ~4% faster in wall time; v2 still uses ~36% less CPU.
- Peak memory is the same, ~16 MB, for both.

<details>
<summary>Raw runs (wall, user, sys in s; peak RSS in KB)</summary>

| Run | Logging | `data_logger` | `data_logger_v2` |
|---|---|---|---|
| 1 | on | 3.68, 5.91, 1.70, 15896 | 3.06, 3.70, 0.66, 16540 |
| 2 | on | 3.50, 5.76, 1.48, 16416 | 3.05, 3.77, 0.58, 16520 |
| 3 | on | 3.49, 5.55, 1.63, 15960 | 3.04, 3.71, 0.64, 16320 |
| 4 | on | 3.56, 5.66, 1.77, 15768 | 3.09, 3.75, 0.66, 16208 |
| 5 | on | 3.49, 5.70, 1.58, 16088 | 3.09, 3.85, 0.59, 17448 |
| 1 | off | 2.04, 3.87, 1.04, 16232 | 2.11, 2.70, 0.50, 16368 |
| 2 | off | 2.11, 3.97, 1.26, 16224 | 2.14, 2.76, 0.51, 15896 |
| 3 | off | 2.05, 3.80, 1.17, 16700 | 2.12, 2.65, 0.57, 15900 |
| 4 | off | 2.03, 3.87, 1.09, 16300 | 2.13, 2.59, 0.65, 15840 |
| 5 | off | 2.08, 3.93, 1.19, 16024 | 2.14, 2.63, 0.63, 16320 |

</details>

## Output equivalence

Exports were compared record by record, keyed by `(timeframe, timestamp)`, in both the logged and
the silent runs:

- **All 114,248 records both engines export are identical**, feature values included.
- v2 exports **310 more** (287 5m and 23 1h), all on 2024-01-01. `data_logger` emits nothing until
  every stream has delivered once, and the 1d stream's first bar closes at the end of that day; v2
  releases those columns as `Partial` frames, from the earliest first candle. This is also the
  287-frame difference above.

## Where the time goes

CPU profiles, one run each, logging off. Seconds are cumulative CPU samples; the totals are
3.57 s (`data_logger`) and 3.74 s (`data_logger_v2`).

| | `data_logger` | `data_logger_v2` |
|---|---|---|
| Ordering and alignment | ~0.36 s — `SynchronizerV2` (`runV2`) and `OrderedFanIn` | **~0.2 s** — normalise, align, readiness, `pipe.FanIn`, `Next`, `View.Candles` |
| Feature evaluation | 0.99 s — `FeatureChain.Process`, one goroutine per stream, in parallel | **1.73 s** — `FeatureChain.Evaluate` on the engine goroutine, 46% of samples |
| JSONL export | 0.63 s | 0.45 s |
| Garbage collection (`gcBgMarkWorker`) | 0.92 s (26%) | 0.78 s (21%) |
| CSV providers | 0.15 s | 0.11 s |

- **v2's cost is the feature bridge.** At every stream update `Evaluate` refills the chain's
  `SortedWindow` from the `View` history: ~100 candles, each pushed under a lock with a binary-search
  insert (`sort.Search` alone is 0.44 s). That is ~11M pushes for the year, against one push per
  candle in `Process`.
- **`data_logger` overlaps its stages.** Three feature goroutines, the synchronizer and the export
  goroutine run at once, so its wall time is well below its CPU time. v2 evaluates features and
  exports serially on the engine goroutine.
- **The data layer itself is the cheaper part:** ~0.2 s of CPU for the year in v2, against ~0.36 s
  for the synchronizer path it replaces.

## Findings

- With logging on, as the engines run by default, v2 is faster and cheaper.
- With logging off, wall time is a near tie and v2 still uses about a third less CPU; its wall time is
  bounded by the serial feature bridge, not by the data layer.
- The bridge is temporary: features move into the `FrameBuffer` in iteration 4 of
  [the data layer v2 plan](../plans/data-layer-v2/main.md). Before that, refilling the window
  without a sorted insert (the `View` history is already ordered) or evaluating each updated stream's
  features in parallel would address most of the gap.

## Caveats

- Three streams only. Data layer v2 targets hundreds to thousands of streams; this run does not
  measure scaling.
- Profiles are single runs, with profiling overhead; read them as proportions, not timings.
- WSL2 on a laptop CPU; nothing else heavy ran alongside, and the five runs per case stayed within a
  narrow range.
- Per-frame logging uses zerolog's pretty console writer even when output goes to `/dev/null`;
  the "on" figures include that formatting.

## Reproduce

From the repo root (the configs' data paths are relative), with the BTCUSDT 2024 files in
`data/BTCUSDT/`:

```bash
B=$(mktemp -d)

# matched configs: the three direct streams of configs/data_logger.json, unpaced, no limit
python3 - "$B" <<'EOF'
import json, sys
B = sys.argv[1]
cfg = json.load(open('configs/data_logger.json'))
cfg['providers'] = [p for p in cfg['providers'] if p['type'] == 'binance_csv']
cfg['strategies'] = []
cfg['engine']['options'] = {}
for engine in ('data_logger', 'data_logger_v2'):
    cfg['engine']['type'] = engine
    cfg['engine']['exporter']['output_dir'] = f'{B}/out-{engine}'
    json.dump(cfg, open(f'{B}/{engine}.json', 'w'), indent=2)
EOF

# binaries: as built, and with logging disabled
go build -o "$B/whatever" ./cmd
git worktree add --detach "$B/wt" HEAD
sed -i 's/Level:      "debug",/Level:      "disabled",/' "$B/wt/internal/logger/logger.go"
(cd "$B/wt" && go build -o "$B/whatever-nolog" ./cmd)
git worktree remove --force "$B/wt"

# five alternating runs per engine and binary
for bin in whatever whatever-nolog; do
  for i in 1 2 3 4 5; do
    for e in data_logger data_logger_v2; do
      /usr/bin/time -a -o "$B/times-$bin.txt" -f "$e %e %U %S %M" \
        "$B/$bin" --config "$B/$e.json" >/dev/null 2>&1
    done
  done
done
cat "$B"/times-*.txt
```
