# Stage 4 — ported features (features layer v2, iteration 1)

## Context

[main.md](main.md) stage 4. Stage 3 built the contract (`internal/protocol/feature`), `reg.FeatureTypes`
and the engine's columns, graph and view, tested with test-only features; nothing registers in
`reg.FeatureTypes` yet.

This stage ports `sma`, `ema`, `rsi`, `vwap` and `engulfing_candle` onto the contract, in a new
package `internal/domains/featuresv2` (`featv2`) beside `internal/domains/features` (`feat`). Each is
registered under the original's type name, with its maths copied unchanged. Nodes read their stream's
candles from one copy per stream owned by the feature engine: filled once per frame at the stream's
longest lookback, each node taking the prefix it needs, rather than each node copying what it reads
into its own scratch. The ring walk that fills it steps slot indices instead of dividing per slot. The
ports are then checked against the originals at every column where the bridge evaluates:

- a table of every registered type, checked against the engine's switch;
- a comparison over the BTCUSDT reference dataset, on a random four-month sample per run and
  skipped without the dataset: the same sampling and behaviour as the data layer's reference test;
- a check that a lookback beyond the data layer's cap fails `data.New`.

```
featv2 init   reg.FeatureTypes.Register("sma", newSMA) … five types
factory       config.Decode ─▶ feature.Feature{ID, Lookback, Bind: Bind[float64] | Bind[int]}
pin           each stream updated at the frame: history[s] = view.Data(s, depth[s], history[s])
node          in.Last(n) ─▶ the original's maths ─▶ (value, true) | (0, false)
checked       newGraph ─▶ New ─▶ pin ─▶ fire ─▶ ViewOf  ==  FeatureChain.Evaluate(sub, view.Data(s, L))
```

Outside the new package and the tests: the data layer's reference sampling moves into a helper
package; `frame`'s ring walk steps slot indices; the contract's `Input` gains `Last`, and the feature
engine sizes, allocates and fills the copies. `cmd` does not import `featv2` until stage 5.

The plan, with the decisions it records in [main.md](main.md) and
[features.md](../../discussions/features.md), is committed before step 1. It was amended twice after
step 1: first to iterate the ring in place (`85792d5`), then, after benchmarks, to read from one copy
per stream. The second amendment supersedes the first; with the decisions it records in main.md,
features.md and [data-layer-v2.md](../../discussions/data-layer-v2.md), it is committed before step 2.
The stage is implemented in five steps, in order. Each step is a review checkpoint: its changes stay
unstaged for review, are committed once approved, and the next step starts from that commit. Every
step leaves the tree building, tested and `make check` clean.

| Step | Adds | Commit |
|---|---|---|
| 1 | The reference sample shared | `refactor(data): share the reference sample` |
| 2 | The ring walk stepping slot indices | `perf(frame): step slot indices in the ring walk` |
| 3 | One candle copy per stream | `feat(features): read candles from one engine copy per stream` |
| 4 | The five ported features | `feat(features): port the five features to the feature contract` |
| 5 | Checks against the originals; the stage's documentation | `test(features): check the ported features against the originals` |

## Decisions

- **`internal/domains/featuresv2`, package `featv2`.** Named after `internal/domains/features`
  (`feat`). It is renamed into `internal/domains/features` when `data_logger` and the bridge retire.
  Resolves main.md's open item.
- **Helpers are copied, not shared.** A domain imports only `protocol` ([engine.md](../../discussions/engine.md)),
  so `featv2` does not import `feat`. It copies `PeriodConfig`, `EMAConfig` with
  `DefaultEMASmoothing`, `EngulfingConfig`, `DirectionalMarker` and its constants, the base-ID
  constants, `iDWithPeriod`, `iDWithParam` and `roundDecimals`. The copies are removed when the
  originals retire.
- **Same config schemas.** The factories decode the originals' config structs strictly, so one config
  entry builds in both registries (stage 5).
- **Candles are read from one engine copy per stream.** The feature engine owns a `[]proto.Candle`
  per stream, as deep as the longest `Lookback` among the instances reading that stream's candles.
  Each frame, before any node fires, it refills the copy of every stream updated at the frame
  (`Fresh`, age 0) with `view.Data(s, depth, copy)`: one ring walk per stream. A node's `in.Last(n)`
  is the copy's first `min(n, len)` candles, newest first: a sub-slice, with no copy and no
  allocation. Replaces per-node scratch ([features.md](../../discussions/features.md) § Reversed on
  2026-11-08).
- **Why a copy per stream.** Benchmarked over rings of 1000 `Fresh` candles, 10 to 1000 streams, every
  stream read once per frame with the reference variants (ten reads, 419 candles in all, the longest
  100): about 0.8 µs per stream, against 1.9 µs iterating the ring in place per node and 2.5–3.1 µs
  with per-node scratch. At ten times the depth: 8–11 µs, against 17–20 and 25–31. The copy walks the
  ring once and every node then loops over a contiguous slice; iterating walks the ring per node.
  With one feature per stream, iterating in place is fastest, but configs run many per stream.
  Memory: a copy holds its stream's longest lookback, 5.6 KB at 100 candles, whatever the number of
  variants; per-node scratch held the sum of the reads.
- **Depth per stream.** The instances reading candles are those without dependencies: a feature with
  `Deps` reads none (stage 3). A single-stream instance counts for its stream, a pair instance for
  both ends. A stream no instance reads has no copy. `newGraph` computes the depths and allocates
  nothing, so `--validate-only` pays nothing; `New` allocates.
- **Filled where nodes fire.** Only streams updated at the frame are refilled, and those are the
  only ones whose nodes fire: a node runs only when every input is `Fresh`, a pair only when both ends
  are (stage 6). Elsewhere a copy keeps its last frame's candles, and nothing reads it. `pin` fills
  before any node fires, so nodes fired in parallel (iteration 2) only read the copies.
- **Shared, and read-only by contract.** Every node on a stream reads one backing array. A node never
  writes the slice and never keeps it past its call; Go cannot enforce either. A port writing into it
  would make the reference check diverge.
- **Dependency reads are unchanged.** `Dep` and `PairDep` inputs have no copy: `Last` returns nil on
  them, and they read through `Data(n, dst)` into scratch. A copy of a dependency's results would be
  filled between firings, after the dependency and before its dependents, which is scheduling
  (stage 6). It waits for the first feature reading a dependency (iteration 3).
- **The ring walk steps slot indices.** `Ring.Data` computes its latest slot's index once and steps
  it back `Window` slots per bar, wrapping, instead of `At`'s division and modulo per slot, and loads
  `first` once. A slot holding another column still stops the walk. Its results are unchanged. A
  100-candle read measured about 7% faster (730 → 683 ns), and that walk now fills each stream's copy.
- **Maths copied unchanged, including the iteration order.** A node runs the original's loops over
  `[]proto.Candle` in the same order, so floating-point sums match bit for bit. Rounding to 6
  decimals stays, as does `rsi`'s unrounded 100.
- **Reads equal the bridge's.** A stream's copy is `view.Data(s, depth)`, and its depth is at least
  any reader's n, so `Last(n)` is `Data(s, n)`: the prefix of the bridge's `view.Data(s, lookback)`
  that `SortedWindow.Last(n)` takes. Close times strictly increase, so the window's sort changes
  nothing. Values compare with `==`.
- **One read shape.** Every port reads its last `n` candles and needs at least `min` of them. A
  package helper `overCandles[T](n, min, f)` returns the `feature.Bind[T]`: it takes `Candles`, and its
  node returns false when `in.Last(n)` holds fewer than `min` candles, otherwise `f(candles)`. Nodes
  hold no scratch and allocate nothing per call, as stage 6's `Step` requires.

  | Type | Lookback | `n` | `min` | `f` returns false | Value |
  |---|---|---|---|---|---|
  | `sma` | p | p | p | — | `float64` |
  | `ema` | 2p | 2p | p | — | `float64` |
  | `rsi` | p+1 | p+1 | p+1 | — | `float64` |
  | `vwap` | p | p | p | zero total volume | `float64` |
  | `engulfing_candle` | 2 | 2 | 2 | — | `int` |

- **Absent is false.** Wherever the original writes nothing (warm-up, and `vwap`'s zero volume), the
  node returns false and the engine writes `Gap`.
- **`DirectionalMarker` stays an alias of `int`,** so `engulfing_candle`'s `Bind[int]` reaches the
  engine's `int` case. A defined type would need its own case, importing `featv2`.
- **IDs format floats with `'f'`.** `iDWithParam` uses `strconv.FormatFloat(v, 'f', -1, 64)`. This is
  identical to the originals' `'g'` for values in [1e-4, 1e6), such as `ema_20`, `ema_20_s3` and
  `ema_20_s2.5`. Outside that range there is no exponent: `ema_20_s0.00001`, not `ema_20_s1e-05`. No
  positive parameter emits `-`. A negative one still would, but none of the five accepts one, and
  `Expand` rejects such an ID (stage 5). The originals keep `'g'`.
- **Checks live in `internal/engine/features`.** Comparing against the originals needs both feature
  packages, and the reference check needs the data layer; a domain may import neither. The engine's
  tests may import domains, which never import the engine back.
- **The registered-types table sees only the packages linked into its test binary.** It blank-imports
  `featv2`. A later package that registers feature types (iteration 3) must be added there too, or its
  types go into `featv2`.
- **The reference test behaves like the data layer's.** The selection in `TestDataPipelineReference`
  moves unchanged into `internal/engine/data/datatest`: a four-month window starting on a random day
  from 2024-01-01 to 2024-09-01, logged, with each timeframe's rows written to the test's temp dir.
  Both reference tests use it, so each run samples afresh, and both are skipped without the files.
  There is no always-on stand-in, so a checkout without the dataset checks the ports' IDs, config and
  allocations, not their values.
- **The reference variants are fixed.** They are `configs/data_logger_v2.json`'s variants (`sma` 20,
  50 and 100, `ema` 20 and 50, `rsi` 14 and 21, `vwap` 20 and 50, `engulfing_candle`), plus period 3
  of each period type and `ema_20_s3`. The longest lookback is 100.
- **The history cap stays internal to the data layer,** which tests it. A variant beyond the cap
  fails the reference test through `data.New`'s error, and is meant to. `TestPortedLookbackCap`
  checks that a ported feature's `Lookback` reaches the cap: on a direct 1d stream, `sma_365` is
  accepted and `sma_366` is rejected. The 365 is the cap's current value, written into the test, so
  changing the cap means changing the test.
- **Runtime.** At a lookback of 100, one bridge `Evaluate` with the reference variants costs about
  12 µs, or 92 µs under `-race` (measured on a copy of `SortedWindow`). About 108k stream updates in
  four months add roughly 1.3 s, or 10 s under `-race`, on top of the data layer's run.

## Step 1 — the reference sample

Committed: `23a5c5a`.

### Changes — new `internal/engine/data/datatest/datatest.go`

```go
// Package datatest samples the BTCUSDT reference dataset for reference tests.
package datatest

// Sample is a window of the BTCUSDT reference dataset, all of 2024, written per timeframe to a temp dir.
type Sample struct {
	Start, End time.Time
	files      map[proto.Timeframe]string
}

/*
Reference samples data/BTCUSDT/BTCUSDT-<tf>-2024-01.csv for each of tfs, found from the module root:
four months starting on a random day from 2024-01-01 to 2024-09-01, logged. It skips t when a file is
missing.
*/
func Reference(t testing.TB, tfs ...proto.Timeframe) *Sample

// Provider is a binance_csv provider over sub's timeframe in the sample, initialised for sub.
func (s *Sample) Provider(t testing.TB, sub proto.Subscription) proto.DataProvider
```

- `sampleCSV` moves here unchanged from `internal/engine/data/data_test.go`. The window is chosen as
  today (`rand.IntN(245)` days, then 4 months).
- The dataset is found by walking up from the working directory to `go.mod`, not via `../../../data`,
  so a test at any depth can use it.

### Changes — `internal/engine/data/data_test.go`

`TestDataPipelineReference` builds its providers with `datatest.Reference` and `Sample.Provider`. Its
comparison, and its count of hours and days from `Start` and `End`, are unchanged. `sampleCSV` is
removed.

No behaviour changes, so no new tests. `datatest` has no tests of its own: both reference tests
exercise it.

### Acceptance

- `go test -race -count=1 -run Reference -v ./internal/engine/data/` passes with the dataset and skips
  without it; `make check` clean.

## Step 2 — the ring walk stepping slot indices

### Changes — `internal/protocol/frame/frame.go`

`Ring.Data` keeps its signature and its results:

```go
/*
Data appends up to n Fresh values to dst[:0], newest first, one bar (window slots) per step back
from the latest update at or before col. Gap and Ended slots are skipped. The walk covers at most the
ring's bars and stops below the first column or at a slot holding another column: a bar overwritten.
It steps the slot index rather than computing it per slot.
*/
func (r *Ring[T]) Data(col int64, n int, dst []T) []T
```

- The latest slot's index is `(col / period) % len`, computed once; each bar back subtracts `window`,
  adding `len` when negative. `first` is loaded once. Each slot's column is compared with the bar's,
  as `At` does.

### Tests — `internal/protocol/frame/frame_test.go`

- `TestRingData`'s cases stay, plus a rolling ring and a coarse ring read across the slot index wrap.
- `TestRingAllocations` stays: reads into a slice with capacity allocate nothing.

### Acceptance

- `go test -race ./internal/protocol/... ./internal/engine/...` passes; with the dataset,
  `go test -race -count=1 -run Reference -v ./internal/engine/data/` passes; `make check` clean.

## Step 3 — one candle copy per stream

### Changes — `internal/protocol/feature/feature.go`

```go
/*
NewBinder places a feature at at. candles and each dependency's view are re-pinned by the engine each
frame; history holds one candle copy per stream, refilled by the engine at each frame its stream
updates; deps maps a dependency type to a *frame.View[T] of its value type.
*/
func NewBinder(at [2]int, candles *frame.View[proto.Candle], history [][]proto.Candle,
	deps map[string]any) *Binder

/*
Last returns up to n Fresh values, newest first: the first n of the engine's copy of a candle input,
filled at the frame. It is valid during the node's call and never written. A dependency input has no
copy: nil.
*/
func (in Input[T]) Last(n int) []T
```

- `Input` gains the address of its stream's copy, which `Candles` and `PairCandles` set; `Dep` and
  `PairDep` leave it nil.
- `Data(n, dst)` stays: dependency inputs read through it.

### Changes — `internal/engine/features`

- `graph.go`: `Graph` gains `history`, one `[]proto.Candle` per stream, nil until `New`, and the
  depth of each stream: the longest `Lookback` among the instances without dependencies on it, a
  pair instance counting for both ends. `newGraph` computes the depths and passes `history` to
  `NewBinder`.
- `engine.go`: `New` allocates each stream's copy with its depth as capacity, for streams with a
  depth. `pin` refills the copy of every stream updated at the frame (`Latest` is `Fresh`, age 0) with
  `v.Data(s, depth, copy)`, before any node fires.
- `features_test.go`: the test feature `sum` reads `in.Last(n)` and holds no scratch; `diff`, a
  dependent, keeps `Data`.

### Tests

- `internal/protocol/feature/feature_test.go`: `Last` returns at most n candles of its stream's copy,
  following the copy as the engine refills it; on a pair, each end its own stream's; nil on a
  dependency input; reading allocates nothing.
- `internal/engine/features`:
  - depths: the longest candle-reading `Lookback` per stream, a pair counting for both ends, a
    dependent not counting; no copy for a stream nothing reads;
  - `pin` refills only the streams updated at the frame, and a node's `Last(n)` equals
    `Data(s, n)` there, on direct and rolling streams;
  - `TestEngineAllocations` stays at zero with `pin` filling the copies.

### Acceptance

- `go test -race ./internal/protocol/... ./internal/engine/...` passes; `make check` clean.

## Step 4 — the five ported features

### Changes — new `internal/domains/featuresv2`

- `featv2.go`: package doc; `DirectionalMarker` and its constants; `PeriodConfig`; `iDWithPeriod`;
  `iDWithParam` with `'f'`; `roundDecimals`; `overCandles[T]`.
- `sma.go`, `ema.go`, `rsi.go`, `vwap.go` and `engulfing.go`, one per type, each with:
  - its base-ID constant, and its config struct where it has its own (`EMAConfig`,
    `EngulfingConfig`);
  - a factory registered in `init` with `reg.FeatureTypes.Register`;
  - its maths as a function over `[]proto.Candle`, newest first.

For example:

```go
func newSMA(opts map[string]any) (feature.Feature, error) {
	cfg := PeriodConfig{}
	if err := config.Decode(opts, &cfg); err != nil {
		return feature.Feature{}, err
	}
	p := cfg.Period
	return feature.Feature{
		ID:       iDWithPeriod(SMAID, p),
		Lookback: p,
		Bind: overCandles(p, p, func(candles []proto.Candle) (float64, bool) {
			return sma(candles, p), true
		}),
	}, nil
}
```

### Tests — new `internal/domains/featuresv2/featv2_test.go`

Features are built through `reg.FeatureTypes.Create`, as the originals' tests do.

- **`TestFeatures`**: a table of type, options, ID and lookback covering `sma_20`, `ema_20` (smoothing
  omitted and 2), `ema_20_s3`, `ema_20_s2.5`, `ema_20_s0.00001`, `rsi_14`, `vwap_50` and
  `engulfing_candle`. Every row is checked for `Pair` false, no `Deps`, and `Bind` of its value type;
  no ID contains `-`.
- **`TestConfigErrors`**: a missing, zero or negative `period` for each period type; a zero or
  negative `smoothing`; an unknown key; and any key on `engulfing_candle`.
- **`TestNodeAllocations`**: every type's node, bound with `feature.NewBinder` over a candle buffer
  pinned with `frame.NewView` and a copy filled from it, returns a value and allocates nothing per
  call.

### Acceptance

- `go test -race ./internal/domains/featuresv2/` passes; `make check` clean.

## Step 5 — checks against the originals

### Changes — new `internal/engine/features/ported_test.go`

It blank-imports `featv2` and imports `feat`, `internal/engine/data`, `datatest` and `provider`. Its
comparison
helper takes a pinned engine, whose `pin` has filled the copies, and a `FeatureChain` of the
originals. At each stream updated at the frame's column (`Fresh`, age 0) it:

1. fires every port;
2. evaluates the chain like the bridge, `Evaluate(sub, view.Data(s, chain.Lookback(), history))`;
3. checks that `ViewOf` reads `Fresh` with the original's value where the snapshot holds the ID, and
   `Gap` where it does not.

It reads through `ViewOf[int]` for a `Bind[int]` port and `ViewOf[float64]` otherwise. It counts the
values and absences each variant was compared on.

- **`TestFeatureTypes`**: a table of options per type. The types in `reg.FeatureTypes` and in the
  table are the same set. Each type's variant reaches `newColumn`'s switch and binds at a stream.
- **`TestPortedReference`**: `datatest.Reference` with 5m, 1h and 1d. It runs BTCUSDT 5m deriving 1h
  and 1d, beside direct 1h and 1d, through `data.New` with the chain's lookback, over the fixed
  reference variants. Every variant is compared on at least one value and one absence on every
  stream. Skipped without the dataset.
- **`TestPortedLookbackCap`** (no dataset needed): `data.New` over a direct BTCUSDT 1d stream, with a
  `binance_csv` provider created but not initialised, since `New` does no I/O. `sma_365`'s `Lookback`
  is accepted; `sma_366`'s is rejected with an error naming the stream. The 365 is written in the
  test and changes with the data layer's cap.

### Documentation

- [features.md](../../discussions/features.md):
  - WIP: the five types ported in `internal/domains/featuresv2`, registered in `reg.FeatureTypes`
    and checked against the originals; nothing expands them yet.
  - Solution: references this stage.
  - § Feature layer pieces: feature types in `internal/domains/featuresv2` until the originals retire.
- [main.md](main.md): stage 4 marked implemented.
- `docs/Architecture.md`: `internal/domains` gains `featuresv2`, registering in `FeatureTypes`;
  `internal/engine/data` gains `datatest`; the feature engine owns one candle copy per stream.
- `docs/Commands.md`:
  - the `make test` row lists `internal/domains/featuresv2`;
  - a new row, `go test -count=1 -run Reference -v ./internal/engine/features/`, compares the ported
    features with the originals over a random four-month sample of the reference dataset, logged,
    and is skipped without the files.

### Acceptance

- `go test -race ./internal/engine/features/ ./internal/domains/featuresv2/` passes; with the dataset,
  `go test -count=1 -run Reference -v ./internal/engine/features/` passes; `make check` clean.
- Stage acceptance ([main.md](main.md)): each port produces the original's value at every column
  where the bridge evaluates, on direct and rolling streams, warm-up absence included; IDs match the
  originals' (`ema_20_s3`); every registered type is in the table and has a case in the switch; nodes
  hold no candle scratch and allocate nothing per call.

## Documentation recorded at planning

Committed with this plan, before step 1:

- [main.md](main.md):
  - stage 4's touches and acceptance: `featuresv2`, the shared sample, the reference check on a
    four-month sample, and the cap check;
  - the open item on the ported features' package, resolved;
  - the open item on filling the exported record: export only results `Fresh` at the record's
    column, since a `Gap` carries the last value (stage 7).
- [features.md](../../discussions/features.md) § Open Questions, *Parameters in IDs*: the ported
  `iDWithParam` formats with `'f'`; negative values are left to `Expand` (stage 5).

Committed with the second amendment, before step 2, superseding the first (`85792d5`):

- [main.md](main.md): stage 4's touches and acceptance (the walk, the copies, `Last`); stage 6's
  memory estimate counting the copies.
- [features.md](../../discussions/features.md):
  - § Feature graph, *A node is the function `Bind` returns*: nodes read candles through `Last`;
  - § Feature buffer: one candle copy per stream; the memory warning counting the copies;
  - § Feature layer pieces: the engine allocating the copies;
  - § Contract sketch: scratch for dependency reads and working memory;
  - § Reversed on 2026-11-08: per-node scratch replaced by a copy per stream, in place of the
    iterator `85792d5` recorded.
- [data-layer-v2.md](../../discussions/data-layer-v2.md): `85792d5`'s iterator reverted
  (`View.Data`'s slice signature and *Reads copy into a caller-supplied slice* restored, its
  § Reversed on 2026-11-08 removed); the walk stepping slot indices noted.

## Out of scope

- `cmd`'s import of `featv2`, `Spec`, `Expand` and rejecting `-` in variant IDs (stage 5).
- The originals and their IDs, `swing_highlow`, and retiring or renaming either package.
- Exact recursion (iteration 4), and checking the maths against textbook values: the originals are
  the reference.
- Exporting `Gap` results (stage 7).
- Copies of dependency results (iteration 3), and filling the copies from `Step`'s schedules
  (stage 6).

## Verification

```bash
# step 1
go test -race -count=1 -run Reference -v ./internal/engine/data/
# steps 2 and 3
go test -race ./internal/protocol/... ./internal/engine/...
go test -race -count=1 -run Reference -v ./internal/engine/data/
# step 4
go test -race ./internal/domains/featuresv2/
# step 5
go test -race ./internal/engine/features/ ./internal/domains/featuresv2/
go test -count=1 -run Reference -v ./internal/engine/features/
# every step
make check
```
