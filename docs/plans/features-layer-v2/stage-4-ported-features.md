# Stage 4 — ported features (features layer v2, iteration 1)

## Context

[main.md](main.md) stage 4. Stage 3 built the contract (`internal/protocol/feature`), `reg.FeatureTypes`
and the engine's columns, graph and view, tested with test-only features; nothing registers in
`reg.FeatureTypes` yet.

This stage ports `sma`, `ema`, `rsi`, `vwap` and `engulfing_candle` onto the contract, in a new
package `internal/domains/featuresv2` (`featv2`) beside `internal/domains/features` (`feat`). Each is
registered under the original's type name, with its maths copied unchanged. The ports are then
checked against the originals at every column where the bridge evaluates:

- a table of every registered type, checked against the engine's switch;
- a comparison over the BTCUSDT reference dataset, on a random four-month sample per run and
  skipped without the dataset: the same sampling and behaviour as the data layer's reference test;
- a check that a lookback beyond the data layer's cap fails `data.New`.

```
featv2 init   reg.FeatureTypes.Register("sma", newSMA) … five types
factory       config.Decode ─▶ feature.Feature{ID, Lookback, Bind: Bind[float64] | Bind[int]}
node          in.Data(n, scratch) ─▶ the original's maths ─▶ (value, true) | (0, false)
checked       newGraph ─▶ New ─▶ pin ─▶ fire ─▶ ViewOf  ==  FeatureChain.Evaluate(sub, view.Data(s, L))
```

Outside the new package and the tests, only the data layer's reference sampling changes: it moves
into a helper package. `cmd` does not import `featv2` until stage 5.

The plan, with the decisions it records in [main.md](main.md) and
[features.md](../../discussions/features.md), is committed before step 1. The stage is then
implemented in three steps, in order. Each step is a review checkpoint: its changes stay unstaged
for review, are committed once approved, and the next step starts from that commit. Every step
leaves the tree building, tested and `make check` clean.

| Step | Adds | Commit |
|---|---|---|
| 1 | The reference sample shared | `refactor(data): share the reference sample` |
| 2 | The five ported features | `feat(features): port the five features to the feature contract` |
| 3 | Checks against the originals; the stage's documentation | `test(features): check the ported features against the originals` |

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
- **Maths copied unchanged, including the iteration order.** A node runs the original's loops over
  `[]proto.Candle` in the same order, so floating-point sums match bit for bit. Rounding to 6 decimals
  stays, as does `rsi`'s unrounded 100.
- **Reads equal the bridge's.** `Ring.Data` walks back at most the ring's bars and stops at n `Fresh`
  values. A node's `in.Data(n)` is therefore the prefix of the bridge's `view.Data(s, lookback)` that
  `SortedWindow.Last(n)` takes, and close times strictly increase, so the window's sort changes
  nothing. Values compare with `==`.
- **One read shape.** Every port reads its last `n` candles and needs at least `min` of them. A
  package helper `overCandles[T](n, min, f)` returns the `feature.Bind[T]`: it takes `Candles`,
  allocates `make([]proto.Candle, 0, n)` as scratch, and its node returns false below `min`, otherwise
  `f(candles)`. Nodes allocate nothing per call, as stage 6's `Step` requires.

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

## Step 2 — the five ported features

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
			return sma(candles), true
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
  pinned with `frame.NewView`, allocates nothing per call.

### Acceptance

- `go test -race ./internal/domains/featuresv2/` passes; `make check` clean.

## Step 3 — checks against the originals

### Changes — new `internal/engine/features/ported_test.go`

It blank-imports `featv2` and imports `feat`, `internal/engine/data`, `datatest` and `provider`. Its
comparison
helper takes a pinned engine and a `FeatureChain` of the originals. At each stream updated at the
frame's column (`Fresh`, age 0) it:

1. fires every port;
2. evaluates the chain like the bridge, `Evaluate(sub, view.Data(s, chain.Lookback()))`;
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
  `internal/engine/data` gains `datatest`.
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
  originals' (`ema_20_s3`); every registered type is in the table and has a case in the switch.

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

## Out of scope

- `cmd`'s import of `featv2`, `Spec`, `Expand` and rejecting `-` in variant IDs (stage 5).
- The originals and their IDs, `swing_highlow`, and retiring or renaming either package.
- Exact recursion (iteration 4), and checking the maths against textbook values: the originals are
  the reference.
- Exporting `Gap` results (stage 7).

## Verification

```bash
# step 1
go test -race -count=1 -run Reference -v ./internal/engine/data/
# step 2
go test -race ./internal/domains/featuresv2/
# step 3
go test -race ./internal/engine/features/ ./internal/domains/featuresv2/
go test -count=1 -run Reference -v ./internal/engine/features/
# every step
make check
```
