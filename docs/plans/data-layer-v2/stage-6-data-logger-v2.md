# Stage 6 — `data_logger_v2` engine (data layer v2, iteration 1)

## Context

Stages 1–5.1 built `internal/engine/data` for replay: the `Layer` (`New`, `Start`, `Next`,
`Close`) releases frames in column order as pinned `View`s, derived legs aggregated. Nothing runs it:
`data_logger` still runs `provider → FeatureChain → OrderedFanIn → SynchronizerV2 → ticker gate →
exporter`. This stage adds the engine that consumes the module, and closes iteration 1.

```
data_logger_v2.Run: Init providers ─▶ data.New ─▶ Start
                    loop: Next ─▶ pacer.Wait ─▶ per updated stream: feature bridge ─▶ exporter ─▶ Frame log ─▶ limit
                    Close (drain) ─▶ exporter Close
```

The stage is one plan in four steps, implemented in order. After each step the changes stay
unstaged for review; the step is committed once approved, then the next starts.

1. Rename `Layer` → `DataPipeline`.
2. Engine building blocks: feature bridge, pacer, frame logger.
3. The `data_logger_v2` engine.
4. The v2 config and closing iteration 1.

## Decisions

- **`Layer` becomes `DataPipeline`** (`*data.DataPipeline`). Identifiers follow: `newLayer` →
  `newDataPipeline`, receiver `l` → `dp`, test helpers and names. Error messages say "pipeline"
  (`data: pipeline not started`, `data: pipeline closed`, `data: pipeline already started`). Lowercase
  "layer" in comments stays where it names the data layer. Executed stage docs and the discussion's
  WIP history keep `Layer`.
- **Feature bridge.** `FeatureChain` gains `Lookback()`, the `lookback` passed to `data.New`, and
  `Evaluate(sub, history)`, which refills the chain's window from one stream's history and runs the
  chain once. It refills rather than pushes the newest candle: a derived stream's history is stepped
  (`c, c−W, …`), not its last rolling candles. O(lookback) per stream update. One chain serves every
  stream — features are stateless.
- **Pacer in `flow/timing`**, as sketched in [pipeline-runtime.md](../../discussions/pipeline-runtime.md)
  § Clock: `Pacer.Wait(ctx, frameTime)` with `Unpaced` and `Fixed`. `newPacer(*TickerConfig)` maps the
  existing `ticker` block onto it, beside `newTicker`; `Ticker` and `Gate` are untouched. Factor and
  manual modes stay unimplemented.
- **The pacer waits after `Next`**, since `Wait` takes the frame's time — not before, as main.md's
  loop reads today.
- **Frame logging.** `logger` cannot import `data` (`data` imports `logger`), so `Logger.Frame`
  takes plain values: boundary, column, status, completeness, the streams updated, the leg count —
  `SyncEvent`'s fields, per column.
- **A stream updates at a column when its leg is `Fresh` with age 0.** Each update exports one
  record; carried, `Gap` and `Ended` legs export nothing. Streams go in stream-table order, so a
  frame's exports are deterministic.
- **`limit` counts frames** — every column `Next` returns, with or without an update. The engine
  reuses `DataLoggerOptions` (`limit`, `ticker`).
- **The feature chain is built in the factory.** `NewFeatureChain` does no I/O, so duplicate and
  cyclic features fail `--validate-only`.
- **`DataPipeline` belongs to `Run`'s goroutine.** `Run` calls `Next` and `Close`; the engine's `Close()`
  cancels `Run` and waits for it to return.
- **The v2 config avoids two streams of one timeframe** — 5m deriving 1h, plus a direct 1d — because
  the JSONL record carries no stream identity: a rolling and a direct 1h would export identical
  `symbol` / `timeframe`. The exporter is unchanged. Location: `configs/data_logger_v2.json`.

## Step 1 — rename `Layer` → `DataPipeline`

### Changes

- `internal/engine/data/data.go`: `type Layer` → `type DataPipeline`; `newLayer` →
  `newDataPipeline`; receivers `(l *Layer)` → `(dp *DataPipeline)`; doc comments naming the type
  ("Layer is the data layer of one run, and its pipeline" → "DataPipeline is the data layer of one
  run"); `ErrNotStarted`, `ErrClosed` and `Start`'s second-start error say "pipeline" instead of
  "layer".
- `internal/engine/data/data_test.go`: `startLayer` → `startDataPipeline`; `TestLayerRun`,
  `TestLayerCancel`, `TestLayerClose`, `TestLayerStart`, `TestLayerProviderErrors`, `TestLayerSkew`,
  `TestLayerReference` → `TestDataPipeline…`; locals `l` → `dp`.
- `internal/engine/data/lane_test.go`: `newLayer` → `newDataPipeline`; locals `l` → `dp`.
- No behaviour change beyond the error texts; tests compare the sentinels, not their text.

### Documentation

- `docs/Architecture.md`: "the `Layer` is the pipeline" → "`DataPipeline` is the data layer's
  pipeline".
- [main.md](main.md):
  - Decisions taken by this plan: pipeline construction and module API name `DataPipeline`.
  - Structure: `stage-6-data-logger-v2.md` — one plan in four steps.
  - Stage 6 entry: Touches gains the rename (`internal/engine/data`); the loop reads `Next → pacer →
    feature bridge → exporter → limit`; Contents lists the four steps.
- [data-layer-v2.md](../../discussions/data-layer-v2.md): § Module and stages, "The `Layer` is the
  pipeline" → "`DataPipeline` is the data layer's pipeline", and "the `Layer` is the typed pipeline
  instance" → "`DataPipeline` is the typed pipeline instance"; Solution § Events, the module API
  block returns `*DataPipeline` with `(dp *DataPipeline)` receivers. WIP history untouched.
- `docs/Commands.md` needs nothing: the Reference command runs `-run Reference`, which still matches.

### Acceptance

- No `Layer`, `newLayer` or `startLayer` identifier, and no error message naming the layer, left in
  `internal/engine/data`.
- `go test -race ./internal/engine/data/...` and `make check` clean.

## Step 2 — engine building blocks

### Feature bridge — `internal/domains/features/feature.go`

```go
// Lookback is the longest history any feature in the chain reads, in candles; 0 without features.
func (fc *FeatureChain) Lookback() int

/*
Evaluate runs the chain once over one stream's history and returns the snapshot. history is newest
first, history[0] the candle being evaluated, and should hold Lookback candles where available. It
refills the chain's window, so a chain serves either Process or Evaluate, from one goroutine.
*/
func (fc *FeatureChain) Evaluate(sub proto.Subscription, history []proto.Candle) proto.Snapshot
```

- `Lookback` returns `calculateRequiredCapacity(fc.features)`.
- `Evaluate`: `window.Clear()`, pushes `history` oldest first as `MarketData{Subscription: sub,
  Candle: c}` (a rejected candle is skipped with a warning, as `Process` does), runs every feature in
  order into a new snapshot.

### Pacer — new `internal/flow/timing/pacer.go`

```go
/*
Pacer paces frame consumption in replay: the engine waits on it after taking each frame, before
processing it. It changes when frames are consumed, never their content.
*/
type Pacer interface {
	// Wait blocks until the frame at frameTime (Unix ms) may be processed, or ctx is done.
	Wait(ctx context.Context, frameTime int64) error
}

// Unpaced lets every frame through at once.
func Unpaced() Pacer

// Fixed lets one frame through per interval, whatever its time; the first after one interval.
func Fixed(interval time.Duration) Pacer
```

- `Unpaced.Wait` returns `ctx.Err()`.
- `Fixed` starts a `time.Ticker` on its first `Wait` and returns on its next tick or `ctx.Err()`. A
  tick missed while a frame is processed is kept, one at most (`time.Ticker` semantics, as the
  `fixed` ticker's one-slot tick channel). No `Stop`: an unreferenced ticker is collected (Go ≥ 1.23).

### `newPacer` — `internal/engine/ticker.go`

```go
// newPacer builds the replay pacer for a ticker block. A nil block means realtime: unpaced.
func newPacer(c *TickerConfig) (timing.Pacer, error)
```

Same switch as `newTicker`: `fixed` → `timing.Fixed(interval)`, `realtime` and nil →
`timing.Unpaced()`, anything else an error.

### Frame logging — `internal/logger/logger.go`

```go
// Frame logs a released data-layer frame; streams lists the streams that updated at its column.
func (l *Logger) Frame(time, col int64, status int, complete bool, streams []string, legs int)
```

Fields: `event` = `frame`, `time` (boundary, Unix ms), `col`, `status`, `complete`, `now_count`,
`leg_count`, `streams`; message `Frame`. `SyncEvent` is unchanged.

### Tests

- `internal/domains/features/feature_test.go`:
  - **`TestEvaluateMatchesProcess`** — a chain of `sma_3`, `ema_5`, `vwap_4`, `rsi_3` and
    `engulfing_candle` over 40 synthetic candles: at every candle, `Evaluate` over the last
    `Lookback()` candles, newest first, equals the snapshot `Process` emitted for it.
  - **`TestLookback`** — the largest feature lookback (`ema_5` → 10); 0 for an empty chain.
- New `internal/flow/timing/pacer_test.go`:
  - **`TestUnpaced`** — 1000 waits return nil at once; a cancelled context returns
    `context.Canceled`.
  - **`TestFixed`** — five waits at 10 ms take at least 45 ms; cancelling during a wait returns
    `context.Canceled` within 100 ms.
- `internal/engine/ticker_test.go`: **`TestNewPacer`** — nil and `realtime` do not wait; `fixed` at
  20 ms waits at least 15 ms before its first frame.
- `Frame` is exercised by step 3's engine tests, which read frames back from the log.

### Documentation

- `docs/Commands.md`: `make test` package list adds `internal/flow/timing`.

### Acceptance

- The tests above pass; `Ticker`, `Gate`, `Process` and `SyncEvent` unchanged; `make check` clean.

## Step 3 — the `data_logger_v2` engine

### Engine — new `internal/engine/data_logger_v2.go`

```go
const DataLoggerV2ID = "data_logger_v2"

/*
DataLoggerV2 runs the data layer in replay and exports every stream update with its features. Run
initialises each provider with its subscription, then builds and starts the data layer; each frame
is paced, every stream updated at its column is evaluated by the feature chain over its history and
exported, and the frame is logged. The run ends with the data, at limit frames, or when ctx is done
or Close is called; the data layer is then closed, then the exporter.
*/
type DataLoggerV2 struct {
	id        string
	providers map[proto.Subscription]proto.DataProvider
	chain     *feat.FeatureChain
	exporter  proto.Exporter // nil: frames are logged only
	logger    *logger.Logger
	cfg       DataLoggerOptions

	mu      sync.Mutex
	cancel  context.CancelFunc // Run's; nil until Run
	closed  bool
	stopped chan struct{} // closed once Run returns
}

func (e *DataLoggerV2) Run(ctx context.Context) error

// Close stops Run and waits for it to return; before Run, Run returns at once. Idempotent.
func (e *DataLoggerV2) Close()
```

- **Factory** (registered in `init`): `_providers`, `_features`, optional `_exporter`;
  `parseDataLoggerOptions`; `feat.NewFeatureChain(features)`. Derived subscriptions are accepted;
  `_subscriptions` is not read — the map's keys are the subscriptions. No I/O.
- **`Run`**:
  1. Wraps `ctx` with a cancel kept for `Close`; returns nil at once if already closed;
     `defer close(stopped)`.
  2. Exporter `Init`; its `Close` deferred, so it runs on every later return.
  3. `Init` on each provider with its key, in subscription-ID order; a failure returns
     `provider of <id>: init: <err>`. Nothing is started.
  4. `dp, err := data.New(providers, chain.Lookback(), logger)`; `dp.Start()` — on failure the
     pipeline has closed itself and its error is returned; otherwise `defer dp.Close()`.
  5. `newPacer(cfg.Ticker)`, then the loop below.
  6. Returns nil at the end of the data, at the limit or on `context.Canceled`; another `ctx` error
     (a deadline) is returned, as `data_logger` does.
- **Loop**, one frame per pass:
  1. `v, err := dp.Next(ctx)`: an error ends the run (`ctx` done); `nil, nil` is the end of the data.
  2. `pacer.Wait(ctx, v.Boundary())`.
  3. For each stream `s` of `dp.Streams().All` with `Leg(s)` `Fresh` at age 0: its ID joins the
     updated list; with an exporter, `chain.Evaluate(sub, v.Candles(s, lookback, history))` and
     `Export(MarketData{Subscription: sub, Candle: c}, &snap)` — an export error is logged.
     `history` is one slice reused across streams and frames.
  4. `logger.Frame(v.Boundary(), v.Col(), int(v.Status()), v.Complete(), updated, len(All))`.
  5. Counts the frame; at `limit`, logs "limit reached" and ends the run.
- Locals avoid the name `data`, which would shadow the package: the pipeline is `dp`.
- `internal/engine/data_logger.go`: `DataLoggerOptions.Limit`'s comment → "caps frames"; nothing
  else.

### Tests — new `internal/engine/data_logger_v2_test.go`

Fixtures: `barProvider` — a `DataProvider` over a candle slice, with an optional per-candle delay,
`Init` and `Start` errors, and init / start / close counts; `bars(tf, from, n)` building UTC-aligned
candles with Binance-style `CloseTs`; a `recordingExporter` keeping each record's market data and
snapshot copy, with `Init` / `Close` counts; a JSON logger over a locked buffer, from which `frames`
parses the `frame` events. Engines are built directly with these, except the factory test.

- **`TestDataLoggerV2Factory`** — through `reg.Engines.Create`: a derived subscription is accepted;
  a negative `limit`, an invalid ticker type and two identical features are errors; no provider is
  initialised.
- **`TestDataLoggerV2Exports`** — BTCUSDT 5m deriving 1h, and a direct 1h, over one UTC day; one 5m
  bar missing at 10:00; `sma_3`:
  - exports: 287 5m, 287 rolling 1h, 24 direct 1h, each under its own subscription, in column order;
  - one frame per column from the first to the last; the 10:00 frame `Partial`, listing neither the
    5m nor the rolling 1h stream;
  - a 5m record's `sma_3` is the mean of its last three 5m closes; a rolling 1h record's at column
    `c` is the mean of the rolling closes at `c`, `c − 12` and `c − 24` (stepped history); absent
    with fewer than three bars.
- **`TestDataLoggerV2FixedPacerSkew`** — the original skew problem: 5m, 1h and 1d lanes over two
  days, every stream with a bar at the first column; the 5m lane delayed per candle while 1h and 1d
  send at once; `fixed` pacer at 1 ms. Every column from the first to the last is released once, in
  order, `Complete`; every candle is exported once.
- **`TestDataLoggerV2Limit`** — `limit` 10, `fixed` at 5 ms, a day of bars: `Run` returns within
  1 s; exactly 10 frames logged and exports only from them; each provider closed once; exporter
  initialised and closed once.
- **`TestDataLoggerV2Stop`** — 30 days of 5m, `fixed` at 10 ms:
  - cancelling `ctx` after 50 ms: `Run` returns nil promptly; providers and exporter closed once;
  - `Close` from another goroutine: `Run` returns nil, `Close` returns after it; a second `Close`
    is a no-op;
  - `Close` before `Run`: `Run` returns nil and initialises no provider.
- **`TestDataLoggerV2Failures`**:
  - exporter `Init` fails: its error; no provider initialised;
  - a provider's `Init` fails: an error naming its subscription; no provider started; exporter
    closed;
  - a provider's `Start` fails: an error naming its subscription; the providers started before it
    closed once; exporter closed.

`cmd/main_test.go`: **`TestBuildDataLoggerV2AcceptsDerivations`** — the
`TestBuildDataLoggerRejectsDerivations` config with `data_logger_v2` builds without errors.

### Documentation

- [engine.md](../../discussions/engine.md) § Registered engines: a `data_logger_v2` row — data layer
  v2 (`internal/engine/data`) → paced `Next` → feature bridge → log + export; `limit` caps frames.

### Acceptance

- The tests above pass under `-race`; `data_logger` and its tests unchanged; `make check` clean.

## Step 4 — the v2 config and closing iteration 1

### Config — new `configs/data_logger_v2.json`

- Providers `btcusdt_5m` and `btcusdt_1d` (`binance_csv`, the 2024 BTCUSDT files `config.json`
  uses).
- Subscriptions: `BTCUSDT` 5m on `btcusdt_5m` with `"derive": ["1h"]`; `BTCUSDT` 1d on `btcusdt_1d`
  — a coarse direct stream and a derivation, no two of one timeframe.
- Features as in `config.json`.
- Engine `data_logger_v2`: the `jsonl` exporter to `data/exports`; options `ticker` `fixed` 50 ms,
  `limit` 1000, as `config.json`.

### Documentation

- `docs/Commands.md`: running and validating the v2 config (`make run CONFIG=configs/data_logger_v2.json`,
  `make validate CONFIG=…`), and what it exports — 5m, rolling 1h and 1d rows in one JSONL file.
- [main.md](main.md): stage 6 marked implemented, with a Plan line; the iteration 1 diagram's loop
  reads `Next ─▶ pacer ─▶ feature bridge ─▶ exporter`; Open items: frame logging format and the v2
  config's location resolved in stage 6.
- [data-layer-v2.md](../../discussions/data-layer-v2.md): WIP — stage 6 implemented,
  `docs/plans/data-layer-v2/stage-6-data-logger-v2.md`, iteration 1 complete; Open Questions §
  Exporter — a rolling and a direct stream of one timeframe export identical `symbol` / `timeframe`
  until the record carries the stream.
- [pipeline-runtime.md](../../discussions/pipeline-runtime.md): Open Questions § Migration path
  resolved — a separate `data_logger_v2` engine on the module, features through a bridge,
  `data_logger` until retired; § Clock — the pacer is implemented in `flow/timing` (`Unpaced`,
  `Fixed`) and waited on after `Next`; WIP points to the data-layer-v2 plan.
- [multi-subscription.md](../../discussions/multi-subscription.md): `data_logger_v2` runs beside
  `SynchronizerV2`; live ordering, pacing and derive-versus-subscribe point to data-layer-v2.
- [README.md](../../discussions/README.md): data-layer-v2 row — iteration 1 (stages 1–6)
  implemented; pipeline-runtime row — partly implemented through data-layer-v2.

### Acceptance

- `make validate` passes on `config.json` and `configs/data_logger_v2.json`.
- `make run CONFIG=configs/data_logger_v2.json` exports 5m, rolling 1h and 1d records, features
  present once warm; `make run` (`data_logger`) behaves as before.
- Iteration 1's "done when" in [main.md](main.md) holds.

## Out of scope

- Retiring `data_logger`, `SynchronizerV2` and `proto.SyncEventV2`.
- Exporter changes: a stream identity, the window count, leg state or completeness in the record.
- Detecting a shared provider instance or the 365-day cap under `--validate-only`: both surface from
  `data.New` at run time.
- Factor and manual pacer modes; the live path (iteration 2).
- One provider instance per subscription at the composition root.

## Verification

- After each step: `make check`; `go test -race` over the packages it touches.
- At the end: `go test -race ./internal/engine/... ./internal/flow/... ./internal/domains/features/... ./cmd/...`
  clean; `go test -count=1 -run Reference -v ./internal/engine/data/` runs, not skipped;
  `make validate` on both configs; a run of the v2 config inspected.
