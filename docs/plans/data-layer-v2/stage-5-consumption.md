# Stage 5 — consumption and rolling aggregation (data layer v2, iteration 1)

## Context

Stage 5 of [main.md](main.md): the engine-side half of the module. `Layer` wraps the
[stage 4](stage-4-align-readiness.md) replay pipeline: `data.New` builds the layer and instantiates
its pipeline, `Start` runs the providers and the pipeline, `Next` aggregates each released column's
derived legs in a loop on the engine goroutine and returns its `View`, and `Close` stops the run.
Design in [data-layer-v2.md](../../discussions/data-layer-v2.md) (§ Aggregation, § Consumption,
§ Rolling aggregation semantics), refined here where noted.

```
provider_i ─▶ normalise ─▶ align_i ─evaluate─┐
provider_j ─▶ normalise ─▶ align_j ─evaluate─┼─ pipe.FanIn ─▶ readiness ─▶ Next(): aggregate (loop), consume ─▶ *View
      │                       ▲              ┘                                                          │
      └─ errors ─▶ logged     └──── gate: consumed column ◀─────────────────────────────────────────────┘
```

Replay only. `data_logger_v2` and the engine-side pacer are stage 6; the deadline stage, `Pending`
and `Late` are iteration 2.

## Decisions

- **`Event` folds into `View`.** The View is pinned at the event's column and the event describes
  it, so one value carries both: `View` gains `Status()` beside `Col()` and `Complete()`, readiness
  emits Views, and the `Event` type goes. `Late`'s revised legs (iteration 2) will travel with the
  View.
- **`Next` returns `(*View, error)`.** A View while frames remain; `nil, nil` once the run has ended
  — every lane closed and every frame returned; `ErrNotStarted` before `Start`; `ErrClosed` after
  `Close`, whose drain consumed the remaining frames; `ctx.Err()` once `ctx` is done. `Next` reuses
  one View held by the layer, so the returned pointer is valid until the next call: `Next` consumes
  the column, and the align stages may then overwrite history only a View at an earlier column
  reads.
- **Aggregation is a loop in `Next`, then the column is consumed.** On the engine goroutine, `Next`
  updates every derivation whose source updates at the column, in stream-table order, then consumes
  the column. Each update is O(1) — a few ring reads, deque steps, two compensated adds and one slot
  write — estimated below the cost of handing it to a goroutine. Released slots keep one writer, the engine
  goroutine; aggregation reads only its sources' kept bars, which ring capacity keeps clear of the
  align stages. `BenchmarkAggregate` records the loop's cost per column, the baseline for parallel
  aggregation if wide frames need it.
- **Derived legs take their source's state at its update columns.** Source `Fresh`: the derived slot
  is `Fresh` with the window's candle and `count` the `Fresh` source bars in it. Source `Gap` or
  `Ended`: the derived slot has that state and carries the latest `Fresh` derived candle and its
  count — the window ending at the source's latest `Fresh` bar, the same carry a direct leg makes.
  The window slides at every source update column either way: the bar `W` back leaves it.
- **The window's candle is built from its `Fresh` bars.** `OpenTs` and `Open` from the oldest,
  `CloseTs` and `Close` from the source bar at the column, `High` / `Low` the extremes, `Volume`
  their sum. At a UTC boundary with a full window it equals the closed bar.
- **O(1) per update.** Volume is a running sum with Neumaier compensation, so add-and-subtract does
  not drift over a year of updates; `High` / `Low` are monotonic deques of `(column, value)`, `W`
  entries each, allocated in `New`; the oldest `Fresh` bar is a column pointer moved forward over
  `Gap` slots (amortised O(1)). The bar leaving the window, `W` back, is read from the source ring,
  which keeps `W + 1` bars.
- **`Close` is synchronous and stops the run without a new stage convention.** It closes the
  providers, stops the gate — waiting align stages wake and every later wait fails, so align stops
  writing and drains its input — and reads the remaining Views until readiness closes, discarding
  them unaggregated. Every stage still exits because its input closed. Once `Close` returns the
  output is drained, so a frame released but not yet returned is never delivered, and `Next` returns
  `ErrClosed`. It relies on each provider closing its outputs once closed (`binance_csv` does), and
  must not run concurrently with `Next`; another goroutine stops a run by cancelling `ctx`, after
  which the engine goroutine calls `Close`.
- **Provider errors are drained and logged by the layer.** Undrained, a provider blocks on its error
  channel (`binance_csv` buffers 10). Exposing them waits for the provider drop contract
  (iteration 2).
- **`New` instantiates, `Start` runs.** `New` performs no I/O and starts no goroutine: it maps
  providers to direct streams in stream-table order and builds the stream table, buffer, gate,
  aggregator and the replay pipeline (`replayPipeline`). A direct subscription without a provider,
  one provider instance for two subscriptions (`Init` takes one) and the 365-day cap are errors there.
  `Start` calls `Init`, `Streams()` and `Start` on each provider, then runs the pipeline over their
  outputs; a provider failing to start closes those already started and leaves the layer unstarted. A layer runs once: `Start` again is an error, and after
  `Close` it returns `ErrClosed`. `lookback` is the caller's: stage 6 passes the longest feature
  `Lookback()`.

## Changes

### View and events — `internal/engine/data/view.go`, `event.go`, `buffer.go`

```go
// View is the FrameBuffer pinned at one released column, and that column's frame: its status and
// completeness. No read sees a later column.
type View struct {
	buf      *FrameBuffer
	col      int64
	complete bool
	status   Status
}

func (v View) Status() Status
```

- `Complete()`'s comment takes `Event.Complete`'s: every active direct stream's latest due candle at
  or before the column is present.
- `event.go` keeps `Status`, `Current` and `evaluate`; `Event` is deleted.
- `FrameBuffer.view(col, complete)` sets `status: Current` (iteration 2 adds `Late`).

### Readiness and composer — `readiness.go`, `pipeline.go`

- `step(e evaluate, dst []View) []View`; `release` appends `r.buf.view(col, r.complete(col))`;
  `run` returns `<-chan View` (capacity `defaultHeadroom`).
- `pipeline.ready` is a `pipe.Stage[evaluate, View]`; `run` returns `<-chan View`. The doc comment's
  "stage 5 wraps the pipeline in the module API" names `Layer`.

### Gate and align — `align.go`

```go
type gate struct {
	mu       sync.Mutex
	cond     sync.Cond
	consumed int64 // −1 before the first column
	stopped  bool
}

// wait blocks until the engine has consumed col; false if the gate is stopped first.
func (g *gate) wait(col int64) bool

// stop wakes every waiting align stage; wait returns false from then on.
func (g *gate) stop()
```

- `write` returns false when its capacity wait fails. `run` then discards the rest of its input
  without writing or sending, sends no `Ended`, and closes its output.

### Aggregation — new `internal/engine/data/aggregate.go`

```go
// deque is a fixed-capacity ring of (column, value): W entries.
type deque struct {
	cols    []int64
	vals    []float64
	head, n int
}

/*
window is one derived stream's rolling state over its source's bars at columns
col − (W−1)·Period … col; only the engine goroutine touches it.
*/
type window struct {
	buf            *FrameBuffer
	stream, source int
	period, w      int64   // w: source bars per window
	started        bool    // the derived ring's first column is set
	fresh          int64   // Fresh source bars in the window
	oldest         int64   // oldest Fresh source column in the window; −1 for none
	vol, comp      float64 // running volume and its Neumaier compensation
	hi, lo         deque   // monotonic: the highest high and lowest low at the front
	count          uint16  // count of the last Fresh write, carried by Gap and Ended
}

/*
update slides the window to col, one of its source's update columns, and writes the derived slot
there: Fresh with the window's candle and the count of Fresh bars in it; Gap and Ended carrying the
latest Fresh derived candle and its count. A source not started at col, or ended before it, writes
nothing.
*/
func (w *window) update(col int64)

// aggregator materialises the derived legs of released columns on the engine goroutine.
type aggregator struct {
	windows []window // one per derived stream, in stream-table order
}

func newAggregator(buf *FrameBuffer) *aggregator

// step aggregates col, the column just released: update on each window whose source updates there.
func (a *aggregator) step(col int64)
```

- The source's slot at `col` is read through the `View` bounds (`latest`): nothing below the
  source's first column, which its align stage may still be wrapping onto at startup.
- On the first write, `buf.start(stream, col)`. Writes go through `put`, unchanged; derived slots
  are written only by `step`. `step` allocates nothing.

### Module API — new `internal/engine/data/data.go`

```go
var (
	ErrNotStarted = errors.New("data: layer not started")
	ErrClosed     = errors.New("data: layer closed")
)

// Layer is the data layer of one run: the stream table, the FrameBuffer and the replay pipeline.
type Layer struct {
	streams   *Streams
	buf       *FrameBuffer
	gate      *gate
	agg       *aggregator
	stages    pipeline
	providers []proto.DataProvider // per direct stream, in stream-table order
	log       *logger.Logger

	started, closed bool
	events          <-chan View     // nil until Start
	errsDone        <-chan struct{} // closed once every provider's error channel has
	view            View            // the frame Next returned last
}

/*
New builds the data layer over validated direct subscriptions and their provider instances, one per
subscription, and instantiates its pipeline. lookback is the longest read in bars, the same for
every stream. It performs no I/O and starts nothing; Start runs the layer.
*/
func New(subs []proto.Subscription, providers map[proto.Subscription]proto.DataProvider,
	lookback int, log *logger.Logger) (*Layer, error)

// newLayer is New with the rings' headroom in columns.
func newLayer(subs []proto.Subscription, providers map[proto.Subscription]proto.DataProvider,
	lookback int, headroom int64, log *logger.Logger) (*Layer, error)

func (l *Layer) Streams() *Streams

/*
Start initialises and starts each direct stream's provider, then runs the pipeline over their
outputs; provider errors are logged. If a provider fails to start, the ones already started are
closed and the layer stays unstarted. A layer runs once: Start again is an error, and after Close it
returns ErrClosed.
*/
func (l *Layer) Start() error

/*
Next returns the next frame: the View pinned at the next released column, its derived legs
aggregated. It returns nil, nil once the run has ended; ErrNotStarted before Start; ErrClosed after
Close; ctx.Err() once ctx is done. The View is valid until the next call, which consumes its column.
*/
func (l *Layer) Next(ctx context.Context) (*View, error)

/*
Close stops the run: it closes the providers, stops the gate and discards the remaining frames until
every stage has exited, so it returns with the output drained. A layer never started has nothing to
stop. Next returns ErrClosed from then on. It must not run concurrently with Next.
*/
func (l *Layer) Close()
```

- `New` calls `newLayer` with `defaultHeadroom`; `newLayer` builds `stages` with `replayPipeline`.
- Provider errors: `pipe.Forward(pipe.FanIn(errs...), …)` logging each with `log.Error`;
  `errsDone` is its done channel. `Close` waits on it.

## Tests

Conventions as before: the shared fixture (5m grid; BTCUSDT 5m deriving 1h, BTCUSDT 1h deriving
1d, ETHUSDT 1d), `day0`, `bar` / `bars`, `span`, `quiet`, `throttled`.

### Existing tests

- Readiness and pipeline tests collect Views: `frames(buf, segs...)` and `model(buf, accepted)`
  build them over the fixture's buffer, `consumeAll` reads `<-chan View`; `BenchmarkReadiness`
  consumes `Col()`. Their scenarios are unchanged.

### `align_test.go`

- **`TestGate`** gains: `stop` releases every waiter with false; `wait` after `stop` returns false at
  once, even for a consumed column.
- **`TestAlignStop`** — `headroom` 4, a 5m lane of 100 candles, nothing consumed: align blocks at
  `day0+25`; after `stop` it drains the rest of its lane without writing a slot or sending an
  evaluate, sends no `Ended`, and closes its output.

### `aggregate_test.go`

Source slots are written with `put` and `start`, then `step` runs over each column in order; derived
legs are read through Views.

**`TestAggregate`**
- warm-up: 5m `Fresh` from `day0+1`: rolling 1h count 1 … 12, then 12; at each 1h boundary the
  candle equals the 1h bar built from its twelve 5m bars;
- a 5m gap at `x`: rolling 1h `Gap` at `x`, carrying the previous rolling candle and count (`Leg`
  age 1); the next eleven windows hold count 11 and leave `x` out;
- a gap at a window's oldest bar: `Open` and `OpenTs` come from its next `Fresh` bar;
- 5m `Ended` at `e`: rolling 1h `Ended` at `e`, carrying the latest `Fresh` rolling candle; nothing
  written after;
- coarse source: rolling 1d from 1h written only at 1h update columns, carried between them;
- source not started: no derived slot before its first column; `Leg` zero candle, `Gap`, −1.

**`TestAggregateModel`** — BTCUSDT 5m deriving 1h and 1d, ten days of 5m with seeded random gaps,
an early end and volumes over several magnitudes: at every column the rolling 1h and rolling 1d legs
— candle, state, count, age — equal a brute-force aggregation of the `Fresh` bars in their windows;
volume within 1e-9 relative.

**`TestAggregateAllocations`** — `step` allocates nothing.

**`BenchmarkAggregate`** — `n` = 10, 100, 1000 derived streams (direct 1m sources deriving 1h); each
iteration puts every source's next `Fresh` slot, then calls `step` on the column. Reports ns per
column and per derivation (`b.ReportMetric`).

### `data_test.go`

A `fakeProvider` implements `proto.DataProvider` over a candle slice: `Init` records its
subscription and makes its channels, `Start` sends from a goroutine (optionally throttled) selecting
on `done`, `Close` closes `done`; it can fail `Init` or `Start`, send errors, and counts its calls.

- **`TestNew`** — a direct subscription without a provider, and one provider instance for two
  subscriptions, are errors naming the subscription; the 365-day cap surfaces; `Streams()` holds the
  derived streams; nothing runs: no provider is touched and `Next` returns `ErrNotStarted`.
- **`TestLayerRun`** — the `TestPipelineRun` input through fake providers, `headroom` 4: `Next`
  returns each column once, in order, `Current`, with `model`'s completeness and derived legs equal to
  `TestAggregateModel`'s brute force over the accepted candles; then `nil, nil`, and `nil, nil` again
  on the next call.
- **`TestLayerCancel`** — a cancelled `ctx`: `Next` returns `nil, ctx.Err()`; `Close` returns.
- **`TestLayerClose`** — one `Next`, then `Close`, in two runs: `headroom` 4, where align stages wait
  on the gate, and `headroom` 600, where readiness blocks on its full channel and align on its
  evaluates. `Close` returns within 10 s, every provider is closed, the event and error channels are
  closed, and `Next` returns `ErrClosed` — the frames left at `Close` are never returned; a second
  `Close` is a no-op.
- **`TestLayerStart`** — an `Init` failure returns an error naming the stream and starts no provider,
  and `Next` still returns `ErrNotStarted`; a `Start` failure closes the providers already started,
  and `Close` afterwards touches nothing; `Start` twice is an error; `Start` after `Close` returns
  `ErrClosed`.
- **`TestLayerProviderErrors`** — a provider sending more errors than its buffer holds: the run
  completes and each error is logged.
- **`TestLayerSkew`** — `TestPipelineSkew` through `Layer`, `headroom` 4, one lane throttled then the
  other: same frames both ways, and at every frame every stream's `Leg`, `Count` and `Candles`
  (`lookback` bars) are read while the align stages stage ahead. Meaningful under `-race`.
- **`TestLayerReference`** — the BTCUSDT reference dataset, `data/BTCUSDT/BTCUSDT-{5m,1h,1d}-2024-01.csv`
  (all of 2024, agreeing exactly in decimal; skipped when absent). Each run samples four months at
  random: a start day drawn from 2024-01-01 … 2024-09-01, the rows of each file with `OpenTs` in
  `[start, start + 4 months)` written to `t.TempDir()`, the span logged so a failure names it.
  `binance_csv` providers over the samples: BTCUSDT 5m deriving 1h and 1d, BTCUSDT 1h and BTCUSDT 1d
  as references. At every frame where the direct 1h is `Fresh` at age 0, the rolling 1h has count 12
  and its candle equals the direct 1h bar — timestamps and OHLC exactly, volume within 1e-9 relative;
  likewise the rolling 1d (count 288) against the direct 1d. Every hour and day of the span is
  compared. Rolling 1d over 5m equals rolling 1d over the rolling 1h, so a 1h → 1d derivation's match
  follows from the two.

## Documentation updates

- [main.md](main.md):
  - Decisions taken by this plan: `Event` folds into `View`; `Next` returns `(*View, error)` —
    `nil, nil` at the end of the run, `ErrNotStarted`, `ErrClosed`; `New` instantiates the pipeline,
    `Start` runs it; aggregation is a loop in `Next`; `Close` is synchronous and discards the frames
    not yet returned; provider errors logged by the layer until the drop contract.
  - Structure: `stage-5-consumption.md` — *implemented*.
  - Stage 5 entry ✅ implemented. Touches add `view.go`, `event.go`, `readiness.go`, `pipeline.go`
    (Views) and `align.go` (gate `stop`); Contents: derived legs take their source's state, the
    window's candle from its `Fresh` bars, compensated volume, aggregation in a loop before `Next`
    consumes the column, `Close` drains; "row ownership as decided" → derived slots are written only
    by `Next`, on the engine goroutine. Plan: `stage-5-consumption.md`.
  - Open items: stopping align stages (`Close`) and the derived leg state under source gaps resolved
    by stage 5.
- [data-layer-v2.md](../../discussions/data-layer-v2.md):
  - § Aggregation: derived legs take their source's state; `Gap` / `Ended` carry the latest `Fresh`
    derived candle and its count; the candle from the window's `Fresh` bars; count = `Fresh` bars;
    compensated running volume.
  - § Consumption: `Next` returns the pinned `View` — valid until the next call — or `nil, nil` at
    the end of the run, `ErrNotStarted`, `ErrClosed`; it aggregates the column's derived legs in a
    loop, then consumes the column; `Close` is synchronous — it closes providers, stops the gate and
    drains, discarding the frames not yet returned; provider errors are logged by the layer.
  - Solution: readiness emits Views in the pipeline table; § Events code block → `Status` and
    `evaluate`, `Event` folded into `View`; § FrameBuffer code block: `View` with `status` and
    `Status()`; the `BenchmarkAggregate` result.
  - Open Questions: provider drop contract notes that errors are logged by the layer until then.
  - WIP: stage 5 implemented — `docs/plans/data-layer-v2/stage-5-consumption.md`.
- [pipeline-runtime.md](../../discussions/pipeline-runtime.md): step loop — `f, err := data.Next(ctx)`
  returns the pinned `View` (status, completeness, aggregated legs), or `nil, nil` at the end of the
  run.
- `docs/Architecture.md`: `internal/engine/data` — add aggregation and the module API (`Layer`:
  `New`, `Start`, `Next`, `Close`).
- `docs/Commands.md`: `go test -count=1 -run Reference -v ./internal/engine/data/` — rolling 1h and
  1d from 5m against the BTCUSDT reference dataset over a random four-month sample; skipped without
  `data/BTCUSDT`. `-count=1` bypasses the test cache, which would otherwise replay the last sample.
  `go test -run '^$' -bench Aggregate -benchmem ./internal/engine/data/` — aggregation at 10, 100
  and 1000 derived streams, per column and per derivation.
- [README.md](../../discussions/README.md): data-layer-v2 row → stages 1–5 implemented.

## Out of scope

- `data_logger_v2`, the pacer, the feature bridge, frame logging — stage 6.
- Exposing provider errors, the provider drop contract, the deadline stage, `Pending`, `Late` and
  revision recompute — iteration 2.
- A per-stream updated set on `View` (dirty set); stage 6 reads `Leg` state and age.
- Parallel aggregation — a fixed worker pool over groups of derivations — until wide frames need it;
  `BenchmarkAggregate` is its baseline.
- Changes to `put`, `OrderedFanIn`, `SynchronizerV2` or `data_logger`.

## Verification

- `make check` clean.
- `go test -race ./internal/engine/data/... ./internal/flow/...` clean.
- `go test -count=1 -run Reference -v ./internal/engine/data/` runs, not skipped, locally.
- `go test -run '^$' -bench Aggregate -benchmem ./internal/engine/data/` runs; results recorded in
  the discussion.
- `make validate` passes on `config.json`.
- Nothing this stage introduces is left without a caller in production code or tests.
