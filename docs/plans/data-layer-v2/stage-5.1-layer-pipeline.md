# Stage 5.1 — the layer is the pipeline (data layer v2, iteration 1)

> Gate `stop` since removed: `Close` consumes each frame it discards; a start failure first frees the gate.

## Context

Adjusts [stage 5](stage-5-consumption.md), as built, and the composer of stages
[3.1](stage-3.1-lanes.md) and [4](stage-4-align-readiness.md). Today `Layer` holds a separate
`pipeline` value: per-stream stage factories (`normalise`, `align`), the readiness stage, and
`lanes` / `run` to wire them, built per mode by `replayPipeline`. Its providers sit in a separate
slice, initialised, started and closed by `Layer.Start` and `Layer.Close`. Keeping the layer and its
pipeline apart is not the intended shape: the data layer is the pipeline, and each direct stream's
lane runs everything about that stream.

```
lane_i: provider_i ─▶ normalise_i ─▶ (deadline_i, live) ─▶ align_i ─evaluate─┐
lane_j: provider_j ─▶ normalise_j ─▶ (deadline_j, live) ─▶ align_j ─evaluate─┼─ pipe.FanIn ─▶ readiness ─▶ Views ─▶ Next()
                                                                             ┘
```

After this stage the engine initialises the providers; `New` instantiates every stage — one lane per
direct stream, readiness, the aggregator — as fields of the `Layer`; each lane starts its provider
and stages under the layer's run context, and closes the provider when that context is cancelled;
`Start` joins the lanes into readiness. Frames are unchanged.

## Decisions

- **The `Layer` is the pipeline.** It holds its stages as typed fields: `lanes`, `ready`, `agg`.
  `New` instantiates them; `Start` runs the lanes and joins their evaluates into readiness with
  `pipe.FanIn`. No separate composer type, no stage factories.
- **Providers arrive initialised.** The engine calls `Init` on each provider with its key's
  subscription before `data.New` — in `Run`, never at construction, so `--validate-only` stays a dry
  run — as `data_logger` does today. `New` takes each provider's `Streams()`, which performs no I/O.
  The layer starts the providers and closes the ones it started; a provider it never started stays
  the engine's.
- **`New` takes the provider map only.** Its keys are the direct subscriptions, so every subscription
  has exactly one provider by construction: the `subs` argument and the missing-provider error go,
  and the stream table is built from the keys (`NewStreams` sorts them, so map order does not
  matter). A nil provider is an error.
- **`New` keeps the duplicate check.** A map does not stop two keys sharing one instance, and an
  instance initialised twice is bound to its second subscription only. The root cause is the
  composition root, which hands one instance per provider block to every subscription naming it;
  making it create one instance per subscription
  ([multi-subscription.md](../../discussions/multi-subscription.md) §3) is outside this stage. Until
  then `New` rejects one instance under two keys.
- **A lane is one direct stream's chain of stages:** ingest (its provider), normalise, align. `New`
  instantiates stream `i`'s normaliser and aligner separately and gives the lane its provider and
  that provider's outputs.
- **The lane runs under the layer's run context.** `lane.run(ctx)` starts the provider, then chains
  normalise and align over its output, so a lane's stages start only once its provider has. Once the
  provider has started, `context.AfterFunc(ctx, …)` closes it when `ctx` is cancelled — no watcher
  goroutine, so none leaks if the layer is never closed. The lane has no `close` method and no
  started flag: only started providers are registered.
- **`Close` stays synchronous.** `Start` creates the run context and keeps only its `cancel`. `Close`
  cancels it — every running lane closes its provider — stops the gate, drains the frames, then waits
  on the layer's `closing` WaitGroup, which each registered provider's `AfterFunc` marks done once
  the provider's `Close` has returned. Without the wait, a provider that had already finished could be closed after
  `Close` returns. The run context is the layer's own, not derived from the engine's: cancelling the
  engine's would end the lanes and look like the end of the run (`nil, nil`), without the drain.
- **A provider failing to start closes the layer.** The lanes before it are already running: `Start`
  joins them into readiness and closes the layer through `Close`, the one shutdown path, then returns
  the error; `Next` and `Start` return `ErrClosed` from then on. This replaces stage 5's "a start
  failure leaves the layer unstarted". `Init` failures are the engine's (stage 6).
- **The mode is chosen per lane when it is built.** The live lane (iteration 2) gains a deadline
  stage that `lane.run` chains between normalise and align and that stops with the run context;
  there is no composer per mode.
- **No generic pipeline tooling.** The open question of `pipe.Pipeline[In, Out]` with `Lanes` and
  `Then` is answered: the `Layer` is the typed pipeline instance.
- **Remove what earlier stages introduced and this stage leaves without a caller**, in production code
  or tests: `pipeline.go` (`pipeline`, `replayPipeline`, `lanes`, `run`), `Layer.stages` and
  `Layer.providers`, `Start`'s `Init` loop, and the pipeline tests the layer tests already cover
  (`TestPipelineRun`, `TestPipelineSkew`, with their helpers `consumeAll` and `throttled`, and
  `feedAll`, whose callers move to fake providers). Code that predates stage 3 is untouched.
- **Kept:** the normaliser, aligner, gate, readiness and aggregator, unchanged; `Next` and `Close`
  with their contracts; `pipe.Stage` as the stage convention — the normaliser's and aligner's `run`.

## Changes

### Lane — new `internal/engine/data/lane.go`; `pipeline.go` deleted

```go
/*
lane is one direct stream's chain of stages, built by New and run by Start: ingest (its provider),
normalise, then align, one goroutine each. The live lane (iteration 2) adds the deadline stage
between normalise and align.
*/
type lane struct {
	provider  proto.DataProvider // initialised by the engine
	data      <-chan proto.MarketData
	errs      <-chan error
	normalise *normaliser
	align     *aligner
	closing   *sync.WaitGroup // the layer's: done once the provider is closed
}

// newLane builds direct stream s's lane over its initialised provider; its align stage waits on g for
// ring capacity.
func newLane(buf *FrameBuffer, g *gate, s int, p proto.DataProvider, closing *sync.WaitGroup,
	log *logger.Logger) *lane

/*
run starts the lane's provider, then chains normalise and align over its output; it returns the
lane's evaluates and the provider's errors. Once ctx is done the provider is closed. Nothing runs if
the provider fails to start.
*/
func (ln *lane) run(ctx context.Context) (<-chan evaluate, <-chan error, error)
```

### Layer — `internal/engine/data/data.go`

```go
/*
Layer is the data layer of one run, and its pipeline: the stream table, the FrameBuffer, one lane per
direct stream, readiness and the aggregator. New builds it, Start runs it, Next hands the engine one
frame at a time and Close stops it. Next and Close belong to the engine goroutine and must not run
concurrently.
*/
type Layer struct {
	streams *Streams
	buf     *FrameBuffer
	gate    *gate
	lanes   []*lane // per direct stream, in stream-table order
	ready   *readiness
	agg     *aggregator
	log     *logger.Logger

	started, closed bool
	cancel          context.CancelFunc // the run context's; nil until Start
	closing         sync.WaitGroup     // the started providers, until closed
	events          <-chan View        // nil until Start
	errsDone        <-chan struct{}    // closed once every running provider's error channel has
	view            View               // the frame Next returned last
}

/*
New builds the data layer over provider instances keyed by their direct subscription, each already
initialised for it, and instantiates its stages. lookback is the longest read in bars, the same for
every stream. It performs no I/O and starts nothing; Start runs the layer.
*/
func New(providers map[proto.Subscription]proto.DataProvider, lookback int, log *logger.Logger) (*Layer, error)

// newLayer is New with the rings' headroom in columns.
func newLayer(providers map[proto.Subscription]proto.DataProvider, lookback int, headroom int64,
	log *logger.Logger) (*Layer, error)

/*
Start runs each lane under the layer's run context — its provider, then its stages — and joins their
evaluates into readiness; provider errors are logged. A provider failing to start closes the layer,
the lanes already running included, and Start returns its error. A layer runs once: Start again is
an error, and after Close it returns ErrClosed.
*/
func (l *Layer) Start() error
```

- `newLayer` builds the stream table from the map's keys, gives each direct stream the provider under
  its subscription — a nil provider or one instance under two keys is an error naming the
  subscription — and builds `lanes` with `newLane` (taking each provider's `Streams()`), `ready` with
  `newReadiness`, `agg` with `newAggregator`. Still no I/O and no goroutine.
- `Start`: creates the run context, keeping `cancel`; `run` on each lane in order, stopping at the
  first failure; `errsDone` forwards the running lanes' errors and
  `events = ready.run(pipe.FanIn(evs...))` over their evaluates; on a failure, `Close`, then the
  error wrapped with the stream's ID.
- `Close`: marks the layer closed; if it never started, returns; otherwise `cancel()`, `gate.stop()`,
  drains `events`, `closing.Wait()`, `<-errsDone`. `Next` is unchanged.
- The `New` doc comment names the stages and the initialised providers instead of "instantiates its
  pipeline".

### Comments elsewhere

- `normalise.go`, `align.go`: unchanged — they already name "one direct stream's lane".

## Tests

Conventions as before: the shared fixture, `day0`, `bar` / `bars`, `span`, `quiet`. `fakes` returns
fake providers already initialised with their stream's subscription; `fakeProvider` gains `sent`,
counting candles delivered to its output (capacity 100).

### `pipeline_test.go` → `lane_test.go`

- `TestPipelineRun`, `TestPipelineSkew`, `consumeAll`, `throttled` and `feedAll` are removed:
  `TestLayerRun` and `TestLayerSkew` cover the same inputs end to end through `Start` and `Next`, and
  the lane tests feed fake providers. `model` moves to `data_test.go`, its only user.

**`TestLane`** — replaces `TestPipelineLanes`. A layer over the fixture's subscriptions with fake
providers and `headroom` 600, so no align stage waits. Each lane, run under a context, over its
stream's candles — a day of 5m bars with a gap at `day0+50` and an invalid bar at `day0+60`, 1h bars
ending at 11:00, one 1d bar — returns exactly its own stream's evaluates: `Fresh` at the accepted
columns, `Gap` at `day0+50` and `day0+60`, then `Ended` one period after the last, and closes. Its
provider is started once and not closed until the context is cancelled; after cancelling,
`closing.Wait()` returns with each provider closed once.

**`TestLaneIndependent`** — replaces `TestPipelineLanesIndependent`. `headroom` 1200, so the 1h
ring (126 slots) never makes align wait. All three lanes run; the 1h lane's evaluates are never read,
its provider holding 1000 bars. The 5m and 1d lanes run to their `Ended`. The 1h provider's `sent`
settles at exactly `100 + 2·laneBuffer + 2` — its own full output, normalise's held candle and full
output, align's held item and full output — checked after a 50 ms settle. Draining the 1h lane
afterwards, consuming each column as the engine would — its ring cannot hold 1000 bars otherwise —
brings `sent` to 1000 and lets every goroutine exit; the context is then cancelled.

### `data_test.go`

- `model` moves here.
- **`TestNew`** takes the map only: "a subscription without a provider" becomes **a nil provider**,
  an error naming the subscription; one instance under two keys and the 365-day cap stay errors;
  "built, not run" holds the stream table from the keys, derived streams included, and no provider is
  started or closed.
- **`TestLayerStart`**:
  - the `Init` failure case is removed — `Init` is the engine's;
  - a `Start` failure on the 5m lane returns an error naming it; the 1h lane before it has its
    provider closed once, the 5m provider is not closed and the 1d one never started; `Next` and
    `Start` then return `ErrClosed`, and `Close` touches nothing more;
  - `Start` twice is an error; `Start` after `Close` returns `ErrClosed`.
- **`TestLayerClose`**: unchanged — every provider closed exactly once when `Close` returns, in both
  runs; also after a run that ended on its own (`TestLayerRun`'s input, every frame read, then
  `Close`).
- `startLayer` builds the layer from the map alone; `TestLayerReference` initialises its
  `binance_csv` providers with their subscriptions before `New`. The other `TestLayer*` tests are
  unchanged.

## Documentation updates

- [stage-5-consumption.md](stage-5-consumption.md): a note under the title — partly superseded by
  this plan (the `pipeline` composer, `Layer.stages`, `Layer.providers`, `New`'s `subs`, `Init` in
  `Start`, a start failure leaving the layer unstarted, the pipeline tests).
- [main.md](main.md):
  - Decisions taken by this plan, pipeline construction: "(decided in stages 3, 3.1 and 4, revised in
    5.1) Stages follow `pipe.Stage`. The `Layer` is the pipeline: `New` instantiates a lane per
    direct stream — its provider, normaliser and aligner — readiness and the aggregator; each lane
    starts its provider, then its stages, under the layer's run context, and closes the provider when
    it is cancelled; `Start` joins the lanes' evaluates into readiness with `pipe.FanIn`. The mode is
    chosen per lane. No package-level instance and no generic pipeline tooling."
  - Decisions taken by this plan, module API: "`New` instantiates the layer and its pipeline" →
    "`New` takes the provider map — keyed by direct subscription, providers initialised by the engine
    — and instantiates the layer's stages"; `Start`: a provider start failure closes the layer;
    `Close` cancels the run context and waits for the providers it closes.
  - Stage 6 entry: `data_logger_v2` initialises each provider with its subscription, then passes its
    `_providers` map to `data.New`; an `Init` failure is the engine's error.
  - Structure: `stage-5.1-layer-pipeline.md` — *implemented*, after stage 5. Ordering: 5.1 on 5; 6 on
    5.1.
  - Iteration 1 diagram: the provider inside lane `i` — `lane_i: provider_i ─▶ normalise ─▶ align`.
  - Stage 5 entry: "Partly superseded by stage 5.1 (the composer, `New`'s arguments, provider
    lifecycle, start failures)". New stage 5.1 entry matching this doc, marked implemented: Goal,
    Touches (`lane.go` new, `pipeline.go` deleted, `data.go`, tests), Contents (the decisions above),
    Acceptance (frames unchanged; each lane carries only its own stream's evaluates; an unread lane
    stops only its own provider; providers closed exactly once by `Close`; start failures as above),
    Plan.
  - Out of scope: one provider instance per subscription at the composition root — until then `New`
    rejects a shared instance.
- [data-layer-v2.md](../../discussions/data-layer-v2.md):
  - § Module and stages: "In: `Subscription`s and provider instances" → "In: provider instances,
    initialised by the engine and keyed by their direct subscription".
  - § Module and stages: "Stage order" names the per-stream stages a lane; "One unexported composer
    (`pipeline`) wires the stages in one place, per mode" → **The `Layer` is the pipeline**: `New`
    instantiates a lane per direct stream (provider, normalise, align) plus readiness and the
    aggregator; each lane starts its provider, then its stages, under the layer's run context and
    closes the provider when it is cancelled; `Start` joins the lanes into readiness; the mode is
    chosen per lane — the live lane adds the deadline stage; no package-level instance, no generic
    pipeline tooling.
  - § Deadline: "The replay composer leaves it out" → "The replay lane leaves it out"; the deadline
    stage stops with the run context.
  - § Consumption, lifecycle: the engine initialises the providers; `Start` runs the lanes; a provider
    start failure closes the layer; `Close` cancels the run context, stops the gate, drains, and
    waits for the providers to close.
  - Solution § Pipeline: the diagram's per-stream band is labelled lane `i`, from provider to align.
  - Solution § Events, module API: `New(providers, lookback, log)`.
  - Open Questions: "Generic pipeline tooling" removed — answered by the `Layer`.
  - WIP: stage 5.1 implemented — `docs/plans/data-layer-v2/stage-5.1-layer-pipeline.md`.
- [multi-subscription.md](../../discussions/multi-subscription.md): the shared-instance row notes that
  `data.New` rejects one instance under two subscriptions until per-sub materialisation lands.
- `docs/Architecture.md`: `internal/engine/data` — the `Layer` is the pipeline: per-stream lanes
  (provider, normalise, align), readiness joined with `pipe.FanIn`, rolling aggregation; API `New`,
  `Start`, `Next`, `Close`; providers arrive initialised.
- [README.md](../../discussions/README.md): data-layer-v2 row → stages 1–5.1 implemented.

## Out of scope

- One provider instance per subscription at the composition root (`cmd/instantiate.go`,
  `cmd/wire.go`; [multi-subscription.md](../../discussions/multi-subscription.md) §3).
- Initialising providers in `data_logger_v2` — stage 6.
- The deadline stage and the live lane — iteration 2.
- Changes to the normaliser, aligner, gate, readiness, aggregator or the `Next` / `Close` contracts.
- Generic pipeline tooling in `flow/pipe`.

## Verification

- `make check` clean.
- `go test -race ./internal/engine/data/... ./internal/flow/...` clean.
- `go test -count=1 -run Reference -v ./internal/engine/data/` runs, not skipped, locally.
- `make validate` passes on `config.json`.
- No reference to `pipeline`, `replayPipeline`, `Layer.stages` or `Layer.providers` remains in
  `internal/engine/data`, and the layer calls no provider's `Init`.
- Nothing stages 3–5 introduced is left without a caller in production code or tests.
