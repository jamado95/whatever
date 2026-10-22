# Stage 3.1 — per-stream lanes (data layer v2, iteration 1)

## Context

Adjusts [stage 3](stage-3-lanes.md), as built, to the 2026-10-22 redesign in
[data-layer-v2.md](../../discussions/data-layer-v2.md) (Decisions § Alignment, § Deadline, § Release,
§ Reversed). Stage 3 merged every lane into one stream ordered by (column, stream) for a single
aligner. The redesign drops the ordered merge: each direct stream gets a lane — a chain of per-stream
stages, one goroutine each — and readiness joins the lanes by counting evaluates.

```
replay: provider_i ─▶ normalise ─▶ align ─evaluate─┐
live:   provider_i ─▶ normalise ─▶ deadline ─▶ align ─evaluate─┼─▶ readiness ─▶ events
        └──────────── lane i: one goroutine per stage ────────┘      (stage 4)
```

This stage leaves the lane holding only normalise and returns one output per lane. Align and
readiness are stage 4, the deadline stage iteration 2.

## Decisions

- **Keep:** the normalise rules and their tests, the drop counters and logging, `Streams.column`, the
  `binance_csv` µs fix, `pipe.Stage`, and the heap `OrderedFanIn` with its model test and benchmark —
  `SynchronizerV2` still merges with it.
- **Delete the ordered merge from the module:** `order.go` (`before`, `orderLanes`) and the
  merged-order tests. The module no longer calls `OrderedFanIn`.
- **Remove what stage 3 introduced and this stage leaves without a caller**, in production code or
  tests: `order.go` (`before`, `orderLanes`), `lane.end` (the end marker), the test helper
  `firstDiff`, and `pipe.Merge`, whose only user was the merge — stage 4 adds what readiness's join
  needs. Code that predates stage 3 is untouched.
- **Normalise is one stage of the lane, not the lane.** `lane` becomes `normaliser` and `lane.go`
  becomes `normalise.go`, so align (`align.go`) and the deadline stage (`deadline.go`) sit beside it
  as siblings. `laneItem` keeps its name: it is what flows between a lane's stages.
- **No end marker.** A lane's end is its channel closing; align (stage 4) writes `Ended` at
  `lastCol + Period` from its own state, or reports a stream that never started. The −1 convention
  goes with it.
- **`laneItem` carries column and candle only.** A lane is one stream, so the stream index is implied
  by the channel and known to every stage built for it. The deadline stage adds a `Pending` kind in
  iteration 2.
- **The composer returns one output per lane.** `pipeline.normalise` builds a lane's first stage;
  `run` starts one lane per direct stream and returns their outputs, `[]<-chan laneItem`, indexed
  like the direct streams. Stage 4 chains align onto each lane and joins them with readiness; live
  inserts the deadline stage. Generic pipeline tooling (`pipe.Pipeline`, `Lanes`, `Then`) stays an
  open question.
- **Lanes are independent.** An unread lane blocks only its own provider, once its buffer is full;
  the others run on. This is the property per-stream headroom relies on, tested here at the
  normalise stage.

## Changes

### Normalise — `internal/engine/data/lane.go` → `normalise.go`

```go
// laneItem is one value on a lane, between its per-stream stages: a candle at its column.
type laneItem struct {
	col int64        // the column the candle closes in
	c   proto.Candle // validated, ms timestamps
}

// normaliser is the normalise stage of one direct stream's lane; only its goroutine touches its state.
type normaliser struct {
	streams *Streams
	stream  int
	tfMs    int64
	last    proto.Candle // last accepted candle
	lastCol int64        // its column; −1 before the first
	drops   [numDrops]int
	log     *logger.Logger
}

func newNormaliser(streams *Streams, stream int, log *logger.Logger) *normaliser

// run normalises in: accepted candles at their columns; once in closes, a summary line if anything
// was dropped, then the output closes. It is a pipe.Stage.
func (n *normaliser) run(in <-chan proto.MarketData) <-chan laneItem

func (n *normaliser) accept(c proto.Candle) (int64, drop) // unchanged rules
```

- `end()` is removed.
- `laneBuffer` keeps its value (100); it is the output capacity of each lane stage.
- The summary line reads "normalise dropped candles". The first-drop warning, `drop`, `msLimit`,
  `spanTolerance`, `valid` and `finite` are unchanged.

### Ordered merge — delete `internal/engine/data/order.go`

### Pipe — `internal/flow/pipe/pipeline.go`

`Merge` is deleted; `Stage` and the existing combinators stay. `fanout.go` is unchanged.

### Composer — `internal/engine/data/pipeline.go`

```go
/*
pipeline is the data layer's stage graph, wired in one place. Each direct stream gets a lane: a
chain of per-stream stages, one goroutine each. Stage 4 chains align onto each lane and joins the
lanes with readiness; the live pipeline (iteration 2) inserts the deadline stage; stage 5 wraps the
pipeline in the module API.
*/
type pipeline struct {
	normalise func(stream int) pipe.Stage[proto.MarketData, laneItem]
}

// replayPipeline builds the replay stages: each lane is normalise only, until stage 4.
func replayPipeline(streams *Streams, log *logger.Logger) pipeline

// run starts one lane per direct stream; ins[i] is direct stream i's provider output, and the
// result's i-th channel is its lane's output.
func (p pipeline) run(ins []<-chan proto.MarketData) []<-chan laneItem
```

## Tests

Same conventions and fixture as stage 3: the stage 2 shared fixture, Binance-style candles from
2024-01-01 00:00 UTC, loggers discarding output except in the logging subtest.

### `lane_test.go` → `normalise_test.go`

- `TestLaneAccept` → **`TestNormaliseAccept`**, unchanged apart from the names.
- `TestLaneRun` → **`TestNormaliseRun`**:
  - accepted candles in input order, then the output closes — no marker;
  - drops at the tail: the output holds only the accepted candles;
  - nothing accepted (empty input, or every candle dropped): the output closes with nothing on it;
  - counters per rule and logging, unchanged apart from the summary text.
- Helpers `runLane` → `runNormaliser`; `bar`, `bars`, `quiet`, `day0` unchanged.

### `pipeline_test.go`

`TestPipelineOrder`, `TestPipelineBackpressure` and `firstDiff` are removed with the merge;
`feedAll` stays for the new tests.

**`TestPipelineLanes`**
- One lane per direct stream: one day of 5m candles with a gap and an invalid candle, 1h candles
  ending at 11:00, one 1d candle. Lane `i` carries exactly stream `i`'s accepted candles, in order,
  then closes.
- No direct streams: `run(nil)` returns no lanes.

**`TestPipelineLanesIndependent`**
- The 1h lane's output is never read; its provider feeds 1000 bars into an unbuffered channel,
  counting sends. The 5m and 1d lanes drain completely. The 1h sends settle at exactly
  `laneBuffer + 1` — the full buffer plus the candle held by its normalise goroutine — checked after a
  50 ms settle. Draining the 1h lane afterwards lets every goroutine exit.

## Documentation updates

- [stage-3-lanes.md](stage-3-lanes.md): a note under the title — partly superseded by this plan
  (ordered merge, end marker, merged-order tests).
- [main.md](main.md):
  - Context and Decisions carried in: stages become per stream — ingest → normalise → deadline
    (live) → align, then readiness (ready → commit) → materialise; the `OrderedFanIn` bullet is
    replaced by per-stream staging within per-stream headroom, gaps proven by each stream's own next
    candle, readiness counting evaluates and a deadline stage per lane in live; align stages, not a
    single data layer goroutine, write slots beyond the released head; a stream's align stage blocks
    when its headroom runs out.
  - The iteration 1 diagram follows the pipeline above.
  - Structure: `stage-3.1-lanes.md` after stage 3; `stage-4-release.md` becomes
    `stage-4-align-readiness.md`. Ordering: 3.1 on 3; 4 on 2 and 3.1.
  - Stage 3 entry: a pointer to 3.1. New stage 3.1 entry matching this doc, marked implemented.
  - Stage 4 re-scoped — align and readiness:
    - **Goal:** per-stream align stages and one readiness stage turn the lanes into in-order
      `Current` frame events.
    - **Touches:** new `align.go`, `readiness.go`, `event.go`; `pipeline.go` chains align and joins
      with readiness.
    - **Contents:** align stages `Fresh`, `Gap` and `Ended` slots within per-stream headroom, takes
      the carried candle from its own state, reports before blocking and sends an evaluate per
      resolved column; readiness keeps per-column counts against the streams due there and releases
      decided columns in order with completeness by last due candle; the engine's consumption wakes
      blocked align stages. Evaluate delivery (`FanIn` against per-column atomic counters) is
      benchmarked first.
    - **Acceptance:** table tests over injected evaluates, with the same frames under any arrival
      order; a coarse stream between closes is not waited on; a missing coarse bar is `Gap` and its
      hour `Partial`; a stream not yet started makes frames `Partial`; an ended lane leaves the
      expected set; a gap is proven by the stream's own next candle; the last column is released once
      every lane has ended; per-stream blocking without deadlock under skew; readiness benchmark at
      N = 1000.
  - Stage 5: "with the align stages staging ahead" in place of the aligner.
  - Iteration 2: the deadline stage per lane (one timer at its next due column's `boundary +
    lateness`, `Pending` markers) replaces the release-loop clock trigger, `FanIn` arrival order and
    the `Watermark` type; out-of-order candles become `Late` instead of normalise dropping them.
  - Iteration 3: startup history is bounded by per-stream headroom, with providers blocking rather
    than dropping while they deliver it — not routed through an ordered merge.
  - Open items: evaluate delivery and headroom wake-up — stage 4.
  - Iteration 1 "done when": the `OrderedFanIn` and readiness benchmarks.
  - Decisions taken by this plan, pipeline construction: stages follow `pipe.Stage`; `pipe.Merge` is
    dropped.
- [data-layer-v2.md](../../discussions/data-layer-v2.md):
  - Decisions § Module and stages: the stage convention names `pipe.Stage` only; the `pipe.Merge`
    fan-in goes.
  - Decisions § Normalisation: lane items carry column and candle; the lane is the stream.
  - WIP: stage 3.1 implemented, referencing this plan; the stage 3 line of "Plan impact" resolved and
    the note that `main.md` is not yet updated removed.
- [pipeline-runtime.md](../../discussions/pipeline-runtime.md):
  - § Clock: the replay watermark becomes each stream's own progress, counted by readiness — no
    global order; the `Watermark` interface and its `DataWatermark` / `WallWatermark` comment are
    replaced by the deadline stage per lane on the injected `Clock`; "the aligner never reads
    `time.Now()`" becomes "only the deadline stage reads the clock".
  - Backtest versus live table: ordering — none, per-stream headroom (replay) / arrival order with
    a deadline stage per lane (live); backpressure and buffers refer to align stages and readiness,
    not the aligner.
  - The runtime pipeline sketch and its "Alignment" step name align per stream and readiness.
- `docs/Architecture.md`: `internal/engine/data` — stream table, `FrameBuffer` and the per-stream
  lanes so far; align, readiness and the module API as built per `docs/plans/data-layer-v2`. The
  design rule on pipeline stages names `pipe.Stage` only.
- [README.md](../../discussions/README.md): data-layer-v2 row → stages 1–3.1 implemented.

## Out of scope

- Align, readiness, evaluate delivery, headroom waits and wake-up, events — stage 4.
- The deadline stage, `Pending` markers and `Late` handling of out-of-order candles — iteration 2.
- Generic pipeline tooling in `flow/pipe`.
- Changes to `OrderedFanIn`, `SynchronizerV2` or `data_logger`.

## Verification

- `make check` clean.
- `go test -race ./internal/engine/data/... ./internal/flow/...` clean.
- `make validate` passes on `config.json`.
- No reference to `orderLanes`, `before` or the end marker remains in `internal/engine/data`.
- Nothing stage 3 introduced is left without a caller in production code or tests.
