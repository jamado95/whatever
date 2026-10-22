# Stage 4 — align and readiness (data layer v2, iteration 1)

## Context

Stage 4 of [main.md](main.md): per-stream align stages and one readiness stage turn the normalise
lanes of [stage 3.1](stage-3.1-lanes.md) into in-order `Current` frame events, staging every direct
stream into the [stage 2](stage-2-framebuffer.md) `FrameBuffer`. Design in
[data-layer-v2.md](../../discussions/data-layer-v2.md) (§ Alignment, § Release, § Capacity), refined
here where noted.

```
provider_i ─▶ normalise ─▶ align_i ─evaluate─┐
provider_j ─▶ normalise ─▶ align_j ─evaluate─┼─ pipe.FanIn ─▶ readiness ─▶ events ─▶ (engine, stage 5)
                              │               ┘                   │
                              └─ slots ─▶ FrameBuffer ◀─ View ─────┘ completeness
                              ▲
                              └── gate: waits on the engine's consumed column for ring capacity
```

Replay only; the deadline stage, `Pending` and `Late` are iteration 2. `data.New`, `Next` and the
engine's consumption are stage 5; here tests drive consumption directly.

## Decisions

- **Readiness keeps per-stream progress, not per-column counts.** `through[s]`: every due column of
  direct stream `s` up to it is decided. On each evaluate readiness checks `head+1` — decided once
  every stream due there (`(head+1) % Period == 0`) has `through ≥ head+1` — and releases while that
  holds. The check resumes at the stream that blocked it last: an evaluate from any other stream
  cannot change the outcome and costs O(1); each column costs O(N) in total.
- **Completeness is read from the `View`** pinned at the column being released: `Complete` when every
  direct stream's `Leg` is `Fresh` or `Ended`. A stream not yet started reads `Gap` (`Partial`); a
  stream that ended without a candle is skipped. Safe: every slot it reads has been evaluated.
- **Evaluates reach readiness through `pipe.FanIn`.** Align stays a `pipe.Stage`; no per-column
  atomic counters. The readiness benchmark measures align → `FanIn` → readiness end to end.
- **Ring capacity replaces the headroom check.** Align writes column `x` when it lands on a slot that
  is empty or older than the oldest bar a view may still read:
  `x ≤ max(f, latest(consumed) − (bars − 1)·Period) + (len(slots) − 1)·Period`, `f` the stream's first
  column, `latest(c) = c − c % Period`. Equivalently: `x ≤ f + (len(slots) − 1)·Period`, or
  `consumed ≥ x − spare·Period` with `spare = len(slots) − bars`. At startup a stream fills its whole
  ring; in steady state it writes up to `consumed + spare·Period`. Headroom only sizes the ring.
- **The first candle never waits** (`f ≤ f + (len(slots) − 1)·Period`), so its `Fresh` evaluate also
  decides the columns before `f`: no separate "not started before `f`" report. Gaps are still written
  one due column at a time, each after its own capacity wait, then reported.
- **Reads stop at a stream's first column.** Filling the ring at startup wraps onto the slots of
  columns before `f`, which views still probe — a stream not yet started, a history walk past its
  first bar. The ring records its first column (`start`, before the first `put`); `Leg`, `Count` and
  `Candles` never touch slots below it.
- **The gate** holds the engine's consumed column — the column of the view it is processing; views at
  earlier columns are no longer read. Align stages wait on it (`sync.Cond`); `consume(col)` sets it
  and broadcasts. Stage 5's `Next` calls `consume`; −1 before the first column.
- **`put` unchanged.** A `Gap` or `Ended` slot carries the candle of the slot at `col − Period` — the
  align stage's own previous write, which no one else writes in iteration 1. Iteration 2's `Late`
  writes revisit it.
- **`defaultHeadroom` = 128 columns**, a package constant; the event channel holds as many. Readiness
  blocking on a full event channel costs throughput only: the engine's consumption depends on events
  alone.
- **Every column from the first to the last is released**, including columns where no stream is due
  (only coarse streams left): their legs carry. The first column is the earliest first candle across
  streams, known once every stream has reported.
- **A column is released only once some stream has written at or beyond it** — `reach`, the latest
  `Fresh` or `Gap` column across streams. Without it, columns where no open stream is due would be
  released past the last data in some arrival orders (5m and 1h ended, the 1d stream's next due
  column a day away). With it, columns between coarse closes wait for the next bar, and the run ends
  at the latest `Fresh` column in every order. `Gap` counts so that a gap across every stream, longer
  than the ring's spare slots, cannot deadlock; an open stream's next due column always fits once the
  engine has consumed the head (`spare ≥ 1`).
- **`Event` carries `Status`, `Complete` and `Col`.** `Current` is the only status; `Late` and its
  `Legs` arrive in iteration 2, `Backfill` in iteration 3.

## Changes

### Events — new `internal/engine/data/event.go`

```go
// Status is a frame event's kind. Late (iteration 2) and Backfill (iteration 3) follow.
type Status uint8

const Current Status = iota // the newest column, released for the first time

// Event is a released column; the engine reads it through a View pinned at Col.
type Event struct {
	Status   Status
	Complete bool  // every active direct stream's latest due candle at or before Col is present
	Col      int64 // absolute column: boundary ms / Stride
}

/*
evaluate tells readiness that a direct stream has decided its due columns up to col: Fresh or Gap at
col, or Ended from col on — col −1 when the lane ended without a candle.
*/
type evaluate struct {
	stream int
	col    int64
	state  LegState
}
```

### Buffer — `internal/engine/data/buffer.go`, `view.go`

```go
// defaultHeadroom sizes each ring's slots beyond its kept bars, in columns; also the event channel's capacity.
const defaultHeadroom = 128

type ring struct {
	slots []slot
	bars  int64        // bars kept for reads; the slots beyond them are headroom
	first atomic.Int64 // first column; reads never go below it. 0 until start
	end   atomic.Int64 // Ended column; math.MaxInt64 while open
}

// start records stream s's first column, before its first put.
func (b *FrameBuffer) start(s int, col int64)
```

- `View.latest` returns nil for a column below the ring's first, without reading a slot; `Candles`
  stops at the first column (in place of column 0). With `first` 0 by default, existing callers and
  tests behave as before.
- Comments naming "the aligner" (`FrameBuffer`, `newFrameBuffer`, `Candles`) name the align stages and
  ring capacity instead. `put` is unchanged.

### Align — new `internal/engine/data/align.go`

```go
/*
gate holds the engine's consumed column: the column of the view it is processing. Align stages wait
on it for ring capacity; the engine's consumption (stage 5) wakes them.
*/
type gate struct {
	mu       sync.Mutex
	cond     sync.Cond
	consumed int64 // −1 before the first column
}

func newGate() *gate

// wait blocks until the engine has consumed col.
func (g *gate) wait(col int64)

// consume moves the engine to col — views before it are no longer read — and wakes the waiting align stages.
func (g *gate) consume(col int64)

// aligner is the align stage of one direct stream's lane; only its goroutine touches its state.
type aligner struct {
	buf    *FrameBuffer
	gate   *gate
	stream int
	period int64
	slots  int64 // ring length
	spare  int64 // ring slots beyond the kept bars
	first  int64 // first Fresh column; −1 before
	last   int64 // last Fresh column
}

func newAligner(buf *FrameBuffer, g *gate, stream int) *aligner

/*
run stages each candle at its column: a Gap slot at every due column it skipped, then the Fresh slot,
each written once ring capacity allows and followed by its evaluate. Once in closes it writes Ended at
last + Period, or sends Ended at −1 for a lane that accepted no candle, then closes its output. It is
a pipe.Stage.
*/
func (a *aligner) run(in <-chan laneItem) <-chan evaluate

// write waits for ring capacity at col, puts the slot and sends its evaluate.
func (a *aligner) write(out chan<- evaluate, col int64, c proto.Candle, state LegState)
```

- `write` waits only when `col > first + (slots − 1)·period`, then on `gate.wait(col − spare·period)`.
- On the first candle: `first = col`, `buf.start(stream, col)`, then `write`.
- Slots are written with count 0 (unused for direct streams). Output capacity `laneBuffer`.

### Readiness — new `internal/engine/data/readiness.go`

```go
/*
readiness releases columns in order. It keeps each direct stream's progress, through: every due
column up to it is decided. On each evaluate it checks head+1 — decided once every stream due there
has through ≥ head+1, and released once some stream has also written at or beyond it — resuming at
the stream that blocked the last check. A released column is Complete when every direct stream's leg
in the View pinned there is Fresh or Ended; streams that ended without a candle are skipped.
*/
type readiness struct {
	buf        *FrameBuffer
	through    []int64 // per direct stream; MinInt64 before its first evaluate, MaxInt64 once ended
	empty      []bool  // ended without a candle: never in the expected set
	unreported int     // direct streams yet to send their first evaluate
	start      int64   // earliest first column across streams
	reach      int64   // latest Fresh or Gap column across streams
	head       int64   // last released column
	cursor     int     // direct streams below it are decided at head+1; all of them: waiting on reach
}

func newReadiness(buf *FrameBuffer) *readiness

// step applies one evaluate and appends the events it releases to dst.
func (r *readiness) step(e evaluate, dst []Event) []Event

// run is the readiness stage, a pipe.Stage over the joined evaluates; output capacity defaultHeadroom.
func (r *readiness) run(in <-chan evaluate) <-chan Event
```

`step`, in order:

1. A stream's first evaluate: `unreported−−`; `Fresh` lowers `start`; `Ended` marks it `empty`.
2. `Fresh` / `Gap`: `through = col`, `reach = max(reach, col)`. `Ended`: `through = MaxInt64`.
3. Nothing is released while `unreported > 0`. The evaluate that completes the reports sets
   `head = start − 1`; if no stream started, nothing is ever released.
4. Scan only when the evaluate's stream is `cursor`, when every stream is decided at `head+1` and it
   waits on `reach`, or when step 3 just set `head`. Advance `cursor` past the streams decided at
   `head+1`; once all are and `head+1 ≤ reach`, release `head+1`, advance `head`, reset `cursor` to 0
   and repeat.

The output closes once the input does: every lane has ended, so every column up to `reach` — the
latest `Fresh` column by then — has been released.

### Composer — `internal/engine/data/pipeline.go`

```go
type pipeline struct {
	normalise func(stream int) pipe.Stage[proto.MarketData, laneItem]
	align     func(stream int) pipe.Stage[laneItem, evaluate]
	ready     pipe.Stage[evaluate, Event]
}

// replayPipeline builds the replay stages over buf; align stages wait on g for ring capacity.
func replayPipeline(buf *FrameBuffer, g *gate, log *logger.Logger) pipeline

// lanes starts each direct stream's lane up to normalise; ins[i] is direct stream i's provider output.
func (p pipeline) lanes(ins []<-chan proto.MarketData) []<-chan laneItem

// run chains align onto each lane, joins their evaluates with pipe.FanIn and returns readiness's events.
func (p pipeline) run(ins []<-chan proto.MarketData) <-chan Event
```

The stream table comes from `buf`. The doc comment drops "until stage 4".

## Tests

Conventions as before: the stage 2 shared fixture (5m grid; BTCUSDT 5m, BTCUSDT 1h, ETHUSDT 1d),
`day0`, `bar` / `bars`, `span`, `quiet`. With `lookback` 20 and `headroom` 4 the rings are: 5m 25
slots (20 bars, spare 5), 1h 27 (25 bars, spare 2), 1d 22. Tests that must not wait use a large
headroom.

### `align_test.go`

**`TestGate`**
- `wait` returns at once when consumed ≥ col.
- Goroutines waiting at different columns: `consume(c)` releases exactly those with col ≤ c.

**`TestAlign`** — one stream at a time, rings large enough never to wait; evaluates and slots:
- consecutive candles: `Fresh` per column, then `Ended` at last + Period;
- a skipped 5m column: `Gap` there, carrying the previous candle, before the next `Fresh`;
- a skipped 1h bar: one `Gap` at its due column only;
- a gap of several due columns: one `Gap` each, in order;
- no candle: a single `Ended` at −1, no slot written, the ring's first column unset;
- the first candle sets the ring's first column.

**`TestAlignCapacity`** — `headroom` 4, nothing consumed unless stated:
- startup: 5m candles from `day0` on; align writes exactly `day0 … day0+24` (25 slots) and stops;
  `consume(day0+19)` changes nothing; `consume(day0+20)` releases `day0+25` only; `consume(day0+30)`
  reaches `day0+35`.
- coarse: 1h from `day0`; 27 bars up to `day0+312`; `consume(day0+299)` nothing, `consume(day0+300)`
  `day0+324`, `consume(day0+311)` nothing more, `consume(day0+312)` `day0+336`.
- long gap: 5m candles at `day0` and `day0+100`; `Fresh` and 24 `Gap` evaluates arrive before align
  blocks at `day0+25`; consuming to `day0+95` lets the rest through up to `Fresh day0+100`; the lane
  then closes and `Ended day0+101` waits for `consume(day0+96)`.
- Evaluates are checked after a 50 ms settle, as in stage 3.1.

### `view_test.go`, `buffer_test.go`

**`TestViewFirst`** — the 5m stream starts at `f` and fills its ring, wrapping onto the slots of
columns before `f`: `Leg` before `f` is a zero candle, `Gap`, −1; `Count` 0; `Candles` empty before
`f` and stops at `f` after it.

**`TestFrameBufferStartup`** — like `TestFrameBufferConcurrency`: a writer starts the 5m stream at `f`
and fills the ring, publishing each column; once the first is published, the reader checks views
before `f` and at published columns. Meaningful under `-race`: without the first-column bound, reads
before `f` touch slots being written.

### `readiness_test.go`

**`TestReadiness`** — real aligners stage each stream's candles into a large-headroom buffer and their
evaluates are collected per stream; readiness then steps them in several orders — stream by stream,
reversed, round-robin and 20 seeded random merges keeping each stream's order — and every order
yields the same events (columns relative to `day0`):

| Scenario | Candles | Events |
|---|---|---|
| aligned | 5m `0…288`, 1h `0…288` by 12, 1d `0, 288` | `0…288` Complete |
| missing coarse bar | aligned, 1h without `24` | `24…35` Partial, the rest Complete |
| gap proven by the stream's next candle | aligned, 5m without `50` | `50` Partial, the rest Complete |
| streams not yet started | 5m from `1`, 1h from `12`, 1d `288` | `1…287` Partial, `288` Complete |
| lanes ending early | aligned, 1h to `120`, 5m to `200` | `0…288` Complete, `201…287` with no stream due |
| a stream that never starts | aligned, 1d none | `0…288` Complete |
| no stream starts | none | no events |

**`TestReadinessHold`** — evaluates stepped by hand over slots written with `put` and `start`:
- 5m `Fresh 0…11` and 1h `Fresh 0`: nothing, 1d has not reported;
- 1d `Fresh 0`: releases `0…11` — the 1h stream is not waited on between closes;
- 5m `Fresh 12…20`: nothing — `12` waits on the 1h stream, and decided columns behind it wait too;
- 1h `Fresh 12`: releases `12…20` at once;
- 5m `Ended 21`: nothing — `21…23` have no stream due and wait for a bar beyond them;
- 1h `Fresh 24`: releases `21…24`.

**`BenchmarkReadiness`** — `n` = 10, 100, 1000 direct 1m streams; each align stage's input is a
`laneItem` channel pre-filled with `b.N + 1` consecutive candles and closed before the timer starts;
aligners → `pipe.FanIn` → readiness; the loop receives `b.N` events and calls `consume` on each.
Reports ns per column and ns per evaluate (`b.ReportMetric`); remaining events are drained after the
timer.

### `pipeline_test.go`

- `TestPipelineLanes` and `TestPipelineLanesIndependent` call `lanes`; `replayPipeline` takes the
  fixture's buffer and a gate.
- **`TestPipelineRun`** — the `TestPipelineLanes` input (a day of 5m with a gap at `day0+50` and an
  invalid bar at `day0+60`, 1h to 11:00) with two 1d bars, at `day0` and `day0+288`, so frames mix
  `Partial` and `Complete`; `headroom` 4, consuming each event as it arrives: events cover
  `day0 … day0+288` once each, in order; `Complete` matches a brute-force model over the accepted
  candles.
- **`TestPipelineSkew`** — ten days of 5m, 1h and 1d bars, `headroom` 4: the 1h and 1d lanes fed at
  once and the 5m lane from a throttled provider, then the reverse. Both runs release every column
  exactly once, in order, with the same events, within a 10 s deadline (a deadlock fails).

## Documentation updates

- [main.md](main.md):
  - Decisions carried in, § Capacity: "a stream's align stage blocks when its ring is full".
  - Structure: `stage-4-align-readiness.md` — *implemented*.
  - Stage 4 entry ✅ implemented. Touches add `buffer.go` / `view.go` (first column) and `pipe.FanIn`;
    Contents: per-stream progress with resuming scan, `View` completeness, ring-capacity limit from the
    oldest readable bar, the gate, `defaultHeadroom`; the readiness benchmark is end to end. Plan:
    `stage-4-align-readiness.md`.
  - Stage 5: `Next` calls `gate.consume`; `Close` releases align stages waiting on the gate.
  - Open items: headroom value, event channel capacity, evaluate delivery and wake-up resolved by
    stage 4.
- [data-layer-v2.md](../../discussions/data-layer-v2.md):
  - § Period: "readiness stays a per-column count" → readiness checks the streams due at the column;
    skew: an align stage blocks once its ring is full.
  - § Module and stages: readiness as per-stream progress in the carried-over ideas.
  - § Alignment: the first candle never waits, so its `Fresh` evaluate decides the columns before it
    (the start report goes); "Headroom is per stream" → ring capacity from the oldest readable bar.
  - § Release: readiness keeps per-stream progress, checks the next column on each evaluate resuming
    at the blocking stream, and reads completeness from the `View`; a column is released only once
    some stream has written at or beyond it, so the run ends at the latest `Fresh` column.
  - § Capacity: headroom sizes each ring's spare slots; align's limit is the ring's capacity from the
    oldest readable bar — at startup the whole ring.
  - § FrameBuffer: reads stop at a stream's first column; per-column counts removed.
  - Solution: the pipeline table's align and readiness rows; § Events code block (`Event` without
    `Legs` until iteration 2, `evaluate`; the start-report note removed); § FrameBuffer `ring` with
    `first`; "Headroom keeps staging off readable history" → ring capacity.
  - Open Questions: evaluate throughput and headroom wake-up removed (resolved, with the benchmark
    result); headroom size keeps only the live burst sizing (iteration 3).
  - WIP: stage 4 implemented — `docs/plans/data-layer-v2/stage-4-align-readiness.md`; the stage 4
    "Plan impact" bullet removed.
- [pipeline-runtime.md](../../discussions/pipeline-runtime.md): backtest backpressure — an align stage
  with a full ring blocks its provider; skew bounded by per-stream ring capacity.
- `docs/Architecture.md`: `internal/engine/data` — stream table, `FrameBuffer`, per-stream lanes
  (normalise, align) and readiness; the module API as built per `docs/plans/data-layer-v2`.
- `docs/Commands.md`: `go test -run '^$' -bench Readiness -benchmem ./internal/engine/data/` —
  align → `FanIn` → readiness at 10, 100 and 1000 1m streams, per released column.
- [README.md](../../discussions/README.md): data-layer-v2 row → stages 1–4 implemented.

## Out of scope

- `data.New`, `Next`, `Close` and stopping align stages blocked on the gate, provider errors,
  aggregation — stage 5.
- The deadline stage, `Pending`, `Late`, revision metadata, changes to `put` — iteration 2.
- `Backfill` and live burst sizing — iteration 3.
- Coalescing evaluates; waking only the align stages whose column fits (a waiter heap).
- Changes to `OrderedFanIn`, `SynchronizerV2` or `data_logger`.

## Verification

- `make check` clean.
- `go test -race ./internal/engine/data/... ./internal/flow/...` clean.
- `make validate` passes on `config.json`.
- `go test -run '^$' -bench Readiness -benchmem ./internal/engine/data/` runs; results recorded in
  the discussion.
- Nothing this stage introduces is left without a caller in production code or tests.
