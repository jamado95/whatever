# Data Layer V2

## Topic

The data layer as the engine's first layer: a distinct module, `internal/engine/data`, that turns
subscriptions and provider instances into in-order, status-tagged frames, and owns the
`FrameBuffer` every other layer reads and writes through. Continuation of [[data-layer]], which
covers providers and venue normalisation; the runtime model it serves — synchronous step per
frame, status semantics, clock split — is in [[pipeline-runtime]].

Opened 2026-10-17.

## Current Understanding

### What it replaces

| Today | Site | Becomes |
|---|---|---|
| Provider injection, one instance per sub | `cmd/wire.go` (`_providers`) | Module input |
| `FeatureChain`'s `SortedWindow`, one per chain | `internal/domains/features/feature.go` | `FrameBuffer` history |
| `syncState` matrix, `runV2`, readiness | `internal/flow/align/synchronizer_v2.go` | Staging and release stages |
| `OrderedFanIn` linear-scan merge | `internal/flow/pipe/fanout.go` | Replay ordering stage: `OrderedFanIn` over all provider outputs, heap-based |
| `Ticker` gate on the synchronizer output | `internal/engine/ticker.go` | Engine-side pacer ([[pipeline-runtime]]) |
| Per-candle `map[string]any` snapshot | `internal/protocol/snapshot.go` | `FrameBuffer` feature entries |
| Independent subscriptions per timeframe | `config.json` | Direct subscriptions at the grid timeframe or a multiple; derivations declared inside the direct subscription |
| Multi-timeframe column machinery (period map, owning columns, `2 × coarsest` window) | `internal/flow/align/synchronizer_v2.go` | Per-stream period (due columns, last close); the window gives way to replay ordering ahead of staging |

### Rolling aggregation semantics

A rolling `W` window over a base stream emits one candle per base close, covering the last `W` of
base time. Built only from closed base candles, so it is consistent with [[data-layer]]'s
closed-candles-only decision.

- **Window span ≠ period.** Rolling 1h from 1m covers 60 minutes, updates every minute, and occupies the
  grid as a 1m stream. A derived stream updates at every close of its source — rolling 1d from a
  direct 1h updates hourly. The derived subscription's `Timeframe` is the window.
- **Stepped history.** Consecutive rolling candles share `W−1` base candles. 14 hourly periods
  ending now means every 60th rolling candle (`t, t−60, t−120, …`) — hourly bars phase-shifted to the
  current minute, not the last 14 rolling candles. Reads derive this step from the timeframe
  (Decisions § FrameBuffer).
- **Closed bars come free.** For UTC-aligned windows the rolling candle at a boundary column is the
  closed bar. Session-defined FX / equity days need a calendar (out of scope, see Decisions).
- **A revised base candle at `c` revises `W` derived candles** — columns `c … c+W−1`.
- **O(1) update:** open is the base candle `W` back, close the newest, volume a running sum,
  high / low a monotonic deque.
- **Statistics:** returns over overlapping windows are autocorrelated by construction; backtest
  significance tests on them must account for it.

### Period: direct streams coarser than the grid

The grid is the finest direct timeframe. A direct subscription may be coarser — a source without
finer data — and each stream updates at its own period:

```
column          10:58  10:59  11:00  11:01 … 11:59  12:00
BTC 1m            F      F      F      F       F      F
X 1h  (direct)    c      c      F      c       c      F    F = updates here
X 1h  (rolling)   F      F      F      F       F      F    c = carries its last update
```

- **Placement is unchanged.** A 1h bar closing at 10:59:59.999 lands in column 11:00 (`ceilDiv`), the
  frame of the 1m bars closing at the same instant. It becomes visible when 11:00 is released — never
  at 10:30.
- **A coarse direct stream is expected only at its due columns**; between them it carries its last
  close. Readiness stays a per-column count, against the streams due at that column.
- **Two proofs that a coarse stream has nothing at a non-due column:** its period (both modes), and
  in replay its next bar waiting in the merge. Leg states need the period in both modes; live has
  nothing else.
- **Replay skew is the merge's job.** Fed at equal candles/s, a 1h lane runs 60× faster in data time
  than a 1m lane. `OrderedFanIn` pulls by data time: the 1h lane's next bar waits in the merge and its
  provider blocks, so lanes advance together whatever their feed speed. Per-lane tickers are gone;
  pacing is engine-side.
- **Staging cannot absorb skew.** Under arrival order the fast lane fills headroom beyond the released
  head, which only the slow lane's data advances; the data layer blocks, the slow lane can no longer
  deliver, and the run deadlocks (today's V2 window evicts instead — incomplete frames). Ordering
  must precede staging; with it, staging runs about one column ahead of the released head.
- **Live has no such skew** — a lane cannot run ahead of the wall clock — except startup history (Open
  Questions § Warm-up ownership).

**Derive where possible.** Coarser series of a (symbol, provider) that has finer data are derived,
not subscribed: no cold-start skew between its timeframes, gaps exactly the base's, consistent by
construction, one provider stream.

## Decisions

### Module and stages

- **Module `internal/engine/data`.** In: `Subscription`s and provider instances. Out: in-order frame
  events and the `FrameBuffer`.
- **Stage order:** provider ingestion → alignment → buffer ordering logic → frame alignment /
  readiness → commit → aggregation. Commit releases ready columns and emits their events in column
  order; aggregation runs when the engine dequeues an event (see Consumption).
- **The `FrameBuffer` is the staging environment** — rows beyond the released head hold undecided
  columns. No separate staging model.
- **The data layer does not use `SynchronizerV2`** and never imports `align`. Carried over as ideas,
  not code: ceilDiv placement (`Streams.column`), the merge injected per mode, and the apply →
  evaluate → emit loop, rewritten as the aligner's stage → ready → commit.
- **Stage convention:** a stage is a `pipe.Stage[In, Out]` — it starts its own goroutine, owns its
  output and closes it once its input closes, and never drops a value silently; a fan-in is a
  `pipe.Merge[T]`. Goroutine boundaries sit only where parallelism or blocking needs them (lanes,
  merge, aligner, engine); steps inside one goroutine are plain functions called in sequence. No
  third-party pipeline library: none offers a merge ordered by key, and the hot path needs typed
  channels without per-item boxing.
- **One unexported composer** (`pipeline`) wires the stages in one place, with per-mode stages
  injected. The module exports a `data.New` constructor (stage 5); no package-level instance.

### Normalisation

- **ms is the timestamp contract; providers meet it.** Venue format stays in the provider —
  `binance_csv` scales µs ([[data-layer]] defect 7); the lane rejects anything outside the ms range.
- **One lane per direct stream** applies the rules in order: validate (`OpenTs > 0`, ms timestamps,
  finite OHLCV, `V ≥ 0`, `L ≤ min(O, C)`, `H ≥ max(O, C)`, span within `[timeframe − 1 s,
  timeframe]`), column by ceilDiv, due column, identical duplicate, column beyond the last accepted.
- **Monotonicity is by column**, not `CloseTs`: two closes in one column would write one slot twice.
- **The span may fall short of the timeframe, never exceed it** — under ceilDiv a close past its
  boundary would land one column late. The span check also catches a file wired to the wrong
  subscription.
- **Drops are logged once per rule per lane, then summarised** when the lane ends; identical
  duplicates are only counted.
- **A lane carries column, stream index, end flag and candle**; the stream index is authoritative.

### Subscriptions and the grid

- **The grid is the finest direct timeframe**, computed when the stream table is built. Every
  direct timeframe is a multiple of it by the closed timeframe enum.
- **Derivations are declared on the direct subscription** — `"derive": ["1h", "1d"]`, held in
  `proto.Subscription.Derive` as a `TimeframeDerivationSet` (a `uint8` bitmap, so `Subscription`
  stays small and comparable). Rolling windows only; a coarse direct stream is a source like any
  other.
- **Config parsing validates subscriptions:** stream IDs are unique — a symbol may have several
  direct timeframes on one provider; each derived window is coarser than its source and a multiple of
  it; provider names may not start with `derived_`.
- **The stream table is built by the data layer** (`data.NewStreams`), never by the composition
  root: one stream per direct subscription and per derived window, direct streams first, then
  derived, each sorted by ID. A derived stream's ID is `symbol:timeframe:derived_<source ID>`, e.g.
  `BTCUSDT:1h:derived_BTCUSDT:1m:binance`, assigned only there. Strategy references stay direct-only
  until the strategy layer decides otherwise.
- **A frame is the state of every registered stream at one moment** — market legs plus their
  features, strategy signals and other records — on the grid. Streams are always time-aligned within
  a frame; each updates at its own period.
- **Period.** A direct stream of period `P` is due at columns where `c % P == 0`: bars are assumed
  UTC-aligned, crypto style. A derived stream updates at every close of its source. Between updates a
  leg carries its last update; fresh versus carried is inferred from the stream's kind (direct /
  derived) and period — no extra leg state.

### Aggregation

- **Rolling windows**, derived candles stored in the `FrameBuffer`.
- **Emitted from the first base candle**, carrying the count of base candles in the window (below `W`
  during warm-up).

### Release

- **Frames are emitted strictly in column order**, each with a status — `Current`, `Late`,
  `Backfill` — and a separate completeness flag — `Complete`, `Partial`.
- **A column is released once every direct stream due at it is decided**: present, proven missing
  (the stream already has data beyond it), or presumed missing (live deadline passed). Streams not due
  are not waited on; a live deadline applies only at a stream's due columns.
- **Completeness is by last due candle:** `Complete` if every active stream's latest due candle at or
  before the column is present, otherwise `Partial`. A missing 1h bar makes its whole hour `Partial`.
- **`Current` is the newest column; `Late` is a revision of a released column and carries that
  column.** If `c+1` is complete while `c` is not, `c` is already decided: `c` goes out
  `Current` + `Partial`, then `c+1` `Current` + `Complete`. Data later arriving for `c` re-emits it
  `Late` + `Complete` / `Partial`.
- **One release stage, one rule; ordering and watermark are injected per mode.** Replay: ordered merge,
  watermark from the merged stream's order. Live: arrival order (`FanIn`) and clock − lateness.
- **Replay ordering is `OrderedFanIn` across all provider output channels**, its output channelled to
  the release stage. The merged stream is globally time-ordered, so the first candle beyond column `c`
  proves no lane has anything left at or before `c`: `c` is decided, and any stream without a candle
  at `c` is `Gap`. The merge requires:
  - an **end-of-lane marker**, so a closed lane's stream leaves the expected set instead of reading as
    missing in every later column. It is ordered at the lane's next due column (`lastCol + Period`);
    a lane that accepted no candle sends it at column −1, which sorts first and must not start the
    released head;
  - **releasing the final column when the merged channel closes**, since no later candle will arrive;
  - a **heap** in place of the linear scan of heads: `container/heap`, the root replaced and fixed
    on refill, no allocation — 268 vs 1680 ns per value at 1000 inputs, every value tied, channel
    hand-offs included.
- **Replay ordering precedes staging** and covers coarse lanes with no special case; staging only
  bounds skew, it cannot absorb it (Period).
- **Live does not use `OrderedFanIn`:** it waits while any open lane has no pending candle, so one
  silent stream would stall all of them.
- **Live has two release triggers** feeding one in-order release loop: the *data trigger* releases
  columns that are complete or whose gaps are proven, as data arrives; the *clock trigger* releases
  every unreleased column up to `now − lateness` when a deadline expires, marking still-undecided
  legs `Pending`. Replay uses only the data trigger.
- **Partial and Complete frames reach every component**; each decides what to do with them.
- **Lanes are assumed time-ordered.** Replay relies on it — gaps are proven from the merged order and
  no `Late` occurs; a live lane delivering out of order is repaired by `Late`.
- **An ended stream leaves the expected set**; its leg is `Ended` from then on and does not count
  toward completeness. A stream that has not started yet counts as missing — frames before its first
  candle are `Partial`.
- **Leg states:** `Fresh`, `Gap` (proven missing), `Pending` (presumed missing at a live deadline),
  `Ended`.
- **Live lateness is a hardcoded value** for now.
- **Session calendars are out of scope.** Closed markets look like silent streams (`Pending`,
  `Partial`) until a calendar exists; it can be added later as a module inside the data layer.

### Consumption

- **The engine consumes events in order, one at a time.** It reads the next event only after the
  whole pipeline has finished the current one; the data layer keeps staging and releasing ahead
  meanwhile.
- **Release, then materialise.** The data layer goroutine writes base candles into rows beyond the
  released head, decides readiness, marks columns released and emits their events. On dequeue,
  `data.Next()` — data layer code on the engine goroutine — materialises the column: a `Late` event
  writes its revised base candles and recomputes the derived rows it affects up to the last processed
  column; a `Current` / `Backfill` event aggregates the column's derived legs. Then it returns the view.
- **Row ownership.** The data layer goroutine writes only rows beyond the released head, base candles
  only. Released rows are written only on the engine goroutine — materialisation and the phases. A
  late candle travels in its `Late` event and is never written into a released row by the data layer.
- **A revision becomes visible at its `Late` event's position.** Frames before it see the old data;
  the `Late` frame applies it and triggers recompute; frames after see revised data with corrected
  history. No frame ever mixes old and revised values.

### Capacity

- **Depth = deepest declared read + headroom**, within the 365-day cap. Headroom bounds how far the
  data layer may run ahead: staged rows plus released-but-unconsumed events.
- **When headroom runs out the data layer blocks, in replay and live alike**, pushing backpressure to
  the providers.
- **Live: the provider deals with backpressure.** Its buffered output channel accumulates candles
  while the data layer is blocked and drops once full; the data layer never drops. Dropped candles
  surface downstream as proven gaps (`Partial`), repairable through `Late` if the provider later
  backfills them within depth.

### FrameBuffer

- **Owned by the data layer**, which exposes write access for features, strategies and execution.
- **Capability passing:** components never receive the buffer. Each phase gets a read-only view plus
  only its own section's writer.
- **One ring per stream, one slot per update:** `slot = (col / Period) % size`. Each ring is sized to
  its own stream's reads, so a coarse stream costs its own bars and one deep read no longer sets the
  depth of every stream. Feature entries will follow the same rings (iteration 4).
- **Stream shape: `Period` and `Window`** on the stream table. `Period` is the columns between a
  stream's updates — a direct stream's timeframe ÷ grid, a derived stream's source `Period`;
  `Window` is the updates one bar spans — 1 direct, source bars per window derived. A bar spans
  `Period × Window` columns.
- **Sizing:** `lookback` bars for every stream (the longest feature lookback); a direct stream keeps
  at least its largest derived window + 1; each ring adds `ceil(headroom / Period)` slots — the most
  the aligner can write ahead of the engine — plus one spare. The 365-day cap applies per stream; exceeding it is a construction error naming the stream.
- **No per-column storage.** `Complete` travels with the event and its `View`; the released head and
  the presence count are the aligner's own state.
- **Revision metadata** records whether a column was revised and which legs changed.
- **Fills and order updates** are written through the execution section's writer; they do not pass
  through ingestion or alignment.
- **Typed record sections via generics** — `Section[Signal]`, `Section[Order]`, `Section[Fill]`,
  written through `RecordWriter[T]` and read with `Records(view, section)` (Go methods cannot take type
  parameters).
- **Writers are scoped to their owner:** a feature's writer writes only its own key; a strategy's
  signal writer stamps its own ID.
- **History reads derive their step from the stream's timeframe** — one bar, `Period × Window`
  columns, counted back from the stream's latest update at or before the view's column. A rolling 1h
  reads `c, c−60, …`; a direct 1h at 10:30 reads `10:00, 9:00, …`. Newest first; `Gap` and `Ended`
  slots are skipped; a read covers at most the stream's kept bars. Feature history takes its
  stream's step, so a recursive feature on a rolling stream reads its previous same-phase value.
- **`Leg` of a stream not yet started** is a zero candle, `Gap`, age −1; `Count` is the source bars
  in the latest window — 1 for a started direct stream.
- **One write path, `put`,** for both writers. Ownership is a convention: the aligner writes direct
  slots only for columns it has not released, the engine goroutine only released columns; a violation
  shows up as a data race under `-race`. Rewriting a column replaces its slot.

## Solution

Proposed detail below the decisions; not yet planned.

### Pipeline

```
          ┌──────────────────────────── data layer (internal/engine/data) ────────────────────────────┐
          │ data layer goroutines                                       │ engine goroutine, on dequeue │
providers ─▶ ingest ─▶ normalise ─▶ order ─▶ stage ─▶ ready ─▶ commit ─▶ events ─▶ Next(): materialise   │
          │  per lane            aligner: rows beyond the released head │   (Late apply / aggregate)   │
          │                         └──────────── FrameBuffer ──────────┴──────────────┘               │
          └──────────────────────────────────────────────────────────────────────────┬─────────────────┘
                                                                        view + section writer per phase
                                   engine step: features → strategies → portfolio → execution
```

| Proposed stage | Decided stage | Runs on | Job |
|---|---|---|---|
| Ingest | Provider ingestion | Provider goroutine | Provider lifecycle, `ReceivedAt`, errors. Under backpressure the provider's buffered output absorbs, then drops |
| Normalise | Alignment | Lane, one goroutine per direct stream | Rules in order (Decisions § Normalisation): validate, column (`ceilDiv`), due column, identical duplicate, column order; drops logged once per rule and summarised; end marker on close. µs scaling is the provider's ([[data-layer]] defect 7) |
| Order | Buffer ordering logic | Merge goroutine → aligner | Replay: `OrderedFanIn` (`container/heap`) over the lanes by (column, stream), with end-of-lane markers. Live: `FanIn`, arrival order. Injected by the composer |
| Stage | Buffer ordering logic | Aligner | Write base candles into `FrameBuffer` rows beyond the released head; per-row presence count and each stream's latest column replace `syncState` |
| Ready | Frame alignment / readiness | Aligner | Decide each direct stream due at the column; set completeness (last due candles) and leg states |
| Commit | Commit | Aligner | Mark ready columns released and emit their events in column order; a candle for a released column becomes a `Late` event |
| Materialise | Aggregation | Engine goroutine, in `Next()` | `Late`: write revised base candles, recompute affected derived rows up to the last processed column, record revision metadata. Otherwise: aggregate the column's derived legs |

- **Why aggregation runs at dequeue.** Aggregating in the data layer goroutine would read base rows
  of the last `W` columns while a dequeued `Late` writes one of them — a data race. Run on the engine
  goroutine, released rows have a single writer, and released-but-unconsumed rows are aggregated when
  their own event is processed, so they pick up revisions without extra work.
- **Publication.** Rows staged by the data layer become visible to the engine through the event's
  channel send; views are pinned at or below the engine's column, never ahead of the released head.
- **Only direct streams, at their due columns, are in the expected set.** Derived legs are functions of base legs and
  inherit their state, so aggregating after readiness changes no release decision.
- Release follows the watermark model in [[pipeline-runtime]] § Clock. The aligner never reads
  `time.Now()`.
- Venue-format normalisation stays in the provider ([[data-layer]]).

### Release rule

For column `c`, each direct stream due at `c` is:

| State | Condition | Leg state |
|---|---|---|
| Present | Its candle for `c` has arrived | `Fresh` |
| Proven missing | It has data beyond `c` — lanes are ordered | `Gap` |
| Presumed missing | Live only: boundary + lateness passed with nothing from it | `Pending` |
| Undecided | None of the above | Column held |

- **In-order release is guaranteed** given ordered lanes and deadlines that grow with the column: if
  `c+1` is decided, `c` already is. A column is never first released behind a newer one, so `Late`
  only ever means revision.

```go
// both triggers update the decided state, then run the same loop
for next := head + 1; decided(next) || next <= watermark; next++ {
    emit(next) // Complete if all present; otherwise Partial with Gap / Pending legs
}
```

- **Order holds across triggers.** A complete column past the watermark waits for earlier columns;
  the clock sweep that releases them releases it right after.
- **One timer, not polling.** The aligner wakes at the oldest unreleased column's `boundary +
  lateness` (`Clock.Wake`) and re-arms after each release.
- **No re-emit on proof after `Pending`.** A `Pending` leg later proven missing changes no data, so it
  produces no `Late`; only arriving data revises a released column.
- **Clock skew.** Deadlines compare the local clock with venue timestamps, so lateness also absorbs
  skew; the host clock is NTP-synced.
- **Per-stream lateness, later:** stream `s` is presumed missing at `c` once
  `now ≥ boundary(c) + lateness(s)`; with one hardcoded value the watermark is global.
- A wrongly proven gap — a lane delivering out of order, e.g. a backfill after reconnect — is repaired
  by `Late`. A revision older than the buffer depth is dropped and logged.
- A missing leg is never fabricated: the view returns the stream's last known candle with its state
  and age.

| Scenario | Outcome |
|---|---|
| Streams arrive at slightly different times | Hold, then `Current` + `Complete` |
| Stream skips a candle, then sends the next | Proven gap: `Current` + `Partial` immediately |
| Stream goes silent (live) | Deadline: `Current` + `Partial` (`Pending`); arrival re-emits `Late` |
| Stream not started yet | `Partial` until its first candle |
| Coarse direct stream between closes | Not waited on; carries its last close |
| Coarse bar missing at its due column | `Gap` / `Pending` there; frames `Partial` until its next close |
| Stream ends | Leaves the expected set |
| Live lane out of order (`t+1 → t+3 → t+2`) | `t+2` `Current` + `Partial` (`Gap`), `t+3` `Current` + `Complete`; `t+2`'s arrival re-emits it `Late` + `Complete` |
| Several streams miss one column | `Partial` with each missing leg; one `Late` per arrival (`Partial` until the last, then `Complete`); other streams' columns unaffected |
| Replay | A gap is proven by the first merged candle beyond its column; no deadline, no `Pending`, no `Late` |

| | Backtest | Live |
|---|---|---|
| Ordering | `OrderedFanIn` (heap) over provider outputs | `FanIn`, arrival order into staging |
| Gap proof | First merged candle beyond the column | Stream's follow-up arrival |
| Deadline | None | Hardcoded lateness |
| Revisions | Never | Any column still in the buffer |

### Events

```go
package data // internal/engine/data

type Status uint8

const (
    Current Status = iota
    Late
    Backfill
)

type Event struct {
    Status   Status
    Complete bool
    Col      int64 // absolute column (boundary ms / stride); Current: new head; Late: revised column
    Legs     []Leg // Late only: revised base candles; Current data is already in the row
}

type Leg struct {
    Stream int // index into the layer's stream table
    State  LegState
    Data   proto.MarketData
}
```

Columns are absolute, never ring slots. `Col` is carried on every event so "Current is the newest
column" is checked rather than assumed by consumers.

### FrameBuffer

```go
// One ring per stream, one slot per update (stage 2).
type FrameBuffer struct {
    streams *Streams
    rings   []ring
}

type ring struct {
    slots []slot
    bars  int64        // bars kept for reads; the remaining slots are headroom
    end   atomic.Int64 // Ended column; MaxInt64 while open
}

type slot struct {
    col   int64        // column held; -1 = empty
    fresh int64        // newest Fresh column at or before col; -1 = none
    c     proto.Candle // Fresh: its candle; Gap / Ended: the last Fresh candle
    state LegState     // Fresh | Gap | Ended; Pending in iteration 2
    count uint16       // derived: base candles in the window
}

func (b *FrameBuffer) put(s int, col int64, c proto.Candle, state LegState, count uint16)

// Components receive a View plus their own writer, never the buffer.
type View struct { // pinned: sees only columns ≤ col
    buf      *FrameBuffer
    col      int64
    complete bool
}

func (v View) Col() int64
func (v View) Boundary() int64 // col × Stride: Unix ms, UTC
func (v View) Complete() bool
func (v View) Leg(stream int) (c proto.Candle, s LegState, age int) // carried: last Fresh candle, its age
func (v View) Count(stream int) int
func (v View) Candles(stream, n int, dst []proto.Candle) []proto.Candle // newest first

// Later: features (iteration 4), revisions (iteration 2), records (strategy step loop).
func (v View) Feature(key FeatureKey, stream, n int, dst []float64) []float64
func (v View) Revised() []int // streams whose legs changed at Col; empty if never revised

type FeatureWriter interface{ Set(stream int, v float64) } // bound to one feature key

type Section[T any] struct{ id int }                   // Signals, Orders, Fills
type RecordWriter[T any] interface{ Append(recs ...T) } // bound to the current column and the owner's ID
func Records[T any](v View, s Section[T]) []T
```

- **Pinned views** stop look-ahead: a phase at column `c` cannot read `c+1`.
- **Write discipline** is structural: a phase can only write the section whose writer it was handed.
  Feature workers may write disjoint cells in parallel; the phase barrier publishes them. Tests run
  with `go test -race`.
- **Layout is built before `Start`:** the stream table from the data layer and the reads it must
  hold — in iteration 1 one `lookback` for every stream, the longest feature lookback; per-stream
  declared reads from the feature graph and strategies in iteration 4.
- **A source keeps its largest window + 1 bars**, since aggregation reads base history from the
  buffer.
- **Headroom keeps staging off readable history.** Staged writes wrap a ring onto its oldest slots;
  headroom ensures those slots are beyond every read of the column being processed.
- **A declared depth beyond the 365-day cap** is a construction error, not a silent truncation.
- **Reads copy into a caller-supplied slice** (`dst`): a ring wrap splits history into two segments,
  and the hot path should not allocate.
- **Mixed-timeframe cross reads.** A rolling and a direct stream of one timeframe line up only at the
  direct stream's closes, so a cross feature mixing them samples there ([[pipeline-runtime]] § Features
  phase, sampling clock).
- **Memory.** A slot is about 80 B, and a stream costs its own slots: the stage-2 test table (5m
  grid, five streams, `lookback` 20) holds 801 slots, against 28,820 had every stream shared rows at
  the deepest read. A 200-bar read on a direct 1d stream costs 200 slots, not 288,000 rows for every
  stream of a 1m grid. A rolling 1d over a 1m grid still needs `bars × 1440` slots — the cost of
  stepped reads, not of the layout.

## Open Questions

- **Stage boundaries** — whether the proposed stages map onto the decided ones as tabulated (deferred).
- **Lateness** — per venue / timeframe from measured arrival delays; hardcoded until then.
- **Guaranteeing replay lane order** — e.g. sorting each lane once at dataset conversion; the CSV
  quality check can flag violations. Until then, an out-of-order source file yields `Late` in replay.
- **Live reorder tolerance** — with lateness well below the grid step the deadline usually decides
  first (`Pending`); proof matters live on fine grids or reconnect bursts, where out-of-order delivery
  is likely. Whether to wait briefly before trusting a proof.
- **Headroom size** — sized for the worst expected burst (startup warm-up, reconnect backfill); value
  and units undecided.
- **Provider drop contract** — buffer size per provider, whether drops are reported (e.g. on the
  error channel), and whether sustained drops raise an overload signal so risk can halt trading.
- **Warm-up ownership** — `Backfill` implies the data layer drives historical warm-up ([[data-layer]]).
  Startup history has replay's skew — a 1h lane's history spans 60× the data time of a 1m lane's —
  so under arrival order it deadlocks staging (Period); it must go through the ordered merge.
- **`Layout` declaration** — how features and strategies declare depth in bars per stream.
- **Non-UTC-aligned bars** (deferred) — FX / equity daily bars close at a session time that moves in
  UTC with daylight saving. Options: infer an offset per stream, or place each bar at the next UTC
  due column (a delay, never early).
- **Multiplexing providers** — `Init([]Subscription)` from [[data-layer]] still open; the module's
  input edge should not preclude it.
- **Exporter** — read from the `FrameBuffer` directly?

## WIP

- Plan: `docs/plans/data-layer-v2/main.md`. Stage 1 (subscription model, stream table)
  implemented — `docs/plans/data-layer-v2/stage-1-streams.md`.
- Stage 2 (`FrameBuffer` core) implemented — `docs/plans/data-layer-v2/stage-2-framebuffer.md`.
- Stage 3 (lanes, normalisation, ordered merge, stage convention and composer) implemented —
  `docs/plans/data-layer-v2/stage-3-lanes.md`.

## Resources

- `internal/flow/align/synchronizer_v2.go`, `internal/flow/pipe/fanout.go`,
  `internal/domains/features/feature.go`, `internal/protocol/provider.go`,
  `internal/protocol/snapshot.go`, `internal/engine/ticker.go`, `cmd/wire.go`
- Related: [[data-layer]], [[pipeline-runtime]], [[multi-subscription]], [[features]], [[config]]
