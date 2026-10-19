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
| `OrderedFanIn` linear-scan merge | `internal/flow/pipe/fanout.go` | Ordering stage (heap merge / precomputed index) |
| `Ticker` gate on the synchronizer output | `internal/engine/ticker.go` | Engine-side pacer ([[pipeline-runtime]]) |
| Per-candle `map[string]any` snapshot | `internal/protocol/snapshot.go` | `FrameBuffer` feature entries |
| Independent subscriptions per timeframe | `config.json` | One timeframe for all direct subscriptions; coarser ones derived |
| Multi-timeframe column machinery (period map, owning columns, `2 × coarsest` window) | `internal/flow/align/synchronizer_v2.go` | One candle per stream per column |

### Rolling aggregation semantics

A rolling `W` window over a base stream emits one candle per base close, covering the last `W` of
base time. Built only from closed base candles, so it is consistent with [[data-layer]]'s
closed-candles-only decision.

- **Window ≠ cadence.** Rolling 1h from 1m covers 60 minutes, updates every minute, and occupies the
  grid as a 1m stream. The derived subscription's `Timeframe` is the window.
- **Stepped history.** Consecutive rolling candles share `W−1` base candles. 14 hourly periods
  ending now means every 60th rolling candle (`t, t−60, t−120, …`) — hourly bars phase-shifted to the
  current minute, not the last 14 rolling candles.
- **Closed bars come free.** For UTC-aligned windows the rolling candle at a boundary column is the
  closed bar. Session-defined FX / equity days need a calendar (out of scope, see Decisions).
- **A revised base candle at `c` revises `W` derived candles** — columns `c … c+W−1`.
- **O(1) update:** open is the base candle `W` back, close the newest, volume a running sum,
  high / low a monotonic deque.
- **Statistics:** returns over overlapping windows are autocorrelated by construction; backtest
  significance tests on them must account for it.

### Why a single direct timeframe

All direct streams share one timeframe — the finest data, and the grid — and every coarser series is
derived. This removes cold-start skew between a symbol's timeframes (all start at the base's first
column), removes coarse-candle gaps (a derived stream's gaps are exactly its base's), keeps
timeframes consistent by construction and needs one provider stream per symbol. Every direct stream
closes in every column, so the multi-timeframe machinery of today's synchronizer disappears:
readiness is a per-column count against the active direct streams, and each column has one deadline.

Consequence: a source with only coarse history sets the resolution for the whole run — a 1d-only
venue means a 1d grid, or leaving that source out.

## Decisions

### Module and stages

- **Module `internal/engine/data`.** In: `Subscription`s and provider instances. Out: in-order frame
  events and the `FrameBuffer`.
- **Stage order:** provider ingestion → alignment → buffer ordering logic → frame alignment /
  readiness → commit → aggregation. Commit releases ready columns and emits their events in column
  order; aggregation runs when the engine dequeues an event (see Consumption).
- **The `FrameBuffer` is the staging environment** — rows beyond the released head hold undecided
  columns. No separate staging model.

### Subscriptions and the grid

- **One timeframe for all direct subscriptions** — the finest data, and the grid. One direct
  subscription per (symbol, provider). Every coarser timeframe is a derived rolling subscription
  whose window is an integer multiple of it. Both are validated at the composition root.
- **Derived subscriptions** name their source with `"from"` and have no provider. ID:
  `symbol:timeframe:derived_<source ID>`, e.g. `BTCUSDT:1h:derived_BTCUSDT:1m:binance`.
- **A frame is the state of every registered stream at one moment** — market legs plus their
  features, strategy signals and other records — on a grid at the direct timeframe. Every direct
  stream closes in every column; streams are always time-aligned within a frame.

### Aggregation

- **Rolling windows**, derived candles stored in the `FrameBuffer`.
- **Emitted from the first base candle**, carrying the count of base candles in the window (below `W`
  during warm-up).

### Release

- **Frames are emitted strictly in column order**, each with a status — `Current`, `Late`,
  `Backfill` — and a separate completeness flag — `Complete`, `Partial`.
- **A column is released once every expected stream is decided at it**: present, proven missing (the
  stream already has data beyond it), or presumed missing (live deadline passed). `Complete` if all
  present, otherwise `Partial`.
- **`Current` is the newest column; `Late` is a revision of a released column and carries that
  column.** If `c+1` is complete while `c` is not, `c` is already decided: `c` goes out
  `Current` + `Partial`, then `c+1` `Current` + `Complete`. Data later arriving for `c` re-emits it
  `Late` + `Complete` / `Partial`.
- **One release stage, one rule; ordering and watermark are injected per mode.** Replay: heap merge
  and data watermark (minimum next close across lanes). Live: arrival order and clock − lateness.
- **Live has two release triggers** feeding one in-order release loop: the *data trigger* releases
  columns that are complete or whose gaps are proven, as data arrives; the *clock trigger* releases
  every unreleased column up to `now − lateness` when a deadline expires, marking still-undecided
  legs `Pending`. Replay uses only the data trigger.
- **Partial and Complete frames reach every component**; each decides what to do with them.
- **Lanes are assumed time-ordered.** Replay relies on it — gaps are proven from merge heads and no
  `Late` occurs; a live lane delivering out of order is repaired by `Late`.
- **An ended stream leaves the expected set.** A stream that has not started yet counts as missing —
  frames before its first candle are `Partial`.
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
- **One row per column**, holding every stream's leg, its feature entries and the column's records.
  Depth is a single row count — the maximum depth declared by features and strategies plus headroom
  (Capacity), capped at 365 days of rows at the grid timeframe.
- **Revision metadata** records whether a column was revised and which legs changed.
- **Fills and order updates** are written through the execution section's writer; they do not pass
  through ingestion or alignment.

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
| Normalise | Alignment | Lane | Validate, map `CloseTs` to its column (`ceilDiv`), drop identical duplicates, scale µs timestamps ([[data-layer]] defect 7) |
| Order | Buffer ordering logic | Aligner | Replay: heap merge or precomputed frame index. Live: arrival order |
| Stage | Buffer ordering logic | Aligner | Write base candles into `FrameBuffer` rows beyond the released head; per-row presence count and each stream's latest column replace `syncState` |
| Ready | Frame alignment / readiness | Aligner | Decide each base stream per column; set completeness and leg states |
| Commit | Commit | Aligner | Mark ready columns released and emit their events in column order; a candle for a released column becomes a `Late` event |
| Materialise | Aggregation | Engine goroutine, in `Next()` | `Late`: write revised base candles, recompute affected derived rows up to the last processed column, record revision metadata. Otherwise: aggregate the column's derived legs |

- **Why aggregation runs at dequeue.** Aggregating in the data layer goroutine would read base rows
  of the last `W` columns while a dequeued `Late` writes one of them — a data race. Run on the engine
  goroutine, released rows have a single writer, and released-but-unconsumed rows are aggregated when
  their own event is processed, so they pick up revisions without extra work.
- **Publication.** Rows staged by the data layer become visible to the engine through the event's
  channel send; views are pinned at or below the engine's column, never ahead of the released head.
- **Only base (direct) streams are in the expected set.** Derived legs are functions of base legs and
  inherit their state, so aggregating after readiness changes no release decision.
- Release follows the watermark model in [[pipeline-runtime]] § Clock. The aligner never reads
  `time.Now()`.
- Venue-format normalisation stays in the provider ([[data-layer]]).

### Release rule

For column `c`, each expected base stream is:

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
| Stream ends | Leaves the expected set |
| Live lane out of order (`t+1 → t+3 → t+2`) | `t+2` `Current` + `Partial` (`Gap`), `t+3` `Current` + `Complete`; `t+2`'s arrival re-emits it `Late` + `Complete` |
| Several streams miss one column | `Partial` with each missing leg; one `Late` per arrival (`Partial` until the last, then `Complete`); other streams' columns unaffected |
| Replay | Gaps proven instantly from the merge; no deadline, no `Pending`, no `Late` |

| | Backtest | Live |
|---|---|---|
| Ordering | Heap merge or precomputed frame index | Arrival order into staging |
| Gap proof | Instant, from merge heads | Stream's follow-up arrival |
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
// Owned by the data layer; engine phases receive a View plus one writer, never the buffer.
type View interface { // pinned: sees only columns ≤ Col()
    Col() int64
    Status() Status
    Complete() bool
    Leg(stream int) (proto.MarketData, LegState)
    Candles(stream, n, step int) []proto.Candle // step > 1 for rolling windows
    Feature(stream, slot, n, step int) []float64
    Records(sec Section) []Record
    Revised() []int // streams whose legs changed at Col; empty if never revised
}

type FeatureWriter interface{ Set(stream, slot int, v float64) } // features phase, current column
type RecordWriter interface{ Append(recs ...Record) }             // one per section: signals, orders, fills
```

- **Pinned views** stop look-ahead: a phase at column `c` cannot read `c+1`.
- **Write discipline** is structural: a phase can only write the section whose writer it was handed.
  Feature workers may write disjoint cells in parallel; the phase barrier publishes them. Tests run
  with `go test -race`.
- **Layout is built before `Start`:** the stream table from the data layer, feature slots and each
  reader's `(depth, step)` from the feature graph and strategies.
- **Depth must also cover** the largest rolling window `W`, since aggregation reads base history from
  the buffer.
- **Headroom keeps staging off readable history.** Staged writes wrap the ring onto the oldest rows;
  headroom ensures those rows are beyond every declared read of the column being processed.
- **A declared depth beyond the 365-day cap** is a construction error, not a silent truncation.
- **Memory.** A row holds every stream, so the cost is rows × streams × (candle + feature entries) at
  the finest grid. E.g. a 200-day read on a rolling 1d over a 1m grid needs 288,000 rows; at 200
  streams and 10 features that is ≈ 7.8 GB. The physical layout is an implementation detail.

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
- **Ended stream's leg** — what a frame shows for a stream after it leaves the expected set.
- **Warm-up ownership** — `Backfill` implies the data layer drives historical warm-up ([[data-layer]]).
- **`Layout` declaration** — how features and strategies declare `(depth, step)`.
- **`Record` typing** — typed sections for signals, orders and fills, or a generic record.
- **Multiplexing providers** — `Init([]Subscription)` from [[data-layer]] still open; the module's
  input edge should not preclude it.
- **Exporter** — read from the `FrameBuffer` directly?

## WIP

- Nothing implemented, no plan.

## Resources

- `internal/flow/align/synchronizer_v2.go`, `internal/flow/pipe/fanout.go`,
  `internal/domains/features/feature.go`, `internal/protocol/provider.go`,
  `internal/protocol/snapshot.go`, `internal/engine/ticker.go`, `cmd/wire.go`
- Related: [[data-layer]], [[pipeline-runtime]], [[multi-subscription]], [[features]], [[config]]
