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
| `syncState` matrix, `runV2`, readiness | `internal/flow/align/synchronizer_v2.go` | Per-stream align stages (staging) and one readiness stage (release) |
| `OrderedFanIn` linear-scan merge | `internal/flow/pipe/fanout.go` | Not used by the data layer — per-stream ring capacity bounds skew; the heap upgrade stays for `SynchronizerV2` |
| `Ticker` gate on the synchronizer output | `internal/engine/ticker.go` | Engine-side pacer ([[pipeline-runtime]]) |
| Per-candle `map[string]any` snapshot | `internal/protocol/snapshot.go` | `FrameBuffer` feature entries |
| Independent subscriptions per timeframe | `config.json` | Direct subscriptions at the grid timeframe or a multiple; derivations declared inside the direct subscription |
| Multi-timeframe column machinery (period map, owning columns, `2 × coarsest` window) | `internal/flow/align/synchronizer_v2.go` | Per-stream period (due columns, last close); the window gives way to per-stream ring capacity |

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
  close. Readiness checks only the streams due at a column.
- **A coarse stream has nothing at a non-due column by its period**, in both modes. A missing bar at
  a due column is proven by the stream's own next bar (lanes are ordered) or, live, presumed at its
  deadline.
- **Replay skew is bounded per stream.** Fed at equal candles/s, a 1h lane runs 60× faster in data
  time than a 1m lane. Its align stage blocks once its ring is full, its provider blocks behind it,
  and the other lanes keep flowing. Per-lane tickers are gone; pacing is engine-side.
- **Blocking must be per stream.** One goroutine staging every stream deadlocks under skew: blocked
  on the fast lane's full ring, it stops draining the slow lane too, so the released head never moves
  (today's V2 window evicts instead — incomplete frames). With one align stage per stream, only the
  stream ahead blocks. Take the lowest column `m` any lane is blocked on: every stream has resolved its
  due columns below `m`, readiness releases up to `m − 1`, the engine consumes them, and the lane at
  `m` resumes. It holds while each ring keeps a slot beyond its kept bars, provided each lane reports
  what it knows before it blocks (Decisions § Alignment).
- **Live has no such skew** — a lane cannot run ahead of the wall clock — except startup history (Open
  Questions § Warm-up ownership).

**Derive where possible.** Coarser series of a (symbol, provider) that has finer data are derived,
not subscribed: no cold-start skew between its timeframes, gaps exactly the base's, consistent by
construction, one provider stream.

## Decisions

### Module and stages

- **Module `internal/engine/data`.** In: `Subscription`s and provider instances. Out: in-order frame
  events and the `FrameBuffer`.
- **Stage order.** Per direct stream, one goroutine per stage: ingest → normalise → deadline (live
  only) → align. Then one readiness stage (ready → commit) releases columns and emits their events in
  column order. Aggregation runs when the engine dequeues an event (see Consumption).
- **The `FrameBuffer` is the staging environment** — slots beyond the released head hold undecided
  columns. No separate staging model.
- **Alignment is per stream, readiness is per frame.** Placing a datapoint needs only its own
  stream; deciding when a frame goes out, and whether it is complete, needs every stream due at the
  column.
- **The data layer does not use `SynchronizerV2`** and never imports `align`. Carried over as ideas,
  not code: ceilDiv placement (`Streams.column`), readiness against the streams due at a column, and
  the apply → evaluate → emit loop, rewritten as align → evaluate → release.
- **Stage convention:** a stage is a `pipe.Stage[In, Out]` — it starts its own goroutine, owns its
  output and closes it once its input closes, and never drops a value silently. Goroutine boundaries
  sit only where parallelism or blocking needs them (the
  per-stream lane stages, readiness, the engine); steps inside one goroutine are plain functions
  called in sequence. No third-party pipeline library: the hot path needs typed channels without
  per-item boxing or wrapping.
- **One unexported composer** (`pipeline`) wires the stages in one place, per mode: replay leaves out
  the deadline stage. The module exports a `data.New` constructor (stage 5); no package-level
  instance.

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
- **Lane items carry column and candle**; live adds the deadline stage's `Pending` markers. A lane
  is one stream, so its index is implied by the lane. A lane's end is its channel closing.

### Alignment

- **One align stage per direct stream stages its own stream** into its ring, beyond the released
  head: `Fresh` at the candle's column; `Gap` at each skipped due column, proven by the stream's own
  next candle; `Pending` on a deadline marker (live); `Ended` at `lastCol + Period` when its input
  closes. A lane that accepted no candle reports that it ended before starting and never enters the
  expected set.
- **Evaluate.** After writing a column's slot, align sends `evaluate(stream, col, state)` to
  readiness.
- **Ownership by evaluate.** Align is the only writer of its stream's slots until it sends that
  column's evaluate; afterwards the slot belongs to readiness and the engine. A candle for a column
  already evaluated is `Late`: forwarded, never written. Every slot has one writer at any moment, with
  no locks — the channel send publishes the write.
- **Report before blocking.** The first candle never waits — a stream may fill its whole ring before
  anything is read — so its `Fresh` evaluate at `f` also decides the columns before `f`. Gaps are
  written one due column at a time, each after its own capacity wait, then reported. Reporting a gap
  before writing its slot would release a column whose slot is missing; writing a long gap before
  reporting any of it would deadlock once it exceeds the ring.
- **Ring capacity is per stream.** Align writes a column once its slot holds nothing a view may still
  read: older than the stream's first column until the engine's history passes it, then older than
  `latest(consumed) − (bars − 1)·Period`. At startup a stream fills its whole ring; in steady state it
  writes up to `consumed + spare·Period`, `spare` being the ring's slots beyond its kept bars.
  Otherwise it waits on the gate — the engine's consumed column, woken by consumption — and its
  provider blocks (replay) or buffers and drops (live). Only the stream ahead blocks.
- **The carried candle comes from the stream's previous slot** — align's own last write, which
  nothing else writes in iteration 1. From iteration 2 the engine writes `Late` candles into released
  slots, so align will keep it in its own state.

### Deadline (live)

- **Its own lane stage, between normalise and align, live only.** The replay composer leaves it out,
  so align is identical in both modes and free of time.
- **The deadline belongs to the column:** `boundary(c) + lateness`, `boundary(c) = c × Stride`. It is
  known before any data arrives and compared with the injected `Clock` (the NTP-synced host clock);
  `ReceivedAt` plays no part.
- **One timer per lane**, armed at the stream's next due column. When it fires with no candle for
  that column, the stage first reads any candle already waiting on its input, then sends a `Pending`
  marker. A candle for a later column before the deadline is a gap proof; the timer moves to the next
  due column.
- **It never writes slots.** The marker and the candles travel through one ordered channel, so align
  sees them in a definite order: the marker becomes a `Pending` slot and an evaluate; a candle for that
  column afterwards is `Late`.
- **Readiness has no clock.** Every evaluate comes from an align stage. Owning the deadline in the
  lane removes the race between a deadline decision and a candle being staged; the alternative —
  readiness owns one timer and seals each stream under a lock — was rejected for the shared lock.
- **Per-stream lateness** becomes a setting of each deadline stage when it is needed.

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
- **One readiness stage, one rule, no clock.** On every evaluate it releases in column order while
  every stream due at the next column is decided. Evaluates come from the align stages — staged, gap,
  pending, ended. Replay and live differ only by the deadline stage: replay decides by present, gap or
  ended; live adds pending.
- **Readiness keeps per-stream progress**, not a count per column: `through`, every due column up to
  it decided (∞ once ended). Each evaluate checks the next column, resuming at the stream that
  blocked the last check — O(1) per evaluate, O(N) per column. Completeness is read from the `View`
  pinned at the column. The first frame is the earliest first candle, known once every stream has
  reported.
- **A column goes out only once some stream has written at or beyond it** (`reach`, the latest
  `Fresh` or `Gap` column). Columns where no stream is due wait for the next bar, and a run ends at
  the latest `Fresh` column in any arrival order.
- **Evaluates reach readiness through `pipe.FanIn`.** align → `FanIn` → readiness costs 527 ns per
  evaluate at 1000 1m streams (≈ 0.53 ms per column, ≈ 4.6 min per year of 1m data); per-column
  atomic counters were not needed.
- **Live: data trigger plus deadline fallback.** A column goes out as soon as every due stream is
  present or proven missing — no wait for lateness. A `Pending` marker decides only a stream still
  missing at `boundary + lateness`. With lateness under the grid step at most one column waits at a
  time; several deadlines pass together only when the engine stalls beyond headroom, the process
  pauses or the clock jumps, or at startup and reconnect.
- **No ordered merge.** Gaps are proven by each stream's own next candle; skew is bounded by
  per-stream ring capacity (Period). No final-column special case either: an ending lane's `Ended`
  slot decides its remaining columns, and `reach` ends the run.
- **Partial and Complete frames reach every component**; each decides what to do with them.
- **Lanes are assumed time-ordered.** Replay relies on it — gaps are proven from each lane's own order
  and no `Late` occurs; a live lane delivering out of order is repaired by `Late`.
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
- **Release, then materialise.** The align stages write base candles into slots beyond the released
  head; readiness decides, marks columns released and emits their events. On dequeue,
  `data.Next()` — data layer code on the engine goroutine — materialises the column: a `Late` event
  writes its revised base candles and recomputes the derived slots it affects up to the last processed
  column; a `Current` / `Backfill` event aggregates the column's derived legs. Then it returns the view.
- **Slot ownership.** A stream's align stage writes its base slots only until it sends their
  evaluate — always beyond the released head. Released slots are written only on the engine
  goroutine — materialisation and the phases. A late candle travels in its `Late` event and is never
  written into a released slot by the data layer.
- **A revision becomes visible at its `Late` event's position.** Frames before it see the old data;
  the `Late` frame applies it and triggers recompute; frames after see revised data with corrected
  history. No frame ever mixes old and revised values.

### Capacity

- **Depth = deepest declared read + headroom**, within the 365-day cap. Headroom sizes each ring's
  slots beyond its kept bars; align's limit is the ring's capacity from the oldest readable bar
  (§ Alignment) — the whole ring at startup, `consumed + spare·Period` in steady state. It covers
  staged slots and released-but-unconsumed events.
- **`defaultHeadroom` is 128 columns**; the event channel holds as many. Readiness blocking on a full
  event channel costs throughput only: the engine's consumption depends on events alone.
- **When a stream's ring is full its align stage blocks, in replay and live alike**, pushing
  backpressure to that stream's provider. The engine's consumption wakes it.
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
  at least its largest derived window + 1; each ring adds `ceil(headroom / Period)` slots plus one
  spare. The 365-day cap applies per stream; exceeding it is a construction error naming the stream.
- **Reads stop at a stream's first column**, recorded before its first write: filling the ring at
  startup wraps onto the slots of earlier columns, which views still probe.
- **No per-column storage.** `Complete` travels with the event and its `View`; the released head and
  each stream's progress are readiness's own state.
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
- **One write path, `put`,** for both writers. Ownership is a convention: a stream's align stage
  writes its direct slots until it sends their evaluate, the engine goroutine only released columns;
  a violation shows up as a data race under `-race`. Rewriting a column replaces its slot.

### Reversed on 2026-10-22

Superseded by § Alignment, § Deadline, § Release and § Capacity above; kept for context.

| Was | Now | Why |
|---|---|---|
| Replay ordering is `OrderedFanIn` over every lane, with end-of-lane markers at the next due column, final-column release on merge close, and a heap | No ordered merge; gaps proven by each stream's own next candle | Per-stream blocking bounds skew without a global order |
| Ordering precedes staging; staging cannot absorb skew | Staging is per stream; skew is bounded per stream | The deadlock came from one goroutine blocking every stream |
| One aligner goroutine stages, decides and commits | Align per stream (staging), readiness once (ready → commit) | Placement is per stream; only release needs every stream |
| One timer in the release loop, at the oldest unreleased column's deadline | A deadline stage per lane; readiness has no clock | Removes the race between a deadline decision and a candle being staged |
| Live: `FanIn` arrival order into staging | Lanes stage directly; evaluates reach readiness | Follows from per-stream staging |
| The aligner writes direct slots for columns it has not released | A stream's align stage writes its slots until it sends their evaluate | Ownership hands over at the evaluate |
| Readiness keeps a count per column against the streams due there | Per-stream progress checked at the next column; release capped at `reach` (stage 4) | Starts and ends decide whole ranges of columns, which a count per column cannot hold |
| Align blocks beyond `consumed + headroom`; a start report "not started before `f`" | Ring capacity from the oldest readable bar; the first candle never waits (stage 4) | A stream may fill its ring before anything is read; headroom only sizes the ring |

## Solution

Detail below the decisions; built per `docs/plans/data-layer-v2` as far as WIP lists.

### Pipeline

```
          ┌──────────────────────────────── data layer (internal/engine/data) ──────────────────────────────┐
          │ per direct stream, one goroutine per stage                    │ 1 goroutine │ engine, on dequeue │
provider_i ─▶ normalise ─▶ deadline (live) ─▶ align ─evaluate─┐                                               │
provider_j ─▶ normalise ─▶ deadline (live) ─▶ align ─evaluate─┼─▶ readiness: ready → commit ─▶ events ─▶ Next():  │
          │                                     │               ┘                               materialise      │
          │                                     └─ slots beyond the released head ── FrameBuffer ──┘           │
          └──────────────────────────────────────────────────────────────────────────────────┬──────────────┘
                                                                            view + section writer per phase
                                       engine step: features → strategies → portfolio → execution
```

| Stage | Runs on | Job |
|---|---|---|
| Ingest | Provider goroutine | Provider lifecycle, `ReceivedAt`, errors; venue format → `Candle` in ms. Under backpressure the provider's buffered output absorbs, then drops (live) |
| Normalise | One goroutine per direct stream | Rules in order (Decisions § Normalisation): validate, column (`ceilDiv`), due column, identical duplicate, column order; drops logged once per rule and summarised. µs scaling is the provider's ([[data-layer]] defect 7) |
| Deadline | One goroutine per direct stream, live only | Timer at the next due column's `boundary + lateness`; sends a `Pending` marker when the stream misses it (Decisions § Deadline) |
| Align | One goroutine per direct stream | Stage the stream's slots (`Fresh`, `Gap`, `Pending`, `Ended`) beyond the released head, within its ring's capacity; send an evaluate per resolved column; forward `Late` candles (Decisions § Alignment) |
| Readiness | One goroutine | On each evaluate, joined through `pipe.FanIn`: per-stream progress checked at the next column; release decided columns within `reach`, in order, with completeness read from the `View`; emit `Current` events, and `Late` events after their column |
| Materialise | Engine goroutine, in `Next()` | `Late`: write revised base candles, recompute affected derived slots up to the last processed column, record revision metadata. Otherwise: aggregate the column's derived legs |

- **Why aggregation runs at dequeue.** Aggregating in the data layer would read base slots of the
  last `W` columns while a dequeued `Late` writes one of them — a data race. Run on the engine
  goroutine, released slots have a single writer, and released-but-unconsumed columns are aggregated
  when their own event is processed, so they pick up revisions without extra work.
- **Publication.** Align writes a slot, then sends its evaluate; readiness releases a column only
  after every due stream's evaluate, then sends the event; the engine reads after receiving it. Views
  are pinned at or below the engine's column, never ahead of the released head.
- **Only direct streams, at their due columns, are in the expected set.** Derived legs are functions
  of base legs and inherit their state, so aggregating after readiness changes no release decision.
- Release keeps the clock split in [[pipeline-runtime]] § Clock: the data layer decides frame
  content and status, the engine paces. Only the deadline stage reads the clock, and it is injected.
- Venue-format normalisation stays in the provider ([[data-layer]]).

### Release rule

For column `c`, each direct stream due at `c` is:

| State | Condition | Leg state |
|---|---|---|
| Present | Its align stage staged a candle at `c` | `Fresh` |
| Proven missing | Its own next candle is beyond `c` — lanes are ordered | `Gap` |
| Presumed missing | Live only: its deadline stage sent a `Pending` marker for `c` | `Pending` |
| Ended | Its lane closed at or before `c` | `Ended`, out of the expected set |
| Undecided | No evaluate for `c` yet | Column held |

- **In-order release is guaranteed** given ordered lanes and deadlines that grow with the column: if
  `c+1` is decided, `c` already is. A column is never first released behind a newer one, so `Late`
  only ever means revision.

```go
// readiness, on every evaluate (from align: staged, gap, ended; live: pending)
for next := head + 1; decided(next) && next <= reach; next++ {
    emit(next, complete(next)) // Current; Complete, or Partial with Gap / Pending legs
}
// decided(c):  every stream due at c is present, gap, pending or ended — through[s] ≥ c
// complete(c): every active stream's latest due candle at or before c is present — read from the View
// reach:       the latest Fresh or Gap column across streams
```

- **Order holds.** A complete column past an undecided one waits for it.
- **One timer per lane, not polling.** Each deadline stage arms one timer at its stream's next due
  column and re-arms after deciding it.
- **No re-emit on proof after `Pending`.** A `Pending` leg later proven missing changes no data, so it
  produces no `Late`; only arriving data revises a released column.
- **Clock skew.** Deadlines compare the local clock with venue timestamps, so lateness also absorbs
  skew; the host clock is NTP-synced.
- **Per-stream lateness, later:** stream `s` is presumed missing at `c` once
  `now ≥ boundary(c) + lateness(s)` — a setting of its deadline stage.
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
| Replay | A gap is proven by the stream's own next candle; no deadline, no `Pending`, no `Late` |

| | Backtest | Live |
|---|---|---|
| Lane stages | normalise → align | normalise → deadline → align |
| Skew | Bounded by per-stream ring capacity | None, except startup history |
| Gap proof | Stream's own next candle | Stream's own next candle |
| Deadline | None | Deadline stage, hardcoded lateness |
| Revisions | Never | Any column still in the buffer |

### Events

```go
package data // internal/engine/data

type Status uint8

const (
    Current Status = iota
    Late     // iteration 2
    Backfill // iteration 3
)

type Event struct {
    Status   Status
    Complete bool
    Col      int64 // absolute column (boundary ms / stride); Current: new head; Late: revised column
    Legs     []Leg // iteration 2, Late only: revised base candles; Current data is already in the row
}

type Leg struct {
    Stream int // index into the layer's stream table
    State  LegState
    Data   proto.MarketData
}
```

Columns are absolute, never ring slots. `Col` is carried on every event so "Current is the newest
column" is checked rather than assumed by consumers.

```go
// align → readiness, internal
type evaluate struct {
    stream int
    col    int64    // the column resolved; Ended: from col on, −1 for a lane without a candle
    state  LegState // Fresh, Gap, Pending or Ended
}
```

A stream's first evaluate is `Fresh` at its first column `f`, sent without waiting; it also decides
the columns before `f`.

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
    first atomic.Int64 // first column; reads never go below it
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
- **Ring capacity keeps staging off readable history.** Staged writes wrap a ring onto its oldest
  slots; align writes a slot only once it holds nothing a view may still read, and reads stop at the
  stream's first column.
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

- **Generic pipeline tooling** — `pipe.Pipeline[In, Out]` with `Lanes` and `Then` so the data layer
  pipeline is a typed instance rather than a module-internal struct; proposed, undecided.
- **Lateness** — per venue / timeframe from measured arrival delays; hardcoded until then.
- **Guaranteeing replay lane order** — e.g. sorting each lane once at dataset conversion; the CSV
  quality check can flag violations. Until then, an out-of-order source file yields `Late` in replay.
- **Live reorder tolerance** — with lateness well below the grid step the deadline usually decides
  first (`Pending`); proof matters live on fine grids or reconnect bursts, where out-of-order delivery
  is likely. Whether to wait briefly before trusting a proof.
- **Headroom size for live bursts** — replay uses `defaultHeadroom` (128 columns); live startup
  warm-up and reconnect backfill (iteration 3) may need more.
- **Provider drop contract** — buffer size per provider, whether drops are reported (e.g. on the
  error channel), and whether sustained drops raise an overload signal so risk can halt trading.
- **Warm-up ownership** — `Backfill` implies the data layer drives historical warm-up ([[data-layer]]).
  Startup history has replay's skew — a 1h lane's history spans 60× the data time of a 1m lane's.
  Per-stream ring capacity bounds it without deadlock (Period), but a live provider must then block rather
  than drop while it delivers history.
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
  `docs/plans/data-layer-v2/stage-3-lanes.md`. Planned and built before the 2026-10-22 redesign
  (Decisions § Reversed).
- Stage 3.1 (per-stream lanes: the ordered merge and end marker removed, normalise as the lane's
  first stage, one output per lane) implemented — `docs/plans/data-layer-v2/stage-3.1-lanes.md`.
- Stage 4 (align stages and the gate, readiness, each ring's first column; the composer joins the
  lanes with `pipe.FanIn`) implemented — `docs/plans/data-layer-v2/stage-4-align-readiness.md`.
- **Plan impact of the redesign**, reflected in `main.md`: iteration 2 gains the deadline stage,
  alongside `Pending` and `Late`.

## Resources

- `internal/flow/align/synchronizer_v2.go`, `internal/flow/pipe/fanout.go`,
  `internal/domains/features/feature.go`, `internal/protocol/provider.go`,
  `internal/protocol/snapshot.go`, `internal/engine/ticker.go`, `cmd/wire.go`
- Related: [[data-layer]], [[pipeline-runtime]], [[multi-subscription]], [[features]], [[config]]
