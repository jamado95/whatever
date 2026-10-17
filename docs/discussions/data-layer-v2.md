# Data Layer V2

## Topic

The data layer as the engine's first layer: a distinct module, `internal/engine/data`, that turns
subscriptions and provider instances into a stream of status-tagged frame events, and defines the
`FrameBuffer` every other layer reads. Continuation of [[data-layer]], which covers providers and
normalisation; the runtime model it serves — synchronous step per frame, status semantics, clock
split — is in [[pipeline-runtime]].

Opened 2026-10-17.

## Current Understanding

### What it replaces

| Today | Site | Becomes |
|---|---|---|
| Provider injection, one instance per sub | `cmd/wire.go` (`_providers`) | Module input |
| `FeatureChain`'s `SortedWindow`, one per chain | `internal/domains/features/feature.go` | `FrameBuffer` stream rings |
| `syncState` matrix, `runV2`, readiness | `internal/flow/align/synchronizer_v2.go` | Staging and release stages |
| `OrderedFanIn` linear-scan merge | `internal/flow/pipe/fanout.go` | Ordering stage (heap merge / precomputed index) |
| `Ticker` gate on the synchronizer output | `internal/engine/ticker.go` | Engine-side pacer ([[pipeline-runtime]]) |
| Per-candle `map[string]any` snapshot | `internal/protocol/snapshot.go` | `FrameBuffer` feature slots |

### Rolling aggregation semantics

A rolling `W` window over a base stream emits one candle per base close, covering the last `W` of
base time. Built only from closed base candles, so it is consistent with [[data-layer]]'s
closed-candles-only decision.

- **Window ≠ cadence.** Rolling 1h from 1m covers 60 minutes, updates every minute, and occupies the
  column grid as a 1m stream. The derived subscription's `Timeframe` is the window.
- **Stepped history.** Consecutive rolling candles share `W−1` base candles. An RSI-14 over the last
  14 rolling 1h candles spans 14 minutes; 14 hourly periods ending now means every 60th rolling
  candle (`t, t−60, t−120, …`) — hourly bars phase-shifted to the current minute.
- **Closed bars come free.** For UTC-aligned windows, the rolling candle at a boundary column is the
  closed bar. Session-defined FX / equity days need a calendar.
- **A late base candle revises `W` derived candles** — columns `c … c+W−1`.
- **O(1) update:** open is the base candle `W` back, close the newest, volume a running sum,
  high / low a monotonic deque (matters at 1d over 1m, `W` = 1440).
- **Memory and warm-up:** RSI-14 on rolling 1d over 1m stores 14 × 1440 ≈ 20k rolling candles
  (≈ 1.1 MB per stream) and needs 14 days of 1m data before its first value.
- **Statistics:** returns over overlapping windows are autocorrelated by construction; backtest
  significance tests on them must account for it.

## Decisions

- **Module `internal/engine/data`.** Internal pipeline stages: provider ingestion, alignment,
  aggregation, buffer ordering logic, frame alignment / readiness.
- **Edges.** In: `Subscription`s and provider instances. Out: a channel of status-tagged frame events
  (`Current`, `Late`, `Backfill` — semantics in [[pipeline-runtime]]).
- **`Late` events carry the column they correspond to; `Current` is the newest column.**
- **Aggregation is rolling**, deriving higher timeframes from lower ones inside the data layer.
- **Derived subscriptions** name their source with `"from"` and have no provider. Their ID is
  `symbol:timeframe:derived_<source ID>`, e.g. `BTCUSDT:1h:derived_BTCUSDT:1m:binance`.
- **`FrameBuffer` is a dedicated object defined by this module and operated by the engine.** The engine
  is its only writer — market data via `Commit`, plus the feature, strategy and execution sections.
  The data layer never writes it.
- **Revision metadata records whether a column was revised and which legs changed.**
- **Fills and order updates are written into the `FrameBuffer` by the engine**; they do not pass through
  the data layer's ingestion or alignment.

## Solution

Proposed detail below the decisions; not yet planned.

### Stages

```
                    per lane (provider goroutine)                 cross-lane (one aligner goroutine)
Subscriptions ─┐
providers ─────┼─▶ ingest ─▶ normalise ─▶ aggregate ─┐
               └─▶ ingest ─▶ normalise ─▶ aggregate ─┼─▶ order ─▶ stage ─▶ release ─▶ events chan
                                                     ┘
```

| Proposed stage | Decided stage | Scope | Job |
|---|---|---|---|
| Ingest | Provider ingestion | Per provider | Provider lifecycle, `ReceivedAt`, errors |
| Normalise | Alignment | Per lane | Validate candles, map `CloseTs` to its column (`ceilDiv`), drop duplicates |
| Aggregate | Aggregation | Per lane | Rolling windows over the base stream |
| Order | Buffer ordering logic | Cross-lane | Replay: heap merge or precomputed frame index. Live: arrival order |
| Stage | Buffer ordering logic | Cross-lane | Staging window (today's `syncState`) holding out-of-order data until release |
| Release | Frame alignment / readiness | Cross-lane | Release on complete or watermark passed; assign status |

- Lane stages run inline in the lane's goroutine; cross-lane stages in one aligner goroutine. Two
  channel hops: lanes → aligner, and the output channel.
- **Aggregation precedes ordering** because it is lane-local. A derived candle closes at the same
  column as the base candle completing it, and both enter the merge together, ordered by the
  subscription-ID tie-break.
- Venue-format normalisation stays in the provider ([[data-layer]]); the data layer's normalise
  stage handles only the column mapping and integrity checks.
- Release follows the watermark model in [[pipeline-runtime]] § Clock: data time in replay, wall
  clock − allowed lateness live. The aligner never reads `time.Now()`.
- Live: a provider's socket reader is never blocked — the lane → aligner channel holds at least one
  boundary burst (N), and a slow engine queues events rather than stalling ingestion.

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
    Status Status
    Col    int64 // Current: new head; Late: earliest revised column
    Legs   []Leg // closed (Current / Backfill) or revised (Late) candles
}

type Leg struct {
    Stream int   // index into the layer's stream table
    Col    int64 // the leg's own column; a rolling revision spans W of them
    Data   proto.MarketData
}
```

- Columns are absolute (boundary ms / stride), never ring slots; the buffer maps column to slot.
- `Col` is carried on every event, `Current` included, so "Current is the newest column" is an
  invariant `Commit` checks rather than an assumption consumers make.
- A `Late` event already contains the derived revisions, since aggregation runs inside the layer.

### FrameBuffer

```go
// single writer: the engine loop
func NewFrameBuffer(l Layout) *FrameBuffer // streams + feature slots + depths

// writes
func (b *FrameBuffer) Commit(ev Event) error                         // market section; Current must be > Head()
func (b *FrameBuffer) SetFeature(stream, slot int, col int64, v float64)
func (b *FrameBuffer) Append(col int64, sec Section, recs ...Record) // signals, orders, fills

// reads
func (b *FrameBuffer) Head() int64                    // newest committed column
func (b *FrameBuffer) At(col int64) (View, bool)      // false if evicted or not yet committed
func (b *FrameBuffer) Revisions(col int64) []Revision // empty if never revised

type Revision struct {
    Stream int // leg that changed
}

type View interface { // pinned: sees only columns ≤ Col()
    Col() int64
    Leg(stream int) (proto.MarketData, LegState) // Closed | ForwardFilled | Missing
    Candles(stream, n, step int) []proto.Candle  // step > 1 for rolling windows
    Feature(stream, slot, n, step int) []float64
    Records(sec Section) []Record
}
```

- **Pinned views** stop look-ahead: a phase processing column `c` cannot read `c+1` even once it is
  committed.
- **Write discipline:** the engine runs one phase at a time and each section is written only during
  its phase. Feature workers may write disjoint cells in parallel — storage is preallocated, so
  distinct elements do not race — and the phase barrier publishes them. No locks.
- **Layout is built before `Start`:** the data layer contributes the stream table; the feature graph
  contributes slots and each reader's `(depth, step)`.
- **Storage:** per stream, a ring indexed by the stream's own candle index plus a column →
  candle-index map, so a 1d stream beside 1m streams holds candles, not 1440 cells each. Feature
  outputs are `[]float64` rings with keys resolved to slots at construction. Depth per stream =
  max(staging window, deepest stepped read).
- **Invariant: buffer depth ≥ staging-window width**, so a `Late` column is always resident.
- **`LegState`** distinguishes a leg that closed at the column, a forward-filled one, and one missing
  at a deadline release; a later `Late` corrects `Missing`.

### Derived subscriptions

```json
{ "symbol": "BTCUSDT", "timeframe": "1h", "from": "BTCUSDT:1m:binance" }
```

- `Provider` is set to `derived_<source ID>`, so `Subscription.ID()` is unchanged and yields
  `BTCUSDT:1h:derived_BTCUSDT:1m:binance`. Embedding the full source ID keeps derivations from
  different source timeframes distinct.
- Validation at the composition root: `from` references a direct subscription; the window is an
  integer multiple of the source timeframe; the symbol matches the source; no `provider` field.
- The ID contains extra `:` separators. No production code splits IDs today; anything that does
  later must not assume three parts.

## Open Questions

- **Stage boundaries** — whether the proposed stages map onto the decided ones as tabulated (deferred).
- **Gaps in a rolling window** — emit with a base-candle count, or hold to the watermark?
- **Session calendars** for FX / equity day windows — here, or a separate calendar service
  ([[data-layer]]).
- **Warm-up ownership** — `Backfill` implies the data layer drives historical warm-up, an open question
  in [[data-layer]].
- **`Late` payload size** — a late 1m candle under a rolling 1d revises 1440 derived legs. Ship them in
  the event, or let `Commit` recompute derived legs from the revised base?
- **`Layout` builder** — how the feature graph registers slots, depths and steps.
- **`Record` typing** — typed sections for signals, orders and fills, or a generic record.
- **`LegState` on deadline release** — forward-fill a missing leg or leave it empty.
- **Multiplexing providers** — `Init([]Subscription)` from [[data-layer]] still open; the module's input
  edge should not preclude it.
- **Exporter** — read from the `FrameBuffer` directly?

## WIP

- Nothing implemented, no plan.

## Resources

- `internal/flow/align/synchronizer_v2.go`, `internal/flow/pipe/fanout.go`,
  `internal/domains/features/feature.go`, `internal/protocol/provider.go`,
  `internal/protocol/snapshot.go`, `internal/engine/ticker.go`, `cmd/wire.go`
- Related: [[data-layer]], [[pipeline-runtime]], [[multi-subscription]], [[features]], [[config]]
