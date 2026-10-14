# Multi-Subscription Flows

## Topic

`Architecture.md` targets hundreds to thousands of concurrent streams, correlated-asset signals and
multi-asset portfolios. The engine started out carrying exactly one `Subscription` — one symbol at
one timeframe — end to end.

Objective of this discussion: work out how multiple subscriptions enter the design — where they are
declared, how they are carried through the pipeline, and whether they advance under a shared clock
or independently. Four subtopics:

1. Multiple timeframes for the same symbol.
2. Fully independent multi-subscription flows.
3. Scope of subscriptions in config — top-level, nested per-engine, or per-provider — and the
   resulting dependency structure in code.
4. Actively synchronising subscriptions (through the ticker module, for example) versus letting them
   flow freely from providers and resolving synchronicity downstream.

Three clusters are decided — the config scope (§3), the strategy/feature input seam (§5) and the
synchronizer behind it: **Synchronizer V2**, which replaced the arrival-order V1 (now removed).
Stages 1–4 of `docs/plans/multi-subscription/main.md` are implemented, so `data_logger` runs N
subscriptions through the ordered V2 synchronizer.

## Current Understanding

### State of the code

| Site | Shape |
|---|---|
| `proto.Subscription` | `{Symbol, Timeframe, Provider}`; `ID()` = `symbol:timeframe:provider`. It is the stream key. |
| `proto.MarketData` | `{Subscription, Candle, ReceivedAt}`. `Candle.CloseTs` is the venue's close time verbatim — for Binance **ms, 1ms before the boundary**. `Timeframe` exposes `TimeframeS()` and `TimeframeMs()`. |
| `config.Config.Subscriptions` | Top-level list; required fields, unique `ID()`, provider reference resolved at the composition root. |
| Provider instances | Keyed by instance `name`; each sub is injected the instance its `provider` names (`_providers map[Subscription]DataProvider`). Subs naming the same block **share one instance** and `Init` overwrites its sub — the per-sub materialisation decided in §3 is not implemented. The default config sidesteps it with one block per sub. |
| `data_logger` | Per sub: `provider → ticker → FeatureChain`; lanes → `align.NewOrderedSynchronizer` → one shared exporter, exporting each leg with `CloseTs == Time()`. |
| `FeatureChain` | One `SortedWindow` per chain, one chain per sub — the shared-window blocker below is resolved in `data_logger`. |
| `Ticker` | One instance per lane. `fixedInterval` releases one value per tick regardless of timeframe; `realtime` does not pace. |
| `pipe.FanIn` / `pipe.OrderedFanIn` | `FanIn` merges lanes in arrival order. `OrderedFanIn` is a k-way merge by a `less` func: it emits the least pending value and blocks while any open input has none. |
| `internal/flow/align` | V2 only: `SynchronizerV2.Sync` → `*SyncEventV2`. `NewOrderedSynchronizer` merges with `OrderedFanIn` by `CloseTs` (wired in `data_logger`); `NewSynchronizerV2` merges with `FanIn` (not wired). |
| `Strategy.Init` | `<-chan *SyncEventV2`. Its callers `signal_logger` / `full` are build-tagged out; `buy_the_dip` (`wip`) still takes `<-chan MarketData`. |
| `RiskManager.Init` | One `<-chan MarketData`. |

### Shared-window blocker and its consequence

`FeatureChain` holds one window sized to the maximum `Lookback()`; pushing two subscriptions through
one chain interleaves symbols and timeframes silently. Resolved by one chain per sub (see
[[features]]).

The consequence stands: per-stream chains make memory scale as
`streams × max_lookback × sizeof(MarketData)` — the real constraint on "hundreds of streams" — and
make cross-asset features (correlation, spread, beta) impossible *inside* a chain, since
`Feature.Update` sees one window. Cross-asset features therefore consume the seam (§5) as a stage
after the chains.

## Decisions

- **Cross-stream synchronization is a detached pipeline stage, not the consumer's job.** Strategies
  and cross-asset features read one channel of aligned events; alignment lives in a synchronizer
  combinator upstream. Contract in §5.
- **Synchronizer V2 is the synchronizer.** V1 (arrival-order, `SyncEvent`) is removed;
  `Strategy.Init`, `logger.SyncEvent` and `data_logger` take `*SyncEventV2`.
- **`data_logger` exports each leg with `CloseTs == Time()`** — one record per candle. Candles in
  columns that never complete are not exported: cold start (before every stream has an owning
  candle) and data after one lane ends.
- **V2 columns are in ms (`Timeframe.TimeframeMs()`), mapped by `ceilDiv(CloseTs, strideMs)`** — the
  first boundary at or after the close, so exact-boundary and boundary−1ms (Binance) closes share a
  column. Early closes are absorbed widely: anything in `(B − stride, B]` maps to boundary `B`, so the
  tolerance is a full finest stride (59.999s at 1m, 4m59.999s at 5m), set by the finest timeframe
  present, not the candle's own.
- **Replay ordering is a k-way merge in front of V2** — `align.NewOrderedSynchronizer` composes
  `pipe.OrderedFanIn` (by `CloseTs`, ties by `Subscription.ID()`) with V2. Lanes reach V2 in
  timestamp order, so skew never reaches the window, columns complete in order (all `Current`) and
  output is reproducible. Works under `realtime` and per-lane `fixedInterval` tickers (a leading lane
  blocks; ticks are skipped, values kept). Replay only: the merge waits while any open lane has no
  pending candle.
- **A stream is identified by `(provider, symbol, timeframe)`.** `proto.Subscription` carries all
  three and is itself the stream key; consumers index the seam by `Subscription`. No separate
  `StreamKey` type.
- **One subscription per provider instance** for increment 1 (a provider block is a template
  materialised once per sub, §3); increment 2's multiplexing venues may take many subs per instance
  via `Init([]Subscription)`.
- **Feature state is per-stream, not per-engine.** Forced by the shared-window blocker.
- **Config scope** — top-level subscription list (§3).

## Solution

### 1. Multiple timeframes for the same symbol

The fork is **derive or subscribe**:

- **Subscribe independently** — two provider instances, `BTCUSDT@1m` and `BTCUSDT@1h`. Simple, works
  for any venue. But the two streams can disagree (different source rounding, different arrival
  times), and a 1h bar arrives once per 60 1m bars, so consumers see wildly different rates.
- **Derive by aggregation** — subscribe to the finest timeframe and roll it up. Guarantees the
  coarse series is exactly consistent with the fine one, and one venue connection serves many
  timeframes. Costs a new aggregation concept (a provider decorator? a pipeline stage? a feature?)
  and cannot produce history older than the fine subscription's warm-up.

Aggregation looks stronger for correctness and for connection economy, but it needs a home in the
architecture that does not currently exist. Note also that `Lookback()` is expressed in candles, so
the same number means different wall-clock spans per timeframe — already an open question in
[[features]] and sharper here.

### 2. Fully independent multi-subscription flows

The cheapest generalisation: N parallel single-subscription pipelines — provider instance → ticker →
feature chain → strategy — sharing only the singleton risk manager and the executors.

- Requires no change to `Subscription`, `DataProvider`, or `FeatureChain`.
- Gets multi-asset portfolios immediately, since risk is already the single portfolio owner.
- Does **not** get cross-asset features or correlated signals: the flows never meet before risk.
- Pushes the fan-in problem entirely onto `RiskManager.Init`, which takes one data channel today.

Superseded as the first increment by the seam (§5), but still the shape `RiskManager` fan-in faces.

### 3. Scope of subscriptions in config — decided

**Decided.** Subscriptions are a **top-level list**, siblings of providers/features/strategies, each
naming the provider that serves it:

```json
"providers":     [ { "type": "binance_csv", "name": "binance" } ],
"subscriptions": [ { "symbol": "BTCUSDT", "timeframe": "1m", "provider": "binance" },
                   { "symbol": "BTCUSDT", "timeframe": "1h", "provider": "binance" } ],
"strategies":    [ { "type": "pairs_trade",
                     "subscriptions": ["BTCUSDT:1m:binance", "BTCUSDT:1h:binance"] } ]
```

The load-bearing choices:

- **No name; identity from fields.** A subscription is `{symbol, timeframe, provider}`; its `ID()` —
  the canonical `symbol:timeframe:provider` — is the reference handle, and the `(symbol, timeframe,
  provider)` tuple must be unique across the list. The config subscription *is* `proto.Subscription`.
- **Each sub names its provider.** Providers have an instance `name` (default = `type`); the
  reference resolves in the composition root.
- **A provider block is a config template.** Increment 1 materialises one provider instance per
  subscription bound to it (→ N instances), leaving `Init(sub, limit)` untouched — *not yet
  implemented*, see State of the code. Increment 2's multiplexing venue (`Init([]Subscription)`,
  [[data-layer]]) can collapse many subs onto one instance without changing the config model.
- **Features: global set, one chain per sub.** Per-sub feature sets are a clean later addition (a
  sub gains an optional `features` ref).
- **Strategy→sub assignment is composition-root level.** The strategy's block lists subscription
  IDs; the composition root validates each and wires the matching chains into that strategy's
  synchronizer (§5).

**Validation split:** every cross-reference (sub→provider, strategy→sub, engine→strategy) resolves in
the composition root / `--validate-only`, no I/O. Only *capability* validation (does the venue carry
this symbol at this timeframe) stays at `Init`. Strict decoding from [[config]] applies unchanged.

Rejected: **per-provider** and **per-engine** scoping (no earlier validation, since a provider cannot
check symbols without I/O); **named-map with by-name references** (same validation benefit, more
indirection).

### 4. Active synchronisation versus free flow

The [[engine]] fan-out policy question in a new guise — the determinism-versus-liveness axis.

- **Actively synchronised, e.g. through the ticker.** A coordinating clock releases values across all
  streams in timestamp order. Deterministic and reproducible, as `docs/Backtesting.md` requires, and
  the only way to prevent look-ahead in a multi-stream backtest. Costs: the slowest stream paces
  everything, it needs bounded lookahead buffering, and `Ticker` grows from a single-channel gate into
  a multi-stream scheduler.
- **Free flow, synchronicity resolved downstream.** Providers emit independently; the seam aligns.
  Matches live trading and keeps `Ticker` as-is. Costs: non-reproducible ordering.
- **Hybrid by mode** — free flow live, coordinated replay clock in backtest. Correct in both, at the
  cost of two paths that can drift, and it forces the alignment semantics to be specified twice.

Clock models assessed for the seam:

| Model | Clock | Emission gate | Out-of-order | Jitter |
|---|---|---|---|---|
| **A — arrival-order (V1, removed)** | Next-higher `CloseTs` arrival | Arrival of an advancing candle | Singleton at current time; breaks `Now ⊆ NowFwdFill` and `Time = leg CloseTs` | Immediate, no coalescing |
| **B — data-driven** | Min `CloseTs` > current across subs | New data from any sub | Discarded (immutable boundaries) | Deadline per boundary |
| **Wall-clock** | Real-time tick (N ms) | Any new data since last tick | Not a concept (`Time` = decision time) | Absorbed into tick |
| **V2 — buffer matrix (chosen)** | Column grid at finest stride; newest datapoint advances the window | Column complete (every stream's owning candle present) | Accepted while in window — late frame emitted `Stale`; below window dropped | Absorbed by the window (2× coarsest period) |

V2 tolerates *bounded* disorder but does not order streams; with `FanIn`, replay cross-lane order
is set by per-lane tickers and issue 2 (WIP) showed that leaves skew far outside the window. The
"actively synchronised" option is realised at the seam instead of the ticker: the ordered
synchronizer merges lanes by `CloseTs` in front of V2 (Decisions). The live/backtest split remains
open — `data_logger` uses the ordered merge unconditionally, since every provider today is replay.

### 5. Strategy/feature input seam — `SyncEventV2`

**Decided.** Synchronization is a `flow/align` combinator between the per-stream feature chains and
their consumers. This settles the strategy input contract left open in [[strategy-layer]].

```go
type SyncDataFreshness int

const (
    CurrentMarketData SyncDataFreshness = iota // column after the latest dispatched one
    StaleMarketData                            // column completed behind the latest dispatched one
    AheadMarketData                            // defined, unused
)

// Built by NewSyncEventV2(status, data); read through Time(), Status(), Data().
type SyncEventV2 struct {
    time   int64                // newest leg's CloseTs — the column's time
    status SyncDataFreshness
    data   []ExtendedMarketData // one forward-filled leg per stream, oldest → newest
}

// SynchronizerV2.Sync(map[Subscription]<-chan ExtendedMarketData) <-chan *SyncEventV2

// consumer contract (strategies, cross-asset feature stages)
Init(in <-chan *SyncEventV2) error
```

Semantics:

- **Column grid.** Time is cut into ms columns at the finest timeframe present (`stride`); every other
  timeframe must be an integer multiple. A candle's column is the first boundary at or after its
  `CloseTs`. A stream's *owning column* at `c` is the latest column ≤ `c`
  at which it closes.
- **Complete frames only.** A column dispatches once every stream's owning candle exists. Cold
  start, gaps and stalled streams hold frames rather than emit partial or stale-filled ones.
- **`Data()` is the aligned picture** — one forward-filled leg per stream (V1's `NowFwdFill`). Legs
  with `CloseTs == Time()` are what just closed (V1's `Now`).
- **Dispatch-once, freshness-tagged.** The runner emits each column at most once: `Current` past the
  latest dispatched column, `Stale` for a frame completed behind it. A `Readiness` policy picks which
  complete columns ship and in what order, through read-only state queries; default
  `orderedCurrentReadiness`.
- **Bounded window.** `width = 2 × coarsest period` columns. Below it, data survives only as each
  stream's forward-fill floor; older datapoints are dropped and frames evicted before completing never
  emit.
- **One value per stream, never history.** Lookback stays the consumer's own window — a `corr(a,b)`
  feature buffers the aligned pairs itself ([[features]]). The seam aligns *across* streams at one
  instant; the window buffers *within* a stream over time.

History: V1 (arrival-order, flush-on-advance) was increment 1 — live-correct, not backtest-safe, and
it violated its own invariants under out-of-order arrival. V2 replaces it.

**Plans executed:** `docs/plans/multi-subscription/main.md` (stages 1–4), `docs/plans/synchronizer-v2.md`,
`docs/plans/synchronizer-v2-readiness-split.md`, `docs/plans/synchronizer-v2-test-segregation.md`,
`docs/plans/synchronizer-v2-ms-columns.md`, `docs/plans/synchronizer-v2-ordered.md`,
`docs/plans/synchronizer-v2-migration.md`.

## Open Questions

- How does the engine choose the ordered synchronizer (replay) vs `FanIn` (live)? No replay/live
  switch exists today; the provider type is the obvious signal.
- Live ordering: a timeframe-aware merge (after a close at T, a lane's next close is ≥ T + timeframe,
  so earlier data releases without its head), with a deadline for missing candles.
- Pacing: one ticker on the synchronizer's output (one aligned frame per tick) instead of per-lane
  tickers?
- V2 window `width` policy — fixed `2 × coarsest period` gives same-timeframe sets one column of lag
  tolerance.
- Live: does V2 serve live as well, or does a wall-clock emitter sit beside it (§4)?
- Derive coarse timeframes by aggregation, or subscribe to each independently? If aggregation, what
  component owns it?
- Should `Lookback()` become duration-based rather than candle-based once one engine sees several
  timeframes?
- Does `RiskManager.Init` take one merged data channel or a keyed set? A merged channel is a smaller
  change but discards the provenance risk needs for per-symbol marking.
- A cross-asset feature consumes `SyncEventV2` after the per-stream chains; what does it *emit* — a
  plain `ExtendedMarketData`, or another `SyncEventV2` for chaining?
- What is the memory and goroutine budget per stream, and at what stream count does the
  channel-per-stage topology stop paying? Nothing measures throughput or occupancy ([[engine]]).

## WIP

**V2 migration — done** (`docs/plans/synchronizer-v2-migration.md`). V1 and `proto.SyncEvent` are
removed; `data_logger` runs `NewOrderedSynchronizer`. A manual run of the default config (limit 300)
emits 289 frames, all `Current`, and exports 289 5m + 25 1h records with no duplicates. No test runs
an engine; that run is the only end-to-end check. Two issues had blocked it — the default config
emitted **no events** on V2:

**Issue 1 — `CloseTs` unit and close offset — resolved** (`docs/plans/synchronizer-v2-ms-columns.md`).
V2 assumed `CloseTs` in seconds on the boundary; Binance's is ms, 1ms before it. Fixed with ms
columns and `ceilDiv` (Decisions).

**Issue 2 — lane skew vs the window — resolved for replay** (`docs/plans/synchronizer-v2-ordered.md`).
Per-lane tickers let lanes drift apart in data time, and with `FanIn` the leading lane dragged the
window past the lagging one. The merge has to sit directly in front of V2 — gating at the tickers is
not enough, since the chains' 100-candle output buffers let a backlogged lane lag by more than the
window. Fixed by the ordered synchronizer (Decisions).

## Resources

- `internal/protocol/provider.go` (`Subscription`, `MarketData`, `SyncEventV2`)
- `internal/flow/align/synchronizer_v2.go`
- `internal/engine/data_logger.go`, `cmd/main.go` (provider injection), `internal/config/config.go`
- `internal/domains/features/feature.go` (`FeatureChain`), `internal/flow/timing/timing.go`
  (`Ticker`), `internal/flow/pipe/fanout.go`
- `internal/domains/provider/binance_csv.go` (`CloseTs` parsing), `config.json`
- `docs/Architecture.md`, `docs/Backtesting.md`
- Related: [[data-layer]], [[features]], [[strategy-layer]], [[engine]], [[risk-management]],
  [[config]]
