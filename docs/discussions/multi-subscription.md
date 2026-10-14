# Multi-Subscription Flows

## Topic

Every flow in the engine today carries exactly one `Subscription` — one symbol at one timeframe.
`Architecture.md` targets hundreds to thousands of concurrent streams, correlated-asset signals and
multi-asset portfolios, none of which are reachable from a single-subscription pipeline.

Objective of this discussion: work out how multiple subscriptions enter the design — where they are
declared, how they are carried through the pipeline, and whether they advance under a shared clock
or independently. Four subtopics are under active research:

1. Multiple timeframes for the same symbol.
2. Fully independent multi-subscription flows.
3. Scope of subscriptions in config — top-level, nested per-engine, or per-provider — and the
   resulting dependency structure in code.
4. Actively synchronising subscriptions (through the ticker module, for example) versus letting them
   flow freely from providers and resolving synchronicity downstream.

This is a research topic. Two clusters are now decided — the config scope (Solution §3) and the
strategy/feature input seam (Solution §5); the rest remains open and no implementation plan exists
yet.

## Current Understanding

### What is single-subscription today

| Site | Shape |
|---|---|
| `proto.Subscription` | `{Symbol string, Timeframe Timeframe}` — one of each. |
| `proto.DataProvider.Init(sub, limit)` | Binds a provider **instance** to one subscription. |
| `config.Config.Subscription` | One top-level `subscription` block, injected as `_subscription`. |
| `EngineConfig.Provider` | A single provider name per engine. |
| `FeatureChain` | One shared `SortedWindow[MarketData]` across all features. |
| `Ticker.Gate(ctx, in)` | Gates one channel; `fixedInterval` releases one value per tick. |
| `RiskManager.Init(data, …)` | One market-data channel. |
| `Strategy.Init(data)` | One market-data channel. |

Notably, `subscription` has already been hoisted out of the engine block to the top level of
`config.json` — a directional hint about scope, but not a decision about multiplicity.

### What already helps

- **Streams are self-describing.** `MarketData` carries `Symbol`, `Timeframe` and `ProviderID`, so
  an interleaved stream can be demultiplexed downstream with no side channel. The identity of a
  stream is effectively the triple `(provider, symbol, timeframe)` — a "stream key" the code has no
  name for yet.
- **`SortedWindow[T]` is generic over `TimeStamped`**, not tied to `MarketData`, so a per-stream or
  keyed window needs no change to the buffer itself.
- **`flow/pipe` already has the combinators** — `FanOut`, `FanIn`, `FanInTo` — though `FanIn`
  explicitly makes no ordering guarantee, which matters for subtopic 4.
- **`buy_the_dip` already keys its state by symbol**, anticipating multi-symbol strategies even
  though nothing can feed it more than one.

### The hard blocker

`FeatureChain` holds **one** window shared by every feature, sized to the maximum `Lookback()`. Push
two subscriptions into one chain and every feature reads a window with interleaved symbols and
timeframes — silently wrong output, not an error. Any multi-subscription design must either run one
chain per stream or make the window key-aware. See [[features]].

The second-order consequence: per-stream chains mean memory scales as
`streams × max_lookback × sizeof(MarketData)`, which is the real constraint on "hundreds of streams",
and they make cross-asset features (correlation, spread, beta) structurally impossible — `Feature.Update`
receives a single window. Cross-asset features are the explicit motivation for multi-stream support in
`Architecture.md`, so this tension sits at the centre of the topic rather than at its edge.

## Decisions

Two clusters are committed: the **config scope** (Solution §3) and the **strategy/feature input
seam** (`SyncEvent`, Solution §5). The constraints below still bound the rest of the design space and
are load-bearing:

- **Cross-stream synchronization is a detached pipeline stage, not the strategy's job.** Strategies
  and cross-asset features consume one `<-chan SyncEvent`; alignment lives in a synchronizer
  combinator upstream. Full contract in §5.
- **A stream is identified by `(provider, symbol, timeframe)`.** Demux downstream is always possible
  because `MarketData` carries all three; no design needs a parallel identity channel. Reified in
  `protocol` as `StreamKey{Provider, Subscription}`; consumers index the seam by `StreamKey` (§5).
- **One subscription per provider instance** — kept for increment 1 (a provider block is a template
  materialised once per sub, subtopic 3); increment 2's multiplexing venues may take many subs per
  instance via `Init([]Subscription)`.
- **Feature state is per-stream, not per-engine.** Forced by the shared-window blocker above,
  regardless of which option the rest of the design takes.

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

Worth treating as the likely first increment even if it is not the destination, because it is the
only option that is additive rather than structural.

### 3. Scope of subscriptions in config — decided

**Decided.** Subscriptions are a **top-level named list**, siblings of providers/features/strategies,
each naming the provider that serves it:

```json
"providers":     [ { "type": "binance_csv", "name": "binance" } ],
"subscriptions": [ { "name": "btc_1m", "symbol": "BTCUSDT", "timeframe": "1m", "provider": "binance" },
                   { "name": "btc_1h", "symbol": "BTCUSDT", "timeframe": "1h", "provider": "binance" } ],
"strategies":    [ { "type": "pairs_trade", "subscriptions": ["btc_1m", "btc_1h"] } ]
```

The load-bearing choices:

- **Named, top-level.** A sub is independent of who serves it (repoint by changing one field), and
  strategies reference subs by name — a stable handle beats a positional index or a bare
  `(symbol, timeframe)` tuple. Generalises today's singular top-level `subscription`; `engine.provider`
  and the singular `subscription` block are removed.
- **Each sub names its provider.** Providers gain an instance `name` (default = `type`) so assignment
  is general — the one new addressability primitive, since the composition root keys providers by
  `type` today. The reference resolves in the composition root, the same static check already emitted
  for `engine.provider`.
- **A provider block is a config template.** Increment 1 materialises one provider instance per
  subscription bound to it (→ N instances), leaving `Init(sub, limit)` untouched. Increment 2's
  multiplexing venue (`Init([]Subscription)`, [[data-layer]]) can collapse many subs onto one
  instance *without changing the config model* — only the instantiation strategy changes.
- **Features: global set, one chain per sub** — every sub runs the same feature set as an independent
  chain instance. Per-sub feature sets are a clean later addition (a sub gains an optional `features`
  ref), enabled by named subs.
- **Strategy→sub assignment is composition-root level.** The strategy's config block lists its
  subscription names; the composition root validates each reference and wires the matching feature
  chains into that strategy's `SyncEvent` synchronizer (§5).

**Validation split** — this is what keeps subscription checks out of `Init`: every cross-reference
(sub→provider, strategy→sub, engine→strategy) resolves and is checked in the composition root /
`--validate-only`, no I/O. Only *capability* validation (does the venue actually carry this symbol at
this timeframe) stays at `Init`, because it is a network/file call and factories may not do I/O. The
strict per-component decoding from [[config]] extends unchanged — a typo in a symbol or timeframe
remains a startup error.

Rejected: **per-provider** and **per-engine** scoping — a provider cannot validate symbol existence
without I/O anyway, so nesting subs under it buys no earlier validation. **Top-level named-map with
by-name references everywhere** was folded down to the list-with-`name` above: same static-validation
benefit, less indirection, consistent with the existing array-of-blocks config style.

### 4. Active synchronisation versus free flow

This is the deepest of the four, and it is the [[engine]] fan-out policy question in a new guise —
the same determinism-versus-liveness axis called out as a cross-cutting theme in the index.

- **Actively synchronised, e.g. through the ticker.** A coordinating ticker advances a single logical
  clock and releases values across all streams in timestamp order. Deterministic and reproducible,
  which the methodology in `docs/Backtesting.md` requires. It is also the only way to prevent
  look-ahead in a multi-stream backtest: without a shared clock a strategy can see a 1h bar closing
  at `t+1` while still processing 1m bars at `t`. Costs: the slowest stream paces everything, it
  needs bounded lookahead buffering to order across streams at all, and `Ticker` grows from a
  single-channel gate into a scheduler aware of many streams — a substantially larger interface than
  `Gate(ctx, in)`.
- **Free flow, synchronicity resolved downstream.** Providers emit independently; consumers align
  what they receive. Matches live trading, where the market cannot be made to wait, and keeps
  `Ticker` as-is. Costs: non-reproducible ordering, and every consumer that cares about alignment
  re-implements it.
- **Hybrid by mode** — free flow live, coordinated replay clock in backtest. Correct in both, at the
  cost of two paths that can drift, and it forces the alignment semantics to be specified twice.

The hybrid is probably where this lands, but it should be settled together with the fan-out policy
rather than separately, since both are the same decision about whether the pipeline is allowed to be
non-deterministic. Its concrete home is the input seam in §5, which localises the whole determinism
decision into a single combinator.

### 5. Strategy/feature input seam — `SyncEvent`

**Decided (increment 1 / Path A).** Synchronization is pulled out of every consumer into a
`flow`-level combinator sitting between the per-stream feature chains and their consumers.
Strategies and cross-asset features both read one channel of aligned events; neither aligns streams
itself. This resolves the strategy input contract left open in [[strategy-layer]].

```go
// StreamKey and SyncEvent live in protocol, under the subscription types (beside Subscription).
type StreamKey struct {
    Provider     string
    Subscription Subscription // {Symbol, Timeframe} — full runtime stream identity
}

type SyncEvent struct {
    Time       int64                                // boundary CloseTs the group aligns on
    Now        []ExtendedMarketData                 // legs that closed at Time (≥1), stably ordered
    NowFwdFill map[StreamKey]ExtendedMarketData     // as-of latest per stream; Now ⊆ NowFwdFill
}

// consumer contract (strategies, cross-asset feature stages)
Init(in <-chan SyncEvent) error
```

`StreamKey` (provider + subscription) is the stream's identity everywhere — at wiring, inside the
synchronizer, and as the key consumers index `NowFwdFill` by. A consumer declares its inputs by
subscription *name* (config); the engine resolves each to a `StreamKey` at wiring and hands the
consumer that mapping, so it indexes the event and keeps two streams sharing `(symbol, timeframe)`
across venues distinct — cross-venue arbitrage on the same instrument stays expressible. Feature-key
dependencies are declared per `StreamKey` too.

Semantics:

- **Grouping is by candle close time.** All legs whose `Candle.CloseTs == Time` land in one event; a
  single-stream boundary emits a one-element `Now`. The finest stream paces emission (1m + 1h ⇒ one
  event per minute; on the hour both close and `Now` carries two).
- **`Now` is what just closed; `NowFwdFill` is the full aligned picture** — exactly one (latest) EMD
  per stream, non-advancing legs forward-filled from their last close. `Now` is always a
  subset of `NowFwdFill`. Example at a 10:37 1m boundary: `Now = [1m@10:37]`,
  `NowFwdFill = {1m@10:37, 1h@10:00}`.
- **One value per stream, never history.** Lookback stays the consumer's own window — a `corr(a,b)`
  feature buffers the aligned pairs itself, as features already own their window ([[features]]). The
  seam aligns *across* streams at one instant; the window buffers *within* a stream over time.

Phasing — same `SyncEvent` contract both times, only the watermark changes:

- **Increment 1 — arrival-order, flush-on-advance.** Buffer by `Time`, flush when a later `CloseTs`
  arrives. Cheap, live-correct, and knowingly *not* backtest-safe (arrival order ≠ timestamp order,
  so a replay can reorder or leak look-ahead). Accepted for increment 1 — see the determinism theme
  in [[engine]].
- **Increment 2 — timestamp-ordered k-way merge.** Strong watermark, deterministic, backtest-safe;
  swaps in behind the same contract with no consumer change, and becomes the single home of the
  determinism-vs-liveness policy (subtopic 4).

## Open Questions

- Derive coarse timeframes by aggregation, or subscribe to each independently? If aggregation, what
  component owns it?
- Should `Lookback()` become duration-based rather than candle-based once one engine sees several
  timeframes?
- ~~Where are subscriptions declared, and does a provider instance bind to one subscription or many?~~
  **Decided** — top-level named list, one provider instance per sub in increment 1 (subtopic 3);
  many-per-instance (`Init([]Subscription)`) stays open for increment 2's multiplexing venues.
- Does `RiskManager.Init` take one merged data channel or a keyed set? A merged channel is a smaller
  change but discards the provenance risk needs for per-symbol marking.
- A cross-asset feature consumes `SyncEvent` as a stage after the per-stream chains (§5); still open
  is what it *emits* — a plain `ExtendedMarketData`, or another `SyncEvent` for chaining?
- ~~Where does `StreamKey` live?~~ **Decided** — `protocol`, beside `Subscription`, as
  `StreamKey{Provider, Subscription}`; `SyncEvent` alongside it. Consumers index by `StreamKey` (§5).
- What is the memory and goroutine budget per stream, and at what stream count does the current
  channel-per-stage topology stop paying? Nothing measures throughput or occupancy today
  ([[engine]]), so the "hundreds of streams" target cannot currently be evaluated.

## WIP

- Seam contract decided (§5); no implementation, no plan, no code yet.
- Still entangled with two decisions elsewhere: the fan-out determinism policy ([[engine]]), which
  increment 2's synchronizer converges with, and the multi-symbol provider shape ([[data-layer]]).
  The strategy input contract ([[strategy-layer]]) is now settled by §5.
- The per-stream feature chains plus the increment-1 (arrival-order) synchronizer are the
  prerequisites an implementation plan would start from.

## Resources

- `internal/protocol/provider.go` (`Subscription`, `DataProvider`, `MarketData`)
- `internal/config/config.go` (top-level `subscription`), `config.json`
- `internal/domains/features/feature.go` (`FeatureChain`, shared window)
- `internal/flow/timing/timing.go` (`Ticker`, `Gate`), `internal/flow/pipe/fanout.go`
- `docs/Architecture.md` (concurrent-stream and multi-asset goals), `docs/Backtesting.md`
- Related: [[data-layer]], [[features]], [[strategy-layer]], [[engine]], [[risk-management]],
  [[config]]
