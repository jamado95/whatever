# Strategy Layer

## Topic

Strategy modules consume featured market data, apply trading logic, and emit `Signal`s — statements of trade *intent*, not orders. Strategies may hold internal state where memory between data points is required. Multiple strategies run concurrently and their signals are merged before reaching risk management.

Objective of this discussion: define the intent/order boundary, record why strategies are allowed mutable state when features are not, and confront the fact that no strategy currently compiles.

## Current Understanding

### Contract

`proto.Strategy` (`internal/protocol/strategy.go:12`) mirrors the provider lifecycle:

```
Init(data <-chan MarketData) -> Streams() (<-chan Signal, <-chan error) -> Start() -> Close()
```

The input element type is decided to change from `MarketData` to `SyncEvent` (multi-stream, featured,
aligned — [[multi-subscription]] §5); the code still shows `MarketData` today.

```go
type Signal struct {
	Symbol    string
	Side      Side
	Timestamp int64
	ExpiresAt int64
	Source    string
	Metadata  map[string]any
}
```

`Signal` carries **no size, no price and no order type**. Sizing, pricing and netting are risk
management's responsibility (see [[risk-management]]). `Source` is the emitting strategy's instance ID, so downstream layers can attribute and weight signals. `Metadata` is a free-form escape hatch for strategy-specific context (`buy_the_dip` puts trend maturity and pullback depth there).

### State of implementations

**No strategy compiles into the default build.**

- `internal/domains/strategy/start.go` contains only `package strategy`.
- `internal/domains/strategy/buy_the_dip.go` is behind `//go:build wip`.

Consequently `reg.Strategies` is empty at runtime, `config.json` has `buy_the_dip` disabled and
`engine.strategies: []`, and the `signal_logger` engine — whose entire purpose is to run strategies — has nothing to run.

### `buy_the_dip` review

The wip strategy detects an uptrend, waits for a pullback of `PullbackPercent` from the recent high, and emits a buy. Trend maturity is graded `NoTrend → EarlyTrend → ConfirmedTrend → MatureTrend` based on consecutive higher closes and counts of higher-highs / higher-lows.

Defects found while reviewing:

1. **Swing highs are never detected.** `isSwingHigh := false` at `buy_the_dip.go:246`, and the loop below it can only ever set it to `false`. It should initialise to `true` (the swing-low block immediately below does exactly that). `hhCount` is therefore always 0 and `ConfirmedTrend` / `MatureTrend` are unreachable — only `EarlyTrend` can fire.
2. **Side case mismatch.** It emits `Side: "BUY"` while `proto.Buy == "buy"`. Nothing compares against the constant yet, so this is latent.
3. **Duplicated swing logic.** The same detection lives (also broken) in the `swing_highlow` feature. This is exactly the kind of derived metric that belongs in the feature layer.
4. **Config is ignored.** The registry factory calls `DefaultBuyTheDipConfig()` and never reads `opts`, so the `trendLength` / `pullbackPct` in `config.json` have no effect.
5. **Consumes `MarketData`**, computing indicators inline, rather than reading a `Snapshot`.

Points 3 and 5 are the interesting ones: they are symptoms of the interface gap, not of sloppiness. With no way to receive features, a strategy has no choice but to recompute them.

## Decisions

- **Signals are intent, not orders.** A strategy says "I want to be long BTCUSDT now", never how much
  or at what price. Sizing needs portfolio-wide state a strategy cannot see — see
  [[risk-management]].
- **Strategies may hold mutable state**, unlike features ([[features]]). Strategy logic is
  path-dependent, and forcing it through a shared window would push strategy state into features.
  Cost: strategies are not trivially reproducible and need reset semantics for backtesting.
- **Strategies are per-instance identified**, so concurrent instances with different parameters stay
  distinguishable in signal attribution — a prerequisite for parameter sweeps.
- **Signals carry an expiry**, letting risk discard stale intent. Nothing consumes it yet.

## Solution

Current shape — one goroutine per strategy, `select` on `done` and the data channel, signals pushed to a buffered channel, merged by the engine via `pipeline.FanIn`.

The input contract is **decided** in [[multi-subscription]] §5: a strategy receives a single
`<-chan SyncEvent` carrying featured `ExtendedMarketData` for the subscriptions it declares, aligned
across streams by a synchronizer stage upstream. This subsumes the "receive `ExtendedMarketData`"
option below (`SyncEvent.Now` / `NowFwdFill` hold EMDs) while detaching cross-stream alignment from
the strategy. The options weighed, kept for context:

- **`ExtendedMarketData`** (the `WIP.md` ToDo). Minimal change: swap the channel element type in
  `Strategy.Init`. Strategy pulls what it needs via `GetSnapshot`. Simple, but every strategy sees every feature, and a missing key is a runtime miss rather than a startup error.
- **Declared feature dependencies, like features have.** Strategy declares `[]KeyRef`; the engine validates at wiring time that the configured feature set satisfies every strategy. Catches misconfiguration before the first candle, and makes the strategy's inputs self-documenting. More interface surface.
- **Typed per-strategy input structs**, built by the engine from the snapshot. Compile-time safety, but requires codegen or heavy boilerplate and couples the engine to individual strategies.

The live remnant of the "declared dependencies" option is **feature-key** validation: `SyncEvent`
settles *which streams* a strategy receives (declared subscriptions, resolved at the composition
root — [[multi-subscription]] §3), but not *which feature keys* within them. A missing `ema_20` is
still a runtime miss unless a strategy also declares `[]KeyRef`. That stays open below.

**Multi-symbol** is likewise settled: one strategy instance receives many streams through the single
`SyncEvent` channel and keys its state by `StreamKey`. `buy_the_dip` already keys state by symbol,
so this widens the key rather than changing the model.

## Open Questions

- ~~Which input contract?~~ **Decided** — `<-chan SyncEvent` ([[multi-subscription]] §5). Still open:
  whether a strategy also declares `[]KeyRef` feature dependencies for startup validation of the keys
  it reads (the live remnant of the "declared dependencies" option).
- ~~One strategy instance per symbol, or many?~~ **Decided** — one instance, many streams, state keyed
  by `StreamKey` ([[multi-subscription]] §5).
- Should `Signal` carry a confidence / strength scalar so risk management can size proportionally, or is that leaking sizing concerns back into the strategy?
- Should `Side` be extended (close, reduce, flat) or is exit intent expressed as an opposite-side signal? Currently there is no way for a strategy to say "close my position" distinct from "open the opposite".
- Reset semantics for backtesting: walk-forward re-optimisation needs strategies re-instantiated or reset between windows. Does `Strategy` need `Reset()`, or is construction-per-window sufficient?
- Should strategies see fills / positions, or is the strategy strictly feed-forward? Feed-forward is cleaner but prevents position-aware logic (e.g. pyramiding, scaling out).

## WIP

- Nothing works. Zero strategies in the default build, `signal_logger` has no input.
- `buy_the_dip` needs: the `isSwingHigh` fix, swing detection moved to the `swing_highlow` feature, config wired through the factory, `Side` constants used, and the build tag removed.
- Recommended order: input contract settled (`SyncEvent`, [[multi-subscription]] §5) → fix `swing_highlow` in [[features]] → rewrite `buy_the_dip` against features → un-tag → exercise via `signal_logger`.

## Resources

- `internal/protocol/strategy.go`, `internal/domains/strategy/`
- `docs/Protocol.md` § Strategy layer
- `WIP.md` (ToDo: "adapt Strategy interfaces/instances to receive ExtendedMarketData type")
- Related: [[features]] (input), [[risk-management]] (consumer), [[engine]] (fan-in and wiring),
  [[multi-subscription]] (input contract §5, sub declaration §3)
