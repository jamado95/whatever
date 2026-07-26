# Risk Management

## Topic

Risk management holds the portfolio-wide view — balances, positions, exposure, realised and
unrealised P&L — and is the only component that turns strategy *intent* into executable *orders*.
It sizes positions, enforces exposure limits, and can act on monitored position state independently
of any strategy signal.

Objective of this discussion: record the central design decision (risk as the sole signal→order
translator and single portfolio owner) and enumerate what the current type model cannot express.

## Current Understanding

### Contract

`proto.RiskManager` (`internal/protocol/risk.go:23`) is the only component that takes three inputs:

```go
type RiskManager interface {
	Init(data <-chan MarketData, signals <-chan Signal, fills <-chan Fill) error
	Streams() (<-chan Order, <-chan error)
	Start() error
	Portfolio() PortfolioState
	Close()
}
```

- `data` — market prices, to mark positions to market.
- `signals` — merged intent from all strategies.
- `fills` — execution feedback, to update the position ledger.
- Output: `Order`s to the execution layer.
- `Portfolio()` is a synchronous accessor, the only non-channel read in any protocol interface.

Note it has **no `ID()`** — every other component has one. Consistent with it being a singleton.

```go
type Position struct {
	Symbol        string
	Side          Side
	Size          float64
	EntryPrice    float64
	CurrentPrice  float64
	UnrealizedPnL float64
	Timestamp     int64
}

type PortfolioState struct {
	Balances      map[string]float64
	Positions     map[string]Position   // keyed by symbol
	TotalValue    float64
	UnrealizedPnL float64
	RealizedPnL   float64
	Exposure      map[string]float64
	Timestamp     int64
}
```

### State of implementations

**Nothing is implemented.** `internal/domains/risk/risk.go` is two lines:

```go
package risk

// RiskManager implementation
```

There is no `reg.Risk` registry slot (unlike providers, strategies, features, execution, engines and
exporters, which all have one), and `cmd/main.go` has no risk construction stage. The `full` engine
requires `_risk` in its options and therefore cannot be instantiated — see [[engine]].

This is the single hard blocker on the signal→order path.

## Decisions

- **Risk is the sole signal→order translator.** No strategy can emit an order. Rationale: sizing
  requires portfolio-wide state that no individual strategy sees, and a single translation point is
  the only place where a global limit can actually be enforced.
- **Risk is a singleton.** One instance per engine, owning the one portfolio view. Reflected in the
  interface having no `ID()` and in the `full` engine taking a single `proto.RiskManager` while
  taking slices of strategies and executors.
- **Risk is the portfolio ledger**, updated from the fill stream. Positions are derived from
  executions, not from strategy expectations. This is why `fills` is an input.
- **Risk marks to market itself**, which is why it receives the `data` stream in parallel with
  strategies (`full.go:215` fans the gated data out to `len(strategies)+1` consumers, the extra one
  being risk).
- **Risk may act without a signal.** Since it sees prices and fills continuously, it can emit
  protective orders — stop-outs, exposure reduction — with no strategy involvement. This is stated
  in `Protocol.md` and is the reason it consumes market data rather than only signals.

## Solution

The interface shape is settled and reasonable. The **type model** is where the gaps are:

- **One position per symbol.** `Positions map[string]Position` with a single `Side` cannot represent
  hedged (simultaneous long and short) positions, per-strategy position attribution, or multi-leg
  structures. Netting is therefore forced, which is a real decision — it just has not been made
  explicitly.
- **No cash or margin model.** `Balances map[string]float64` is a bag of asset quantities. There is
  no notion of available vs. reserved margin, no leverage, no maintenance requirement. Futures and
  FX cannot be sized correctly without it.
- **No accounting currency.** `TotalValue` and the P&L fields are unit-less floats. Multi-asset and
  FX portfolios need an explicit base currency and a conversion path.
- **No fees or funding.** `RealizedPnL` cannot be correct without them, and neither `Fill` nor
  `Position` carries a cost field.
- **`Exposure map[string]float64` is undefined** — keyed by symbol, asset, sector, venue? Notional
  or percentage? This needs specifying before any limit can be written against it.
- **No risk limits in the type model at all.** Max position size, max portfolio exposure, max
  drawdown, per-strategy allocation, correlation limits — none are represented. They would live in
  the implementation's config today, which makes them invisible to anything that wants to reason
  about or report on them.
- **`float64` for money.** Fine for research, questionable for live accounting. Worth deciding
  deliberately rather than by default.

Sizing approaches to consider once implementation starts — fixed fractional, volatility-targeted
(needs an ATR/σ feature), Kelly-style, or equal-weight across active signals. The choice interacts
with [[strategy-layer]]: a confidence scalar on `Signal` only makes sense under some of them.

A related question is **conflict resolution**. Two strategies emitting opposite signals for the same
symbol at the same timestamp is not handled by anything today. Options: net them, first-wins,
weight by `Source` allocation, or reject and log. This is a policy decision that belongs here rather
than in the engine's fan-in.

## Open Questions

- Netting or hedged positions? Decide before implementing the ledger — it is not a cheap change
  afterwards.
- Is the position ledger keyed by symbol, or by (symbol, strategy)? Per-strategy attribution is
  needed for allocation and for evaluating strategies independently in backtests, but it conflicts
  with a netted view of venue reality.
- Where are risk limits declared — `config.json`, code, or a dedicated policy type in the protocol?
- Base currency and FX conversion: needed for the multi-asset goal in `Architecture.md`, absent
  today.
- How are multi-strategy conflicting signals resolved (options above)?
- `Portfolio()` is a synchronous read on a concurrently-mutated state — needs a defined memory model
  (snapshot copy under lock is the obvious answer, but `PortfolioState` contains maps, so a shallow
  copy is not enough).
- Does risk need `ID()` after all, if backtesting ever runs multiple isolated portfolios in one
  process? Parameter sweeps suggest yes.
- Should risk emit portfolio updates on a stream (for logging, export and monitoring) rather than
  only exposing a pull accessor? `logger.Portfolio()` already exists and has no producer.

## WIP

- Not implemented. Stub file only, no registry slot, no config, not wired in `cmd/main.go`.
- Blocks: the `full` engine, any order flow, and any backtest that reports P&L.
- A minimal implementation — fixed-fractional sizing, symbol-keyed netted ledger, mark-to-market
  from the data stream, no limits — plus a mock executor from [[execution-layer]] would make the
  `full` engine runnable end to end for the first time. That is probably the right sequencing:
  smallest thing that closes the loop, then refine the type model against a working baseline.

## Resources

- `internal/protocol/risk.go`, `internal/domains/risk/risk.go`
- `docs/Protocol.md` § Risk Management
- `internal/logger/logger.go` (`Position`, `Portfolio` log helpers already exist, unused)
- Related: [[strategy-layer]] (signal producer), [[execution-layer]] (order consumer),
  [[monitor-layer]] (position feedback), [[engine]] (wiring)
