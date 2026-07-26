# Monitor Layer

## Topic

The monitor layer captures realtime state about open market positions and feeds it to risk
management. `Protocol.md` notes it may merge with the execution layer as an internal but
independent component, since it talks to the same brokers and can reuse the same availability and
error-handling machinery.

Objective of this discussion: decide whether monitoring is a distinct layer at all, and if so, what it is authoritative for.

## Current Understanding

### Contract

`internal/protocol/monitor.go` is the whole of it — 16 lines, and the file carries a `// REVIEW`
marker on the one type it defines:

```go
type MonitorSubscription struct {
	Subscription Subscription
	Side         Side
	Status       string
	Timestamp    int64
}

type Monitor interface {
	ID() string
	Init(fills <-chan Fill) error
	Streams() (<-chan Fill, <-chan error)
	Start() error
	Close()
}
```

`MonitorSubscription` is referenced nowhere.

### State of implementations

**Nothing exists.** There is no `internal/domains/monitor` package, no registry slot in
`internal/registry/registry.go`, no config section, and no engine wires a monitor.
`docs/Architecture.md` lists `monitor` among the domains — that is aspirational, not current.

### The interface does not match the stated purpose

As declared, `Monitor` takes fills in and emits fills out — a pass-through in the fill path. But
`Protocol.md` describes it as capturing *position* state from the broker and emitting position and
portfolio updates. Those are different components:

- **Fill relay** sits between execution and risk on the fill channel. Passive. Adds nothing that
  the engine's existing `FanInTo` (`full.go:263`) does not already do.
- **Position poller** independently queries the venue for account and position state and emits
  `Position` / `PortfolioState` updates. Active, and the only thing that can detect divergence
  between what the engine thinks it holds and what the venue says it holds.

The second is the one that justifies a layer. The first is already redundant — the `full` engine
relays fills to risk directly without a monitor.

The `Fill -> Fill` shape plus the `REVIEW` marker suggests the interface was sketched from the
plumbing position it would occupy rather than from what it should do.

## Decisions

Very little has been decided here. What can be inferred from the code and `Protocol.md`:

- **Monitoring is separated from execution in the protocol**, even though both talk to the same
  venue. Rationale in `Protocol.md`: the concerns differ (order submission vs. state observation),
  while the transport, auth and availability logic is shared and should be reused rather than
  duplicated.
- **Risk management is the consumer**, and it is expected to act on monitor signals independently
  of the strategy signal path — i.e. the monitor can trigger risk action with no strategy involved
  (stop-outs, margin events).

Both of these are inherited from `Protocol.md` rather than validated in code.

## Solution

Three shapes worth considering:

- **Monitor as an independent position poller.** Own component, own venue client, emits
  `PortfolioState`/`Position` updates on a timer or venue push. Keeps the "act independently of
  strategy" property clean. Costs a second venue integration surface and raises the reconciliation
  question: when the venue disagrees with the engine's internal ledger, who wins?
- **Monitor folded into the executor.** Executors already hold the venue client, credentials and
  backoff logic; adding a `Positions() <-chan PortfolioState` stream to `Executor` avoids
  duplicating all of it. `Protocol.md` explicitly floats this. Cost: `Executor` grows a second
  responsibility, and the mock executor has to fake position state too.
- **No monitor; risk derives everything from the fill stream.** Risk already receives fills and can
  maintain its own ledger — this is what `RiskManager.Init(data, signals, fills)` implies. Simplest
  and sufficient for backtesting, where there is no external truth to reconcile against. Fails in
  live trading the moment anything happens outside the engine: manual intervention, liquidation,
  funding, a fill the engine missed during a disconnect.

The distinguishing question is **reconciliation**, and it only bites in live trading. For
backtesting and paper trading, option 3 is adequate and the monitor is unnecessary. That argues for
deferring this layer until live execution exists — but it also argues for not baking
"risk owns the only ledger" into the risk interface in a way that blocks a monitor later.

## Open Questions

- Is this a layer, or a capability of the executor? Decide alongside the execution-report model in
  [[execution-layer]] — the two interfaces overlap heavily.
- If the engine's ledger and the venue disagree, which is authoritative, and what is the correction
  procedure? This is the actual design problem; everything else is plumbing.
- Push (user data stream / websocket) or poll? Push is lower latency but needs reconnect and
  gap-recovery handling; poll is simpler but bounded by rate limits.
- What does the monitor emit — `Fill`, `Position`, `PortfolioState`, or a union? The current
  `Fill -> Fill` signature is almost certainly wrong.
- Does the monitor observe market prices to mark positions to market, or does risk do that from the
  `MarketData` stream it already receives? `Position.CurrentPrice` and `UnrealizedPnL` have to be
  computed somewhere and both layers can see the data.
- What is `MonitorSubscription` for? It looks like a per-position subscription concept that was
  never developed. Delete or specify.

## WIP

- Interface sketch only, marked `REVIEW`, shape disputed above. No implementation, no registry
  entry, no wiring.
- Blocked on [[execution-layer]] — there is nothing to monitor until orders can be placed, and the
  fold-into-executor option cannot be evaluated without a real executor.
- Recommendation: leave unimplemented, but resolve the `Fill -> Fill` signature (or delete the file)
  so it stops implying a design that has not been agreed.

## Resources

- `internal/protocol/monitor.go`
- `docs/Protocol.md` § Monitor layer
- Related: [[execution-layer]] (shared venue plumbing, possible merge target),
  [[risk-management]] (consumer), [[engine]] (unwired)
