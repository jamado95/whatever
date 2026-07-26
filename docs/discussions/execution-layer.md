# Execution Layer

## Topic

The execution layer is the abstraction boundary over broker and exchange APIs. It receives `Order`s from risk management, submits them to a venue, and reports `Fill`s back. It owns everything venue-specific: request/response shapes, authentication, rate limits, retry and backoff, and availability reporting. It must support a test mode where orders are logged rather than sent.

Objective of this discussion: define the order lifecycle model before any venue integration is
written, since the current `Order`/`Fill` types encode a simpler world than live trading requires.

## Current Understanding

### Contract

`proto.Executor` (`internal/protocol/execution.go:21`) follows the same lifecycle as every other
component:

```
Init(orders <-chan Order) -> Streams() (<-chan Fill, <-chan error) -> Start() -> Close()
```

```go
type Order struct {
	ID        string
	Symbol    string
	Side      Side
	Size      float64
	Price     *float64   // nil => market order
	Timestamp int64
	ExpiresAt int64
	Source    string
}

type Fill struct {
	Order     Order
	FillPrice float64
	FillSize  float64
	FilledAt  int64
}
```

Order type is implied by `Price` being nil (market) or set (limit). `Fill` embeds the whole `Order` rather than referencing it by ID, so a fill is self-describing.

### State of implementations

**Nothing is implemented.** The entire domain is three files containing a package clause and a
comment:

- `internal/domains/execution/interface.go` — `package execution` only.
- `internal/domains/execution/binance.go` — `// BinanceExecutor - live execution`.
- `internal/domains/execution/mock.go` — `// MockExecutor - test mode (logging only)`.

`reg.Execution` exists in the registry (`internal/registry/registry.go:21`) but nothing registers into it, `cmd/main.go` never constructs executors, and `config.json` has no executors section. The `full` engine requires `_executors` and therefore cannot be instantiated (see [[engine]]).

## Decisions

- **One abstraction per venue, venue specifics fully contained.** Adding a broker must not require changes outside `internal/domains/execution`. Same rationale as the provider layer.
- **Channel-in / channel-out, not request-response.** The executor consumes an order stream and
  produces a fill stream, consistent with the rest of the pipeline. This means order submission is asynchronous by construction and the caller never blocks on venue latency.
- **`Fill` embeds the originating `Order`.** Downstream consumers (risk, monitoring, logging) do not need an order book to interpret a fill. Cost: duplication on partial fills, where N fills each carry a full copy of the order.
- **Multiple executors run concurrently.** The `full` engine fans orders out to all executors (`full.go:247`) and fans fills back in. This supports live + shadow/mock execution side by side, which is how the test mode requirement in `Protocol.md` is intended to be met.
- **A mock executor is a first-class component, not a flag.** Test mode is a different registered executor, not a boolean on the live one — keeps credential-carrying code paths out of test runs entirely.

## Solution

The order model needs to be settled before writing the first venue adapter, because the current
types assume things that live venues do not provide:

**What is missing from the type model:**

- **No order state machine.** Real orders move through `submitted → accepted → partially_filled →
  filled | cancelled | rejected | expired`. Today the only observable outcome is a `Fill` or an
  error on the error channel. A rejected order is indistinguishable from a transport failure.
- **No venue order ID.** `Order.ID` is generated internally. Cancel, amend and reconciliation all
  need the venue's identifier, which only exists after submission.
- **No cancel or amend path.** The input is a one-way order channel. Expressing "cancel order X"
  requires either a second channel, a command type on the existing channel, or a synchronous method outside the stream contract.
- **No order type enum or time-in-force.** `Price != nil` covers market/limit only — no stop, no
  stop-limit, no IOC/FOK/GTC, no post-only or reduce-only.
- **No fees or slippage in `Fill`.** P&L cannot be computed accurately without them, and the
  backtesting methodology in `docs/Backtesting.md` depends on realistic cost modelling.
- **`ExpiresAt` is declared but has no defined semantics** — client-side drop, or venue GTD?

Options for the state problem:

- **Add an `OrderUpdate`/`OrderStatus` stream** alongside fills. `Streams()` becomes
  `(<-chan OrderUpdate, <-chan Fill, <-chan error)`. Explicit, but widens the interface and every
  consumer must handle a new event type.
- **Make `Fill` a special case of a general `ExecutionReport`**, one stream carrying all state
  transitions. Closest to how FIX and most venue APIs actually work, and probably the least
  surprising long-term. Consumers filter for the transitions they care about.
- **Keep fills-only, model everything else as errors.** Simplest, but loses the distinction between "venue rejected this order" and "the network is down", which is exactly the distinction the availability-reporting requirement needs.

Option 2 looks strongest, but this should be decided against a concrete venue API rather than in the
abstract. Binance spot is the obvious first target since the data layer already integrates with it.

**Reliability requirements** from `Protocol.md`, none of which have a home yet: backoff and retry policy, idempotency on retry (a retried order must not double-submit — needs a client order ID), rate limiting, circuit breaking, and availability/latency reporting. Where retry policy lives is itself a question: per-executor, or a shared middleware the way `internal/pipeline` provides shared stream combinators?

## Open Questions

- Which execution-report model (options above)? Decide against the Binance spot API.
- Where do cancel/amend commands enter? Second channel, command union, or out-of-band method?
- Client order IDs for idempotent retry: generated by `idgen`, or venue-specific format constraints?
- Do fees belong on `Fill`, or are they reconciled separately from account statements? Crypto
  (per-trade, sometimes in a third asset) and equities (commission + exchange fees) differ enough that a single field may not fit.
- Is there a paper-trading executor distinct from the mock — one that simulates fills against live market data with a slippage model? The backtesting engine needs exactly this fill simulator, so it may be shared rather than duplicated.
- How are credentials configured? `config.json` is checked in; secrets cannot live there.
- Does the executor own position reconciliation, or does that belong to [[monitor-layer]]?

## WIP

- Nothing implemented. This is the largest unbuilt piece of the protocol.
- Highest-value first step is a **mock executor**: it unblocks the `full` engine end to end, it is the fill simulator the backtesting engine needs, and it requires none of the credential, retry or rate-limit machinery. Doing it before the live adapter also forces the state model to be settled on the engine's terms rather than Binance's.
- `reg.Execution` registry slot exists and is unused; `cmd/main.go` has no executor wiring stage.

## Resources

- `internal/protocol/execution.go`, `internal/domains/execution/`
- `docs/Protocol.md` § Execution layer
- Related: [[risk-management]] (produces orders), [[monitor-layer]] (shares venue plumbing),
  [[engine]] (fan-out/fan-in wiring)
