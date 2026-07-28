# Engine

## Topic

The engine wires the protocol layers together: it constructs components from config, connects their channels, controls ingestion timing, and owns startup and shutdown. Multiple engines exist with different topologies so that partial pipelines can be exercised during development.

Objective of this discussion: document the wiring and timing model, record the decisions behind the registry/DI approach and the ticker abstraction, and identify the correctness issues that must be resolved before a backtesting engine is built on this foundation.

## Current Understanding

### Contract

```go
type Runnable interface {   // internal/registry/registry.go:32
	Run(ctx context.Context) error
	Close()
}
```

Every component follows the same four-phase lifecycle — `Init` (allocate channels, validate) →
`Streams` (hand out read ends) → `Start` (spawn goroutines) → `Close`. The engine drives all of
them in that order and is the only place that knows the topology.

### Registered engines

| Engine | File | State |
|---|---|---|
| `data_logger` | `internal/engine/data_logger.go` | **Working.** provider → ticker → feature chain → log + export. The only exercised path in the repo. |
| `signal_logger` | `internal/engine/signal_logger.go` | Wired, but has no strategies to run and a goroutine-leak bug (below). No feature chain. |
| `full` | `internal/engine/full.go` | **Cannot be instantiated.** Requires `_risk` and `_executors`; `cmd/main.go` never provides them, and neither component exists. |
| `backtesting` | `internal/engine/backtesting.go` | A single comment line. |

### Wiring in the `full` engine

The most complete topology, at `full.go:200`:

```
provider ──ticker.Gate──▶ FanOut(n_strategies + 1)
                            ├─▶ strategy[i] ──▶ signals ──FanIn──┐
                            └─▶ risk ◀─────────────────────────────┘
                                 │
                                 ├─▶ orders ──FanOut(n_executors)──▶ executor[i]
                                 └◀── fillsRelay ◀──FanInTo── fills[i]
```

All component error channels are merged into one `FanIn` and counted; passing `ErrorThreshold`
triggers `Close()`.

### Dependency injection

`cmd/main.go` is a hand-written composition root: read `config.json`, instantiate enabled providers, strategies and features from the registries, then stuff resolved instances into the engine's option map under underscore-prefixed keys (`_provider`, `_strategies`, `_features`, `_exporter`). The engine factory type-asserts them back out.

The registry (`internal/registry/registry.go`) is a set of package-level, generic,
mutex-guarded `map[string]Factory[T]`, populated by `init()` side-effect imports in `main.go`.

### Timing

`internal/timing/timing.go` defines `Ticker[T]` with `Gate(ctx, in) <-chan T`, plus `Tick`,
`SetFactor`, `Start`, `Stop`. Two implementations:

- `realtime` — `Gate` returns the input channel unchanged, everything else is a no-op.
- `fixedInterval` — releases one value per tick, creating backpressure that propagates upstream to the provider.

`SetFactor` (simulation speed) is declared on the interface but the "Simulated" ticker it refers to does not exist.

### Known gaps and defects

1. **Ticker type falls back silently.** ~~Resolved~~ — see [[config]]. `TickerType` is now a
string enum with a validating `UnmarshalJSON`; there is no realtime fallback path.
2. **`FanOut` drops data.** On a full output buffer it evicts the oldest value (`pipeline/fanout.go:28`). Silent, non-deterministic loss — acceptable for a live feed where
staleness beats lag, fatal for backtesting reproducibility, and fatal for any strategy whose state depends on seeing every candle.
3. **`signal_logger` goroutine leak.** `eErrs := make([]<-chan error, len(strategies)+1)` followed by `append` (`signal_logger.go:143`) produces a slice with `n+1` leading `nil` channels. Ranging over a `nil` channel blocks forever, so the `FanIn` wait group never completes and its output is never closed. Uses `make(..., 0, n+1)`.
4. **Config parsing is triplicated.** ~~Resolved~~ — see [[config]]. The three parsers are gone;
each engine declares an options struct decoded by `config.Decode`.
5. **`NewFullEngine` is dead code.** ~~Resolved~~ — deleted alongside the parser it duplicated.
6. **Untyped, unvalidated config.** ~~Resolved~~ — see [[config]] for the five failure modes and
the decoding contract that closed them.
7. **Inconsistent shutdown.** `DataLogger` uses `sync.Once`; `Engine` and `SignalLogger` use a `select`-on-`done` idiom; `Engine.Close()` can be called from the error-threshold goroutine concurrently with `Run`'s own call.
8. **`full` returns `ctx.Err()` unconditionally** (`full.go:324`), so a normal Ctrl-C exit reports `context.Canceled` as an engine error. `DataLogger` explicitly absorbs this; `full` does not.
9. **Single provider per engine.** Every engine takes exactly one `_provider` with one `Subscription`, so the "hundreds of concurrent streams" goal in `Architecture.md` is architecturally unreachable today.
10. **Features only run in `data_logger`.** Neither `signal_logger` nor `full` constructs a `FeatureChain`, so in the only engine that reaches strategies, strategies see raw candles.
11. **Thin test coverage.** `internal/config`, `internal/engine` and `internal/domains/provider`
have tests as of the config work; `pipeline`, `timing` and the feature maths still have none.

## Decisions

- **Engines own wiring; domains own logic.** Domains never import each other, only `protocol`. This is what makes alternative topologies (log-only, signal-only, full, backtest) cheap to add.
- **Multiple engines rather than one configurable engine.** Each engine is a fixed topology. Rationale: a partial pipeline stays runnable while downstream layers are unbuilt — which is the only reason anything in this repo runs today, given that risk and execution are empty.
- **Registry + `init()` side-effect registration.** Components self-register; adding one requires no change to `main.go`. Cost: package-level mutable state, and construction is untyped
(`map[string]any`).
- **`_`-prefixed option keys carry resolved instances.** A pragmatic hack for passing constructed dependencies through the same untyped map used for scalar config. Works, but means the DI graph is invisible to the type system and errors surface as runtime assertion failures.
- **Timing is the engine's concern, not the provider's.** The `Ticker` gate sits between provider and everything downstream, so the same provider serves live and replay. `fixedInterval` propagates backpressure upstream rather than buffering, which keeps memory bounded during fast replay.
- **Context cancellation is the shutdown signal.** `main.go` installs a `signal.NotifyContext` for SIGINT/SIGTERM and `Run(ctx)` blocks until cancelled or complete.
- **Errors are streamed, not returned.** Every component exposes an error channel; the engine merges and counts them, shutting down past a threshold. Keeps error handling out of the hot path.
- **Config decoding is strict and lives with each component.** See [[config]] for the contract,
  the enum rules and the `--validate-only` dry run.

## Solution

Two things need resolving before the backtesting engine is written, because a backtest inherits both:

**Determinism.** `FanOut`'s drop-oldest policy makes runs non-reproducible under load, which
directly contradicts the methodology in `docs/Backtesting.md` — permutation tests and walk-forward
comparisons are meaningless if two runs on the same data differ. Options:

- **Blocking fan-out** (slowest consumer sets the pace). Deterministic, simple, but one slow
  strategy stalls the whole pipeline and a deadlock becomes possible if a consumer never drains.
- **Policy per engine** — drop for live, block for backtest. Correct behaviour in both modes, at the cost of two code paths and the risk that they diverge.
- **Unbounded buffering for backtests.** Deterministic and non-blocking, but memory grows with the slowest consumer's lag.
- **Drop, but count and report.** Keeps current behaviour and makes the loss visible. Insufficient on its own for backtesting, but worth having regardless — silent loss is the real problem.

Option 2 is the likely answer; option 4 should happen either way.

**Config typing.** Resolved and moved to [[config]] — component config structs decoded strictly by
`config.Decode`, validating enums, and a `--validate-only` dry run. Defects 1, 4 and 6 below are
closed by that work.

**Backtesting engine.** Not yet designed. It needs, at minimum: a deterministic ticker (probably a "drain as fast as possible" mode, or `Manual` driven step-by-step), a fill simulator (see [[execution-layer]] — the mock executor and the backtest fill model are plausibly the same component), a risk implementation for P&L, and many cheap engine instances for parameter sweeps. That last requirement is flagged as unresolved in `Architecture.md` ("Backtesting strat param optimisation needs many instances (how to handle TBD)") and is a genuine tension with package-level registry state.

## Open Questions

- Fan-out policy (options above) — decide before the backtesting engine.
- How does a backtest instantiate hundreds of engine variants? The registry is package-global but factories are pure, so parallel instantiation looks safe — this needs confirming rather than assuming.
  - Sub-question with a bearing on [[config]]: do parameter sweeps go *through* `config.json`
    (generated files fed to `config.Load`) or bypass it (typed config structs built directly in Go
    and handed to factories in-process)? Bypassing looks better when the sweep driver is Go in the
    same binary — serialising to JSON only to parse it back is pointless. It does narrow what
    `--validate-only` is for: generated config files are its main use today, leaving only
    hand-edited configs if sweeps bypass the file.
- Multi-provider / multi-symbol engines: how does the topology change? One engine per symbol with a supervising process, or one engine fanning many subscriptions through one feature-chain-per-symbol?
- Should `data_logger` and `signal_logger` merge into one composable "observation" engine now that the difference is just which stage terminates? Or does keeping them separate remain worth the duplication?
- Does the `Ticker` abstraction need the `Simulated` (factor-based) and `Manual` implementations its interface already implies, or should `SetFactor`/`Tick` be dropped until something needs them?
- Where do metrics and health reporting live? Nothing measures throughput, latency or channel occupancy today, and the parallelism goals in `Architecture.md` cannot be evaluated without them.

## WIP

- `data_logger` is the only working engine: `binance_csv` → `fixedInterval` (100ms, confirmed
  active) → feature chain → console log + JSONL export to `data/exports/`.
- `signal_logger` is blocked on [[strategy-layer]]; `full` is blocked on [[risk-management]] and
  [[execution-layer]]; `backtesting` is unstarted.
- `WIP.md` ToDo "adapt 'data_logger' and 'full' engine to new domain/processors layer" is partly
  done — `data_logger` runs the feature chain, `full` does not.
- Defects 1, 4, 5 and 6 are closed by the config work ([[config]]). Remaining near-term order:
  fix defects 2–3 → add tests for `pipeline` and `timing` → settle fan-out policy → design the
  backtesting engine.

## Resources

- `internal/engine/`, `internal/pipeline/`, `internal/timing/`, `internal/registry/`,
  `internal/config/`, `cmd/main.go`, `config.json`
- `docs/Protocol.md` § Engine, `docs/Architecture.md`, `docs/Backtesting.md`
- Related: every other protocol discussion — [[data-layer]], [[features]], [[strategy-layer]],
  [[risk-management]], [[execution-layer]], [[monitor-layer]]
