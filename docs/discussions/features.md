# Features

## Topic

The feature layer turns raw `MarketData` into a timestamped snapshot of derived metrics —
indicators, patterns, technical signals — that strategies consume. Features must be composable
(a feature may depend on other features) and must not leak future information.

Objective of this discussion: pin down the purity/statelessness decision that shapes this layer,
document the composition mechanism that exists but is unused, and identify what blocks features
from reaching strategies.

## Current Understanding

### Contract

`proto.Feature` (`internal/protocol/feature.go:12`):

```go
type Feature interface {
	ID() KeyRef
	Dependencies() []KeyRef
	Lookback() int
	Update(history *SortedWindow[MarketData], snap *Snapshot)
}
```

The file states the intent explicitly: *"Features are PURE FUNCTIONS wrapped in structs. They have
NO mutable state — only configuration (immutable after construction). All state comes from
parameters."* Every registered feature honours this: the struct holds only `id` and `period`.

`Update` receives the full rolling window and a per-candle `Snapshot`, and writes its output into the snapshot. Composition works by a downstream feature reading an upstream key out of the same snapshot — the dependency graph only controls evaluation order.

### Snapshot and typed keys

`internal/protocol/snapshot.go` implements a heterogeneous map with generic accessors:
`Key[T]` carries a phantom type, `NewKey[T](name)`, `GetSnapshot[T]`/`SetSnapshot[T]`.
`KeyRef{Name}` is the untyped form used for the dependency graph (a `Key[T]` cannot be stored in a `[]KeyRef` without erasing T).

The comment claims this is "completely safe at runtime". That is not quite right: `GetSnapshot`
does an unchecked `val.(T)` (`snapshot.go:45`) which panics if two `Key`s with different `T` share a name. Safety rests on the key-to-type invariant being maintained by convention — nothing enforces uniqueness of feature IDs across type parameters.

### FeatureChain

`internal/domains/features/feature.go` — construction is `duplicate check → topological sort → size window`:

- `checkDuplicates` errors on two features sharing a `KeyRef`.
- `topologicalSort` is Kahn's algorithm over `Dependencies()`.
- `calculateRequiredCapacity` sizes one shared `SortedWindow[MarketData]` to the **maximum**
  `Lookback()` across all features.
- `Process(in <-chan MarketData) <-chan ExtendedMarketData` runs one goroutine: push candle to the window, run every feature in sorted order against a fresh `Snapshot`, emit.

Features that do not yet have enough history simply do not write to the snapshot. The exported
JSONL confirms this — early rows are missing indicator columns entirely (`data/exports/*.jsonl` row 1 has no `engulfing_candle`, row 2 does).

### Implementations

| Feature | Output type | Notes |
|---|---|---|
| `sma` | `float64` | Straightforward window mean of closes. |
| `ema` | `float64` | Windowed recomputation, not recursive — see Decisions. `Lookback = 2 × period`. |
| `rsi` | `float64` | Simple average gain/loss, **not** Wilder's smoothing. |
| `vwap` | `float64` | Rolling `period`-candle VWAP over typical price, not session-anchored. |
| `engulfing_candle` | `DirectionalMarker` (int) | Strict engulfing; documented rationale in-file. |
| `swing_highlow` | `DirectionalMarker` | Behind `//go:build wip`, **does not compile** — empty `for neigh := range candles {}` and references to undefined `checkIdx`/`state`/`engulfing`. |

All float outputs are rounded to 6 decimals via `roundDecimals` before being written.

### Known gaps and defects

1. **Features never reach strategies.** `proto.Strategy.Init` takes `<-chan MarketData`, and the
   `FeatureChain` produces `ExtendedMarketData`. Only the `data_logger` engine runs the chain, and
   it logs/exports rather than forwarding. This is the long-standing item in `WIP.md`.
2. **Missing dependency reports as a cycle.** `topologicalSort` sets in-degree from
   `len(Dependencies())` without checking the dependency exists in the set, so a feature depending on an unconfigured feature can never reach in-degree 0 and the chain fails with "circular dependency detected in processors" — a misleading error for a common misconfiguration.
3. **No feature declares dependencies today.** The composition path is entirely untested.
4. **Sequential, unbuffered evaluation.** `Process` uses one goroutine and an unbuffered output
   channel (there is a `TODO: buffer size` at `feature.go:56`). Independent features are not
   evaluated in parallel, which is at odds with the parallelism goals in `Architecture.md`.
5. **`Push` error ignored** at `feature.go:65`; a zero-timestamp candle silently fails to enter the window while the feature pass still runs.
6. **No tests.** Indicator maths — the one part of this codebase that is trivially unit-testable
   against known reference values — has zero coverage.

## Decisions

- **Features are pure functions over a shared window.** State lives in `SortedWindow`, not in the feature. Rationale: a stateless feature can be re-instantiated cheaply for parameter sweeps, run on arbitrary slices of history, and produces identical output for identical input — which is what the backtesting pipeline (`docs/Backtesting.md`) requires for reproducibility.
- **One shared window, sized to the maximum lookback.** Avoids N buffers of the same data. Cost: every feature pays the memory of the greediest one, and lookback is expressed in candles rather than in time.
- **Composition through the snapshot, ordering through the dependency graph.** Features do not call each other; they read each other's outputs by key. Keeps features independent and the graph explicit.
- **Insufficient history means "absent", not "zero".** A feature that cannot compute writes nothing, so consumers can distinguish "not ready" from a real value. This is why exported rows have ragged columns.
- **Strict definitions over permissive ones** for pattern features (documented for `engulfing`): lower signal frequency, higher information content, better suited to statistical validation.
- **A feature ID encodes every parameter that distinguishes one instance from another.** Previously an ID was `name_period`, which was accidentally sufficient because `period` was the only parameter any feature took. Adding `ema.smoothing` broke that: two variants differing only in smoothing both keyed `ema_20`. Defaulted values are omitted from the ID so the common names (`ema_20`, `vwap_50`) stay stable — they appear in exports and in `make plot INDICATORS=`; only a non-default extends it (`ema_20_s3`). See [[config]].
- **Duplicate feature IDs are a construction error, not a silent drop.** `removeDuplicates` used to keep the first and discard the rest. Now that an ID covers every distinguishing parameter, a collision means two variants were configured identically — a config mistake.

## Solution

The purity decision has a cost worth stating: **`ema` recomputes from the window each candle rather than carrying the recursive state**, seeding with an SMA of the oldest `period` candles inside a `2 × period` window. This is an approximation of a true EMA — the seed moves with the window, so the value differs from a continuously-accumulated EMA, and the error depends on `period`. The same concern applies to `rsi`, which uses a simple average rather than Wilder's smoothing and therefore does not match the conventional RSI most references and charting tools report.

Alternatives if exactness matters:

- **Allow recursive state in features**, with an explicit `Reset()`. Exact, cheaper per candle
  (O(1) vs O(period)), but breaks reproducibility guarantees and complicates parameter sweeps.
- **Keep purity, increase warm-up.** Seed further back (e.g. `5 × period`) so the seed's influence decays below tolerance. Cheap to do, costs memory and warm-up candles.
- **Keep purity, accept the approximation, document it.** Defensible if strategies are validated against these exact feature definitions rather than against external charts.

Not yet decided. This matters most when comparing engine output against third-party charts or
published strategies; it matters little if features are only ever compared against themselves.

## Open Questions

- EMA/RSI exactness: which of the three options above? What tolerance is acceptable?
- Should `Lookback()` be expressed in candles or in duration? Duration is timeframe-independent and necessary once one chain sees multiple timeframes.
- Multi-symbol: the chain holds one window and assumes a single instrument stream. Does a chain instance exist per symbol, or does the window become keyed by symbol? Cross-asset feature (correlation, spread, beta) — explicitly a goal in `Architecture.md` — need the latter, or need a distinct "multi-stream feature" concept.
- Should the type-safety hole in `Snapshot` be closed (registry of key name → type, checked at chain construction), or is the convention sufficient?
- Parallel evaluation of independent graph levels: worth it, or is per-candle feature cost too small for the goroutine overhead to pay off? Needs a benchmark before deciding.
- `Dependencies()` is declared but unused. Is it kept for the composition roadmap, or removed until a real composite feature demands it?

## WIP

- Registered and working: `sma`, `ema`, `rsi`, `vwap`, `engulfing_candle`.
- `swing_highlow` is broken behind a `wip` build tag. It duplicates swing-detection logic that also exists (also broken) inside `buy_the_dip` — see [[strategy-layer]]. Consolidating swing detection into this feature is the obvious next step.
- `config.json` currently enables `engulfing_candle`, `vwap` (20, 50) and `ema` (20, smoothing 2.0); `rsi` and `sma` are disabled.
- Every feature now declares a config struct decoded strictly ([[config]]); `PeriodConfig` is shared by the period-only features, `swing_highlow` adds an even-period rule in `Validate()` — unexercised, since it sits behind the `wip` build tag.
- Fibonacci and trend features are listed as ToDo in `WIP.md`.

## Resources

- `internal/protocol/feature.go`, `internal/protocol/snapshot.go`, `internal/protocol/sorted_window.go`
- `internal/domains/features/`
- `docs/Protocol.md` § Features
- Related: [[data-layer]] (produces `MarketData`), [[strategy-layer]] (intended consumer),
  [[engine]] (only `data_logger` runs the chain)
