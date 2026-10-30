# Features

## Topic

The feature layer turns aligned market data into derived metrics — indicators, statistics,
cross-asset relations — that strategies consume. Features must be composable (a feature may depend
on other features, on one stream or across streams), must not leak future information, and must
scale to hundreds or thousands of streams.

Objective of this discussion: the **feature graph** that replaces today's per-stream `FeatureChain`
once features move onto the `FrameBuffer` (iteration 4 of `docs/plans/data-layer-v2`) — node shape,
composition, scheduling, firing, storage, identity and stream binding.

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

Features are pure functions wrapped in structs: configuration only, all state from parameters.
Every registered feature honours this — the struct holds only `id` and `period`. `Update` reads the
rolling window and writes its output into a per-candle `Snapshot`; composition would work by reading
an upstream key out of the same snapshot, but `Dependencies()` is declared and unused.

### Snapshot

`internal/protocol/snapshot.go` — a heterogeneous map with typed keys (`Key[T]`,
`GetSnapshot[T]` / `SetSnapshot[T]`); `KeyRef{Name}` is the untyped form for the dependency graph.
`GetSnapshot` does an unchecked `val.(T)` (`snapshot.go:45`), so type safety rests on convention.
The `Snapshot` leaves the data path in iteration 4 ([[data-layer-v2]]).

### FeatureChain

`internal/domains/features/feature.go` — construction is `duplicate check → topological sort → size
window`:

- `checkDuplicates` errors on two features sharing a `KeyRef`.
- `topologicalSort` is Kahn's algorithm over `Dependencies()`.
- One `SortedWindow[MarketData]` per chain, sized to the **maximum** `Lookback()`.
- `Process(in)` runs one goroutine: push the candle, run every feature in sorted order against a
  fresh `Snapshot`, emit. Used by `data_logger`, one chain per subscription.
- `Evaluate(sub, history)` runs the chain once over one stream's history — the `data_logger_v2`
  bridge, evaluated over each updated stream's `View` history (data-layer-v2 plan, stage 6).

Features without enough history write nothing; exported rows have ragged columns.

### Implementations

| Feature | Output type | Notes |
|---|---|---|
| `sma` | `float64` | Window mean of closes. |
| `ema` | `float64` | Windowed recomputation, not recursive: seeded with an SMA inside a `2 × period` window, so the seed moves with the window. |
| `rsi` | `float64` | Simple average gain/loss, **not** Wilder's smoothing. |
| `vwap` | `float64` | Rolling `period`-candle VWAP over typical price, not session-anchored. |
| `engulfing_candle` | `DirectionalMarker` (int) | Strict engulfing; documented rationale in-file. |
| `swing_highlow` | `DirectionalMarker` | Behind `//go:build wip`, **does not compile** — empty `for neigh := range candles {}` and references to undefined `checkIdx`/`state`/`engulfing`. |

All float outputs are rounded to 6 decimals via `roundDecimals`. `ema` and `rsi` therefore differ
from the conventional values charting tools report; Decisions § Feature graph makes the exact,
recursive forms possible.

### Known gaps and defects

1. **Missing dependency reports as a cycle.** `topologicalSort` sets in-degree from
   `len(Dependencies())` without checking the dependency exists, so a feature depending on an
   unconfigured feature fails with "circular dependency detected in processors".
2. **Indicator maths is untested.** Tests cover `Process` buffering, malformed-candle dropping,
   `Evaluate` against `Process` and `Lookback`; no feature is checked against reference values.

## Decisions

- **Features are pure functions.** Configuration only, no state of their own; identical output for
  identical input, cheap to re-instantiate for sweeps — the reproducibility `docs/Backtesting.md`
  requires.
- **Insufficient history means "absent", not "zero".** A feature that cannot compute writes nothing.
- **Strict definitions over permissive ones** for pattern features (documented for `engulfing`).
- **A feature ID encodes every parameter that distinguishes one instance from another.** Defaulted
  values are omitted (`ema_20`, `vwap_50`); a non-default extends the ID (`ema_20_s3`). A duplicate
  ID is a construction error. See [[config]].
- **Cross-asset metrics are features, not strategy internals.** Correlation, spread, beta and
  similar live in the feature layer as pure functions of aligned history, with the reproducibility
  and exporter visibility of every other feature.
- **Families are a taxonomy, not execution tiers.** Technical, Statistical, Market/Microstructure,
  Cross-Asset and Fundamental/Alternative describe what features exist and what data they need;
  every family's features are nodes of one feature graph (Solution).

### Feature graph

Agreed 2026-10-30. Refines [[pipeline-runtime]] § Features phase, replacing its tier barrier.

- **One node shape.** A node is a feature instance at a coordinate — a stream, or a pair of streams.
  It reads its coordinate's candles, or its dependencies' results at the same coordinate (on each
  end, for a pair). Direct, feature-of-feature, cross-raw and cross-feature are derived from that,
  not separate mechanisms.
- **Composition is in code, never in config.** A feature receives only the `View`; how it reads its
  dependencies is internal to it. `Dependencies()` names the feature types it reads, each once
  (`zscore` reports `rsi`), and nothing when it reads candles only. It validates the config and
  builds the graph; it is not an input list. `Lookback()` is its read depth: its dependencies'
  results, or its candles.
- **An instance is bound at construction** to its coordinate and its dependencies' variants, and
  reads them for the streams it was instantiated on.
- **One instance per dependency variant, at every level.** With `rsi_14` and `rsi_21` configured,
  `zscore_50` becomes `zscore_50-rsi_14` and `zscore_50-rsi_21`; instances multiply across a
  feature's own variants and every level of its dependencies. A dependency type with no configured
  variant is a construction error naming both features.
- **Combining variants of one type is not a feature.** No instance sees two variants of one type, so
  a crossover (`ema_20` against `ema_50`) is not a feature. Indicators built from variants of one type
  (MACD) compute them internally and store their own struct as the result's value.
- **Feature types declare their properties in code:** arity (single-stream or pair) and, for pair
  features, symmetry.
- **Pair features expand like any other.** `corr` declares `returns` once; each pair gets one
  instance per `returns` variant (`corr_100-returns_1`, `corr_100-returns_5`), reading that variant
  on both of its streams.
- **Edges are same-column reads.** Reads of the past — a dependency's or a feature's own — set
  depth, not order.
- **Depth and warm-up come from the graph.** A result's depth is the largest `Lookback` among its
  readers; warm-up adds along paths (`zscore_50-rsi_14` is valid after about 64 bars). Computed at
  construction — the features' part of the `Layout` declaration ([[data-layer-v2]]).
- **Recursive features stay pure.** `x_c = f(x_prev, input_c)` reads `x_prev` from its own result's
  history at its coordinate's step; state beyond the output (Wilder's average gain and loss, running
  sums) is stored in the result. No feature keeps state of its own. Exact EMA and Wilder RSI become
  possible; values depend on the seed column, so warm-up length matters, and across a gap the
  recursion steps from the last `Fresh` value.
- **Determinism by construction.** Each node writes only its own result, so writes are disjoint and
  need no locks; the counter decrement publishes a write to same-frame readers; nothing is emitted
  in completion order.
- **Composed IDs are the composition, in order, dash-separated**, outermost first, generated by
  `ID()`: `zscore_50-rsi_14`, `corr_100-returns_1`. Several dependencies compose in `Dependencies()`
  order (`f_1-rsi_14-vwap_20`), unambiguous since each type's dependencies are fixed in code. A single
  feature's ID never contains `-`.

### Scheduling and firing

- **Ready-driven scheduling.** A node runs once its inputs at the column are computed, through
  pending-input counters over the frame's active subgraph. Each stream's local subgraph is one task,
  run serially in topological order, nodes feeding pair nodes first; finishing such a node
  decrements its pair dependents' counters mid-task, so synchronisation is paid only on cross-stream
  edges. Stream tasks are sharded statically over a fixed worker pool; ready pair nodes go on a
  shared queue.
- **Serial first.** One worker is a precomputed serial topological order with no atomics — the sweep
  default (one run per core) and the first thing built. The parallel scheduler is gated on a
  benchmark at realistic scale: a goroutine hand-off (36–109 ns) costs as much as a cheap feature
  (SMA-50, 21 ns).
- **Features complete within one engine cycle**, between `Next` calls. Running features on arrival,
  before release, stays rejected ([[pipeline-runtime]]).
- **Firing.** A node fires at column `c` only if every input it reads at `c` is `Fresh`. A carried
  input — a `Gap`, `Pending` or `Ended` leg at a due column, or a coarse stream between its due
  columns — does not fire it. `Partial` frames compute over their `Fresh` streams. A skipped column
  leaves the result absent and history reads skip it, like `Gap` candle slots. A node that runs but
  writes nothing (warm-up) still releases its dependents.
- **A pair's sampling clock** defaults to the least common multiple of its streams' periods, so both
  ends are due where it fires.
- **Due-pattern schedules.** Which nodes may fire at `c` depends only on which periods divide `c`, a
  small fixed set of patterns. Each pattern's schedule — stream tasks per worker, pair nodes,
  initial counters, topological order — is built at construction. Per frame: pick the pattern, drop
  streams whose leg is not `Fresh` and pair nodes with a non-`Fresh` end (both known when `Next()`
  returns), copy the counters, run.

### Feature buffer

- **One ring per result, each result stored once.** A result is a feature instance at its
  coordinate, keyed `(feature ID, stream)` or `(feature ID, pair)`. A pair result is one ring,
  referenced from both streams.
- **Slots are `{col, value}`.** A column the feature skipped leaves its slot holding an older column
  (or −1), so a reader checking `col` sees it absent — no flag, no `nil`, no allocation.
- **A value is a float, or the feature's own struct** for multi-output features (MACD). Each ring is
  typed by its feature's value type, so writes never allocate.
- **Sized at construction.** A ring holds `depth × Window` slots at its coordinate's period (the
  stream's, or the pair's sampling clock); a value sits at `(col / Period) % size`. No headroom:
  results are written only on the engine goroutine, at the column being processed.
- **Per-stream feature map, built once and read-only:** `s → map[key]int`, the key a feature ID plus,
  for pair features, the pair — one entry per feature on `s` and one per pair feature for every pair
  `s` belongs to. Features never look it up: their inputs' rings are resolved at construction;
  consumers resolve their keys once, when wired.
- **Memory warning.** Construction estimates the feature buffer's memory (results × slots × slot
  size) and logs a warning above 1 GB, naming the largest universes.

### Universes

- **Every feature binds to a universe**, single-stream and pair alike, so each stream has its own
  feature set. A single-stream feature without one runs on every stream; a pair feature must name
  one, and a universe without pairs is a validation error for it.
- **Dependencies follow their dependents.** A feature instantiates its dependency variants on its
  own streams, also where the dependency's universe does not reach.
- **A universe is a list of self-contained rules.** Its streams are those its rules select or name;
  its pairs are the union of its rules' pairs. Duplicates collapse; self-pairs are excluded.
- **A rule has filters and at most one pairing:**
  - none — selects the streams passing its filters;
  - `references` — each reference paired with every stream passing the filters;
  - `pairs` — a list of explicit pairs, or `"*"` for every pair of streams passing the filters.

  Named streams are taken as named; filters choose their partners.
- **Filters:** symbol (with globs, `*USDT`), timeframe, provider, kind (direct / derived). A list is
  OR, fields are AND.
- **No rules over both ends of a pair.** Same-timeframe pairs come from one `*` rule per timeframe.

## Solution

No plan executed for the feature graph yet; iteration 4 of `docs/plans/data-layer-v2` is its
implementation vehicle.

### Node shape

| Category | Reads | Example |
|---|---|---|
| Direct | Its stream's candles | `rsi_14` |
| Feature of feature | A dependency's result on its stream | `zscore_50-rsi_14` |
| Pair, raw | Both streams' candles | `corr_100` over closes |
| Pair, features | A dependency's result on each end | `corr_100-returns_1` |

### Scheduling

```
worker 1: [A: returns ─▶ rsi ─▶ zscore-rsi]
              │           │
              ▼           ▼
   corr(returns A,B)   corr(rsi A,B)    ← queued when its counter reaches 0
              ▲           ▲
worker 2: [B: returns ─▶ rsi ─▶ …]
```

With tier barriers a frame takes the slowest worker's time per tier, summed over tiers; ready-driven
it takes roughly the larger of the longest dependency chain and total work over workers. The gain is
load imbalance and stacked barriers (pair nodes feeding pair nodes); small for uniform cheap
features over evenly sharded streams.

**Due patterns, example.** A 5m grid with direct 5m, 1h and 1d streams has periods {1, 12, 288} in
grid columns, and three patterns: `c % 288 == 0` (everything may fire), `c % 12 == 0` (5m and 1h
nodes) and every other column (5m nodes). Derived streams take their source's period.

### Feature buffer

```
results   r → ring of {col, value}          one per (feature ID, stream) or (feature ID, pair)
streams   A → { rsi_14: r1, zscore_50-rsi_14: r2, corr_100-returns_1 (A,B): r7, (A,C): r8 }
          B → { rsi_14: r3, zscore_50-rsi_14: r4, corr_100-returns_1 (A,B): r7 }   ← r7 stored once
```

### Universes

Illustrative config, not settled syntax:

```json
"universes": [
  { "name": "usdt", "rules": [ { "symbol": ["*USDT"] } ] },
  { "name": "btc_lead",
    "rules": [
      { "references": ["BTCUSDT:1h:binance"], "timeframe": ["1h"], "provider": ["binance"] },
      { "pairs": [["ETHUSDT:1h:binance", "SOLUSDT:1h:binance"]] }
    ] },
  { "name": "majors_1h", "rules": [ { "pairs": "*", "symbol": ["*USDT"], "timeframe": ["1h"] } ] }
],
"features": [
  { "type": "rsi", "universe": "usdt", "variants": [{ "period": 14 }, { "period": 21 }] },
  { "type": "zscore", "variants": [{ "period": 50 }] },
  { "type": "returns", "variants": [{ "period": 1 }] },
  { "type": "corr", "universe": "btc_lead", "variants": [{ "period": 100 }] }
]
```

`zscore` depends on `rsi` and `corr` on `returns` in code, giving `zscore_50-rsi_14`,
`zscore_50-rsi_21` on the `usdt` streams and `corr_100-returns_1` on the `btc_lead` pairs, with
`returns_1` pulled onto each end.

```
S(rule)      = streams passing the rule's filters
pairs(rule)  = references × S(rule)  |  explicit pairs  |  every pair of S(rule)
pairs(U)     = ∪ pairs(rule)          streams(U) = ∪ streams each rule selects or names
```

### Target architecture: layered feature engine

```
Market Data ─▶ Feature Engineering ─▶ Feature Vector ─▶ Strategy Logic ─▶ Buy / Sell / Hold
                 ├── Technical
                 ├── Statistical
                 ├── Market/Microstructure
                 ├── Cross-Asset
                 └── Fundamental/Alternative
```

- **Technical** — single-stream price/volume indicators (`sma`, `ema`, `rsi`, `vwap`, `engulfing`).
  Reachable now.
- **Statistical** — volatility, z-score, autocorrelation, moments. Reachable now.
- **Market/Microstructure** — order-book and trade-flow features. Needs non-candle data — gated on
  [[data-layer]].
- **Cross-Asset** — correlation, spread, beta, cointegration; pair features bound to universes.
- **Fundamental/Alternative** — funding, on-chain, sentiment, macro; needs data sources not modelled
  today.

The **feature vector** is the per-column feature results a strategy reads; its read contract is
bound up with the strategy input contract ([[strategy-layer]]).

## Open Questions

- **Asymmetric pair order** (beta, hedged spread, lead-lag) — which stream is the first operand, for
  reference rules and `*`. Proposed: `(stream, reference)` from reference rules, both orders from
  `*`, explicit pairs as written; symmetric features store each pair once, in sorted order.
- **Parameters in IDs** must not emit `-`: `iDWithParam` formats with `'g'` (`ema_20_s1e-05`), and
  negative values carry a sign.
- **Per-stream map key** — a struct `{feature ID, pair}` versus a built string, or a dense
  `idx[s][k]` table with −1 where absent; plan-level.
- **`Late` revisions** (pinned) — recompute the subgraph reachable from the revised stream over
  `[c, now]`, pair nodes included.
- **Strategies as graph nodes**, run as soon as their inputs are ready — pending measurements.
- **Heavy features** — asynchronous computation with results in a later frame ([[pipeline-runtime]]).

## WIP

- Feature graph agreed in discussion (Decisions § Feature graph onwards); not planned.
- Registered and working: `sma`, `ema`, `rsi`, `vwap`, `engulfing_candle`.
- `swing_highlow` is broken behind a `wip` build tag. It duplicates swing-detection logic that also
  exists (also broken) inside `buy_the_dip` — see [[strategy-layer]]. Consolidating swing detection
  into this feature is the obvious next step.
- `config.json` enables `engulfing_candle`, `vwap` (20, 50) and `ema` (20, smoothing 2.0); `rsi` and
  `sma` are disabled.
- Every feature declares a config struct decoded strictly ([[config]]); `PeriodConfig` is shared by
  the period-only features, `swing_highlow` adds an even-period rule in `Validate()` — unexercised,
  since it sits behind the `wip` build tag.
- Fibonacci and trend features are listed as ToDo in `WIP.md`.
- `Process` drops and warns on candles the window rejects (zero timestamp) — a stopgap at the
  consumer; `data_logger_v2` validates at ingestion ([[data-layer-v2]] § Normalisation).

## Resources

- `internal/protocol/feature.go`, `internal/protocol/snapshot.go`, `internal/protocol/sorted_window.go`
- `internal/domains/features/`, `internal/engine/data_logger_v2.go` (bridge),
  `internal/engine/data/view.go`, `internal/engine/data/buffer.go`
- `docs/Protocol.md` § Features
- Related: [[data-layer-v2]] (`FrameBuffer`, periods, leg states), [[pipeline-runtime]] (step loop,
  Features phase), [[multi-subscription]], [[strategy-layer]] (consumer), [[data-layer]], [[engine]]
