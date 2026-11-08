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
- **Insufficient history means "absent", not "zero".** A feature that cannot compute returns no
  value; its slot at the column is a `Gap`.
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
- **Composition is in code, never in config.** A feature receives only handles to its inputs at its
  coordinate; how it reads its dependencies is internal to it. Its `Deps` names the feature types it
  reads, each once (`zscore` lists `rsi`), and none when it reads candles only. It validates the
  config and builds the graph; it is not an input list. `Lookback` is its read depth: its
  dependencies' results, or its candles.
- **An instance is bound once, at expansion,** to its coordinate and its dependencies' variants, and
  reads them for the streams it was instantiated on.
- **One instance per dependency variant, at every level.** With `rsi_14` and `rsi_21` configured,
  `zscore_50` becomes `zscore_50-rsi_14` and `zscore_50-rsi_21`; instances multiply across a
  feature's own variants and every level of its dependencies. A dependency type with no configured
  variant is a construction error naming both features.
- **Combining variants of one type is not a feature.** No instance sees two variants of one type, so
  a crossover (`ema_20` against `ema_50`) is not a feature. Indicators built from variants of one type
  (MACD) compute them internally and store their own struct as the result's value.
- **One feature type.** A factory returns a `feature.Feature`: `ID`, `Lookback`, `Pair`, `Deps`,
  `Bind`. Expansion reads arity and dependencies from those fields without binding anything. Variants
  of one type declare the same ones, which expansion checks; symmetry joins them with asymmetric pair
  order (Open Questions). A type's base ID is the name it registers under.
- **A node is the function `Bind` returns.** `Bind`, a `feature.Bind[T]` held as `any`, takes the
  node's `Input` handles from a binder, narrowed to one coordinate, and returns `func() (T, bool)`:
  the value, or false when it cannot compute. A node reads its stream's candles as a prefix of the
  engine's copy, `Input.Last(n)` (§ Feature buffer). A node never writes. `T` is any type the feature
  engine supports (§ Feature buffer); a dependency's is checked at `Bind`. See § Reversed on
  2026-11-08.
- **Pair features expand like any other.** `corr` declares `returns` once; each pair gets one
  instance per `returns` variant (`corr_100-returns_1`, `corr_100-returns_5`), reading that variant
  on both of its streams.
- **Edges are same-column reads.** Reads of the past — a dependency's or a feature's own — set
  depth, not order.
- **One lookback for every stream.** `data.New` keeps the longest `Lookback` across all features, which
  covers every read, feature-on-feature included; per-stream depth waits until memory demands it.
  Warm-up adds along paths (`zscore_50-rsi_14` is valid after about 64 bars).
- **Recursive features stay pure.** `x_c = f(x_prev, input_c)` reads `x_prev` directly from its own
  past results at its coordinate's step, before writing `x_c`, so no extra depth is declared; state
  beyond the output (Wilder's average gain and loss, running sums) is stored in the result, as the
  feature's own struct. No feature keeps state of its own. Exact EMA and Wilder RSI become
  possible; values depend on the seed column, so warm-up length matters, and across a gap the
  recursion steps from the last `Fresh` value.
- **Determinism by construction.** Each node's value is written only to its own result, so writes
  are disjoint and need no locks; the counter decrement publishes a write to same-frame readers; nothing is emitted
  in completion order.
- **Identity.** A type registers its base ID (`ema`); its factory builds the variant ID from its own
  parameters (`ema_20_s3`). Composed IDs are the composition, in order, dash-separated, outermost
  first — `zscore_50-rsi_14`, `corr_100-returns_1` — built by expansion (§ Feature layer). Several
  dependencies compose in `Deps` order (`f_1-rsi_14-vwap_20`), unambiguous since each type's
  dependencies are fixed in code. A variant ID never contains `-`.

### Feature layer

- **Expansion and binding at construction, rings at run.** The composition root injects variants and
  resolved universes, and knows nothing of expansion. The engine's factory calls `features.Expand`,
  a pure function turning them and the stream table into a validated `Graph` — instances with
  composed IDs, every node bound, the pair table, the lookback — so `--validate-only` reports
  expansion and bind errors, all in one pass. A node needs the addresses of its inputs' views, not
  rings, so `features.New(graph)`, in `Run` beside `data.New`, only allocates the rings and builds
  the schedules; `Step(view)` runs one frame. A `Graph` runs once: it holds bound nodes and the views
  they read. See § Reversed on 2026-11-07.
- **One stream table for every layer.** The stream table is its own protocol domain
  (`proto.Streams`), built once by the composition root and injected into the data layer and the
  feature layer: a stream's index in it is its index in the data rings, the feature buffers and
  resolved universes ([[data-layer-v2]] § Subscriptions and the grid).
- **Universe resolution belongs to config** — a pure function of rules over the stream table,
  independent of the feature engine.
- **Views are part of the protocol layer**, in `internal/protocol/frame`. Generic, concrete rings
  (`frame.Ring[T]`, one per entry, reads stepped by `Period × Window`) in a `frame.Buffer[T]`, read
  through a `frame.View[T]` pinned at a frame: a read-only snapshot for consumers, with direct calls.
  Each layer sizes and writes its own buffers. The frame header is unexported, so a view cannot be
  re-pinned; views are made with `NewView` and `Pin`.
- **`protocol` imports nothing outside itself.** `internal/protocol` and its subpackages import only
  each other. The root imports neither `frame` nor `feature`: a cycle either way, through
  `frame_test.go` for `frame`.

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
  is a `Gap` slot carrying the last value, and history reads skip it, like `Gap` candle slots. A
  node that runs but cannot compute (warm-up) is written `Gap` too, and still releases its
  dependents.
- **A pair's sampling clock** defaults to the least common multiple of its streams' periods, so both
  ends are due where it fires.
- **Due-pattern schedules.** Which nodes may fire at `c` depends only on which periods divide `c`, a
  small fixed set of patterns. Each pattern's schedule — stream tasks per worker, pair nodes,
  initial counters, topological order — is built at construction. Per frame: pick the pattern, drop
  streams whose leg is not `Fresh` and pair nodes with a non-`Fresh` end (both known when `Next()`
  returns), copy the counters, run.

### Feature buffer

- **The feature layer owns the store**; the data layer never sees it. Reverses the feature entries
  sketched in [[data-layer-v2]] § FrameBuffer (`View.Feature`, `FeatureWriter`).
- **Only the feature engine holds feature buffers.** Holding a `frame.Buffer` is holding write access
  to its rings, so nodes and consumers get views, never buffers or rings: nodes cannot pin their
  inputs themselves, and the engine hands them their inputs' views. The engine writes what each
  node returns into the node's own ring.
- **One ring per result, each result stored once.** A result is a feature instance at its
  coordinate. Each composed ID has one typed buffer — not each variant ID, which `zscore_50-rsi_14`
  and `zscore_50-rsi_21` share — with one ring per stream of the stream table, or per pair of the
  feature layer's pair table (every pair a pair instance runs on, sorted), empty where the ID does
  not run. A pair result is one ring, at the pair's index.
- **Slots carry their state:** `{Col, ValCol, Val, State}`, as candle slots do. A column a feature
  skips is a `Gap` slot carrying the last `Fresh` value, so absence is a state rather than a column
  mismatch.
- **Value types are the engine's switch.** Each ring is typed by its feature's value type, so writes
  never allocate. The contract leaves `T` unconstrained; the feature engine supports the types its
  switch names — `float64` and `int` today. A multi-output feature (MACD) or recursive state declares
  its own struct in its feature's domain package, which the engine imports for its case and
  consumers for `ViewOf`. A compile-time list of the types would put domain types in `protocol`, an
  import cycle, so a missing case or a malformed `Bind` is caught by a test over every registered type
  and by expansion (`--validate-only`), not the compiler.
- **Only the feature engine creates feature buffers.** It switches on a feature's `Bind`
  (`case feature.Bind[float64]`), which hands it the typed function, and instantiates
  `frame.Buffer[T]` there — Go instantiates it only where `T` is known. Registering through the
  engine would keep the switch out, but domain packages would import the engine; merging the
  contract into the feature domain would move feature registration out of `internal/registry`.
  Rejected as value types: `map[string]float64`, which allocates per write or aliases one map across
  the history.
- **Sized to the lookback.** A stream's result rings hold `lookback × Window` slots at its `Period`,
  a value at `(col / Period) % size`. No headroom: results are written only on the engine goroutine,
  at the column being processed. Result rings wrap sooner than data rings, but for every column a
  view can read both hold that column's entry.
- **A pair's ring steps at its sampling period** — `max(Pa, Pb)`, where both ends are due — and one
  bar is its coarser end's: `Window = max(Pa × Wa, Pb × Wb) / Period`. Sizing it like its coarser
  end's fails for a rolling 1h (period 1, window 12) paired with a direct 1h (period 12): at period
  1 the ring anchors on columns the pair never writes. The timeframe enum is a divisibility chain, so
  both ends' periods divide the pair's — an invariant new timeframes must keep.
- **One candle copy per stream.** The engine owns a `[]proto.Candle` per stream, as deep as the
  longest `Lookback` among the instances reading its candles (a pair counts for both ends), and
  refills it with `View.Data` at each frame the stream updates, before any node fires. A node's
  `Input.Last(n)` is its first n candles: one ring walk per stream, no copy per node. Every node on
  the stream shares it, never writing it or keeping it past its call. Dependency results have no copy
  until a feature reads one (iteration 3); they read through `Data` into scratch.
- **No per-stream feature map.** Buffers share the stream table's and the pair table's indices, so
  features never look anything up — their inputs' views and ring indices are taken at `Bind` — and
  consumers resolve a composed ID once with `ViewOf` and a pair with `View.Pair`.
- **Memory warning.** `features.New` estimates the feature buffer's memory (results × slots × slot
  size, and the candle copies) and logs a warning above 1 GB, naming the largest universes.

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

  Named streams are taken as named; filters, `kind` included, choose their partners. An
  explicit-pairs rule takes no filter.
- **Filters:** symbol and provider (instance names), both with globs (`*USDT`, `btc_*`); timeframe;
  kind (direct / derived). A list is OR, fields are AND, an absent field does not constrain.
- **A named stream is its stream ID** — a direct stream's subscription ID (`BTCUSDT:1h:binance`), a
  derived stream's `symbol:timeframe:derived_<source ID>` (`BTCUSDT:1h:derived_BTCUSDT:5m:binance`)
  — so a name matches at most one stream, and one that matches none is an error. Filters match a
  derived stream by its own timeframe and its source's provider. Stream IDs are unique by
  construction, so no rule on derivations is needed.
- **No rules over both ends of a pair.** Same-timeframe pairs come from one `*` rule per timeframe.

### Reversed on 2026-11-03

Superseded by § Feature layer and § Feature buffer above; kept for context.

| Was | Now | Why |
|---|---|---|
| The composition root calls the exported expansion function and injects the instances; the engine binds, allocates and schedules | The engine's factory calls `Expand`, which returns a validated `Graph`; `features.New(graph)` runs in `Run` | The composition root holds no feature-layer logic; `--validate-only` still covers expansion; the engine's run state is built in `Run`, beside `data.New` |
| A per-stream feature map, `s → map[key]int` | Buffers indexed by the stream table and a pair table | One index space with the data layer; no map key to choose |
| A pair's ring sized like its coarser end's | Stepping at `max(Pa, Pb)`, with its coarser end's bar | A rolling coarse end has a finer period than the pair's sampling clock |

### Reversed on 2026-11-07

Superseded by § Feature graph, § Feature layer and § Feature buffer above; kept for context.

| Was | Now | Why |
|---|---|---|
| `Feature[T]`, `FeatureNode[T]`, `Factory[T]`, `Type`, `Variant`, `Descriptor` and a registry type of their own | One `feature.Feature` struct; a node is the function `Bind` returns; the generic registry | They split one feature by lifetime and by typed or erased form |
| `features.New` allocates the store and binds | `Expand` binds; `New` allocates | `--validate-only` covers every `Bind`; a node needs views' addresses, not rings |
| Rings, slots and `View` in `internal/flow/frame` | `internal/protocol/frame` | Views are part of the protocol layer, and `protocol` imports nothing outside itself |

### Reversed on 2026-11-08

Superseded by § Feature graph and § Feature buffer above; kept for context.

| Was | Now | Why |
|---|---|---|
| A node copies its candle reads into scratch allocated in `Bind`: `Input.Data(n, dst)` | Candle reads are prefixes of one engine copy per stream, `Input.Last(n)`, refilled once per frame at the stream's longest lookback | Scratch held a copy of a stream's history per variant. With the reference variants, a copy per stream read about 3× faster than per-node scratch and 2.2× faster than iterating the ring in place per node, holding the longest lookback per stream |

## Solution

Executed stages of `docs/plans/features-layer-v2/main.md`, iteration 4 of
`docs/plans/data-layer-v2`: `stage-1-frame.md` (`frame`), `stage-2-universes.md` (the stream table and
universes), `stage-3-contract-store.md` (`frame` in the protocol layer, the contract, the registry, and
the feature graph, engine and view), `stage-4-ported-features.md` (the five feature types ported, read
from one candle copy per stream, and checked against the originals).

### Node shape

| Category | Reads | Example |
|---|---|---|
| Direct | Its stream's candles | `rsi_14` |
| Feature of feature | A dependency's result on its stream | `zscore_50-rsi_14` |
| Pair, raw | Both streams' candles | `corr_100` over closes |
| Pair, features | A dependency's result on each end | `corr_100-returns_1` |

### Feature layer pieces

| Piece | Job | When | Home |
|---|---|---|---|
| Contract | `Feature`, `Bind[T]`, binder and `Input` handles | — | `internal/protocol/feature` |
| Feature types | Pure maths; a factory returning a `Feature`, registered under the base ID | — | `internal/domains/featuresv2` until the originals in `internal/domains/features` retire |
| Stream table | One entry per direct subscription and derived window; the index space of every layer | Composition root | `internal/protocol` |
| Registry and config | Type → factory; variants decoded strictly; `universes` block, `universe` per feature; universe resolution | Config parse, composition root | `internal/registry`, `internal/config` |
| Expansion | Variants × dependency variants × coordinates; validation; composed IDs; each variant's value type recovered; every node bound; the `Graph` | Engine construction | `internal/engine/features` (pure function) |
| Feature engine | Allocate the rings (`frame.Buffer[T]` per composed ID) and one candle copy per stream, due-pattern schedules; `Step(view)` | `Run`, then each frame | `internal/engine/features` |
| Frame primitives | Generic rings and buffers, pinned `View[T]` | — | `internal/protocol/frame` |
| Consumers | Exporter and strategies read through the `View` | After `Step` | — |

Leaving the data path: `Snapshot`, `SortedWindow`, `FeatureChain`, `Update`, and the
`data_logger_v2` bridge.

### Contract sketch

Implemented by `docs/plans/features-layer-v2/stage-3-contract-store.md`. A factory returns one `Feature`;
expansion calls its `Bind` once per instance, taking typed handles to its inputs at its coordinate —
value types are checked there, at construction — and the function it returns is the node.

```go
reg.FeatureTypes.Register("zscore", newZScore)

type Feature struct {
	ID       string   // "zscore_50", rendered by the factory
	Lookback int
	Pair     bool     // runs on a pair of streams
	Deps     []string // {"rsi"}
	Bind     any      // a Bind[T]
}

// Inputs: Candles(b), Dep[T](b, "rsi"), PairCandles(b), PairDep[T](b, "rsi").
// The node reads through them; false: cannot compute (warm-up).
type Bind[T any] = func(b *Binder) func() (T, bool)
```

- Firing is the engine's job; a node runs only when every input is `Fresh`. The engine writes
  `Fresh`, or `Gap` when the node returns false, and `Gap` / `Ended` at columns it skips.
- The binder keeps its first error and the engine reads it after `Bind`, so `Bind` returns none and
  never reads an input.
- Nodes read candles through `Input.Last(n)`, a prefix of the engine's copy of their stream, and
  dependencies through `Data(n, dst)`. Scratch, allocated in `Bind`, holds dependency reads and
  working memory, never carried values.

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
buffer per composed ID, typed by its value      rings indexed like every layer's
rsi_14               frame.Buffer[float64]      [A] [B] [C]       stream table; empty where it does not run
zscore_50-rsi_14     frame.Buffer[float64]      [A] [B] [ ]
corr_100-returns_1   frame.Buffer[float64]      [AB] [AC]         pair table; (A,B) stored once
```

### Universes

Settled syntax (`stage-2-universes.md`): validated by `Load`, resolved by the composition root,
consumed from stage 5:

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
  `*`, explicit pairs as written; symmetric features store each pair once, in sorted order. Symmetry
  joins `Feature` with it.
- **Parameters in IDs** must not emit `-`: `iDWithParam` formats with `'g'` (`ema_20_s1e-05`), and
  negative values carry a sign. Planned: the ported `iDWithParam` formats with `'f'`
  (`ema_20_s0.00001`), identical to `'g'` for values in [1e-4, 1e6) (stage 4); `Expand` rejects a
  variant ID with `-`, as from a negative value (stage 5).
- **How a strategy reads a struct result.** `ViewOf` is in `engine/features` and a struct value type
  in its feature's domain, while a domain imports only `protocol` ([[engine]]). To settle before
  strategies read features.
- **`Input` reads once dependency results have copies** (iteration 3). Candle inputs read through
  `Last`; `Data` and `Latest` stay for dependency inputs, which have no copy (stage 4). If dependency
  results get copies, refilled after a dependency fires and before its dependents, `Input` could
  become `Last`-only. To assess then: whether copies pay off for dependents reading only the current
  value, and whether any node needs an input's state or age (`Late` recomputes, a relaxed firing
  rule), which only `Latest` gives.
- **Pair features over pair features** (a z-score of a spread) — a node reading a pair result;
  iteration 1 allows single-stream dependencies only.
- **`Late` revisions** (pinned) — recompute the subgraph reachable from the revised stream over
  `[c, now]`, pair nodes included.
- **Strategies as graph nodes**, run as soon as their inputs are ready — pending measurements.
- **Heavy features** — asynchronous computation with results in a later frame ([[pipeline-runtime]]).

## WIP

- Feature graph agreed in discussion (Decisions § Feature graph onwards); planned in
  `docs/plans/features-layer-v2/main.md`.
- `frame` implemented — generic rings, buffers and pinned views — with the data layer running on it:
  `docs/plans/features-layer-v2/stage-1-frame.md`. Stage 3 moved it to `internal/protocol/frame`.
- The stream table in `internal/protocol`, built by the composition root, and universes in config,
  resolved over it, implemented; nothing consumes universes yet —
  `docs/plans/features-layer-v2/stage-2-universes.md`. Config guide: `configs/README.md`.
- The contract in `internal/protocol/feature`, `reg.FeatureTypes`, and the feature graph, engine and
  view in `internal/engine/features` implemented, tested with hand-built instances and hand-fired
  nodes; nothing registers, expands or schedules yet —
  `docs/plans/features-layer-v2/stage-3-contract-store.md`.
- `sma`, `ema`, `rsi`, `vwap` and `engulfing_candle` ported in `internal/domains/featuresv2`,
  registered in `reg.FeatureTypes` and reading candles from the engine's copy per stream; they equal
  the originals at every column where the bridge evaluates, over a random four-month sample of the
  reference dataset. Nothing expands them yet —
  `docs/plans/features-layer-v2/stage-4-ported-features.md`.
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
- `internal/protocol/feature/`, `internal/protocol/frame/`, `internal/engine/features/`
- `internal/domains/features/`, `internal/engine/data_logger_v2.go` (bridge),
  `internal/engine/data/view.go`, `internal/engine/data/buffer.go`
- `docs/Protocol.md` § Features
- Related: [[data-layer-v2]] (`FrameBuffer`, periods, leg states), [[pipeline-runtime]] (step loop,
  Features phase), [[multi-subscription]], [[strategy-layer]] (consumer), [[data-layer]], [[engine]]
