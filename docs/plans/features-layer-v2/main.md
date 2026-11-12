# Features layer v2 — main plan

## Context

The feature layer design in [features.md](../../discussions/features.md) is settled at concept level
(Decisions § Feature graph onwards); nothing is implemented. Today `data_logger_v2` evaluates the
per-stream `FeatureChain` over each updated stream's `View` history — the temporary bridge from
[data layer v2](../data-layer-v2/main.md) stage 6 — and exports a `Snapshot` per stream update.

This plan replaces the bridge with the feature layer: a feature graph expanded at the composition
root, a feature store owned by the feature layer, and a feature engine that steps every frame. It
supersedes iteration 4 of the data layer v2 plan. Iteration 1 builds the serial engine and ports the
five existing features with their maths unchanged; iteration 2 adds the parallel scheduler;
iteration 3 adds new features; iteration 4 makes recursive features exact.

This is the **main** doc of a tiered plan: it fixes the iterations and iteration 1's stages. Each
stage's fine-grained plan is its own doc, approved before that stage is implemented. Do not
implement from this doc.

## Decisions carried in

From [features.md](../../discussions/features.md):

- **Graph.** A node is a feature instance at a coordinate — a stream or a pair. Composition is in
  code: each feature declares its arity and the feature types it reads (`Pair`, `Deps`; symmetry
  with asymmetric pair order); the config never names inputs. One instance per dependency variant,
  at every level; pair features take the same variant on both ends.
- **Identity.** A type registers its base ID; its factory builds the variant ID; expansion composes
  the rest, dash-separated, outermost first (`zscore_50-rsi_14`).
- **Universes.** Every feature binds to one; without one a single-stream feature runs on every
  stream. Self-contained rules (references, explicit pairs, `*`) with filters on symbol, timeframe,
  provider and kind; derived streams match by their own timeframe and their source's provider.
  Resolution belongs to config.
- **Pieces.** The composition root injects variants and resolved universes; the engine's factory
  calls `features.Expand`, a pure function returning a validated `Graph` with every node bound, so
  `--validate-only` covers expansion and binding; `features.New(graph)`, in `Run`, allocates the
  rings, builds the schedules and steps each frame.
- **Store.** Owned by the feature layer. One ring per feature instance, sized `lookback × Window`
  at its stream's `Period`, aligned by column with the data rings; a pair's ring steps at its ends'
  larger period, one bar its coarser end's. A memory warning above 1 GB.
- **Views in `internal/protocol/frame`.** Views are part of the protocol layer. Read-only, pinned at
  a frame; writes stay inside each layer.
- **Scheduling and firing.** Ready-driven over the frame's active subgraph, serial first. A node
  fires only when every input is `Fresh`; a pair samples at its ends' larger period; schedules are
  precompiled per due pattern. Features complete between `Next` calls.
- **Purity.** Features keep no state; recursive ones read their own past results.
- **One lookback for every stream**, kept by `data.New`.

## Decisions taken by this plan

- **Generic rings in `protocol/frame`.** `Slot[T]`, `Ring[T]` and `Buffer[T]` are concrete generic
  types and `View[T]` pins a buffer at a frame: reads are written once and called directly. Each
  layer sizes and writes its own buffers; the data layer keeps its staging, and the derived window
  count is dropped. `View[T]` embeds an unexported header, so a view cannot be re-pinned; views are made with
  `NewView` and `Pin`. Stage 1 built them in `flow/frame`; stage 3 moved them.
- **`protocol` imports nothing outside itself.** `internal/protocol` and its subpackages import only
  each other, which is why `frame` moved: the contract reads views.
- **Feature slots carry state.** A result is written at every due column of its coordinate — `Fresh`,
  or `Gap` carrying the last `Fresh` value — and `Ended` once its stream ends; a column mismatch means
  overwritten or not started, as for candles. Replaces `{col, value}` slots and absence by column
  mismatch ([features.md](../../discussions/features.md)).
- **One typed buffer per composed ID**, one ring per stream of the stream table, or per pair of the
  feature layer's pair table for pair features, empty where the ID does not run. A buffer per variant
  ID would put `zscore_50-rsi_14` and `zscore_50-rsi_21` on the same rings.
- **One feature type; the engine recovers the value type.** The contract, in
  `internal/protocol/feature`: a factory returns a `feature.Feature` — `ID`, `Lookback`, `Pair`,
  `Deps`, `Bind` — and the registry is the generic `registry[feature.Feature]`. `Bind`, a
  `feature.Bind[T]` held as `any`, takes a node's inputs at one coordinate and returns the node,
  `func() (T, bool)`. The feature engine recovers `T` with a type switch on `Bind`, one case per
  supported type — `float64` and `int` — so only it creates `frame.Buffer[T]`. A struct value type
  (MACD) is declared in its feature's domain package and adds a case. A missing case or a malformed
  `Bind` is caught by `Expand` (`--validate-only`) once a config enables the type, not the compiler:
  a compile-time list of the types would put domain types in `protocol`. Stage 4's test over the
  registered types was removed in stage 5. Nodes never write: the engine
  writes what the node returns — `Fresh`, or `Gap` when it cannot compute.
- **`Bind` runs once, before any ring exists.** A node needs the addresses of its inputs' views, not
  rings. `Expand` binds every instance, so `--validate-only` covers dependency value types and arity,
  and `New` only allocates. A `Graph` holds bound nodes and the views they read, so it runs once.
- **Consumers read only the feature layer's view.** `Step` returns a `features.View` pinned at the
  data view's frame, embedding it; `ViewOf[T](fv, id)` narrows it to a composed ID's
  `frame.View[T]`, type-checked.
- **Buffers stay with their owners.** Holding a `frame.Buffer` is holding write access to its rings,
  so only the data layer and the feature engine hold buffers. Nodes never hold one, not even to pin
  an input: the engine pins their inputs and hands them views.
- **`data_logger_v2` only.** Nothing already in place changes: `data_logger`, `FeatureChain`,
  `proto.Feature`, `Snapshot`, `SortedWindow`, the existing feature implementations, `reg.Features`
  and the exporter contract stay as they are, as `data_logger` stayed beside `data_logger_v2`.
- **The five features are ported beside the originals.** `sma`, `ema`, `rsi`, `vwap` and
  `engulfing_candle` get implementations on the new contract with their maths copied unchanged,
  rounding included, registered in a new registry under the same type names.
- **Iteration 1 is a regression.** `data_logger_v2` exports the same records with the same feature
  values as through the bridge. Exact recursion, which changes values, is iteration 4.
- **Test-only features exercise dependencies and pairs** until iteration 3 adds real ones: expansion,
  store and scheduler handle both from iteration 1.
- **This plan precedes the live-mode plan** (data layer v2 iteration 2), which is planned against
  `frame` once stage 1 lands.

## Structure

Tiered. Stage docs live beside this one, produced iteratively:

- `stage-1-frame.md` — *implemented.*
- `stage-2-universes.md` — *implemented.*
- `stage-3-contract-store.md` — *implemented.*
- `stage-4-ported-features.md` — *implemented.*
- `stage-5-expansion.md` — *implemented.*
- `stage-6-engine.md` — *to plan.*
- `stage-7-data-logger-v2.md` — *to plan.*

Ordering: 1 and 2 are independent; 3 depends on 1; 4 on 3; 5 on 2 and 3; 6 on 1, 3 and 5; 7 on 4
and 6.

## Iteration 1 — serial feature engine

**Goal.** `data_logger_v2` runs features through the feature engine instead of the bridge: variants
from the new registry, universes from config, instances expanded and bound by the engine's factory,
results in the feature store, one serial step per frame. Exports are identical to the bridge's; `data_logger`
is unchanged.

```
composition root:       config ─▶ registry v2 (variants) + resolved universes ─▶ injected
data_logger_v2 factory: features.Expand(streams, universes, specs) ─▶ graph         (--validate-only)
data_logger_v2 Run:     features.New(graph) ─▶ loop: data.Next ─▶ pacer ─▶ fe.Step(view) ─▶ exporter
                                                         │                     │
                                                         └───── frame.View ────┘ data rings + result rings
```

### Stage 1 — `flow/frame` ✅ implemented

- **Goal:** generic rings, buffers and pinned views shared by the data buffer and the feature store;
  the data layer runs on them with behaviour unchanged.
- **Touches:** new `internal/flow/frame` — `Slot[T]`, `Ring[T]`, `Buffer[T]`, `View[T]`, `NewView`,
  `Pin`, `Status`, `State`; `internal/engine/data` (`FrameBuffer` on `frame.Buffer[proto.Candle]`,
  the derived window count dropped; `view.go` deleted; `Next` returns `frame.View[proto.Candle]`);
  `internal/engine/data_logger_v2.go`; tests; `docs/Architecture.md`.
- **Boundary:** the data layer's sizing, staging and gate stay in `internal/engine/data`. No feature
  code yet.
- **Acceptance:** every existing `internal/engine/data` test passes unchanged in behaviour; reads
  allocate nothing; `go test -race ./internal/engine/data/... ./internal/flow/frame/...` clean;
  benchmarks within noise; `data_logger_v2` output unchanged.
- Plan: `stage-1-frame.md`. Stage 3 moved the package to `internal/protocol/frame`.

### Stage 2 — stream table and universes ✅ implemented

- **Goal:** one stream table shared by config, the data layer and the feature layer; universes are
  declared, validated and resolvable over it.
- **Touches:** `internal/protocol` — the stream table, moved from `internal/engine/data`, built by the
  composition root and injected into `data.New` (`internal/engine/data`, `data_logger_v2`, `cmd`);
  `internal/config` — the `universes` block, `universe` on feature entries, the resolver (a pure
  function of rules over the stream table, naming streams by their IDs), every validation error
  collected; tests; `configs/README.md`, the config guide.
- **Boundary:** the data layer runs on the injected table with behaviour unchanged; nothing consumes
  universes yet; `data_logger` ignores them.
- **Acceptance:** resolution unit-tested for stream selection, references, explicit pairs and `*`;
  filters with symbol and provider globs; `kind` as a filter; derived streams named by their ID and
  matching their source's provider in filters; duplicate pairs collapsing and self-pairs excluded.
  Path-named errors for unknown universes, malformed rules and unknown stream IDs;
  `--validate-only` covers them.
- Plan: `stage-2-universes.md`.

### Stage 3 — contract, registry and store ✅ implemented

- **Goal:** `frame` in the protocol layer, the new feature contract, its registry, and the graph,
  engine and view that bind nodes and hold their results.
- **Touches:** `internal/flow/frame` moved to `internal/protocol/frame`, its importers' paths; new
  `internal/protocol/feature` — `Feature`, `Bind[T]`, the binder and its `Input[T]` handles;
  `reg.FeatureTypes` in `internal/registry`, a `registry[feature.Feature]`; new
  `internal/engine/features` — the type switch on `Bind`, `results[T]` with one `frame.Buffer[T]` per
  composed ID in the stream and pair tables, `newGraph` binding every node before any ring exists,
  `New` allocating them, `features.View` and `ViewOf[T]`; tests.
- **Boundary:** no expansion or scheduling; tests build instances, pin and fire by hand.
- **Acceptance:** nothing under `internal/protocol` imports outside it; a bound node reads candles and
  its dependency's results through views pinned at the frame, and the engine writes what it returns;
  value types are checked at `Bind` and at `ViewOf`; every supported value type allocates and binds,
  and an unsupported one is an error naming it; every bind problem is reported in one pass with no
  ring allocated; a warming-up or skipped column reads as `Gap` carrying the last value; history
  stepped like the data rings on direct and rolling streams; a pair result is one ring, stepping at
  its sampling period; no allocation on writes or reads.
- Plan: `stage-3-contract-store.md`.

### Stage 4 — ported features

- **Goal:** `sma`, `ema`, `rsi`, `vwap` and `engulfing_candle` on the new contract.
- **Touches:** the ring walk in `internal/protocol/frame` stepping slot indices; one candle copy per
  stream in `internal/engine/features`, refilled once per frame at the stream's longest lookback and
  read by nodes as prefixes through `Input.Last`; new `internal/domains/featuresv2` (`featv2`) beside
  `internal/domains/features`, registering in `reg.FeatureTypes`, its IDs formatting floats with
  `'f'`; the data layer's reference sampling moved into `internal/engine/data/datatest`, shared by both
  reference tests; tests, among them, in `internal/engine/features`, one building a variant of every
  registered type from a table of options and checking it against the engine's switch, a comparison
  with the originals over the reference dataset, and a check that a lookback beyond the data layer's
  cap fails `data.New`.
- **Acceptance:** each ported feature produces the original's value at every column where the bridge
  evaluates, over a random four-month sample of the BTCUSDT reference dataset per run (skipped without
  it, like the data layer's), on direct and rolling streams, including warm-up absence; IDs match the
  originals' (`ema_20_s3`); every registered type is in the table and has a case in the switch; nodes
  hold no candle scratch and allocate nothing per call.
- Plan: `stage-4-ported-features.md`.
- **Changed after the stage:** IDs include defaults (`ema_20_s2`), built by `variantID` from each
  port's config struct; the originals' still omit them (`ema_20`). See Open items (stage 7).

### Stage 5 — expansion ✅ implemented

- **Goal:** the pure function that turns configuration into a validated feature graph.
- **Touches:** `internal/engine/features` (`Expand(streams, universes, specs) (*Graph, error)`:
  variants × dependency variants × coordinates, dependencies pulled onto their dependents' streams,
  composed IDs, the graph checks, ending in stage 3's `newGraph`, which binds every instance; the
  `Graph` holds the columns, the pair table and the lookback, the instances left to stage 6;
  stage 4's registered-types test removed); `internal/protocol/feature` (`Feature.Validate`, the
  checks on one variant); the composition root (`cmd`) resolves each universe with `config.Resolve`,
  keeping only those that resolve (`config.ResolveUniverses` removed), builds the new registry's
  variants beside today's features, validates them and injects them as specs — the config entry's
  type, its universe's name, the variant — with the resolved universes, and `wire` splits a joined
  engine error per part;
  `data_logger_v2`'s factory calls `Expand`; tests with test-only features.
- **Boundary:** the graph is built in `data_logger_v2`'s factory but not run yet.
- **Acceptance:** expansion over chains, several dependency types and pairs; a type in several
  entries, each with its own universe; errors for an unknown dependency type or one with no variant,
  a pair dependency type, type-level cycles, a pair feature without a universe or with a universe
  without pairs, a variant ID configured twice, variants of one type declaring different `Pair` or
  `Deps`, a value type the engine does not support, and a bind error, all from `Expand`; a variant
  ID containing `-` from `Feature.Validate`; `--validate-only` reports them one line each, `Expand`
  stopping at its first check stage with errors and reporting all of that stage's; any error fails
  the build; `data_logger` unchanged but for an unregistered type or a bad
  option reported once per registry.
- Plan: `stage-5-expansion.md`.

### Stage 6 — serial engine

- **Goal:** the feature engine runs a frame.
- **Touches:** `internal/engine/features` — `New(graph, …)`, called in `Run`: memory estimate and
  warning, counting the candle copies (stage 4), the rings, due-pattern schedules; `Step(view)`: pick
  the pattern, drop nodes whose inputs are not `Fresh`, run the serial topological order; tests with
  test-only features.
- **Acceptance:** dependencies computed before their dependents; a coarse stream's nodes fire only at
  its due columns; `Gap` and `Ended` legs fire nothing, and their nodes' results are written `Gap` /
  `Ended` carrying the last value; `Partial` frames compute their `Fresh` streams; pairs fire at their
  ends' larger period only when both ends are `Fresh`; a run equals a brute-force evaluation over the
  same frames; the warning fires above 1 GB; no allocation per `Step`.

### Stage 7 — `data_logger_v2` integration

- **Goal:** `data_logger_v2` runs the feature engine; the bridge is gone.
- **Touches:** `internal/engine/data_logger_v2.go` (`features.New(graph)` in `Run`, `Step` after the
  pacer, the exported record filled from the feature layer's view, `data.New` sized from the graph's
  lookback); its tests; a serial `Step` benchmark at N = 10, 100, 1000
  streams; `docs/Commands.md`.
- **Acceptance:** on `configs/data_logger_v2.json`, records and feature values identical to the
  bridge's output; `--validate-only` valid for both configs; `make check` clean; benchmark checked
  in.

### Iteration 1 — done when

- `make check` and `go test -race` on `internal/protocol/frame`, `internal/engine/data` and
  `internal/engine/features` clean.
- `data_logger_v2` exports through the feature engine, identical to the bridge; `data_logger`
  behaves as before.
- Discussion and reference docs updated (below).

## Iteration 2 — parallel scheduler

High-level only; gated on stage 7's benchmark.

- Fixed worker pool; each stream's local subgraph one task, sharded statically; pair nodes queued
  once their counters reach zero, decremented mid-task by their producers.
- Identical results at every worker count, under `-race`; the serial path stays the one-worker case.
- Benchmark against the serial engine at N = 10, 100, 1000; the default worker count follows it.

## Iteration 3 — new features

High-level only.

- `returns`, `zscore` (over `rsi`) and `corr` (over `returns`): feature-of-feature and pair nodes
  end to end over universes.
- Exporter output for pair results.
- The composition root builds types that exist only in the new registry.

## Iteration 4 — exact recursion

High-level only.

- Recursive `ema` and Wilder `rsi` on the new contract, reading their own past results; values
  change against `data_logger`, documented in the exports.

## Targeted discussion-doc updates (as stages land)

- [features.md](../../discussions/features.md): WIP references this plan; Solution references each
  executed stage; resolve the open items below.
- [data-layer-v2.md](../../discussions/data-layer-v2.md): the `View` and rings in `frame` (stages 1
  and 3); the stream table in `internal/protocol`, built by the composition root (stage 2); feature
  entries in the feature layer (stage 7).
- [pipeline-runtime.md](../../discussions/pipeline-runtime.md): Features phase implemented, serial
  (stage 6).
- [config.md](../../discussions/config.md): universes implemented, the derivation rule dropped
  (stage 2).
- [README.md](../../discussions/README.md): features row status.
- [data layer v2 main plan](../data-layer-v2/main.md): iteration 4 points to this plan.
- `docs/Architecture.md`: `frame` (stage 1, moved to `internal/protocol/frame` in stage 3), the stream
  table in `internal/protocol` (stage 2), what `protocol` may import, the contract in
  `internal/protocol/feature` and `reg.FeatureTypes` (stage 3), `internal/engine/features` (stage 6).
- `docs/Commands.md`: benchmark commands (stage 7).
- `configs/README.md`: written in stage 2; pair-feature rules (stage 5); universes in use, and the
  feature types from the new registry (stage 7).

## Out of scope

- `data_logger` and everything it runs on — retiring them is a separate follow-up.
- `Late` recompute — pinned in [features.md](../../discussions/features.md).
- Strategies and their reads of features.
- Per-stream candle depth.
- Asymmetric pair order — open in [features.md](../../discussions/features.md).
- Heavy (asynchronous) features.
- The live release path — the live-mode plan, after this one.

## Open items to resolve within stages

- The contract's home — resolved in stage 3: `internal/protocol/feature`, beside
  `internal/protocol/frame`. The protocol root cannot host it: importing `frame` there is a test cycle
  through `frame_test.go`. The subpackage also keeps the new names clear of `proto.Feature`.
- What a node receives, and how it writes `Gap` for a column it skips — resolved in stage 3: nothing;
  a node is the function `Bind` returns, reading `Input` handles taken at `Bind`, narrowed to its
  coordinate, and returning its value. The engine writes `Fresh` or `Gap`, and `Gap` / `Ended` for
  columns it skips.
- The new registry's shape and name beside `reg.Features` — resolved in stage 3: `reg.FeatureTypes`,
  a `registry[feature.Feature]`.
- The ported features' package — resolved in stage 4: `internal/domains/featuresv2` (`featv2`),
  renamed into `internal/domains/features` when the originals retire.
- How the composition root builds the stream table `Expand` needs — resolved in stage 2:
  `proto.NewStreams`, injected as `_streams`.
- Filling the exported record from results — the exporter contract is untouched; only results
  `Fresh` at the record's column are exported, since a `Gap` carries the last value (stage 7).
- `FeatureChain.Evaluate` loses its only caller in stage 7: kept as the regression reference, or
  removed with the bridge (stage 7).
- Stage 7's comparison with the bridge: the ports' `ema` IDs include the default smoothing
  (`ema_20_s2`, `ema_50_s2` on `configs/data_logger_v2.json`), the originals' omit it, so the
  exported records differ in those keys. Either the comparison maps the IDs, or the acceptance takes
  the new keys as intended (stage 7).
- Variants are built in both registries, `_features` from `reg.Features` and the specs from
  `reg.FeatureTypes`, from the same entries, so an unregistered type or a bad option is reported
  twice. Accepted until `data_logger` and the originals retire (stage 5).
