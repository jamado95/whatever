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
  code: each type's registered descriptor names the feature types it reads (arity, symmetry,
  dependencies); the config never names inputs. One instance per dependency variant, at every
  level; pair features take the same variant on both ends.
- **Identity.** A type registers its base ID; its factory builds the variant ID; expansion composes
  the rest, dash-separated, outermost first (`zscore_50-rsi_14`).
- **Universes.** Every feature binds to one; without one a single-stream feature runs on every
  stream. Self-contained rules (references, explicit pairs, `*`) with filters on symbol, timeframe,
  provider and kind; derived streams match by their own timeframe and their source's provider.
  Resolution belongs to config.
- **Pieces.** The composition root calls a pure expansion function exported by
  `internal/engine/features` and injects the instances; the feature engine binds them, allocates the
  store, builds the schedules and steps each frame.
- **Store.** Owned by the feature layer. One ring per feature instance, sized `lookback × Window`
  at its stream's `Period`, aligned by column with the data rings; a pair's ring sized like its
  coarser end's. A memory warning above 1 GB.
- **Views in `internal/flow/frame`.** Read-only, pinned at a frame; writes stay inside each layer.
- **Scheduling and firing.** Ready-driven over the frame's active subgraph, serial first. A node
  fires only when every input is `Fresh`; a pair samples at its coarser end's period; schedules are
  precompiled per due pattern. Features complete between `Next` calls.
- **Purity.** Features keep no state; recursive ones read their own past results.
- **One lookback for every stream**, kept by `data.New`.

## Decisions taken by this plan

- **Generic rings in `flow/frame`.** `Slot[T]`, `Ring[T]` and `Buffer[T]` are concrete generic
  types and `View[T]` pins a buffer at a frame: reads are written once and called directly. Each
  layer sizes and writes its own buffers; the data layer keeps its staging, and the derived window
  count is dropped. `View[T]` embeds an unexported header, so a view cannot be re-pinned; views are made with
  `NewView` and `Pin`.
- **Feature slots carry state.** A node writes at every due column of its coordinate — `Fresh`, or
  `Gap` carrying the last `Fresh` value — and `Ended` once its stream ends; a column mismatch means
  overwritten or not started, as for candles. Replaces `{col, value}` slots and absence by column
  mismatch ([features.md](../../discussions/features.md)).
- **One typed buffer per feature variant ID**, one ring per stream (per pair for pair features),
  empty where the variant does not run.
- **Consumers read only the feature layer's view.** `Step` returns a `features.View` pinned at the
  data view's frame; `ViewOf[T](fv, id)` narrows it to a `frame.View[T]`, type-checked.
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
  `flow/frame` once stage 1 lands.

## Structure

Tiered. Stage docs live beside this one, produced iteratively:

- `stage-1-frame.md` — *implemented.*
- `stage-2-universes.md` — *to plan.*
- `stage-3-contract-store.md` — *to plan.*
- `stage-4-ported-features.md` — *to plan.*
- `stage-5-expansion.md` — *to plan.*
- `stage-6-engine.md` — *to plan.*
- `stage-7-data-logger-v2.md` — *to plan.*

Ordering: 1 and 2 are independent; 3 depends on 1; 4 on 3; 5 on 2 and 3; 6 on 1, 3 and 5; 7 on 4
and 6.

## Iteration 1 — serial feature engine

**Goal.** `data_logger_v2` runs features through the feature engine instead of the bridge: variants
from the new registry, universes from config, instances expanded at the composition root, results
in the feature store, one serial step per frame. Exports are identical to the bridge's; `data_logger`
is unchanged.

```
composition root: config (universes) ─▶ registry v2 (variants, descriptors) ─▶ Expand ─▶ instances
data_logger_v2:   features.New(instances) ─▶ loop: data.Next ─▶ pacer ─▶ features.Step(view) ─▶ exporter
                                                       │                       │
                                                       └── flow/frame View ────┘ data rings + result rings
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
- Plan: `stage-1-frame.md`.

### Stage 2 — config: universes

- **Goal:** universes are declared, validated and resolvable.
- **Touches:** `internal/config` — the `universes` block, `universe` on feature entries, the resolver
  (a pure function of rules over stream descriptors built from subscriptions and their
  derivations), the rule that each `(symbol, derived timeframe, provider)` is derived at most once;
  tests.
- **Boundary:** nothing consumes universes yet; `data_logger` ignores them.
- **Acceptance:** resolution unit-tested for stream selection, references, explicit pairs and `*`;
  filters with symbol globs; `kind` choosing between a direct and a derived match, both without it;
  derived streams matching their source's provider; duplicate pairs collapsing and self-pairs
  excluded. Path-named errors for unknown universes, malformed rules and duplicate derivations;
  `--validate-only` covers them.

### Stage 3 — contract, registry and store

- **Goal:** the new feature contract, its registry, and the feature store that binds nodes.
- **Touches:** the contract package (variant: `ID`, `Lookback`, `Bind`; node: `Compute`; the
  binder); a new registry in `internal/registry` taking base ID, descriptor and factory; the store in
  `internal/engine/features` — one `frame.Buffer[T]` per variant, binding, each node writing only its
  own ring; `features.View` and `ViewOf[T]`; tests.
- **Boundary:** no expansion or scheduling; tests bind nodes by hand.
- **Acceptance:** a bound node reads candles and its dependency's results through views pinned at
  the frame and writes its own; value types are checked at `Bind` and at `ViewOf`; a skipped column
  reads as `Gap` carrying the last value; history stepped like the data rings on direct and rolling
  streams; a pair result is one ring; no allocation on writes or reads.

### Stage 4 — ported features

- **Goal:** `sma`, `ema`, `rsi`, `vwap` and `engulfing_candle` on the new contract.
- **Touches:** a new feature package beside `internal/domains/features`; registration in the new
  registry; tests.
- **Acceptance:** each ported feature produces the original's value at every column of the BTCUSDT
  reference dataset, on direct and rolling streams, including warm-up absence; IDs match the
  originals' (`ema_20_s3`).

### Stage 5 — expansion

- **Goal:** the pure function that turns configuration into instances.
- **Touches:** `internal/engine/features` (`Expand`: variants × dependency variants × coordinates,
  dependencies pulled onto their dependents' streams, composed IDs, validation); the composition
  root (`cmd`) builds the new registry's variants beside today's features and calls `Expand`; tests
  with test-only features.
- **Boundary:** the instances are injected into `data_logger_v2` but not run yet.
- **Acceptance:** expansion over chains, several dependency types and pairs; errors for an unknown
  dependency type or one with no variant, type-level cycles, a pair feature without a universe or
  with a universe without pairs, and duplicate composed IDs; `data_logger` and its feature
  instantiation unchanged.

### Stage 6 — serial engine

- **Goal:** the feature engine runs a frame.
- **Touches:** `internal/engine/features` — `New(instances, …)`: bind, allocate the variant buffers,
  memory estimate and warning, due-pattern schedules; `Step(view)`: pick the pattern, drop nodes
  whose inputs are not `Fresh`, run the serial topological order; tests with test-only features.
- **Acceptance:** dependencies computed before their dependents; a coarse stream's nodes fire only at
  its due columns; `Gap` and `Ended` legs fire nothing, and their nodes write `Gap` / `Ended` carrying
  the last value; `Partial` frames compute their `Fresh` streams; pairs fire at their coarser end's period only when both ends
  are `Fresh`; a run equals a brute-force evaluation over the same frames; the warning fires above
  1 GB; no allocation per `Step`.

### Stage 7 — `data_logger_v2` integration

- **Goal:** `data_logger_v2` runs the feature engine; the bridge is gone.
- **Touches:** `internal/engine/data_logger_v2.go` (`features.New`, `Step` after the pacer, the
  exported record filled from the feature layer's view, `data.New` sized from the instances' longest
  `Lookback`); its tests; a serial `Step` benchmark at N = 10, 100, 1000
  streams; `docs/Commands.md`.
- **Acceptance:** on `configs/data_logger_v2.json`, records and feature values identical to the
  bridge's output; `--validate-only` valid for both configs; `make check` clean; benchmark checked
  in.

### Iteration 1 — done when

- `make check` and `go test -race` on `internal/flow/frame`, `internal/engine/data` and
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
- [data-layer-v2.md](../../discussions/data-layer-v2.md): the `View` and rings in `flow/frame`
  (stage 1); feature entries in the feature layer (stage 7).
- [pipeline-runtime.md](../../discussions/pipeline-runtime.md): Features phase implemented, serial
  (stage 6).
- [config.md](../../discussions/config.md): universes and the derivation rule implemented
  (stage 2).
- [README.md](../../discussions/README.md): features row status.
- [data layer v2 main plan](../data-layer-v2/main.md): iteration 4 points to this plan.
- `docs/Architecture.md`: `internal/flow/frame` (stage 1), `internal/engine/features` (stage 6).
- `docs/Commands.md`: benchmark commands (stage 7).

## Out of scope

- `data_logger` and everything it runs on — retiring them is a separate follow-up.
- `Late` recompute — pinned in [features.md](../../discussions/features.md).
- Strategies and their reads of features.
- Per-stream candle depth.
- Asymmetric pair order — open in [features.md](../../discussions/features.md).
- Heavy (asynchronous) features.
- The live release path — the live-mode plan, after this one.

## Open items to resolve within stages

- The contract's home: `internal/protocol` cannot import `flow/frame`, which imports it (stage 3).
- What `Compute` receives — its inputs' views, pinned by the engine, never buffers — and how a node writes `Gap`
  for a column it skips (stage 3).
- The new registry's shape and name beside `reg.Features` (stage 3).
- The ported features' package (stage 4).
- How the composition root builds the stream table `Expand` needs (stage 5).
- Filling the exported record from results — the exporter contract is untouched (stage 7).
- `FeatureChain.Evaluate` loses its only caller in stage 7: kept as the regression reference, or
  removed with the bridge (stage 7).
