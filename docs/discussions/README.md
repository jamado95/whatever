# Discussions

Protocol-level discussion documents, one per component in `docs/Protocol.md`. These are the
high-level, evolving discussions that inform finer-grained topic discussions and the implementation
plans in `docs/plans/`.

Opened 2026-07-26 from a full review of the codebase at commit `fbee380`.

| Document | Component | Implementation state |
|---|---|---|
| [config.md](config.md) | Config parsing / validation | Implemented — strict decoding, validating enums, `--validate-only` |
| [data-layer.md](data-layer.md) | Data layer / providers | Working (CSV); historical provider untested against a bounded range; no live provider |
| [features.md](features.md) | Features | Working; 5 registered features; not reachable by strategies |
| [strategy-layer.md](strategy-layer.md) | Strategy layer | Nothing compiles into the default build |
| [execution-layer.md](execution-layer.md) | Execution layer | Empty stubs only |
| [monitor-layer.md](monitor-layer.md) | Monitor layer | Interface sketch only, marked `REVIEW` |
| [risk-management.md](risk-management.md) | Risk management | Stub file only |
| [engine.md](engine.md) | Engine | `data_logger` works; `signal_logger` blocked; `full` uninstantiable; `backtesting` unstarted |

## Cross-cutting themes

Three issues recur across several documents and are probably worth resolving as themes rather than
per-layer:

1. **Silent configuration failure.** ~~Untyped `map[string]any` options with discarded type
   assertions mean typos and type mismatches produce zero values instead of errors.~~ **Resolved**
   as a theme — see [[config]]. Components declare config structs decoded strictly; unknown keys,
   wrong types and invalid enum values are startup errors. The `binance_historical` time window
   ([[data-layer]]) and the ticker fallback ([[engine]]) are closed. One instance remains outside
   the config layer: `buy_the_dip` ignores its options entirely ([[strategy-layer]] defect 4),
   deferred because wiring it changes strategy behaviour.
2. **Determinism vs. liveness.** `FanOut` drops data under load and features approximate their
   recursive definitions. Both are defensible for live trading and both undermine the reproducibility
   the backtesting methodology requires. See [[engine]] and [[features]].
3. **The feature → strategy gap.** Features exist and work; strategies cannot receive them. This one
   interface change unblocks the strategy layer, the `signal_logger` engine, and everything
   downstream. See [[strategy-layer]].

## Not yet opened

- **Exporter** — implemented (`internal/domains/exporter`, JSONL) and planned under
  `docs/plans/data-viz-exporter.md`, but not a `Protocol.md` component. It is the bridge to the
  backtesting and visualisation pipeline and likely deserves its own discussion.
- **Backtesting pipeline** — methodology captured in `docs/Backtesting.md` and
  `research/Backtesting.md`; the engine-side piece is discussed under [[engine]].
