# Data layer v2 — main plan

## Context

The data layer design in [data-layer-v2.md](../../discussions/data-layer-v2.md) and the runtime
model in [pipeline-runtime.md](../../discussions/pipeline-runtime.md) are settled at concept level;
nothing is implemented. Today `data_logger` runs `provider → FeatureChain → OrderedFanIn →
SynchronizerV2 → ticker gate → exporter`, one goroutine per stage.

The axis is **release mode**. Iteration 1 builds the module `internal/engine/data` for replay —
lanes, ordered merge, staging, readiness, commit, `FrameBuffer`, rolling aggregation — and a new
engine, `data_logger_v2`, on top of it, with features through a temporary bridge. Iteration 2 adds
the live release path (clock, deadlines, `Late`). Iteration 3 adds warm-up and backfill. Iteration 4
moves features into the `FrameBuffer`; it is not part of this implementation run.

This is the **main** doc of a tiered plan: it fixes the iterations and iteration 1's stages. Each
stage's fine-grained plan is its own doc, approved before that stage is implemented. Do not
implement from this doc.

## Decisions carried in

From [data-layer-v2.md](../../discussions/data-layer-v2.md) and
[pipeline-runtime.md](../../discussions/pipeline-runtime.md):

- **Module `internal/engine/data`.** In: subscriptions and provider instances. Out: in-order frame
  events and the `FrameBuffer`. Stages: ingest → normalise → order → stage → ready → commit →
  materialise.
- **Grid and cadence.** The grid is the finest direct timeframe; direct timeframes are integer
  multiples, bars UTC-aligned (`c % P == 0`). Derivations are rolling windows declared inside the
  direct subscription's config. Derived streams update at every close of their source.
- **Release.** Strictly in column order. A column is released once every direct stream due at it is
  decided. Completeness by last due candle. Leg states `Fresh`, `Gap`, `Pending`, `Ended`.
- **Replay ordering is `OrderedFanIn` (heap) over provider outputs, ahead of staging**, with
  end-of-lane markers and final-column release. Live uses `FanIn` and a deadline.
- **Release, then materialise.** The data layer goroutine writes base candles only into rows beyond
  the released head; `data.Next()` on the engine goroutine aggregates derived legs and returns a
  pinned view.
- **History reads take their step from the stream's timeframe**, anchored at its latest update.
- **Capacity.** Depth = deepest declared read + headroom, 365-day cap; the data layer blocks when
  headroom runs out.

## Decisions taken by this plan

- **A separate engine.** `data_logger_v2` (`internal/engine/data_logger_v2.go`) is a new registered
  engine on the module. The current `data_logger`, `SynchronizerV2`, `OrderedFanIn` callers and
  `proto.SyncEventV2` stay as they are; retiring them is a follow-up once `data_logger_v2` replaces
  `data_logger`. Answers the migration-path question in
  [pipeline-runtime.md](../../discussions/pipeline-runtime.md).
- **Feature bridge.** The `Feature` contract stays for this implementation run. `data_logger_v2`
  evaluates each stream's `FeatureChain` over that stream's `View` history at its update columns,
  giving the exporter a `Snapshot` as today. Iteration 4 replaces it.
- **Replay first.** No live provider exists ([data-layer.md](../../discussions/data-layer.md)), so the
  live path follows in iteration 2, tested against a fake clock.
- **Out-of-order replay candles are dropped and logged** in iteration 1 — `Late` arrives in
  iteration 2.
- **Minimal `FrameBuffer`.** Iteration 1 holds market legs only; revision metadata arrives in
  iteration 2, feature entries in iteration 4, record sections (signals, orders, fills) with the
  strategy step loop (out of scope).

## Structure

Tiered. Stage docs live beside this one, produced iteratively:

- `stage-1-streams.md` — *implemented.*
- `stage-2-framebuffer.md`
- `stage-3-lanes.md`
- `stage-4-release.md`
- `stage-5-consumption.md`
- `stage-6-data-logger-v2.md`

Ordering: 1 first; 2 and 3 depend on 1 and are independent of each other; 4 depends on 2 and 3; 5
on 4; 6 on 5.

## Iteration 1 — replay data layer

**Goal.** `data_logger_v2` runs on `internal/engine/data` in replay: any mix of direct timeframes
and rolling derivations, released in column order with completeness and leg states, paced by the
engine, exported once per stream update. `data_logger` is unchanged.

```
            ┌──────────────────────── internal/engine/data ─────────────────────────┐
provider_i ─▶ lane_i: normalise ─┐                                                    │
provider_j ─▶ lane_j: normalise ─┼▶ OrderedFanIn (heap) ─▶ stage ─▶ ready ─▶ commit ─▶ events
            │                    ┘                 └── FrameBuffer rows beyond head ──┘   │
            └──────────────────────────────────────────────────────────────────────┬────┘
data_logger_v2 loop: pacer ─▶ data.Next() (aggregate, view) ─▶ feature bridge ─▶ exporter
```

### Stage 1 — subscription model and stream table ✅ implemented

- **Goal:** subscriptions carry their derivations, config parsing validates them, and the module gains
  the stream table it builds when instantiated.
- **Touches:** new `internal/protocol/subscription.go` (`Timeframe` and `Subscription` moved from
  `provider.go`; `Subscription.Derive` as a `TimeframeDerivationSet` bitmap) — a protocol change
  planned here; `internal/config` (validation); new `internal/engine/data/streams.go` (kind, step,
  source, window, grid stride, derived IDs); tests in `internal/protocol`, `internal/config`, `cmd`,
  `internal/engine/data`.
- **Validation** in config parsing: stream-ID deduplication (a symbol may have several direct
  timeframes on one provider); every derivation coarser than and a multiple of its source. The grid
  is computed when the table is built; multiples of it follow from the timeframe enum.
- **Derived identity** `symbol:timeframe:derived_<source ID>`, assigned by the stream table;
  strategy references stay direct-only.
- **Boundary:** nothing builds the table at run time yet; `data_logger` rejects derivations with an
  explicit error and is otherwise unchanged.
- **Acceptance:** configs with coarse direct streams and derivations validate; each rule above fails
  with a path-named error; the table's step, window and order are unit-tested; the current run is
  unchanged.

### Stage 2 — `FrameBuffer` core

- **Goal:** the stream-major ring of market legs and the pinned `View`.
- **Touches:** new `internal/engine/data/buffer.go`, `view.go`.
- **Contents:** slots by absolute column, row meta (released, complete, presence count), legs
  (candle, state, age, window count); `View.Col`, `Complete`, `Leg`, `Candles(stream, n, dst)` with
  the timeframe-derived step and anchoring; depth from declared reads in bars × step, the largest
  window `W` and headroom; a depth beyond the 365-day cap is a construction error.
- **Deferred:** revision metadata (iteration 2), feature entries and writers (iteration 4), record
  sections (out of scope).
- **Acceptance:** unit tests for ring wrap, pinning (nothing beyond `Col`), stepped reads for a direct
  coarse stream and a rolling stream, the cap error; `go test -race` clean.

### Stage 3 — lanes, normalisation, ordered merge

- **Goal:** each provider's output becomes a validated lane; lanes merge into one time-ordered
  stream.
- **Touches:** `internal/flow/pipe/fanout.go` (`OrderedFanIn` on a heap, same contract and tie rule,
  so `SynchronizerV2` keeps working), new `internal/engine/data/lane.go`, tests and a merge benchmark
  at N = 1000.
- **Normalise:** validate, map `CloseTs` to its column, scale µs timestamps, drop identical
  duplicates, reject a direct candle off its due column, drop and log non-increasing `CloseTs`.
- **End of lane:** a marker ordered at the lane's next due column, so the stream leaves the expected
  set there rather than at the end of the run.
- **Acceptance:** merge output ordered by (column, stream) across mixed timeframes; a coarse lane's
  provider blocks while its next bar waits in the merge; each normalise rule tested; existing
  `OrderedFanIn` and synchronizer tests still pass; benchmark checked in.

### Stage 4 — staging, readiness and commit

- **Goal:** the aligner turns the merged stream into in-order `Current` frame events.
- **Touches:** new `internal/engine/data/aligner.go`, `event.go`.
- **Contents:** stage base candles beyond the released head; per-column presence count against the
  streams due there; completeness by last due candle; leg states `Fresh`, `Gap`, `Ended`; the release
  loop on the data trigger, written so the iteration-2 clock trigger plugs into the same loop; final
  column released when the merge closes; blocking when headroom is exhausted.
- **Acceptance:** table tests over injected merged orders — a coarse stream between closes is not
  waited on; a missing coarse bar is `Gap` at its due column and its hour is `Partial`; a stream not
  yet started makes frames `Partial`; an ended lane leaves the expected set; a gap is proven by the next
  merged candle; the final column is released. Readiness benchmark at N = 1000.

### Stage 5 — consumption and rolling aggregation

- **Goal:** the engine-side half: `data.Next()` materialises a released column and returns its view.
- **Touches:** new `internal/engine/data/data.go` (module API: construct, `Start`, `Next`, `Close`,
  provider errors), `aggregate.go`.
- **Contents:** rolling aggregation of derived legs at their source's update columns, in O(1) per
  update (open `W` back, running volume, monotonic deques for high and low), window count below `W`
  during warm-up; marking the column consumed, which frees headroom; row ownership as decided.
- **Acceptance:** at UTC boundaries a rolling 1h from 5m equals the closed 1h bar from the CSV; a
  derivation of a coarse direct stream updates at its source's cadence; warm-up count; `-race` clean
  with the aligner staging ahead.

### Stage 6 — `data_logger_v2` engine

- **Goal:** a new engine that consumes the module and exports featured stream updates.
- **Touches:** new `internal/engine/data_logger_v2.go` (registered as `data_logger_v2`; loop: pacer →
  `Next` → feature bridge → exporter → limit) and its tests; an engine-side pacer from the existing
  `realtime` / `fixed` ticker modes, reusing the `ticker` config shape and leaving `Ticker.Gate`
  untouched; `internal/domains/features/feature.go` (evaluate a chain over a given window, beside
  `Process`); `internal/logger` (frame logging, beside `SyncEvent`); a config file exercising a coarse
  direct stream and a derivation. The default `config.json` and `data_logger` stay as they are.
- **Acceptance:** the v2 config exports one record per stream update with features; with the `fixed`
  pacer every frame is released whatever the lanes' relative speed (the original skew problem);
  `--validate-only` valid for both configs; `make check` clean.

### Iteration 1 — done when

- `make check` and `go test -race ./internal/engine/data/...` clean; merge and readiness benchmarks
  checked in.
- `data_logger_v2` runs replay on the module with mixed direct timeframes and derivations;
  `data_logger` behaves as before.
- Discussion and reference docs updated (below).

## Iteration 2 — live release path

High-level only.

- `Clock` and `Watermark` in `flow/timing`; `FanIn` arrival order; the clock trigger with one timer
  at the oldest unreleased column's boundary + lateness, at due columns only; `Pending` legs;
  hardcoded lateness.
- `Late` events: a candle for a released column travels in its event; `Next()` writes it, recomputes
  affected derived rows, records revision metadata; `View.Revised()`. The bridge reads revised
  history from then on; recomputing stored feature history waits for iteration 4.
- Replay out-of-order candles become `Late` instead of being dropped.
- Provider drop contract.
- Tested with a fake clock and simulated lanes; an end-to-end live run waits for a live provider.

## Iteration 3 — warm-up and backfill

High-level only. Depends on warm-up ownership in [data-layer.md](../../discussions/data-layer.md).

- `Backfill` status: startup history runs through the ordered merge, then hands over to arrival
  order; the pacer is bypassed; the first frame after catch-up is `Current`.
- Catch-up after reconnect or overrun.

## Iteration 4 — features on the `FrameBuffer` (not in this run)

High-level only; planned after iterations 1–3. Needs discussion first: the `Feature` contract change
in [features.md](../../discussions/features.md) and the `Layout` declaration open in
[data-layer-v2.md](../../discussions/data-layer-v2.md).

- Feature section in the buffer, `FeatureWriter` scoped to its key, `View.Feature(key, stream, n,
  dst)` at the stream's step.
- `Feature` reads the view and writes its slot; `Dependencies()` carries depth in bars; the
  `Snapshot` map and `SortedWindow` leave the data path.
- Features step only streams updated at the column; `Late` recomputes the affected range.
- The exporter reads features from the view; the bridge is deleted.

## Targeted discussion-doc updates (as stages land)

- [data-layer-v2.md](../../discussions/data-layer-v2.md): WIP references this plan; resolve stage
  boundaries (as tabulated), derivation config (stage 1) and replay lane order (dropped until
  iteration 2).
- [pipeline-runtime.md](../../discussions/pipeline-runtime.md): migration path resolved (separate
  `data_logger_v2` engine on the module, features through a bridge).
- [multi-subscription.md](../../discussions/multi-subscription.md): `data_logger_v2` runs beside
  `SynchronizerV2`; live ordering, pacing and derive-versus-subscribe point to data-layer-v2.
- [engine.md](../../discussions/engine.md): the `data_logger_v2` engine and its engine-side pacer.
- [README.md](../../discussions/README.md): data-layer-v2 row status.
- `docs/Architecture.md`: add `internal/engine/data`.
- `docs/Commands.md`: running the v2 config; `make test` package list; benchmark command.

## Out of scope

- Retiring `data_logger`, `SynchronizerV2` and `proto.SyncEventV2` — a follow-up once
  `data_logger_v2` replaces `data_logger`.
- Features in the `FrameBuffer` — iteration 4, not in this run.
- The strategy, portfolio and execution step loop and the record sections — with
  [strategy-layer.md](../../discussions/strategy-layer.md) and the `signal_logger` / `full` engines.
- Session calendars and non-UTC-aligned bars.
- Provider multiplexing (`Init([]Subscription)`).
- Sweep infrastructure: precomputed frame index, shared memory-mapped dataset, cross-run sharing.
- Per-stream lateness, heavy asynchronous features, exporter redesign.

## Open items to resolve within stages

- Headroom value and event channel capacity — stage 4.
- Derived leg state when its source has a gap inside the window — stage 5.
- Frame logging format and the v2 config file's location — stage 6.
