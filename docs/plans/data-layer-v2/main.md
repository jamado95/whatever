# Data layer v2 — main plan

## Context

The data layer design in [data-layer-v2.md](../../discussions/data-layer-v2.md) and the runtime
model in [pipeline-runtime.md](../../discussions/pipeline-runtime.md) are settled at concept level;
nothing is implemented. Today `data_logger` runs `provider → FeatureChain → OrderedFanIn →
SynchronizerV2 → ticker gate → exporter`, one goroutine per stage.

The axis is **release mode**. Iteration 1 builds the module `internal/engine/data` for replay —
per-stream lanes (normalise, align), readiness, commit, `FrameBuffer`, rolling aggregation — and a new
engine, `data_logger_v2`, on top of it, with features through a temporary bridge. Iteration 2 adds
the live release path (clock, deadlines, `Late`). Iteration 3 adds warm-up and backfill. Iteration 4
moves features into the `FrameBuffer`; it is not part of this implementation run.

This is the **main** doc of a tiered plan: it fixes the iterations and the stages of iterations 1
and 2. Each
stage's fine-grained plan is its own doc, approved before that stage is implemented. Do not
implement from this doc.

## Decisions carried in

From [data-layer-v2.md](../../discussions/data-layer-v2.md) and
[pipeline-runtime.md](../../discussions/pipeline-runtime.md):

- **Module `internal/engine/data`.** In: subscriptions and provider instances. Out: in-order frame
  events and the `FrameBuffer`. Stages, per direct stream: ingest → normalise → deadline (live) →
  align; then readiness (ready → commit) → materialise.
- **Grid and period.** The grid is the finest direct timeframe; direct timeframes are integer
  multiples, bars UTC-aligned (`c % P == 0`). Derivations are rolling windows declared inside the
  direct subscription's config. Derived streams update at every close of their source.
- **Release.** Strictly in column order. A column is released once every direct stream due at it is
  decided. Completeness by last due candle. Leg states `Fresh`, `Gap`, `Pending`, `Ended`.
- **No ordered merge** (redesign of 2026-10-22). Each stream's align stage stages its own slots
  within its ring's capacity; gaps are proven by the stream's own next candle; readiness tracks each
  stream's progress from its evaluates. Live adds a deadline stage per lane.
- **Release, then materialise.** The align stages write base candles only into slots beyond the
  released head; `data.Next()` on the engine goroutine aggregates derived legs and returns a pinned
  view.
- **History reads take their step from the stream's timeframe**, anchored at its latest update.
- **Capacity.** Depth = deepest declared read + headroom, 365-day cap; a stream's align stage blocks
  when its ring is full.

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
- **Pipeline construction** (decided in stages 3, 3.1 and 4, revised in 5.1 and 6). Stages follow
  `pipe.Stage`. `DataPipeline` is the data layer's pipeline: `New` instantiates a lane per direct
  stream — its provider, normaliser and aligner — readiness and the aggregator; each lane starts its
  provider, then its stages, under the layer's run context, and closes the provider when it is
  cancelled; `Start` joins the lanes' evaluates into readiness with `pipe.FanIn`. The mode is chosen
  per lane. No package-level instance and no generic pipeline tooling. The module does not use
  `SynchronizerV2`.
- **Module API** (decided in stages 5 and 5.1; `Layer` renamed `DataPipeline` in stage 6). `Event`
  folds into `View`, which carries `Status()` beside `Col()` and `Complete()`. `New` takes the
  provider map — keyed by direct subscription, providers initialised by the engine — and returns a
  `*DataPipeline`, its stages instantiated without I/O; `Start` runs it, and a provider start
  failure closes the layer; `Next(ctx)` returns `(*View, error)` — `nil, nil` at the end of the run,
  `ErrNotStarted`, `ErrClosed`, `ctx.Err()`; `Close` is synchronous: it cancels the run context,
  discards the frames not yet returned and waits for the providers it closes. Aggregation is a loop
  in `Next` on the engine goroutine. Provider errors are logged by the layer until the provider drop
  contract (iteration 2).
- **`Late` before the deadline** (iteration 2). `Late` is mode-independent — an out-of-order replay
  source yields it — so it is built and tested on the replay path first; the deadline stage then
  lands with a candle after its `Pending` already handled.
- **Live mode per lane follows the provider.** An optional interface on the provider marks it live;
  a live lane gets the deadline stage, and `New` takes the clock. No mode in the config.

## Structure

Tiered. Stage docs live beside this one, produced iteratively:

- `stage-1-streams.md` — *implemented.*
- `stage-2-framebuffer.md` — *implemented.*
- `stage-3-lanes.md` — *implemented; partly superseded by 3.1.*
- `stage-3.1-lanes.md` — *implemented.*
- `stage-4-align-readiness.md` — *implemented.*
- `stage-5-consumption.md` — *implemented; partly superseded by 5.1.*
- `stage-5.1-layer-pipeline.md` — *implemented.*
- `stage-6-data-logger-v2.md` — *implemented*; one plan in four steps: rename `Layer` →
  `DataPipeline`, engine building blocks, the engine, the v2 config.

Ordering: 1 first; 2 and 3 depend on 1 and are independent of each other; 3.1 on 3; 4 depends on 2
and 3.1; 5 on 4; 5.1 on 5; 6 on 5.1.

Iteration 2:

- `stage-7-late.md` — *planned.*
- `stage-8-derived-revisions.md` — *planned.*
- `stage-9-deadline.md` — *planned.*
- `stage-10-provider-drops.md` — *planned once the drop contract is settled in discussion.*

Ordering: 7 first; 8 and 9 depend on 7 and are independent of each other; 10 follows the
drop-contract discussion and closes iteration 2.

## Iteration 1 — replay data layer

**Goal.** `data_logger_v2` runs on `internal/engine/data` in replay: any mix of direct timeframes
and rolling derivations, released in column order with completeness and leg states, paced by the
engine, exported once per stream update. `data_logger` is unchanged.

```
┌──────────────────────────────── internal/engine/data ─────────────────────────────────┐
│ lane_i: provider_i ─▶ normalise ─▶ align ─evaluate─┐                                  │
│ lane_j: provider_j ─▶ normalise ─▶ align ─evaluate─┼▶ readiness: ready ─▶ commit ─▶ events
│                                      │             ┘                                  │
│                                      └── FrameBuffer slots beyond the released head   │
└───────────────────────────────────────────────────────────────────────────────────────┘
data_logger_v2 loop: data.Next() (aggregate, view) ─▶ pacer ─▶ feature bridge ─▶ exporter
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

### Stage 2 — `FrameBuffer` core ✅ implemented

- **Goal:** the market legs of every stream and the pinned `View`.
- **Touches:** `internal/engine/data/streams.go` (`Period` and `Window` replace `Step`), new
  `buffer.go`, `view.go` and their tests.
- **Contents:** one ring per stream, one slot per update (`slot = (col / Period) % size`), each sized
  from `lookback`, the largest derived window and headroom, the 365-day cap per stream; slots hold
  the candle, state (`Fresh`, `Gap`, `Ended`), last `Fresh` column and window count; one write path,
  `put`, ownership by convention. `View` (a struct): `Col`, `Boundary`, `Complete`, `Leg`, `Count`,
  `Candles(stream, n, dst)` — newest first, one bar per step, gaps skipped. No per-column storage.
- **Deferred:** revision metadata (iteration 2), feature entries and writers (iteration 4), record
  sections (out of scope).
- **Acceptance:** construction and sizing, the write path, `Leg` / `Count` / `Candles` scenarios, no
  allocations on reads, a writer-ahead concurrency test and a reference-model test; `go test -race`
  clean.
- Plan: `stage-2-framebuffer.md`.

### Stage 3 — lanes, normalisation, ordered merge ✅ implemented

- **Goal:** each provider's output becomes a validated lane; lanes merge into one time-ordered
  stream; the pipeline's stage convention and composer are fixed.
- **Touches:** `internal/flow/pipe/pipeline.go` (`Stage`, `Merge`), `internal/flow/pipe/fanout.go`
  (`OrderedFanIn` on `container/heap`, same contract and tie rule, so `SynchronizerV2` keeps
  working), `internal/domains/provider/binance_csv.go` (µs → ms), `internal/engine/data/streams.go`
  (`column`), new `lane.go`, `order.go`, `pipeline.go` (the unexported composer), tests and a merge
  benchmark at N = 10, 100, 1000.
- **Normalise:** one lane per direct stream; rules in order — validate (ms timestamps, OHLCV, span
  within `[timeframe − 1 s, timeframe]`), column by ceilDiv, due column, identical duplicate, column
  beyond the last accepted. Drops logged once per rule per lane, then summarised.
- **End of lane:** a marker ordered at the lane's next due column, so the stream leaves the expected
  set there rather than at the end of the run; −1 for a lane that accepted no candle.
- **Acceptance:** merge output ordered by (column, stream) across mixed timeframes; a coarse lane's
  provider blocks while its next bar waits in the merge; each normalise rule tested; existing
  `OrderedFanIn` and synchronizer tests still pass; benchmark checked in.
- Plan: `stage-3-lanes.md`. The ordered merge, the end marker and the merged-order tests are
  superseded by stage 3.1.

### Stage 3.1 — per-stream lanes ✅ implemented

- **Goal:** adjust stage 3 to the 2026-10-22 redesign: no ordered merge; each direct stream gets a
  lane of per-stream stages, normalise only until stage 4.
- **Touches:** `lane.go` → `normalise.go` (`lane` → `normaliser`; `laneItem` holds column and
  candle; no end marker — a lane ends when its channel closes), `order.go` deleted, `pipeline.go`
  (`run` returns one output per lane), `internal/flow/pipe/pipeline.go` (`Merge` removed), tests.
- **Acceptance:** each lane carries exactly its own stream's accepted candles, in order, then closes;
  an unread lane stops its provider at `laneBuffer + 1` while the others run to the end; nothing
  stage 3 introduced is left without a caller.
- Plan: `stage-3.1-lanes.md`.

### Stage 4 — align and readiness ✅ implemented

- **Goal:** per-stream align stages and one readiness stage turn the lanes into in-order `Current`
  frame events.
- **Touches:** new `internal/engine/data/align.go` (gate, align stage), `readiness.go`, `event.go`;
  `buffer.go` / `view.go` (each ring's first column, `defaultHeadroom`); `pipeline.go` chains align
  onto each lane and joins the lanes' evaluates with `pipe.FanIn` into readiness; tests and a
  readiness benchmark.
- **Contents:** align stages `Fresh`, `Gap` and `Ended` slots, taking the carried candle from its own
  ring's previous slot, reporting before blocking and sending an evaluate per resolved column. It
  waits for ring capacity measured from the oldest readable bar — the whole ring at startup, then up
  to `consumed + spare·Period` — on a gate holding the engine's consumed column; reads stop at a
  stream's first column. Readiness keeps each stream's progress (`through`), checks the next column
  on each evaluate resuming at the blocking stream, caps release at `reach` (the latest `Fresh` or
  `Gap` column) and reads completeness from the `View`. `defaultHeadroom` is 128 columns; the event
  channel holds as many.
- **Acceptance:** table tests over evaluates staged by real align stages, the same frames under 23
  arrival orders; a coarse stream between closes is not waited on; a missing coarse bar is `Gap` and
  its hour `Partial`; streams not yet started make frames `Partial`; ended lanes leave the expected
  set; a gap is proven by the stream's own next candle; the run ends at the latest `Fresh` column;
  per-stream blocking without deadlock under skew; readiness benchmark at N = 10, 100, 1000.
- Plan: `stage-4-align-readiness.md`.

### Stage 5 — consumption and rolling aggregation ✅ implemented

- **Goal:** the engine-side half: `data.Next()` materialises a released column and returns its view.
- **Touches:** new `internal/engine/data/data.go` (module API: `Layer`, the exported `data.New`
  constructor around the composer, `Start`, `Next`, `Close`, provider errors; no package-level
  instance), `aggregate.go`; `view.go`, `event.go`, `readiness.go`, `pipeline.go` (`Event` folded
  into `View`), `align.go` (gate `stop`); tests and an aggregation benchmark.
- **Contents:** rolling aggregation of derived legs at their source's update columns, in O(1) per
  update — the bar at the column enters, the bar `W` back leaves; open from the oldest `Fresh` bar,
  compensated running volume, monotonic deques for high and low — in a loop in `Next` before it
  consumes the column (the gate's `consume`), which frees ring capacity. Derived legs take their
  source's state; `Gap` and `Ended` carry the latest `Fresh` derived candle and its count; count is
  the `Fresh` bars in the window, below `W` during warm-up. `Close` closes providers, stops the gate
  (waiting align stages give up and drain) and drains the frames. Derived slots are written only by
  `Next`, on the engine goroutine.
- **Acceptance:** rolling 1h and 1d from 5m equal the direct 1h and 1d bars of the BTCUSDT reference
  dataset over a random four-month sample; a derivation of a coarse direct stream updates at its
  source's period; warm-up count; a brute-force model over gaps and an end; `Next` and `Close`
  states; `-race` clean with the align stages staging ahead.
- Plan: `stage-5-consumption.md`. Partly superseded by stage 5.1 (the composer, `New`'s arguments,
  provider lifecycle, start failures).

### Stage 5.1 — the layer is the pipeline ✅ implemented

- **Goal:** the data layer is the pipeline: each direct stream's lane owns its provider and stages.
- **Touches:** new `internal/engine/data/lane.go`, `pipeline.go` deleted, `data.go`; tests
  (`pipeline_test.go` → `lane_test.go`).
- **Contents:** the `Layer` holds `lanes`, `ready` and `agg`; `New(providers, lookback, log)` builds
  the stream table from the map's keys — a nil provider or one instance under two keys is an error —
  and a lane per direct stream from its initialised provider; `lane.run(ctx)` starts the provider,
  then normalise and align, and closes the provider through `context.AfterFunc` once the layer's run
  context is cancelled; `Start` joins the lanes into readiness, and a provider start failure closes
  the layer; `Close` cancels the run context, stops the gate, drains and waits for the providers to
  close. The composer, its stage factories and `Init` in the layer are gone.
- **Acceptance:** frames unchanged; each lane carries only its own stream's evaluates; an unread lane
  stops only its own provider; providers closed exactly once by `Close`, also after a run that ended
  on its own; start failures as above.
- Plan: `stage-5.1-layer-pipeline.md`.

### Stage 6 — `data_logger_v2` engine ✅ implemented

- **Goal:** a new engine that consumes the module and exports featured stream updates.
- **Touches:** `internal/engine/data` (`Layer` renamed `DataPipeline`); new
  `internal/engine/data_logger_v2.go` (registered as `data_logger_v2`; loop: `Next` → pacer →
  feature bridge → exporter → limit) and its tests; an engine-side pacer in `flow/timing` from the
  existing `realtime` / `fixed` ticker modes, reusing the `ticker` config shape and leaving
  `Ticker.Gate` untouched; `internal/domains/features/feature.go` (evaluate a chain over a given
  window, beside `Process`); `internal/logger` (frame logging, beside `SyncEvent`);
  `configs/data_logger_v2.json`, exercising a coarse direct stream and a derivation. The default
  `config.json` and `data_logger` stay as they are.
  `data_logger_v2` initialises each provider with its subscription, then passes its `_providers`
  map to `data.New`; an `Init` failure is the engine's error.
- **Contents:** one plan in four steps, each reviewed and committed before the next: (1) rename
  `Layer` → `DataPipeline`; (2) engine building blocks — feature bridge, pacer, frame logger; (3) the
  engine; (4) the v2 config and closing iteration 1.
- **Acceptance:** the v2 config exports one record per stream update with features; with the `fixed`
  pacer every frame is released whatever the lanes' relative speed (the original skew problem);
  `--validate-only` valid for both configs; `make check` clean.
- Plan: `stage-6-data-logger-v2.md`.

### Iteration 1 — done when

- `make check` and `go test -race ./internal/engine/data/...` clean; the `OrderedFanIn` and
  readiness benchmarks checked in.
- `data_logger_v2` runs replay on the module with mixed direct timeframes and derivations;
  `data_logger` behaves as before.
- Discussion and reference docs updated (below).

## Iteration 2 — live release path

**Goal.** Live lanes release on deadlines: a stream still missing at `boundary + lateness` is
`Pending`. A candle for a column its align stage already evaluated — late live, or out of order in
either mode — reaches the engine as a `Late` frame that revises its base leg and the derived legs
over it. Ordered replay output is unchanged; out-of-order replay output is deterministic. Tested with
a fake clock and simulated lanes; an end-to-end live run waits for a live provider.

```
┌──────────────────────────────── internal/engine/data ──────────────────────────────────────┐
│ live:   provider_i ─▶ normalise ─▶ deadline ─▶ align ─evaluate / late─┐                    │
│ replay: provider_j ─▶ normalise ─────────────▶ align ─evaluate / late─┼▶ readiness ─▶ events
│                                                                       ┘  Current, Late     │
└────────────────────────────────────────────────────────────────────────────────────────────┘
data.Next(): Late → base slot, carried candles, derived legs, revision metadata
```

### Stage 7 — the `Late` path

- **Goal:** a candle for a column its align stage already evaluated reaches the engine as a `Late`
  frame and revises the stream's base leg. Mode-independent: an out-of-order replay source exercises
  it.
- **Touches:** `normalise.go` (out-of-order candles forwarded as late items instead of dropped),
  `align.go` (late candles forwarded, never written; the carried candle kept in align's own state,
  since the engine now writes released slots), `event.go` (`Late`; the evaluate carries the late
  candle), `readiness.go` (a `Late` held until its column is released, emitted after it), `view.go`
  (revised legs, `Revised()`), `buffer.go` (revision flag per slot), `data.go` (`Next` writes the
  revised base slot), `internal/engine/data_logger_v2.go` and frame logging; tests.
- **Contents:** the feature bridge reads revised history from the `Late` frame on; recomputing
  stored feature history waits for iteration 4.
- **Resolve in the plan:** the `Late` frame's position in replay — emitted on arrival it depends on
  lane scheduling, so it is anchored to the stream's own progress or equivalent; carried candles and
  ages after a revision — `Gap` slots after the column carry the old candle, released columns not yet
  consumed included (fixed at their own `Next`, as aggregation is); a revision older than ring depth
  is dropped and logged; an identical re-delivery changes nothing; what `data_logger_v2` exports for
  a `Late` frame.
- **Boundary:** derived legs over a revised column are not recomputed until stage 8.
- **Acceptance:** `t+1 → t+3 → t+2` gives `t+2` `Current` + `Partial` (`Gap`), `t+3` `Current` +
  `Complete`, then `t+2` `Late` + `Complete`; frames before a `Late` see the old data, frames after it
  the revised data; carried candles and ages correct after a revision; the same frames under every
  arrival order across lanes; a revision beyond depth dropped and logged; an out-of-order CSV fixture
  through `data_logger_v2`; `-race` clean.

### Stage 8 — derived legs under revision

- **Goal:** a `Late` frame recomputes the derived legs over its column: a revised base candle at `c`
  rewrites the derived slots `c … c + (W−1)·Period` up to the last consumed column; released columns
  not yet consumed pick it up at their own `Next`.
- **Touches:** `aggregate.go` (a window's running state — deques, compensated volume, oldest `Fresh`
  bar — rebuilt from `c − (W−1)·Period`, then slid to the consumed column, O(W + span)), `data.go`,
  `view.go` (`Revised()` names the derived legs); tests and a benchmark.
- **Acceptance:** a brute-force model over random revisions, gaps and an end; a revision at the
  window's leaving edge; derived revisions visible from the `Late` frame on, never before; a
  benchmark of one `Late` on rolling 1d over 1m.

### Stage 9 — clock, deadline and `Pending`

- **Goal:** live lanes presume a silent stream missing at `boundary(c) + lateness`; a candle after its
  `Pending` is `Late` (stage 7).
- **Touches:** `flow/timing` (`Clock` with `Now` and `Wake`; system and fake clocks),
  `internal/protocol/provider.go` (optional interface marking a provider live), new
  `internal/engine/data/deadline.go`, `normalise.go` (lane item carries a marker), `align.go`
  (`Pending` slot and evaluate), `buffer.go` / `view.go` (`Pending` leg state, carrying the last
  `Fresh` candle), `readiness.go` (`Pending` decides its column; the frame is `Partial`),
  `aggregate.go` (derived legs take `Pending` from their source), `lane.go` / `data.go` (the deadline
  stage on live lanes; `New` takes the clock; hardcoded lateness); tests.
- **Contents:** one timer per lane at its next due column; on firing the stage reads a candle already
  waiting on its input before sending a `Pending` marker; a candle beyond the due column is a gap
  proof and moves the timer. Proofs are trusted at once and `Late` repairs a wrong one; reorder
  tolerance stays open.
- **Resolve in the plan:** whether `Pending` extends `reach` — otherwise a column where every due
  stream is silent is never released; a lane silent from startup — its first evaluate is `Pending`,
  while readiness's first report assumes `Fresh` or `Ended`; several deadlines passing at once
  (engine stall, clock jump); the live interface's name and how `New` takes the clock.
- **Acceptance:** with a fake clock and simulated lanes — a column with every due stream present
  goes out before its deadline; a silent stream gives `Current` + `Partial` (`Pending`), then `Late`
  on arrival; a `Pending` later proven a gap emits no `Late`; a clock jump releases the passed
  deadlines in column order; silence from startup; replay lanes have no deadline stage and their
  frames are unchanged; `-race` clean.

### Stage 10 — provider drop contract

- **Prerequisite:** the contract is settled in [data-layer-v2.md](../../discussions/data-layer-v2.md)
  (Open Questions § Provider drop contract) — buffer size per provider, whether and how drops are
  reported, whether sustained drops raise an overload signal — and the stage is planned from it.
- **Expected shape, to confirm there:** a typed drop report on the provider's error channel,
  counted and logged per lane by the layer in place of today's error logging; an overload signal
  points to the risk layer. Tested with a fake provider.
- **Acceptance:** fixed by its plan.

### Iteration 2 — done when

- `make check` and `go test -race ./internal/engine/data/...` clean.
- Out-of-order replay sources yield deterministic `Late` frames through `data_logger_v2`; ordered
  replay frames unchanged; live deadlines, `Pending` and `Late` covered with a fake clock and
  simulated lanes.
- Discussion and reference docs updated (below).

## Iteration 3 — warm-up and backfill

High-level only. Depends on warm-up ownership in [data-layer.md](../../discussions/data-layer.md).

- `Backfill` status: startup history is bounded by per-stream ring capacity, with providers blocking
  rather than dropping while they deliver it; the pacer is bypassed; the first frame after catch-up
  is `Current`.
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

Iteration 2:

- [data-layer-v2.md](../../discussions/data-layer-v2.md): WIP references stages 7–10; Decisions gain
  live mode via the provider, the `Late` position in replay, carried candles after a revision,
  `Pending` and `reach`, and a lane silent from startup; the drop contract moves from Open Questions
  to Decisions once settled.
- [pipeline-runtime.md](../../discussions/pipeline-runtime.md): § Clock — an out-of-order replay
  source yields `Late`.
- [data-layer.md](../../discussions/data-layer.md): the provider drop contract.

## Out of scope

- Retiring `data_logger`, `SynchronizerV2` and `proto.SyncEventV2` — a follow-up once
  `data_logger_v2` replaces `data_logger`.
- One provider instance per subscription at the composition root — until then `data.New` rejects a
  shared instance.
- Features in the `FrameBuffer` — iteration 4, not in this run.
- The strategy, portfolio and execution step loop and the record sections — with
  [strategy-layer.md](../../discussions/strategy-layer.md) and the `signal_logger` / `full` engines.
- Session calendars and non-UTC-aligned bars.
- Provider multiplexing (`Init([]Subscription)`).
- Sweep infrastructure: precomputed frame index, shared memory-mapped dataset, cross-run sharing.
- Per-stream lateness, heavy asynchronous features, exporter redesign.

## Open items to resolve within stages

- Headroom value, event channel capacity, evaluate delivery and the wake-up — resolved in stage 4.
- Stopping align stages waiting on the gate (`Close`) and the derived leg state under source gaps —
  resolved in stage 5.
- Frame logging format and the v2 config file's location — resolved in stage 6.
- Iteration 2: the items under each stage's *Resolve in the plan*; the provider drop contract
  before stage 10.
