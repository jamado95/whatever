# Data layer live mode — main plan

## Context

Iteration 2 of [data layer v2](../data-layer-v2/main.md). Iteration 1 built the module
`internal/engine/data` and the `data_logger_v2` engine for replay. This plan adds the live release
path: per-lane deadlines, `Pending` legs, `Late` revisions and the provider drop contract. Warm-up
and backfill (iteration 3) and features on the `FrameBuffer` (iteration 4) stay in the data layer v2
plan.

No live provider exists ([data-layer.md](../../discussions/data-layer.md)), so the live path is
tested with a fake clock and simulated lanes; an end-to-end live run waits for a live provider.

This is the **main** doc of a tiered plan: it fixes the stages. Each stage's fine-grained plan is
its own doc, approved before that stage is implemented. Do not implement from this doc.

## Decisions carried in

From [data-layer-v2.md](../../discussions/data-layer-v2.md) (§ Deadline, § Alignment, § Release,
§ Consumption) and the [data layer v2 plan](../data-layer-v2/main.md):

- **Deadline stage per live lane**, between normalise and align, absent from replay lanes. The
  deadline belongs to the column, `boundary(c) + lateness`, compared with an injected `Clock`; one
  timer per lane; it never writes slots, and readiness has no clock. Lateness is hardcoded.
- **`Late`.** A candle for a column its align stage already evaluated is `Late`: forwarded, never
  written by align. It travels in its event; `Next()` writes it, recomputes the affected derived
  slots and records revision metadata. A revision becomes visible at its `Late` frame's position; no
  frame mixes old and revised values.
- **`Pending`** is a leg state; a `Pending` leg later proven missing emits no `Late`.
- **Iteration 1 stopgaps this plan replaces:** out-of-order candles dropped and logged by normalise;
  provider errors logged by the layer until the drop contract.

## Decisions taken by this plan

- **`Late` before the deadline.** `Late` is mode-independent — an out-of-order replay source yields
  it — so it is built and tested on the replay path first; the deadline stage then lands with a
  candle after its `Pending` already handled.
- **Live mode per lane follows the provider.** An optional interface on the provider marks it live;
  a live lane gets the deadline stage, and `New` takes the clock. No mode in the config.

## Structure

Tiered. Stage docs live beside this one, produced iteratively:

- `stage-1-late.md` — *planned.*
- `stage-2-derived-revisions.md` — *planned.*
- `stage-3-deadline.md` — *planned.*
- `stage-4-provider-drops.md` — *planned once the drop contract is settled in discussion.*

Ordering: 1 first; 2 and 3 depend on 1 and are independent of each other; 4 follows the
drop-contract discussion and closes the plan.

## Live release path

**Goal.** Live lanes release on deadlines: a stream still missing at `boundary + lateness` is
`Pending`. A candle for a column its align stage already evaluated — late live, or out of order in
either mode — reaches the engine as a `Late` frame that revises its base leg and the derived legs
over it. Ordered replay output is unchanged; out-of-order replay output is deterministic.

```
┌──────────────────────────────── internal/engine/data ──────────────────────────────────────┐
│ live:   provider_i ─▶ normalise ─▶ deadline ─▶ align ─evaluate / late─┐                    │
│ replay: provider_j ─▶ normalise ─────────────▶ align ─evaluate / late─┼▶ readiness ─▶ events
│                                                                       ┘  Current, Late     │
└────────────────────────────────────────────────────────────────────────────────────────────┘
data.Next(): Late → base slot, carried candles, derived legs, revision metadata
```

### Stage 1 — the `Late` path

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
- **Boundary:** derived legs over a revised column are not recomputed until stage 2.
- **Acceptance:** `t+1 → t+3 → t+2` gives `t+2` `Current` + `Partial` (`Gap`), `t+3` `Current` +
  `Complete`, then `t+2` `Late` + `Complete`; frames before a `Late` see the old data, frames after it
  the revised data; carried candles and ages correct after a revision; the same frames under every
  arrival order across lanes; a revision beyond depth dropped and logged; an out-of-order CSV fixture
  through `data_logger_v2`; `-race` clean.

### Stage 2 — derived legs under revision

- **Goal:** a `Late` frame recomputes the derived legs over its column: a revised base candle at `c`
  rewrites the derived slots `c … c + (W−1)·Period` up to the last consumed column; released columns
  not yet consumed pick it up at their own `Next`.
- **Touches:** `aggregate.go` (a window's running state — deques, compensated volume, oldest `Fresh`
  bar — rebuilt from `c − (W−1)·Period`, then slid to the consumed column, O(W + span)), `data.go`,
  `view.go` (`Revised()` names the derived legs); tests and a benchmark.
- **Acceptance:** a brute-force model over random revisions, gaps and an end; a revision at the
  window's leaving edge; derived revisions visible from the `Late` frame on, never before; a
  benchmark of one `Late` on rolling 1d over 1m.

### Stage 3 — clock, deadline and `Pending`

- **Goal:** live lanes presume a silent stream missing at `boundary(c) + lateness`; a candle after its
  `Pending` is `Late` (stage 1).
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

### Stage 4 — provider drop contract

- **Prerequisite:** the contract is settled in [data-layer-v2.md](../../discussions/data-layer-v2.md)
  (Open Questions § Provider drop contract) — buffer size per provider, whether and how drops are
  reported, whether sustained drops raise an overload signal — and the stage is planned from it.
- **Expected shape, to confirm there:** a typed drop report on the provider's error channel,
  counted and logged per lane by the layer in place of today's error logging; an overload signal
  points to the risk layer. Tested with a fake provider.
- **Acceptance:** fixed by its plan.

### Done when

- `make check` and `go test -race ./internal/engine/data/...` clean.
- Out-of-order replay sources yield deterministic `Late` frames through `data_logger_v2`; ordered
  replay frames unchanged; live deadlines, `Pending` and `Late` covered with a fake clock and
  simulated lanes.
- Discussion and reference docs updated (below).

## Targeted discussion-doc updates (as stages land)

- [data-layer-v2.md](../../discussions/data-layer-v2.md): WIP references this plan; Decisions gain
  live mode via the provider, the `Late` position in replay, carried candles after a revision,
  `Pending` and `reach`, and a lane silent from startup; the drop contract moves from Open Questions
  to Decisions once settled.
- [pipeline-runtime.md](../../discussions/pipeline-runtime.md): § Clock — an out-of-order replay
  source yields `Late`.
- [data-layer.md](../../discussions/data-layer.md): the provider drop contract.
- [README.md](../../discussions/README.md): data-layer-v2 row status.

## Out of scope

- Warm-up and backfill (iteration 3) and features on the `FrameBuffer` (iteration 4) — the
  [data layer v2 plan](../data-layer-v2/main.md).
- A live provider and an end-to-end live run.
- Per-stream and measured lateness; live reorder tolerance (open in
  [data-layer-v2.md](../../discussions/data-layer-v2.md)).

## Open items to resolve within stages

- The items under each stage's *Resolve in the plan*; the provider drop contract before stage 4.
