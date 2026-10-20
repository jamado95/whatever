# Synchronizer V2 — amendment: Readiness owns the evaluation scope

Amends `docs/plans/synchronizer-v2.md` (§2 **`Apply`** and **`Readiness`**). That plan stays as executed;
this doc supersedes only the `Apply` return value and the `Evaluate` contract.

## Context

`Apply` returns `[lo, hi]` — the columns the datapoint can make ready — and `Evaluate` checks only that
range. Choosing which columns to evaluate is a dispatch-policy concern, but it is decided inside the
state mutation, so every policy inherits the ordered policy's scope. Policies with a different scope
(e.g. a clock-driven policy that only checks the newest columns) cannot opt out of it.

Goal: `Apply` reports only what landed; Readiness decides which columns to evaluate, using read-only
state queries. No behaviour change.

## Decisions

- `Apply(emd) (id string, col int64, ok bool)` — the cell written (matrix or floor); `ok=false` when dropped.
- New geometry query `covers(id, col) (lo, hi int64, ok bool)` on `syncState`: the in-window columns
  forward-filled by the stream's datapoint at `col` (today's `lo/hi` arithmetic, moved out of `Apply`).
- `Readiness.Evaluate(st *syncState, id string, col int64) []int64` — called per accepted datapoint;
  returns columns to dispatch, in dispatch order. Read-only. A policy may query any state (`ready`,
  `covers`, `dispatched`, `baseCol`, `maxCol`) and may ignore `id`/`col` entirely.
- Unchanged: completeness (`ready(c)`) and `dispatched` stay in `syncState`; the runner still marks
  `dispatched` and computes `Current`/`Stale`; trigger stays arrival-driven.

## Assumptions

- `covers` is evaluated after `Apply`'s window advance/evict, as `lo/hi` is today.
- A late floor (`col < baseCol`) clamps to `lo = baseCol`; `ok` is always true for it, since
  `col == owningCol(id, baseCol)` implies `col + period - 1 ≥ baseCol`.

## Changes

### 1. `internal/stream/synchronizer_v2.go`

- `Apply`: return `id, col, true` for matrix and floor writes, `"", 0, false` for drops. Drop the
  `lo/hi` computation; update doc.
- Add `covers(id, col)`: `lo = max(col, baseCol)`, `hi = min(col+period[id]-1, maxCol)`, `ok = lo ≤ hi`.
- `Readiness` interface + doc: new signature and contract (above).
- `orderedReadiness.Evaluate`: `lo, hi, ok := st.covers(id, col)`; if `!ok` return nil; else unchanged
  loop (`!dispatched && ready`).
- `runV2`: `id, col, ok := st.Apply(emd)`; `rdn.Evaluate(st, id, col)`.

### 2. `internal/stream/synchronizer_v2_test.go`

- Existing `TestV2*` tests unchanged (behaviour-preserving).
- `TestV2Covers`: in-window candle (full period), clamp to `maxCol`, late floor clamped to `baseCol`,
  datapoint below window → `!ok`.
- `TestV2ApplyDropsBelowWindow`: datapoint older than `owningCol(id, baseCol)` → `ok=false`.
- `TestV2ScopeMatchesFullScan`: test-only `fullScanReadiness` (ignores `id`/`col`, scans
  `[baseCol, maxCol]`) produces the same events as `orderedReadiness` over the existing scenarios'
  inputs plus seeded random permutations (fixed seed) of a 2-symbol × (5m, 1h) dataset with gaps,
  spanning > `width`. Shows that a policy can choose its own scope, and that the ordered scope misses
  nothing.

### 3. Docs

- `docs/discussions/multi-subscription.md` (Synchronizer V2 WIP bullet): one line — `Apply` reports the
  written cell; Readiness chooses its evaluation scope via state queries. Add this plan to the plan
  references. Notify user.
- `docs/Commands.md`: no change.

## Out of scope

- Clock-driven trigger (`OnTick`), hard-clock or contiguous policies.
- Moving `dispatched` or status computation out of state/runner.
- `width` policy; V2 wiring into `data_logger`.

## Verification

- `gofmt -l internal/`, `make build`, `go vet ./...`, `make test`.
- `go test ./internal/stream/ -run TestV2 -v`.
