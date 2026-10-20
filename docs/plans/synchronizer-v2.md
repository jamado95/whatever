# Synchronizer V2 — freshness-status dispatch on `SyncEventV2`

## Context

`internal/stream/synchronizer_v2.go` emits V1 `SyncEvent`s and dispatches only a *contiguous* run of
complete columns from a forward-only `nextCol`. Scratch runs showed three defects against its purpose:

- **Late earliest candle is lost (cases E/G).** `nextCol` is pinned at cold start; a coarse stream's
  earlier candle arriving afterwards never dispatches its columns, even when 5m data is in buffer.
- **Order-dependent output.** Same data, different arrival order → different event sets (E vs F).
- **Gaps leak bad frames (cases C/D).** `ready()` checks `firstCol ≤ owner ≤ lastCol` instead of owner
  existence → resumed streams emit stale-floor fills (C) or partial frames (D).

Goal: emit `SyncEventV2` via its constructor; dispatch **any** ready column; tag it `Current` if it is
after the latest dispatched column, `Stale` if before; emit each dispatch pass in ascending column order.
`width` is untouched (separate refactor).

## Decisions (from user)

- `SyncEventV2.Data()` = **exactly one EMD per synced stream** (forward-filled), sorted oldest→newest;
  `Time` = **newest** (= the column). Invariant: every leg closed at or before `Time`.
- Rename `LateMarketData` → `StaleMarketData`.
- Keep the `Readiness` interface; modify `orderedReadiness` in place.
- Plan stored standalone (this doc).

## Assumptions

- "Columns emitted in sequence, earliest to latest": within one dispatch pass (one arrival), columns
  are emitted ascending; across passes, order follows arrival (single goroutine).
- Status: `c > hwm` → `Current` (and `hwm = c`); `c < hwm` → `Stale`. Each column dispatches at most once.
- A column evicted from the window without dispatching is dropped (unchanged).
- `Data()` returns a clone (constructor already defensively copies).

## Changes

### 1. `internal/protocol/provider.go` (applied by the user ahead of steps 2–4)

- `LateMarketData` → `StaleMarketData`.
- `NewSyncEventV2`: `time = ordered[len(ordered)-1].Timestamp()` (newest); fix the field comments
  (`data`: one leg per synced stream, each closed at or before `time`).
- Add `func (se *SyncEventV2) Data() []ExtendedMarketData` (returns `slices.Clone(se.data)`).

### 2. `internal/stream/synchronizer_v2.go`

**State (`syncState`)**
- Re-add `floorCol map[string]int64`; drop `hasFloor` (presence = comma-ok on `floor`).
- Remove `firstCol`, `lastCol`, `allStarted()`, `startCol()` — subsumed by existence-based readiness
  (a stream with no data, or columns before its first candle, have no owner).
- Add `dispatched map[int64]bool`, pruned in `evict`.

**Owner lookup — gap-aware `at(id, c)`**
- `oc := owningCol(id, c)`; `oc ≥ baseCol` → `cell[id][oc]` must exist; else `floor[id]` must exist
  **and** `floorCol[id] == oc`. `ready(c)` = `at` ok for every sub. Fixes C/D.

**`Apply(emd) (lo, hi int64, ok bool)`** — returns the columns the datapoint can make ready:
- `col ≥ baseCol` → record in matrix as today (no `firstCol`/`lastCol` bookkeeping).
- `col < baseCol` and `col == owningCol(id, baseCol)` → late owner of the window's first columns:
  store as `floor`/`floorCol` (enables case G). Otherwise drop (`ok=false`).
- Window advance/evict unchanged, plus `delete(dispatched, c)` for evicted columns.
- Touched range: `[max(col, baseCol), min(col+period-1, maxCol)]` — only columns this candle owns
  can change readiness, so evaluation stays O(period × subs), not O(width × subs).

**`legs(c) []ExtendedMarketData`** — replaces `event(c)`: one `at(id, c)` per sub.

**`Readiness`**
- Signature → `Evaluate(st *syncState, lo, hi int64) []int64`: ready, not-yet-dispatched columns in
  `[lo, hi]`, ascending. `orderedReadiness` implements this (no contiguous-run break). Read-only.

**Runner (`runV2`) / `Sync`**
- Output → `<-chan *proto.SyncEventV2`; `Sync` signature updated accordingly (no external callers —
  `data_logger` uses V1 `Synchronize`).
- Loop: `lo, hi, ok := st.Apply(emd)`; for each `c` from `Evaluate`: compute status vs `hwm`
  (init `-1`), mark `dispatched[c]`, `proto.NewSyncEventV2(status, st.legs(c))`, send. Constructor
  error is unreachable (ready ⇒ one leg per sub) → skip defensively.
- `nextCol` removed.

V1 (`synchronizer.go`, `SyncEvent`) untouched.

### 3. Tests

`internal/stream/synchronizer_v2_test.go` — port to `*SyncEventV2` (reuse `sub`, `emd`, `ordered`,
`collect`-style helper from `synchronizer_test.go`; add a `legOf(ev, sub)` helper over `Data()`).
- `assertInvariants`: `len(Data()) == len(subs)`, one leg per sub, sorted ascending,
  `Time() == last leg ts`, every leg ts ≤ `Time()`, `Time() % stride == 0`.
- Port the 6 existing tests (expected times unchanged, all `Current`).
- New:
  - **E**: 5m 0–23, 1h@12, late 1h@0 → cols 12–23 `Current`, then 0–11 `Stale` ascending.
  - **G**: 5m 0–30, 1h@12, late 1h@0 (col 0 evicted, base 7) → 12–23 `Current`, then 7–11 `Stale`.
  - **F**: both 1h before 5m → 0–23 all `Current`.
  - **Multi-symbol / same tf**: col k+1 complete before col k → k+1 `Current`, k `Stale`.
  - **Combination**: 2 symbols × (1m, 5m) → one leg per stream, fwd-fill correct.
  - **Gaps C/D**: no events for columns whose owner is missing; D's late owner → `Stale`.
  - **Duplicate arrival** of a dispatched column → not re-emitted.

`internal/protocol/provider_test.go` (new) — `NewSyncEventV2`: empty → error; unsorted input →
sorted `Data()`, `Time()` = newest; `Data()` mutation doesn't affect the event.

### 4. Docs

- `docs/discussions/multi-subscription.md`: WIP note on the V2 buffer-matrix synchronizer
  (`SyncEventV2`, freshness status, existence-based readiness) + plan reference. Notify user.
- `docs/Commands.md`: no change.

## Verification

- `gofmt -l internal/`, `make build`, `go vet ./...`, `make test`.
- `go test ./internal/stream/ -run TestV2 -v` and `go test ./internal/protocol/ -v`.
