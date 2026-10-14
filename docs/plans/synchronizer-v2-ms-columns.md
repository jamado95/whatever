# Synchronizer V2 — millisecond, boundary-ceil column mapping

## Context

The V2 migration is blocked by two issues recorded in `docs/discussions/multi-subscription.md` (WIP).
This plan fixes **issue 1** only.

V2 maps `col = CloseTs / stride`, with `stride` taken from `TimeframeS()`. That assumes `CloseTs` is
in seconds and falls exactly on the boundary. Binance `CloseTs` is in ms and 1ms before the boundary.
Converting to ms alone is not enough: every candle shifts back by one column, so co-closing candles
still share a column, but the grid's phase moves. `owningCol` (`c - c%p`, counted from epoch column 0)
then looks 11 columns before every 1h candle, so `ready()` never holds.

**Decision:** stride in ms via a new `Timeframe.TimeframeMs()`, and `col = ceilDiv(CloseTs, strideMs)`,
i.e. the first boundary at or after the close. Exact-boundary and boundary−1ms closes map to the same
column.

**Scope:** the synchronizer fix only, plus the `TimeframeMs()` helper it uses. V1 stays as it is,
`data_logger` stays on V1, and V2 keeps its names. Issue 2 (ticker pacing), `limit`, the V1 removal
and the migration are all out of scope.

## Changes

### 1. Discussion — `docs/discussions/multi-subscription.md` (notify user)
Issue-1 updates only:
- **Decisions:** add *V2 columns are in ms (`Timeframe.TimeframeMs()`), mapped by
  `ceilDiv(CloseTs, strideMs)`.*
- **WIP, Issue 1:** reword it as a grid-phase problem (both streams shift equally; `owningCol`'s
  phase breaks), and mark it resolved by this plan.
- **Open Questions:** remove the `CloseTs` unit/convention bullet.
- **Solution, "Plans executed":** add `docs/plans/synchronizer-v2-ms-columns.md`.

### 2. `internal/protocol/provider.go`: `TimeframeMs`
- Add `func (t *Timeframe) TimeframeMs() int64 { return int64(t.TimeframeS()) * 1000 }` next to
  `TimeframeS`, with the same pointer receiver.
- `internal/protocol/provider_test.go`: a table test checking `TimeframeMs() == TimeframeS()*1000`
  for every value in `ValidTimeframes()`.

### 3. `internal/flow/align/synchronizer_v2.go`
- Add a package-level `column(ts, stride int64) int64` that returns `(ts + stride - 1) / stride`.
  Comment: first boundary at or after `ts`; non-negative `ts`.
- `newSyncState`: use `tf.TimeframeMs()` directly in place of `int64(tf.TimeframeS())`, both for
  the minimum (stride) and for `period`. Rename `secs`/`minS` to `ms`/`minMs`, and change the field
  comment to `stride int64 // ms per column`. `period` is a ratio, so its value is unchanged.
- `Apply`: `col = column(emd.Timestamp(), st.stride)`.
- `syncState` doc: state the assumptions (`CloseTs` in ms; a close at or up to one stride before a
  boundary owns that boundary's column).
- Nothing else reads `stride`. `owningCol`, `covers`, eviction and `Time()` (the raw newest
  `CloseTs`) are unchanged.

### 4. Tests: units only, plus a regression variant
These tests fail after the switch because their fixtures are in seconds. They are converted, not
weakened.

`internal/flow/align/synchronizer_v2_test.go`:
- Add `const sec = 1000` (ms per second). Fixture strides become `60*sec` / `300*sec`, and
  timestamps become `…*sec`. This covers `emd(...)` literals, `emdRange` call sites, the `legWant`
  `ts` values, and the inputs to `TestV2Covers` / `TestV2ApplyDropsBelowWindow`.
- Assert helpers (`assertInvariants`, `assertFrames`, `assertColumns`, `assertLeg`) derive a column
  with `column(e.Time(), stride)` instead of `e.Time()/stride`.
- `assertInvariants`: drop `Time % stride == 0`. `Time` is the raw close, which is boundary−1ms for
  Binance. The remaining checks (one leg per stream, sorted, `Time` = newest leg, no leg after
  `Time`) still hold.
- **Regression:** `TestV2Scenarios` runs every fixture twice, once with exact boundaries and once
  with a `binanceClose(fx)` helper that subtracts 1ms from every `CloseTs`. The expected columns are
  identical in both runs; expected leg `ts` values are shifted by −1ms in the offset run.
  Under the old floor mapping, the offset run fails (phase break).

`internal/flow/align/readiness_test.go`:
- Convert the random-order base fixture to ms (`c*300*sec`, stride `300*sec`).
- `TestOrderedCurrentSequence` also runs the `binanceClose` variant, with the same expected frames.

The shared helpers in `synchronizer_test.go` (`sub`, `emd`, `ordered`) and all V1 tests stay
untouched.

### 5. Save plan
Copy this plan to `docs/plans/synchronizer-v2-ms-columns.md`.

## Verification
- `go build ./...`, `go vet ./...`
- `go test ./internal/flow/align/ -run 'TestV2|TestOrderedCurrent' -v`: both the exact-boundary and
  the boundary−1ms variants pass.
- Sanity check: temporarily revert `Apply` to floor division and confirm the boundary−1ms variants
  fail, then restore it.
- `make test`, `make lint`, `make fmt` (expect no diff).
- No end-to-end run: `data_logger` still uses V1, so the engine is not exercised by this change.
