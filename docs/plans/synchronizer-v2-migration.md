# Synchronizer V2 migration — ordered V2 wired, V1 removed

## Context

`docs/discussions/multi-subscription.md` (Decisions, WIP) records V2 as *the* synchronizer, with the
migration pending: both blockers are resolved (ms columns — `synchronizer-v2-ms-columns.md`; lane skew —
`synchronizer-v2-ordered.md`), and the WIP says the migration should use `NewOrderedSynchronizer`.
`data_logger` still runs V1 (`align.Synchronize` → `proto.SyncEvent`). This plan wires the ordered V2
into `data_logger`, moves every `SyncEvent` consumer to `*SyncEventV2`, and deletes V1 with its tests.

Baseline (verified): `go build ./...`, `go vet ./...`, `go test ./...` green. Tagged builds
(`signalengine`, `fullengine`, `wip`) already fail to compile for unrelated reasons (they pass
`<-chan MarketData` to `Strategy.Init`; `swing_hl.go` errors) — left as is.

## Assumptions / decisions

- **V2 names stay** (`SynchronizerV2`, `SyncEventV2`, `NewSynchronizerV2`, `runV2`, `synchronizer_v2*.go`),
  per the documented migration scope; renaming would rewrite the V2 tests the user wants unchanged.
- **`NewSynchronizerV2` (FanIn) stays**: it is V2, not V1, and existing tests use it.
- **Export rule:** `data_logger` exports legs with `CloseTs == ev.Time()` (WIP-recorded) → one record
  per candle. Behaviour change vs V1, inherent to V2: candles in columns that never complete (cold
  start before every stream has an owning candle; data after one lane ends) are not exported.
- Ordered merge is used unconditionally — both existing providers (`binance_csv`, `binance_historical`)
  are replay.
- Deleted tests: all V1 tests in `synchronizer_test.go` (`TestSynchronize*`,
  `TestSubscriptionIDKeyDistinction`, `TestNowFwdFillIsSnapshot`). Every other test is kept verbatim.

## Changes

### 1. `internal/flow/align`
- Delete `synchronizer.go` (`Synchronize`, `synchronize`, `makeEvent`).
- `synchronizer_v2.go`: package doc "…into SyncEvents." → "…into SyncEventV2 frames." No other change.
- Delete `synchronizer_test.go`; move `sub`, `emd`, `ordered` and `TestHelperShapes` verbatim to the
  top of `synchronizer_v2_test.go` (add `"strings"` import). Reword `sub`'s comment
  ("so NowFwdFill keys are distinct" → "so subscription IDs are distinct"). Drop V1-only helpers
  `collect`, `nowHas`, `fwdFillAt`, `eventTimes`. `readiness_test.go` untouched.

### 2. `internal/protocol`
- `provider.go`: delete `SyncEvent` and its comment. `SyncEventV2` untouched.
- `strategy.go`: `Init(data <-chan *SyncEventV2) error`.

### 3. `internal/logger/logger.go`
`SyncEvent(ev *proto.SyncEventV2)`: log `time` (`ev.Time()`), `status` (`int(ev.Status())`),
`now_count` / `subscriptions` over legs with `Timestamp() == ev.Time()` (V1 `Now` equivalent),
`leg_count` = `len(ev.Data())`. Replaces `fwd_fill_count`.

### 4. `internal/engine/data_logger.go`
- `syncOut := align.NewOrderedSynchronizer(nil).Sync(streams)`.
- Drain loop: `for _, emd := range ev.Data()`, `continue` when `emd.Timestamp() != ev.Time()`
  (forward-filled leg), otherwise export as today. Update the drain comment (`SyncEvent`/`Now` wording).

### 5. Docs (notify user)
- `docs/discussions/multi-subscription.md`:
  - Topic intro: `data_logger` runs N subscriptions through the ordered V2; V1 removed.
  - State-of-the-code: `data_logger` row (ordered V2, exports legs closing at `Time()`), `align` row
    (V2 only; `NewOrderedSynchronizer` wired, `NewSynchronizerV2` unwired), `Strategy.Init` row
    (`<-chan *SyncEventV2`).
  - Decisions: V2 bullet → migration done, plus the export rule and its cold-start/lane-end caveat.
  - §4 table: "(V1, superseded)" → "(V1, removed)". §5: drop "target of the migration".
  - Plans executed: add `docs/plans/synchronizer-v2-migration.md`.
  - WIP: replace the migration paragraph with a completion note; condense Issues 1/2 to one line each
    pointing at their plans (rationale already in Decisions).
  - Resources: drop `synchronizer.go (V1, to be removed)` and `SyncEvent`.
- `docs/discussions/strategy-layer.md`: input contract `SyncEvent` → `*SyncEventV2`; `SyncEvent.Now` /
  `NowFwdFill` → `SyncEventV2.Data()` (one forward-filled leg per stream); fix "code still shows
  `MarketData`" (code shows `*SyncEventV2`; `buy_the_dip` still takes `MarketData`).
- `WIP.md`: remove the "migrate to synchronizer_v2 … deprecate synchronizer" ToDo.
- No change: `docs/Commands.md` (no command changes), `docs/Architecture.md`, historical `docs/plans/*`.

### 6. Save plan
Copy this plan to `docs/plans/synchronizer-v2-migration.md`.

## Verification
- Before edits: `go test -list '.*' ./... > <scratch>/before.txt`.
- `go build ./...`, `go vet ./...`, `make lint`, `make fmt` (expect no diff), `make test`,
  `go test -race ./internal/flow/...`.
- Tests unchanged: `go test -list` after vs before differs only by the deleted V1 tests;
  `git diff internal/flow/align/readiness_test.go` empty; `synchronizer_v2_test.go` diff is only the
  moved helpers + import.
- No V1 left: `grep -rnE 'proto\.SyncEvent\b|chan SyncEvent\b|Synchronize\(|synchronize\(|NowFwdFill|arrival-order' --include='*.go' .`
  → no hits.
- End-to-end (no workspace side effects): scratch copy of `config.json` with exporter `output_dir` in
  the scratchpad and `limit: 300`; `make validate CONFIG=…`, then `make run CONFIG=…`. Expect a
  non-zero "data logger stream complete" count, `SyncEvent` logs with status 0 and 1h legs on hour
  boundaries, and a JSONL export containing both 1h and 5m records with no duplicate
  `(subscription, close_ts)`.
