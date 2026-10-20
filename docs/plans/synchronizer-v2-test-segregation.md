# Synchronizer V2 — dispatch-once in runner + Synchronizer/policy test segregation

## Context

The `SynchronizerV2` doc claims "a column dispatches at most once" as a synchronizer invariant, but it
is enforced only by `orderedCurrentReadiness` filtering `st.dispatched`; a policy without the filter
would emit duplicates. Separately, V2 tests mix Synchronizer properties (completeness, legs, eviction)
with policy properties (emission order, Current/Stale), so neither can evolve independently.

Outcome: the runner owns dispatch-once; policies only pick complete columns. Tests split into
Synchronizer tests (state units + emitted column set + legs, policy-agnostic assertions) and
`orderedCurrentReadiness` tests (sequence + status), with **every existing order/status assertion
preserved** in the policy tests. State unit tests count as Synchronizer tests.

## Changes

### 1. `internal/stream/synchronizer_v2.go`

- `runV2`: `if st.dispatched[c] { continue }` before status/hwm, so duplicates within or across
  passes are skipped.
- `orderedCurrentReadiness.Evaluate`: drop the `!st.dispatched[c]` filter (returns complete columns in
  `covers`).
- Docs: `Readiness` — runner skips already-dispatched columns, so a policy need not filter them;
  `orderedCurrentReadiness` — "emits the complete ones … the runner drops any already dispatched";
  `SynchronizerV2` invariant line already accurate.

### 2. Shared fixtures — `internal/stream/synchronizer_v2_test.go`

- `type fixture struct { subs []proto.Subscription; stride int64; emds []proto.ExtendedMarketData }`.
- Package-level test subs (e.g. `btc1m, btc5m, btc1h, eth1m, eth5m, eth1h`) so expectations can name legs.
- One fixture func per scenario, inputs moved verbatim from today's tests: `fxInOrderForwardFill`,
  `fxLateBoundaryCandle`, `fxColdStart`, `fxStalledStream`, `fxDropAndDuplicate`,
  `fxLateEarliestCandle`, `fxLateFloor`, `fxEarlyCoarse`, `fxMultiSymbolOutOfOrder`,
  `fxMultiSymbolMultiTimeframe`, `fxGapAfterEviction`, `fxGapInWindow`. Each with a ≤2-line comment.
- `run(fx, rdn)` helper replacing `runOrdered` (default `orderedCurrentReadiness{}`).
- New `assertColumns(t, events, stride, want []int64)`: emitted column multiset, order/status ignored;
  fails on duplicates.
- Existing `assertInvariants`, `assertLeg`, `frames`, `assertFrames`, `legOf`, `emdRange`, `seq` kept.

### 3. Synchronizer tests — `internal/stream/synchronizer_v2_test.go`

- Unchanged: `TestV2Covers`, `TestV2ApplyDropsBelowWindow`, `TestV2EmptyInput`.
- `TestV2Scenarios` (table, `t.Run` per fixture): `assertInvariants` + `assertColumns` + legs:

  | Fixture | Columns | Legs (from today's assertions) |
  |---|---|---|
  | InOrderForwardFill | 60..62 | 1h 60..62=3600; 1m col 60=3600 (co-close) |
  | LateBoundaryCandle | 5..12 | 5m 5..9=300, 10..12=600 |
  | ColdStart | 5..8 | 5m 5..8=300 |
  | StalledStream | 5..9 | 5m 5..9=300 |
  | DropAndDuplicate | 10..14 | — |
  | LateEarliestCandle | 0..23 | 1h 0..11=0, 12..23=3600 |
  | LateFloor | 7..23 | 1h 7..11=0 |
  | EarlyCoarse | 0..23 | — |
  | MultiSymbolOutOfOrder | 1..3 | eth col 2=120 |
  | MultiSymbolMultiTimeframe | 5..11 | btc5m/eth5m 5..9=300, 10..11=600 |
  | GapAfterEviction | 12..23, 36..47 | — |
  | GapInWindow | 12..36 | 1h 24..35=7200 |

- New `TestV2DispatchOnce`: test-only `dupReadiness` returns every complete window column twice per
  arrival; on `fxLateEarliestCandle` and `fxDropAndDuplicate`, `assertColumns` passes (each column
  once).

### 4. Policy tests — `internal/stream/readiness_test.go` (new)

- `TestOrderedCurrentSequence` (table over the same fixtures): `assertFrames` with today's exact
  sequences, e.g. LateEarliestCandle → 12..23 cur, 0..11 stale; LateFloor → 12..23 cur, 7..11 stale;
  GapInWindow → 12..23 cur, 36 cur, 24..35 stale; MultiSymbolOutOfOrder → 1 cur, 3 cur, 2 stale;
  all others → their current ascending all-Current ranges.
- `TestOrderedCurrentScopeMatchesFullScan` moved here with `fullScanReadiness` and `eventKeys`;
  its inline scenarios replaced by the shared fixtures + the seeded random permutations.
  `fullScanReadiness` drops its `dispatched` filter (runner handles it).

Assertion audit: every `assertFrames` call today → policy table; every `assertLeg`/`legOf` and the
implicit column set → Synchronizer table; `assertInvariants` → Synchronizer table.

### 5. Docs

- Save this plan as `docs/plans/synchronizer-v2-test-segregation.md`.
- `docs/discussions/multi-subscription.md` (Synchronizer V2 bullet): runner enforces dispatch-once;
  add plan reference. Notify user.
- `docs/Commands.md`: no change.

## Verification

- `gofmt -l internal/`, `make build`, `go vet ./...`, `make test`.
- `go test ./internal/stream/ -run 'TestV2|TestOrderedCurrent' -v`.
- Mutation check: temporarily remove the runner's dispatch-once guard → `TestV2DispatchOnce` fails;
  restore.
