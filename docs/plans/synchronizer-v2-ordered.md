# Ordered synchronizer — `pipe.OrderedFanIn` composed with Synchronizer V2

## Context

The V2 migration is blocked by issue 2 in `docs/discussions/multi-subscription.md` (WIP): lanes drift
apart in data time, and V2's window (2× the coarsest period) then drops the lagging lane.
`SynchronizerV2.Sync` merges lanes with `pipe.FanIn`, which passes values on in arrival order. Issue 1
(ms columns) is resolved (`docs/plans/synchronizer-v2-ms-columns.md`, commit `1c9e6ca`).

**Decision:** add an ordered synchronizer that replaces `FanIn` with a new `pipe.OrderedFanIn`, a
k-way merge by `CloseTs`. It releases the lowest pending value across open lanes, so V2 receives data
in timestamp order and lanes running ahead wait upstream through backpressure. Given ordered input,
columns complete in column order: every frame is `Current` and the output is reproducible.

It works with both ticker types in replay:
- **`realtime`:** the merge waits on whichever lane is behind.
- **Per-lane `fixedInterval`:** a lane that runs ahead blocks on send. Its ticks are skipped but its
  values are kept, so the finest lane's ticker sets the pace.

The constraint is the data source, not the ticker. The merge blocks while any open lane has no pending
value, so it is for replay only; a live feed's coarse lane would hold finer data until its next close.

**Scope:** `OrderedFanIn`, the ordered synchronizer constructor, tests, and the docs. Out of scope:
wiring it into `data_logger`, removing V1, moving the ticker after the synchronizer, and choosing
between replay and live. The V2 names stay as they are, and existing tests are unchanged.

## Changes

### 1. `internal/flow/pipe/fanout.go`: `OrderedFanIn`
```go
// OrderedFanIn merges inputs that are each already ordered by less into one
// ordered output: it holds one pending value per open input and emits the
// least, ties going to the earlier input. Blocks while any open input has no
// pending value, so a silent input stalls the merge. Output closes when all
// inputs close.
func OrderedFanIn[T any](less func(a, b T) bool, ins ...<-chan T) <-chan T
```
- One goroutine runs the loop. It keeps `heads []T`, `has []bool` and `open []bool`, and repeats:
  1. Receive (blocking) on every open input that has no pending value. Mark an input closed when it
     reports `!ok`.
  2. If no input has a pending value, return, which closes the output.
  3. Find the minimum, scanning in input order with `less(heads[i], heads[min])`, so ties go to the
     earlier input.
  4. Send it and clear that slot.
- No goroutine per input and no `ctx`, matching `FanIn`. Cancellation comes from upstream closing the
  inputs.
- No protocol dependency: `pipe` stays generic.

### 2. `internal/flow/align/synchronizer_v2.go`: ordered instance
- Add a field to `SynchronizerV2`:
  `merge func(...<-chan proto.ExtendedMarketData) <-chan proto.ExtendedMarketData`.
  `NewSynchronizerV2` sets it to `pipe.FanIn[proto.ExtendedMarketData]`, so behaviour is unchanged.
- Add a constructor:
  ```go
  // NewOrderedSynchronizer builds a SynchronizerV2 that merges lanes in CloseTs
  // order (pipe.OrderedFanIn) instead of arrival order, so lane skew never reaches
  // the window. Replay only: the merge waits while any open lane has no pending
  // datapoint.
  func NewOrderedSynchronizer(rdn Readiness) *SynchronizerV2
  ```
  It reuses `NewSynchronizerV2(rdn)` for the readiness default, then sets `merge` to call
  `pipe.OrderedFanIn` ordering by `Timestamp()`, ties by `Subscription.ID()` inside the comparison,
  so `Sync` stays merge-agnostic.
- `Sync`: only change is calling `s.merge(chans...)` instead of `pipe.FanIn(chans...)`.
- `SynchronizerV2` doc: one line noting that the merge strategy is FanIn by default and ordered via
  `NewOrderedSynchronizer`.

### 3. Tests
**New `internal/flow/pipe/fanout_test.go`** (`pipe` has no tests today). `TestOrderedFanIn`, a table of
buffered `int` inputs with `less = a < b`:
- interleaved sorted inputs → globally sorted output;
- ties → earlier input first (each value tagged with its input index);
- uneven lengths, one input closing early → the rest drain in order;
- an input that is empty and already closed, among others → ignored;
- zero inputs → output closed.

`TestOrderedFanInWaitsForSlowInput`:
- Setup: the fast input is buffered with `2, 3`; the slow input is unbuffered.
- Assert nothing is emitted within 50ms. A correct implementation never emits here, so this can't
  fail spuriously.
- Then send `1` on the slow input and close both; assert the output is `1, 2, 3`.

**`internal/flow/align/synchronizer_v2_test.go`** (existing tests untouched): `TestOrderedSynchronizerSkew`
- Inputs:
  - subscriptions `btc5m`, `btc1h` with stride `300*sec`;
  - 5m at cols 0..60 and 1h at cols 0, 12, …, 60, which is longer than the 24-column window;
  - each lane sent on its own buffered channel through `NewOrderedSynchronizer(nil).Sync`. Both lanes
    are fully buffered up front, which is the worst skew possible.
- Expected, using the existing helpers:
  - `assertFrames(frames(0, 60, cur))`: every column, in order, all `Current`;
  - `assertInvariants`;
  - `assertLeg` for 1h forward-fill per hour.
- Run it with exact-boundary closes and with `binanceClose(fx)`.
- Determinism: repeat 20 times and compare `eventKeys` (from `readiness_test.go`).
- Contrast: feeding the same data 1h-first through `runFx` emits fewer columns. This shows the merge
  is what saves them.

Also cover `NewOrderedSynchronizer(nil).Sync(nil)`, which must return a closed channel.

### 4. Docs (notify user)
`docs/discussions/multi-subscription.md`:
- **Decisions:** replay ordering is a k-way merge (`pipe.OrderedFanIn`) composed with V2 as
  `align.NewOrderedSynchronizer`. It works with `realtime` and `fixedInterval` tickers and is replay
  only.
- **WIP, Issue 2:** set the direction (an ordered merge in front of V2) and mark the ordered
  synchronizer implemented by this plan. Wiring into `data_logger` and removing V1 are still pending.
  Drop the ticker-scaling and `width` directions.
- **§4:** the closing paragraph says "actively synchronised" is now realised at the seam through the
  ordered merge.
- **State-of-the-code table:** `pipe.OrderedFanIn`; `align` gains `NewOrderedSynchronizer`.
- **Open Questions:**
  - replace the replay-pacing bullet with: how does the engine choose ordered (replay) vs `FanIn`
    (live)?
  - add: a timeframe-aware merge for live (after a close at T, the next close is ≥ T + timeframe),
    with a deadline for missing candles.
  - add: a single ticker on the synchronizer's output instead of per-lane tickers.
- **Solution, "Plans executed":** add `docs/plans/synchronizer-v2-ordered.md`.

`docs/Commands.md`: add `internal/flow/pipe` to the packages `make test` covers.

### 5. Save plan
Copy this plan to `docs/plans/synchronizer-v2-ordered.md`.

## Verification
- `go build ./...`, `go vet ./...`
- `go test ./internal/flow/pipe/ -v`
- `go test ./internal/flow/align/ -v`: the new tests pass and the existing `TestV2*` / `TestOrderedCurrent*`
  tests are unchanged.
- `go test -race ./internal/flow/...`
- Sanity check: temporarily point `NewOrderedSynchronizer`'s merge at `pipe.FanIn` and confirm
  `TestOrderedSynchronizerSkew` fails (it should, nondeterministically or with dropped columns); then
  restore it.
- `make test`, `make lint`, `make fmt` (expect no diff).
- No end-to-end run: `data_logger` still uses V1.
