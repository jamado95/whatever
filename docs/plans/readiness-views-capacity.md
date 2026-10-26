# Readiness views capacity — the event channel sized from the rings

## Context

Readiness's output, the channel `Next` reads, is `make(chan View, defaultHeadroom)`: 128, fixed
(`internal/engine/data/readiness.go`). Once a ring holds full history, align writes up to
`consumed + spare·Period`, with `spare = ceil(headroom / Period) + 1`. A Period-1 stream therefore
resolves `headroom + 1` columns past the engine's column. Two consequences:

- At the default headroom, readiness releases 129 frames, fills the channel with 128 and blocks
  holding the last one.
- `newDataPipeline`'s `headroom` never reaches the channel. At headroom 600 (tests), columns past 128
  wait as unprocessed evaluates, so an engine that drains the 128 quickly waits on readiness.

Measured in a scratch run (four 1m streams, lanes producing instantly, engine paused after 1000
frames):

| Headroom | Released past the engine | In channel | Held by readiness |
|---|---|---|---|
| 128 | 129 | 128/128 | 1 |
| 600 | 129 (the rest staged as evaluates) | 128/128 | 1 |

**Goal:** the channel holds every column align can resolve past the engine's column once rings hold
full history. After startup, readiness then never blocks on the engine, and whenever the next column
is resolved, `Next` finds it already released.

This amends the decision "`defaultHeadroom` is 128 columns; the event channel holds as many" in
[data-layer-v2.md](../discussions/data-layer-v2.md) (§ Capacity). It is independent of the
[live-mode plan](data-layer-live-mode/main.md).

## Decisions

- **Capacity is the largest `spare·Period` over direct streams.** `spare` is a ring's slots beyond
  its kept bars, the same value `newAligner` derives. `newReadiness` computes it from the rings, so
  it follows the headroom the rings were built with.
- **Why it suffices:** a released column is at most `reach`, the latest column some stream wrote.
  Past its startup fill, each stream writes at most `consumed + spare·Period`. The frames released
  but not yet received therefore never exceed the capacity.
- **Startup may still block.** Before anything is read, a ring fills completely (kept bars plus
  spare), so readiness may block on a full channel until the engine's consumption passes the
  startup fill. This costs throughput only, as § Capacity already accepts. Covering startup would
  take the longest direct ring span, `max(slots · Period)`; unchanged.
- **The coarsest direct stream sets the capacity once its Period exceeds headroom.** Its spare is 2,
  so its lead is `2·Period`:

  | Streams | Headroom | Capacity |
  |---|---|---|
  | 5m grid with a direct 1d (`testSubs`) | 4 or 128 | 576 |
  | 1m grid with a direct 1d | 128 | 2880 (about 69 KB; a `View` is 24 B) |

  While a Period-1 stream is active, only `headroom + 1` frames can be released. The extra capacity
  is used only once every finer stream has ended. Not taken: sizing at the finest stream's lead
  (`headroom + 1`), which is tighter but lets readiness block in that end-of-run case.
- **`defaultHeadroom` stays 128** and now only sizes the rings.

## Changes

### 1. `internal/engine/data/readiness.go`

- `readiness` gains `lead int64`: the most columns an align stage can resolve past the engine's
  column once its ring holds full history, the largest `spare·Period` over direct streams; the
  output channel's capacity.
- `newReadiness`: over `buf.rings[:buf.streams.Directs]`, `lead = max(lead, (len(r.slots) − r.bars)
  · r.period)`. Direct rings have `Window` 1, so `len(slots) − bars` is the aligner's `spare`.
- `run`: `make(chan View, r.lead)`. Doc: once every ring holds full history, its output holds every
  frame align can resolve ahead of the engine, so it never blocks on the engine; at startup it may.

### 2. `internal/engine/data/buffer.go`

- `defaultHeadroom` comment: drop "also the event channel's capacity".

### 3. Tests: `internal/engine/data/readiness_test.go`

- **`TestReadinessCapacity`**, table-driven: `cap` of `newReadiness(buf).run(in)`'s output.

  | Streams | Lookback | Headroom | Want |
  |---|---|---|---|
  | `testSubs()` | 20 | 4 | 576: ETH 1d, 2 spare × Period 288 |
  | BTC 5m, BTC 1h | 20 | 128 | 144: 1h, 12 spare × Period 12, above the 5m stream's 129 |
  | BTC 5m, ETH 5m | 20 | 200 | 201: `headroom + 1` |

- **`TestReadinessBacklog`**: BTC 5m and ETH 5m, lookback 20, headroom 200. Each stream gets 1000
  columns from `day0` through `feedLane` into its aligner; the aligners join through `pipe.FanIn`
  into readiness, with one gate, as in `BenchmarkReadiness`.
  - Receive and consume frames through `day0 + 100`.
  - The channel fills to 201 frames, every column align resolved past the engine, polled with a
    10 s deadline. The fixed capacity stopped at 128.
  - Consuming one more frame refills it to 201.
  - Draining the rest gives every column from `day0` to `day0 + 999` in order, then the output
    closes.
- Existing tests unchanged. `BenchmarkReadiness` runs with a capacity of 129 instead of 128.

### 4. Docs

- [data-layer-v2.md](../discussions/data-layer-v2.md) § Capacity, replacing the `defaultHeadroom`
  bullet:
  > **`defaultHeadroom` is 128 columns.** The event channel holds the largest `spare·Period` over
  > direct streams: the most columns align can resolve past the engine's column once rings hold full
  > history. After startup, readiness never blocks on the engine, and every resolved column is
  > already released when `Next` asks for it. At startup a stream fills its whole ring and readiness
  > may block on a full channel. That costs throughput only: the engine's consumption depends on
  > events alone, and align keeps staging into its own channel meanwhile (§ Alignment). Never
  > blocking at startup too would take the longest direct ring span, `max(slots · Period)`.
- [data-layer-v2.md](../discussions/data-layer-v2.md) § Alignment ("even while readiness waits on
  a full event channel"): unchanged, still true at startup.
- [data-layer-v2.md](../discussions/data-layer-v2.md) WIP: add a bullet referencing this plan.
- `docs/Commands.md`, `docs/Architecture.md`: no change. The executed
  [data layer v2 plan](data-layer-v2/main.md) stays as is.

## Out of scope

- Startup blocking (`max(slots · Period)`).
- The `defaultHeadroom` value and headroom for live bursts (data-layer-v2 Open Questions).
- The evaluate channel's capacity.

## Verification

- `make check`.
- `go test -race ./internal/engine/data/`, as in the live-mode plan.
- `go test -run TestReadinessBacklog -v ./internal/engine/data/` fails against the fixed capacity,
  stopping at 128, before the change.
