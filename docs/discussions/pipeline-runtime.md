# Pipeline Runtime

## Topic

How a run executes, where cross-stream alignment sits, and who owns time:

1. **Runtime model** — goroutine + channel per stage (today) versus a synchronous step per frame.
2. **Alignment point** — after the per-stream feature chains (today's seam, [[multi-subscription]]
   §5) versus once, in a data layer that is the engine's first stage.
3. **Clock** — where live release deadlines and replay pacing live.

This is the determinism-versus-liveness theme from the [README](README.md) applied to the whole
pipeline rather than to one combinator. The data layer module itself — stages, aggregation, events,
`FrameBuffer` — is detailed in [[data-layer-v2]].

Opened 2026-10-17 from a design review of the feature engine.

## Current Understanding

### Today

`data_logger`: per sub `provider → FeatureChain.Process` (goroutine, 100-candle output buffer) →
`OrderedFanIn` → `runV2` (goroutine) → `ticker.Gate` → drain. `full` adds `FanOut` to strategies,
`FanIn` of signals into risk, and a risk ⇄ executors cycle (orders out, fills back).

### Channel-per-stage costs in a backtest

1. **Little intra-run parallelism.** Per-candle work per stage is the same order as a channel
   hand-off, and every lane converges on the single synchronizer goroutine.
2. **Order is emergent from scheduling.** Determinism is re-imposed at every fan point —
   `OrderedFanIn`, ticker gating, `FanOut`'s drop policy ([[engine]] defect 2), and V2 issue 2,
   where chain output buffers let lanes skew past the window.
3. **Feedback loops.** A fill for bar `t` must reach the ledger before bar `t+1` reaches risk; across
   channels nothing guarantees it, and the risk ⇄ executor cycle can deadlock on full buffers.

### Where parallelism pays

Each run serial and deterministic; parallelism **across runs**. Sweeps and the permutation /
walk-forward tests in `docs/Backtesting.md` need hundreds of independent runs. Live parallelism
belongs within a step (across legs, across strategies) or across independent stream groups — not
across pipeline stages.

### Ordering versus alignment

V2 solves two distinct problems; the runtime choice changes neither.

| Problem | Replay | Live |
|---|---|---|
| **Ordering** — out-of-order arrival across streams | Trivial: k-way merge, all data available | Inherent: window + deadline |
| **Alignment** — column grid, mixed timeframes, forward-fill, completeness | `syncState` | `syncState` |

`syncState.Apply` / `Readiness.Evaluate` are already synchronous and carry over; only the `runV2`
goroutine wrapper is specific to the channel model.

### Measured unit costs

Micro-benchmarks, 2026-10-17, Intel Core Ultra 9 275HX (24 cores), Go 1.26. Indicative, not
end-to-end.

| Operation | Cost | Site today |
|---|---|---|
| Channel hand-off, buffered / unbuffered | 36 / 109 ns | every stage hop |
| `map[string]any` snapshot, 10 floats | 339 ns, 13 allocs | `FeatureChain.Process`, per candle |
| 10 floats into `[]float64` slots | 3 ns, 0 allocs | proposed buffer |
| SMA-50 from ring / recursive EMA | 21 / 1.2 ns | feature maths |
| V2 `ready()` scan, N=1000 | 7.7 µs per call | per arrival |
| Per-column arrival counter | ~0.3 ns | proposed readiness |
| Frame build — copy + sort 1000 legs | 41 µs, 245 KB | `NewSyncEventV2`, per frame |
| Merge, linear scan / heap, N=1000 | 356 / 56 ns per item | `OrderedFanIn` / proposed |
| Parse one Binance kline CSV row | 261 ns | ingestion |

### Extrapolated run cost

1000 streams × 1m × 1 year = 525.6M candles.

- **Today's bottleneck is readiness.** Arrivals reach V2 in ID order, so the k-th arrival scans ~k
  streams before hitting a missing one — ~N²/2 lookups ≈ 3.9 ms per column ≈ **34 min per run** in
  the synchronizer goroutine. Lanes run in parallel and finish well before it.
- **CPU per run, today → frame-based with fixes:** parse 137 s → 0 (once per sweep); snapshot maps
  178 s → 1.5 s; merge 187 s → 30 s (heap) or 0 (precomputed index); readiness ~2000 s → ~0;
  frame build 21 s + ~129 GB allocated → ~0 (views); features (10 cheap) 105 s → 105 s; 3+ channel
  hops per candle → none.

### Backtest versus live

| | Backtest | Live |
|---|---|---|
| Bound by | CPU throughput | Waiting on data and venue |
| Clock | Data-driven, as fast as possible | Wall clock / venue |
| Backpressure | Pull, to the source | Never upstream of the aligner |
| Ordering | Precomputed frame index or heap merge, no deadline | Window + deadline |
| `Late` | Never occurs | Bounded recompute |
| Buffers | Rings; shared memory-mapped dataset | Rings + aligner window + frame-event queue |
| Parallelism | Across runs; share data, frame index, fixed features | Within a step, only for heavy features |
| Hot costs | Readiness, merge, snapshot allocations, parsing | Network, alignment wait, GC tail |

Live throughput is small — 1000 × 1m streams average ~17 candles/s, arriving as a ~1000-candle burst
per boundary; a frame's feature work is ~0.2 ms. Latency is dominated by venue publish delay,
network, the alignment wait and the order round trip — unmeasured, plausibly ms to 100s of ms.

## Decisions

- **The engine's first layer is a frame-based data layer.** It owns (1) ingestion from providers,
  (2) frame alignment — ordering and readiness, and (3) defines the `FrameBuffer` every other layer
  reads, which the engine operates.
  The rest of the engine runs as a synchronous step per frame.
- **Feature outputs live in the frame buffer**, giving every consumer implicit feature history
  (a z-score of RSI reads RSI's history).
- **The data layer emits status-tagged frame events on a channel**; components declare which
  statuses they consume.
- **`Late` is one status** — a column first dispatched behind newer ones, or an already-dispatched
  column whose data changed. It replaces V2's `Stale` and exists so state-keeping consumers can
  update toward complete, higher-quality data.
- **Features consume `Late` and recompute; strategies and downstream skip it.**
- **A frame is the full per-step record:** aligned multi-stream featured data plus strategy, order,
  execution and other downstream outputs.
- **The engine is the `FrameBuffer`'s only writer** — market data via `Commit`, plus the feature,
  strategy and execution sections. Fills and order updates are written there by the engine and do not
  pass through the data layer ([[data-layer-v2]]).
- **The data layer is the module `internal/engine/data`** ([[data-layer-v2]]).

These reverse recorded positions not yet updated in their own documents:

- `Architecture.md` — "`flow/pipe` is the primary data/control flow mechanism".
- [[multi-subscription]] — seam after the per-stream chains; `Stale` semantics; dispatch-once.
- [[features]] — one `SortedWindow` per chain; cross-asset features as a stage after the chains.
- [[engine]] — the ticker as a gate between provider and pipeline.

## Solution

Direction only; no plan. Earlier options — a synchronous core alone, and alignment at ingestion with
strictly increasing output — are subsumed here; strictly increasing output was dropped in favour of
`Late` revisions.

### Shape

```
             ┌──────────────── data layer ───────────────┐
providers ──▶│ ingest ──▶ aligner (readiness, watermark)  │── frame events ──▶ engine step loop
             │               staging window               │   (status-tagged)   commit → FrameBuffer
             └────────────────────────────────────────────┘                          │
                                features ──▶ strategies ──▶ portfolio/risk ──▶ execution ──▶ fills
```

1. **Ingestion** — one provider instance per sub. Concurrent; never blocks a provider's socket reader.
2. **Alignment** — readiness and the watermark decide when a column is released and with which
   status (Clock below).
3. **`FrameBuffer`** — per-stream raw data, feature outputs and per-step records, written only by the
   engine. `Late` revisions land here.

Stage-level detail in [[data-layer-v2]].

### Step loop

```go
for {
    f := data.Next(ctx)               // receive frame event, commit to buffer (view + dirty set)
    fd := features.Step(f)            // tier 0 parallel over dirty legs → barrier → tier 1+
    sigs := strategies.Step(f, fd)    // parallel over woken strategies; merged in ID order
    orders := portfolio.Step(f, sigs) // serial: one ledger, netting, limits
    exec.Submit(orders)               // async; the engine appends fills to the next frame
}
```

- **The engine is the channel's only consumer** and dispatches by each component's accepted
  statuses. Components reading the channel directly would restore channel-per-stage and lose the
  step barrier.
- **Commit, then compute.** The aligner writes only its staging window. The engine loop commits a
  frame event into the buffer, then runs the phases; the buffer is immutable during phases, so reads
  need no locks and there is one writer goroutine.
- **Dirty sets drive work.** A frame event carries the legs that closed and the cells revised.
  Features step only dirty streams, cross features run when an input is dirty and their sampling
  clock fires, strategies wake when their subs or keys change. Without this every frame costs
  N × F — a 1d stream beside 1m streams would recompute 1440× per candle.
- **Determinism** holds at any worker count provided every phase merges its outputs in a fixed order.

### Frame record rules

- Sections — market legs, features, signals, orders, fills — each written only by its phase.
- **Visibility:** a phase sees earlier phases' sections of frame `t` and everything before `t`.
- **Immutability:** `Late` rewrites only market and feature sections; signals, orders and fills are
  history.
- **Fills join the next frame by arrival.** Backtest: the simulated broker fills `t−1`'s orders at bar
  `t`, deterministically. Live: placement depends on timing, so a session is replayable from recorded
  frames, not from market data alone.

### Status set

| Status | Meaning | Features | Strategies / downstream |
|---|---|---|---|
| `Current` | Newest column, first dispatch | Step | Step |
| `Late` | Column dispatched behind newer ones, or revised | Recompute range | Skip |
| `Backfill` | Startup warm-up; catch-up after reconnect or overrun | Step | Skip |

`Backfill` bypasses the pacer; the first frame after catch-up is `Current`. Overrun is frames
arriving faster than steps complete — rare in steady state at candle timeframes, routine during
warm-up and reconnect backfills.

### `Late` handling

- A revised candle at column `c` was read by every later column whose window includes it, so
  affected features recompute `[c, now]`, bounded by lookback, plus dependent tier-1+ nodes.
- Pure window features self-correct going forward, but stored history between `c` and now stays
  wrong until recomputed. Recursive features need state at `c−1` and a replay (open).
- Requires relaxing V2's dispatch-once invariant: today a correction to a dispatched column is
  written to the matrix but `dispatched[c]` suppresses re-emission.
- Cost is small and live-only — e.g. 200 candles × 10 features ≈ 40 µs.

### Features phase

- **One graph keyed by `(stream, feature)`.** Cross features are ordinary nodes depending on several
  streams' keys; no seam, split or re-sync. The earlier question of how a cross feature emits —
  annotating each leg's snapshot versus a derived stream — is moot.
- **Tiers:** tier 0 (own stream only) runs parallel over dirty legs; tier 1+ (depends on a cross
  node) runs after a barrier.
- **`Dependencies()` becomes `(key, depth)`**, so the buffer sizes each feature's ring to its deepest
  reader.
- **Cross features declare a sampling clock** (close of all legs, or a named timeframe). Forward-filled
  legs otherwise bias them — a 1h correlation in a group with a 5m stream sees each value 12×.
- Running tier 0 on arrival, before alignment, was considered for latency; unnecessary at candle
  timeframes, where compute is far below the alignment wait.

### Parallelism per phase

| Phase | Unit | Notes |
|---|---|---|
| Features, tier 0 | Streams sharded across a fixed worker pool | Worker owns its shard's state; inline for narrow frames |
| Features, tier 1+ | Cross-feature nodes | After the tier-0 barrier |
| Strategies | Strategy instances | Own state, read the immutable buffer; outputs merged by ID |
| Portfolio / risk | Mostly serial | One ledger; per-instrument marking can parallelise |
| Execution | Async I/O per venue | Engine appends returned fills to the next frame |

Backtests choose per run: a worker pool for wide frames, or one worker per run when sweeping.

### Buffer

API, storage layout, write discipline and invariants of the `FrameBuffer` are in [[data-layer-v2]].

### Clock: watermark and pacer

Rule: whatever changes frame content or status belongs to the data layer; whatever changes only when
frames are consumed belongs to the engine. Replay output is therefore identical under any pacing.

- **Watermark (data layer).** A column is released when complete (fast path) or when the watermark
  passes it (deadline path, with the legs present); data for a column below the watermark is `Late`.
  - Live: wall clock − allowed lateness, per venue or timeframe; needs timers, since a deadline can
    expire with no data arriving.
  - Replay: data time — the minimum next-candle `CloseTs` across lanes. Exact; nothing is ever late.
- **The aligner never reads `time.Now()`.** Time is injected, so deadline logic is testable with a
  fake clock, and a live-simulation replay (virtual wall clock, seeded arrival delays) can exercise
  the live policy deterministically.
- **Pacer (engine), replay only** — live runs unpaced. Modes map onto today's `Ticker`: none
  (`realtime`), fixed per frame (`fixedInterval`), factor (`SetFactor`, unimplemented), manual
  (`Tick`, unimplemented). This settles [[engine]]'s question on the `Simulated` / `Manual` tickers.

```go
// flow/timing — times in ms, matching CloseTs
type Clock interface {
    Now() int64
    Wake(at int64) <-chan struct{} // fires once Now() >= at
}

type Pacer interface {
    Wait(ctx context.Context, frameTime int64) error
}

// data layer
type Watermark interface {
    Current() int64 // columns at or below are releasable; data below is Late
}
// live: WallWatermark{clock, lateness}; replay: DataWatermark (min next CloseTs across lanes)
```

### Backtest specifics

- One run per core; throughput scales with core count.
- **Share across sweep runs** what depends only on the data: the dataset (CSV → binary columnar once,
  memory-mapped read-only, shared through the page cache), the frame index (depends only on dataset
  and subscription set, so replay needs no merge or readiness), and features the sweep does not vary.
  Permutation tests change the data and cannot share.
- Memory: rings ≈ 35 MB per run (N=1000, depth 256, 10 features). Full history at N=1000 × 1m × 1y
  is ≈ 29 GB of candles and ≈ 42 GB for 10 features — full sharing fits around 100 streams; beyond
  that, memory-map and chunk by time.

### Live specifics

1. Never block a provider's socket reader: the provider → aligner channel holds at least one boundary
   burst (N), and the aligner sustains it (~1 ms per burst with counter readiness). A slow engine
   queues frame events — the `Backfill` case — rather than stalling ingestion.
2. The frame-event queue normally holds 0–1; its depth measures overrun.
3. Aligner and ring storage in each stream's own candle units.
4. Allocation-free hot path, to keep GC out of tail latency.

Live compute matters only for heavy cross features — a 1000×1000 covariance update is ~500k pair
updates per frame, PCA is O(n³) — which run asynchronously with results landing in a later frame, or
less often; and for tick / order-book data, where event rates rise by orders of magnitude.

### Fixes independent of the redesign

Counter readiness, heap merge, slot-indexed feature outputs and frame views apply to today's code.
Readiness alone accounts for ~34 min of a 1000-stream backtest run.

## Open Questions

- **Migration path** — rewrite, or add `Step` / `Push` methods beside the channel APIs first and
  build a backtest engine on them while `data_logger` stays on channels.
- **Recursive features under `Late`** — save state per column across the window, or accept revisions
  going forward only?
- **Overrun policy** — coalesce pending frames into one step (union of dirty sets; features see every
  close, strategies the latest) versus queue and process each.
- **Allowed lateness** — values per venue / timeframe; what a deadline-released frame records for a
  missing leg (forward-fill with a per-leg freshness flag?).
- **Live sharding** — what is an independent stream group once cross features link streams?
  Connected components of the feature / strategy dependency graph?
- **Frame view and dirty-set representation** at scale.
- **Heavy features** — asynchronous computation with results in a later frame: mechanism and
  visibility rules.
- **Recording frames** for live replay and audit; does the exporter read frames directly?
- **Backtest fill timing** (no earlier than next bar open) — with [[execution-layer]].
- **End-to-end benchmark** of `SynchronizerV2` at N = 10, 100, 1000 to confirm the readiness
  extrapolation.

## WIP

- Nothing implemented, no plan.
- Unit costs above come from micro-benchmarks run outside the repository; no benchmark is checked in.

## Resources

- `internal/flow/align/synchronizer_v2.go` (`syncState`, `ready`, `runV2`),
  `internal/flow/pipe/fanout.go` (`OrderedFanIn`), `internal/flow/timing/timing.go` (`Ticker`),
  `internal/domains/features/feature.go` (`Process`), `internal/protocol/snapshot.go`,
  `internal/engine/data_logger.go`, `internal/engine/full.go`
- `docs/Architecture.md`, `docs/Backtesting.md`
- Related: [[data-layer-v2]], [[multi-subscription]], [[features]], [[engine]], [[data-layer]],
  [[strategy-layer]], [[risk-management]], [[execution-layer]]
