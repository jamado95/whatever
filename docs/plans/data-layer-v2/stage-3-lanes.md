# Stage 3 — lanes, normalisation, ordered merge (data layer v2, iteration 1)

## Context

Stage 3 of [main.md](main.md). Each provider's output becomes a validated lane; lanes merge into one
stream ordered by (column, stream), which the aligner (stage 4) will consume. This stage also fixes
how the data layer pipeline is built: a stage convention in `flow/pipe` and one unexported composer
in the module. Nothing runs the pipeline at run time yet — the module API arrives in stage 5.

```
 ingest (provider)           normalise (lane, 1 goroutine per direct stream)          order (1 goroutine)
provider_0 ─MarketData(ms)─▶ lane_0: validate → column → due → duplicate → order ─laneItem─┐
provider_1 ─MarketData(ms)─▶ lane_1: …                                            ─laneItem─┼─▶ OrderedFanIn ─laneItem─▶ stage 4: aligner
provider_k ─MarketData(ms)─▶ lane_k: …        on close: end marker + drop summary ─laneItem─┘   (col, stream)     (stage → ready → commit)
```

| Stage | In | Out | Runs on | State | Job |
|---|---|---|---|---|---|
| Ingest | venue data | `<-chan proto.MarketData`, ms timestamps | provider goroutine | provider's | Venue format → `Candle` in ms; `binance_csv` scales µs |
| Normalise | `<-chan proto.MarketData` | `<-chan laneItem`, buffered | one goroutine per direct stream | last accepted candle and column, drop counters | Rules in order; end marker when the input closes |
| Order | `[]<-chan laneItem`, in stream order | `<-chan laneItem` by (column, stream) | one merge goroutine | heap of lane heads | Pulls by data time; a lane whose head waits blocks its provider |

## Decisions

- **The data layer does not use `SynchronizerV2`.** `internal/engine/data` never imports `align`.
  Carried over as ideas, not code: the ceilDiv column formula (`Streams.column`), the merge injected
  per mode (the composer's `order`), and the apply → evaluate → emit loop (stage 4's aligner).
  `align` and `data_logger` stay unchanged until retired.
- **Stage convention, named in `flow/pipe`.** A stage is `pipe.Stage[In, Out]`: it starts its own
  goroutine, owns its output and closes it once its input closes, and never drops a value silently.
  A fan-in is `pipe.Merge[T]`; `FanIn` and a bound `OrderedFanIn` fit it. Goroutine boundaries sit
  only where parallelism or blocking needs them (lanes, merge, aligner, engine); steps inside one
  goroutine — the normalise rules, later stage → ready → commit — are plain functions called in
  sequence. No third-party pipeline library: none offers a merge ordered by key, and the hot path
  needs typed channels without per-item boxing or wrapping.
- **One composer, unexported.** `pipeline` in `internal/engine/data` holds one field per stage and
  wires them in one place; per-mode stages are injected, so live ordering (iteration 2) swaps
  `order` and tests can replace any stage. Stage 4 adds `align`. Stage 5 exports a `data.New`
  constructor around it; no package-level instance or factory singleton.
- **ms is the timestamp contract; providers meet it.** `binance_csv` scales µs timestamps (Binance
  spot archives from 2025-01, [data-layer.md](../../discussions/data-layer.md) defect 7). Venue
  format stays in the provider; the lane rejects anything outside the ms range rather than guessing
  units.
- **Placement stays ceilDiv:** `col = ceil(CloseTs / Stride)` — a close at or up to one stride
  before a boundary lands in that boundary's column.
- **Span within `[timeframe − 1 s, timeframe]`.** A close at or just before its boundary (Binance:
  boundary − 1 ms) passes; a span beyond the timeframe is rejected, since under ceilDiv a close past
  its boundary would shift the bar one column with no error. The span check also catches a file
  wired to the wrong subscription.
- **Monotonicity by column, not `CloseTs`.** Two closes in one column (10:59:59.999 and
  11:00:00.000) are increasing `CloseTs` but would write one slot twice.
- **Drops are logged once per rule per lane, then summarised.** The first drop of each rule logs a
  warning; every drop is counted; a lane with drops logs one summary line when it ends. Identical
  duplicates are only counted.
- **End marker at the lane's next due column**, `lastCol + Period`, so the stream leaves the
  expected set there. A lane that ends before accepting any candle sends its marker at column −1:
  it sorts first, and stage 4 must not start its released head from it.
- **The lane carries only what the aligner needs:** column, stream index, end flag and candle.
  `Subscription` and `ReceivedAt` are dropped; the stream index is authoritative.
- **Lanes take no `done` and do not read provider errors.** Shutdown and errors follow `data_logger`:
  stage 5 drains the providers' error channels and, on `Close`, closes providers and keeps draining
  the merged output so lanes unwind.
- **`OrderedFanIn` moves onto `container/heap`, contract unchanged.** On refill the root is replaced
  and `heap.Fix`ed — no allocation; `heap.Pop` only when an input closes. Measured at 1000 heads, all
  tied, 88-byte items: Push + Pop 139 ns and 2 allocs (160 B) per value; root replace + `Fix` 101 ns,
  0 allocs; a hand-rolled generic sift-down 125 ns, 0 allocs. Ties go to the earlier input by an
  explicit index comparison, since the heap is not stable.

## Changes

### Ingest — `internal/domains/provider/binance_csv.go`

```go
// usThreshold separates µs from ms epoch timestamps: ms stays below it until the year 5138, µs
// exceeds it from 1973.
const usThreshold = 1e14

// toMs scales a µs timestamp to ms; ms passes through. Binance spot archives from 2025-01 carry µs.
func toMs(ts int64) int64
```

`parseCsvRecordToCandle` applies `toMs` to `OpenTime` and `CloseTime`. `data_logger` gets the fix
too.

### Stage convention — `internal/flow/pipe/pipeline.go`

```go
/*
Stage is one step of a pipeline. It starts its own goroutine, owns its output and closes it once its
input closes; it never drops a value silently. Steps that share a goroutine are plain functions
called in sequence: a channel hop belongs only where parallelism or blocking needs one.
*/
type Stage[In, Out any] func(in <-chan In) <-chan Out

// Merge joins several streams into one and closes its output once every input has closed.
type Merge[T any] func(ins ...<-chan T) <-chan T
```

### Order — `internal/flow/pipe/fanout.go`

`OrderedFanIn` keeps its signature and contract: one pending value per open input, filled in index
order; the least is emitted, ties to the earlier input; it blocks while any open input has no pending
value; the output is unbuffered and closes once every input has closed.

```go
// pending is one input's value awaiting emission.
type pending[T any] struct {
	v  T
	in int
}

// pendingHeap orders pending values by less, ties to the earlier input.
type pendingHeap[T any] struct {
	items []pending[T]
	less  func(a, b T) bool
}

func (h *pendingHeap[T]) Len() int
func (h *pendingHeap[T]) Less(i, j int) bool // less(a, b) || !less(b, a) && a.in < b.in
func (h *pendingHeap[T]) Swap(i, j int)
func (h *pendingHeap[T]) Push(x any)
func (h *pendingHeap[T]) Pop() any
```

Loop: fill each input once, `heap.Init`; then emit the root, read the root's input again — a value
replaces the root and `heap.Fix(h, 0)`, a closed input `heap.Pop(h)` — until the heap is empty.

### Stream table — `internal/engine/data/streams.go`

```go
// column maps a close timestamp to the first column boundary at or after it (ceilDiv): a close at
// or up to one stride before a boundary lands in that boundary's column.
func (s *Streams) column(ts int64) int64
```

### Normalise — new `internal/engine/data/lane.go`

```go
// laneItem is one value of a lane and of the merged stream: a candle at its column, or the lane's
// end marker.
type laneItem struct {
	col    int64        // candle: the column it closes in; end: the lane's next due column, −1 if it had no candles
	stream int          // index in the stream table
	end    bool         // the stream has nothing at col or later
	c      proto.Candle // candle: validated, ms timestamps
}

// drop is why a lane rejected a candle; keep accepts it.
type drop uint8

const (
	keep       drop = iota
	invalid         // fails validation
	offDue          // closes off its stream's due column
	duplicate       // identical to the last accepted candle
	outOfOrder      // its column is not beyond the last accepted one
	numDrops
)

// msLimit bounds ms epoch timestamps; a larger one is in another unit (µs) and is invalid.
const msLimit = 1e14

/*
spanTolerance is how far a candle's span (CloseTs − OpenTs) may fall short of its timeframe. Venues
close a bar at its boundary or just before it — Binance at boundary − 1 ms — so a valid span lies in
[timeframe − spanTolerance, timeframe]. A longer span is rejected: under ceilDiv placement a close
past its boundary would land one column late.
*/
const spanTolerance = 1000 // ms

// laneBuffer is a lane's output capacity, matching the provider and feature-chain buffers.
const laneBuffer = 100

// lane is the normalise stage of one direct stream; only its goroutine touches its state.
type lane struct {
	streams *Streams
	stream  int
	tfMs    int64
	last    proto.Candle // last accepted candle
	lastCol int64        // its column; −1 before the first
	drops   [numDrops]int
	log     *logger.Logger
}

func newLane(streams *Streams, stream int, log *logger.Logger) *lane

// run normalises in: accepted candles at their columns, then the end marker and, if anything was
// dropped, a summary line once in closes. It is a pipe.Stage.
func (l *lane) run(in <-chan proto.MarketData) <-chan laneItem

// accept applies the rules in order and returns the candle's column, or why it was dropped.
func (l *lane) accept(c proto.Candle) (int64, drop)

// valid reports whether c has ms timestamps, finite and consistent OHLCV, and a span that fits tfMs.
func valid(c proto.Candle, tfMs int64) bool
```

Rules, in order:

| # | Rule | Check | Drop | Logged |
|---|---|---|---|---|
| 1 | validate | `OpenTs > 0`; `CloseTs < msLimit`; O, H, L, C, V finite; `V ≥ 0`; `L ≤ min(O, C)`; `H ≥ max(O, C)`; `tf − spanTolerance ≤ CloseTs − OpenTs ≤ tf` | `invalid` | first per lane, counted |
| 2 | column | `col = streams.column(CloseTs)` | — | — |
| 3 | due | `col % Period == 0` | `offDue` | first per lane, counted |
| 4 | duplicate | `c == last` | `duplicate` | counted only |
| 5 | order | `col > lastCol` | `outOfOrder` | first per lane, counted |

An accepted candle updates `last` and `lastCol`; a dropped one changes neither. Prices may be zero or
negative — no instrument-specific checks.

### Order stage — new `internal/engine/data/order.go`

```go
// before orders the merged stream by (column, stream).
func before(a, b laneItem) bool

// orderLanes is the replay order stage: OrderedFanIn over the lanes, by (column, stream). A lane's
// provider blocks while its next value waits in the merge.
func orderLanes(lanes ...<-chan laneItem) <-chan laneItem
```

### Composer — new `internal/engine/data/pipeline.go`

```go
/*
pipeline is the data layer's stage graph, wired in one place. Each field is one stage; the per-mode
stages are injected, so live ordering (iteration 2) swaps order and tests can replace any stage.
Stage 4 adds align; stage 5 wraps the pipeline in the module API.
*/
type pipeline struct {
	normalise func(stream int) pipe.Stage[proto.MarketData, laneItem] // one lane per direct stream
	order     pipe.Merge[laneItem]                                    // replay: orderLanes; live: FanIn
}

// replayPipeline builds the replay stages: a lane per direct stream, ordered by (column, stream).
func replayPipeline(streams *Streams, log *logger.Logger) pipeline

// run wires the stages; ins[i] is direct stream i's provider output.
func (p pipeline) run(ins []<-chan proto.MarketData) <-chan laneItem
```

## Tests

Grouped by concept, one test function per concept with `t.Run` subtests, a `/* */` block above each.
Fixture: the stage 2 shared fixture — a 5m grid, `BTCUSDT 5m` deriving 1h, `BTCUSDT 1h` deriving 1d,
`ETHUSDT 1d` — with Binance-style candles (`CloseTs` = boundary − 1 ms) from 2024-01-01 00:00 UTC.
Loggers discard their output except in the logging subtest, which writes to a buffer.

### `internal/domains/provider` — new `binance_csv_test.go`

**`TestParseCsvRecordToCandle`**
- A 2024 ms record parses unchanged.
- A 2025 µs record (`1735689600000000 … 1735689899999999`) parses to `1735689600000` and
  `1735689899999`.

### `internal/flow/pipe` — `fanout_test.go`

- `TestOrderedFanIn` and `TestOrderedFanInWaitsForSlowInput` unchanged.
- **`TestOrderedFanInModel`** — fixed seeds; 50 inputs of random length (0–40), sorted values from a
  small range so ties are frequent; the output equals a stable sort of every (value, input) by value,
  then input.
- **`BenchmarkOrderedFanIn`** — N = 10, 100, 1000; every input carries the same sequence, so every
  value ties, as streams on one grid do; `b.N` values in total, pre-filled into closed buffered
  channels before the timer; `ReportAllocs`. Run on the linear scan before the change and on the
  heap after; both recorded below. The numbers include two channel operations per value, so they
  sit above the heap-only figures in Decisions.

### `internal/flow/align`

- Existing tests unchanged; `TestOrderedSynchronizerSkew` exercises the heap through
  `NewOrderedSynchronizer`.

### `internal/engine/data` — `streams_test.go`

**`TestStreamsColumn`**
- Boundary − 1 ms and the exact boundary land in the boundary's column; boundary + 1 ms lands in the
  next one.

### New `lane_test.go`

**`TestLaneAccept` — the rules**
- A valid 5m candle is accepted at its column.
- `invalid`: `OpenTs` 0; a µs `CloseTs`; a NaN close; an infinite high; negative volume; low above
  the open; high below the close.
- Span: `tf − 1 s` and `tf` pass; `tf − 1001 ms` and `tf + 1 ms` fail.
- `offDue`: a 1h candle closing at 10:30 is dropped; at 11:00 accepted.
- `duplicate`: an identical repeat. A different candle in the same column, or an earlier column, is
  `outOfOrder`.
- Rule order decides the reason: an identical repeat is `duplicate`, not `outOfOrder`; a µs repeat is
  `invalid`.
- A dropped candle moves nothing: after an `invalid` candle at a column, a valid one at that column
  is accepted.

**`TestLaneRun` — the stage**
- Accepted candles in input order, then the marker at `lastCol + Period` (1h lane last at 11:00 →
  12:00), then the channel closes.
- Drops at the tail: the marker follows the last accepted candle.
- An empty input, or one where every candle is dropped: only a marker at −1.
- Counters per rule after close.
- Logging: one warning per rule that dropped anything, none for duplicates, and one summary line.

### New `pipeline_test.go`

**`TestPipelineOrder`**
- One day of 5m, 1h and 1d candles with a 5m gap and the 1h lane ending at 11:00: the output is
  sorted by (column, stream); each stream's candles keep their order; the 1h marker lands at 12:00
  among that column's items by stream index; the count is candles plus one marker per lane.
- No direct streams: the output closes.

**`TestPipelineBackpressure`**
- The 5m lane is fed one candle at a time by the test; a goroutine feeds 1000 1h bars into an
  unbuffered channel, counting sends. While the merge sits at column `c`, the sends stay at most the
  1h bars due by `c` plus `laneBuffer` plus 2 (one in the lane goroutine, one pending in the heap);
  checked after a 50 ms settle, as in `TestOrderedFanInWaitsForSlowInput`. The 1d lane gets an empty,
  closed input — its −1 marker goes first — so a silent lane does not hold the merge. Found during
  implementation: the sends settle exactly on the bound (107 of 1000).

## Documentation updates

- [data-layer-v2.md](../../discussions/data-layer-v2.md):
  - Decisions § Module and stages: the data layer does not use `SynchronizerV2`; the stage convention
    (`pipe.Stage`, `pipe.Merge`, goroutine boundaries only where needed); the unexported composer
    with per-mode stages; `data.New` exported in stage 5, no singleton.
  - Decisions § Release: end marker at the lane's next due column, −1 for a lane with no candles.
  - Solution § Pipeline table: Normalise row — the five rules, the ms contract met by providers (µs
    scaling moves to the provider), drops logged once per rule and summarised; Order row —
    `container/heap`, wired by the composer.
  - WIP references this plan.
- [data-layer.md](../../discussions/data-layer.md): defect 7 resolved in `binance_csv`; the gap and
  integrity open question answered for v2 — the lane validates and drops, providers keep venue
  format.
- [main.md](main.md): stage 3 entry marked implemented, its touches and rules matching this doc
  (`pipe.Stage` / `pipe.Merge`, `order.go`, `pipeline.go`, `binance_csv.go`); stage 5 exports
  `data.New`, no singleton.
- `docs/Architecture.md`: Design Rules — pipeline stages follow `pipe.Stage` / `pipe.Merge`;
  `internal/engine/data` gains lanes and the ordered merge.
- `docs/Commands.md`: the benchmark command,
  `go test -run '^$' -bench OrderedFanIn -benchmem ./internal/flow/pipe/`.
- [pipeline-runtime.md](../../discussions/pipeline-runtime.md): the merge row of the measured costs
  takes the benchmark's numbers.
- [README.md](../../discussions/README.md): data-layer-v2 row → stages 1–3 implemented.

## Out of scope

- Staging, readiness, commit, final-column release and handling the −1 marker — stage 4.
- The module API (`data.New`, `Start`, `Next`, `Close`), mapping providers to streams, draining
  provider errors and shutdown — stage 5.
- Live ordering (`FanIn`) and `Late` — iteration 2.
- Retiring `align`, `SynchronizerV2` and `data_logger`.
- CSV header rows ([data-layer.md](../../discussions/data-layer.md) defect 6); `binance_historical`,
  whose REST klines are ms.
- Checking `MarketData.Subscription` against the lane's stream.

## Verification

- `make check` clean.
- `go test -race ./internal/engine/data/... ./internal/flow/... ./internal/domains/provider/...`
  clean.
- `make validate` passes on `config.json`.
- Benchmark before and after recorded below.

## Benchmark results

`BenchmarkOrderedFanIn`, 2026-10-21, Intel Core Ultra 9 275HX, Go 1.26; ns per value, median of
three runs, 0 allocations throughout. Every value ties and each includes two channel operations.

| Inputs | Linear scan | Heap |
|---|---|---|
| 10 | 136 | 139 |
| 100 | 317 | 192 |
| 1000 | 1682 | 269 |

At 10 inputs channel hand-offs dominate and both are equal; at 1000 the heap is 6.3× faster.
