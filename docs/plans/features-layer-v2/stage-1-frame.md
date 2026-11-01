# Stage 1 — `flow/frame` (features layer v2, iteration 1)

## Context

[main.md](main.md) stage 1. The data layer's `View` (`internal/engine/data/view.go`) is a concrete
struct over the `FrameBuffer`, whose rings and slots are private to `internal/engine/data`. The
feature layer needs the same rings — one slot per update, stepped history, carried values, bounds —
and the same pinned, read-only access, so that features read candles and other features' results
the same way.

This stage moves the ring into `internal/flow/frame` as generic, concrete types — `Slot[T]`,
`Ring[T]`, `Buffer[T]` — with the pinned `View[T]` over them. `Data` and `Latest` are written once and
called directly, without dispatch. The data layer's `FrameBuffer` becomes a `frame.Buffer[proto.Candle]`
plus what only the data layer knows: the stream table, sizing and staging. The derived window count
is dropped. Nothing outside `data_logger_v2` changes, and frames come out identical.

```
consumer ─▶ frame.View[T]{header, *Buffer[T]} ─▶ Ring[T] per entry ─▶ Slot[T]
              data:     View[proto.Candle], entry = stream             (this stage)
              features: View[T] per variant, entry = stream or pair    (stage 3)
```

The stage is one plan in two steps, implemented in order. After each step the changes stay unstaged
for review; the step is committed once approved, then the next starts.

1. The `flow/frame` package.
2. The data layer on `flow/frame`.

## Decisions

- **The ring is generic and concrete.** `Slot[T]` holds `Col`, `ValCol` (the column `Val` was
  written at — `Col` when `Fresh`, the carried value's column when not), `Val` and `State` — today's
  data slot with the candle as `Val`, its `fresh` field renamed so it does not read as a state. `Ring[T]` holds the
  slots, `period`, `window`, readable `bars`, and the `first` / `end` bounds as atomics, as today.
  `Buffer[T]` is one ring per entry. Reads only touch slot metadata and copy `Val`, so `T` needs no
  constraint; each value type compiles to its own code.
- **One read path for every layer.** `Ring.Latest(col)` and `Ring.Data(col, n, dst)` are today's
  `View.Leg` and `View.Candles`: anchored at the latest update at or before `col`, clamped to `end`,
  never below `first`; history steps `period × window` columns, skips `Gap` and `Ended`, covers at
  most `bars` and stops at a slot holding another column.
- **One write path.** `Ring.Put(col, v, s)` is today's `put`: `Gap` and `Ended` carry the last
  `Fresh` value and its `ValCol` from the previous update, and `Ended` records the end. `Start(col)`
  records the first column.
- **An empty ring reads as not started** — zero value, `Gap`, −1, no history. Data rings are never
  empty; feature buffers rely on it from stage 3.
- **Owners write; consumers only read.** `Put`, `Start` and raw slot access (`At`, `First`, `Len`,
  `Bars`, `Period`) are exported for the layer that owns a buffer. A `View` exposes neither its
  buffer nor its rings, so a consumer holding only views cannot write.
- **`At(col)` returns the slot holding `col`, or nil** — on an empty ring, below the first column,
  or when the slot holds another column (never written, overwritten, past the end). The ring never
  reads below its own first column, so an owner's startup fill is never raced. `Latest`, `Data` and
  `Put`'s carry go through it; only `Put`'s own write addresses the slot directly.
- **`View[T]` embeds an unexported `header`** — column, grid stride, status, completeness — whose
  methods `Col`, `Boundary`, `Status` and `Complete` are promoted. Outside `frame` the field cannot
  be named, so a view cannot be re-pinned to another column. There is no exported header type.
- **Views are made by two constructors.** `NewView(col, stride, status, complete, buf)` pins a
  buffer at a released column — the data layer's. `Pin(v, buf)` pins another buffer at `v`'s frame —
  how the feature layer derives its views (stage 3). `View.Data(i, n, dst)` and `View.Latest(i)` read
  entry `i` at the view's column.
- **`State` replaces `LegState`, and `Status` moves too.** `frame.State` keeps `Fresh`, `Gap`, `Ended`
  (`Pending` arrives with live mode); `frame.Status` keeps `Current` (`Late`, `Backfill` later). `data`
  drops both types without aliases.
- **The derived window count is dropped.** Nothing outside tests read it (`View.Count`), and keeping
  it in an array beside each derived ring cost ~7 ns per derivation at 1000 streams — the second
  cache line on every derived write. The aggregator keeps its own count of `Fresh` bars for the
  window; nothing stores it per slot. Revisited if a consumer needs it, by counting the source's
  bars.
- **`Next` returns a `frame.View[proto.Candle]` by value**, valid until the next `Next` or `Close`, and
  `ErrDone` once the run has ended (was `nil, nil`). Views are read-only projections, and a pointer
  to the pipeline's own copy would let a consumer overwrite it.
- **Only a buffer's owner holds it.** `frame` exports the ring write path because owners live in other
  packages, so possession is the guard: the data layer keeps its buffer in unexported fields and hands
  out views only.

## Step 1 — the `flow/frame` package

### Changes — new `internal/flow/frame/frame.go`

```go
package frame

// Status is a frame's kind; Late and Backfill follow with the live and warm-up paths.
type Status uint8

const Current Status = iota // the newest column, released for the first time

// State is the state of an entry's update at a column.
type State uint8

const (
	Fresh State = iota // a value was written at the column
	Gap                // no value at the column; carries the last Fresh value
	Ended              // the entry's source ended; carries the last Fresh value
)

// Slot is one update of an entry.
type Slot[T any] struct {
	Col    int64 // column held; -1 = empty
	ValCol int64 // column Val was written at: Col when Fresh, the carried value's when not; -1 = none
	Val    T     // Fresh: its value; Gap / Ended: the last Fresh value
	State  State
}

// Shape sizes a ring: Slots slots, one update every Period columns, Window updates per bar, Bars
// bars readable.
type Shape struct{ Slots, Period, Window, Bars int64 }

/*
Ring is one entry's history, one slot per update: slot (col / period) % len. Its owner writes it with
Start and Put; Views read it. Nothing is locked: the owner publishes its writes before readers see
them, and first and end are atomics because an owner may set them while views read.
*/
type Ring[T any] struct {
	slots                []Slot[T]
	period, window, bars int64
	first                atomic.Int64 // first column; reads never go below it. 0 until Start
	end                  atomic.Int64 // Ended column; math.MaxInt64 while open
}

func (r *Ring[T]) Start(col int64)
func (r *Ring[T]) Put(col int64, v T, s State)  // Gap / Ended carry the previous update's Fresh value

func (r *Ring[T]) At(col int64) *Slot[T]        // the slot holding col, or nil
func (r *Ring[T]) First() int64
func (r *Ring[T]) Len() int64
func (r *Ring[T]) Bars() int64
func (r *Ring[T]) Period() int64

// Latest returns the value at the latest update at or before col, its state and its age in columns
// since its last Fresh value: the zero value, Gap and -1 when there is none.
func (r *Ring[T]) Latest(col int64) (T, State, int)

// Data appends up to n Fresh values to dst[:0], newest first, one bar per step back from the latest
// update at or before col.
func (r *Ring[T]) Data(col int64, n int, dst []T) []T

// Buffer is one ring per entry: a stream in the data buffer.
type Buffer[T any] struct{ rings []Ring[T] }

func NewBuffer[T any](shapes []Shape) *Buffer[T] // every slot empty, every ring open
func (b *Buffer[T]) Ring(i int) *Ring[T]
func (b *Buffer[T]) Len() int

type header struct {
	col, stride int64
	status      Status
	complete    bool
}

func (h header) Col() int64      // the absolute column: boundary ms / stride
func (h header) Boundary() int64 // col × stride: Unix ms, UTC — not a candle's CloseTs
func (h header) Status() Status
func (h header) Complete() bool  // every active direct stream's latest due candle is present

// View is a Buffer pinned at its frame's column: a read-only snapshot for consumers.
type View[T any] struct {
	header
	buf *Buffer[T]
}

func NewView[T any](col, stride int64, status Status, complete bool, buf *Buffer[T]) View[T]
func Pin[T, U any](v View[U], buf *Buffer[T]) View[T] // buf at v's frame

func (v View[T]) Data(i, n int, dst []T) []T   { return v.buf.rings[i].Data(v.col, n, dst) }
func (v View[T]) Latest(i int) (T, State, int) { return v.buf.rings[i].Latest(v.col) }
```

`Latest`, `Data` and `Put` keep today's bodies (`view.go`'s `latest`, `Leg` and `Candles`;
`buffer.go`'s `put`) with slot fields renamed, plus the empty-ring guard.

### Tests — new `internal/flow/frame/frame_test.go`

- **`TestRingAt`** — a held column returns its slot; below the first column, overwritten, never
  written and an empty ring return nil.
- **`TestRingPut`** — `Fresh` writes; `Gap` and `Ended` carry the last `Fresh` value and column from
  the previous update, or zero and −1 without one; `Ended` records the end; rewriting a column
  replaces its slot.
- **`TestRingLatest`** — anchoring at the latest update for period 1 and 12; clamped at the end;
  below the first column, or on an empty ring, not started; age since the last `Fresh` value.
- **`TestRingData`** — newest first, one bar (`period × window` columns) per step on direct-like and
  rolling-like shapes; `Gap` and `Ended` skipped; at most `bars`; stops at the first column and at an
  overwritten slot; empty ring returns nothing.
- **`TestRingAllocations`** — `Latest` and `Data` with `dst` capacity allocate nothing, for
  `proto.Candle` and `float64` values.
- **`TestView`** — `Col`, `Boundary` (`col × stride`), `Status`, `Complete` as built; `Data` and
  `Latest` read at the view's column; `Pin` keeps the frame over another buffer.

### Acceptance

- `go test -race ./internal/flow/frame/` passes; nothing else changes.

## Step 2 — the data layer on `flow/frame`

### Changes

**`internal/engine/data/buffer.go`**

- `LegState`, `slot` and `ring` are removed. `FrameBuffer` holds the stream table and a
  `*frame.Buffer[proto.Candle]`.
- `newFrameBuffer` keeps its sizing and the 365-day cap, and builds one `frame.Shape` per stream —
  `Slots` as today, `Period`, `Window`, `Bars` — for `frame.NewBuffer`.
- `ring(s) *frame.Ring[proto.Candle]` returns a stream's ring; `start` calls its `Start`; `put` calls
  its `Put` and loses its `count` parameter.
- `view(col, complete)` returns `frame.NewView(col, b.streams.Stride, frame.Current, complete, b.buf)`.

**`internal/engine/data/view.go`** — deleted.

**`internal/engine/data/event.go`** — `Status` and `Current` removed; `evaluate.state` is
`frame.State`.

**`internal/engine/data/align.go`** — the ring's `Len` and `Bars` size the aligner; states are
`frame.*`.

**`internal/engine/data/aggregate.go`** — the source ring through `ring(s)`. The update reads the
source's slot at `col` with `At`; nil means the source has no update there — not started, or ended
before `col` — and nothing is written. The leaving bar and the window scan read through `At` too.
The carried `count` and its writes go. States are `frame.*`.

**`internal/engine/data/readiness.go`** — the lead from each direct ring's `Len`, `Bars` and
`Period`; `complete` calls `ring(s).Latest(col)`; `step`, `release` and `run` carry
`frame.View[proto.Candle]`.

**`internal/engine/data/data.go`** — `views <-chan frame.View[proto.Candle]`; `Next` returns
`(frame.View[proto.Candle], error)`, with `ErrDone` at the end of the run, so the `now` field goes;
`v.col` becomes `v.Col()` in `Next` and `Close`.

**`internal/engine/data_logger_v2.go`** — ends its loop on `data.ErrDone`; `view.Leg(s)` →
`view.Latest(s)`, `data.Fresh` → `frame.Fresh`, `view.Candles(s, lookback, history)` → `view.Data(s, lookback, history)`; imports
`internal/flow/frame`.

### Tests

Mechanical, no behaviour change:

- `LegState` → `frame.State`; `Fresh`, `Gap`, `Ended`, `Current` → `frame.*`; `View` →
  `frame.View[proto.Candle]`; `.col` on a view → `.Col()`.
- `.Leg(s)` → `.Latest(s)`, `.Candles(s, …)` → `.Data(s, …)`.
- Direct slot access — `buf.rings[s]`, `.at(col)`, `.first.Load()`, slot fields `col`, `fresh`, `c`,
  `state` — → `buf.ring(s).At(col)`, `.First()`, `Col`, `ValCol`, `Val`, `State`.
- The count goes: `put` calls drop their `count` argument; `TestViewCount`, the derived-count `TestPut`
  case, the model's and the aggregation tests' count expectations are removed; the reference test
  compares candles and states only. "Ended records the end column" checks a read past the end, since
  `end` is private to `frame`.
- `view_test.go` is deleted: its read cases are `frame` behaviour now, and the ones `frame_test.go`
  lacked — a gap longer than the ring, the age reset after a gap, `Ended` after a gap or before any
  value, a view pinned before the end, history after the end and across the slot index wrap, a ring
  filled from its first column — move there. `TestViewBoundary`, which checks the data layer's
  stride and a 1d stream's UTC-midnight column, moves to `buffer_test.go`.

### Documentation

- `docs/Architecture.md` — `internal/flow`: add `frame` (generic rings, buffers and pinned views);
  the data module line notes its `FrameBuffer` is a `frame.Buffer[proto.Candle]` and frames are
  `frame.View`s.
- `docs/Commands.md` — the `make test` row lists `internal/flow/frame`.
- [data-layer-v2.md](../../discussions/data-layer-v2.md) — § FrameBuffer and the Solution sketch:
  rings, slots and the `View` live in `flow/frame`; `Leg` / `Candles` are `Latest` / `Data`; `Count`
  is data-internal; `LegState` and `Status` are `frame.State` and `frame.Status`. WIP references this
  stage.
- [features.md](../../discussions/features.md) — Decisions § Feature layer and § Feature buffer:
  views are concrete `frame.View[T]` over generic rings, reads direct; feature slots carry `State`
  and `Fresh`, so absence is a state rather than a column mismatch. WIP references this stage.
- [main.md](main.md) — stage 1 marked implemented.

### Acceptance

- Every existing `internal/engine/data` test passes with only the mechanical edits above.
- `go test -race ./internal/engine/data/... ./internal/flow/frame/...` clean; `make check` clean.
- `BenchmarkReadiness` and `BenchmarkAggregate` within noise of their results before the stage.
- `data_logger_v2` on `configs/data_logger_v2.json` exports the same records as before the stage.

## Out of scope

- The feature store, `features.View`, `ViewOf` and the Gap-writing rule for nodes — stage 3.
- `Pending`, `Late`, `Backfill` and revision metadata — the live-mode plan, after this one.
- `data_logger`, `SynchronizerV2` and `proto.SyncEventV2`.

## Verification

```bash
go test -race ./internal/flow/frame/
go test -race ./internal/engine/data/... ./internal/flow/frame/...
go test -run '^$' -bench 'Readiness|Aggregate' -benchmem ./internal/engine/data/
make check
```

Benchmarks are run before step 2 and after it, and compared. The `data_logger_v2` export is
compared against one written before step 2 from the same config.
