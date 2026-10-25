# Stage 2 — `FrameBuffer` core (data layer v2, iteration 1)

## Context

Stage 2 of [main.md](main.md). The `FrameBuffer` holds every stream's market legs; a pinned `View`
reads them. This stage builds the storage, the reads and the one write path the aligner (stage 4)
and `data.Next()` (stage 5) will use. Nothing writes to the buffer at run time yet.

## Decisions

- **`Period` and `Window` replace `Step` in the stream table.** `Period` is the columns between a
  stream's updates — a direct stream's timeframe ÷ `Stride`, a derived stream's source `Period`.
  `Window` is the updates one bar spans — 1 for a direct stream, source bars per window for a derived
  one. A bar spans `Period × Window` columns, its timeframe. "Cadence" in the discussion becomes
  "period".

  | Stream on a 5m grid | `Period` | `Window` |
  |---|---|---|
  | 5m direct | 1 | 1 |
  | 1h direct | 12 | 1 |
  | rolling 1h from 5m | 1 | 12 |
  | rolling 1d from 1h direct | 12 | 24 |
  | 1d direct | 288 | 1 |

- **One ring per stream, one slot per update:** `slot = (col / Period) % size` — revises "one row per
  column, depth is a single row count" in [data-layer-v2.md](../../discussions/data-layer-v2.md).
  Each ring is sized to its own stream's reads, so a coarse stream costs its own bars and one deep
  read no longer sets the depth of every stream.
- **No per-column storage.** `Complete` travels with the frame event and the `View` built from it.
  The released head and the presence count are the aligner's own state (stage 4).
- **Sizing:** `newFrameBuffer(streams, lookback, headroom)`. `lookback` is the longest feature
  lookback in bars, the same for every stream; `headroom` is in grid columns, its value set in
  stage 4. Bars kept per stream:
  - direct: `max(lookback, W + 1, 1)`, `W` the largest `Window` derived from it (the `+1` is the bar
    leaving the window);
  - derived: `max(lookback, 1)`, i.e. that many × `Window` slots.

  Each ring adds `ceil(headroom / Period)` slots — the most the aligner can write ahead of the
  engine's column, wherever the view's anchor falls — plus one spare slot as a margin. The 365-day
  cap applies per stream to
  `bars × Period × Window × Stride`; exceeding it is a construction error naming the stream.
  Per-stream declared reads wait for iteration 4.
- **Leg states:** `Fresh`, `Gap`, `Ended`; `Pending` arrives in iteration 2. A leg's state is that of
  the stream's latest update at or before the column; carried versus updated is `age` (0 = updated
  at this column). `Gap` and `Ended` legs carry the last `Fresh` candle.
- **Explicit slots.** The aligner writes every due column of a stream from its first candle on —
  `Fresh` with the candle, or `Gap` — and an `Ended` slot at the lane's end column. A stream with no
  slot before its end has not started.
- **`Leg(stream)`** returns `(candle, state, age)`, age in grid columns since the last `Fresh` candle;
  a stream not yet started returns `(zero candle, Gap, −1)`.
- **`Count(stream)`** returns the source bars in the stream's latest window at or before the column:
  a derived stream's stored count (below `Window` during warm-up); 1 for a direct stream once
  started; 0 before.
- **`Candles(stream, n, dst)`** reads from the stream's latest update at or before the view's column,
  one bar (`Window` slots) per step, newest first, appended to `dst[:0]`. `Gap` and `Ended` slots are
  skipped; the walk covers at most the stream's kept bars — headroom keeps only those clear of the
  aligner's writes — and stops at a slot holding another column (not started, or overwritten). Fewer
  than `n` bars is not an error.
- **`View` is a concrete struct passed by value** — buffer, column, completeness. `Boundary()` is
  `col × Stride`: the column's boundary in Unix ms, UTC — not a candle's `CloseTs` (Binance:
  `CloseTs + 1 ms`).
- **One write path, `put`,** shared by both writers. Ownership is a convention, not checked: the
  aligner writes direct slots only for columns it has not released; the engine goroutine writes only
  released columns. No locks: the event's channel send publishes the aligner's slots, and headroom
  keeps its writes off readable slots (stage 4). A violation shows up as a data race under `-race`.
  Rewriting a column replaces its slot — last write wins, which iteration 2's `Late` relies on.

## Changes

### Stream table — `internal/engine/data/streams.go`

`Step` is removed; `Period` and `Window` carry the stream's shape.

```go
// Stream is one entry of the stream table; its index in Streams.All is its identity in the data layer.
type Stream struct {
	Sub    proto.Subscription // derived: symbol, window timeframe, Provider "derived_" + source ID
	Kind   Kind
	Period int64 // columns between updates: direct, timeframe / Stride; derived, its source's Period
	Window int64 // updates per bar: direct 1; derived, source bars per window
	Source int   // derived: index of its source stream; direct: -1
}
```

`streams_test.go` checks `Period` and `Window` against the table above in place of `Step`.

### Buffer — new `internal/engine/data/buffer.go`

```go
// LegState is the state of a stream's latest update at or before a column.
type LegState uint8

const (
	Fresh LegState = iota // its candle closed at the update column
	Gap                   // proven missing at the update column; carries the last Fresh candle
	Ended                 // the stream's lane closed; carries the last Fresh candle
)

/*
FrameBuffer holds every stream's market legs, one ring per stream with one slot per update. The
aligner writes direct slots for columns it has not released; the engine goroutine writes released
columns and reads through pinned Views.
*/
type FrameBuffer struct {
	streams *Streams
	rings   []ring
}

type ring struct {
	slots []slot
	bars  int64        // bars kept for reads; the slots beyond them are headroom
	end   atomic.Int64 // Ended column; math.MaxInt64 while open
}

type slot struct {
	col   int64        // column held; -1 = empty
	fresh int64        // newest Fresh column at or before col; -1 = none
	c     proto.Candle // Fresh: its candle; Gap / Ended: the last Fresh candle
	state LegState
	count uint16 // derived: base candles in the window
}

func newFrameBuffer(streams *Streams, lookback int, headroom int64) (*FrameBuffer, error)

// put writes stream s's slot at col. A Gap or Ended slot carries the last Fresh candle from the
// stream's previous slot; Ended also records the end column.
func (b *FrameBuffer) put(s int, col int64, c proto.Candle, state LegState, count uint16)

// view returns the View pinned at col.
func (b *FrameBuffer) view(col int64, complete bool) View
```

- A slot is about 80 B: column, last `Fresh` column, candle, state, window count.
- `ring.end` is atomic because views read it while the aligner may write it.
- `put` copies the carried candle from the slot at `col − Period`; with no such slot (the stream's
  first due column) the copy is a zero candle and last `Fresh` −1.

### View — new `internal/engine/data/view.go`

```go
// View is the FrameBuffer pinned at one column: no read sees a later column.
type View struct {
	buf      *FrameBuffer
	col      int64
	complete bool
}

func (v View) Col() int64
func (v View) Boundary() int64 // col × Stride: Unix ms, UTC
func (v View) Complete() bool
func (v View) Leg(stream int) (c proto.Candle, s LegState, age int)
func (v View) Count(stream int) int // source bars in the latest window; 0 if not started
func (v View) Candles(stream, n int, dst []proto.Candle) []proto.Candle // newest first
```

`Leg` at view column `c`, anchor `a = c − c % Period`, the stream's latest update column at or
before `c`:

| Condition | Returns |
|---|---|
| `a ≥ end` | the `Ended` slot: last `Fresh` candle, `Ended`, `c − fresh` |
| slot holds `a` | its candle and state, `c − fresh` |
| otherwise | not started: zero candle, `Gap`, −1 |

`Candles` walks back from `a` (or from `end` once ended) `Window` slots at a time, at most the
stream's kept bars, collecting `Fresh` slots until `n`. Found during implementation: a walk bounded
by the ring length would cross into the headroom slots the aligner is writing whenever gaps are
skipped — a data race.

## Tests

Grouped by concept: one test function per concept, each scenario a `t.Run` subtest — Go's
equivalent of `describe` / `it`. A `/* */` block above each function states what the group covers.

Shared fixture unless a subtest says otherwise: a 5m grid, `BTCUSDT 5m` deriving 1h, `BTCUSDT 1h`
deriving 1d, `ETHUSDT 1d`; `lookback` 20, `headroom` 4.

### `streams_test.go`

- **`TestStreamsEntries`** checks `Period` and `Window` against the table in Decisions in place of
  `Step`.

### New `buffer_test.go`

**`TestNewFrameBuffer` — construction and sizing**
- Slots per ring: 5m direct 25, rolling 1h 245, 1h direct 27, rolling 1d 482, 1d direct 22 — against
  28,820 for every stream with shared rows.
- `lookback` 0: every stream keeps at least 1 bar; a source keeps `W + 1`.
- `headroom` 0, and `headroom` below `Period`: a 1d stream with `headroom` 4 gets 2 extra slots.
- 1m grid: `Period`, `Window` and slot counts rescale — a 1h direct stream has `Period` 60.
- Cap: exactly 365 days of bars passes; one bar more fails naming the stream; a rolling 1d from 5m
  with `lookback` 366 fails naming the derived stream, not its source.

**`TestPut` — the write path**
- `Fresh` stores its candle, with last `Fresh` = its column.
- `Gap` and `Ended` carry the previous slot's last `Fresh` candle; with no previous slot, a zero
  candle and −1.
- Consecutive gaps keep carrying the same `Fresh` candle.
- `Ended` records the end column.
- Rewriting a column: last write wins.
- A derived slot stores its window count.

**`TestFrameBufferConcurrency` — writer ahead of reader**
- One goroutine writes direct slots up to `headroom` columns ahead, publishing each column on a
  channel; the reader reads views pinned behind it. Clean under `-race`.

**`TestFrameBufferModel` — reference model**
- Fixed seed. Random `Fresh` / `Gap` / `Ended` writes across the fixture over several ring lengths;
  at random pinned columns, `Leg`, `Count` and `Candles` equal a brute-force model holding the full
  history in a slice.

### New `view_test.go`

**`TestViewLeg`**
- `Fresh` at a due column: age 0.
- Coarse direct stream between closes: age 1 just after the close, `Period − 1` just before the next.
- `Gap`: the last `Fresh` candle and its age; consecutive gaps keep the candle, age keeps growing.
- `Fresh` after a gap: age back to 0.
- `Ended` straight after a gap: the `Fresh` candle from before the gap, with its age.
- A lane ended before any candle: `Ended`, zero candle, −1.
- Not started: zero candle, `Gap`, −1.
- Pinned before the end: a view below the end column sees the earlier state.
- Derived from a coarse direct stream: anchored at the source's last update.

**`TestViewCount`**
- Derived during warm-up: 1 … `Window`, then `Window`.
- Direct: 1 once started; 0 before.
- A count written at a later column is not visible.

**`TestViewCandles`**
- Newest first; `n` bars when history allows, fewer when it is short.
- The 11:00 bar already written, view pinned at 10:30: the read starts at 10:00 and never returns
  11:00. Same for a derived stream.
- Rolling 1h steps 12 slots; a step landing on a `Gap` slot is skipped.
- After `Ended`: the bars before the end; the `Ended` slot is not returned as a bar.
- Not started: empty. `n` = 0: empty. `n` beyond the kept bars: the kept bars.
- Wrap seam, after three ring lengths: a read across the slot-index wrap is exact; a read deeper
  than the kept bars returns only the kept bars.

**`TestViewAllocations`**
- With `dst` capacity, `testing.AllocsPerRun` is 0 for `Candles`, `Leg` and `Count`.

**`TestViewBoundary`**
- `Boundary()` is `col × Stride`. At realistic epoch columns (≈5.6M at 5m), a UTC-midnight column is
  due for a 1d stream and `Boundary()` is that midnight.

## Documentation updates

- [data-layer-v2.md](../../discussions/data-layer-v2.md):
  - "Cadence" becomes "period" throughout, including the § Cadence heading.
  - Decisions § FrameBuffer — per-stream rings with one slot per update replace "one row per column";
    `Period` and `Window` on the stream table; sizing from `lookback` and windows; no per-column
    storage; reads newest first, skipping gaps; the not-started `Leg` convention; one write path,
    ownership by convention.
  - Solution § FrameBuffer — the code block and the memory bullet follow this doc; `View` is a struct
    with `Boundary()`.
  - WIP references this plan.
- [main.md](main.md): stage 2 entry marked implemented, its contents matching this doc.
- [pipeline-runtime.md](../../discussions/pipeline-runtime.md) and [main.md](main.md): "cadence" becomes "period".
- [README.md](../../discussions/README.md): data-layer-v2 row → stages 1–2 implemented.

## Out of scope

- Deciding gaps, ends and releases, the released head, the headroom value and the consumed column —
  stage 4.
- Aggregation and derived legs' states and ends — stage 5.
- `Pending`, revision metadata and `Late` writes — iteration 2. `Late` writes released direct slots
  on the engine goroutine, so `put`'s copy of the carried candle must then come from aligner-side
  state rather than the previous slot.
- Feature entries and writers — iteration 4; record sections — out of scope.

## Verification

- `make check` clean.
- `go test -race ./internal/engine/data/...` clean.
