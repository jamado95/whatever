package frame

import (
	"math"
	"sync/atomic"
)

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
Ring is one entry's history, one slot per update: the slot of col is (col / period) % len. Its owner
writes it with Start and Put; Views read it. Nothing is locked: the owner publishes its writes before
readers see them, and first and end are atomics because an owner may set them while views read.
*/
type Ring[T any] struct {
	slots  []Slot[T]
	period int64        // columns between updates
	window int64        // updates per bar
	bars   int64        // bars readable; the slots beyond them are the owner's
	first  atomic.Int64 // first column; reads never go below it. 0 until Start
	end    atomic.Int64 // Ended column; math.MaxInt64 while open
}

/*
Start records the ring's first column, before its first Put. Reads never go below it, so the owner
may fill the whole ring before anything is read, wrapping onto the slots of earlier columns.
*/
func (r *Ring[T]) Start(col int64) {
	r.first.Store(col)
}

/*
Put writes the slot at col, one of the ring's update columns. A Gap or Ended slot carries the last
Fresh value from the previous update's slot, or the zero value when there is none; Ended also records
the end column. Rewriting a column replaces its slot.
*/
func (r *Ring[T]) Put(col int64, v T, s State) {
	sl := Slot[T]{Col: col, ValCol: col, Val: v, State: s}
	if s != Fresh {
		var zero T
		sl.Val, sl.ValCol = zero, -1
		if prev := r.At(col - r.period); prev != nil {
			sl.Val, sl.ValCol = prev.Val, prev.ValCol
		}
	}

	*r.slot(col) = sl

	if s == Ended {
		r.end.Store(col)
	}
}

// At returns the slot holding col, or nil: on an empty ring, below the first column, or when the slot
// holds another column — never written, overwritten, or past the end.
func (r *Ring[T]) At(col int64) *Slot[T] {
	if len(r.slots) == 0 || col < r.first.Load() {
		return nil
	}

	sl := r.slot(col)
	if sl.Col != col {
		return nil
	}

	return sl
}

func (r *Ring[T]) slot(col int64) *Slot[T] {
	return &r.slots[(col/r.period)%int64(len(r.slots))]
}

func (r *Ring[T]) First() int64 { return r.first.Load() }

func (r *Ring[T]) Len() int64 { return int64(len(r.slots)) }

func (r *Ring[T]) Bars() int64 { return r.bars }

func (r *Ring[T]) Period() int64 { return r.period }

// latest returns the slot of the latest update at or before col — the Ended slot once past the end —
// or nil if there is none.
func (r *Ring[T]) latest(col int64) *Slot[T] {
	if len(r.slots) == 0 {
		return nil
	}

	a := col - col%r.period
	if end := r.end.Load(); a >= end {
		a = end
	}
	return r.At(a)
}

// Latest returns the value at the latest update at or before col, its state and its age in columns
// since its last Fresh value: the zero value, Gap and -1 when there is none.
func (r *Ring[T]) Latest(col int64) (v T, s State, age int) {
	sl := r.latest(col)
	if sl == nil {
		return v, Gap, -1
	}
	if sl.ValCol < 0 {
		return sl.Val, sl.State, -1
	}
	return sl.Val, sl.State, int(col - sl.ValCol)
}

/*
Data appends up to n Fresh values to dst[:0], newest first, one bar (window slots) per step back
from the latest update at or before col. Gap and Ended slots are skipped. The walk covers at most the
ring's bars and stops below the first column or at a slot holding another column: a bar overwritten.
It steps the slot index rather than computing it per slot.
*/
func (r *Ring[T]) Data(col int64, n int, dst []T) []T {
	dst = dst[:0]
	sl := r.latest(col)
	if sl == nil || n <= 0 {
		return dst
	}

	first, size := r.first.Load(), int64(len(r.slots))
	step, back := r.period*r.window, r.window%size
	j := (sl.Col / r.period) % size
	for i, c := int64(0), sl.Col; i < r.bars && len(dst) < n && c >= first; i, c = i+1, c-step {
		s := &r.slots[j]
		if s.Col != c {
			break
		}
		if s.State == Fresh {
			dst = append(dst, s.Val)
		}
		if j -= back; j < 0 {
			j += size
		}
	}
	return dst
}

/*
Buffer is one ring per entry: a stream in the data buffer. Its rings are written through exported
methods, so whoever holds a Buffer can write it: only the layer that owns it may hold it, and it
hands consumers Views, never the Buffer or its rings.
*/
type Buffer[T any] struct {
	rings []Ring[T]
}

// NewBuffer builds one ring per shape, every slot empty and every ring open.
func NewBuffer[T any](shapes []Shape) *Buffer[T] {
	b := &Buffer[T]{rings: make([]Ring[T], len(shapes))}
	for i, sh := range shapes {
		r := &b.rings[i]
		r.slots = make([]Slot[T], sh.Slots)
		for j := range r.slots {
			r.slots[j].Col = -1
		}
		r.period, r.window, r.bars = sh.Period, sh.Window, sh.Bars
		r.end.Store(math.MaxInt64)
	}
	return b
}

func (b *Buffer[T]) Ring(i int) *Ring[T] { return &b.rings[i] }

func (b *Buffer[T]) Len() int { return len(b.rings) }

// header is a frame: its column, the grid stride, its status and completeness. Unexported, so
// nothing outside this package can replace a View's frame.
type header struct {
	col      int64
	stride   int64
	status   Status
	complete bool
}

// Col is the absolute column: boundary ms / stride.
func (h header) Col() int64 { return h.col }

// Boundary is the column's boundary in Unix ms, UTC — not a candle's CloseTs.
func (h header) Boundary() int64 { return h.col * h.stride }

// Complete reports whether every active direct stream's latest due candle at or before the column
// is present.
func (h header) Complete() bool { return h.complete }

func (h header) Status() Status { return h.status }

// View is a Buffer pinned at one frame's column. No read sees a later column.
type View[T any] struct {
	header
	buf *Buffer[T]
}

// NewView pins buf at col, a released column.
func NewView[T any](col, stride int64, status Status, complete bool, buf *Buffer[T]) View[T] {
	return View[T]{
		header: header{col: col, stride: stride, status: status, complete: complete},
		buf:    buf,
	}
}

// Pin pins buf at v's frame.
func Pin[T, U any](v View[U], buf *Buffer[T]) View[T] {
	return View[T]{header: v.header, buf: buf}
}

// Data appends up to n of entry i's Fresh values to dst[:0], newest first, one bar per step back
// from its latest update at or before the view's column.
func (v View[T]) Data(i, n int, dst []T) []T {
	return v.buf.rings[i].Data(v.col, n, dst)
}

// Latest returns entry i's value at its latest update at or before the view's column, its state
// and its age in columns since its last Fresh value: the zero value, Gap and -1 when there is none.
func (v View[T]) Latest(i int) (T, State, int) {
	return v.buf.rings[i].Latest(v.col)
}
