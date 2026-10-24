package data

import (
	"math"

	proto "whatever/internal/protocol"
)

// deque is a fixed-capacity ring of (column, value) holding a window's running maximum at the front.
type deque struct {
	cols    []int64
	vals    []float64
	head, n int
}

func newDeque(capacity int64) deque {
	return deque{cols: make([]int64, capacity), vals: make([]float64, capacity)}
}

// front returns the maximum; the deque must not be empty.
func (d *deque) front() float64 { return d.vals[d.head] }

// expire drops the entries at columns up to col.
func (d *deque) expire(col int64) {
	for d.n > 0 && d.cols[d.head] <= col {
		d.head = (d.head + 1) % len(d.cols)
		d.n--
	}
}

// push appends (col, v) after dropping the entries at the back that v reaches.
func (d *deque) push(col int64, v float64) {
	for d.n > 0 && d.vals[(d.head+d.n-1)%len(d.cols)] <= v {
		d.n--
	}

	if d.n == len(d.cols) {
		panic("deque capacity exceeded: expire entries before push")
	}

	i := (d.head + d.n) % len(d.cols)
	d.cols[i], d.vals[i] = col, v
	d.n++
}

/*
window is one derived stream's rolling state over its source's bars at columns
col − (W−1)·Period … col; only the engine goroutine touches it.
*/
type window struct {
	buf            *FrameBuffer
	stream, source int
	period, w      int64   // w: source bars per window
	started        bool    // the derived ring's first column is set
	fresh          int64   // Fresh source bars in the window
	oldest         int64   // oldest Fresh source column in the window; −1 for none
	vol, comp      float64 // running volume and its Neumaier compensation
	hi, lo         deque   // lo holds negated lows, so both keep their extreme at the front
	count          uint16  // count of the last Fresh write, carried by Gap and Ended
}

/*
update slides the window to col, one of its source's update columns, and writes the derived slot
there: Fresh with the window's candle and the count of Fresh bars in it; Gap and Ended carrying the
latest Fresh derived candle and its count. A source not started at col, or ended before it, writes
nothing.

The bar at col enters the window and the bar at col − W·Period leaves it. The source keeps W + 1
bars, so the leaving bar is still readable. The window's candle takes Open and OpenTs from its oldest
Fresh bar, Close and CloseTs from the bar at col, High and Low from the deques and Volume from the
compensated running sum.
*/
func (w *window) update(col int64) {
	src := w.buf.view(col, false).latest(w.source)
	if src == nil || src.col != col {
		return
	}

	// start derived ring's first column in main data buffer
	if !w.started {
		w.buf.start(w.stream, col)
		w.started = true
	}

	srcRing := &w.buf.rings[w.source]
	first := srcRing.first.Load()
	out := col - w.w*w.period
	if out >= first {
		if sl := srcRing.at(out, w.period); sl.col == out && sl.state == Fresh {
			w.fresh--
			w.add(-sl.c.Volume)
		}
	}
	w.hi.expire(out)
	w.lo.expire(out)
	if w.fresh == 0 {
		w.vol, w.comp = 0, 0 // an empty window sums to exactly zero
	}

	if src.state != Fresh {
		w.buf.put(w.stream, col, proto.Candle{}, src.state, w.count)
		return
	}

	c := src.c
	w.fresh++
	w.add(c.Volume)
	w.hi.push(col, c.High)
	w.lo.push(col, -c.Low)
	if w.oldest < 0 || w.oldest <= out {
		w.oldest = w.nextFresh(max(out+w.period, first), col)
	}

	openC := srcRing.at(w.oldest, w.period).c
	w.count = uint16(w.fresh)

	w.buf.put(w.stream, col, proto.Candle{
		OpenTs:  openC.OpenTs,
		CloseTs: c.CloseTs,
		Open:    openC.Open,
		High:    w.hi.front(),
		Low:     -w.lo.front(),
		Close:   c.Close,
		Volume:  w.vol + w.comp,
	}, Fresh, w.count)
}

// nextFresh returns the first Fresh source column in from … to; to is Fresh.
func (w *window) nextFresh(from, to int64) int64 {
	r := &w.buf.rings[w.source]
	for col := from; col < to; col += w.period {
		if sl := r.at(col, w.period); sl.col == col && sl.state == Fresh {
			return col
		}
	}
	return to
}

// add adds x to the running volume with Neumaier compensation.
func (w *window) add(x float64) {
	t := w.vol + x
	if math.Abs(w.vol) >= math.Abs(x) {
		w.comp += (w.vol - t) + x
	} else {
		w.comp += (x - t) + w.vol
	}
	w.vol = t
}

// aggregator materialises the derived legs of released columns on the engine goroutine.
type aggregator struct {
	windows []window // one per derived stream, in stream-table order
}

func newAggregator(buf *FrameBuffer) *aggregator {
	st := buf.streams
	a := &aggregator{windows: make([]window, 0, len(st.All)-st.Directs)}
	for i, s := range st.All[st.Directs:] {
		a.windows = append(a.windows, window{
			buf:    buf,
			stream: st.Directs + i,
			source: s.Source,
			period: s.Period,
			w:      s.Window,
			oldest: -1,
			hi:     newDeque(s.Window),
			lo:     newDeque(s.Window),
		})
	}
	return a
}

// step aggregates col, the column just released: update on each window whose source updates there.
func (a *aggregator) step(col int64) {
	for i := range a.windows {
		if w := &a.windows[i]; col%w.period == 0 {
			w.update(col)
		}
	}
}
