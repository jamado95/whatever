package data

import proto "whatever/internal/protocol"

// View is the FrameBuffer pinned at one column: no read sees a later column.
type View struct {
	buf      *FrameBuffer
	col      int64
	complete bool
}

func (v View) Col() int64 { return v.col }

// Boundary is the column's boundary in Unix ms, UTC — not a candle's CloseTs.
func (v View) Boundary() int64 { return v.col * v.buf.streams.Stride }

func (v View) Complete() bool { return v.complete }

// latest returns the slot of stream s's latest update at or before the view's column — its Ended
// slot once past the end — or nil if the stream has not started.
func (v View) latest(s int) *slot {
	period := v.buf.streams.All[s].Period
	r := &v.buf.rings[s]

	a := v.col - v.col%period
	if end := r.end.Load(); a >= end {
		a = end
	}
	if sl := r.at(a, period); sl.col == a {
		return sl
	}
	return nil
}

// Leg returns the stream's candle, state and age in columns since its last Fresh candle. A stream
// not yet started returns a zero candle, Gap and -1.
func (v View) Leg(stream int) (c proto.Candle, s LegState, age int) {
	sl := v.latest(stream)
	if sl == nil {
		return proto.Candle{}, Gap, -1
	}
	if sl.fresh < 0 {
		return sl.c, sl.state, -1
	}
	return sl.c, sl.state, int(v.col - sl.fresh)
}

// Count returns the source bars in the stream's latest window: a derived stream's stored count, 1
// for a direct stream once it has a Fresh candle, 0 before.
func (v View) Count(stream int) int {
	sl := v.latest(stream)
	switch {
	case sl == nil:
		return 0
	case v.buf.streams.All[stream].Kind == Derived:
		return int(sl.count)
	case sl.fresh >= 0:
		return 1
	default:
		return 0
	}
}

/*
Candles appends up to n of the stream's Fresh bars to dst[:0], newest first, one bar (Window slots)
per step back from its latest update. Gap and Ended slots are skipped. The walk covers at most the
stream's kept bars, which headroom keeps clear of the aligner's writes, and stops at a slot holding
another column: the stream had not started, or the bar was overwritten.
*/
func (v View) Candles(stream, n int, dst []proto.Candle) []proto.Candle {
	dst = dst[:0]
	sl := v.latest(stream)
	if sl == nil || n <= 0 {
		return dst
	}

	st := v.buf.streams.All[stream]
	r := &v.buf.rings[stream]
	step := st.Period * st.Window
	for i, col := int64(0), sl.col; i < r.bars && col >= 0 && len(dst) < n; i, col = i+1, col-step {
		s := r.at(col, st.Period)
		if s.col != col {
			break
		}
		if s.state == Fresh {
			dst = append(dst, s.c)
		}
	}
	return dst
}
