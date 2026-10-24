package data

import "math"

/*
readiness releases columns in order. It keeps each direct stream's progress, through: every due
column up to it is decided. On each evaluate it checks head+1 — decided once every stream due there
has through ≥ head+1, and released once some stream has also written at or beyond it — resuming at
the stream that blocked the last check. A released column is Complete when every direct stream's leg
in the View pinned there is Fresh or Ended; streams that ended without a candle are skipped.
*/
type readiness struct {
	buf        *FrameBuffer
	through    []int64 // per direct stream; MinInt64 before its first evaluate, MaxInt64 once ended
	empty      []bool  // ended without a candle: never in the expected set
	unreported int     // direct streams yet to send their first evaluate
	start      int64   // earliest first column across streams
	reach      int64   // latest Fresh or Gap column across streams
	head       int64   // last released column
	cursor     int     // direct streams below it are decided at head+1; all of them: waiting on reach
}

func newReadiness(buf *FrameBuffer) *readiness {
	n := buf.streams.Directs
	r := &readiness{
		buf:        buf,
		through:    make([]int64, n),
		empty:      make([]bool, n),
		unreported: n,
		start:      math.MaxInt64,
		reach:      math.MinInt64,
	}
	for i := range r.through {
		r.through[i] = math.MinInt64
	}
	return r
}

/*
step applies one evaluate and appends the frames it releases to dst. Nothing is released until every
stream has reported; the first column is then the earliest first candle, and with no stream started
nothing is ever released. The scan runs only when this evaluate can change its outcome: it comes from
the stream blocking head+1, head+1 is decided and waits on reach, or it completes the reports.
*/
func (r *readiness) step(e evaluate, dst []View) []View {
	scan := e.stream == r.cursor || r.cursor == len(r.through)
	if r.through[e.stream] == math.MinInt64 {
		r.unreported--
		switch e.state {
		case Fresh:
			r.start = min(r.start, e.col)
		case Ended:
			r.empty[e.stream] = true
		}
		if r.unreported == 0 {
			r.head, scan = r.start-1, true
		}
	}

	if e.state == Ended {
		r.through[e.stream] = math.MaxInt64
	} else {
		r.through[e.stream] = e.col
		r.reach = max(r.reach, e.col)
	}

	// check if all streams have reported first event, and only when this evaluate can change the outcome
	if r.unreported > 0 || !scan {
		return dst
	}
	return r.release(dst)
}

// release releases head+1 while every stream due there has decided it and it is within reach.
func (r *readiness) release(dst []View) []View {
	for {
		col := r.head + 1
		for ; r.cursor < len(r.through); r.cursor++ {
			// due at col and not yet decided: col is held.
			if col%r.buf.streams.All[r.cursor].Period == 0 && r.through[r.cursor] < col {
				return dst
			}
		}
		if col > r.reach {
			return dst
		}
		dst = append(dst, r.buf.view(col, r.complete(col)))
		r.head, r.cursor = col, 0
	}
}

// complete reports whether every direct stream that started has a Fresh or Ended leg at col.
func (r *readiness) complete(col int64) bool {
	v := r.buf.view(col, false)
	for s := range r.through {
		if r.empty[s] {
			continue
		}
		if _, state, _ := v.Leg(s); state == Gap {
			return false
		}
	}
	return true
}

// run is the readiness stage, a pipe.Stage over the joined evaluates; its output closes once its input does.
func (r *readiness) run(in <-chan evaluate) <-chan View {
	out := make(chan View, defaultHeadroom)

	go func() {
		defer close(out)
		var vs []View
		for e := range in {
			vs = r.step(e, vs[:0])
			for _, v := range vs {
				out <- v
			}
		}
	}()

	return out
}
