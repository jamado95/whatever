package data

import (
	"sync"

	proto "whatever/internal/protocol"
)

/*
gate holds the engine's consumed column: the column of the view it is processing; views at earlier
columns are no longer read. Align stages wait on it for ring capacity; the engine's consumption wakes
them.
*/
type gate struct {
	cond     sync.Cond
	consumed int64 // −1 before the first column
}

func newGate() *gate {
	g := &gate{consumed: -1}
	g.cond.L = new(sync.Mutex)
	return g
}

// wait blocks until the engine has consumed col.
func (g *gate) wait(col int64) {
	g.cond.L.Lock()
	defer g.cond.L.Unlock()

	for g.consumed < col {
		g.cond.Wait()
	}
}

// consume moves the engine to col and wakes the waiting align stages.
func (g *gate) consume(col int64) {
	g.cond.L.Lock()
	g.consumed = col
	g.cond.L.Unlock()
	g.cond.Broadcast()
}

// aligner is the align stage of one direct stream's lane; only its goroutine touches its state.
type aligner struct {
	buf    *FrameBuffer
	gate   *gate
	stream int
	period int64
	slots  int64 // ring length
	spare  int64 // ring slots beyond the kept bars
	first  int64 // first Fresh column; −1 before
	last   int64 // last Fresh column
}

func newAligner(buf *FrameBuffer, g *gate, stream int) *aligner {
	r := &buf.rings[stream]
	n := int64(len(r.slots))
	return &aligner{
		buf:    buf,
		gate:   g,
		stream: stream,
		period: buf.streams.All[stream].Period,
		slots:  n,
		spare:  n - r.bars,
		first:  -1,
		last:   -1,
	}
}

/*
run stages each candle at its column: a Gap slot at every due column it skipped, then the Fresh slot,
each written once ring capacity allows and followed by its evaluate. Once in closes it writes Ended at
last + Period, or sends Ended at −1 for a lane that accepted no candle, then closes its output. The
output holds an evaluate per ring slot, as many as can be unread at once, so it waits only on ring
capacity, never on readiness. It is a pipe.Stage.
*/
func (a *aligner) run(in <-chan laneItem) <-chan evaluate {
	out := make(chan evaluate, a.slots)
	write := a.write(out)

	go func() {
		defer close(out)

		for it := range in {
			// if first stream event, init the buffer's stream
			if a.first < 0 {
				a.first = it.col
				a.buf.start(a.stream, it.col)
			} else {
				// candle arrives after next expected col -> fill in cols in between as Gap
				for col := a.last + a.period; col < it.col; col += a.period {
					write(col, proto.Candle{}, Gap)
				}
			}

			// write new column to buffer
			write(it.col, it.c, Fresh)
			a.last = it.col
		}

		// stream did not receive any data
		if a.first < 0 {
			out <- evaluate{stream: a.stream, col: -1, state: Ended}
			return
		}

		write(a.last+a.period, proto.Candle{}, Ended)
	}()

	return out
}

/*
write waits for ring capacity at col, puts the slot and sends its evaluate. col fits when its slot
holds nothing a view may still read: older than the stream's first column until the engine's history
passes it — the whole ring at startup — then older than latest(consumed) − (bars − 1)·period, which
allows up to consumed + spare·period.
*/
func (a *aligner) write(out chan<- evaluate) func(int64, proto.Candle, LegState) {
	return func(col int64, c proto.Candle, state LegState) {
		if col > a.first+(a.slots-1)*a.period {
			a.gate.wait(col - a.spare*a.period)
		}

		a.buf.put(a.stream, col, c, state, 0)
		out <- evaluate{stream: a.stream, col: col, state: state}
	}
}
