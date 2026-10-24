package data

import (
	"sync"

	proto "whatever/internal/protocol"
)

/*
gate holds the engine's consumed column: the column of the view it is processing; views at earlier
columns are no longer read. Align stages wait on it for ring capacity; the engine's consumption wakes
them, and stopping the gate releases them for good.
*/
type gate struct {
	mu       sync.Mutex
	cond     sync.Cond
	consumed int64 // −1 before the first column
	stopped  bool
}

func newGate() *gate {
	g := &gate{consumed: -1}
	g.cond.L = &g.mu
	return g
}

// wait blocks until the engine has consumed col; false if the gate is stopped first.
func (g *gate) wait(col int64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	for g.consumed < col && !g.stopped {
		g.cond.Wait()
	}
	return !g.stopped
}

// consume moves the engine to col and wakes the waiting align stages.
func (g *gate) consume(col int64) {
	g.mu.Lock()
	g.consumed = col
	g.mu.Unlock()
	g.cond.Broadcast()
}

// stop wakes every waiting align stage; wait returns false from then on.
func (g *gate) stop() {
	g.mu.Lock()
	g.stopped = true
	g.mu.Unlock()
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
last + Period, or sends Ended at −1 for a lane that accepted no candle, then closes its output. Once a
capacity wait fails on the stopped gate it discards the rest of in without writing or sending, and
closes its output with no Ended. It is a pipe.Stage.
*/
func (a *aligner) run(in <-chan laneItem) <-chan evaluate {
	out := make(chan evaluate, laneBuffer)
	write := a.write(out)

	go func() {
		defer close(out)

		for it := range in {
			if !a.stage(write, it) {
				for range in { // stopped: drain so the lane unwinds
				}
				return
			}
		}

		if a.first < 0 {
			out <- evaluate{stream: a.stream, col: -1, state: Ended}
			return
		}

		write(a.last+a.period, proto.Candle{}, Ended)
	}()

	return out
}

// stage writes the item's Fresh slot and the Gap slots before it; false once a capacity wait fails.
func (a *aligner) stage(write func(int64, proto.Candle, LegState) bool, it laneItem) bool {
	if a.first < 0 {
		a.first = it.col
		a.buf.start(a.stream, it.col)
	} else {
		// candle arrives after next expected col -> fill in cols in between as Gap
		for col := a.last + a.period; col < it.col; col += a.period {
			if !write(col, proto.Candle{}, Gap) {
				return false
			}
		}
	}

	if !write(it.col, it.c, Fresh) {
		return false
	}
	a.last = it.col
	return true
}

/*
write waits for ring capacity at col, puts the slot and sends its evaluate; false, writing nothing,
when it must wait and the gate is stopped. col fits when its slot holds nothing a view may still read: older than the
stream's first column until the engine's history passes it — the whole ring at startup — then older
than latest(consumed) − (bars − 1)·period, which allows up to consumed + spare·period.
*/
func (a *aligner) write(out chan<- evaluate) func(int64, proto.Candle, LegState) bool {
	return func(col int64, c proto.Candle, state LegState) bool {
		if col > a.first+(a.slots-1)*a.period && !a.gate.wait(col-a.spare*a.period) {
			return false
		}

		a.buf.put(a.stream, col, c, state, 0)
		out <- evaluate{stream: a.stream, col: col, state: state}
		return true
	}
}
