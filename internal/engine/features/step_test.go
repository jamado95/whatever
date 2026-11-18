package features

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	proto "whatever/internal/protocol"
	"whatever/internal/protocol/feature"
	"whatever/internal/protocol/frame"
)

// TestStepOrder covers the serial order: with dependents listed before their dependencies, each is
// Fresh with its dependencies' values at the same column once warmed up.
func TestStepOrder(t *testing.T) {
	coords := [][2]int{stream(btc5m), stream(rolling1h)}
	r := newRun(t, 30, slices.Concat(
		over(ddiff(), "ddiff-diff-last", map[string]string{"diff": "diff-last"}, coords...),
		over(diff(), "diff-last", map[string]string{"last": "last"}, coords...),
		on(last(), coords...),
	))
	bars := []int64{btc5m: 1, rolling1h: 12} // columns per bar

	for col := int64(0); col < 30; col++ {
		ddiffs := mustView[float64](t, r.step(col), "ddiff-diff-last")
		for _, s := range []int{btc5m, rolling1h} {
			b := bars[s]
			d := func(c int64) float64 { return closeAt(s, c) - closeAt(s, c-b) }
			x, state, _ := ddiffs.Latest(s)
			switch {
			case col < 2*b && state != frame.Gap:
				t.Errorf("stream %d at %d: ddiff %v %d, want Gap while warming up", s, col, x, state)
			case col >= 2*b && (x != d(col)-d(col-b) || state != frame.Fresh):
				t.Errorf("stream %d at %d: ddiff %v %d, want %v Fresh", s, col, x, state, d(col)-d(col-b))
			}
		}
	}
}

// TestStepDue covers due patterns: the periods dividing a column pick its schedule, so a coarse
// stream's nodes are written only at its due columns, and nothing where no period divides.
func TestStepDue(t *testing.T) {
	r := newRun(t, 26, on(last(), stream(direct1h), stream(btc5m)))
	if want := []int64{1, 12}; !slices.Equal(r.e.periods, want) {
		t.Fatalf("periods = %v, want %v", r.e.periods, want)
	}
	for _, c := range []struct {
		col  int64
		want int
	}{{0, 2}, {13, 1}, {24, 2}} {
		if got := r.e.pattern(c.col); got != c.want {
			t.Errorf("pattern(%d) = %d, want %d", c.col, got, c.want)
		}
	}

	lasts := r.g.columns["last"].(*results[float64])
	for col := int64(0); col <= 25; col++ {
		r.step(col)
		if got, want := lasts.buf.Ring(direct1h).At(col) != nil, col%12 == 0; got != want {
			t.Errorf("direct1h at %d: written %t, want %t", col, got, want)
		}
		if lasts.buf.Ring(btc5m).At(col) == nil {
			t.Errorf("btc5m at %d: not written", col)
		}
	}

	coarse := newRun(t, 14, on(last(), stream(direct1h)))
	if got := coarse.e.pattern(13); got != 0 || len(coarse.e.schedules[0]) != 0 {
		t.Errorf("pattern(13) = %d with %d nodes, want 0 with none", got, len(coarse.e.schedules[0]))
	}
}

/*
TestStepLegs covers legs that fire nothing: a Gap leg in a Partial frame, where the other streams
compute; a stream not started; and a stream ending, written Ended once and nothing after, its history
still readable.
*/
func TestStepLegs(t *testing.T) {
	all := [][2]int{stream(btc5m), stream(eth5m), stream(rolling1h)}
	r := newRun(t, 13, slices.Concat(
		on(last(), all...),
		over(diff(), "diff-last", map[string]string{"last": "last"}, all...),
	))
	r.buf.Ring(btc5m).Put(5, proto.Candle{}, frame.Gap)
	r.buf.Ring(rolling1h).Start(3)
	r.buf.Ring(eth5m).Put(8, proto.Candle{}, frame.Ended)

	type read struct {
		v     float64
		state frame.State
		age   int
	}
	latest := func(v frame.View[float64], s int) read {
		x, state, age := v.Latest(s)
		return read{x, state, age}
	}
	diffAt := func(s int, col int64) float64 { return closeAt(s, col) - closeAt(s, col-1) }

	lasts := r.g.columns["last"].(*results[float64])
	diffs := r.g.columns["diff-last"].(*results[float64])
	for col := int64(0); col <= 12; col++ {
		v := r.e.Step(frame.NewView(col, testStride, frame.Current, col != 5, r.buf))
		lastAt, diffOf := mustView[float64](t, v, "last"), mustView[float64](t, v, "diff-last")

		switch col {
		case 5:
			if got, want := latest(lastAt, eth5m), (read{closeAt(eth5m, 5), frame.Fresh, 0}); got != want {
				t.Errorf("eth5m last in a Partial frame = %+v, want %+v", got, want)
			}
			if got, want := latest(lastAt, btc5m), (read{closeAt(btc5m, 4), frame.Gap, 1}); got != want {
				t.Errorf("btc5m last at its Gap = %+v, want %+v", got, want)
			}
			if got, want := latest(diffOf, btc5m), (read{diffAt(btc5m, 4), frame.Gap, 1}); got != want {
				t.Errorf("btc5m diff at its Gap = %+v, want %+v", got, want)
			}
		case 8:
			if got, want := latest(lastAt, eth5m), (read{closeAt(eth5m, 7), frame.Ended, 1}); got != want {
				t.Errorf("eth5m last at its end = %+v, want %+v", got, want)
			}
			if got, want := latest(diffOf, eth5m), (read{diffAt(eth5m, 7), frame.Ended, 1}); got != want {
				t.Errorf("eth5m diff at its end = %+v, want %+v", got, want)
			}
		case 12:
			if got, want := latest(lastAt, eth5m), (read{closeAt(eth5m, 7), frame.Ended, 5}); got != want {
				t.Errorf("eth5m last past its end = %+v, want %+v", got, want)
			}
			if got, want := lastAt.Data(eth5m, 2, nil), []float64{closeAt(eth5m, 7)}; !slices.Equal(got, want) {
				t.Errorf("eth5m last history past its end = %v, want %v", got, want)
			}
		}

		if col > 8 && (lasts.buf.Ring(eth5m).At(col) != nil || diffs.buf.Ring(eth5m).At(col) != nil) {
			t.Errorf("eth5m at %d: written after its end", col)
		}
		got, want := latest(lastAt, rolling1h), read{closeAt(rolling1h, col), frame.Fresh, 0}
		if col < 3 {
			want = read{0, frame.Gap, -1}
		}
		if got != want {
			t.Errorf("rolling1h last at %d, starting at 3 = %+v, want %+v", col, got, want)
		}
	}
}

// TestStepStrict covers strict firing: a dependent of a dependency that cannot compute is written Gap
// without firing, though it would compute.
func TestStepStrict(t *testing.T) {
	r := newRun(t, 4, slices.Concat(
		on(sum(3), stream(btc5m)),
		on(last(), stream(btc5m)),
		over(ratio(), "ratio-sum_3-last", map[string]string{"sum": "sum_3", "last": "last"}, stream(btc5m)),
	))
	for col := int64(0); col < 4; col++ {
		x, state, _ := mustView[float64](t, r.step(col), "ratio-sum_3-last").Latest(btc5m)
		if col < 2 {
			if state != frame.Gap {
				t.Errorf("ratio at %d, sum_3 warming up = %v %d, want Gap", col, x, state)
			}
			continue
		}
		want := (closeAt(btc5m, col) + closeAt(btc5m, col-1) + closeAt(btc5m, col-2)) / closeAt(btc5m, col)
		if x != want || state != frame.Fresh {
			t.Errorf("ratio at %d = %v %d, want %v Fresh", col, x, state, want)
		}
	}
}

// stateNames name a slot's state; "none" is a slot holding another column.
var stateNames = [...]string{frame.Fresh: "Fresh", frame.Gap: "Gap", frame.Ended: "Ended"}

/*
TestStepPairs covers pair firing: a pair is written at its ends' larger period, Gap where either end's
leg is, and Ended once, at its first due column at or after either end ended.
*/
func TestStepPairs(t *testing.T) {
	coarse, fine := pair(direct1h, rolling1h), pair(eth5m, rolling1h)
	r := newRun(t, 50, slices.Concat(
		on(last(), stream(direct1h), stream(rolling1h)),
		on(spread(), coarse, fine),
		over(pdiff(), "pdiff-last", map[string]string{"last": "last"}, coarse),
	))
	r.buf.Ring(rolling1h).Put(24, proto.Candle{}, frame.Gap)
	r.buf.Ring(rolling1h).Put(41, proto.Candle{}, frame.Ended)

	written := func(id string, ring int, col int64) string {
		if sl := r.g.columns[id].(*results[float64]).buf.Ring(ring).At(col); sl != nil {
			return stateNames[sl.State]
		}
		return "none"
	}
	coarseWant := func(col int64) string {
		switch {
		case col%12 != 0:
			return "none"
		case col == 24:
			return "Gap"
		case col == 48:
			return "Ended"
		}
		return "Fresh"
	}
	fineWant := func(col int64) string {
		switch {
		case col == 24:
			return "Gap"
		case col == 41:
			return "Ended"
		case col > 41:
			return "none"
		}
		return "Fresh"
	}

	c, f := r.g.pairs[coarse], r.g.pairs[fine]
	for col := int64(0); col <= 49; col++ {
		r.step(col)
		for _, w := range []struct {
			id, want string
			ring     int
		}{
			{"spread", coarseWant(col), c},
			{"pdiff-last", coarseWant(col), c},
			{"spread", fineWant(col), f},
		} {
			if got := written(w.id, w.ring, col); got != w.want {
				t.Errorf("%s on pair %d at %d: %s, want %s", w.id, w.ring, col, got, w.want)
			}
		}
	}

	ended := r.g.columns["spread"].(*results[float64]).buf.Ring(c).At(48)
	if want := closeAt(direct1h, 36) - closeAt(rolling1h, 36); ended.Val != want || ended.ValCol != 36 {
		t.Errorf("coarse pair Ended = %+v, want carrying %v from 36", *ended, want)
	}
}

/*
TestStepBruteForce compares Step with bruteStep, the firing rules applied as defined, over random legs
and Expand's placement order, dependents before their dependencies. After each column, every slot
written there, and the state Latest reads, is equal on every ring.
*/
func TestStepBruteForce(t *testing.T) {
	st := testStreams()
	universes := map[string]proto.Universe{
		"pairs": universe(st, []int{direct1h, btc5m, eth5m, rolling1h},
			pair(direct1h, rolling1h), pair(btc5m, eth5m), pair(eth5m, rolling1h)),
	}
	specs := []feature.Spec{ // dependents first, so Expand places them before their dependencies
		spec("pdiff", "pairs", pdiff()), spec("spread", "pairs", spread()),
		spec("ddiff", "", ddiff()), spec("ratio", "", ratio()), spec("diff", "", diff()),
		spec("sign", "", sign()), spec("sum", "", sum(3)), spec("last", "", last()),
	}
	instances, err := expand(st, universes, specs)
	if err != nil {
		t.Fatal(err)
	}

	const cols = 200
	seen := make(map[frame.State]int)
	for seed := uint64(1); seed <= 8; seed++ {
		buf := randomLegs(st, cols, rand.New(rand.NewPCG(seed, seed)))
		got, want := mustGraph(t, st, instances), mustGraph(t, st, instances)
		e, b := New(got, quietLog()), New(want, quietLog())

		for col := int64(0); col < cols; col++ {
			v := at(buf, col)
			e.Step(v)
			bruteStep(b, st, instances, v)

			for _, in := range instances {
				ring := got.ring(in.at)
				gs, gok, gl := readSlot(got.columns[in.id], ring, col)
				ws, wok, wl := readSlot(want.columns[in.id], ring, col)
				if gs != ws || gok != wok || gl != wl {
					t.Fatalf("seed %d, %s on %s at %d: Step %+v %t %d, brute force %+v %t %d",
						seed, in.id, coordinate(st, in.at), col, gs, gok, gl, ws, wok, wl)
				}
				if gok {
					seen[gs.State]++
				}
			}
		}
	}
	for _, s := range []frame.State{frame.Fresh, frame.Gap, frame.Ended} {
		if seen[s] == 0 {
			t.Errorf("no %s slot written: the legs do not exercise it", stateNames[s])
		}
	}
}

/*
randomLegs is a buffer over st with its test closes at every due column below cols, its legs drawn
from rng per stream: a start in the first quarter, an end in the second half for about half of them,
and a Gap at about one due column in ten between.
*/
func randomLegs(st *proto.Streams, cols int64, rng *rand.Rand) *frame.Buffer[proto.Candle] {
	buf := streamCandles(st, cols)
	for s, stream := range st.All {
		r, p := buf.Ring(s), stream.Period
		start := rng.Int64N(cols/4/p) * p
		r.Start(start)

		end := cols
		if rng.IntN(2) == 0 {
			end = (cols/2 + rng.Int64N(cols/2)) / p * p
		}
		for col := start + p; col < end; col += p {
			if rng.IntN(10) == 0 {
				r.Put(col, proto.Candle{}, frame.Gap)
			}
		}
		if end < cols {
			r.Put(end, proto.Candle{}, frame.Ended)
		}
	}
	return buf
}

// mustGraph is newGraph, failing the test on an error.
func mustGraph(t *testing.T, st *proto.Streams, instances []instance) *Graph {
	t.Helper()
	g, err := newGraph(st, instances)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

/*
bruteStep pins e at v's frame and applies the firing rules as defined, over instances rather than
e's schedules: it passes over them in reverse until each due at the column — its ring's period
dividing it — is decided, deciding one once its dependencies are. A due instance whose result has
ended is left alone; one with an ended leg is written Ended; one whose legs are Fresh and whose
dependencies are Fresh at the column fires; any other is written Gap. Legs are read from the candles,
and dependencies' states from their results.
*/
func bruteStep(e *Engine, st *proto.Streams, instances []instance, v frame.View[proto.Candle]) {
	type placed struct {
		id string
		at [2]int
	}
	e.pin(v)
	col := v.Col()

	legOf := func(s int) frame.State {
		_, state, age := v.Latest(s)
		if state == frame.Fresh && age != 0 {
			return frame.Gap // not updated at the column
		}
		return state
	}

	decided := make(map[placed]bool)
	for progress := true; progress; {
		progress = false
		for i := len(instances) - 1; i >= 0; i-- {
			in := instances[i]
			if decided[placed{in.id, in.at}] || col%ringShape(st, in.at, 1).Period != 0 {
				continue
			}
			var ends []int
			for _, s := range in.at {
				if s >= 0 {
					ends = append(ends, s)
				}
			}

			ready, depsFresh := true, true
			for _, id := range in.deps {
				for _, s := range ends {
					dep := placed{id, [2]int{s, -1}}
					ready = ready && decided[dep]
					sl, ok, _ := readSlot(e.g.columns[id], e.g.ring(dep.at), col)
					depsFresh = depsFresh && ok && sl.State == frame.Fresh
				}
			}
			if !ready {
				continue
			}
			decided[placed{in.id, in.at}], progress = true, true

			leg := frame.Fresh
			for _, s := range ends {
				switch l := legOf(s); {
				case l == frame.Ended:
					leg = frame.Ended
				case l == frame.Gap && leg == frame.Fresh:
					leg = frame.Gap
				}
			}

			c, ring := e.g.columns[in.id], e.g.ring(in.at)
			switch _, _, latest := readSlot(c, ring, col); {
			case latest == frame.Ended:
			case leg == frame.Ended:
				c.skip(ring, col, frame.Ended)
			case leg == frame.Fresh && depsFresh:
				c.fire(ring, col)
			default:
				c.skip(ring, col, frame.Gap)
			}
		}
	}
}

// readSlot reads column c's ring at col: the slot of col, its value boxed, false where the slot holds
// another column; and the state Latest reads there.
func readSlot(c column, ring int, col int64) (frame.Slot[any], bool, frame.State) {
	switch r := c.(type) {
	case *results[float64]:
		return readRing(r.buf.Ring(ring), col)
	case *results[int]:
		return readRing(r.buf.Ring(ring), col)
	}
	panic(fmt.Sprintf("column %T", c))
}

func readRing[T any](r *frame.Ring[T], col int64) (frame.Slot[any], bool, frame.State) {
	_, latest, _ := r.Latest(col)
	sl := r.At(col)
	if sl == nil {
		return frame.Slot[any]{}, false, latest
	}
	return frame.Slot[any]{Col: sl.Col, ValCol: sl.ValCol, Val: sl.Val, State: sl.State}, true, latest
}
