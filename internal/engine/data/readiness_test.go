package data

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"whatever/internal/flow/pipe"
	proto "whatever/internal/protocol"
)

// segment is a run of released columns, from..to inclusive, sharing one completeness.
type segment struct {
	from, to int64
	complete bool
}

// frames returns the Current frames of the segments over buf, in order.
func frames(buf *FrameBuffer, segs ...segment) []View {
	var vs []View
	for _, sg := range segs {
		for col := sg.from; col <= sg.to; col++ {
			vs = append(vs, buf.view(col, sg.complete))
		}
	}
	return vs
}

// stageAll runs a real align stage per direct stream over its candle columns and returns each
// stream's evaluates; the rings must hold every slot without waiting.
func stageAll(f fixture, cols map[int][]int64) [][]evaluate {
	seqs := make([][]evaluate, f.st.Directs)
	for s := range seqs {
		seqs[s] = settled(newAligner(f.buf, newGate(), s).run(feedLane(items(s, cols[s]...))))
	}
	return seqs
}

// orders returns arrival orders of the per-stream evaluates, each keeping every stream's own order:
// stream by stream, reversed, round-robin, and seeded random merges.
func orders(seqs [][]evaluate) [][]evaluate {
	var byStream, reversed, robin []evaluate
	for s := range seqs {
		byStream = append(byStream, seqs[s]...)
		reversed = append(reversed, seqs[len(seqs)-1-s]...)
	}
	for k := 0; ; k++ {
		n := len(robin)
		for _, seq := range seqs {
			if k < len(seq) {
				robin = append(robin, seq[k])
			}
		}
		if len(robin) == n {
			break
		}
	}

	out := [][]evaluate{byStream, reversed, robin}
	for seed := range uint64(20) {
		rng := rand.New(rand.NewPCG(seed, 7))
		next := make([]int, len(seqs))
		var merged []evaluate
		for len(merged) < len(byStream) {
			s := rng.IntN(len(seqs))
			if next[s] < len(seqs[s]) {
				merged = append(merged, seqs[s][next[s]])
				next[s]++
			}
		}
		out = append(out, merged)
	}
	return out
}

/*
TestReadiness covers release over evaluates staged by real align stages, under many arrival orders:
every order yields the same frames. Scenarios: aligned streams, a missing coarse bar, a gap proven by
the stream's own next candle, streams not yet started, lanes ending early, a stream that never
starts, and none starting.
*/
func TestReadiness(t *testing.T) {
	const d = day0
	f0 := newFixture(t)
	s5m, s1h, s1d := f0.idx(idBTC5m), f0.idx(idBTC1h), f0.idx(idETH1d)
	without := func(cols []int64, c int64) []int64 {
		return slices.DeleteFunc(slices.Clone(cols), func(x int64) bool { return x == c })
	}
	fiveMin, hourly, daily := span(d, d+288, 1), span(d, d+288, 12), []int64{d, d + 288}

	tests := []struct {
		name string
		cols map[int][]int64
		want []segment
	}{
		{"aligned",
			map[int][]int64{s5m: fiveMin, s1h: hourly, s1d: daily},
			[]segment{{d, d + 288, true}}},
		{"missing coarse bar",
			map[int][]int64{s5m: fiveMin, s1h: without(hourly, d+24), s1d: daily},
			[]segment{{d, d + 23, true}, {d + 24, d + 35, false}, {d + 36, d + 288, true}}},
		{"gap proven by the stream's next candle",
			map[int][]int64{s5m: without(fiveMin, d+50), s1h: hourly, s1d: daily},
			[]segment{{d, d + 49, true}, {d + 50, d + 50, false}, {d + 51, d + 288, true}}},
		{"streams not yet started",
			map[int][]int64{s5m: span(d+1, d+288, 1), s1h: span(d+12, d+288, 12), s1d: {d + 288}},
			[]segment{{d + 1, d + 287, false}, {d + 288, d + 288, true}}},
		{"lanes ending early", // 201…287 have no stream due
			map[int][]int64{s5m: span(d, d+200, 1), s1h: span(d, d+120, 12), s1d: daily},
			[]segment{{d, d + 288, true}}},
		{"a stream that never starts",
			map[int][]int64{s5m: fiveMin, s1h: hourly},
			[]segment{{d, d + 288, true}}},
		{"no stream starts", nil, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixtureWith(t, testSubs(), 20, 600)
			want := frames(f.buf, tt.want...)
			for i, order := range orders(stageAll(f, tt.cols)) {
				r := newReadiness(f.buf)
				var got []View
				for _, e := range order {
					got = r.step(e, got)
				}
				if !slices.Equal(got, want) {
					t.Fatalf("order %d: %d frames %v,\nwant %d", i, len(got), got, len(want))
				}
			}
		})
	}
}

/*
TestReadinessHold steps evaluates by hand: nothing is released before every stream reports, a coarse
stream is not waited on between its closes, decided columns wait behind an undecided one, and
columns with no stream due wait for a bar beyond them.
*/
func TestReadinessHold(t *testing.T) {
	const d = day0
	f := newFixture(t)
	s5m, s1h, s1d := f.idx(idBTC5m), f.idx(idBTC1h), f.idx(idETH1d)
	for _, s := range []int{s5m, s1h, s1d} {
		f.buf.start(s, d)
	}
	f.fresh(s5m, span(d, d+20, 1)...)
	f.buf.put(s5m, d+21, candle(s5m, d+21), Ended, 0)
	f.fresh(s1h, d, d+12, d+24)
	f.fresh(s1d, d)

	r := newReadiness(f.buf)
	steps := []struct {
		name string
		in   []evaluate
		want []View
	}{
		{"1d has not reported",
			append(evals(s5m, Fresh, span(d, d+11, 1)...), evals(s1h, Fresh, d)...), nil},
		{"1h is not waited on between closes",
			evals(s1d, Fresh, d), frames(f.buf, segment{d, d + 11, true})},
		{"decided columns wait behind 1h's next close",
			evals(s5m, Fresh, span(d+12, d+20, 1)...), nil},
		{"1h's close releases them at once",
			evals(s1h, Fresh, d+12), frames(f.buf, segment{d + 12, d + 20, true})},
		{"no stream due at 21…23: they wait for a bar beyond them",
			evals(s5m, Ended, d+21), nil},
		{"1h's next close reaches them",
			evals(s1h, Fresh, d+24), frames(f.buf, segment{d + 21, d + 24, true})},
	}
	for _, st := range steps {
		var got []View
		for _, e := range st.in {
			got = r.step(e, got)
		}
		if !slices.Equal(got, st.want) {
			t.Fatalf("%s: frames %v, want %v", st.name, got, st.want)
		}
	}
}

// TestReadinessRun covers the stage: it emits step's frames in order and closes once its input does.
func TestReadinessRun(t *testing.T) {
	const d = day0
	f := newFixtureWith(t, testSubs(), 20, 600)
	s5m, s1h, s1d := f.idx(idBTC5m), f.idx(idBTC1h), f.idx(idETH1d)
	seqs := stageAll(f, map[int][]int64{s5m: span(d, d+24, 1), s1h: span(d, d+24, 12), s1d: {d}})

	in := make(chan evaluate, 100)
	for _, e := range orders(seqs)[2] { // round-robin
		in <- e
	}
	close(in)

	var got []View
	for v := range newReadiness(f.buf).run(in) {
		got = append(got, v)
	}
	if want := frames(f.buf, segment{d, d + 24, true}); !slices.Equal(got, want) {
		t.Fatalf("frames %v, want %v", got, want)
	}
}

/*
BenchmarkReadiness measures align → pipe.FanIn → readiness per released column, with n direct 1m
streams on one grid. Each align stage's lane is filled and closed before the timer starts; the loop
consumes every event as the engine would. ns/evaluate is the time per column divided by n.
*/
func BenchmarkReadiness(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			subs := make([]proto.Subscription, n)
			for i := range subs {
				subs[i] = sub(fmt.Sprintf("S%04d", i), proto.Timeframe1m, "bench")
			}
			buf, err := newFrameBuffer(NewStreams(subs), 1, defaultHeadroom)
			if err != nil {
				b.Fatal(err)
			}

			per := int64(b.N) + 1
			lanes := make([]chan laneItem, n)
			for s := range lanes {
				lanes[s] = make(chan laneItem, per)
				for col := int64(1); col <= per; col++ {
					lanes[s] <- laneItem{col: col, c: candle(s, col)}
				}
				close(lanes[s])
			}

			b.ReportAllocs()
			b.ResetTimer()
			g := newGate()
			evs := make([]<-chan evaluate, n)
			for s := range evs {
				evs[s] = newAligner(buf, g, s).run(lanes[s])
			}
			out := newReadiness(buf).run(pipe.FanIn(evs...))
			for range b.N {
				g.consume((<-out).Col())
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/evaluate")

			for v := range out { // drain so every goroutine exits
				g.consume(v.Col())
			}
		})
	}
}
