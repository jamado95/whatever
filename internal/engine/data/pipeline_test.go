package data

import (
	"math"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	proto "whatever/internal/protocol"
)

// feedAll returns a closed channel holding cs as provider output.
func feedAll(cs []proto.Candle) <-chan proto.MarketData {
	ch := make(chan proto.MarketData, len(cs))
	for _, c := range cs {
		ch <- proto.MarketData{Candle: c}
	}
	close(ch)
	return ch
}

/*
TestPipelineLanes covers the composer: one lane per direct stream, each carrying exactly its own
stream's accepted candles in order before closing; no direct streams, no lanes.
*/
func TestPipelineLanes(t *testing.T) {
	f := newFixture(t)
	st := f.st
	s1h, s5m, s1d := mustIndex(t, st, idBTC1h), mustIndex(t, st, idBTC5m), mustIndex(t, st, idETH1d)

	t.Run("one lane per direct stream", func(t *testing.T) {
		// a day of 5m bars with a gap at day0+50 and an invalid bar at day0+60; 1h bars ending at
		// 11:00; one 1d bar
		fiveMin := slices.DeleteFunc(span(day0+1, day0+288, 1), func(c int64) bool { return c == day0+50 })
		cols := map[int][]int64{
			s5m: fiveMin,
			s1h: span(day0+12, day0+132, 12),
			s1d: {day0 + 288},
		}
		ins := make([]<-chan proto.MarketData, st.Directs)
		want := make([][]laneItem, st.Directs)
		for s, cs := range cols {
			in := bars(st, s, cs...)
			for i, c := range cs {
				if s == s5m && c == day0+60 {
					in[i].Volume = -1
					continue
				}
				want[s] = append(want[s], laneItem{col: c, c: bar(st, s, c)})
			}
			ins[s] = feedAll(in)
		}

		lanes := replayPipeline(f.buf, newGate(), quiet()).lanes(ins)
		if len(lanes) != st.Directs {
			t.Fatalf("%d lanes, want %d", len(lanes), st.Directs)
		}
		for s, lane := range lanes {
			var got []laneItem
			for it := range lane {
				got = append(got, it)
			}
			if !slices.Equal(got, want[s]) {
				t.Fatalf("lane %d: got %d items, want %d", s, len(got), len(want[s]))
			}
		}
	})

	t.Run("no direct streams", func(t *testing.T) {
		if lanes := replayPipeline(f.buf, newGate(), quiet()).lanes(nil); len(lanes) != 0 {
			t.Fatalf("%d lanes, want none", len(lanes))
		}
	})
}

/*
TestPipelineLanesIndependent covers per-stream blocking: a lane nobody reads stops its own provider
once its buffer is full, while the other lanes run to the end.
*/
func TestPipelineLanesIndependent(t *testing.T) {
	f := newFixture(t)
	st := f.st
	s1h, s5m, s1d := mustIndex(t, st, idBTC1h), mustIndex(t, st, idBTC5m), mustIndex(t, st, idETH1d)

	coarse := make(chan proto.MarketData)
	ins := make([]<-chan proto.MarketData, st.Directs)
	ins[s1h] = coarse
	ins[s5m] = feedAll(bars(st, s5m, span(day0+1, day0+288, 1)...))
	ins[s1d] = feedAll(bars(st, s1d, day0+288))

	var sent atomic.Int64
	go func() {
		defer close(coarse)
		for k := int64(1); k <= 1000; k++ {
			coarse <- proto.MarketData{Candle: bar(st, s1h, day0+12*k)}
			sent.Add(1)
		}
	}()

	lanes := replayPipeline(f.buf, newGate(), quiet()).lanes(ins)
	for _, s := range []int{s5m, s1d} {
		n := 0
		for range lanes[s] {
			n++
		}
		if want := map[int]int{s5m: 288, s1d: 1}[s]; n != want {
			t.Fatalf("lane %d delivered %d candles, want %d", s, n, want)
		}
	}

	time.Sleep(50 * time.Millisecond)
	// the full lane buffer, plus the candle held by the normalise goroutine
	if n, want := sent.Load(), int64(laneBuffer+1); n != want {
		t.Fatalf("unread lane's provider sent %d bars, want %d", n, want)
	}

	for range lanes[s1h] { // drain so every goroutine exits
	}
	if n := sent.Load(); n != 1000 {
		t.Fatalf("provider sent %d bars after its lane was drained, want 1000", n)
	}
}

// throttled returns cs as provider output from a goroutine that pauses every 50 candles.
func throttled(cs []proto.Candle) <-chan proto.MarketData {
	ch := make(chan proto.MarketData)
	go func() {
		defer close(ch)
		for i, c := range cs {
			if i%50 == 0 {
				time.Sleep(time.Millisecond)
			}
			ch <- proto.MarketData{Candle: c}
		}
	}()
	return ch
}

// consumeAll reads every frame, consuming each column as the engine would; a stall of 10 s fails.
func consumeAll(t *testing.T, g *gate, frames <-chan View) []View {
	t.Helper()
	var got []View
	deadline := time.After(10 * time.Second)
	for {
		select {
		case v, ok := <-frames:
			if !ok {
				return got
			}
			got = append(got, v)
			g.consume(v.Col())
		case <-deadline:
			t.Fatalf("stalled after %d events", len(got))
		}
	}
}

/*
model computes the frames expected from each stream's accepted columns by brute force: every column
from the earliest first candle to the latest candle, Complete when every stream that has started and
not ended has a candle at its latest due column.
*/
func model(buf *FrameBuffer, accepted map[int][]int64) []View {
	st := buf.streams
	first, last := int64(math.MaxInt64), int64(math.MinInt64)
	sets := make(map[int]map[int64]bool)
	for s, cs := range accepted {
		if len(cs) == 0 {
			continue
		}
		first, last = min(first, cs[0]), max(last, cs[len(cs)-1])
		sets[s] = make(map[int64]bool)
		for _, c := range cs {
			sets[s][c] = true
		}
	}

	var vs []View
	for col := first; col <= last; col++ {
		complete := true
		for s, cs := range accepted {
			if len(cs) == 0 {
				continue
			}
			p := st.All[s].Period
			switch {
			case col < cs[0]: // not started
				complete = false
			case col >= cs[len(cs)-1]+p: // ended
			case !sets[s][col-col%p]:
				complete = false
			}
		}
		vs = append(vs, buf.view(col, complete))
	}
	return vs
}

/*
TestPipelineRun covers the replay pipeline end to end with headroom 4: a day of 5m bars with a gap
and an invalid bar, 1h bars ending at 11:00 and two 1d bars, each frame consumed as it arrives.
Every column is released once, in order, with the completeness of a brute-force model.
*/
func TestPipelineRun(t *testing.T) {
	const d = day0
	f := newFixture(t)
	s1h, s5m, s1d := f.idx(idBTC1h), f.idx(idBTC5m), f.idx(idETH1d)

	fiveMin := slices.DeleteFunc(span(d+1, d+288, 1), func(c int64) bool { return c == d+50 })
	in5m := bars(f.st, s5m, fiveMin...)
	in5m[slices.Index(fiveMin, d+60)].Volume = -1 // invalid: dropped by normalise, then a Gap

	ins := make([]<-chan proto.MarketData, f.st.Directs)
	ins[s5m] = feedAll(in5m)
	ins[s1h] = feedAll(bars(f.st, s1h, span(d+12, d+132, 12)...))
	ins[s1d] = feedAll(bars(f.st, s1d, d, d+288))

	g := newGate()
	got := consumeAll(t, g, replayPipeline(f.buf, g, quiet()).run(ins))
	want := model(f.buf, map[int][]int64{
		s5m: slices.DeleteFunc(fiveMin, func(c int64) bool { return c == d+60 }),
		s1h: span(d+12, d+132, 12),
		s1d: {d, d + 288},
	})
	if !slices.Equal(got, want) {
		t.Fatalf("%d frames %v,\nwant %d %v", len(got), got, len(want), want)
	}
}

/*
TestPipelineSkew covers per-stream blocking under skew with headroom 4: ten days of 5m, 1h and 1d
bars, the coarse lanes fed at once and the 5m lane throttled, then the reverse. Both runs release
every column once, in order, with the same frames, and neither stalls.
*/
func TestPipelineSkew(t *testing.T) {
	const d = day0
	st := NewStreams(testSubs())
	s1h, s5m, s1d := mustIndex(t, st, idBTC1h), mustIndex(t, st, idBTC5m), mustIndex(t, st, idETH1d)
	cols := map[int][]int64{
		s5m: span(d+1, d+2880, 1),
		s1h: span(d+12, d+2880, 12),
		s1d: span(d+288, d+2880, 288),
	}

	for _, slow := range [][]int{{s5m}, {s1h, s1d}} {
		f := newFixture(t)
		want := model(f.buf, cols)
		ins := make([]<-chan proto.MarketData, f.st.Directs)
		for s, cs := range cols {
			if slices.Contains(slow, s) {
				ins[s] = throttled(bars(f.st, s, cs...))
			} else {
				ins[s] = feedAll(bars(f.st, s, cs...))
			}
		}

		g := newGate()
		if got := consumeAll(t, g, replayPipeline(f.buf, g, quiet()).run(ins)); !slices.Equal(got, want) {
			t.Fatalf("slow lanes %v: %d frames, want %d", slow, len(got), len(want))
		}
	}
}
