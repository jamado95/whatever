package data

import (
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
	st := NewStreams(testSubs())
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

		lanes := replayPipeline(st, quiet()).run(ins)
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
		if lanes := replayPipeline(st, quiet()).run(nil); len(lanes) != 0 {
			t.Fatalf("%d lanes, want none", len(lanes))
		}
	})
}

/*
TestPipelineLanesIndependent covers per-stream blocking: a lane nobody reads stops its own provider
once its buffer is full, while the other lanes run to the end.
*/
func TestPipelineLanesIndependent(t *testing.T) {
	st := NewStreams(testSubs())
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

	lanes := replayPipeline(st, quiet()).run(ins)
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
