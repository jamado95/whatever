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
TestPipelineOrder covers the replay pipeline end to end: lanes of mixed timeframes merge in (column,
stream) order, each lane's end marker lands among its column's items by stream index, and an empty
pipeline closes.
*/
func TestPipelineOrder(t *testing.T) {
	st := NewStreams(testSubs())
	s1h, s5m, s1d := mustIndex(t, st, idBTC1h), mustIndex(t, st, idBTC5m), mustIndex(t, st, idETH1d)

	t.Run("mixed timeframes", func(t *testing.T) {
		// a day of 5m bars with a gap at day0+50; 1h bars ending at 11:00; one 1d bar
		cols := map[int][]int64{
			s5m: slices.DeleteFunc(span(day0+1, day0+288, 1), func(c int64) bool { return c == day0+50 }),
			s1h: span(day0+12, day0+132, 12),
			s1d: {day0 + 288},
		}
		ins := make([]<-chan proto.MarketData, st.Directs)
		var want []laneItem
		for s, cs := range cols {
			ins[s] = feedAll(bars(st, s, cs...))
			for _, c := range cs {
				want = append(want, laneItem{col: c, stream: s, c: bar(st, s, c)})
			}
		}
		want = append(want,
			laneItem{col: day0 + 289, stream: s5m, end: true},
			laneItem{col: day0 + 144, stream: s1h, end: true}, // 12:00, beside the 5m bar there
			laneItem{col: day0 + 576, stream: s1d, end: true},
		)
		slices.SortFunc(want, func(a, b laneItem) int {
			if before(a, b) {
				return -1
			}
			return 1
		})

		var got []laneItem
		for it := range replayPipeline(st, quiet()).run(ins) {
			got = append(got, it)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("got %d items, want %d; first difference at %d",
				len(got), len(want), firstDiff(got, want))
		}
	})

	t.Run("no direct streams", func(t *testing.T) {
		if _, ok := <-replayPipeline(st, quiet()).run(nil); ok {
			t.Fatal("output did not close")
		}
	})
}

func firstDiff(a, b []laneItem) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}

/*
TestPipelineBackpressure covers replay skew: a coarse lane fed as fast as it can go blocks its
provider while its next bar waits in the merge, so it runs at most its buffers ahead of the slowest
lane in data time.
*/
func TestPipelineBackpressure(t *testing.T) {
	st := NewStreams(testSubs())
	s1h, s5m, s1d := mustIndex(t, st, idBTC1h), mustIndex(t, st, idBTC5m), mustIndex(t, st, idETH1d)

	fine := make(chan proto.MarketData)
	coarse := make(chan proto.MarketData)
	ins := make([]<-chan proto.MarketData, st.Directs)
	ins[s5m], ins[s1h], ins[s1d] = fine, coarse, feedAll(nil)

	var sent atomic.Int64
	go func() {
		defer close(coarse)
		for k := int64(1); k <= 1000; k++ {
			coarse <- proto.MarketData{Candle: bar(st, s1h, day0+12*k)}
			sent.Add(1)
		}
	}()

	merged := replayPipeline(st, quiet()).run(ins)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range merged {
		}
	}()

	const last = day0 + 60 // 05:00
	for c := int64(day0 + 1); c <= last; c++ {
		fine <- proto.MarketData{Candle: bar(st, s5m, c)}
	}
	time.Sleep(50 * time.Millisecond)

	// 1h bars due by 05:00, one pending in the merge, the lane's buffer, one held by the lane
	bound := int64(5 + 1 + laneBuffer + 1)
	if n := sent.Load(); n > bound {
		t.Fatalf("coarse provider sent %d bars, want at most %d", n, bound)
	}

	close(fine)
	<-done
	if n := sent.Load(); n != 1000 {
		t.Fatalf("coarse provider sent %d bars after the fine lane ended, want 1000", n)
	}
}
