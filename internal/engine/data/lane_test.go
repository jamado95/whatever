package data

import (
	"context"
	"slices"
	"testing"
	"time"
)

/*
TestLane covers a lane's chain: run under a context, each lane returns exactly its own stream's
evaluates — Fresh at the accepted columns, Gap where a candle is missing or dropped, then Ended one
period after the last — and closes. Its provider starts once and is closed only once the context is
cancelled.
*/
func TestLane(t *testing.T) {
	const d = day0
	st := NewStreams(testSubs())
	s1h, s5m, s1d := mustIndex(t, st, idBTC1h), mustIndex(t, st, idBTC5m), mustIndex(t, st, idETH1d)
	fs, provs := fakes(st)

	// a day of 5m bars with a gap at day0+50 and an invalid bar at day0+60; 1h bars ending at 11:00;
	// one 1d bar
	fiveMin := slices.DeleteFunc(span(d+1, d+288, 1), func(c int64) bool { return c == d+50 })
	fs[s5m].candles = bars(st, s5m, fiveMin...)
	fs[s5m].candles[slices.Index(fiveMin, d+60)].Volume = -1
	fs[s1h].candles = bars(st, s1h, span(d+12, d+132, 12)...)
	fs[s1d].candles = bars(st, s1d, d+288)

	want := map[int][]evaluate{
		s1h: append(evals(s1h, Fresh, span(d+12, d+132, 12)...), evals(s1h, Ended, d+144)...),
		s1d: append(evals(s1d, Fresh, d+288), evals(s1d, Ended, d+576)...),
	}
	for col := int64(d + 1); col <= d+288; col++ {
		state := Fresh
		if col == d+50 || col == d+60 {
			state = Gap
		}
		want[s5m] = append(want[s5m], evaluate{stream: s5m, col: col, state: state})
	}
	want[s5m] = append(want[s5m], evals(s5m, Ended, d+289)...)

	l, err := newLayer(provs, 20, 600, quiet())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	for s, ln := range l.lanes {
		evs, errs, err := ln.run(ctx)
		if err != nil {
			t.Fatalf("lane %d: %v", s, err)
		}
		var got []evaluate
		for e := range evs {
			got = append(got, e)
		}
		for range errs {
		}
		if !slices.Equal(got, want[s]) {
			t.Errorf("lane %d: evaluates %v,\nwant %v", s, got, want[s])
		}
		if n, c := fs[s].starts.Load(), fs[s].closes.Load(); n != 1 || c != 0 {
			t.Errorf("lane %d: provider started %d and closed %d times, want 1 and 0", s, n, c)
		}
	}

	cancel()
	l.closing.Wait()
	for s, f := range fs {
		if n := f.closes.Load(); n != 1 {
			t.Errorf("lane %d: provider closed %d times after cancel, want 1", s, n)
		}
	}
}

/*
TestLaneIndependent covers per-stream blocking: a lane nobody reads stops its own provider once its
stages' buffers are full, while the other lanes run to their end.
*/
func TestLaneIndependent(t *testing.T) {
	const d = day0
	st := NewStreams(testSubs())
	s1h, s5m, s1d := mustIndex(t, st, idBTC1h), mustIndex(t, st, idBTC5m), mustIndex(t, st, idETH1d)
	fs, provs := fakes(st)
	fs[s1h].candles = bars(st, s1h, span(d+12, d+12*1000, 12)...)
	fs[s5m].candles = bars(st, s5m, span(d+1, d+288, 1)...)
	fs[s1d].candles = bars(st, s1d, d+288)

	l, err := newLayer(provs, 20, 1200, quiet()) // the 1h ring (126 slots) never makes align wait
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		l.closing.Wait()
	}()

	evs := make([]<-chan evaluate, len(l.lanes))
	for s, ln := range l.lanes {
		if evs[s], _, err = ln.run(ctx); err != nil {
			t.Fatalf("lane %d: %v", s, err)
		}
	}
	for s, want := range map[int]int{s5m: 289, s1d: 2} { // Fresh each, then Ended
		n := 0
		for range evs[s] {
			n++
		}
		if n != want {
			t.Fatalf("lane %d delivered %d evaluates, want %d", s, n, want)
		}
	}

	time.Sleep(50 * time.Millisecond)
	// the provider's full output, normalise's held candle and full output, align's held item and
	// full output
	if n, want := fs[s1h].sent.Load(), int64(fakeBuffer+2*laneBuffer+2); n != want {
		t.Fatalf("unread lane's provider sent %d bars, want %d", n, want)
	}

	for e := range evs[s1h] { // drain, consuming as the engine would, so every goroutine exits
		l.gate.consume(e.col)
	}
	if n := fs[s1h].sent.Load(); n != 1000 {
		t.Fatalf("provider sent %d bars after its lane was drained, want 1000", n)
	}
}
