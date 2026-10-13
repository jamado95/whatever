package stream

import (
	"strings"
	"testing"

	proto "whatever/internal/protocol"
)

// sub is a test helper that creates a Subscription. Provider is always non-empty
// so NowFwdFill keys are distinct and comparable.
func sub(symbol string, tf proto.Timeframe) proto.Subscription {
	return proto.Subscription{Symbol: symbol, Timeframe: tf, Provider: "test"}
}

// emd constructs a stub ExtendedMarketData carrying the given subscription and
// the given CloseTs. The Candle is empty except for CloseTs.
func emd(sub proto.Subscription, closeTs int64) proto.ExtendedMarketData {
	return proto.ExtendedMarketData{
		MarketData: proto.MarketData{
			Subscription: sub,
			Candle:       proto.Candle{CloseTs: closeTs},
		},
	}
}

// collect drains a SyncEvent channel into a slice.
func collect(ch <-chan proto.SyncEvent) []proto.SyncEvent {
	var events []proto.SyncEvent
	for ev := range ch {
		events = append(events, ev)
	}
	return events
}

// ordered creates a buffered channel and sends the given EMDs in order, then
// closes it. Returns a receive-only channel so the call site is clearly the
// data source.
func ordered(emds ...proto.ExtendedMarketData) <-chan proto.ExtendedMarketData {
	ch := make(chan proto.ExtendedMarketData, len(emds))
	for _, e := range emds {
		ch <- e
	}
	close(ch)
	return ch
}

// nowHas reports whether Now contains a leg with the given subscription and
// CloseTs — verifying the carried value, not just the count.
func nowHas(now []proto.ExtendedMarketData, s proto.Subscription, ts int64) bool {
	for _, e := range now {
		if e.Subscription == s && e.Timestamp() == ts {
			return true
		}
	}
	return false
}

// fwdFillAt returns the CloseTs NowFwdFill holds for a subscription (its
// forward-filled value). ok=false when the key is absent.
func fwdFillAt(fwd map[proto.Subscription]proto.ExtendedMarketData, s proto.Subscription) (int64, bool) {
	e, ok := fwd[s]
	if !ok {
		return 0, false
	}
	return e.Timestamp(), true
}

func TestSynchronizeSingleLane(t *testing.T) {
	s1m := sub("BTCUSDT", proto.Timeframe1m)

	ch := ordered(
		emd(s1m, 100),
		emd(s1m, 200),
		emd(s1m, 300),
	)

	events := collect(synchronize(ch))
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3", len(events))
	}
	wantTs := []int64{100, 200, 300}
	for i, ev := range events {
		if ev.Time != wantTs[i] {
			t.Errorf("event[%d]: Time = %d, want %d", i, ev.Time, wantTs[i])
		}
		if len(ev.Now) != 1 || !nowHas(ev.Now, s1m, wantTs[i]) {
			t.Errorf("event[%d]: Now = %+v, want lone s1m@%d", i, ev.Now, wantTs[i])
		}
		if len(ev.NowFwdFill) != 1 {
			t.Errorf("event[%d]: NowFwdFill len = %d, want 1", i, len(ev.NowFwdFill))
		}
		if v, ok := fwdFillAt(ev.NowFwdFill, s1m); !ok || v != wantTs[i] {
			t.Errorf("event[%d]: NowFwdFill[s1m] = (%d, %v), want (%d, true)", i, v, ok, wantTs[i])
		}
	}
}

func TestSynchronizeCoClose(t *testing.T) {
	s1m := sub("BTCUSDT", proto.Timeframe1m)
	s5m := sub("BTCUSDT", proto.Timeframe5m)

	ch := ordered(
		emd(s1m, 100),
		emd(s5m, 100),
	)

	events := collect(synchronize(ch))
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	ev := events[0]
	if ev.Time != 100 {
		t.Errorf("Time = %d, want 100", ev.Time)
	}
	if len(ev.Now) != 2 {
		t.Errorf("Now len = %d, want 2", len(ev.Now))
	}
	// Now must carry each leg with its real value.
	if !nowHas(ev.Now, s1m, 100) {
		t.Errorf("Now missing s1m@100: %+v", ev.Now)
	}
	if !nowHas(ev.Now, s5m, 100) {
		t.Errorf("Now missing s5m@100: %+v", ev.Now)
	}
	// Each subscription forward-filled to its CloseTs at this boundary.
	for _, s := range []proto.Subscription{s1m, s5m} {
		if v, ok := fwdFillAt(ev.NowFwdFill, s); !ok || v != 100 {
			t.Errorf("NowFwdFill[%v] = (%d, %v), want (100, true)", s, v, ok)
		}
	}
}

func TestSynchronizeNowSubsetOfNowFwdFill(t *testing.T) {
	s1m := sub("BTCUSDT", proto.Timeframe1m)
	s1h := sub("BTCUSDT", proto.Timeframe1h)

	// Co-close at 100, then 1m closes at 200 — 1h forward-filled.
	ch := ordered(
		emd(s1m, 100),
		emd(s1h, 100),
		emd(s1m, 200),
	)

	events := collect(synchronize(ch))
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2, times: %v", len(events), eventTimes(events))
	}

	// First event: both close at 100.
	e0 := events[0]
	if e0.Time != 100 {
		t.Errorf("event[0].Time = %d, want 100", e0.Time)
	}
	if !nowHas(e0.Now, s1m, 100) || !nowHas(e0.Now, s1h, 100) {
		t.Errorf("event[0]: Now should carry s1m@100 and s1h@100: %+v", e0.Now)
	}

	// Second event: only 1m closes at 200; 1h forward-filled.
	e1 := events[1]
	if e1.Time != 200 {
		t.Errorf("event[1].Time = %d, want 200", e1.Time)
	}
	if !nowHas(e1.Now, s1m, 200) {
		t.Errorf("event[1]: Now should carry s1m@200: %+v", e1.Now)
	}
	if len(e1.Now) != 1 {
		t.Errorf("event[1]: Now len = %d, want 1 (only 1m)", len(e1.Now))
	}
	// Forward-fill carries the real values: 1m at its 200 close, 1h held at 100.
	if v, ok := fwdFillAt(e1.NowFwdFill, s1m); !ok || v != 200 {
		t.Errorf("event[1]: NowFwdFill[s1m] = (%d, %v), want (200, true)", v, ok)
	}
	if v, ok := fwdFillAt(e1.NowFwdFill, s1h); !ok || v != 100 {
		t.Errorf("event[1]: NowFwdFill[s1h] = (%d, %v), want (100, true) forward-filled", v, ok)
	}
	// Now ⊆ NowFwdFill for every event (each Now value equals the held value).
	for _, e := range events {
		for _, leg := range e.Now {
			if legheld, ok := fwdFillAt(e.NowFwdFill, leg.Subscription); !ok || legheld != leg.Timestamp() {
				t.Errorf("Time=%d: Now leg %v@%d not matching NowFwdFill value %d (present %v)", e.Time, leg.Subscription, leg.Timestamp(), legheld, ok)
			}
		}
	}
}

func TestSynchronizeForwardFillAcrossMultipleCloses(t *testing.T) {
	s1m := sub("BTCUSDT", proto.Timeframe1m)
	s1h := sub("BTCUSDT", proto.Timeframe1h)

	// 1h closes at 0, then 1m closes at 100, 200, 300. 1h forward-filled.
	ch := ordered(
		emd(s1h, 0),
		emd(s1m, 100),
		emd(s1m, 200),
		emd(s1m, 300),
	)

	events := collect(synchronize(ch))
	if len(events) != 4 {
		t.Fatalf("got %d events, want 4", len(events))
	}
	// Event 0: 1h at t=0, only leg.
	if events[0].Time != 0 || !nowHas(events[0].Now, s1h, 0) {
		t.Errorf("event[0]: Time=%d Now=%+v, want a lone s1h@0 leg", events[0].Time, events[0].Now)
	}
	// Events 1-3: 1m-only, 1h forward-filled to its last close (t=0).
	want1m := []int64{100, 200, 300}
	for i := 1; i < 4; i++ {
		e := events[i]
		if e.Time != want1m[i-1] {
			t.Errorf("event[%d]: Time=%d, want %d", i, e.Time, want1m[i-1])
		}
		if !nowHas(e.Now, s1m, want1m[i-1]) || len(e.Now) != 1 {
			t.Errorf("event[%d]: Now = %+v, want lone s1m@%d", i, e.Now, want1m[i-1])
		}
		// 1h must stay at its last close (0), never regressing.
		if v, ok := fwdFillAt(e.NowFwdFill, s1h); !ok || v != 0 {
			t.Errorf("event[%d]: NowFwdFill[s1h] = (%d, %v), want (0, true)", i, v, ok)
		}
		// The advancing 1m leg is forward-filled to its own latest close.
		if v, ok := fwdFillAt(e.NowFwdFill, s1m); !ok || v != want1m[i-1] {
			t.Errorf("event[%d]: NowFwdFill[s1m] = (%d, %v), want (%d, true)", i, v, ok, want1m[i-1])
		}
	}
}

func TestSynchronizeFinalFlushOnClose(t *testing.T) {
	s1m := sub("BTCUSDT", proto.Timeframe1m)

	ch := ordered(emd(s1m, 100))

	events := collect(synchronize(ch))
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1 (final flush)", len(events))
	}
	if events[0].Time != 100 {
		t.Errorf("final event Time = %d, want 100", events[0].Time)
	}
}

func TestSynchronizeEmptyInput(t *testing.T) {
	ch := ordered() // empty, immediately closed
	events := collect(synchronize(ch))
	if len(events) != 0 {
		t.Fatalf("got %d events, want 0", len(events))
	}
}

func TestSynchronizeOutOfOrderArrival(t *testing.T) {
	s1m := sub("BTCUSDT", proto.Timeframe1m)

	// 200 arrives first, then 100 (earlier), then 300.
	ch := ordered(
		emd(s1m, 200),
		emd(s1m, 100),
		emd(s1m, 300),
	)

	events := collect(synchronize(ch))
	// 200 forms the current group; 100 arrives out-of-order → emitted immediately
	// as a singleton at the current boundary. Then 300 flushes the pending 200
	// group. The out-of-order leg must NOT regress the forward-fill.
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3", len(events))
	}
	// Out-of-order singleton: Time is the current boundary (200), and the
	// forward-fill still holds the newer 200 — not the older 100.
	if events[0].Time != 200 {
		t.Errorf("event[0].Time = %d, want 200 (boundary at arrival)", events[0].Time)
	}
	if !nowHas(events[0].Now, s1m, 100) {
		t.Errorf("event[0]: Now should carry the out-of-order s1m@100: %+v", events[0].Now)
	}
	if v, ok := fwdFillAt(events[0].NowFwdFill, s1m); !ok || v != 200 {
		t.Errorf("event[0]: NowFwdFill[s1m] = (%d, %v), want (200, true) — must not regress to 100", v, ok)
	}
	// Pending 200 group flushed by the 300 advance.
	if events[1].Time != 200 || !nowHas(events[1].Now, s1m, 200) {
		t.Errorf("event[1]: Time=%d Now=%+v, want 200 carrying s1m@200", events[1].Time, events[1].Now)
	}
	// Final leg.
	if events[2].Time != 300 || !nowHas(events[2].Now, s1m, 300) {
		t.Errorf("event[2]: Time=%d Now=%+v, want 300 carrying s1m@300", events[2].Time, events[2].Now)
	}
	if v, ok := fwdFillAt(events[2].NowFwdFill, s1m); !ok || v != 300 {
		t.Errorf("event[2]: NowFwdFill[s1m] = (%d, %v), want (300, true)", v, ok)
	}
}

func TestSynchronizeSeparateSymbolsCoClose(t *testing.T) {
	btc1m := sub("BTCUSDT", proto.Timeframe1m)
	eth1m := sub("ETHUSDT", proto.Timeframe1m)

	ch := ordered(
		emd(btc1m, 100),
		emd(eth1m, 100),
	)

	events := collect(synchronize(ch))
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	ev := events[0]
	if !nowHas(ev.Now, btc1m, 100) || !nowHas(ev.Now, eth1m, 100) {
		t.Errorf("Now = %+v, want both BTC and ETH legs at 100", ev.Now)
	}
	if v, ok := fwdFillAt(ev.NowFwdFill, btc1m); !ok || v != 100 {
		t.Errorf("NowFwdFill[btc] = (%d, %v), want (100, true)", v, ok)
	}
	if v, ok := fwdFillAt(ev.NowFwdFill, eth1m); !ok || v != 100 {
		t.Errorf("NowFwdFill[eth] = (%d, %v), want (100, true)", v, ok)
	}
}

func TestSubscriptionIDKeyDistinction(t *testing.T) {
	// Same symbol+timeframe, different providers → distinct subscriptions.
	sA := proto.Subscription{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1m, Provider: "a"}
	sB := proto.Subscription{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1m, Provider: "b"}

	ch := ordered(
		emd(sA, 100),
		emd(sB, 100),
	)

	events := collect(synchronize(ch))
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	// Both distinct subscriptions present, each forward-filled to its own value.
	if len(events[0].NowFwdFill) != 2 {
		t.Errorf("NowFwdFill len = %d, want 2 (distinct providers = distinct keys)", len(events[0].NowFwdFill))
	}
	for _, s := range []proto.Subscription{sA, sB} {
		if v, ok := fwdFillAt(events[0].NowFwdFill, s); !ok || v != 100 {
			t.Errorf("NowFwdFill[%v] = (%d, %v), want (100, true) — distinct by Provider", s, v, ok)
		}
	}
	// Both co-closing legs carried in Now.
	if !nowHas(events[0].Now, sA, 100) || !nowHas(events[0].Now, sB, 100) {
		t.Errorf("Now = %+v, want both sA@sB legs at 100", events[0].Now)
	}
}

// TestNowFwdFillShallowCopy verifies that mutating a received NowFwdFill entry
// does not corrupt the synchronizer's internal state. The map is a snapshot.
func TestNowFwdFillIsSnapshot(t *testing.T) {
	s1m := sub("BTCUSDT", proto.Timeframe1m)
	s1h := sub("BTCUSDT", proto.Timeframe1h)

	ch := ordered(
		emd(s1h, 0),
		emd(s1m, 100),
	)

	events := collect(synchronize(ch))
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}

	e0, e1 := events[0], events[1]
	// Check carried values before mutation.
	if v, ok := fwdFillAt(e0.NowFwdFill, s1h); !ok || v != 0 {
		t.Errorf("event[0]: NowFwdFill[s1h] = (%d, %v), want (0, true)", v, ok)
	}
	if v, ok := fwdFillAt(e1.NowFwdFill, s1h); !ok || v != 0 {
		t.Errorf("event[1]: NowFwdFill[s1h] = (%d, %v), want (0, true)", v, ok)
	}
	if v, ok := fwdFillAt(e1.NowFwdFill, s1m); !ok || v != 100 {
		t.Errorf("event[1]: NowFwdFill[s1m] = (%d, %v), want (100, true)", v, ok)
	}

	// Mutate the first event's NowFwdFill — second event must be unaffected.
	e0.NowFwdFill[proto.Subscription{}] = proto.ExtendedMarketData{}
	e1.NowFwdFill[s1h] = proto.ExtendedMarketData{} // also mutate a real key

	// e1 is a snapshot; its entries survive the mutation (value preserved).
	if v, ok := fwdFillAt(e1.NowFwdFill, s1h); !ok || v != 0 {
		t.Errorf("event[1]: NowFwdFill[s1h] after mutation = (%d, %v), want (0, true) — snapshot", v, ok)
	}
}

func TestSynchronizeShuffledArrivals(t *testing.T) {
	s1m := sub("BTCUSDT", proto.Timeframe1m)
	s5m := sub("BTCUSDT", proto.Timeframe5m)

	// s5m at 0, then co-close at 100, then s1m at 200.
	ch := ordered(
		emd(s5m, 0),
		emd(s1m, 100),
		emd(s5m, 100),
		emd(s1m, 200),
	)

	events := collect(synchronize(ch))
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3, times: %v", len(events), eventTimes(events))
	}
	// Event 0: lone s5m at t=0.
	if events[0].Time != 0 || !nowHas(events[0].Now, s5m, 0) {
		t.Errorf("event[0]: Time=%d Now=%+v, want lone s5m@0", events[0].Time, events[0].Now)
	}
	// Event 1: co-close at t=100 carrying both legs at 100.
	if events[1].Time != 100 || !nowHas(events[1].Now, s1m, 100) || !nowHas(events[1].Now, s5m, 100) {
		t.Errorf("event[1]: Time=%d Now=%+v, want s1m@100 + s5m@100", events[1].Time, events[1].Now)
	}
	// Event 2: lone s1m@200, s5m forward-filled to its last close (100).
	if events[2].Time != 200 || !nowHas(events[2].Now, s1m, 200) {
		t.Errorf("event[2]: Time=%d Now=%+v, want lone s1m@200", events[2].Time, events[2].Now)
	}
	if v, ok := fwdFillAt(events[2].NowFwdFill, s5m); !ok || v != 100 {
		t.Errorf("event[2]: NowFwdFill[s5m] = (%d, %v), want (100, true) forward-filled", v, ok)
	}
	if v, ok := fwdFillAt(events[2].NowFwdFill, s1m); !ok || v != 200 {
		t.Errorf("event[2]: NowFwdFill[s1m] = (%d, %v), want (200, true)", v, ok)
	}
}

func TestSynchronizeIdenticalSubsCollapse(t *testing.T) {
	s1 := sub("BTCUSDT", proto.Timeframe1m)
	s2 := sub("BTCUSDT", proto.Timeframe1m) // structurally identical

	ch := ordered(
		emd(s1, 100),
		emd(s2, 100),
	)

	events := collect(synchronize(ch))
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	// Both arrivals land in Now (identical subscription is not deduped there).
	if len(events[0].Now) != 2 || !nowHas(events[0].Now, s1, 100) {
		t.Errorf("Now = %+v, want two s1@100 legs", events[0].Now)
	}
	// s1 == s2 → collated to a single forward-fill key holding one value.
	if len(events[0].NowFwdFill) != 1 {
		t.Errorf("NowFwdFill len = %d, want 1 (identical subs collapse)", len(events[0].NowFwdFill))
	}
	if v, ok := fwdFillAt(events[0].NowFwdFill, s1); !ok || v != 100 {
		t.Errorf("NowFwdFill[s1] = (%d, %v), want (100, true)", v, ok)
	}
}

func TestSynchronizeMultipleAdvance(t *testing.T) {
	s1m := sub("BTCUSDT", proto.Timeframe1m)
	s5m := sub("BTCUSDT", proto.Timeframe5m)

	// Three boundaries, co-close at the second.
	ch := ordered(
		emd(s1m, 100),
		emd(s5m, 200),
		emd(s1m, 200),
		emd(s1m, 300),
	)

	events := collect(synchronize(ch))
	// 100: flush (1m only), 200: co-close, 300: flush (1m only)
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3, times: %v", len(events), eventTimes(events))
	}
	if events[0].Time != 100 || !nowHas(events[0].Now, s1m, 100) {
		t.Errorf("event[0]: Time=%d Now=%+v, want lone s1m@100", events[0].Time, events[0].Now)
	}
	if events[1].Time != 200 || !nowHas(events[1].Now, s5m, 200) || !nowHas(events[1].Now, s1m, 200) {
		t.Errorf("event[1]: Time=%d Now=%+v, want s5m@200 + s1m@200", events[1].Time, events[1].Now)
	}
	if events[2].Time != 300 || !nowHas(events[2].Now, s1m, 300) {
		t.Errorf("event[2]: Time=%d Now=%+v, want lone s1m@300", events[2].Time, events[2].Now)
	}
	// s5m forward-filled to its last close (200) into the final event.
	if v, ok := fwdFillAt(events[2].NowFwdFill, s5m); !ok || v != 200 {
		t.Errorf("event[2]: NowFwdFill[s5m] = (%d, %v), want (200, true) forward-filled", v, ok)
	}
}

func eventTimes(events []proto.SyncEvent) []int64 {
	ts := make([]int64, len(events))
	for i, e := range events {
		ts[i] = e.Time
	}
	return ts
}

func TestHelperShapes(t *testing.T) {
	s := sub("BTCUSDT", proto.Timeframe1m)
	if s.Symbol != "BTCUSDT" || s.Timeframe != proto.Timeframe1m || s.Provider != "test" {
		t.Errorf("helper sub = %+v, want BTCUSDT/1m/test", s)
	}
	if !strings.Contains(s.ID(), ":test") {
		t.Errorf("ID() must contain provider: got %q", s.ID())
	}
	e := emd(s, 42)
	if e.Timestamp() != 42 {
		t.Errorf("emd timestamp = %d, want 42", e.Timestamp())
	}
	if e.Subscription != s {
		t.Errorf("emd subscription = %+v, want %+v", e.Subscription, s)
	}
}
