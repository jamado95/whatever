package align

import (
	"slices"
	"testing"

	proto "whatever/internal/protocol"
)

// fixture is a scenario input: subscriptions, column stride and arrival order.
type fixture struct {
	subs   []proto.Subscription
	stride int64
	emds   []proto.ExtendedMarketData
}

// runFx drives the V2 core over a fixture's arrival order with the given
// readiness module and collects the emitted events.
func runFx(fx fixture, rdn Readiness) []*proto.SyncEventV2 {
	var events []*proto.SyncEventV2
	for ev := range runV2(ordered(fx.emds...), fx.subs, rdn) {
		events = append(events, ev)
	}
	return events
}

// legOf returns the CloseTs of the event's leg for a subscription.
func legOf(ev *proto.SyncEventV2, s proto.Subscription) (int64, bool) {
	for _, e := range ev.Data() {
		if e.Subscription == s {
			return e.Timestamp(), true
		}
	}
	return 0, false
}

// emdRange returns one datapoint per column in [from, to] for a subscription.
func emdRange(s proto.Subscription, stride, from, to int64) []proto.ExtendedMarketData {
	var out []proto.ExtendedMarketData
	for c := from; c <= to; c++ {
		out = append(out, emd(s, c*stride))
	}
	return out
}

func seq(parts ...[]proto.ExtendedMarketData) []proto.ExtendedMarketData {
	var out []proto.ExtendedMarketData
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

type frame struct {
	col    int64
	status proto.SyncDataFreshness
}

// frames returns the frames [from, to] in ascending order, all with status.
func frames(from, to int64, status proto.SyncDataFreshness) []frame {
	var out []frame
	for c := from; c <= to; c++ {
		out = append(out, frame{c, status})
	}
	return out
}

// assertInvariants checks the SyncEventV2 contract on every emitted event:
// exactly one leg per stream, legs sorted oldest to newest, Time is the newest
// leg, and no leg closes after Time.
func assertInvariants(t *testing.T, events []*proto.SyncEventV2, subs []proto.Subscription, stride int64) {
	t.Helper()
	for _, e := range events {
		data := e.Data()
		if len(data) != len(subs) {
			t.Errorf("Time=%d: %d legs, want one per stream (%d)", e.Time(), len(data), len(subs))
			continue
		}
		seen := make(map[proto.Subscription]bool, len(subs))
		for i, leg := range data {
			seen[leg.Subscription] = true
			if i > 0 && leg.Timestamp() < data[i-1].Timestamp() {
				t.Errorf("Time=%d: legs not sorted oldest to newest", e.Time())
			}
			if leg.Timestamp() > e.Time() {
				t.Errorf("Time=%d: leg %v closes after Time at %d", e.Time(), leg.Subscription, leg.Timestamp())
			}
		}
		for _, s := range subs {
			if !seen[s] {
				t.Errorf("Time=%d: missing leg for %v", e.Time(), s)
			}
		}
		if e.Time() != data[len(data)-1].Timestamp() {
			t.Errorf("Time=%d is not the newest leg %d", e.Time(), data[len(data)-1].Timestamp())
		}
	}
}

// assertFrames checks emission order, column and status.
func assertFrames(t *testing.T, events []*proto.SyncEventV2, stride int64, want []frame) {
	t.Helper()
	got := make([]frame, len(events))
	for i, e := range events {
		got[i] = frame{column(e.Time(), stride), e.Status()}
	}
	if len(got) != len(want) {
		t.Fatalf("frames = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("frame[%d] = %v, want %v (all: %v)", i, got[i], want[i], got)
		}
	}
}

// cols returns the columns [from, to] in ascending order.
func cols(from, to int64) []int64 {
	var out []int64
	for c := from; c <= to; c++ {
		out = append(out, c)
	}
	return out
}

// assertColumns checks the set of emitted columns, ignoring order and status;
// a column emitted twice fails.
func assertColumns(t *testing.T, events []*proto.SyncEventV2, stride int64, want []int64) {
	t.Helper()
	got := make([]int64, len(events))
	for i, e := range events {
		got[i] = column(e.Time(), stride)
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("columns = %v, want %v", got, want)
	}
}

// assertLeg checks a subscription's leg CloseTs on every event in [from, to].
func assertLeg(t *testing.T, events []*proto.SyncEventV2, stride int64, s proto.Subscription, from, to, wantTs int64) {
	t.Helper()
	for _, e := range events {
		c := column(e.Time(), stride)
		if c < from || c > to {
			continue
		}
		if ts, ok := legOf(e, s); !ok || ts != wantTs {
			t.Errorf("col %d: leg %v = (%d,%v), want (%d,true)", c, s, ts, ok, wantTs)
		}
	}
}

const (
	cur   = proto.CurrentMarketData
	stale = proto.StaleMarketData

	sec = int64(1000) // ms per second; fixtures are in ms like real CloseTs
)

var (
	btc1m = sub("BTCUSDT", proto.Timeframe1m)
	btc5m = sub("BTCUSDT", proto.Timeframe5m)
	btc1h = sub("BTCUSDT", proto.Timeframe1h)
	eth1m = sub("ETHUSDT", proto.Timeframe1m)
	eth5m = sub("ETHUSDT", proto.Timeframe5m)
)

// fxInOrderForwardFill: a 1m+1h co-close, then 1m-only frames with 1h
// forward-filled.
func fxInOrderForwardFill() fixture {
	return fixture{[]proto.Subscription{btc1m, btc1h}, 60 * sec, []proto.ExtendedMarketData{
		emd(btc1h, 3600*sec), emd(btc1m, 3600*sec), // both close at the hour
		emd(btc1m, 3660*sec), // 1m advances, 1h forward-filled
		emd(btc1m, 3720*sec),
	}}
}

// fxLateBoundaryCandle: the 5m@600 boundary candle arrives after 1m cols 10..11,
// holding them until it lands.
func fxLateBoundaryCandle() fixture {
	return fixture{[]proto.Subscription{btc1m, btc5m}, 60 * sec, []proto.ExtendedMarketData{
		emd(btc5m, 300*sec), emd(btc1m, 300*sec), // start at col 5
		emd(btc1m, 360*sec), emd(btc1m, 420*sec), emd(btc1m, 480*sec), emd(btc1m, 540*sec), // cols 6..9
		emd(btc1m, 600*sec), // col 10: 1m at the next 5m boundary, but 5m@600 not yet here
		emd(btc1m, 660*sec), // col 11
		emd(btc5m, 600*sec), // late 5m boundary candle fills the gap
		emd(btc1m, 720*sec), // col 12
	}}
}

// fxColdStart: 1m produces several candles before 5m ever appears.
func fxColdStart() fixture {
	return fixture{[]proto.Subscription{btc1m, btc5m}, 60 * sec, []proto.ExtendedMarketData{
		emd(btc1m, 300*sec), emd(btc1m, 360*sec), emd(btc1m, 420*sec), // 5m still silent
		emd(btc5m, 300*sec), // 5m arrives
		emd(btc1m, 480*sec),
	}}
}

// fxStalledStream: 5m stalls after col 5 while 1m advances well past the window;
// the next 5m boundary (col 10) never arrives.
func fxStalledStream() fixture {
	return fixture{[]proto.Subscription{btc1m, btc5m}, 60 * sec, seq(
		[]proto.ExtendedMarketData{emd(btc5m, 300*sec)},
		emdRange(btc1m, 60*sec, 5, 30),
	)}
}

// fxDropAndDuplicate: after cols 10..13, 1m@600 arrives below the window and
// 1m@780 repeats a dispatched column.
func fxDropAndDuplicate() fixture {
	return fixture{[]proto.Subscription{btc1m}, 60 * sec, []proto.ExtendedMarketData{
		emd(btc1m, 600*sec), emd(btc1m, 660*sec), emd(btc1m, 720*sec), emd(btc1m, 780*sec), // cols 10..13
		emd(btc1m, 600*sec), // ancient: below the window now
		emd(btc1m, 780*sec), // duplicate in-window, already dispatched
		emd(btc1m, 840*sec), // col 14
	}}
}

// fxLateEarliestCandle: 5m 0..23 and 1h@12 arrive first; the earliest 1h@0
// lands late, then 5m runs on without 1h@24.
func fxLateEarliestCandle() fixture {
	return fixture{[]proto.Subscription{btc5m, btc1h}, 300 * sec, seq(
		emdRange(btc5m, 300*sec, 0, 23),
		[]proto.ExtendedMarketData{emd(btc1h, 3600*sec), emd(btc1h, 0)},
		emdRange(btc5m, 300*sec, 24, 30), // waits on 1h@24, never arrives
	)}
}

// fxLateFloor: 5m runs past the window (base 7) before 1h@12 and 1h@0 arrive;
// 1h@0 lands below the window as the floor.
func fxLateFloor() fixture {
	return fixture{[]proto.Subscription{btc5m, btc1h}, 300 * sec, seq(
		emdRange(btc5m, 300*sec, 0, 30),
		[]proto.ExtendedMarketData{emd(btc1h, 3600*sec), emd(btc1h, 0)},
	)}
}

// fxEarlyCoarse: both 1h candles arrive before any 5m.
func fxEarlyCoarse() fixture {
	return fixture{[]proto.Subscription{btc5m, btc1h}, 300 * sec, seq(
		[]proto.ExtendedMarketData{emd(btc1h, 3600*sec), emd(btc1h, 0)},
		emdRange(btc5m, 300*sec, 0, 30),
	)}
}

// fxMultiSymbolOutOfOrder: two symbols on one timeframe; col 3 completes before
// col 2, whose ETH candle lands last.
func fxMultiSymbolOutOfOrder() fixture {
	return fixture{[]proto.Subscription{btc1m, eth1m}, 60 * sec, []proto.ExtendedMarketData{
		emd(btc1m, 60*sec), emd(eth1m, 60*sec), // col 1
		emd(btc1m, 120*sec),                      // col 2 waits on eth
		emd(eth1m, 180*sec), emd(btc1m, 180*sec), // col 3 complete
		emd(eth1m, 120*sec), // col 2 completes late
	}}
}

// fxMultiSymbolMultiTimeframe: 2 symbols × (1m, 5m) over cols 5..11; ETH 5m@600
// arrives last.
func fxMultiSymbolMultiTimeframe() fixture {
	var emds []proto.ExtendedMarketData
	for ts := int64(300); ts <= 660; ts += 60 {
		if ts%300 == 0 {
			emds = append(emds, emd(btc5m, ts*sec))
			if ts != 600 {
				emds = append(emds, emd(eth5m, ts*sec))
			}
		}
		emds = append(emds, emd(btc1m, ts*sec), emd(eth1m, ts*sec))
	}
	emds = append(emds, emd(eth5m, 600*sec))
	return fixture{[]proto.Subscription{btc1m, btc5m, eth1m, eth5m}, 60 * sec, emds}
}

// fxGapAfterEviction: 1h@24 never arrives and is evicted (base 25) before 1h@36
// resumes.
func fxGapAfterEviction() fixture {
	return fixture{[]proto.Subscription{btc5m, btc1h}, 300 * sec, seq(
		[]proto.ExtendedMarketData{emd(btc1h, 12*300*sec)},
		emdRange(btc5m, 300*sec, 12, 48),
		[]proto.ExtendedMarketData{emd(btc1h, 36*300*sec)},
	)}
}

// fxGapInWindow: 1h@24 is missing while 1h@36 arrives, then 1h@24 lands late,
// still in-window.
func fxGapInWindow() fixture {
	return fixture{[]proto.Subscription{btc5m, btc1h}, 300 * sec, seq(
		[]proto.ExtendedMarketData{emd(btc1h, 12*300*sec)},
		emdRange(btc5m, 300*sec, 12, 36),
		[]proto.ExtendedMarketData{emd(btc1h, 36*300*sec), emd(btc1h, 24*300*sec)},
	)}
}

// binanceClose shifts every CloseTs 1ms before its boundary, as Binance stamps
// them; columns must not change.
func binanceClose(fx fixture) fixture {
	emds := make([]proto.ExtendedMarketData, len(fx.emds))
	for i, e := range fx.emds {
		e.Candle.CloseTs--
		emds[i] = e
	}
	return fixture{fx.subs, fx.stride, emds}
}

// legWant is an expected leg CloseTs for a subscription over columns [from, to].
type legWant struct {
	sub          proto.Subscription
	from, to, ts int64
}

// TestV2Scenarios checks Synchronizer behaviour, independent of dispatch order
// and status: which columns are emitted (once each), their legs, and invariants.
func TestV2Scenarios(t *testing.T) {
	tests := []struct {
		name string
		fx   fixture
		cols []int64
		legs []legWant
	}{
		// Coarse leg forward-fills; the co-close frame carries both hour candles.
		{"in-order forward fill", fxInOrderForwardFill(), cols(60, 62),
			[]legWant{{btc1h, 60, 62, 3600 * sec}, {btc1m, 60, 60, 3600 * sec}}},
		// Frames needing 5m@600 hold, then fill with it — never with stale 5m@300.
		{"late boundary candle", fxLateBoundaryCandle(), cols(5, 12),
			[]legWant{{btc5m, 5, 9, 300 * sec}, {btc5m, 10, 12, 600 * sec}}},
		// Nothing emits before every stream has data; no frame lacks a leg.
		{"cold start", fxColdStart(), cols(5, 8),
			[]legWant{{btc5m, 5, 8, 300 * sec}}},
		// Frames needing the stalled candle never emit; the loop never blocks.
		{"stalled stream", fxStalledStream(), cols(5, 9),
			[]legWant{{btc5m, 5, 9, 300 * sec}}},
		// Below-window datapoint is dropped; duplicate column not re-emitted.
		{"drop and duplicate", fxDropAndDuplicate(), cols(10, 14), nil},
		// Late earliest candle still releases its frames, each filled with it.
		{"late earliest candle", fxLateEarliestCandle(), cols(0, 23),
			[]legWant{{btc1h, 0, 11, 0}, {btc1h, 12, 23, 3600 * sec}}},
		// Late floor is accepted and fills in-window cols 7..11.
		{"late floor", fxLateFloor(), cols(7, 23),
			[]legWant{{btc1h, 7, 11, 0}}},
		// Coarse data first: every frame still emits.
		{"early coarse", fxEarlyCoarse(), cols(0, 23), nil},
		// Late-completed col 2 carries the late ETH candle.
		{"multi-symbol out of order", fxMultiSymbolOutOfOrder(), cols(1, 3),
			[]legWant{{eth1m, 2, 2, 120 * sec}}},
		// One leg per stream across symbols, 5m forward-filled; late ETH 5m holds cols 10..11.
		{"multi-symbol multi-timeframe", fxMultiSymbolMultiTimeframe(), cols(5, 11),
			[]legWant{{btc5m, 5, 9, 300 * sec}, {btc5m, 10, 11, 600 * sec}, {eth5m, 5, 9, 300 * sec}, {eth5m, 10, 11, 600 * sec}}},
		// Resumed 1h@36 does not release cols 24..35 against a stale 1h@12.
		{"gap after eviction", fxGapAfterEviction(), append(cols(12, 23), cols(36, 47)...), nil},
		// Cols 24..35 hold (no partial frames), then fill with the late 1h@24.
		{"gap in window", fxGapInWindow(), cols(12, 36),
			[]legWant{{btc1h, 24, 35, 24 * 300 * sec}}},
	}
	for _, tc := range tests {
		// Same columns with boundary-1ms closes; expected legs shift by 1ms.
		for variant, shift := range map[string]int64{"boundary": 0, "boundary-1ms": 1} {
			fx := tc.fx
			if shift != 0 {
				fx = binanceClose(fx)
			}
			t.Run(tc.name+"/"+variant, func(t *testing.T) {
				events := runFx(fx, orderedCurrentReadiness{})
				assertInvariants(t, events, fx.subs, fx.stride)
				assertColumns(t, events, fx.stride, tc.cols)
				for _, l := range tc.legs {
					assertLeg(t, events, fx.stride, l.sub, l.from, l.to, l.ts-shift)
				}
			})
		}
	}
}

// TestV2EmptyInput: no data yields no events and closes.
func TestV2EmptyInput(t *testing.T) {
	s1m := sub("BTCUSDT", proto.Timeframe1m)
	if events := runFx(fixture{subs: []proto.Subscription{s1m}, stride: 60 * sec}, orderedCurrentReadiness{}); len(events) != 0 {
		t.Fatalf("got %d events on empty input, want 0", len(events))
	}
	if _, ok := <-NewSynchronizerV2(nil).Sync(nil); ok {
		t.Fatal("Sync with no subscriptions must return a closed channel")
	}
}

// TestV2Covers: covers spans a full period in-window, clamps to maxCol and to
// baseCol (late floor), and is empty below the window.
func TestV2Covers(t *testing.T) {
	s5m := sub("BTCUSDT", proto.Timeframe5m)
	s1h := sub("BTCUSDT", proto.Timeframe1h)
	st := newSyncState([]proto.Subscription{s5m, s1h})
	for _, e := range emdRange(s5m, 300*sec, 0, 30) { // window base moves to col 7
		st.Apply(e)
	}
	id, col, ok := st.Apply(emd(s1h, 0)) // late floor
	if !ok || id != s1h.ID() || col != 0 {
		t.Fatalf("Apply(late floor) = (%q,%d,%v), want (%q,0,true)", id, col, ok, s1h.ID())
	}

	cases := []struct {
		name   string
		id     string
		col    int64
		lo, hi int64
		ok     bool
	}{
		{"in-window full period", s1h.ID(), 12, 12, 23, true},
		{"clamped to maxCol", s1h.ID(), 24, 24, 30, true},
		{"late floor clamped to baseCol", s1h.ID(), 0, 7, 11, true},
		{"below window", s5m.ID(), 3, 0, 0, false},
	}
	for _, tc := range cases {
		lo, hi, ok := st.covers(tc.id, tc.col)
		if ok != tc.ok || (ok && (lo != tc.lo || hi != tc.hi)) {
			t.Errorf("%s: covers = (%d,%d,%v), want (%d,%d,%v)", tc.name, lo, hi, ok, tc.lo, tc.hi, tc.ok)
		}
	}
}

// TestV2ApplyDropsBelowWindow: a datapoint older than the window's first owning
// column is rejected and leaves the floor untouched.
func TestV2ApplyDropsBelowWindow(t *testing.T) {
	s5m := sub("BTCUSDT", proto.Timeframe5m)
	s1h := sub("BTCUSDT", proto.Timeframe1h)
	st := newSyncState([]proto.Subscription{s5m, s1h})
	for _, e := range emdRange(s5m, 300*sec, 0, 30) { // base 7, s5m floor at col 6
		st.Apply(e)
	}
	if _, _, ok := st.Apply(emd(s5m, 3*300*sec)); ok {
		t.Fatal("Apply(col 3 below window) ok=true, want dropped")
	}
	if fc := st.floorCol[s5m.ID()]; fc != 6 {
		t.Errorf("floorCol = %d after drop, want 6", fc)
	}
}

// dupReadiness returns every complete window column twice per arrival, so only
// the runner's dispatch-once guard keeps emissions unique.
type dupReadiness struct{}

func (dupReadiness) Evaluate(st *syncState, _ string, _ int64) []int64 {
	var out []int64
	for c := st.baseCol; c <= st.maxCol; c++ {
		if st.ready(c) {
			out = append(out, c, c)
		}
	}
	return out
}

// TestV2DispatchOnce: the runner emits each column at most once, whatever the
// policy returns.
func TestV2DispatchOnce(t *testing.T) {
	for name, tc := range map[string]struct {
		fx   fixture
		cols []int64
	}{
		"late earliest candle": {fxLateEarliestCandle(), cols(0, 23)},
		"drop and duplicate":   {fxDropAndDuplicate(), cols(10, 14)},
	} {
		t.Run(name, func(t *testing.T) {
			assertColumns(t, runFx(tc.fx, dupReadiness{}), tc.fx.stride, tc.cols)
		})
	}
}

// syncFx runs a fixture through Sync, each subscription on its own lane fully
// buffered up front, and collects the emitted events.
func syncFx(s *SynchronizerV2, fx fixture) []*proto.SyncEventV2 {
	lanes := make(map[proto.Subscription]chan proto.ExtendedMarketData, len(fx.subs))
	for _, sub := range fx.subs {
		lanes[sub] = make(chan proto.ExtendedMarketData, len(fx.emds))
	}
	for _, e := range fx.emds {
		lanes[e.Subscription] <- e
	}
	in := make(map[proto.Subscription]<-chan proto.ExtendedMarketData, len(lanes))
	for sub, ch := range lanes {
		close(ch)
		in[sub] = ch
	}
	var events []*proto.SyncEventV2
	for ev := range s.Sync(in) {
		events = append(events, ev)
	}
	return events
}

// fxSkewedLanes: 5m cols 0..60 and 1h cols 0..60, far past the 24-column
// window, with the whole 1h lane arriving before any 5m.
func fxSkewedLanes() fixture {
	return fixture{[]proto.Subscription{btc5m, btc1h}, 300 * sec, seq(
		[]proto.ExtendedMarketData{
			emd(btc1h, 0), emd(btc1h, 12*300*sec), emd(btc1h, 24*300*sec),
			emd(btc1h, 36*300*sec), emd(btc1h, 48*300*sec), emd(btc1h, 60*300*sec),
		},
		emdRange(btc5m, 300*sec, 0, 60),
	)}
}

// TestOrderedSynchronizerSkew: with lanes skewed far beyond the window, the
// ordered merge still emits every column, in order, all Current, every run.
func TestOrderedSynchronizerSkew(t *testing.T) {
	for name, tc := range map[string]struct {
		fx     fixture
		offset int64
	}{
		"exact boundary": {fxSkewedLanes(), 0},
		"binance close":  {binanceClose(fxSkewedLanes()), -1},
	} {
		t.Run(name, func(t *testing.T) {
			fx := tc.fx
			events := syncFx(NewOrderedSynchronizer(nil), fx)
			assertInvariants(t, events, fx.subs, fx.stride)
			assertFrames(t, events, fx.stride, frames(0, 60, cur))
			for h := int64(0); h <= 60; h += 12 {
				assertLeg(t, events, fx.stride, btc1h, h, h+11, h*fx.stride+tc.offset)
			}

			want := eventKeys(events)
			for range 20 {
				if got := eventKeys(syncFx(NewOrderedSynchronizer(nil), fx)); !slices.Equal(got, want) {
					t.Fatalf("ordered output differs between runs:\n  %v\nvs\n  %v", got, want)
				}
			}

			// Same data in skewed arrival order without the merge loses columns.
			if n := len(runFx(fx, orderedCurrentReadiness{})); n >= len(events) {
				t.Errorf("unordered skewed arrival emitted %d frames, want fewer than %d", n, len(events))
			}
		})
	}
}

// TestOrderedSynchronizerEmpty: no subscriptions yields a closed channel.
func TestOrderedSynchronizerEmpty(t *testing.T) {
	if _, ok := <-NewOrderedSynchronizer(nil).Sync(nil); ok {
		t.Fatal("Sync with no subscriptions must return a closed channel")
	}
}
