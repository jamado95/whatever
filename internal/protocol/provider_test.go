package proto

import "testing"

func syncLeg(symbol string, closeTs int64) ExtendedMarketData {
	return ExtendedMarketData{MarketData: MarketData{
		Subscription: Subscription{Symbol: symbol, Timeframe: Timeframe1m, Provider: "test"},
		Candle:       Candle{CloseTs: closeTs},
	}}
}

func TestNewSyncEventV2Empty(t *testing.T) {
	if _, err := NewSyncEventV2(CurrentMarketData, nil); err == nil {
		t.Fatal("want error for empty data")
	}
}

// TestNewSyncEventV2Ordering: legs are sorted oldest to newest and Time is the
// newest leg; the caller's slice and the returned Data() are not aliased.
func TestNewSyncEventV2Ordering(t *testing.T) {
	in := []ExtendedMarketData{syncLeg("C", 300), syncLeg("A", 0), syncLeg("B", 120)}
	ev, err := NewSyncEventV2(StaleMarketData, in)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Time() != 300 {
		t.Errorf("Time = %d, want 300 (newest)", ev.Time())
	}
	if ev.Status() != StaleMarketData {
		t.Errorf("Status = %v, want StaleMarketData", ev.Status())
	}
	data := ev.Data()
	for i, want := range []int64{0, 120, 300} {
		if data[i].Timestamp() != want {
			t.Errorf("Data()[%d] = %d, want %d", i, data[i].Timestamp(), want)
		}
	}
	if in[0].Timestamp() != 300 {
		t.Error("constructor mutated the caller's slice")
	}
	data[0] = syncLeg("X", 999)
	if ev.Data()[0].Timestamp() != 0 {
		t.Error("mutating Data() result changed the event")
	}
}

func TestTimeframeMs(t *testing.T) {
	for _, tf := range ValidTimeframes() {
		if got, want := tf.TimeframeMs(), int64(tf.TimeframeS())*1000; got != want {
			t.Errorf("%s: TimeframeMs = %d, want %d", tf, got, want)
		}
	}
}
