package feat

import (
	"math"
	"reflect"
	"slices"
	"testing"

	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"
)

// TestProcessOutputIsBuffered guards the decision that the chain emits on a
// buffered channel, so feature computation is not blocked candle-by-candle on
// the downstream consumer.
func TestProcessOutputIsBuffered(t *testing.T) {
	chain, err := NewFeatureChain(nil)
	if err != nil {
		t.Fatalf("NewFeatureChain: %v", err)
	}

	in := make(chan proto.MarketData)
	close(in) // let the internal goroutine drain and exit cleanly

	out := chain.Process(in)
	if got := cap(out); got != outputBufferSize {
		t.Errorf("output channel capacity = %d, want %d", got, outputBufferSize)
	}
}

// TestProcessDropsMalformedCandles verifies a candle the window rejects (zero
// timestamp) is dropped rather than forwarded with a stale snapshot.
func TestProcessDropsMalformedCandles(t *testing.T) {
	chain, err := NewFeatureChain(nil)
	if err != nil {
		t.Fatalf("NewFeatureChain: %v", err)
	}

	in := make(chan proto.MarketData, 2)
	in <- proto.MarketData{}                                 // zero CloseTs → rejected
	in <- proto.MarketData{Candle: proto.Candle{CloseTs: 1}} // valid
	close(in)

	var got []proto.ExtendedMarketData
	for emd := range chain.Process(in) {
		got = append(got, emd)
	}

	if len(got) != 1 {
		t.Fatalf("emitted %d candles, want 1 (malformed dropped)", len(got))
	}
	if got[0].Candle.CloseTs != 1 {
		t.Errorf("emitted candle CloseTs = %d, want 1", got[0].Candle.CloseTs)
	}
}

// bridgeChain builds the chain the bridge tests run: sma_3, ema_5, vwap_4, rsi_3 and
// engulfing_candle.
func bridgeChain(t *testing.T) *FeatureChain {
	t.Helper()
	var features []proto.Feature
	for _, f := range []struct {
		id   string
		opts map[string]any
	}{
		{SMAID, map[string]any{"period": 3}},
		{EMAID, map[string]any{"period": 5}},
		{VWAPID, map[string]any{"period": 4}},
		{RSIID, map[string]any{"period": 3}},
		{EngulfingID, nil},
	} {
		feature, err := reg.Features.Create(f.id, f.opts)
		if err != nil {
			t.Fatalf("create %s: %v", f.id, err)
		}
		features = append(features, feature)
	}
	chain, err := NewFeatureChain(features)
	if err != nil {
		t.Fatalf("NewFeatureChain: %v", err)
	}
	return chain
}

// TestEvaluateMatchesProcess verifies that Evaluate over a candle's last Lookback candles, newest
// first, yields the snapshot Process emitted for that candle.
func TestEvaluateMatchesProcess(t *testing.T) {
	sub := proto.Subscription{Symbol: "BTCUSDT", Timeframe: proto.Timeframe5m, Provider: "fake"}
	candles := make([]proto.Candle, 40)
	for i := range candles {
		open := 100 + 10*math.Sin(float64(i)/3)
		closePrice := open + 4*math.Cos(float64(i)*1.7)
		candles[i] = proto.Candle{
			OpenTs:  int64(i) * 300_000,
			CloseTs: int64(i+1)*300_000 - 1,
			Open:    open,
			High:    max(open, closePrice) + 1,
			Low:     min(open, closePrice) - 1,
			Close:   closePrice,
			Volume:  10 + float64(i%7),
		}
	}

	in := make(chan proto.MarketData, len(candles))
	for _, c := range candles {
		in <- proto.MarketData{Subscription: sub, Candle: c}
	}
	close(in)
	var want []proto.ExtendedMarketData
	for emd := range bridgeChain(t).Process(in) {
		want = append(want, emd)
	}

	chain := bridgeChain(t)
	n := chain.Lookback()
	for i := range candles {
		history := slices.Clone(candles[max(0, i-n+1) : i+1])
		slices.Reverse(history)
		got := chain.Evaluate(sub, history)
		if !reflect.DeepEqual(got.Data(), want[i].Indicators.Data()) {
			t.Errorf("candle %d: Evaluate = %v, Process = %v", i, got.Data(), want[i].Indicators.Data())
		}
	}
	if got := len(want[len(want)-1].Indicators.Data()); got != 5 {
		t.Errorf("last snapshot holds %d features, want 5", got)
	}
}

// TestLookback verifies the chain's lookback is its features' largest, 0 without features.
func TestLookback(t *testing.T) {
	if got := bridgeChain(t).Lookback(); got != 10 {
		t.Errorf("Lookback = %d, want 10 (ema_5)", got)
	}
	empty, err := NewFeatureChain(nil)
	if err != nil {
		t.Fatalf("NewFeatureChain: %v", err)
	}
	if got := empty.Lookback(); got != 0 {
		t.Errorf("empty Lookback = %d, want 0", got)
	}
}
