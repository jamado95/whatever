package feat

import (
	"testing"

	proto "whatever/internal/protocol"
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
