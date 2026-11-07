package features

import (
	"fmt"

	proto "whatever/internal/protocol"
	"whatever/internal/protocol/feature"
	"whatever/internal/protocol/frame"
)

const testStride = 300_000

// closeAt is stream i's test close at col, its steps differing per column: a read of the wrong column
// shows up in any difference of closes.
func closeAt(i int, col int64) float64 { return float64(100*(i+1)) + float64(col*col) }

// last is its stream's close.
func last() feature.Feature {
	return feature.Feature{
		ID:       "last",
		Lookback: 1,
		Bind: func(b *feature.Binder) func() (float64, bool) {
			in := feature.Candles(b)
			return func() (float64, bool) {
				c, s, _ := in.Latest()
				return c.Close, s == frame.Fresh
			}
		},
	}
}

// sum is its last n closes, unable to compute with fewer in its window.
func sum(n int) feature.Feature {
	return feature.Feature{
		ID:       fmt.Sprintf("sum_%d", n),
		Lookback: n,
		Bind: func(b *feature.Binder) func() (float64, bool) {
			in := feature.Candles(b)
			candles := make([]proto.Candle, 0, n)
			return func() (float64, bool) {
				candles = in.Data(n, candles)
				if len(candles) < n {
					return 0, false
				}
				var total float64
				for _, c := range candles {
					total += c.Close
				}
				return total, true
			}
		},
	}
}

// diff is the change between the last two values of its dependency last.
func diff() feature.Feature {
	return feature.Feature{
		ID:       "diff",
		Lookback: 2,
		Deps:     []string{"last"},
		Bind: func(b *feature.Binder) func() (float64, bool) {
			in := feature.Dep[float64](b, "last")
			vals := make([]float64, 0, 2)
			return func() (float64, bool) {
				vals = in.Data(2, vals)
				if len(vals) < 2 {
					return 0, false
				}
				return vals[0] - vals[1], true
			}
		},
	}
}

// sign is an int feature: 1 when its stream's candle rose, -1 when it fell, 0 otherwise.
func sign() feature.Feature {
	return feature.Feature{
		ID:       "sign",
		Lookback: 1,
		Bind: func(b *feature.Binder) func() (int, bool) {
			in := feature.Candles(b)
			return func() (int, bool) {
				c, s, _ := in.Latest()
				switch {
				case s != frame.Fresh:
					return 0, false
				case c.Close > c.Open:
					return 1, true
				case c.Close < c.Open:
					return -1, true
				}
				return 0, true
			}
		},
	}
}

// seen is the state its dependency last reads at the frame.
func seen() feature.Feature {
	return feature.Feature{
		ID:       "seen",
		Lookback: 1,
		Deps:     []string{"last"},
		Bind: func(b *feature.Binder) func() (int, bool) {
			in := feature.Dep[float64](b, "last")
			return func() (int, bool) {
				_, s, _ := in.Latest()
				return int(s), true
			}
		},
	}
}

// spread is a pair feature: its first stream's close less its second's.
func spread() feature.Feature {
	return feature.Feature{
		ID:       "spread",
		Lookback: 1,
		Pair:     true,
		Bind: func(b *feature.Binder) func() (float64, bool) {
			x, y := feature.PairCandles(b)
			return func() (float64, bool) {
				cx, _, _ := x.Latest()
				cy, _, _ := y.Latest()
				return cx.Close - cy.Close, true
			}
		},
	}
}

// pdiff is a pair feature over last: its first stream's value less its second's.
func pdiff() feature.Feature {
	return feature.Feature{
		ID:       "pdiff",
		Lookback: 1,
		Pair:     true,
		Deps:     []string{"last"},
		Bind: func(b *feature.Binder) func() (float64, bool) {
			x, y := feature.PairDep[float64](b, "last")
			return func() (float64, bool) {
				vx, _, _ := x.Latest()
				vy, _, _ := y.Latest()
				return vx - vy, true
			}
		},
	}
}

// label has a value type the engine does not support.
func label() feature.Feature {
	return feature.Feature{
		ID:       "label",
		Lookback: 1,
		Bind: func(*feature.Binder) func() (string, bool) {
			return func() (string, bool) { return "", false }
		},
	}
}

// shapes is n rings of a column per update, each holding 8 updates.
func shapes(n int) []frame.Shape {
	out := make([]frame.Shape, n)
	for i := range out {
		out[i] = frame.Shape{Slots: 8, Period: 1, Window: 1, Bars: 8}
	}
	return out
}

// candles is a buffer of n streams with their test closes, Fresh at columns 0 to cols-1. Odd columns
// fall and even ones rise.
func candles(n int, cols int64) *frame.Buffer[proto.Candle] {
	buf := frame.NewBuffer[proto.Candle](shapes(n))
	for i := range n {
		for col := range cols {
			c := closeAt(i, col)
			buf.Ring(i).Put(col, proto.Candle{Open: c - 1 + 2*float64(col%2), Close: c}, frame.Fresh)
		}
	}
	return buf
}

// at pins buf at col.
func at(buf *frame.Buffer[proto.Candle], col int64) frame.View[proto.Candle] {
	return frame.NewView(col, testStride, frame.Current, true, buf)
}

// The test stream table's indices.
const (
	direct1h  = iota // BTCUSDT:1h:btc_1h: period 12
	btc5m            // BTCUSDT:5m:btc_5m: period 1
	eth5m            // ETHUSDT:5m:eth_5m: period 1
	rolling1h        // BTCUSDT:1h:derived_BTCUSDT:5m:btc_5m: period 1, window 12
)

// testStreams is BTCUSDT 5m deriving 1h, BTCUSDT 1h and ETHUSDT 5m.
func testStreams() *proto.Streams {
	return proto.NewStreams([]proto.Subscription{
		{Symbol: "BTCUSDT", Timeframe: proto.Timeframe5m, Provider: "btc_5m",
			Derive: proto.NewTimeframeDerivationSet(proto.Timeframe1h)},
		{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1h, Provider: "btc_1h"},
		{Symbol: "ETHUSDT", Timeframe: proto.Timeframe5m, Provider: "eth_5m"},
	})
}

// streamCandles is a buffer over the table with its test closes, Fresh at every due column below cols.
func streamCandles(st *proto.Streams, cols int64) *frame.Buffer[proto.Candle] {
	sh := make([]frame.Shape, len(st.All))
	for i, s := range st.All {
		sh[i] = frame.Shape{Slots: cols, Period: s.Period, Window: s.Window, Bars: cols}
	}
	buf := frame.NewBuffer[proto.Candle](sh)
	for i, s := range st.All {
		for col := int64(0); col < cols; col += s.Period {
			c := closeAt(i, col)
			buf.Ring(i).Put(col, proto.Candle{Open: c - 1 + 2*float64(col%2), Close: c}, frame.Fresh)
		}
	}
	return buf
}

// on places f, under its own ID, at each coordinate.
func on(f feature.Feature, ats ...[2]int) []instance {
	return over(f, f.ID, nil, ats...)
}

// over places f under composed ID id, its dependencies fed by deps, at each coordinate.
func over(f feature.Feature, id string, deps map[string]string, ats ...[2]int) []instance {
	out := make([]instance, len(ats))
	for i, at := range ats {
		out[i] = instance{feature: f, id: id, at: at, deps: deps}
	}
	return out
}
