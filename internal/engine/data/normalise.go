package data

import (
	"math"

	"whatever/internal/logger"
	proto "whatever/internal/protocol"
)

// laneItem is one value on a lane, between its per-stream stages: a candle at its column.
type laneItem struct {
	col int64        // the column the candle closes in
	c   proto.Candle // validated, ms timestamps
}

// drop is why the normalise stage rejected a candle; keep accepts it.
type drop uint8

const (
	keep       drop = iota
	invalid         // fails validation
	offDue          // closes off its stream's due column
	duplicate       // identical to the last accepted candle
	outOfOrder      // its column is not beyond the last accepted one
	numDrops
)

var dropNames = [numDrops]string{"keep", "invalid", "off_due", "duplicate", "out_of_order"}

func (d drop) String() string { return dropNames[d] }

// msLimit bounds ms epoch timestamps; a larger one is in another unit (µs) and is invalid.
const msLimit = 1e14

/*
spanTolerance is how far a candle's span (CloseTs − OpenTs) may fall short of its timeframe. Venues
close a bar at its boundary or just before it — Binance at boundary − 1 ms — so a valid span lies in
[timeframe − spanTolerance, timeframe]. A longer span is rejected: under ceilDiv placement a close
past its boundary would land one column late.
*/
const spanTolerance = 1000 // ms

// laneBuffer is the output capacity of each lane stage, matching the provider and feature-chain buffers.
const laneBuffer = 100

// normaliser is the normalise stage of one direct stream's lane; only its goroutine touches its state.
type normaliser struct {
	streams *Streams
	stream  int
	tfMs    int64
	last    proto.Candle // last accepted candle
	lastCol int64        // its column; −1 before the first
	drops   [numDrops]int
	log     *logger.Logger
}

func newNormaliser(streams *Streams, stream int, log *logger.Logger) *normaliser {
	s := streams.All[stream]
	return &normaliser{
		streams: streams,
		stream:  stream,
		tfMs:    s.Sub.Timeframe.TimeframeMs(),
		lastCol: -1,
		log:     log.With("stream", s.Sub.ID()),
	}
}

/*
run normalises in: accepted candles at their columns; once in closes, a summary line if anything was
dropped, then the output closes. It is a pipe.Stage.
*/
func (n *normaliser) run(in <-chan proto.MarketData) <-chan laneItem {
	out := make(chan laneItem, laneBuffer)

	go func() {
		defer close(out)
		for md := range in {
			col, d := n.accept(md.Candle)
			if d != keep {
				n.dropped(d, md.Candle)
				continue
			}
			out <- laneItem{col: col, c: md.Candle}
		}
		n.summary()
	}()

	return out
}

/*
accept applies the rules in order — validate, column, due, duplicate, order — and returns the
candle's column, or why it was dropped. Only an accepted candle moves the stage's state.
*/
func (n *normaliser) accept(c proto.Candle) (int64, drop) {
	span := c.CloseTs - c.OpenTs
	valid := c.OpenTs > 0 && c.CloseTs < msLimit &&
		span >= n.tfMs-spanTolerance && span <= n.tfMs &&
		finite(c.Open) && finite(c.High) && finite(c.Low) && finite(c.Close) && finite(c.Volume) &&
		c.Volume >= 0 && c.Low <= min(c.Open, c.Close) && c.High >= max(c.Open, c.Close)

	// valid indicates whether c has ms timestamps, finite and consistent OHLCV, and a span that fits tfMs.
	if !valid {
		return 0, invalid
	}

	col := n.streams.column(c.CloseTs)
	switch {
	case col%n.streams.All[n.stream].Period != 0: // due
		return col, offDue
	case c == n.last: // duplicate
		return col, duplicate
	case col <= n.lastCol: // order
		return col, outOfOrder
	}

	n.last, n.lastCol = c, col

	return col, keep
}

func finite(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }

// dropped counts a drop and warns on the first of each rule; duplicates are only counted.
func (n *normaliser) dropped(d drop, c proto.Candle) {
	n.drops[d]++
	if n.drops[d] == 1 && d != duplicate {
		n.log.With("reason", d.String()).With("close_ts", c.CloseTs).
			Warn("dropping candle; further drops for this reason are only counted")
	}
}

// summary logs the stage's drop counts once its input ends, if it dropped anything.
func (n *normaliser) summary() {
	if n.drops == [numDrops]int{} {
		return
	}
	log := n.log
	for d := invalid; d < numDrops; d++ {
		log = log.With(d.String(), n.drops[d])
	}
	log.Warn("normalise dropped candles")
}
