package data

import (
	"math"

	"whatever/internal/logger"
	proto "whatever/internal/protocol"
)

// laneItem is one value of a lane and of the merged stream: a candle at its column, or the lane's
// end marker.
type laneItem struct {
	col    int64        // candle: the column it closes in; end: the lane's next due column, −1 if it had no candles
	stream int          // index in the stream table
	end    bool         // the stream has nothing at col or later
	c      proto.Candle // candle: validated, ms timestamps
}

// drop is why a lane rejected a candle; keep accepts it.
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

// laneBuffer is a lane's output capacity, matching the provider and feature-chain buffers.
const laneBuffer = 100

// lane is the normalise stage of one direct stream; only its goroutine touches its state.
type lane struct {
	streams *Streams
	stream  int
	tfMs    int64
	last    proto.Candle // last accepted candle
	lastCol int64        // its column; −1 before the first
	drops   [numDrops]int
	log     *logger.Logger
}

func newLane(streams *Streams, stream int, log *logger.Logger) *lane {
	s := streams.All[stream]
	return &lane{
		streams: streams,
		stream:  stream,
		tfMs:    s.Sub.Timeframe.TimeframeMs(),
		lastCol: -1,
		log:     log.With("stream", s.Sub.ID()),
	}
}

/*
run normalises in: accepted candles at their columns, then the end marker and, if anything was
dropped, a summary line once in closes. It is a pipe.Stage.
*/
func (l *lane) run(in <-chan proto.MarketData) <-chan laneItem {
	out := make(chan laneItem, laneBuffer)

	go func() {
		defer close(out)
		for md := range in {
			col, d := l.accept(md.Candle)
			if d != keep {
				l.dropped(d, md.Candle)
				continue
			}
			out <- laneItem{col: col, stream: l.stream, c: md.Candle}
		}
		out <- l.end()
		l.summary()
	}()

	return out
}

/*
accept applies the rules in order — validate, column, due, duplicate, order — and returns the
candle's column, or why it was dropped. Only an accepted candle moves the lane's state.
*/
func (l *lane) accept(c proto.Candle) (int64, drop) {
	if !valid(c, l.tfMs) {
		return 0, invalid
	}
	col := l.streams.column(c.CloseTs)
	switch {
	case col%l.streams.All[l.stream].Period != 0:
		return col, offDue
	case c == l.last:
		return col, duplicate
	case col <= l.lastCol:
		return col, outOfOrder
	}
	l.last, l.lastCol = c, col
	return col, keep
}

// valid reports whether c has ms timestamps, finite and consistent OHLCV, and a span that fits tfMs.
func valid(c proto.Candle, tfMs int64) bool {
	span := c.CloseTs - c.OpenTs
	return c.OpenTs > 0 && c.CloseTs < msLimit &&
		span >= tfMs-spanTolerance && span <= tfMs &&
		finite(c.Open) && finite(c.High) && finite(c.Low) && finite(c.Close) && finite(c.Volume) &&
		c.Volume >= 0 && c.Low <= min(c.Open, c.Close) && c.High >= max(c.Open, c.Close)
}

func finite(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }

// end is the lane's end marker: at its next due column, or −1 if it accepted no candle.
func (l *lane) end() laneItem {
	col := int64(-1)
	if l.lastCol >= 0 {
		col = l.lastCol + l.streams.All[l.stream].Period
	}
	return laneItem{col: col, stream: l.stream, end: true}
}

// dropped counts a drop and warns on the first of each rule; duplicates are only counted.
func (l *lane) dropped(d drop, c proto.Candle) {
	l.drops[d]++
	if l.drops[d] == 1 && d != duplicate {
		l.log.With("reason", d.String()).With("close_ts", c.CloseTs).
			Warn("dropping candle; further drops for this reason are only counted")
	}
}

// summary logs the lane's drop counts once it ends, if it dropped anything.
func (l *lane) summary() {
	if l.drops == [numDrops]int{} {
		return
	}
	log := l.log
	for d := invalid; d < numDrops; d++ {
		log = log.With(d.String(), l.drops[d])
	}
	log.Warn("lane dropped candles")
}
