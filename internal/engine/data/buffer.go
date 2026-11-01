package data

import (
	"fmt"

	"whatever/internal/flow/frame"
	proto "whatever/internal/protocol"
)

// maxHistoryMs caps the history a stream may keep: 365 days.
const maxHistoryMs = 365 * 24 * 60 * 60 * 1000

// defaultHeadroom sizes each ring's slots beyond its kept bars, in columns.
const defaultHeadroom = 128

/*
FrameBuffer holds every stream's market legs, one frame ring per stream with one slot per update. A
direct stream's align stage writes its slots until it sends their evaluate; the engine goroutine
writes released columns and reads through pinned Views. Nothing is locked: the evaluate and event
sends publish the align stages' slots, and ring capacity keeps their writes off the slots views read.
*/
type FrameBuffer struct {
	streams *proto.Streams
	buf     *frame.Buffer[proto.Candle]
}

/*
newFrameBuffer sizes one ring per stream. lookback is the longest read in bars, the same for every
stream; headroom is how many columns an align stage may write beyond the engine's column once its
ring holds full history. A direct stream keeps max(lookback, W+1, 1) bars, W the largest window
derived from it; a derived stream keeps max(lookback, 1) bars of Window slots each. Each ring adds
ceil(headroom / Period) slots plus one spare. A stream whose kept bars span more than 365 days is an
error.
*/
func newFrameBuffer(streams *proto.Streams, lookback int, headroom int64) (*FrameBuffer, error) {
	if lookback < 0 || headroom < 0 {
		return nil, fmt.Errorf("frame buffer: lookback %d and headroom %d must not be negative",
			lookback, headroom)
	}

	// largest window derived from each direct stream
	maxWindow := make([]int64, len(streams.All))
	for _, s := range streams.All[streams.Directs:] {
		maxWindow[s.Source] = max(maxWindow[s.Source], s.Window)
	}

	shapes := make([]frame.Shape, len(streams.All))
	for i, s := range streams.All {
		bars := max(int64(lookback), 1)
		if maxWindow[i] > 0 {
			bars = max(bars, maxWindow[i]+1)
		}
		if span := bars * s.Period * s.Window * streams.Stride; span > maxHistoryMs {
			return nil, fmt.Errorf("frame buffer: stream %s: %d bars of %s exceed the 365-day cap",
				s.Sub.ID(), bars, s.Sub.Timeframe)
		}

		shapes[i] = frame.Shape{
			Slots:  bars*s.Window + (headroom+s.Period-1)/s.Period + 1,
			Period: s.Period,
			Window: s.Window,
			Bars:   bars,
		}
	}

	buf := frame.NewBuffer[proto.Candle](shapes)

	return &FrameBuffer{streams: streams, buf: buf}, nil
}

// ring returns stream s's ring.
func (b *FrameBuffer) ring(s int) *frame.Ring[proto.Candle] {
	return b.buf.Ring(s)
}

/*
start records stream s's first column, before its first put. Reads never go below it, so the
stream's align stage may fill the whole ring before anything is read, wrapping onto the slots of
columns before it.
*/
func (b *FrameBuffer) start(s int, col int64) {
	b.ring(s).Start(col)
}

/*
put writes stream s's slot at col, one of the stream's update columns. A Gap or Ended slot carries
the last Fresh candle from the stream's previous slot, or a zero candle when there is none; Ended
also records the end column. Rewriting a column replaces its slot.
*/
func (b *FrameBuffer) put(s int, col int64, c proto.Candle, state frame.State) {
	b.ring(s).Put(col, c, state)
}

// view returns the View pinned at col, a Current frame.
func (b *FrameBuffer) view(col int64, complete bool) frame.View[proto.Candle] {
	return frame.NewView(col, b.streams.Stride, frame.Current, complete, b.buf)
}
