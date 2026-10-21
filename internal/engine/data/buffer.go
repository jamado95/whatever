package data

import (
	"fmt"
	"math"
	"sync/atomic"

	proto "whatever/internal/protocol"
)

// maxHistoryMs caps the history a stream may keep: 365 days.
const maxHistoryMs = 365 * 24 * 60 * 60 * 1000

// LegState is the state of a stream's latest update at or before a column.
type LegState uint8

const (
	Fresh LegState = iota // its candle closed at the update column
	Gap                   // proven missing at the update column; carries the last Fresh candle
	Ended                 // the stream's lane closed; carries the last Fresh candle
)

/*
FrameBuffer holds every stream's market legs, one ring per stream with one slot per update. The
aligner writes direct slots for columns it has not released; the engine goroutine writes released
columns and reads through pinned Views. Nothing is locked: the frame event's channel send publishes
the aligner's slots, and headroom keeps its writes off the slots views read.
*/
type FrameBuffer struct {
	streams *Streams
	rings   []ring
}

type ring struct {
	slots []slot
	bars  int64        // bars kept for reads; the slots beyond them are headroom
	end   atomic.Int64 // Ended column; math.MaxInt64 while open
}

type slot struct {
	col   int64        // column held; -1 = empty
	fresh int64        // newest Fresh column at or before col; -1 = none
	c     proto.Candle // Fresh: its candle; Gap / Ended: the last Fresh candle
	state LegState
	count uint16 // derived: base candles in the window
}

/*
newFrameBuffer sizes one ring per stream. lookback is the longest read in bars, the same for every
stream; headroom is how many columns the aligner may write beyond the engine's column. A direct
stream keeps max(lookback, W+1, 1) bars, W the largest window derived from it; a derived stream keeps
max(lookback, 1) bars of Window slots each. Each ring adds ceil(headroom / Period) slots — the most
the aligner can write ahead of the engine's column — plus one spare. A stream whose kept bars span
more than 365 days is an error.
*/
func newFrameBuffer(streams *Streams, lookback int, headroom int64) (*FrameBuffer, error) {
	if lookback < 0 || headroom < 0 {
		return nil, fmt.Errorf("frame buffer: lookback %d and headroom %d must not be negative",
			lookback, headroom)
	}

	// largest window derived from each direct stream
	maxWindow := make([]int64, len(streams.All))
	for _, s := range streams.All[streams.Directs:] {
		maxWindow[s.Source] = max(maxWindow[s.Source], s.Window)
	}

	buf := &FrameBuffer{streams: streams, rings: make([]ring, len(streams.All))}
	for i, s := range streams.All {
		bars := max(int64(lookback), 1)
		if maxWindow[i] > 0 {
			bars = max(bars, maxWindow[i]+1)
		}
		if span := bars * s.Period * s.Window * streams.Stride; span > maxHistoryMs {
			return nil, fmt.Errorf("frame buffer: stream %s: %d bars of %s exceed the 365-day cap",
				s.Sub.ID(), bars, s.Sub.Timeframe)
		}

		ring := &buf.rings[i]
		ring.bars = bars
		ring.slots = make([]slot, bars*s.Window+(headroom+s.Period-1)/s.Period+1)
		for j := range ring.slots {
			ring.slots[j].col = -1
		}
		ring.end.Store(math.MaxInt64)
	}
	return buf, nil
}

// at returns the slot that holds col, or would.
func (r *ring) at(col, period int64) *slot {
	return &r.slots[(col/period)%int64(len(r.slots))]
}

/*
put writes stream s's slot at col, one of the stream's update columns. A Gap or Ended slot carries
the last Fresh candle from the stream's previous slot, or a zero candle when there is none; Ended
also records the end column. Rewriting a column replaces its slot.
*/
func (b *FrameBuffer) put(s int, col int64, c proto.Candle, state LegState, count uint16) {
	period := b.streams.All[s].Period
	r := &b.rings[s]

	sl := slot{col: col, fresh: col, c: c, state: state, count: count}
	if state != Fresh {
		sl.c, sl.fresh = proto.Candle{}, -1
		if pc := col - period; pc >= 0 {
			if prev := r.at(pc, period); prev.col == pc {
				sl.c, sl.fresh = prev.c, prev.fresh
			}
		}
	}
	*r.at(col, period) = sl

	if state == Ended {
		r.end.Store(col)
	}
}

// view returns the View pinned at col.
func (b *FrameBuffer) view(col int64, complete bool) View {
	return View{buf: b, col: col, complete: complete}
}
