// Package align provides pipeline combinators that operate across
// per-subscription feature chains — the synchronizer that aligns
// independent streams into SyncEventV2 frames.
package align

import (
	"sort"

	"whatever/internal/flow/pipe"
	proto "whatever/internal/protocol"
)

// Readiness selects which frames (columns) to dispatch and in what order,
// evaluating structurally over the synchronizer state. Different modules
// implement different dispatch policies (ordered, out-of-order, ...), each
// choosing its own evaluation scope through read-only state queries (ready,
// covers, dispatched, baseCol, maxCol).
type Readiness interface {
	// Evaluate is called after each accepted datapoint, identified by its
	// stream id and column, and returns the columns to dispatch, in dispatch
	// order. The runner skips already-dispatched columns. Must not mutate state.
	Evaluate(st *syncState, id string, col int64) []int64
}

/*
syncState is the time-aware buffer matrix. Columns are absolute time slots at
the finest granularity (stride); rows are subscriptions. cell[id][col] indexes
the datapoint in data[id] that closed at that column; an absent key is nil.
A datapoint below baseCol survives as floor[id] (at floorCol[id]) — the
forward-fill carry-in for in-window columns whose owning candle precedes the
window. A column dispatches only once every stream's owning candle exists, so
intra-stream gaps hold frames rather than leaking partial or stale ones.

Assumes every timeframe is an integer multiple of the finest present timeframe,
and CloseTs in ms; a close at or up to one stride before a boundary owns that
boundary's column.
*/
type syncState struct {
	stride int64                // ms per column = finest timeframe present
	subs   []proto.Subscription // stable order (sorted by ID)
	period map[string]int64     // subID -> columns per candle

	data     map[string][]proto.ExtendedMarketData // per-sub growing datapoints (compacted)
	cell     map[string]map[int64]int              // subID -> col -> index into data[subID]
	floor    map[string]proto.ExtendedMarketData   // subID -> latest datapoint below baseCol (fwd-fill carry-in)
	floorCol map[string]int64                      // subID -> column of floor

	dispatched map[int64]bool // in-window columns already emitted

	maxCol  int64 // highest column seen
	baseCol int64 // oldest column retained; -1 until first datapoint
	width   int64 // window span in columns
}

func newSyncState(subs []proto.Subscription) *syncState {
	st := &syncState{
		subs:       subs,
		period:     make(map[string]int64, len(subs)),
		data:       make(map[string][]proto.ExtendedMarketData, len(subs)),
		cell:       make(map[string]map[int64]int, len(subs)),
		floor:      make(map[string]proto.ExtendedMarketData, len(subs)),
		floorCol:   make(map[string]int64, len(subs)),
		dispatched: make(map[int64]bool),
		baseCol:    -1,
	}

	// stride = finest timeframe present; its multiples cover every other.
	minMs := int64(-1)
	for _, s := range subs {
		tf := s.Timeframe
		ms := tf.TimeframeMs()
		if minMs == -1 || ms < minMs {
			minMs = ms
		}
	}
	st.stride = minMs

	var maxPeriod int64 = 1
	for _, s := range subs {
		id := s.ID()
		tf := s.Timeframe
		p := tf.TimeframeMs() / st.stride
		st.period[id] = p
		st.cell[id] = make(map[int64]int)
		if p > maxPeriod {
			maxPeriod = p
		}
	}
	// width spans one coarse candle (maxTimestampS/minTimestampS) plus an equal
	// lag slack; a stream lagging past this has its stalled frames dropped.
	st.width = maxPeriod + maxPeriod
	return st
}

// column maps a timestamp to the first column boundary at or after it, so an
// exact-boundary close and one just before the boundary share a column.
// Assumes non-negative ts.
func column(ts, stride int64) int64 {
	return (ts + stride - 1) / stride
}

// owningCol is the latest column at or before c where the stream closes.
func (st *syncState) owningCol(id string, c int64) int64 {
	p := st.period[id]
	return c - (c % p)
}

// Apply ingests one datapoint and returns the cell it wrote (stream id, column).
// In-window datapoints are recorded in the matrix, advancing/compacting the
// window as the clock moves forward. A datapoint below the window is kept only
// when it owns the window's first column (a late floor); anything older is
// dropped (ok=false).
func (st *syncState) Apply(emd proto.ExtendedMarketData) (id string, col int64, ok bool) {
	id = emd.Subscription.ID()
	col = column(emd.Timestamp(), st.stride)

	switch {
	case st.baseCol < 0 || col >= st.baseCol:
		st.data[id] = append(st.data[id], emd)
		st.cell[id][col] = len(st.data[id]) - 1
		if col > st.maxCol {
			st.maxCol = col
		}
		newBase := st.maxCol - st.width + 1
		if newBase < 0 {
			newBase = 0
		}
		if st.baseCol < 0 {
			st.baseCol = newBase // first datapoint, nothing to evict yet
		} else if newBase > st.baseCol {
			st.evict(newBase)
			st.baseCol = newBase
		}
	case col == st.owningCol(id, st.baseCol):
		st.floor[id] = emd // late owner of the window's first columns
		st.floorCol[id] = col
	default:
		return "", 0, false // older than any retained column needs
	}
	return id, col, true
}

// covers returns the in-window column range [lo, hi] forward-filled by the
// stream's datapoint at col; ok=false when the range is empty.
func (st *syncState) covers(id string, col int64) (lo, hi int64, ok bool) {
	lo = max(col, st.baseCol)
	hi = min(col+st.period[id]-1, st.maxCol)
	return lo, hi, lo <= hi
}

// evict drops columns below newBase, promoting each stream's highest evicted
// datapoint to its floor, then compacts the underlying arrays.
func (st *syncState) evict(newBase int64) {
	for c := st.baseCol; c < newBase; c++ {
		delete(st.dispatched, c)
		for _, s := range st.subs {
			id := s.ID()
			idx, ok := st.cell[id][c]
			if !ok {
				continue
			}
			st.floor[id] = st.data[id][idx]
			st.floorCol[id] = c
			delete(st.cell[id], c)
		}
	}
	for _, s := range st.subs {
		st.compact(s.ID())
	}
}

// compact rebuilds data[id] to hold only still-referenced datapoints, rebasing
// the matrix indices. Runs lazily once dead entries dominate the array.
func (st *syncState) compact(id string) {
	cells := st.cell[id]
	data := st.data[id]
	if len(data) <= 2*len(cells)+8 {
		return
	}
	idxs := make([]int, 0, len(cells))
	for _, idx := range cells {
		idxs = append(idxs, idx)
	}
	sort.Ints(idxs)
	remap := make(map[int]int, len(idxs))
	nd := make([]proto.ExtendedMarketData, len(idxs))
	for ni, oi := range idxs {
		remap[oi] = ni
		nd[ni] = data[oi]
	}
	for c, oi := range cells {
		cells[c] = remap[oi]
	}
	st.data[id] = nd
}

// ready reports whether frame c is complete: every stream's owning candle at c
// exists. Cold-start streams and intra-stream gaps fail it.
func (st *syncState) ready(c int64) bool {
	for _, s := range st.subs {
		if _, ok := st.at(s.ID(), c); !ok {
			return false
		}
	}
	return true
}

// at returns the stream's forward-fill value at column c: the datapoint at its
// owning column, drawn from the matrix when in-window or the floor when below it.
func (st *syncState) at(id string, c int64) (proto.ExtendedMarketData, bool) {
	oc := st.owningCol(id, c)
	if oc >= st.baseCol {
		if idx, ok := st.cell[id][oc]; ok {
			return st.data[id][idx], true
		}
		return proto.ExtendedMarketData{}, false // gap: owning candle missing
	}
	if f, ok := st.floor[id]; ok && st.floorCol[id] == oc {
		return f, true
	}
	return proto.ExtendedMarketData{}, false
}

// legs materializes a ready column as one forward-filled datapoint per stream.
func (st *syncState) legs(c int64) []proto.ExtendedMarketData {
	legs := make([]proto.ExtendedMarketData, 0, len(st.subs))
	for _, s := range st.subs {
		if emd, ok := st.at(s.ID(), c); ok {
			legs = append(legs, emd)
		}
	}
	return legs
}

/*
== Ordered Current Readiness Policy ==

orderedCurrentReadiness dispatches each frame (column) as soon as it becomes
complete — every stream's owning candle present — without waiting for earlier
incomplete frames.

Per arriving datapoint it checks only the columns that datapoint forward-fills
and returns the complete ones in ascending column order; the runner drops any
already dispatched. A frame evicted before completing is never emitted.

Expected sequence: frames mostly ascend as Current. A frame completed after a
later one was dispatched (late or gap-filling candle) is emitted on its
completion, behind the newest frame, and the runner tags it Stale. E.g. 5m
cols 0..23 with 1h@12 before 1h@0 yields 12..23 Current, then 0..11 Stale.
*/
type orderedCurrentReadiness struct{}

func (orderedCurrentReadiness) Evaluate(st *syncState, id string, col int64) []int64 {
	lo, hi, ok := st.covers(id, col)
	if !ok {
		return nil
	}
	var cols []int64
	for c := lo; c <= hi; c++ {
		if st.ready(c) {
			cols = append(cols, c)
		}
	}
	return cols
}

/*
== SynchronizerV2 ==

SynchronizerV2 aligns N subscription streams into SyncEventV2 frames over a
sliding window of columns at the finest timeframe (others must be multiples).
Evicted data survives only as each stream's forward-fill floor.

Pipeline, per datapoint: Apply records it → Readiness picks columns to dispatch
→ the runner emits them. Readiness only decides which complete frames ship and
in what order, via read-only queries; storage, completeness, dispatch tracking
and freshness status stay with the synchronizer.

Invariants: a frame is complete only when every stream's owning candle exists;
each event holds one forward-filled leg per stream, closed at or before its
column; a column dispatches at most once, Current if newest so far, else Stale.

Lanes merge in arrival order (pipe.FanIn) by default, or in CloseTs order via
NewOrderedSynchronizer.
*/
type SynchronizerV2 struct {
	readiness Readiness
	merge     func(...<-chan proto.ExtendedMarketData) <-chan proto.ExtendedMarketData
}

// NewSynchronizerV2 builds a synchronizer with the given readiness module,
// defaulting to orderedCurrentReadiness.
func NewSynchronizerV2(rdn Readiness) *SynchronizerV2 {
	if rdn == nil {
		rdn = orderedCurrentReadiness{}
	}
	return &SynchronizerV2{readiness: rdn, merge: pipe.FanIn[proto.ExtendedMarketData]}
}

// NewOrderedSynchronizer builds a SynchronizerV2 that merges lanes in CloseTs
// order (pipe.OrderedFanIn) instead of arrival order, so lane skew never reaches
// the window. Replay only: the merge waits while any open lane has no pending
// datapoint.
func NewOrderedSynchronizer(rdn Readiness) *SynchronizerV2 {
	s := NewSynchronizerV2(rdn)
	s.merge = func(ins ...<-chan proto.ExtendedMarketData) <-chan proto.ExtendedMarketData {
		return pipe.OrderedFanIn(func(a, b proto.ExtendedMarketData) bool {
			if a.Timestamp() != b.Timestamp() {
				return a.Timestamp() < b.Timestamp()
			}
			return a.Subscription.ID() < b.Subscription.ID() // deterministic ties
		}, ins...)
	}
	return s
}

// Sync fan-ins the per-subscription lanes and emits aligned SyncEventV2s.
func (s *SynchronizerV2) Sync(subs map[proto.Subscription]<-chan proto.ExtendedMarketData) <-chan *proto.SyncEventV2 {
	if len(subs) == 0 {
		out := make(chan *proto.SyncEventV2)
		close(out)
		return out
	}
	chans := make([]<-chan proto.ExtendedMarketData, 0, len(subs))
	list := make([]proto.Subscription, 0, len(subs))
	for sub, ch := range subs {
		chans = append(chans, ch)
		list = append(list, sub)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ID() < list[j].ID() })
	return runV2(s.merge(chans...), list, s.readiness)
}

// runV2 is the core alignment loop over a pre-merged channel, so tests can
// inject a controlled arrival order. A column after the latest dispatched one is
// Current; a column dispatched behind it (a late-completed frame) is Stale.
func runV2(merged <-chan proto.ExtendedMarketData, subs []proto.Subscription, rdn Readiness) <-chan *proto.SyncEventV2 {
	out := make(chan *proto.SyncEventV2)
	go func() {
		defer close(out)
		st := newSyncState(subs)
		hwm := int64(-1) // latest dispatched column
		for emd := range merged {
			id, col, ok := st.Apply(emd)
			if !ok {
				continue
			}
			for _, c := range rdn.Evaluate(st, id, col) {
				if st.dispatched[c] {
					continue // dispatch-once
				}
				status := proto.CurrentMarketData
				if c < hwm {
					status = proto.StaleMarketData
				} else {
					hwm = c
				}
				st.dispatched[c] = true
				ev, err := proto.NewSyncEventV2(status, st.legs(c))
				if err != nil {
					continue // unreachable: a ready column has one leg per stream
				}
				out <- ev
			}
		}
	}()
	return out
}
