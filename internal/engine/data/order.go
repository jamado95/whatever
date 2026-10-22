package data

import "whatever/internal/flow/pipe"

// before orders the merged stream by (column, stream).
func before(a, b laneItem) bool {
	return a.col < b.col || a.col == b.col && a.stream < b.stream
}

// orderLanes is the replay order stage: OrderedFanIn over the lanes, by (column, stream). A lane's
// provider blocks while its next value waits in the merge.
func orderLanes(lanes ...<-chan laneItem) <-chan laneItem {
	return pipe.OrderedFanIn(before, lanes...)
}
