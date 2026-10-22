package data

import (
	"whatever/internal/flow/pipe"
	"whatever/internal/logger"
	proto "whatever/internal/protocol"
)

/*
pipeline is the data layer's stage graph, wired in one place. Each field is one stage; the per-mode
stages are injected, so live ordering (iteration 2) swaps order and tests can replace any stage.
Stage 4 adds align; stage 5 wraps the pipeline in the module API.
*/
type pipeline struct {
	normalise func(stream int) pipe.Stage[proto.MarketData, laneItem] // one lane per direct stream
	order     pipe.Merge[laneItem]                                    // replay: orderLanes; live: FanIn
}

// replayPipeline builds the replay stages: a lane per direct stream, ordered by (column, stream).
func replayPipeline(streams *Streams, log *logger.Logger) pipeline {
	return pipeline{
		normalise: func(stream int) pipe.Stage[proto.MarketData, laneItem] {
			return newLane(streams, stream, log).run
		},
		order: orderLanes,
	}
}

// run wires the stages; ins[i] is direct stream i's provider output.
func (p pipeline) run(ins []<-chan proto.MarketData) <-chan laneItem {
	lanes := make([]<-chan laneItem, len(ins))
	for i, in := range ins {
		lanes[i] = p.normalise(i)(in)
	}
	return p.order(lanes...)
}
