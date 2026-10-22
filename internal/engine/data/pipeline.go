package data

import (
	"whatever/internal/flow/pipe"
	"whatever/internal/logger"
	proto "whatever/internal/protocol"
)

/*
pipeline is the data layer's stage graph, wired in one place. Each direct stream gets a lane: a
chain of per-stream stages, one goroutine each. Stage 4 chains align onto each lane and joins the
lanes with readiness; the live pipeline (iteration 2) inserts the deadline stage; stage 5 wraps the
pipeline in the module API.
*/
type pipeline struct {
	normalise func(stream int) pipe.Stage[proto.MarketData, laneItem]
}

// replayPipeline builds the replay stages: each lane is normalise only, until stage 4.
func replayPipeline(streams *Streams, log *logger.Logger) pipeline {
	return pipeline{
		normalise: func(stream int) pipe.Stage[proto.MarketData, laneItem] {
			return newNormaliser(streams, stream, log).run
		},
	}
}

// run starts one lane per direct stream; ins[i] is direct stream i's provider output, and the
// result's i-th channel is its lane's output.
func (p pipeline) run(ins []<-chan proto.MarketData) []<-chan laneItem {
	lanes := make([]<-chan laneItem, len(ins))
	for i, in := range ins {
		lanes[i] = p.normalise(i)(in)
	}
	return lanes
}
