package data

import (
	"whatever/internal/flow/pipe"
	"whatever/internal/logger"
	proto "whatever/internal/protocol"
)

/*
pipeline is the data layer's stage graph, wired in one place. Each direct stream gets a lane: a
chain of per-stream stages, one goroutine each — normalise, then align. pipe.FanIn joins the lanes'
evaluates into readiness, which emits the frames as Views. The live pipeline (iteration 2) inserts the
deadline stage; Layer wraps the pipeline in the module API.
*/
type pipeline struct {
	normalise func(stream int) pipe.Stage[proto.MarketData, laneItem]
	align     func(stream int) pipe.Stage[laneItem, evaluate]
	ready     pipe.Stage[evaluate, View]
}

// replayPipeline builds the replay stages over buf; align stages wait on g for ring capacity.
func replayPipeline(buf *FrameBuffer, g *gate, log *logger.Logger) pipeline {
	return pipeline{
		normalise: func(stream int) pipe.Stage[proto.MarketData, laneItem] {
			return newNormaliser(buf.streams, stream, log).run
		},
		align: func(stream int) pipe.Stage[laneItem, evaluate] {
			return newAligner(buf, g, stream).run
		},
		ready: newReadiness(buf).run,
	}
}

// lanes starts each direct stream's lane up to normalise; ins[i] is direct stream i's provider
// output, and the result's i-th channel is its lane's normalised candles.
func (p pipeline) lanes(ins []<-chan proto.MarketData) []<-chan laneItem {
	lanes := make([]<-chan laneItem, len(ins))
	for i, in := range ins {
		lanes[i] = p.normalise(i)(in)
	}
	return lanes
}

// run chains align onto each lane, joins their evaluates with pipe.FanIn and returns readiness's
// frames; ins[i] is direct stream i's provider output.
func (p pipeline) run(ins []<-chan proto.MarketData) <-chan View {
	lanes := p.lanes(ins)
	evs := make([]<-chan evaluate, len(lanes))
	for i, lane := range lanes {
		evs[i] = p.align(i)(lane)
	}
	return p.ready(pipe.FanIn(evs...))
}
