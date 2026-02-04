package processor

import (
	"fmt"
	proto "whatever/internal/protocol"
)

type Chain struct {
	processors []proto.Processor
}

func NewChain(processors []proto.Processor) (*Chain, error) {
	sorted, err := topologicalSort(processors)
	if err != nil {
		return nil, err
	}
	return &Chain{processors: sorted}, nil
}

func (c *Chain) Process(in <-chan proto.MarketData) <-chan proto.EnrichedMarketData {
	out := make(chan proto.EnrichedMarketData)

	go func() {
		defer close(out)

		for candle := range in {
			snap := proto.Snapshot{}

			for _, proc := range c.processors {
				proc.Update(candle, &snap)
			}

			out <- proto.EnrichedMarketData{
				MarketData: candle,
				Indicators: &snap,
			}
		}
	}()

	return out
}

func (c *Chain) AvailableKeys() []proto.KeyRef {
	keys := make([]proto.KeyRef, len(c.processors))
	for i, proc := range c.processors {
		keys[i] = proto.KeyRef{Name: proc.ID()}
	}
	return keys
}

func topologicalSort(processors []proto.Processor) ([]proto.Processor, error) {
	idToProcessor := make(map[string]proto.Processor)
	for _, p := range processors {
		idToProcessor[p.ID()] = p
	}

	inDegree := make(map[string]int)
	dependents := make(map[string][]string)

	for _, p := range processors {
		inDegree[p.ID()] = len(p.Dependencies())
		for _, dep := range p.Dependencies() {
			dependents[dep.Name] = append(dependents[dep.Name], p.ID())
		}
	}

	var queue []proto.Processor
	for _, p := range processors {
		if inDegree[p.ID()] == 0 {
			queue = append(queue, p)
		}
	}

	var sorted []proto.Processor
	for len(queue) > 0 {
		proc := queue[0]
		queue = queue[1:]
		sorted = append(sorted, proc)

		for _, depID := range dependents[proc.ID()] {
			inDegree[depID]--
			if inDegree[depID] == 0 {
				queue = append(queue, idToProcessor[depID])
			}
		}
	}

	if len(sorted) != len(processors) {
		return nil, fmt.Errorf("circular dependency detected in processors")
	}

	return sorted, nil
}
