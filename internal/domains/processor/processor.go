package processor

import (
	"fmt"
	"whatever/types"
)

type Chain struct {
	processors []Processor
}

func NewChain(processors []Processor) (*Chain, error) {
	sorted, err := topologicalSort(processors)
	if err != nil {
		return nil, err
	}
	return &Chain{processors: sorted}, nil
}

func (c *Chain) Process(in <-chan types.MarketData) <-chan EnrichedMarketData {
	out := make(chan EnrichedMarketData)

	go func() {
		defer close(out)

		for candle := range in {
			snap := NewSnapshot()

			for _, proc := range c.processors {
				proc.Update(candle, snap)
			}

			out <- EnrichedMarketData{
				MarketData: candle,
				Indicators: snap,
			}
		}
	}()

	return out
}

func (c *Chain) AvailableKeys() []KeyRef {
	keys := make([]KeyRef, len(c.processors))
	for i, proc := range c.processors {
		keys[i] = KeyRef{Name: proc.ID()}
	}
	return keys
}

func topologicalSort(processors []Processor) ([]Processor, error) {
	idToProcessor := make(map[string]Processor)
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

	var queue []Processor
	for _, p := range processors {
		if inDegree[p.ID()] == 0 {
			queue = append(queue, p)
		}
	}

	var sorted []Processor
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
