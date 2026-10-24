package data

import (
	"context"
	"sync"

	"whatever/internal/logger"
	proto "whatever/internal/protocol"
)

/*
lane is one direct stream's chain of stages, built by New and run by Start: ingest (its provider),
normalise, then align, one goroutine each. The live lane (iteration 2) adds the deadline stage
between normalise and align.
*/
type lane struct {
	provider  proto.DataProvider // initialised by the engine
	data      <-chan proto.MarketData
	errs      <-chan error
	normalise *normaliser
	align     *aligner
	closing   *sync.WaitGroup // the layer's: done once the provider is closed
}

func (ln *lane) Subscription() proto.Subscription {
	return ln.provider.Subscription()
}

// newLane builds direct stream s's lane over its initialised provider; its align stage waits on g for
// ring capacity.
func newLane(
	buf *FrameBuffer,
	g *gate,
	s int,
	p proto.DataProvider,
	closing *sync.WaitGroup,
	log *logger.Logger,
) *lane {
	data, errs := p.Streams()
	return &lane{
		provider:  p,
		data:      data,
		errs:      errs,
		normalise: newNormaliser(buf.streams, s, log),
		align:     newAligner(buf, g, s),
		closing:   closing,
	}
}

/*
run starts the lane's provider, then chains normalise and align over its output; it returns the
lane's evaluates and the provider's errors. Once ctx is done the provider is closed. Nothing runs if
the provider fails to start.
*/
func (ln *lane) run(ctx context.Context) (<-chan evaluate, <-chan error, error) {
	if err := ln.provider.Start(); err != nil {
		return nil, nil, err
	}

	// register ctx close callback that shutdowns running provider
	ln.closing.Add(1)
	context.AfterFunc(ctx, func() {
		defer ln.closing.Done()
		ln.provider.Close()
	})

	// ingestion -> normalise -> align
	composition := ln.align.run(ln.normalise.run(ln.data))
	return composition, ln.errs, nil
}
