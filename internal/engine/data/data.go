package data

import (
	"context"
	"errors"
	"fmt"

	"whatever/internal/flow/pipe"
	"whatever/internal/logger"
	proto "whatever/internal/protocol"
)

var (
	// ErrNotStarted is returned by Next before Start.
	ErrNotStarted = errors.New("data: layer not started")
	// ErrClosed is returned by Next and Start after Close.
	ErrClosed = errors.New("data: layer closed")
)

/*
Layer is the data layer of one run: the stream table, the FrameBuffer and the replay pipeline. New
builds it, Start runs it, Next hands the engine one frame at a time and Close stops it. Next and
Close belong to the engine goroutine and must not run concurrently.
*/
type Layer struct {
	streams   *Streams
	buf       *FrameBuffer
	gate      *gate
	agg       *aggregator
	stages    pipeline
	providers []proto.DataProvider // per direct stream, in stream-table order
	log       *logger.Logger

	started, closed bool
	events          <-chan View     // nil until Start
	errsDone        <-chan struct{} // closed once every provider's error channel has
	view            View            // the frame Next returned last
}

/*
New builds the data layer over validated direct subscriptions and their provider instances, one per
subscription, and instantiates its pipeline. lookback is the longest read in bars, the same for
every stream. It performs no I/O and starts nothing; Start runs the layer.
*/
func New(subs []proto.Subscription, providers map[proto.Subscription]proto.DataProvider,
	lookback int, log *logger.Logger) (*Layer, error) {
	return newLayer(subs, providers, lookback, defaultHeadroom, log)
}

// newLayer is New with the rings' headroom in columns.
func newLayer(subs []proto.Subscription, providers map[proto.Subscription]proto.DataProvider,
	lookback int, headroom int64, log *logger.Logger) (*Layer, error) {
	streams := NewStreams(subs)
	buf, err := newFrameBuffer(streams, lookback, headroom)
	if err != nil {
		return nil, fmt.Errorf("data: %w", err)
	}

	provs := make([]proto.DataProvider, streams.Directs)
	owner := make(map[proto.DataProvider]string, streams.Directs)
	for i, s := range streams.All[:streams.Directs] {
		p, ok := providers[s.Sub]
		if !ok || p == nil {
			return nil, fmt.Errorf("data: subscription %q has no provider", s.Sub.ID())
		}
		if other, shared := owner[p]; shared {
			return nil, fmt.Errorf("data: subscriptions %q and %q share one provider instance",
				other, s.Sub.ID())
		}
		owner[p] = s.Sub.ID()
		provs[i] = p
	}

	g := newGate()
	return &Layer{
		streams:   streams,
		buf:       buf,
		gate:      g,
		agg:       newAggregator(buf),
		stages:    replayPipeline(buf, g, log),
		providers: provs,
		log:       log,
	}, nil
}

func (l *Layer) Streams() *Streams { return l.streams }

/*
Start initialises and starts each direct stream's provider, then runs the pipeline over their
outputs; provider errors are logged. If a provider fails to start, the ones already started are
closed and the layer stays unstarted. A layer runs once: Start again is an error, and after Close it
returns ErrClosed.
*/
func (l *Layer) Start() error {
	switch {
	case l.closed:
		return ErrClosed
	case l.started:
		return errors.New("data: layer already started")
	}

	ins := make([]<-chan proto.MarketData, len(l.providers))
	errs := make([]<-chan error, len(l.providers))
	for i, p := range l.providers {
		sub := l.streams.All[i].Sub
		if err := p.Init(sub); err != nil {
			return fmt.Errorf("data: provider of %s: init: %w", sub.ID(), err)
		}
		ins[i], errs[i] = p.Streams()
	}
	for i, p := range l.providers {
		if err := p.Start(); err != nil {
			for _, started := range l.providers[:i] {
				started.Close()
			}
			return fmt.Errorf("data: provider of %s: start: %w", l.streams.All[i].Sub.ID(), err)
		}
	}

	l.errsDone = pipe.Forward(pipe.FanIn(errs...), func(err error) { l.log.Error(err, "provider error") })
	l.events = l.stages.run(ins)
	l.started = true
	return nil
}

/*
Next returns the next frame: the View pinned at the next released column, its derived legs
aggregated. It returns nil, nil once the run has ended; ErrNotStarted before Start; ErrClosed after
Close; ctx.Err() once ctx is done. The View is valid until the next call, which consumes its column.
*/
func (l *Layer) Next(ctx context.Context) (*View, error) {
	switch {
	case l.closed:
		return nil, ErrClosed
	case !l.started:
		return nil, ErrNotStarted
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case v, ok := <-l.events:
		if !ok {
			return nil, nil
		}
		l.agg.step(v.col)
		l.gate.consume(v.col)
		l.view = v
		return &l.view, nil
	}
}

/*
Close stops the run: it closes the providers, stops the gate and discards the remaining frames until
every stage has exited, so it returns with the output drained. A layer never started has nothing to
stop. Next returns ErrClosed from then on. It must not run concurrently with Next.
*/
func (l *Layer) Close() {
	if l.closed {
		return
	}
	l.closed = true
	if !l.started {
		return
	}

	for _, p := range l.providers {
		p.Close()
	}
	l.gate.stop()
	for range l.events {
	}
	<-l.errsDone
}
