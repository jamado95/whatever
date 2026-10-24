package data

import (
	"context"
	"errors"
	"fmt"
	"sync"

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
Layer is the data layer of one run, and its pipeline: the stream table, the FrameBuffer, one lane per
direct stream, readiness and the aggregator. New builds it, Start runs it, Next hands the engine one
frame at a time and Close stops it. Next and Close belong to the engine goroutine and must not run
concurrently.
*/
type Layer struct {
	streams *Streams
	buf     *FrameBuffer
	gate    *gate
	lanes   []*lane // per direct stream, in stream-table order
	ready   *readiness
	agg     *aggregator
	log     *logger.Logger

	started, closed bool
	cancel          context.CancelFunc // the run context's; nil until Start
	closing         *sync.WaitGroup    // the started providers, until closed
	views           <-chan View        // nil until Start
	errsDone        <-chan struct{}    // closed once every running provider's error channel has closed
	now             View               // the frame Next returned last
}

/*
New builds the data layer over provider instances keyed by their direct subscription, each already
initialised for it, and instantiates its stages. lookback is the longest read in bars, the same for
every stream. It performs no I/O and starts nothing; Start runs the layer.
*/
func New(providers map[proto.Subscription]proto.DataProvider, lookback int, log *logger.Logger) (*Layer, error) {
	return newLayer(providers, lookback, defaultHeadroom, log)
}

// newLayer is New with the rings' headroom in columns.
func newLayer(
	providers map[proto.Subscription]proto.DataProvider,
	lookback int,
	headroom int64,
	log *logger.Logger,
) (*Layer, error) {
	subs := make([]proto.Subscription, 0, len(providers))
	for sub := range providers {
		subs = append(subs, sub)
	}
	streams := NewStreams(subs)

	buf, err := newFrameBuffer(streams, lookback, headroom)
	if err != nil {
		return nil, fmt.Errorf("data: %w", err)
	}

	gate := newGate()
	readiness := newReadiness(buf)
	aggregator := newAggregator(buf)
	lanes := make([]*lane, 0, streams.Directs)
	closing := new(sync.WaitGroup)

	// a map holds one provider per subscription, but not one subscription per provider
	owner := make(map[proto.DataProvider]string, streams.Directs)
	for i, s := range streams.All[:streams.Directs] {
		p := providers[s.Sub]
		if p == nil {
			return nil, fmt.Errorf("data: subscription %q has a nil provider", s.Sub.ID())
		}
		if other, shared := owner[p]; shared {
			return nil, fmt.Errorf("data: subscriptions %q and %q share one provider instance",
				other, s.Sub.ID())
		}
		owner[p] = s.Sub.ID()
		lanes = append(lanes, newLane(buf, gate, i, p, closing, log))
	}

	return &Layer{
		streams: streams,
		buf:     buf,
		gate:    gate,
		lanes:   lanes,
		ready:   readiness,
		agg:     aggregator,
		closing: closing,
		log:     log,
	}, nil
}

func (l *Layer) Streams() *Streams { return l.streams }

/*
Start runs each lane under the layer's run context — its provider, then its stages — and joins their
evaluates into readiness; provider errors are logged. A provider failing to start closes the layer,
the lanes already running included, and Start returns its error. A layer runs once: Start again is
an error, and after Close it returns ErrClosed.
*/
func (l *Layer) Start() error {
	switch {
	case l.closed:
		return ErrClosed
	case l.started:
		return errors.New("data: layer already started")
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.cancel, l.started = cancel, true

	evals := make([]<-chan evaluate, 0, len(l.lanes))
	errs := make([]<-chan error, 0, len(l.lanes))
	var failed error
	for _, ln := range l.lanes {
		ev, er, err := ln.run(ctx)
		if err != nil {
			failed = fmt.Errorf("data: provider of %s: start: %w", ln.Subscription().ID(), err)
			break
		}
		evals, errs = append(evals, ev), append(errs, er)
	}

	l.errsDone = pipe.Forward(pipe.FanIn(errs...), func(err error) { l.log.Error(err, "provider error") })
	l.views = l.ready.run(pipe.FanIn(evals...))

	// Close drains the lanes already running
	if failed != nil {
		l.Close()
		return failed
	}
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
	case v, ok := <-l.views:
		if !ok {
			return nil, nil
		}
		// aggregate derived streams
		l.agg.step(v.col)
		// advance gate to col
		l.gate.consume(v.col)
		l.now = v

		return &l.now, nil
	}
}

/*
Close stops the run: it cancels the run context, so every running lane closes its provider, stops
the gate and discards the remaining frames until every stage has exited, then waits for the providers
to close. It returns with the output drained. A layer never started has nothing to stop. Next returns
ErrClosed from then on. It must not run concurrently with Next.
*/
func (l *Layer) Close() {
	if l.closed {
		return
	}
	l.closed = true
	if l.cancel == nil {
		return
	}

	l.cancel()
	l.gate.stop()

	//discard any remaining frames
	for range l.views {
	}

	l.closing.Wait()
	<-l.errsDone
}
