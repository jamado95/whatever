package data

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"whatever/internal/flow/pipe"
	"whatever/internal/logger"
	proto "whatever/internal/protocol"
)

var (
	// ErrNotStarted is returned by Next before Start.
	ErrNotStarted = errors.New("data: pipeline not started")
	// ErrClosed is returned by Next and Start after Close.
	ErrClosed = errors.New("data: pipeline closed")
)

/*
DataPipeline is the data layer of one run: the stream table, the FrameBuffer, one lane per
direct stream, readiness and the aggregator. New builds it, Start runs it, Next hands the engine one
frame at a time and Close stops it. Next and Close belong to the engine goroutine and must not run
concurrently.
*/
type DataPipeline struct {
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
func New(providers map[proto.Subscription]proto.DataProvider, lookback int, log *logger.Logger) (*DataPipeline, error) {
	return newDataPipeline(providers, lookback, defaultHeadroom, log)
}

// newDataPipeline is New with the rings' headroom in columns.
func newDataPipeline(
	providers map[proto.Subscription]proto.DataProvider,
	lookback int,
	headroom int64,
	log *logger.Logger,
) (*DataPipeline, error) {
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

	return &DataPipeline{
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

func (dp *DataPipeline) Streams() *Streams { return dp.streams }

/*
Start runs each lane under the layer's run context — its provider, then its stages — and joins their
evaluates into readiness; provider errors are logged. A provider failing to start frees the gate —
readiness releases nothing while a lane never started, so Close would have no frame to consume — then
closes the layer, the lanes already running included, and Start returns its error. A layer runs
once: Start again is an error, and after Close it returns ErrClosed.
*/
func (dp *DataPipeline) Start() error {
	switch {
	case dp.closed:
		return ErrClosed
	case dp.started:
		return errors.New("data: pipeline already started")
	}
	ctx, cancel := context.WithCancel(context.Background())
	dp.cancel, dp.started = cancel, true

	evals := make([]<-chan evaluate, 0, len(dp.lanes))
	errs := make([]<-chan error, 0, len(dp.lanes))
	var failed error
	for _, ln := range dp.lanes {
		ev, er, err := ln.run(ctx)
		if err != nil {
			failed = fmt.Errorf("data: provider of %s: start: %w", ln.Subscription().ID(), err)
			break
		}
		evals, errs = append(evals, ev), append(errs, er)
	}

	dp.errsDone = pipe.Forward(pipe.FanIn(errs...), func(err error) { dp.log.Error(err, "provider error") })
	dp.views = dp.ready.run(pipe.FanIn(evals...))

	// Close and drain the lanes already running
	if failed != nil {
		// readiness releases nothing, so reads nothing, while a lane never started: free the gate
		dp.gate.consume(math.MaxInt64)
		dp.Close()
		return failed
	}
	return nil
}

/*
Next returns the next frame: the View pinned at the next released column, its derived legs
aggregated. It returns nil, nil once the run has ended; ErrNotStarted before Start; ErrClosed after
Close; ctx.Err() once ctx is done. The View is valid until the next call, which consumes its column.
*/
func (dp *DataPipeline) Next(ctx context.Context) (*View, error) {
	switch {
	case dp.closed:
		return nil, ErrClosed
	case !dp.started:
		return nil, ErrNotStarted
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case v, ok := <-dp.views:
		if !ok {
			return nil, nil
		}
		// aggregate derived streams
		dp.agg.step(v.col)
		// advance gate to col
		dp.gate.consume(v.col)
		dp.now = v

		return &dp.now, nil
	}
}

/*
Close stops the run: it cancels the run context, so every running lane closes its provider, and
discards the remaining frames until every stage has exited, consuming each so align stages waiting
on ring capacity proceed; then it waits for the providers to close. It returns with the output
drained. The View from the last Next is invalid from then on: the consumed frames let align wrap
onto its oldest bars. A layer never started has nothing to stop. Next returns ErrClosed from then on.
It must not run concurrently with Next.
*/
func (dp *DataPipeline) Close() {
	if dp.closed {
		return
	}
	dp.closed = true
	if dp.cancel == nil {
		return
	}

	dp.cancel()

	//discard any remaining frames
	for v := range dp.views {
		dp.gate.consume(v.col) // drain gate to flush any frames waiting on align
	}

	dp.closing.Wait()
	<-dp.errsDone
}
