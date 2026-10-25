package timing

import (
	"context"
	"time"
)

/*
Pacer paces frame consumption in replay: the engine waits on it after taking each frame, before
processing it. It changes when frames are consumed, never their content.
*/
type Pacer interface {
	// Wait blocks until the frame at frameTime (Unix ms) may be processed, or ctx is done.
	Wait(ctx context.Context, frameTime int64) error
}

type unpaced struct{}

// Unpaced lets every frame through at once.
func Unpaced() Pacer { return unpaced{} }

func (unpaced) Wait(ctx context.Context, _ int64) error { return ctx.Err() }

// fixed releases one frame per tick of a ticker started on the first Wait.
type fixed struct {
	interval time.Duration
	ticker   *time.Ticker
}

/*
Fixed lets one frame through per interval, whatever its time; the first after one interval. A tick
missed while a frame is processed is kept, one at most. The ticker is never stopped: an
unreferenced ticker is collected.
*/
func Fixed(interval time.Duration) Pacer { return &fixed{interval: interval} }

func (f *fixed) Wait(ctx context.Context, _ int64) error {
	if f.ticker == nil {
		f.ticker = time.NewTicker(f.interval)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.ticker.C:
		return nil
	}
}
