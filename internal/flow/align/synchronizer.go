// Package align provides pipeline combinators that operate across
// per-subscription feature chains — the synchronizer that aligns
// independent streams into SyncEvents.
package align

import (
	"whatever/internal/flow/pipe"
	proto "whatever/internal/protocol"
)

// Synchronize fan-ins N feature-chain outputs, each keyed by Subscription, and
// emits SyncEvents grouped by candle CloseTs using flush-on-advance.
//
// Grouping: equal CloseTs accumulates into the current group; a later CloseTs
// flushes and emits. The final group flushes when all input channels close.
// NowFwdFill is a forward-filled snapshot of the latest EMD per subscription
// at each emission; Now is always a subset of NowFwdFill.
//
// This is the increment-1 (arrival-order) synchronizer: out-of-order arrivals
// across lanes emit as late singletons. Backtest determinism is iteration 2.
func Synchronize(streams map[proto.Subscription]<-chan proto.ExtendedMarketData) <-chan proto.SyncEvent {
	chans := make([]<-chan proto.ExtendedMarketData, 0, len(streams))
	for _, ch := range streams {
		chans = append(chans, ch)
	}
	return synchronize(pipe.FanIn(chans...))
}

// synchronize is the core flush-on-advance logic, accepting a pre-merged
// channel. Exported for testing with controlled arrival order.
func synchronize(merged <-chan proto.ExtendedMarketData) <-chan proto.SyncEvent {
	out := make(chan proto.SyncEvent)
	go func() {
		defer close(out)

		var (
			group       []proto.ExtendedMarketData
			currentTime int64 = -1
			latest            = make(map[proto.Subscription]proto.ExtendedMarketData)
		)

		for emd := range merged {
			ts := emd.Timestamp()
			sub := emd.Subscription

			switch {
			case currentTime == -1:
				currentTime = ts
				group = []proto.ExtendedMarketData{emd}
			case ts == currentTime:
				group = append(group, emd)
			case ts > currentTime:
				out <- makeEvent(currentTime, group, latest)
				currentTime = ts
				group = []proto.ExtendedMarketData{emd}
			default:
				// ts < currentTime: out-of-order arrival, emit as singleton.
				out <- makeEvent(currentTime, []proto.ExtendedMarketData{emd}, latest)
				continue // dont allow older candle to regress the fwd fill
			}

			latest[sub] = emd
		}

		if len(group) > 0 {
			out <- makeEvent(currentTime, group, latest)
		}
	}()
	return out
}

func makeEvent(
	t int64,
	now []proto.ExtendedMarketData,
	latest map[proto.Subscription]proto.ExtendedMarketData,
) proto.SyncEvent {
	fwdFill := make(map[proto.Subscription]proto.ExtendedMarketData, len(latest))
	for k, v := range latest {
		fwdFill[k] = v
	}
	return proto.SyncEvent{Time: t, Now: now, NowFwdFill: fwdFill}
}
