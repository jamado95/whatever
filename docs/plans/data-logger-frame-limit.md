# data_logger frame limit — engine-bounded runs, provider `limit` removed

## Context

`DataLoggerOptions.Limit` was only forwarded to each provider's `Init(sub, limit)` as a
per-subscription candle cap. With multi-subscription, the engine's output is the `SyncEventV2` frame
stream from `align.NewOrderedSynchronizer(nil).Sync(streams)`, and that is the unit to bound. Goal:
count frames drained from the synchronizer, close every provider at `Limit`, and let the pipeline
unwind so `Run` returns `nil` and `cmd/main.go` exits cleanly.

Two steps:
1. Engine-level frame count in `data_logger`, with providers still capped by `Limit`.
2. A manual run showed step 1's cap never fires on the default 5m+1h config: providers ran dry at
   `limit − 11` frames, because a frame needs every lane. Decision: engine-level limits replace
   provider limits. `DataProvider.Init` drops `limit`, and `signal_logger`/`full` drop their `limit`
   option, whose only use was forwarding it.

## Shutdown chain (existing)

`provider.Close()` → `close(done)` → provider `run()` returns → `close(data)`/`close(errs)` →
`Gate` (realtime passes through; `fixedInterval` ranges `in`) → `FeatureChain.Process` closes →
`pipe.OrderedFanIn` closes once drained → `runV2` closes `syncOut` → drain goroutine exits → `wg`
done.

Constraints:
- The drain goroutine keeps reading `syncOut` past the limit, or upstream blocks and `wg.Wait()`
  never returns.
- The drain goroutine must not call `e.Close()`, because `Close` waits on `wg` (deadlock).
- With a `fixed` ticker, around 100 buffered candles per lane would trickle out per tick after
  close. The gates therefore get a cancellable context.

## Changes

**`internal/engine/data_logger.go`**
- `limitHit chan struct{}` field, closed by the drain goroutine when `count == Limit`. Frames after
  that are discarded (not logged or exported) so upstream can unwind.
- `gateCtx, cancelGates := context.WithCancel(ctx)` is passed to every `ticker.Gate`. `Run`
  stores the cancel on the struct (mutex-guarded), and `Close()` calls it before closing providers,
  so any shutdown path, including an external `Close()`, stops the gates at once.
- The `Run` select adds `<-e.limitHit`; on any exit it calls `e.Close()`.
- The `wait` goroutine closes its channel instead of sending, so it can't leak when `Run` exits
  through another case.
- `prov.Init(sub)`.

**`internal/protocol/provider.go`**: `DataProvider.Init(sub Subscription) error`.

**`internal/domains/provider/`**: `binance_csv.go` and `binance_historical.go` drop the `limit`
field and the limit checks. They stream until EOF / end of data / `done`.

**`internal/engine/signal_logger.go`, `full.go`** (build-tagged): remove `Limit` from the options
struct and from `Validate`, and call `Init(sub)`. Both tagged builds still fail only on the existing
`Strategy.Init` type mismatch.

## Tests: `internal/engine/data_logger_test.go`

The tests build `&DataLogger{...}` directly, with a fake provider (5m candles, Binance-style ms
`CloseTs`, infinite or finite) and a counting exporter.

| Case | Assert |
|---|---|
| 1 sub, `Limit=N` | exports == N, provider closed |
| 2 subs (5m), `Limit=N` | exports == 2N, both providers closed |
| `Limit=0`, finite provider | runs to the natural end, exports == M |
| `fixed` 20ms ticker, `Limit=3` | returns in < 1s, which covers gate cancellation (verified to fail at ~2s without it) |

## Docs

- `docs/discussions/data-layer.md`: contract is `Init(sub)`; decision "providers are unbounded
  streams".
- `docs/discussions/engine.md`: `data_logger` row, a "Run length" entry under Solution, and a WIP
  line.
- `docs/discussions/multi-subscription.md`: `Init(sub)` reference, plans list, and WIP.

## Verification

- `make fmt`, `make test`, `make lint`, `go test -race ./...`.
- Manual run on a scratch copy of `config.json` (5ms ticker, exporter in a scratch dir), for limit
  20 and limit 300. Expect "limit reached count=N", then "data engine close" and "whtv shutdown
  complete", with exit 0.
