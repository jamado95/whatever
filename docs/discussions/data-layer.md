# Data Layer

## Topic

The data layer ingests market data from heterogeneous sources (static files, REST APIs, live streams) and normalises it into a single internal representation consumed by the rest of the
pipeline. It is the boundary between third-party data formats and the engine's domain types.

Objective of this discussion: establish what the data layer currently guarantees, where the
current contract will break as we add live streams, multiple assets and non-candle data, and what should change before the backtesting engine is built on top of it.

## Current Understanding

### Contract

`proto.DataProvider` (`internal/protocol/provider.go:70`) is a four-phase lifecycle:

```
Init(sub Subscription, limit int) -> Streams() (<-chan MarketData, <-chan error) -> Start() -> Close()
```

`Init` allocates channels and validates configuration; `Streams` hands out the read ends; `Start`
spawns the producing goroutine; `Close` signals a `done` channel. Channels are created in `Init`,
so `Streams` must be called after `Init` — this ordering is implicit, not enforced by the type.

Normalised types (`internal/protocol/provider.go`):
- `Candle{OpenTs, CloseTs, Open, High, Low, Close, Volume}` — OHLCV only, ms epoch timestamps.
- `MarketData{Symbol, ProviderID, Timeframe, Candle, ReceivedAt *int64}`.
- `MarketData.Timestamp()` returns `Candle.CloseTs`, which makes it satisfy `proto.TimeStamped` and
  therefore storable in `SortedWindow`.
- `Subscription{Symbol, Timeframe}` — exactly one symbol and one timeframe per provider instance.
- `Timeframe` is a closed enum (`1m`, `5m`, `15m`, `1h`, `1d`) with `IsValid()`.

### Implementations

| Provider | File | State |
|---|---|---|
| `binance_csv` | `internal/domains/provider/binance_csv.go` | Working. Reads Binance monthly kline CSVs, 12-column layout, no header row expected. |
| `binance_historical` | `internal/domains/provider/binance_historical.go` | Compiles and runs, but see "start/end window" below. |
| live / websocket | — | Does not exist. |

Both providers push into a 100-buffered `data` channel and a 10-buffered `errs` channel, honour
`limit`, and select on `done` to abort. `binance_historical` paginates by 1000-candle pages,
advancing `startTime` to `lastCandle.CloseTs + 1`.

Auxiliary tooling exists outside the engine: `cmd/scripts/binance_download_klines` fetches the CSVs
that `binance_csv` reads, and `cmd/scripts/binance_market_info/` queries symbols/tickers.

### Known gaps and defects

1. **`binance_historical` time window is never applied.** ~~Resolved~~ — see [[config]]. The
   factory asserted `opts["startTime"].(int64)` against a map that only ever holds `float64`, so
   the window was always `0`. It now decodes into a typed struct; regression test in
   `binance_historical_test.go`. The provider still has never run against a bounded range in
   practice — worth exercising once.
2. **`ReceivedAt` is declared but never populated** by either provider. Anything that wants to
   reason about ingestion latency, or to distinguish event time from arrival time, has no data.
   `Timestamp()` deliberately uses `CloseTs` (event time) instead — see Decisions.
3. **No retry, backoff or rate limiting** in the HTTP provider. A single non-200 response pushes
   one error and terminates the stream permanently (`binance_historical.go:157`).
4. **One subscription per provider instance.** Multi-symbol operation requires N provider
   instances, and the engine layer only wires one (see [[engine]]).
5. **Close-before-Start** logs a warning and leaves `done` open; the lifecycle is not idempotent in
   both directions.
6. **CSV parsing assumes headerless files.** Binance has added header rows to some newer archive
   files; those would emit a parse error per file rather than being skipped. Currently benign
   because the local `data/` archives are headerless.

## Decisions

- **Event time (`Candle.CloseTs`) is the canonical timestamp**, not arrival time. It is the only
  choice that makes historical replay and live operation behave identically, and `SortedWindow`
  ordering depends on it.
- **Candle-close is the unit of ingestion.** Providers emit closed candles only; there are no
  partial-bar updates. This removes look-ahead ambiguity for free and matches the causality
  requirement in `docs/Backtesting.md`.
- **Normalisation happens inside the provider**, not in the engine. Each provider owns its vendor's format, so adding a venue never touches downstream code.
- **Providers are registry-constructed from `map[string]any` options.** Uniform with every other component. Each provider now declares a config struct decoded strictly by `config.Decode`, so the map is an interchange format rather than the schema — see [[config]]; [[engine]] covers the DI side.

## Solution

Current design — provider owns a goroutine, pushes onto channels, is gated downstream by the
engine's `timing.Ticker`. This keeps backpressure control out of the provider and in the engine,
which is what lets the same provider serve realtime and fixed-interval replay.

Options considered for the multi-symbol problem:

- **N provider instances, one per subscription** (implicit today). Simple, no interface change,
  but instance count grows with symbols × timeframes and each carries its own HTTP client and
  goroutine. Fan-in has to happen in the engine.
- **`Init([]Subscription)` — one provider, many streams.** Fewer connections (important for
  websockets, where venues multiplex many symbols on one socket), but the provider must then
  either interleave into one channel (requiring downstream demux) or return a channel per
  subscription (changing `Streams()`).
- **Provider returns a keyed map of channels.** Most explicit, largest interface change.

Not yet decided. The websocket provider is the forcing function — venue-side multiplexing makes
option 2 or 3 substantially more natural, so this should be settled before that work starts.

## Open Questions

- Multi-symbol: which of the three options above? Does `Subscription` need to carry a venue/market identifier once we have more than Binance?
- Non-candle data: tick, trade and order-book snapshots do not fit `Candle`. Does `MarketData`
  become an interface / sum type, or do we add parallel `TickData`/`BookData` streams with their
  own provider interface? The `TimeStamped` constraint on `SortedWindow` suggests the latter is
  cheap to add.
- Options and futures (per `Protocol.md`) need instrument metadata — expiry, strike, contract size, funding. Where does an instrument/reference-data concept live? It is not currently modelled anywhere.
- Gap and integrity handling: missing candles, exchange halts, duplicate timestamps. `SortedWindow` will happily accept out-of-order and duplicate entries. Should the provider guarantee a contiguous, deduplicated, monotonic sequence, or should the engine validate?
- Does the data layer own historical warm-up (fetch N candles before live start so features have
  lookback), or is that the engine's job?
- FX and equities need session/calendar awareness (market hours, holidays) that crypto does not. Does that belong here or in a separate calendar service?

## WIP

- Working end to end: `binance_csv` → feature chain → JSONL export via the `data_logger` engine.
  This is the only exercised path in the codebase.
- `binance_historical` is wired and registered but disabled in `config.json`, and its time-window bug means it has probably never run against a bounded range.
- No live provider, no non-crypto provider, no tests in `internal/domains/provider`.

## Resources

- `internal/protocol/provider.go`, `internal/domains/provider/`
- `docs/Protocol.md` § Data layer
- `cmd/scripts/binance_download_klines/`, `cmd/scripts/README.md`
- Related: [[features]] (consumer of `MarketData`), [[engine]] (lifecycle and timing gate)
