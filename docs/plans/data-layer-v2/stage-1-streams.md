# Stage 1 — subscription model and stream table (data layer v2, iteration 1)

## Context

Stage 1 of [main.md](main.md). A direct subscription gains the rolling windows derived from it;
config parsing validates them; the new module `internal/engine/data` gains the stream table it will
build from the subscriptions when it is instantiated. No runtime code builds the table yet — the data
layer's construction (stage 5) and `data_logger_v2` (stage 6) do. `data_logger` is unchanged apart
from rejecting derivations.

## Decisions

- **`proto.Subscription` carries its derivations** — a protocol change authorised by this plan.
  ```json
  { "symbol": "BTCUSDT", "timeframe": "5m", "provider": "btcusdt_5m", "derive": ["1h", "1d"] }
  ```
- **`Derive` is a `TimeframeDerivationSet` — a `uint8` bitmap, one bit per timeframe**, not a slice.
  One byte holds every derivation, so the `Subscription` copied into every `MarketData` stays small
  (48 → 56 bytes with alignment, versus 72 for a slice header), and `Subscription` stays comparable:
  it is a map key in `cmd/wire.go`, `data_logger`, `SynchronizerV2.Sync` and their tests. The
  timeframe enum is closed — five values, five of the eight bits.
- **`ID()` is unchanged**; derivations are not part of a subscription's identity.
- **Derived identity lives in the stream table.** `NewStreams` names each derived stream
  `symbol:tf:derived_<source ID>`, e.g. `BTCUSDT:1h:derived_BTCUSDT:5m:btcusdt_5m`. Nothing outside
  the data layer builds derived IDs. Provider names starting with `derived_` are rejected, so derived
  and direct IDs never collide.
- **Validation belongs to config parsing**, in `Config.validate`:
  - stream-ID deduplication: two direct subscriptions with the same ID are rejected (the existing
    check). Derived IDs cannot repeat — a window appears once per `derive` list and the `derived_`
    prefix keeps them apart from direct IDs. A symbol may have several direct timeframes on one
    provider, which reverses the "one direct subscription per (symbol, provider)" decision;
  - each derived window is coarser than its subscription's timeframe and a multiple of it.
- **No grid rules to validate.** The grid — the finest direct timeframe — is computed when the stream
  table is built. That every timeframe is a multiple of the grid follows from the closed enum
  (`1m 5m 15m 1h 1d`).
- **The stream table is an engine concern.** `data.NewStreams` turns validated subscriptions into one
  `Stream` per direct subscription and one per derived window, plus the grid. It validates nothing
  and is called by the data layer when it is instantiated, not by `wire`.
- **Stream order:** direct streams first, then derived, each group sorted by ID — deterministic and
  independent of config order. The index is a stream's identity inside the data layer.
- **Strategy references stay direct-only.** No strategy runs in the default build; how strategies
  reference derived streams is decided with the strategy layer.

## Changes

### Protocol — new `internal/protocol/subscription.go`

Moved unchanged from `provider.go`: `Timeframe` with its constants, `IsValid`, `ValidTimeframes`,
`UnmarshalJSON`, `TimeframeS`, `TimeframeMs`; `Subscription` with `ID()`. `provider.go` keeps `Side`,
`Candle`, `MarketData`, the `SyncEventV2` types and `DataProvider`. Added:

```go
// TimeframeDerivationSet is the bitmap of rolling windows derived from a subscription: bit i set
// means ValidTimeframes()[i] is derived. Decoded from a JSON list; a repeated timeframe is an error.
type TimeframeDerivationSet uint8

func NewTimeframeDerivationSet(tfs ...Timeframe) TimeframeDerivationSet
func (s TimeframeDerivationSet) Has(tf Timeframe) bool
func (s TimeframeDerivationSet) List() []Timeframe // ascending duration
func (s *TimeframeDerivationSet) UnmarshalJSON(b []byte) error

type Subscription struct {
	Symbol    string
	Timeframe Timeframe
	Provider  string
	Derive    TimeframeDerivationSet // rolling windows derived from this direct subscription
}

// DerivedProviderPrefix marks a derived subscription's Provider; provider names may not use it.
const DerivedProviderPrefix = "derived_"

```

- Bit `i` is `ValidTimeframes()[i]`, so `List()` is in ascending duration. Unknown timeframes fail
  through `Timeframe.UnmarshalJSON`.
- Existing `Subscription` literals use named fields and compile unchanged.

### Config — `internal/config/config.go`

`validate()`:

- The duplicate-ID check is unchanged.
- For each window in `Derive`: `subscriptions[i] (<ID>): derive "5m" must be coarser than "5m"`, and
  `… must be a multiple of …`. The multiple check cannot fail with today's timeframes; it is explicit
  so new timeframes are covered.
- A provider name starting with `proto.DerivedProviderPrefix` is rejected.

### Stream table — new `internal/engine/data/streams.go`

```go
// Kind is a stream's origin.
type Kind uint8

const (
	Direct  Kind = iota // fed by a provider
	Derived             // rolling window over a direct stream
)

// Stream is one entry of the stream table; its index in Streams.All is its identity in the data layer.
type Stream struct {
	Sub    proto.Subscription // derived: symbol, window timeframe, Provider "derived_" + source ID
	Kind   Kind
	Step   int64 // columns per timeframe: the history-read step, and a direct stream's cadence
	Source int   // derived: index of its source stream; direct: -1
	Window int64 // derived: source bars per window; direct: 0
}

// Streams is the stream table: direct streams first, then derived, each group sorted by ID.
type Streams struct {
	Stride  int64 // ms per column: the finest direct timeframe
	All     []Stream
	Directs int // All[:Directs] are direct
	index   map[string]int
}

// NewStreams builds the stream table from validated direct subscriptions.
func NewStreams(subs []proto.Subscription) *Streams

// Index returns the index of the stream with the given ID.
func (s *Streams) Index(id string) (int, bool)
```

`NewStreams` walks each subscription's `Derive.List()` and builds the derived streams directly. A
derived stream's `Sub` is filled in that loop, so `Sub.ID()` is its stream ID; stages 5–6 stamp
it on derived candles, since the exporter and logger read `MarketData.Subscription`. The package
imports `proto` only.

### Engine — `internal/engine/data_logger.go`

- Construction fails with `data_logger: derived subscriptions are not supported` when any
  subscription has a non-empty `Derive`.

### Repository — `.gitignore`

- The bare `data` rule ignored every directory named `data`, including this module; it is anchored
  to the repo root (`/data`). Found during implementation.

### Tests

- `internal/protocol`: `TestTimeframeMs` and `TestSubscriptionID` move from `provider_test.go` to a
  new `subscription_test.go`, which also covers: `TimeframeDerivationSet` decodes a list into the
  expected bits, rejects an unknown or repeated timeframe, is zero when absent; `List` is in ascending duration; `ID()`
  ignores `Derive`; a `Subscription` with `Derive` set works as a map key.
- `internal/config`: `derive` decodes; a window equal to or finer than its subscription fails; one
  symbol at two timeframes on one provider is accepted; a `derived_` provider name fails.
- `internal/engine/data/streams_test.go`: grid is the finest direct timeframe (5m among 5m, 1h, 1d);
  `Step` and `Window` — direct 1h on a 5m grid → `Step` 12; 1d derived from 1h on a 5m grid → `Step`
  288, `Window` 24, `Source` = the 1h stream; derived IDs; `Index`; order independent of config order.
- `cmd`: `data_logger` with a derivation fails construction.

## Documentation updates

- [data-layer-v2.md](../../discussions/data-layer-v2.md): Decisions § Subscriptions and the grid —
  `derive` on `proto.Subscription` as a `TimeframeDerivationSet` bitmap, derived identity and the
  reserved prefix, validation in config parsing, the stream table built by the data layer with its
  order; replace "one direct subscription per (symbol, provider)" with stream-ID deduplication; drop
  the "Derivation config" open question; WIP references this plan.
- `docs/Architecture.md`: add `internal/engine/data` — the data layer v2 module, built per
  `docs/plans/data-layer-v2`.
- `docs/Commands.md`: add `internal/engine/data` to the `make test` package list.
- [README.md](../../discussions/README.md): data-layer-v2 row → plan approved, stage 1 implemented.

## Out of scope

- Building the table at run time — stages 5 and 6.
- Due-column and cadence helpers on the table — added by the stage that first needs them.
- Providers, the exporter and `config.json`.
- Subscriptions sharing one provider block share one provider instance and fail at run time (the
  second `Init` overwrites the first, the second `Start` fails); unchanged and unvalidated here.

## Verification

- `make check` clean.
- `make validate` passes on the current `config.json`.
- `--validate-only` on a config with `derive` fails only with the `data_logger` rejection; on a
  derivation not coarser than its source, it fails naming the subscription.
