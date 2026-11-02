# Stage 2 — stream table and universes (features layer v2, iteration 1)

## Context

[main.md](main.md) stage 2. Features bind to universes ([features.md](../../discussions/features.md)
§ Universes): named stream and pair sets built from self-contained rules, resolved by config as a
pure function of rules over the stream table. Nothing declares or resolves them yet.

The stream table — one entry per direct subscription and per derived window, with the grid's
stride, periods and windows — lives in `internal/engine/data` and is built only by `data.New`, so
config cannot import it and the feature layer would rebuild it. This stage first moves it into
`internal/protocol`, built once by the composition root and injected into the data layer, so config,
the data layer and the feature layer share one index space.

It then adds the `universes` block, `universe` on feature entries and the resolver over the stream
table. `Load` checks their shape; the composition root, which builds the table, resolves universes
over it. `Config.validate()` collects every error instead of returning
the first. Nothing consumes universes yet: `data_logger` and `data_logger_v2` ignore them.

```
Load          shape: universes, rules, unknown universes
instantiate   streams := proto.NewStreams(cfg.Subscriptions)        built once
              cfg.ResolveUniverses(streams)                         errors join build's
wire          _streams ─▶ data_logger_v2 ─▶ data.New                the full table
stage 5       Expand(…, resolved universes, streams) ─▶ instances  the engine never sees a universe
```

The stage is one plan in four steps, implemented in order. After each step the changes stay
unstaged for review; the step is committed once approved, then the next starts.

1. The stream table in `internal/protocol`, injected into the data layer.
2. Universes and the resolver.
3. Config wiring and collected errors.
4. The config guide, `configs/README.md`.

## Decisions

- **The stream table is its own protocol domain.** `internal/protocol/stream.go` takes `Stream`,
  `Streams` and `NewStreams` from `internal/engine/data` with behaviour unchanged: one stream per
  direct subscription and per derived window, direct first, each group sorted by ID; the stride the
  finest direct timeframe; derived IDs assigned only in `NewStreams`. It validates nothing: config
  validates subscriptions before any table is built.
- **Built once at the composition root and injected.** `instantiate` builds the table from the
  subscriptions `Load` validated and keeps it in `components`; `wire` injects it as `_streams`,
  beside the other instances; `data_logger_v2` hands it to `data.New`, which no longer builds its
  own; stage 5 hands the same table to `Expand`, and the feature engine indexes its rings by it. The
  engine always receives the full table, never one filtered by a universe. Reverses "built by the
  data layer, never by the composition root"; derived IDs keep one source, `NewStreams`. Recorded in
  [data-layer-v2.md](../../discussions/data-layer-v2.md) § Reversed on 2026-11-01.
- **`data.New` checks the table against its providers:** every direct stream has a provider, every
  provider's subscription is a direct stream of the table, and one instance serves one stream.
- **`StreamKind` is a string enum**, `direct` and `derived`, with a validating `UnmarshalJSON` — the
  config rule for enums — so a rule's `kind` filter decodes into it. It replaces data's iota `Kind`,
  which only `NewStreams` and tests read.
- **`Column` and `Provider` on `Streams`.** `Column(ts)` is data's `column`, exported: the grid
  mapping sits with the stride, and normalisation calls it. `Provider(i)` is the provider instance
  feeding stream `i` — its own, or its source's — which the `provider` filter matches.
- **`universes` is a list of `{name, rules}`**, not a map keyed by name: `encoding/json` keeps the
  last of two duplicate keys silently. Names are required and unique; a universe has at least one
  rule. An unused universe is valid.
- **`universe` sits on the feature entry** and applies to every variant. A different universe per
  variant is a second entry. A disabled entry's universe is not checked.
- **A rule has filters and at most one pairing.** Filters: `symbol` and `provider` (globs,
  `path.Match`), `timeframe` (`proto.Timeframe`) and `kind` (`direct`, `derived`). A list is OR,
  fields are AND, an absent field does not constrain. Pairings: `references`, or `pairs` — `"*"` or
  a list of explicit pairs. `{}` selects every stream.
- **`provider` matches provider instance names** — the subscription's `provider` field; a derived
  stream matches its source's.
- **A named stream is its stream ID**, looked up with `Streams.Index`, so it names at most one
  stream: a direct stream by its subscription ID (`BTCUSDT:1h:btc_1h`), a derived stream by
  `symbol:timeframe:derived_<source ID>` (`BTCUSDT:1h:derived_BTCUSDT:5m:btc_5m`). Reverses
  [features.md](../../discussions/features.md)'s source-provider names ("no derived ID is ever
  needed"), which let one name match a direct and a derived stream once subscriptions share a
  provider.
- **Names are taken as named:** every filter, `kind` included, chooses partners only.
- **An explicit-pairs rule takes no filter**: it chooses no partners, so a filter would do nothing.
- **Malformed is an error, not "matches nothing":** an empty list (`"symbol": []`, `"pairs": []`),
  an empty value (`"symbol": [""]`), an invalid glob, or an explicit pair without exactly two
  distinct names.
- **A name that matches no stream is an error**, as a strategy's subscription reference is. Lookup
  is the only check on a name: the table's IDs are valid by construction, so a malformed name is
  one that matches no stream, and config never parses the ID format. A filter
  that selects nothing is valid; a universe an enabled feature names that resolves to no streams is
  an error.
- **Pairs are unordered**, stored as `(lower index, higher index)`; asymmetric order stays open
  ([main.md](main.md) § Out of scope). Only a stream paired with itself is excluded: pairs across
  timeframes, or within one symbol, are valid.
- **`Load` checks shape; the composition root resolves.** Everything that needs no table — names,
  rules, unknown universes — is in `validate()`. Resolution
  needs the table, so `instantiate` resolves every universe over the one table it built, through
  config's `ResolveUniverses`; its errors are path-named and join `build`'s. A config with both a
  shape error and an unmatched name reports the second on the next run, as `Load` and `build` errors
  split today. `Load` stops on subscription errors, so resolution never runs over a broken table.
- **Universes stay at the composition root.** `Resolve` returns indices into `Streams.All` — the
  data layer's, and from stage 5 the feature layer's. Stage 2 resolves for errors only and drops the
  results; stage 5 keeps them for `Expand`, whose instances carry their coordinates. The engine never
  receives a universe.
- **No derivation rule.** Stream IDs are unique by construction, so one window derived from two
  sources of a symbol on one provider is valid: the two streams have different IDs.
- **`validate()` collects every error**; `Load` returns them as one `errors.Join`, and `main` logs
  each, as it does `build`'s. A decode error still ends `Load` alone.
- **Errors from enum and union decoding carry no path** (`kind`, `pairs`), as an invalid timeframe's
  does today; every other universe error is path-named.

## Step 1 — the stream table in `internal/protocol`

### Changes

**New `internal/protocol/stream.go`** — `internal/engine/data/streams.go`'s types and bodies, with:

```go
// StreamKind is a stream's origin; a string, so a config filter decodes it.
type StreamKind string

const (
	StreamDirect  StreamKind = "direct"  // fed by a provider
	StreamDerived StreamKind = "derived" // rolling window over a direct stream
)

func (k *StreamKind) UnmarshalJSON(b []byte) error // rejects any other value

// Stream is one entry of the stream table; its index in Streams.All is its identity.
type Stream struct {
	Sub    Subscription // derived: symbol, window timeframe, Provider "derived_" + source ID
	Kind   StreamKind
	Period int64 // columns between updates: direct, timeframe / Stride; derived, its source's Period
	Window int64 // updates per bar: direct 1; derived, source bars per window
	Source int   // derived: index of its source stream; direct: -1
}

// Streams is the stream table: direct streams first, then derived, each group sorted by ID.
type Streams struct {
	Stride  int64 // ms per column: the finest direct timeframe
	All     []Stream
	Directs int // All[:Directs] are direct
	index   map[string]int
}

func NewStreams(subs []Subscription) *Streams
func (s *Streams) Column(ts int64) int64       // data's column: the boundary at or after a close
func (s *Streams) Index(id string) (int, bool)
func (s *Streams) Provider(i int) string       // the instance feeding stream i: its own, or its source's
```

**`internal/engine/data`**

- `streams.go` is deleted; its package comment moves to `data.go`.
- `Stream`, `Streams`, `Kind`, `Direct`, `Derived` → `proto.Stream`, `proto.Streams`,
  `proto.StreamKind`, `proto.StreamDirect`, `proto.StreamDerived`; normalisation calls `Column`.
- `New(streams *proto.Streams, providers map[proto.Subscription]proto.DataProvider, lookback int,
  log)`, and `newDataPipeline` likewise: the table is used as given. Beside the nil and
  shared-instance checks, a provider whose subscription is not a direct stream of the table is an
  error — `data: subscription "…" is not a direct stream of the table`.
- `Streams()` returns `*proto.Streams`.

**`internal/engine/data_logger_v2.go`** — the factory takes `_streams` (`config.Dep[*proto.Streams]`);
`Run` hands it to `data.New`.

**`cmd`** — `instantiate` builds `proto.NewStreams(cfg.Subscriptions)` into `components.streams`;
`wire` injects it as `_streams`, beside `_subscriptions`; `data_logger` ignores it.

### Tests

- `internal/engine/data/streams_test.go` moves to `internal/protocol/stream_test.go` — grid,
  entries, order and column, unchanged — with **`TestStreamKindUnmarshal`** (`direct`, `derived`,
  anything else) and **`TestStreamsProvider`** (a direct stream's own instance, a derived stream's
  source's).
- `internal/engine/data` tests, mechanical: `NewStreams` → `proto.NewStreams`; kind constants →
  `proto.Stream*`; `newDataPipeline(st, providers, …)`; `.column(` → `.Column(`; `testSubs` and
  `mustIndex` move to `data_test.go`. New **`TestNewRejectsUnknownSubscription`** — a provider keyed
  by a subscription outside the table.
- `internal/engine/data_logger_v2_test.go` — the factory's options and `newTestDataLoggerV2` carry
  the table; the factory fails without `_streams`.
- `cmd` — `TestBuildDataLoggerV2AcceptsDerivations` covers the injection, unchanged.

### Documentation

- `docs/Architecture.md` — `internal/protocol`: domain interfaces and the stream table; the data
  module runs on the stream table the composition root injects.
- [data-layer-v2.md](../../discussions/data-layer-v2.md) — the decision and its reversal were
  recorded at planning (§ Subscriptions and the grid, § Reversed on 2026-11-01). At implementation:
  § Module and stages names `proto.Streams.Column` for ceilDiv placement; the WIP entry moves from
  planned to implemented.
- [features.md](../../discussions/features.md) — § Feature layer recorded the shared table at
  planning; at implementation, WIP notes the stream table implemented.

### Acceptance

- Every existing test passes with only the mechanical edits above; `go test -race
  ./internal/protocol/ ./internal/engine/... ./cmd/` clean; `make check` clean.
- `BenchmarkReadiness` and `BenchmarkAggregate` within noise of their results before the step.
- `data_logger_v2` exports the same records as before the step, from the same config.

## Step 2 — universes and the resolver

### Changes — new `internal/config/universe.go`

```go
// Universe is a named set of streams and pairs, the union of its rules' selections.
type Universe struct {
	Name  string `json:"name"`
	Rules []Rule `json:"rules"`
}

// Rule selects streams by filters, and pairs them by at most one of References or Pairs.
type Rule struct {
	Symbol     []string          `json:"symbol"`    // globs
	Timeframe  []proto.Timeframe `json:"timeframe"`
	Provider   []string          `json:"provider"`  // globs over provider instance names
	Kind       []proto.StreamKind `json:"kind"`
	References []string          `json:"references"` // stream IDs
	Pairs      *Pairs            `json:"pairs"`
}

// Pairs is "*", every pair of the rule's streams, or a list of explicit pairs.
type Pairs struct {
	All  bool
	List [][]string // two names each; checked, since a [2]string would drop extras silently
}

func (p *Pairs) UnmarshalJSON(b []byte) error // "*" or [[a, b], …]; any other string is an error

// Resolved is a universe's streams and pairs as indices into the stream table's All.
type Resolved struct {
	Streams []int    // ascending
	Pairs   [][2]int // a < b, ascending
}

/*
Resolve returns u's streams and pairs over st. Each rule's streams S are the streams passing its
filters; a references rule pairs each reference with S, explicit pairs are taken as written, "*"
pairs S. Streams are S plus every named stream; duplicates collapse and self-pairs are dropped. A
name that is no stream's ID is an error, path-named from the rule.
*/
func Resolve(u Universe, st *proto.Streams) (Resolved, []error)

// check returns u's shape errors, path-named from the rule: pairing, empty lists and values, globs.
func (u Universe) check() []error
```

- Matching reads each stream's `Sub.Symbol`, `Sub.Timeframe`, `Provider(i)` and `Kind`: each set
  filter must hold — any `symbol` glob, any `timeframe`, any `provider` glob, any `kind`. A name is
  looked up with `Index`.
- Pairs are gathered in a set keyed `[2]int{min, max}`, then sorted.
- `check` and `Resolve` errors are relative to the universe (`rules[0].references[1]: …`); their
  callers prefix `universes[i] (name)`. `Resolve` assumes a checked universe.

### Tests — new `internal/config/universe_test.go`

Tables from `proto.NewStreams` over subscriptions with derivations, on two providers; expected
streams are looked up by ID through `Index`.

- **`TestResolveFilters`** — `{}` selects every stream; `symbol` globs (`*USDT`); `timeframe`;
  `provider` exact and glob; `kind`; a list is OR, fields are AND.
- **`TestResolveDerived`** — a derived stream matches by its window timeframe and its source's
  provider.
- **`TestResolveNamed`** — direct and derived streams named by their IDs; no filter, `kind`
  included, drops a named stream; a name that is no stream's ID — a derived stream written with its
  source's provider, and a malformed name, among them — is an error naming its path.
- **`TestResolveReferences`** — each reference paired with every partner, itself excluded;
  references are among the streams; no partners, no pairs.
- **`TestResolveExplicitPairs`** — pairs as written, a derived end named by its ID.
- **`TestResolveAllPairs`** — every unordered pair of the rule's streams.
- **`TestResolveUnion`** — rules union; a pair from two rules, or written both ways, is one pair;
  output sorted.
- **`TestUniverseCheck`** — no rules; `references` with `pairs`; empty lists; empty values; a
  malformed glob; an explicit pair of one, three or two equal names; filters on explicit pairs.
  Errors are collected, path-named.
- **`TestPairsUnmarshal`** — `"*"`, a list, another string.

### Acceptance

- `go test -race ./internal/config/` passes; nothing outside `internal/config` changes.

## Step 3 — config wiring and collected errors

### Changes

**`internal/config/config.go`**

- `Config.Universes []Universe` (`json:"universes"`); `FeatureConfig.Universe string`
  (`json:"universe"`).
- `validate()` returns `[]error` and runs every check:
  1. Features: missing `type` (as today); an enabled entry naming an unknown universe —
     `features[3] (rsi): unknown universe "usdt"`.
  2. Subscriptions: today's checks, collected.
  3. Providers: today's checks, collected.
  4. Universes: empty or duplicate names, then `check`.
  5. Engine: missing `type` (as today).
- `Load` returns `errors.Join(errs...)` when `validate()` reports any.

**`internal/config/universe.go`**

```go
/*
ResolveUniverses resolves every universe over st, keyed by name, for a config Load accepted. Errors
are prefixed universes[i] (name); a universe an enabled feature names that resolves to no streams is
an error.
*/
func (c *Config) ResolveUniverses(st *proto.Streams) (map[string]Resolved, []error)
```

— `universes[1] (usdt): selects no streams`.

**`cmd/instantiate.go`** — after building the table, `cfg.ResolveUniverses(c.streams)`; its errors
join `instantiate`'s, and the results are dropped until stage 5.

**`cmd/main.go`** — a `Load` error is logged one line per joined error, followed by the count, as
`build`'s are; the build-error loop moves into a helper both use.

### Tests

**`internal/config/config_test.go`**

- **`TestLoadUniverses`** — a block with each pairing decodes; a feature's `universe` is kept.
- **`TestUniverseUnknownFieldRejected`** — a misspelt rule key (`refrences`) fails `Load`.
- **`TestUniverseErrors`** — duplicate and empty names; an unknown universe on an enabled feature,
  none on a disabled one; rule shape errors prefixed with their universe.
- **`TestResolveUniverses`** — results keyed by name; an unmatched name, path-named; a universe an
  enabled feature names selecting no streams, while an unused or disabled-only empty one passes.
- **`TestValidateCollectsErrors`** — independent errors in features, subscriptions, providers,
  universes' shape and the engine are all reported by one `Load`.

Existing tests keep passing unchanged: they match a substring of the error, which the joined error
still contains.

**`cmd/main_test.go`** — the reporting helper splits a joined error into its parts and logs one line
each; **`TestBuildReportsUniverseErrors`** — an unmatched name fails `build`, path-named, beside
another component's error.

### Documentation

- [config.md](../../discussions/config.md) — § Dry run: `Load` collects its validation errors too;
  universe resolution runs in `build`.
  WIP: universes move from planned to implemented; Solution references this stage and lists
  `universe.go`.
- [features.md](../../discussions/features.md) — § Universes was updated at planning (stream-ID
  names, instance-name filters with globs, filters choosing partners only). At implementation:
  Solution § Universes, the syntax settled and its example on instance names; WIP references this
  stage.
- [main.md](main.md) — stage 2 marked implemented.

### Acceptance

- `go test -race ./internal/config/ ./cmd/` passes; `make check` clean.
- `--validate-only` on `configs/data_logger.json` and `configs/data_logger_v2.json` reports valid.
- `--validate-only` on a config with several independent shape errors reports each on its own line
  with its path; with those fixed, an unmatched stream name is reported the same way.

## Step 4 — the config guide

### Changes — new `configs/README.md`

How to fill in a config file for the default build. Direct and short: a table per block, one line
per field, and universes shown by example over a fixed set of streams, with what each rule resolves
to.

1. **Start here** — a complete `data_logger_v2` config, small enough to read whole; `make validate`.
   Strictness in three lines: unknown keys and invalid values are errors; errors are reported with
   their path, file errors together first, then construction errors (unmatched stream names among
   them); disabled blocks are not checked.
2. **Blocks** — one row each: `providers`, `subscriptions`, `features`, `universes`, `strategies`,
   `engine`, required or optional.
3. **Providers** — `type`; `name` (defaults to `type`, unique, never `derived_…`); `disabled`.
   `binance_csv`: `file`, checked when the run starts, not by `--validate-only`.
   `binance_historical`: `startTime`, `endTime` (Unix ms, 0 = unbounded). One instance per
   subscription.
4. **Subscriptions** — `symbol`; `timeframe` (`1m`, `5m`, `15m`, `1h`, `1d`); `provider`, an enabled
   instance's name; `derive`, coarser multiples of `timeframe`, `data_logger_v2` only. Stream IDs: a
   direct stream's is its subscription's, `symbol:timeframe:provider`; a derived stream's is
   `symbol:timeframe:derived_<source ID>`.
5. **Features** — `type`, `disabled`, `universe`, `variants` (one instance per variant; none, one
   instance without options). Per type: parameters, defaults and IDs — `sma_20`, `ema_20`,
   `ema_20_s3`, `rsi_14`, `vwap_20`, `engulfing_candle`. A duplicate ID is an error.
6. **Universes** — below.
7. **Strategies** — `type`, `disabled`, `subscriptions` (stream names); none in the default build.
8. **Engine** — `type`: `data_logger` (no `derive`) or `data_logger_v2`; `strategies`; `exporter`
   (`jsonl`, `output_dir`, `disabled`); `options.limit` (frames, 0 = unlimited); `options.ticker`
   (`realtime` or absent: unpaced; `fixed` with an `interval` such as `"50ms"`).
9. **Common errors** — a table of mistake → error, this stage's included.

**Universes.** Validated but not yet run: no engine reads them until the feature engine lands.

- Filters: a table of `symbol` (globs), `timeframe`, `provider` (instance names, globs), `kind`
  (`direct`, `derived`). A list is OR, fields are AND, an absent field does not constrain.
- Rules, one line each with its consequence: filters only — the streams passing them; `references`
  — each reference paired with every stream passing the filters; `pairs` list — as written; `"*"` —
  every pair of the streams passing the filters. Names are stream IDs, taken as named; filters,
  `kind` included, choose partners only. Pairs are unordered; self-pairs are dropped; duplicates
  collapse; a universe is the union of its rules.
- Binding: `universe` on a feature entry; without one a single-stream feature runs on every stream;
  pairs serve pair features, none registered yet.

The examples run over five subscriptions, one provider instance each:

```json
{ "symbol": "BTCUSDT", "timeframe": "5m", "provider": "btc_5m", "derive": ["1h"] },
{ "symbol": "BTCUSDT", "timeframe": "1h", "provider": "btc_1h" },
{ "symbol": "ETHUSDT", "timeframe": "5m", "provider": "eth_5m", "derive": ["1h"] },
{ "symbol": "SOLUSDT", "timeframe": "1h", "provider": "sol_1h" },
{ "symbol": "ETHBTC",  "timeframe": "1h", "provider": "ethbtc_1h" }
```

| | Stream ID | Kind |
|---|---|---|
| A | `BTCUSDT:5m:btc_5m` | direct |
| B | `BTCUSDT:1h:derived_BTCUSDT:5m:btc_5m` | derived from A |
| C | `BTCUSDT:1h:btc_1h` | direct |
| D | `ETHUSDT:5m:eth_5m` | direct |
| E | `ETHUSDT:1h:derived_ETHUSDT:5m:eth_5m` | derived from D |
| F | `SOLUSDT:1h:sol_1h` | direct |
| G | `ETHBTC:1h:ethbtc_1h` | direct |

| Rule | Streams | Pairs | Shows |
|---|---|---|---|
| `{}` | A–G | — | no filter selects every stream |
| `{"symbol": ["*USDT"]}` | A–F | — | a glob |
| `{"timeframe": ["1h"]}` | B C E F G | — | derived streams match their window |
| `{"timeframe": ["1h"], "kind": ["direct"]}` | C F G | — | `kind` drops derived streams |
| `{"symbol": ["BTCUSDT", "ETHUSDT"], "timeframe": ["1h"]}` | B C E | — | OR within a field, AND across |
| `{"provider": ["*_5m"]}` | A B D E | — | derived streams match their source's provider |
| `{"references": ["BTCUSDT:1h:btc_1h"], "timeframe": ["1h"]}` | B C E F G | CB CE CF CG | a reference against every partner, never itself |
| `{"references": ["BTCUSDT:5m:btc_5m"], "symbol": ["*USDT"], "timeframe": ["1h"]}` | A B C E F | AB AC AE AF | the reference is taken as named: filters choose partners |
| `{"references": ["BTCUSDT:1h:btc_1h", "SOLUSDT:1h:sol_1h"], "symbol": ["ETH*"], "timeframe": ["1h"]}` | C E F G | CE CG FE FG | references are not paired with each other; `ETH*` matches `ETHBTC` |
| `{"references": ["BTCUSDT:1h:derived_BTCUSDT:5m:btc_5m"], "timeframe": ["1h"], "kind": ["direct"]}` | B C F G | BC BF BG | a derived reference by its ID; `kind` chooses partners only |
| `{"references": ["BTCUSDT:1h:btc_5m"]}` | error | | no stream has that ID: a derived stream is named by its own |
| `{"pairs": [["BTCUSDT:1h:btc_1h", "ETHUSDT:1h:derived_ETHUSDT:5m:eth_5m"], ["SOLUSDT:1h:sol_1h", "ETHBTC:1h:ethbtc_1h"]]}` | C E F G | CE FG | explicit pairs, as written |
| `{"pairs": [["BTCUSDT:1h:btc_1h", "SOLUSDT:1h:sol_1h"]], "kind": ["direct"]}` | error | | explicit pairs take no filter |
| `{"pairs": "*", "symbol": ["*USDT"], "timeframe": ["1h"]}` | B C E F | BC BE BF CE CF EF | every pair, same-symbol included |
| `{"pairs": "*", "symbol": ["*USDT"], "timeframe": ["1h"], "kind": ["direct"]}` | C F | CF | `kind` keeps one stream per symbol here |
| `{"pairs": "*", "symbol": ["BTCUSDT"]}` | A B C | AB AC BC | pairs across timeframes |

And one universe of three rules — `*` over direct `*USDT` 1h streams, the explicit pair
`["SOLUSDT:1h:sol_1h", "BTCUSDT:1h:btc_1h"]`, and `{"symbol": ["ETHBTC"]}` — resolves to streams
C F G and the single pair CF: rules union, a pair written either way is one pair, and a rule
without a pairing adds streams only.

The feature example binds `rsi` to a `*USDT` universe and leaves `ema` without one: `rsi_14` on
A–F, `ema_20` on A–G.

### Tests — `internal/config/universe_test.go`

- **`TestResolveExamples`** — the README's examples over the table `proto.NewStreams` builds from the
  same five subscriptions, the three-rule universe included, so the guide and the resolver cannot
  disagree. The README's letters label streams; the test finds them by ID.

### Documentation

- `docs/Commands.md` — the run section points to `configs/README.md` for the file format.
- [main.md](main.md) — `configs/README.md` joins the targeted updates.

### Acceptance

- `TestResolveExamples` passes, and every README example appears in it.
- The README's complete config, saved to a file, passes `make validate CONFIG=<file>`.

## Out of scope

- Consuming universes: "no universe means every stream", and pair features requiring a universe
  with pairs — stage 5.
- One provider instance serving several subscriptions — the multi-subscription per-sub
  materialisation; `data.New` keeps rejecting a shared instance.
- A duplicate variant across two entries of one type on different universes — stage 5's duplicate
  composed IDs.
- Asymmetric pair order — open in [features.md](../../discussions/features.md).
- Universes in the checked-in configs — stage 7.

## Verification

```bash
go test -race ./internal/protocol/ ./internal/engine/... ./cmd/
go test -run '^$' -bench 'Readiness|Aggregate' -benchmem ./internal/engine/data/
go test -race ./internal/config/
go test -race ./internal/config/ ./cmd/
make check
make validate
make validate CONFIG=configs/data_logger.json
```

Step 1's benchmarks are run before and after it, and compared; its `data_logger_v2` export is
compared against one written before it from the same config.
