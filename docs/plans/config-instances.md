# Config instances — subscriptions built by the composition root, universes as components

## Context

[config.md](../discussions/config.md) § Decisions and [protocol.md](../discussions/protocol.md)
§ Decisions, agreed 2026-11-18. Config parses; the composition root builds. Every config block
decodes into a config type, and what reaches the code is an instance the composition root builds
from it: a value type by its protocol constructor, a component by its registry factory. Two blocks
break that today:

- `subscriptions` decodes straight into `proto.Subscription`, which config then `Validate`s. The
  file's shape lives on a protocol type, and `Config` holds values nothing has constructed.
- `universes` decodes straight into `proto.Universe`, the rules. Their shape is checked in `Load`,
  their resolution in `cmd`. The rule language, a config format, is in protocol; the resolved value
  the engine consumes has no constructor and no `Validate`, its invariants holding only because
  `Resolve` sorts.

```
today
Load   subscriptions ─▶ []proto.Subscription ── s.Validate()
       universes     ─▶ []proto.Universe (rules) ── u.Validate()
cmd    proto.NewStreams(cfg.Subscriptions)
       u.Resolve(streams) ─▶ map[string]proto.ResolvedUniverse                         _universes

after
Load   []SubscriptionConfig ── at least one
       []UniverseConfig     ── names, type present, feature references
cmd    proto.NewSubscription each, duplicates ─▶ c.subscriptions        nil when one fails
       proto.NewStreams(c.subscriptions)
       reg.Universes.Create(type, opts + _streams) ─▶ map[string]proto.Universe        _universes
         rules factory: Decode ─ Validate ─ resolve ─ proto.NewUniverse
engine features.Expand(st, universes, specs): each universe's Streams is st
```

Three steps, implemented in order. After each the changes stay unstaged for review; the step is
committed once approved, then the next starts.

1. `proto.NewSubscription`; `SubscriptionConfig`; subscriptions built in `instantiate`.
2. Universes as components: `reg.Universes`, the `rules` type in `internal/domains/universe`,
   `config.UniverseConfig`, `addUniverse` through the registry.
3. `ResolvedUniverse` → `proto.Universe{Streams, Index, Pairs}`, `NewUniverse` and `Validate`; the
   factory ends in `NewUniverse`; `Expand` checks each universe's table.

Executes on `feature/config-instances`, branched from `feature/features-layer-v2` at `27dd54c`.

## Decisions

- **Constructors return the value with its errors.** `NewX(…) (X, []error)`: the value is returned
  even when invalid, so a caller can name it (the duplicate check reads an invalid subscription's
  `ID`); it is usable only without errors. A list, as `Validate` returns, because `cmd` prefixes each
  error with its path.
- **Fields stay exported** (agreed). Tests and other producers may write literals; `Validate` checks
  them.
- **Config parses; the composition root builds.** `Config` stays the file's shape:
  `Config.Subscriptions` is `[]SubscriptionConfig`, and `validate` keeps only the at-least-one
  check. `addSubscriptions`, in `cmd/instantiate.go`, builds each entry. Amended at review: the plan
  first built subscriptions in `Load`, returning a `Config` decoded from an unexported `file`.
- **Duplicate subscriptions are found where they are built.** A subscription's identity is derived
  from its built fields, as a feature variant's ID is, so `addSubscriptions` checks duplicates, with
  today's rule and messages. Names given in the file — providers, universes — stay config's.
- **A subscription failing to build holds back what is built over it.** Any invalid or duplicate
  entry leaves `c.subscriptions` nil: no stream table, no universes, no subscription or strategy
  reference checks in `wire`, no engine. They would only restate its error, as unmatched names
  ([config.md](../discussions/config.md) § Decisions, an engine with an unresolved dependency).
  A subscription naming a missing provider is still built; `wire` reports it, as today.
- **`SubscriptionConfig` carries the JSON tags**, `proto.Subscription` none. `derive` decodes into
  `proto.TimeframeDerivationSet`, whose `UnmarshalJSON` stays in protocol beside `Timeframe`'s and
  `StreamKind`'s: a value set's decoding is its invariant ([protocol.md](../discussions/protocol.md)
  § Decisions). `UniversePairs.UnmarshalJSON`, a rule's format, moves with the rules.
- **The stream table stays in `cmd`** (`instantiate`): derived from the subscriptions, not a block.
- **Universes are registry components** (agreed). Entry: `name`, `type`, and the type's options.
  `UniverseConfig` reserves `name` and `type`, a non-string either is a parse error, the rest is
  `Opts`. Config checks: name required and unique, type required, feature → universe references.
  `type` is required, as on every component; `rules` is the one type. Config files change: each
  universe entry gains `"type": "rules"`. None of `configs/*.json` has a universe; the config guide
  and tests do.
- **No `disabled` on universes** — not reserved, so the `rules` factory rejects it as an unknown
  option ([config.md](../discussions/config.md) § Open Questions).
- **The `rules` type lives in `internal/domains/universe`** (`rules.go`), registered as `rules`.
  Its config struct `RulesConfig{Rules []Rule}` is today's `Universe` without its name; `Rule` and
  `Pairs` are `UniverseRule` and `UniversePairs`; `RulesConfig.Validate() error` joins today's
  `Universe.Validate` list for `config.Decode`; `resolve` is today's `Resolve`. Bodies and messages
  unchanged.
- **The factory reports every error of its stage.** Decoding stops at the first JSON error; shape
  errors come back joined from `Validate`, resolution errors joined from `resolve`. `addUniverse`
  splits a joined error and prefixes each part `universe "name":`, as today's resolution errors.
- **Universe errors are all construction errors.** Shape errors move from `Load`
  (`universes[i] (name): rules[0].symbol: empty list`) to construction
  (`universe "name": rules[0].symbol: empty list`); decoding errors from `Load`'s
  `failed to parse config: json: unknown field "refrences"` to `universe "name": json: unknown field
  "refrences"`. A universe's shape and its resolution are now reported in the same pass. Config's
  own universe errors keep `universes[i]` / `universes[i] (name)`.
- **A joined factory error is split by one helper**, `parts(err) []error` in `cmd/main.go`, used
  by `reportErrors`, `wire` (the engine's error) and `addUniverse`; the first two hold the same
  loop today.
- **`Universe{Streams, Index, Pairs}`**: `Streams` is the stream table (`*Streams`) the universe
  indexes, `Index` its streams as indices into `Streams.All`, `Pairs` its pairs of indices. The
  field named for the table's type holds the table; consumers that read today's
  `ResolvedUniverse.Streams` read `Index`.
- **`NewUniverse` normalises, `Validate` is strict.** `NewUniverse(st, index, pairs)` copies its
  inputs, sorts and dedups the index, orders each pair `a < b`, sorts and dedups the pairs, then
  validates. `Validate` rejects anything not in that form, so a literal is held to what the engine
  relies on. A self-pair and a pair end outside `Index` are rejected, not repaired: a factory
  producing one has a bug. Dropping self-pairs stays the `rules` type's semantics.
- **Each universe holds its table by pointer**; the table is shared, never copied or written after
  `NewStreams`. `Expand` keeps its `st` argument — a feature without a universe runs on every
  stream, and the data layer is sized from it — and rejects a used universe whose `Streams` is not
  `st`. Not a singleton ([protocol.md](../discussions/protocol.md) § Decisions).

## Step 1 — `NewSubscription` and built subscriptions

### Changes

**`internal/protocol/subscription.go`**

```go
/*
NewSubscription returns the subscription of its fields with its Validate errors. It is returned even
when invalid, so a caller can name it by its ID; it is usable only without errors.
*/
func NewSubscription(symbol string, tf Timeframe, provider string, derive TimeframeDerivationSet) (Subscription, []error)
```

`Subscription`, `ID` and `Validate` are unchanged. `NewStreams`'s comment: "built by
`NewSubscription` without errors" in place of "Rules are enforced by config parsing".

**`internal/config/config.go`**

```go
// SubscriptionConfig is a subscriptions entry, built by the composition root with
// proto.NewSubscription.
type SubscriptionConfig struct {
	Symbol    string                       `json:"symbol"`
	Timeframe proto.Timeframe              `json:"timeframe"`
	Provider  string                       `json:"provider"`
	Derive    proto.TimeframeDerivationSet `json:"derive"`
}
```

- `Config.Subscriptions` is `[]SubscriptionConfig`.
- `validate` keeps `subscriptions: at least one subscription is required`; its per-entry and
  duplicate loop moves to `cmd`.

**`cmd/instantiate.go`**

```go
/*
addSubscriptions builds each entry of cfg with proto.NewSubscription into c.subscriptions, in order.
An invalid entry and a duplicate of an earlier one are errors, prefixed subscriptions[i]; an invalid
entry is not checked for duplicates, but a later valid copy of it is. Any error leaves
c.subscriptions nil.
*/
func (c *components) addSubscriptions(cfg []config.SubscriptionConfig) []error
```

- `components` gains `subscriptions []proto.Subscription`; `streams` is nil without them.
- `instantiate`'s step 0: `addSubscriptions`, then, with subscriptions, the stream table
  (`proto.NewStreams(c.subscriptions)`) and the universes over it.

**`cmd/wire.go`** — reads `c.subscriptions` for the provider and strategy reference checks,
`_subscriptions` and the provider map. With subscriptions nil it skips the strategy reference check
and leaves the engine unbuilt; non-nil, they are as configured, so the provider check's
`subscriptions[i]` indices are the config's.

### Tests

- `internal/protocol/subscription_test.go`:
  - New **`TestNewSubscription`**: a valid subscription comes back with its fields and no error; an
    invalid one comes back with its fields, so its `ID`, and exactly `Validate`'s errors.
  - `TestSubscriptionDecodesDerive` is deleted: `proto.Subscription` is no longer decoded. Its
    absent-`derive` check moves to config.
- `internal/config/config_test.go`:
  - `TestLoadDerive` gains a second subscription without `derive`, wanting an empty set.
  - `TestDuplicateSubscriptionRejected`, `TestInvalidSubscriptionNotCheckedForDuplicates`,
    `TestMissingSubscriptionProvider`, `TestMissingSubscriptionTimeframe`,
    `TestDeriveNotCoarserRejected` and `TestSymbolTimeframesOnOneProviderAccepted` move to `cmd`.
  - `TestValidateCollectsErrors` no longer wants its subscriptions' errors.
- `cmd/instantiate_test.go` — new **`TestInstantiateSubscriptions`**: valid subscriptions built in
  order, with the table and a universe over it; invalid entries and duplicates, with today's
  messages and duplicate rule; a missing timeframe reported alone; any error leaving no
  subscriptions, table or universes.
- `cmd/wire_test.go` — subscriptions on the stub components; new **`TestWireWithoutSubscriptions`**:
  no reference errors and no engine.
- `cmd/main_test.go`, `cmd/instantiate_test.go` — `Config` literals take `[]config.SubscriptionConfig`.

### Documentation

- `docs/Architecture.md`, Design Rules — after the protocol rule: "A value type is built by its
  constructor, `NewX`, returning the value with its `Validate` errors. Config parses the file; the
  composition root builds every block from it: value types (subscriptions) by their constructors,
  components by registry factories."
- [config.md](../discussions/config.md):
  - § Decisions: "Config parses; the composition root builds", and a failing subscription holding
    back what is built over it, in place of the planning-time bullet.
  - § Path from file to component: `main.go` builds each subscription with `NewSubscription`.
  - § Dry run: subscriptions are construction errors and hold back the table, universes and engine.
  - Solution: this plan; the table gains `SubscriptionConfig`, `addSubscriptions` and
    `NewSubscription`.
- [protocol.md](../discussions/protocol.md):
  - § What protocol holds: `Subscription` with `NewSubscription` and `Validate`.
  - § Config-decoded types: subscriptions are decoded into `SubscriptionConfig` and built by the
    composition root, which checks duplicates.
  - WIP: step 1 implemented.
- `configs/README.md` — the strictness note: subscriptions are construction errors, and one failing
  holds back the universes and the engine until fixed.

### Acceptance

- `go test -race ./internal/protocol/ ./internal/config/ ./cmd/` passes; `make check` clean.
- `make validate` and `make validate CONFIG=configs/data_logger.json` valid; a config with a bad
  derivation and a duplicate reports the same subscription lines as before, now with construction
  errors.

## Step 2 — universes as components

### Changes

**`internal/registry/registry.go`**

```go
Universes = &registry[proto.ResolvedUniverse]{ // proto.Universe from step 3
	factories: make(map[string]Factory[proto.ResolvedUniverse]),
}
```

**New `internal/domains/universe/rules.go`** — `internal/protocol/universe.go`'s rules, renamed,
as a factory:

```go
// RulesID is the rules universe type: streams and pairs selected by self-contained rules.
const RulesID = "rules"

func init() { reg.Universes.Register(RulesID, newRules) }

// RulesConfig is a rules universe's options: its streams and pairs are the union of its rules'.
type RulesConfig struct {
	Rules []Rule `json:"rules"`
}

// Rule selects streams by filters, and pairs them by at most one of References or Pairs.
type Rule struct { /* UniverseRule's fields and tags, unchanged */ }

// Pairs is "*", every pair of the rule's streams, or a list of explicit pairs.
type Pairs struct { /* UniversePairs's, unchanged */ }

func (p *Pairs) UnmarshalJSON(b []byte) error

// Validate returns c's shape errors, joined, path-named from the rule. Universe.Validate's body.
func (c RulesConfig) Validate() error

// resolve returns c's streams and pairs over st, or each name no stream has. Universe.Resolve's body.
func (c RulesConfig) resolve(st *proto.Streams) (proto.ResolvedUniverse, []error)

/*
newRules builds a rules universe: its options decoded strictly and validated, then resolved over the
injected stream table. Each stage's errors come back joined.
*/
func newRules(opts map[string]any) (proto.ResolvedUniverse, error) {
	// config.Decode(opts, &cfg) — strips _streams, rejects unknown keys, runs Validate
	// st := config.Dep[*proto.Streams](opts, "_streams")
	// u, errs := cfg.resolve(st); return u, errors.Join(errs...)
}
```

Unexported helpers move with them: `Rule.check`, `Rule.matches`, `Pairs.explicit`, `emptyList`,
`globAny`, `checkGlob`. Every message is unchanged.

**`internal/protocol/universe.go`** — only `ResolvedUniverse` stays. `Universe`, `UniverseRule`,
`UniversePairs`, `Validate`, `Resolve` and the helpers go.

**`internal/config/config.go`**

```go
// UniverseConfig is a universes entry: a named component whose non-reserved keys are its type's
// options, checked by its factory rather than here.
type UniverseConfig struct {
	Name string
	Type string
	Opts map[string]any
}

// UnmarshalJSON takes name and type, each a string if present, and leaves every other key in Opts.
func (u *UniverseConfig) UnmarshalJSON(data []byte) error
```

- `Config.Universes` is `[]UniverseConfig`.
- `validate`'s universe loop keeps the name checks and adds the type's:
  `universes[i] (name): missing required 'type' field`, prefixed by `universePath`. The
  `u.Validate()` call goes.
- A non-string `name` or `type` fails the parse: `failed to parse config: universe 'name' must be a
  string`.

**`cmd/main.go`** — blank-imports `internal/domains/universe`. New `parts(err error) []error`: a
joined error's parts, else the error alone; `reportErrors` uses it.

**`cmd/instantiate.go`**

- `addUniverse(cfg config.UniverseConfig, fcfg []config.FeatureConfig) []error`:
  - an unregistered type: `universe "name": type "x" not registered`;
  - options copied into a new map with `_streams` set to `c.streams`, then
    `reg.Universes.Create(cfg.Type, opts)`;
  - each of `parts(err)`, prefixed `universe "name":`;
  - the empty-universe check and the map entry, unchanged.
- `instantiate`'s step 0 comment: universes built from the registry over the table.

**`cmd/wire.go`** — the engine's joined error is split with `parts`.

**`configs/README.md`**

- Every universe example gains `"type": "rules"`: § Start here, § Universes (the three-rule
  universe, the bound example). The examples table lists rules only, unchanged.
- § Universes: "A universe is a `name`, a `type` and the type's options. `rules`, the one type,
  takes a list of `rules`." Names unique, `type` required.
- The strictness note: universes are built with the other components, so every universe error is
  reported with construction errors.
- Common errors:
  - "A misspelt key": `failed to parse config: json: unknown field "subscribtions"`; new row, a
    misspelt universe option: `universe "btc": json: unknown field "refrences"`.
  - An invalid `kind`, `pairs` neither `"*"` nor a list: prefixed `universe "btc":` in place of
    `failed to parse config:`.
  - An empty list in a rule, `references` and `pairs` in one rule, a filter on explicit pairs:
    prefixed `universe "btc":` in place of `universes[0] (btc):`.
  - New rows: a universe without a type, `universes[0] (btc): missing required 'type' field`; an
    unknown type, `universe "btc": type "rulez" not registered`.

### Tests

- **New `internal/domains/universe/rules_test.go`** — `internal/protocol/universe_test.go`'s
  tests, moved:
  - helpers: `testStreams`, `render`, `runResolve`, `wantErrs`, and `mustIndex`, copied from
    `stream_test.go`; `universe(t, rules...)` becomes `rules(t, js...) RulesConfig`, decoding each
    rule strictly.
  - `TestResolveFilters`, `TestResolveDerived`, `TestResolveNamed`, `TestResolveReferences`,
    `TestResolveExplicitPairs`, `TestResolveAllPairs`, `TestResolveUnion` and `TestResolveExamples`
    call `resolve`; `TestUniverseValidate` → `TestRulesValidate`, unwrapping the joined error;
    `TestUniversePairsUnmarshal` → `TestPairsUnmarshal`. Cases unchanged.
  - New **`TestRulesFactory`**, through `reg.Universes.Create(RulesID, opts)` with `_streams`: a
    valid universe resolves; an unknown option is `json: unknown field`; several shape errors come
    back joined, resolution not attempted; several unmatched names come back joined; a missing
    `_streams` is `missing dependency "_streams"`.
- `internal/protocol/universe_test.go` is deleted. `wantErrs`, which `subscription_test.go` uses,
  moves to `subscription_test.go`.
- `internal/config/config_test.go`:
  - `universeConfig`'s entries carry `"type": "rules"`.
  - `TestLoadUniverses`: each entry's name and type are taken, its `rules` left in `Opts`, and
    `name` and `type` not; the feature's universe as today.
  - `TestUniverseUnknownFieldRejected` → `TestUniverseOptionsLeftToFactory`: a misspelt rule key
    loads, left in `Opts`.
  - `TestUniverseErrors`: the rule-shape case goes (now the factory's); new case, a missing type.
  - New **`TestUniverseNameNotString`**: `"name": 3` fails the parse,
    `universe 'name' must be a string`.
  - `TestValidateCollectsErrors`: the universe has no type, wanting
    `universes[0] (u): missing required 'type' field`.
- `cmd/instantiate_test.go`:
  - A helper `rulesUniverse(name string, rules ...map[string]any) config.UniverseConfig`.
  - `TestInstantiateSpecs`: its universes through the helper; unchanged otherwise.
  - `TestInstantiateUniverses`: through the helper; new cases, an unregistered type and a universe
    with two shape errors, each reported `universe "name":`, kept out of the map.
- `cmd/main_test.go`:
  - `TestBuildReportsUniverseErrors` through `config.UniverseConfig`.
  - New **`TestParts`**: a joined error's parts, a plain error alone.

### Documentation

- `docs/Architecture.md`:
  - `internal/domains`: new sub-bullet, "internal/domains/universe: universe types, registered in
    `Universes`; `rules` selects streams and pairs by filters, references and pairings".
  - `internal/protocol`: universes are "resolved universes (`ResolvedUniverse`: a set of streams and
    pairs over the stream table, built by a universe type)".
  - `internal/registry`: adds `Universes`.
- [config.md](../discussions/config.md):
  - § Dry run: `Load` collects its validation errors; universes, shape and resolution, are
    construction errors.
  - Solution table: the universe rows name `UniverseConfig` and the `rules` factory;
    `addUniverse` through `reg.Universes`.
  - WIP: the universes bullet describes components; the interim note goes.
- [protocol.md](../discussions/protocol.md):
  - § What protocol holds and § Config-decoded types: universes are built by a universe type at the
    composition root; protocol holds the resolved set.
  - Solution: the universe row of `protocol-types.md`'s table is noted as superseded by this plan.
- [features.md](../discussions/features.md):
  - § Feature layer: "Universe resolution belongs to protocol" → "Universe selection is a
    component's — a universe type registered in `reg.Universes`, `rules` in
    `internal/domains/universe`, a pure function of its options over the stream table. See
    § Reversed on 2026-11-18."
  - New § Reversed on 2026-11-18: was, universes in protocol (`proto.Universe`, its rules,
    `Universe.Resolve`); now, the `rules` universe type, built by `reg.Universes` at the composition
    root; why, the rule language is a config format crossing no layer, and another selection is a
    new factory ([[protocol]]).
  - § Feature layer pieces, the registry row: universes in `internal/domains/universe`.
  - Solution § Universes: the example's entries carry `"type": "rules"`; "validated by `Load`" →
    "built by the composition root".

### Acceptance

- `go test -race ./internal/protocol/ ./internal/config/ ./internal/domains/universe/ ./cmd/`
  passes; `make check` clean.
- `make validate` and `make validate CONFIG=configs/data_logger.json` valid.
- A config with a rule shape error and an unmatched name in two universes reports both in one run.
- `internal/protocol` imports nothing outside it; `internal/domains/universe` imports only
  `config`, `protocol` and `registry`.

## Step 3 — `proto.Universe` and `NewUniverse`

### Changes

**`internal/protocol/universe.go`** — `ResolvedUniverse` renamed, its `Streams` renamed `Index`,
and given its table as `Streams`:

```go
// Universe is a set of streams and pairs over a stream table: what a universe type builds, and what
// features are placed on.
type Universe struct {
	Streams *Streams // the table it indexes; shared, never written
	Index   []int    // its streams, indices into Streams.All; ascending, unique
	Pairs   [][2]int // a < b, ascending, unique; both ends in Index
}

/*
NewUniverse returns the universe of index and pairs over st with its Validate errors. The inputs are
copied, the index sorted and deduplicated, each pair ordered a < b, the pairs sorted and
deduplicated. A self-pair or a pair end outside index is an error, not repaired.
*/
func NewUniverse(st *Streams, index []int, pairs [][2]int) (Universe, []error)

/*
Validate returns u's errors: no stream table alone; otherwise each index entry not in the table, an
index not ascending and unique, each pair not ordered a < b or of a stream with itself, pairs not
ascending and unique, and each pair end not in Index.
*/
func (u Universe) Validate() []error
```

Messages: `no stream table`; `stream 9 is not in the table of 7 streams`;
`index not ascending and unique at 2`; `pair (4, 2) is not ordered a < b`;
`pair (3, 3) pairs a stream with itself`; `pairs not ascending and unique at 1`;
`pair (1, 5): stream 5 is not in the universe`.

**`internal/protocol/stream.go`** — `Streams`'s comment: never written after `NewStreams`;
universes hold it by pointer.

**`internal/domains/universe/rules.go`** — `resolve` returns the selected stream indices and pairs
as collected (its sorting goes) and `newRules` ends in `proto.NewUniverse(st, index, pairs)`, its
errors joined. They can only be bugs: `resolve` drops self-pairs and adds both ends of every pair.

**`internal/registry/registry.go`** — `Universes` is `registry[proto.Universe]`.

**`internal/engine/features/expand.go`** — `Expand`, `expand`, `place` and `coordinates` take
`map[string]proto.Universe`. `coordinates`, on a spec's universe: `u.Streams != st` is
`features: rsi_14: universe "btc" is over another stream table`, checked before its pairs and
index; a single-stream feature's coordinates read `u.Index`.

**`internal/engine/data_logger_v2.go`** — `_universes` is `config.Dep[map[string]proto.Universe]`.

**`cmd/instantiate.go`** — `components.universes map[string]proto.Universe`; `addUniverse`'s
empty-universe check reads `len(u.Index)`.

### Tests

- `internal/protocol/universe_test.go`, new:
  - **`TestNewUniverse`**: an unsorted index with duplicates, reversed and repeated pairs come back
    canonical, `Streams` the given table, the caller's slices untouched; a self-pair, a pair end
    outside the index and an index entry past the table are each reported.
  - **`TestUniverseValidate`**: a canonical literal and an empty universe are valid; no stream table
    is reported alone; an unsorted or duplicate index, a reversed pair, unsorted or duplicate pairs
    are each reported.
- `internal/domains/universe/rules_test.go`: `render` and `runResolve` read `proto.Universe`'s
  `Index`; `runResolve` also wants `Streams` to be the table resolved over.
- `internal/engine/features/expand_test.go`:
  - `universe(st, index, pairs...)` sets `Streams`; `testUniverses` becomes
    `testUniverses(st *proto.Streams) map[string]proto.Universe`, every caller passing the table it
    expands over.
  - `TestExpandErrors` gains a universe over another `testStreams()` table, wanting
    `universe "btc" is over another stream table`.
- `internal/engine/features/step_test.go`, `internal/engine/data_logger_v2_test.go`,
  `cmd/wire_test.go`, `cmd/instantiate_test.go` — `proto.ResolvedUniverse` → `proto.Universe`,
  `.Streams` → `.Index`; universes built over the test's table set `Streams`.

### Documentation

- `docs/Architecture.md`, `internal/protocol`: "universes (`Universe`: a set of streams and pairs
  over the stream table — `Streams`, the table by pointer; `Index`, its streams' indices; `Pairs` —
  built by `NewUniverse` from a universe type's selection)".
- [features.md](../discussions/features.md):
  - § Feature layer, "One stream table for every layer": universes hold the table by pointer;
    `Expand` rejects a universe over another.
  - § Reversed on 2026-11-18, new row: was, `ResolvedUniverse`, its order kept by `Resolve`; now,
    `proto.Universe{Streams, Index, Pairs}`, built by `NewUniverse`; why, the invariants consumers rely on
    hold for every universe type ([[protocol]]).
  - WIP: this plan implemented; the planned note goes.
- [protocol.md](../discussions/protocol.md):
  - § What protocol holds: `Universe` with `NewUniverse` and `Validate`; `ResolvedUniverse` gone.
  - Solution: this plan executed, its table rows for subscriptions and universes.
  - WIP: implemented, nothing pending.
- [config.md](../discussions/config.md) — Solution lists this plan as executed; WIP: implemented.
- [README.md](../discussions/README.md) — `config.md`'s and `protocol.md`'s rows: implemented.

### Acceptance

- `go test -race ./internal/protocol/ ./internal/config/ ./internal/domains/universe/
  ./internal/engine/... ./cmd/` passes; `make check` clean.
- `make validate` and `make validate CONFIG=configs/data_logger.json` valid.
- No `ResolvedUniverse` remains in code or current docs.

## Documentation recorded at planning

Committed with this plan:

- [protocol.md](../discussions/protocol.md) — § Decisions, agreed 2026-11-18: constructors,
  universes as a protocol value selected by a component, one stream table shared by pointer; the
  2026-11-15 universe bullet marked revised; WIP: planned.
- [config.md](../discussions/config.md) — § Decisions: every block decodes into a config type and
  reaches the code as an instance (revised at review: config parses, the composition root builds);
  universes are components. § Open Questions: `disabled` on a
  universe. WIP: planned.
- [features.md](../discussions/features.md) — WIP: planned.
- [README.md](../discussions/README.md) — `config.md`'s and `protocol.md`'s rows: planned.

## Out of scope

- Unexported fields and accessors on protocol value types.
- `disabled` on universes; other universe types.
- Constructors for other protocol value types.
- The exporter as a top-level component ([config.md](../discussions/config.md) § Open Questions).
- `docs/plans/multi-subscription/`, untracked in the working tree, not touched.

## Verification

```bash
# step 1
go test -race ./internal/protocol/ ./internal/config/ ./cmd/
# step 2
go test -race ./internal/protocol/ ./internal/config/ ./internal/domains/universe/ ./cmd/
go list -f '{{join .Imports "\n"}}' ./internal/domains/universe | grep whatever/   # config, protocol, registry
# step 3
go test -race ./internal/protocol/ ./internal/config/ ./internal/domains/universe/ ./internal/engine/... ./cmd/
grep -rn ResolvedUniverse --include=*.go .                                          # nothing
# every step
make validate
make validate CONFIG=configs/data_logger.json
go list -f '{{join .Imports "\n"}}' ./internal/protocol | grep whatever/            # nothing
make check
```
