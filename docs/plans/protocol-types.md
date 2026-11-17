# Protocol types — universes, the feature handoff and subscription invariants

## Context

[protocol.md](../discussions/protocol.md) § Decisions: protocol holds the types layers exchange,
each with its own invariants. `cmd` and `config` reach the core only through protocol and the
registry. Today three things break that:

- Universes live in `internal/config`, and the core consumes them: `engine/features/expand.go` and
  `engine/data_logger_v2.go` import `config` for `config.Resolved`.
- `cmd/instantiate.go` imports `internal/engine/features` for `features.Spec`, the composition
  root's handoff to the engine.
- Consumers read feature results through `features.View` and `ViewOf`, in
  `internal/engine/features`. A domain (a strategy) cannot import the engine.

A subscription's own invariants are also checked in `config.validate`, not on the type.

```
Load       cfg.Universes []proto.Universe ── u.Validate()            config prefixes universes[i] (name)
           cfg.Subscriptions ── s.Validate(), s.Complete()           config prefixes subscriptions[i] (ID)
cmd        u.Resolve(streams) ─▶ map[string]proto.ResolvedUniverse   _universes
           reg.FeatureTypes ─▶ []feature.Spec                         _featuresv2
engine     features.Expand(streams, universes, specs) ─▶ Graph
           fe.Step(view) ─▶ feature.FeaturesView ─▶ feature.ViewOf[T]
```

The plan has three steps, implemented in order. Step 2 changes `engine/features/engine.go`, which
features-layer-v2 stage 6 step 2 also changes, so this plan runs after stage 6 and before stage 7.
Stage 7 then fills the exported record through `feature.FeaturesView`. After each step the changes
stay unstaged for review; the step is committed once approved, then the next starts.

1. Universes in `internal/protocol`.
2. `Spec`, `FeaturesView` and `ViewOf` in `internal/protocol/feature`.
3. `Subscription.Validate`.

## Decisions

- **Universe names in the root package.** `Universe` keeps its name. `Rule` → `UniverseRule`,
  `Pairs` → `UniversePairs`, `Resolved` → `ResolvedUniverse`. `proto.Rule` and `proto.Pairs` would
  not say what they belong to.
- **`Resolve` becomes a method,** `Universe.Resolve(st *Streams)`, and `check` becomes
  `Universe.Validate() []error`. It returns a list, as `check` did, because config prefixes each
  error with its path. `feature.Feature.Validate` returns one error, since its caller reports it
  whole.
- **Unexported helpers move with the types:** `UniverseRule.check` and `matches`,
  `UniversePairs.explicit`, `emptyList`, `globAny`, `checkGlob`. `universePath` stays in config,
  moved into `config.go`: it names a config path.
- **Every error message is unchanged.** Config and `cmd` keep their prefixes
  (`universes[i] (name):`, `universe "name":`, `subscriptions[i] (ID):`), except `ViewOf`'s, below.
- **`FeaturesView` holds each result's buffer, and `ViewOf` pins it at the view's own frame**, as
  today's `View` does through the graph. The index maps a composed ID to its column's
  `*frame.Buffer[T]` (`column.buffer()`), in an unexported field, so consumers still get only
  pinned views. A `FeaturesView` kept past the next `Step` reads its own frame: no read sees a later
  column. Amended at implementation: the plan first held the engine's views, re-pinned each frame,
  which an older `FeaturesView` read at the newer frame (`TestEngineFrame`).
- **The index is built once, in `New`.** `Step` returns `feature.NewFeaturesView(v, e.results,
  e.g.pairs)`, a struct holding two map references, so `Step` still allocates nothing.
- **`ViewOf`'s errors take its package's prefix**: `feature: unknown result "x"`,
  `feature: result "x" is not int`, as `feature: dependency "last" is not float64` does.
- **`Spec` sits beside `Feature`** in `internal/protocol/feature/feature.go`, unchanged.
- **A subscription validates itself.** `Validate() []error` returns each missing field; with none
  missing, it returns each derived window not coarser than the timeframe or not a multiple of it.
  `Complete() bool` reports whether `ID` names the subscription. Config needs it for the path and
  the duplicate check. Config keeps the duplicate check, the at-least-one check and the provider
  name rules, which span entries.

## Step 1 — universes in `internal/protocol`

### Changes

**New `internal/protocol/universe.go`** — `internal/config/universe.go`'s types and bodies, renamed:

```go
// Universe is a named set of streams and pairs, the union of its rules' selections.
type Universe struct {
	Name  string         `json:"name"`
	Rules []UniverseRule `json:"rules"`
}

// UniverseRule selects streams by filters, and pairs them by at most one of References or Pairs.
type UniverseRule struct {
	Symbol     []string       `json:"symbol"` // globs
	Timeframe  []Timeframe    `json:"timeframe"`
	Provider   []string       `json:"provider"` // globs over provider instance names
	Kind       []StreamKind   `json:"kind"`
	References []string       `json:"references"` // stream IDs
	Pairs      *UniversePairs `json:"pairs"`
}

// UniversePairs is "*", every pair of the rule's streams, or a list of explicit pairs.
type UniversePairs struct {
	All  bool
	List [][]string
}

func (p *UniversePairs) UnmarshalJSON(b []byte) error

// ResolvedUniverse is a universe's streams and pairs as indices into the stream table's All.
type ResolvedUniverse struct {
	Streams []int    // ascending
	Pairs   [][2]int // a < b, ascending
}

/*
Validate returns u's shape errors, path-named from the rule: pairing, empty lists and values, globs.
Names are left to Resolve: a name is valid if it is a stream's ID.
*/
func (u Universe) Validate() []error

/*
Resolve returns u's streams and pairs over st. … (config.Resolve's comment, unchanged). u must have
passed Validate.
*/
func (u Universe) Resolve(st *Streams) (ResolvedUniverse, []error)
```

**`internal/config`**

- `universe.go` is deleted; `universePath` moves to `config.go`.
- `Config.Universes []proto.Universe`; `validate` calls `u.Validate()`.

**`cmd/instantiate.go`** — `components.universes map[string]proto.ResolvedUniverse`;
`addUniverse(cfg proto.Universe, …)` calls `cfg.Resolve(c.streams)`.

**`internal/engine/features/expand.go`** — `Expand`, `expand`, `place` and `coordinates` take
`map[string]proto.ResolvedUniverse`. The package no longer imports `config`.

**`internal/engine/data_logger_v2.go`** — `_universes` is
`config.Dep[map[string]proto.ResolvedUniverse]`.

### Tests

- `internal/config/universe_test.go` moves to `internal/protocol/universe_test.go` (package
  `proto`), with these changes:
  - `proto.` qualifiers dropped; `Resolve(u, st)` → `u.Resolve(st)`; `u.check()` → `u.Validate()`;
    `Resolved` → `ResolvedUniverse`; `Rule` → `UniverseRule`.
  - Its `index` helper is deleted in favour of `stream_test.go`'s `mustIndex`, which has the same
    signature.
  - `TestUniverseCheck` → `TestUniverseValidate`; `TestPairsUnmarshal` →
    `TestUniversePairsUnmarshal`.
  - The cases are unchanged.
- `internal/config/config_test.go` — `TestLoadUniverses` can no longer call the unexported
  `explicit`: `Pairs != nil && !Pairs.All`. The other universe tests go through `Load`, unchanged.
- `cmd/main_test.go`, `cmd/instantiate_test.go`, `cmd/wire_test.go` — `config.Universe`,
  `config.Rule`, `config.Resolved` → `proto.Universe`, `proto.UniverseRule`,
  `proto.ResolvedUniverse`.
- `internal/engine/features/expand_test.go` (`universe`, `testUniverses`), `step_test.go`,
  `internal/engine/data_logger_v2_test.go` — `config.Resolved` → `proto.ResolvedUniverse`.

### Documentation

- `docs/Architecture.md`:
  - `internal/protocol`: "the types layers exchange, each with its own invariants: domain
    interfaces, the stream table (…), and universes (`Universe`, named stream and pair sets
    resolved over the stream table into a `ResolvedUniverse`)".
  - Design Rules: "Protocol defines contracts; implementations live in domains" becomes:
    - "Protocol holds the types layers exchange: contracts, and value types holding their own
      invariants (`Validate`, validating `UnmarshalJSON`). Implementations live in domains, which
      fill in protocol values rather than embed protocol types."
    - "`cmd` and `config` reach the core only through protocol and the registry; the core imports
      `config` only for the factory helpers."
- [features.md](../discussions/features.md):
  - § Feature layer: "Universe resolution belongs to config" → "Universe resolution belongs to
    protocol — `Universe.Resolve`, a pure function of rules over the stream table, independent of
    the feature engine. See § Reversed on 2026-11-15." The interim note recorded at planning is
    dropped.
  - New § Reversed on 2026-11-15:
    - Was: universes in config (`config.Universe`, `Resolve`, `Resolved`).
    - Now: universes in protocol (`proto.Universe`, `Universe.Resolve`, `ResolvedUniverse`).
    - Why: the engine consumes resolved universes, which are decoded from the file like
      subscriptions; `cmd` and `config` reach the core only through protocol ([[protocol]]).
  - § Feature layer pieces: the registry and config row names `internal/protocol` for universes.
  - WIP: universes moved into protocol, per this plan.
- [config.md](../discussions/config.md):
  - Solution lists this plan.
  - Its table row → `UniverseRule`, `UniversePairs`, `Universe.Validate`, `Universe.Resolve` in
    `internal/protocol/universe.go`; `universePath` in `config.go`.
  - WIP "Universe resolution is config's" → protocol's (`Universe.Resolve`), its interim note
    dropped.
- [protocol.md](../discussions/protocol.md) — WIP: step 1 implemented.

### Acceptance

- `go test -race ./internal/protocol/ ./internal/config/ ./internal/engine/... ./cmd/` passes;
  `make check` clean; `make validate` and `make validate CONFIG=configs/data_logger.json` valid.
- `internal/engine/features` imports no `config`; `internal/protocol` imports nothing outside it.

## Step 2 — `Spec`, `FeaturesView` and `ViewOf` in `internal/protocol/feature`

### Changes

**`internal/protocol/feature/feature.go`** — `Spec` moves here from `expand.go`, unchanged:

```go
// Spec is one configured variant: what the composition root builds from a feature entry.
type Spec struct {
	Type     string  // the entry's type: the base ID other types' Deps name
	Universe string  // the entry's universe; "" runs a single-stream feature on every stream
	Feature  Feature // the variant reg.FeatureTypes built from the entry's options
}
```

**New `internal/protocol/feature/view.go`** — `internal/engine/features/view.go`, renamed and
holding views instead of the graph:

```go
// FeaturesView is the feature layer at one frame: the data view, and every result pinned at its
// column. Valid until the next frame is stepped.
type FeaturesView struct {
	frame.View[proto.Candle]
	results map[string]any // by composed ID: its *frame.Buffer[T], pinned by ViewOf
	pairs   map[[2]int]int // the pair table: (a, b), a < b → entry
}

// NewFeaturesView returns the feature layer at v's frame. results holds each composed ID's
// *frame.Buffer[T]; pairs is the pair table. Both are shared, never written.
func NewFeaturesView(v frame.View[proto.Candle], results map[string]any, pairs map[[2]int]int) FeaturesView

// ViewOf returns composed ID id's results at v's frame: entry i is stream i, or pair i of the pair
// table. An unknown ID, or a value type other than T, is an error.
func ViewOf[T any](v FeaturesView, id string) (frame.View[T], error) // asserts *frame.Buffer[T], pins it at v's frame

// Pair returns the pair (a, b)'s entry in pair results, in either order.
func (v FeaturesView) Pair(a, b int) (int, bool)
```

**`internal/engine/features`**

- `view.go` is deleted. `expand.go` takes `[]feature.Spec`.
- `column` gains `buffer() any`: its `*frame.Buffer[T]`, nil until `alloc`.
- `Engine` gains `results map[string]any`, filled once in `New`, after allocating, from
  `g.columns` with each column's `buffer()`. `pin` returns
  `feature.NewFeaturesView(v, e.results, e.g.pairs)`, and `Step` returns `feature.FeaturesView`.

**`cmd/instantiate.go`** — `components.featuresV2 []feature.Spec`. It imports
`internal/protocol/feature` instead of `internal/engine/features`.

**`internal/engine/data_logger_v2.go`** — `_featuresv2` is `config.Dep[[]feature.Spec]`.

### Tests

- **New `internal/protocol/feature/view_test.go`** — **`TestFeaturesView`**, over buffers built and
  pinned by hand (`buffers()`):
  - `ViewOf[float64]` reads what the buffer pinned at the view's frame reads; two `FeaturesView`s
    sharing one index each read their own frame.
  - `ViewOf[int]` on a float64 result is `feature: result "x" is not int`; an unknown ID is
    `feature: unknown result "y"`.
  - `Pair` finds a pair in either order, and reports a missing one.
  - `ViewOf` and `Pair` allocate nothing.
- `internal/engine/features` — `View` → `feature.FeaturesView`, `ViewOf` → `feature.ViewOf`,
  `Spec` → `feature.Spec` (`features_test.go`'s `spec`, `expand_test.go`, `step_test.go`,
  `engine_test.go`). `TestViewOf`'s three messages take the `feature:` prefix.
  `TestEngineAllocations` is unchanged and still wants zero.
- `cmd/wire_test.go`, `internal/engine/data_logger_v2_test.go` — `features.Spec` → `feature.Spec`.

### Documentation

- `docs/Architecture.md`:
  - `internal/engine/features`: "the `View` consumers read results through" → "handing consumers
    each frame's `feature.FeaturesView`".
  - `internal/protocol/feature`: adds `Spec`, one configured variant the composition root hands
    the engine, and `FeaturesView` with `ViewOf`, the feature layer at a frame that consumers read
    results through. "Holds no buffer" becomes "holds no buffer of its own; a `FeaturesView`
    pins the engine's".
- [features.md](../discussions/features.md):
  - § Feature buffer: `View.Pair` → `FeaturesView.Pair`; "Only the feature engine holds feature
    buffers" notes that a `FeaturesView` carries them unexported, pinned by `ViewOf`.
  - § Feature layer pieces: consumers read through `feature.FeaturesView`.
  - Open Questions, struct results: `ViewOf` is in `internal/protocol/feature`; the struct type's
    home is still open.
  - § Reversed on 2026-11-15, new row:
    - Was: `features.View` and `ViewOf` in `internal/engine/features`, reading the graph.
    - Now: `feature.FeaturesView` and `ViewOf` in `internal/protocol/feature`, reading the views
      the engine pins.
    - Why: domains read results without importing the engine.
- [main.md](features-layer-v2/main.md) — Decisions, "Consumers read only the feature layer's view":
  `Step` returns a `feature.FeaturesView`, per this plan.
- [protocol.md](../discussions/protocol.md) — WIP: step 2 implemented.

### Acceptance

- `go test -race ./internal/protocol/... ./internal/engine/... ./cmd/` passes; `make check` clean;
  `make validate` valid.
- `cmd` imports no `internal/engine` package other than through `main.go`'s blank imports.

## Step 3 — `Subscription.Validate`

### Changes

**`internal/protocol/subscription.go`**

```go
// Complete reports whether s has every field ID reads: a symbol, a timeframe and a provider.
func (s Subscription) Complete() bool

/*
Validate returns s's errors: each missing field; or, with none missing, each derived window not
coarser than the timeframe or not a multiple of it.
*/
func (s Subscription) Validate() []error

// missing returns the names of s's empty string fields, lowercased: config.validate's loop, moved.
func (s Subscription) missing() []string
```

Messages, as config's without their prefix: `missing required 'timeframe' field`,
`derive "5m" must be coarser than "1h"`, `derive "…" must be a multiple of "…"`.

**`internal/config/config.go`** — `validate`'s subscription loop:

```go
for i, s := range c.Subscriptions {
	at := fmt.Sprintf("subscriptions[%d]", i)
	if !s.Complete() { // no ID, and no timeframe to derive from
		// each of s.Validate(), prefixed at
		continue
	}
	// duplicate check on s.ID(), unchanged
	// each of s.Validate(), prefixed at (ID)
}
```

The `reflect` import goes; `strings` stays for the provider name check.

### Tests

- `internal/protocol/subscription_test.go` — **`TestSubscriptionValidate`**:
  - A complete subscription without derivations has no error.
  - Each missing field gives its own error; a missing timeframe with derivations gives the missing
    field only.
  - A derived window that is equal, finer or not a multiple gives its error.
  - `Complete` is true exactly when no field is missing.
- `internal/config/config_test.go` — unchanged. `TestMissingSubscriptionTimeframe`,
  `TestDeriveNotCoarserRejected`, `TestDuplicateSubscriptionRejected` and `TestValidateCollectsErrors`
  pin the prefixed messages and their order.

### Documentation

- [config.md](../discussions/config.md) — Solution table: `Subscription.Validate` and `Complete` in
  `internal/protocol/subscription.go`; config keeps the checks that span entries.
- [protocol.md](../discussions/protocol.md):
  - Solution lists this plan as executed.
  - WIP: implemented, nothing pending.
- [README.md](../discussions/README.md) — `protocol.md`'s row: implemented.

### Acceptance

- `go test -race ./internal/protocol/ ./internal/config/ ./cmd/` passes; `make check` clean.
- `--validate-only` on a config with a missing provider, a bad derivation and a duplicate reports
  the same lines as before the step.

## Documentation recorded at planning

Committed with this plan, beside the discussion it executes:

- [protocol.md](../discussions/protocol.md):
  - Solution: planned in this plan; `FeaturesView` holds each result's pinned view, not its buffer
    (reverted to buffers at implementation, § Decisions).
  - WIP: planned.
- [features.md](../discussions/features.md), [config.md](../discussions/config.md) — the interim
  notes say "planned in `docs/plans/protocol-types.md`".
- [README.md](../discussions/README.md) — `protocol.md`'s row: planned.

## Out of scope

- Moving the factory helpers out of `config`: kept ([protocol.md](../discussions/protocol.md)
  § Decisions).
- Where a struct result type lives ([features.md](../discussions/features.md) § Open Questions).
- A test enforcing protocol's or `cmd`'s import rules.
- features-layer-v2 stage 6 step 2 and stage 7.

## Verification

```bash
# step 1
go test -race ./internal/protocol/ ./internal/config/ ./internal/engine/... ./cmd/
make validate
make validate CONFIG=configs/data_logger.json
go list -f '{{join .Imports "\n"}}' ./internal/engine/features | grep internal/config   # nothing
# step 2
go test -race ./internal/protocol/... ./internal/engine/... ./cmd/
go list -f '{{join .Imports "\n"}}' ./cmd | grep engine/features                       # nothing
# step 3
go test -race ./internal/protocol/ ./internal/config/ ./cmd/
# every step
make check
```
