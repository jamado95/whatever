# Protocol

## Topic

What `internal/protocol` holds, and the import direction between the composition root (`cmd`),
`config` and the core (`engine`, `domains`).

Objective of this discussion: a rule for what belongs in protocol, so types that cross a layer
boundary carry their own invariants there, and `cmd` and `config` reach the core only through
protocol and the registry. First application: universes.

Opened 2026-11-15 from a review of the features-layer-v2 import graph at commit `0d71d03`.

## Current Understanding

### What protocol holds

More than interfaces:

| Kind | Examples |
|---|---|
| Interfaces | `DataProvider`, `Strategy`, `RiskManager`, `Executor`, `Exporter`, the originals' `Feature` |
| Config-decoded value types | `Subscription` with `NewSubscription` and `Validate`; `Universe` with `Validate` and `Resolve`; `Timeframe`, `TimeframeDerivationSet`, `StreamKind` and `UniversePairs`, with validating `UnmarshalJSON` |
| Structures with logic | `Streams` and `NewStreams`, `ResolvedUniverse`, `SortedWindow`, `Snapshot` |
| Contracts as concrete types | `feature.Feature` with `Validate`, `Spec`, `Binder`, `Input[T]`, `FeaturesView` and `ViewOf`; `frame` rings, buffers and views |

`docs/Architecture.md` § Design Rules states the rule in § Decisions.

### Import graph

`config` imports only `protocol`. `cmd` imports `config`, `registry`, `protocol`,
`protocol/feature` and `logger`, and blank-imports the domains and `engine` so their factories
register. The core imports `config` only for the factory helpers — `Decode`, `DecodeOptions`,
`Dep`, `OptDep`, `Duration` — never for the file's shape.

### Config-decoded types

Universes and subscriptions are decoded from the file and consumed by the core. Subscriptions are
decoded into `config.SubscriptionConfig` and built by the composition root with
`proto.NewSubscription`. Each checks its own invariants: `Universe.Validate` its rules' shape,
`Subscription.Validate` its fields, timeframe and derived windows. Config adds the checks that span
entries — names required and unique, feature → universe references, provider names — and prefixes
each error with its path; the composition root checks duplicate subscriptions as it builds them. It
resolves universes over the stream table (`Universe.Resolve`) and injects them; `Expand` consumes
`ResolvedUniverse`.

### Embedding versus protocol values

The feature contract has the domain factory return a `feature.Feature` value, and the engine
consumes that struct, so `Validate` and binding run at the boundary for every variant. A protocol
base type embedded in a domain type enforces nothing: the core sees an interface, the outer type can
shadow promoted methods, and a zero-value embed skips its constructor.

## Decisions

Agreed 2026-11-15.

- **Protocol holds the types layers exchange, not only interfaces.** A type belongs in protocol when
  it crosses a layer boundary and its invariants hold whichever implementation produces or consumes
  it. Those invariants live on the type (`Validate`, validating `UnmarshalJSON`), not in the layer
  that reads it first. Out of protocol: mechanisms (`Decode`, `Dep`, the registry), the file's own
  shape (`Config`, `ComponentConfig`), cross-entry checks (unique names, references between blocks),
  which stay in config, and per-implementation logic, which stays in domains.
- **`cmd` and `config` reach the core only through protocol and the registry**, blank imports for
  registration excepted.
- **The core keeps importing `config` for the factory helpers** — `Decode`, `DecodeOptions`, `Dep`,
  `OptDep`, `Duration`, which read a factory's `map[string]any` — never for the file's shape.
  `config` imports only protocol, so no cycle.
- **Domains fill in protocol values; embedding does not enforce invariants.** The core consumes
  protocol values that domains construct, as with `feature.Feature`. Embedding only for shared data
  fields. See § Embedding versus protocol values.
- **Universes are a protocol primitive, in the root package** beside `stream.go`, `Resolved`
  renamed `ResolvedUniverse`. Rules, their shape check, `Resolve` and `Resolved` move out of config.
  Reverses [[features]] § Feature layer "Universe resolution belongs to config"; its reason, a pure
  function independent of the feature engine, still holds. Config keeps the `universes` block's
  cross-entry checks. *Revised 2026-11-18, below: only the resolved universe stays in protocol.*

Agreed 2026-11-18, planned in [`docs/plans/config-instances.md`](../plans/config-instances.md):

- **A value type is built by its constructor.** `NewX(…) (X, []error)` returns the value with its
  `Validate` errors. Fields stay exported, so a literal (a test's) is checked with `Validate`.
  `NewSubscription` is the first; the composition root builds every subscription with it.
- **A universe is a protocol value; how it selects streams is a component's.** `proto.Universe` is
  the resolved set (today's `ResolvedUniverse`, renamed): `Streams`, the stream table it indexes;
  `Index`, its streams' indices into it; and `Pairs`. `NewUniverse` normalises — sorts, dedups,
  orders each pair `a < b` — and `Validate` enforces what consumers rely on: a table, indices in it,
  index and pairs ascending and unique, no self-pair, both ends of a pair in `Index`. Selection is a universe type's, registered in
  `reg.Universes`, its factory ending in `NewUniverse`. Today's rules, their shape check and `Resolve`
  become the `rules` type in `internal/domains/universe`, so another selection (top N by volume, a
  correlation cluster) is a new factory. The rule language is a config format, not a contract between
  layers: only the resolved set crosses into the engine.
- **One stream table, shared by pointer.** Each `Universe` holds the table it was built over, never
  a copy. The table is never written after `NewStreams`. `Expand` rejects a universe over a table
  other than its own. Not a singleton: tests and research runs build several tables in one process,
  and protocol holds no state.
- **Feature results are read through protocol.** `View` and `ViewOf` move to
  `internal/protocol/feature`, `View` renamed `FeaturesView`, so strategies read results without
  importing the engine ([[features]] § Open Questions, struct results).
- **`protocol` still imports nothing outside itself** ([[features]] § Feature layer). New
  subpackages import the root, never the reverse.

## Solution

Executed: [`docs/plans/protocol-types.md`](../plans/protocol-types.md), 2026-11-17.

| Piece | Was | Now |
|---|---|---|
| `Universe`, `Rule`, `Pairs`, `Resolve`, `Resolved` | `internal/config/universe.go` | `internal/protocol/universe.go`: `Universe`, `UniverseRule`, `UniversePairs`, `ResolvedUniverse`; `Universe.Validate`, `Universe.Resolve` |
| `View`, `ViewOf`, `Pair` | `internal/engine/features/view.go`, holding the engine's `*Graph` | `internal/protocol/feature`, `View` renamed `FeaturesView`. It holds an index `New` builds once — each composed ID's `*frame.Buffer[T]`, type-erased and unexported — and the pair table; `ViewOf` pins a buffer at the view's own frame, so a view kept past the next `Step` still reads its frame |
| Universe names required and unique, feature → universe references, error paths | `config.validate`, `universePath` | Unchanged |
| `Spec` | `internal/engine/features` | `internal/protocol/feature`; `cmd` imports no engine package |
| A subscription's required fields and derived windows | `config.validate` | `proto.Subscription.Validate`, which also rejects an unknown timeframe and names the subscription in a derived window's error; duplicates stay in config, which reports none for an invalid entry |
| Protocol's description and design rule | `docs/Architecture.md` | The rule in § Decisions |

## Open Questions

None.

## WIP

- Implemented — `docs/plans/protocol-types.md`, steps 1 to 3.
- [`docs/plans/config-instances.md`](../plans/config-instances.md): step 1, `NewSubscription`,
  implemented. Planned: universes as components, `ResolvedUniverse` → `Universe` with its table and
  `NewUniverse`.

## Resources

- `internal/protocol/universe.go`, `internal/protocol/subscription.go`,
  `internal/protocol/feature/view.go`, `internal/config/config.go`, `cmd/instantiate.go`
- `docs/Architecture.md` § Design Rules, `docs/Protocol.md`
- Related: [[features]] (§ Feature layer, § Universes), [[config]], [[engine]] (composition root),
  [[strategy-layer]]
