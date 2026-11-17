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
| Config-decoded value types | `Subscription`; `Timeframe`, `TimeframeDerivationSet` and `StreamKind`, with validating `UnmarshalJSON` |
| Structures with logic | `Streams` and `NewStreams`, `SortedWindow`, `Snapshot` |
| Contracts as concrete types | `feature.Feature` with `Validate`, `Binder`, `Input[T]`; `frame` rings, buffers and views |

`docs/Architecture.md` still describes it as "domain interfaces, and the stream table", with the
design rule "Protocol defines contracts; implementations live in domains".

### Import graph

`config` imports only `protocol`. `cmd` imports `config`, `registry`, `protocol` and `logger`, and
blank-imports the domains and `engine` so their factories register. The edges into or out of the
core:

| Edge | Files | Carries |
|---|---|---|
| `cmd` → `engine/features` | `cmd/instantiate.go` | `features.Spec`, the composition root's handoff to the engine |
| core → `config`, universes | `engine/features/expand.go`, `engine/data_logger_v2.go` | `config.Resolved` |
| core → `config`, factory mechanism | 19 of the 20 non-test files importing `config` | `Decode`, `DecodeOptions`, `Dep`, `OptDep`, `Duration` — Architecture.md § Design Rules |

### Universes

`Universe`, `Rule` and `Pairs` (the block's schema and shape check), `Resolve` (a pure function of
rules over `proto.Streams`) and `Resolved` (indices into the stream table) live in
`internal/config/universe.go`. `Load` checks their shape and the feature → universe references; the
composition root resolves them (`addUniverse`) and injects them; `Expand` consumes `Resolved`.
[[features]] § Feature layer placed them in config: "a pure function of rules over the stream table,
independent of the feature engine".

They have the shape of `proto.Subscription`: decoded from the file into `Config`, consumed by the
core. A subscription's own invariants — required fields, each derived window coarser than and a
multiple of its source — are still checked in `config.validate`, not on the type.

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
  cross-entry checks.
- **Feature results are read through protocol.** `View` and `ViewOf` move to
  `internal/protocol/feature`, `View` renamed `FeaturesView`, so strategies read results without
  importing the engine ([[features]] § Open Questions, struct results).
- **`protocol` still imports nothing outside itself** ([[features]] § Feature layer). New
  subpackages import the root, never the reverse.

## Solution

Planned in [`docs/plans/protocol-types.md`](../plans/protocol-types.md); steps 1 and 2 executed:

| Piece | Now | Proposed |
|---|---|---|
| `Universe`, `Rule`, `Pairs`, `Resolve`, `Resolved` | `internal/config/universe.go` | `internal/protocol/universe.go`: `Universe`, `UniverseRule`, `UniversePairs`, `ResolvedUniverse`; `Universe.Validate`, `Universe.Resolve` |
| `View`, `ViewOf`, `Pair` | `internal/engine/features/view.go` | `internal/protocol/feature`, `View` renamed `FeaturesView`. It holds the engine's `*Graph` today; in protocol it holds an index `New` builds once — each composed ID's `*frame.Buffer[T]`, type-erased and unexported, and the pair table. `Step` still returns it; `ViewOf` asserts `*frame.Buffer[T]` and pins it at the view's own frame, so a view kept past the next `Step` still reads its frame |
| Universe names required and unique, feature → universe references, error paths | `config.validate`, `universePath` | Unchanged |
| `Spec` | `internal/engine/features` | `internal/protocol/feature`; `cmd` then imports no engine package |
| A subscription's required fields, derived windows coarser and a multiple | `config.validate` | `proto.Subscription.Validate` and `Complete`; duplicates stay in config |
| Protocol's description and design rule | `docs/Architecture.md` | The rule in § Decisions |

## Open Questions

None.

## WIP

- Steps 1 and 2 implemented: universes in `internal/protocol/universe.go`, so
  `internal/engine/features` no longer imports `config`; `Spec`, `FeaturesView` and `ViewOf` in
  `internal/protocol/feature`, so `cmd` imports no engine package. Step 3
  (`Subscription.Validate`) remains — `docs/plans/protocol-types.md`.

## Resources

- `internal/protocol/`, `internal/config/universe.go`, `internal/config/config.go`,
  `internal/engine/features/expand.go`, `cmd/instantiate.go`
- `docs/Architecture.md` § Design Rules, `docs/Protocol.md`
- Related: [[features]] (§ Feature layer, § Universes), [[config]], [[engine]] (composition root),
  [[strategy-layer]]
