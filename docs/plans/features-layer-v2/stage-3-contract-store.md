# Stage 3 — contract, registry and store (features layer v2, iteration 1)

## Context

[main.md](main.md) stage 3. The feature graph is settled at concept level
([features.md](../../discussions/features.md)), and `flow/frame` gives every layer the same rings and
pinned views. Nothing defines what a feature is on them: how a variant is built, how it reads its
candles and its dependencies' results at one coordinate, where its results live, and how a consumer
reads them.

This stage adds the contract in a new `internal/protocol/feature` package, the registry of feature
types beside `reg.Features`, and the feature store in a new `internal/engine/features` package. A
feature type registers its base ID, descriptor and factory with its value type; a variant binds at a
coordinate into a node that computes a value; the store recovers each variant's value type through
a switch naming the types it supports, allocates one `frame.Buffer[T]` per composed ID, binds every
node, and writes what each node returns into its own ring. Buffers, rings and the writes to them
exist only in the store.

```
registration   feature.NewType[T](base, descriptor, factory) ─▶ reg.FeatureTypes   T recorded
config         reg.FeatureTypes.Create(type, opts) ─▶ feature.Variant              T erased
store          per composed ID: columnFor(variant.Zero()) ─▶ results[T]: frame.Buffer[T], readers' view
               per instance: Feature[T].Bind(binder) ─▶ FeatureNode[T] ─▶ node[T]: the node, its ring
each frame     pin(data view) ─▶ runner fire(col) / skip(col, state) ─▶ features.View ─▶ ViewOf[T]
```

Expansion (stage 5) and scheduling (stage 6) are not built: tests build instances by hand, pin the
store at frames, and fire or skip runners themselves. Nothing outside the new packages and the
registry changes.

The plan, with the decisions it records in [features.md](../../discussions/features.md) and
[main.md](main.md), is committed before step 1. The stage is then implemented in four steps, in
order. Each step is a review checkpoint: its changes stay unstaged for review, are committed once
approved, and the next step starts from that commit. Every step leaves the tree building, tested and
`make check` clean.

| Step | Adds | Commit |
|---|---|---|
| 1 | The contract: variants, types, the binder and its input handles | `feat(protocol): add the feature contract` |
| 2 | `reg.FeatureTypes` | `feat(registry): add the feature type registry` |
| 3 | Typed result buffers and their runners | `feat(features): add typed result buffers and their runners` |
| 4 | The store and its view; the stage's documentation | `feat(features): add the feature store and its view` |

## Decisions

- **The contract lives in `internal/protocol/feature`.** A subpackage keeps its names clear of
  `proto.Feature`, which stays unchanged beside it. `frame` imports nothing, and only
  `frame_test.go` imports `proto`, so the protocol root could have hosted it; the subpackage
  imports `frame` and `proto` with no cycle either way.
- **The contract holds no buffer, ring or result.** Feature authors use `Feature[T]`,
  `FeatureNode[T]`, `Descriptor`, `NewType[T]`, the binder and its `Input[T]` handles; the engine
  uses `Type` and `Variant` with `T` erased, `Coord` and `NewBinder`. `frame.NewBuffer`, the typed
  results, the runners that write them and the pinning live in `internal/engine/features`, so only
  the feature engine creates or holds feature buffers.
- **The contract is generic over the value type.** `Feature[T]` is a variant — `ID`, `Lookback`,
  `Bind(*Binder) (FeatureNode[T], error)`; `FeatureNode[T]` is a variant bound at one coordinate —
  `Compute() (T, bool)`. Two interfaces, so `Compute` is never called on an unbound variant by
  type; one struct may implement both, its `Bind` returning a bound copy. Scratch for
  allocation-free reads is allocated in `Bind`, never by the factory: a factory's slice would be
  shared by every bound copy.
- **The engine's switch is the set of value types.** `T` is unconstrained in the contract.
  `NewType[T]` records the base ID, descriptor and value type — `Zero()`, a zero `T` — and the
  registry hands out `feature.Variant`s with `T` erased. The store switches on `Zero()`, one case per
  supported type — `float64` and `int` today — and asserts the variant to `Feature[T]`: the only
  place `T` is turned back into code, since Go instantiates `frame.NewBuffer[T]` only where `T` is
  known. A struct value type (MACD, recursive state) is declared in its feature's domain package;
  the engine imports that package for its case, consumers for `ViewOf`. A constraint listing the
  types would put domain types in `protocol`, an import cycle, so a missing case is caught by a
  test over every registered type (stage 4) and by `Expand` (stage 5), not by the compiler.
  Registration through the engine was rejected: domain packages would import the engine.
- **Nodes never write.** `Compute` returns the value, or false when it cannot compute (warm-up); the
  store's runner wrapping the node writes `Fresh`, or `Gap` carrying the last `Fresh` value. `skip`
  writes `Gap` or `Ended` without computing, for columns the engine does not fire. A node holds no
  ring and no buffer: only its runner holds its one ring.
- **Inputs are handles narrowed to the coordinate.** `Input[T]` is a pointer to a view the store
  re-pins each frame and a ring index: `Latest()` and `Data(n, dst)` read that ring at the pinned
  frame, so a node cannot read another stream or a later column. The binder hands them out:
  `Candles` and `Dep[T]` for a single-stream node, `PairCandles` and `PairDep[T]` returning both
  ends for a pair node. A dependency is named by its feature type (`"rsi"`); the store resolves which
  variant. The binder holds each dependency's view as a `*frame.View[T]` behind `any`, so `Dep[T]`
  checks the type at bind without naming an engine type. Calling the wrong arity's function, naming
  a type the instance has no dependency on, or reading a dependency as the wrong `T` is a bind error.
- **Dependencies are single-stream in iteration 1.** A pair node reads a single-stream dependency on
  each end; a feature reading a pair feature's results is not defined by the discussion.
- **The descriptor is arity and dependencies.** Symmetry joins it with asymmetric pair order: every
  pair is unordered in iteration 1 ([main.md](main.md) § Out of scope).
- **A variant ID never contains `-`.** `Type.Create` rejects one, naming it: `-` separates composed
  IDs. `iDWithParam`'s `'g'` format (`ema_20_s1e-05`) is ported in stage 4 and caught here.
- **The registry is `reg.FeatureTypes`**, a type of its own beside the generic `registry[T]`:
  descriptors and value types must be readable without creating a variant. It holds `feature.Type`s
  by base ID; registering a base twice replaces it, as in `registry[T]`.
- **One typed buffer per composed ID**, not per variant ID: `zscore_50-rsi_14` and
  `zscore_50-rsi_21` are one variant on the same streams. A feature without dependencies has its
  variant ID as composed ID.
- **Buffers share the layers' index spaces.** A single-stream ID's buffer has one ring per stream of
  the stream table; a pair ID's, one ring per pair of the store's pair table — every pair any pair
  instance runs on, sorted, each `(a, b)` with `a < b`. Rings are empty where the ID does not run
  (~64 B each). Replaces the per-stream feature map of
  [features.md](../../discussions/features.md): a consumer resolves an ID once with `ViewOf` and a
  pair with `View.Pair`.
- **Rings hold one lookback.** `L` is the longest `Lookback` across instances, at least 1 — the
  lookback the data layer keeps. A stream's result ring is
  `{Slots: L × W, Period: P, Window: W, Bars: L}` at its stream's `P` and `W`, so its history steps
  one bar like the candles beneath it. No headroom: results are written only at the frame's column.
- **A pair's ring steps at its sampling period.** `Period = max(Pa, Pb)` — where both ends are due,
  on the timeframes' divisibility chain — and one bar is its coarser end's:
  `Window = max(Pa × Wa, Pb × Wb) / Period`. "Sized like its coarser end's" fails for a rolling 1h
  (P 1, W 12) paired with a direct 1h (P 12, W 1): at period 1 the ring anchors on columns the pair
  never writes.
- **Allocate, then bind; bind order is free.** Value types are recorded, so the store allocates every
  buffer, then binds every instance; a dependency need not be bound first. Firing order is stage 6's.
  The store checks what expansion will guarantee: each dependency's composed ID runs at the
  instance's coordinate (on each end, for a pair), an ID runs once per coordinate, and one ID has one
  variant.
- **The store's input and runners are internal.** `instance` and `runner` are unexported: from
  stage 5 `Expand` builds the instances inside a `Graph` that `features.New` consumes, and stage 6
  fires the runners in the same package; this stage's tests build and fire them in the package.
- **`features.View` embeds the data view**, so consumers read candles and the frame from it, and
  `ViewOf[T](fv, id)` returns a composed ID's results as a `frame.View[T]` pinned at its frame —
  unknown IDs and the wrong `T` are errors. A map lookup per call, allocation-free; resolved keys
  wait for a consumer that needs them.

## Step 1 — the contract

### Changes — new `internal/protocol/feature/feature.go`

```go
package feature

// Arity is the coordinate a feature runs at.
type Arity uint8

const (
	Single Arity = iota // one stream
	Pair                // a pair of streams, unordered
)

// Descriptor is a feature type's shape, read without building a variant.
type Descriptor struct {
	Arity        Arity
	Dependencies []string // feature types read, each once, in composition order; none reads candles
}

/*
Feature is a variant of a feature type with value type T, built by the type's factory from a feature
entry's options. Bind places it at one coordinate, taking its inputs from b. A variant may be its own
node: Bind then returns a bound copy, its scratch allocated there.
*/
type Feature[T any] interface {
	ID() string    // variant ID: the base ID and its parameters; never contains '-'
	Lookback() int // bars of its inputs it reads: candles, or its dependencies' results
	Bind(b *Binder) (FeatureNode[T], error)
}

// FeatureNode is a variant bound at one coordinate. It keeps no value between frames.
type FeatureNode[T any] interface {
	// Compute returns the value at the frame its inputs are pinned at, or false when it cannot
	// compute.
	Compute() (T, bool)
}

// Factory builds a variant from a feature entry's options.
type Factory[T any] func(opts map[string]any) (Feature[T], error)

// Type is a registered feature type: its base ID, descriptor and factory, its value type erased.
type Type struct {
	Base       string
	Descriptor Descriptor
	zero       any // a zero T
	create     func(opts map[string]any) (Variant, error)
}

func NewType[T any](base string, d Descriptor, f Factory[T]) Type

// Zero is a zero value of the type's T: what the feature engine switches on.
func (t Type) Zero() any

// Create builds a variant from opts; a variant ID containing '-' is an error.
func (t Type) Create(opts map[string]any) (Variant, error)

/*
Variant is a variant with its value type erased: a Feature[T] whose T is Zero's type, which the
feature engine recovers with a type switch.
*/
type Variant interface {
	ID() string
	Lookback() int
	Type() string // base ID
	Descriptor() Descriptor
	Zero() any
}
```

`Create` returns a `*variant[T]` embedding the `Feature[T]` with the base ID, descriptor and zero
value, so it is both a `Variant` and a `Feature[T]`.

### Changes — new `internal/protocol/feature/bind.go`

```go
// Coord is where a node runs: stream A, or the pair (A, B), A < B.
type Coord struct {
	Pair bool
	A, B int
}

// Binder is a node's coordinate at Bind, with its candles and dependencies there.
type Binder struct {
	at      Coord
	candles *frame.View[proto.Candle]
	deps    map[string]any // by dependency type: the *frame.View[T] its results are read through
}

/*
NewBinder places a variant at at. candles and each dependency's view are re-pinned by the engine
each frame; deps maps a dependency type to a *frame.View[T] of its value type.
*/
func NewBinder(at Coord, candles *frame.View[proto.Candle], deps map[string]any) *Binder

// Input is one ring at a node's coordinate, read at the frame the engine pinned.
type Input[T any] struct {
	v *frame.View[T]
	i int
}

func (in Input[T]) Latest() (T, frame.State, int)
func (in Input[T]) Data(n int, dst []T) []T

func Candles(b *Binder) (Input[proto.Candle], error)                        // single-stream nodes
func PairCandles(b *Binder) (Input[proto.Candle], Input[proto.Candle], error) // pair nodes: A, B
func Dep[T any](b *Binder, typ string) (Input[T], error)
func PairDep[T any](b *Binder, typ string) (Input[T], Input[T], error)
```

Errors name what failed: `feature: dependency "rsi" is not float64`, `feature: Candles on a pair
node`, `feature: no dependency of type "rsi"`.

### Tests

**New `internal/protocol/feature/feature_test.go`** — test features over `float64` and `int`:

- **`TestTypeCreate`** — the variant reports its ID, `Lookback`, base ID and descriptor, and is a
  `Feature[T]` of its type's `T`; `Zero` on the type and the variant is `T`'s zero value; a factory
  error is returned; an ID containing `-` is an error naming it.

**New `internal/protocol/feature/bind_test.go`** — views over `frame` buffers built by the test,
re-pinned with `frame.NewView` and `frame.Pin`:

- **`TestBindInputs`** — `Candles` and `Dep[T]` read ring `A` of their view; `PairCandles` and
  `PairDep[T]` read rings `A` and `B`; reads follow the view as it is re-pinned.
- **`TestBindErrors`** — `Candles` / `Dep` on a pair binder, `PairCandles` / `PairDep` on a single
  one, an undeclared dependency type, and a dependency read as the wrong `T` are errors.
- **`TestInputAllocations`** — `Latest` and `Data` with `dst` capacity allocate nothing.

### Documentation

- `docs/Architecture.md` — `internal/protocol`: the feature contract in `protocol/feature`.
- `docs/Commands.md` — the `make test` row lists `internal/protocol/feature`.

### Acceptance

- `go test -race ./internal/protocol/feature/` passes; `make check` clean; nothing outside the new
  package changes.

## Step 2 — the feature type registry

### Changes — `internal/registry/registry.go`

```go
// FeatureTypes holds the feature layer's types by base ID.
FeatureTypes = &featureTypes{types: make(map[string]feature.Type)}

func (r *featureTypes) Register(t feature.Type)
func (r *featureTypes) Create(base string, opts map[string]any) (feature.Variant, error)
func (r *featureTypes) Type(base string) (feature.Type, bool) // descriptor and value type, no variant
func (r *featureTypes) Has(base string) bool
func (r *featureTypes) List() []string
```

`Create` on an unknown base is `unknown component: <base>`, as `registry[T]`'s. Nothing registers
yet: stage 4 ports the features.

### Tests — new `internal/registry/registry_test.go`

- **`TestFeatureTypes`** — on a registry of its own: `Register`, `Has`, `Type`, `Create`, `List`; an
  unknown base is an error; `Create` returns the type's variant, or `Type.Create`'s error.

### Documentation

- `docs/Architecture.md` — `internal/registry`: feature types with descriptors and value types
  (`FeatureTypes`).
- `docs/Commands.md` — the `make test` row lists `internal/registry`.

### Acceptance

- `go test -race ./internal/registry/` passes; `make check` clean.

## Step 3 — typed result buffers and their runners

### Changes — new `internal/engine/features/results.go`

```go
package features

// results is one composed ID's results: its variant, a buffer of its value type, and the view its
// readers read, re-pinned each frame.
type results[T any] struct {
	f    feature.Feature[T]
	buf  *frame.Buffer[T]
	view frame.View[T]
}

// column is a results with its value type erased: what the store holds per composed ID.
type column interface {
	pin(v frame.View[proto.Candle])
	cell() any                                        // &view, the *frame.View[T] dependents bind to
	bind(b *feature.Binder, ring int) (runner, error) // the variant at b's coordinate, writing ring
}

/*
columnFor returns the column constructor for zero's type: one case per supported value type. A
struct value type adds a case, importing its feature's domain package.
*/
func columnFor(zero any) (func(v feature.Variant, shapes []frame.Shape) (column, error), bool) {
	switch zero.(type) {
	case float64:
		return newResults[float64], true
	case int:
		return newResults[int], true
	}
	return nil, false
}

// newColumn allocates v's buffer, one ring per shape; a variant of an unsupported value type is an
// error.
func newColumn(v feature.Variant, shapes []frame.Shape) (column, error) {
	newCol, ok := columnFor(v.Zero())
	if !ok {
		return nil, fmt.Errorf("features: %s: unsupported value type %T", v.ID(), v.Zero())
	}
	return newCol(v, shapes)
}

// runner is a bound node, fired or skipped at each due column of its coordinate.
type runner interface {
	fire(col int64)                // Compute; writes Fresh, or Gap when it cannot compute
	skip(col int64, s frame.State) // writes s, Gap or Ended, carrying the last Fresh value
}
```

- `newResults[T]` asserts the variant to `feature.Feature[T]` — `NewType[T]` pairs `Zero` with it, so
  a failure is an error, not a panic — and calls `frame.NewBuffer[T](shapes)`; `pin` sets `view`
  with `frame.Pin`.
- `columnFor` is apart from `newColumn` so a type's value type can be checked without building a
  variant: stage 4 checks every registered type's `Zero` against it.
- `results[T].bind` calls the variant's `Bind` and returns a `node[T]` holding the `FeatureNode[T]`
  and ring `ring` of the buffer.
- `node[T].fire` writes `Put(col, v, Fresh)`, or `Put(col, zero, Gap)` when `Compute` returns false;
  `skip` writes `Put(col, zero, s)`. `frame.Ring.Put` carries the last `Fresh` value.

### Tests

**New `internal/engine/features/features_test.go`** — test-only features, created through
`feature.NewType` and `Type.Create` as the registry does: `last` (its stream's close, `Lookback` 1),
`sum` (its last `n` closes, warm-up below `n`), `diff` (over `last`: its last two values), `sign`
(`int`) and `label` (`string`, unsupported); and helpers building a stream table — BTCUSDT 5m deriving
1h, BTCUSDT 1h and ETHUSDT 5m — candles in a `frame.Buffer[proto.Candle]` written by the test, and
data views pinned with `frame.NewView`.

**New `internal/engine/features/results_test.go`** — columns built with `newColumn` over hand-made
shapes, bound with `feature.NewBinder`:

- **`TestColumnValueTypes`** — `columnFor` supports `float64` and `int`, not `string`; a column of
  each supported type allocates its shapes and reads back its own type; `newColumn` on `label` is an
  error naming its ID and type.
- **`TestColumnBind`** — a bound `last` reads its stream's candle at the pinned frame; `diff`, bound
  to `last`'s `cell()`, reads `last`'s result at the same column after `last` fires; re-pinning moves
  both to the next frame.
- **`TestRunnerFire`** — `fire` writes `Fresh`; `sum` warming up writes `Gap` with no value
  (`ValCol` −1), and after its first value `Gap` carries it.
- **`TestRunnerSkip`** — `skip(Gap)` carries the last value and its column; `skip(Ended)` carries it
  and ends the ring: reads past the end return the `Ended` slot.
- **`TestColumnAllocations`** — `pin`, `fire` and `skip` allocate nothing.

### Documentation

- `docs/Commands.md` — the `make test` row lists `internal/engine/features`.

### Acceptance

- `go test -race ./internal/engine/features/` passes; `make check` clean.

## Step 4 — the store and its view

### Changes — new `internal/engine/features/store.go`

```go
// instance is a variant at one coordinate: its composed ID, and the composed ID feeding each of its
// dependencies there — on each end, for a pair.
type instance struct {
	variant feature.Variant
	id      string
	at      feature.Coord
	deps    map[string]string // dependency type → composed ID
}

/*
store holds every result: one buffer per composed ID, of its variant's value type, with one ring per
stream of the table, or per pair of the pair table, empty where the ID does not run. Readers' views
are re-pinned each frame; each runner writes only its own ring.
*/
type store struct {
	columns map[string]column        // by composed ID
	all     []column                 // pinned each frame
	pairs   map[[2]int]int           // the pair table: (a, b), a < b → ring
	candles frame.View[proto.Candle] // the candles nodes read, re-pinned each frame
}

/*
newStore builds the pair table from the pair instances, sizes and allocates one buffer per composed
ID, and binds every instance; runners follow instances' order. Rings hold L bars, L the longest
Lookback, at least 1: a stream's at its Period and Window, a pair's at max(Pa, Pb) with its coarser
end's bar. Errors name the composed ID and coordinate.
*/
func newStore(streams *proto.Streams, instances []instance) (*store, []runner, error)

// pin re-pins the candles and every result at v's frame, before any runner fires at it.
func (s *store) pin(v frame.View[proto.Candle]) View
```

`newStore` checks, before allocating: a pair's ends are two streams of the table, lower first; one
composed ID runs once per coordinate and has one variant (variants compare as interface values);
every dependency's composed ID runs at the instance's stream, or on both ends of its pair. Each
binder gets `&s.candles` and its dependencies' `cell()`s; each instance binds with ring `A` of its
column, or its pair's index. Errors are wrapped with the composed ID and coordinate:
`features: zscore_50-rsi_14 on BTCUSDT:1h:btc_1h: feature: no dependency of type "rsi"`.

### Changes — new `internal/engine/features/view.go`

```go
// View is the feature layer at one frame: the data view, and every result pinned at its column.
// Valid until the next pin.
type View struct {
	frame.View[proto.Candle]
	s *store
}

// ViewOf returns composed ID id's results at v's frame: entry i is stream i, or pair i of the pair
// table. An unknown ID, or a value type other than T, is an error.
func ViewOf[T any](v View, id string) (frame.View[T], error)

// Pair returns the pair (a, b)'s entry in pair results, in either order.
func (v View) Pair(a, b int) (int, bool)
```

`ViewOf` asserts the column to `*results[T]` and pins its buffer at `v`'s frame.

### Tests

**`internal/engine/features/features_test.go`** — adds `spread` (pair: close A − close B) and
`pdiff` (pair over `last`).

**New `internal/engine/features/store_test.go`**

- **`TestStoreFrame`** — `last` and `diff` on every stream: `diff` fired after `last` reads its
  result at the same column; `ViewOf` reads both, and no later column; `sign` reads back as `int`.
- **`TestStoreGap`** — after `skip(Gap)` on `last`, `diff` sees `Gap` in `Latest` and skips the
  column in `Data`.
- **`TestStoreHistory`** — result history steps one bar: the direct 1h on the 5m grid (period 12)
  and the rolling 1h from 5m (period 1, window 12); `Gap` skipped.
- **`TestStorePairs`** — one pair table, sorted, shared by every pair ID; `Pair` in either order; a
  pair result is one ring; `spread` and `pdiff` read both ends; shapes: rolling 1h × direct 1h →
  period 12, window 1; rolling 1h × ETHUSDT 5m → period 1, window 12.
- **`TestStoreShapes`** — rings empty where an ID does not run; a stream's ring is
  `{L × W, P, W, L}` with `L` the longest `Lookback`; `L` is 1 without lookback.
- **`TestStoreTypes`** — `diff` over `sign` (`Dep[float64]` over `int`) fails at bind, naming the
  composed ID, coordinate and dependency; `label` fails allocation; `ViewOf` with the wrong `T`, or
  an unknown ID, is an error.
- **`TestStoreErrors`** — an ID twice on one coordinate; a dependency not running at the stream, or
  on one end of a pair; two variants under one ID; a pair's ends equal, reversed or out of range;
  each named by ID and coordinate.
- **`TestStoreAllocations`** — `pin`, `Input` reads with `dst` capacity and `ViewOf` allocate
  nothing.

### Documentation

- [features.md](../../discussions/features.md) — WIP: the contract, registry and store implemented;
  Solution references this stage.
- [main.md](main.md) — stage 3 marked implemented.

### Acceptance

- `go test -race ./internal/protocol/feature/ ./internal/registry/ ./internal/engine/features/`
  passes; `make check` clean.

## Documentation recorded at planning

Committed with this plan, before step 1:

- [features.md](../../discussions/features.md) — the contract settled (§ Feature graph, Solution
  § Contract sketch); value types named by the engine's switch, one typed buffer per composed ID in
  the stream and pair tables, the pair ring's period (§ Feature buffer); expansion in the engine's
  construction and the store in its run (§ Feature layer, § Reversed on 2026-11-03); slots written
  `Gap` when a node cannot compute or is skipped.
- [main.md](main.md) — the decisions, stages 3 to 7, and the open items this stage resolves.

## Out of scope

- Expansion, the `Graph`, `Spec` and injection from the composition root — stage 5.
- `features.New`, firing rules, schedules, `Step` and the memory warning — stage 6.
- The ported features — stage 4.
- Value types beyond `float64` and `int` — a case each, with the first feature that needs one.
- The test of every registered type against `columnFor` — stage 4, once types are registered.
- Reading a node's own past results (recursive features) — iteration 4: `Latest` at the column
  being written anchors on the slot about to be written, so it needs a read of the previous update.
- Pair features over pair features; asymmetric pair order and symmetry in the descriptor.
- `data_logger_v2`, `data_logger`, `proto.Feature` and `reg.Features`.

## Verification

```bash
# step 1
go test -race ./internal/protocol/feature/
# step 2
go test -race ./internal/registry/
# step 3
go test -race ./internal/engine/features/
# step 4
go test -race ./internal/protocol/feature/ ./internal/registry/ ./internal/engine/features/
# every step
make check
```
