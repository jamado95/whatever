# Stage 3 — contract, registry and store (features layer v2, iteration 1)

## Context

[main.md](main.md) stage 3. The feature graph is settled at concept level
([features.md](../../discussions/features.md)), and the `frame` package gives every layer the same
rings and pinned views. Nothing defines what a feature is on them: how it is configured, how it reads
its candles and its dependencies' results at one coordinate, where its results live, and how a
consumer reads them.

This stage moves `frame` into the protocol layer, and adds the contract in a new
`internal/protocol/feature` package, the feature types' registry beside `reg.Features`, and the
graph, engine and view in a new `internal/engine/features` package. A factory returns one
`feature.Feature`; its `Bind` places it at a coordinate and returns the node, a function computing a
value. The engine recovers the value type by switching on `Bind`, binds every node before any ring
exists, so `--validate-only` covers binding, allocates one `frame.Buffer[T]` per composed ID when it
runs, and writes what each node returns into its own ring. Buffers, rings and the writes to them
exist only in the engine.

```
registration   reg.FeatureTypes.Register(base, factory)
config         reg.FeatureTypes.Create(type, opts) ─▶ feature.Feature       T held by its Bind
factory        newGraph: per composed ID, newColumn(feature, shapes) ─▶ results[T], no rings
                         per instance, column.bind ─▶ feature's Bind ─▶ node: func() (T, bool)
run            New(graph): every column allocates its frame.Buffer[T]
each frame     pin(data view) ─▶ fire(ring, col) / skip(ring, col, state) ─▶ features.View ─▶ ViewOf[T]
```

Expansion (stage 5) and scheduling (stage 6) are not built: tests build instances by hand, call
`newGraph` — the last part of stage 5's `Expand` — pin the engine at frames, and fire or skip columns
themselves. Outside the new packages, only `frame`'s path and the registry change.

The plan, with the decisions it records in [features.md](../../discussions/features.md),
[main.md](main.md) and the other discussion docs, is committed before step 1. The stage is then
implemented in four steps, in order. Each step is a review checkpoint: its changes stay unstaged for
review, are committed once approved, and the next step starts from that commit. Every step leaves
the tree building, tested and `make check` clean.

| Step | Adds | Commit |
|---|---|---|
| 1 | `frame` in the protocol layer | `refactor(protocol): move frame into the protocol layer` |
| 2 | The contract and `reg.FeatureTypes` | `feat(protocol): add the feature contract and its registry` |
| 3 | Typed result columns | `feat(features): add typed result columns` |
| 4 | The graph, engine and view; the stage's documentation | `feat(features): add the feature graph, engine and view` |

## Decisions

- **Views are part of the protocol layer.** `View` is what layers hand each other and consumers, so
  `frame` moves from `internal/flow/frame` to `internal/protocol/frame`, reversing stage 1's
  placement. `protocol` already holds `SortedWindow` and `Snapshot`, their v1 equivalents.
- **`protocol` imports nothing outside itself.** `internal/protocol` and its subpackages import only
  each other. The root imports neither subpackage: `feature` imports the root, and `frame_test.go`
  imports the root, so either import is a cycle.
- **One feature type.** A factory returns a `feature.Feature`, a struct: `ID`, `Lookback`, `Pair`,
  `Deps`, `Bind`. No registered type, erased variant or node type sits beside it.
- **A node is the function `Bind` returns.** `Bind` is a `feature.Bind[T]`: it takes the node's inputs
  at one coordinate from the binder, allocates its scratch and returns `func() (T, bool)`, the value
  at the frame its inputs are pinned at, or false when it cannot compute (warm-up). `Bind` runs per
  instance, so scratch is per node and iteration 2 can fire one feature's nodes in parallel; a
  factory's scratch would be shared. Long features write `Bind: r.bind` rather than nesting.
- **The engine's switch is the set of value types.** `feature.Bind[T]` is an alias of a plain function
  type, so `case feature.Bind[float64]:` matches a literal, a method value and a named function, and
  hands the switch the typed function directly: the only place `T` is turned back into code, since Go
  instantiates `frame.NewBuffer[T]` only where `T` is known. `float64` and `int` today. A struct value
  type (MACD, recursive state) is declared in its feature's domain package; the engine imports that
  package for its case, consumers for `ViewOf`.
- **`Bind` is `any`, so the compiler does not check it.** A value type with no case, a wrong signature
  and a missing `Bind` all reach the switch's default: an error naming the feature and the type found,
  reported by `Expand` (stage 5) and caught by a test over every registered type (stage 4). A
  compile-time list of the types would put domain types in `protocol`, an import cycle. Registration
  through the engine was rejected: domain packages would import the engine.
- **Arity and dependencies are fields, read without binding.** They live on each variant, not on a
  type: variants of one type must declare the same `Pair` and `Deps`, which `Expand` checks (stage 5),
  since composed IDs rely on it. Symmetry joins `Feature` with asymmetric pair order: every pair is
  unordered in iteration 1 ([main.md](main.md) § Out of scope).
- **The binder keeps its first error; `Bind` returns none.** An input call that fails records its
  error and returns an `Input` with no view; the engine reads `Err` after `Bind` and discards the
  node. `Bind` therefore never reads an input, and has nothing else that can fail: options are
  validated by the factory. Wrong arity, a type the instance has no dependency on and a dependency
  read as the wrong `T` are all bind errors.
- **Nodes never write.** The engine writes what a node returns: `Fresh`, or `Gap` carrying the last
  `Fresh` value when it returns false. `skip` writes `Gap` or `Ended` without calling it, for columns
  the engine does not fire. A node holds no ring and no buffer.
- **Inputs are handles narrowed to the coordinate.** `Input[T]` is a pointer to a view the engine
  re-pins each frame and a ring index: `Latest()` and `Data(n, dst)` read that ring at the pinned
  frame, so a node cannot read another stream or a later column. The binder hands them out: `Candles`
  and `Dep[T]` for a single-stream node, `PairCandles` and `PairDep[T]` returning both ends for a pair
  node. A dependency is named by its feature type (`"rsi"`); expansion resolves which variant. The
  binder holds each dependency's view as a `*frame.View[T]` behind `any`, so `Dep[T]` checks the type
  without naming an engine type.
- **Dependencies are single-stream in iteration 1.** A pair node reads a single-stream dependency on
  each end; a feature reading a pair feature's results is not defined by the discussion.
- **A variant ID never contains `-`.** `-` separates composed IDs, so `Expand` rejects one, naming it
  (stage 5). `iDWithParam`'s `'g'` format (`ema_20_s1e-05`) is fixed when it is ported (stage 4).
- **The registry is `reg.FeatureTypes`, a `registry[feature.Feature]`:** the existing generic registry,
  beside `reg.Features`. A type's base ID is the name it registers under; it reaches expansion through
  the config entry's type (stage 5's `Spec`).
- **One typed buffer per composed ID**, not per variant ID: `zscore_50-rsi_14` and
  `zscore_50-rsi_21` are one variant on the same streams. A feature without dependencies has its
  variant ID as composed ID.
- **Buffers share the layers' index spaces.** A single-stream ID's buffer has one ring per stream of
  the stream table; a pair ID's, one ring per pair of the graph's pair table — every pair any pair
  instance runs on, sorted, each `(a, b)` with `a < b`. Rings are empty where the ID does not run
  (~64 B each). Stream 3 is ring 3 in every candle and result buffer, so an input needs no lookup.
- **Rings hold one lookback.** `L` is the longest `Lookback` across instances, at least 1 — the
  lookback the data layer keeps. A stream's result ring is
  `{Slots: L × W, Period: P, Window: W, Bars: L}` at its stream's `P` and `W`, so its history steps
  one bar like the candles beneath it. No headroom: results are written only at the frame's column.
- **A pair's ring steps at its sampling period.** `Period = max(Pa, Pb)` — where both ends are due,
  on the timeframes' divisibility chain — and one bar is its coarser end's:
  `Window = max(Pa × Wa, Pb × Wb) / Period`. "Sized like its coarser end's" fails for a rolling 1h
  (P 1, W 12) paired with a direct 1h (P 12, W 1): at period 1 the ring anchors on columns the pair
  never writes.
- **`Bind` runs once, before any ring exists.** A node needs the addresses of its inputs' views, not
  rings: `Input{&results.view, i}` reaches the rings only through what `pin` copies into that view each
  frame. `newGraph` creates every column with no buffer, then binds every instance in any order, and
  `New` only allocates, so nothing is rebound. `--validate-only` stops after the factory, so it covers
  every `Bind`, and every problem is reported in one pass: `newGraph` and `Expand` return
  `errors.Join`, and `reportErrors` splits it.
- **A `Graph` runs once.** It holds bound nodes and the views they read, so each `New` needs its own
  `Expand`. Binding allocates each node's scratch in the factory, so a validate-only run pays for it:
  59 MB in a prototype at 1000 streams × 5 variants × a lookback of ~200, against 31 MB of `float64`
  rings. A real run needs it anyway.
- **`Engine` holds the run; there is no separate store.** It allocates and pins here; stage 6 adds
  schedules, `Step` and the memory warning to it.
- **`newGraph` checks what expansion will guarantee:** each dependency's composed ID runs at the
  instance's coordinate (on each end, for a pair), an ID runs once per coordinate, and one ID has one
  variant ID.
- **The graph's input is internal.** `instance` and `column` are unexported: from stage 5 `Expand`
  builds the instances, and stage 6 fires the columns, in the same package; this stage's tests build
  and fire them in the package.
- **`features.View` embeds the data view**, so consumers read candles and the frame from it, and
  `ViewOf[T](fv, id)` returns a composed ID's results as a `frame.View[T]` pinned at its frame —
  unknown IDs and the wrong `T` are errors. A map lookup per call, allocation-free; resolved keys
  wait for a consumer that needs them.
- **A domain never imports `engine/features`.** That is a cycle once the switch imports that domain
  for a struct value type, as is a domain's internal test doing so; an external `package x_test` is
  fine. No domain is imported in this stage: the switch has no struct case yet.

## Step 1 — `frame` in the protocol layer

### Changes

- `git mv internal/flow/frame internal/protocol/frame`; package name and API unchanged.
- The import path, in its importers: `internal/engine/data/` (`aggregate.go`, `align.go`, `buffer.go`,
  `data.go`, `event.go`, `readiness.go` and the tests `aggregate_test.go`, `align_test.go`,
  `buffer_test.go`, `data_test.go`, `lane_test.go`, `readiness_test.go`) and
  `internal/engine/data_logger_v2.go`.

No behaviour changes, so no new tests.

### Documentation

- `docs/Architecture.md` — `internal/flow` loses `frame`; `internal/protocol` gains it.
- `docs/Commands.md` — the `make test` row lists `internal/protocol/frame`, not `internal/flow/frame`.

### Acceptance

- `go test -race ./internal/protocol/frame/ ./internal/engine/...` passes; `make check` clean.
- Nothing under `internal/protocol` imports outside it: the invariant command in § Verification prints
  nothing.

## Step 2 — the contract and its registry

### Changes — new `internal/protocol/feature/feature.go`

```go
package feature

/*
Feature is a feature as its factory configured it: what the engine needs to place it, and how to bind
it at a coordinate.
*/
type Feature struct {
	ID       string   // variant ID: the base ID and its parameters; never contains '-'
	Lookback int      // bars of its inputs it reads: candles, or its dependencies' results
	Pair     bool     // runs on a pair of streams, unordered; otherwise on one stream
	Deps     []string // feature types read, each once, in composition order; none reads candles
	Bind     any      // a Bind[T], T its value type: what the feature engine switches on
}

/*
Bind places a feature at one coordinate: it takes its inputs from b, allocates its scratch and returns
its node, which returns the value at the frame its inputs are pinned at, or false when it cannot
compute. Bind never reads an input: a failed one has no view.
*/
type Bind[T any] = func(b *Binder) func() (T, bool)

/*
Binder is a node's coordinate at Bind, with its candles and dependencies there. It keeps its first
error: the engine reads Err after Bind and discards the node.
*/
type Binder struct {
	at      [2]int // a stream, or a pair; the second is -1 on a stream
	candles *frame.View[proto.Candle]
	deps    map[string]any // by dependency type: the *frame.View[T] its results are read through
	err     error
}

/*
NewBinder places a feature at at. candles and each dependency's view are re-pinned by the engine each
frame; deps maps a dependency type to a *frame.View[T] of its value type.
*/
func NewBinder(at [2]int, candles *frame.View[proto.Candle], deps map[string]any) *Binder

func (b *Binder) Err() error

// Input is one ring at a node's coordinate, read at the frame the engine pinned.
type Input[T any] struct {
	v *frame.View[T]
	i int
}

func (in Input[T]) Latest() (T, frame.State, int)
func (in Input[T]) Data(n int, dst []T) []T

func Candles(b *Binder) Input[proto.Candle]                                  // single-stream nodes
func PairCandles(b *Binder) (Input[proto.Candle], Input[proto.Candle])       // pair nodes: A, B
func Dep[T any](b *Binder, typ string) Input[T]
func PairDep[T any](b *Binder, typ string) (Input[T], Input[T])
```

Errors name what failed: `feature: Candles on a pair node`, `feature: PairCandles on a single-stream
node`, `feature: Dep "rsi" on a pair node`, `feature: PairDep "rsi" on a single-stream node`,
`feature: no dependency of type "rsi"`, `feature: dependency "rsi" is not float64`.

A feature, for reference (stage 4 ports them):

```go
func newRSI(opts map[string]any) (feature.Feature, error) {
	cfg := PeriodConfig{}
	if err := config.Decode(opts, &cfg); err != nil {
		return feature.Feature{}, err
	}
	n := cfg.Period + 1
	return feature.Feature{
		ID:       iDWithPeriod(RSIID, cfg.Period),
		Lookback: n,
		Bind: func(b *feature.Binder) func() (float64, bool) {
			in := feature.Candles(b)
			candles := make([]proto.Candle, 0, n)
			return func() (float64, bool) {
				candles = in.Data(n, candles)
				if len(candles) < n {
					return 0, false
				}
				return rsi(candles), true
			}
		},
	}, nil
}
```

### Changes — `internal/registry/registry.go`

```go
FeatureTypes = &registry[feature.Feature]{
	factories: make(map[string]Factory[feature.Feature]),
}
```

Nothing registers yet: stage 4 ports the features. The generic registry's behaviour is unchanged, so
this adds no registry test.

### Tests — new `internal/protocol/feature/feature_test.go`

Views over `frame` buffers built by the test, re-pinned with `frame.NewView` and `frame.Pin`:

- **`TestBindInputs`** — `Candles` and `Dep[T]` read ring `A` of their view; `PairCandles` and
  `PairDep[T]` read rings `A` and `B`; inputs taken before the first pin read at each frame as the
  views are re-pinned.
- **`TestBindErrors`** — `Candles` / `Dep` on a pair binder, `PairCandles` / `PairDep` on a single
  one, an undeclared dependency type, and a dependency read as the wrong `T` each set `Err` to their
  message; a binder keeps its first error; a clean binder's `Err` is nil.
- **`TestInputAllocations`** — `Latest` and `Data` with `dst` capacity allocate nothing.

### Documentation

- `docs/Architecture.md` — `internal/protocol`: the feature contract in `protocol/feature`;
  `internal/registry`: `FeatureTypes`.
- `docs/Commands.md` — the `make test` row lists `internal/protocol/feature`.

### Acceptance

- `go test -race ./internal/protocol/feature/` passes; the invariant command prints nothing;
  `make check` clean.

## Step 3 — typed result columns

### Changes — new `internal/engine/features/results.go`

```go
package features

// column is one composed ID's results with their value type erased: what the graph holds.
type column interface {
	cell() any                               // &view, the *frame.View[T] dependents bind to
	bind(b *feature.Binder, ring int) error  // the feature at b's coordinate, its node writing ring
	alloc()                                  // the buffer, one ring per shape; New calls it once
	pin(v frame.View[proto.Candle])
	fire(ring int, col int64)                // the node; writes Fresh, or Gap when it cannot compute
	skip(ring int, col int64, s frame.State) // writes s, Gap or Ended, carrying the last Fresh value
}

// results is one composed ID's results: its buffer, its readers' view and the node of each ring.
type results[T any] struct {
	newNode feature.Bind[T]
	shapes  []frame.Shape
	buf     *frame.Buffer[T]   // nil until alloc
	view    frame.View[T]      // re-pinned each frame; nodes hold its address from Bind
	nodes   []func() (T, bool) // by ring; nil where the ID does not run
}

/*
newColumn is the engine's switch: one case per supported value type. A struct value type adds a case,
importing its feature's domain package. It allocates no ring, so stage 4 can check any feature with it.
*/
func newColumn(f feature.Feature, shapes []frame.Shape) (column, error) {
	switch bind := f.Bind.(type) {
	case feature.Bind[float64]:
		return newResults(bind, shapes), nil
	case feature.Bind[int]:
		return newResults(bind, shapes), nil
	}
	return nil, fmt.Errorf("features: %s: unsupported Bind %T", f.ID, f.Bind)
}
```

- `results[T].bind` calls `newNode(b)`, returns `b.Err()` if set, keeping no node, and otherwise
  stores the node at `ring`.
- `alloc` calls `frame.NewBuffer[T](shapes)`; `pin` sets `view` with `frame.Pin`.
- `fire` writes `Put(col, v, Fresh)`, or `skip(ring, col, Gap)` when the node returns false; `skip`
  writes `Put(col, zero, s)`. `frame.Ring.Put` carries the last `Fresh` value.

### Tests

**New `internal/engine/features/features_test.go`** — test-only features, as factories return them:
`last` (its stream's close, `Lookback` 1), `sum` (its last `n` closes, warm-up below `n`), `diff` (over
`last`: its last two values), `sign` (`int`) and `label` (`string`, unsupported); and helpers building
a stream table — BTCUSDT 5m deriving 1h, BTCUSDT 1h and ETHUSDT 5m — candles in a
`frame.Buffer[proto.Candle]` written by the test, and data views pinned with `frame.NewView`.

**New `internal/engine/features/results_test.go`** — columns built with `newColumn` over hand-made
shapes, bound with `feature.NewBinder`:

- **`TestColumnValueTypes`** — `Bind`s over `float64` and `int`, each written as a literal, a method
  value and a named function, reach their case and read back their own type; `label`, a wrong
  signature and a nil `Bind` are errors naming the feature and the type found.
- **`TestColumnBind`** — `last` and `diff` bind before `alloc`, `diff` to `last`'s `cell()`; after
  `alloc` and `pin`, `last` reads its stream's candle at the pinned frame, `diff` reads `last`'s result
  at the same column after `last` fires, and re-pinning moves both to the next frame. A failed bind
  returns the binder's error and keeps no node.
- **`TestColumnFire`** — `fire` writes `Fresh`; `sum` warming up writes `Gap` with no value (`ValCol`
  −1), and after its first value `Gap` carries it.
- **`TestColumnSkip`** — `skip(Gap)` carries the last value and its column; `skip(Ended)` carries it
  and ends the ring: reads past the end return the `Ended` slot.
- **`TestColumnAllocations`** — `pin`, `fire` and `skip` allocate nothing.

### Documentation

- `docs/Commands.md` — the `make test` row lists `internal/engine/features`.

### Acceptance

- `go test -race ./internal/engine/features/` passes; `make check` clean.

## Step 4 — the graph, engine and view

### Changes — new `internal/engine/features/graph.go`

```go
// instance is a feature at one coordinate: its composed ID, and the composed ID feeding each of its
// dependencies there — on each end, for a pair.
type instance struct {
	feature feature.Feature
	id      string
	at      [2]int            // a stream, or a pair a < b; the second is -1 on a stream
	deps    map[string]string // dependency type → composed ID
}

/*
Graph is a validated feature graph: one column per composed ID with every node bound, and no ring
allocated. The engine's factory builds it, so --validate-only covers it. It runs once: its nodes hold
the addresses of its views.
*/
type Graph struct {
	columns map[string]column        // by composed ID
	all     []column                 // pinned each frame
	pairs   map[[2]int]int           // the pair table: (a, b), a < b → ring
	candles frame.View[proto.Candle] // the candles nodes read, re-pinned each frame
}

/*
newGraph builds the pair table from the pair instances, sizes one column per composed ID and binds
every instance. Rings hold L bars, L the longest Lookback, at least 1: a stream's at its Period and
Window, a pair's at max(Pa, Pb) with its coarser end's bar. Every problem is reported, joined, each
naming the composed ID and coordinate.
*/
func newGraph(streams *proto.Streams, instances []instance) (*Graph, error)
```

`newGraph` checks, before binding: a pair's ends are two streams of the table, lower first; one
composed ID runs once per coordinate and has one variant ID; every dependency's composed ID runs at
the instance's stream, or on both ends of its pair. Each binder gets `&g.candles` and its dependencies'
`cell()`s; each instance binds with ring `A` of its column, or its pair's index. Errors are wrapped
with the composed ID and coordinate: `features: zscore_50-rsi_14 on BTCUSDT:1h:btc_1h: feature: no
dependency of type "rsi"`.

### Changes — new `internal/engine/features/engine.go`

```go
// Engine is one run of a Graph.
type Engine struct {
	g *Graph
}

// New allocates every column's buffer.
func New(g *Graph) *Engine

// pin re-pins the candles and every result at v's frame, before any node fires at it.
func (e *Engine) pin(v frame.View[proto.Candle]) View
```

### Changes — new `internal/engine/features/view.go`

```go
// View is the feature layer at one frame: the data view, and every result pinned at its column.
// Valid until the next pin.
type View struct {
	frame.View[proto.Candle]
	g *Graph
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

**New `internal/engine/features/graph_test.go`** — `Graph`s from `newGraph` over hand-built instances:

- **`TestGraphNoRings`** — `newGraph` allocates no buffer and every instance's node is bound; `New`
  allocates them, and the nodes then read and write without rebinding.
- **`TestGraphShapes`** — rings empty where an ID does not run; a stream's ring is `{L × W, P, W, L}`
  with `L` the longest `Lookback`; `L` is 1 without lookback.
- **`TestGraphBindErrors`** — `diff` over `sign` (`Dep[float64]` over `int`), `spread` at a stream,
  `last` at a pair and `label` are all reported by one `newGraph` call, each naming its composed ID
  and coordinate, with no ring allocated.
- **`TestGraphErrors`** — an ID twice on one coordinate; a dependency not running at the stream, or
  on one end of a pair; two variant IDs under one composed ID; a pair's ends equal, reversed or out of
  range; each named by ID and coordinate.

**New `internal/engine/features/engine_test.go`**

- **`TestEngineFrame`** — `last` and `diff` on every stream: `diff` fired after `last` reads its
  result at the same column; `ViewOf` reads both, and no later column; `sign` reads back as `int`.
- **`TestEngineGap`** — after `skip(Gap)` on `last`, `diff` sees `Gap` in `Latest` and skips the
  column in `Data`.
- **`TestEngineHistory`** — result history steps one bar: the direct 1h on the 5m grid (period 12)
  and the rolling 1h from 5m (period 1, window 12); `Gap` skipped.
- **`TestEnginePairs`** — one pair table, sorted, shared by every pair ID; `Pair` in either order; a
  pair result is one ring; `spread` and `pdiff` read both ends; shapes: rolling 1h × direct 1h →
  period 12, window 1; rolling 1h × ETHUSDT 5m → period 1, window 12.
- **`TestViewOf`** — the embedded data view reads candles; `ViewOf` with the wrong `T`, or an unknown
  ID, is an error.
- **`TestEngineAllocations`** — `pin`, `Input` reads with `dst` capacity and `ViewOf` allocate
  nothing.

### Documentation

- [features.md](../../discussions/features.md) — WIP: `frame` in the protocol layer, the contract,
  registry, graph and engine implemented; Solution references this stage.
- [main.md](main.md) — stage 3 marked implemented.

### Acceptance

- `go test -race ./internal/protocol/... ./internal/engine/features/` passes; the invariant command
  prints nothing; `make check` clean.

## Documentation recorded at planning

Committed with this plan, before step 1:

- [features.md](../../discussions/features.md) — the contract settled (§ Feature graph, Solution
  § Contract sketch); value types named by the engine's switch on `Bind`, one typed buffer per composed
  ID in the stream and pair tables, the pair ring's period (§ Feature buffer); binding in expansion and
  rings at run (§ Feature layer); `frame` in the protocol layer and what `protocol` may import; the
  design's reversals (§ Reversed on 2026-11-03, § Reversed on 2026-11-07); slots written `Gap` when a
  node cannot compute or is skipped.
- [main.md](main.md) — the decisions, stages 3 to 7, and the open items this stage resolves.
- [data-layer-v2.md](../../discussions/data-layer-v2.md) — rings, slots and `View` in
  `internal/protocol/frame`.
- [config.md](../../discussions/config.md), [pipeline-runtime.md](../../discussions/pipeline-runtime.md)
  — dependencies come from each feature's `Deps`.
- `docs/Architecture.md` — the rule on what `protocol` may import. `frame`'s location changes in step 1
  and the contract's entry arrives in step 2, so the document keeps matching the code.

## Out of scope

- Expansion, `Spec` and injection from the composition root; the `-` rule on variant IDs; variants of
  one type declaring different `Pair` or `Deps` — stage 5.
- Firing rules, schedules, `Step` and the memory warning — stage 6.
- The ported features — stage 4.
- Value types beyond `float64` and `int` — a case each, with the first feature that needs one.
- The test of every registered type against `newColumn` — stage 4, once types are registered.
- Reading a node's own past results (recursive features) — iteration 4: `Latest` at the column
  being written anchors on the slot about to be written, so it needs a read of the previous update.
- Pair features over pair features; asymmetric pair order and symmetry.
- How a strategy reads a struct result: `ViewOf` is in `engine/features` and the type in a feature
  domain, while a domain imports only `protocol` ([features.md](../../discussions/features.md) § Open
  Questions).
- `data_logger_v2`, `data_logger`, `proto.Feature` and `reg.Features`.

## Verification

```bash
# step 1
go test -race ./internal/protocol/frame/ ./internal/engine/...
# step 2
go test -race ./internal/protocol/feature/
# step 3
go test -race ./internal/engine/features/
# step 4
go test -race ./internal/protocol/... ./internal/engine/features/
# every step
make check
# steps 1, 2 and 4: prints nothing when protocol imports nothing outside itself
go list -f '{{join .Imports "\n"}}' ./internal/protocol/... | grep '^whatever/' | grep -v '^whatever/internal/protocol'
```
