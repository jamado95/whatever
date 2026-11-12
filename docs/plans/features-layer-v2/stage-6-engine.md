# Stage 6 — serial engine (features layer v2, iteration 1)

## Context

[main.md](main.md) stage 6. Stage 3 built the columns, whose nodes fire and skip by hand, and `New`,
which allocates the rings; stage 4 the candle copies `pin` refills; stage 5 `Expand`, whose
`newGraph` binds every instance and then drops it. Nothing decides which nodes run at a frame, in what
order, or what a node that does not fire is written.

This stage adds the serial feature engine: the graph keeps its nodes, `New` builds one schedule per
due pattern, and `Step` runs a frame. `New` warns when the feature buffers would exceed 1 GiB. Nothing
calls `Step` outside tests until stage 7.

```
newGraph  bind every instance ─▶ link: one node per instance, with its dependencies' nodes
New       memoryWarning ─▶ log │ alloc rings, candle copies │ order ─▶ periods ─▶ a schedule per pattern
Step(v)   pin: legs, candle copies, views ─▶ pattern(col) ─▶ each node due, in order:
            ended before: nothing │ leg Ended: Ended │ leg Fresh, dependencies Fresh: fire │ else: Gap
```

The plan, with the decisions it records in [main.md](main.md) and
[features.md](../../discussions/features.md), is committed before step 1. The stage is then
implemented in two steps, in order. Each step is a review checkpoint: its changes stay unstaged for
review, are committed once approved, and the next step starts from that commit. Every step leaves the
tree building, tested and `make check` clean.

| Step | Adds | Commit |
|---|---|---|
| 1 | Nodes, schedules and `Step` | `feat(features): run a frame through the serial schedule` |
| 2 | The memory warning; the stage's documentation | `feat(features): warn when the feature buffers exceed 1 GiB` |

## Decisions

- **The graph keeps its nodes.** `newGraph`, once every instance is bound, links each to its
  dependencies' nodes at its stream, or at each end of its pair. Stage 5 left the instances to this
  stage; the nodes replace them, holding only what a frame needs: the column, its ring, the
  coordinate, the period and the dependencies.
- **Serial order is stream-major.** Each stream's nodes, streams ascending, then each pair's, by pair
  table index; on one coordinate, dependencies first, by depth. Pair nodes read only single-stream
  results (`Expand` rejects pair dependencies), so they follow every stream. A stream's run is the
  stream task iteration 2 shards.
- **A due pattern is a prefix of the periods.** A node is due where its ring's period divides the
  column: its stream's, or `max(Pa, Pb)` for a pair, itself a stream's. Timeframes form a divisibility
  chain, so the periods dividing a column are always the smallest ones, and pattern `k` holds the
  nodes whose period is one of the `k` smallest; pattern 0, where none divides, is empty. The chain
  already sizes pair rings; `TestTimeframeInvariants` in `internal/protocol` guards it.
- **Timeframe invariants are tested over `ValidTimeframes`,** so adding or changing a timeframe needs
  no test change. A timeframe missing from it, or from `IsValid`, is rejected by config loading, so
  the test does not check the declared constants against it.
- **Legs are read once per frame.** `pin` reads every stream's leg — `Fresh` when updated at the
  column (age 0), `Ended` once ended, `Gap` otherwise — and the candle refill reads them. A pair's leg
  is `Ended` when either end has ended, `Fresh` when both are, `Gap` otherwise.
- **Strict firing.** A node fires only when its leg is `Fresh` and each dependency fired with a value
  at the column. A dependency that cannot compute (warm-up) is written `Gap`, and its dependents are
  written `Gap` without firing, though they might have computed. Resolves "still releases its
  dependents" in [features.md](../../discussions/features.md) § Firing: releasing is the parallel
  scheduler's counter decrement, not firing.
- **Firing state is per node.** `fire` reports whether the node returned a value; the engine keeps it
  per node for the frame, and dependents read the flag, not their dependencies' rings. A dependency is
  due wherever its dependent is — the same stream, or an end whose period divides the pair's — and
  comes earlier in the order, so its flag is always the current frame's.
- **`Ended` is written once,** at a coordinate's first due column at or after its leg ended, and
  nothing is written after: its history stays readable, as on the data rings. A pair ends when either
  end does; `Ended` wins over `Gap`, since the pair can never fire again.
- **A stream that has not started reads `Gap`,** so its nodes are written `Gap` at each due column. A
  reader cannot tell it from an empty slot (`Latest` gives `Gap`, age −1, either way). A stream that
  ends without a candle never starts.
- **`Partial` frames need nothing of their own:** legs decide, and the frame's `Complete` flag is not
  read.
- **`Step` takes `Current` frames in increasing column order,** as `data.Next` releases them. `Late`
  and `Backfill` come with the live mode; nothing checks.
- **The memory warning is isolated in `memoryWarning`.** It estimates every result ring's slots and
  each stream's candle copy from their sizes, allocating nothing, so `New` calls it before allocating
  and tests check it on graphs no test could allocate. Above 1 GiB it names the total, the lookback
  every result ring holds, and the three composed IDs holding the most memory. Not universes: a result
  is stored once per composed ID and coordinate, so universes sharing a pulled dependency cannot each
  be charged for it. Each column reports its own size, `bytes`, since only it knows its slot type.
- **`New` takes a logger:** `New(g, log)`, as `data.New` does, for the warning.
- **Hand-fired tests move to `Step`.** The engine tests' `run.step` and the ported comparison step the
  engine instead of firing each node; tests that inject a result's `Gap` by hand keep `pin`, `fire`
  and `skip`, as store tests.

## Step 1 — nodes, schedules and `Step`

### Changes — `internal/engine/features/graph.go`

```go
// node is a bound instance: its column's ring at its coordinate, due every period columns, and the
// nodes of its dependencies there: at its stream, or at each end of its pair.
type node struct {
	c      column
	ring   int
	at     [2]int
	period int64
	deps   []int // indices in Graph.nodes
}

// link returns one node per instance, each with its dependencies' nodes at its stream, or at each end
// of its pair, in sorted dependency type order. check has found every dependency there.
func (g *Graph) link(st *proto.Streams, instances []instance) []node
```

- `Graph` gains `nodes []node`, in `newGraph`'s order; `newGraph` sets it with `link` once every
  instance is bound without error.

### Changes — `internal/engine/features/results.go`

- `column.fire` reports whether the node computed: `fire(ring int, col int64) bool`, true when it
  wrote `Fresh`.

### Changes — `internal/engine/features/engine.go`

```go
// Engine is one run of a Graph: its schedules and each frame's firing state.
type Engine struct {
	g         *Graph
	periods   []int64       // the nodes' periods, ascending: a divisibility chain
	schedules [][]int       // by pattern: the nodes due, in serial order
	legs      []frame.State // by stream, at the frame: Fresh when updated there, Ended, or Gap
	fresh     []bool        // by node: fired with a value at its last due column
	ended     []bool        // by node: Ended written
}

/*
Step runs v's frame, a Current frame after the last one stepped. It pins every result at the frame and
refills the candle copies of the streams updated there, then runs the nodes due at its column in
serial order. A node fires when its leg is Fresh — both ends', for a pair — and each of its
dependencies fired with a value at the column; otherwise it is written Gap, or Ended at its first due
column at or after its leg ended, and nothing after. It returns the feature layer at the frame, valid
until the next Step.
*/
func (e *Engine) Step(v frame.View[proto.Candle]) View

/*
order is the serial order of g's nodes: each stream's, streams ascending, then each pair's, by pair
table index; on one coordinate, dependencies first. A node's depth is 0 without dependencies, and one
more than its deepest dependency's otherwise.
*/
func order(g *Graph) []int

/*
schedules returns the nodes' periods, ascending, and one schedule per due pattern: schedule k holds the
nodes of order whose period is one of the k smallest, in order. The periods form a divisibility chain,
so those dividing a column are always the smallest ones.
*/
func schedules(g *Graph, order []int) ([]int64, [][]int)

// pattern is col's due pattern: how many of the periods, ascending, divide it.
func (e *Engine) pattern(col int64) int

// streamLeg is stream s's leg at v's frame: Fresh when updated there (Fresh, age 0), Ended once ended,
// Gap otherwise: a Gap slot, not started, or between its due columns.
func streamLeg(v frame.View[proto.Candle], s int) frame.State

// leg is a node's leg at the frame: its stream's; for a pair, Ended when either end has ended, Fresh
// when both are, Gap otherwise.
func (e *Engine) leg(at [2]int) frame.State
```

- `New` also builds the order, the periods and the schedules, and allocates `legs`, `fresh` and
  `ended`, so `Step` allocates nothing.
- `Step`, for each node of its pattern's schedule: nothing once `Ended` is written; `Ended` written
  when its leg is `Ended`; fired when its leg is `Fresh` and every dependency's `fresh` flag is set,
  its flag set from `fire`; otherwise `Gap` written. A node not fired clears its flag.
- `pin` sets `legs` with `streamLeg` before the refill; `refill` takes `legs` and refills the streams
  whose leg is `Fresh`, instead of reading `Latest` itself.

### Tests

**New `internal/engine/features/step_test.go`** — over the test stream table, with candle buffers
from `streamCandles` whose legs a test rewrites with `Put` (`Gap`, `Ended`) and `Start` (a late
start):

- **`TestStepOrder`** — instances listed dependents first: `ddiff` over `diff` over `last` on `btc5m`
  and `rolling1h`; from its warm-up on, each dependent is `Fresh` with the value of its dependencies
  at the same column.
- **`TestStepDue`** — patterns over periods {1, 12}: 2 at 24, 1 at 13; `direct1h`'s `last` holds a slot
  at multiples of 12 only, `btc5m`'s at every column; a graph on `direct1h` only has pattern 0 at 13,
  where `Step` writes nothing.
- **`TestStepLegs`** — one run: at 5, `btc5m`'s leg is `Gap` in a `Partial` frame; `eth5m`'s nodes are
  `Fresh`, `btc5m`'s `last` and `diff-last` `Gap`, carrying their values at 4. `rolling1h` starts at
  3: its `last` is `Gap` before, `Fresh` from 3. `eth5m` ends at 8: its nodes are `Ended` at 8,
  carrying their values at 7, with no slot written after; at 12, `Latest` reads `Ended` at age 5 and
  `Data` still reads their history.
- **`TestStepStrict`** — `ratio-sum_3-last` on `btc5m` is `Gap` at 0 and 1, while `sum_3` warms up,
  though `ratio` would compute; `Fresh` from 2, `sum_3`'s value over `last`'s.
- **`TestStepPairs`** — `spread` on (`direct1h`, `rolling1h`) and (`eth5m`, `rolling1h`), `pdiff-last`
  on the first: the coarse pair writes at multiples of 12 only, the fine one at every column.
  `rolling1h` `Gap` at 24: both pairs and `pdiff-last` `Gap` there. `rolling1h` ends at 41: the fine
  pair is `Ended` at 41, the coarse pair and `pdiff-last` at 48, their first due column at or after
  it.
- **`TestStepBruteForce`** — over fixed seeds: `expand` places `last`, `sum_3` and `sign` on every
  stream, `ddiff` over `diff` over `last`, `ratio` over `sum_3` and `last`, and `spread` and `pdiff` on
  (`direct1h`, `rolling1h`), (`btc5m`, `eth5m`) and (`eth5m`, `rolling1h`); each seed draws a start
  and an end, or none, per stream and `Gap` legs at one due column in ten, over 200 columns. Two graphs
  from the same instances, one stepped by `Step`, the other by `bruteStep`, which applies the firing
  rules as defined: it passes over the instances in reverse until every one due at the column is
  decided, deciding one once its dependencies are, reading legs from the candles and dependencies'
  states from their results at the column. After each column, every slot of every ring is equal.

**`internal/protocol/subscription_test.go`** — **`TestTimeframeInvariants`**, looping over
`ValidTimeframes()`:

- strictly ascending duration, as `TimeframeDerivationSet.List` assumes, and no more timeframes than
  `TimeframeDerivationSet` has bits (`reflect.TypeFor[TimeframeDerivationSet]().Bits()`);
- each duration divides every longer one: the divisibility chain stream periods, pair rings and the
  feature engine's due patterns rely on.

**`internal/engine/features/engine_test.go`**:

- `run.step` steps the engine with `Step`; `run.instances` is removed with the hand-firing it served.
- `TestEnginePairs` reads column 23 with `pin`, not a second step, since `Step` takes increasing
  columns.
- `TestEngineGap` and `TestEngineHistory` keep firing by hand at the column where they inject a
  result's `Gap`: `Step` would not fire `seen-last` and `diff-last` over a `Gap` `last`. They cover
  reads.
- `TestEngineAllocations` steps successive columns with `Step`, consumer reads included: no
  allocation.

**`internal/engine/features/ported_test.go`** — `compare` steps the engine with `Step` instead of
firing each port by hand at each updated stream.

### Acceptance

- `go test -race ./internal/engine/features/ ./internal/protocol/` passes; `make check` clean.
- `go test -count=1 -run Reference -v ./internal/engine/features/` passes with the reference dataset.

## Step 2 — the memory warning

### Changes — new `internal/engine/features/memory.go`

```go
// memoryLimit is the estimate above which New warns: 1 GiB.
const memoryLimit = 1 << 30

/*
memoryWarning estimates what New allocates for g — every result ring's slots and each stream's candle
copy — from their sizes, allocating nothing. Above memoryLimit it returns a warning naming the total,
the lookback every result ring holds, and the three composed IDs holding the most memory, ties by ID.
*/
func memoryWarning(g *Graph) (string, bool)
```

Warning: `features: buffers estimated at 3.6 GiB, above 1 GiB, at a lookback of 4194304 bars;
largest: last 1664.0 MiB, sum_4194304 1536.0 MiB, sign 128.0 MiB`. Candle copies count in the total
only: they belong to streams, not IDs.

### Changes — `internal/engine/features/results.go`

- `column` gains `bytes() int64`: its shapes' slots × the size of `frame.Slot[T]`.

### Changes — `internal/engine/features/engine.go`

- `New(g *Graph, log *logger.Logger) *Engine` logs `memoryWarning`'s warning with `Warn` before
  allocating.

### Tests

- **New `internal/engine/features/memory_test.go`** — **`TestMemoryWarning`**: a graph below the limit
  gives none. A graph never passed to `New`, with `sum` at a lookback of 2²² on `rolling1h`, `last` on
  `btc5m` and `rolling1h`, `sign` on `eth5m` and `sum_2` on `btc5m`, gives the warning above, its
  total including the candle copies; `sign` and `sum_2` hold the same, and `sign` is named.
- `New`'s callers in `engine_test.go`, `graph_test.go` and `ported_test.go` pass `quietLog()`.

### Documentation

- [features.md](../../discussions/features.md):
  - WIP: the serial engine implemented — `New` builds the schedules, `Step` runs a frame; nothing
    calls it outside tests until stage 7.
  - Solution: references this stage.
- [pipeline-runtime.md](../../discussions/pipeline-runtime.md) — WIP: the feature engine runs a frame
  serially; `data_logger_v2` runs it from stage 7.
- [main.md](main.md) — stage 6 marked implemented.
- `docs/Architecture.md` — `internal/engine/features`: `New`, allocating the rings and candle copies,
  building one schedule per due pattern and warning above 1 GiB; `Step`, running a frame serially.

### Acceptance

- `go test -race ./internal/engine/features/` passes; `make check` clean.
- Stage acceptance ([main.md](main.md), as amended): dependencies computed before their dependents; a
  coarse stream's nodes fire only at its due columns; `Gap` and `Ended` legs fire nothing, their nodes
  written `Gap`, or `Ended` once; a dependent of a dependency that cannot compute written `Gap`
  without firing; `Partial` frames compute their `Fresh` streams; pairs fire at their ends' larger
  period only when both ends are `Fresh`, and end when either end does; a run equals a brute-force
  evaluation over the same frames; the warning fires above 1 GiB; no allocation per `Step`.

## Documentation recorded at planning

Committed with this plan, before step 1:

- [main.md](main.md):
  - Structure: `stage-6-engine.md` planned.
  - Stage 6: touches and acceptance as this plan amends them — the graph keeps its nodes; `New(graph,
    log)`; strict firing; `Ended` written once, a pair ending with either end; the warning, in
    `memoryWarning`, naming the lookback and the largest composed IDs; the hand-fired tests and the
    ported comparison through `Step`; `internal/protocol`'s test of the timeframes' invariants.
  - Iteration 1's diagram and stage 7: `features.New(graph, log)`.
- [features.md](../../discussions/features.md):
  - § Scheduling and firing, *Firing*: a node that cannot compute releases its dependents, which,
    reading a non-`Fresh` input, are written `Gap` without firing; a coordinate whose leg ends is
    written `Ended` once, at its first due column at or after the end, and nothing after; a pair ends
    when either end does.
  - § Feature buffer, *Memory warning*: naming the lookback and the composed IDs holding the most
    memory, not universes, which cannot each be charged for a shared result.

## Out of scope

- `data_logger_v2` running the engine, `data.New` sized from `Graph.Lookback`, exports, and the `Step`
  benchmark (stage 7).
- The parallel scheduler, stream tasks and counters (iteration 2).
- `Late` recomputes, `Pending` legs and `Backfill` frames (the live mode).
- Pair features over pair features, and asymmetric pair order.
- Counting nodes' scratch in the memory estimate.

## Verification

```bash
# step 1
go test -race ./internal/engine/features/ ./internal/protocol/
go test -count=1 -run Reference -v ./internal/engine/features/
# step 2
go test -race ./internal/engine/features/
# every step
make check
```
