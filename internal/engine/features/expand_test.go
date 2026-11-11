package features

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"whatever/internal/config"
)

// names are the test table's streams, short.
var names = [...]string{direct1h: "d1h", btc5m: "btc5m", eth5m: "eth5m", rolling1h: "r1h"}

// universe is a resolved universe of the test table: its streams, and its pairs.
func universe(streams []int, pairs ...[2]int) config.Resolved {
	return config.Resolved{Streams: streams, Pairs: pairs}
}

// testUniverses are the universes the tests bind to.
var testUniverses = map[string]config.Resolved{
	"btc":   universe([]int{btc5m}),
	"eth":   universe([]int{eth5m}),
	"d1h":   universe([]int{direct1h}),
	"pairs": universe([]int{direct1h, btc5m, eth5m, rolling1h}, pair(direct1h, rolling1h), pair(btc5m, eth5m)),
}

// rendered is each instance as id@stream or id@a,b, with its dependencies in Deps order; sorted.
func rendered(instances []instance) []string {
	out := make([]string, len(instances))
	for i, in := range instances {
		s := in.id + "@" + names[in.at[0]]
		if in.at[1] >= 0 {
			s += "," + names[in.at[1]]
		}
		var deps []string
		for _, d := range in.feature.Deps {
			deps = append(deps, d+"="+in.deps[d])
		}
		if len(deps) > 0 {
			s += " [" + strings.Join(deps, " ") + "]"
		}
		out[i] = s
	}
	slices.Sort(out)
	return out
}

// mustExpand is specs' instances, rendered, failing the test on any error from Expand.
func mustExpand(t *testing.T, specs ...Spec) []string {
	t.Helper()
	if _, err := Expand(testStreams(), testUniverses, specs); err != nil {
		t.Fatal(err)
	}
	instances, err := expand(testStreams(), testUniverses, specs)
	if err != nil {
		t.Fatal(err)
	}
	return rendered(instances)
}

func wantInstances(t *testing.T, got []string, want ...string) {
	t.Helper()
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("instances:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// every is id on every stream of the test table.
func every(id string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = id + "@" + n
	}
	return out
}

// TestExpandPlacement covers where a spec runs: every stream without a universe, its universe's
// streams, or its universe's pairs, which are then the pair table.
func TestExpandPlacement(t *testing.T) {
	specs := []Spec{
		spec("last", "", last()),
		spec("sum", "eth", sum(2)),
		spec("spread", "pairs", spread()),
	}
	wantInstances(t, mustExpand(t, specs...),
		slices.Concat(every("last"), []string{"sum_2@eth5m", "spread@d1h,r1h", "spread@btc5m,eth5m"})...)

	g, err := Expand(testStreams(), testUniverses, specs)
	if err != nil {
		t.Fatal(err)
	}
	want := map[[2]int]int{pair(direct1h, rolling1h): 0, pair(btc5m, eth5m): 1}
	if !maps.Equal(g.pairs, want) {
		t.Errorf("pair table = %v, want %v", g.pairs, want)
	}
}

// TestExpandDependencies covers dependencies following their dependents: onto every stream they run
// on, beyond the dependency's own universe, down a chain, and onto both ends of a pair.
func TestExpandDependencies(t *testing.T) {
	t.Run("beyond the dependency's universe", func(t *testing.T) {
		got := mustExpand(t, spec("last", "eth", last()), spec("diff", "", diff()))
		wantInstances(t, got, slices.Concat(every("last"), []string{
			"diff-last@d1h [last=last]", "diff-last@btc5m [last=last]",
			"diff-last@eth5m [last=last]", "diff-last@r1h [last=last]",
		})...)
	})

	t.Run("down a chain", func(t *testing.T) {
		got := mustExpand(t, spec("last", "eth", last()), spec("diff", "eth", diff()), spec("ddiff", "btc", ddiff()))
		wantInstances(t, got,
			"ddiff-diff-last@btc5m [diff=diff-last]",
			"diff-last@btc5m [last=last]", "diff-last@eth5m [last=last]",
			"last@btc5m", "last@eth5m")
	})

	t.Run("onto both ends of a pair", func(t *testing.T) {
		got := mustExpand(t, spec("last", "eth", last()), spec("pdiff", "pairs", pdiff()))
		wantInstances(t, got,
			"pdiff-last@d1h,r1h [last=last]", "pdiff-last@btc5m,eth5m [last=last]",
			"last@d1h", "last@btc5m", "last@eth5m", "last@r1h")
	})
}

// TestExpandVariants covers composition: one composed ID per combination of dependency variants, in
// Deps order, and a type configured in two entries, each on its own universe.
func TestExpandVariants(t *testing.T) {
	t.Run("every combination, in Deps order", func(t *testing.T) {
		got := mustExpand(t,
			spec("sum", "btc", sum(2)), spec("sum", "btc", sum(3)),
			spec("last", "btc", last()), spec("ratio", "btc", ratio()))
		wantInstances(t, got,
			"ratio-sum_2-last@btc5m [sum=sum_2 last=last]",
			"ratio-sum_3-last@btc5m [sum=sum_3 last=last]",
			"sum_2@btc5m", "sum_3@btc5m", "last@btc5m")
	})

	t.Run("a type in two entries", func(t *testing.T) {
		got := mustExpand(t,
			spec("sum", "btc", sum(2)), spec("sum", "eth", sum(3)),
			spec("last", "d1h", last()), spec("ratio", "d1h", ratio()))
		wantInstances(t, got,
			"sum_2@btc5m", "sum_3@eth5m", "last@d1h",
			"ratio-sum_2-last@d1h [sum=sum_2 last=last]",
			"ratio-sum_3-last@d1h [sum=sum_3 last=last]",
			"sum_2@d1h", "sum_3@d1h")
	})
}

// TestExpandLookback covers the graph's lookback: the longest across its instances, those pulled in
// as dependencies included, and 1 with none.
func TestExpandLookback(t *testing.T) {
	for _, c := range []struct {
		name  string
		specs []Spec
		want  int
	}{
		{"the longest", []Spec{spec("last", "", last()), spec("sum", "eth", sum(9)), spec("ratio", "btc", ratio())}, 9},
		{"none", nil, 1},
	} {
		g, err := Expand(testStreams(), testUniverses, c.specs)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got := g.Lookback(); got != c.want {
			t.Errorf("%s: lookback %d, want %d", c.name, got, c.want)
		}
	}
}

// TestExpandErrors covers each of the graph's checks, alone.
func TestExpandErrors(t *testing.T) {
	pairSum := sum(3)
	pairSum.Pair = true
	depSum := sum(3)
	depSum.Deps = []string{"last"}

	for _, c := range []struct {
		name  string
		specs []Spec
		want  string
	}{
		{"a variant ID twice", []Spec{spec("sum", "", sum(2)), spec("sum", "eth", sum(2))},
			"features: sum_2 is configured twice"},
		{"variants of one type unlike in Pair", []Spec{spec("sum", "", sum(2)), spec("sum", "pairs", pairSum)},
			"features: sum: sum_3 declares Pair or Deps unlike sum_2"},
		{"variants of one type unlike in Deps", []Spec{
			spec("last", "", last()), spec("sum", "", sum(2)), spec("sum", "", depSum)},
			"features: sum: sum_3 declares Pair or Deps unlike sum_2"},
		{"an unregistered dependency type", []Spec{spec("diff", "", diffOf("diff", "ghost"))},
			`features: diff: dependency type "ghost" is not registered`},
		{"a dependency type with no variant", []Spec{spec("diff", "", diff())},
			`features: diff: no variant of dependency type "last" is configured`},
		{"a pair dependency type", []Spec{spec("spread", "pairs", spread()), spec("diff", "", diffOf("diff", "spread"))},
			`features: diff: dependency type "spread" is a pair feature, not supported yet`},
		{"a cycle, its dependent adding nothing", []Spec{
			spec("loop_a", "", follow("loop_a", "loop_b")), spec("loop_b", "", follow("loop_b", "loop_a")),
			spec("tail", "", follow("tail", "loop_a"))},
			"features: dependency cycle: loop_a -> loop_b -> loop_a"},
		{"a type depending on itself", []Spec{spec("loop_self", "", follow("loop_self", "loop_self"))},
			"features: dependency cycle: loop_self -> loop_self"},
		{"a pair feature without a universe", []Spec{spec("spread", "", spread())},
			"features: spread: a pair feature needs a universe"},
		{"a universe without pairs", []Spec{spec("spread", "eth", spread())},
			`features: spread: universe "eth" has no pairs`},
		{"an unknown universe", []Spec{spec("last", "ghost", last())},
			`features: last: unknown universe "ghost"`},
		{"a dependent of a type with an error adds none", []Spec{
			spec("diff", "", diffOf("diff", "ghost")), spec("ddiff", "", ddiff())},
			`features: diff: dependency type "ghost" is not registered`},
	} {
		g, err := Expand(testStreams(), testUniverses, c.specs)
		if err == nil || err.Error() != c.want {
			t.Errorf("%s:\n got %v\nwant %s", c.name, err, c.want)
		}
		if g != nil {
			t.Errorf("%s: a graph despite the error", c.name)
		}
	}
}

// TestExpandStages covers the check stages: a stage's errors arrive together and stop expansion, the
// same on every call.
func TestExpandStages(t *testing.T) {
	btcID := testStreams().All[btc5m].Sub.ID()
	bindErrSpecs := []Spec{
		spec("last", "", sign()), // an int variant of the type diff reads as float64
		spec("diff", "btc", diff()),
	}

	for _, c := range []struct {
		name  string
		specs []Spec
		want  []string
	}{
		{"placement errors together, hiding a bind error", slices.Concat([]Spec{
			spec("spread", "", spread()), spec("sum", "ghost", sum(2))}, bindErrSpecs),
			[]string{
				"features: spread: a pair feature needs a universe",
				`features: sum_2: unknown universe "ghost"`,
			}},
		{"a duplicate hiding a cycle", []Spec{
			spec("sum", "", sum(2)), spec("sum", "", sum(2)),
			spec("loop_a", "", follow("loop_a", "loop_b")), spec("loop_b", "", follow("loop_b", "loop_a"))},
			[]string{"features: sum_2 is configured twice"}},
		{"a dependency error hiding a placement error", []Spec{
			spec("diff", "", diff()), spec("spread", "", spread())},
			[]string{`features: diff: no variant of dependency type "last" is configured`}},
		{"newGraph's value type and bind errors together", slices.Concat([]Spec{
			spec("label", "", label())}, bindErrSpecs),
			[]string{
				"features: label: unsupported Bind func(*feature.Binder) func() (string, bool)",
				fmt.Sprintf(`features: diff-sign on %s: feature: dependency "last" is not float64`, btcID),
			}},
	} {
		_, first := Expand(testStreams(), testUniverses, c.specs)
		if first == nil {
			t.Errorf("%s: no error", c.name)
			continue
		}
		if got := strings.Split(first.Error(), "\n"); !slices.Equal(got, c.want) {
			t.Errorf("%s:\n%s\nwant:\n%s", c.name, strings.Join(got, "\n"), strings.Join(c.want, "\n"))
		}
		if _, again := Expand(testStreams(), testUniverses, c.specs); again == nil || again.Error() != first.Error() {
			t.Errorf("%s: a second call reports:\n%v\nthe first:\n%v", c.name, again, first)
		}
	}
}
