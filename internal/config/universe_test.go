package config

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	proto "whatever/internal/protocol"
)

// The streams of testStreams, by ID.
const (
	btc5m  = "BTCUSDT:5m:binance"
	btc1h  = "BTCUSDT:1h:kraken"
	btc1hD = "BTCUSDT:1h:derived_BTCUSDT:5m:binance"
	eth1h  = "ETHUSDT:1h:binance"
	eth5m  = "ETHUSDT:5m:kraken"
	eth1hD = "ETHUSDT:1h:derived_ETHUSDT:5m:kraken"
	ethbtc = "ETHBTC:1h:kraken"
)

// testStreams is a table on two providers, binance and kraken, with a derived 1h window on each.
func testStreams() *proto.Streams {
	derive1h := proto.NewTimeframeDerivationSet(proto.Timeframe1h)
	return proto.NewStreams([]proto.Subscription{
		{Symbol: "BTCUSDT", Timeframe: proto.Timeframe5m, Provider: "binance", Derive: derive1h},
		{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1h, Provider: "kraken"},
		{Symbol: "ETHUSDT", Timeframe: proto.Timeframe1h, Provider: "binance"},
		{Symbol: "ETHUSDT", Timeframe: proto.Timeframe5m, Provider: "kraken", Derive: derive1h},
		{Symbol: "ETHBTC", Timeframe: proto.Timeframe1h, Provider: "kraken"},
	})
}

// universe decodes each rule strictly, as Load does.
func universe(t *testing.T, rules ...string) Universe {
	t.Helper()
	u := Universe{Name: "test"}
	for _, js := range rules {
		var r Rule
		dec := json.NewDecoder(strings.NewReader(js))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&r); err != nil {
			t.Fatalf("decode rule %s: %v", js, err)
		}
		u.Rules = append(u.Rules, r)
	}
	return u
}

func index(t *testing.T, st *proto.Streams, id string) int {
	t.Helper()
	i, ok := st.Index(id)
	if !ok {
		t.Fatalf("no stream %q", id)
	}
	return i
}

// render lists r by stream ID, for failure messages.
func render(st *proto.Streams, r Resolved) string {
	streams := make([]string, len(r.Streams))
	for i, s := range r.Streams {
		streams[i] = st.All[s].Sub.ID()
	}
	pairs := make([]string, len(r.Pairs))
	for i, p := range r.Pairs {
		pairs[i] = st.All[p[0]].Sub.ID() + " ~ " + st.All[p[1]].Sub.ID()
	}
	return fmt.Sprintf("streams %v\npairs %v", streams, pairs)
}

type resolveCase struct {
	name    string
	rules   []string
	streams []string
	pairs   [][2]string // either order
}

// runResolve resolves each case over st and wants exactly its streams and pairs, sorted.
func runResolve(t *testing.T, st *proto.Streams, cases []resolveCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, errs := Resolve(universe(t, tc.rules...), st)
			if len(errs) > 0 {
				t.Fatalf("Resolve: %v", errs)
			}

			var want Resolved
			for _, id := range tc.streams {
				want.Streams = append(want.Streams, index(t, st, id))
			}
			slices.Sort(want.Streams)
			for _, p := range tc.pairs {
				a, b := index(t, st, p[0]), index(t, st, p[1])
				want.Pairs = append(want.Pairs, [2]int{min(a, b), max(a, b)})
			}
			slices.SortFunc(want.Pairs, func(a, b [2]int) int {
				return cmp.Or(cmp.Compare(a[0], b[0]), cmp.Compare(a[1], b[1]))
			})

			if !slices.Equal(got.Streams, want.Streams) || !slices.Equal(got.Pairs, want.Pairs) {
				t.Errorf("got\n%s\nwant\n%s", render(st, got), render(st, want))
			}
		})
	}
}

// wantErrs fails unless errs are exactly one per want, in order, each containing it.
func wantErrs(t *testing.T, errs []error, want ...string) {
	t.Helper()
	if len(errs) != len(want) {
		t.Fatalf("got %d errors %v, want %d %q", len(errs), errs, len(want), want)
	}
	for i, err := range errs {
		if !strings.Contains(err.Error(), want[i]) {
			t.Errorf("error %d = %q, want it to contain %q", i, err, want[i])
		}
	}
}

func TestResolveFilters(t *testing.T) {
	runResolve(t, testStreams(), []resolveCase{
		{"no filter selects every stream", []string{`{}`},
			[]string{btc5m, btc1h, btc1hD, eth1h, eth5m, eth1hD, ethbtc}, nil},
		{"symbol glob", []string{`{"symbol": ["*USDT"]}`},
			[]string{btc5m, btc1h, btc1hD, eth1h, eth5m, eth1hD}, nil},
		{"timeframe", []string{`{"timeframe": ["5m"]}`},
			[]string{btc5m, eth5m}, nil},
		{"provider exact", []string{`{"provider": ["kraken"]}`},
			[]string{btc1h, eth5m, eth1hD, ethbtc}, nil},
		{"provider glob", []string{`{"provider": ["bin*"]}`},
			[]string{btc5m, btc1hD, eth1h}, nil},
		{"kind", []string{`{"kind": ["derived"]}`},
			[]string{btc1hD, eth1hD}, nil},
		{"a list is OR", []string{`{"symbol": ["BTCUSDT", "ETHBTC"]}`},
			[]string{btc5m, btc1h, btc1hD, ethbtc}, nil},
		{"fields are AND", []string{`{"symbol": ["ETH*"], "timeframe": ["1h"], "kind": ["direct"]}`},
			[]string{eth1h, ethbtc}, nil},
		{"a filter selecting nothing", []string{`{"symbol": ["SOL*"]}`}, nil, nil},
	})
}

func TestResolveDerived(t *testing.T) {
	runResolve(t, testStreams(), []resolveCase{
		{"its window timeframe and its source's provider", []string{`{"timeframe": ["1h"], "provider": ["binance"]}`},
			[]string{btc1hD, eth1h}, nil},
		{"not its source's timeframe", []string{`{"timeframe": ["5m"], "kind": ["derived"]}`}, nil, nil},
		{"not its own provider field", []string{`{"provider": ["derived_*"]}`}, nil, nil},
	})
}

func TestResolveNamed(t *testing.T) {
	runResolve(t, testStreams(), []resolveCase{
		{"a derived stream by its ID, kind choosing partners only",
			[]string{`{"references": ["BTCUSDT:1h:derived_BTCUSDT:5m:binance"], "timeframe": ["1h"], "kind": ["direct"]}`},
			[]string{btc1hD, btc1h, eth1h, ethbtc},
			[][2]string{{btc1hD, btc1h}, {btc1hD, eth1h}, {btc1hD, ethbtc}}},
		{"a direct stream outside every filter",
			[]string{`{"references": ["BTCUSDT:5m:binance"], "symbol": ["ETH*"], "timeframe": ["1h"]}`},
			[]string{btc5m, eth1h, eth1hD, ethbtc},
			[][2]string{{btc5m, eth1h}, {btc5m, eth1hD}, {btc5m, ethbtc}}},
	})

	t.Run("a name that is no stream's ID", func(t *testing.T) {
		u := universe(t,
			`{"references": ["BTCUSDT:1h:kraken", "BTCUSDT:1h:binance", "BTCUSDT"]}`,
			`{"pairs": [["BTCUSDT:1h:kraken", "SOLUSDT:1h:kraken"]]}`)
		got, errs := Resolve(u, testStreams())
		wantErrs(t, errs,
			`rules[0].references[1]: no stream has ID "BTCUSDT:1h:binance"`,
			`rules[0].references[2]: no stream has ID "BTCUSDT"`,
			`rules[1].pairs[0][1]: no stream has ID "SOLUSDT:1h:kraken"`)
		if len(got.Pairs) != 6 {
			t.Errorf("got %d pairs, want rule 0's 6 by its valid reference", len(got.Pairs))
		}
	})
}

func TestResolveReferences(t *testing.T) {
	runResolve(t, testStreams(), []resolveCase{
		{"every partner, itself excluded", []string{`{"references": ["BTCUSDT:1h:kraken"], "timeframe": ["1h"]}`},
			[]string{btc1h, btc1hD, eth1h, eth1hD, ethbtc},
			[][2]string{{btc1h, btc1hD}, {btc1h, eth1h}, {btc1h, eth1hD}, {btc1h, ethbtc}}},
		{"references are not partners of each other",
			[]string{`{"references": ["BTCUSDT:1h:kraken", "ETHBTC:1h:kraken"], "symbol": ["ETHUSDT"], "timeframe": ["1h"]}`},
			[]string{btc1h, ethbtc, eth1h, eth1hD},
			[][2]string{{btc1h, eth1h}, {btc1h, eth1hD}, {ethbtc, eth1h}, {ethbtc, eth1hD}}},
		{"no partners, no pairs", []string{`{"references": ["BTCUSDT:1h:kraken"], "symbol": ["SOL*"]}`},
			[]string{btc1h}, nil},
	})
}

func TestResolveExplicitPairs(t *testing.T) {
	runResolve(t, testStreams(), []resolveCase{
		{"as written", []string{`{"pairs": [
			["BTCUSDT:1h:kraken", "ETHUSDT:1h:derived_ETHUSDT:5m:kraken"],
			["ETHBTC:1h:kraken", "BTCUSDT:5m:binance"]]}`},
			[]string{btc1h, eth1hD, ethbtc, btc5m},
			[][2]string{{btc1h, eth1hD}, {ethbtc, btc5m}}},
	})
}

func TestResolveAllPairs(t *testing.T) {
	runResolve(t, testStreams(), []resolveCase{
		{"every unordered pair", []string{`{"pairs": "*", "symbol": ["ETH*"], "timeframe": ["1h"]}`},
			[]string{eth1h, eth1hD, ethbtc},
			[][2]string{{eth1h, eth1hD}, {eth1h, ethbtc}, {eth1hD, ethbtc}}},
		{"across timeframes", []string{`{"pairs": "*", "symbol": ["BTCUSDT"]}`},
			[]string{btc5m, btc1h, btc1hD},
			[][2]string{{btc5m, btc1h}, {btc5m, btc1hD}, {btc1h, btc1hD}}},
		{"one stream, no pairs", []string{`{"pairs": "*", "symbol": ["BTC*"], "kind": ["derived"]}`},
			[]string{btc1hD}, nil},
	})
}

func TestResolveUnion(t *testing.T) {
	runResolve(t, testStreams(), []resolveCase{
		{"a pair from two rules, written both ways, is one pair",
			[]string{
				`{"pairs": "*", "symbol": ["ETHUSDT"]}`,
				`{"pairs": [["ETHUSDT:1h:derived_ETHUSDT:5m:kraken", "ETHUSDT:5m:kraken"]]}`,
				`{"references": ["ETHUSDT:5m:kraken"], "symbol": ["ETHUSDT"], "timeframe": ["1h"]}`,
			},
			[]string{eth1h, eth5m, eth1hD},
			[][2]string{{eth1h, eth5m}, {eth1h, eth1hD}, {eth5m, eth1hD}}},
		{"a rule without a pairing adds streams only, sorted",
			[]string{
				`{"symbol": ["ETHBTC"]}`,
				`{"pairs": [["ETHUSDT:1h:binance", "BTCUSDT:5m:binance"]]}`,
			},
			[]string{ethbtc, eth1h, btc5m},
			[][2]string{{eth1h, btc5m}}},
	})
}

func TestUniverseCheck(t *testing.T) {
	cases := []struct {
		name  string
		rules []string
		want  []string
	}{
		{"valid rules", []string{
			`{}`,
			`{"symbol": ["*USDT"], "timeframe": ["1h"], "provider": ["bin*"], "kind": ["direct"]}`,
			`{"references": ["BTCUSDT:1h:derived_BTCUSDT:5m:binance"], "symbol": ["ETH*"]}`,
			`{"pairs": "*", "symbol": ["*USDT"]}`,
			`{"pairs": [["BTCUSDT:1h:kraken", "ETHBTC:1h:kraken"]]}`,
		}, nil},
		{"no rules", nil, []string{"rules: at least one rule is required"}},
		{"references with pairs", []string{`{"references": ["BTCUSDT:1h:kraken"], "pairs": "*"}`},
			[]string{"rules[0]: references and pairs are exclusive"}},
		{"empty lists", []string{
			`{"symbol": [], "timeframe": [], "provider": [], "kind": [], "references": []}`,
			`{"pairs": []}`,
		}, []string{
			"rules[0].symbol: empty list",
			"rules[0].timeframe: empty list",
			"rules[0].provider: empty list",
			"rules[0].kind: empty list",
			"rules[0].references: empty list",
			"rules[1].pairs: empty list",
		}},
		{"empty values", []string{`{"symbol": ["BTC*", ""], "provider": [""], "references": [""]}`},
			[]string{"rules[0].symbol: empty value", "rules[0].provider: empty value", "rules[0].references: empty value"}},
		{"malformed globs", []string{`{"symbol": ["BTC*", "[BTC"], "provider": ["bin\\"]}`},
			[]string{`rules[0].symbol[1]: invalid glob "[BTC"`, `rules[0].provider[0]: invalid glob "bin\\"`}},
		{"explicit pairs not of two distinct names", []string{`{"pairs": [
			["BTCUSDT:1h:kraken"],
			["BTCUSDT:1h:kraken", "ETHBTC:1h:kraken", "ETHUSDT:1h:binance"],
			["BTCUSDT:1h:kraken", "BTCUSDT:1h:kraken"],
			["BTCUSDT:1h:kraken", ""]]}`},
			[]string{
				"rules[0].pairs[0]: a pair names two streams, got 1",
				"rules[0].pairs[1]: a pair names two streams, got 3",
				`rules[0].pairs[2]: "BTCUSDT:1h:kraken" is paired with itself`,
				"rules[0].pairs[3]: empty value",
			}},
		{"filters on explicit pairs", []string{`{"pairs": [["BTCUSDT:1h:kraken", "ETHBTC:1h:kraken"]], "kind": ["direct"]}`},
			[]string{"rules[0]: explicit pairs take no filter"}},
		{"collected across rules", []string{
			`{"symbol": ["["]}`,
			`{}`,
			`{"references": [""], "pairs": [["BTCUSDT:1h:kraken", "ETHBTC:1h:kraken"]]}`,
		}, []string{
			"rules[0].symbol[0]: invalid glob",
			"rules[2]: references and pairs are exclusive",
			"rules[2].references: empty value",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wantErrs(t, universe(t, tc.rules...).check(), tc.want...)
		})
	}
}

func TestPairsUnmarshal(t *testing.T) {
	var all Pairs
	if err := json.Unmarshal([]byte(`"*"`), &all); err != nil || !all.All || all.List != nil {
		t.Errorf(`"*" = %+v, %v; want All`, all, err)
	}

	var list Pairs
	err := json.Unmarshal([]byte(`[["a", "b"], ["c"]]`), &list)
	if err != nil || list.All || !slices.EqualFunc(list.List, [][]string{{"a", "b"}, {"c"}}, slices.Equal) {
		t.Errorf("a list = %+v, %v; want it as written", list, err)
	}

	for _, js := range []string{`"all"`, `3`, `[["a", 1]]`} {
		var p Pairs
		if err := json.Unmarshal([]byte(js), &p); err == nil {
			t.Errorf("%s decoded as %+v, want an error", js, p)
		}
	}
}

// readme labels the streams of configs/README.md § Universes by the guide's letters.
var readme = map[string]string{
	"A": "BTCUSDT:5m:btc_5m",
	"B": "BTCUSDT:1h:derived_BTCUSDT:5m:btc_5m",
	"C": "BTCUSDT:1h:btc_1h",
	"D": "ETHUSDT:5m:eth_5m",
	"E": "ETHUSDT:1h:derived_ETHUSDT:5m:eth_5m",
	"F": "SOLUSDT:1h:sol_1h",
	"G": "ETHBTC:1h:ethbtc_1h",
}

// readmeStreams is the table of the guide's five example subscriptions.
func readmeStreams() *proto.Streams {
	derive1h := proto.NewTimeframeDerivationSet(proto.Timeframe1h)
	return proto.NewStreams([]proto.Subscription{
		{Symbol: "BTCUSDT", Timeframe: proto.Timeframe5m, Provider: "btc_5m", Derive: derive1h},
		{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1h, Provider: "btc_1h"},
		{Symbol: "ETHUSDT", Timeframe: proto.Timeframe5m, Provider: "eth_5m", Derive: derive1h},
		{Symbol: "SOLUSDT", Timeframe: proto.Timeframe1h, Provider: "sol_1h"},
		{Symbol: "ETHBTC", Timeframe: proto.Timeframe1h, Provider: "ethbtc_1h"},
	})
}

// TestResolveExamples resolves every universe example of configs/README.md, written as the guide
// writes it, so the guide and the resolver cannot disagree.
func TestResolveExamples(t *testing.T) {
	st := readmeStreams()
	for _, id := range readme {
		index(t, st, id) // the guide's stream IDs are the table's
	}

	examples := []struct {
		rules   []string
		streams string // the guide's letters
		pairs   string // two letters each
	}{
		{[]string{`{}`}, "A B C D E F G", ""},
		{[]string{`{"symbol": ["*USDT"]}`}, "A B C D E F", ""},
		{[]string{`{"timeframe": ["1h"]}`}, "B C E F G", ""},
		{[]string{`{"timeframe": ["1h"], "kind": ["direct"]}`}, "C F G", ""},
		{[]string{`{"symbol": ["BTCUSDT", "ETHUSDT"], "timeframe": ["1h"]}`}, "B C E", ""},
		{[]string{`{"provider": ["*_5m"]}`}, "A B D E", ""},
		{[]string{`{"references": ["BTCUSDT:1h:btc_1h"], "timeframe": ["1h"]}`}, "B C E F G", "CB CE CF CG"},
		{[]string{`{"references": ["BTCUSDT:5m:btc_5m"], "symbol": ["*USDT"], "timeframe": ["1h"]}`}, "A B C E F", "AB AC AE AF"},
		{[]string{`{"references": ["BTCUSDT:1h:btc_1h", "SOLUSDT:1h:sol_1h"], "symbol": ["ETH*"], "timeframe": ["1h"]}`}, "C E F G", "CE CG FE FG"},
		{[]string{`{"references": ["BTCUSDT:1h:derived_BTCUSDT:5m:btc_5m"], "timeframe": ["1h"], "kind": ["direct"]}`}, "B C F G", "BC BF BG"},
		{[]string{`{"pairs": [["BTCUSDT:1h:btc_1h", "ETHUSDT:1h:derived_ETHUSDT:5m:eth_5m"], ["SOLUSDT:1h:sol_1h", "ETHBTC:1h:ethbtc_1h"]]}`}, "C E F G", "CE FG"},
		{[]string{`{"pairs": "*", "symbol": ["*USDT"], "timeframe": ["1h"]}`}, "B C E F", "BC BE BF CE CF EF"},
		{[]string{`{"pairs": "*", "symbol": ["*USDT"], "timeframe": ["1h"], "kind": ["direct"]}`}, "C F", "CF"},
		{[]string{`{"pairs": "*", "symbol": ["BTCUSDT"]}`}, "A B C", "AB AC BC"},
		{[]string{
			`{ "pairs": "*", "symbol": ["*USDT"], "timeframe": ["1h"], "kind": ["direct"] }`,
			`{ "pairs": [["SOLUSDT:1h:sol_1h", "BTCUSDT:1h:btc_1h"]] }`,
			`{ "symbol": ["ETHBTC"] }`,
		}, "C F G", "CF"},
		{[]string{`{ "symbol": ["*USDT"] }`}, "A B C D E F", ""}, // the usdt universe rsi_14 binds to
	}

	cases := make([]resolveCase, len(examples))
	for i, ex := range examples {
		cases[i] = resolveCase{name: strings.Join(ex.rules, " "), rules: ex.rules}
		for _, l := range strings.Fields(ex.streams) {
			cases[i].streams = append(cases[i].streams, readme[l])
		}
		for _, p := range strings.Fields(ex.pairs) {
			cases[i].pairs = append(cases[i].pairs, [2]string{readme[p[:1]], readme[p[1:]]})
		}
	}
	runResolve(t, st, cases)

	errExamples := []struct{ rule, want string }{
		{`{"references": ["BTCUSDT:1h:btc_5m"]}`, `no stream has ID "BTCUSDT:1h:btc_5m"`},
		{`{"pairs": [["BTCUSDT:1h:btc_1h", "SOLUSDT:1h:sol_1h"]], "kind": ["direct"]}`, "explicit pairs take no filter"},
	}
	for _, ex := range errExamples {
		t.Run(ex.rule, func(t *testing.T) {
			u := universe(t, ex.rule)
			errs := u.check()
			if len(errs) == 0 {
				_, errs = Resolve(u, st)
			}
			wantErrs(t, errs, ex.want)
		})
	}
}
