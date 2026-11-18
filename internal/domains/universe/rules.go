package universe

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path"
	"reflect"
	"slices"

	"whatever/internal/config"
	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"
)

// RulesID is the rules universe type: streams and pairs selected by self-contained rules.
const RulesID = "rules"

/*
Factory builds a rules universe: its options decoded strictly and validated, then resolved over the
injected stream table. Each stage's errors come back joined.
*/
func init() {
	reg.Universes.Register(RulesID, func(opts map[string]any) (proto.Universe, error) {
		var cfg RulesConfig
		if err := config.Decode(opts, &cfg); err != nil {
			return proto.Universe{}, err
		}

		st, err := config.Dep[*proto.Streams](opts, "_streams")
		if err != nil {
			return proto.Universe{}, err
		}

		u, errs := cfg.resolve(st)
		return u, errors.Join(errs...)
	})
}

/*
Factory builds a rules universe: its options decoded strictly and validated, then resolved over the
injected stream table. Each stage's errors come back joined.
*/

// RulesConfig is a rules universe's options: its streams and pairs are the union of its rules'.
type RulesConfig struct {
	Rules []Rule `json:"rules"`
}

// Rule selects streams by filters, and pairs them by at most one of References or Pairs.
type Rule struct {
	Symbol     []string           `json:"symbol"` // globs
	Timeframe  []proto.Timeframe  `json:"timeframe"`
	Provider   []string           `json:"provider"` // globs over provider instance names
	Kind       []proto.StreamKind `json:"kind"`
	References []string           `json:"references"` // stream IDs
	Pairs      *Pairs             `json:"pairs"`
}

// check returns r's shape errors, each prefixed at.
func (r Rule) check(at string) []error {
	var errs []error
	add := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}

	if r.References != nil && r.Pairs != nil {
		add(fmt.Errorf("%s: references and pairs are exclusive", at))
	}

	// every list field for empty lists and values, path-named by its JSON key
	rv := reflect.ValueOf(r)
	for i := 0; i < rv.NumField(); i++ {
		fieldT := rv.Type().Field(i)
		if fieldT.Type.Kind() != reflect.Slice {
			continue
		}

		rAt := fmt.Sprintf("%s.%s", at, fieldT.Tag.Get("json"))
		for _, e := range emptyList(rAt, rv.Field(i).Interface()) {
			add(e)
		}
	}

	for i, g := range r.Symbol {
		add(checkGlob(fmt.Sprintf("%s.symbol[%d]", at, i), g))
	}
	for i, g := range r.Provider {
		add(checkGlob(fmt.Sprintf("%s.provider[%d]", at, i), g))
	}

	if !r.Pairs.explicit() {
		return errs
	}
	if len(r.Pairs.List) == 0 {
		add(fmt.Errorf("%s.pairs: empty list", at))
	}
	if r.Symbol != nil || r.Timeframe != nil || r.Provider != nil || r.Kind != nil {
		add(fmt.Errorf("%s: explicit pairs take no filter", at))
	}
	for i, p := range r.Pairs.List {
		pat := fmt.Sprintf("%s.pairs[%d]", at, i)
		if len(p) != 2 {
			add(fmt.Errorf("%s: a pair names two streams, got %d", pat, len(p)))
			continue
		}
		if p[0] == p[1] {
			add(fmt.Errorf("%s: %q is paired with itself", pat, p[0]))
		}
		for _, e := range emptyList(pat, p) {
			add(e)
		}
	}
	return errs
}

// matches reports whether stream i passes every filter r sets.
func (r Rule) matches(st *proto.Streams, i int) bool {
	s := st.All[i]
	return globAny(r.Symbol, s.Sub.Symbol) &&
		(len(r.Timeframe) == 0 || slices.Contains(r.Timeframe, s.Sub.Timeframe)) &&
		globAny(r.Provider, st.Provider(i)) &&
		(len(r.Kind) == 0 || slices.Contains(r.Kind, s.Kind))
}

// Pairs is "*", every pair of the rule's streams, or a list of explicit pairs.
type Pairs struct {
	All  bool
	List [][]string // two names each; checked, since a [2]string would drop extras silently
}

// UnmarshalJSON decodes "*" or [[a, b], …]; any other string is an error.
func (p *Pairs) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		if s != "*" {
			return fmt.Errorf("pairs must be \"*\" or a list of pairs, got %q", s)
		}

		*p = Pairs{All: true}

		return nil
	}

	var list [][]string
	if err := json.Unmarshal(b, &list); err != nil {
		return fmt.Errorf("pairs must be \"*\" or a list of pairs: %w", err)
	}

	*p = Pairs{List: list}

	return nil
}

// explicit reports whether p lists its pairs rather than pairing a rule's streams.
func (p *Pairs) explicit() bool { return p != nil && !p.All }

/*
resolve returns rc's universe over st, in proto.Universe's canonical form. Each rule's streams S are
the streams passing its filters; a references rule pairs each reference with S, explicit pairs are
taken as written, "*" pairs S. Streams are S plus every named stream; duplicates collapse and
self-pairs are dropped. A name that is no stream's ID is an error, path-named from the rule. rc must
have passed Validate.
*/
func (rc RulesConfig) resolve(st *proto.Streams) (proto.Universe, []error) {
	var errs []error
	streams := make(map[int]bool)
	pairs := make(map[[2]int]bool)

	// lookup returns the stream named id, recording an error for a name no stream has
	lookup := func(at, id string) (int, bool) {
		i, ok := st.Index(id)
		if !ok {
			errs = append(errs, fmt.Errorf("%s: no stream has ID %q", at, id))
		}
		return i, ok
	}
	pair := func(a, b int) {
		if a != b {
			pairs[[2]int{min(a, b), max(a, b)}] = true
		}
	}

	for ri, r := range rc.Rules {
		at := fmt.Sprintf("rules[%d]", ri)

		// explicit pairs take no filter, so select nothing beyond their names
		if r.Pairs.explicit() {
			for pi, p := range r.Pairs.List {
				a, okA := lookup(fmt.Sprintf("%s.pairs[%d][0]", at, pi), p[0])
				b, okB := lookup(fmt.Sprintf("%s.pairs[%d][1]", at, pi), p[1])
				if okA && okB {
					streams[a], streams[b] = true, true
					pair(a, b)
				}
			}
			continue
		}

		var s []int
		for i := range st.All {
			if r.matches(st, i) {
				s = append(s, i)
				streams[i] = true
			}
		}

		if r.Pairs != nil {
			for x, a := range s {
				for _, b := range s[x+1:] {
					pair(a, b)
				}
			}
		}
		for ki, id := range r.References {
			ref, ok := lookup(fmt.Sprintf("%s.references[%d]", at, ki), id)
			if !ok {
				continue
			}
			streams[ref] = true
			for _, b := range s {
				pair(ref, b)
			}
		}
	}

	ps := slices.Collect(maps.Keys(pairs))
	slices.SortFunc(ps, func(a, b [2]int) int {
		return cmp.Or(cmp.Compare(a[0], b[0]), cmp.Compare(a[1], b[1]))
	})

	return proto.Universe{Streams: st, Index: slices.Sorted(maps.Keys(streams)), Pairs: ps}, errs
}

/*
Validate returns rc's shape errors, joined, path-named from the rule: pairing, empty lists and values,
globs. Names are left to resolve: a name is valid if it is a stream's ID.
*/
func (rc RulesConfig) Validate() error {
	if len(rc.Rules) == 0 {
		return errors.New("rules: at least one rule is required")
	}
	var errs []error
	for ri, r := range rc.Rules {
		errs = append(errs, r.check(fmt.Sprintf("rules[%d]", ri))...)
	}
	return errors.Join(errs...)
}

/*
emptyList returns an error for a list given but empty, or one per empty value in it, each prefixed
at. A nil list is an absent field and does not constrain; anything but a slice is an error.
*/
func emptyList(at string, l any) []error {
	lv := reflect.ValueOf(l)
	if lv.Kind() != reflect.Slice {
		return []error{fmt.Errorf("%s: not a list", at)}
	}
	if lv.IsNil() {
		return []error{} // absent: does not constrain
	}
	if lv.Len() == 0 {
		return []error{fmt.Errorf("%s: empty list", at)}
	}

	var errs []error
	for i := 0; i < lv.Len(); i++ {
		if lv.Index(i).IsZero() {
			errs = append(errs, fmt.Errorf("%s: empty value at %d", at, i))
		}
	}

	return errs
}

// globAny reports whether name matches any of globs; no globs do not constrain.
func globAny(globs []string, name string) bool {
	if len(globs) == 0 {
		return true
	}

	for _, g := range globs {
		if ok, _ := path.Match(g, name); ok {
			return true
		}
	}

	return false
}

func checkGlob(at, g string) error {
	if _, err := path.Match(g, ""); err != nil {
		return fmt.Errorf("%s: invalid glob %q", at, g)
	}
	return nil
}
