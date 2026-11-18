package proto

import (
	"testing"
)

/*
TestUniverseValidate covers a universe as a type fills it: a canonical one and an empty one are valid;
no stream table is reported alone; an index entry past the table, an unsorted or duplicate index, a
self-pair, a reversed pair, unsorted or duplicate pairs and a pair end outside the index are each
reported.
*/
func TestUniverseValidate(t *testing.T) {
	st := NewStreams(testSubs())
	cases := []struct {
		name string
		u    Universe
		want []string
	}{
		{"canonical", Universe{Streams: st, Index: []int{0, 2, 4}, Pairs: [][2]int{{0, 2}, {0, 4}, {2, 4}}}, nil},
		{"empty", Universe{Streams: st}, nil},
		{"no stream table", Universe{Index: []int{9, 1}, Pairs: [][2]int{{1, 1}}},
			[]string{"no stream table"}},
		{"past the table", Universe{Streams: st, Index: []int{0, 7}},
			[]string{"stream 7 is not in the table of 5 streams"}},
		{"index unsorted and duplicate", Universe{Streams: st, Index: []int{2, 1, 1}},
			[]string{"index not ascending and unique at 1", "index not ascending and unique at 2"}},
		{"a reversed pair", Universe{Streams: st, Index: []int{0, 2}, Pairs: [][2]int{{2, 0}}},
			[]string{"pair (2, 0) is not ordered a < b"}},
		{"a self-pair", Universe{Streams: st, Index: []int{1}, Pairs: [][2]int{{1, 1}}},
			[]string{"pair (1, 1) pairs a stream with itself"}},
		{"a pair end outside the index", Universe{Streams: st, Index: []int{0, 1}, Pairs: [][2]int{{0, 4}}},
			[]string{"pair (0, 4): stream 4 is not in the universe"}},
		{"pairs unsorted and duplicate", Universe{Streams: st, Index: []int{0, 1, 2}, Pairs: [][2]int{{1, 2}, {0, 2}, {0, 2}}},
			[]string{"pairs not ascending and unique at 1", "pairs not ascending and unique at 2"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wantErrs(t, tc.u.Validate(), tc.want...)
		})
	}
}
