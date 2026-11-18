package proto

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
)

/*
Universe is a set of streams and pairs over a stream table: what a universe type fills in, in this
canonical form, and what features are placed on. The composition root runs Validate on every universe
a type returns.
*/
type Universe struct {
	Streams *Streams // the table it indexes; shared, never written
	Index   []int    // its streams, indices into Streams.All; ascending, unique
	Pairs   [][2]int // a < b, ascending, unique; both ends in Index
}

/*
Validate returns u's errors: no stream table alone; otherwise each index entry not in the table, an
index not ascending and unique, each pair not ordered a < b or of a stream with itself, pairs not
ascending and unique, and each pair end not in Index.
*/
func (u Universe) Validate() []error {
	if u.Streams == nil {
		return []error{errors.New("no stream table")}
	}

	var errs []error
	all := len(u.Streams.All)
	for i, s := range u.Index {
		if s < 0 || s >= all {
			errs = append(errs, fmt.Errorf("stream %d is not in the table of %d streams", s, all))
		}
		if i > 0 && u.Index[i-1] >= s {
			errs = append(errs, fmt.Errorf("index not ascending and unique at %d", i))
		}
	}

	for i, p := range u.Pairs {
		switch {
		case p[0] == p[1]:
			errs = append(errs, fmt.Errorf("pair (%d, %d) pairs a stream with itself", p[0], p[1]))
		case p[0] > p[1]:
			errs = append(errs, fmt.Errorf("pair (%d, %d) is not ordered a < b", p[0], p[1]))
		}
		if i > 0 && comparePairs(u.Pairs[i-1], p) >= 0 {
			errs = append(errs, fmt.Errorf("pairs not ascending and unique at %d", i))
		}
		ends := p[:]
		if p[0] == p[1] {
			ends = p[:1]
		}
		for _, end := range ends {
			if _, ok := slices.BinarySearch(u.Index, end); !ok {
				errs = append(errs, fmt.Errorf("pair (%d, %d): stream %d is not in the universe", p[0], p[1], end))
			}
		}
	}

	return errs
}

// comparePairs orders pairs by their first stream, then their second.
func comparePairs(a, b [2]int) int {
	return cmp.Or(cmp.Compare(a[0], b[0]), cmp.Compare(a[1], b[1]))
}
