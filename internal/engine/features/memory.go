package features

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"unsafe"

	proto "whatever/internal/protocol"
)

// memoryLimit is the estimate above which New warns: 1 GiB.
const memoryLimit = 1 << 30

/*
memoryWarning estimates what New allocates for g — every result ring's slots and each stream's candle
copy — from their sizes, allocating nothing. Above memoryLimit it returns a warning naming the total,
the lookback every result ring holds, and the three composed IDs holding the most memory, ties by ID.
Candle copies count in the total only: they belong to streams, not IDs.
*/
func memoryWarning(g *Graph) (string, bool) {
	type usage struct {
		id    string
		bytes int64
	}
	ids := make([]usage, 0, len(g.columns))
	var total int64
	for id, c := range g.columns {
		b := c.bytes()
		ids = append(ids, usage{id, b})
		total += b
	}
	for _, d := range g.depth {
		total += int64(d) * int64(unsafe.Sizeof(proto.Candle{}))
	}
	if total <= memoryLimit {
		return "", false
	}

	slices.SortFunc(ids, func(a, b usage) int {
		return cmp.Or(cmp.Compare(b.bytes, a.bytes), strings.Compare(a.id, b.id))
	})

	largest := make([]string, 0, 3)
	for _, u := range ids[:min(3, len(ids))] {
		largest = append(largest, fmt.Sprintf("%s %.1f MiB", u.id, float64(u.bytes)/(1<<20)))
	}

	return fmt.Sprintf(
		"features: buffers estimated at %.1f GiB, above %d GiB, at a lookback of %d bars; largest: %s",
		float64(total)/(1<<30), memoryLimit>>30, g.lookback, strings.Join(largest, ", "),
	), true
}
