package proto

// ResolvedUniverse is a universe's streams and pairs as indices into the stream table's All.
type ResolvedUniverse struct {
	Streams []int    // ascending
	Pairs   [][2]int // a < b, ascending
}
