package pipe

import (
	"slices"
	"testing"
	"time"
)

// tagged is a value labelled with the index of the input it came from.
type tagged struct {
	v, in int
}

func lessTagged(a, b tagged) bool { return a.v < b.v }

// feed returns a closed, buffered channel holding vals, each tagged with in.
func feed(in int, vals ...int) <-chan tagged {
	ch := make(chan tagged, len(vals))
	for _, v := range vals {
		ch <- tagged{v, in}
	}
	close(ch)
	return ch
}

func drain[T any](ch <-chan T) []T {
	var out []T
	for v := range ch {
		out = append(out, v)
	}
	return out
}

func TestOrderedFanIn(t *testing.T) {
	tests := []struct {
		name   string
		inputs [][]int
		want   []tagged
	}{
		{"interleaved", [][]int{{1, 4, 7}, {2, 5, 8}, {3, 6, 9}},
			[]tagged{{1, 0}, {2, 1}, {3, 2}, {4, 0}, {5, 1}, {6, 2}, {7, 0}, {8, 1}, {9, 2}}},
		// Equal values go to the earlier input first.
		{"ties", [][]int{{1, 2}, {1, 2}},
			[]tagged{{1, 0}, {1, 1}, {2, 0}, {2, 1}}},
		// The short input closes early; the rest drain in order.
		{"uneven lengths", [][]int{{1}, {2, 3, 4}},
			[]tagged{{1, 0}, {2, 1}, {3, 1}, {4, 1}}},
		{"empty input", [][]int{{}, {1, 2}},
			[]tagged{{1, 1}, {2, 1}}},
		{"no inputs", nil, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ins := make([]<-chan tagged, len(tc.inputs))
			for i, vals := range tc.inputs {
				ins[i] = feed(i, vals...)
			}
			if got := drain(OrderedFanIn(lessTagged, ins...)); !slices.Equal(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestOrderedFanInWaitsForSlowInput: nothing is emitted while an open input has
// no pending value, even if another input holds later values.
func TestOrderedFanInWaitsForSlowInput(t *testing.T) {
	fast := make(chan int, 2)
	fast <- 2
	fast <- 3
	close(fast)
	slow := make(chan int)

	out := OrderedFanIn(func(a, b int) bool { return a < b }, fast, slow)
	select {
	case v := <-out:
		t.Fatalf("emitted %d before the slow input had a value", v)
	case <-time.After(50 * time.Millisecond):
	}

	slow <- 1
	close(slow)
	if got := drain(out); !slices.Equal(got, []int{1, 2, 3}) {
		t.Fatalf("got %v, want [1 2 3]", got)
	}
}
