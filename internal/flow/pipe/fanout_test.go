package pipe

import (
	"fmt"
	"math/rand/v2"
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

/*
TestOrderedFanInModel checks the heap against a reference: many inputs of random length holding
sorted values from a small range, so ties are frequent. The output must equal a stable sort of every
value by value, then input.
*/
func TestOrderedFanInModel(t *testing.T) {
	for seed := range uint64(5) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 0))
			inputs := make([][]int, 50)
			var want []tagged
			for i := range inputs {
				vals := make([]int, rng.IntN(41))
				for j := range vals {
					vals[j] = rng.IntN(20)
				}
				slices.Sort(vals)
				inputs[i] = vals
				for _, v := range vals {
					want = append(want, tagged{v, i})
				}
			}
			slices.SortStableFunc(want, func(a, b tagged) int { return a.v - b.v })

			ins := make([]<-chan tagged, len(inputs))
			for i, vals := range inputs {
				ins[i] = feed(i, vals...)
			}
			if got := drain(OrderedFanIn(lessTagged, ins...)); !slices.Equal(got, want) {
				t.Fatalf("got %v,\nwant %v", got, want)
			}
		})
	}
}

/*
BenchmarkOrderedFanIn measures the merge per value with n inputs that all carry the same sequence, so
every value ties, as streams on one grid do. Inputs are filled and closed before the timer starts.
*/
func BenchmarkOrderedFanIn(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			per := b.N/n + 1
			ins := make([]<-chan int, n)
			for i := range ins {
				ch := make(chan int, per)
				for v := range per {
					ch <- v
				}
				close(ch)
				ins[i] = ch
			}

			b.ReportAllocs()
			b.ResetTimer()
			out := OrderedFanIn(func(a, b int) bool { return a < b }, ins...)
			for range b.N {
				<-out
			}
			b.StopTimer()
			for range out { // drain the remainder so the merge goroutine exits
			}
		})
	}
}
