package pipe

import (
	"container/heap"
	"sync"
)

// DefaultFanOutBuffer is the per-output capacity used when FanOut is called
// without an explicit size.
const DefaultFanOutBuffer = 1000

// FanOut sends each value from input to all output channels.
// Each output has an independent buffer. Drops oldest if buffer full.
// Closes all outputs when input closes.
func FanOut[T any](in <-chan T, n int, bufferSize ...int) []<-chan T {
	size := DefaultFanOutBuffer
	if len(bufferSize) > 0 {
		size = bufferSize[0]
	}

	// create n broadcast channels with same size buffers
	outs := make([]chan T, n)
	for i := range outs {
		outs[i] = make(chan T, size)
	}

	go func() {
		defer func() {
			for _, out := range outs {
				close(out)
			}
		}()

		for val := range in {
			for _, out := range outs {
				select {
				case out <- val:
				default:
					// Buffer full - drop oldest
					select {
					case <-out:
					default:
					}
					out <- val
				}
			}
		}
	}()

	// Convert to receive-only
	result := make([]<-chan T, n)
	for i, out := range outs {
		result[i] = out
	}
	return result
}

// FanIn combines multiple inputs into a single output.
// Output closes when all inputs close.
// No ordering guarantees - values emitted as they arrive.
func FanIn[T any](ins ...<-chan T) <-chan T {
	out := make(chan T)

	// track completion of all inputs
	var wg sync.WaitGroup
	wg.Add(len(ins))

	for _, in := range ins {
		go func(ch <-chan T) {
			defer wg.Done()
			for val := range ch {
				out <- val
			}
		}(in)
	}

	go func() {
		// wait for all inputs to close to close(out)
		wg.Wait()
		close(out)
	}()

	return out
}

// OrderedFanIn merges inputs that are each already ordered by less into one
// ordered output: it holds one pending value per open input and emits the
// least, ties going to the earlier input. Blocks while any open input has no
// pending value, so a silent input stalls the merge. Output closes when all
// inputs close. Pending values sit in a heap: O(log n) per value.
func OrderedFanIn[T any](less func(a, b T) bool, ins ...<-chan T) <-chan T {
	out := make(chan T)

	go func() {
		defer close(out)

		h := &pendingHeap[T]{items: make([]pending[T], 0, len(ins)), less: less}
		for i, in := range ins {
			if v, ok := <-in; ok {
				h.items = append(h.items, pending[T]{v, i})
			}
		}
		heap.Init(h)

		for h.Len() > 0 {
			top := &h.items[0]
			out <- top.v
			// refill in place: replacing the root and fixing it does not allocate
			if v, ok := <-ins[top.in]; ok {
				top.v = v
				heap.Fix(h, 0)
			} else {
				heap.Pop(h)
			}
		}
	}()

	return out
}

// pending is one input's value awaiting emission.
type pending[T any] struct {
	v  T
	in int
}

// pendingHeap orders pending values by less, ties to the earlier input.
type pendingHeap[T any] struct {
	items []pending[T]
	less  func(a, b T) bool
}

func (h *pendingHeap[T]) Len() int { return len(h.items) }

func (h *pendingHeap[T]) Less(i, j int) bool {
	a, b := &h.items[i], &h.items[j]
	return h.less(a.v, b.v) || !h.less(b.v, a.v) && a.in < b.in
}

func (h *pendingHeap[T]) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

func (h *pendingHeap[T]) Push(x any) { h.items = append(h.items, x.(pending[T])) }

func (h *pendingHeap[T]) Pop() any {
	n := len(h.items) - 1
	x := h.items[n]
	h.items = h.items[:n]
	return x
}

// FanInTo combines multiple inputs into a single output.
// Output closes when all inputs close.
// No ordering guarantees - values emitted as they arrive.
func FanInTo[T any](out chan<- T, ins ...<-chan T) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(len(ins))

	for _, in := range ins {
		go func(ch <-chan T) {
			defer wg.Done()
			for v := range ch {
				out <- v
			}
		}(in)
	}

	return &wg
}
