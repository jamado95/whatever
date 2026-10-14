package pipe

import "sync"

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
// inputs close.
func OrderedFanIn[T any](less func(a, b T) bool, ins ...<-chan T) <-chan T {
	out := make(chan T)

	go func() {
		defer close(out)

		heads := make([]T, len(ins))
		has := make([]bool, len(ins))
		open := make([]bool, len(ins))
		for i := range open {
			open[i] = true
		}

		for {
			// refill every open input without a pending value
			for i, in := range ins {
				if !open[i] || has[i] {
					continue
				}
				v, ok := <-in
				if !ok {
					open[i] = false
					continue
				}
				heads[i], has[i] = v, true
			}

			least := -1
			for i := range heads {
				if has[i] && (least < 0 || less(heads[i], heads[least])) {
					least = i
				}
			}
			if least < 0 {
				return // all inputs closed and drained
			}

			out <- heads[least]
			has[least] = false
		}
	}()

	return out
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
