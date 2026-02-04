package pipeline

import "sync"

// FanOut sends each value from input to all output channels.
// Each output has an independent buffer. Drops oldest if buffer full.
// Closes all outputs when input closes.
func FanOut[T any](in <-chan T, n int, bufferSize int) []<-chan T {

	// create n broadcast channels with same size buffers
	outs := make([]chan T, n)
	for i := range outs {
		outs[i] = make(chan T, bufferSize)
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
