package pipeline

import (
	"context"
	"time"
)

// Map transforms each value from input.
// Output closes when input closes.
func Map[T any, U any](in <-chan T, fn func(T) U) <-chan U {
	out := make(chan U)

	go func() {
		defer close(out)
		for val := range in {
			out <- fn(val)
		}
	}()

	return out
}

// MapErr transforms with possible error.
// Errors routed to error channel, successful values to output.
// Both channels close when input closes.
func MapErr[T any, U any](in <-chan T, fn func(T) (U, error)) (<-chan U, <-chan error) {
	out := make(chan U)
	errs := make(chan error)

	go func() {
		defer close(out)
		defer close(errs)
		for val := range in {
			result, err := fn(val)
			if err != nil {
				errs <- err
			} else {
				out <- result
			}
		}
	}()

	return out, errs
}

// Filter passes through values where predicate returns true.
// Output closes when input closes.
func Filter[T any](in <-chan T, pred func(T) bool) <-chan T {
	out := make(chan T)

	go func() {
		defer close(out)
		for val := range in {
			if pred(val) {
				out <- val
			}
		}
	}()

	return out
}

// ForEach consumes channel, calling fn for each value.
// Blocks until input closes.
func ForEach[T any](in <-chan T, fn func(T)) {
	for val := range in {
		fn(val)
	}
}

// Forward consumes input, calling fn for each value. Non-blocking, returns immediately.
// Returns a done channel that closes when input is exhausted.
func Forward[T any](in <-chan T, fn func(T)) <-chan struct{} {
	done := make(chan struct{})

	go func() {
		defer close(done)
		for val := range in {
			fn(val)
		}
	}()

	return done
}

// Tap passes through all values while also calling fn (for logging, metrics).
// Output closes when input closes.
func Tap[T any](in <-chan T, fn func(T)) <-chan T {
	out := make(chan T)

	go func() {
		defer close(out)
		for val := range in {
			fn(val)
			out <- val
		}
	}()

	return out
}

// Batch collects values into slices.
// Emits when batch is full or timeout expires (whichever comes first).
// Output closes when input closes (emits partial batch if any).
func Batch[T any](in <-chan T, size int, timeout time.Duration) <-chan []T {
	out := make(chan []T)

	go func() {
		defer close(out)

		batch := make([]T, 0, size)
		timer := time.NewTimer(timeout)
		timer.Stop()

		flush := func() {
			if len(batch) > 0 {
				out <- batch
				batch = make([]T, 0, size)
			}
			timer.Stop()
		}

		for {
			select {
			case val, ok := <-in:
				if !ok {
					flush()
					return
				}

				if len(batch) == 0 {
					timer.Reset(timeout)
				}

				batch = append(batch, val)

				if len(batch) >= size {
					flush()
				}

			case <-timer.C:
				flush()
			}
		}
	}()

	return out
}

// WithContext wraps a channel to respect context cancellation.
// Output closes when input closes OR context is cancelled.
func WithContext[T any](ctx context.Context, in <-chan T) <-chan T {
	out := make(chan T)

	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case val, ok := <-in:
				if !ok {
					return
				}
				select {
				case <-ctx.Done():
					return
				case out <- val:
				}
			}
		}
	}()

	return out
}

// Collect drains the entire channel into a slice.
// Blocks until input closes.
func Collect[T any](in <-chan T) []T {
	var result []T
	for val := range in {
		result = append(result, val)
	}
	return result
}

// Take passes through at most n values then closes.
func Take[T any](in <-chan T, n int) <-chan T {
	out := make(chan T)

	go func() {
		defer close(out)
		count := 0
		for val := range in {
			if count >= n {
				return
			}
			out <- val
			count++
		}
	}()

	return out
}
