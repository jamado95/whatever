package timing

import (
	"sync"
	"time"
)

// Ticker controls when values are released downstream.
type Ticker[T any] interface {
	// Gate wraps a channel with timing control.
	// Returns a channel that emits values according to the ticker's timing rules.
	Gate(in <-chan T) <-chan T

	// Tick manually releases the next value (for Manual ticker or step-through).
	Tick()

	// SetFactor adjusts simulation speed (for Simulated ticker).
	// factor=1 is realtime, factor=3600 means 1 hour of data per second.
	SetFactor(factor float64)

	// Start begins automatic ticking.
	Start()

	// Stop pauses automatic ticking.
	Stop()
}

// realtime lets everything through immediately.
type realtime[T any] struct{}

func Realtime[T any]() Ticker[T] {
	return &realtime[T]{}
}

func (r *realtime[T]) Gate(in <-chan T) <-chan T {
	return in
}

func (r *realtime[T]) Tick()                    {}
func (r *realtime[T]) SetFactor(factor float64) {}
func (r *realtime[T]) Start()                   {}
func (r *realtime[T]) Stop()                    {}

// fixedInterval releases one value per interval.
type fixedInterval[T any] struct {
	interval time.Duration
	running  bool
	mu       sync.Mutex
	tick     chan struct{}
}

func FixedInterval[T any](interval time.Duration) Ticker[T] {
	return &fixedInterval[T]{
		interval: interval,
		tick:     make(chan struct{}, 1),
	}
}

/*
Timed gate that allows values to pass through at a fixed interval.
Creates controlled backpressure that propagates upstream
*/
func (f *fixedInterval[T]) Gate(in <-chan T) <-chan T {
	out := make(chan T)

	go func() {
		defer close(out)
		for val := range in {
			// ticker sync point
			<-f.tick
			out <- val
		}
	}()

	return out
}

func (f *fixedInterval[T]) Tick() {
	select {
	case f.tick <- struct{}{}:
	default:
	}
}

func (f *fixedInterval[T]) SetFactor(factor float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Adjust interval based on factor (higher factor = shorter interval)
	if factor > 0 {
		f.interval = time.Duration(float64(f.interval) / factor)
	}
}

func (f *fixedInterval[T]) Start() {
	f.mu.Lock()
	if f.running {
		f.mu.Unlock()
		return
	}
	f.running = true
	f.mu.Unlock()

	go func() {
		ticker := time.NewTicker(f.interval)
		defer ticker.Stop()

		for {
			f.mu.Lock()
			if !f.running {
				f.mu.Unlock()
				return
			}
			f.mu.Unlock()

			<-ticker.C
			f.Tick()
		}
	}()
}

func (f *fixedInterval[T]) Stop() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.running = false
}
