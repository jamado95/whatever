package reg

import (
	"context"
	"fmt"
	"sync"

	proto "whatever/internal/protocol"
)

var (
	Providers = &registry[proto.DataProvider]{
		factories: make(map[string]Factory[proto.DataProvider]),
	}
	Strategies = &registry[proto.Strategy]{
		factories: make(map[string]Factory[proto.Strategy]),
	}
	Features = &registry[proto.Feature]{
		factories: make(map[string]Factory[proto.Feature]),
	}
	Execution = &registry[proto.Executor]{
		factories: make(map[string]Factory[proto.Executor]),
	}
	Engines = &registry[Runnable]{
		factories: make(map[string]Factory[Runnable]),
	}
)

type Runnable interface {
	Run(ctx context.Context) error
	Close()
}

// Factory creates a component from options
type Factory[T any] func(opts map[string]any) (T, error)

// Registry holds factories for a component type
type registry[T any] struct {
	mu        sync.RWMutex
	factories map[string]Factory[T]
}

func (r *registry[T]) Register(name string, factory Factory[T]) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factories[name] = factory
}

func (r *registry[T]) Create(name string, opts map[string]any) (T, error) {
	r.mu.RLock()
	factory, ok := r.factories[name]
	r.mu.RUnlock()

	var zero T
	if !ok {
		return zero, fmt.Errorf("unknown component: %s", name)
	}
	return factory(opts)
}

func (r *registry[T]) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.factories))
	for name := range r.factories {
		names = append(names, name)
	}
	return names
}

func (r *registry[T]) Has(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.factories[name]
	return ok
}
