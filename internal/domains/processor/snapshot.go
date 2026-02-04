package processor

type Key[T any] struct {
	name string
}

func (k Key[T]) Name() string { return k.name }

type KeyRef struct {
	Name string
}

func (k Key[T]) Ref() KeyRef {
	return KeyRef{Name: k.name}
}

type Snapshot struct {
	data map[string]any
}

func NewSnapshot() *Snapshot {
	return &Snapshot{data: make(map[string]any)}
}

// key Key[T] allows for type inference snapshot `s` by type key
// this is not a full type safe compile-time check, but its completely safe at runtime
func Get[T any](s *Snapshot, key Key[T]) (T, bool) {
	val, ok := s.data[key.name]
	if !ok {
		var zero T
		return zero, false
	}
	return val.(T), true
}

func Set[T any](s *Snapshot, key Key[T], val T) {
	s.data[key.name] = val
}
