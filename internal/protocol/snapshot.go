package proto

// snapshot.go defines the Snapshot type and its type-safe accessors.
//
// Snapshot stores heterogeneous values in an untyped map and exposes generic
// Get and Set functions keyed by Key[T], allowing callers to read and write
// strongly typed values under a trusted key-to-type invariant.

type Key[T any] struct {
	name string
}

func NewKey[T any](name string) Key[T] {
	return Key[T]{name: name}
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

func NewSnapshot() Snapshot {
	return Snapshot{
		data: make(map[string]any),
	}
}

// key Key[T] allows for type inference snapshot `s` by type key
// this is not a full type safe compile-time check, but its completely safe at runtime
func GetSnapshot[T any](s *Snapshot, key Key[T]) (T, bool) {
	val, ok := s.data[key.name]
	if !ok {
		var zero T
		return zero, false
	}
	return val.(T), true
}

func SetSnapshot[T any](s *Snapshot, key Key[T], val T) {
	s.data[key.name] = val
}

func (s *Snapshot) Data() map[string]any {
	return s.data
}
