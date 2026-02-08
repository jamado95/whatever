package proto

import (
	"errors"
	"sort"
	"sync"
)

var ErrZeroTimestamp = errors.New("SortedWindow.Timestamp is zero")

// TimeStamped is the interface for types that can be stored in time-sorted windows
type TimeStamped interface {
	Timestamp() int64
}

// SortedWindow is a fixed-capacity buffer that keeps MarketData sorted by ReceivedAt.
// When full, the oldest entry (smallest ReceivedAt) is evicted on Push.
type SortedWindow[T TimeStamped] struct {
	mu       sync.RWMutex
	data     []T
	capacity int
}

// NewMarketDataBuffer creates a new buffer with the given capacity.
func NewSortedWindow[T TimeStamped](capacity int) *SortedWindow[T] {
	if capacity <= 0 {
		capacity = 1
	}
	return &SortedWindow[T]{
		data:     make([]T, 0, capacity),
		capacity: capacity,
	}
}

// Push inserts MarketData in sorted order by ReceivedAt.
// Returns ErrZeroTimestamp if item.ReceivedAt is nil.
// If buffer is full, the oldest entry is evicted.
func (sw *SortedWindow[T]) Push(item T) error {
	if item.Timestamp() == 0 {
		return ErrZeroTimestamp
	}

	sw.mu.Lock()
	defer sw.mu.Unlock()

	ts := item.Timestamp()

	// Find insertion point using binary search
	idx := sort.Search(len(sw.data), func(i int) bool {
		return sw.data[i].Timestamp() >= ts
	})

	// Insert at idx
	var zero T
	sw.data = append(sw.data, zero)
	copy(sw.data[idx+1:], sw.data[idx:])
	sw.data[idx] = item

	// Evict oldest if over capacity
	if len(sw.data) > sw.capacity {
		sw.data = sw.data[1:]
	}

	return nil
}

// Last returns the most recent n entries (largest ReceivedAt values).
// Returns fewer if buffer has less than n entries.
// The returned slice is a copy, safe for concurrent use.
func (sw *SortedWindow[T]) Last(n int) []T {
	sw.mu.RLock()
	defer sw.mu.RUnlock()

	if n <= 0 {
		return nil
	}
	if n > len(sw.data) {
		n = len(sw.data)
	}

	start := len(sw.data) - n
	result := make([]T, n)
	copy(result, sw.data[start:])
	return result
}

// Len returns the current number of entries in the buffer.
func (sw *SortedWindow[T]) Len() int {
	sw.mu.RLock()
	defer sw.mu.RUnlock()
	return len(sw.data)
}

// Cap returns the buffer capacity.
func (sw *SortedWindow[T]) Cap() int {
	return sw.capacity
}

// Clear removes all entries from the buffer.
func (sw *SortedWindow[T]) Clear() {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.data = sw.data[:0]
}
