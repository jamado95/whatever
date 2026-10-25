package timing

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestUnpaced verifies Unpaced never waits and reports a done context.
func TestUnpaced(t *testing.T) {
	p := Unpaced()
	start := time.Now()
	for i := range 1000 {
		if err := p.Wait(context.Background(), int64(i)); err != nil {
			t.Fatalf("Wait %d: %v", i, err)
		}
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Errorf("1000 waits took %v, want no wait", d)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Wait(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Errorf("Wait on a cancelled context = %v, want context.Canceled", err)
	}
}

// TestFixed verifies Fixed lets one frame through per interval, the first after one, and stops
// waiting once the context is cancelled.
func TestFixed(t *testing.T) {
	p := Fixed(10 * time.Millisecond)
	start := time.Now()
	for i := range 5 {
		if err := p.Wait(context.Background(), int64(i)); err != nil {
			t.Fatalf("Wait %d: %v", i, err)
		}
	}
	if d := time.Since(start); d < 45*time.Millisecond {
		t.Errorf("5 waits at 10ms took %v, want at least 45ms", d)
	}

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(10*time.Millisecond, cancel)
	start = time.Now()
	if err := Fixed(time.Hour).Wait(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Errorf("Wait cancelled mid-wait = %v, want context.Canceled", err)
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Errorf("cancelled Wait returned after %v, want within 100ms", d)
	}
}
