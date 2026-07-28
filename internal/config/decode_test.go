package config

import (
	"errors"
	"strings"
	"testing"
	"time"
)

type sample struct {
	Period    int      `json:"period"`
	Smoothing float64  `json:"smoothing"`
	Name      string   `json:"name"`
	Interval  Duration `json:"interval"`
}

type validated struct {
	Period int `json:"period"`
}

var errNoPeriod = errors.New("period is required")

func (v *validated) Validate() error {
	if v.Period == 0 {
		return errNoPeriod
	}
	return nil
}

func TestDecodeWidensJSONNumbers(t *testing.T) {
	// config.Load produces float64 for every JSON number; decoding must place
	// them in the declared field type rather than failing an assertion.
	var got sample
	if err := Decode(map[string]any{"period": float64(14)}, &got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Period != 14 {
		t.Errorf("Period = %d, want 14", got.Period)
	}
}

func TestDecodeRejectsUnknownField(t *testing.T) {
	var got sample
	err := Decode(map[string]any{"period": float64(14), "smoothingg": 2.0}, &got)
	if err == nil {
		t.Fatal("expected an error for an unknown field, got nil")
	}
	if !strings.Contains(err.Error(), "smoothingg") {
		t.Errorf("error should name the offending key, got: %v", err)
	}
}

func TestDecodeStripsDependencyKeys(t *testing.T) {
	// Dependency keys hold live instances that neither round-trip through JSON
	// nor correspond to any declared field.
	type dep struct{ ch chan int }

	var got sample
	opts := map[string]any{
		"period":    float64(20),
		"_provider": &dep{ch: make(chan int)},
		"_features": []any{&dep{}, &dep{}},
	}
	if err := Decode(opts, &got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Period != 20 {
		t.Errorf("Period = %d, want 20", got.Period)
	}
}

func TestDecodeRejectsWrongType(t *testing.T) {
	var got sample
	err := Decode(map[string]any{"period": "fourteen"}, &got)
	if err == nil {
		t.Fatal("expected an error for a string in an int field, got nil")
	}
}

func TestDecodeRunsValidate(t *testing.T) {
	var got validated
	err := Decode(map[string]any{}, &got)
	if !errors.Is(err, errNoPeriod) {
		t.Fatalf("expected Validate's error to propagate, got: %v", err)
	}

	if err := Decode(map[string]any{"period": float64(5)}, &got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
}

func TestDecodeLeavesDefaultsForAbsentFields(t *testing.T) {
	got := sample{Smoothing: 2.0}
	if err := Decode(map[string]any{"period": float64(20)}, &got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Smoothing != 2.0 {
		t.Errorf("Smoothing = %v, want the 2.0 default to survive", got.Smoothing)
	}
}

func TestDurationDecoding(t *testing.T) {
	var got sample
	if err := Decode(map[string]any{"interval": "100ms"}, &got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Interval.Duration() != 100*time.Millisecond {
		t.Errorf("Interval = %v, want 100ms", got.Interval.Duration())
	}
}

func TestDurationRejectsBareNumber(t *testing.T) {
	var got sample
	// A bare number would otherwise be read as nanoseconds, which is never what
	// a config author means.
	if err := Decode(map[string]any{"interval": float64(100)}, &got); err == nil {
		t.Fatal("expected an error for a numeric duration, got nil")
	}
}

func TestDurationRejectsGarbage(t *testing.T) {
	var got sample
	if err := Decode(map[string]any{"interval": "100 milliseconds"}, &got); err == nil {
		t.Fatal("expected an error for an unparseable duration, got nil")
	}
}

func TestDep(t *testing.T) {
	type provider interface{ ID() string }

	opts := map[string]any{"_n": 42}

	n, err := Dep[int](opts, "_n")
	if err != nil {
		t.Fatalf("Dep: %v", err)
	}
	if n != 42 {
		t.Errorf("got %d, want 42", n)
	}

	if _, err := Dep[int](opts, "_missing"); err == nil {
		t.Error("expected an error for a missing dependency")
	}
	if _, err := Dep[provider](opts, "_n"); err == nil {
		t.Error("expected an error for a dependency of the wrong type")
	}
}

func TestOptDep(t *testing.T) {
	opts := map[string]any{"_n": 42}

	if _, ok, err := OptDep[int](opts, "_missing"); err != nil || ok {
		t.Errorf("absent optional dependency: got ok=%v err=%v, want ok=false err=nil", ok, err)
	}

	n, ok, err := OptDep[int](opts, "_n")
	if err != nil || !ok || n != 42 {
		t.Errorf("got n=%d ok=%v err=%v, want 42/true/nil", n, ok, err)
	}

	// Present but wrong type is a mistake, not an absence.
	if _, _, err := OptDep[string](opts, "_n"); err == nil {
		t.Error("expected an error for an optional dependency of the wrong type")
	}
}
