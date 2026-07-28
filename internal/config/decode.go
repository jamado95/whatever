package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// DepPrefix marks option keys that carry resolved dependency instances injected
// by the composition root rather than values read from the config file.
const DepPrefix = "_"

// Validator is implemented by config structs with constraints that the type
// system cannot express (required fields, cross-field rules). Decode runs it
// after a successful decode.
type Validator interface {
	Validate() error
}

// Decode strictly decodes opts into dst.
//
// Dependency keys (see DepPrefix) are stripped first: they hold live instances
// that are not config and do not survive a JSON round-trip. The remainder is
// re-marshalled and decoded with unknown fields rejected, so a misspelled or
// unsupported key is an error rather than a silently ignored one. Numbers are
// widened by encoding/json, which removes the class of bug where an assertion
// to a type the map can never hold fails silently.
//
// dst must be a non-nil pointer to a struct.
func Decode(opts map[string]any, dst any) error {
	clean := make(map[string]any, len(opts))
	for k, v := range opts {
		if strings.HasPrefix(k, DepPrefix) {
			continue
		}
		clean[k] = v
	}

	raw, err := json.Marshal(clean)
	if err != nil {
		return fmt.Errorf("re-encoding options: %w", err)
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}

	if v, ok := dst.(Validator); ok {
		return v.Validate()
	}
	return nil
}

// OptionsKey is the engine block key holding engine-specific tuning. It is
// nested so that the keys the composition root resolves and the keys the engine
// factory reads form disjoint sets, each strictly validated by one reader.
const OptionsKey = "options"

// DecodeOptions strictly decodes the nested options block of an engine config.
// An absent block decodes as empty, so any genuinely required field still
// surfaces through Validate rather than through a missing-key check here.
func DecodeOptions(opts map[string]any, dst any) error {
	nested, ok := opts[OptionsKey]
	if !ok {
		return Decode(map[string]any{}, dst)
	}

	m, ok := nested.(map[string]any)
	if !ok {
		return fmt.Errorf("%q must be an object, got %T", OptionsKey, nested)
	}
	return Decode(m, dst)
}

// Dep extracts a dependency instance injected under key, reporting a typed
// error rather than yielding a zero value when it is missing or of the wrong
// type.
func Dep[T any](opts map[string]any, key string) (T, error) {
	var zero T

	v, ok := opts[key]
	if !ok {
		return zero, fmt.Errorf("missing dependency %q", key)
	}

	dep, ok := v.(T)
	if !ok {
		return zero, fmt.Errorf("dependency %q is %T, want %T", key, v, zero)
	}
	return dep, nil
}

// OptDep behaves like Dep but reports absence separately from failure, for
// dependencies that are legitimately optional.
func OptDep[T any](opts map[string]any, key string) (T, bool, error) {
	var zero T

	v, ok := opts[key]
	if !ok {
		return zero, false, nil
	}

	dep, ok := v.(T)
	if !ok {
		return zero, false, fmt.Errorf("dependency %q is %T, want %T", key, v, zero)
	}
	return dep, true, nil
}

// Duration is a time.Duration that decodes from a Go duration string such as
// "100ms". JSON has no duration type, and decoding into time.Duration directly
// would silently accept a bare number of nanoseconds.
type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string such as \"100ms\": %w", err)
	}

	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}

	*d = Duration(parsed)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}
