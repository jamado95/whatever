package provider

import "fmt"

type ProviderFactory func(opts map[string]any) (DataProvider, error)

var registry = make(map[string]ProviderFactory)

func Register(name string, factory ProviderFactory) {
	registry[name] = factory
}

func Create(name string, opts map[string]any) (DataProvider, error) {
	factory, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("unknown provider: %s", name)
	}
	return factory(opts)
}

func List() []string {
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	return names
}
