package proto

type Signal struct {
	Symbol    string
	Side      Side
	Timestamp int64
	ExpiresAt int64
	Source    string
	Metadata  map[string]any
}

type Strategy interface {
	ID() string
	Init(data <-chan MarketData) error
	Streams() (<-chan Signal, <-chan error)
	Start() error
	Close()
}
