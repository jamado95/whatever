package strategy

import proto "whatever/internal/protocol"

type Strategy interface {
	ID() string
	Init(data <-chan proto.MarketData) error
	Streams() (<-chan proto.Signal, <-chan error)
	Start() error
	Close()
}
