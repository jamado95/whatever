package provider

import proto "whatever/internal/protocol"

type DataError struct {
	Code    string
	Message string
}

func (e DataError) Error() string {
	return e.Message
}

type DataProvider interface {
	ID() string
	Init(sub proto.Subscription, limit int) error
	Streams() (<-chan proto.MarketData, <-chan error)
	Start() error
	Close()
}
