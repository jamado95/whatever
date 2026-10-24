package provider

import (
	"whatever/internal/logger"
	proto "whatever/internal/protocol"
)

// baseProvider holds what every provider shares: its ID, subscription, output channels and logger.
type baseProvider struct {
	id     string
	sub    proto.Subscription
	data   chan proto.MarketData
	errs   chan error
	logger *logger.Logger
}

func (b *baseProvider) ID() string {
	return b.id
}

func (b *baseProvider) Subscription() proto.Subscription {
	return b.sub
}

func (b *baseProvider) Streams() (<-chan proto.MarketData, <-chan error) {
	return b.data, b.errs
}
