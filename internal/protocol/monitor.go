package proto

// REVIEW
type MonitorSubscription struct {
	Subscription Subscription
	Side         Side
	Status       string
	Timestamp    int64
}

type Monitor interface {
	ID() string
	Init(fills <-chan Fill) error
	Streams() (<-chan Fill, <-chan error)
	Start() error
	Close()
}
