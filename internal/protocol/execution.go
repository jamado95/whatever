package proto

type Order struct {
	ID        string
	Symbol    string
	Side      Side
	Size      float64
	Price     *float64
	Timestamp int64
	ExpiresAt int64
	Source    string
}

type Fill struct {
	Order     Order
	FillPrice float64
	FillSize  float64
	FilledAt  int64
}

type Executor interface {
	ID() string
	Init(orders <-chan Order) error
	Streams() (<-chan Fill, <-chan error)
	Start() error
	Close()
}
