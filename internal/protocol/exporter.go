package proto

type Exporter interface {
	ID() string
	Init() error
	Export(md MarketData, snap *Snapshot) error
	Close() error
}
