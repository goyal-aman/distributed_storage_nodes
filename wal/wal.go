package wal

var (
	walPath = "/wal/wal.txt"
)

type Wal struct {
}

type WalOpts func(*Wal)

func NewWal(opts WalOpts) *Wal {
	return &Wal{}
}
