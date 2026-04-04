package types

import (
	"fmt"
	"time"
)

type Gossip struct {
	Id            string
	Host          string
	EndOfKeyRange uint64
	LastUpdate    time.Time
}

func (g Gossip) XEndOfKeyRange() uint64 {
	return g.EndOfKeyRange
}

// StorageNode
type StorageNode struct {
	Id string

	// EndOfKeyRange is the first last key in the keyrange node is responsible for
	EndOfKeyRange uint64

	// Host is ip:port
	Host string
}

func (s *StorageNode) String() string {
	return fmt.Sprintf("{endOfKeyRange:%d, status:%s}", s.EndOfKeyRange, "unknown")
}

func (s StorageNode) XEndOfKeyRange() uint64 {
	return s.EndOfKeyRange
}
