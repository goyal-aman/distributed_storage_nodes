package types

import (
	"fmt"
	"time"
)

// NoteState
type NodeState string

var (
	JOINING       NodeState = "JOINING"
	BOOTSTRAPPING NodeState = "BOOTSTRAPPING"
	AVAILABLE     NodeState = "AVAILABLE"
)

type Gossip struct {
	Id            string
	Host          string
	EndOfKeyRange uint64
	LastUpdate    time.Time
	State         NodeState
}

func (g Gossip) XEndOfKeyRange() uint64 {
	return g.EndOfKeyRange
}

func (g Gossip) XState() NodeState {
	return g.State
}

// StorageNode
type StorageNode struct {
	Id string

	// EndOfKeyRange is the first last key in the keyrange node is responsible for
	EndOfKeyRange uint64

	// Host is ip:port
	Host string

	// even though StorageNode is not being used as this point, I've updated
	// because "Just in case"
	State NodeState
}

func (s *StorageNode) String() string {
	return fmt.Sprintf("{endOfKeyRange:%d, status:%s}", s.EndOfKeyRange, "unknown")
}

func (s StorageNode) XEndOfKeyRange() uint64 {
	return s.EndOfKeyRange
}

func (s StorageNode) XState() NodeState {
	return s.State
}

type StoreEntry struct {
	Value interface{}

	// Version
	// why is it uint64? not sure tbh, it feels like
	// for now, bigger value is better.
	Version uint64
}
