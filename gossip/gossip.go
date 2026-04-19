package gossip

import (
	"sync"

	"github.com/goyal-aman/distributed-storage-nodes/types"
)

type Cloneable[T any] interface {
	Clone() T
}

// Gossip is thread safe container for gossip of node
// api are designed for correctness and easy reasoning
type Gossip[K comparable, T interface {
	Cloneable[T]
	types.HasState
}] struct {
	mu    sync.RWMutex
	store map[K]T

	// maintains available nodes present
	stateCount map[types.NodeState]int
}

func NewGossip[K comparable, T interface {
	Cloneable[T]
	types.HasState
}]() *Gossip[K, T] {
	return &Gossip[K, T]{
		store:      make(map[K]T),
		stateCount: make(map[types.NodeState]int),
	}
}

func (g *Gossip[K, T]) Get(key K) (*T, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	val, exist := g.store[key]
	if !exist {
		return nil, false
	}
	return &val, true
}

func (g *Gossip[K, T]) Read() map[K]T {
	g.mu.RLock()
	defer g.mu.RUnlock()

	c := map[K]T{}
	for k, v := range g.store {
		c[k] = v.Clone()
	}
	return c
}

func (g *Gossip[K, T]) Upsert(key K, val T) {
	clone := val.Clone()
	g.mu.Lock()
	defer g.mu.Unlock()

	oldVal, exist := g.store[key]
	g.store[key] = clone

	// decr the count of the old state
	if exist {
		g.stateCount[oldVal.XState()] -= 1
	}

	// incr the count of new state
	g.stateCount[clone.XState()] += 1
}

func (g *Gossip[K, T]) Size() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.store)
}

func (g *Gossip[K, T]) NumNodeInState(s types.NodeState) int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	cnt, exist := g.stateCount[s]
	if exist {
		return cnt
	}
	return 0
}
