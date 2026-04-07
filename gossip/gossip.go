package gossip

import (
	"sync"
)

type Cloneable[T any] interface {
	Clone() T
}

// Gossip is thread safe container for gossip of node
// api are designed for correctness and easy reasoning
type Gossip[K comparable, T interface{ Cloneable[T] }] struct {
	mu    sync.RWMutex
	store map[K]T
}

func NewGossip[K comparable, T interface{ Cloneable[T] }]() *Gossip[K, T] {
	return &Gossip[K, T]{
		store: make(map[K]T),
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
	g.store[key] = clone
}

func (g *Gossip[K, T]) Size() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.store)
}
