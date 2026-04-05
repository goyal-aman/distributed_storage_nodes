package store

import "github.com/goyal-aman/distributed-storage-nodes/types"

type Store interface {
	// Get get by key
	Get(key string) interface{}

	// Put store value againt key
	Put(key string, value interface{}) error

	Snapshot() map[string][]types.StoreEntry
}
