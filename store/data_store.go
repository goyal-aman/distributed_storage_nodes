package store

import (
	"sync"

	"github.com/goyal-aman/distributed-storage-nodes/types"
)

type DataStore struct {
	// mu
	// any read and write operation in store
	// must take lock before operations begins
	// this is needed for snapshop api, which
	// requires point in time view of store
	mu sync.RWMutex

	// store
	// against each key there are multiple version
	// of values stored. At this point, I've
	// not given much though on GC of old version.
	// Most recent verions are always stored in end of slice
	store map[string][]types.StoreEntry
}

func (d *DataStore) Get(key string) (any, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	val, exist := d.store[key]
	latestVersion := val[len(val)-1]
	if exist {
		return latestVersion.Value, nil
	}
	return nil, nil
}

// Put
// store val against the key. if key already exist in the store
// then find the most recent version and add new entry with most_recent_version+1
// (key, val, most_recent_version+1)
func (d *DataStore) Put(key string, val any) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	entries, exist := d.store[key]
	if !exist {
		d.store[key] = []types.StoreEntry{
			{Value: val, Version: 1},
		}
		return nil
	}

	mostRecentEntry := entries[len(entries)-1]
	entries = append(entries, types.StoreEntry{
		Value:   val,
		Version: mostRecentEntry.Version + 1,
	})

	d.store[key] = entries

	return nil
}

// Snapshot return point-in-time view of store
// return is map[string][]types.StoreEntry
func (d *DataStore) Snapshot() map[string][]types.StoreEntry {
	d.mu.RLock()
	defer d.mu.RUnlock()

	deepCopy := make(map[string][]types.StoreEntry, len(d.store))
	// create deep copy of existing store

	for k, v := range d.store {
		newSlice := make([]types.StoreEntry, len(v))
		for idx, item := range v {
			newSlice[idx] = types.StoreEntry{Value: item.Value, Version: item.Version}
		}
		deepCopy[k] = newSlice
	}
	return deepCopy
}
