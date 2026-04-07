package store

import (
	"log/slog"
	"sort"
	"sync"

	"github.com/goyal-aman/distributed-storage-nodes/types"
)

// Compile time check
var _ Store = (*DataStore)(nil)

type KeyFilter func(key string) bool

var (
	AllKeys = func(key string) bool { return true }
)

type Opts func(*DataStore)
type Snapshot map[string][]types.StoreEntry
type PostWriteHookCncl func()

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

	// postWriteHook
	// after a Put is success, if postWriteHook is set
	// key, value and latest version is sent to postWriteHook
	// this is used to enable replication. when replication
	// request from new-node comes for point-in-time snapshot
	// the Snapshot api, sets postWriteHook. Depending on the
	// the implementatin of postWriteHook it can be sync or async.
	// Snapshot api returns callback, which when called makes
	// postWriteHook nil, thus stopping write replication
	postWriteHook func(key string, val any, version uint64)
}

func NewDataStore() Store {
	return &DataStore{
		store: make(map[string][]types.StoreEntry),
	}
}

func (d *DataStore) Get(key string) (any, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	val, exist := d.store[key]
	if exist {
		latestVersion := val[len(val)-1]
		return latestVersion.Value, nil
	}
	return nil, nil
}


// Put
// store val against the key. if key already exist in the store
// then find the most recent version and add new entry with most_recent_version+1
// (key, val, most_recent_version+1)
func (d *DataStore) Put(key string, val any) error {
	return d.PutRaw(key, val, nil)
}

func (d *DataStore) PutRaw(key string, val any, version *uint64) error {
	d.mu.Lock()

	entries, exist := d.store[key]
	var newVersion uint64
	if !exist {
		newVersion = 1
		d.store[key] = []types.StoreEntry{
			{Value: val, Version: newVersion},
		}
	} else {
		mostRecentEntry := entries[len(entries)-1]
		newVersion = mostRecentEntry.Version + 1
		if version != nil {
			newVersion = *version
		}
		entries = append(entries, types.StoreEntry{
			Value:   val,
			Version: newVersion,
		})
		d.store[key] = entries
	}
	// I know its not optimised
	ar := d.store[key]
	sort.Slice(d.store[key], func(a, b int) bool {
		return ar[a].Version < ar[b].Version
	})

	// hook runs out side the lock.
	// why? because hook is user supplied implementation.
	// which can block puts indefinitely. By putting hook
	// outside lock, slow hook execution wont bog down the
	// performance of Put
	hook := d.postWriteHook
	d.mu.Unlock()

	if hook != nil {
		d.postWriteHook(key, val, newVersion)
	}
	return nil
}

// Snapshot return point-in-time view of store
// return is map[string][]types.StoreEntry and postWriteHookCncn
// when snapshot is requested, it also has option to set post write hook.
// when post write hook is set, all new changes are streamed to postWriteHook.
// if a change is streamed via postWriteHook, it may also be present is Snapshot
func (d *DataStore) Snapshot(
	filter KeyFilter,
	opts ...Opts,
) (Snapshot, PostWriteHookCncl) {
	// need write lock to populate postWriteHook
	// after unlock all the new writes are being
	// replicated
	d.mu.Lock()
	for _, opt := range opts {
		opt(d)
	}
	d.mu.Unlock()

	resetPostWriteHook := func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		d.postWriteHook = nil
		slog.Info("postWriteHookReset complete")
	}

	d.mu.RLock()
	defer d.mu.RUnlock()

	// create deep copy of existing store
	deepCopy := make(map[string][]types.StoreEntry, len(d.store))
	for k, v := range d.store {
		if !filter(k) {
			continue
		}
		newSlice := make([]types.StoreEntry, len(v))
		for idx, item := range v {
			newSlice[idx] = types.StoreEntry{Value: item.Value, Version: item.Version}
		}
		deepCopy[k] = newSlice
	}
	return deepCopy, resetPostWriteHook

}

func (d *DataStore) Size() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.store)
}

func WithPostWriteHook(hook func(key string, val any, version uint64)) Opts {
	return func(d *DataStore) {
		d.postWriteHook = hook
	}
}