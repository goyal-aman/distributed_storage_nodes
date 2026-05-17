package lsmtree

import (
	"context"
	"fmt"
	"sync"

	"github.com/goyal-aman/distributed-storage-nodes/commitlog"
	"github.com/goyal-aman/distributed-storage-nodes/sequencegenerator"
	"github.com/goyal-aman/distributed-storage-nodes/store"
	"github.com/goyal-aman/distributed-storage-nodes/types"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("github.com/goyal-aman/distributed_storage_nodes/lsmtree")

// Compile time check
// var _ Store = (*DataStore)(nil)

type ILSMTree interface {
	Put(
		ctx context.Context,
		key string,
		val []byte,
	) (types.Version, error)

	PutRaw(
		ctx context.Context,
		key string,
		val []byte,
		version types.Version,
		isReplica bool,
	) error

	Get(
		ctx context.Context,
		key string,
	) ([]byte, *types.Version, error)

	Snapshot(
		ctx context.Context,
		filter store.KeyFilter,
		opts ...store.Opts) (store.Snapshot, store.PostWriteHookCncl)

	Size() int
}

var (
	AllKeys = func(key string) bool { return true }
)

// compile time check
var _ ILSMTree = (*LSMTree)(nil)

type LSMTree struct {
	// mu
	// any read and write operation in store
	// must take lock before operations begins
	// this is needed for snapshop api, which
	// requires point in time view of store
	mu sync.RWMutex

	seqGenerator sequencegenerator.ITimedSeqGenerator
	commitLog    commitlog.ICommitLog

	// inMemStore
	// against each key there are multiple version
	// of values stored. At this point, I've
	// not given much though on GC of old version.
	// Most recent verions are always stored in end of slice
	inMemStore map[string]types.StoreEntryV2
}

func NewLSMTree(
	logPathPrefix string,
	fileNameSuffix string,
) (ILSMTree, error) {
	fileName := fmt.Sprintf("%s/commit_log%s.log", logPathPrefix, fileNameSuffix)
	commitLog, err := commitlog.NewCommitLog(fileName)
	if err != nil {
		return nil, err
	}

	return &LSMTree{
		seqGenerator: sequencegenerator.NewAtomicCounter(),
		commitLog:    commitLog,
		inMemStore:   make(map[string]types.StoreEntryV2),
	}, nil
}

// Get
// returns the value of key. if key not found
// returns nil. If there is any error during get
// operation then returns err
// TODO: currently get only checks in inMemStore.
// it needs to look in SSTable as well.
func (d *LSMTree) Get(
	ctx context.Context,
	key string,
) ([]byte, *types.Version, error) {
	ctx, span := tracer.Start(ctx, "Get From LSMTree")
	defer span.End()
	d.mu.RLock()
	defer d.mu.RUnlock()
	val, exist := d.inMemStore[key]
	if exist {
		return val.Value, &val.Version, nil
	}
	return nil, nil, nil
}

// Put
// store val against the key. For each put
// a new version is generated using timedSeqGenerator
func (d *LSMTree) Put(
	ctx context.Context,
	key string,
	val []byte,
) (types.Version, error) {
	ctx, span := tracer.Start(ctx, "Put in LSMTree")
	defer span.End()
	version := d.seqGenerator.Next()
	if err := d.PutRaw(ctx, key, val, version, false); err != nil {
		var zero types.Version
		return zero, err
	}
	return version, nil
}

// Put
// store val against the key.
func (d *LSMTree) PutRaw(
	ctx context.Context,
	key string,
	val []byte,
	version types.Version,
	isReplica bool, // not being used, figure out how to handle this in commitlog
) error {
	ctx, span := tracer.Start(ctx, "PutRaw in LSMTree")
	defer span.End()

	d.mu.Lock()
	err := d.commitLog.Write(ctx, version, key, val)
	if err != nil {
		return fmt.Errorf("error writting to commitlog: %w", err)
	}

	d.inMemStore[key] = types.StoreEntryV2{Value: val, Version: version}

	d.mu.Unlock()

	return nil
}

// Snapshot
// this is a placeholder method to replace store.DataStore
// in node. I'll this about how to implement snapshot
// in LSMTree later also whether or not its even needed
func (l *LSMTree) Snapshot(
	ctx context.Context,
	filter store.KeyFilter,
	opts ...store.Opts) (store.Snapshot, store.PostWriteHookCncl) {
	return nil, nil
}

// Size
// returns num keys in inMemStore for now
// to make it compatible with store.DataStore.
// Ideally it should return the total number of
// unique keys available in LSMTree
func (l *LSMTree) Size() int {
	return len(l.inMemStore)
}
