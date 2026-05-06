package lsmtree

import (
	"context"
	"fmt"
	"sync"

	"github.com/goyal-aman/distributed-storage-nodes/commitlog"
	"github.com/goyal-aman/distributed-storage-nodes/sequencegenerator"
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
		isReplica bool,
	) (*types.Version, error)
}

type KeyFilter func(key string) bool

var (
	AllKeys = func(key string) bool { return true }
)

type Opts func(*LSMTree)

type PostWriteHookCncl func()

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

func NewLSMTree(logPath string) (ILSMTree, error) {
	commitLog, err := commitlog.NewCommitLog(logPath)
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
) (any, error) {
	ctx, span := tracer.Start(ctx, "Get From LSMTree")
	defer span.End()
	d.mu.RLock()
	defer d.mu.RUnlock()
	val, exist := d.inMemStore[key]
	if exist {
		return val.Value, nil
	}
	return nil, nil
}

// Put
// store val against the key. For each put
// a new version is generated using timedSeqGenerator
func (d *LSMTree) Put(
	ctx context.Context,
	key string,
	val []byte,
	isReplica bool,
) (*types.Version, error) {
	ctx, span := tracer.Start(ctx, "Put in LSMTree")
	defer span.End()
	version := d.seqGenerator.Next()
	return d.putRaw(ctx, key, val, version, false)
}

// Put
// store val against the key.
func (d *LSMTree) putRaw(
	ctx context.Context,
	key string,
	val []byte,
	version types.Version,
	isReplica bool, // not being used, figure out how to handle this in commitlog
) (*types.Version, error) {
	ctx, span := tracer.Start(ctx, "PutRaw in LSMTree")
	defer span.End()

	d.mu.Lock()
	err := d.commitLog.Write(ctx, version, key, val)
	if err != nil {
		return nil, fmt.Errorf("error writting to commitlog: %w", err)
	}

	d.inMemStore[key] = types.StoreEntryV2{Value: val, Version: version}

	d.mu.Unlock()

	return &version, nil
}
