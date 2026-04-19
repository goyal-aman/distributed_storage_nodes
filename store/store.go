package store

type Store interface {
	// Get get by key
	Get(key string) (any, error)

	GetValAndVersion(key string) (any, uint64, error)

	// Put store value againt key
	Put(key string, value interface{}) (uint64, error)

	PutRaw(key string, val any, version *uint64, isReplica bool) (uint64, error)

	Snapshot(filter KeyFilter, opts ...Opts) (Snapshot, PostWriteHookCncl)

	Size() int
}
