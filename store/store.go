package store

type Store interface {
	// Get get by key
	Get(key string) (any, error)

	// Put store value againt key
	Put(key string, value interface{}) error

	Snapshot(opts ...Opts) (Snapshot, PostWriteHookCncl)

	Size() int
}
