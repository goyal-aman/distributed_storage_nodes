package types

import (
	"fmt"

	"github.com/goyal-aman/distributed-storage-nodes/types"
)

type XCompare[T any] interface {
	CmpOrder(T) int
}

type XString interface {
	String() string
}

type LogItem struct {
	Key       string
	Value     []byte
	Version   types.Version
	IsReplica bool
}

// XHasOrder
// x < o : x.XHasOrder(o) < 0
// x == o : x.XHasOrder(o) = 0
// x > 0 : x.XHasOrder(o) > 0
func (l LogItem) CmpOrder(other LogItem) int {
	if l.Version.TimestampEpochUtcInSec < other.Version.TimestampEpochUtcInSec {
		return -1
	} else if l.Version.TimestampEpochUtcInSec > other.Version.TimestampEpochUtcInSec {
		return 1
	} else {
		if l.Version.Count < other.Version.Count {
			return -1
		} else if l.Version.Count == other.Version.Count {
			return 0
		}
		return 1

	}
}

func (l LogItem) String() string {
	return fmt.Sprintf("%s %s %d %d", l.Key, l.Value, l.Version.TimestampEpochUtcInSec, l.Version.Count)
}
