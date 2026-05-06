package sequencegenerator

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/goyal-aman/distributed-storage-nodes/types"
)

// TODO: think of better name. This is actually
// not a counter but a object which returns curr
// version for node.
type ITimedSeqGenerator interface {
	Next() types.Version
}

var _ ITimedSeqGenerator = (*TimedSeqGenerator)(nil)

type TimedSeqGenerator struct {
	mu                    sync.Mutex
	countPerSec           atomic.Uint64
	prevTimeEpochUTCInSec int64
}

func NewAtomicCounter() ITimedSeqGenerator {
	return &TimedSeqGenerator{
		prevTimeEpochUTCInSec: time.Now().Unix(),
	}
}

// Next
// returns timed seq on each call.
// if multiple calls are made in a second, then count value is
// increased monotonically. For example at timestamp 12345 if
// two calls are made then first returns {12345, 1}, and second
// returns {12345, 2}
func (c *TimedSeqGenerator) Next() types.Version {
	c.mu.Lock()
	defer c.mu.Unlock()

	nowEpochUTCInSec := time.Now().Unix()

	// count resets persec
	if c.prevTimeEpochUTCInSec != nowEpochUTCInSec {
		c.countPerSec.Store(0)
		c.prevTimeEpochUTCInSec = nowEpochUTCInSec
	}

	return types.Version{
		TimestampEpochUtcInSec: nowEpochUTCInSec,
		Count:                  c.countPerSec.Add(1),
	}
}
