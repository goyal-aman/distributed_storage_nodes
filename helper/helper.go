package helper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"slices"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goyal-aman/distributed-storage-nodes/err"
	"github.com/goyal-aman/distributed-storage-nodes/types"
)

func ToBytesReader(a any) *bytes.Reader {
	dataBytes, _ := json.Marshal(a)
	return bytes.NewReader(dataBytes)
}

func HashKey(key string, mod uint64) uint64 {
	// this returns 256 bit number
	// for our usecase we only care about 64 bits
	sum := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(sum[:8]) % mod
}

func GetNNode[T interface {
	types.HasEndOfKeyRange
	types.HasState
}](
	node []T,
	token uint64,
	N int,
) ([]T, error) {
	// N must be <= len(node)
	if N > len(node) {
		return nil, err.ErrNotEnoughNodes
	}

	startNodeIdx := -1
	for i, node := range node {
		// only available nodes are considered for traffic routing
		if node.XState() == types.AVAILABLE &&
			token < node.XEndOfKeyRange() {
			startNodeIdx = i
			break
		}
	}

	// required node not found
	if startNodeIdx == -1 {
		return nil, err.ErrNoNodeWithRequiredEndOfKeyRange
	}

	nodes := make([]T, 0, N)
	for len(nodes) < N {
		currIdx := (startNodeIdx + len(nodes)) % len(node)
		nodes = append(nodes, node[currIdx])
	}

	return nodes, nil
}

// GetNode
// takes in any type which has XEndOfKeyRange() and State()
// defined. find the node which owns the the token (token=hash(key))
// returns only nodes with XState() == types.AVAILABLE
// nodes must be in increases order of EndOfKeyRange
func GetNode[T interface {
	types.HasEndOfKeyRange
	types.HasState
}](
	nodes []T,
	token uint64) (*T, error) {
	for _, node := range nodes {

		// only available nodes are considered for traffic routing
		if node.XState() == types.AVAILABLE &&
			token < node.XEndOfKeyRange() {
			return &node, nil
		}
	}
	return nil, err.ErrNoNodeWithRequiredEndOfKeyRange
}

func StrToUInt64(s string) (uint64, error) {
	return strconv.ParseUint(s, 10, 64)
}

func MapToArr[K comparable, V any](m map[K]V) []V {
	arr := []V{}
	for _, val := range m {
		arr = append(arr, val)
	}
	return arr
}

// MaxTime
// return t1 if t1>t2 otherwise t2
func MaxTime(t1, t2 time.Time) time.Time {
	if t1.After(t2) {
		slog.Debug("maxTime", "winner", t1, "a", t1, "b", t2)
		return t1
	}
	slog.Debug("maxTime", "winner", t2, "a", t1, "b", t2)
	return t2
}

// GetNodeByToken
// returns the next available node which handles the token (token = hash(key))
// nodes must be in increases order of EndOfKeyRange
func GetNodeByToken[T interface {
	types.HasEndOfKeyRange
	types.HasState
}](nodes []T, token uint64) (*T, error) {
	for _, node := range nodes {
		if node.XState() == types.AVAILABLE &&
			token <= node.XEndOfKeyRange() {
			return &node, nil
		}
	}
	return nil, err.ErrNoNodeWithRequiredEndOfKeyRange
}

// GetNodeForReplication
// for a given owner node it returns a another
// node n which should follow writes of owner node
func GetNodeForReplication[T interface {
	types.HasEndOfKeyRange
	types.HasState
}](nodes []T, eokr uint64) *T {
	for _, n := range slices.Backward(nodes) {
		if n.XEndOfKeyRange() < eokr &&
			n.XState() == types.BOOTSTRAPPING {
			return &n
		}
	}
	return nil
}

func SortNodeInPlace[T interface{ types.HasEndOfKeyRange }](ar []T) {
	sort.Slice(ar, func(a, b int) bool {
		// increasing order in EndOfKeyRange
		return ar[a].XEndOfKeyRange() < ar[b].XEndOfKeyRange()
	})

}

type container[R any] struct {
	R R
	E error
}

func (c container[R]) String() string {
	errStr := ""
	if c.E != nil {
		errStr = c.E.Error()
	}
	return fmt.Sprintf("{R:%s, Err:%s}", c.R, errStr)
}

// RunUntilMinSuccessOrTimeout
// Starts worker with provided inputs
// as soon as minSuccess number of workers are completed it cancels remaining workers
// it waits for atmost 'timeout' duration before cancelling all workers.
func RunUntilMinSuccessOrTimeout[I, R any](
	fn func(context.Context, I) (R, error),
	work []I,
	minSuccess int,
	timeout time.Duration,
) []container[R] {
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var success atomic.Int32

	rChan := make(chan container[R], len(work))

	var wg sync.WaitGroup

	for _, w := range work {
		w := w

		wg.Add(1)

		// runs until the work completed or ctx Completed
		go func(ctx context.Context) {
			defer wg.Done()

			r, err := fn(ctx, w)
			res := container[R]{r, err}

			select {
			case rChan <- res:
				if err == nil {
					if success.Add(1) >= int32(minSuccess) {
						cancel()
					}
				}
			case <-ctx.Done():
			}
		}(ctxWithTimeout)
	}

	// special handling for when minSuccess requires is 0
	// as soon as all the request is fired, cancel the context
	// at this point I see no real world usecase for this.
	// but it exist because implementation allows minSuccess
	// with 0 is possible input.
	if minSuccess == 0 {
		cancel()
	}

	go func() {
		wg.Wait()
		close(rChan)
	}()

	output := []container[R]{}
	for c := range rChan {
		output = append(output, c)
	}

	return output

}

func PickNRandom[T any](arr []T, n int) []T {
	randPerm := rand.Perm(len(arr))

	// Pick first n elements
	result := make([]T, n)
	for i := 0; i < n; i++ {
		result[i] = arr[randPerm[i]]
	}

	return result
}
