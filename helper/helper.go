package helper

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"log/slog"
	"strconv"
	"time"

	"github.com/goyal-aman/distributed-storage-nodes/err"
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

type HasEndOfKeyRange interface {
	XEndOfKeyRange() uint64
}

// GetNode
// find the node which owns the the token (token=hash(key))
func GetNode[T HasEndOfKeyRange](
	nodes []T,
	token uint64) (*T, error) {
	for _, node := range nodes {
		if token < node.XEndOfKeyRange() {
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
