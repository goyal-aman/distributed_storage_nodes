package helper

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"

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
