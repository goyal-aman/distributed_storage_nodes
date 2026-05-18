package commitlog_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	"github.com/goyal-aman/distributed-storage-nodes/commitlog"
	lsmtreetype "github.com/goyal-aman/distributed-storage-nodes/lsmtree/types"
	"github.com/goyal-aman/distributed-storage-nodes/types"
)

func TestCommitLog_NewCommitLog(t *testing.T) {

	tests := []struct {
		name      string
		expectErr error
	}{
		{
			name:      "creating commitlog shouldn't give error",
			expectErr: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tempdir := t.TempDir()
			commitLogPath := tempdir + "/commitlog_test.log"

			_, err := commitlog.GetOrCreateCommitLog(commitLogPath)
			if err != test.expectErr {
				t.Errorf("NewCommitLog returned %s while expected %s", err, test.expectErr)
			}
		})
	}
}

func TestCommitLog_Write(t *testing.T) {

	tests := []struct {
		name           string
		expectWriteErr error
		LogItems       []lsmtreetype.LogItem
	}{
		{
			name:           "writes to commit log should be correct",
			expectWriteErr: nil,
			LogItems: []lsmtreetype.LogItem{
				{
					Key:     "key1",
					Value:   ToByteArr("val1"),
					Version: types.Version{Count: uint64(1), TimestampEpochUtcInSec: 123456},
				},
				{
					Key:     "key2",
					Value:   ToByteArr(map[string]string{"aman": "goyal"}),
					Version: types.Version{Count: uint64(2), TimestampEpochUtcInSec: 123456},
				},
				{
					Key:     "key3",
					Value:   ToByteArr(123),
					Version: types.Version{Count: uint64(3), TimestampEpochUtcInSec: 123456},
				},
				{
					Key:     "key4",
					Value:   ToByteArr(123.1),
					Version: types.Version{Count: uint64(3), TimestampEpochUtcInSec: 123456},
				},
				{
					Key:     "key4",
					Value:   ToByteArr([]interface{}{"aman", map[string]int{"aman": 1}, 123}),
					Version: types.Version{Count: uint64(3), TimestampEpochUtcInSec: 123456},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tempdir := t.TempDir()
			commitLogPath := tempdir + "/commitlog_test.log"
			commitLog, err := commitlog.GetOrCreateCommitLog(commitLogPath)
			if err != nil {
				t.Errorf("received unexpected error while creating commitlog: %s", err.Error())
			}

			for _, item := range test.LogItems {
				writeErr := commitLog.Write(context.Background(), item.Version, item.Key, item.Value, item.IsReplica)
				if test.expectWriteErr != writeErr {
					t.Errorf("mismatch: received write err: %s, expected write err: %s", writeErr.Error(), test.expectWriteErr.Error())
				}
			}

			receivedLogItems, err := commitLog.Replay()
			if err != nil {
				t.Errorf("error in reading commitlog: %s", err.Error())
			}

			if len(receivedLogItems) != len(test.LogItems) {
				t.Errorf("mismatch: len(receivedLogItems) != expected Logitem")
			}

			if idx, isEqual := EqualIgnoringOrder(receivedLogItems, test.LogItems); !isEqual {
				t.Errorf("mismatch: received log items aren't equal to expected log items at index : %d", idx)
				t.Log("expectedLogItems", test.LogItems[idx])
				t.Log("receivedLogItems", receivedLogItems[idx])
			}

		})
	}
}

func ToByteArr(a any) []byte {
	b, _ := json.Marshal(a)
	return b
}

func EqualIgnoringOrder[T interface {
	lsmtreetype.XCompare[T]
	lsmtreetype.XString
}](a, b []T) (int, bool) {
	if len(a) != len(b) {
		return -1, false
	}
	// Create copies to avoid modifying original slices
	aCopy := slices.Clone(a)
	bCopy := slices.Clone(b)

	slices.SortFunc(aCopy, func(a, b T) int {
		return a.CmpOrder(b)
	})
	slices.SortFunc(bCopy, func(a, b T) int {
		return a.CmpOrder(b)
	})

	for i := range len(a) {
		if aCopy[i].String() != bCopy[i].String() {
			return i, false
		}
	}

	return -1, true
}
