package commitlog

import (
	"bufio"
	"context"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"strconv"
	"strings"

	lsmtreetype "github.com/goyal-aman/distributed-storage-nodes/lsmtree/types"
	"github.com/goyal-aman/distributed-storage-nodes/types"
)

type ICommitLog interface {
	Write(ctx context.Context, version types.Version, key string, value []byte) error
	ReadAll() ([]lsmtreetype.LogItem, error)
}

// compile time check
var _ ICommitLog = (*CommitLog)(nil)

type CommitLog struct {
	logPath     string
	logFileDesc *os.File
}

// NewCommitLog
// logPath: complete path of the logfile with name and ext of file
// for example: a/b/c/commit.log
func NewCommitLog(logPath string) (ICommitLog, error) {
	// O_CREATE: create if not there, O_RDWR: read/write, O_APPEND: append mode
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("error opening commitlog: %w", err)
	}

	return &CommitLog{
		logPath:     logPath,
		logFileDesc: file,
	}, nil

}

// Write
// ideally the data is written in following format
// format: <keyLen_32bits><Key>[version<timestamp_64bit_int><count_64bit_uint>]<valLen><val><crc_32_bit>
// but initially I am writting in human readable format.
// human readable format: "<key> <value> <timestamp> <count> <crc>"
func (c *CommitLog) Write(ctx context.Context, version types.Version, key string, value []byte) error {
	logLine := c.toLogLineWithCRC(key, value, version.TimestampEpochUtcInSec, version.Count)
	slog.Debug("written logline: ", slog.String("log_line", logLine))
	if _, err := c.logFileDesc.Write([]byte(logLine)); err != nil {
		return fmt.Errorf("error writting to commitlog: %w", err)
	}
	return nil
}

func (c *CommitLog) toLogLineWithCRC(key string, value []byte, timestamp int64, count uint64) string {
	loglineFH := c.toLogLine(key, value, timestamp, count)
	checksum := c.checkSum(loglineFH)
	logLine := fmt.Sprintf("%s %d\n", loglineFH, checksum)
	return logLine
}

func (c *CommitLog) toLogLine(key string, value []byte, timestamp int64, count uint64) string {
	return fmt.Sprintf("%s %s %d %d", key, string(value), timestamp, count)
}

func (c *CommitLog) checkSum(s string) uint32 {
	return crc32.ChecksumIEEE([]byte(s))
}

// ReadAll
// returns all items available in commitlog as slice of `lsmtreetype.LogItem`
// or returns err if something goes wrong.
func (c *CommitLog) ReadAll() ([]lsmtreetype.LogItem, error) {
	return c.readHumanReadableCommitLog()
}

// readHumanReadableCommitLog
// this implementation reads the commit log which
// contains the log items in human readable format.
func (c *CommitLog) readHumanReadableCommitLog() ([]lsmtreetype.LogItem, error) {
	file, err := os.Open(c.logPath)
	if err != nil {
		return nil, fmt.Errorf("error while reading commitlog: %w", err)
	}
	scanner := bufio.NewScanner(file)

	logItems := make([]lsmtreetype.LogItem, 0)
	for scanner.Scan() {
		line := scanner.Text()
		slog.Debug("scanned line items", slog.String("text", line))

		items := strings.Split(line, " ")

		key, valueStr, timestampStr, countStr, checkSumStr := items[0], items[1], items[2], items[3], items[4]

		timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing timestamp str to int64: %w", err)
		}

		count, err := strconv.ParseUint(countStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing count str to uint64: %w", err)
		}

		checkSum64, err := strconv.ParseUint(checkSumStr, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("error parsing checkSum str to uint32: %w", err)
		}
		readCheckSum := uint32(checkSum64)
		logLine := c.toLogLine(key, []byte(valueStr), timestamp, count)
		calculatedCheckSum := c.checkSum(logLine)

		if readCheckSum != calculatedCheckSum {
			return nil, fmt.Errorf(
				"checksum mismatch: read checksum: %d, calculated checksum: %d, readline: %s",
				readCheckSum, calculatedCheckSum, logLine)
		}

		logItems = append(logItems, lsmtreetype.LogItem{
			Key:   key,
			Value: []byte(valueStr),
			Version: types.Version{
				TimestampEpochUtcInSec: timestamp,
				Count:                  count,
			},
		})
	}
	return logItems, nil
}
