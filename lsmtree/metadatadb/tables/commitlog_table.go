package tables

import "fmt"

type CommitLogStatus string

const (
	StatusLive        CommitLogStatus = "LIVE"
	StatusToBeFlushed CommitLogStatus = "TO_BE_FLUSHED"
	StatusFlushed     CommitLogStatus = "FLUSHED"
)

type CommitLogTableSchema struct {
	Id     int    `db:"id"`
	Name   string `db:"name"`
	Status string `db:"status"`
}

func (c CommitLogTableSchema) String() string {
	return fmt.Sprintf("{Id:'%d', Name:'%s', Status:'%s'}", c.Id, c.Name, c.Status)
}

func (CommitLogTableSchema) CreateTableStatement() string {
	return `
			CREATE TABLE commitlog (
				id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
				name TEXT NOT NULL UNIQUE,
				status TEXT NOT NULL CHECK (
					status IN ('LIVE', 'TO_BE_FLUSHED', 'FLUSHED')
				)
			);
		`
}

func (CommitLogTableSchema) CreateIndexStatement() string {
	return `CREATE UNIQUE INDEX idx_commitlog_single_live
		ON commitlog(status)
		WHERE status = 'LIVE';`
}
