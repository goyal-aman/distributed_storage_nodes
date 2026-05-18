package metadatadb

import (
	"context"
	"errors"
	"log/slog"

	"github.com/goyal-aman/distributed-storage-nodes/lsmtree/metadatadb/tables"
	"github.com/goyal-aman/distributed-storage-nodes/lsmtree/sqlitedb"
)

type ICommitLogTable interface {
	EnsureCommitLogTable() error
	GetLiveCommitLog(
		ctx context.Context,
	) ([]tables.CommitLogTableSchema, error)
	InsertCommitLog(
		ctx context.Context,
		name, status string,
	) error
}

type ISSTableTable interface {
	EnsureSSTableTable() error
}

type IMetadataDB interface {
	ICommitLogTable
	ISSTableTable
}

var _ IMetadataDB = (*MetadataDB)(nil)

type MetadataDB struct {
	sqllitedb *sqlitedb.SqliteDB
}

func NewMetadataDB(dbDir, dbName string) (IMetadataDB, error) {
	db, err := sqlitedb.NewSqliteDB(dbDir, dbName)
	if err != nil {
		return nil, err
	}
	slog.With("dir", dbDir).With("dbName", dbName).Info("created metadatadb")

	lsmdb := &MetadataDB{
		sqllitedb: db,
	}

	if err := lsmdb.EnsureCommitLogTable(); err != nil {
		return nil, err
	}

	if err := lsmdb.EnsureSSTableTable(); err != nil {
		return nil, err
	}

	return lsmdb, nil
}

func (ldb *MetadataDB) EnsureSSTableTable() error {

	// create sstable table
	createTableStatements := []string{
		`CREATE TABLE IF NOT EXISTS sstable (
        	id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        	name TEXT NOT NULL,
			level INTEGER NOT NULL);
    	`,
	}

	var createTableErr error
	for _, createTableStatement := range createTableStatements {
		_, err := ldb.sqllitedb.Exec(createTableStatement)
		if createTableErr != nil {
			createTableErr = errors.Join(createTableErr, err)
		}
	}

	if createTableErr != nil {
		return createTableErr
	}

	return nil
}

func (ldb *MetadataDB) EnsureCommitLogTable() error {
	// create commitlog table
	createTableStatements := []string{
		tables.CommitLogTableSchema{}.CreateTableStatement(),
		tables.CommitLogTableSchema{}.CreateIndexStatement(),
	}

	var createTableErr error
	for _, createTableStatement := range createTableStatements {
		_, err := ldb.sqllitedb.Exec(createTableStatement)
		if createTableErr != nil {
			createTableErr = errors.Join(createTableErr, err)
		}
	}

	if createTableErr != nil {
		return createTableErr
	}

	return nil
}

func (ldb *MetadataDB) GetLiveCommitLog(
	ctx context.Context,
) ([]tables.CommitLogTableSchema, error) {
	SelectLiveCommitLog := `
	SELECT * 
	FROM commitlog
	WHERE status='LIVE'
	`
	rows, err := ldb.sqllitedb.DB().QueryContext(ctx, SelectLiveCommitLog)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	returnRows := make([]tables.CommitLogTableSchema, 0)

	for rows.Next() {

		tableRow := tables.CommitLogTableSchema{}

		if err := rows.Scan(&tableRow.Id, &tableRow.Name, &tableRow.Status); err != nil {
			return nil, err
		}

		returnRows = append(returnRows, tableRow)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return returnRows, nil
}

func (ldb *MetadataDB) InsertCommitLog(
	ctx context.Context,
	name, status string,
) error {
	_, err := ldb.sqllitedb.DB().ExecContext(
		ctx,
		`
		INSERT INTO commitlog (name, status)
		VALUES (?, ?)
		`,
		name,
		status,
	)
	return err
}
