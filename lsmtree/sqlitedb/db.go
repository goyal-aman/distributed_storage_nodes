package sqlitedb

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

type SqliteDB struct {
	db *sql.DB
}

func NewSqliteDB(path, name string) (*SqliteDB, error) {
	if strings.TrimSpace(name) == "" {
		return nil, ErrInvalidDbName
	}
	if strings.TrimSpace(path) == "" {
		return nil, ErrInvalidDbPath
	}

	dbName := fmt.Sprintf("%s/%s.db", path, name)
	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		return nil, fmt.Errorf("%w, %w", ErrInitDb, err)
	}
	return &SqliteDB{
		db: db,
	}, nil
}

func (s *SqliteDB) Close() error {
	return s.db.Close()
}

func (s *SqliteDB) Exec(statement string) (sql.Result, error) {
	return s.db.Exec(statement)
}

func (s SqliteDB) GetTables() ([]string, error) {
	rows, err := s.db.Query(`
		SELECT name
		FROM sqlite_master
		WHERE type='table'
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tables, nil
}

func (s *SqliteDB) DB() *sql.DB {
	return s.db
}
