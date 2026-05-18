package sqlitedb_test

import (
	"slices"
	"testing"

	"github.com/goyal-aman/distributed-storage-nodes/lsmtree/sqlitedb"
)

func TestSqliteDB_NewSqliteDB(t *testing.T) {

	tests := []struct {
		name      string
		expectErr error
		dbName    string
		dbDir     string
	}{
		{
			name:      "creating sqlitedb shouldn't give error",
			expectErr: nil,
			dbName:    "dbtest",
			dbDir:     t.TempDir(),
		},
		{
			name:      "creating sqlitedb should give error",
			expectErr: sqlitedb.ErrInvalidDbName,
			dbName:    "",
			dbDir:     t.TempDir(),
		},
		{
			name:      "creating sqlitedb should give error",
			expectErr: sqlitedb.ErrInvalidDbPath,
			dbName:    "testdb",
			dbDir:     "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			_, err := sqlitedb.NewSqliteDB(test.dbDir, test.dbName)
			if err != test.expectErr {
				t.Errorf("NewSqliteDB returned '%s' while expected '%s'", err.Error(), test.expectErr)
			}
		})
	}
}

func TestSqliteDB_Exec_CreateTable(t *testing.T) {

	tests := []struct {
		name                   string
		createDBexpectErr      error
		createTableStatement   string
		expectedCreateTableErr error
		expectedTableName      string
	}{
		{
			name:              "sqlitedb exec create table shouldn't give error",
			createDBexpectErr: nil,
			createTableStatement: `
    			CREATE TABLE IF NOT EXISTS commitlog (
	        		id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    	    		name TEXT
    			);`,
			expectedCreateTableErr: nil,
			expectedTableName:      "commitlog",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tempdir := t.TempDir()
			dbName := "dbtest"

			db, err := sqlitedb.NewSqliteDB(tempdir, dbName)
			if err != test.createDBexpectErr {
				t.Errorf("NewSqliteDB returned '%s' while expected '%s'", err.Error(), test.createDBexpectErr)
			}

			_, createTableErr := db.Exec(test.createTableStatement)
			if createTableErr != test.expectedCreateTableErr {
				t.Errorf("Exec returned '%s' while expected '%s'", err.Error(), test.createDBexpectErr)
			}

			tableNames, err := db.GetTables()
			if !slices.Contains(tableNames, test.expectedTableName) {
				t.Errorf("GetTables returned '%s' do not contain '%s'", tableNames, test.expectedTableName)
			}

		})
	}
}
