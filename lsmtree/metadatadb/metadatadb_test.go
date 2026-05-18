package metadatadb_test

import (
	"context"
	"slices"
	"testing"

	"github.com/goyal-aman/distributed-storage-nodes/lsmtree/metadatadb"
	"github.com/goyal-aman/distributed-storage-nodes/lsmtree/metadatadb/tables"
	"github.com/goyal-aman/distributed-storage-nodes/lsmtree/sqlitedb"
)

func TestMetadataDB_GetLiveCommitLog(t *testing.T) {

	tests := []struct {
		name                        string
		expectedCreateMetadataDbErr error
		commitLogName               string
		commitLogStatus             string
		expectedInsertErr           error
		expectedErrGetLiveCommitLog error
		expectedCommitLogRows       []tables.CommitLogTableSchema
		dbName                      string
		dbDir                       string
	}{
		{
			name:                        "test commmitlog lifecycle",
			expectedCreateMetadataDbErr: nil,
			commitLogName:               "commitLog1",
			commitLogStatus:             "LIVE",
			expectedInsertErr:           nil,
			expectedErrGetLiveCommitLog: nil,
			dbName:                      "testMetataDb",
			dbDir:                       t.TempDir(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			ctx := context.Background()
			metadataDb, createMetadataDbErr := metadatadb.NewMetadataDB(test.dbDir, test.dbName)

			if createMetadataDbErr != test.expectedCreateMetadataDbErr {
				t.Errorf("NewMetadataDB returned '%s' while expected '%s'", createMetadataDbErr.Error(), test.expectedCreateMetadataDbErr)
			}

			insertErr := metadataDb.InsertCommitLog(ctx, test.commitLogName, test.commitLogStatus)
			if insertErr != test.expectedInsertErr {
				t.Errorf("InsertCommitLog returned '%s' while expected '%s'", insertErr.Error(), test.expectedInsertErr)
			}

			rows, errGetLiveCommitLog := metadataDb.GetLiveCommitLog(ctx)
			if errGetLiveCommitLog != test.expectedErrGetLiveCommitLog {
				t.Errorf("GetLiveCommitLog returned '%s' while expected '%s'", insertErr.Error(), test.expectedInsertErr)
			}

			if len(rows) != 1 {
				t.Errorf("GetLiveCommitLog returned '%d' while expected '%d' rows", len(rows), 1)
			}

			onlyRow := rows[0]
			if onlyRow.Name != test.commitLogName {
				t.Errorf("GetLiveCommitLog returned Name '%s' while expected '%s' ", onlyRow.Name, test.commitLogName)
			}
			if onlyRow.Status != test.commitLogStatus {
				t.Errorf("GetLiveCommitLog returned Status '%s' while expected '%s' ", onlyRow.Status, test.commitLogStatus)
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
