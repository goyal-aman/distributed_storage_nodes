package sqlitedb

import "fmt"

var (
	ErrInitDb        = fmt.Errorf("err in init db")
	ErrInvalidDbName = fmt.Errorf("err invalid db name")
	ErrInvalidDbPath = fmt.Errorf("err invalid db path")
)
