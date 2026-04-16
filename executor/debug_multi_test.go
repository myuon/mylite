package executor

import (
	"fmt"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

func TestDebugMultiUpdate(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	e.Execute("CREATE DATABASE test")
	e.Execute("USE test")
	e.Execute("CREATE TABLE t1(pk INT, a INT, b INT, PRIMARY KEY (pk)) ENGINE=InnoDB")
	e.Execute("INSERT INTO t1 VALUES (0,0,0)")
	r, err := e.Execute("UPDATE t1 AS A, t1 AS B SET A.a = 1, B.b = 2")
	fmt.Printf("Result: %v, err: %v\n", r, err)
	r2, _ := e.Execute("SELECT * FROM t1")
	fmt.Printf("Rows: %v\n", r2.Rows)
}
