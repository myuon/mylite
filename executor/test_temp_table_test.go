package executor

import (
    "fmt"
    "testing"
    "github.com/myuon/mylite/catalog"
    "github.com/myuon/mylite/storage"
)

func TestTempTableCardinality(t *testing.T) {
    cat := catalog.New()
    store := storage.NewEngine()
    e := New(cat, store)
    
    if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
        t.Fatalf("setup: %v", err)
    }
    e.CurrentDB = "test"
    
    if _, err := e.Execute("CREATE TEMPORARY TABLE t1 (f1 int, f2 int primary key, UNIQUE KEY (f1))"); err != nil {
        t.Fatalf("create temp table: %v", err)
    }
    
    fmt.Printf("tempTables: %v\n", e.tempTables)
    
    result, err := e.Execute("SHOW INDEXES FROM t1")
    if err != nil {
        t.Fatalf("show indexes: %v", err)
    }
    for _, row := range result.Rows {
        fmt.Printf("Row: %v\n", row)
    }
}
