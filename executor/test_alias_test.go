package executor

import (
    "testing"
    "github.com/myuon/mylite/catalog"
    "github.com/myuon/mylite/storage"
)

func TestWildcardAlias(t *testing.T) {
    cat := catalog.New()
    store := storage.NewEngine()
    e := New(cat, store)
    
    if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
        t.Fatalf("setup: %v", err)
    }
    e.CurrentDB = "test"
    
    if _, err := e.Execute("CREATE TABLE t1 (a int, b int, c int)"); err != nil {
        t.Fatalf("create t1: %v", err)
    }
    if _, err := e.Execute("CREATE TABLE t2 (d int)"); err != nil {
        t.Fatalf("create t2: %v", err)
    }
    
    tests := []struct {
        query   string
        wantErr bool
    }{
        {"SELECT t1.* AS 'with_alias' FROM t1", true},
        {"INSERT INTO nonexist SELECT t1.* AS 'with_alias' FROM t1", true},
        {"CREATE TABLE t3 SELECT t1.* AS 'with_alias' FROM t1", true},
        {"CREATE TABLE t3 SELECT t2.* AS 'with_alias', 1, 2 FROM t2", true},
    }
    
    for _, tt := range tests {
        _, err := e.Execute(tt.query)
        if tt.wantErr && err == nil {
            t.Errorf("expected error for %q but got nil", tt.query)
        } else if !tt.wantErr && err != nil {
            t.Errorf("unexpected error for %q: %v", tt.query, err)
        } else if err != nil {
            t.Logf("got expected error for %q: %v", tt.query, err)
        }
    }
}
