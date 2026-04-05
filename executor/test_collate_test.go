package executor

import (
    "fmt"
    "testing"
    "github.com/myuon/mylite/catalog"
    "github.com/myuon/mylite/storage"
)

func TestCollateCreate(t *testing.T) {
    cat := catalog.New()
    store := storage.NewEngine()
    e := New(cat, store)
    
    stmts := []string{
        "CREATE SCHEMA s1 COLLATE binary",
        "CREATE SCHEMA s2 COLLATE 'binary'",
    }
    
    for _, s := range stmts {
        _, err := e.Execute(s)
        if err != nil {
            fmt.Printf("ERROR for %q: %v\n", s, err)
        } else {
            fmt.Printf("OK for %q\n", s)
        }
    }
}
