package executor

import (
    "fmt"
    "testing"

    "github.com/myuon/mylite/catalog"
    "github.com/myuon/mylite/storage"
)

// TestInnodbMaterialized tests the innodb case where 3-row inner with outer having key → MATERIALIZED
// Expected: 3 rows with MATERIALIZED (like innodb_all result line 40-42)
func TestInnodbMaterialized(t *testing.T) {
    cat := catalog.New()
    store := storage.NewEngine()
    e := New(cat, store)
    if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
        t.Fatalf("create db: %v", err)
    }
    e.CurrentDB = "test"

    cmds := []string{
        "SET optimizer_switch='semijoin=on,materialization=on,firstmatch=on,loosescan=on,index_condition_pushdown=on,mrr=on,mrr_cost_based=off'",
        // t1 has 3 rows, no index on a (join col)
        "CREATE TABLE t1 (a int, b int)",
        "INSERT INTO t1 VALUES (1,1),(1,1),(2,2)",
        // t2 has many rows, key(b) where b is the join col
        "CREATE TABLE t2 (a int, b int, KEY(b))",
        "INSERT INTO t2 VALUES (0,0),(1,0),(2,1),(3,1),(4,2),(5,2),(6,3),(7,3),(8,4),(9,4)",
        "INSERT INTO t2 VALUES (10,5),(11,5),(12,6),(13,6),(14,7),(15,7)",
    }
    for _, cmd := range cmds {
        if _, err := e.Execute(cmd); err != nil {
            t.Fatalf("Setup %q failed: %v", cmd, err)
        }
    }
    
    fmt.Println("=== explain select * from t2 where b in (select a from t1) ===")
    res, err := e.Execute("EXPLAIN SELECT * FROM t2 WHERE b IN (SELECT a FROM t1)")
    if err != nil {
        t.Fatalf("Error: %v", err)
    }
    for _, row := range res.Rows {
        fmt.Printf("  id=%v, selectType=%v, table=%v, type=%v, extra=%v\n", row[0], row[1], row[2], row[4], row[11])
    }
    
    // Expected: 3 rows: <subquery2> (ALL), t2 (ref via key(b)), t1 (MATERIALIZED)
    if len(res.Rows) != 3 {
        t.Errorf("Expected 3 rows, got %d", len(res.Rows))
    }
    // Check that t1 is MATERIALIZED
    mat := false
    for _, row := range res.Rows {
        if fmt.Sprintf("%v", row[1]) == "MATERIALIZED" {
            mat = true
        }
    }
    if !mat {
        t.Errorf("Expected t1 to be MATERIALIZED")
    }
}
