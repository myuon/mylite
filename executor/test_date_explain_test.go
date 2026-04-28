package executor

import (
    "fmt"
    "testing"

    "github.com/myuon/mylite/catalog"
    "github.com/myuon/mylite/storage"
)

// TestDateTypeExplain tests EXPLAIN for incompatible date/int type IN subqueries
// Expected: SIMPLE rows with FirstMatch (not MATERIALIZED)
func TestDateTypeExplain(t *testing.T) {
    cat := catalog.New()
    store := storage.NewEngine()
    e := New(cat, store)
    if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
        t.Fatalf("create db: %v", err)
    }
    e.CurrentDB = "test"

    cmds := []string{
        "SET optimizer_switch='semijoin=on,materialization=on,firstmatch=on,loosescan=on,index_condition_pushdown=on,mrr=on,mrr_cost_based=off'",
        "CREATE TABLE t1 (a date)",
        "INSERT INTO t1 VALUES ('2008-01-01'),('2008-01-01'),('2008-02-01'),('2008-02-01')",
        "CREATE TABLE t2 (a int)",
        "INSERT INTO t2 VALUES (1),(2)",
        "CREATE TABLE t3 (a char(10))",
        "INSERT INTO t3 SELECT * FROM t1",
        "INSERT INTO t3 VALUES (1),(2)",
    }
    for _, cmd := range cmds {
        if _, err := e.Execute(cmd); err != nil {
            t.Fatalf("Setup %q failed: %v", cmd, err)
        }
    }
    
    tests := []struct{
        query string
        expectedRows int
    }{
        {"EXPLAIN SELECT * FROM t2 WHERE a IN (SELECT a FROM t1)", 2},
        {"EXPLAIN SELECT * FROM t2 WHERE a IN (SELECT a FROM t2)", 2},  // int vs int, 2 rows each, no index
        {"EXPLAIN SELECT * FROM t2 WHERE a IN (SELECT a FROM t3)", 2},
        {"EXPLAIN SELECT * FROM t1 WHERE a IN (SELECT a FROM t3)", 2},
    }
    
    for _, tc := range tests {
        t.Run(tc.query, func(t *testing.T) {
            res, err := e.Execute(tc.query)
            if err != nil {
                t.Fatalf("Error: %v", err)
            }
            fmt.Printf("Query: %s\n", tc.query)
            for _, row := range res.Rows {
                fmt.Printf("  id=%v, selectType=%v, table=%v, type=%v, extra=%v\n", row[0], row[1], row[2], row[4], row[11])
            }
            if len(res.Rows) != tc.expectedRows {
                t.Errorf("Expected %d rows, got %d", tc.expectedRows, len(res.Rows))
            }
        })
    }
}

// TestSimpleINExplain tests the simple case: outer=t2(2 rows int), inner=t2(2 rows int)
// Expected: 2 SIMPLE rows (no MATERIALIZED)
func TestSimpleINExplain(t *testing.T) {
    cat := catalog.New()
    store := storage.NewEngine()
    e := New(cat, store)
    if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
        t.Fatalf("create db: %v", err)
    }
    e.CurrentDB = "test"

    cmds := []string{
        "SET optimizer_switch='semijoin=on,materialization=on,firstmatch=on,loosescan=on,index_condition_pushdown=on,mrr=on,mrr_cost_based=off'",
        "CREATE TABLE tx (a int)",
        "INSERT INTO tx VALUES (1),(2)",
    }
    for _, cmd := range cmds {
        if _, err := e.Execute(cmd); err != nil {
            t.Fatalf("Setup %q failed: %v", cmd, err)
        }
    }
    
    fmt.Println("=== explain select * from tx where a in (select a from tx) ===")
    res, err := e.Execute("EXPLAIN SELECT * FROM tx WHERE a IN (SELECT a FROM tx)")
    if err != nil {
        t.Fatalf("Error: %v", err)
    }
    for _, row := range res.Rows {
        fmt.Printf("  id=%v, selectType=%v, table=%v, type=%v, extra=%v\n", row[0], row[1], row[2], row[4], row[11])
    }
    
    // Expected: 2 SIMPLE rows (cost_based=off, firstmatch=on, 2 rows <= 6 → FirstMatch)
    if len(res.Rows) != 2 {
        t.Errorf("Expected 2 rows, got %d", len(res.Rows))
    }
}

// TestDateDateSubquery tests the DATE-DATE case from subquery_sj.inc line 638
// t0 has 2 DATE rows, t1 is created from t0 and then doubled (4 rows), both DATE
// Expected: 2 SIMPLE rows (cost_based=ON, outer no index, inner 4 rows ≤ 6 threshold)
func TestDateDateSubquery(t *testing.T) {
    cat := catalog.New()
    store := storage.NewEngine()
    e := New(cat, store)
    if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
        t.Fatalf("create db: %v", err)
    }
    e.CurrentDB = "test"

    cmds := []string{
        "SET optimizer_switch='semijoin=on,materialization=on,firstmatch=on,loosescan=on,index_condition_pushdown=on,mrr=on,mrr_cost_based=off'",
        "CREATE TABLE t0 (a date)",
        "INSERT INTO t0 VALUES ('2008-01-01'),('2008-02-02')",
        "CREATE TABLE t1 AS SELECT * FROM t0",
        "INSERT INTO t1 SELECT * FROM t0",
    }
    for _, cmd := range cmds {
        if _, err := e.Execute(cmd); err != nil {
            t.Fatalf("Setup %q failed: %v", cmd, err)
        }
    }
    
    fmt.Println("=== explain select * from t0 where a in (select a from t1) ===")
    res, err := e.Execute("EXPLAIN SELECT * FROM t0 WHERE a IN (SELECT a FROM t1)")
    if err != nil {
        t.Fatalf("Error: %v", err)
    }
    for _, row := range res.Rows {
        fmt.Printf("  id=%v, selectType=%v, table=%v, type=%v, extra=%v\n", row[0], row[1], row[2], row[4], row[11])
    }
    
    // Expected: 2 SIMPLE rows (no MATERIALIZED)
    if len(res.Rows) != 2 {
        t.Errorf("Expected 2 rows, got %d", len(res.Rows))
    }
    for _, row := range res.Rows {
        if fmt.Sprintf("%v", row[1]) == "MATERIALIZED" {
            t.Errorf("Got unexpected MATERIALIZED row: %v", row)
        }
    }
}

// TestDateContextFull tests the full context around the DATE section in subquery_sj.inc
func TestDateContextFull(t *testing.T) {
    cat := catalog.New()
    store := storage.NewEngine()
    e := New(cat, store)
    if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
        t.Fatalf("create db: %v", err)
    }
    e.CurrentDB = "test"

    // Simulate some earlier context from subquery_sj.inc (around line 600+)
    // to see if state affects the DATE test
    cmds := []string{
        "SET optimizer_switch='semijoin=on,materialization=on,firstmatch=on,loosescan=on,index_condition_pushdown=on,mrr=on,mrr_cost_based=off'",
        // Create tables similar to what comes before the DATE section
        "CREATE TABLE t0 (a INT)",
        "INSERT INTO t0 VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)",
        "CREATE TABLE t2 (a int)",
        "INSERT INTO t2 VALUES (1),(2)",
        "CREATE TABLE t3 (a int, filler char(100), KEY(a))",
        "INSERT INTO t3 SELECT A.a + 10*B.a, 'filler' FROM t0 A, t0 B",
        // Explain query that uses t3 with key and t2 without key
        "EXPLAIN SELECT * FROM t3 WHERE a IN (SELECT a FROM t2) AND (a > 5 OR a < 10)",
        // Drop tables to simulate the drop before DATE section
        "DROP TABLE t0, t2, t3",
        // Now create the DATE tables
        "CREATE TABLE t0 (a DATE)",
        "INSERT INTO t0 VALUES ('2008-01-01'),('2008-02-02')",
        "CREATE TABLE t1 AS SELECT * FROM t0",
        "INSERT INTO t1 SELECT * FROM t0",
    }
    for _, cmd := range cmds {
        if _, err := e.Execute(cmd); err != nil {
            t.Fatalf("Setup %q failed: %v", cmd, err)
        }
    }
    
    fmt.Println("=== explain select * from t0 where a in (select a from t1) [full context] ===")
    res, err := e.Execute("EXPLAIN SELECT * FROM t0 WHERE a IN (SELECT a FROM t1)")
    if err != nil {
        t.Fatalf("Error: %v", err)
    }
    for _, row := range res.Rows {
        fmt.Printf("  id=%v, selectType=%v, table=%v, type=%v, extra=%v\n", row[0], row[1], row[2], row[4], row[11])
    }
    
    // Expected: 2 SIMPLE rows
    if len(res.Rows) != 2 {
        t.Errorf("Expected 2 rows, got %d", len(res.Rows))
    }
}
