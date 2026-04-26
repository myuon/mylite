package executor

import (
    "testing"
    "fmt"
    
    "github.com/myuon/mylite/catalog"
    "github.com/myuon/mylite/storage"
)

func TestSubqueryComplexExplain(t *testing.T) {
    cat := catalog.New()
    store := storage.NewEngine()
    e := New(cat, store)
    
    if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
        t.Fatalf("create db: %v", err)
    }
    e.CurrentDB = "test"
    
    // Set semijoin=off, materialization=on
    if _, err := e.Execute("SET optimizer_switch='semijoin=off'"); err != nil {
        t.Fatalf("set optimizer_switch: %v", err)
    }
    
    for _, stmt := range []string{
        "CREATE TABLE t1 (a1 varchar(10), a2 varchar(10))",
        "CREATE TABLE t2 (b1 varchar(10), b2 varchar(10))",
        "CREATE TABLE t3 (c1 varchar(10), c2 varchar(10))",
        "CREATE TABLE t2i (b1 varchar(10), b2 varchar(10))",
    } {
        if _, err := e.Execute(stmt); err != nil {
            t.Fatalf("create table: %v", err)
        }
    }
    
    res, err := e.Execute(`EXPLAIN select * from t1
        where (a1, a2) in (select b1, b2 from t2
        where b2 in (select c2 from t3 where c2 LIKE '%02') or
        b2 in (select c2 from t3 where c2 LIKE '%03')) and
        (a1, a2) in (select c1, c2 from t3
        where (c1, c2) in (select b1, b2 from t2i where b2 > '0'))`)
    
    if err != nil {
        t.Fatalf("EXPLAIN failed: %v", err)
    }
    
    fmt.Printf("Got %d rows:\n", len(res.Rows))
    for _, row := range res.Rows {
        fmt.Printf("%v\t%v\t%v\n", row[0], row[1], row[2])
    }
    
    // Expected: [1 PRIMARY t1, 5 SUBQUERY t3, 6 SUBQUERY t2i, 2 SUBQUERY t2, 4 SUBQUERY t3, 3 SUBQUERY t3]
    expected := []struct{ id, st, tbl string }{
        {"1", "PRIMARY", "t1"},
        {"5", "SUBQUERY", "t3"},
        {"6", "SUBQUERY", "t2i"},
        {"2", "SUBQUERY", "t2"},
        {"4", "SUBQUERY", "t3"},
        {"3", "SUBQUERY", "t3"},
    }
    
    if len(res.Rows) != len(expected) {
        t.Errorf("expected %d rows, got %d", len(expected), len(res.Rows))
        return
    }
    
    for i, exp := range expected {
        row := res.Rows[i]
        idStr := fmt.Sprintf("%v", row[0])
        stStr := fmt.Sprintf("%v", row[1])
        tblStr := fmt.Sprintf("%v", row[2])
        if idStr != exp.id || stStr != exp.st || tblStr != exp.tbl {
            t.Errorf("row %d: expected {id=%s, st=%s, tbl=%s}, got {id=%v, st=%v, tbl=%v}",
                i, exp.id, exp.st, exp.tbl, row[0], row[1], row[2])
        }
    }
}

func TestSubquerySimpleExplain(t *testing.T) {
    cat := catalog.New()
    store := storage.NewEngine()
    e := New(cat, store)
    
    if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
        t.Fatalf("create db: %v", err)
    }
    e.CurrentDB = "test"
    
    if _, err := e.Execute("SET optimizer_switch='semijoin=off'"); err != nil {
        t.Fatalf("set optimizer_switch: %v", err)
    }
    
    for _, stmt := range []string{
        "CREATE TABLE t1 (a1 varchar(10), a2 varchar(10))",
        "CREATE TABLE t2 (b1 varchar(10), b2 varchar(10))",
        "CREATE TABLE t3 (c1 varchar(10), c2 varchar(10))",
        "CREATE TABLE t2i (b1 varchar(10), b2 varchar(10))",
    } {
        if _, err := e.Execute(stmt); err != nil {
            t.Fatalf("create table: %v", err)
        }
    }
    
    res, err := e.Execute(`EXPLAIN select * from t1
        where (a1, a2) in (select b1, b2 from t2 where b1 > '0') and
        (a1, a2) in (select c1, c2 from t3
        where (c1, c2) in (select b1, b2 from t2i where b2 > '0'))`)
    
    if err != nil {
        t.Fatalf("EXPLAIN failed: %v", err)
    }
    
    fmt.Printf("Simple got %d rows:\n", len(res.Rows))
    for _, row := range res.Rows {
        fmt.Printf("%v\t%v\t%v\n", row[0], row[1], row[2])
    }
    
    // Expected: [1 PRIMARY t1, 3 SUBQUERY t3, 4 SUBQUERY t2i, 2 SUBQUERY t2]
    expected := []struct{ id, st, tbl string }{
        {"1", "PRIMARY", "t1"},
        {"3", "SUBQUERY", "t3"},
        {"4", "SUBQUERY", "t2i"},
        {"2", "SUBQUERY", "t2"},
    }
    
    if len(res.Rows) != len(expected) {
        t.Errorf("expected %d rows, got %d", len(expected), len(res.Rows))
        return
    }
    
    for i, exp := range expected {
        row := res.Rows[i]
        idStr := fmt.Sprintf("%v", row[0])
        stStr := fmt.Sprintf("%v", row[1])
        tblStr := fmt.Sprintf("%v", row[2])
        if idStr != exp.id || stStr != exp.st || tblStr != exp.tbl {
            t.Errorf("row %d: expected {id=%s, st=%s, tbl=%s}, got {id=%v, st=%v, tbl=%v}",
                i, exp.id, exp.st, exp.tbl, row[0], row[1], row[2])
        }
    }
}
