package executor

import (
	"fmt"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

func TestDateComparison(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)

	queries := []string{"CREATE DATABASE IF NOT EXISTS test"}
	for _, q := range queries {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}
	e.CurrentDB = "test"

	setup := []string{
		"CREATE TABLE t1 (f1 date, f2 datetime, f3 timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)",
		"INSERT INTO t1 VALUES('2001-01-01','2001-01-01 01:01:01','2001-01-01 01:01:01')",
		"INSERT INTO t1 VALUES('2001-02-05','2001-02-05 00:00:00','2001-02-05 01:01:01')",
		"INSERT INTO t1 VALUES('2001-03-10','2001-03-09 01:01:01','2001-03-10 01:01:01')",
		"INSERT INTO t1 VALUES('2001-04-15','2001-04-15 00:00:00','2001-04-15 00:00:00')",
		"INSERT INTO t1 VALUES('2001-05-20','2001-05-20 01:01:01','2001-05-20 01:01:01')",
	}
	for _, q := range setup {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}

	// Debug: check individual conditions
	queries2 := []string{
		"SELECT f1, f3 FROM t1",
		"SELECT f1, f3 FROM t1 WHERE f1 >= '2001-02-05 00:00:00'",
		"SELECT f1, f3 FROM t1 WHERE f3 <= '2001-04-15'",
		"SELECT f1, f3 FROM t1 WHERE f1 >= '2001-02-05 00:00:00' AND f3 <= '2001-04-15'",
	}
	for _, q := range queries2 {
		result, err := e.Execute(q)
		if err != nil {
			t.Fatalf("query %q: %v", q, err)
		}
		fmt.Printf("Query: %s\n", q)
		for _, row := range result.Rows {
			fmt.Printf("  %v\n", row)
		}
		fmt.Println()
	}
}

func TestDateCastComparison(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)

	queries := []string{"CREATE DATABASE IF NOT EXISTS test"}
	for _, q := range queries {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}
	e.CurrentDB = "test"

	setup := []string{
		"CREATE TABLE t1 (f1 date, f2 datetime, f3 timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)",
		"INSERT INTO t1(f1) VALUES(curdate())",
	}
	for _, q := range setup {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}

	result, err := e.Execute("SELECT curdate() < now(), f1 < now(), cast(f1 as date) < now() FROM t1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	for _, row := range result.Rows {
		fmt.Printf("curdate()<now, f1<now, cast(f1 as date)<now: %v\n", row)
	}
	
	// Also test
	result2, err := e.Execute("SELECT cast(f1 as date) FROM t1")
	if err != nil {
		t.Fatalf("query2: %v", err)
	}
	for _, row := range result2.Rows {
		fmt.Printf("cast(f1 as date) type=%T value=%v\n", row[0], row[0])
	}
}

func TestCreateTableDefaultValidation(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)

	queries := []string{"CREATE DATABASE IF NOT EXISTS test"}
	for _, q := range queries {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}
	e.CurrentDB = "test"

	// This should succeed
	result, err := e.Execute("CREATE TABLE t1 (da date default '1962-03-03 23:33:34', dt datetime default '1962-03-03')")
	fmt.Printf("CREATE t1 result=%v err=%v\n", result, err)

	// This should fail with invalid default
	_, err2 := e.Execute("CREATE TABLE t2 (da date default '1962-03-32 23:33:34', dt datetime default '1962-03-03')")
	fmt.Printf("CREATE t2 (bad default) err=%v\n", err2)

	// Check if t2 was created
	_, err3 := e.Execute("SHOW TABLES")
	fmt.Printf("SHOW TABLES err=%v\n", err3)
}

func TestFullTypeDateTime(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)

	setup := []string{
		"CREATE DATABASE IF NOT EXISTS test",
	}
	for _, q := range setup {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}
	e.CurrentDB = "test"

	queries := []string{
		"set @org_mode=@@sql_mode",
		"create table t1 (da date default '1962-03-03 23:33:34', dt datetime default '1962-03-03')",
		// drop table after test
		"drop table t1",
	}
	for _, q := range queries {
		if _, err := e.Execute(q); err != nil {
			fmt.Printf("query %q: err=%v\n", q, err)
		}
	}

	// Test invalid defaults - should fail
	_, err := e.Execute("create table t1 (da date default '1962-03-32 23:33:34', dt datetime default '1962-03-03')")
	fmt.Printf("create with invalid date default: %v\n", err)
	
	_, err = e.Execute("create table t1 (t time default '916:00:00 a')")
	fmt.Printf("create with invalid time default: %v\n", err)

	// Restore sql_mode
	e.Execute("set @@sql_mode= @org_mode")
	
	// Now create the test table
	if _, err := e.Execute("create table t1 (f1 date, f2 datetime, f3 timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)"); err != nil {
		t.Fatalf("create table t1: %v", err)
	}
	
	inserts := []string{
		"insert into t1(f1) values(curdate())",
	}
	for _, q := range inserts {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	
	result, err := e.Execute("select curdate() < now(), f1 < now(), cast(f1 as date) < now() from t1")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	fmt.Printf("curdate<now, f1<now, cast(f1 as date)<now: %v\n", result.Rows)
	
	e.Execute("delete from t1")
	
	for _, ins := range []string{
		"insert into t1 values('2001-01-01','2001-01-01 01:01:01','2001-01-01 01:01:01')",
		"insert into t1 values('2001-02-05','2001-02-05 00:00:00','2001-02-05 01:01:01')",
		"insert into t1 values('2001-03-10','2001-03-09 01:01:01','2001-03-10 01:01:01')",
		"insert into t1 values('2001-04-15','2001-04-15 00:00:00','2001-04-15 00:00:00')",
		"insert into t1 values('2001-05-20','2001-05-20 01:01:01','2001-05-20 01:01:01')",
	} {
		if _, err := e.Execute(ins); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	
	result2, err := e.Execute("select f1, f3 from t1 where f1 >= '2001-02-05 00:00:00' and f3 <= '2001-04-15'")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	fmt.Printf("select f1, f3 with where: %d rows\n", len(result2.Rows))
	for _, row := range result2.Rows {
		fmt.Printf("  %v\n", row)
	}
}

func TestNonNormalizedDateCompare(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)

	setup := []string{
		"CREATE DATABASE IF NOT EXISTS test",
	}
	for _, q := range setup {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}
	e.CurrentDB = "test"

	setup2 := []string{
		"CREATE TABLE t1 (f1 date, f2 datetime, f3 timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)",
		"INSERT INTO t1 VALUES('2001-01-01','2001-01-01 01:01:01','2001-01-01 01:01:01')",
		"INSERT INTO t1 VALUES('2001-02-05','2001-02-05 00:00:00','2001-02-05 01:01:01')",
		"INSERT INTO t1 VALUES('2001-03-10','2001-03-09 01:01:01','2001-03-10 01:01:01')",
		"INSERT INTO t1 VALUES('2001-04-15','2001-04-15 00:00:00','2001-04-15 00:00:00')",
		"INSERT INTO t1 VALUES('2001-05-20','2001-05-20 01:01:01','2001-05-20 01:01:01')",
	}
	for _, q := range setup2 {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}

	// First query - standard format
	r1, err := e.Execute("SELECT f1, f3 FROM t1 WHERE f1 >= '2001-02-05 00:00:00' AND f3 <= '2001-04-15'")
	if err != nil {
		t.Fatalf("q1: %v", err)
	}
	fmt.Printf("q1 (standard format): %d rows\n", len(r1.Rows))
	for _, row := range r1.Rows {
		fmt.Printf("  f1=%v, f3=%v\n", row[0], row[1])
	}

	// Second query - non-normalized format  
	r2, err := e.Execute("SELECT f1, f3 FROM t1 WHERE f1 >= '2001-2-5 0:0:0' AND f2 <= '2001-4-15'")
	if err != nil {
		t.Fatalf("q2: %v", err)
	}
	fmt.Printf("q2 (non-normalized): %d rows\n", len(r2.Rows))
	for _, row := range r2.Rows {
		fmt.Printf("  f1=%v, f3=%v\n", row[0], row[1])
	}
}

func TestCastDateComparison(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)

	setup := []string{"CREATE DATABASE IF NOT EXISTS test"}
	for _, q := range setup {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}
	e.CurrentDB = "test"

	result, err := e.Execute("SELECT 1 FROM dual WHERE CAST('2001-1-1 2:3:4' AS DATE) = CAST('2001-01-01' AS DATETIME)")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	fmt.Printf("cast('2001-1-1 2:3:4' as date) = cast('2001-01-01' as datetime): %d rows, rows=%v\n", len(result.Rows), result.Rows)
	
	// Debug components
	r1, _ := e.Execute("SELECT CAST('2001-1-1 2:3:4' AS DATE)")
	fmt.Printf("cast('2001-1-1 2:3:4' as date): %v\n", r1.Rows)
	
	r2, _ := e.Execute("SELECT CAST('2001-01-01' AS DATETIME)")
	fmt.Printf("cast('2001-01-01' as datetime): %v\n", r2.Rows)
}

func TestDateInComparison(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)

	setup := []string{"CREATE DATABASE IF NOT EXISTS test"}
	for _, q := range setup {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}
	e.CurrentDB = "test"

	if _, err := e.Execute("CREATE TABLE t1 (f1 date)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, v := range []string{"'01-01-01'", "'01-01-02'", "'01-01-03'"} {
		if _, err := e.Execute("INSERT INTO t1 VALUES(" + v + ")"); err != nil {
			t.Fatalf("insert %s: %v", v, err)
		}
	}
	
	// Show stored values
	r0, _ := e.Execute("SELECT f1 FROM t1")
	fmt.Printf("stored values: %v\n", r0.Rows)
	
	r1, err := e.Execute("SELECT * FROM t1 WHERE f1 IN ('01-01-01','2001-01-02','2001-01-03 00:00:00')")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	fmt.Printf("IN result: %d rows, %v\n", len(r1.Rows), r1.Rows)
	
	// Individual comparisons
	r2, _ := e.Execute("SELECT f1 = '01-01-01', f1 = '2001-01-01' FROM t1 WHERE f1 = '2001-01-01'")
	fmt.Printf("equality checks: %v\n", r2.Rows)
}

func TestDateInComparison2(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)

	setup := []string{"CREATE DATABASE IF NOT EXISTS test"}
	for _, q := range setup {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}
	e.CurrentDB = "test"

	e.Execute("CREATE TABLE t1 (f1 date)")
	e.Execute("INSERT INTO t1 VALUES('2001-01-01')")
	e.Execute("INSERT INTO t1 VALUES('2001-01-02')")
	e.Execute("INSERT INTO t1 VALUES('2001-01-03')")
	
	tests := []string{
		"SELECT * FROM t1 WHERE f1 IN ('01-01-01')",
		"SELECT * FROM t1 WHERE f1 IN ('2001-01-01')",
		"SELECT * FROM t1 WHERE f1 IN ('2001-01-03 00:00:00')",
		"SELECT * FROM t1 WHERE f1 IN ('01-01-01','2001-01-02')",
		"SELECT * FROM t1 WHERE f1 IN ('01-01-01','2001-01-02','2001-01-03 00:00:00')",
	}
	for _, q := range tests {
		r, err := e.Execute(q)
		if err != nil {
			fmt.Printf("%s => err: %v\n", q, err)
		} else {
			fmt.Printf("%s => %d rows: %v\n", q, len(r.Rows), r.Rows)
		}
	}
}
