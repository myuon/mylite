package executor

import (
	"fmt"
	"testing"
	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

func TestPrepareReplaceSelect(t *testing.T) {
	cat := catalog.New()
	stor := storage.NewEngine()
	exec := New(cat, stor)
	
	must := func(sql string) {
		_, err := exec.Execute(sql)
		if err != nil {
			t.Fatalf("Query %q failed: %v", sql, err)
		}
	}
	mustFail := func(sql string) {
		_, err := exec.Execute(sql)
		if err == nil {
			t.Fatalf("Query %q should have failed but didn't", sql)
		}
		t.Logf("Expected failure: %v", err)
	}
	_ = mustFail

	must("CREATE DATABASE IF NOT EXISTS test")
	exec.CurrentDB = "test"
	must("CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(30))")
	must("INSERT INTO t1 VALUES (1, 'one'), (2, 'two')")
	
	// Test PREPARE ... FROM with doubled single quotes
	must("PREPARE stmt1 FROM ' replace into t1 (a,b) select 100, ''hundred'' '")
	must("EXECUTE stmt1")
	
	// Check what's stored
	res, err := exec.Execute("SELECT * FROM t1 WHERE a=100")
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	fmt.Printf("After first execute: rows=%v\n", res.Rows)
	
	must("EXECUTE stmt1")  // Should replace the row, not fail with duplicate key
	must("EXECUTE stmt1")  // Should replace the row again
	
	res, err = exec.Execute("SELECT * FROM t1 WHERE a=100")
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	fmt.Printf("After three executes: rows=%v\n", res.Rows)
}

func TestPrepareDoubleQuotes(t *testing.T) {
	cat := catalog.New()
	stor := storage.NewEngine()
	exec := New(cat, stor)
	
	must := func(sql string) *Result {
		res, err := exec.Execute(sql)
		if err != nil {
			t.Fatalf("Query %q failed: %v", sql, err)
		}
		return res
	}
	
	must("CREATE DATABASE IF NOT EXISTS test")
	exec.CurrentDB = "test"
	must("CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(30))")
	must("INSERT INTO t1 VALUES (1, 'one')")
	
	// Test PREPARE with double quotes (should work like single quotes)
	must(`PREPARE stmt1 FROM "SELECT * FROM t1 WHERE a = 1"`)
	res := must("EXECUTE stmt1")
	t.Logf("Result: %v rows=%v", res.Columns, res.Rows)
	if len(res.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(res.Rows))
	}
}

func TestPrepareReplaceFullContext(t *testing.T) {
	// Simulate the full ps_modify.inc context around the REPLACE
	cat := catalog.New()
	stor := storage.NewEngine()
	exec := New(cat, stor)
	
	must := func(sql string) {
		_, err := exec.Execute(sql)
		if err != nil {
			t.Fatalf("Query %q failed: %v", sql, err)
		}
	}

	must("CREATE DATABASE IF NOT EXISTS test")
	exec.CurrentDB = "test"
	must("CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(30))")
	
	// ps_renew setup
	must("DELETE FROM t1")
	must("INSERT INTO t1 VALUES (1,'one'), (2,'two'), (3,'three'), (4,'four')")
	
	// From ps_modify.inc - insert section setup
	must("SET @1000=1000")
	must("SET @x1000_2='x1000_2'")
	must("SET @x1000_3='x1000_3'")
	must("SET @x1000='x1000'")
	must("SET @1100=1100")
	must("SET @x1100='x1100'")
	must("SET @100=100")
	must("SET @updated='updated'")
	
	must("INSERT INTO t1 VALUES(1000,'x1000_1')")
	must("INSERT INTO t1 VALUES(@1000,@x1000_2),(@1000,@x1000_3) ON DUPLICATE KEY UPDATE a = a + @100, b = concat(b,@updated)")
	
	res, _ := exec.Execute("SELECT a,b FROM t1 WHERE a >= 1000 ORDER BY a")
	t.Logf("After first ODKU: %v", res.Rows)
	must("DELETE FROM t1 WHERE a >= 1000")
	
	must("INSERT INTO t1 VALUES(1000,'x1000_1')")
	must("PREPARE stmt1 FROM ' insert into t1 values(?,?),(?,?) on duplicate key update a = a + ?, b = concat(b,?) '")
	must("EXECUTE stmt1 USING @1000, @x1000_2, @1000, @x1000_3, @100, @updated")
	res, _ = exec.Execute("SELECT a,b FROM t1 WHERE a >= 1000 ORDER BY a")
	t.Logf("After prepared ODKU 1: %v", res.Rows)
	must("DELETE FROM t1 WHERE a >= 1000")
	
	must("INSERT INTO t1 VALUES(1000,'x1000_1')")
	must("EXECUTE stmt1 USING @1000, @x1000_2, @1100, @x1000_3, @100, @updated")
	res, _ = exec.Execute("SELECT a,b FROM t1 WHERE a >= 1000 ORDER BY a")
	t.Logf("After prepared ODKU 2: %v", res.Rows)
	must("DELETE FROM t1 WHERE a >= 1000")
	
	// Check table contents before REPLACE
	res, _ = exec.Execute("SELECT * FROM t1 ORDER BY a")
	t.Logf("t1 before REPLACE: %v", res.Rows)

	// Now the REPLACE
	must("PREPARE stmt1 FROM ' replace into t1 (a,b) select 100, ''hundred'' '")
	must("EXECUTE stmt1")
	
	res, _ = exec.Execute("SELECT * FROM t1 WHERE a=100")
	t.Logf("After first REPLACE execute: %v", res.Rows)
	
	must("EXECUTE stmt1") // Should not fail with duplicate key
	must("EXECUTE stmt1")
	
	res, _ = exec.Execute("SELECT * FROM t1 WHERE a=100")
	t.Logf("After three REPLACE executes: %v", res.Rows)
	
	if len(res.Rows) != 1 {
		t.Errorf("Expected 1 row with a=100, got %d rows", len(res.Rows))
	}
}
