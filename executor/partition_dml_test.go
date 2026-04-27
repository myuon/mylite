package executor

import (
	"fmt"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

func TestPartitionDML(t *testing.T) {
	stor := storage.NewEngine()
	cat := catalog.New()
	exec := New(cat, stor)

	must := func(sql string) {
		if _, err := exec.Execute(sql); err != nil {
			t.Fatalf("Error in %q: %v", sql, err)
		}
	}

	must("CREATE DATABASE IF NOT EXISTS test")
	must("USE test")

	must(`CREATE TABLE t1 (a INT NOT NULL, b VARCHAR(64), PRIMARY KEY(a))
PARTITION BY RANGE (a) (
  PARTITION pNeg VALUES LESS THAN (0),
  PARTITION p0_29 VALUES LESS THAN (30),
  PARTITION p30_299 VALUES LESS THAN (300),
  PARTITION pMax VALUES LESS THAN MAXVALUE
)`)

	for _, v := range []int{-5, 5, 10, 50, 100, 500} {
		must(fmt.Sprintf("INSERT INTO t1 VALUES (%d, 'val%d')", v, v))
	}

	// Test SELECT FROM PARTITION
	result, err := exec.Execute("SELECT * FROM t1 PARTITION (p0_29) ORDER BY a")
	if err != nil {
		t.Fatalf("SELECT PARTITION error: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows from p0_29, got %d: %v", len(result.Rows), result.Rows)
	}
	t.Logf("SELECT from p0_29: %v", result.Rows)

	// Test DELETE FROM PARTITION
	res, err := exec.Execute("DELETE FROM t1 PARTITION (p30_299)")
	if err != nil {
		t.Fatalf("DELETE PARTITION error: %v", err)
	}
	if res.AffectedRows != 2 {
		t.Errorf("Expected 2 rows deleted from p30_299, got %d", res.AffectedRows)
	}

	result, err = exec.Execute("SELECT * FROM t1 ORDER BY a")
	if err != nil {
		t.Fatalf("SELECT error: %v", err)
	}
	if len(result.Rows) != 4 {
		t.Errorf("Expected 4 rows after delete, got %d: %v", len(result.Rows), result.Rows)
	}

	// Test SELECT with multiple partitions
	result, err = exec.Execute("SELECT * FROM t1 PARTITION (pNeg, p0_29) ORDER BY a")
	if err != nil {
		t.Fatalf("SELECT multi-partition error: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows from pNeg+p0_29, got %d: %v", len(result.Rows), result.Rows)
	}
	t.Logf("SELECT from pNeg+p0_29: %v", result.Rows)
}

func TestPartitionDMLInsert(t *testing.T) {
	stor := storage.NewEngine()
	cat := catalog.New()
	exec := New(cat, stor)

	must := func(sql string) {
		if _, err := exec.Execute(sql); err != nil {
			t.Fatalf("Error in %q: %v", sql, err)
		}
	}

	must("CREATE DATABASE IF NOT EXISTS test")
	must("USE test")

	must(`CREATE TABLE t1 (a INT NOT NULL, b VARCHAR(64), PRIMARY KEY(a))
PARTITION BY RANGE (a) (
  PARTITION p0_29 VALUES LESS THAN (30),
  PARTITION p30_299 VALUES LESS THAN (300)
)`)

	// Valid INSERT: row belongs to p0_29
	must("INSERT INTO t1 PARTITION (p0_29) VALUES (5, 'five')")

	// Invalid INSERT: row 50 does not belong to p0_29
	_, err := exec.Execute("INSERT INTO t1 PARTITION (p0_29) VALUES (50, 'fifty')")
	if err == nil {
		t.Error("Expected error for row not matching partition, got nil")
	} else {
		t.Logf("Got expected error: %v", err)
	}

	// Valid INSERT for p30_299
	must("INSERT INTO t1 PARTITION (p30_299) VALUES (50, 'fifty')")

	result, err := exec.Execute("SELECT * FROM t1 ORDER BY a")
	if err != nil {
		t.Fatalf("SELECT error: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d: %v", len(result.Rows), result.Rows)
	}
}

func TestPartitionDMLSubpartitions(t *testing.T) {
	stor := storage.NewEngine()
	cat := catalog.New()
	exec := New(cat, stor)

	must := func(sql string) {
		if _, err := exec.Execute(sql); err != nil {
			t.Fatalf("Error in %q: %v", sql, err)
		}
	}

	must("CREATE DATABASE IF NOT EXISTS test")
	must("USE test")

	must(`CREATE TABLE t1 (a INT NOT NULL, b VARCHAR(64), PRIMARY KEY(a))
PARTITION BY RANGE (a)
SUBPARTITION BY HASH (a) SUBPARTITIONS 3
(
  PARTITION pNeg VALUES LESS THAN (0)
  (SUBPARTITION subp0, SUBPARTITION subp1, SUBPARTITION subp2),
  PARTITION p0_29 VALUES LESS THAN (30)
  (SUBPARTITION subp3, SUBPARTITION subp4, SUBPARTITION subp5)
)`)

	for _, v := range []int{-3, -2, -1, 0, 1, 2, 3, 10} {
		must(fmt.Sprintf("INSERT INTO t1 VALUES (%d, 'val%d')", v, v))
	}

	// Test SELECT FROM PARTITION (parent partition)
	result, err := exec.Execute("SELECT * FROM t1 PARTITION (p0_29) ORDER BY a")
	if err != nil {
		t.Fatalf("SELECT PARTITION p0_29 error: %v", err)
	}
	t.Logf("p0_29 rows: %v", result.Rows)
	if len(result.Rows) != 5 {
		t.Errorf("Expected 5 rows from p0_29 (0,1,2,3,10), got %d: %v", len(result.Rows), result.Rows)
	}

	// Test SELECT FROM subpartition
	result, err = exec.Execute("SELECT * FROM t1 PARTITION (subp3) ORDER BY a")
	if err != nil {
		t.Fatalf("SELECT PARTITION subp3 error: %v", err)
	}
	t.Logf("subp3 rows: %v", result.Rows)
}
