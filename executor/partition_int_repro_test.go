package executor

import (
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// TestPartitionIntInnodbBigintPKLookupMaxRow guards the fix for the
// parts/partition_int_innodb regression where a primary-key lookup for a
// BIGINT value near math.MaxInt64 missed its row.
//
// The underlying bug was in storage.valuesEqualLoose, which routed equality
// through float64. float64 cannot distinguish 9223372036854775806 from
// 9223372036854775807, so SELECT/DELETE WHERE a=9223372036854775806 either
// matched the wrong row or returned nothing.
func TestPartitionIntInnodbBigintPKLookupMaxRow(t *testing.T) {
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
	must(`CREATE TABLE t3 (a bigint NOT NULL, primary key(a))
ENGINE='InnoDB' partition by key (a) partitions 7`)
	must(`insert into t3 values (9223372036854775807), (9223372036854775806),
(9223372036854775805), (9223372036854775804), (-9223372036854775808),
(-9223372036854775807), (1), (-1), (0)`)

	// All 9 rows must be present, including MaxInt64-1.
	if res, err := exec.Execute("select * from t3"); err != nil {
		t.Fatalf("select error: %v", err)
	} else if len(res.Rows) != 9 {
		t.Errorf("expected 9 rows after insert, got %d", len(res.Rows))
	}

	// PK equality lookup for MaxInt64-1 must return exactly one row.
	res, err := exec.Execute("select * from t3 where a=9223372036854775806")
	if err != nil {
		t.Fatalf("select where error: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Errorf("select where a=MaxInt64-1: expected 1 row, got %d", len(res.Rows))
	}

	// Same query for MaxInt64 must independently match exactly one row,
	// proving the two adjacent values are no longer aliased.
	if res, err := exec.Execute("select * from t3 where a=9223372036854775807"); err != nil {
		t.Fatalf("select error: %v", err)
	} else if len(res.Rows) != 1 {
		t.Errorf("select where a=MaxInt64: expected 1 row, got %d", len(res.Rows))
	}

	// Delete-by-PK on MaxInt64-1 should remove exactly that row.
	must("delete from t3 where a=9223372036854775806")
	if res, err := exec.Execute("select * from t3 where a=9223372036854775806"); err != nil {
		t.Fatalf("select error: %v", err)
	} else if len(res.Rows) != 0 {
		t.Errorf("after delete, expected 0 rows for MaxInt64-1, got %d", len(res.Rows))
	}
	if res, err := exec.Execute("select * from t3 where a=9223372036854775807"); err != nil {
		t.Fatalf("select error: %v", err)
	} else if len(res.Rows) != 1 {
		t.Errorf("after delete, MaxInt64 row should still exist, got %d rows", len(res.Rows))
	}
}
