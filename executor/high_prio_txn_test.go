package executor

import (
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// TestHighPrioTxnPreemption simulates the high_prio_trx_1 test scenario:
//   T1 (con1): START TRANSACTION; UPDATE t1 SET c1=1 WHERE c1=0;
//   T2 (con2): START TRANSACTION (HIGH PRIORITY); UPDATE t1 SET c1=2 WHERE c1=0;
//   T2: COMMIT; (should succeed, c1=2)
//   T1: COMMIT; (should fail with ER_ERROR_DURING_COMMIT)
func TestHighPrioTxnPreemption(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)

	// Setup
	for _, q := range []string{
		"CREATE DATABASE IF NOT EXISTS test",
	} {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup db: %v", err)
		}
	}
	e.CurrentDB = "test"

	for _, q := range []string{
		"CREATE TABLE t1 (c1 INT NOT NULL PRIMARY KEY) ENGINE=InnoDB",
		"INSERT INTO t1 VALUES (0)",
	} {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup table: %v", err)
		}
	}

	// Create two connection executors (con1 and con2)
	con1 := e.Clone()
	con1.CurrentDB = "test"
	con2 := e.Clone()
	con2.CurrentDB = "test"

	// con1: START TRANSACTION
	if _, err := con1.Execute("START TRANSACTION"); err != nil {
		t.Fatalf("con1 START TRANSACTION: %v", err)
	}

	// con1: UPDATE t1 SET c1=1 WHERE c1=0
	r, err := con1.Execute("UPDATE t1 SET c1=1 WHERE c1=0")
	if err != nil {
		t.Fatalf("con1 UPDATE: %v", err)
	}
	t.Logf("con1 UPDATE result: affected=%d", r.AffectedRows)

	// Verify the table now has c1=1
	result, err := e.Execute("SELECT c1 FROM t1")
	if err != nil {
		t.Fatalf("verify after con1 UPDATE: %v", err)
	}
	t.Logf("After con1 UPDATE (via default conn): rows=%v", result.Rows)

	// con2: SET GLOBAL DEBUG='+d,dbug_set_high_prio_trx'
	if _, err := con2.Execute("SET GLOBAL DEBUG='+d,dbug_set_high_prio_trx'"); err != nil {
		t.Fatalf("con2 SET GLOBAL DEBUG: %v", err)
	}

	// con2: START TRANSACTION (will be high-priority)
	if _, err := con2.Execute("START TRANSACTION"); err != nil {
		t.Fatalf("con2 START TRANSACTION: %v", err)
	}
	t.Logf("con2 isHighPriority=%v", con2.isHighPriority)

	// con2: SET GLOBAL DEBUG='-d,dbug_set_high_prio_trx'
	if _, err := con2.Execute("SET GLOBAL DEBUG='-d,dbug_set_high_prio_trx'"); err != nil {
		t.Fatalf("con2 SET GLOBAL DEBUG (clear): %v", err)
	}

	// con2: UPDATE t1 SET c1=2 WHERE c1=0 (high-priority, should preempt con1)
	t.Logf("con2 starting high-priority UPDATE...")
	r2, err2 := con2.Execute("UPDATE t1 SET c1=2 WHERE c1=0")
	if err2 != nil {
		t.Fatalf("con2 UPDATE (high-priority): %v", err2)
	}
	t.Logf("con2 UPDATE result: affected=%d", r2.AffectedRows)

	// con2: COMMIT (should succeed)
	if _, err := con2.Execute("COMMIT"); err != nil {
		t.Fatalf("con2 COMMIT: %v", err)
	}
	t.Logf("con2 COMMIT succeeded")

	// Verify the table now has c1=2
	result, err = e.Execute("SELECT c1 FROM t1")
	if err != nil {
		t.Fatalf("verify after con2 COMMIT: %v", err)
	}
	t.Logf("After con2 COMMIT: rows=%v", result.Rows)
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Fatalf("expected c1=2, got %v", result.Rows[0][0])
	}

	// con1: COMMIT (should fail with ER_ERROR_DURING_COMMIT = 1180)
	_, err = con1.Execute("COMMIT")
	if err == nil {
		t.Fatalf("con1 COMMIT should have failed with ER_ERROR_DURING_COMMIT")
	}
	t.Logf("con1 COMMIT error (expected): %v", err)
	if !isMySQLError(err, 1180) {
		t.Fatalf("expected MySQL error 1180, got: %v", err)
	}
	t.Logf("Test PASSED: con1 got ER_ERROR_DURING_COMMIT as expected")
}
