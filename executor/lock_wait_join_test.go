package executor

import (
	"strconv"
	"strings"
	"testing"
)

func TestLockWaitTimeout_DeleteBlockedByJoinExaminedRow(t *testing.T) {
	e := newTestExecutor(t)
	if _, err := e.Execute("DROP TABLE IF EXISTS t1"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := e.Execute("CREATE TABLE t1 (a INT PRIMARY KEY, b INT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 1; i <= 20; i++ {
		if i == 2 {
			if _, err := e.Execute("INSERT INTO t1 VALUES (2, NULL)"); err != nil {
				t.Fatalf("insert: %v", err)
			}
			continue
		}
		if _, err := e.Execute("INSERT INTO t1 VALUES (" + strconv.Itoa(i) + ", " + strconv.Itoa(i) + ")"); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	if _, err := e.Execute(`SET SESSION transaction_isolation='read-committed'`); err != nil {
		t.Fatalf("set iso: %v", err)
	}
	if _, err := e.Execute(`SET SESSION innodb_lock_wait_timeout=1`); err != nil {
		t.Fatalf("set timeout: %v", err)
	}

	other := e.Clone()
	other.sessionScopeVars["innodb_lock_wait_timeout"] = "1"
	if _, err := e.Execute("BEGIN"); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := e.Execute(`SELECT 1 FROM t1 NATURAL JOIN (SELECT 2 AS a, 1 AS b UNION ALL SELECT 2 AS a, 2 AS b) AS t2 FOR UPDATE`); err != nil {
		t.Fatalf("lock: %v", err)
	}
	if _, err := other.Execute("BEGIN"); err != nil {
		t.Fatalf("begin other: %v", err)
	}
	_, err := other.Execute("DELETE FROM t1")
	if err == nil {
		t.Fatal("expected lock wait timeout on delete")
	}
	if !strings.Contains(err.Error(), "Lock wait timeout exceeded") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLockWaitTimeout_DeleteWhereA3BlockedByJoinForUpdate(t *testing.T) {
	e := newTestExecutor(t)
	if _, err := e.Execute("DROP TABLE IF EXISTS t1"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := e.Execute("CREATE TABLE t1 (a INT PRIMARY KEY, b INT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 1; i <= 20; i++ {
		if i == 2 {
			if _, err := e.Execute("INSERT INTO t1 VALUES (2, NULL)"); err != nil {
				t.Fatalf("insert: %v", err)
			}
			continue
		}
		if _, err := e.Execute("INSERT INTO t1 VALUES (" + strconv.Itoa(i) + ", 1)"); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	if _, err := e.Execute(`SET SESSION transaction_isolation='read-committed'`); err != nil {
		t.Fatalf("set iso: %v", err)
	}
	other := e.Clone()
	other.sessionScopeVars["innodb_lock_wait_timeout"] = "1"
	if _, err := e.Execute("BEGIN"); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := e.Execute(`SELECT 1 FROM t1 NATURAL JOIN (SELECT 3 AS a, 2 AS b UNION ALL SELECT 3 AS a, 1 AS b) AS t2 FOR UPDATE`); err != nil {
		t.Fatalf("lock: %v", err)
	}
	_, err := other.Execute("DELETE FROM t1 WHERE a=3")
	if err == nil {
		t.Fatal("expected lock wait timeout")
	}
	if !strings.Contains(err.Error(), "Lock wait timeout exceeded") {
		t.Fatalf("unexpected error: %v", err)
	}
}
