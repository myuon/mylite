package executor

import (
	"fmt"
	"testing"
)

func TestMultiTableMaterialized(t *testing.T) {
	e := newTestExecutor(t)
	// Set up t0
	if _, err := e.Execute("CREATE TABLE t0 (a INT)"); err != nil {
		t.Fatalf("create t0: %v", err)
	}
	for i := 0; i < 10; i++ {
		if _, err := e.Execute(fmt.Sprintf("INSERT INTO t0 VALUES (%d)", i)); err != nil {
			t.Fatalf("insert t0: %v", err)
		}
	}

	if _, err := e.Execute("DROP TABLE IF EXISTS t1"); err != nil {
		t.Fatalf("drop t1: %v", err)
	}
	if _, err := e.Execute("CREATE TABLE t1 (kp1 INT, kp2 INT, c INT, filler CHAR(100), KEY(kp1, kp2))"); err != nil {
		t.Fatalf("create t1: %v", err)
	}
	for a := 0; a < 10; a++ {
		for b := 0; b < 10; b++ {
			for c := 0; c < 10; c++ {
				v := a + 10*(b+10*c)
				if _, err := e.Execute(fmt.Sprintf("INSERT INTO t1 VALUES (%d, 0, 0, 'filler')", v)); err != nil {
					t.Fatalf("insert t1: %v", err)
				}
			}
		}
	}
	for i := 0; i < 20; i++ {
		if _, err := e.Execute(fmt.Sprintf("INSERT INTO t1 VALUES (%d, 0, 0, 'filler')", i)); err != nil {
			t.Fatalf("insert t1 dup: %v", err)
		}
	}

	if _, err := e.Execute("CREATE TABLE t3 (a INT)"); err != nil {
		t.Fatalf("create t3: %v", err)
	}
	for a := 0; a < 10; a++ {
		for b := 0; b < 10; b++ {
			v := a + 10*b
			if _, err := e.Execute(fmt.Sprintf("INSERT INTO t3 VALUES (%d)", v)); err != nil {
				t.Fatalf("insert t3: %v", err)
			}
		}
	}

	if _, err := e.Execute("CREATE TABLE t4 (pk INT PRIMARY KEY)"); err != nil {
		t.Fatalf("create t4: %v", err)
	}
	if _, err := e.Execute("INSERT INTO t4 SELECT a FROM t3"); err != nil {
		t.Fatalf("insert t4: %v", err)
	}

	res, err := e.Execute("EXPLAIN select * from t3 where a in (select t1.kp1 from t1,t4 where kp1<20 and t4.pk=t1.c)")
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	t.Logf("EXPLAIN rows:")
	for _, row := range res.Rows {
		t.Logf("  %v", row)
	}
}
