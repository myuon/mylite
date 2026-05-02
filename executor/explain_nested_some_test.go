package executor

import (
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// TestExplain_NestedSomeInSemijoin reproduces the failing case from
// other/explain_json_all/none around line 1015:
//
//	EXPLAIN SELECT * FROM t1
//	 WHERE t1.a IN (SELECT c FROM t2
//	                  WHERE (SELECT e FROM t3) < SOME (SELECT e FROM t3 WHERE t1.b))
//
// MySQL emits:
//
//	1 PRIMARY t1
//	1 PRIMARY t2
//	4 DEPENDENT SUBQUERY t3
//	3 SUBQUERY t3
//
// Before this fix, mylite emitted both nested SUBQUERY rows with id=3
// because mergedDependent collapsed the second sibling subquery's id onto
// the first sibling's allocated id (instead of the parent IN-subquery's id).
// After the fix, the two nested subqueries get distinct ids and MySQL's
// DEPENDENT-first display order is reproduced.
func TestExplain_NestedSomeInSemijoin(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"
	for _, q := range []string{
		"CREATE TABLE t1 (a INT, b INT)",
		"CREATE TABLE t2 (c INT, d INT)",
		"CREATE TABLE t3 (e INT)",
		"INSERT INTO t1 VALUES (1,10), (2,10)",
		"INSERT INTO t2 VALUES (2,10), (2,20)",
		"INSERT INTO t3 VALUES (10), (30)",
	} {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	res, err := e.Execute(`EXPLAIN SELECT * FROM t1 WHERE t1.a IN (SELECT c FROM t2 WHERE (SELECT e FROM t3) < SOME(SELECT e FROM t3 WHERE t1.b))`)
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	type want struct {
		id    int64
		st    string
		table interface{}
	}
	expected := []want{
		{1, "PRIMARY", "t1"},
		{1, "PRIMARY", "t2"},
		{4, "DEPENDENT SUBQUERY", "t3"},
		{3, "SUBQUERY", "t3"},
	}
	if len(res.Rows) != len(expected) {
		t.Fatalf("expected %d rows, got %d: %v", len(expected), len(res.Rows), res.Rows)
	}
	for i, w := range expected {
		r := res.Rows[i]
		id, _ := r[0].(int64)
		st, _ := r[1].(string)
		if id != w.id || st != w.st || r[2] != w.table {
			t.Errorf("row %d: want (id=%d st=%q table=%v), got (id=%v st=%q table=%v)",
				i, w.id, w.st, w.table, r[0], r[1], r[2])
		}
	}
}
