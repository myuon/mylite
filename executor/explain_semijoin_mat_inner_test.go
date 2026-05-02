package executor

import (
	"strings"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// TestExplain_FirstMatchInnerAttachedCondition_JSON: when MySQL semijoin-flattens
// an IN-subquery using the FirstMatch strategy and neither side has an extra
// literal predicate on the join column, the inner table block carries the
// IN-equality itself as `attached_condition` (e.g. `(t2.c = t1.a)`).
//
// Reproduces the inner-side attached_condition gap on the FirstMatch t2 table
// of the "complex subqueries" query in explain_json.inc:
//
//	EXPLAIN FORMAT=JSON SELECT * FROM t1
//	WHERE t1.a IN (SELECT c FROM t2
//	               WHERE (SELECT e FROM t3) < SOME(SELECT e FROM t3 WHERE t1.b));
func TestExplain_FirstMatchInnerAttachedCondition_JSON(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"
	for _, q := range []string{
		"SET optimizer_switch='semijoin=on,materialization=on,firstmatch=on,loosescan=on,index_condition_pushdown=on,mrr=on'",
		"CREATE TABLE t1 (a INT, b INT)",
		"CREATE TABLE t2 (c INT, d INT)",
		"CREATE TABLE t3 (e INT)",
		"CREATE TABLE t4 (f INT, g INT)",
		"INSERT INTO t1 VALUES (1,10), (2,10)",
		"INSERT INTO t2 VALUES (2,10), (2,20)",
		"INSERT INTO t3 VALUES (10), (30)",
		"INSERT INTO t4 VALUES (2,10), (2,10)",
	} {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	res, err := e.Execute(`EXPLAIN FORMAT=JSON SELECT * FROM t1 WHERE t1.a IN (SELECT c FROM t2 WHERE (SELECT e FROM t3) < SOME(SELECT e FROM t3 WHERE t1.b))`)
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 EXPLAIN row, got %d", len(res.Rows))
	}
	js, _ := res.Rows[0][0].(string)
	// FirstMatch t2 inner block: must carry attached_condition with the
	// IN-equality `t2.c = t1.a` (MySQL emits both column refs).
	if !strings.Contains(js, `"first_match"`) {
		t.Fatalf("expected first_match annotation in JSON:\n%s", js)
	}
	// Find the t2 table block and ensure it has attached_condition mentioning t2.c and t1.a.
	idx := strings.Index(js, `"table_name": "t2"`)
	if idx < 0 {
		t.Fatalf("missing t2 table_name in JSON:\n%s", js)
	}
	end := idx + 1200
	if end > len(js) {
		end = len(js)
	}
	segment := js[idx:end]
	if !strings.Contains(segment, `"attached_condition"`) {
		t.Errorf("FirstMatch t2 block missing attached_condition; segment:\n%s", segment)
	}
	if !strings.Contains(segment, "`t2`.`c`") || !strings.Contains(segment, "`t1`.`a`") {
		t.Errorf("expected IN-equality (t2.c = t1.a) in t2's attached_condition; segment:\n%s", segment)
	}
}
