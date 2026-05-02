package executor

import (
	"strings"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// Verifies that EXPLAIN FORMAT=JSON for a semijoin (IN→FirstMatch) with
// nested subqueries emits a `nested_loop` wrapping all primary tables and
// keeps `attached_subqueries` (with proper dependent/cacheable flags) on the
// driving table block.  Reproduces the structure mismatch around line 942
// of explain_json_all / line 997 of explain_json_none.
func TestExplain_NestedSomeInSemijoin_JSON(t *testing.T) {
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
		"INSERT INTO t1 VALUES (1,10), (2,10)",
		"INSERT INTO t2 VALUES (2,10), (2,20)",
		"INSERT INTO t3 VALUES (10), (30)",
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
	if !strings.Contains(js, `"nested_loop"`) {
		t.Errorf("expected nested_loop in JSON output, got:\n%s", js)
	}
	if !strings.Contains(js, `"first_match": "t1"`) {
		t.Errorf("expected first_match: t1 in JSON output, got:\n%s", js)
	}
	// Both attached subqueries must be wrapped with dependent/cacheable.
	if strings.Count(js, `"dependent":`) < 2 {
		t.Errorf("expected at least 2 dependent flags (one per attached_subquery), got:\n%s", js)
	}
}
