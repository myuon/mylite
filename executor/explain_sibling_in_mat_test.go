package executor

import (
	"strings"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// TestExplain_SiblingINSubqueriesBothMaterialized: when the outer WHERE has
// multiple sibling top-level `col IN (SELECT ...)` predicates combined with
// AND, MySQL keeps each one as its own materialized `<subqueryN>` placeholder
// rather than flattening one of them via DuplicateWeedout (which would force
// all tables onto id=1 SIMPLE rows, incompatible with multiple sibling
// `<subqueryN>` placeholders).
//
// Reproduces the "semi-join materialization (if enabled)" query from
// explain_json.inc:
//
//	EXPLAIN FORMAT=JSON SELECT * FROM t1
//	WHERE t1.a IN (SELECT t2.a FROM t2 WHERE t2.a > 0) AND
//	      t1.a IN (SELECT t3.a FROM t3 WHERE t3.a IN
//	                  (SELECT t4.a FROM t4 WHERE a > 0));
//
// Expected: TWO `<subqueryN>` placeholders (`<subquery2>` for t2, `<subquery3>`
// for t3+t4), both with `materialized_from_subquery`.
func TestExplain_SiblingINSubqueriesBothMaterialized(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"
	for _, q := range []string{
		"SET optimizer_switch='semijoin=on,materialization=on,firstmatch=on,loosescan=on,index_condition_pushdown=on,mrr=on'",
		"CREATE TABLE t1 (a INT)",
		"INSERT INTO t1 VALUES (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1)",
		"CREATE TABLE t2 (a INT) SELECT * FROM t1",
		"CREATE TABLE t3 (a INT) SELECT * FROM t1",
		"CREATE TABLE t4 (a INT) SELECT * FROM t1",
	} {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	res, err := e.Execute(`EXPLAIN FORMAT=JSON SELECT * FROM t1 WHERE t1.a IN (SELECT t2.a FROM t2 WHERE t2.a > 0) AND t1.a IN (SELECT t3.a FROM t3 WHERE t3.a IN (SELECT t4.a FROM t4 WHERE a > 0))`)
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 EXPLAIN row, got %d", len(res.Rows))
	}
	js, _ := res.Rows[0][0].(string)
	// Both sibling subqueries must materialize and survive as <subqueryN>
	// placeholders -- DuplicateWeedout flattening must NOT consume one of them.
	if !strings.Contains(js, `"<subquery2>"`) {
		t.Errorf("expected <subquery2> placeholder; got:\n%s", js)
	}
	if !strings.Contains(js, `"<subquery3>"`) {
		t.Errorf("expected <subquery3> placeholder; got:\n%s", js)
	}
	// Each placeholder block must carry materialized_from_subquery.
	if strings.Count(js, "materialized_from_subquery") < 2 {
		t.Errorf("expected 2+ materialized_from_subquery blocks; got:\n%s", js)
	}
	// Multi-table materialization (<subquery3> contains t3+t4) should appear
	// before the single-table <subquery2> in the outer nested_loop.
	idx3 := strings.Index(js, `"<subquery3>"`)
	idx2 := strings.Index(js, `"<subquery2>"`)
	if idx3 < 0 || idx2 < 0 || idx3 > idx2 {
		t.Errorf("expected <subquery3> to appear before <subquery2> (cost-based ordering); idx2=%d idx3=%d\n%s", idx2, idx3, js)
	}
	// JSON ref entries must be fully qualified with the database name
	// (`test.t1.a`), not the bare `t1.a` form used in tabular EXPLAIN.
	if !strings.Contains(js, `"test.t1.a"`) {
		t.Errorf("expected fully qualified ref `test.t1.a` in <subqueryN> placeholder; got:\n%s", js)
	}
}
