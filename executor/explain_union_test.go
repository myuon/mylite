package executor

import (
	"strings"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// helper: t1 (7 rows), t2 (2 rows), t3 (empty), t4 (empty) — same shape as
// dolt-mysql-tests/files/include/explain_json.inc fixtures.
func newUnionExec(t *testing.T) *Executor {
	t.Helper()
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"
	for _, q := range []string{
		"CREATE TABLE t1 (i INT)",
		"CREATE TABLE t2 (i INT)",
		"CREATE TABLE t3 (i INT)",
		"CREATE TABLE t4 (i INT)",
		"INSERT INTO t1 VALUES (1), (2), (3), (4), (5), (6), (7)",
		"INSERT INTO t2 VALUES (1), (2)",
	} {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	return e
}

// TestExplain_UnionEmptyBranch_Text: a UNION branch over an empty table emits
// "no matching row in const table" instead of a normal table row.
func TestExplain_UnionEmptyBranch_Text(t *testing.T) {
	e := newUnionExec(t)
	res, err := e.Execute(`EXPLAIN SELECT * FROM t1 UNION SELECT * FROM t2 UNION SELECT * FROM t3`)
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	if len(res.Rows) != 4 {
		t.Fatalf("expected 4 rows (PRIMARY, UNION, UNION-empty, UNION RESULT), got %d: %v", len(res.Rows), res.Rows)
	}
	// Third row: empty UNION branch over t3
	r := res.Rows[2]
	if id, ok := r[0].(int64); !ok || id != 3 {
		t.Errorf("third row id: want 3, got %v", r[0])
	}
	if r[2] != nil {
		t.Errorf("third row table: want nil, got %v", r[2])
	}
	extra, _ := r[11].(string)
	if !strings.Contains(extra, "no matching row in const table") {
		t.Errorf("third row extra: want 'no matching row in const table', got %q", extra)
	}
}

// TestExplain_UnionAllNoUnionResult_Text: UNION ALL with no DISTINCT step does
// not need a temporary table and so MySQL skips the UNION RESULT row in
// tabular EXPLAIN.
func TestExplain_UnionAllNoUnionResult_Text(t *testing.T) {
	e := newUnionExec(t)
	res, err := e.Execute(`EXPLAIN (SELECT t1.i FROM t1 JOIN t2) UNION ALL (SELECT * FROM t3 WHERE i IN (SELECT i FROM t4 ORDER BY RAND()))`)
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	for _, r := range res.Rows {
		if st, _ := r[1].(string); st == "UNION RESULT" {
			t.Errorf("UNION ALL chain should not emit UNION RESULT row in text EXPLAIN, got: %v", r)
		}
	}
}

// TestExplain_UnionEmptyBranch_JSON: the EXPLAIN FORMAT=JSON output for a
// UNION (DISTINCT) wraps each branch in a query_specifications entry with
// dependent:false, cacheable:true, and emits message:"no matching row in
// const table" for the empty branch.
func TestExplain_UnionEmptyBranch_JSON(t *testing.T) {
	e := newUnionExec(t)
	res, err := e.Execute(`EXPLAIN FORMAT=JSON SELECT * FROM t1 UNION SELECT * FROM t2 UNION SELECT * FROM t3`)
	if err != nil {
		t.Fatalf("EXPLAIN FORMAT=JSON failed: %v", err)
	}
	if len(res.Rows) == 0 || res.Rows[0][0] == nil {
		t.Fatalf("expected JSON result, got %v", res.Rows)
	}
	jsonStr, _ := res.Rows[0][0].(string)
	if !strings.Contains(jsonStr, `"union_result":`) {
		t.Errorf("expected union_result block; got:\n%s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"using_temporary_table": true`) {
		t.Errorf("expected using_temporary_table:true for UNION DISTINCT; got:\n%s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"dependent": false`) || !strings.Contains(jsonStr, `"cacheable": true`) {
		t.Errorf("expected dependent:false, cacheable:true on each branch; got:\n%s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"message": "no matching row in const table"`) {
		t.Errorf("expected message for empty t3 branch; got:\n%s", jsonStr)
	}
	// Outer query_block should NOT have a select_id (UNION at top emits
	// only union_result inside query_block).
	if strings.Contains(jsonStr, `"query_block": {
    "select_id"`) {
		t.Errorf("outer query_block should not contain select_id at top for UNION; got:\n%s", jsonStr)
	}
}

// TestExplain_UnionAll_JSON: pure UNION ALL emits union_result with
// using_temporary_table:false and no table_name/access_type.
func TestExplain_UnionAll_JSON(t *testing.T) {
	e := newUnionExec(t)
	res, err := e.Execute(`EXPLAIN FORMAT=JSON (SELECT t1.i FROM t1 JOIN t2) UNION ALL (SELECT * FROM t3 WHERE i IN (SELECT i FROM t4 ORDER BY RAND()))`)
	if err != nil {
		t.Fatalf("EXPLAIN FORMAT=JSON failed: %v", err)
	}
	jsonStr, _ := res.Rows[0][0].(string)
	if !strings.Contains(jsonStr, `"using_temporary_table": false`) {
		t.Errorf("expected using_temporary_table:false for UNION ALL; got:\n%s", jsonStr)
	}
	if strings.Contains(jsonStr, `"table_name": "<union`) {
		t.Errorf("UNION ALL union_result should not contain a <unionN,M> table_name; got:\n%s", jsonStr)
	}
	// JOIN inside UNION ALL branch should produce nested_loop with both tables.
	if !strings.Contains(jsonStr, `"nested_loop":`) {
		t.Errorf("expected nested_loop for JOIN branch; got:\n%s", jsonStr)
	}
}
