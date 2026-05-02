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

// TestExplain_CrossJoin_SmallerTableDrives_JSON: for a pure two-table
// Cartesian product with no ON/WHERE, MySQL drives the join from the smaller
// table.  In our fixture t1 has 7 rows, t2 has 2 rows; the JSON nested_loop
// must list t2 first (outer driver) and t1 second with using_join_buffer:BNL.
func TestExplain_CrossJoin_SmallerTableDrives_JSON(t *testing.T) {
	e := newUnionExec(t)
	res, err := e.Execute(`EXPLAIN FORMAT=JSON (SELECT t1.i FROM t1 JOIN t2) UNION ALL (SELECT * FROM t3 WHERE i IN (SELECT i FROM t4 ORDER BY RAND()))`)
	if err != nil {
		t.Fatalf("EXPLAIN FORMAT=JSON failed: %v", err)
	}
	jsonStr, _ := res.Rows[0][0].(string)
	idxT2 := strings.Index(jsonStr, `"table_name": "t2"`)
	idxT1 := strings.Index(jsonStr, `"table_name": "t1"`)
	if idxT2 < 0 || idxT1 < 0 {
		t.Fatalf("expected both table_name entries; got:\n%s", jsonStr)
	}
	if idxT2 > idxT1 {
		t.Errorf("expected smaller t2 before t1 in nested_loop; got t1 at %d before t2 at %d:\n%s", idxT1, idxT2, jsonStr)
	}
	// t1 (inner) should have using_join_buffer; t2 (outer driver) should not.
	idxBNL := strings.Index(jsonStr, `"using_join_buffer": "Block Nested Loop"`)
	if idxBNL < idxT1 {
		t.Errorf("expected using_join_buffer to appear under t1 (after %d); got at %d:\n%s", idxT1, idxBNL, jsonStr)
	}
}

// TestExplain_CrossJoin_OuterTable_NoUsedColumns_JSON: when only one table's
// column is selected (here `t1.i`), the OUTER table (t2) emits no
// used_columns block.  An unqualified column reference inside a separate
// UNION branch's subquery (e.g. `i IN (SELECT i FROM t4 …)`) must NOT pull
// `i` into t2's used_columns.
func TestExplain_CrossJoin_OuterTable_NoUsedColumns_JSON(t *testing.T) {
	e := newUnionExec(t)
	res, err := e.Execute(`EXPLAIN FORMAT=JSON (SELECT t1.i FROM t1 JOIN t2) UNION ALL (SELECT * FROM t3 WHERE i IN (SELECT i FROM t4 ORDER BY RAND()))`)
	if err != nil {
		t.Fatalf("EXPLAIN FORMAT=JSON failed: %v", err)
	}
	jsonStr, _ := res.Rows[0][0].(string)
	idxT2 := strings.Index(jsonStr, `"table_name": "t2"`)
	idxT1 := strings.Index(jsonStr, `"table_name": "t1"`)
	if idxT2 < 0 || idxT1 < 0 || idxT2 > idxT1 {
		t.Fatalf("expected nested_loop with t2 before t1; got:\n%s", jsonStr)
	}
	// The slice between t2's table_name and t1's table_name describes the
	// t2 entry. It must not contain a used_columns block.
	t2Block := jsonStr[idxT2:idxT1]
	if strings.Contains(t2Block, "used_columns") {
		t.Errorf("outer table t2 must not emit used_columns when only t1.i is selected; got:\n%s", t2Block)
	}
	// t1 (the inner table that supplies the selected column) should still
	// list `i` in used_columns.
	t1Block := jsonStr[idxT1:]
	if !strings.Contains(t1Block, `"used_columns": [`) || !strings.Contains(t1Block, `"i"`) {
		t.Errorf("inner table t1 should emit used_columns:[i]; got:\n%s", t1Block)
	}
}
