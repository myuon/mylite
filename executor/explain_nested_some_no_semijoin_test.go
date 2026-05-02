package executor

import (
	"strings"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// Verifies that EXPLAIN FORMAT=JSON for a query whose IN-subquery body itself
// contains nested scalar subqueries (here `(SELECT e FROM t3)` and
// `SOME(SELECT e FROM t3 WHERE t1.b)`) places those nested subqueries inside
// the outer IN-subquery's table block as `attached_subqueries`, rather than
// flattening them all under the outermost table block.
//
// Reproduces the structure mismatch in explain_json_none around line 1015 of
// the normalized output.  When semijoin/firstmatch are off, the outer plan is
// a single table (t1) with attached_subqueries: [select#2 t2], and select#2
// itself carries attached_subqueries: [select#4 dep, select#3 cacheable].
func TestExplain_NestedSomeNoSemijoin_JSON(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"
	for _, q := range []string{
		"SET optimizer_switch='semijoin=off,materialization=off,index_condition_pushdown=off,mrr=off'",
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
	// There should be NO nested_loop (no semijoin/firstmatch).
	if strings.Contains(js, `"nested_loop"`) {
		t.Errorf("did not expect nested_loop with semijoin off, got:\n%s", js)
	}
	// Outer t1 has attached_subqueries with select_id=2.
	if !strings.Contains(js, `"select_id": 2`) {
		t.Errorf("expected select_id=2 in attached_subqueries, got:\n%s", js)
	}
	// select#2's t2 block must carry its own nested attached_subqueries
	// containing select#3 and select#4.  We check that the substring
	// `"select_id": 4` appears AFTER `"select_id": 2` in the JSON, and
	// that the JSON contains at least two `"attached_subqueries"` keys
	// (one on t1, one on t2).
	if strings.Count(js, `"attached_subqueries"`) < 2 {
		t.Errorf("expected nested attached_subqueries on t2's block, got:\n%s", js)
	}
	idx2 := strings.Index(js, `"select_id": 2`)
	idx3 := strings.Index(js, `"select_id": 3`)
	idx4 := strings.Index(js, `"select_id": 4`)
	if idx2 < 0 || idx3 < 0 || idx4 < 0 {
		t.Fatalf("missing select_ids 2/3/4 in JSON: %s", js)
	}
	if !(idx2 < idx4 && idx2 < idx3) {
		t.Errorf("expected select#3 and select#4 to appear after select#2, got positions 2=%d 3=%d 4=%d", idx2, idx3, idx4)
	}
	// MySQL's display order: DEPENDENT first, then non-DEPENDENT.
	// select#4 is the DEPENDENT (correlated SOME), select#3 is the cacheable
	// (uncorrelated `(SELECT e FROM t3)`).
	if !(idx4 < idx3) {
		t.Errorf("expected DEPENDENT select#4 to appear before cacheable select#3 in attached_subqueries, got 3=%d 4=%d", idx3, idx4)
	}
	// select#4's t3 block must carry an attached_condition (the IN→EXISTS
	// rewriter's `<if>(outer_field_is_not_null, ...)` marker).  mtrrun masks
	// the value to "#" so the textual content does not matter, but the field
	// must be present.
	if !strings.Contains(js, "outer_field_is_not_null") {
		t.Errorf("expected select#4 t3 block to carry an attached_condition with outer_field_is_not_null marker, got:\n%s", js)
	}
}
