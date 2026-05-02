package executor

import (
	"strings"
	"testing"
)

// TestExplain_OptimizedAwaySubqueriesInUnionBranch_JSON verifies that when a
// UNION branch is collapsed to "no matching row in const table" because its
// outer table is empty, any inner subqueries in its WHERE/SELECT/HAVING are
// emitted as `optimized_away_subqueries` nested under that branch's
// query_block (matching MySQL's EXPLAIN FORMAT=JSON output).  Refs #264.
func TestExplain_OptimizedAwaySubqueriesInUnionBranch_JSON(t *testing.T) {
	e := newUnionExec(t)
	// Mirror dolt-mysql-tests/files/suite/other/t/explain_json_none.test:
	// disable semijoin and materialization so the IN subquery cannot be
	// flattened and is emitted as a SUBQUERY row.
	if _, err := e.Execute(`SET optimizer_switch='semijoin=off'`); err != nil {
		t.Fatalf("set optimizer_switch semijoin=off: %v", err)
	}
	if _, err := e.Execute(`SET optimizer_switch='materialization=off'`); err != nil {
		t.Fatalf("set optimizer_switch materialization=off: %v", err)
	}
	res, err := e.Execute(`EXPLAIN FORMAT=JSON (SELECT t1.i FROM t1 JOIN t2) UNION ALL (SELECT * FROM t3 WHERE i IN (SELECT i FROM t4 ORDER BY RAND()))`)
	if err != nil {
		t.Fatalf("EXPLAIN FORMAT=JSON failed: %v", err)
	}
	jsonStr, _ := res.Rows[0][0].(string)
	if !strings.Contains(jsonStr, `"optimized_away_subqueries":`) {
		t.Errorf("expected optimized_away_subqueries block in UNION branch with empty outer table; got:\n%s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"message": "Not optimized, outer query is empty"`) {
		t.Errorf("expected nested subquery message; got:\n%s", jsonStr)
	}
	// The optimized_away_subqueries block must be nested INSIDE the
	// select_id=2 query_block, not at the outer union_result level.
	idxBranch2 := strings.Index(jsonStr, `"select_id": 2`)
	idxOptAway := strings.Index(jsonStr, `"optimized_away_subqueries"`)
	idxBranch3 := strings.Index(jsonStr, `"select_id": 3`)
	if idxBranch2 < 0 || idxOptAway < 0 || idxBranch3 < 0 {
		t.Fatalf("expected select_id=2, optimized_away_subqueries, and select_id=3 markers; got:\n%s", jsonStr)
	}
	if !(idxBranch2 < idxOptAway && idxOptAway < idxBranch3) {
		t.Errorf("expected order: select_id=2, optimized_away_subqueries, select_id=3; got positions %d, %d, %d:\n%s",
			idxBranch2, idxOptAway, idxBranch3, jsonStr)
	}
}
