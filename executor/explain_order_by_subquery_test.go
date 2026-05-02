package executor

import (
	"strings"
	"testing"
)

// TestExplain_OrderBySubquery_OptimizedAway verifies that when the outer
// ORDER BY references a non-correlated subquery (an "optimizer-time"
// subquery), MySQL EXPLAIN FORMAT=JSON wraps the table block in
// `ordering_operation` with `using_filesort: false` and emits the
// subquery as `optimized_away_subqueries` (instead of
// `attached_subqueries`).  Refs #264.
func TestExplain_OrderBySubquery_OptimizedAway(t *testing.T) {
	e := newUnionExec(t)
	res, err := e.Execute(`EXPLAIN FORMAT=JSON SELECT * FROM t1 ORDER BY (SELECT LENGTH(1) FROM t2 LIMIT 1)`)
	if err != nil {
		t.Fatalf("EXPLAIN FORMAT=JSON failed: %v", err)
	}
	jsonStr, _ := res.Rows[0][0].(string)
	if !strings.Contains(jsonStr, `"ordering_operation":`) {
		t.Errorf("expected ordering_operation block; got:\n%s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"using_filesort": false`) {
		t.Errorf("expected using_filesort: false; got:\n%s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"optimized_away_subqueries":`) {
		t.Errorf("expected optimized_away_subqueries; got:\n%s", jsonStr)
	}
	if strings.Contains(jsonStr, `"attached_subqueries":`) {
		t.Errorf("did not expect attached_subqueries (subquery should be optimized_away); got:\n%s", jsonStr)
	}
	// The ordering_operation must wrap both the table and the subqueries,
	// with subqueries appearing after the table.
	idxOrder := strings.Index(jsonStr, `"ordering_operation"`)
	idxTable := strings.Index(jsonStr, `"table":`)
	idxOptAway := strings.Index(jsonStr, `"optimized_away_subqueries"`)
	if idxOrder < 0 || idxTable < 0 || idxOptAway < 0 {
		t.Fatalf("expected ordering_operation, table, optimized_away_subqueries; got:\n%s", jsonStr)
	}
	if !(idxOrder < idxTable && idxTable < idxOptAway) {
		t.Errorf("expected order ordering_operation, table, optimized_away_subqueries; got positions %d, %d, %d",
			idxOrder, idxTable, idxOptAway)
	}
}
