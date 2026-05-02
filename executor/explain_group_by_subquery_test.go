package executor

import (
	"strings"
	"testing"
)

// TestExplain_GroupBySubquery_GroupByQueriesArray verifies that when the
// outer GROUP BY references a subquery (e.g. ALL/ANY), MySQL EXPLAIN
// FORMAT=JSON wraps the table block in a `grouping_operation` with
// `using_temporary_table: true, using_filesort: false` and emits the
// subqueries as `group_by_subqueries` (not `attached_subqueries`).
// Refs #264.
func TestExplain_GroupBySubquery_GroupByQueriesArray(t *testing.T) {
	e := newUnionExec(t)
	res, err := e.Execute(
		`EXPLAIN FORMAT=JSON SELECT * FROM t1 GROUP BY i > ALL (SELECT i FROM t2) OR i < ALL (SELECT i FROM t2)`)
	if err != nil {
		t.Fatalf("EXPLAIN FORMAT=JSON failed: %v", err)
	}
	jsonStr, _ := res.Rows[0][0].(string)
	if !strings.Contains(jsonStr, `"grouping_operation":`) {
		t.Errorf("expected grouping_operation block; got:\n%s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"using_temporary_table": true`) {
		t.Errorf("expected using_temporary_table: true; got:\n%s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"using_filesort": false`) {
		t.Errorf("expected using_filesort: false; got:\n%s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"group_by_subqueries":`) {
		t.Errorf("expected group_by_subqueries; got:\n%s", jsonStr)
	}
	if strings.Contains(jsonStr, `"attached_subqueries":`) {
		t.Errorf("did not expect attached_subqueries (subquery should be group_by_subqueries); got:\n%s", jsonStr)
	}
	if strings.Contains(jsonStr, `"sort_cost":`) {
		t.Errorf("did not expect sort_cost (using_filesort is false); got:\n%s", jsonStr)
	}
	// Each group_by_subqueries entry must include dependent/cacheable flags
	// and an attached_condition on the inner table.
	if !strings.Contains(jsonStr, `"dependent": true`) {
		t.Errorf("expected dependent: true on group_by_subqueries entries; got:\n%s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"cacheable": false`) {
		t.Errorf("expected cacheable: false on group_by_subqueries entries; got:\n%s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"attached_condition":`) {
		t.Errorf("expected attached_condition on inner tables; got:\n%s", jsonStr)
	}
	// Ordering: grouping_operation > table > group_by_subqueries.
	idxGroup := strings.Index(jsonStr, `"grouping_operation"`)
	idxTable := strings.Index(jsonStr, `"table":`)
	idxSubs := strings.Index(jsonStr, `"group_by_subqueries"`)
	if idxGroup < 0 || idxTable < 0 || idxSubs < 0 {
		t.Fatalf("expected grouping_operation, table, group_by_subqueries; got:\n%s", jsonStr)
	}
	if !(idxGroup < idxTable && idxTable < idxSubs) {
		t.Errorf("expected order grouping_operation, table, group_by_subqueries; got positions %d, %d, %d",
			idxGroup, idxTable, idxSubs)
	}
}
