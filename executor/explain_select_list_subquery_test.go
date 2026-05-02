package executor

import (
	"strings"
	"testing"
)

// TestExplain_SelectListSubquery_Uncacheable verifies that when a SELECT-list
// subquery contains nondeterministic content (e.g. ORDER BY RAND()), MySQL
// EXPLAIN labels the row "UNCACHEABLE SUBQUERY" and emits the subquery as
// `select_list_subqueries` (not `attached_subqueries`) in EXPLAIN
// FORMAT=JSON, with `dependent: false, cacheable: false`.  When the inner
// subquery has ORDER BY + LIMIT, the inner table is wrapped in an
// `ordering_operation` block.  Refs #264.
func TestExplain_SelectListSubquery_Uncacheable(t *testing.T) {
	e := newUnionExec(t)

	// Tabular EXPLAIN: select_type should be UNCACHEABLE SUBQUERY, with
	// "Using temporary; Using filesort" in Extra.
	res, err := e.Execute(`EXPLAIN SELECT (SELECT i + 1 FROM t1 ORDER BY RAND() LIMIT 1), i FROM t1`)
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(res.Rows), res.Rows)
	}
	if st, _ := res.Rows[1][1].(string); st != "UNCACHEABLE SUBQUERY" {
		t.Errorf("subquery select_type: want UNCACHEABLE SUBQUERY, got %q", st)
	}
	if extra, _ := res.Rows[1][11].(string); !strings.Contains(extra, "Using temporary") || !strings.Contains(extra, "Using filesort") {
		t.Errorf("subquery Extra: want 'Using temporary; Using filesort', got %q", extra)
	}

	// EXPLAIN FORMAT=JSON: select_list_subqueries with dependent/cacheable
	// flags and an inner ordering_operation.
	res, err = e.Execute(`EXPLAIN FORMAT=JSON SELECT (SELECT i + 1 FROM t1 ORDER BY RAND() LIMIT 1), i FROM t1`)
	if err != nil {
		t.Fatalf("EXPLAIN FORMAT=JSON failed: %v", err)
	}
	jsonStr, _ := res.Rows[0][0].(string)
	if !strings.Contains(jsonStr, `"select_list_subqueries":`) {
		t.Errorf("expected select_list_subqueries; got:\n%s", jsonStr)
	}
	if strings.Contains(jsonStr, `"attached_subqueries":`) {
		t.Errorf("did not expect attached_subqueries; got:\n%s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"dependent": false`) {
		t.Errorf("expected dependent: false; got:\n%s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"cacheable": false`) {
		t.Errorf("expected cacheable: false; got:\n%s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"ordering_operation":`) {
		t.Errorf("expected ordering_operation in inner subquery; got:\n%s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"using_temporary_table": true`) {
		t.Errorf("expected using_temporary_table: true on ordering_operation; got:\n%s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"using_filesort": true`) {
		t.Errorf("expected using_filesort: true on ordering_operation; got:\n%s", jsonStr)
	}

	// Ordering of keys in the JSON output: select_list_subqueries should
	// appear after the outer `table` block, not nested inside it.
	idxTable := strings.Index(jsonStr, `"table":`)
	idxSelList := strings.Index(jsonStr, `"select_list_subqueries"`)
	if idxTable < 0 || idxSelList < 0 {
		t.Fatalf("expected table and select_list_subqueries; got:\n%s", jsonStr)
	}
	if !(idxTable < idxSelList) {
		t.Errorf("expected table before select_list_subqueries; got positions %d, %d", idxTable, idxSelList)
	}
}
