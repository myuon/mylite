package executor

import (
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// TestExplainJSONReorderNestedINMat3Rows: when a materialized subquery
// body has 3 rows from a nested `(outer.col, ...) IN (SELECT inner.col,
// ... FROM inner_t LEFT JOIN inner2_t ON ...)`, MySQL emits the inner
// tables before the outer table inside the materialized subquery's
// nested_loop.  The reorder helper must rearrange the tabular order
// (outer, inner1, inner2) to (inner1, inner2, outer).
func TestExplainJSONReorderNestedINMat3Rows(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	e.CurrentDB = "test"
	innerSQL := `SELECT grandparent1.col_int_nokey AS g1
FROM t1 AS grandparent1
WHERE (grandparent1.col_int_nokey, grandparent1.col_int_key) IN
(SELECT parent1.col_int_key AS p1, parent1.col_int_key AS p2
 FROM t1 AS parent1
 LEFT JOIN t2 AS parent2
 ON parent1.col_int_nokey = parent2.col_int_key)
AND grandparent1.col_int_key <> 3`

	// Tabular row layout: id, selectType, table, partitions, accessType,
	// possibleKeys, key, keyLen, ref, rows, filtered, extra
	mkRow := func(name string) []interface{} {
		return []interface{}{
			int64(2), "MATERIALIZED", name, nil, "ALL",
			nil, nil, nil, nil, int64(11), 100.0, "",
		}
	}
	rows := [][]interface{}{
		mkRow("grandparent1"),
		mkRow("parent1"),
		mkRow("parent2"),
	}
	out, info := e.explainJSONReorderNestedINMatRows(rows, innerSQL)
	if len(out) != 3 {
		t.Fatalf("expected 3 rows; got %d", len(out))
	}
	got := []string{
		out[0][2].(string),
		out[1][2].(string),
		out[2][2].(string),
	}
	wantOrder := []string{"parent1", "parent2", "grandparent1"}
	for i := range wantOrder {
		if got[i] != wantOrder[i] {
			t.Errorf("position %d: want %s, got %s (full: %v)", i, wantOrder[i], got[i], got)
		}
	}
	// joinInfo must be nil for the 3-row case (no BNL injection on parent2).
	if info != nil {
		t.Errorf("expected joinInfo nil for 3-row reorder, got %+v", info)
	}
}
