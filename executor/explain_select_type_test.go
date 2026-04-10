package executor

import (
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// newTestExecutor creates a minimal Executor for EXPLAIN testing.
func newTestExecutor(t *testing.T) *Executor {
	t.Helper()
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	// Create a test database and table so EXPLAIN can resolve table names.
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"
	if _, err := e.Execute("CREATE TABLE t1 (id INT PRIMARY KEY, val VARCHAR(100))"); err != nil {
		t.Fatalf("create t1: %v", err)
	}
	if _, err := e.Execute("CREATE TABLE t2 (id INT PRIMARY KEY, t1_id INT)"); err != nil {
		t.Fatalf("create t2: %v", err)
	}
	return e
}

func TestExplainSelectType_Simple(t *testing.T) {
	e := newTestExecutor(t)
	res, err := e.Execute("EXPLAIN SELECT * FROM t1")
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatal("expected at least one row")
	}
	selectType := res.Rows[0][1]
	if selectType != "SIMPLE" {
		t.Errorf("expected select_type=SIMPLE, got %v", selectType)
	}
}

func TestExplainSelectType_SimpleNoTable(t *testing.T) {
	e := newTestExecutor(t)
	res, err := e.Execute("EXPLAIN SELECT 1")
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatal("expected at least one row")
	}
	selectType := res.Rows[0][1]
	if selectType != "SIMPLE" {
		t.Errorf("expected select_type=SIMPLE, got %v", selectType)
	}
}

func TestExplainSelectType_SubqueryInWhere(t *testing.T) {
	e := newTestExecutor(t)
	// Non-correlated IN subquery: with semijoin=on (default), MySQL flattens this
	// into a semi-join, showing all rows as SIMPLE at id=1.
	res, err := e.Execute("EXPLAIN SELECT * FROM t1 WHERE id IN (SELECT t1_id FROM t2)")
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	if len(res.Rows) < 2 {
		t.Fatalf("expected at least 2 rows, got %d", len(res.Rows))
	}
	types := collectSelectTypes(res.Rows)
	// With semijoin=on, all rows should be SIMPLE (semi-join flattening)
	for _, st := range types {
		if st != "SIMPLE" {
			t.Errorf("expected all rows to be SIMPLE (semijoin flattening), got types: %v", types)
			return
		}
	}
}

func TestExplainSelectType_SubqueryInSelect(t *testing.T) {
	e := newTestExecutor(t)
	res, err := e.Execute("EXPLAIN SELECT (SELECT 1 FROM t2 LIMIT 1) FROM t1")
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	if len(res.Rows) < 2 {
		t.Fatalf("expected at least 2 rows, got %d", len(res.Rows))
	}
	if res.Rows[0][1] != "PRIMARY" {
		t.Errorf("expected first row select_type=PRIMARY, got %v", res.Rows[0][1])
	}
	found := false
	for _, row := range res.Rows[1:] {
		if row[1] == "SUBQUERY" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected a SUBQUERY row, got types: %v", collectSelectTypes(res.Rows))
	}
}

func TestExplainSelectType_DerivedTable(t *testing.T) {
	e := newTestExecutor(t)
	res, err := e.Execute("EXPLAIN SELECT * FROM (SELECT id FROM t1) AS dt")
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	if len(res.Rows) < 2 {
		t.Fatalf("expected at least 2 rows, got %d", len(res.Rows))
	}
	types := collectSelectTypes(res.Rows)
	if types[0] != "PRIMARY" {
		t.Errorf("expected first row select_type=PRIMARY, got %v", types[0])
	}
	found := false
	for _, st := range types[1:] {
		if st == "DERIVED" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected a DERIVED row, got types: %v", types)
	}
}

func TestExplainSelectType_Union(t *testing.T) {
	e := newTestExecutor(t)
	res, err := e.Execute("EXPLAIN SELECT id FROM t1 UNION SELECT id FROM t2")
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	types := collectSelectTypes(res.Rows)
	if len(types) < 3 {
		t.Fatalf("expected at least 3 rows (PRIMARY, UNION, UNION RESULT), got %d: %v", len(types), types)
	}
	if types[0] != "PRIMARY" {
		t.Errorf("expected first row select_type=PRIMARY, got %v", types[0])
	}
	foundUnion := false
	foundResult := false
	for _, st := range types[1:] {
		if st == "UNION" {
			foundUnion = true
		}
		if st == "UNION RESULT" {
			foundResult = true
		}
	}
	if !foundUnion {
		t.Errorf("expected a UNION row, got types: %v", types)
	}
	if !foundResult {
		t.Errorf("expected a UNION RESULT row, got types: %v", types)
	}
}

func TestExplainSelectType_UnionAll(t *testing.T) {
	e := newTestExecutor(t)
	res, err := e.Execute("EXPLAIN SELECT id FROM t1 UNION ALL SELECT id FROM t2")
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	types := collectSelectTypes(res.Rows)
	if len(types) < 2 {
		t.Fatalf("expected at least 2 rows, got %d: %v", len(types), types)
	}
	if types[0] != "PRIMARY" {
		t.Errorf("expected first row select_type=PRIMARY, got %v", types[0])
	}
	foundUnion := false
	for _, st := range types[1:] {
		if st == "UNION" {
			foundUnion = true
		}
	}
	if !foundUnion {
		t.Errorf("expected a UNION row, got types: %v", types)
	}
}

func TestExplainSelectType_DependentSubquery(t *testing.T) {
	e := newTestExecutor(t)
	// Add column 'b' to t2 for the correlated reference
	if _, err := e.Execute("ALTER TABLE t2 ADD COLUMN b INT"); err != nil {
		t.Fatalf("alter t2: %v", err)
	}
	// Correlated IN subquery: with semijoin=on (default), MySQL flattens correlated
	// IN subqueries into semi-joins too (the correlation becomes a join condition).
	// All rows appear as SIMPLE at id=1.
	res, err := e.Execute("EXPLAIN SELECT * FROM t1 WHERE id IN (SELECT t1_id FROM t2 WHERE t2.b = t1.id)")
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	if len(res.Rows) < 2 {
		t.Fatalf("expected at least 2 rows, got %d", len(res.Rows))
	}
	types := collectSelectTypes(res.Rows)
	// With semijoin=on, all rows should be SIMPLE (semi-join flattening)
	for _, st := range types {
		if st != "SIMPLE" {
			t.Errorf("expected all rows to be SIMPLE (semijoin flattening), got types: %v", types)
			return
		}
	}
}

func TestExplainSelectType_DependentUnion(t *testing.T) {
	e := newTestExecutor(t)
	// Correlated subquery with UNION: references t1.id from outer query
	res, err := e.Execute("EXPLAIN SELECT * FROM t1 WHERE id IN (SELECT t1_id FROM t2 WHERE t2.t1_id = t1.id UNION SELECT id FROM t2 WHERE t2.t1_id = t1.id)")
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	types := collectSelectTypes(res.Rows)
	if types[0] != "PRIMARY" {
		t.Errorf("expected first row select_type=PRIMARY, got %v", types[0])
	}
	foundDepSub := false
	foundDepUnion := false
	for _, st := range types[1:] {
		if st == "DEPENDENT SUBQUERY" {
			foundDepSub = true
		}
		if st == "DEPENDENT UNION" {
			foundDepUnion = true
		}
	}
	if !foundDepSub {
		t.Errorf("expected a DEPENDENT SUBQUERY row, got types: %v", types)
	}
	if !foundDepUnion {
		t.Errorf("expected a DEPENDENT UNION row, got types: %v", types)
	}
}

func TestExplainSelectType_Materialized(t *testing.T) {
	e := newTestExecutor(t)
	// Non-correlated IN subquery: with semijoin=on (default), MySQL uses semi-join
	// flattening, showing all rows as SIMPLE. MATERIALIZED strategy is shown when
	// semijoin=off or big_tables=ON (forces disk-based execution).
	// Test the semijoin=off case explicitly.
	if _, err := e.Execute("SET optimizer_switch='semijoin=off'"); err != nil {
		t.Fatalf("set optimizer_switch: %v", err)
	}
	res, err := e.Execute("EXPLAIN SELECT * FROM t1 WHERE id IN (SELECT t1_id FROM t2)")
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	types := collectSelectTypes(res.Rows)
	if types[0] != "PRIMARY" {
		t.Errorf("expected first row select_type=PRIMARY (semijoin=off), got %v", types[0])
	}
	found := false
	for _, st := range types[1:] {
		if st == "MATERIALIZED" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected a MATERIALIZED row (semijoin=off), got types: %v", types)
	}
}

func TestExplainSelectType_NonINSubqueryStaysSubquery(t *testing.T) {
	e := newTestExecutor(t)
	// Non-correlated subquery NOT in IN context should stay SUBQUERY
	res, err := e.Execute("EXPLAIN SELECT (SELECT 1 FROM t2 LIMIT 1) FROM t1")
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	types := collectSelectTypes(res.Rows)
	found := false
	for _, st := range types[1:] {
		if st == "SUBQUERY" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected a SUBQUERY row (not MATERIALIZED), got types: %v", types)
	}
}

// collectSelectTypes extracts the select_type column from EXPLAIN rows.
func collectSelectTypes(rows [][]interface{}) []string {
	types := make([]string, len(rows))
	for i, row := range rows {
		if s, ok := row[1].(string); ok {
			types[i] = s
		}
	}
	return types
}
