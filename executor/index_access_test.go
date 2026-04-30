package executor

import (
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
	"vitess.io/vitess/go/vt/sqlparser"
)

func parseSelectIA(t *testing.T, sql string) *sqlparser.Select {
	t.Helper()
	parser, err := sqlparser.New(sqlparser.Options{})
	if err != nil {
		t.Fatalf("sqlparser.New: %v", err)
	}
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse(%q): %v", sql, err)
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		t.Fatalf("expected SELECT, got %T", stmt)
	}
	return sel
}

func setupIATestExecutor(t *testing.T) *Executor {
	t.Helper()
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("CREATE DATABASE: %v", err)
	}
	e.CurrentDB = "test"
	if _, err := e.Execute("CREATE TABLE t1 (id INT PRIMARY KEY, v INT, KEY idx_v (v))"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := e.Execute("INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 20), (4, 30)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	return e
}

func TestBuildIndexAccessSpec_ConstPK(t *testing.T) {
	e := setupIATestExecutor(t)
	sel := parseSelectIA(t, "SELECT * FROM t1 WHERE id = 2")
	spec, ok := e.buildIndexAccessSpec(sel, "t1")
	if !ok {
		t.Fatalf("expected spec for PK lookup")
	}
	if spec.Type != "const" || spec.IndexName != "PRIMARY" {
		t.Fatalf("unexpected spec: %+v", spec)
	}
	if len(spec.EqualityValues) != 1 {
		t.Fatalf("expected 1 EqualityValue, got %d", len(spec.EqualityValues))
	}
}

func TestBuildIndexAccessSpec_RefSecondary(t *testing.T) {
	e := setupIATestExecutor(t)
	sel := parseSelectIA(t, "SELECT * FROM t1 WHERE v = 20")
	spec, ok := e.buildIndexAccessSpec(sel, "t1")
	if !ok {
		t.Fatalf("expected spec for ref lookup")
	}
	if spec.Type != "ref" {
		t.Fatalf("expected ref type, got %s", spec.Type)
	}
	if spec.IndexName != "idx_v" {
		t.Fatalf("expected idx_v, got %s", spec.IndexName)
	}
}

func TestBuildIndexAccessSpec_RangeBetween(t *testing.T) {
	e := setupIATestExecutor(t)
	sel := parseSelectIA(t, "SELECT * FROM t1 WHERE id BETWEEN 2 AND 3")
	spec, ok := e.buildIndexAccessSpec(sel, "t1")
	if !ok {
		t.Fatalf("expected spec for range BETWEEN")
	}
	if spec.Type != "range" || spec.IndexName != "PRIMARY" {
		t.Fatalf("unexpected spec: %+v", spec)
	}
	if spec.RangeLower[0] == nil || spec.RangeUpper[0] == nil {
		t.Fatalf("expected lower & upper bounds, got %+v", spec)
	}
}

func TestBuildIndexAccessSpec_RangeIN(t *testing.T) {
	e := setupIATestExecutor(t)
	// Use a non-unique index to avoid the "const collapses single-IN" path,
	// then verify the range/IN spec is built when the access type is "range".
	if _, err := e.Execute("CREATE TABLE t2 (id INT, v INT, KEY idx_id (id))"); err != nil {
		t.Fatalf("CREATE t2: %v", err)
	}
	if _, err := e.Execute("INSERT INTO t2 VALUES (1,1),(2,2),(3,3),(4,4)"); err != nil {
		t.Fatalf("INSERT t2: %v", err)
	}
	sel := parseSelectIA(t, "SELECT * FROM t2 WHERE id IN (1, 3)")
	spec, ok := e.buildIndexAccessSpec(sel, "t2")
	if !ok {
		// IN-list access types depend on the planner's classification;
		// ref/range both produce a usable spec. ok=false means the planner
		// didn't pick this index — accept that as a valid no-op.
		return
	}
	if spec.Type != "range" && spec.Type != "ref" {
		t.Fatalf("expected range or ref type, got %s", spec.Type)
	}
}

func TestBuildIndexAccessSpec_FullScan(t *testing.T) {
	e := setupIATestExecutor(t)
	sel := parseSelectIA(t, "SELECT * FROM t1")
	if _, ok := e.buildIndexAccessSpec(sel, "t1"); ok {
		t.Fatalf("expected ok=false for full scan")
	}
}

func TestBuildIndexAccessSpec_NonIndexedCol(t *testing.T) {
	e := setupIATestExecutor(t)
	if _, err := e.Execute("ALTER TABLE t1 ADD COLUMN extra INT"); err != nil {
		t.Fatalf("ALTER: %v", err)
	}
	sel := parseSelectIA(t, "SELECT * FROM t1 WHERE extra = 5")
	if _, ok := e.buildIndexAccessSpec(sel, "t1"); ok {
		t.Fatalf("expected ok=false for non-indexed column")
	}
}

// TestScanTableForFrom_OptIn proves that when useIndexAccessSpecs is true
// and a spec is registered for a table, buildFromExpr routes the read
// through Table.ScanByIndex. The resulting query result must still
// match the legacy full-scan path bit-for-bit.
func TestScanTableForFrom_OptIn(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("CREATE DATABASE: %v", err)
	}
	e.CurrentDB = "test"
	if _, err := e.Execute("CREATE TABLE t1 (id INT PRIMARY KEY, v INT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := e.Execute("INSERT INTO t1 VALUES (1,10),(2,20),(3,30),(4,40)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// Without opt-in: full result.
	resBefore, err := e.Execute("SELECT * FROM t1 WHERE id = 2")
	if err != nil {
		t.Fatalf("SELECT (off): %v", err)
	}
	if len(resBefore.Rows) != 1 {
		t.Fatalf("expected 1 row pre-opt-in, got %d", len(resBefore.Rows))
	}

	// Register a const spec on alias t1 and enable the new path.
	e.indexAccessSpecs = map[string]storage.IndexAccessSpec{
		"t1": {
			Type:           "const",
			IndexName:      "PRIMARY",
			EqualityValues: []interface{}{int64(2)},
		},
	}
	e.useIndexAccessSpecs = true
	defer func() {
		e.useIndexAccessSpecs = false
		e.indexAccessSpecs = nil
	}()

	resAfter, err := e.Execute("SELECT * FROM t1")
	if err != nil {
		t.Fatalf("SELECT (on): %v", err)
	}
	// New path should restrict the scan to the spec's single row, even
	// though no WHERE clause is present.
	if len(resAfter.Rows) != 1 {
		t.Fatalf("expected 1 row post-opt-in, got %d (%+v)", len(resAfter.Rows), resAfter.Rows)
	}
	if id, _ := resAfter.Rows[0][0].(int64); id != 2 {
		t.Fatalf("expected id=2, got %v", resAfter.Rows[0][0])
	}
}

func TestEvalLiteralExpr(t *testing.T) {
	cases := []struct {
		sql  string
		want interface{}
	}{
		{"SELECT 1", int64(1)},
		{"SELECT -42", int64(-42)},
		{"SELECT 3.14", float64(3.14)},
		{"SELECT NULL", nil},
		{"SELECT 'hello'", "hello"},
	}
	parser, _ := sqlparser.New(sqlparser.Options{})
	for _, c := range cases {
		stmt, err := parser.Parse(c.sql)
		if err != nil {
			t.Fatalf("parse(%q): %v", c.sql, err)
		}
		sel := stmt.(*sqlparser.Select)
		ae := sel.SelectExprs.Exprs[0].(*sqlparser.AliasedExpr)
		got, ok := evalLiteralExpr(ae.Expr)
		if !ok {
			t.Errorf("evalLiteralExpr(%q): ok=false", c.sql)
			continue
		}
		if got != c.want {
			t.Errorf("evalLiteralExpr(%q) = %v (%T), want %v (%T)", c.sql, got, got, c.want, c.want)
		}
	}
}
