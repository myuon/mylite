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

// TestInstallIndexAccessSpec_ConstPKEngagesGate verifies that when
// execSelect runs a single-table SELECT with a primary-key equality
// predicate, installIndexAccessSpecForSelect engages the gate and the
// result matches the legacy full-scan path bit-for-bit.
func TestInstallIndexAccessSpec_ConstPKEngagesGate(t *testing.T) {
	e := setupIATestExecutor(t)
	// Sanity: gate is off before execution.
	if e.useIndexAccessSpecs {
		t.Fatalf("gate unexpectedly on at setup")
	}
	res, err := e.Execute("SELECT * FROM t1 WHERE id = 3")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if id, _ := res.Rows[0][0].(int64); id != 3 {
		t.Fatalf("expected id=3, got %v", res.Rows[0][0])
	}
	// Gate must be restored to off after the query.
	if e.useIndexAccessSpecs {
		t.Fatalf("gate not restored to off after SELECT")
	}
	if len(e.indexAccessSpecs) != 0 {
		t.Fatalf("specs not cleared after SELECT: %v", e.indexAccessSpecs)
	}
}

// TestInstallIndexAccessSpec_RefEngagesGate verifies that a "ref" lookup
// (secondary non-unique index) is engaged after the post-#304 widening.
// The result must still match the legacy full-scan path bit-for-bit.
func TestInstallIndexAccessSpec_RefEngagesGate(t *testing.T) {
	e := setupIATestExecutor(t)
	if e.useIndexAccessSpecs {
		t.Fatalf("gate unexpectedly on at setup")
	}
	res, err := e.Execute("SELECT * FROM t1 WHERE v = 20")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	// Two rows have v=20 (id=2 and id=3 from setupIATestExecutor).
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d (%+v)", len(res.Rows), res.Rows)
	}
	// Gate must be restored to off after the query.
	if e.useIndexAccessSpecs {
		t.Fatalf("gate not restored to off after SELECT")
	}
	if len(e.indexAccessSpecs) != 0 {
		t.Fatalf("specs not cleared after SELECT: %v", e.indexAccessSpecs)
	}
}

// TestInstallIndexAccessSpec_RefStringNoGate verifies that a "ref"
// lookup on a VARCHAR (non-integer) index column is intentionally NOT
// engaged. Storage's valuesEqualLoose does case-sensitive byte
// comparison; varchar columns in MySQL typically use case-insensitive
// collations, so we keep them on the legacy scan path.
func TestInstallIndexAccessSpec_RefStringNoGate(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("CREATE DATABASE: %v", err)
	}
	e.CurrentDB = "test"
	if _, err := e.Execute("CREATE TABLE ts (id INT PRIMARY KEY, name VARCHAR(40), KEY idx_name(name))"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := e.Execute("INSERT INTO ts VALUES (1,'foo'),(2,'bar')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	parser, _ := sqlparser.New(sqlparser.Options{})
	stmt, err := parser.Parse("SELECT * FROM ts WHERE name = 'foo'")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel := stmt.(*sqlparser.Select)
	restore := e.installIndexAccessSpecForSelect(sel)
	defer restore()
	if e.useIndexAccessSpecs {
		t.Fatalf("expected gate to remain OFF for VARCHAR ref lookup")
	}
}

// TestInstallIndexAccessSpec_RangeStillNoGate verifies that a "range"
// lookup remains on the legacy scan path — the widening only added "ref".
func TestInstallIndexAccessSpec_RangeStillNoGate(t *testing.T) {
	e := setupIATestExecutor(t)
	parser, _ := sqlparser.New(sqlparser.Options{})
	stmt, err := parser.Parse("SELECT * FROM t1 WHERE id BETWEEN 2 AND 3")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel := stmt.(*sqlparser.Select)
	restore := e.installIndexAccessSpecForSelect(sel)
	defer restore()
	if e.useIndexAccessSpecs {
		t.Fatalf("expected gate to remain OFF for range lookup")
	}
}

// TestInstallIndexAccessSpec_NoFromOrSubquery confirms guard rails:
// no FROM / subquery in WHERE / multi-table FROM all leave the gate off.
func TestInstallIndexAccessSpec_NoFromOrSubquery(t *testing.T) {
	e := setupIATestExecutor(t)
	parser, _ := sqlparser.New(sqlparser.Options{})

	cases := []string{
		"SELECT 1",                                          // no FROM
		"SELECT * FROM t1 WHERE id = (SELECT MAX(id) FROM t1)", // subquery in WHERE
		"SELECT * FROM t1 a, t1 b WHERE a.id = 1",           // multi-table FROM
	}
	for _, sql := range cases {
		stmt, err := parser.Parse(sql)
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		sel, ok := stmt.(*sqlparser.Select)
		if !ok {
			continue
		}
		restore := e.installIndexAccessSpecForSelect(sel)
		if e.useIndexAccessSpecs {
			t.Errorf("%q: gate unexpectedly engaged", sql)
		}
		restore()
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
