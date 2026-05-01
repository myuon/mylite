package executor

import (
	"strings"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// helper: set up t1..t4 schema for explain_json.inc-style queries (t4 stays empty)
func newOrSubqExec(t *testing.T) *Executor {
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

// TestExplain_OrSubquery_EmptyT4_Text verifies the text EXPLAIN of an OR-of-IN-subquery
// where the inner table (t4) is empty, the empty subquery is shown as
// "no matching row in const table" with NULL table.
func TestExplain_OrSubquery_EmptyT4_Text(t *testing.T) {
	e := newOrSubqExec(t)
	res, err := e.Execute(`EXPLAIN SELECT * FROM t1
  WHERE i IN (SELECT i FROM t2 WHERE t1.i = 10 ORDER BY RAND())
     OR i IN (SELECT i FROM t4 ORDER BY RAND())`)
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	foundEmptyMsg := false
	for _, r := range res.Rows {
		// row[0] = id, row[1] = select_type, row[2] = table, row[11] = Extra
		if id, ok := r[0].(int64); ok && id == 3 {
			if r[2] != nil {
				t.Errorf("select_id=3 expected NULL table, got %v", r[2])
			}
			extra := ""
			if r[11] != nil {
				extra = r[11].(string)
			}
			if !strings.Contains(extra, "no matching row in const table") {
				t.Errorf("select_id=3 expected Extra 'no matching row in const table', got %q", extra)
			}
			foundEmptyMsg = true
		}
	}
	if !foundEmptyMsg {
		t.Fatalf("expected select_id=3 row, got rows: %v", res.Rows)
	}
}

// TestExplain_OrSubquery_EmptyT4_JSON verifies the EXPLAIN FORMAT=JSON output for the
// same query: the empty-table subquery should appear under attached_subqueries as a
// query_block with select_id=3 and "message": "no matching row in const table".
func TestExplain_OrSubquery_EmptyT4_JSON(t *testing.T) {
	e := newOrSubqExec(t)
	res, err := e.Execute(`EXPLAIN FORMAT=JSON SELECT * FROM t1
  WHERE i IN (SELECT i FROM t2 WHERE t1.i = 10 ORDER BY RAND())
     OR i IN (SELECT i FROM t4 ORDER BY RAND())`)
	if err != nil {
		t.Fatalf("EXPLAIN FORMAT=JSON failed: %v", err)
	}
	if len(res.Rows) == 0 || res.Rows[0][0] == nil {
		t.Fatalf("expected JSON result, got %v", res.Rows)
	}
	jsonStr, ok := res.Rows[0][0].(string)
	if !ok {
		t.Fatalf("expected string JSON output, got %T: %v", res.Rows[0][0], res.Rows[0][0])
	}
	if !strings.Contains(jsonStr, `"message": "no matching row in const table"`) {
		t.Errorf("expected JSON to contain message for empty t4 subquery; got:\n%s", jsonStr)
	}
}
