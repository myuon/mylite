package executor

import (
	"strings"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// TestExplain_OuterTableMaterializedINNotNull_JSON: when MySQL materializes an
// IN-subquery into `<subqueryN>` and probes via eq_ref on `<auto_key>`, the
// outer table block carries `attached_condition: (col is not null)` because the
// eq_ref probe on the materialized table never matches NULL outer values.
//
// Reproduces the missing-attached_condition gap on the t1 outer table block of
// the "semi-join materialization" query in explain_json.inc:
//
//	EXPLAIN FORMAT=JSON SELECT * FROM t1
//	WHERE t1.a IN (SELECT t2.a FROM t2 WHERE t2.a >  0) AND
//	      t1.a IN (SELECT t3.a FROM t3 WHERE t3.a IN
//	                  (SELECT t4.a FROM t4 WHERE a > 0));
func TestExplain_OuterTableMaterializedINNotNull_JSON(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"
	for _, q := range []string{
		"SET optimizer_switch='semijoin=on,materialization=on,firstmatch=on,loosescan=on,index_condition_pushdown=on,mrr=on'",
		"CREATE TABLE t1 (a INT)",
		"INSERT INTO t1 VALUES (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1), (1)",
		"CREATE TABLE t2 (a INT) SELECT * FROM t1",
		"CREATE TABLE t3 (a INT) SELECT * FROM t1",
		"CREATE TABLE t4 (a INT) SELECT * FROM t1",
	} {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	res, err := e.Execute(`EXPLAIN FORMAT=JSON SELECT * FROM t1 WHERE t1.a IN (SELECT t2.a FROM t2 WHERE t2.a > 0) AND t1.a IN (SELECT t3.a FROM t3 WHERE t3.a IN (SELECT t4.a FROM t4 WHERE a > 0))`)
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 EXPLAIN row, got %d", len(res.Rows))
	}
	js, _ := res.Rows[0][0].(string)
	// At least one <subqueryN> placeholder must be present (semijoin
	// materialization produced a materialized inner table).
	if !strings.Contains(js, `"<subquery`) {
		t.Fatalf("expected at least one <subqueryN> placeholder in JSON:\n%s", js)
	}
	// Locate the outer t1 table block and ensure it carries the not-null
	// attached_condition added for the materialized IN-subquery probe.
	idx := strings.Index(js, `"table_name": "t1"`)
	if idx < 0 {
		t.Fatalf("missing t1 table_name in JSON:\n%s", js)
	}
	// Look ahead through the t1 block (up to the next table_name or end).
	end := strings.Index(js[idx+1:], `"table_name"`)
	if end < 0 {
		end = len(js) - idx
	}
	segment := js[idx : idx+1+end]
	if !strings.Contains(segment, "(`test`.`t1`.`a` is not null)") {
		t.Errorf("expected outer t1 block to carry attached_condition `(test.t1.a is not null)`; segment:\n%s", segment)
	}
}

// TestExplain_NoMaterializedSubquery_NoOuterNotNull: when there is no
// materialized IN-subquery in the query, the outer table must NOT receive the
// `(col is not null)` attached_condition.  This guards against regressing
// simpler queries that only use FirstMatch / non-semijoin plans.
func TestExplain_NoMaterializedSubquery_NoOuterNotNull(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"
	for _, q := range []string{
		"CREATE TABLE t1 (a INT)",
		"INSERT INTO t1 VALUES (1), (2), (3)",
	} {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	res, err := e.Execute(`EXPLAIN FORMAT=JSON SELECT a FROM t1 WHERE a > 0`)
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 EXPLAIN row, got %d", len(res.Rows))
	}
	js, _ := res.Rows[0][0].(string)
	if strings.Contains(js, "(`test`.`t1`.`a` is not null)") {
		t.Errorf("did not expect not-null attached_condition for non-materialized query:\n%s", js)
	}
}
