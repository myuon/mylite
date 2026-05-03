package executor

import (
	"strings"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// TestExplain_ExistsBodyNotFlattenedWhenSemijoinOff verifies that when
// optimizer_switch=semijoin=off, an EXISTS subquery whose body itself has
// nested scalar subqueries is NOT flattened into the outer query block.
//
// MySQL keeps such an EXISTS as a separate DEPENDENT SUBQUERY block in
// `attached_subqueries`, with the outer table appearing alone (no nested_loop
// wrapper) under `query_block.table` and no FirstMatch/Not exists annotations
// on the outer side.
//
// Reproduces issue #264 (other/explain_json_none first-diff at line 1214 →
// 1240+ after fix). Without the fix, the EXISTS body's tables (t3, t2)
// were emitted at the outer query id with FirstMatch annotations even with
// semijoin=off.
func TestExplain_ExistsBodyNotFlattenedWhenSemijoinOff(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"
	for _, q := range []string{
		"SET optimizer_switch='semijoin=off,materialization=off,firstmatch=on,loosescan=on,index_condition_pushdown=on,mrr=on'",
		"CREATE TABLE t1 (i1 INTEGER NOT NULL, c1 VARCHAR(1) NOT NULL) ENGINE=InnoDB",
		"INSERT INTO t1 VALUES (2,'w')",
		"CREATE TABLE t2 (i1 INTEGER NOT NULL, c1 VARCHAR(1) NOT NULL, c2 VARCHAR(1) NOT NULL, KEY (c1, i1)) ENGINE=InnoDB",
		"INSERT INTO t2 VALUES (8,'d','d')",
		"INSERT INTO t2 VALUES (4,'v','v')",
		"CREATE TABLE t3 (c1 VARCHAR(1) NOT NULL) ENGINE=InnoDB",
		"INSERT INTO t3 VALUES ('v')",
	} {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	res, err := e.Execute(`EXPLAIN FORMAT=JSON SELECT i1 FROM t1 WHERE EXISTS (SELECT t2.c1 FROM (t2 INNER JOIN t3 ON (t3.c1 = t2.c1)) WHERE t2.c2 != t1.c1 AND t2.c2 = (SELECT MIN(t3.c1) FROM t3))`)
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 EXPLAIN row, got %d", len(res.Rows))
	}
	js, _ := res.Rows[0][0].(string)
	if testing.Verbose() {
		t.Logf("FULL JSON:\n%s", js)
	}

	// The outer (select_id=1) query_block must NOT contain a nested_loop —
	// the EXISTS body should not be inlined into id=1 when semijoin is off.
	// We expect a single `"table": { "table_name": "t1", ... }` directly under
	// the top-level query_block. Look for the first `"select_id": 1` substring
	// and check that the next structural key is `"table"` (not `"nested_loop"`).
	idx := strings.Index(js, `"select_id": 1`)
	if idx < 0 {
		t.Fatalf("expected select_id 1 in output:\n%s", js)
	}
	// Search for the first `"nested_loop"` and the first `"table": {` after select_id=1.
	rest := js[idx:]
	tablePos := strings.Index(rest, `"table": {`)
	nestedPos := strings.Index(rest, `"nested_loop"`)
	if tablePos < 0 {
		t.Fatalf("expected `\"table\": {` after select_id=1, got:\n%s", js)
	}
	if nestedPos >= 0 && nestedPos < tablePos {
		t.Errorf("EXISTS body was flattened into outer nested_loop with semijoin=off; got nested_loop before table:\n%s", js)
	}

	// The outer t1 table should NOT carry a FirstMatch / Not exists annotation
	// (those are semijoin / antijoin markers added when MySQL flattens the EXISTS).
	t1Idx := strings.Index(js, `"table_name": "t1"`)
	if t1Idx < 0 {
		t.Fatalf("expected t1 table block:\n%s", js)
	}
	// Look forward in t1's block for the next table_name boundary.
	after := js[t1Idx:]
	endIdx := strings.Index(after[len(`"table_name": "t1"`):], `"table_name":`)
	t1Block := after
	if endIdx > 0 {
		t1Block = after[:len(`"table_name": "t1"`)+endIdx]
	}
	if strings.Contains(t1Block, `"first_match"`) || strings.Contains(t1Block, `"FirstMatch`) {
		t.Errorf("outer t1 carries FirstMatch with semijoin=off:\n%s", t1Block)
	}
	if strings.Contains(t1Block, `"Not exists"`) {
		t.Errorf("outer t1 carries Not exists with semijoin=off:\n%s", t1Block)
	}

	// The EXISTS body (select#2) must appear as a DEPENDENT SUBQUERY block
	// inside attached_subqueries — i.e. the inner t2 table should NOT be at
	// id=1.
	if !strings.Contains(js, `"attached_subqueries"`) {
		t.Errorf("expected attached_subqueries on outer t1 block:\n%s", js)
	}
	if !strings.Contains(js, `"select_id": 2`) {
		t.Errorf("expected separate select_id 2 for EXISTS body:\n%s", js)
	}
}
