package executor

import (
	"strings"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// TestExplain_ExistsBody_InnerProbeAttachedSubqueries verifies that when the
// EXISTS body decorrelation pulls a non-correlated scalar subquery up to the
// outer driver table's attached_subqueries (issue #402), MySQL ALSO duplicates
// the same subquery onto the LAST inner ref-access "probe" table — the one
// carrying the `inner.col = (scalar_subq)` predicate after the IN→EXISTS
// rewrite.  mylite previously only attached it to the outer driver, leaving the
// inner probe with attached_condition but no attached_subqueries (mtr test
// other/explain_json_all line 1303 first-diff, +52 line KPI delta).
func TestExplain_ExistsBody_InnerProbeAttachedSubqueries(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"
	for _, q := range []string{
		"SET optimizer_switch='semijoin=on,materialization=on,firstmatch=on,loosescan=on,index_condition_pushdown=on,mrr=on'",
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
	// The output should have THREE attached_subqueries occurrences:
	// 1) on outer t1 (issue #402, already covered by ExistsBodyDecorrelate test)
	// 2) on the LAST primary table (t2, ref-access with first_match) — this test
	// (Note: only 2 attached_subqueries in the canonical case; the count
	// equals the number of consumers of the same scalar subquery.)
	got := strings.Count(js, `"attached_subqueries"`)
	if got < 2 {
		t.Errorf("expected at least 2 attached_subqueries (outer t1 + inner t2 probe), got %d:\n%s", got, js)
	}
	// Sanity: the last primary table (t2) is ref-access with first_match and
	// must include attached_subqueries.  Locate the t2 block and verify.
	t2Idx := strings.Index(js, `"table_name": "t2"`)
	if t2Idx == -1 {
		t.Fatalf("expected t2 table block in JSON, got:\n%s", js)
	}
	t2Block := js[t2Idx:]
	// Bound t2 block by the closing of its enclosing object (heuristic: until
	// the next `},` followed by `]` for the nested_loop end).  For diagnostic
	// purposes here, just check the overall t2 block contains both
	// attached_condition and attached_subqueries.
	if !strings.Contains(t2Block, `"first_match"`) {
		t.Errorf("expected first_match on t2 (semijoin probe), got:\n%s", t2Block)
	}
	// The first attached_subqueries after the t2 marker is what we care about.
	// (It may belong to the t2 block or a later sibling, but in this query t2
	// is the LAST nested_loop entry so any subsequent attached_subqueries IS
	// the t2 block's.)
	if !strings.Contains(t2Block, `"attached_subqueries"`) {
		t.Errorf("expected attached_subqueries on inner t2 probe block, got:\n%s", t2Block)
	}
}
