package executor

import (
	"strings"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// TestExplain_ExistsBodyDecorrelate verifies issue #402: when an EXISTS body
// contains an outer-correlated comparison (e.g. `t2.c2 != t1.c1`) AND a
// non-correlated scalar subquery comparison sharing an inner column
// (e.g. `t2.c2 = (SELECT MIN(t3.c1) FROM t3)`), MySQL pulls a derived
// `(scalar_subq) <op> outer.col` predicate up to the OUTER table's
// `attached_condition` and duplicates the scalar subquery onto the outer
// table's `attached_subqueries` list.
func TestExplain_ExistsBodyDecorrelate(t *testing.T) {
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
	// The outer t1 table block must carry attached_condition AND
	// attached_subqueries (containing the non-correlated MIN scalar subq).
	t1IndexA := strings.Index(js, `"table_name": "t1"`)
	if t1IndexA == -1 {
		t.Fatalf("expected t1 table block, got:\n%s", js)
	}
	// Look for the next table_name after t1 to bound the t1 block.
	rest := js[t1IndexA:]
	t2IndexA := strings.Index(rest[len(`"table_name": "t1"`):], `"table_name":`)
	var t1Block string
	if t2IndexA >= 0 {
		t1Block = rest[:len(`"table_name": "t1"`)+t2IndexA]
	} else {
		t1Block = rest
	}
	if !strings.Contains(t1Block, `"attached_condition"`) {
		t.Errorf("expected attached_condition on outer t1 block (issue #402), got:\n%s", t1Block)
	}
	if !strings.Contains(t1Block, `"attached_subqueries"`) {
		t.Errorf("expected attached_subqueries on outer t1 block (issue #402), got:\n%s", t1Block)
	}
	// The attached_subqueries on t1 must contain the inner MIN subquery
	// (select_id 3, with t3 as the inner table).
	if !strings.Contains(t1Block, `"select_id": 3`) {
		t.Errorf("expected select_id 3 inside t1's attached_subqueries (issue #402), got:\n%s", t1Block)
	}
	// The attached_subqueries entry on t1 must use dependent: false / cacheable: true
	// (non-correlated subquery).
	if !strings.Contains(t1Block, `"dependent": false`) {
		t.Errorf("expected dependent: false on t1's attached_subquery entry, got:\n%s", t1Block)
	}
	if !strings.Contains(t1Block, `"cacheable": true`) {
		t.Errorf("expected cacheable: true on t1's attached_subquery entry, got:\n%s", t1Block)
	}
}
