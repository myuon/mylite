package executor

import (
	"strings"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// TestExplain_ExistsBody2TablesNestedLoopAttached verifies that when an EXISTS
// subquery body has multiple FROM tables and produces multiple EXPLAIN rows
// sharing the same select_id (e.g. id=2 with t2 + t3), the JSON EXPLAIN
// emits a SINGLE attached_subqueries entry whose query_block contains a
// `nested_loop` array — not multiple separate attached_subqueries entries
// each with a single `table`.
//
// MySQL's expected shape (issue #264, other/explain_json_none line 1240+):
//
//	"attached_subqueries": [
//	  {
//	    "dependent": true, "cacheable": false,
//	    "query_block": {
//	      "select_id": 2,
//	      "nested_loop": [
//	        { "table": { "table_name": "t3", ... } },
//	        { "table": { "table_name": "t2", ... } }
//	      ]
//	    }
//	  }
//	]
//
// Tables are reordered: drivers (ALL access) first, probes (ref/eq_ref) last.
// The driver loses its `using_join_buffer` field (MySQL doesn't show it on
// the outer driver of the EXISTS-body chain).
func TestExplain_ExistsBody2TablesNestedLoopAttached(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"
	for _, q := range []string{
		"SET optimizer_switch='semijoin=off,materialization=off,firstmatch=on,loosescan=on,index_condition_pushdown=off,mrr=off'",
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

	// There should be exactly ONE select_id=2 query_block (not two split
	// across separate attached_subqueries entries).  Since the scalar subquery
	// (id=3) appears nested inside the t2 probe block, we count occurrences of
	// the literal `"select_id": 2` after the outer t1 block.
	t1End := strings.Index(js, `"table_name": "t1"`)
	if t1End < 0 {
		t.Fatalf("expected t1 table block:\n%s", js)
	}
	rest := js[t1End:]
	count := strings.Count(rest, `"select_id": 2`)
	if count != 1 {
		t.Errorf("expected exactly 1 `select_id: 2` query_block, got %d (rows for the EXISTS body should be merged into a single nested_loop):\n%s", count, js)
	}

	// The single select_id=2 query_block should contain a `nested_loop` array
	// whose first table is t3 (driver, ALL access) and second is t2 (probe,
	// ref access).  Check ordering by string position.
	idx2 := strings.Index(js, `"select_id": 2`)
	if idx2 < 0 {
		t.Fatalf("expected select_id 2:\n%s", js)
	}
	body := js[idx2:]
	nlIdx := strings.Index(body, `"nested_loop"`)
	if nlIdx < 0 {
		t.Errorf("expected nested_loop in select_id=2 block:\n%s", js)
	}
	t3Idx := strings.Index(body, `"table_name": "t3"`)
	t2Idx := strings.Index(body, `"table_name": "t2"`)
	if t3Idx < 0 || t2Idx < 0 || t3Idx > t2Idx {
		t.Errorf("expected t3 (driver) before t2 (probe) in nested_loop, got t3@%d t2@%d:\n%s", t3Idx, t2Idx, js)
	}

	// The driver (t3, listed first) should NOT carry `using_join_buffer`
	// — that field is dropped from the outer driver of the EXISTS-body chain.
	if t3Idx >= 0 && t2Idx > t3Idx {
		t3Block := body[t3Idx:t2Idx]
		if strings.Contains(t3Block, `"using_join_buffer"`) {
			t.Errorf("driver (t3) should not carry using_join_buffer:\n%s", t3Block)
		}
	}
}
