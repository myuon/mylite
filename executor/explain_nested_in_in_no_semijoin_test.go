package executor

import (
	"strings"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// TestExplain_NestedINInNoSemijoin_DistinctIDs verifies that, when both
// semijoin and materialization are disabled, a nested non-correlated IN
// subquery that lives inside another non-correlated IN subquery's WHERE
// keeps a DISTINCT select_id from its parent.  MySQL emits separate
// DEPENDENT SUBQUERY ids for each level of the chain because the
// non-correlated inner subquery only acquires its dependency through the
// outer IN→EXISTS rewrite (it isn't inherently correlated to the outer
// table set).
//
// Repro: explain_json_none "semi-join materialization (if enabled)" block
// (line ~1066 in the result file) executes
//
//   EXPLAIN FORMAT=JSON SELECT * FROM t1
//   WHERE t1.a IN (SELECT t2.a FROM t2 WHERE t2.a > 0) AND
//         t1.a IN (SELECT t3.a FROM t3 WHERE t3.a IN
//                                          (SELECT t4.a FROM t4 WHERE a > 0));
//
// The expected ids in the tabular EXPLAIN are
//
//   1 PRIMARY              t1
//   3 DEPENDENT SUBQUERY   t3
//   4 DEPENDENT SUBQUERY   t4   ← nested inside select#3, NOT merged onto id=3
//   2 DEPENDENT SUBQUERY   t2
//
// The previous implementation collapsed t4 onto id=3 because the
// `mergedDependent` rule fired for any DEPENDENT SUBQUERY-inside-DEPENDENT
// SUBQUERY pair regardless of whether the inner subquery was inherently
// correlated to a real outer table.
func TestExplain_NestedINInNoSemijoin_DistinctIDs(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"
	for _, q := range []string{
		"SET optimizer_switch='semijoin=off,materialization=off,index_condition_pushdown=off,mrr=off'",
		"CREATE TABLE t1 (a INT)",
		"INSERT INTO t1 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1)",
		"CREATE TABLE t2 (a INT)",
		"INSERT INTO t2 SELECT * FROM t1",
		"CREATE TABLE t3 (a INT)",
		"INSERT INTO t3 SELECT * FROM t1",
		"CREATE TABLE t4 (a INT)",
		"INSERT INTO t4 SELECT * FROM t1",
	} {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}

	// Tabular EXPLAIN: ids must be 1, 3, 4, 2 (in mtrrun's reverse-by-group order).
	res, err := e.Execute(`EXPLAIN SELECT * FROM t1 WHERE t1.a IN (SELECT t2.a FROM t2 WHERE t2.a > 0) AND t1.a IN (SELECT t3.a FROM t3 WHERE t3.a IN (SELECT t4.a FROM t4 WHERE a > 0))`)
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	if len(res.Rows) != 4 {
		t.Fatalf("expected 4 EXPLAIN rows, got %d", len(res.Rows))
	}
	wantIDs := []int64{1, 4, 3, 2}
	wantTbls := []string{"t1", "t4", "t3", "t2"}
	for i, want := range wantIDs {
		gotID, _ := res.Rows[i][0].(int64)
		if gotID != want {
			t.Errorf("row %d id = %d, want %d", i, gotID, want)
		}
		gotTbl, _ := res.Rows[i][2].(string)
		if gotTbl != wantTbls[i] {
			t.Errorf("row %d table = %q, want %q", i, gotTbl, wantTbls[i])
		}
	}

	// JSON EXPLAIN: select#4 must be NESTED inside select#3's
	// attached_subqueries (not a sibling of select#3 under t1).
	res, err = e.Execute(`EXPLAIN FORMAT=JSON SELECT * FROM t1 WHERE t1.a IN (SELECT t2.a FROM t2 WHERE t2.a > 0) AND t1.a IN (SELECT t3.a FROM t3 WHERE t3.a IN (SELECT t4.a FROM t4 WHERE a > 0))`)
	if err != nil {
		t.Fatalf("JSON EXPLAIN failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 JSON EXPLAIN row, got %d", len(res.Rows))
	}
	js, _ := res.Rows[0][0].(string)
	// select#3 must precede select#4 in the JSON, and select#4 must lie
	// inside select#3's attached_subqueries block.
	idx3 := strings.Index(js, `"select_id": 3`)
	idx4 := strings.Index(js, `"select_id": 4`)
	if idx3 < 0 {
		t.Fatalf("missing select_id 3 in JSON: %s", js)
	}
	if idx4 < 0 {
		t.Fatalf("missing select_id 4 in JSON: %s", js)
	}
	if idx3 > idx4 {
		t.Errorf("select_id 3 must appear before select_id 4 in JSON: idx3=%d idx4=%d", idx3, idx4)
	}
	// Verify nesting: between select#3 and select#4 there must be an
	// `attached_subqueries` opening (i.e. select#4 is a CHILD of select#3,
	// not a sibling).
	between := js[idx3:idx4]
	if !strings.Contains(between, "attached_subqueries") {
		t.Errorf("select_id 4 must be nested under select_id 3's attached_subqueries; got slice:\n%s", between)
	}
}
