package executor

import (
	"strings"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// TestExplain_NestedINInMaterializedSubqueryReorder: when a materialized
// IN-subquery body itself contains a nested `outer.col IN (SELECT inner.col
// FROM inner_t WHERE ...)`, MySQL flattens the inner IN via semijoin and
// emits the inner_t row first inside the materialized subquery's
// `nested_loop`, with the outer_t row second carrying
// `using_join_buffer: Block Nested Loop` and
// `attached_condition: (outer_t.col = inner_t.col)`.
//
// Reproduces the `<subquery3>` body in the "semi-join materialization (if
// enabled)" query of `other/explain_json.inc` (explain_json_all line 1096+).
//
// Without this handling, the rows appear in tabular order (outer_t first,
// inner_t second) which does not match MySQL's JSON shape.
func TestExplain_NestedINInMaterializedSubqueryReorder(t *testing.T) {
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
	js, _ := res.Rows[0][0].(string)

	// Inside <subquery3>'s nested_loop, t4 must appear before t3.
	sub3 := strings.Index(js, `"<subquery3>"`)
	if sub3 < 0 {
		t.Fatalf("expected <subquery3> placeholder; got:\n%s", js)
	}
	// Restrict search to the <subquery3> body (up to the next placeholder).
	sub2 := strings.Index(js[sub3+1:], `"<subquery2>"`)
	body := js[sub3:]
	if sub2 > 0 {
		body = js[sub3 : sub3+1+sub2]
	}
	idxT4 := strings.Index(body, `"table_name": "t4"`)
	idxT3 := strings.Index(body, `"table_name": "t3"`)
	if idxT4 < 0 || idxT3 < 0 || idxT4 > idxT3 {
		t.Errorf("expected t4 before t3 inside <subquery3> nested_loop; idxT4=%d idxT3=%d body:\n%s", idxT4, idxT3, body)
	}
	// The second table (t3) must carry using_join_buffer: Block Nested Loop
	// and attached_condition: (test.t3.a = test.t4.a).
	if !strings.Contains(body, `"using_join_buffer": "Block Nested Loop"`) {
		t.Errorf("expected `using_join_buffer: Block Nested Loop` in <subquery3> body; got:\n%s", body)
	}
	if !strings.Contains(body, "(`test`.`t3`.`a` = `test`.`t4`.`a`)") {
		t.Errorf("expected `(test.t3.a = test.t4.a)` attached_condition in <subquery3> body; got:\n%s", body)
	}
	// The driver row (t4) must keep the inner-WHERE filter as its
	// attached_condition, not the surrounding outer IN expression.
	if !strings.Contains(body, "(`test`.`t4`.`a` > 0)") {
		t.Errorf("expected `(test.t4.a > 0)` attached_condition for t4 driver in <subquery3>; got:\n%s", body)
	}
}
