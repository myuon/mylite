package executor

import (
	"strings"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// TestExplain_ExistsBodyTableReorder verifies issue #403: when an EXISTS body
// has multiple FROM tables and one is accessed via a key (ref/eq_ref) driven
// by an equi-join condition, MySQL reorders the body's tables so the
// non-keyed (ALL-scan) driver appears FIRST and the key-access table appears
// LAST in the EXPLAIN nested_loop chain.  mylite previously emitted them in
// textual FROM-clause order regardless of access type.
//
// In the canonical case below, the body is `(t2 INNER JOIN t3 ON
// t3.c1 = t2.c1)`. Textual order would put t2 first, but t2 has KEY (c1, i1)
// allowing ref access driven by t3.c1, so MySQL emits t3 (ALL driver) first
// and t2 (ref probe with first_match) last.
func TestExplain_ExistsBodyTableReorder(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)

	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"

	for _, q := range []string{
		"SET optimizer_switch='semijoin=on,materialization=on,firstmatch=on,loosescan=on,index_condition_pushdown=on,mrr=on'",
		"CREATE TABLE t1 (i1 INTEGER NOT NULL, c1 VARCHAR(1) NOT NULL) charset latin1 ENGINE=InnoDB",
		"INSERT INTO t1 VALUES (2,'w')",
		"CREATE TABLE t2 (i1 INTEGER NOT NULL, c1 VARCHAR(1) NOT NULL, c2 VARCHAR(1) NOT NULL, KEY (c1, i1)) charset latin1 ENGINE=InnoDB",
		"INSERT INTO t2 VALUES (8,'d','d')",
		"INSERT INTO t2 VALUES (4,'v','v')",
		"CREATE TABLE t3 (c1 VARCHAR(1) NOT NULL) charset latin1 ENGINE=InnoDB",
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

	// The EXISTS body's tables should appear in order t3 (driver), then t2 (ref).
	t1Idx := strings.Index(js, `"table_name": "t1"`)
	t3Idx := strings.Index(js, `"table_name": "t3",`+"\n          \"access_type\": \"ALL\"")
	t2Idx := strings.LastIndex(js, `"table_name": "t2"`)
	if t1Idx == -1 {
		t.Fatalf("expected t1 table block, got:\n%s", js)
	}
	if t3Idx == -1 {
		// Fall back to a relaxed match (t3 with ALL access_type somewhere after t1).
		idx := strings.Index(js[t1Idx:], `"table_name": "t3"`)
		if idx == -1 {
			t.Fatalf("expected t3 table block in EXISTS body, got:\n%s", js)
		}
		t3Idx = t1Idx + idx
	}
	if t2Idx == -1 {
		t.Fatalf("expected t2 table block, got:\n%s", js)
	}
	if !(t1Idx < t3Idx && t3Idx < t2Idx) {
		t.Errorf("EXISTS body order wrong: expected t1 (%d) < t3 (%d) < t2 (%d)", t1Idx, t3Idx, t2Idx)
	}

	// The driver (t3) must NOT carry `using_join_buffer` (BNL) or
	// `attached_condition` — both are absent in MySQL's expected output.
	t3Block := js[t3Idx:]
	endIdx := strings.Index(t3Block[len(`"table_name": "t3"`):], `"table_name":`)
	if endIdx > 0 {
		t3Block = t3Block[:len(`"table_name": "t3"`)+endIdx]
	}
	if strings.Contains(t3Block, `"using_join_buffer"`) {
		t.Errorf("EXISTS body driver t3 should not have using_join_buffer, got:\n%s", t3Block)
	}
	if strings.Contains(t3Block, `"attached_condition"`) {
		t.Errorf("EXISTS body driver t3 should not have attached_condition, got:\n%s", t3Block)
	}

	// The probe (t2) must carry `first_match: t1` (since it's the LAST inner
	// of the EXISTS body chain) and `access_type: ref`.
	t2Block := js[t2Idx:]
	if !strings.Contains(t2Block, `"access_type": "ref"`) {
		t.Errorf("expected t2 access_type=ref (issue #403/#406), got:\n%s", t2Block)
	}
	if !strings.Contains(t2Block, `"first_match": "t1"`) {
		t.Errorf("expected first_match=t1 on t2 (last inner of EXISTS body), got:\n%s", t2Block)
	}
}
