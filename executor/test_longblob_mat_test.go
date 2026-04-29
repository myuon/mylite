package executor

import (
	"fmt"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// TestLongblobMaterialized tests subquery_sj_mat line ~4135 case:
// where a1 IN (select b1 from t2_16 where b1 > '0')
// Expected: MATERIALIZED t2_16 (1 row)
// Actual (bug): inline FirstMatch SIMPLE rows
func TestLongblobMaterialized(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"

	cmds := []string{
		"SET optimizer_switch='semijoin=on,materialization=on'",
		"SET optimizer_switch='loosescan=off'",
		"SET optimizer_switch='firstmatch=off'",
		"SET optimizer_switch='duplicateweedout=off'",
		"SET optimizer_switch='index_condition_pushdown=off'",
		"SET optimizer_switch='mrr=off'",
		"CREATE TABLE t1_16 (a1 blob(16), a2 blob(16))",
		"CREATE TABLE t2_16 (b1 blob(16), b2 blob(16))",
		"INSERT INTO t1_16 VALUES ('1 - 00xxxxxxxxxxxx', '2 - 00xxxxxxxxxxxx')",
		"INSERT INTO t1_16 VALUES ('1 - 01xxxxxxxxxxxx', '2 - 01xxxxxxxxxxxx')",
		"INSERT INTO t1_16 VALUES ('1 - 02xxxxxxxxxxxx', '2 - 02xxxxxxxxxxxx')",
		"INSERT INTO t2_16 VALUES ('1 - 01xxxxxxxxxxxx', '2 - 01xxxxxxxxxxxx')",
		"INSERT INTO t2_16 VALUES ('1 - 02xxxxxxxxxxxx', '2 - 02xxxxxxxxxxxx')",
		"INSERT INTO t2_16 VALUES ('1 - 03xxxxxxxxxxxx', '2 - 03xxxxxxxxxxxx')",
	}
	for _, cmd := range cmds {
		if _, err := e.Execute(cmd); err != nil {
			t.Fatalf("Setup %q failed: %v", cmd, err)
		}
	}

	fmt.Println("=== explain select left(a1,7), left(a2,7) from t1_16 where a1 in (select b1 from t2_16 where b1 > '0') ===")
	res, err := e.Execute("EXPLAIN SELECT LEFT(a1,7), LEFT(a2,7) FROM t1_16 WHERE a1 IN (SELECT b1 FROM t2_16 WHERE b1 > '0')")
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	for _, row := range res.Rows {
		fmt.Printf("  id=%v, selectType=%v, table=%v, type=%v, extra=%v\n", row[0], row[1], row[2], row[4], row[11])
	}
	// Expected: 3 rows: <subquery2> (ALL), t1_16 (ALL), t2_16 (MATERIALIZED ALL)
	if len(res.Rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(res.Rows))
	}
	mat := false
	for _, row := range res.Rows {
		if fmt.Sprintf("%v", row[1]) == "MATERIALIZED" {
			mat = true
		}
	}
	if !mat {
		t.Errorf("Expected t2_16 to be MATERIALIZED, got rows above")
	}
}
