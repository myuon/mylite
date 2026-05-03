package executor

import (
	"fmt"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// TestExistsBodyRefPromote covers issue #406: when an EXISTS body's inner
// table can be probed via a key prefix match driven by an equi-join condition
// in the body's JOIN ON clause, MySQL promotes that inner table from ALL → ref
// access.  Previously mylite kept the table at ALL because explainDetectAccessType
// only inspected the inner SELECT's WHERE clause and ignored its JOIN ON
// equi-join conditions.
func TestExistsBodyRefPromote(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)

	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"

	stmts := []string{
		`CREATE TABLE t1 (i1 INTEGER NOT NULL, c1 VARCHAR(1) NOT NULL) charset latin1 ENGINE=InnoDB`,
		`INSERT INTO t1 VALUES (2,'w')`,
		`CREATE TABLE t2 (i1 INTEGER NOT NULL, c1 VARCHAR(1) NOT NULL, c2 VARCHAR(1) NOT NULL, KEY (c1, i1)) charset latin1 ENGINE=InnoDB`,
		`INSERT INTO t2 VALUES (8,'d','d')`,
		`INSERT INTO t2 VALUES (4,'v','v')`,
		`CREATE TABLE t3 (c1 VARCHAR(1) NOT NULL) charset latin1 ENGINE=InnoDB`,
		`INSERT INTO t3 VALUES ('v')`,
	}
	for _, s := range stmts {
		if _, err := e.Execute(s); err != nil {
			t.Fatalf("setup %q: %v", s, err)
		}
	}

	res, err := e.Execute(`EXPLAIN SELECT i1 FROM t1 WHERE EXISTS (SELECT t2.c1 FROM (t2 INNER JOIN t3 ON (t3.c1 = t2.c1)) WHERE t2.c2 != t1.c1 AND t2.c2 = (SELECT MIN(t3.c1) FROM t3))`)
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}

	// Find the t2 row at id=1.  It should be promoted to ref/c1 with the join
	// driven by test.t3.c1.
	found := false
	for _, row := range res.Rows {
		if len(row) < 9 {
			continue
		}
		if fmt.Sprintf("%v", row[2]) != "t2" {
			continue
		}
		found = true
		if got, want := fmt.Sprintf("%v", row[4]), "ref"; got != want {
			t.Errorf("t2 access_type: got %q, want %q", got, want)
		}
		if got, want := fmt.Sprintf("%v", row[5]), "c1"; got != want {
			t.Errorf("t2 possible_keys: got %q, want %q", got, want)
		}
		if got, want := fmt.Sprintf("%v", row[6]), "c1"; got != want {
			t.Errorf("t2 key: got %q, want %q", got, want)
		}
		if got, want := fmt.Sprintf("%v", row[8]), "test.t3.c1"; got != want {
			t.Errorf("t2 ref: got %q, want %q", got, want)
		}
	}
	if !found {
		t.Fatalf("no row for t2 in EXPLAIN result; rows=%v", res.Rows)
	}
}
