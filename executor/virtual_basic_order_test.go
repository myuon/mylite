package executor

import (
	"strings"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// Regression for innodb/virtual_basic: SELECT on a covering secondary index over a
// VIRTUAL generated VARCHAR column must return rows in collation order, not byte order.
func TestVirtualBasicSelectPOrder(t *testing.T) {
	t.Helper()
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("CREATE DATABASE: %v", err)
	}
	e.CurrentDB = "test"
	ddl := `CREATE TABLE t (a INT, b INT, c INT GENERATED ALWAYS AS(a+b), h VARCHAR(10), j INT, m INT GENERATED ALWAYS AS(b + j), n VARCHAR(10), p VARCHAR(20) GENERATED ALWAYS AS(CONCAT(n, h)), INDEX idx1(c), INDEX idx2 (m), INDEX idx3(p))`
	if _, err := e.Execute(ddl); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	for _, ins := range []string{
		`INSERT INTO t VALUES(11, 22, DEFAULT, "AAA", 8, DEFAULT, "XXX", DEFAULT)`,
		`INSERT INTO t VALUES(1, 2, DEFAULT, "uuu", 9, DEFAULT, "uu", DEFAULT)`,
		`INSERT INTO t VALUES(3, 4, DEFAULT, "uooo", 1, DEFAULT, "umm", DEFAULT)`,
	} {
		if _, err := e.Execute(ins); err != nil {
			t.Fatalf("INSERT: %v", err)
		}
	}
	r, err := e.Execute("SELECT p FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	var got []string
	for _, row := range r.Rows {
		got = append(got, row[0].(string))
	}
	want := []string{"ummuooo", "uuuuu", "XXXAAA"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("order mismatch: got %v want %v", got, want)
	}
}
