package executor

import (
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

func TestCorrelatedSubqueryInUpdate(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)

	queries := []string{
		"CREATE DATABASE IF NOT EXISTS test",
	}
	for _, q := range queries {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}
	e.CurrentDB = "test"

	setup := []string{
		"CREATE TABLE t1 (a INT NOT NULL, b INT, PRIMARY KEY (a))",
		"CREATE TABLE t2 (a INT NOT NULL, b INT, PRIMARY KEY (a))",
		"INSERT INTO t1 VALUES (0, 10),(1, 11),(2, 12)",
		"INSERT INTO t2 VALUES (1, 21),(2, 22),(3, 23)",
	}
	for _, q := range setup {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}

	// Correlated subquery: for each t1 row, finds the matching t2 row by t1.a = t2.a
	// Should succeed (returns exactly 1 or 0 rows per outer row)
	_, err := e.Execute("UPDATE t1 SET b = (SELECT b FROM t2 WHERE t1.a = t2.a)")
	if err != nil {
		t.Fatalf("expected correlated subquery UPDATE to succeed, got: %v", err)
	}

	result, err := e.Execute("SELECT a, b FROM t1 ORDER BY a")
	if err != nil {
		t.Fatalf("select: %v", err)
	}

	// Expected: row0: a=0, b=NULL (no match in t2 for a=0)
	//           row1: a=1, b=21 (matched t2 row with a=1, b=21)
	//           row2: a=2, b=22 (matched t2 row with a=2, b=22)
	expected := [][]interface{}{
		{int64(0), nil},
		{int64(1), int64(21)},
		{int64(2), int64(22)},
	}
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d: %v", len(result.Rows), result.Rows)
	}
	for i, row := range result.Rows {
		a := row[0]
		b := row[1]
		if a != expected[i][0] || b != expected[i][1] {
			t.Errorf("row %d: expected a=%v b=%v, got a=%v b=%v", i, expected[i][0], expected[i][1], a, b)
		}
	}
}

func TestCorrelatedSubqueryInDelete(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)

	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("setup: %v", err)
	}
	e.CurrentDB = "test"

	setup := []string{
		"CREATE TABLE t1 (a INT NOT NULL, b INT, PRIMARY KEY (a))",
		"CREATE TABLE t2 (a INT NOT NULL, b INT, PRIMARY KEY (a))",
		"INSERT INTO t1 VALUES (0, 10),(1, 11),(2, 12)",
		"INSERT INTO t2 VALUES (1, 21),(2, 12),(3, 23)",
	}
	for _, q := range setup {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}

	// Delete rows where t1.b matches the corresponding t2.b for the same a
	_, err := e.Execute("DELETE FROM t1 WHERE b = (SELECT b FROM t2 WHERE t1.a = t2.a)")
	if err != nil {
		t.Fatalf("expected correlated subquery DELETE to succeed, got: %v", err)
	}

	result, err := e.Execute("SELECT a, b FROM t1 ORDER BY a")
	if err != nil {
		t.Fatalf("select: %v", err)
	}

	// Row with a=2,b=12 should be deleted (matches t2 row a=2,b=12)
	// Row with a=0 has no match in t2, so stays; a=1,b=11 != t2.b=21, stays
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows after delete, got %d: %v", len(result.Rows), result.Rows)
	}
}
