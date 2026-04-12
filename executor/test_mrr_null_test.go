package executor

import (
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

func TestMRRNullQuery(t *testing.T) {
	cat := catalog.New()
	cat.CreateDatabase("test")
	store := storage.NewEngine()
	e := New(cat, store)
	e.CurrentDB = "test"

	queries := []string{
		"SET optimizer_switch='derived_merge=off'",
		"CREATE TABLE t1 (i1 INTEGER NOT NULL, c1 VARCHAR(1)) charset utf8mb4",
		"INSERT INTO t1 VALUES (1,'a'), (2, NULL)",
		"CREATE TABLE t2 (c1 VARCHAR(1), i1 INTEGER NOT NULL, KEY (c1)) charset utf8mb4",
		"INSERT INTO t2 VALUES ('a', 1), (NULL, 2)",
	}

	for _, q := range queries {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("Error executing %s: %v", q, err)
		}
	}

	selectQuery := `SELECT * FROM (SELECT * FROM t1) AS d1 WHERE d1.c1 IN (SELECT c1 FROM t2) AND d1.c1 IS NULL ORDER BY d1.i1`
	result, err := e.Execute(selectQuery)
	if err != nil {
		t.Fatalf("SELECT error: %v", err)
	}

	if !result.IsResultSet {
		t.Error("Expected IsResultSet=true")
	}
	if len(result.Columns) != 2 {
		t.Errorf("Expected 2 columns (i1, c1), got %d: %v", len(result.Columns), result.Columns)
	}
	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 rows, got %d", len(result.Rows))
	}
}

func TestMRRBug58463(t *testing.T) {
	cat := catalog.New()
	cat.CreateDatabase("test")
	store := storage.NewEngine()
	e := New(cat, store)
	e.CurrentDB = "test"

	queries := []string{
		`CREATE TABLE t1 (pk INT NOT NULL, PRIMARY KEY (pk)) ENGINE=MyISAM`,
		`INSERT INTO t1 VALUES (2)`,
		`CREATE TABLE t2 (pk INT NOT NULL, i1 INT NOT NULL, i2 INT NOT NULL, c1 VARCHAR(1024) CHARACTER SET utf8, PRIMARY KEY (pk), KEY k1 (i1))`,
		`INSERT INTO t2 VALUES (3, 9, 1, NULL)`,
	}

	for _, q := range queries {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("Error executing %s: %v", q, err)
		}
	}

	explainQuery := `EXPLAIN SELECT i1 FROM t1 LEFT JOIN t2 ON t1.pk = t2.i2 WHERE t2.i1 > 5 AND t2.pk IS NULL ORDER BY i1`
	result, err := e.Execute(explainQuery)
	if err != nil {
		t.Fatalf("EXPLAIN error: %v", err)
	}
	t.Logf("EXPLAIN rows: %d", len(result.Rows))
	for _, row := range result.Rows {
		t.Logf("  EXPLAIN row: %v", row)
	}
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 EXPLAIN row (Impossible WHERE), got %d", len(result.Rows))
	}

	selectQuery := `SELECT i1 FROM t1 LEFT JOIN t2 ON t1.pk = t2.i2 WHERE t2.i1 > 5 AND t2.pk IS NULL ORDER BY i1`
	result2, err := e.Execute(selectQuery)
	if err != nil {
		t.Fatalf("SELECT error: %v", err)
	}
	if !result2.IsResultSet {
		t.Error("Expected IsResultSet=true")
	}
	if len(result2.Columns) != 1 {
		t.Errorf("Expected 1 column (i1), got %d: %v", len(result2.Columns), result2.Columns)
	}
	if len(result2.Rows) != 0 {
		t.Errorf("Expected 0 rows, got %d", len(result2.Rows))
	}
}
