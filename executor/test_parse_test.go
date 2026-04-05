package executor

import (
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

func TestShowIndexCardinality(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"

	_, err := e.Execute(`CREATE TABLE t1 (a INT, KEY (a))`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	result, err := e.Execute("SHOW INDEX FROM t1")
	if err != nil {
		t.Fatalf("show index: %v", err)
	}
	if len(result.Rows) > 0 {
		// Column 6 is Cardinality (0-indexed)
		t.Logf("Columns: %v", result.Columns)
		t.Logf("Row: %v (types)", result.Rows[0])
		for i, v := range result.Rows[0] {
			t.Logf("  col[%d] %s = %v (%T)", i, result.Columns[i], v, v)
		}
	}
}
