package executor

import (
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

func TestSQLSelectLimit(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)

	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("setup: %v", err)
	}
	e.CurrentDB = "test"

	setup := []string{
		"CREATE TABLE t1 (a int auto_increment primary key, b char(20))",
		"INSERT INTO t1 VALUES (1, 'test'), (2, 'test2')",
		"SET SQL_SELECT_LIMIT=4",
	}
	for _, q := range setup {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}

	t.Logf("sql_select_limit value: %q", e.sessionScopeVars["sql_select_limit"])

	res, err := e.Execute("select 1 from t1,t1 as t2,t1 as t3")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	t.Logf("Rows returned: %d", len(res.Rows))
	if len(res.Rows) != 5 {
		t.Errorf("Expected 5 rows with SQL_SELECT_LIMIT=4 (limit+1), got %d", len(res.Rows))
	}
}
