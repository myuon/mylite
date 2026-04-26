package executor

import (
	"testing"
	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

func TestEmptyStringAlias(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"
	
	// Test SELECT @var AS ""
	e.Execute("SET @utf8_message = 'Testcase: 3.5.1.1:'")
	res, err := e.Execute(`SELECT @utf8_message AS ""`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	t.Logf("Columns for AS \"\": %v", res.Columns)
	if len(res.Columns) == 0 {
		t.Fatal("no columns")
	}
	// The column header should be empty string, not the expression text
	if res.Columns[0] != "" {
		t.Errorf("expected empty column name for AS \"\", got %q", res.Columns[0])
	}
	// Test SELECT @var AS ''
	res2, err2 := e.Execute(`SELECT @utf8_message AS ''`)
	if err2 != nil {
		t.Fatalf("select2: %v", err2)
	}
	t.Logf("Columns for AS '': %v", res2.Columns)
	if len(res2.Columns) == 0 {
		t.Fatal("no columns2")
	}
	if res2.Columns[0] != "" {
		t.Errorf("expected empty column name for AS '', got %q", res2.Columns[0])
	}
}
