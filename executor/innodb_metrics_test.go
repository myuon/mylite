package executor

import (
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

func TestInnoDBMonitorEnableAll(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("SET GLOBAL innodb_monitor_enable = all"); err != nil {
		t.Fatalf("enable all: %v", err)
	}
	rs, err := e.Execute("SELECT name FROM information_schema.innodb_metrics WHERE status != 'enabled'")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(rs.Rows) != 0 {
		t.Fatalf("expected 0 disabled rows, got %d first=%v", len(rs.Rows), rs.Rows[0])
	}
}
