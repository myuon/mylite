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

func TestInnoDBMonitorICPCounters(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	for _, sql := range []string{
		"CREATE TABLE monitor_test(a char(3), b int, c char(2), primary key (a(1), c(1)), key(b)) engine = innodb",
		"SET GLOBAL innodb_monitor_enable = 'icp%'",
		"INSERT INTO monitor_test VALUES('13', 2, 'aa')",
	} {
		if _, err := e.Execute(sql); err != nil {
			t.Fatalf("%q: %v", sql, err)
		}
	}
	if _, err := e.Execute("SELECT a FROM monitor_test WHERE b < 1 FOR UPDATE"); err != nil {
		t.Fatalf("first select: %v", err)
	}
	assertMetricCount := func(name string, want int64) {
		t.Helper()
		rs, err := e.Execute("SELECT count FROM information_schema.innodb_metrics WHERE name = '" + name + "'")
		if err != nil {
			t.Fatalf("select %s: %v", name, err)
		}
		if len(rs.Rows) != 1 {
			t.Fatalf("metric %s: expected 1 row, got %d", name, len(rs.Rows))
		}
		got, _ := rs.Rows[0][0].(int64)
		if got != want {
			t.Fatalf("metric %s: got %d want %d", name, got, want)
		}
	}
	assertMetricCount("icp_attempts", 1)
	assertMetricCount("icp_out_of_range", 1)
	assertMetricCount("icp_match", 0)

	if _, err := e.Execute("SELECT a FROM monitor_test WHERE b < 3 FOR UPDATE"); err != nil {
		t.Fatalf("second select: %v", err)
	}
	assertMetricCount("icp_attempts", 2)
	assertMetricCount("icp_out_of_range", 1)
	assertMetricCount("icp_match", 1)
}
