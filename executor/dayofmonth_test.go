package executor

import (
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

func TestDayofmonthZeroDate(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)

	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"
	// Use the same SQL mode as MTR runner context
	e.sqlMode = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"

	if _, err := e.Execute("CREATE TABLE t1 (d date, dt datetime, t timestamp, c char(10))"); err != nil {
		t.Fatalf("create t1: %v", err)
	}

	// Check what types are stored in catalog
	if db, err := cat.GetDatabase("test"); err == nil {
		if td, ok := db.Tables["t1"]; ok {
			for _, col := range td.Columns {
				t.Logf("column: name=%s type=%s", col.Name, col.Type)
			}
		}
	}

	if _, err := e.Execute(`INSERT IGNORE INTO t1 VALUES ('0000-00-00', '0000-00-00', '0000-00-00', '0000-00-00')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	res, err := e.Execute("SELECT dayofmonth('0000-00-00'), dayofmonth(d), dayofmonth(dt), dayofmonth(t), dayofmonth(c) FROM t1")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatal("expected at least one row")
	}

	row := res.Rows[0]
	t.Logf("dayofmonth row: %v", row)

	// dayofmonth('0000-00-00') → NULL (string literal)
	if row[0] != nil {
		t.Errorf("dayofmonth('0000-00-00'): expected nil, got %v (%T)", row[0], row[0])
	}
	// dayofmonth(d) where d is DATE → 0
	if row[1] == nil {
		t.Errorf("dayofmonth(d): expected 0, got nil")
	} else if v, ok := row[1].(int64); !ok || v != 0 {
		t.Errorf("dayofmonth(d): expected int64(0), got %v (%T)", row[1], row[1])
	}
	// dayofmonth(dt) where dt is DATETIME → 0
	if row[2] == nil {
		t.Errorf("dayofmonth(dt): expected 0, got nil")
	} else if v, ok := row[2].(int64); !ok || v != 0 {
		t.Errorf("dayofmonth(dt): expected int64(0), got %v (%T)", row[2], row[2])
	}
	// dayofmonth(t) where t is TIMESTAMP → 0
	if row[3] == nil {
		t.Errorf("dayofmonth(t): expected 0, got nil")
	} else if v, ok := row[3].(int64); !ok || v != 0 {
		t.Errorf("dayofmonth(t): expected int64(0), got %v (%T)", row[3], row[3])
	}
	// dayofmonth(c) where c is char(10) → NULL
	if row[4] != nil {
		t.Errorf("dayofmonth(c): expected nil, got %v (%T)", row[4], row[4])
	}
}
