package executor

import (
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// TestDatetimeColumnInBooleanContext exercises MySQL's coercion rule for
// DATETIME values used directly as boolean predicates (e.g. JOIN ... ON dt_col,
// WHERE dt_col). The numeric form of a datetime is its YYYYMMDDhhmmss value,
// so '0000-00-00 00:00:00' is falsy and any real datetime is truthy.
//
// Regression for BUG#38075 / GH-285: a LEFT JOIN ... ON it2.datetime_key
// previously matched every row because the bare column reference was treated
// as truthy regardless of its value, producing a Cartesian product.
func TestDatetimeColumnInBooleanContext(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"
	// Permissive SQL mode so '0000-00-00 00:00:00' inserts.
	e.sqlMode = ""
	if _, err := e.Execute("CREATE TABLE it2 (int_key int NOT NULL, datetime_key datetime NOT NULL)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := e.Execute(`INSERT INTO it2 VALUES
        (5,'2002-04-10 14:25:30'),
        (0,'0000-00-00 00:00:00'),
        (4,'0000-00-00 00:00:00'),
        (3,'2006-02-14 18:06:35')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rs, err := e.Execute("SELECT int_key FROM it2 WHERE datetime_key ORDER BY int_key")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	got := []string{}
	for _, row := range rs.Rows {
		got = append(got, formatVal(row[0]))
	}
	want := []string{"3", "5"}
	if len(got) != len(want) {
		t.Fatalf("expected %v rows, got %v: %v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d: got %q want %q", i, got[i], want[i])
		}
	}
}

func formatVal(v interface{}) string {
	switch x := v.(type) {
	case int64:
		if x < 0 {
			return "-" + formatPositiveInt(uint64(-x))
		}
		return formatPositiveInt(uint64(x))
	case string:
		return x
	}
	return ""
}

func formatPositiveInt(u uint64) string {
	if u == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for u > 0 {
		i--
		b[i] = byte('0' + u%10)
		u /= 10
	}
	return string(b[i:])
}
