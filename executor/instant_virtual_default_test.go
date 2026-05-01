package executor

import (
	"testing"
)

// TestInstantAddVirtualHasDefault verifies that VIRTUAL generated columns
// added via INSTANT ALTER do not appear in INFORMATION_SCHEMA.INNODB_COLUMNS
// with has_default=1. MySQL only marks physically stored INSTANT-added
// columns with has_default=1 because virtual columns are computed on read
// and never carry an InnoDB-level default.
func TestInstantAddVirtualHasDefault(t *testing.T) {
	e := newTestExecutor(t)
	if _, err := e.Execute("CREATE TABLE tinst_vd(a INT NOT NULL AUTO_INCREMENT PRIMARY KEY, b INT) ROW_FORMAT=REDUNDANT"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute("ALTER TABLE tinst_vd ADD COLUMN c INT NOT NULL, ADD COLUMN d INT GENERATED ALWAYS AS ((b * 2)) VIRTUAL"); err != nil {
		t.Fatal(err)
	}

	// Get table_id from innodb_tables.
	r, err := e.Execute("SELECT table_id FROM information_schema.innodb_tables WHERE name like '%tinst_vd%'")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) == 0 {
		t.Fatalf("no innodb_tables row for tinst_vd")
	}
	tableID := r.Rows[0][0]

	// count(*) WHERE has_default=1 should be 1 (only column `c`).
	r, err = e.Execute("SELECT count(*) FROM information_schema.innodb_columns WHERE table_id = ? AND has_default = 1")
	if err != nil {
		// Fallback: literal interpolation if the placeholder route is unsupported here.
		r, err = e.Execute("SELECT count(*) FROM information_schema.innodb_columns WHERE has_default = 1 AND table_id = " + sprintAny(tableID))
		if err != nil {
			t.Fatal(err)
		}
	} else {
		_ = tableID
	}
	if len(r.Rows) == 0 {
		t.Fatalf("no count rows")
	}
	got := r.Rows[0][0]
	if !equalInt(got, 1) {
		t.Fatalf("expected count(*) WHERE has_default=1 = 1 (only `c`), got %v", got)
	}

	// Sanity: the virtual column `d` should be present with has_default=0.
	r, err = e.Execute("SELECT name, has_default FROM information_schema.innodb_columns WHERE name = 'd' AND has_default = 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 0 {
		t.Fatalf("virtual column `d` must not have has_default=1, rows=%v", r.Rows)
	}
}

func sprintAny(v interface{}) string {
	switch x := v.(type) {
	case int64:
		return itoa(x)
	case int:
		return itoa(int64(x))
	}
	return ""
}

func itoa(i int64) string {
	// minimal int64 to string without strconv import noise.
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

func equalInt(v interface{}, want int64) bool {
	switch x := v.(type) {
	case int:
		return int64(x) == want
	case int32:
		return int64(x) == want
	case int64:
		return x == want
	case uint64:
		return int64(x) == want
	}
	return false
}
