package executor

import (
	"fmt"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// TestTimestampFractionalComparison exercises Bug#50774: comparing a plain
// TIMESTAMP column against a string literal with .0 fractional seconds.
func TestTimestampFractionalComparison(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"

	if _, err := e.Execute("CREATE TABLE t1 ( a TIMESTAMP, KEY ( a ) )"); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, ts := range []string{"'2010-02-01 09:31:01'", "'2010-02-01 09:31:02'", "'2010-02-01 09:31:03'", "'2010-02-01 09:31:04'"} {
		if _, err := e.Execute("INSERT INTO t1 VALUES( " + ts + " )"); err != nil {
			t.Fatalf("insert %s: %v", ts, err)
		}
	}

	tests := []struct {
		query string
		want  []string
	}{
		{
			"SELECT * FROM t1 WHERE a >= '2010-02-01 09:31:02.0'",
			[]string{"2010-02-01 09:31:02", "2010-02-01 09:31:03", "2010-02-01 09:31:04"},
		},
		{
			"SELECT * FROM t1 WHERE '2010-02-01 09:31:02.0' <= a",
			[]string{"2010-02-01 09:31:02", "2010-02-01 09:31:03", "2010-02-01 09:31:04"},
		},
		{
			"SELECT * FROM t1 WHERE a <= '2010-02-01 09:31:02.0'",
			[]string{"2010-02-01 09:31:01", "2010-02-01 09:31:02"},
		},
		{
			"SELECT * FROM t1 WHERE '2010-02-01 09:31:02.0' >= a",
			[]string{"2010-02-01 09:31:01", "2010-02-01 09:31:02"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.query, func(t *testing.T) {
			rs, err := e.Execute(tc.query)
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			got := []string{}
			for _, row := range rs.Rows {
				got = append(got, fmt.Sprintf("%v", row[0]))
			}
			if len(got) != len(tc.want) {
				t.Errorf("got %d rows %v, want %d rows %v", len(got), got, len(tc.want), tc.want)
				return
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Errorf("row %d: got %q want %q", i, got[i], tc.want[i])
				}
			}
		})
	}

	if _, err := e.Execute("DROP TABLE t1"); err != nil {
		t.Fatalf("drop: %v", err)
	}
}
