package executor

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

func TestBinaryPadding(t *testing.T) {
	cat := catalog.New()
	stor := storage.NewEngine()
	exec := New(cat, stor)

	must := func(sql string) {
		_, err := exec.Execute(sql)
		if err != nil {
			t.Fatalf("Query %q failed: %v", sql, err)
		}
	}
	queryRows := func(sql string) []string {
		res, err := exec.Execute(sql)
		if err != nil {
			t.Fatalf("Query %q failed: %v", sql, err)
		}
		var out []string
		for _, row := range res.Rows {
			parts := make([]string, len(row))
			for i, v := range row {
				parts[i] = fmt.Sprintf("%v", v)
			}
			out = append(out, strings.Join(parts, "\t"))
		}
		return out
	}

	must("CREATE DATABASE IF NOT EXISTS test")
	exec.CurrentDB = "test"

	// Verify padBinaryValue directly
	padded := padBinaryValue(int64(0x61), 3)
	s, ok := padded.(string)
	if !ok {
		t.Fatalf("padBinaryValue returned non-string for int64: %T", padded)
	}
	h := hex.EncodeToString([]byte(s))
	if h != "610000" {
		t.Errorf("padBinaryValue(int64(0x61), 3) = %q, want 610000", h)
	}

	// Test 1: 0x00 padding
	must("CREATE TABLE t1 (s1 binary(3))")
	must("INSERT INTO t1 VALUES (0x61), (0x6120), (0x612020)")

	rows := queryRows("SELECT HEX(s1) FROM t1")
	expected := []string{"610000", "612000", "612020"}
	for i, r := range rows {
		if i >= len(expected) {
			t.Errorf("unexpected row %d: %q", i, r)
			continue
		}
		if r != expected[i] {
			t.Errorf("HEX row %d: got %q, want %q", i, r, expected[i])
		}
	}
	must("DROP TABLE t1")

	// Test 2: length with concat
	must("CREATE TABLE t1 (s1 binary(2), s2 varbinary(2))")
	must("INSERT INTO t1 VALUES (0x4100, 0x4100)")
	rows = queryRows("SELECT LENGTH(CONCAT('*',s1,'*',s2,'*')) FROM t1")
	if len(rows) != 1 || rows[0] != "7" {
		t.Errorf("concat length: got %v, want [7]", rows)
	}
	must("DROP TABLE t1")
}

func TestBinaryNullBytes(t *testing.T) {
	cat := catalog.New()
	stor := storage.NewEngine()
	exec := New(cat, stor)

	must := func(sql string) {
		_, err := exec.Execute(sql)
		if err != nil {
			t.Fatalf("Query %q failed: %v", sql, err)
		}
	}
	query1 := func(sql string) interface{} {
		res, err := exec.Execute(sql)
		if err != nil {
			t.Fatalf("Query %q failed: %v", sql, err)
		}
		if len(res.Rows) == 0 {
			return nil
		}
		return res.Rows[0][0]
	}

	must("CREATE DATABASE IF NOT EXISTS test")
	exec.CurrentDB = "test"
	must("CREATE TABLE t1 (s1 binary(2), s2 varbinary(2))")
	must("INSERT INTO t1 VALUES (0x4100, 0x4100)")

	// Check values are stored correctly
	if v := query1("SELECT HEX(s1) FROM t1"); v != "4100" {
		t.Errorf("HEX(s1): got %v, want 4100", v)
	}
	if v := query1("SELECT HEX(s2) FROM t1"); v != "4100" {
		t.Errorf("HEX(s2): got %v, want 4100", v)
	}
	if v := query1("SELECT LENGTH(s1) FROM t1"); fmt.Sprintf("%v", v) != "2" {
		t.Errorf("LENGTH(s1): got %v, want 2", v)
	}
	if v := query1("SELECT LENGTH(s2) FROM t1"); fmt.Sprintf("%v", v) != "2" {
		t.Errorf("LENGTH(s2): got %v, want 2", v)
	}
	if v := query1("SELECT LENGTH(CONCAT('*',s1,'*',s2,'*')) FROM t1"); fmt.Sprintf("%v", v) != "7" {
		t.Errorf("LENGTH(concat): got %v, want 7", v)
	}
	must("DROP TABLE t1")
}
