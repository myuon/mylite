package executor

import (
	"strings"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// TestNormalizeCreateTableEngineSelect verifies that normalizeCreateTableEngineSelect
// correctly handles CREATE TABLE ... SELECT queries with charset specifications.
func TestNormalizeCreateTableEngineSelect(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantKept    string // substring that should be present in output
		wantRemoved string // substring that should NOT be present in output
	}{
		{
			name:     "column-level charset should be preserved",
			input:    "CREATE TABLE t1 (a CHAR(10) CHARACTER SET cp1251) SELECT 1 AS a",
			wantKept: "CHARACTER SET cp1251",
		},
		{
			name:        "table-level charset should be stripped (with col defs)",
			input:       "CREATE TABLE t1 (a CHAR(10)) DEFAULT CHARSET=latin1 SELECT 1 AS a",
			wantKept:    "SELECT 1 AS a",
			wantRemoved: "CHARSET=latin1",
		},
		{
			name:        "engine option should be stripped",
			input:       "CREATE TABLE t1 (a INT) ENGINE=InnoDB SELECT 1 AS a",
			wantKept:    "SELECT 1 AS a",
			wantRemoved: "ENGINE=InnoDB",
		},
		{
			name:        "table-level charset without col defs should be stripped",
			input:       "CREATE TABLE t1 charset latin1 SELECT 1 AS a",
			wantKept:    "SELECT 1 AS a",
			wantRemoved: "charset latin1",
		},
		{
			name:        "multiline SELECT with charset should work",
			input:       "create table t1 charset latin1\nselect bin(130)",
			wantKept:    "select bin(130)",
			wantRemoved: "charset latin1",
		},
		{
			name:     "AS SELECT with charset should work",
			input:    "CREATE TABLE t1 charset utf8mb4 AS SELECT format(123,2,'no_NO')",
			wantKept: "SELECT format(123,2,'no_NO')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeCreateTableEngineSelect(tt.input)
			if tt.wantKept != "" && !strings.Contains(result, tt.wantKept) {
				t.Errorf("normalizeCreateTableEngineSelect(%q) = %q\nwant it to contain %q", tt.input, result, tt.wantKept)
			}
			if tt.wantRemoved != "" && strings.Contains(result, tt.wantRemoved) {
				t.Errorf("normalizeCreateTableEngineSelect(%q) = %q\nwant it NOT to contain %q", tt.input, result, tt.wantRemoved)
			}
		})
	}
}

// TestKoi8rCreateTable verifies that CREATE TABLE with column-level CHARACTER SET
// and a SELECT clause creates the table correctly (regression test for bug where
// normalizeCreateTableEngineSelect stripped charset from column definitions).
func TestKoi8rCreateTable(t *testing.T) {
	cat := catalog.New()
	stor := storage.NewEngine()
	e := New(cat, stor)

	cat.CreateDatabaseWithCharset("test", "utf8mb4", "utf8mb4_0900_ai_ci") //nolint:errcheck
	stor.EnsureDatabase("test")
	e.CurrentDB = "test"

	// Non-UTF8 bytes in koi8r literal - verifies that normalizeCreateTableEngineSelect
	// does not strip CHARACTER SET from column definitions.
	query := "CREATE TABLE t1 (a CHAR(10) CHARACTER SET cp1251) SELECT _koi8r'\xd0\xd2\xcf\xc2\xc1' AS a"
	_, err := e.Execute(query)
	if err != nil {
		t.Errorf("CREATE TABLE returned unexpected error: %v", err)
	}

	db, _ := cat.GetDatabase("test")
	if _, err3 := db.GetTable("t1"); err3 != nil {
		t.Errorf("Table t1 was not created: %v", err3)
	}
}

// TestCharsetCreateTableSelect verifies CREATE TABLE ... charset ... SELECT (no col defs)
// creates the table correctly.
func TestCharsetCreateTableSelect(t *testing.T) {
	cat := catalog.New()
	stor := storage.NewEngine()
	e := New(cat, stor)

	cat.CreateDatabaseWithCharset("test", "utf8mb4", "utf8mb4_0900_ai_ci") //nolint:errcheck
	stor.EnsureDatabase("test")
	e.CurrentDB = "test"

	query := "create table t1 charset latin1\nselect 'hello' as a"
	_, err := e.Execute(query)
	if err != nil {
		t.Errorf("CREATE TABLE charset latin1 SELECT returned unexpected error: %v", err)
	}

	db, _ := cat.GetDatabase("test")
	if _, err3 := db.GetTable("t1"); err3 != nil {
		t.Errorf("Table t1 was not created: %v", err3)
	}
}
