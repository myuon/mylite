package executor

import (
	"strings"
	"testing"
	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

func TestCollationMixError(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)

	stmts := []string{
		"CREATE DATABASE testdb",
		"USE testdb",
		"CREATE TABLE t1 (a char(1) character set latin1 collate latin1_general_ci, b char(1) character set latin1 collate latin1_swedish_ci, c char(1) character set latin1 collate latin1_danish_ci)",
		"INSERT INTO t1 VALUES ('A','B','C'),('a','c','c')",
	}
	for _, s := range stmts {
		if _, err := e.Execute(s); err != nil {
			t.Fatalf("Setup error for %q: %v", s, err)
		}
	}

	tests := []struct {
		query       string
		wantErrCode string
	}{
		{"select * from t1 where a in (b)", "1267"},
		{"select * from t1 where a in (b,c)", "1270"},
		{"select * from t1 where 'a' in (a,b,c)", "1271"},
	}
	for _, tt := range tests {
		_, err := e.Execute(tt.query)
		if err == nil {
			t.Errorf("Expected error %s for query %q, got nil", tt.wantErrCode, tt.query)
			continue
		}
		if !strings.Contains(err.Error(), tt.wantErrCode) {
			t.Errorf("Expected error code %s for query %q, got: %v", tt.wantErrCode, tt.query, err)
		}
	}
}
