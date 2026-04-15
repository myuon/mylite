package executor

import (
	"fmt"
	"testing"
	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
	"vitess.io/vitess/go/vt/sqlparser"
	"strings"
)

func TestValidateSetPasswordSyntax(t *testing.T) {
	tests := []struct {
		query   string
		wantErr bool
	}{
		{"SET PASSWORD FOR @'localhost' = 'SoSecret'", true},
		{"SET PASSWORD FOR test_user1@'localhost' = 'SoSecret'", false},
		{"SET PASSWORD FOR test_user1@'localhost' = NULL", true},
		{"SET PASSWORD FOR test_user2@'localhost' = 'SoSecret'", false},
	}
	
	for _, tt := range tests {
		err := validateSetPasswordSyntax(tt.query)
		gotErr := err != nil
		if gotErr != tt.wantErr {
			t.Errorf("validateSetPasswordSyntax(%q): gotErr=%v, wantErr=%v, err=%v", tt.query, gotErr, tt.wantErr, err)
		}
	}
}

func TestPrepareInnerQuery(t *testing.T) {
	// Simulate what execPrepare does
	p := sqlparser.NewTestParser()
	
	queries := []struct {
		outer   string
		wantErr bool
	}{
		{`PREPARE stmt FROM "SET PASSWORD FOR @'localhost' = 'SoSecret'"`, true},
		{`PREPARE stmt FROM "SET PASSWORD FOR test_user1@'localhost' = 'SoSecret'"`, false},
		{`PREPARE stmt FROM "SET PASSWORD FOR test_user1@'localhost' = NULL"`, true},
	}
	
	for _, tt := range queries {
		stmt, err := p.Parse(tt.outer)
		if err != nil {
			t.Fatalf("parse error for %q: %v", tt.outer, err)
		}
		
		prepStmt, ok := stmt.(*sqlparser.PrepareStmt)
		if !ok {
			t.Fatalf("not PrepareStmt for %q: %T", tt.outer, stmt)
		}
		
		query := sqlparser.String(prepStmt.Statement)
		if len(query) >= 2 {
			if (query[0] == '\'' && query[len(query)-1] == '\'') || (query[0] == '"' && query[len(query)-1] == '"') {
				query = query[1 : len(query)-1]
			}
		}
		query = strings.ReplaceAll(query, "\\n", "\n")
		query = strings.ReplaceAll(query, "\\t", "\t")
		query = strings.ReplaceAll(query, "\\'", "'")
		query = strings.ReplaceAll(query, "\\\"", "\"")
		query = strings.ReplaceAll(query, "\\\\", "\\")
		
		upperQuery := strings.ToUpper(strings.TrimSpace(query))
		
		fmt.Printf("outer: %q\n", tt.outer)
		fmt.Printf("inner query: %q\n", query)
		fmt.Printf("upper: %q\n", upperQuery)
		fmt.Printf("has prefix: %v\n", strings.HasPrefix(upperQuery, "SET PASSWORD FOR "))
		
		syntaxErr := validateSetPasswordSyntax(strings.TrimSpace(query))
		fmt.Printf("syntaxErr: %v\n\n", syntaxErr)
		
		gotErr := syntaxErr != nil
		if gotErr != tt.wantErr {
			t.Errorf("for %q: gotErr=%v, wantErr=%v", tt.outer, gotErr, tt.wantErr)
		}
	}
}

func TestSetPasswordPrepare(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	e.CurrentDB = "mysql"
	
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{"valid prepare", `PREPARE stmt FROM "SET PASSWORD FOR test_user1@'localhost' = 'SoSecret'"`, false},
		{"invalid prepare @var", `PREPARE stmt FROM "SET PASSWORD FOR @'localhost' = 'SoSecret'"`, true},
		{"invalid prepare NULL", `PREPARE stmt FROM "SET PASSWORD FOR test_user1@'localhost' = NULL"`, true},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := e.Execute(tt.query)
			if tt.wantErr && err == nil {
				t.Errorf("expected error but got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if err != nil {
				t.Logf("got error: %v", err)
			}
		})
	}
}
