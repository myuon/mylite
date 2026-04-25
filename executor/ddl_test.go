package executor

import (
	"strings"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// newDDLTestExecutor creates an Executor with a "test" database for DDL tests.
func newDDLTestExecutor(t *testing.T) *Executor {
	t.Helper()
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"
	return e
}

// TestRenameTable_SelfRename verifies that renaming a table to itself returns Error 1050.
func TestRenameTable_SelfRename(t *testing.T) {
	e := newDDLTestExecutor(t)

	if _, err := e.Execute("CREATE TABLE t1 (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("create t1: %v", err)
	}

	_, err := e.Execute("RENAME TABLE t1 TO t1")
	if err == nil {
		t.Fatal("expected error 1050 for self-rename, got nil")
	}
	if !strings.Contains(err.Error(), "1050") {
		t.Errorf("expected error 1050 (Table already exists), got: %v", err)
	}
}

// TestRenameTable_MultiStepConflict verifies that multi-step renames detect conflicts
// where an intermediate name collides with a later target.
// e.g. RENAME TABLE a TO b, c TO b — the second pair targets "b" which was just created.
func TestRenameTable_MultiStepConflict(t *testing.T) {
	e := newDDLTestExecutor(t)

	if _, err := e.Execute("CREATE TABLE a (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("create a: %v", err)
	}
	if _, err := e.Execute("CREATE TABLE c (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("create c: %v", err)
	}

	// a -> b is valid, but c -> b conflicts because b now exists (from a -> b)
	_, err := e.Execute("RENAME TABLE a TO b, c TO b")
	if err == nil {
		t.Fatal("expected error 1050 for conflicting multi-step rename, got nil")
	}
	if !strings.Contains(err.Error(), "1050") {
		t.Errorf("expected error 1050 (Table already exists), got: %v", err)
	}
}

// TestRenameTable_MultiStepChain verifies that a valid chain rename (a->b, b->c) succeeds.
func TestRenameTable_MultiStepChain(t *testing.T) {
	e := newDDLTestExecutor(t)

	if _, err := e.Execute("CREATE TABLE a (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("create a: %v", err)
	}
	if _, err := e.Execute("CREATE TABLE b (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("create b: %v", err)
	}

	// a -> b_new, b -> c: both valid (a's old name freed, b moved to c)
	if _, err := e.Execute("RENAME TABLE a TO a_new, b TO c"); err != nil {
		t.Fatalf("expected valid chain rename to succeed: %v", err)
	}
}
