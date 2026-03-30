package executor

import (
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

func newProcTestExecutor(t *testing.T) *Executor {
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

func TestProcedureOutParam(t *testing.T) {
	e := newProcTestExecutor(t)

	// Create procedure with OUT parameter
	_, err := e.Execute("CREATE PROCEDURE p_out(OUT x INT) BEGIN SET x = 42; END")
	if err != nil {
		t.Fatalf("CREATE PROCEDURE failed: %v", err)
	}

	// Call with user variable
	_, err = e.Execute("CALL p_out(@val)")
	if err != nil {
		t.Fatalf("CALL failed: %v", err)
	}

	// Check that @val was set
	res, err := e.Execute("SELECT @val")
	if err != nil {
		t.Fatalf("SELECT @val failed: %v", err)
	}
	if len(res.Rows) == 0 || len(res.Rows[0]) == 0 {
		t.Fatal("expected result row")
	}
	val := res.Rows[0][0]
	// Accept both int64(42) and string "42"
	switch v := val.(type) {
	case int64:
		if v != 42 {
			t.Errorf("expected 42, got %d", v)
		}
	case string:
		if v != "42" {
			t.Errorf("expected '42', got %q", v)
		}
	default:
		t.Errorf("expected 42, got %v (type %T)", val, val)
	}
}

func TestProcedureInoutParam(t *testing.T) {
	e := newProcTestExecutor(t)

	// Create procedure with INOUT parameter
	_, err := e.Execute("CREATE PROCEDURE p_inout(INOUT x INT) BEGIN SET x = x + 10; END")
	if err != nil {
		t.Fatalf("CREATE PROCEDURE failed: %v", err)
	}

	// Set initial value
	_, err = e.Execute("SET @v = 5")
	if err != nil {
		t.Fatalf("SET @v failed: %v", err)
	}

	// Call with user variable
	_, err = e.Execute("CALL p_inout(@v)")
	if err != nil {
		t.Fatalf("CALL failed: %v", err)
	}

	// Check that @v was updated
	res, err := e.Execute("SELECT @v")
	if err != nil {
		t.Fatalf("SELECT @v failed: %v", err)
	}
	if len(res.Rows) == 0 || len(res.Rows[0]) == 0 {
		t.Fatal("expected result row")
	}
	val := res.Rows[0][0]
	switch v := val.(type) {
	case int64:
		if v != 15 {
			t.Errorf("expected 15, got %d", v)
		}
	case string:
		if v != "15" {
			t.Errorf("expected '15', got %q", v)
		}
	default:
		t.Errorf("expected 15, got %v (type %T)", val, val)
	}
}
