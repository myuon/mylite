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

// TestProcHandlerDropRecreate reproduces the storedproc test scenario where:
// 1. h1 has handler for 1318, calls sp1 with wrong args (caught), succeeds
// 2. h1 is dropped and sp1 is dropped
// 3. h1 is recreated with handler for 1305, calls sp1 which doesn't exist (caught)
// 4. CALL h1 must succeed (not "h1 does not exist")
func TestProcHandlerDropRecreate(t *testing.T) {
	e := newProcTestExecutor(t)

	// CREATE sp1 with 2 params
	_, err := e.Execute("CREATE PROCEDURE sp1 (x int, y int) BEGIN set @y=x; END")
	if err != nil {
		t.Fatalf("CREATE sp1 failed: %v", err)
	}

	// CREATE h1 with handler for 1318 (wrong number of args)
	body1 := `CREATE PROCEDURE h1 ()
BEGIN
    declare continue handler for 1318 set @x2 = 1;
    set @x=0;
  CALL sp1 (1);
    set @x=1;
    SELECT @x, @x2;
END`
	_, err = e.Execute(body1)
	if err != nil {
		t.Fatalf("CREATE h1 (1318 handler) failed: %v", err)
	}

	// CALL h1 - sp1(1) triggers error 1318, handler catches it with CONTINUE
	_, err = e.Execute("CALL h1()")
	if err != nil {
		t.Fatalf("CALL h1 (1318 handler) failed: %v", err)
	}

	// cleanup
	_, err = e.Execute("DROP PROCEDURE h1")
	if err != nil {
		t.Fatalf("DROP h1 failed: %v", err)
	}
	_, err = e.Execute("DROP PROCEDURE sp1")
	if err != nil {
		t.Fatalf("DROP sp1 failed: %v", err)
	}

	// Recreate h1 with handler for 1305 (procedure does not exist)
	body2 := `CREATE PROCEDURE h1 ()
BEGIN
    declare continue handler for 1305 set @x2 = 1;
    set @x=0;
  CALL sp1 (1);
    set @x=1;
    SELECT @x, @x2;
END`
	_, err = e.Execute(body2)
	if err != nil {
		t.Fatalf("CREATE h1 (1305 handler) failed: %v", err)
	}

	// CALL h1 - sp1 doesn't exist → error 1305 → handler catches it with CONTINUE
	_, err = e.Execute("CALL h1()")
	if err != nil {
		t.Fatalf("CALL h1 (1305 handler) failed: %v", err)
	}

	// cleanup
	_, err = e.Execute("DROP PROCEDURE h1")
	if err != nil {
		t.Fatalf("DROP h1 (final) failed: %v", err)
	}
}
