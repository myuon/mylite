package executor

import (
	"fmt"
	"strings"
	"testing"
)

// TestCreateTableUnknownEngine_NoSubstitution verifies that with NO_ENGINE_SUBSTITUTION
// (the default sql_mode) an unknown engine returns Error 1286 and no table is created.
func TestCreateTableUnknownEngine_NoSubstitution(t *testing.T) {
	e := newTestExecutor(t)

	_, err := e.Execute("CREATE TABLE tx1 (id INT) ENGINE=ROCKETENGINE")
	if err == nil {
		t.Fatal("expected error for unknown engine with NO_ENGINE_SUBSTITUTION, got nil")
	}
	if !strings.Contains(err.Error(), "1286") {
		t.Errorf("expected error 1286, got: %v", err)
	}
}

// TestCreateTableUnknownEngine_WithSubstitution verifies that without NO_ENGINE_SUBSTITUTION
// an unknown engine emits Warning 1286 + 1266 and the table is created using InnoDB.
func TestCreateTableUnknownEngine_WithSubstitution(t *testing.T) {
	e := newTestExecutor(t)

	if _, err := e.Execute("SET sql_mode=''"); err != nil {
		t.Fatalf("SET sql_mode: %v", err)
	}

	if _, err := e.Execute("CREATE TABLE tx1 (id INT) ENGINE=ROCKETENGINE"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Should have warnings 1286 and 1266
	warnResult, err := e.Execute("SHOW WARNINGS")
	if err != nil {
		t.Fatalf("SHOW WARNINGS: %v", err)
	}
	var has1286, has1266 bool
	for _, row := range warnResult.Rows {
		if len(row) >= 2 {
			code := fmt.Sprintf("%v", row[1])
			if code == "1286" {
				has1286 = true
			}
			if code == "1266" {
				has1266 = true
			}
		}
	}
	if !has1286 {
		t.Error("expected Warning 1286 (Unknown storage engine), not found")
	}
	if !has1266 {
		t.Error("expected Warning 1266 (Using storage engine InnoDB), not found")
	}
}

// TestCreateTableUnknownEngine_ShowCreateTableUsesInnoDB verifies Bug 1:
// after engine substitution, SHOW CREATE TABLE reports ENGINE=InnoDB, not the unknown engine.
func TestCreateTableUnknownEngine_ShowCreateTableUsesInnoDB(t *testing.T) {
	e := newTestExecutor(t)

	if _, err := e.Execute("SET sql_mode=''"); err != nil {
		t.Fatalf("SET sql_mode: %v", err)
	}

	if _, err := e.Execute("CREATE TABLE tx1 (id INT) ENGINE=ROCKETENGINE"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	result, err := e.Execute("SHOW CREATE TABLE tx1")
	if err != nil {
		t.Fatalf("SHOW CREATE TABLE: %v", err)
	}
	if len(result.Rows) == 0 {
		t.Fatal("SHOW CREATE TABLE returned no rows")
	}

	createSQL := ""
	for _, row := range result.Rows {
		for _, col := range row {
			createSQL += fmt.Sprintf("%v", col) + " "
		}
	}

	if strings.Contains(strings.ToUpper(createSQL), "ROCKETENGINE") {
		t.Errorf("SHOW CREATE TABLE contains ROCKETENGINE; expected InnoDB substitution. Got: %s", createSQL)
	}
	if !strings.Contains(strings.ToUpper(createSQL), "INNODB") {
		t.Errorf("SHOW CREATE TABLE does not contain InnoDB. Got: %s", createSQL)
	}
}

// TestAlterTableUnknownEngine_WithSubstitution verifies Bug 2:
// ALTER TABLE ENGINE=UnknownEngine should emit Warning 1286 + 1266 (same as CREATE TABLE).
func TestAlterTableUnknownEngine_WithSubstitution(t *testing.T) {
	e := newTestExecutor(t)

	if _, err := e.Execute("CREATE TABLE tx1 (id INT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	if _, err := e.Execute("SET sql_mode=''"); err != nil {
		t.Fatalf("SET sql_mode: %v", err)
	}

	if _, err := e.Execute("ALTER TABLE tx1 ENGINE=ROCKETENGINE"); err != nil {
		t.Fatalf("ALTER TABLE: %v", err)
	}

	warnResult, err := e.Execute("SHOW WARNINGS")
	if err != nil {
		t.Fatalf("SHOW WARNINGS: %v", err)
	}
	var has1286, has1266 bool
	for _, row := range warnResult.Rows {
		if len(row) >= 2 {
			code := fmt.Sprintf("%v", row[1])
			if code == "1286" {
				has1286 = true
			}
			if code == "1266" {
				has1266 = true
			}
		}
	}
	if !has1286 {
		t.Error("expected Warning 1286 (Unknown storage engine) from ALTER TABLE, not found")
	}
	if !has1266 {
		t.Error("expected Warning 1266 (Using storage engine InnoDB) from ALTER TABLE, not found")
	}
}

// TestAlterTableUnknownEngine_NoSubstitution verifies that with NO_ENGINE_SUBSTITUTION
// ALTER TABLE with an unknown engine returns Error 1286.
func TestAlterTableUnknownEngine_NoSubstitution(t *testing.T) {
	e := newTestExecutor(t)

	if _, err := e.Execute("CREATE TABLE tx1 (id INT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	_, err := e.Execute("ALTER TABLE tx1 ENGINE=ROCKETENGINE")
	if err == nil {
		t.Fatal("expected error for unknown engine with NO_ENGINE_SUBSTITUTION, got nil")
	}
	if !strings.Contains(err.Error(), "1286") {
		t.Errorf("expected error 1286, got: %v", err)
	}
}
