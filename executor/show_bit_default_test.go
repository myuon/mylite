package executor

import (
	"strings"
	"testing"
)

// TestBitBinaryDefaultShow exercises SHOW CREATE TABLE rendering of DEFAULT
// values for BIT, BINARY, and VARBINARY columns added via ALTER TABLE with
// b'...' / 0x... literals. MySQL renders these as:
//   - BIT(N) DEFAULT b'...' (unquoted)
//   - BINARY(N) DEFAULT '<bytes>' (padded to width with NUL escapes)
//   - VARBINARY(N) DEFAULT '<bytes>' (high-bit non-utf8 byte rendered as ?)
func TestBitBinaryDefaultShow(t *testing.T) {
	e := newTestExecutor(t)
	if _, err := e.Execute("CREATE TABLE bit_def_t1(a INT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute("ALTER TABLE bit_def_t1 ADD COLUMN k2 BIT(8) NOT NULL DEFAULT b'101010'"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute("ALTER TABLE bit_def_t1 ADD COLUMN o1 BINARY(10) DEFAULT 0x11223344"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute("ALTER TABLE bit_def_t1 ADD COLUMN o2 VARBINARY(10) DEFAULT 0x55667788"); err != nil {
		t.Fatal(err)
	}
	r, err := e.Execute("SHOW CREATE TABLE bit_def_t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) == 0 || len(r.Rows[0]) < 2 {
		t.Fatalf("expected one row with create stmt, got %v", r.Rows)
	}
	createStmt, ok := r.Rows[0][1].(string)
	if !ok {
		t.Fatalf("expected create stmt to be string, got %T", r.Rows[0][1])
	}
	checks := []string{
		"`k2` bit(8) NOT NULL DEFAULT b'101010'",
		"`o1` binary(10) DEFAULT '\x11\"3D\\0\\0\\0\\0\\0\\0'",
		"`o2` varbinary(10) DEFAULT 'Ufw?'",
	}
	for _, want := range checks {
		if !strings.Contains(createStmt, want) {
			t.Errorf("create stmt missing %q\nfull stmt:\n%s", want, createStmt)
		}
	}
}
