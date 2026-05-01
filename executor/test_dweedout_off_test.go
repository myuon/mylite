package executor

import (
	"fmt"
	"strings"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// TestDweedoutOff_EMPNUM verifies that when optimizer_switch=duplicateweedout=off
// and the inner SELECT is "EMPNUM-style" (flat nested IN where every nested
// subquery has exactly 1 table in FROM), mylite uses MATERIALIZED rather than
// flatten via DuplicateWeedout. See issue #306.
func TestDweedoutOff_EMPNUM(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"

	cmds := []string{
		"SET optimizer_switch='semijoin=on,materialization=on'",
		"SET optimizer_switch='loosescan=off'",
		"SET optimizer_switch='firstmatch=off'",
		"SET optimizer_switch='duplicateweedout=off'",
		"CREATE TABLE staff (EMPNUM CHAR(3) NOT NULL, EMPNAME CHAR(20), GRADE DECIMAL(4), CITY CHAR(15))",
		"CREATE TABLE proj (PNUM CHAR(3) NOT NULL, PNAME CHAR(20), PTYPE CHAR(6), BUDGET DECIMAL(9), CITY CHAR(15))",
		"CREATE TABLE works (EMPNUM CHAR(3) NOT NULL, PNUM CHAR(3) NOT NULL, HOURS DECIMAL(5))",
		"INSERT INTO staff VALUES ('E1','Alice',12,'Deale')",
		"INSERT INTO staff VALUES ('E2','Betty',10,'Vienna')",
		"INSERT INTO staff VALUES ('E3','Carmen',13,'Vienna')",
		"INSERT INTO proj VALUES  ('P1','MXSS','Design',10000,'Deale')",
		"INSERT INTO proj VALUES  ('P2','CALM','Code',30000,'Vienna')",
		"INSERT INTO works VALUES  ('E1','P1',40)",
	}
	for _, cmd := range cmds {
		if _, err := e.Execute(cmd); err != nil {
			t.Fatalf("Setup %q failed: %v", cmd, err)
		}
	}

	res, err := e.Execute("EXPLAIN SELECT EMPNUM, EMPNAME FROM staff WHERE EMPNUM IN (SELECT EMPNUM FROM works WHERE PNUM IN (SELECT PNUM FROM proj))")
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	hasMaterialized := false
	hasStartTemporary := false
	for _, row := range res.Rows {
		st := fmt.Sprintf("%v", row[1])
		ex := fmt.Sprintf("%v", row[11])
		if st == "MATERIALIZED" {
			hasMaterialized = true
		}
		if strings.Contains(ex, "Start temporary") {
			hasStartTemporary = true
		}
	}
	if !hasMaterialized {
		t.Errorf("Expected at least one MATERIALIZED row for EMPNUM-style nested IN with duplicateweedout=off; got rows: %+v", res.Rows)
	}
	if hasStartTemporary {
		t.Errorf("Did not expect DuplicateWeedout flatten (Start temporary) for EMPNUM-style; got rows: %+v", res.Rows)
	}
}

// TestDweedoutOff_4Level verifies that when optimizer_switch=duplicateweedout=off
// and the inner SELECT has a multi-table inner subquery (4-level nested IN),
// mylite still falls back to DuplicateWeedout-style flatten (Start temporary /
// End temporary), matching MySQL. See issue #306.
func TestDweedoutOff_4Level(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"

	cmds := []string{
		"SET optimizer_switch='semijoin=on,materialization=on'",
		"SET optimizer_switch='loosescan=off'",
		"SET optimizer_switch='firstmatch=off'",
		"SET optimizer_switch='duplicateweedout=off'",
		"CREATE TABLE t1_16 (a1 blob(16), a2 blob(16))",
		"CREATE TABLE t2_16 (b1 blob(16), b2 blob(16))",
		"CREATE TABLE t1 (a1 char(8), a2 char(8))",
		"CREATE TABLE t2 (b1 char(8), b2 char(8))",
		"CREATE TABLE t3 (c1 char(8), c2 char(8))",
		"INSERT INTO t1_16 VALUES ('1 - 01xxxxxxxxxxxx', '2 - 01xxxxxxxxxxxx')",
		"INSERT INTO t2_16 VALUES ('1 - 01xxxxxxxxxxxx', '2 - 01xxxxxxxxxxxx')",
		"INSERT INTO t1 VALUES ('1 - 01', '2 - 01')",
		"INSERT INTO t2 VALUES ('1 - 01', '2 - 01')",
		"INSERT INTO t3 VALUES ('1 - 01', '2 - 01')",
	}
	for _, cmd := range cmds {
		if _, err := e.Execute(cmd); err != nil {
			t.Fatalf("Setup %q failed: %v", cmd, err)
		}
	}

	q := `EXPLAIN SELECT * FROM t1
WHERE CONCAT(a1,'x') IN
(SELECT LEFT(a1,8) FROM t1_16
WHERE (a1,a2) IN
(SELECT t2_16.b1, t2_16.b2 FROM t2_16, t2
WHERE t2.b2 = SUBSTRING(t2_16.b2,1,6) AND
t2.b1 IN (SELECT c1 FROM t3 WHERE c2 > '0')))`
	res, err := e.Execute(q)
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	hasMaterialized := false
	hasStartTemporary := false
	allSimpleID1 := true
	for _, row := range res.Rows {
		st := fmt.Sprintf("%v", row[1])
		ex := fmt.Sprintf("%v", row[11])
		if st == "MATERIALIZED" {
			hasMaterialized = true
		}
		if strings.Contains(ex, "Start temporary") {
			hasStartTemporary = true
		}
		if fmt.Sprintf("%v", row[0]) != "1" || st != "SIMPLE" {
			allSimpleID1 = false
		}
	}
	if hasMaterialized {
		t.Errorf("Did not expect MATERIALIZED rows for 4-level multi-table inner subquery with duplicateweedout=off; got rows: %+v", res.Rows)
	}
	if !hasStartTemporary {
		t.Errorf("Expected DuplicateWeedout flatten (Start temporary marker) for 4-level case; got rows: %+v", res.Rows)
	}
	if !allSimpleID1 {
		t.Errorf("Expected all rows to be id=1 SIMPLE for 4-level DuplicateWeedout flatten; got rows: %+v", res.Rows)
	}
}

// TestDweedoutOff_IndexedFlatten verifies that when nested IN join columns are
// backed by PRIMARY KEY indexes, mylite still flattens (eq_ref path) even with
// duplicateweedout=off, matching the v1 view + procedure case in
// other/subquery_sj_mat (BUG#50089).
func TestDweedoutOff_IndexedFlatten(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"

	cmds := []string{
		"SET optimizer_switch='semijoin=on,materialization=on'",
		"SET optimizer_switch='loosescan=off'",
		"SET optimizer_switch='firstmatch=off'",
		"SET optimizer_switch='duplicateweedout=off'",
		"CREATE TABLE t1 (t1field INTEGER, PRIMARY KEY(t1field))",
		"INSERT INTO t1 VALUES (1),(2)",
	}
	for _, cmd := range cmds {
		if _, err := e.Execute(cmd); err != nil {
			t.Fatalf("Setup %q failed: %v", cmd, err)
		}
	}

	// EXPLAIN SELECT t1field FROM t1 WHERE t1field IN
	//   (SELECT t1field FROM t1 a WHERE a.t1field IN (SELECT t1field FROM t1));
	// MySQL flattens to 3 SIMPLE rows because t1.t1field has PRIMARY KEY → eq_ref.
	res, err := e.Execute("EXPLAIN SELECT t1field FROM t1 WHERE t1field IN (SELECT t1field FROM t1 a WHERE a.t1field IN (SELECT t1field FROM t1))")
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	hasMaterialized := false
	for _, row := range res.Rows {
		if fmt.Sprintf("%v", row[1]) == "MATERIALIZED" {
			hasMaterialized = true
			break
		}
	}
	if hasMaterialized {
		t.Errorf("Expected flatten (no MATERIALIZED) for indexed nested IN with duplicateweedout=off; got rows: %+v", res.Rows)
	}
}
