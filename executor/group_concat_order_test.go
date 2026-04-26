package executor

import (
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

func TestGroupConcatOrderByAscCI(t *testing.T) {
	cat := catalog.New()
	st := storage.NewEngine()
	exec := New(cat, st)

	queries := []string{
		"create table t1 (grp int, c char(10) not null)",
		"insert into t1 values (3,'E')",
		"insert into t1 values (3,'C')",
		"insert into t1 values (3,'D')",
		"insert into t1 values (3,'d')",
		"insert into t1 values (3,'d')",
		"insert into t1 values (3,'D')",
	}
	for _, q := range queries {
		_, err := exec.Execute(q)
		if err != nil {
			t.Fatalf("setup query %q failed: %v", q, err)
		}
	}

	// Test GROUP_CONCAT ORDER BY c ASC (case-insensitive)
	result, err := exec.Execute("select group_concat(c order by c) from t1 group by grp")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(result.Rows) == 0 {
		t.Fatal("no rows returned")
	}
	got := toString(result.Rows[0][0])
	// Under utf8mb4_0900_ai_ci: C=c, D=d, so sorted: C,D,d,d,D,E (stable sort preserves insertion order for equal)
	if got != "C,D,d,d,D,E" {
		t.Errorf("expected 'C,D,d,d,D,E', got %q", got)
	}
}

func TestGroupConcatOrderByDescCI(t *testing.T) {
	cat := catalog.New()
	st := storage.NewEngine()
	exec := New(cat, st)

	queries := []string{
		"create table t1 (grp int, c char(10) not null)",
		"insert into t1 values (2,'b')",
		"insert into t1 values (2,'c')",
	}
	for _, q := range queries {
		_, err := exec.Execute(q)
		if err != nil {
			t.Fatalf("setup query %q failed: %v", q, err)
		}
	}

	result, err := exec.Execute(`select group_concat(distinct c order by c desc separator ",") from t1 group by grp`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(result.Rows) == 0 {
		t.Fatal("no rows returned")
	}
	got := toString(result.Rows[0][0])
	if got != "c,b" {
		t.Errorf("expected 'c,b', got %q", got)
	}
}

func TestGroupConcatDistinctCI(t *testing.T) {
	cat := catalog.New()
	st := storage.NewEngine()
	exec := New(cat, st)

	queries := []string{
		"create table t1 (grp int, c char(10) not null)",
		"insert into t1 values (3,'E')",
		"insert into t1 values (3,'C')",
		"insert into t1 values (3,'D')",
		"insert into t1 values (3,'d')",
		"insert into t1 values (3,'d')",
		"insert into t1 values (3,'D')",
	}
	for _, q := range queries {
		_, err := exec.Execute(q)
		if err != nil {
			t.Fatalf("setup query %q failed: %v", q, err)
		}
	}

	// DISTINCT with ORDER BY ASC: unique values (C, D, E) sorted ascending
	result, err := exec.Execute("select group_concat(distinct c order by c) from t1 group by grp")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(result.Rows) == 0 {
		t.Fatal("no rows returned")
	}
	got := toString(result.Rows[0][0])
	if got != "C,D,E" {
		t.Errorf("expected 'C,D,E', got %q", got)
	}
}

func TestGroupConcatNullValues(t *testing.T) {
	cat := catalog.New()
	st := storage.NewEngine()
	exec := New(cat, st)

	queries := []string{
		"create table t1 (grp int, c char(10))",
		"insert into t1 values (1,NULL)",
		"insert into t1 values (2,'b')",
		"insert into t1 values (2,NULL)",
	}
	for _, q := range queries {
		_, err := exec.Execute(q)
		if err != nil {
			t.Fatalf("setup query %q failed: %v", q, err)
		}
	}

	result, err := exec.Execute("select grp, group_concat(c order by c) from t1 group by grp")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	// grp=1: all NULL → result should be NULL
	if result.Rows[0][1] != nil {
		t.Errorf("grp=1 expected NULL, got %v", result.Rows[0][1])
	}
	// grp=2: has 'b' (non-NULL) → result should be 'b'
	got2 := toString(result.Rows[1][1])
	if got2 != "b" {
		t.Errorf("grp=2 expected 'b', got %q", got2)
	}
}
