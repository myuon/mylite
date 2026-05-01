package executor

import (
	"fmt"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// TestNotInNestedInMaterialized covers the Bug#12797534 query from
// subquery_sj_mat.result: a NOT IN (SELECT … WHERE col IN (SELECT … LEFT JOIN …))
// must keep the inner body MATERIALIZED instead of being flattened to
// DuplicateWeedout.  DuplicateWeedout is unsafe in anti-join contexts when the
// nested IN body has a LEFT JOIN, because LEFT JOIN preserves NULL-extended
// rows that DuplicateWeedout would erroneously discard.
//
// Expected EXPLAIN structure (id is masked by mtr in the .result file):
//
//	1 SIMPLE       t1
//	1 SIMPLE       <subquery2>     eq_ref <auto_key>
//	2 MATERIALIZED parent1         ALL
//	2 MATERIALIZED parent2         eq_ref PRIMARY
//	2 MATERIALIZED grandparent1    ref col_varchar_key
func TestNotInNestedInMaterialized(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"

	cmds := []string{
		"CREATE TABLE t1 (g1 VARCHAR(1) NOT NULL) charset utf8mb4 ENGINE=InnoDB",
		"INSERT INTO t1 VALUES ('d'), ('s')",
		"CREATE TABLE t2 (pk INT NOT NULL, col_int_key INT NOT NULL, col_varchar_key VARCHAR(1) NOT NULL, col_varchar_nokey VARCHAR(1) NOT NULL, PRIMARY KEY (pk), KEY col_varchar_key(col_varchar_key, col_int_key)) charset utf8mb4 ENGINE=InnoDB",
		"INSERT INTO t2 VALUES (1,4,'j','j'), (2,6,'v','v'), (3,3,'c','c'), (4,5,'m','m'), (5,3,'d','d'), (6,246,'d','d'), (7,2,'y','y'), (8,9,'t','t'), (9,3,'d','d'), (10,8,'s','s'), (11,1,'r','r'), (12,8,'m','m'), (13,8,'b','b'), (14,5,'x','x'), (15,7,'g','g'), (16,5,'p','p'), (17,1,'q','q'), (18,6,'w','w'), (19,2,'d','d'), (20,9,'e','e')",
		"CREATE TABLE t3 (pk INTEGER NOT NULL, PRIMARY KEY (pk)) ENGINE=InnoDB",
		"INSERT INTO t3 VALUES (10)",
	}
	for _, cmd := range cmds {
		if _, err := e.Execute(cmd); err != nil {
			t.Fatalf("Setup %q failed: %v", cmd, err)
		}
	}

	q := `EXPLAIN SELECT *
FROM t1
WHERE g1 NOT IN
(SELECT  grandparent1.col_varchar_nokey AS g1
FROM t2 AS grandparent1
WHERE grandparent1.col_varchar_key IN
(SELECT parent1.col_varchar_nokey AS p1
FROM t2 AS parent1 LEFT JOIN t3 AS parent2 USING (pk)
)
AND grandparent1.col_varchar_key IS NOT NULL
)`
	res, err := e.Execute(q)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if len(res.Rows) != 5 {
		t.Errorf("expected 5 EXPLAIN rows, got %d", len(res.Rows))
	}
	matRows := 0
	for _, row := range res.Rows {
		if fmt.Sprintf("%v", row[1]) == "MATERIALIZED" {
			matRows++
		}
	}
	if matRows != 3 {
		t.Errorf("expected 3 MATERIALIZED rows (parent1, parent2, grandparent1), got %d", matRows)
	}
}
