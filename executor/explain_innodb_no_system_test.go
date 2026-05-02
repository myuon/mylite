package executor

import (
	"strings"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// TestExplain_InnoDBSingleRowKeepsALL: when a sole-table SELECT scans an
// explicitly InnoDB table that happens to hold a single row, MySQL keeps
// access_type "ALL" rather than promoting to "system". InnoDB row counts
// come from probabilistic statistics that the optimizer does not treat as
// exact at compile time; the "system" optimization is reserved for engines
// (e.g. MyISAM) where the row count is known exactly.
//
// Reproduces the EXISTS scenario at explain_json_all line 1297+: t1 was
// created with an explicit `ENGINE=InnoDB` clause and contains one row;
// MySQL emits access_type=ALL for it.  Without this exception mylite
// promotes ALL→system and the JSON shape diverges (the "attached_condition"
// and any "attached_subqueries" the optimizer would attach to the table
// disappear).
func TestExplain_InnoDBSingleRowKeepsALL(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"
	for _, q := range []string{
		"CREATE TABLE t1 (i1 INTEGER NOT NULL, c1 VARCHAR(1) NOT NULL) charset latin1 ENGINE=InnoDB",
		"INSERT INTO t1 VALUES (2,'w')",
		"CREATE TABLE t3 (c1 VARCHAR(1) NOT NULL) charset latin1 ENGINE=InnoDB",
		"INSERT INTO t3 VALUES ('v')",
	} {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	res, err := e.Execute(`EXPLAIN FORMAT=JSON SELECT i1 FROM t1 WHERE c1 = (SELECT MIN(c1) FROM t3)`)
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	js, _ := res.Rows[0][0].(string)
	// Find t1's table block and assert access_type:"ALL", not "system".
	t1Idx := strings.Index(js, `"table_name": "t1"`)
	if t1Idx < 0 {
		t.Fatalf("expected t1 in EXPLAIN JSON; got:\n%s", js)
	}
	end := t1Idx + 200
	if end > len(js) {
		end = len(js)
	}
	t1Block := js[t1Idx:end]
	if strings.Contains(t1Block, `"access_type": "system"`) {
		t.Errorf("t1 (InnoDB, 1 row) should keep access_type ALL, got 'system'; block:\n%s", t1Block)
	}
	if !strings.Contains(t1Block, `"access_type": "ALL"`) {
		t.Errorf("t1 (InnoDB, 1 row) should have access_type ALL; block:\n%s", t1Block)
	}
}

// TestExplain_NonInnoDBSingleRowPromotesSystem: when the resolved engine is
// MyISAM (e.g. force_myisam_default.inc has set default_storage_engine=MyISAM
// for the session) ALL→system promotion still applies.  MyISAM keeps an
// exact row count so MySQL's optimizer treats the single-row table as
// effectively constant.
func TestExplain_NonInnoDBSingleRowPromotesSystem(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)
	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"
	for _, q := range []string{
		// Mirror force_myisam_default.inc so the implicit engine resolves to
		// MyISAM rather than mylite's process-default InnoDB.
		"SET @@SESSION.default_storage_engine = MyISAM",
		"CREATE TABLE t1 (i INT)",
		"INSERT INTO t1 VALUES (1)",
	} {
		if _, err := e.Execute(q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	res, err := e.Execute(`EXPLAIN FORMAT=JSON SELECT i FROM t1`)
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	js, _ := res.Rows[0][0].(string)
	if !strings.Contains(js, `"access_type": "system"`) {
		t.Errorf("t1 (MyISAM, 1 row) should be promoted to system; got:\n%s", js)
	}
}
