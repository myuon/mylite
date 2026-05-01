package executor

import (
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// TestInsertSelectBulkAIReservation verifies that INSERT...SELECT under the
// default innodb_autoinc_lock_mode (1) reserves AUTO_INCREMENT values in
// power-of-2 chunks like MySQL's "bulk INSERT" mode.
func TestInsertSelectBulkAIReservation(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)

	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("setup: %v", err)
	}
	e.CurrentDB = "test"

	if _, err := e.Execute("CREATE TABLE t1 (a INT NOT NULL AUTO_INCREMENT PRIMARY KEY, b INT) ENGINE=InnoDB"); err != nil {
		t.Fatalf("create t1: %v", err)
	}
	if _, err := e.Execute("INSERT INTO t1 VALUES(0, 1), (0, 2), (0, 3), (0, 4), (0, 5)"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// 3x INSERT...SELECT, doubling each time.
	for i := 0; i < 3; i++ {
		if _, err := e.Execute("INSERT INTO t1(b) SELECT b FROM t1"); err != nil {
			t.Fatalf("insert select #%d: %v", i, err)
		}
	}

	tbl, err := store.GetTable("test", "t1")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	got := tbl.AutoIncrementValue()
	// AutoIncrementValue() stores the last-used AI value, so SHOW CREATE TABLE
	// reports AUTO_INCREMENT=last+1. With power-of-2 bulk reservation:
	//   seed   (5 rows VALUES, reserve=5): last=5
	//   round 1 (5 src,  reserve=7):       last=12
	//   round 2 (10 src, reserve=15):      last=27
	//   round 3 (20 src, reserve=31):      last=58
	// SHOW CREATE TABLE would report AUTO_INCREMENT=59.
	if got != 58 {
		t.Errorf("expected AI counter = 58 after 3 INSERT...SELECT (SHOW would print AUTO_INCREMENT=59), got %d", got)
	}
}
