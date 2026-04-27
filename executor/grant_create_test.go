package executor

import (
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

func TestGrantCreateTable(t *testing.T) {
	cat := catalog.New()
	stor := storage.NewEngine()
	e := New(cat, stor)

	_, err := e.Execute("CREATE DATABASE db_datadict")
	if err != nil { t.Fatal("CreateDB:", err) }

	_, err = e.Execute("CREATE USER 'testuser1'@'localhost'")
	if err != nil { t.Fatal("CreateUser:", err) }

	_, err = e.Execute("GRANT CREATE, SELECT ON db_datadict.* TO 'testuser1'@'localhost' WITH GRANT OPTION")
	if err != nil { t.Fatal("Grant:", err) }

	// Switch to testuser1
	_, err = e.Execute("SET @__current_user = 'testuser1'")
	if err != nil { t.Fatal("SetUser:", err) }

	_, err = e.Execute("USE db_datadict")
	if err != nil { t.Fatal("Use:", err) }

	// Try to create table as testuser1
	_, err = e.Execute("CREATE TABLE tb3 (f1 TEXT)")
	if err != nil {
		t.Errorf("Expected success, got error: %v", err)
	} else {
		t.Log("SUCCESS: CREATE TABLE tb3 worked")
	}
}
