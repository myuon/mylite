package executor

import (
	"fmt"
	"testing"
	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

func TestSavepointProcedure(t *testing.T) {
	cat := catalog.New()
	stor := storage.NewEngine()
	exec := New(cat, stor)
	
	must := func(sql string) {
		_, err := exec.Execute(sql)
		if err != nil {
			t.Fatalf("Query %q failed: %v", sql, err)
		}
	}
	expectErr := func(sql string) error {
		_, err := exec.Execute(sql)
		if err == nil {
			t.Fatalf("Query %q should have failed", sql)
		}
		return err
	}
	
	must("CREATE DATABASE IF NOT EXISTS test")
	must("USE test")
	
	must("begin")
	must("savepoint sv")
	must("create procedure p1() begin end")
	err := expectErr("rollback to savepoint sv")
	fmt.Printf("rollback error: %v\n", err)
	
	must("begin")
	must("savepoint sv")
	
	_, err2 := exec.Execute("alter procedure p1 comment 'changed comment'")
	fmt.Printf("alter result: err=%v\n", err2)
	
	err3 := expectErr("rollback to savepoint sv")
	fmt.Printf("rollback error 2: %v\n", err3)
	
	must("begin")
	must("savepoint sv")
	must("drop procedure p1")
	err4 := expectErr("rollback to savepoint sv")
	fmt.Printf("rollback error 3: %v\n", err4)
}
