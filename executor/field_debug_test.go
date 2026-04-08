package executor

import (
	"fmt"
	"strings"
	"testing"
	
	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

func TestFieldDecimalDebug(t *testing.T) {
	cat := catalog.New()
	stor := storage.NewEngine()
	exec := New(cat, stor)

	exec.Execute("CREATE DATABASE IF NOT EXISTS test")
	exec.Execute("USE test")

	res, err := exec.Execute(`select field("b","a",NULL),field(1,0,NULL)+0,field(1.0,0.0,NULL)+0.0,field(1.0e1,0.0e1,NULL)+0.0e1`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	
	for _, row := range res.Rows {
		parts := make([]string, len(row))
		for i, v := range row {
			parts[i] = fmt.Sprintf("%v (type=%T)", v, v)
		}
		fmt.Println(strings.Join(parts, " | "))
	}
}
