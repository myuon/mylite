package executor_test

import (
	"fmt"
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestAlter64(t *testing.T) {
	var parts []string
	for i := 1; i <= 64; i++ {
		parts = append(parts, fmt.Sprintf("add key a%03d_long_123456789_123456789_123456789_123456789_123456789_1234 (c1,c2,c3)", i))
	}
	sql := "alter table t1 " + strings.Join(parts, ", ")
	stmt, err := sqlparser.NewTestParser().Parse(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	alter := stmt.(*sqlparser.AlterTable)
	t.Logf("Number of ALTER options: %d\n", len(alter.AlterOptions))
}
