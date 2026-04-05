package executor

import (
	"fmt"
	"testing"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestEnumValLen(t *testing.T) {
	longVal := "abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij"
	sql := fmt.Sprintf("CREATE TABLE t7(c1 ENUM('a', '%s'))", longVal)
	parser, _ := sqlparser.New(sqlparser.Options{})
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	ct := stmt.(*sqlparser.CreateTable)
	for _, col := range ct.TableSpec.Columns {
		t.Logf("col: %s type: %s", col.Name.String(), col.Type.Type)
		for i, ev := range col.Type.EnumValues {
			t.Logf("  enum[%d]: %q (len=%d)", i, ev, len(ev))
		}
	}
}
