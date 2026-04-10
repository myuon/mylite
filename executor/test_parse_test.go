package executor

import (
	"testing"
	"fmt"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestParseISUnknown(t *testing.T) {
	p := sqlparser.NewTestParser()
	stmt, err := p.Parse("SELECT * FROM x1 WHERE (d1,d2) IN (SELECT d1, d2 FROM x2) IS UNKNOWN")
	if err != nil {
		t.Logf("parse error: %v", err)
		return
	}
	sel := stmt.(*sqlparser.Select)
	where := sel.Where.Expr
	fmt.Printf("where type: %T\n", where)
	if isExpr, ok := where.(*sqlparser.IsExpr); ok {
		fmt.Printf("is expr right: %v\n", isExpr.Right)
	}
}
