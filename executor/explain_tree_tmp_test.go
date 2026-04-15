package executor

import (
    "fmt"
    "strings"
    "testing"
    "vitess.io/vitess/go/vt/sqlparser"
)

func TestExplainParseQuery(t *testing.T) {
    p := sqlparser.NewTestParser()
    query := "SELECT JSON_OBJECTAGG(NULL, '') FROM t1, t1 AS t2 GROUP BY t1.pk LIMIT 2 OFFSET 5"
    stmt, err := p.Parse(query)
    if err != nil {
        t.Fatal(err)
    }
    sel := stmt.(*sqlparser.Select)
    
    if len(sel.SelectExprs.Exprs) > 0 {
        se := sel.SelectExprs.Exprs[0]
        if ae, ok := se.(*sqlparser.AliasedExpr); ok {
            fmt.Println("Expr:", sqlparser.String(ae.Expr))
            fmt.Println("Expr lower:", strings.ToLower(sqlparser.String(ae.Expr)))
        }
    }
    
    if sel.GroupBy != nil && len(sel.GroupBy.Exprs) > 0 {
        fmt.Println("GroupBy[0]:", sqlparser.String(sel.GroupBy.Exprs[0]))
    }
    
    for i, te := range sel.From {
        switch t2 := te.(type) {
        case *sqlparser.AliasedTableExpr:
            tn, _ := t2.Expr.(sqlparser.TableName)
            alias := t2.As.String()
            fmt.Printf("[%d] table=%q alias=%q\n", i, tn.Name.String(), alias)
        }
    }
}
