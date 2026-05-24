package executor

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/myuon/mylite/storage"
	"vitess.io/vitess/go/vt/sqlparser"
)

// innodbLockWaitTimeoutSec returns the session innodb_lock_wait_timeout in seconds.
func (e *Executor) innodbLockWaitTimeoutSec() float64 {
	if v, ok := e.getSysVarSession("innodb_lock_wait_timeout"); ok {
		if t, err := strconv.ParseFloat(v, 64); err == nil {
			return t
		}
	}
	return 50.0
}

// rowsExaminedByJoinForTable returns base-table rows InnoDB locks during RC join scans.
func (e *Executor) rowsExaminedByJoinForTable(stmt *sqlparser.Select, dbName, tableName string, pkCols []string) []storage.Row {
	if len(stmt.From) != 1 {
		return nil
	}
	jte, ok := stmt.From[0].(*sqlparser.JoinTableExpr)
	if !ok || (jte.Join != sqlparser.NaturalJoinType && jte.Join != sqlparser.NaturalLeftJoinType) {
		return nil
	}
	baseATE, baseOK := jte.LeftExpr.(*sqlparser.AliasedTableExpr)
	derivedATE, derivedOK := jte.RightExpr.(*sqlparser.AliasedTableExpr)
	if !baseOK || !derivedOK {
		return nil
	}
	baseTN, ok := baseATE.Expr.(sqlparser.TableName)
	if !ok || !strings.EqualFold(baseTN.Name.String(), tableName) {
		return nil
	}
	if _, ok := derivedATE.Expr.(*sqlparser.DerivedTable); !ok {
		return nil
	}
	rightCols := make(map[string]bool)
	for _, c := range derivedTableColumnNames(derivedATE) {
		rightCols[strings.ToLower(c)] = true
	}
	var naturalCols []string
	for _, c := range tableColumnNamesForLock(e, dbName, tableName) {
		if rightCols[strings.ToLower(c)] {
			naturalCols = append(naturalCols, c)
		}
	}
	if len(naturalCols) == 0 {
		return nil
	}
	probeVals := derivedTableProbeValues(derivedATE, naturalCols)
	if len(probeVals) == 0 {
		return nil
	}
	storTbl, err := e.Storage.GetTable(dbName, tableName)
	if err != nil {
		return nil
	}
	var out []storage.Row
	for _, row := range storTbl.Rows {
		if rowMatchesProbeValues(row, tableName, pkCols, probeVals) {
			out = append(out, row)
		}
	}
	return out
}

func tableColumnNamesForLock(e *Executor, dbName, tableName string) []string {
	db, err := e.Catalog.GetDatabase(dbName)
	if err != nil {
		return nil
	}
	def, err := db.GetTable(tableName)
	if err != nil || def == nil {
		return nil
	}
	var cols []string
	for _, c := range def.Columns {
		cols = append(cols, c.Name)
	}
	return cols
}

func derivedTableColumnNames(ate *sqlparser.AliasedTableExpr) []string {
	dt, ok := ate.Expr.(*sqlparser.DerivedTable)
	if !ok {
		return nil
	}
	sel, ok := flattenDerivedSelectForLock(dt.Select)
	if !ok {
		return nil
	}
	var cols []string
	for _, expr := range sel.SelectExprs.Exprs {
		se, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}
		if !se.As.IsEmpty() {
			cols = append(cols, se.As.String())
		} else if col, ok := se.Expr.(*sqlparser.ColName); ok {
			cols = append(cols, col.Name.String())
		}
	}
	return cols
}

func flattenUnionSelectsForLock(u *sqlparser.Union) []*sqlparser.Select {
	var out []*sqlparser.Select
	var walk func(sqlparser.TableStatement)
	walk = func(stmt sqlparser.TableStatement) {
		switch s := stmt.(type) {
		case *sqlparser.Select:
			out = append(out, s)
		case *sqlparser.Union:
			walk(s.Left)
			walk(s.Right)
		}
	}
	walk(u)
	return out
}

func flattenDerivedSelectForLock(sel sqlparser.TableStatement) (*sqlparser.Select, bool) {
	switch s := sel.(type) {
	case *sqlparser.Select:
		return s, true
	case *sqlparser.Union:
		if selects := flattenUnionSelectsForLock(s); len(selects) > 0 {
			return selects[0], true
		}
	}
	return nil, false
}

func derivedTableProbeValues(ate *sqlparser.AliasedTableExpr, naturalCols []string) map[string]map[string]bool {
	dt, ok := ate.Expr.(*sqlparser.DerivedTable)
	if !ok {
		return nil
	}
	var selects []*sqlparser.Select
	switch s := dt.Select.(type) {
	case *sqlparser.Select:
		selects = []*sqlparser.Select{s}
	case *sqlparser.Union:
		selects = flattenUnionSelectsForLock(s)
	default:
		return nil
	}
	probe := make(map[string]map[string]bool)
	for _, col := range naturalCols {
		probe[strings.ToLower(col)] = make(map[string]bool)
	}
	for _, sel := range selects {
		colIdx := make(map[string]int)
		for i, expr := range sel.SelectExprs.Exprs {
			ae, ok := expr.(*sqlparser.AliasedExpr)
			if !ok {
				continue
			}
			name := ""
			if !ae.As.IsEmpty() {
				name = strings.ToLower(ae.As.String())
			} else if c, ok := ae.Expr.(*sqlparser.ColName); ok {
				name = strings.ToLower(c.Name.String())
			}
			if name != "" {
				colIdx[name] = i
			}
		}
		for _, col := range naturalCols {
			lc := strings.ToLower(col)
			idx, ok := colIdx[lc]
			if !ok {
				continue
			}
			ae := sel.SelectExprs.Exprs[idx].(*sqlparser.AliasedExpr)
			if lit, ok := ae.Expr.(*sqlparser.Literal); ok {
				probe[lc][fmt.Sprintf("%v", lit.Val)] = true
			}
		}
	}
	return probe
}

func mergeLockRowsByPK(dbName, tableName string, pkCols []string, a, b []storage.Row) []storage.Row {
	seen := make(map[string]bool)
	var out []storage.Row
	add := func(row storage.Row) {
		key := buildRowLockKey(dbName, tableName, pkCols, row)
		if seen[key] {
			return
		}
		seen[key] = true
		out = append(out, row)
	}
	for _, row := range a {
		add(row)
	}
	for _, row := range b {
		add(row)
	}
	return out
}

func rowMatchesProbeValues(row storage.Row, tableName string, pkCols []string, probe map[string]map[string]bool) bool {
	// Lock rows that would be examined via eq_ref on the primary key during the join.
	for _, pk := range pkCols {
		colName := pk
		if ci := strings.Index(colName, "("); ci >= 0 {
			colName = colName[:ci]
		}
		lc := strings.ToLower(colName)
		vals, ok := probe[lc]
		if !ok || len(vals) == 0 {
			return false
		}
		v, ok := row[colName]
		if !ok {
			v = row[tableName+"."+colName]
		}
		if !ok {
			return false
		}
		if !vals[fmt.Sprintf("%v", v)] {
			return false
		}
	}
	return len(pkCols) > 0
}

type lockableTableRef struct {
	db   string
	name string
}

func collectLockableBaseTables(from []sqlparser.TableExpr, defaultDB string) []lockableTableRef {
	var out []lockableTableRef
	var walk func(sqlparser.TableExpr)
	walk = func(te sqlparser.TableExpr) {
		switch t := te.(type) {
		case *sqlparser.AliasedTableExpr:
			if tn, ok := t.Expr.(sqlparser.TableName); ok {
				db := defaultDB
				if !tn.Qualifier.IsEmpty() {
					db = tn.Qualifier.String()
				}
				out = append(out, lockableTableRef{db: db, name: tn.Name.String()})
			}
		case *sqlparser.JoinTableExpr:
			walk(t.LeftExpr)
			walk(t.RightExpr)
		case *sqlparser.ParenTableExpr:
			for _, inner := range t.Exprs {
				walk(inner)
			}
		}
	}
	for _, te := range from {
		walk(te)
	}
	return out
}

func selectFromHasJoin(from []sqlparser.TableExpr) bool {
	for _, te := range from {
		if tableExprContainsJoin(te) {
			return true
		}
	}
	return false
}

func tableExprContainsJoin(te sqlparser.TableExpr) bool {
	switch t := te.(type) {
	case *sqlparser.JoinTableExpr:
		return true
	case *sqlparser.ParenTableExpr:
		for _, inner := range t.Exprs {
			if tableExprContainsJoin(inner) {
				return true
			}
		}
	}
	return false
}
