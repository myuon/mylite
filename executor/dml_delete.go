package executor

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
	"vitess.io/vitess/go/vt/sqlparser"
)

// classifyDeletePredsForTables splits AND predicates into per-table buckets
// and cross-table predicates. A predicate goes to table i if all its column
// references use the same qualifier matching tableAliases[i].
func classifyDeletePredsForTables(where sqlparser.Expr, tableAliases []string) (perTable [][]sqlparser.Expr, cross []sqlparser.Expr) {
	perTable = make([][]sqlparser.Expr, len(tableAliases))
	preds := splitANDPredicates(where)
	for _, pred := range preds {
		cols := extractColumnRefs(pred)
		matchedTable := -1
		allSameTable := true
		for _, col := range cols {
			cn, ok := col.(*sqlparser.ColName)
			if !ok || cn.Qualifier.IsEmpty() {
				allSameTable = false
				break
			}
			qualifier := cn.Qualifier.Name.String()
			found := -1
			for i, alias := range tableAliases {
				if strings.EqualFold(qualifier, alias) {
					found = i
					break
				}
			}
			if found < 0 {
				allSameTable = false
				break
			}
			if matchedTable < 0 {
				matchedTable = found
			} else if matchedTable != found {
				allSameTable = false
				break
			}
		}
		if allSameTable && matchedTable >= 0 {
			perTable[matchedTable] = append(perTable[matchedTable], pred)
		} else {
			cross = append(cross, pred)
		}
	}
	return
}

func (e *Executor) execDelete(stmt *sqlparser.Delete) (*Result, error) {
	// Handle WITH clause (CTEs) on DELETE statements.
	if stmt.With != nil && len(stmt.With.CTEs) > 0 {
		outerCTEMap := e.cteMap
		newCTEMap := make(map[string]*cteTable)
		if outerCTEMap != nil {
			for k, v := range outerCTEMap {
				newCTEMap[k] = v
			}
		}
		e.cteMap = newCTEMap
		defer func() { e.cteMap = outerCTEMap }()

		for _, cte := range stmt.With.CTEs {
			cteName := cte.ID.String()
			var subResult *Result
			if stmt.With.Recursive {
				var err error
				subResult, err = e.execRecursiveCTE(cteName, cte.Subquery, cte.Columns, newCTEMap)
				if err != nil {
					return nil, fmt.Errorf("CTE '%s': %w", cteName, err)
				}
			} else {
				var err error
				subResult, err = e.execTableStmtForUnion(cte.Subquery)
				if err != nil {
					return nil, fmt.Errorf("CTE '%s': %w", cteName, err)
				}
			}
			columns := subResult.Columns
			if len(cte.Columns) > 0 {
				for ci, ca := range cte.Columns {
					if ci < len(columns) {
						columns[ci] = ca.String()
					}
				}
			}
			colOrder := strings.Join(columns, "\x00")
			cteRows := make([]storage.Row, len(subResult.Rows))
			for i, row := range subResult.Rows {
				r := make(storage.Row, len(columns)+1)
				for j, col := range columns {
					if j < len(row) {
						r[col] = row[j]
					}
				}
				r["__column_order__"] = colOrder
				cteRows[i] = r
			}
			newCTEMap[cteName] = &cteTable{
				columns: columns,
				rows:    cteRows,
			}
		}
	}

	// Set insideDML so that sub-SELECTs in WHERE clause acquire row locks
	// (InnoDB acquires shared locks on rows read by subqueries within DML).
	prevInsideDML := e.insideDML
	if e.rowLockManager != nil {
		e.insideDML = true
	}
	defer func() {
		e.insideDML = prevInsideDML
		if !e.shouldAcquireRowLocks() && e.rowLockManager != nil {
			e.rowLockManager.ReleaseRowLocks(e.connectionID)
		}
	}()

	if len(stmt.TableExprs) == 0 {
		return nil, fmt.Errorf("no table specified")
	}

	// Multi-table DELETE: when there are multiple source tables (FROM clause).
	// Also when Targets is populated with real table names (not just modifiers like QUICK).
	// Single-table delete always has exactly 1 TableExpr.
	if len(stmt.TableExprs) > 1 {
		return e.execMultiTableDeleteAST(stmt)
	}
	if len(stmt.TableExprs) == 1 {
		if _, ok := stmt.TableExprs[0].(*sqlparser.JoinTableExpr); ok {
			return e.execMultiTableDeleteAST(stmt)
		}
	}
	// DELETE QUICK sets Targets=[QUICK] with 1 TableExpr; that's still single-table.
	if len(stmt.Targets) > 0 && len(stmt.Targets) != len(stmt.TableExprs) {
		return e.execMultiTableDeleteAST(stmt)
	}

	tableName := ""
	deleteDB := e.CurrentDB
	switch te := stmt.TableExprs[0].(type) {
	case *sqlparser.AliasedTableExpr:
		tableName = sqlparser.String(te.Expr)
		tableName = strings.Trim(tableName, "`")
		if strings.Contains(tableName, ".") {
			deleteDB, tableName = resolveTableNameDB(tableName, e.CurrentDB)
		}
	default:
		return nil, fmt.Errorf("unsupported table expression: %T", te)
	}

	// Resolve views: if tableName is a view, replace with the underlying base table.
	// Also collect the view's WHERE condition to merge with the DELETE's WHERE.
	var viewWhereExpr sqlparser.Expr
	if baseTable, isView, viewWhere, err := e.resolveViewToBaseTable(tableName); err != nil {
		return nil, err
	} else if isView {
		tableName = baseTable
		viewWhereExpr = viewWhere
	}
	// Merge view's WHERE condition into the DELETE's WHERE clause.
	if viewWhereExpr != nil {
		if stmt.Where == nil {
			stmt.Where = &sqlparser.Where{Type: sqlparser.WhereClause, Expr: viewWhereExpr}
		} else {
			stmt.Where.Expr = &sqlparser.AndExpr{Left: viewWhereExpr, Right: stmt.Where.Expr}
		}
	}

	// Reject DELETE on information_schema tables.
	if strings.EqualFold(deleteDB, "information_schema") {
		return nil, mysqlError(1044, "42000", "Access denied for user 'root'@'localhost' to database 'information_schema'")
	}

	// Handle performance_schema tables
	if strings.EqualFold(deleteDB, "performance_schema") {
		lowerTable := strings.ToLower(tableName)
		if lowerTable == "setup_actors" || lowerTable == "setup_objects" {
			return e.execPerfSchemaDelete(stmt, lowerTable)
		}
		return nil, mysqlError(1142, "42000", fmt.Sprintf("DELETE command denied to user 'root'@'localhost' for table '%s'", tableName))
	}

	tbl, err := e.Storage.GetTable(deleteDB, tableName)
	if err != nil {
		return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", deleteDB, tableName))
	}

	// MySQL error 1442: can't modify a table inside a trigger if the triggering statement is
	// already modifying that table.
	if e.functionOrTriggerDepth > 0 && strings.EqualFold(e.modifyingTable, tableName) {
		return nil, mysqlError(1442, "HY000",
			fmt.Sprintf("Can't update table '%s' in stored function/trigger because it is already used by statement which invoked this stored function/trigger.", tableName))
	}

	// Set queryTableDef so that IS NULL checks on zero-date NOT NULL columns work correctly
	// (MySQL: 0000-00-00 IS NULL = TRUE for NOT NULL date columns).
	oldQueryTableDef := e.queryTableDef
	e.queryTableDef = tbl.Def
	defer func() { e.queryTableDef = oldQueryTableDef }()

	// Track which table we're modifying so triggers can detect recursive modification.
	prevModifyingTable := e.modifyingTable
	e.modifyingTable = tableName
	defer func() { e.modifyingTable = prevModifyingTable }()

	tbl.Lock()
	defer tbl.Unlock()

	// Validate WHERE clause column references against table columns.
	// This catches unknown columns even when the table has no rows.
	if stmt.Where != nil {
		availCols := make(map[string]bool)
		for _, col := range tbl.Def.Columns {
			availCols[strings.ToLower(col.Name)] = true
		}
		if len(availCols) > 0 {
			walkErr := sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
				switch node.(type) {
				case *sqlparser.Subquery, *sqlparser.Select, *sqlparser.Union:
					return false, nil
				}
				if colName, ok := node.(*sqlparser.ColName); ok {
					if colName.Qualifier.IsEmpty() {
						cn := strings.ToLower(colName.Name.String())
						if !availCols[cn] {
							return false, mysqlError(1054, "42S22",
								fmt.Sprintf("Unknown column '%s' in 'where clause'", colName.Name.String()))
						}
					}
				}
				return true, nil
			}, stmt.Where.Expr)
			if walkErr != nil {
				return nil, walkErr
			}
		}
	}

	// Validate ORDER BY column references for single-table DELETE.
	// MySQL requires that ORDER BY columns exist in the deleted table.
	if stmt.OrderBy != nil {
		availCols := make(map[string]bool)
		for _, col := range tbl.Def.Columns {
			availCols[strings.ToLower(col.Name)] = true
		}
		for _, order := range stmt.OrderBy {
			expr := order.Expr
			switch e2 := expr.(type) {
			case *sqlparser.ColName:
				colName := strings.ToLower(e2.Name.String())
				if !availCols[colName] {
					if !e2.Qualifier.IsEmpty() {
						// Qualified reference like t2.x - check if qualifier matches the table
						qualifier := strings.ToLower(e2.Qualifier.Name.String())
						if !strings.EqualFold(qualifier, tableName) {
							// Qualifier doesn't match the table being deleted
							return nil, mysqlError(1054, "42S22",
								fmt.Sprintf("Unknown column '%s.%s' in 'order clause'", e2.Qualifier.Name.String(), e2.Name.String()))
						}
					}
					return nil, mysqlError(1054, "42S22",
						fmt.Sprintf("Unknown column '%s' in 'order clause'", e2.Name.String()))
				}
				// Even if column exists, qualifier must match the table
				if !e2.Qualifier.IsEmpty() {
					qualifier := strings.ToLower(e2.Qualifier.Name.String())
					if !strings.EqualFold(qualifier, tableName) && !strings.EqualFold(qualifier, deleteDB) {
						return nil, mysqlError(1054, "42S22",
							fmt.Sprintf("Unknown column '%s.%s' in 'order clause'", e2.Qualifier.Name.String(), e2.Name.String()))
					}
				}
			default:
				// For function calls or other expressions in ORDER BY, evaluate them to catch errors.
				// MySQL evaluates ORDER BY expressions and errors on invalid ones (e.g., f1(10) when f1 takes 0 args).
				dummyRow := storage.Row{}
				if len(tbl.Rows) > 0 {
					dummyRow = tbl.Rows[0]
				}
				if _, evalErr := e.evalRowExpr(expr, dummyRow); evalErr != nil {
					return nil, evalErr
				}
			case *sqlparser.Subquery:
				// Subquery in ORDER BY: validate inner SELECT's columns for the no-FROM case.
				// MySQL: "DELETE FROM t1 ORDER BY (SELECT x)" where x doesn't exist is an error.
				if e2.Select != nil {
					sel, ok := e2.Select.(*sqlparser.Select)
					if ok && sel.SelectExprs != nil && len(sel.SelectExprs.Exprs) > 0 {
						firstExpr := sel.SelectExprs.Exprs[0]
						if _, isStarExpr := firstExpr.(*sqlparser.StarExpr); !isStarExpr {
							if aliasedExpr, ok2 := firstExpr.(*sqlparser.AliasedExpr); ok2 {
								if colExpr, ok3 := aliasedExpr.Expr.(*sqlparser.ColName); ok3 {
									colName := strings.ToLower(colExpr.Name.String())
									if !availCols[colName] {
										return nil, mysqlError(1054, "42S22",
											fmt.Sprintf("Unknown column '%s' in 'field list'", colExpr.Name.String()))
									}
								}
							}
						}
					}
				}
			}
		}
	}

	// SQL_SAFE_UPDATES: reject DELETE without WHERE using a KEY column (unless LIMIT present).
	if err := e.checkSafeUpdate(tbl.Def, func() sqlparser.Expr {
		if stmt.Where != nil {
			return stmt.Where.Expr
		}
		return nil
	}(), stmt.Limit); err != nil {
		return nil, err
	}

	// If ORDER BY or LIMIT is specified, we need to determine which rows to
	// delete in order, then limit the deletion count.
	if stmt.OrderBy != nil || stmt.Limit != nil {
		// Get table def for column names (needed by applyOrderBy).
		db, dbErr := e.Catalog.GetDatabase(e.CurrentDB)
		if dbErr != nil {
			return nil, dbErr
		}
		def, defErr := db.GetTable(tableName)
		if defErr != nil {
			return nil, defErr
		}
		colNames := make([]string, len(def.Columns))
		for i, c := range def.Columns {
			colNames[i] = c.Name
		}
		numericOrderCols := numericOrderColumnSet(def, colNames)

		// When ORDER BY is specified, sort ALL rows first, then evaluate WHERE in sorted
		// order (MySQL semantics: rows are scanned in ORDER BY order, WHERE is evaluated
		// per-row in that order, and scanning stops once LIMIT rows match).
		// This ensures side effects in WHERE (e.g. @a:= col) reflect only the deleted rows.
		type indexedRow struct {
			idx int
			row storage.Row
		}

		// Build flat representation of ALL rows for sorting.
		allFlatRows := make([][]interface{}, len(tbl.Rows))
		for i, row := range tbl.Rows {
			r := make([]interface{}, len(colNames))
			for j, cn := range colNames {
				r[j] = row[cn]
			}
			r = append(r, i) // original index as last element
			allFlatRows[i] = r
		}

		// Sort ALL rows if ORDER BY is present.
		if stmt.OrderBy != nil {
			allFlatRows, err = applyOrderByWithTypeHints(stmt.OrderBy, colNames, allFlatRows, effectiveTableCollation(def), numericOrderCols)
			if err != nil {
				return nil, err
			}
		} else if stmt.Limit != nil && len(def.PrimaryKey) > 0 {
			// When LIMIT without ORDER BY, InnoDB scans in PRIMARY KEY order.
			var orderBy sqlparser.OrderBy
			for _, pkCol := range def.PrimaryKey {
				orderBy = append(orderBy, &sqlparser.Order{
					Expr:      &sqlparser.ColName{Name: sqlparser.NewIdentifierCI(pkCol)},
					Direction: sqlparser.AscOrder,
				})
			}
			allFlatRows, err = applyOrderByWithTypeHints(orderBy, colNames, allFlatRows, effectiveTableCollation(def), numericOrderCols)
			if err != nil {
				return nil, err
			}
		}

		// Compute limit count.
		limitCount := int64(-1) // -1 means no limit
		if stmt.Limit != nil {
			flatLimit, applyErr := applyLimit(stmt.Limit, allFlatRows)
			if applyErr != nil {
				return nil, applyErr
			}
			limitCount = int64(len(flatLimit))
		}

		// Evaluate WHERE in sorted order, stopping at limitCount matches.
		var flatRows [][]interface{}
		for _, r := range allFlatRows {
			origIdx := r[len(r)-1].(int)
			row := tbl.Rows[origIdx]
			match := true
			if stmt.Where != nil {
				m, wErr := e.evalWhere(stmt.Where.Expr, row)
				if wErr != nil {
					if bool(stmt.Ignore) {
						e.addWarning("Warning", 1242, strings.TrimPrefix(wErr.Error(), "ERROR 1242 (21000): "))
						match = false
					} else {
						return nil, wErr
					}
				} else {
					match = m
				}
			}
			if match {
				flatRows = append(flatRows, r)
				if limitCount >= 0 && int64(len(flatRows)) >= limitCount {
					break
				}
			}
		}

		// Collect the original indices to delete.
		deleteSet := make(map[int]bool, len(flatRows))
		for _, r := range flatRows {
			origIdx := r[len(r)-1].(int)
			deleteSet[origIdx] = true
		}

		// Acquire row locks for rows to be deleted (blocks if rows are locked by another connection)
		if e.rowLockManager != nil && len(deleteSet) > 0 && e.shouldAcquireRowLocks() {
			delIndices := make([]int, 0, len(deleteSet))
			for idx := range deleteSet {
				delIndices = append(delIndices, idx)
			}
			tbl.Unlock()
			lockErr := e.acquireRowLocksForRows(deleteDB, tableName, def, tbl.Rows, delIndices)
			tbl.Lock()
			if lockErr != nil {
				return nil, lockErr
			}
		}

		// Enforce FOREIGN KEY constraints for rows being deleted
		fkViolated := map[int]bool{}
		for idx := range deleteSet {
			tbl.Unlock()
			if fkErr := e.checkForeignKeyOnDelete(deleteDB, tableName, tbl.Rows[idx]); fkErr != nil {
				tbl.Lock()
				if bool(stmt.Ignore) {
					// DELETE IGNORE: add warning, skip this row
					errCode := 1451
					msg := strings.TrimPrefix(fkErr.Error(), "ERROR 1451 (23000): ")
					e.addWarning("Warning", errCode, msg)
					delete(deleteSet, idx)
					fkViolated[idx] = true
				} else {
					return nil, fkErr
				}
			} else {
				tbl.Lock()
			}
		}

		newRows := make([]storage.Row, 0, len(tbl.Rows)-len(deleteSet))
		for i, row := range tbl.Rows {
			if !deleteSet[i] {
				newRows = append(newRows, row)
			}
		}
		tbl.Rows = newRows
		tbl.InvalidateIndexes()
		return &Result{AffectedRows: uint64(len(deleteSet))}, nil
	}

	// Acquire row locks for matching rows (blocks if rows are locked by another connection)
	if e.rowLockManager != nil && e.shouldAcquireRowLocks() {
		var matchIndices []int
		for i, row := range tbl.Rows {
			match := true
			if stmt.Where != nil {
				m, mErr := e.evalWhere(stmt.Where.Expr, row)
				if mErr != nil {
					return nil, mErr
				}
				match = m
			}
			if match {
				matchIndices = append(matchIndices, i)
			}
		}
		if len(matchIndices) > 0 {
			tbl.Unlock()
			lockErr := e.acquireRowLocksForRows(deleteDB, tableName, tbl.Def, tbl.Rows, matchIndices)
			tbl.Lock()
			if lockErr != nil {
				return nil, lockErr
			}
		}
	}

	newRows := make([]storage.Row, 0)
	var affected uint64
	for _, row := range tbl.Rows {
		match := true
		if stmt.Where != nil {
			m, err := e.evalWhere(stmt.Where.Expr, row)
			if err != nil {
				if bool(stmt.Ignore) {
					// DELETE IGNORE: suppress WHERE eval errors (e.g. subquery > 1 row), skip row
					e.addWarning("Warning", 1242, strings.TrimPrefix(err.Error(), "ERROR 1242 (21000): "))
					match = false
				} else {
					return nil, err
				}
			} else {
				match = m
			}
		}
		if match {
			// Fire BEFORE DELETE triggers
			tbl.Unlock()
			if err := e.fireTriggers(tableName, "BEFORE", "DELETE", nil, row); err != nil {
				tbl.Lock()
				return nil, err
			}
			tbl.Lock()

			// Enforce FOREIGN KEY constraints: check child rows referencing this parent row
			tbl.Unlock()
			if err := e.checkForeignKeyOnDelete(deleteDB, tableName, row); err != nil {
				tbl.Lock()
				if bool(stmt.Ignore) {
					// DELETE IGNORE: add warning, skip this row (don't delete it)
					errCode := 1451
					msg := strings.TrimPrefix(err.Error(), "ERROR 1451 (23000): ")
					e.addWarning("Warning", errCode, msg)
					newRows = append(newRows, row)
					continue
				}
				return nil, err
			}
			tbl.Lock()

			affected++

			// Fire AFTER DELETE triggers
			tbl.Unlock()
			if err := e.fireTriggers(tableName, "AFTER", "DELETE", nil, row); err != nil {
				tbl.Lock()
				return nil, err
			}
			tbl.Lock()
		} else {
			newRows = append(newRows, row)
		}
	}
	tbl.Rows = newRows
	tbl.InvalidateIndexes()

	return &Result{AffectedRows: affected}, nil
}

func numericOrderColumnSet(def *catalog.TableDef, colNames []string) map[int]bool {
	if def == nil || len(colNames) == 0 {
		return nil
	}
	typeByName := make(map[string]string, len(def.Columns))
	for _, col := range def.Columns {
		typeByName[strings.ToLower(col.Name)] = col.Type
	}
	result := make(map[int]bool)
	for idx, name := range colNames {
		if colType, ok := typeByName[strings.ToLower(name)]; ok && isNumericOrderColumnType(colType) {
			result[idx] = true
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// isNonSortableCharset returns true for character sets whose collation order
// cannot be approximated by Go's default byte-level string comparison.
func isNonSortableCharset(cs string) bool {
	switch strings.ToLower(cs) {
	case "sjis", "cp932", "ujis", "eucjpms", "euckr", "gb2312", "gbk", "gb18030", "big5":
		return true
	}
	return false
}

// isFloatOrderColumnType returns true for floating-point and decimal column types whose values
// may be stored as formatted strings and need explicit numeric comparison in ORDER BY to avoid
// incorrect lexicographic ordering. For example, "5.00" < "11.11" lexicographically but
// 5.00 < 11.11 numerically.
// Note: NUMERIC is normalized to DECIMAL in DDL, so "decimal" covers both NUMERIC and DECIMAL.
func isFloatOrderColumnType(colType string) bool {
	t := strings.ToLower(strings.TrimSpace(colType))
	t = strings.TrimSuffix(t, " unsigned")
	t = strings.TrimSpace(t)
	if i := strings.IndexByte(t, '('); i >= 0 {
		t = strings.TrimSpace(t[:i])
	}
	switch t {
	case "double", "real", "numeric", "decimal":
		return true
	default:
		return false
	}
}

func isNumericOrderColumnType(colType string) bool {
	t := strings.ToLower(strings.TrimSpace(colType))
	t = strings.TrimSuffix(t, " unsigned")
	t = strings.TrimSpace(t)
	if i := strings.IndexByte(t, '('); i >= 0 {
		t = strings.TrimSpace(t[:i])
	}
	switch t {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint",
		"decimal", "numeric", "float", "double", "real", "year", "bit":
		return true
	default:
		return false
	}
}

func numericTypeRange(colType string) (maxAbs float64, isUnsigned bool, ok bool) {
	s := strings.ToLower(strings.TrimSpace(colType))
	isUnsigned = strings.Contains(s, "unsigned")
	if fields := strings.Fields(s); len(fields) > 0 {
		s = fields[0]
	}
	base := s
	if i := strings.IndexByte(base, '('); i >= 0 {
		base = base[:i]
	}
	var m, d int
	switch base {
	case "decimal", "numeric":
		if n, err := fmt.Sscanf(s, base+"(%d,%d)", &m, &d); !(err == nil && n == 2) {
			if n2, err2 := fmt.Sscanf(s, base+"(%d)", &m); err2 == nil && n2 == 1 {
				d = 0
			} else if s == base {
				m, d = 10, 0
			} else {
				return 0, isUnsigned, false
			}
		}
	case "float", "double", "real":
		if n, err := fmt.Sscanf(s, base+"(%d,%d)", &m, &d); !(err == nil && n == 2) {
			if n2, err2 := fmt.Sscanf(s, base+"(%d)", &m); err2 == nil && n2 == 1 {
				d = 0
			} else {
				// Bare FLOAT/DOUBLE/REAL has no M,D clipping in this engine path.
				return 0, isUnsigned, false
			}
		}
	default:
		return 0, isUnsigned, false
	}
	intDigits := m - d
	if intDigits <= 0 {
		intDigits = 1
	}
	maxIntPart := 1.0
	for i := 0; i < intDigits; i++ {
		maxIntPart *= 10
	}
	maxFrac := 1.0
	for i := 0; i < d; i++ {
		maxFrac *= 10
	}
	maxVal := maxIntPart - 1.0/maxFrac
	if d == 0 {
		maxVal = maxIntPart - 1
	}
	return maxVal, isUnsigned, true
}

func parseNumericPrefixMySQL(s string) (float64, bool) {
	// MySQL-style prefix parse: consume leading numeric token, ignore trailing junk.
	trimmed := strings.TrimLeft(s, " \t\r\n")
	re := regexp.MustCompile(`^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?`)
	token := re.FindString(trimmed)
	if token == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(token, 64)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			if strings.HasPrefix(token, "-") {
				return math.Inf(-1), true
			}
			return math.Inf(1), true
		}
		return 0, false
	}
	return f, true
}

// execMultiTableDeleteAST handles multi-table DELETE statements parsed by vitess.
func (e *Executor) execMultiTableDeleteAST(stmt *sqlparser.Delete) (*Result, error) {
	// Build rows from all source tables (FROM clause = TableExprs)
	// Start with first table
	if len(stmt.TableExprs) == 0 {
		return &Result{}, nil
	}

	// When there is no current database, any unqualified target reference → "No database selected".
	// MySQL returns ER_NO_DB_ERROR (3D000) before doing any other validation.
	if e.CurrentDB == "" {
		for _, target := range stmt.Targets {
			if target.Qualifier.IsEmpty() {
				return nil, mysqlError(1046, "3D000", "No database selected")
			}
		}
		// Also check FROM tables: if any unqualified table ref uses no qualifier, no DB selected.
		hasUnqualifiedFrom := false
		var checkTableExprs func(te sqlparser.TableExpr)
		checkTableExprs = func(te sqlparser.TableExpr) {
			switch t := te.(type) {
			case *sqlparser.AliasedTableExpr:
				if tn, ok := t.Expr.(sqlparser.TableName); ok {
					if tn.Qualifier.IsEmpty() {
						hasUnqualifiedFrom = true
					}
				}
			case *sqlparser.JoinTableExpr:
				checkTableExprs(t.LeftExpr)
				checkTableExprs(t.RightExpr)
			case *sqlparser.ParenTableExpr:
				for _, inner := range t.Exprs {
					checkTableExprs(inner)
				}
			}
		}
		for _, te := range stmt.TableExprs {
			checkTableExprs(te)
		}
		if hasUnqualifiedFrom {
			return nil, mysqlError(1046, "3D000", "No database selected")
		}
	}

	// SQL_SAFE_UPDATES: for multi-table DELETE, check if safe update mode requires a key-based WHERE.
	// MySQL allows multi-table DELETE in safe update mode only if there's a LIMIT or a WHERE using a key.
	if e.isSafeUpdateEnabled() && stmt.Limit == nil {
		// Check if WHERE uses a key column from any of the deleted tables.
		safeOK := false
		var whereExpr sqlparser.Expr
		if stmt.Where != nil {
			whereExpr = stmt.Where.Expr
		}
		// Look through target tables (stmt.Targets, or the first table in stmt.TableExprs if no targets).
		targetTables := stmt.Targets
		if len(targetTables) == 0 && len(stmt.TableExprs) > 0 {
			// Single-table style: first TableExpr is the target.
			alias, _, _ := extractTableAlias(stmt.TableExprs[0])
			targetTables = sqlparser.TableNames{sqlparser.TableName{Name: sqlparser.NewIdentifierCS(alias)}}
		}
		for _, te := range stmt.TableExprs {
			_, tableName, _ := extractTableAlias(te)
			dbName := e.CurrentDB
			if dbCat, err := e.Catalog.GetDatabase(dbName); err == nil {
				if tblDef, err2 := dbCat.GetTable(tableName); err2 == nil {
					if whereExpr != nil && whereUsesKeyColumnDirectly(whereExpr, func() map[string]bool {
						kc := make(map[string]bool)
						for _, col := range tblDef.PrimaryKey {
							kc[strings.ToLower(stripPrefixLengthFromCol(col))] = true
						}
						for _, idx := range tblDef.Indexes {
							for _, col := range idx.Columns {
								kc[strings.ToLower(stripPrefixLengthFromCol(col))] = true
							}
						}
						return kc
					}()) {
						safeOK = true
						break
					}
				}
			}
		}
		if !safeOK {
			return nil, mysqlError(1175, "HY000", "You are using safe update mode and you tried to update a table without a WHERE that uses a KEY column.")
		}
	}

	// Collect table aliases for per-table predicate classification.
	// Also build a flat list of all table info for target validation.
	type tableExprInfo struct {
		name           string
		explicitAlias  string // empty if no explicit alias
		db             string
		effectiveAlias string // alias if aliased, else name (table name only, no db prefix)
		rowAlias       string // alias used as row key prefix by buildFromExpr (matches extractTableAliasFromAliased)
	}
	// collectAllTableInfos recursively extracts all leaf AliasedTableExprs from a TableExpr.
	var collectAllTableInfos func(te sqlparser.TableExpr) []tableExprInfo
	collectAllTableInfos = func(te sqlparser.TableExpr) []tableExprInfo {
		switch t := te.(type) {
		case *sqlparser.AliasedTableExpr:
			dbName := e.CurrentDB
			tblName := ""
			hasDBQualifier := false
			if tn, ok2 := t.Expr.(sqlparser.TableName); ok2 {
				tblName = tn.Name.String()
				if !tn.Qualifier.IsEmpty() {
					dbName = tn.Qualifier.String()
					hasDBQualifier = true
				}
			}
			explAlias := ""
			if !t.As.IsEmpty() {
				explAlias = t.As.String()
			}
			effAlias := tblName
			if explAlias != "" {
				effAlias = explAlias
			}
			// rowAlias is what buildFromExpr uses as the key prefix in rows.
			// extractTableAliasFromAliased returns "db.table" when there's a DB qualifier and no AS.
			rowAlias := effAlias
			if explAlias == "" && hasDBQualifier {
				rowAlias = dbName + "." + tblName
			}
			return []tableExprInfo{{name: tblName, explicitAlias: explAlias, db: dbName, effectiveAlias: effAlias, rowAlias: rowAlias}}
		case *sqlparser.JoinTableExpr:
			left := collectAllTableInfos(t.LeftExpr)
			right := collectAllTableInfos(t.RightExpr)
			return append(left, right...)
		case *sqlparser.ParenTableExpr:
			var result []tableExprInfo
			for _, inner := range t.Exprs {
				result = append(result, collectAllTableInfos(inner)...)
			}
			return result
		}
		return nil
	}

	var tableAliases []string
	var allTableExprInfos []tableExprInfo
	for _, te := range stmt.TableExprs {
		alias, _, _ := extractTableAlias(te)
		tableAliases = append(tableAliases, alias)
		allTableExprInfos = append(allTableExprInfos, collectAllTableInfos(te)...)
	}

	// Check for duplicate table aliases/names in the FROM clause.
	// MySQL returns ER_NONUNIQ_TABLE (1066, 42000): "Not unique table/alias: 'X'".
	// MySQL fires this error when:
	// - Two entries share the same explicit alias (both have AS <same>)
	// - An unqualified table name (no db prefix, no explicit alias) duplicates another alias
	// When a cross-DB qualified table (e.g. db1.a1) "conflicts" with an explicit alias,
	// MySQL instead lets the table-not-found error take priority.
	{
		type aliasEntry struct {
			explicitAlias string
			hasDBQualifier bool // true if table has a non-current-db qualifier AND no explicit alias
		}
		seenAliases := make(map[string]aliasEntry)
		for _, info := range allTableExprInfos {
			key := strings.ToLower(info.effectiveAlias)
			isQualifiedNoAlias := info.explicitAlias == "" && info.db != "" && !strings.EqualFold(info.db, e.CurrentDB)
			entry := aliasEntry{
				explicitAlias:  info.explicitAlias,
				hasDBQualifier: isQualifiedNoAlias,
			}
			if prev, seen := seenAliases[key]; seen {
				// Fire "Not unique table/alias" unless BOTH entries are cross-DB qualified tables
				// with no explicit aliases (in that case, table-not-found takes priority).
				// Also skip if one entry is a cross-DB qualified table-name-only (no explicit alias)
				// and the other has an explicit alias — MySQL lets table-not-found win.
				prevQualifiedNoAlias := prev.hasDBQualifier && prev.explicitAlias == ""
				curQualifiedNoAlias := entry.hasDBQualifier && entry.explicitAlias == ""
				if !prevQualifiedNoAlias && !curQualifiedNoAlias {
					return nil, mysqlError(1066, "42000",
						fmt.Sprintf("Not unique table/alias: '%s'", info.effectiveAlias))
				}
				// Both are qualified-no-alias: skip dup check, let table-not-found win
			} else {
				seenAliases[key] = entry
			}
		}
	}

	// Check if any target is a non-updatable view (e.g. has LIMIT, GROUP BY, etc.).
	// MySQL returns ER_NON_UPDATABLE_TABLE (1288) in this case.
	for _, target := range stmt.Targets {
		targetName := target.Name.String()
		if _, _, isView := e.lookupView(targetName); isView {
			// It's a view - check if it's updatable
			_, _, _, resolveErr := e.resolveViewToBaseTable(targetName)
			if resolveErr != nil {
				return nil, mysqlError(1288, "HY000",
					fmt.Sprintf("The target table %s of the DELETE is not updatable", targetName))
			}
		}
	}

	// Validate target tables: each target must match a table expr by alias (if aliased) or by name (if not aliased).
	// MySQL error 1109 (ER_UNKNOWN_TABLE): "Unknown table 'T' in MULTI DELETE".
	if len(stmt.Targets) > 0 {
		for _, target := range stmt.Targets {
			targetName := target.Name.String()
			targetDB := e.CurrentDB
			if !target.Qualifier.IsEmpty() {
				targetDB = target.Qualifier.String()
			}
			found := false
			for _, info := range allTableExprInfos {
				if !target.Qualifier.IsEmpty() {
					// Qualified target (e.g. db2.alias): match by TABLE NAME only (not alias),
					// because the qualifier implies a real table reference.
					if strings.EqualFold(info.name, targetName) && strings.EqualFold(info.db, targetDB) {
						// When the table has an explicit alias, the target must use the alias (not the table name),
						// unless the table is NOT aliased.
						if info.explicitAlias == "" {
							found = true
							break
						}
						// Has explicit alias: qualified target by table name is invalid (must use alias)
						// e.g., "DELETE FROM db2.t1 USING db2.t1 AS alias ..." - must target as "alias"
					}
				} else if info.explicitAlias != "" {
					// Target has no qualifier, and table has explicit alias: match by alias only
					if strings.EqualFold(info.explicitAlias, targetName) {
						found = true
						break
					}
				} else {
					// No qualifier, no explicit alias: match by table name only if same DB as current
					if strings.EqualFold(info.name, targetName) && strings.EqualFold(info.db, e.CurrentDB) {
						found = true
						break
					}
				}
			}
			if !found {
				return nil, mysqlError(1109, "42S02",
					fmt.Sprintf("Unknown table '%s' in MULTI DELETE", targetName))
			}
		}
	}

	// Classify WHERE predicates into per-table-only predicates
	var perTablePreds [][]sqlparser.Expr
	var crossPreds []sqlparser.Expr
	if stmt.Where != nil {
		perTablePreds, crossPreds = classifyDeletePredsForTables(stmt.Where.Expr, tableAliases)
	} else {
		perTablePreds = make([][]sqlparser.Expr, len(tableAliases))
	}

	// Validate WHERE clause column references against available table columns.
	// This ensures unknown columns are caught even when tables are empty.
	if stmt.Where != nil {
		availCols := make(map[string]bool)
		for _, te := range stmt.TableExprs {
			alias, tableName, _ := extractTableAlias(te)
			dbName := e.CurrentDB
			if dbCat, err2 := e.Catalog.GetDatabase(dbName); err2 == nil {
				if tblCat, err3 := dbCat.GetTable(tableName); err3 == nil {
					for _, col := range tblCat.Columns {
						availCols[strings.ToLower(col.Name)] = true
						// Also add alias-qualified form
						availCols[strings.ToLower(alias+"."+col.Name)] = true
					}
				}
			}
		}
		if len(availCols) > 0 {
			walkErr := sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
				switch node.(type) {
				case *sqlparser.Subquery, *sqlparser.Select, *sqlparser.Union:
					return false, nil
				}
				if colName, ok := node.(*sqlparser.ColName); ok {
					if colName.Qualifier.IsEmpty() {
						cn := strings.ToLower(colName.Name.String())
						if !availCols[cn] {
							return false, mysqlError(1054, "42S22",
								fmt.Sprintf("Unknown column '%s' in 'where clause'", colName.Name.String()))
						}
					}
				}
				return true, nil
			}, stmt.Where.Expr)
			if walkErr != nil {
				return nil, walkErr
			}
		}
	}

	// Build rows from each table, pre-filtering with table-specific predicates
	allTableRows := make([][]storage.Row, len(stmt.TableExprs))
	for i, te := range stmt.TableExprs {
		rows, err := e.buildFromExpr(te)
		if err != nil {
			return nil, err
		}
		// Apply per-table predicates to filter rows before cross-joining
		if len(perTablePreds[i]) > 0 {
			var pred sqlparser.Expr = perTablePreds[i][0]
			for j := 1; j < len(perTablePreds[i]); j++ {
				pred = &sqlparser.AndExpr{Left: pred, Right: perTablePreds[i][j]}
			}
			filtered := make([]storage.Row, 0)
			for _, row := range rows {
				match, err := e.evalWhere(pred, row)
				if err != nil {
					continue
				}
				if match {
					filtered = append(filtered, row)
				}
			}
			rows = filtered
		}
		allTableRows[i] = rows
	}

	// Cross join the pre-filtered tables
	allRows := allTableRows[0]
	for i := 1; i < len(allTableRows); i++ {
		allRows = crossProduct(allRows, allTableRows[i])
	}

	// Apply remaining cross-table WHERE predicates
	if len(crossPreds) > 0 {
		var crossPred sqlparser.Expr = crossPreds[0]
		for j := 1; j < len(crossPreds); j++ {
			crossPred = &sqlparser.AndExpr{Left: crossPred, Right: crossPreds[j]}
		}
		// Even when there are no rows (e.g. INNER JOIN produces 0 rows), MySQL still
		// evaluates non-correlated scalar subqueries in the WHERE clause and raises
		// errors such as ER_SUBQUERY_NO_1_ROW (1242). We simulate this by evaluating
		// the predicate once on an empty dummy row when allRows is empty. Column
		// references in the predicate will return NULL (harmless), but subquery errors
		// will propagate correctly.
		if len(allRows) == 0 && !bool(stmt.Ignore) {
			_, evalErr := e.evalWhere(crossPred, storage.Row{})
			if evalErr != nil {
				return nil, evalErr
			}
		}
		filtered := make([]storage.Row, 0)
		for _, row := range allRows {
			match, err := e.evalWhere(crossPred, row)
			if err != nil {
				if bool(stmt.Ignore) {
					// DELETE IGNORE: suppress WHERE eval errors (e.g. subquery > 1 row), skip row
					e.addWarning("Warning", 1242, strings.TrimPrefix(err.Error(), "ERROR 1242 (21000): "))
					continue
				}
				return nil, err
			}
			if match {
				filtered = append(filtered, row)
			}
		}
		allRows = filtered
	}

	// Delete matched rows from target tables.
	// Two-phase approach:
	//   Phase 1: Collect all rows to delete and fire BEFORE triggers.
	//             If any BEFORE trigger fails, abort (MySQL semantics: whole statement rolls back).
	//   Phase 2: Do the actual deletions, then fire AFTER triggers.
	type deleteEntry struct {
		tbl          *storage.Table
		resolvedName string
		resolvedDB   string
		targetAlias  string
		targetName   string
		indices      []int
		rows         []storage.Row
	}
	var deleteEntries []deleteEntry

	// Collect what to delete
	for _, target := range stmt.Targets {
		targetName := target.Name.String()
		targetDB := e.CurrentDB
		if !target.Qualifier.IsEmpty() {
			targetDB = target.Qualifier.String()
		}

		// Resolve the target to the actual table name/db via allTableExprInfos.
		// The target might be an alias (e.g. "t1" which is "db1.t2 AS t1"),
		// so we must look up by effectiveAlias to get the real table and DB.
		// Also extract rowAlias: the key prefix used in row maps by buildFromExpr.
		resolvedName := targetName
		resolvedDB := targetDB
		resolvedRowAlias := targetName // the alias used as row key prefix by buildFromExpr
		for _, info := range allTableExprInfos {
			if strings.EqualFold(info.effectiveAlias, targetName) {
				resolvedName = info.name
				resolvedDB = info.db
				if resolvedDB == "" {
					resolvedDB = e.CurrentDB
				}
				resolvedRowAlias = info.rowAlias
				break
			}
		}

		tbl, err := e.Storage.GetTable(resolvedDB, resolvedName)
		if err != nil {
			continue
		}
		targetAlias := resolvedRowAlias
		deleteIndicesMap := make(map[int]bool)
		for _, matchedRow := range allRows {
			if idx := matchRowToTable(matchedRow, tbl, targetAlias, targetName); idx >= 0 && !deleteIndicesMap[idx] {
				deleteIndicesMap[idx] = true
			}
		}
		if len(deleteIndicesMap) == 0 {
			continue
		}
		sortedIdx := make([]int, 0, len(deleteIndicesMap))
		for idx := range deleteIndicesMap {
			sortedIdx = append(sortedIdx, idx)
		}
		sort.Ints(sortedIdx)
		rows := make([]storage.Row, len(sortedIdx))
		for i, idx := range sortedIdx {
			rows[i] = tbl.Rows[idx]
		}
		deleteEntries = append(deleteEntries, deleteEntry{
			tbl:          tbl,
			resolvedName: resolvedName,
			resolvedDB:   resolvedDB,
			targetAlias:  targetAlias,
			targetName:   targetName,
			indices:      sortedIdx,
			rows:         rows,
		})
	}

	// Phase 1: Fire all BEFORE DELETE triggers and check FK constraints.
	// If any fail, abort before deleting anything.
	// For IGNORE mode: skip FK-violated rows rather than aborting.
	var finalEntries []deleteEntry
	for _, de := range deleteEntries {
		var validIndices []int
		var validRows []storage.Row
		for i, row := range de.rows {
			// Fire BEFORE DELETE trigger.
			if err := e.fireTriggers(de.resolvedName, "BEFORE", "DELETE", nil, row); err != nil {
				return nil, err
			}
			// Check FK constraints.
			if fkErr := e.checkForeignKeyOnDelete(de.resolvedDB, de.resolvedName, row); fkErr != nil {
				if bool(stmt.Ignore) {
					// IGNORE: skip this row, add a warning.
					errCode := 1451
					msg := strings.TrimPrefix(fkErr.Error(), "ERROR 1451 (23000): ")
					e.addWarning("Warning", errCode, msg)
					continue
				}
				return nil, fkErr
			}
			validIndices = append(validIndices, de.indices[i])
			validRows = append(validRows, row)
		}
		if len(validIndices) > 0 {
			de.indices = validIndices
			de.rows = validRows
			finalEntries = append(finalEntries, de)
		}
	}
	deleteEntries = finalEntries

	// Phase 2: Do all deletions and fire AFTER DELETE triggers.
	// Save original rows for rollback if an AFTER trigger fails.
	type tableSnapshot struct {
		tbl      *storage.Table
		origRows []storage.Row
	}
	var snapshots []tableSnapshot

	var totalAffected uint64
	for _, de := range deleteEntries {
		deleteIndicesSet := make(map[int]bool, len(de.indices))
		for _, idx := range de.indices {
			deleteIndicesSet[idx] = true
		}
		de.tbl.Lock()
		// Save original rows before deletion.
		origRows := make([]storage.Row, len(de.tbl.Rows))
		copy(origRows, de.tbl.Rows)
		snapshots = append(snapshots, tableSnapshot{tbl: de.tbl, origRows: origRows})

		newRows := make([]storage.Row, 0, len(de.tbl.Rows)-len(de.indices))
		for i, row := range de.tbl.Rows {
			if !deleteIndicesSet[i] {
				newRows = append(newRows, row)
			}
		}
		de.tbl.Rows = newRows
		de.tbl.InvalidateIndexes()
		de.tbl.Unlock()
		totalAffected += uint64(len(de.indices))

		// Fire AFTER DELETE triggers. On failure, roll back all deletions.
		for _, row := range de.rows {
			if err := e.fireTriggers(de.resolvedName, "AFTER", "DELETE", nil, row); err != nil {
				// Rollback: restore all modified tables to their original rows.
				for _, snap := range snapshots {
					snap.tbl.Lock()
					snap.tbl.Rows = snap.origRows
					snap.tbl.InvalidateIndexes()
					snap.tbl.Unlock()
				}
				return nil, err
			}
		}
	}

	return &Result{AffectedRows: totalAffected}, nil
}

// findTopLevelWhereIndex returns the index of the top-level WHERE keyword
// (not inside parentheses) in the given string, or -1 if not found.
// It matches WHERE preceded by whitespace or start-of-string.
func findTopLevelWhereIndex(s string) int {
	upper := strings.ToUpper(s)
	depth := 0
	for i := 0; i < len(upper); i++ {
		switch upper[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		default:
			if depth == 0 && i+6 <= len(upper) && upper[i:i+5] == "WHERE" {
				// Check that WHERE is preceded by whitespace or is at start
				if i == 0 || upper[i-1] == ' ' || upper[i-1] == '\n' || upper[i-1] == '\t' || upper[i-1] == '\r' {
					// Check that WHERE is followed by whitespace
					if i+5 < len(upper) && (upper[i+5] == ' ' || upper[i+5] == '\n' || upper[i+5] == '\t') {
						return i
					}
				}
			}
		}
	}
	return -1
}

// execMultiTableDelete handles multi-table DELETE statements:
// Syntax 1: DELETE t1,t2 FROM t1,t2,t3 WHERE ...
// Syntax 2: DELETE FROM t1,t2 USING t1,t2,t3 WHERE ...
// Supports: QUICK/LOW_PRIORITY/IGNORE modifiers, t1.* syntax, db.table syntax
func (e *Executor) execMultiTableDelete(query string) (*Result, error) {
	upper := strings.ToUpper(strings.TrimSpace(query))
	// Normalize whitespace (replace tabs/newlines with spaces) for easier parsing
	normalizedQuery := strings.Join(strings.Fields(strings.TrimSpace(query)), " ")
	rest := strings.TrimSpace(normalizedQuery[len("DELETE "):])
	restUpper := strings.ToUpper(rest)

	// Strip modifiers: LOW_PRIORITY, QUICK, IGNORE
	for _, mod := range []string{"LOW_PRIORITY ", "QUICK ", "IGNORE "} {
		for strings.HasPrefix(restUpper, mod) {
			rest = strings.TrimSpace(rest[len(mod):])
			restUpper = strings.ToUpper(rest)
		}
	}

	var deleteTargets []string
	var fromTablesStr string
	var whereClause string

	// Detect syntax: "FROM ... USING ..." vs "targets FROM tables WHERE ..."
	if strings.HasPrefix(restUpper, "FROM ") {
		// Syntax 2: DELETE [mods] FROM target_tables USING source_tables WHERE ...
		rest = strings.TrimSpace(rest[len("FROM "):])
		restUpper = strings.ToUpper(rest)
		usingIdx := strings.Index(restUpper, " USING ")
		if usingIdx < 0 {
			return nil, fmt.Errorf("invalid multi-table DELETE syntax: missing USING")
		}
		targetsStr := strings.TrimSpace(rest[:usingIdx])
		afterUsing := strings.TrimSpace(rest[usingIdx+len(" USING "):])
		for _, t := range strings.Split(targetsStr, ",") {
			t = strings.TrimSpace(t)
			t = strings.Trim(t, "`")
			t = strings.TrimSuffix(t, ".*")
			if t != "" {
				deleteTargets = append(deleteTargets, t)
			}
		}
		if whereIdx := findTopLevelWhereIndex(afterUsing); whereIdx >= 0 {
			whereClause = strings.TrimSpace(afterUsing[whereIdx+len("WHERE "):])
			whereClause = strings.TrimSuffix(whereClause, ";")
			fromTablesStr = strings.TrimSpace(afterUsing[:whereIdx])
		} else {
			fromTablesStr = strings.TrimSuffix(strings.TrimSpace(afterUsing), ";")
		}
	} else {
		// Syntax 1: DELETE target_tables FROM source_tables WHERE ...
		_ = upper
		fromIdx := strings.Index(restUpper, " FROM ")
		if fromIdx < 0 {
			return nil, fmt.Errorf("invalid multi-table DELETE syntax: missing FROM")
		}
		targetsStr := strings.TrimSpace(rest[:fromIdx])
		afterFrom := strings.TrimSpace(rest[fromIdx+len(" FROM "):])
		for _, t := range strings.Split(targetsStr, ",") {
			t = strings.TrimSpace(t)
			t = strings.Trim(t, "`")
			t = strings.TrimSuffix(t, ".*")
			if t != "" {
				deleteTargets = append(deleteTargets, t)
			}
		}
		if whereIdx := findTopLevelWhereIndex(afterFrom); whereIdx >= 0 {
			whereClause = strings.TrimSpace(afterFrom[whereIdx+len("WHERE "):])
			whereClause = strings.TrimSuffix(whereClause, ";")
			fromTablesStr = strings.TrimSpace(afterFrom[:whereIdx])
		} else {
			fromTablesStr = strings.TrimSuffix(strings.TrimSpace(afterFrom), ";")
		}
	}

	// Resolve qualified target names (db.table -> use the table name part for matching)
	// But keep track of db for each target
	deleteTargetDBs := make(map[string]string) // table name -> db name
	for i, t := range deleteTargets {
		if parts := strings.Split(t, "."); len(parts) == 2 {
			deleteTargetDBs[parts[1]] = parts[0]
			deleteTargets[i] = parts[1] // use table name for matching
		} else if len(parts) > 2 {
			// db.table.* -> take second to last as table
			deleteTargetDBs[parts[len(parts)-2]] = parts[0]
			deleteTargets[i] = parts[len(parts)-2]
		}
	}
	_ = deleteTargetDBs

	// Parse table refs and WHERE using vitess by constructing a SELECT statement.
	// This handles JOINs, subqueries, and complex WHERE clauses correctly.
	var tableRefs []deleteTableRef

	selectSQL := "SELECT 1 FROM " + fromTablesStr
	if whereClause != "" {
		selectSQL += " WHERE " + whereClause
	}
	parsedStmt, err := e.parser().Parse(selectSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse WHERE clause: %v", err)
	}
	sel, ok := parsedStmt.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("failed to parse WHERE clause")
	}

	// Extract table refs from parsed FROM clause
	var extractTableExprs func(te sqlparser.TableExpr)
	extractTableExprs = func(te sqlparser.TableExpr) {
		switch t := te.(type) {
		case *sqlparser.AliasedTableExpr:
			tn, ok := t.Expr.(sqlparser.TableName)
			if !ok {
				return
			}
			name := tn.Name.String()
			alias := name
			db := e.CurrentDB
			if !tn.Qualifier.IsEmpty() {
				db = tn.Qualifier.String()
			}
			if !t.As.IsEmpty() {
				alias = t.As.String()
			}
			tableRefs = append(tableRefs, deleteTableRef{name: name, alias: alias, db: db})
		case *sqlparser.JoinTableExpr:
			extractTableExprs(t.LeftExpr)
			extractTableExprs(t.RightExpr)
		case *sqlparser.ParenTableExpr:
			for _, expr := range t.Exprs {
				extractTableExprs(expr)
			}
		}
	}
	for _, te := range sel.From {
		extractTableExprs(te)
	}

	if len(tableRefs) == 0 {
		return &Result{}, nil
	}

	// Validate WHERE clause column references against available table columns.
	// This catches unknown columns even when the tables are empty.
	if sel.Where != nil {
		availCols := make(map[string]bool)
		for _, ref := range tableRefs {
			db := ref.db
			if db == "" {
				db = e.CurrentDB
			}
			if tblDef, err2 := e.Storage.GetTable(db, ref.name); err2 == nil {
				for _, row := range tblDef.Rows {
					for k := range row {
						availCols[strings.ToLower(k)] = true
					}
					break
				}
			}
			// Also use catalog for column definitions
			if dbCat, err2 := e.Catalog.GetDatabase(db); err2 == nil {
				if tblCat, err3 := dbCat.GetTable(ref.name); err3 == nil {
					for _, col := range tblCat.Columns {
						availCols[strings.ToLower(col.Name)] = true
					}
				}
			}
		}
		walkErr := sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
			switch node.(type) {
			case *sqlparser.Subquery, *sqlparser.Select, *sqlparser.Union:
				return false, nil
			}
			if colName, ok := node.(*sqlparser.ColName); ok {
				if colName.Qualifier.IsEmpty() {
					cn := strings.ToLower(colName.Name.String())
					if !availCols[cn] {
						return false, mysqlError(1054, "42S22",
							fmt.Sprintf("Unknown column '%s' in 'where clause'", colName.Name.String()))
					}
				}
			}
			return true, nil
		}, sel.Where.Expr)
		if walkErr != nil {
			return nil, walkErr
		}
	}

	// Build joined rows from the FROM clause, handling JOINs.
	var allRows []storage.Row
	var buildErr error
	allRows, buildErr = e.buildDeleteFromRows(sel.From, tableRefs)
	if buildErr != nil {
		return nil, buildErr
	}

	// Apply WHERE filter
	if sel.Where != nil {
		filtered := make([]storage.Row, 0)
		for _, row := range allRows {
			match, mErr := e.evalWhere(sel.Where.Expr, row)
			if mErr != nil {
				return nil, mErr
			}
			if match {
				filtered = append(filtered, row)
			}
		}
		allRows = filtered
	}

	// Delete matched rows from target tables
	var totalAffected uint64
	for _, target := range deleteTargets {
		// Find the matching table ref (to get the right db and alias)
		targetDB := e.CurrentDB
		targetAlias := target
		if dbOverride, ok := deleteTargetDBs[target]; ok {
			targetDB = dbOverride
		}
		for _, ref := range tableRefs {
			if ref.name == target {
				targetDB = ref.db
				targetAlias = ref.alias
				break
			}
		}
		tbl, err := e.Storage.GetTable(targetDB, target)
		if err != nil {
			continue
		}
		deleteIndices := make(map[int]bool)
		for _, matchedRow := range allRows {
			if idx := matchRowToTable(matchedRow, tbl, targetAlias, target); idx >= 0 && !deleteIndices[idx] {
				deleteIndices[idx] = true
			}
		}
		if len(deleteIndices) > 0 {
			tbl.Lock()
			newRows := make([]storage.Row, 0, len(tbl.Rows)-len(deleteIndices))
			for i, row := range tbl.Rows {
				if !deleteIndices[i] {
					newRows = append(newRows, row)
				}
			}
			tbl.Rows = newRows
			tbl.InvalidateIndexes()
			tbl.Unlock()
			totalAffected += uint64(len(deleteIndices))
		}
	}

	return &Result{AffectedRows: totalAffected}, nil
}

func (e *Executor) getTableRowsWithAliasDB(dbName, tableName, alias string) ([]storage.Row, error) {
	tbl, err := e.Storage.GetTable(dbName, tableName)
	if err != nil {
		return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", dbName, tableName))
	}
	raw := tbl.Scan()
	// Evaluate virtual generated columns on read
	hasVirtual := false
	if tbl.Def != nil {
		for _, col := range tbl.Def.Columns {
			if genExpr := generatedColumnExpr(col.Type); genExpr != "" {
				colUpper := strings.ToUpper(col.Type)
				if !strings.Contains(colUpper, "STORED") {
					hasVirtual = true
					break
				}
			}
		}
	}
	if hasVirtual {
		for _, row := range raw {
			e.populateGeneratedColumns(row, tbl.Def.Columns)
		}
	}
	result := make([]storage.Row, len(raw))
	for i, row := range raw {
		newRow := make(storage.Row, len(row)*2)
		for k, v := range row {
			newRow[k] = v
			newRow[alias+"."+k] = v
		}
		result[i] = newRow
	}
	return result, nil
}

type deleteTableRef struct {
	name  string
	alias string
	db    string
}

// buildDeleteFromRows resolves the FROM clause of a multi-table DELETE into
// joined storage.Row slices, handling JOINs and cross products while preserving
// per-table column identity (alias.col keys) needed for row deletion.
func (e *Executor) buildDeleteFromRows(from sqlparser.TableExprs, tableRefs []deleteTableRef) ([]storage.Row, error) {
	if len(from) == 0 {
		return nil, nil
	}
	rows, err := e.buildDeleteTableExprRows(from[0], tableRefs)
	if err != nil {
		return nil, err
	}
	for i := 1; i < len(from); i++ {
		right, err := e.buildDeleteTableExprRows(from[i], tableRefs)
		if err != nil {
			return nil, err
		}
		rows = crossProduct(rows, right)
	}
	return rows, nil
}

func (e *Executor) buildDeleteTableExprRows(te sqlparser.TableExpr, tableRefs []deleteTableRef) ([]storage.Row, error) {
	switch t := te.(type) {
	case *sqlparser.AliasedTableExpr:
		tn, ok := t.Expr.(sqlparser.TableName)
		if !ok {
			return nil, fmt.Errorf("unsupported table expression in multi-table DELETE")
		}
		name := tn.Name.String()
		db := e.CurrentDB
		alias := name
		if !tn.Qualifier.IsEmpty() {
			db = tn.Qualifier.String()
		}
		if !t.As.IsEmpty() {
			alias = t.As.String()
		}
		return e.getTableRowsWithAliasDB(db, name, alias)
	case *sqlparser.JoinTableExpr:
		leftRows, err := e.buildDeleteTableExprRows(t.LeftExpr, tableRefs)
		if err != nil {
			return nil, err
		}
		rightRows, err := e.buildDeleteTableExprRows(t.RightExpr, tableRefs)
		if err != nil {
			return nil, err
		}
		isLeft := t.Join == sqlparser.LeftJoinType || t.Join == sqlparser.NaturalLeftJoinType
		var result []storage.Row
		for _, lRow := range leftRows {
			matched := false
			for _, rRow := range rightRows {
				combined := mergeDeleteRows(lRow, rRow)
				if t.Condition.On != nil {
					ok, err := e.evalWhere(t.Condition.On, combined)
					if err != nil {
						return nil, err
					}
					if !ok {
						continue
					}
				}
				matched = true
				result = append(result, combined)
			}
			if !matched && isLeft {
				// LEFT JOIN with no match: combine left row with NULLs for right
				nullRow := make(storage.Row)
				for k := range lRow {
					nullRow[k] = lRow[k]
				}
				// Add NULL entries for right table columns
				if len(rightRows) > 0 {
					for k := range rightRows[0] {
						nullRow[k] = nil
					}
				}
				result = append(result, nullRow)
			}
		}
		return result, nil
	case *sqlparser.ParenTableExpr:
		return e.buildDeleteFromRows(t.Exprs, tableRefs)
	default:
		return nil, fmt.Errorf("unsupported table expression type in multi-table DELETE")
	}
}

func mergeDeleteRows(left, right storage.Row) storage.Row {
	merged := make(storage.Row, len(left)+len(right))
	for k, v := range left {
		merged[k] = v
	}
	for k, v := range right {
		merged[k] = v
	}
	return merged
}

// matchRowToTable returns the index of the row in tbl.Rows that matches
// matchedRow by comparing column values using qualified (alias.col, tableName.col)
// or unqualified (col) key lookups. Returns -1 if no match is found.
// This is the strict variant used by DELETE: every column in tbl.Def.Columns
// must be found in matchedRow for a match.
func matchRowToTable(matchedRow storage.Row, tbl *storage.Table, alias, tableName string) int {
	for i, existingRow := range tbl.Rows {
		allMatch := true
		for _, col := range tbl.Def.Columns {
			mv, ok := matchedRow[alias+"."+col.Name]
			if !ok {
				mv, ok = matchedRow[tableName+"."+col.Name]
			}
			if !ok {
				mv, ok = matchedRow[col.Name]
			}
			if !ok {
				allMatch = false
				break
			}
			ev := existingRow[col.Name]
			if fmt.Sprintf("%v", mv) != fmt.Sprintf("%v", ev) {
				allMatch = false
				break
			}
		}
		if allMatch {
			return i
		}
	}
	return -1
}

// matchRowToTableLenient returns the index of the row in tbl.Rows that matches
// matchedRow using a lenient strategy: it iterates over the storage row's keys,
// skips columns not found in matchedRow, and requires at least one column to match.
// This preserves the original multi-table UPDATE matching behavior.
func matchRowToTableLenient(matchedRow storage.Row, tbl *storage.Table, alias, tableName string) int {
	// When the table has a primary key, match by PK columns only.
	// This is more robust when other columns may have been modified in a
	// previous step (e.g., multi-alias self-join updates).
	pkCols := tbl.Def.PrimaryKey
	if len(pkCols) == 0 {
		for _, col := range tbl.Def.Columns {
			if col.PrimaryKey {
				pkCols = append(pkCols, col.Name)
			}
		}
	}
	if len(pkCols) > 0 {
		// Build PK values from the matched (snapshot) row using the alias prefix.
		pkVals := make(map[string]string, len(pkCols))
		allPKFound := true
		for _, pk := range pkCols {
			qualKey := alias + "." + pk
			mv, ok := matchedRow[qualKey]
			if !ok {
				mv, ok = matchedRow[tableName+"."+pk]
			}
			if !ok {
				mv, ok = matchedRow[pk]
			}
			if !ok {
				allPKFound = false
				break
			}
			pkVals[pk] = fmt.Sprintf("%v", mv)
		}
		if allPKFound {
			for i, existingRow := range tbl.Rows {
				match := true
				for _, pk := range pkCols {
					if fmt.Sprintf("%v", existingRow[pk]) != pkVals[pk] {
						match = false
						break
					}
				}
				if match {
					return i
				}
			}
			return -1
		}
	}

	// Fallback: full-row match using alias-qualified keys.
	for i, existingRow := range tbl.Rows {
		isMatch := true
		matchedCols := 0
		for k, v := range existingRow {
			qualKey := alias + "." + k
			if mv, ok := matchedRow[qualKey]; ok {
				if fmt.Sprintf("%v", mv) != fmt.Sprintf("%v", v) {
					isMatch = false
					break
				}
				matchedCols++
			} else if mv, ok := matchedRow[tableName+"."+k]; ok {
				if fmt.Sprintf("%v", mv) != fmt.Sprintf("%v", v) {
					isMatch = false
					break
				}
				matchedCols++
			}
		}
		if isMatch && matchedCols > 0 {
			return i
		}
	}
	return -1
}

// maxCrossProductRows limits the result of a cross product to prevent
// memory exhaustion from multi-way Cartesian products.
const maxCrossProductRows = 100_000

func crossProduct(left, right []storage.Row) []storage.Row {
	estimated := len(left) * len(right)
	if estimated > maxCrossProductRows {
		// Allocate up to the limit
		estimated = maxCrossProductRows
	}
	result := make([]storage.Row, 0, estimated)
	for _, l := range left {
		for _, r := range right {
			if len(result) >= maxCrossProductRows {
				return result
			}
			combined := make(storage.Row, len(l)+len(r))
			for k, v := range l {
				combined[k] = v
			}
			for k, v := range r {
				combined[k] = v
			}
			result = append(result, combined)
		}
	}
	return result
}
