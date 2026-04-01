package executor

import (
	"errors"
	"fmt"
	"math"
	"regexp"
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
	if baseTable, isView, err := e.resolveViewToBaseTable(tableName); err != nil {
		return nil, err
	} else if isView {
		tableName = baseTable
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

	tbl.Lock()
	defer tbl.Unlock()

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

		// Build a list of candidate row indices that match WHERE.
		type indexedRow struct {
			idx int
			row storage.Row
		}
		var candidates []indexedRow
		for i, row := range tbl.Rows {
			match := true
			if stmt.Where != nil {
				m, wErr := e.evalWhere(stmt.Where.Expr, row)
				if wErr != nil {
					if bool(stmt.Ignore) {
						// DELETE IGNORE: suppress WHERE eval errors (e.g. subquery > 1 row), skip row
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
				candidates = append(candidates, indexedRow{idx: i, row: row})
			}
		}

		// Convert candidates to [][]interface{} for applyOrderBy / applyLimit.
		flatRows := make([][]interface{}, len(candidates))
		for i, c := range candidates {
			r := make([]interface{}, len(colNames))
			for j, cn := range colNames {
				r[j] = c.row[cn]
			}
			// Append original index as last element for tracking.
			r = append(r, c.idx)
			flatRows[i] = r
		}

		if stmt.OrderBy != nil {
			flatRows, err = applyOrderByWithTypeHints(stmt.OrderBy, colNames, flatRows, effectiveTableCollation(def), numericOrderCols)
			if err != nil {
				return nil, err
			}
		} else if stmt.Limit != nil && len(def.PrimaryKey) > 0 {
			// When LIMIT without ORDER BY, InnoDB scans in PRIMARY KEY order.
			// Build an ORDER BY clause from the primary key columns.
			var orderBy sqlparser.OrderBy
			for _, pkCol := range def.PrimaryKey {
				orderBy = append(orderBy, &sqlparser.Order{
					Expr:      &sqlparser.ColName{Name: sqlparser.NewIdentifierCI(pkCol)},
					Direction: sqlparser.AscOrder,
				})
			}
			flatRows, err = applyOrderByWithTypeHints(orderBy, colNames, flatRows, effectiveTableCollation(def), numericOrderCols)
			if err != nil {
				return nil, err
			}
		}
		if stmt.Limit != nil {
			flatRows, err = applyLimit(stmt.Limit, flatRows)
			if err != nil {
				return nil, err
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
		for idx := range deleteSet {
			tbl.Unlock()
			if fkErr := e.checkForeignKeyOnDelete(deleteDB, tableName, tbl.Rows[idx]); fkErr != nil {
				tbl.Lock()
				return nil, fkErr
			}
			tbl.Lock()
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

	// Collect table aliases for per-table predicate classification
	var tableAliases []string
	for _, te := range stmt.TableExprs {
		alias, _, _ := extractTableAlias(te)
		tableAliases = append(tableAliases, alias)
	}

	// Classify WHERE predicates into per-table-only predicates
	var perTablePreds [][]sqlparser.Expr
	var crossPreds []sqlparser.Expr
	if stmt.Where != nil {
		perTablePreds, crossPreds = classifyDeletePredsForTables(stmt.Where.Expr, tableAliases)
	} else {
		perTablePreds = make([][]sqlparser.Expr, len(tableAliases))
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

	// Delete matched rows from target tables
	var totalAffected uint64
	for _, target := range stmt.Targets {
		targetName := target.Name.String()
		targetDB := e.CurrentDB
		if !target.Qualifier.IsEmpty() {
			targetDB = target.Qualifier.String()
		}
		tbl, err := e.Storage.GetTable(targetDB, targetName)
		if err != nil {
			continue
		}
		// Build alias for qualified column lookup (e.g., "d1.t1")
		targetAlias := targetName
		if targetDB != e.CurrentDB {
			targetAlias = targetDB + "." + targetName
		}
		deleteIndices := make(map[int]bool)
		for _, matchedRow := range allRows {
			if idx := matchRowToTable(matchedRow, tbl, targetAlias, targetName); idx >= 0 && !deleteIndices[idx] {
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
	rest := strings.TrimSpace(query[len("DELETE "):])
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
const maxCrossProductRows = 10_000

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
