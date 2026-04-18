package executor

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
	"vitess.io/vitess/go/vt/sqlparser"
)

// checkGapLockForInsert checks if the INSERT's new row PK falls within a gap
// lock held by another connection. Returns true if the INSERT should be blocked.
// This simulates InnoDB gap locks in REPEATABLE READ for PK-based index scans.
func (e *Executor) checkGapLockForInsert(dbName, tableName string, stmt *sqlparser.Insert) bool {
	if e.rowLockManager == nil {
		return false
	}
	// Get the table definition for PK columns
	db, err := e.Catalog.GetDatabase(dbName)
	if err != nil {
		return false
	}
	def, err := db.GetTable(tableName)
	if err != nil || def == nil || len(def.PrimaryKey) == 0 {
		return false
	}

	// Only check single-column integer PKs for gap lock (common case)
	if len(def.PrimaryKey) != 1 {
		return false
	}
	pkCol := def.PrimaryKey[0]

	// Find the PK column index in the INSERT columns
	pkIdx := -1
	for i, col := range stmt.Columns {
		if strings.EqualFold(col.String(), pkCol) {
			pkIdx = i
			break
		}
	}
	// If no explicit column list, use column position from table def
	if pkIdx == -1 && len(stmt.Columns) == 0 {
		for i, col := range def.Columns {
			if strings.EqualFold(col.Name, pkCol) {
				pkIdx = i
				break
			}
		}
	}
	if pkIdx < 0 {
		return false
	}

	// Extract PK values from the INSERT VALUES
	valuesStmt, ok := stmt.Rows.(sqlparser.Values)
	if !ok || len(valuesStmt) == 0 {
		return false
	}
	for _, vt := range valuesStmt {
		if pkIdx >= len(vt) {
			continue
		}
		lit, ok := vt[pkIdx].(*sqlparser.Literal)
		if !ok {
			continue
		}
		insertPKVal, err := strconv.ParseInt(lit.Val, 10, 64)
		if err != nil {
			continue
		}

		// Get all locked PK values for this table from other connections
		prefix := dbName + ":" + tableName + ":"
		lockedPKs := e.rowLockManager.GetOtherLockedKeysWithPrefix(e.connectionID, prefix)
		if len(lockedPKs) == 0 {
			return false
		}

		// Parse locked PK values and check if the insert PK falls between them
		var lockedVals []int64
		for _, key := range lockedPKs {
			// Key format: "db:table:pkval"
			parts := strings.SplitN(key, ":", 3)
			if len(parts) < 3 {
				continue
			}
			v, err := strconv.ParseInt(parts[2], 10, 64)
			if err != nil {
				continue
			}
			lockedVals = append(lockedVals, v)
		}
		if len(lockedVals) < 2 {
			return false
		}

		// Sort locked values
		sort.Slice(lockedVals, func(i, j int) bool { return lockedVals[i] < lockedVals[j] })

		// Check if insertPKVal falls between any two consecutive locked values
		for i := 0; i < len(lockedVals)-1; i++ {
			if insertPKVal > lockedVals[i] && insertPKVal < lockedVals[i+1] {
				return true
			}
		}
	}
	return false
}

func (e *Executor) execInsert(stmt *sqlparser.Insert) (*Result, error) {
	// Set insideDML so that sub-SELECTs (INSERT...SELECT, subqueries in
	// VALUES) acquire row locks, mimicking InnoDB's implicit locking.
	// InnoDB always acquires shared locks on source rows during INSERT...SELECT,
	// even in autocommit mode.
	prevInsideInsert := e.insideDML
	if e.rowLockManager != nil {
		e.insideDML = true
	}
	defer func() {
		e.insideDML = prevInsideInsert
		// In autocommit mode (no explicit transaction), release row locks acquired
		// during this INSERT statement since it auto-commits immediately.
		if !e.shouldAcquireRowLocks() && e.rowLockManager != nil {
			e.rowLockManager.ReleaseRowLocks(e.connectionID)
		}
	}()

	tableName := stmt.Table.TableNameString()
	insertDB := e.CurrentDB
	if tn, ok := stmt.Table.Expr.(sqlparser.TableName); ok && !tn.Qualifier.IsEmpty() {
		insertDB = tn.Qualifier.String()
	} else if tn, ok := stmt.Table.Expr.(sqlparser.TableName); ok && tn.Qualifier.IsEmpty() {
		// Qualifier is explicitly empty - don't split on "." in table name
		tableName = tn.Name.String()
	} else if strings.Contains(tableName, ".") {
		insertDB, tableName = resolveTableNameDB(tableName, e.CurrentDB)
	}

	// Resolve views: if tableName is a view, replace with the underlying base table.
	// Also capture any WITH CHECK OPTION condition for enforcement.
	originalViewName := tableName
	var viewCheckExpr sqlparser.Expr
	if baseTable, isView, _, err := e.resolveViewToBaseTable(tableName); err != nil {
		return nil, err
	} else if isView {
		viewCheckExpr = e.getViewCheckCondition(originalViewName)
		tableName = baseTable
	}

	// MySQL error 1442: can't modify a table inside a trigger if the triggering statement is
	// already modifying that table.
	if e.functionOrTriggerDepth > 0 && strings.EqualFold(e.modifyingTable, tableName) {
		return nil, mysqlError(1442, "HY000",
			fmt.Sprintf("Can't update table '%s' in stored function/trigger because it is already used by statement which invoked this stored function/trigger.", tableName))
	}

	// Reject INSERT on information_schema tables.
	if strings.EqualFold(insertDB, "information_schema") {
		return nil, mysqlError(1044, "42000", fmt.Sprintf("Access denied for user 'root'@'localhost' to database 'information_schema'"))
	}

	// Reject INSERT on performance_schema tables (except writable setup tables)
	if strings.EqualFold(insertDB, "performance_schema") {
		lowerTable := strings.ToLower(tableName)
		if lowerTable != "setup_actors" && lowerTable != "setup_objects" {
			return nil, mysqlError(1142, "42000", fmt.Sprintf("INSERT command denied to user 'root'@'localhost' for table '%s'", tableName))
		}
		return e.execPerfSchemaInsert(stmt, lowerTable)
	}

	// Gap lock simulation: in REPEATABLE READ (or stricter), if another
	// connection holds row locks on this table, the INSERT may be blocked
	// by gap locks when the new row's PK falls between two locked PKs.
	if e.rowLockManager != nil && e.shouldAcquireRowLocks() {
		isoLevel, _ := e.getSysVar("transaction_isolation")
		if isoLevel == "" {
			isoLevel, _ = e.getSysVar("tx_isolation")
		}
		isReadCommitted := strings.EqualFold(isoLevel, "READ-COMMITTED") || strings.EqualFold(isoLevel, "READ COMMITTED")
		isReadUncommitted := strings.EqualFold(isoLevel, "READ-UNCOMMITTED") || strings.EqualFold(isoLevel, "READ UNCOMMITTED")
		if !isReadCommitted && !isReadUncommitted {
			// Check if the new row's PK falls between locked PKs (gap lock)
			if blocked := e.checkGapLockForInsert(insertDB, tableName, stmt); blocked {
				timeout := 50.0
				if v, ok := e.getSysVar("innodb_lock_wait_timeout"); ok {
					if t, err := strconv.ParseFloat(v, 64); err == nil {
						timeout = t
					}
				}
				// Wait up to timeout, checking periodically
				deadline := time.Now().Add(time.Duration(timeout * float64(time.Second)))
				for time.Now().Before(deadline) {
					if !e.checkGapLockForInsert(insertDB, tableName, stmt) {
						break
					}
					time.Sleep(50 * time.Millisecond)
				}
				if e.checkGapLockForInsert(insertDB, tableName, stmt) {
					return nil, mysqlError(1205, "HY000", "Lock wait timeout exceeded; try restarting transaction")
				}
			}
		}
	}

	tbl, err := e.Storage.GetTable(insertDB, tableName)
	if err != nil {
		return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", insertDB, tableName))
	}

	// Apply SET INSERT_ID if set
	if e.nextInsertID > 0 {
		tbl.AutoIncrement.Store(e.nextInsertID - 1)
		tbl.AIExplicitlySet = true
		e.nextInsertID = 0
	}

	// Get column names and normalize them to match the table def's column names
	// (SQL column names may differ in case from the table definition, e.g. SET A=NULL vs col.Name="a")
	colNames := make([]string, len(stmt.Columns))
	for i, col := range stmt.Columns {
		colNames[i] = col.String()
	}
	// Normalize column names to match actual table column names (case-insensitive lookup)
	// and validate that all specified columns exist in the table.
	if len(colNames) > 0 {
		colNameByLower := make(map[string]string, len(tbl.Def.Columns))
		for _, col := range tbl.Def.Columns {
			colNameByLower[strings.ToLower(col.Name)] = col.Name
		}
		for i, cn := range colNames {
			if actual, ok := colNameByLower[strings.ToLower(cn)]; ok {
				colNames[i] = actual
			} else {
				return nil, mysqlError(1054, "42S22",
					fmt.Sprintf("Unknown column '%s' in 'field list'", cn))
			}
		}
	}

	// If no columns specified, use all columns from table def
	if len(colNames) == 0 {
		for _, col := range tbl.Def.Columns {
			colNames = append(colNames, col.Name)
		}
	}

	// Handle INSERT ... SELECT (including UNION)
	// Fast path: directly build storage rows from SELECT results, bypassing
	// AST string-literal roundtrip and per-row evalExpr overhead.
	insertSelectFastPath := func(selResult *Result) (*Result, error) {
		if len(colNames) == 0 {
			for _, col := range tbl.Def.Columns {
				colNames = append(colNames, col.Name)
			}
		}

		// Check if any explicitly specified column is a generated column
		// MySQL rejects INSERT ... SELECT when generated columns are in the target list
		for _, cn := range colNames {
			for _, col := range tbl.Def.Columns {
				if strings.EqualFold(col.Name, cn) && isGeneratedColumnType(col.Type) {
					return nil, mysqlError(3105, "HY000",
						fmt.Sprintf("The value specified for generated column '%s' in table '%s' is not allowed.",
							cn, tbl.Def.Name))
				}
			}
		}

		// Check if fast path is applicable: no ON DUPLICATE KEY, no REPLACE, no IGNORE
		canFastPath := len(stmt.OnDup) == 0 &&
			stmt.Action != sqlparser.ReplaceAct &&
			!bool(stmt.Ignore)

		if !canFastPath {
			return nil, nil // fall through to slow path
		}

		// Check for triggers on this table
		if db, dbErr := e.Catalog.GetDatabase(insertDB); dbErr == nil {
			if len(db.GetTriggersForTable(tableName, "BEFORE", "INSERT")) > 0 ||
				len(db.GetTriggersForTable(tableName, "AFTER", "INSERT")) > 0 {
				return nil, nil // fall through to slow path
			}
		}

		// Precompute column metadata for coercion
		type colMeta struct {
			idx        int
			col        catalog.ColumnDef
			padLen     int
			hasGenExpr bool
		}
		colMetaMap := make(map[string]*colMeta, len(tbl.Def.Columns))
		hasGeneratedCols := false
		autoColName := ""
		for ci, col := range tbl.Def.Columns {
			cm := &colMeta{idx: ci, col: col, padLen: binaryPadLength(col.Type)}
			cm.hasGenExpr = generatedColumnExpr(col.Type) != ""
			if cm.hasGenExpr {
				hasGeneratedCols = true
			}
			if col.AutoIncrement {
				autoColName = col.Name
			}
			colMetaMap[col.Name] = cm
		}

		var lastInsertID int64
		var firstAutoInsertID int64
		var affected uint64

		// Pre-allocate bulk rows
		bulkRows := make([]storage.Row, 0, len(selResult.Rows))

		for _, selRow := range selResult.Rows {
			row := make(storage.Row, len(colNames)+4) // slight over-alloc for defaults
			for i, v := range selRow {
				if i >= len(colNames) {
					break
				}
				colName := colNames[i]
				if cm, ok := colMetaMap[colName]; ok {
					v = e.coerceColumnValueForWrite(cm.col.Type, v)
				}
				row[colName] = v
			}

			// Populate generated columns
			if hasGeneratedCols {
				if err := e.populateGeneratedColumns(row, tbl.Def.Columns); err != nil {
					return nil, err
				}
			}

			// Fill in defaults for missing columns
			for _, col := range tbl.Def.Columns {
				if _, exists := row[col.Name]; !exists {
					if col.AutoIncrement {
						// Let storage handle it
					} else if genExpr := generatedColumnExpr(col.Type); genExpr != "" {
						if v, err := e.evalGeneratedColumnExpr(genExpr, row); err == nil {
							row[col.Name] = v
						}
					} else if col.Default != nil {
						defVal := *col.Default
						defUpper := strings.ToUpper(defVal)
						if defUpper == "CURRENT_TIMESTAMP" || defUpper == "CURRENT_TIMESTAMP()" || defUpper == "NOW()" {
							defVal = e.nowTime().Format("2006-01-02 15:04:05")
						}
						row[col.Name] = defVal
					} else if !col.Nullable {
						row[col.Name] = implicitZeroValue(col.Type)
					}
				}
			}

			// Strict mode NOT NULL checks
			if e.isStrictMode() {
				for _, col := range tbl.Def.Columns {
					isAutoGenCol := col.AutoIncrement || isGeneratedColumnType(col.Type)
					if !col.Nullable && !isAutoGenCol {
						explicitlySpecified := false
						for _, cn := range colNames {
							if strings.EqualFold(cn, col.Name) {
								explicitlySpecified = true
								break
							}
						}
						if !explicitlySpecified && col.Default == nil {
							return nil, mysqlError(1364, "HY000", fmt.Sprintf("Field '%s' doesn't have a default value", col.Name))
						}
						if rv, exists := row[col.Name]; exists && rv == nil && explicitlySpecified {
							return nil, mysqlError(1048, "23000", fmt.Sprintf("Column '%s' cannot be null", col.Name))
						}
					}
				}
			}

			// Enforce FOREIGN KEY constraints: verify parent row exists
			if err := e.checkForeignKeyOnInsert(insertDB, tableName, row); err != nil {
				return nil, err
			}

			bulkRows = append(bulkRows, row)
		}

		// Use BulkInsert for efficiency
		if autoColName == "" && len(bulkRows) > 0 {
			ids, err := tbl.BulkInsert(bulkRows)
			if err != nil {
				return nil, err
			}
			affected = uint64(len(ids))
		} else {
			// Has auto-increment: insert one by one for correct ID tracking
			noAutoValueOnZeroSelect := strings.Contains(e.sqlMode, "NO_AUTO_VALUE_ON_ZERO")
			for _, row := range bulkRows {
				autoGeneratedThisRow := false
				if autoColName != "" {
					if v, exists := row[autoColName]; !exists || v == nil {
						autoGeneratedThisRow = true
					} else {
						isZeroVal := false
						switch av := v.(type) {
						case int64:
							isZeroVal = av == 0
						case uint64:
							isZeroVal = av == 0
						case float64:
							isZeroVal = int64(av) == 0
						case string:
							isZeroVal = strings.TrimSpace(av) == "" || strings.TrimSpace(av) == "0"
						}
						autoGeneratedThisRow = isZeroVal && !noAutoValueOnZeroSelect
					}
				}
				// With innodb_autoinc_lock_mode=0, failed inserts should not
				// consume auto-increment values. Save and restore on failure.
				var savedAutoInc int64
				lockMode0 := false
				if autoGeneratedThisRow {
					if lm, ok := e.getSysVar("innodb_autoinc_lock_mode"); ok && lm == "0" {
						lockMode0 = true
						savedAutoInc = tbl.AutoIncrementValue()
					}
				}
				// Tag row with connection ID for transaction isolation
				if e.inTransaction && e.txnActiveSet != nil {
					row["__txn_conn_id__"] = e.connectionID
				}
				id, err := tbl.Insert(row, noAutoValueOnZeroSelect)
				if err != nil {
					if lockMode0 {
						tbl.AutoIncrement.Store(savedAutoInc)
					}
					delete(row, "__txn_conn_id__")
					return nil, err
				}
				// Record undo entry for transaction rollback
				if e.inTransaction {
					e.txnUndoLog = append(e.txnUndoLog, undoEntry{
						op:     "INSERT",
						db:     insertDB,
						table:  tableName,
						oldRow: storage.CloneRow(row),
					})
				}
				lastInsertID = id
				if autoGeneratedThisRow && firstAutoInsertID == 0 && id > 0 {
					firstAutoInsertID = id
				}
				affected++
			}
		}

		if firstAutoInsertID > 0 {
			lastInsertID = firstAutoInsertID
		}
		// Only overwrite e.lastInsertID if an auto-increment insert occurred.
		if lastInsertID > 0 {
			e.lastInsertID = lastInsertID
		}

		if lastInsertID > 0 {
			for _, col := range tbl.Def.Columns {
				if col.AutoIncrement && !col.Nullable {
					e.lastAutoIncID = lastInsertID
					break
				}
			}
		}

		if affected > 0 {
			if db, dbErr := e.Catalog.GetDatabase(insertDB); dbErr == nil {
				if def, defErr := db.GetTable(tableName); defErr == nil && e.innodbStatsAutoRecalcEnabled(def) && e.innodbStatsPersistentEnabled(def) {
					e.maybeRecalcStats(insertDB, tableName, int64(affected))
				}
			}
		}

		return &Result{AffectedRows: affected, InsertID: uint64(lastInsertID)}, nil
	}

	if sel, ok := stmt.Rows.(*sqlparser.Select); ok {
		// For INSERT...SELECT with no real FROM clause, validate bare column references
		// in the SELECT list. MySQL raises ER_BAD_FIELD_ERROR for unresolvable column names.
		selHasRealFrom := false
		for _, f := range sel.From {
			if ate, ok2 := f.(*sqlparser.AliasedTableExpr); ok2 {
				if tn, ok3 := ate.Expr.(sqlparser.TableName); ok3 {
					if strings.ToLower(tn.Name.String()) != "dual" {
						selHasRealFrom = true
						break
					}
				} else {
					selHasRealFrom = true
					break
				}
			} else {
				selHasRealFrom = true
				break
			}
		}
		if !selHasRealFrom {
			for _, expr := range sel.SelectExprs.Exprs {
				if se, ok2 := expr.(*sqlparser.AliasedExpr); ok2 {
					if err := validateNoFromTopLevelColRefs(se.Expr); err != nil {
						return nil, err
					}
				}
			}
		}
		selResult, err := e.execSelect(sel)
		if err != nil {
			return nil, err
		}
		if result, err := insertSelectFastPath(selResult); err != nil {
			return nil, err
		} else if result != nil {
			return result, nil
		}
		// Fall through to slow path
		// Build defaults map from source SELECT tables for DEFAULT(col) support in ON DUPLICATE KEY UPDATE.
		if len(stmt.OnDup) > 0 {
			sourceDefaults := make(map[string]interface{})
			// Walk the FROM clause to find source table names
			for _, tableExpr := range sel.From {
				var srcTableName string
				switch te := tableExpr.(type) {
				case *sqlparser.AliasedTableExpr:
					if tname, ok := te.Expr.(sqlparser.TableName); ok {
						srcTableName = tname.Name.String()
					}
				}
				if srcTableName == "" {
					continue
				}
				if srcDB, dbErr := e.Catalog.GetDatabase(e.CurrentDB); dbErr == nil {
					if srcDef, tblErr := srcDB.GetTable(srcTableName); tblErr == nil {
						for _, col := range srcDef.Columns {
							colKey := strings.ToLower(col.Name)
							if col.Default != nil {
								sourceDefaults[colKey] = *col.Default
							} else {
								sourceDefaults[colKey] = nil
							}
						}
					}
				}
			}
			if len(sourceDefaults) > 0 {
				prevDefaultsByColName := e.defaultsByColName
				e.defaultsByColName = sourceDefaults
				defer func() { e.defaultsByColName = prevDefaultsByColName }()
			}
		}
		// Check for ambiguous column references in ON DUPLICATE KEY UPDATE expressions.
		// MySQL error 1052: if an unqualified column name in the ODKU RHS expression
		// exists in both the SELECT source result columns AND the target table columns,
		// it is ambiguous and MySQL raises ER_NON_UNIQ_ERROR.
		// MySQL WL#5094: when the source SELECT is grouped (GROUP BY) or has aggregates,
		// column references in ODKU must refer only to target table columns (source cols
		// are not accessible from grouped queries).
		if len(stmt.OnDup) > 0 {
			// Determine if the source SELECT is grouped or aggregate-only.
			selectIsGrouped := sel.GroupBy != nil && len(sel.GroupBy.Exprs) > 0
			if !selectIsGrouped {
				// Check for aggregate functions in select exprs
				for _, se := range sel.SelectExprs.Exprs {
					if ae, ok := se.(*sqlparser.AliasedExpr); ok {
						if isAggregateExpr(ae.Expr) {
							selectIsGrouped = true
							break
						}
					}
				}
			}
			var sourceColSet map[string]bool
			if !selectIsGrouped {
				// Non-grouped SELECT: source column names are accessible in ODKU.
				sourceColSet = make(map[string]bool, len(selResult.Columns))
				for _, sc := range selResult.Columns {
					sourceColSet[strings.ToLower(sc)] = true
				}
			} else {
				// Grouped SELECT: source column names are NOT accessible in ODKU.
				sourceColSet = make(map[string]bool)
			}
			targetColSet := make(map[string]bool, len(tbl.Def.Columns))
			for _, tc := range tbl.Def.Columns {
				targetColSet[strings.ToLower(tc.Name)] = true
			}
			for _, upd := range stmt.OnDup {
				// Check all unqualified column references in the update RHS expression.
				// If a column name exists in both source and target, it is ambiguous.
				// If a column name exists in neither source nor target, it is unknown.
				if walkErr := sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
					// Don't walk into subqueries – column references there are validated separately.
					switch node.(type) {
					case *sqlparser.Subquery, *sqlparser.Select, *sqlparser.Union:
						return false, nil
					}
					if colName, ok := node.(*sqlparser.ColName); ok {
						if colName.Qualifier.IsEmpty() {
							cn := strings.ToLower(colName.Name.String())
							if sourceColSet[cn] && targetColSet[cn] {
								return false, mysqlError(1052, "23000",
									fmt.Sprintf("Column '%s' in field list is ambiguous", colName.Name.String()))
							}
							if !sourceColSet[cn] && !targetColSet[cn] {
								return false, mysqlError(1054, "42S22",
									fmt.Sprintf("Unknown column '%s' in 'field list'", colName.Name.String()))
							}
						}
					}
					return true, nil
				}, upd.Expr); walkErr != nil {
					return nil, walkErr
				}
			}
		}
		var valRows sqlparser.Values
		for _, selRow := range selResult.Rows {
			var tuple sqlparser.ValTuple
			for _, v := range selRow {
				if v == nil {
					tuple = append(tuple, &sqlparser.NullVal{})
				} else {
					tuple = append(tuple, sqlparser.NewStrLiteral(fmt.Sprintf("%v", v)))
				}
			}
			valRows = append(valRows, tuple)
		}
		stmt.Rows = valRows
		// Save source column names for ON DUPLICATE KEY UPDATE column reference resolution.
		// When INSERT...SELECT ON DUPLICATE KEY UPDATE uses bare column names like 'a',
		// they refer to the source SELECT columns, not the target table columns.
		// Exception: grouped queries (GROUP BY or aggregates) don't expose source col names.
		if len(stmt.OnDup) > 0 {
			odkvSourceIsGrouped := sel.GroupBy != nil && len(sel.GroupBy.Exprs) > 0
			if !odkvSourceIsGrouped {
				for _, se := range sel.SelectExprs.Exprs {
					if ae, ok2 := se.(*sqlparser.AliasedExpr); ok2 {
						if isAggregateExpr(ae.Expr) {
							odkvSourceIsGrouped = true
							break
						}
					}
				}
			}
			if !odkvSourceIsGrouped {
				prevSourceColNames := e.onDupSourceColNames
				e.onDupSourceColNames = selResult.Columns
				defer func() { e.onDupSourceColNames = prevSourceColNames }()
			}
		}
	} else if union, ok := stmt.Rows.(*sqlparser.Union); ok {
		unionResult, err := e.execUnion(union)
		if err != nil {
			return nil, err
		}
		if result, err := insertSelectFastPath(unionResult); err != nil {
			return nil, err
		} else if result != nil {
			return result, nil
		}
		// Fall through to slow path
		// For UNION queries, source column names are NOT accessible in ODKU (same as grouped SELECT).
		// MySQL WL#5094: validate that ODKU column references don't refer to source columns.
		if len(stmt.OnDup) > 0 {
			// UNION source: column names are not accessible in ODKU; target cols only.
			sourceColSet := make(map[string]bool) // empty: no source col names for UNION
			targetColSet := make(map[string]bool, len(tbl.Def.Columns))
			for _, tc := range tbl.Def.Columns {
				targetColSet[strings.ToLower(tc.Name)] = true
			}
			for _, upd := range stmt.OnDup {
				if walkErr := sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
					switch node.(type) {
					case *sqlparser.Subquery, *sqlparser.Select, *sqlparser.Union:
						return false, nil
					}
					if colName, ok := node.(*sqlparser.ColName); ok {
						if colName.Qualifier.IsEmpty() {
							cn := strings.ToLower(colName.Name.String())
							if sourceColSet[cn] && targetColSet[cn] {
								return false, mysqlError(1052, "23000",
									fmt.Sprintf("Column '%s' in field list is ambiguous", colName.Name.String()))
							}
							if !sourceColSet[cn] && !targetColSet[cn] {
								return false, mysqlError(1054, "42S22",
									fmt.Sprintf("Unknown column '%s' in 'field list'", colName.Name.String()))
							}
						}
					}
					return true, nil
				}, upd.Expr); walkErr != nil {
					return nil, walkErr
				}
			}
		}
		var valRows sqlparser.Values
		for _, selRow := range unionResult.Rows {
			var tuple sqlparser.ValTuple
			for _, v := range selRow {
				if v == nil {
					tuple = append(tuple, &sqlparser.NullVal{})
				} else {
					tuple = append(tuple, sqlparser.NewStrLiteral(fmt.Sprintf("%v", v)))
				}
			}
			valRows = append(valRows, tuple)
		}
		stmt.Rows = valRows
	}

	rows, ok := stmt.Rows.(sqlparser.Values)
	if !ok {
		return nil, fmt.Errorf("unsupported INSERT format")
	}

	// Collect primary key column names and unique key column names from the table def.
	var pkCols []string
	var uniqueCols []string
	for _, col := range tbl.Def.Columns {
		if col.PrimaryKey {
			pkCols = append(pkCols, col.Name)
		}
		if col.Unique {
			uniqueCols = append(uniqueCols, col.Name)
		}
	}
	// Also use PrimaryKey slice from the TableDef (set from table-level PRIMARY KEY constraint).
	if len(pkCols) == 0 && len(tbl.Def.PrimaryKey) > 0 {
		pkCols = tbl.Def.PrimaryKey
	}
	// Add unique columns from index definitions
	for _, idx := range tbl.Def.Indexes {
		if idx.Unique && len(idx.Columns) == 1 {
			uniqueCols = append(uniqueCols, idx.Columns[0])
		}
	}

	var lastInsertID int64
	var firstAutoInsertID int64
	var affected uint64
	var odkuCount uint64  // number of rows that triggered ON DUPLICATE KEY UPDATE
	var totalRows uint64  // total rows processed from VALUES/SELECT
	autoColName := ""
	for _, col := range tbl.Def.Columns {
		if col.AutoIncrement {
			autoColName = col.Name
			break
		}
	}

	// For transactional engines (InnoDB) in strict mode without IGNORE, a multi-row
	// INSERT must be atomic: if any row fails, all rows inserted in this INSERT are rolled back.
	// For non-transactional engines (MyISAM), rows inserted before the error are KEPT
	// (MySQL's actual behavior: MyISAM commits each row as it's written).
	tblEngineForSnap := strings.ToUpper(tbl.Def.Engine)
	if tblEngineForSnap == "" {
		if eng, ok := e.getSysVar("default_storage_engine"); ok && eng != "" {
			tblEngineForSnap = strings.ToUpper(eng)
		}
	}
	isTransactionalEngine := tblEngineForSnap != "MYISAM" && tblEngineForSnap != "MRG_MYISAM" && tblEngineForSnap != "MEMORY" && tblEngineForSnap != "ARCHIVE" && tblEngineForSnap != "CSV"
	snapshotRowCount := -1
	if isTransactionalEngine && e.isStrictMode() && !bool(stmt.Ignore) && len(rows) > 1 {
		tbl.Lock()
		snapshotRowCount = len(tbl.Rows)
		tbl.Unlock()
	}
	// rollbackInsertedRows removes rows added in this INSERT statement after an error (InnoDB only).
	rollbackInsertedRows := func() {
		if snapshotRowCount >= 0 {
			tbl.Lock()
			if len(tbl.Rows) > snapshotRowCount {
				tbl.Rows = tbl.Rows[:snapshotRowCount]
				tbl.InvalidateIndexes()
			}
			tbl.Unlock()
		}
	}


	for _, valTuple := range rows {
		totalRows++
		row := make(storage.Row)
		origValues := make(storage.Row) // original values before formatting (for strict mode checks)
		for i, val := range valTuple {
			if i >= len(colNames) {
				break
			}
			// Check if value is being inserted into a generated column
			// MySQL rejects any non-DEFAULT value (including NULL) for generated columns
			if _, isDefault := val.(*sqlparser.Default); !isDefault {
				for _, col := range tbl.Def.Columns {
					if col.Name == colNames[i] && isGeneratedColumnType(col.Type) {
						return nil, mysqlError(3105, "HY000",
							fmt.Sprintf("The value specified for generated column '%s' in table '%s' is not allowed.",
								colNames[i], tbl.Def.Name))
					}
				}
			}
			v, err := e.evalExpr(val)
			// For INSERT ... SET col1=val1, col2=col1 — if the expression is a bare
			// column name reference and that column was already assigned in this same
			// INSERT SET clause, use the previously-assigned value (left-to-right
			// evaluation, matching MySQL behaviour).
			if colRef, isColRef := val.(*sqlparser.ColName); isColRef && colRef.Qualifier.IsEmpty() && err == nil {
				refName := colRef.Name.String()
				for j, prevCol := range colNames {
					if j >= i {
						break
					}
					if strings.EqualFold(prevCol, refName) {
						if prevVal, found := row[prevCol]; found {
							v = prevVal
						}
						break
					}
				}
			}
			// If the expression is DEFAULT keyword, resolve the column's actual default
			if _, isDefault := val.(*sqlparser.Default); isDefault {
				for _, col := range tbl.Def.Columns {
					if col.Name == colNames[i] {
						if col.Default != nil {
							defVal := *col.Default
							defUpper := strings.ToUpper(defVal)
							if defUpper == "CURRENT_TIMESTAMP" || defUpper == "CURRENT_TIMESTAMP()" || defUpper == "NOW()" {
								v = e.nowTime().Format("2006-01-02 15:04:05")
							} else if defUpper == "NULL" {
								v = nil
							} else {
								// Try to parse as expression; fallback to string
								v = strings.Trim(defVal, "'")
							}
						} else if strings.HasPrefix(strings.ToUpper(col.Type), "TIMESTAMP") {
							// TIMESTAMP columns implicitly default to CURRENT_TIMESTAMP
							v = e.nowTime().Format("2006-01-02 15:04:05")
						} else if col.AutoIncrement {
							v = nil // AUTO_INCREMENT will be handled later
						} else if !col.Nullable && !isGeneratedColumnType(col.Type) {
							// DEFAULT keyword used on NOT NULL column with no actual default
							// In strict mode: error 1364; in non-strict: warning + zero value
							if e.isStrictMode() && !bool(stmt.Ignore) {
								return nil, mysqlError(1364, "HY000", fmt.Sprintf("Field '%s' doesn't have a default value", col.Name))
							}
							e.addWarning("Warning", 1364, fmt.Sprintf("Field '%s' doesn't have a default value", col.Name))
							v = implicitZeroValue(col.Type)
						} else {
							v = implicitZeroValue(col.Type)
						}
						break
					}
				}
			}
			if err != nil {
				var intOvErr *intOverflowError
				if errors.As(err, &intOvErr) {
					// For BINARY/VARBINARY columns, convert large hex literals to binary strings.
					overflowStr := intOvErr.val
					if intOvErr.kind == "BINARY" {
						isBinCol := false
						isCharCol := false
						for _, col := range tbl.Def.Columns {
							if col.Name == colNames[i] {
								colUpper := strings.ToUpper(col.Type)
								if strings.HasPrefix(colUpper, "BINARY") || strings.HasPrefix(colUpper, "VARBINARY") || colUpper == "BLOB" || colUpper == "TINYBLOB" || colUpper == "MEDIUMBLOB" || colUpper == "LONGBLOB" {
									isBinCol = true
								}
								if strings.Contains(colUpper, "CHAR") || strings.Contains(colUpper, "TEXT") {
									isCharCol = true
								}
								break
							}
						}
						if isBinCol || isCharCol {
							// Decode hex string to binary bytes
							if bs, herr := hex.DecodeString(overflowStr); herr == nil {
								v = string(bs)
								err = nil
							}
						}
					}
					// For DECIMAL/FLOAT/DOUBLE/REAL columns, parse overflow as float
					isDecCol := false
					for _, col := range tbl.Def.Columns {
						if col.Name == colNames[i] {
							colUpper := strings.ToUpper(col.Type)
							if strings.Contains(colUpper, "DECIMAL") || strings.Contains(colUpper, "FLOAT") || strings.Contains(colUpper, "DOUBLE") || colUpper == "REAL" || strings.HasPrefix(colUpper, "REAL ") {
								isDecCol = true
							}
							break
						}
					}
					if err != nil && isDecCol {
						if f, ferr := strconv.ParseFloat(overflowStr, 64); ferr == nil {
							v = f
							err = nil
						}
					}
					// For INT columns in non-strict mode, clip to type range
					if err != nil && !e.isStrictMode() {
						isIntCol := false
						isUnsigned := false
						for _, col := range tbl.Def.Columns {
							if col.Name == colNames[i] {
								colUpper := strings.ToUpper(col.Type)
								if strings.Contains(colUpper, "INT") || strings.Contains(colUpper, "INTEGER") {
									isIntCol = true
									isUnsigned = strings.Contains(colUpper, "UNSIGNED")
								}
								break
							}
						}
						if isIntCol {
							// Clip to type range
							if strings.HasPrefix(overflowStr, "-") {
								if isUnsigned {
									v = int64(0)
								} else {
									v = int64(math.MinInt64)
								}
							} else {
								if isUnsigned {
									v = uint64(math.MaxUint64)
								} else {
									v = int64(math.MaxInt64)
								}
							}
							err = nil
						}
					}
					if err != nil {
						if bool(stmt.Ignore) {
							e.addWarning("Warning", 1264, fmt.Sprintf("Out of range value for column '%s' at row 1", colNames[i]))
							// In IGNORE mode, coerce overflow literal to clamped value.
							// Use overflowStr to determine the correct clamped value.
							for _, col := range tbl.Def.Columns {
								if col.Name == colNames[i] {
									v = coerceIntegerValue(col.Type, overflowStr)
									break
								}
							}
							err = nil
						} else {
							return nil, mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colNames[i]))
						}
					}
				} else {
					if bool(stmt.Ignore) {
						e.addWarning("Warning", 1264, fmt.Sprintf("Out of range value for column '%s' at row 1", colNames[i]))
					} else {
						return nil, err
					}
				}
			}
			origValues[colNames[i]] = v
			// Pad BINARY(N), format DECIMAL, validate ENUM/SET.
			for _, col := range tbl.Def.Columns {
				if col.Name == colNames[i] {
					if v != nil {
						// In strict mode, check DECIMAL range and unsigned constraint before clipping
						if e.isStrictMode() {
							colUpper := strings.ToUpper(col.Type)
							isDecType := strings.Contains(colUpper, "DECIMAL") || strings.Contains(colUpper, "FLOAT") || strings.Contains(colUpper, "DOUBLE") || colUpper == "REAL" || strings.HasPrefix(colUpper, "REAL ")
							if isDecType {
								// In strict mode, string values that cannot be parsed as a valid
								// float need special handling depending on the column type:
								//   - DECIMAL/NUMERIC: "Incorrect decimal value" error 1366 (HY000)
								//   - FLOAT/DOUBLE/REAL: "Data truncated" error 1265 (01000)
								// This check must happen BEFORE checkDecimalRange which calls toFloat()
								// and may treat the value as in-range.
								isDecimalOrNumeric := strings.Contains(colUpper, "DECIMAL") || strings.Contains(colUpper, "NUMERIC")
								isFloatType := !isDecimalOrNumeric && (strings.Contains(colUpper, "FLOAT") || strings.Contains(colUpper, "DOUBLE") || strings.Contains(colUpper, "REAL"))
								if sv, isStr := v.(string); isStr {
									svTrimmed := strings.TrimSpace(sv)
									if _, pfErr := strconv.ParseFloat(svTrimmed, 64); pfErr != nil {
										// String doesn't parse as a valid float.
										if bool(stmt.Ignore) {
											if prefix2, ok2 := extractNumericPrefix(svTrimmed); ok2 && prefix2 != svTrimmed {
												e.addWarning("Note", 1265, fmt.Sprintf("Data truncated for column '%s' at row 1", col.Name))
												v = prefix2
												// fall through to coerceColumnValueForWrite for proper formatting
											} else if isDecimalOrNumeric {
												e.addWarning("Warning", 1366, fmt.Sprintf("Incorrect decimal value: '%s' for column '%s' at row 1", sv, col.Name))
												v = float64(0)
											} else {
												// FLOAT/DOUBLE/REAL: Warning 1265 (Data truncated)
												e.addWarning("Warning", 1265, fmt.Sprintf("Data truncated for column '%s' at row 1", col.Name))
												v = float64(0)
											}
										} else if isDecimalOrNumeric {
											return nil, mysqlError(1366, "HY000", fmt.Sprintf("Incorrect decimal value: '%s' for column '%s' at row 1", sv, col.Name))
										} else if isFloatType {
											// FLOAT/DOUBLE/REAL: Data truncated error 1265 / 01000
											return nil, mysqlError(1265, "01000", fmt.Sprintf("Data truncated for column '%s' at row 1", col.Name))
										}
										// Skip further decimal range validation since we've handled it.
										// Run coerceColumnValueForWrite for proper formatting then break.
										v = e.coerceColumnValueForWrite(col.Type, v)
										row[colNames[i]] = v
										break
									}
								}
								_ = isFloatType
								if strings.Contains(colUpper, "UNSIGNED") || strings.Contains(colUpper, "ZEROFILL") {
									f := toFloat(v)
									if f < 0 {
										if bool(stmt.Ignore) {
											e.addWarning("Warning", 1264, fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
											v = int64(0)
										} else {
											return nil, mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
										}
									}
								}
								if err := checkDecimalRange(col.Type, v); err != nil {
									if bool(stmt.Ignore) {
										e.addWarning("Warning", 1264, fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
									} else {
										return nil, mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
									}
								}
								// Bare FLOAT/DOUBLE/REAL: check if value exceeds float32/float64 range in strict mode.
								// float64(float32(x)) returns ±Inf when x exceeds float32 range for FLOAT columns.
								colUpperBase := colUpper
								if idx := strings.Index(colUpperBase, "("); idx >= 0 {
									colUpperBase = colUpperBase[:idx]
								}
								colUpperBase = strings.TrimSpace(colUpperBase)
								if colUpperBase == "FLOAT" {
									f := toFloat(v)
									if f32 := float32(f); math.IsInf(float64(f32), 0) {
										if bool(stmt.Ignore) {
											e.addWarning("Warning", 1264, fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
										} else {
											return nil, mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
										}
									}
								} else if colUpperBase == "DOUBLE" || colUpperBase == "REAL" {
									f := toFloat(v)
									if math.IsInf(f, 0) {
										if bool(stmt.Ignore) {
											e.addWarning("Warning", 1264, fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
										} else {
											return nil, mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
										}
									}
								}
							}
							// Strict mode: check integer type constraints
							if err := checkIntegerStrict(col.Type, col.Name, v); err != nil {
								if bool(stmt.Ignore) {
									e.addWarning("Warning", 1264, fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
									v = coerceIntegerValue(col.Type, v)
								} else {
									return nil, err
								}
							}
							// Strict mode: validate DATE/DATETIME/TIMESTAMP/TIME values
							if sv, ok := v.(string); ok {
								// handleDateTimeErr processes a date/time validation error.
								// For IGNORE or non-transactional MyISAM under STRICT_TRANS_TABLES,
								// emit a warning and store zero value. Otherwise return the error.
								handleDateTimeErr := func(err error) error {
									tblEngine := strings.ToUpper(tbl.Def.Engine)
									if tblEngine == "" {
										if eng, ok := e.getSysVar("default_storage_engine"); ok && eng != "" {
											tblEngine = strings.ToUpper(eng)
										}
									}
									isNonTransactional := tblEngine == "MYISAM" || tblEngine == "MRG_MYISAM" || tblEngine == "MEMORY" || tblEngine == "ARCHIVE" || tblEngine == "CSV"
									strictAllTables := strings.Contains(e.sqlMode, "STRICT_ALL_TABLES") || strings.Contains(e.sqlMode, "TRADITIONAL")
									// For MyISAM under STRICT_TRANS_TABLES (not STRICT_ALL_TABLES):
									// if at least one row has already been inserted, convert error to warning.
									// If this is the very first row (affected==0), return the error.
									isPostFirstRow := isNonTransactional && !strictAllTables && affected > 0
									if bool(stmt.Ignore) || isPostFirstRow {
										e.addWarning("Warning", 1264, fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
										v = zeroDateTimeValue(col.Type)
										return nil
									}
									return err
								}
								if err := checkDateStrict(col.Type, col.Name, sv, e.sqlMode, int(totalRows)); err != nil {
									if handledErr := handleDateTimeErr(err); handledErr != nil {
										rollbackInsertedRows()
										return nil, handledErr
									}
								}
								if err := checkTimeStrict(col.Type, col.Name, sv, int(totalRows)); err != nil {
									if handledErr := handleDateTimeErr(err); handledErr != nil {
										rollbackInsertedRows()
										return nil, handledErr
									}
								}
							}
						} else {
							// Non-strict mode: check for out-of-range integer values and generate warnings
							if err := checkIntegerStrict(col.Type, col.Name, v); err != nil {
								e.addWarning("Warning", 1264, fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
							}
						}
					}
					// For DECIMAL columns: if a string value has an invalid numeric suffix that
					// can be truncated (e.g. "123.4e" -> "123.4"), emit Note 1265 proactively.
					// formatDecimalValue will silently handle the truncation; we need to warn here.
					if v != nil {
						colUpper2 := strings.ToUpper(col.Type)
						if strings.Contains(colUpper2, "DECIMAL") {
							if sv, isStr := v.(string); isStr {
								svTrimmed := strings.TrimSpace(sv)
								if _, perr2 := strconv.ParseFloat(svTrimmed, 64); perr2 != nil {
									if prefix2, ok2 := extractNumericPrefix(svTrimmed); ok2 && prefix2 != svTrimmed {
										e.addWarning("Note", 1265, fmt.Sprintf("Data truncated for column '%s' at row 1", col.Name))
									}
								}
							}
						}
					}
					// When ALLOW_INVALID_DATES is active, DATE/DATETIME columns may have
					// days out of the normal month range. parseMySQLDateValue rejects these,
					// so we format them directly instead of going through coerce.
					if strings.Contains(e.sqlMode, "ALLOW_INVALID_DATES") {
						colUpper3 := strings.ToUpper(col.Type)
						if sv3, isStr := v.(string); isStr && (colUpper3 == "DATE" || strings.HasPrefix(colUpper3, "DATETIME")) {
							if formatted := parseMySQLDateValueAllowInvalid(sv3); formatted != "" {
								v = formatted
								break
							}
						}
					}
					v = e.coerceColumnValueForWrite(col.Type, v)
					break
				}
			}
			row[colNames[i]] = v
		}
		// Fill in defaults for non-generated, non-specified columns BEFORE computing generated columns.
		// This ensures generated column expressions can reference base columns with default values.
		{
			colNameSet := make(map[string]bool, len(colNames))
			for _, cn := range colNames {
				colNameSet[cn] = true
			}
			for _, col := range tbl.Def.Columns {
				if colNameSet[col.Name] || isGeneratedColumnType(col.Type) || col.AutoIncrement {
					continue
				}
				if _, exists := row[col.Name]; !exists {
					if col.Default != nil {
						defVal := *col.Default
						defUpper := strings.ToUpper(defVal)
						if defUpper == "CURRENT_TIMESTAMP" || defUpper == "CURRENT_TIMESTAMP()" || defUpper == "NOW()" {
							row[col.Name] = e.nowTime().Format("2006-01-02 15:04:05")
						} else if defUpper == "NULL" {
							row[col.Name] = nil
						} else {
							row[col.Name] = strings.Trim(defVal, "'")
						}
					}
				}
			}
		}

		if err := e.populateGeneratedColumns(row, tbl.Def.Columns); err != nil {
			return nil, err
		}

		// Check NOT NULL constraint on generated columns after computing their values
		for _, col := range tbl.Def.Columns {
			if isGeneratedColumnType(col.Type) && !col.Nullable {
				if val, ok := row[col.Name]; ok && val == nil {
					return nil, mysqlError(1048, "23000", fmt.Sprintf("Column '%s' cannot be null", col.Name))
				}
			}
		}

		// Check explicit NULL on NOT NULL columns (always an error, even non-strict)
		{
			colNameSet := make(map[string]bool, len(colNames))
			for _, cn := range colNames {
				colNameSet[cn] = true
			}
			for _, col := range tbl.Def.Columns {
				isAutoGenCol := col.AutoIncrement || isGeneratedColumnType(col.Type)
				// In strict mode, missing NOT NULL columns without defaults are errors
				if e.isStrictMode() && !col.Nullable && !isAutoGenCol && col.Default == nil && !colNameSet[col.Name] {
					if bool(stmt.Ignore) {
						e.addWarning("Warning", 1364, fmt.Sprintf("Field '%s' doesn't have a default value", col.Name))
						row[col.Name] = implicitZeroValue(col.Type)
					} else {
						return nil, mysqlError(1364, "HY000", fmt.Sprintf("Field '%s' doesn't have a default value", col.Name))
					}
				}
				// Explicit NULL into NOT NULL column
				if !col.Nullable && !isAutoGenCol && colNameSet[col.Name] {
					if v, ok := row[col.Name]; ok && v == nil {
						// In multi-row INSERT non-strict mode: convert NULL to zero + warning
						// In single-row INSERT or strict mode: error
						isMultiRow := false
						if valTuples, ok := stmt.Rows.(sqlparser.Values); ok {
							isMultiRow = len(valTuples) > 1
						}
						if (e.isStrictMode() || !isMultiRow) && !bool(stmt.Ignore) {
							return nil, mysqlError(1048, "23000", fmt.Sprintf("Column '%s' cannot be null", col.Name))
						}
						e.addWarning("Warning", 1048, fmt.Sprintf("Column '%s' cannot be null", col.Name))
						row[col.Name] = implicitZeroValue(col.Type)
					}
				}
			}
		}

		// Check for explicit NULL on PRIMARY KEY columns (always an error, even non-strict)
		// MySQL never allows NULL in PK columns.
		if len(tbl.Def.PrimaryKey) > 0 {
			pkSet := make(map[string]bool, len(tbl.Def.PrimaryKey))
			for _, pk := range tbl.Def.PrimaryKey {
				pkSet[pk] = true
			}
			for i, cn := range colNames {
				if i < len(valTuple) && pkSet[cn] {
					if row[cn] == nil {
						// Check if column is auto_increment (NULL on AI PK is fine - generates next value)
						isAI := false
						for _, col := range tbl.Def.Columns {
							if col.Name == cn && col.AutoIncrement {
								isAI = true
								break
							}
						}
						if !isAI {
							return nil, mysqlError(1048, "23000", fmt.Sprintf("Column '%s' cannot be null", cn))
						}
					}
				}
			}
		}

		// ON DUPLICATE KEY UPDATE: check for existing row with matching PK or UNIQUE key.
		if len(stmt.OnDup) > 0 {
			dupIdx := e.findDuplicateRow(tbl, row, pkCols, uniqueCols)
			if dupIdx >= 0 {
				// Apply the ON DUPLICATE KEY UPDATE expressions to the existing row.
				tbl.Lock()
				// Snapshot the existing row so that ODKU expressions (e.g. a = a + 1)
				// read the pre-update values of columns, matching MySQL semantics.
				odduSnap := make(storage.Row, len(tbl.Rows[dupIdx]))
				for k, v := range tbl.Rows[dupIdx] {
					odduSnap[k] = v
				}
				prevOnDupValuesRow := e.onDupValuesRow
				// Augment the VALUES row with source column names (for INSERT...SELECT ODKU).
				// In INSERT t0 SELECT a FROM t1 ON DUPLICATE KEY UPDATE k = a + t1.a + 10,
				// 'a' refers to the source column, not the target 'k'. We add source col
				// name entries to the row so that evalColNameExpr can find them.
				odkvRow := row
				if len(e.onDupSourceColNames) > 0 {
					odkvRow = make(storage.Row, len(row)+len(e.onDupSourceColNames))
					for k, v := range row {
						odkvRow[k] = v
					}
					// Add source column name → value mappings.
					// colNames[i] is the target column; onDupSourceColNames[i] is the source column.
					for i, srcColName := range e.onDupSourceColNames {
						if i < len(colNames) {
							if v, ok := row[colNames[i]]; ok {
								odkvRow[srcColName] = v
								// Also add qualified "table.col" form (e.g. "t1.a")
								// so that t1.a resolves correctly.
							}
						}
					}
				}
				e.onDupValuesRow = odkvRow
				prevCorrelatedRow := e.correlatedRow
				e.correlatedRow = odduSnap
				prevDefaultsTableDef := e.defaultsTableDef
				e.defaultsTableDef = tbl.Def
				for _, upd := range stmt.OnDup {
					colName := upd.Name.Name.String()
					val, err := e.evalExpr(upd.Expr)
					if err != nil {
						e.onDupValuesRow = prevOnDupValuesRow
						e.correlatedRow = prevCorrelatedRow
						e.defaultsTableDef = prevDefaultsTableDef
						tbl.Unlock()
						return nil, err
					}
					// Pad BINARY(N) values, coerce DATE/TIME.
					for _, col := range tbl.Def.Columns {
						if col.Name == colName {
							val = e.coerceColumnValueForWrite(col.Type, val)
							break
						}
					}
					tbl.Rows[dupIdx][colName] = val
				}
				e.onDupValuesRow = prevOnDupValuesRow
				e.correlatedRow = prevCorrelatedRow
				e.defaultsTableDef = prevDefaultsTableDef
				// Recompute generated columns after ON DUPLICATE KEY UPDATE
				for _, col := range tbl.Def.Columns {
					if isGeneratedColumnType(col.Type) {
						expr := generatedColumnExpr(col.Type)
						if expr != "" {
							val, err := e.evalGeneratedColumnExpr(expr, tbl.Rows[dupIdx])
							if err == nil {
								val = coerceIntegerValue(col.Type, val)
								val = formatDecimalValue(col.Type, val)
								val = coerceDateTimeValue(col.Type, val)
								tbl.Rows[dupIdx][col.Name] = val
							}
						}
					}
				}
				// Apply ON UPDATE CURRENT_TIMESTAMP for columns with that property.
				// MySQL only fires ON UPDATE if the row actually changed (at least one column value differs).
				rowActuallyChanged := false
				for k, newVal := range tbl.Rows[dupIdx] {
					if fmt.Sprintf("%v", newVal) != fmt.Sprintf("%v", odduSnap[k]) {
						rowActuallyChanged = true
						break
					}
				}
				if rowActuallyChanged {
					for _, col := range tbl.Def.Columns {
						if col.OnUpdateCurrentTimestamp {
							// Only update if the column wasn't explicitly set in the ON DUP clause
							explicitlySet := false
							for _, upd := range stmt.OnDup {
								if upd.Name.Name.String() == col.Name {
									explicitlySet = true
									break
								}
							}
							if !explicitlySet {
								nowStr := e.nowTime().Format("2006-01-02 15:04:05")
								tbl.Rows[dupIdx][col.Name] = nowStr
							}
						}
					}
				}
				// Check NOT NULL constraints on the updated row.
				updatedRow := tbl.Rows[dupIdx]
				var firstNotNullViolation string
				for _, col := range tbl.Def.Columns {
					if !col.Nullable && !col.AutoIncrement && !isGeneratedColumnType(col.Type) {
						if updatedRow[col.Name] == nil {
							if bool(stmt.Ignore) {
								// INSERT IGNORE: coerce NULL to zero value, add warning, continue update
								e.addWarning("Warning", 1048, fmt.Sprintf("Column '%s' cannot be null", col.Name))
								tbl.Rows[dupIdx][col.Name] = implicitZeroValue(col.Type)
							} else {
								if firstNotNullViolation == "" {
									firstNotNullViolation = col.Name
								}
							}
						}
					}
				}
				if firstNotNullViolation != "" {
					// Rollback the update
					tbl.Rows[dupIdx] = odduSnap
					tbl.InvalidateIndexes()
					tbl.Unlock()
					return nil, mysqlError(1048, "23000", fmt.Sprintf("Column '%s' cannot be null", firstNotNullViolation))
				}
				// Check that the updated row does not violate any other unique constraints.
				// E.g. ON DUPLICATE KEY UPDATE b=4 might create a duplicate on another UNIQUE key.
				var uniqueViolationErr error
				// Check unique indexes
				for _, idx := range tbl.Def.Indexes {
					if !idx.Unique {
						continue
					}
					// Check if any unique index column was actually modified.
					idxModified := false
					for _, c := range idx.Columns {
						baseC := stripPrefixLengthFromCol(c)
						ov := fmt.Sprintf("%v", odduSnap[baseC])
						nv := fmt.Sprintf("%v", updatedRow[baseC])
						if ov != nv {
							idxModified = true
							break
						}
					}
					if !idxModified {
						continue
					}
					for j, other := range tbl.Rows {
						if j == dupIdx {
							continue
						}
						match := true
						vals := make([]string, 0, len(idx.Columns))
						for _, c := range idx.Columns {
							baseC := stripPrefixLengthFromCol(c)
							nv := updatedRow[baseC]
							ov := other[baseC]
							if nv == nil || ov == nil {
								match = false
								break
							}
							if fmt.Sprintf("%v", nv) != fmt.Sprintf("%v", ov) {
								match = false
								break
							}
							vals = append(vals, formatValForDupKey(nv))
						}
						if match {
							uniqueViolationErr = mysqlError(1062, "23000", fmt.Sprintf("Duplicate entry '%s' for key '%s'", strings.Join(vals, "-"), idx.Name))
							break
						}
					}
					if uniqueViolationErr != nil {
						break
					}
				}
				if uniqueViolationErr != nil {
					// Rollback the update by restoring the snapshot
					tbl.Rows[dupIdx] = odduSnap
					tbl.InvalidateIndexes()
					tbl.Unlock()
					// INSERT IGNORE suppresses duplicate key errors from ODKU too
					if bool(stmt.Ignore) && strings.Contains(uniqueViolationErr.Error(), "1062") {
						e.addWarning("Warning", 1062, uniqueViolationErr.Error())
						continue
					}
					return nil, uniqueViolationErr
				}
				tbl.InvalidateIndexes()
				tbl.Unlock()
				// MySQL counts ON DUPLICATE KEY UPDATE as 2 affected rows when a row is updated.
				affected += 2
				odkuCount++
				continue
			}
		}

		// Fill in default/auto_increment values before trigger so NEW.col works
		fullRow := make(storage.Row, len(row))
		for k, v := range row {
			fullRow[k] = v
		}
		// Add missing columns with defaults
		for _, col := range tbl.Def.Columns {
			if _, exists := fullRow[col.Name]; !exists {
				if col.AutoIncrement {
					// BEFORE INSERT triggers can read NEW.auto_col, but this must not
					// consume the counter before the actual insert.
					fullRow[col.Name] = tbl.AutoIncrement.Load() + 1
				} else if genExpr := generatedColumnExpr(col.Type); genExpr != "" {
					if v, err := e.evalGeneratedColumnExpr(genExpr, fullRow); err == nil {
						fullRow[col.Name] = v
					}
				} else if col.Default != nil {
					defVal := *col.Default
					defUpper := strings.ToUpper(defVal)
					// Evaluate dynamic defaults
					if defUpper == "CURRENT_TIMESTAMP" || defUpper == "CURRENT_TIMESTAMP()" ||
						defUpper == "NOW()" {
						defVal = e.nowTime().Format("2006-01-02 15:04:05")
					}
					fullRow[col.Name] = defVal
				} else if !col.Nullable {
					// NOT NULL columns without default get the type's zero value
					fullRow[col.Name] = implicitZeroValue(col.Type)
				} else {
					fullRow[col.Name] = nil
				}
			}
		}

		// Fire BEFORE INSERT triggers (may modify fullRow via SET NEW.col = val)
		if err := e.fireTriggers(tableName, "BEFORE", "INSERT", fullRow, nil); err != nil {
			return nil, err
		}

		// Apply trigger modifications back to the row being inserted
		// Only copy columns that were explicitly set by the user or modified by triggers
		for _, col := range tbl.Def.Columns {
			if col.AutoIncrement {
				continue // Don't override auto_increment handling
			}
			if v, ok := fullRow[col.Name]; ok {
				row[col.Name] = v
			}
		}

		// REPLACE: delete ALL existing duplicate rows (after BEFORE INSERT, before actual insert)
		// MySQL REPLACE deletes every row that conflicts on any unique/primary key with the
		// new row, then inserts the new row. We loop until no more duplicates remain.
		// We track the position of the first deleted row so we can reinsert at the same slot,
		// preserving MySQL's physical row ordering behavior.
		replaceAtIdx := -1
		if stmt.Action == sqlparser.ReplaceAct {
			for {
				dupIdx := e.findDuplicateRow(tbl, row, pkCols, uniqueCols)
				if dupIdx < 0 {
					break
				}
				tbl.Mu.RLock()
				oldRow := make(storage.Row, len(tbl.Rows[dupIdx]))
				for k, v := range tbl.Rows[dupIdx] {
					oldRow[k] = v
				}
				tbl.Mu.RUnlock()

				// Fire BEFORE DELETE trigger for the old row being replaced
				if err := e.fireTriggers(tableName, "BEFORE", "DELETE", nil, oldRow); err != nil {
					return nil, err
				}

				tbl.Lock()
				tbl.Rows = append(tbl.Rows[:dupIdx], tbl.Rows[dupIdx+1:]...)
				tbl.InvalidateIndexes()
				tbl.Unlock()
				affected++ // REPLACE counts each deleted row
				if replaceAtIdx < 0 {
					replaceAtIdx = dupIdx // remember position of first deleted row for reinsertion
				} else if dupIdx < replaceAtIdx {
					replaceAtIdx = dupIdx // track earliest position
				}

				// Fire AFTER DELETE trigger
				if err := e.fireTriggers(tableName, "AFTER", "DELETE", nil, oldRow); err != nil {
					return nil, err
				}
			}
		}

		// Strict mode validation before insert
		if e.isStrictMode() {
			for _, col := range tbl.Def.Columns {
				isAutoGenCol := col.AutoIncrement || isGeneratedColumnType(col.Type)
				// NOT NULL check
				if !col.Nullable && !isAutoGenCol {
					rv, exists := row[col.Name]
					// Check if column was explicitly specified in the INSERT
					explicitlySpecified := false
					for _, cn := range colNames {
						if strings.EqualFold(cn, col.Name) {
							explicitlySpecified = true
							break
						}
					}
					if !explicitlySpecified && col.Default == nil {
						// Column not specified and has no default -> error 1364
						if bool(stmt.Ignore) {
							e.addWarning("Warning", 1364, fmt.Sprintf("Field '%s' doesn't have a default value", col.Name))
							row[col.Name] = implicitZeroValue(col.Type)
						} else {
							return nil, mysqlError(1364, "HY000", fmt.Sprintf("Field '%s' doesn't have a default value", col.Name))
						}
					} else if exists && rv == nil && explicitlySpecified {
						if bool(stmt.Ignore) {
							e.addWarning("Warning", 1048, fmt.Sprintf("Column '%s' cannot be null", col.Name))
							row[col.Name] = implicitZeroValue(col.Type)
						} else {
							return nil, mysqlError(1048, "23000", fmt.Sprintf("Column '%s' cannot be null", col.Name))
						}
					}
				}
				rv, exists := row[col.Name]
				if exists && rv != nil {
					colUpper := strings.ToUpper(col.Type)
					isSpatialType := strings.Contains(colUpper, "POINT") || strings.Contains(colUpper, "LINESTRING") || strings.Contains(colUpper, "POLYGON") || strings.Contains(colUpper, "GEOMETRY") || strings.Contains(colUpper, "GEOMCOLLECTION")
					isIntType := !isSpatialType && (strings.Contains(colUpper, "INT") || strings.Contains(colUpper, "INTEGER"))
					isDecimalType := strings.Contains(colUpper, "DECIMAL") || strings.Contains(colUpper, "FLOAT") || strings.Contains(colUpper, "DOUBLE") || colUpper == "REAL" || strings.HasPrefix(colUpper, "REAL ")
					isNumericType := isIntType || isDecimalType
					isUnsigned := strings.Contains(colUpper, "UNSIGNED") || strings.Contains(colUpper, "ZEROFILL") // ZEROFILL implies UNSIGNED
					if isNumericType {
						switch val := rv.(type) {
						case int64:
							outOfRange := false
							if isUnsigned && val < 0 {
								outOfRange = true
							} else if isIntType {
								// Check signed/unsigned range by integer type
								min, max := insertIntTypeRange(colUpper)
								if val < min || val > max {
									outOfRange = true
								}
							}
							if outOfRange {
								if bool(stmt.Ignore) {
									e.addWarning("Warning", 1264, fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
									row[col.Name] = insertClampToIntTypeRange(val, colUpper)
								} else {
									return nil, mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
								}
							}
						case float64:
							outOfRangeF := false
							if isUnsigned && val < 0 {
								outOfRangeF = true
							} else if isIntType {
								minF, maxF := float64(math.MinInt64), float64(math.MaxInt64)
								rMin, rMax := insertIntTypeRange(colUpper)
								minF, maxF = float64(rMin), float64(rMax)
								if val < minF || val > maxF {
									outOfRangeF = true
								}
							}
							if outOfRangeF {
								if bool(stmt.Ignore) {
									e.addWarning("Warning", 1264, fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
									row[col.Name] = insertClampToIntTypeRangeFloat(val, colUpper)
								} else {
									return nil, mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
								}
							}
						case string:
							numText := strings.TrimSpace(val)
							if uq, uerr := strconv.Unquote(numText); uerr == nil {
								numText = strings.TrimSpace(uq)
							}
							if isIntType {
								lv := strings.ToLower(numText)
								if lv == "true" {
									row[col.Name] = int64(1)
									break
								}
								if lv == "false" {
									row[col.Name] = int64(0)
									break
								}
								if parsedInt, perr := strconv.ParseInt(numText, 10, 64); perr != nil {
									if _, perr := strconv.ParseFloat(numText, 64); perr != nil {
										if bool(stmt.Ignore) {
											e.addWarning("Warning", 1366, fmt.Sprintf("Incorrect integer value: '%s' for column '%s' at row 1", val, col.Name))
											row[col.Name] = int64(0)
											break
										}
										return nil, mysqlError(1366, "HY000", fmt.Sprintf("Incorrect integer value: '%s' for column '%s' at row 1", val, col.Name))
									}
								} else {
									// Check range for the specific integer type
									rMin, rMax := insertIntTypeRange(colUpper)
									if parsedInt < rMin || parsedInt > rMax {
										if bool(stmt.Ignore) {
											e.addWarning("Warning", 1264, fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
											row[col.Name] = insertClampToIntTypeRange(parsedInt, colUpper)
											break
										}
										return nil, mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
									}
								}
							} else if isDecimalType {
								if _, perr := strconv.ParseFloat(numText, 64); perr != nil {
									// Try to extract a valid numeric prefix (e.g. "123.4abc" -> "123.4")
									// MySQL behavior: truncate invalid suffix and emit Note 1265.
									// Exception: if the string ends with E/e not followed by digits (incomplete
									// scientific notation like "-100E"), MySQL emits "Incorrect decimal value" (1366).
									trimmedNum := strings.TrimSpace(numText)
									hasIncompleteExp := false
									if len(trimmedNum) > 0 {
										lastChar := trimmedNum[len(trimmedNum)-1]
										// Check if string ends with E/e (or E+/e+/e-/E-) without following digits
										if lastChar == 'E' || lastChar == 'e' || lastChar == '+' || lastChar == '-' {
											// ends with E, E+, E-, etc. → incomplete scientific notation
											for k := len(trimmedNum) - 1; k >= 0; k-- {
												ch := trimmedNum[k]
												if ch == 'E' || ch == 'e' {
													hasIncompleteExp = true
													break
												}
												if ch >= '0' && ch <= '9' {
													break // digit found before E: not an exponent issue
												}
											}
										}
									}
									if hasIncompleteExp {
										// Incomplete scientific notation → "Incorrect decimal value"
										if bool(stmt.Ignore) {
											e.addWarning("Warning", 1366, fmt.Sprintf("Incorrect decimal value: '%s' for column '%s' at row 1", val, col.Name))
											row[col.Name] = float64(0)
											break
										}
										return nil, mysqlError(1366, "HY000", fmt.Sprintf("Incorrect decimal value: '%s' for column '%s' at row 1", val, col.Name))
									}
									if prefix, ok := extractNumericPrefix(numText); ok && prefix != numText {
										e.addWarning("Note", 1265, fmt.Sprintf("Data truncated for column '%s' at row 1", col.Name))
										numText = prefix
										// fall through to use the truncated prefix
									} else {
										if bool(stmt.Ignore) {
											e.addWarning("Warning", 1366, fmt.Sprintf("Incorrect decimal value: '%s' for column '%s' at row 1", val, col.Name))
											row[col.Name] = float64(0)
											break
										}
										return nil, mysqlError(1366, "HY000", fmt.Sprintf("Incorrect decimal value: '%s' for column '%s' at row 1", val, col.Name))
									}
								}
							}
							// Check unsigned constraint for string-typed decimal values
							if isUnsigned {
								if f, perr := strconv.ParseFloat(numText, 64); perr == nil && f < 0 {
									if bool(stmt.Ignore) {
										e.addWarning("Warning", 1264, fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
										row[col.Name] = int64(0)
									} else {
										return nil, mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
									}
								}
							}
						}
						if isDecimalType && strings.Contains(colUpper, "DECIMAL") {
							if derr := checkDecimalRange(col.Type, rv); derr != nil {
								if bool(stmt.Ignore) {
									e.addWarning("Warning", 1264, fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
								} else {
									return nil, mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
								}
							}
						}
					}
					// String length check (use original value before padding/formatting)
					isCharType := strings.Contains(colUpper, "CHAR") || strings.Contains(colUpper, "BINARY")
					isBlobType := colUpper == "BLOB" || colUpper == "TINYBLOB" || colUpper == "MEDIUMBLOB" || colUpper == "LONGBLOB"
					isTextType := colUpper == "TEXT" || colUpper == "TINYTEXT" || colUpper == "MEDIUMTEXT" || colUpper == "LONGTEXT"
					if isCharType || isBlobType || isTextType {
						checkVal := rv
						if ov, ok := origValues[col.Name]; ok && ov != nil {
							checkVal = ov
						}
						// For BINARY/VARBINARY columns, convert integer hex literals to bytes
						isBinaryColType := strings.Contains(colUpper, "BINARY") || isBlobType
						if strings.Contains(colUpper, "BINARY") {
							if converted := hexIntToBytes(checkVal); converted != checkVal {
								checkVal = converted
							}
						}
						if sv, ok := checkVal.(string); ok {
							maxLen := extractCharLength(col.Type)
							// For binary/blob columns, use byte length (not rune length)
							var svLen int
							if isBinaryColType {
								svLen = len(sv)
							} else {
								// For non-binary CHAR/VARCHAR, determine effective charset.
								// For multi-byte non-UTF8 charsets (ucs2, utf16, utf32, big5, gbk, etc.)
								// or when the string contains non-UTF-8 bytes, we cannot accurately
								// count characters. Skip the length check in those cases.
								effectiveCharset := col.Charset
								if effectiveCharset == "" && tbl.Def != nil {
									effectiveCharset = tbl.Def.Charset
								}
								if effectiveCharset == "" {
									effectiveCharset = "utf8mb4"
								}
								effectiveCharset = strings.ToLower(effectiveCharset)
								isUtf8Compatible := effectiveCharset == "utf8" || effectiveCharset == "utf8mb4" ||
									effectiveCharset == "utf8mb3" || effectiveCharset == "ascii" ||
									effectiveCharset == "" || isSingleByteCharset(effectiveCharset)
								if !isUtf8Compatible || !utf8.ValidString(sv) {
									svLen = 0 // skip check for multi-byte charsets
								} else {
									svLen = len([]rune(sv))
								}
							}
							if maxLen > 0 && svLen > maxLen {
								// For BINARY/VARBINARY columns, any excess bytes cause a strict mode
								// error (unlike CHAR/VARCHAR where trailing spaces are allowed).
								if isBinaryColType {
									if bool(stmt.Ignore) || !e.isStrictMode() {
										// IGNORE or non-strict: truncate with warning
										row[col.Name] = sv[:maxLen]
										rv = row[col.Name]
										e.addWarning("Warning", 1265, fmt.Sprintf("Data truncated for column '%s' at row 1", col.Name))
									} else {
										return nil, mysqlError(1406, "22001", fmt.Sprintf("Data too long for column '%s' at row 1", col.Name))
									}
								} else {
									// For CHAR/VARCHAR: trailing spaces can be truncated with just a warning
									excess := string([]rune(sv)[maxLen:])
									onlySpaces := strings.TrimRight(excess, " ") == ""
									if onlySpaces {
										row[col.Name] = string([]rune(sv)[:maxLen])
										rv = row[col.Name]
										e.addWarning("Note", 1265, fmt.Sprintf("Data truncated for column '%s' at row 1", col.Name))
									} else if bool(stmt.Ignore) || !e.isStrictMode() {
										// INSERT IGNORE or non-strict mode: warn but do NOT truncate
										// (Dolt does not enforce VARCHAR length limits)
										e.addWarning("Warning", 1265, fmt.Sprintf("Data truncated for column '%s' at row 1", col.Name))
									} else {
										return nil, mysqlError(1406, "22001", fmt.Sprintf("Data too long for column '%s' at row 1", col.Name))
									}
								}
							}
						}
					}
					// UTF8 charset validation for string columns (TEXT, CHAR, VARCHAR, etc.)
					// If a string contains invalid UTF8 bytes and the column uses a UTF8 charset,
					// emit Warning 1366 and replace value with empty string.
					// Note: BLOB types store binary data and must NOT be UTF-8 validated.
					colTypeLower := strings.ToLower(col.Type)
					isBlobColType := colTypeLower == "blob" || colTypeLower == "tinyblob" || colTypeLower == "mediumblob" || colTypeLower == "longblob"
					isTextOrCharType := !isBlobColType && (strings.Contains(colTypeLower, "text") ||
						strings.Contains(colTypeLower, "char") ||
						strings.Contains(colTypeLower, "blob"))
					if isTextOrCharType && !strings.Contains(colTypeLower, "binary") {
						var rawBytes []byte
						var hasRawBytes bool
						isHexLiteral := false
						if s, ok := rv.(string); ok {
							rawBytes, hasRawBytes = []byte(s), true
						} else if hb, ok := rv.(HexBytes); ok {
							// HexBytes stores hex digits (e.g. "F4B8AD"), decode to raw bytes.
							decoded, err := hex.DecodeString(string(hb))
							if err == nil {
								rawBytes, hasRawBytes = decoded, true
								isHexLiteral = true
							}
						}
						if hasRawBytes {
							colCharset := col.Charset
							if colCharset == "" && tbl.Def != nil {
								colCharset = tbl.Def.Charset
							}
							colCharset = strings.ToLower(colCharset)
							isUtf8Charset := colCharset == "utf8" || colCharset == "utf8mb3" || colCharset == "utf8mb4"
							isAsciiCharset := colCharset == "ascii"
							rawStr := string(rawBytes)
							if isUtf8Charset && !utf8.ValidString(rawStr) {
								if e.isStrictMode() && isHexLiteral && !bool(stmt.Ignore) {
									// In strict mode, x'...' hex literals with invalid UTF-8 are errors.
									// String literals may use connection charset encoding; we only enforce
									// for hex literals where the binary content is explicit.
									// INSERT IGNORE converts errors to warnings.
									return nil, mysqlError(1300, "HY000", fmt.Sprintf("Incorrect string value: '%s' for column '%s' at row 1", formatBytesForWarning(rawStr), col.Name))
								}
								e.addWarning("Warning", 1366, fmt.Sprintf("Incorrect string value: '%s' for column '%s' at row 1", formatBytesForWarning(rawStr), col.Name))
								row[col.Name] = ""
							} else if isAsciiCharset && !isValidAsciiString(rawStr) {
								// ascii charset: bytes > 0x7F are invalid
								e.addWarning("Warning", 1300, fmt.Sprintf("Invalid ascii character string: '%s'", formatAsciiInvalidBytes(rawStr)))
							}
						}
					}
					// ENUM/SET validity check in strict mode
					isEnumType := strings.HasPrefix(strings.ToLower(col.Type), "enum(")
					isSetType := strings.HasPrefix(strings.ToLower(col.Type), "set(")
					if isEnumType || isSetType {
						// Count allowed values for numeric validation
						enumInner := ""
						if isEnumType {
							enumInner = col.Type[5 : len(col.Type)-1]
						} else {
							enumInner = col.Type[4 : len(col.Type)-1]
						}
						allowedCount := len(splitEnumValues(enumInner))

						sv, isStr := rv.(string)
						if !isStr {
							if ev, isEnum := rv.(EnumValue); isEnum {
								sv = string(ev)
								isStr = true
							}
						}
						if isStr {
							origStr := ""
							if ov, ok2 := origValues[col.Name]; ok2 {
								if os, ok3 := ov.(string); ok3 {
									origStr = os
								} else if oev, ok3 := ov.(EnumValue); ok3 {
									origStr = string(oev)
								}
							}
							// ENUM: empty string is invalid unless '' is explicitly in the allowed list
							if isEnumType && sv == "" {
								emptyAllowed := false
								for _, ev := range splitEnumValues(enumInner) {
									trimmed := strings.Trim(strings.TrimSpace(ev), "'")
									if trimmed == "" {
										emptyAllowed = true
										break
									}
								}
								if !emptyAllowed {
									if bool(stmt.Ignore) || !e.isTraditionalMode() {
										e.addWarning("Warning", 1265, fmt.Sprintf("Data truncated for column '%s' at row 1", col.Name))
									} else {
										return nil, mysqlError(1265, "01000", fmt.Sprintf("Data truncated for column '%s' at row 1", col.Name))
									}
								}
							}
							// SET: if some members were dropped during validation,
							// it means some members were invalid. Compare member counts
							// rather than string equality to handle case-insensitive
							// matches where the canonical form may differ from the input
							// (e.g., "Ä" vs "ä" with non-UTF-8 charsets).
							if isSetType && origStr != "" && sv != origStr {
								inputMembers := strings.Split(origStr, ",")
								validMembers := strings.Split(sv, ",")
								// A mismatch is only a truncation error if members were dropped
								membersDropped := len(validMembers) < len(inputMembers) ||
									(len(sv) == 0 && len(origStr) > 0)
								if membersDropped {
									if bool(stmt.Ignore) || !e.isStrictMode() {
										e.addWarning("Warning", 1265, fmt.Sprintf("Data truncated for column '%s' at row 1", col.Name))
									} else {
										return nil, mysqlError(1265, "01000", fmt.Sprintf("Data truncated for column '%s' at row 1", col.Name))
									}
								}
							}
						} else if nv, ok := rv.(int64); ok {
							// Numeric values: validate range for ENUM/SET
							if isEnumType && (nv < 0 || nv > int64(allowedCount)) {
								if bool(stmt.Ignore) || !e.isTraditionalMode() {
									e.addWarning("Warning", 1265, fmt.Sprintf("Data truncated for column '%s' at row 1", col.Name))
								} else {
									return nil, mysqlError(1265, "01000", fmt.Sprintf("Data truncated for column '%s' at row 1", col.Name))
								}
							}
							if isSetType {
								maxVal := int64((1 << allowedCount) - 1)
								if nv < 0 || nv > maxVal {
									if bool(stmt.Ignore) || !e.isStrictMode() {
										e.addWarning("Warning", 1265, fmt.Sprintf("Data truncated for column '%s' at row 1", col.Name))
									} else {
										return nil, mysqlError(1265, "01000", fmt.Sprintf("Data truncated for column '%s' at row 1", col.Name))
									}
								}
							}
						}
					}
				}
			}
		}

		// Non-strict mode: warn about strings that exceed column length but do NOT truncate
		// (Dolt does not enforce VARCHAR/CHAR length limits)
		if !e.isStrictMode() {
			for _, col := range tbl.Def.Columns {
				rv, exists := row[col.Name]
				if !exists || rv == nil {
					continue
				}
				colUpper := strings.ToUpper(col.Type)
				isCharType := strings.Contains(colUpper, "CHAR") || strings.Contains(colUpper, "BINARY")
				if !isCharType {
					continue
				}
				sv, ok := rv.(string)
				if !ok {
					continue
				}
				maxLen := extractCharLength(col.Type)
				runeLen := 0
				effectiveCs := col.Charset
				if effectiveCs == "" && tbl.Def != nil {
					effectiveCs = tbl.Def.Charset
				}
				if effectiveCs == "" {
					effectiveCs = "utf8mb4"
				}
				effectiveCs = strings.ToLower(effectiveCs)
				isUtf8Cs := effectiveCs == "utf8" || effectiveCs == "utf8mb4" ||
					effectiveCs == "utf8mb3" || effectiveCs == "ascii" ||
					effectiveCs == "" || isSingleByteCharset(effectiveCs)
				if isUtf8Cs && utf8.ValidString(sv) {
					runeLen = len([]rune(sv))
				}
				if maxLen > 0 && runeLen > maxLen {
					e.addWarning("Warning", 1265, fmt.Sprintf("Data truncated for column '%s' at row 1", col.Name))
				}
			}
		}

		// Enforce CHECK constraints
		if tbl.Def != nil && len(tbl.Def.CheckConstraints) > 0 {
			checkViolated := false
			for _, cc := range tbl.Def.CheckConstraints {
				checkResult, err := e.evaluateCheckConstraint(cc.Expr, row)
				if err != nil {
					continue // if we can't evaluate, skip
				}
				if !checkResult {
					if bool(stmt.Ignore) {
						// INSERT IGNORE: add warning and skip this row
						e.addWarning("Warning", 3819, fmt.Sprintf("Check constraint '%s' is violated.", cc.Name))
						checkViolated = true
						break
					}
					return nil, mysqlError(3819, "HY000", fmt.Sprintf("Check constraint '%s' is violated.", cc.Name))
				}
			}
			if checkViolated {
				continue
			}
		}

		// Enforce view WITH CHECK OPTION: the inserted/replaced row must satisfy
		// the view's WHERE condition.
		if viewCheckExpr != nil {
			match, err := e.evalWhere(viewCheckExpr, row)
			if err != nil || !match {
				if bool(stmt.Ignore) {
					// INSERT IGNORE: skip row and add warning
					e.addWarning("Warning", 1369, fmt.Sprintf("CHECK OPTION failed '%s.%s'", e.CurrentDB, originalViewName))
					continue
				}
				return nil, mysqlError(1369, "HY000", fmt.Sprintf("CHECK OPTION failed '%s.%s'", e.CurrentDB, originalViewName))
			}
		}

		// Enforce FOREIGN KEY constraints: verify parent row exists
		if err := e.checkForeignKeyOnInsert(insertDB, tableName, row); err != nil {
			if bool(stmt.Ignore) {
				// INSERT IGNORE: skip row and add warning
				e.addWarning("Warning", 1452, err.Error())
				continue
			}
			return nil, err
		}

		noAutoValueOnZero := strings.Contains(e.sqlMode, "NO_AUTO_VALUE_ON_ZERO")
		autoGeneratedThisRow := false
		if autoColName != "" {
			if v, exists := row[autoColName]; !exists || v == nil {
				autoGeneratedThisRow = true
			} else {
				isZeroVal := false
				switch av := v.(type) {
				case int64:
					isZeroVal = av == 0
				case uint64:
					isZeroVal = av == 0
				case float64:
					isZeroVal = int64(av) == 0
				case string:
					isZeroVal = strings.TrimSpace(av) == "" || strings.TrimSpace(av) == "0"
				}
				// With NO_AUTO_VALUE_ON_ZERO, explicit 0 is stored as-is, not as auto-increment
				autoGeneratedThisRow = isZeroVal && !noAutoValueOnZero
			}
		}

		// Acquire row lock on the PK value being inserted.
		// This blocks if another transaction already holds a lock on the same
		// PK (e.g., another uncommitted INSERT with the same PK value).
		if e.rowLockManager != nil && len(pkCols) > 0 && e.shouldAcquireRowLocks() {
			lockKey := buildRowLockKey(insertDB, tableName, pkCols, row)
			lockTimeout := 50.0
			if v, ok := e.getSysVar("innodb_lock_wait_timeout"); ok {
				if t, tErr := strconv.ParseFloat(v, 64); tErr == nil {
					lockTimeout = t
				}
			}
			if rlErr := e.rowLockManager.AcquireRowLock(e.connectionID, lockKey, lockTimeout); rlErr != nil {
				e.handleRollbackOnTimeout()
				return nil, mysqlError(1205, "HY000", "Lock wait timeout exceeded; try restarting transaction")
			}
		}

		// With innodb_autoinc_lock_mode=0, failed inserts should not
		// consume auto-increment values. Save and restore on failure.
		var savedAutoInc2 int64
		lockMode0_2 := false
		if autoGeneratedThisRow {
			if lm, ok := e.getSysVar("innodb_autoinc_lock_mode"); ok && lm == "0" {
				lockMode0_2 = true
				savedAutoInc2 = tbl.AutoIncrementValue()
			}
		}
		// Tag row with connection ID for transaction isolation
		if e.inTransaction && e.txnActiveSet != nil {
			row["__txn_conn_id__"] = e.connectionID
		}

		// In strict mode, pre-validate auto_increment value range.
		// MySQL computes the next AI value using auto_increment_increment and
		// auto_increment_offset. If the resulting value exceeds the column type's
		// range, strict mode must reject the insert.
		if autoGeneratedThisRow && e.isStrictMode() && autoColName != "" {
			// Compute the next AI value respecting auto_increment_increment/offset
			cur := tbl.AutoIncrementValue()
			aiIncrement := int64(1)
			aiOffset := int64(1)
			if v, ok := e.getSysVar("auto_increment_increment"); ok {
				if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
					aiIncrement = n
				}
			}
			if v, ok := e.getSysVar("auto_increment_offset"); ok {
				if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
					aiOffset = n
				}
			}
			// Compute next value: smallest value >= cur+1 such that (value - offset) % increment == 0
			nextAI := cur + 1
			if aiIncrement > 1 || aiOffset != 1 {
				// Align nextAI to the sequence offset + k*increment
				base := aiOffset
				if nextAI > base {
					rem := (nextAI - base) % aiIncrement
					if rem != 0 {
						nextAI = nextAI + (aiIncrement - rem)
					}
				} else {
					nextAI = base
				}
			}
			// Find the auto_increment column def and check range
			for _, col := range tbl.Def.Columns {
				if col.AutoIncrement && strings.EqualFold(col.Name, autoColName) {
					if checkErr := checkIntegerStrict(col.Type, col.Name, nextAI); checkErr != nil {
						if bool(stmt.Ignore) {
							e.addWarning("Warning", 1264, fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
							// Clamp to max
							_, maxVal := insertIntTypeRange(strings.ToUpper(col.Type))
							row[col.Name] = maxVal
							autoGeneratedThisRow = false // value is now explicit
						} else {
							return nil, checkErr
						}
					}
					break
				}
			}
		}

		id, err := tbl.Insert(row, noAutoValueOnZero)
		if err != nil {
			if lockMode0_2 {
				tbl.AutoIncrement.Store(savedAutoInc2)
			}
			delete(row, "__txn_conn_id__")
			// INSERT IGNORE: silently skip duplicate key errors
			if bool(stmt.Ignore) && strings.Contains(err.Error(), "1062") {
				continue
			}
			if strings.Contains(err.Error(), "Failed to read auto-increment value from storage engine") {
				return nil, mysqlError(1467, "HY000", "Failed to read auto-increment value from storage engine")
			}
			return nil, err
		}
		// Record undo entry for transaction rollback
		if e.inTransaction {
			e.txnUndoLog = append(e.txnUndoLog, undoEntry{
				op:     "INSERT",
				db:     insertDB,
				table:  tableName,
				oldRow: storage.CloneRow(row),
			})
		}
		lastInsertID = id
		if autoGeneratedThisRow && firstAutoInsertID == 0 && id > 0 {
			firstAutoInsertID = id
		}
		affected++

		// For REPLACE: move the newly inserted row to the position of the deleted row
		// to preserve MySQL's physical row ordering (new row occupies the same slot).
		if replaceAtIdx >= 0 {
			tbl.Lock()
			newIdx := len(tbl.Rows) - 1
			if replaceAtIdx < newIdx {
				// Move the row from the end to replaceAtIdx by shifting rows
				moved := tbl.Rows[newIdx]
				copy(tbl.Rows[replaceAtIdx+1:newIdx+1], tbl.Rows[replaceAtIdx:newIdx])
				tbl.Rows[replaceAtIdx] = moved
				tbl.InvalidateIndexes()
			}
			tbl.Unlock()
			replaceAtIdx = -1 // reset for next iteration
		}

		// Fire AFTER INSERT triggers
		if err := e.fireTriggers(tableName, "AFTER", "INSERT", fullRow, nil); err != nil {
			return nil, err
		}
	}

	if firstAutoInsertID > 0 {
		lastInsertID = firstAutoInsertID
	}
	// Only overwrite e.lastInsertID if an auto-increment insert occurred.
	// When only ON DUPLICATE KEY UPDATE fired (no new rows), preserve any
	// value that LAST_INSERT_ID(expr) set during the UPDATE clause evaluation.
	if lastInsertID > 0 {
		e.lastInsertID = lastInsertID
	}

	// evaluateCheckConstraint is defined below.

	// Set lastAutoIncID for sql_auto_is_null.
	// Only set for NOT NULL auto-increment columns (MySQL behavior:
	// sql_auto_is_null only applies when the auto-increment column is NOT NULL).
	if lastInsertID > 0 {
		for _, col := range tbl.Def.Columns {
			if col.AutoIncrement {
				if !col.Nullable {
					e.lastAutoIncID = lastInsertID
				}
				break
			}
		}
	}

	// Auto-recalc InnoDB persistent stats after DML (MySQL 10% threshold)
	if affected > 0 {
		if db, dbErr := e.Catalog.GetDatabase(insertDB); dbErr == nil {
			if def, defErr := db.GetTable(tableName); defErr == nil && e.innodbStatsAutoRecalcEnabled(def) && e.innodbStatsPersistentEnabled(def) {
				e.maybeRecalcStats(insertDB, tableName, int64(affected))
			}
		}
	}

	// Build info message for INSERT/REPLACE with Records/Duplicates counts.
	// This is used by the mtrrunner to emit the "info: Records: N  Duplicates: M  Warnings: 0" line.
	var infoMsg string
	if stmt.Action == sqlparser.InsertAct || stmt.Action == sqlparser.ReplaceAct {
		duplicates := odkuCount
		if stmt.Action == sqlparser.ReplaceAct {
			// For REPLACE, duplicates = number of deleted+reinserted rows
			duplicates = affected - totalRows
			if totalRows > affected {
				duplicates = 0
			}
		}
		infoMsg = fmt.Sprintf("Records: %d  Duplicates: %d  Warnings: 0", totalRows, duplicates)
	}
	e.lastInsertInfo = infoMsg

	return &Result{
		AffectedRows: affected,
		InsertID:     uint64(lastInsertID),
		InfoMessage:  infoMsg,
	}, nil
}

func generatedColumnExpr(colType string) string {
	upper := strings.ToUpper(colType)
	const marker = " GENERATED ALWAYS AS ("
	start := strings.Index(upper, marker)
	if start < 0 {
		return ""
	}
	i := start + len(marker)
	depth := 1
	for ; i < len(colType); i++ {
		switch colType[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return strings.TrimSpace(colType[start+len(marker) : i])
			}
		}
	}
	return ""
}

func isGeneratedColumnType(colType string) bool {
	return generatedColumnExpr(colType) != ""
}

func (e *Executor) evalGeneratedColumnExpr(expr string, row storage.Row) (interface{}, error) {
	stmt, err := e.parser().Parse("SELECT " + expr)
	if err != nil {
		return nil, err
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok || len(sel.SelectExprs.Exprs) != 1 {
		return nil, fmt.Errorf("invalid generated column expression")
	}
	aliased, ok := sel.SelectExprs.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("invalid generated column expression")
	}
	return e.evalRowExpr(aliased.Expr, row)
}

// blockedGcolFunctions is the set of functions not allowed in generated column expressions.
var blockedGcolFunctions = map[string]string{
	"rand":                  "rand",
	"load_file":             "load_file",
	"curdate":               "curdate",
	"current_date":          "curdate",
	"curtime":               "curtime",
	"current_time":          "curtime",
	"now":                   "now",
	"current_timestamp":     "now",
	"localtime":             "now",
	"localtimestamp":        "now",
	"unix_timestamp":        "unix_timestamp",
	"utc_date":              "utc_date",
	"utc_time":              "utc_time",
	"utc_timestamp":         "utc_timestamp",
	"uuid":                  "uuid",
	"uuid_short":            "uuid_short",
	"connection_id":         "connection_id",
	"current_user":          "current_user",
	"found_rows":            "found_rows",
	"last_insert_id":        "last_insert_id",
	"row_count":             "row_count",
	"session_user":          "user",
	"system_user":           "user",
	"user":                  "user",
	"version":               "version()",
	"sleep":                 "sleep",
	"sysdate":               "sysdate",
	"statement_digest":      "statement_digest",
	"statement_digest_text": "statement_digest_text",
	"get_lock":              "get_lock",
	"is_free_lock":          "is_free_lock",
	"is_used_lock":          "is_used_lock",
	"release_lock":          "release_lock",
	"release_all_locks":     "release_all_locks",
	"benchmark":             "benchmark",
	"name_const":            "",
	"values":                "values",
	"database":              "database",
	"schema":                "database",
	"master_pos_wait":       "master_pos_wait",
	"source_pos_wait":       "source_pos_wait",
	"encrypt":               "`encrypt`",
	"updatexml":             "updatexml",
	"json_merge":            "json_merge",
}

// findBlockedFunctionInExpr walks an expression tree and returns the name of the
// first disallowed function found, or "" if none.
func findBlockedFunctionInExpr(expr sqlparser.Expr) (string, bool) {
	var blocked string
	found := false
	sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if found {
			return false, nil
		}
		switch v := node.(type) {
		case *sqlparser.FuncExpr:
			name := strings.ToLower(v.Name.String())
			if b, ok := blockedGcolFunctions[name]; ok {
				blocked = b
				found = true
				return false, nil
			}
		case *sqlparser.CurTimeFuncExpr:
			name := strings.ToLower(v.Name.String())
			if b, ok := blockedGcolFunctions[name]; ok {
				blocked = b
				found = true
				return false, nil
			}
		case *sqlparser.LockingFunc:
			name := strings.ToLower(sqlparser.String(v))
			if idx := strings.Index(name, "("); idx > 0 {
				name = strings.TrimSpace(name[:idx])
			}
			if b, ok := blockedGcolFunctions[name]; ok {
				blocked = b
				found = true
				return false, nil
			}
		case *sqlparser.ValuesFuncExpr:
			blocked = "values"
			found = true
			return false, nil
		}
		return true, nil
	}, expr)
	return blocked, found
}

func (e *Executor) populateGeneratedColumns(row storage.Row, cols []catalog.ColumnDef) error {
	for _, col := range cols {
		expr := generatedColumnExpr(col.Type)
		if expr == "" {
			continue
		}
		// Always re-evaluate generated column expressions (virtual or stored).
		// The expression result must override any implicit zero value that was
		// set when processing the DEFAULT keyword for this column.
		v, err := e.evalGeneratedColumnExpr(expr, row)
		if err != nil {
			return err
		}
		row[col.Name] = v
	}
	return nil
}

// findDuplicateRow returns the index of an existing row in tbl that has the same
// primary key or unique key value as the candidate row. Returns -1 if no duplicate found.
func (e *Executor) findDuplicateRow(tbl *storage.Table, candidate storage.Row, pkCols, uniqueCols []string) int {
	tbl.Mu.RLock()
	defer tbl.Mu.RUnlock()

	// MySQL checks constraints in order: PRIMARY KEY first across all rows,
	// then each UNIQUE key in definition order across all rows.
	// This ensures that when multiple keys conflict with different rows,
	// the PRIMARY KEY match takes precedence.

	// 1. Check primary key match across all rows first.
	if len(pkCols) > 0 {
		for i, existing := range tbl.Rows {
			match := true
			for _, col := range pkCols {
				baseCol := stripPrefixLengthFromCol(col)
				cv, cok := candidate[baseCol]
				ev, eok := existing[baseCol]
				if !cok || !eok || cv == nil || ev == nil {
					match = false
					break
				}
				if fmt.Sprintf("%v", cv) != fmt.Sprintf("%v", ev) {
					match = false
					break
				}
			}
			if match {
				return i
			}
		}
	}

	// 2. Check each unique key in definition order across all rows.
	// Single-column unique keys
	for _, col := range uniqueCols {
		baseCol := stripPrefixLengthFromCol(col)
		cv, cok := candidate[baseCol]
		if !cok || cv == nil {
			continue
		}
		cvStr := fmt.Sprintf("%v", cv)
		// Get column type to determine if PAD SPACE comparison applies
		colType := ""
		for _, c := range tbl.Def.Columns {
			if strings.EqualFold(c.Name, baseCol) {
				colType = strings.ToUpper(c.Type)
				break
			}
		}
		isPadSpace := strings.Contains(colType, "CHAR") && !strings.Contains(colType, "BINARY")
		for i, existing := range tbl.Rows {
			ev, eok := existing[baseCol]
			if eok && ev != nil {
				evStr := fmt.Sprintf("%v", ev)
				if isPadSpace {
					if strings.TrimRight(cvStr, " ") == strings.TrimRight(evStr, " ") {
						return i
					}
				} else if cvStr == evStr {
					return i
				}
			}
		}
	}

	// 3. Check multi-column unique indexes in definition order.
	for _, idx := range tbl.Def.Indexes {
		if !idx.Unique || len(idx.Columns) <= 1 {
			continue
		}
		for i, existing := range tbl.Rows {
			match := true
			for _, col := range idx.Columns {
				baseCol := stripPrefixLengthFromCol(col)
				cv, cok := candidate[baseCol]
				ev, eok := existing[baseCol]
				if !cok || !eok || cv == nil || ev == nil {
					match = false
					break
				}
				cvStr := fmt.Sprintf("%v", cv)
				evStr := fmt.Sprintf("%v", ev)
				// For string columns, use PAD SPACE comparison (trim trailing spaces)
				colType := ""
				for _, c := range tbl.Def.Columns {
					if strings.EqualFold(c.Name, baseCol) {
						colType = strings.ToUpper(c.Type)
						break
					}
				}
				isPadSpace := strings.Contains(colType, "CHAR") && !strings.Contains(colType, "BINARY")
				var valuesMatch bool
				if isPadSpace {
					valuesMatch = strings.TrimRight(cvStr, " ") == strings.TrimRight(evStr, " ")
				} else {
					valuesMatch = cvStr == evStr
				}
				if !valuesMatch {
					match = false
					break
				}
			}
			if match {
				return i
			}
		}
	}
	return -1
}

// evaluateCheckConstraint evaluates a CHECK constraint expression against a row.
// Returns true if the constraint is satisfied, false otherwise.
func (e *Executor) evaluateCheckConstraint(exprStr string, row storage.Row) (bool, error) {
	// Parse the expression
	selectSQL := "SELECT " + exprStr
	parsed, err := e.parser().Parse(selectSQL)
	if err != nil {
		return true, err // can't parse, assume valid
	}
	sel, ok := parsed.(*sqlparser.Select)
	if !ok || len(sel.SelectExprs.Exprs) == 0 {
		return true, fmt.Errorf("could not parse check expression")
	}
	ae, ok := sel.SelectExprs.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return true, fmt.Errorf("could not parse check expression")
	}
	// Evaluate the expression with row context
	val, err := e.evalRowExpr(ae.Expr, row)
	if err != nil {
		return true, err
	}
	if val == nil {
		return true, nil // NULL result means constraint is satisfied (MySQL behavior)
	}
	return isTruthy(val), nil
}

// formatValForDupKey formats a value for use in a Duplicate Entry error message.
// Binary values have non-printable/non-ASCII bytes escaped as \xNN (uppercase hex).
func formatValForDupKey(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	s := fmt.Sprintf("%v", v)
	hasNonPrintable := false
	for i := 0; i < len(s); i++ {
		b := s[i]
		if b < 0x20 || b >= 0x7F {
			hasNonPrintable = true
			break
		}
	}
	if !hasNonPrintable {
		return s
	}
	var result strings.Builder
	for i := 0; i < len(s); i++ {
		b := s[i]
		if b < 0x20 || b >= 0x7F {
			fmt.Fprintf(&result, "\\x%02X", b)
		} else {
			result.WriteByte(b)
		}
	}
	return result.String()
}

// formatBytesForWarning formats the invalid bytes in a string as MySQL does in Warning 1366:
// e.g. '\xFF' or '\xEF\xBF' showing the first few invalid bytes.
func formatBytesForWarning(s string) string {
	// Find the first invalid UTF8 sequence and show it
	var result strings.Builder
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size <= 1 {
			// Invalid byte - show it as \xNN
			fmt.Fprintf(&result, "\\x%02X", s[i])
			i++
			// Show a few more invalid bytes if present
			count := 1
			for i < len(s) && count < 4 {
				r2, size2 := utf8.DecodeRuneInString(s[i:])
				if r2 == utf8.RuneError && size2 <= 1 {
					fmt.Fprintf(&result, "\\x%02X", s[i])
					i++
					count++
				} else {
					break
				}
			}
			break
		}
		i += size
	}
	if result.Len() == 0 {
		return s
	}
	return result.String()
}

// isValidAsciiString returns true if all bytes in s are valid ASCII (0x00-0x7F).
func isValidAsciiString(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] > 0x7F {
			return false
		}
	}
	return true
}

// formatAsciiInvalidBytes formats a string with invalid ASCII bytes for Warning 1300.
// MySQL shows: first non-ASCII byte as \xNN, then remaining bytes as printable chars.
// Example: x'8142' → '\x81B'
func formatAsciiInvalidBytes(s string) string {
	var result strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] > 0x7F {
			fmt.Fprintf(&result, "\\x%02X", s[i])
		} else {
			result.WriteByte(s[i])
		}
	}
	return result.String()
}

// insertIntTypeRange returns (min, max) int64 valid range for the given integer column type.
func insertIntTypeRange(colUpper string) (int64, int64) {
	isUnsigned := strings.Contains(colUpper, "UNSIGNED")
	base := colUpper
	if i := strings.IndexByte(base, '('); i >= 0 {
		base = base[:i]
	}
	base = strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(base, "UNSIGNED", ""), "ZEROFILL", ""))
	base = strings.TrimSpace(base)
	switch base {
	case "TINYINT":
		if isUnsigned {
			return 0, 255
		}
		return -128, 127
	case "SMALLINT":
		if isUnsigned {
			return 0, 65535
		}
		return -32768, 32767
	case "MEDIUMINT":
		if isUnsigned {
			return 0, 16777215
		}
		return -8388608, 8388607
	case "INT", "INTEGER":
		if isUnsigned {
			return 0, 4294967295
		}
		return -2147483648, 2147483647
	case "BIGINT":
		if isUnsigned {
			return 0, math.MaxInt64 // can't represent MaxUint64 in int64
		}
		return math.MinInt64, math.MaxInt64
	}
	return math.MinInt64, math.MaxInt64
}

// insertClampToIntTypeRange clamps val to the valid range for colUpper.
func insertClampToIntTypeRange(val int64, colUpper string) int64 {
	min, max := insertIntTypeRange(colUpper)
	if val < min {
		return min
	}
	if val > max {
		return max
	}
	return val
}

// insertClampToIntTypeRangeFloat clamps float64 val to integer type range.
func insertClampToIntTypeRangeFloat(val float64, colUpper string) int64 {
	min, max := insertIntTypeRange(colUpper)
	minF, maxF := float64(min), float64(max)
	if val < minF {
		return min
	}
	if val > maxF {
		return max
	}
	return int64(val)
}
