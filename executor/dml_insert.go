package executor

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

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
	if baseTable, isView, err := e.resolveViewToBaseTable(tableName); err != nil {
		return nil, err
	} else if isView {
		tableName = baseTable
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

	// Get column names
	colNames := make([]string, len(stmt.Columns))
	for i, col := range stmt.Columns {
		colNames[i] = col.String()
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
					v = coerceColumnValue(cm.col.Type, v)
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
			for _, row := range bulkRows {
				autoGeneratedThisRow := false
				if autoColName != "" {
					if v, exists := row[autoColName]; !exists || v == nil {
						autoGeneratedThisRow = true
					} else {
						switch av := v.(type) {
						case int64:
							autoGeneratedThisRow = av == 0
						case uint64:
							autoGeneratedThisRow = av == 0
						case float64:
							autoGeneratedThisRow = int64(av) == 0
						case string:
							autoGeneratedThisRow = strings.TrimSpace(av) == "" || strings.TrimSpace(av) == "0"
						}
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
				id, err := tbl.Insert(row)
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
		e.lastInsertID = lastInsertID

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
	autoColName := ""
	for _, col := range tbl.Def.Columns {
		if col.AutoIncrement {
			autoColName = col.Name
			break
		}
	}

	for _, valTuple := range rows {
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
						}
						break
					}
				}
			}
			if err != nil {
				var intOvErr *intOverflowError
				if errors.As(err, &intOvErr) {
					// For DECIMAL/FLOAT/DOUBLE columns, parse overflow as float
					isDecCol := false
					overflowStr := intOvErr.val
					for _, col := range tbl.Def.Columns {
						if col.Name == colNames[i] {
							colUpper := strings.ToUpper(col.Type)
							if strings.Contains(colUpper, "DECIMAL") || strings.Contains(colUpper, "FLOAT") || strings.Contains(colUpper, "DOUBLE") {
								isDecCol = true
							}
							break
						}
					}
					if isDecCol {
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
							isDecType := strings.Contains(colUpper, "DECIMAL") || strings.Contains(colUpper, "FLOAT") || strings.Contains(colUpper, "DOUBLE")
							if isDecType {
								if strings.Contains(colUpper, "UNSIGNED") {
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
						}
					}
					v = coerceColumnValue(col.Type, v)
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
				prevOnDupValuesRow := e.onDupValuesRow
				e.onDupValuesRow = row
				for _, upd := range stmt.OnDup {
					colName := upd.Name.Name.String()
					val, err := e.evalExpr(upd.Expr)
					if err != nil {
						e.onDupValuesRow = prevOnDupValuesRow
						tbl.Unlock()
						return nil, err
					}
					// Pad BINARY(N) values, coerce DATE/TIME.
					for _, col := range tbl.Def.Columns {
						if col.Name == colName {
							val = coerceColumnValue(col.Type, val)
							break
						}
					}
					tbl.Rows[dupIdx][colName] = val
				}
				e.onDupValuesRow = prevOnDupValuesRow
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
				// Apply ON UPDATE CURRENT_TIMESTAMP for columns with that property
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
				tbl.InvalidateIndexes()
				tbl.Unlock()
				// MySQL counts ON DUPLICATE KEY UPDATE as 2 affected rows when a row is updated.
				affected += 2
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

		// REPLACE: delete existing duplicate row (after BEFORE INSERT, before actual insert)
		if stmt.Action == sqlparser.ReplaceAct {
			dupIdx := e.findDuplicateRow(tbl, row, pkCols, uniqueCols)
			if dupIdx >= 0 {
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
				affected++ // REPLACE counts deleted row + inserted row = 2

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
					isDecimalType := strings.Contains(colUpper, "DECIMAL") || strings.Contains(colUpper, "FLOAT") || strings.Contains(colUpper, "DOUBLE")
					isNumericType := isIntType || isDecimalType
					isUnsigned := strings.Contains(colUpper, "UNSIGNED")
					if isNumericType {
						switch val := rv.(type) {
						case int64:
							if isUnsigned && val < 0 {
								if bool(stmt.Ignore) {
									e.addWarning("Warning", 1264, fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
									row[col.Name] = int64(0)
								} else {
									return nil, mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
								}
							}
						case float64:
							if isUnsigned && val < 0 {
								if bool(stmt.Ignore) {
									e.addWarning("Warning", 1264, fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
									row[col.Name] = int64(0)
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
								if _, perr := strconv.ParseInt(numText, 10, 64); perr != nil {
									if _, perr := strconv.ParseFloat(numText, 64); perr != nil {
										if bool(stmt.Ignore) {
											e.addWarning("Warning", 1366, fmt.Sprintf("Incorrect integer value: '%s' for column '%s' at row 1", val, col.Name))
											row[col.Name] = int64(0)
											break
										}
										return nil, mysqlError(1366, "HY000", fmt.Sprintf("Incorrect integer value: '%s' for column '%s' at row 1", val, col.Name))
									}
								}
							} else if isDecimalType {
								if _, perr := strconv.ParseFloat(numText, 64); perr != nil {
									if bool(stmt.Ignore) {
										e.addWarning("Warning", 1366, fmt.Sprintf("Incorrect decimal value: '%s' for column '%s' at row 1", val, col.Name))
										row[col.Name] = float64(0)
										break
									}
									return nil, mysqlError(1366, "HY000", fmt.Sprintf("Incorrect decimal value: '%s' for column '%s' at row 1", val, col.Name))
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
					if isCharType {
						checkVal := rv
						if ov, ok := origValues[col.Name]; ok && ov != nil {
							checkVal = ov
						}
						if sv, ok := checkVal.(string); ok {
							maxLen := extractCharLength(col.Type)
							if maxLen > 0 && len([]rune(sv)) > maxLen {
								if bool(stmt.Ignore) {
									// INSERT IGNORE: truncate the value instead of error
									row[col.Name] = string([]rune(sv)[:maxLen])
									rv = row[col.Name]
								} else {
									return nil, mysqlError(1406, "22001", fmt.Sprintf("Data too long for column '%s' at row 1", col.Name))
								}
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
							// ENUM: empty string is invalid (not in the allowed list)
							if isEnumType && sv == "" {
								if bool(stmt.Ignore) {
									e.addWarning("Warning", 1265, fmt.Sprintf("Data truncated for column '%s' at row 1", col.Name))
								} else {
									return nil, mysqlError(1265, "01000", fmt.Sprintf("Data truncated for column '%s' at row 1", col.Name))
								}
							}
							// SET: if the validated value differs from original,
							// it means some members were invalid
							if isSetType && origStr != "" && sv != origStr {
								if bool(stmt.Ignore) {
									e.addWarning("Warning", 1265, fmt.Sprintf("Data truncated for column '%s' at row 1", col.Name))
								} else {
									return nil, mysqlError(1265, "01000", fmt.Sprintf("Data truncated for column '%s' at row 1", col.Name))
								}
							}
						} else if nv, ok := rv.(int64); ok {
							// Numeric values: validate range for ENUM/SET
							if isEnumType && (nv < 0 || nv > int64(allowedCount)) {
								if bool(stmt.Ignore) {
									e.addWarning("Warning", 1265, fmt.Sprintf("Data truncated for column '%s' at row 1", col.Name))
								} else {
									return nil, mysqlError(1265, "01000", fmt.Sprintf("Data truncated for column '%s' at row 1", col.Name))
								}
							}
							if isSetType {
								maxVal := int64((1 << allowedCount) - 1)
								if nv < 0 || nv > maxVal {
									if bool(stmt.Ignore) {
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

		// Enforce CHECK constraints
		if tbl.Def != nil && len(tbl.Def.CheckConstraints) > 0 {
			for _, cc := range tbl.Def.CheckConstraints {
				checkResult, err := e.evaluateCheckConstraint(cc.Expr, row)
				if err != nil {
					continue // if we can't evaluate, skip
				}
				if !checkResult {
					return nil, mysqlError(3819, "HY000", fmt.Sprintf("Check constraint '%s' is violated.", cc.Name))
				}
			}
		}

		// Enforce FOREIGN KEY constraints: verify parent row exists
		if err := e.checkForeignKeyOnInsert(insertDB, tableName, row); err != nil {
			return nil, err
		}

		autoGeneratedThisRow := false
		if autoColName != "" {
			if v, exists := row[autoColName]; !exists || v == nil {
				autoGeneratedThisRow = true
			} else {
				switch av := v.(type) {
				case int64:
					autoGeneratedThisRow = av == 0
				case uint64:
					autoGeneratedThisRow = av == 0
				case float64:
					autoGeneratedThisRow = int64(av) == 0
				case string:
					autoGeneratedThisRow = strings.TrimSpace(av) == "" || strings.TrimSpace(av) == "0"
				}
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
		id, err := tbl.Insert(row)
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

		// Fire AFTER INSERT triggers
		if err := e.fireTriggers(tableName, "AFTER", "INSERT", fullRow, nil); err != nil {
			return nil, err
		}
	}

	if firstAutoInsertID > 0 {
		lastInsertID = firstAutoInsertID
	}
	e.lastInsertID = lastInsertID

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

	return &Result{
		AffectedRows: affected,
		InsertID:     uint64(lastInsertID),
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
		// Always evaluate the generated column expression, even if the key
		// already exists with a nil value (e.g., DEFAULT was specified).
		// Only skip if a non-nil value was explicitly provided.
		if val, exists := row[col.Name]; exists && val != nil {
			continue
		}
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
		for i, existing := range tbl.Rows {
			ev, eok := existing[baseCol]
			if eok && ev != nil && fmt.Sprintf("%v", cv) == fmt.Sprintf("%v", ev) {
				return i
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
