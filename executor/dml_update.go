package executor

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
	"vitess.io/vitess/go/vt/sqlparser"
)

// exprReferencesTable checks if a subquery's FROM clause contains a reference
// to the given table name.  Correlated column references (e.g., outer.col in
// WHERE) are intentionally ignored -- MySQL only raises error 1093 when the
// target table appears as a source table in the subquery's FROM clause.
func exprReferencesTable(expr sqlparser.SQLNode, tableName string) bool {
	found := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if found {
			return false, nil
		}
		// Only inspect table expressions inside FROM clauses
		if ate, ok := node.(*sqlparser.AliasedTableExpr); ok {
			if tn, ok2 := ate.Expr.(sqlparser.TableName); ok2 {
				if strings.EqualFold(tn.Name.String(), tableName) {
					found = true
					return false, nil
				}
			}
		}
		return true, nil
	}, expr)
	return found
}

// subqueryReferencesTable checks if any SET expression's subquery references
// the same table being updated.
func subqueryReferencesTable(exprs sqlparser.UpdateExprs, tableName string) bool {
	for _, upd := range exprs {
		found := false
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
			if found {
				return false, nil
			}
			if sub, ok := node.(*sqlparser.Subquery); ok {
				if exprReferencesTable(sub.Select, tableName) {
					found = true
					return false, nil
				}
			}
			return true, nil
		}, upd.Expr)
		if found {
			return true
		}
	}
	return false
}

func (e *Executor) execUpdate(stmt *sqlparser.Update) (*Result, error) {
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

	// Check for multi-table UPDATE (comma-separated tables)
	if len(stmt.TableExprs) > 1 || isMultiTableUpdate(stmt) {
		return e.execMultiTableUpdate(stmt)
	}

	tableName := ""
	updateDB := e.CurrentDB
	switch te := stmt.TableExprs[0].(type) {
	case *sqlparser.AliasedTableExpr:
		tableName = sqlparser.String(te.Expr)
		tableName = strings.Trim(tableName, "`")
		if strings.Contains(tableName, ".") {
			updateDB, tableName = resolveTableNameDB(tableName, e.CurrentDB)
		}
	default:
		return nil, fmt.Errorf("unsupported table expression: %T", te)
	}

	// Resolve views: if tableName is a view, replace with the underlying base table.
	// Also collect the view's WHERE condition to merge with the UPDATE's WHERE.
	var viewWhereExpr sqlparser.Expr
	if baseTable, isView, viewWhere, err := e.resolveViewToBaseTable(tableName); err != nil {
		return nil, err
	} else if isView {
		tableName = baseTable
		viewWhereExpr = viewWhere
	}
	// Merge view's WHERE condition into the UPDATE's WHERE clause.
	// If the view has a WHERE clause, AND it with the UPDATE's WHERE clause.
	if viewWhereExpr != nil {
		if stmt.Where == nil {
			stmt.Where = &sqlparser.Where{Type: sqlparser.WhereClause, Expr: viewWhereExpr}
		} else {
			stmt.Where.Expr = &sqlparser.AndExpr{Left: viewWhereExpr, Right: stmt.Where.Expr}
		}
	}

	// Handle performance_schema tables
	if strings.EqualFold(updateDB, "performance_schema") {
		lowerTable := strings.ToLower(tableName)
		switch lowerTable {
		case "setup_instruments", "setup_consumers", "setup_threads", "threads",
			"setup_actors", "setup_objects":
			return e.execPerfSchemaUpdate(stmt, lowerTable)
		default:
			return nil, mysqlError(1142, "42000", fmt.Sprintf("UPDATE command denied to user 'root'@'localhost' for table '%s'", tableName))
		}
	}

	tbl, err := e.Storage.GetTable(updateDB, tableName)
	if err != nil {
		return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", updateDB, tableName))
	}
	orderCollation := ""
	if db, dbErr := e.Catalog.GetDatabase(updateDB); dbErr == nil {
		if def, defErr := db.GetTable(tableName); defErr == nil {
			orderCollation = effectiveTableCollation(def)
		}
	}

	// Check for self-referencing subqueries (MySQL error 1093)
	if subqueryReferencesTable(stmt.Exprs, tableName) {
		return nil, mysqlError(1093, "HY000", fmt.Sprintf("You can't specify target table '%s' for update in FROM clause", tableName))
	}

	tbl.Lock()
	defer tbl.Unlock()
	colTypeByName := make(map[string]string, len(tbl.Def.Columns))
	for _, col := range tbl.Def.Columns {
		colTypeByName[strings.ToLower(col.Name)] = col.Type
	}

	// Determine matching row indices
	var matchingIndices []int

	// Fast path: PK equality lookup for single-column PK without ORDER BY/LIMIT
	usedPKFastPath := false
	if stmt.Where != nil && len(tbl.Def.PrimaryKey) == 1 && stmt.OrderBy == nil && stmt.Limit == nil {
		pkCol := stripPrefixLengthFromCol(tbl.Def.PrimaryKey[0])
		if pkVal, remainExpr := extractPKEquality(stmt.Where.Expr, pkCol); pkVal != nil {
			tbl.EnsurePKRowIndex()
			keyStr := fmt.Sprintf("%v", pkVal)
			// Coerce the PK value through the column type so that numeric TIME/YEAR
			// values match the stored formatted strings (e.g. 111127 -> "11:11:27").
			if pkColType, ok := colTypeByName[strings.ToLower(pkCol)]; ok {
				coerced := coerceValueForColumnType(catalog.ColumnDef{Type: pkColType}, pkVal)
				if coerced != nil {
					keyStr = fmt.Sprintf("%v", coerced)
				}
			}
			rowIdx := tbl.LookupRowIndexByPK(keyStr)
			if rowIdx >= 0 {
				if remainExpr != nil {
					m, err := e.evalWhere(remainExpr, tbl.Rows[rowIdx])
					if err != nil {
						if bool(stmt.Ignore) {
							// UPDATE IGNORE: suppress WHERE eval errors (e.g. subquery > 1 row), skip row
							e.addWarning("Warning", 1242, strings.TrimPrefix(err.Error(), "ERROR 1242 (21000): "))
						} else {
							return nil, err
						}
					} else if m {
						matchingIndices = append(matchingIndices, rowIdx)
					}
				} else {
					matchingIndices = append(matchingIndices, rowIdx)
				}
			}
			usedPKFastPath = true
		}
	}

	if !usedPKFastPath {
		for i, row := range tbl.Rows {
			match := true
			if stmt.Where != nil {
				m, err := e.evalWhere(stmt.Where.Expr, row)
				if err != nil {
					if bool(stmt.Ignore) {
						// UPDATE IGNORE: suppress WHERE eval errors (e.g. subquery > 1 row), skip row
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
				matchingIndices = append(matchingIndices, i)
			}
		}
	}

	// Apply ORDER BY to matching rows
	if stmt.OrderBy != nil && len(stmt.OrderBy) > 0 {
		sort.SliceStable(matchingIndices, func(a, b int) bool {
			for _, order := range stmt.OrderBy {
				orderExpr := order.Expr
				if collateExpr, ok := orderExpr.(*sqlparser.CollateExpr); ok {
					orderExpr = collateExpr.Expr
				}
				colName := sqlparser.String(orderExpr)
				colName = strings.Trim(colName, "`")
				va := rowValueByColumnName(tbl.Rows[matchingIndices[a]], colName)
				vb := rowValueByColumnName(tbl.Rows[matchingIndices[b]], colName)
				cmp := compareByCollation(va, vb, orderCollation)
				if colType, ok := colTypeByName[strings.ToLower(colName)]; ok && isNumericOrderColumnType(colType) {
					cmp = compareNumeric(va, vb)
				}
				if cmp == 0 {
					continue
				}
				asc := order.Direction == sqlparser.AscOrder || order.Direction == 0
				if asc {
					return cmp < 0
				}
				return cmp > 0
			}
			return false
		})
	}

	// Apply LIMIT
	if stmt.Limit != nil && stmt.Limit.Rowcount != nil {
		tmpExec := &Executor{}
		lim, limErr := tmpExec.evalExpr(stmt.Limit.Rowcount)
		if limErr == nil {
			if n, ok := lim.(int64); ok && int(n) < len(matchingIndices) {
				matchingIndices = matchingIndices[:n]
			}
		}
	}

	// Check if any SET targets a generated column (MySQL error 3105)
	for _, upd := range stmt.Exprs {
		colName := upd.Name.Name.String()
		if _, isDefault := upd.Expr.(*sqlparser.Default); !isDefault {
			for _, col := range tbl.Def.Columns {
				if strings.EqualFold(col.Name, colName) && isGeneratedColumnType(col.Type) {
					return nil, mysqlError(3105, "HY000",
						fmt.Sprintf("The value specified for generated column '%s' in table '%s' is not allowed.",
							colName, tbl.Def.Name))
				}
			}
		}
	}

	// Acquire row locks for UPDATE.
	// In REPEATABLE READ with full table scan (no PK lookup), lock ALL rows.
	// In READ COMMITTED or with PK lookup, lock only matching rows.
	if e.rowLockManager != nil && len(tbl.Rows) > 0 && e.shouldAcquireRowLocks() {
		isoLevel, _ := e.getSysVar("transaction_isolation")
		if isoLevel == "" {
			isoLevel, _ = e.getSysVar("tx_isolation")
		}
		isReadCommitted := strings.EqualFold(isoLevel, "READ-COMMITTED") || strings.EqualFold(isoLevel, "READ COMMITTED")
		var lockIndices []int
		if isReadCommitted || usedPKFastPath {
			lockIndices = matchingIndices
		} else {
			// REPEATABLE READ full table scan: lock all rows
			lockIndices = make([]int, len(tbl.Rows))
			for i := range tbl.Rows {
				lockIndices[i] = i
			}
		}
		if len(lockIndices) > 0 {
			tbl.Unlock() // release table lock while waiting for row locks
			err := e.acquireRowLocksForRows(updateDB, tableName, tbl.Def, tbl.Rows, lockIndices)
			tbl.Lock() // re-acquire table lock
			if err != nil {
				return nil, err
			}
		}
	}

	var affected uint64
	var matchedRows uint64
	for _, i := range matchingIndices {
		row := tbl.Rows[i]
		_ = row

		// Build OLD and NEW row for triggers
		oldRow := make(storage.Row, len(row))
		for k, v := range row {
			oldRow[k] = v
		}

		// Compute NEW values using row context for column references
		newRow := make(storage.Row, len(row))
		for k, v := range row {
			newRow[k] = v
		}
		for _, upd := range stmt.Exprs {
			colName := upd.Name.Name.String()
			// MySQL evaluates SET clauses left-to-right; each clause sees
			// values already updated by preceding clauses, so use newRow.
			val, err := e.evalRowExpr(upd.Expr, newRow)
			if err != nil {
				return nil, err
			}
			for _, col := range tbl.Def.Columns {
				if col.Name == colName {
						if val != nil {
						colUpper := strings.ToUpper(col.Type)
						// In non-strict mode, invalid numeric strings are coerced to 0 with warning.
						isNumericType := strings.Contains(colUpper, "DECIMAL") || strings.Contains(colUpper, "NUMERIC") ||
							strings.Contains(colUpper, "FLOAT") || strings.Contains(colUpper, "DOUBLE") || strings.Contains(colUpper, "REAL") ||
							strings.Contains(colUpper, "INT") || strings.Contains(colUpper, "INTEGER")
						if !e.isStrictMode() && isNumericType {
							if sv, ok := val.(string); ok {
								if _, err := strconv.ParseFloat(strings.TrimSpace(sv), 64); err != nil {
									e.addWarning("Warning", 1366, fmt.Sprintf("Incorrect decimal value: '%s' for column '%s' at row %d", sv, col.Name, i+1))
									if pv, ok := parseNumericPrefixMySQL(sv); ok {
										val = pv
									} else {
										val = int64(0)
									}
								}
							}
						}
						if !e.isStrictMode() && strings.HasPrefix(colUpper, "TIME") {
							if sv, ok := val.(string); ok {
								parsed := parseMySQLTimeValue(sv)
								if strings.Count(parsed, ":") != 2 {
									e.addWarning("Warning", 1265, fmt.Sprintf("Data truncated for column '%s' at row %d", col.Name, i+1))
								}
							}
						}
						// In non-strict mode, UPDATE that clips DECIMAL/FLOAT/DOUBLE/REAL should produce warning 1264.
						if !e.isStrictMode() && (strings.Contains(colUpper, "DECIMAL") || strings.Contains(colUpper, "NUMERIC") || strings.Contains(colUpper, "FLOAT") || strings.Contains(colUpper, "DOUBLE") || strings.Contains(colUpper, "REAL")) {
							f, cls := decimalParseValue(val)
							if maxAbs, unsignedCol, ok := numericTypeRange(col.Type); ok {
								outOfRange := cls == "overflow_pos" || cls == "overflow_neg" || math.Abs(f) > maxAbs
								if (unsignedCol && (cls == "overflow_neg" || f < 0)) || outOfRange {
									e.addWarning("Warning", 1264, fmt.Sprintf("Out of range value for column '%s' at row %d", col.Name, i+1))
								}
							}
						}
					}
					// String length check for CHAR/VARCHAR/BINARY/VARBINARY/BLOB/TEXT columns
					if val != nil {
						colUp := strings.ToUpper(col.Type)
						isCharType := strings.Contains(colUp, "CHAR") || strings.Contains(colUp, "BINARY")
						isBlobType := colUp == "BLOB" || colUp == "TINYBLOB" || colUp == "MEDIUMBLOB" || colUp == "LONGBLOB"
						isTextType := colUp == "TEXT" || colUp == "TINYTEXT" || colUp == "MEDIUMTEXT" || colUp == "LONGTEXT"
						if isCharType || isBlobType || isTextType {
							if sv, ok := val.(string); ok {
								maxLen := extractCharLength(col.Type)
								isBinaryCol := strings.Contains(colUp, "BINARY") || isBlobType
								var svLen int
								if isBinaryCol {
									svLen = len(sv)
								} else {
									// For CHAR/VARCHAR/TEXT, determine effective charset for char counting.
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
									svLen = 0
									if isUtf8Cs && utf8.ValidString(sv) {
										svLen = len([]rune(sv))
									}
								}
								if maxLen > 0 && svLen > maxLen {
									if isBinaryCol {
										// BLOB/BINARY: truncate at byte boundary
										if bool(stmt.Ignore) || !e.isStrictMode() {
											val = sv[:maxLen]
											e.addWarning("Warning", 1265, fmt.Sprintf("Data truncated for column '%s' at row %d", col.Name, i+1))
										} else {
											return nil, mysqlError(1406, "22001", fmt.Sprintf("Data too long for column '%s' at row %d", col.Name, i+1))
										}
									} else {
										excess := string([]rune(sv)[maxLen:])
										onlySpaces := strings.TrimRight(excess, " ") == ""
										if onlySpaces {
											val = string([]rune(sv)[:maxLen])
											e.addWarning("Note", 1265, fmt.Sprintf("Data truncated for column '%s' at row %d", col.Name, i+1))
										} else if e.isStrictMode() {
											if bool(stmt.Ignore) {
												val = string([]rune(sv)[:maxLen])
												e.addWarning("Warning", 1265, fmt.Sprintf("Data truncated for column '%s' at row %d", col.Name, i+1))
											} else {
												return nil, mysqlError(1406, "22001", fmt.Sprintf("Data too long for column '%s' at row %d", col.Name, i+1))
											}
										} else {
											// Non-strict mode: warn but do NOT truncate (Dolt compatibility)
											e.addWarning("Warning", 1265, fmt.Sprintf("Data truncated for column '%s' at row %d", col.Name, i+1))
										}
									}
								}
							}
						}
					}
					val = coerceColumnValue(col.Type, val)
					break
				}
			}
			newRow[colName] = val
		}

		// Recalculate generated/virtual columns after SET clauses are applied,
		// since their base columns may have changed.
		for _, col := range tbl.Def.Columns {
			genExpr := generatedColumnExpr(col.Type)
			if genExpr == "" {
				continue
			}
			if v, err := e.evalGeneratedColumnExpr(genExpr, newRow); err == nil {
				newRow[col.Name] = v
			}
		}

		// Fire BEFORE UPDATE triggers (unlock table to avoid deadlock since trigger may access other tables)
		// Trigger may modify newRow via SET NEW.col = val
		tbl.Unlock()
		if err := e.fireTriggers(tableName, "BEFORE", "UPDATE", newRow, oldRow); err != nil {
			tbl.Lock()
			return nil, err
		}
		tbl.Lock()

		// Enforce NOT NULL constraints on final NEW values.
		// Convert NULL to the implicit zero/default value first so that
		// subsequent constraint checks (duplicate key, etc.) see the
		// converted value rather than nil.
		var nullToZeroCols []string
		for _, col := range tbl.Def.Columns {
			if col.Nullable {
				continue
			}
			if val, ok := newRow[col.Name]; ok && val == nil {
				colUpper := strings.ToUpper(col.Type)
				isTimestamp := strings.HasPrefix(colUpper, "TIMESTAMP")
				if isTimestamp {
					// TIMESTAMP NOT NULL: MySQL converts NULL to CURRENT_TIMESTAMP
					newRow[col.Name] = e.nowTime().Format("2006-01-02 15:04:05")
					if !e.isStrictMode() || bool(stmt.Ignore) {
						e.addWarning("Warning", 1048, fmt.Sprintf("Column '%s' cannot be null", col.Name))
					}
				} else {
					// DATE/DATETIME/other NOT NULL: convert to implicit zero value
					newRow[col.Name] = implicitZeroValue(col.Type)
					nullToZeroCols = append(nullToZeroCols, col.Name)
					if !e.isStrictMode() {
						e.addWarning("Warning", 1048, fmt.Sprintf("Column '%s' cannot be null", col.Name))
					}
				}
			}
		}

		// Enforce FOREIGN KEY constraints on UPDATE
		tbl.Unlock()
		if fkErr := e.checkForeignKeyOnUpdate(updateDB, tableName, oldRow, newRow); fkErr != nil {
			tbl.Lock()
			return nil, fkErr
		}
		tbl.Lock()

		// Enforce PRIMARY KEY / UNIQUE constraints for the updated row.
		dupErr := func() error {
			// Resolve primary key columns (table-level or column-level declaration).
			pkCols := make([]string, 0, len(tbl.Def.PrimaryKey))
			if len(tbl.Def.PrimaryKey) > 0 {
				pkCols = append(pkCols, tbl.Def.PrimaryKey...)
			} else {
				for _, col := range tbl.Def.Columns {
					if col.PrimaryKey {
						pkCols = append(pkCols, col.Name)
					}
				}
			}
			if len(pkCols) > 0 {
				// Check if any PK column was actually modified.
				pkModified := false
				for _, pk := range pkCols {
					basePk := stripPrefixLengthFromCol(pk)
					oldVal := fmt.Sprintf("%v", oldRow[basePk])
					newVal := fmt.Sprintf("%v", newRow[basePk])
					if oldVal != newVal {
						pkModified = true
						break
					}
				}
				if pkModified {
					// PK value changed -- check for duplicates via full scan
					for j, other := range tbl.Rows {
						if j == i {
							continue
						}
						match := true
						for _, pk := range pkCols {
							basePk := stripPrefixLengthFromCol(pk)
							if fmt.Sprintf("%v", other[basePk]) != fmt.Sprintf("%v", newRow[basePk]) {
								match = false
								break
							}
						}
						if match {
							keyVals := make([]string, len(pkCols))
							for k, pk := range pkCols {
								keyVals[k] = fmt.Sprintf("%v", newRow[stripPrefixLengthFromCol(pk)])
							}
							return mysqlError(1062, "23000", fmt.Sprintf("Duplicate entry '%s' for key 'PRIMARY'", strings.Join(keyVals, "-")))
						}
					}
				}
				// If PK not modified, no duplicate is possible since the row already existed.
			}
			for _, idx := range tbl.Def.Indexes {
				if !idx.Unique {
					continue
				}
				// Check if any unique index column was actually modified.
				idxModified := false
				for _, c := range idx.Columns {
					baseC := stripPrefixLengthFromCol(c)
					ov := fmt.Sprintf("%v", oldRow[baseC])
					nv := fmt.Sprintf("%v", newRow[baseC])
					if ov != nv {
						idxModified = true
						break
					}
				}
				if !idxModified {
					continue
				}
				for j, other := range tbl.Rows {
					if j == i {
						continue
					}
					match := true
					vals := make([]string, 0, len(idx.Columns))
					for _, c := range idx.Columns {
						baseC := stripPrefixLengthFromCol(c)
						nv := newRow[baseC]
						ov := other[baseC]
						// NULL values do not conflict under UNIQUE.
						if nv == nil || ov == nil {
							match = false
							break
						}
						if fmt.Sprintf("%v", nv) != fmt.Sprintf("%v", ov) {
							match = false
							break
						}
						vals = append(vals, fmt.Sprintf("%v", nv))
					}
					if match {
						return mysqlError(1062, "23000", fmt.Sprintf("Duplicate entry '%s' for key '%s'", strings.Join(vals, "-"), idx.Name))
					}
				}
			}
			return nil
		}()
		if dupErr != nil {
			if bool(stmt.Ignore) {
				e.addWarning("Warning", 1062, strings.TrimPrefix(dupErr.Error(), "ERROR 1062 (23000): "))
				continue
			}
			return nil, dupErr
		}

		// In strict mode, if we converted NULL to zero for NOT NULL columns
		// and no duplicate key absorbed the row, produce an error.
		if e.isStrictMode() && len(nullToZeroCols) > 0 {
			if bool(stmt.Ignore) {
				// UPDATE IGNORE: add warnings for each NULL-to-zero conversion
				for _, colName := range nullToZeroCols {
					e.addWarning("Warning", 1048, fmt.Sprintf("Column '%s' cannot be null", colName))
				}
			} else {
				return nil, mysqlError(1048, "23000", fmt.Sprintf("Column '%s' cannot be null", nullToZeroCols[0]))
			}
		}

		// Apply the trigger-modified newRow values to the actual row
		// and detect whether any column actually changed
		rowChanged := false
		for _, col := range tbl.Def.Columns {
			if val, ok := newRow[col.Name]; ok {
				if padLen := binaryPadLength(col.Type); padLen > 0 && val != nil {
					val = padBinaryValue(val, padLen)
				}
				oldVal := tbl.Rows[i][col.Name]
				if !valuesEqual(oldVal, val) {
					rowChanged = true
				}
				tbl.Rows[i][col.Name] = val
			}
		}
		matchedRows++
		if rowChanged {
			affected++
			// Apply ON UPDATE CURRENT_TIMESTAMP for columns not explicitly set
			for _, col := range tbl.Def.Columns {
				if col.OnUpdateCurrentTimestamp {
					explicitlySet := false
					for _, upd := range stmt.Exprs {
						if strings.EqualFold(upd.Name.Name.String(), col.Name) {
							explicitlySet = true
							break
						}
					}
					if !explicitlySet {
						nowStr := e.nowTime().Format("2006-01-02 15:04:05")
						// Use fractional seconds if column type has precision
						colUpper := strings.ToUpper(col.Type)
						if strings.Contains(colUpper, "TIMESTAMP(6)") || strings.Contains(colUpper, "DATETIME(6)") {
							nowStr = e.nowTime().Format("2006-01-02 15:04:05.000000")
						} else if strings.Contains(colUpper, "TIMESTAMP(3)") || strings.Contains(colUpper, "DATETIME(3)") {
							nowStr = e.nowTime().Format("2006-01-02 15:04:05.000")
						}
						tbl.Rows[i][col.Name] = nowStr
					}
				}
			}
			// If an auto-increment column was updated, advance the AI counter
			// so subsequent INSERT with NULL/0 won't generate a duplicate.
			for _, col := range tbl.Def.Columns {
				if col.AutoIncrement {
					if newVal, ok := newRow[col.Name]; ok && newVal != nil {
						iv := toInt64(newVal)
						if iv > tbl.AutoIncrement.Load() {
							tbl.AutoIncrement.Store(iv)
						}
					}
					break
				}
			}
		}

		// Fire AFTER UPDATE triggers
		tbl.Unlock()
		if err := e.fireTriggers(tableName, "AFTER", "UPDATE", newRow, oldRow); err != nil {
			tbl.Lock()
			return nil, err
		}
		tbl.Lock()
	}

	if affected > 0 {
		updatesPK := false
		if len(tbl.Def.PrimaryKey) > 0 {
			pkSet := make(map[string]bool, len(tbl.Def.PrimaryKey))
			for _, pk := range tbl.Def.PrimaryKey {
				pkSet[strings.ToLower(stripPrefixLengthFromCol(pk))] = true
			}
			for _, upd := range stmt.Exprs {
				if pkSet[strings.ToLower(upd.Name.Name.String())] {
					updatesPK = true
					break
				}
			}
		}
		if updatesPK {
			tbl.InvalidateIndexes()
		} else {
			tbl.InvalidateNonPKIndexes()
		}
	}

	warnCount := len(e.warnings)
	infoMsg := fmt.Sprintf("Rows matched: %d  Changed: %d  Warnings: %d", matchedRows, affected, warnCount)
	e.lastUpdateInfo = infoMsg
	return &Result{
		AffectedRows: affected,
		MatchedRows:  matchedRows,
		ChangedRows:  affected,
		InfoMessage:  infoMsg,
	}, nil
}

func isMultiTableUpdate(stmt *sqlparser.Update) bool {
	if len(stmt.TableExprs) > 1 {
		return true
	}
	if len(stmt.TableExprs) == 1 {
		if _, ok := stmt.TableExprs[0].(*sqlparser.JoinTableExpr); ok {
			return true
		}
	}
	return false
}

// collectTableRefAliases walks the table expressions from an UPDATE statement
// and returns a map of alias -> real table name (lowercase) for all table references.
// If the executor is provided, views are resolved to their underlying table.
func (e *Executor) collectTableRefAliases(tes sqlparser.TableExprs) map[string]string {
	m := make(map[string]string)
	var walk func(te sqlparser.TableExpr)
	walk = func(te sqlparser.TableExpr) {
		switch t := te.(type) {
		case *sqlparser.AliasedTableExpr:
			if tn, ok := t.Expr.(sqlparser.TableName); ok {
				realName := strings.ToLower(tn.Name.String())
				// Resolve views to their underlying table
				if e != nil {
					if viewSQL, ok := e.views[realName]; ok {
						// Try to extract the underlying table from the view
						if viewStmt, err := e.parser().Parse(viewSQL); err == nil {
							if sel, ok := viewStmt.(*sqlparser.Select); ok && len(sel.From) == 1 {
								if ate, ok := sel.From[0].(*sqlparser.AliasedTableExpr); ok {
									if vtn, ok := ate.Expr.(sqlparser.TableName); ok {
										realName = strings.ToLower(vtn.Name.String())
									}
								}
							}
						}
					}
				}
				alias := strings.ToLower(tn.Name.String())
				if !t.As.IsEmpty() {
					alias = strings.ToLower(t.As.String())
				}
				m[alias] = realName
			}
		case *sqlparser.JoinTableExpr:
			walk(t.LeftExpr)
			walk(t.RightExpr)
		case *sqlparser.ParenTableExpr:
			for _, ex := range t.Exprs {
				walk(ex)
			}
		}
	}
	for _, te := range tes {
		walk(te)
	}
	return m
}

// execMultiTableUpdate handles multi-table UPDATE statements.
func (e *Executor) execMultiTableUpdate(stmt *sqlparser.Update) (*Result, error) {
	// Check for PK/partition key update conflict: if the same underlying table
	// appears more than once (with different aliases) and a PK column is being
	// updated, MySQL returns an error.
	aliasToReal := e.collectTableRefAliases(stmt.TableExprs)
	// Build reverse map: real table name -> list of aliases
	realToAliases := make(map[string][]string)
	for alias, real := range aliasToReal {
		realToAliases[real] = append(realToAliases[real], alias)
	}
	// For tables that appear more than once, check if any SET expression
	// updates a PK column.
	for realName, aliases := range realToAliases {
		if len(aliases) < 2 {
			continue
		}
		// Get PK columns for this table
		tbl, err := e.Storage.GetTable(e.CurrentDB, realName)
		if err != nil {
			continue
		}
		pkCols := make(map[string]bool)
		for _, col := range tbl.Def.Columns {
			if col.PrimaryKey {
				pkCols[strings.ToLower(col.Name)] = true
			}
		}
		// Also use the PrimaryKey slice from the table definition
		for _, pk := range tbl.Def.PrimaryKey {
			pkCols[strings.ToLower(pk)] = true
		}
		if len(pkCols) == 0 {
			continue
		}
		// Check SET clauses for PK column updates
		for _, upd := range stmt.Exprs {
			colName := strings.ToLower(upd.Name.Name.String())
			if !pkCols[colName] {
				continue
			}
			// PK column is being updated — check if the qualifier matches one of the aliases
			qual := strings.ToLower(strings.Trim(sqlparser.String(upd.Name.Qualifier), "`"))
			if qual == "" {
				// Unqualified — if the column belongs to this table, it's a conflict
				for _, col := range tbl.Def.Columns {
					if strings.EqualFold(col.Name, colName) {
						// Find two alias names (uppercased) for the error message
						sort.Strings(aliases)
						return nil, mysqlError(1706, "HY000", fmt.Sprintf("Primary key/partition key update is not allowed since the table is updated both as '%s' and '%s'.",
							strings.ToUpper(aliases[0]), strings.ToUpper(aliases[1])))
					}
				}
			}
			for _, a := range aliases {
				if qual == a {
					sort.Strings(aliases)
					return nil, mysqlError(1706, "HY000", fmt.Sprintf("Primary key/partition key update is not allowed since the table is updated both as '%s' and '%s'.",
						strings.ToUpper(aliases[0]), strings.ToUpper(aliases[1])))
				}
			}
		}
	}

	// Build cross product of all tables, with WHERE predicate pushdown
	var allRows []storage.Row
	var err error

	if len(stmt.TableExprs) == 1 {
		allRows, err = e.buildFromExprWithWhere(stmt.TableExprs[0], stmt.Where)
		if err != nil {
			return nil, err
		}
	} else {
		allRows, err = e.buildFromExprWithWhere(stmt.TableExprs[0], stmt.Where)
		if err != nil {
			return nil, err
		}
		for i := 1; i < len(stmt.TableExprs); i++ {
			rightRows, err := e.buildFromExpr(stmt.TableExprs[i])
			if err != nil {
				return nil, err
			}
			allRows = crossProduct(allRows, rightRows)
		}
	}

	// Filter by WHERE
	var matchedRows []storage.Row
	for _, row := range allRows {
		if stmt.Where != nil {
			match, err := e.evalWhere(stmt.Where.Expr, row)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}
		matchedRows = append(matchedRows, row)
	}

	var affected uint64

	// Pre-resolve each SET expression's target table so we can group by table.
	type resolvedUpd struct {
		colName     string
		targetDB    string
		targetTable string
		upd         *sqlparser.UpdateExpr
	}
	resolvedUpds := make([]resolvedUpd, 0, len(stmt.Exprs))
	for idx := range stmt.Exprs {
		upd := stmt.Exprs[idx]
		colName := upd.Name.Name.String()
		qualStr := sqlparser.String(upd.Name.Qualifier)
		qualStr = strings.Trim(qualStr, "`")

		var targetDB, targetTable string
		if strings.Contains(qualStr, ".") {
			parts := strings.SplitN(qualStr, ".", 2)
			targetDB = parts[0]
			targetTable = parts[1]
		} else if qualStr != "" {
			targetTable = qualStr
			targetDB = e.CurrentDB
		} else {
			targetDB = e.CurrentDB
			targetTable = ""
			for _, te := range stmt.TableExprs {
				resolved := resolveColumnTable(te, colName, e)
				if resolved != "" {
					targetTable = resolved
					break
				}
			}
		}
		resolvedUpds = append(resolvedUpds, resolvedUpd{colName, targetDB, targetTable, upd})
	}

nextRow:
	for _, mrow := range matchedRows {
		// Group SET expressions by target table key (db.table) and apply all
		// columns for the same table together so that matchRowToTableLenient
		// is called only once per table before any storage modifications.
		type tableKey struct{ db, table string }
		type pendingCol struct {
			colName string
			upd     *sqlparser.UpdateExpr
		}
		tableOrder := make([]tableKey, 0, 4)
		tableGroups := make(map[tableKey][]pendingCol)
		for _, ru := range resolvedUpds {
			tk := tableKey{ru.targetDB, ru.targetTable}
			if _, exists := tableGroups[tk]; !exists {
				tableOrder = append(tableOrder, tk)
			}
			tableGroups[tk] = append(tableGroups[tk], pendingCol{ru.colName, ru.upd})
		}

		for _, tk := range tableOrder {
			cols := tableGroups[tk]

			tbl, err := e.Storage.GetTable(tk.db, tk.table)
			if err != nil {
				continue
			}

			targetAlias := tk.table
			if tk.db != e.CurrentDB {
				targetAlias = tk.db + "." + tk.table
			}

			tbl.Lock()
			i := matchRowToTableLenient(mrow, tbl, targetAlias, tk.table)
			if i < 0 {
				tbl.Unlock()
				continue
			}

			// Evaluate all SET values and coerce types.
			colVals := make(map[string]interface{}, len(cols))
			var unlockAndReturn error
			for _, pc := range cols {
				val, err := e.evalRowExpr(pc.upd.Expr, mrow)
				if err != nil {
					unlockAndReturn = err
					break
				}
				for _, col := range tbl.Def.Columns {
					if col.Name == pc.colName {
						if val == nil && !col.Nullable {
							unlockAndReturn = mysqlError(1048, "23000", fmt.Sprintf("Column '%s' cannot be null", pc.colName))
							break
						}
						val = coerceColumnValue(col.Type, val)
						break
					}
				}
				if unlockAndReturn != nil {
					break
				}
				colVals[pc.colName] = val
			}
			if unlockAndReturn != nil {
				tbl.Unlock()
				return nil, unlockAndReturn
			}

			// Build candidate row with all column changes and enforce PK uniqueness.
			candidate := make(storage.Row, len(tbl.Rows[i]))
			for k, v := range tbl.Rows[i] {
				candidate[k] = v
			}
			for colName, val := range colVals {
				candidate[colName] = val
			}

			pkCols := make([]string, 0, len(tbl.Def.PrimaryKey))
			if len(tbl.Def.PrimaryKey) > 0 {
				pkCols = append(pkCols, tbl.Def.PrimaryKey...)
			} else {
				for _, col := range tbl.Def.Columns {
					if col.PrimaryKey {
						pkCols = append(pkCols, col.Name)
					}
				}
			}
			if len(pkCols) > 0 {
				for j, other := range tbl.Rows {
					if j == i {
						continue
					}
					matchPK := true
					for _, pk := range pkCols {
						if fmt.Sprintf("%v", other[pk]) != fmt.Sprintf("%v", candidate[pk]) {
							matchPK = false
							break
						}
					}
					if matchPK {
						vals := make([]string, len(pkCols))
						for k, pk := range pkCols {
							vals[k] = fmt.Sprintf("%v", candidate[pk])
						}
						dupErr := mysqlError(1062, "23000", fmt.Sprintf("Duplicate entry '%s' for key 'PRIMARY'", strings.Join(vals, "-")))
						if bool(stmt.Ignore) {
							e.addWarning("Warning", 1062, strings.TrimPrefix(dupErr.Error(), "ERROR 1062 (23000): "))
							tbl.Unlock()
							continue nextRow
						}
						tbl.Unlock()
						return nil, dupErr
					}
				}
			}

			// Apply all column updates at once.
			anyChanged := false
			for colName, val := range colVals {
				oldVal := tbl.Rows[i][colName]
				tbl.Rows[i][colName] = val
				if !valuesEqual(oldVal, val) {
					anyChanged = true
				}
			}
			// Apply ON UPDATE CURRENT_TIMESTAMP if any value actually changed
			if anyChanged {
				explicitCols := make(map[string]bool)
				for _, u := range stmt.Exprs {
					explicitCols[strings.ToLower(u.Name.Name.String())] = true
				}
				for _, col := range tbl.Def.Columns {
					if col.OnUpdateCurrentTimestamp && !explicitCols[strings.ToLower(col.Name)] {
						nowStr := e.nowTime().Format("2006-01-02 15:04:05")
						colUpper := strings.ToUpper(col.Type)
						if strings.Contains(colUpper, "TIMESTAMP(6)") || strings.Contains(colUpper, "DATETIME(6)") {
							nowStr = e.nowTime().Format("2006-01-02 15:04:05.000000")
						} else if strings.Contains(colUpper, "TIMESTAMP(3)") || strings.Contains(colUpper, "DATETIME(3)") {
							nowStr = e.nowTime().Format("2006-01-02 15:04:05.000")
						}
						tbl.Rows[i][col.Name] = nowStr
					}
				}
			}
			tbl.Unlock()
		}
		affected++
	}

	return &Result{AffectedRows: affected}, nil
}
