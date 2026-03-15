package executor

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
	"vitess.io/vitess/go/vt/sqlparser"
)

// Result represents the result of a query execution.
type Result struct {
	Columns      []string
	Rows         [][]interface{}
	AffectedRows uint64
	InsertID     uint64
	IsResultSet  bool // true for SELECT, SHOW, etc.
}

// txSavepoint holds the catalog and storage state captured at BEGIN time.
type txSavepoint struct {
	// Storage snapshot per database name.
	storageSnap map[string]*storage.DatabaseSnapshot
	// Catalog snapshot: db name -> table name -> *catalog.TableDef (shallow copy is fine;
	// TableDef itself is not mutated after creation).
	catalogSnap map[string]map[string]*catalog.TableDef
}

// Executor handles SQL execution.
type Executor struct {
	Catalog       *catalog.Catalog
	Storage       *storage.Engine
	CurrentDB     string
	inTransaction bool
	savepoint     *txSavepoint
}

func New(cat *catalog.Catalog, store *storage.Engine) *Executor {
	return &Executor{
		Catalog:   cat,
		Storage:   store,
		CurrentDB: "test",
	}
}

// Execute parses and executes a SQL statement.
func (e *Executor) Execute(query string) (*Result, error) {
	stmt, err := sqlparser.NewTestParser().Parse(query)
	if err != nil {
		return nil, fmt.Errorf("parse error: %v", err)
	}

	switch s := stmt.(type) {
	case *sqlparser.CreateDatabase:
		return e.execCreateDatabase(s)
	case *sqlparser.DropDatabase:
		return e.execDropDatabase(s)
	case *sqlparser.Use:
		return e.execUse(s)
	case *sqlparser.CreateTable:
		return e.execCreateTable(s)
	case *sqlparser.DropTable:
		return e.execDropTable(s)
	case *sqlparser.Insert:
		return e.execInsert(s)
	case *sqlparser.Select:
		return e.execSelect(s)
	case *sqlparser.Update:
		return e.execUpdate(s)
	case *sqlparser.Delete:
		return e.execDelete(s)
	case *sqlparser.AlterTable:
		return e.execAlterTable(s)
	case *sqlparser.Show:
		return e.execShow(s, query)
	case *sqlparser.ExplainTab:
		return e.execDescribe(s)
	case *sqlparser.Begin:
		return e.execBegin()
	case *sqlparser.Commit:
		return e.execCommit()
	case *sqlparser.Rollback:
		return e.execRollback()
	case *sqlparser.Set:
		// Accept SET statements silently
		return &Result{}, nil
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", s)
	}
}

func (e *Executor) execCreateDatabase(stmt *sqlparser.CreateDatabase) (*Result, error) {
	name := stmt.DBName.String()
	err := e.Catalog.CreateDatabase(name)
	if err != nil {
		if stmt.IfNotExists {
			return &Result{}, nil
		}
		return nil, err
	}
	e.Storage.EnsureDatabase(name)
	return &Result{AffectedRows: 1}, nil
}

func (e *Executor) execDropDatabase(stmt *sqlparser.DropDatabase) (*Result, error) {
	name := stmt.DBName.String()
	err := e.Catalog.DropDatabase(name)
	if err != nil {
		if stmt.IfExists {
			return &Result{}, nil
		}
		return nil, err
	}
	e.Storage.DropDatabase(name)
	if e.CurrentDB == name {
		e.CurrentDB = ""
	}
	return &Result{}, nil
}

func (e *Executor) execUse(stmt *sqlparser.Use) (*Result, error) {
	name := stmt.DBName.String()
	_, err := e.Catalog.GetDatabase(name)
	if err != nil {
		return nil, err
	}
	e.CurrentDB = name
	return &Result{}, nil
}

func (e *Executor) execCreateTable(stmt *sqlparser.CreateTable) (*Result, error) {
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return nil, err
	}

	tableName := stmt.Table.Name.String()

	columns := make([]catalog.ColumnDef, 0)
	var primaryKeys []string

	for _, col := range stmt.TableSpec.Columns {
		colDef := catalog.ColumnDef{
			Name:     col.Name.String(),
			Type:     col.Type.Type,
			Nullable: col.Type.Options != nil && col.Type.Options.Null != nil && *col.Type.Options.Null,
		}

		if col.Type.Options != nil {
			if col.Type.Options.Autoincrement {
				colDef.AutoIncrement = true
			}
			if col.Type.Options.Default != nil {
				defStr := sqlparser.String(col.Type.Options.Default)
				colDef.Default = &defStr
			}
			if col.Type.Options.KeyOpt == 1 { // colKeyPrimary
				colDef.PrimaryKey = true
				primaryKeys = append(primaryKeys, colDef.Name)
			}
			if col.Type.Options.KeyOpt == 2 { // colKeyUnique
				colDef.Unique = true
			}
		}

		// If NOT NULL is not explicitly set and no NULL is set, default depends on PK
		if col.Type.Options != nil && col.Type.Options.Null != nil && !*col.Type.Options.Null {
			colDef.Nullable = false
		}

		columns = append(columns, colDef)
	}

	// Process index definitions for PRIMARY KEY
	for _, idx := range stmt.TableSpec.Indexes {
		if idx.Info.Type == sqlparser.IndexTypePrimary {
			primaryKeys = nil
			for _, idxCol := range idx.Columns {
				primaryKeys = append(primaryKeys, idxCol.Column.String())
			}
		}
	}

	def := &catalog.TableDef{
		Name:       tableName,
		Columns:    columns,
		PrimaryKey: primaryKeys,
	}

	err = db.CreateTable(def)
	if err != nil {
		if stmt.IfNotExists {
			return &Result{}, nil
		}
		return nil, err
	}
	e.Storage.CreateTable(e.CurrentDB, def)
	return &Result{}, nil
}

func (e *Executor) execDropTable(stmt *sqlparser.DropTable) (*Result, error) {
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return nil, err
	}

	for _, table := range stmt.FromTables {
		tableName := table.Name.String()
		err := db.DropTable(tableName)
		if err != nil {
			if stmt.IfExists {
				continue
			}
			return nil, err
		}
		e.Storage.DropTable(e.CurrentDB, tableName)
	}
	return &Result{}, nil
}

func (e *Executor) captureSnapshot() *txSavepoint {
	sp := &txSavepoint{
		storageSnap: make(map[string]*storage.DatabaseSnapshot),
		catalogSnap: make(map[string]map[string]*catalog.TableDef),
	}
	// Snapshot all databases currently in the catalog.
	for dbName, db := range e.Catalog.Databases {
		sp.storageSnap[dbName] = e.Storage.SnapshotDatabase(dbName)
		tablesCopy := make(map[string]*catalog.TableDef, len(db.Tables))
		for tName, tDef := range db.Tables {
			tablesCopy[tName] = tDef
		}
		sp.catalogSnap[dbName] = tablesCopy
	}
	return sp
}

func (e *Executor) execBegin() (*Result, error) {
	if e.inTransaction {
		// Implicit commit of previous transaction before starting a new one.
		e.savepoint = nil
	}
	e.savepoint = e.captureSnapshot()
	e.inTransaction = true
	return &Result{}, nil
}

func (e *Executor) execCommit() (*Result, error) {
	if !e.inTransaction {
		return &Result{}, nil
	}
	e.inTransaction = false
	e.savepoint = nil
	return &Result{}, nil
}

func (e *Executor) execRollback() (*Result, error) {
	if !e.inTransaction {
		return &Result{}, nil
	}
	sp := e.savepoint
	e.inTransaction = false
	e.savepoint = nil

	if sp == nil {
		return &Result{}, nil
	}

	// Restore catalog: replace each database's table map with the snapshot.
	// First, remove databases that were created during the transaction.
	for dbName := range e.Catalog.Databases {
		if _, existed := sp.catalogSnap[dbName]; !existed {
			delete(e.Catalog.Databases, dbName)
			e.Storage.DropDatabase(dbName)
		}
	}
	// Restore tables in each snapshotted database.
	for dbName, tables := range sp.catalogSnap {
		db, ok := e.Catalog.Databases[dbName]
		if !ok {
			// Database was dropped during the transaction; recreate it.
			e.Catalog.Databases[dbName] = &catalog.Database{
				Name:   dbName,
				Tables: make(map[string]*catalog.TableDef),
			}
			db = e.Catalog.Databases[dbName]
		}
		// Replace the table map wholesale.
		db.Tables = tables
		// Restore storage.
		e.Storage.RestoreDatabase(dbName, sp.storageSnap[dbName])
	}

	return &Result{}, nil
}

func (e *Executor) execInsert(stmt *sqlparser.Insert) (*Result, error) {
	tableName := stmt.Table.TableNameString()

	tbl, err := e.Storage.GetTable(e.CurrentDB, tableName)
	if err != nil {
		return nil, err
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

	rows, ok := stmt.Rows.(sqlparser.Values)
	if !ok {
		return nil, fmt.Errorf("unsupported INSERT format")
	}

	var lastInsertID int64
	var affected uint64

	for _, valTuple := range rows {
		row := make(storage.Row)
		for i, val := range valTuple {
			if i >= len(colNames) {
				break
			}
			v, err := evalExpr(val)
			if err != nil {
				return nil, err
			}
			row[colNames[i]] = v
		}

		id, err := tbl.Insert(row)
		if err != nil {
			return nil, err
		}
		lastInsertID = id
		affected++
	}

	return &Result{
		AffectedRows: affected,
		InsertID:     uint64(lastInsertID),
	}, nil
}

// buildFromExpr builds rows from any TableExpr (AliasedTableExpr or JoinTableExpr).
// Each row has both un-prefixed keys (for backwards compat with single-table queries)
// and "alias.col" prefixed keys (for JOIN disambiguation).
func (e *Executor) buildFromExpr(expr sqlparser.TableExpr) ([]storage.Row, error) {
	switch te := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		alias, tableName, err := extractTableAliasFromAliased(te)
		if err != nil {
			return nil, err
		}
		tbl, err := e.Storage.GetTable(e.CurrentDB, tableName)
		if err != nil {
			return nil, err
		}
		raw := tbl.Scan()
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
	case *sqlparser.JoinTableExpr:
		return e.buildJoinedRowsFromJoin(te)
	default:
		return nil, fmt.Errorf("unsupported table expression: %T", expr)
	}
}

func (e *Executor) buildJoinedRowsFromJoin(join *sqlparser.JoinTableExpr) ([]storage.Row, error) {
	leftRows, err := e.buildFromExpr(join.LeftExpr)
	if err != nil {
		return nil, err
	}

	rightAlias, rightTableName, err := extractTableAlias(join.RightExpr)
	if err != nil {
		return nil, err
	}
	rightTbl, err := e.Storage.GetTable(e.CurrentDB, rightTableName)
	if err != nil {
		return nil, err
	}
	rightRows := rightTbl.Scan()

	isLeft := join.Join == sqlparser.LeftJoinType || join.Join == sqlparser.NaturalLeftJoinType

	var result []storage.Row
	for _, leftRow := range leftRows {
		matched := false
		for _, rightRow := range rightRows {
			combined := make(storage.Row)
			for k, v := range leftRow {
				combined[k] = v
			}
			for k, v := range rightRow {
				combined[k] = v
				combined[rightAlias+"."+k] = v
			}

			// Evaluate ON condition
			if join.Condition != nil && join.Condition.On != nil {
				match, err := evalWhere(join.Condition.On, combined)
				if err != nil {
					return nil, err
				}
				if !match {
					continue
				}
			}
			result = append(result, combined)
			matched = true
		}

		// LEFT JOIN: include left row with NULLs for right columns when no match
		if isLeft && !matched {
			combined := make(storage.Row)
			for k, v := range leftRow {
				combined[k] = v
			}
			for _, col := range rightTbl.Def.Columns {
				combined[col.Name] = nil
				combined[rightAlias+"."+col.Name] = nil
			}
			result = append(result, combined)
		}
	}
	return result, nil
}

func extractTableAlias(expr sqlparser.TableExpr) (alias, tableName string, err error) {
	switch te := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		return extractTableAliasFromAliased(te)
	default:
		return "", "", fmt.Errorf("expected AliasedTableExpr on right side of JOIN, got %T", expr)
	}
}

func extractTableAliasFromAliased(te *sqlparser.AliasedTableExpr) (alias, tableName string, err error) {
	tName := sqlparser.String(te.Expr)
	tName = strings.Trim(tName, "`")
	al := tName
	if !te.As.IsEmpty() {
		al = te.As.String()
	}
	return al, tName, nil
}

func (e *Executor) execSelect(stmt *sqlparser.Select) (*Result, error) {
	// Handle SELECT without FROM (e.g., SELECT 1, SELECT @@version_comment)
	if len(stmt.From) == 0 {
		return e.execSelectNoFrom(stmt)
	}

	// Build rows from FROM clause (handles single table and JOINs)
	allRows, err := e.buildFromExpr(stmt.From[0])
	if err != nil {
		return nil, err
	}

	// Apply WHERE filter
	if stmt.Where != nil {
		filtered := make([]storage.Row, 0)
		for _, row := range allRows {
			match, err := evalWhere(stmt.Where.Expr, row)
			if err != nil {
				return nil, err
			}
			if match {
				filtered = append(filtered, row)
			}
		}
		allRows = filtered
	}

	// Check if we have GROUP BY or aggregate functions
	hasGroupBy := stmt.GroupBy != nil && len(stmt.GroupBy.Exprs) > 0
	hasAggregates := selectExprsHaveAggregates(stmt.SelectExprs.Exprs)

	if hasGroupBy || hasAggregates {
		return e.execSelectGroupBy(stmt, allRows)
	}

	// Build result columns and rows (non-aggregate path)
	colNames, colExprs, err := e.resolveSelectExprs(stmt.SelectExprs.Exprs, allRows)
	if err != nil {
		return nil, err
	}

	resultRows := make([][]interface{}, 0, len(allRows))
	for _, row := range allRows {
		resultRow := make([]interface{}, len(colExprs))
		for i, expr := range colExprs {
			val, err := evalRowExpr(expr, row)
			if err != nil {
				return nil, err
			}
			resultRow[i] = val
		}
		resultRows = append(resultRows, resultRow)
	}

	// Apply ORDER BY
	if stmt.OrderBy != nil {
		resultRows, err = applyOrderBy(stmt.OrderBy, colNames, resultRows)
		if err != nil {
			return nil, err
		}
	}

	// Apply LIMIT
	if stmt.Limit != nil {
		resultRows, err = applyLimit(stmt.Limit, resultRows)
		if err != nil {
			return nil, err
		}
	}

	return &Result{
		Columns:     colNames,
		Rows:        resultRows,
		IsResultSet: true,
	}, nil
}

// selectExprsHaveAggregates returns true if any select expression is an aggregate function.
func selectExprsHaveAggregates(exprs []sqlparser.SelectExpr) bool {
	for _, expr := range exprs {
		ae, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}
		if isAggregateExpr(ae.Expr) {
			return true
		}
	}
	return false
}

func isAggregateExpr(expr sqlparser.Expr) bool {
	switch expr.(type) {
	case *sqlparser.CountStar, *sqlparser.Count,
		*sqlparser.Sum, *sqlparser.Max, *sqlparser.Min, *sqlparser.Avg:
		return true
	}
	return false
}

// execSelectGroupBy handles SELECT with GROUP BY or aggregate functions.
func (e *Executor) execSelectGroupBy(stmt *sqlparser.Select, allRows []storage.Row) (*Result, error) {
	type group struct {
		key  string
		rows []storage.Row
	}

	var groups []group
	groupIndex := make(map[string]int)

	if stmt.GroupBy != nil && len(stmt.GroupBy.Exprs) > 0 {
		for _, row := range allRows {
			key := computeGroupKey(stmt.GroupBy.Exprs, row)
			if idx, ok := groupIndex[key]; ok {
				groups[idx].rows = append(groups[idx].rows, row)
			} else {
				groupIndex[key] = len(groups)
				groups = append(groups, group{key: key, rows: []storage.Row{row}})
			}
		}
	} else {
		// No GROUP BY but has aggregates: treat all rows as one group
		groups = []group{{key: "", rows: allRows}}
	}

	// Compute column names
	colNames := make([]string, 0, len(stmt.SelectExprs.Exprs))
	for _, expr := range stmt.SelectExprs.Exprs {
		switch se := expr.(type) {
		case *sqlparser.AliasedExpr:
			if !se.As.IsEmpty() {
				colNames = append(colNames, se.As.String())
			} else if isAggregateExpr(se.Expr) {
				colNames = append(colNames, sqlparser.String(se.Expr))
			} else if colName, ok := se.Expr.(*sqlparser.ColName); ok {
				colNames = append(colNames, colName.Name.String())
			} else {
				colNames = append(colNames, sqlparser.String(se.Expr))
			}
		default:
			return nil, fmt.Errorf("unsupported select expression in GROUP BY: %T", expr)
		}
	}

	// Compute result rows per group
	resultRows := make([][]interface{}, 0, len(groups))
	for _, g := range groups {
		repRow := storage.Row{}
		if len(g.rows) > 0 {
			repRow = g.rows[0]
		}
		resultRow := make([]interface{}, 0, len(stmt.SelectExprs.Exprs))
		for _, expr := range stmt.SelectExprs.Exprs {
			ae, ok := expr.(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("unsupported select expression in GROUP BY: %T", expr)
			}
			val, err := evalAggregateExpr(ae.Expr, g.rows, repRow)
			if err != nil {
				return nil, err
			}
			resultRow = append(resultRow, val)
		}
		resultRows = append(resultRows, resultRow)
	}

	// Apply HAVING
	if stmt.Having != nil {
		filtered := make([][]interface{}, 0)
		for _, row := range resultRows {
			havingRow := make(storage.Row)
			for i, col := range colNames {
				havingRow[col] = row[i]
			}
			match, err := evalWhere(stmt.Having.Expr, havingRow)
			if err != nil {
				return nil, err
			}
			if match {
				filtered = append(filtered, row)
			}
		}
		resultRows = filtered
	}

	// Apply ORDER BY
	var err error
	if stmt.OrderBy != nil {
		resultRows, err = applyOrderBy(stmt.OrderBy, colNames, resultRows)
		if err != nil {
			return nil, err
		}
	}

	// Apply LIMIT
	if stmt.Limit != nil {
		resultRows, err = applyLimit(stmt.Limit, resultRows)
		if err != nil {
			return nil, err
		}
	}

	return &Result{
		Columns:     colNames,
		Rows:        resultRows,
		IsResultSet: true,
	}, nil
}

// computeGroupKey builds a string key for a row based on GROUP BY expressions.
func computeGroupKey(groupByExprs []sqlparser.Expr, row storage.Row) string {
	parts := make([]string, 0, len(groupByExprs))
	for _, expr := range groupByExprs {
		val, _ := evalRowExpr(expr, row)
		parts = append(parts, fmt.Sprintf("%v", val))
	}
	return strings.Join(parts, "\x00")
}

// evalAggregateExpr evaluates an expression that may be an aggregate function over a group.
func evalAggregateExpr(expr sqlparser.Expr, groupRows []storage.Row, repRow storage.Row) (interface{}, error) {
	switch e := expr.(type) {
	case *sqlparser.CountStar:
		return int64(len(groupRows)), nil
	case *sqlparser.Count:
		if len(e.Args) == 0 {
			return int64(len(groupRows)), nil
		}
		count := int64(0)
		for _, row := range groupRows {
			val, err := evalRowExpr(e.Args[0], row)
			if err != nil {
				return nil, err
			}
			if val != nil {
				count++
			}
		}
		return count, nil
	case *sqlparser.Sum:
		sum := float64(0)
		hasVal := false
		for _, row := range groupRows {
			val, err := evalRowExpr(e.Arg, row)
			if err != nil {
				return nil, err
			}
			if val != nil {
				sum += toFloat(val)
				hasVal = true
			}
		}
		if !hasVal {
			return nil, nil
		}
		if sum == float64(int64(sum)) {
			return int64(sum), nil
		}
		return sum, nil
	case *sqlparser.Max:
		var maxVal interface{}
		for _, row := range groupRows {
			val, err := evalRowExpr(e.Arg, row)
			if err != nil {
				return nil, err
			}
			if val == nil {
				continue
			}
			if maxVal == nil || compareNumeric(val, maxVal) > 0 {
				maxVal = val
			}
		}
		return maxVal, nil
	case *sqlparser.Min:
		var minVal interface{}
		for _, row := range groupRows {
			val, err := evalRowExpr(e.Arg, row)
			if err != nil {
				return nil, err
			}
			if val == nil {
				continue
			}
			if minVal == nil || compareNumeric(val, minVal) < 0 {
				minVal = val
			}
		}
		return minVal, nil
	case *sqlparser.Avg:
		sum := float64(0)
		count := int64(0)
		for _, row := range groupRows {
			val, err := evalRowExpr(e.Arg, row)
			if err != nil {
				return nil, err
			}
			if val != nil {
				sum += toFloat(val)
				count++
			}
		}
		if count == 0 {
			return nil, nil
		}
		return sum / float64(count), nil
	}
	// Non-aggregate: return value from representative row
	return evalRowExpr(expr, repRow)
}

// resolveSelectExprs returns column names and original expressions for non-aggregate SELECTs.
// It handles star expansion using actual row data (needed for JOINs).
func (e *Executor) resolveSelectExprs(exprs []sqlparser.SelectExpr, rows []storage.Row) ([]string, []sqlparser.Expr, error) {
	cols := make([]string, 0)
	colExprs := make([]sqlparser.Expr, 0)

	for _, expr := range exprs {
		switch se := expr.(type) {
		case *sqlparser.StarExpr:
			// Expand star using the first row's un-prefixed keys.
			if len(rows) > 0 {
				seen := make(map[string]bool)
				for k := range rows[0] {
					if !strings.Contains(k, ".") && !seen[k] {
						seen[k] = true
						cols = append(cols, k)
						colExprs = append(colExprs, &sqlparser.ColName{Name: sqlparser.NewIdentifierCI(k)})
					}
				}
			}
		case *sqlparser.AliasedExpr:
			name := ""
			if !se.As.IsEmpty() {
				name = se.As.String()
			} else if colName, ok := se.Expr.(*sqlparser.ColName); ok {
				name = colName.Name.String()
			} else {
				name = sqlparser.String(se.Expr)
			}
			cols = append(cols, name)
			colExprs = append(colExprs, se.Expr)
		default:
			return nil, nil, fmt.Errorf("unsupported select expression: %T", se)
		}
	}
	return cols, colExprs, nil
}

func (e *Executor) execSelectNoFrom(stmt *sqlparser.Select) (*Result, error) {
	colNames := make([]string, 0)
	values := make([]interface{}, 0)

	for _, expr := range stmt.SelectExprs.Exprs {
		switch se := expr.(type) {
		case *sqlparser.AliasedExpr:
			name := ""
			if !se.As.IsEmpty() {
				name = se.As.String()
			} else {
				name = sqlparser.String(se.Expr)
			}
			colNames = append(colNames, name)

			v, err := evalExpr(se.Expr)
			if err != nil {
				return nil, err
			}
			values = append(values, v)
		default:
			return nil, fmt.Errorf("unsupported select expression: %T", se)
		}
	}

	return &Result{
		Columns:     colNames,
		Rows:        [][]interface{}{values},
		IsResultSet: true,
	}, nil
}

func (e *Executor) execUpdate(stmt *sqlparser.Update) (*Result, error) {
	if len(stmt.TableExprs) == 0 {
		return nil, fmt.Errorf("no table specified")
	}

	tableName := ""
	switch te := stmt.TableExprs[0].(type) {
	case *sqlparser.AliasedTableExpr:
		tableName = sqlparser.String(te.Expr)
		tableName = strings.Trim(tableName, "`")
	default:
		return nil, fmt.Errorf("unsupported table expression: %T", te)
	}

	tbl, err := e.Storage.GetTable(e.CurrentDB, tableName)
	if err != nil {
		return nil, err
	}

	tbl.Lock()
	defer tbl.Unlock()

	var affected uint64
	for i, row := range tbl.Rows {
		match := true
		if stmt.Where != nil {
			m, err := evalWhere(stmt.Where.Expr, row)
			if err != nil {
				return nil, err
			}
			match = m
		}
		if !match {
			continue
		}

		for _, upd := range stmt.Exprs {
			colName := upd.Name.Name.String()
			val, err := evalExpr(upd.Expr)
			if err != nil {
				return nil, err
			}
			tbl.Rows[i][colName] = val
		}
		affected++
	}

	return &Result{AffectedRows: affected}, nil
}

func (e *Executor) execDelete(stmt *sqlparser.Delete) (*Result, error) {
	if len(stmt.TableExprs) == 0 {
		return nil, fmt.Errorf("no table specified")
	}

	tableName := ""
	switch te := stmt.TableExprs[0].(type) {
	case *sqlparser.AliasedTableExpr:
		tableName = sqlparser.String(te.Expr)
		tableName = strings.Trim(tableName, "`")
	default:
		return nil, fmt.Errorf("unsupported table expression: %T", te)
	}

	tbl, err := e.Storage.GetTable(e.CurrentDB, tableName)
	if err != nil {
		return nil, err
	}

	tbl.Lock()
	defer tbl.Unlock()

	newRows := make([]storage.Row, 0)
	var affected uint64
	for _, row := range tbl.Rows {
		match := true
		if stmt.Where != nil {
			m, err := evalWhere(stmt.Where.Expr, row)
			if err != nil {
				return nil, err
			}
			match = m
		}
		if match {
			affected++
		} else {
			newRows = append(newRows, row)
		}
	}
	tbl.Rows = newRows

	return &Result{AffectedRows: affected}, nil
}

// columnDefFromAST converts a vitess ColumnDefinition into our catalog.ColumnDef.
func columnDefFromAST(col *sqlparser.ColumnDefinition) catalog.ColumnDef {
	colDef := catalog.ColumnDef{
		Name:     col.Name.String(),
		Type:     col.Type.Type,
		Nullable: true, // default nullable unless NOT NULL specified
	}
	if col.Type.Options != nil {
		if col.Type.Options.Null != nil {
			colDef.Nullable = *col.Type.Options.Null
		}
		if col.Type.Options.Autoincrement {
			colDef.AutoIncrement = true
		}
		if col.Type.Options.Default != nil {
			defStr := sqlparser.String(col.Type.Options.Default)
			colDef.Default = &defStr
		}
		if col.Type.Options.KeyOpt == 1 { // colKeyPrimary
			colDef.PrimaryKey = true
		}
		if col.Type.Options.KeyOpt == 2 { // colKeyUnique
			colDef.Unique = true
		}
	}
	return colDef
}

func (e *Executor) execAlterTable(stmt *sqlparser.AlterTable) (*Result, error) {
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return nil, err
	}

	tableName := stmt.Table.Name.String()

	// Ensure the storage table exists.
	tbl, err := e.Storage.GetTable(e.CurrentDB, tableName)
	if err != nil {
		return nil, err
	}

	for _, opt := range stmt.AlterOptions {
		switch op := opt.(type) {

		case *sqlparser.AddColumns:
			for _, col := range op.Columns {
				colDef := columnDefFromAST(col)
				if addErr := db.AddColumn(tableName, colDef); addErr != nil {
					return nil, addErr
				}
				// Determine the default value to fill in existing rows.
				var defVal interface{}
				if colDef.Default != nil {
					// Parse the default string as a literal if possible.
					defVal = *colDef.Default
				}
				tbl.AddColumn(colDef.Name, defVal)
			}

		case *sqlparser.DropColumn:
			colName := op.Name.Name.String()
			if dropErr := db.DropColumn(tableName, colName); dropErr != nil {
				return nil, dropErr
			}
			tbl.DropColumn(colName)

		case *sqlparser.ModifyColumn:
			colDef := columnDefFromAST(op.NewColDefinition)
			if modErr := db.ModifyColumn(tableName, colDef); modErr != nil {
				return nil, modErr
			}

		case *sqlparser.ChangeColumn:
			oldName := op.OldColumn.Name.String()
			colDef := columnDefFromAST(op.NewColDefinition)
			if chgErr := db.ChangeColumn(tableName, oldName, colDef); chgErr != nil {
				return nil, chgErr
			}
			// Rename the key in all existing rows if the column name changed.
			if oldName != colDef.Name {
				tbl.RenameColumn(oldName, colDef.Name)
			}

		case *sqlparser.AddIndexDefinition:
			// Silently accept index additions (no index enforcement in mylite).

		case *sqlparser.AddConstraintDefinition:
			// Silently accept constraint additions.

		default:
			// Unsupported ALTER option — ignore silently to stay compatible.
		}
	}

	return &Result{}, nil
}

// execDescribe handles DESCRIBE <table> and DESC <table> (parsed as *sqlparser.ExplainTab).
func (e *Executor) execDescribe(stmt *sqlparser.ExplainTab) (*Result, error) {
	return e.describeTable(stmt.Table.Name.String())
}

// describeTable returns column metadata for a table, matching MySQL DESCRIBE output.
func (e *Executor) describeTable(tableName string) (*Result, error) {
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return nil, err
	}
	tblDef, err := db.GetTable(tableName)
	if err != nil {
		return nil, err
	}

	cols := []string{"Field", "Type", "Null", "Key", "Default", "Extra"}
	rows := make([][]interface{}, 0, len(tblDef.Columns))
	for _, col := range tblDef.Columns {
		nullable := "YES"
		if !col.Nullable {
			nullable = "NO"
		}
		key := ""
		if col.PrimaryKey {
			key = "PRI"
		} else if col.Unique {
			key = "UNI"
		}
		var defVal interface{}
		if col.Default != nil {
			defVal = *col.Default
		}
		extra := ""
		if col.AutoIncrement {
			extra = "auto_increment"
		}
		rows = append(rows, []interface{}{col.Name, col.Type, nullable, key, defVal, extra})
	}

	return &Result{
		Columns:     cols,
		Rows:        rows,
		IsResultSet: true,
	}, nil
}

func (e *Executor) execShow(stmt *sqlparser.Show, query string) (*Result, error) {
	// Dispatch based on the structured ShowBasic command type when available.
	if basic, ok := stmt.Internal.(*sqlparser.ShowBasic); ok {
		switch basic.Command {
		case sqlparser.Column:
			// SHOW COLUMNS FROM <table> / SHOW FULL COLUMNS FROM <table>
			return e.describeTable(basic.Tbl.Name.String())
		}
	}

	upper := strings.ToUpper(strings.TrimSpace(query))

	if strings.HasPrefix(upper, "SHOW TABLES") {
		db, err := e.Catalog.GetDatabase(e.CurrentDB)
		if err != nil {
			return nil, err
		}
		tables := db.ListTables()
		rows := make([][]interface{}, len(tables))
		for i, t := range tables {
			rows[i] = []interface{}{t}
		}
		return &Result{
			Columns:     []string{fmt.Sprintf("Tables_in_%s", e.CurrentDB)},
			Rows:        rows,
			IsResultSet: true,
		}, nil
	}

	if strings.HasPrefix(upper, "SHOW DATABASES") {
		dbs := e.Catalog.ListDatabases()
		rows := make([][]interface{}, len(dbs))
		for i, d := range dbs {
			rows[i] = []interface{}{d}
		}
		return &Result{
			Columns:     []string{"Database"},
			Rows:        rows,
			IsResultSet: true,
		}, nil
	}

	// Accept other SHOW statements silently
	return &Result{
		Columns:     []string{"Value"},
		Rows:        [][]interface{}{},
		IsResultSet: true,
	}, nil
}

func (e *Executor) resolveSelectColumns(exprs []sqlparser.SelectExpr, def *catalog.TableDef) ([]string, error) {
	cols := make([]string, 0)
	for _, expr := range exprs {
		switch se := expr.(type) {
		case *sqlparser.StarExpr:
			for _, col := range def.Columns {
				cols = append(cols, col.Name)
			}
		case *sqlparser.AliasedExpr:
			if !se.As.IsEmpty() {
				cols = append(cols, se.As.String())
			} else if colName, ok := se.Expr.(*sqlparser.ColName); ok {
				cols = append(cols, colName.Name.String())
			} else {
				cols = append(cols, sqlparser.String(se.Expr))
			}
		default:
			return nil, fmt.Errorf("unsupported select expression: %T", se)
		}
	}
	return cols, nil
}

func evalExpr(expr sqlparser.Expr) (interface{}, error) {
	switch v := expr.(type) {
	case *sqlparser.Literal:
		switch v.Type {
		case sqlparser.IntVal:
			n, err := strconv.ParseInt(v.Val, 10, 64)
			if err != nil {
				return nil, err
			}
			return n, nil
		case sqlparser.FloatVal:
			f, err := strconv.ParseFloat(v.Val, 64)
			if err != nil {
				return nil, err
			}
			return f, nil
		case sqlparser.StrVal:
			return v.Val, nil
		case sqlparser.HexVal:
			return v.Val, nil
		}
	case *sqlparser.NullVal:
		return nil, nil
	case sqlparser.BoolVal:
		return bool(v), nil
	case *sqlparser.ColName:
		// Return column name as string for use in row lookup
		return v.Name.String(), nil
	case *sqlparser.Variable:
		// Handle @@variables
		name := strings.ToLower(v.Name.String())
		switch name {
		case "version_comment":
			return "mylite", nil
		case "version":
			return "8.4.0-mylite", nil
		}
		return "", nil
	case *sqlparser.Default:
		return nil, nil
	case *sqlparser.UnaryExpr:
		val, err := evalExpr(v.Expr)
		if err != nil {
			return nil, err
		}
		if v.Operator == sqlparser.UMinusOp {
			switch n := val.(type) {
			case int64:
				return -n, nil
			case float64:
				return -n, nil
			}
		}
		return val, nil
	}
	return nil, fmt.Errorf("unsupported expression: %T (%s)", expr, sqlparser.String(expr))
}

func evalWhere(expr sqlparser.Expr, row storage.Row) (bool, error) {
	switch v := expr.(type) {
	case *sqlparser.ComparisonExpr:
		left, err := evalRowExpr(v.Left, row)
		if err != nil {
			return false, err
		}
		right, err := evalRowExpr(v.Right, row)
		if err != nil {
			return false, err
		}
		return compareValues(left, right, v.Operator)
	case *sqlparser.AndExpr:
		l, err := evalWhere(v.Left, row)
		if err != nil {
			return false, err
		}
		r, err := evalWhere(v.Right, row)
		if err != nil {
			return false, err
		}
		return l && r, nil
	case *sqlparser.OrExpr:
		l, err := evalWhere(v.Left, row)
		if err != nil {
			return false, err
		}
		r, err := evalWhere(v.Right, row)
		if err != nil {
			return false, err
		}
		return l || r, nil
	case *sqlparser.IsExpr:
		val, err := evalRowExpr(v.Left, row)
		if err != nil {
			return false, err
		}
		switch v.Right {
		case sqlparser.IsNullOp:
			return val == nil, nil
		case sqlparser.IsNotNullOp:
			return val != nil, nil
		case sqlparser.IsTrueOp:
			return val == true || val == int64(1), nil
		case sqlparser.IsFalseOp:
			return val == false || val == int64(0), nil
		}
	}
	return false, fmt.Errorf("unsupported WHERE expression: %T", expr)
}

func evalRowExpr(expr sqlparser.Expr, row storage.Row) (interface{}, error) {
	switch v := expr.(type) {
	case *sqlparser.ColName:
		colName := v.Name.String()
		// Try qualified lookup first (alias.col) if qualifier is set
		if !v.Qualifier.IsEmpty() {
			qualified := v.Qualifier.Name.String() + "." + colName
			if val, ok := row[qualified]; ok {
				return val, nil
			}
		}
		// Fall back to un-prefixed lookup
		val, ok := row[colName]
		if !ok {
			return nil, nil
		}
		return val, nil
	default:
		return evalExpr(expr)
	}
}

func compareValues(left, right interface{}, op sqlparser.ComparisonExprOperator) (bool, error) {
	// Handle NULL comparisons
	if left == nil || right == nil {
		return false, nil // NULL comparisons always false in SQL (except IS NULL)
	}

	switch op {
	case sqlparser.EqualOp:
		return fmt.Sprintf("%v", left) == fmt.Sprintf("%v", right), nil
	case sqlparser.NotEqualOp:
		return fmt.Sprintf("%v", left) != fmt.Sprintf("%v", right), nil
	case sqlparser.LessThanOp:
		return compareNumeric(left, right) < 0, nil
	case sqlparser.GreaterThanOp:
		return compareNumeric(left, right) > 0, nil
	case sqlparser.LessEqualOp:
		return compareNumeric(left, right) <= 0, nil
	case sqlparser.GreaterEqualOp:
		return compareNumeric(left, right) >= 0, nil
	}
	return false, fmt.Errorf("unsupported comparison operator: %s", op.ToString())
}

func compareNumeric(a, b interface{}) int {
	fa := toFloat(a)
	fb := toFloat(b)
	if fa < fb {
		return -1
	}
	if fa > fb {
		return 1
	}
	return 0
}

func toFloat(v interface{}) float64 {
	switch n := v.(type) {
	case int64:
		return float64(n)
	case float64:
		return n
	case string:
		f, _ := strconv.ParseFloat(n, 64)
		return f
	case bool:
		if n {
			return 1
		}
		return 0
	}
	return 0
}

func applyOrderBy(orderBy sqlparser.OrderBy, colNames []string, rows [][]interface{}) ([][]interface{}, error) {
	// Simple single-column ordering for now
	if len(orderBy) == 0 {
		return rows, nil
	}

	order := orderBy[0]
	colName := sqlparser.String(order.Expr)
	colName = strings.Trim(colName, "`")

	colIdx := -1
	for i, c := range colNames {
		if c == colName {
			colIdx = i
			break
		}
	}
	if colIdx == -1 {
		return rows, nil
	}

	// Bubble sort for simplicity
	asc := order.Direction == sqlparser.AscOrder || order.Direction == 0
	for i := 0; i < len(rows); i++ {
		for j := i + 1; j < len(rows); j++ {
			cmp := compareNumeric(rows[i][colIdx], rows[j][colIdx])
			if (asc && cmp > 0) || (!asc && cmp < 0) {
				rows[i], rows[j] = rows[j], rows[i]
			}
		}
	}
	return rows, nil
}

func applyLimit(limit *sqlparser.Limit, rows [][]interface{}) ([][]interface{}, error) {
	if limit.Rowcount == nil {
		return rows, nil
	}

	lim, err := evalExpr(limit.Rowcount)
	if err != nil {
		return nil, err
	}
	n, ok := lim.(int64)
	if !ok {
		return rows, nil
	}

	offset := int64(0)
	if limit.Offset != nil {
		off, err := evalExpr(limit.Offset)
		if err != nil {
			return nil, err
		}
		offset, _ = off.(int64)
	}

	if offset >= int64(len(rows)) {
		return [][]interface{}{}, nil
	}
	end := offset + n
	if end > int64(len(rows)) {
		end = int64(len(rows))
	}
	return rows[offset:end], nil
}
