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

// Executor handles SQL execution.
type Executor struct {
	Catalog   *catalog.Catalog
	Storage   *storage.Engine
	CurrentDB string
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
	case *sqlparser.Show:
		return e.execShow(s, query)
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

func (e *Executor) execSelect(stmt *sqlparser.Select) (*Result, error) {
	// Handle SELECT without FROM (e.g., SELECT 1, SELECT @@version_comment)
	if len(stmt.From) == 0 {
		return e.execSelectNoFrom(stmt)
	}

	tableName := ""
	switch from := stmt.From[0].(type) {
	case *sqlparser.AliasedTableExpr:
		tableName = sqlparser.String(from.Expr)
		// Remove backticks
		tableName = strings.Trim(tableName, "`")
	default:
		return nil, fmt.Errorf("unsupported FROM clause: %T", from)
	}

	tbl, err := e.Storage.GetTable(e.CurrentDB, tableName)
	if err != nil {
		return nil, err
	}

	allRows := tbl.Scan()

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

	// Build result columns and rows
	colNames, err := e.resolveSelectColumns(stmt.SelectExprs.Exprs, tbl.Def)
	if err != nil {
		return nil, err
	}

	resultRows := make([][]interface{}, 0, len(allRows))
	for _, row := range allRows {
		resultRow := make([]interface{}, len(colNames))
		for i, col := range colNames {
			resultRow[i] = row[col]
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

func (e *Executor) execShow(stmt *sqlparser.Show, query string) (*Result, error) {
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
