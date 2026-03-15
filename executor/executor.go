package executor

import (
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

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

// fullSnapshot holds a complete snapshot of all databases for MYLITE SNAPSHOT commands.
type fullSnapshot struct {
	storageSnap map[string]*storage.DatabaseSnapshot
	catalogSnap map[string]map[string]*catalog.TableDef
}

// cteTable holds pre-computed rows for a Common Table Expression.
type cteTable struct {
	columns []string
	rows    []storage.Row
}

// Executor handles SQL execution.
type Executor struct {
	Catalog        *catalog.Catalog
	Storage        *storage.Engine
	CurrentDB      string
	inTransaction  bool
	savepoint      *txSavepoint
	snapshots      map[string]*fullSnapshot
	lastInsertID   int64
	// cteMap holds CTE virtual tables for the currently executing query.
	cteMap         map[string]*cteTable
	// sqlMode stores the current SQL mode (e.g. "TRADITIONAL", "STRICT_TRANS_TABLES").
	sqlMode        string
	// sqlAutoIsNull enables MySQL sql_auto_is_null behavior.
	sqlAutoIsNull  bool
	// lastAutoIncID stores the last auto-increment ID for sql_auto_is_null support.
	lastAutoIncID  int64
	// fixedTimestamp holds a fixed time for SET TIMESTAMP=N support.
	fixedTimestamp *time.Time
}

func New(cat *catalog.Catalog, store *storage.Engine) *Executor {
	return &Executor{
		Catalog:   cat,
		Storage:   store,
		CurrentDB: "test",
		snapshots: make(map[string]*fullSnapshot),
	}
}

// mysqlError formats an error message in MySQL error style.
// Format: "ERROR <code> (<state>): <message>"
func mysqlError(code int, state, message string) error {
	return fmt.Errorf("ERROR %d (%s): %s", code, state, message)
}

// Execute parses and executes a SQL statement.
// matchLike matches a string against a SQL LIKE pattern.
// % matches any sequence of characters, _ matches any single character.
func matchLike(s, pattern string) bool {
	return matchLikeHelper(s, pattern, 0, 0)
}

func matchLikeHelper(s, p string, si, pi int) bool {
	for pi < len(p) {
		if p[pi] == '%' {
			pi++
			for si <= len(s) {
				if matchLikeHelper(s, p, si, pi) {
					return true
				}
				si++
			}
			return false
		} else if p[pi] == '_' {
			if si >= len(s) {
				return false
			}
			si++
			pi++
		} else {
			if si >= len(s) || strings.ToLower(string(s[si])) != strings.ToLower(string(p[pi])) {
				return false
			}
			si++
			pi++
		}
	}
	return si == len(s)
}

// normalizeTypeAliases replaces MySQL type aliases that the vitess parser
// doesn't support with their canonical equivalents.
func normalizeTypeAliases(query string) string {
	upper := strings.ToUpper(query)
	// Only apply to CREATE TABLE or ALTER TABLE statements
	if !strings.Contains(upper, "CREATE TABLE") && !strings.Contains(upper, "ALTER TABLE") {
		return query
	}
	// Replace DOUBLE PRECISION with DOUBLE (case-insensitive, word-boundary aware)
	result := replaceTypeWord(query, "DOUBLE PRECISION", "DOUBLE")
	result = replaceTypeWord(result, "DEC", "DECIMAL")
	result = replaceTypeWord(result, "FIXED", "DECIMAL")
	result = replaceTypeWord(result, "NUMERIC", "DECIMAL")
	result = replaceTypeWord(result, "SERIAL", "BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE")
	return result
}

// replaceTypeWord replaces a type keyword in a SQL query case-insensitively,
// only when it appears as a whole word (not part of a larger identifier)
// and not inside a quoted string.
func replaceTypeWord(query, old, replacement string) string {
	upper := strings.ToUpper(query)
	oldUpper := strings.ToUpper(old)
	idx := 0
	for {
		pos := strings.Index(upper[idx:], oldUpper)
		if pos == -1 {
			break
		}
		absPos := idx + pos
		endPos := absPos + len(old)

		// Check if we're inside a quoted string
		inQuote := false
		quoteChar := byte(0)
		for i := 0; i < absPos; i++ {
			ch := query[i]
			if !inQuote && (ch == '\'' || ch == '"') {
				inQuote = true
				quoteChar = ch
			} else if inQuote && ch == quoteChar {
				inQuote = false
			}
		}
		if inQuote {
			idx = endPos
			continue
		}

		// Check word boundaries
		if absPos > 0 {
			ch := query[absPos-1]
			if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '_' {
				idx = endPos
				continue
			}
		}
		if endPos < len(query) {
			ch := query[endPos]
			if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '_' || (ch >= '0' && ch <= '9') {
				idx = endPos
				continue
			}
		}
		query = query[:absPos] + replacement + query[endPos:]
		upper = strings.ToUpper(query)
		idx = absPos + len(replacement)
	}
	return query
}

func (e *Executor) Execute(query string) (*Result, error) {
	// Handle MYLITE control commands before passing to the SQL parser.
	trimmed := strings.TrimSpace(query)
	upper := strings.ToUpper(trimmed)
	if strings.HasPrefix(upper, "MYLITE ") {
		return e.execMyliteCommand(trimmed)
	}

	// Normalize SQL type aliases that vitess parser doesn't support
	query = normalizeTypeAliases(query)

	// Handle CREATE TRIGGER before vitess parser (it cannot parse triggers)
	if strings.HasPrefix(upper, "CREATE TRIGGER") {
		return e.execCreateTrigger(trimmed)
	}
	// Handle DROP TRIGGER
	if strings.HasPrefix(upper, "DROP TRIGGER") {
		return e.execDropTrigger(trimmed)
	}
	// Handle CREATE PROCEDURE (with BEGIN...END body that vitess can't parse)
	if strings.HasPrefix(upper, "CREATE PROCEDURE") && strings.Contains(upper, "BEGIN") {
		return e.execCreateProcedure(trimmed)
	}
	// Handle DROP PROCEDURE with IF EXISTS (vitess may not parse all variants)
	if strings.HasPrefix(upper, "DROP PROCEDURE") {
		return e.execDropProcedureFallback(trimmed)
	}
	// Handle CALL procedure
	if strings.HasPrefix(upper, "CALL ") {
		return e.execCallProcedure(trimmed)
	}

	stmt, err := sqlparser.NewTestParser().Parse(query)
	if err != nil {
		// Accept statements that Vitess parser doesn't support
		if strings.HasPrefix(upper, "SET ") {
			e.handleRawSet(trimmed)
			return &Result{}, nil
		}
		if strings.HasPrefix(upper, "USE ") {
			nearText := strings.TrimPrefix(trimmed, "USE ")
			nearText = strings.TrimPrefix(nearText, "use ")
			return nil, mysqlError(1064, "42000", fmt.Sprintf("You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '%s' at line 1", nearText))
		}
		if strings.HasPrefix(upper, "DROP FUNCTION") ||
			strings.HasPrefix(upper, "CREATE FUNCTION") ||
			strings.HasPrefix(upper, "CREATE EVENT") ||
			strings.HasPrefix(upper, "DROP EVENT") ||
			strings.HasPrefix(upper, "CREATE USER") ||
			strings.HasPrefix(upper, "DROP USER") ||
			strings.HasPrefix(upper, "ALTER USER") ||
			strings.HasPrefix(upper, "GRANT ") ||
			strings.HasPrefix(upper, "REVOKE ") ||
			strings.HasPrefix(upper, "FLUSH ") ||
			strings.HasPrefix(upper, "RESET ") ||
			strings.HasPrefix(upper, "HANDLER ") ||
			strings.HasPrefix(upper, "INSTALL ") ||
			strings.HasPrefix(upper, "UNINSTALL ") ||
			strings.HasPrefix(upper, "CHECKSUM ") ||
			strings.HasPrefix(upper, "REPAIR ") ||
			strings.HasPrefix(upper, "OPTIMIZE ") ||
			strings.HasPrefix(upper, "CHECK ") ||
			strings.HasPrefix(upper, "DELIMITER ") ||
			strings.HasPrefix(upper, "DECLARE ") ||
			strings.HasPrefix(upper, "RETURN ") ||
			strings.HasPrefix(upper, "OPEN ") ||
			strings.HasPrefix(upper, "CLOSE ") ||
			strings.HasPrefix(upper, "FETCH ") ||
			strings.HasPrefix(upper, "SIGNAL ") ||
			strings.HasPrefix(upper, "RESIGNAL") ||
			strings.HasPrefix(upper, "GET DIAGNOSTICS") ||
			strings.HasPrefix(upper, "XA ") ||
			strings.HasPrefix(upper, "SAVEPOINT") ||
			strings.HasPrefix(upper, "RELEASE SAVEPOINT") ||
			strings.HasPrefix(upper, "ALTER PROCEDURE") ||
			strings.HasPrefix(upper, "ALTER FUNCTION") ||
			strings.HasPrefix(upper, "CHANGE ") ||
			strings.HasPrefix(upper, "START ") ||
			strings.HasPrefix(upper, "STOP ") ||
			strings.HasPrefix(upper, "PURGE ") ||
			strings.HasPrefix(upper, "BINLOG ") ||
			strings.HasPrefix(upper, "DO ") ||
			strings.HasPrefix(upper, "END") {
			return &Result{}, nil
		}
		// For multi-table DELETE: DELETE t1,t2 FROM t1,t2,t3 WHERE ...
		// or DELETE [QUICK] FROM t1,t2 USING t1,t2,t3 WHERE ...
		if strings.HasPrefix(upper, "DELETE ") {
			return e.execMultiTableDelete(trimmed)
		}
		return nil, mysqlError(1064, "42000", fmt.Sprintf("You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '%s' at line 1", truncateNear(trimmed)))
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
	case *sqlparser.TruncateTable:
		return e.execTruncateTable(s)
	case *sqlparser.Set:
		return e.execSet(s)
	case *sqlparser.LockTables:
		// Accept LOCK TABLES silently
		return &Result{}, nil
	case *sqlparser.UnlockTables:
		// Accept UNLOCK TABLES silently
		return &Result{}, nil
	case *sqlparser.Analyze:
		// Return a minimal ANALYZE TABLE result set for compatibility
		tableName := s.Table.Name.String()
		return &Result{
			Columns:     []string{"Table", "Op", "Msg_type", "Msg_text"},
			Rows:        [][]interface{}{{fmt.Sprintf("%s.%s", e.CurrentDB, tableName), "analyze", "status", "OK"}},
			IsResultSet: true,
		}, nil
	case *sqlparser.CallProc:
		return e.execCallProcFromAST(s)
	case *sqlparser.Load:
		// Accept LOAD DATA silently
		return &Result{}, nil
	case *sqlparser.PrepareStmt, *sqlparser.ExecuteStmt, *sqlparser.DeallocateStmt:
		// Accept PREPARE/EXECUTE/DEALLOCATE silently
		return &Result{}, nil
	case *sqlparser.AlterDatabase:
		// Accept ALTER DATABASE silently
		return &Result{}, nil
	case *sqlparser.DropProcedure:
		return e.execDropProcedureAST(s)
	case *sqlparser.CreateProcedure:
		// Simple CREATE PROCEDURE without BEGIN...END body (already handled above for complex ones)
		return &Result{}, nil
	case *sqlparser.CreateView:
		// Accept CREATE VIEW silently
		return &Result{}, nil
	case *sqlparser.DropView:
		// Accept DROP VIEW silently
		return &Result{}, nil
	case *sqlparser.Union:
		return e.execUnion(s)
	case *sqlparser.RenameTable:
		return e.execRenameTable(s)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", s)
	}
}

func (e *Executor) execRenameTable(stmt *sqlparser.RenameTable) (*Result, error) {
	for _, pair := range stmt.TablePairs {
		oldName := pair.FromTable.Name.String()
		newName := pair.ToTable.Name.String()
		// Check if target database exists
		targetDB := e.CurrentDB
		if !pair.ToTable.Qualifier.IsEmpty() {
			targetDB = pair.ToTable.Qualifier.String()
		}
		if _, err := e.Catalog.GetDatabase(targetDB); err != nil {
			return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", targetDB))
		}
		db, err := e.Catalog.GetDatabase(e.CurrentDB)
		if err != nil {
			return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", e.CurrentDB))
		}
		// Check if new name already exists
		if _, err := db.GetTable(newName); err == nil {
			return nil, mysqlError(1050, "42S01", fmt.Sprintf("Table '%s' already exists", newName))
		}
		// Get old table def
		def, err := db.GetTable(oldName)
		if err != nil {
			return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", e.CurrentDB, oldName))
		}
		// Rename in catalog
		def.Name = newName
		db.DropTable(oldName)  //nolint:errcheck
		db.CreateTable(def)    //nolint:errcheck
		// Rename in storage
		if tbl, err := e.Storage.GetTable(e.CurrentDB, oldName); err == nil {
			tbl.Def = def
			e.Storage.CreateTable(e.CurrentDB, def)
			// Copy rows
			if newTbl, err := e.Storage.GetTable(e.CurrentDB, newName); err == nil {
				newTbl.Rows = tbl.Rows
				newTbl.AutoIncrement.Store(tbl.AutoIncrementValue())
			}
			e.Storage.DropTable(e.CurrentDB, oldName)
		}
	}
	return &Result{}, nil
}

func (e *Executor) execCreateDatabase(stmt *sqlparser.CreateDatabase) (*Result, error) {
	name := stmt.DBName.String()
	// Reject creating system schemas
	sysSchemas := map[string]bool{
		"mysql": true, "information_schema": true, "performance_schema": true, "sys": true,
	}
	if sysSchemas[strings.ToLower(name)] {
		return nil, mysqlError(3802, "HY000", fmt.Sprintf("Access to system schema '%s' is rejected.", name))
	}
	err := e.Catalog.CreateDatabase(name)
	if err != nil {
		if stmt.IfNotExists {
			return &Result{}, nil
		}
		return nil, mysqlError(1007, "HY000", fmt.Sprintf("Can't create database '%s'; database exists", name))
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
		return nil, mysqlError(1008, "HY000", fmt.Sprintf("Can't drop database '%s'; database doesn't exist", name))
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
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", name))
	}
	e.CurrentDB = name
	return &Result{}, nil
}

// execSet handles parsed SET statements.
func (e *Executor) execSet(stmt *sqlparser.Set) (*Result, error) {
	for _, expr := range stmt.Exprs {
		name := strings.ToLower(expr.Var.Name.String())
		val := sqlparser.String(expr.Expr)
		val = strings.Trim(val, "'\"")
		switch name {
		case "sql_mode":
			if strings.ToUpper(val) == "DEFAULT" {
				e.sqlMode = ""
			} else {
				e.sqlMode = strings.ToUpper(val)
			}
		case "sql_auto_is_null":
			e.sqlAutoIsNull = val == "1" || strings.ToUpper(val) == "ON" || strings.ToUpper(val) == "TRUE"
		case "timestamp":
			n, err := strconv.ParseFloat(val, 64)
			if err == nil {
				if n == 0 {
					e.fixedTimestamp = nil
				} else {
					t := time.Unix(int64(n), 0)
					e.fixedTimestamp = &t
				}
			}
		}
	}
	return &Result{}, nil
}

// handleRawSet handles SET statements that the parser couldn't parse.
func (e *Executor) handleRawSet(raw string) {
	upper := strings.ToUpper(raw)
	if strings.Contains(upper, "SQL_MODE") {
		if idx := strings.Index(upper, "="); idx >= 0 {
			val := strings.TrimSpace(raw[idx+1:])
			val = strings.Trim(val, "'\"")
			val = strings.TrimSuffix(val, ";")
			val = strings.TrimSpace(val)
			if strings.ToUpper(val) == "DEFAULT" {
				e.sqlMode = ""
			} else {
				e.sqlMode = strings.ToUpper(val)
			}
		}
	}
	if strings.Contains(upper, "SQL_AUTO_IS_NULL") {
		if idx := strings.Index(upper, "="); idx >= 0 {
			val := strings.TrimSpace(raw[idx+1:])
			val = strings.Trim(val, "'\"")
			val = strings.TrimSuffix(val, ";")
			val = strings.TrimSpace(val)
			e.sqlAutoIsNull = val == "1" || strings.ToUpper(val) == "ON"
		}
	}
	if strings.Contains(upper, "TIMESTAMP") && !strings.Contains(upper, "SQL_MODE") {
		if idx := strings.Index(upper, "="); idx >= 0 {
			val := strings.TrimSpace(raw[idx+1:])
			val = strings.Trim(val, "'\"")
			val = strings.TrimSuffix(val, ";")
			val = strings.TrimSpace(val)
			n, err := strconv.ParseFloat(val, 64)
			if err == nil {
				if n == 0 {
					e.fixedTimestamp = nil
				} else {
					t := time.Unix(int64(n), 0)
					e.fixedTimestamp = &t
				}
			}
		}
	}
}

// nowTime returns the current time, respecting SET TIMESTAMP.
func (e *Executor) nowTime() time.Time {
	if e.fixedTimestamp != nil {
		return *e.fixedTimestamp
	}
	return time.Now()
}

// isStrictMode returns true when sql_mode includes STRICT_TRANS_TABLES, STRICT_ALL_TABLES, or TRADITIONAL.
func (e *Executor) isStrictMode() bool {
	return strings.Contains(e.sqlMode, "TRADITIONAL") ||
		strings.Contains(e.sqlMode, "STRICT_TRANS_TABLES") ||
		strings.Contains(e.sqlMode, "STRICT_ALL_TABLES")
}

// extractCharLength returns the max character length from a CHAR(N) or VARCHAR(N) type string.
func extractCharLength(colType string) int {
	lower := strings.ToLower(strings.TrimSpace(colType))
	var n int
	for _, prefix := range []string{"char(", "varchar(", "binary(", "varbinary("} {
		if strings.HasPrefix(lower, prefix) {
			if _, err := fmt.Sscanf(lower[len(prefix)-1:], "(%d)", &n); err == nil {
				return n
			}
		}
	}
	return 0
}

// checkDecimalRange checks if a value fits within a DECIMAL(M,D) column's range.
func checkDecimalRange(colType string, v interface{}) error {
	lower := strings.ToLower(colType)
	lower = strings.TrimSuffix(strings.TrimSpace(lower), " unsigned")
	lower = strings.TrimSpace(lower)
	var m, d int
	if n, err := fmt.Sscanf(lower, "decimal(%d,%d)", &m, &d); err == nil && n == 2 {
		f := toFloat(v)
		if f < 0 {
			f = -f
		}
		intDigits := m - d
		if intDigits <= 0 {
			intDigits = 1
		}
		maxVal := 1.0
		for i := 0; i < intDigits; i++ {
			maxVal *= 10
		}
		if f >= maxVal {
			return fmt.Errorf("out of range")
		}
	}
	return nil
}

// validateEnumSetValue validates and normalizes a value for ENUM/SET columns.
func validateEnumSetValue(colType string, v interface{}) interface{} {
	lower := strings.ToLower(colType)
	if !strings.HasPrefix(lower, "enum(") && !strings.HasPrefix(lower, "set(") {
		return v
	}
	s, ok := v.(string)
	if !ok {
		return v
	}
	isEnum := strings.HasPrefix(lower, "enum(")
	inner := ""
	if isEnum {
		inner = colType[5 : len(colType)-1]
	} else {
		inner = colType[4 : len(colType)-1]
	}
	var allowed []string
	for _, part := range splitEnumValues(inner) {
		part = strings.Trim(part, "'")
		allowed = append(allowed, part)
	}
	if isEnum {
		if s == "" {
			return s
		}
		for _, a := range allowed {
			if strings.EqualFold(s, a) {
				return a
			}
		}
		return ""
	}
	// SET validation
	if s == "" {
		return s
	}
	members := strings.Split(s, ",")
	var valid []string
	for _, m := range members {
		m = strings.TrimSpace(m)
		for _, a := range allowed {
			if strings.EqualFold(m, a) {
				valid = append(valid, a)
				break
			}
		}
	}
	return strings.Join(valid, ",")
}

func splitEnumValues(s string) []string {
	var result []string
	var current strings.Builder
	inQuote := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '\'' {
			inQuote = !inQuote
			current.WriteByte(ch)
		} else if ch == ',' && !inQuote {
			result = append(result, strings.TrimSpace(current.String()))
			current.Reset()
		} else {
			current.WriteByte(ch)
		}
	}
	rest := strings.TrimSpace(current.String())
	if rest != "" {
		result = append(result, rest)
	}
	return result
}

// formatDecimalValue formats a value for DECIMAL(M,D), DOUBLE(M,D), or FLOAT(M,D) columns.
func formatDecimalValue(colType string, v interface{}) interface{} {
	lower := strings.ToLower(colType)
	cleanLower := strings.TrimSuffix(strings.TrimSpace(lower), " unsigned")
	cleanLower = strings.TrimSpace(cleanLower)
	var prefix string
	for _, p := range []string{"decimal", "double", "float"} {
		if strings.HasPrefix(cleanLower, p+"(") {
			prefix = p
			break
		}
	}
	if prefix == "" {
		return v
	}
	var m, d int
	if n, err := fmt.Sscanf(cleanLower, prefix+"(%d,%d)", &m, &d); err == nil && n == 2 {
		f := toFloat(v)
		if d == 0 {
			return int64(f)
		}
		return fmt.Sprintf("%.*f", d, f)
	}
	return v
}

func (e *Executor) execCreateTable(stmt *sqlparser.CreateTable) (*Result, error) {
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", e.CurrentDB))
	}

	tableName := stmt.Table.Name.String()

	if stmt.TableSpec == nil {
		// CREATE TABLE ... LIKE
		if stmt.OptLike != nil {
			srcName := stmt.OptLike.LikeTable.Name.String()
			return e.execCreateTableLike(tableName, srcName)
		}
		// CREATE TABLE ... SELECT
		if stmt.Select != nil {
			selectSQL := sqlparser.String(stmt.Select)
			return e.execCreateTableSelect(tableName, selectSQL)
		}
		return &Result{}, nil
	}

	columns := make([]catalog.ColumnDef, 0)
	var primaryKeys []string

	for _, col := range stmt.TableSpec.Columns {
		// Validate ENUM/SET value lengths (MySQL max is 255 characters).
		colTypeLower := strings.ToLower(col.Type.Type)
		if colTypeLower == "enum" || colTypeLower == "set" {
			for _, ev := range col.Type.EnumValues {
				v := strings.Trim(ev, "'")
				if len(v) > 255 {
					return nil, mysqlError(1097, "HY000", fmt.Sprintf("Too long enumeration/set value for column %s.", col.Name.String()))
				}
			}
		}

		// Default: nullable unless NOT NULL is explicitly specified
		nullable := true
		if col.Type.Options != nil && col.Type.Options.Null != nil {
			nullable = *col.Type.Options.Null
		}

		colDef := catalog.ColumnDef{
			Name:     col.Name.String(),
			Type:     buildColumnTypeString(col.Type),
			Nullable: nullable,
		}

		if col.Type.Options != nil {
			if col.Type.Options.Autoincrement {
				colDef.AutoIncrement = true
			}
			if col.Type.Options.Default != nil {
				defStr := sqlparser.String(col.Type.Options.Default)
				colDef.Default = &defStr
			}
			switch col.Type.Options.KeyOpt {
			case sqlparser.ColKeyPrimary, sqlparser.ColKey: // PRIMARY KEY or KEY
				colDef.PrimaryKey = true
				colDef.Nullable = false // PRIMARY KEY implies NOT NULL
				primaryKeys = append(primaryKeys, colDef.Name)
			case sqlparser.ColKeyUnique, sqlparser.ColKeyUniqueKey:
				colDef.Unique = true
			}
		}

		// Save comment
		if col.Type.Options != nil && col.Type.Options.Comment != nil {
			colDef.Comment = col.Type.Options.Comment.Val
		}

		columns = append(columns, colDef)
	}

	// Process index definitions
	var indexes []catalog.IndexDef
	for _, idx := range stmt.TableSpec.Indexes {
		var idxCols []string
		for _, idxCol := range idx.Columns {
			colStr := idxCol.Column.String()
			if idxCol.Length != nil {
				colStr += fmt.Sprintf("(%d)", *idxCol.Length)
			}
			idxCols = append(idxCols, colStr)
		}
		if idx.Info.Type == sqlparser.IndexTypePrimary {
			primaryKeys = nil
			primaryKeys = append(primaryKeys, idxCols...)
		} else {
			isUnique := idx.Info.Type == sqlparser.IndexTypeUnique
			idxName := idx.Info.Name.String()
			if idxName == "" {
				idxName = idxCols[0]
			}
			idxComment := ""
			for _, opt := range idx.Options {
				if strings.ToUpper(opt.Name) == "COMMENT" {
					idxComment = opt.String
				}
			}
			indexes = append(indexes, catalog.IndexDef{
				Name:    idxName,
				Columns: idxCols,
				Unique:  isUnique,
				Comment: idxComment,
			})
		}
	}

	// Add UNIQUE KEY from column-level constraints
	for _, col := range columns {
		if col.Unique {
			indexes = append(indexes, catalog.IndexDef{
				Name:    col.Name,
				Columns: []string{col.Name},
				Unique:  true,
			})
		}
	}

	def := &catalog.TableDef{
		Name:       tableName,
		Columns:    columns,
		PrimaryKey: primaryKeys,
		Indexes:    indexes,
	}

	err = db.CreateTable(def)
	if err != nil {
		if stmt.IfNotExists {
			return &Result{}, nil
		}
		return nil, mysqlError(1050, "42S01", fmt.Sprintf("Table '%s' already exists", tableName))
	}
	e.Storage.CreateTable(e.CurrentDB, def)

	// Set AUTO_INCREMENT start value and table comment from table options
	for _, opt := range stmt.TableSpec.Options {
		switch strings.ToUpper(opt.Name) {
		case "AUTO_INCREMENT":
			if val, err := strconv.ParseInt(opt.Value.Val, 10, 64); err == nil {
				if tbl, err := e.Storage.GetTable(e.CurrentDB, tableName); err == nil {
					tbl.AutoIncrement.Store(val - 1) // Store val-1 because next insert increments first
				}
			}
		case "COMMENT":
			def.Comment = opt.Value.Val
		}
	}

	return &Result{}, nil
}

func (e *Executor) execDropTable(stmt *sqlparser.DropTable) (*Result, error) {
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", e.CurrentDB))
	}

	for _, table := range stmt.FromTables {
		tableName := table.Name.String()
		err := db.DropTable(tableName)
		if err != nil {
			if stmt.IfExists {
				continue
			}
			return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", e.CurrentDB, tableName))
		}
		e.Storage.DropTable(e.CurrentDB, tableName)
		// Drop triggers associated with this table (MySQL behavior)
		e.dropTriggersForTable(db, tableName)
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

// execMyliteCommand handles MYLITE-specific control commands:
//   - MYLITE CREATE SNAPSHOT <name>
//   - MYLITE RESTORE SNAPSHOT <name>
//   - MYLITE DROP SNAPSHOT <name>
func (e *Executor) execMyliteCommand(query string) (*Result, error) {
	upper := strings.ToUpper(strings.TrimSpace(query))
	// Strip leading "MYLITE " prefix (7 chars).
	rest := strings.TrimSpace(query[7:])
	restUpper := strings.TrimSpace(upper[7:])

	if strings.HasPrefix(restUpper, "CREATE SNAPSHOT ") {
		name := strings.TrimSpace(rest[len("CREATE SNAPSHOT "):])
		if name == "" {
			return nil, fmt.Errorf("MYLITE CREATE SNAPSHOT: missing snapshot name")
		}
		snap := &fullSnapshot{
			storageSnap: make(map[string]*storage.DatabaseSnapshot),
			catalogSnap: make(map[string]map[string]*catalog.TableDef),
		}
		for dbName, db := range e.Catalog.Databases {
			snap.storageSnap[dbName] = e.Storage.SnapshotDatabase(dbName)
			tablesCopy := make(map[string]*catalog.TableDef, len(db.Tables))
			for tName, tDef := range db.Tables {
				tablesCopy[tName] = tDef
			}
			snap.catalogSnap[dbName] = tablesCopy
		}
		e.snapshots[name] = snap
		return &Result{}, nil
	}

	if strings.HasPrefix(restUpper, "RESTORE SNAPSHOT ") {
		name := strings.TrimSpace(rest[len("RESTORE SNAPSHOT "):])
		if name == "" {
			return nil, fmt.Errorf("MYLITE RESTORE SNAPSHOT: missing snapshot name")
		}
		snap, ok := e.snapshots[name]
		if !ok {
			return nil, fmt.Errorf("MYLITE RESTORE SNAPSHOT: snapshot '%s' not found", name)
		}
		// Remove databases created after snapshot.
		for dbName := range e.Catalog.Databases {
			if _, existed := snap.catalogSnap[dbName]; !existed {
				delete(e.Catalog.Databases, dbName)
				e.Storage.DropDatabase(dbName)
			}
		}
		// Restore each snapshotted database.
		for dbName, tables := range snap.catalogSnap {
			db, ok := e.Catalog.Databases[dbName]
			if !ok {
				e.Catalog.Databases[dbName] = &catalog.Database{
					Name:   dbName,
					Tables: make(map[string]*catalog.TableDef),
				}
				db = e.Catalog.Databases[dbName]
			}
			db.Tables = tables
			e.Storage.RestoreDatabase(dbName, snap.storageSnap[dbName])
		}
		return &Result{}, nil
	}

	if strings.HasPrefix(restUpper, "DROP SNAPSHOT ") {
		name := strings.TrimSpace(rest[len("DROP SNAPSHOT "):])
		if name == "" {
			return nil, fmt.Errorf("MYLITE DROP SNAPSHOT: missing snapshot name")
		}
		if _, ok := e.snapshots[name]; !ok {
			return nil, fmt.Errorf("MYLITE DROP SNAPSHOT: snapshot '%s' not found", name)
		}
		delete(e.snapshots, name)
		return &Result{}, nil
	}

	return nil, fmt.Errorf("unknown MYLITE command: %s", query)
}

func (e *Executor) execInsert(stmt *sqlparser.Insert) (*Result, error) {
	tableName := stmt.Table.TableNameString()

	tbl, err := e.Storage.GetTable(e.CurrentDB, tableName)
	if err != nil {
		return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", e.CurrentDB, tableName))
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

	// Handle INSERT ... SELECT
	if sel, ok := stmt.Rows.(*sqlparser.Select); ok {
		selResult, err := e.execSelect(sel)
		if err != nil {
			return nil, err
		}
		// Convert SELECT result to Values
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
		// If no columns specified in INSERT, use source columns
		if len(colNames) == 0 {
			for _, col := range tbl.Def.Columns {
				colNames = append(colNames, col.Name)
			}
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
	var affected uint64

	for _, valTuple := range rows {
		row := make(storage.Row)
		for i, val := range valTuple {
			if i >= len(colNames) {
				break
			}
			v, err := e.evalExpr(val)
			if err != nil {
				if strings.HasPrefix(err.Error(), "INT_OVERFLOW:") {
					return nil, mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colNames[i]))
				}
				return nil, err
			}
			// Pad BINARY(N), format DECIMAL, validate ENUM/SET.
			for _, col := range tbl.Def.Columns {
				if col.Name == colNames[i] {
					if padLen := binaryPadLength(col.Type); padLen > 0 && v != nil {
						v = padBinaryValue(v, padLen)
					}
					if v != nil {
						v = formatDecimalValue(col.Type, v)
						v = validateEnumSetValue(col.Type, v)
					}
					break
				}
			}
			row[colNames[i]] = v
		}

		// ON DUPLICATE KEY UPDATE: check for existing row with matching PK or UNIQUE key.
		if len(stmt.OnDup) > 0 {
			dupIdx := e.findDuplicateRow(tbl, row, pkCols, uniqueCols)
			if dupIdx >= 0 {
				// Apply the ON DUPLICATE KEY UPDATE expressions to the existing row.
				tbl.Lock()
				for _, upd := range stmt.OnDup {
					colName := upd.Name.Name.String()
					val, err := e.evalExpr(upd.Expr)
					if err != nil {
						tbl.Unlock()
						return nil, err
					}
					// Pad BINARY(N) values.
					for _, col := range tbl.Def.Columns {
						if col.Name == colName {
							if padLen := binaryPadLength(col.Type); padLen > 0 && val != nil {
								val = padBinaryValue(val, padLen)
							}
							break
						}
					}
					tbl.Rows[dupIdx][colName] = val
				}
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
					fullRow[col.Name] = tbl.AutoIncrementValue() + 1
				} else if col.Default != nil {
					fullRow[col.Name] = *col.Default
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
				// NOT NULL check
				if !col.Nullable && !col.AutoIncrement {
					rv, exists := row[col.Name]
					if !exists || rv == nil {
						return nil, mysqlError(1048, "23000", fmt.Sprintf("Column '%s' cannot be null", col.Name))
					}
				}
				rv, exists := row[col.Name]
				if exists && rv != nil {
					colUpper := strings.ToUpper(col.Type)
					isIntType := strings.Contains(colUpper, "INT") || strings.Contains(colUpper, "INTEGER")
					isDecimalType := strings.Contains(colUpper, "DECIMAL") || strings.Contains(colUpper, "FLOAT") || strings.Contains(colUpper, "DOUBLE")
					isNumericType := isIntType || isDecimalType
					isUnsigned := strings.Contains(colUpper, "UNSIGNED")
					if isNumericType {
						switch val := rv.(type) {
						case int64:
							if isUnsigned && val < 0 {
								return nil, mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
							}
						case float64:
							if isUnsigned && val < 0 {
								return nil, mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
							}
						case string:
							if isIntType {
								if _, perr := strconv.ParseInt(val, 10, 64); perr != nil {
									if _, perr := strconv.ParseFloat(val, 64); perr != nil {
										return nil, mysqlError(1366, "HY000", fmt.Sprintf("Incorrect integer value: '%s' for column '%s' at row 1", val, col.Name))
									}
								}
							} else if isDecimalType {
								if _, perr := strconv.ParseFloat(val, 64); perr != nil {
									return nil, mysqlError(1366, "HY000", fmt.Sprintf("Incorrect decimal value: '%s' for column '%s' at row 1", val, col.Name))
								}
							}
						}
						if isDecimalType && strings.Contains(colUpper, "DECIMAL") {
							if derr := checkDecimalRange(col.Type, rv); derr != nil {
								return nil, mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", col.Name))
							}
						}
					}
					// String length check
					isCharType := strings.Contains(colUpper, "CHAR") || strings.Contains(colUpper, "BINARY")
					if isCharType {
						if sv, ok := rv.(string); ok {
							maxLen := extractCharLength(col.Type)
							if maxLen > 0 && len([]rune(sv)) > maxLen {
								return nil, mysqlError(1406, "22001", fmt.Sprintf("Data too long for column '%s' at row 1", col.Name))
							}
						}
					}
					// ENUM/SET validity check in strict mode
					isEnumType := strings.HasPrefix(strings.ToLower(col.Type), "enum(")
					isSetType := strings.HasPrefix(strings.ToLower(col.Type), "set(")
					if isEnumType || isSetType {
						if sv, ok := rv.(string); ok {
							// Check if value was modified by validateEnumSetValue
							origRV := row[col.Name]
							origStr, _ := origRV.(string)
							_ = origStr
							if isEnumType && sv == "" {
								return nil, mysqlError(1265, "01000", fmt.Sprintf("Data truncated for column '%s' at row 1", col.Name))
							}
						}
					}
				}
			}
		}

		id, err := tbl.Insert(row)
		if err != nil {
			// INSERT IGNORE: silently skip duplicate key errors
			if bool(stmt.Ignore) && strings.Contains(err.Error(), "1062") {
				continue
			}
			return nil, err
		}
		lastInsertID = id
		affected++

		// Fire AFTER INSERT triggers
		if err := e.fireTriggers(tableName, "AFTER", "INSERT", fullRow, nil); err != nil {
			return nil, err
		}
	}

	e.lastInsertID = lastInsertID
	// Set lastAutoIncID for sql_auto_is_null (only NOT NULL auto-inc columns)
	if lastInsertID > 0 {
		for _, col := range tbl.Def.Columns {
			if col.AutoIncrement && !col.Nullable {
				e.lastAutoIncID = lastInsertID
				break
			}
		}
	}
	return &Result{
		AffectedRows: affected,
		InsertID:     uint64(lastInsertID),
	}, nil
}

// findDuplicateRow returns the index of an existing row in tbl that has the same
// primary key or unique key value as the candidate row. Returns -1 if no duplicate found.
func (e *Executor) findDuplicateRow(tbl *storage.Table, candidate storage.Row, pkCols, uniqueCols []string) int {
	tbl.Mu.RLock()
	defer tbl.Mu.RUnlock()

	// Also collect multi-column unique indexes
	var multiColUnique [][]string
	for _, idx := range tbl.Def.Indexes {
		if idx.Unique && len(idx.Columns) > 1 {
			multiColUnique = append(multiColUnique, idx.Columns)
		}
	}

	for i, existing := range tbl.Rows {
		// Check primary key match.
		if len(pkCols) > 0 {
			match := true
			for _, col := range pkCols {
				cv, cok := candidate[col]
				ev, eok := existing[col]
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
		// Check unique key match (single-column unique keys).
		for _, col := range uniqueCols {
			cv, cok := candidate[col]
			ev, eok := existing[col]
			if cok && eok && cv != nil && ev != nil &&
				fmt.Sprintf("%v", cv) == fmt.Sprintf("%v", ev) {
				return i
			}
		}
		// Check multi-column unique indexes.
		for _, cols := range multiColUnique {
			match := true
			for _, col := range cols {
				cv, cok := candidate[col]
				ev, eok := existing[col]
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
		// Handle MySQL's virtual DUAL table: one empty row, no columns.
		if strings.ToLower(tableName) == "dual" {
			return []storage.Row{{}}, nil
		}
		// Check CTE map first.
		if e.cteMap != nil {
			if cteTbl, ok := e.cteMap[tableName]; ok {
				result := make([]storage.Row, len(cteTbl.rows))
				for i, row := range cteTbl.rows {
					newRow := make(storage.Row, len(row)*2)
					for k, v := range row {
						newRow[k] = v
						newRow[alias+"."+k] = v
					}
					result[i] = newRow
				}
				return result, nil
			}
		}
		// Handle INFORMATION_SCHEMA virtual tables.
		var qualifier string
		var bareTableName string
		if tn, ok := te.Expr.(sqlparser.TableName); ok {
			qualifier = tn.Qualifier.String()
			bareTableName = tn.Name.String()
		} else {
			bareTableName = tableName
		}
		if e.isInformationSchemaTable(qualifier, bareTableName) {
			isAlias := alias
			if isAlias == tableName {
				// No explicit AS alias; use qualifier-qualified name as prefix.
				if qualifier != "" {
					isAlias = qualifier + "." + bareTableName
				} else {
					isAlias = bareTableName
				}
			}
			return e.buildInformationSchemaRows(bareTableName, isAlias)
		}
		tbl, err := e.Storage.GetTable(e.CurrentDB, tableName)
		if err != nil {
			return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", e.CurrentDB, tableName))
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

	rightRows, err := e.buildFromExpr(join.RightExpr)
	if err != nil {
		return nil, err
	}

	// Determine right alias and table def for NULL padding
	rightAlias, _, _ := extractTableAlias(join.RightExpr)
	leftAlias, _, _ := extractTableAlias(join.LeftExpr)

	// Get right table columns for NULL padding (LEFT JOIN unmatched)
	var rightColNames []string
	if ate, ok := join.RightExpr.(*sqlparser.AliasedTableExpr); ok {
		_, tName, _ := extractTableAliasFromAliased(ate)
		if rtbl, err := e.Storage.GetTable(e.CurrentDB, tName); err == nil {
			for _, col := range rtbl.Def.Columns {
				rightColNames = append(rightColNames, col.Name)
			}
		}
	}
	// If we couldn't get columns from storage, derive from rows
	if len(rightColNames) == 0 && len(rightRows) > 0 {
		seen := make(map[string]bool)
		for k := range rightRows[0] {
			if !strings.Contains(k, ".") && !seen[k] {
				seen[k] = true
				rightColNames = append(rightColNames, k)
			}
		}
	}

	var leftColNames []string
	if ate, ok := join.LeftExpr.(*sqlparser.AliasedTableExpr); ok {
		_, tName, _ := extractTableAliasFromAliased(ate)
		if ltbl, err := e.Storage.GetTable(e.CurrentDB, tName); err == nil {
			for _, col := range ltbl.Def.Columns {
				leftColNames = append(leftColNames, col.Name)
			}
		}
	}
	if len(leftColNames) == 0 && len(leftRows) > 0 {
		seen := make(map[string]bool)
		for k := range leftRows[0] {
			if !strings.Contains(k, ".") && !seen[k] {
				seen[k] = true
				leftColNames = append(leftColNames, k)
			}
		}
	}

	joinType := join.Join

	// Handle RIGHT JOIN by swapping left and right and treating as LEFT JOIN
	if joinType == sqlparser.RightJoinType || joinType == sqlparser.NaturalRightJoinType {
		leftRows, rightRows = rightRows, leftRows
		leftAlias, rightAlias = rightAlias, leftAlias
		leftColNames, rightColNames = rightColNames, leftColNames
		if joinType == sqlparser.RightJoinType {
			joinType = sqlparser.LeftJoinType
		} else {
			joinType = sqlparser.NaturalLeftJoinType
		}
	}

	// Build ON condition for NATURAL joins (auto-join on common column names)
	isNatural := joinType == sqlparser.NaturalJoinType || joinType == sqlparser.NaturalLeftJoinType
	var naturalCols []string
	if isNatural {
		rightSet := make(map[string]bool)
		for _, c := range rightColNames {
			rightSet[strings.ToLower(c)] = true
		}
		for _, c := range leftColNames {
			if rightSet[strings.ToLower(c)] {
				naturalCols = append(naturalCols, c)
			}
		}
	}

	isLeft := joinType == sqlparser.LeftJoinType || joinType == sqlparser.NaturalLeftJoinType
	isCross := joinType == sqlparser.NormalJoinType

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
				if rightAlias != "" {
					combined[rightAlias+"."+k] = v
				}
			}

			// CROSS JOIN: no condition, all combinations
			if isCross {
				result = append(result, combined)
				matched = true
				continue
			}

			// NATURAL JOIN: match on common columns
			if isNatural {
				if len(naturalCols) == 0 {
					// No common columns = cross join
					result = append(result, combined)
					matched = true
					continue
				}
				allMatch := true
				for _, col := range naturalCols {
					lv := leftRow[col]
					rv := rightRow[col]
					if lv == nil || rv == nil || fmt.Sprintf("%v", lv) != fmt.Sprintf("%v", rv) {
						allMatch = false
						break
					}
				}
				if allMatch {
					result = append(result, combined)
					matched = true
				}
				continue
			}

			// Evaluate ON condition
			if join.Condition != nil && join.Condition.On != nil {
				match, err := e.evalWhere(join.Condition.On, combined)
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
			for _, col := range rightColNames {
				combined[col] = nil
				if rightAlias != "" {
					combined[rightAlias+"."+col] = nil
				}
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

	// Process WITH clause (Common Table Expressions) if present.
	if stmt.With != nil && len(stmt.With.CTEs) > 0 {
		// Save any outer CTE map and restore on exit.
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
			// Execute the CTE subquery.
			subSel, ok := cte.Subquery.(*sqlparser.Select)
			if !ok {
				return nil, fmt.Errorf("CTE '%s': only SELECT subqueries are supported", cteName)
			}
			subResult, err := e.execSelect(subSel)
			if err != nil {
				return nil, fmt.Errorf("CTE '%s': %w", cteName, err)
			}
			// Convert result rows into storage.Row maps.
			cteRows := make([]storage.Row, len(subResult.Rows))
			for i, row := range subResult.Rows {
				r := make(storage.Row, len(subResult.Columns))
				for j, col := range subResult.Columns {
					r[col] = row[j]
				}
				cteRows[i] = r
			}
			newCTEMap[cteName] = &cteTable{
				columns: subResult.Columns,
				rows:    cteRows,
			}
		}
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
			match, err := e.evalWhere(stmt.Where.Expr, row)
			if err != nil {
				return nil, err
			}
			if match {
				filtered = append(filtered, row)
			}
		}
		allRows = filtered
		// Clear sql_auto_is_null after WHERE evaluation
		if e.sqlAutoIsNull && e.lastAutoIncID > 0 {
			e.lastAutoIncID = 0
		}
	}

	// Check if we have GROUP BY or aggregate functions
	hasGroupBy := stmt.GroupBy != nil && len(stmt.GroupBy.Exprs) > 0
	hasAggregates := selectExprsHaveAggregates(stmt.SelectExprs.Exprs)

	if hasGroupBy || hasAggregates {
		return e.execSelectGroupBy(stmt, allRows)
	}

	// Build result columns and rows (non-aggregate path)
	// Collect table definitions for proper column ordering in SELECT *
	var selectTableDefs []*catalog.TableDef
	if len(stmt.From) > 0 {
		if ate, ok := stmt.From[0].(*sqlparser.AliasedTableExpr); ok {
			if tn, ok := ate.Expr.(sqlparser.TableName); ok {
				tblName := tn.Name.String()
				if db, err := e.Catalog.GetDatabase(e.CurrentDB); err == nil {
					if td, err := db.GetTable(tblName); err == nil {
						selectTableDefs = append(selectTableDefs, td)
					}
				}
			}
		}
	}
	colNames, colExprs, err := e.resolveSelectExprs(stmt.SelectExprs.Exprs, allRows, selectTableDefs...)
	if err != nil {
		return nil, err
	}

	resultRows := make([][]interface{}, 0, len(allRows))
	for _, row := range allRows {
		resultRow := make([]interface{}, len(colExprs))
		for i, expr := range colExprs {
			val, err := e.evalRowExpr(expr, row)
			if err != nil {
				return nil, err
			}
			resultRow[i] = val
		}
		resultRows = append(resultRows, resultRow)
	}

	// Apply SELECT DISTINCT
	if stmt.Distinct {
		seen := make(map[string]bool)
		unique := make([][]interface{}, 0)
		for _, row := range resultRows {
			key := fmt.Sprintf("%v", row)
			if !seen[key] {
				seen[key] = true
				unique = append(unique, row)
			}
		}
		resultRows = unique
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

// aggregateDisplayName returns the MySQL-style display name for aggregate expressions.
// MySQL returns "COUNT(c1)", "SUM(c1)", etc. (uppercase function name).
func aggregateDisplayName(expr sqlparser.Expr) string {
	s := sqlparser.String(expr)
	// Replace lowercase function names with uppercase
	for _, fn := range []string{"count", "sum", "avg", "min", "max"} {
		if strings.HasPrefix(s, fn+"(") {
			return strings.ToUpper(fn) + s[len(fn):]
		}
	}
	return s
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
				// MySQL returns aggregate function names in uppercase
				colNames = append(colNames, aggregateDisplayName(se.Expr))
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
		for gi, row := range resultRows {
			havingRow := make(storage.Row)
			for i, col := range colNames {
				havingRow[col] = row[i]
			}
			// Also evaluate aggregates from the HAVING clause against the group rows
			var groupRows []storage.Row
			if gi < len(groups) {
				groupRows = groups[gi].rows
			}
			// Evaluate HAVING with aggregate support
			match, err := e.evalHaving(stmt.Having.Expr, havingRow, groupRows)
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
		// MySQL AVG() returns DECIMAL with 4 decimal places by default.
		return fmt.Sprintf("%.4f", sum/float64(count)), nil
	}
	// Non-aggregate: return value from representative row
	return evalRowExpr(expr, repRow)
}

// resolveSelectExprs returns column names and original expressions for non-aggregate SELECTs.
// It handles star expansion using actual row data (needed for JOINs).
// tableDefs is optional; when provided, * expansion uses schema-defined column order.
func (e *Executor) resolveSelectExprs(exprs []sqlparser.SelectExpr, rows []storage.Row, tableDefs ...*catalog.TableDef) ([]string, []sqlparser.Expr, error) {
	cols := make([]string, 0)
	colExprs := make([]sqlparser.Expr, 0)

	for _, expr := range exprs {
		switch se := expr.(type) {
		case *sqlparser.StarExpr:
			// Expand star using table definition column order if available
			if len(tableDefs) > 0 {
				for _, td := range tableDefs {
					for _, col := range td.Columns {
						cols = append(cols, col.Name)
						colExprs = append(colExprs, &sqlparser.ColName{Name: sqlparser.NewIdentifierCI(col.Name)})
					}
				}
			} else if len(rows) > 0 {
				// Check if this is an information_schema table by matching row keys
				// against known column orders.
				usedOrder := false
				for _, order := range infoSchemaColumnOrder {
					if len(order) > 0 {
						if _, ok := rows[0][order[0]]; ok {
							// Use predefined column order for information_schema
							for _, colName := range order {
								if _, exists := rows[0][colName]; exists {
									cols = append(cols, colName)
									colExprs = append(colExprs, &sqlparser.ColName{Name: sqlparser.NewIdentifierCI(colName)})
								}
							}
							usedOrder = true
							break
						}
					}
				}
				if !usedOrder {
					// Fallback: use row keys (may have non-deterministic order)
					seen := make(map[string]bool)
					for k := range rows[0] {
						if !strings.Contains(k, ".") && !seen[k] {
							seen[k] = true
							cols = append(cols, k)
							colExprs = append(colExprs, &sqlparser.ColName{Name: sqlparser.NewIdentifierCI(k)})
						}
					}
				}
			}
		case *sqlparser.AliasedExpr:
			name := ""
			if !se.As.IsEmpty() {
				name = se.As.String()
			} else if colName, ok := se.Expr.(*sqlparser.ColName); ok {
				name = colName.Name.String()
				// Case-insensitive column name resolution: if the row has a
				// key that matches case-insensitively, use that key's case
				// (needed for information_schema columns which are UPPERCASE).
				if len(rows) > 0 {
					upperName := strings.ToUpper(name)
					for k := range rows[0] {
						if strings.ToUpper(k) == upperName && !strings.Contains(k, ".") {
							name = k
							break
						}
					}
				}
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

func (e *Executor) execUnion(stmt *sqlparser.Union) (*Result, error) {
	// Execute left side
	leftResult, err := e.Execute(sqlparser.String(stmt.Left))
	if err != nil {
		return nil, err
	}

	// Execute right side
	rightResult, err := e.Execute(sqlparser.String(stmt.Right))
	if err != nil {
		return nil, err
	}

	// Combine rows
	allRows := make([][]interface{}, 0, len(leftResult.Rows)+len(rightResult.Rows))
	allRows = append(allRows, leftResult.Rows...)
	allRows = append(allRows, rightResult.Rows...)

	// UNION (not UNION ALL) removes duplicates
	if !stmt.Distinct {
		// UNION ALL - keep all rows
	} else {
		// UNION - remove duplicates
		seen := make(map[string]bool)
		unique := make([][]interface{}, 0)
		for _, row := range allRows {
			key := fmt.Sprintf("%v", row)
			if !seen[key] {
				seen[key] = true
				unique = append(unique, row)
			}
		}
		allRows = unique
	}

	// Apply ORDER BY if present
	if stmt.OrderBy != nil {
		allRows, err = applyOrderBy(stmt.OrderBy, leftResult.Columns, allRows)
		if err != nil {
			return nil, err
		}
	}

	// Apply LIMIT
	if stmt.Limit != nil {
		allRows, err = applyLimit(stmt.Limit, allRows)
		if err != nil {
			return nil, err
		}
	}

	return &Result{
		Columns:     leftResult.Columns,
		Rows:        allRows,
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

			v, err := e.evalExpr(se.Expr)
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
		return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", e.CurrentDB, tableName))
	}

	tbl.Lock()
	defer tbl.Unlock()

	var affected uint64
	for i, row := range tbl.Rows {
		match := true
		if stmt.Where != nil {
			m, err := e.evalWhere(stmt.Where.Expr, row)
			if err != nil {
				return nil, err
			}
			match = m
		}
		if !match {
			continue
		}

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
			val, err := e.evalRowExpr(upd.Expr, row)
			if err != nil {
				return nil, err
			}
			for _, col := range tbl.Def.Columns {
				if col.Name == colName {
					if padLen := binaryPadLength(col.Type); padLen > 0 && val != nil {
						val = padBinaryValue(val, padLen)
					}
					break
				}
			}
			newRow[colName] = val
		}

		// Fire BEFORE UPDATE triggers (unlock table to avoid deadlock since trigger may access other tables)
		// Trigger may modify newRow via SET NEW.col = val
		tbl.Unlock()
		if err := e.fireTriggers(tableName, "BEFORE", "UPDATE", newRow, oldRow); err != nil {
			tbl.Lock()
			return nil, err
		}
		tbl.Lock()

		// Apply the trigger-modified newRow values to the actual row
		for _, col := range tbl.Def.Columns {
			if val, ok := newRow[col.Name]; ok {
				if padLen := binaryPadLength(col.Type); padLen > 0 && val != nil {
					val = padBinaryValue(val, padLen)
				}
				tbl.Rows[i][col.Name] = val
			}
		}
		affected++

		// Fire AFTER UPDATE triggers
		tbl.Unlock()
		if err := e.fireTriggers(tableName, "AFTER", "UPDATE", newRow, oldRow); err != nil {
			tbl.Lock()
			return nil, err
		}
		tbl.Lock()
	}

	return &Result{AffectedRows: affected}, nil
}

func (e *Executor) execDelete(stmt *sqlparser.Delete) (*Result, error) {
	if len(stmt.TableExprs) == 0 {
		return nil, fmt.Errorf("no table specified")
	}

	// Multi-table DELETE: when Targets is populated
	if len(stmt.Targets) > 0 {
		return e.execMultiTableDeleteAST(stmt)
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
		return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", e.CurrentDB, tableName))
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
					return nil, wErr
				}
				match = m
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
			flatRows, err = applyOrderBy(stmt.OrderBy, colNames, flatRows)
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

		newRows := make([]storage.Row, 0, len(tbl.Rows)-len(deleteSet))
		for i, row := range tbl.Rows {
			if !deleteSet[i] {
				newRows = append(newRows, row)
			}
		}
		tbl.Rows = newRows
		return &Result{AffectedRows: uint64(len(deleteSet))}, nil
	}

	newRows := make([]storage.Row, 0)
	var affected uint64
	for _, row := range tbl.Rows {
		match := true
		if stmt.Where != nil {
			m, err := e.evalWhere(stmt.Where.Expr, row)
			if err != nil {
				return nil, err
			}
			match = m
		}
		if match {
			// Fire BEFORE DELETE triggers
			tbl.Unlock()
			if err := e.fireTriggers(tableName, "BEFORE", "DELETE", nil, row); err != nil {
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

	return &Result{AffectedRows: affected}, nil
}

// columnDefFromAST converts a vitess ColumnDefinition into our catalog.ColumnDef.
func columnDefFromAST(col *sqlparser.ColumnDefinition) catalog.ColumnDef {
	colDef := catalog.ColumnDef{
		Name:     col.Name.String(),
		Type:     buildColumnTypeString(col.Type),
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
		if col.Type.Options.Comment != nil {
			colDef.Comment = col.Type.Options.Comment.Val
		}
	}
	return colDef
}

func (e *Executor) execAlterTable(stmt *sqlparser.AlterTable) (*Result, error) {
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", e.CurrentDB))
	}

	tableName := stmt.Table.Name.String()

	// Ensure the storage table exists.
	tbl, err := e.Storage.GetTable(e.CurrentDB, tableName)
	if err != nil {
		return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", e.CurrentDB, tableName))
	}

	for _, opt := range stmt.AlterOptions {
		switch op := opt.(type) {

		case *sqlparser.AddColumns:
			for _, col := range op.Columns {
				colDef := columnDefFromAST(col)
				position := ""
				afterCol := ""
				if op.First {
					position = "FIRST"
				} else if op.After != nil {
					position = "AFTER"
					afterCol = op.After.Name.String()
				}
				if addErr := db.AddColumnAt(tableName, colDef, position, afterCol); addErr != nil {
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
			// Check if this would leave the table with no columns
			tableDef, _ := db.GetTable(tableName)
			if tableDef != nil && len(tableDef.Columns) <= 1 {
				return nil, mysqlError(1090, "42000", "You can't delete all columns with ALTER TABLE; use DROP TABLE instead")
			}
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
			// Store index definition so SHOW CREATE TABLE can display it.
			var idxCols []string
			for _, idxCol := range op.IndexDefinition.Columns {
				colStr := idxCol.Column.String()
				if idxCol.Length != nil {
					colStr += fmt.Sprintf("(%d)", *idxCol.Length)
				}
				idxCols = append(idxCols, colStr)
			}
			isUnique := op.IndexDefinition.Info.Type == sqlparser.IndexTypeUnique
			isPrimary := op.IndexDefinition.Info.Type == sqlparser.IndexTypePrimary
			idxName := op.IndexDefinition.Info.Name.String()
			if idxName == "" && len(idxCols) > 0 {
				idxName = idxCols[0]
			}
			// Check for USING method
			usingMethod := ""
			for _, opt := range op.IndexDefinition.Options {
				if opt.Name == "USING" {
					// InnoDB does not support HASH; silently ignore it.
					if strings.ToUpper(opt.String) != "HASH" {
						usingMethod = opt.String
					}
				}
			}
			if isPrimary {
				db.SetPrimaryKey(tableName, idxCols)
			} else {
				db.AddIndex(tableName, catalog.IndexDef{
					Name:    idxName,
					Columns: idxCols,
					Unique:  isUnique,
					Using:   usingMethod,
				})
			}

		case *sqlparser.AddConstraintDefinition:
			// Silently accept constraint additions.

		case *sqlparser.DropKey:
			if op.Type == sqlparser.PrimaryKeyType {
				db.DropPrimaryKey(tableName)
			} else {
				idxName := op.Name.String()
				if err := db.DropIndex(tableName, idxName); err != nil {
					return nil, mysqlError(1091, "42000", fmt.Sprintf("Can't DROP '%s'; check that column/key exists", idxName))
				}
			}

		case sqlparser.TableOptions:
			for _, to := range op {
				if strings.ToUpper(to.Name) == "AUTO_INCREMENT" {
					if val, err := strconv.ParseInt(to.Value.Val, 10, 64); err == nil {
						tbl.AutoIncrement.Store(val - 1)
					}
				}
			}

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
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", e.CurrentDB))
	}
	tblDef, err := db.GetTable(tableName)
	if err != nil {
		return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", e.CurrentDB, tableName))
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
		rows = append(rows, []interface{}{col.Name, mysqlDisplayType(col.Type), nullable, key, defVal, extra})
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
		likePattern := ""
		if basic.Filter != nil {
			likePattern = basic.Filter.Like
		}
		switch basic.Command {
		case sqlparser.Column:
			// SHOW COLUMNS FROM <table> / SHOW FULL COLUMNS FROM <table>
			return e.describeTable(basic.Tbl.Name.String())
		case sqlparser.TableStatus:
			// SHOW TABLE STATUS [FROM db] [LIKE ...]
			return e.showTableStatus()
		case sqlparser.Database: // SHOW DATABASES / SHOW SCHEMAS
			dbs := e.Catalog.ListDatabases()
			sort.Strings(dbs)
			rows := make([][]interface{}, 0, len(dbs))
			for _, d := range dbs {
				if likePattern != "" && !matchLike(d, likePattern) {
					continue
				}
				rows = append(rows, []interface{}{d})
			}
			colName := "Database"
			if likePattern != "" {
				colName = fmt.Sprintf("Database (%s)", likePattern)
			}
			return &Result{Columns: []string{colName}, Rows: rows, IsResultSet: true}, nil
		case sqlparser.Charset: // SHOW CHARACTER SET
			charsets := [][]interface{}{
				{"armscii8", "ARMSCII-8 Armenian", "armscii8_general_ci", int64(1)},
				{"ascii", "US ASCII", "ascii_general_ci", int64(1)},
				{"binary", "Binary pseudo charset", "binary", int64(1)},
				{"latin1", "cp1252 West European", "latin1_swedish_ci", int64(1)},
				{"utf8", "UTF-8 Unicode", "utf8_general_ci", int64(3)},
				{"utf8mb3", "UTF-8 Unicode", "utf8mb3_general_ci", int64(3)},
				{"utf8mb4", "UTF-8 Unicode", "utf8mb4_0900_ai_ci", int64(4)},
			}
			rows := make([][]interface{}, 0)
			for _, cs := range charsets {
				if likePattern == "" || matchLike(cs[0].(string), likePattern) {
					rows = append(rows, cs)
				}
			}
			return &Result{Columns: []string{"Charset", "Description", "Default collation", "Maxlen"}, Rows: rows, IsResultSet: true}, nil
		case sqlparser.Collation: // SHOW COLLATION
			collations := [][]interface{}{
				{"utf8mb4_0900_ai_ci", "utf8mb4", int64(255), "Yes", "Yes", int64(0), "NO PAD"},
				{"utf8mb4_general_ci", "utf8mb4", int64(45), "", "Yes", int64(1), "PAD SPACE"},
				{"utf8_general_ci", "utf8", int64(33), "Yes", "Yes", int64(1), "PAD SPACE"},
				{"latin1_swedish_ci", "latin1", int64(8), "Yes", "Yes", int64(1), "PAD SPACE"},
				{"ascii_general_ci", "ascii", int64(11), "Yes", "Yes", int64(1), "PAD SPACE"},
				{"binary", "binary", int64(63), "Yes", "Yes", int64(1), "NO PAD"},
			}
			rows := make([][]interface{}, 0)
			for _, c := range collations {
				if likePattern == "" || matchLike(c[0].(string), likePattern) {
					rows = append(rows, c)
				}
			}
			return &Result{Columns: []string{"Collation", "Charset", "Id", "Default", "Compiled", "Sortlen", "Pad_attribute"}, Rows: rows, IsResultSet: true}, nil
		case sqlparser.Table: // SHOW TABLES
			db, err := e.Catalog.GetDatabase(e.CurrentDB)
			if err != nil {
				return nil, err
			}
			tables := db.ListTables()
			sort.Strings(tables)
			rows := make([][]interface{}, 0, len(tables))
			for _, t := range tables {
				if likePattern != "" && !matchLike(t, likePattern) {
					continue
				}
				rows = append(rows, []interface{}{t})
			}
			return &Result{
				Columns:     []string{fmt.Sprintf("Tables_in_%s", e.CurrentDB)},
				Rows:        rows,
				IsResultSet: true,
			}, nil
		}
	}

	upper := strings.ToUpper(strings.TrimSpace(query))

	if strings.HasPrefix(upper, "SHOW TABLES") {
		db, err := e.Catalog.GetDatabase(e.CurrentDB)
		if err != nil {
			return nil, err
		}
		tables := db.ListTables()
		sort.Strings(tables)
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

	if strings.HasPrefix(upper, "SHOW DATABASES") || strings.HasPrefix(upper, "SHOW SCHEMAS") {
		dbs := e.Catalog.ListDatabases()
		sort.Strings(dbs)
		// Handle LIKE pattern
		likePattern := ""
		if idx := strings.Index(upper, "LIKE "); idx >= 0 {
			rest := strings.TrimSpace(query[idx+5:])
			likePattern = strings.Trim(rest, "'\"")
		}
		rows := make([][]interface{}, 0, len(dbs))
		for _, d := range dbs {
			if likePattern != "" && !matchLike(d, likePattern) {
				continue
			}
			rows = append(rows, []interface{}{d})
		}
		colName := "Database"
		if likePattern != "" {
			colName = fmt.Sprintf("Database (%s)", likePattern)
		}
		return &Result{
			Columns:     []string{colName},
			Rows:        rows,
			IsResultSet: true,
		}, nil
	}

	// SHOW CHARACTER SET
	if strings.HasPrefix(upper, "SHOW CHARACTER SET") || strings.HasPrefix(upper, "SHOW CHARSET") {
		likePattern := ""
		if idx := strings.Index(upper, "LIKE "); idx >= 0 {
			rest := strings.TrimSpace(query[idx+5:])
			likePattern = strings.Trim(rest, "'\"")
		}
		charsets := [][]interface{}{
			{"armscii8", "ARMSCII-8 Armenian", "armscii8_general_ci", int64(1)},
			{"ascii", "US ASCII", "ascii_general_ci", int64(1)},
			{"binary", "Binary pseudo charset", "binary", int64(1)},
			{"latin1", "cp1252 West European", "latin1_swedish_ci", int64(1)},
			{"utf8", "UTF-8 Unicode", "utf8_general_ci", int64(3)},
			{"utf8mb3", "UTF-8 Unicode", "utf8mb3_general_ci", int64(3)},
			{"utf8mb4", "UTF-8 Unicode", "utf8mb4_0900_ai_ci", int64(4)},
		}
		rows := make([][]interface{}, 0)
		for _, cs := range charsets {
			if likePattern == "" || matchLike(cs[0].(string), likePattern) {
				rows = append(rows, cs)
			}
		}
		return &Result{
			Columns:     []string{"Charset", "Description", "Default collation", "Maxlen"},
			Rows:        rows,
			IsResultSet: true,
		}, nil
	}

	// SHOW COLLATION
	if strings.HasPrefix(upper, "SHOW COLLATION") {
		likePattern := ""
		if idx := strings.Index(upper, "LIKE "); idx >= 0 {
			rest := strings.TrimSpace(query[idx+5:])
			likePattern = strings.Trim(rest, "'\"")
		}
		collations := [][]interface{}{
			{"utf8mb4_0900_ai_ci", "utf8mb4", int64(255), "Yes", "Yes", int64(0), "NO PAD"},
			{"utf8mb4_general_ci", "utf8mb4", int64(45), "", "Yes", int64(1), "PAD SPACE"},
			{"utf8_general_ci", "utf8", int64(33), "Yes", "Yes", int64(1), "PAD SPACE"},
			{"latin1_swedish_ci", "latin1", int64(8), "Yes", "Yes", int64(1), "PAD SPACE"},
			{"ascii_general_ci", "ascii", int64(11), "Yes", "Yes", int64(1), "PAD SPACE"},
			{"binary", "binary", int64(63), "Yes", "Yes", int64(1), "NO PAD"},
		}
		rows := make([][]interface{}, 0)
		for _, c := range collations {
			if likePattern == "" || matchLike(c[0].(string), likePattern) {
				rows = append(rows, c)
			}
		}
		return &Result{
			Columns:     []string{"Collation", "Charset", "Id", "Default", "Compiled", "Sortlen", "Pad_attribute"},
			Rows:        rows,
			IsResultSet: true,
		}, nil
	}

	// SHOW CREATE TABLE <table>
	if strings.HasPrefix(upper, "SHOW CREATE TABLE") {
		parts := strings.Fields(query)
		if len(parts) >= 4 {
			tableName := strings.Trim(parts[3], "`")
			return e.showCreateTable(tableName)
		}
	}

	// Accept other SHOW statements silently
	return &Result{
		Columns:     []string{"Value"},
		Rows:        [][]interface{}{},
		IsResultSet: true,
	}, nil
}

// buildColumnTypeString builds a type string from a sqlparser.ColumnType,
// including length, scale, unsigned, zerofill, and enum values,
// but excluding options like NOT NULL, AUTO_INCREMENT, etc.
// binaryPadLength checks if a column type is BINARY(N) (not VARBINARY) and
// returns the fixed width N. Returns 0 if the column is not a fixed-width binary type.
func binaryPadLength(colType string) int {
	lower := strings.ToLower(colType)
	// Must match "binary(N)" but not "varbinary(N)"
	if strings.HasPrefix(lower, "binary(") && !strings.HasPrefix(lower, "varbinary") {
		var n int
		if _, err := fmt.Sscanf(lower, "binary(%d)", &n); err == nil {
			return n
		}
	}
	return 0
}

// padBinaryValue pads a string value to the given length with null bytes.
func padBinaryValue(val interface{}, padLen int) interface{} {
	if val == nil || padLen <= 0 {
		return val
	}
	s, ok := val.(string)
	if !ok {
		return val
	}
	if len(s) < padLen {
		s = s + strings.Repeat("\x00", padLen-len(s))
	} else if len(s) > padLen {
		s = s[:padLen]
	}
	return s
}

func buildColumnTypeString(ct *sqlparser.ColumnType) string {
	s := strings.ToLower(ct.Type)
	if ct.Length != nil && ct.Scale != nil {
		s += fmt.Sprintf("(%d,%d)", *ct.Length, *ct.Scale)
	} else if ct.Length != nil {
		s += fmt.Sprintf("(%d)", *ct.Length)
	}
	if len(ct.EnumValues) > 0 {
		vals := make([]string, len(ct.EnumValues))
		for i, v := range ct.EnumValues {
			// Vitess parser stores enum values with surrounding quotes;
			// strip them before re-quoting to avoid double-quoting.
			v = strings.Trim(v, "'")
			vals[i] = fmt.Sprintf("'%s'", v)
		}
		s += "(" + strings.Join(vals, ",") + ")"
	}
	if ct.Unsigned {
		s += " unsigned"
	}
	if ct.Zerofill {
		s += " zerofill"
	}
	return s
}

// mysqlDisplayType returns the MySQL display type with width for SHOW CREATE TABLE.
func mysqlDisplayType(colType string) string {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	// Extract base type and any existing parameters
	base := upper
	suffix := ""
	if idx := strings.Index(upper, "("); idx >= 0 {
		// Already has width specified, just lowercase it
		// But also normalize REAL to DOUBLE, NUMERIC to DECIMAL, INTEGER to INT
		result := strings.ToLower(colType)
		if strings.HasPrefix(result, "real") {
			result = "double" + result[4:]
		}
		if strings.HasPrefix(result, "numeric") {
			result = "decimal" + result[7:]
		}
		if strings.HasPrefix(result, "integer") {
			result = "int" + result[7:]
		}
		return result
	}
	// Check for ZEROFILL suffix (must check before UNSIGNED since ZEROFILL implies UNSIGNED)
	if strings.HasSuffix(base, " ZEROFILL") {
		base = strings.TrimSuffix(base, " ZEROFILL")
		suffix = " zerofill"
	}
	// Check for UNSIGNED suffix
	if strings.HasSuffix(base, " UNSIGNED") {
		base = strings.TrimSuffix(base, " UNSIGNED")
		suffix = " unsigned" + suffix
	} else if strings.Contains(suffix, "zerofill") {
		// ZEROFILL implies UNSIGNED in MySQL
		suffix = " unsigned" + suffix
	}

	// Add default display widths (differ for signed vs unsigned in MySQL)
	isUnsigned := suffix != ""
	switch base {
	case "TINYINT":
		if isUnsigned {
			return "tinyint(3)" + suffix
		}
		return "tinyint(4)" + suffix
	case "SMALLINT":
		if isUnsigned {
			return "smallint(5)" + suffix
		}
		return "smallint(6)" + suffix
	case "MEDIUMINT":
		if isUnsigned {
			return "mediumint(8)" + suffix
		}
		return "mediumint(9)" + suffix
	case "INT", "INTEGER":
		if isUnsigned {
			return "int(10)" + suffix
		}
		return "int(11)" + suffix
	case "BIGINT":
		return "bigint(20)" + suffix
	case "FLOAT":
		return "float" + suffix
	case "DOUBLE", "REAL":
		return "double" + suffix
	case "DECIMAL", "NUMERIC":
		return "decimal(10,0)" + suffix
	case "CHAR":
		return "char(1)"
	case "BINARY":
		return "binary(1)"
	case "BIT":
		return "bit(1)"
	case "YEAR":
		return "year(4)"
	case "BOOL", "BOOLEAN":
		return "tinyint(1)"
	default:
		return strings.ToLower(colType)
	}
}

func (e *Executor) showCreateTable(tableName string) (*Result, error) {
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return nil, err
	}
	def, err := db.GetTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("ERROR 1146 (42S02): Table '%s.%s' doesn't exist", e.CurrentDB, tableName)
	}

	// Get AUTO_INCREMENT value
	autoIncVal := int64(0)
	if tbl, err := e.Storage.GetTable(e.CurrentDB, tableName); err == nil {
		autoIncVal = tbl.AutoIncrementValue()
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf("CREATE TABLE `%s` (\n", tableName))

	var colDefs []string
	var pkCols []string
	for _, col := range def.Columns {
		var parts []string
		parts = append(parts, fmt.Sprintf("  `%s`", col.Name))
		parts = append(parts, mysqlDisplayType(col.Type))
		colTypeLower := strings.ToLower(col.Type)
		isTimestamp := strings.HasPrefix(colTypeLower, "timestamp")
		if !col.Nullable {
			parts = append(parts, "NOT NULL")
		} else if isTimestamp {
			// MySQL explicitly shows NULL for nullable timestamp columns
			parts = append(parts, "NULL")
		}
		if col.AutoIncrement {
			parts = append(parts, "AUTO_INCREMENT")
		} else if col.Default != nil {
			defVal := *col.Default
			// MySQL SHOW CREATE TABLE quotes default values
			if defVal == "NULL" || defVal == "null" {
				parts = append(parts, "DEFAULT NULL")
			} else if strings.HasPrefix(defVal, "'") {
				// Already quoted - pad BINARY default values.
				if padLen := binaryPadLength(col.Type); padLen > 0 {
					inner := defVal[1 : len(defVal)-1] // strip quotes
					if len(inner) < padLen {
						inner = inner + strings.Repeat("\\0", padLen-len(inner))
					}
					defVal = "'" + inner + "'"
				}
				parts = append(parts, fmt.Sprintf("DEFAULT %s", defVal))
			} else {
				// Pad BINARY default values.
				if padLen := binaryPadLength(col.Type); padLen > 0 && len(defVal) < padLen {
					defVal = defVal + strings.Repeat("\\0", padLen-len(defVal))
				}
				parts = append(parts, fmt.Sprintf("DEFAULT '%s'", defVal))
			}
		} else if col.Nullable {
			// MySQL doesn't show DEFAULT NULL for BLOB/TEXT types
			isBlobOrText := strings.Contains(colTypeLower, "blob") || strings.Contains(colTypeLower, "text")
			if !isBlobOrText {
				parts = append(parts, "DEFAULT NULL")
			}
		}
		if col.Comment != "" {
			parts = append(parts, fmt.Sprintf("COMMENT '%s'", col.Comment))
		}
		colDefs = append(colDefs, strings.Join(parts, " "))
		if col.PrimaryKey {
			pkCols = append(pkCols, col.Name)
		}
	}
	if len(pkCols) == 0 {
		pkCols = def.PrimaryKey
	}

	hasTrailingDefs := len(pkCols) > 0 || len(def.Indexes) > 0

	for i, cd := range colDefs {
		b.WriteString(cd)
		if i < len(colDefs)-1 || hasTrailingDefs {
			b.WriteString(",")
		}
		b.WriteString("\n")
	}
	if len(pkCols) > 0 {
		quotedPK := make([]string, len(pkCols))
		for i, pk := range pkCols {
			quotedPK[i] = fmt.Sprintf("`%s`", pk)
		}
		hasMore := len(def.Indexes) > 0
		b.WriteString(fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(quotedPK, ",")))
		if hasMore {
			b.WriteString(",")
		}
		b.WriteString("\n")
	}
	for i, idx := range def.Indexes {
		quotedCols := make([]string, len(idx.Columns))
		for j, c := range idx.Columns {
			// Handle column with length prefix like "c1(10)"
			if lparen := strings.Index(c, "("); lparen >= 0 {
				quotedCols[j] = fmt.Sprintf("`%s`%s", c[:lparen], c[lparen:])
			} else {
				quotedCols[j] = fmt.Sprintf("`%s`", c)
			}
		}
		usingStr := ""
		if idx.Using != "" {
			usingStr = " USING " + idx.Using
		}
		if idx.Unique {
			b.WriteString(fmt.Sprintf("  UNIQUE KEY `%s` (%s)%s", idx.Name, strings.Join(quotedCols, ","), usingStr))
		} else {
			b.WriteString(fmt.Sprintf("  KEY `%s` (%s)%s", idx.Name, strings.Join(quotedCols, ","), usingStr))
		}
		if i < len(def.Indexes)-1 {
			b.WriteString(",")
		}
		b.WriteString("\n")
	}

	trailer := ") ENGINE=InnoDB"
	if autoIncVal > 0 {
		trailer += fmt.Sprintf(" AUTO_INCREMENT=%d", autoIncVal+1)
	}
	trailer += " DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	if def.Comment != "" {
		trailer += fmt.Sprintf(" COMMENT='%s'", def.Comment)
	}
	b.WriteString(trailer)

	return &Result{
		Columns:     []string{"Table", "Create Table"},
		Rows:        [][]interface{}{{tableName, b.String()}},
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

// evalExpr evaluates a SQL expression that does not depend on a row context.
// It is a method on *Executor so that functions like LAST_INSERT_ID() and
// DATABASE() can access executor state.
func (e *Executor) evalExpr(expr sqlparser.Expr) (interface{}, error) {
	switch v := expr.(type) {
	case *sqlparser.Literal:
		switch v.Type {
		case sqlparser.IntVal:
			n, err := strconv.ParseInt(v.Val, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("INT_OVERFLOW:%s", v.Val)
			}
			return n, nil
		case sqlparser.FloatVal, sqlparser.DecimalVal:
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
		case "max_allowed_packet":
			return int64(67108864), nil
		case "character_set_client":
			return "utf8mb4", nil
		case "character_set_connection":
			return "utf8mb4", nil
		case "character_set_results":
			return "utf8mb4", nil
		case "collation_connection":
			return "utf8mb4_general_ci", nil
		case "sql_mode":
			return e.sqlMode, nil
		case "autocommit":
			return int64(1), nil
		}
		return "", nil
	case *sqlparser.Default:
		return nil, nil
	case *sqlparser.UnaryExpr:
		val, err := e.evalExpr(v.Expr)
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
	case *sqlparser.FuncExpr:
		return e.evalFuncExpr(v)
	case *sqlparser.ConvertExpr:
		// CAST(expr AS type)
		val, err := e.evalExpr(v.Expr)
		if err != nil {
			return nil, err
		}
		if v.Type == nil {
			return val, nil
		}
		typeName := strings.ToUpper(v.Type.Type)
		switch typeName {
		case "SIGNED", "INT", "INTEGER", "BIGINT":
			return toInt64(val), nil
		case "UNSIGNED":
			return toInt64(val), nil
		case "CHAR", "VARCHAR", "TEXT":
			return toString(val), nil
		case "DECIMAL", "FLOAT", "DOUBLE":
			return toFloat(val), nil
		}
		return val, nil
	case *sqlparser.CaseExpr:
		return e.evalCaseExpr(v)
	case *sqlparser.BinaryExpr:
		left, err := e.evalExpr(v.Left)
		if err != nil {
			return nil, err
		}
		right, err := e.evalExpr(v.Right)
		if err != nil {
			return nil, err
		}
		return evalBinaryExpr(left, right, v.Operator)
	case *sqlparser.ComparisonExpr:
		// Allow comparison expressions to be used as boolean values (e.g. in IF args)
		left, err := e.evalExpr(v.Left)
		if err != nil {
			return nil, err
		}
		right, err := e.evalExpr(v.Right)
		if err != nil {
			return nil, err
		}
		result, err := compareValues(left, right, v.Operator)
		if err != nil {
			return nil, err
		}
		if result {
			return int64(1), nil
		}
		return int64(0), nil
	case *sqlparser.TrimFuncExpr:
		// TRIM / LTRIM / RTRIM parsed as TrimFuncExpr
		val, err := e.evalExpr(v.StringArg)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		s := toString(val)
		switch v.TrimFuncType {
		case sqlparser.LTrimType:
			return strings.TrimLeft(s, " \t\n\r"), nil
		case sqlparser.RTrimType:
			return strings.TrimRight(s, " \t\n\r"), nil
		default: // NormalTrimType
			if v.TrimArg != nil {
				// TRIM(trimChar FROM str) - trim specific char
				trimVal, err := e.evalExpr(v.TrimArg)
				if err != nil {
					return nil, err
				}
				trimStr := toString(trimVal)
				switch v.Type {
				case sqlparser.LeadingTrimType:
					return strings.TrimLeft(s, trimStr), nil
				case sqlparser.TrailingTrimType:
					return strings.TrimRight(s, trimStr), nil
				default: // Both
					return strings.Trim(s, trimStr), nil
				}
			}
			return strings.TrimSpace(s), nil
		}
	case *sqlparser.SubstrExpr:
		// SUBSTRING(str, from, to) parsed as SubstrExpr
		strVal, err := e.evalExpr(v.Name)
		if err != nil {
			return nil, err
		}
		if strVal == nil {
			return nil, nil
		}
		s := []rune(toString(strVal))
		posVal, err := e.evalExpr(v.From)
		if err != nil {
			return nil, err
		}
		pos := int(toInt64(posVal))
		if pos > 0 {
			pos-- // 1-based to 0-based
		} else if pos < 0 {
			pos = len(s) + pos
		}
		if pos < 0 {
			pos = 0
		}
		if pos >= len(s) {
			return "", nil
		}
		if v.To != nil {
			lenVal, err := e.evalExpr(v.To)
			if err != nil {
				return nil, err
			}
			length := int(toInt64(lenVal))
			if length <= 0 {
				return "", nil
			}
			end := pos + length
			if end > len(s) {
				end = len(s)
			}
			return string(s[pos:end]), nil
		}
		return string(s[pos:]), nil
	case *sqlparser.IntroducerExpr:
		// e.g. _latin1 'string' — ignore the charset and evaluate the inner expression
		return e.evalExpr(v.Expr)
	case *sqlparser.CastExpr:
		// Simplified CAST: just evaluate the inner expression
		return e.evalExpr(v.Expr)
	case *sqlparser.CurTimeFuncExpr:
		// NOW(), CURRENT_TIMESTAMP(), CURTIME(), etc.
		name := strings.ToLower(v.Name.String())
		now := e.nowTime()
		switch name {
		case "now", "current_timestamp", "localtime", "localtimestamp", "sysdate":
			return now.Format("2006-01-02 15:04:05"), nil
		case "curdate", "current_date":
			return now.Format("2006-01-02"), nil
		case "curtime", "current_time":
			return now.Format("15:04:05"), nil
		case "utc_timestamp":
			return e.nowTime().UTC().Format("2006-01-02 15:04:05"), nil
		case "utc_date":
			return e.nowTime().UTC().Format("2006-01-02"), nil
		case "utc_time":
			return e.nowTime().UTC().Format("15:04:05"), nil
		default:
			return now.Format("2006-01-02 15:04:05"), nil
		}
	}
	return nil, fmt.Errorf("unsupported expression: %T (%s)", expr, sqlparser.String(expr))
}

// evalFuncExpr handles MySQL built-in function calls.
func (e *Executor) evalFuncExpr(v *sqlparser.FuncExpr) (interface{}, error) {
	name := strings.ToLower(v.Name.String())
	switch name {
	case "last_insert_id":
		if len(v.Exprs) > 0 {
			val, err := e.evalExpr(v.Exprs[0])
			if err != nil {
				return nil, err
			}
			e.lastInsertID = toInt64(val)
			return e.lastInsertID, nil
		}
		return e.lastInsertID, nil
	case "now", "current_timestamp", "sysdate":
		return e.nowTime().Format("2006-01-02 15:04:05"), nil
	case "curdate", "current_date":
		return e.nowTime().Format("2006-01-02"), nil
	case "curtime", "current_time":
		return e.nowTime().Format("15:04:05"), nil
	case "database", "schema":
		return e.CurrentDB, nil
	case "version":
		return "8.4.0-mylite", nil
	case "concat":
		var sb strings.Builder
		for _, argExpr := range v.Exprs {
			val, err := e.evalExpr(argExpr)
			if err != nil {
				return nil, err
			}
			if val == nil {
				return nil, nil // CONCAT with NULL returns NULL
			}
			sb.WriteString(toString(val))
		}
		return sb.String(), nil
	case "concat_ws":
		if len(v.Exprs) < 1 {
			return nil, nil
		}
		sepVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		if sepVal == nil {
			return nil, nil
		}
		sep := toString(sepVal)
		var parts []string
		for _, argExpr := range v.Exprs[1:] {
			val, err := e.evalExpr(argExpr)
			if err != nil {
				return nil, err
			}
			if val == nil {
				continue // CONCAT_WS skips NULLs
			}
			parts = append(parts, toString(val))
		}
		return strings.Join(parts, sep), nil
	case "ifnull", "nvl":
		if len(v.Exprs) < 2 {
			return nil, fmt.Errorf("IFNULL requires 2 arguments")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		if val != nil {
			return val, nil
		}
		return e.evalExpr(v.Exprs[1])
	case "coalesce":
		for _, argExpr := range v.Exprs {
			val, err := e.evalExpr(argExpr)
			if err != nil {
				return nil, err
			}
			if val != nil {
				return val, nil
			}
		}
		return nil, nil
	case "if":
		if len(v.Exprs) < 3 {
			return nil, fmt.Errorf("IF requires 3 arguments")
		}
		cond, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		if isTruthy(cond) {
			return e.evalExpr(v.Exprs[1])
		}
		return e.evalExpr(v.Exprs[2])
	case "upper", "ucase":
		if len(v.Exprs) < 1 {
			return nil, fmt.Errorf("UPPER requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		return strings.ToUpper(toString(val)), nil
	case "lower", "lcase":
		if len(v.Exprs) < 1 {
			return nil, fmt.Errorf("LOWER requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		return strings.ToLower(toString(val)), nil
	case "length", "octet_length":
		if len(v.Exprs) < 1 {
			return nil, fmt.Errorf("LENGTH requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		return int64(len(toString(val))), nil
	case "char_length", "character_length":
		if len(v.Exprs) < 1 {
			return nil, fmt.Errorf("CHAR_LENGTH requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		return int64(len([]rune(toString(val)))), nil
	case "substring", "substr", "mid":
		if len(v.Exprs) < 2 {
			return nil, fmt.Errorf("SUBSTRING requires at least 2 arguments")
		}
		strVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		if strVal == nil {
			return nil, nil
		}
		s := []rune(toString(strVal))
		posVal, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, err
		}
		pos := int(toInt64(posVal))
		// MySQL positions are 1-based; negative positions count from end
		if pos > 0 {
			pos-- // convert to 0-based
		} else if pos < 0 {
			pos = len(s) + pos
		}
		if pos < 0 {
			pos = 0
		}
		if pos >= len(s) {
			return "", nil
		}
		if len(v.Exprs) >= 3 {
			lenVal, err := e.evalExpr(v.Exprs[2])
			if err != nil {
				return nil, err
			}
			length := int(toInt64(lenVal))
			if length <= 0 {
				return "", nil
			}
			end := pos + length
			if end > len(s) {
				end = len(s)
			}
			return string(s[pos:end]), nil
		}
		return string(s[pos:]), nil
	case "trim":
		if len(v.Exprs) < 1 {
			return nil, fmt.Errorf("TRIM requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		return strings.TrimSpace(toString(val)), nil
	case "ltrim":
		if len(v.Exprs) < 1 {
			return nil, fmt.Errorf("LTRIM requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		return strings.TrimLeft(toString(val), " \t\n\r"), nil
	case "rtrim":
		if len(v.Exprs) < 1 {
			return nil, fmt.Errorf("RTRIM requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		return strings.TrimRight(toString(val), " \t\n\r"), nil
	case "replace":
		if len(v.Exprs) < 3 {
			return nil, fmt.Errorf("REPLACE requires 3 arguments")
		}
		strVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		fromVal, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, err
		}
		toVal, err := e.evalExpr(v.Exprs[2])
		if err != nil {
			return nil, err
		}
		if strVal == nil {
			return nil, nil
		}
		return strings.ReplaceAll(toString(strVal), toString(fromVal), toString(toVal)), nil
	case "left":
		if len(v.Exprs) < 2 {
			return nil, fmt.Errorf("LEFT requires 2 arguments")
		}
		strVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		if strVal == nil {
			return nil, nil
		}
		lenVal, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, err
		}
		s := []rune(toString(strVal))
		n := int(toInt64(lenVal))
		if n <= 0 {
			return "", nil
		}
		if n > len(s) {
			n = len(s)
		}
		return string(s[:n]), nil
	case "right":
		if len(v.Exprs) < 2 {
			return nil, fmt.Errorf("RIGHT requires 2 arguments")
		}
		strVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		if strVal == nil {
			return nil, nil
		}
		lenVal, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, err
		}
		s := []rune(toString(strVal))
		n := int(toInt64(lenVal))
		if n <= 0 {
			return "", nil
		}
		if n > len(s) {
			n = len(s)
		}
		return string(s[len(s)-n:]), nil
	case "abs":
		if len(v.Exprs) < 1 {
			return nil, fmt.Errorf("ABS requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		f := toFloat(val)
		if f < 0 {
			f = -f
		}
		if f == float64(int64(f)) {
			return int64(f), nil
		}
		return f, nil
	case "floor":
		if len(v.Exprs) < 1 {
			return nil, fmt.Errorf("FLOOR requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		f := toFloat(val)
		return int64(f), nil
	case "ceil", "ceiling":
		if len(v.Exprs) < 1 {
			return nil, fmt.Errorf("CEIL requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		f := toFloat(val)
		n := int64(f)
		if float64(n) < f {
			n++
		}
		return n, nil
	case "round":
		if len(v.Exprs) < 1 {
			return nil, fmt.Errorf("ROUND requires at least 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		f := toFloat(val)
		decimals := int64(0)
		if len(v.Exprs) >= 2 {
			dv, err := e.evalExpr(v.Exprs[1])
			if err != nil {
				return nil, err
			}
			decimals = toInt64(dv)
		}
		if decimals == 0 {
			return int64(f + 0.5), nil
		}
		factor := 1.0
		for i := int64(0); i < decimals; i++ {
			factor *= 10
		}
		return float64(int64(f*factor+0.5)) / factor, nil
	case "mod":
		if len(v.Exprs) < 2 {
			return nil, fmt.Errorf("MOD requires 2 arguments")
		}
		v0, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		v1, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, err
		}
		if v0 == nil || v1 == nil {
			return nil, nil
		}
		d := toInt64(v1)
		if d == 0 {
			return nil, nil
		}
		return toInt64(v0) % d, nil
	case "isnull":
		if len(v.Exprs) < 1 {
			return nil, fmt.Errorf("ISNULL requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		if val == nil {
			return int64(1), nil
		}
		return int64(0), nil
	case "nullif":
		if len(v.Exprs) < 2 {
			return nil, fmt.Errorf("NULLIF requires 2 arguments")
		}
		v0, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		v1, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, err
		}
		if fmt.Sprintf("%v", v0) == fmt.Sprintf("%v", v1) {
			return nil, nil
		}
		return v0, nil
	case "unix_timestamp":
		if len(v.Exprs) == 0 {
			return int64(e.nowTime().Unix()), nil
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, err
		}
		return int64(t.Unix()), nil
	case "from_unixtime":
		if len(v.Exprs) < 1 {
			return nil, fmt.Errorf("FROM_UNIXTIME requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		ts := toInt64(val)
		t := time.Unix(ts, 0)
		if len(v.Exprs) >= 2 {
			fmtVal, err := e.evalExpr(v.Exprs[1])
			if err != nil {
				return nil, err
			}
			return mysqlDateFormat(t, toString(fmtVal)), nil
		}
		return t.Format("2006-01-02 15:04:05"), nil
	case "year":
		if len(v.Exprs) < 1 {
			return nil, fmt.Errorf("YEAR requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, err
		}
		return int64(t.Year()), nil
	case "month":
		if len(v.Exprs) < 1 {
			return nil, fmt.Errorf("MONTH requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, err
		}
		return int64(t.Month()), nil
	case "day", "dayofmonth":
		if len(v.Exprs) < 1 {
			return nil, fmt.Errorf("DAY requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, err
		}
		return int64(t.Day()), nil
	case "hour":
		if len(v.Exprs) < 1 {
			return nil, fmt.Errorf("HOUR requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, err
		}
		return int64(t.Hour()), nil
	case "minute":
		if len(v.Exprs) < 1 {
			return nil, fmt.Errorf("MINUTE requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, err
		}
		return int64(t.Minute()), nil
	case "second":
		if len(v.Exprs) < 1 {
			return nil, fmt.Errorf("SECOND requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, err
		}
		return int64(t.Second()), nil
	case "date":
		if len(v.Exprs) < 1 {
			return nil, fmt.Errorf("DATE requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, err
		}
		return t.Format("2006-01-02"), nil
	case "time":
		if len(v.Exprs) < 1 {
			return nil, fmt.Errorf("TIME requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, err
		}
		return t.Format("15:04:05"), nil
	case "datediff":
		if len(v.Exprs) < 2 {
			return nil, fmt.Errorf("DATEDIFF requires 2 arguments")
		}
		v0, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		v1, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, err
		}
		t0, err := parseDateTimeValue(v0)
		if err != nil {
			return nil, err
		}
		t1, err := parseDateTimeValue(v1)
		if err != nil {
			return nil, err
		}
		// Truncate to date only for comparison
		d0 := time.Date(t0.Year(), t0.Month(), t0.Day(), 0, 0, 0, 0, time.UTC)
		d1 := time.Date(t1.Year(), t1.Month(), t1.Day(), 0, 0, 0, 0, time.UTC)
		diff := int64(d0.Sub(d1).Hours() / 24)
		return diff, nil
	case "date_format":
		if len(v.Exprs) < 2 {
			return nil, fmt.Errorf("DATE_FORMAT requires 2 arguments")
		}
		dateVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		fmtVal, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, err
		}
		t, err := parseDateTimeValue(dateVal)
		if err != nil {
			return nil, err
		}
		return mysqlDateFormat(t, toString(fmtVal)), nil
	case "hex":
		if len(v.Exprs) < 1 {
			return nil, nil
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		switch tv := val.(type) {
		case int64:
			return strings.ToUpper(fmt.Sprintf("%X", tv)), nil
		case float64:
			return strings.ToUpper(fmt.Sprintf("%X", int64(tv))), nil
		default:
			s := toString(val)
			return strings.ToUpper(fmt.Sprintf("%X", []byte(s))), nil
		}
	case "unhex":
		if len(v.Exprs) < 1 {
			return nil, nil
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		decoded, err := hex.DecodeString(toString(val))
		if err != nil {
			return nil, nil
		}
		return string(decoded), nil
	case "addtime":
		if len(v.Exprs) < 2 {
			return nil, nil
		}
		base, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		interval, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, err
		}
		t, err := parseDateTimeValue(base)
		if err != nil {
			// Return as-is for unparseable values
			return toString(base), nil
		}
		dur, err := parseMySQLTimeInterval(toString(interval))
		if err != nil {
			return toString(base), nil
		}
		return t.Add(dur).Format("2006-01-02 15:04:05"), nil
	case "subtime":
		if len(v.Exprs) < 2 {
			return nil, nil
		}
		base, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		interval, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, err
		}
		t, err := parseDateTimeValue(base)
		if err != nil {
			return toString(base), nil
		}
		dur, err := parseMySQLTimeInterval(toString(interval))
		if err != nil {
			return toString(base), nil
		}
		return t.Add(-dur).Format("2006-01-02 15:04:05"), nil
	case "repeat":
		if len(v.Exprs) < 2 {
			return nil, nil
		}
		s, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, err
		}
		n, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, err
		}
		count := int(toInt64(n))
		if count <= 0 || s == nil {
			return "", nil
		}
		return strings.Repeat(toString(s), count), nil
	case "cast", "convert":
		// Simplified CAST: just evaluate the inner expression
		if len(v.Exprs) >= 1 {
			return e.evalExpr(v.Exprs[0])
		}
		return nil, nil
	}
	// Unknown function: return nil rather than error to be lenient
	return nil, fmt.Errorf("unsupported function: %s", name)
}

// parseDateTimeValue parses a date/time interface value into a time.Time.
// Supports string formats: "2006-01-02", "2006-01-02 15:04:05", "15:04:05", "2006-01-02T15:04:05".
func parseDateTimeValue(val interface{}) (time.Time, error) {
	if val == nil {
		return time.Time{}, fmt.Errorf("NULL date value")
	}
	s := toString(val)
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02",
		"15:04:05",
		"2006-01-02 15:04:05.999999999",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse date/time value: %q", s)
}

// parseMySQLTimeInterval parses MySQL time interval strings like "1 01:01:01" or "01:01:01".
func parseMySQLTimeInterval(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	var days, hours, mins, secs int

	// Format: "D HH:MM:SS" or "HH:MM:SS" or "D"
	if idx := strings.Index(s, " "); idx >= 0 {
		d, err := strconv.Atoi(s[:idx])
		if err != nil {
			return 0, err
		}
		days = d
		s = s[idx+1:]
	}

	parts := strings.Split(s, ":")
	switch len(parts) {
	case 3:
		h, _ := strconv.Atoi(parts[0])
		m, _ := strconv.Atoi(parts[1])
		sc, _ := strconv.Atoi(parts[2])
		hours, mins, secs = h, m, sc
	case 2:
		h, _ := strconv.Atoi(parts[0])
		m, _ := strconv.Atoi(parts[1])
		hours, mins = h, m
	case 1:
		if parts[0] != "" {
			h, _ := strconv.Atoi(parts[0])
			hours = h
		}
	}

	return time.Duration(days)*24*time.Hour +
		time.Duration(hours)*time.Hour +
		time.Duration(mins)*time.Minute +
		time.Duration(secs)*time.Second, nil
}

// mysqlDateFormat converts a MySQL DATE_FORMAT format string (e.g. "%Y-%m-%d") to a Go time.Time string.
func mysqlDateFormat(t time.Time, format string) string {
	var sb strings.Builder
	i := 0
	for i < len(format) {
		if format[i] == '%' && i+1 < len(format) {
			i++
			switch format[i] {
			case 'Y':
				sb.WriteString(t.Format("2006"))
			case 'y':
				sb.WriteString(t.Format("06"))
			case 'm':
				sb.WriteString(t.Format("01"))
			case 'c':
				sb.WriteString(fmt.Sprintf("%d", t.Month()))
			case 'd':
				sb.WriteString(t.Format("02"))
			case 'e':
				sb.WriteString(fmt.Sprintf("%d", t.Day()))
			case 'H':
				sb.WriteString(t.Format("15"))
			case 'h', 'I':
				sb.WriteString(t.Format("03"))
			case 'i':
				sb.WriteString(t.Format("04"))
			case 's', 'S':
				sb.WriteString(t.Format("05"))
			case 'p':
				sb.WriteString(t.Format("PM"))
			case 'W':
				sb.WriteString(t.Format("Monday"))
			case 'w':
				sb.WriteString(fmt.Sprintf("%d", t.Weekday()))
			case 'j':
				sb.WriteString(fmt.Sprintf("%d", t.YearDay()))
			case 'M':
				sb.WriteString(t.Format("January"))
			case 'b':
				sb.WriteString(t.Format("Jan"))
			case 'T':
				sb.WriteString(t.Format("15:04:05"))
			case 'r':
				sb.WriteString(t.Format("03:04:05 PM"))
			case '%':
				sb.WriteByte('%')
			default:
				sb.WriteByte('%')
				sb.WriteByte(format[i])
			}
		} else {
			sb.WriteByte(format[i])
		}
		i++
	}
	return sb.String()
}

// evalCaseExpr handles CASE expressions.
func (e *Executor) evalCaseExpr(v *sqlparser.CaseExpr) (interface{}, error) {
	var baseVal interface{}
	if v.Expr != nil {
		var err error
		baseVal, err = e.evalExpr(v.Expr)
		if err != nil {
			return nil, err
		}
	}
	for _, when := range v.Whens {
		condVal, err := e.evalExpr(when.Cond)
		if err != nil {
			return nil, err
		}
		matched := false
		if v.Expr != nil {
			// Simple CASE: compare base to each WHEN value
			matched = fmt.Sprintf("%v", baseVal) == fmt.Sprintf("%v", condVal)
		} else {
			// Searched CASE: each WHEN is a boolean expression
			matched = isTruthy(condVal)
		}
		if matched {
			return e.evalExpr(when.Val)
		}
	}
	if v.Else != nil {
		return e.evalExpr(v.Else)
	}
	return nil, nil
}

// evalBinaryExpr evaluates arithmetic binary expressions.
func evalBinaryExpr(left, right interface{}, op sqlparser.BinaryExprOperator) (interface{}, error) {
	lf := toFloat(left)
	rf := toFloat(right)
	var result float64
	switch op {
	case sqlparser.PlusOp:
		result = lf + rf
	case sqlparser.MinusOp:
		result = lf - rf
	case sqlparser.MultOp:
		result = lf * rf
	case sqlparser.DivOp:
		if rf == 0 {
			return nil, nil // MySQL returns NULL for division by zero
		}
		result = lf / rf
	case sqlparser.IntDivOp:
		if rf == 0 {
			return nil, nil
		}
		return int64(lf / rf), nil
	case sqlparser.ModOp:
		if rf == 0 {
			return nil, nil
		}
		return int64(lf) % int64(rf), nil
	default:
		return nil, fmt.Errorf("unsupported binary operator: %v", op)
	}
	if result == float64(int64(result)) {
		return int64(result), nil
	}
	return result, nil
}

// toString converts a value to string.
func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case int64:
		return strconv.FormatInt(val, 10)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		if val {
			return "1"
		}
		return "0"
	}
	return fmt.Sprintf("%v", v)
}

// toInt64 converts a value to int64.
func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case float64:
		return int64(n)
	case string:
		i, _ := strconv.ParseInt(n, 10, 64)
		return i
	case bool:
		if n {
			return 1
		}
		return 0
	}
	return 0
}

// isTruthy returns true if the value is considered truthy in MySQL.
func isTruthy(v interface{}) bool {
	if v == nil {
		return false
	}
	switch val := v.(type) {
	case bool:
		return val
	case int64:
		return val != 0
	case float64:
		return val != 0
	case string:
		return val != "" && val != "0"
	}
	return false
}

// evalRowExpr evaluates an expression in the context of a table row.
// It handles column lookups and delegates other expressions to e.evalExpr.
func (e *Executor) evalRowExpr(expr sqlparser.Expr, row storage.Row) (interface{}, error) {
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
		if val, ok := row[colName]; ok {
			return val, nil
		}
		// Case-insensitive fallback (needed for information_schema columns)
		upperName := strings.ToUpper(colName)
		for k, v := range row {
			if strings.ToUpper(k) == upperName {
				return v, nil
			}
		}
		return nil, nil
	case *sqlparser.BinaryExpr:
		left, err := e.evalRowExpr(v.Left, row)
		if err != nil {
			return nil, err
		}
		right, err := e.evalRowExpr(v.Right, row)
		if err != nil {
			return nil, err
		}
		return evalBinaryExpr(left, right, v.Operator)
	case *sqlparser.UnaryExpr:
		val, err := e.evalRowExpr(v.Expr, row)
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
	case *sqlparser.FuncExpr:
		// Evaluate function arguments with row context
		return e.evalFuncExprWithRow(v, row)
	case *sqlparser.CountStar, *sqlparser.Count, *sqlparser.Sum, *sqlparser.Max, *sqlparser.Min, *sqlparser.Avg:
		// For HAVING clause: look up the aggregate display name in the row
		displayName := aggregateDisplayName(expr)
		if val, ok := row[displayName]; ok {
			return val, nil
		}
		// Fallback: compute the aggregate (for single-row context)
		return e.evalExpr(expr)
	default:
		return e.evalExpr(expr)
	}
}

// evalFuncExprWithRow evaluates a function expression with row context for column references.
func (e *Executor) evalFuncExprWithRow(v *sqlparser.FuncExpr, row storage.Row) (interface{}, error) {
	// Evaluate function arguments with row context to resolve column references
	name := strings.ToLower(v.Name.String())

	// Helper to evaluate args with row context
	evalArgs := func() ([]interface{}, error) {
		args := make([]interface{}, len(v.Exprs))
		for i, argExpr := range v.Exprs {
			val, err := e.evalRowExpr(argExpr, row)
			if err != nil {
				return nil, err
			}
			args[i] = val
		}
		return args, nil
	}

	switch name {
	case "upper", "ucase":
		args, err := evalArgs()
		if err != nil {
			return nil, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		return strings.ToUpper(toString(args[0])), nil
	case "lower", "lcase":
		args, err := evalArgs()
		if err != nil {
			return nil, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		return strings.ToLower(toString(args[0])), nil
	case "concat":
		args, err := evalArgs()
		if err != nil {
			return nil, err
		}
		var sb strings.Builder
		for _, a := range args {
			if a == nil {
				return nil, nil
			}
			sb.WriteString(toString(a))
		}
		return sb.String(), nil
	case "length", "octet_length":
		args, err := evalArgs()
		if err != nil {
			return nil, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		return int64(len(toString(args[0]))), nil
	case "char_length", "character_length":
		args, err := evalArgs()
		if err != nil {
			return nil, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		return int64(len([]rune(toString(args[0])))), nil
	case "date":
		args, err := evalArgs()
		if err != nil {
			return nil, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		t, err := parseDateTimeValue(args[0])
		if err != nil {
			return nil, err
		}
		return t.Format("2006-01-02"), nil
	case "year":
		args, err := evalArgs()
		if err != nil {
			return nil, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		t, err := parseDateTimeValue(args[0])
		if err != nil {
			return nil, err
		}
		return int64(t.Year()), nil
	case "month":
		args, err := evalArgs()
		if err != nil {
			return nil, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		t, err := parseDateTimeValue(args[0])
		if err != nil {
			return nil, err
		}
		return int64(t.Month()), nil
	case "day", "dayofmonth":
		args, err := evalArgs()
		if err != nil {
			return nil, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		t, err := parseDateTimeValue(args[0])
		if err != nil {
			return nil, err
		}
		return int64(t.Day()), nil
	case "hour":
		args, err := evalArgs()
		if err != nil {
			return nil, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		t, err := parseDateTimeValue(args[0])
		if err != nil {
			return nil, err
		}
		return int64(t.Hour()), nil
	case "minute":
		args, err := evalArgs()
		if err != nil {
			return nil, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		t, err := parseDateTimeValue(args[0])
		if err != nil {
			return nil, err
		}
		return int64(t.Minute()), nil
	case "second":
		args, err := evalArgs()
		if err != nil {
			return nil, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, nil
		}
		t, err := parseDateTimeValue(args[0])
		if err != nil {
			return nil, err
		}
		return int64(t.Second()), nil
	case "if":
		if len(v.Exprs) < 3 {
			return nil, fmt.Errorf("IF requires 3 arguments")
		}
		cond, err := e.evalRowExpr(v.Exprs[0], row)
		if err != nil {
			return nil, err
		}
		if isTruthy(cond) {
			return e.evalRowExpr(v.Exprs[1], row)
		}
		return e.evalRowExpr(v.Exprs[2], row)
	case "ifnull", "nvl":
		if len(v.Exprs) < 2 {
			return nil, nil
		}
		val, err := e.evalRowExpr(v.Exprs[0], row)
		if err != nil {
			return nil, err
		}
		if val != nil {
			return val, nil
		}
		return e.evalRowExpr(v.Exprs[1], row)
	case "coalesce":
		for _, argExpr := range v.Exprs {
			val, err := e.evalRowExpr(argExpr, row)
			if err != nil {
				return nil, err
			}
			if val != nil {
				return val, nil
			}
		}
		return nil, nil
	default:
		// Fallback: delegate to evalFuncExpr (no row context for args)
		return e.evalFuncExpr(v)
	}
}

// evalRowExpr is a package-level shim for backward-compatible callers that
// do not have access to an executor.  It creates a temporary executor with
// empty state, which is sufficient for column-lookup and literal evaluation.
func evalRowExpr(expr sqlparser.Expr, row storage.Row) (interface{}, error) {
	e := &Executor{}
	return e.evalRowExpr(expr, row)
}

// evalHaving evaluates a HAVING predicate, with support for aggregate functions
// that are computed against the group's rows.
func (e *Executor) evalHaving(expr sqlparser.Expr, havingRow storage.Row, groupRows []storage.Row) (bool, error) {
	// Pre-compute any aggregate expressions in the HAVING clause and add to the row
	enrichedRow := make(storage.Row, len(havingRow))
	for k, v := range havingRow {
		enrichedRow[k] = v
	}
	// Walk the expression to find aggregates and compute them
	e.addAggregatesToRow(expr, enrichedRow, groupRows)
	return e.evalWhere(expr, enrichedRow)
}

// addAggregatesToRow walks an expression tree and computes any aggregate functions,
// storing their results in the row with their display names.
func (e *Executor) addAggregatesToRow(expr sqlparser.Expr, row storage.Row, groupRows []storage.Row) {
	switch v := expr.(type) {
	case *sqlparser.ComparisonExpr:
		e.addAggregatesToRow(v.Left, row, groupRows)
		e.addAggregatesToRow(v.Right, row, groupRows)
	case *sqlparser.AndExpr:
		e.addAggregatesToRow(v.Left, row, groupRows)
		e.addAggregatesToRow(v.Right, row, groupRows)
	case *sqlparser.OrExpr:
		e.addAggregatesToRow(v.Left, row, groupRows)
		e.addAggregatesToRow(v.Right, row, groupRows)
	case *sqlparser.CountStar, *sqlparser.Count, *sqlparser.Sum, *sqlparser.Max, *sqlparser.Min, *sqlparser.Avg:
		displayName := aggregateDisplayName(expr)
		if _, ok := row[displayName]; !ok {
			repRow := storage.Row{}
			if len(groupRows) > 0 {
				repRow = groupRows[0]
			}
			val, err := evalAggregateExpr(expr, groupRows, repRow)
			if err == nil {
				row[displayName] = val
			}
		}
	}
}

// evalWhere evaluates a WHERE predicate against a row.
func (e *Executor) evalWhere(expr sqlparser.Expr, row storage.Row) (bool, error) {
	switch v := expr.(type) {
	case *sqlparser.ComparisonExpr:
		// Handle IN / NOT IN specially because the right side is a ValTuple, not a scalar.
		if v.Operator == sqlparser.InOp || v.Operator == sqlparser.NotInOp {
			left, err := e.evalRowExpr(v.Left, row)
			if err != nil {
				return false, err
			}
			tuple, ok := v.Right.(sqlparser.ValTuple)
			if !ok {
				return false, fmt.Errorf("IN/NOT IN right side must be a value tuple, got %T", v.Right)
			}
			for _, tupleExpr := range tuple {
				val, err := e.evalRowExpr(tupleExpr, row)
				if err != nil {
					return false, err
				}
				if fmt.Sprintf("%v", left) == fmt.Sprintf("%v", val) {
					return v.Operator == sqlparser.InOp, nil
				}
			}
			return v.Operator == sqlparser.NotInOp, nil
		}
		left, err := e.evalRowExpr(v.Left, row)
		if err != nil {
			return false, err
		}
		right, err := e.evalRowExpr(v.Right, row)
		if err != nil {
			return false, err
		}
		return compareValues(left, right, v.Operator)
	case *sqlparser.AndExpr:
		l, err := e.evalWhere(v.Left, row)
		if err != nil {
			return false, err
		}
		r, err := e.evalWhere(v.Right, row)
		if err != nil {
			return false, err
		}
		return l && r, nil
	case *sqlparser.OrExpr:
		l, err := e.evalWhere(v.Left, row)
		if err != nil {
			return false, err
		}
		r, err := e.evalWhere(v.Right, row)
		if err != nil {
			return false, err
		}
		return l || r, nil
	case *sqlparser.IsExpr:
		val, err := e.evalRowExpr(v.Left, row)
		if err != nil {
			return false, err
		}
		switch v.Right {
		case sqlparser.IsNullOp:
			if e.sqlAutoIsNull && e.lastAutoIncID > 0 && val != nil {
				if fmt.Sprintf("%v", val) == fmt.Sprintf("%v", e.lastAutoIncID) {
					return true, nil
				}
			}
			return val == nil, nil
		case sqlparser.IsNotNullOp:
			return val != nil, nil
		case sqlparser.IsTrueOp:
			return val == true || val == int64(1), nil
		case sqlparser.IsFalseOp:
			return val == false || val == int64(0), nil
		}
	case *sqlparser.BetweenExpr:
		val, err := e.evalRowExpr(v.Left, row)
		if err != nil {
			return false, err
		}
		from, err := e.evalRowExpr(v.From, row)
		if err != nil {
			return false, err
		}
		to, err := e.evalRowExpr(v.To, row)
		if err != nil {
			return false, err
		}
		geFrom, err := compareValues(val, from, sqlparser.GreaterEqualOp)
		if err != nil {
			return false, err
		}
		leTo, err := compareValues(val, to, sqlparser.LessEqualOp)
		if err != nil {
			return false, err
		}
		result := geFrom && leTo
		if v.IsBetween {
			return result, nil
		}
		return !result, nil
	case *sqlparser.ExistsExpr:
		// EXISTS subquery - not supported yet, return false
		return false, nil
	case *sqlparser.NotExpr:
		inner, err := e.evalWhere(v.Expr, row)
		if err != nil {
			return false, err
		}
		return !inner, nil
	}
	return false, fmt.Errorf("unsupported WHERE expression: %T", expr)
}

// evalWhere is a package-level shim for backward-compatible callers.
func evalWhere(expr sqlparser.Expr, row storage.Row) (bool, error) {
	e := &Executor{}
	return e.evalWhere(expr, row)
}

func compareValues(left, right interface{}, op sqlparser.ComparisonExprOperator) (bool, error) {
	// NULL-safe equal (<=>): true if both NULL, false if one is NULL, otherwise normal equality.
	if op == sqlparser.NullSafeEqualOp {
		if left == nil && right == nil {
			return true, nil
		}
		if left == nil || right == nil {
			return false, nil
		}
		return fmt.Sprintf("%v", left) == fmt.Sprintf("%v", right), nil
	}

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

	// Use a bare executor: LIMIT values are always literals.
	e := &Executor{}
	lim, err := e.evalExpr(limit.Rowcount)
	if err != nil {
		return nil, err
	}
	n, ok := lim.(int64)
	if !ok {
		return rows, nil
	}

	offset := int64(0)
	if limit.Offset != nil {
		off, err := e.evalExpr(limit.Offset)
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

// execTruncateTable handles TRUNCATE TABLE statements.
func (e *Executor) execTruncateTable(stmt *sqlparser.TruncateTable) (*Result, error) {
	tableName := stmt.Table.Name.String()
	tbl, err := e.Storage.GetTable(e.CurrentDB, tableName)
	if err != nil {
		return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", e.CurrentDB, tableName))
	}
	tbl.Truncate()
	return &Result{AffectedRows: 0, IsResultSet: false}, nil
}

// ==============================================================================
// Trigger support
// ==============================================================================

// execCreateTrigger parses and stores a CREATE TRIGGER statement.
// Format: CREATE TRIGGER name timing event ON table FOR EACH ROW BEGIN ... END
func (e *Executor) execCreateTrigger(query string) (*Result, error) {
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", e.CurrentDB))
	}

	// Parse the CREATE TRIGGER statement manually
	// CREATE TRIGGER <name> <BEFORE|AFTER> <INSERT|UPDATE|DELETE> ON <table> FOR EACH ROW [BEGIN] <body> [END]
	upper := strings.ToUpper(query)
	// Remove "CREATE TRIGGER " prefix
	rest := strings.TrimSpace(query[len("CREATE TRIGGER "):])

	// Extract trigger name
	parts := strings.Fields(rest)
	if len(parts) < 6 {
		return nil, fmt.Errorf("invalid CREATE TRIGGER syntax")
	}
	triggerName := parts[0]
	timing := strings.ToUpper(parts[1])   // BEFORE or AFTER
	event := strings.ToUpper(parts[2])     // INSERT, UPDATE, or DELETE

	// Find "ON" keyword
	onIdx := -1
	for i, p := range parts {
		if strings.ToUpper(p) == "ON" && i > 2 {
			onIdx = i
			break
		}
	}
	if onIdx < 0 {
		return nil, fmt.Errorf("invalid CREATE TRIGGER syntax: missing ON")
	}
	tableName := parts[onIdx+1]
	tableName = strings.Trim(tableName, "`")

	// Extract body: everything after "FOR EACH ROW"
	_ = upper // already have it
	forEachIdx := strings.Index(upper, "FOR EACH ROW")
	if forEachIdx < 0 {
		return nil, fmt.Errorf("invalid CREATE TRIGGER syntax: missing FOR EACH ROW")
	}
	body := strings.TrimSpace(query[forEachIdx+len("FOR EACH ROW"):])

	// Parse the body into individual SQL statements
	var bodyStatements []string
	bodyUpper := strings.ToUpper(strings.TrimSpace(body))
	if strings.HasPrefix(bodyUpper, "BEGIN") {
		// Strip BEGIN and END
		inner := strings.TrimSpace(body[len("BEGIN"):])
		if strings.HasSuffix(strings.ToUpper(strings.TrimSpace(inner)), "END") {
			inner = strings.TrimSpace(inner[:len(inner)-len("END")])
		}
		// Split by semicolons (respecting quoted strings)
		bodyStatements = splitTriggerBody(inner)
	} else {
		// Single statement trigger
		body = strings.TrimRight(body, ";")
		bodyStatements = []string{strings.TrimSpace(body)}
	}

	// Validate: AFTER triggers cannot modify NEW row
	if timing == "AFTER" {
		for _, stmt := range bodyStatements {
			stmtUpper := strings.ToUpper(stmt)
			if strings.Contains(stmtUpper, "SET NEW.") {
				return nil, mysqlError(1362, "HY000", "Updating of NEW row is not allowed in after trigger")
			}
		}
	}
	// Validate: BEFORE/AFTER DELETE triggers cannot reference NEW
	if event == "DELETE" {
		for _, stmt := range bodyStatements {
			stmtUpper := strings.ToUpper(stmt)
			if strings.Contains(stmtUpper, "NEW.") {
				return nil, mysqlError(1363, "HY000", "There is no NEW row in on DELETE trigger")
			}
		}
	}
	// Validate: BEFORE/AFTER INSERT triggers cannot reference OLD
	if event == "INSERT" {
		for _, stmt := range bodyStatements {
			stmtUpper := strings.ToUpper(stmt)
			if strings.Contains(stmtUpper, "OLD.") {
				return nil, mysqlError(1363, "HY000", "There is no OLD row in on INSERT trigger")
			}
		}
	}

	trigDef := &catalog.TriggerDef{
		Name:   triggerName,
		Timing: timing,
		Event:  event,
		Table:  tableName,
		Body:   bodyStatements,
	}
	db.CreateTrigger(trigDef)

	return &Result{}, nil
}

// splitTriggerBody splits the body of a trigger/procedure into individual SQL statements.
func splitTriggerBody(body string) []string {
	var stmts []string
	var current strings.Builder
	inSingle := false
	inDouble := false
	depth := 0 // track nested BEGIN...END

	words := body
	i := 0
	for i < len(words) {
		ch := words[i]
		switch {
		case ch == '\'' && !inDouble:
			inSingle = !inSingle
			current.WriteByte(ch)
		case ch == '"' && !inSingle:
			inDouble = !inDouble
			current.WriteByte(ch)
		case ch == ';' && !inSingle && !inDouble && depth == 0:
			stmt := strings.TrimSpace(current.String())
			if stmt != "" {
				stmts = append(stmts, stmt)
			}
			current.Reset()
		default:
			// Track nested BEGIN...END for IF/WHILE blocks
			if !inSingle && !inDouble {
				remaining := strings.ToUpper(words[i:])
				if strings.HasPrefix(remaining, "BEGIN") && (i+5 >= len(words) || !isAlphaNum(words[i+5])) {
					depth++
				}
				if strings.HasPrefix(remaining, "END") && (i+3 >= len(words) || !isAlphaNum(words[i+3])) && depth > 0 {
					depth--
				}
			}
			current.WriteByte(ch)
		}
		i++
	}
	rest := strings.TrimSpace(current.String())
	if rest != "" {
		stmts = append(stmts, rest)
	}
	return stmts
}

func isAlphaNum(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
}

// dropTriggersForTable removes all triggers associated with the given table.
func (e *Executor) dropTriggersForTable(db *catalog.Database, tableName string) {
	if db.Triggers == nil {
		return
	}
	var toRemove []string
	for name, tr := range db.Triggers {
		if strings.EqualFold(tr.Table, tableName) {
			toRemove = append(toRemove, name)
		}
	}
	for _, name := range toRemove {
		db.DropTrigger(name)
	}
}

// execDropTrigger handles DROP TRIGGER [IF EXISTS] name
func (e *Executor) execDropTrigger(query string) (*Result, error) {
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", e.CurrentDB))
	}

	upper := strings.ToUpper(strings.TrimSpace(query))
	rest := strings.TrimSpace(query[len("DROP TRIGGER"):])

	ifExists := false
	if strings.HasPrefix(strings.ToUpper(rest), "IF EXISTS") {
		ifExists = true
		rest = strings.TrimSpace(rest[len("IF EXISTS"):])
	}
	name := strings.TrimRight(strings.TrimSpace(rest), ";")
	name = strings.Trim(name, "`")
	_ = upper

	if _, ok := db.Triggers[name]; !ok && !ifExists {
		return nil, mysqlError(1360, "HY000", fmt.Sprintf("Trigger does not exist"))
	}
	db.DropTrigger(name)
	return &Result{}, nil
}

// fireTriggers executes all triggers matching the given timing and event for the specified table.
// The newRow and oldRow maps provide NEW and OLD pseudo-record values.
// For BEFORE triggers, SET NEW.col = val modifies newRow in place.
func (e *Executor) fireTriggers(tableName, timing, event string, newRow, oldRow storage.Row) error {
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return err
	}

	triggers := db.GetTriggersForTable(tableName, timing, event)
	for _, tr := range triggers {
		for _, stmtStr := range tr.Body {
			stmtUpper := strings.ToUpper(strings.TrimSpace(stmtStr))
			// Handle SET NEW.col = value in BEFORE triggers
			if strings.HasPrefix(stmtUpper, "SET NEW.") && timing == "BEFORE" && newRow != nil {
				e.handleSetNew(stmtStr, newRow, oldRow)
				continue
			}
			// Substitute NEW.col and OLD.col references
			resolved := e.resolveNewOldRefs(stmtStr, newRow, oldRow)
			_, err := e.Execute(resolved)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// handleSetNew processes "SET NEW.col = expr" statements in BEFORE triggers.
func (e *Executor) handleSetNew(stmtStr string, newRow, oldRow storage.Row) {
	// Parse: SET NEW.col = expr
	rest := strings.TrimSpace(stmtStr[len("SET "):])
	eqIdx := strings.Index(rest, "=")
	if eqIdx < 0 {
		return
	}
	colRef := strings.TrimSpace(rest[:eqIdx])
	valExpr := strings.TrimSpace(rest[eqIdx+1:])
	valExpr = strings.TrimRight(valExpr, ";")

	// Extract column name from NEW.col
	if !strings.HasPrefix(strings.ToUpper(colRef), "NEW.") {
		return
	}
	colName := colRef[4:] // strip "NEW."

	// Resolve OLD/NEW references in the value expression
	resolved := e.resolveNewOldRefs(valExpr, newRow, oldRow)

	// Try to parse and evaluate the value expression
	val, err := e.evaluateSimpleExpr(resolved)
	if err != nil {
		return
	}
	newRow[colName] = val
}

// evaluateSimpleExpr evaluates a simple expression string (used for trigger SET NEW.col = expr).
func (e *Executor) evaluateSimpleExpr(expr string) (interface{}, error) {
	// Try to parse as a SELECT expression to use the full evaluator
	selectSQL := "SELECT " + expr
	stmt, err := sqlparser.NewTestParser().Parse(selectSQL)
	if err != nil {
		// Fallback: treat as literal
		return expr, nil
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok || len(sel.SelectExprs.Exprs) == 0 {
		return expr, nil
	}
	ae, ok := sel.SelectExprs.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return expr, nil
	}
	return e.evalExpr(ae.Expr)
}

// resolveNewOldRefs replaces NEW.col and OLD.col references in a SQL statement
// with the actual values from the row.
func (e *Executor) resolveNewOldRefs(stmtStr string, newRow, oldRow storage.Row) string {
	// Replace NEW.col and OLD.col with actual values
	result := stmtStr

	// Process NEW.xxx references
	if newRow != nil {
		result = replaceRowRefs(result, "NEW", newRow)
	}
	// Process OLD.xxx references
	if oldRow != nil {
		result = replaceRowRefs(result, "OLD", oldRow)
	}
	return result
}

// replaceRowRefs replaces prefix.col references (e.g. NEW.c1) with actual values.
func replaceRowRefs(stmt, prefix string, row storage.Row) string {
	// Find all occurrences of PREFIX.identifier (case-insensitive prefix)
	result := stmt
	prefixUpper := strings.ToUpper(prefix)
	i := 0
	for i < len(result) {
		// Look for prefix followed by dot
		remaining := result[i:]
		remainingUpper := strings.ToUpper(remaining)
		if !strings.HasPrefix(remainingUpper, prefixUpper+".") {
			i++
			continue
		}
		// Check word boundary before prefix
		if i > 0 && isAlphaNum(result[i-1]) {
			i++
			continue
		}
		// Extract column name after the dot
		dotPos := i + len(prefix) + 1
		end := dotPos
		for end < len(result) && (isAlphaNum(result[end]) || result[end] == '_') {
			end++
		}
		if end == dotPos {
			i++
			continue
		}
		colName := result[dotPos:end]

		// Look up value in row (case-insensitive)
		var val interface{}
		found := false
		for k, v := range row {
			if strings.EqualFold(k, colName) {
				val = v
				found = true
				break
			}
		}

		var replacement string
		if !found || val == nil {
			replacement = "NULL"
		} else {
			switch v := val.(type) {
			case string:
				replacement = "'" + strings.ReplaceAll(v, "'", "''") + "'"
			default:
				replacement = fmt.Sprintf("%v", v)
			}
		}
		result = result[:i] + replacement + result[end:]
		i += len(replacement)
	}
	return result
}

// ==============================================================================
// Stored Procedure support
// ==============================================================================

// execCreateProcedure parses and stores a CREATE PROCEDURE statement with BEGIN...END body.
func (e *Executor) execCreateProcedure(query string) (*Result, error) {
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", e.CurrentDB))
	}

	// Parse: CREATE PROCEDURE name (params) [characteristics] BEGIN ... END
	upper := strings.ToUpper(query)
	rest := strings.TrimSpace(query[len("CREATE PROCEDURE "):])

	// Extract procedure name (up to first '(')
	parenIdx := strings.Index(rest, "(")
	if parenIdx < 0 {
		return nil, fmt.Errorf("invalid CREATE PROCEDURE syntax: missing parameter list")
	}
	procName := strings.TrimSpace(rest[:parenIdx])
	procName = strings.Trim(procName, "`")

	// Extract params between first '(' and matching ')'
	paramStart := parenIdx + 1
	depth := 1
	paramEnd := paramStart
	for paramEnd < len(rest) && depth > 0 {
		if rest[paramEnd] == '(' {
			depth++
		} else if rest[paramEnd] == ')' {
			depth--
		}
		if depth > 0 {
			paramEnd++
		}
	}
	paramStr := strings.TrimSpace(rest[paramStart:paramEnd])
	params := parseProcParams(paramStr)

	// Extract body: find BEGIN...END
	_ = upper
	afterParams := rest[paramEnd+1:]
	beginIdx := strings.Index(strings.ToUpper(afterParams), "BEGIN")
	if beginIdx < 0 {
		return nil, fmt.Errorf("invalid CREATE PROCEDURE syntax: missing BEGIN")
	}
	bodyStr := strings.TrimSpace(afterParams[beginIdx+len("BEGIN"):])
	if strings.HasSuffix(strings.ToUpper(strings.TrimSpace(bodyStr)), "END") {
		bodyStr = strings.TrimSpace(bodyStr[:len(bodyStr)-len("END")])
	}

	bodyStmts := splitTriggerBody(bodyStr)

	procDef := &catalog.ProcedureDef{
		Name:   procName,
		Params: params,
		Body:   bodyStmts,
	}
	db.CreateProcedure(procDef)

	return &Result{}, nil
}

// parseProcParams parses a procedure parameter list string.
func parseProcParams(paramStr string) []catalog.ProcParam {
	if strings.TrimSpace(paramStr) == "" {
		return nil
	}
	var params []catalog.ProcParam
	// Split by commas (not inside parens)
	parts := splitByComma(paramStr)
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		words := strings.Fields(p)
		param := catalog.ProcParam{}
		idx := 0
		// Check for IN/OUT/INOUT prefix
		if len(words) > 0 {
			modeUpper := strings.ToUpper(words[0])
			if modeUpper == "IN" || modeUpper == "OUT" || modeUpper == "INOUT" {
				param.Mode = modeUpper
				idx = 1
			} else {
				param.Mode = "IN" // default
			}
		}
		if idx < len(words) {
			param.Name = words[idx]
			idx++
		}
		if idx < len(words) {
			param.Type = strings.Join(words[idx:], " ")
		}
		params = append(params, param)
	}
	return params
}

// splitByComma splits a string by commas, respecting parentheses.
func splitByComma(s string) []string {
	var parts []string
	var current strings.Builder
	depth := 0
	for _, ch := range s {
		switch ch {
		case '(':
			depth++
			current.WriteRune(ch)
		case ')':
			depth--
			current.WriteRune(ch)
		case ',':
			if depth == 0 {
				parts = append(parts, current.String())
				current.Reset()
			} else {
				current.WriteRune(ch)
			}
		default:
			current.WriteRune(ch)
		}
	}
	rest := current.String()
	if strings.TrimSpace(rest) != "" {
		parts = append(parts, rest)
	}
	return parts
}

// execDropProcedureFallback handles DROP PROCEDURE [IF EXISTS] name
func (e *Executor) execDropProcedureFallback(query string) (*Result, error) {
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", e.CurrentDB))
	}

	rest := strings.TrimSpace(query[len("DROP PROCEDURE"):])
	ifExists := false
	restUpper := strings.ToUpper(rest)
	if strings.HasPrefix(restUpper, "IF EXISTS") {
		ifExists = true
		rest = strings.TrimSpace(rest[len("IF EXISTS"):])
	}
	name := strings.TrimRight(strings.TrimSpace(rest), ";")
	name = strings.Trim(name, "`")

	if db.GetProcedure(name) == nil && !ifExists {
		return nil, mysqlError(1305, "42000", fmt.Sprintf("PROCEDURE %s.%s does not exist", e.CurrentDB, name))
	}
	db.DropProcedure(name)
	return &Result{}, nil
}

// execDropProcedureAST handles DROP PROCEDURE parsed by vitess.
func (e *Executor) execDropProcedureAST(stmt *sqlparser.DropProcedure) (*Result, error) {
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", e.CurrentDB))
	}
	name := stmt.Name.Name.String()
	name = strings.Trim(name, "`")
	if db.GetProcedure(name) == nil && !stmt.IfExists {
		return nil, mysqlError(1305, "42000", fmt.Sprintf("PROCEDURE %s.%s does not exist", e.CurrentDB, name))
	}
	db.DropProcedure(name)
	return &Result{}, nil
}

// execCallProcedure handles CALL procedure_name(args) from text.
func (e *Executor) execCallProcedure(query string) (*Result, error) {
	// Parse: CALL proc_name(arg1, arg2, ...)
	rest := strings.TrimSpace(query[len("CALL "):])
	rest = strings.TrimRight(rest, ";")

	// Extract procedure name and args
	parenIdx := strings.Index(rest, "(")
	var procName string
	var argStrs []string
	if parenIdx < 0 {
		procName = strings.TrimSpace(rest)
	} else {
		procName = strings.TrimSpace(rest[:parenIdx])
		argPart := rest[parenIdx+1:]
		if closeParen := strings.LastIndex(argPart, ")"); closeParen >= 0 {
			argPart = argPart[:closeParen]
		}
		argStrs = splitByComma(argPart)
	}
	procName = strings.Trim(procName, "`")

	// Handle well-known no-op procedures (e.g. mtr.add_suppression)
	if strings.Contains(procName, ".") {
		return &Result{}, nil
	}

	return e.callProcedureByName(procName, argStrs)
}

// execCallProcFromAST handles CALL parsed by vitess.
func (e *Executor) execCallProcFromAST(stmt *sqlparser.CallProc) (*Result, error) {
	procName := stmt.Name.Name.String()
	procName = strings.Trim(procName, "`")

	// Handle well-known no-op procedures
	qualifier := stmt.Name.Qualifier.String()
	if qualifier != "" {
		return &Result{}, nil
	}

	var argStrs []string
	for _, arg := range stmt.Params {
		argStrs = append(argStrs, sqlparser.String(arg))
	}

	return e.callProcedureByName(procName, argStrs)
}

// callProcedureByName looks up and executes a stored procedure.
func (e *Executor) callProcedureByName(procName string, argStrs []string) (*Result, error) {
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", e.CurrentDB))
	}

	proc := db.GetProcedure(procName)
	if proc == nil {
		// Silently accept calls to non-existent procedures for compatibility
		return &Result{}, nil
	}

	// Build parameter mapping: bind IN params, track OUT params
	paramVars := make(map[string]interface{})
	outVarMap := make(map[string]string) // param name -> @variable name
	for i, param := range proc.Params {
		if i < len(argStrs) {
			argVal := strings.TrimSpace(argStrs[i])
			if strings.HasPrefix(argVal, "@") {
				// User variable reference
				if param.Mode == "IN" || param.Mode == "INOUT" {
					// Read current value of user variable (for now, use as string)
					paramVars[param.Name] = argVal
				}
				if param.Mode == "OUT" || param.Mode == "INOUT" {
					outVarMap[param.Name] = argVal
				}
			} else {
				// Literal value
				paramVars[param.Name] = argVal
			}
		}
	}

	// Execute body statements
	for _, stmtStr := range proc.Body {
		stmtUpper := strings.ToUpper(strings.TrimSpace(stmtStr))
		// Handle SELECT ... INTO
		if strings.Contains(stmtUpper, " INTO ") && strings.HasPrefix(stmtUpper, "SELECT") {
			err := e.execSelectInto(stmtStr, paramVars, outVarMap)
			if err != nil {
				return nil, err
			}
			continue
		}
		// Handle DECLARE - skip
		if strings.HasPrefix(stmtUpper, "DECLARE") {
			continue
		}
		// Handle SET
		if strings.HasPrefix(stmtUpper, "SET") {
			_, err := e.Execute(stmtStr)
			if err != nil {
				return nil, err
			}
			continue
		}
		// Execute other statements
		_, err := e.Execute(stmtStr)
		if err != nil {
			return nil, err
		}
	}

	return &Result{}, nil
}

// execSelectInto handles SELECT ... INTO variable inside a stored procedure.
func (e *Executor) execSelectInto(stmtStr string, paramVars map[string]interface{}, outVarMap map[string]string) error {
	// Parse: SELECT expr INTO varname FROM ...
	// We need to extract the INTO clause and rewrite the SELECT without it
	upper := strings.ToUpper(stmtStr)
	intoIdx := strings.Index(upper, " INTO ")
	if intoIdx < 0 {
		return nil
	}

	// Find what comes after INTO: variable name, then FROM/WHERE/etc.
	afterInto := stmtStr[intoIdx+len(" INTO "):]
	// The variable name ends at the next keyword (FROM, WHERE, etc.) or end of string
	var varName string
	var restOfQuery string
	for _, kw := range []string{" FROM ", " WHERE ", " GROUP ", " ORDER ", " LIMIT ", " HAVING "} {
		kwIdx := strings.Index(strings.ToUpper(afterInto), kw)
		if kwIdx >= 0 {
			varName = strings.TrimSpace(afterInto[:kwIdx])
			restOfQuery = afterInto[kwIdx:]
			break
		}
	}
	if varName == "" {
		varName = strings.TrimSpace(afterInto)
	}

	// Build a SELECT without the INTO clause
	selectPart := stmtStr[:intoIdx]
	rewrittenSQL := selectPart + restOfQuery

	result, err := e.Execute(rewrittenSQL)
	if err != nil {
		return err
	}

	// Assign result to the output variable
	if result != nil && len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
		val := result.Rows[0][0]
		// If varName is a parameter name, look up the OUT variable
		if outVar, ok := outVarMap[varName]; ok {
			_ = outVar
			_ = val
			// For now, we just store it (user variables @xxx are not fully implemented)
		}
		paramVars[varName] = val
	}

	return nil
}

// truncateNear truncates a SQL string for error messages (MySQL shows ~80 chars).
func truncateNear(s string) string {
	if len(s) > 80 {
		return s[:80]
	}
	return s
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
		whereUpper := strings.ToUpper(afterUsing)
		if whereIdx := strings.Index(whereUpper, " WHERE "); whereIdx >= 0 {
			whereClause = strings.TrimSpace(afterUsing[whereIdx+len(" WHERE "):])
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
		whereUpper := strings.ToUpper(afterFrom)
		if whereIdx := strings.Index(whereUpper, " WHERE "); whereIdx >= 0 {
			whereClause = strings.TrimSpace(afterFrom[whereIdx+len(" WHERE "):])
			whereClause = strings.TrimSuffix(whereClause, ";")
			fromTablesStr = strings.TrimSpace(afterFrom[:whereIdx])
		} else {
			fromTablesStr = strings.TrimSuffix(strings.TrimSpace(afterFrom), ";")
		}
	}

	// Resolve qualified target names (db.table -> use that db for lookup)
	// For now just extract the table name part
	for i, t := range deleteTargets {
		if parts := strings.Split(t, "."); len(parts) > 1 {
			deleteTargets[i] = parts[len(parts)-1] // take last part as table name
		}
	}

	// Parse table refs
	type tableRef struct {
		name  string
		alias string
		db    string
	}
	var tableRefs []tableRef
	for _, t := range strings.Split(fromTablesStr, ",") {
		t = strings.TrimSpace(t)
		t = strings.Trim(t, ";")
		parts := strings.Fields(t)
		if len(parts) == 0 {
			continue
		}
		name := strings.Trim(parts[0], "`")
		alias := name
		db := e.CurrentDB
		// Handle db.table qualified names
		if dotParts := strings.Split(name, "."); len(dotParts) == 2 {
			db = dotParts[0]
			name = dotParts[1]
			alias = name
		}
		if len(parts) >= 3 && strings.ToUpper(parts[1]) == "AS" {
			alias = strings.Trim(parts[2], "`")
		} else if len(parts) >= 2 && strings.ToUpper(parts[1]) != "AS" {
			alias = strings.Trim(parts[1], "`")
		}
		tableRefs = append(tableRefs, tableRef{name: name, alias: alias, db: db})
	}

	if len(tableRefs) == 0 {
		return &Result{}, nil
	}

	// Build cross-product of all table rows
	allRows, err := e.getTableRowsWithAliasDB(tableRefs[0].db, tableRefs[0].name, tableRefs[0].alias)
	if err != nil {
		return nil, err
	}
	for i := 1; i < len(tableRefs); i++ {
		tRows, err := e.getTableRowsWithAliasDB(tableRefs[i].db, tableRefs[i].name, tableRefs[i].alias)
		if err != nil {
			return nil, err
		}
		allRows = crossProduct(allRows, tRows)
	}

	// Apply WHERE filter
	if whereClause != "" {
		// Build a SELECT statement to parse the WHERE clause
		// Use the first table as a dummy FROM to help vitess parse qualified column refs
		selectSQL := "SELECT 1 FROM dual WHERE " + whereClause
		parsedStmt, err := sqlparser.NewTestParser().Parse(selectSQL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse WHERE clause: %v", err)
		}
		sel, ok := parsedStmt.(*sqlparser.Select)
		if !ok || sel.Where == nil {
			return nil, fmt.Errorf("failed to parse WHERE clause")
		}
		filtered := make([]storage.Row, 0)
		for _, row := range allRows {
			match, err := e.evalWhere(sel.Where.Expr, row)
			if err != nil {
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
	for _, target := range deleteTargets {
		// Find the matching table ref (to get the right db)
		targetDB := e.CurrentDB
		for _, ref := range tableRefs {
			if ref.name == target || ref.alias == target {
				targetDB = ref.db
				break
			}
		}
		tbl, err := e.Storage.GetTable(targetDB, target)
		if err != nil {
			continue
		}
		deleteIndices := make(map[int]bool)
		for _, matchedRow := range allRows {
			for i, existingRow := range tbl.Rows {
				if deleteIndices[i] {
					continue
				}
				allMatch := true
				for _, col := range tbl.Def.Columns {
					mv, ok := matchedRow[target+"."+col.Name]
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
					deleteIndices[i] = true
				}
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

func crossProduct(left, right []storage.Row) []storage.Row {
	var result []storage.Row
	for _, l := range left {
		for _, r := range right {
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

// execCreateTableLike handles CREATE TABLE t2 LIKE t1.
func (e *Executor) execCreateTableLike(newTableName, srcTableName string) (*Result, error) {
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", e.CurrentDB))
	}
	srcDef, err := db.GetTable(srcTableName)
	if err != nil {
		return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", e.CurrentDB, srcTableName))
	}
	newCols := make([]catalog.ColumnDef, len(srcDef.Columns))
	copy(newCols, srcDef.Columns)
	newIndexes := make([]catalog.IndexDef, len(srcDef.Indexes))
	copy(newIndexes, srcDef.Indexes)
	var newPK []string
	if srcDef.PrimaryKey != nil {
		newPK = make([]string, len(srcDef.PrimaryKey))
		copy(newPK, srcDef.PrimaryKey)
	}
	newDef := &catalog.TableDef{
		Name:       newTableName,
		Columns:    newCols,
		PrimaryKey: newPK,
		Indexes:    newIndexes,
	}
	if err := db.CreateTable(newDef); err != nil {
		return nil, mysqlError(1050, "42S01", fmt.Sprintf("Table '%s' already exists", newTableName))
	}
	e.Storage.CreateTable(e.CurrentDB, newDef)
	return &Result{}, nil
}

// execCreateTableSelect handles CREATE TABLE t2 [AS] SELECT ...
func (e *Executor) execCreateTableSelect(newTableName, selectSQL string) (*Result, error) {
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", e.CurrentDB))
	}
	result, err := e.Execute(selectSQL)
	if err != nil {
		return nil, err
	}
	var cols []catalog.ColumnDef
	for _, colName := range result.Columns {
		cols = append(cols, catalog.ColumnDef{
			Name:     colName,
			Type:     "text",
			Nullable: true,
		})
	}
	newDef := &catalog.TableDef{
		Name:    newTableName,
		Columns: cols,
	}
	if err := db.CreateTable(newDef); err != nil {
		return nil, mysqlError(1050, "42S01", fmt.Sprintf("Table '%s' already exists", newTableName))
	}
	e.Storage.CreateTable(e.CurrentDB, newDef)
	tbl, _ := e.Storage.GetTable(e.CurrentDB, newTableName)
	for _, row := range result.Rows {
		sRow := make(storage.Row)
		for i, colName := range result.Columns {
			if i < len(row) {
				sRow[colName] = row[i]
			}
		}
		tbl.Insert(sRow) //nolint:errcheck
	}
	return &Result{}, nil
}
