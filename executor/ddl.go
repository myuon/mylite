package executor

import (
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

// isMySQLLogTable returns true if the given database and table name refer to
// one of the MySQL log tables (general_log or slow_log).
func isMySQLLogTable(dbName, tableName string) bool {
	if !strings.EqualFold(dbName, "mysql") {
		return false
	}
	lower := strings.ToLower(tableName)
	return lower == "general_log" || lower == "slow_log"
}

// isLogTableLoggingEnabled checks whether logging is currently enabled for the
// given MySQL log table. general_log is protected when general_log='ON',
// slow_log is protected when slow_query_log='ON'.
func (e *Executor) isLogTableLoggingEnabled(tableName string) bool {
	lower := strings.ToLower(tableName)
	switch lower {
	case "general_log":
		if v, ok := e.getGlobalVar("general_log"); ok {
			return strings.EqualFold(v, "ON") || v == "1"
		}
		return true // default is ON
	case "slow_log":
		if v, ok := e.getGlobalVar("slow_query_log"); ok {
			return strings.EqualFold(v, "ON") || v == "1"
		}
		return true // default is ON
	}
	return false
}

func (e *Executor) execRenameTable(stmt *sqlparser.RenameTable) (*Result, error) {
	// RENAME TABLE is atomic: validate all pairs before executing any rename.
	// We simulate the rename chain to validate that each source exists and each
	// target doesn't conflict, accounting for intermediate renames.
	// "effective" tracks which (db, name) keys exist after simulated renames so far.
	type tableKey struct{ db, name string }
	// Start with "unknown" — we'll query the catalog on demand and track additions/removals.
	removed := make(map[tableKey]bool)
	added := make(map[tableKey]bool)

	tableExists := func(db, name string) bool {
		k := tableKey{db, name}
		if removed[k] {
			return false
		}
		if added[k] {
			return true
		}
		catDB, err := e.Catalog.GetDatabase(db)
		if err != nil {
			return false
		}
		_, err = catDB.GetTable(name)
		return err == nil
	}

	type renamePair struct {
		srcDB, oldName, targetDB, newName string
	}
	pairs := make([]renamePair, 0, len(stmt.TablePairs))

	for _, pair := range stmt.TablePairs {
		oldName := pair.FromTable.Name.String()
		newName := pair.ToTable.Name.String()
		srcDB := e.CurrentDB
		if !pair.FromTable.Qualifier.IsEmpty() {
			srcDB = pair.FromTable.Qualifier.String()
		}
		targetDB := e.CurrentDB
		if !pair.ToTable.Qualifier.IsEmpty() {
			targetDB = pair.ToTable.Qualifier.String()
		}

		// Validate databases exist
		if _, err := e.Catalog.GetDatabase(targetDB); err != nil {
			return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", targetDB))
		}
		if _, err := e.Catalog.GetDatabase(srcDB); err != nil {
			return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", srcDB))
		}

		// Validate target doesn't exist first (MySQL checks destination conflicts before source existence)
		if tableExists(targetDB, newName) {
			return nil, mysqlError(1050, "42S01", fmt.Sprintf("Table '%s' already exists", newName))
		}
		// Validate source exists (considering prior simulated renames)
		if !tableExists(srcDB, oldName) {
			return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", srcDB, oldName))
		}

		// Simulate this rename for subsequent validations
		removed[tableKey{srcDB, oldName}] = true
		added[tableKey{targetDB, newName}] = true
		// If we had previously "added" the source, remove it from added too
		if added[tableKey{srcDB, oldName}] {
			delete(added, tableKey{srcDB, oldName})
		}

		pairs = append(pairs, renamePair{srcDB, oldName, targetDB, newName})
	}

	// All validations passed; execute all renames.
	for _, p := range pairs {
		srcCatDB, _ := e.Catalog.GetDatabase(p.srcDB)
		targetCatDB, _ := e.Catalog.GetDatabase(p.targetDB)

		def, err := srcCatDB.GetTable(p.oldName)
		if err != nil {
			continue // shouldn't happen after validation
		}
		// Rename in catalog
		def.Name = p.newName
		srcCatDB.DropTable(p.oldName)    //nolint:errcheck
		targetCatDB.CreateTable(def)      //nolint:errcheck
		// Rename in storage
		if tbl, err := e.Storage.GetTable(p.srcDB, p.oldName); err == nil {
			tbl.Def = def
			e.Storage.CreateTable(p.targetDB, def)
			// Copy rows
			if newTbl, err := e.Storage.GetTable(p.targetDB, p.newName); err == nil {
				newTbl.Rows = tbl.Rows
				newTbl.AutoIncrement.Store(tbl.AutoIncrementValue())
			}
			e.Storage.DropTable(p.srcDB, p.oldName)
		}
		e.removeInnoDBStatsRows(p.srcDB, p.oldName)
		e.upsertInnoDBStatsRows(p.targetDB, p.newName, e.tableRowCount(p.targetDB, p.newName))
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
	// Extract charset and collation from CREATE DATABASE options
	charset := ""
	collation := ""
	for _, opt := range stmt.CreateOptions {
		switch opt.Type {
		case sqlparser.CharacterSetType:
			charset = strings.ToLower(strings.Trim(opt.Value, "'\""))
		case sqlparser.CollateType:
			collation = strings.ToLower(strings.Trim(opt.Value, "'\""))
		}
	}
	// If no explicit charset, use the session-level character_set_server
	if charset == "" {
		if csVal, ok := e.getSysVarSession("character_set_server"); ok {
			charset = csVal
		}
		// Fall through to catalog default (utf8mb4) if session value not set
	}
	// Validate charset name
	if charset != "" && !isKnownCharset(charset) {
		return nil, mysqlError(1115, "42000", fmt.Sprintf("Unknown character set: '%s'", charset))
	}
	// Validate collation name
	if collation != "" && !isKnownCollation(collation) {
		return nil, mysqlError(1273, "HY000", fmt.Sprintf("Unknown collation: '%s'", collation))
	}
	// Validate collation is compatible with charset (ER_COLLATION_CHARSET_MISMATCH = 1253)
	if charset != "" && collation != "" {
		if collCharset, ok := catalog.CharsetForCollation(collation); ok {
			if !strings.EqualFold(collCharset, charset) {
				return nil, mysqlError(1253, "42000", fmt.Sprintf("COLLATION '%s' is not valid for CHARACTER SET '%s'", collation, charset))
			}
		}
	}
	err := e.Catalog.CreateDatabaseWithCharset(name, charset, collation)
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

// execAlterDatabase handles ALTER DATABASE ... CHARACTER SET / COLLATE.
func (e *Executor) execAlterDatabase(stmt *sqlparser.AlterDatabase) (*Result, error) {
	name := stmt.DBName.String()
	if name == "" {
		name = e.CurrentDB
	}
	db, err := e.Catalog.GetDatabase(name)
	if err != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", name))
	}
	for _, opt := range stmt.AlterOptions {
		switch opt.Type {
		case sqlparser.CharacterSetType:
			csVal := strings.ToLower(strings.Trim(opt.Value, "'\""))
			if !isKnownCharset(csVal) {
				return nil, mysqlError(1115, "42000", fmt.Sprintf("Unknown character set: '%s'", csVal))
			}
			db.CharacterSet = csVal
			// Update collation to default for the new charset
			db.CollationName = catalog.DefaultCollationForCharset(csVal)
		case sqlparser.CollateType:
			collVal := strings.ToLower(strings.Trim(opt.Value, "'\""))
			if !isKnownCollation(collVal) {
				return nil, mysqlError(1273, "HY000", fmt.Sprintf("Unknown collation: '%s'", collVal))
			}
			db.CollationName = collVal
			// Derive charset from collation name (e.g. "utf8_general_ci" -> "utf8")
			parts := strings.SplitN(collVal, "_", 2)
			if len(parts) > 0 {
				db.CharacterSet = parts[0]
			}
		}
	}
	return &Result{}, nil
}

// execCreateDatabaseRaw handles CREATE DATABASE/SCHEMA ... CHARACTER SET from raw SQL
// when the vitess parser doesn't correctly extract the charset.
func (e *Executor) execCreateDatabaseRaw(query string) (*Result, error) {
	upper := strings.ToUpper(strings.TrimSpace(query))
	rest := ""
	if strings.HasPrefix(upper, "CREATE DATABASE ") {
		rest = strings.TrimSpace(query[len("CREATE DATABASE "):])
	} else if strings.HasPrefix(upper, "CREATE SCHEMA ") {
		rest = strings.TrimSpace(query[len("CREATE SCHEMA "):])
	}
	// Handle IF NOT EXISTS
	ifNotExists := false
	restUpper := strings.ToUpper(rest)
	if strings.HasPrefix(restUpper, "IF NOT EXISTS ") {
		ifNotExists = true
		rest = strings.TrimSpace(rest[len("IF NOT EXISTS "):])
		restUpper = strings.ToUpper(rest)
	}
	// Extract database name (first token)
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return &Result{}, nil
	}
	dbName := strings.Trim(fields[0], "`")

	// Reject system schemas
	sysSchemas := map[string]bool{
		"mysql": true, "information_schema": true, "performance_schema": true, "sys": true,
	}
	if sysSchemas[strings.ToLower(dbName)] {
		return nil, mysqlError(3802, "HY000", fmt.Sprintf("Access to system schema '%s' is rejected.", dbName))
	}

	// Extract CHARACTER SET and COLLATE from rest
	charset := ""
	collation := ""
	fullUpper := strings.ToUpper(strings.Join(fields[1:], " "))
	csIdx := strings.Index(fullUpper, "CHARACTER SET ")
	if csIdx >= 0 {
		afterCS := strings.TrimSpace(fullUpper[csIdx+len("CHARACTER SET "):])
		// Skip optional '=' after CHARACTER SET
		afterCS = strings.TrimPrefix(afterCS, "= ")
		afterCS = strings.TrimPrefix(afterCS, "=")
		afterCS = strings.TrimSpace(afterCS)
		csFields := strings.Fields(afterCS)
		if len(csFields) > 0 {
			charset = strings.ToLower(strings.Trim(csFields[0], "'\""))
		}
	}
	// Also check in original (non-uppercased) for COLLATE value quoting
	fullOrig := strings.Join(fields[1:], " ")
	collIdx := strings.Index(fullUpper, "COLLATE ")
	if collIdx >= 0 {
		afterColl := strings.TrimSpace(fullUpper[collIdx+len("COLLATE "):])
		// Skip optional '=' after COLLATE
		afterColl = strings.TrimPrefix(afterColl, "= ")
		afterColl = strings.TrimPrefix(afterColl, "=")
		afterColl = strings.TrimSpace(afterColl)
		collFields := strings.Fields(afterColl)
		if len(collFields) > 0 {
			collVal := collFields[0]
			// Check if the collation value is unquoted in the original query.
			// In MySQL, certain reserved keywords (like 'binary') must be quoted when used as collation names.
			origAfterColl := strings.TrimSpace(fullOrig[collIdx+len("COLLATE "):])
			origAfterColl = strings.TrimPrefix(origAfterColl, "= ")
			origAfterColl = strings.TrimPrefix(origAfterColl, "=")
			origAfterColl = strings.TrimSpace(origAfterColl)
			if len(origAfterColl) > 0 {
				firstChar := origAfterColl[0]
				if firstChar != '\'' && firstChar != '"' && firstChar != '`' {
					// Unquoted collation name — check if it's a reserved keyword
					reservedKeywords := map[string]bool{
						"binary": true,
					}
					if reservedKeywords[strings.ToLower(strings.Fields(origAfterColl)[0])] {
						return nil, mysqlError(1064, "42000", fmt.Sprintf("You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'binary' at line 1"))
					}
				}
			}
			collation = strings.ToLower(strings.Trim(collVal, "'\""))
		}
	}

	// Validate character set name
	if charset != "" && !isKnownCharset(charset) {
		return nil, mysqlError(1115, "42000", fmt.Sprintf("Unknown character set: '%s'", charset))
	}
	// Validate collation name
	if collation != "" && !isKnownCollation(collation) {
		return nil, mysqlError(1273, "HY000", fmt.Sprintf("Unknown collation: '%s'", collation))
	}
	// Validate collation is compatible with charset (ER_COLLATION_CHARSET_MISMATCH = 1253)
	if charset != "" && collation != "" {
		if collCharset, ok := catalog.CharsetForCollation(collation); ok {
			if !strings.EqualFold(collCharset, charset) {
				return nil, mysqlError(1253, "42000", fmt.Sprintf("COLLATION '%s' is not valid for CHARACTER SET '%s'", collation, charset))
			}
		}
	}

	err := e.Catalog.CreateDatabaseWithCharset(dbName, charset, collation)
	if err != nil {
		if ifNotExists {
			return &Result{}, nil
		}
		return nil, mysqlError(1007, "HY000", fmt.Sprintf("Can't create database '%s'; database exists", dbName))
	}
	e.Storage.EnsureDatabase(dbName)
	return &Result{AffectedRows: 1}, nil
}

// execAlterDatabaseRaw handles ALTER DATABASE/SCHEMA ... CHARACTER SET/COLLATE from raw SQL.
// This is needed because the vitess parser doesn't handle CHARACTER SET in ALTER DATABASE.
func (e *Executor) execAlterDatabaseRaw(query string) (*Result, error) {
	upper := strings.ToUpper(strings.TrimSpace(query))
	// Extract database name: ALTER DATABASE <name> or ALTER SCHEMA <name>
	rest := ""
	if strings.HasPrefix(upper, "ALTER DATABASE ") {
		rest = strings.TrimSpace(query[len("ALTER DATABASE "):])
	} else if strings.HasPrefix(upper, "ALTER SCHEMA ") {
		rest = strings.TrimSpace(query[len("ALTER SCHEMA "):])
	}
	// Parse: <dbname> [DEFAULT] CHARACTER SET <charset> [COLLATE <collation>]
	// or: <dbname> [DEFAULT] COLLATE <collation>
	fields := strings.Fields(rest)
	if len(fields) < 3 {
		return &Result{}, nil
	}
	dbName := strings.Trim(fields[0], "`")
	// ALTER DATABASE DEFAULT CHARACTER SET ... means alter the current database
	if strings.ToUpper(dbName) == "DEFAULT" {
		dbName = e.CurrentDB
	}
	db, err := e.Catalog.GetDatabase(dbName)
	if err != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", dbName))
	}

	restUpper := strings.ToUpper(strings.Join(fields[1:], " "))
	// Extract CHARACTER SET
	csIdx := strings.Index(restUpper, "CHARACTER SET ")
	if csIdx >= 0 {
		afterCS := strings.TrimSpace(restUpper[csIdx+len("CHARACTER SET "):])
		afterCS = strings.TrimPrefix(afterCS, "= ")
		afterCS = strings.TrimPrefix(afterCS, "=")
		afterCS = strings.TrimSpace(afterCS)
		csFields := strings.Fields(afterCS)
		if len(csFields) > 0 {
			charset := strings.ToLower(csFields[0])
			db.CharacterSet = charset
			db.CollationName = catalog.DefaultCollationForCharset(charset)
		}
	}
	// Extract COLLATE
	collIdx := strings.Index(restUpper, "COLLATE ")
	if collIdx >= 0 {
		afterColl := strings.TrimSpace(restUpper[collIdx+len("COLLATE "):])
		afterColl = strings.TrimPrefix(afterColl, "= ")
		afterColl = strings.TrimPrefix(afterColl, "=")
		afterColl = strings.TrimSpace(afterColl)
		collFields := strings.Fields(afterColl)
		if len(collFields) > 0 {
			collation := strings.ToLower(collFields[0])
			db.CollationName = collation
			// Derive charset from collation
			parts := strings.SplitN(collation, "_", 2)
			if len(parts) > 0 {
				db.CharacterSet = parts[0]
			}
		}
	}

	return &Result{}, nil
}

// execAlterTableOrderBy handles ALTER TABLE ... ORDER BY col1, col2, ...
func (e *Executor) execAlterTableOrderBy(query string) (*Result, error) {
	upper := strings.ToUpper(query)
	// Extract table name: it's the next token after ALTER TABLE
	altIdx := strings.Index(upper, "ALTER TABLE ") + len("ALTER TABLE ")
	obIdx := strings.Index(upper, " ORDER BY ")
	if altIdx < 0 || obIdx < 0 {
		return &Result{}, nil
	}

	// The table name is the first token after ALTER TABLE
	rest := strings.TrimSpace(query[altIdx:])
	var tableName string
	// Handle backtick-quoted names
	if len(rest) > 0 && rest[0] == '`' {
		endBT := strings.Index(rest[1:], "`")
		if endBT >= 0 {
			tableName = rest[1 : endBT+1]
			rest = strings.TrimSpace(rest[endBT+2:])
		}
	} else {
		// Unquoted name: up to first space or comma
		endIdx := strings.IndexAny(rest, " \t,")
		if endIdx >= 0 {
			tableName = rest[:endIdx]
			rest = strings.TrimSpace(rest[endIdx:])
		} else {
			tableName = rest
			rest = ""
		}
	}

	// If there are other ALTER operations before ORDER BY (e.g., ADD COLUMN ... , ORDER BY ...),
	// strip the ORDER BY clause and execute the rest through the regular parser, then apply ORDER BY.
	midPart := strings.TrimSpace(query[altIdx+len(tableName) : obIdx])
	midPart = strings.TrimSpace(midPart)
	if midPart != "" {
		// There are other operations: execute the ALTER TABLE without ORDER BY first
		alterNoOrder := strings.TrimRight(strings.TrimSpace(query[:obIdx]), ",")
		_, err := e.Execute(alterNoOrder)
		if err != nil {
			return nil, err
		}
	}
	orderByStr := strings.TrimSpace(query[obIdx+len(" ORDER BY "):])

	tbl, err := e.Storage.GetTable(e.CurrentDB, tableName)
	if err != nil {
		return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", e.CurrentDB, tableName))
	}
	orderCollation := ""
	if db, dbErr := e.Catalog.GetDatabase(e.CurrentDB); dbErr == nil {
		if def, defErr := db.GetTable(tableName); defErr == nil {
			orderCollation = effectiveTableCollation(def)
		}
	}

	// Parse ORDER BY columns
	type orderCol struct {
		name string
		desc bool
	}
	var orderCols []orderCol
	for _, part := range strings.Split(orderByStr, ",") {
		part = strings.TrimSpace(part)
		part = strings.TrimSuffix(part, ";")
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		fields := strings.Fields(part)
		col := orderCol{name: strings.Trim(fields[0], "`")}
		if len(fields) > 1 && strings.ToUpper(fields[1]) == "DESC" {
			col.desc = true
		}
		orderCols = append(orderCols, col)
	}

	// Sort the rows in the storage table
	tbl.Mu.Lock()
	sort.SliceStable(tbl.Rows, func(i, j int) bool {
		for _, oc := range orderCols {
			vi := rowValueByColumnName(tbl.Rows[i], oc.name)
			vj := rowValueByColumnName(tbl.Rows[j], oc.name)
			cmp := compareByCollation(vi, vj, orderCollation)
			if cmp == 0 {
				continue
			}
			if oc.desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
	tbl.Mu.Unlock()

	return &Result{}, nil
}

func hasArrayCastExpr(expr sqlparser.Expr) bool {
	if expr == nil {
		return false
	}
	switch v := expr.(type) {
	case *sqlparser.CastExpr:
		return v.Array || hasArrayCastExpr(v.Expr)
	case *sqlparser.ConvertExpr:
		return hasArrayCastExpr(v.Expr)
	case *sqlparser.BinaryExpr:
		return hasArrayCastExpr(v.Left) || hasArrayCastExpr(v.Right)
	case *sqlparser.AndExpr:
		return hasArrayCastExpr(v.Left) || hasArrayCastExpr(v.Right)
	case *sqlparser.OrExpr:
		return hasArrayCastExpr(v.Left) || hasArrayCastExpr(v.Right)
	case *sqlparser.NotExpr:
		return hasArrayCastExpr(v.Expr)
	case *sqlparser.ComparisonExpr:
		return hasArrayCastExpr(v.Left) || hasArrayCastExpr(v.Right)
	case *sqlparser.CollateExpr:
		return hasArrayCastExpr(v.Expr)
	case *sqlparser.FuncExpr:
		for _, arg := range v.Exprs {
			if hasArrayCastExpr(arg) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func validateArrayIndexExpression(expr sqlparser.Expr) error {
	if !hasArrayCastExpr(expr) {
		return nil
	}
	castExpr, ok := expr.(*sqlparser.CastExpr)
	if !ok || !castExpr.Array {
		return mysqlError(1235, "42000", "This version of MySQL doesn't yet support 'CAST-ing data to array type'")
	}
	if hasArrayCastExpr(castExpr.Expr) {
		return mysqlError(1235, "42000", "This version of MySQL doesn't yet support 'CAST-ing data to array type'")
	}
	typeName := strings.ToUpper(castExpr.Type.Type)
	switch typeName {
	case "UNSIGNED", "SIGNED", "INT", "INTEGER", "BIGINT", "DECIMAL", "DATE", "TIME", "DATETIME":
		return nil
	case "CHAR", "BINARY":
		if castExpr.Type.Length == nil || *castExpr.Type.Length > 1024 {
			return mysqlError(1235, "42000", "This version of MySQL doesn't yet support 'CAST-ing data to array type'")
		}
		if castExpr.Type.Charset.Name != "" {
			return mysqlError(1235, "42000", "This version of MySQL doesn't yet support 'CAST-ing data to array type'")
		}
		return nil
	default:
		return mysqlError(1235, "42000", "This version of MySQL doesn't yet support 'CAST-ing data to array type'")
	}
}

func (e *Executor) execCreateTable(stmt *sqlparser.CreateTable) (*Result, error) {
	// DDL causes an implicit COMMIT in MySQL (release row locks, end transaction).
	if e.inTransaction {
		e.execCommit()
	}
	dbName := e.CurrentDB
	tableName := stmt.Table.Name.String()
	if !stmt.Table.Qualifier.IsEmpty() {
		dbName = stmt.Table.Qualifier.String()
	}
	db, err := e.Catalog.GetDatabase(dbName)
	if err != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", dbName))
	}

	if stmt.TableSpec == nil {
		// CREATE TABLE ... LIKE
		if stmt.OptLike != nil {
			srcName := stmt.OptLike.LikeTable.Name.String()
			srcDB := dbName
			if !stmt.OptLike.LikeTable.Qualifier.IsEmpty() {
				srcDB = stmt.OptLike.LikeTable.Qualifier.String()
			}
			return e.execCreateTableLike(dbName, tableName, srcDB, srcName)
		}
		// CREATE TABLE ... SELECT
		if stmt.Select != nil {
			// CREATE TABLE IF NOT EXISTS ... SELECT: skip if table already exists
			if stmt.IfNotExists {
				if _, err := e.Storage.GetTable(dbName, tableName); err == nil {
					return &Result{}, nil
				}
			}
			// When the SELECT has a WITH clause (CTE), always use the parsed AST string
			// to preserve the WITH clause. Otherwise prefer extracting from the original
			// query text to preserve case and spacing.
			selectSQL := ""
			hasWith := false
			switch sel := stmt.Select.(type) {
			case *sqlparser.Select:
				hasWith = sel.With != nil && len(sel.With.CTEs) > 0
			case *sqlparser.Union:
				hasWith = sel.With != nil && len(sel.With.CTEs) > 0
			}
			if !hasWith {
				selectSQL = e.extractSelectFromQuery(e.currentQuery)
			}
			if selectSQL == "" {
				selectSQL = sqlparser.String(stmt.Select)
			}
			result, err := e.execCreateTableSelect(dbName, tableName, selectSQL)
			if err == nil && stmt.Temp {
				e.tempTables[tableName] = true
			}
			return result, err
		}
		// Special case: CREATE TABLE t (PRIMARY KEY (a)) SELECT ...
		// Vitess parses this with TableSpec=nil and Select=nil when the parens
		// contain only index definitions (no column definitions). Detect this
		// by checking if the current query text contains SELECT, and if the
		// paren block only has index definitions.
		if result, err := e.tryExecCreateTableIndexOnlySelect(stmt, dbName, tableName); result != nil || err != nil {
			return result, err
		}

		// TableSpec is nil but it's not a CREATE TABLE ... LIKE or ... SELECT.
		// This happens when vitess accepts invalid syntax that MySQL rejects
		// (e.g. CHECK without parentheses, bare CONSTRAINT without key type).
		// Return a parse error to match MySQL behaviour.
		return nil, mysqlError(1064, "42000",
			"You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '' at line 1")
	}

	columns := make([]catalog.ColumnDef, 0)
	var primaryKeys []string

	// Check for unsupported storage engines with generated columns
	{
		engine := "InnoDB" // default
		// Check explicit ENGINE= in CREATE TABLE
		for _, opt := range stmt.TableSpec.Options {
			if strings.EqualFold(opt.Name, "ENGINE") || strings.EqualFold(opt.Name, "engine") {
				engine = tableOptionString(opt)
				break
			}
		}
		// If no explicit engine, check session default_storage_engine
		if engine == "InnoDB" {
			if e.sessionScopeVars != nil || e.globalScopeVars != nil {
				if eng, ok := e.getSysVar("default_storage_engine"); ok && eng != "" {
					engine = eng
				}
			}
		}
		engineUpper := strings.ToUpper(engine)
		// FEDERATED is compiled in but disabled
		if engineUpper == "FEDERATED" {
			return nil, mysqlError(1286, "42000", fmt.Sprintf("Unknown storage engine '%s'", engine))
		}
		if engineUpper == "MEMORY" || engineUpper == "MERGE" || engineUpper == "MRG_MYISAM" || engineUpper == "BLACKHOLE" || engineUpper == "ARCHIVE" {
			for _, col := range stmt.TableSpec.Columns {
				if col.Type.Options != nil && col.Type.Options.As != nil {
					return nil, mysqlError(3106, "HY000", "'Specified storage engine' is not supported for generated columns.")
				}
			}
		}
	}

	// Check for reserved InnoDB internal column names
	reservedInnoDBCols := map[string]bool{
		"db_row_id": true, "db_trx_id": true, "db_roll_ptr": true,
	}
	for _, col := range stmt.TableSpec.Columns {
		colNameLower := strings.ToLower(col.Name.String())
		if reservedInnoDBCols[colNameLower] {
			return nil, mysqlError(1166, "42000", fmt.Sprintf("Incorrect column name '%s'", col.Name.String()))
		}
	}

	// Extract table-level charset before processing columns so that
	// buildColumnTypeString can propagate binary charset to columns.
	var tableCharset string
	for _, opt := range stmt.TableSpec.Options {
		if strings.EqualFold(opt.Name, "CHARACTER SET") || strings.EqualFold(opt.Name, "CHARSET") {
			tableCharset = opt.String
		}
	}

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

		// Check virtual generated column with KEY/PRIMARY KEY
		if col.Type.Options != nil && col.Type.Options.As != nil &&
			col.Type.Options.Storage != sqlparser.StoredStorage &&
			(col.Type.Options.KeyOpt == 1 || col.Type.Options.KeyOpt == 6) {
			return nil, mysqlError(3106, "HY000", "'Defining a virtual generated column as primary key' is not supported for generated columns.")
		}

		// Reject generated columns with DEFAULT, AUTO_INCREMENT, SERIAL DEFAULT VALUE, or ON UPDATE
		if col.Type.Options != nil && col.Type.Options.As != nil {
			if col.Type.Options.Default != nil {
				return nil, mysqlError(1221, "HY000", "Incorrect usage of DEFAULT and generated column")
			}
			if col.Type.Options.Autoincrement {
				return nil, mysqlError(1221, "HY000", "Incorrect usage of AUTO_INCREMENT and generated column")
			}
			if col.Type.Options.OnUpdate != nil {
				return nil, mysqlError(1221, "HY000", "Incorrect usage of ON UPDATE and generated column")
			}
		}

		// Validate generated column expressions for blocked functions
		if col.Type.Options != nil && col.Type.Options.As != nil {
			blocked, found := findBlockedFunctionInExpr(col.Type.Options.As)
			if found {
				if blocked != "" {
					return nil, mysqlError(3102, "HY000",
						fmt.Sprintf("Expression of generated column '%s' contains a disallowed function: %s.",
							col.Name.String(), blocked))
				}
				return nil, mysqlError(3102, "HY000",
					fmt.Sprintf("Expression of generated column '%s' contains a disallowed function.",
						col.Name.String()))
			}
		}

		// Default: nullable unless NOT NULL is explicitly specified
		nullable := true
		if col.Type.Options != nil && col.Type.Options.Null != nil {
			nullable = *col.Type.Options.Null
		}

		colDef := catalog.ColumnDef{
			Name:     col.Name.String(),
			Type:     buildColumnTypeString(col.Type, tableCharset),
			Nullable: nullable,
		}
		// Capture column-level charset if explicitly specified.
		// When charset is "binary", the type has already been converted to its binary
		// equivalent (e.g. char->binary, varchar->varbinary, text->blob) in
		// buildColumnTypeString, so we don't need to store the charset separately.
		if col.Type.Charset.Name != "" {
			csLower := strings.ToLower(col.Type.Charset.Name)
			if csLower != "binary" {
				colDef.Charset = csLower
				colDef.Collation = catalog.DefaultCollationForCharset(colDef.Charset)
			}
		} else if col.Type.Charset.Binary {
			// BINARY modifier without explicit CHARACTER SET means binary collation of the current charset.
			// e.g. VARCHAR(30) BINARY with table charset latin1 -> collation latin1_bin.
			// We store the charset explicitly so SHOW CREATE TABLE can display the collation.
			cs := tableCharset
			if cs == "" {
				cs = "utf8mb4"
			}
			colDef.Charset = cs
			colDef.Collation = catalog.BinaryCollationForCharset(cs)
		} else if strings.EqualFold(tableCharset, "binary") {
			// Table-level CHARACTER SET binary propagates to columns without explicit charset.
		}
		if tUpper := strings.ToUpper(strings.TrimSpace(colDef.Type)); strings.HasPrefix(tUpper, "BIT(") {
			var width int
			if n, err := fmt.Sscanf(tUpper, "BIT(%d)", &width); err == nil && n == 1 {
				if width < 1 {
					return nil, mysqlError(1441, "HY000", fmt.Sprintf("Invalid size for column '%s'.", colDef.Name))
				} else if width > 64 {
					return nil, mysqlError(1439, "42000", fmt.Sprintf("Display width out of range for column '%s' (max = 64)", colDef.Name))
				}
			}
		}
		if err := validateNumericTypeSpec(colDef.Type, colDef.Name); err != nil {
			return nil, mysqlError(1426, "42000", err.Error())
		}
		// YEAR column only supports YEAR or YEAR(4).
		if strings.EqualFold(col.Type.Type, "year") && col.Type.Length != nil {
			yearLen := int(*col.Type.Length)
			if yearLen != 4 {
				return nil, mysqlError(1818, "HY000", "Supports only YEAR or YEAR(4) column.")
			}
		}

		if col.Type.Options != nil {
			if col.Type.Options.As != nil && hasArrayCastExpr(col.Type.Options.As) {
				return nil, mysqlError(1235, "42000", "This version of MySQL doesn't yet support 'CAST-ing data to array type'")
			}
			if col.Type.Options.Default != nil && hasArrayCastExpr(col.Type.Options.Default) {
				return nil, mysqlError(1235, "42000", "This version of MySQL doesn't yet support 'CAST-ing data to array type'")
			}
			if col.Type.Options.Autoincrement {
				colDef.AutoIncrement = true
			}
			if col.Type.Options.Default != nil {
				defStr := sqlparser.String(col.Type.Options.Default)
				// DEFAULT NULL means the column has a NULL default (no explicit value)
				if strings.EqualFold(defStr, "null") {
					// Keep colDef.Default as nil to represent NULL default
				} else {
					// Strip surrounding quotes from default values (vitess adds them)
					if len(defStr) >= 2 && defStr[0] == '\'' && defStr[len(defStr)-1] == '\'' {
						defStr = defStr[1 : len(defStr)-1]
					}
					// MySQL strips trailing spaces from SET/ENUM default values
					colTypeLower := strings.ToLower(col.Type.Type)
					if colTypeLower == "set" || colTypeLower == "enum" {
						defStr = strings.TrimRight(defStr, " ")
					}
					colDef.Default = &defStr
				}
			}
			// Validate zero date/datetime/timestamp defaults in strict mode
			if colDef.Default != nil && e.isStrictMode() {
				colTypeUpper := strings.ToUpper(strings.TrimSpace(col.Type.Type))
				hasNoZeroDate := strings.Contains(e.sqlMode, "NO_ZERO_DATE") || strings.Contains(e.sqlMode, "TRADITIONAL")
				hasNoZeroInDate := strings.Contains(e.sqlMode, "NO_ZERO_IN_DATE") || strings.Contains(e.sqlMode, "TRADITIONAL")
				switch colTypeUpper {
				case "TIMESTAMP", "DATETIME":
					// NO_ZERO_DATE catches zero dates (0000-00-00) and zero-in-date (2012-02-00)
					// NO_ZERO_IN_DATE also catches zero-in-date values
					if (hasNoZeroDate || hasNoZeroInDate) && isZeroInDateValue(*colDef.Default) {
						return nil, mysqlError(1067, "42000", fmt.Sprintf("Invalid default value for '%s'", colDef.Name))
					}
				case "DATE":
					if (hasNoZeroDate || hasNoZeroInDate) && isZeroInDateValue(*colDef.Default) {
						return nil, mysqlError(1067, "42000", fmt.Sprintf("Invalid default value for '%s'", colDef.Name))
					}
				}
			}
			if col.Type.Options.OnUpdate != nil {
				onUpdateStr := strings.ToUpper(sqlparser.String(col.Type.Options.OnUpdate))
				if strings.Contains(onUpdateStr, "CURRENT_TIMESTAMP") || strings.Contains(onUpdateStr, "NOW") {
					colDef.OnUpdateCurrentTimestamp = true
				}
			}
			// Validate that PRIMARY KEY/KEY cannot be INVISIBLE at column level
			if col.Type.Options.Invisible != nil && (col.Type.Options.KeyOpt == sqlparser.ColKeyPrimary || col.Type.Options.KeyOpt == sqlparser.ColKey) {
				return nil, mysqlError(3522, "HY000", "A primary key index cannot be invisible")
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

		// Save comment (MySQL truncates column comments > 1024 chars, errors in strict/traditional mode)
		if col.Type.Options != nil && col.Type.Options.Comment != nil {
			comment := col.Type.Options.Comment.Val
			if mysqlCharLen(comment) > 1024 {
				if e.isStrictMode() {
					return nil, mysqlError(1629, "HY000", fmt.Sprintf("Comment for field '%s' is too long (max = 1024)", colDef.Name))
				}
				comment = mysqlTruncateChars(comment, 1024)
			}
			colDef.Comment = comment
		}

		columns = append(columns, colDef)
	}

	// Pre-scan table options for ROW_FORMAT (needed for key length validation)
	tableRowFormat := ""
	for _, opt := range stmt.TableSpec.Options {
		if strings.EqualFold(strings.TrimSpace(opt.Name), "ROW_FORMAT") {
			tableRowFormat = strings.ToUpper(tableOptionString(opt))
		}
	}

	// Process index definitions
	var indexes []catalog.IndexDef
	hasArrayMVIIndex := false
	for _, idx := range stmt.TableSpec.Indexes {
		// Reject the reserved InnoDB clustered index name.
		if strings.EqualFold(idx.Info.Name.String(), "GEN_CLUST_INDEX") {
			return nil, mysqlError(1280, "42000", "Incorrect index name 'GEN_CLUST_INDEX'")
		}
		var idxCols []string
		var idxOrders []string
		for _, idxCol := range idx.Columns {
			if err := validateArrayIndexExpression(idxCol.Expression); err != nil {
				return nil, err
			}
			if hasArrayCastExpr(idxCol.Expression) {
				hasArrayMVIIndex = true
			}
			colStr := strings.ToLower(idxCol.Column.String())
			if idxCol.Expression != nil {
				colStr = fmt.Sprintf("(%s)", strings.TrimSpace(sqlparser.String(idxCol.Expression)))
			} else if idxCol.Length != nil {
				colStr += fmt.Sprintf("(%d)", *idxCol.Length)
			}
			idxCols = append(idxCols, colStr)
			if idxCol.Direction == sqlparser.DescOrder {
				idxOrders = append(idxOrders, "DESC")
			} else {
				idxOrders = append(idxOrders, "")
			}
		}
		// Check for duplicate column names in the index (ER_DUP_FIELDNAME = 1060)
		// Skip functional index expressions (those starting with '(') since they
		// don't map to real column names and stripPrefixLengthFromCol returns "".
		{
			seen := make(map[string]bool, len(idxCols))
			for _, c := range idxCols {
				if strings.HasPrefix(c, "(") {
					continue // functional index expression, skip duplicate check
				}
				base := strings.ToLower(stripPrefixLengthFromCol(c))
				if seen[base] {
					return nil, mysqlError(1060, "42S21", fmt.Sprintf("Duplicate column name '%s'", stripPrefixLengthFromCol(c)))
				}
				seen[base] = true
			}
		}
		// Check max key parts per index (ER_TOO_MANY_KEY_PARTS = 1070)
		if len(idxCols) > 16 {
			return nil, mysqlError(1070, "42000", "Too many key parts specified; max 16 parts allowed")
		}
		// Check key prefix length limits (ER_TOO_LONG_KEY = 1071)
		// COMPACT/REDUNDANT row format: max 767 bytes per key part
		// DYNAMIC/COMPRESSED: max 3072 bytes per key part
		for _, idxCol := range idx.Columns {
			if idxCol.Length != nil {
				prefixLen := *idxCol.Length
				maxPrefixLen := 3072 // DYNAMIC default
				if tableRowFormat == "COMPACT" || tableRowFormat == "REDUNDANT" {
					maxPrefixLen = 767
				}
				if prefixLen > maxPrefixLen {
					return nil, mysqlError(1071, "42000", fmt.Sprintf("Specified key was too long; max key length is %d bytes", maxPrefixLen))
				}
			}
		}
		if idx.Info.Type == sqlparser.IndexTypePrimary {
			primaryKeys = nil
			// Use the actual column names from the columns slice (preserving case)
			// to ensure row lookups work correctly.
			for _, idxCol := range idxCols {
				matched := false
				for _, col := range columns {
					if strings.EqualFold(col.Name, idxCol) {
						primaryKeys = append(primaryKeys, col.Name)
						matched = true
						break
					}
				}
				if !matched {
					primaryKeys = append(primaryKeys, idxCol)
				}
			}
			// Mark PK columns as NOT NULL (PRIMARY KEY implies NOT NULL)
			for i, col := range columns {
				for _, pk := range idxCols {
					if strings.EqualFold(col.Name, pk) {
						columns[i].Nullable = false
					}
				}
			}
		} else {
			isUnique := idx.Info.Type == sqlparser.IndexTypeUnique
			idxType := ""
			if idx.Info.Type == sqlparser.IndexTypeFullText {
				idxType = "FULLTEXT"
			} else if idx.Info.Type == sqlparser.IndexTypeSpatial {
				idxType = "SPATIAL"
				// SPATIAL indexes require geometrical column types
				for _, ic := range idxCols {
					icName := stripPrefixLengthFromCol(ic)
					isGeo := false
					for _, col := range columns {
						if strings.EqualFold(col.Name, icName) {
							colUpper := strings.ToUpper(col.Type)
							if strings.Contains(colUpper, "GEOMETRY") || strings.Contains(colUpper, "POINT") ||
								strings.Contains(colUpper, "LINESTRING") || strings.Contains(colUpper, "POLYGON") ||
								strings.Contains(colUpper, "MULTIPOINT") || strings.Contains(colUpper, "MULTILINESTRING") ||
								strings.Contains(colUpper, "MULTIPOLYGON") || strings.Contains(colUpper, "GEOMETRYCOLLECTION") {
								isGeo = true
							}
							break
						}
					}
					if !isGeo {
						return nil, mysqlError(1687, "42000", "A SPATIAL index may only contain a geometrical type column")
					}
				}
			}
			idxName := idx.Info.Name.String()
			if idxName == "" {
				if cn := idx.Info.ConstraintName.String(); cn != "" {
					idxName = cn
				} else {
					// Use column name without prefix length for default index name
					idxName = stripPrefixLengthFromCol(idxCols[0])
				}
			}
			idxComment := ""
			usingMethod := ""
			for _, opt := range idx.Options {
				if strings.ToUpper(opt.Name) == "COMMENT" {
					if opt.Value != nil {
						idxComment = opt.Value.Val
					} else {
						idxComment = opt.String
					}
				}
				if opt.Name == "USING" {
					usingMethod = strings.ToUpper(opt.String)
				}
			}
			idxInvisible := false
			for _, opt := range idx.Options {
				if strings.EqualFold(opt.Name, "VISIBLE") {
					idxInvisible = false
				} else if strings.EqualFold(opt.Name, "INVISIBLE") {
					idxInvisible = true
				}
			}
			// Validate that PRIMARY KEY cannot be INVISIBLE
			if idxInvisible && idx.Info.Type == sqlparser.IndexTypePrimary {
				return nil, mysqlError(3522, "HY000", "A primary key index cannot be invisible")
			}
			// MySQL returns error for index comments > 1024 in strict/TRADITIONAL mode;
			// in non-strict mode it truncates silently.
			if mysqlCharLen(idxComment) > 1024 {
				if e.isStrictMode() {
					return nil, mysqlError(1688, "HY000", fmt.Sprintf("Comment for index '%s' is too long (max = 1024)", idxName))
				}
				idxComment = mysqlTruncateChars(idxComment, 1024)
			}
			indexes = append(indexes, catalog.IndexDef{
				Name:      idxName,
				Columns:   idxCols,
				Orders:    idxOrders,
				Unique:    isUnique,
				Type:      idxType,
				Using:     usingMethod,
				Comment:   idxComment,
				Invisible: idxInvisible,
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

	// Validate foreign key constraints: detect duplicate constraint names (ER_FK_DUP_NAME)
	// and create implicit indexes for foreign key columns (MySQL auto-creates these).
	fkNames := make(map[string]bool)
	fkIdx := 0
	for _, constraint := range stmt.TableSpec.Constraints {
		if fkDef, ok := constraint.Details.(*sqlparser.ForeignKeyDefinition); ok {
			name := constraint.Name.String()
			nameLower := strings.ToLower(name)
			if nameLower != "" {
				if fkNames[nameLower] {
					return nil, mysqlError(1826, "HY000", fmt.Sprintf("Duplicate foreign key constraint name '%s'", name))
				}
				fkNames[nameLower] = true
			}
			// Reject FK on virtual generated columns and certain FK actions on stored generated columns
			for _, srcCol := range fkDef.Source {
				srcName := srcCol.String()
				for _, col := range columns {
					if strings.EqualFold(col.Name, srcName) && isGeneratedColumnType(col.Type) {
						if !strings.Contains(strings.ToUpper(col.Type), "STORED") {
							// Virtual generated column cannot have FK
							// MySQL includes the FK name and column name; approximate with generic message
							fkName := constraint.Name.String()
							if fkName == "" {
								fkName = fmt.Sprintf("%s_ibfk_%d", tableName, fkIdx+1)
							}
							return nil, mysqlError(3108, "HY000", fmt.Sprintf("Foreign key '%s' uses virtual column '%s' which is not supported.", fkName, srcName))
						}
						if fkDef.ReferenceDefinition != nil {
							if fkDef.ReferenceDefinition.OnUpdate == sqlparser.SetNull {
								return nil, mysqlError(3104, "HY000", "Cannot define foreign key with ON UPDATE SET NULL clause on a generated column.")
							}
							if fkDef.ReferenceDefinition.OnUpdate == sqlparser.Cascade {
								return nil, mysqlError(3104, "HY000", "Cannot define foreign key with ON UPDATE CASCADE clause on a generated column.")
							}
							if fkDef.ReferenceDefinition.OnDelete == sqlparser.SetNull {
								return nil, mysqlError(3104, "HY000", "Cannot define foreign key with ON DELETE SET NULL clause on a generated column.")
							}
						}
					}
				}
			}
			// Create implicit index for FK columns (MySQL does this automatically)
			var fkCols []string
			for _, col := range fkDef.Source {
				fkCols = append(fkCols, col.String())
			}
			if len(fkCols) > 0 {
				// Check if an index already covers these columns
				covered := false
				for _, idx := range indexes {
					if len(idx.Columns) >= len(fkCols) {
						match := true
						for k, fc := range fkCols {
							if !strings.EqualFold(idx.Columns[k], fc) {
								match = false
								break
							}
						}
						if match {
							covered = true
							break
						}
					}
				}
				if !covered {
					idxName := name
					if idxName == "" {
						// MySQL auto-names FK indexes after the first FK column.
						// If that name is taken, append _2, _3, etc.
						baseIdxName := fkCols[0]
						idxName = baseIdxName
						suffix := 2
						for {
							taken := false
							for _, idx := range indexes {
								if strings.EqualFold(idx.Name, idxName) {
									taken = true
									break
								}
							}
							if !taken {
								break
							}
							idxName = fmt.Sprintf("%s_%d", baseIdxName, suffix)
							suffix++
						}
					}
					idxOrders := make([]string, len(fkCols))
					indexes = append(indexes, catalog.IndexDef{
						Name:    idxName,
						Columns: fkCols,
						Orders:  idxOrders,
					})
				}
			}
		}
	}

	// Extract CHECK constraints
	var checkConstraints []catalog.CheckConstraint
	checkIdx := 0
	for _, constraint := range stmt.TableSpec.Constraints {
		if checkDef, ok := constraint.Details.(*sqlparser.CheckConstraintDefinition); ok {
			name := constraint.Name.String()
			if name == "" {
				checkIdx++
				name = fmt.Sprintf("%s_chk_%d", tableName, checkIdx)
			}
			checkConstraints = append(checkConstraints, catalog.CheckConstraint{
				Name: name,
				Expr: sqlparser.String(checkDef.Expr),
			})
		}
	}

	// Extract FOREIGN KEY constraints
	var foreignKeys []catalog.ForeignKeyDef
	fkAutoIdx := 0
	for _, constraint := range stmt.TableSpec.Constraints {
		if fkDef, ok := constraint.Details.(*sqlparser.ForeignKeyDefinition); ok {
			name := constraint.Name.String()
			if name == "" {
				fkAutoIdx++
				name = fmt.Sprintf("%s_ibfk_%d", tableName, fkAutoIdx)
			}
			var cols []string
			for _, col := range fkDef.Source {
				cols = append(cols, col.String())
			}
			fk := catalog.ForeignKeyDef{
				Name:    name,
				Columns: cols,
			}
			if fkDef.ReferenceDefinition != nil {
				fk.ReferencedTable = fkDef.ReferenceDefinition.ReferencedTable.Name.String()
				for _, col := range fkDef.ReferenceDefinition.ReferencedColumns {
					fk.ReferencedColumns = append(fk.ReferencedColumns, col.String())
				}
				fk.OnDelete = referenceActionToString(fkDef.ReferenceDefinition.OnDelete)
				fk.OnUpdate = referenceActionToString(fkDef.ReferenceDefinition.OnUpdate)
			}
			foreignKeys = append(foreignKeys, fk)
		}
	}

	// Check max indexes per table (ER_TOO_MANY_KEYS = 1069)
	// MySQL counts: primary key (if any) + all secondary indexes
	{
		totalKeys := len(indexes)
		if len(primaryKeys) > 0 {
			totalKeys++
		}
		if totalKeys > 64 {
			return nil, mysqlError(1069, "42000", "Too many keys specified; max 64 keys allowed")
		}
	}

	def := &catalog.TableDef{
		Name:             tableName,
		Columns:          columns,
		PrimaryKey:       primaryKeys,
		Indexes:          indexes,
		CheckConstraints: checkConstraints,
		ForeignKeys:      foreignKeys,
	}
	// Inherit database defaults unless overridden by explicit table options.
	if db.CharacterSet != "" {
		def.Charset = strings.ToLower(db.CharacterSet)
	}
	if db.CollationName != "" {
		def.Collation = strings.ToLower(db.CollationName)
	}

	// Process table options (comment, charset, collate) BEFORE creating the table,
	// so that strict-mode errors prevent the table from being created.
	charsetSpecified := false
	collationSpecified := false
	tablespaceName := ""
	hasDataDirectory := false
	for _, opt := range stmt.TableSpec.Options {
		optName := strings.ToUpper(strings.TrimSpace(opt.Name))
		optVal := tableOptionString(opt)
		switch optName {
		case "COMMENT":
			comment := opt.Value.Val
			if mysqlCharLen(comment) > 2048 {
				if e.isStrictMode() {
					return nil, mysqlError(1628, "HY000", fmt.Sprintf("Comment for table '%s' is too long (max = 2048)", tableName))
				}
				comment = mysqlTruncateChars(comment, 2048)
			}
			def.Comment = comment
		case "CHARSET", "CHARACTER SET":
			def.Charset = strings.ToLower(opt.String)
			charsetSpecified = true
		case "COLLATE":
			def.Collation = strings.ToLower(opt.String)
			collationSpecified = true
		case "ENGINE":
			def.Engine = strings.ToUpper(opt.String)
		case "ROW_FORMAT":
			def.RowFormat = strings.ToUpper(tableOptionString(opt))
		case "KEY_BLOCK_SIZE":
			def.KeyBlockSize = parseTableOptionInt(opt)
		case "STATS_PERSISTENT":
			def.StatsPersistent = parseTableOptionInt(opt)
		case "STATS_AUTO_RECALC":
			def.StatsAutoRecalc = parseTableOptionInt(opt)
		case "STATS_SAMPLE_PAGES":
			def.StatsSamplePages = parseTableOptionInt(opt)
		case "MAX_ROWS":
			def.MaxRows = parseTableOptionUint64Clamped(opt)
		case "MIN_ROWS", "AVG_ROW_LENGTH":
			// Accepted but not stored/displayed
		case "INSERT_METHOD":
			def.InsertMethod = strings.ToUpper(tableOptionString(opt))
		case "UNION":
			if opt.Tables != nil {
				for _, tn := range opt.Tables {
					def.UnionTables = append(def.UnionTables, tn.Name.String())
				}
			}
		case "TABLESPACE":
			tablespaceName = strings.ToLower(strings.Trim(strings.TrimSpace(optVal), "`'\""))
		case "DATA DIRECTORY":
			hasDataDirectory = strings.TrimSpace(optVal) != ""
		}
	}
	if strings.EqualFold(def.Engine, "INNODB") && hasDataDirectory && tablespaceName == "innodb_system" {
		return nil, mysqlError(1478, "HY000", "Table storage engine 'InnoDB' does not support the create option 'DATA DIRECTORY'")
	}
	if hasArrayMVIIndex && def.Engine != "" && !strings.EqualFold(def.Engine, "INNODB") {
		return nil, mysqlError(1178, "42000", "The storage engine for the table doesn't support check")
	}
	// In InnoDB strict mode with page size > 16k, reject ROW_FORMAT=COMPRESSED
	// and KEY_BLOCK_SIZE since they're not supported at larger page sizes.
	if e.isInnoDBStrictMode() {
		pageSize := e.getInnoDBPageSize()
		if pageSize > 16384 {
			isCompressed := strings.EqualFold(def.RowFormat, "COMPRESSED")
			hasKeyBlockSize := def.KeyBlockSize != nil && *def.KeyBlockSize > 0
			if isCompressed || hasKeyBlockSize {
				return nil, mysqlError(1031, "HY000", fmt.Sprintf("Table storage engine for '%s' doesn't have this option", tableName))
			}
		}
	}
	// Validate collation is compatible with charset (ER_COLLATION_CHARSET_MISMATCH = 1253)
	if charsetSpecified && collationSpecified && def.Charset != "" && def.Collation != "" {
		if collCharset, ok := catalog.CharsetForCollation(def.Collation); ok {
			if !strings.EqualFold(collCharset, def.Charset) {
				return nil, mysqlError(1253, "42000", fmt.Sprintf("COLLATION '%s' is not valid for CHARACTER SET '%s'", def.Collation, def.Charset))
			}
		}
	}
	// If charset was set but collation was not, always derive collation for that charset.
	if charsetSpecified && !collationSpecified {
		def.Collation = catalog.DefaultCollationForCharset(def.Charset)
	} else if def.Charset != "" && def.Collation == "" {
		def.Collation = catalog.DefaultCollationForCharset(def.Charset)
	}

	// Extract partition metadata for row ordering.
	// MySQL returns rows in partition order; for RANGE partitions this means
	// ascending order on the partition expression column(s).
	if stmt.TableSpec.PartitionOption != nil {
		po := stmt.TableSpec.PartitionOption
		switch po.Type {
		case sqlparser.RangeType:
			def.PartitionType = "RANGE"
		case sqlparser.ListType:
			def.PartitionType = "LIST"
		case sqlparser.HashType:
			def.PartitionType = "HASH"
		case sqlparser.KeyType:
			def.PartitionType = "KEY"
		}
		// For RANGE partitions with a simple column expression, record the
		// column so that Scan can sort rows in partition order.
		if po.Type == sqlparser.RangeType && po.Expr != nil {
			if col, ok := po.Expr.(*sqlparser.ColName); ok {
				def.PartitionColumns = []string{col.Name.String()}
			}
		}
	}

	// If no explicit ENGINE was specified, resolve from session default_storage_engine.
	// This ensures table.Engine is always set so ANALYZE TABLE can determine the correct response.
	if def.Engine == "" {
		if eng, ok := e.getSysVar("default_storage_engine"); ok && eng != "" {
			def.Engine = strings.ToUpper(eng)
		} else {
			def.Engine = "INNODB"
		}
	}

	// Temporary tables are connection-scoped in MySQL, but this simplified engine
	// uses a shared catalog. Recreate temporary tables idempotently to avoid
	// cross-session name collisions in MTR multi-connection tests.
	if stmt.Temp {
		// Check if there's an existing permanent table we need to save.
		// If this session already has a temp table with this name (re-creating it),
		// we already saved the permanent one, so don't overwrite the saved state.
		if _, alreadyTemp := e.tempTables[tableName]; !alreadyTemp {
			// Save the permanent table state before shadowing it.
			if existingDef, err2 := db.GetTable(tableName); err2 == nil {
				savedTable := e.Storage.SaveTable(dbName, tableName)
				e.tempTableSavedPermanent[tableName] = &savedPermTable{
					def:   existingDef,
					table: savedTable,
				}
			}
		}
		_ = db.DropTable(tableName)
		e.Storage.DropTable(dbName, tableName)
		delete(e.tempTables, tableName)
	}

	err = db.CreateTable(def)
	if err != nil {
		if stmt.IfNotExists {
			return &Result{}, nil
		}
		return nil, mysqlError(1050, "42S01", fmt.Sprintf("Table '%s' already exists", tableName))
	}
	e.Storage.CreateTable(dbName, def)

	// Track temporary tables
	if stmt.Temp {
		e.tempTables[tableName] = true
	}

	// Set AUTO_INCREMENT start value from table options (needs table to exist in storage)
	for _, opt := range stmt.TableSpec.Options {
		if strings.ToUpper(opt.Name) == "AUTO_INCREMENT" {
			if val, err := strconv.ParseInt(opt.Value.Val, 10, 64); err == nil {
				if tbl, err := e.Storage.GetTable(dbName, tableName); err == nil {
					tbl.AutoIncrement.Store(val - 1) // Store val-1 because next insert increments first
					tbl.AIExplicitlySet = true
				}
			}
		}
	}

	// Handle CREATE TABLE (cols...) SELECT ... : insert rows from the SELECT
	if stmt.Select != nil {
		selectSQL := sqlparser.String(stmt.Select)
		// MySQL raises ER_CANT_UPDATE_TABLE_IN_CREATE_TABLE_SELECT when CREATE TABLE
		// with a SELECT ... FOR UPDATE cannot acquire locks (another connection holds them).
		if isSELECTForUpdate(selectSQL) && e.rowLockManager != nil {
			srcTable := explainTableNameFromQuery(selectSQL)
			if srcTable != "" {
				if tbl, tblErr := e.Storage.GetTable(e.CurrentDB, srcTable); tblErr == nil {
					if srcDB2, dbErr2 := e.Catalog.GetDatabase(dbName); dbErr2 == nil {
						if def2, defErr2 := srcDB2.GetTable(srcTable); defErr2 == nil && len(tbl.Rows) > 0 {
							allIndices := make([]int, len(tbl.Rows))
							for i := range tbl.Rows {
								allIndices[i] = i
							}
							if lockErr := e.acquireRowLocksForRows(e.CurrentDB, srcTable, def2, tbl.Rows, allIndices); lockErr != nil {
								e.handleRollbackOnTimeout()
								return nil, mysqlError(1615, "HY000", fmt.Sprintf("Can't update table '%s' while '%s' is being created.", srcTable, tableName))
							}
							// Release the locks immediately; we just needed to verify availability
							e.rowLockManager.ReleaseRowLocks(e.connectionID)
						}
					}
				}
			}
		}
		selResult, selErr := e.Execute(selectSQL)
		if selErr != nil {
			return nil, selErr
		}
		if selResult != nil && selResult.IsResultSet {
			tbl, tblErr := e.Storage.GetTable(dbName, tableName)
			if tblErr == nil {
				// Build the final column list following SELECT column order (MySQL behavior).
				// Columns from the SELECT are placed first (in SELECT order), then any
				// explicitly-defined columns that don't appear in the SELECT.
				// When a SELECT column matches an explicit column def, the explicit def is used.
				explicitColsByName := make(map[string]catalog.ColumnDef, len(def.Columns))
				for _, c := range def.Columns {
					explicitColsByName[strings.ToLower(c.Name)] = c
				}
				var reorderedCols []catalog.ColumnDef
				seenInSelect := make(map[string]bool)
				for _, selCol := range selResult.Columns {
					key := strings.ToLower(selCol)
					seenInSelect[key] = true
					if defCol, ok := explicitColsByName[key]; ok {
						// Use the explicit column definition
						reorderedCols = append(reorderedCols, defCol)
					} else {
						// New column from SELECT — infer type and attributes
						attrs := e.inferColumnAttrs(selectSQL, selCol)
						colType := attrs.colType
						if colType == "" {
							colType = "text"
						}
						newCol := catalog.ColumnDef{
							Name:     selCol,
							Type:     colType,
							Nullable: attrs.nullable,
							Charset:  attrs.charset,
						}
						if attrs.hasDefault {
							newCol.Default = &attrs.defaultVal
						}
						reorderedCols = append(reorderedCols, newCol)
						tbl.AddColumn(selCol, nil)
					}
				}
				// Append any explicit columns not present in the SELECT
				for _, c := range def.Columns {
					if !seenInSelect[strings.ToLower(c.Name)] {
						reorderedCols = append(reorderedCols, c)
						tbl.AddColumn(c.Name, nil)
					}
				}
				def.Columns = reorderedCols
				// Insert select results
				var insertErr error
				for _, selRow := range selResult.Rows {
					row := make(storage.Row)
					for j, selCol := range selResult.Columns {
						if j < len(selRow) {
							row[selCol] = selRow[j]
						}
					}
					if _, err := tbl.Insert(row); err != nil {
						insertErr = err
						break
					}
				}
				if insertErr != nil {
					// On insert failure (e.g. duplicate key), drop the newly created table
					// to match MySQL's behavior of rolling back the CREATE TABLE ... SELECT.
					db.DropTable(tableName)       //nolint:errcheck
					e.Storage.DropTable(dbName, tableName)
					return nil, insertErr
				}
			}
		}
	}

	// Use FromCreate variant so that SHOW INDEX cardinality returns NULL
	// for freshly created tables (no ANALYZE run yet). MySQL behavior.
	e.upsertInnoDBStatsRowsFromCreate(dbName, tableName, e.tableRowCount(dbName, tableName))

	return &Result{}, nil
}

func validateNumericTypeSpec(colType, colName string) error {
	s := strings.ToLower(strings.TrimSpace(colType))
	if fields := strings.Fields(s); len(fields) > 0 {
		s = fields[0]
	}
	base := s
	if i := strings.IndexByte(base, '('); i >= 0 {
		base = base[:i]
	}
	parseMD := func(src, prefix string) (int, int, bool) {
		var m, d int
		if n, err := fmt.Sscanf(src, prefix+"(%d,%d)", &m, &d); err == nil && n == 2 {
			return m, d, true
		}
		if n, err := fmt.Sscanf(src, prefix+"(%d)", &m); err == nil && n == 1 {
			return m, 0, true
		}
		return 0, 0, false
	}

	switch base {
	case "decimal", "numeric":
		m, d, ok := parseMD(s, base)
		if !ok {
			return nil
		}
		if m > 65 {
			return fmt.Errorf("Too-big precision %d specified for '%s'. Maximum is 65.", m, colName)
		}
		if m < d {
			return fmt.Errorf("For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '%s').", colName)
		}
	case "float", "double", "real":
		m, d, ok := parseMD(s, base)
		if !ok {
			return nil
		}
		if m < d {
			return fmt.Errorf("For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '%s').", colName)
		}
	// YEAR validation is handled separately (different error code)
	}
	return nil
}

func (e *Executor) execDropTable(stmt *sqlparser.DropTable) (*Result, error) {
	// DDL causes an implicit COMMIT in MySQL (release row locks, end transaction).
	if e.inTransaction {
		e.execCommit()
	}
	for _, table := range stmt.FromTables {
		tableName := table.Name.String()
		dbName := e.CurrentDB
		if !table.Qualifier.IsEmpty() {
			dbName = table.Qualifier.String()
		}
		// Reject DROP TABLE on information_schema tables.
		if strings.EqualFold(dbName, "information_schema") {
			return nil, mysqlError(1044, "42000", "Access denied for user 'root'@'localhost' to database 'information_schema'")
		}
		// Protect MySQL log tables from DROP when logging is enabled
		if isMySQLLogTable(dbName, tableName) && e.isLogTableLoggingEnabled(tableName) {
			return nil, mysqlError(1580, "HY000", "You cannot 'DROP' a log table if logging is enabled")
		}
		db, err := e.Catalog.GetDatabase(dbName)
		if err != nil {
			if stmt.IfExists {
				if e.sqlNotesEnabled() {
					e.addWarning("Note", 1051, fmt.Sprintf("Unknown table '%s.%s'", dbName, tableName))
				}
				continue
			}
			return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", dbName))
		}
		err = db.DropTable(tableName)
		if err != nil {
			if stmt.IfExists {
				if e.sqlNotesEnabled() {
					e.addWarning("Note", 1051, fmt.Sprintf("Unknown table '%s.%s'", dbName, tableName))
				}
				continue
			}
			return nil, mysqlError(1051, "42S02", fmt.Sprintf("Unknown table '%s.%s'", dbName, tableName))
		}
		e.Storage.DropTable(dbName, tableName)
		e.removeInnoDBStatsRows(dbName, tableName)
		// Clear analysis state for the dropped table.
		if e.tableNeedsAnalyze != nil {
			delete(e.tableNeedsAnalyze, dbName+"."+tableName)
		}
		// If this was a temporary table, restore any permanent table it was shadowing.
		if _, wasTemp := e.tempTables[tableName]; wasTemp {
			if saved, ok := e.tempTableSavedPermanent[tableName]; ok {
				// Restore the permanent table catalog entry and storage.
				if db2, err2 := e.Catalog.GetDatabase(dbName); err2 == nil {
					_ = db2.CreateTable(saved.def)
				}
				e.Storage.RestoreTable(dbName, tableName, saved.table)
				delete(e.tempTableSavedPermanent, tableName)
			}
		}
		// Clean up temp table tracking
		delete(e.tempTables, tableName)
		// Drop triggers associated with this table (MySQL behavior)
		e.dropTriggersForTable(db, tableName)
	}
	return &Result{}, nil
}

// columnDefFromAST converts a vitess ColumnDefinition into our catalog.ColumnDef.
// implicitDefaultForType returns the implicit default value for a NOT NULL column
// that has no explicit DEFAULT clause. MySQL uses the type's zero value:
// 0 for numeric types, "" for string types, "0000-00-00" for date, etc.
func implicitDefaultForType(colType string) interface{} {
	upper := strings.ToUpper(colType)
	// Strip length/precision like INT(11), DECIMAL(10,2), etc.
	base := upper
	if idx := strings.IndexByte(base, '('); idx >= 0 {
		base = base[:idx]
	}
	base = strings.TrimSpace(base)
	// Remove UNSIGNED / ZEROFILL suffix
	base = strings.TrimSuffix(base, " UNSIGNED")
	base = strings.TrimSuffix(base, " ZEROFILL")
	base = strings.TrimSpace(base)

	switch base {
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT", "BIT":
		return int64(0)
	case "FLOAT", "DOUBLE", "REAL":
		return float64(0)
	case "DECIMAL", "NUMERIC", "DEC", "FIXED":
		return "0"
	case "DATE":
		return "0000-00-00"
	case "TIME":
		return "00:00:00"
	case "DATETIME", "TIMESTAMP":
		return "0000-00-00 00:00:00"
	case "YEAR":
		return "0000"
	case "ENUM":
		return ""
	case "SET":
		return ""
	default:
		// All string/blob/binary types default to empty string
		return ""
	}
}

func columnDefFromAST(col *sqlparser.ColumnDefinition) catalog.ColumnDef {
	colDef := catalog.ColumnDef{
		Name:     col.Name.String(),
		Type:     buildColumnTypeString(col.Type, ""),
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
			// Strip surrounding quotes from default values (vitess adds them)
			if len(defStr) >= 2 && defStr[0] == '\'' && defStr[len(defStr)-1] == '\'' {
				defStr = defStr[1 : len(defStr)-1]
			}
			colDef.Default = &defStr
		}
		if col.Type.Options.OnUpdate != nil {
			onUpdateStr := strings.ToUpper(sqlparser.String(col.Type.Options.OnUpdate))
			if strings.Contains(onUpdateStr, "CURRENT_TIMESTAMP") || strings.Contains(onUpdateStr, "NOW") {
				colDef.OnUpdateCurrentTimestamp = true
			}
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
	dbName := e.CurrentDB
	if !stmt.Table.Qualifier.IsEmpty() {
		dbName = stmt.Table.Qualifier.String()
	}
	// Reject DDL on information_schema tables.
	if strings.EqualFold(dbName, "information_schema") {
		return nil, mysqlError(1044, "42000", "Access denied for user 'root'@'localhost' to database 'information_schema'")
	}

	// Reject DDL on performance_schema tables
	if strings.EqualFold(dbName, "performance_schema") {
		return nil, mysqlError(1044, "42000", "Access denied for user 'root'@'localhost' to database 'performance_schema'")
	}

	tableName := stmt.Table.Name.String()

	// Protect MySQL log tables from ALTER when logging is enabled
	if isMySQLLogTable(dbName, tableName) && e.isLogTableLoggingEnabled(tableName) {
		return nil, mysqlError(1580, "HY000", "You cannot 'ALTER' a log table if logging is enabled")
	}

	// Check engine restrictions (disabled engines, unknown engines, log table constraints)
	for _, opt := range stmt.AlterOptions {
		if tblOpts, ok := opt.(sqlparser.TableOptions); ok {
			for _, to := range tblOpts {
				if strings.EqualFold(to.Name, "ENGINE") {
					engineVal := strings.ToUpper(tableOptionString(to))
					// Check if engine exists
					switch engineVal {
					case "INNODB", "MYISAM", "CSV", "ARCHIVE", "BLACKHOLE", "HEAP", "MEMORY",
						"MERGE", "MRG_MYISAM", "NDB", "NDBCLUSTER", "EXAMPLE",
						"PERFORMANCE_SCHEMA":
						// Known engines
					case "FEDERATED":
						// FEDERATED is compiled in but disabled
						return nil, mysqlError(1286, "42000", fmt.Sprintf("Unknown storage engine '%s'", tableOptionString(to)))
					default:
						return nil, mysqlError(1286, "42000", fmt.Sprintf("Unknown storage engine '%s'", tableOptionString(to)))
					}
					// MEMORY and similar engines cannot be used for log tables
					if isMySQLLogTable(dbName, tableName) {
						if engineVal == "MEMORY" || engineVal == "HEAP" {
							return nil, mysqlError(1579, "HY000", "This storage engine cannot be used for log tables")
						}
					}
				}
			}
		}
	}

	db, err := e.Catalog.GetDatabase(dbName)
	if err != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", dbName))
	}

	// Ensure the storage table exists.
	tbl, err := e.Storage.GetTable(dbName, tableName)
	if err != nil {
		return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", dbName, tableName))
	}

	// Pre-check: ALGORITHM=INPLACE rejection for operations that require COPY
	{
		isInplace := false
		hasStoredGcolAdd := false
		for _, opt := range stmt.AlterOptions {
			switch av := opt.(type) {
			case sqlparser.AlgorithmValue:
				if strings.EqualFold(string(av), "INPLACE") {
					isInplace = true
				}
			case *sqlparser.AddColumns:
				for _, col := range av.Columns {
					if col.Type.Options != nil && col.Type.Options.As != nil {
						colTypeStr := strings.ToUpper(sqlparser.String(col.Type))
						if strings.Contains(colTypeStr, "STORED") {
							hasStoredGcolAdd = true
						}
					}
				}
			}
		}
		if isInplace && hasStoredGcolAdd {
			return nil, mysqlError(1845, "0A000", "ALGORITHM=INPLACE is not supported. Reason: Cannot change column type INPLACE. Try ALGORITHM=COPY.")
		}
	}

	// Pre-check: AUTO_INCREMENT columns must have an accompanying index
	{
		autoIncrCols := map[string]bool{}
		indexedCols := map[string]bool{}
		// Collect existing indexed columns
		tableDef, _ := db.GetTable(tableName)
		if tableDef != nil {
			for _, pk := range tableDef.PrimaryKey {
				indexedCols[strings.ToLower(stripPrefixLengthFromCol(pk))] = true
			}
			for _, idx := range tableDef.Indexes {
				for _, c := range idx.Columns {
					indexedCols[strings.ToLower(stripPrefixLengthFromCol(c))] = true
				}
			}
		}
		for _, opt := range stmt.AlterOptions {
			switch op := opt.(type) {
			case *sqlparser.AddColumns:
				for _, col := range op.Columns {
					if col.Type.Options != nil && col.Type.Options.Autoincrement {
						autoIncrCols[strings.ToLower(col.Name.String())] = true
					}
					if col.Type.Options != nil && col.Type.Options.KeyOpt != sqlparser.ColKeyNone {
						indexedCols[strings.ToLower(col.Name.String())] = true
					}
				}
			case *sqlparser.AddIndexDefinition:
				for _, idxCol := range op.IndexDefinition.Columns {
					indexedCols[strings.ToLower(idxCol.Column.String())] = true
				}
			}
		}
		for col := range autoIncrCols {
			if !indexedCols[col] {
				return nil, mysqlError(1075, "42000", "Incorrect table definition; there can be only one auto column and it must be defined as a key")
			}
		}
	}

	for _, opt := range stmt.AlterOptions {
		switch op := opt.(type) {

		case *sqlparser.AddColumns:
			for _, col := range op.Columns {
				// Reject generated columns with DEFAULT, AUTO_INCREMENT, or ON UPDATE
				if col.Type.Options != nil && col.Type.Options.As != nil {
					if col.Type.Options.Default != nil {
						return nil, mysqlError(1221, "HY000", "Incorrect usage of DEFAULT and generated column")
					}
					if col.Type.Options.Autoincrement {
						return nil, mysqlError(1221, "HY000", "Incorrect usage of AUTO_INCREMENT and generated column")
					}
					if col.Type.Options.OnUpdate != nil {
						return nil, mysqlError(1221, "HY000", "Incorrect usage of ON UPDATE and generated column")
					}
				}
				// Check for multiple primary key before virtual PK check
				if col.Type.Options != nil && (col.Type.Options.KeyOpt == 1 || col.Type.Options.KeyOpt == 6) {
					// KeyOpt 1 = PRIMARY KEY, 6 = KEY
					if col.Type.Options.KeyOpt == 1 {
						// Check if table already has a primary key
						tableDef, _ := db.GetTable(tableName)
						if tableDef != nil && len(tableDef.PrimaryKey) > 0 {
							return nil, mysqlError(1068, "42000", "Multiple primary key defined")
						}
					}
				}
				// Check virtual generated column with KEY/PRIMARY KEY
				if col.Type.Options != nil && col.Type.Options.As != nil &&
					col.Type.Options.Storage != sqlparser.StoredStorage &&
					(col.Type.Options.KeyOpt == 1 || col.Type.Options.KeyOpt == 6) {
					return nil, mysqlError(3106, "HY000", "'Defining a virtual generated column as primary key' is not supported for generated columns.")
				}
				// Check unsupported storage engine for generated columns
				if col.Type.Options != nil && col.Type.Options.As != nil {
					tableEngine := ""
					if tableDef, tdErr := db.GetTable(tableName); tdErr == nil {
						tableEngine = strings.ToUpper(tableDef.Engine)
					}
					if tableEngine == "" {
						// Check session default_storage_engine
						if e.sessionScopeVars != nil || e.globalScopeVars != nil {
							if eng, ok := e.getSysVar("default_storage_engine"); ok {
								tableEngine = strings.ToUpper(eng)
							}
						}
					}
					if tableEngine == "MEMORY" || tableEngine == "MERGE" || tableEngine == "MRG_MYISAM" || tableEngine == "BLACKHOLE" || tableEngine == "ARCHIVE" {
						return nil, mysqlError(3106, "HY000", "'Specified storage engine' is not supported for generated columns.")
					}
				}
				colDef := columnDefFromAST(col)
				// Check for invalid zero date/datetime/timestamp defaults in strict mode
				if colDef.Default != nil && e.isStrictMode() {
					colTypeUpper := strings.ToUpper(strings.TrimSpace(col.Type.Type))
					hasNoZeroDate := strings.Contains(e.sqlMode, "NO_ZERO_DATE") || strings.Contains(e.sqlMode, "TRADITIONAL")
					hasNoZeroInDate := strings.Contains(e.sqlMode, "NO_ZERO_IN_DATE") || strings.Contains(e.sqlMode, "TRADITIONAL")
					switch colTypeUpper {
					case "TIMESTAMP", "DATETIME", "DATE":
						// NO_ZERO_DATE and NO_ZERO_IN_DATE both reject zero-component date defaults
						if (hasNoZeroDate || hasNoZeroInDate) && isZeroInDateValue(*colDef.Default) {
							return nil, mysqlError(1067, "42000", fmt.Sprintf("Invalid default value for '%s'", colDef.Name))
						}
					}
				}
				// Check column comment length in strict/traditional mode (MySQL max is 1024 characters)
				if mysqlCharLen(colDef.Comment) > 1024 && e.isStrictMode() {
					return nil, mysqlError(1629, "HY000", fmt.Sprintf("Comment for field '%s' is too long (max = 1024)", colDef.Name))
				}
				// Truncate comment to 1024 characters in non-strict mode
				if mysqlCharLen(colDef.Comment) > 1024 {
					colDef.Comment = mysqlTruncateChars(colDef.Comment, 1024)
				}
				position := ""
				afterCol := ""
				if op.First {
					position = "FIRST"
				} else if op.After != nil {
					position = "AFTER"
					afterCol = op.After.Name.String()
				}
				if addErr := db.AddColumnAt(tableName, colDef, position, afterCol); addErr != nil {
					if strings.Contains(addErr.Error(), "already exists") {
						return nil, mysqlError(1060, "42S21", fmt.Sprintf("Duplicate column name '%s'", colDef.Name))
					}
					return nil, addErr
				}
				// Determine the default value to fill in existing rows.
				genExpr := generatedColumnExpr(colDef.Type)
				if genExpr != "" {
					// For generated columns, compute values for existing rows
					tbl.AddColumn(colDef.Name, nil)
					tbl.Mu.Lock()
					for i := range tbl.Rows {
						v, err := e.evalGeneratedColumnExpr(genExpr, tbl.Rows[i])
						if err != nil {
							tbl.Mu.Unlock()
							// If evaluation fails (e.g., out of range), rollback the column addition
							db.DropColumn(tableName, colDef.Name)
							tbl.DropColumn(colDef.Name)
							return nil, err
						}
						tbl.Rows[i][colDef.Name] = v
					}
					tbl.Mu.Unlock()
				} else if colDef.AutoIncrement {
					// For AUTO_INCREMENT columns, fill existing rows with sequential values
					tbl.AddColumn(colDef.Name, nil)
					tbl.Mu.Lock()
					for i := range tbl.Rows {
						autoVal := tbl.AutoIncrement.Add(1)
						tbl.Rows[i][colDef.Name] = autoVal
					}
					tbl.Mu.Unlock()
				} else {
					var defVal interface{}
					if colDef.Default != nil {
						// Parse the default string as a literal if possible.
						defVal = *colDef.Default
					} else if !colDef.Nullable {
						// NOT NULL column without explicit DEFAULT gets the type's zero value
						defVal = implicitDefaultForType(colDef.Type)
					}
					tbl.AddColumn(colDef.Name, defVal)
				}
			}

		case *sqlparser.DropColumn:
			colName := op.Name.Name.String()
			// Check if this would leave the table with no columns.
			// Count net effect of all DROP/ADD COLUMN ops in this ALTER TABLE.
			tableDef, _ := db.GetTable(tableName)
			if tableDef != nil {
				netCols := len(tableDef.Columns)
				for _, altOpt := range stmt.AlterOptions {
					switch altOp := altOpt.(type) {
					case *sqlparser.DropColumn:
						netCols--
					case *sqlparser.AddColumns:
						netCols += len(altOp.Columns)
					}
				}
				if netCols <= 0 {
					return nil, mysqlError(1090, "42000", "You can't delete all columns with ALTER TABLE; use DROP TABLE instead")
				}
			}
			if dropErr := db.DropColumn(tableName, colName); dropErr != nil {
				return nil, dropErr
			}
			tbl.DropColumn(colName)

		case *sqlparser.ModifyColumn:
			colDef := columnDefFromAST(op.NewColDefinition)
			if mysqlCharLen(colDef.Comment) > 1024 {
				if e.isStrictMode() {
					return nil, mysqlError(1629, "HY000", fmt.Sprintf("Comment for field '%s' is too long (max = 1024)", colDef.Name))
				}
				colDef.Comment = mysqlTruncateChars(colDef.Comment, 1024)
			}
			// Check for STORED<->VIRTUAL change on generated columns
			if tableDef, tdErr := db.GetTable(tableName); tdErr == nil {
				for _, existCol := range tableDef.Columns {
					if strings.EqualFold(existCol.Name, colDef.Name) {
						oldIsGen := isGeneratedColumnType(existCol.Type)
						newIsGen := isGeneratedColumnType(colDef.Type)
						if oldIsGen && newIsGen {
							oldStored := strings.Contains(strings.ToUpper(existCol.Type), "STORED")
							newStored := strings.Contains(strings.ToUpper(colDef.Type), "STORED")
							if oldStored != newStored {
								return nil, mysqlError(3106, "HY000", "'Changing the STORED status' is not supported for generated columns.")
							}
						}
						break
					}
				}
			}
			if modErr := db.ModifyColumn(tableName, colDef); modErr != nil {
				return nil, modErr
			}
			// Recompute generated column values if expression changed
			if genExpr := generatedColumnExpr(colDef.Type); genExpr != "" {
				tbl.Lock()
				for i := range tbl.Rows {
					if v, err := e.evalGeneratedColumnExpr(genExpr, tbl.Rows[i]); err == nil {
						tbl.Rows[i][colDef.Name] = v
					}
				}
				tbl.Unlock()
			} else {
				tbl.Lock()
				for i := range tbl.Rows {
					if cur, ok := tbl.Rows[i][colDef.Name]; ok {
						tbl.Rows[i][colDef.Name] = coerceValueForColumnType(colDef, cur)
					}
				}
				tbl.Unlock()
			}

		case *sqlparser.ChangeColumn:
			oldName := op.OldColumn.Name.String()
			colDef := columnDefFromAST(op.NewColDefinition)
			if mysqlCharLen(colDef.Comment) > 1024 {
				if e.isStrictMode() {
					return nil, mysqlError(1629, "HY000", fmt.Sprintf("Comment for field '%s' is too long (max = 1024)", colDef.Name))
				}
				colDef.Comment = mysqlTruncateChars(colDef.Comment, 1024)
			}
			if chgErr := db.ChangeColumn(tableName, oldName, colDef); chgErr != nil {
				return nil, chgErr
			}
			// Rename the key in all existing rows if the column name changed.
			if oldName != colDef.Name {
				tbl.RenameColumn(oldName, colDef.Name)
			}
			// Recompute generated column values if expression changed
			if genExpr := generatedColumnExpr(colDef.Type); genExpr != "" {
				tbl.Lock()
				for i := range tbl.Rows {
					if v, err := e.evalGeneratedColumnExpr(genExpr, tbl.Rows[i]); err == nil {
						tbl.Rows[i][colDef.Name] = v
					}
				}
				tbl.Unlock()
			} else {
				tbl.Lock()
				for i := range tbl.Rows {
					if cur, ok := tbl.Rows[i][colDef.Name]; ok {
						tbl.Rows[i][colDef.Name] = coerceValueForColumnType(colDef, cur)
					}
				}
				tbl.Unlock()
			}

		case *sqlparser.AddIndexDefinition:
			// Reject the reserved InnoDB clustered index name.
			if strings.EqualFold(op.IndexDefinition.Info.Name.String(), "GEN_CLUST_INDEX") {
				return nil, mysqlError(1280, "42000", "Incorrect index name 'GEN_CLUST_INDEX'")
			}
			// Validate that all index columns exist in the table
			tableDef, tdErr := db.GetTable(tableName)
			if tdErr == nil {
				for _, idxCol := range op.IndexDefinition.Columns {
					if err := validateArrayIndexExpression(idxCol.Expression); err != nil {
						return nil, err
					}
					if hasArrayCastExpr(idxCol.Expression) && tableDef.Engine != "" && !strings.EqualFold(tableDef.Engine, "INNODB") {
						return nil, mysqlError(1178, "42000", "The storage engine for the table doesn't support check")
					}
					if idxCol.Expression != nil {
						continue
					}
					colName := idxCol.Column.String()
					found := false
					for _, col := range tableDef.Columns {
						if strings.EqualFold(col.Name, colName) {
							found = true
							break
						}
					}
					if !found {
						return nil, mysqlError(1072, "42000", fmt.Sprintf("Key column '%s' doesn't exist in table", colName))
					}
				}
			}
			// Store index definition so SHOW CREATE TABLE can display it.
			var idxCols []string
			var idxOrders []string
			for _, idxCol := range op.IndexDefinition.Columns {
				colStr := strings.ToLower(idxCol.Column.String())
				if idxCol.Expression != nil {
					colStr = fmt.Sprintf("(%s)", strings.TrimSpace(sqlparser.String(idxCol.Expression)))
				} else if idxCol.Length != nil {
					colStr += fmt.Sprintf("(%d)", *idxCol.Length)
				}
				idxCols = append(idxCols, colStr)
				if idxCol.Direction == sqlparser.DescOrder {
					idxOrders = append(idxOrders, "DESC")
				} else {
					idxOrders = append(idxOrders, "")
				}
			}
			// Check for duplicate column names in the index
			{
				seen := make(map[string]bool, len(idxCols))
				for _, c := range idxCols {
					if strings.HasPrefix(c, "(") {
						continue // functional index expression
					}
					base := strings.ToLower(stripPrefixLengthFromCol(c))
					if seen[base] {
						return nil, mysqlError(1060, "42S21", fmt.Sprintf("Duplicate column name '%s'", stripPrefixLengthFromCol(c)))
					}
					seen[base] = true
				}
			}
			// Check max key parts per index (ER_TOO_MANY_KEY_PARTS = 1070)
			if len(idxCols) > 16 {
				return nil, mysqlError(1070, "42000", "Too many key parts specified; max 16 parts allowed")
			}
			// Check max indexes per table (ER_TOO_MANY_KEYS = 1069)
			if tdErr == nil {
				totalKeys := len(tableDef.Indexes)
				if len(tableDef.PrimaryKey) > 0 {
					totalKeys++
				}
				// Adding one more index
				totalKeys++
				if totalKeys > 64 {
					return nil, mysqlError(1069, "42000", "Too many keys specified; max 64 keys allowed")
				}
			}
			// Check key prefix length limits (ER_TOO_LONG_KEY = 1071)
			{
				tableRowFormat := ""
				if tableDef, tdErr2 := db.GetTable(tableName); tdErr2 == nil {
					tableRowFormat = strings.ToUpper(tableDef.RowFormat)
				}
				for _, idxCol := range op.IndexDefinition.Columns {
					if idxCol.Length != nil {
						prefixLen := *idxCol.Length
						maxPrefixLen := 3072 // DYNAMIC default
						if tableRowFormat == "COMPACT" || tableRowFormat == "REDUNDANT" {
							maxPrefixLen = 767
						}
						if prefixLen > maxPrefixLen {
							return nil, mysqlError(1071, "42000", fmt.Sprintf("Specified key was too long; max key length is %d bytes", maxPrefixLen))
						}
					}
				}
			}
			isUnique := op.IndexDefinition.Info.Type == sqlparser.IndexTypeUnique
			isPrimary := op.IndexDefinition.Info.Type == sqlparser.IndexTypePrimary
			idxType := ""
			if op.IndexDefinition.Info.Type == sqlparser.IndexTypeFullText {
				idxType = "FULLTEXT"
			} else if op.IndexDefinition.Info.Type == sqlparser.IndexTypeSpatial {
				idxType = "SPATIAL"
				// SPATIAL indexes require geometrical column types
				tableDef, _ := db.GetTable(tableName)
				if tableDef != nil {
					for _, ic := range idxCols {
						icName := stripPrefixLengthFromCol(ic)
						isGeo := false
						for _, col := range tableDef.Columns {
							if strings.EqualFold(col.Name, icName) {
								colUpper := strings.ToUpper(col.Type)
								if strings.Contains(colUpper, "GEOMETRY") || strings.Contains(colUpper, "POINT") ||
									strings.Contains(colUpper, "LINESTRING") || strings.Contains(colUpper, "POLYGON") ||
									strings.Contains(colUpper, "MULTIPOINT") || strings.Contains(colUpper, "MULTILINESTRING") ||
									strings.Contains(colUpper, "MULTIPOLYGON") || strings.Contains(colUpper, "GEOMETRYCOLLECTION") {
									isGeo = true
								}
								break
							}
						}
						if !isGeo {
							return nil, mysqlError(1687, "42000", "A SPATIAL index may only contain a geometrical type column")
						}
					}
				}
			}
			idxName := op.IndexDefinition.Info.Name.String()
			if idxName == "" {
				if cn := op.IndexDefinition.Info.ConstraintName.String(); cn != "" {
					idxName = cn
				} else if len(idxCols) > 0 {
					idxName = stripPrefixLengthFromCol(idxCols[0])
				}
			}
			// Check for USING method and COMMENT
			usingMethod := ""
			idxComment := ""
			idxInvisible := false
			for _, opt := range op.IndexDefinition.Options {
				if opt.Name == "USING" {
					usingMethod = strings.ToUpper(opt.String)
				}
				if strings.ToUpper(opt.Name) == "COMMENT" {
					if opt.Value != nil {
						idxComment = opt.Value.Val
					} else {
						idxComment = opt.String
					}
				}
				if strings.EqualFold(opt.Name, "VISIBLE") {
					idxInvisible = false
				} else if strings.EqualFold(opt.Name, "INVISIBLE") {
					idxInvisible = true
				}
			}
			// Validate that PRIMARY KEY cannot be INVISIBLE
			if idxInvisible && isPrimary {
				return nil, mysqlError(3522, "HY000", "A primary key index cannot be invisible")
			}
			// Check for duplicate values in existing data when adding UNIQUE/PRIMARY index
			if isUnique || isPrimary {
				// Extract prefix lengths from idxCols (e.g., "f1(4)" -> 4)
				baseCols := make([]string, len(idxCols))
				prefixLens := make([]int, len(idxCols))
				for ci, c := range idxCols {
					baseCols[ci] = stripPrefixLengthFromCol(c)
					// Parse prefix length if present
					if idx := strings.Index(c, "("); idx >= 0 {
						end := strings.Index(c, ")")
						if end > idx {
							if pl, err := strconv.Atoi(c[idx+1 : end]); err == nil {
								prefixLens[ci] = pl
							}
						}
					}
				}
				seen := make(map[string]bool)
				for _, row := range tbl.Rows {
					keyParts := make([]string, len(baseCols))
					hasNull := false
					for ci, c := range baseCols {
						v := rowValueByColumnName(row, c)
						if v == nil {
							keyParts[ci] = "NULL"
							hasNull = true
						} else {
							s := fmt.Sprintf("%v", v)
							// Apply prefix length and trim trailing nulls/spaces
							if prefixLens[ci] > 0 && len(s) > prefixLens[ci] {
								s = s[:prefixLens[ci]]
							}
							s = strings.TrimRight(s, "\x00 ")
							keyParts[ci] = s
						}
					}
					if hasNull {
						continue // rows with any NULL in the key don't violate uniqueness
					}
					key := strings.Join(keyParts, "-")
					if seen[key] {
						return nil, mysqlError(1062, "23000", fmt.Sprintf("Duplicate entry '%s' for key '%s'", strings.Join(keyParts, "-"), idxName))
					}
					seen[key] = true
				}
			}
			// In strict mode with NO_ZERO_DATE or TRADITIONAL, validate existing rows
			// for zero date values in indexed DATETIME/DATE/TIMESTAMP columns.
			if e.isStrictMode() && (strings.Contains(e.sqlMode, "NO_ZERO_DATE") || strings.Contains(e.sqlMode, "TRADITIONAL")) {
				if tdErr == nil {
					// Build a map of indexed column names to their types
					type idxColInfo struct {
						name    string
						colType string
					}
					var dtCols []idxColInfo
					for _, ic := range idxCols {
						baseName := stripPrefixLengthFromCol(ic)
						for _, col := range tableDef.Columns {
							if strings.EqualFold(col.Name, baseName) {
								colUpper := strings.ToUpper(col.Type)
								if strings.Contains(colUpper, "DATETIME") || strings.Contains(colUpper, "DATE") || strings.Contains(colUpper, "TIMESTAMP") {
									dtCols = append(dtCols, idxColInfo{name: baseName, colType: col.Type})
								}
								break
							}
						}
					}
					if len(dtCols) > 0 {
						tbl.Mu.RLock()
						for rowIdx, row := range tbl.Rows {
							for _, dtCol := range dtCols {
								v := rowValueByColumnName(row, dtCol.name)
								if v == nil {
									continue
								}
								vs := fmt.Sprintf("%v", v)
								// Zero date values: "0000-00-00", "0000-00-00 00:00:00"
								if vs == "0000-00-00" || vs == "0000-00-00 00:00:00" {
									tbl.Mu.RUnlock()
									colType := strings.ToUpper(dtCol.colType)
									var displayVal string
									if strings.Contains(colType, "DATE") && !strings.Contains(colType, "DATETIME") {
										displayVal = "0000-00-00"
									} else {
										displayVal = "0000-00-00 00:00:00"
									}
									return nil, mysqlError(1292, "22007", fmt.Sprintf("Incorrect datetime value: '%s' for column '%s' at row %d", displayVal, dtCol.name, rowIdx+1))
								}
							}
						}
						tbl.Mu.RUnlock()
					}
				}
			}
			if isPrimary {
				// Map lowercase idxCols to actual column names (preserving case)
				actualPKCols := make([]string, len(idxCols))
				for i, idxCol := range idxCols {
					actualPKCols[i] = idxCol // default to lowercase
					if tableDef, tdErr2 := db.GetTable(tableName); tdErr2 == nil {
						for _, col := range tableDef.Columns {
							if strings.EqualFold(col.Name, idxCol) {
								actualPKCols[i] = col.Name
								break
							}
						}
					}
				}
				db.SetPrimaryKey(tableName, actualPKCols)
				tableDef, tdErr := db.GetTable(tableName)
				if tdErr == nil && strings.EqualFold(tableDef.Engine, "InnoDB") {
					orderCollation := effectiveTableCollation(tableDef)
					tbl.Mu.Lock()
					sort.SliceStable(tbl.Rows, func(i, j int) bool {
						for _, colName := range idxCols {
							vi := rowValueByColumnName(tbl.Rows[i], colName)
							vj := rowValueByColumnName(tbl.Rows[j], colName)
							cmp := compareByCollation(vi, vj, orderCollation)
							if cmp != 0 {
								return cmp < 0
							}
						}
						return false
					})
					tbl.Mu.Unlock()
				}
			} else {
				// MySQL returns error for index comments > 1024 in strict/TRADITIONAL mode;
				// in non-strict mode it truncates silently.
				if mysqlCharLen(idxComment) > 1024 {
					if e.isStrictMode() {
						return nil, mysqlError(1688, "HY000", fmt.Sprintf("Comment for index '%s' is too long (max = 1024)", idxName))
					}
					idxComment = mysqlTruncateChars(idxComment, 1024)
				}
				db.AddIndex(tableName, catalog.IndexDef{
					Name:      idxName,
					Columns:   idxCols,
					Orders:    idxOrders,
					Unique:    isUnique,
					Type:      idxType,
					Using:     usingMethod,
					Comment:   idxComment,
					Invisible: idxInvisible,
				})
			}

		case *sqlparser.AddConstraintDefinition:
			// Create implicit index for foreign key constraints (MySQL auto-creates these).
			if fkDef, ok := op.ConstraintDefinition.Details.(*sqlparser.ForeignKeyDefinition); ok {
				// Reject FK on virtual generated columns and certain FK actions on stored generated columns
				tableDef0, _ := db.GetTable(tableName)
				if tableDef0 != nil {
					for _, srcCol := range fkDef.Source {
						srcName := srcCol.String()
						for _, col := range tableDef0.Columns {
							if strings.EqualFold(col.Name, srcName) && isGeneratedColumnType(col.Type) {
								if !strings.Contains(strings.ToUpper(col.Type), "STORED") {
									// MySQL includes the FK name and column name
									altFkName := op.ConstraintDefinition.Name.String()
									if altFkName == "" {
										altFkName = tableName + "_ibfk_1"
									}
									return nil, mysqlError(3108, "HY000", fmt.Sprintf("Foreign key '%s' uses virtual column '%s' which is not supported.", altFkName, srcName))
								}
								if fkDef.ReferenceDefinition != nil {
									if fkDef.ReferenceDefinition.OnUpdate == sqlparser.SetNull {
										return nil, mysqlError(3104, "HY000", "Cannot define foreign key with ON UPDATE SET NULL clause on a generated column.")
									}
									if fkDef.ReferenceDefinition.OnUpdate == sqlparser.Cascade {
										return nil, mysqlError(3104, "HY000", "Cannot define foreign key with ON UPDATE CASCADE clause on a generated column.")
									}
									if fkDef.ReferenceDefinition.OnDelete == sqlparser.SetNull {
										return nil, mysqlError(3104, "HY000", "Cannot define foreign key with ON DELETE SET NULL clause on a generated column.")
									}
								}
							}
						}
					}
				}
				var fkCols []string
				for _, col := range fkDef.Source {
					fkCols = append(fkCols, col.String())
				}
				if len(fkCols) > 0 {
					idxName := op.ConstraintDefinition.Name.String()
					if idxName == "" {
						idxName = fkCols[0]
					}
					// Check if an index already covers these columns
					covered := false
					tableDef, _ := db.GetTable(tableName)
					if tableDef != nil {
						for _, idx := range tableDef.Indexes {
							if len(idx.Columns) >= len(fkCols) {
								match := true
								for k, fc := range fkCols {
									if !strings.EqualFold(idx.Columns[k], fc) {
										match = false
										break
									}
								}
								if match {
									covered = true
									break
								}
							}
						}
					}
					if !covered {
						db.AddIndex(tableName, catalog.IndexDef{
							Name:    idxName,
							Columns: fkCols,
							Orders:  make([]string, len(fkCols)),
						})
					}
				}
				// Store the FK constraint in the table definition
				fkName := op.ConstraintDefinition.Name.String()
				if fkName == "" {
					fkName = tableName + "_ibfk_1"
				}
				fk := catalog.ForeignKeyDef{
					Name:    fkName,
					Columns: fkCols,
				}
				if fkDef.ReferenceDefinition != nil {
					fk.ReferencedTable = fkDef.ReferenceDefinition.ReferencedTable.Name.String()
					for _, col := range fkDef.ReferenceDefinition.ReferencedColumns {
						fk.ReferencedColumns = append(fk.ReferencedColumns, col.String())
					}
					fk.OnDelete = referenceActionToString(fkDef.ReferenceDefinition.OnDelete)
					fk.OnUpdate = referenceActionToString(fkDef.ReferenceDefinition.OnUpdate)
				}
				if td, _ := db.GetTable(tableName); td != nil {
					td.ForeignKeys = append(td.ForeignKeys, fk)
				}
			}

		case *sqlparser.DropKey:
			if op.Type == sqlparser.PrimaryKeyType {
				db.DropPrimaryKey(tableName)
			} else if op.Type == sqlparser.ForeignKeyType {
				// Remove FK constraint from table definition
				fkName := op.Name.String()
				if td, _ := db.GetTable(tableName); td != nil {
					newFKs := make([]catalog.ForeignKeyDef, 0, len(td.ForeignKeys))
					for _, fk := range td.ForeignKeys {
						if !strings.EqualFold(fk.Name, fkName) {
							newFKs = append(newFKs, fk)
						}
					}
					td.ForeignKeys = newFKs
				}
			} else if op.Type == sqlparser.CheckKeyType {
				// CHECK constraints: silently accept DROP.
			} else {
				idxName := op.Name.String()
				if err := db.DropIndex(tableName, idxName); err != nil {
					return nil, mysqlError(1091, "42000", fmt.Sprintf("Can't DROP '%s'; check that column/key exists", idxName))
				}
			}

		case sqlparser.TableOptions:
			for _, to := range op {
				switch strings.ToUpper(to.Name) {
				case "ENGINE":
					tableDef, _ := db.GetTable(tableName)
					if tableDef != nil {
						tableDef.Engine = tableOptionString(to)
					}
				case "AUTO_INCREMENT":
					if val, err := strconv.ParseInt(to.Value.Val, 10, 64); err == nil {
						tbl.AutoIncrement.Store(val - 1)
						tbl.AIExplicitlySet = true
					}
				case "COMMENT":
					comment := to.Value.Val
					if mysqlCharLen(comment) > 2048 {
						if e.isStrictMode() {
							return nil, mysqlError(1628, "HY000", fmt.Sprintf("Comment for table '%s' is too long (max = 2048)", tableName))
						}
						comment = mysqlTruncateChars(comment, 2048)
					}
					tableDef, _ := db.GetTable(tableName)
					if tableDef != nil {
						tableDef.Comment = comment
					}
				case "ROW_FORMAT":
					tableDef, _ := db.GetTable(tableName)
					if tableDef != nil {
						tableDef.RowFormat = strings.ToUpper(tableOptionString(to))
					}
				case "KEY_BLOCK_SIZE":
					tableDef, _ := db.GetTable(tableName)
					if tableDef != nil {
						tableDef.KeyBlockSize = parseTableOptionInt(to)
					}
				case "STATS_PERSISTENT":
					tableDef, _ := db.GetTable(tableName)
					if tableDef != nil {
						tableDef.StatsPersistent = parseTableOptionInt(to)
					}
				case "STATS_AUTO_RECALC":
					tableDef, _ := db.GetTable(tableName)
					if tableDef != nil {
						tableDef.StatsAutoRecalc = parseTableOptionInt(to)
					}
				case "STATS_SAMPLE_PAGES":
					tableDef, _ := db.GetTable(tableName)
					if tableDef != nil {
						tableDef.StatsSamplePages = parseTableOptionInt(to)
					}
				case "MAX_ROWS":
					tableDef, _ := db.GetTable(tableName)
					if tableDef != nil {
						tableDef.MaxRows = parseTableOptionUint64Clamped(to)
					}
				case "MIN_ROWS", "AVG_ROW_LENGTH":
					// Accepted but not stored/displayed
				case "INSERT_METHOD":
					tableDef, _ := db.GetTable(tableName)
					if tableDef != nil {
						tableDef.InsertMethod = strings.ToUpper(tableOptionString(to))
					}
				}
			}

		case *sqlparser.AlterColumn:
			colName := op.Column.Name.String()
			tableDef, _ := db.GetTable(tableName)
			if tableDef != nil {
				for i, col := range tableDef.Columns {
					if col.Name == colName {
						if op.DropDefault {
							tableDef.Columns[i].Default = nil
							tableDef.Columns[i].DefaultDropped = true
						} else if op.DefaultVal != nil {
							defStr := sqlparser.String(op.DefaultVal)
							defStr = strings.Trim(defStr, "'")
							tableDef.Columns[i].Default = &defStr
							tableDef.Columns[i].DefaultDropped = false
						}
						break
					}
				}
			}

		case *sqlparser.AlterIndex:
			// ALTER TABLE ... ALTER INDEX <name> VISIBLE/INVISIBLE
			idxName := op.Name.String()
			// PRIMARY KEY cannot be made invisible
			if op.Invisible && strings.EqualFold(idxName, "PRIMARY") {
				return nil, mysqlError(3522, "HY000", "A primary key index cannot be invisible")
			}
			tableDef, _ := db.GetTable(tableName)
			if tableDef != nil {
				for i, idx := range tableDef.Indexes {
					if strings.EqualFold(idx.Name, idxName) {
						// If making invisible: check if this is an implicit primary key
						// (first NOT NULL UNIQUE when table has no explicit primary key)
						if op.Invisible && !hasPrimaryKey(tableDef) && isFirstNotNullUnique(tableDef, idx) {
							return nil, mysqlError(3522, "HY000", "A primary key index cannot be invisible")
						}
						tableDef.Indexes[i].Invisible = op.Invisible
						break
					}
				}
			}

		case *sqlparser.RenameIndex:
			// ALTER TABLE ... RENAME INDEX <old> TO <new>
			oldName := op.OldName.String()
			newName := op.NewName.String()
			tableDef, _ := db.GetTable(tableName)
			if tableDef != nil {
				for i, idx := range tableDef.Indexes {
					if strings.EqualFold(idx.Name, oldName) {
						tableDef.Indexes[i].Name = newName
						break
					}
				}
			}

		case *sqlparser.RenameTableName:
			newName := op.Table.Name.String()
			// Get the current table def
			def, getErr := db.GetTable(tableName)
			if getErr != nil {
				return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", dbName, tableName))
			}
			// Check new name doesn't already exist
			if _, getErr := db.GetTable(newName); getErr == nil {
				return nil, mysqlError(1050, "42S01", fmt.Sprintf("Table '%s' already exists", newName))
			}
			// Rename in catalog
			def.Name = newName
			db.DropTable(tableName) //nolint:errcheck
			db.CreateTable(def)     //nolint:errcheck
			// Rename in storage
			e.Storage.CreateTable(dbName, def)
			if newTbl, getErr := e.Storage.GetTable(dbName, newName); getErr == nil {
				newTbl.Rows = tbl.Rows
				newTbl.AutoIncrement.Store(tbl.AutoIncrementValue())
			}
			e.Storage.DropTable(dbName, tableName)
			// Update tableName for any subsequent ALTER operations
			tableName = newName
			tbl, _ = e.Storage.GetTable(dbName, newName)

		case *sqlparser.AlterCharset:
			// ALTER TABLE ... CONVERT TO CHARACTER SET <newCharset>
			// Re-encode all string column values from the table's current charset to UTF-8.
			newCharset := strings.ToLower(op.CharacterSet)
			canonNew := canonicalCharset(newCharset)
			tableDef, _ := db.GetTable(tableName)
			oldCharset := ""
			if tableDef != nil {
				oldCharset = strings.ToLower(tableDef.Charset)
			}
			canonOld := canonicalCharset(oldCharset)
			// Only re-encode if converting from a non-UTF8 charset to utf8
			if canonOld != "" && canonOld != "utf8" && canonOld != "utf8mb4" &&
				(canonNew == "utf8" || canonNew == "utf8mb4") {
				dec := charsetDecoder(canonOld)
				if dec != nil && tbl != nil {
					isStringCol := func(colType string) bool {
						upper := strings.ToUpper(strings.TrimSpace(colType))
						return strings.HasPrefix(upper, "CHAR") ||
							strings.HasPrefix(upper, "VARCHAR") ||
							strings.HasPrefix(upper, "TEXT") ||
							strings.HasPrefix(upper, "TINYTEXT") ||
							strings.HasPrefix(upper, "MEDIUMTEXT") ||
							strings.HasPrefix(upper, "LONGTEXT") ||
							strings.HasPrefix(upper, "ENUM") ||
							strings.HasPrefix(upper, "SET")
					}
					stringCols := make(map[string]bool)
					if tableDef != nil {
						for _, col := range tableDef.Columns {
							if isStringCol(col.Type) {
								stringCols[col.Name] = true
							}
						}
					}
					if len(stringCols) > 0 {
						tbl.Mu.Lock()
						for i := range tbl.Rows {
							row := tbl.Rows[i]
							for colName := range stringCols {
								if val, ok := row[colName]; ok && val != nil {
									var rawBytes []byte
									switch v := val.(type) {
									case string:
										rawBytes = []byte(v)
									case []byte:
										rawBytes = v
									}
									if rawBytes == nil {
										// Handle int64/uint64: treat as raw byte value
										switch iv := val.(type) {
										case int64:
											if iv >= 0 && iv <= 255 {
												rawBytes = []byte{byte(iv)}
											}
										case uint64:
											if iv <= 255 {
												rawBytes = []byte{byte(iv)}
											}
										}
									}
									if rawBytes != nil {
										if converted, err := dec.Bytes(rawBytes); err == nil {
											row[colName] = string(converted)
										}
									}
								}
							}
						}
						tbl.Mu.Unlock()
					}
				}
			}
			// Update the table's charset in the catalog
			if tableDef != nil {
				tableDef.Charset = newCharset
				if op.Collate != "" {
					tableDef.Collation = op.Collate
				}
			}

		default:
			// Unsupported ALTER option — ignore silently to stay compatible.
		}
	}

	// ALTER TABLE affected rows: MySQL reports the number of rows when data modification
	// is required (adding stored columns, modifying column types, etc.). Adding only virtual
	// generated columns doesn't require row rewrite so affected rows = 0.
	var alterAffected uint64
	requiresRowRewrite := false
	for _, opt := range stmt.AlterOptions {
		switch op := opt.(type) {
		case *sqlparser.AddColumns:
			for _, col := range op.Columns {
				if col.Type.Options != nil && col.Type.Options.As != nil {
					// Stored generated column requires row rewrite
					if col.Type.Options.Storage == sqlparser.StoredStorage {
						requiresRowRewrite = true
					}
				} else {
					// Regular (non-generated) column doesn't require row rewrite for virtual-only ALTERs
					// but does if there are stored columns too
				}
			}
		case *sqlparser.ModifyColumn:
			requiresRowRewrite = true
		case *sqlparser.ChangeColumn:
			requiresRowRewrite = true
		case *sqlparser.AlterColumn:
			// ALTER COLUMN SET DEFAULT doesn't require row rewrite
		}
	}
	if requiresRowRewrite && tbl != nil {
		alterAffected = uint64(len(tbl.Scan()))
	}
	return &Result{AffectedRows: alterAffected}, nil
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

// isVarbinaryType returns true if the column type is VARBINARY(N) or BINARY without a length.
func isVarbinaryType(colType string) bool {
	lower := strings.ToLower(strings.TrimSpace(colType))
	return strings.HasPrefix(lower, "varbinary(")
}

// looksLikeBinaryData returns true if s contains bytes that are unlikely to appear
// in a normal text string (null bytes or bytes < 0x09), indicating it's binary data
// from a BINARY/VARBINARY column.
func looksLikeBinaryData(s string) bool {
	for i := 0; i < len(s); i++ {
		b := s[i]
		if b == 0x00 || b < 0x09 {
			return true
		}
	}
	return false
}

// hexIntToBytes converts an int64 or uint64 (from a 0xNN hex literal) to its
// big-endian byte string representation, stripping leading zero bytes.
// Returns the original value unchanged if it is not an integer type.
func hexIntToBytes(val interface{}) interface{} {
	switch tv := val.(type) {
	case int64:
		if tv == 0 {
			return "\x00"
		}
		var buf [8]byte
		buf[0] = byte(tv >> 56)
		buf[1] = byte(tv >> 48)
		buf[2] = byte(tv >> 40)
		buf[3] = byte(tv >> 32)
		buf[4] = byte(tv >> 24)
		buf[5] = byte(tv >> 16)
		buf[6] = byte(tv >> 8)
		buf[7] = byte(tv)
		start := 0
		for start < 7 && buf[start] == 0 {
			start++
		}
		return string(buf[start:])
	case uint64:
		if tv == 0 {
			return "\x00"
		}
		var buf [8]byte
		buf[0] = byte(tv >> 56)
		buf[1] = byte(tv >> 48)
		buf[2] = byte(tv >> 40)
		buf[3] = byte(tv >> 32)
		buf[4] = byte(tv >> 24)
		buf[5] = byte(tv >> 16)
		buf[6] = byte(tv >> 8)
		buf[7] = byte(tv)
		start := 0
		for start < 7 && buf[start] == 0 {
			start++
		}
		return string(buf[start:])
	default:
		return val
	}
}

// padDecimalDefault pads a decimal default value to the declared scale.
// e.g. DECIMAL(10,8) with default "3.141592" -> "3.14159200"
func padDecimalDefault(colType, defVal string) string {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	if !strings.HasPrefix(upper, "DECIMAL") && !strings.HasPrefix(upper, "NUMERIC") {
		return defVal
	}
	// Parse DECIMAL(p,s)
	var p, s int
	if n, err := fmt.Sscanf(upper, "DECIMAL(%d,%d)", &p, &s); n == 2 && err == nil && s > 0 {
		// Pad the default value
		dotIdx := strings.Index(defVal, ".")
		if dotIdx < 0 {
			// No decimal point: add ".000...0"
			return defVal + "." + strings.Repeat("0", s)
		}
		decimals := defVal[dotIdx+1:]
		if len(decimals) < s {
			return defVal + strings.Repeat("0", s-len(decimals))
		}
		return defVal
	}
	return defVal
}

// padBinaryValue pads a value to the given fixed length with null bytes.
// Integer values (from 0xNN hex literals) are first converted to their
// big-endian byte representation before padding.
func padBinaryValue(val interface{}, padLen int) interface{} {
	if val == nil || padLen <= 0 {
		return val
	}
	// Convert integer hex literals to byte strings first
	converted := hexIntToBytes(val)
	s, ok := converted.(string)
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

func buildColumnTypeString(ct *sqlparser.ColumnType, tableCharset string) string {
	s := strings.ToLower(ct.Type)

	// When CHARACTER SET binary is explicitly specified, MySQL normalizes text types
	// to their binary equivalents (char->binary, varchar->varbinary, text->blob, etc.).
	// This also applies when the table-level charset is binary.
	// NOTE: When just the BINARY modifier is used (ct.Charset.Binary=true, ct.Charset.Name=""),
	// this is the deprecated "BINARY as attribute of a type" syntax which means binary collation
	// of the current charset, NOT a type conversion to VARBINARY/BINARY.
	isBinaryCharsetExplicit := strings.EqualFold(ct.Charset.Name, "binary") || (ct.Charset.Name == "" && !ct.Charset.Binary && strings.EqualFold(tableCharset, "binary"))
	if isBinaryCharsetExplicit {
		switch strings.ToUpper(ct.Type) {
		case "CHAR":
			s = "binary"
		case "VARCHAR":
			s = "varbinary"
		case "TEXT":
			s = "blob"
		case "TINYTEXT":
			s = "tinyblob"
		case "MEDIUMTEXT":
			s = "mediumblob"
		case "LONGTEXT":
			s = "longblob"
		}
	}

	if ct.Length != nil && ct.Scale != nil {
		s += fmt.Sprintf("(%d,%d)", *ct.Length, *ct.Scale)
	} else if ct.Length != nil {
		s += fmt.Sprintf("(%d)", *ct.Length)
	}
	if len(ct.EnumValues) > 0 {
		vals := make([]string, len(ct.EnumValues))
		isSet := strings.EqualFold(ct.Type, "set")
		for i, v := range ct.EnumValues {
			// Vitess parser stores enum values with surrounding quotes;
			// strip them before re-quoting to avoid double-quoting.
			v = strings.Trim(v, "'")
			// MySQL strips trailing spaces from SET/ENUM values
			if isSet || strings.EqualFold(ct.Type, "enum") {
				v = strings.TrimRight(v, " ")
			}
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
	if ct.Options != nil && ct.Options.As != nil {
		storage := " virtual"
		if ct.Options.Storage == sqlparser.StoredStorage {
			storage = " stored"
		}
		s += " generated always as (" + sqlparser.String(ct.Options.As) + ")" + storage
	}
	return s
}

// referenceActionToString converts a sqlparser.ReferenceAction to a string.
func referenceActionToString(action sqlparser.ReferenceAction) string {
	switch action {
	case sqlparser.Restrict:
		return "RESTRICT"
	case sqlparser.Cascade:
		return "CASCADE"
	case sqlparser.NoAction:
		return "NO ACTION"
	case sqlparser.SetNull:
		return "SET NULL"
	case sqlparser.SetDefault:
		return "SET DEFAULT"
	default:
		return "" // DefaultAction = RESTRICT behavior
	}
}

func tableOptionString(opt *sqlparser.TableOption) string {
	if opt == nil {
		return ""
	}
	if opt.String != "" {
		return opt.String
	}
	if opt.Value != nil {
		return opt.Value.Val
	}
	return ""
}

func parseTableOptionInt(opt *sqlparser.TableOption) *int {
	raw := strings.TrimSpace(tableOptionString(opt))
	if raw == "" {
		return nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return nil
	}
	v := n
	return &v
}

// parseTableOptionUint64Clamped parses a table option as uint64,
// clamping values exceeding uint32 max to math.MaxUint32 (MySQL MyISAM behaviour).
func parseTableOptionUint64Clamped(opt *sqlparser.TableOption) *uint64 {
	raw := strings.TrimSpace(tableOptionString(opt))
	if raw == "" {
		return nil
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		// Try parsing as a signed int in case negative or overflow
		n = math.MaxUint32
	}
	if n > math.MaxUint32 {
		n = math.MaxUint32
	}
	v := n
	return &v
}

// execTruncateTable handles TRUNCATE TABLE statements.
func (e *Executor) execTruncateTable(stmt *sqlparser.TruncateTable) (*Result, error) {
	tableName := stmt.Table.Name.String()
	dbName := e.CurrentDB
	if q := stmt.Table.Qualifier.String(); q != "" {
		dbName = q
	}
	// Reject TRUNCATE on information_schema tables.
	if strings.EqualFold(dbName, "information_schema") {
		return nil, mysqlError(1044, "42000", "Access denied for user 'root'@'localhost' to database 'information_schema'")
	}

	// Handle performance_schema tables
	if strings.EqualFold(dbName, "performance_schema") {
		if perfSchemaTruncateDenied(tableName) {
			return nil, mysqlError(1142, "42000", fmt.Sprintf("DROP command denied to user 'root'@'localhost' for table '%s'", tableName))
		}
		lowerTable := strings.ToLower(tableName)
		// For writable tables with in-memory state, clear the rows
		switch lowerTable {
		case "setup_actors":
			e.psSetupActors = []storage.Row{}
			e.psSetupActorsInit = true
		case "setup_objects":
			e.psSetupObjects = []storage.Row{}
			e.psSetupObjectsInit = true
		case "events_statements_summary_by_digest", "events_statements_histogram_by_digest":
			// Clear digests and mark as truncated so new statements are recorded
			e.psDigests = nil
			if e.psTruncated == nil {
				e.psTruncated = make(map[string]bool)
			}
			e.psTruncated[lowerTable] = true
		default:
			// Track truncated PS tables so they return empty result sets
			if e.psTruncated == nil {
				e.psTruncated = make(map[string]bool)
			}
			e.psTruncated[lowerTable] = true
		}
		return &Result{AffectedRows: 0, IsResultSet: false}, nil
	}
	tbl, err := e.Storage.GetTable(dbName, tableName)
	if err != nil {
		return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", dbName, tableName))
	}
	tbl.Truncate()
	return &Result{AffectedRows: 0, IsResultSet: false}, nil
}

// inferColumnType tries to determine the column type from the source table of a SELECT statement.
// For UNION queries, it merges types from all branches (taking the widest varchar).
func (e *Executor) inferColumnType(selectSQL, colName string) string {
	stmt, err := e.parser().Parse(selectSQL)
	if err != nil {
		return ""
	}
	switch s := stmt.(type) {
	case *sqlparser.Select:
		return e.inferColumnTypeFromSelect(s, colName)
	case *sqlparser.Union:
		return e.inferColumnTypeFromUnion(s, colName)
	}
	return ""
}

// inferColumnAttrs infers full column attributes (type, charset, nullable, default) for a column
// from a SELECT SQL string. Falls back to type-only inference when full attrs aren't available.
func (e *Executor) inferColumnAttrs(selectSQL, colName string) columnAttrs {
	attrs := columnAttrs{nullable: true}
	stmt, err := e.parser().Parse(selectSQL)
	if err != nil {
		return attrs
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		attrs.colType = e.inferColumnType(selectSQL, colName)
		return attrs
	}
	// Find the expression for colName
	for _, expr := range sel.SelectExprs.Exprs {
		ae, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}
		alias := ""
		if !ae.As.IsEmpty() {
			alias = ae.As.String()
		}
		if alias != "" {
			if !strings.EqualFold(alias, colName) {
				continue
			}
		} else {
			if col, ok2 := ae.Expr.(*sqlparser.ColName); ok2 {
				if !strings.EqualFold(col.Name.String(), colName) {
					continue
				}
			} else {
				exprStr := normalizeCharsetIntroducersForMatch(sqlparser.String(ae.Expr))
				normalizedColName := normalizeCharsetIntroducersForMatch(colName)
				// Normalize whitespace for comparison: sqlparser.String() may collapse or
				// add spaces differently (e.g., "a, b" vs "a,b"). Strip all whitespace.
				exprStrNorm := strings.ReplaceAll(strings.ToLower(exprStr), " ", "")
				colNameNorm := strings.ReplaceAll(strings.ToLower(normalizedColName), " ", "")
				if exprStrNorm != colNameNorm {
					continue
				}
			}
		}
		// Found the expression — get full attrs
		a := e.inferExprAttrs(ae.Expr)
		if a.colType == "" {
			a.colType = e.inferColumnTypeFromSelect(sel, colName)
		}
		return a
	}
	// Fall back to type inference
	attrs.colType = e.inferColumnTypeFromSelect(sel, colName)
	return attrs
}

// inferColumnTypeFromSelect infers the column type from a single SELECT statement.
func (e *Executor) inferColumnTypeFromSelect(sel *sqlparser.Select, colName string) string {
	// Get the source table from the FROM clause
	for _, from := range sel.From {
		ate, ok := from.(*sqlparser.AliasedTableExpr)
		if !ok {
			continue
		}
		tn, ok := ate.Expr.(sqlparser.TableName)
		if !ok {
			continue
		}
		srcDB := e.CurrentDB
		if !tn.Qualifier.IsEmpty() {
			srcDB = tn.Qualifier.String()
		}
		db, err := e.Catalog.GetDatabase(srcDB)
		if err != nil {
			continue
		}
		tblDef, err := db.GetTable(tn.Name.String())
		if err != nil {
			continue
		}
		for _, col := range tblDef.Columns {
			if strings.EqualFold(col.Name, colName) {
				return col.Type
			}
		}
	}
	// No FROM clause (e.g. SELECT 'literal') — infer type from expressions
	// Find the expression corresponding to this column name by alias or position
	for _, expr := range sel.SelectExprs.Exprs {
		ae, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}
		alias := ""
		if !ae.As.IsEmpty() {
			alias = ae.As.String()
		}
		if alias != "" && !strings.EqualFold(alias, colName) {
			continue
		}
		if alias == "" {
			if col, ok2 := ae.Expr.(*sqlparser.ColName); ok2 {
				if !strings.EqualFold(col.Name.String(), colName) {
					continue
				}
			} else {
				// For non-column expressions (FuncExpr, etc.), compare the string representation.
				// Normalize charset introducers before comparing (sqlparser uses _utf8mb3 'x'
				// but MySQL column names show _utf8'x').
				exprStr := normalizeCharsetIntroducersForMatch(sqlparser.String(ae.Expr))
				normalizedColName := normalizeCharsetIntroducersForMatch(colName)
				if !strings.EqualFold(exprStr, normalizedColName) {
					continue
				}
			}
		}
		return e.inferExprType(ae.Expr)
	}
	return ""
}

// normalizeCharsetIntroducersForMatch normalizes charset introducer syntax for string matching.
// Converts _utf8mb3 (with or without space) to _utf8 to allow matching.
func normalizeCharsetIntroducersForMatch(s string) string {
	s = strings.ReplaceAll(s, "_utf8mb3 '", "_utf8'")
	s = strings.ReplaceAll(s, "_utf8mb3'", "_utf8'")
	return s
}

// inferColumnTypeFromUnion infers the column type from a UNION by merging all branches.
// The colName parameter is used for the first SELECT branch; subsequent branches
// use positional matching.
func (e *Executor) inferColumnTypeFromUnion(u *sqlparser.Union, colName string) string {
	selects := e.flattenUnionStatements(u)
	types := make([]string, 0, len(selects))
	for i, s := range selects {
		sel, ok := s.(*sqlparser.Select)
		if !ok {
			continue
		}
		var t string
		if i == 0 {
			t = e.inferColumnTypeFromSelect(sel, colName)
		} else {
			// For non-first branches, find the column by position matching colName
			t = e.inferColumnTypeFromSelectByPosition(sel, colName, selects[0])
		}
		if t != "" {
			types = append(types, t)
		}
	}
	return mergeColumnTypes(types)
}

// flattenUnionStatements flattens a Union AST into a slice of TableStatement (each is a *Select).
func (e *Executor) flattenUnionStatements(u *sqlparser.Union) []sqlparser.TableStatement {
	var result []sqlparser.TableStatement
	switch left := u.Left.(type) {
	case *sqlparser.Union:
		result = append(result, e.flattenUnionStatements(left)...)
	default:
		result = append(result, left)
	}
	switch right := u.Right.(type) {
	case *sqlparser.Union:
		result = append(result, e.flattenUnionStatements(right)...)
	default:
		result = append(result, right)
	}
	return result
}

// inferColumnTypeFromSelectByPosition finds the column type in a SELECT by matching
// the position of colName in the first SELECT branch.
func (e *Executor) inferColumnTypeFromSelectByPosition(sel *sqlparser.Select, colName string, firstBranch sqlparser.TableStatement) string {
	// Find the position of colName in the first branch
	pos := -1
	if firstSel, ok := firstBranch.(*sqlparser.Select); ok {
		for i, expr := range firstSel.SelectExprs.Exprs {
			ae, ok := expr.(*sqlparser.AliasedExpr)
			if !ok {
				if _, ok2 := expr.(*sqlparser.StarExpr); ok2 {
					// Star expansion — can't easily determine position, skip
					pos = 0
					break
				}
				continue
			}
			alias := ""
			if !ae.As.IsEmpty() {
				alias = ae.As.String()
			}
			if alias != "" && strings.EqualFold(alias, colName) {
				pos = i
				break
			}
			if col, ok2 := ae.Expr.(*sqlparser.ColName); ok2 {
				if strings.EqualFold(col.Name.String(), colName) {
					pos = i
					break
				}
			}
		}
	}
	if pos < 0 {
		return ""
	}
	// Get the expression at position pos in the current SELECT
	if pos < len(sel.SelectExprs.Exprs) {
		expr := sel.SelectExprs.Exprs[pos]
		if ae, ok := expr.(*sqlparser.AliasedExpr); ok {
			// First try table column lookup
			t := e.inferColumnTypeFromSelect(sel, colName)
			if t != "" {
				return t
			}
			return e.inferExprType(ae.Expr)
		}
	}
	return ""
}

// inferExprType infers the SQL column type from a literal or function expression.
func (e *Executor) inferExprType(expr sqlparser.Expr) string {
	switch v := expr.(type) {
	case *sqlparser.Literal:
		switch v.Type {
		case sqlparser.StrVal:
			n := len(v.Val)
			return fmt.Sprintf("varchar(%d)", n)
		case sqlparser.IntVal:
			return "int"
		case sqlparser.FloatVal:
			return "double"
		case sqlparser.HexVal:
			// MySQL: hex literals act as BIGINT UNSIGNED in arithmetic context
			return "bigint unsigned"
		}
	case *sqlparser.BinaryExpr:
		// Arithmetic expressions (+,-,*,/) return numeric types.
		// If either operand is hex, result is bigint unsigned.
		leftType := e.inferExprType(v.Left)
		rightType := e.inferExprType(v.Right)
		if leftType == "bigint unsigned" || rightType == "bigint unsigned" {
			return "bigint unsigned"
		}
		if leftType == "double" || rightType == "double" {
			return "double"
		}
		if leftType == "int" || rightType == "int" {
			return "bigint"
		}
		return "bigint"
	case *sqlparser.FuncExpr:
		name := strings.ToLower(v.Name.String())
		switch name {
		case "repeat":
			// REPEAT(str, count) — type is varchar(len(str)*count)
			if len(v.Exprs) == 2 {
				strLen := 0
				if lit, ok := v.Exprs[0].(*sqlparser.Literal); ok && lit.Type == sqlparser.StrVal {
					strLen = len(lit.Val)
				}
				cnt := 0
				if lit, ok := v.Exprs[1].(*sqlparser.Literal); ok && lit.Type == sqlparser.IntVal {
					cnt, _ = strconv.Atoi(lit.Val)
				}
				if strLen > 0 && cnt > 0 {
					return fmt.Sprintf("varchar(%d)", strLen*cnt)
				}
			}
		case "str_to_date":
			// str_to_date always returns datetime(6) type; MySQL infers the actual subtype
			// from the format string used at CREATE TABLE time.
			if len(v.Exprs) >= 2 {
				fmtLit, ok := v.Exprs[1].(*sqlparser.Literal)
				if ok && fmtLit.Type == sqlparser.StrVal {
					return inferStrToDateType(fmtLit.Val)
				}
			}
			return "datetime(6)"
		case "database", "schema":
			// MySQL: database() returns varchar(34) CHARACTER SET utf8
			return "varchar(34)"
		case "user", "current_user", "session_user", "system_user":
			// MySQL: user() returns varchar(288) CHARACTER SET utf8
			return "varchar(288)"
		case "version":
			// MySQL: version() returns char(60)
			return "char(60)"
		case "charset", "collation":
			// MySQL: charset()/collation() return varchar(64) CHARACTER SET utf8
			return "varchar(64)"
		case "connection_id":
			return "bigint unsigned"
		case "last_insert_id", "row_count", "found_rows":
			return "bigint"
		case "if", "ifnull", "nullif", "coalesce", "greatest", "least":
			// For functions where all arguments resolve to NULL, MySQL returns binary(0).
			// For IF(cond, then, else), check the then/else args (indices 1 and 2).
			// For COALESCE/GREATEST/LEAST, check all args.
			argsToCheck := v.Exprs
			if name == "if" && len(v.Exprs) == 3 {
				argsToCheck = v.Exprs[1:] // skip condition
			} else if name == "ifnull" && len(v.Exprs) == 2 {
				argsToCheck = v.Exprs // check both
			}
			allNull := len(argsToCheck) > 0
			for _, arg := range argsToCheck {
				t := e.inferExprType(arg)
				if t != "binary(0)" && t != "" {
					allNull = false
					break
				}
			}
			if allNull {
				return "binary(0)"
			}
		}
	case *sqlparser.IntroducerExpr:
		// _charset'string' — type is varchar(len) with the given charset
		if lit, ok := v.Expr.(*sqlparser.Literal); ok && lit.Type == sqlparser.StrVal {
			return fmt.Sprintf("varchar(%d)", len(lit.Val))
		}
	case *sqlparser.NullVal:
		// MySQL uses binary(0) for NULL literal columns in CREATE TABLE AS SELECT
		return "binary(0)"
	case *sqlparser.ConvertExpr:
		// CAST(x AS type) / CONVERT(x, type) — use the target type
		return convertTypeToSQLType(v.Type)
	case *sqlparser.CastExpr:
		// CAST(x AS type) — use the target type
		return convertTypeToSQLType(v.Type)
	}
	return ""
}

// convertTypeToSQLType converts a vitess ConvertType to a MySQL column type string.
func convertTypeToSQLType(ct *sqlparser.ConvertType) string {
	if ct == nil {
		return ""
	}
	typeName := strings.ToLower(ct.Type)
	switch typeName {
	case "signed", "signed integer":
		return "bigint"
	case "unsigned", "unsigned integer":
		return "bigint unsigned"
	case "decimal":
		if ct.Length != nil && ct.Scale != nil {
			return fmt.Sprintf("decimal(%d,%d)", *ct.Length, *ct.Scale)
		} else if ct.Length != nil {
			return fmt.Sprintf("decimal(%d)", *ct.Length)
		}
		return "decimal(10,0)"
	case "char":
		if ct.Length != nil {
			return fmt.Sprintf("char(%d)", *ct.Length)
		}
		return "char(0)"
	case "nchar":
		if ct.Length != nil {
			return fmt.Sprintf("char(%d)", *ct.Length)
		}
		return "char(0)"
	case "binary":
		if ct.Length != nil {
			return fmt.Sprintf("binary(%d)", *ct.Length)
		}
		return "binary(0)"
	case "date":
		return "date"
	case "datetime":
		if ct.Length != nil {
			return fmt.Sprintf("datetime(%d)", *ct.Length)
		}
		return "datetime"
	case "time":
		if ct.Length != nil {
			return fmt.Sprintf("time(%d)", *ct.Length)
		}
		return "time"
	case "year":
		return "year(4)"
	case "json":
		return "json"
	case "float":
		return "double"
	case "double":
		return "double"
	case "real":
		return "double"
	}
	return ""
}

// columnAttrs holds inferred column attributes beyond just type.
type columnAttrs struct {
	colType  string
	charset  string
	nullable bool
	hasDefault bool
	defaultVal string
}

// inferExprAttrs infers type, charset, nullable, and default from a function/expr.
// Used by CREATE TABLE ... SELECT to set correct column attributes.
func (e *Executor) inferExprAttrs(expr sqlparser.Expr) columnAttrs {
	attrs := columnAttrs{nullable: true}
	switch v := expr.(type) {
	case *sqlparser.FuncExpr:
		name := strings.ToLower(v.Name.String())
		switch name {
		case "database", "schema":
			attrs.colType = "varchar(34)"
			attrs.charset = "utf8"
			attrs.nullable = true
		case "user", "current_user", "session_user", "system_user":
			attrs.colType = "varchar(288)"
			attrs.charset = "utf8"
			attrs.nullable = false
			attrs.hasDefault = true
			attrs.defaultVal = ""
		case "charset", "collation":
			attrs.colType = "varchar(64)"
			attrs.charset = "utf8"
			attrs.nullable = false
			attrs.hasDefault = true
			attrs.defaultVal = ""
		}
	case *sqlparser.IntroducerExpr:
		// _charset'string' — charset comes from the introducer
		cs := strings.ToLower(strings.TrimPrefix(v.CharacterSet, "_"))
		if cs == "utf8mb3" {
			cs = "utf8"
		}
		attrs.charset = cs
		if lit, ok := v.Expr.(*sqlparser.Literal); ok && lit.Type == sqlparser.StrVal {
			attrs.colType = fmt.Sprintf("varchar(%d)", len(lit.Val))
		}
	}
	if attrs.colType == "" {
		attrs.colType = e.inferExprType(expr)
	}
	return attrs
}

// inferStrToDateType infers the column type from a str_to_date format string.
// MySQL uses the format to determine if the result is datetime, date, time, etc.
// A "full date" requires at least a year OR month specifier (not just %d/%j alone).
func inferStrToDateType(format string) string {
	hasFullDate := false // year or month specifier present
	hasDay := false      // only day specifier present
	hasTime := false
	hasMicro := false
	for i := 0; i < len(format); i++ {
		if format[i] == '%' && i+1 < len(format) {
			i++
			switch format[i] {
			case 'Y', 'y', 'm', 'c', 'M', 'b', 'U', 'u', 'V', 'v', 'X', 'x', 'W', 'w':
				hasFullDate = true
			case 'd', 'e', 'D', 'j':
				hasDay = true
			case 'H', 'h', 'I', 'k', 'l', 'i', 's', 'S', 'T', 'r', 'p':
				hasTime = true
			case 'f':
				hasMicro = true
				hasTime = true
			}
		}
	}
	// MySQL type inference rules for str_to_date:
	// - date+time (year/month present + time) → datetime[(6)]
	// - date only (year or month, no time) → date
	// - day+time (no year/month, has time) → time[(6)]
	// - day only (no year/month, no time) → date (e.g. %d alone)
	// - time only → time[(6)]
	if hasFullDate && hasTime {
		if hasMicro {
			return "datetime(6)"
		}
		return "datetime"
	}
	if hasFullDate {
		return "date"
	}
	if hasTime {
		// time-only or day+time
		if hasMicro {
			return "time(6)"
		}
		return "time"
	}
	if hasDay {
		// day-only (no year/month, no time) → date
		return "date"
	}
	return "datetime(6)"
}

// mergeColumnTypes merges a list of column types from UNION branches,
// returning the widest compatible type. varchar(N) types are merged by taking max(N).
// If any branch has text/blob/longtext, the result is that type.
func mergeColumnTypes(types []string) string {
	if len(types) == 0 {
		return ""
	}
	if len(types) == 1 {
		return types[0]
	}
	maxVarcharLen := 0
	hasText := false
	hasBlob := false
	hasInt := false
	hasDouble := false
	for _, t := range types {
		lower := strings.ToLower(strings.TrimSpace(t))
		if lower == "text" || lower == "mediumtext" || lower == "longtext" {
			hasText = true
		} else if lower == "blob" || lower == "mediumblob" || lower == "longblob" {
			hasBlob = true
		} else if lower == "int" || lower == "bigint" || lower == "tinyint" || lower == "smallint" {
			hasInt = true
		} else if lower == "double" || lower == "float" || lower == "decimal" {
			hasDouble = true
		} else if strings.HasPrefix(lower, "varchar(") {
			var n int
			if _, err := fmt.Sscanf(lower, "varchar(%d)", &n); err == nil {
				if n > maxVarcharLen {
					maxVarcharLen = n
				}
			}
		}
	}
	if hasBlob {
		return "blob"
	}
	if hasText {
		return "text"
	}
	if maxVarcharLen > 0 {
		return fmt.Sprintf("varchar(%d)", maxVarcharLen)
	}
	if hasInt {
		return "int"
	}
	if hasDouble {
		return "double"
	}
	// Fall back to first non-empty type
	return types[0]
}

// execCreateTableLike handles CREATE TABLE t2 LIKE t1.
func (e *Executor) execCreateTableLike(targetDBName, newTableName, srcDBName, srcTableName string) (*Result, error) {
	db, err := e.Catalog.GetDatabase(targetDBName)
	if err != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", targetDBName))
	}
	srcDB, err := e.Catalog.GetDatabase(srcDBName)
	if err != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", srcDBName))
	}
	srcDef, err := srcDB.GetTable(srcTableName)
	if err != nil {
		return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", srcDBName, srcTableName))
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
	e.Storage.CreateTable(targetDBName, newDef)
	e.upsertInnoDBStatsRows(targetDBName, newTableName, 0)
	return &Result{}, nil
}

// extractSelectFromQuery extracts the SELECT portion from a CREATE TABLE ... SELECT query,
// preserving the original case and spacing from the raw query text.
// Returns "" if the SELECT portion cannot be found.
func (e *Executor) extractSelectFromQuery(query string) string {
	depth := 0
	selectIdx := -1
	for i := 0; i < len(query)-5; i++ {
		switch query[i] {
		case '(':
			depth++
		case ')':
			depth--
		case '\'':
			for i++; i < len(query) && query[i] != '\''; i++ {
				if query[i] == '\\' {
					i++
				}
			}
		}
		if depth == 0 && (query[i] == 's' || query[i] == 'S') {
			if i+6 <= len(query) && strings.EqualFold(query[i:i+6], "SELECT") {
				if i == 0 || !isIdentChar(query[i-1]) {
					selectIdx = i
					break
				}
			}
		}
	}
	if selectIdx < 0 {
		return ""
	}
	return strings.TrimSpace(query[selectIdx:])
}

// tryExecCreateTableIndexOnlySelect handles the case where vitess parses
// CREATE TABLE t (PRIMARY KEY (a)) SELECT ... with TableSpec=nil and Select=nil
// because the paren block contains only index definitions (no column defs).
// Returns (nil, nil) if the current query doesn't match this pattern.
func (e *Executor) tryExecCreateTableIndexOnlySelect(stmt *sqlparser.CreateTable, dbName, tableName string) (*Result, error) {
	// Check if the current query text has SELECT in it
	currentQuery := e.currentQuery
	upperQuery := strings.ToUpper(currentQuery)
	if !strings.Contains(upperQuery, "SELECT") {
		return nil, nil
	}
	// Find SELECT outside parentheses
	depth := 0
	selectIdx := -1
	for i := 0; i < len(currentQuery)-5; i++ {
		switch currentQuery[i] {
		case '(':
			depth++
		case ')':
			depth--
		case '\'':
			for i++; i < len(currentQuery) && currentQuery[i] != '\''; i++ {
				if currentQuery[i] == '\\' {
					i++
				}
			}
		}
		if depth == 0 && (currentQuery[i] == 's' || currentQuery[i] == 'S') {
			if i+6 <= len(currentQuery) && strings.EqualFold(currentQuery[i:i+6], "SELECT") {
				if i == 0 || !isIdentChar(currentQuery[i-1]) {
					selectIdx = i
					break
				}
			}
		}
	}
	if selectIdx < 0 {
		return nil, nil
	}
	// Extract the index/constraint block: the part between CREATE TABLE name and SELECT
	// Find the index block: everything between the first '(' at depth=0 and the matching ')'
	// Parse PRIMARY KEY columns from the block
	prefix := strings.TrimSpace(currentQuery[:selectIdx])
	selectSQL := strings.TrimSpace(currentQuery[selectIdx:])

	// Extract PRIMARY KEY columns from prefix like "CREATE TABLE t2 ( PRIMARY KEY (a) )"
	re := regexp.MustCompile(`(?i)\bPRIMARY\s+KEY\s*\(([^)]+)\)`)
	pkMatch := re.FindStringSubmatch(prefix)
	var primaryKeyCols []string
	if pkMatch != nil {
		for _, col := range strings.Split(pkMatch[1], ",") {
			primaryKeyCols = append(primaryKeyCols, strings.TrimSpace(col))
		}
	}

	// Run the SELECT to determine columns
	result, err := e.Execute(selectSQL)
	if err != nil {
		return nil, err
	}
	if result == nil || !result.IsResultSet {
		return &Result{}, nil
	}

	// Build column defs from SELECT result
	cols := make([]catalog.ColumnDef, 0, len(result.Columns))
	for _, colName := range result.Columns {
		colType := "text"
		if inferredType := e.inferColumnType(selectSQL, colName); inferredType != "" {
			colType = inferredType
		}
		isPK := false
		for _, pk := range primaryKeyCols {
			if strings.EqualFold(pk, colName) {
				isPK = true
				break
			}
		}
		colDef := catalog.ColumnDef{
			Name:       colName,
			Type:       colType,
			Nullable:   !isPK,
			PrimaryKey: isPK,
		}
		cols = append(cols, colDef)
	}

	newDef := &catalog.TableDef{
		Name:       tableName,
		Columns:    cols,
		PrimaryKey: primaryKeyCols,
	}

	db, dbErr := e.Catalog.GetDatabase(dbName)
	if dbErr != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", dbName))
	}
	if err := db.CreateTable(newDef); err != nil {
		return nil, mysqlError(1050, "42S01", fmt.Sprintf("Table '%s' already exists", tableName))
	}
	e.Storage.CreateTable(dbName, newDef)
	tbl, _ := e.Storage.GetTable(dbName, tableName)

	// Insert rows with constraint checking
	for _, selRow := range result.Rows {
		row := make(storage.Row)
		for j, colName := range result.Columns {
			if j < len(selRow) {
				row[colName] = selRow[j]
			}
		}
		if _, insertErr := tbl.Insert(row); insertErr != nil {
			// On failure, roll back by dropping the table
			db.DropTable(tableName)       //nolint:errcheck
			e.Storage.DropTable(dbName, tableName)
			return nil, insertErr
		}
	}

	if stmt.Temp {
		e.tempTables[tableName] = true
	}
	e.upsertInnoDBStatsRows(dbName, tableName, e.tableRowCount(dbName, tableName))
	return &Result{}, nil
}

// isSELECTForUpdate returns true if the SELECT query contains a FOR UPDATE clause.
func isSELECTForUpdate(selectSQL string) bool {
	upper := strings.ToUpper(selectSQL)
	return strings.Contains(upper, " FOR UPDATE")
}

// execCreateTableSelect handles CREATE TABLE t2 [AS] SELECT ...
// targetDB specifies the database to create the table in (may differ from e.CurrentDB for cross-db CREATE TABLE).
func (e *Executor) execCreateTableSelect(targetDB, newTableName, selectSQL string) (*Result, error) {
	if targetDB == "" {
		targetDB = e.CurrentDB
	}
	db, err := e.Catalog.GetDatabase(targetDB)
	if err != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", targetDB))
	}
	// CREATE TABLE ... AS SELECT needs to acquire locks on source table rows.
	// If another connection holds FOR UPDATE locks, this should time out.
	if e.rowLockManager != nil {
		// Extract source table name from SELECT
		srcTable := explainTableNameFromQuery(selectSQL)
		if srcTable != "" {
			srcDB := e.CurrentDB
			if tbl, tblErr := e.Storage.GetTable(srcDB, srcTable); tblErr == nil {
				if def, defErr := db.GetTable(srcTable); defErr == nil && len(tbl.Rows) > 0 {
					allIndices := make([]int, len(tbl.Rows))
					for i := range tbl.Rows {
						allIndices[i] = i
					}
					if lockErr := e.acquireRowLocksForRows(srcDB, srcTable, def, tbl.Rows, allIndices); lockErr != nil {
						e.handleRollbackOnTimeout()
						// MySQL raises a specific error when CREATE TABLE ... SELECT ... FOR UPDATE
						// cannot acquire locks on the source table
						if isSELECTForUpdate(selectSQL) {
							return nil, mysqlError(1615, "HY000", fmt.Sprintf("Can't update table '%s' while '%s' is being created.", srcTable, newTableName))
						}
						return nil, lockErr
					}
					// Release the locks immediately; we just needed to verify availability
					e.rowLockManager.ReleaseRowLocks(e.connectionID)
				}
			}
		}
	}
	// Mark as DML context so that overflow in strict mode raises errors (not warnings).
	prevInsideDML := e.insideDML
	e.insideDML = true
	result, err := e.Execute(selectSQL)
	e.insideDML = prevInsideDML
	if err != nil {
		return nil, err
	}
	var cols []catalog.ColumnDef
	for _, colName := range result.Columns {
		attrs := e.inferColumnAttrs(selectSQL, colName)
		colType := attrs.colType
		if colType == "" {
			colType = "text"
		}
		col := catalog.ColumnDef{
			Name:     colName,
			Type:     colType,
			Nullable: attrs.nullable,
			Charset:  attrs.charset,
		}
		if attrs.hasDefault {
			col.Default = &attrs.defaultVal
		}
		cols = append(cols, col)
	}
	newDef := &catalog.TableDef{
		Name:    newTableName,
		Columns: cols,
	}
	if err := db.CreateTable(newDef); err != nil {
		return nil, mysqlError(1050, "42S01", fmt.Sprintf("Table '%s' already exists", newTableName))
	}
	e.Storage.CreateTable(targetDB, newDef)
	tbl, _ := e.Storage.GetTable(targetDB, newTableName)
	// Build a column type map for coercion
	colTypeMap := make(map[string]string, len(cols))
	for _, col := range cols {
		colTypeMap[col.Name] = col.Type
	}
	for _, row := range result.Rows {
		sRow := make(storage.Row)
		for i, colName := range result.Columns {
			if i < len(row) {
				val := row[i]
				if colType, ok := colTypeMap[colName]; ok && val != nil {
					val = coerceColumnValue(colType, val)
				}
				sRow[colName] = val
			}
		}
		tbl.Insert(sRow) //nolint:errcheck
	}
	e.upsertInnoDBStatsRows(targetDB, newTableName, e.tableRowCount(targetDB, newTableName))
	return &Result{}, nil
}

// hasPrimaryKey returns true if the table has an explicit primary key defined.
// isZeroDateValue returns true if val represents a zero date/datetime/timestamp value.
// MySQL considers "0", "0000-00-00", "0000-00-00 00:00:00" as zero dates.
func isZeroDateValue(val string) bool {
	v := strings.TrimSpace(val)
	return v == "0" || v == "0000-00-00" || v == "0000-00-00 00:00:00" || v == "0000-00-00 00:00:00.000000"
}

// isZeroInDateValue returns true if val represents a date with a zero component (year/month/day).
// This is used for NO_ZERO_IN_DATE checks (e.g., '2012-02-00' has zero day).
func isZeroInDateValue(val string) bool {
	v := strings.TrimSpace(val)
	// First check for fully zero date
	if isZeroDateValue(v) {
		return true
	}
	// Strip time component if present
	datepart := v
	if idx := strings.Index(v, " "); idx >= 0 {
		datepart = v[:idx]
	}
	// Check YYYY-MM-DD format for zero components
	parts := strings.Split(datepart, "-")
	if len(parts) == 3 {
		for _, p := range parts {
			if p == "0" || p == "00" {
				return true
			}
		}
	}
	return false
}

func hasPrimaryKey(tbl *catalog.TableDef) bool {
	if len(tbl.PrimaryKey) > 0 {
		return true
	}
	for _, col := range tbl.Columns {
		if col.PrimaryKey {
			return true
		}
	}
	return false
}

// isFirstNotNullUnique returns true if the given index is the first NOT NULL UNIQUE index
// that would be implicitly promoted as the primary key.
func isFirstNotNullUnique(tbl *catalog.TableDef, idx catalog.IndexDef) bool {
	if !idx.Unique {
		return false
	}
	// Check if all columns in the index are NOT NULL
	for _, col := range idx.Columns {
		baseCol := strings.TrimSpace(col)
		for _, c := range tbl.Columns {
			if strings.EqualFold(c.Name, baseCol) {
				if c.Nullable {
					return false
				}
				break
			}
		}
	}
	// Check if this is the first NOT NULL UNIQUE index
	for _, other := range tbl.Indexes {
		if !other.Unique {
			continue
		}
		if strings.EqualFold(other.Name, idx.Name) {
			// This is the first NOT NULL UNIQUE index we've checked
			return true
		}
		// Check if this other index is also all NOT NULL
		allNotNull := true
		for _, col := range other.Columns {
			baseCol := strings.TrimSpace(col)
			for _, c := range tbl.Columns {
				if strings.EqualFold(c.Name, baseCol) {
					if c.Nullable {
						allNotNull = false
					}
					break
				}
			}
		}
		if allNotNull {
			// Found an earlier NOT NULL UNIQUE index
			return false
		}
	}
	return false
}
