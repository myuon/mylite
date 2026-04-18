package executor

import (
	"encoding/hex"
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

// validateUTF8StringForDDL checks if a string contains valid utf8mb3 (3-byte UTF-8) when character_set_client=binary.
// MySQL raises ER_INVALID_CHARACTER_STRING (1300) when binary strings contain invalid utf8/utf8mb3 sequences in DDL contexts.
// Returns an error with the invalid bytes shown in hex if the string is invalid.
func validateUTF8StringForDDL(s string) error {
	// Validate as utf8mb3: bytes must form valid UTF-8 sequences, and no codepoint may exceed U+FFFF (3-byte max)
	for i := 0; i < len(s); {
		b := s[i]
		var seqLen int
		switch {
		case b < 0x80:
			// ASCII (1 byte)
			i++
			continue
		case b < 0xC0:
			// Continuation byte without start byte - invalid
			seqLen = 1
		case b < 0xE0:
			// 2-byte sequence
			seqLen = 2
		case b < 0xF0:
			// 3-byte sequence
			seqLen = 3
		default:
			// 4+ byte sequence: invalid for utf8mb3 (and also invalid for most cases)
			seqLen = 4
			if i+seqLen > len(s) {
				seqLen = len(s) - i
			}
			end := i + 3 // show 3 bytes in error (like MySQL does)
			if end > len(s) {
				end = len(s)
			}
			hexBytes := fmt.Sprintf("%X", s[i:end])
			return mysqlError(1300, "HY000", fmt.Sprintf("Invalid utf8 character string: '%s'", hexBytes))
		}
		// Validate continuation bytes
		if i+seqLen > len(s) {
			// Truncated sequence
			end := len(s)
			if end-i > 3 {
				end = i + 3
			}
			hexBytes := fmt.Sprintf("%X", s[i:end])
			return mysqlError(1300, "HY000", fmt.Sprintf("Invalid utf8 character string: '%s'", hexBytes))
		}
		valid := true
		for j := 1; j < seqLen; j++ {
			if s[i+j]&0xC0 != 0x80 {
				valid = false
				break
			}
		}
		if !valid {
			// Show start byte plus continuation bytes (MySQL shows 3 bytes in error message)
			end := i + 3
			if end > len(s) {
				end = len(s)
			}
			hexBytes := fmt.Sprintf("%X", s[i:end])
			return mysqlError(1300, "HY000", fmt.Sprintf("Invalid utf8 character string: '%s'", hexBytes))
		}
		// For 3-byte sequences, also validate the codepoint is in range (U+0000-U+FFFF)
		// (utf8mb3 max is U+FFFF, 4-byte sequences already rejected above)
		i += seqLen
	}
	return nil
}

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

		// Validate identifier lengths (MySQL max is 64 characters)
		if len(newName) > 64 {
			return nil, mysqlError(1059, "42000", fmt.Sprintf("Identifier name '%s' is too long", newName))
		}
		if len(oldName) > 64 {
			return nil, mysqlError(1059, "42000", fmt.Sprintf("Identifier name '%s' is too long", oldName))
		}
		// Validate databases exist
		if _, err := e.Catalog.GetDatabase(targetDB); err != nil {
			return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", targetDB))
		}
		if _, err := e.Catalog.GetDatabase(srcDB); err != nil {
			return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", srcDB))
		}

		// Skip validation for no-op renames (same table, same db) - MySQL allows this
		isSameTable := strings.EqualFold(srcDB, targetDB) && strings.EqualFold(oldName, newName)
		// Validate target doesn't exist first (MySQL checks destination conflicts before source existence)
		if !isSameTable && tableExists(targetDB, newName) {
			return nil, mysqlError(1050, "42S01", fmt.Sprintf("Table '%s' already exists", newName))
		}
		// Validate source exists (considering prior simulated renames)
		if !tableExists(srcDB, oldName) {
			return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", srcDB, oldName))
		}

		// Skip no-op renames (same table, same db) - MySQL allows this silently
		if isSameTable {
			continue
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
		// Handle temporary table tracking: if the renamed table was a temp table,
		// update the tempTables map and restore any saved permanent table.
		if e.tempTables != nil && e.tempTables[p.oldName] {
			delete(e.tempTables, p.oldName)
			e.tempTables[p.newName] = true
			// Restore the saved permanent table for the old name (if any).
			if saved, ok := e.tempTableSavedPermanent[p.oldName]; ok {
				delete(e.tempTableSavedPermanent, p.oldName)
				// Restore permanent table definition in catalog
				if restoreDef := saved.def; restoreDef != nil {
					catDB, _ := e.Catalog.GetDatabase(p.srcDB)
					if catDB != nil {
						_ = catDB.CreateTable(restoreDef)
					}
				}
				// Restore permanent table data in storage
				if saved.table != nil {
					e.Storage.RestoreTable(p.srcDB, p.oldName, saved.table)
				}
			}
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
	// Extract charset and collation from CREATE DATABASE options
	charset := ""
	collation := ""
	for _, opt := range stmt.CreateOptions {
		switch opt.Type {
		case sqlparser.CharacterSetType:
			newCS := strings.ToLower(strings.Trim(opt.Value, "'\""))
			if charset != "" && !strings.EqualFold(charset, newCS) {
				return nil, mysqlError(1302, "HY000", fmt.Sprintf("Conflicting declarations: 'CHARACTER SET %s' and 'CHARACTER SET %s'", charset, newCS))
			}
			charset = newCS
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
	// If no explicit collation, use the session-level collation_server.
	if collation == "" {
		if collVal, ok := e.getSysVarSession("collation_server"); ok && collVal != "" {
			collation = strings.ToLower(collVal)
		}
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
			if !charsetAliasEqual(collCharset, charset) {
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
		// Revert character_set_database/collation_database to the session-level
		// character_set_server/collation_server. New connections inherit the global
		// value as their session value, so this properly reflects per-connection state.
		if cs, ok := e.getSysVarSession("character_set_server"); ok && cs != "" {
			e.setSysVar("character_set_database", cs, false)
		}
		if coll, ok := e.getSysVarSession("collation_server"); ok && coll != "" {
			e.setSysVar("collation_database", coll, false)
		}
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
	// Check for conflicting CHARACTER SET declarations (MySQL error 1302).
	charset := ""
	collation := ""
	fullUpper := strings.ToUpper(strings.Join(fields[1:], " "))
	{
		remaining := fullUpper
		for {
			csIdx := strings.Index(remaining, "CHARACTER SET ")
			if csIdx < 0 {
				break
			}
			afterCS := strings.TrimSpace(remaining[csIdx+len("CHARACTER SET "):])
			afterCS = strings.TrimPrefix(afterCS, "= ")
			afterCS = strings.TrimPrefix(afterCS, "=")
			afterCS = strings.TrimSpace(afterCS)
			csFields := strings.Fields(afterCS)
			if len(csFields) > 0 {
				newCS := strings.ToLower(strings.Trim(csFields[0], "'\""))
				if charset != "" && !strings.EqualFold(charset, newCS) {
					return nil, mysqlError(1302, "HY000", fmt.Sprintf("Conflicting declarations: 'CHARACTER SET %s' and 'CHARACTER SET %s'", charset, newCS))
				}
				charset = newCS
			}
			remaining = remaining[csIdx+len("CHARACTER SET "):]
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
			if !charsetAliasEqual(collCharset, charset) {
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
		if e.CurrentDB == "" {
			return nil, mysqlError(1046, "3D000", "No database selected")
		}
		dbName = e.CurrentDB
	}
	// Empty identifier (e.g. ALTER DATABASE `` ...) is invalid
	if dbName == "" {
		return nil, mysqlError(1102, "42000", "Incorrect database name ''")
	}
	// Check identifier length before database lookup (MySQL error 1059 / ER_TOO_LONG_IDENT)
	if len(dbName) > 64 {
		return nil, mysqlError(1059, "42000", fmt.Sprintf("Identifier name '%s' is too long", dbName))
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

	// Handle qualified table names like `mysql.db` (db=mysql, table=db)
	tableDBName := e.CurrentDB
	actualTableName := tableName
	if dotIdx := strings.Index(tableName, "."); dotIdx >= 0 {
		tableDBName = tableName[:dotIdx]
		actualTableName = tableName[dotIdx+1:]
	}

	tbl, err := e.Storage.GetTable(tableDBName, actualTableName)
	if err != nil {
		return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", tableDBName, actualTableName))
	}
	orderCollation := ""
	if db, dbErr := e.Catalog.GetDatabase(tableDBName); dbErr == nil {
		if def, defErr := db.GetTable(actualTableName); defErr == nil {
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
				if _, alreadyTemp := e.tempTables[tableName]; alreadyTemp {
					return &Result{}, nil
				}
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
			// For TEMPORARY tables, save any existing permanent table before shadowing it.
			// We do this AFTER building selectSQL but BEFORE calling execCreateTableSelect
			// so that if the table shadows itself (e.g. CREATE TEMP t1 SELECT * FROM t1),
			// the SELECT still sees the original permanent table.
			// Strategy: execute the SELECT first, then swap tables.
			if stmt.Temp {
				if _, alreadyTemp := e.tempTables[tableName]; !alreadyTemp {
					// Execute SELECT while permanent table is still visible
					prevInsideDML := e.insideDML
					e.insideDML = true
					selResult, selErr := e.Execute(selectSQL)
					e.insideDML = prevInsideDML
					// Release any row locks acquired by the inner SELECT (insideDML=true causes
					// shared locks to be acquired; release them since CREATE TEMPORARY TABLE is not transactional).
					if e.rowLockManager != nil && !e.inTransaction {
						e.rowLockManager.ReleaseRowLocks(e.connectionID)
					}
					if selErr != nil {
						return nil, selErr
					}
					// Now save and drop the permanent table
					if existingDef, err2 := db.GetTable(tableName); err2 == nil {
						savedTable := e.Storage.SaveTable(dbName, tableName)
						e.tempTableSavedPermanent[tableName] = &savedPermTable{
							def:   existingDef,
							table: savedTable,
						}
						_ = db.DropTable(tableName)
						e.Storage.DropTable(dbName, tableName)
					}
					// Build column defs from SELECT result
					var cols []catalog.ColumnDef
					for _, colName := range selResult.Columns {
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
					newDef := &catalog.TableDef{Name: tableName, Columns: cols}
					if err2 := db.CreateTable(newDef); err2 != nil {
						return nil, mysqlError(1050, "42S01", fmt.Sprintf("Table '%s' already exists", tableName))
					}
					e.Storage.CreateTable(dbName, newDef)
					tbl, _ := e.Storage.GetTable(dbName, tableName)
					colTypeMap := make(map[string]string, len(cols))
					for _, col := range cols {
						colTypeMap[col.Name] = col.Type
					}
					for _, row := range selResult.Rows {
						sRow := make(storage.Row)
						for i, colName := range selResult.Columns {
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
					e.upsertInnoDBStatsRows(dbName, tableName, e.tableRowCount(dbName, tableName))
					e.tempTables[tableName] = true
					return &Result{}, nil
				}
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
		// This can happen when vitess cannot fully parse partition syntax
		// (e.g. NODEGROUP, RANGE COLUMNS with multi-value MAXVALUE, etc.).
		// Try re-parsing with the PARTITION BY clause stripped so the table
		// structure can still be created (partitioning is treated as a no-op).
		recoveredFromPartition := false
		if upper := strings.ToUpper(e.currentQuery); strings.Contains(upper, "PARTITION BY") {
			if stripped := stripCreateTablePartitionClause(e.currentQuery); stripped != "" {
				if newStmt, err2 := e.parser().Parse(stripped); err2 == nil {
					if ct2, ok := newStmt.(*sqlparser.CreateTable); ok && ct2.TableSpec != nil {
						stmt = ct2
						recoveredFromPartition = true
					}
				}
			}
		}
		if !recoveredFromPartition {
			// This happens when vitess accepts invalid syntax that MySQL rejects
			// (e.g. CHECK without parentheses, bare CONSTRAINT without key type).
			// Return a parse error to match MySQL behaviour.
			return nil, mysqlError(1064, "42000",
				"You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '' at line 1")
		}
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

	// CSV engine requires all columns to be explicitly NOT NULL (ER_CHECK_NOT_IMPLEMENTED = 1178)
	{
		engine := ""
		for _, opt := range stmt.TableSpec.Options {
			if strings.EqualFold(opt.Name, "ENGINE") {
				engine = strings.ToUpper(tableOptionString(opt))
				break
			}
		}
		if engine == "CSV" {
			for _, col := range stmt.TableSpec.Columns {
				isNotNull := col.Type.Options != nil && col.Type.Options.Null != nil && !*col.Type.Options.Null
				if !isNotNull {
					return nil, mysqlError(1178, "42000", "The storage engine for the table doesn't support nullable columns")
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
	// Also check for conflicting CHARACTER SET declarations (MySQL error 1302).
	var tableCharset string
	for _, opt := range stmt.TableSpec.Options {
		if strings.EqualFold(opt.Name, "CHARACTER SET") || strings.EqualFold(opt.Name, "CHARSET") {
			if tableCharset != "" && !strings.EqualFold(tableCharset, opt.String) {
				return nil, mysqlError(1302, "HY000", fmt.Sprintf("Conflicting declarations: 'CHARACTER SET %s' and 'CHARACTER SET %s'", tableCharset, opt.String))
			}
			tableCharset = opt.String
		}
	}

	for _, col := range stmt.TableSpec.Columns {
		// Validate CHAR/BINARY/VARCHAR/VARBINARY length limits
		colTypeLower := strings.ToLower(col.Type.Type)
		if col.Type.Length != nil {
			length := int64(*col.Type.Length)
			switch colTypeLower {
			case "char", "binary":
				if length > 255 {
					return nil, mysqlError(1074, "42000", fmt.Sprintf("Column length too big for column '%s' (max = 255); use BLOB or TEXT instead", col.Name.String()))
				}
			case "varchar", "varbinary":
				// For varchar(N) with N > 65535: MySQL promotes to text type silently,
				// UNLESS the column has a non-NULL default (which can't be stored in a text type).
				// In that case, error 1074 is returned.
				// We check this here before default processing; the promotion happens in buildColumnTypeString.
				if length > 65535 {
					hasNonNullDefault := col.Type.Options != nil && col.Type.Options.Default != nil &&
						!strings.EqualFold(sqlparser.String(col.Type.Options.Default), "null")
					if hasNonNullDefault {
						return nil, mysqlError(1074, "42000", fmt.Sprintf("Column length too big for column '%s' (max = 65535); use BLOB or TEXT instead", col.Name.String()))
					}
				}
			}
		}

		// Validate ENUM/SET value lengths (MySQL max is 255 characters).
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
			// However, if the resulting type is already a native binary/blob type (e.g. "long byte" → mediumblob),
			// don't add a charset since blob types don't have a charset.
			colTypeLowerCheck := strings.ToLower(strings.TrimSpace(colDef.Type))
			isBlobType := strings.HasSuffix(colTypeLowerCheck, "blob") || colTypeLowerCheck == "binary" ||
				strings.HasPrefix(colTypeLowerCheck, "binary(") || strings.HasPrefix(colTypeLowerCheck, "varbinary")
			if !isBlobType {
				cs := tableCharset
				if cs == "" {
					cs = "utf8mb4"
				}
				colDef.Charset = cs
				colDef.Collation = catalog.BinaryCollationForCharset(cs)
			}
		} else if strings.EqualFold(tableCharset, "binary") {
			// Table-level CHARACTER SET binary propagates to columns without explicit charset.
		}
		// Override/set collation from explicit COLLATE clause.
		if col.Type.Options != nil && col.Type.Options.Collate != "" {
			collLower := strings.ToLower(col.Type.Options.Collate)
			// Validate the collation is known.
			collCharset, collKnown := catalog.CharsetForCollation(collLower)
			if !collKnown {
				return nil, mysqlError(1273, "HY000", fmt.Sprintf("Unknown collation: '%s'", col.Type.Options.Collate))
			}
			// Validate collation is compatible with any explicit column charset.
			if colDef.Charset != "" && !charsetAliasEqual(collCharset, colDef.Charset) {
				return nil, mysqlError(1253, "42000", fmt.Sprintf("COLLATION '%s' is not valid for CHARACTER SET '%s'", col.Type.Options.Collate, colDef.Charset))
			}
			colDef.Collation = collLower
			// If no charset was set explicitly, derive it from the collation.
			if colDef.Charset == "" {
				colDef.Charset = collCharset
			}
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
					// Blob/Text/Geometry/JSON columns cannot have a non-empty non-NULL default value.
					// MySQL errors on non-empty defaults (e.g. default 'hello') and warns on empty defaults (default '').
					colTypeForDefault := strings.ToLower(colDef.Type)
					isBlobTextDefault := strings.Contains(colTypeForDefault, "blob") || strings.Contains(colTypeForDefault, "text") ||
						colTypeForDefault == "json" || colTypeForDefault == "geometry" ||
						colTypeForDefault == "point" || colTypeForDefault == "linestring" || colTypeForDefault == "polygon" ||
						colTypeForDefault == "multipoint" || colTypeForDefault == "multilinestring" || colTypeForDefault == "multipolygon" ||
						colTypeForDefault == "geometrycollection" || colTypeForDefault == "geomcollection"
					if isBlobTextDefault {
						// MySQL 8.0 allows expression-based defaults (e.g., DEFAULT (CAST(...) AS JSON))
						// for JSON/BLOB/TEXT columns. Only literal defaults are disallowed.
						_, isLiteralDefault := col.Type.Options.Default.(*sqlparser.Literal)
						if isLiteralDefault {
							// Get the raw default value to check if it's empty
							rawDefault := sqlparser.String(col.Type.Options.Default)
							// Strip quotes if present
							if len(rawDefault) >= 2 && rawDefault[0] == '\'' && rawDefault[len(rawDefault)-1] == '\'' {
								rawDefault = rawDefault[1 : len(rawDefault)-1]
							}
							if rawDefault != "" {
								return nil, mysqlError(1101, "42000", fmt.Sprintf("BLOB, TEXT, GEOMETRY or JSON column '%s' can't have a default value", col.Name.String()))
							}
							// Empty default '' on blob type: MySQL emits warning but allows it.
							// Skip default processing for this column (leave colDef.Default as nil).
						}
						// Expression default (non-literal): MySQL allows it, store it as-is.
						if !isLiteralDefault {
							defStr = sqlparser.String(col.Type.Options.Default)
							colDef.Default = &defStr
						}
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
						// Normalize now() / current_timestamp() to CURRENT_TIMESTAMP
						defStr = normalizeCurrentTimestampDefault(defStr)
						colDef.Default = &defStr
					}
				}
			}
			// Validate: ENUM/SET NOT NULL columns cannot have explicit NULL default (error 1067).
			if !nullable && col.Type.Options != nil && col.Type.Options.Default != nil {
				defStr2 := sqlparser.String(col.Type.Options.Default)
				if strings.EqualFold(defStr2, "null") {
					colTypeLower2 := strings.ToLower(strings.TrimSpace(col.Type.Type))
					if colTypeLower2 == "enum" || colTypeLower2 == "set" {
						return nil, mysqlError(1067, "42000", fmt.Sprintf("Invalid default value for '%s'", colDef.Name))
					}
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
			// Validate that PRIMARY KEY/KEY cannot be INVISIBLE at column level.
			// MySQL treats "col INT PRIMARY KEY INVISIBLE" as a parse error (ER_PARSE_ERROR).
			if col.Type.Options.Invisible != nil && (col.Type.Options.KeyOpt == sqlparser.ColKeyPrimary || col.Type.Options.KeyOpt == sqlparser.ColKey) {
				return nil, mysqlError(1064, "42000", fmt.Sprintf("You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'INVISIBLE )' at line 1"))
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
			// When character_set_client=binary, validate the comment as UTF-8
			if cs, _ := e.getSysVar("character_set_client"); strings.ToLower(cs) == "binary" {
				if err := validateUTF8StringForDDL(comment); err != nil {
					return nil, err
				}
			}
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

	// Pre-scan table options for ROW_FORMAT and ENGINE (needed for key length validation)
	tableRowFormat := ""
	tableEngine := "INNODB" // default
	for _, opt := range stmt.TableSpec.Options {
		name := strings.ToUpper(strings.TrimSpace(opt.Name))
		if name == "ROW_FORMAT" {
			tableRowFormat = strings.ToUpper(tableOptionString(opt))
		} else if name == "ENGINE" {
			tableEngine = strings.ToUpper(tableOptionString(opt))
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
		// Also check ER_WRONG_SUB_KEY (1089): prefix key on non-string column
		// MyISAM: max 1000 bytes; InnoDB COMPACT/REDUNDANT: 767; InnoDB DYNAMIC: 3072
		// For PRIMARY KEY or UNIQUE: error; for regular INDEX: warning (key silently used as-is)
		isUniqueOrPrimary := idx.Info.Type == sqlparser.IndexTypeUnique || idx.Info.Type == sqlparser.IndexTypePrimary
		for ci, idxCol := range idx.Columns {
			if idxCol.Length != nil {
				// ER_WRONG_SUB_KEY (1089): prefix lengths are only valid for string/blob columns
				colNameLower := strings.ToLower(idxCol.Column.String())
				for _, col := range columns {
					if strings.ToLower(col.Name) == colNameLower {
						colTypeUpper := strings.ToUpper(strings.TrimSpace(col.Type))
						// Strip generated column expression (GENERATED ALWAYS AS ...) before type check
						if genIdx := strings.Index(colTypeUpper, " GENERATED"); genIdx >= 0 {
							colTypeUpper = strings.TrimSpace(colTypeUpper[:genIdx])
						}
						if i := strings.IndexByte(colTypeUpper, '('); i >= 0 {
							colTypeUpper = colTypeUpper[:i]
						}
						colTypeUpper = strings.TrimSpace(colTypeUpper)
						isStringType := colTypeUpper == "CHAR" || colTypeUpper == "VARCHAR" ||
							colTypeUpper == "BINARY" || colTypeUpper == "VARBINARY" ||
							colTypeUpper == "TEXT" || colTypeUpper == "TINYTEXT" || colTypeUpper == "MEDIUMTEXT" || colTypeUpper == "LONGTEXT" ||
							colTypeUpper == "BLOB" || colTypeUpper == "TINYBLOB" || colTypeUpper == "MEDIUMBLOB" || colTypeUpper == "LONGBLOB"
						if !isStringType {
							return nil, mysqlError(1089, "HY000", "Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys")
						}
						break
					}
				}
				prefixLen := *idxCol.Length
				var maxPrefixLen int
				if tableEngine == "MYISAM" || tableEngine == "ARCHIVE" || tableEngine == "HEAP" || tableEngine == "MEMORY" {
					maxPrefixLen = 1000
				} else {
					maxPrefixLen = 3072 // InnoDB DYNAMIC default
					if tableRowFormat == "COMPACT" || tableRowFormat == "REDUNDANT" {
						maxPrefixLen = 767
					}
				}
				if prefixLen > maxPrefixLen {
					isMyISAM := tableEngine == "MYISAM" || tableEngine == "ARCHIVE" || tableEngine == "HEAP" || tableEngine == "MEMORY"
					if isUniqueOrPrimary || !isMyISAM {
						// InnoDB: always error for too-long key prefix
						// MyISAM: error only for UNIQUE/PRIMARY
						return nil, mysqlError(1071, "42000", fmt.Sprintf("Specified key was too long; max key length is %d bytes", maxPrefixLen))
					}
					// MyISAM regular index: add warning and truncate the stored prefix length
					e.addWarning("Warning", 1071, fmt.Sprintf("Specified key was too long; max key length is %d bytes", maxPrefixLen))
					// Determine bytes per character for this column's charset to compute truncated length
					colName := strings.ToLower(idxCol.Column.String())
					bytesPerChar := 1
					for _, col := range columns {
						if strings.ToLower(col.Name) == colName {
							cs := strings.ToLower(col.Charset)
							if cs == "" {
								cs = strings.ToLower(tableCharset)
							}
							if cs == "" {
								cs = "utf8mb4"
							}
							if cs == "utf8mb4" {
								bytesPerChar = 4
							} else if cs == "utf8" || cs == "utf8mb3" {
								bytesPerChar = 3
							} else if cs == "ucs2" || cs == "utf16" || cs == "utf16le" || cs == "utf32" {
								bytesPerChar = 4
							} else {
								bytesPerChar = 1
							}
							break
						}
					}
					truncatedLen := maxPrefixLen / bytesPerChar
					colStr := colName + fmt.Sprintf("(%d)", truncatedLen)
					idxCols[ci] = colStr
				}
			} else {
				// No explicit prefix length: check full column byte width against the max key length.
				// For VARCHAR/CHAR with multi-byte charsets, the full column can exceed the limit.
				colNameLower := strings.ToLower(idxCol.Column.String())
				for _, col := range columns {
					if strings.ToLower(col.Name) == colNameLower {
						colTypeUpper := strings.ToUpper(strings.TrimSpace(col.Type))
						baseType := colTypeUpper
						if i := strings.IndexByte(baseType, '('); i >= 0 {
							baseType = strings.TrimSpace(baseType[:i])
						}
						isStringType := baseType == "CHAR" || baseType == "VARCHAR" ||
							baseType == "BINARY" || baseType == "VARBINARY"
						if !isStringType {
							break
						}
						charLen := extractCharLength(col.Type)
						if charLen <= 0 {
							break
						}
						cs := strings.ToLower(col.Charset)
						if cs == "" {
							cs = strings.ToLower(tableCharset)
						}
						bpc := 1
						switch cs {
						case "utf8mb4", "":
							bpc = 4
						case "utf8", "utf8mb3":
							bpc = 3
						case "utf32", "utf16", "utf16le", "ucs2":
							bpc = 4
						}
						fullByteWidth := charLen * bpc
						var maxKeyLen int
						if tableEngine == "MYISAM" || tableEngine == "ARCHIVE" || tableEngine == "HEAP" || tableEngine == "MEMORY" {
							maxKeyLen = 1000
						} else {
							maxKeyLen = 3072
							if tableRowFormat == "COMPACT" || tableRowFormat == "REDUNDANT" {
								maxKeyLen = 767
							}
						}
						if fullByteWidth > maxKeyLen {
							isMyISAM := tableEngine == "MYISAM" || tableEngine == "ARCHIVE" || tableEngine == "HEAP" || tableEngine == "MEMORY"
							if isUniqueOrPrimary || !isMyISAM {
								return nil, mysqlError(1071, "42000", fmt.Sprintf("Specified key was too long; max key length is %d bytes", maxKeyLen))
							}
							e.addWarning("Warning", 1071, fmt.Sprintf("Specified key was too long; max key length is %d bytes", maxKeyLen))
						}
						break
					}
				}
			}
		}

		// Validate that PRIMARY KEY cannot be INVISIBLE (ER_PK_INDEX_CANT_BE_INVISIBLE = 3895).
		// This must be checked before the if/else block since PRIMARY KEY takes the if branch.
		{
			for _, opt := range idx.Options {
				if strings.EqualFold(opt.Name, "INVISIBLE") && idx.Info.Type == sqlparser.IndexTypePrimary {
					return nil, mysqlError(3895, "HY000", "A primary key index cannot be invisible")
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

	// Resolve duplicate index names: MySQL auto-renames duplicates by appending _2, _3, etc.
	// (but skip PRIMARY KEY and UNIQUE constraints that have explicit names).
	{
		usedNames := make(map[string]bool)
		for i := range indexes {
			baseName := indexes[i].Name
			if baseName == "" {
				continue
			}
			nameLower := strings.ToLower(baseName)
			if !usedNames[nameLower] {
				usedNames[nameLower] = true
			} else {
				// Collision: find a unique suffix
				for suffix := 2; ; suffix++ {
					candidate := fmt.Sprintf("%s_%d", baseName, suffix)
					candidateLower := strings.ToLower(candidate)
					if !usedNames[candidateLower] {
						indexes[i].Name = candidate
						usedNames[candidateLower] = true
						break
					}
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

	// Validate key length for inline PRIMARY KEY column definitions.
	// Explicit index PRIMARY KEY entries are checked in the index loop above.
	// Inline column-level "PRIMARY KEY" (ColKeyPrimary) bypasses that loop,
	// so we check here: for each primary key column, verify full byte width.
	if len(primaryKeys) > 0 {
		var maxKeyLen int
		if tableEngine == "MYISAM" || tableEngine == "ARCHIVE" || tableEngine == "HEAP" || tableEngine == "MEMORY" {
			maxKeyLen = 1000
		} else {
			maxKeyLen = 3072
			if tableRowFormat == "COMPACT" || tableRowFormat == "REDUNDANT" {
				maxKeyLen = 767
			}
		}
		for _, pkCol := range primaryKeys {
			for _, col := range columns {
				if !strings.EqualFold(col.Name, pkCol) {
					continue
				}
				colTypeUpper := strings.ToUpper(strings.TrimSpace(col.Type))
				baseType := colTypeUpper
				if i := strings.IndexByte(baseType, '('); i >= 0 {
					baseType = strings.TrimSpace(baseType[:i])
				}
				isStringType := baseType == "CHAR" || baseType == "VARCHAR" ||
					baseType == "BINARY" || baseType == "VARBINARY"
				if !isStringType {
					break
				}
				charLen := extractCharLength(col.Type)
				if charLen <= 0 {
					break
				}
				cs := strings.ToLower(col.Charset)
				if cs == "" {
					cs = strings.ToLower(tableCharset)
				}
				bpc := 1
				switch cs {
				case "utf8mb4", "":
					bpc = 4
				case "utf8", "utf8mb3":
					bpc = 3
				case "utf32", "utf16", "utf16le", "ucs2":
					bpc = 4
				}
				if charLen*bpc > maxKeyLen {
					return nil, mysqlError(1071, "42000", fmt.Sprintf("Specified key was too long; max key length is %d bytes", maxKeyLen))
				}
				break
			}
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
			csVal := strings.ToLower(opt.String)
			if !isKnownCharset(csVal) {
				return nil, mysqlError(1115, "42000", fmt.Sprintf("Unknown character set: '%s'", csVal))
			}
			def.Charset = csVal
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
			if !charsetAliasEqual(collCharset, def.Charset) {
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
		} else {
			// This session already has a temp table with this name.
			// Without IF NOT EXISTS, MySQL returns error 1050.
			if !stmt.IfNotExists {
				return nil, mysqlError(1050, "42S01", fmt.Sprintf("Table '%s' already exists", tableName))
			}
			// IF NOT EXISTS: emit Note warning and skip.
			e.warnings = append(e.warnings, Warning{Level: "Note", Code: 1050, Message: fmt.Sprintf("Table '%s' already exists", tableName)})
			return &Result{}, nil
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
		// If a TEMP table exists with this name and we're creating a PERMANENT table,
		// save the permanent def as "pending" - it will become visible when the temp is dropped.
		if !stmt.Temp && e.tempTables != nil && e.tempTables[tableName] {
			if e.pendingPermanentWhileTemp == nil {
				e.pendingPermanentWhileTemp = make(map[string]*savedPermTable)
			}
			// Save just the definition (no rows yet - new table)
			e.pendingPermanentWhileTemp[tableName] = &savedPermTable{def: def, table: nil}
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
	// DDL also releases all table-level locks (MySQL behavior).
	// Only do this for non-TEMP tables since TEMP drops don't affect lock state.
	if !stmt.Temp && e.tableLockManager != nil {
		e.tableLockManager.UnlockAll(e.connectionID)
	}
	// For DROP TEMPORARY TABLE, validate all tables first (atomically).
	// If any table is not a temporary table, fail without dropping anything.
	if stmt.Temp && !stmt.IfExists {
		for _, table := range stmt.FromTables {
			tableName := table.Name.String()
			dbName := e.CurrentDB
			if !table.Qualifier.IsEmpty() {
				dbName = table.Qualifier.String()
			}
			if _, isTemp := e.tempTables[tableName]; !isTemp {
				return nil, mysqlError(1051, "42S02", fmt.Sprintf("Unknown table '%s.%s'", dbName, tableName))
			}
		}
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
		// For DROP TEMPORARY TABLE with IF EXISTS, skip non-temp tables with a warning.
		if stmt.Temp && stmt.IfExists {
			if _, isTemp := e.tempTables[tableName]; !isTemp {
				if e.sqlNotesEnabled() {
					e.addWarning("Note", 1051, fmt.Sprintf("Unknown table '%s.%s'", dbName, tableName))
				}
				continue
			}
		}
		db, err := e.Catalog.GetDatabase(dbName)
		if err != nil {
			// For TEMPORARY tables, even if the database was dropped, clean up temp tracking.
			if _, isTemp := e.tempTables[tableName]; isTemp {
				e.Storage.DropTable(dbName, tableName)
				delete(e.tempTables, tableName)
				delete(e.tempTableSavedPermanent, tableName)
				if e.pendingPermanentWhileTemp != nil {
					delete(e.pendingPermanentWhileTemp, tableName)
				}
				continue
			}
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
			// If a permanent table was created while this temp existed, now add it to catalog.
			if e.pendingPermanentWhileTemp != nil {
				if pending, ok := e.pendingPermanentWhileTemp[tableName]; ok {
					if db2, err2 := e.Catalog.GetDatabase(dbName); err2 == nil {
						_ = db2.CreateTable(pending.def)
					}
					if pending.def != nil {
						e.Storage.CreateTable(dbName, pending.def)
					}
					delete(e.pendingPermanentWhileTemp, tableName)
				}
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
	// Capture column-level charset if explicitly specified (same logic as CREATE TABLE path).
	if col.Type.Charset.Name != "" {
		csLower := strings.ToLower(col.Type.Charset.Name)
		if csLower != "binary" {
			colDef.Charset = csLower
			colDef.Collation = catalog.DefaultCollationForCharset(colDef.Charset)
		}
	}
	// Override/set collation from explicit COLLATE clause.
	if col.Type.Options != nil && col.Type.Options.Collate != "" {
		collLower := strings.ToLower(col.Type.Options.Collate)
		colDef.Collation = collLower
		if colDef.Charset == "" {
			if collCharset, ok := catalog.CharsetForCollation(collLower); ok {
				colDef.Charset = collCharset
			}
		}
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
			// Normalize now() / current_timestamp() to CURRENT_TIMESTAMP
			defStr = normalizeCurrentTimestampDefault(defStr)
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

	// Check for WITH VALIDATION / WITHOUT VALIDATION clauses
	// MySQL restricts WITH VALIDATION to index-only changes (no column modifications)
	withValidation := false
	for _, opt := range stmt.AlterOptions {
		if validation, ok := opt.(*sqlparser.Validation); ok && validation.With {
			withValidation = true
			// WITH VALIDATION is not allowed with index-only changes
			hasColumnChange := false
			for _, o := range stmt.AlterOptions {
				switch o.(type) {
				case *sqlparser.AddColumns, *sqlparser.DropColumn,
					*sqlparser.ModifyColumn, *sqlparser.ChangeColumn:
					hasColumnChange = true
				}
			}
			if !hasColumnChange {
				return nil, mysqlError(3869, "HY000", "Incorrect usage of ALTER and WITH VALIDATION")
			}
		}
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
						// When NO_ENGINE_SUBSTITUTION is active, return error; otherwise substitute with InnoDB and warn.
						if strings.Contains(e.sqlMode, "NO_ENGINE_SUBSTITUTION") {
							return nil, mysqlError(1286, "42000", fmt.Sprintf("Unknown storage engine '%s'", tableOptionString(to)))
						}
						e.warnings = append(e.warnings, Warning{Level: "Warning", Code: 1286, Message: fmt.Sprintf("Unknown storage engine '%s'", tableOptionString(to))})
						// Engine substitution: proceed with InnoDB (handled via the table option being stored as unknown;
						// subsequent logic will use INNODB as the stored engine)
					}
					// MEMORY and similar engines cannot be used for log tables
					if isMySQLLogTable(dbName, tableName) {
						if engineVal == "MEMORY" || engineVal == "HEAP" {
							return nil, mysqlError(1579, "HY000", "This storage engine cannot be used for log tables")
						}
					}
					// Reject non-InnoDB engines not supported by mylite
					switch engineVal {
					case "MYISAM", "MEMORY", "HEAP", "MERGE", "MRG_MYISAM", "BLACKHOLE", "ARCHIVE":
						return nil, ErrUnsupported(fmt.Sprintf("ENGINE=%s (only InnoDB is supported)", tableOptionString(to)))
					}
				}
			}
		}
	}

	// Pre-check: PRIMARY KEY cannot be INVISIBLE. MySQL checks this before table lookup.
	for _, opt := range stmt.AlterOptions {
		if addIdx, ok := opt.(*sqlparser.AddIndexDefinition); ok {
			if addIdx.IndexDefinition.Info.Type == sqlparser.IndexTypePrimary {
				for _, idxOpt := range addIdx.IndexDefinition.Options {
					if strings.EqualFold(idxOpt.Name, "INVISIBLE") {
						return nil, mysqlError(3895, "HY000", "A primary key index cannot be invisible")
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
		// WITH VALIDATION + ALGORITHM=INPLACE is not supported for virtual generated columns
		// MySQL requires ALGORITHM=COPY to perform validation scans
		if isInplace && withValidation {
			return nil, mysqlError(1846, "0A000", "ALGORITHM=INPLACE is not supported for this operation. Try ALGORITHM=COPY.")
		}
	}

	// Pre-check: CSV engine requires all columns to be NOT NULL (ER_CHECK_NOT_IMPLEMENTED = 1178)
	// This applies to ADD COLUMN, MODIFY COLUMN, and CHANGE COLUMN on CSV tables.
	{
		tableEngine := ""
		if tableDef, tdErr := db.GetTable(tableName); tdErr == nil && tableDef != nil {
			tableEngine = strings.ToUpper(tableDef.Engine)
		}
		// Also check if ALTER TABLE itself changes the engine to CSV
		for _, opt := range stmt.AlterOptions {
			if tblOpts, ok := opt.(sqlparser.TableOptions); ok {
				for _, to := range tblOpts {
					if strings.EqualFold(to.Name, "ENGINE") {
						tableEngine = strings.ToUpper(tableOptionString(to))
					}
				}
			}
		}
		if tableEngine == "CSV" {
			for _, opt := range stmt.AlterOptions {
				switch op := opt.(type) {
				case *sqlparser.AddColumns:
					for _, col := range op.Columns {
						isNotNull := col.Type.Options != nil && col.Type.Options.Null != nil && !*col.Type.Options.Null
						if !isNotNull {
							return nil, mysqlError(1178, "42000", "The storage engine for the table doesn't support nullable columns")
						}
					}
				case *sqlparser.ModifyColumn:
					col := op.NewColDefinition
					isNotNull := col.Type.Options != nil && col.Type.Options.Null != nil && !*col.Type.Options.Null
					if !isNotNull {
						return nil, mysqlError(1178, "42000", "The storage engine for the table doesn't support nullable columns")
					}
				case *sqlparser.ChangeColumn:
					col := op.NewColDefinition
					isNotNull := col.Type.Options != nil && col.Type.Options.Null != nil && !*col.Type.Options.Null
					if !isNotNull {
						return nil, mysqlError(1178, "42000", "The storage engine for the table doesn't support nullable columns")
					}
				}
			}
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

	// Pre-validate all ADD COLUMN operations: in strict mode with NO_ZERO_DATE, adding a NOT NULL
	// date/datetime/timestamp column without a default to a non-empty table fails. We must check
	// ALL AddColumns opcodes before processing any, so partial application doesn't occur.
	if e.isStrictMode() {
		hasNoZeroDate := strings.Contains(e.sqlMode, "NO_ZERO_DATE") || strings.Contains(e.sqlMode, "TRADITIONAL")
		if hasNoZeroDate {
			tbl.Mu.RLock()
			hasRows := len(tbl.Rows) > 0
			tbl.Mu.RUnlock()
			if hasRows {
				for _, opt := range stmt.AlterOptions {
					if ac, ok := opt.(*sqlparser.AddColumns); ok {
						for _, col := range ac.Columns {
							if col.Type.Options == nil || col.Type.Options.Default == nil {
								isNotNull := col.Type.Options != nil && col.Type.Options.Null != nil && !*col.Type.Options.Null
								if isNotNull {
									colTypeBase := strings.ToUpper(strings.TrimSpace(col.Type.Type))
									if idx := strings.IndexByte(colTypeBase, '('); idx >= 0 {
										colTypeBase = colTypeBase[:idx]
									}
									colTypeBase = strings.TrimSpace(colTypeBase)
									if colTypeBase == "DATE" || colTypeBase == "DATETIME" || colTypeBase == "TIMESTAMP" {
										return nil, mysqlError(1292, "22007", fmt.Sprintf("Incorrect date value: '0000-00-00' for column '%s' at row 1", col.Name.String()))
									}
								}
							}
						}
					}
				}
			}
		}
	}

	// Pre-validate: check if DROP COLUMN operations would remove all columns.
	{
		tableDef, _ := db.GetTable(tableName)
		if tableDef != nil {
			netCols := len(tableDef.Columns)
			for _, altOpt := range stmt.AlterOptions {
				switch altOp := altOpt.(type) {
				case *sqlparser.DropColumn:
					// Only count existing columns in the drop
					dropName := strings.ToLower(altOp.Name.Name.String())
					for _, col := range tableDef.Columns {
						if strings.ToLower(col.Name) == dropName {
							netCols--
							break
						}
					}
				case *sqlparser.AddColumns:
					netCols += len(altOp.Columns)
				}
			}
			if netCols <= 0 {
				return nil, mysqlError(1090, "42000", "You can't delete all columns with ALTER TABLE; use DROP TABLE instead")
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
				// YEAR column only supports YEAR or YEAR(4).
				if strings.EqualFold(col.Type.Type, "year") && col.Type.Length != nil {
					yearLen := int(*col.Type.Length)
					if yearLen != 4 {
						return nil, mysqlError(1818, "HY000", "Supports only YEAR or YEAR(4) column.")
					}
				}
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
				// When character_set_client=binary, validate the comment as UTF-8
				if cs, _ := e.getSysVar("character_set_client"); strings.ToLower(cs) == "binary" {
					if col.Type.Options != nil && col.Type.Options.Comment != nil {
						if err := validateUTF8StringForDDL(colDef.Comment); err != nil {
							return nil, err
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
						// Check if value is out of range for the column type
						// Only enforce range check when WITH VALIDATION is specified
						if withValidation {
							if rangeErr := checkIntegerRangeForColumn(colDef.Type, colDef.Name, v); rangeErr != nil {
								tbl.Mu.Unlock()
								db.DropColumn(tableName, colDef.Name)
								tbl.DropColumn(colDef.Name)
								return nil, rangeErr
							}
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
			if dropErr := db.DropColumn(tableName, colName); dropErr != nil {
				return nil, dropErr
			}
			tbl.DropColumn(colName)

		case *sqlparser.ModifyColumn:
			colDef := columnDefFromAST(op.NewColDefinition)
			// YEAR column only supports YEAR or YEAR(4).
			if strings.EqualFold(op.NewColDefinition.Type.Type, "year") && op.NewColDefinition.Type.Length != nil {
				yearLen := int(*op.NewColDefinition.Type.Length)
				if yearLen != 4 {
					return nil, mysqlError(1818, "HY000", "Supports only YEAR or YEAR(4) column.")
				}
			}
			if mysqlCharLen(colDef.Comment) > 1024 {
				if e.isStrictMode() {
					return nil, mysqlError(1629, "HY000", fmt.Sprintf("Comment for field '%s' is too long (max = 1024)", colDef.Name))
				}
				colDef.Comment = mysqlTruncateChars(colDef.Comment, 1024)
			}
			// Check for STORED<->VIRTUAL change on generated columns; also capture old column type.
			var oldColType string
			if tableDef, tdErr := db.GetTable(tableName); tdErr == nil {
				for _, existCol := range tableDef.Columns {
					if strings.EqualFold(existCol.Name, colDef.Name) {
						oldColType = existCol.Type
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
			// When converting CHAR → VARCHAR, MySQL strips trailing spaces from stored values
			// (since CHAR pads with spaces, but VARCHAR preserves exact content).
			oldIsChar := strings.HasPrefix(strings.ToLower(strings.TrimSpace(oldColType)), "char(") || strings.EqualFold(strings.TrimSpace(oldColType), "char")
			newIsVarchar := strings.HasPrefix(strings.ToLower(strings.TrimSpace(colDef.Type)), "varchar(")
			charToVarchar := oldIsChar && newIsVarchar
			if modErr := db.ModifyColumn(tableName, colDef); modErr != nil {
				return nil, modErr
			}
			// Recompute generated column values if expression changed
			if genExpr := generatedColumnExpr(colDef.Type); genExpr != "" {
				tbl.Lock()
				var rangeErr error
				for i := range tbl.Rows {
					if v, err := e.evalGeneratedColumnExpr(genExpr, tbl.Rows[i]); err == nil {
						// For MODIFY COLUMN, always check range (existing data must fit new type)
						if re := checkIntegerRangeForColumn(colDef.Type, colDef.Name, v); re != nil {
							rangeErr = re
							break
						}
						tbl.Rows[i][colDef.Name] = v
					}
				}
				tbl.Unlock()
				if rangeErr != nil {
					return nil, rangeErr
				}
			} else {
				// VARCHAR/CHAR truncation: compute new max length once.
				newColTypeLower := strings.ToLower(strings.TrimSpace(colDef.Type))
				newMaxLen := 0
				isVarcharOrChar := strings.HasPrefix(newColTypeLower, "varchar(") || strings.HasPrefix(newColTypeLower, "char(")
				if isVarcharOrChar {
					newMaxLen = extractCharLength(colDef.Type)
				}
				tbl.Lock()
				truncWarningIssued := false
				for i := range tbl.Rows {
					if cur, ok := tbl.Rows[i][colDef.Name]; ok {
						if cur == nil && !colDef.Nullable {
							// ALTER TABLE MODIFY COLUMN x NOT NULL: convert NULL to zero value
							tbl.Rows[i][colDef.Name] = implicitDefaultForType(colDef.Type)
						} else {
							val := e.coerceValueForColumnTypeForWrite(colDef, cur)
							// CHAR → VARCHAR: strip trailing spaces from stored padded values
							if charToVarchar {
								if s, ok := val.(string); ok {
									val = strings.TrimRight(s, " ")
								}
							}
							// VARCHAR/CHAR: truncate to new column max length if too long
							if isVarcharOrChar && newMaxLen > 0 {
								if s, ok := val.(string); ok {
									runes := []rune(s)
									if len(runes) > newMaxLen {
										val = string(runes[:newMaxLen])
										if !truncWarningIssued {
											e.addWarning("Warning", 1265, fmt.Sprintf("Data truncated for column '%s' at row %d", colDef.Name, i+1))
											truncWarningIssued = true
										}
									}
								}
							}
							tbl.Rows[i][colDef.Name] = val
						}
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
			// Capture old column type to detect CHAR→VARCHAR conversion.
			var oldColTypeChg string
			if tableDef2, tdErr2 := db.GetTable(tableName); tdErr2 == nil {
				for _, existCol := range tableDef2.Columns {
					if strings.EqualFold(existCol.Name, oldName) {
						oldColTypeChg = existCol.Type
						break
					}
				}
			}
			oldIsCharChg := strings.HasPrefix(strings.ToLower(strings.TrimSpace(oldColTypeChg)), "char(") || strings.EqualFold(strings.TrimSpace(oldColTypeChg), "char")
			newIsVarcharChg := strings.HasPrefix(strings.ToLower(strings.TrimSpace(colDef.Type)), "varchar(")
			charToVarcharChg := oldIsCharChg && newIsVarcharChg
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
				var rangeErr2 error
				for i := range tbl.Rows {
					if v, err := e.evalGeneratedColumnExpr(genExpr, tbl.Rows[i]); err == nil {
						// For CHANGE COLUMN, always check range (existing data must fit new type)
						if re := checkIntegerRangeForColumn(colDef.Type, colDef.Name, v); re != nil {
							rangeErr2 = re
							break
						}
						tbl.Rows[i][colDef.Name] = v
					}
				}
				tbl.Unlock()
				if rangeErr2 != nil {
					return nil, rangeErr2
				}
			} else {
				// VARCHAR/CHAR truncation: compute new max length once.
				newColTypeLowerChg := strings.ToLower(strings.TrimSpace(colDef.Type))
				newMaxLenChg := 0
				isVarcharOrCharChg := strings.HasPrefix(newColTypeLowerChg, "varchar(") || strings.HasPrefix(newColTypeLowerChg, "char(")
				if isVarcharOrCharChg {
					newMaxLenChg = extractCharLength(colDef.Type)
				}
				tbl.Lock()
				truncWarnChg := false
				for i := range tbl.Rows {
					if cur, ok := tbl.Rows[i][colDef.Name]; ok {
						if cur == nil && !colDef.Nullable {
							// ALTER TABLE CHANGE COLUMN x NOT NULL: convert NULL to zero value
							tbl.Rows[i][colDef.Name] = implicitDefaultForType(colDef.Type)
						} else {
							val := e.coerceValueForColumnTypeForWrite(colDef, cur)
							// CHAR → VARCHAR: strip trailing spaces from stored padded values
							if charToVarcharChg {
								if s, ok := val.(string); ok {
									val = strings.TrimRight(s, " ")
								}
							}
							// VARCHAR/CHAR: truncate to new column max length if too long
							if isVarcharOrCharChg && newMaxLenChg > 0 {
								if s, ok := val.(string); ok {
									runes := []rune(s)
									if len(runes) > newMaxLenChg {
										val = string(runes[:newMaxLenChg])
										if !truncWarnChg {
											e.addWarning("Warning", 1265, fmt.Sprintf("Data truncated for column '%s' at row %d", colDef.Name, i+1))
											truncWarnChg = true
										}
									}
								}
							}
							tbl.Rows[i][colDef.Name] = val
						}
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
				altTableEngine := "INNODB"
				if tableDef, tdErr2 := db.GetTable(tableName); tdErr2 == nil {
					tableRowFormat = strings.ToUpper(tableDef.RowFormat)
					altTableEngine = strings.ToUpper(tableDef.Engine)
				}
				isUniqueOrPrimaryIdx := op.IndexDefinition.Info.Type == sqlparser.IndexTypeUnique || op.IndexDefinition.Info.Type == sqlparser.IndexTypePrimary
				for _, idxCol := range op.IndexDefinition.Columns {
					if idxCol.Length != nil {
						prefixLen := *idxCol.Length
						var maxPrefixLen int
						if altTableEngine == "MYISAM" || altTableEngine == "ARCHIVE" || altTableEngine == "HEAP" || altTableEngine == "MEMORY" {
							maxPrefixLen = 1000
						} else {
							maxPrefixLen = 3072 // InnoDB DYNAMIC default
							if tableRowFormat == "COMPACT" || tableRowFormat == "REDUNDANT" {
								maxPrefixLen = 767
							}
						}
						if prefixLen > maxPrefixLen {
							isMyISAMAlt := altTableEngine == "MYISAM" || altTableEngine == "ARCHIVE" || altTableEngine == "HEAP" || altTableEngine == "MEMORY"
							if isUniqueOrPrimaryIdx || !isMyISAMAlt {
								return nil, mysqlError(1071, "42000", fmt.Sprintf("Specified key was too long; max key length is %d bytes", maxPrefixLen))
							}
							e.addWarning("Warning", 1071, fmt.Sprintf("Specified key was too long; max key length is %d bytes", maxPrefixLen))
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
			// Validate index name length (MySQL max is 64 characters)
			if len(idxName) > 64 {
				return nil, mysqlError(1059, "42000", fmt.Sprintf("Identifier name '%s' is too long", idxName))
			}
			// Auto-deduplicate index name: if a key with this name already exists,
			// append _2, _3, etc. (MySQL behavior for ADD KEY without explicit name)
			if idxName != "" && op.IndexDefinition.Info.Name.String() == "" && tdErr == nil {
				baseIdxName := idxName
				suffix := 2
				for {
					taken := false
					for _, existing := range tableDef.Indexes {
						if strings.EqualFold(existing.Name, idxName) {
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
			// Validate that PRIMARY KEY cannot be INVISIBLE (ER_PK_INDEX_CANT_BE_INVISIBLE = 3895)
			if idxInvisible && isPrimary {
				return nil, mysqlError(3895, "HY000", "A primary key index cannot be invisible")
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
					displayParts := make([]string, len(baseCols))
					hasNull := false
					for ci, c := range baseCols {
						v := rowValueByColumnName(row, c)
						if v == nil {
							keyParts[ci] = "NULL"
							displayParts[ci] = "NULL"
							hasNull = true
						} else {
							s := fmt.Sprintf("%v", v)
							// Apply prefix length and trim trailing nulls/spaces
							if prefixLens[ci] > 0 && len(s) > prefixLens[ci] {
								s = s[:prefixLens[ci]]
							}
							s = strings.TrimRight(s, "\x00 ")
							keyParts[ci] = s
							// For error message display, format BIT values as binary byte strings
							colType := tbl.Def.ColType(c)
							displayParts[ci] = storage.DisplayValueWithColType(v, colType)
						}
					}
					if hasNull {
						continue // rows with any NULL in the key don't violate uniqueness
					}
					key := strings.Join(keyParts, "-")
					if seen[key] {
						return nil, mysqlError(1062, "23000", fmt.Sprintf("Duplicate entry '%s' for key '%s'", strings.Join(displayParts, "-"), idxName))
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
							// Validate: NOT NULL ENUM/SET cannot have NULL default
							if strings.EqualFold(defStr, "null") && !col.Nullable {
								colTypeLower2 := strings.ToLower(strings.TrimSpace(col.Type))
								if strings.HasPrefix(colTypeLower2, "enum(") || strings.HasPrefix(colTypeLower2, "set(") {
									return nil, mysqlError(1067, "42000", fmt.Sprintf("Invalid default value for '%s'", colName))
								}
							}
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
			// PRIMARY KEY cannot be made invisible via ALTER INDEX PRIMARY.
			// MySQL treats this as a parse error since PRIMARY is a keyword.
			if op.Invisible && strings.EqualFold(idxName, "PRIMARY") {
				return nil, mysqlError(1064, "42000", "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'PRIMARY INVISIBLE' at line 1")
			}
			tableDef, _ := db.GetTable(tableName)
			if tableDef != nil {
				for i, idx := range tableDef.Indexes {
					if strings.EqualFold(idx.Name, idxName) {
						// If making invisible: check if this is an implicit primary key
						// (first NOT NULL UNIQUE when table has no explicit primary key)
						if op.Invisible && !hasPrimaryKey(tableDef) && isFirstNotNullUnique(tableDef, idx) {
							return nil, mysqlError(3895, "HY000", "A primary key index cannot be invisible")
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
			// Determine target database: use qualifier if given, else current db
			targetDBName := e.CurrentDB
			if !op.Table.Qualifier.IsEmpty() {
				targetDBName = op.Table.Qualifier.String()
			}
			targetDB, targetDBErr := e.Catalog.GetDatabase(targetDBName)
			if targetDBErr != nil {
				return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", targetDBName))
			}
			// Get the current table def
			def, getErr := db.GetTable(tableName)
			if getErr != nil {
				return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", dbName, tableName))
			}
			// Determine if the table being renamed is a temporary table
			isRenamingTemp := e.tempTables != nil && e.tempTables[tableName]
			// Check new name doesn't already exist (skip for temp tables - they can shadow permanent ones)
			// Also skip if renaming to the same name (no-op, MySQL allows this)
			isSameName := strings.EqualFold(dbName, targetDBName) && strings.EqualFold(tableName, newName)
			if !isSameName {
				if isRenamingTemp {
					// If renaming a temp table to a name that already has a temp table, fail with 1050.
					if e.tempTables != nil && e.tempTables[newName] {
						return nil, mysqlError(1050, "42S01", fmt.Sprintf("Table '%s' already exists", newName))
					}
				} else if _, getErr := targetDB.GetTable(newName); getErr == nil {
					return nil, mysqlError(1050, "42S01", fmt.Sprintf("Table '%s' already exists", newName))
				}
			}
			// If renaming a temp table, save any permanent table at newName BEFORE the rename
			if isRenamingTemp {
				if _, alreadySaved := e.tempTableSavedPermanent[newName]; !alreadySaved {
					if existingPermDef, err2 := targetDB.GetTable(newName); err2 == nil {
						savedTable := e.Storage.SaveTable(targetDBName, newName)
						e.tempTableSavedPermanent[newName] = &savedPermTable{
							def:   existingPermDef,
							table: savedTable,
						}
						_ = targetDB.DropTable(newName)
						e.Storage.DropTable(targetDBName, newName)
					}
				}
			}
			// Rename in catalog (skip for no-op same-name renames)
			if !isSameName {
				def.Name = newName
				db.DropTable(tableName) //nolint:errcheck
				targetDB.CreateTable(def) //nolint:errcheck
				// Rename in storage
				e.Storage.CreateTable(targetDBName, def)
				if newTbl, getErr := e.Storage.GetTable(targetDBName, newName); getErr == nil {
					newTbl.Rows = tbl.Rows
					newTbl.AutoIncrement.Store(tbl.AutoIncrementValue())
				}
				e.Storage.DropTable(dbName, tableName)
			}
			// Handle temporary table tracking: if the renamed table was a temp table,
			// update the tempTables map and restore any saved permanent table for old name.
			if isRenamingTemp {
				delete(e.tempTables, tableName)
				e.tempTables[newName] = true
				// Restore the saved permanent table for the old name (if any).
				if saved, ok := e.tempTableSavedPermanent[tableName]; ok {
					delete(e.tempTableSavedPermanent, tableName)
					if saved.def != nil {
						_ = db.CreateTable(saved.def)
					}
					if saved.table != nil {
						e.Storage.RestoreTable(dbName, tableName, saved.table)
					}
				}
				// If a permanent table was created while this temp existed, now add it to catalog.
				if e.pendingPermanentWhileTemp != nil {
					if pending, ok := e.pendingPermanentWhileTemp[tableName]; ok {
						if pending.def != nil {
							_ = db.CreateTable(pending.def)
							e.Storage.CreateTable(dbName, pending.def)
						}
						delete(e.pendingPermanentWhileTemp, tableName)
					}
				}
			}
			// Update tableName/dbName for any subsequent ALTER operations
			tableName = newName
			dbName = targetDBName
			db = targetDB
			tbl, _ = e.Storage.GetTable(targetDBName, newName)

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
	// Handle HexBytes (x'...' literal): decode hex string to raw bytes, then pad
	if hb, ok := val.(HexBytes); ok {
		decoded, err := hex.DecodeString(string(hb))
		if err != nil {
			decoded = []byte{}
		}
		s := string(decoded)
		if len(s) < padLen {
			s = s + strings.Repeat("\x00", padLen-len(s))
		} else if len(s) > padLen {
			s = s[:padLen]
		}
		return s
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

	// Normalize MySQL type aliases
	// "long" and "long text" → mediumtext; "long binary"/"long byte" → mediumblob
	// Also: "long" with BINARY modifier (ct.Charset.Binary=true) means "long byte" → mediumblob
	// Note: vitess pre-normalizes "long" → MEDIUMTEXT internally, so we check both.
	switch strings.ToUpper(s) {
	case "LONG":
		if ct.Charset.Binary {
			s = "mediumblob"
		} else {
			s = "mediumtext"
		}
	case "LONG VARBINARY", "LONG BINARY", "LONG BYTE":
		s = "mediumblob"
	}

	// When the BINARY modifier is set on a LOB text type, convert to the binary equivalent.
	// e.g. "long byte" → vitess parses as MEDIUMTEXT + Charset.Binary=true → mediumblob
	// Note: BINARY modifier on non-LOB types (varchar, char) means binary collation, NOT type conversion.
	if ct.Charset.Binary && ct.Charset.Name == "" {
		switch strings.ToUpper(s) {
		case "TINYTEXT":
			s = "tinyblob"
		case "TEXT":
			s = "blob"
		case "MEDIUMTEXT":
			s = "mediumblob"
		case "LONGTEXT":
			s = "longblob"
		}
	}

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
	// Promote blob/text/varbinary/varchar types based on declared length
	if ct.Length != nil {
		// Determine effective charset for byte-length calculation
		effectiveCharset := ct.Charset.Name
		if effectiveCharset == "" {
			effectiveCharset = tableCharset
		}
		s = promoteBlobTextType(s, ct.Type, int64(*ct.Length), effectiveCharset)
	}
	return s
}

// promoteBlobTextType promotes blob/text/varbinary/varchar types based on declared length.
// MySQL auto-selects the smallest type that can hold the declared length.
// For VARCHAR, the effective length in bytes is length * bytesPerChar(charset).
func promoteBlobTextType(s string, originalType string, length int64, charset string) string {
	base := strings.ToUpper(originalType)
	// For BLOB types: blob(N) → tinyblob/blob/mediumblob/longblob based on N
	switch base {
	case "BLOB":
		if length <= 255 {
			return "tinyblob"
		} else if length <= 65535 {
			return "blob"
		} else if length <= 16777215 {
			return "mediumblob"
		}
		return "longblob"
	case "TEXT":
		if length <= 255 {
			return "tinytext"
		} else if length <= 65535 {
			return "text"
		} else if length <= 16777215 {
			return "mediumtext"
		}
		return "longtext"
	case "TINYBLOB":
		return "tinyblob"
	case "MEDIUMBLOB":
		return "mediumblob"
	case "LONGBLOB":
		return "longblob"
	case "TINYTEXT":
		return "tinytext"
	case "MEDIUMTEXT":
		return "mediumtext"
	case "LONGTEXT":
		return "longtext"
	case "VARBINARY":
		if length > 65535 {
			if length <= 16777215 {
				return "mediumblob"
			}
			return "longblob"
		}
	case "VARCHAR":
		// MySQL uses byte length for promotion thresholds.
		// For multibyte charsets (utf8=3 bytes/char, utf8mb4=4 bytes/char), the byte length
		// may exceed 65535 even if the char count doesn't.
		byteLen := length * int64(charsetBytesPerChar(charset))
		if byteLen > 65535 {
			if byteLen <= 16777215 {
				return "mediumtext"
			}
			return "longtext"
		} else if length > 65535 {
			// char count alone exceeds 65535 (e.g. latin1 with 70000 chars)
			if length <= 16777215 {
				return "mediumtext"
			}
			return "longtext"
		}
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
	// Preprocess ODBC escape sequences before parsing (e.g. {d'...'} → DATE '...')
	// Also map ODBC colName to its rewritten form for expression matching.
	rewrittenColName := colName
	if rewritten, hasODBC := rewriteODBCEscapes(selectSQL); hasODBC {
		selectSQL = rewritten
		// Also rewrite the colName so that it matches the rewritten expression
		if rewrittenCol, hasODBCCol := rewriteODBCEscapes(colName); hasODBCCol {
			rewrittenColName = rewrittenCol
		}
	}
	stmt, err := e.parser().Parse(selectSQL)
	if err != nil {
		return attrs
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		attrs.colType = e.inferColumnType(selectSQL, colName)
		return attrs
	}
	// Build source table definitions for column type lookups (same as inferColumnTypeFromSelect)
	var srcTableDefs []*catalog.TableDef
	for _, from := range sel.From {
		ate, ok2 := from.(*sqlparser.AliasedTableExpr)
		if !ok2 {
			continue
		}
		tn, ok2 := ate.Expr.(sqlparser.TableName)
		if !ok2 {
			continue
		}
		srcDB := e.CurrentDB
		if !tn.Qualifier.IsEmpty() {
			srcDB = tn.Qualifier.String()
		}
		db, dbErr := e.Catalog.GetDatabase(srcDB)
		if dbErr != nil {
			continue
		}
		tblDef, tblErr := db.GetTable(tn.Name.String())
		if tblErr != nil {
			continue
		}
		srcTableDefs = append(srcTableDefs, tblDef)
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
			if !strings.EqualFold(alias, colName) && !strings.EqualFold(alias, rewrittenColName) {
				continue
			}
		} else {
			if col, ok2 := ae.Expr.(*sqlparser.ColName); ok2 {
				if !strings.EqualFold(col.Name.String(), colName) && !strings.EqualFold(col.Name.String(), rewrittenColName) {
					continue
				}
			} else {
				exprStr := normalizeCharsetIntroducersForMatch(sqlparser.String(ae.Expr))
				normalizedColName := normalizeCharsetIntroducersForMatch(colName)
				normalizedRewrittenColName := normalizeCharsetIntroducersForMatch(rewrittenColName)
				// Normalize whitespace for comparison: sqlparser.String() may collapse or
				// add spaces differently (e.g., "a, b" vs "a,b"). Strip all whitespace.
				exprStrNorm := strings.ReplaceAll(strings.ToLower(exprStr), " ", "")
				colNameNorm := strings.ReplaceAll(strings.ToLower(normalizedColName), " ", "")
				rewrittenColNameNorm := strings.ReplaceAll(strings.ToLower(normalizedRewrittenColName), " ", "")
				// For string literals, also try matching the unquoted value against colName.
				// (e.g. colName "2001-01-01 10:10:10" matches expr "'2001-01-01 10:10:10'")
				strLitMatch := false
				if lit, ok3 := ae.Expr.(*sqlparser.Literal); ok3 && lit.Type == sqlparser.StrVal {
					litNorm := strings.ReplaceAll(strings.ToLower(lit.Val), " ", "")
					if litNorm == colNameNorm {
						strLitMatch = true
					}
				}
				if exprStrNorm != colNameNorm && exprStrNorm != rewrittenColNameNorm && !strLitMatch {
					continue
				}
			}
		}
		// Found the expression — check for bitwise op on BINARY/VARBINARY columns first
		if binExpr, ok2 := ae.Expr.(*sqlparser.BinaryExpr); ok2 {
			isBitOp := binExpr.Operator == sqlparser.BitAndOp ||
				binExpr.Operator == sqlparser.BitOrOp ||
				binExpr.Operator == sqlparser.BitXorOp ||
				binExpr.Operator == sqlparser.ShiftLeftOp ||
				binExpr.Operator == sqlparser.ShiftRightOp
			if isBitOp {
				getBinaryWidth := func(expr sqlparser.Expr) (bool, int) {
					if colRef, ok3 := expr.(*sqlparser.ColName); ok3 {
						t := ""
						for _, tblDef := range srcTableDefs {
							for _, col := range tblDef.Columns {
								if strings.EqualFold(col.Name, colRef.Name.String()) {
									t = col.Type
									break
								}
							}
						}
						lower := strings.ToLower(t)
						if strings.Contains(lower, "binary") {
							width := 0
							if n, err := fmt.Sscanf(lower, "varbinary(%d)", &width); n == 1 && err == nil {
								return true, width
							}
							if n, err := fmt.Sscanf(lower, "binary(%d)", &width); n == 1 && err == nil {
								return true, width
							}
							return true, 0
						}
					}
					return false, 0
				}
				leftIsBin, leftW := getBinaryWidth(binExpr.Left)
				rightIsBin, rightW := getBinaryWidth(binExpr.Right)
				if leftIsBin || rightIsBin {
					w := leftW
					if rightW > w {
						w = rightW
					}
					colType := fmt.Sprintf("varbinary(%d)", w)
					return columnAttrs{colType: colType, nullable: true}
				}
			}
		}
		// Get full attrs
		a := e.inferExprAttrs(ae.Expr)
		if a.colType == "" {
			a.colType = e.inferColumnTypeFromSelect(sel, colName)
		}
		// If result is binary(0) from a FuncExpr (e.g. IFNULL with column args where all
		// non-null args are column references), try to infer from source table column types.
		if a.colType == "binary(0)" {
			if fn, ok := ae.Expr.(*sqlparser.FuncExpr); ok {
				fname := strings.ToLower(fn.Name.String())
				switch fname {
				case "if", "ifnull", "coalesce", "greatest", "least", "nullif":
					// Find any column reference among args and use its type
					argsToCheck := fn.Exprs
					if fname == "if" && len(fn.Exprs) == 3 {
						argsToCheck = fn.Exprs[1:]
					}
					for _, arg := range argsToCheck {
						if colRef, ok2 := arg.(*sqlparser.ColName); ok2 {
							for _, tblDef := range srcTableDefs {
								for _, col := range tblDef.Columns {
									if strings.EqualFold(col.Name, colRef.Name.String()) {
										// For inferred result types, ZEROFILL is stripped but UNSIGNED is kept.
										colT := col.Type
										if strings.Contains(strings.ToLower(colT), "zerofill") {
											colT = strings.TrimSpace(strings.ReplaceAll(strings.ToLower(colT), "zerofill", ""))
											colT = strings.TrimSpace(colT)
											if !strings.Contains(colT, "unsigned") {
												colT = colT + " unsigned"
											}
										}
										a.colType = colT
										a.nullable = true // IFNULL with NULL arg is nullable
										goto foundColType
									}
								}
							}
						}
					}
				foundColType:
				}
			}
		}
		return a
	}
	// Fall back to type inference
	attrs.colType = e.inferColumnTypeFromSelect(sel, colName)
	return attrs
}

// inferColumnTypeFromSelect infers the column type from a single SELECT statement.
func (e *Executor) inferColumnTypeFromSelect(sel *sqlparser.Select, colName string) string {
	// Get source table definitions for column type lookups
	var srcTableDefs []*catalog.TableDef
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
		srcTableDefs = append(srcTableDefs, tblDef)
	}

	// Helper: find column type in all source table defs
	findColType := func(cn string) string {
		for _, tblDef := range srcTableDefs {
			for _, col := range tblDef.Columns {
				if strings.EqualFold(col.Name, cn) {
					return col.Type
				}
			}
		}
		return ""
	}

	// If SELECT *, all source table columns are implicitly included.
	// In that case, look up colName directly in the source tables.
	for _, selExpr := range sel.SelectExprs.Exprs {
		if _, ok := selExpr.(*sqlparser.StarExpr); ok {
			if t := findColType(colName); t != "" {
				return t
			}
		}
	}

	// Check if the column is a direct reference or an aliased expression
	for _, selExpr := range sel.SelectExprs.Exprs {
		ae, ok := selExpr.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}
		// Get the alias or auto-generated name
		exprColName := ""
		if !ae.As.IsEmpty() {
			exprColName = ae.As.String()
		} else if colRef, ok2 := ae.Expr.(*sqlparser.ColName); ok2 {
			exprColName = colRef.Name.String()
		}
		if !strings.EqualFold(exprColName, colName) {
			continue
		}

		// Found the expression - determine its type
		switch ex := ae.Expr.(type) {
		case *sqlparser.ColName:
			// Direct column reference
			if t := findColType(ex.Name.String()); t != "" {
				return t
			}
		case *sqlparser.LagLeadExpr:
			// LEAD/LAG: result type is determined by type promotion of value arg and default arg
			if ex.OverClause != nil {
				// Get primary column type (first arg)
				var primaryCol string
				if colRef, ok2 := ex.Expr.(*sqlparser.ColName); ok2 {
					primaryCol = colRef.Name.String()
				}
				var defaultCol string
				if ex.Default != nil {
					if colRef, ok2 := ex.Default.(*sqlparser.ColName); ok2 {
						defaultCol = colRef.Name.String()
					}
				}
				primaryType := findColType(primaryCol)
				defaultType := ""
				if defaultCol != "" {
					defaultType = findColType(defaultCol)
				}
				if primaryType != "" {
					return promoteStringColumnType(primaryType, defaultType)
				}
			}
		case *sqlparser.NTHValueExpr:
			// NTH_VALUE: result type is the value arg type
			if ex.OverClause != nil {
				if colRef, ok2 := ex.Expr.(*sqlparser.ColName); ok2 {
					if t := findColType(colRef.Name.String()); t != "" {
						return t
					}
				}
			}
		case *sqlparser.BinaryExpr:
			// For bitwise ops (&, |, ^, <<, >>), if either operand is BINARY/VARBINARY,
			// the result type in MySQL is VARBINARY with the operand's width.
			isBitOp := ex.Operator == sqlparser.BitAndOp ||
				ex.Operator == sqlparser.BitOrOp ||
				ex.Operator == sqlparser.BitXorOp ||
				ex.Operator == sqlparser.ShiftLeftOp ||
				ex.Operator == sqlparser.ShiftRightOp
			if isBitOp {
				getBinaryWidth := func(expr sqlparser.Expr) (string, int) {
					if colRef, ok2 := expr.(*sqlparser.ColName); ok2 {
						t := findColType(colRef.Name.String())
						lower := strings.ToLower(t)
						if strings.Contains(lower, "binary") {
							// Extract width: varbinary(N) or binary(N)
							width := 0
							if n, err := fmt.Sscanf(lower, "varbinary(%d)", &width); n == 1 && err == nil {
								return "varbinary", width
							}
							if n, err := fmt.Sscanf(lower, "binary(%d)", &width); n == 1 && err == nil {
								return "binary", width
							}
							return "binary", 0
						}
					}
					return "", 0
				}
				leftKind, leftWidth := getBinaryWidth(ex.Left)
				rightKind, rightWidth := getBinaryWidth(ex.Right)
				if leftKind != "" || rightKind != "" {
					// At least one operand is BINARY/VARBINARY → result is VARBINARY
					width := leftWidth
					if rightWidth > width {
						width = rightWidth
					}
					if width > 0 {
						return fmt.Sprintf("varbinary(%d)", width)
					}
					return "varbinary(16)"
				}
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
		// For bitwise ops (&, |, ^, <<, >>) on BINARY/VARBINARY columns, MySQL returns VARBINARY.
		if binExpr, ok2 := ae.Expr.(*sqlparser.BinaryExpr); ok2 {
			isBitOp := binExpr.Operator == sqlparser.BitAndOp ||
				binExpr.Operator == sqlparser.BitOrOp ||
				binExpr.Operator == sqlparser.BitXorOp ||
				binExpr.Operator == sqlparser.ShiftLeftOp ||
				binExpr.Operator == sqlparser.ShiftRightOp
			if isBitOp {
				getBinaryWidth := func(expr sqlparser.Expr) int {
					if colRef, ok3 := expr.(*sqlparser.ColName); ok3 {
						t := findColType(colRef.Name.String())
						lower := strings.ToLower(t)
						if strings.Contains(lower, "binary") {
							width := 0
							if n, err := fmt.Sscanf(lower, "varbinary(%d)", &width); n == 1 && err == nil {
								return width
							}
							if n, err := fmt.Sscanf(lower, "binary(%d)", &width); n == 1 && err == nil {
								return width
							}
						}
					}
					return 0
				}
				leftW := getBinaryWidth(binExpr.Left)
				rightW := getBinaryWidth(binExpr.Right)
				if leftW > 0 || rightW > 0 {
					w := leftW
					if rightW > w {
						w = rightW
					}
					return fmt.Sprintf("varbinary(%d)", w)
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

// promoteStringColumnType returns the promoted type when combining two column types.
// This mirrors MySQL's type promotion rules for LEAD/LAG and IFNULL.
func promoteStringColumnType(primaryType, defaultType string) string {
	if defaultType == "" {
		return primaryType
	}
	// If both are CHAR/VARCHAR, promote to VARCHAR with max length and utf8mb4
	primaryUpper := strings.ToUpper(primaryType)
	defaultUpper := strings.ToUpper(defaultType)
	isCharOrVarchar := func(t string) bool {
		return strings.HasPrefix(t, "CHAR") || strings.HasPrefix(t, "VARCHAR")
	}
	if isCharOrVarchar(primaryUpper) && isCharOrVarchar(defaultUpper) {
		// Extract lengths
		n1 := extractTypeLength(primaryUpper, 1)
		n2 := extractTypeLength(defaultUpper, 1)
		maxN := n1
		if n2 > n1 {
			maxN = n2
		}
		// Use VARCHAR with max length and utf8mb4 charset
		return fmt.Sprintf("varchar(%d)", maxN)
	}
	return primaryType
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
			// Use character (rune) count, not byte count, for the VARCHAR width.
			n := len([]rune(v.Val))
			return fmt.Sprintf("varchar(%d)", n)
		case sqlparser.IntVal:
			n, err := strconv.ParseUint(v.Val, 10, 64)
			if err == nil {
				if n > 4294967295 { // > UINT32_MAX: needs bigint unsigned
					// Display width = number of digits in the value
					digits := len(v.Val)
					return fmt.Sprintf("bigint(%d) unsigned", digits)
				} else if n > 2147483647 { // > INT32_MAX: needs int unsigned
					return "int unsigned"
				}
			}
			// Display width = number of digits in the literal
			digits := len(v.Val)
			return fmt.Sprintf("int(%d)", digits)
		case sqlparser.FloatVal:
			return "double"
		case sqlparser.DecimalVal:
			// DECIMAL literal: infer decimal(M,D) from the string representation.
			s := v.Val
			if dot := strings.IndexByte(s, '.'); dot >= 0 {
				intDigits := dot
				if strings.HasPrefix(s, "-") {
					intDigits--
				}
				fracDigits := len(s) - dot - 1
				m := intDigits + fracDigits
				if m < 1 {
					m = 1
				}
				return fmt.Sprintf("decimal(%d,%d)", m, fracDigits)
			}
			// No decimal point: decimal(N,0)
			n := len(s)
			if strings.HasPrefix(s, "-") {
				n--
			}
			if n < 1 {
				n = 1
			}
			return fmt.Sprintf("decimal(%d,0)", n)
		case sqlparser.DateVal:
			return "date"
		case sqlparser.TimeVal:
			// Determine fractional seconds precision from the literal value
			if dot := strings.LastIndex(v.Val, "."); dot >= 0 {
				frac := strings.TrimRight(v.Val[dot+1:], "0")
				if len(frac) == 0 {
					return "time"
				}
				// Use full fractional length (not stripped of trailing zeros)
				n := len(v.Val) - dot - 1
				if n > 6 {
					n = 6
				}
				return fmt.Sprintf("time(%d)", n)
			}
			return "time"
		case sqlparser.TimestampVal:
			// Determine fractional seconds precision from the literal
			if spIdx := strings.Index(v.Val, " "); spIdx >= 0 {
				timePart := v.Val[spIdx+1:]
				if dot := strings.LastIndex(timePart, "."); dot >= 0 {
					frac := timePart[dot+1:]
					n := len(frac)
					if n > 0 && n <= 6 {
						return fmt.Sprintf("datetime(%d)", n)
					}
				}
			}
			return "datetime"
		case sqlparser.HexVal:
			// MySQL: x'...' hex literals
			// For small hex literals (≤ 4 bytes = fits in INT), MySQL uses "int(3) unsigned".
			if len(v.Val) <= 8 { // 8 hex chars = 4 bytes
				return "int(3) unsigned"
			}
			return "bigint unsigned"
		case sqlparser.HexNum:
			// MySQL: 0x... hex literals
			// For small hex literals (≤ 4 bytes = fits in INT), MySQL uses "int(3) unsigned".
			if len(v.Val) <= 8 { // 8 hex chars = 4 bytes (the val includes "0x" prefix)
				return "int(3) unsigned"
			}
			return "bigint unsigned"
		}
	case *sqlparser.UnaryExpr:
		// Handle negative integer literals: -9223372036854775808 etc.
		if v.Operator == sqlparser.UMinusOp {
			if lit, ok := v.Expr.(*sqlparser.Literal); ok && lit.Type == sqlparser.IntVal {
				n, err := strconv.ParseUint(lit.Val, 10, 64)
				if err == nil {
					if n > 9223372036854775808 { // negation would be < INT64_MIN: use decimal
						digits := len(lit.Val)
						return fmt.Sprintf("decimal(%d,0)", digits)
					} else if n > 2147483647 { // > INT32_MAX: needs bigint (signed)
						return "bigint"
					}
				} else {
					// ParseUint failed: value is too large even for uint64 → overflow → decimal
					digits := len(lit.Val)
					return fmt.Sprintf("decimal(%d,0)", digits)
				}
			}
			// Unary minus on a decimal preserves the decimal type.
			inner := e.inferExprType(v.Expr)
			if strings.HasPrefix(inner, "decimal(") {
				return inner
			}
		}
		return e.inferExprType(v.Expr)
	case *sqlparser.BinaryExpr:
		// Arithmetic expressions (+,-,*,/) return numeric types.
		// If either operand is hex, result is int(3) unsigned (for small hex) or bigint unsigned (for large hex).
		leftType := e.inferExprType(v.Left)
		rightType := e.inferExprType(v.Right)
		if leftType == "bigint unsigned" || rightType == "bigint unsigned" {
			return "bigint unsigned"
		}
		if leftType == "int(3) unsigned" || rightType == "int(3) unsigned" {
			return "int(3) unsigned"
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
		case "round", "truncate":
			// ROUND(x, d) / TRUNCATE(x, d): if x is a decimal literal, infer decimal(M,D)
			// where D is the number of decimal places requested (or 0 for negative d).
			if len(v.Exprs) >= 1 {
				argType := e.inferExprType(v.Exprs[0])
				if strings.HasPrefix(argType, "decimal(") {
					var argM, argD int
					if n, _ := fmt.Sscanf(argType, "decimal(%d,%d)", &argM, &argD); n == 2 {
						outD := argD // default: same scale
						scaleVal := argD
						if len(v.Exprs) >= 2 {
							scaleArg := v.Exprs[1]
							scaleOK := false
							// Handle both plain literal (-1 as Literal{IntVal,"-1"}) and unary minus
							if lit, ok := scaleArg.(*sqlparser.Literal); ok && lit.Type == sqlparser.IntVal {
								if d, err := strconv.Atoi(lit.Val); err == nil {
									scaleVal, scaleOK = d, true
								}
							} else if ue, ok := scaleArg.(*sqlparser.UnaryExpr); ok && ue.Operator == sqlparser.UMinusOp {
								if lit2, ok2 := ue.Expr.(*sqlparser.Literal); ok2 && lit2.Type == sqlparser.IntVal {
									if d, err := strconv.Atoi(lit2.Val); err == nil {
										scaleVal, scaleOK = -d, true
									}
								}
							}
							if scaleOK {
								if scaleVal < 0 {
									outD = 0
								} else if scaleVal < argD {
									outD = scaleVal
								} else {
									outD = argD
								}
							}
						}
						// M = integer digits of arg + output scale.
						// For ROUND: add 1 for potential carry (e.g. 9.9 rounds to 10).
						intDigits := argM - argD
						if intDigits < 1 {
							intDigits = 1
						}
						outM := intDigits + outD
						if name == "round" {
							outM++ // extra digit for rounding carry
						}
						if outM < 1 {
							outM = 1
						}
						return fmt.Sprintf("decimal(%d,%d)", outM, outD)
					}
				}
			}
		case "abs":
			// ABS(x): preserve decimal type of x, but +1 to M to accommodate sign.
			if len(v.Exprs) >= 1 {
				argType := e.inferExprType(v.Exprs[0])
				if strings.HasPrefix(argType, "decimal(") {
					var m, d int
					if n, _ := fmt.Sscanf(argType, "decimal(%d,%d)", &m, &d); n == 2 {
						return fmt.Sprintf("decimal(%d,%d)", m+1, d)
					}
					return argType
				}
			}
		case "nullif":
			// NULLIF(x, y) uses the type of the first argument.
			// If first arg is NULL, MySQL returns char(0).
			if len(v.Exprs) >= 1 {
				t := e.inferExprType(v.Exprs[0])
				if t != "" && t != "binary(0)" {
					return t
				}
			}
			// first arg is null or unknown — return char(0)
			return "char(0)"
		case "substring_index":
			// SUBSTRING_INDEX(str, delim, count) — return type based on first arg
			if len(v.Exprs) >= 1 {
				t := e.inferExprType(v.Exprs[0])
				if t == "binary(0)" || t == "" {
					return "char(0)"
				}
				return t
			}
			return "char(0)"
		case "elt":
			// ELT(n, str1, str2, ...) — return type is max varchar of string args
			maxLen := 0
			for i, arg := range v.Exprs {
				if i == 0 {
					continue // skip the index arg
				}
				t := e.inferExprType(arg)
				if strings.HasPrefix(t, "varchar(") {
					var n int
					if _, err := fmt.Sscanf(t, "varchar(%d)", &n); err == nil && n > maxLen {
						maxLen = n
					}
				}
			}
			if maxLen > 0 {
				return fmt.Sprintf("varchar(%d)", maxLen)
			}
		case "concat":
			// CONCAT(str1, str2, ...) — sum of varchar lengths of non-null args
			totalLen := 0
			hasStr := false
			for _, arg := range v.Exprs {
				t := e.inferExprType(arg)
				if t == "binary(0)" || t == "" {
					continue
				}
				if strings.HasPrefix(t, "varchar(") {
					var n int
					if _, err := fmt.Sscanf(t, "varchar(%d)", &n); err == nil {
						totalLen += n
						hasStr = true
					}
				}
			}
			if hasStr {
				return fmt.Sprintf("varchar(%d)", totalLen)
			}
			return "char(0)"
		case "concat_ws":
			// CONCAT_WS(sep, str1, str2, ...) — MySQL computes max potential length as:
			// sum of all positional arg lengths (null args = 0) + sep*(numArgs-1)
			// If sep is null → result length = sum of all arg lengths (no separators)
			if len(v.Exprs) >= 1 {
				sepType := e.inferExprType(v.Exprs[0])
				sepLen := 0
				sepIsNull := (sepType == "binary(0)" || sepType == "")
				if !sepIsNull {
					if strings.HasPrefix(sepType, "varchar(") {
						fmt.Sscanf(sepType, "varchar(%d)", &sepLen)
					}
				}
				numArgs := len(v.Exprs) - 1 // number of data args (excluding sep)
				strTotal := 0
				for i, arg := range v.Exprs {
					if i == 0 {
						continue
					}
					t := e.inferExprType(arg)
					if t != "binary(0)" && t != "" && strings.HasPrefix(t, "varchar(") {
						var n int
						if _, err := fmt.Sscanf(t, "varchar(%d)", &n); err == nil {
							strTotal += n
						}
					}
				}
				total := strTotal
				if !sepIsNull && numArgs > 1 {
					total += sepLen * (numArgs - 1)
				}
				return fmt.Sprintf("varchar(%d)", total)
			}
			return "char(0)"
		case "make_set":
			// MAKE_SET(bits, str1, str2, ...) — MySQL counts all args (including null args as 0-len)
			// for string total, plus (numArgs-1) commas for separators between them.
			numDataArgs := len(v.Exprs) - 1 // number of string args (excluding bits)
			strTotal := 0
			for i, arg := range v.Exprs {
				if i == 0 {
					continue // skip bits arg
				}
				t := e.inferExprType(arg)
				if t != "binary(0)" && t != "" && strings.HasPrefix(t, "varchar(") {
					var n int
					if _, err := fmt.Sscanf(t, "varchar(%d)", &n); err == nil {
						strTotal += n
					}
				}
			}
			if numDataArgs > 0 {
				total := strTotal
				if numDataArgs > 1 {
					total += numDataArgs - 1 // commas between all arg slots
				}
				return fmt.Sprintf("varchar(%d)", total)
			}
			return "char(0)"
		case "export_set":
			// EXPORT_SET(bits, on, off, [sep, [num_bits]])
			// MySQL: varchar computed from on/off/sep lengths * num_bits
			// Default num_bits=64. MySQL uses a fixed formula.
			// on=len(on_str), off=len(off_str), sep=len(sep_str), n=num_bits
			// Result length = n*max(on,off) + (n-1)*len(sep)
			onLen, offLen, sepLen := 0, 0, 0
			numBits := 64
			if len(v.Exprs) >= 2 {
				t := e.inferExprType(v.Exprs[1])
				if strings.HasPrefix(t, "varchar(") {
					fmt.Sscanf(t, "varchar(%d)", &onLen)
				}
			}
			if len(v.Exprs) >= 3 {
				t := e.inferExprType(v.Exprs[2])
				if strings.HasPrefix(t, "varchar(") {
					fmt.Sscanf(t, "varchar(%d)", &offLen)
				}
			}
			if len(v.Exprs) >= 4 {
				t := e.inferExprType(v.Exprs[3])
				if strings.HasPrefix(t, "varchar(") {
					fmt.Sscanf(t, "varchar(%d)", &sepLen)
				}
			}
			if len(v.Exprs) >= 5 {
				if lit, ok := v.Exprs[4].(*sqlparser.Literal); ok && lit.Type == sqlparser.IntVal {
					if n, err := strconv.Atoi(lit.Val); err == nil {
						numBits = n
					}
				}
			}
			maxOnOff := onLen
			if offLen > maxOnOff {
				maxOnOff = offLen
			}
			total := numBits*maxOnOff + (numBits-1)*sepLen
			return fmt.Sprintf("varchar(%d)", total)
		case "replace":
			// REPLACE(str, from_str, to_str) — if str is null, char(0); otherwise varchar(len(str))
			if len(v.Exprs) >= 1 {
				t := e.inferExprType(v.Exprs[0])
				if t == "binary(0)" || t == "" {
					return "char(0)"
				}
				return t
			}
			return "char(0)"
		case "lpad", "rpad":
			// LPAD(str, len, padstr) / RPAD(str, len, padstr)
			// Result length is always the second arg (len)
			if len(v.Exprs) >= 2 {
				if lit, ok := v.Exprs[1].(*sqlparser.Literal); ok && lit.Type == sqlparser.IntVal {
					if n, err := strconv.Atoi(lit.Val); err == nil {
						return fmt.Sprintf("varchar(%d)", n)
					}
				}
			}
			return "text"
		case "makedate":
			// MAKEDATE(year, dayofyear) returns a date value
			return "date"
		case "maketime":
			// MAKETIME(hour, minute, second) returns a time value (no microseconds for integer args)
			return "time"
		case "timediff":
			// TIMEDIFF(expr1, expr2) returns time(6)
			return "time(6)"
		case "addtime", "subtime":
			// ADDTIME(expr, expr) / SUBTIME(expr, expr)
			// If first arg is datetime/timestamp => datetime(6), if time => time(6)
			if len(v.Exprs) >= 1 {
				firstType := e.inferExprType(v.Exprs[0])
				if strings.HasPrefix(firstType, "datetime") || strings.HasPrefix(firstType, "timestamp") {
					return "datetime(6)"
				}
				if strings.HasPrefix(firstType, "time") {
					return "time(6)"
				}
			}
			return "time(6)"
		case "timestamp":
			// TIMESTAMP(expr) or TIMESTAMP(date, time) returns datetime
			return "datetime"
		case "date":
			// DATE(expr) extracts the date part and returns date
			return "date"
		case "time":
			// TIME(expr) extracts the time part and returns time(6)
			return "time(6)"
		case "sec_to_time":
			// SEC_TO_TIME(seconds) returns time(N) where N is the fractional precision of the argument.
			// For integer arguments: time. For decimal/float with D fractional digits: time(min(D,6)).
			if len(v.Exprs) == 1 {
				fsp := 0
				if lit, ok := v.Exprs[0].(*sqlparser.Literal); ok {
					switch lit.Type {
					case sqlparser.DecimalVal:
						if dot := strings.IndexByte(lit.Val, '.'); dot >= 0 {
							fsp = len(lit.Val) - dot - 1
						}
					case sqlparser.FloatVal:
						if dot := strings.IndexByte(lit.Val, '.'); dot >= 0 {
							fsp = len(lit.Val) - dot - 1
						}
					}
				} else {
					// Non-literal argument: infer from type
					argType := e.inferExprType(v.Exprs[0])
					if strings.HasPrefix(argType, "decimal(") {
						// Extract scale from decimal(M,D)
						var m, d int
						fmt.Sscanf(argType, "decimal(%d,%d)", &m, &d)
						fsp = d
					} else if argType == "double" || argType == "float" {
						fsp = 6
					}
				}
				if fsp > 6 {
					fsp = 6
				}
				if fsp == 0 {
					return "time"
				}
				return fmt.Sprintf("time(%d)", fsp)
			}
			return "time"
		case "date_format":
			// DATE_FORMAT(date, format) returns varchar(N); use a fixed width matching MySQL default
			return "varchar(10)"
		case "period_add":
			// PERIOD_ADD(period, months) returns int
			return "int"
		case "period_diff":
			// PERIOD_DIFF(period1, period2) returns int
			return "int"
		case "if", "ifnull", "coalesce", "greatest", "least":
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
			var decimalType string
			hasDouble := false
			hasNullArg := false
			maxVarcharLen := 0
			for _, arg := range argsToCheck {
				t := e.inferExprType(arg)
				if t == "binary(0)" || t == "" {
					if t == "binary(0)" {
						hasNullArg = true
					}
					continue
				}
				allNull = false
				if strings.HasPrefix(t, "decimal(") {
					decimalType = t
				} else if t == "double" {
					hasDouble = true
				} else if strings.HasPrefix(t, "varchar(") {
					var n int
					if _, err := fmt.Sscanf(t, "varchar(%d)", &n); err == nil && n > maxVarcharLen {
						maxVarcharLen = n
					}
				} else if strings.HasPrefix(t, "char(") {
					var n int
					if _, err := fmt.Sscanf(t, "char(%d)", &n); err == nil && n > maxVarcharLen {
						maxVarcharLen = n
					}
				}
			}
			if allNull {
				return "binary(0)"
			}
			// If any arg is double, return double (double beats decimal).
			if hasDouble {
				return "double"
			}
			// If any arg is a decimal, return the decimal type.
			// (int literals mixed with decimal still yield decimal in MySQL)
			if decimalType != "" {
				_ = hasNullArg // nullable handled in inferExprAttrs
				return decimalType
			}
			// If any arg is a string (varchar/char), return varchar with max length.
			if maxVarcharLen > 0 {
				return fmt.Sprintf("varchar(%d)", maxVarcharLen)
			}
		}
	case *sqlparser.IntroducerExpr:
		// _charset'string' — type is varchar(len) with the given charset
		if lit, ok := v.Expr.(*sqlparser.Literal); ok && lit.Type == sqlparser.StrVal {
			return fmt.Sprintf("varchar(%d)", len(lit.Val))
		}
	case *sqlparser.InsertExpr:
		// INSERT(str, pos, len, newstr) — type based on str (first arg).
		// If str is null → use newstr; if both null → char(0).
		t := e.inferExprType(v.Str)
		if t != "binary(0)" && t != "" {
			return t
		}
		t2 := e.inferExprType(v.NewStr)
		if t2 != "binary(0)" && t2 != "" {
			return t2
		}
		return "char(0)"
	case *sqlparser.TrimFuncExpr:
		// TRIM([remstr FROM] str) — type based on StringArg (the main string).
		// If StringArg is null → char(0), otherwise varchar(len).
		t := e.inferExprType(v.StringArg)
		if t == "binary(0)" || t == "" {
			return "char(0)"
		}
		return t
	case *sqlparser.CaseExpr:
		// CASE WHEN ... THEN val ELSE val END — return the widest type among non-null branches.
		maxVarcharLen := 0
		for _, when := range v.Whens {
			t := e.inferExprType(when.Val)
			if t == "binary(0)" || t == "" {
				continue
			}
			if strings.HasPrefix(t, "varchar(") {
				var n int
				if _, err := fmt.Sscanf(t, "varchar(%d)", &n); err == nil && n > maxVarcharLen {
					maxVarcharLen = n
				}
			} else if strings.HasPrefix(t, "char(") {
				var n int
				if _, err := fmt.Sscanf(t, "char(%d)", &n); err == nil && n > maxVarcharLen {
					maxVarcharLen = n
				}
			}
		}
		if v.Else != nil {
			t := e.inferExprType(v.Else)
			if t != "binary(0)" && t != "" {
				if strings.HasPrefix(t, "varchar(") {
					var n int
					if _, err := fmt.Sscanf(t, "varchar(%d)", &n); err == nil && n > maxVarcharLen {
						maxVarcharLen = n
					}
				} else if strings.HasPrefix(t, "char(") {
					var n int
					if _, err := fmt.Sscanf(t, "char(%d)", &n); err == nil && n > maxVarcharLen {
						maxVarcharLen = n
					}
				}
			}
		}
		if maxVarcharLen > 0 {
			return fmt.Sprintf("varchar(%d)", maxVarcharLen)
		}
	case *sqlparser.NullVal:
		// MySQL uses binary(0) for NULL literal columns in CREATE TABLE AS SELECT
		return "binary(0)"
	case *sqlparser.ConvertExpr:
		// CAST(x AS type) / CONVERT(x, type) — use the target type
		ct := convertTypeToSQLType(v.Type)
		// For CAST(x AS CHAR) with no length, infer varchar width from inner expression type.
		if ct == "char(0)" {
			ct = inferCastAsCharType(e.inferExprType(v.Expr))
		}
		return ct
	case *sqlparser.CastExpr:
		// CAST(x AS type) — use the target type
		ct := convertTypeToSQLType(v.Type)
		// For CAST(x AS CHAR) with no length, infer varchar width from inner expression type.
		if ct == "char(0)" {
			ct = inferCastAsCharType(e.inferExprType(v.Expr))
		}
		return ct
	}
	return ""
}

// inferCastAsCharType returns the appropriate varchar type for CAST(x AS CHAR) when no length
// is specified, based on the display width of the inner expression type.
func inferCastAsCharType(innerType string) string {
	upperInner := strings.ToUpper(strings.TrimSpace(innerType))
	switch {
	case upperInner == "TIME":
		return "varchar(10)"
	case strings.HasPrefix(upperInner, "TIME("):
		// TIME(N) display width: HH:MM:SS.ffffff = 8+1+N = 9+N, but MySQL uses 10+N-1... actually:
		// time(0) = HH:MM:SS = 8, time(1) = HH:MM:SS.f = 10, time(6) = HH:MM:SS.ffffff = 15
		// but for small N, MySQL rounds up. Use 10 for consistency when N==0, or 10+N for larger.
		fsp := 0
		fmt.Sscanf(upperInner, "TIME(%d)", &fsp)
		if fsp == 0 {
			return "varchar(10)"
		}
		return fmt.Sprintf("varchar(%d)", 9+fsp)
	case upperInner == "DATE":
		return "varchar(10)"
	case upperInner == "DATETIME":
		return "varchar(19)"
	case strings.HasPrefix(upperInner, "DATETIME("):
		fsp := 0
		fmt.Sscanf(upperInner, "DATETIME(%d)", &fsp)
		if fsp == 0 {
			return "varchar(19)"
		}
		return fmt.Sprintf("varchar(%d)", 19+1+fsp)
	default:
		return "varchar(10)"
	}
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
	case *sqlparser.Literal:
		// Temporal literals: DATE'...', TIME'...', TIMESTAMP'...' — MySQL uses NOT NULL with zero default
		switch v.Type {
		case sqlparser.DateVal:
			attrs.colType = "date"
			attrs.nullable = false
			attrs.hasDefault = true
			attrs.defaultVal = "0000-00-00"
		case sqlparser.TimeVal:
			// Determine fractional seconds precision from the literal value
			colType := "time"
			defaultVal := "00:00:00"
			if dot := strings.LastIndex(v.Val, "."); dot >= 0 {
				frac := v.Val[dot+1:]
				// Only count non-trailing-zero digits for precision
				n := len(frac)
				if n > 0 {
					// Use full length including trailing zeros
					if n > 6 {
						n = 6
					}
					colType = fmt.Sprintf("time(%d)", n)
					defaultVal = "00:00:00." + strings.Repeat("0", n)
				}
			}
			attrs.colType = colType
			attrs.nullable = false
			attrs.hasDefault = true
			attrs.defaultVal = defaultVal
		case sqlparser.TimestampVal:
			// Determine fractional seconds precision from the literal value
			colType := "datetime"
			if spIdx := strings.Index(v.Val, " "); spIdx >= 0 {
				timePart := v.Val[spIdx+1:]
				if dot := strings.LastIndex(timePart, "."); dot >= 0 {
					frac := timePart[dot+1:]
					n := len(frac)
					if n > 0 && n <= 6 {
						colType = fmt.Sprintf("datetime(%d)", n)
					}
				}
			}
			attrs.colType = colType
			attrs.nullable = false
			// MySQL TIMESTAMP/DATETIME literal columns don't show an explicit DEFAULT in SHOW CREATE TABLE
			attrs.hasDefault = false
		case sqlparser.IntVal:
			// Integer literals produce NOT NULL DEFAULT '0' columns
			attrs.nullable = false
			attrs.hasDefault = true
			attrs.defaultVal = "0"
		case sqlparser.FloatVal, sqlparser.DecimalVal:
			// Float/decimal literals produce NOT NULL DEFAULT '0' columns
			attrs.nullable = false
			attrs.hasDefault = true
			attrs.defaultVal = "0"
		case sqlparser.StrVal:
			// String literals produce NOT NULL DEFAULT '' varchar columns.
			// The charset comes from the current connection charset (character_set_client).
			n := len([]rune(v.Val))
			attrs.colType = fmt.Sprintf("varchar(%d)", n)
			attrs.nullable = false
			attrs.hasDefault = true
			attrs.defaultVal = ""
			// Inherit the current connection charset for string literal columns.
			if cs, ok := e.getSysVar("character_set_client"); ok && cs != "" && cs != "utf8mb4" && cs != "utf8" {
				attrs.charset = cs
			}
		}
	case *sqlparser.IntroducerExpr:
		// _charset'string' — charset comes from the introducer
		cs := strings.ToLower(strings.TrimPrefix(v.CharacterSet, "_"))
		if cs == "utf8mb3" {
			cs = "utf8"
		}
		attrs.charset = cs
		if lit, ok := v.Expr.(*sqlparser.Literal); ok && lit.Type == sqlparser.StrVal {
			// Use character (rune) count, not byte count, for the VARCHAR width.
			attrs.colType = fmt.Sprintf("varchar(%d)", len([]rune(lit.Val)))
			// MySQL: charset-introduced string literals produce NOT NULL DEFAULT '' columns.
			attrs.nullable = false
			attrs.hasDefault = true
			attrs.defaultVal = ""
		}
	}
	if attrs.colType == "" {
		attrs.colType = e.inferExprType(expr)
	}
	// For varchar/char result types inferred from string expressions,
	// propagate the current connection charset (e.g. latin2 from "set names latin2").
	colTypeLowerForCS := strings.ToLower(attrs.colType)
	if attrs.charset == "" && (strings.HasPrefix(colTypeLowerForCS, "varchar(") || strings.HasPrefix(colTypeLowerForCS, "char(")) {
		if cs, ok := e.getSysVar("character_set_client"); ok && cs != "" && cs != "utf8mb4" && cs != "utf8" {
			attrs.charset = cs
		}
	}
	// For arithmetic expressions involving hex/integer literals, MySQL uses NOT NULL with DEFAULT 0
	if _, isBin := expr.(*sqlparser.BinaryExpr); isBin {
		colType := strings.ToLower(attrs.colType)
		if strings.Contains(colType, "int") || colType == "double" || colType == "decimal" {
			attrs.nullable = false
			attrs.hasDefault = true
			attrs.defaultVal = "0"
		}
	}
	// For numeric functions that return decimal, MySQL uses NOT NULL with appropriate DEFAULT.
	// ROUND, TRUNCATE, ABS, and unary minus on a decimal literal arg → NOT NULL + DEFAULT.
	if fn, ok := expr.(*sqlparser.FuncExpr); ok {
		fname := strings.ToLower(fn.Name.String())
		switch fname {
		case "round", "truncate", "abs":
			if strings.HasPrefix(strings.ToLower(attrs.colType), "decimal(") {
				attrs.nullable = false
				attrs.hasDefault = true
				var m, d int
				if n, _ := fmt.Sscanf(attrs.colType, "decimal(%d,%d)", &m, &d); n == 2 && d > 0 {
					attrs.defaultVal = "0." + strings.Repeat("0", d)
				} else {
					attrs.defaultVal = "0"
				}
			}
		}
	}
	if _, isUnary := expr.(*sqlparser.UnaryExpr); isUnary {
		colTypeLower := strings.ToLower(attrs.colType)
		if strings.HasPrefix(colTypeLower, "decimal(") {
			attrs.nullable = false
			attrs.hasDefault = true
			var m, d int
			if n, _ := fmt.Sscanf(attrs.colType, "decimal(%d,%d)", &m, &d); n == 2 && d > 0 {
				attrs.defaultVal = "0." + strings.Repeat("0", d)
			} else {
				attrs.defaultVal = "0"
			}
		} else if strings.HasPrefix(colTypeLower, "bigint") || strings.HasPrefix(colTypeLower, "int") {
			// Unary minus on an integer literal: NOT NULL DEFAULT 0
			attrs.nullable = false
			attrs.hasDefault = true
			attrs.defaultVal = "0"
		}
	}
	// For IF/COALESCE/IFNULL/NULLIF/GREATEST/LEAST with a decimal result type,
	// MySQL uses NOT NULL with DEFAULT '0.0' (or '0.00' for scale=2, etc.),
	// unless any value arg is NULL (then DEFAULT NULL is used).
	if fn, ok := expr.(*sqlparser.FuncExpr); ok {
		fname := strings.ToLower(fn.Name.String())
		switch fname {
		case "nullif":
			// NULLIF always returns NULL when args are equal, so always nullable (DEFAULT NULL).
			// Still set the type correctly (decimal if applicable), but leave nullable=true.
		case "if", "ifnull", "coalesce", "greatest", "least":
			colTypeLower := strings.ToLower(attrs.colType)
			isNumericType := strings.HasPrefix(colTypeLower, "decimal(") || colTypeLower == "double"
			isVarcharType := strings.HasPrefix(colTypeLower, "varchar(") || strings.HasPrefix(colTypeLower, "char(")
			if isNumericType {
				// Check if any value arg is a NULL literal
				argsToCheck := fn.Exprs
				if fname == "if" && len(fn.Exprs) == 3 {
					argsToCheck = fn.Exprs[1:] // skip condition
				}
				hasNullArg := false
				for _, arg := range argsToCheck {
					t := e.inferExprType(arg)
					if t == "binary(0)" {
						hasNullArg = true
						break
					}
				}
				if !hasNullArg {
					attrs.nullable = false
					attrs.hasDefault = true
					if strings.HasPrefix(colTypeLower, "decimal(") {
						// Extract scale from decimal(M,D) to form default like "0.0" or "0.00"
						var m, d int
						if n, _ := fmt.Sscanf(attrs.colType, "decimal(%d,%d)", &m, &d); n == 2 && d > 0 {
							attrs.defaultVal = "0." + strings.Repeat("0", d)
						} else {
							attrs.defaultVal = "0"
						}
					} else {
						attrs.defaultVal = "0"
					}
				}
			} else if isVarcharType {
				// For varchar result: determine nullability based on MySQL rules.
				// IFNULL(a,b): NOT NULL if b is non-null (guaranteed fallback).
				// COALESCE: NOT NULL if any non-null arg exists at a position before all nulls.
				// Actually MySQL's rule: COALESCE/IFNULL is NOT NULL if ANY arg is guaranteed non-null.
				switch fname {
				case "ifnull":
					// ifnull(a, b): NOT NULL if b is non-null
					if len(fn.Exprs) >= 2 {
						bt := e.inferExprType(fn.Exprs[1])
						if bt != "binary(0)" && bt != "" {
							attrs.nullable = false
							attrs.hasDefault = true
							attrs.defaultVal = ""
						}
					}
				case "coalesce":
					// coalesce: NOT NULL if any arg is non-null
					for _, arg := range fn.Exprs {
						t := e.inferExprType(arg)
						if t != "binary(0)" && t != "" {
							attrs.nullable = false
							attrs.hasDefault = true
							attrs.defaultVal = ""
							break
						}
					}
				}
			}
		}
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
	// Release any row locks acquired by the inner SELECT (insideDML=true causes shared
	// locks to be acquired on source rows; those locks must not outlive this call since
	// CREATE TABLE ... SELECT is not a transactional operation).
	if e.rowLockManager != nil && !e.inTransaction {
		e.rowLockManager.ReleaseRowLocks(e.connectionID)
	}
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

// normalizeCurrentTimestampDefault normalizes CURRENT_TIMESTAMP synonyms (now(), current_timestamp())
// to canonical CURRENT_TIMESTAMP form for storage.
func normalizeCurrentTimestampDefault(s string) string {
	upper := strings.ToUpper(strings.TrimSpace(s))
	if upper == "NOW()" || upper == "CURRENT_TIMESTAMP()" {
		return "CURRENT_TIMESTAMP"
	}
	// now(N) or current_timestamp(N) with precision
	if strings.HasPrefix(upper, "NOW(") && strings.HasSuffix(upper, ")") {
		n := upper[4 : len(upper)-1]
		return "CURRENT_TIMESTAMP(" + n + ")"
	}
	if strings.HasPrefix(upper, "CURRENT_TIMESTAMP(") {
		return upper // already canonical
	}
	return s
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

// stripCreateTablePartitionClause removes the PARTITION BY ... clause from a
// CREATE TABLE statement, returning the stripped SQL or "" if the clause could
// not be found. This is used as a fallback when vitess cannot fully parse the
// partition syntax (e.g. NODEGROUP, multi-column MAXVALUE in RANGE COLUMNS).
// The table column definition body (inside the outermost parens) is preserved.
func stripCreateTablePartitionClause(query string) string {
	// Find the outermost CREATE TABLE ... ( ... ) block — we want the position
	// of the closing paren of the column definition list. Everything after that
	// (before any ENGINE= or other table options) is the PARTITION BY clause.
	upper := strings.ToUpper(query)
	// Locate "CREATE" at the beginning
	if !strings.HasPrefix(strings.TrimSpace(upper), "CREATE") {
		return ""
	}
	// Find the first '(' which opens the column definition list
	openIdx := strings.Index(query, "(")
	if openIdx < 0 {
		return ""
	}
	// Walk to find the matching closing paren at depth 0
	depth := 0
	closeIdx := -1
	inSingle := false
	inDouble := false
	inBacktick := false
	for i := openIdx; i < len(query); i++ {
		ch := query[i]
		if inSingle {
			if ch == '\'' {
				inSingle = false
			}
			continue
		}
		if inDouble {
			if ch == '"' {
				inDouble = false
			}
			continue
		}
		if inBacktick {
			if ch == '`' {
				inBacktick = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				closeIdx = i
				goto foundClose
			}
		}
	}
foundClose:
	if closeIdx < 0 {
		return ""
	}
	// Everything up to and including the closing paren is the table definition.
	// Check if there's a PARTITION BY after the closing paren.
	afterClose := strings.TrimSpace(query[closeIdx+1:])
	afterCloseUpper := strings.ToUpper(afterClose)
	if !strings.HasPrefix(afterCloseUpper, "PARTITION BY") {
		// There may be table options (ENGINE=, CHARSET=) before PARTITION BY
		// Check if PARTITION BY appears anywhere after closeIdx
		if !strings.Contains(afterCloseUpper, "PARTITION BY") {
			return ""
		}
	}
	// Return the CREATE TABLE ... (defs) part only, stripping PARTITION BY onwards.
	// Also strip any table options that appear between the closing paren and PARTITION BY.
	partitionIdx := strings.Index(afterCloseUpper, "PARTITION BY")
	if partitionIdx < 0 {
		return ""
	}
	// Keep table options between ) and PARTITION BY (e.g. ENGINE=InnoDB)
	tableOptions := strings.TrimSpace(afterClose[:partitionIdx])
	result := query[:closeIdx+1]
	if tableOptions != "" {
		result += " " + tableOptions
	}
	return result
}
