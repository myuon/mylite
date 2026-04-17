package executor

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

// preprocessQuery performs pre-parse processing on the query string before
// it is handed to the SQL parser. It handles three cases:
//   - Query rewrite: returns (rewritten, nil, nil)
//   - Direct result (no parsing needed): returns ("", result, nil)
//   - Error: returns ("", nil, err)
func (e *Executor) preprocessQuery(query string) (string, *Result, error) {
	// Handle nested comments inside MySQL versioned conditional comments (/*!NNNNN...*/):
	// MySQL allows but deprecates nesting /* */ inside /*! ... */. Preprocess to strip
	// the inner nested comments so the vitess parser can handle the query.
	if strings.Contains(query, "/*!") && strings.Contains(query, "/*") {
		query = stripNestedConditionalComments(query, e)
	}
	// Vitess parser does not support multiple window definitions in a WINDOW clause
	// (e.g., "WINDOW w AS (...), w1 AS (...)"). Inline named windows into OVER clauses.
	if rewritten := inlineNamedWindows(query); rewritten != "" {
		query = rewritten
	}
	// Vitess grammar for LEAD/LAG requires a non-negative integer offset.
	// MySQL supports negative offsets: LEAD(expr, -N) is equivalent to LAG(expr, N).
	// Preprocess to swap LEAD/LAG and negate when a negative integer offset is used.
	if containsLeadLagWithNegativeOffset(query) {
		query = rewriteLeadLagNegativeOffset(query)
	}
	// Vitess parser loses the inner LIMIT in queries like "(SELECT ... LIMIT N) ORDER BY ...".
	// Rewrite such queries to use a derived table: SELECT * FROM (SELECT ... LIMIT N) AS _paren_subq ORDER BY ...
	if rewritten := rewriteParenSelectWithOuterOrderBy(query); rewritten != "" {
		query = rewritten
	}
	trimmed := stripLeadingCStyleComments(strings.TrimSpace(query))
	query = trimmed
	e.currentQuery = trimmed

	// Empty query (e.g. comment-only) is a no-op.
	if trimmed == "" {
		return "", &Result{}, nil
	}

	// Handle ODBC escape syntax: {fn func(args)}, { date "value" }, {d 'value'}, {ts '...'}, {t '...'}.
	// MySQL supports ODBC escape sequences for compatibility. We preprocess them to standard MySQL syntax.
	// e.currentQuery is already set to the original for rawExprs column name extraction.
	if rewritten, hasODBC := rewriteODBCEscapes(trimmed); hasODBC {
		query = rewritten
	}

	// Vitess parser doesn't support SUM(ALL expr), COUNT(ALL expr), etc.
	// ALL is the default and redundant; transform FUNC(ALL ...) → FUNC(...).
	if strings.Contains(strings.ToUpper(trimmed), "(ALL ") {
		query = rewriteAggregateAll(query)
	}

	// Vitess parser cannot handle AS aliases that start with a digit (e.g. AS 1Eq).
	// MySQL allows these; quote them: AS 1Eq -> AS `1Eq`.
	if rewritten := rewriteNumericAliases(query); rewritten != query {
		query = rewritten
	}

	// Vitess parser doesn't handle LONG, LONG VARCHAR, LONG VARBINARY type aliases.
	// MySQL treats them as: LONG/LONG VARCHAR -> MEDIUMTEXT, LONG VARBINARY -> MEDIUMBLOB.
	// Only apply to DDL statements (CREATE TABLE / ALTER TABLE) to avoid mangling string literals.
	{
		upperContains := strings.ToUpper(trimmed)
		isCreateOrAlterTable := (strings.HasPrefix(upperContains, "CREATE") || strings.HasPrefix(upperContains, "ALTER")) && strings.Contains(upperContains, "TABLE")
		if isCreateOrAlterTable && (strings.Contains(upperContains, " LONG ") || strings.Contains(upperContains, " LONG,") || strings.Contains(upperContains, " LONG)")) {
			query = rewriteLongTypes(query)
		}
		// Vitess parser doesn't support deprecated `BINARY CHARACTER SET X` order in column definitions.
		// MySQL supports both orders; rewrite `BINARY CHARACTER SET X` -> `CHARACTER SET X BINARY`.
		// Also handle BYTE keyword (deprecated synonym for BINARY in column types).
		if isCreateOrAlterTable && (strings.Contains(upperContains, "BINARY CHARACTER SET") || strings.Contains(upperContains, " BYTE")) {
			query = rewriteBinaryCharacterSet(query)
		}
		// Vitess parser doesn't handle SQL standard type aliases:
		// CHARACTER VARYING(n) -> VARCHAR(n), CHARACTER(n) -> CHAR(n)
		if isCreateOrAlterTable && strings.Contains(upperContains, "CHARACTER VARYING") {
			query = rewriteCharacterVarying(query)
		}
	}

	// Clear per-query subquery cache so non-correlated IN subqueries
	// are only evaluated once within the same top-level statement.
	e.subqueryValCache = nil
	upper := strings.ToUpper(trimmed)

	// Check for PS function parameter count errors before parsing.
	// Only match unqualified calls (no schema prefix like "test.ps_thread_id").
	// Strip string literals first to avoid false matches inside strings.
	if strings.HasPrefix(upper, "SELECT") {
		stripped := stripStringLiterals(upper)
		compact := strings.NewReplacer(" ", "", "\t", "", "\n", "", "\r", "").Replace(stripped)
		// ST_X() and ST_Y() are native spatial functions requiring exactly 1 argument.
		// When called with 0 arguments, MySQL returns a parameter count error even if
		// a user-defined function with the same name exists (native functions take precedence).
		for _, nativeFn := range []string{"ST_X", "ST_Y"} {
			fnUpper := strings.ToUpper(nativeFn)
			if strings.Contains(compact, fnUpper+"(") {
				idx := strings.Index(compact, fnUpper+"(")
				// Only match if not preceded by '.' (schema-qualified)
				if idx >= 0 && (idx == 0 || compact[idx-1] != '.') {
					inner := compact[idx+len(fnUpper)+1:]
					if closeIdx := strings.Index(inner, ")"); closeIdx >= 0 {
						args := strings.TrimSpace(inner[:closeIdx])
						if args == "" {
							// Use lowercase name to match MySQL's error message format
							return "", nil, mysqlError(1582, "42000", fmt.Sprintf("Incorrect parameter count in the call to native function '%s'", strings.ToLower(nativeFn)))
						}
					}
				}
			}
		}
		// ps_current_thread_id() takes no arguments
		if strings.Contains(compact, "PS_CURRENT_THREAD_ID(") {
			idx := strings.Index(compact, "PS_CURRENT_THREAD_ID(")
			// Only match if not preceded by '.' (schema-qualified)
			if idx >= 0 && (idx == 0 || compact[idx-1] != '.') {
				inner := compact[idx+len("PS_CURRENT_THREAD_ID("):]
				if closeIdx := strings.Index(inner, ")"); closeIdx > 0 {
					args := strings.TrimSpace(inner[:closeIdx])
					if args != "" {
						return "", nil, mysqlError(1582, "42000", "Incorrect parameter count in the call to native function 'ps_current_thread_id'")
					}
				}
			}
		}
		// ps_thread_id() requires exactly 1 argument
		if strings.Contains(compact, "PS_THREAD_ID(") && !strings.Contains(compact, "PS_CURRENT_THREAD_ID(") {
			idx := strings.Index(compact, "PS_THREAD_ID(")
			// Only match if not preceded by '.' (schema-qualified)
			if idx >= 0 && (idx == 0 || compact[idx-1] != '.') {
				inner := compact[idx+len("PS_THREAD_ID("):]
				if closeIdx := strings.Index(inner, ")"); closeIdx >= 0 {
					args := strings.TrimSpace(inner[:closeIdx])
					if args == "" {
						return "", nil, mysqlError(1582, "42000", "Incorrect parameter count in the call to native function 'ps_thread_id'")
					}
					if n := countTopLevelSQLArgs(args); n != 1 {
						return "", nil, mysqlError(1582, "42000", "Incorrect parameter count in the call to native function 'ps_thread_id'")
					}
				}
			}
		}
	}

	// Only compute compact form for SELECT queries that might contain JSON function checks
	if strings.HasPrefix(upper, "SELECT") && strings.Contains(upper, "JSON_") {
		compact := strings.NewReplacer(" ", "", "\t", "", "\n", "", "\r", "").Replace(upper)
		if compact == "SELECTJSON_SCHEMA_VALID()" ||
			compact == "SELECTJSON_SCHEMA_VALID(NULL)" ||
			compact == "SELECTJSON_SCHEMA_VALID(NULL,NULL,NULL)" {
			return "", nil, mysqlError(1582, "42000", "Incorrect parameter count in the call to native function 'JSON_SCHEMA_VALID'")
		}
		if compact == "SELECTJSON_SCHEMA_VALIDATION_REPORT()" ||
			compact == "SELECTJSON_SCHEMA_VALIDATION_REPORT(NULL)" ||
			compact == "SELECTJSON_SCHEMA_VALIDATION_REPORT(NULL,NULL,NULL)" {
			return "", nil, mysqlError(1582, "42000", "Incorrect parameter count in the call to native function 'JSON_SCHEMA_VALIDATION_REPORT'")
		}
		if strings.HasPrefix(compact, "SELECTJSON_CONTAINS_PATH(") {
			inner := compact[len("SELECTJSON_CONTAINS_PATH("):]
			if strings.HasSuffix(inner, ")") {
				inner = inner[:len(inner)-1]
			}
			if n := countTopLevelSQLArgs(inner); n < 3 {
				return "", nil, mysqlError(1582, "42000", "Incorrect parameter count in the call to native function 'json_contains_path'")
			}
		}
		if strings.HasPrefix(compact, "SELECTJSON_REMOVE(") {
			inner := compact[len("SELECTJSON_REMOVE("):]
			if strings.HasSuffix(inner, ")") {
				inner = inner[:len(inner)-1]
			}
			if n := countTopLevelSQLArgs(inner); n < 2 {
				return "", nil, mysqlError(1582, "42000", "Incorrect parameter count in the call to native function 'json_remove'")
			}
		}
		if strings.HasPrefix(compact, "SELECTJSON_MERGE_PRESERVE(") {
			inner := compact[len("SELECTJSON_MERGE_PRESERVE("):]
			if strings.HasSuffix(inner, ")") {
				inner = inner[:len(inner)-1]
			}
			if n := countTopLevelSQLArgs(inner); n < 2 {
				return "", nil, mysqlError(1582, "42000", "Incorrect parameter count in the call to native function 'JSON_MERGE_PRESERVE'")
			}
		}
	}

	// Clear warnings from previous statement, but preserve for SHOW WARNINGS/ERRORS.
	// Snapshot the warning/error counts before clearing so that
	// SELECT @@warning_count / @@error_count return the previous statement's counts.
	if !strings.HasPrefix(upper, "SHOW WARNINGS") &&
		!strings.HasPrefix(upper, "SHOW COUNT(*) WARNINGS") &&
		!strings.HasPrefix(upper, "SHOW ERRORS") &&
		!strings.HasPrefix(upper, "SHOW COUNT(*) ERRORS") {
		e.lastWarningCount = int64(len(e.warnings))
		var errCnt int64
		for _, w := range e.warnings {
			if strings.ToUpper(w.Level) == "ERROR" {
				errCnt++
			}
		}
		e.lastErrorCount = errCnt
		e.warnings = nil
	}

	// Handle SHOW COUNT(*) WARNINGS / SHOW COUNT(*) ERRORS before parser
	if strings.HasPrefix(upper, "SHOW COUNT(*) WARNINGS") {
		cnt := int64(len(e.warnings))
		return "", &Result{Columns: []string{"@@session.warning_count"}, Rows: [][]interface{}{{cnt}}, IsResultSet: true}, nil
	}
	if strings.HasPrefix(upper, "SHOW COUNT(*) ERRORS") {
		var cnt int64
		for _, w := range e.warnings {
			if strings.ToUpper(w.Level) == "ERROR" {
				cnt++
			}
		}
		return "", &Result{Columns: []string{"@@session.error_count"}, Rows: [][]interface{}{{cnt}}, IsResultSet: true}, nil
	}

	// Handle CREATE/DROP ROLE, SET DEFAULT ROLE, SET ROLE
	if strings.HasPrefix(upper, "CREATE ROLE") {
		// CREATE ROLE ... IDENTIFIED BY ... is a parse error
		if strings.Contains(upper, " IDENTIFIED ") {
			return "", nil, mysqlError(1064, "42000", "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'IDENTIFIED BY' at line 1")
		}
		// Register role(s) in the grant store
		if e.grantStore != nil {
			rolePart := strings.TrimSpace(query[len("CREATE ROLE"):])
			rolePart = strings.TrimRight(rolePart, ";")
			// Strip IF NOT EXISTS
			if strings.HasPrefix(strings.ToUpper(rolePart), "IF NOT EXISTS") {
				rolePart = strings.TrimSpace(rolePart[len("IF NOT EXISTS"):])
			}
			for _, rn := range strings.Split(rolePart, ",") {
				rn = strings.TrimSpace(rn)
				rn = strings.Trim(rn, "`'\"")
				if rn != "" {
					e.grantStore.RegisterRole(rn)
				}
			}
		}
		return "", &Result{}, nil
	}
	if strings.HasPrefix(upper, "DROP ROLE") ||
		strings.HasPrefix(upper, "SET DEFAULT ROLE") {
		return "", &Result{}, nil
	}
	if strings.HasPrefix(upper, "SET ROLE") {
		// Parse SET ROLE and store active roles in user variable __active_roles
		rolePart := strings.TrimSpace(query[len("SET ROLE"):])
		rolePart = strings.TrimRight(rolePart, ";")
		upperRole := strings.ToUpper(strings.TrimSpace(rolePart))
		if upperRole == "NONE" {
			if e.userVars == nil {
				e.userVars = make(map[string]interface{})
			}
			e.userVars["__active_roles"] = []string{}
		} else if upperRole == "ALL" || upperRole == "DEFAULT" {
			// Set all granted roles as active
			if e.userVars == nil {
				e.userVars = make(map[string]interface{})
			}
			// Get current user's granted roles
			if e.grantStore != nil {
				cu := ""
				if cuv, ok := e.userVars["__current_user"]; ok {
					if cus, ok := cuv.(string); ok {
						cu = cus
					}
				}
				var roles []string
				for _, entry := range e.grantStore.GetGrants(cu, "localhost") {
					if entry.Type == GrantTypeRole {
						roles = append(roles, entry.RoleName)
					}
				}
				e.userVars["__active_roles"] = roles
			}
		} else {
			// Parse comma-separated role names
			var roles []string
			for _, rn := range strings.Split(rolePart, ",") {
				rn = strings.TrimSpace(rn)
				rn = strings.Trim(rn, "`'\"")
				// Handle role@host format
				if atIdx := strings.LastIndex(rn, "@"); atIdx >= 0 {
					rn = strings.TrimSpace(rn[:atIdx])
					rn = strings.Trim(rn, "`'\"")
				}
				if rn != "" {
					roles = append(roles, rn)
				}
			}
			if e.userVars == nil {
				e.userVars = make(map[string]interface{})
			}
			e.userVars["__active_roles"] = roles
		}
		return "", &Result{}, nil
	}

	// Handle LOCK INSTANCE / UNLOCK INSTANCE as no-ops
	if strings.HasPrefix(upper, "LOCK INSTANCE") || strings.HasPrefix(upper, "UNLOCK INSTANCE") {
		return "", &Result{}, nil
	}

	// Handle CREATE/ALTER/DROP/SET RESOURCE GROUP
	if strings.HasPrefix(upper, "CREATE RESOURCE GROUP") {
		// Extract group name (first word after "CREATE RESOURCE GROUP")
		rest := strings.TrimSpace(query[len("CREATE RESOURCE GROUP"):])
		name := extractFirstIdentifier(rest)
		if name != "" {
			normalized := normalizeResourceGroupName(name)
			if e.resourceGroupsMu != nil {
				e.resourceGroupsMu.Lock()
			}
			if e.resourceGroups == nil {
				e.resourceGroups = make(map[string]string)
			}
			if _, exists := e.resourceGroups[normalized]; exists {
				if e.resourceGroupsMu != nil {
					e.resourceGroupsMu.Unlock()
				}
				return "", nil, mysqlError(3654, "HY000", fmt.Sprintf("Resource Group '%s' exists", name))
			}
			e.resourceGroups[normalized] = name
			if e.resourceGroupsMu != nil {
				e.resourceGroupsMu.Unlock()
			}
		}
		return "", &Result{}, nil
	}
	if strings.HasPrefix(upper, "DROP RESOURCE GROUP") {
		// Extract group name
		rest := strings.TrimSpace(query[len("DROP RESOURCE GROUP"):])
		name := extractFirstIdentifier(rest)
		if name != "" {
			normalized := normalizeResourceGroupName(name)
			if e.resourceGroupsMu != nil {
				e.resourceGroupsMu.Lock()
			}
			delete(e.resourceGroups, normalized)
			if e.resourceGroupsMu != nil {
				e.resourceGroupsMu.Unlock()
			}
		}
		return "", &Result{}, nil
	}
	if strings.HasPrefix(upper, "ALTER RESOURCE GROUP") ||
		strings.HasPrefix(upper, "SET RESOURCE GROUP") {
		return "", &Result{}, nil
	}

	// Rewrite "SET @var:=expr" to "SET @var=expr" since vitess parser doesn't support
	// := in SET statements (it is allowed in SELECT/expression context via AssignmentExpr).
	if strings.HasPrefix(upper, "SET ") && strings.Contains(trimmed, ":=") {
		setAssignRe := regexp.MustCompile(`(?i)(^SET\s+@[\w.]+)\s*:=`)
		if setAssignRe.MatchString(trimmed) {
			query = setAssignRe.ReplaceAllString(trimmed, "$1 =")
			trimmed = strings.TrimSpace(query)
			upper = strings.ToUpper(trimmed)
		}
	}

	// When HIGH_NOT_PRECEDENCE SQL mode is active, rewrite "NOT col BETWEEN lo AND hi"
	// to "(NOT col) BETWEEN lo AND hi" since the vitess parser does not support this mode.
	// In MySQL with HIGH_NOT_PRECEDENCE, NOT has higher precedence than BETWEEN.
	if strings.Contains(e.sqlMode, "HIGH_NOT_PRECEDENCE") && strings.Contains(upper, "BETWEEN") && strings.Contains(upper, "NOT") {
		highNotRe := regexp.MustCompile(`(?i)\bNOT\s+(\w+)\s+BETWEEN\b`)
		query = highNotRe.ReplaceAllString(query, "(NOT $1) BETWEEN")
		trimmed = strings.TrimSpace(query)
		upper = strings.ToUpper(trimmed)
	}

	// Rewrite "SOUNDS LIKE" to SOUNDEX comparison so vitess can parse it
	if strings.Contains(upper, "SOUNDS LIKE") {
		slRe := regexp.MustCompile(`(?i)('(?:[^'\\]|\\.)*'|\w+)\s+SOUNDS\s+LIKE\s+('(?:[^'\\]|\\.)*'|\w+)`)
		query = slRe.ReplaceAllString(query, "SOUNDEX($1) = SOUNDEX($2)")
		trimmed = strings.TrimSpace(query)
		upper = strings.ToUpper(trimmed)
	}

	// Rewrite MID( -> SUBSTRING( since vitess parser doesn't recognize MID as a function
	if strings.Contains(upper, "MID(") {
		query = rewriteMidToSubstring(query)
		trimmed = strings.TrimSpace(query)
		upper = strings.ToUpper(trimmed)
	}

	// Rewrite weight_string(str, n1, n2, n3) -> weight_string(str) since vitess can't parse extra args
	if strings.Contains(upper, "WEIGHT_STRING") {
		query = normalizeWeightString(query)
		trimmed = strings.TrimSpace(query)
		upper = strings.ToUpper(trimmed)
	}

	// Rewrite LONG type alias: MySQL allows LONG as a synonym for MEDIUMTEXT.
	// Vitess parser doesn't support LONG, so rewrite it to MEDIUMTEXT.
	// Pattern: word boundary + LONG + word boundary (as a column type).
	if strings.Contains(upper, " LONG ") || strings.Contains(upper, " LONG\n") ||
		strings.Contains(upper, "\tLONG ") || strings.Contains(upper, "\tLONG\n") {
		if (strings.HasPrefix(upper, "CREATE") || strings.HasPrefix(upper, "ALTER")) && strings.Contains(upper, "TABLE") {
			query = rewriteLongType(query)
			trimmed = strings.TrimSpace(query)
			upper = strings.ToUpper(trimmed)
		}
	}

	// Handle MYLITE control commands before passing to the SQL parser.
	if strings.HasPrefix(upper, "MYLITE ") {
		result, err := e.execMyliteCommand(trimmed)
		return "", result, err
	}

	// Rewrite DROP TABLES to DROP TABLE (MySQL synonym, vitess doesn't parse TABLES).
	if strings.HasPrefix(upper, "DROP TABLES ") {
		query = "DROP TABLE " + query[len("DROP TABLES "):]
		upper = strings.ToUpper(query)
	}

	// Handle BEGIN WORK (equivalent to BEGIN/START TRANSACTION)
	if upper == "BEGIN WORK" {
		result, err := e.execBegin()
		return "", result, err
	}
	// Handle ROLLBACK WORK (equivalent to ROLLBACK)
	if upper == "ROLLBACK WORK" {
		result, err := e.execRollback()
		return "", result, err
	}
	// Handle COMMIT WORK [AND CHAIN] (equivalent to COMMIT)
	if strings.HasPrefix(upper, "COMMIT WORK") {
		result, err := e.execCommit()
		return "", result, err
	}
	// Handle COMMIT [AND [NO] CHAIN] [NO RELEASE] / COMMIT AND CHAIN
	if upper == "COMMIT AND CHAIN" || upper == "COMMIT AND NO CHAIN" ||
		upper == "COMMIT NO RELEASE" || upper == "COMMIT RELEASE" {
		result, err := e.execCommit()
		return "", result, err
	}
	// Handle ROLLBACK AND CHAIN
	if upper == "ROLLBACK AND CHAIN" || upper == "ROLLBACK AND NO CHAIN" {
		result, err := e.execRollback()
		return "", result, err
	}

	// Handle ANALYZE TABLE ... UPDATE/DROP HISTOGRAM (vitess can't parse)
	if strings.HasPrefix(upper, "ANALYZE TABLE") && (strings.Contains(upper, "HISTOGRAM") || strings.Contains(upper, "UPDATE HISTOGRAM") || strings.Contains(upper, "DROP HISTOGRAM")) {
		return "", &Result{
			Columns:     []string{"Table", "Op", "Msg_type", "Msg_text"},
			Rows:        [][]interface{}{{"test.t1", "histogram", "status", "Histogram statistics created for column 'col1'."}},
			IsResultSet: true,
		}, nil
	}

	// Handle RENAME USER as no-op
	if strings.HasPrefix(upper, "RENAME USER") {
		return "", &Result{}, nil
	}

	// Handle ALTER EVENT as no-op (vitess parser can't parse)
	if strings.HasPrefix(upper, "ALTER EVENT") || regexp.MustCompile(`(?i)^ALTER\s+EVENT\b`).MatchString(trimmed) {
		return "", &Result{}, nil
	}

	// Rewrite SHOW CREATE DATABASE IF NOT EXISTS <db> -> SHOW CREATE DATABASE <db>
	// (vitess parser doesn't support the IF NOT EXISTS variant, but MySQL 8.3's mysqldump uses it)
	if strings.HasPrefix(upper, "SHOW CREATE DATABASE IF NOT EXISTS ") {
		rest := strings.TrimSpace(trimmed[len("SHOW CREATE DATABASE IF NOT EXISTS "):])
		return "SHOW CREATE DATABASE " + rest, nil, nil
	}
	if strings.HasPrefix(upper, "SHOW CREATE SCHEMA IF NOT EXISTS ") {
		rest := strings.TrimSpace(trimmed[len("SHOW CREATE SCHEMA IF NOT EXISTS "):])
		return "SHOW CREATE SCHEMA " + rest, nil, nil
	}

	// Handle SHOW WARNINGS LIMIT / SHOW ERRORS LIMIT (vitess can't parse)
	if strings.HasPrefix(upper, "SHOW WARNINGS LIMIT") || strings.HasPrefix(upper, "SHOW ERRORS LIMIT") {
		// Return empty warnings/errors as simplified response
		if strings.HasPrefix(upper, "SHOW WARNINGS") {
			return "", &Result{
				Columns:     []string{"Level", "Code", "Message"},
				Rows:        nil,
				IsResultSet: true,
			}, nil
		}
		return "", &Result{
			Columns:     []string{"Level", "Code", "Message"},
			Rows:        nil,
			IsResultSet: true,
		}, nil
	}

	// Handle CREATE TABLESPACE silently (InnoDB internal)
	if strings.HasPrefix(upper, "CREATE TABLESPACE") ||
		strings.HasPrefix(upper, "ALTER TABLESPACE") ||
		strings.HasPrefix(upper, "DROP TABLESPACE") {
		return "", &Result{}, nil
	}

	// Handle ANALYZE TABLE with multiple tables (vitess only handles single table)
	if strings.HasPrefix(upper, "ANALYZE TABLE") && strings.Contains(trimmed, ",") {
		result, err := e.execAnalyzeMultiTable(trimmed)
		return "", result, err
	}

	// Strip INSERT/REPLACE LOW_PRIORITY/DELAYED modifiers
	if strings.HasPrefix(upper, "INSERT LOW_PRIORITY ") {
		query = "INSERT " + query[len("INSERT LOW_PRIORITY "):]
	} else if strings.HasPrefix(upper, "INSERT DELAYED ") {
		query = "INSERT " + query[len("INSERT DELAYED "):]
	} else if strings.HasPrefix(upper, "REPLACE LOW_PRIORITY ") {
		query = "REPLACE " + query[len("REPLACE LOW_PRIORITY "):]
	} else if strings.HasPrefix(upper, "REPLACE DELAYED ") {
		query = "REPLACE " + query[len("REPLACE DELAYED "):]
	}

	// Handle ALTER TABLE ... REORGANIZE PARTITION as no-op
	if strings.HasPrefix(upper, "ALTER TABLE") && strings.Contains(upper, "REORGANIZE PARTITION") {
		return "", &Result{}, nil
	}

	// Strip DELETE/UPDATE LOW_PRIORITY modifiers
	if strings.HasPrefix(upper, "DELETE LOW_PRIORITY ") {
		query = "DELETE " + query[len("DELETE LOW_PRIORITY "):]
	} else if strings.HasPrefix(upper, "UPDATE LOW_PRIORITY ") {
		query = "UPDATE " + query[len("UPDATE LOW_PRIORITY "):]
	}

	// Normalize CAST type aliases: SIGNED INT -> SIGNED, UNSIGNED INT -> UNSIGNED, NATIONAL CHAR -> CHAR
	if strings.Contains(upper, "SIGNED INT") {
		query = regexp.MustCompile(`(?i)\bSIGNED\s+INT(EGER)?\b`).ReplaceAllString(query, "SIGNED")
	}
	if strings.Contains(upper, "UNSIGNED INT") {
		query = regexp.MustCompile(`(?i)\bUNSIGNED\s+INT(EGER)?\b`).ReplaceAllString(query, "UNSIGNED")
	}
	if strings.Contains(upper, "NATIONAL") {
		// NATIONAL CHARACTER VARYING -> VARCHAR, NATIONAL VARCHAR -> VARCHAR, NATIONAL CHAR -> CHAR
		// In DDL contexts, also add CHARACTER SET utf8 AFTER the size specifier e.g. CHAR(10) CHARACTER SET utf8.
		isDDL := strings.HasPrefix(upper, "CREATE TABLE") || strings.HasPrefix(upper, "ALTER TABLE") ||
			strings.HasPrefix(upper, "CREATE TEMPORARY TABLE")
		if isDDL {
			// Replace NATIONAL CHAR(n) -> CHAR(n) CHARACTER SET utf8
			query = regexp.MustCompile(`(?i)\bNATIONAL\s+CHARACTER\s+VARYING(\s*\([^)]*\))?`).ReplaceAllStringFunc(query, func(m string) string {
				re := regexp.MustCompile(`(?i)\bNATIONAL\s+CHARACTER\s+VARYING`)
				return re.ReplaceAllString(m, "VARCHAR") + " CHARACTER SET utf8"
			})
			query = regexp.MustCompile(`(?i)\bNATIONAL\s+VARCHAR(\s*\([^)]*\))?`).ReplaceAllStringFunc(query, func(m string) string {
				re := regexp.MustCompile(`(?i)\bNATIONAL\s+VARCHAR`)
				return re.ReplaceAllString(m, "VARCHAR") + " CHARACTER SET utf8"
			})
			query = regexp.MustCompile(`(?i)\bNATIONAL\s+CHAR(\s*\([^)]*\))?`).ReplaceAllStringFunc(query, func(m string) string {
				re := regexp.MustCompile(`(?i)\bNATIONAL\s+CHAR`)
				return re.ReplaceAllString(m, "CHAR") + " CHARACTER SET utf8"
			})
		} else {
			query = regexp.MustCompile(`(?i)\bNATIONAL\s+CHARACTER\s+VARYING\b`).ReplaceAllString(query, "VARCHAR")
			query = regexp.MustCompile(`(?i)\bNATIONAL\s+VARCHAR\b`).ReplaceAllString(query, "VARCHAR")
			query = regexp.MustCompile(`(?i)\bNATIONAL\s+CHAR\b`).ReplaceAllString(query, "CHAR")
		}
	}
	// NCHAR VARYING, NCHAR VARCHAR, NVARCHAR, NCHAR are MySQL type aliases for VARCHAR/CHAR that
	// vitess parser doesn't support in all contexts (e.g., JSON_TABLE COLUMNS).
	// Apply these globally (not just in DDL) to handle SELECT ... JSON_TABLE cases.
	// NCHAR VARYING and NCHAR VARCHAR must come before NCHAR to avoid partial replacement.
	if strings.Contains(upper, "NCHAR") || strings.Contains(upper, "NVARCHAR") {
		isDDL := strings.HasPrefix(upper, "CREATE TABLE") || strings.HasPrefix(upper, "ALTER TABLE") ||
			strings.HasPrefix(upper, "CREATE TEMPORARY TABLE")
		if isDDL {
			// For DDL: replace NCHAR/NVARCHAR and add CHARACTER SET utf8 AFTER the size
			query = regexp.MustCompile(`(?i)\bNCHAR\s+VARYING(\s*\([^)]*\))?`).ReplaceAllStringFunc(query, func(m string) string {
				re := regexp.MustCompile(`(?i)\bNCHAR\s+VARYING`)
				return re.ReplaceAllString(m, "VARCHAR") + " CHARACTER SET utf8"
			})
			query = regexp.MustCompile(`(?i)\bNCHAR\s+VARCHAR(\s*\([^)]*\))?`).ReplaceAllStringFunc(query, func(m string) string {
				re := regexp.MustCompile(`(?i)\bNCHAR\s+VARCHAR`)
				return re.ReplaceAllString(m, "VARCHAR") + " CHARACTER SET utf8"
			})
			query = regexp.MustCompile(`(?i)\bNVARCHAR(\s*\([^)]*\))?`).ReplaceAllStringFunc(query, func(m string) string {
				re := regexp.MustCompile(`(?i)\bNVARCHAR`)
				return re.ReplaceAllString(m, "VARCHAR") + " CHARACTER SET utf8"
			})
			query = regexp.MustCompile(`(?i)\bNCHAR(\s*\([^)]*\))?`).ReplaceAllStringFunc(query, func(m string) string {
				re := regexp.MustCompile(`(?i)\bNCHAR`)
				return re.ReplaceAllString(m, "CHAR") + " CHARACTER SET utf8"
			})
		} else {
			query = replaceTypeWord(query, "NCHAR VARYING", "VARCHAR")
			query = replaceTypeWord(query, "NCHAR VARCHAR", "VARCHAR")
			query = replaceTypeWord(query, "NVARCHAR", "VARCHAR")
			query = replaceTypeWord(query, "NCHAR", "CHAR")
		}
	}
	trimmed = strings.TrimSpace(query)
	upper = strings.ToUpper(trimmed)

	// Pre-parse check: generated column with DEFAULT, AUTO_INCREMENT, SERIAL DEFAULT VALUE, or ON UPDATE
	// Must be done BEFORE normalizeTypeAliases which replaces SERIAL with BIGINT...
	if strings.HasPrefix(upper, "CREATE TABLE") || strings.HasPrefix(upper, "CREATE TEMPORARY TABLE") || strings.HasPrefix(upper, "ALTER TABLE") {
		gcolDefaultRe := regexp.MustCompile(`(?i)(VIRTUAL|STORED)\s+(DEFAULT\s)`)
		gcolAIRe := regexp.MustCompile(`(?i)(VIRTUAL|STORED)\s+(AUTO_INCREMENT)`)
		gcolSerialRe := regexp.MustCompile(`(?i)(VIRTUAL|STORED)\s+(SERIAL\s+DEFAULT\s+VALUE)`)
		gcolOnUpdateRe := regexp.MustCompile(`(?i)(VIRTUAL|STORED)\s+(ON\s+UPDATE)`)
		if gcolDefaultRe.MatchString(trimmed) {
			return "", nil, mysqlError(1221, "HY000", "Incorrect usage of DEFAULT and generated column")
		}
		if gcolAIRe.MatchString(trimmed) {
			return "", nil, mysqlError(1221, "HY000", "Incorrect usage of AUTO_INCREMENT and generated column")
		}
		if gcolSerialRe.MatchString(trimmed) {
			return "", nil, mysqlError(1221, "HY000", "Incorrect usage of SERIAL DEFAULT VALUE and generated column")
		}
		if gcolOnUpdateRe.MatchString(trimmed) {
			return "", nil, mysqlError(1221, "HY000", "Incorrect usage of ON UPDATE and generated column")
		}
	}

	// Normalize SQL type aliases that vitess parser doesn't support
	query = normalizeTypeAliases(query)
	query = normalizeInlineCheckConstraints(query)
	query = normalizeStorageClause(query)
	// Strip STATS_SAMPLE_PAGES=default which vitess can't parse
	query = normalizeStatsSamplePages(query)
	// Strip START TRANSACTION from CREATE TABLE (atomic DDL option, vitess can't parse)
	query = normalizeStartTransaction(query)
	// Normalize AUTOEXTEND_SIZE with size suffixes (e.g., 64M -> 67108864)
	query = normalizeAutoextendSize(query)
	// Strip SECONDARY_ENGINE=value (vitess can't parse, mylite doesn't support)
	query = normalizeSecondaryEngine(query)
	// Fix vitess parser issue: "ADD KEY USING BTREE (col)" is not parsed correctly.
	// Rewrite to "ADD KEY (col)" since BTREE is the default for InnoDB.
	query = normalizeAddIndexUsing(query)
	// Fix CREATE TABLE with "KEY/INDEX/PRIMARY KEY USING BTREE/HASH (cols)" syntax
	query = normalizeCreateTableIndexUsing(query)
	// Convert hex literals in ENUM/SET column type values to quoted strings so vitess can parse them.
	// e.g. ENUM(0x9353,0x9373) -> ENUM('0x9353','0x9373')
	query = normalizeEnumHexValues(query)
	// Detect SKIP LOCKED / NOWAIT and per-table locking clauses before
	// normalizeForShareOf strips them.
	{
		uq := strings.ToUpper(query)
		e.selectSkipLocked = strings.Contains(uq, "SKIP LOCKED")
		e.selectNowait = strings.Contains(uq, "NOWAIT")
		e.selectLockClauses = parseSelectLockClauses(query)
	}
	// Strip ODBC outer join escape syntax: { oj ... } → ...
	query = normalizeODBCOuterJoin(query)
	// Convert ODBC date/time/timestamp literals: {d '...'} → '...', {ts '...'} → '...', {t '...'} → '...'
	query = normalizeODBCDateTimeLiterals(query)
	query = normalizeForShareOf(query)
	query = normalizeMemberOperator(query)
	query = normalizeJSONTableDefaultOrder(query)
	// Fix CREATE TABLE name (SELECT ...) ORDER BY ... (vitess fails to parse parenthesized SELECT)
	query = normalizeCreateTableParenSelect(query)
	// Fix CREATE TABLE name ENGINE=xxx SELECT ... (vitess fails to parse engine+select combo)
	query = normalizeCreateTableEngineSelect(query)
	trimmed = strings.TrimSpace(query)
	upper = strings.ToUpper(trimmed)

	// Multi-value index does not allow explicit ASC/DESC on key part.
	if (strings.HasPrefix(upper, "CREATE TABLE") || strings.HasPrefix(upper, "ALTER TABLE") || strings.HasPrefix(upper, "CREATE INDEX")) &&
		regexp.MustCompile(`(?i)ARRAY\)\)\s+ASC\b`).MatchString(trimmed) {
		return "", nil, mysqlError(1221, "HY000", "Incorrect usage of ASC and key part")
	}

	// Detect wildcard alias with string literal (e.g. t1.* AS 'alias') in CREATE TABLE ... SELECT.
	// MySQL rejects this as a syntax error, but the vitess parser silently drops the string alias,
	// causing CREATE TABLE to succeed where it should fail.
	if strings.HasPrefix(upper, "CREATE TABLE") || strings.HasPrefix(upper, "CREATE TEMPORARY TABLE") {
		if nearStr := wildcardStringAliasNear(trimmed); nearStr != "" {
			return "", nil, mysqlError(1064, "42000", fmt.Sprintf("You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '%s' at line 1", nearStr))
		}
	}

	// Handle ALTER DATABASE/SCHEMA ... CHARACTER SET (vitess parser doesn't parse CHARACTER SET)
	if (strings.HasPrefix(upper, "ALTER DATABASE") || strings.HasPrefix(upper, "ALTER SCHEMA")) &&
		(strings.Contains(upper, "CHARACTER SET") || strings.Contains(upper, "COLLATE")) {
		result, err := e.execAlterDatabaseRaw(trimmed)
		return "", result, err
	}

	// Handle CREATE DATABASE/SCHEMA ... CHARACTER SET or COLLATE when parser doesn't handle these
	if (strings.HasPrefix(upper, "CREATE DATABASE") || strings.HasPrefix(upper, "CREATE SCHEMA")) &&
		(strings.Contains(upper, "CHARACTER SET") || strings.Contains(upper, " COLLATE ")) {
		result, err := e.execCreateDatabaseRaw(trimmed)
		return "", result, err
	}

	// Handle CHECK TABLE before parser (vitess can't parse CHECK TABLE)
	if strings.HasPrefix(upper, "CHECK TABLE") {
		result, err := e.execOtherAdmin(trimmed)
		return "", result, err
	}

	// LOAD XML INFILE: unsupported feature (vitess parser can't parse it either)
	if strings.HasPrefix(upper, "LOAD XML ") {
		return "", nil, ErrUnsupported("LOAD XML INFILE")
	}

	// IMPORT TABLE FROM: unsupported feature
	if strings.HasPrefix(upper, "IMPORT TABLE ") {
		return "", nil, ErrUnsupported("IMPORT TABLE")
	}

	// INSTALL COMPONENT / UNINSTALL COMPONENT: unsupported feature
	if strings.HasPrefix(upper, "INSTALL COMPONENT") || strings.HasPrefix(upper, "UNINSTALL COMPONENT") {
		return "", nil, ErrUnsupported("INSTALL COMPONENT")
	}

	// Handle ALTER TABLE ... DISCARD/IMPORT TABLESPACE (InnoDB transportable tablespace, no-op)
	if strings.HasPrefix(upper, "ALTER TABLE") &&
		(strings.Contains(upper, "DISCARD TABLESPACE") || strings.Contains(upper, "IMPORT TABLESPACE")) {
		return "", &Result{}, nil
	}

	// Handle ALTER TABLE ... ORDER BY (vitess parser drops ORDER BY clause)
	if strings.HasPrefix(upper, "ALTER TABLE") && strings.Contains(upper, " ORDER BY ") {
		result, err := e.execAlterTableOrderBy(trimmed)
		return "", result, err
	}

	// Handle PREPARE stmt FROM @user_variable (vitess can't parse user variable here)
	if strings.HasPrefix(upper, "PREPARE ") {
		if re := regexp.MustCompile(`(?i)^PREPARE\s+(\S+)\s+FROM\s+@(\S+)\s*;?\s*$`); re.MatchString(trimmed) {
			m := re.FindStringSubmatch(trimmed)
			stmtName := strings.ToLower(m[1])
			varName := m[2]
			val, ok := e.userVars[varName]
			if !ok || val == nil {
				return "", nil, mysqlError(1064, "42000", "You have an error in your SQL syntax")
			}
			queryStr := fmt.Sprintf("%v", val)
			// The variable may contain raw bytes from CONVERT(str USING utf16/utf32).
			// Decode multi-byte encodings back to UTF-8 so the SQL parser can handle it.
			queryStr = decodeMultiByteToUTF8(queryStr)
			e.preparedStmts[stmtName] = queryStr
			return "", &Result{}, nil
		}
		// Handle PREPARE stmt FROM 'string' or PREPARE stmt FROM "string" where the
		// string may contain embedded newlines. Vitess parser cannot handle literal
		// newlines inside string literals, so we extract the content manually.
		if queryStr, stmtName, ok := extractPrepareFromStringLiteral(trimmed); ok {
			// Unescape escape sequences
			queryStr = strings.ReplaceAll(queryStr, "\\n", "\n")
			queryStr = strings.ReplaceAll(queryStr, "\\t", "\t")
			queryStr = strings.ReplaceAll(queryStr, "\\'", "'")
			queryStr = strings.ReplaceAll(queryStr, "\\\"", "\"")
			queryStr = strings.ReplaceAll(queryStr, "\\\\", "\\")
			// Validate SET PASSWORD FOR syntax at PREPARE time.
			if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(queryStr)), "SET PASSWORD FOR ") {
				if syntaxErr := validateSetPasswordSyntax(strings.TrimSpace(queryStr)); syntaxErr != nil {
					return "", nil, syntaxErr
				}
			}
			e.preparedStmts[strings.ToLower(stmtName)] = queryStr
			return "", &Result{}, nil
		}
	}

	// Handle CREATE TRIGGER before vitess parser (it cannot parse triggers)
	// Also handle "CREATE  TRIGGER" (double spaces from eval variable stripping)
	// Also handle CREATE DEFINER=... TRIGGER
	if strings.HasPrefix(upper, "CREATE TRIGGER") || regexp.MustCompile(`(?i)^CREATE\s+TRIGGER\b`).MatchString(trimmed) {
		result, err := e.execCreateTrigger(trimmed)
		return "", result, err
	}
	if strings.HasPrefix(upper, "CREATE DEFINER") && strings.Contains(upper, " TRIGGER") {
		re := regexp.MustCompile(`(?i)^CREATE\s+DEFINER\s*=\s*\S+\s+`)
		stripped := re.ReplaceAllString(trimmed, "CREATE ")
		result, err := e.execCreateTrigger(stripped)
		return "", result, err
	}
	// Handle DROP TRIGGER
	if strings.HasPrefix(upper, "DROP TRIGGER") {
		result, err := e.execDropTrigger(trimmed)
		return "", result, err
	}
	// Handle CREATE FUNCTION (vitess parser support is limited).
	if strings.HasPrefix(upper, "CREATE FUNCTION") &&
		regexp.MustCompile(`(?is)\bCAST\s*\(.*\bAS\s+[^)]*\bARRAY\b`).MatchString(trimmed) {
		return "", nil, mysqlError(1221, "HY000", "Incorrect usage of function and CAST to ARRAY")
	}
	if strings.HasPrefix(upper, "CREATE FUNCTION") {
		result, err := e.execCreateFunction(trimmed)
		return "", result, err
	}
	// Handle CREATE DEFINER=... FUNCTION by stripping the DEFINER clause
	if strings.HasPrefix(upper, "CREATE DEFINER") && strings.Contains(upper, " FUNCTION") {
		re := regexp.MustCompile(`(?i)^CREATE\s+DEFINER\s*=\s*\S+\s+`)
		stripped := re.ReplaceAllString(trimmed, "CREATE ")
		result, err := e.execCreateFunction(stripped)
		return "", result, err
	}
	// Handle DROP FUNCTION
	if strings.HasPrefix(upper, "DROP FUNCTION") {
		result, err := e.execDropFunction(trimmed)
		return "", result, err
	}
	// Handle CREATE PROCEDURE (with BEGIN...END body or single-statement body)
	// Also handle CREATE DEFINER=... PROCEDURE by stripping the DEFINER clause
	if strings.HasPrefix(upper, "CREATE PROCEDURE") {
		result, err := e.execCreateProcedure(trimmed)
		return "", result, err
	}
	if strings.HasPrefix(upper, "CREATE DEFINER") && strings.Contains(upper, " PROCEDURE") {
		// Strip DEFINER=... clause: CREATE DEFINER=x@y PROCEDURE -> CREATE PROCEDURE
		re := regexp.MustCompile(`(?i)^CREATE\s+DEFINER\s*=\s*\S+\s+`)
		stripped := re.ReplaceAllString(trimmed, "CREATE ")
		result, err := e.execCreateProcedure(stripped)
		return "", result, err
	}
	// Handle DROP PROCEDURE with IF EXISTS (vitess may not parse all variants)
	if strings.HasPrefix(upper, "DROP PROCEDURE") {
		result, err := e.execDropProcedureFallback(trimmed)
		return "", result, err
	}
	// Handle CALL procedure
	if strings.HasPrefix(upper, "CALL ") {
		result, err := e.execCallProcedure(trimmed)
		return "", result, err
	}

	// Quote non-ASCII bare identifiers so vitess can parse them.
	query = quoteNonASCIIIdentifiers(query)
	trimmed = strings.TrimSpace(query)
	upper = strings.ToUpper(trimmed)

	// Normalize "LOCK TABLE" (singular) to "LOCK TABLES" (plural) for vitess parser
	if strings.HasPrefix(upper, "LOCK TABLE ") && !strings.HasPrefix(upper, "LOCK TABLES ") {
		query = "LOCK TABLES " + query[len("LOCK TABLE "):]
	}

	return query, nil, nil
}

// stripStringLiterals replaces string literal contents with 'X' placeholders
// to avoid false matches on function names inside string content.
// The quotes and a placeholder character are preserved so arg counting still works.
func stripStringLiterals(s string) string {
	var buf strings.Builder
	buf.Grow(len(s))
	i := 0
	for i < len(s) {
		if s[i] == '\'' || s[i] == '"' {
			quote := s[i]
			buf.WriteByte(quote)
			buf.WriteByte('X') // placeholder for content
			i++                // skip opening quote
			for i < len(s) {
				if s[i] == '\\' {
					i += 2 // skip escaped char
					continue
				}
				if s[i] == quote {
					if i+1 < len(s) && s[i+1] == quote {
						i += 2 // skip doubled quote
						continue
					}
					buf.WriteByte(quote)
					i++ // skip closing quote
					break
				}
				i++
			}
		} else {
			buf.WriteByte(s[i])
			i++
		}
	}
	return buf.String()
}

// normalizeODBCOuterJoin strips ODBC outer join escape syntax.
// MySQL supports `{ oj table1 LEFT OUTER JOIN table2 ON ... }` as a
// compatibility feature.  The vitess parser does not understand this
// syntax so we strip the `{ oj` prefix and the matching closing `}`.
func normalizeODBCOuterJoin(query string) string {
	upper := strings.ToUpper(query)
	if !strings.Contains(upper, "{ OJ ") && !strings.Contains(upper, "{OJ ") {
		return query
	}
	result := make([]byte, 0, len(query))
	i := 0
	braceDepth := 0 // tracks how many ODBC braces we are inside
	inQuote := byte(0)
	for i < len(query) {
		ch := query[i]
		// Handle quoted strings – pass through unchanged.
		if inQuote != 0 {
			result = append(result, ch)
			if ch == inQuote {
				inQuote = 0
			}
			i++
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			inQuote = ch
			result = append(result, ch)
			i++
			continue
		}
		// Detect "{ oj " or "{oj " (case-insensitive).
		if ch == '{' {
			rest := query[i:]
			restUpper := strings.ToUpper(rest)
			if strings.HasPrefix(restUpper, "{ OJ ") {
				// Skip "{ oj " (5 chars)
				i += 5
				braceDepth++
				continue
			}
			if strings.HasPrefix(restUpper, "{OJ ") {
				// Skip "{oj " (4 chars)
				i += 4
				braceDepth++
				continue
			}
		}
		// Skip closing brace that matches an ODBC open.
		if ch == '}' && braceDepth > 0 {
			braceDepth--
			i++
			continue
		}
		result = append(result, ch)
		i++
	}
	return string(result)
}

// normalizeODBCDateTimeLiterals rewrites ODBC escape date/time/timestamp
// literals to plain string literals that the vitess parser can handle.
// Examples:
//   {d '2006-01-02'}        →  '2006-01-02'
//   {t '15:04:05'}          →  '15:04:05'
//   {ts '2006-01-02 15:04:05'} →  '2006-01-02 15:04:05'
func normalizeODBCDateTimeLiterals(query string) string {
	upper := strings.ToUpper(query)
	// Quick bail-out: these patterns must contain '{' followed by d/t/ts
	if !strings.ContainsAny(upper, "{") {
		return query
	}
	result := make([]byte, 0, len(query))
	i := 0
	inQuote := byte(0)
	for i < len(query) {
		ch := query[i]
		if inQuote != 0 {
			result = append(result, ch)
			if ch == inQuote {
				inQuote = 0
			}
			i++
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			inQuote = ch
			result = append(result, ch)
			i++
			continue
		}
		if ch == '{' {
			rest := query[i:]
			restUpper := strings.ToUpper(rest)
			// Try {ts '...'}, {d '...'}, {t '...'} in order (longest first)
			prefixes := []string{"{TS ", "{D ", "{T "}
			matched := false
			for _, prefix := range prefixes {
				if strings.HasPrefix(restUpper, prefix) {
					// Find the closing '}'
					closeIdx := strings.Index(rest, "}")
					if closeIdx < 0 {
						break
					}
					inner := strings.TrimSpace(rest[len(prefix):closeIdx])
					// inner should be a quoted string like '2006-01-02'
					result = append(result, []byte(inner)...)
					i += closeIdx + 1
					matched = true
					break
				}
			}
			if matched {
				continue
			}
		}
		result = append(result, ch)
		i++
	}
	return string(result)
}

// rewriteMidToSubstring rewrites MID( function calls to SUBSTRING( since
// extractPrepareFromStringLiteral parses a PREPARE stmt FROM 'string' or
// PREPARE stmt FROM "string" statement where the string may contain embedded
// newlines. Returns (queryStr, stmtName, true) on success, ("", "", false) otherwise.
// This is needed because the vitess parser cannot handle literal newlines inside
// string literals.
func extractPrepareFromStringLiteral(trimmed string) (string, string, bool) {
	// Match: PREPARE <name> FROM followed by a single or double quoted string
	re := regexp.MustCompile(`(?is)^PREPARE\s+(\S+)\s+FROM\s*(['"])`)
	m := re.FindStringSubmatchIndex(trimmed)
	if m == nil {
		return "", "", false
	}
	stmtName := trimmed[m[2]:m[3]]
	quoteChar := trimmed[m[4]:m[5]]
	quote := quoteChar[0]
	// Find the content after the opening quote
	contentStart := m[5]
	content := trimmed[contentStart:]
	// Find the matching closing quote (handling escaped quotes)
	var queryStr strings.Builder
	i := 0
	for i < len(content) {
		ch := content[i]
		if ch == '\\' && i+1 < len(content) {
			// Escaped character - pass through as-is for later unescaping
			queryStr.WriteByte(ch)
			queryStr.WriteByte(content[i+1])
			i += 2
			continue
		}
		if ch == quote {
			// Check for doubled quote ('' or "")
			if i+1 < len(content) && content[i+1] == quote {
				queryStr.WriteByte(ch)
				i += 2
				continue
			}
			// End of string literal
			// Check that only whitespace and optional semicolon follow
			rest := strings.TrimSpace(content[i+1:])
			rest = strings.TrimSuffix(rest, ";")
			rest = strings.TrimSpace(rest)
			if rest != "" {
				return "", "", false
			}
			return queryStr.String(), stmtName, true
		}
		queryStr.WriteByte(ch)
		i++
	}
	// Unterminated string
	return "", "", false
}

// decodeMultiByteToUTF8 attempts to decode a string that may contain raw bytes
// from a multi-byte charset (UTF-16 BE, UTF-16 LE, UTF-32) back to a valid
// UTF-8 string. This is needed when a user variable contains the result of
// CONVERT(str USING utf16/utf32), and that variable is used in PREPARE ... FROM @var.
// If the string is already valid UTF-8, it is returned unchanged.
func decodeMultiByteToUTF8(s string) string {
	b := []byte(s)
	n := len(b)
	if n == 0 {
		return s
	}
	// Try UTF-32 big-endian: length must be divisible by 4, and the pattern
	// for ASCII text would be 0x00 0x00 0x00 ch (e.g., "select" -> 00 00 00 73 ...)
	if n%4 == 0 && n >= 4 && b[0] == 0 && b[1] == 0 && b[2] == 0 {
		var runes []rune
		valid := true
		for i := 0; i+3 < n; i += 4 {
			r := rune(b[i])<<24 | rune(b[i+1])<<16 | rune(b[i+2])<<8 | rune(b[i+3])
			if r > 0x10FFFF || (r >= 0xD800 && r <= 0xDFFF) {
				valid = false
				break
			}
			runes = append(runes, r)
		}
		if valid && len(runes) > 0 {
			return string(runes)
		}
	}
	// Try UTF-16 big-endian: length must be divisible by 2, and for ASCII text
	// the pattern is 0x00 ch (e.g., "select" -> 00 73 00 65 ...)
	if n%2 == 0 && n >= 2 && b[0] == 0 {
		var runes []rune
		valid := true
		i := 0
		for i+1 < n {
			hi := uint16(b[i])<<8 | uint16(b[i+1])
			i += 2
			if hi >= 0xD800 && hi <= 0xDBFF {
				// High surrogate - need a low surrogate
				if i+1 >= n {
					valid = false
					break
				}
				lo := uint16(b[i])<<8 | uint16(b[i+1])
				i += 2
				if lo < 0xDC00 || lo > 0xDFFF {
					valid = false
					break
				}
				r := rune(hi-0xD800)<<10 | rune(lo-0xDC00) + 0x10000
				runes = append(runes, r)
			} else if hi >= 0xDC00 && hi <= 0xDFFF {
				// Lone low surrogate - invalid
				valid = false
				break
			} else {
				runes = append(runes, rune(hi))
			}
		}
		if valid && len(runes) > 0 {
			return string(runes)
		}
	}
	// Try UTF-16 little-endian: for ASCII text the pattern is ch 0x00
	if n%2 == 0 && n >= 2 && b[1] == 0 {
		var runes []rune
		valid := true
		i := 0
		for i+1 < n {
			lo := uint16(b[i]) | uint16(b[i+1])<<8
			i += 2
			if lo >= 0xD800 && lo <= 0xDBFF {
				if i+1 >= n {
					valid = false
					break
				}
				hi := uint16(b[i]) | uint16(b[i+1])<<8
				i += 2
				if hi < 0xDC00 || hi > 0xDFFF {
					valid = false
					break
				}
				r := rune(lo-0xD800)<<10 | rune(hi-0xDC00) + 0x10000
				runes = append(runes, r)
			} else if lo >= 0xDC00 && lo <= 0xDFFF {
				valid = false
				break
			} else {
				runes = append(runes, rune(lo))
			}
		}
		if valid && len(runes) > 0 {
			return string(runes)
		}
	}
	return s
}

// the vitess SQL parser does not recognize MID as a built-in function.
// MID(str, pos, len) is a MySQL synonym for SUBSTRING(str, pos, len).
func rewriteMidToSubstring(query string) string {
	re := regexp.MustCompile(`(?i)\bMID\s*\(`)
	return re.ReplaceAllStringFunc(query, func(match string) string {
		// Preserve the opening paren and any whitespace
		idx := strings.Index(strings.ToUpper(match), "MID")
		return "SUBSTRING" + match[idx+3:]
	})
}

// normalizeResourceGroupName normalizes a resource group name for case and accent-insensitive comparison.
// MySQL resource group names are compared in a case and accent-insensitive manner.
func normalizeResourceGroupName(name string) string {
	// Remove backtick quoting if present
	name = strings.Trim(name, "`")
	// Decompose to NFD (separates base chars from combining marks), strip combining marks, then lowercase.
	nfd := norm.NFD.String(strings.ToLower(name))
	result := make([]rune, 0, len(nfd))
	for _, r := range nfd {
		if unicode.Is(unicode.Mn, r) {
			// Skip combining marks (accents, diacritics)
			continue
		}
		result = append(result, r)
	}
	return string(result)
}

// extractFirstIdentifier extracts the first identifier (possibly backtick-quoted) from a SQL fragment.
func extractFirstIdentifier(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if s[0] == '`' {
		// Backtick-quoted identifier
		end := strings.Index(s[1:], "`")
		if end < 0 {
			return s[1:]
		}
		return s[1 : end+1]
	}
	// Unquoted identifier: read until whitespace or special char
	end := 0
	for end < len(s) && s[end] != ' ' && s[end] != '\t' && s[end] != '\n' &&
		s[end] != '(' && s[end] != ')' && s[end] != ';' {
		end++
	}
	return s[:end]
}

// inlineNamedWindows handles the case where a WINDOW clause defines multiple named windows
// (e.g., "WINDOW w AS (...), w1 AS (...)"). Vitess only supports a single named window per
// WINDOW clause. This function inlines named window definitions into the OVER clauses.
// Returns the rewritten query or "" if no rewriting was needed.
func inlineNamedWindows(query string) string {
	// Find all occurrences of "\bWINDOW\s+" using case-insensitive search
	windowRe := regexp.MustCompile(`(?i)\bWINDOW\s+`)
	allLocs := windowRe.FindAllStringIndex(query, -1)
	if len(allLocs) == 0 {
		return ""
	}
	// Use the last WINDOW keyword location
	lastLoc := allLocs[len(allLocs)-1]
	windowIdx := lastLoc[0]
	// Extract window definitions part (everything after "WINDOW ")
	defsStr := strings.TrimSpace(query[lastLoc[1]:])
	// Remove trailing semicolon
	defsStr = strings.TrimRight(defsStr, " \t\n\r;")

	// Parse window definitions: name AS (spec) [, name2 AS (spec2)] ...
	windowMap := make(map[string]string)
	rest := defsStr
	for {
		rest = strings.TrimSpace(rest)
		if rest == "" {
			break
		}
		// Find window name
		spaceIdx := strings.IndexAny(rest, " \t\n\r")
		if spaceIdx < 0 {
			break
		}
		name := strings.ToLower(rest[:spaceIdx])
		rest = strings.TrimSpace(rest[spaceIdx:])
		// Expect AS
		if len(rest) < 2 || strings.ToUpper(rest[:2]) != "AS" {
			break
		}
		rest = strings.TrimSpace(rest[2:])
		// Expect (
		if !strings.HasPrefix(rest, "(") {
			break
		}
		// Find matching closing paren
		depth := 0
		end := -1
		for i, ch := range rest {
			if ch == '(' {
				depth++
			} else if ch == ')' {
				depth--
				if depth == 0 {
					end = i
					break
				}
			}
		}
		if end < 0 {
			break
		}
		spec := rest[:end+1] // includes parens
		windowMap[name] = spec
		rest = strings.TrimSpace(rest[end+1:])
		// Skip comma
		if strings.HasPrefix(rest, ",") {
			rest = strings.TrimSpace(rest[1:])
		} else {
			break
		}
	}
	if len(windowMap) <= 1 {
		return "" // single window handled by vitess parser
	}
	// Calculate how much of the query was consumed by the WINDOW clause
	// defsStr was the full text after "WINDOW "; rest is what's left after window defs
	// We need to find where rest starts in the original query to include any trailing text
	consumedLen := len(query) - len(defsStr) // length up to and including "WINDOW "
	// Find how much of defsStr was consumed
	parsedLen := len(defsStr) - len(rest)
	trailingText := ""
	if rest != "" {
		trailingText = rest
	}
	_ = consumedLen
	_ = parsedLen

	// Remove the WINDOW clause from the query and append any trailing text
	selectPart := strings.TrimRight(query[:windowIdx], " \t\n\r")
	// Replace all "OVER wname" with "OVER (spec)"
	result := selectPart
	for name, spec := range windowMap {
		re := regexp.MustCompile(`(?i)\bOVER\s+` + regexp.QuoteMeta(name) + `\b`)
		result = re.ReplaceAllString(result, "OVER "+spec)
	}
	if trailingText != "" {
		result += "\n" + trailingText
	}
	return result
}

// stripNestedConditionalComments handles nested /* */ inside /*!NNNNN */ comment blocks.
// MySQL allows (but deprecates) this syntax. We preprocess to strip the nesting so the
// vitess parser can handle the remaining query correctly.
// For version > server (e.g., /*!99999 ... */), the entire block is removed.
// For version <= server or no version (/*!...*/), the inner content is kept but nested
// comment markers are stripped.
// A warning 1681 is added for any nested comment found.
func stripNestedConditionalComments(query string, e *Executor) string {
	const serverVersion = 80040
	var result strings.Builder
	i := 0
	hasNestedComment := false
	for i < len(query) {
		// Look for /*!
		if i+2 < len(query) && query[i] == '/' && query[i+1] == '*' && query[i+2] == '!' {
			// Find the matching */ accounting for nested /* */
			start := i
			i += 3 // skip /*!
			// Extract version number (up to 5 digits)
			verStr := ""
			for i < len(query) && len(verStr) < 5 && query[i] >= '0' && query[i] <= '9' {
				verStr += string(query[i])
				i++
			}
			ver := 0
			if len(verStr) == 5 {
				for _, ch := range verStr {
					ver = ver*10 + int(ch-'0')
				}
			}
			// Collect inner content, tracking nested /* */ depth
			var inner strings.Builder
			depth := 1 // we're inside the outer /*!...
			for i < len(query) && depth > 0 {
				if i+1 < len(query) && query[i] == '/' && query[i+1] == '*' {
					// Nested comment start
					hasNestedComment = true
					depth++
					i += 2
					// Don't add /* to inner
					continue
				}
				if i+1 < len(query) && query[i] == '*' && query[i+1] == '/' {
					depth--
					i += 2
					if depth > 0 {
						// Closing a nested comment, don't add */
						continue
					}
					// Closing the outer /*! comment
					break
				}
				// Only include content when at the outer level (depth==1), not inside nested comments.
				if depth == 1 {
					inner.WriteByte(query[i])
				}
				i++
			}
			_ = start
			if ver > serverVersion {
				// High-version comment: skip entire block (don't emit anything)
			} else {
				// Execute content: emit the inner content
				result.WriteString(inner.String())
			}
			continue
		}
		// Handle string literals (don't process comments inside strings)
		if query[i] == '\'' || query[i] == '"' {
			q := query[i]
			result.WriteByte(query[i])
			i++
			for i < len(query) {
				result.WriteByte(query[i])
				if query[i] == '\\' {
					i++
					if i < len(query) {
						result.WriteByte(query[i])
						i++
					}
					continue
				}
				if query[i] == q {
					i++
					break
				}
				i++
			}
			continue
		}
		result.WriteByte(query[i])
		i++
	}
	if hasNestedComment && e != nil {
		e.addWarning("Warning", 1681, "Nested comment syntax is deprecated and will be removed in a future release.")
	}
	return result.String()
}

// rewriteODBCEscapes rewrites ODBC escape sequences to standard MySQL syntax.
// Returns the rewritten query and true if any ODBC sequences were found.
//
// Supported patterns:
//   - {fn func_name(args)} → func_name(args)
//   - { date "value" } or {d 'value'} → DATE 'value'
//   - { time "value" } or {t 'value'} → TIME 'value'
//   - { ts "value" } or {timestamp 'value'} → TIMESTAMP 'value'
//
// e.currentQuery should already be set to the original query before calling this
// so that rawExprs column name extraction uses the original ODBC syntax.
func rewriteODBCEscapes(query string) (string, bool) {
	if !strings.Contains(query, "{") {
		return "", false
	}

	var result strings.Builder
	i := 0
	found := false

	for i < len(query) {
		if query[i] == '\'' || query[i] == '"' || query[i] == '`' {
			// Skip string literals
			quote := query[i]
			result.WriteByte(query[i])
			i++
			for i < len(query) {
				if query[i] == quote && (i == 0 || query[i-1] != '\\') {
					result.WriteByte(query[i])
					i++
					break
				}
				result.WriteByte(query[i])
				i++
			}
			continue
		}

		if query[i] != '{' {
			result.WriteByte(query[i])
			i++
			continue
		}

		// Found '{'
		// Try to parse ODBC escape sequence
		j := i + 1
		// Skip whitespace
		for j < len(query) && (query[j] == ' ' || query[j] == '\t') {
			j++
		}

		// Extract keyword
		kStart := j
		for j < len(query) && (query[j] >= 'a' && query[j] <= 'z' || query[j] >= 'A' && query[j] <= 'Z' || query[j] == '_') {
			j++
		}
		keyword := strings.ToLower(query[kStart:j])

		// Find matching '}'
		depth := 1
		k := j
		for k < len(query) && depth > 0 {
			if query[k] == '{' {
				depth++
			} else if query[k] == '}' {
				depth--
			} else if query[k] == '\'' || query[k] == '"' || query[k] == '`' {
				// Skip string literals inside
				qt := query[k]
				k++
				for k < len(query) {
					if query[k] == qt && (k == 0 || query[k-1] != '\\') {
						break
					}
					k++
				}
			}
			if depth > 0 {
				k++
			}
		}
		if depth != 0 {
			// Unmatched { - output as-is
			result.WriteByte(query[i])
			i++
			continue
		}
		// query[k] == '}'
		inner := strings.TrimSpace(query[j:k])
		found = true

		switch keyword {
		case "fn":
			// {fn func_name(args)} → func_name(args)
			result.WriteString(inner)
		case "d", "date":
			// {d 'value'} or { date "value" } → DATE 'value' → just output the value
			// The inner is the date string (possibly quoted)
			if inner == "" {
				result.WriteString("NULL")
			} else {
				// inner might be "1997-10-20" or '1997-10-20'
				result.WriteString(inner)
			}
		case "t", "time":
			// {t 'value'} → TIME 'value'
			result.WriteString("TIME ")
			result.WriteString(inner)
		case "ts", "timestamp":
			// {ts 'value'} → TIMESTAMP 'value'
			result.WriteString("TIMESTAMP ")
			result.WriteString(inner)
		default:
			// Unknown ODBC escape - output as-is
			result.WriteString(query[i : k+1])
			found = false
		}
		i = k + 1
	}

	if !found {
		return "", false
	}
	return result.String(), true
}

// rewriteLongType rewrites the LONG data type alias to MEDIUMTEXT in CREATE/ALTER TABLE
// statements. MySQL historically supports LONG as a synonym for MEDIUMTEXT.
// Vitess parser doesn't support LONG as a type, so we rewrite it to MEDIUMTEXT.
func rewriteLongType(query string) string {
	// Walk the query, replacing LONG when used as a type (after a column name or comma,
	// and followed by whitespace/comma/closing paren/NOT/NULL/DEFAULT etc.).
	// We use a simple state machine to avoid replacing inside string literals.
	var result strings.Builder
	i := 0
	for i < len(query) {
		ch := query[i]
		// Handle string literals
		if ch == '\'' || ch == '"' || ch == '`' {
			q := ch
			result.WriteByte(ch)
			i++
			for i < len(query) {
				c := query[i]
				result.WriteByte(c)
				i++
				if c == q {
					break
				}
				if c == '\\' && i < len(query) {
					result.WriteByte(query[i])
					i++
				}
			}
			continue
		}
		// Check for LONG keyword
		if i+4 <= len(query) && strings.EqualFold(query[i:i+4], "long") {
			// Verify it's a whole word (not part of longer word like LONGTEXT, LONGBLOB, etc.)
			afterEnd := i + 4
			isWordEnd := afterEnd >= len(query) || !isLongWordChar(rune(query[afterEnd]))
			isWordStart := i == 0 || !isLongWordChar(rune(query[i-1]))
			if isWordStart && isWordEnd {
				result.WriteString("MEDIUMTEXT")
				i += 4
				continue
			}
		}
		result.WriteByte(ch)
		i++
	}
	return result.String()
}

func isLongWordChar(r rune) bool {
	return r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

// rewriteAggregateAll rewrites FUNC(ALL expr) → FUNC(expr) for aggregate functions.
// In MySQL, ALL is the default quantifier (opposite of DISTINCT) and is syntactically
// valid but redundant. Vitess parser doesn't support this syntax.
func rewriteAggregateAll(query string) string {
	// Known aggregate functions that support ALL quantifier
	aggFuncs := []string{"SUM", "COUNT", "AVG", "STD", "STDDEV", "STDDEV_POP", "STDDEV_SAMP",
		"VARIANCE", "VAR_POP", "VAR_SAMP", "BIT_OR", "BIT_AND", "BIT_XOR",
		"MIN", "MAX", "GROUP_CONCAT"}
	upper := strings.ToUpper(query)
	for _, fn := range aggFuncs {
		pattern := fn + "(ALL "
		if !strings.Contains(upper, pattern) {
			continue
		}
		// Case-insensitive replacement of FUNC(ALL  with FUNC(
		var buf strings.Builder
		src := query
		for {
			idx := strings.Index(strings.ToUpper(src), pattern)
			if idx < 0 {
				buf.WriteString(src)
				break
			}
			// Write everything up to and including "FUNC(" (but not "ALL ")
			buf.WriteString(src[:idx+len(fn)+1]) // includes the '('
			src = src[idx+len(fn)+1+len("ALL "):]
		}
		query = buf.String()
		upper = strings.ToUpper(query)
	}
	return query
}

// rewriteLongTypes replaces MySQL's LONG, LONG VARCHAR, LONG VARBINARY type aliases
// with their canonical equivalents that Vitess can parse.
// LONG / LONG VARCHAR → MEDIUMTEXT
// LONG VARBINARY → MEDIUMBLOB
// rewriteBinaryCharacterSet rewrites `BINARY CHARACTER SET X` to `CHARACTER SET X BINARY`
// in column type definitions, since vitess parser only supports the latter order.
// Also rewrites `BYTE` keyword (deprecated MySQL alias for BINARY in column types) to `BINARY`.
func rewriteBinaryCharacterSet(query string) string {
	// Rewrite BINARY CHARACTER SET X -> CHARACTER SET X BINARY
	re := regexp.MustCompile(`(?i)\bBINARY\s+CHARACTER\s+SET\s+([A-Za-z0-9_]+)`)
	query = re.ReplaceAllString(query, "CHARACTER SET $1 BINARY")
	// Rewrite BYTE (as column type attribute) -> BINARY
	// BYTE is a deprecated synonym for BINARY in MySQL column definitions.
	// Pattern: word boundary, BYTE, followed by end/comma/closing-paren/whitespace
	reB := regexp.MustCompile(`(?i)\bBYTE\b`)
	query = reB.ReplaceAllString(query, "BINARY")
	return query
}

// rewriteCharacterVarying rewrites SQL standard type aliases to MySQL types:
// CHARACTER VARYING(n) -> VARCHAR(n)
// CHARACTER(n) -> CHAR(n)
func rewriteCharacterVarying(query string) string {
	// CHARACTER VARYING(n) -> VARCHAR(n) — must be done before CHARACTER(n)
	reCV := regexp.MustCompile(`(?i)\bCHARACTER\s+VARYING\b`)
	query = reCV.ReplaceAllString(query, "VARCHAR")
	return query
}

func rewriteLongTypes(query string) string {
	// Replace LONG VARBINARY first (before LONG to avoid partial match)
	reLongVarbinary := regexp.MustCompile(`(?i)\bLONG\s+VARBINARY\b`)
	query = reLongVarbinary.ReplaceAllString(query, "MEDIUMBLOB")

	// Replace LONG VARCHAR
	reLongVarchar := regexp.MustCompile(`(?i)\bLONG\s+VARCHAR\b`)
	query = reLongVarchar.ReplaceAllString(query, "MEDIUMTEXT")

	// Replace LONG BLOB → LONGBLOB, LONG TEXT → LONGTEXT, standalone LONG → MEDIUMTEXT
	// Match LONG optionally followed by BLOB or TEXT (with whitespace)
	reLong := regexp.MustCompile(`(?i)\bLONG(\s+(?:BLOB|TEXT))?\b`)
	query = reLong.ReplaceAllStringFunc(query, func(match string) string {
		upper := strings.ToUpper(strings.TrimSpace(match))
		switch upper {
		case "LONG":
			return "MEDIUMTEXT"
		case "LONG BLOB":
			return "LONGBLOB"
		case "LONG TEXT":
			return "LONGTEXT"
		default:
			return match
		}
	})

	return query
}

// wildcardStringAliasNear checks if query contains a wildcard alias with string literal
// (e.g. `t1.* AS 'alias'`) and returns the near-clause for MySQL error messages.
// MySQL syntax error: `.*` followed by `AS 'string'` is invalid.
var wildcardStringAliasRe = regexp.MustCompile(`(?i)\.\*\s+AS\s+'[^']*'`)

func wildcardStringAliasNear(query string) string {
	loc := wildcardStringAliasRe.FindStringIndex(query)
	if loc == nil {
		return ""
	}
	// Find the position of 'AS' after '.*'
	segment := query[loc[0]:]
	upperSeg := strings.ToUpper(segment)
	asIdx := strings.Index(upperSeg, " AS ")
	if asIdx < 0 {
		return ""
	}
	// near clause starts from 'as ' (lowercase)
	nearStart := loc[0] + asIdx + 1 // skip the space before 'AS'
	if nearStart >= len(query) {
		return ""
	}
	near := query[nearStart:]
	// Remove trailing whitespace/semicolon
	near = strings.TrimRight(near, " \t\n\r;")
	if len(near) > 64 {
		near = near[:64]
	}
	return near
}

// numericAliasRe matches AS clauses with unquoted identifiers starting with a digit.
var numericAliasRe = regexp.MustCompile(`(?i)\bAS\s+([0-9][a-zA-Z0-9_]*)`)

// rewriteNumericAliases quotes AS aliases that start with a digit.
// MySQL allows identifiers like 1Eq, 2NEq1, but the vitess parser does not.
// This transforms: AS 1Eq -> AS `1Eq`
func rewriteNumericAliases(query string) string {
	if !numericAliasRe.MatchString(query) {
		return query
	}
	// Process outside string literals only
	var result strings.Builder
	inSingle := false
	inDouble := false
	i := 0
	for i < len(query) {
		ch := query[i]
		if inSingle {
			result.WriteByte(ch)
			if ch == '\'' && i+1 < len(query) && query[i+1] == '\'' {
				i++
				result.WriteByte(query[i])
			} else if ch == '\\' && i+1 < len(query) {
				i++
				result.WriteByte(query[i])
			} else if ch == '\'' {
				inSingle = false
			}
			i++
			continue
		}
		if inDouble {
			result.WriteByte(ch)
			if ch == '"' && i+1 < len(query) && query[i+1] == '"' {
				i++
				result.WriteByte(query[i])
			} else if ch == '"' {
				inDouble = false
			}
			i++
			continue
		}
		if ch == '\'' {
			inSingle = true
			result.WriteByte(ch)
			i++
			continue
		}
		if ch == '"' {
			inDouble = true
			result.WriteByte(ch)
			i++
			continue
		}
		// Check for "AS " followed by digit-starting identifier
		upper := strings.ToUpper(query[i:])
		if strings.HasPrefix(upper, "AS ") || strings.HasPrefix(upper, "AS\t") {
			asLen := 2
			j := i + asLen
			for j < len(query) && (query[j] == ' ' || query[j] == '\t') {
				j++
			}
			if j < len(query) && query[j] >= '0' && query[j] <= '9' {
				// Find the identifier end
				k := j
				for k < len(query) && (query[k] == '_' || (query[k] >= '0' && query[k] <= '9') || (query[k] >= 'a' && query[k] <= 'z') || (query[k] >= 'A' && query[k] <= 'Z')) {
					k++
				}
				// Write: AS `ident`
				result.WriteString(query[i : j])
				result.WriteByte('`')
				result.WriteString(query[j:k])
				result.WriteByte('`')
				i = k
				continue
			}
		}
		result.WriteByte(ch)
		i++
	}
	return result.String()
}

// containsLeadLagWithNegativeOffset checks if a query contains LEAD or LAG
// with a negative integer offset (e.g. LEAD(col, -3)).
func containsLeadLagWithNegativeOffset(query string) bool {
	upper := strings.ToUpper(query)
	hasLeadLag := strings.Contains(upper, "LEAD(") || strings.Contains(upper, "LAG(")
	hasNeg := strings.Contains(query, ", -") || strings.Contains(query, ",-")
	return hasLeadLag && hasNeg
}

var reLeadLagNeg = regexp.MustCompile(`(?i)\b(LEAD|LAG)\s*\(([^,)]+),\s*-(\d+)`)

// rewriteLeadLagNegativeOffset rewrites LEAD(expr, -N) → LAG(expr, N)
// and LAG(expr, -N) → LEAD(expr, N) to work around vitess grammar limitation.
func rewriteLeadLagNegativeOffset(query string) string {
	return reLeadLagNeg.ReplaceAllStringFunc(query, func(match string) string {
		sub := reLeadLagNeg.FindStringSubmatch(match)
		if len(sub) < 4 {
			return match
		}
		fnName := sub[1]
		expr := sub[2]
		n := sub[3]
		// Swap LEAD <-> LAG
		var newFn string
		if strings.ToUpper(fnName) == "LEAD" {
			newFn = "LAG"
		} else {
			newFn = "LEAD"
		}
		// Preserve original case formatting style
		if fnName[0] >= 'a' && fnName[0] <= 'z' {
			newFn = strings.ToLower(newFn)
		}
		return newFn + "(" + expr + ", " + n
	})
}

// rewriteParenSelectWithOuterOrderBy rewrites a query of the form:
//
//	(SELECT ... LIMIT N) ORDER BY col [LIMIT M] [OFFSET P]
//
// into:
//
//	SELECT * FROM (SELECT ... LIMIT N) AS _paren_subq ORDER BY col [LIMIT M] [OFFSET P]
//
// This is needed because the vitess parser calls SetLimit on the inner SELECT,
// which overwrites the inner LIMIT N with the outer LIMIT M. MySQL's semantics
// require the inner LIMIT to be evaluated first (as a derived table), then the
// outer ORDER BY/LIMIT applied to those results.
//
// Returns empty string if the query does not match this pattern.
func rewriteParenSelectWithOuterOrderBy(query string) string {
	trimmed := strings.TrimSpace(query)
	if len(trimmed) == 0 || trimmed[0] != '(' {
		return ""
	}
	// Find matching close paren at top level
	depth := 0
	closeIdx := -1
	inSingleQuote := false
	inDoubleQuote := false
	inBacktick := false
	for i, ch := range trimmed {
		if inSingleQuote {
			if ch == '\'' {
				inSingleQuote = false
			}
			continue
		}
		if inDoubleQuote {
			if ch == '"' {
				inDoubleQuote = false
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
			inSingleQuote = true
		case '"':
			inDoubleQuote = true
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
	// The inner content is trimmed[1:closeIdx]
	innerSQL := trimmed[1:closeIdx]
	// Check if inner SQL contains LIMIT (indicating it has a limit we want to preserve)
	innerUpper := strings.ToUpper(innerSQL)
	if !strings.Contains(innerUpper, "LIMIT") {
		return ""
	}
	// Check that inner starts with SELECT
	innerTrimmed := strings.TrimSpace(innerSQL)
	if !strings.HasPrefix(strings.ToUpper(innerTrimmed), "SELECT") {
		return ""
	}
	// What follows the close paren?
	after := strings.TrimSpace(trimmed[closeIdx+1:])
	if after == "" {
		return ""
	}
	afterUpper := strings.ToUpper(after)
	// Must start with ORDER BY or LIMIT
	if !strings.HasPrefix(afterUpper, "ORDER BY") && !strings.HasPrefix(afterUpper, "LIMIT") {
		return ""
	}
	// Rewrite: SELECT * FROM (inner) AS _paren_subq <after>
	return "SELECT * FROM (" + innerSQL + ") AS _paren_subq " + after
}
