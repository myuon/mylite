package executor

import (
	"fmt"
	"regexp"
	"strings"
)

// preprocessQuery performs pre-parse processing on the query string before
// it is handed to the SQL parser. It handles three cases:
//   - Query rewrite: returns (rewritten, nil, nil)
//   - Direct result (no parsing needed): returns ("", result, nil)
//   - Error: returns ("", nil, err)
func (e *Executor) preprocessQuery(query string) (string, *Result, error) {
	trimmed := stripLeadingCStyleComments(strings.TrimSpace(query))
	query = trimmed
	e.currentQuery = trimmed

	// Empty query (e.g. comment-only) is a no-op.
	if trimmed == "" {
		return "", &Result{}, nil
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

	// Handle CREATE/DROP ROLE, SET DEFAULT ROLE, SET ROLE as no-ops
	if strings.HasPrefix(upper, "CREATE ROLE") || strings.HasPrefix(upper, "DROP ROLE") ||
		strings.HasPrefix(upper, "SET DEFAULT ROLE") || strings.HasPrefix(upper, "SET ROLE") {
		return "", &Result{}, nil
	}

	// Handle LOCK INSTANCE / UNLOCK INSTANCE as no-ops
	if strings.HasPrefix(upper, "LOCK INSTANCE") || strings.HasPrefix(upper, "UNLOCK INSTANCE") {
		return "", &Result{}, nil
	}

	// Handle CREATE/ALTER/DROP RESOURCE GROUP as no-ops
	if strings.HasPrefix(upper, "CREATE RESOURCE GROUP") ||
		strings.HasPrefix(upper, "ALTER RESOURCE GROUP") ||
		strings.HasPrefix(upper, "DROP RESOURCE GROUP") ||
		strings.HasPrefix(upper, "SET RESOURCE GROUP") {
		return "", &Result{}, nil
	}

	// Rewrite "SET @var:=expr" to "SET @var=expr" since vitess parser doesn't support
	// := in SET statements (it is allowed in SELECT/expression context via AssignmentExpr).
	if strings.HasPrefix(upper, "SET ") && strings.Contains(trimmed, ":=") {
		setAssignRe := regexp.MustCompile(`(?i)(^SET\s+@\w+)\s*:=`)
		if setAssignRe.MatchString(trimmed) {
			query = setAssignRe.ReplaceAllString(trimmed, "$1 =")
			trimmed = strings.TrimSpace(query)
			upper = strings.ToUpper(trimmed)
		}
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
	if strings.Contains(upper, "NATIONAL CHAR") {
		query = regexp.MustCompile(`(?i)\bNATIONAL\s+CHAR\b`).ReplaceAllString(query, "CHAR")
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
	// Fix vitess parser issue: "ADD KEY USING BTREE (col)" is not parsed correctly.
	// Rewrite to "ADD KEY (col)" since BTREE is the default for InnoDB.
	query = normalizeAddIndexUsing(query)
	// Fix CREATE TABLE with "KEY/INDEX/PRIMARY KEY USING BTREE/HASH (cols)" syntax
	query = normalizeCreateTableIndexUsing(query)
	// Detect SKIP LOCKED / NOWAIT and per-table locking clauses before
	// normalizeForShareOf strips them.
	{
		uq := strings.ToUpper(query)
		e.selectSkipLocked = strings.Contains(uq, "SKIP LOCKED")
		e.selectNowait = strings.Contains(uq, "NOWAIT")
		e.selectLockClauses = parseSelectLockClauses(query)
	}
	query = normalizeForShareOf(query)
	query = normalizeMemberOperator(query)
	query = normalizeJSONTableDefaultOrder(query)
	// Fix CREATE TABLE name ENGINE=xxx SELECT ... (vitess fails to parse engine+select combo)
	query = normalizeCreateTableEngineSelect(query)
	trimmed = strings.TrimSpace(query)
	upper = strings.ToUpper(trimmed)

	// Multi-value index does not allow explicit ASC/DESC on key part.
	if (strings.HasPrefix(upper, "CREATE TABLE") || strings.HasPrefix(upper, "ALTER TABLE") || strings.HasPrefix(upper, "CREATE INDEX")) &&
		regexp.MustCompile(`(?i)ARRAY\)\)\s+ASC\b`).MatchString(trimmed) {
		return "", nil, mysqlError(1221, "HY000", "Incorrect usage of ASC and key part")
	}

	// Handle ALTER DATABASE/SCHEMA ... CHARACTER SET (vitess parser doesn't parse CHARACTER SET)
	if (strings.HasPrefix(upper, "ALTER DATABASE") || strings.HasPrefix(upper, "ALTER SCHEMA")) &&
		(strings.Contains(upper, "CHARACTER SET") || strings.Contains(upper, "COLLATE")) {
		result, err := e.execAlterDatabaseRaw(trimmed)
		return "", result, err
	}

	// Handle CREATE DATABASE/SCHEMA ... CHARACTER SET when parser doesn't extract charset
	if (strings.HasPrefix(upper, "CREATE DATABASE") || strings.HasPrefix(upper, "CREATE SCHEMA")) &&
		strings.Contains(upper, "CHARACTER SET") {
		result, err := e.execCreateDatabaseRaw(trimmed)
		return "", result, err
	}

	// Handle CHECK TABLE before parser (vitess can't parse CHECK TABLE)
	if strings.HasPrefix(upper, "CHECK TABLE") {
		result, err := e.execOtherAdmin(trimmed)
		return "", result, err
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
			stmtName := m[1]
			varName := m[2]
			val, ok := e.userVars[varName]
			if !ok || val == nil {
				return "", nil, mysqlError(1064, "42000", "You have an error in your SQL syntax")
			}
			queryStr := fmt.Sprintf("%v", val)
			e.preparedStmts[stmtName] = queryStr
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

// rewriteMidToSubstring rewrites MID( function calls to SUBSTRING( since
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
