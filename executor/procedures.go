package executor

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
	"vitess.io/vitess/go/vt/sqlparser"
)

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
	timing := strings.ToUpper(parts[1]) // BEFORE or AFTER
	event := strings.ToUpper(parts[2])  // INSERT, UPDATE, or DELETE

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

	// Check if table references performance_schema — deny CREATE TRIGGER on PS tables
	if strings.Contains(tableName, ".") {
		dbTbl := strings.SplitN(tableName, ".", 2)
		trigDB := strings.Trim(dbTbl[0], "`")
		if strings.EqualFold(trigDB, "performance_schema") {
			return nil, mysqlError(1044, "42000", fmt.Sprintf("Access denied for user 'root'@'localhost' to database 'performance_schema'"))
		}
	}

	// Extract body: everything after "FOR EACH ROW"
	// Use a regex to handle variable whitespace (e.g. "FOR  EACH ROW" with two spaces).
	_ = upper // already have it
	forEachRe := regexp.MustCompile(`(?i)FOR\s+EACH\s+ROW`)
	forEachLoc := forEachRe.FindStringIndex(query)
	if forEachLoc == nil {
		return nil, fmt.Errorf("invalid CREATE TRIGGER syntax: missing FOR EACH ROW")
	}
	body := strings.TrimSpace(query[forEachLoc[1]:])

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

// isStandaloneEnd returns true if the upper-cased line represents a standalone
// END (optionally followed by a label) but NOT "END IF", "END WHILE",
// "END LOOP", "END CASE", or "END REPEAT".
func isStandaloneEnd(lineUpper string) bool {
	if lineUpper == "END" {
		return true
	}
	if !strings.HasPrefix(lineUpper, "END ") {
		return false
	}
	after := strings.TrimSpace(lineUpper[4:])
	switch after {
	case "IF", "WHILE", "LOOP", "CASE", "REPEAT":
		return false
	}
	// Also reject if it starts with one of these keywords (e.g. "END IF label")
	for _, kw := range []string{"IF ", "WHILE ", "LOOP ", "CASE ", "REPEAT "} {
		if strings.HasPrefix(after, kw) {
			return false
		}
	}
	return true
}

// countOccurrences counts the number of non-overlapping occurrences of substr in s.
// It only counts whole-word occurrences where "IF " means IF followed by space (not part of END IF).
func countOccurrences(s, substr string) int {
	if substr == "IF " {
		// Count IF that aren't preceded by END
		count := 0
		idx := 0
		for {
			pos := strings.Index(s[idx:], "IF ")
			if pos < 0 {
				break
			}
			absPos := idx + pos
			// Check it's not preceded by "END " or "ELSEIF"
			if absPos >= 4 && s[absPos-4:absPos] == "END " {
				idx = absPos + 3
				continue
			}
			if absPos >= 6 && strings.HasSuffix(s[:absPos], "ELSEIF") {
				idx = absPos + 3
				continue
			}
			if absPos >= 4 && strings.HasSuffix(s[:absPos], "ELSE") {
				idx = absPos + 3
				continue
			}
			count++
			idx = absPos + 3
		}
		return count
	}
	return strings.Count(s, substr)
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

// triggerBodyNeedsRoutineInterpreter returns true if the trigger body contains
// control flow statements (IF, WHILE, CASE, LOOP, REPEAT, DECLARE, BEGIN) that
// require the full routine body interpreter.
func triggerBodyNeedsRoutineInterpreter(body []string) bool {
	for _, stmt := range body {
		upper := strings.ToUpper(strings.TrimSpace(stmt))
		for _, kw := range []string{"IF ", "WHILE ", "CASE ", "CASE\n", "LOOP", "REPEAT ", "DECLARE ", "BEGIN"} {
			if strings.HasPrefix(upper, kw) || upper == strings.TrimSpace(kw) {
				return true
			}
		}
	}
	return false
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
		// If the trigger body contains control flow, use the routine interpreter
		if triggerBodyNeedsRoutineInterpreter(tr.Body) {
			if err := e.fireTriggerWithRoutineInterpreter(tr, timing, newRow, oldRow); err != nil {
				return err
			}
			continue
		}

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

// fireTriggerWithRoutineInterpreter executes a trigger body through the routine
// body interpreter, enabling support for IF/THEN, WHILE, CASE, LOOP, REPEAT,
// DECLARE, and other control flow statements.
func (e *Executor) fireTriggerWithRoutineInterpreter(tr *catalog.TriggerDef, timing string, newRow, oldRow storage.Row) error {
	ctx := &routineContext{
		localVars:     make(map[string]interface{}),
		cursors:       make(map[string]*cursorState),
		cursorDefs:    make(map[string]string),
		triggerNewRow: newRow,
		triggerOldRow: oldRow,
		triggerTiming: timing,
	}

	// Populate local variables for NEW.col and OLD.col so the routine
	// interpreter can resolve them in SET statements and expressions.
	// For SQL statements (INSERT, etc.), resolveNewOldRefs is used instead
	// for proper SQL quoting of string values.
	if newRow != nil {
		for col, val := range newRow {
			ctx.localVars["NEW."+col] = val
		}
	}
	if oldRow != nil {
		for col, val := range oldRow {
			ctx.localVars["OLD."+col] = val
		}
	}

	_, err := e.execRoutineBodyWithContext(tr.Body, ctx)
	if err != nil {
		return err
	}

	// For BEFORE triggers, copy any modified NEW.col values back to newRow
	if timing == "BEFORE" && newRow != nil {
		for k, v := range ctx.localVars {
			if strings.HasPrefix(k, "NEW.") {
				colName := k[4:]
				newRow[colName] = v
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
	stmt, err := e.parser().Parse(selectSQL)
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

	// Extract body: find BEGIN...END or single-statement body
	_ = upper
	afterParams := rest[paramEnd+1:]
	var bodyStmts []string
	beginIdx := strings.Index(strings.ToUpper(afterParams), "BEGIN")
	if beginIdx >= 0 {
		bodyStr := strings.TrimSpace(afterParams[beginIdx+len("BEGIN"):])
		if strings.HasSuffix(strings.ToUpper(strings.TrimSpace(bodyStr)), "END") {
			bodyStr = strings.TrimSpace(bodyStr[:len(bodyStr)-len("END")])
		}
		bodyStmts = splitTriggerBody(bodyStr)
	} else {
		// Single-statement procedure (no BEGIN...END)
		// Skip optional characteristics (LANGUAGE SQL, DETERMINISTIC, etc.)
		bodyStr := strings.TrimSpace(afterParams)
		upperBody := strings.ToUpper(bodyStr)
		for {
			trimmedBody := strings.TrimSpace(bodyStr)
			upperBody = strings.ToUpper(trimmedBody)
			if strings.HasPrefix(upperBody, "LANGUAGE ") {
				if idx := strings.Index(trimmedBody[9:], " "); idx >= 0 {
					bodyStr = trimmedBody[9+idx:]
				} else {
					break
				}
			} else if strings.HasPrefix(upperBody, "NOT DETERMINISTIC") {
				bodyStr = trimmedBody[17:]
			} else if strings.HasPrefix(upperBody, "DETERMINISTIC") {
				bodyStr = trimmedBody[13:]
			} else if strings.HasPrefix(upperBody, "CONTAINS SQL") {
				bodyStr = trimmedBody[12:]
			} else if strings.HasPrefix(upperBody, "NO SQL") {
				bodyStr = trimmedBody[6:]
			} else if strings.HasPrefix(upperBody, "READS SQL DATA") {
				bodyStr = trimmedBody[14:]
			} else if strings.HasPrefix(upperBody, "MODIFIES SQL DATA") {
				bodyStr = trimmedBody[17:]
			} else if strings.HasPrefix(upperBody, "SQL SECURITY DEFINER") {
				bodyStr = trimmedBody[20:]
			} else if strings.HasPrefix(upperBody, "SQL SECURITY INVOKER") {
				bodyStr = trimmedBody[20:]
			} else if strings.HasPrefix(upperBody, "COMMENT ") {
				// Skip COMMENT 'string'
				rest2 := trimmedBody[8:]
				if len(rest2) > 0 && (rest2[0] == '\'' || rest2[0] == '"') {
					q := rest2[0]
					end := strings.IndexByte(rest2[1:], q)
					if end >= 0 {
						bodyStr = rest2[end+2:]
					} else {
						break
					}
				} else {
					break
				}
			} else {
				break
			}
		}
		bodyStr = strings.TrimSpace(bodyStr)
		bodyStr = strings.TrimSuffix(bodyStr, ";")
		bodyStr = strings.TrimSpace(bodyStr)
		if bodyStr == "" {
			return nil, fmt.Errorf("invalid CREATE PROCEDURE syntax: missing body")
		}
		bodyStmts = []string{bodyStr}
	}

	procDef := &catalog.ProcedureDef{
		Name:        procName,
		Params:      params,
		Body:        bodyStmts,
		OriginalSQL: query,
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

	// Handle qualified name (schema.procedure)
	targetDB := db
	dbName := e.CurrentDB
	if dotIdx := strings.Index(name, "."); dotIdx >= 0 {
		dbName = strings.Trim(name[:dotIdx], "`")
		name = strings.Trim(name[dotIdx+1:], "`")
		targetDB2, err2 := e.Catalog.GetDatabase(dbName)
		if err2 != nil {
			if ifExists {
				return &Result{}, nil
			}
			return nil, mysqlError(1305, "42000", fmt.Sprintf("PROCEDURE %s.%s does not exist", dbName, name))
		}
		targetDB = targetDB2
	}

	if targetDB.GetProcedure(name) == nil && !ifExists {
		return nil, mysqlError(1305, "42000", fmt.Sprintf("PROCEDURE %s.%s does not exist", dbName, name))
	}
	targetDB.DropProcedure(name)
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

	// Handle qualified procedure name (db.proc_name)
	if strings.Contains(procName, ".") {
		parts := strings.SplitN(procName, ".", 2)
		dbName := strings.Trim(parts[0], "`")
		procName = strings.Trim(parts[1], "`")
		return e.callProcedureByNameInDB(dbName, procName, argStrs)
	}

	return e.callProcedureByName(procName, argStrs)
}

// execCallProcFromAST handles CALL parsed by vitess.
func (e *Executor) execCallProcFromAST(stmt *sqlparser.CallProc) (*Result, error) {
	procName := stmt.Name.Name.String()
	procName = strings.Trim(procName, "`")

	var argStrs []string
	for _, arg := range stmt.Params {
		argStrs = append(argStrs, sqlparser.String(arg))
	}

	// Handle qualified procedure name (db.proc_name)
	qualifier := stmt.Name.Qualifier.String()
	if qualifier != "" {
		return e.callProcedureByNameInDB(qualifier, procName, argStrs)
	}

	return e.callProcedureByName(procName, argStrs)
}

// callProcedureByNameInDB looks up and executes a stored procedure in a specific database.
func (e *Executor) callProcedureByNameInDB(dbName string, procName string, argStrs []string) (*Result, error) {
	db, err := e.Catalog.GetDatabase(dbName)
	if err != nil {
		// Silently accept calls to procedures in unknown databases for compatibility
		return &Result{}, nil
	}

	proc := db.GetProcedure(procName)
	if proc == nil {
		// Silently accept calls to non-existent procedures for compatibility
		return &Result{}, nil
	}

	// Build parameter mapping: bind IN params, track OUT params
	paramVars := make(map[string]interface{})
	for i, param := range proc.Params {
		if i < len(argStrs) {
			argVal := strings.TrimSpace(argStrs[i])
			if strings.HasPrefix(argVal, "@") {
				if param.Mode == "IN" || param.Mode == "INOUT" {
					paramVars[param.Name] = argVal
				}
			} else {
				if n, err := strconv.ParseInt(argVal, 10, 64); err == nil {
					paramVars[param.Name] = n
				} else {
					paramVars[param.Name] = strings.Trim(argVal, "'\"")
				}
			}
		}
	}

	bodyResult, err := e.execRoutineBody(proc.Body, paramVars)
	if err != nil {
		return nil, err
	}
	// If the routine body produced a result set (e.g. from EXIT HANDLER), return it.
	if bodyResult != nil {
		if r, ok := bodyResult.(*Result); ok && r != nil && r.IsResultSet {
			return r, nil
		}
	}

	return &Result{}, nil
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

	// Build parameter mapping: bind IN/INOUT params, track OUT param user-variable targets.
	paramVars := make(map[string]interface{})
	// outTargets maps parameter name -> caller's user variable name (without '@')
	outTargets := make(map[string]string)
	for i, param := range proc.Params {
		if i < len(argStrs) {
			argVal := strings.TrimSpace(argStrs[i])
			if strings.HasPrefix(argVal, "@") {
				userVar := strings.TrimPrefix(argVal, "@")
				// Track OUT/INOUT targets for writeback
				if param.Mode == "OUT" || param.Mode == "INOUT" {
					outTargets[param.Name] = userVar
				}
				// Resolve the user variable value for IN/INOUT params
				if param.Mode == "IN" || param.Mode == "INOUT" {
					if val, ok := e.userVars[userVar]; ok {
						paramVars[param.Name] = val
					} else {
						paramVars[param.Name] = nil
					}
				}
				// For pure OUT params, initialize to nil
				if param.Mode == "OUT" {
					paramVars[param.Name] = nil
				}
			} else {
				// Literal value - only valid for IN params
				if param.Mode == "IN" {
					if n, err := strconv.ParseInt(argVal, 10, 64); err == nil {
						paramVars[param.Name] = n
					} else {
						paramVars[param.Name] = strings.Trim(argVal, "'\"")
					}
				}
				// Literals for OUT/INOUT are silently ignored (no writeback target)
			}
		}
	}

	// Execute body using the routine executor with cursor support.
	// We create the context directly so we can read back final variable values.
	ctx := &routineContext{
		localVars:  make(map[string]interface{}),
		cursors:    make(map[string]*cursorState),
		cursorDefs: make(map[string]string),
	}
	for k, v := range paramVars {
		ctx.localVars[k] = v
	}
	bodyResult, err := e.execRoutineBodyWithContext(proc.Body, ctx)
	if err != nil {
		return nil, err
	}

	// Write back OUT/INOUT parameter values to caller's user variables
	for paramName, userVar := range outTargets {
		val := ctx.localVars[paramName]
		if e.userVars == nil {
			e.userVars = make(map[string]interface{})
		}
		e.userVars[userVar] = val
	}

	// If the routine body produced a result set (e.g. from EXIT HANDLER or SELECT), return it.
	if bodyResult != nil {
		if r, ok := bodyResult.(*Result); ok && r != nil && r.IsResultSet {
			return r, nil
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
	// MySQL parse errors around unquoted time-like tokens (e.g. 11:11:11)
	// typically show the snippet starting from ':'.
	if idx := strings.IndexByte(s, ':'); idx > 0 {
		prev := s[idx-1]
		if prev >= '0' && prev <= '9' {
			s = s[idx:]
		}
	}
	if len(s) > 80 {
		return s[:80]
	}
	return s
}

// execCreateFunction handles CREATE FUNCTION name(params) RETURNS type BEGIN...END
func (e *Executor) execCreateFunction(query string) (*Result, error) {
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", e.CurrentDB))
	}

	rest := strings.TrimSpace(query[len("CREATE FUNCTION "):])

	// Extract function name (up to first '(')
	parenIdx := strings.Index(rest, "(")
	if parenIdx < 0 {
		return nil, fmt.Errorf("invalid CREATE FUNCTION syntax: missing parameter list")
	}
	funcName := strings.TrimSpace(rest[:parenIdx])
	funcName = strings.Trim(funcName, "`")

	// Handle schema-qualified function names (e.g. test.f or `test`.`f`)
	if dotIdx := strings.Index(funcName, "."); dotIdx >= 0 {
		schemaName := strings.Trim(funcName[:dotIdx], "`")
		funcName = strings.Trim(funcName[dotIdx+1:], "`")
		db, err = e.Catalog.GetDatabase(schemaName)
		if err != nil {
			return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", schemaName))
		}
	}

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

	// Extract RETURNS type and body
	afterParams := rest[paramEnd+1:]
	upperAfter := strings.ToUpper(afterParams)

	// Find RETURNS keyword
	returnsIdx := strings.Index(upperAfter, "RETURNS ")
	returnType := ""
	if returnsIdx >= 0 {
		afterReturns := strings.TrimSpace(afterParams[returnsIdx+len("RETURNS "):])
		// Return type ends at BEGIN/RETURN or at a characteristic keyword
		beginIdx := strings.Index(strings.ToUpper(afterReturns), "BEGIN")
		returnIdx := strings.Index(strings.ToUpper(afterReturns), "RETURN ")
		endIdx := beginIdx
		if endIdx < 0 || (returnIdx >= 0 && returnIdx < endIdx) {
			endIdx = returnIdx
		}
		if endIdx < 0 {
			endIdx = len(afterReturns)
		}
		returnType = strings.TrimSpace(afterReturns[:endIdx])
		// Strip optional characteristics like CONTAINS SQL, NO SQL, READS SQL DATA, etc.
		for _, kw := range []string{"DETERMINISTIC", "NOT DETERMINISTIC", "CONTAINS SQL", "NO SQL", "READS SQL DATA", "MODIFIES SQL DATA", "SQL SECURITY DEFINER", "SQL SECURITY INVOKER"} {
			returnType = strings.TrimSuffix(strings.TrimSpace(returnType), kw)
		}
		returnType = strings.TrimSpace(returnType)
	}

	// Extract body: BEGIN...END or single RETURN expression.
	var bodyStmts []string
	beginIdx := strings.Index(strings.ToUpper(afterParams), "BEGIN")
	if beginIdx >= 0 {
		bodyStr := strings.TrimSpace(afterParams[beginIdx+len("BEGIN"):])
		if strings.HasSuffix(strings.ToUpper(strings.TrimSpace(bodyStr)), "END") {
			bodyStr = strings.TrimSpace(bodyStr[:len(bodyStr)-len("END")])
		}
		bodyStmts = splitTriggerBody(bodyStr)
	} else {
		returnIdx := strings.Index(strings.ToUpper(afterParams), "RETURN ")
		if returnIdx < 0 {
			return nil, fmt.Errorf("invalid CREATE FUNCTION syntax: missing RETURN")
		}
		returnExpr := strings.TrimSpace(afterParams[returnIdx:])
		returnExpr = strings.TrimSuffix(returnExpr, ";")
		bodyStmts = []string{returnExpr}
	}

	funcDef := &catalog.FunctionDef{
		Name:        funcName,
		Params:      params,
		ReturnType:  returnType,
		Body:        bodyStmts,
		OriginalSQL: query,
	}
	db.CreateFunction(funcDef)

	// Check if function name collides with a native function (Warning 1585)
	lowerName := strings.ToLower(funcName)
	if nativeFunctions[lowerName] {
		e.addWarning("Note", 1585, fmt.Sprintf("This function '%s' has the same name as a native function", lowerName))
	}

	return &Result{}, nil
}

// nativeFunctions lists MySQL native function names for collision detection (Warning 1585).
var nativeFunctions = map[string]bool{
	"ps_current_thread_id": true,
	"ps_thread_id":         true,
	"format_bytes":         true,
	"format_pico_time":     true,
}

// execDropFunction handles DROP FUNCTION [IF EXISTS] name
func (e *Executor) execDropFunction(query string) (*Result, error) {
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", e.CurrentDB))
	}

	rest := strings.TrimSpace(query[len("DROP FUNCTION"):])
	ifExists := false
	restUpper := strings.ToUpper(strings.TrimSpace(rest))
	if strings.HasPrefix(restUpper, "IF EXISTS") {
		ifExists = true
		rest = strings.TrimSpace(rest[len("IF EXISTS"):])
		rest = strings.TrimSpace(rest)
	}
	name := strings.TrimRight(strings.TrimSpace(rest), ";")
	name = strings.Trim(name, "`")

	// Handle schema-qualified function names (e.g. test.f or `test`.`f`)
	if dotIdx := strings.Index(name, "."); dotIdx >= 0 {
		schemaName := strings.Trim(name[:dotIdx], "`")
		name = strings.Trim(name[dotIdx+1:], "`")
		db, err = e.Catalog.GetDatabase(schemaName)
		if err != nil {
			if ifExists {
				return &Result{}, nil
			}
			return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", schemaName))
		}
	}

	if db.GetFunction(name) == nil && !ifExists {
		return nil, mysqlError(1305, "42000", fmt.Sprintf("FUNCTION %s.%s does not exist", e.CurrentDB, name))
	}
	db.DropFunction(name)
	return &Result{}, nil
}

// cursorState holds the state of an open cursor during procedure/function execution.
type cursorState struct {
	rows    [][]interface{}
	columns []string
	pos     int
}

// callUserDefinedFunction looks up a user-defined function in the catalog and executes it.
// qualifier is the optional schema qualifier (e.g. "test" in "test.f()").
func (e *Executor) callUserDefinedFunction(name string, argExprs []sqlparser.Expr, row *storage.Row, qualifier ...string) (interface{}, error) {
	e.routineDepth++
	defer func() { e.routineDepth-- }()
	if e.routineDepth > 256 {
		return nil, fmt.Errorf("Error 1456 (HY000): Recursive stored functions and triggers are not allowed")
	}
	if e.Catalog == nil {
		return nil, fmt.Errorf("function not found: %s", name)
	}
	dbName := e.CurrentDB
	if len(qualifier) > 0 && qualifier[0] != "" {
		dbName = qualifier[0]
	}
	if dbName == "" {
		dbName = "test"
	}
	db, err := e.Catalog.GetDatabase(dbName)
	if err != nil {
		return nil, fmt.Errorf("no database")
	}

	fn := db.GetFunction(name)
	if fn == nil {
		return nil, fmt.Errorf("function not found: %s", name)
	}

	// Evaluate arguments
	paramVars := make(map[string]interface{})
	for i, param := range fn.Params {
		if i < len(argExprs) {
			var val interface{}
			var evalErr error
			if row != nil {
				val, evalErr = e.evalRowExpr(argExprs[i], *row)
			} else {
				val, evalErr = e.evalExpr(argExprs[i])
			}
			if evalErr != nil {
				return nil, evalErr
			}
			paramVars[param.Name] = val
		}
	}

	// Execute function body with local variables, cursors, and handlers
	return e.execRoutineBody(fn.Body, paramVars)
}

// leaveError is a sentinel error used to implement LEAVE label in stored routines.
type leaveError struct {
	label string
}

func (e *leaveError) Error() string { return "LEAVE " + e.label }

// iterateError is a sentinel error used to implement ITERATE label in stored routines.
type iterateError struct {
	label string
}

func (e *iterateError) Error() string { return "ITERATE " + e.label }

// signalError represents a SIGNAL/RESIGNAL raised inside a stored routine.
type signalError struct {
	sqlState    string
	mysqlErrno  int
	messageText string
}

func (e *signalError) Error() string {
	code := e.mysqlErrno
	if code == 0 {
		code = 1644 // ER_SIGNAL_EXCEPTION
	}
	msg := e.messageText
	if msg == "" {
		msg = "Unhandled user-defined exception"
	}
	return fmt.Sprintf("ERROR %d (%s): %s", code, e.sqlState, msg)
}

// exitHandlerError wraps the exit-handler action: after running the handler body
// the current BEGIN...END block must terminate.
type exitHandlerError struct{}

func (e *exitHandlerError) Error() string { return "EXIT HANDLER" }

// handlerDef describes a DECLARE HANDLER definition.
type handlerDef struct {
	handlerType string   // "CONTINUE" or "EXIT"
	conditions  []string // condition keys, e.g. "NOT FOUND", "SQLEXCEPTION", "SQLWARNING", "02000"
	body        string   // handler body (SQL to execute)
}

// routineContext holds shared state for a stored routine execution.
type routineContext struct {
	localVars          map[string]interface{}
	cursors            map[string]*cursorState
	cursorDefs         map[string]string
	notFoundHandlerVar string
	done               bool
	handlers           []handlerDef
	currentSignal      *signalError // the signal currently being handled (for bare RESIGNAL)
	triggerNewRow      storage.Row  // non-nil when executing a trigger body (for NEW.col resolution)
	triggerOldRow      storage.Row  // non-nil when executing a trigger body (for OLD.col resolution)
	triggerTiming      string       // "BEFORE" or "AFTER" for trigger execution
	handlerResult      *Result      // result set produced by EXIT HANDLER body (to return from CALL)
}

// childContext creates a child routineContext that shares state with the parent
// but can have its own handler scope. Trigger context is always propagated.
func (ctx *routineContext) childContext() *routineContext {
	child := &routineContext{
		localVars:          ctx.localVars,
		cursors:            ctx.cursors,
		cursorDefs:         ctx.cursorDefs,
		notFoundHandlerVar: ctx.notFoundHandlerVar,
		done:               ctx.done,
		handlers:           ctx.handlers,
		triggerNewRow:       ctx.triggerNewRow,
		triggerOldRow:       ctx.triggerOldRow,
		triggerTiming:       ctx.triggerTiming,
	}
	return child
}

// execRoutineBody executes the body of a stored procedure or function, supporting
// DECLARE, SET, IF, WHILE, REPEAT, CURSOR, HANDLER, RETURN, and general SQL statements.
func (e *Executor) execRoutineBody(body []string, paramVars map[string]interface{}) (interface{}, error) {
	ctx := &routineContext{
		localVars:  make(map[string]interface{}),
		cursors:    make(map[string]*cursorState),
		cursorDefs: make(map[string]string),
	}
	for k, v := range paramVars {
		ctx.localVars[k] = v
	}
	return e.execRoutineBodyWithContext(body, ctx)
}

// execRoutineBodyWithContext executes routine body statements with shared context.
func (e *Executor) execRoutineBodyWithContext(body []string, ctx *routineContext) (interface{}, error) {
	localVars := ctx.localVars
	cursors := ctx.cursors
	cursorDefs := ctx.cursorDefs
	notFoundHandlerVar := ctx.notFoundHandlerVar
	done := ctx.done

	var returnVal interface{}

	for i := 0; i < len(body); i++ {
		stmtStr := strings.TrimSpace(body[i])
		stmtUpper := strings.ToUpper(stmtStr)

		if stmtStr == "" {
			continue
		}

		// Handle DECLARE
		if strings.HasPrefix(stmtUpper, "DECLARE") {
			rest := strings.TrimSpace(stmtStr[len("DECLARE"):])
			restUpper := strings.ToUpper(rest)

			// DECLARE {CONTINUE|EXIT} HANDLER FOR {condition} {body}
			if strings.HasPrefix(restUpper, "CONTINUE HANDLER") || strings.HasPrefix(restUpper, "EXIT HANDLER") {
				handlerType := "CONTINUE"
				afterType := rest[len("CONTINUE HANDLER"):]
				if strings.HasPrefix(restUpper, "EXIT HANDLER") {
					handlerType = "EXIT"
					afterType = rest[len("EXIT HANDLER"):]
				}
				afterType = strings.TrimSpace(afterType)
				afterTypeUpper := strings.ToUpper(afterType)

				// Parse FOR clause
				if strings.HasPrefix(afterTypeUpper, "FOR ") {
					afterFor := strings.TrimSpace(afterType[4:])
					afterForUpper := strings.ToUpper(afterFor)

					// Parse conditions and body
					var conditions []string
					var handlerBody string

					if strings.HasPrefix(afterForUpper, "NOT FOUND") {
						conditions = append(conditions, "NOT FOUND")
						handlerBody = strings.TrimSpace(afterFor[len("NOT FOUND"):])
					} else if strings.HasPrefix(afterForUpper, "SQLEXCEPTION") {
						conditions = append(conditions, "SQLEXCEPTION")
						handlerBody = strings.TrimSpace(afterFor[len("SQLEXCEPTION"):])
					} else if strings.HasPrefix(afterForUpper, "SQLWARNING") {
						conditions = append(conditions, "SQLWARNING")
						handlerBody = strings.TrimSpace(afterFor[len("SQLWARNING"):])
					} else if strings.HasPrefix(afterForUpper, "SQLSTATE") {
						// SQLSTATE 'value'
						stateRest := strings.TrimSpace(afterFor[len("SQLSTATE"):])
						stateRestUpper := strings.ToUpper(stateRest)
						if strings.HasPrefix(stateRestUpper, "VALUE ") {
							stateRest = strings.TrimSpace(stateRest[len("VALUE "):])
						}
						// Extract quoted state
						if len(stateRest) > 0 && (stateRest[0] == '\'' || stateRest[0] == '"') {
							q := stateRest[0]
							end := strings.IndexByte(stateRest[1:], q)
							if end >= 0 {
								sqlState := stateRest[1 : end+1]
								conditions = append(conditions, sqlState)
								handlerBody = strings.TrimSpace(stateRest[end+2:])
							}
						}
					} else {
						// MySQL error number: DECLARE HANDLER FOR 1062 ...
						parts := strings.Fields(afterFor)
						if len(parts) > 0 {
							conditions = append(conditions, parts[0])
							handlerBody = strings.TrimSpace(afterFor[len(parts[0]):])
						}
					}

					// For backward compat: if handler body is SET var = val and condition is NOT FOUND,
					// also set notFoundHandlerVar for the old cursor path
					if len(conditions) > 0 {
						hDef := handlerDef{
							handlerType: handlerType,
							conditions:  conditions,
							body:        handlerBody,
						}
						ctx.handlers = append(ctx.handlers, hDef)

						// Legacy NOT FOUND handler variable tracking
						for _, c := range conditions {
							if c == "NOT FOUND" || c == "02000" {
								setIdx := strings.Index(strings.ToUpper(handlerBody), "SET ")
								if setIdx >= 0 {
									setPart := strings.TrimSpace(handlerBody[setIdx+4:])
									eqIdx := strings.Index(setPart, "=")
									if eqIdx >= 0 {
										varName := strings.TrimSpace(setPart[:eqIdx])
										notFoundHandlerVar = varName
										ctx.notFoundHandlerVar = varName
									}
								}
							}
						}
					}
				}
				continue
			}

			// DECLARE cursor_name CURSOR FOR select_stmt
			if strings.Contains(restUpper, " CURSOR FOR ") {
				parts := strings.SplitN(rest, " ", 2)
				cursorName := strings.TrimSpace(parts[0])
				cursorForIdx := strings.Index(restUpper, "CURSOR FOR ")
				selectSQL := strings.TrimSpace(rest[cursorForIdx+len("CURSOR FOR "):])
				cursorDefs[strings.ToLower(cursorName)] = selectSQL
				continue
			}

			// DECLARE var1[,var2,...] TYPE [DEFAULT val]
			// Parse variable declarations
			declParts := strings.Fields(rest)
			if len(declParts) >= 2 {
				// Collect variable names (comma-separated) before the type keyword
				var varNames []string
				typeIdx := 0
				for j, p := range declParts {
					// Handle comma-separated variable names (e.g., "b,c")
					subNames := strings.Split(strings.TrimRight(p, ","), ",")
					isType := false
					for _, name := range subNames {
						name = strings.TrimSpace(name)
						if name == "" {
							continue
						}
						nameUpper := strings.ToUpper(name)
						if nameUpper == "INT" || nameUpper == "INTEGER" || nameUpper == "BIGINT" ||
							nameUpper == "SMALLINT" || nameUpper == "TINYINT" || nameUpper == "MEDIUMINT" ||
							nameUpper == "CHAR" || nameUpper == "VARCHAR" || nameUpper == "TEXT" ||
							nameUpper == "DECIMAL" || nameUpper == "FLOAT" || nameUpper == "DOUBLE" ||
							nameUpper == "DATE" || nameUpper == "DATETIME" || nameUpper == "TIMESTAMP" ||
							nameUpper == "BOOLEAN" || nameUpper == "BOOL" || nameUpper == "NUMERIC" ||
							nameUpper == "BLOB" || nameUpper == "LONGTEXT" || nameUpper == "MEDIUMTEXT" ||
							nameUpper == "TINYTEXT" || nameUpper == "LONGBLOB" || nameUpper == "MEDIUMBLOB" ||
							nameUpper == "TINYBLOB" || nameUpper == "BINARY" || nameUpper == "VARBINARY" ||
							nameUpper == "ENUM" || nameUpper == "JSON" || nameUpper == "BIT" ||
							nameUpper == "YEAR" || nameUpper == "TIME" ||
							nameUpper == "CONDITION" || // DECLARE x CONDITION FOR ...
							strings.HasPrefix(nameUpper, "CHAR(") || strings.HasPrefix(nameUpper, "VARCHAR(") ||
							strings.HasPrefix(nameUpper, "DECIMAL(") || strings.HasPrefix(nameUpper, "NUMERIC(") ||
							strings.HasPrefix(nameUpper, "ENUM(") || strings.HasPrefix(nameUpper, "BIT(") ||
							strings.HasPrefix(nameUpper, "BINARY(") || strings.HasPrefix(nameUpper, "VARBINARY(") {
							isType = true
							break
						}
					}
					if isType {
						typeIdx = j
						break
					}
					for _, name := range subNames {
						name = strings.TrimSpace(name)
						if name != "" {
							varNames = append(varNames, name)
						}
					}
				}
				// Check if this is a CONDITION declaration (DECLARE x CONDITION FOR ...)
				if typeIdx < len(declParts) && strings.ToUpper(declParts[typeIdx]) == "CONDITION" {
					// DECLARE condition_name CONDITION FOR SQLSTATE VALUE 'xxxxx'
					// We just register it as a no-op for now
					continue
				}

				// Determine default value based on type
				var defaultVal interface{}
				typeName := ""
				if typeIdx < len(declParts) {
					typeName = strings.ToUpper(declParts[typeIdx])
				}
				// String types default to empty string, numeric to 0
				switch {
				case typeName == "CHAR" || typeName == "VARCHAR" || typeName == "TEXT" ||
					typeName == "LONGTEXT" || typeName == "MEDIUMTEXT" || typeName == "TINYTEXT" ||
					typeName == "BLOB" || typeName == "LONGBLOB" || typeName == "MEDIUMBLOB" ||
					typeName == "TINYBLOB" || typeName == "JSON" || typeName == "ENUM" ||
					strings.HasPrefix(typeName, "CHAR(") || strings.HasPrefix(typeName, "VARCHAR(") ||
					strings.HasPrefix(typeName, "ENUM("):
					defaultVal = ""
				case typeName == "DATE" || typeName == "DATETIME" || typeName == "TIMESTAMP" ||
					typeName == "TIME" || typeName == "YEAR":
					defaultVal = nil
				default:
					defaultVal = int64(0)
				}
				// Find DEFAULT value override
				for j := typeIdx; j < len(declParts); j++ {
					if strings.ToUpper(declParts[j]) == "DEFAULT" && j+1 < len(declParts) {
						defStr := strings.Join(declParts[j+1:], " ")
						defStr = strings.TrimRight(defStr, ";")
						if strings.ToUpper(defStr) == "NULL" {
							defaultVal = nil
						} else if n, err := strconv.ParseInt(defStr, 10, 64); err == nil {
							defaultVal = n
						} else if f, err := strconv.ParseFloat(defStr, 64); err == nil {
							defaultVal = f
						} else if strings.ToUpper(defStr) == "TRUE" {
							defaultVal = int64(1)
						} else if strings.ToUpper(defStr) == "FALSE" {
							defaultVal = int64(0)
						} else {
							defaultVal = strings.Trim(defStr, "'\"")
						}
						break
					}
				}
				for _, vn := range varNames {
					localVars[vn] = defaultVal
				}
			}
			continue
		}

		// Handle OPEN cursor_name
		if strings.HasPrefix(stmtUpper, "OPEN ") {
			cursorName := strings.ToLower(strings.TrimSpace(stmtStr[len("OPEN "):]))
			selectSQL, ok := cursorDefs[cursorName]
			if !ok {
				return nil, fmt.Errorf("cursor '%s' is not declared", cursorName)
			}
			// Substitute local variables in the SELECT query
			resolvedSQL := e.substituteLocalVars(selectSQL, localVars)
			result, err := e.Execute(resolvedSQL)
			if err != nil {
				return nil, err
			}
			cs := &cursorState{
				columns: result.Columns,
				pos:     0,
			}
			for _, r := range result.Rows {
				cs.rows = append(cs.rows, r)
			}
			cursors[cursorName] = cs
			continue
		}

		// Handle CLOSE cursor_name
		if strings.HasPrefix(stmtUpper, "CLOSE ") {
			cursorName := strings.ToLower(strings.TrimSpace(stmtStr[len("CLOSE "):]))
			delete(cursors, cursorName)
			continue
		}

		// Handle FETCH cursor_name INTO var1, var2, ...
		if strings.HasPrefix(stmtUpper, "FETCH ") {
			rest := strings.TrimSpace(stmtStr[len("FETCH "):])
			restUpper := strings.ToUpper(rest)
			// Skip optional NEXT FROM
			if strings.HasPrefix(restUpper, "NEXT FROM ") {
				rest = strings.TrimSpace(rest[len("NEXT FROM "):])
			}
			intoIdx := strings.Index(strings.ToUpper(rest), " INTO ")
			if intoIdx < 0 {
				continue
			}
			cursorName := strings.ToLower(strings.TrimSpace(rest[:intoIdx]))
			varsPart := strings.TrimSpace(rest[intoIdx+len(" INTO "):])
			varNames := strings.Split(varsPart, ",")
			for j := range varNames {
				varNames[j] = strings.TrimSpace(varNames[j])
			}

			cs, ok := cursors[cursorName]
			if !ok {
				// Cursor not open - trigger NOT FOUND
				if notFoundHandlerVar != "" {
					localVars[notFoundHandlerVar] = int64(1)
					done = true
				}
				continue
			}
			if cs.pos >= len(cs.rows) {
				// No more rows - trigger NOT FOUND
				if notFoundHandlerVar != "" {
					localVars[notFoundHandlerVar] = int64(1)
					done = true
				}
				continue
			}
			row := cs.rows[cs.pos]
			cs.pos++
			for j, vn := range varNames {
				if j < len(row) {
					localVars[vn] = row[j]
				}
			}
			continue
		}

		// Handle RETURN value
		if strings.HasPrefix(stmtUpper, "RETURN ") {
			exprStr := strings.TrimSpace(stmtStr[len("RETURN "):])
			exprStr = strings.TrimRight(exprStr, ";")
			// Try to evaluate as a local variable first
			if val, ok := localVars[exprStr]; ok {
				return val, nil
			}
			// Try to evaluate as an expression
			val, err := e.evaluateExprWithVars(exprStr, localVars)
			if err != nil {
				// Fallback for RETURN (SELECT ...): evaluate the inner scalar query.
				resolvedExpr := strings.TrimSpace(exprStr)
				if strings.HasPrefix(resolvedExpr, "(") && strings.HasSuffix(resolvedExpr, ")") {
					resolvedExpr = strings.TrimSpace(resolvedExpr[1 : len(resolvedExpr)-1])
				}
				toSQLLiteral := func(v interface{}) string {
					if v == nil {
						return "NULL"
					}
					switch x := v.(type) {
					case string:
						return "'" + strings.ReplaceAll(x, "'", "''") + "'"
					case bool:
						if x {
							return "1"
						}
						return "0"
					default:
						return fmt.Sprintf("%v", x)
					}
				}
				for varName, varVal := range localVars {
					re := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(varName) + `\b`)
					resolvedExpr = re.ReplaceAllString(resolvedExpr, toSQLLiteral(varVal))
				}
				res, qerr := e.Execute(resolvedExpr)
				if qerr != nil || res == nil || len(res.Rows) == 0 || len(res.Rows[0]) == 0 {
					return nil, err
				}
				return res.Rows[0][0], nil
			}
			return val, nil
		}

		// Handle SET statements with local variable substitution
		if strings.HasPrefix(stmtUpper, "SET ") {
			setPart := strings.TrimSpace(stmtStr[4:])
			eqIdx := strings.Index(setPart, "=")
			if eqIdx >= 0 {
				varName := strings.TrimSpace(setPart[:eqIdx])
				valStr := strings.TrimSpace(setPart[eqIdx+1:])
				// Evaluate expression
				val, err := e.evaluateExprWithVars(valStr, localVars)
				if err != nil {
					// Fall back to Execute
					resolvedSQL := e.substituteLocalVars(stmtStr, localVars)
					e.Execute(resolvedSQL) //nolint:errcheck
				} else {
					localVars[varName] = val
				}
			} else {
				resolvedSQL := e.substituteLocalVars(stmtStr, localVars)
				e.Execute(resolvedSQL) //nolint:errcheck
			}
			continue
		}

		// Handle IF...THEN...ELSEIF...ELSE...END IF (may span multiple body statements)
		if strings.HasPrefix(stmtUpper, "IF ") {
			// Collect the full IF block, tracking nesting
			ifBlock := stmtStr
			ifDepth := countOccurrences(strings.ToUpper(ifBlock), "IF ") - countOccurrences(strings.ToUpper(ifBlock), "END IF")
			for ifDepth > 0 && i+1 < len(body) {
				i++
				ifBlock += ";\n" + body[i]
				ifDepth += countOccurrences(strings.ToUpper(body[i]), "IF ") - countOccurrences(strings.ToUpper(body[i]), "END IF")
			}
			_, retVal, err := e.execIfBlockCtx(ifBlock, ctx)
			if err != nil {
				return nil, err
			}
			if retVal != nil {
				return retVal, nil
			}
			continue
		}

		// Handle REPEAT...UNTIL...END REPEAT
		if strings.HasPrefix(stmtUpper, "REPEAT") {
			// Collect the full REPEAT block
			repeatBlock := stmtStr
			for !strings.HasSuffix(strings.ToUpper(strings.TrimSpace(repeatBlock)), "END REPEAT") && i+1 < len(body) {
				i++
				repeatBlock += ";\n" + body[i]
			}
			retVal, err := e.execRepeatBlockCtx(repeatBlock, ctx)
			if err != nil {
				return nil, err
			}
			if retVal != nil {
				return retVal, nil
			}
			continue
		}

		// Handle WHILE...DO...END WHILE
		if strings.HasPrefix(stmtUpper, "WHILE ") {
			whileBlock := stmtStr
			for !strings.HasSuffix(strings.ToUpper(strings.TrimSpace(whileBlock)), "END WHILE") && i+1 < len(body) {
				i++
				whileBlock += ";\n" + body[i]
			}
			retVal, err := e.execWhileBlockCtx(whileBlock, ctx)
			if err != nil {
				return nil, err
			}
			if retVal != nil {
				return retVal, nil
			}
			continue
		}

		// Handle LEAVE label
		if strings.HasPrefix(stmtUpper, "LEAVE ") {
			label := strings.TrimSpace(stmtStr[len("LEAVE "):])
			label = strings.TrimRight(label, ";")
			return nil, &leaveError{label: label}
		}

		// Handle ITERATE label
		if strings.HasPrefix(stmtUpper, "ITERATE ") {
			label := strings.TrimSpace(stmtStr[len("ITERATE "):])
			label = strings.TrimRight(label, ";")
			return nil, &iterateError{label: label}
		}

		// Handle LOOP...END LOOP (with optional label)
		if strings.HasPrefix(stmtUpper, "LOOP") || (strings.Contains(stmtUpper, ":") && strings.Contains(stmtUpper, "LOOP")) {
			// Check for labeled loop: label: LOOP ... END LOOP [label]
			loopBlock := stmtStr
			loopLabel := ""
			startUpper := stmtUpper
			if colonIdx := strings.Index(startUpper, ":"); colonIdx >= 0 {
				possibleLabel := strings.TrimSpace(stmtStr[:colonIdx])
				afterColon := strings.TrimSpace(stmtStr[colonIdx+1:])
				if strings.HasPrefix(strings.ToUpper(afterColon), "LOOP") {
					loopLabel = possibleLabel
					loopBlock = afterColon
				}
			}
			// Collect the full LOOP block
			for !strings.HasSuffix(strings.ToUpper(strings.TrimSpace(loopBlock)), "END LOOP") &&
				!strings.HasSuffix(strings.ToUpper(strings.TrimSpace(loopBlock)), "END LOOP "+strings.ToUpper(loopLabel)) && i+1 < len(body) {
				i++
				loopBlock += ";\n" + body[i]
			}
			retVal, err := e.execLoopBlockCtx(loopBlock, loopLabel, ctx)
			if err != nil {
				return nil, err
			}
			if retVal != nil {
				return retVal, nil
			}
			continue
		}

		// Handle labeled WHILE: label: WHILE ... END WHILE
		if strings.Contains(stmtUpper, ":") && !strings.HasPrefix(stmtUpper, "WHILE ") {
			colonIdx := strings.Index(stmtStr, ":")
			if colonIdx >= 0 {
				afterColon := strings.TrimSpace(stmtStr[colonIdx+1:])
				afterColonUpper := strings.ToUpper(afterColon)
				if strings.HasPrefix(afterColonUpper, "WHILE ") {
					loopLabel := strings.TrimSpace(stmtStr[:colonIdx])
					whileBlock := afterColon
					for !strings.HasSuffix(strings.ToUpper(strings.TrimSpace(whileBlock)), "END WHILE") &&
						!strings.HasSuffix(strings.ToUpper(strings.TrimSpace(whileBlock)), "END WHILE "+strings.ToUpper(loopLabel)) && i+1 < len(body) {
						i++
						whileBlock += ";\n" + body[i]
					}
					retVal, err := e.execWhileBlockWithLabel(whileBlock, loopLabel, ctx)
					if err != nil {
						return nil, err
					}
					if retVal != nil {
						return retVal, nil
					}
					continue
				}
				if strings.HasPrefix(afterColonUpper, "REPEAT") {
					loopLabel := strings.TrimSpace(stmtStr[:colonIdx])
					repeatBlock := afterColon
					for !strings.HasSuffix(strings.ToUpper(strings.TrimSpace(repeatBlock)), "END REPEAT") &&
						!strings.HasSuffix(strings.ToUpper(strings.TrimSpace(repeatBlock)), "END REPEAT "+strings.ToUpper(loopLabel)) && i+1 < len(body) {
						i++
						repeatBlock += ";\n" + body[i]
					}
					retVal, err := e.execRepeatBlockWithLabel(repeatBlock, loopLabel, ctx)
					if err != nil {
						return nil, err
					}
					if retVal != nil {
						return retVal, nil
					}
					continue
				}
				if strings.HasPrefix(afterColonUpper, "LOOP") {
					loopLabel := strings.TrimSpace(stmtStr[:colonIdx])
					loopBlock := afterColon
					for !strings.HasSuffix(strings.ToUpper(strings.TrimSpace(loopBlock)), "END LOOP") &&
						!strings.HasSuffix(strings.ToUpper(strings.TrimSpace(loopBlock)), "END LOOP "+strings.ToUpper(loopLabel)) && i+1 < len(body) {
						i++
						loopBlock += ";\n" + body[i]
					}
					retVal, err := e.execLoopBlockCtx(loopBlock, loopLabel, ctx)
					if err != nil {
						return nil, err
					}
					if retVal != nil {
						return retVal, nil
					}
					continue
				}
				// Handle labeled BEGIN...END block
				if strings.HasPrefix(afterColonUpper, "BEGIN") {
					label := strings.TrimSpace(stmtStr[:colonIdx])
					beginBlock := afterColon
					beginDepth := 1
					for beginDepth > 0 && i+1 < len(body) {
						i++
						line := body[i]
						lineUpper := strings.ToUpper(strings.TrimSpace(line))
						if strings.HasPrefix(lineUpper, "BEGIN") {
							beginDepth++
						}
						if isStandaloneEnd(lineUpper) {
							beginDepth--
						}
						beginBlock += ";\n" + line
					}
					// Strip BEGIN ... END wrapper
					inner := strings.TrimSpace(beginBlock)
					if strings.HasPrefix(strings.ToUpper(inner), "BEGIN") {
						inner = strings.TrimSpace(inner[len("BEGIN"):])
					}
					innerUpper := strings.ToUpper(strings.TrimSpace(inner))
					if strings.HasSuffix(innerUpper, "END "+strings.ToUpper(label)) {
						inner = strings.TrimSpace(inner[:len(inner)-len("END ")-len(label)])
					} else if strings.HasSuffix(innerUpper, "END") {
						inner = strings.TrimSpace(inner[:len(inner)-len("END")])
					}
					inner = strings.TrimRight(inner, ";")
					stmts := splitTriggerBody(inner)
					retVal, err := e.execRoutineBodyWithContext(stmts, ctx)
					if err != nil {
						// Handle LEAVE for this labeled block
						var le *leaveError
						if errors.As(err, &le) && strings.EqualFold(le.label, label) {
							continue
						}
						return nil, err
					}
					if retVal != nil {
						return retVal, nil
					}
					continue
				}
			}
		}

		// Handle CASE statement (control flow, not expression)
		if strings.HasPrefix(stmtUpper, "CASE") && !strings.HasPrefix(stmtUpper, "CASE ") ||
			strings.HasPrefix(stmtUpper, "CASE ") {
			// Collect the full CASE block
			caseBlock := stmtStr
			// Count CASE/END CASE nesting
			caseDepth := countCaseStatements(strings.ToUpper(caseBlock))
			for caseDepth > 0 && i+1 < len(body) {
				i++
				caseBlock += ";\n" + body[i]
				caseDepth = countCaseStatements(strings.ToUpper(caseBlock))
			}
			retVal, err := e.execCaseBlockCtx(caseBlock, ctx)
			if err != nil {
				return nil, err
			}
			if retVal != nil {
				return retVal, nil
			}
			continue
		}

		// Handle SIGNAL sqlstate
		if strings.HasPrefix(stmtUpper, "SIGNAL ") {
			sigErr := e.parseSignal(stmtStr, localVars)
			// Check if there's a matching handler
			handled, exitFlag := e.tryHandler(sigErr, ctx)
			if handled {
				if exitFlag {
					return nil, nil // EXIT handler: stop block
				}
				continue // CONTINUE handler: proceed
			}
			return nil, sigErr
		}

		// Handle RESIGNAL
		if strings.HasPrefix(stmtUpper, "RESIGNAL") {
			resignalRest := strings.TrimSpace(stmtStr[len("RESIGNAL"):])
			if resignalRest == "" {
				// Bare RESIGNAL: re-raise the current condition being handled
				if ctx != nil && ctx.currentSignal != nil {
					return nil, ctx.currentSignal
				}
				// No current signal context; raise default
				return nil, &signalError{sqlState: "45000"}
			}
			sigErr := e.parseSignal(stmtStr, localVars)
			return nil, sigErr
		}

		// Handle GET DIAGNOSTICS (stub - set variables to empty/0)
		if strings.HasPrefix(stmtUpper, "GET DIAGNOSTICS") || strings.HasPrefix(stmtUpper, "GET CURRENT DIAGNOSTICS") || strings.HasPrefix(stmtUpper, "GET STACKED DIAGNOSTICS") {
			continue
		}

		// Handle unlabeled BEGIN...END block
		if stmtUpper == "BEGIN" || strings.HasPrefix(stmtUpper, "BEGIN\n") || strings.HasPrefix(stmtUpper, "BEGIN;") {
			beginBlock := stmtStr
			beginDepth := 1
			for beginDepth > 0 && i+1 < len(body) {
				i++
				line := body[i]
				lineUpper := strings.ToUpper(strings.TrimSpace(line))
				if lineUpper == "BEGIN" || strings.HasPrefix(lineUpper, "BEGIN\n") {
					beginDepth++
				}
				if isStandaloneEnd(lineUpper) {
					beginDepth--
				}
				beginBlock += ";\n" + line
			}
			inner := strings.TrimSpace(beginBlock)
			if strings.HasPrefix(strings.ToUpper(inner), "BEGIN") {
				inner = strings.TrimSpace(inner[len("BEGIN"):])
			}
			if strings.HasSuffix(strings.ToUpper(strings.TrimSpace(inner)), "END") {
				inner = strings.TrimSpace(inner[:len(inner)-len("END")])
			}
			inner = strings.TrimRight(inner, ";")
			stmts := splitTriggerBody(inner)
			// Create a child context sharing state but with own handler scope
			childCtx := ctx.childContext()
			retVal, err := e.execRoutineBodyWithContext(stmts, childCtx)
			if err != nil {
				var exitErr *exitHandlerError
				if errors.As(err, &exitErr) {
					continue
				}
				return nil, err
			}
			if retVal != nil {
				return retVal, nil
			}
			continue
		}

		// Handle CALL inside routine body (nested SP call with local var substitution)
		if strings.HasPrefix(stmtUpper, "CALL ") {
			resolvedSQL := e.substituteLocalVars(stmtStr, localVars)
			_, err := e.Execute(resolvedSQL)
			if err != nil {
				// Check if a handler can catch this error
				handled, exitFlag := e.tryHandler(err, ctx)
				if handled {
					if exitFlag {
						return nil, nil
					}
					continue
				}
				return nil, err
			}
			continue
		}

		// Handle SELECT ... INTO
		if strings.HasPrefix(stmtUpper, "SELECT") && strings.Contains(stmtUpper, " INTO ") {
			err := e.execSelectIntoForRoutine(stmtStr, localVars)
			if err != nil {
				// Check if a handler can catch this error
				handled, exitFlag := e.tryHandler(err, ctx)
				if handled {
					if exitFlag {
						return nil, nil
					}
					continue
				}
				return nil, err
			}
			continue
		}

		// General SQL statement - substitute local variables and execute
		resolvedSQL := stmtStr
		// In trigger context, resolve NEW/OLD refs with proper SQL quoting first
		if ctx.triggerNewRow != nil || ctx.triggerOldRow != nil {
			resolvedSQL = e.resolveNewOldRefs(resolvedSQL, ctx.triggerNewRow, ctx.triggerOldRow)
		}
		resolvedSQL = e.substituteLocalVars(resolvedSQL, localVars)
		stmtResult, err := e.Execute(resolvedSQL)
		if err != nil {
			// Check if a handler can catch this error
			handled, exitFlag := e.tryHandler(err, ctx)
			if handled {
				if exitFlag {
					// Return the result set from the EXIT HANDLER body (if any)
					return ctx.handlerResult, nil
				}
				continue
			}
			return nil, err
		}
		// Store result sets produced by SELECT statements inside the routine body.
		// They will be returned if an EXIT HANDLER fires, or from the routine call itself.
		if stmtResult != nil && stmtResult.IsResultSet {
			ctx.handlerResult = stmtResult
		}
	}

	_ = done
	return returnVal, nil
}

// countCaseStatements counts unmatched CASE statements (CASE - END CASE).
func countCaseStatements(upper string) int {
	caseCount := 0
	endCaseCount := strings.Count(upper, "END CASE")
	// Count CASE that starts a statement (at start or after whitespace/newline)
	idx := 0
	for {
		pos := strings.Index(upper[idx:], "CASE")
		if pos < 0 {
			break
		}
		absPos := idx + pos
		// Check it's a word boundary
		before := true
		if absPos > 0 {
			ch := upper[absPos-1]
			if (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' {
				before = false
			}
		}
		after := true
		endPos := absPos + 4
		if endPos < len(upper) {
			ch := upper[endPos]
			if (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' {
				after = false
			}
		}
		if before && after {
			// Make sure it's not "END CASE"
			if absPos >= 4 && upper[absPos-4:absPos] == "END " {
				// skip, this is END CASE
			} else {
				caseCount++
			}
		}
		idx = absPos + 4
	}
	return caseCount - endCaseCount
}

// execCaseBlockCtx executes a CASE statement block with routine context.
func (e *Executor) execCaseBlockCtx(block string, ctx *routineContext) (interface{}, error) {
	trimmed := strings.TrimSpace(block)
	upper := strings.ToUpper(trimmed)

	// Remove trailing END CASE
	if strings.HasSuffix(upper, "END CASE") {
		trimmed = strings.TrimSpace(trimmed[:len(trimmed)-len("END CASE")])
		trimmed = strings.TrimRight(trimmed, ";")
		trimmed = strings.TrimSpace(trimmed)
	}

	// Determine if this is a simple CASE (CASE expr WHEN ...) or searched CASE (CASE WHEN ...)
	afterCase := strings.TrimSpace(trimmed[4:]) // skip "CASE"
	afterCaseUpper := strings.ToUpper(afterCase)

	var baseExprStr string
	isSimple := false

	if !strings.HasPrefix(afterCaseUpper, "WHEN ") {
		// Simple CASE: extract the base expression up to the first WHEN
		whenIdx := findTopLevelKeyword(afterCase, "WHEN ")
		if whenIdx < 0 {
			return nil, fmt.Errorf("CASE statement missing WHEN")
		}
		baseExprStr = strings.TrimSpace(afterCase[:whenIdx])
		afterCase = strings.TrimSpace(afterCase[whenIdx:])
		afterCaseUpper = strings.ToUpper(afterCase)
		isSimple = true
	}

	// Evaluate base expression for simple CASE
	var baseVal interface{}
	if isSimple && baseExprStr != "" {
		var err error
		baseVal, err = e.evaluateExprWithVars(baseExprStr, ctx.localVars)
		if err != nil {
			return nil, err
		}
	}

	// Parse WHEN...THEN...ELSE blocks
	// We need to find top-level WHEN, THEN, ELSE boundaries
	type whenClause struct {
		cond string
		body string
	}
	var whens []whenClause
	var elseBody string

	remaining := afterCase
	for {
		remainingUpper := strings.ToUpper(remaining)
		if !strings.HasPrefix(remainingUpper, "WHEN ") {
			break
		}
		remaining = strings.TrimSpace(remaining[5:]) // skip "WHEN "

		// Find THEN at top level
		thenIdx := findTopLevelKeyword(remaining, " THEN")
		if thenIdx < 0 {
			break
		}
		cond := strings.TrimSpace(remaining[:thenIdx])
		remaining = strings.TrimSpace(remaining[thenIdx+5:]) // skip " THEN"

		// Find next WHEN or ELSE or end
		nextWhen := findTopLevelKeyword(remaining, "WHEN ")
		nextElse := findTopLevelKeyword(remaining, "ELSE ")
		// Pick the earliest
		endIdx := len(remaining)
		if nextWhen >= 0 && nextWhen < endIdx {
			endIdx = nextWhen
		}
		if nextElse >= 0 && nextElse < endIdx {
			endIdx = nextElse
		}
		bodyStr := strings.TrimSpace(remaining[:endIdx])
		bodyStr = strings.TrimRight(bodyStr, ";")
		whens = append(whens, whenClause{cond: cond, body: bodyStr})
		remaining = strings.TrimSpace(remaining[endIdx:])
	}

	// Check for ELSE
	remainingUpper := strings.ToUpper(remaining)
	if strings.HasPrefix(remainingUpper, "ELSE ") || strings.HasPrefix(remainingUpper, "ELSE\n") {
		elseBody = strings.TrimSpace(remaining[4:])
		elseBody = strings.TrimRight(elseBody, ";")
		elseBody = strings.TrimSpace(elseBody)
	}

	// Evaluate each WHEN clause
	for _, wc := range whens {
		matched := false
		if isSimple {
			// Compare base value to WHEN value
			whenVal, err := e.evaluateExprWithVars(wc.cond, ctx.localVars)
			if err != nil {
				continue
			}
			matched = fmt.Sprintf("%v", baseVal) == fmt.Sprintf("%v", whenVal)
		} else {
			// Searched CASE: evaluate condition as boolean
			condVal, err := e.evaluateExprWithVars(wc.cond, ctx.localVars)
			if err != nil {
				continue
			}
			matched = isTruthy(condVal)
		}
		if matched {
			stmts := splitTriggerBody(wc.body)
			return e.execRoutineBodyWithContext(stmts, ctx)
		}
	}

	// Execute ELSE block if present
	if elseBody != "" {
		stmts := splitTriggerBody(elseBody)
		return e.execRoutineBodyWithContext(stmts, ctx)
	}

	// No match and no ELSE - MySQL raises error 1339
	if isSimple {
		return nil, mysqlError(1339, "20000", "Case not found for CASE statement")
	}

	return nil, nil
}

// parseSignal parses a SIGNAL or RESIGNAL statement and returns a signalError.
func (e *Executor) parseSignal(stmtStr string, localVars map[string]interface{}) *signalError {
	upper := strings.ToUpper(strings.TrimSpace(stmtStr))
	rest := stmtStr
	if strings.HasPrefix(upper, "SIGNAL ") {
		rest = strings.TrimSpace(stmtStr[len("SIGNAL "):])
	} else if strings.HasPrefix(upper, "RESIGNAL") {
		rest = strings.TrimSpace(stmtStr[len("RESIGNAL"):])
	}

	sigErr := &signalError{sqlState: "45000"}

	restUpper := strings.ToUpper(rest)
	// Parse SQLSTATE [VALUE] 'xxxxx'
	if strings.HasPrefix(restUpper, "SQLSTATE") {
		after := strings.TrimSpace(rest[len("SQLSTATE"):])
		afterUpper := strings.ToUpper(after)
		if strings.HasPrefix(afterUpper, "VALUE ") {
			after = strings.TrimSpace(after[len("VALUE "):])
		}
		// Extract quoted state
		if len(after) > 0 && (after[0] == '\'' || after[0] == '"') {
			q := after[0]
			end := strings.IndexByte(after[1:], q)
			if end >= 0 {
				sigErr.sqlState = after[1 : end+1]
				after = strings.TrimSpace(after[end+2:])
			}
		}
		// Parse SET clause
		afterUpper = strings.ToUpper(after)
		if strings.HasPrefix(afterUpper, "SET ") {
			e.parseSignalSetClause(after[4:], sigErr, localVars)
		}
	}

	return sigErr
}

// parseSignalSetClause parses SET MESSAGE_TEXT = '...', MYSQL_ERRNO = ... in SIGNAL.
func (e *Executor) parseSignalSetClause(clause string, sigErr *signalError, localVars map[string]interface{}) {
	// Split by comma (respecting quotes)
	parts := splitByComma(clause)
	for _, part := range parts {
		part = strings.TrimSpace(part)
		eqIdx := strings.Index(part, "=")
		if eqIdx < 0 {
			continue
		}
		key := strings.ToUpper(strings.TrimSpace(part[:eqIdx]))
		val := strings.TrimSpace(part[eqIdx+1:])
		val = strings.Trim(val, "'\"")
		switch key {
		case "MESSAGE_TEXT":
			// Substitute local variables
			resolved := e.substituteLocalVars(val, localVars)
			sigErr.messageText = resolved
		case "MYSQL_ERRNO":
			if n, err := strconv.Atoi(val); err == nil {
				sigErr.mysqlErrno = n
			}
		}
	}
}

// tryHandler checks if any declared handler matches the given error and executes it.
// Returns (handled, exitFlag).
func (e *Executor) tryHandler(err error, ctx *routineContext) (bool, bool) {
	if ctx == nil || len(ctx.handlers) == 0 {
		return false, false
	}

	var sigErr *signalError
	isSignal := errors.As(err, &sigErr)

	for _, h := range ctx.handlers {
		matched := false
		for _, cond := range h.conditions {
			switch cond {
			case "SQLEXCEPTION":
				// Matches any error that is not NOT FOUND or SQLWARNING
				if isSignal {
					// Class '02' = NOT FOUND, class '01' = SQLWARNING, others = SQLEXCEPTION
					if !strings.HasPrefix(sigErr.sqlState, "02") && !strings.HasPrefix(sigErr.sqlState, "01") {
						matched = true
					}
				} else {
					// Non-signal errors are generally SQLEXCEPTION
					matched = true
				}
			case "NOT FOUND":
				if isSignal && strings.HasPrefix(sigErr.sqlState, "02") {
					matched = true
				}
			case "SQLWARNING":
				if isSignal && strings.HasPrefix(sigErr.sqlState, "01") {
					matched = true
				}
			default:
				// Specific SQLSTATE or error number
				if isSignal && sigErr.sqlState == cond {
					matched = true
				}
			}
		}
		if matched {
			// Store the current signal context for bare RESIGNAL
			prevSignal := ctx.currentSignal
			if isSignal {
				ctx.currentSignal = sigErr
			} else {
				// Wrap non-signal errors as a generic SQLEXCEPTION signal
				ctx.currentSignal = &signalError{
					sqlState:    "45000",
					messageText: err.Error(),
				}
			}
			// Execute handler body.
			// Temporarily remove handlers to prevent re-entrant handler invocation
			// (MySQL handlers are not recursive).
			if h.body != "" {
				savedHandlers := ctx.handlers
				ctx.handlers = nil
				// Clear any previous handler result before running the handler body
				ctx.handlerResult = nil
				stmts := splitTriggerBody(h.body)
				handlerBodyResult, herr := e.execRoutineBodyWithContext(stmts, ctx)
				ctx.handlers = savedHandlers
				if herr != nil {
					ctx.currentSignal = prevSignal
					return false, false
				}
				// Store the result set from the handler body (for EXIT handlers,
				// this will be returned as the CALL result).
				if handlerBodyResult != nil {
					if r, ok := handlerBodyResult.(*Result); ok && r != nil && r.IsResultSet {
						ctx.handlerResult = r
					}
				}
			}
			ctx.currentSignal = prevSignal
			return true, h.handlerType == "EXIT"
		}
	}
	return false, false
}

// substituteLocalVars replaces local variable references in a SQL string with their values.
func (e *Executor) substituteLocalVars(sql string, vars map[string]interface{}) string {
	result := sql
	// Sort variable names by length descending to avoid partial replacements
	type kv struct {
		key string
		val interface{}
	}
	var sorted []kv
	for k, v := range vars {
		sorted = append(sorted, kv{k, v})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return len(sorted[i].key) > len(sorted[j].key)
	})
	for _, pair := range sorted {
		valStr := "NULL"
		if pair.val != nil {
			switch v := pair.val.(type) {
			case string:
				valStr = "'" + strings.ReplaceAll(v, "'", "''") + "'"
			default:
				valStr = fmt.Sprintf("%v", v)
			}
		}
		// Replace variable references that appear as standalone words
		result = replaceWordBoundary(result, pair.key, valStr)
	}
	return result
}

// replaceWordBoundary replaces occurrences of word in s only when they appear at word boundaries.
func replaceWordBoundary(s, word, replacement string) string {
	var result strings.Builder
	i := 0
	wordLen := len(word)
	for i < len(s) {
		// Skip quoted strings
		if s[i] == '\'' || s[i] == '"' || s[i] == '`' {
			q := s[i]
			result.WriteByte(q)
			i++
			for i < len(s) && s[i] != q {
				result.WriteByte(s[i])
				i++
			}
			if i < len(s) {
				result.WriteByte(s[i])
				i++
			}
			continue
		}
		if i+wordLen <= len(s) && strings.EqualFold(s[i:i+wordLen], word) {
			// Check word boundary before
			if i > 0 {
				ch := s[i-1]
				if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' || (ch >= '0' && ch <= '9') || ch == '@' || ch == '.' {
					result.WriteByte(s[i])
					i++
					continue
				}
			}
			// Check word boundary after
			end := i + wordLen
			if end < len(s) {
				ch := s[end]
				if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' || (ch >= '0' && ch <= '9') || ch == '.' {
					result.WriteByte(s[i])
					i++
					continue
				}
			}
			result.WriteString(replacement)
			i += wordLen
		} else {
			result.WriteByte(s[i])
			i++
		}
	}
	return result.String()
}

// evaluateExprWithVars evaluates a simple expression string, substituting local variables.
func (e *Executor) evaluateExprWithVars(exprStr string, vars map[string]interface{}) (interface{}, error) {
	resolved := e.substituteLocalVars(exprStr, vars)
	// Try to parse and evaluate as a SQL expression
	selectSQL := "SELECT " + resolved
	result, err := e.Execute(selectSQL)
	if err != nil {
		return nil, err
	}
	if result != nil && len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
		return result.Rows[0][0], nil
	}
	return nil, nil
}

// execSelectIntoForRoutine handles SELECT ... INTO inside a stored routine,
// properly extracting INTO variable names before substituting local vars.
func (e *Executor) execSelectIntoForRoutine(stmtStr string, localVars map[string]interface{}) error {
	upper := strings.ToUpper(stmtStr)
	intoIdx := strings.Index(upper, " INTO ")
	if intoIdx < 0 {
		return nil
	}

	afterInto := stmtStr[intoIdx+len(" INTO "):]
	// Extract variable names (they end at a keyword)
	var varNames []string
	var restOfQuery string
	for _, kw := range []string{" FROM ", " WHERE ", " GROUP ", " ORDER ", " LIMIT ", " HAVING "} {
		kwIdx := strings.Index(strings.ToUpper(afterInto), kw)
		if kwIdx >= 0 {
			varPart := strings.TrimSpace(afterInto[:kwIdx])
			varNames = strings.Split(varPart, ",")
			restOfQuery = afterInto[kwIdx:]
			break
		}
	}
	if len(varNames) == 0 {
		varPart := strings.TrimSpace(afterInto)
		varNames = strings.Split(varPart, ",")
	}
	for j := range varNames {
		varNames[j] = strings.TrimSpace(varNames[j])
	}

	// Build SELECT without INTO, then substitute vars only in that part
	selectPart := stmtStr[:intoIdx]
	rewrittenSQL := selectPart + restOfQuery
	// Substitute local vars in the rewritten SQL
	resolvedSQL := e.substituteLocalVars(rewrittenSQL, localVars)

	result, err := e.Execute(resolvedSQL)
	if err != nil {
		return err
	}

	if result != nil && len(result.Rows) > 0 {
		row := result.Rows[0]
		for j, vn := range varNames {
			if j < len(row) {
				localVars[vn] = row[j]
			}
		}
	}

	return nil
}

// execSelectIntoLocal handles SELECT ... INTO local_var inside a routine body.
func (e *Executor) execSelectIntoLocal(stmtStr string, localVars map[string]interface{}) error {
	upper := strings.ToUpper(stmtStr)
	intoIdx := strings.Index(upper, " INTO ")
	if intoIdx < 0 {
		return nil
	}

	afterInto := stmtStr[intoIdx+len(" INTO "):]
	// The variable names end at the next keyword
	var varNames []string
	var restOfQuery string
	for _, kw := range []string{" FROM ", " WHERE ", " GROUP ", " ORDER ", " LIMIT ", " HAVING "} {
		kwIdx := strings.Index(strings.ToUpper(afterInto), kw)
		if kwIdx >= 0 {
			varPart := strings.TrimSpace(afterInto[:kwIdx])
			varNames = strings.Split(varPart, ",")
			restOfQuery = afterInto[kwIdx:]
			break
		}
	}
	if len(varNames) == 0 {
		varPart := strings.TrimSpace(afterInto)
		varNames = strings.Split(varPart, ",")
	}

	for j := range varNames {
		varNames[j] = strings.TrimSpace(varNames[j])
	}

	// Build SELECT without INTO
	selectPart := stmtStr[:intoIdx]
	rewrittenSQL := selectPart + restOfQuery

	result, err := e.Execute(rewrittenSQL)
	if err != nil {
		return err
	}

	if result != nil && len(result.Rows) > 0 {
		row := result.Rows[0]
		for j, vn := range varNames {
			if j < len(row) {
				localVars[vn] = row[j]
			}
		}
	}

	return nil
}

// execIfBlockCtx executes an IF block with shared routine context.
func (e *Executor) execIfBlockCtx(block string, ctx *routineContext) (bool, interface{}, error) {
	return e.execIfBlock(block, ctx.localVars, ctx.cursors, ctx.cursorDefs, ctx.notFoundHandlerVar, &ctx.done, ctx.handlers, ctx)
}

// execIfBlock executes an IF...THEN...ELSEIF...ELSE...END IF block.
func (e *Executor) execIfBlock(block string, localVars map[string]interface{}, cursors map[string]*cursorState, cursorDefs map[string]string, notFoundHandlerVar string, done *bool, handlers []handlerDef, parentCtx *routineContext) (bool, interface{}, error) {
	trimmed := strings.TrimSpace(block)
	upper := strings.ToUpper(trimmed)

	// Remove trailing END IF (only the outermost)
	if strings.HasSuffix(upper, "END IF") {
		trimmed = strings.TrimSpace(trimmed[:len(trimmed)-len("END IF")])
		// Also remove trailing semicolon if present
		trimmed = strings.TrimSpace(strings.TrimRight(trimmed, ";"))
	}

	// Find the first THEN keyword (at the top level, not inside a nested IF)
	thenIdx := findTopLevelKeyword(trimmed, " THEN")
	if thenIdx < 0 {
		return false, nil, nil
	}

	condStr := strings.TrimSpace(trimmed[3:thenIdx]) // skip "IF "
	bodyAfterThen := strings.TrimSpace(trimmed[thenIdx+len(" THEN"):])

	// In trigger context, resolve NEW/OLD references in the condition
	if parentCtx != nil && (parentCtx.triggerNewRow != nil || parentCtx.triggerOldRow != nil) {
		condStr = e.resolveNewOldRefs(condStr, parentCtx.triggerNewRow, parentCtx.triggerOldRow)
	}

	// Find top-level ELSE (not inside nested IF/END IF)
	thenBody, elseBody, hasElse := splitAtTopLevelElse(bodyAfterThen)

	// Evaluate condition
	condResolved := e.substituteLocalVars(condStr, localVars)
	condVal, err := e.evaluateExprWithVars(condResolved, map[string]interface{}{})
	if err != nil {
		return false, nil, err
	}

	// Build a temporary context for the block execution that shares the same state
	var blockCtx *routineContext
	if parentCtx != nil {
		blockCtx = parentCtx.childContext()
	} else {
		blockCtx = &routineContext{
			localVars:          localVars,
			cursors:            cursors,
			cursorDefs:         cursorDefs,
			notFoundHandlerVar: notFoundHandlerVar,
			done:               *done,
			handlers:           handlers,
		}
	}

	if isTruthy(condVal) {
		stmts := splitTriggerBody(thenBody)
		retVal, err := e.execRoutineBodyWithContext(stmts, blockCtx)
		if err != nil {
			return false, nil, err
		}
		return true, retVal, nil
	} else if hasElse {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(elseBody)), "IF ") {
			// ELSEIF case: wrap in IF...END IF and recurse
			ifBlock := strings.TrimSpace(elseBody)
			if !strings.HasSuffix(strings.ToUpper(strings.TrimSpace(ifBlock)), "END IF") {
				ifBlock += "\nEND IF"
			}
			return e.execIfBlock(ifBlock, localVars, cursors, cursorDefs, notFoundHandlerVar, done, handlers, parentCtx)
		}
		stmts := splitTriggerBody(elseBody)
		retVal, err := e.execRoutineBodyWithContext(stmts, blockCtx)
		if err != nil {
			return false, nil, err
		}
		return true, retVal, nil
	}

	return false, nil, nil
}

// findTopLevelKeyword finds a keyword at the top level (not inside nested IF/END IF blocks).
func findTopLevelKeyword(s, keyword string) int {
	upper := strings.ToUpper(s)
	return strings.Index(upper, keyword)
}

// splitAtTopLevelElse splits body at the top-level ELSE keyword, respecting nested IF blocks.
func splitAtTopLevelElse(body string) (thenBody, elseBody string, hasElse bool) {
	upper := strings.ToUpper(body)
	depth := 0

	for i := 0; i < len(upper); i++ {
		// Track IF nesting
		if i+3 <= len(upper) && upper[i:i+3] == "IF " {
			if i == 0 || !isAlphaNum(body[i-1]) {
				// Make sure it's not ELSEIF or END IF
				if i < 4 || upper[i-4:i] != "END " {
					if i < 4 || !strings.HasSuffix(upper[:i], "ELSE") {
						depth++
					}
				}
			}
		}
		if i+6 <= len(upper) && upper[i:i+6] == "END IF" {
			if i == 0 || !isAlphaNum(body[i-1]) {
				depth--
			}
		}

		// Look for ELSE at depth 0
		if depth == 0 && i+4 <= len(upper) && upper[i:i+4] == "ELSE" {
			if (i == 0 || !isAlphaNum(body[i-1])) && (i+4 >= len(upper) || !isAlphaNum(body[i+4])) {
				// Make sure it's not ELSEIF
				if i+6 <= len(upper) && upper[i:i+6] == "ELSEIF" {
					// It's ELSEIF - treat as ELSE + IF
					thenBody = strings.TrimSpace(body[:i])
					elseBody = strings.TrimSpace(body[i+4:]) // skip ELSE, leave IF
					return thenBody, elseBody, true
				}
				thenBody = strings.TrimSpace(body[:i])
				elseBody = strings.TrimSpace(body[i+4:]) // skip "ELSE"
				return thenBody, elseBody, true
			}
		}
	}

	return body, "", false
}

// execRepeatBlockCtx executes a REPEAT block with shared routine context.
func (e *Executor) execRepeatBlockCtx(block string, ctx *routineContext) (interface{}, error) {
	return e.execRepeatBlock(block, ctx.localVars, ctx.cursors, ctx.cursorDefs, ctx.notFoundHandlerVar, &ctx.done, ctx.handlers, ctx)
}

// execRepeatBlock executes a REPEAT...UNTIL...END REPEAT block.
func (e *Executor) execRepeatBlock(block string, localVars map[string]interface{}, cursors map[string]*cursorState, cursorDefs map[string]string, notFoundHandlerVar string, done *bool, handlers []handlerDef, parentCtx *routineContext) (interface{}, error) {
	upper := strings.ToUpper(strings.TrimSpace(block))

	// Remove REPEAT prefix and END REPEAT suffix
	bodyStr := strings.TrimSpace(block)
	if strings.HasPrefix(upper, "REPEAT") {
		bodyStr = strings.TrimSpace(bodyStr[len("REPEAT"):])
	}

	// Find UNTIL ... END REPEAT
	bodyUpper := strings.ToUpper(bodyStr)
	untilIdx := strings.LastIndex(bodyUpper, "UNTIL ")
	if untilIdx < 0 {
		return nil, fmt.Errorf("REPEAT without UNTIL")
	}

	loopBody := strings.TrimSpace(bodyStr[:untilIdx])
	afterUntil := strings.TrimSpace(bodyStr[untilIdx+len("UNTIL "):])
	// Remove trailing END REPEAT
	endRepeatIdx := strings.LastIndex(strings.ToUpper(afterUntil), "END REPEAT")
	condStr := afterUntil
	if endRepeatIdx >= 0 {
		condStr = strings.TrimSpace(afterUntil[:endRepeatIdx])
	}

	var blockCtx *routineContext
	if parentCtx != nil {
		blockCtx = parentCtx.childContext()
	} else {
		blockCtx = &routineContext{
			localVars:          localVars,
			cursors:            cursors,
			cursorDefs:         cursorDefs,
			notFoundHandlerVar: notFoundHandlerVar,
			done:               *done,
			handlers:           handlers,
		}
	}
	for iterations := 0; iterations < 10000; iterations++ {
		// Execute loop body
		stmts := splitTriggerBody(loopBody)
		retVal, err := e.execRoutineBodyWithContext(stmts, blockCtx)
		if err != nil {
			// LEAVE/ITERATE propagate up to the caller
			return nil, err
		}
		if retVal != nil {
			return retVal, nil
		}

		// Evaluate UNTIL condition
		condResolved := e.substituteLocalVars(condStr, localVars)
		condVal, err := e.evaluateExprWithVars(condResolved, map[string]interface{}{})
		if err != nil {
			return nil, err
		}
		if isTruthy(condVal) {
			break
		}
	}

	return nil, nil
}

// execWhileBlockCtx executes a WHILE block with shared routine context.
func (e *Executor) execWhileBlockCtx(block string, ctx *routineContext) (interface{}, error) {
	return e.execWhileBlock(block, ctx.localVars, ctx.cursors, ctx.cursorDefs, ctx.notFoundHandlerVar, &ctx.done, ctx.handlers, ctx)
}

// execWhileBlock executes a WHILE...DO...END WHILE block.
func (e *Executor) execWhileBlock(block string, localVars map[string]interface{}, cursors map[string]*cursorState, cursorDefs map[string]string, notFoundHandlerVar string, done *bool, handlers []handlerDef, parentCtx *routineContext) (interface{}, error) {
	upper := strings.ToUpper(strings.TrimSpace(block))

	bodyStr := strings.TrimSpace(block)
	if strings.HasPrefix(upper, "WHILE ") {
		bodyStr = strings.TrimSpace(bodyStr[len("WHILE "):])
	}

	// Find DO keyword
	doIdx := strings.Index(strings.ToUpper(bodyStr), " DO")
	if doIdx < 0 {
		return nil, fmt.Errorf("WHILE without DO")
	}
	condStr := strings.TrimSpace(bodyStr[:doIdx])
	afterDo := strings.TrimSpace(bodyStr[doIdx+len(" DO"):])

	// Remove trailing END WHILE
	bodyUpper := strings.ToUpper(afterDo)
	endWhileIdx := strings.LastIndex(bodyUpper, "END WHILE")
	loopBody := afterDo
	if endWhileIdx >= 0 {
		loopBody = strings.TrimSpace(afterDo[:endWhileIdx])
	}

	var blockCtx *routineContext
	if parentCtx != nil {
		blockCtx = parentCtx.childContext()
	} else {
		blockCtx = &routineContext{
			localVars:          localVars,
			cursors:            cursors,
			cursorDefs:         cursorDefs,
			notFoundHandlerVar: notFoundHandlerVar,
			done:               *done,
			handlers:           handlers,
		}
	}
	for iterations := 0; iterations < 10000; iterations++ {
		// Evaluate condition
		condResolved := e.substituteLocalVars(condStr, localVars)
		condVal, err := e.evaluateExprWithVars(condResolved, map[string]interface{}{})
		if err != nil {
			return nil, err
		}
		if !isTruthy(condVal) {
			break
		}

		// Execute loop body
		stmts := splitTriggerBody(loopBody)
		retVal, err := e.execRoutineBodyWithContext(stmts, blockCtx)
		if err != nil {
			return nil, err
		}
		if retVal != nil {
			return retVal, nil
		}
	}

	return nil, nil
}

// execLoopBlockCtx executes a LOOP...END LOOP block with optional LEAVE label.
func (e *Executor) execLoopBlockCtx(block string, label string, ctx *routineContext) (interface{}, error) {
	upper := strings.ToUpper(strings.TrimSpace(block))
	bodyStr := strings.TrimSpace(block)

	// Remove LOOP prefix
	if strings.HasPrefix(upper, "LOOP") {
		bodyStr = strings.TrimSpace(bodyStr[len("LOOP"):])
	}

	// Remove trailing END LOOP [label]
	bodyUpper := strings.ToUpper(bodyStr)
	endSuffix := "END LOOP"
	if label != "" {
		endWithLabel := "END LOOP " + strings.ToUpper(label)
		if idx := strings.LastIndex(bodyUpper, endWithLabel); idx >= 0 {
			bodyStr = strings.TrimSpace(bodyStr[:idx])
		} else if idx := strings.LastIndex(bodyUpper, endSuffix); idx >= 0 {
			bodyStr = strings.TrimSpace(bodyStr[:idx])
		}
	} else if idx := strings.LastIndex(bodyUpper, endSuffix); idx >= 0 {
		bodyStr = strings.TrimSpace(bodyStr[:idx])
	}
	bodyStr = strings.TrimRight(bodyStr, ";")

	blockCtx := ctx.childContext()

	for iterations := 0; iterations < 10000; iterations++ {
		stmts := splitTriggerBody(bodyStr)
		retVal, err := e.execRoutineBodyWithContext(stmts, blockCtx)
		if err != nil {
			// Handle LEAVE for this loop
			var le *leaveError
			if errors.As(err, &le) {
				if label == "" || strings.EqualFold(le.label, label) {
					return nil, nil // exit the loop
				}
				return nil, err // propagate to outer block
			}
			// Handle ITERATE for this loop
			var ie *iterateError
			if errors.As(err, &ie) {
				if label == "" || strings.EqualFold(ie.label, label) {
					continue // restart loop iteration
				}
				return nil, err // propagate to outer block
			}
			return nil, err
		}
		if retVal != nil {
			return retVal, nil
		}
	}
	return nil, nil
}

// execWhileBlockWithLabel executes a labeled WHILE block, supporting LEAVE/ITERATE.
func (e *Executor) execWhileBlockWithLabel(block string, label string, ctx *routineContext) (interface{}, error) {
	upper := strings.ToUpper(strings.TrimSpace(block))
	bodyStr := strings.TrimSpace(block)
	if strings.HasPrefix(upper, "WHILE ") {
		bodyStr = strings.TrimSpace(bodyStr[len("WHILE "):])
	}

	doIdx := strings.Index(strings.ToUpper(bodyStr), " DO")
	if doIdx < 0 {
		return nil, fmt.Errorf("WHILE without DO")
	}
	condStr := strings.TrimSpace(bodyStr[:doIdx])
	afterDo := strings.TrimSpace(bodyStr[doIdx+len(" DO"):])

	// Remove trailing END WHILE [label]
	bodyUpper := strings.ToUpper(afterDo)
	endSuffix := "END WHILE"
	if label != "" {
		endWithLabel := "END WHILE " + strings.ToUpper(label)
		if idx := strings.LastIndex(bodyUpper, endWithLabel); idx >= 0 {
			afterDo = strings.TrimSpace(afterDo[:idx])
		} else if idx := strings.LastIndex(bodyUpper, endSuffix); idx >= 0 {
			afterDo = strings.TrimSpace(afterDo[:idx])
		}
	} else if idx := strings.LastIndex(bodyUpper, endSuffix); idx >= 0 {
		afterDo = strings.TrimSpace(afterDo[:idx])
	}

	blockCtx := ctx.childContext()

	for iterations := 0; iterations < 10000; iterations++ {
		condResolved := e.substituteLocalVars(condStr, ctx.localVars)
		condVal, err := e.evaluateExprWithVars(condResolved, map[string]interface{}{})
		if err != nil {
			return nil, err
		}
		if !isTruthy(condVal) {
			break
		}

		stmts := splitTriggerBody(afterDo)
		retVal, err := e.execRoutineBodyWithContext(stmts, blockCtx)
		if err != nil {
			var le *leaveError
			if errors.As(err, &le) && strings.EqualFold(le.label, label) {
				return nil, nil
			}
			var ie *iterateError
			if errors.As(err, &ie) && strings.EqualFold(ie.label, label) {
				continue
			}
			return nil, err
		}
		if retVal != nil {
			return retVal, nil
		}
	}
	return nil, nil
}

// execRepeatBlockWithLabel executes a labeled REPEAT block, supporting LEAVE/ITERATE.
func (e *Executor) execRepeatBlockWithLabel(block string, label string, ctx *routineContext) (interface{}, error) {
	upper := strings.ToUpper(strings.TrimSpace(block))
	bodyStr := strings.TrimSpace(block)
	if strings.HasPrefix(upper, "REPEAT") {
		bodyStr = strings.TrimSpace(bodyStr[len("REPEAT"):])
	}

	bodyUpper := strings.ToUpper(bodyStr)
	untilIdx := strings.LastIndex(bodyUpper, "UNTIL ")
	if untilIdx < 0 {
		return nil, fmt.Errorf("REPEAT without UNTIL")
	}

	loopBody := strings.TrimSpace(bodyStr[:untilIdx])
	afterUntil := strings.TrimSpace(bodyStr[untilIdx+len("UNTIL "):])
	endRepeatIdx := strings.LastIndex(strings.ToUpper(afterUntil), "END REPEAT")
	condStr := afterUntil
	if endRepeatIdx >= 0 {
		condStr = strings.TrimSpace(afterUntil[:endRepeatIdx])
	}

	blockCtx := ctx.childContext()

	for iterations := 0; iterations < 10000; iterations++ {
		stmts := splitTriggerBody(loopBody)
		retVal, err := e.execRoutineBodyWithContext(stmts, blockCtx)
		if err != nil {
			var le *leaveError
			if errors.As(err, &le) && strings.EqualFold(le.label, label) {
				return nil, nil
			}
			var ie *iterateError
			if errors.As(err, &ie) && strings.EqualFold(ie.label, label) {
				goto checkCond
			}
			return nil, err
		}
		if retVal != nil {
			return retVal, nil
		}
	checkCond:
		condResolved := e.substituteLocalVars(condStr, ctx.localVars)
		condVal, err := e.evaluateExprWithVars(condResolved, map[string]interface{}{})
		if err != nil {
			return nil, err
		}
		if isTruthy(condVal) {
			break
		}
	}
	return nil, nil
}
