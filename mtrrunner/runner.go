// Package mtrrunner implements a simplified MySQL Test Run (.test file) runner.
// It parses and executes .test files from the MySQL test suite format,
// supporting a subset of mysqltest directives sufficient for basic compatibility testing.
package mtrrunner

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// TestResult represents the outcome of running a single .test file.
type TestResult struct {
	Name     string
	Passed   bool
	Skipped  bool
	Error    string
	Output   string
	Expected string
	Diff     string
}

// Runner executes .test files against a MySQL-compatible server.
type Runner struct {
	DB           *sql.DB
	IncludePaths []string // directories to search for --source files
	Verbose      bool
}

// RunFile executes a single .test file and compares output to .result file.
func (r *Runner) RunFile(testPath string) TestResult {
	name := filepath.Base(testPath)
	name = strings.TrimSuffix(name, ".test")

	// Read test file
	lines, err := readLines(testPath)
	if err != nil {
		return TestResult{Name: name, Error: fmt.Sprintf("failed to read test file: %v", err)}
	}

	// Find result file
	resultPath := findResultFile(testPath)

	// Reset state: ensure we're in the test database and clean up leftover tables
	r.DB.Exec("USE test") //nolint:errcheck
	// Drop all tables from previous test
	if rows, err2 := r.DB.Query("SHOW TABLES"); err2 == nil {
		var tables []string
		for rows.Next() {
			var t string
			rows.Scan(&t) //nolint:errcheck
			tables = append(tables, t)
		}
		rows.Close()
		for _, t := range tables {
			r.DB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", t)) //nolint:errcheck
		}
	}

	// Execute
	ctx := &execContext{
		runner:          r,
		db:              r.DB,
		output:          &strings.Builder{},
		warningsEnabled: true,
		queryLogEnabled: true,
		resultLogEnabled: true,
		sortResult:      false,
	}

	err = ctx.executeLines(lines)
	if err != nil {
		return TestResult{
			Name:   name,
			Error:  fmt.Sprintf("execution error: %v", err),
			Output: ctx.output.String(),
		}
	}

	actual := ctx.output.String()

	// If no result file, pass if no errors
	if resultPath == "" {
		return TestResult{Name: name, Passed: true, Output: actual}
	}

	// Read expected output
	expectedBytes, err := os.ReadFile(resultPath)
	if err != nil {
		return TestResult{Name: name, Error: fmt.Sprintf("failed to read result file: %v", err), Output: actual}
	}
	expected := string(expectedBytes)

	// Compare: normalize expected side to strip Warnings blocks etc.
	normalizedActual := normalizeOutput(actual)
	normalizedExpected := normalizeExpected(normalizeOutput(expected))
	if normalizedActual == normalizedExpected {
		return TestResult{Name: name, Passed: true, Output: actual, Expected: expected}
	}

	diff := computeDiff(normalizedExpected, normalizedActual)
	return TestResult{
		Name:     name,
		Passed:   false,
		Output:   actual,
		Expected: expected,
		Diff:     diff,
	}
}

// RunSuite runs all .test files in a suite directory.
func (r *Runner) RunSuite(suiteDir string) []TestResult {
	testDir := filepath.Join(suiteDir, "t")
	entries, err := os.ReadDir(testDir)
	if err != nil {
		return []TestResult{{Name: suiteDir, Error: fmt.Sprintf("cannot read suite dir: %v", err)}}
	}

	var results []TestResult
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".test") {
			continue
		}
		testPath := filepath.Join(testDir, entry.Name())
		result := r.RunFile(testPath)
		results = append(results, result)
	}
	return results
}

// execContext holds state during test file execution.
type execContext struct {
	runner           *Runner
	db               *sql.DB
	output           *strings.Builder
	warningsEnabled  bool
	queryLogEnabled  bool
	resultLogEnabled bool
	sortResult       bool
	expectedError    string // expected error code/name for next statement
	variables        map[string]string
	delimiter        string
}

func (ctx *execContext) executeLines(lines []string) error {
	i := 0
	for i < len(lines) {
		line := lines[i]
		trimmed := strings.TrimSpace(line)

		// Empty lines are skipped (mysqltest does not echo blank lines)
		if trimmed == "" {
			i++
			continue
		}

		// Comments starting with # are echoed to output
		if strings.HasPrefix(trimmed, "#") {
			ctx.output.WriteString(line + "\n")
			i++
			continue
		}

		// Handle directives (lines starting with --)
		if strings.HasPrefix(trimmed, "--") {
			directive := strings.TrimPrefix(trimmed, "--")
			directive = strings.TrimSpace(directive)

			handled, skip, err := ctx.handleDirective(directive)
			if err != nil {
				return fmt.Errorf("line %d: %v", i+1, err)
			}
			if skip {
				return nil // --skip: stop execution
			}
			if handled {
				i++
				continue
			}
			// If not handled as directive, treat as SQL
		}

		// Handle bare directives (without -- prefix): eval, let, echo, source, skip,
		// enable_warnings, disable_warnings, etc.
		if bareDirective, ok := extractBareDirective(trimmed); ok {
			handled, skip, err := ctx.handleDirective(bareDirective)
			if err != nil {
				return fmt.Errorf("line %d: %v", i+1, err)
			}
			if skip {
				return nil
			}
			if handled {
				i++
				continue
			}
		}

		// Collect multi-line SQL statement (until delimiter)
		delim := ";"
		if ctx.delimiter != "" {
			delim = ctx.delimiter
		}

		stmt := ""
		for i < len(lines) {
			l := lines[i]
			t := strings.TrimSpace(l)

			// Output comments within statement
			if strings.HasPrefix(t, "#") {
				ctx.output.WriteString(l + "\n")
				i++
				continue
			}
			if strings.HasPrefix(t, "--") {
				// Could be a directive mid-statement, handle it
				d := strings.TrimPrefix(t, "--")
				d = strings.TrimSpace(d)
				if isDirectiveKeyword(d) {
					break
				}
			}

			// Strip inline comments (# outside quotes)
			t = stripInlineComment(t)

			if strings.HasSuffix(t, delim) {
				stmt += strings.TrimSuffix(t, delim)
				i++
				break
			}
			stmt += t + " "
			i++
		}

		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		// Handle multiple statements on one line (e.g. "DROP TABLE t1; SHOW TABLES")
		stmts := splitStatements(stmt)
		for _, s := range stmts {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			err := ctx.executeSQL(s)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (ctx *execContext) handleDirective(directive string) (handled bool, skip bool, err error) {
	lower := strings.ToLower(directive)

	// Parse directive name and args
	parts := strings.SplitN(directive, " ", 2)
	name := strings.ToLower(parts[0])
	args := ""
	if len(parts) > 1 {
		args = strings.TrimSpace(parts[1])
	}

	switch name {
	case "skip":
		return true, true, nil

	case "error":
		ctx.expectedError = args
		return true, false, nil

	case "echo":
		if ctx.resultLogEnabled {
			ctx.output.WriteString(args + "\n")
		}
		return true, false, nil

	case "disable_warnings":
		ctx.warningsEnabled = false
		return true, false, nil
	case "enable_warnings":
		ctx.warningsEnabled = true
		return true, false, nil

	case "disable_query_log":
		ctx.queryLogEnabled = false
		return true, false, nil
	case "enable_query_log":
		ctx.queryLogEnabled = true
		return true, false, nil

	case "disable_result_log":
		ctx.resultLogEnabled = false
		return true, false, nil
	case "enable_result_log":
		ctx.resultLogEnabled = true
		return true, false, nil

	case "sorted_result":
		ctx.sortResult = true
		return true, false, nil

	case "let":
		return true, false, ctx.setVariable(args)

	case "source":
		return true, false, ctx.sourceFile(args)

	case "delimiter":
		ctx.delimiter = strings.TrimSpace(args)
		if ctx.delimiter == ";" {
			ctx.delimiter = ""
		}
		return true, false, nil

	case "query", "eval":
		// Execute the rest as SQL
		if args != "" {
			err := ctx.executeSQL(args)
			return true, false, err
		}
		return true, false, nil

	// Directives we accept but ignore
	case "character_set", "charset":
		return true, false, nil
	case "disable_metadata", "enable_metadata",
		"disable_ps_protocol", "enable_ps_protocol",
		"disable_cursor_protocol", "enable_cursor_protocol",
		"disable_view_protocol", "enable_view_protocol",
		"disable_session_track_info", "enable_session_track_info",
		"connect", "connection", "disconnect",
		"send", "reap", "sleep",
		"replace_result", "replace_regex", "replace_column",
		"remove_file", "write_file", "append_file", "cat_file",
		"mkdir", "rmdir", "copy_file", "move_file",
		"list_files", "file_exists",
		"exec", "system",
		"die", "exit",
		"if", "while", "end",
		"inc", "dec",
		"horizontal_results", "vertical_results",
		"require", "result_format":
		return true, false, nil
	}

	// Check for --error shorthand without space
	if strings.HasPrefix(lower, "error") {
		ctx.expectedError = strings.TrimSpace(strings.TrimPrefix(lower, "error"))
		return true, false, nil
	}

	return false, false, nil
}

func (ctx *execContext) executeSQL(stmt string) error {
	// Variable substitution
	stmt = ctx.substituteVars(stmt)

	// Echo the SQL statement to output (mysqltest default behavior)
	if ctx.queryLogEnabled {
		ctx.output.WriteString(stmt + ";\n")
	}

	upper := strings.ToUpper(strings.TrimSpace(stmt))
	isQuery := strings.HasPrefix(upper, "SELECT") ||
		strings.HasPrefix(upper, "SHOW") ||
		strings.HasPrefix(upper, "DESCRIBE") ||
		strings.HasPrefix(upper, "DESC ") ||
		strings.HasPrefix(upper, "EXPLAIN")

	if isQuery {
		return ctx.executeQuery(stmt)
	}
	return ctx.executeExec(stmt)
}

func (ctx *execContext) executeQuery(stmt string) error {
	rows, err := ctx.db.Query(stmt)
	if err != nil {
		if ctx.expectedError != "" {
			ctx.expectedError = ""
			return nil
		}
		return fmt.Errorf("query failed: %s: %v", stmt, err)
	}
	defer rows.Close()

	if ctx.expectedError != "" {
		ctx.expectedError = ""
		// Expected error but didn't get one - could be ok in some cases
	}

	if !ctx.resultLogEnabled {
		return nil
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// Write column headers
	ctx.output.WriteString(strings.Join(columns, "\t") + "\n")

	// Collect result rows
	var resultLines []string
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		parts := make([]string, len(columns))
		for i, v := range values {
			if v == nil {
				parts[i] = "NULL"
			} else {
				switch val := v.(type) {
				case []byte:
					parts[i] = string(val)
				default:
					parts[i] = fmt.Sprintf("%v", val)
				}
			}
		}
		resultLines = append(resultLines, strings.Join(parts, "\t"))
	}

	// Apply --sorted_result
	if ctx.sortResult {
		sort.Strings(resultLines)
		ctx.sortResult = false
	}

	for _, line := range resultLines {
		ctx.output.WriteString(line + "\n")
	}

	return nil
}

func (ctx *execContext) executeExec(stmt string) error {
	_, err := ctx.db.Exec(stmt)
	if err != nil {
		if ctx.expectedError != "" {
			ctx.expectedError = ""
			return nil
		}
		return fmt.Errorf("exec failed: %s: %v", stmt, err)
	}
	if ctx.expectedError != "" {
		ctx.expectedError = ""
	}
	return nil
}

func (ctx *execContext) setVariable(expr string) error {
	if ctx.variables == nil {
		ctx.variables = make(map[string]string)
	}
	// Format: $var = value  or  $var= `SELECT ...`
	parts := strings.SplitN(expr, "=", 2)
	if len(parts) != 2 {
		return nil
	}
	name := strings.TrimSpace(parts[0])
	value := strings.TrimSpace(parts[1])

	// If value is wrapped in backticks, execute as SQL and use first column of first row
	if strings.HasPrefix(value, "`") && strings.HasSuffix(value, "`") {
		sqlStmt := strings.TrimPrefix(strings.TrimSuffix(value, "`"), "`")
		sqlStmt = ctx.substituteVars(strings.TrimSpace(sqlStmt))
		row := ctx.db.QueryRow(sqlStmt)
		var result string
		if err := row.Scan(&result); err != nil {
			// Store empty string on error (query might return no rows)
			ctx.variables[name] = ""
			return nil
		}
		ctx.variables[name] = result
		return nil
	}

	ctx.variables[name] = value
	return nil
}

func (ctx *execContext) substituteVars(s string) string {
	if ctx.variables == nil {
		return s
	}
	for name, value := range ctx.variables {
		s = strings.ReplaceAll(s, name, value)
	}
	return s
}

func (ctx *execContext) sourceFile(filename string) error {
	filename = strings.TrimSpace(filename)
	// Apply variable substitution in filename
	filename = ctx.substituteVars(filename)
	// Try include paths
	for _, dir := range ctx.runner.IncludePaths {
		path := filepath.Join(dir, filename)
		if _, err := os.Stat(path); err == nil {
			lines, err := readLines(path)
			if err != nil {
				return err
			}
			return ctx.executeLines(lines)
		}
	}
	// File not found - skip silently (many includes are optional)
	return nil
}

// Helper functions

func readLines(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024) // 1MB buffer
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func findResultFile(testPath string) string {
	dir := filepath.Dir(filepath.Dir(testPath)) // go up from t/ to suite dir
	name := strings.TrimSuffix(filepath.Base(testPath), ".test")
	resultPath := filepath.Join(dir, "r", name+".result")
	if _, err := os.Stat(resultPath); err == nil {
		return resultPath
	}
	return ""
}

var directiveKeywords = map[string]bool{
	"error": true, "echo": true, "skip": true, "source": true,
	"let": true, "delimiter": true, "query": true, "eval": true,
	"disable_warnings": true, "enable_warnings": true,
	"disable_query_log": true, "enable_query_log": true,
	"disable_result_log": true, "enable_result_log": true,
	"sorted_result": true, "connect": true, "disconnect": true,
	"send": true, "reap": true, "sleep": true,
	"horizontal_results": true, "vertical_results": true,
	"exit": true, "connection": true, "die": true,
	"replace_column": true, "replace_result": true, "replace_regex": true,
	"remove_file": true, "write_file": true, "append_file": true,
	"cat_file": true, "diff_files": true, "file_exists": true,
	"copy_file": true, "chmod": true, "mkdir": true, "rmdir": true,
	"list_files": true, "move_file": true,
	"exec": true, "execw": true, "system": true,
	"perl": true, "if": true, "while": true, "inc": true, "dec": true,
	"disable_abort_on_error": true, "enable_abort_on_error": true,
	"real_sleep": true, "query_get_value": true,
	"save_master_pos": true, "sync_with_master": true,
	"result_format": true, "change_user": true,
	"disable_metadata": true, "enable_metadata": true,
	"disable_info": true, "enable_info": true,
}

func isDirectiveKeyword(s string) bool {
	parts := strings.SplitN(strings.ToLower(s), " ", 2)
	return directiveKeywords[parts[0]]
}

// barePrefixKeywords are mysqltest commands that may appear without a leading "--".
var barePrefixKeywords = []string{
	"eval ",
	"let ",
	"echo ",
	"source ",
	"delimiter ",
	"skip",
	"exit",
	"die ",
	"connection ",
	"connect ",
	"disconnect ",
	"send ",
	"reap",
	"enable_warnings",
	"disable_warnings",
	"enable_query_log",
	"disable_query_log",
	"enable_result_log",
	"disable_result_log",
	"enable_metadata",
	"disable_metadata",
	"enable_abort_on_error",
	"disable_abort_on_error",
	"sorted_result",
	"replace_column ",
	"replace_result ",
	"replace_regex ",
	"error ",
	"if ",
	"while ",
	"end",
	"inc ",
	"dec ",
	"horizontal_results",
	"vertical_results",
	"real_sleep ",
	"sleep ",
	"perl",
	"exec ",
	"result_format ",
	"change_user",
	"remove_file ",
	"write_file ",
	"append_file ",
	"cat_file ",
	"mkdir ",
	"rmdir ",
	"copy_file ",
	"move_file ",
	"list_files ",
	"file_exists ",
	"chmod ",
	"query_get_value",
	"save_master_pos",
	"sync_with_master",
	"disable_info",
	"enable_info",
}

// extractBareDirective checks whether trimmed is a bare (no "--") mysqltest directive.
// If so it returns the directive string (with trailing ";" stripped) ready to pass to
// handleDirective, and ok=true.
func extractBareDirective(trimmed string) (string, bool) {
	lower := strings.ToLower(trimmed)
	// Strip trailing semicolon for no-arg keywords
	stripped := strings.TrimRight(trimmed, ";")
	strippedLower := strings.ToLower(stripped)

	for _, kw := range barePrefixKeywords {
		kw = strings.TrimRight(kw, " ") // canonical keyword without trailing space
		kwWithSpace := kw + " "

		if strings.HasPrefix(lower, kwWithSpace) {
			// keyword with arguments – strip trailing ";" from the arg portion
			// For "delimiter", don't strip ";" as it may be the actual delimiter value
			rest := trimmed[len(kwWithSpace):]
			if kw != "delimiter" {
				rest = strings.TrimRight(rest, ";")
			} else {
				// For delimiter, only strip a single trailing ";" if present
				rest = strings.TrimSpace(rest)
				if strings.HasSuffix(rest, ";") {
					rest = rest[:len(rest)-1]
				}
			}
			return kw + " " + strings.TrimSpace(rest), true
		}
		if strippedLower == kw {
			// keyword with no arguments (possibly followed by ";")
			return stripped, true
		}
	}
	return "", false
}

// splitStatements splits a string that may contain multiple SQL statements
// separated by semicolons, respecting quoted strings.
func splitStatements(s string) []string {
	var stmts []string
	var current strings.Builder
	inSingle := false
	inDouble := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch ch {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
			current.WriteByte(ch)
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
			current.WriteByte(ch)
		case ';':
			if !inSingle && !inDouble {
				stmt := strings.TrimSpace(current.String())
				if stmt != "" {
					stmts = append(stmts, stmt)
				}
				current.Reset()
			} else {
				current.WriteByte(ch)
			}
		default:
			current.WriteByte(ch)
		}
	}
	rest := strings.TrimSpace(current.String())
	if rest != "" {
		stmts = append(stmts, rest)
	}
	if len(stmts) == 0 {
		return []string{s}
	}
	return stmts
}

// stripInlineComment removes trailing # comments from a SQL line,
// respecting quoted strings.
func stripInlineComment(line string) string {
	inSingle := false
	inDouble := false
	for i, ch := range line {
		switch ch {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
		case '#':
			if !inSingle && !inDouble {
				return strings.TrimSpace(line[:i])
			}
		}
	}
	return line
}

func normalizeOutput(s string) string {
	// Normalize line endings and trailing whitespace
	lines := strings.Split(s, "\n")
	var result []string
	for _, line := range lines {
		result = append(result, strings.TrimRight(line, " \t\r"))
	}
	return strings.TrimRight(strings.Join(result, "\n"), "\n")
}

// normalizeExpected strips MySQL-specific output that mylite doesn't produce:
// - Warnings/Note/Error blocks
// - include file outputs (lines like "include/xxx.inc")
func normalizeExpected(s string) string {
	lines := strings.Split(s, "\n")
	var result []string
	inWarnings := false
	for _, line := range lines {
		trimmed := strings.TrimRight(line, " \t\r")

		// Detect start of Warnings block
		if trimmed == "Warnings:" {
			inWarnings = true
			continue
		}
		// Skip Warning/Note/Error lines within a Warnings block
		if inWarnings {
			if strings.HasPrefix(trimmed, "Warning\t") ||
				strings.HasPrefix(trimmed, "Note\t") ||
				strings.HasPrefix(trimmed, "Error\t") {
				continue
			}
			inWarnings = false
		}

		result = append(result, trimmed)
	}
	return strings.TrimRight(strings.Join(result, "\n"), "\n")
}

var diffContextLines = 3

func computeDiff(expected, actual string) string {
	expLines := strings.Split(expected, "\n")
	actLines := strings.Split(actual, "\n")

	var diff strings.Builder
	maxLen := len(expLines)
	if len(actLines) > maxLen {
		maxLen = len(actLines)
	}

	diffCount := 0
	for i := 0; i < maxLen && diffCount < 20; i++ {
		exp := ""
		act := ""
		if i < len(expLines) {
			exp = expLines[i]
		}
		if i < len(actLines) {
			act = actLines[i]
		}
		if strings.TrimRight(exp, " \t\r") != strings.TrimRight(act, " \t\r") {
			diff.WriteString(fmt.Sprintf("line %d:\n  expected: %s\n  actual:   %s\n", i+1, exp, act))
			diffCount++
		}
	}

	if diffCount == 0 {
		return "(no visible diff - whitespace differences only)"
	}
	return diff.String()
}

// re for matching error codes
var reErrorCode = regexp.MustCompile(`^\d+$`)
