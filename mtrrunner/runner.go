// Package mtrrunner implements a simplified MySQL Test Run (.test file) runner.
// It parses and executes .test files from the MySQL test suite format,
// supporting a subset of mysqltest directives sufficient for basic compatibility testing.
package mtrrunner

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/transform"
)

// errSkipTest is a sentinel error indicating the test should be skipped.
var errSkipTest = errors.New("skip test")

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
	TmpDir       string // temporary directory for file operations ($MYSQLTEST_VARDIR)
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
	// Drop all tables from previous test (including temporary tables via MYLITE)
	r.DB.Exec("MYLITE RESET_TEMP_TABLES") //nolint:errcheck
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
	// Drop user-created databases from previous test (keep system databases)
	systemDBs := map[string]bool{
		"information_schema": true, "mysql": true, "performance_schema": true,
		"sys": true, "test": true, "mtr": true,
	}
	if rows, err2 := r.DB.Query("SHOW DATABASES"); err2 == nil {
		var databases []string
		for rows.Next() {
			var d string
			rows.Scan(&d) //nolint:errcheck
			if !systemDBs[d] {
				databases = append(databases, d)
			}
		}
		rows.Close()
		for _, d := range databases {
			r.DB.Exec(fmt.Sprintf("DROP DATABASE `%s`", d)) //nolint:errcheck
		}
	}

	// Execute with timeout
	timeout := 600 * time.Second
	doneCh := make(chan error, 1)

	tmpDir := r.TmpDir
	if tmpDir == "" {
		tmpDir, _ = os.MkdirTemp("", "mylite-mtr-*")
	}
	// Ensure tmp subdir exists
	os.MkdirAll(filepath.Join(tmpDir, "tmp"), 0755) //nolint:errcheck

	ectx := &execContext{
		runner:           r,
		db:               r.DB,
		connByName:       map[string]*sql.Conn{},
		output:           &strings.Builder{},
		warningsEnabled:  true,
		queryLogEnabled:  true,
		resultLogEnabled: true,
		sortResult:       false,
		tmpDir:           tmpDir,
		ttsBackups:       map[string]tableSnapshot{},
		variables: map[string]string{
			"$ENGINE":             "InnoDB",
			"$MYSQLTEST_VARDIR":   tmpDir,
			"$MYSQL_TMP_DIR":      filepath.Join(tmpDir, "tmp"),
			"$MYSQL_TEST_DIR":     tmpDir,
			"$MYSQLD_DATADIR":     filepath.Join(tmpDir, "data", "inner") + "/",
			"$MYSQL_SOCKET":       "",
			"$MASTER_MYPORT":      "3306",
			"$MYSQL_VERSION_ID":   "80032",
			"$innodb_page_size":   "16384",
			"$restart_parameters": "restart",
			"$BIG_TEST":           "1",
			"$VALGRIND_TEST":      "0",
		},
	}

	// Read master.opt to apply server options (e.g., --innodb_page_size=32k)
	masterOptPath := filepath.Join(filepath.Dir(testPath), name+"-master.opt")
	if optData, err := os.ReadFile(masterOptPath); err == nil {
		applyMasterOpt(string(optData), ectx)
	}

	timeoutCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	go func() {
		doneCh <- ectx.executeLines(lines)
	}()

	select {
	case err = <-doneCh:
	case <-timeoutCtx.Done():
		return TestResult{Name: name, Error: "timeout: test took too long"}
	}
	ctx := ectx
	ctx.closeConnections()
	if errors.Is(err, errSkipTest) {
		return TestResult{Name: name, Skipped: true}
	}
	if err != nil {
		return TestResult{
			Name:   name,
			Error:  fmt.Sprintf("execution error: %v", err),
			Output: ctx.output.String(),
		}
	}

	// Check if test was skipped via --skip directive
	if ctx.skipped {
		return TestResult{Name: name, Skipped: true}
	}

	actual := ctx.output.String()

	// If no result file, pass if no errors
	if resultPath == "" {
		return TestResult{Name: name, Passed: true, Output: actual}
	}

	// Read expected output, converting encoding if needed
	expectedBytes, err := os.ReadFile(resultPath)
	if err != nil {
		return TestResult{Name: name, Error: fmt.Sprintf("failed to read result file: %v", err), Output: actual}
	}
	// Convert result file encoding to match the test file encoding
	if isSJISEncoded(expectedBytes) {
		if decoded, err := decodeSJIS(expectedBytes); err == nil {
			expectedBytes = decoded
		}
	} else if isEUCJPEncoded(expectedBytes) {
		if decoded, err := decodeEUCJP(expectedBytes); err == nil {
			expectedBytes = decoded
		}
	} else if !isValidUTF8(expectedBytes) {
		// If result file is not valid UTF-8 and test name contains encoding hints,
		// try to decode based on the test name.
		// Note: ucs2 test files are also EUC-JP encoded (the charset refers to MySQL charset being tested).
		if strings.Contains(name, "sjis") {
			if decoded, err := decodeSJIS(expectedBytes); err == nil {
				expectedBytes = decoded
			}
		} else if strings.Contains(name, "ujis") || strings.Contains(name, "ucs2") {
			if decoded, err := decodeEUCJP(expectedBytes); err == nil {
				expectedBytes = decoded
			}
		}
	}
	expected := string(expectedBytes)

	// Compare: normalize expected side to strip Warnings blocks etc.
	normalizedActual := normalizeOutput(actual)
	normalizedActual = strings.ReplaceAll(normalizedActual, "ENGINE=ENGINE", "ENGINE=InnoDB")
	normalizedActual = strings.ReplaceAll(normalizedActual, "ENGINE=MyISAM", "ENGINE=InnoDB")
	normalizedActual = strings.ReplaceAll(normalizedActual, "ENGINE=MEMORY", "ENGINE=InnoDB")
	normalizedActual = normalizeFuncCase(normalizedActual)
	normalizedExpected := normalizeExpected(normalizeOutput(expected))
	normalizedExpected = normalizeFuncCase(normalizedExpected)
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
	connByName       map[string]*sql.Conn // mysqltest named connections
	currentConn      string               // empty means default connection
	output           *strings.Builder
	warningsEnabled  bool
	queryLogEnabled  bool
	resultLogEnabled bool
	sortResult       bool
	expectedError    string // expected error code/name for next statement
	variables        map[string]string
	delimiter        string
	tmpDir           string         // temporary directory for file operations
	replaceColumns   map[int]string // column index (1-based) -> replacement value for next query
	replaceResult    []string       // pairs of [from, to] for --replace_result
	verticalResult   bool           // format next query result as vertical key/value pairs
	verticalResults  bool           // persistent vertical output mode (--vertical_results)
	infoEnabled      bool           // --enable_info: show affected rows and info after DML
	skipped          bool           // set to true when --skip directive is encountered
	ttsBackups       map[string]tableSnapshot
}

type tableSnapshot struct {
	columns []string
	rows    [][]interface{}
}

func (ctx *execContext) executeLines(lines []string) error {
	i := 0
	for i < len(lines) {
		line := lines[i]
		trimmed := strings.TrimSpace(line)

		// Skip mysqltest perl blocks entirely.
		if isPerlBlockStart(trimmed) {
			i = ctx.handlePerlBlock(lines, i)
			continue
		}
		// Handle heredoc file directives (write_file / append_file).
		if isHereDocDirectiveStart(trimmed) {
			handled, newI, err := ctx.handleHereDocDirective(lines, i)
			if err != nil {
				return err
			}
			if handled {
				i = newI
				continue
			}
			i = skipHereDoc(lines, i)
			continue
		}

		// Empty lines are skipped (mysqltest does not echo blank lines)
		if trimmed == "" {
			i++
			continue
		}

		// Comments starting with # are NOT echoed to output (mysqltest behavior)
		if strings.HasPrefix(trimmed, "#") {
			i++
			continue
		}

		// Standalone block terminators from skipped mysqltest/perl blocks.
		if trimmed == "}" || trimmed == "};" {
			i++
			continue
		}

		// Some mysqltest includes execute SQL via a bare variable line ($var).
		if strings.HasPrefix(trimmed, "$") && !strings.ContainsAny(trimmed, " \t") {
			expanded := strings.TrimSpace(ctx.substituteVars(trimmed))
			if expanded != "" && expanded != trimmed {
				if err := ctx.executeSQL(expanded); err != nil {
					return err
				}
			}
			i++
			continue
		}

		// Handle if/while blocks
		ifHandled, ifSkip, newI := ctx.handleIfBlock(lines, i)
		if ifHandled {
			if ifSkip {
				return errSkipTest
			}
			i = newI
			continue
		}

		// Skip { } blocks (from unhandled if/while directives)
		if trimmed == "{" {
			depth := 1
			i++
			for i < len(lines) && depth > 0 {
				t := strings.TrimSpace(lines[i])
				if t == "{" || strings.HasSuffix(t, "{") {
					depth++
				}
				if t == "}" {
					depth--
				}
				i++
			}
			continue
		}

		// Handle if/while with { on the same line
		lowerTrimmed := strings.ToLower(trimmed)
		isWhile := strings.HasPrefix(lowerTrimmed, "while ") || strings.HasPrefix(lowerTrimmed, "while(") ||
			strings.HasPrefix(lowerTrimmed, "--while ") || strings.HasPrefix(lowerTrimmed, "--while(")
		isIf := !isWhile && (strings.HasPrefix(lowerTrimmed, "if ") || strings.HasPrefix(lowerTrimmed, "if(") ||
			strings.HasPrefix(lowerTrimmed, "--if ") || strings.HasPrefix(lowerTrimmed, "--if("))
		isIfWhile := isWhile || isIf

		if isWhile && strings.Contains(trimmed, "{") {
			// Extract condition
			condStr := trimmed
			condStr = strings.TrimPrefix(strings.TrimPrefix(condStr, "--"), "")
			condStr = strings.TrimSpace(condStr)
			if strings.HasPrefix(strings.ToLower(condStr), "--while") {
				condStr = condStr[7:]
			} else if strings.HasPrefix(strings.ToLower(condStr), "while") {
				condStr = condStr[5:]
			}
			condStr = strings.TrimSpace(condStr)
			condStr = strings.TrimSuffix(condStr, "{")
			condStr = strings.TrimSpace(condStr)
			condStr = strings.TrimPrefix(condStr, "(")
			condStr = strings.TrimSuffix(condStr, ")")
			condStr = strings.TrimSpace(condStr)

			// Collect body lines until matching }
			bodyStart := i + 1
			depth := 1
			j := bodyStart
			for j < len(lines) && depth > 0 {
				t := strings.TrimSpace(lines[j])
				for _, ch := range t {
					if ch == '{' {
						depth++
					} else if ch == '}' {
						depth--
					}
				}
				if depth == 0 {
					break
				}
				j++
			}
			bodyLines := lines[bodyStart:j]

			// Execute while loop
			for loopCount := 0; loopCount < 100000; loopCount++ {
				condVal := ctx.substituteVars(condStr)
				n, _ := strconv.Atoi(condVal)
				if n == 0 {
					break
				}
				err := ctx.executeLines(bodyLines)
				if err != nil {
					if errors.Is(err, errSkipTest) {
						return err
					}
					return fmt.Errorf("line %d (while body): %v", i+1, err)
				}
			}
			i = j + 1 // skip past the closing }
			continue
		}
		if isIf && strings.Contains(trimmed, "{") {
			// Skip if blocks
			depth := 1
			i++
			for i < len(lines) && depth > 0 {
				t := strings.TrimSpace(lines[i])
				for _, ch := range t {
					if ch == '{' {
						depth++
					} else if ch == '}' {
						depth--
					}
				}
				i++
			}
			continue
		}
		if isIfWhile {
			// No { on this line - handle the next-line { case
			if isWhile {
				// Look for { on next line and collect body
				nextI := i + 1
				for nextI < len(lines) && strings.TrimSpace(lines[nextI]) == "" {
					nextI++
				}
				if nextI < len(lines) && strings.TrimSpace(lines[nextI]) == "{" {
					// Extract condition
					condStr := trimmed
					if strings.HasPrefix(strings.ToLower(condStr), "--while") {
						condStr = condStr[7:]
					} else if strings.HasPrefix(strings.ToLower(condStr), "while") {
						condStr = condStr[5:]
					}
					condStr = strings.TrimSpace(condStr)
					condStr = strings.TrimPrefix(condStr, "(")
					condStr = strings.TrimSuffix(condStr, ")")
					condStr = strings.TrimSpace(condStr)

					bodyStart := nextI + 1
					depth := 1
					j := bodyStart
					for j < len(lines) && depth > 0 {
						t := strings.TrimSpace(lines[j])
						if t == "{" {
							depth++
						}
						if t == "}" {
							depth--
							if depth == 0 {
								break
							}
						}
						j++
					}
					bodyLines := lines[bodyStart:j]
					for loopCount := 0; loopCount < 100000; loopCount++ {
						condVal := ctx.substituteVars(condStr)
						n, _ := strconv.Atoi(condVal)
						if n == 0 {
							break
						}
						err := ctx.executeLines(bodyLines)
						if err != nil {
							if errors.Is(err, errSkipTest) {
								return err
							}
							return fmt.Errorf("line %d (while body): %v", i+1, err)
						}
					}
					i = j + 1
					continue
				}
			}
			i++
			continue
		}

		// Handle directives (lines starting with --)
		if strings.HasPrefix(trimmed, "--") {
			directive := strings.TrimPrefix(trimmed, "--")
			directive = strings.TrimSpace(directive)
			name, _ := parseDirectiveNameArgs(directive)
			if (name == "query" || name == "query_vertical") &&
				!strings.HasSuffix(strings.TrimSpace(trimmed), ";") {
				fullDirective := directive
				i++
				for i < len(lines) {
					l := strings.TrimSpace(lines[i])
					fullDirective += "\n" + l
					if strings.HasSuffix(strings.TrimSpace(l), ";") {
						break
					}
					i++
				}
				directive = fullDirective
			}

			handled, skip, err := ctx.handleDirective(directive)
			if err != nil && !errors.Is(err, errSkipTest) {
				return fmt.Errorf("line %d: %v", i+1, err)
			}
			if skip {
				return errSkipTest
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
			advancedLine := false
			// For 'let' directives, collect multiline values until ';'
			// but only if the value doesn't end with a backtick (single-line query)
			bdLower := strings.ToLower(bareDirective)
			if strings.HasPrefix(bdLower, "let ") {
				letVal := strings.TrimSpace(bareDirective)
				// Check if the original line already ended with ';' (complete single-line let)
				originalEndsWithSemicolon := strings.HasSuffix(strings.TrimSpace(trimmed), ";")
				// Check if value is incomplete (doesn't end with ';' and not a backtick expression)
				isBacktickExpr := false
				if eqIdx := strings.Index(letVal, "="); eqIdx >= 0 {
					rhs := strings.TrimSpace(letVal[eqIdx+1:])
					isBacktickExpr = strings.HasPrefix(rhs, "`") && strings.HasSuffix(rhs, "`")
				}
				if !isBacktickExpr && !originalEndsWithSemicolon && !strings.HasSuffix(letVal, ";") {
					fullDirective := bareDirective
					i++
					for i < len(lines) {
						l := strings.TrimSpace(lines[i])
						fullDirective += "\n" + l
						i++
						if strings.HasSuffix(l, ";") {
							fullDirective = strings.TrimSuffix(fullDirective, ";")
							break
						}
					}
					bareDirective = fullDirective
					advancedLine = true
				}
			}
			if strings.HasPrefix(bdLower, "query ") ||
				strings.HasPrefix(bdLower, "query_vertical ") ||
				strings.HasPrefix(bdLower, "eval ") {
				if !strings.HasSuffix(strings.TrimSpace(trimmed), ";") {
					fullDirective := bareDirective
					i++
					for i < len(lines) {
						l := strings.TrimSpace(lines[i])
						fullDirective += "\n" + l
						if strings.HasSuffix(strings.TrimSpace(l), ";") {
							fullDirective = strings.TrimSuffix(fullDirective, ";")
							i++ // consume the terminating line so it won't be re-executed as SQL
							break
						}
						i++
					}
					bareDirective = fullDirective
					advancedLine = true
				}
			}
			handled, skip, err := ctx.handleDirective(bareDirective)
			if err != nil && !errors.Is(err, errSkipTest) {
				return fmt.Errorf("line %d: %v", i+1, err)
			}
			if skip {
				return errSkipTest
			}
			if handled {
				if !advancedLine {
					i++
				}
				continue
			}
		}

		// Handle while/if blocks with { on the same line (second pass - after bare directive handling)
		// This is already handled by the isWhile/isIf block above, but if we get here
		// it means the bare directive handler consumed a "while" keyword. Skip to be safe.
		if (strings.HasPrefix(trimmed, "if(") || strings.HasPrefix(trimmed, "if ") ||
			strings.HasPrefix(trimmed, "--if")) &&
			strings.HasSuffix(trimmed, "{") {
			depth := 1
			i++
			for i < len(lines) && depth > 0 {
				t := strings.TrimSpace(lines[i])
				if strings.HasSuffix(t, "{") {
					depth++
				}
				if t == "}" {
					depth--
				}
				i++
			}
			continue
		}

		// Collect multi-line SQL statement (until delimiter)
		delim := ";"
		if ctx.delimiter != "" {
			delim = ctx.delimiter
		}

		// Collect raw lines for echoing and build SQL statement
		var rawLines []string
		stmt := ""
		inStringLiteral := false // track if we're inside a multi-line string literal
		for i < len(lines) {
			l := lines[i]
			t := strings.TrimSpace(l)

			// Skip comments within statement (only if not inside a string literal)
			if !inStringLiteral && strings.HasPrefix(t, "#") {
				i++
				continue
			}
			if !inStringLiteral && strings.HasPrefix(t, "--") {
				// Could be a directive mid-statement, handle it
				d := strings.TrimPrefix(t, "--")
				d = strings.TrimSpace(d)
				if isDirectiveKeyword(d) {
					break
				}
			}

			// For echoing: if inside a string literal, preserve leading whitespace.
			// Otherwise, trim leading whitespace (mysqltest behavior).
			var rawEcho string
			stmtLine := t
			if inStringLiteral {
				rawEcho = stripCommentAfterDelimiter(strings.TrimRight(l, " \t\r\n"), delim)
				stmtLine = strings.TrimRight(l, " \t\r\n")
			} else {
				rawEcho = stripCommentAfterDelimiter(t, delim)
			}

			// Track string literal state: count unescaped single quotes on this line
			for ci := 0; ci < len(t); ci++ {
				if t[ci] == '\'' {
					// Check if escaped
					if ci > 0 && t[ci-1] == '\\' {
						continue
					}
					// Check for '' escape (two consecutive quotes)
					if ci+1 < len(t) && t[ci+1] == '\'' {
						ci++ // skip the next quote
						continue
					}
					inStringLiteral = !inStringLiteral
				}
			}
			// Strip inline comments (# outside quotes) for SQL processing
			if !inStringLiteral {
				t = stripInlineComment(t)
				stmtLine = t
			}

			// When using a custom delimiter, check if this line is a DELIMITER directive
			// that happens to end with the current delimiter (e.g., "DELIMITER ;//" when delim is "//")
			if ctx.delimiter != "" && delim != ";" {
				tLower := strings.ToLower(t)
				if strings.HasPrefix(tLower, "delimiter ") {
					// This is a DELIMITER directive, not a SQL statement
					// Extract the new delimiter value by stripping the current delimiter suffix
					newDelimVal := t
					if strings.HasSuffix(newDelimVal, delim) {
						newDelimVal = newDelimVal[:len(newDelimVal)-len(delim)]
					}
					newDelimVal = strings.TrimSpace(newDelimVal[len("delimiter "):])
					if newDelimVal == ";" {
						ctx.delimiter = ""
					} else {
						ctx.delimiter = newDelimVal
					}
					delim = ";"
					if ctx.delimiter != "" {
						delim = ctx.delimiter
					}
					i++
					break
				}
			}

			rawLines = append(rawLines, rawEcho)

			// Check delimiter on stripped line, but also on original trimmed line
			// (in case inline comment stripping removed the trailing delimiter)
			originalTrimmed := strings.TrimSpace(l)
			hasDelim := strings.HasSuffix(t, delim) || strings.HasSuffix(originalTrimmed, delim)
			if hasDelim {
				stmt += strings.TrimSuffix(stmtLine, delim)
				i++
				break
			}
			stmt += stmtLine + "\n"
			i++
		}

		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		// Handle multiple statements on one line (e.g. "DROP TABLE t1; SHOW TABLES")
		// When using a custom delimiter, don't split by semicolon
		var stmts []string
		if ctx.delimiter != "" {
			stmts = []string{stmt}
		} else {
			stmts = splitStatements(stmt)
		}

		if len(stmts) <= 1 {
			// Single statement: echo raw lines preserving original formatting
			if ctx.queryLogEnabled {
				for _, rl := range rawLines {
					// Apply --replace_result to echoed SQL too
					if len(ctx.replaceResult) > 0 {
						rl = applyReplaceResult(rl, ctx.replaceResult)
					}
					ctx.output.WriteString(rl + "\n")
				}
			}
			for _, s := range stmts {
				s = strings.TrimSpace(s)
				if s == "" {
					continue
				}
				err := ctx.executeSQLNoEcho(s)
				if err != nil {
					return err
				}
			}
		} else {
			// Multiple statements: echo and execute each individually
			for _, s := range stmts {
				s = strings.TrimSpace(s)
				if s == "" {
					continue
				}
				// Use executeSQL which does echo + execute
				err := ctx.executeSQL(s)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (ctx *execContext) handleDirective(directive string) (handled bool, skip bool, err error) {
	lower := strings.ToLower(directive)

	// Parse directive name and args. mysqltest allows forms like:
	//   --connect(...)
	//   --reap;
	// in addition to the usual "--name args".
	name, args := parseDirectiveNameArgs(directive)

	switch name {
	case "skip":
		return true, true, errSkipTest

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
	case "vertical_results":
		ctx.verticalResults = true
		return true, false, nil
	case "horizontal_results":
		ctx.verticalResults = false
		return true, false, nil

	case "sorted_result":
		ctx.sortResult = true
		return true, false, nil

	case "replace_result":
		// Parse pairs: --replace_result from1 to1 from2 to2 ...
		// Substitute variables first (e.g. $ENGINE -> InnoDB)
		ctx.replaceResult = parseReplacePairs(ctx.substituteVars(args))
		return true, false, nil

	case "let":
		return true, false, ctx.setVariable(args)

	case "source":
		err := ctx.sourceFile(args)
		if errors.Is(err, errSkipTest) {
			return true, true, errSkipTest
		}
		return true, false, err

	case "connect":
		connName, dbName := parseConnectDirectiveArgs(args)
		if connName == "" {
			return true, false, nil
		}
		key := strings.ToLower(connName)
		if existing := ctx.connByName[key]; existing != nil {
			existing.Close() //nolint:errcheck
			delete(ctx.connByName, key)
		}
		conn, err := ctx.db.Conn(context.Background())
		if err != nil {
			return true, false, err
		}
		if dbName != "" {
			if _, err := conn.ExecContext(context.Background(), fmt.Sprintf("USE `%s`", dbName)); err != nil {
				conn.Close() //nolint:errcheck
				return true, false, err
			}
		}
		ctx.connByName[key] = conn
		ctx.currentConn = key
		return true, false, nil

	case "connection":
		target := strings.TrimSpace(args)
		target = strings.Trim(target, "()")
		target = strings.TrimSpace(target)
		if target == "" || strings.EqualFold(target, "default") {
			ctx.currentConn = ""
			return true, false, nil
		}
		key := strings.ToLower(target)
		if _, ok := ctx.connByName[key]; ok {
			ctx.currentConn = key
		}
		return true, false, nil

	case "disconnect":
		target := strings.TrimSpace(args)
		target = strings.Trim(target, "()")
		target = strings.TrimSpace(target)
		if target == "" {
			target = ctx.currentConn
		}
		if target == "" || strings.EqualFold(target, "default") {
			return true, false, nil
		}
		key := strings.ToLower(target)
		if conn := ctx.connByName[key]; conn != nil {
			conn.Close() //nolint:errcheck
			delete(ctx.connByName, key)
		}
		if ctx.currentConn == key {
			ctx.currentConn = ""
		}
		return true, false, nil

	case "delimiter":
		newDelim := strings.TrimSpace(args)
		// If the current delimiter is non-standard and the args end with it,
		// strip the current delimiter suffix (e.g., ";//" when delimiter is "//" -> ";")
		if ctx.delimiter != "" && ctx.delimiter != ";" {
			if strings.HasSuffix(newDelim, ctx.delimiter) {
				newDelim = strings.TrimSpace(newDelim[:len(newDelim)-len(ctx.delimiter)])
			}
		}
		ctx.delimiter = newDelim
		if ctx.delimiter == ";" {
			ctx.delimiter = ""
		}
		return true, false, nil

	case "query", "eval", "query_vertical":
		// Execute the rest as SQL. query_vertical uses vertical key/value result formatting.
		if args != "" {
			args = stripInlineHashComments(args)
			args = strings.TrimSpace(args)
			args = strings.TrimSuffix(args, ";")
			if name == "eval" {
				// In eval context, undefined variables expand to empty string
				args = ctx.substituteVars(args)
				args = stripUndefinedVars(args)
				args = strings.TrimSpace(args)
			}
			if name == "query_vertical" {
				ctx.verticalResult = true
			}
			err := ctx.executeSQL(args)
			ctx.verticalResult = false
			if err == nil && name == "eval" && ctx.resultLogEnabled &&
				strings.Contains(strings.ToLower(args), "explain format=tree") {
				ctx.output.WriteString("\n")
			}
			return true, false, err
		}
		return true, false, nil

	case "shutdown_server":
		// Server restart is not supported - skip remaining lines in this block
		return true, true, nil

	case "exec", "execw":
		// Handle --exec echo "..." specially to produce output
		if strings.HasPrefix(args, "echo ") || strings.HasPrefix(args, "echo\t") {
			echoArg := strings.TrimSpace(args[5:])
			// Substitute variables
			echoArg = ctx.substituteVars(echoArg)
			// Check for output redirections (> file means output goes to file, not stdout)
			hasRedirect := false
			// Check for > outside of quotes
			inQuote := byte(0)
			for i := 0; i < len(echoArg); i++ {
				if echoArg[i] == '"' || echoArg[i] == '\'' {
					if inQuote == 0 {
						inQuote = echoArg[i]
					} else if inQuote == echoArg[i] {
						inQuote = 0
					}
				}
				if inQuote == 0 && echoArg[i] == '>' {
					hasRedirect = true
					echoArg = strings.TrimSpace(echoArg[:i])
					break
				}
			}
			// Strip surrounding quotes
			if len(echoArg) >= 2 {
				if (echoArg[0] == '"' && echoArg[len(echoArg)-1] == '"') ||
					(echoArg[0] == '\'' && echoArg[len(echoArg)-1] == '\'') {
					echoArg = echoArg[1 : len(echoArg)-1]
				}
			}
			// Only output if not redirected to a file
			if !hasRedirect && ctx.resultLogEnabled {
				ctx.output.WriteString(echoArg + "\n")
			}
			return true, false, nil
		}
		return true, false, nil

	case "remove_file":
		path := ctx.substituteVars(args)
		path = ctx.resolveFilePath(path)
		os.Remove(path) //nolint:errcheck
		return true, false, nil

	case "copy_file":
		if err := ctx.handleCopyFile(args); err != nil {
			if errors.Is(err, errSkipTest) {
				return true, true, nil
			}
			return true, false, err
		}
		return true, false, nil

	case "replace_column":
		// Parse column replacements: --replace_column N1 val1 N2 val2 ...
		ctx.replaceColumns = make(map[int]string)
		fields := strings.Fields(args)
		for i := 0; i+1 < len(fields); i += 2 {
			if colNum, err := strconv.Atoi(fields[i]); err == nil {
				ctx.replaceColumns[colNum] = fields[i+1]
			}
		}
		return true, false, nil

	case "inc":
		// Increment a variable: --inc $var
		varName := strings.TrimSpace(args)
		varName = strings.TrimRight(varName, ";")
		if ctx.variables != nil {
			if val, ok := ctx.variables[varName]; ok {
				if n, err2 := strconv.Atoi(val); err2 == nil {
					ctx.variables[varName] = strconv.Itoa(n + 1)
				}
			}
		}
		return true, false, nil

	case "dec":
		// Decrement a variable: --dec $var
		varName := strings.TrimSpace(args)
		varName = strings.TrimRight(varName, ";")
		if ctx.variables != nil {
			if val, ok := ctx.variables[varName]; ok {
				if n, err2 := strconv.Atoi(val); err2 == nil {
					ctx.variables[varName] = strconv.Itoa(n - 1)
				}
			}
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
		"send", "reap", "sleep", "send_shutdown",
		"replace_regex",
		"write_file", "append_file", "cat_file",
		"mkdir", "rmdir", "move_file",
		"list_files", "file_exists",
		"system",
		"die", "exit",
		"if", "while", "end",
		"require", "result_format",
		"disable_reconnect", "enable_reconnect",
		"disable_abort_on_error", "enable_abort_on_error",
		"real_sleep",
		"query_get_value",
		"save_master_pos", "sync_with_master",
		"change_user",
		"diff_files", "chmod",
		"remove_files", "remove_files_wildcard",
		"perl":
		return true, false, nil
	case "enable_info":
		ctx.infoEnabled = true
		return true, false, nil
	case "disable_info":
		ctx.infoEnabled = false
		return true, false, nil
	}

	// Check for --error shorthand without space
	if strings.HasPrefix(lower, "error") {
		ctx.expectedError = strings.TrimSpace(strings.TrimPrefix(lower, "error"))
		return true, false, nil
	}

	return false, false, nil
}

// handleIfBlock checks if the current line is an "if" construct and handles it.
// Returns (handled, skip, newIndex).
// "skip" is true when --skip was executed inside the block.
func (ctx *execContext) handleIfBlock(lines []string, i int) (handled bool, skip bool, newI int) {
	trimmed := strings.TrimSpace(lines[i])

	// Check for if directive: --if or bare if
	var condStr string
	if strings.HasPrefix(trimmed, "--") {
		d := strings.TrimSpace(strings.TrimPrefix(trimmed, "--"))
		dl := strings.ToLower(d)
		if !strings.HasPrefix(dl, "if ") && !strings.HasPrefix(dl, "if(") {
			return false, false, i
		}
		condStr = strings.TrimSpace(d[2:])
	} else if strings.HasPrefix(strings.ToLower(trimmed), "if (") || strings.HasPrefix(strings.ToLower(trimmed), "if(") {
		condStr = strings.TrimSpace(trimmed[2:])
	} else {
		return false, false, i
	}

	i++ // consume the if line

	// The condition may span multiple lines until we find the closing )
	// Collect the full condition
	for !isConditionComplete(condStr) && i < len(lines) {
		condStr += "\n" + strings.TrimSpace(lines[i])
		i++
	}

	// Now find the { and collect the block
	for i < len(lines) {
		t := strings.TrimSpace(lines[i])
		if t == "" || strings.HasPrefix(t, "#") {
			i++
			continue
		}
		if t == "{" {
			i++
			break
		}
		// Opening brace might be on the same line as if condition
		break
	}

	// Collect block lines
	var blockLines []string
	depth := 1
	for i < len(lines) && depth > 0 {
		t := strings.TrimSpace(lines[i])
		if t == "{" {
			depth++
		} else if t == "}" {
			depth--
			if depth == 0 {
				i++
				break
			}
		}
		if depth > 0 {
			blockLines = append(blockLines, lines[i])
		}
		i++
	}

	// Evaluate condition
	condResult := ctx.evaluateIfCondition(condStr)
	if condResult {
		err := ctx.executeLines(blockLines)
		if errors.Is(err, errSkipTest) {
			return true, true, i
		}
		if err != nil {
			// Non-skip error - continue past the block
			return true, false, i
		}
	}

	return true, false, i
}

func isConditionComplete(s string) bool {
	// Count parentheses
	depth := 0
	inBacktick := false
	for _, c := range s {
		if c == '`' {
			inBacktick = !inBacktick
		}
		if !inBacktick {
			if c == '(' {
				depth++
			} else if c == ')' {
				depth--
			}
		}
	}
	return depth <= 0
}

func isPerlBlockStart(trimmed string) bool {
	t := strings.TrimSpace(trimmed)
	if strings.HasPrefix(t, "--") {
		t = strings.TrimSpace(strings.TrimPrefix(t, "--"))
	}
	t = strings.ToLower(t)
	return t == "perl" || t == "perl;" || strings.HasPrefix(t, "perl ")
}

func skipPerlBlock(lines []string, i int) int {
	// mysqltest perl blocks usually end at a line containing "EOF",
	// optionally followed by a standalone ";" on the next line.
	i++
	for i < len(lines) {
		t := strings.TrimSpace(lines[i])
		if strings.EqualFold(t, "EOF") || strings.EqualFold(t, "EOF;") {
			i++
			if i < len(lines) && strings.TrimSpace(lines[i]) == ";" {
				i++
			}
			return i
		}
		i++
	}
	return i
}

var (
	perlBackupRe  = regexp.MustCompile(`(?i)ib_backup_tablespaces\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)`)
	perlRestoreRe = regexp.MustCompile(`(?i)ib_restore_tablespaces\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)`)
)

func (ctx *execContext) handlePerlBlock(lines []string, i int) int {
	j := i + 1
	for j < len(lines) {
		t := strings.TrimSpace(lines[j])
		if strings.EqualFold(t, "EOF") || strings.EqualFold(t, "EOF;") {
			j++
			if j < len(lines) && strings.TrimSpace(lines[j]) == ";" {
				j++
			}
			return j
		}
		if m := perlBackupRe.FindStringSubmatch(t); m != nil {
			dbName, tblName := m[1], m[2]
			ctx.captureTableSnapshot(dbName, tblName)
			ctx.output.WriteString("backup: " + tblName + "\n")
		}
		if m := perlRestoreRe.FindStringSubmatch(t); m != nil {
			dbName, tblName := m[1], m[2]
			ctx.restoreTableSnapshot(dbName, tblName)
			ctx.output.WriteString("restore: " + tblName + " .ibd and .cfg files\n")
		}
		j++
	}
	return j
}

func (ctx *execContext) captureTableSnapshot(dbName, tableName string) {
	query := fmt.Sprintf("SELECT * FROM `%s`.`%s`", dbName, tableName)
	rows, err := ctx.db.Query(query)
	if err != nil {
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return
	}
	snap := tableSnapshot{columns: cols, rows: make([][]interface{}, 0)}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return
		}
		copied := make([]interface{}, len(vals))
		for i, v := range vals {
			if bs, ok := v.([]byte); ok {
				copied[i] = string(bs)
			} else {
				copied[i] = v
			}
		}
		snap.rows = append(snap.rows, copied)
	}
	ctx.ttsBackups[strings.ToLower(dbName+"."+tableName)] = snap
}

func (ctx *execContext) restoreTableSnapshot(dbName, tableName string) {
	key := strings.ToLower(dbName + "." + tableName)
	snap, ok := ctx.ttsBackups[key]
	if !ok {
		return
	}
	if _, err := ctx.db.Exec(fmt.Sprintf("DELETE FROM `%s`.`%s`", dbName, tableName)); err != nil {
		return
	}
	if len(snap.columns) == 0 || len(snap.rows) == 0 {
		return
	}
	colParts := make([]string, len(snap.columns))
	qs := make([]string, len(snap.columns))
	for i, c := range snap.columns {
		colParts[i] = fmt.Sprintf("`%s`", c)
		qs[i] = "?"
	}
	insertSQL := fmt.Sprintf(
		"INSERT INTO `%s`.`%s` (%s) VALUES (%s)",
		dbName,
		tableName,
		strings.Join(colParts, ","),
		strings.Join(qs, ","),
	)
	for _, r := range snap.rows {
		if _, err := ctx.db.Exec(insertSQL, r...); err != nil {
			return
		}
	}
}

func isHereDocDirectiveStart(trimmed string) bool {
	t := strings.TrimSpace(trimmed)
	if strings.HasPrefix(t, "--") {
		t = strings.TrimSpace(strings.TrimPrefix(t, "--"))
	}
	name, _ := parseDirectiveNameArgs(t)
	return name == "write_file" || name == "append_file"
}

func skipHereDoc(lines []string, i int) int {
	i++
	for i < len(lines) {
		t := strings.TrimSpace(lines[i])
		if strings.EqualFold(t, "EOF") || strings.EqualFold(t, "EOF;") {
			return i + 1
		}
		i++
	}
	return i
}

func (ctx *execContext) handleHereDocDirective(lines []string, i int) (bool, int, error) {
	trimmed := strings.TrimSpace(lines[i])
	if !isHereDocDirectiveStart(trimmed) {
		return false, i, nil
	}

	directive := strings.TrimSpace(trimmed)
	if strings.HasPrefix(directive, "--") {
		directive = strings.TrimSpace(strings.TrimPrefix(directive, "--"))
	}
	name, args := parseDirectiveNameArgs(directive)
	if name != "write_file" && name != "append_file" {
		return false, i, nil
	}

	pathArg := strings.TrimSpace(strings.TrimRight(args, ";"))
	pathArg = ctx.substituteVars(pathArg)
	if pathArg == "" {
		return false, i, fmt.Errorf("%s requires a file path", name)
	}
	path := ctx.resolveFilePath(pathArg)

	j := i + 1
	var body []string
	for j < len(lines) {
		t := strings.TrimSpace(lines[j])
		if strings.EqualFold(t, "EOF") || strings.EqualFold(t, "EOF;") {
			break
		}
		body = append(body, lines[j])
		j++
	}
	if j >= len(lines) {
		return false, i, fmt.Errorf("%s: missing EOF terminator", name)
	}

	content := strings.Join(body, "\n")
	if len(body) > 0 {
		content += "\n"
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return false, i, err
	}
	if name == "append_file" {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return false, i, err
		}
		_, werr := f.WriteString(content)
		cerr := f.Close()
		if werr != nil {
			return false, i, werr
		}
		if cerr != nil {
			return false, i, cerr
		}
	} else {
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			return false, i, err
		}
	}

	return true, j + 1, nil
}

func (ctx *execContext) evaluateIfCondition(condStr string) bool {
	// Remove outer parentheses
	condStr = strings.TrimSpace(condStr)
	if strings.HasPrefix(condStr, "(") && strings.HasSuffix(condStr, ")") {
		condStr = condStr[1 : len(condStr)-1]
		condStr = strings.TrimSpace(condStr)
	}

	// Handle negation
	negated := false
	if strings.HasPrefix(condStr, "!") {
		negated = true
		condStr = strings.TrimSpace(condStr[1:])
	}

	evalCond := func() bool {
		// Check for backtick query expression
		return ctx.evaluateIfConditionInner(condStr)
	}

	result := evalCond()
	if negated {
		return !result
	}
	return result
}

func (ctx *execContext) evaluateIfConditionInner(condStr string) bool {
	// Check for backtick query expression
	if strings.HasPrefix(condStr, "`") && strings.HasSuffix(condStr, "`") {
		query := condStr[1 : len(condStr)-1]
		query = ctx.substituteVars(query)
		// Execute query and check result
		rows, err := ctx.db.Query(query)
		if err != nil {
			return false
		}
		defer rows.Close()
		if rows.Next() {
			var val interface{}
			if err := rows.Scan(&val); err != nil {
				return false
			}
			if val == nil {
				return false
			}
			s := fmt.Sprintf("%v", val)
			// In mysqltest, backtick returns "1" for true
			return s != "0" && s != "" && s != "FALSE"
		}
		return false
	}

	// Check for variable comparison
	condStr = ctx.substituteVars(condStr)

	// Simple variable truth check: $var
	if strings.HasPrefix(condStr, "$") {
		val, ok := ctx.variables[condStr]
		return ok && val != "" && val != "0"
	}

	// Numeric check
	n, err := strconv.Atoi(condStr)
	if err == nil {
		return n != 0
	}

	return condStr != "" && condStr != "0"
}

func formatResultCell(v interface{}) string {
	formatMySQLFloat := func(f float64, bitSize int) string {
		if math.IsNaN(f) {
			return "NaN"
		}
		if math.IsInf(f, 1) {
			return "inf"
		}
		if math.IsInf(f, -1) {
			return "-inf"
		}
		abs := math.Abs(f)
		if abs != 0 && (abs >= 1e14 || abs < 1e-4) {
			s := strconv.FormatFloat(f, 'e', 5, bitSize)
			s = strings.Replace(s, "e+0", "e", 1)
			s = strings.Replace(s, "e-0", "e-", 1)
			s = strings.Replace(s, "e+", "e", 1)
			return s
		}
		return strconv.FormatFloat(f, 'f', -1, bitSize)
	}
	normalizeScientific := func(s string) string {
		if !strings.ContainsAny(s, "eE") {
			return s
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return s
		}
		return formatMySQLFloat(f, 64)
	}
	switch val := v.(type) {
	case nil:
		return "NULL"
	case []byte:
		return normalizeScientific(string(val))
	case float64:
		return formatMySQLFloat(val, 64)
	case float32:
		return formatMySQLFloat(float64(val), 32)
	case string:
		return normalizeScientific(val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func (ctx *execContext) executeSQL(stmt string) error {
	// Variable substitution
	stmt = ctx.substituteVars(stmt)

	// Echo the SQL statement to output (mysqltest default behavior)
	if ctx.queryLogEnabled {
		echoLine := stmt + ";"
		if len(ctx.replaceResult) > 0 {
			echoLine = applyReplaceResult(echoLine, ctx.replaceResult)
		}
		ctx.output.WriteString(echoLine + "\n")
	}

	return ctx.executeSQLInner(stmt)
}

func (ctx *execContext) executeSQLNoEcho(stmt string) error {
	// Variable substitution
	stmt = ctx.substituteVars(stmt)
	return ctx.executeSQLInner(stmt)
}

func (ctx *execContext) executeSQLInner(stmt string) error {
	// Strip trailing # comments from SQL (MySQL treats # as line comment)
	lines := strings.Split(stmt, "\n")
	for i, l := range lines {
		if idx := strings.Index(l, " #"); idx >= 0 {
			// Only strip if not inside a string
			inStr := false
			for j := 0; j < idx; j++ {
				if l[j] == '\'' {
					inStr = !inStr
				}
			}
			if !inStr {
				lines[i] = l[:idx]
			}
		}
	}
	stmt = strings.TrimSpace(strings.Join(lines, "\n"))
	upper := strings.ToUpper(strings.TrimSpace(stmt))
	isQuery := strings.HasPrefix(upper, "SELECT") ||
		strings.HasPrefix(upper, "SHOW") ||
		strings.HasPrefix(upper, "DESCRIBE") ||
		strings.HasPrefix(upper, "DESC ") ||
		strings.HasPrefix(upper, "EXPLAIN") ||
		strings.HasPrefix(upper, "ANALYZE") ||
		strings.HasPrefix(upper, "CHECK ") ||
		strings.HasPrefix(upper, "CHECKSUM ") ||
		strings.HasPrefix(upper, "OPTIMIZE ") ||
		strings.HasPrefix(upper, "REPAIR ")

	// EXECUTE might be either a query or exec depending on the prepared statement
	if strings.HasPrefix(upper, "EXECUTE ") {
		return ctx.executeQueryOrExec(stmt)
	}
	// Parenthesized SELECT/UNION statements (e.g. "(SELECT ...) UNION ...")
	// are result-set statements even though they don't start with "SELECT".
	if strings.HasPrefix(upper, "(") {
		return ctx.executeQueryOrExec(stmt)
	}

	if isQuery {
		return ctx.executeQuery(stmt)
	}
	return ctx.executeExec(stmt)
}

func stripInlineHashComments(stmt string) string {
	lines := strings.Split(stmt, "\n")
	for i, l := range lines {
		if idx := strings.Index(l, " #"); idx >= 0 {
			inStr := false
			for j := 0; j < idx; j++ {
				if l[j] == '\'' {
					inStr = !inStr
				}
			}
			if !inStr {
				lines[i] = l[:idx]
			}
		}
	}
	return strings.Join(lines, "\n")
}

func (ctx *execContext) executeQuery(stmt string) error {
	useVertical := ctx.verticalResult || ctx.verticalResults

	activeConn := ctx.getActiveConn()
	var (
		rows *sql.Rows
		err  error
	)
	if activeConn != nil {
		rows, err = activeConn.QueryContext(context.Background(), stmt)
	} else {
		rows, err = ctx.db.Query(stmt)
	}
	if err != nil {
		if ctx.expectedError != "" {
			// Output the error message (mysqltest format)
			if ctx.resultLogEnabled {
				ctx.output.WriteString(formatMySQLError(err) + "\n")
			}
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

	// Skip output for statements that return no columns (e.g., SELECT INTO OUTFILE)
	if len(columns) == 0 {
		return nil
	}

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
			parts[i] = formatResultCell(v)
		}
		// Apply --replace_column
		if len(ctx.replaceColumns) > 0 {
			for colIdx, replacement := range ctx.replaceColumns {
				if colIdx >= 1 && colIdx <= len(parts) {
					parts[colIdx-1] = replacement
				}
			}
		}
		if useVertical {
			for i, col := range columns {
				resultLines = append(resultLines, col+"\t"+parts[i])
			}
		} else {
			resultLines = append(resultLines, strings.Join(parts, "\t"))
		}
	}

	// Apply --sorted_result
	if ctx.sortResult {
		sort.Strings(resultLines)
		ctx.sortResult = false
	}
	// Clear replace_columns after use
	ctx.replaceColumns = nil

	// Apply --replace_result to output
	if len(ctx.replaceResult) > 0 {
		for i, line := range resultLines {
			resultLines[i] = applyReplaceResult(line, ctx.replaceResult)
		}
		ctx.replaceResult = nil
	}

	if !useVertical {
		// Write column headers for regular (horizontal) results.
		ctx.output.WriteString(strings.Join(columns, "\t") + "\n")
	}

	for _, line := range resultLines {
		ctx.output.WriteString(line + "\n")
	}

	return nil
}

// executeQueryOrExec tries to execute a statement as a query first (returning rows),
// and falls back to exec if the result set has no columns (e.g. INSERT/UPDATE/DELETE).
func (ctx *execContext) executeQueryOrExec(stmt string) error {
	activeConn := ctx.getActiveConn()
	var (
		rows *sql.Rows
		err  error
	)
	if activeConn != nil {
		rows, err = activeConn.QueryContext(context.Background(), stmt)
	} else {
		rows, err = ctx.db.Query(stmt)
	}
	if err != nil {
		if ctx.expectedError != "" {
			if ctx.resultLogEnabled {
				ctx.output.WriteString(formatMySQLError(err) + "\n")
			}
			ctx.expectedError = ""
			return nil
		}
		return fmt.Errorf("query failed: %s: %v", stmt, err)
	}
	defer rows.Close()

	if ctx.expectedError != "" {
		ctx.expectedError = ""
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// If no columns, it's a non-SELECT statement (INSERT/UPDATE/DELETE)
	if len(columns) == 0 {
		return nil
	}

	if !ctx.resultLogEnabled {
		return nil
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
			parts[i] = formatResultCell(v)
		}
		// Apply --replace_column
		if len(ctx.replaceColumns) > 0 {
			for colIdx, replacement := range ctx.replaceColumns {
				if colIdx >= 1 && colIdx <= len(parts) {
					parts[colIdx-1] = replacement
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
	// Clear replace_columns after use
	ctx.replaceColumns = nil

	// Apply --replace_result to output
	if len(ctx.replaceResult) > 0 {
		for i, line := range resultLines {
			resultLines[i] = applyReplaceResult(line, ctx.replaceResult)
		}
		ctx.replaceResult = nil
	}

	for _, line := range resultLines {
		ctx.output.WriteString(line + "\n")
	}

	return nil
}

func (ctx *execContext) executeExec(stmt string) error {
	if ctx.expectedError != "" {
		// Use a dedicated connection so that if the statement succeeds,
		// we can retrieve warnings via SHOW WARNINGS on the same connection.
		return ctx.executeExecWithExpectedError(stmt)
	}
	activeConn := ctx.getActiveConn()
	var result sql.Result
	var err error
	if activeConn != nil {
		result, err = activeConn.ExecContext(context.Background(), stmt)
	} else {
		result, err = ctx.db.Exec(stmt)
	}
	if err != nil {
		return fmt.Errorf("exec failed: %s: %v", stmt, err)
	}
	if ctx.infoEnabled && result != nil {
		affected, _ := result.RowsAffected()
		ctx.output.WriteString(fmt.Sprintf("affected rows: %d\n", affected))
		upper := strings.ToUpper(strings.TrimSpace(stmt))
		if strings.HasPrefix(upper, "ALTER TABLE") || strings.HasPrefix(upper, "LOAD DATA") ||
			strings.HasPrefix(upper, "CREATE INDEX") || strings.HasPrefix(upper, "DROP INDEX") {
			ctx.output.WriteString(fmt.Sprintf("info: Records: %d  Duplicates: 0  Warnings: 0\n", affected))
		}
	}
	return nil
}

// executeExecWithExpectedError runs a statement on a single connection when
// --error was specified, so that SHOW WARNINGS can be issued on the same
// connection if the statement succeeds instead of returning an error.
func (ctx *execContext) executeExecWithExpectedError(stmt string) error {
	expectedCode := ctx.expectedError
	ctx.expectedError = ""

	conn := ctx.getActiveConn()
	ownedConn := false
	if conn == nil {
		var err error
		conn, err = ctx.db.Conn(context.Background())
		if err != nil {
			return fmt.Errorf("failed to acquire connection: %v", err)
		}
		ownedConn = true
	}
	if ownedConn {
		defer conn.Close()
	}

	_, execErr := conn.ExecContext(context.Background(), stmt)
	if execErr != nil {
		if ctx.resultLogEnabled {
			ctx.output.WriteString(formatMySQLError(execErr) + "\n")
		}
		return nil
	}

	// Statement succeeded but we expected an error — check for warnings
	// MySQL test framework shows warnings as errors in this case
	if ctx.resultLogEnabled {
		ctx.outputWarningsOnConn(conn, expectedCode)
	}
	return nil
}

// outputWarningsOnConn queries SHOW WARNINGS on a specific connection and
// outputs them in mysqltest format.
func (ctx *execContext) outputWarningsOnConn(conn *sql.Conn, expectedCode string) {
	rows, err := conn.QueryContext(context.Background(), "SHOW WARNINGS")
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var level, message string
		var code int
		if err := rows.Scan(&level, &code, &message); err != nil {
			continue
		}
		// Use SQLSTATE code: warnings use 01000, errors use the mapped SQLSTATE
		sqlstate := mysqlCodeToSQLState(code)
		ctx.output.WriteString(fmt.Sprintf("ERROR %s: %s\n", sqlstate, message))
		return // Only show first warning
	}
}

func (ctx *execContext) getActiveConn() *sql.Conn {
	if ctx.currentConn == "" {
		return nil
	}
	return ctx.connByName[strings.ToLower(ctx.currentConn)]
}

func (ctx *execContext) closeConnections() {
	for name, conn := range ctx.connByName {
		if conn != nil {
			conn.Close() //nolint:errcheck
		}
		delete(ctx.connByName, name)
	}
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
	// In mysqltest, 'let create = ...' defines '$create'. Ensure '$' prefix.
	if !strings.HasPrefix(name, "$") && !strings.HasPrefix(name, "@") {
		name = "$" + name
	}
	value := strings.TrimSpace(parts[1])

	// If value is wrapped in backticks, execute as SQL and use first column of first row
	if strings.HasPrefix(value, "`") && strings.HasSuffix(value, "`") {
		sqlStmt := strings.TrimPrefix(strings.TrimSuffix(value, "`"), "`")
		sqlStmt = ctx.substituteVars(strings.TrimSpace(sqlStmt))
		row := ctx.db.QueryRow(sqlStmt)
		var result string
		if err := row.Scan(&result); err != nil {
			// On error, keep existing value if set (e.g. $ENGINE default)
			if _, exists := ctx.variables[name]; !exists {
				ctx.variables[name] = ""
			}
			return nil
		}
		ctx.variables[name] = result
		return nil
	}

	// Apply variable substitution to the value
	value = ctx.substituteVars(value)

	// Handle query_get_value(SQL, colName, rowNum)
	if strings.HasPrefix(strings.ToLower(value), "query_get_value(") && strings.HasSuffix(value, ")") {
		inner := value[len("query_get_value(") : len(value)-1]
		inner = ctx.substituteVars(inner)
		// Parse from the end: last comma separates rowNum, second-to-last separates colName
		lastComma := strings.LastIndex(inner, ",")
		if lastComma < 0 {
			ctx.variables[name] = ""
			return nil
		}
		rowNumStr := strings.TrimSpace(inner[lastComma+1:])
		rest := inner[:lastComma]
		secondLastComma := strings.LastIndex(rest, ",")
		if secondLastComma < 0 {
			ctx.variables[name] = ""
			return nil
		}
		sqlStmt := strings.TrimSpace(rest[:secondLastComma])
		// rowNum is 1-based
		rowNum, _ := strconv.Atoi(rowNumStr)
		if rowNum < 1 {
			rowNum = 1
		}
		rows, err := ctx.db.Query(sqlStmt)
		if err != nil {
			ctx.variables[name] = ""
			return nil
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		// Find column index
		colName := strings.TrimSpace(rest[secondLastComma+1:])
		colIdx := -1
		for ci, cn := range cols {
			if strings.EqualFold(cn, colName) {
				colIdx = ci
				break
			}
		}
		if colIdx < 0 {
			colIdx = 0
		}
		rowCount := 0
		for rows.Next() {
			rowCount++
			vals := make([]interface{}, len(cols))
			ptrs := make([]interface{}, len(cols))
			for ci := range vals {
				ptrs[ci] = &vals[ci]
			}
			if err := rows.Scan(ptrs...); err != nil {
				continue
			}
			if rowCount == rowNum {
				v := vals[colIdx]
				if v == nil {
					ctx.variables[name] = "NULL"
				} else if bs, ok := v.([]byte); ok {
					ctx.variables[name] = string(bs)
				} else {
					ctx.variables[name] = fmt.Sprintf("%v", v)
				}
				return nil
			}
		}
		ctx.variables[name] = "No such row"
		return nil
	}

	ctx.variables[name] = value
	return nil
}

// evalMTRArithmetic tries to evaluate a simple arithmetic expression (like "5/2").
// Returns the result as string and true if successful.
func evalMTRArithmetic(expr string) (string, bool) {
	expr = strings.TrimSpace(expr)
	// Try simple integer
	if _, err := strconv.Atoi(expr); err == nil {
		return expr, true
	}
	// Try division
	if parts := strings.SplitN(expr, "/", 2); len(parts) == 2 {
		a, errA := strconv.Atoi(strings.TrimSpace(parts[0]))
		b, errB := strconv.Atoi(strings.TrimSpace(parts[1]))
		if errA == nil && errB == nil && b != 0 {
			return strconv.Itoa(a / b), true
		}
	}
	// Try multiplication
	if parts := strings.SplitN(expr, "*", 2); len(parts) == 2 {
		a, errA := strconv.Atoi(strings.TrimSpace(parts[0]))
		b, errB := strconv.Atoi(strings.TrimSpace(parts[1]))
		if errA == nil && errB == nil {
			return strconv.Itoa(a * b), true
		}
	}
	// Try addition
	if parts := strings.SplitN(expr, "+", 2); len(parts) == 2 {
		a, errA := strconv.Atoi(strings.TrimSpace(parts[0]))
		b, errB := strconv.Atoi(strings.TrimSpace(parts[1]))
		if errA == nil && errB == nil {
			return strconv.Itoa(a + b), true
		}
	}
	// Try subtraction (be careful not to match negative numbers)
	if idx := strings.LastIndex(expr, "-"); idx > 0 {
		a, errA := strconv.Atoi(strings.TrimSpace(expr[:idx]))
		b, errB := strconv.Atoi(strings.TrimSpace(expr[idx+1:]))
		if errA == nil && errB == nil {
			return strconv.Itoa(a - b), true
		}
	}
	return "", false
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

// applyMasterOpt parses a master.opt file and applies relevant options to the
// exec context variables (e.g., --innodb_page_size=32k sets $innodb_page_size).
func applyMasterOpt(content string, ctx *execContext) {
	for _, token := range strings.Fields(content) {
		token = strings.TrimPrefix(token, "--")
		if strings.Contains(token, "=") {
			parts := strings.SplitN(token, "=", 2)
			key := strings.TrimSpace(parts[0])
			val := strings.TrimSpace(parts[1])
			// Convert size suffixes (k, m, g)
			if strings.HasSuffix(strings.ToLower(val), "k") {
				if n, err := strconv.Atoi(val[:len(val)-1]); err == nil {
					val = strconv.Itoa(n * 1024)
				}
			} else if strings.HasSuffix(strings.ToLower(val), "m") {
				if n, err := strconv.Atoi(val[:len(val)-1]); err == nil {
					val = strconv.Itoa(n * 1024 * 1024)
				}
			}
			// Set as variable
			ctx.variables["$"+key] = val
			// Also apply server-level settings via SET
			if strings.EqualFold(key, "innodb_page_size") {
				ctx.db.Exec(fmt.Sprintf("SET GLOBAL innodb_page_size = %s", val)) //nolint:errcheck
			}
		}
	}
}

// stripUndefinedVars removes remaining $variable references that were not
// substituted. In MySQL's mysqltest, undefined variables in eval context
// expand to empty string.
func stripUndefinedVars(s string) string {
	// Match $identifier or ${identifier} patterns
	re := regexp.MustCompile(`\$\{[A-Za-z_][A-Za-z0-9_]*\}|\$[A-Za-z_][A-Za-z0-9_]*`)
	return re.ReplaceAllString(s, "")
}

func (ctx *execContext) sourceFile(filename string) error {
	filename = strings.TrimSpace(filename)
	// Apply variable substitution in filename
	filename = ctx.substituteVars(filename)
	baseName := strings.ToLower(filepath.Base(filename))
	// Treat proc-control include as no-op in this single-node runner.
	if baseName == "restart_mysqld.inc" {
		if ctx.resultLogEnabled {
			ctx.output.WriteString("# restart\n")
		}
		return nil
	}

	// Normalize common MySQL test suite paths
	// suite/engines/funcs/t/foo.inc -> just the basename (search in include paths)
	candidates := []string{filename}
	if strings.HasPrefix(filename, "suite/") {
		candidates = append(candidates, strings.TrimPrefix(filename, "suite/"))
	}
	base := filepath.Base(filename)
	if base != filename {
		candidates = append(candidates, base)
	}
	// Map suite/engines/funcs/ paths to engine_funcs/
	if strings.Contains(filename, "suite/engines/funcs/") {
		mapped := strings.Replace(filename, "suite/engines/funcs/", "engine_funcs/", 1)
		candidates = append(candidates, mapped)
	}

	// Try include paths
	for _, candidate := range candidates {
		for _, dir := range ctx.runner.IncludePaths {
			path := filepath.Join(dir, candidate)
			if _, err := os.Stat(path); err == nil {
				lines, err := readLines(path)
				if err != nil {
					return err
				}
				return ctx.executeLines(lines)
			}
		}
	}
	// File not found - skip silently (many includes are optional)
	return nil
}

// Helper functions

func readLines(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// Detect if the file is SJIS/CP932 encoded by checking first few lines for
	// --character_set sjis or --charset sjis directive.
	// SJIS bytes can contain 0x5C (backslash) which breaks SQL parsing.
	if isSJISEncoded(data) {
		decoded, err := decodeSJIS(data)
		if err == nil {
			data = decoded
		}
	} else if isEUCJPEncoded(data) {
		decoded, err := decodeEUCJP(data)
		if err == nil {
			data = decoded
		}
	}

	var lines []string
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024) // 1MB buffer
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

// isSJISEncoded checks if the file content starts with a SJIS charset directive.
func isSJISEncoded(data []byte) bool {
	// Check first few lines for --character_set sjis or --charset sjis
	header := data
	if len(header) > 512 {
		header = header[:512]
	}
	lower := bytes.ToLower(header)
	return bytes.Contains(lower, []byte("character_set sjis")) ||
		bytes.Contains(lower, []byte("charset sjis"))
}

// isEUCJPEncoded checks if the file content starts with a UJIS/EUCJP charset directive.
func isEUCJPEncoded(data []byte) bool {
	header := data
	if len(header) > 512 {
		header = header[:512]
	}
	lower := bytes.ToLower(header)
	return bytes.Contains(lower, []byte("character_set ujis")) ||
		bytes.Contains(lower, []byte("charset ujis")) ||
		bytes.Contains(lower, []byte("character_set eucjpms")) ||
		bytes.Contains(lower, []byte("charset eucjpms"))
}

// decodeSJIS converts SJIS/CP932 encoded bytes to UTF-8.
func decodeSJIS(data []byte) ([]byte, error) {
	reader := transform.NewReader(bytes.NewReader(data), japanese.ShiftJIS.NewDecoder())
	return readAll(reader)
}

// decodeEUCJP converts EUC-JP encoded bytes to UTF-8.
func decodeEUCJP(data []byte) ([]byte, error) {
	reader := transform.NewReader(bytes.NewReader(data), japanese.EUCJP.NewDecoder())
	return readAll(reader)
}

// readAll reads all bytes from a reader.
func readAll(r *transform.Reader) ([]byte, error) {
	var buf bytes.Buffer
	_, err := buf.ReadFrom(r)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// isValidUTF8 checks if bytes are valid UTF-8 using utf8.Valid.
func isValidUTF8(data []byte) bool {
	return utf8.Valid(data)
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
	"remove_files": true, "remove_files_wildcard": true,
	"exec": true, "execw": true, "system": true,
	"perl": true, "if": true, "while": true, "inc": true, "dec": true,
	"disable_abort_on_error": true, "enable_abort_on_error": true,
	"real_sleep": true, "query_get_value": true,
	"save_master_pos": true, "sync_with_master": true,
	"result_format": true, "change_user": true,
	"disable_metadata": true, "enable_metadata": true,
	"disable_info": true, "enable_info": true,
	"shutdown_server":   true,
	"send_shutdown":     true,
	"disable_reconnect": true, "enable_reconnect": true,
	"query_vertical": true,
	"require":        true,
}

func isDirectiveKeyword(s string) bool {
	name, _ := parseDirectiveNameArgs(s)
	return directiveKeywords[name]
}

// barePrefixKeywords are mysqltest commands that may appear without a leading "--".
var barePrefixKeywords = []string{
	"eval ",
	"let ",
	"echo ",
	"source ",
	"require ",
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
	"remove_files ",
	"remove_files_wildcard ",
	"move_file ",
	"list_files ",
	"file_exists ",
	"chmod ",
	"query_get_value",
	"save_master_pos",
	"sync_with_master",
	"disable_info",
	"enable_info",
	"shutdown_server",
	"shutdown_server ",
	"disable_reconnect",
	"enable_reconnect",
	"query_vertical ",
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

	// Also support bare parenthesized form, e.g.:
	//   connect(con1,localhost,root,,test);
	//   connection(default);
	name, args := parseDirectiveNameArgs(stripped)
	if name != "" && directiveKeywords[name] && strings.HasPrefix(strings.TrimSpace(lower), name+"(") {
		if args != "" {
			return name + " " + args, true
		}
		return name, true
	}
	return "", false
}

func parseDirectiveNameArgs(directive string) (name, args string) {
	d := strings.TrimSpace(directive)
	if d == "" {
		return "", ""
	}

	// Support parenthesized forms like "connect(...)".
	// If both '(' and whitespace exist, whichever appears first defines the split.
	wsIdx := strings.IndexAny(d, " \t")
	parIdx := strings.IndexByte(d, '(')
	if parIdx >= 0 && (wsIdx < 0 || parIdx < wsIdx) {
		name = strings.ToLower(strings.TrimSpace(d[:parIdx]))
		args = strings.TrimSpace(d[parIdx:])
		return strings.TrimRight(name, ";"), args
	}

	// Otherwise prefer the first whitespace separator when present.
	if wsIdx >= 0 {
		name = strings.ToLower(strings.TrimSpace(d[:wsIdx]))
		args = strings.TrimSpace(d[wsIdx+1:])
		return strings.TrimRight(name, ";"), args
	}

	// Support trailing semicolon in no-arg form, e.g. "reap;".
	return strings.ToLower(strings.TrimRight(d, ";")), ""
}

func parseConnectDirectiveArgs(args string) (connName string, dbName string) {
	trimmed := strings.TrimSpace(args)
	trimmed = strings.TrimSuffix(trimmed, ";")
	trimmed = strings.TrimSpace(trimmed)
	if strings.HasPrefix(trimmed, "(") && strings.HasSuffix(trimmed, ")") {
		trimmed = strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	}
	if trimmed == "" {
		return "", ""
	}

	parts := strings.Split(trimmed, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
		parts[i] = strings.Trim(parts[i], "`\"'")
	}
	connName = parts[0]
	if len(parts) >= 5 {
		dbName = parts[4]
	}
	return connName, dbName
}

// formatMySQLError formats an error into mysqltest expected format.
// MySQL errors look like: "ERROR SQLSTATE (ERRNO): message"
// e.g. "ERROR 23000: Column 'c1' cannot be null"
func formatMySQLError(err error) string {
	msg := err.Error()
	// go-mysql-org errors have format: "Error <code> (<state>): <message>"
	re := regexp.MustCompile(`Error (\d+) \(([^)]+)\): (.*)`)
	if m := re.FindStringSubmatch(msg); m != nil {
		innerMsg := m[3]
		// Handle "ERROR XXXX (YYYY): message" format from mylite error wrapping
		innerRe := regexp.MustCompile(`ERROR (\d+) \(([^)]+)\): (.*)`)
		if im := innerRe.FindStringSubmatch(innerMsg); im != nil {
			return fmt.Sprintf("ERROR %s: %s", im[2], im[3])
		}
		return fmt.Sprintf("ERROR %s: %s", m[2], m[3])
	}
	// Handle bare "ERROR XXXX (YYYY): message" format (no outer Error wrapper)
	bareRe := regexp.MustCompile(`ERROR (\d+) \(([^)]+)\): (.*)`)
	if m := bareRe.FindStringSubmatch(msg); m != nil {
		return fmt.Sprintf("ERROR %s: %s", m[2], m[3])
	}
	return "ERROR HY000: " + msg
}

// mysqlCodeToSQLState maps a MySQL error code to its SQLSTATE.
// Most warnings use the generic SQLSTATE 01000.
func mysqlCodeToSQLState(code int) string {
	// Common warning/error code to SQLSTATE mappings
	switch code {
	case 1048:
		return "23000"
	case 1062:
		return "23000"
	case 1264:
		return "22003"
	case 1265:
		return "01000"
	case 1366:
		return "HY000"
	default:
		// Warnings (codes typically in 1000-1999 range) default to 01000
		return "01000"
	}
}

// parseReplacePairs parses --replace_result arguments into pairs of [from, to].
func parseReplacePairs(args string) []string {
	// Arguments are space-separated pairs. Quoted strings are supported.
	var result []string
	i := 0
	for i < len(args) {
		// Skip spaces
		for i < len(args) && args[i] == ' ' {
			i++
		}
		if i >= len(args) {
			break
		}
		ch := args[i]
		if ch == '"' {
			// Quoted string
			i++
			start := i
			for i < len(args) && args[i] != '"' {
				i++
			}
			result = append(result, args[start:i])
			if i < len(args) {
				i++ // skip closing quote
			}
		} else {
			// Unquoted token
			start := i
			for i < len(args) && args[i] != ' ' {
				i++
			}
			result = append(result, args[start:i])
		}
	}
	return result
}

// applyReplaceResult applies --replace_result substitutions to a line.
func applyReplaceResult(line string, pairs []string) string {
	for i := 0; i+1 < len(pairs); i += 2 {
		from := pairs[i]
		to := pairs[i+1]
		if from != "" {
			line = strings.ReplaceAll(line, from, to)
		}
	}
	return line
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
// stripCommentAfterDelimiter strips inline comments (#) that appear AFTER the
// statement delimiter. Comments before the delimiter are preserved for echoing.
// e.g. "SET TIMESTAMP=1; # comment" → "SET TIMESTAMP=1;"
// e.g. "ALTER FUNCTION sf1 #DET# ;" → "ALTER FUNCTION sf1 #DET# ;" (preserved)
func stripCommentAfterDelimiter(line, delim string) string {
	// Find the delimiter position (respecting quotes)
	delimIdx := -1
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
		}
		if !inSingle && !inDouble && strings.HasPrefix(line[i:], delim) {
			delimIdx = i
			// Don't break -- find the LAST delimiter on the line
		}
	}
	if delimIdx < 0 {
		// No delimiter found; strip comment as usual
		return stripInlineComment(line)
	}
	// Keep everything up to and including the delimiter
	afterDelim := line[delimIdx+len(delim):]
	return strings.TrimRight(line[:delimIdx+len(delim)]+stripInlineComment(afterDelim), " \t")
}

func stripInlineComment(line string) string {
	inSingle := false
	inDouble := false
	inBacktick := false
	for i, ch := range line {
		switch ch {
		case '\'':
			if !inDouble && !inBacktick {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle && !inBacktick {
				inDouble = !inDouble
			}
		case '`':
			if !inSingle && !inDouble {
				inBacktick = !inBacktick
			}
		case '#':
			if !inSingle && !inDouble && !inBacktick {
				return strings.TrimSpace(line[:i])
			}
		}
	}
	return line
}

// normalizeFuncCase normalizes SQL function names in column headers
// to be case-insensitive. MySQL preserves original case, vitess uppercases.
func normalizeFuncCase(s string) string {
	// Common SQL functions that appear as column headers
	funcs := []string{
		"COUNT", "SUM", "AVG", "MIN", "MAX",
		"CONCAT", "SUBSTR", "SUBSTRING", "LEFT", "RIGHT",
		"UPPER", "LOWER", "LENGTH", "TRIM", "REPLACE",
		"IFNULL", "COALESCE", "NULLIF", "IF",
		"HEX", "UNHEX", "CAST", "CONVERT",
	}
	for _, fn := range funcs {
		lower := strings.ToLower(fn)
		s = strings.ReplaceAll(s, lower+"(", fn+"(")
	}
	return s
}

func normalizeOutput(s string) string {
	// Normalize line endings and trailing whitespace
	lines := strings.Split(s, "\n")
	var result []string
	for _, line := range lines {
		result = append(result, strings.TrimRight(line, " \t\r"))
	}
	out := strings.TrimRight(strings.Join(result, "\n"), "\n")
	// Normalize TRIM display: MySQL sometimes omits space before FROM for certain multibyte chars.
	out = normalizeTrimFromSpacing(out)
	// Normalize SUBSTRING display: MySQL shows "SUBSTRING(col FROM pos)" but vitess shows "SUBSTRING(col,pos)"
	out = normalizeSubstringDisplay(out)
	// Normalize case of "using" keyword: MySQL sometimes shows "using" lowercase, sometimes "USING" uppercase.
	// Normalize all to lowercase for consistent comparison.
	out = strings.ReplaceAll(out, " USING ", " using ")
	// Normalize TRIM keywords: vitess outputs lowercase trailing/leading/both, MySQL uppercase
	out = strings.ReplaceAll(out, "TRAILING ", "trailing ")
	out = strings.ReplaceAll(out, "LEADING ", "leading ")
	out = strings.ReplaceAll(out, "BOTH ", "both ")
	// Normalize function name case: MySQL preserves original query case for function names,
	// but vitess normalizes to lowercase. Lowercase all function names for comparison.
	out = normalizeFunctionNameCase(out)
	return out
}

// normalizeFunctionNameCase lowercases common MySQL function names in the output
// for consistent comparison (MySQL preserves query case, vitess normalizes to lowercase).
func normalizeFunctionNameCase(s string) string {
	// Sorted by length descending to avoid partial matches (e.g., TRIM inside LTRIM)
	funcNames := []string{
		"COUNT", "MIN", "MAX", "SUM", "AVG",
		"CHARACTER_LENGTH", "OCTET_LENGTH", "CHAR_LENGTH",
		"CONCAT_WS", "SUBSTRING", "COALESCE",
		"CONVERT", "CHARSET", "REVERSE", "REPLACE",
		"LOCATE", "CONCAT", "INSERT", "IFNULL", "NULLIF",
		"LENGTH", "SUBSTR", "INSTR", "UPPER", "LOWER",
		"UCASE", "LCASE", "UNHEX",
		"LTRIM", "RTRIM", "RIGHT", "RPAD", "LPAD",
		"TRIM", "LEFT",
		"HEX", "IF",
	}
	for _, fn := range funcNames {
		lower := strings.ToLower(fn)
		// Only replace when followed by ( to avoid replacing in data
		s = strings.ReplaceAll(s, fn+"(", lower+"(")
	}
	return s
}

// normalizeSubstringDisplay normalizes SUBSTRING display between MySQL and vitess formats.
// MySQL: "SUBSTRING(col FROM pos)" or "SUBSTRING(col FROM pos FOR len)"
// Vitess: "SUBSTRING(col,pos)" or "SUBSTRING(col,pos,len)"
// Normalize both to the comma-separated form.
func normalizeSubstringDisplay(s string) string {
	// Replace "SUBSTRING(xxx FROM yyy)" or "substring(xxx FROM yyy)" with "substring(xxx,yyy)"
	re := regexp.MustCompile(`(?i)SUBSTRING\(([^)]+?) FROM ([^)]+?)\)`)
	s = re.ReplaceAllStringFunc(s, func(match string) string {
		m := re.FindStringSubmatch(match)
		if m == nil {
			return match
		}
		inner := m[1]
		rest := m[2]
		// Handle "FROM x FOR y" -> "x,y"
		if idx := strings.Index(rest, " FOR "); idx >= 0 {
			return "substring(" + inner + "," + rest[:idx] + "," + rest[idx+5:] + ")"
		}
		return "substring(" + inner + "," + rest + ")"
	})
	return s
}

// normalizeTrimFromSpacing ensures consistent spacing before FROM in TRIM expressions
// and normalizes trailing spaces in function argument lists that MySQL adds for display alignment.
func normalizeTrimFromSpacing(s string) string {
	// Normalize 'FROM -> ' FROM (MySQL sometimes omits space before FROM)
	re := regexp.MustCompile(`'FROM `)
	s = re.ReplaceAllString(s, "' FROM ")
	// Normalize trailing spaces before closing paren in column headers
	// MySQL adds display-width padding spaces for CJK characters
	// e.g., "'丂丂' )" -> "'丂丂')"
	reSp := regexp.MustCompile(`' \)`)
	s = reSp.ReplaceAllString(s, "')")
	return s
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
	out := strings.Join(result, "\n")
	// Normalize ENGINE placeholders and non-InnoDB engines (we only support InnoDB)
	out = strings.ReplaceAll(out, "ENGINE=ENGINE", "ENGINE=InnoDB")
	out = strings.ReplaceAll(out, "ENGINE=MyISAM", "ENGINE=InnoDB")
	out = strings.ReplaceAll(out, "ENGINE=MEMORY", "ENGINE=InnoDB")
	return strings.TrimRight(out, "\n")
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

// resolveFilePath resolves a file path, substituting variables and making absolute.
func (ctx *execContext) resolveFilePath(path string) string {
	path = ctx.substituteVars(path)
	path = strings.TrimSpace(path)
	if !filepath.IsAbs(path) {
		// Build candidate paths with MySQL test suite path mappings
		candidates := []string{path}
		if strings.Contains(path, "suite/engines/funcs/") {
			mapped := strings.Replace(path, "suite/engines/funcs/", "engine_funcs/", 1)
			candidates = append(candidates, mapped)
		}
		candidates = append(candidates, filepath.Base(path))

		// Try to resolve relative to search paths
		for _, candidate := range candidates {
			for _, dir := range ctx.runner.IncludePaths {
				full := filepath.Join(dir, candidate)
				if _, err := os.Stat(full); err == nil {
					return full
				}
			}
		}
		// Default to tmpDir
		if ctx.tmpDir != "" {
			return filepath.Join(ctx.tmpDir, path)
		}
	}
	return path
}

// handleCopyFile implements the --copy_file directive.
func (ctx *execContext) handleCopyFile(args string) error {
	args = ctx.substituteVars(args)
	parts := strings.Fields(args)
	if len(parts) < 2 {
		return fmt.Errorf("copy_file requires 2 arguments: source dest")
	}
	src := ctx.resolveFilePath(parts[0])
	dst := ctx.resolveFilePath(parts[1])

	data, err := os.ReadFile(src)
	if err != nil && os.IsNotExist(err) {
		// Some tests refer to $MYSQLTEST_VARDIR/std_data, while fixtures live under files/std_data.
		if alt, ok := ctx.findStdDataFile(filepath.Base(src)); ok {
			src = alt
			data, err = os.ReadFile(src)
		}
	}
	if err != nil {
		// Physical InnoDB file operations are unsupported in this runner.
		// Skip the whole test instead of reporting an execution error.
		if os.IsNotExist(err) && isInnoDBPhysicalFile(src) {
			return errSkipTest
		}
		return fmt.Errorf("copy_file: cannot read source '%s': %v", src, err)
	}
	// Ensure destination directory exists
	os.MkdirAll(filepath.Dir(dst), 0755) //nolint:errcheck
	return os.WriteFile(dst, data, 0644)
}

func (ctx *execContext) findStdDataFile(base string) (string, bool) {
	candidates := make([]string, 0, len(ctx.runner.IncludePaths)*2)
	for _, dir := range ctx.runner.IncludePaths {
		candidates = append(candidates,
			filepath.Join(dir, "std_data", base),
			filepath.Join(filepath.Dir(dir), "std_data", base),
		)
	}
	for _, candidate := range candidates {
		if st, err := os.Stat(candidate); err == nil && !st.IsDir() {
			return candidate, true
		}
	}
	return "", false
}

func isInnoDBPhysicalFile(path string) bool {
	lower := strings.ToLower(path)
	return strings.HasSuffix(lower, ".ibd") ||
		strings.HasSuffix(lower, ".cfg") ||
		strings.HasSuffix(lower, ".cfp") ||
		strings.HasSuffix(lower, ".sdi")
}
