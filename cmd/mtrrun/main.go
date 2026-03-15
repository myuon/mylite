// mtrrun executes MySQL Test Run (.test) files against mylite.
// Usage:
//
//	mtrrun [flags] <suite> [testname]
//	mtrrun [flags] <path/to/test.test>
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/executor"
	"github.com/myuon/mylite/mtrrunner"
	"github.com/myuon/mylite/server"
	"github.com/myuon/mylite/storage"
)

func main() {
	suiteRoot := flag.String("suite-root", "testdata/dolt-mysql-tests/files/suite", "root directory for test suites")
	includeRoot := flag.String("include-root", "testdata/dolt-mysql-tests/files/include", "root directory for include files")
	verbose := flag.Bool("verbose", false, "verbose output")
	maxTests := flag.Int("max", 0, "maximum number of tests to run (0=all)")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "Usage: mtrrun [flags] <suite|test.test> [testname]")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Start mylite server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("failed to find free port: %v", err)
	}
	addr := listener.Addr().String()
	listener.Close()

	cat := catalog.New()
	store := storage.NewEngine()
	exec := executor.New(cat, store)
	srv := server.New(exec, addr)
	go func() {
		srv.Start() //nolint:errcheck
	}()
	defer srv.Close()

	// Wait for server
	var db *sql.DB
	for i := 0; i < 50; i++ {
		db, err = sql.Open("mysql", fmt.Sprintf("root:@tcp(%s)/test", addr))
		if err == nil {
			err = db.Ping()
			if err == nil {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		log.Fatalf("failed to connect to mylite: %v", err)
	}
	defer db.Close()

	runner := &mtrrunner.Runner{
		DB: db,
		IncludePaths: []string{
			*includeRoot,
		},
		Verbose: *verbose,
	}

	target := args[0]

	// Check if it's a direct .test file path
	if strings.HasSuffix(target, ".test") {
		result := runner.RunFile(target)
		printResult(result, *verbose)
		if !result.Passed && !result.Skipped {
			os.Exit(1)
		}
		return
	}

	// Otherwise, treat as suite name
	suiteDir := filepath.Join(*suiteRoot, target)
	if _, err := os.Stat(suiteDir); os.IsNotExist(err) {
		log.Fatalf("suite directory not found: %s", suiteDir)
	}

	// Add suite-specific includes
	suiteInclude := filepath.Join(suiteDir, "include")
	if _, err := os.Stat(suiteInclude); err == nil {
		runner.IncludePaths = append(runner.IncludePaths, suiteInclude)
	}

	// Specific test within suite?
	testFilter := ""
	if len(args) > 1 {
		testFilter = args[1]
	}

	// Discover tests
	testDir := filepath.Join(suiteDir, "t")
	entries, err := os.ReadDir(testDir)
	if err != nil {
		log.Fatalf("cannot read test dir: %v", err)
	}

	var passed, failed, skipped, errors int
	total := 0

	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".test") {
			continue
		}
		testName := strings.TrimSuffix(entry.Name(), ".test")
		if testFilter != "" && testName != testFilter {
			continue
		}

		if *maxTests > 0 && total >= *maxTests {
			break
		}
		total++

		// Each test gets a fresh DB state - reconnect to reset
		// For now, just DROP all tables before each test
		resetDB(db)

		testPath := filepath.Join(testDir, entry.Name())
		result := runner.RunFile(testPath)
		printResult(result, *verbose)

		switch {
		case result.Skipped:
			skipped++
		case result.Passed:
			passed++
		case result.Error != "":
			errors++
		default:
			failed++
		}
	}

	fmt.Printf("\n=== Summary ===\n")
	fmt.Printf("Total: %d, Passed: %d, Failed: %d, Skipped: %d, Errors: %d\n",
		total, passed, failed, skipped, errors)

	if failed+errors > 0 {
		os.Exit(1)
	}
}

func printResult(r mtrrunner.TestResult, verbose bool) {
	if r.Skipped {
		fmt.Printf("SKIP  %s\n", r.Name)
		return
	}
	if r.Passed {
		fmt.Printf("PASS  %s\n", r.Name)
		return
	}
	if r.Error != "" {
		fmt.Printf("ERROR %s: %s\n", r.Name, r.Error)
		if verbose && r.Output != "" {
			fmt.Printf("  Output:\n%s\n", indent(r.Output))
		}
		return
	}
	fmt.Printf("FAIL  %s\n", r.Name)
	if r.Diff != "" {
		fmt.Printf("%s\n", indent(r.Diff))
	}
}

func indent(s string) string {
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		lines[i] = "  " + l
	}
	return strings.Join(lines, "\n")
}

func resetDB(db *sql.DB) {
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			tables = append(tables, name)
		}
	}
	for _, t := range tables {
		db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", t)) //nolint:errcheck
	}
}
