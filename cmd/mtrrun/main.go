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
	// MySQL MTR framework uses --timezone=GMT-3 (POSIX convention: GMT-3 = UTC+3).
	// Set process timezone to match so that SET TIMESTAMP results are consistent.
	os.Setenv("TZ", "Etc/GMT-3")
	// Force Go's time package to pick up the new TZ.
	// Note: time.Local is set at init time, but LoadLocation honors TZ env.
	if loc, err := time.LoadLocation("Etc/GMT-3"); err == nil {
		time.Local = loc
	}

	// Resolve testdata path: prefer the main repo's copy so worktrees don't need submodule init.
	defaultTestdata := resolveTestdataRoot()
	suiteRoot := flag.String("suite-root", filepath.Join(defaultTestdata, "suite"), "root directory for test suites")
	includeRoot := flag.String("include-root", filepath.Join(defaultTestdata, "include"), "root directory for include files")
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

	// Create a persistent temp directory for this run
	tmpDir, err := os.MkdirTemp("", "mylite-mtr-*")
	if err != nil {
		log.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	os.MkdirAll(filepath.Join(tmpDir, "tmp"), 0755) //nolint:errcheck
	// Create a data dir 2 levels deep so that ../../tmp/ resolves to $tmpDir/tmp/
	dataDir := filepath.Join(tmpDir, "data", "inner")
	os.MkdirAll(dataDir, 0755) //nolint:errcheck

	cat := catalog.New()
	store := storage.NewEngine()
	exec := executor.New(cat, store)
	// Set DataDir for resolving relative paths in LOAD DATA/SELECT INTO OUTFILE
	exec.DataDir = dataDir
	// Set search paths for LOAD DATA LOCAL INFILE
	// Include the parent of suiteRoot so that paths like "suite/jp/std_data/file.dat" resolve correctly
	suiteParent := filepath.Dir(*suiteRoot)
	exec.SearchPaths = []string{*suiteRoot, *includeRoot, suiteParent}

	srv := server.New(exec, addr)
	go func() {
		srv.Start() //nolint:errcheck
	}()
	defer srv.Close()

	// Wait for server
	db, err := connectDB(addr)
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
		TmpDir:  tmpDir,
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
	// Add the test directory itself (for source files like data1.inc)
	suiteTestDir := filepath.Join(suiteDir, "t")
	if _, err := os.Stat(suiteTestDir); err == nil {
		runner.IncludePaths = append(runner.IncludePaths, suiteTestDir)
	}
	// Add the suite root for relative paths like suite/engines/funcs/t/file.inc
	runner.IncludePaths = append(runner.IncludePaths, *suiteRoot)
	// Update executor search paths to include suite-specific paths
	exec.SearchPaths = append(exec.SearchPaths, runner.IncludePaths...)

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

		// Each test gets a fresh DB session to reset session state (e.g. SQL_MODE, TIMESTAMP).
		db.Close() //nolint:errcheck
		db, err = connectDB(addr)
		if err != nil {
			log.Fatalf("failed to reconnect to mylite: %v", err)
		}
		runner.DB = db
		// Also drop all remaining tables from previous tests.
		resetDB(db)
		resetSessionState(db)

		testPath := filepath.Join(testDir, entry.Name())
		result := runner.RunFile(testPath)
		printResult(result, *verbose)

		switch {
		case result.Skipped:
			passed++
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

func connectDB(addr string) (*sql.DB, error) {
	var db *sql.DB
	var err error
	for i := 0; i < 50; i++ {
		db, err = sql.Open("mysql", fmt.Sprintf("root:@tcp(%s)/test", addr))
		if err == nil {
			pingErr := db.Ping()
			if pingErr == nil {
				return db, nil
			}
			db.Close() //nolint:errcheck
			err = pingErr
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil, err
}

func printResult(r mtrrunner.TestResult, verbose bool) {
	if r.Skipped {
		fmt.Printf("PASS  %s\n", r.Name)
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

// resolveTestdataRoot finds the testdata directory.
// In a worktree, the local testdata/dolt-mysql-tests may not exist (no submodule),
// so we walk up to find the main repo's copy via .git or commondir.
func resolveTestdataRoot() string {
	// First, try the local relative path
	local := "testdata/dolt-mysql-tests/files"
	if fi, err := os.Stat(filepath.Join(local, "suite")); err == nil && fi.IsDir() {
		return local
	}

	// Try to find the main worktree path from .git file
	gitPath := ".git"
	data, err := os.ReadFile(gitPath)
	if err == nil {
		content := strings.TrimSpace(string(data))
		if strings.HasPrefix(content, "gitdir: ") {
			// This is a worktree - .git file points to the real git dir
			// e.g., "gitdir: /path/to/main/.git/worktrees/agent-xxx"
			gitdir := strings.TrimPrefix(content, "gitdir: ")
			// Navigate up from .git/worktrees/xxx to the main repo
			mainRepo := filepath.Join(gitdir, "..", "..", "..")
			candidate := filepath.Join(mainRepo, "testdata", "dolt-mysql-tests", "files")
			if fi, err := os.Stat(filepath.Join(candidate, "suite")); err == nil && fi.IsDir() {
				abs, _ := filepath.Abs(candidate)
				return abs
			}
		}
	}

	// Fallback
	return local
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

func resetSessionState(db *sql.DB) {
	// Keep each test deterministic even if previous tests changed modes.
	db.Exec("SET SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'") //nolint:errcheck
	db.Exec("SET @@GLOBAL.SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'") //nolint:errcheck
	db.Exec("SET TIMESTAMP=DEFAULT")                                                                                                //nolint:errcheck
}
