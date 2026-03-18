// mtrrun executes MySQL Test Run (.test) files against mylite.
// Usage:
//
//	mtrrun [flags] [suite] [testname]
//	mtrrun [flags] <path/to/test.test>
//
// If no suite is specified, runs all suites.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
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
	os.Setenv("TZ", "Etc/GMT-3")
	if loc, err := time.LoadLocation("Etc/GMT-3"); err == nil {
		time.Local = loc
	}

	defaultTestdata := resolveTestdataRoot()
	suiteRoot := flag.String("suite-root", filepath.Join(defaultTestdata, "suite"), "root directory for test suites")
	includeRoot := flag.String("include-root", filepath.Join(defaultTestdata, "include"), "root directory for include files")
	verbose := flag.Bool("verbose", false, "verbose output")
	maxTests := flag.Int("max", 0, "maximum number of tests to run per suite (0=all)")
	jobs := flag.Int("j", 0, "number of parallel test workers (0=auto, 1=sequential)")
	timeout := flag.Duration("timeout", 30*time.Second, "timeout per test (0=no timeout)")
	flag.Parse()

	args := flag.Args()

	// No args: run all suites
	if len(args) == 0 {
		runAllSuites(*suiteRoot, *includeRoot, *verbose, *maxTests, *jobs, *timeout)
		return
	}

	target := args[0]

	// Check if it's a direct .test file path
	if strings.HasSuffix(target, ".test") {
		runSingleTest(target, *suiteRoot, *includeRoot, *verbose)
		return
	}

	// Specific test within suite?
	testFilter := ""
	if len(args) > 1 {
		testFilter = args[1]
	}

	results := runSuite(target, testFilter, *suiteRoot, *includeRoot, *verbose, *maxTests, *jobs, *timeout)
	printSuiteSummary(target, results)

	if hasFailures(results) {
		os.Exit(1)
	}
}

// runAllSuites discovers and runs all test suites.
func runAllSuites(suiteRoot, includeRoot string, verbose bool, maxTests, jobs int, timeout time.Duration) {
	start := time.Now()

	entries, err := os.ReadDir(suiteRoot)
	if err != nil {
		log.Fatalf("cannot read suite root: %v", err)
	}

	var suiteNames []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		testDir := filepath.Join(suiteRoot, e.Name(), "t")
		if _, err := os.Stat(testDir); err == nil {
			suiteNames = append(suiteNames, e.Name())
		}
	}

	var totalPassed, totalFailed, totalSkipped, totalErrors, totalTests int

	type suiteResult struct {
		name    string
		results []mtrrunner.TestResult
		elapsed time.Duration
	}

	// Limit concurrent suites to avoid resource exhaustion (each suite spawns up to 8 workers)
	maxConcurrentSuites := runtime.NumCPU()
	if maxConcurrentSuites > 8 {
		maxConcurrentSuites = 8
	}
	sem := make(chan struct{}, maxConcurrentSuites)

	resultsCh := make(chan suiteResult, len(suiteNames))
	var wg sync.WaitGroup
	for _, suite := range suiteNames {
		wg.Add(1)
		go func(s string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			t0 := time.Now()
			res := runSuite(s, "", suiteRoot, includeRoot, verbose, maxTests, jobs, timeout)
			resultsCh <- suiteResult{name: s, results: res, elapsed: time.Since(t0)}
		}(suite)
	}
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	// Collect results and print in completion order
	var allResults []suiteResult
	for sr := range resultsCh {
		allResults = append(allResults, sr)
	}
	// Sort by suite name for deterministic output
	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].name < allResults[j].name
	})
	for _, sr := range allResults {
		p, f, s, e := countResults(sr.results)
		printSuiteSummaryCompact(sr.name, len(sr.results), p, f, s, e, sr.elapsed)
		totalPassed += p
		totalFailed += f
		totalSkipped += s
		totalErrors += e
		totalTests += len(sr.results)
	}

	elapsed := time.Since(start)
	fmt.Printf("\n=== Grand Total ===\n")
	fmt.Printf("Suites: %d, Total: %d, Passed: %d, Failed: %d, Skipped: %d, Errors: %d\n",
		len(suiteNames), totalTests, totalPassed, totalFailed, totalSkipped, totalErrors)
	fmt.Printf("Time: %.1fs\n", elapsed.Seconds())

	if totalFailed+totalErrors > 0 {
		os.Exit(1)
	}
}

// runSuite runs all tests in a single suite and returns results.
func runSuite(suiteName, testFilter, suiteRoot, includeRoot string, verbose bool, maxTests, jobs int, timeout time.Duration) []mtrrunner.TestResult {
	suiteDir := filepath.Join(suiteRoot, suiteName)
	if _, err := os.Stat(suiteDir); os.IsNotExist(err) {
		log.Fatalf("suite directory not found: %s", suiteDir)
	}

	// Build include paths for this suite
	includePaths := []string{includeRoot}
	suiteInclude := filepath.Join(suiteDir, "include")
	if _, err := os.Stat(suiteInclude); err == nil {
		includePaths = append(includePaths, suiteInclude)
	}
	suiteTestDir := filepath.Join(suiteDir, "t")
	if _, err := os.Stat(suiteTestDir); err == nil {
		includePaths = append(includePaths, suiteTestDir)
	}
	includePaths = append(includePaths, suiteRoot)

	searchPaths := []string{suiteRoot, includeRoot, filepath.Dir(suiteRoot)}
	searchPaths = append(searchPaths, includePaths...)

	// Discover tests
	testDir := filepath.Join(suiteDir, "t")
	entries, err := os.ReadDir(testDir)
	if err != nil {
		return nil
	}

	var testPaths []string
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".test") {
			continue
		}
		testName := strings.TrimSuffix(entry.Name(), ".test")
		if testFilter != "" && testName != testFilter {
			continue
		}
		testPaths = append(testPaths, filepath.Join(testDir, entry.Name()))
		if maxTests > 0 && len(testPaths) >= maxTests {
			break
		}
	}

	if len(testPaths) == 0 {
		return nil
	}

	// Determine parallelism
	numJobs := jobs
	if numJobs <= 0 {
		numJobs = runtime.NumCPU()
		if numJobs > 8 {
			numJobs = 8
		}
	}
	if numJobs > len(testPaths) {
		numJobs = len(testPaths)
	}

	if numJobs <= 1 {
		return runSequential(testPaths, includePaths, searchPaths, verbose, timeout)
	}
	return runParallel(testPaths, includePaths, searchPaths, verbose, numJobs, timeout)
}

// worker represents a dedicated mylite server instance for running tests.
type worker struct {
	srv         *server.Server
	exec        *executor.Executor
	cat         *catalog.Catalog
	store       *storage.Engine
	addr        string
	tmpDir      string
	searchPaths []string
}

func newWorker(searchPaths []string) (*worker, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("failed to find free port: %v", err)
	}
	addr := listener.Addr().String()
	listener.Close()

	tmpDir, err := os.MkdirTemp("", "mylite-mtr-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %v", err)
	}
	os.MkdirAll(filepath.Join(tmpDir, "tmp"), 0755) //nolint:errcheck
	dataDir := filepath.Join(tmpDir, "data", "inner")
	os.MkdirAll(dataDir, 0755) //nolint:errcheck

	cat := catalog.New()
	store := storage.NewEngine()
	exec := executor.New(cat, store)
	exec.DataDir = dataDir
	exec.SearchPaths = searchPaths

	srv := server.New(exec, addr)
	go func() {
		srv.Start() //nolint:errcheck
	}()

	return &worker{
		srv:         srv,
		exec:        exec,
		cat:         cat,
		store:       store,
		addr:        addr,
		tmpDir:      tmpDir,
		searchPaths: searchPaths,
	}, nil
}

func (w *worker) close() {
	w.srv.Close()
	os.RemoveAll(w.tmpDir)
}

func (w *worker) runTest(testPath string, includePaths []string, verbose bool, timeout time.Duration) mtrrunner.TestResult {
	testName := strings.TrimSuffix(filepath.Base(testPath), ".test")

	if timeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		ch := make(chan mtrrunner.TestResult, 1)
		go func() {
			ch <- w.runTestInner(testPath, includePaths, verbose)
		}()

		select {
		case result := <-ch:
			return result
		case <-ctx.Done():
			return mtrrunner.TestResult{
				Name:    testName,
				Error:   fmt.Sprintf("timeout after %s", timeout),
				Skipped: true,
			}
		}
	}

	return w.runTestInner(testPath, includePaths, verbose)
}

func (w *worker) runTestInner(testPath string, includePaths []string, verbose bool) mtrrunner.TestResult {
	db, err := connectDB(w.addr)
	if err != nil {
		return mtrrunner.TestResult{
			Name:  strings.TrimSuffix(filepath.Base(testPath), ".test"),
			Error: fmt.Sprintf("failed to connect: %v", err),
		}
	}
	defer db.Close()

	resetDB(db)
	resetSessionState(db)

	runner := &mtrrunner.Runner{
		DB:           db,
		IncludePaths: includePaths,
		Verbose:      verbose,
		TmpDir:       w.tmpDir,
	}

	return runner.RunFile(testPath)
}

type indexedResult struct {
	index  int
	result mtrrunner.TestResult
}

func runParallel(testPaths []string, includePaths, searchPaths []string, verbose bool, numJobs int, timeout time.Duration) []mtrrunner.TestResult {
	// Create worker pool
	workers := make([]*worker, numJobs)
	for i := 0; i < numJobs; i++ {
		w, err := newWorker(searchPaths)
		if err != nil {
			log.Fatalf("failed to create worker %d: %v", i, err)
		}
		workers[i] = w
	}
	defer func() {
		for _, w := range workers {
			w.close()
		}
	}()

	// Wait for all workers to be ready
	for _, w := range workers {
		db, err := connectDB(w.addr)
		if err != nil {
			log.Fatalf("failed to connect to worker: %v", err)
		}
		db.Close()
	}

	// Distribute tests to workers via channel
	testCh := make(chan struct {
		index int
		path  string
	}, len(testPaths))
	for i, p := range testPaths {
		testCh <- struct {
			index int
			path  string
		}{i, p}
	}
	close(testCh)

	resultCh := make(chan indexedResult, len(testPaths))

	var wg sync.WaitGroup
	for i := 0; i < numJobs; i++ {
		wg.Add(1)
		go func(w *worker) {
			defer wg.Done()
			for t := range testCh {
				result := w.runTest(t.path, includePaths, verbose, timeout)
				resultCh <- indexedResult{index: t.index, result: result}
			}
		}(workers[i])
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	results := make([]mtrrunner.TestResult, len(testPaths))
	for ir := range resultCh {
		results[ir.index] = ir.result
	}

	return results
}

func runSequential(testPaths []string, includePaths, searchPaths []string, verbose bool, timeout time.Duration) []mtrrunner.TestResult {
	w, err := newWorker(searchPaths)
	if err != nil {
		log.Fatalf("failed to create worker: %v", err)
	}
	defer w.close()

	db, err := connectDB(w.addr)
	if err != nil {
		log.Fatalf("failed to connect to mylite: %v", err)
	}
	db.Close()

	var results []mtrrunner.TestResult
	for _, testPath := range testPaths {
		result := w.runTest(testPath, includePaths, verbose, timeout)
		results = append(results, result)
	}

	return results
}

func countResults(results []mtrrunner.TestResult) (passed, failed, skipped, errors int) {
	for _, r := range results {
		switch {
		case r.Skipped:
			skipped++
		case r.Passed:
			passed++
		case r.Error != "":
			errors++
		default:
			failed++
		}
	}
	return
}

func hasFailures(results []mtrrunner.TestResult) bool {
	for _, r := range results {
		if !r.Passed && !r.Skipped {
			return true
		}
	}
	return false
}

func printSuiteSummary(suiteName string, results []mtrrunner.TestResult) {
	for _, r := range results {
		printResult(r, false)
	}
	p, f, s, e := countResults(results)
	fmt.Printf("\n=== Summary ===\n")
	fmt.Printf("Total: %d, Passed: %d, Failed: %d, Skipped: %d, Errors: %d\n",
		len(results), p, f, s, e)
}

func printSuiteSummaryCompact(suiteName string, total, passed, failed, skipped, errors int, elapsed time.Duration) {
	status := "OK"
	if failed+errors > 0 {
		status = "FAIL"
	}
	fmt.Printf("%-30s %4d tests: %4d passed, %4d failed, %4d skipped, %4d errors  [%s]  (%.1fs)\n",
		suiteName, total, passed, failed, skipped, errors, status, elapsed.Seconds())
}

func runSingleTest(target, suiteRoot, includeRoot string, verbose bool) {
	searchPaths := []string{suiteRoot, includeRoot, filepath.Dir(suiteRoot)}
	includePaths := []string{includeRoot}

	suiteDir := filepath.Dir(filepath.Dir(target))
	suiteInclude := filepath.Join(suiteDir, "include")
	if _, err := os.Stat(suiteInclude); err == nil {
		includePaths = append(includePaths, suiteInclude)
	}
	suiteTestDir := filepath.Join(suiteDir, "t")
	if _, err := os.Stat(suiteTestDir); err == nil {
		includePaths = append(includePaths, suiteTestDir)
	}
	includePaths = append(includePaths, suiteRoot)
	searchPaths = append(searchPaths, includePaths...)

	w, err := newWorker(searchPaths)
	if err != nil {
		log.Fatalf("failed to create worker: %v", err)
	}
	defer w.close()

	db, err := connectDB(w.addr)
	if err != nil {
		log.Fatalf("failed to connect to mylite: %v", err)
	}
	db.Close()

	result := w.runTest(target, includePaths, verbose, 0)
	printResult(result, verbose)
	if !result.Passed && !result.Skipped {
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

func resolveTestdataRoot() string {
	local := "testdata/dolt-mysql-tests/files"
	if fi, err := os.Stat(filepath.Join(local, "suite")); err == nil && fi.IsDir() {
		return local
	}

	gitPath := ".git"
	data, err := os.ReadFile(gitPath)
	if err == nil {
		content := strings.TrimSpace(string(data))
		if strings.HasPrefix(content, "gitdir: ") {
			gitdir := strings.TrimPrefix(content, "gitdir: ")
			mainRepo := filepath.Join(gitdir, "..", "..", "..")
			candidate := filepath.Join(mainRepo, "testdata", "dolt-mysql-tests", "files")
			if fi, err := os.Stat(filepath.Join(candidate, "suite")); err == nil && fi.IsDir() {
				abs, _ := filepath.Abs(candidate)
				return abs
			}
		}
	}

	return local
}

func resetDB(db *sql.DB) {
	// Drop all databases except system ones, then recreate 'test'
	rows, err := db.Query("SHOW DATABASES")
	if err != nil {
		return
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			databases = append(databases, name)
		}
	}
	for _, d := range databases {
		if d == "information_schema" || d == "performance_schema" || d == "mysql" || d == "test" || d == "mtr" || d == "sys" {
			continue
		}
		db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", d)) //nolint:errcheck
	}

	// Drop tables in test database
	rows2, err := db.Query("SHOW TABLES")
	if err != nil {
		return
	}
	defer rows2.Close()

	var tables []string
	for rows2.Next() {
		var name string
		if err := rows2.Scan(&name); err == nil {
			tables = append(tables, name)
		}
	}
	sort.Strings(tables)
	for _, t := range tables {
		db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", t)) //nolint:errcheck
	}
}

func resetSessionState(db *sql.DB) {
	db.Exec("SET SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'") //nolint:errcheck
	db.Exec("SET @@GLOBAL.SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'") //nolint:errcheck
	db.Exec("SET TIMESTAMP=DEFAULT") //nolint:errcheck
}
