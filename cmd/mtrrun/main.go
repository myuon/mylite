// mtrrun executes MySQL Test Run (.test) files against mylite.
// Usage:
//
//	mtrrun [flags] [suite] [testname]
//	mtrrun [flags] <path/to/test.test>
//
// Flags:
//
//	-suite  Run only specified suite(s), comma-separated (e.g. -suite sys_vars,innodb)
//	-test   Run only specified test(s) in suite/testname format, comma-separated
//	        (e.g. -test sys_vars/gtid_owned_basic,other/bool)
//
// If no suite is specified, runs all enabled suites.
package main
import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
	_ "github.com/go-sql-driver/mysql"
	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/executor"
	"github.com/myuon/mylite/mtrrunner"
	"github.com/myuon/mylite/server"
	"github.com/myuon/mylite/storage"
)
// skipTests lists tests known to be unfixable. Key: "suite/testname".
var skipTests map[string]bool

// testResultJSON is the per-test entry in the JSON result log.
type testResultJSON struct {
	Suite   string `json:"suite"`
	Name    string `json:"name"`
	Status  string `json:"status"` // "pass", "fail", "error", "timeout", "skip"
	Elapsed string `json:"elapsed,omitempty"`
	Error   string `json:"error,omitempty"`
	Diff    string `json:"diff,omitempty"`
}

// runSkippedOnly when true, inverts the skip logic: only run tests IN the skiplist.
var runSkippedOnly bool

//go:embed skiplist.json
var skipListData []byte

type skipEntry struct {
	Test   string `json:"test"`
	Reason string `json:"reason"`
}

type skipList struct {
	Skips []skipEntry `json:"skips"`
}

func init() {
	var sl skipList
	if err := json.Unmarshal(skipListData, &sl); err != nil {
		log.Fatalf("failed to parse skiplist.json: %v", err)
	}
	skipTests = make(map[string]bool, len(sl.Skips))
	for _, e := range sl.Skips {
		skipTests[e.Test] = true
	}
}
var mtrrunLockFile = filepath.Join(os.TempDir(), "mtrrun.lock")

// acquireLock creates a PID lock file to prevent concurrent mtrrun executions.
// Returns a cleanup function that removes the lock, or an error if already running.
func acquireLock() (func(), error) {
	// Check for existing lock and whether the owner is still alive.
	if data, err := os.ReadFile(mtrrunLockFile); err == nil {
		var pid int
		if n, _ := fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &pid); n == 1 {
			if proc, err := os.FindProcess(pid); err == nil {
				if proc.Signal(syscall.Signal(0)) == nil {
					return nil, fmt.Errorf("mtrrun is already running (PID %d), lock: %s", pid, mtrrunLockFile)
				}
			}
		}
		// Stale lock — remove it.
		os.Remove(mtrrunLockFile)
	}

	f, err := os.OpenFile(mtrrunLockFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("mtrrun is already running, lock: %s", mtrrunLockFile)
	}
	fmt.Fprintf(f, "%d", os.Getpid())
	f.Close()

	return func() { os.Remove(mtrrunLockFile) }, nil
}

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
	timeout := flag.Duration("timeout", 20*time.Second, "timeout per test (0=no timeout)")
	suiteFilter := flag.String("suite", "", "run only specified suite(s), comma-separated (e.g. sys_vars,innodb)")
	testFilter := flag.String("test", "", "run only specified test(s), comma-separated in suite/testname format (e.g. sys_vars/gtid_owned_basic,other/bool)")
	force := flag.Bool("force", false, "kill existing mtrrun process and remove lock before starting")
	skippedOnly := flag.Bool("skipped-only", false, "run only tests that are in the skiplist (inverse of normal behavior)")
	flag.Parse()
	runSkippedOnly = *skippedOnly

	if *force {
		if data, err := os.ReadFile(mtrrunLockFile); err == nil {
			var pid int
			if n, _ := fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &pid); n == 1 {
				if proc, err := os.FindProcess(pid); err == nil {
					proc.Signal(syscall.SIGTERM) //nolint:errcheck
				}
			}
			os.Remove(mtrrunLockFile) //nolint:errcheck
			fmt.Fprintf(os.Stderr, "force: removed lock file %s\n", mtrrunLockFile)
		}
	}

	unlock, err := acquireLock()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	defer unlock()
	args := flag.Args()
	// No args: run all suites (with optional -suite/-test filtering)
	if len(args) == 0 {
		runAllSuites(*suiteRoot, *includeRoot, *verbose, *maxTests, *jobs, *timeout, *suiteFilter, *testFilter)
		return
	}
	target := args[0]
	// Check if it's a direct .test file path
	if strings.HasSuffix(target, ".test") {
		runSingleTest(target, *suiteRoot, *includeRoot, *verbose)
		return
	}
	// Specific test within suite?
	var argTestFilter map[string]bool
	if len(args) > 1 {
		argTestFilter = map[string]bool{args[1]: true}
	}
	results := runSuite(target, argTestFilter, *suiteRoot, *includeRoot, *verbose, *maxTests, *jobs, *timeout)
	printSuiteSummary(target, results)
	if hasFailures(results) {
		os.Exit(1)
	}
}
// runAllSuites discovers and runs all test suites sequentially.
// suiteFilter: comma-separated suite names to run (empty=all enabled).
// testFilterStr: comma-separated "suite/testname" pairs to run (empty=all).
func runAllSuites(suiteRoot, includeRoot string, verbose bool, maxTests, jobs int, timeout time.Duration, suiteFilter, testFilterStr string) {
	start := time.Now()

	// Parse -test flag into per-suite test name sets, and collect implied suites.
	testsBySuite := map[string]map[string]bool{}
	if testFilterStr != "" {
		for _, t := range strings.Split(testFilterStr, ",") {
			t = strings.TrimSpace(t)
			if t == "" {
				continue
			}
			parts := strings.SplitN(t, "/", 2)
			if len(parts) != 2 {
				log.Fatalf("-test value %q must be in suite/testname format", t)
			}
			suite, name := parts[0], parts[1]
			if testsBySuite[suite] == nil {
				testsBySuite[suite] = map[string]bool{}
			}
			testsBySuite[suite][name] = true
		}
	}

	// Parse -suite flag into a set.
	suiteFilterSet := map[string]bool{}
	if suiteFilter != "" {
		for _, s := range strings.Split(suiteFilter, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				suiteFilterSet[s] = true
			}
		}
	}
	// -test implies the suites of those tests.
	for s := range testsBySuite {
		suiteFilterSet[s] = true
	}

	entries, err := os.ReadDir(suiteRoot)
	if err != nil {
		log.Fatalf("cannot read suite root: %v", err)
	}
	// Enabled suites whitelist. Add suites one at a time and fix until all pass.
	//
	// Out-of-scope suites (not enabled):
	// - x: X Protocol (mysqlx) plugin required
	// - binlog, binlog_gtid: binary log / replication
	// - secondary_engine: secondary engine plugin required
	enabledSuites := map[string]bool{
		// Phase 1: Core engine (high pass rate)
		"engine_funcs":  true,
		"engine_iuds":   true,
		"jp":            true,
		"json":          true,
		"gcol":          true,
		"gis":           true,
		"innodb_fts":    true,
		"parts":         true,
		"funcs_1":       true,
		"innodb":        true,
		"stress":        true,
		"other":         true,
		"perfschema":    true,
		"sys_vars":      true,
		"sysschema":     true,
		"funcs_2":       true,
		"large_tests":   true,
		"innodb_undo":   true,
		"innodb_zip":    true,
		"gcol_ndb":      true,
		"innodb_gis":    true,
		"innodb_stress": true,
		"max_parts":     true,
		"opt_trace":     true,
		"auth_sec":      true,
		"collations":             true,
		"query_rewrite_plugins": true,
		"encryption":            true,
		"binlog_nogtid":         true,
	}
	var suiteNames []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		// When -suite or -test is specified, only run matching suites (skip enabledSuites whitelist).
		if len(suiteFilterSet) > 0 {
			if !suiteFilterSet[e.Name()] {
				continue
			}
		} else if len(enabledSuites) > 0 && !enabledSuites[e.Name()] {
			continue
		}
		testDir := filepath.Join(suiteRoot, e.Name(), "t")
		if _, err := os.Stat(testDir); err == nil {
			suiteNames = append(suiteNames, e.Name())
		}
	}
	var allResultsJSON []testResultJSON
	var totalPassed, totalFailed, totalSkipped, totalErrors, totalTimeouts, totalTests int
	for _, sn := range suiteNames {
		fmt.Fprintf(os.Stderr, "[%s] starting suite %s...\n", time.Now().Format("15:04:05"), sn)
		results := runSuite(sn, testsBySuite[sn], suiteRoot, includeRoot, verbose, maxTests, jobs, timeout)
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		fmt.Fprintf(os.Stderr, "[%s] finished suite %s (%d tests) goroutines=%d heap=%.0fMB\n",
			time.Now().Format("15:04:05"), sn, len(results), runtime.NumGoroutine(), float64(m.HeapInuse)/1024/1024)
		runtime.GC()
		if len(results) == 0 {
			continue
		}
		p, f, s, e, t := countResults(results)
		for _, r := range results {
			jr := testResultJSON{Suite: sn, Name: r.Name, Elapsed: r.Elapsed.Round(time.Millisecond).String()}
			switch {
			case r.Passed:
				jr.Status = "pass"
			case r.Skipped:
				jr.Status = "skip"
			case r.Timeout:
				jr.Status = "timeout"
			case r.Error != "":
				jr.Status = "error"
				jr.Error = r.Error
			default:
				jr.Status = "fail"
				jr.Diff = r.Diff
			}
			allResultsJSON = append(allResultsJSON, jr)
		}
		totalPassed += p
		totalFailed += f
		totalSkipped += s
		totalErrors += e
		totalTimeouts += t
		totalTests += len(results)
	}
	elapsed := time.Since(start)
	fmt.Printf("\n=== Grand Total ===\n")
	fmt.Printf("Suites: %d, Total: %d, Passed: %d, Failed: %d, Skipped: %d, Errors: %d, Timeouts: %d\n",
		len(suiteNames), totalTests, totalPassed, totalFailed, totalSkipped, totalErrors, totalTimeouts)
	fmt.Printf("Time: %.1fs\n", elapsed.Seconds())

	// Save results to JSON log
	saveResultLog(allResultsJSON, totalPassed, totalFailed, totalSkipped, totalErrors, totalTimeouts, elapsed)

	if totalFailed+totalErrors > 0 {
		os.Exit(1)
	}
}
// saveResultLog writes test results to a timestamped JSON file in .mtrrun-logs/.
func saveResultLog(results []testResultJSON, passed, failed, skipped, errors, timeouts int, elapsed time.Duration) {
	logDir := ".mtrrun-logs"
	os.MkdirAll(logDir, 0755)
	ts := time.Now().Format("20060102-150405")
	filename := filepath.Join(logDir, fmt.Sprintf("result-%s.json", ts))

	type logFile struct {
		Timestamp string      `json:"timestamp"`
		Elapsed   string      `json:"elapsed"`
		Summary   interface{} `json:"summary"`
		Tests     interface{} `json:"tests"`
	}

	data, _ := json.MarshalIndent(logFile{
		Timestamp: time.Now().Format(time.RFC3339),
		Elapsed:   elapsed.Round(time.Millisecond).String(),
		Summary: map[string]int{
			"passed":   passed,
			"failed":   failed,
			"skipped":  skipped,
			"errors":   errors,
			"timeouts": timeouts,
			"total":    passed + failed + skipped + errors + timeouts,
		},
		Tests: results,
	}, "", "  ")
	os.WriteFile(filename, data, 0644)
	fmt.Printf("Results saved to: %s\n", filename)
}

// runSuite runs all tests in a single suite and returns results.
// testFilter: if non-nil, only run tests whose names are in this set.
func runSuite(suiteName string, testFilter map[string]bool, suiteRoot, includeRoot string, verbose bool, maxTests, jobs int, timeout time.Duration) []mtrrunner.TestResult {
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
	var skippedResults []mtrrunner.TestResult
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".test") {
			continue
		}
		testName := strings.TrimSuffix(entry.Name(), ".test")
		if testFilter != nil && !testFilter[testName] {
			continue
		}
		isSkipped := skipTests[suiteName+"/"+testName]
		if runSkippedOnly {
			if !isSkipped {
				continue
			}
		} else {
			if isSkipped {
				skippedResults = append(skippedResults, mtrrunner.TestResult{
					Name:    testName,
					Skipped: true,
				})
				continue
			}
		}
		testPaths = append(testPaths, filepath.Join(testDir, entry.Name()))
		if maxTests > 0 && len(testPaths) >= maxTests {
			break
		}
	}
	if len(testPaths) == 0 {
		return skippedResults
	}
	// Determine parallelism
	numJobs := jobs
	if numJobs <= 0 {
		numJobs = runtime.NumCPU() / 2
		if numJobs < 2 {
			numJobs = 2
		}
		if numJobs > 4 {
			numJobs = 4
		}
	}
	if numJobs > len(testPaths) {
		numJobs = len(testPaths)
	}
	var results []mtrrunner.TestResult
	if numJobs <= 1 {
		results = runSequential(testPaths, includePaths, searchPaths, verbose, timeout)
	} else {
		results = runParallel(testPaths, includePaths, searchPaths, verbose, numJobs, timeout)
	}
	return append(skippedResults, results...)
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
	db          *sql.DB // reusable DB connection pool for this worker
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
	// Symlink std_data into temp dir so LOAD DATA with $MYSQLTEST_VARDIR/std_data/... works
	for _, sp := range searchPaths {
		stdData := filepath.Join(sp, "std_data")
		if fi, err := os.Stat(stdData); err == nil && fi.IsDir() {
			target := filepath.Join(tmpDir, "std_data")
			if _, err := os.Lstat(target); os.IsNotExist(err) {
				// Use absolute path for symlink target so it resolves correctly
				// from the temp directory.
				absStdData, absErr := filepath.Abs(stdData)
				if absErr == nil {
					stdData = absStdData
				}
				os.Symlink(stdData, target) //nolint:errcheck
			}
			break
		}
	}
	cat := catalog.New()
	store := storage.NewEngine()
	exec := executor.New(cat, store)
	exec.DataDir = dataDir
	exec.SearchPaths = searchPaths
	srv := server.New(exec, addr)
	go func() {
		srv.Start() //nolint:errcheck
	}()
	// Create a reusable DB connection pool for this worker
	db, err := connectDB(addr)
	if err != nil {
		srv.Close()
		os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("failed to connect to worker: %v", err)
	}
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(2)
	return &worker{
		srv:         srv,
		exec:        exec,
		cat:         cat,
		store:       store,
		addr:        addr,
		tmpDir:      tmpDir,
		searchPaths: searchPaths,
		db:          db,
	}, nil
}
func (w *worker) close() {
	if w.db != nil {
		w.db.Close()
	}
	w.srv.Close()
	os.RemoveAll(w.tmpDir)
}
// resetState creates a fresh catalog/storage/executor and swaps it into the server.
// This ensures complete isolation between tests without restarting the TCP listener.
func (w *worker) resetState() {
	w.cat = catalog.New()
	w.store = storage.NewEngine()
	w.exec = executor.New(w.cat, w.store)
	w.exec.DataDir = filepath.Join(w.tmpDir, "data", "inner")
	w.exec.SearchPaths = w.searchPaths
	w.srv.SetExecutor(w.exec)
}
func (w *worker) runTest(testPath string, includePaths []string, verbose bool, timeout time.Duration) mtrrunner.TestResult {
	testName := strings.TrimSuffix(filepath.Base(testPath), ".test")
	t0 := time.Now()
	if timeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		ch := make(chan mtrrunner.TestResult, 1)
		go func() {
			ch <- w.runTestInner(testPath, includePaths, verbose)
		}()
		select {
		case result := <-ch:
			result.Elapsed = time.Since(t0)
			return result
		case <-ctx.Done():
			// Abandon the stuck worker entirely and rebuild from scratch.
			// The old goroutine will eventually die when its connections error out.
			w.rebuild()
			return mtrrunner.TestResult{
				Name:    testName,
				Timeout: true,
				Elapsed: time.Since(t0),
			}
		}
	}
	result := w.runTestInner(testPath, includePaths, verbose)
	result.Elapsed = time.Since(t0)
	return result
}
// rebuild tears down the current server and creates a fresh one.
// The old server's goroutines are abandoned but will exit when their
// connections are closed by the OS or when the process exits.
func (w *worker) rebuild() {
	// Close server to break TCP connections of stuck goroutines
	if w.srv != nil {
		w.srv.Close()
	}
	if w.db != nil {
		w.db.Close()
		w.db = nil
	}
	// Nil out references so old executor/catalog/storage can be GC'd
	// once the stuck goroutine's stack is collected
	w.exec = nil
	w.cat = nil
	w.store = nil
	// Create fresh server
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	w.addr = listener.Addr().String()
	listener.Close()
	w.cat = catalog.New()
	w.store = storage.NewEngine()
	w.exec = executor.New(w.cat, w.store)
	w.exec.DataDir = filepath.Join(w.tmpDir, "data", "inner")
	w.exec.SearchPaths = w.searchPaths
	w.srv = server.New(w.exec, w.addr)
	go w.srv.Start()
	// Reconnect DB
	for i := 0; i < 50; i++ {
		db, err := connectDB(w.addr)
		if err == nil {
			db.SetMaxIdleConns(1)
			db.SetMaxOpenConns(2)
			w.db = db
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}
func (w *worker) runTestInner(testPath string, includePaths []string, verbose bool) mtrrunner.TestResult {
	// Reset executor state for full isolation between tests
	w.resetState()
	resetSessionState(w.db)
	// Use a dedicated DB connection for this test so timeout can close it
	testDB, err := connectDB(w.addr)
	if err != nil {
		return mtrrunner.TestResult{
			Name:  strings.TrimSuffix(filepath.Base(testPath), ".test"),
			Error: fmt.Sprintf("failed to connect: %v", err),
		}
	}
	defer testDB.Close()
	testDB.SetMaxIdleConns(4)
	testDB.SetMaxOpenConns(16)
	runner := &mtrrunner.Runner{
		DB:           testDB,
		IncludePaths: includePaths,
		Verbose:      verbose,
		TmpDir:       w.tmpDir,
		ServerAddr:   w.addr,
	}
	return runner.RunFile(testPath)
}
type indexedResult struct {
	index  int
	result mtrrunner.TestResult
}

// printVerboseResult prints a single test result line for -verbose mode.
// mu may be nil when called from sequential execution.
func printVerboseResult(r mtrrunner.TestResult, mu *sync.Mutex) {
	var line string
	timeStr := fmt.Sprintf("(%.1fs)", r.Elapsed.Seconds())
	switch {
	case r.Timeout:
		line = fmt.Sprintf("  TIMEOUT: %s %s", r.Name, timeStr)
	case r.Skipped:
		line = fmt.Sprintf("  SKIP: %s", r.Name)
	case r.Passed:
		line = fmt.Sprintf("  PASS: %s %s", r.Name, timeStr)
	case r.Error != "":
		line = fmt.Sprintf("  ERROR: %s: %s %s", r.Name, r.Error, timeStr)
	default:
		diffLines := 0
		if r.Diff != "" {
			diffLines = len(strings.Split(r.Diff, "\n"))
		}
		line = fmt.Sprintf("  FAIL: %s (diff_lines=%d) %s", r.Name, diffLines, timeStr)
	}
	if mu != nil {
		mu.Lock()
		defer mu.Unlock()
	}
	fmt.Println(line)
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
func countResults(results []mtrrunner.TestResult) (passed, failed, skipped, errors, timeouts int) {
	for _, r := range results {
		switch {
		case r.Timeout:
			timeouts++
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
		if !r.Passed && !r.Skipped && !r.Timeout {
			return true
		}
	}
	return false
}
func printSuiteSummary(suiteName string, results []mtrrunner.TestResult) {
	for _, r := range results {
		printResult(r, false)
	}
	p, f, s, e, t := countResults(results)
	fmt.Printf("\n=== Summary ===\n")
	fmt.Printf("Total: %d, Passed: %d, Failed: %d, Skipped: %d, Errors: %d, Timeouts: %d\n",
		len(results), p, f, s, e, t)
}
func printSuiteSummaryCompact(suiteName string, total, passed, failed, skipped, errors, timeouts int, elapsed time.Duration) {
	status := "OK"
	if failed+errors > 0 {
		status = "FAIL"
	}
	fmt.Printf("%-30s %4d tests: %4d passed, %4d failed, %4d skipped, %4d errors, %4d timeouts  [%s]  (%.1fs)\n",
		suiteName, total, passed, failed, skipped, errors, timeouts, status, elapsed.Seconds())
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
	timeStr := fmt.Sprintf("(%.1fs)", r.Elapsed.Seconds())
	if r.Timeout {
		fmt.Printf("TIMEOUT %-38s %s\n", r.Name, timeStr)
		return
	}
	if r.Skipped {
		fmt.Printf("SKIP  %-40s %s\n", r.Name, timeStr)
		return
	}
	if r.Passed {
		fmt.Printf("PASS  %-40s %s\n", r.Name, timeStr)
		return
	}
	if r.Error != "" {
		fmt.Printf("ERROR %-40s %s: %s\n", r.Name, timeStr, r.Error)
		if verbose && r.Output != "" {
			fmt.Printf("  Output:\n%s\n", indent(r.Output))
		}
		return
	}
	fmt.Printf("FAIL  %-40s %s\n", r.Name, timeStr)
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
		if abs, err := filepath.Abs(local); err == nil {
			return abs
		}
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
func resetSessionState(db *sql.DB) {
	db.Exec("SET SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'")          //nolint:errcheck
	db.Exec("SET @@GLOBAL.SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'") //nolint:errcheck
	db.Exec("SET TIMESTAMP=DEFAULT")                                                                                                                         //nolint:errcheck
}
