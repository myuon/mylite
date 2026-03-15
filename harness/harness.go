// Package harness provides a test harness that runs the same SQL against both
// mylite and a real MySQL 8.4 instance, comparing the results.
package harness

import (
	"database/sql"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/executor"
	"github.com/myuon/mylite/server"
	"github.com/myuon/mylite/storage"
)

// TestCase represents a single SQL test case.
type TestCase struct {
	Name string
	// Setup SQL statements run before the test query (e.g., CREATE TABLE, INSERT).
	Setup []string
	// Query is the SQL statement to test.
	Query string
	// Teardown SQL statements run after the test (e.g., DROP TABLE).
	Teardown []string
	// SkipResultCompare skips comparing result rows (useful for non-deterministic results).
	SkipResultCompare bool
	// ExpectError indicates the query should produce an error on both sides.
	ExpectError bool
	// MyliteOnly skips running setup/query/teardown against real MySQL.
	// Use this for MYLITE-specific commands that are not valid MySQL SQL.
	MyliteOnly bool
}

// QueryResult holds the result of executing a query.
type QueryResult struct {
	Columns []string
	Rows    [][]string
	Error   error
	// For non-SELECT statements
	AffectedRows int64
	LastInsertID int64
	IsExec       bool
}

// Harness manages connections to both mylite and MySQL.
type Harness struct {
	t            *testing.T
	myliteDB     *sql.DB
	mysqlDB      *sql.DB
	myliteServer *server.Server
	myliteAddr   string
	mysqlDSN     string
}

// NewHarness creates a new test harness.
// mysqlDSN should be a DSN for a real MySQL 8.4 instance (e.g., "root:@tcp(127.0.0.1:3306)/").
// If mysqlDSN is empty, only mylite tests are run.
func NewHarness(t *testing.T, mysqlDSN string) *Harness {
	t.Helper()

	// Start mylite on a random port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	addr := listener.Addr().String()
	listener.Close()

	cat := catalog.New()
	store := storage.NewEngine()
	exec := executor.New(cat, store)
	srv := server.New(exec, addr)

	go func() {
		if err := srv.Start(); err != nil {
			// Server stopped
		}
	}()

	// Wait for mylite to be ready
	var myliteDB *sql.DB
	for i := 0; i < 50; i++ {
		myliteDB, err = sql.Open("mysql", fmt.Sprintf("root:@tcp(%s)/test", addr))
		if err == nil {
			err = myliteDB.Ping()
			if err == nil {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("failed to connect to mylite: %v", err)
	}

	h := &Harness{
		t:            t,
		myliteDB:     myliteDB,
		myliteServer: srv,
		myliteAddr:   addr,
		mysqlDSN:     mysqlDSN,
	}

	// Connect to real MySQL if DSN provided
	if mysqlDSN != "" {
		mysqlDB, err := sql.Open("mysql", mysqlDSN)
		if err != nil {
			t.Skipf("skipping MySQL comparison: %v", err)
		}
		if err := mysqlDB.Ping(); err != nil {
			t.Skipf("skipping MySQL comparison (cannot connect): %v", err)
		}
		h.mysqlDB = mysqlDB
	}

	return h
}

// Close cleans up resources.
func (h *Harness) Close() {
	if h.myliteDB != nil {
		h.myliteDB.Close()
	}
	if h.mysqlDB != nil {
		h.mysqlDB.Close()
	}
	if h.myliteServer != nil {
		h.myliteServer.Close()
	}
}

// Run executes a test case against both databases and compares results.
func (h *Harness) Run(tc TestCase) {
	h.t.Run(tc.Name, func(t *testing.T) {
		useMySQL := h.mysqlDB != nil && !tc.MyliteOnly

		// Run setup
		for _, stmt := range tc.Setup {
			if _, err := h.myliteDB.Exec(stmt); err != nil {
				t.Fatalf("mylite setup failed: %s: %v", stmt, err)
			}
			if useMySQL {
				if _, err := h.mysqlDB.Exec(stmt); err != nil {
					t.Fatalf("mysql setup failed: %s: %v", stmt, err)
				}
			}
		}

		// Run teardown at the end
		defer func() {
			for _, stmt := range tc.Teardown {
				h.myliteDB.Exec(stmt) //nolint:errcheck
				if useMySQL {
					h.mysqlDB.Exec(stmt) //nolint:errcheck
				}
			}
		}()

		// Execute the test query
		myliteResult := executeQuery(h.myliteDB, tc.Query)
		t.Logf("mylite result: columns=%v, rows=%d, error=%v", myliteResult.Columns, len(myliteResult.Rows), myliteResult.Error)

		if tc.ExpectError {
			if myliteResult.Error == nil {
				t.Errorf("mylite: expected error but got none")
			}
		} else {
			if myliteResult.Error != nil {
				t.Errorf("mylite error: %v", myliteResult.Error)
			}
		}

		// Compare with MySQL if available
		if useMySQL {
			mysqlResult := executeQuery(h.mysqlDB, tc.Query)
			t.Logf("mysql result: columns=%v, rows=%d, error=%v", mysqlResult.Columns, len(mysqlResult.Rows), mysqlResult.Error)

			compareResults(t, tc, myliteResult, mysqlResult)
		}
	})
}

// RunStatements executes multiple test cases.
func (h *Harness) RunStatements(cases []TestCase) {
	for _, tc := range cases {
		h.Run(tc)
	}
}

func executeQuery(db *sql.DB, query string) QueryResult {
	upper := strings.TrimSpace(strings.ToUpper(query))

	// Determine if this is a SELECT-like query
	isQuery := strings.HasPrefix(upper, "SELECT") ||
		strings.HasPrefix(upper, "SHOW") ||
		strings.HasPrefix(upper, "DESCRIBE") ||
		strings.HasPrefix(upper, "DESC ")

	if !isQuery {
		result, err := db.Exec(query)
		if err != nil {
			return QueryResult{Error: err, IsExec: true}
		}
		affected, _ := result.RowsAffected()
		lastID, _ := result.LastInsertId()
		return QueryResult{
			AffectedRows: affected,
			LastInsertID: lastID,
			IsExec:       true,
		}
	}

	rows, err := db.Query(query)
	if err != nil {
		return QueryResult{Error: err}
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return QueryResult{Error: err}
	}

	var resultRows [][]string
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return QueryResult{Error: err}
		}
		row := make([]string, len(columns))
		for i, v := range values {
			if v == nil {
				row[i] = "NULL"
			} else {
				switch val := v.(type) {
				case []byte:
					row[i] = string(val)
				default:
					row[i] = fmt.Sprintf("%v", val)
				}
			}
		}
		resultRows = append(resultRows, row)
	}

	return QueryResult{
		Columns: columns,
		Rows:    resultRows,
	}
}

func compareResults(t *testing.T, tc TestCase, mylite, mysql QueryResult) {
	t.Helper()

	// Both should error or both should succeed
	if (mylite.Error != nil) != (mysql.Error != nil) {
		t.Errorf("error mismatch:\n  mylite error: %v\n  mysql error: %v", mylite.Error, mysql.Error)
		return
	}

	if mylite.Error != nil {
		// Both errored, that's ok for ExpectError cases
		return
	}

	if mylite.IsExec && mysql.IsExec {
		if mylite.AffectedRows != mysql.AffectedRows {
			t.Errorf("affected rows mismatch: mylite=%d, mysql=%d", mylite.AffectedRows, mysql.AffectedRows)
		}
		return
	}

	if tc.SkipResultCompare {
		return
	}

	// Compare columns
	if len(mylite.Columns) != len(mysql.Columns) {
		t.Errorf("column count mismatch: mylite=%d, mysql=%d", len(mylite.Columns), len(mysql.Columns))
		return
	}

	// Compare rows
	if len(mylite.Rows) != len(mysql.Rows) {
		t.Errorf("row count mismatch: mylite=%d, mysql=%d\n  mylite rows: %v\n  mysql rows: %v",
			len(mylite.Rows), len(mysql.Rows), mylite.Rows, mysql.Rows)
		return
	}

	for i := range mylite.Rows {
		for j := range mylite.Rows[i] {
			if mylite.Rows[i][j] != mysql.Rows[i][j] {
				t.Errorf("row %d col %d (%s) mismatch:\n  mylite: %s\n  mysql:  %s",
					i, j, mylite.Columns[j], mylite.Rows[i][j], mysql.Rows[i][j])
			}
		}
	}
}
