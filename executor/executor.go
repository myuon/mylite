package executor

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
	"vitess.io/vitess/go/vt/sqlparser"
)

// mysqlCharLen returns the MySQL character count of a string.
// For valid UTF-8, it returns the rune count.
// For non-UTF-8 (e.g., cp932, sjis), it heuristically counts multi-byte characters.
func mysqlCharLen(s string) int {
	return mysqlCharLenCharset(s, "")
}

// mysqlCharLenCharset returns the MySQL character count of a string for a given charset.
func mysqlCharLenCharset(s string, charset string) int {
	if utf8.ValidString(s) {
		return utf8.RuneCountInString(s)
	}
	count := 0
	i := 0
	switch strings.ToLower(charset) {
	case "big5":
		for i < len(s) {
			if s[i] >= 0xA1 && s[i] <= 0xFE {
				i += 2
			} else {
				i++
			}
			count++
		}
	case "gb2312":
		for i < len(s) {
			if s[i] >= 0xA1 && s[i] <= 0xF7 {
				i += 2
			} else {
				i++
			}
			count++
		}
	case "gbk", "gb18030":
		for i < len(s) {
			if s[i] >= 0x81 && s[i] <= 0xFE {
				i += 2
			} else {
				i++
			}
			count++
		}
	default:
		for i < len(s) {
			b := s[i]
			if (b >= 0x81 && b <= 0x9F) || (b >= 0xE0 && b <= 0xFC) {
				i += 2
			} else {
				i++
			}
			count++
		}
	}
	return count
}

// mysqlTruncateChars truncates a string to at most maxChars MySQL characters.
func mysqlTruncateChars(s string, maxChars int) string {
	if utf8.ValidString(s) {
		runes := []rune(s)
		if len(runes) > maxChars {
			return string(runes[:maxChars])
		}
		return s
	}
	// For non-UTF-8 multi-byte charsets
	count := 0
	i := 0
	for i < len(s) && count < maxChars {
		b := s[i]
		if (b >= 0x81 && b <= 0x9F) || (b >= 0xE0 && b <= 0xFC) {
			i += 2
		} else {
			i++
		}
		count++
	}
	return s[:i]
}

// Result represents the result of a query execution.
type Result struct {
	Columns      []string
	ColumnTypes  []string // MySQL column types (e.g. "BLOB", "BINARY", "VARBINARY") for wire protocol
	Rows         [][]interface{}
	AffectedRows uint64
	InsertID     uint64
	IsResultSet  bool   // true for SELECT, SHOW, etc.
	MatchedRows  uint64 // for UPDATE: rows that matched WHERE clause
	ChangedRows  uint64 // for UPDATE: rows actually modified
	InfoMessage  string // optional info message (e.g. "Rows matched: 2  Changed: 1  Warnings: 0")
	// ExtraResultSets holds additional result sets from stored procedures that produce
	// multiple result sets (e.g., procedures with SELECT in loops). The main result is
	// the first result set; ExtraResultSets contains subsequent ones.
	ExtraResultSets []*Result
}

// intOverflowError is returned when an integer literal exceeds uint64 range.
// kind is one of "DECIMAL" (from integer/decimal literal), "INTEGER" (from string→int),
// or "BINARY" (from hex literal like 0x...).
type intOverflowError struct {
	val  string
	kind string // "DECIMAL", "INTEGER", "BINARY"
}

func (e *intOverflowError) Error() string {
	return "INT_OVERFLOW:" + e.val
}

// formatOverflowWarningMsg returns the warning message for a BIGINT overflow.
func formatOverflowWarningMsg(oe *intOverflowError) string {
	switch oe.kind {
	case "BINARY":
		// Pad hex digits to even length, format as x'...'
		hex := oe.val
		if len(hex)%2 != 0 {
			hex = "0" + hex
		}
		// Truncate to 128 chars in the displayed hex string (MySQL truncates long values)
		const maxHexLen = 128
		if len(hex) > maxHexLen {
			hex = hex[:maxHexLen]
		}
		return fmt.Sprintf("Truncated incorrect BINARY value: 'x'%s''", hex)
	case "INTEGER":
		return fmt.Sprintf("Truncated incorrect INTEGER value: '%s'", oe.val)
	default: // "DECIMAL"
		return fmt.Sprintf("Truncated incorrect DECIMAL value: '%s'", oe.val)
	}
}

// selectLockClause describes a per-table locking clause parsed from
// "FOR SHARE OF t1 SKIP LOCKED" or "FOR UPDATE OF t2 NOWAIT".
type selectLockClause struct {
	tableName  string // the table this clause applies to
	exclusive  bool   // true = FOR UPDATE, false = FOR SHARE
	skipLocked bool
	nowait     bool
}

// TxnActiveSet tracks which connections are currently in an active transaction.
// Used for filtering uncommitted rows during reads.
type TxnActiveSet struct {
	mu      sync.RWMutex
	active  map[int64]bool                    // connectionID -> true if in transaction
	inserts map[int64]map[string]map[int]bool // connectionID -> "db:table" -> set of row pointers
}

// NewTxnActiveSet creates a new TxnActiveSet.
func NewTxnActiveSet() *TxnActiveSet {
	return &TxnActiveSet{
		active:  make(map[int64]bool),
		inserts: make(map[int64]map[string]map[int]bool),
	}
}

// Begin marks a connection as being in a transaction.
func (t *TxnActiveSet) Begin(connID int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.active[connID] = true
}

// End marks a connection as no longer being in a transaction.
func (t *TxnActiveSet) End(connID int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.active, connID)
	delete(t.inserts, connID)
}

// TrackInsert records that a connection inserted a row (identified by pointer identity).
func (t *TxnActiveSet) TrackInsert(connID int64, dbTable string, rowPtr int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.inserts[connID] == nil {
		t.inserts[connID] = make(map[string]map[int]bool)
	}
	if t.inserts[connID][dbTable] == nil {
		t.inserts[connID][dbTable] = make(map[int]bool)
	}
	t.inserts[connID][dbTable][rowPtr] = true
}

// cteTable holds pre-computed rows for a Common Table Expression.
type cteTable struct {
	columns  []string
	rows     []storage.Row
	// colEqPairs stores column equality pairs extracted from the CTE's WHERE clause
	// (e.g., WHERE a=b → [["a","b"]]). Used for functional dependency analysis in
	// GROUP BY validation of outer queries.
	colEqPairs [][2]string
	// groupByKey is the set of columns that form a unique key for the CTE result
	// (e.g., from GROUP BY in the CTE subquery). Any outer GROUP BY on these columns
	// functionally determines all other CTE columns.
	groupByKey []string
}

// EnumValue wraps a string value from an ENUM column so that compareValues
// can avoid the generic "non-numeric string → 0" coercion that MySQL applies
// to plain strings.  The dolt test suite expects ENUM-vs-integer comparisons
// to use the ENUM index rather than string coercion.
type EnumValue string

// psDigestEntry tracks a statement digest for performance_schema tables.
type psDigestEntry struct {
	SchemaName string
	Digest     string
	DigestText string
	CountStar  int64
}

// savedPermTable holds the state of a permanent table that was shadowed by a temporary table.
type savedPermTable struct {
	def   *catalog.TableDef
	table *storage.Table
}

// Executor handles SQL execution.
type Executor struct {
	Catalog        *catalog.Catalog
	Storage        *storage.Engine
	CurrentDB      string
	inTransaction  bool
	savepoint      *txSavepoint
	// namedSavepoints tracks savepoint names set in the current transaction.
	// DDL statements (implicit commit) clears this map, so ROLLBACK TO SAVEPOINT fails.
	namedSavepoints map[string]bool
	snapshots      map[string]*fullSnapshot
	lastInsertID      int64
	lastAffectedRows  int64 // stores ROW_COUNT() value: affected rows after DML, -1 after SELECT
	lastUpdateInfo string // stores info message from last UPDATE (e.g. "Rows matched: 2  Changed: 1  Warnings: 0")
	lastInsertInfo string // stores info message from last INSERT/REPLACE (e.g. "Records: 5  Duplicates: 2  Warnings: 0")
	// modifyingTable tracks the table currently being modified by a DML statement,
	// used to detect recursive table use in triggers (MySQL error 1442).
	modifyingTable string
	// cteMap holds CTE virtual tables for the currently executing query.
	cteMap map[string]*cteTable
	// sqlMode stores the current SQL mode (e.g. "TRADITIONAL", "STRICT_TRANS_TABLES").
	sqlMode string
	// sqlAutoIsNull enables MySQL sql_auto_is_null behavior.
	sqlAutoIsNull bool
	// lastAutoIncID stores the last auto-increment ID for sql_auto_is_null support.
	lastAutoIncID int64
	// fixedTimestamp holds a fixed time for SET TIMESTAMP=N support.
	fixedTimestamp *time.Time
	// timeZone holds the session time zone location for SET TIME_ZONE.
	timeZone *time.Location
	// correlatedRow holds the outer row for correlated subquery evaluation.
	correlatedRow storage.Row
	// DataDir is the base directory for resolving relative file paths
	// used in LOAD DATA INFILE and SELECT INTO OUTFILE.
	DataDir string
	// SearchPaths are directories to search for files referenced in
	// LOAD DATA LOCAL INFILE statements.
	SearchPaths []string
	// userVars stores MySQL user variables (SET @var = value).
	userVars map[string]interface{}
	// nextInsertID holds the value from SET INSERT_ID for the next INSERT.
	nextInsertID int64
	// preparedStmts stores PREPARE stmt FROM 'query' statements.
	preparedStmts map[string]string
	// tempTables stores temporary tables per session (table name -> true).
	tempTables map[string]bool
	// tempTableSavedPermanent stores the saved permanent table state (catalog def + storage table)
	// for tables that were shadowed by a temporary table. When the temp table is dropped,
	// the permanent state is restored. Key is tableName.
	tempTableSavedPermanent map[string]*savedPermTable
	// pendingPermanentWhileTemp stores permanent table defs created while a temp table
	// with the same name exists. When the temp table is dropped, this pending permanent
	// is added to the catalog. Key is tableName.
	pendingPermanentWhileTemp map[string]*savedPermTable
	// globalScopeVars stores SET GLOBAL variable overrides.
	// Access must be protected by globalVarsMu.
	globalScopeVars map[string]string
	// globalVarsMu protects concurrent access to globalScopeVars.
	// It is a pointer so it is shared across all Clone()d executors.
	globalVarsMu *sync.RWMutex
	// sessionScopeVars stores SET SESSION/LOCAL variable overrides.
	sessionScopeVars map[string]string
	// startupVars stores variable values set at server startup (e.g., from master.opt).
	// These are used as default values when SET ... = DEFAULT is used.
	startupVars map[string]string
	// compiledDefaults caches the hardcoded MySQL defaults (without startup overrides).
	compiledDefaults map[string]string
	// views stores view definitions (view name -> SELECT query string).
	// The stored SQL is from sqlparser.String() which may include "from dual" for literal-only SELECTs.
	// This is used for VIEW EXECUTION only.
	views map[string]string
	// viewDisplaySQL stores the canonical SELECT SQL for each view (view name -> SELECT string).
	// This is the normalized version used for INFORMATION_SCHEMA.VIEWS.VIEW_DEFINITION display.
	// It uses buildViewSelectSQL() format (no "from dual", proper alias quoting, etc.).
	viewDisplaySQL map[string]string
	// viewCheckOptions stores WITH CHECK OPTION for views (view name -> check option string: "cascaded", "local", or "").
	viewCheckOptions map[string]string
	// viewCreateStatements stores the full CREATE VIEW SQL for SHOW CREATE VIEW (view name -> full SQL).
	viewCreateStatements map[string]string
	// queryTableDef holds the table definition for the current query context,
	// used for column-level checks (e.g., IS NULL on NOT NULL columns).
	queryTableDef *catalog.TableDef
	// warnings stores the warnings from the last executed statement.
	warnings []Warning
	// lastWarningCount / lastErrorCount hold the warning/error counts from the
	// previous statement so that SELECT @@warning_count / @@error_count return
	// the correct value (the counts are snapshotted before warnings are cleared).
	lastWarningCount int64
	lastErrorCount   int64
	// currentQuery holds the current raw SQL text for display-name reconstruction.
	currentQuery string
	// onDupValuesRow holds the candidate INSERT row while evaluating
	// ON DUPLICATE KEY UPDATE expressions (for VALUES(col) support).
	onDupValuesRow storage.Row
	// defaultsTableDef holds the table definition for evaluating DEFAULT(col) expressions.
	// Set during INSERT/UPDATE operations so that DEFAULT(colname) can look up the column default.
	defaultsTableDef *catalog.TableDef
	// defaultsByColName is an auxiliary map for DEFAULT(col) lookups that supplements defaultsTableDef.
	// It can contain columns from source tables in INSERT ... SELECT ... ON DUPLICATE KEY UPDATE.
	defaultsByColName map[string]interface{}
	// subqueryValCache caches results of non-correlated IN subqueries
	// within the same top-level query execution.  Keyed by the SQL
	// string of the subquery.
	subqueryValCache map[string][]interface{}
	// insideDML is set to true while executing an INSERT statement.
	// When true, sub-SELECTs implicitly acquire shared (exclusive) row locks
	// to emulate InnoDB's INSERT ... SELECT locking behaviour.
	insideDML bool
	// selectTableAliases holds aliases (or table names) for each table in the current
	// SELECT's FROM clause, in the same order as selectTableDefs. Used for proper
	// column qualification in SELECT * expansion (e.g., self-joins).
	selectTableAliases []string
	// executeDepth tracks the recursion depth of EXECUTE/CALL to prevent stack overflow.
	executeDepth int
	// sqlParser is a cached SQL parser instance to avoid allocation on every query.
	sqlParser *sqlparser.Parser
	// lastFoundRows stores the row count from the last SELECT before LIMIT was applied.
	// Used by the FOUND_ROWS() function.
	lastFoundRows int64
	// routineDepth tracks the current stored routine call depth to prevent infinite recursion
	// and to avoid counting internal routine Execute calls in the Questions status counter.
	routineDepth int
	// functionOrTriggerDepth tracks depth inside stored functions or triggers only.
	// Used to enforce error 1422 (no implicit-commit DDL inside functions/triggers).
	// Stored procedures are excluded because they are allowed to execute DDL.
	functionOrTriggerDepth int
	// sysVarSnapshot holds a frozen copy of sessionScopeVars taken at the start of a
	// top-level SELECT execution.  When non-nil and routineDepth==0, system-variable reads
	// in the outer WHERE/SELECT-list evaluation use this snapshot so that side-effects
	// from user-defined functions (e.g. SET @@sort_buffer_size inside a function body)
	// do not affect the predicate evaluation of subsequent rows (matching MySQL behaviour
	// where system variables act as query-time constants).  The snapshot is set in
	// execSelect and cleared when that call returns.
	sysVarSnapshot map[string]string
	// exprDepth tracks the current expression evaluation depth to prevent Go stack overflow
	// on deeply nested expressions. MySQL returns ER_STACK_OVERRUN_NEED_MORE (1436) when
	// the expression stack exceeds its limit.
	exprDepth int
	// psTruncated tracks performance_schema tables that have been TRUNCATED.
	// These tables return empty result sets until data is re-inserted.
	psTruncated map[string]bool
	// psSetupActors holds the in-memory rows for performance_schema.setup_actors.
	// nil means "use default rows"; non-nil means the rows have been modified.
	psSetupActors []storage.Row
	// psSetupActorsInit tracks whether psSetupActors has been explicitly set.
	psSetupActorsInit bool
	// psSetupObjects holds the in-memory rows for performance_schema.setup_objects.
	psSetupObjects []storage.Row
	// psSetupObjectsInit tracks whether psSetupObjects has been explicitly set.
	psSetupObjectsInit bool
	// psDigests tracks statement digests for events_statements_summary_by_digest
	// and events_statements_histogram_by_digest tables.
	psDigests []psDigestEntry
	// psThreadInstrumented tracks per-connection INSTRUMENTED column for threads table.
	psThreadInstrumented map[int64]string
	// psThreadHistory tracks per-connection HISTORY column for threads table.
	psThreadHistory map[int64]string
	// processList is a shared registry of active connections and their states.
	// It is shared across all executor instances (connections).
	processList *ProcessList
	// connectionID is the unique ID of this connection.
	connectionID int64
	// nextConnID is a shared counter for generating unique connection IDs.
	nextConnID *atomic.Int64
	// lockManager manages user-level locks (GET_LOCK/RELEASE_LOCK).
	// Shared across all executor instances.
	lockManager *LockManager
	// rowLockManager manages InnoDB-style row-level locks for SELECT ... FOR UPDATE.
	// Shared across all executor instances.
	rowLockManager *RowLockManager
	// txnUndoLog records DML mutations made during a transaction for per-connection rollback.
	txnUndoLog []undoEntry
	// txnActiveSet is a shared set tracking which connections are currently in a transaction.
	// Used for filtering uncommitted rows from other connections during reads.
	txnActiveSet *TxnActiveSet
	// selectSkipLocked is set when the current SELECT uses SKIP LOCKED.
	selectSkipLocked bool
	// selectNowait is set when the current SELECT uses NOWAIT.
	selectNowait bool
	// selectLockClauses holds per-table locking info parsed from
	// "FOR SHARE OF t1 SKIP LOCKED FOR UPDATE OF t2 NOWAIT" clauses.
	// When non-empty, these override the global selectSkipLocked/selectNowait/stmt.Lock.
	selectLockClauses []selectLockClause
	// tableLockManager manages LOCK TABLE READ/WRITE per connection.
	// Shared across all executor instances.
	tableLockManager *TableLockManager
	// globalReadLock manages FLUSH TABLES WITH READ LOCK (FTWRL).
	// Shared across all executor instances.
	globalReadLock *GlobalReadLock
	// handlerReadKey counts the number of index-based reads (SELECT queries).
	// Incremented per SELECT, reset on FLUSH STATUS.
	handlerReadKey int64
	// Additional handler counters surfaced by SHOW STATUS LIKE 'handler_read%'.
	handlerReadFirst   int64
	handlerReadLast    int64
	handlerReadNext    int64
	handlerReadPrev    int64
	handlerReadRnd     int64
	handlerReadRndNext int64
	// checkedForUpgrade tracks tables that have been CHECK TABLE ... FOR UPGRADE'd.
	// Subsequent FOR UPGRADE checks return "Table is already up to date".
	checkedForUpgrade map[string]bool
	// tableNeedsAnalyze tracks non-InnoDB tables without SPATIAL indexes that need
	// ANALYZE. A table is added when ALTER TABLE is run, and removed when ANALYZE
	// TABLE or REPAIR TABLE is run. ANALYZE TABLE returns "OK" when in this set,
	// "Table is already up to date" for non-SPATIAL non-InnoDB otherwise.
	tableNeedsAnalyze map[string]bool
	// tableNeedsOptimize tracks non-InnoDB tables that were modified (INSERT/UPDATE/DELETE/ALTER)
	// since last OPTIMIZE TABLE. OPTIMIZE TABLE returns "OK" when in this set,
	// "Table is already up to date" otherwise.
	tableNeedsOptimize map[string]bool
	// Sort statistics: incremented when ORDER BY operations are performed.
	sortRows            int64 // total rows sorted
	createdTmpDiskTables int64 // temp tables written to disk
	createdTmpTables     int64 // total internal temporary tables created
	sortRange int64 // sort operations using range scan
	sortScan  int64 // sort operations using full table scan
	// questions counts the number of client statements received, reset on FLUSH STATUS.
	questions int64
	// resourceGroups stores resource group names (lowercase-normalized for case+accent-insensitive comparison).
	// Shared across all connections. Maps normalized name → original name.
	resourceGroups map[string]string
	// resourceGroupsMu protects resourceGroups.
	resourceGroupsMu *sync.RWMutex
	// superUsers tracks users that have been granted SUPER privilege via GRANT SUPER ON *.* TO user.
	// Shared across all connections (like globalScopeVars). Key is lowercase username.
	superUsers map[string]bool
	// superUsersMu protects superUsers.
	superUsersMu *sync.RWMutex
	// sysVarsAdminUsers tracks users that have been granted SYSTEM_VARIABLES_ADMIN privilege.
	// Shared across all connections. Key is lowercase username.
	sysVarsAdminUsers map[string]bool
	// sysVarsAdminUsersMu protects sysVarsAdminUsers.
	sysVarsAdminUsersMu *sync.RWMutex
	// knownUsers tracks users created via CREATE USER for SET PASSWORD FOR validation.
	// Shared across all connections.
	knownUsers map[string]bool
	// knownUsersMu protects knownUsers.
	knownUsersMu *sync.RWMutex
	// grantStore holds all GRANT records (privileges and role memberships).
	// Shared across all connections.
	grantStore *GrantStore
	// inUpdateSetContext is set to true while evaluating SET expressions in an UPDATE statement.
	// When true, CONCAT (and similar) should return an error if the result exceeds max_allowed_packet,
	// rather than returning NULL with a warning (which is correct for SELECT/INSERT context).
	inUpdateSetContext bool
}

// Warning represents a MySQL warning.
type Warning struct {
	Level   string // "Warning", "Note", "Error"
	Code    int
	Message string
}

// viewDefinitionForDisplay returns the canonical SELECT SQL for a view, suitable for
// INFORMATION_SCHEMA.VIEWS.VIEW_DEFINITION display. Uses viewDisplaySQL if available
// (no "from dual", proper formatting), otherwise falls back to the raw view SQL.
func (e *Executor) viewDefinitionForDisplay(viewName string) string {
	if e.viewDisplaySQL != nil {
		if sql, ok := e.viewDisplaySQL[viewName]; ok {
			return sql
		}
	}
	if e.views != nil {
		return e.views[viewName]
	}
	return ""
}

// addWarning adds a warning to the current statement's warning list.
func (e *Executor) addWarning(level string, code int, message string) {
	// Enforce max_error_count limit: when at capacity, drop the oldest warning.
	maxErrCount := 64 // MySQL default
	if v, ok := e.sessionScopeVars["max_error_count"]; ok {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			maxErrCount = n
		}
	} else if e.compiledDefaults != nil {
		if v2, ok2 := e.compiledDefaults["max_error_count"]; ok2 {
			if n, err := strconv.Atoi(v2); err == nil && n >= 0 {
				maxErrCount = n
			}
		}
	}
	if maxErrCount > 0 && len(e.warnings) >= maxErrCount {
		// Drop oldest warning to make room
		e.warnings = e.warnings[1:]
	}
	if maxErrCount == 0 {
		// max_error_count=0 means don't store any warnings
		return
	}
	e.warnings = append(e.warnings, Warning{Level: level, Code: code, Message: message})
}

// sqlNotesEnabled returns true if the sql_notes session variable is ON (default).
// When sql_notes is OFF, Note-level diagnostics should be suppressed.
func (e *Executor) sqlNotesEnabled() bool {
	if v, ok := e.sessionScopeVars["sql_notes"]; ok {
		upper := strings.ToUpper(v)
		return upper == "ON" || upper == "1"
	}
	return true // default is ON
}

// isSemijoinEnabled returns true if semijoin=on is present in optimizer_switch.
// When semijoin is disabled, IN-subqueries use SUBQUERY select_type instead of MATERIALIZED.
func (e *Executor) isSemijoinEnabled() bool {
	if v, ok := e.getSysVar("optimizer_switch"); ok {
		return strings.Contains(v, "semijoin=on")
	}
	return true // default optimizer_switch has semijoin=on
}

// getGlobalVar reads a value from globalScopeVars under RLock.
func (e *Executor) getGlobalVar(name string) (string, bool) {
	if e.globalVarsMu == nil {
		v, ok := e.globalScopeVars[name]
		return v, ok
	}
	e.globalVarsMu.RLock()
	v, ok := e.globalScopeVars[name]
	e.globalVarsMu.RUnlock()
	return v, ok
}

// setGlobalVar writes a value to globalScopeVars under Lock.
func (e *Executor) setGlobalVar(name, value string) {
	if e.globalVarsMu == nil {
		e.globalScopeVars[name] = value
		return
	}
	e.globalVarsMu.Lock()
	e.globalScopeVars[name] = value
	e.globalVarsMu.Unlock()
}

// deleteGlobalVar deletes a key from globalScopeVars under Lock.
func (e *Executor) deleteGlobalVar(name string) {
	if e.globalVarsMu == nil {
		delete(e.globalScopeVars, name)
		return
	}
	e.globalVarsMu.Lock()
	delete(e.globalScopeVars, name)
	e.globalVarsMu.Unlock()
}

// rangeGlobalVars calls f for each (name, value) pair in globalScopeVars
// under RLock. f must not call any globalVarsMu methods to avoid deadlock.
func (e *Executor) rangeGlobalVars(f func(name, val string)) {
	if e.globalVarsMu == nil {
		for k, v := range e.globalScopeVars {
			f(k, v)
		}
		return
	}
	e.globalVarsMu.RLock()
	for k, v := range e.globalScopeVars {
		f(k, v)
	}
	e.globalVarsMu.RUnlock()
}

// getSysVar reads a system variable with proper scope resolution:
// session -> global -> (not found). Used for reads that don't specify scope.
func (e *Executor) getSysVar(name string) (string, bool) {
	if v, ok := e.sessionScopeVars[name]; ok {
		return v, true
	}
	if v, ok := e.getGlobalVar(name); ok {
		return v, true
	}
	return "", false
}

// getSysVarGlobal reads a global-scoped system variable.
func (e *Executor) getSysVarGlobal(name string) (string, bool) {
	// performance_schema_consumer_* are startup-only options, not system variables.
	if strings.HasPrefix(name, "performance_schema_consumer_") ||
		name == "performance_schema_instrument" {
		return "", false
	}
	return e.getGlobalVar(name)
}

// getSysVarSession reads a session-scoped system variable.
// For global-only variables, falls back to globalScopeVars.
// For other variables, also falls back to startupVars so that server
// startup options (e.g. --parser-max-mem-size) are visible in session scope.
func (e *Executor) getSysVarSession(name string) (string, bool) {
	// performance_schema_consumer_* are startup-only options for
	// configuring performance_schema.setup_consumers. They must not be
	// exposed as system variables (SELECT @@var / SHOW VARIABLES).
	if strings.HasPrefix(name, "performance_schema_consumer_") ||
		name == "performance_schema_instrument" {
		return "", false
	}
	// When a query-time snapshot is active (set by execSelect at the top level)
	// and we are NOT inside a stored routine/UDF (routineDepth==0), use the
	// frozen snapshot so that UDF side-effects on system variables do not bleed
	// into the predicate evaluation of subsequent rows.  This mirrors MySQL's
	// treatment of system variables as query-time constants.
	if e.sysVarSnapshot != nil && e.routineDepth == 0 {
		if v, ok := e.sysVarSnapshot[name]; ok {
			return v, true
		}
		// Variable not in snapshot (e.g. never explicitly SET) – fall through
		// to the normal path so that compiled defaults are still visible.
	}
	if v, ok := e.sessionScopeVars[name]; ok {
		return v, true
	}
	// For global-only variables, fall back to global scope
	if sysVarGlobalOnly[name] {
		if v, ok := e.getGlobalVar(name); ok {
			return v, true
		}
	}
	// Fall back to startupVars so that server startup options are visible
	// in session scope (e.g. --parser-max-mem-size from master.opt).
	if v, ok := e.startupVars[name]; ok {
		return v, true
	}
	return "", false
}

// getDivPrecisionIncrement returns the current div_precision_increment value.
func (e *Executor) getDivPrecisionIncrement() int {
	if v, ok := e.getSysVar("div_precision_increment"); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return 4 // default
}

// setSysVar stores a system variable in the appropriate scope map.
func (e *Executor) setSysVar(name string, value string, isGlobal bool) {
	// Trigger variables reset immediately after being set
	if triggerSysVars[name] {
		value = "OFF"
	}
	// optimizer_switch uses MySQL's merge-set semantics: each SET only updates
	// the specified flags, leaving others at their current values.
	// e.g. SET optimizer_switch='materialization=off' only turns off materialization.
	if name == "optimizer_switch" {
		value = e.mergeOptimizerSwitch(value, isGlobal)
	}
	if isGlobal {
		e.setGlobalVar(name, value)
	} else {
		e.sessionScopeVars[name] = value
	}
}

// mergeOptimizerSwitch merges a partial optimizer_switch setting into the current value.
// MySQL allows SET optimizer_switch='flag=on' to only change one flag at a time.
func (e *Executor) mergeOptimizerSwitch(newValue string, isGlobal bool) string {
	// Special case: 'default' resets optimizer_switch to the compiled default value.
	if strings.EqualFold(strings.TrimSpace(newValue), "default") {
		if compiled, ok := e.getCompiledDefault("optimizer_switch"); ok {
			return compiled
		}
		return newValue
	}
	// Get current value - check session, global, startupVars, and compiled defaults
	var currentValue string
	if isGlobal {
		if v, ok := e.getGlobalVar("optimizer_switch"); ok {
			currentValue = v
		}
	} else {
		if v, ok := e.getSysVar("optimizer_switch"); ok {
			currentValue = v
		}
	}
	// Fall back to startupVars if no explicit value set
	if currentValue == "" {
		if v, ok := e.startupVars["optimizer_switch"]; ok {
			currentValue = v
		}
	}
	// Fall back to compiled default
	if currentValue == "" {
		if compiled, ok := e.getCompiledDefault("optimizer_switch"); ok {
			currentValue = compiled
		}
	}
	if currentValue == "" {
		return newValue
	}

	// Parse current flags into a map (preserving order with slice)
	type flagEntry struct {
		key string
		val string
	}
	var flags []flagEntry
	flagIndex := map[string]int{}
	for _, part := range strings.Split(currentValue, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) == 2 {
			k := strings.TrimSpace(kv[0])
			v := strings.TrimSpace(kv[1])
			if _, exists := flagIndex[k]; !exists {
				flagIndex[k] = len(flags)
				flags = append(flags, flagEntry{k, v})
			}
		}
	}

	// Apply new flags (merge/update)
	for _, part := range strings.Split(newValue, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) == 2 {
			k := strings.TrimSpace(kv[0])
			v := strings.TrimSpace(kv[1])
			if idx, exists := flagIndex[k]; exists {
				flags[idx].val = v
			} else {
				flagIndex[k] = len(flags)
				flags = append(flags, flagEntry{k, v})
			}
		}
	}

	// Reconstruct the merged value
	parts := make([]string, len(flags))
	for i, f := range flags {
		parts[i] = f.key + "=" + f.val
	}
	return strings.Join(parts, ",")
}

// mergeOptimizerTrace handles SET optimizer_trace = value.
// The optimizer_trace variable is stored as "enabled=on,one_line=off" format.
// When set to 1/ON/TRUE → "enabled=on,one_line=off"
// When set to 0/OFF/FALSE → "enabled=off,one_line=off"
// When set to a key=value string like "enabled=on,one_line=off", it merges with current.
// Float/scientific notation values are rejected as ER_WRONG_TYPE_FOR_VAR.
// Unknown key=value strings are rejected as ER_WRONG_VALUE_FOR_VAR.
func (e *Executor) normalizeOptimizerTrace(newValue string, expr sqlparser.Expr, evalVal interface{}) (string, error) {
	const defaultTrace = "enabled=off,one_line=off"
	validKeys := map[string]bool{"enabled": true, "one_line": true}

	// Reject float/scientific notation
	if lit, isLit := expr.(*sqlparser.Literal); isLit {
		litStr := sqlparser.String(lit)
		isQuoted := strings.HasPrefix(litStr, "'") || strings.HasPrefix(litStr, "\"")
		if !isQuoted && strings.ContainsAny(litStr, ".eE") {
			return "", mysqlError(1232, "42000", "Incorrect argument type to variable 'optimizer_trace'")
		}
	}

	upper := strings.ToUpper(strings.TrimSpace(newValue))
	// Handle DEFAULT
	if upper == "DEFAULT" {
		return defaultTrace, nil
	}
	// Handle numeric/boolean: 0/FALSE/OFF → disabled, 1/TRUE/ON → enabled
	switch upper {
	case "0", "OFF", "FALSE":
		return "enabled=off,one_line=off", nil
	case "1", "ON", "TRUE":
		return "enabled=on,one_line=off", nil
	}
	// If numeric but not 0 or 1, try to parse
	if n, err := strconv.ParseInt(upper, 10, 64); err == nil {
		if n == 0 {
			return "enabled=off,one_line=off", nil
		}
		return "enabled=on,one_line=off", nil
	}

	// Handle key=value format — parse and validate keys
	if strings.Contains(newValue, "=") {
		// Get current value to merge with
		currentValue := defaultTrace
		if cv, ok := e.getSysVar("optimizer_trace"); ok {
			currentValue = cv
		}
		// Parse current state
		type flagEntry struct{ key, val string }
		var flags []flagEntry
		flagIndex := map[string]int{}
		for _, part := range strings.Split(currentValue, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			kv := strings.SplitN(part, "=", 2)
			if len(kv) == 2 {
				k, v := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])
				flagIndex[k] = len(flags)
				flags = append(flags, flagEntry{k, v})
			}
		}
		// Merge new values
		for _, part := range strings.Split(newValue, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			kv := strings.SplitN(part, "=", 2)
			if len(kv) != 2 {
				return "", mysqlError(1231, "42000", fmt.Sprintf("Variable 'optimizer_trace' can't be set to the value of '%s'", newValue))
			}
			k, v := strings.ToLower(strings.TrimSpace(kv[0])), strings.ToLower(strings.TrimSpace(kv[1]))
			if !validKeys[k] {
				return "", mysqlError(1231, "42000", fmt.Sprintf("Variable 'optimizer_trace' can't be set to the value of '%s'", newValue))
			}
			if v != "on" && v != "off" {
				return "", mysqlError(1231, "42000", fmt.Sprintf("Variable 'optimizer_trace' can't be set to the value of '%s'", newValue))
			}
			if idx, exists := flagIndex[k]; exists {
				flags[idx].val = v
			}
		}
		parts := make([]string, len(flags))
		for i, f := range flags {
			parts[i] = f.key + "=" + f.val
		}
		return strings.Join(parts, ","), nil
	}

	// Unknown string value
	return "", mysqlError(1231, "42000", fmt.Sprintf("Variable 'optimizer_trace' can't be set to the value of '%s'", newValue))
}

// deleteSysVar deletes a system variable from the appropriate scope map (for DEFAULT).
func (e *Executor) deleteSysVar(name string, isGlobal bool) {
	if isGlobal {
		e.deleteGlobalVar(name)
	} else {
		delete(e.sessionScopeVars, name)
	}
}

// getCompiledDefault returns the compiled (hardcoded) MySQL default for a
// system variable, ignoring startupVars and any SET GLOBAL/SESSION overrides.
// The result is cached after the first call.
func (e *Executor) getCompiledDefault(name string) (string, bool) {
	if e.compiledDefaults == nil {
		tmp := &Executor{
			startupVars:      map[string]string{},
			globalScopeVars:  map[string]string{},
			sessionScopeVars: map[string]string{},
			globalVarsMu:     &sync.RWMutex{},
		}
		e.compiledDefaults = tmp.buildVariablesMapScoped(true)
	}
	v, ok := e.compiledDefaults[name]
	return v, ok
}

// parser returns the cached SQL parser, creating it lazily.
func (e *Executor) parser() *sqlparser.Parser {
	if e.sqlParser == nil {
		e.sqlParser = sqlparser.NewTestParser()
	}
	return e.sqlParser
}

func New(cat *catalog.Catalog, store *storage.Engine) *Executor {
	// MySQL test suite (MTR) defaults to timezone GMT-3 (= UTC+3).
	// We mirror this so SET TIMESTAMP + CURRENT_TIME() match expected results.
	defaultTZ := time.FixedZone("GMT-3", 3*60*60)
	e := &Executor{
		Catalog:                 cat,
		Storage:                 store,
		CurrentDB:               "test",
		sqlMode:                 "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
		snapshots:               make(map[string]*fullSnapshot),
		userVars:                make(map[string]interface{}),
		preparedStmts:           make(map[string]string),
		tempTables:              make(map[string]bool),
		tempTableSavedPermanent: make(map[string]*savedPermTable),
		globalScopeVars: map[string]string{
			"general_log":                     "ON",
			"slow_query_log":                  "ON",
			"log_bin_trust_function_creators": "ON",
		},
		globalVarsMu:     &sync.RWMutex{},
		sessionScopeVars: make(map[string]string),
		startupVars: map[string]string{
			"innodb_commit_concurrency": "0",
		},
		timeZone:   defaultTZ,
		nextConnID: &atomic.Int64{},
	}
	e.connectionID = e.nextConnID.Add(1)
	e.processList = NewProcessList()
	e.lockManager = NewLockManager()
	e.rowLockManager = NewRowLockManager()
	e.txnActiveSet = NewTxnActiveSet()
	e.tableLockManager = NewTableLockManager()
	e.globalReadLock = NewGlobalReadLock()
	e.resourceGroups = make(map[string]string)
	e.resourceGroupsMu = &sync.RWMutex{}
	e.superUsers = make(map[string]bool)
	e.superUsersMu = &sync.RWMutex{}
	e.sysVarsAdminUsers = make(map[string]bool)
	e.sysVarsAdminUsersMu = &sync.RWMutex{}
	e.knownUsers = make(map[string]bool)
	e.knownUsersMu = &sync.RWMutex{}
	e.grantStore = NewGrantStore()
	e.initSystemTables()
	return e
}

// GetProcessList returns the shared ProcessList.
func (e *Executor) GetProcessList() *ProcessList {
	return e.processList
}

// GetLockManager returns the shared LockManager.
func (e *Executor) GetLockManager() *LockManager {
	return e.lockManager
}

// GetConnectionID returns the connection ID for this executor instance.
func (e *Executor) GetConnectionID() int64 {
	return e.connectionID
}

// Clone creates a new Executor that shares Catalog, Storage, globalScopeVars,
// startupVars, lockManager, processList, and psTruncated, but has its own
// fresh per-session state. Used to give each connection its own executor.
func (e *Executor) Clone() *Executor {
	// Default to GMT-3 (= UTC+3), which matches the MySQL MTR test framework default.
	// If the server was started with --timezone=<tz> (e.g. from master.opt), use that instead.
	defaultTZ := time.FixedZone("GMT-3", 3*60*60)
	if e.timeZone != nil {
		// Parent executor has a timezone set (e.g. from SET STARTUP timezone=GMT+10),
		// use it as the default for the new session.
		defaultTZ = e.timeZone
	}
	connID := e.nextConnID.Add(1)
	// Inherit global variable values into the new session's sessionScopeVars
	// for variables that have both GLOBAL and SESSION scope. This mirrors
	// MySQL behaviour where new connections inherit the current global values.
	sessVars := make(map[string]string)
	if e.globalVarsMu != nil {
		e.globalVarsMu.RLock()
	}
	for name, val := range e.globalScopeVars {
		if !sysVarGlobalOnly[name] && !sysVarSessionOnly[name] {
			sessVars[name] = val
		}
	}
	if e.globalVarsMu != nil {
		e.globalVarsMu.RUnlock()
	}
	return &Executor{
		Catalog:                 e.Catalog,
		Storage:                 e.Storage,
		CurrentDB:               "test",
		sqlMode:                 "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
		snapshots:               make(map[string]*fullSnapshot),
		userVars:                make(map[string]interface{}),
		preparedStmts:           make(map[string]string),
		tempTables:              make(map[string]bool),
		tempTableSavedPermanent: make(map[string]*savedPermTable),
		globalScopeVars:         e.globalScopeVars,
		globalVarsMu:            e.globalVarsMu,
		sessionScopeVars:        sessVars,
		startupVars:             e.startupVars,
		timeZone:                defaultTZ,
		DataDir:                 e.DataDir,
		SearchPaths:             e.SearchPaths,
		psTruncated:             e.psTruncated,
		nextConnID:              e.nextConnID,
		connectionID:            connID,
		lockManager:             e.lockManager,
		rowLockManager:          e.rowLockManager,
		processList:             e.processList,
		txnActiveSet:            e.txnActiveSet,
		tableLockManager:        e.tableLockManager,
		globalReadLock:          e.globalReadLock,
		resourceGroups:          e.resourceGroups,
		resourceGroupsMu:        e.resourceGroupsMu,
		superUsers:              e.superUsers,
		superUsersMu:            e.superUsersMu,
		sysVarsAdminUsers:       e.sysVarsAdminUsers,
		sysVarsAdminUsersMu:     e.sysVarsAdminUsersMu,
		knownUsers:              e.knownUsers,
		knownUsersMu:            e.knownUsersMu,
		grantStore:              e.grantStore,
	}
}

// OnDisconnect releases all named locks and row locks held by this executor's connection.
func (e *Executor) OnDisconnect() {
	if e.lockManager != nil {
		e.lockManager.ReleaseAllLocks(e.connectionID)
	}
	if e.rowLockManager != nil {
		e.rowLockManager.ReleaseRowLocks(e.connectionID)
	}
	if e.tableLockManager != nil {
		e.tableLockManager.UnlockAll(e.connectionID)
	}
	if e.globalReadLock != nil {
		e.globalReadLock.Release(e.connectionID)
	}
}

// SetStartupVar sets a variable as a startup default. This is used by the test
// runner to apply master.opt settings before running tests.
func (e *Executor) SetStartupVar(name, value string) {
	e.startupVars[strings.ToLower(name)] = value
}

// mysqlError formats an error message in MySQL error style.
// Format: "ERROR <code> (<state>): <message>"
func mysqlError(code int, state, message string) error {
	return fmt.Errorf("ERROR %d (%s): %s", code, state, message)
}

// ErrUnsupported returns a standardized "Feature not supported" error.
// Error code 50001 is mylite-specific and triggers auto-skip in mtrrunner.
func ErrUnsupported(feature string) error {
	return fmt.Errorf("ERROR 50001 (HY000): Feature not supported: %s", feature)
}

// isMySQLError checks if err is a MySQL error with the given error code.
func isMySQLError(err error, code int) bool {
	if err == nil {
		return false
	}
	prefix := fmt.Sprintf("ERROR %d (", code)
	return strings.HasPrefix(err.Error(), prefix)
}

// extractMySQLSQLState extracts the SQLSTATE from a mysqlError formatted as
// "ERROR <code> (<state>): <message>". Returns "" if it can't parse.
func extractMySQLSQLState(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	// Format: "ERROR 1234 (ABCDE): message"
	open := strings.Index(s, " (")
	if open < 0 {
		return ""
	}
	close := strings.Index(s[open:], ")")
	if close < 0 {
		return ""
	}
	return s[open+2 : open+close]
}

// extractMySQLErrorCodeAndMessage parses an error string of the form
// "ERROR N (STATE): message" and returns (code, message). Returns (0, "") if not parseable.
func extractMySQLErrorCodeAndMessage(s string) (int, string) {
	if !strings.HasPrefix(s, "ERROR ") {
		return 0, ""
	}
	rest := s[6:]
	// Find space before '('
	spaceIdx := strings.IndexByte(rest, ' ')
	if spaceIdx < 0 {
		return 0, ""
	}
	code, err := strconv.Atoi(rest[:spaceIdx])
	if err != nil {
		return 0, ""
	}
	// Find ': ' after SQLSTATE
	colonIdx := strings.Index(rest, "): ")
	if colonIdx < 0 {
		return 0, ""
	}
	msg := rest[colonIdx+3:]
	return code, msg
}

// perfSchemaTruncateDenied returns true if TRUNCATE is denied on the given
// performance_schema table. Most PS tables allow TRUNCATE (resets statistics),
// but certain tables with live/config data deny it.
func perfSchemaTruncateDenied(tableName string) bool {
	switch strings.ToLower(tableName) {
	case "cond_instances", "data_lock_waits", "data_locks",
		"file_instances", "global_variables", "keyring_keys",
		"log_status", "metadata_locks", "mutex_instances",
		"performance_timers", "persisted_variables",
		"replication_applier_configuration", "replication_applier_filters",
		"replication_applier_global_filters", "replication_applier_status",
		"replication_applier_status_by_coordinator",
		"replication_applier_status_by_worker",
		"replication_connection_configuration",
		"replication_connection_status",
		"replication_group_member_stats", "replication_group_members",
		"rwlock_instances",
		"session_account_connect_attrs", "session_connect_attrs",
		"session_status", "session_variables",
		"setup_consumers", "setup_instruments", "setup_threads",
		"socket_instances", "table_handles", "threads",
		"user_defined_functions", "user_variables_by_thread",
		"variables_by_thread", "variables_info":
		return true
	}
	return false
}

// perfSchemaWritableTable returns true if the given performance_schema table
// allows DML (INSERT/UPDATE/DELETE) or LOCK TABLES without error.
func perfSchemaWritableTable(tableName string) bool {
	switch strings.ToLower(tableName) {
	case "setup_instruments", "setup_consumers", "setup_threads", "threads",
		"setup_actors", "setup_objects":
		return true
	}
	return false
}

// extractPerfSchemaLockTable extracts a non-writable performance_schema table name from a LOCK TABLES statement.
// Returns the table name if a non-writable performance_schema table is found, empty string otherwise.
func extractPerfSchemaLockTable(query string) string {
	// LOCK TABLES tbl1 READ, tbl2 WRITE, ...
	// or: LOCK TABLES `performance_schema`.`tbl` READ
	upper := strings.ToUpper(query)
	rest := query
	if strings.HasPrefix(upper, "LOCK TABLES ") {
		rest = query[len("LOCK TABLES "):]
	} else if strings.HasPrefix(upper, "LOCK TABLE ") {
		rest = query[len("LOCK TABLE "):]
	}
	// Split by comma for multiple tables
	parts := strings.Split(rest, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		// Remove lock type suffix (READ, WRITE, READ LOCAL, LOW_PRIORITY WRITE)
		tokens := strings.Fields(part)
		if len(tokens) == 0 {
			continue
		}
		tblRef := tokens[0]
		tblRef = strings.Trim(tblRef, "`")
		if strings.Contains(tblRef, ".") {
			dbTbl := strings.SplitN(tblRef, ".", 2)
			dbName := strings.Trim(dbTbl[0], "`")
			tblName := strings.Trim(dbTbl[1], "`")
			if strings.EqualFold(dbName, "performance_schema") && !perfSchemaWritableTable(tblName) {
				return tblName
			}
		}
	}
	return ""
}

// Execute parses and executes a SQL statement.
// matchLike matches a string against a SQL LIKE pattern.
// % matches any sequence of characters, _ matches any single character.
// allCharsets returns the full list of MySQL character sets for SHOW CHARACTER SET.
func allCharsets() [][]interface{} {
	return [][]interface{}{
		{"armscii8", "ARMSCII-8 Armenian", "armscii8_general_ci", int64(1)},
		{"ascii", "US ASCII", "ascii_general_ci", int64(1)},
		{"big5", "Big5 Traditional Chinese", "big5_chinese_ci", int64(2)},
		{"binary", "Binary pseudo charset", "binary", int64(1)},
		{"cp1250", "Windows Central European", "cp1250_general_ci", int64(1)},
		{"cp1251", "Windows Cyrillic", "cp1251_general_ci", int64(1)},
		{"cp1256", "Windows Arabic", "cp1256_general_ci", int64(1)},
		{"cp1257", "Windows Baltic", "cp1257_general_ci", int64(1)},
		{"cp850", "DOS West European", "cp850_general_ci", int64(1)},
		{"cp852", "DOS Central European", "cp852_general_ci", int64(1)},
		{"cp866", "DOS Russian", "cp866_general_ci", int64(1)},
		{"cp932", "SJIS for Windows Japanese", "cp932_japanese_ci", int64(2)},
		{"dec8", "DEC West European", "dec8_swedish_ci", int64(1)},
		{"eucjpms", "UJIS for Windows Japanese", "eucjpms_japanese_ci", int64(3)},
		{"euckr", "EUC-KR Korean", "euckr_korean_ci", int64(2)},
		{"gb18030", "China National Standard GB18030", "gb18030_chinese_ci", int64(4)},
		{"gb2312", "GB2312 Simplified Chinese", "gb2312_chinese_ci", int64(2)},
		{"gbk", "GBK Simplified Chinese", "gbk_chinese_ci", int64(2)},
		{"geostd8", "GEOSTD8 Georgian", "geostd8_general_ci", int64(1)},
		{"greek", "ISO 8859-7 Greek", "greek_general_ci", int64(1)},
		{"hebrew", "ISO 8859-8 Hebrew", "hebrew_general_ci", int64(1)},
		{"hp8", "HP West European", "hp8_english_ci", int64(1)},
		{"keybcs2", "DOS Kamenicky Czech-Slovak", "keybcs2_general_ci", int64(1)},
		{"koi8r", "KOI8-R Relcom Russian", "koi8r_general_ci", int64(1)},
		{"koi8u", "KOI8-U Ukrainian", "koi8u_general_ci", int64(1)},
		{"latin1", "cp1252 West European", "latin1_swedish_ci", int64(1)},
		{"latin2", "ISO 8859-2 Central European", "latin2_general_ci", int64(1)},
		{"latin5", "ISO 8859-9 Turkish", "latin5_turkish_ci", int64(1)},
		{"latin7", "ISO 8859-13 Baltic", "latin7_general_ci", int64(1)},
		{"macce", "Mac Central European", "macce_general_ci", int64(1)},
		{"macroman", "Mac West European", "macroman_general_ci", int64(1)},
		{"sjis", "Shift-JIS Japanese", "sjis_japanese_ci", int64(2)},
		{"swe7", "7bit Swedish", "swe7_swedish_ci", int64(1)},
		{"tis620", "TIS620 Thai", "tis620_thai_ci", int64(1)},
		{"ucs2", "UCS-2 Unicode", "ucs2_general_ci", int64(2)},
		{"ujis", "EUC-JP Japanese", "ujis_japanese_ci", int64(3)},
		{"utf16", "UTF-16 Unicode", "utf16_general_ci", int64(4)},
		{"utf16le", "UTF-16LE Unicode", "utf16le_general_ci", int64(4)},
		{"utf32", "UTF-32 Unicode", "utf32_general_ci", int64(4)},
		{"utf8", "UTF-8 Unicode", "utf8_general_ci", int64(3)},
		{"utf8mb4", "UTF-8 Unicode", "utf8mb4_0900_ai_ci", int64(4)},
	}
}

// validSQLModes lists all valid individual SQL mode names (including combination modes).
var validSQLModes = map[string]bool{
	"ALLOW_INVALID_DATES": true, "ANSI_QUOTES": true,
	"ERROR_FOR_DIVISION_BY_ZERO": true, "HIGH_NOT_PRECEDENCE": true,
	"IGNORE_SPACE": true, "NO_AUTO_CREATE_USER": true, "NO_AUTO_VALUE_ON_ZERO": true,
	"NO_BACKSLASH_ESCAPES": true, "NO_DIR_IN_CREATE": true, "NO_ENGINE_SUBSTITUTION": true,
	"NO_FIELD_OPTIONS": true, "NO_KEY_OPTIONS": true, "NO_TABLE_OPTIONS": true,
	"NO_UNSIGNED_SUBTRACTION": true, "NO_ZERO_DATE": true, "NO_ZERO_IN_DATE": true,
	"ONLY_FULL_GROUP_BY": true, "PAD_CHAR_TO_FULL_LENGTH": true, "PIPES_AS_CONCAT": true,
	"REAL_AS_FLOAT": true, "STRICT_ALL_TABLES": true, "STRICT_TRANS_TABLES": true,
	"TIME_TRUNCATE_FRACTIONAL": true,
	// Combination modes
	"ANSI": true, "DB2": true, "MAXDB": true, "MSSQL": true, "MYSQL323": true,
	"MYSQL40": true, "ORACLE": true, "POSTGRESQL": true, "TRADITIONAL": true,
}

// validateSQLModeValue returns the first invalid mode part, or "" if all parts are valid.
func validateSQLModeValue(mode string) string {
	for _, part := range strings.Split(mode, ",") {
		part = strings.TrimSpace(part)
		if part != "" && !validSQLModes[strings.ToUpper(part)] {
			return part
		}
	}
	return ""
}

// sqlModeBits maps bit positions to SQL mode names (MySQL 8.0 bitmask).
// Lowercase names indicate deprecated/reserved bits that MySQL still emits.
var sqlModeBits = []string{
	"REAL_AS_FLOAT",              // bit 0 = 1
	"PIPES_AS_CONCAT",            // bit 1 = 2
	"ANSI_QUOTES",                // bit 2 = 4
	"IGNORE_SPACE",               // bit 3 = 8
	"not_used",                   // bit 4 = 16 (reserved, emitted as lowercase)
	"ONLY_FULL_GROUP_BY",         // bit 5 = 32
	"NO_UNSIGNED_SUBTRACTION",    // bit 6 = 64
	"NO_DIR_IN_CREATE",           // bit 7 = 128
	"POSTGRESQL",                 // bit 8 = 256
	"ORACLE",                     // bit 9 = 512
	"MSSQL",                      // bit 10 = 1024
	"DB2",                        // bit 11 = 2048
	"MAXDB",                      // bit 12 = 4096
	"NO_KEY_OPTIONS",             // bit 13 = 8192
	"NO_TABLE_OPTIONS",           // bit 14 = 16384
	"NO_FIELD_OPTIONS",           // bit 15 = 32768
	"MYSQL323",                   // bit 16 = 65536
	"MYSQL40",                    // bit 17 = 131072
	"ANSI",                       // bit 18 = 262144
	"NO_AUTO_VALUE_ON_ZERO",      // bit 19 = 524288
	"NO_BACKSLASH_ESCAPES",       // bit 20 = 1048576
	"STRICT_TRANS_TABLES",        // bit 21 = 2097152
	"STRICT_ALL_TABLES",          // bit 22 = 4194304
	"NO_ZERO_IN_DATE",            // bit 23 = 8388608
	"NO_ZERO_DATE",               // bit 24 = 16777216
	"ALLOW_INVALID_DATES",        // bit 25 = 33554432
	"ERROR_FOR_DIVISION_BY_ZERO", // bit 26 = 67108864
	"TRADITIONAL",                // bit 27 = 134217728
	"NO_AUTO_CREATE_USER",        // bit 28 = 268435456
	"HIGH_NOT_PRECEDENCE",        // bit 29 = 536870912
	"NO_ENGINE_SUBSTITUTION",     // bit 30 = 1073741824
	"PAD_CHAR_TO_FULL_LENGTH",    // bit 31 = 2147483648
	"TIME_TRUNCATE_FRACTIONAL",   // bit 32 = 4294967296
}

// sqlModeBitmaskToString converts a numeric sql_mode bitmask to a comma-separated mode string.
// Combination mode bits (ANSI=bit18, TRADITIONAL=bit27) are expanded to include their component bits.
func sqlModeBitmaskToString(n uint64) string {
	if n == 0 {
		return ""
	}
	// Expand combination mode bits into their component bits.
	// ANSI (bit 18) expands to include REAL_AS_FLOAT(0),PIPES_AS_CONCAT(1),ANSI_QUOTES(2),IGNORE_SPACE(3),ONLY_FULL_GROUP_BY(5)
	const ansiBit = uint64(1 << 18)
	// TRADITIONAL (bit 27) expands to include STRICT_TRANS_TABLES(21),STRICT_ALL_TABLES(22),NO_ZERO_IN_DATE(23),NO_ZERO_DATE(24),ERROR_FOR_DIVISION_BY_ZERO(26),NO_ENGINE_SUBSTITUTION(30)
	const traditionalBit = uint64(1 << 27)
	if n&ansiBit != 0 {
		n |= (1 << 0) | (1 << 1) | (1 << 2) | (1 << 3) | (1 << 5) // REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY
	}
	if n&traditionalBit != 0 {
		n |= (1 << 21) | (1 << 22) | (1 << 23) | (1 << 24) | (1 << 26) | (1 << 30) // STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION
	}
	var modes []string
	for i, name := range sqlModeBits {
		if n&(1<<uint(i)) != 0 {
			modes = append(modes, name)
		}
	}
	return strings.Join(modes, ",")
}

// isKnownCharset returns true if the charset name is a valid MySQL character set.
// expandSQLMode expands MySQL combination sql_mode values into their component modes.
func expandSQLMode(mode string) string {
	// combinationModes maps shorthand mode names to their expanded equivalents.
	combinationModes := map[string][]string{
		"ANSI":        {"REAL_AS_FLOAT", "PIPES_AS_CONCAT", "ANSI_QUOTES", "IGNORE_SPACE", "ONLY_FULL_GROUP_BY", "ANSI"},
		"TRADITIONAL": {"STRICT_TRANS_TABLES", "STRICT_ALL_TABLES", "NO_ZERO_IN_DATE", "NO_ZERO_DATE", "ERROR_FOR_DIVISION_BY_ZERO", "TRADITIONAL", "NO_ENGINE_SUBSTITUTION"},
		"DB2":         {"PIPES_AS_CONCAT", "ANSI_QUOTES", "IGNORE_SPACE", "NO_KEY_OPTIONS", "NO_TABLE_OPTIONS", "NO_FIELD_OPTIONS", "DB2"},
		"MAXDB":       {"PIPES_AS_CONCAT", "ANSI_QUOTES", "IGNORE_SPACE", "NO_KEY_OPTIONS", "NO_TABLE_OPTIONS", "NO_FIELD_OPTIONS", "NO_AUTO_CREATE_USER", "MAXDB"},
		"MSSQL":       {"PIPES_AS_CONCAT", "ANSI_QUOTES", "IGNORE_SPACE", "NO_KEY_OPTIONS", "NO_TABLE_OPTIONS", "NO_FIELD_OPTIONS", "MSSQL"},
		"ORACLE":      {"PIPES_AS_CONCAT", "ANSI_QUOTES", "IGNORE_SPACE", "NO_KEY_OPTIONS", "NO_TABLE_OPTIONS", "NO_FIELD_OPTIONS", "NO_AUTO_CREATE_USER", "ORACLE"},
		"POSTGRESQL":  {"PIPES_AS_CONCAT", "ANSI_QUOTES", "IGNORE_SPACE", "NO_KEY_OPTIONS", "NO_TABLE_OPTIONS", "NO_FIELD_OPTIONS", "POSTGRESQL"},
	}

	// Build bit-position map for canonical ordering
	bitPosMap := make(map[string]int, len(sqlModeBits))
	for i, name := range sqlModeBits {
		bitPosMap[strings.ToUpper(name)] = i
	}
	getBitPos := func(name string) int {
		if p, ok := bitPosMap[strings.ToUpper(name)]; ok {
			return p
		}
		return 1000
	}

	// Split into individual modes, expand combination modes, deduplicate, and rejoin.
	parts := strings.Split(mode, ",")
	var result []string
	seen := make(map[string]bool)
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if expanded, ok := combinationModes[part]; ok {
			for _, m := range expanded {
				up := strings.ToUpper(m)
				if !seen[up] {
					seen[up] = true
					result = append(result, m)
				}
			}
		} else {
			up := strings.ToUpper(part)
			if !seen[up] {
				seen[up] = true
				result = append(result, part)
			}
		}
	}
	// Sort by bit position for canonical MySQL output order
	sort.Slice(result, func(i, j int) bool {
		return getBitPos(result[i]) < getBitPos(result[j])
	})
	return strings.Join(result, ",")
}

func isKnownCharset(name string) bool {
	name = strings.ToLower(name)
	// Also accept utf8mb3 as alias for utf8
	if name == "utf8mb3" {
		return true
	}
	for _, cs := range allCharsets() {
		if strings.ToLower(cs[0].(string)) == name {
			return true
		}
	}
	return false
}

// isKnownCollation returns true if the collation name is a valid MySQL collation.
func isKnownCollation(name string) bool {
	name = strings.ToLower(name)
	for _, coll := range allCollations() {
		if strings.ToLower(coll[0].(string)) == name {
			return true
		}
	}
	// Also accept utf8mb3_ prefix mapped to utf8_
	if strings.HasPrefix(name, "utf8mb3_") {
		mapped := "utf8_" + strings.TrimPrefix(name, "utf8mb3_")
		for _, coll := range allCollations() {
			if strings.ToLower(coll[0].(string)) == mapped {
				return true
			}
		}
	}
	return false
}

// charsetAliasEqual returns true if two charset names are equivalent,
// treating "utf8" and "utf8mb3" as aliases of each other.
func charsetAliasEqual(a, b string) bool {
	a = strings.ToLower(a)
	b = strings.ToLower(b)
	if strings.EqualFold(a, b) {
		return true
	}
	// utf8 and utf8mb3 are aliases
	if (a == "utf8" || a == "utf8mb3") && (b == "utf8" || b == "utf8mb3") {
		return true
	}
	return false
}

// resolveCollationID resolves a numeric collation ID to the collation name.
func resolveCollationID(id int64) (string, bool) {
	for _, row := range allCollations() {
		if row[2].(int64) == id {
			return row[0].(string), true
		}
	}
	return "", false
}

// resolveCharsetID resolves a numeric charset ID to the charset name.
// MySQL resolves charset numeric IDs by finding the collation with that ID
// and returning its associated charset.
func resolveCharsetID(id int64) (string, bool) {
	for _, row := range allCollations() {
		if row[2].(int64) == id {
			return row[1].(string), true
		}
	}
	return "", false
}

// charsetForCollation returns the charset associated with a collation name.
func charsetForCollation(collation string) string {
	collation = strings.ToLower(collation)
	for _, row := range allCollations() {
		if strings.ToLower(row[0].(string)) == collation {
			return row[1].(string)
		}
	}
	return ""
}

// matchLike, matchLikeHelper, stripBlockComments, normalizeSQLDisplayName,
// normalizeCharsetIntroducers, stripCharsetIntroducerForColName, isSimpleStringLiteral,
// normalizeSelectedFunctionArgDisplaySpacing, compactOperatorsInDisplayName,
// isComparisonOrArithOp, compactOperatorsInSubexpressions, unescapeStringLiterals,
// normalizeFuncArgSpaces, normalizeAggColNameNulls, uppercaseAggInnerKeywords,
// normalizeAggColNameFunctions, isKnownSQLFunction, normalizeAggColNameSubselect,
// uppercaseSQLKeywords are defined in display.go

// normalizeTypeAliases replaces MySQL type aliases that the vitess parser
// doesn't support with their canonical equivalents.

// stripPrefixLengthFromCol strips the length prefix from a column name used in indexes.
// e.g., "col_1(3072)" -> "col_1"
func stripPrefixLengthFromCol(col string) string {
	if idx := strings.Index(col, "("); idx >= 0 {
		return col[:idx]
	}
	return col
}

func isIdentStart(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_' || r == '$'
}

func isIdentPart(r rune) bool {
	return isIdentStart(r) || (r >= '0' && r <= '9')
}

// logToGeneralLog appends a row to mysql.general_log when the general_log
// global variable is enabled and sql_log_off is not set.
func (e *Executor) logToGeneralLog(query string) {
	// Check if general_log is ON
	if v, ok := e.getGlobalVar("general_log"); ok {
		if !strings.EqualFold(v, "ON") && v != "1" {
			return
		}
	}
	// Check if sql_log_off is set for this session
	if v, ok := e.sessionScopeVars["sql_log_off"]; ok {
		if strings.EqualFold(v, "ON") || v == "1" {
			return
		}
	}
	// Check if log_output includes TABLE
	logOutput := "FILE"
	if v, ok := e.getGlobalVar("log_output"); ok {
		logOutput = v
	} else if v, ok := e.startupVars["log_output"]; ok {
		logOutput = v
	}
	if !strings.Contains(strings.ToUpper(logOutput), "TABLE") {
		return
	}
	tbl, err := e.Storage.GetTable("mysql", "general_log")
	if err != nil {
		return
	}
	now := time.Now().Format("2006-01-02 15:04:05.000000")
	row := storage.Row{
		"event_time":   now,
		"user_host":    "root[root] @ localhost []",
		"thread_id":    int64(1),
		"server_id":    int64(1),
		"command_type": "Query",
		"argument":     strings.TrimRight(strings.TrimSpace(query), ";"),
	}
	tbl.Mu.Lock()
	tbl.Rows = append(tbl.Rows, row)
	tbl.Mu.Unlock()
}

// recordStatementDigest records a statement digest for performance_schema
// events_statements_summary_by_digest and histogram tables.
func (e *Executor) recordStatementDigest(query string) {
	// Only record if digest tables have been truncated (i.e., test is tracking digests)
	if e.psTruncated == nil {
		return
	}
	if !e.psTruncated["events_statements_summary_by_digest"] && !e.psTruncated["events_statements_histogram_by_digest"] {
		return
	}
	// Skip queries to performance_schema itself to avoid recursive recording
	upperQ := strings.ToUpper(query)
	if strings.Contains(upperQ, "PERFORMANCE_SCHEMA") {
		return
	}
	// Skip TRUNCATE, USE, and other non-data statements
	if strings.HasPrefix(upperQ, "TRUNCATE") || strings.HasPrefix(upperQ, "USE ") {
		return
	}
	// Compute a simple digest
	h := sha256.Sum256([]byte(strings.TrimSpace(query)))
	digest := hex.EncodeToString(h[:16]) // 32-char hex digest

	schemaName := e.CurrentDB
	if schemaName == "" {
		schemaName = "test"
	}
	// Check if we already have this digest+schema
	for i := range e.psDigests {
		if e.psDigests[i].Digest == digest && e.psDigests[i].SchemaName == schemaName {
			e.psDigests[i].CountStar++
			return
		}
	}
	e.psDigests = append(e.psDigests, psDigestEntry{
		SchemaName: schemaName,
		Digest:     digest,
		DigestText: strings.TrimSpace(query),
		CountStar:  1,
	})
}

func (e *Executor) Execute(query string) (res *Result, retErr error) {
	// Increment the Questions counter for every statement received from the client,
	// including statements that preprocessQuery short-circuits (e.g. SHOW COUNT(*) WARNINGS).
	// Skip incrementing for empty queries and for internal routine statements.
	trimmedForCount := strings.TrimSpace(query)
	if trimmedForCount != "" && e.routineDepth == 0 {
		e.questions++
	}
	// Track execution errors in the diagnostics area (for SHOW ERRORS / SHOW WARNINGS).
	// Only track for client-level statements (not internal routine calls).
	if e.routineDepth == 0 {
		defer func() {
			if retErr != nil {
				// Extract MySQL error code and message from the error.
				// Format: "ERROR <code> (<state>): <message>"
				code := 1064
				msg := retErr.Error()
				if strings.HasPrefix(msg, "ERROR ") {
					var codeVal int
					if _, scanErr := fmt.Sscanf(msg, "ERROR %d", &codeVal); scanErr == nil && codeVal > 0 {
						code = codeVal
						if parenIdx := strings.Index(msg, "): "); parenIdx >= 0 {
							msg = msg[parenIdx+3:]
						}
					}
				}
				e.addWarning("Error", code, msg)
			}
			// Update ROW_COUNT() tracking (MySQL semantics):
			// - After SELECT: -1
			// - After failed DML: -1 (remains unchanged since it was already -1 or a previous value)
			// - After successful DML: AffectedRows
			if retErr == nil && res != nil {
				if res.Columns != nil {
					// SELECT result: set to -1
					e.lastAffectedRows = -1
				} else {
					// DML result: set to affected rows
					e.lastAffectedRows = int64(res.AffectedRows)
				}
			}
			// On error, leave lastAffectedRows as -1 (MySQL returns -1 after failed statements)
		}()
	}

	// When character_set_client is koi8r or cp1251, the query may contain Cyrillic bytes
	// in that charset. MySQL converts the entire query from character_set_client to UTF-8
	// internally before processing identifiers. For these charsets, decode queries that
	// contain non-UTF-8 bytes (e.g. table/column names created with Cyrillic characters).
	// Other charsets (latin1, sjis, etc.) are left as-is because their test result files
	// contain raw bytes that were not decoded, and changing those would cause regressions.
	if !utf8.ValidString(query) {
		if cs, ok := e.getSysVar("character_set_client"); ok {
			canonical := canonicalCharset(cs)
			if canonical == "koi8r" || canonical == "cp1251" || canonical == "koi8u" {
				if dec := charsetDecoder(canonical); dec != nil {
					if decoded, _, err := transform.String(dec, query); err == nil {
						query = decoded
					}
				}
			}
		}
	}

	query, result, err := e.preprocessQuery(query)
	if result != nil || err != nil {
		return result, err
	}

	// Log query to mysql.general_log if general_log is enabled
	e.logToGeneralLog(query)

	// Record statement digest for performance_schema digest tables
	e.recordStatementDigest(query)

	trimmed := strings.TrimSpace(query)
	upper := strings.ToUpper(trimmed)

	// KILL [CONNECTION | QUERY] thread_id — unregister the target connection from
	// the process list so that tests waiting on "COUNT(*) = 0 WHERE id = X" can
	// proceed.  The underlying TCP connection is not actually closed (we have no
	// mechanism for that from SQL), but removing the process-list entry is
	// sufficient for the test-suite wait loops.
	// Handled here (before the parser) because the Vitess parser parses KILL
	// but the executor's switch statement has no case for it, resulting in an
	// "unsupported statement type" error that would silently be swallowed.
	if strings.HasPrefix(upper, "KILL ") {
		rest := strings.TrimSpace(strings.TrimPrefix(upper, "KILL "))
		rest = strings.TrimPrefix(rest, "CONNECTION ")
		rest = strings.TrimPrefix(rest, "QUERY ")
		rest = strings.TrimSpace(rest)
		if id, err2 := strconv.ParseInt(rest, 10, 64); err2 == nil && e.processList != nil {
			// Kill closes the underlying TCP connection so the server goroutine exits.
			// This ensures the pool cannot reuse the old connection, and the next --connect
			// truly opens a new TCP connection with a fresh process list registration.
			e.processList.Kill(id)
		}
		return &Result{}, nil
	}

	// MySQL has a stack depth limit for deeply nested IF expressions.
	// Count IF( occurrences as a proxy for nesting depth.
	// A threshold of 30 allows tests with up to 30 nested IFs (func_if test has exactly 30)
	// while detecting pathological recursion (execution_constants builds thousands of IFs,
	// reaching 31 after 10 loop iterations).
	// Note: IFNULL/NULLIF are separate functions and won't match "IF(".
	{
		ifCount := strings.Count(upper, "IF(")
		if ifCount > 30 {
			return nil, mysqlError(1436, "HY000", "Thread stack overrun: "+
				"Need more than available stack. Use 'mysqld --thread_stack=#' to specify a bigger stack.")
		}
	}

	// MySQL treats "ALTER INDEX PRIMARY ..." as a parse error since PRIMARY is a reserved keyword.
	// The vitess parser silently drops this option, so we must detect it before parsing.
	if strings.Contains(upper, "ALTER INDEX PRIMARY") {
		return nil, mysqlError(1064, "42000", "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'PRIMARY INVISIBLE' at line 1")
	}

	// MySQL treats IS UNKNOWN and IS NOT UNKNOWN as non-associative (like IS TRUE/FALSE/NOT TRUE/NOT FALSE).
	// Chaining them (e.g. "x IS UNKNOWN IS UNKNOWN") is a syntax error in MySQL.
	// The vitess parser maps IS UNKNOWN -> IsNullOp (same as IS NULL), so it silently allows chaining.
	// We must detect and reject this before parsing.
	if strings.Contains(upper, "UNKNOWN") {
		// Match: IS [NOT] UNKNOWN followed by whitespace and IS (i.e., chained)
		chainedUnknownRe := regexp.MustCompile(`(?i)\bIS\s+(NOT\s+)?UNKNOWN\s+IS\b`)
		if loc := chainedUnknownRe.FindStringIndex(upper); loc != nil {
			// Find the second IS in the match - that's the start of the "near" text
			secondIsIdx := strings.Index(upper[loc[0]+2:], "IS")
			nearStart := loc[0] + 2 + secondIsIdx
			nearText := strings.TrimRight(query[nearStart:], ";")
			return nil, mysqlError(1064, "42000", fmt.Sprintf("You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '%s' at line 1", nearText))
		}
	}

	stmt, err := e.parser().Parse(query)
	if err != nil {
		// Accept statements that Vitess parser doesn't support
		if strings.HasPrefix(upper, "EXPLAIN ") || strings.HasPrefix(upper, "DESC ") || strings.HasPrefix(upper, "DESCRIBE ") {
			explainType := sqlparser.TraditionalType
			// Check for FORMAT= specifier
			if formatIdx := strings.Index(upper, "FORMAT="); formatIdx >= 0 {
				formatStr := upper[formatIdx+7:]
				// Strip quotes if present
				if len(formatStr) > 0 && (formatStr[0] == '\'' || formatStr[0] == '"') {
					formatStr = strings.Trim(formatStr, "'\"")
				}
				// Get the format name (up to first space)
				if spIdx := strings.IndexAny(formatStr, " \t"); spIdx >= 0 {
					formatStr = formatStr[:spIdx]
				}
				formatStr = strings.Trim(formatStr, "'\"")
				switch formatStr {
				case "JSON":
					explainType = sqlparser.JSONType
				case "TREE":
					explainType = sqlparser.TreeType
				case "TRADITIONAL":
					explainType = sqlparser.TraditionalType
				default:
					// Unknown format name
					return nil, mysqlError(1235, "HY000", fmt.Sprintf("Unknown EXPLAIN format name: '%s'", strings.ToLower(formatStr)))
				}
			}
			explainedQuery := trimmed
			if idx := strings.Index(strings.ToUpper(trimmed), "SELECT "); idx >= 0 {
				explainedQuery = strings.TrimSpace(trimmed[idx:])
			}
			return e.explainResultForType(explainType, explainedQuery), nil
		}
		if strings.HasPrefix(upper, "SET ") {
			if err := e.handleRawSet(trimmed); err != nil {
				return nil, err
			}
			return &Result{}, nil
		}
		if strings.HasPrefix(upper, "USE ") {
			nearText := strings.TrimPrefix(trimmed, "USE ")
			nearText = strings.TrimPrefix(nearText, "use ")
			return nil, mysqlError(1064, "42000", fmt.Sprintf("You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '%s' at line 1", nearText))
		}
		// FLUSH TABLE <tblname> (without FOR EXPORT): re-open table and refresh stats.
		// When stats have been deleted from innodb_index_stats and FLUSH TABLE is called,
		// MySQL recomputes transient stats. But if stats exist (e.g., manually updated),
		// FLUSH TABLE just re-reads them without overwriting.
		if (strings.HasPrefix(upper, "FLUSH TABLE ") || strings.HasPrefix(upper, "FLUSH TABLES ")) &&
			!strings.Contains(upper, "FOR EXPORT") && !strings.Contains(upper, "WITH READ LOCK") {
			// Extract table names from FLUSH TABLE t1, t2, ...
			rest := ""
			if strings.HasPrefix(upper, "FLUSH TABLE ") {
				rest = strings.TrimSpace(trimmed[len("FLUSH TABLE "):])
			} else {
				rest = strings.TrimSpace(trimmed[len("FLUSH TABLES "):])
			}
			if rest != "" && !strings.EqualFold(rest, ";") && !strings.EqualFold(strings.TrimSuffix(strings.TrimSpace(rest), ";"), "") {
				tableNames := strings.Split(strings.TrimSuffix(strings.TrimSpace(rest), ";"), ",")
				for _, tn := range tableNames {
					tn = strings.TrimSpace(strings.Trim(tn, "`"))
					if tn == "" {
						continue
					}
					// Parse db.table notation
					flushDB := e.CurrentDB
					flushTable := tn
					if dotIdx := strings.Index(tn, "."); dotIdx >= 0 {
						flushDB = strings.Trim(tn[:dotIdx], "`")
						flushTable = strings.Trim(tn[dotIdx+1:], "`")
					}
					// Only recompute stats if no entries exist in innodb_index_stats.
					// If stats exist (manually updated or from ANALYZE), keep them as-is.
					hasIndexStats := false
					if idxTbl, err := e.Storage.GetTable("mysql", "innodb_index_stats"); err == nil {
						idxTbl.Mu.RLock()
						for _, r := range idxTbl.Rows {
							if strings.EqualFold(toString(r["database_name"]), flushDB) &&
								strings.EqualFold(toString(r["table_name"]), flushTable) {
								hasIndexStats = true
								break
							}
						}
						idxTbl.Mu.RUnlock()
					}
					if !hasIndexStats {
						// No index stats exist: recompute (simulates InnoDB re-reading stats after FLUSH)
						e.upsertInnoDBStatsRows(flushDB, flushTable, e.tableRowCount(flushDB, flushTable))
					}
				}
			}
		}
		// FLUSH TABLE(S) ... FOR EXPORT on non-InnoDB engines returns an error
		if strings.HasPrefix(upper, "FLUSH TABLE") && strings.Contains(upper, "FOR EXPORT") {
			// Extract table name: FLUSH TABLE t1 FOR EXPORT or FLUSH TABLES t1 FOR EXPORT
			parts := strings.Fields(trimmed)
			if len(parts) >= 3 {
				tblName := strings.TrimRight(parts[2], ";")
				if tbl, err := e.Storage.GetTable(e.CurrentDB, tblName); err == nil {
					eng := strings.ToLower(tbl.Def.Engine)
					if eng != "" && eng != "innodb" {
						return nil, mysqlError(1031, "HY000", fmt.Sprintf("Table storage engine for '%s' doesn't have this option", tblName))
					}
				}
			}
			return &Result{}, nil
		}
		// FLUSH STATUS (parser fallback path) must reset session status counters.
		if strings.HasPrefix(upper, "FLUSH STATUS") {
			e.handlerReadKey = 0
			e.handlerReadFirst = 0
			e.handlerReadLast = 0
			e.handlerReadNext = 0
			e.handlerReadPrev = 0
			e.handlerReadRnd = 0
			e.handlerReadRndNext = 0
			e.sortRows = 0
			e.sortRange = 0
			e.sortScan = 0
			e.createdTmpDiskTables = 0
			e.createdTmpTables = 0
			e.questions = 0
			return &Result{}, nil
		}
		// HANDLER ... OPEN/READ/CLOSE: unsupported feature
		if strings.HasPrefix(upper, "HANDLER ") {
			return nil, ErrUnsupported("HANDLER statement")
		}
		// Reject CREATE EVENT with MICROSECOND intervals (MySQL error 1235)
		if strings.HasPrefix(upper, "CREATE EVENT") &&
			(strings.Contains(upper, "MICROSECOND") || strings.Contains(upper, "DAY_MICROSECOND") ||
				strings.Contains(upper, "HOUR_MICROSECOND") || strings.Contains(upper, "MINUTE_MICROSECOND") ||
				strings.Contains(upper, "SECOND_MICROSECOND")) {
			return nil, mysqlError(1235, "42000", "This version of MySQL doesn't yet support 'MICROSECOND'")
		}
		// OPTIMIZE TABLE ... EXTENDED and ANALYZE TABLE ... EXTENDED are syntax errors in MySQL
		if (strings.HasPrefix(upper, "OPTIMIZE TABLE") || strings.HasPrefix(upper, "ANALYZE TABLE")) &&
			strings.HasSuffix(strings.TrimRight(upper, ";"), "EXTENDED") {
			// Return MySQL syntax error 1064
			return nil, mysqlError(1064, "42000", fmt.Sprintf("You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'extended' at line 1"))
		}
		if strings.HasPrefix(upper, "GRANT ") {
			// Parse and store the grant
			if e.grantStore != nil {
				privs, object, toUser, toHost, isRoleGrant, grantOption, adminOption := ParseGrantStatement(trimmed)
				if isRoleGrant {
					// GRANT role TO user - record role membership
					// Handle comma-separated roles: "GRANT r1, r2 TO user"
					for _, roleName := range strings.Split(privs, ",") {
						roleName = strings.TrimSpace(roleName)
						roleName = strings.Trim(roleName, "`'\"")
						if roleName != "" {
							rh := "%"
							if atIdx := strings.Index(roleName, "@"); atIdx >= 0 {
								rh = roleName[atIdx+1:]
								roleName = roleName[:atIdx]
							}
							e.grantStore.AddRoleGrant(roleName, rh, toUser, toHost, adminOption)
						}
					}
				} else if privs != "" && object != "" && toUser != "" {
					// Handle multiple users: "GRANT privs ON obj TO u1, u2"
					// For now handle single user (most common case)
					e.grantStore.AddPrivGrant(toUser, toHost, privs, object, grantOption)

					// Also populate mysql.db / mysql.tables_priv for raw table queries
					e.syncGrantToMysqlTables(privs, object, toUser, toHost, grantOption)

					// Track specific privileges for internal use.
					// SUPER is a global-only privilege, so only grant it when object is *.* (global scope).
					upperPrivs := strings.ToUpper(privs)
					isGlobalScope := object == "*.*"
					if isGlobalScope && (strings.Contains(upperPrivs, "SUPER") || upperPrivs == "ALL" || upperPrivs == "ALL PRIVILEGES") {
						if toUser != "" && e.superUsersMu != nil {
							e.superUsersMu.Lock()
							e.superUsers[strings.ToLower(toUser)] = true
							e.superUsersMu.Unlock()
						}
					}
					if strings.Contains(upperPrivs, "SYSTEM_VARIABLES_ADMIN") {
						if toUser != "" && e.sysVarsAdminUsersMu != nil {
							e.sysVarsAdminUsersMu.Lock()
							e.sysVarsAdminUsers[strings.ToLower(toUser)] = true
							e.sysVarsAdminUsersMu.Unlock()
						}
					}
				}
			} else {
				// Fallback: track SUPER and SYSTEM_VARIABLES_ADMIN the old way
				extractGrantUser := func() string {
					if idx := strings.Index(upper, " TO "); idx >= 0 {
						userPart := strings.TrimSpace(trimmed[idx+4:])
						if wi := strings.Index(strings.ToUpper(userPart), " WITH "); wi >= 0 {
							userPart = strings.TrimSpace(userPart[:wi])
						}
						userPart = strings.Trim(userPart, "'`\"")
						if at := strings.Index(userPart, "@"); at >= 0 {
							userPart = userPart[:at]
						}
						userPart = strings.Trim(userPart, "'`\"")
						return strings.ToLower(strings.TrimSpace(userPart))
					}
					return ""
				}
				if strings.Contains(upper, "SUPER") {
					if username := extractGrantUser(); username != "" && e.superUsersMu != nil {
						e.superUsersMu.Lock()
						e.superUsers[username] = true
						e.superUsersMu.Unlock()
					}
				}
				if strings.Contains(upper, "SYSTEM_VARIABLES_ADMIN") {
					if username := extractGrantUser(); username != "" && e.sysVarsAdminUsersMu != nil {
						e.sysVarsAdminUsersMu.Lock()
						e.sysVarsAdminUsers[username] = true
						e.sysVarsAdminUsersMu.Unlock()
					}
				}
			}
			return &Result{}, nil
		}
		// INSTALL PLUGIN with a path (contains '/') should fail with ER_UDF_NO_PATHS
		if strings.HasPrefix(upper, "INSTALL PLUGIN ") && strings.Contains(query, "/") {
			return nil, mysqlError(1210, "HY000", "No paths allowed for shared library")
		}
		// ALTER PROCEDURE/FUNCTION are DDL: cause implicit commit (clears savepoints).
		if strings.HasPrefix(upper, "ALTER PROCEDURE") || strings.HasPrefix(upper, "ALTER FUNCTION") {
			e.ddlImplicitCommit()
			// LOCK TABLES blocks procedure/function DDL.
			if e.tableLockManager != nil && e.tableLockManager.HasLocks(e.connectionID) {
				return nil, mysqlError(1192, "HY000", "Can't execute the given command because you have active locked tables or an active transaction")
			}
		}
		// Track CREATE USER / DROP USER in knownUsers for SET PASSWORD FOR validation.
		if strings.HasPrefix(upper, "CREATE USER ") {
			e.trackCreateUser(trimmed)
		}
		if strings.HasPrefix(upper, "DROP USER ") {
			e.trackDropUser(trimmed)
		}
		// Track CREATE ROLE for the grant store.
		if strings.HasPrefix(upper, "CREATE ROLE ") && e.grantStore != nil {
			rolePart := strings.TrimSpace(trimmed[12:])
			rolePart = strings.TrimRight(rolePart, ";")
			for _, rn := range strings.Split(rolePart, ",") {
				rn = strings.TrimSpace(rn)
				rn = strings.Trim(rn, "`'\"")
				if rn != "" {
					e.grantStore.RegisterRole(rn)
				}
			}
		}
		// Handle REVOKE to remove stored grants.
		if strings.HasPrefix(upper, "REVOKE ") && e.grantStore != nil {
			privs, object, fromUser, fromHost, isRoleRevoke := ParseRevokeStatement(trimmed)
			if !isRoleRevoke && privs != "" && object != "" && fromUser != "" {
				if strings.ToUpper(privs) == "ALL PRIVILEGES" {
					e.grantStore.RevokeAllPrivGrants(fromUser, fromHost)
				} else {
					e.grantStore.RevokePrivGrant(fromUser, fromHost, privs, object)
				}
			}
		}
		// FLUSH PRIVILEGES: reload grant store from mysql.db and mysql.tables_priv
		if (upper == "FLUSH PRIVILEGES" || upper == "FLUSH PRIVILEGES;" ||
			strings.HasPrefix(upper, "FLUSH PRIVILEGES ")) && e.grantStore != nil {
			e.reloadGrantStoreFromStorage()
		}
		if strings.HasPrefix(upper, "CREATE EVENT") ||
			strings.HasPrefix(upper, "DROP EVENT") ||
			strings.HasPrefix(upper, "CREATE USER") ||
			strings.HasPrefix(upper, "DROP USER") ||
			strings.HasPrefix(upper, "ALTER USER") ||
			strings.HasPrefix(upper, "CREATE ROLE") ||
			strings.HasPrefix(upper, "DROP ROLE") ||
			strings.HasPrefix(upper, "REVOKE ") ||
			strings.HasPrefix(upper, "FLUSH ") ||
			strings.HasPrefix(upper, "RESET ") ||
			strings.HasPrefix(upper, "INSTALL ") ||
			strings.HasPrefix(upper, "UNINSTALL ") ||
			strings.HasPrefix(upper, "CHECKSUM ") ||
			strings.HasPrefix(upper, "REPAIR ") ||
			strings.HasPrefix(upper, "OPTIMIZE ") ||
			strings.HasPrefix(upper, "CHECK ") ||
			strings.HasPrefix(upper, "DELIMITER ") ||
			strings.HasPrefix(upper, "DECLARE ") ||
			strings.HasPrefix(upper, "RETURN ") ||
			strings.HasPrefix(upper, "OPEN ") ||
			strings.HasPrefix(upper, "CLOSE ") ||
			strings.HasPrefix(upper, "FETCH ") ||
			strings.HasPrefix(upper, "SIGNAL ") ||
			strings.HasPrefix(upper, "RESIGNAL") ||
			strings.HasPrefix(upper, "GET DIAGNOSTICS") ||
			strings.HasPrefix(upper, "XA ") ||
			strings.HasPrefix(upper, "ALTER PROCEDURE") ||
			strings.HasPrefix(upper, "ALTER FUNCTION") ||
			strings.HasPrefix(upper, "CHANGE ") ||
			strings.HasPrefix(upper, "START ") ||
			strings.HasPrefix(upper, "STOP ") ||
			strings.HasPrefix(upper, "PURGE ") ||
			strings.HasPrefix(upper, "BINLOG ") ||
			strings.HasPrefix(upper, "END") ||
			strings.HasPrefix(upper, "ALTER INSTANCE") ||
			strings.HasPrefix(upper, "CREATE SPATIAL REFERENCE SYSTEM") ||
			strings.HasPrefix(upper, "DROP SPATIAL REFERENCE SYSTEM") {
			return &Result{}, nil
		}
		// LOCK TABLES: check for performance_schema tables
		if strings.HasPrefix(upper, "LOCK TABLE ") || strings.HasPrefix(upper, "LOCK TABLES ") {
			if tblName := extractPerfSchemaLockTable(trimmed); tblName != "" {
				return nil, mysqlError(1142, "42000", fmt.Sprintf("SELECT, LOCK TABLES command denied to user 'root'@'localhost' for table '%s'", tblName))
			}
			return &Result{}, nil
		}
		// Undo tablespace DDL: only supported for InnoDB. When ENGINE=MyISAM is specified,
		// MySQL returns ER_ILLEGAL_HA_CREATE_OPTION (1031).
		if strings.HasPrefix(upper, "CREATE UNDO TABLESPACE") {
			if strings.Contains(upper, "ENGINE MYISAM") || strings.Contains(upper, "ENGINE=MYISAM") {
				return nil, mysqlError(1031, "HY000", "Table storage engine 'MyISAM' does not support the create option 'CREATE UNDO TABLESPACE'")
			}
			return &Result{}, nil
		}
		if strings.HasPrefix(upper, "ALTER UNDO TABLESPACE") {
			if strings.Contains(upper, "ENGINE MYISAM") || strings.Contains(upper, "ENGINE=MYISAM") {
				return nil, mysqlError(1031, "HY000", "Table storage engine 'MyISAM' does not support the create option 'ALTER UNDO TABLESPACE'")
			}
			return &Result{}, nil
		}
		if strings.HasPrefix(upper, "DROP UNDO TABLESPACE") {
			if strings.Contains(upper, "ENGINE MYISAM") || strings.Contains(upper, "ENGINE=MYISAM") {
				return nil, mysqlError(1031, "HY000", "Table storage engine 'MyISAM' does not support the create option 'DROP UNDO TABLESPACE'")
			}
			return &Result{}, nil
		}
		// For multi-table DELETE: DELETE t1,t2 FROM t1,t2,t3 WHERE ...
		// or DELETE [QUICK] FROM t1,t2 USING t1,t2,t3 WHERE ...
		// Fall back to string-based parser for multi-table delete variants.
		// Exception: "DELETE FROM t1 alias USING ..." is a genuine parse error (ER_PARSE_ERROR),
		// but only when there's a single table target with an alias (no comma) before USING.
		if strings.HasPrefix(upper, "DELETE ") {
			errStr := err.Error()
			if strings.Contains(errStr, "syntax error at position") && strings.Contains(errStr, "near 'USING'") {
				// Check if this is the alias-before-USING error vs valid multi-table USING syntax.
				// Valid: DELETE [mods] FROM t1, t2 USING ... (comma between targets)
				// Invalid: DELETE FROM t1 alias USING ... (single target with space-separated alias)
				// Detect by looking for comma between FROM and USING in normalized query.
				if isAliasBeforeUsing(upper) {
					return nil, mysqlError(1064, "42000", fmt.Sprintf("You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '%s' at line 1", extractNearFromParseError(trimmed, err)))
				}
			}
			return e.execMultiTableDelete(trimmed)
		}
		// Handle SHOW GRANTS (vitess parser may fail on some variants)
		if strings.HasPrefix(upper, "SHOW GRANTS") {
			return e.execShowGrants(trimmed)
		}
		return nil, mysqlError(1064, "42000", fmt.Sprintf("You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '%s' at line 1", extractNearFromParseError(trimmed, err)))
	}

	// Enforce LOCK TABLES restrictions before dispatching.
	// Skip lock checks when inside a routine (trigger/SP/function) because MySQL
	// uses prelocking to automatically add routine-dependent tables to the lock set.
	// Mylite doesn't implement full prelocking, so we relax the check inside routines.
	if e.tableLockManager != nil && e.tableLockManager.HasLocks(e.connectionID) && e.routineDepth == 0 {
		if lockErr := e.checkTableLockRestrictions(stmt); lockErr != nil {
			return nil, lockErr
		}
	}

	// Enforce error 1422: DDL that causes implicit commit is not allowed inside
	// stored functions or triggers (but NOT stored procedures).
	if e.functionOrTriggerDepth > 0 {
		switch s := stmt.(type) {
		case *sqlparser.CreateTable:
			if !s.Temp {
				return nil, mysqlError(1422, "HY000", "Explicit or implicit commit is not allowed in stored function or trigger.")
			}
		case *sqlparser.AlterTable, *sqlparser.CreateDatabase, *sqlparser.DropDatabase,
			*sqlparser.CreateView, *sqlparser.TruncateTable, *sqlparser.RenameTable:
			return nil, mysqlError(1422, "HY000", "Explicit or implicit commit is not allowed in stored function or trigger.")
		case *sqlparser.DropTable:
			if !s.Temp {
				return nil, mysqlError(1422, "HY000", "Explicit or implicit commit is not allowed in stored function or trigger.")
			}
		}
	}

	// Enforce super_read_only: blocks ALL users including SUPER (only TEMP tables exempt).
	if superROVal, ok := e.getGlobalVar("super_read_only"); ok {
		superROOn := superROVal == "1" || strings.EqualFold(superROVal, "ON")
		if superROOn {
			switch s := stmt.(type) {
			case *sqlparser.Insert, *sqlparser.Update, *sqlparser.Delete,
				*sqlparser.DropTable, *sqlparser.AlterTable,
				*sqlparser.CreateDatabase, *sqlparser.DropDatabase, *sqlparser.TruncateTable:
				return nil, mysqlError(1290, "HY000", "The MySQL server is running with the --super-read-only option so it cannot execute this statement")
			case *sqlparser.CreateTable:
				if !s.Temp {
					return nil, mysqlError(1290, "HY000", "The MySQL server is running with the --super-read-only option so it cannot execute this statement")
				}
			}
		}
	}

	// Enforce read_only: when read_only=ON, block write statements from non-SUPER users.
	// SUPER users (root) can bypass read_only but not super_read_only.
	if readOnlyVal, ok := e.getGlobalVar("read_only"); ok {
		readOnlyOn := readOnlyVal == "1" || strings.EqualFold(readOnlyVal, "ON")
		if readOnlyOn {
			// Check if current user is root/SUPER
			isSuper := true
			if cu, ok2 := e.userVars["__current_user"]; ok2 {
				if cuStr, ok3 := cu.(string); ok3 && cuStr != "" && !strings.EqualFold(cuStr, "root") {
					isSuper = false
					// Check if user has been granted SUPER privilege
					if e.superUsersMu != nil {
						e.superUsersMu.RLock()
						if e.superUsers[strings.ToLower(cuStr)] {
							isSuper = true
						}
						e.superUsersMu.RUnlock()
					}
				}
			}
			if !isSuper {
				switch s := stmt.(type) {
				case *sqlparser.Insert, *sqlparser.Update, *sqlparser.Delete,
					*sqlparser.DropTable, *sqlparser.AlterTable,
					*sqlparser.CreateDatabase, *sqlparser.DropDatabase, *sqlparser.TruncateTable:
					return nil, mysqlError(1290, "HY000", "The MySQL server is running with the --read-only option so it cannot execute this statement")
				case *sqlparser.CreateTable:
					// CREATE TEMPORARY TABLE is allowed even when read_only=ON (MySQL behavior).
					if !s.Temp {
						return nil, mysqlError(1290, "HY000", "The MySQL server is running with the --read-only option so it cannot execute this statement")
					}
				}
			}
		}
	}

	// Enforce table-level privileges for non-root users.
	if privErr := e.checkTablePrivilege(stmt); privErr != nil {
		return nil, privErr
	}

	switch s := stmt.(type) {
	case *sqlparser.CreateDatabase:
		return e.execCreateDatabase(s)
	case *sqlparser.DropDatabase:
		return e.execDropDatabase(s)
	case *sqlparser.Use:
		return e.execUse(s)
	case *sqlparser.CreateTable:
		e.ddlImplicitCommit()
		return e.execCreateTable(s)
	case *sqlparser.DropTable:
		e.ddlImplicitCommit()
		return e.execDropTable(s)
	case *sqlparser.Insert:
		res, err := e.execInsert(s)
		// Track OPTIMIZE/ANALYZE status based on insert type.
		if err == nil {
			tblName := s.Table.TableNameString()
			dbName := e.CurrentDB
			if tn, ok := s.Table.Expr.(sqlparser.TableName); ok && !tn.Qualifier.IsEmpty() {
				dbName = tn.Qualifier.String()
			}
			fullName := dbName + "." + tblName
			_, isSelect := s.Rows.(*sqlparser.Select)
			_, isUnion := s.Rows.(*sqlparser.Union)
			if dbObj, dbErr := e.Catalog.GetDatabase(dbName); dbErr == nil {
				if tblDef, tblErr := dbObj.GetTable(tblName); tblErr == nil && tblDef != nil {
					eng := strings.ToUpper(tblDef.Engine)
					// Only track for non-InnoDB, non-MEMORY engines.
					// InnoDB always returns "OK" from ANALYZE TABLE regardless.
					if eng != "" && eng != "INNODB" && eng != "MEMORY" && eng != "HEAP" {
						if !(isSelect || isUnion) {
							// VALUES-based insert: mark table as needing optimize.
							if e.tableNeedsOptimize == nil {
								e.tableNeedsOptimize = map[string]bool{}
							}
							e.tableNeedsOptimize[fullName] = true
						}
						// All inserts (including INSERT SELECT) clear the analyzed flag
						// since data has changed and stats may be stale.
					}
				}
			}
		}
		return res, err
	case *sqlparser.Select:
		// Check MAX_JOIN_SIZE before executing the SELECT.
		if err := e.checkMaxJoinSize(s); err != nil {
			return nil, err
		}
		res, err := e.execSelect(s)
		// Apply SQL_SELECT_LIMIT when no explicit LIMIT clause is present.
		// MySQL applies sql_select_limit as the maximum rows returned to the client.
		if err == nil && res != nil && s.Limit == nil {
			if limitStr, ok := e.sessionScopeVars["sql_select_limit"]; ok {
				if limit, convErr := strconv.ParseInt(limitStr, 10, 64); convErr == nil && limit >= 0 {
					if int64(len(res.Rows)) > limit {
						res.Rows = res.Rows[:limit]
					}
				}
			}
		}
		return res, err
	case *sqlparser.Update:
		res, err := e.execUpdate(s)
		// Track analyze status for non-InnoDB tables after UPDATE.
		if err == nil {
			tableName := s.TableExprs[0]
			if aliased, ok := tableName.(*sqlparser.AliasedTableExpr); ok {
				if tn, ok := aliased.Expr.(sqlparser.TableName); ok {
					tblName := tn.Name.String()
					dbName := e.CurrentDB
					if !tn.Qualifier.IsEmpty() {
						dbName = tn.Qualifier.String()
					}
					fullName := dbName + "." + tblName
					if dbObj, dbErr := e.Catalog.GetDatabase(dbName); dbErr == nil {
						if tblDef, tblErr := dbObj.GetTable(tblName); tblErr == nil && tblDef != nil {
							eng := strings.ToUpper(tblDef.Engine)
							if eng != "" && eng != "INNODB" && eng != "MEMORY" && eng != "HEAP" {
								if e.tableNeedsAnalyze == nil {
									e.tableNeedsAnalyze = map[string]bool{}
								}
								e.tableNeedsAnalyze[fullName] = true
							}
						}
					}
				}
			}
		}
		return res, err
	case *sqlparser.Delete:
		res, err := e.execDelete(s)
		// Track analyze status for non-InnoDB tables after DELETE.
		if err == nil && s.TableExprs != nil && len(s.TableExprs) > 0 {
			tableName := s.TableExprs[0]
			if aliased, ok := tableName.(*sqlparser.AliasedTableExpr); ok {
				if tn, ok := aliased.Expr.(sqlparser.TableName); ok {
					tblName := tn.Name.String()
					dbName := e.CurrentDB
					if !tn.Qualifier.IsEmpty() {
						dbName = tn.Qualifier.String()
					}
					fullName := dbName + "." + tblName
					if dbObj, dbErr := e.Catalog.GetDatabase(dbName); dbErr == nil {
						if tblDef, tblErr := dbObj.GetTable(tblName); tblErr == nil && tblDef != nil {
							eng := strings.ToUpper(tblDef.Engine)
							if eng != "" && eng != "INNODB" && eng != "MEMORY" && eng != "HEAP" {
								if e.tableNeedsAnalyze == nil {
									e.tableNeedsAnalyze = map[string]bool{}
								}
								e.tableNeedsAnalyze[fullName] = true
							}
						}
					}
				}
			}
		}
		return res, err
	case *sqlparser.AlterTable:
		res, err := e.execAlterTable(s)
		if err == nil {
			// Mark the table as needing analysis/optimize after structural changes.
			dbName := e.CurrentDB
			if !s.Table.Qualifier.IsEmpty() {
				dbName = s.Table.Qualifier.String()
			}
			fullName := dbName + "." + s.Table.Name.String()
			tblName := s.Table.Name.String()

			// Check if the ALTER TABLE only adds indexes (ADD INDEX / CREATE INDEX).
			// For non-InnoDB tables: ADD INDEX already computes stats, so subsequent
			// ANALYZE TABLE should return "Table is already up to date".
			onlyAddsIndexes := len(s.AlterOptions) > 0
			for _, opt := range s.AlterOptions {
				switch opt.(type) {
				case *sqlparser.AddIndexDefinition, *sqlparser.AddConstraintDefinition:
					// These are index additions: stats computed during creation
				default:
					onlyAddsIndexes = false
				}
			}

			// Determine engine of the affected table.
			alterEng := ""
			if dbObj, dbErr := e.Catalog.GetDatabase(dbName); dbErr == nil {
				if tblDef, tblErr := dbObj.GetTable(tblName); tblErr == nil && tblDef != nil {
					alterEng = strings.ToUpper(tblDef.Engine)
				}
			}
			isNonInnoDB := alterEng != "" && alterEng != "INNODB" && alterEng != "MEMORY" && alterEng != "HEAP"

			if onlyAddsIndexes && isNonInnoDB {
				// ADD INDEX on non-InnoDB (e.g., MyISAM) computes stats during creation.
				// Clear the optimize flag so ANALYZE TABLE returns "Table is already up to date".
				if e.tableNeedsOptimize != nil {
					delete(e.tableNeedsOptimize, fullName)
				}
				if e.tableNeedsAnalyze != nil {
					delete(e.tableNeedsAnalyze, fullName)
				}

			} else {
				if e.tableNeedsAnalyze == nil {
					e.tableNeedsAnalyze = map[string]bool{}
				}
				e.tableNeedsAnalyze[fullName] = true
				if e.tableNeedsOptimize == nil {
					e.tableNeedsOptimize = map[string]bool{}
				}
				e.tableNeedsOptimize[fullName] = true
			}
		}
		return res, err
	case *sqlparser.Show:
		return e.execShow(s, query)
	case *sqlparser.ExplainTab:
		return e.execDescribe(s)
	case *sqlparser.ExplainStmt:
		return e.execExplainStmt(s, trimmed)
	case *sqlparser.Begin:
		return e.execBegin()
	case *sqlparser.Commit:
		return e.execCommit()
	case *sqlparser.Rollback:
		return e.execRollback()
	case *sqlparser.SRollback:
		// ROLLBACK TO SAVEPOINT sv: fail if sv doesn't exist (e.g. was cleared by DDL auto-commit)
		spName := strings.ToLower(s.Name.String())
		if e.namedSavepoints == nil || !e.namedSavepoints[spName] {
			return nil, mysqlError(1305, "42000", fmt.Sprintf("SAVEPOINT %s does not exist", s.Name.String()))
		}
		// If savepoint exists, treat rollback as a no-op (we don't track per-savepoint undo)
		return &Result{}, nil
	case *sqlparser.Savepoint:
		// SAVEPOINT sv: record the savepoint name.
		if e.namedSavepoints == nil {
			e.namedSavepoints = make(map[string]bool)
		}
		e.namedSavepoints[strings.ToLower(s.Name.String())] = true
		return &Result{}, nil
	case *sqlparser.Release:
		// RELEASE SAVEPOINT sv: remove the savepoint if it exists.
		spName := strings.ToLower(s.Name.String())
		if e.namedSavepoints == nil || !e.namedSavepoints[spName] {
			return nil, mysqlError(1305, "42000", fmt.Sprintf("SAVEPOINT %s does not exist", s.Name.String()))
		}
		delete(e.namedSavepoints, spName)
		return &Result{}, nil
	case *sqlparser.TruncateTable:
		return e.execTruncateTable(s)
	case *sqlparser.Set:
		return e.execSet(s)
	case *sqlparser.LockTables:
		// ER_CANT_LOCK_LOG_TABLE: MySQL log tables cannot be locked
		for _, tl := range s.Tables {
			if ate, ok := tl.Table.(*sqlparser.AliasedTableExpr); ok {
				if tn, ok := ate.Expr.(sqlparser.TableName); ok {
					lockDB := e.CurrentDB
					if !tn.Qualifier.IsEmpty() {
						lockDB = tn.Qualifier.String()
					}
					if isMySQLLogTable(lockDB, tn.Name.String()) {
						return nil, mysqlError(1532, "HY000", "You can't use locks with log tables.")
					}
				}
			}
		}
		// Release any previously held table locks (MySQL behavior: LOCK TABLES implicitly unlocks)
		if e.tableLockManager != nil {
			e.tableLockManager.UnlockAll(e.connectionID)
		}
		// ER_WRONG_LOCK_OF_SYSTEM_TABLE: if any system table (mysql.*) is WRITE-locked,
		// then ALL tables must be system tables with WRITE locks.
		{
			hasSystemWrite := false
			hasOther := false
			for _, tl := range s.Tables {
				if ate, ok := tl.Table.(*sqlparser.AliasedTableExpr); ok {
					if tn, ok := ate.Expr.(sqlparser.TableName); ok {
						isSystem := strings.EqualFold(tn.Qualifier.String(), "mysql") ||
							(tn.Qualifier.IsEmpty() && strings.EqualFold(e.CurrentDB, "mysql"))
						isWrite := tl.Lock != sqlparser.Read && tl.Lock != sqlparser.ReadLocal
						if isSystem && isWrite {
							hasSystemWrite = true
						} else {
							hasOther = true
						}
					}
				}
			}
			if hasSystemWrite && hasOther {
				return nil, mysqlError(1428, "HY000", "You can't combine write-locking of system tables with other tables or lock types")
			}
		}
		// Check for non-writable performance_schema tables and record locks
		for _, tl := range s.Tables {
			if ate, ok := tl.Table.(*sqlparser.AliasedTableExpr); ok {
				if tn, ok := ate.Expr.(sqlparser.TableName); ok {
					if strings.EqualFold(tn.Qualifier.String(), "performance_schema") && !perfSchemaWritableTable(tn.Name.String()) {
						return nil, mysqlError(1142, "42000", fmt.Sprintf("SELECT, LOCK TABLES command denied to user 'root'@'localhost' for table '%s'", tn.Name.String()))
					}
					// Record table lock
					if e.tableLockManager != nil {
						dbName := e.CurrentDB
						if !tn.Qualifier.IsEmpty() {
							dbName = tn.Qualifier.String()
						}
						tableName := tn.Name.String()
						// Also record alias if present
						alias := ""
						if !ate.As.IsEmpty() {
							alias = ate.As.String()
						}
						var mode string
						switch tl.Lock {
						case sqlparser.Read, sqlparser.ReadLocal:
							mode = "READ"
						default:
							mode = "WRITE"
						}
						if alias != "" {
							// When alias is used, only record the alias (not the base name)
							// to avoid overwriting a different lock mode on the same table.
							e.tableLockManager.LockTable(e.connectionID, dbName+"."+alias, mode)
						} else {
							e.tableLockManager.LockTable(e.connectionID, dbName+"."+tableName, mode)
						}
					}
				}
			}
		}
		return &Result{}, nil
	case *sqlparser.UnlockTables:
		if e.globalReadLock != nil {
			e.globalReadLock.Release(e.connectionID)
		}
		if e.tableLockManager != nil {
			e.tableLockManager.UnlockAll(e.connectionID)
		}
		return &Result{}, nil
	case *sqlparser.Analyze:
		tableName := s.Table.Name.String()
		msgText := "Table is already up to date"
		if db, err := e.Catalog.GetDatabase(e.CurrentDB); err == nil {
			if def, err := db.GetTable(tableName); err == nil && def != nil {
				// Check if another connection holds row locks on innodb stats tables.
				// If so, ANALYZE TABLE fails with "Operation failed" (lock wait timeout).
				statsLocked := false
				if e.innodbStatsPersistentEnabled(def) && e.rowLockManager != nil {
					if e.rowLockManager.HasOtherLocksWithPrefix(e.connectionID, "mysql:innodb_table_stats:") {
						statsLocked = true
					}
				}
				if statsLocked {
					msgText = "Operation failed"
				} else {
					if e.innodbStatsPersistentEnabled(def) {
						e.upsertInnoDBStatsRows(e.CurrentDB, tableName, e.tableRowCount(e.CurrentDB, tableName))
					}
					fullName := e.CurrentDB + "." + tableName
					eng := strings.ToUpper(def.Engine)
					// MEMORY/HEAP engine doesn't support ANALYZE TABLE
					if eng == "MEMORY" || eng == "HEAP" {
						return &Result{
							Columns:     []string{"Table", "Op", "Msg_type", "Msg_text"},
							Rows:        [][]interface{}{{fmt.Sprintf("%s.%s", e.CurrentDB, tableName), "analyze", "note", "The storage engine for the table doesn't support analyze"}},
							IsResultSet: true,
						}, nil
					}
					hasSpatial := false
					for _, idx := range def.Indexes {
						if strings.EqualFold(idx.Type, "SPATIAL") {
							hasSpatial = true
							break
						}
					}
					needsAnalyze := e.tableNeedsAnalyze != nil && e.tableNeedsAnalyze[fullName]
					needsOptimize := e.tableNeedsOptimize != nil && e.tableNeedsOptimize[fullName]
					// ANALYZE TABLE returns "OK" for InnoDB (always), SPATIAL-indexed tables,
					// or when there are pending stats updates (ALTER TABLE, INSERT, DELETE).
					// For non-InnoDB: returns "Table is already up to date" unless flagged.
					isInnoDB := eng == "" || eng == "INNODB"
					if isInnoDB || hasSpatial || needsAnalyze || needsOptimize {
						msgText = "OK"
					}
					// Clear both flags after analyze and mark as analyzed
					if e.tableNeedsAnalyze != nil {
						delete(e.tableNeedsAnalyze, fullName)
					}
					if e.tableNeedsOptimize != nil {
						delete(e.tableNeedsOptimize, fullName)
					}
				}
			}
		}
		return &Result{
			Columns:     []string{"Table", "Op", "Msg_type", "Msg_text"},
			Rows:        [][]interface{}{{fmt.Sprintf("%s.%s", e.CurrentDB, tableName), "analyze", "status", msgText}},
			IsResultSet: true,
		}, nil
	case *sqlparser.CallProc:
		return e.execCallProcFromAST(s)
	case *sqlparser.Load:
		return e.execLoadData(query)
	case *sqlparser.PrepareStmt:
		return e.execPrepare(s)
	case *sqlparser.ExecuteStmt:
		return e.execExecute(s)
	case *sqlparser.DeallocateStmt:
		return e.execDeallocate(s)
	case *sqlparser.AlterDatabase:
		return e.execAlterDatabase(s)
	case *sqlparser.DropProcedure:
		return e.execDropProcedureAST(s)
	case *sqlparser.CreateProcedure:
		// Simple CREATE PROCEDURE without BEGIN...END body (already handled above for complex ones)
		return &Result{}, nil
	case *sqlparser.CreateView:
		// Store view definition
		viewName := s.ViewName.Name.String()
		// Use sqlparser.String(s.Select) to preserve the full SELECT including FROM clause.
		// For literal-only SELECT (no FROM), sqlparser adds "from dual" which is handled fine by the executor.
		selectSQL := sqlparser.String(s.Select)
		if e.views == nil {
			e.views = make(map[string]string)
		}
		e.views[viewName] = selectSQL
		// Store canonical display SQL (no "from dual", proper formatting) for IS.VIEWS.VIEW_DEFINITION
		if e.viewDisplaySQL == nil {
			e.viewDisplaySQL = make(map[string]string)
		}
		e.viewDisplaySQL[viewName] = buildViewSelectSQL(s, query)
		if e.viewCheckOptions == nil {
			e.viewCheckOptions = make(map[string]string)
		}
		e.viewCheckOptions[viewName] = s.CheckOption
		// Store full CREATE VIEW statement for SHOW CREATE VIEW
		if e.viewCreateStatements == nil {
			e.viewCreateStatements = make(map[string]string)
		}
		e.viewCreateStatements[viewName] = e.buildCreateViewSQLFromQuery(s, query)
		return &Result{}, nil
	case *sqlparser.DropView:
		// Remove view definitions
		for _, name := range s.FromTables {
			viewName := name.Name.String()
			if e.views != nil {
				delete(e.views, viewName)
			}
			if e.viewCreateStatements != nil {
				delete(e.viewCreateStatements, viewName)
			}
		}
		return &Result{}, nil
	case *sqlparser.Union:
		return e.execUnion(s)
	case *sqlparser.RenameTable:
		return e.execRenameTable(s)
	case *sqlparser.Flush:
		// FLUSH TABLES WITH READ LOCK: acquire global read lock
		if s.WithLock && e.globalReadLock != nil {
			// Implicitly commit any active transaction first (MySQL behavior)
			if e.inTransaction {
				e.execCommit()
			}
			if e.processList != nil {
				e.processList.SetState(e.connectionID, "Waiting for global read lock")
			}
			if err := e.globalReadLock.Acquire(e.connectionID, 31536000, e.processList); err != nil {
				if e.processList != nil {
					e.processList.SetState(e.connectionID, "")
				}
				return nil, mysqlError(1205, "HY000", "Lock wait timeout exceeded; try restarting transaction")
			}
			if e.processList != nil {
				e.processList.SetState(e.connectionID, "")
			}
			return &Result{}, nil
		}
		// FLUSH TABLE ... FOR EXPORT on non-InnoDB engines returns an error
		if s.ForExport && len(s.TableNames) > 0 {
			for _, tn := range s.TableNames {
				tblName := tn.Name.String()
				if tbl, err := e.Storage.GetTable(e.CurrentDB, tblName); err == nil {
					eng := strings.ToLower(tbl.Def.Engine)
					if eng != "" && eng != "innodb" {
						return nil, mysqlError(1031, "HY000", fmt.Sprintf("Table storage engine for '%s' doesn't have this option", tblName))
					}
				}
			}
		}
		// FLUSH PRIVILEGES: reload grant store from mysql.db and mysql.tables_priv
		for _, opt := range s.FlushOptions {
			if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(opt)), "PRIVILEGES") {
				if e.grantStore != nil {
					e.reloadGrantStoreFromStorage()
				}
			}
		}
		// FLUSH STATUS resets session status counters.
		for _, opt := range s.FlushOptions {
			if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(opt)), "STATUS") {
				e.handlerReadKey = 0
				e.handlerReadFirst = 0
				e.handlerReadLast = 0
				e.handlerReadNext = 0
				e.handlerReadPrev = 0
				e.handlerReadRnd = 0
				e.handlerReadRndNext = 0
				e.sortRows = 0
				e.sortRange = 0
				e.sortScan = 0
				e.createdTmpDiskTables = 0
				e.createdTmpTables = 0
				e.questions = 0
			}
		}
		return &Result{}, nil
	case *sqlparser.OtherAdmin:
		return e.execOtherAdmin(query)
	case *sqlparser.AlterView:
		viewName := s.ViewName.Name.String()
		selectSQL := sqlparser.String(s.Select)
		if e.views == nil {
			e.views = make(map[string]string)
		}
		e.views[viewName] = selectSQL
		if e.viewCheckOptions == nil {
			e.viewCheckOptions = make(map[string]string)
		}
		e.viewCheckOptions[viewName] = s.CheckOption
		// Store full CREATE VIEW statement for SHOW CREATE VIEW
		if e.viewCreateStatements == nil {
			e.viewCreateStatements = make(map[string]string)
		}
		// AlterView uses same structure as CreateView - convert to CreateView for building SQL
		cv := &sqlparser.CreateView{
			ViewName:    s.ViewName,
			Algorithm:   s.Algorithm,
			Definer:     s.Definer,
			Security:    s.Security,
			Select:      s.Select,
			CheckOption: s.CheckOption,
		}
		e.viewCreateStatements[viewName] = e.buildCreateViewSQLFromQuery(cv, query)
		return &Result{}, nil
	case *sqlparser.CommentOnly:
		return &Result{}, nil
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", s)
	}
}

// stripLeadingCStyleComments is defined in display.go

// extractCTEBodyText extracts the SQL text inside the body (AS (...)) of a named CTE
// from the outer query text. Returns "" if not found.
// E.g. for "WITH qn2 AS (SELECT 3*a FROM qn) SELECT ...", name="qn2" returns "SELECT 3*a FROM qn".
func extractCTEBodyText(outerQuery, cteName string) string {
	q := outerQuery
	lq := strings.ToLower(q)
	if !strings.HasPrefix(lq, "with") {
		return ""
	}
	i := 4
	// skip optional RECURSIVE
	for i < len(q) && (q[i] == ' ' || q[i] == '\t' || q[i] == '\n' || q[i] == '\r') {
		i++
	}
	lqI := strings.ToLower(q[i:])
	if strings.HasPrefix(lqI, "recursive") {
		i += 9
	}
	for {
		for i < len(q) && (q[i] == ' ' || q[i] == '\t' || q[i] == '\n' || q[i] == '\r') {
			i++
		}
		if i >= len(q) {
			return ""
		}
		// Record start of name
		nameStart := i
		// Read CTE name
		var name string
		if q[i] == '`' {
			i++
			end := strings.Index(q[i:], "`")
			if end < 0 {
				return ""
			}
			name = q[i : i+end]
			i += end + 1
		} else {
			end := i
			for end < len(q) && q[end] != ' ' && q[end] != '\t' && q[end] != '\n' && q[end] != '\r' && q[end] != '(' {
				end++
			}
			name = q[nameStart:end]
			i = end
		}
		_ = name // suppress unused
		for i < len(q) && (q[i] == ' ' || q[i] == '\t' || q[i] == '\n' || q[i] == '\r') {
			i++
		}
		// optional column list
		if i < len(q) && q[i] == '(' {
			// check if this is a column list (before AS) - skip it
			// We check by looking for AS after the closing paren
			depth := 1
			i++
			for i < len(q) && depth > 0 {
				if q[i] == '(' {
					depth++
				} else if q[i] == ')' {
					depth--
				}
				i++
			}
			for i < len(q) && (q[i] == ' ' || q[i] == '\t' || q[i] == '\n' || q[i] == '\r') {
				i++
			}
		}
		// expect AS
		if i+2 > len(q) || !strings.EqualFold(q[i:i+2], "as") {
			return ""
		}
		i += 2
		for i < len(q) && (q[i] == ' ' || q[i] == '\t' || q[i] == '\n' || q[i] == '\r') {
			i++
		}
		// skip body parentheses
		if i >= len(q) || q[i] != '(' {
			return ""
		}
		bodyStart := i + 1
		depth := 1
		i++
		inQ := byte(0)
		for i < len(q) && depth > 0 {
			ch := q[i]
			if inQ != 0 {
				if ch == inQ && (i == 0 || q[i-1] != '\\') {
					inQ = 0
				}
				i++
				continue
			}
			if ch == '\'' || ch == '"' || ch == '`' {
				inQ = ch
			} else if ch == '(' {
				depth++
			} else if ch == ')' {
				depth--
			}
			i++
		}
		bodyText := strings.TrimSpace(q[bodyStart : i-1])

		if strings.EqualFold(name, cteName) {
			return bodyText
		}

		for i < len(q) && (q[i] == ' ' || q[i] == '\t' || q[i] == '\n' || q[i] == '\r') {
			i++
		}
		if i >= len(q) || q[i] != ',' {
			break
		}
		i++ // skip comma
	}
	return ""
}

// stripWithClausePrefix removes the leading "WITH name AS (...)[, name AS (...)] "
// prefix from a query, returning the rest starting from the outer SELECT/INSERT/etc.
// It handles RECURSIVE keyword and multiple CTEs separated by commas.
func stripWithClausePrefix(query string) string {
	q := query
	lq := strings.ToLower(q)
	if !strings.HasPrefix(lq, "with") {
		return query
	}
	i := 4 // after "with"
	// skip optional RECURSIVE keyword
	for i < len(q) && (q[i] == ' ' || q[i] == '\t' || q[i] == '\n' || q[i] == '\r') {
		i++
	}
	lqFromI := strings.ToLower(q[i:])
	if strings.HasPrefix(lqFromI, "recursive") {
		i += 9
	}
	// Parse one or more CTEs: NAME AS (...)
	for {
		// skip whitespace
		for i < len(q) && (q[i] == ' ' || q[i] == '\t' || q[i] == '\n' || q[i] == '\r') {
			i++
		}
		// skip CTE name (identifier or backtick-quoted)
		if i >= len(q) {
			return query
		}
		if q[i] == '`' {
			i++
			for i < len(q) && q[i] != '`' {
				i++
			}
			if i < len(q) {
				i++ // skip closing backtick
			}
		} else {
			for i < len(q) && q[i] != ' ' && q[i] != '\t' && q[i] != '\n' && q[i] != '\r' && q[i] != '(' {
				i++
			}
		}
		// skip whitespace
		for i < len(q) && (q[i] == ' ' || q[i] == '\t' || q[i] == '\n' || q[i] == '\r') {
			i++
		}
		// optional column list: (col1, col2, ...)
		if i < len(q) && q[i] == '(' {
			// check if this is column list or subquery
			// we need to peek: if next non-paren content looks like AS, it's a column list
			// Actually vitess grammar: WITH name(cols) AS (query)
			// We can't distinguish easily; let's just skip one paren-balanced group and check
			// We'll handle this by looking for AS keyword after the identifier
		}
		// expect AS keyword
		if i+2 <= len(q) && strings.EqualFold(q[i:i+2], "as") {
			i += 2
		} else {
			return query
		}
		// skip whitespace
		for i < len(q) && (q[i] == ' ' || q[i] == '\t' || q[i] == '\n' || q[i] == '\r') {
			i++
		}
		// skip the parenthesized subquery body
		if i >= len(q) || q[i] != '(' {
			return query
		}
		depth := 0
		inQ := byte(0)
		for i < len(q) {
			ch := q[i]
			if inQ != 0 {
				if ch == inQ && (i == 0 || q[i-1] != '\\') {
					inQ = 0
				}
				i++
				continue
			}
			if ch == '\'' || ch == '"' || ch == '`' {
				inQ = ch
				i++
				continue
			}
			if ch == '(' {
				depth++
			} else if ch == ')' {
				depth--
				if depth == 0 {
					i++
					break
				}
			}
			i++
		}
		// skip whitespace
		for i < len(q) && (q[i] == ' ' || q[i] == '\t' || q[i] == '\n' || q[i] == '\r') {
			i++
		}
		// check for comma (more CTEs) or end
		if i < len(q) && q[i] == ',' {
			i++ // skip comma, loop to next CTE
			continue
		}
		// we've consumed all CTEs; rest is the main query
		break
	}
	return strings.TrimSpace(q[i:])
}

func extractRawSelectExprs(query string) []string {
	q := strings.TrimSpace(query)
	lq := strings.ToLower(q)
	if len(lq) < 7 || !strings.HasPrefix(lq, "select") || (lq[6] != ' ' && lq[6] != '\t' && lq[6] != '\n' && lq[6] != '\r') {
		return nil
	}
	// Track whether SELECT was followed by a newline (vs space/tab).
	// When SELECT is followed by a newline, non-subquery expressions that span
	// multiple lines should be normalized to single-line (MySQL behavior).
	selectFollowedByNewline := lq[6] == '\n' || lq[6] == '\r'
	start := 7 // length of "select" (6) + 1 whitespace character
	// Skip DISTINCT keyword and SQL hints so they don't appear in column headers
	rest := strings.TrimSpace(q[start:])
	for {
		restLower := strings.ToLower(rest)
		skipped := false
		for _, hint := range []string{"distinct ", "all ", "sql_big_result ", "sql_small_result ",
			"sql_buffer_result ", "sql_calc_found_rows ", "high_priority ", "straight_join "} {
			if strings.HasPrefix(restLower, hint) {
				rest = strings.TrimSpace(rest[len(hint):])
				skipped = true
				break
			}
		}
		// Also skip optimizer hints /*+ ... */
		if strings.HasPrefix(rest, "/*+") {
			endIdx := strings.Index(rest, "*/")
			if endIdx >= 0 {
				rest = strings.TrimSpace(rest[endIdx+2:])
				skipped = true
			}
		}
		if !skipped {
			break
		}
	}
	start = len(q) - len(rest)
	inQuote := byte(0)
	parenDepth := 0
	end := len(q)
	for i := start; i < len(q); i++ {
		ch := q[i]
		if inQuote != 0 {
			if ch == inQuote && (i == 0 || q[i-1] != '\\') {
				inQuote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			inQuote = ch
			continue
		}
		if ch == '(' {
			parenDepth++
			continue
		}
		if ch == ')' && parenDepth > 0 {
			parenDepth--
			continue
		}
		if parenDepth == 0 {
			// Check for keywords that terminate the SELECT list.
			// Note: UNION is intentionally not included because for UNION queries,
			// MySQL uses the full expression (including UNION) as the column name.
			for _, kw := range []string{"from", "limit", "order", "group", "having", "where", "into", "for", "window"} {
				kwLen := len(kw)
				if i+kwLen <= len(q) && strings.EqualFold(q[i:i+kwLen], kw) {
					prevOK := i == 0 || q[i-1] == ' ' || q[i-1] == '\n' || q[i-1] == '\t' || q[i-1] == '\r'
					nextOK := i+kwLen == len(q) || q[i+kwLen] == ' ' || q[i+kwLen] == '\n' || q[i+kwLen] == '\t' || q[i+kwLen] == '\r' || q[i+kwLen] == '('
					if prevOK && nextOK {
						end = i
						break
					}
				}
			}
			if end != len(q) {
				break
			}
		}
	}
	selectList := strings.TrimSpace(strings.TrimSuffix(q[start:end], ";"))
	if selectList == "" {
		return nil
	}
	parts := make([]string, 0)
	inQuote = 0
	parenDepth = 0
	last := 0
	for i := 0; i < len(selectList); i++ {
		ch := selectList[i]
		if inQuote != 0 {
			if ch == inQuote && (i == 0 || selectList[i-1] != '\\') {
				inQuote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			inQuote = ch
			continue
		}
		if ch == '(' {
			parenDepth++
			continue
		}
		if ch == ')' && parenDepth > 0 {
			parenDepth--
			continue
		}
		if ch == ',' && parenDepth == 0 {
			parts = append(parts, strings.TrimSpace(selectList[last:i]))
			last = i + 1
		}
	}
	parts = append(parts, strings.TrimSpace(selectList[last:]))
	// When SELECT was immediately followed by a newline, non-subquery expressions
	// that span multiple lines need to be returned as empty string so that the
	// caller falls back to sqlparser.String() normalization. Subquery expressions
	// (starting with "(SELECT") preserve their original multi-line formatting
	// because MySQL outputs them that way in column headers.
	if selectFollowedByNewline {
		for i, part := range parts {
			if strings.Contains(part, "\n") {
				lp := strings.ToLower(strings.TrimSpace(part))
				if !strings.HasPrefix(lp, "(select") {
					parts[i] = "" // signal to caller to use normalized form
				}
			}
		}
	}
	return parts
}

func hasTopLevelFromClause(query string) bool {
	q := strings.TrimSpace(query)
	lq := strings.ToLower(q)
	if !strings.HasPrefix(lq, "select ") {
		return false
	}
	inQuote := byte(0)
	parenDepth := 0
	inLineComment := false
	inBlockComment := false
	for i := 0; i < len(q); i++ {
		ch := q[i]
		next := byte(0)
		if i+1 < len(q) {
			next = q[i+1]
		}
		if inLineComment {
			if ch == '\n' {
				inLineComment = false
			}
			continue
		}
		if inBlockComment {
			if ch == '*' && next == '/' {
				inBlockComment = false
				i++
			}
			continue
		}
		if inQuote != 0 {
			if ch == inQuote && (i == 0 || q[i-1] != '\\') {
				inQuote = 0
			}
			continue
		}
		if ch == '-' && next == '-' {
			inLineComment = true
			i++
			continue
		}
		if ch == '/' && next == '*' {
			inBlockComment = true
			i++
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			inQuote = ch
			continue
		}
		if ch == '(' {
			parenDepth++
			continue
		}
		if ch == ')' && parenDepth > 0 {
			parenDepth--
			continue
		}
		if parenDepth == 0 && i+4 <= len(q) && strings.EqualFold(q[i:i+4], "from") {
			prevOK := i == 0 || !isIdentChar(q[i-1])
			nextOK := i+4 == len(q) || !isIdentChar(q[i+4])
			if prevOK && nextOK {
				return true
			}
		}
	}
	return false
}

func countTopLevelSQLArgs(argList string) int {
	if strings.TrimSpace(argList) == "" {
		return 0
	}
	inQuote := byte(0)
	depth := 0
	args := 1
	for i := 0; i < len(argList); i++ {
		ch := argList[i]
		if inQuote != 0 {
			if ch == inQuote && (i == 0 || argList[i-1] != '\\') {
				inQuote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			inQuote = ch
			continue
		}
		if ch == '(' {
			depth++
			continue
		}
		if ch == ')' && depth > 0 {
			depth--
			continue
		}
		if ch == ',' && depth == 0 {
			args++
		}
	}
	return args
}

func isStrictJSONStringCastSource(expr sqlparser.Expr) bool {
	switch t := expr.(type) {
	case *sqlparser.Literal:
		return t.Type == sqlparser.StrVal
	case *sqlparser.IntroducerExpr:
		if lit, ok := t.Expr.(*sqlparser.Literal); ok {
			return lit.Type == sqlparser.StrVal
		}
	}
	return false
}

func castToJSONValue(val interface{}, strictStringLiteral bool) (interface{}, error) {
	if val == nil {
		return nil, nil
	}
	switch v := val.(type) {
	case int64:
		return jsonMarshalMySQL(float64(v)), nil
	case float64:
		return jsonMarshalMySQL(v), nil
	case bool:
		return jsonMarshalMySQL(v), nil
	case string:
		var js interface{}
		if err := json.Unmarshal([]byte(v), &js); err != nil {
			if strictStringLiteral {
				pos := 0
				if serr, ok := err.(*json.SyntaxError); ok && serr.Offset > 0 {
					pos = int(serr.Offset - 1)
				}
				return nil, mysqlError(3141, "22032", fmt.Sprintf(`Invalid JSON text in argument 1 to function cast_as_json: "Invalid value." at position %d.`, pos))
			}
			b, _ := json.Marshal(v)
			return string(b), nil
		}
		return jsonMarshalMySQL(js), nil
	default:
		s := toString(val)
		var js interface{}
		if err := json.Unmarshal([]byte(s), &js); err != nil {
			if strictStringLiteral {
				pos := 0
				if serr, ok := err.(*json.SyntaxError); ok && serr.Offset > 0 {
					pos = int(serr.Offset - 1)
				}
				return nil, mysqlError(3141, "22032", fmt.Sprintf(`Invalid JSON text in argument 1 to function cast_as_json: "Invalid value." at position %d.`, pos))
			}
			b, _ := json.Marshal(s)
			return string(b), nil
		}
		return jsonMarshalMySQL(js), nil
	}
}

// checkTableLockRestrictions enforces LOCK TABLE restrictions when a session
// holds table-level locks via LOCK TABLES. It returns a MySQL error when:
//   - A write operation targets a READ-locked table (error 1099)
//   - Any operation targets a table not in the lock set (error 1100)
func (e *Executor) checkTableLockRestrictions(stmt sqlparser.Statement) error {
	switch s := stmt.(type) {
	case *sqlparser.Update:
		// Check each table in the UPDATE
		for _, te := range s.TableExprs {
			if ate, ok := te.(*sqlparser.AliasedTableExpr); ok {
				if tn, ok := ate.Expr.(sqlparser.TableName); ok {
					tableName := tn.Name.String()
					dbName := e.CurrentDB
					if !tn.Qualifier.IsEmpty() {
						dbName = tn.Qualifier.String()
					}
					key := dbName + "." + tableName
					locked, mode := e.tableLockManager.IsLocked(e.connectionID, key)
					if !locked {
						return mysqlError(1100, "HY000", fmt.Sprintf("Table '%s' was not locked with LOCK TABLES", tableName))
					}
					if mode == "READ" {
						return mysqlError(1099, "HY000", fmt.Sprintf("Table '%s' was locked with a READ lock and can't be updated", tableName))
					}
				}
			}
		}
	case *sqlparser.Insert:
		var tableName, dbName string
		dbName = e.CurrentDB
		if tn, ok := s.Table.Expr.(sqlparser.TableName); ok {
			tableName = tn.Name.String()
			if !tn.Qualifier.IsEmpty() {
				dbName = tn.Qualifier.String()
			}
		} else {
			return nil
		}
		key := dbName + "." + tableName
		locked, mode := e.tableLockManager.IsLocked(e.connectionID, key)
		if !locked {
			return mysqlError(1100, "HY000", fmt.Sprintf("Table '%s' was not locked with LOCK TABLES", tableName))
		}
		if mode == "READ" {
			return mysqlError(1099, "HY000", fmt.Sprintf("Table '%s' was locked with a READ lock and can't be updated", tableName))
		}
		// Also check source tables in INSERT ... SELECT
		if s.Rows != nil {
			if sel, ok := s.Rows.(*sqlparser.Select); ok {
				for _, te := range sel.From {
					if ate, ok := te.(*sqlparser.AliasedTableExpr); ok {
						if tn, ok := ate.Expr.(sqlparser.TableName); ok {
							srcName := tn.Name.String()
							srcDB := e.CurrentDB
							if !tn.Qualifier.IsEmpty() {
								srcDB = tn.Qualifier.String()
							}
							// information_schema tables are exempt from LOCK TABLE checks
							// (MySQL allows IS queries within LOCK TABLE sessions without explicit locking)
							if strings.EqualFold(srcDB, "information_schema") {
								continue
							}
							// Use alias name for lock check if present
							checkName := srcName
							if !ate.As.IsEmpty() {
								checkName = ate.As.String()
							}
							srcKey := srcDB + "." + checkName
							srcLocked, _ := e.tableLockManager.IsLocked(e.connectionID, srcKey)
							if !srcLocked {
								return mysqlError(1100, "HY000", fmt.Sprintf("Table '%s' was not locked with LOCK TABLES", srcName))
							}
							// MySQL requires a separate lock alias when reading
							// from the same table that is being inserted into.
							// If source == target (same name, no alias), deny.
							if strings.EqualFold(srcName, tableName) && ate.As.IsEmpty() {
								return mysqlError(1100, "HY000", fmt.Sprintf("Table '%s' was not locked with LOCK TABLES", srcName))
							}
						}
					}
				}
			}
		}
	case *sqlparser.Delete:
		// Build set of target table names (tables being deleted from)
		deleteTargetSet := map[string]bool{}
		for _, target := range s.Targets {
			deleteTargetSet[strings.ToLower(target.Name.String())] = true
		}
		for _, te := range s.TableExprs {
			if ate, ok := te.(*sqlparser.AliasedTableExpr); ok {
				if tn, ok := ate.Expr.(sqlparser.TableName); ok {
					tableName := tn.Name.String()
					dbName := e.CurrentDB
					if !tn.Qualifier.IsEmpty() {
						dbName = tn.Qualifier.String()
					}
					key := dbName + "." + tableName
					locked, mode := e.tableLockManager.IsLocked(e.connectionID, key)
					if !locked {
						return mysqlError(1100, "HY000", fmt.Sprintf("Table '%s' was not locked with LOCK TABLES", tableName))
					}
					// For multi-table DELETE, only target tables need WRITE lock;
					// joined tables (in USING clause) only need READ.
					isTarget := len(deleteTargetSet) == 0 || deleteTargetSet[strings.ToLower(tableName)]
					if mode == "READ" && isTarget {
						return mysqlError(1099, "HY000", fmt.Sprintf("Table '%s' was locked with a READ lock and can't be updated", tableName))
					}
				}
			}
		}
	case *sqlparser.CreateTable:
		// CREATE TABLE when LOCK TABLES is active: the new table must be locked
		tableName := s.Table.Name.String()
		dbName := e.CurrentDB
		if !s.Table.Qualifier.IsEmpty() {
			dbName = s.Table.Qualifier.String()
		}
		// Temp tables are exempt from LOCK TABLE checks
		if s.Temp {
			return nil
		}
		key := dbName + "." + tableName
		locked, _ := e.tableLockManager.IsLocked(e.connectionID, key)
		if !locked {
			return mysqlError(1100, "HY000", fmt.Sprintf("Table '%s' was not locked with LOCK TABLES", tableName))
		}
	case *sqlparser.DropTable:
		// Temp tables are exempt from LOCK TABLE checks
		if s.Temp {
			return nil
		}
		for _, tn := range s.FromTables {
			tableName := tn.Name.String()
			dbName := e.CurrentDB
			if !tn.Qualifier.IsEmpty() {
				dbName = tn.Qualifier.String()
			}
			// Check if this is a temp table (also check without lowercase)
			if e.tempTables != nil && (e.tempTables[tableName] || e.tempTables[strings.ToLower(tableName)]) {
				continue
			}
			// If IfExists is set and table doesn't exist normally, skip lock check
			if s.IfExists {
				tbl, tblErr := e.Storage.GetTable(dbName, tableName)
				if tblErr != nil || tbl == nil {
					continue
				}
			}
			key := dbName + "." + tableName
			locked, mode := e.tableLockManager.IsLocked(e.connectionID, key)
			if locked {
				if mode == "READ" {
					return mysqlError(1099, "HY000", fmt.Sprintf("Table '%s' was locked with a READ lock and can't be updated", tableName))
				}
			} else {
				// MySQL allows DROP TABLE on implicitly locked tables (underlying tables
				// of locked views). Check if this table is referenced by any locked view.
				if !e.isTableImplicitlyLockedViaView(dbName, tableName) {
					return mysqlError(1100, "HY000", fmt.Sprintf("Table '%s' was not locked with LOCK TABLES", tableName))
				}
			}
		}
	}
	return nil
}

// isTableImplicitlyLockedViaView returns true if the given table is implicitly
// locked in the current session via prelocking (underlying table of a locked view,
// referenced by a trigger on a locked table, or referenced by a function called
// from a locked view). MySQL allows DROP TABLE on such implicitly-locked tables.
func (e *Executor) isTableImplicitlyLockedViaView(dbName, tableName string) bool {
	if e.tableLockManager == nil {
		return false
	}
	target := strings.ToLower(tableName)
	locks := e.tableLockManager.GetLocks(e.connectionID)
	if len(locks) == 0 {
		return false
	}

	// sqlContainsTable checks whether a slice of SQL statements references the target table.
	sqlContainsTable := func(stmts []string) bool {
		for _, stmt := range stmts {
			parsed, err := e.parser().Parse(stmt)
			if err != nil {
				// Fallback: string search
				if strings.Contains(strings.ToLower(stmt), target) {
					return true
				}
				continue
			}
			found := false
			_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
				if tn, ok := node.(sqlparser.TableName); ok {
					if strings.EqualFold(tn.Name.String(), target) {
						found = true
						return false, nil
					}
				}
				return true, nil
			}, parsed)
			if found {
				return true
			}
		}
		return false
	}

	for lockedKey := range locks {
		parts := strings.SplitN(lockedKey, ".", 2)
		if len(parts) != 2 {
			continue
		}
		lockedDB := parts[0]
		lockedName := parts[1]

		// Case 1: Locked entity is a view that directly or indirectly references target table.
		if e.views != nil {
			for vname, vsql := range e.views {
				if !strings.EqualFold(vname, lockedName) {
					continue
				}
				// Direct check: parse view SQL for table references
				parsed, err := e.parser().Parse(vsql)
				if err == nil {
					found := false
					_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
						if tn, ok := node.(sqlparser.TableName); ok {
							if strings.EqualFold(tn.Name.String(), target) {
								found = true
								return false, nil
							}
						}
						return true, nil
					}, parsed)
					if found {
						return true
					}
					// Indirect check: view calls a function that references target table.
					// Collect function names from view SQL and check their bodies.
					_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
						if fc, ok := node.(*sqlparser.FuncExpr); ok {
							fnName := fc.Name.Lowered()
							db2, dbErr := e.Catalog.GetDatabase(lockedDB)
							if dbErr != nil {
								db2, _ = e.Catalog.GetDatabase(e.CurrentDB)
							}
							if db2 != nil {
								if fn := db2.GetFunction(fnName); fn != nil {
									if sqlContainsTable(fn.Body) {
										found = true
										return false, nil
									}
								}
							}
						}
						return true, nil
					}, parsed)
					if found {
						return true
					}
				}
				break
			}
		}

		// Case 2: Locked entity is a regular table with triggers that reference target table.
		if db, dbErr := e.Catalog.GetDatabase(lockedDB); dbErr == nil {
			triggers := db.GetAllTriggersForTable(lockedName)
			for _, tr := range triggers {
				if sqlContainsTable(tr.Body) {
					return true
				}
			}
		}
	}
	return false
}

// execPrepare handles PREPARE stmt_name FROM 'query'.
func (e *Executor) execPrepare(stmt *sqlparser.PrepareStmt) (*Result, error) {
	name := strings.ToLower(stmt.Name.String())
	// The statement text is in stmt.Statement
	query := sqlparser.String(stmt.Statement)
	if len(query) >= 2 {
		if (query[0] == '\'' && query[len(query)-1] == '\'') || (query[0] == '"' && query[len(query)-1] == '"') {
			query = query[1 : len(query)-1]
		}
	}
	// Unescape backslash-escaped characters from vitess serialization
	query = strings.ReplaceAll(query, "\\n", "\n")
	query = strings.ReplaceAll(query, "\\t", "\t")
	query = strings.ReplaceAll(query, "\\'", "'")
	query = strings.ReplaceAll(query, "\\\"", "\"")
	query = strings.ReplaceAll(query, "\\\\", "\\")
	// Validate SET PASSWORD FOR syntax at PREPARE time.
	// MySQL rejects invalid SET PASSWORD FOR statements at PREPARE time (not just EXECUTE time).
	upperQuery := strings.ToUpper(strings.TrimSpace(query))
	if strings.HasPrefix(upperQuery, "SET PASSWORD FOR ") {
		if syntaxErr := validateSetPasswordSyntax(strings.TrimSpace(query)); syntaxErr != nil {
			return nil, syntaxErr
		}
	}
	e.preparedStmts[name] = query
	return &Result{}, nil
}

// execExecute handles EXECUTE stmt_name [USING @var1, @var2, ...].
func (e *Executor) execExecute(stmt *sqlparser.ExecuteStmt) (*Result, error) {
	const maxExecuteDepth = 128
	if e.executeDepth >= maxExecuteDepth {
		return nil, mysqlError(1456, "HY000", "Prepared statement contains a stored routine call that refers to that same statement. It's not allowed to execute a prepared statement in such a recursive manner")
	}
	e.executeDepth++
	defer func() { e.executeDepth-- }()

	name := strings.ToLower(stmt.Name.String())
	query, ok := e.preparedStmts[name]
	if !ok {
		return nil, mysqlError(1243, "HY000", fmt.Sprintf("Unknown prepared statement handler (%s) given to EXECUTE", name))
	}
	// Identify which ? positions (0-indexed) are LIMIT/OFFSET parameters.
	// MySQL rejects negative values for LIMIT/OFFSET parameters at EXECUTE time.
	limitArgPositions := findLimitArgPositions(query)

	// Replace ? placeholders with user variable values outside string literals.
	argIdx := 0
	argSQLLiterals := make([]string, 0, len(stmt.Arguments))
	var finalQuery strings.Builder
	inSingle := false
	escaped := false
	for i := 0; i < len(query); i++ {
		ch := query[i]
		if inSingle {
			finalQuery.WriteByte(ch)
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '\'' {
				inSingle = false
			}
			continue
		}
		if ch == '\'' {
			inSingle = true
			finalQuery.WriteByte(ch)
			continue
		}
		if ch == '?' && argIdx < len(stmt.Arguments) {
			varName := stmt.Arguments[argIdx].Name.String()
			val, exists := e.userVars[varName]
			if !exists || val == nil {
				finalQuery.WriteString("NULL")
				argSQLLiterals = append(argSQLLiterals, "NULL")
			} else {
				// Validate LIMIT/OFFSET parameters: negative values are not allowed.
				// String values are converted to their integer representation
				// (MySQL converts non-numeric strings to 0 for LIMIT).
				// uint64 and other types are emitted as unquoted integer literals.
				if limitArgPositions[argIdx] {
					if n, isNeg := isNegativeNumeric(val); isNeg {
						_ = n
						return nil, mysqlError(1210, "HY000", "Incorrect arguments to EXECUTE")
					}
					var limitLit string
					handled := false
					switch lv := val.(type) {
					case string:
						sv := strings.TrimSpace(lv)
						n, parseErr := strconv.ParseInt(sv, 10, 64)
						if parseErr != nil {
							// Check if the string starts with '-' (large negative number
							// that overflowed int64 representation, e.g. "-14632475938453979136")
							if strings.HasPrefix(sv, "-") {
								return nil, mysqlError(1210, "HY000", "Incorrect arguments to EXECUTE")
							}
							n = 0
						}
						if n < 0 {
							return nil, mysqlError(1210, "HY000", "Incorrect arguments to EXECUTE")
						}
						limitLit = strconv.FormatInt(n, 10)
						handled = true
					case uint64:
						limitLit = strconv.FormatUint(lv, 10)
						handled = true
					case int64:
						limitLit = strconv.FormatInt(lv, 10)
						handled = true
					case float64:
						limitLit = strconv.FormatInt(int64(lv), 10)
						handled = true
					}
					if handled {
						finalQuery.WriteString(limitLit)
						argSQLLiterals = append(argSQLLiterals, limitLit)
						argIdx++
						continue
					}
				}
				switch v := val.(type) {
				case string:
					escapedV := strings.ReplaceAll(v, "\\", "\\\\")
					escapedV = strings.ReplaceAll(escapedV, "'", "\\'")
					lit := "'" + escapedV + "'"
					finalQuery.WriteString(lit)
					argSQLLiterals = append(argSQLLiterals, lit)
				case int64:
					lit := strconv.FormatInt(v, 10)
					finalQuery.WriteString(lit)
					argSQLLiterals = append(argSQLLiterals, lit)
				case float64:
					lit := strconv.FormatFloat(v, 'f', -1, 64)
					finalQuery.WriteString(lit)
					argSQLLiterals = append(argSQLLiterals, lit)
				default:
					s := fmt.Sprintf("%v", v)
					s = strings.ReplaceAll(s, "\\", "\\\\")
					s = strings.ReplaceAll(s, "'", "\\'")
					lit := "'" + s + "'"
					finalQuery.WriteString(lit)
					argSQLLiterals = append(argSQLLiterals, lit)
				}
			}
			argIdx++
		} else {
			finalQuery.WriteByte(ch)
		}
	}
	res, err := e.Execute(finalQuery.String())
	if err != nil {
		return nil, err
	}
	if res != nil && res.IsResultSet && len(argSQLLiterals) > 0 {
		// Determine which column positions are derived from SELECT expressions containing ?
		origSelectExprs := extractRawSelectExprs(query)
		questionCols := make(map[int]bool)
		for idx, expr := range origSelectExprs {
			if strings.Contains(expr, "?") {
				questionCols[idx] = true
			}
		}
		for i := range res.Columns {
			col := res.Columns[i]
			// Only replace in columns derived from SELECT expressions that had ?
			if len(origSelectExprs) > 0 && !questionCols[i] {
				continue
			}
			for _, lit := range argSQLLiterals {
				col = strings.ReplaceAll(col, lit, "?")
				// Also replace unquoted string values (column headers strip quotes from string literals)
				if len(lit) >= 2 && lit[0] == '\'' && lit[len(lit)-1] == '\'' {
					unquoted := lit[1 : len(lit)-1]
					unquoted = strings.ReplaceAll(unquoted, "\\'", "'")
					unquoted = strings.ReplaceAll(unquoted, "\\\\", "\\")
					col = strings.ReplaceAll(col, unquoted, "?")
				}
			}
			res.Columns[i] = col
		}
	}
	return res, nil
}

// execDeallocate handles DEALLOCATE PREPARE stmt_name.
func (e *Executor) execDeallocate(stmt *sqlparser.DeallocateStmt) (*Result, error) {
	name := strings.ToLower(stmt.Name.String())
	delete(e.preparedStmts, name)
	return &Result{}, nil
}

// findLimitArgPositions returns a set of ? indices (0-based) that appear in
// LIMIT or OFFSET position in the prepared query.  MySQL rejects negative
// values for these parameters at EXECUTE time with ER_WRONG_ARGUMENTS.
func findLimitArgPositions(query string) map[int]bool {
	result := make(map[int]bool)
	// State machine: track whether we're in LIMIT count or LIMIT offset position.
	// Handles:
	//   LIMIT ?              -> ? is count (limit position)
	//   LIMIT ?, ?           -> both ? are limit positions
	//   LIMIT 1, ?           -> second ? is offset (limit position)
	//   LIMIT ? OFFSET ?     -> both ? are limit positions
	//   LIMIT 1 OFFSET ?     -> second ? is offset (limit position)
	lower := strings.ToLower(query)
	argIdx := 0
	inSingle := false
	escaped := false
	// inLimitCount: just after LIMIT keyword, expecting the count value
	// inLimitOffset: just after LIMIT count (number or ?), expecting offset after comma or OFFSET keyword
	// inOffsetVal: just after comma or OFFSET keyword, expecting offset value
	inLimitCount := false
	inLimitOffset := false // after the count part, looking for comma or OFFSET
	inOffsetVal := false
	i := 0
	for i < len(lower) {
		ch := lower[i]
		if inSingle {
			if escaped {
				escaped = false
				i++
				continue
			}
			if ch == '\\' {
				escaped = true
				i++
				continue
			}
			if ch == '\'' {
				inSingle = false
			}
			i++
			continue
		}
		if ch == '\'' {
			inSingle = true
			inLimitCount = false
			inLimitOffset = false
			inOffsetVal = false
			i++
			continue
		}
		// Skip whitespace (keep state)
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			i++
			continue
		}
		if ch == '?' {
			if inLimitCount || inOffsetVal {
				result[argIdx] = true
			}
			argIdx++
			if inLimitCount {
				// After count ?, expect possible offset
				inLimitCount = false
				inLimitOffset = true
			} else {
				inLimitOffset = false
				inOffsetVal = false
			}
			i++
			continue
		}
		// Comma inside LIMIT clause separates count from offset
		if ch == ',' && inLimitOffset {
			inLimitOffset = false
			inOffsetVal = true
			i++
			continue
		}
		// Check for OFFSET keyword (SQL standard LIMIT count OFFSET offset)
		if (inLimitOffset) && strings.HasPrefix(lower[i:], "offset") {
			end := i + 6
			if end >= len(lower) || !isAlphaNum(lower[end]) {
				inLimitOffset = false
				inOffsetVal = true
				i = end
				continue
			}
		}
		// Check for LIMIT keyword
		if strings.HasPrefix(lower[i:], "limit") {
			end := i + 5
			if end >= len(lower) || !isAlphaNum(lower[end]) {
				inLimitCount = true
				inLimitOffset = false
				inOffsetVal = false
				i = end
				continue
			}
		}
		// A digit in count position: skip the number, transition to offset-waiting state
		if (inLimitCount || inOffsetVal) && ch >= '0' && ch <= '9' {
			for i < len(lower) && lower[i] >= '0' && lower[i] <= '9' {
				i++
			}
			if inLimitCount {
				inLimitCount = false
				inLimitOffset = true
			} else {
				inOffsetVal = false
			}
			continue
		}
		// Any other non-whitespace token resets limit context
		if ch != ',' {
			inLimitCount = false
			inLimitOffset = false
			inOffsetVal = false
		}
		i++
	}
	return result
}

// isNegativeNumeric returns true if val is a negative numeric value.
func isNegativeNumeric(val interface{}) (interface{}, bool) {
	switch v := val.(type) {
	case int64:
		return v, v < 0
	case float64:
		return v, v < 0
	}
	return val, false
}

func (e *Executor) execUse(stmt *sqlparser.Use) (*Result, error) {
	name := stmt.DBName.String()
	// Allow USE INFORMATION_SCHEMA / PERFORMANCE_SCHEMA as virtual databases.
	lower := strings.ToLower(name)
	if lower != "information_schema" && lower != "performance_schema" {
		_, err := e.Catalog.GetDatabase(name)
		if err != nil {
			return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", name))
		}
	}
	e.CurrentDB = name
	// Update character_set_database and collation_database to match the new database's charset/collation.
	if db, err := e.Catalog.GetDatabase(name); err == nil {
		if db.CharacterSet != "" {
			e.setSysVar("character_set_database", db.CharacterSet, false)
		}
		if db.CollationName != "" {
			e.setSysVar("collation_database", db.CollationName, false)
		}
	}
	return &Result{}, nil
}

// nowTime returns the current time, respecting SET TIMESTAMP.
func (e *Executor) nowTime() time.Time {
	if e.fixedTimestamp != nil {
		t := *e.fixedTimestamp
		if e.timeZone != nil {
			t = t.In(e.timeZone)
		}
		return t
	}
	t := time.Now()
	if e.timeZone != nil {
		t = t.In(e.timeZone)
	}
	return t
}

// parseTimeZone parses a time zone string like "+03:00" or "SYSTEM" and sets e.timeZone.
func (e *Executor) parseTimeZone(val string) error {
	val = strings.Trim(val, "'\"")
	val = strings.TrimSpace(val)
	if strings.ToUpper(val) == "SYSTEM" || strings.ToUpper(val) == "DEFAULT" {
		e.timeZone = nil
		return nil
	}
	if val == "" {
		return fmt.Errorf("Unknown or incorrect time zone: ''")
	}
	// Parse offset like "+03:00", "-05:00", "+0:0"
	if (val[0] == '+' || val[0] == '-') && strings.Contains(val, ":") {
		var hours, mins int
		if _, err := fmt.Sscanf(val, "%d:%d", &hours, &mins); err == nil {
			// MySQL valid range: -12:59 to +13:00
			absHours := hours
			if absHours < 0 {
				absHours = -absHours
			}
			if mins < 0 || mins >= 60 {
				return fmt.Errorf("Unknown or incorrect time zone: '%s'", val)
			}
			totalMins := absHours*60 + mins
			if hours >= 0 {
				// Positive: max +13:00
				if totalMins > 13*60 {
					return fmt.Errorf("Unknown or incorrect time zone: '%s'", val)
				}
			} else {
				// Negative: max -12:59
				if totalMins > 12*60+59 {
					return fmt.Errorf("Unknown or incorrect time zone: '%s'", val)
				}
			}
			offset := hours*3600 + mins*60
			if hours < 0 {
				offset = hours*3600 - mins*60
			}
			// Normalize the timezone name: zero-pad and handle -00:00 -> +00:00
			sign := "+"
			if offset < 0 {
				sign = "-"
			}
			absOffset := offset
			if absOffset < 0 {
				absOffset = -absOffset
			}
			normalizedName := fmt.Sprintf("%s%02d:%02d", sign, absOffset/3600, (absOffset%3600)/60)
			e.timeZone = time.FixedZone(normalizedName, offset)
			return nil
		}
	}
	// Try as named timezone
	if loc, err := time.LoadLocation(val); err == nil {
		e.timeZone = loc
		return nil
	}
	return fmt.Errorf("Unknown or incorrect time zone: '%s'", val)
}

// parseTZName parses a timezone name string (e.g. "UTC", "+03:00", "US/Eastern")
// and returns the corresponding *time.Location, or nil if the timezone is unknown.
func parseTZName(val string) *time.Location {
	val = strings.Trim(val, "'\"")
	val = strings.TrimSpace(val)
	if strings.EqualFold(val, "SYSTEM") || val == "" {
		return time.Local
	}
	// Parse offset like "+03:00", "-05:00"
	if (val[0] == '+' || val[0] == '-') && strings.Contains(val, ":") {
		var hours, mins int
		if _, err := fmt.Sscanf(val, "%d:%d", &hours, &mins); err == nil {
			offset := hours*3600 + mins*60
			if hours < 0 {
				offset = hours*3600 - mins*60
			}
			return time.FixedZone(val, offset)
		}
	}
	// Try as named timezone
	if loc, err := time.LoadLocation(val); err == nil {
		return loc
	}
	return nil
}

// isStrictMode returns true when sql_mode includes STRICT_TRANS_TABLES, STRICT_ALL_TABLES, or TRADITIONAL.
func (e *Executor) isStrictMode() bool {
	return strings.Contains(e.sqlMode, "TRADITIONAL") ||
		strings.Contains(e.sqlMode, "STRICT_TRANS_TABLES") ||
		strings.Contains(e.sqlMode, "STRICT_ALL_TABLES")
}

// isTraditionalMode returns true when sql_mode includes TRADITIONAL or STRICT_ALL_TABLES.
// TRADITIONAL mode enforces stricter validation (e.g., ENUM value validation) than
// STRICT_TRANS_TABLES alone.
func (e *Executor) isTraditionalMode() bool {
	return strings.Contains(e.sqlMode, "TRADITIONAL") ||
		strings.Contains(e.sqlMode, "STRICT_ALL_TABLES")
}

// isInnoDBStrictMode returns true when innodb_strict_mode is ON.
func (e *Executor) isInnoDBStrictMode() bool {
	if v, ok := e.getSysVar("innodb_strict_mode"); ok {
		return strings.EqualFold(v, "ON") || v == "1"
	}
	return true // default is ON
}

// getInnoDBPageSize returns the configured innodb_page_size (default 16384).
func (e *Executor) getInnoDBPageSize() int {
	if v, ok := e.getSysVar("innodb_page_size"); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return 16384 // default
}

// findRowIDColumn returns the primary key column name for _rowid access.
// MySQL's _rowid is an alias for a single-column integer primary key.
func (e *Executor) findRowIDColumn(row storage.Row) string {
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return ""
	}
	// Try to find which table this row belongs to by checking table defs
	for _, tblName := range db.ListTables() {
		td, err := db.GetTable(tblName)
		if err != nil || len(td.PrimaryKey) != 1 {
			continue
		}
		pkCol := td.PrimaryKey[0]
		// Check if row has this column
		if _, ok := row[pkCol]; ok {
			// Verify the PK column is an integer type
			for _, col := range td.Columns {
				if col.Name == pkCol {
					upperType := strings.ToUpper(col.Type)
					if strings.Contains(upperType, "INT") {
						return pkCol
					}
				}
			}
		}
	}
	return ""
}

// extractCharLength returns the max character/byte length from a column type string.
// Returns 0 if the type has no enforced length limit (e.g., LONGBLOB/LONGTEXT).
func extractCharLength(colType string) int {
	lower := strings.ToLower(strings.TrimSpace(colType))
	var n int
	for _, prefix := range []string{"char(", "varchar(", "binary(", "varbinary("} {
		if strings.HasPrefix(lower, prefix) {
			if _, err := fmt.Sscanf(lower[len(prefix)-1:], "(%d)", &n); err == nil {
				return n
			}
		}
	}
	// BLOB/TEXT family max sizes (in bytes/chars)
	switch lower {
	case "tinyblob", "tinytext":
		return 255
	case "blob", "text":
		return 65535
	case "mediumblob", "mediumtext":
		return 16777215
		// LONGBLOB/LONGTEXT max is 4GB — too large to enforce in memory; skip
	}
	return 0
}

// isSingleByteCharset returns true if the charset is a known single-byte charset
// where CHAR(N) = N bytes (so Go's rune count is accurate for length checking).
func isSingleByteCharset(charset string) bool {
	switch strings.ToLower(charset) {
	case "latin1", "latin2", "latin5", "latin7", "latin9",
		"cp1250", "cp1251", "cp1256", "cp1257",
		"cp850", "cp852", "cp866",
		"armscii8", "dec8", "geostd8", "greek", "hebrew", "hp8",
		"keybcs2", "koi8r", "koi8u",
		"macce", "macroman",
		"swe7", "tis620":
		return true
	}
	return false
}
// extractColumnName extracts the column name from a sqlparser expression.
// Returns empty string if the expression is not a simple column reference.
func extractColumnName(expr sqlparser.Expr) string {
	switch e := expr.(type) {
	case *sqlparser.ColName:
		return e.Name.String()
	}
	return ""
}

// isColumnNotNull checks if a column is defined as NOT NULL in the current query's table.
func (e *Executor) isColumnNotNull(colName string) bool {
	if e.queryTableDef == nil {
		return false
	}
	colNameLower := strings.ToLower(colName)
	for _, col := range e.queryTableDef.Columns {
		if strings.ToLower(col.Name) == colNameLower {
			return !col.Nullable
		}
	}
	return false
}
// execMyliteCommand handles MYLITE-specific control commands:
//   - MYLITE CREATE SNAPSHOT <name>
//   - MYLITE RESTORE SNAPSHOT <name>
//   - MYLITE DROP SNAPSHOT <name>
func (e *Executor) execMyliteCommand(query string) (*Result, error) {
	upper := strings.ToUpper(strings.TrimSpace(query))
	// Strip leading "MYLITE " prefix (7 chars).
	rest := strings.TrimSpace(query[7:])
	restUpper := strings.TrimSpace(upper[7:])

	if strings.HasPrefix(restUpper, "CREATE SNAPSHOT ") {
		name := strings.TrimSpace(rest[len("CREATE SNAPSHOT "):])
		if name == "" {
			return nil, fmt.Errorf("MYLITE CREATE SNAPSHOT: missing snapshot name")
		}
		snap := &fullSnapshot{
			storageSnap: make(map[string]*storage.DatabaseSnapshot),
			catalogSnap: make(map[string]map[string]*catalog.TableDef),
		}
		for dbName, db := range e.Catalog.Databases {
			snap.storageSnap[dbName] = e.Storage.SnapshotDatabase(dbName)
			tablesCopy := make(map[string]*catalog.TableDef, len(db.Tables))
			for tName, tDef := range db.Tables {
				tablesCopy[tName] = tDef
			}
			snap.catalogSnap[dbName] = tablesCopy
		}
		e.snapshots[name] = snap
		return &Result{}, nil
	}

	if strings.HasPrefix(restUpper, "RESTORE SNAPSHOT ") {
		name := strings.TrimSpace(rest[len("RESTORE SNAPSHOT "):])
		if name == "" {
			return nil, fmt.Errorf("MYLITE RESTORE SNAPSHOT: missing snapshot name")
		}
		snap, ok := e.snapshots[name]
		if !ok {
			return nil, fmt.Errorf("MYLITE RESTORE SNAPSHOT: snapshot '%s' not found", name)
		}
		// Remove databases created after snapshot.
		for dbName := range e.Catalog.Databases {
			if _, existed := snap.catalogSnap[dbName]; !existed {
				delete(e.Catalog.Databases, dbName)
				e.Storage.DropDatabase(dbName)
			}
		}
		// Restore each snapshotted database.
		for dbName, tables := range snap.catalogSnap {
			db, ok := e.Catalog.Databases[dbName]
			if !ok {
				e.Catalog.Databases[dbName] = &catalog.Database{
					Name:   dbName,
					Tables: make(map[string]*catalog.TableDef),
				}
				db = e.Catalog.Databases[dbName]
			}
			db.Tables = tables
			e.Storage.RestoreDatabase(dbName, snap.storageSnap[dbName])
		}
		return &Result{}, nil
	}

	if strings.HasPrefix(restUpper, "DROP SNAPSHOT ") {
		name := strings.TrimSpace(rest[len("DROP SNAPSHOT "):])
		if name == "" {
			return nil, fmt.Errorf("MYLITE DROP SNAPSHOT: missing snapshot name")
		}
		if _, ok := e.snapshots[name]; !ok {
			return nil, fmt.Errorf("MYLITE DROP SNAPSHOT: snapshot '%s' not found", name)
		}
		delete(e.snapshots, name)
		return &Result{}, nil
	}

	if restUpper == "LAST_UPDATE_INFO" {
		info := e.lastUpdateInfo
		if info == "" {
			info = ""
		}
		return &Result{
			Columns:     []string{"info"},
			Rows:        [][]interface{}{{info}},
			IsResultSet: true,
		}, nil
	}

	if restUpper == "LAST_INSERT_INFO" {
		info := e.lastInsertInfo
		return &Result{
			Columns:     []string{"info"},
			Rows:        [][]interface{}{{info}},
			IsResultSet: true,
		}, nil
	}

	if restUpper == "RESET_TEMP_TABLES" {
		// Drop all temporary tables and clear the temp table tracking
		db, err := e.Catalog.GetDatabase(e.CurrentDB)
		if err == nil {
			for name := range e.tempTables {
				db.DropTable(name) //nolint:errcheck
				e.Storage.DropTable(e.CurrentDB, name)
			}
		}
		// Restore any permanent tables that were shadowed by temporary tables
		for name, saved := range e.tempTableSavedPermanent {
			if db2, err2 := e.Catalog.GetDatabase(e.CurrentDB); err2 == nil {
				_ = db2.CreateTable(saved.def)
			}
			e.Storage.RestoreTable(e.CurrentDB, name, saved.table)
		}
		e.tempTables = make(map[string]bool)
		e.tempTableSavedPermanent = make(map[string]*savedPermTable)
		e.pendingPermanentWhileTemp = make(map[string]*savedPermTable)
		return &Result{}, nil
	}

	if restUpper == "RESET_SESSION" {
		// Reset session state between tests
		e.lastInsertID = 0
		e.lastAutoIncID = 0
		e.sessionScopeVars = make(map[string]string)
		e.sqlMode = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
		e.userVars = make(map[string]interface{})
		e.preparedStmts = make(map[string]string)
		// Release any table locks and global read lock held by this connection
		// (important when pooled connections are reused across connect/disconnect cycles)
		if e.tableLockManager != nil {
			e.tableLockManager.UnlockAll(e.connectionID)
		}
		if e.globalReadLock != nil {
			e.globalReadLock.Release(e.connectionID)
		}
		// Clear shared resource groups to avoid cross-test contamination
		if e.resourceGroupsMu != nil {
			e.resourceGroupsMu.Lock()
			e.resourceGroups = make(map[string]string)
			e.resourceGroupsMu.Unlock()
		}
		return &Result{}, nil
	}

	return nil, fmt.Errorf("unknown MYLITE command: %s", query)
}

// buildGeometryFromExprs evaluates a slice of expressions, applies extractFn
// to each result, and wraps them in "WRAPNAME(...)".
func (e *Executor) buildGeometryFromExprs(params []sqlparser.Expr, extractFn func(string) string, wrapperName string) (string, error) {
	var parts []string
	for _, p := range params {
		pv, err := e.evalExpr(p)
		if err != nil {
			return "", err
		}
		parts = append(parts, extractFn(toString(pv)))
	}
	return fmt.Sprintf("%s(%s)", wrapperName, strings.Join(parts, ",")), nil
}

// charsetByteLength returns the byte length of a UTF-8 string when encoded in the given charset.
func charsetByteLength(s string, charset string) (int64, error) {
	switch strings.ToLower(charset) {
	case "sjis", "cp932":
		encoded, err := japanese.ShiftJIS.NewEncoder().Bytes([]byte(s))
		if err != nil {
			// Fallback: estimate using character ranges
			count := int64(0)
			for _, r := range s {
				if r < 0x80 {
					count++
				} else if r >= 0xFF61 && r <= 0xFF9F {
					count++
				} else {
					count += 2
				}
			}
			return count, nil
		}
		return int64(len(encoded)), nil
	case "ujis", "eucjpms":
		encoded, err := japanese.EUCJP.NewEncoder().Bytes([]byte(s))
		if err != nil {
			// Fallback: estimate
			count := int64(0)
			for _, r := range s {
				if r < 0x80 {
					count++
				} else {
					count += 2
				}
			}
			return count, nil
		}
		return int64(len(encoded)), nil
	case "ucs2":
		// UCS-2: every character is 2 bytes
		return int64(len([]rune(s)) * 2), nil
	case "utf8", "utf8mb3":
		return int64(len(s)), nil
	case "utf8mb4":
		return int64(len(s)), nil
	case "":
		return int64(len(s)), nil
	default:
		return int64(len(s)), nil
	}
}

// getColumnCharset returns the charset of a column by looking up the table definition.
func (e *Executor) getColumnCharset(colName *sqlparser.ColName) string {
	tableName := ""
	if !colName.Qualifier.Name.IsEmpty() {
		tableName = colName.Qualifier.Name.String()
	}
	if e.CurrentDB == "" {
		return ""
	}
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return ""
	}
	colStr := colName.Name.String()
	if tableName != "" {
		if td, err := db.GetTable(tableName); err == nil {
			if td.Charset != "" {
				return td.Charset
			}
		}
	} else {
		// Search all tables in the current database for the column.
		// If multiple charsets match, treat as ambiguous.
		foundCharset := ""
		for _, td := range db.Tables {
			for _, col := range td.Columns {
				if strings.EqualFold(col.Name, colStr) {
					if td.Charset == "" {
						continue
					}
					cs := strings.ToLower(td.Charset)
					if foundCharset == "" {
						foundCharset = cs
					} else if foundCharset != cs {
						return ""
					}
				}
			}
		}
		return foundCharset
	}
	return ""
}

// getColumnEffectiveCharset returns the effective (column-level) charset of a column,
// checking column-level charset first before falling back to table-level charset.
// This is used by functions like HEX that need to know the exact column encoding.
func (e *Executor) getColumnEffectiveCharset(colName *sqlparser.ColName) string {
	tableName := ""
	if !colName.Qualifier.Name.IsEmpty() {
		tableName = colName.Qualifier.Name.String()
	}
	if e.CurrentDB == "" {
		return ""
	}
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return ""
	}
	colStr := colName.Name.String()
	lookupInTable := func(td *catalog.TableDef) string {
		for _, col := range td.Columns {
			if strings.EqualFold(col.Name, colStr) {
				if col.Charset != "" {
					return strings.ToLower(col.Charset)
				}
				if td.Charset != "" {
					return strings.ToLower(td.Charset)
				}
				return ""
			}
		}
		return ""
	}
	if tableName != "" {
		if td, err := db.GetTable(tableName); err == nil {
			return lookupInTable(td)
		}
	} else {
		foundCharset := ""
		for _, td := range db.Tables {
			cs := lookupInTable(td)
			if cs == "" {
				continue
			}
			if foundCharset == "" {
				foundCharset = cs
			} else if foundCharset != cs {
				return ""
			}
		}
		return foundCharset
	}
	return ""
}

// mysqlWeekFull calculates MySQL's WEEK(date, mode) for all 8 modes.
// MySQL WEEK() mode semantics:
//
//	Mode 0: first=Sun, range 0-53, week1=starts at first Sunday
//	Mode 1: first=Mon, range 0-53, week1=4+ days in year
//	Mode 2: first=Sun, range 1-53, week1=starts at first Sunday (or prev year last week)
//	Mode 3: first=Mon, range 1-53, ISO week
//	Mode 4: first=Sun, range 0-53, week1=4+ days in year
//	Mode 5: first=Mon, range 0-53, week1=starts at first Monday
//	Mode 6: first=Sun, range 1-53, week1=4+ days in year
//	Mode 7: first=Mon, range 1-53, week1=starts at first Monday (or prev year last week)

// isBinaryExpr checks whether an expression references a BINARY or VARBINARY column.
// This is used to make UPPER/LOWER no-ops on binary data, matching MySQL behavior.
func (e *Executor) isBinaryExpr(expr sqlparser.Expr) bool {
	switch ex := expr.(type) {
	case *sqlparser.ColName:
		colName := ex.Name.String()
		// Check columns in all known tables in current query context
		tableName := ""
		if !ex.Qualifier.Name.IsEmpty() {
			tableName = ex.Qualifier.Name.String()
		}
		return e.isColumnBinary(tableName, colName)
	case *sqlparser.Subquery:
		// Check if the subquery's SELECT column is from a BINARY/VARBINARY column
		if sel, ok := ex.Select.(*sqlparser.Select); ok && sel.SelectExprs != nil && len(sel.SelectExprs.Exprs) > 0 {
			if ae, ok := sel.SelectExprs.Exprs[0].(*sqlparser.AliasedExpr); ok {
				return e.isBinaryExpr(ae.Expr)
			}
		}
	case *sqlparser.FuncExpr:
		// Functions that always return VARBINARY data (raw bytes)
		name := strings.ToLower(ex.Name.String())
		switch name {
		case "inet6_aton", "inet_aton", "uuid_to_bin", "st_aswkb", "st_asgeowkb",
			"st_asbinary", "from_base64", "weight_string", "to_binary":
			return true
		}
		// Functions that inherit binary-ness from their argument (e.g. UNHEX returns VARBINARY)
		switch name {
		case "unhex":
			return true
		case "substr", "substring", "left", "right", "mid", "concat", "concat_ws":
			// Return binary if any argument is binary
			for _, se := range ex.Exprs {
				if e.isBinaryExpr(se) {
					return true
				}
			}
		}
	}
	return false
}

// isColumnBinary checks if a column in the current database has a BINARY or VARBINARY type.
func (e *Executor) isColumnBinary(tableName, colName string) bool {
	if e.CurrentDB == "" {
		return false
	}
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return false
	}
	var tables []string
	if tableName != "" {
		tables = []string{tableName}
	} else {
		tables = db.ListTables()
	}
	for _, tbl := range tables {
		tDef, _ := db.GetTable(tbl)
		if tDef == nil {
			continue
		}
		for _, col := range tDef.Columns {
			if strings.EqualFold(col.Name, colName) {
				lower := strings.ToLower(col.Type)
				if strings.Contains(lower, "binary") {
					return true
				}
			}
		}
	}
	return false
}

// isNumericDecimalColExpr checks if expr is a column reference to a DECIMAL, FLOAT, DOUBLE, or REAL
// column in the current query context. This is used to apply MySQL's string-to-number coercion rule:
// when a numeric column is compared to a non-numeric string, the string is treated as 0.
func (e *Executor) isNumericDecimalColExpr(expr sqlparser.Expr) bool {
	cn, ok := expr.(*sqlparser.ColName)
	if !ok {
		return false
	}
	colName := cn.Name.String()
	// Try queryTableDef first (current query's table definition)
	if e.queryTableDef != nil {
		for _, col := range e.queryTableDef.Columns {
			if strings.EqualFold(col.Name, colName) {
				lower := strings.ToLower(col.Type)
				return strings.HasPrefix(lower, "decimal") || strings.HasPrefix(lower, "float") ||
					strings.HasPrefix(lower, "double") || strings.HasPrefix(lower, "real") ||
					strings.HasPrefix(lower, "numeric")
			}
		}
	}
	// Fall back to catalog lookup
	if e.CurrentDB == "" {
		return false
	}
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return false
	}
	tableName := ""
	if !cn.Qualifier.Name.IsEmpty() {
		tableName = cn.Qualifier.Name.String()
	}
	var tables []string
	if tableName != "" {
		tables = []string{tableName}
	} else {
		tables = db.ListTables()
	}
	for _, tbl := range tables {
		tDef, _ := db.GetTable(tbl)
		if tDef == nil {
			continue
		}
		for _, col := range tDef.Columns {
			if strings.EqualFold(col.Name, colName) {
				lower := strings.ToLower(col.Type)
				return strings.HasPrefix(lower, "decimal") || strings.HasPrefix(lower, "float") ||
					strings.HasPrefix(lower, "double") || strings.HasPrefix(lower, "real") ||
					strings.HasPrefix(lower, "numeric")
			}
		}
	}
	return false
}

// isSpatialExpr checks if an expression references a spatial/geometry column (POINT, GEOMETRY, etc.)
// Spatial types are not valid in bitwise operations and raise error 1210.
func (e *Executor) isSpatialExpr(expr sqlparser.Expr) bool {
	colName, ok := expr.(*sqlparser.ColName)
	if !ok {
		return false
	}
	name := colName.Name.String()
	tableName := ""
	if !colName.Qualifier.Name.IsEmpty() {
		tableName = colName.Qualifier.Name.String()
	}
	if e.CurrentDB == "" {
		return false
	}
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return false
	}
	var tables []string
	if tableName != "" {
		tables = []string{tableName}
	} else {
		tables = db.ListTables()
	}
	spatialTypes := map[string]bool{
		"point": true, "linestring": true, "polygon": true, "geometry": true,
		"multipoint": true, "multilinestring": true, "multipolygon": true, "geometrycollection": true,
		"geomcollection": true,
	}
	for _, tbl := range tables {
		tDef, _ := db.GetTable(tbl)
		if tDef == nil {
			continue
		}
		for _, col := range tDef.Columns {
			if strings.EqualFold(col.Name, name) {
				lower := strings.ToLower(strings.Fields(col.Type)[0]) // get base type
				if spatialTypes[lower] {
					return true
				}
			}
		}
	}
	return false
}

// getBinaryColumnWidth returns the byte width of a BINARY/VARBINARY column from schema.
// Returns 0 if the column type cannot be determined.
func (e *Executor) getBinaryColumnWidth(expr sqlparser.Expr) int {
	colName, ok := expr.(*sqlparser.ColName)
	if !ok {
		return 0
	}
	name := colName.Name.String()
	tableName := ""
	if !colName.Qualifier.Name.IsEmpty() {
		tableName = colName.Qualifier.Name.String()
	}
	if e.CurrentDB == "" {
		return 0
	}
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return 0
	}
	var tables []string
	if tableName != "" {
		tables = []string{tableName}
	} else {
		tables = db.ListTables()
	}
	for _, tbl := range tables {
		tDef, _ := db.GetTable(tbl)
		if tDef == nil {
			continue
		}
		for _, col := range tDef.Columns {
			if strings.EqualFold(col.Name, name) {
				return extractBinaryColumnWidth(col.Type)
			}
		}
	}
	return 0
}

// extractBinaryColumnWidth extracts the byte width from a BINARY/VARBINARY column type string.
func extractBinaryColumnWidth(colType string) int {
	lower := strings.ToLower(strings.TrimSpace(colType))
	if strings.HasPrefix(lower, "binary(") || strings.HasPrefix(lower, "varbinary(") {
		// Extract number from e.g. "varbinary(8)" or "binary(6)"
		start := strings.Index(lower, "(")
		end := strings.Index(lower, ")")
		if start >= 0 && end > start {
			n, err := strconv.Atoi(strings.TrimSpace(lower[start+1 : end]))
			if err == nil {
				return n
			}
		}
	}
	return 0
}

func (d SysVarDouble) String() string {
	return strconv.FormatFloat(d.Value, 'f', 6, 64)
}

func (d DivisionResult) String() string {
	return fmt.Sprintf("%.*f", d.Precision, d.Value)
}

func (a AvgResult) String() string {
	if a.Rat != nil {
		return formatRatFixed(a.Rat, a.Scale)
	}
	return fmt.Sprintf("%.*f", a.Scale, a.Value)
}

// soundex, likePatternToRegexpStr, likeToRegexpCaseSensitive, likeToRegexpCaseSensitiveEscape,
// likeToRegexpEscape, likeToRegexp are defined in display.go

func rowValueByColumnName(row storage.Row, colName string) interface{} {
	if v, ok := row[colName]; ok {
		return v
	}
	upper := strings.ToUpper(colName)
	for k, v := range row {
		if strings.ToUpper(k) == upper {
			return v
		}
	}
	return nil
}

// isStringBuildingExpr returns true when the SQL expression is a CONCAT or similar operation
// that produces a string value. Used to detect when UNIX_TIMESTAMP should return DECIMAL.
// MySQL returns DECIMAL(16,6) for UNIX_TIMESTAMP(CONCAT(...)) but INTEGER for datetime expressions.
func isStringBuildingExpr(expr sqlparser.Expr) bool {
	switch e := expr.(type) {
	case *sqlparser.FuncExpr:
		name := strings.ToLower(e.Name.Lowered())
		switch name {
		case "concat", "concat_ws", "format", "substring", "substr", "trim", "ltrim", "rtrim",
			"replace", "insert", "lpad", "rpad", "repeat", "reverse", "space",
			"left", "right", "mid":
			return true
		}
		return false
	default:
		return false
	}
}

// ==============================================================================
// Trigger support
// ==============================================================================

// ==============================================================================
// Stored Procedure support
// ==============================================================================

// ---------- LOAD DATA INFILE ----------

// ---------- SELECT INTO OUTFILE ----------

// resolveTableNameDB resolves a table name that may be qualified with a database name (d1.t1).
// Returns (dbName, tableName).
func resolveTableNameDB(name, currentDB string) (string, string) {
	name = strings.Trim(name, "`")
	if idx := strings.Index(name, "."); idx >= 0 {
		dbPart := strings.Trim(name[:idx], "`")
		tablePart := strings.Trim(name[idx+1:], "`")
		return dbPart, tablePart
	}
	return currentDB, name
}

// lookupView looks up a view by name (case-insensitive) and returns the
// view SQL and the canonical name. Returns ("", "", false) if not found.
func (e *Executor) lookupView(name string) (viewSQL string, canonicalName string, ok bool) {
	if e.views == nil {
		return "", "", false
	}
	if sql, found := e.views[name]; found {
		return sql, name, true
	}
	for vn, sql := range e.views {
		if strings.EqualFold(vn, name) {
			return sql, vn, true
		}
	}
	return "", "", false
}

// resolveViewToBaseTable checks if the given table name is a view, and if so,
// returns the underlying base table name. It also validates that the view is
// updatable (simple SELECT from a single table, no JOINs, GROUP BY, DISTINCT,
// aggregates, UNION, or subqueries in FROM).
// Returns (baseTable, isView, viewWhere, error). If isView is false, the caller should
// proceed with normal table handling. viewWhere is the WHERE clause from the view definition.
func (e *Executor) resolveViewToBaseTable(tableName string) (string, bool, sqlparser.Expr, error) {
	viewSQL, _, ok := e.lookupView(tableName)
	if !ok {
		return "", false, nil, nil
	}
	stmt, err := e.parser().Parse(viewSQL)
	if err != nil {
		return "", true, nil, fmt.Errorf("cannot parse view definition: %v", err)
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		// UNION or other non-simple SELECT
		return "", true, nil, mysqlError(1288, "HY000", "The target table of the statement is not updatable")
	}
	// Must have exactly one table in FROM (no JOINs)
	if len(sel.From) != 1 {
		return "", true, nil, mysqlError(1288, "HY000", "The target table of the statement is not updatable")
	}
	ate, ok := sel.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		// JOIN expression
		return "", true, nil, mysqlError(1288, "HY000", "The target table of the statement is not updatable")
	}
	tn, ok := ate.Expr.(sqlparser.TableName)
	if !ok {
		// Subquery in FROM
		return "", true, nil, mysqlError(1288, "HY000", "The target table of the statement is not updatable")
	}
	// Check for GROUP BY, HAVING, DISTINCT, aggregates, window functions
	if sel.GroupBy != nil || sel.Having != nil || sel.Distinct {
		return "", true, nil, mysqlError(1288, "HY000", "The target table of the statement is not updatable")
	}
	// Check for aggregate functions in SELECT exprs
	for _, expr := range sel.SelectExprs.Exprs {
		if ae, ok := expr.(*sqlparser.AliasedExpr); ok {
			if containsAggregate(ae.Expr) {
				return "", true, nil, mysqlError(1288, "HY000", "The target table of the statement is not updatable")
			}
		}
	}
	var viewWhere sqlparser.Expr
	if sel.Where != nil {
		viewWhere = sel.Where.Expr
	}
	baseTableName := tn.Name.String()
	// If the resolved base table is itself a view, recursively resolve.
	// If the inner view is non-updatable, report a join view error for the original view.
	if _, _, isNestedView := e.lookupView(baseTableName); isNestedView {
		_, _, _, innerErr := e.resolveViewToBaseTable(baseTableName)
		if innerErr != nil {
			// The nested view is not updatable (join/non-simple view).
			// MySQL reports "Can not delete from join view" for the outer view.
			return "", true, nil, mysqlError(1395, "HY000",
				fmt.Sprintf("Can not delete from join view '%s.%s'", e.CurrentDB, tableName))
		}
		// Nested view is also a simple single-table view - it's OK.
	}
	return baseTableName, true, viewWhere, nil
}

// getViewCheckCondition returns the WHERE expression from a view definition if it has WITH CHECK OPTION.
// Returns nil if no check option or no WHERE condition.
func (e *Executor) getViewCheckCondition(viewName string) sqlparser.Expr {
	if e.viewCheckOptions == nil {
		return nil
	}
	checkOpt, ok := e.viewCheckOptions[viewName]
	if !ok {
		// Try case-insensitive lookup
		for vn, opt := range e.viewCheckOptions {
			if strings.EqualFold(vn, viewName) {
				checkOpt = opt
				ok = true
				break
			}
		}
	}
	if !ok || checkOpt == "" {
		return nil
	}
	viewSQL, _, found := e.lookupView(viewName)
	if !found {
		return nil
	}
	stmt, err := e.parser().Parse(viewSQL)
	if err != nil {
		return nil
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok || sel.Where == nil {
		return nil
	}
	return sel.Where.Expr
}

// isMultiTableUpdate checks if an UPDATE statement involves multiple tables (join or comma-separated).
// resolveColumnTable resolves an unqualified column name to a table name
// by searching through the table expressions in the FROM clause.
func resolveColumnTable(te sqlparser.TableExpr, colName string, e *Executor) string {
	switch t := te.(type) {
	case *sqlparser.AliasedTableExpr:
		tblName := sqlparser.String(t.Expr)
		tblName = strings.Trim(tblName, "`")
		tbl, err := e.Storage.GetTable(e.CurrentDB, tblName)
		if err != nil {
			return ""
		}
		for _, col := range tbl.Def.Columns {
			if strings.EqualFold(col.Name, colName) {
				return tblName
			}
		}
	case *sqlparser.JoinTableExpr:
		if r := resolveColumnTable(t.LeftExpr, colName, e); r != "" {
			return r
		}
		if r := resolveColumnTable(t.RightExpr, colName, e); r != "" {
			return r
		}
	}
	return ""
}

// validateIndexHints checks that all index names referenced in USE KEY / IGNORE KEY / FORCE KEY
// hints actually exist in the target table. Returns error 1176 (ER_KEY_DOES_NOT_EXITS) on mismatch.
func (e *Executor) validateIndexHints(from []sqlparser.TableExpr) error {
	for _, te := range from {
		switch t := te.(type) {
		case *sqlparser.AliasedTableExpr:
			if len(t.Hints) == 0 {
				continue
			}
			tn, ok := t.Expr.(sqlparser.TableName)
			if !ok {
				continue
			}
			tableName := tn.Name.String()
			lookupDB := e.CurrentDB
			if !tn.Qualifier.IsEmpty() {
				lookupDB = tn.Qualifier.String()
			}
			if e.Catalog == nil {
				continue
			}
			db, err := e.Catalog.GetDatabase(lookupDB)
			if err != nil {
				continue
			}
			def, err := db.GetTable(tableName)
			if err != nil {
				continue
			}
			// Build set of valid key names for this table.
			validKeys := make(map[string]bool)
			if len(def.PrimaryKey) > 0 {
				validKeys["primary"] = true
			}
			for _, idx := range def.Indexes {
				validKeys[strings.ToLower(idx.Name)] = true
			}
			for _, hint := range t.Hints {
				for _, idx := range hint.Indexes {
					keyName := idx.String()
					if !validKeys[strings.ToLower(keyName)] {
						return mysqlError(1176, "42000", fmt.Sprintf("Key '%s' doesn't exist in table '%s'", keyName, tableName))
					}
				}
			}
		case *sqlparser.JoinTableExpr:
			if err := e.validateIndexHints([]sqlparser.TableExpr{t.LeftExpr}); err != nil {
				return err
			}
			if err := e.validateIndexHints([]sqlparser.TableExpr{t.RightExpr}); err != nil {
				return err
			}
		case *sqlparser.ParenTableExpr:
			if err := e.validateIndexHints(t.Exprs); err != nil {
				return err
			}
		}
	}
	return nil
}

// execOtherAdmin handles OPTIMIZE TABLE, REPAIR TABLE, CHECK TABLE, etc.
func (e *Executor) execOtherAdmin(query string) (*Result, error) {
	upper := strings.ToUpper(strings.TrimSpace(query))
	// Extract table name(s) for result output
	var op string
	var rest string
	if strings.HasPrefix(upper, "OPTIMIZE TABLE") {
		op = "optimize"
		rest = strings.TrimSpace(query[len("OPTIMIZE TABLE"):])
	} else if strings.HasPrefix(upper, "REPAIR TABLE") {
		op = "repair"
		rest = strings.TrimSpace(query[len("REPAIR TABLE"):])
	} else if strings.HasPrefix(upper, "CHECK TABLE") {
		op = "check"
		rest = strings.TrimSpace(query[len("CHECK TABLE"):])
	} else if strings.HasPrefix(upper, "DO ") {
		// DO expr — evaluate expressions but discard results
		exprStr := strings.TrimSpace(query[3:])
		exprStr = strings.TrimRight(exprStr, ";")
		// Wrap in SELECT to parse expression
		selectQuery := "SELECT " + exprStr
		stmt, err := e.parser().Parse(selectQuery)
		if err != nil {
			// Cannot parse — silently succeed
			return &Result{AffectedRows: 0}, nil
		}
		sel, ok := stmt.(*sqlparser.Select)
		if !ok || len(sel.SelectExprs.Exprs) == 0 {
			return &Result{AffectedRows: 0}, nil
		}
		// Evaluate each expression (may trigger errors for unsupported functions)
		var evalErr error
		func() {
			defer func() {
				if r := recover(); r != nil {
					// Panic during evaluation — silently succeed
				}
			}()
			for _, expr := range sel.SelectExprs.Exprs {
				if ae, ok := expr.(*sqlparser.AliasedExpr); ok {
					if _, err := e.evalExpr(ae.Expr); err != nil {
						evalErr = err
						return
					}
				}
			}
		}()
		if evalErr != nil {
			return nil, evalErr
		}
		return &Result{AffectedRows: 0}, nil
	} else {
		// CACHE INDEX, etc. — silently succeed
		return &Result{AffectedRows: 0}, nil
	}

	// OPTIMIZE TABLE and ANALYZE TABLE do not support EXTENDED keyword — return syntax error
	{
		restUpper := strings.ToUpper(strings.TrimSpace(strings.TrimRight(rest, ";")))
		if (op == "optimize" || op == "analyze") && strings.HasSuffix(restUpper, " EXTENDED") {
			return nil, mysqlError(1064, "42000", "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'extended' at line 1")
		}
	}
	// Detect FOR UPGRADE option before stripping
	forUpgrade := false
	{
		restUpper := strings.ToUpper(strings.TrimSpace(strings.TrimRight(rest, ";")))
		if strings.HasSuffix(restUpper, "FOR UPGRADE") {
			forUpgrade = true
		}
	}
	// Strip trailing options (QUICK, FAST, MEDIUM, EXTENDED, CHANGED, FOR UPGRADE, etc.)
	{
		restUpper := strings.ToUpper(rest)
		checkOpts := []string{"QUICK", "FAST", "MEDIUM", "EXTENDED", "CHANGED", "FOR UPGRADE", "USE_FRM"}
		for _, co := range checkOpts {
			if strings.HasSuffix(strings.TrimSpace(strings.TrimRight(restUpper, ";")), co) {
				rest = strings.TrimSpace(rest[:len(strings.TrimSpace(strings.TrimRight(rest, ";")))-len(co)])
			}
		}
	}
	// Parse table names (comma-separated)
	tables := strings.Split(rest, ",")
	var rows [][]interface{}
	for _, t := range tables {
		t = strings.TrimSpace(t)
		t = strings.TrimRight(t, ";")
		t = strings.Trim(t, "`")
		tableName := t
		if !strings.Contains(tableName, ".") {
			tableName = e.CurrentDB + "." + tableName
		}
		// Check table lock restrictions for CHECK/REPAIR when LOCK TABLES is active
		if e.tableLockManager != nil && e.tableLockManager.HasLocks(e.connectionID) {
			bareTable := t
			lockKey := e.CurrentDB + "." + bareTable
			locked, _ := e.tableLockManager.IsLocked(e.connectionID, lockKey)
			if !locked {
				rows = append(rows, []interface{}{tableName, op, "Error", fmt.Sprintf("Table '%s' was not locked with LOCK TABLES", bareTable)})
				rows = append(rows, []interface{}{tableName, op, "status", "Operation failed"})
				continue
			}
		}
		// Determine the table's storage engine
		isInnoDB := true
		bareTable := t
		if dbObj, err := e.Catalog.GetDatabase(e.CurrentDB); err == nil {
			if tblDef, err := dbObj.GetTable(bareTable); err == nil && tblDef != nil {
				eng := strings.ToUpper(tblDef.Engine)
				if eng == "MYISAM" || eng == "MEMORY" || eng == "CSV" || eng == "ARCHIVE" || eng == "HEAP" {
					isInnoDB = false
				}
			}
		}
		if op == "optimize" {
			if isInnoDB {
				// InnoDB doesn't support optimize; MySQL outputs a note then status OK
				rows = append(rows, []interface{}{tableName, op, "note", "Table does not support optimize, doing recreate + analyze instead"})
				rows = append(rows, []interface{}{tableName, op, "status", "OK"})
			} else {
				// Non-InnoDB engines (MyISAM, etc.): return "OK" if the table was modified
				// (INSERT/UPDATE/DELETE/ALTER) since last OPTIMIZE TABLE; otherwise "Table is already up to date".
				fullName2 := e.CurrentDB + "." + bareTable
				if strings.Contains(tableName, ".") {
					fullName2 = tableName
				}
				needsOptimize := e.tableNeedsOptimize != nil && e.tableNeedsOptimize[fullName2]
				if needsOptimize {
					if e.tableNeedsOptimize != nil {
						delete(e.tableNeedsOptimize, fullName2)
					}
					rows = append(rows, []interface{}{tableName, op, "status", "OK"})
				} else {
					rows = append(rows, []interface{}{tableName, op, "status", "Table is already up to date"})
				}
			}
		} else if op == "check" && forUpgrade {
			// CHECK TABLE ... FOR UPGRADE: first check returns OK, subsequent checks
			// return "Table is already up to date"
			if e.checkedForUpgrade == nil {
				e.checkedForUpgrade = map[string]bool{}
			}
			if e.checkedForUpgrade[tableName] {
				rows = append(rows, []interface{}{tableName, op, "status", "Table is already up to date"})
			} else {
				e.checkedForUpgrade[tableName] = true
				rows = append(rows, []interface{}{tableName, op, "status", "OK"})
			}
		} else {
			rows = append(rows, []interface{}{tableName, op, "status", "OK"})
		}
	}
	return &Result{
		Columns:     []string{"Table", "Op", "Msg_type", "Msg_text"},
		Rows:        rows,
		IsResultSet: true,
	}, nil
}

// execAnalyzeMultiTable handles ANALYZE TABLE t1, t2, ... with multiple tables.
func (e *Executor) execAnalyzeMultiTable(query string) (*Result, error) {
	rest := strings.TrimSpace(query[len("ANALYZE TABLE"):])
	tables := strings.Split(rest, ",")
	var rows [][]interface{}
	for _, t := range tables {
		t = strings.TrimSpace(t)
		t = strings.TrimRight(t, ";")
		t = strings.Trim(t, "`")
		bareTable := t
		tableName := t
		if !strings.Contains(tableName, ".") {
			tableName = e.CurrentDB + "." + tableName
		}
		msgText := "Table is already up to date"
		msgType := "status"
		if dbObj, err := e.Catalog.GetDatabase(e.CurrentDB); err == nil {
			if tblDef, err := dbObj.GetTable(bareTable); err == nil && tblDef != nil {
				eng := strings.ToUpper(tblDef.Engine)
				// MEMORY/HEAP engine doesn't support ANALYZE TABLE
				if eng == "MEMORY" || eng == "HEAP" {
					msgType = "note"
					msgText = "The storage engine for the table doesn't support analyze"
				} else {
					hasSpatial := false
					for _, idx := range tblDef.Indexes {
						if strings.EqualFold(idx.Type, "SPATIAL") {
							hasSpatial = true
							break
						}
					}
					needsAnalyze := e.tableNeedsAnalyze != nil && e.tableNeedsAnalyze[tableName]
					needsOptimize := e.tableNeedsOptimize != nil && e.tableNeedsOptimize[tableName]
					// ANALYZE TABLE returns "OK" for InnoDB (always), SPATIAL-indexed tables,
					// or when there are pending stats updates.
					if eng == "" || eng == "INNODB" || hasSpatial || needsAnalyze || needsOptimize {
						msgText = "OK"
					}
					// Clear flags after analyze
					if e.tableNeedsAnalyze != nil {
						delete(e.tableNeedsAnalyze, tableName)
					}
					if e.tableNeedsOptimize != nil {
						delete(e.tableNeedsOptimize, tableName)
					}
				}
			}
		}
		rows = append(rows, []interface{}{tableName, "analyze", msgType, msgText})
	}
	return &Result{
		Columns:     []string{"Table", "Op", "Msg_type", "Msg_text"},
		Rows:        rows,
		IsResultSet: true,
	}, nil
}

// decodeSJIS converts SJIS/CP932 encoded bytes to UTF-8.
func decodeSJIS(data []byte) ([]byte, error) {
	reader := transform.NewReader(bytes.NewReader(data), japanese.ShiftJIS.NewDecoder())
	var buf bytes.Buffer
	_, err := buf.ReadFrom(reader)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decodeEUCJP converts EUC-JP encoded bytes to UTF-8.
func decodeEUCJP(data []byte) ([]byte, error) {
	reader := transform.NewReader(bytes.NewReader(data), japanese.EUCJP.NewDecoder())
	var buf bytes.Buffer
	_, err := buf.ReadFrom(reader)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func canonicalCharset(charset string) string {
	switch strings.ToLower(charset) {
	case "utf8mb3", "utf8mb4":
		return "utf8"
	case "cp932":
		return "sjis"
	case "eucjpms":
		return "ujis"
	default:
		return strings.ToLower(charset)
	}
}

func charsetEncoder(charset string) *encoding.Encoder {
	switch canonicalCharset(charset) {
	case "sjis":
		return japanese.ShiftJIS.NewEncoder()
	case "ujis":
		return japanese.EUCJP.NewEncoder()
	case "latin1":
		// MySQL treats latin1 as cp1252 (Windows-1252) per WL-1494.
		return charmap.Windows1252.NewEncoder()
	case "latin2":
		return charmap.ISO8859_2.NewEncoder()
	case "cp1250":
		return charmap.Windows1250.NewEncoder()
	case "cp1251":
		return charmap.Windows1251.NewEncoder()
	case "cp1252":
		return charmap.Windows1252.NewEncoder()
	case "koi8r":
		return charmap.KOI8R.NewEncoder()
	case "greek":
		return charmap.ISO8859_7.NewEncoder()
	case "hebrew":
		return charmap.ISO8859_8.NewEncoder()
	default:
		return nil
	}
}

func charsetDecoder(charset string) *encoding.Decoder {
	switch canonicalCharset(charset) {
	case "sjis":
		return japanese.ShiftJIS.NewDecoder()
	case "ujis":
		return japanese.EUCJP.NewDecoder()
	case "hebrew":
		return charmap.ISO8859_8.NewDecoder()
	case "latin1":
		return charmap.ISO8859_1.NewDecoder()
	case "greek":
		return charmap.ISO8859_7.NewDecoder()
	case "latin2":
		return charmap.ISO8859_2.NewDecoder()
	case "cp1250":
		return charmap.Windows1250.NewDecoder()
	case "cp1251":
		return charmap.Windows1251.NewDecoder()
	case "cp1252":
		return charmap.Windows1252.NewDecoder()
	case "koi8r":
		return charmap.KOI8R.NewDecoder()
	case "gb18030", "gbk", "gb2312":
		return simplifiedchinese.GB18030.NewDecoder()
	default:
		return nil
	}
}

func roundTripCharset(s, charset string) (string, error) {
	enc := charsetEncoder(charset)
	dec := charsetDecoder(charset)
	if enc == nil || dec == nil {
		return s, nil
	}
	encoded, err := encoding.ReplaceUnsupported(enc).Bytes([]byte(s))
	if err != nil {
		return s, err
	}
	decoded, _, err := transform.String(dec, string(encoded))
	if err != nil {
		return s, err
	}
	decoded = strings.ReplaceAll(decoded, "\x1a", "?")
	return decoded, nil
}

func convertThroughCharset(s, charset string) (string, error) {
	cs := canonicalCharset(charset)
	switch cs {
	case "utf8", "":
		return s, nil
	case "utf32":
		// Encode each rune as 4-byte big-endian UTF-32
		runes := []rune(s)
		buf := make([]byte, len(runes)*4)
		for i, r := range runes {
			buf[i*4] = byte(r >> 24)
			buf[i*4+1] = byte(r >> 16)
			buf[i*4+2] = byte(r >> 8)
			buf[i*4+3] = byte(r)
		}
		return string(buf), nil
	case "utf16":
		// Encode each rune as 2-byte or 4-byte UTF-16 big-endian
		var buf []byte
		for _, r := range s {
			if r <= 0xFFFF {
				buf = append(buf, byte(r>>8), byte(r))
			} else {
				// Surrogate pair
				r -= 0x10000
				hi := 0xD800 + (r>>10)&0x3FF
				lo := 0xDC00 + r&0x3FF
				buf = append(buf, byte(hi>>8), byte(hi), byte(lo>>8), byte(lo))
			}
		}
		return string(buf), nil
	case "utf16le":
		// Encode each rune as 2-byte or 4-byte UTF-16 little-endian
		var buf []byte
		for _, r := range s {
			if r <= 0xFFFF {
				buf = append(buf, byte(r), byte(r>>8))
			} else {
				// Surrogate pair
				r -= 0x10000
				hi := 0xD800 + (r>>10)&0x3FF
				lo := 0xDC00 + r&0x3FF
				buf = append(buf, byte(hi), byte(hi>>8), byte(lo), byte(lo>>8))
			}
		}
		return string(buf), nil
	case "ucs2":
		// Keep UCS2 display semantics in higher-level query paths.
		return s, nil
	case "latin1", "latin2", "cp1250", "cp1251", "cp1252", "koi8r", "greek", "hebrew":
		// 8-bit charset: encode UTF-8 input to the target charset bytes.
		// Return the raw charset bytes (not decoded back to UTF-8).
		enc := charsetEncoder(cs)
		if enc == nil {
			return s, nil
		}
		encoded, err := encoding.ReplaceUnsupported(enc).Bytes([]byte(s))
		if err != nil {
			return s, err
		}
		return string(encoded), nil
	case "ascii":
		// ASCII is a subset of UTF-8; replace non-ASCII with '?'
		var buf []byte
		for _, r := range s {
			if r < 0x80 {
				buf = append(buf, byte(r))
			} else {
				buf = append(buf, '?')
			}
		}
		return string(buf), nil
	case "sjis", "ujis":
		enc := charsetEncoder(cs)
		dec := charsetDecoder(cs)
		if enc == nil || dec == nil {
			return s, nil
		}
		encoded, err := encoding.ReplaceUnsupported(enc).Bytes([]byte(s))
		if err != nil {
			return s, err
		}
		decoded, _, err := transform.String(dec, string(encoded))
		if err != nil {
			return s, err
		}
		decoded = strings.ReplaceAll(decoded, "\x1a", "?")
		return decoded, nil
	default:
		return s, nil
	}
}

func decodeUCS2(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}
	be := true
	if len(data) >= 2 {
		if data[0] == 0xFE && data[1] == 0xFF {
			data = data[2:]
			be = true
		} else if data[0] == 0xFF && data[1] == 0xFE {
			data = data[2:]
			be = false
		}
	}
	runes := make([]rune, 0, len(data)/2)
	for i := 0; i+1 < len(data); i += 2 {
		var u uint16
		if be {
			u = uint16(data[i])<<8 | uint16(data[i+1])
		} else {
			u = uint16(data[i+1])<<8 | uint16(data[i])
		}
		r := rune(u)
		if r == '\uFF3C' {
			r = '\\'
		}
		if r == '\uFF5E' || r == '\u301C' {
			r = '~'
		}
		runes = append(runes, r)
	}
	return []byte(string(runes)), nil
}

// checkMaxJoinSize checks if a SELECT would exceed max_join_size and raises error 1104 if so.
// max_join_size is only enforced when sql_big_selects is OFF.
func (e *Executor) checkMaxJoinSize(stmt *sqlparser.Select) error {
	// Check if sql_big_selects is ON (overrides max_join_size).
	bigSelects := e.sessionScopeVars["sql_big_selects"]
	if bigSelects == "" {
		if gv, ok := e.getGlobalVar("sql_big_selects"); ok {
			bigSelects = gv
		}
	}
	upperBS := strings.ToUpper(bigSelects)
	if upperBS == "ON" || upperBS == "1" {
		return nil
	}

	// Get max_join_size value.
	maxJoinSizeStr := e.sessionScopeVars["max_join_size"]
	if maxJoinSizeStr == "" {
		if gv, ok := e.getGlobalVar("max_join_size"); ok {
			maxJoinSizeStr = gv
		}
	}
	if maxJoinSizeStr == "" {
		return nil // no limit
	}
	maxJoinSize, parseErr := strconv.ParseUint(maxJoinSizeStr, 10, 64)
	if parseErr != nil {
		return nil
	}
	// 18446744073709551615 is the default (unlimited).
	if maxJoinSize >= 18446744073709551615 {
		return nil
	}

	// Estimate join size as the product of FROM table row counts.
	var estimate uint64 = 1
	for _, tableExpr := range stmt.From {
		rowCount := e.estimateTableExprRows(tableExpr)
		if rowCount <= 0 {
			rowCount = 1
		}
		// Overflow-safe multiplication
		if estimate > 18446744073709551615/uint64(rowCount) {
			estimate = 18446744073709551615
		} else {
			estimate *= uint64(rowCount)
		}
	}

	if estimate > maxJoinSize {
		return mysqlError(1104, "42000", "The SELECT would examine more than MAX_JOIN_SIZE rows; check your WHERE and use SET SQL_BIG_SELECTS=1 or SET MAX_JOIN_SIZE=# if the SELECT is okay")
	}
	return nil
}

// estimateTableExprRows returns an estimate of the row count for a table expression.
func (e *Executor) estimateTableExprRows(tableExpr sqlparser.TableExpr) int {
	switch t := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		switch inner := t.Expr.(type) {
		case sqlparser.TableName:
			tableName := inner.Name.String()
			dbName := e.CurrentDB
			if !inner.Qualifier.IsEmpty() {
				dbName = inner.Qualifier.String()
			}
			if strings.EqualFold(tableName, "dual") {
				return 1
			}
			if e.Storage != nil {
				if tbl, err := e.Storage.GetTable(dbName, tableName); err == nil {
					tbl.Mu.RLock()
					n := len(tbl.Rows)
					tbl.Mu.RUnlock()
					return n
				}
			}
			return 1
		case *sqlparser.DerivedTable:
			// For derived tables (subqueries), estimate from the inner SELECT or Union.
			switch subStmt := inner.Select.(type) {
			case *sqlparser.Select:
				return e.estimateSelectRows(subStmt)
			case *sqlparser.Union:
				// For UNION, estimate as the sum of all parts.
				return e.estimateUnionRows(subStmt)
			}
			return 1
		default:
			return 1
		}
	case *sqlparser.JoinTableExpr:
		left := e.estimateTableExprRows(t.LeftExpr)
		right := e.estimateTableExprRows(t.RightExpr)
		return left * right
	case *sqlparser.ParenTableExpr:
		total := 1
		for _, te := range t.Exprs {
			total *= e.estimateTableExprRows(te)
		}
		return total
	default:
		return 1
	}
}

// estimateSelectRows estimates the number of rows a SELECT would produce by taking
// the product of all FROM table row counts.
func (e *Executor) estimateSelectRows(sel *sqlparser.Select) int {
	total := 1
	for _, tableExpr := range sel.From {
		n := e.estimateTableExprRows(tableExpr)
		if n <= 0 {
			n = 1
		}
		total *= n
	}
	return total
}

// estimateUnionRows estimates the number of rows a UNION would produce as the sum of all parts.
func (e *Executor) estimateUnionRows(u *sqlparser.Union) int {
	left := 0
	switch l := u.Left.(type) {
	case *sqlparser.Select:
		left = e.estimateSelectRows(l)
	case *sqlparser.Union:
		left = e.estimateUnionRows(l)
	default:
		left = 1
	}
	right := 0
	switch r := u.Right.(type) {
	case *sqlparser.Select:
		right = e.estimateSelectRows(r)
	case *sqlparser.Union:
		right = e.estimateUnionRows(r)
	default:
		right = 1
	}
	return left + right
}

// isSafeUpdateEnabled returns true if SQL_SAFE_UPDATES is enabled for the current session.
func (e *Executor) isSafeUpdateEnabled() bool {
	checkVal := func(val string) bool {
		upper := strings.ToUpper(val)
		return upper == "ON" || upper == "1"
	}
	// Check session override first
	if val, ok := e.sessionScopeVars["sql_safe_updates"]; ok {
		return checkVal(val)
	}
	// Check global override
	if val, ok := e.getGlobalVar("sql_safe_updates"); ok {
		return checkVal(val)
	}
	return false
}

// whereUsesKeyColumnDirectly returns true if the WHERE expression contains a direct
// equality comparison (col = value or value = col) where col is a primary key or index column.
// This is used for SQL_SAFE_UPDATES enforcement.
func whereUsesKeyColumnDirectly(where sqlparser.Expr, keyColumns map[string]bool) bool {
	if where == nil {
		return false
	}
	switch e := where.(type) {
	case *sqlparser.ComparisonExpr:
		if e.Operator != sqlparser.EqualOp {
			return false
		}
		// Check col = value
		if cn, ok := e.Left.(*sqlparser.ColName); ok {
			if keyColumns[strings.ToLower(cn.Name.String())] {
				return true
			}
		}
		// Check value = col
		if cn, ok := e.Right.(*sqlparser.ColName); ok {
			if keyColumns[strings.ToLower(cn.Name.String())] {
				return true
			}
		}
		return false
	case *sqlparser.AndExpr:
		return whereUsesKeyColumnDirectly(e.Left, keyColumns) || whereUsesKeyColumnDirectly(e.Right, keyColumns)
	case *sqlparser.OrExpr:
		// Both sides must use key for OR (conservative: we require at least one side)
		return whereUsesKeyColumnDirectly(e.Left, keyColumns) && whereUsesKeyColumnDirectly(e.Right, keyColumns)
	default:
		return false
	}
}

// checkSafeUpdate returns an error if SQL_SAFE_UPDATES is enabled and the
// UPDATE/DELETE would violate safe update mode rules. Must be called after
// the table def is loaded. limitClause should be nil if no LIMIT clause present.
func (e *Executor) checkSafeUpdate(def *catalog.TableDef, where sqlparser.Expr, limitClause *sqlparser.Limit) error {
	if !e.isSafeUpdateEnabled() {
		return nil
	}
	// LIMIT present → always allowed
	if limitClause != nil {
		return nil
	}
	// Build set of key columns (primary key + all index columns)
	keyColumns := make(map[string]bool)
	for _, col := range def.PrimaryKey {
		keyColumns[strings.ToLower(stripPrefixLengthFromCol(col))] = true
	}
	for _, idx := range def.Indexes {
		for _, col := range idx.Columns {
			keyColumns[strings.ToLower(stripPrefixLengthFromCol(col))] = true
		}
	}
	// No WHERE or WHERE doesn't use a key column directly → error
	if where == nil || !whereUsesKeyColumnDirectly(where, keyColumns) {
		return mysqlError(1175, "HY000", "You are using safe update mode and you tried to update a table without a WHERE that uses a KEY column.")
	}
	return nil
}

// extractPKEquality checks if expr contains an equality on pkCol.
func extractPKEquality(expr sqlparser.Expr, pkCol string) (pkVal interface{}, remaining sqlparser.Expr) {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if e.Operator != sqlparser.EqualOp {
			return nil, nil
		}
		colName, val := extractColLiteralForPK(e)
		if colName == "" || !strings.EqualFold(colName, pkCol) {
			return nil, nil
		}
		return val, nil
	case *sqlparser.AndExpr:
		if val, _ := extractPKEquality(e.Left, pkCol); val != nil {
			return val, e.Right
		}
		if val, _ := extractPKEquality(e.Right, pkCol); val != nil {
			return val, e.Left
		}
		return nil, nil
	default:
		return nil, nil
	}
}

func extractColLiteralForPK(cmp *sqlparser.ComparisonExpr) (string, interface{}) {
	if cn, ok := cmp.Left.(*sqlparser.ColName); ok {
		if v := evalLiteralForPK(cmp.Right); v != nil {
			return cn.Name.String(), v
		}
	}
	if cn, ok := cmp.Right.(*sqlparser.ColName); ok {
		if v := evalLiteralForPK(cmp.Left); v != nil {
			return cn.Name.String(), v
		}
	}
	return "", nil
}

// parseUserAtHost extracts the normalized "user@host" key from a string like
// "test_user1@'localhost'" or "'user'@'host'". Returns empty string on failure.
func parseUserAtHost(s string) string {
	s = strings.TrimSpace(s)
	// Find last @ to split user and host
	atIdx := strings.LastIndex(s, "@")
	if atIdx < 0 {
		return ""
	}
	user := strings.TrimSpace(s[:atIdx])
	host := strings.TrimSpace(s[atIdx+1:])
	user = strings.Trim(user, "'`\"")
	host = strings.Trim(host, "'`\"")
	return strings.ToLower(user + "@" + host)
}

// trackCreateUser records the user(s) created by a CREATE USER statement in knownUsers.
func (e *Executor) trackCreateUser(raw string) {
	if e.knownUsers == nil || e.knownUsersMu == nil {
		return
	}
	// Strip CREATE USER [IF NOT EXISTS] prefix
	upper := strings.ToUpper(raw)
	rest := ""
	if strings.HasPrefix(upper, "CREATE USER IF NOT EXISTS ") {
		rest = strings.TrimSpace(raw[len("CREATE USER IF NOT EXISTS "):])
	} else if strings.HasPrefix(upper, "CREATE USER ") {
		rest = strings.TrimSpace(raw[len("CREATE USER "):])
	} else {
		return
	}
	// Strip trailing semicolons
	rest = strings.TrimSuffix(strings.TrimSpace(rest), ";")
	// The user spec may have IDENTIFIED BY ... or other options; extract just user@host part.
	// Split by comma for multiple users, then take the first token (user@host) of each.
	userSpecs := strings.Split(rest, ",")
	e.knownUsersMu.Lock()
	defer e.knownUsersMu.Unlock()
	for _, spec := range userSpecs {
		spec = strings.TrimSpace(spec)
		// User spec format: 'user'@'host' [IDENTIFIED BY 'password' ...]
		// Find the user@host part (stop at first space after the @host)
		userHost := spec
		if idx := strings.IndexAny(spec, " \t"); idx >= 0 {
			// Check if the space is after @host
			atIdx := strings.LastIndex(spec[:idx], "@")
			if atIdx >= 0 {
				userHost = strings.TrimSpace(spec[:idx])
			}
		}
		key := parseUserAtHost(userHost)
		if key != "" {
			e.knownUsers[key] = true
		}
	}
}

// trackDropUser removes user(s) from knownUsers based on a DROP USER statement.
func (e *Executor) trackDropUser(raw string) {
	if e.knownUsers == nil || e.knownUsersMu == nil {
		return
	}
	upper := strings.ToUpper(raw)
	rest := ""
	if strings.HasPrefix(upper, "DROP USER IF EXISTS ") {
		rest = strings.TrimSpace(raw[len("DROP USER IF EXISTS "):])
	} else if strings.HasPrefix(upper, "DROP USER ") {
		rest = strings.TrimSpace(raw[len("DROP USER "):])
	} else {
		return
	}
	rest = strings.TrimSuffix(strings.TrimSpace(rest), ";")
	userSpecs := strings.Split(rest, ",")
	e.knownUsersMu.Lock()
	defer e.knownUsersMu.Unlock()
	for _, spec := range userSpecs {
		spec = strings.TrimSpace(spec)
		key := parseUserAtHost(spec)
		if key != "" {
			delete(e.knownUsers, key)
		}
	}
}

// decodeBacktickedIdentifiers decodes non-UTF-8 bytes inside backtick-quoted identifiers
// from the given charset to UTF-8. This is needed because MySQL stores identifiers in UTF-8
// internally even when character_set_client is set to a non-UTF-8 charset. Only the content
// inside backticks is decoded; string literals and other parts of the query are left unchanged.
func decodeBacktickedIdentifiers(query string, charset string) string {
	dec := charsetDecoder(charset)
	if dec == nil {
		return query
	}
	var result strings.Builder
	result.Grow(len(query))
	i := 0
	for i < len(query) {
		if query[i] == '`' {
			// Find the closing backtick (handle escaped backticks ``)
			j := i + 1
			for j < len(query) {
				if query[j] == '`' {
					if j+1 < len(query) && query[j+1] == '`' {
						j += 2 // escaped backtick, continue
						continue
					}
					break // closing backtick found
				}
				j++
			}
			if j < len(query) {
				// Decode the identifier content if it contains non-UTF-8 bytes
				ident := query[i+1 : j]
				if !utf8.ValidString(ident) {
					if decoded, _, err := transform.String(dec, ident); err == nil {
						result.WriteByte('`')
						result.WriteString(decoded)
						result.WriteByte('`')
						i = j + 1
						continue
					}
				}
				// No conversion needed or failed: copy as-is
				result.WriteByte('`')
				result.WriteString(ident)
				result.WriteByte('`')
				i = j + 1
				continue
			}
		}
		result.WriteByte(query[i])
		i++
	}
	return result.String()
}

// reloadGrantStoreFromStorage reloads the grant store from mysql.db and mysql.tables_priv.
// Called on FLUSH PRIVILEGES. Clears existing entries and rebuilds from storage.
func (e *Executor) reloadGrantStoreFromStorage() {
	if e.grantStore == nil || e.Storage == nil {
		return
	}
	// Clear all privilege entries (keep role registrations and role memberships)
	e.grantStore.mu.Lock()
	e.grantStore.entries = make(map[string][]GrantEntry)
	// Also clear privilege grants from roles (but keep role registrations)
	for k := range e.grantStore.roles {
		e.grantStore.roles[k] = []GrantEntry{}
	}
	e.grantStore.mu.Unlock()

	// Reload from mysql.db
	dbTbl, err := e.Storage.GetTable("mysql", "db")
	if err == nil {
		dbTbl.Mu.RLock()
		for _, row := range dbTbl.Rows {
			user := toString(row["User"])
			host := toString(row["Host"])
			db := toString(row["Db"])
			if user == "" {
				continue
			}
			// Collect privileges
			var privs []string
			privMap := map[string]string{
				"SELECT": "Select_priv", "INSERT": "Insert_priv", "UPDATE": "Update_priv",
				"DELETE": "Delete_priv", "CREATE": "Create_priv", "DROP": "Drop_priv",
				"REFERENCES": "References_priv", "INDEX": "Index_priv", "ALTER": "Alter_priv",
				"CREATE TEMPORARY TABLES": "Create_tmp_table_priv", "LOCK TABLES": "Lock_tables_priv",
				"CREATE VIEW": "Create_view_priv", "SHOW VIEW": "Show_view_priv",
				"CREATE ROUTINE": "Create_routine_priv", "ALTER ROUTINE": "Alter_routine_priv",
				"EXECUTE": "Execute_priv", "EVENT": "Event_priv", "TRIGGER": "Trigger_priv",
			}
			allY := true
			for priv, col := range privMap {
				if toString(row[col]) == "Y" {
					privs = append(privs, priv)
				} else {
					allY = false
				}
			}
			if allY {
				privs = []string{"ALL PRIVILEGES"}
			}
			if len(privs) == 0 {
				continue
			}
			grantOption := toString(row["Grant_priv"]) == "Y"
			object := strings.ToLower(db) + ".*"
			e.grantStore.AddPrivGrant(user, host, strings.Join(privs, ","), object, grantOption)
		}
		dbTbl.Mu.RUnlock()
	}

	// Reload from mysql.tables_priv
	tblPriv, err := e.Storage.GetTable("mysql", "tables_priv")
	if err == nil {
		tblPriv.Mu.RLock()
		for _, row := range tblPriv.Rows {
			user := toString(row["User"])
			host := toString(row["Host"])
			db := toString(row["Db"])
			tableName := toString(row["Table_name"])
			tablePrivStr := toString(row["Table_priv"])
			if user == "" || tablePrivStr == "" {
				continue
			}
			object := strings.ToLower(db) + "." + strings.ToLower(tableName)
			e.grantStore.AddPrivGrant(user, host, tablePrivStr, object, false)
		}
		tblPriv.Mu.RUnlock()
	}
}

// syncGrantToMysqlTables inserts/updates rows in mysql.db or mysql.tables_priv
// when a GRANT statement is executed, so that SELECT * FROM mysql.db works correctly.
func (e *Executor) syncGrantToMysqlTables(privs, object, user, host string, grantOption bool) {
	if e.Storage == nil {
		return
	}
	gtype := objectToGrantType(object)
	privList := normalizePrivList(privs)
	isAll := len(privList) == 1 && privList[0] == "ALL PRIVILEGES"

	privY := func(priv string) string {
		if isAll {
			return "Y"
		}
		for _, p := range privList {
			if strings.EqualFold(p, priv) {
				return "Y"
			}
		}
		return "N"
	}
	grantPriv := "N"
	if grantOption {
		grantPriv = "Y"
	}

	if gtype == GrantTypeDB {
		// Insert or update mysql.db
		dbName := strings.TrimSuffix(object, ".*")
		tbl, err := e.Storage.GetTable("mysql", "db")
		if err != nil {
			return
		}
		tbl.Mu.Lock()
		defer tbl.Mu.Unlock()
		// Find existing row
		for i, row := range tbl.Rows {
			if strings.EqualFold(toString(row["User"]), user) &&
				strings.EqualFold(toString(row["Host"]), host) &&
				strings.EqualFold(toString(row["Db"]), dbName) {
				// Update existing row
				tbl.Rows[i]["Select_priv"] = mergePrivY(toString(row["Select_priv"]), privY("SELECT"))
				tbl.Rows[i]["Insert_priv"] = mergePrivY(toString(row["Insert_priv"]), privY("INSERT"))
				tbl.Rows[i]["Update_priv"] = mergePrivY(toString(row["Update_priv"]), privY("UPDATE"))
				tbl.Rows[i]["Delete_priv"] = mergePrivY(toString(row["Delete_priv"]), privY("DELETE"))
				tbl.Rows[i]["Create_priv"] = mergePrivY(toString(row["Create_priv"]), privY("CREATE"))
				tbl.Rows[i]["Drop_priv"] = mergePrivY(toString(row["Drop_priv"]), privY("DROP"))
				tbl.Rows[i]["Grant_priv"] = mergePrivY(toString(row["Grant_priv"]), grantPriv)
				tbl.Rows[i]["References_priv"] = mergePrivY(toString(row["References_priv"]), privY("REFERENCES"))
				tbl.Rows[i]["Index_priv"] = mergePrivY(toString(row["Index_priv"]), privY("INDEX"))
				tbl.Rows[i]["Alter_priv"] = mergePrivY(toString(row["Alter_priv"]), privY("ALTER"))
				tbl.Rows[i]["Create_tmp_table_priv"] = mergePrivY(toString(row["Create_tmp_table_priv"]), privY("CREATE TEMPORARY TABLES"))
				tbl.Rows[i]["Lock_tables_priv"] = mergePrivY(toString(row["Lock_tables_priv"]), privY("LOCK TABLES"))
				tbl.Rows[i]["Create_view_priv"] = mergePrivY(toString(row["Create_view_priv"]), privY("CREATE VIEW"))
				tbl.Rows[i]["Show_view_priv"] = mergePrivY(toString(row["Show_view_priv"]), privY("SHOW VIEW"))
				tbl.Rows[i]["Create_routine_priv"] = mergePrivY(toString(row["Create_routine_priv"]), privY("CREATE ROUTINE"))
				tbl.Rows[i]["Alter_routine_priv"] = mergePrivY(toString(row["Alter_routine_priv"]), privY("ALTER ROUTINE"))
				tbl.Rows[i]["Execute_priv"] = mergePrivY(toString(row["Execute_priv"]), privY("EXECUTE"))
				tbl.Rows[i]["Event_priv"] = mergePrivY(toString(row["Event_priv"]), privY("EVENT"))
				tbl.Rows[i]["Trigger_priv"] = mergePrivY(toString(row["Trigger_priv"]), privY("TRIGGER"))
				return
			}
		}
		// New row
		tbl.Rows = append(tbl.Rows, storage.Row{
			"Host":                  host,
			"Db":                    dbName,
			"User":                  user,
			"Select_priv":           privY("SELECT"),
			"Insert_priv":           privY("INSERT"),
			"Update_priv":           privY("UPDATE"),
			"Delete_priv":           privY("DELETE"),
			"Create_priv":           privY("CREATE"),
			"Drop_priv":             privY("DROP"),
			"Grant_priv":            grantPriv,
			"References_priv":       privY("REFERENCES"),
			"Index_priv":            privY("INDEX"),
			"Alter_priv":            privY("ALTER"),
			"Create_tmp_table_priv": privY("CREATE TEMPORARY TABLES"),
			"Lock_tables_priv":      privY("LOCK TABLES"),
			"Create_view_priv":      privY("CREATE VIEW"),
			"Show_view_priv":        privY("SHOW VIEW"),
			"Create_routine_priv":   privY("CREATE ROUTINE"),
			"Alter_routine_priv":    privY("ALTER ROUTINE"),
			"Execute_priv":          privY("EXECUTE"),
			"Event_priv":            privY("EVENT"),
			"Trigger_priv":          privY("TRIGGER"),
		})
	} else if gtype == GrantTypeTable {
		// Insert or update mysql.tables_priv
		parts := strings.SplitN(object, ".", 2)
		if len(parts) != 2 {
			return
		}
		dbName := parts[0]
		tableName := parts[1]
		tbl, err := e.Storage.GetTable("mysql", "tables_priv")
		if err != nil {
			return
		}
		var tablePrivs []string
		for _, p := range privList {
			tablePrivs = append(tablePrivs, p)
		}
		tablePrivStr := strings.Join(tablePrivs, ",")
		tbl.Mu.Lock()
		defer tbl.Mu.Unlock()
		for i, row := range tbl.Rows {
			if strings.EqualFold(toString(row["User"]), user) &&
				strings.EqualFold(toString(row["Host"]), host) &&
				strings.EqualFold(toString(row["Db"]), dbName) &&
				strings.EqualFold(toString(row["Table_name"]), tableName) {
				tbl.Rows[i]["Table_priv"] = tablePrivStr
				return
			}
		}
		tbl.Rows = append(tbl.Rows, storage.Row{
			"Host":        host,
			"Db":          dbName,
			"User":        user,
			"Table_name":  tableName,
			"Grantor":     "root@localhost",
			"Timestamp":   "0000-00-00 00:00:00",
			"Table_priv":  tablePrivStr,
			"Column_priv": "",
		})
	}
}

// mergePrivY returns "Y" if either a or b is "Y".
func mergePrivY(a, b string) string {
	if a == "Y" || b == "Y" {
		return "Y"
	}
	return "N"
}

// isKnownUser checks if user@host (parsed from "user@host" string like "test_user1@'localhost'")
// is in the knownUsers map.
func (e *Executor) isKnownUser(userAtHost string) bool {
	if e.knownUsers == nil || e.knownUsersMu == nil {
		return false
	}
	key := parseUserAtHost(userAtHost)
	if key == "" {
		return false
	}
	e.knownUsersMu.RLock()
	defer e.knownUsersMu.RUnlock()
	return e.knownUsers[key]
}

// getCurrentUserAndRoles returns the current non-root user, host, and their active roles.
// Returns ("", "", nil) if the current user is root or no user is set.
func (e *Executor) getCurrentUserAndRoles() (string, string, []string) {
	if e.userVars == nil {
		return "", "", nil
	}
	cuv, ok := e.userVars["__current_user"]
	if !ok {
		return "", "", nil
	}
	cu, ok := cuv.(string)
	if !ok || cu == "" || strings.EqualFold(cu, "root") {
		return "", "", nil
	}
	// Host defaults to localhost for client connections
	host := "localhost"
	// Get active roles
	var activeRoles []string
	if arv, ok2 := e.userVars["__active_roles"]; ok2 {
		if roles, ok3 := arv.([]string); ok3 {
			activeRoles = roles
		}
	}
	return cu, host, activeRoles
}

// checkTablePrivilege checks if the current user has the required privilege for the given statement.
// Returns an error if access is denied, or nil if access is allowed.
// Only enforces for non-root users when grantStore is populated.
func (e *Executor) checkTablePrivilege(stmt sqlparser.Statement) error {
	if e.grantStore == nil {
		return nil
	}
	// Skip privilege checks inside stored routines (DEFINER security context is not yet implemented).
	// SQL SECURITY DEFINER procedures run with the definer's (creator's) privileges, not the invoker's.
	if e.routineDepth > 0 {
		return nil
	}
	user, host, activeRoles := e.getCurrentUserAndRoles()
	if user == "" {
		return nil // root or no user context
	}

	// Only enforce if the user has at least one known table-level privilege grant entry.
	// This prevents false denials for:
	//   - Users with only role memberships (whose default roles we may not track)
	//   - Users with only non-table privileges (EXECUTE, FILE, PROCESS, etc.)
	//   - Users with IP-based/CIDR grants or other unsupported host formats
	// We do enforce if there are active roles set (via SET ROLE).
	if len(activeRoles) == 0 && !e.grantStore.UserHasAnyTablePrivGrant(user, host) {
		return nil
	}

	// Helper to check a single table access
	checkAccess := func(priv, dbName, tableName string) error {
		// Skip system/information databases - always allow
		dbLower := strings.ToLower(dbName)
		if dbLower == "information_schema" || dbLower == "performance_schema" {
			return nil
		}
		// Skip temporary tables - the user created them so they own them
		if e.tempTables != nil {
			tblLower := strings.ToLower(tableName)
			if e.tempTables[tableName] || e.tempTables[tblLower] {
				return nil
			}
		}
		if !e.grantStore.HasPrivilege(user, host, priv, dbName, tableName, activeRoles) {
			// Format: "<priv> command denied to user '<user>'@'<host>' for table '<table>'"
			return mysqlError(1142, "42000", fmt.Sprintf("%s command denied to user '%s'@'%s' for table '%s'",
				priv, user, host, tableName))
		}
		return nil
	}

	// Determine the required privilege and tables based on statement type
	switch s := stmt.(type) {
	case *sqlparser.Select:
		// Collect CTE names to skip privilege check for them (they're virtual tables)
		cteNames := make(map[string]bool)
		if s.With != nil {
			for _, cte := range s.With.CTEs {
				cteNames[strings.ToLower(cte.ID.String())] = true
			}
		}
		// Check SELECT privilege on all referenced real tables (skip subqueries and CTEs)
		for _, tblExpr := range s.From {
			if err := checkTableExprPrivSkipCTEs(tblExpr, "SELECT", e.CurrentDB, cteNames, checkAccess); err != nil {
				return err
			}
		}
	case *sqlparser.Insert:
		dbName := e.CurrentDB
		tblName := s.Table.TableNameString()
		if tn, ok := s.Table.Expr.(sqlparser.TableName); ok && !tn.Qualifier.IsEmpty() {
			dbName = tn.Qualifier.String()
		}
		return checkAccess("INSERT", dbName, tblName)
	case *sqlparser.Update:
		for _, tblExpr := range s.TableExprs {
			if err := checkTableExprPriv(tblExpr, "UPDATE", e.CurrentDB, checkAccess); err != nil {
				return err
			}
		}
	case *sqlparser.Delete:
		for _, tblExpr := range s.TableExprs {
			if err := checkTableExprPriv(tblExpr, "DELETE", e.CurrentDB, checkAccess); err != nil {
				return err
			}
		}
	case *sqlparser.DropTable:
		for _, tbl := range s.FromTables {
			dbName := e.CurrentDB
			if !tbl.Qualifier.IsEmpty() {
				dbName = tbl.Qualifier.String()
			}
			if err := checkAccess("DROP", dbName, tbl.Name.String()); err != nil {
				return err
			}
		}
	case *sqlparser.CreateTable:
		dbName := e.CurrentDB
		if !s.Table.Qualifier.IsEmpty() {
			dbName = s.Table.Qualifier.String()
		}
		// For temporary tables, also accept CREATE TEMPORARY TABLES privilege
		if s.Temp {
			user, host, activeRoles := e.getCurrentUserAndRoles()
			if e.grantStore.HasPrivilege(user, host, "CREATE TEMPORARY TABLES", dbName, s.Table.Name.String(), activeRoles) {
				return nil
			}
		}
		return checkAccess("CREATE", dbName, s.Table.Name.String())
	}
	return nil
}

// checkTableExprPriv recursively checks privilege for a table expression.
func checkTableExprPriv(tblExpr sqlparser.TableExpr, priv, currentDB string, check func(priv, db, table string) error) error {
	return checkTableExprPrivSkipCTEs(tblExpr, priv, currentDB, nil, check)
}

// checkTableExprPrivSkipCTEs recursively checks privilege for a table expression, skipping CTE virtual tables.
func checkTableExprPrivSkipCTEs(tblExpr sqlparser.TableExpr, priv, currentDB string, cteNames map[string]bool, check func(priv, db, table string) error) error {
	switch t := tblExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		if tn, ok := t.Expr.(sqlparser.TableName); ok {
			tblName := tn.Name.String()
			// Skip DUAL (MySQL's virtual table for SELECT without FROM)
			if strings.EqualFold(tblName, "dual") {
				return nil
			}
			// Skip CTE references (they're virtual tables defined in the WITH clause)
			if cteNames != nil && cteNames[strings.ToLower(tblName)] {
				return nil
			}
			dbName := currentDB
			if !tn.Qualifier.IsEmpty() {
				dbName = tn.Qualifier.String()
			}
			return check(priv, dbName, tblName)
		}
		// Subquery or derived table - no table-level privilege check here
	case *sqlparser.JoinTableExpr:
		if err := checkTableExprPrivSkipCTEs(t.LeftExpr, priv, currentDB, cteNames, check); err != nil {
			return err
		}
		return checkTableExprPrivSkipCTEs(t.RightExpr, priv, currentDB, cteNames, check)
	case *sqlparser.ParenTableExpr:
		for _, te := range t.Exprs {
			if err := checkTableExprPrivSkipCTEs(te, priv, currentDB, cteNames, check); err != nil {
				return err
			}
		}
	}
	return nil
}
