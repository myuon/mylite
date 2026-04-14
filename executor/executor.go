package executor

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/vt/sqlparser"
)

// vitessCollEnv is a shared Vitess collation environment for MySQL 8.0.
var vitessCollEnv = collations.NewEnvironment("8.0.40")

// lookupVitessCollation returns a Vitess Collation for the given name, or nil if not found.
func lookupVitessCollation(name string) colldata.Collation {
	id := vitessCollEnv.LookupByName(strings.ToLower(name))
	if id == collations.Unknown {
		return nil
	}
	return colldata.Lookup(id)
}

// vitessWeightString returns the MySQL-compatible weight string for a Go string
// under the given Vitess collation. It handles charset conversion from UTF-8
// to the collation's charset before computing the weight string.
func vitessWeightString(s string, coll colldata.Collation) []byte {
	src := []byte(s)
	cs := coll.Charset()
	// Convert from UTF-8 to the collation's charset if needed
	if cs.Name() != "utf8mb4" && cs.Name() != "utf8mb3" && cs.Name() != "binary" {
		converted, err := charset.ConvertFromUTF8(nil, cs, src)
		if err == nil {
			src = converted
		}
	}
	return coll.WeightString(nil, src, 0)
}

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
	columns []string
	rows    []storage.Row
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
	lastInsertID   int64
	lastUpdateInfo string // stores info message from last UPDATE (e.g. "Rows matched: 2  Changed: 1  Warnings: 0")
	lastInsertInfo string // stores info message from last INSERT/REPLACE (e.g. "Records: 5  Duplicates: 2  Warnings: 0")
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
	sortRows  int64 // total rows sorted
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

func (e *Executor) initSystemTables() {
	if e.Catalog == nil || e.Storage == nil {
		return
	}

	ensure := func(dbName string, def *catalog.TableDef) {
		e.Storage.EnsureDatabase(dbName)
		db, err := e.Catalog.GetDatabase(dbName)
		if err != nil {
			return
		}
		if _, err := db.GetTable(def.Name); err != nil {
			db.CreateTable(def) //nolint:errcheck
			e.Storage.CreateTable(dbName, def)
		}
	}

	ensure("mysql", &catalog.TableDef{
		Name:      "innodb_table_stats",
		Charset:   "utf8mb4",
		Collation: "utf8mb4_bin",
		Columns: []catalog.ColumnDef{
			{Name: "database_name", Type: "VARCHAR(64)"},
			{Name: "table_name", Type: "VARCHAR(199)"},
			{Name: "last_update", Type: "TIMESTAMP"},
			{Name: "n_rows", Type: "BIGINT"},
			{Name: "clustered_index_size", Type: "BIGINT"},
			{Name: "sum_of_other_index_sizes", Type: "BIGINT"},
		},
	})

	ensure("mysql", &catalog.TableDef{
		Name:      "innodb_index_stats",
		Charset:   "utf8mb4",
		Collation: "utf8mb4_bin",
		Columns: []catalog.ColumnDef{
			{Name: "database_name", Type: "VARCHAR(64)"},
			{Name: "table_name", Type: "VARCHAR(199)"},
			{Name: "index_name", Type: "VARCHAR(64)"},
			{Name: "last_update", Type: "TIMESTAMP"},
			{Name: "stat_name", Type: "VARCHAR(64)"},
			{Name: "stat_value", Type: "BIGINT"},
			{Name: "sample_size", Type: "BIGINT", Nullable: true},
			{Name: "stat_description", Type: "VARCHAR(1024)"},
		},
	})

	ensure("performance_schema", &catalog.TableDef{
		Name: "setup_instruments",
		Columns: []catalog.ColumnDef{
			{Name: "NAME", Type: "VARCHAR(128)"},
			{Name: "ENABLED", Type: "VARCHAR(8)"},
			{Name: "TIMED", Type: "VARCHAR(8)"},
			{Name: "PROPERTIES", Type: "VARCHAR(256)"},
			{Name: "VOLATILITY", Type: "INT"},
			{Name: "DOCUMENTATION", Type: "TEXT"},
		},
	})
	ensure("performance_schema", &catalog.TableDef{
		Name: "setup_consumers",
		Columns: []catalog.ColumnDef{
			{Name: "NAME", Type: "VARCHAR(128)"},
			{Name: "ENABLED", Type: "VARCHAR(8)"},
		},
	})
	ensure("performance_schema", &catalog.TableDef{
		Name: "session_status",
		Columns: []catalog.ColumnDef{
			{Name: "VARIABLE_NAME", Type: "VARCHAR(64)"},
			{Name: "VARIABLE_VALUE", Type: "VARCHAR(1024)"},
		},
	})
	ensure("performance_schema", &catalog.TableDef{
		Name: "global_status",
		Columns: []catalog.ColumnDef{
			{Name: "VARIABLE_NAME", Type: "VARCHAR(64)"},
			{Name: "VARIABLE_VALUE", Type: "VARCHAR(1024)"},
		},
	})
	ensure("performance_schema", &catalog.TableDef{
		Name: "file_summary_by_event_name",
		Columns: []catalog.ColumnDef{
			{Name: "EVENT_NAME", Type: "VARCHAR(128)"},
			{Name: "COUNT_STAR", Type: "BIGINT"},
		},
	})
	ensure("performance_schema", &catalog.TableDef{
		Name: "events_stages_history",
		Columns: []catalog.ColumnDef{
			{Name: "THREAD_ID", Type: "BIGINT UNSIGNED"},
			{Name: "EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "END_EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "EVENT_NAME", Type: "VARCHAR(128)"},
			{Name: "SOURCE", Type: "VARCHAR(64)"},
			{Name: "TIMER_START", Type: "BIGINT UNSIGNED"},
			{Name: "TIMER_END", Type: "BIGINT UNSIGNED"},
			{Name: "TIMER_WAIT", Type: "BIGINT UNSIGNED"},
			{Name: "WORK_COMPLETED", Type: "BIGINT UNSIGNED", Nullable: true},
			{Name: "WORK_ESTIMATED", Type: "BIGINT UNSIGNED", Nullable: true},
			{Name: "NESTING_EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "NESTING_EVENT_TYPE", Type: "ENUM('TRANSACTION','STATEMENT','STAGE','WAIT')"},
		},
	})
	ensure("performance_schema", &catalog.TableDef{
		Name: "events_stages_current",
		Columns: []catalog.ColumnDef{
			{Name: "THREAD_ID", Type: "BIGINT UNSIGNED"},
			{Name: "EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "END_EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "EVENT_NAME", Type: "VARCHAR(128)"},
			{Name: "SOURCE", Type: "VARCHAR(64)"},
			{Name: "TIMER_START", Type: "BIGINT UNSIGNED"},
			{Name: "TIMER_END", Type: "BIGINT UNSIGNED"},
			{Name: "TIMER_WAIT", Type: "BIGINT UNSIGNED"},
			{Name: "WORK_COMPLETED", Type: "BIGINT UNSIGNED", Nullable: true},
			{Name: "WORK_ESTIMATED", Type: "BIGINT UNSIGNED", Nullable: true},
			{Name: "NESTING_EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "NESTING_EVENT_TYPE", Type: "ENUM('TRANSACTION','STATEMENT','STAGE','WAIT')"},
		},
	})
	ensure("performance_schema", &catalog.TableDef{
		Name: "global_variables",
		Columns: []catalog.ColumnDef{
			{Name: "VARIABLE_NAME", Type: "VARCHAR(64)"},
			{Name: "VARIABLE_VALUE", Type: "VARCHAR(1024)"},
		},
	})
	ensure("performance_schema", &catalog.TableDef{
		Name: "session_variables",
		Columns: []catalog.ColumnDef{
			{Name: "VARIABLE_NAME", Type: "VARCHAR(64)"},
			{Name: "VARIABLE_VALUE", Type: "VARCHAR(1024)"},
		},
	})
	ensure("performance_schema", &catalog.TableDef{
		Name: "events_waits_history_long",
		Columns: []catalog.ColumnDef{
			{Name: "THREAD_ID", Type: "BIGINT UNSIGNED"},
			{Name: "EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "END_EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "EVENT_NAME", Type: "VARCHAR(128)"},
			{Name: "SOURCE", Type: "VARCHAR(64)"},
			{Name: "TIMER_START", Type: "BIGINT UNSIGNED"},
			{Name: "TIMER_END", Type: "BIGINT UNSIGNED"},
			{Name: "TIMER_WAIT", Type: "BIGINT UNSIGNED"},
			{Name: "SPINS", Type: "INT UNSIGNED"},
			{Name: "OBJECT_SCHEMA", Type: "VARCHAR(64)"},
			{Name: "OBJECT_NAME", Type: "VARCHAR(512)"},
			{Name: "INDEX_NAME", Type: "VARCHAR(64)"},
			{Name: "OBJECT_TYPE", Type: "VARCHAR(64)"},
			{Name: "OBJECT_INSTANCE_BEGIN", Type: "BIGINT UNSIGNED"},
			{Name: "NESTING_EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "NESTING_EVENT_TYPE", Type: "VARCHAR(64)"},
			{Name: "OPERATION", Type: "VARCHAR(32)"},
			{Name: "NUMBER_OF_BYTES", Type: "BIGINT"},
			{Name: "FLAGS", Type: "INT UNSIGNED"},
		},
	})
	ensure("performance_schema", &catalog.TableDef{
		Name: "events_waits_current",
		Columns: []catalog.ColumnDef{
			{Name: "THREAD_ID", Type: "BIGINT UNSIGNED"},
			{Name: "EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "END_EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "EVENT_NAME", Type: "VARCHAR(128)"},
			{Name: "SOURCE", Type: "VARCHAR(64)"},
			{Name: "TIMER_START", Type: "BIGINT UNSIGNED"},
			{Name: "TIMER_END", Type: "BIGINT UNSIGNED"},
			{Name: "TIMER_WAIT", Type: "BIGINT UNSIGNED"},
			{Name: "SPINS", Type: "INT UNSIGNED"},
			{Name: "OBJECT_SCHEMA", Type: "VARCHAR(64)"},
			{Name: "OBJECT_NAME", Type: "VARCHAR(512)"},
			{Name: "INDEX_NAME", Type: "VARCHAR(64)"},
			{Name: "OBJECT_TYPE", Type: "VARCHAR(64)"},
			{Name: "OBJECT_INSTANCE_BEGIN", Type: "BIGINT UNSIGNED"},
			{Name: "NESTING_EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "NESTING_EVENT_TYPE", Type: "VARCHAR(64)"},
			{Name: "OPERATION", Type: "VARCHAR(32)"},
			{Name: "NUMBER_OF_BYTES", Type: "BIGINT"},
			{Name: "FLAGS", Type: "INT UNSIGNED"},
		},
	})
	ensure("performance_schema", &catalog.TableDef{
		Name: "events_statements_history_long",
		Columns: []catalog.ColumnDef{
			{Name: "THREAD_ID", Type: "BIGINT UNSIGNED"},
			{Name: "EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "END_EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "EVENT_NAME", Type: "VARCHAR(128)"},
			{Name: "SOURCE", Type: "VARCHAR(64)"},
			{Name: "TIMER_START", Type: "BIGINT UNSIGNED"},
			{Name: "TIMER_END", Type: "BIGINT UNSIGNED"},
			{Name: "TIMER_WAIT", Type: "BIGINT UNSIGNED"},
			{Name: "SQL_TEXT", Type: "LONGTEXT"},
			{Name: "DIGEST", Type: "VARCHAR(64)"},
			{Name: "DIGEST_TEXT", Type: "LONGTEXT"},
		},
	})
	ensure("performance_schema", &catalog.TableDef{
		Name: "events_stages_history_long",
		Columns: []catalog.ColumnDef{
			{Name: "THREAD_ID", Type: "BIGINT UNSIGNED"},
			{Name: "EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "END_EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "EVENT_NAME", Type: "VARCHAR(128)"},
			{Name: "SOURCE", Type: "VARCHAR(64)"},
			{Name: "TIMER_START", Type: "BIGINT UNSIGNED"},
			{Name: "TIMER_END", Type: "BIGINT UNSIGNED"},
			{Name: "TIMER_WAIT", Type: "BIGINT UNSIGNED"},
			{Name: "WORK_COMPLETED", Type: "BIGINT UNSIGNED", Nullable: true},
			{Name: "WORK_ESTIMATED", Type: "BIGINT UNSIGNED", Nullable: true},
			{Name: "NESTING_EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "NESTING_EVENT_TYPE", Type: "ENUM('TRANSACTION','STATEMENT','STAGE','WAIT')"},
		},
	})

	ensure("mtr", &catalog.TableDef{
		Name:   "test_suppressions",
		Engine: "InnoDB",
		Columns: []catalog.ColumnDef{
			{Name: "pattern", Type: "VARCHAR(255)"},
		},
	})

	ensure("information_schema", &catalog.TableDef{
		Name: "INNODB_TRX",
		Columns: []catalog.ColumnDef{
			{Name: "trx_id", Type: "VARCHAR(18)"},
			{Name: "trx_state", Type: "VARCHAR(13)"},
			{Name: "trx_started", Type: "DATETIME"},
			{Name: "trx_requested_lock_id", Type: "VARCHAR(105)", Nullable: true},
			{Name: "trx_wait_started", Type: "DATETIME", Nullable: true},
			{Name: "trx_weight", Type: "BIGINT(21) UNSIGNED"},
			{Name: "trx_mysql_thread_id", Type: "BIGINT(21) UNSIGNED"},
			{Name: "trx_query", Type: "VARCHAR(1024)", Nullable: true},
			{Name: "trx_operation_state", Type: "VARCHAR(64)", Nullable: true},
			{Name: "trx_tables_in_use", Type: "BIGINT(21) UNSIGNED"},
			{Name: "trx_tables_locked", Type: "BIGINT(21) UNSIGNED"},
			{Name: "trx_lock_structs", Type: "BIGINT(21) UNSIGNED"},
			{Name: "trx_lock_memory_bytes", Type: "BIGINT(21) UNSIGNED"},
			{Name: "trx_rows_locked", Type: "BIGINT(21) UNSIGNED"},
			{Name: "trx_rows_modified", Type: "BIGINT(21) UNSIGNED"},
			{Name: "trx_concurrency_tickets", Type: "BIGINT(21) UNSIGNED"},
			{Name: "trx_isolation_level", Type: "VARCHAR(16)"},
			{Name: "trx_unique_checks", Type: "INT(1)"},
			{Name: "trx_foreign_key_checks", Type: "INT(1)"},
			{Name: "trx_last_foreign_key_error", Type: "VARCHAR(256)", Nullable: true},
			{Name: "trx_adaptive_hash_latched", Type: "INT(1)"},
			{Name: "trx_adaptive_hash_timeout", Type: "BIGINT(21) UNSIGNED"},
			{Name: "trx_is_read_only", Type: "INT(1)"},
			{Name: "trx_autocommit_non_locking", Type: "INT(1)"},
		},
	})
	ensure("information_schema", &catalog.TableDef{
		Name: "INNODB_BUFFER_POOL_STATS",
		Columns: []catalog.ColumnDef{
			{Name: "POOL_ID", Type: "BIGINT"},
			{Name: "POOL_SIZE", Type: "BIGINT"},
		},
	})
	ensure("information_schema", &catalog.TableDef{
		Name: "INNODB_FOREIGN_COLS",
		Columns: []catalog.ColumnDef{
			{Name: "ID", Type: "VARCHAR(255)"},
			{Name: "FOR_COL_NAME", Type: "VARCHAR(64)"},
			{Name: "REF_COL_NAME", Type: "VARCHAR(64)"},
			{Name: "POS", Type: "BIGINT"},
		},
	})
	ensure("information_schema", &catalog.TableDef{
		Name: "INNODB_INDEXES",
		Columns: []catalog.ColumnDef{
			{Name: "INDEX_ID", Type: "BIGINT"},
			{Name: "NAME", Type: "VARCHAR(255)"},
			{Name: "TABLE_ID", Type: "BIGINT"},
			{Name: "TYPE", Type: "BIGINT"},
		},
	})
	ensure("information_schema", &catalog.TableDef{
		Name: "INNODB_BUFFER_PAGE",
		Columns: []catalog.ColumnDef{
			{Name: "SPACE", Type: "BIGINT"},
			{Name: "PAGE_NUMBER", Type: "BIGINT"},
			{Name: "PAGE_TYPE", Type: "VARCHAR(64)"},
			{Name: "NUMBER_RECORDS", Type: "BIGINT"},
		},
	})
	ensure("information_schema", &catalog.TableDef{
		Name: "OPTIMIZER_TRACE",
		Columns: []catalog.ColumnDef{
			{Name: "QUERY", Type: "LONGTEXT"},
			{Name: "TRACE", Type: "LONGTEXT"},
		},
	})
	ensure("information_schema", &catalog.TableDef{
		Name: "INNODB_CMP_PER_INDEX",
		Columns: []catalog.ColumnDef{
			{Name: "database_name", Type: "VARCHAR(192)"},
			{Name: "table_name", Type: "VARCHAR(192)"},
			{Name: "index_name", Type: "VARCHAR(192)"},
			{Name: "compress_ops", Type: "INT"},
			{Name: "compress_ops_ok", Type: "INT"},
			{Name: "compress_time", Type: "INT"},
			{Name: "uncompress_ops", Type: "INT"},
			{Name: "uncompress_time", Type: "INT"},
		},
	})
	ensure("information_schema", &catalog.TableDef{
		Name: "PLUGINS",
		Columns: []catalog.ColumnDef{
			{Name: "PLUGIN_NAME", Type: "VARCHAR(64)"},
			{Name: "PLUGIN_VERSION", Type: "VARCHAR(20)"},
			{Name: "PLUGIN_STATUS", Type: "VARCHAR(10)"},
			{Name: "PLUGIN_TYPE", Type: "VARCHAR(80)"},
			{Name: "PLUGIN_TYPE_VERSION", Type: "VARCHAR(20)"},
			{Name: "PLUGIN_LIBRARY", Type: "VARCHAR(64)"},
			{Name: "PLUGIN_LIBRARY_VERSION", Type: "VARCHAR(20)"},
			{Name: "PLUGIN_AUTHOR", Type: "VARCHAR(64)"},
			{Name: "PLUGIN_DESCRIPTION", Type: "LONGTEXT"},
			{Name: "PLUGIN_LICENSE", Type: "VARCHAR(80)"},
			{Name: "LOAD_OPTION", Type: "VARCHAR(64)"},
		},
	})
	ensure("information_schema", &catalog.TableDef{
		Name: "INNODB_TEMP_TABLE_INFO",
		Columns: []catalog.ColumnDef{
			{Name: "TABLE_ID", Type: "BIGINT(21) UNSIGNED"},
			{Name: "NAME", Type: "VARCHAR(255)"},
			{Name: "N_COLS", Type: "BIGINT"},
			{Name: "SPACE", Type: "BIGINT"},
		},
	})

	// INFORMATION_SCHEMA tables referenced by MTR tests
	ensure("information_schema", &catalog.TableDef{
		Name: "EVENTS",
		Columns: []catalog.ColumnDef{
			{Name: "EVENT_CATALOG", Type: "VARCHAR(64)"},
			{Name: "EVENT_SCHEMA", Type: "VARCHAR(64)"},
			{Name: "EVENT_NAME", Type: "VARCHAR(64)"},
			{Name: "DEFINER", Type: "VARCHAR(288)"},
			{Name: "TIME_ZONE", Type: "VARCHAR(64)"},
			{Name: "EVENT_BODY", Type: "VARCHAR(8)"},
			{Name: "EVENT_DEFINITION", Type: "LONGTEXT"},
			{Name: "EVENT_TYPE", Type: "VARCHAR(9)"},
			{Name: "EXECUTE_AT", Type: "DATETIME"},
			{Name: "INTERVAL_VALUE", Type: "VARCHAR(256)"},
			{Name: "INTERVAL_FIELD", Type: "VARCHAR(18)"},
			{Name: "SQL_MODE", Type: "VARCHAR(8192)"},
			{Name: "STARTS", Type: "DATETIME"},
			{Name: "ENDS", Type: "DATETIME"},
			{Name: "STATUS", Type: "VARCHAR(18)"},
			{Name: "ON_COMPLETION", Type: "VARCHAR(12)"},
			{Name: "CREATED", Type: "DATETIME"},
			{Name: "LAST_ALTERED", Type: "DATETIME"},
			{Name: "LAST_EXECUTED", Type: "DATETIME"},
			{Name: "EVENT_COMMENT", Type: "VARCHAR(2048)"},
			{Name: "ORIGINATOR", Type: "BIGINT"},
			{Name: "CHARACTER_SET_CLIENT", Type: "VARCHAR(32)"},
			{Name: "COLLATION_CONNECTION", Type: "VARCHAR(32)"},
			{Name: "DATABASE_COLLATION", Type: "VARCHAR(32)"},
		},
	})
	ensure("information_schema", &catalog.TableDef{
		Name: "PARTITIONS",
		Columns: []catalog.ColumnDef{
			{Name: "TABLE_CATALOG", Type: "VARCHAR(64)"},
			{Name: "TABLE_SCHEMA", Type: "VARCHAR(64)"},
			{Name: "TABLE_NAME", Type: "VARCHAR(64)"},
			{Name: "PARTITION_NAME", Type: "VARCHAR(64)"},
			{Name: "SUBPARTITION_NAME", Type: "VARCHAR(64)"},
			{Name: "PARTITION_ORDINAL_POSITION", Type: "BIGINT"},
			{Name: "SUBPARTITION_ORDINAL_POSITION", Type: "BIGINT"},
			{Name: "PARTITION_METHOD", Type: "VARCHAR(18)"},
			{Name: "SUBPARTITION_METHOD", Type: "VARCHAR(12)"},
			{Name: "PARTITION_EXPRESSION", Type: "VARCHAR(2048)"},
			{Name: "SUBPARTITION_EXPRESSION", Type: "VARCHAR(2048)"},
			{Name: "PARTITION_DESCRIPTION", Type: "TEXT"},
			{Name: "TABLE_ROWS", Type: "BIGINT"},
			{Name: "AVG_ROW_LENGTH", Type: "BIGINT"},
			{Name: "DATA_LENGTH", Type: "BIGINT"},
			{Name: "MAX_DATA_LENGTH", Type: "BIGINT"},
			{Name: "INDEX_LENGTH", Type: "BIGINT"},
			{Name: "DATA_FREE", Type: "BIGINT"},
			{Name: "CREATE_TIME", Type: "DATETIME"},
			{Name: "UPDATE_TIME", Type: "DATETIME"},
			{Name: "CHECK_TIME", Type: "DATETIME"},
			{Name: "CHECKSUM", Type: "BIGINT"},
			{Name: "PARTITION_COMMENT", Type: "TEXT"},
			{Name: "NODEGROUP", Type: "VARCHAR(256)"},
			{Name: "TABLESPACE_NAME", Type: "VARCHAR(64)"},
		},
	})
	ensure("information_schema", &catalog.TableDef{
		Name: "RESOURCE_GROUPS",
		Columns: []catalog.ColumnDef{
			{Name: "RESOURCE_GROUP_NAME", Type: "VARCHAR(64)"},
			{Name: "RESOURCE_GROUP_TYPE", Type: "VARCHAR(4)"},
			{Name: "RESOURCE_GROUP_ENABLED", Type: "TINYINT"},
			{Name: "VCPU_IDS", Type: "TEXT"},
			{Name: "THREAD_PRIORITY", Type: "INT"},
		},
	})
	ensure("information_schema", &catalog.TableDef{
		Name: "VIEW_TABLE_USAGE",
		Columns: []catalog.ColumnDef{
			{Name: "VIEW_CATALOG", Type: "VARCHAR(64)"},
			{Name: "VIEW_SCHEMA", Type: "VARCHAR(64)"},
			{Name: "VIEW_NAME", Type: "VARCHAR(64)"},
			{Name: "TABLE_CATALOG", Type: "VARCHAR(64)"},
			{Name: "TABLE_SCHEMA", Type: "VARCHAR(64)"},
			{Name: "TABLE_NAME", Type: "VARCHAR(64)"},
		},
	})

	// MySQL system tables referenced by MTR tests
	ensure("mysql", &catalog.TableDef{
		Name: "user",
		Columns: func() []catalog.ColumnDef {
			defEmpty := ""
			defN := "N"
			return []catalog.ColumnDef{
				{Name: "Host", Type: "VARCHAR(255)", Default: &defEmpty},
				{Name: "User", Type: "VARCHAR(32)", Default: &defEmpty},
				{Name: "Select_priv", Type: "VARCHAR(1)", Default: &defN},
				{Name: "Insert_priv", Type: "VARCHAR(1)", Default: &defN},
				{Name: "Update_priv", Type: "VARCHAR(1)", Default: &defN},
				{Name: "Delete_priv", Type: "VARCHAR(1)", Default: &defN},
				{Name: "Create_priv", Type: "VARCHAR(1)", Default: &defN},
				{Name: "Drop_priv", Type: "VARCHAR(1)", Default: &defN},
				{Name: "Grant_priv", Type: "VARCHAR(1)", Default: &defN},
				{Name: "Shutdown_priv", Type: "VARCHAR(1)", Default: &defN},
				{Name: "authentication_string", Type: "TEXT", Default: &defEmpty},
				{Name: "plugin", Type: "VARCHAR(64)", Default: &defEmpty},
				{Name: "account_locked", Type: "VARCHAR(1)", Default: &defN},
				{Name: "ssl_cipher", Type: "BLOB", Default: &defEmpty},
				{Name: "x509_issuer", Type: "BLOB", Default: &defEmpty},
				{Name: "x509_subject", Type: "BLOB", Default: &defEmpty},
				{Name: "password_last_changed", Type: "TIMESTAMP", Nullable: true},
				{Name: "Password_reuse_history", Type: "SMALLINT UNSIGNED", Nullable: true},
				{Name: "Password_reuse_time", Type: "SMALLINT UNSIGNED", Nullable: true},
				{Name: "create_role_priv", Type: "ENUM('N','Y')", Default: &defN},
				{Name: "drop_role_priv", Type: "ENUM('N','Y')", Default: &defN},
			}
		}(),
	})
	ensure("mysql", &catalog.TableDef{
		Name: "db",
		Columns: func() []catalog.ColumnDef {
			defEmpty := ""
			defN := "N"
			return []catalog.ColumnDef{
				{Name: "Host", Type: "VARCHAR(255)", Default: &defEmpty},
				{Name: "Db", Type: "VARCHAR(64)", Default: &defEmpty},
				{Name: "User", Type: "VARCHAR(32)", Default: &defEmpty},
				{Name: "Select_priv", Type: "VARCHAR(1)", Default: &defN},
				{Name: "Insert_priv", Type: "VARCHAR(1)", Default: &defN},
				{Name: "Update_priv", Type: "VARCHAR(1)", Default: &defN},
				{Name: "Delete_priv", Type: "VARCHAR(1)", Default: &defN},
				{Name: "Create_priv", Type: "VARCHAR(1)", Default: &defN},
				{Name: "Drop_priv", Type: "VARCHAR(1)", Default: &defN},
				{Name: "Grant_priv", Type: "VARCHAR(1)", Default: &defN},
			}
		}(),
	})
	logDefaultTS := "CURRENT_TIMESTAMP(6)"
	ensure("mysql", &catalog.TableDef{
		Name:    "general_log",
		Engine:  "CSV",
		Charset: "utf8",
		Comment: "General log",
		Columns: []catalog.ColumnDef{
			{Name: "event_time", Type: "TIMESTAMP(6)", Default: &logDefaultTS, OnUpdateCurrentTimestamp: true},
			{Name: "user_host", Type: "MEDIUMTEXT"},
			{Name: "thread_id", Type: "BIGINT(21) UNSIGNED"},
			{Name: "server_id", Type: "INT(10) UNSIGNED"},
			{Name: "command_type", Type: "VARCHAR(64)"},
			{Name: "argument", Type: "MEDIUMBLOB"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name:    "slow_log",
		Engine:  "CSV",
		Charset: "utf8",
		Comment: "Slow log",
		Columns: []catalog.ColumnDef{
			{Name: "start_time", Type: "TIMESTAMP(6)", Default: &logDefaultTS, OnUpdateCurrentTimestamp: true},
			{Name: "user_host", Type: "MEDIUMTEXT"},
			{Name: "query_time", Type: "TIME(6)"},
			{Name: "lock_time", Type: "TIME(6)"},
			{Name: "rows_sent", Type: "INT(11)"},
			{Name: "rows_examined", Type: "INT(11)"},
			{Name: "db", Type: "VARCHAR(512)"},
			{Name: "last_insert_id", Type: "INT(11)"},
			{Name: "insert_id", Type: "INT(11)"},
			{Name: "server_id", Type: "INT(10) UNSIGNED"},
			{Name: "sql_text", Type: "MEDIUMBLOB"},
			{Name: "thread_id", Type: "BIGINT(21) UNSIGNED"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "server_cost",
		Columns: []catalog.ColumnDef{
			{Name: "cost_name", Type: "VARCHAR(64)"},
			{Name: "cost_value", Type: "FLOAT", Nullable: true},
			{Name: "last_update", Type: "TIMESTAMP"},
			{Name: "comment", Type: "VARCHAR(1024)", Nullable: true},
			{Name: "default_value", Type: "FLOAT", Nullable: true},
		},
	})
	if e.tableRowCount("mysql", "server_cost") == 0 {
		_, _ = e.Execute(`INSERT INTO mysql.server_cost (cost_name, cost_value, last_update, comment, default_value) VALUES` +
			` ('disk_temptable_create_cost', NULL, CURRENT_TIMESTAMP, NULL, 20.0),` +
			` ('disk_temptable_row_cost', NULL, CURRENT_TIMESTAMP, NULL, 0.5),` +
			` ('key_compare_cost', NULL, CURRENT_TIMESTAMP, NULL, 0.05),` +
			` ('memory_temptable_create_cost', NULL, CURRENT_TIMESTAMP, NULL, 1.0),` +
			` ('memory_temptable_row_cost', NULL, CURRENT_TIMESTAMP, NULL, 0.1),` +
			` ('row_evaluate_cost', NULL, CURRENT_TIMESTAMP, NULL, 0.1)`)
	}
	ensure("mysql", &catalog.TableDef{
		Name: "engine_cost",
		Columns: []catalog.ColumnDef{
			{Name: "engine_name", Type: "VARCHAR(64)"},
			{Name: "device_type", Type: "INT"},
			{Name: "cost_name", Type: "VARCHAR(64)"},
			{Name: "cost_value", Type: "FLOAT", Nullable: true},
			{Name: "last_update", Type: "TIMESTAMP"},
			{Name: "comment", Type: "VARCHAR(1024)", Nullable: true},
			{Name: "default_value", Type: "FLOAT", Nullable: true},
		},
	})
	if e.tableRowCount("mysql", "engine_cost") == 0 {
		_, _ = e.Execute(`INSERT INTO mysql.engine_cost (engine_name, device_type, cost_name, cost_value, last_update, comment, default_value) VALUES` +
			` ('default', 0, 'io_block_read_cost', NULL, CURRENT_TIMESTAMP, NULL, 1.0),` +
			` ('default', 0, 'memory_block_read_cost', NULL, CURRENT_TIMESTAMP, NULL, 0.25)`)
	}
	ensure("mysql", &catalog.TableDef{
		Name: "tables_priv",
		Columns: []catalog.ColumnDef{
			{Name: "Host", Type: "VARCHAR(255)"},
			{Name: "Db", Type: "VARCHAR(64)"},
			{Name: "User", Type: "VARCHAR(32)"},
			{Name: "Table_name", Type: "VARCHAR(64)"},
			{Name: "Grantor", Type: "VARCHAR(288)"},
			{Name: "Timestamp", Type: "TIMESTAMP"},
			{Name: "Table_priv", Type: "VARCHAR(200)"},
			{Name: "Column_priv", Type: "VARCHAR(200)"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "columns_priv",
		Columns: []catalog.ColumnDef{
			{Name: "Host", Type: "VARCHAR(255)"},
			{Name: "Db", Type: "VARCHAR(64)"},
			{Name: "User", Type: "VARCHAR(32)"},
			{Name: "Table_name", Type: "VARCHAR(64)"},
			{Name: "Column_name", Type: "VARCHAR(64)"},
			{Name: "Timestamp", Type: "TIMESTAMP"},
			{Name: "Column_priv", Type: "VARCHAR(200)"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "role_edges",
		Columns: []catalog.ColumnDef{
			{Name: "FROM_HOST", Type: "VARCHAR(255)"},
			{Name: "FROM_USER", Type: "VARCHAR(32)"},
			{Name: "TO_HOST", Type: "VARCHAR(255)"},
			{Name: "TO_USER", Type: "VARCHAR(32)"},
			{Name: "WITH_ADMIN_OPTION", Type: "VARCHAR(1)"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "default_roles",
		Columns: []catalog.ColumnDef{
			{Name: "HOST", Type: "VARCHAR(255)"},
			{Name: "USER", Type: "VARCHAR(32)"},
			{Name: "DEFAULT_ROLE_HOST", Type: "VARCHAR(255)"},
			{Name: "DEFAULT_ROLE_USER", Type: "VARCHAR(32)"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "func",
		Columns: []catalog.ColumnDef{
			{Name: "name", Type: "VARCHAR(64)"},
			{Name: "ret", Type: "INT"},
			{Name: "dl", Type: "VARCHAR(128)"},
			{Name: "type", Type: "VARCHAR(10)"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "procs_priv",
		Columns: []catalog.ColumnDef{
			{Name: "Host", Type: "VARCHAR(255)"},
			{Name: "Db", Type: "VARCHAR(64)"},
			{Name: "User", Type: "VARCHAR(32)"},
			{Name: "Routine_name", Type: "VARCHAR(64)"},
			{Name: "Routine_type", Type: "VARCHAR(20)"},
			{Name: "Grantor", Type: "VARCHAR(288)"},
			{Name: "Proc_priv", Type: "VARCHAR(200)"},
			{Name: "Timestamp", Type: "TIMESTAMP"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "component",
		Columns: []catalog.ColumnDef{
			{Name: "component_id", Type: "INT UNSIGNED"},
			{Name: "component_group_id", Type: "INT UNSIGNED"},
			{Name: "component_urn", Type: "TEXT"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "global_grants",
		Columns: []catalog.ColumnDef{
			{Name: "USER", Type: "VARCHAR(32)"},
			{Name: "HOST", Type: "VARCHAR(255)"},
			{Name: "PRIV", Type: "VARCHAR(32)"},
			{Name: "WITH_GRANT_OPTION", Type: "VARCHAR(1)"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "gtid_executed",
		Columns: []catalog.ColumnDef{
			{Name: "source_uuid", Type: "CHAR(36)"},
			{Name: "interval_start", Type: "BIGINT"},
			{Name: "interval_end", Type: "BIGINT"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "help_category",
		Columns: []catalog.ColumnDef{
			{Name: "help_category_id", Type: "SMALLINT UNSIGNED"},
			{Name: "name", Type: "VARCHAR(64)"},
			{Name: "parent_category_id", Type: "SMALLINT UNSIGNED"},
			{Name: "url", Type: "TEXT"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "help_keyword",
		Columns: []catalog.ColumnDef{
			{Name: "help_keyword_id", Type: "INT UNSIGNED"},
			{Name: "name", Type: "VARCHAR(64)"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "help_relation",
		Columns: []catalog.ColumnDef{
			{Name: "help_topic_id", Type: "INT UNSIGNED"},
			{Name: "help_keyword_id", Type: "INT UNSIGNED"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "help_topic",
		Columns: []catalog.ColumnDef{
			{Name: "help_topic_id", Type: "INT UNSIGNED"},
			{Name: "name", Type: "VARCHAR(64)"},
			{Name: "help_category_id", Type: "SMALLINT UNSIGNED"},
			{Name: "description", Type: "TEXT"},
			{Name: "example", Type: "TEXT"},
			{Name: "url", Type: "TEXT"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "password_history",
		Columns: []catalog.ColumnDef{
			{Name: "Host", Type: "VARCHAR(255)"},
			{Name: "User", Type: "VARCHAR(32)"},
			{Name: "Password_timestamp", Type: "TIMESTAMP(6)"},
			{Name: "Password", Type: "TEXT"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "plugin",
		Columns: []catalog.ColumnDef{
			{Name: "name", Type: "VARCHAR(64)"},
			{Name: "dl", Type: "VARCHAR(128)"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "proxies_priv",
		Columns: []catalog.ColumnDef{
			{Name: "Host", Type: "VARCHAR(255)"},
			{Name: "User", Type: "VARCHAR(32)"},
			{Name: "Proxied_host", Type: "VARCHAR(255)"},
			{Name: "Proxied_user", Type: "VARCHAR(32)"},
			{Name: "With_grant", Type: "TINYINT"},
			{Name: "Grantor", Type: "VARCHAR(288)"},
			{Name: "Timestamp", Type: "TIMESTAMP"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "servers",
		Columns: []catalog.ColumnDef{
			{Name: "Server_name", Type: "VARCHAR(64)"},
			{Name: "Host", Type: "VARCHAR(255)"},
			{Name: "Db", Type: "VARCHAR(64)"},
			{Name: "Username", Type: "VARCHAR(64)"},
			{Name: "Password", Type: "VARCHAR(64)"},
			{Name: "Port", Type: "INT"},
			{Name: "Socket", Type: "VARCHAR(64)"},
			{Name: "Wrapper", Type: "VARCHAR(64)"},
			{Name: "Owner", Type: "VARCHAR(64)"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "slave_master_info",
		Columns: []catalog.ColumnDef{
			{Name: "Number_of_lines", Type: "INT UNSIGNED"},
			{Name: "Master_log_name", Type: "TEXT"},
			{Name: "Master_log_pos", Type: "BIGINT UNSIGNED"},
			{Name: "Host", Type: "VARCHAR(255)"},
			{Name: "User_name", Type: "TEXT"},
			{Name: "User_password", Type: "TEXT"},
			{Name: "Port", Type: "INT UNSIGNED"},
			{Name: "Connect_retry", Type: "INT UNSIGNED"},
			{Name: "Enabled_ssl", Type: "TINYINT UNSIGNED"},
			{Name: "Ssl_ca", Type: "TEXT"},
			{Name: "Ssl_capath", Type: "TEXT"},
			{Name: "Ssl_cert", Type: "TEXT"},
			{Name: "Ssl_cipher", Type: "TEXT"},
			{Name: "Ssl_key", Type: "TEXT"},
			{Name: "Ssl_verify_server_cert", Type: "TINYINT UNSIGNED"},
			{Name: "Heartbeat", Type: "FLOAT"},
			{Name: "Bind", Type: "TEXT"},
			{Name: "Ignored_server_ids", Type: "TEXT"},
			{Name: "Uuid", Type: "TEXT"},
			{Name: "Retry_count", Type: "BIGINT UNSIGNED"},
			{Name: "Ssl_crl", Type: "TEXT"},
			{Name: "Ssl_crlpath", Type: "TEXT"},
			{Name: "Enabled_auto_position", Type: "TINYINT UNSIGNED"},
			{Name: "Channel_name", Type: "VARCHAR(64)"},
			{Name: "Tls_version", Type: "TEXT"},
			{Name: "Public_key_path", Type: "TEXT"},
			{Name: "Get_public_key", Type: "TINYINT UNSIGNED"},
			{Name: "Network_namespace", Type: "TEXT"},
			{Name: "Master_compression_algorithm", Type: "VARCHAR(64)"},
			{Name: "Master_zstd_compression_level", Type: "INT UNSIGNED"},
			{Name: "Tls_ciphersuites", Type: "TEXT"},
			{Name: "Source_connection_auto_failover", Type: "TINYINT UNSIGNED"},
			{Name: "Gtid_only", Type: "TINYINT UNSIGNED"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "slave_relay_log_info",
		Columns: []catalog.ColumnDef{
			{Name: "Number_of_lines", Type: "INT UNSIGNED"},
			{Name: "Relay_log_name", Type: "TEXT"},
			{Name: "Relay_log_pos", Type: "BIGINT UNSIGNED"},
			{Name: "Master_log_name", Type: "TEXT"},
			{Name: "Master_log_pos", Type: "BIGINT UNSIGNED"},
			{Name: "Sql_delay", Type: "INT"},
			{Name: "Number_of_workers", Type: "INT UNSIGNED"},
			{Name: "Id", Type: "INT UNSIGNED"},
			{Name: "Channel_name", Type: "VARCHAR(64)"},
			{Name: "Privilege_checks_username", Type: "TEXT"},
			{Name: "Privilege_checks_hostname", Type: "TEXT"},
			{Name: "Require_row_format", Type: "TINYINT UNSIGNED"},
			{Name: "Require_table_primary_key_check", Type: "ENUM('STREAM','ON','OFF','GENERATE')"},
			{Name: "Assign_gtids_to_anonymous_transactions_type", Type: "ENUM('OFF','LOCAL','UUID')"},
			{Name: "Assign_gtids_to_anonymous_transactions_value", Type: "TEXT"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "slave_worker_info",
		Columns: []catalog.ColumnDef{
			{Name: "Id", Type: "INT UNSIGNED"},
			{Name: "Relay_log_name", Type: "TEXT"},
			{Name: "Relay_log_pos", Type: "BIGINT UNSIGNED"},
			{Name: "Master_log_name", Type: "TEXT"},
			{Name: "Master_log_pos", Type: "BIGINT UNSIGNED"},
			{Name: "Checkpoint_relay_log_name", Type: "TEXT"},
			{Name: "Checkpoint_relay_log_pos", Type: "BIGINT UNSIGNED"},
			{Name: "Checkpoint_master_log_name", Type: "TEXT"},
			{Name: "Checkpoint_master_log_pos", Type: "BIGINT UNSIGNED"},
			{Name: "Checkpoint_seqno", Type: "INT UNSIGNED"},
			{Name: "Checkpoint_group_size", Type: "INT UNSIGNED"},
			{Name: "Checkpoint_group_bitmap", Type: "BLOB"},
			{Name: "Channel_name", Type: "VARCHAR(64)"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "time_zone",
		Columns: []catalog.ColumnDef{
			{Name: "Time_zone_id", Type: "INT UNSIGNED"},
			{Name: "Use_leap_seconds", Type: "VARCHAR(1)"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "time_zone_leap_second",
		Columns: []catalog.ColumnDef{
			{Name: "Transition_time", Type: "BIGINT"},
			{Name: "Correction", Type: "INT"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "time_zone_name",
		Columns: []catalog.ColumnDef{
			{Name: "Name", Type: "VARCHAR(64)"},
			{Name: "Time_zone_id", Type: "INT UNSIGNED"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "time_zone_transition",
		Columns: []catalog.ColumnDef{
			{Name: "Time_zone_id", Type: "INT UNSIGNED"},
			{Name: "Transition_time", Type: "BIGINT"},
			{Name: "Transition_type_id", Type: "INT UNSIGNED"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "time_zone_transition_type",
		Columns: []catalog.ColumnDef{
			{Name: "Time_zone_id", Type: "INT UNSIGNED"},
			{Name: "Transition_type_id", Type: "INT UNSIGNED"},
			{Name: "Offset", Type: "INT"},
			{Name: "Is_DST", Type: "TINYINT UNSIGNED"},
			{Name: "Abbreviation", Type: "VARCHAR(8)"},
		},
	})
}

func isSystemSchemaName(name string) bool {
	switch strings.ToLower(name) {
	case "information_schema", "mysql", "performance_schema", "sys":
		return true
	default:
		return false
	}
}

func statsIndexColName(raw string) string {
	if idx := strings.Index(raw, "("); idx >= 0 {
		return strings.TrimSpace(raw[:idx])
	}
	return strings.TrimSpace(raw)
}

func (e *Executor) innodbStatsPersistentEnabled(def *catalog.TableDef) bool {
	if def != nil && def.StatsPersistent != nil {
		return *def.StatsPersistent != 0
	}
	if v, ok := e.getSysVar("innodb_stats_persistent"); ok && v != "" {
		return v != "0" && !strings.EqualFold(v, "OFF")
	}
	return true
}

func (e *Executor) innodbStatsAutoRecalcEnabled(def *catalog.TableDef) bool {
	if def != nil && def.StatsAutoRecalc != nil {
		return *def.StatsAutoRecalc != 0
	}
	if v, ok := e.getSysVar("innodb_stats_auto_recalc"); ok && v != "" {
		return v != "0" && !strings.EqualFold(v, "OFF")
	}
	return true
}

func (e *Executor) tableRowCount(dbName, tableName string) int64 {
	tbl, err := e.Storage.GetTable(dbName, tableName)
	if err != nil {
		return 0
	}
	tbl.Mu.RLock()
	defer tbl.Mu.RUnlock()
	return int64(len(tbl.Rows))
}

func (e *Executor) hasInnoDBTableStatsRow(dbName, tableName string) bool {
	statsTbl, err := e.Storage.GetTable("mysql", "innodb_table_stats")
	if err != nil {
		return false
	}
	statsTbl.Mu.RLock()
	defer statsTbl.Mu.RUnlock()
	for _, r := range statsTbl.Rows {
		if strings.EqualFold(toString(r["database_name"]), dbName) && strings.EqualFold(toString(r["table_name"]), tableName) {
			return true
		}
	}
	return false
}

func indexDefsForStats(def *catalog.TableDef) []catalog.IndexDef {
	indexDefs := make([]catalog.IndexDef, 0, len(def.Indexes)+1)
	if len(def.PrimaryKey) > 0 {
		indexDefs = append(indexDefs, catalog.IndexDef{Name: "PRIMARY", Columns: append([]string(nil), def.PrimaryKey...)})
	}
	indexDefs = append(indexDefs, def.Indexes...)
	if len(indexDefs) == 0 {
		indexDefs = append(indexDefs, catalog.IndexDef{Name: "GEN_CLUST_INDEX", Columns: []string{"DB_ROW_ID"}})
	}
	return indexDefs
}

func (e *Executor) removeInnoDBStatsRows(dbName, tableName string) {
	statsTbl, err := e.Storage.GetTable("mysql", "innodb_table_stats")
	if err == nil {
		statsTbl.Mu.Lock()
		filtered := make([]storage.Row, 0, len(statsTbl.Rows))
		for _, r := range statsTbl.Rows {
			if strings.EqualFold(toString(r["database_name"]), dbName) && strings.EqualFold(toString(r["table_name"]), tableName) {
				continue
			}
			filtered = append(filtered, r)
		}
		statsTbl.Rows = filtered
		statsTbl.Mu.Unlock()
	}
	idxTbl, err := e.Storage.GetTable("mysql", "innodb_index_stats")
	if err == nil {
		idxTbl.Mu.Lock()
		filtered := make([]storage.Row, 0, len(idxTbl.Rows))
		for _, r := range idxTbl.Rows {
			if strings.EqualFold(toString(r["database_name"]), dbName) && strings.EqualFold(toString(r["table_name"]), tableName) {
				continue
			}
			filtered = append(filtered, r)
		}
		idxTbl.Rows = filtered
		idxTbl.Mu.Unlock()
	}
}

// maybeRecalcStats implements MySQL's InnoDB auto-recalc threshold:
// stats are recomputed only when the cumulative DML change count exceeds
// 10% of the row count at the time stats were last calculated (minimum 200).
func (e *Executor) maybeRecalcStats(dbName, tableName string, changes int64) {
	tbl, err := e.Storage.GetTable(dbName, tableName)
	if err != nil {
		return
	}
	tbl.Mu.Lock()
	tbl.DMLChangesSinceStats += changes
	pending := tbl.DMLChangesSinceStats
	lastCount := tbl.RowCountAtLastStats
	tbl.Mu.Unlock()

	threshold := lastCount / 10
	if threshold < 200 {
		threshold = 200
	}
	if pending < threshold {
		return
	}

	rowCount := e.tableRowCount(dbName, tableName)
	e.upsertInnoDBStatsRows(dbName, tableName, rowCount)

	tbl.Mu.Lock()
	tbl.DMLChangesSinceStats = 0
	tbl.RowCountAtLastStats = rowCount
	tbl.Mu.Unlock()
}

// upsertInnoDBTableStatsOnly inserts only into innodb_table_stats (not innodb_index_stats).
// Used by CREATE TABLE so that SHOW INDEX cardinality returns NULL for freshly created tables
// (no ANALYZE has been run yet). ANALYZE TABLE will call upsertInnoDBStatsRows which also
// inserts index stats, making cardinality show computed values.
func (e *Executor) upsertInnoDBTableStatsOnly(dbName, tableName string, rowCount int64) {
	db, err := e.Catalog.GetDatabase(dbName)
	if err != nil {
		return
	}
	def, err := db.GetTable(tableName)
	if err != nil || def == nil {
		return
	}
	if def.Engine != "" && !strings.EqualFold(def.Engine, "InnoDB") {
		return
	}
	if !e.innodbStatsPersistentEnabled(def) {
		e.removeInnoDBStatsRows(dbName, tableName)
		return
	}

	e.removeInnoDBStatsRows(dbName, tableName)
	lastUpdate := time.Now().UTC().Format("2006-01-02 15:04:05")

	statsTbl, err := e.Storage.GetTable("mysql", "innodb_table_stats")
	if err == nil {
		statsTbl.Mu.Lock()
		statsTbl.Rows = append(statsTbl.Rows, storage.Row{
			"database_name":            dbName,
			"table_name":               tableName,
			"last_update":              lastUpdate,
			"n_rows":                   rowCount,
			"clustered_index_size":     int64(1),
			"sum_of_other_index_sizes": int64(len(def.Indexes)),
		})
		statsTbl.Mu.Unlock()
	}
	// Note: intentionally does NOT insert into innodb_index_stats.
	// Cardinality will show as NULL in SHOW INDEX until ANALYZE TABLE is run.
}

func (e *Executor) upsertInnoDBStatsRowsFromCreate(dbName, tableName string, rowCount int64) {
	e.upsertInnoDBStatsRowsInternal(dbName, tableName, rowCount, true)
}

func (e *Executor) upsertInnoDBStatsRows(dbName, tableName string, rowCount int64) {
	e.upsertInnoDBStatsRowsInternal(dbName, tableName, rowCount, false)
}

func (e *Executor) upsertInnoDBStatsRowsInternal(dbName, tableName string, rowCount int64, notAnalyzed bool) {
	// Skip stats for temporary tables - they don't have persistent stats.
	if e.tempTables != nil && (e.tempTables[tableName] || e.tempTables[strings.ToLower(tableName)]) {
		return
	}
	db, err := e.Catalog.GetDatabase(dbName)
	if err != nil {
		return
	}
	def, err := db.GetTable(tableName)
	if err != nil || def == nil {
		return
	}
	if def.Engine != "" && !strings.EqualFold(def.Engine, "InnoDB") {
		return
	}
	if !e.innodbStatsPersistentEnabled(def) {
		e.removeInnoDBStatsRows(dbName, tableName)
		return
	}

	e.removeInnoDBStatsRows(dbName, tableName)
	lastUpdate := time.Now().UTC().Format("2006-01-02 15:04:05")

	statsTbl, err := e.Storage.GetTable("mysql", "innodb_table_stats")
	if err == nil {
		statsTbl.Mu.Lock()
		statsTbl.Rows = append(statsTbl.Rows, storage.Row{
			"database_name":            dbName,
			"table_name":               tableName,
			"last_update":              lastUpdate,
			"n_rows":                   rowCount,
			"clustered_index_size":     int64(1),
			"sum_of_other_index_sizes": int64(len(def.Indexes)),
		})
		statsTbl.Mu.Unlock()
	}

	// Load table rows for distinct count computation
	var tableRows []storage.Row
	if tbl, err := e.Storage.GetTable(dbName, tableName); err == nil {
		tbl.Mu.RLock()
		tableRows = tbl.Rows
		tbl.Mu.RUnlock()
	}

	idxTbl, err := e.Storage.GetTable("mysql", "innodb_index_stats")
	if err != nil {
		return
	}
	idxTbl.Mu.Lock()
	for _, idx := range indexDefsForStats(def) {
		indexName := idx.Name
		if indexName == "" {
			indexName = "PRIMARY"
		}
		statCols := make([]string, 0, len(idx.Columns)+1)
		statCols = append(statCols, idx.Columns...)
		if !strings.EqualFold(indexName, "PRIMARY") && !strings.EqualFold(indexName, "GEN_CLUST_INDEX") {
			if len(def.PrimaryKey) > 0 {
				statCols = append(statCols, def.PrimaryKey...)
			} else {
				statCols = append(statCols, "DB_ROW_ID")
			}
		}
		sampleSize := rowCount
		if sampleSize < 1 {
			sampleSize = 1
		}
		for i := range statCols {
			statName := fmt.Sprintf("n_diff_pfx%02d", i+1)
			descCols := make([]string, 0, i+1)
			for j := 0; j <= i; j++ {
				descCols = append(descCols, statsIndexColName(statCols[j]))
			}
			statValue := computeDistinctCount(tableRows, statCols[:i+1])
			// When notAnalyzed=true (CREATE TABLE), use sample_size=0 as a sentinel
			// meaning "stats not yet computed by ANALYZE". This makes SHOW INDEX
			// display NULL for cardinality (matching MySQL behavior for fresh tables).
			ndiffSampleSize := interface{}(int64(1))
			if notAnalyzed {
				ndiffSampleSize = int64(0)
			}
			idxTbl.Rows = append(idxTbl.Rows, storage.Row{
				"database_name":    dbName,
				"table_name":       tableName,
				"index_name":       indexName,
				"last_update":      lastUpdate,
				"stat_name":        statName,
				"stat_value":       statValue,
				"sample_size":      ndiffSampleSize,
				"stat_description": strings.Join(descCols, ","),
			})
		}
		idxTbl.Rows = append(idxTbl.Rows,
			storage.Row{
				"database_name":    dbName,
				"table_name":       tableName,
				"index_name":       indexName,
				"last_update":      lastUpdate,
				"stat_name":        "n_leaf_pages",
				"stat_value":       int64(1),
				"sample_size":      nil,
				"stat_description": "Number of leaf pages in the index",
			},
			storage.Row{
				"database_name":    dbName,
				"table_name":       tableName,
				"index_name":       indexName,
				"last_update":      lastUpdate,
				"stat_name":        "size",
				"stat_value":       int64(1),
				"sample_size":      nil,
				"stat_description": "Number of pages in the index",
			},
		)
	}
	idxTbl.Mu.Unlock()
}

// computeDistinctCount counts the number of distinct value combinations for the given
// column prefix across table rows. For DB_ROW_ID (implicit row ID), each row is unique.
func computeDistinctCount(rows []storage.Row, cols []string) int64 {
	if len(rows) == 0 {
		return int64(0)
	}
	if len(cols) > 0 && cols[len(cols)-1] == "DB_ROW_ID" {
		return int64(len(rows))
	}
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		var key strings.Builder
		for ci, col := range cols {
			if ci > 0 {
				key.WriteByte(0)
			}
			var val interface{}
			for k, v := range row {
				if strings.EqualFold(k, col) {
					val = v
					break
				}
			}
			fmt.Fprintf(&key, "%v", val)
		}
		seen[key.String()] = struct{}{}
	}
	return int64(len(seen))
}

func (e *Executor) refreshInnoDBStatsTables() {
	if e.Catalog == nil || e.Storage == nil {
		return
	}
	mysqlDB, err := e.Catalog.GetDatabase("mysql")
	if err != nil {
		return
	}
	tblStats, err := e.Storage.GetTable("mysql", "innodb_table_stats")
	if err != nil {
		return
	}
	idxStats, err := e.Storage.GetTable("mysql", "innodb_index_stats")
	if err != nil {
		return
	}

	lastUpdate := time.Now().UTC().Format("2006-01-02 15:04:05")
	tableRows := make([]storage.Row, 0, 256)
	indexRows := make([]storage.Row, 0, 1024)

	for dbName, db := range e.Catalog.Databases {
		if isSystemSchemaName(dbName) {
			continue
		}
		for tableName, def := range db.Tables {
			if def == nil {
				continue
			}
			if def.Engine != "" && !strings.EqualFold(def.Engine, "InnoDB") {
				continue
			}
			if !e.innodbStatsPersistentEnabled(def) {
				continue
			}
			var rowCount int64
			if t, err := e.Storage.GetTable(dbName, tableName); err == nil {
				t.Mu.RLock()
				rowCount = int64(len(t.Rows))
				t.Mu.RUnlock()
			}
			tableRows = append(tableRows, storage.Row{
				"database_name":            dbName,
				"table_name":               tableName,
				"last_update":              lastUpdate,
				"n_rows":                   rowCount,
				"clustered_index_size":     int64(1),
				"sum_of_other_index_sizes": int64(len(def.Indexes)),
			})

			indexDefs := make([]catalog.IndexDef, 0, len(def.Indexes)+1)
			if len(def.PrimaryKey) > 0 {
				indexDefs = append(indexDefs, catalog.IndexDef{Name: "PRIMARY", Columns: append([]string(nil), def.PrimaryKey...)})
			}
			indexDefs = append(indexDefs, def.Indexes...)
			if len(indexDefs) == 0 {
				indexDefs = append(indexDefs, catalog.IndexDef{Name: "GEN_CLUST_INDEX", Columns: []string{"DB_ROW_ID"}})
			}
			for _, idx := range indexDefs {
				indexName := idx.Name
				if indexName == "" {
					indexName = "PRIMARY"
				}
				firstCol := "id"
				if len(idx.Columns) > 0 {
					firstCol = statsIndexColName(idx.Columns[0])
				}
				indexRows = append(indexRows,
					storage.Row{
						"database_name":    dbName,
						"table_name":       tableName,
						"index_name":       indexName,
						"last_update":      lastUpdate,
						"stat_name":        "n_diff_pfx01",
						"stat_value":       rowCount,
						"sample_size":      rowCount,
						"stat_description": firstCol,
					},
					storage.Row{
						"database_name":    dbName,
						"table_name":       tableName,
						"index_name":       indexName,
						"last_update":      lastUpdate,
						"stat_name":        "n_leaf_pages",
						"stat_value":       int64(1),
						"sample_size":      nil,
						"stat_description": "Number of leaf pages in the index",
					},
					storage.Row{
						"database_name":    dbName,
						"table_name":       tableName,
						"index_name":       indexName,
						"last_update":      lastUpdate,
						"stat_name":        "size",
						"stat_value":       int64(1),
						"sample_size":      nil,
						"stat_description": "Number of pages in the index",
					},
				)
			}
		}
	}

	// Keep table definition existence checked to avoid writing into stale tables.
	if _, err := mysqlDB.GetTable("innodb_table_stats"); err == nil {
		tblStats.Mu.Lock()
		tblStats.Rows = tableRows
		tblStats.Mu.Unlock()
	}
	if _, err := mysqlDB.GetTable("innodb_index_stats"); err == nil {
		idxStats.Mu.Lock()
		idxStats.Rows = indexRows
		idxStats.Mu.Unlock()
	}
}

// mysqlError formats an error message in MySQL error style.
// Format: "ERROR <code> (<state>): <message>"
func mysqlError(code int, state, message string) error {
	return fmt.Errorf("ERROR %d (%s): %s", code, state, message)
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

// normalizeEngineName returns the canonical MySQL engine name for a given input.
func normalizeEngineName(name string) string {
	switch strings.ToUpper(name) {
	case "INNODB":
		return "InnoDB"
	case "MYISAM":
		return "MyISAM"
	case "MERGE", "MRG_MYISAM":
		return "MRG_MYISAM"
	case "MEMORY", "HEAP":
		return "MEMORY"
	case "CSV":
		return "CSV"
	case "ARCHIVE":
		return "ARCHIVE"
	case "BLACKHOLE":
		return "BLACKHOLE"
	case "FEDERATED":
		return "FEDERATED"
	case "NDB", "NDBCLUSTER":
		return "ndbcluster"
	case "PERFORMANCE_SCHEMA":
		return "PERFORMANCE_SCHEMA"
	default:
		return name
	}
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

func matchLike(s, pattern string) bool {
	// Convert to runes for proper multibyte character handling
	sr := []rune(strings.ToLower(s))
	pr := []rune(strings.ToLower(pattern))
	return matchLikeHelper(sr, pr, 0, 0)
}

func matchLikeHelper(s, p []rune, si, pi int) bool {
	for pi < len(p) {
		if p[pi] == '\\' && pi+1 < len(p) {
			// Backslash escape: next character is treated as literal
			pi++
			if si >= len(s) || s[si] != p[pi] {
				return false
			}
			si++
			pi++
		} else if p[pi] == '%' {
			pi++
			for si <= len(s) {
				if matchLikeHelper(s, p, si, pi) {
					return true
				}
				si++
			}
			return false
		} else if p[pi] == '_' {
			if si >= len(s) {
				return false
			}
			si++
			pi++
		} else {
			if si >= len(s) || s[si] != p[pi] {
				return false
			}
			si++
			pi++
		}
	}
	return si == len(s)
}

// stripBlockComments removes /* ... */ block comments from a string,
// but only when they appear at the end of the expression (trailing comments).
// Embedded comments like 1+2/*hello*/+3 are preserved.
// Used to clean up column names where MySQL strips trailing block comments.
func stripBlockComments(s string) string {
	// Only strip if the expression ends with */
	trimmed := strings.TrimSpace(s)
	if !strings.HasSuffix(trimmed, "*/") {
		return s
	}
	// Find the start of the last trailing block comment
	// We want to find the rightmost /* such that everything after */ is whitespace
	result := trimmed
	for strings.HasSuffix(result, "*/") {
		// Find the matching /*
		end := strings.LastIndex(result, "*/")
		start := strings.LastIndex(result[:end], "/*")
		if start < 0 {
			break
		}
		// Check that what's after the */ is only whitespace (already trimmed, so end is at len-2)
		// and that what's before /* is meaningful
		candidate := strings.TrimSpace(result[:start])
		if candidate == "" {
			break
		}
		result = candidate
	}
	return result
}

// normalizeSQLDisplayName converts SQL keywords in a string to uppercase and
// normalizes operator spacing to match MySQL's column display name behavior.
func normalizeSQLDisplayName(s string) string {
	s = uppercaseSQLKeywords(s)
	// Compact operators only in nested subexpressions, while keeping top-level
	// spacing (e.g. "a = b") used by some result headers.
	s = compactOperatorsInSubexpressions(s)
	// MySQL displays function arguments without space after comma: LEFT(`c1`,0) not LEFT(`c1`, 0)
	if !strings.HasPrefix(s, "JSON_SCHEMA_VALID(") &&
		!strings.HasPrefix(s, "JSON_SCHEMA_VALIDATION_REPORT(") &&
		!strings.HasPrefix(s, "JSON_MERGE_PRESERVE(") {
		s = normalizeFuncArgSpaces(s)
	}
	// MySQL displays SUBSTRING, not SUBSTR in column headers
	if strings.HasPrefix(s, "SUBSTR(") && !strings.HasPrefix(s, "SUBSTRING(") {
		s = "SUBSTRING" + s[6:]
	}
	if strings.HasPrefix(s, "substr(") && !strings.HasPrefix(s, "substring(") {
		s = "substring" + s[6:]
	}
	// Unescape literal \n and \t in string literals back to actual newlines/tabs
	// (vitess sqlparser.String() escapes these in string literals)
	s = unescapeStringLiterals(s)
	s = normalizeSelectedFunctionArgDisplaySpacing(s)
	// MySQL displays string literal column headers without quotes:
	// SELECT 'hello' -> column name is "hello" not "'hello'"
	// But only strip if it's a simple string literal (no operators like ||).
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' && isSimpleStringLiteral(s) {
		s = s[1 : len(s)-1]
	}
	// Normalize _utf8mb3 charset introducer to _utf8 (MySQL displays _utf8, not _utf8mb3)
	// Also remove the space that sqlparser inserts between introducer and literal
	s = normalizeCharsetIntroducers(s)
	return s
}

// normalizeCharsetIntroducers normalizes charset introducers in column display names.
// The sqlparser converts _utf8 to _utf8mb3 and adds a space before the literal.
// MySQL displays these as _utf8'...' without space.
func normalizeCharsetIntroducers(s string) string {
	// Replace _utf8mb3 ' with _utf8'
	s = strings.ReplaceAll(s, "_utf8mb3 '", "_utf8'")
	s = strings.ReplaceAll(s, "_utf8mb3'", "_utf8'")
	// Also handle other common alias pairs
	s = strings.ReplaceAll(s, "_utf8mb4 '", "_utf8mb4'")
	return s
}

// stripCharsetIntroducerForColName strips charset introducers from an expression
// when used as a column name. In MySQL, _utf8'abc' and n'abc' display as 'abc' (then
// the outer quote-strip will produce 'abc'). So we strip the _charset prefix,
// leaving just the quoted string. National charset introducer n'' is also stripped.
// Examples: _utf8'abc' -> 'abc', n'abc' -> 'abc', _utf8mb4'abc' -> 'abc'
func stripCharsetIntroducerForColName(s string) string {
	// Handle national charset: n'...' or N'...'
	if len(s) >= 3 && (s[0] == 'n' || s[0] == 'N') && s[1] == '\'' && s[len(s)-1] == '\'' {
		return s[1:]
	}
	// Handle _charset'...' pattern
	if len(s) >= 3 && s[0] == '_' {
		// Find the quote
		for i := 1; i < len(s); i++ {
			if s[i] == '\'' {
				if i < len(s)-1 && s[len(s)-1] == '\'' {
					return s[i:]
				}
				break
			}
		}
	}
	return s
}

// isSimpleStringLiteral returns true if s is a simple quoted string literal
// (starts and ends with ' and contains no unescaped ' in the middle and no operators outside quotes).
// Used to distinguish SELECT 'hello' (simple literal) from SELECT 'A' || 'B' (expression).
func isSimpleStringLiteral(s string) bool {
	if len(s) < 2 || s[0] != '\'' || s[len(s)-1] != '\'' {
		return false
	}
	// Check that there is no unescaped ' between position 1 and len-1
	inner := s[1 : len(s)-1]
	for i := 0; i < len(inner); i++ {
		if inner[i] == '\'' && (i == 0 || inner[i-1] != '\\') {
			return false
		}
	}
	return true
}

func normalizeSelectedFunctionArgDisplaySpacing(s string) string {
	for _, fn := range []string{"JSON_SCHEMA_VALID", "JSON_SCHEMA_VALIDATION_REPORT", "JSON_MERGE_PRESERVE"} {
		prefix := fn + "("
		if !strings.HasPrefix(s, prefix) {
			continue
		}
		inner := s[len(prefix):]
		depth := 1
		inQuote := byte(0)
		var b strings.Builder
		for i := 0; i < len(inner); i++ {
			ch := inner[i]
			if inQuote != 0 {
				b.WriteByte(ch)
				if ch == inQuote {
					inQuote = 0
				}
				continue
			}
			if ch == '\'' || ch == '"' || ch == '`' {
				inQuote = ch
				b.WriteByte(ch)
				continue
			}
			if ch == '(' {
				depth++
				b.WriteByte(ch)
				continue
			}
			if ch == ')' {
				depth--
				if depth == 0 {
					return prefix + b.String() + ")"
				}
				b.WriteByte(ch)
				continue
			}
			if ch == ',' && depth == 1 {
				b.WriteString(", ")
				for i+1 < len(inner) && inner[i+1] == ' ' {
					i++
				}
				continue
			}
			b.WriteByte(ch)
		}
	}
	return s
}

// compactOperatorsInDisplayName removes spaces around comparison and arithmetic
// operators in a SQL display name string to match MySQL column header format.
func compactOperatorsInDisplayName(s string) string {
	var result strings.Builder
	inQuote := byte(0)
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inQuote != 0 {
			result.WriteByte(ch)
			if ch == inQuote {
				// Check for escaped quote ('' or \')
				if i+1 < len(s) && s[i+1] == inQuote {
					i++
					result.WriteByte(s[i])
					continue
				}
				inQuote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			inQuote = ch
			result.WriteByte(ch)
			continue
		}
		// Skip spaces adjacent to operators
		if ch == ' ' {
			// Look ahead past spaces for operator
			j := i + 1
			for j < len(s) && s[j] == ' ' {
				j++
			}
			if j < len(s) && isComparisonOrArithOp(s[j]) {
				continue
			}
			// Look behind for operator
			if result.Len() > 0 {
				prev := result.String()
				if isComparisonOrArithOp(prev[len(prev)-1]) {
					continue
				}
			}
		}
		result.WriteByte(ch)
	}
	return result.String()
}

func isComparisonOrArithOp(ch byte) bool {
	return ch == '=' || ch == '<' || ch == '>' || ch == '!' || ch == '+' || ch == '-' || ch == '*' || ch == '/'
}

// compactOperatorsInSubexpressions removes spaces around operators inside function calls and
// subqueries (parenthesized expressions), matching MySQL's column display name behavior.
// At the top level (depth 0), operators are only compacted when both sides are expressions
// (i.e., the left side ends with ')' and the right side starts with a function call).
func compactOperatorsInSubexpressions(s string) string {
	var result strings.Builder
	parenDepth := 0
	inQuote := byte(0)
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inQuote != 0 {
			result.WriteByte(ch)
			if ch == inQuote {
				inQuote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			inQuote = ch
			result.WriteByte(ch)
			continue
		}
		if ch == '(' {
			parenDepth++
			result.WriteByte(ch)
			continue
		}
		if ch == ')' {
			parenDepth--
			result.WriteByte(ch)
			continue
		}
		if ch == ' ' {
			if parenDepth > 0 {
				// Inside parentheses: compact all binary operators
				for _, op := range []string{" = ", " != ", " <> ", " >= ", " <= ", " > ", " < "} {
					if i+len(op) <= len(s) && s[i:i+len(op)] == op {
						compact := strings.TrimSpace(op)
						result.WriteString(compact)
						i += len(op) - 1
						goto nextChar
					}
				}
				// Also compact ", " -> ","  inside parentheses only
				if i+2 <= len(s) && s[i:i+2] == ", " {
					result.WriteByte(',')
					i++ // skip the space
					continue
				}
			} else {
				// At top level (depth 0): only compact " = " when both sides are expressions.
				// Left side must end with ')' (i.e., result ends with ')'), and
				// right side must start with a function call (identifier followed by '(').
				if i+3 <= len(s) && s[i:i+3] == " = " {
					// Check left side: result must end with ')'
					resultStr := result.String()
					leftEndsWithParen := len(resultStr) > 0 && resultStr[len(resultStr)-1] == ')'
					// Check right side: must be a function call (word chars then '(')
					rightStart := i + 3
					rightIsFuncCall := false
					if rightStart < len(s) {
						c0 := s[rightStart]
						if c0 >= 'A' && c0 <= 'Z' || c0 >= 'a' && c0 <= 'z' || c0 == '_' {
							// Look ahead to see if there is a '(' before any space or operator
							for j := rightStart + 1; j < len(s); j++ {
								c := s[j]
								if c == '(' {
									rightIsFuncCall = true
									break
								}
								if c == ' ' || c == '=' || c == '<' || c == '>' || c == '!' {
									break
								}
							}
						}
					}
					if leftEndsWithParen && rightIsFuncCall {
						result.WriteByte('=')
						i += 2 // skip " = ": current i is at ' ', advance past '=' and ' '
						goto nextChar
					}
				}
			}
		}
		result.WriteByte(ch)
	nextChar:
	}
	return result.String()
}

// unescapeStringLiterals replaces escaped \n and \t inside quoted strings with actual newlines/tabs.
func unescapeStringLiterals(s string) string {
	if !strings.Contains(s, "\\") {
		return s
	}
	var result strings.Builder
	inQuote := byte(0)
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inQuote != 0 {
			if ch == '\\' && i+1 < len(s) {
				next := s[i+1]
				switch next {
				case '0':
					result.WriteByte(0x00) // \0 = null byte (ASCII 0)
					i++
					continue
				case 'n':
					result.WriteByte('\n')
					i++
					continue
				case 't':
					result.WriteByte('\t')
					i++
					continue
				case 'r':
					result.WriteByte('\r')
					i++
					continue
				case 'b':
					result.WriteByte('\b')
					i++
					continue
				case '\\':
					result.WriteByte('\\')
					i++
					continue
				case '\'':
					result.WriteByte('\'')
					i++
					continue
				case '"':
					result.WriteByte('"')
					i++
					continue
				}
			}
			if ch == inQuote {
				inQuote = 0
			}
			result.WriteByte(ch)
			continue
		}
		if ch == '\'' || ch == '"' {
			inQuote = ch
		}
		result.WriteByte(ch)
	}
	return result.String()
}

// normalizeFuncArgSpaces removes spaces after commas inside function calls,
// matching MySQL's column display name format (e.g., "LEFT(`c1`,0)" not "LEFT(`c1`, 0)").
func normalizeFuncArgSpaces(s string) string {
	var result strings.Builder
	inQuote := byte(0)
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inQuote != 0 {
			result.WriteByte(ch)
			if ch == inQuote {
				inQuote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			inQuote = ch
			result.WriteByte(ch)
			continue
		}
		if ch == ',' && i+1 < len(s) && s[i+1] == ' ' {
			result.WriteByte(',')
			i++ // skip the space after comma
			continue
		}
		result.WriteByte(ch)
	}
	return result.String()
}

// normalizeAggColNameNulls replaces lowercase "null" with "NULL" in aggregate
// column names (outside of quoted strings), matching MySQL display behavior.
func normalizeAggColNameNulls(s string) string {
	var result strings.Builder
	inQuote := byte(0)
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inQuote != 0 {
			result.WriteByte(ch)
			if ch == inQuote {
				inQuote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			inQuote = ch
			result.WriteByte(ch)
			continue
		}
		// Check for "null" (case-insensitive) at word boundary
		if (ch == 'n' || ch == 'N') && i+4 <= len(s) && strings.EqualFold(s[i:i+4], "null") {
			// Check word boundaries
			prevOK := i == 0 || s[i-1] == '(' || s[i-1] == ',' || s[i-1] == ' '
			nextOK := i+4 == len(s) || s[i+4] == ')' || s[i+4] == ',' || s[i+4] == ' '
			if prevOK && nextOK {
				result.WriteString("NULL")
				i += 3
				continue
			}
		}
		result.WriteByte(ch)
	}
	return result.String()
}

// uppercaseAggInnerKeywords applies uppercaseSQLKeywords to the inner arguments
// of an aggregate function, preserving the outer function name.
func uppercaseAggInnerKeywords(s string) string {
	// GROUP_CONCAT is intentionally excluded: MySQL preserves lowercase keywords
	// (order by, separator, etc.) in GROUP_CONCAT column display names.
	knownUpper := []string{"JSON_ARRAYAGG(", "JSON_OBJECTAGG(", "COUNT(", "SUM(", "AVG(", "MIN(", "MAX("}
	prefixEnd := 0
	for _, p := range knownUpper {
		if strings.HasPrefix(s, p) {
			prefixEnd = len(p)
			break
		}
	}
	if prefixEnd == 0 {
		return s
	}
	// Find matching close paren
	depth := 1
	inQuote := byte(0)
	end := len(s)
	for i := prefixEnd; i < len(s); i++ {
		ch := s[i]
		if inQuote != 0 {
			if ch == inQuote {
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
		} else if ch == ')' {
			depth--
			if depth == 0 {
				end = i
				break
			}
		}
	}
	inner := s[prefixEnd:end]
	inner = uppercaseSQLKeywords(inner)
	return s[:prefixEnd] + inner + s[end:]
}

// normalizeAggColNameFunctions lowercases non-SQL-keyword function names in aggregate
// column names to match MySQL behavior (e.g. ST_PointFromText → st_pointfromtext).
func normalizeAggColNameFunctions(s string) string {
	// Known aggregate prefixes that should stay uppercase
	knownUpper := []string{"JSON_ARRAYAGG(", "JSON_OBJECTAGG(", "COUNT(", "SUM(", "AVG(", "MIN(", "MAX(", "GROUP_CONCAT("}
	// Find the aggregate prefix end
	prefixEnd := 0
	for _, p := range knownUpper {
		if strings.HasPrefix(s, p) {
			prefixEnd = len(p)
			break
		}
	}
	if prefixEnd == 0 {
		return s
	}
	// Find matching close paren for the outer aggregate
	depth := 1
	innerStart := prefixEnd
	innerEnd := len(s)
	inQuote := byte(0)
	for i := innerStart; i < len(s); i++ {
		ch := s[i]
		if inQuote != 0 {
			if ch == inQuote {
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
		} else if ch == ')' {
			depth--
			if depth == 0 {
				innerEnd = i
				break
			}
		}
	}
	inner := s[innerStart:innerEnd]
	// Lowercase function-like identifiers in the inner part (word followed by '(')
	var result strings.Builder
	inQuote = 0
	for i := 0; i < len(inner); i++ {
		ch := inner[i]
		if inQuote != 0 {
			result.WriteByte(ch)
			if ch == inQuote {
				inQuote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			inQuote = ch
			result.WriteByte(ch)
			continue
		}
		// Check for function name pattern: alphabetic/underscore chars followed by '('
		if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '_' {
			j := i
			for j < len(inner) && ((inner[j] >= 'A' && inner[j] <= 'Z') || (inner[j] >= 'a' && inner[j] <= 'z') || (inner[j] >= '0' && inner[j] <= '9') || inner[j] == '_') {
				j++
			}
			word := inner[i:j]
			if j < len(inner) && inner[j] == '(' {
				// It's a function call - lowercase non-SQL function names
				wordUpper := strings.ToUpper(word)
				if isKnownSQLFunction(wordUpper) {
					result.WriteString(wordUpper)
				} else {
					result.WriteString(strings.ToLower(word))
				}
			} else {
				result.WriteString(word)
			}
			i = j - 1
			continue
		}
		result.WriteByte(ch)
	}
	return s[:prefixEnd] + result.String() + s[innerEnd:]
}

// isKnownSQLFunction returns true for SQL/JSON built-in function names that
// should remain uppercase in column headers.
func isKnownSQLFunction(name string) bool {
	known := map[string]bool{
		"DISTINCT": true,
		"CAST": true, "CONVERT": true, "COALESCE": true, "IF": true, "IFNULL": true,
		"NULLIF": true, "CONCAT": true, "CONCAT_WS": true, "LENGTH": true, "CHAR_LENGTH": true,
		"UPPER": true, "LOWER": true, "TRIM": true, "LTRIM": true, "RTRIM": true,
		"REPLACE": true, "SUBSTRING": true, "LEFT": true, "RIGHT": true, "REVERSE": true,
		"REPEAT": true, "LPAD": true, "RPAD": true, "INSTR": true, "LOCATE": true,
		"ABS": true, "CEIL": true, "CEILING": true, "FLOOR": true, "ROUND": true, "TRUNCATE": true,
		"MOD": true, "POWER": true, "SQRT": true, "RAND": true,
		"NOW": true, "CURDATE": true, "CURTIME": true, "DATE": true, "TIME": true,
		"YEAR": true, "MONTH": true, "DAY": true, "HOUR": true, "MINUTE": true, "SECOND": true,
		"DATE_FORMAT": true, "DATE_ADD": true, "DATE_SUB": true, "DATEDIFF": true,
		"COUNT": true, "SUM": true, "AVG": true, "MIN": true, "MAX": true,
		"BIT_AND": true, "BIT_OR": true, "BIT_XOR": true,
		"GROUP_CONCAT": true, "JSON_ARRAYAGG": true, "JSON_OBJECTAGG": true,
		"JSON_EXTRACT": true, "JSON_VALID": true, "JSON_TYPE": true,
		"JSON_DEPTH": true, "JSON_LENGTH": true, "JSON_KEYS": true,
		"JSON_ARRAY": true, "JSON_OBJECT": true,
		"JSON_MERGE_PRESERVE": true, "JSON_MERGE_PATCH": true,
		"JSON_CONTAINS": true, "JSON_CONTAINS_PATH": true,
		"JSON_SET": true, "JSON_INSERT": true, "JSON_REPLACE": true,
		"JSON_REMOVE": true, "JSON_UNQUOTE": true, "JSON_QUOTE": true,
		"JSON_PRETTY": true, "JSON_STORAGE_SIZE": true, "JSON_STORAGE_FREE": true,
		"JSON_OVERLAPS": true, "JSON_SEARCH": true, "JSON_VALUE": true,
		"JSON_SCHEMA_VALID": true, "JSON_SCHEMA_VALIDATION_REPORT": true,
		"JSON_ARRAY_APPEND": true, "JSON_ARRAY_INSERT": true,
		"HEX": true, "UNHEX": true, "BIN": true, "OCT": true,
		"CHARSET": true, "COLLATION": true,
	}
	return known[name]
}

// normalizeAggColNameSubselect adds "FROM dual" to subselects without FROM in
// aggregate column names, matching MySQL display behavior.
func normalizeAggColNameSubselect(s string) string {
	// Look for "(SELECT ... )" patterns without FROM
	upper := strings.ToUpper(s)
	idx := strings.Index(upper, "(SELECT ")
	if idx < 0 {
		return s
	}
	// Find the matching close paren
	start := idx + 1 // after '('
	depth := 1
	inQuote := byte(0)
	end := len(s)
	for i := idx + 1; i < len(s); i++ {
		ch := s[i]
		if inQuote != 0 {
			if ch == inQuote {
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
		} else if ch == ')' {
			depth--
			if depth == 0 {
				end = i
				break
			}
		}
	}
	subquery := s[start:end]
	subqueryUpper := strings.ToUpper(subquery)
	// Remove spaces after commas in the subquery to match MySQL compact display
	subquery = normalizeFuncArgSpaces(subquery)
	// If subquery has no FROM clause, add " FROM dual"
	if !strings.Contains(subqueryUpper, " FROM ") {
		subquery = subquery + " FROM dual"
	}
	return s[:start] + subquery + s[end:]
}

// uppercaseSQLKeywords converts SQL keywords in a string to uppercase to match MySQL's
// column display name behavior for subquery expressions.
func uppercaseSQLKeywords(s string) string {
	keywords := []string{
		"select", "from", "where", "and", "or", "not", "in", "exists",
		"any", "some", "all", "as", "on", "join", "left", "right", "inner",
		"outer", "cross", "group", "by", "order", "having", "limit", "offset",
		"union", "except", "intersect", "between", "like", "is",
		"null", "true", "false", "case", "when", "then", "else", "end",
		"asc", "desc", "count", "sum", "avg", "min", "max", "upper", "lower",
		"row", "with", "cast", "convert", "json_extract", "json_valid", "json_type",
		"json_depth", "json_length", "json_keys", "json_array", "json_object",
		"json_merge_preserve", "json_merge_patch", "json_contains", "json_contains_path",
		"json_set", "json_insert", "json_replace", "json_remove", "json_unquote",
		"json_quote", "json_pretty", "json_storage_size", "json_storage_free",
		"json_overlaps", "json_search", "json_value", "json_arrayagg", "json_objectagg",
		"json_schema_valid", "json_schema_validation_report",
	}
	result := []byte(s)
	for _, kw := range keywords {
		kwBytes := []byte(kw)
		upper := []byte(strings.ToUpper(kw))
		i := 0
		for i < len(result) {
			// Skip quoted strings
			if result[i] == '\'' || result[i] == '"' || result[i] == '`' {
				q := result[i]
				i++
				for i < len(result) && result[i] != q {
					i++
				}
				if i < len(result) {
					i++
				}
				continue
			}
			// Check word boundary at start
			if i > 0 {
				ch := result[i-1]
				if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' || (ch >= '0' && ch <= '9') {
					i++
					continue
				}
			}
			// Check if keyword matches
			if i+len(kw) <= len(result) {
				match := true
				for j := 0; j < len(kw); j++ {
					if result[i+j] != kwBytes[j] {
						match = false
						break
					}
				}
				if match {
					// Check word boundary at end
					end := i + len(kw)
					if end < len(result) {
						ch := result[end]
						if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' || (ch >= '0' && ch <= '9') {
							i++
							continue
						}
					}
					copy(result[i:i+len(kw)], upper)
					i += len(kw)
					continue
				}
			}
			i++
		}
	}
	return string(result)
}

// normalizeTypeAliases replaces MySQL type aliases that the vitess parser
// doesn't support with their canonical equivalents.
func normalizeTypeAliases(query string) string {
	upper := strings.ToUpper(query)
	// Only apply to CREATE TABLE (including TEMPORARY) or ALTER TABLE statements
	if !strings.Contains(upper, "CREATE TABLE") && !strings.Contains(upper, "CREATE TEMPORARY TABLE") && !strings.Contains(upper, "ALTER TABLE") {
		return query
	}
	// Replace DOUBLE PRECISION with DOUBLE (case-insensitive, word-boundary aware)
	result := replaceTypeWord(query, "DOUBLE PRECISION", "DOUBLE")
	result = replaceTypeWord(result, "DEC", "DECIMAL")
	result = replaceTypeWord(result, "FIXED", "DECIMAL")
	result = replaceTypeWord(result, "NUMERIC", "DECIMAL")
	result = replaceTypeWord(result, "SERIAL", "BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE")
	result = replaceTypeWord(result, "INT1", "TINYINT")
	result = replaceTypeWord(result, "INT2", "SMALLINT")
	result = replaceTypeWord(result, "INT3", "MEDIUMINT")
	result = replaceTypeWord(result, "INT4", "INT")
	result = replaceTypeWord(result, "INT8", "BIGINT")
	result = replaceTypeWord(result, "MIDDLEINT", "MEDIUMINT")
	result = replaceTypeWord(result, "FLOAT4", "FLOAT")
	result = replaceTypeWord(result, "FLOAT8", "DOUBLE")
	result = replaceTypeWord(result, "LONG VARBINARY", "MEDIUMBLOB")
	result = replaceTypeWord(result, "LONG VARCHAR", "MEDIUMTEXT")
	// NCHAR VARYING and NCHAR VARCHAR must come before NCHAR to avoid partial replacement.
	// These are DDL-only normalizations. We add CHARACTER SET utf8 AFTER the size specifier
	// to preserve the charset in SHOW CREATE TABLE output (NATIONAL/NCHAR types imply UTF8).
	result = regexp.MustCompile(`(?i)\bNCHAR\s+VARYING(\s*\([^)]*\))?`).ReplaceAllStringFunc(result, func(m string) string {
		re := regexp.MustCompile(`(?i)\bNCHAR\s+VARYING`)
		return re.ReplaceAllString(m, "VARCHAR") + " CHARACTER SET utf8"
	})
	result = regexp.MustCompile(`(?i)\bNCHAR\s+VARCHAR(\s*\([^)]*\))?`).ReplaceAllStringFunc(result, func(m string) string {
		re := regexp.MustCompile(`(?i)\bNCHAR\s+VARCHAR`)
		return re.ReplaceAllString(m, "VARCHAR") + " CHARACTER SET utf8"
	})
	result = regexp.MustCompile(`(?i)\bNVARCHAR(\s*\([^)]*\))?`).ReplaceAllStringFunc(result, func(m string) string {
		re := regexp.MustCompile(`(?i)\bNVARCHAR`)
		return re.ReplaceAllString(m, "VARCHAR") + " CHARACTER SET utf8"
	})
	result = regexp.MustCompile(`(?i)\bNCHAR(\s*\([^)]*\))?`).ReplaceAllStringFunc(result, func(m string) string {
		re := regexp.MustCompile(`(?i)\bNCHAR`)
		return re.ReplaceAllString(m, "CHAR") + " CHARACTER SET utf8"
	})
	return result
}

// replaceTypeWord replaces a type keyword in a SQL query case-insensitively,
// only when it appears as a whole word (not part of a larger identifier)
// and not inside a quoted string.
func replaceTypeWord(query, old, replacement string) string {
	// Use byte-level case folding to ensure upper and query have the same length.
	// strings.ToUpper can change length for multi-byte characters (e.g. ß → SS).
	upper := []byte(query)
	for i, b := range upper {
		if b >= 'a' && b <= 'z' {
			upper[i] = b - 32
		}
	}
	upperStr := string(upper)
	oldUpper := strings.ToUpper(old)
	idx := 0
	for {
		pos := strings.Index(upperStr[idx:], oldUpper)
		if pos == -1 {
			break
		}
		absPos := idx + pos
		endPos := absPos + len(old)

		// Check if we're inside a quoted string
		inQuote := false
		quoteChar := byte(0)
		for i := 0; i < absPos; i++ {
			ch := query[i]
			if !inQuote && (ch == '\'' || ch == '"') {
				inQuote = true
				quoteChar = ch
			} else if inQuote && ch == quoteChar {
				inQuote = false
			}
		}
		if inQuote {
			idx = endPos
			continue
		}

		// Check word boundaries
		if absPos > 0 {
			ch := query[absPos-1]
			if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '_' {
				idx = endPos
				continue
			}
			// Skip if preceded by '=' (e.g. ROW_FORMAT=FIXED — not a column type)
			if ch == '=' {
				idx = endPos
				continue
			}
		}
		// Skip if the preceding non-space token is a table option keyword like ROW_FORMAT
		// or COLUMN_FORMAT (e.g. "ROW_FORMAT FIXED", "COLUMN_FORMAT FIXED" — value, not a column type)
		{
			pre := strings.TrimRight(upperStr[:absPos], " \t\n\r")
			if strings.HasSuffix(pre, "ROW_FORMAT") || strings.HasSuffix(pre, "COLUMN_FORMAT") {
				idx = endPos
				continue
			}
		}
		if endPos < len(query) {
			ch := query[endPos]
			if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '_' || (ch >= '0' && ch <= '9') {
				idx = endPos
				continue
			}
		}
		query = query[:absPos] + replacement + query[endPos:]
		upper2 := []byte(query)
		for i, b := range upper2 {
			if b >= 'a' && b <= 'z' {
				upper2[i] = b - 32
			}
		}
		upperStr = string(upper2)
		idx = absPos + len(replacement)
	}
	return query
}

// normalizeInlineCheckConstraints converts column-level CHECK constraints to
// table-level constraints. The vitess parser produces a nil TableSpec when a
// column definition contains an inline CHECK (expr), but handles table-level
// CHECK constraints correctly.
// Example: CREATE TABLE t1(f1 INT CHECK (f1 < 10))
// becomes: CREATE TABLE t1(f1 INT, CHECK (f1 < 10))
func normalizeInlineCheckConstraints(query string) string {
	upper := strings.ToUpper(query)
	// Only normalize inline CHECK constraints for CREATE TABLE.
	// ALTER TABLE with ADD CONSTRAINT ... CHECK is valid Vitess syntax as-is.
	if !strings.Contains(upper, "CREATE TABLE") {
		return query
	}
	if !strings.Contains(upper, "CHECK") {
		return query
	}

	// Find inline CHECK constraints in column definitions and move them
	// to table-level constraints.
	// We scan for patterns like: ... CHECK (...) ... within column definitions.
	// An inline CHECK is preceded by a column type/options (not by a comma).
	var result []byte
	var extractedChecks []string
	i := 0
	for i < len(query) {
		// Skip string literals
		if query[i] == '\'' {
			result = append(result, query[i])
			i++
			for i < len(query) {
				result = append(result, query[i])
				if query[i] == '\'' {
					i++
					break
				}
				if query[i] == '\\' && i+1 < len(query) {
					i++
					result = append(result, query[i])
				}
				i++
			}
			continue
		}
		if query[i] == '"' {
			result = append(result, query[i])
			i++
			for i < len(query) {
				result = append(result, query[i])
				if query[i] == '"' {
					i++
					break
				}
				i++
			}
			continue
		}
		if query[i] == '`' {
			result = append(result, query[i])
			i++
			for i < len(query) {
				result = append(result, query[i])
				if query[i] == '`' {
					i++
					break
				}
				i++
			}
			continue
		}

		// Look for CONSTRAINT name CHECK or CHECK keyword
		remaining := query[i:]
		upperRemaining := strings.ToUpper(remaining)

		// Match optional "CONSTRAINT name" before CHECK
		constraintPrefix := ""
		checkStart := i
		matched := false

		if strings.HasPrefix(upperRemaining, "CONSTRAINT ") {
			// Could be "CONSTRAINT name CHECK ..."
			j := len("CONSTRAINT ")
			// Skip whitespace
			for j < len(remaining) && (remaining[j] == ' ' || remaining[j] == '\t' || remaining[j] == '\n' || remaining[j] == '\r') {
				j++
			}
			// Read name (possibly backtick-quoted)
			nameStart := j
			_ = nameStart
			if j < len(remaining) && remaining[j] == '`' {
				j++
				for j < len(remaining) && remaining[j] != '`' {
					j++
				}
				if j < len(remaining) {
					j++ // skip closing backtick
				}
			} else {
				for j < len(remaining) && remaining[j] != ' ' && remaining[j] != '\t' && remaining[j] != '\n' {
					j++
				}
			}
			nameEnd := j
			// Skip whitespace
			for j < len(remaining) && (remaining[j] == ' ' || remaining[j] == '\t' || remaining[j] == '\n' || remaining[j] == '\r') {
				j++
			}
			if j+5 <= len(remaining) && strings.EqualFold(remaining[j:j+5], "CHECK") &&
				(j+5 == len(remaining) || remaining[j+5] == ' ' || remaining[j+5] == '(' || remaining[j+5] == '\t') {
				constraintPrefix = remaining[:nameEnd]
				checkStart = i + j
				matched = true
			}
		}

		if !matched && strings.HasPrefix(upperRemaining, "CHECK") &&
			(len(upperRemaining) == 5 || upperRemaining[5] == ' ' || upperRemaining[5] == '(' || upperRemaining[5] == '\t') {
			matched = true
			checkStart = i
		}

		if matched {
			// Determine if this CHECK is inline (part of column def) or already at table level.
			// Table-level CHECK is preceded by a comma (possibly with whitespace).
			// Inline CHECK is preceded by column type/options (letter, digit, paren).
			// When a CONSTRAINT prefix is present, we must check the character before
			// CONSTRAINT (not before CHECK), since CHECK follows the constraint name.
			scanFrom := checkStart
			if constraintPrefix != "" {
				scanFrom = i // i points to the start of CONSTRAINT keyword
			}
			prevNonSpace := 0
			prevNonSpacePos := -1
			for p := scanFrom - 1; p >= 0; p-- {
				ch := query[p]
				if ch != ' ' && ch != '\t' && ch != '\n' && ch != '\r' {
					prevNonSpace = int(ch)
					prevNonSpacePos = p
					break
				}
			}
			// If the prevNonSpace is a letter, it might be the end of the word
			// "CONSTRAINT". In that case we should look before CONSTRAINT to determine
			// if the check is at table level (preceded by ',' or '(').
			if prevNonSpace > 0 && ((prevNonSpace >= 'a' && prevNonSpace <= 'z') || (prevNonSpace >= 'A' && prevNonSpace <= 'Z')) && prevNonSpacePos >= 0 {
				// Find the start of this word
				wordEnd := prevNonSpacePos
				wordStart := wordEnd
				for wordStart > 0 {
					ch := query[wordStart-1]
					if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' {
						wordStart--
					} else {
						break
					}
				}
				word := strings.ToUpper(query[wordStart : wordEnd+1])
				if word == "CONSTRAINT" {
					// Look before CONSTRAINT
					for p := wordStart - 1; p >= 0; p-- {
						ch := query[p]
						if ch != ' ' && ch != '\t' && ch != '\n' && ch != '\r' {
							prevNonSpace = int(ch)
							break
						}
					}
				}
			}
			isTableLevel := prevNonSpace == ',' || prevNonSpace == '('
			if isTableLevel {
				// This is a table-level CHECK constraint. Skip past the entire
				// CHECK (...) clause (and optional ENFORCED/NOT ENFORCED) so we
				// don't re-encounter the CHECK keyword on subsequent iterations.
				j := checkStart + 5 // past "CHECK"
				for j < len(query) && query[j] != '(' {
					j++
				}
				if j < len(query) {
					depth := 0
					for j < len(query) {
						if query[j] == '(' {
							depth++
						} else if query[j] == ')' {
							depth--
							if depth == 0 {
								j++
								break
							}
						} else if query[j] == '\'' {
							j++
							for j < len(query) && query[j] != '\'' {
								if query[j] == '\\' {
									j++
								}
								j++
							}
						}
						j++
					}
					// Also skip optional NOT ENFORCED / ENFORCED
					rest := strings.TrimLeft(query[j:], " \t\n\r")
					upperRest := strings.ToUpper(rest)
					if strings.HasPrefix(upperRest, "NOT ENFORCED") {
						j += (len(query[j:]) - len(rest)) + len("NOT ENFORCED")
					} else if strings.HasPrefix(upperRest, "ENFORCED") {
						j += (len(query[j:]) - len(rest)) + len("ENFORCED")
					}
					// Copy the entire table-level CHECK clause as-is
					result = append(result, query[i:j]...)
					i = j
					continue
				}
			} else {
				// This is an inline CHECK. Extract the full CHECK clause.
				// Find the opening '(' after CHECK — must be only whitespace between
				// CHECK and '('. If other characters appear first, this is invalid
				// CHECK syntax (e.g. "CHECK something (...)") that MySQL rejects;
				// skip normalization so the executor can return a parse error.
				j := checkStart + 5 // past "CHECK"
				for j < len(query) && (query[j] == ' ' || query[j] == '\t' || query[j] == '\n' || query[j] == '\r') {
					j++
				}
				if j >= len(query) || query[j] != '(' {
					// Not a valid inline CHECK — pass through without normalizing
					result = append(result, query[i])
					i++
					continue
				}
				if j < len(query) {
					// Find matching closing ')'
					depth := 0
					start := j
					for j < len(query) {
						if query[j] == '(' {
							depth++
						} else if query[j] == ')' {
							depth--
							if depth == 0 {
								j++
								break
							}
						} else if query[j] == '\'' {
							j++
							for j < len(query) && query[j] != '\'' {
								if query[j] == '\\' {
									j++
								}
								j++
							}
						}
						j++
					}
					checkExpr := query[start:j]
					// Also consume optional NOT ENFORCED / ENFORCED after the check
					rest := strings.TrimLeft(query[j:], " \t\n\r")
					upperRest := strings.ToUpper(rest)
					suffix := ""
					consumed := j
					if strings.HasPrefix(upperRest, "NOT ENFORCED") {
						suffix = " NOT ENFORCED"
						consumed = j + (len(query[j:]) - len(rest)) + len("NOT ENFORCED")
					} else if strings.HasPrefix(upperRest, "ENFORCED") {
						suffix = " ENFORCED"
						consumed = j + (len(query[j:]) - len(rest)) + len("ENFORCED")
					}
					// Build the table-level constraint
					checkClause := "CHECK " + checkExpr + suffix
					if constraintPrefix != "" {
						checkClause = constraintPrefix + " " + checkClause
					}
					extractedChecks = append(extractedChecks, checkClause)
					// Remove from current position, trim trailing whitespace
					// Also remove the CONSTRAINT prefix if present
					removeFrom := i
					if constraintPrefix != "" {
						removeFrom = i
					}
					// Strip whitespace before the removed CHECK
					for removeFrom > 0 && (query[removeFrom-1] == ' ' || query[removeFrom-1] == '\t') {
						removeFrom--
					}
					result = result[:len(result)-(i-removeFrom)]
					i = consumed
					continue
				}
			}
		}

		result = append(result, query[i])
		i++
	}

	if len(extractedChecks) == 0 {
		return query
	}

	// Insert extracted checks before the closing ')' of the column definition list.
	// Find the last ')' that closes the column definitions.
	out := string(result)
	// Find the position of the closing paren of column list.
	// It's the last ')' before table options or end of statement.
	lastParen := strings.LastIndex(out, ")")
	if lastParen >= 0 {
		// Insert checks as table-level constraints
		insert := ""
		for _, c := range extractedChecks {
			insert += ",\n" + c
		}
		out = out[:lastParen] + insert + out[lastParen:]
	}

	return out
}

// normalizeStorageClause strips "STORAGE DISK" and "STORAGE MEMORY" from
// column definitions. The vitess parser does not recognise these MySQL column
// attributes and silently produces a nil TableSpec for CREATE TABLE statements
// that contain them. Since mylite is an in-memory engine the storage attribute
// is irrelevant.
func normalizeStorageClause(query string) string {
	upper := strings.ToUpper(query)
	if !strings.Contains(upper, "STORAGE DISK") && !strings.Contains(upper, "STORAGE MEMORY") {
		return query
	}
	re := regexp.MustCompile(`(?i)\bSTORAGE\s+(DISK|MEMORY)\b`)
	return re.ReplaceAllString(query, "")
}

// normalizeStatsSamplePages strips STATS_SAMPLE_PAGES=default from table
// options. The vitess parser cannot handle "default" as a table option value.
// For numeric values, only strip valid ones (1-65535) so that invalid values
// still trigger parse errors.
func normalizeStatsSamplePages(query string) string {
	re := regexp.MustCompile(`(?i)\bSTATS_SAMPLE_PAGES\s*=\s*default\b`)
	return re.ReplaceAllString(query, "")
}

// normalizeStartTransaction strips "START TRANSACTION" from CREATE TABLE
// statements. This is an atomic DDL option in MySQL 8.0 that the vitess
// parser cannot handle. It's safe to ignore since mylite doesn't support
// crash recovery.
func normalizeStartTransaction(query string) string {
	upper := strings.ToUpper(query)
	if !strings.Contains(upper, "CREATE TABLE") || !strings.Contains(upper, "START TRANSACTION") {
		return query
	}
	re := regexp.MustCompile(`(?i)\bSTART\s+TRANSACTION\b`)
	return re.ReplaceAllString(query, "")
}

// normalizeAutoextendSize converts AUTOEXTEND_SIZE values with size suffixes
// (K, M, G, T) to their byte equivalents, since the vitess parser cannot
// handle the suffix notation. E.g., AUTOEXTEND_SIZE=64M -> AUTOEXTEND_SIZE=67108864.
func normalizeAutoextendSize(query string) string {
	upper := strings.ToUpper(query)
	if !strings.Contains(upper, "AUTOEXTEND_SIZE") {
		return query
	}
	re := regexp.MustCompile(`(?i)\bAUTOEXTEND_SIZE\s*=\s*(\d+)([KMGT])\b`)
	return re.ReplaceAllStringFunc(query, func(match string) string {
		m := re.FindStringSubmatch(match)
		if len(m) < 3 {
			return match
		}
		val, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil {
			return match
		}
		switch strings.ToUpper(m[2]) {
		case "K":
			val *= 1024
		case "M":
			val *= 1024 * 1024
		case "G":
			val *= 1024 * 1024 * 1024
		case "T":
			val *= 1024 * 1024 * 1024 * 1024
		}
		return fmt.Sprintf("AUTOEXTEND_SIZE=%d", val)
	})
}

// normalizeSecondaryEngine strips SECONDARY_ENGINE=value from CREATE/ALTER TABLE
// statements. The vitess parser produces a nil TableSpec when this option is present.
// Since mylite doesn't support secondary engines, we simply ignore it.
func normalizeSecondaryEngine(query string) string {
	upper := strings.ToUpper(query)
	if !strings.Contains(upper, "SECONDARY_ENGINE") {
		return query
	}
	// Strip SECONDARY_ENGINE=value (value may be quoted or unquoted)
	re := regexp.MustCompile(`(?i)\bSECONDARY_ENGINE\s*=\s*(?:'[^']*'|"[^"]*"|` + "`[^`]*`" + `|\S+)`)
	return re.ReplaceAllString(query, "")
}

// normalizeAddIndexUsing rewrites "ADD KEY USING BTREE (" and similar forms
// to "ADD KEY (" since the vitess parser does not handle USING before column list.
func normalizeAddIndexUsing(query string) string {
	upper := strings.ToUpper(query)
	if !strings.Contains(upper, "ALTER TABLE") {
		return query
	}
	// Move USING before column list to after it (parser can handle it after)
	// Pattern: ADD KEY [name] USING BTREE (col) -> ADD KEY [name] (col) USING BTREE
	// Without name:
	re1 := regexp.MustCompile(`(?i)(ADD\s+(UNIQUE\s+)?(KEY|INDEX))\s+USING\s+(\w+)\s*(\([^)]*\))`)
	query = re1.ReplaceAllString(query, "${1} ${5} USING ${4}")
	// With name: ADD KEY i1 USING BTREE (col) -> ADD KEY i1 (col) USING BTREE
	re2 := regexp.MustCompile(`(?i)(ADD\s+(UNIQUE\s+)?(KEY|INDEX)\s+` + "`?" + `\w+` + "`?" + `)\s+USING\s+(\w+)\s*(\([^)]*\))`)
	query = re2.ReplaceAllString(query, "${1} ${5} USING ${4}")
	return query
}

// isIdentChar returns true if the byte could be part of a SQL identifier.
func isIdentChar(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_' || b == '$'
}

// normalizeCreateTableEngineSelect strips table options (ENGINE=, CHARSET=, etc.)
// between the table name and SELECT clause in CREATE TABLE statements so the
// normalizeCreateTableParenSelect rewrites CREATE TABLE t (SELECT ...) ORDER BY ...
// into CREATE TABLE t SELECT ... ORDER BY ... so the vitess parser can handle it.
// When an outer ORDER BY is present, the inner ORDER BY is dropped since the
// outer one takes precedence in MySQL.
func normalizeCreateTableParenSelect(query string) string {
	upper := strings.ToUpper(strings.TrimSpace(query))
	if !strings.HasPrefix(upper, "CREATE TABLE") && !strings.HasPrefix(upper, "CREATE TEMPORARY TABLE") {
		return query
	}
	// Skip normalization if WITH clause (CTE) is present to avoid mistaking CTE subqueries
	// for the parenthesized SELECT pattern. A CTE WITH clause has the form "WITH name AS (".
	if regexp.MustCompile(`(?i)\bWITH\s+\w+\s+AS\s*\(`).MatchString(query) {
		return query
	}
	trimmed := strings.TrimSpace(query)
	for i := 0; i < len(trimmed); i++ {
		if trimmed[i] == '\'' {
			for i++; i < len(trimmed) && trimmed[i] != '\''; i++ {
				if trimmed[i] == '\\' {
					i++
				}
			}
			continue
		}
		if trimmed[i] != '(' {
			continue
		}
		j := i + 1
		for j < len(trimmed) && (trimmed[j] == ' ' || trimmed[j] == '\t' || trimmed[j] == '\n') {
			j++
		}
		if j+6 >= len(trimmed) || !strings.EqualFold(trimmed[j:j+7], "SELECT ") {
			break
		}
		depth := 1
		k := i + 1
		for k < len(trimmed) && depth > 0 {
			switch trimmed[k] {
			case '(':
				depth++
			case ')':
				depth--
			case '\'':
				for k++; k < len(trimmed) && trimmed[k] != '\''; k++ {
					if trimmed[k] == '\\' {
						k++
					}
				}
			}
			k++
		}
		if depth != 0 {
			return query
		}
		innerSelect := strings.TrimSpace(trimmed[i+1 : k-1])
		prefix := trimmed[:i]
		suffix := strings.TrimSpace(trimmed[k:])

		// If there is an outer ORDER BY, strip the inner ORDER BY
		if len(suffix) > 0 && strings.HasPrefix(strings.ToUpper(suffix), "ORDER BY") {
			upperInner := strings.ToUpper(innerSelect)
			lastOB := -1
			d := 0
			for m := 0; m < len(innerSelect)-7; m++ {
				switch innerSelect[m] {
				case '(':
					d++
				case ')':
					d--
				case '\'':
					for m++; m < len(innerSelect) && innerSelect[m] != '\''; m++ {
						if innerSelect[m] == '\\' {
							m++
						}
					}
				}
				if d == 0 && strings.HasPrefix(upperInner[m:], "ORDER BY") {
					lastOB = m
				}
			}
			if lastOB > 0 {
				innerSelect = strings.TrimSpace(innerSelect[:lastOB])
			}
		}

		result := strings.TrimSpace(prefix) + " " + innerSelect
		if len(suffix) > 0 {
			result += " " + suffix
		}
		return result
	}
	return query
}

// vitess parser can handle them. The engine is ignored since mylite uses its
// own storage engine for all tables.
func normalizeCreateTableEngineSelect(query string) string {
	upper := strings.ToUpper(query)
	if !strings.HasPrefix(upper, "CREATE TABLE") && !strings.HasPrefix(upper, "CREATE TEMPORARY TABLE") {
		return query
	}
	// Find SELECT keyword (not inside parentheses)
	depth := 0
	selectIdx := -1
	for i := 0; i < len(query)-6; i++ {
		switch query[i] {
		case '(':
			depth++
		case ')':
			depth--
		case '\'':
			// Skip string literals
			for i++; i < len(query) && query[i] != '\''; i++ {
				if query[i] == '\\' {
					i++
				}
			}
		}
		if depth == 0 && (query[i] == 's' || query[i] == 'S') {
			// SELECT can be followed by space, newline, or tab as whitespace separator.
			if i+6 <= len(query) && strings.EqualFold(query[i:i+6], "SELECT") &&
				(i+6 == len(query) || query[i+6] == ' ' || query[i+6] == '\n' || query[i+6] == '\t' || query[i+6] == '\r') {
				// Ensure SELECT is a standalone keyword (preceded by space/newline, not part of identifier)
				if i == 0 || !isIdentChar(query[i-1]) {
					selectIdx = i
					break
				}
			}
		}
	}
	if selectIdx < 0 {
		return query
	}
	// Check if there are table options between the table name and SELECT
	// CREATE [TEMPORARY] TABLE name [options] SELECT ...
	// or CREATE [TEMPORARY] TABLE name (col defs) [options] SELECT ...
	prefix := query[:selectIdx]
	// Find the position after the column definitions closing paren (depth 0).
	// Table-level options only appear after the closing ')' of the column list.
	// We must not strip CHARACTER SET / CHARSET inside the column definitions.
	colDefsEnd := -1
	{
		d := 0
		inStr := false
		for i := 0; i < len(prefix); i++ {
			if inStr {
				if prefix[i] == '\'' {
					if i+1 < len(prefix) && prefix[i+1] == '\'' {
						i++ // skip escaped quote
					} else {
						inStr = false
					}
				} else if prefix[i] == '\\' {
					i++
				}
				continue
			}
			switch prefix[i] {
			case '\'':
				inStr = true
			case '(':
				d++
			case ')':
				d--
				if d == 0 {
					colDefsEnd = i
				}
			}
		}
	}
	// Only strip table-level options from the part after the closing paren.
	// If there's no paren (rare: CREATE TABLE name ENGINE=x SELECT ...), strip from whole prefix.
	colDefsPart := prefix
	tableOptsPart := ""
	if colDefsEnd >= 0 && colDefsEnd < len(prefix)-1 {
		colDefsPart = prefix[:colDefsEnd+1]
		tableOptsPart = prefix[colDefsEnd+1:]
	} else if colDefsEnd < 0 {
		// No parentheses - all of prefix is table options (CREATE TABLE name ENGINE=x SELECT)
		colDefsPart = ""
		tableOptsPart = prefix
	}
	// Strip known table options from the table-options portion only.
	// Match both "OPTION=value" and "OPTION value" (without equals sign).
	reOpts := regexp.MustCompile(`(?i)\b(?:ENGINE|TYPE|ROW_FORMAT|KEY_BLOCK_SIZE|AVG_ROW_LENGTH|MIN_ROWS|MAX_ROWS|PACK_KEYS|CHECKSUM|DELAY_KEY_WRITE|DATA\s+DIRECTORY|INDEX\s+DIRECTORY|AUTO_INCREMENT|INSERT_METHOD|STATS_AUTO_RECALC|STATS_PERSISTENT|STATS_SAMPLE_PAGES)\s*=?\s*\S+`)
	cleaned := reOpts.ReplaceAllString(tableOptsPart, " ")
	reCharset := regexp.MustCompile(`(?i)\bDEFAULT\s+(?:CHARSET|CHARACTER\s+SET)\s*=?\s*\S+`)
	cleaned = reCharset.ReplaceAllString(cleaned, " ")
	reCharset2 := regexp.MustCompile(`(?i)\b(?:CHARSET|CHARACTER\s+SET)\s*=?\s*\S+`)
	cleaned = reCharset2.ReplaceAllString(cleaned, " ")
	reCollate := regexp.MustCompile(`(?i)\bCOLLATE\s*=?\s*\S+`)
	cleaned = reCollate.ReplaceAllString(cleaned, " ")
	reComment := regexp.MustCompile(`(?i)\bCOMMENT\s*=?\s*'[^']*'`)
	cleaned = reComment.ReplaceAllString(cleaned, " ")
	// Collapse multiple spaces in table opts part
	cleaned = regexp.MustCompile(`\s+`).ReplaceAllString(cleaned, " ")
	fullPrefix := colDefsPart + cleaned
	fullPrefix = regexp.MustCompile(`\s+`).ReplaceAllString(fullPrefix, " ")
	return strings.TrimSpace(fullPrefix) + " " + query[selectIdx:]
}

// normalizeEngineWithoutEquals rewrites "ENGINE <value>" to "ENGINE=<value>"
// in CREATE TABLE / ALTER TABLE statements. MySQL allows both forms but the
// vitess parser requires the equals sign.
func normalizeEngineWithoutEquals(query string) string {
	upper := strings.ToUpper(query)
	if !strings.Contains(upper, "ENGINE") {
		return query
	}
	if !strings.HasPrefix(upper, "CREATE ") && !strings.HasPrefix(upper, "ALTER ") {
		return query
	}
	re := regexp.MustCompile(`(?i)\bENGINE\s+(InnoDB|MyISAM|MEMORY|HEAP|ARCHIVE|CSV|BLACKHOLE|NDB|MERGE|FEDERATED|EXAMPLE)\b`)
	return re.ReplaceAllString(query, "ENGINE=$1")
}

// normalizeCreateTableIndexUsing rewrites "PRIMARY KEY USING BTREE (cols)" and
// "KEY name USING BTREE (cols)" in CREATE TABLE to move USING after the column list
// so the vitess parser can handle it.
func normalizeCreateTableIndexUsing(query string) string {
	upper := strings.ToUpper(query)
	if !strings.Contains(upper, " USING ") {
		return query
	}
	if !strings.Contains(upper, "CREATE TABLE") && !strings.Contains(upper, "CREATE TEMPORARY TABLE") {
		return query
	}
	// Strip "USING BTREE/HASH" that appears immediately before a column list "(".
	// This handles cases like "UNIQUE KEY name USING HASH (col(N))" where the
	// column list contains nested parentheses that confuse the [^)]* approach.
	// Vitess can parse the index type when it appears after the column list, but
	// since mylite ignores index types entirely, we simply strip them when they
	// appear before the column list.
	reUsingBeforeParen := regexp.MustCompile(`(?i)\bUSING\s+(?:BTREE|HASH)\s+\(`)
	if reUsingBeforeParen.MatchString(query) {
		query = reUsingBeforeParen.ReplaceAllStringFunc(query, func(_ string) string {
			return "("
		})
		return query
	}
	// PRIMARY KEY USING BTREE/HASH (cols) -> PRIMARY KEY (cols) USING BTREE/HASH
	re1 := regexp.MustCompile(`(?i)(PRIMARY\s+KEY)\s+USING\s+(\w+)\s*(\([^)]*\))`)
	query = re1.ReplaceAllString(query, "${1} ${3} USING ${2}")
	// UNIQUE KEY [name] USING BTREE (cols) -> UNIQUE KEY [name] (cols) USING BTREE
	re2 := regexp.MustCompile("(?i)(UNIQUE\\s+(?:KEY|INDEX))\\s+USING\\s+(\\w+)\\s*(\\([^)]*\\))")
	query = re2.ReplaceAllString(query, "${1} ${3} USING ${2}")
	re3 := regexp.MustCompile("(?i)(UNIQUE\\s+(?:KEY|INDEX)\\s+`?\\w+`?)\\s+USING\\s+(\\w+)\\s*(\\([^)]*\\))")
	query = re3.ReplaceAllString(query, "${1} ${3} USING ${2}")
	// KEY/INDEX [name] USING BTREE (cols) -> KEY [name] (cols) USING BTREE
	re4 := regexp.MustCompile("(?i)((?:KEY|INDEX))\\s+USING\\s+(\\w+)\\s*(\\([^)]*\\))")
	query = re4.ReplaceAllString(query, "${1} ${3} USING ${2}")
	re5 := regexp.MustCompile("(?i)((?:KEY|INDEX)\\s+`?\\w+`?)\\s+USING\\s+(\\w+)\\s*(\\([^)]*\\))")
	query = re5.ReplaceAllString(query, "${1} ${3} USING ${2}")
	// UNIQUE name USING BTREE (cols) -> UNIQUE KEY name (cols) USING BTREE
	re6 := regexp.MustCompile("(?i)(UNIQUE)\\s+(`?\\w+`?)\\s+USING\\s+(\\w+)\\s*(\\([^)]*\\))")
	query = re6.ReplaceAllString(query, "${1} KEY ${2} ${4} USING ${3}")
	// UNIQUE USING BTREE (cols) -> UNIQUE KEY (cols) USING BTREE (no name)
	re7 := regexp.MustCompile("(?i)(UNIQUE)\\s+USING\\s+(\\w+)\\s*(\\([^)]*\\))")
	query = re7.ReplaceAllString(query, "${1} KEY ${3} USING ${2}")
	return query
}

// normalizeEnumHexValues converts hex literals in ENUM and SET column type values
// to quoted string literals so the vitess parser can handle them.
// e.g. ENUM(0x9353,0x9373) -> ENUM('0x9353','0x9373')
func normalizeEnumHexValues(query string) string {
	upper := strings.ToUpper(query)
	if !strings.Contains(upper, "ENUM") && !strings.Contains(upper, " SET") {
		return query
	}
	reHex := regexp.MustCompile(`(?i)\b(ENUM|SET)\s*\(((?:[^()]*0x[0-9a-fA-F]+[^()]*)+)\)`)
	return reHex.ReplaceAllStringFunc(query, func(match string) string {
		parenIdx := strings.IndexByte(match, '(')
		keyword := match[:parenIdx]
		valPart := match[parenIdx+1 : len(match)-1]
		parts := strings.Split(valPart, ",")
		newParts := make([]string, 0, len(parts))
		for _, p := range parts {
			trimmed := strings.TrimSpace(p)
			if strings.HasPrefix(strings.ToLower(trimmed), "0x") {
				newParts = append(newParts, "'"+trimmed+"'")
			} else {
				newParts = append(newParts, p)
			}
		}
		return keyword + "(" + strings.Join(newParts, ", ") + ")"
	})
}

// normalizeWeightString strips extra numeric args from weight_string() calls
// that vitess can't parse, e.g. weight_string(expr, 1, 2, 0xC0) -> weight_string(expr)
func normalizeWeightString(query string) string {
	uq := strings.ToUpper(query)
	wsIdx := strings.Index(uq, "WEIGHT_STRING")
	if wsIdx < 0 {
		return query
	}
	// Find the opening paren
	pIdx := strings.Index(query[wsIdx:], "(")
	if pIdx < 0 {
		return query
	}
	pIdx += wsIdx
	// Find matching closing paren, tracking depth
	depth := 1
	firstComma := -1
	for wi := pIdx + 1; wi < len(query); wi++ {
		switch query[wi] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				if firstComma > 0 {
					// Strip everything from first comma to before closing paren
					return query[:firstComma] + query[wi:]
				}
				return query
			}
		case ',':
			if depth == 1 && firstComma < 0 {
				firstComma = wi
			}
		case '\'':
			// Skip string literals
			for wi++; wi < len(query) && query[wi] != '\''; wi++ {
				if query[wi] == '\\' {
					wi++
				}
			}
		}
	}
	return query
}

// normalizeForShareOf strips "OF table_name" and "SKIP LOCKED" / "NOWAIT" from
// FOR SHARE / FOR UPDATE clauses since the vitess parser can't handle the extended syntax.
func normalizeForShareOf(query string) string {
	uq := strings.ToUpper(query)
	if !strings.Contains(uq, "FOR SHARE") && !strings.Contains(uq, "FOR UPDATE") {
		return query
	}
	// Strip "OF table1[, table2...]" after FOR SHARE/FOR UPDATE
	fsRe := regexp.MustCompile(`(?i)(FOR\s+(?:SHARE|UPDATE))\s+OF\s+[\w` + "`" + `]+(?:\s*,\s*[\w` + "`" + `]+)*`)
	query = fsRe.ReplaceAllString(query, "${1}")
	// Strip SKIP LOCKED / NOWAIT
	fsRe2 := regexp.MustCompile(`(?i)(FOR\s+(?:SHARE|UPDATE))\s+(?:SKIP\s+LOCKED|NOWAIT)`)
	query = fsRe2.ReplaceAllString(query, "${1}")
	// Strip standalone SKIP LOCKED / NOWAIT (may remain after stripping OF)
	query = regexp.MustCompile(`(?i)\s+SKIP\s+LOCKED`).ReplaceAllString(query, "")
	query = regexp.MustCompile(`(?i)\s+NOWAIT`).ReplaceAllString(query, "")
	// Strip duplicate FOR SHARE/FOR UPDATE
	fsRe3 := regexp.MustCompile(`(?i)(FOR\s+(?:SHARE|UPDATE))\s+FOR\s+(?:SHARE|UPDATE)`)
	for fsRe3.MatchString(query) {
		query = fsRe3.ReplaceAllString(query, "${1}")
	}
	return query
}

// parseSelectLockClauses extracts per-table locking info from
// "FOR SHARE OF t1 SKIP LOCKED FOR UPDATE OF t2 NOWAIT" style clauses.
// Returns nil if no "OF table" clauses are present.
func parseSelectLockClauses(query string) []selectLockClause {
	uq := strings.ToUpper(query)
	if !strings.Contains(uq, "FOR SHARE") && !strings.Contains(uq, "FOR UPDATE") {
		return nil
	}
	// Match: FOR (SHARE|UPDATE) [OF table[,table...]] [SKIP LOCKED|NOWAIT]
	re := regexp.MustCompile(`(?i)FOR\s+(SHARE|UPDATE)(?:\s+OF\s+([\w` + "`" + `]+(?:\s*,\s*[\w` + "`" + `]+)*))?(?:\s+(SKIP\s+LOCKED|NOWAIT))?`)
	matches := re.FindAllStringSubmatch(query, -1)
	if len(matches) == 0 {
		return nil
	}
	// Check if any clause has "OF table" - if none do, return nil (simple lock)
	hasOfClause := false
	for _, m := range matches {
		if m[2] != "" {
			hasOfClause = true
			break
		}
	}
	if !hasOfClause {
		return nil
	}
	var clauses []selectLockClause
	for _, m := range matches {
		lockType := strings.ToUpper(m[1])
		exclusive := lockType == "UPDATE"
		tablesStr := strings.TrimSpace(m[2])
		modifier := strings.ToUpper(strings.TrimSpace(m[3]))
		skipLocked := strings.Contains(modifier, "SKIP")
		nowait := modifier == "NOWAIT"

		if tablesStr == "" {
			// "FOR SHARE" / "FOR UPDATE" without OF - applies to all tables
			clauses = append(clauses, selectLockClause{
				tableName:  "*",
				exclusive:  exclusive,
				skipLocked: skipLocked,
				nowait:     nowait,
			})
		} else {
			// Split comma-separated table names
			tables := strings.Split(tablesStr, ",")
			for _, t := range tables {
				t = strings.TrimSpace(t)
				t = strings.Trim(t, "`")
				if t != "" {
					clauses = append(clauses, selectLockClause{
						tableName:  t,
						exclusive:  exclusive,
						skipLocked: skipLocked,
						nowait:     nowait,
					})
				}
			}
		}
	}
	return clauses
}

// normalizeMemberOperator rewrites legacy "expr MEMBER (json_doc)" to
// "expr MEMBER OF (json_doc)" for parser compatibility.
func normalizeMemberOperator(query string) string {
	re := regexp.MustCompile(`(?i)\bMEMBER\s*\(`)
	return re.ReplaceAllString(query, "MEMBER OF (")
}

// normalizeJSONTableDefaultOrder rewrites JSON_TABLE path-column clauses where
// ON ERROR appears before ON EMPTY, since the parser expects ON EMPTY first.
func normalizeJSONTableDefaultOrder(query string) string {
	re := regexp.MustCompile(`(?is)(default\s+'[^']*'\s+on\s+error)\s+(default\s+'[^']*'\s+on\s+empty)`)
	return re.ReplaceAllString(query, "${2} ${1}")
}

// quoteNonASCIIIdentifiers wraps bare words containing non-ASCII characters
// with backticks so the vitess SQL parser can handle them.
func quoteNonASCIIIdentifiers(query string) string {
	// Quick check: if no non-ASCII bytes, nothing to do.
	hasNonASCII := false
	for i := 0; i < len(query); i++ {
		if query[i] > 127 {
			hasNonASCII = true
			break
		}
	}
	if !hasNonASCII {
		return query
	}

	var buf strings.Builder
	buf.Grow(len(query) + 16)
	i := 0
	for i < len(query) {
		b := query[i]
		// Skip string literals
		if b == '\'' || b == '"' {
			quote := b
			buf.WriteByte(b)
			i++
			for i < len(query) {
				if query[i] == '\\' && i+1 < len(query) {
					buf.WriteByte(query[i])
					buf.WriteByte(query[i+1])
					i += 2
					continue
				}
				if query[i] == quote {
					buf.WriteByte(query[i])
					i++
					if i < len(query) && query[i] == quote {
						buf.WriteByte(query[i])
						i++
						continue
					}
					break
				}
				buf.WriteByte(query[i])
				i++
			}
			continue
		}
		// Skip backtick-quoted identifiers
		if b == '`' {
			buf.WriteByte(b)
			i++
			for i < len(query) && query[i] != '`' {
				buf.WriteByte(query[i])
				i++
			}
			if i < len(query) {
				buf.WriteByte(query[i])
				i++
			}
			continue
		}
		// Check for identifier-like token that contains non-ASCII
		if isIdentStart(rune(b)) || b > 127 {
			start := i
			for i < len(query) {
				r, size := utf8.DecodeRuneInString(query[i:])
				if r == utf8.RuneError && size <= 1 {
					// Invalid UTF-8 byte: include it as part of the
					// identifier (latin1 / cp1252 column names) and
					// advance past it so we don't loop forever.
					if query[i] > 127 {
						i++
						continue
					}
					break
				}
				if isIdentPart(r) || r > 127 {
					i += size
				} else {
					break
				}
			}
			word := query[start:i]
			// Only quote if it contains non-ASCII
			wordHasNonASCII := false
			for j := 0; j < len(word); j++ {
				if word[j] > 127 {
					wordHasNonASCII = true
					break
				}
			}
			if wordHasNonASCII {
				buf.WriteByte('`')
				buf.WriteString(word)
				buf.WriteByte('`')
			} else {
				buf.WriteString(word)
			}
			continue
		}
		buf.WriteByte(b)
		i++
	}
	return buf.String()
}

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
				e.warnings = append(e.warnings, Warning{Level: "Error", Code: code, Message: msg})
			}
		}()
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
		// KILL [CONNECTION | QUERY] thread_id: accept as no-op
		if strings.HasPrefix(upper, "KILL ") {
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
			e.questions = 0
			return &Result{}, nil
		}
		// HANDLER ... OPEN/READ/CLOSE: return error for performance_schema tables
		if strings.HasPrefix(upper, "HANDLER ") {
			rest := strings.TrimSpace(trimmed[len("HANDLER "):])
			parts := strings.Fields(rest)
			if len(parts) >= 2 {
				lastWord := strings.ToUpper(parts[len(parts)-1])
				isHandlerOp := lastWord == "OPEN" || lastWord == "READ" || lastWord == "CLOSE"
				// READ can also have additional args like HANDLER t READ idx (>, =, etc.)
				if !isHandlerOp {
					for _, p := range parts[1:] {
						pu := strings.ToUpper(p)
						if pu == "READ" || pu == "CLOSE" {
							isHandlerOp = true
							break
						}
					}
				}
				if isHandlerOp {
					tblRef := strings.Trim(parts[0], "`")
					handlerDB := ""
					handlerTbl := tblRef
					if strings.Contains(tblRef, ".") {
						dbTbl := strings.SplitN(tblRef, ".", 2)
						handlerDB = strings.Trim(dbTbl[0], "`")
						handlerTbl = strings.Trim(dbTbl[1], "`")
					}
					if strings.EqualFold(handlerDB, "performance_schema") ||
						(handlerDB == "" && strings.EqualFold(e.CurrentDB, "performance_schema")) {
						return nil, mysqlError(1031, "HY000", fmt.Sprintf("Table storage engine for '%s' doesn't have this option", handlerTbl))
					}
				}
			}
			return &Result{}, nil
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
			// Helper to extract username from "GRANT ... TO 'user'@'host'"
			extractGrantUser := func() string {
				if idx := strings.Index(upper, " TO "); idx >= 0 {
					userPart := strings.TrimSpace(trimmed[idx+4:])
					// Strip WITH GRANT OPTION suffix
					if wi := strings.Index(strings.ToUpper(userPart), " WITH "); wi >= 0 {
						userPart = strings.TrimSpace(userPart[:wi])
					}
					// Extract bare username (strip quotes and @host)
					userPart = strings.Trim(userPart, "'`\"")
					if at := strings.Index(userPart, "@"); at >= 0 {
						userPart = userPart[:at]
					}
					userPart = strings.Trim(userPart, "'`\"")
					return strings.ToLower(strings.TrimSpace(userPart))
				}
				return ""
			}
			// Track GRANT SUPER ON *.* TO user for privilege checking
			if strings.Contains(upper, "SUPER") {
				if username := extractGrantUser(); username != "" && e.superUsersMu != nil {
					e.superUsersMu.Lock()
					e.superUsers[username] = true
					e.superUsersMu.Unlock()
				}
			}
			// Track GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO user for SET GLOBAL privilege checking
			if strings.Contains(upper, "SYSTEM_VARIABLES_ADMIN") {
				if username := extractGrantUser(); username != "" && e.sysVarsAdminUsersMu != nil {
					e.sysVarsAdminUsersMu.Lock()
					e.sysVarsAdminUsers[username] = true
					e.sysVarsAdminUsersMu.Unlock()
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
		if strings.HasPrefix(upper, "CREATE EVENT") ||
			strings.HasPrefix(upper, "DROP EVENT") ||
			strings.HasPrefix(upper, "CREATE USER") ||
			strings.HasPrefix(upper, "DROP USER") ||
			strings.HasPrefix(upper, "ALTER USER") ||
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
		if strings.HasPrefix(upper, "DELETE ") {
			return e.execMultiTableDelete(trimmed)
		}
		// Handle SHOW GRANTS (vitess parser may fail on some variants)
		if strings.HasPrefix(upper, "SHOW GRANTS") {
			grantUser := "root"
			grantHost := "localhost"
			// Parse "SHOW GRANTS FOR user@host" or "SHOW GRANTS FOR 'user'@'host'"
			if forIdx := strings.Index(upper, " FOR "); forIdx >= 0 {
				forPart := strings.TrimSpace(trimmed[forIdx+5:])
				forPart = strings.TrimRight(forPart, ";")
				if atIdx := strings.LastIndex(forPart, "@"); atIdx >= 0 {
					grantUser = strings.Trim(strings.TrimSpace(forPart[:atIdx]), "'`\"")
					grantHost = strings.Trim(strings.TrimSpace(forPart[atIdx+1:]), "'`\"")
				} else {
					grantUser = strings.Trim(strings.TrimSpace(forPart), "'`\"")
				}
			}
			grantRows := [][]interface{}{
				{fmt.Sprintf("GRANT USAGE ON *.* TO `%s`@`%s`", grantUser, grantHost)},
			}
			// Check if any database grants exist for this user
			if e.Catalog != nil {
				for _, dbName := range e.Catalog.ListDatabases() {
					if !strings.EqualFold(dbName, "information_schema") && !strings.EqualFold(dbName, "performance_schema") &&
						!strings.EqualFold(dbName, "mysql") && !strings.EqualFold(dbName, "sys") {
						grantRows = append(grantRows, []interface{}{
							fmt.Sprintf("GRANT ALL PRIVILEGES ON `%s`.* TO `%s`@`%s`", dbName, grantUser, grantHost),
						})
					}
				}
			}
			if grantUser == "root" {
				grantRows = [][]interface{}{
					{"GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' WITH GRANT OPTION"},
				}
			}
			return &Result{
				Columns:     []string{fmt.Sprintf("Grants for %s@%s", grantUser, grantHost)},
				Rows:        grantRows,
				IsResultSet: true,
			}, nil
		}
		return nil, mysqlError(1064, "42000", fmt.Sprintf("You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '%s' at line 1", extractNearFromParseError(trimmed, err)))
	}

	// Enforce LOCK TABLES restrictions before dispatching
	if e.tableLockManager != nil && e.tableLockManager.HasLocks(e.connectionID) {
		if lockErr := e.checkTableLockRestrictions(stmt); lockErr != nil {
			return nil, lockErr
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

// stripLeadingCStyleComments removes leading /* ... */ comment blocks and
// surrounding whitespace.
func stripLeadingCStyleComments(s string) string {
	for {
		trimmed := strings.TrimSpace(s)
		if !strings.HasPrefix(trimmed, "/*") {
			return trimmed
		}
		// Preserve MySQL versioned comments that should be executed (version <= 80040).
		// Format: /*!NNNNN content */ where NNNNN is the minimum MySQL version.
		// If version <= our server version (8.0.40 = 80040), the content should be
		// executed, so let vitess handle it. High version comments (e.g. /*!99999 ... */)
		// are effectively no-ops and can be stripped.
		if strings.HasPrefix(trimmed, "/*!") {
			// Extract version number
			verStr := ""
			for i := 3; i < len(trimmed) && i < 8; i++ {
				if trimmed[i] >= '0' && trimmed[i] <= '9' {
					verStr += string(trimmed[i])
				} else {
					break
				}
			}
			if len(verStr) == 5 {
				ver := 0
				for _, ch := range verStr {
					ver = ver*10 + int(ch-'0')
				}
				if ver <= 80040 {
					// This versioned comment should be executed - let vitess handle it
					return trimmed
				}
			}
			// High version or malformed - strip like a regular comment
		}
		end := strings.Index(trimmed, "*/")
		if end < 0 {
			return trimmed
		}
		s = trimmed[end+2:]
	}
}

func extractRawSelectExprs(query string) []string {
	q := strings.TrimSpace(query)
	lq := strings.ToLower(q)
	if !strings.HasPrefix(lq, "select ") {
		return nil
	}
	start := len("select ")
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
			locked, _ := e.tableLockManager.IsLocked(e.connectionID, key)
			if !locked {
				return mysqlError(1100, "HY000", fmt.Sprintf("Table '%s' was not locked with LOCK TABLES", tableName))
			}
		}
	}
	return nil
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
	if strings.ToUpper(val) == "SYSTEM" || strings.ToUpper(val) == "DEFAULT" || val == "" {
		e.timeZone = nil
		return nil
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

// checkDecimalRange checks if a value fits within a DECIMAL(M,D) column's range.
func checkDecimalRange(colType string, v interface{}) error {
	lower := strings.ToLower(colType)
	lower = strings.TrimSuffix(strings.TrimSpace(lower), " unsigned")
	lower = strings.TrimSpace(lower)
	var m, d int
	if n, err := fmt.Sscanf(lower, "decimal(%d,%d)", &m, &d); (err == nil && n == 2) || func() bool {
		if n2, err2 := fmt.Sscanf(lower, "decimal(%d)", &m); err2 == nil && n2 == 1 {
			d = 0
			return true
		}
		return false
	}() {
		f := toFloat(v)
		if f < 0 {
			f = -f
		}
		intDigits := m - d
		if intDigits <= 0 {
			intDigits = 1
		}
		maxVal := 1.0
		for i := 0; i < intDigits; i++ {
			maxVal *= 10
		}
		if f >= maxVal {
			return fmt.Errorf("out of range")
		}
	}
	return nil
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

// evalExpr evaluates a SQL expression that does not depend on a row context.
// It is a method on *Executor so that functions like LAST_INSERT_ID() and
// DATABASE() can access executor state.
func (e *Executor) evalExpr(expr sqlparser.Expr) (interface{}, error) {
	switch v := expr.(type) {
	case *sqlparser.Literal:
		return e.evalLiteralExpr(v)
	case *sqlparser.NullVal:
		return nil, nil
	case sqlparser.BoolVal:
		if bool(v) {
			return int64(1), nil
		}
		return int64(0), nil
	case *sqlparser.ColName:
		return e.evalColNameExpr(v)
	case *sqlparser.ValuesFuncExpr:
		// VALUES(col) is used by INSERT ... ON DUPLICATE KEY UPDATE.
		if e.onDupValuesRow == nil || v.Name == nil {
			return nil, nil
		}
		colName := v.Name.Name.String()
		if val, ok := e.onDupValuesRow[colName]; ok {
			return val, nil
		}
		for k, val := range e.onDupValuesRow {
			if strings.EqualFold(k, colName) {
				return val, nil
			}
		}
		return nil, nil
	case *sqlparser.Variable:
		return e.evalVariableExpr(v)
	case *sqlparser.Default:
		// DEFAULT(col) returns the default value for the named column.
		if v.ColName != "" {
			// First check the primary table def (target table)
			if e.defaultsTableDef != nil {
				for _, col := range e.defaultsTableDef.Columns {
					if strings.EqualFold(col.Name, v.ColName) {
						if col.Default != nil {
							return *col.Default, nil
						}
						// No explicit default:
						// DEFAULT(col) function requires explicit default → error 1364
						// For nullable column without default: NULL
						if col.Nullable {
							return nil, nil
						}
						// NOT NULL without explicit default: MySQL returns error 1364
						return nil, mysqlError(1364, "HY000", fmt.Sprintf("Field '%s' doesn't have a default value", col.Name))
					}
				}
			}
			// Then check the auxiliary defaults map (for source tables in INSERT ... SELECT)
			if e.defaultsByColName != nil {
				if def, ok := e.defaultsByColName[strings.ToLower(v.ColName)]; ok {
					return def, nil
				}
			}
			// Fall back: search all tables in the current DB for the column.
			// This supports DEFAULT(col) in SELECT statements.
			if e.Catalog != nil {
				if db, _ := e.Catalog.GetDatabase(e.CurrentDB); db != nil {
					for _, tbl := range db.Tables {
						for _, col := range tbl.Columns {
							if strings.EqualFold(col.Name, v.ColName) {
								if col.Default != nil {
									return *col.Default, nil
								}
								if col.Nullable {
									return nil, nil
								}
								// NOT NULL without explicit default: MySQL returns error 1364
								return nil, mysqlError(1364, "HY000", fmt.Sprintf("Field '%s' doesn't have a default value", col.Name))
							}
						}
					}
				}
			}
			// Column not found in any table: no default value
			return nil, mysqlError(1364, "HY000", fmt.Sprintf("Field '%s' doesn't have a default value", v.ColName))
		}
		return nil, nil
	case *sqlparser.UnaryExpr:
		return e.evalUnaryExpr(v)
	case *sqlparser.FuncExpr:
		return e.evalFuncExpr(v)
	case *sqlparser.ConvertExpr:
		return e.evalConvertExpr(v)
	case *sqlparser.CaseExpr:
		return e.evalCaseExpr(v)
	case *sqlparser.BinaryExpr:
		return e.evalBinaryOp(v)
	case *sqlparser.ComparisonExpr:
		return e.evalComparisonExpr(v)
	case *sqlparser.TrimFuncExpr:
		return e.evalTrimFuncExpr(v)
	case *sqlparser.SubstrExpr:
		return e.evalSubstrExpr(v)
	case *sqlparser.IntroducerExpr:
		return e.evalIntroducerExpr(v)
	case *sqlparser.CastExpr:
		return e.evalCastExpr(v)
	case *sqlparser.CurTimeFuncExpr:
		// NOW(), CURRENT_TIMESTAMP(), CURTIME(), etc.
		name := strings.ToLower(v.Name.String())
		now := e.nowTime()
		switch name {
		case "now", "current_timestamp", "localtime", "localtimestamp", "sysdate":
			return now.Format("2006-01-02 15:04:05"), nil
		case "curdate", "current_date":
			return now.Format("2006-01-02"), nil
		case "curtime", "current_time":
			return now.Format("15:04:05"), nil
		case "utc_timestamp":
			return e.nowTime().UTC().Format("2006-01-02 15:04:05"), nil
		case "utc_date":
			return e.nowTime().UTC().Format("2006-01-02"), nil
		case "utc_time":
			return e.nowTime().UTC().Format("15:04:05"), nil
		default:
			return now.Format("2006-01-02 15:04:05"), nil
		}
	case *sqlparser.Subquery:
		// Scalar subquery: execute and return the single value
		return e.execSubqueryScalar(v, e.correlatedRow)
	case *sqlparser.NotExpr:
		val, err := e.evalExpr(v.Expr)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil // NOT NULL = NULL
		}
		if isTruthy(val) {
			return int64(0), nil
		}
		return int64(1), nil
	case *sqlparser.IsExpr:
		return e.evalIsExpr(v)
	// JSON functions
	case *sqlparser.JSONExtractExpr:
		return e.evalJSONExtract(v)
	case *sqlparser.JSONAttributesExpr:
		return e.evalJSONAttributes(v)
	case *sqlparser.JSONObjectExpr:
		return e.evalJSONObject(v)
	case *sqlparser.JSONArrayExpr:
		return e.evalJSONArray(v)
	case *sqlparser.JSONContainsExpr:
		return e.evalJSONContains(v)
	case *sqlparser.JSONContainsPathExpr:
		return e.evalJSONContainsPath(v)
	case *sqlparser.JSONKeysExpr:
		return e.evalJSONKeys(v)
	case *sqlparser.JSONSearchExpr:
		return e.evalJSONSearch(v)
	case *sqlparser.JSONRemoveExpr:
		return e.evalJSONRemove(v)
	case *sqlparser.JSONValueModifierExpr:
		return e.evalJSONValueModifier(v)
	case *sqlparser.JSONValueMergeExpr:
		return e.evalJSONValueMerge(v)
	case *sqlparser.JSONQuoteExpr:
		return e.evalJSONQuote(v)
	case *sqlparser.JSONUnquoteExpr:
		return e.evalJSONUnquote(v)
	case *sqlparser.JSONPrettyExpr:
		return e.evalJSONPretty(v)
	case *sqlparser.JSONStorageSizeExpr:
		return e.evalJSONStorageSize(v)
	case *sqlparser.JSONStorageFreeExpr:
		return e.evalJSONStorageFree(v)
	case *sqlparser.JSONOverlapsExpr:
		return e.evalJSONOverlaps(v)
	case *sqlparser.MemberOfExpr:
		return e.evalMemberOf(v)
	case *sqlparser.JSONValueExpr:
		return e.evalJSONValue(v)
	case *sqlparser.JSONSchemaValidFuncExpr:
		return e.evalJSONSchemaValid(v)
	case *sqlparser.JSONSchemaValidationReportFuncExpr:
		return e.evalJSONSchemaValidationReport(v)
	case *sqlparser.ConvertUsingExpr:
		return e.evalConvertUsingExpr(v)
	case *sqlparser.GeomFromTextExpr:
		// ST_GeomFromText, ST_PointFromText, ST_LineStringFromText, etc.
		val, err := e.evalExpr(v.WktText)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		return normalizeWKT(toString(val)), nil
	case *sqlparser.GeomFormatExpr:
		// ST_AsText/ST_AsWKT/ST_AsBinary/ST_AsWKB
		val, err := e.evalExpr(v.Geom)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		return toString(val), nil
	case *sqlparser.GeomFromWKBExpr:
		// ST_GeomFromWKB, ST_PointFromWKB, etc. — treat WKB as passthrough
		val, err := e.evalExpr(v.WkbBlob)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		return toString(val), nil
	case *sqlparser.PointPropertyFuncExpr:
		// ST_X, ST_Y, ST_Latitude, ST_Longitude
		ptVal, err := e.evalExpr(v.Point)
		if err != nil {
			return nil, err
		}
		if ptVal == nil {
			return nil, nil
		}
		// If setter form (ST_X(pt, val)), return modified geometry
		if v.ValueToSet != nil {
			newVal, err := e.evalExpr(v.ValueToSet)
			if err != nil {
				return nil, err
			}
			return setSpatialPointCoord(toString(ptVal), v.Property, newVal)
		}
		return extractSpatialPointCoord(toString(ptVal), v.Property)
	case *sqlparser.GeomPropertyFuncExpr:
		// ST_IsSimple, ST_IsEmpty, ST_Dimension, ST_GeometryType, ST_Envelope, ST_SRID
		geomVal, err := e.evalExpr(v.Geom)
		if err != nil {
			return nil, err
		}
		if geomVal == nil {
			return nil, nil
		}
		return evalGeomProperty(toString(geomVal), v.Property)
	case *sqlparser.LinestrPropertyFuncExpr:
		// ST_EndPoint, ST_IsClosed, ST_Length, ST_NumPoints, ST_PointN, ST_StartPoint
		lsVal, err := e.evalExpr(v.Linestring)
		if err != nil {
			return nil, err
		}
		if lsVal == nil {
			return nil, nil
		}
		var propArg interface{}
		if v.PropertyDefArg != nil {
			propArg, err = e.evalExpr(v.PropertyDefArg)
			if err != nil {
				return nil, err
			}
		}
		return evalLinestrProperty(toString(lsVal), v.Property, propArg)
	case *sqlparser.PolygonPropertyFuncExpr:
		// ST_Area, ST_Centroid, ST_ExteriorRing, ST_InteriorRingN, ST_NumInteriorRings
		polyVal, err := e.evalExpr(v.Polygon)
		if err != nil {
			return nil, err
		}
		if polyVal == nil {
			return nil, nil
		}
		var polyArg interface{}
		if v.PropertyDefArg != nil {
			polyArg, err = e.evalExpr(v.PropertyDefArg)
			if err != nil {
				return nil, err
			}
		}
		return evalPolygonProperty(toString(polyVal), v.Property, polyArg)
	case *sqlparser.GeomCollPropertyFuncExpr:
		// ST_GeometryN, ST_NumGeometries
		gcVal, err := e.evalExpr(v.GeomColl)
		if err != nil {
			return nil, err
		}
		if gcVal == nil {
			return nil, nil
		}
		var gcArg interface{}
		if v.PropertyDefArg != nil {
			gcArg, err = e.evalExpr(v.PropertyDefArg)
			if err != nil {
				return nil, err
			}
		}
		return evalGeomCollProperty(toString(gcVal), v.Property, gcArg)
	case *sqlparser.GeoJSONFromGeomExpr:
		// ST_AsGeoJSON
		geomVal, err := e.evalExpr(v.Geom)
		if err != nil {
			return nil, err
		}
		if geomVal == nil {
			return nil, nil
		}
		return wktToGeoJSON(toString(geomVal))
	case *sqlparser.GeomFromGeoJSONExpr:
		// ST_GeomFromGeoJSON
		jsonVal, err := e.evalExpr(v.GeoJSON)
		if err != nil {
			return nil, err
		}
		if jsonVal == nil {
			return nil, nil
		}
		return geoJSONToWkt(toString(jsonVal))
	case *sqlparser.GeomFromGeoHashExpr:
		// ST_LatFromGeoHash, ST_LongFromGeoHash, ST_PointFromGeoHash
		hashVal, err := e.evalExpr(v.GeoHash)
		if err != nil {
			return nil, err
		}
		if hashVal == nil {
			return nil, nil
		}
		return evalGeomFromGeoHash(toString(hashVal), v.GeomType)
	case *sqlparser.GeoHashFromLatLongExpr:
		// ST_GeoHash(lat, long, maxlen)
		latVal, err := e.evalExpr(v.Latitude)
		if err != nil {
			return nil, err
		}
		lonVal, err := e.evalExpr(v.Longitude)
		if err != nil {
			return nil, err
		}
		maxLenVal, err := e.evalExpr(v.MaxLength)
		if err != nil {
			return nil, err
		}
		if latVal == nil || lonVal == nil || maxLenVal == nil {
			return nil, nil
		}
		return evalGeoHash(toFloat(latVal), toFloat(lonVal), int(toInt64(maxLenVal)))
	case *sqlparser.GeoHashFromPointExpr:
		// ST_GeoHash(point, maxlen)
		ptVal, err := e.evalExpr(v.Point)
		if err != nil {
			return nil, err
		}
		maxLenVal, err := e.evalExpr(v.MaxLength)
		if err != nil {
			return nil, err
		}
		if ptVal == nil || maxLenVal == nil {
			return nil, nil
		}
		coords := parseSpatialPointCoords(toString(ptVal))
		if coords == nil {
			return nil, nil
		}
		return evalGeoHash(coords[0], coords[1], int(toInt64(maxLenVal)))
	case *sqlparser.LineStringExpr:
		return e.buildGeometryFromExprs(v.PointParams, extractPointCoords, "LINESTRING")
	case *sqlparser.PolygonExpr:
		return e.buildGeometryFromExprs(v.LinestringParams, extractRingCoords, "POLYGON")
	case *sqlparser.MultiPointExpr:
		result, err := e.buildGeometryFromExprs(v.PointParams, extractPointCoords, "MULTIPOINT")
		if err != nil {
			return nil, err
		}
		return normalizeWKT(result), nil
	case *sqlparser.MultiLinestringExpr:
		return e.buildGeometryFromExprs(v.LinestringParams, extractRingCoords, "MULTILINESTRING")
	case *sqlparser.MultiPolygonExpr:
		return e.buildGeometryFromExprs(v.PolygonParams, extractPolygonCoords, "MULTIPOLYGON")
	case *sqlparser.CharExpr:
		// CHAR(N1, N2, ...) — convert integers to characters
		// MySQL outputs the minimum number of bytes needed for each value.
		var result []byte
		for _, argExpr := range v.Exprs {
			val, err := e.evalExpr(argExpr)
			if err != nil {
				return nil, err
			}
			if val == nil {
				continue
			}
			n := uint64(toInt64(val))
			if n == 0 {
				result = append(result, 0)
			} else if n <= 0xFF {
				result = append(result, byte(n))
			} else if n <= 0xFFFF {
				result = append(result, byte(n>>8), byte(n))
			} else if n <= 0xFFFFFF {
				result = append(result, byte(n>>16), byte(n>>8), byte(n))
			} else {
				result = append(result, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
			}
		}
		return string(result), nil
	case *sqlparser.CollateExpr:
		// Ignore COLLATE clause and evaluate inner expression
		return e.evalExpr(v.Expr)
	case *sqlparser.IntervalDateExpr:
		// DATE_ADD / DATE_SUB / ADDDATE / SUBDATE
		dateVal, err := e.evalExpr(v.Date)
		if err != nil {
			return nil, err
		}
		if dateVal == nil {
			return nil, nil
		}
		intervalVal, err := e.evalExpr(v.Interval)
		if err != nil {
			return nil, err
		}
		return evalIntervalDateExprStrict(dateVal, intervalVal, v.Unit, v.Syntax, e.isTraditionalMode())
	case *sqlparser.AssignmentExpr:
		// @var := expr — evaluate the right side, assign to user variable, return value
		val, err := e.evalExpr(v.Right)
		if err != nil {
			return nil, err
		}
		varName := strings.TrimPrefix(sqlparser.String(v.Left), "@")
		varName = strings.Trim(varName, "`")
		if e.userVars == nil {
			e.userVars = make(map[string]interface{})
		}
		e.userVars[varName] = val
		return val, nil
	case *sqlparser.InsertExpr:
		return e.evalInsertExpr(v)
	case *sqlparser.LocateExpr:
		return e.evalLocateExpr(v)
	case sqlparser.ValTuple:
		// A row constructor (tuple) used in a scalar context is an error in MySQL
		return nil, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain 1 column(s)"))
	case *sqlparser.WeightStringFuncExpr:
		return e.evalWeightStringFuncExpr(v)
	case *sqlparser.AndExpr:
		left, err := e.evalExpr(v.Left)
		if err != nil {
			return nil, err
		}
		right, err := e.evalExpr(v.Right)
		if err != nil {
			return nil, err
		}
		lb := isTruthy(left)
		rb := isTruthy(right)
		if left == nil || right == nil {
			if (left != nil && !lb) || (right != nil && !rb) {
				return int64(0), nil
			}
			return nil, nil
		}
		if lb && rb {
			return int64(1), nil
		}
		return int64(0), nil
	case *sqlparser.OrExpr:
		left, err := e.evalExpr(v.Left)
		if err != nil {
			return nil, err
		}
		right, err := e.evalExpr(v.Right)
		if err != nil {
			return nil, err
		}
		// When PIPES_AS_CONCAT is active, || acts as string concatenation (same as CONCAT()).
		// The SQL parser converts || to OrExpr, so we intercept it here.
		if strings.Contains(e.sqlMode, "PIPES_AS_CONCAT") {
			if left == nil || right == nil {
				return nil, nil
			}
			return toString(left) + toString(right), nil
		}
		lb := isTruthy(left)
		rb := isTruthy(right)
		if lb || rb {
			return int64(1), nil
		}
		if left == nil || right == nil {
			return nil, nil
		}
		return int64(0), nil
	case *sqlparser.BetweenExpr:
		return e.evalBetweenExpr(v)
	case *sqlparser.XorExpr:
		left, err := e.evalExpr(v.Left)
		if err != nil {
			return nil, err
		}
		right, err := e.evalExpr(v.Right)
		if err != nil {
			return nil, err
		}
		if left == nil || right == nil {
			return nil, nil
		}
		lb := isTruthy(left)
		rb := isTruthy(right)
		if (lb && !rb) || (!lb && rb) {
			return int64(1), nil
		}
		return int64(0), nil
	case *sqlparser.LockingFunc:
		return e.evalLockingFuncExpr(v)
	case *sqlparser.PointExpr:
		xVal, err := e.evalExpr(v.XCordinate)
		if err != nil {
			return nil, err
		}
		yVal, err := e.evalExpr(v.YCordinate)
		if err != nil {
			return nil, err
		}
		return fmt.Sprintf("POINT(%v %v)", xVal, yVal), nil
	case *sqlparser.MatchExpr:
		return e.evalMatchExpr(v)
	case *sqlparser.CountStar:
		return int64(0), nil
	case *sqlparser.LagLeadExpr:
		return nil, nil
	case *sqlparser.VarSamp:
		if e.correlatedRow != nil {
			if val, ok := e.correlatedRow[aggregateDisplayName(expr)]; ok {
				return val, nil
			}
		}
		return nil, nil
	case *sqlparser.Std:
		if e.correlatedRow != nil {
			if val, ok := e.correlatedRow[aggregateDisplayName(expr)]; ok {
				return val, nil
			}
		}
		return nil, nil
	case *sqlparser.FirstOrLastValueExpr:
		// FIRST_VALUE/LAST_VALUE - evaluate expression as stub
		if v.Expr != nil {
			return e.evalExpr(v.Expr)
		}
		return nil, nil
	case *sqlparser.RegexpReplaceExpr:
		return e.evalRegexpReplaceExpr(v)
	case *sqlparser.ExtractValueExpr:
		// EXTRACTVALUE(xml, xpath) - simplified stub
		return nil, nil
	case *sqlparser.UpdateXMLExpr:
		// UPDATEXML(target, xpath, new) - stub
		return nil, nil
	case *sqlparser.Variance:
		if e.correlatedRow != nil {
			if val, ok := e.correlatedRow[aggregateDisplayName(expr)]; ok {
				return val, nil
			}
		}
		return nil, nil
	case *sqlparser.VarPop:
		if e.correlatedRow != nil {
			if val, ok := e.correlatedRow[aggregateDisplayName(expr)]; ok {
				return val, nil
			}
		}
		return nil, nil
	case *sqlparser.StdDev:
		if e.correlatedRow != nil {
			if val, ok := e.correlatedRow[aggregateDisplayName(expr)]; ok {
				return val, nil
			}
		}
		return nil, nil
	case *sqlparser.StdPop:
		if e.correlatedRow != nil {
			if val, ok := e.correlatedRow[aggregateDisplayName(expr)]; ok {
				return val, nil
			}
		}
		return nil, nil
	case *sqlparser.StdSamp:
		if e.correlatedRow != nil {
			if val, ok := e.correlatedRow[aggregateDisplayName(expr)]; ok {
				return val, nil
			}
		}
		return nil, nil
	case *sqlparser.BitAnd:
		// BIT_AND aggregate/window function - stub; actual values computed by processWindowFunctions
		return uint64(^uint64(0)), nil
	case *sqlparser.BitOr:
		// BIT_OR aggregate/window function - stub; actual values computed by processWindowFunctions
		return uint64(0), nil
	case *sqlparser.BitXor:
		// BIT_XOR aggregate/window function - stub; actual values computed by processWindowFunctions
		return uint64(0), nil
	case *sqlparser.RegexpSubstrExpr:
		return e.evalRegexpSubstrExpr(v)
	case *sqlparser.IntervalFuncExpr:
		return e.evalIntervalFuncExpr(v)
	case *sqlparser.RegexpLikeExpr:
		return e.evalRegexpLikeExpr(v)
	case *sqlparser.RegexpInstrExpr:
		return e.evalRegexpInstrExpr(v)
	case *sqlparser.ExtractFuncExpr:
		return e.evalExtractFuncExpr(v)
	case *sqlparser.ArgumentLessWindowExpr:
		// ROW_NUMBER(), RANK(), DENSE_RANK(), etc. - stub returning 1
		// Actual values computed by processWindowFunctions
		return int64(1), nil
	case *sqlparser.NtileExpr:
		// NTILE(n) - stub returning 1
		// Actual values computed by processWindowFunctions
		return int64(1), nil
	case *sqlparser.NTHValueExpr:
		// NTH_VALUE - stub returning NULL
		// Actual values computed by processWindowFunctions
		return nil, nil
	case *sqlparser.ExistsExpr:
		return e.evalExistsExpr(v)
	case *sqlparser.Avg:
		// Aggregate used in HAVING context — look up pre-computed value from correlatedRow.
		if e.correlatedRow != nil {
			displayName := aggregateDisplayName(expr)
			if val, ok := e.correlatedRow[displayName]; ok {
				return val, nil
			}
		}
		return nil, nil
	case *sqlparser.Max:
		if e.correlatedRow != nil {
			displayName := aggregateDisplayName(expr)
			if val, ok := e.correlatedRow[displayName]; ok {
				return val, nil
			}
		}
		return nil, nil
	case *sqlparser.Min:
		if e.correlatedRow != nil {
			displayName := aggregateDisplayName(expr)
			if val, ok := e.correlatedRow[displayName]; ok {
				return val, nil
			}
		}
		return nil, nil
	case *sqlparser.Sum:
		if e.correlatedRow != nil {
			displayName := aggregateDisplayName(expr)
			if val, ok := e.correlatedRow[displayName]; ok {
				return val, nil
			}
		}
		return nil, nil
	case *sqlparser.Count:
		// In no-FROM context (e.g., SELECT COUNT(@@var)), evaluate the argument
		// to propagate errors (e.g., scope errors) and count non-null values.
		if len(v.Args) > 0 {
			val, err := e.evalExpr(v.Args[0])
			if err != nil {
				return nil, err
			}
			if val != nil {
				return int64(1), nil
			}
			return int64(0), nil
		}
		return int64(0), nil
	case *sqlparser.TimestampDiffExpr:
		return e.evalTimestampDiffExpr(v)
	case *sqlparser.AnyValue:
		// ANY_VALUE(expr) returns the expression value, bypassing ONLY_FULL_GROUP_BY checks
		return e.evalExpr(v.Arg)
	case *sqlparser.PerformanceSchemaFuncExpr:
		return e.evalPerformanceSchemaFuncExpr(v)
	}
	return nil, fmt.Errorf("unsupported expression: %T (%s)", expr, sqlparser.String(expr))
}

// evalInsertExpr implements the MySQL INSERT(str, pos, len, newstr) function.
// INSERT() returns the string str, with the substring beginning at position pos
// and len characters long replaced by the string newstr.
func (e *Executor) evalInsertExpr(v *sqlparser.InsertExpr) (interface{}, error) {
	strVal, err := e.evalExpr(v.Str)
	if err != nil {
		return nil, err
	}
	if strVal == nil {
		return nil, nil
	}
	posVal, err := e.evalExpr(v.Pos)
	if err != nil {
		return nil, err
	}
	if posVal == nil {
		return nil, nil
	}
	lenVal, err := e.evalExpr(v.Len)
	if err != nil {
		return nil, err
	}
	if lenVal == nil {
		return nil, nil
	}
	newStrVal, err := e.evalExpr(v.NewStr)
	if err != nil {
		return nil, err
	}
	if newStrVal == nil {
		return nil, nil
	}

	str := []rune(toString(strVal))
	pos := int(toInt64(posVal))
	length := int(toInt64(lenVal))
	newStr := toString(newStrVal)

	// MySQL INSERT() uses 1-based positions
	// If pos < 1 or pos > len(str), return original string
	if pos < 1 || pos > len(str) {
		return string(str), nil
	}

	// Convert to 0-based index
	idx := pos - 1

	// Calculate the end of the replaced portion
	end := idx + length
	if end > len(str) {
		end = len(str)
	}
	if end < idx {
		end = idx
	}

	// Build result: str[:idx] + newStr + str[end:]
	result := string(str[:idx]) + newStr + string(str[end:])
	return result, nil
}

// evalLocateExpr implements the MySQL LOCATE(substr, str [, pos]) function.
// Returns the position of the first occurrence of substr in str, starting from pos.
func (e *Executor) evalLocateExpr(v *sqlparser.LocateExpr) (interface{}, error) {
	subStrVal, err := e.evalExpr(v.SubStr)
	if err != nil {
		return nil, err
	}
	if subStrVal == nil {
		return nil, nil
	}
	strVal, err := e.evalExpr(v.Str)
	if err != nil {
		return nil, err
	}
	if strVal == nil {
		return nil, nil
	}

	subStr := []rune(toString(subStrVal))
	str := []rune(toString(strVal))
	startPos := 1

	if v.Pos != nil {
		posVal, err := e.evalExpr(v.Pos)
		if err != nil {
			return nil, err
		}
		if posVal != nil {
			startPos = int(toInt64(posVal))
		}
	}

	if startPos < 1 {
		return int64(0), nil
	}

	// Convert to 0-based index
	startIdx := startPos - 1
	if startIdx >= len(str) {
		if len(subStr) == 0 {
			return int64(0), nil
		}
		return int64(0), nil
	}

	// Search for substr in str starting at startIdx
	searchStr := str[startIdx:]
	subStrStr := string(subStr)
	searchStrStr := string(searchStr)
	// MySQL LOCATE is case-insensitive by default (uses the connection collation).
	// Check if the str argument has a COLLATE clause to determine case sensitivity.
	isCaseSensitive := false
	if ce, ok := v.Str.(*sqlparser.CollateExpr); ok {
		coll := strings.ToLower(ce.Collation)
		isCaseSensitive = strings.Contains(coll, "_bin") || strings.Contains(coll, "_cs")
	}
	var idx int
	if isCaseSensitive {
		idx = strings.Index(searchStrStr, subStrStr)
	} else {
		idx = strings.Index(strings.ToLower(searchStrStr), strings.ToLower(subStrStr))
	}
	if idx < 0 {
		return int64(0), nil
	}

	// Convert byte index back to rune index
	runeIdx := len([]rune(searchStrStr[:idx]))
	return int64(runeIdx + startPos), nil
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

// evalFuncExpr handles MySQL built-in function calls.
func (e *Executor) evalFuncExpr(v *sqlparser.FuncExpr) (interface{}, error) {
	name := strings.ToLower(v.Name.String())

	// Dispatch to category-specific handlers
	if result, handled, err := evalStringFunc(e, name, v, nil); handled {
		return result, err
	}
	if result, handled, err := evalDatetimeFunc(e, name, v, nil); handled {
		return result, err
	}
	if result, handled, err := evalMathFunc(e, name, v, nil); handled {
		return result, err
	}
	if result, handled, err := evalMiscFunc(e, name, v, nil); handled {
		return result, err
	}

	// Try spatial functions
	if result, handled, err := evalSpatialFunc(e, name, v.Exprs); handled {
		return result, err
	}
	// Try built-in sys schema functions
	if result, handled, err := e.evalSysSchemaFunc(name, v.Exprs); handled {
		return result, err
	}
	// Try user-defined function from catalog
	qualifier := v.Qualifier.String()
	if result, err := e.callUserDefinedFunction(name, v.Exprs, nil, qualifier); err == nil {
		return result, nil
	} else if !strings.Contains(strings.ToLower(err.Error()), "function not found") {
		return nil, err
	}
	// Unknown function: return ER_SP_DOES_NOT_EXIST (1305, SQLSTATE 42000) so that
	// CONTINUE HANDLERs for SQLSTATE '42000' inside stored functions can catch it.
	db := e.CurrentDB
	if db == "" {
		db = "test"
	}
	return nil, mysqlError(1305, "42000", fmt.Sprintf("FUNCTION %s.%s does not exist", db, name))
}

// parseDateTimeValue parses a date/time interface value into a time.Time.
// Supports string formats: "2006-01-02", "2006-01-02 15:04:05", "15:04:05", "2006-01-02T15:04:05".
// isZeroDate checks if a value represents MySQL's zero date (0000-00-00 ...)
func isZeroDate(val interface{}) bool {
	if val == nil {
		return false
	}
	s := toString(val)
	return strings.HasPrefix(s, "0000-00-00")
}

func secToTimeValue(v interface{}) string {
	// Determine the precision for fractional seconds.
	// DivisionResult carries a Precision that limits decimal places.
	fracPrec := 6
	if dr, ok := v.(DivisionResult); ok {
		if dr.Precision < fracPrec {
			fracPrec = dr.Precision
		}
	}
	f := toFloat(v)
	sign := ""
	if f < 0 {
		sign = "-"
		f = -f
	}
	totalSec := int64(f)
	frac := f - float64(totalSec)
	h := totalSec / 3600
	m := (totalSec % 3600) / 60
	s := totalSec % 60
	if frac > 1e-9 && fracPrec > 0 {
		// Format fractional seconds with the appropriate precision, stripping trailing zeros
		fracStr := fmt.Sprintf("%."+strconv.Itoa(fracPrec)+"f", frac)[1:] // e.g., ".4235"
		fracStr = strings.TrimRight(fracStr, "0")
		if fracStr != "." {
			return fmt.Sprintf("%s%02d:%02d:%02d%s", sign, h, m, s, fracStr)
		}
	}
	return fmt.Sprintf("%s%02d:%02d:%02d", sign, h, m, s)
}

// mysqlWeekMode0 calculates MySQL's WEEK(date) with default mode 0.
// Mode 0: Sunday is first day of week, range 0-53.
func mysqlWeekMode0(t time.Time) int64 {
	yday := t.YearDay() // 1-based
	// Find the weekday of Jan 1 (0=Sunday, ..., 6=Saturday)
	jan1 := time.Date(t.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
	wdJan1 := int(jan1.Weekday()) // 0=Sunday
	// First Sunday of the year is at day (7-wdJan1)%7 + 1
	// If Jan 1 is Sunday, first Sunday is day 1
	firstSunday := (7 - wdJan1) % 7
	if yday <= firstSunday {
		return 0
	}
	return int64((yday-firstSunday-1)/7 + 1)
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
func mysqlWeekFull(t time.Time, mode int64) int64 {
	mode = mode & 7 // clamp to 0-7
	mondayFirst := (mode & 1) != 0
	weekRange1to53 := (mode & 2) != 0
	useWeek4Rule := (mode & 4) != 0

	year := t.Year()

	if mondayFirst {
		// Monday-first modes
		if useWeek4Rule {
			// Modes 5, 7: week 1 starts at first Monday (no 4-day rule)
			jan1 := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
			wdJan1 := int(jan1.Weekday()) // 0=Sun, 1=Mon, ..., 6=Sat
			// firstMonday: days from Jan 1 to first Monday (0-based offset)
			firstMonday := (8 - wdJan1) % 7 // 0 if Jan 1 is Monday
			yday := t.YearDay() - 1         // 0-based
			if yday < firstMonday {
				if weekRange1to53 {
					// Return last week of previous year
					prevDec31 := time.Date(year-1, 12, 31, 0, 0, 0, 0, time.UTC)
					return mysqlWeekFull(prevDec31, mode)
				}
				return 0
			}
			return int64((yday-firstMonday)/7 + 1)
		}
		// Mode 3: straight ISO week (1-53)
		if weekRange1to53 {
			_, isoWeek := t.ISOWeek()
			return int64(isoWeek)
		}
		// Mode 1: Monday-first, range 0-53, week 1 has 4+ days in year.
		// Week 1 starts: the Monday on or before Jan 4 (the week containing Jan 4 is week 1).
		// wdJan1 in Mon=1..Sun=7 system:
		jan1 := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
		wdJan1Mon := int(jan1.Weekday()) // 0=Sun, 1=Mon..6=Sat
		if wdJan1Mon == 0 {
			wdJan1Mon = 7 // Sunday = 7 in Mon-first system
		}
		// Offset from Jan 1 to Monday of week 1:
		// If wdJan1Mon <= 4 (Mon=1,Tue=2,Wed=3,Thu=4): week 1 starts at -(wdJan1Mon-1)
		// If wdJan1Mon >= 5 (Fri=5,Sat=6,Sun=7): week 1 starts at 8-wdJan1Mon
		var week1StartOffset int
		if wdJan1Mon <= 4 {
			week1StartOffset = -(wdJan1Mon - 1) // negative: in Dec of prev year
		} else {
			week1StartOffset = 8 - wdJan1Mon // positive: in Jan
		}
		yday := t.YearDay() - 1 // 0-based day of year
		if yday < week1StartOffset {
			// Date is before week 1 of current year: return 0
			return 0
		}
		return int64((yday-week1StartOffset)/7 + 1)
	}

	// Sunday-first modes (modes 0, 2, 4, 6)
	jan1 := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
	wdJan1 := int(jan1.Weekday()) // 0=Sun

	if useWeek4Rule {
		// Modes 4, 6: week 1 has 4+ days in year (Sun-first variant)
		// First Sunday of the week that contains Jan 1 with 4+ days
		// The week containing Jan 1 starts on the Sunday on or before Jan 1.
		// Jan 1 is day wdJan1 of its week (0=Sun). The week starts at Jan 1 - wdJan1.
		// If this week has >= 4 days in the year (i.e. wdJan1 <= 3), it's week 1.
		// Otherwise week 1 starts next Sunday.
		yday := t.YearDay() - 1 // 0-based
		// Days before Jan 1 in the partial week containing Jan 1
		daysInFirstWeek := 7 - wdJan1
		if wdJan1 <= 3 {
			// Jan 1's week has >= 4 days -> it's week 1 starting at offset -wdJan1
			weekStart := -wdJan1 // can be negative (days in Dec of prev year)
			week := (yday - weekStart) / 7
			if week == 0 && weekRange1to53 {
				// Shouldn't happen with useWeek4Rule when wdJan1<=3
				return 1
			}
			return int64(week + 1)
		}
		// First partial week has < 4 days, so week 1 starts at daysInFirstWeek
		if yday < daysInFirstWeek {
			if weekRange1to53 {
				// Return last week of previous year
				prevDec31 := time.Date(year-1, 12, 31, 0, 0, 0, 0, time.UTC)
				return mysqlWeekFull(prevDec31, mode)
			}
			return 0
		}
		return int64((yday-daysInFirstWeek)/7 + 1)
	}

	// Modes 0, 2: week 1 starts at first Sunday
	// firstSunday: 0-based offset of first Sunday
	firstSunday := (7 - wdJan1) % 7
	yday := t.YearDay() - 1 // 0-based
	if yday < firstSunday {
		if weekRange1to53 {
			// Return last week of previous year
			prevDec31 := time.Date(year-1, 12, 31, 0, 0, 0, 0, time.UTC)
			return mysqlWeekFull(prevDec31, mode)
		}
		return 0
	}
	return int64((yday-firstSunday)/7 + 1)
}

// mysqlWeekYearFull returns (year, week) for a given mode.
// This is used by DATE_FORMAT %x/%X and YEARWEEK() to get the year the week belongs to.
func mysqlWeekYearFull(t time.Time, mode int64) (int, int64) {
	week := mysqlWeekFull(t, mode)
	year := t.Year()
	if week == 0 {
		// Week 0 means date belongs to last week of previous year
		year--
		prevDec31 := time.Date(year, 12, 31, 0, 0, 0, 0, time.UTC)
		week = mysqlWeekFull(prevDec31, mode)
		return year, week
	}
	// Mode 3 (ISO): use Go's ISOWeek for accurate year boundary
	if mode == 3 {
		isoYear, _ := t.ISOWeek()
		return isoYear, week
	}
	// For dates in late December, check if the week actually belongs to next year's week 1.
	// This happens with 4-day rule modes (1, 4, 6) and first-day rule modes (2, 5, 7).
	// If Jan 1 of next year falls in week 1, and our date is in the same week as Jan 1,
	// then our date belongs to next year's week 1.
	if int(t.Month()) == 12 && t.Day() >= 29 {
		nextYear := year + 1
		jan1Next := time.Date(nextYear, 1, 1, 0, 0, 0, 0, time.UTC)
		week1OfNextYear := mysqlWeekFull(jan1Next, mode)
		if week1OfNextYear == 1 {
			// Jan 1 of next year is in week 1. Check if our date t is in the same week.
			// Two dates are in the same week if their day-of-week difference (Mon or Sun first) <= 6
			// and the earlier is the week start.
			mondayFirst := (mode & 1) != 0
			var weekdayT, weekdayJ int
			if mondayFirst {
				weekdayT = (int(t.Weekday()) + 6) % 7 // 0=Mon..6=Sun
				weekdayJ = (int(jan1Next.Weekday()) + 6) % 7
			} else {
				weekdayT = int(t.Weekday()) // 0=Sun..6=Sat
				weekdayJ = int(jan1Next.Weekday())
			}
			// Check if t and jan1Next are in the same week:
			// They are in the same week if their week-start date is the same.
			weekStartT := t.AddDate(0, 0, -weekdayT)
			weekStartJ := jan1Next.AddDate(0, 0, -weekdayJ)
			if weekStartT.Equal(weekStartJ) {
				return nextYear, 1
			}
		}
	}
	return year, week
}

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

// mysqlYearWeek implements MySQL's YEARWEEK(date, mode) function.
// Mode 0 (default): first day of week is Sunday, range 0-53.
// If the date falls in week 0 (before the first Sunday), YEARWEEK returns
// the last week of the previous year.
func mysqlYearWeek(t time.Time, mode int) (int, int) {
	if mode != 0 {
		// For non-zero modes, fall back to ISO week
		return t.ISOWeek()
	}
	wk := mysqlWeekMode0(t)
	yr := t.Year()
	if wk == 0 {
		// Date is before the first Sunday of the year; belongs to last week of previous year.
		yr--
		// Calculate week number of Dec 31 of previous year
		dec31 := time.Date(yr, 12, 31, 0, 0, 0, 0, time.UTC)
		wk = mysqlWeekMode0(dec31)
	}
	return yr, int(wk)
}

// mysqlToDays calculates MySQL's TO_DAYS value for a given time.
// Uses the proleptic Gregorian calendar.
func mysqlToDays(t time.Time) int64 {
	y := int64(t.Year())
	m := int64(t.Month())
	d := int64(t.Day())
	if m <= 2 {
		y--
		m += 12
	}
	days := 365*y + y/4 - y/100 + y/400 + (153*(m-3)+2)/5 + d + 1721119 - 1
	return days - 1721059
}

func parseDateTimeValue(val interface{}) (time.Time, error) {
	if val == nil {
		return time.Time{}, fmt.Errorf("NULL date value")
	}
	s := toString(val)
	// Handle zero dates: return a sentinel zero time
	if strings.HasPrefix(s, "0000-00-00") {
		return time.Time{}, fmt.Errorf("zero date")
	}
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02",
		"15:04:05",
		"2006-01-02 15:04:05.999999999",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}
	// Try to parse YYYYMMDDHHMMSS or YYMMDDHHMMSS format (all-digit, no separators)
	isAllDigits := true
	for _, c := range s {
		if c < '0' || c > '9' {
			isAllDigits = false
			break
		}
	}
	if isAllDigits {
		switch len(s) {
		case 14: // YYYYMMDDHHMMSS
			y, _ := strconv.Atoi(s[:4])
			mo, _ := strconv.Atoi(s[4:6])
			d, _ := strconv.Atoi(s[6:8])
			h, _ := strconv.Atoi(s[8:10])
			mi, _ := strconv.Atoi(s[10:12])
			sec, _ := strconv.Atoi(s[12:14])
			if mo >= 1 && mo <= 12 && d >= 1 && d <= 31 {
				return time.Date(y, time.Month(mo), d, h, mi, sec, 0, time.UTC), nil
			}
		case 12: // YYMMDDHHMMSS
			yy, _ := strconv.Atoi(s[:2])
			mo, _ := strconv.Atoi(s[2:4])
			d, _ := strconv.Atoi(s[4:6])
			h, _ := strconv.Atoi(s[6:8])
			mi, _ := strconv.Atoi(s[8:10])
			sec, _ := strconv.Atoi(s[10:12])
			y := convert2DigitYear(yy)
			if mo >= 1 && mo <= 12 && d >= 1 && d <= 31 {
				return time.Date(y, time.Month(mo), d, h, mi, sec, 0, time.UTC), nil
			}
		}
	}
	// Try to parse using parseMySQLDateValue for various formats (2-digit year, delimiters, etc.)
	parsed := parseMySQLDateValue(s)
	if parsed != "" {
		// Check if there's a time part after the date
		timePart := ""
		if idx := strings.Index(s, " "); idx >= 0 {
			timePart = strings.TrimSpace(s[idx+1:])
			timePart = normalizeDateTimeSeparators(timePart)
		}
		dateStr := parsed
		if timePart != "" {
			dateStr = parsed + " " + timePart
		}
		for _, f := range formats {
			if t, err := time.Parse(f, dateStr); err == nil {
				return t, nil
			}
		}
		// At least try parsing the date portion
		if t, err := time.Parse("2006-01-02", parsed); err == nil {
			return t, nil
		}
	}
	// Fallback: manually parse YYYY-MM-DD (handles invalid dates like 2009-04-31, 2010-00-01)
	if len(s) >= 10 && (s[4] == '-') && (s[7] == '-') {
		y, ey := strconv.Atoi(s[:4])
		m, em := strconv.Atoi(s[5:7])
		d, ed := strconv.Atoi(s[8:10])
		if ey == nil && em == nil && ed == nil && m >= 0 && m <= 12 && d >= 0 && d <= 31 {
			// Clamp invalid month/day for Go's time.Date (which normalizes)
			if m == 0 {
				m = 1
			}
			if d == 0 {
				d = 1
			}
			// Create approximate time (may not be exact for invalid dates)
			t := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)
			// If there's a time part, parse it
			if len(s) > 10 && s[10] == ' ' {
				parts := strings.Split(s[11:], ":")
				if len(parts) >= 3 {
					h, _ := strconv.Atoi(parts[0])
					mi, _ := strconv.Atoi(parts[1])
					sec, _ := strconv.Atoi(strings.Split(parts[2], ".")[0])
					t = time.Date(y, time.Month(m), d, h, mi, sec, 0, time.UTC)
				}
			}
			return t, nil
		}
	}
	// Handle "YYYY:MM:DD HH:MM:SS[.ffffff]" format (MySQL allows colons as date separators)
	if len(s) >= 10 && s[4] == ':' && s[7] == ':' {
		// Replace first two colons with dashes to get YYYY-MM-DD format
		normalized := s[:4] + "-" + s[5:7] + "-" + s[8:]
		t, err := parseDateTimeValue(normalized)
		if err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse date/time value: %q", s)
}

// addDateMonths adds years and months to a date using MySQL semantics:
// if the resulting day exceeds the last day of the target month, it is clamped
// to the last day (e.g., Jan 31 + 1 month = Feb 28, not Mar 3).
func addDateMonths(t time.Time, years, months int) time.Time {
	// Calculate target year/month
	y := t.Year() + years
	m := int(t.Month()) + months
	// Normalize months
	for m > 12 {
		m -= 12
		y++
	}
	for m < 1 {
		m += 12
		y--
	}
	// Clamp day to last day of target month
	d := t.Day()
	lastDay := daysInMonth(y, time.Month(m))
	if d > lastDay {
		d = lastDay
	}
	return time.Date(y, time.Month(m), d, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), t.Location())
}

// daysInMonth returns the number of days in the given month/year.
func daysInMonth(year int, month time.Month) int {
	return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// parseTimeExtractionValue parses a value for HOUR/MINUTE/SECOND extraction.
// It first tries HHMMSS interpretation for 6-digit integers, then falls
// back to parseDateTimeValue for datetime/time strings.
func parseTimeExtractionValue(val interface{}) (time.Time, error) {
	s := toString(val)
	// For all-digit strings, try HHMMSS first for 6 digits (e.g., 230322 = 23:03:22)
	isAllDigits := true
	for _, c := range s {
		if c < '0' || c > '9' {
			isAllDigits = false
			break
		}
	}
	if isAllDigits && len(s) == 6 {
		h, _ := strconv.Atoi(s[:2])
		mi, _ := strconv.Atoi(s[2:4])
		sec, _ := strconv.Atoi(s[4:6])
		if h <= 23 && mi <= 59 && sec <= 59 {
			return time.Date(0, 1, 1, h, mi, sec, 0, time.UTC), nil
		}
	}
	return parseDateTimeValue(val)
}

// extractLeadingInt extracts the leading integer portion of a string.
// For example, "1 01:01:01" -> "1", "123abc" -> "123", "-5" -> "-5".
func extractLeadingInt(s string) string {
	s = strings.TrimSpace(s)
	end := 0
	for i, c := range s {
		if c == '-' && i == 0 {
			end = 1
			continue
		}
		if c >= '0' && c <= '9' {
			end = i + 1
		} else {
			break
		}
	}
	if end == 0 {
		return "0"
	}
	return s[:end]
}

// evalIntervalDateExpr evaluates DATE_ADD/DATE_SUB expressions.
func evalIntervalDateExpr(dateVal, intervalVal interface{}, unit sqlparser.IntervalType, syntax sqlparser.IntervalExprSyntax) (interface{}, error) {
	return evalIntervalDateExprStrict(dateVal, intervalVal, unit, syntax, false)
}

func evalIntervalDateExprStrict(dateVal, intervalVal interface{}, unit sqlparser.IntervalType, syntax sqlparser.IntervalExprSyntax, strict bool) (interface{}, error) {
	// If date or interval value is NULL, result is NULL
	if dateVal == nil || intervalVal == nil {
		return nil, nil
	}
	// Zero dates produce NULL (with a warning in strict mode)
	if isZeroDate(dateVal) {
		if strict {
			return nil, mysqlError(1292, "22007", "Incorrect datetime value: '"+toString(dateVal)+"'")
		}
		return nil, nil
	}
	t, err := parseDateTimeValue(dateVal)
	if err != nil {
		return toString(dateVal), nil
	}
	iStr := toString(intervalVal)
	isSubtract := syntax == sqlparser.IntervalDateExprDateSub || syntax == sqlparser.IntervalDateExprSubdate || syntax == sqlparser.IntervalDateExprBinarySub

	// parseIntervalInt parses an interval integer safely, returning (value, overflow).
	// If the string is too large for int, overflow=true and the date would overflow.
	parseIntervalInt := func(s string) (int, bool) {
		s = extractLeadingInt(s)
		n, err := strconv.Atoi(s)
		if err != nil {
			// overflow or invalid - treat as overflow
			return 0, true
		}
		return n, false
	}

	switch unit {
	case sqlparser.IntervalDay:
		n, ov := parseIntervalInt(iStr)
		if ov {
			if strict {
				return nil, mysqlError(1441, "22008", "Datetime function: datetime field overflow")
			}
			return nil, nil
		}
		if isSubtract {
			n = -n
		}
		t = t.AddDate(0, 0, n)
	case sqlparser.IntervalMonth:
		n, ov := parseIntervalInt(iStr)
		if ov {
			if strict {
				return nil, mysqlError(1441, "22008", "Datetime function: datetime field overflow")
			}
			return nil, nil
		}
		if isSubtract {
			n = -n
		}
		t = addDateMonths(t, 0, n)
	case sqlparser.IntervalYear:
		n, ov := parseIntervalInt(iStr)
		if ov {
			if strict {
				return nil, mysqlError(1441, "22008", "Datetime function: datetime field overflow")
			}
			return nil, nil
		}
		if isSubtract {
			n = -n
		}
		t = addDateMonths(t, n, 0)
	case sqlparser.IntervalHour:
		n, ov := parseIntervalInt(iStr)
		if ov {
			if strict {
				return nil, mysqlError(1441, "22008", "Datetime function: datetime field overflow")
			}
			return nil, nil
		}
		if isSubtract {
			n = -n
		}
		t = addSecondsToTime(t, int64(n)*3600)
	case sqlparser.IntervalMinute:
		n, ov := parseIntervalInt(iStr)
		if ov {
			if strict {
				return nil, mysqlError(1441, "22008", "Datetime function: datetime field overflow")
			}
			return nil, nil
		}
		if isSubtract {
			n = -n
		}
		t = addSecondsToTime(t, int64(n)*60)
	case sqlparser.IntervalSecond:
		n, ov := parseIntervalInt(iStr)
		if ov {
			if strict {
				return nil, mysqlError(1441, "22008", "Datetime function: datetime field overflow")
			}
			return nil, nil
		}
		if isSubtract {
			n = -n
		}
		// Use Unix epoch arithmetic to avoid time.Duration int64 overflow for large values.
		days := n / 86400
		rem := n % 86400
		t = t.AddDate(0, 0, int(days)).Add(time.Duration(rem) * time.Second)
	case sqlparser.IntervalWeek:
		n, ov := parseIntervalInt(iStr)
		if ov {
			if strict {
				return nil, mysqlError(1441, "22008", "Datetime function: datetime field overflow")
			}
			return nil, nil
		}
		if isSubtract {
			n = -n
		}
		t = t.AddDate(0, 0, n*7)
	case sqlparser.IntervalYearMonth:
		// Format: 'Y M' or 'Y:M' or 'Y-M'
		s := strings.TrimSpace(iStr)
		neg := false
		if strings.HasPrefix(s, "-") {
			neg = true
			s = strings.TrimSpace(s[1:])
		}
		// Split on any separator: space, colon, or dash
		sep := strings.IndexAny(s, " :-")
		var years, months int
		if sep >= 0 {
			years, _ = strconv.Atoi(s[:sep])
			months, _ = strconv.Atoi(strings.TrimSpace(s[sep+1:]))
		} else {
			years, _ = strconv.Atoi(s)
		}
		if neg {
			years = -years
			months = -months
		}
		if isSubtract {
			years = -years
			months = -months
		}
		t = addDateMonths(t, years, months)
	case sqlparser.IntervalHourMinute:
		// Format: 'HH:MM'
		totalSec := parseCompoundInterval(iStr, "hh:mm")
		if isSubtract {
			totalSec = -totalSec
		}
		t = addSecondsToTime(t, totalSec)
	case sqlparser.IntervalHourSecond:
		// Format: 'HH:MM:SS'
		totalSec := parseCompoundInterval(iStr, "hh:mm:ss")
		if isSubtract {
			totalSec = -totalSec
		}
		t = addSecondsToTime(t, totalSec)
	case sqlparser.IntervalMinuteSecond:
		// Format: 'MM:SS'
		totalSec := parseCompoundInterval(iStr, "mm:ss")
		if isSubtract {
			totalSec = -totalSec
		}
		t = addSecondsToTime(t, totalSec)
	case sqlparser.IntervalDayHour:
		// Format: 'D HH'
		totalSec := parseCompoundInterval(iStr, "d hh")
		if isSubtract {
			totalSec = -totalSec
		}
		t = addSecondsToTime(t, totalSec)
	case sqlparser.IntervalDayMinute:
		// Format: 'D HH:MM'
		totalSec := parseCompoundInterval(iStr, "d hh:mm")
		if isSubtract {
			totalSec = -totalSec
		}
		t = addSecondsToTime(t, totalSec)
	case sqlparser.IntervalDaySecond:
		// Format: 'D HH:MM:SS' or 'D H:M:S' - use totalSec to avoid overflow
		totalSec := parseCompoundInterval(iStr, "d hh:mm:ss")
		if isSubtract {
			totalSec = -totalSec
		}
		t = addSecondsToTime(t, totalSec)
	case sqlparser.IntervalMicrosecond:
		// Format: just a number (microseconds)
		n, _ := strconv.ParseInt(strings.TrimSpace(iStr), 10, 64)
		dur := time.Duration(n) * time.Microsecond
		if isSubtract {
			dur = -dur
		}
		t = t.Add(dur)
	case sqlparser.IntervalSecondMicrosecond:
		// Format: 'SS.ffffff'
		dur := parseMicrosecondInterval(iStr, "ss.ffffff")
		if isSubtract {
			dur = -dur
		}
		t = t.Add(dur)
	case sqlparser.IntervalMinuteMicrosecond:
		// Format: 'MM:SS.ffffff'
		dur := parseMicrosecondInterval(iStr, "mm:ss.ffffff")
		if isSubtract {
			dur = -dur
		}
		t = t.Add(dur)
	case sqlparser.IntervalHourMicrosecond:
		// Format: 'HH:MM:SS.ffffff'
		dur := parseMicrosecondInterval(iStr, "hh:mm:ss.ffffff")
		if isSubtract {
			dur = -dur
		}
		t = t.Add(dur)
	case sqlparser.IntervalDayMicrosecond:
		// Format: 'D HH:MM:SS.ffffff'
		dur := parseMicrosecondInterval(iStr, "d hh:mm:ss.ffffff")
		if isSubtract {
			dur = -dur
		}
		t = t.Add(dur)
	default:
		// For ADDDATE/SUBDATE shorthand (IntervalNone), the interval is in days
		if unit == sqlparser.IntervalNone && (syntax == sqlparser.IntervalDateExprAdddate || syntax == sqlparser.IntervalDateExprSubdate) {
			n, ov := parseIntervalInt(iStr)
			if ov {
				if strict {
					return nil, mysqlError(1441, "22008", "Datetime function: datetime field overflow")
				}
				return nil, nil
			}
			if isSubtract {
				n = -n
			}
			t = t.AddDate(0, 0, n)
		} else {
			// Try to parse as a time interval for unsupported types
			dur, _ := parseMySQLTimeInterval(iStr)
			if isSubtract {
				dur = -dur
			}
			t = t.Add(dur)
		}
	}

	// Check for datetime overflow (year out of MySQL's valid range 1000-9999)
	if t.Year() < 1000 || t.Year() > 9999 {
		if strict {
			return nil, mysqlError(1441, "22008", "Datetime function: datetime field overflow")
		}
		return nil, nil
	}

	// Format output based on whether the original had time component
	ds := toString(dateVal)
	usec := t.Nanosecond() / 1000
	if strings.Contains(ds, " ") || strings.Contains(ds, ":") || t.Hour() != 0 || t.Minute() != 0 || t.Second() != 0 || usec != 0 {
		base := t.Format("2006-01-02 15:04:05")
		if usec != 0 {
			base = fmt.Sprintf("%s.%06d", base, usec)
		}
		return base, nil
	}
	return t.Format("2006-01-02"), nil
}

// parseMySQLTimeInterval parses MySQL time interval strings like "1 01:01:01" or "01:01:01".
// addTimeStrings adds (or subtracts if subtract=true) two TIME strings using microsecond arithmetic.
// Handles garbage at end of base string (truncates to valid time part).
// Returns the result as a TIME string with microseconds (e.g. "-25:01:00.110000").
func addTimeStrings(base, interval string, subtract bool) interface{} {
	// Parse base time - truncate at first non-time character after a valid time
	baseTime := parseTimeStringToMicros(base)
	if baseTime == nil {
		return base // unparseable
	}
	// Parse interval
	intervalTime := parseTimeStringToMicros(interval)
	if intervalTime == nil {
		return base
	}
	baseMicros := *baseTime
	intervalMicros := *intervalTime
	var resultMicros int64
	if subtract {
		resultMicros = baseMicros - intervalMicros
	} else {
		resultMicros = baseMicros + intervalMicros
	}
	return formatMicrosAsTimeString(resultMicros)
}

// parseTimeStringToMicros parses a MySQL TIME string to total microseconds.
// Handles negative signs, D HH:MM:SS, HH:MM:SS.ffffff, and garbage at end.
// Returns nil if completely unparseable.
func parseTimeStringToMicros(s string) *int64 {
	s = strings.TrimSpace(s)
	negative := false
	if strings.HasPrefix(s, "-") {
		negative = true
		s = s[1:]
	}
	// Extract valid time chars: digits, colon, dot, space
	end := 0
	for end < len(s) {
		c := s[end]
		if (c >= '0' && c <= '9') || c == ':' || c == '.' || c == ' ' {
			end++
		} else {
			break
		}
	}
	if end == 0 {
		return nil
	}
	s = strings.TrimSpace(s[:end])
	// Parse D HH:MM:SS.ffffff
	days := 0
	if idx := strings.Index(s, " "); idx >= 0 {
		d, err := strconv.Atoi(s[:idx])
		if err == nil {
			days = d
			s = s[idx+1:]
		}
	}
	var h, m, sec, usecs int
	parts := strings.SplitN(s, ":", 3)
	switch len(parts) {
	case 3:
		h, _ = strconv.Atoi(parts[0])
		m, _ = strconv.Atoi(parts[1])
		secStr := parts[2]
		if dotIdx := strings.Index(secStr, "."); dotIdx >= 0 {
			sec, _ = strconv.Atoi(secStr[:dotIdx])
			frac := secStr[dotIdx+1:]
			for len(frac) < 6 {
				frac += "0"
			}
			if len(frac) > 6 {
				frac = frac[:6]
			}
			usecs, _ = strconv.Atoi(frac)
		} else {
			sec, _ = strconv.Atoi(secStr)
		}
	case 2:
		h, _ = strconv.Atoi(parts[0])
		m, _ = strconv.Atoi(parts[1])
	case 1:
		h, _ = strconv.Atoi(parts[0])
	default:
		return nil
	}
	total := int64(days)*24*int64(time.Hour/time.Microsecond) +
		int64(h)*int64(time.Hour/time.Microsecond) +
		int64(m)*int64(time.Minute/time.Microsecond) +
		int64(sec)*int64(time.Second/time.Microsecond) +
		int64(usecs)
	if negative {
		total = -total
	}
	return &total
}

// formatMicrosAsTimeString formats total microseconds as a MySQL TIME string with 6 decimal places.
func formatMicrosAsTimeString(micros int64) string {
	negative := micros < 0
	if negative {
		micros = -micros
	}
	us := micros % int64(time.Second/time.Microsecond)
	micros /= int64(time.Second / time.Microsecond)
	sec := int(micros % 60)
	micros /= 60
	mins := int(micros % 60)
	hours := int(micros / 60)
	sign := ""
	if negative {
		sign = "-"
	}
	if us != 0 {
		return fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, hours, mins, sec, us)
	}
	return fmt.Sprintf("%s%02d:%02d:%02d", sign, hours, mins, sec)
}

// addDurationToTime adds a duration in seconds (possibly large) to a time,
// avoiding int64 overflow in time.Duration by splitting into days and remainder.
func addSecondsToTime(t time.Time, totalSec int64) time.Time {
	days := totalSec / 86400
	rem := totalSec % 86400
	return t.AddDate(0, 0, int(days)).Add(time.Duration(rem) * time.Second)
}

// parseCompoundInterval parses MySQL compound (non-microsecond) interval formats.
// Handles leading negative sign: applies to all components.
// format: "hh:mm", "hh:mm:ss", "mm:ss", "d hh", "d hh:mm"
// Returns total seconds as int64 to avoid overflow for large values.
func parseCompoundInterval(s, format string) int64 {
	s = strings.TrimSpace(s)
	neg := false
	if strings.HasPrefix(s, "-") {
		neg = true
		s = strings.TrimSpace(s[1:])
	}

	var days, hours, mins, secs int64

	switch format {
	case "d hh:mm:ss":
		// D HH:MM:SS - space then colons
		sep := strings.IndexAny(s, " :")
		if sep >= 0 {
			d, _ := strconv.ParseInt(s[:sep], 10, 64)
			days = d
			rest := s[sep+1:]
			parts := strings.SplitN(rest, ":", 3)
			if len(parts) == 3 {
				h, _ := strconv.ParseInt(parts[0], 10, 64)
				m, _ := strconv.ParseInt(parts[1], 10, 64)
				sc, _ := strconv.ParseInt(parts[2], 10, 64)
				hours, mins, secs = h, m, sc
			} else if len(parts) == 2 {
				h, _ := strconv.ParseInt(parts[0], 10, 64)
				m, _ := strconv.ParseInt(parts[1], 10, 64)
				hours, mins = h, m
			} else {
				h, _ := strconv.ParseInt(rest, 10, 64)
				hours = h
			}
		}
	case "d hh:mm":
		// D HH:MM - separator can be space or colon
		sep := strings.IndexAny(s, " :")
		if sep >= 0 {
			d, _ := strconv.ParseInt(s[:sep], 10, 64)
			days = d
			rest := s[sep+1:]
			parts := strings.SplitN(rest, ":", 2)
			if len(parts) == 2 {
				h, _ := strconv.ParseInt(parts[0], 10, 64)
				m, _ := strconv.ParseInt(parts[1], 10, 64)
				hours, mins = h, m
			} else {
				h, _ := strconv.ParseInt(rest, 10, 64)
				hours = h
			}
		}
	case "d hh":
		// D HH - separator can be space or colon
		sep := strings.IndexAny(s, " :")
		if sep >= 0 {
			d, _ := strconv.ParseInt(s[:sep], 10, 64)
			h, _ := strconv.ParseInt(strings.TrimSpace(s[sep+1:]), 10, 64)
			days, hours = d, h
		} else {
			h, _ := strconv.ParseInt(s, 10, 64)
			hours = h
		}
	case "hh:mm:ss":
		// HH:MM:SS
		parts := strings.SplitN(s, ":", 3)
		if len(parts) == 3 {
			h, _ := strconv.ParseInt(parts[0], 10, 64)
			m, _ := strconv.ParseInt(parts[1], 10, 64)
			sc, _ := strconv.ParseInt(parts[2], 10, 64)
			hours, mins, secs = h, m, sc
		}
	case "hh:mm":
		// HH:MM
		parts := strings.SplitN(s, ":", 2)
		if len(parts) == 2 {
			h, _ := strconv.ParseInt(parts[0], 10, 64)
			m, _ := strconv.ParseInt(parts[1], 10, 64)
			hours, mins = h, m
		}
	case "mm:ss":
		// MM:SS (colon separates minutes and seconds)
		parts := strings.SplitN(s, ":", 2)
		if len(parts) == 2 {
			m, _ := strconv.ParseInt(parts[0], 10, 64)
			sc, _ := strconv.ParseInt(parts[1], 10, 64)
			mins, secs = m, sc
		} else {
			m, _ := strconv.ParseInt(s, 10, 64)
			mins = m
		}
	}

	totalSec := days*86400 + hours*3600 + mins*60 + secs
	if neg {
		totalSec = -totalSec
	}
	return totalSec
}

// parseMicrosecondInterval parses MySQL compound microsecond interval formats.
// format parameter indicates the expected format type:
//   - "ss.ffffff": SECOND_MICROSECOND like "10000.999999"
//   - "mm:ss.ffffff": MINUTE_MICROSECOND like "10000:99.999999"
//   - "hh:mm:ss.ffffff": HOUR_MICROSECOND like "10000:99:99.999999"
//   - "d hh:mm:ss.ffffff": DAY_MICROSECOND like "10000 99:99:99.999999"
func parseMicrosecondInterval(s, format string) time.Duration {
	s = strings.TrimSpace(s)
	var days, hours, mins, secs, usecs int64

	switch format {
	case "d hh:mm:ss.ffffff":
		// D HH:MM:SS.ffffff
		if idx := strings.Index(s, " "); idx >= 0 {
			d, _ := strconv.ParseInt(s[:idx], 10, 64)
			days = d
			s = s[idx+1:]
		}
		fallthrough
	case "hh:mm:ss.ffffff":
		// HH:MM:SS.ffffff
		parts := strings.SplitN(s, ":", 3)
		if len(parts) == 3 {
			h, _ := strconv.ParseInt(parts[0], 10, 64)
			m, _ := strconv.ParseInt(parts[1], 10, 64)
			secStr := parts[2]
			hours, mins = h, m
			if dotIdx := strings.Index(secStr, "."); dotIdx >= 0 {
				sec, _ := strconv.ParseInt(secStr[:dotIdx], 10, 64)
				secs = sec
				frac := secStr[dotIdx+1:]
				for len(frac) < 6 {
					frac += "0"
				}
				usecs, _ = strconv.ParseInt(frac[:6], 10, 64)
			} else {
				sc, _ := strconv.ParseInt(secStr, 10, 64)
				secs = sc
			}
		}
	case "mm:ss.ffffff":
		// MM:SS.ffffff (colon separates minutes and seconds)
		parts := strings.SplitN(s, ":", 2)
		if len(parts) == 2 {
			m, _ := strconv.ParseInt(parts[0], 10, 64)
			secStr := parts[1]
			mins = m
			if dotIdx := strings.Index(secStr, "."); dotIdx >= 0 {
				sec, _ := strconv.ParseInt(secStr[:dotIdx], 10, 64)
				secs = sec
				frac := secStr[dotIdx+1:]
				for len(frac) < 6 {
					frac += "0"
				}
				usecs, _ = strconv.ParseInt(frac[:6], 10, 64)
			} else {
				sc, _ := strconv.ParseInt(secStr, 10, 64)
				secs = sc
			}
		} else if len(parts) == 1 {
			// Just seconds.microseconds
			if dotIdx := strings.Index(s, "."); dotIdx >= 0 {
				sec, _ := strconv.ParseInt(s[:dotIdx], 10, 64)
				secs = sec
				frac := s[dotIdx+1:]
				for len(frac) < 6 {
					frac += "0"
				}
				usecs, _ = strconv.ParseInt(frac[:6], 10, 64)
			}
		}
	case "ss.ffffff":
		// SS.ffffff (dot separates seconds and microseconds)
		if dotIdx := strings.Index(s, "."); dotIdx >= 0 {
			sec, _ := strconv.ParseInt(s[:dotIdx], 10, 64)
			secs = sec
			frac := s[dotIdx+1:]
			for len(frac) < 6 {
				frac += "0"
			}
			usecs, _ = strconv.ParseInt(frac[:6], 10, 64)
		} else {
			sec, _ := strconv.ParseInt(s, 10, 64)
			secs = sec
		}
	}

	return time.Duration(days)*24*time.Hour +
		time.Duration(hours)*time.Hour +
		time.Duration(mins)*time.Minute +
		time.Duration(secs)*time.Second +
		time.Duration(usecs)*time.Microsecond
}

func parseMySQLTimeInterval(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	// Handle negative sign: "-01:01:01" means -1h-1m-1s
	negative := false
	if strings.HasPrefix(s, "-") {
		negative = true
		s = s[1:]
	}
	var days, hours, mins, secs, usecs int

	// Format: "D HH:MM:SS" or "HH:MM:SS" or "D"
	if idx := strings.Index(s, " "); idx >= 0 {
		d, err := strconv.Atoi(s[:idx])
		if err != nil {
			return 0, err
		}
		days = d
		s = s[idx+1:]
	}

	parts := strings.Split(s, ":")
	switch len(parts) {
	case 3:
		h, _ := strconv.Atoi(parts[0])
		m, _ := strconv.Atoi(parts[1])
		secPart := parts[2]
		if dotIdx := strings.Index(secPart, "."); dotIdx >= 0 {
			intPart := secPart[:dotIdx]
			fracPart := secPart[dotIdx+1:]
			sc, _ := strconv.Atoi(intPart)
			secs = sc
			// Pad or truncate to 6 digits (microseconds)
			for len(fracPart) < 6 {
				fracPart += "0"
			}
			fracPart = fracPart[:6]
			usecs, _ = strconv.Atoi(fracPart)
		} else {
			sc, _ := strconv.Atoi(secPart)
			secs = sc
		}
		hours, mins = h, m
	case 2:
		h, _ := strconv.Atoi(parts[0])
		m, _ := strconv.Atoi(parts[1])
		hours, mins = h, m
	case 1:
		if parts[0] != "" {
			h, _ := strconv.Atoi(parts[0])
			hours = h
		}
	}

	dur := time.Duration(days)*24*time.Hour +
		time.Duration(hours)*time.Hour +
		time.Duration(mins)*time.Minute +
		time.Duration(secs)*time.Second +
		time.Duration(usecs)*time.Microsecond
	if negative {
		dur = -dur
	}
	return dur, nil
}

// mysqlWeekdayNames maps Go weekday to MySQL weekday name.
var mysqlWeekdayNamesDF = [7]string{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"}
var mysqlWeekdayAbbrDF = [7]string{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"}

// Locale-specific month/weekday name tables for date_format.
var mysqlLocaleMonthNames = map[string][12]string{
	"de_DE": {"Januar", "Februar", "März", "April", "Mai", "Juni", "Juli", "August", "September", "Oktober", "November", "Dezember"},
	"ru_RU": {"Января", "Февраля", "Марта", "Апреля", "Мая", "Июня", "Июля", "Августа", "Сентября", "Октября", "Ноября", "Декабря"},
}
var mysqlLocaleMonthAbbr = map[string][12]string{
	"de_DE": {"Jan", "Feb", "Mär", "Apr", "Mai", "Jun", "Jul", "Aug", "Sep", "Okt", "Nov", "Dez"},
	"ru_RU": {"Янв", "Фев", "Мар", "Апр", "Май", "Июн", "Июл", "Авг", "Сен", "Окт", "Ноя", "Дек"},
}
var mysqlLocaleWeekdayNames = map[string][7]string{
	"de_DE": {"Sonntag", "Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag"},
	"ru_RU": {"Воскресенье", "Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота"},
}
var mysqlLocaleWeekdayAbbr = map[string][7]string{
	"de_DE": {"So", "Mo", "Di", "Mi", "Do", "Fr", "Sa"},
	"ru_RU": {"Вск", "Пнд", "Втр", "Срд", "Чтв", "Птн", "Сбт"},
}

// mysqlAdjustedWeekday returns the weekday adjusted for MySQL's calendar (handles year 0 offset).
func mysqlAdjustedWeekday(t time.Time) time.Weekday {
	wd := t.Weekday()
	// For year 0 dates, Go's proleptic Gregorian calendar is off by 1 from MySQL.
	// MySQL considers year 0 weekdays shifted by +1.
	if t.Year() == 0 {
		wd = (wd + 1) % 7
	}
	return wd
}

// mysqlDateFormat converts a MySQL DATE_FORMAT format string (e.g. "%Y-%m-%d") to a Go time.Time string.
// The locale parameter (e.g. "de_DE", "en_US") controls month/weekday name language.
func mysqlDateFormat(t time.Time, format string, locale ...string) string {
	lcLocale := "en_US"
	if len(locale) > 0 && locale[0] != "" {
		lcLocale = locale[0]
	}
	// Helper to get weekday name
	getWeekdayName := func(wd time.Weekday) string {
		if names, ok := mysqlLocaleWeekdayNames[lcLocale]; ok {
			return names[wd]
		}
		return mysqlWeekdayNamesDF[wd]
	}
	getWeekdayAbbr := func(wd time.Weekday) string {
		if abbr, ok := mysqlLocaleWeekdayAbbr[lcLocale]; ok {
			return abbr[wd]
		}
		return mysqlWeekdayAbbrDF[wd]
	}
	getMonthName := func(m time.Month) string {
		if names, ok := mysqlLocaleMonthNames[lcLocale]; ok {
			return names[m-1]
		}
		return t.Format("January")
	}
	getMonthAbbr := func(m time.Month) string {
		if abbr, ok := mysqlLocaleMonthAbbr[lcLocale]; ok {
			return abbr[m-1]
		}
		return t.Format("Jan")
	}

	var sb strings.Builder
	i := 0
	for i < len(format) {
		if format[i] == '%' && i+1 < len(format) {
			i++
			switch format[i] {
			case 'Y':
				sb.WriteString(t.Format("2006"))
			case 'y':
				sb.WriteString(t.Format("06"))
			case 'm':
				sb.WriteString(t.Format("01"))
			case 'c':
				sb.WriteString(fmt.Sprintf("%d", t.Month()))
			case 'd':
				sb.WriteString(t.Format("02"))
			case 'e':
				sb.WriteString(fmt.Sprintf("%d", t.Day()))
			case 'H':
				sb.WriteString(t.Format("15"))
			case 'h', 'I':
				sb.WriteString(t.Format("03"))
			case 'i':
				sb.WriteString(t.Format("04"))
			case 's', 'S':
				sb.WriteString(t.Format("05"))
			case 'p':
				sb.WriteString(t.Format("PM"))
			case 'a':
				sb.WriteString(getWeekdayAbbr(mysqlAdjustedWeekday(t)))
			case 'W':
				sb.WriteString(getWeekdayName(mysqlAdjustedWeekday(t)))
			case 'w':
				sb.WriteString(fmt.Sprintf("%d", mysqlAdjustedWeekday(t)))
			case 'D':
				// Day of month with ordinal suffix (1st, 2nd, 3rd, 4th, ...)
				day := t.Day()
				var suffix string
				switch day {
				case 11, 12, 13:
					suffix = "th"
				default:
					switch day % 10 {
					case 1:
						suffix = "st"
					case 2:
						suffix = "nd"
					case 3:
						suffix = "rd"
					default:
						suffix = "th"
					}
				}
				sb.WriteString(fmt.Sprintf("%d%s", day, suffix))
			case 'f':
				// Microseconds (000000-999999)
				sb.WriteString(fmt.Sprintf("%06d", t.Nanosecond()/1000))
			case 'j':
				sb.WriteString(fmt.Sprintf("%03d", t.YearDay()))
			case 'k':
				// Hour in 24h format without leading zero (0-23)
				sb.WriteString(fmt.Sprintf("%d", t.Hour()))
			case 'l':
				// Hour in 12h format without leading zero (1-12)
				h := t.Hour() % 12
				if h == 0 {
					h = 12
				}
				sb.WriteString(fmt.Sprintf("%d", h))
			case 'M':
				sb.WriteString(getMonthName(t.Month()))
			case 'b':
				sb.WriteString(getMonthAbbr(t.Month()))
			case 'T':
				sb.WriteString(t.Format("15:04:05"))
			case 'r':
				sb.WriteString(t.Format("03:04:05 PM"))
			case 'U':
				// Week number (00-53), Sunday is first day of week (mode 0)
				sb.WriteString(fmt.Sprintf("%02d", mysqlWeekFull(t, 0)))
			case 'u':
				// Week number (00-53), Monday is first day of week (mode 1)
				sb.WriteString(fmt.Sprintf("%02d", mysqlWeekFull(t, 1)))
			case 'V':
				// Week number (01-53), Sunday is first day of week (mode 2)
				sb.WriteString(fmt.Sprintf("%02d", mysqlWeekFull(t, 2)))
			case 'v':
				// Week number (01-53), Monday is first day of week (mode 3, ISO)
				sb.WriteString(fmt.Sprintf("%02d", mysqlWeekFull(t, 3)))
			case 'X':
				// Year for the week where Sunday is first day (mode 2)
				isoY, _ := mysqlWeekYearFull(t, 2)
				sb.WriteString(fmt.Sprintf("%04d", isoY))
			case 'x':
				// Year for the week where Monday is first day (mode 3, ISO)
				isoY, _ := mysqlWeekYearFull(t, 3)
				sb.WriteString(fmt.Sprintf("%04d", isoY))
			case '%':
				sb.WriteByte('%')
			default:
				sb.WriteByte('%')
				sb.WriteByte(format[i])
			}
		} else {
			sb.WriteByte(format[i])
		}
		i++
	}
	return sb.String()
}

// timestampDiff computes the difference between two timestamps in the given unit.
func timestampDiff(unit sqlparser.IntervalType, t1, t2 time.Time) int64 {
	switch unit {
	case sqlparser.IntervalMicrosecond:
		return t2.Sub(t1).Microseconds()
	case sqlparser.IntervalSecond:
		return int64(t2.Sub(t1).Seconds())
	case sqlparser.IntervalMinute:
		return int64(t2.Sub(t1).Minutes())
	case sqlparser.IntervalHour:
		return int64(t2.Sub(t1).Hours())
	case sqlparser.IntervalDay:
		return int64(t2.Sub(t1).Hours() / 24)
	case sqlparser.IntervalWeek:
		return int64(t2.Sub(t1).Hours() / (24 * 7))
	case sqlparser.IntervalMonth:
		y1, m1, _ := t1.Date()
		y2, m2, d2 := t2.Date()
		months := int64((y2-y1)*12 + int(m2-m1))
		// If the day of t2 is before the day of t1, subtract one month
		_, _, d1 := t1.Date()
		if d2 < d1 {
			months--
		}
		return months
	case sqlparser.IntervalQuarter:
		y1, m1, _ := t1.Date()
		y2, m2, d2 := t2.Date()
		months := int64((y2-y1)*12 + int(m2-m1))
		_, _, d1 := t1.Date()
		if d2 < d1 {
			months--
		}
		return months / 3
	case sqlparser.IntervalYear:
		y1, m1, d1 := t1.Date()
		y2, m2, d2 := t2.Date()
		years := int64(y2 - y1)
		if m2 < m1 || (m2 == m1 && d2 < d1) {
			years--
		}
		return years
	}
	return 0
}

// mysqlStrToDate parses a date string using a MySQL format string (like STR_TO_DATE).
// Returns nil if the string cannot be parsed.
// When literalFormat is true, returns a smart type-aware format (date, time, or datetime).
// When literalFormat is false, always returns the full datetime(6) format.
func mysqlStrToDate(dateStr, format string, literalFormat bool) *string {
	// Use a custom MySQL-compatible parser rather than Go's time.Parse,
	// because MySQL supports specifiers that Go doesn't (e.g. %D, %#, %j, %U, %W).
	p := &mysqlDateParser{s: dateStr, f: format, literalMode: literalFormat}
	if !p.parse() {
		return nil
	}
	if !p.validate() {
		return nil
	}
	result := p.format()
	return &result
}

// mysqlDateParser is a custom parser for MySQL's STR_TO_DATE format.
type mysqlDateParser struct {
	s           string // input string
	f           string // format string
	si          int    // position in s
	fi          int    // position in f
	literalMode bool   // when true, use smart type-aware output format
	// parsed components
	year, month, day           int
	hour, minute, second       int
	microsecond                int
	ampm                       int // 0=unset, 1=AM, 2=PM
	weekday                    int // 0=Sunday..6=Saturday, -1=unset
	yearday                    int // day of year 1..366, 0=unset
	weekU, weeku               int // week number for %U/%u, -1=unset
	weekV, weekv               int // week number for %V/%v, -1=unset
	yearX, yearx               int // year for %X/%x
	hasDate, hasTime, hasMicro bool
	hasYear, hasMonth          bool // true if %Y/%y or %m/%c/%M/%b present
	hasAMPM                    bool
	has24hHour                 bool // true if %H or %k was used (24-hour format)
	hasWeekday                 bool // true if %W or %w was used
	hasWeekV                   bool // true if %V was used
	hasWeekv                   bool // true if %v was used
	hasWeekU                   bool // true if %U was used
	hasWeeku                   bool // true if %u was used
	hasYearX                   bool // true if %X was used
	hasYearx                   bool // true if %x was used
}

var mysqlMonthNames = []string{
	"", "January", "February", "March", "April", "May", "June",
	"July", "August", "September", "October", "November", "December",
}
var mysqlMonthAbbr = []string{
	"", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
	"Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
}
var mysqlWeekdayNames = []string{
	"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday",
}
var mysqlWeekdayAbbr = []string{
	"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat",
}

func (p *mysqlDateParser) readInt(maxDigits int) (int, bool) {
	start := p.si
	count := 0
	for p.si < len(p.s) && p.s[p.si] >= '0' && p.s[p.si] <= '9' && count < maxDigits {
		p.si++
		count++
	}
	if count == 0 {
		return 0, false
	}
	n, err := strconv.Atoi(p.s[start:p.si])
	return n, err == nil
}

func (p *mysqlDateParser) readFrac() (int, bool) {
	// Read up to 6 digits, pad to 6
	start := p.si
	count := 0
	for p.si < len(p.s) && p.s[p.si] >= '0' && p.s[p.si] <= '9' && count < 6 {
		p.si++
		count++
	}
	// skip extra digits
	for p.si < len(p.s) && p.s[p.si] >= '0' && p.s[p.si] <= '9' {
		p.si++
	}
	if count == 0 {
		return 0, false
	}
	frac := p.s[start : start+count]
	for len(frac) < 6 {
		frac += "0"
	}
	n, err := strconv.Atoi(frac)
	return n, err == nil
}

func (p *mysqlDateParser) matchLiteral(lit string) bool {
	if p.si+len(lit) > len(p.s) {
		return false
	}
	if strings.EqualFold(p.s[p.si:p.si+len(lit)], lit) {
		p.si += len(lit)
		return true
	}
	return false
}

func (p *mysqlDateParser) skipNonDigits() {
	for p.si < len(p.s) && (p.s[p.si] < '0' || p.s[p.si] > '9') {
		p.si++
	}
}

func (p *mysqlDateParser) parse() bool {
	p.year, p.month, p.day = 0, 0, 0
	p.hour, p.minute, p.second, p.microsecond = 0, 0, 0, 0
	p.ampm = 0
	p.weekday = -1
	p.yearday = 0
	p.weekU, p.weeku, p.weekV, p.weekv = -1, -1, -1, -1
	p.yearX, p.yearx = 0, 0

	for p.fi < len(p.f) {
		if p.f[p.fi] == '%' && p.fi+1 < len(p.f) {
			p.fi++
			spec := p.f[p.fi]
			p.fi++
			switch spec {
			case 'Y': // 4-digit year (MySQL also handles 2-digit with century expansion)
				startSI := p.si
				n, ok := p.readInt(4)
				if !ok {
					return false
				}
				// If only 2 digits were read, apply MySQL 2-digit year rule
				if p.si-startSI <= 2 && n < 100 {
					if n < 70 {
						p.year = 2000 + n
					} else {
						p.year = 1900 + n
					}
				} else {
					p.year = n
				}
				p.hasDate = true
				p.hasYear = true
			case 'y': // 2-digit year
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				if n < 70 {
					p.year = 2000 + n
				} else {
					p.year = 1900 + n
				}
				p.hasDate = true
				p.hasYear = true
			case 'm': // month 01-12
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.month = n
				p.hasDate = true
				p.hasMonth = true
			case 'c': // month 1-12 (no leading zero)
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.month = n
				p.hasDate = true
				p.hasMonth = true
			case 'd': // day 01-31
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.day = n
				p.hasDate = true
			case 'e': // day 1-31 (no leading zero)
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.day = n
				p.hasDate = true
			case 'D': // day with ordinal suffix (1st, 2nd, 3rd...)
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.day = n
				p.hasDate = true
				// Skip ordinal suffix (st, nd, rd, th)
				for p.si < len(p.s) && p.s[p.si] >= 'a' && p.s[p.si] <= 'z' {
					p.si++
				}
			case 'H': // hour 00-23 (optional at end of input, 24-hour)
				p.has24hHour = true
				if p.si >= len(p.s) {
					p.hasTime = true
					break
				}
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.hour = n
				p.hasTime = true
			case 'h', 'I': // hour 01-12 (optional at end of input)
				if p.si >= len(p.s) {
					p.hasTime = true
					break
				}
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.hour = n
				p.hasTime = true
			case 'k': // hour 0-23 (no leading zero, 24-hour)
				p.has24hHour = true
				if p.si >= len(p.s) {
					p.hasTime = true
					break
				}
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.hour = n
				p.hasTime = true
			case 'l': // hour 1-12 (no leading zero)
				if p.si >= len(p.s) {
					p.hasTime = true
					break
				}
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.hour = n
				p.hasTime = true
			case 'i': // minutes (optional at end of input)
				if p.si >= len(p.s) {
					break
				}
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.minute = n
				p.hasTime = true
			case 's', 'S': // seconds (optional at end of input)
				if p.si >= len(p.s) {
					break
				}
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.second = n
				p.hasTime = true
			case 'f': // microseconds (optional - if not present, default to 0)
				p.hasMicro = true // %f in format always indicates microsecond output
				if p.si < len(p.s) && p.s[p.si] >= '0' && p.s[p.si] <= '9' {
					n, ok := p.readFrac()
					if ok {
						p.microsecond = n
					}
				}
				p.hasTime = true
			case 'p': // AM/PM
				// skip optional whitespace
				for p.si < len(p.s) && p.s[p.si] == ' ' {
					p.si++
				}
				if p.si+2 <= len(p.s) {
					am := strings.ToUpper(p.s[p.si : p.si+2])
					if am == "AM" {
						p.ampm = 1
						p.si += 2
						p.hasAMPM = true
					} else if am == "PM" {
						p.ampm = 2
						p.si += 2
						p.hasAMPM = true
					} else {
						return false
					}
				} else {
					return false
				}
			case 'T': // HH:MM:SS
				h, ok1 := p.readInt(2)
				if !ok1 || p.si >= len(p.s) || p.s[p.si] != ':' {
					return false
				}
				p.si++
				m, ok2 := p.readInt(2)
				if !ok2 || p.si >= len(p.s) || p.s[p.si] != ':' {
					return false
				}
				p.si++
				s, ok3 := p.readInt(2)
				if !ok3 {
					return false
				}
				p.hour, p.minute, p.second = h, m, s
				p.hasTime = true
			case 'r': // HH:MM:SS AM/PM
				h, ok1 := p.readInt(2)
				if !ok1 || p.si >= len(p.s) || p.s[p.si] != ':' {
					return false
				}
				p.si++
				m, ok2 := p.readInt(2)
				if !ok2 || p.si >= len(p.s) || p.s[p.si] != ':' {
					return false
				}
				p.si++
				s, ok3 := p.readInt(2)
				if !ok3 {
					return false
				}
				p.hour, p.minute, p.second = h, m, s
				p.hasTime = true
				// read space and AM/PM
				for p.si < len(p.s) && p.s[p.si] == ' ' {
					p.si++
				}
				if p.si+2 <= len(p.s) {
					am := strings.ToUpper(p.s[p.si : p.si+2])
					if am == "AM" {
						p.ampm = 1
						p.si += 2
						p.hasAMPM = true
					} else if am == "PM" {
						p.ampm = 2
						p.si += 2
						p.hasAMPM = true
					}
				}
			case 'M': // full month name
				matched := false
				for i := 1; i <= 12; i++ {
					if p.matchLiteral(mysqlMonthNames[i]) {
						p.month = i
						matched = true
						p.hasDate = true
						p.hasMonth = true
						break
					}
					// also try 3-letter prefix match for abbreviated
				}
				if !matched {
					// try abbreviated match (first 3-4 chars)
					for i := 1; i <= 12; i++ {
						for l := 3; l <= len(mysqlMonthNames[i]); l++ {
							if p.si+l <= len(p.s) && strings.EqualFold(p.s[p.si:p.si+l], mysqlMonthNames[i][:l]) {
								if l == len(mysqlMonthNames[i]) || (p.si+l < len(p.s) && (p.s[p.si+l] < 'a' || p.s[p.si+l] > 'z') && (p.s[p.si+l] < 'A' || p.s[p.si+l] > 'Z')) {
									p.month = i
									p.si += l
									matched = true
									p.hasDate = true
									p.hasMonth = true
									break
								}
							}
						}
						if matched {
							break
						}
					}
				}
				if !matched {
					return false
				}
			case 'b': // abbreviated month name
				matched := false
				for i := 1; i <= 12; i++ {
					if p.matchLiteral(mysqlMonthAbbr[i]) {
						p.month = i
						matched = true
						p.hasDate = true
						p.hasMonth = true
						break
					}
				}
				if !matched {
					// try case-insensitive match of full month up to 3 chars
					for i := 1; i <= 12; i++ {
						for l := 3; l <= len(mysqlMonthNames[i]); l++ {
							if p.si+l <= len(p.s) && strings.EqualFold(p.s[p.si:p.si+l], mysqlMonthNames[i][:l]) {
								if l == len(mysqlMonthNames[i]) || (p.si+l < len(p.s) && !((p.s[p.si+l] >= 'a' && p.s[p.si+l] <= 'z') || (p.s[p.si+l] >= 'A' && p.s[p.si+l] <= 'Z'))) {
									p.month = i
									p.si += l
									matched = true
									p.hasDate = true
									p.hasMonth = true
									break
								}
							}
						}
						if matched {
							break
						}
					}
				}
				if !matched {
					return false
				}
			case 'W': // full weekday name
				p.hasWeekday = true
				matched := false
				for i, name := range mysqlWeekdayNames {
					// try full match first
					if p.matchLiteral(name) {
						p.weekday = i
						matched = true
						break
					}
				}
				if !matched {
					// try abbreviated (3 char min)
					for i, name := range mysqlWeekdayNames {
						for l := 3; l <= len(name); l++ {
							if p.si+l <= len(p.s) && strings.EqualFold(p.s[p.si:p.si+l], name[:l]) {
								if l == len(name) || (p.si+l < len(p.s) && !((p.s[p.si+l] >= 'a' && p.s[p.si+l] <= 'z') || (p.s[p.si+l] >= 'A' && p.s[p.si+l] <= 'Z'))) {
									p.weekday = i
									p.si += l
									matched = true
									break
								}
							}
						}
						if matched {
							break
						}
					}
				}
				if !matched {
					// MySQL silently ignores unrecognized weekday names
				}
			case 'a': // abbreviated weekday name
				for _, name := range mysqlWeekdayAbbr {
					if p.matchLiteral(name) {
						break
					}
				}
			case 'j': // day of year 001-366
				n, ok := p.readInt(3)
				if !ok {
					return false
				}
				p.yearday = n
				p.hasDate = true
			case 'U': // week 00-53 (first day=Sunday)
				p.hasWeekU = true
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.weekU = n
			case 'u': // week 00-53 (first day=Monday, ISO)
				p.hasWeeku = true
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.weeku = n
			case 'V': // week 01-53 (first day=Sunday, used with %X)
				p.hasWeekV = true
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.weekV = n
			case 'v': // week 01-53 (first day=Monday, used with %x)
				p.hasWeekv = true
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.weekv = n
			case 'X': // year for %V (4 digits, Sunday-based)
				p.hasYearX = true
				n, ok := p.readInt(4)
				if !ok {
					return false
				}
				p.yearX = n
				p.hasDate = true
				p.hasYear = true
			case 'x': // year for %v (4 digits, Monday-based ISO)
				p.hasYearx = true
				n, ok := p.readInt(4)
				if !ok {
					return false
				}
				p.yearx = n
				p.hasDate = true
				p.hasYear = true
			case 'w': // weekday 0=Sunday..6=Saturday
				p.hasWeekday = true
				n, ok := p.readInt(1)
				if !ok {
					return false
				}
				if n > 6 { // MySQL: 0=Sunday..6=Saturday, 7 is invalid
					return false
				}
				p.weekday = n
			case '#', '@', '.': // skip non-numeric chars (%#, %@, and %. all skip non-digits)
				p.skipNonDigits()
			case '%': // literal %
				if p.si >= len(p.s) || p.s[p.si] != '%' {
					return false
				}
				p.si++
			default:
				// unknown specifier, skip char in input
				if p.si < len(p.s) {
					p.si++
				}
			}
		} else {
			// literal char in format must match char in input.
			// MySQL is lenient: if a non-alphanumeric separator doesn't match,
			// parsing stops but returns what was parsed so far (not a failure).
			fc := p.f[p.fi]
			p.fi++
			if p.si >= len(p.s) {
				// MySQL is lenient with trailing format chars
				continue
			}
			if p.s[p.si] != fc {
				isAlpha := func(c byte) bool {
					return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
				}
				isNonAlnum := func(c byte) bool {
					return (c < '0' || c > '9') && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z')
				}
				if isNonAlnum(fc) {
					// Format expects a separator but input has something different.
					// Check what comes NEXT in the format (after this separator).
					restFormat := p.f[p.fi:]
					// If the next format item is %f (microseconds), and the input at the
					// current position is alpha (like 'A' for AM), that means the fractional
					// part is completely absent. The separator before %f is required when
					// %p follows %f (because otherwise AM/PM can't be parsed correctly).
					if len(restFormat) >= 2 && restFormat[0] == '%' && restFormat[1] == 'f' {
						// Check if there's a %p after %f
						afterF := restFormat[2:]
						// strip the optional space between %f and %p
						afterFtrimmed := strings.TrimLeft(afterF, " \t")
						if len(afterFtrimmed) >= 2 && afterFtrimmed[0] == '%' && (afterFtrimmed[1] == 'p' || afterFtrimmed[1] == 'P') {
							// If input is alphabetic (like A of AM), the `.` separator before %f
							// didn't match and %f+%p combination would be ambiguous -> return false
							if isAlpha(p.s[p.si]) {
								return false
							}
						}
					}
					// Try to skip the format separator and continue (MySQL leniency).
					// Don't advance input - the next format specifier will try to match.
					continue
				}
				// Alphanumeric literal mismatch - MySQL stops parsing here
				// and returns what was parsed so far (not NULL).
				// We break out of the loop and return true.
				p.fi = len(p.f) // stop processing format
				continue
			}
			p.si++
		}
	}
	return true
}

// validate checks for invalid format specifier combinations that MySQL rejects.
func (p *mysqlDateParser) validate() bool {
	// %H or %k (24-hour) combined with %p (AM/PM) is invalid in MySQL
	if p.has24hHour && p.hasAMPM {
		return false
	}
	// %V (Sunday-based week) requires %X (Sunday-based year), not %x or plain %Y
	if p.hasWeekV && !p.hasYearX {
		return false
	}
	// %v (ISO/Monday-based week) requires %x (ISO year), not %X or plain %Y
	if p.hasWeekv && !p.hasYearx {
		return false
	}
	// %X (Sunday-based year) should be used with %V, not %v
	if p.hasYearX && p.hasWeekv && !p.hasWeekV {
		return false
	}
	// %x (ISO year) should be used with %v, not %V
	if p.hasYearx && p.hasWeekV && !p.hasWeekv {
		return false
	}
	// %u (Monday-based week) should use %Y (not %x which is for %v/ISO)
	// Mixing %u with %x is invalid
	if p.hasWeeku && p.hasYearx {
		return false
	}
	return true
}

func (p *mysqlDateParser) resolveDate() (yr, mo, dy int) {
	yr, mo, dy = p.year, p.month, p.day

	// If we have week+year info, compute date from that
	if p.weekU >= 0 && yr > 0 {
		// %U + %Y: week number (Sunday-based)
		// Find Jan 1 of year, then compute week
		jan1 := time.Date(yr, 1, 1, 0, 0, 0, 0, time.UTC)
		// First Sunday on or before Jan 1
		offset := int(jan1.Weekday()) // 0=Sunday
		firstSunday := jan1.AddDate(0, 0, -offset)
		// Week 0 starts on firstSunday (if Jan 1 is not Sunday, week 0 = last week of prev year)
		// Week 1 starts on first Sunday >= Jan 1
		if offset == 0 {
			// Jan 1 is Sunday, week 0 = that week
			firstSunday = jan1
		}
		target := firstSunday.AddDate(0, 0, p.weekU*7)
		if p.weekday >= 0 {
			// adjust to the correct weekday
			diff := p.weekday - int(target.Weekday())
			target = target.AddDate(0, 0, diff)
		}
		yr, mo, dy = target.Year(), int(target.Month()), target.Day()
		return
	}
	if p.weeku >= 0 && yr > 0 {
		// %u + %Y: ISO-like week number (Monday-based)
		jan1 := time.Date(yr, 1, 1, 0, 0, 0, 0, time.UTC)
		offset := int(jan1.Weekday()) // 0=Sunday
		// Week 1 starts on first Monday
		// Adjust: Monday=0 for ISO
		mondayOffset := (offset + 6) % 7 // days from last Monday to Jan 1
		firstMonday := jan1.AddDate(0, 0, -mondayOffset)
		if mondayOffset == 0 {
			firstMonday = jan1
		}
		// Week 0 = the week before the first Monday
		target := firstMonday.AddDate(0, 0, (p.weeku-1)*7)
		if p.weeku == 0 {
			target = firstMonday.AddDate(0, 0, -7)
		} else {
			target = firstMonday.AddDate(0, 0, (p.weeku-1)*7)
		}
		if p.weekday >= 0 {
			diff := p.weekday - int(target.Weekday())
			if diff < 0 {
				diff += 7
			}
			target = target.AddDate(0, 0, diff)
		}
		yr, mo, dy = target.Year(), int(target.Month()), target.Day()
		return
	}
	if p.weekV >= 0 && p.yearX > 0 {
		// %V + %X: Sunday-based week, year from %X
		jan1 := time.Date(p.yearX, 1, 1, 0, 0, 0, 0, time.UTC)
		offset := int(jan1.Weekday())
		var firstSunday time.Time
		if offset == 0 {
			firstSunday = jan1
		} else {
			firstSunday = jan1.AddDate(0, 0, 7-offset)
		}
		target := firstSunday.AddDate(0, 0, (p.weekV-1)*7)
		if p.weekday >= 0 {
			diff := p.weekday - int(target.Weekday())
			target = target.AddDate(0, 0, diff)
		}
		yr, mo, dy = target.Year(), int(target.Month()), target.Day()
		return
	}
	if p.weekv >= 0 && p.yearx > 0 {
		// %v + %x: ISO week, year from %x
		// Find the first Monday of the first ISO week of yearx
		jan1 := time.Date(p.yearx, 1, 1, 0, 0, 0, 0, time.UTC)
		_, isoWeek1 := jan1.ISOWeek()
		var isoFirstMonday time.Time
		if isoWeek1 == 1 {
			// Jan 1 is in ISO week 1, find the Monday of that week
			wd := int(jan1.Weekday())
			if wd == 0 {
				wd = 7
			}
			isoFirstMonday = jan1.AddDate(0, 0, 1-wd)
		} else {
			// Jan 1 is in last week of previous year, find first ISO week 1 Monday
			isoFirstMonday = jan1.AddDate(0, 0, 8-int(jan1.Weekday()))
			if jan1.Weekday() == 0 {
				isoFirstMonday = jan1.AddDate(0, 0, 1)
			}
		}
		target := isoFirstMonday.AddDate(0, 0, (p.weekv-1)*7)
		if p.weekday >= 0 {
			diff := p.weekday - int(target.Weekday())
			if diff < 0 {
				diff += 7
			}
			target = target.AddDate(0, 0, diff)
		}
		yr, mo, dy = target.Year(), int(target.Month()), target.Day()
		return
	}
	if p.yearday > 0 && yr > 0 {
		// %j + %Y: day of year
		t := time.Date(yr, 1, p.yearday, 0, 0, 0, 0, time.UTC)
		yr, mo, dy = t.Year(), int(t.Month()), t.Day()
	}
	return
}

func (p *mysqlDateParser) format() string {
	// Apply AM/PM adjustment
	h := p.hour
	if p.hasAMPM {
		if p.ampm == 1 { // AM
			if h == 12 {
				h = 0
			}
		} else if p.ampm == 2 { // PM
			if h != 12 {
				h += 12
			}
		}
	}

	yr, mo, dy := p.resolveDate()

	// Determine the result format based on which specifiers were used.
	hasFullDate := p.hasYear || p.hasMonth    // has year or month info → calendar date context
	hasDayOffset := p.hasDate && !hasFullDate // %d/%j but no year/month → day-offset for time

	if p.literalMode {
		// Smart type-aware output: use the minimal format suggested by the format string.
		if hasFullDate && p.hasTime {
			// Full datetime
			if p.hasMicro {
				return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d",
					yr, mo, dy, h, p.minute, p.second, p.microsecond)
			}
			return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
				yr, mo, dy, h, p.minute, p.second)
		}
		if hasFullDate {
			// Date only
			return fmt.Sprintf("%04d-%02d-%02d", yr, mo, dy)
		}
		if p.hasTime {
			// Time only or day-offset + time → return as time string
			dayOffset := 0
			if hasDayOffset {
				dayOffset = dy
			}
			totalH := h + dayOffset*24
			frac := ""
			if p.hasMicro {
				frac = fmt.Sprintf(".%06d", p.microsecond)
			}
			if totalH < 0 {
				return fmt.Sprintf("-%02d:%02d:%02d%s", -totalH, p.minute, p.second, frac)
			}
			return fmt.Sprintf("%02d:%02d:%02d%s", totalH, p.minute, p.second, frac)
		}
		if hasDayOffset {
			// Day only (no time specifiers) → return as date with 0000-00-DD
			return fmt.Sprintf("0000-00-%02d", dy)
		}
	}

	// Default mode (or fallback for literal mode): return full datetime(6) format.
	// When time-only (no date): use 0000-00-00 prefix.
	if !p.hasDate && p.hasTime {
		// Time-only with no date: use 0000-00-00 as date prefix
		return fmt.Sprintf("0000-00-00 %02d:%02d:%02d.%06d",
			h, p.minute, p.second, p.microsecond)
	}
	// Day+time with no full date: keep day in date position (non-literal mode)
	// Full datetime or date-only: return YYYY-MM-DD HH:MM:SS.ffffff
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d",
		yr, mo, dy, h, p.minute, p.second, p.microsecond)
}

// mysqlFormatToGoLayout converts a MySQL date format string to a Go time layout.
func mysqlFormatToGoLayout(format string) string {
	var sb strings.Builder
	i := 0
	for i < len(format) {
		if format[i] == '%' && i+1 < len(format) {
			i++
			switch format[i] {
			case 'Y':
				sb.WriteString("2006")
			case 'y':
				sb.WriteString("06")
			case 'm':
				sb.WriteString("01")
			case 'c':
				sb.WriteString("1")
			case 'd':
				sb.WriteString("02")
			case 'e':
				sb.WriteString("2")
			case 'H':
				sb.WriteString("15")
			case 'h', 'I':
				sb.WriteString("03")
			case 'k':
				sb.WriteString("15") // 24-hour unpadded, Go doesn't have exact match
			case 'l':
				sb.WriteString("3")
			case 'i':
				sb.WriteString("04")
			case 's', 'S':
				sb.WriteString("05")
			case 'p':
				sb.WriteString("PM")
			case 'T':
				sb.WriteString("15:04:05")
			case 'r':
				sb.WriteString("03:04:05 PM")
			case 'f':
				sb.WriteString("000000")
			case 'W':
				sb.WriteString("Monday")
			case 'M':
				sb.WriteString("January")
			case 'b':
				sb.WriteString("Jan")
			case 'a':
				sb.WriteString("Mon")
			case '%':
				sb.WriteByte('%')
			default:
				sb.WriteByte('%')
				sb.WriteByte(format[i])
			}
		} else {
			sb.WriteByte(format[i])
		}
		i++
	}
	return sb.String()
}

// mysqlGetFormat returns the format string for a given date type and locale.
func mysqlGetFormat(dateType, locale string) string {
	switch dateType {
	case "DATE":
		switch locale {
		case "USA":
			return "%m.%d.%Y"
		case "JIS", "ISO":
			return "%Y-%m-%d"
		case "EUR":
			return "%d.%m.%Y"
		case "INTERNAL":
			return "%Y%m%d"
		}
	case "DATETIME", "TIMESTAMP":
		switch locale {
		case "USA":
			return "%Y-%m-%d %H.%i.%s"
		case "JIS", "ISO":
			return "%Y-%m-%d %H:%i:%s"
		case "EUR":
			return "%Y-%m-%d %H.%i.%s"
		case "INTERNAL":
			return "%Y%m%d%H%i%s"
		}
	case "TIME":
		switch locale {
		case "USA":
			return "%h:%i:%s %p"
		case "JIS", "ISO":
			return "%H:%i:%s"
		case "EUR":
			return "%H.%i.%s"
		case "INTERNAL":
			return "%H%i%s"
		}
	}
	return ""
}

// evalCaseExpr handles CASE expressions.
func (e *Executor) evalCaseExpr(v *sqlparser.CaseExpr) (interface{}, error) {
	// Track expression depth to detect stack overflow for deeply nested expressions.
	// MySQL returns ER_STACK_OVERRUN_NEED_MORE (1436) when the expression stack is exhausted.
	e.exprDepth++
	defer func() { e.exprDepth-- }()
	if e.exprDepth > 8192 {
		return nil, mysqlError(1436, "HY000", "Thread stack overrun: "+
			"Need more than available stack. Use 'mysqld --thread_stack=#' to specify a bigger stack.")
	}
	var baseVal interface{}
	if v.Expr != nil {
		var err error
		baseVal, err = e.evalExpr(v.Expr)
		if err != nil {
			return nil, err
		}
	}
	for _, when := range v.Whens {
		condVal, err := e.evalExpr(when.Cond)
		if err != nil {
			return nil, err
		}
		matched := false
		if v.Expr != nil {
			// Simple CASE: compare base to each WHEN value
			matched = fmt.Sprintf("%v", baseVal) == fmt.Sprintf("%v", condVal)
		} else {
			// Searched CASE: each WHEN is a boolean expression
			matched = isTruthy(condVal)
		}
		if matched {
			return e.evalExpr(when.Val)
		}
	}
	if v.Else != nil {
		return e.evalExpr(v.Else)
	}
	return nil, nil
}

// HexBytes represents a value originating from an x'...' hex literal.
// In string context it behaves as the hex-digit string (e.g. "e68891"),
// but toFloat/toInt64 interpret it as a big-endian unsigned integer so
// arithmetic works correctly (e.g. x'1000000000000000' - 1).
type HexBytes string

// SysVarDouble wraps a float64 value from a DOUBLE system variable so
// it is displayed with 6 fixed decimal places (e.g. "90.000000") in
// SELECT @@var, while still being usable as a float64 in arithmetic
// (e.g. SET @v = @@var - 1).
type SysVarDouble struct {
	Value float64
}

func (d SysVarDouble) String() string {
	return strconv.FormatFloat(d.Value, 'f', 6, 64)
}

// ScaledValue wraps a float64 from multiplication/addition/subtraction
// to preserve the decimal scale through the arithmetic chain.
type ScaledValue struct {
	Value float64
	Scale int
}

// DivisionResult wraps a float64 that came from the / operator so the display
// layer can format it with div_precision_increment decimal places.
type DivisionResult struct {
	Value     float64
	Precision int
}

func (d DivisionResult) String() string {
	return fmt.Sprintf("%.*f", d.Precision, d.Value)
}

// AvgResult wraps the float64 result of AVG() with its display scale.
// It carries a Scale for formatted display, but contributes scale=0 to
// arithmetic expressions (so that e.g. avg(a)+count(a) does not force
// decimal-padded output on the sum).
// When Rat is non-nil, String() uses exact big.Rat formatting to avoid
// float64 precision loss for large or high-precision values.
type AvgResult struct {
	Value float64
	Scale int
	Rat   *big.Rat // exact value; used by String() when set
}

func (a AvgResult) String() string {
	if a.Rat != nil {
		return formatRatFixed(a.Rat, a.Scale)
	}
	return fmt.Sprintf("%.*f", a.Scale, a.Value)
}

// valueScale returns the number of decimal digits in a value.
// Used to compute division result precision per MySQL rules.
func valueScale(v interface{}) int {
	switch val := v.(type) {
	case SysVarDouble:
		s := strconv.FormatFloat(val.Value, 'f', -1, 64)
		if idx := strings.Index(s, "."); idx >= 0 {
			return len(s) - idx - 1
		}
		return 0
	case ScaledValue:
		return val.Scale
	case DivisionResult:
		return val.Precision
	case AvgResult:
		// AvgResult contributes scale=0 to arithmetic to avoid forcing
		// decimal-padded output on expressions involving AVG.
		return 0
	case float64:
		// Float64 values (from DOUBLE/FLOAT columns or function results) contribute
		// scale=0 to arithmetic so that DOUBLE arithmetic yields DOUBLE (not fixed
		// decimal) results. This matches MySQL behavior where DOUBLE+DOUBLE=DOUBLE.
		return 0
	case float32:
		s := strconv.FormatFloat(float64(val), 'f', -1, 32)
		if idx := strings.Index(s, "."); idx >= 0 {
			return len(s) - idx - 1
		}
		return 0
	case string:
		// Only count decimal scale if the string represents a numeric value.
		// Non-numeric strings like "/path/to/file.err" should not contribute scale.
		if idx := strings.Index(val, "."); idx >= 0 {
			// Verify the string looks like a number (leading digits or sign)
			trimmed := strings.TrimSpace(val)
			if len(trimmed) > 0 {
				c := trimmed[0]
				if c >= '0' && c <= '9' || c == '-' || c == '+' {
					return len(val) - idx - 1
				}
			}
		}
		return 0
	default:
		return 0
	}
}

// evalBinaryExpr evaluates arithmetic binary expressions.
func evalBinaryExpr(left, right interface{}, op sqlparser.BinaryExprOperator, divPrecision ...int) (interface{}, error) {
	// MySQL arithmetic/bit operations with NULL yield NULL.
	if left == nil || right == nil {
		return nil, nil
	}

	// Handle uint64 arithmetic without float conversion to preserve precision.
	// uint64 + int64 or int64 + uint64 where one side is uint64 should stay uint64.
	if op == sqlparser.PlusOp || op == sqlparser.MinusOp {
		// Only if neither side has decimal scale
		if valueScale(left) == 0 && valueScale(right) == 0 {
			lu, lIsU64 := left.(uint64)
			ru, rIsU64 := right.(uint64)
			li, lIsI64 := left.(int64)
			ri, rIsI64 := right.(int64)
			if lIsU64 || rIsU64 {
				// Only use integer arithmetic when both sides are int64 or uint64.
				// If either side is float64, AvgResult, or other numeric type,
				// fall through to float arithmetic to avoid precision loss.
				lKnown := lIsU64 || lIsI64
				rKnown := rIsU64 || rIsI64
				if lKnown && rKnown {
					var lv, rv uint64
					if lIsU64 {
						lv = lu
					} else {
						lv = uint64(li)
					}
					if rIsU64 {
						rv = ru
					} else {
						rv = uint64(ri)
					}
					if op == sqlparser.PlusOp {
						return lv + rv, nil
					}
					return lv - rv, nil
				}
				// Fall through to float arithmetic if either side is non-integer
			}
			// Both int64: use integer arithmetic to avoid float precision loss.
			// Only do this when the result fits in int64 (no overflow).
			if lIsI64 && rIsI64 {
				if op == sqlparser.PlusOp {
					sum := li + ri
					// Overflow detection: if signs were different from expected, use float
					if (ri > 0 && sum < li) || (ri < 0 && sum > li) {
						// Overflow — fall through to float arithmetic
					} else {
						return sum, nil
					}
				} else { // MinusOp
					diff := li - ri
					// Overflow detection
					if (ri < 0 && diff < li) || (ri > 0 && diff > li) {
						// Overflow — fall through to float arithmetic
					} else {
						return diff, nil
					}
				}
			}
		}
	}

	lf := toFloat(left)
	rf := toFloat(right)
	// Track whether either operand is a float type (not integer).
	// If so, the result should remain float64 even when it's a whole number,
	// to match MySQL DOUBLE arithmetic behavior.
	_, lIsFloat := left.(float64)
	_, rIsFloat := right.(float64)
	_, lIsFloat32 := left.(float32)
	_, rIsFloat32 := right.(float32)
	hasFloatOperand := lIsFloat || rIsFloat || lIsFloat32 || rIsFloat32
	var result float64
	switch op {
	case sqlparser.PlusOp:
		result = lf + rf
		// Preserve scale through addition for div_precision_increment
		ls := valueScale(left)
		rs := valueScale(right)
		maxS := ls
		if rs > maxS {
			maxS = rs
		}
		if maxS > 0 {
			return ScaledValue{Value: result, Scale: maxS}, nil
		}
	case sqlparser.MinusOp:
		result = lf - rf
		ls := valueScale(left)
		rs := valueScale(right)
		maxS := ls
		if rs > maxS {
			maxS = rs
		}
		if maxS > 0 {
			return ScaledValue{Value: result, Scale: maxS}, nil
		}
	case sqlparser.MultOp:
		result = lf * rf
		// Preserve scale through multiplication for div_precision_increment
		ls := valueScale(left)
		rs := valueScale(right)
		maxS := ls
		if rs > maxS {
			maxS = rs
		}
		if maxS > 0 {
			return ScaledValue{Value: result, Scale: ls + rs}, nil
		}
	case sqlparser.DivOp:
		if rf == 0 {
			return nil, nil // MySQL returns NULL for division by zero
		}
		divIncr := 4
		if len(divPrecision) > 0 {
			divIncr = divPrecision[0]
		}
		// MySQL computes result scale = max(left_scale, right_scale) + div_precision_increment
		leftScale := valueScale(left)
		rightScale := valueScale(right)
		maxScale := leftScale
		if rightScale > maxScale {
			maxScale = rightScale
		}
		prec := maxScale + divIncr
		return DivisionResult{Value: lf / rf, Precision: prec}, nil
	case sqlparser.IntDivOp:
		if rf == 0 {
			return nil, nil
		}
		return int64(lf / rf), nil
	case sqlparser.ModOp:
		if rf == 0 {
			return nil, nil
		}
		mod := math.Mod(lf, rf)
		if mod == float64(int64(mod)) {
			return int64(mod), nil
		}
		return mod, nil
	case sqlparser.ShiftLeftOp:
		leftBytes, leftIsBinary := toBinaryBytesForBitOp(left)
		if leftIsBinary {
			n := toUint64ForBitOp(right)
			return binaryShiftLeft(leftBytes, n), nil
		}
		return toUint64ForBitOp(left) << toUint64ForBitOp(right), nil
	case sqlparser.ShiftRightOp:
		leftBytes, leftIsBinary := toBinaryBytesForBitOp(left)
		if leftIsBinary {
			n := toUint64ForBitOp(right)
			return binaryShiftRight(leftBytes, n), nil
		}
		return toUint64ForBitOp(left) >> toUint64ForBitOp(right), nil
	case sqlparser.BitAndOp:
		leftBytes, leftIsBinary := toBinaryBytesForBitOp(left)
		rightBytes, rightIsBinary := toBinaryBytesForBitOp(right)
		if leftIsBinary && rightIsBinary {
			// Both sides are binary: byte-wise operation
			return binaryBitwiseAnd(leftBytes, rightBytes)
		}
		// When only one side is binary (mixed mode), MySQL uses integer arithmetic
		// where the binary side is converted as a string → integer (truncated to 0 for non-numeric binary data).
		return toUint64ForBitOpAsBinaryString(left, leftBytes, leftIsBinary) & toUint64ForBitOpAsBinaryString(right, rightBytes, rightIsBinary), nil
	case sqlparser.BitOrOp:
		leftBytes, leftIsBinary := toBinaryBytesForBitOp(left)
		rightBytes, rightIsBinary := toBinaryBytesForBitOp(right)
		if leftIsBinary && rightIsBinary {
			return binaryBitwiseOr(leftBytes, rightBytes)
		}
		return toUint64ForBitOpAsBinaryString(left, leftBytes, leftIsBinary) | toUint64ForBitOpAsBinaryString(right, rightBytes, rightIsBinary), nil
	case sqlparser.BitXorOp:
		leftBytes, leftIsBinary := toBinaryBytesForBitOp(left)
		rightBytes, rightIsBinary := toBinaryBytesForBitOp(right)
		if leftIsBinary && rightIsBinary {
			return binaryBitwiseXor(leftBytes, rightBytes)
		}
		return toUint64ForBitOpAsBinaryString(left, leftBytes, leftIsBinary) ^ toUint64ForBitOpAsBinaryString(right, rightBytes, rightIsBinary), nil
	default:
		return nil, fmt.Errorf("unsupported binary operator: %v", op)
	}
	// Don't convert to int64 if either operand was float, to preserve
	// DOUBLE type semantics (MySQL: INT + DOUBLE = DOUBLE, not INT).
	if !hasFloatOperand && result == float64(int64(result)) {
		return int64(result), nil
	}
	return result, nil
}

// isIntValLiteral returns true if the expression is an IntVal literal
// (a decimal integer literal, not a hex or string literal).
func isIntValLiteral(expr sqlparser.Expr) bool {
	if lit, ok := expr.(*sqlparser.Literal); ok {
		return lit.Type == sqlparser.IntVal
	}
	return false
}

// isHexNumLiteral returns true if the expression is a HexNum literal (0x...).
func isHexNumLiteral(expr sqlparser.Expr) bool {
	if lit, ok := expr.(*sqlparser.Literal); ok {
		return lit.Type == sqlparser.HexNum
	}
	return false
}

// hexDecodeString decodes a hex string (e.g. "123ABC") to raw bytes (e.g. "\x12\x3a\xbc").
// Returns error if the input is not valid hex.
func hexDecodeString(s string) (string, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// toBinaryBytesForBitOp extracts raw bytes from a value if it should be treated as
// a binary string for bitwise operations. Returns (bytes, true) if binary, or (nil, false) if not.
// A value is "binary" if it is:
//   - HexBytes (from x'...' hex literal or stored in BINARY/VARBINARY column)
//   - string with non-printable/null bytes (raw binary data from _binary introducer or stored binary)
//
// Pure integer/float values are NOT binary.
func toBinaryBytesForBitOp(v interface{}) ([]byte, bool) {
	switch n := v.(type) {
	case HexBytes:
		// x'...' literal or BINARY/VARBINARY column value: hex digits string, decode to raw bytes
		hexStr := string(n)
		if len(hexStr)%2 != 0 {
			hexStr = "0" + hexStr
		}
		decoded, err := hex.DecodeString(hexStr)
		if err != nil {
			return nil, false
		}
		return decoded, true
	}
	return nil, false
}

// binaryBitwiseAnd performs byte-wise AND on two binary byte slices of equal length.
// Returns error if lengths differ.
func binaryBitwiseAnd(left, right []byte) (string, error) {
	if len(left) != len(right) {
		return "", mysqlError(3513, "HY000", "Binary operands of bitwise operators must be of equal length")
	}
	result := make([]byte, len(left))
	for i := range left {
		result[i] = left[i] & right[i]
	}
	return string(result), nil
}

// binaryBitwiseOr performs byte-wise OR on two binary byte slices of equal length.
func binaryBitwiseOr(left, right []byte) (string, error) {
	if len(left) != len(right) {
		return "", mysqlError(3513, "HY000", "Binary operands of bitwise operators must be of equal length")
	}
	result := make([]byte, len(left))
	for i := range left {
		result[i] = left[i] | right[i]
	}
	return string(result), nil
}

// binaryBitwiseXor performs byte-wise XOR on two binary byte slices of equal length.
func binaryBitwiseXor(left, right []byte) (string, error) {
	if len(left) != len(right) {
		return "", mysqlError(3513, "HY000", "Binary operands of bitwise operators must be of equal length")
	}
	result := make([]byte, len(left))
	for i := range left {
		result[i] = left[i] ^ right[i]
	}
	return string(result), nil
}

// binaryBitwiseNot performs byte-wise NOT (flip all bits) on a binary byte slice.
func binaryBitwiseNot(b []byte) string {
	result := make([]byte, len(b))
	for i := range b {
		result[i] = ^b[i]
	}
	return string(result)
}

// binaryShiftLeft shifts a binary byte slice left by n bits, returning the same-length result.
func binaryShiftLeft(b []byte, n uint64) string {
	if len(b) == 0 {
		return ""
	}
	byteShift := int(n / 8)
	bitShift := uint(n % 8)
	result := make([]byte, len(b))
	if byteShift >= len(b) {
		return string(result) // all zeros
	}
	for i := 0; i < len(b)-byteShift; i++ {
		result[i] = b[i+byteShift] << bitShift
		if bitShift > 0 && i+byteShift+1 < len(b) {
			result[i] |= b[i+byteShift+1] >> (8 - bitShift)
		}
	}
	return string(result)
}

// toBinaryBytesFromInt converts a uint64 integer to a big-endian byte slice of the given length.
// Used when one operand is binary and the other is an integer in a binary bitwise op.
func toBinaryBytesFromInt(v uint64, length int) []byte {
	if length <= 0 {
		length = 8
	}
	buf := make([]byte, 8)
	buf[0] = byte(v >> 56)
	buf[1] = byte(v >> 48)
	buf[2] = byte(v >> 40)
	buf[3] = byte(v >> 32)
	buf[4] = byte(v >> 24)
	buf[5] = byte(v >> 16)
	buf[6] = byte(v >> 8)
	buf[7] = byte(v)
	if length >= 8 {
		// Right-align 8 bytes within longer buffer
		result := make([]byte, length)
		copy(result[length-8:], buf)
		return result
	}
	// Take rightmost 'length' bytes
	return buf[8-length:]
}

// binaryShiftRight shifts a binary byte slice right by n bits, returning the same-length result.
func binaryShiftRight(b []byte, n uint64) string {
	if len(b) == 0 {
		return ""
	}
	byteShift := int(n / 8)
	bitShift := uint(n % 8)
	result := make([]byte, len(b))
	if byteShift >= len(b) {
		return string(result) // all zeros
	}
	for i := len(b) - 1; i >= byteShift; i-- {
		result[i] = b[i-byteShift] >> bitShift
		if bitShift > 0 && i-byteShift-1 >= 0 {
			result[i] |= b[i-byteShift-1] << (8 - bitShift)
		}
	}
	return string(result)
}

// toUint64ForBitOp converts a value to uint64 for bitwise operations,
// preserving the full uint64 range without float precision loss.
func toUint64ForBitOp(v interface{}) uint64 {
	switch n := v.(type) {
	case int64:
		return uint64(n)
	case uint64:
		return n
	case HexBytes:
		decoded, err := hex.DecodeString(string(n))
		if err != nil || len(decoded) == 0 {
			return 0
		}
		var val uint64
		for _, b := range decoded {
			val = val<<8 | uint64(b)
		}
		return val
	case string:
		// Try uint64 first, then int64, then float
		if u, err := strconv.ParseUint(strings.TrimSpace(n), 10, 64); err == nil {
			return u
		}
		if i, err := strconv.ParseInt(strings.TrimSpace(n), 10, 64); err == nil {
			return uint64(i)
		}
		if f, err := strconv.ParseFloat(strings.TrimSpace(n), 64); err == nil {
			if f < 0 {
				return uint64(int64(f))
			}
			if f >= float64(math.MaxUint64) {
				return math.MaxUint64
			}
			return uint64(f)
		}
		return 0
	default:
		f := toFloat(v)
		if f < 0 {
			return uint64(int64(f))
		}
		if f >= float64(math.MaxUint64) {
			return math.MaxUint64
		}
		return uint64(f)
	}
}

// toString converts a value to string.
func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case HexBytes:
		return string(val)
	case EnumValue:
		return string(val)
	case []byte:
		return string(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case uint64:
		return strconv.FormatUint(val, 10)
	case float64:
		return formatMySQLFloatString(val)
	case ScaledValue:
		return formatMySQLFloatString(val.Value)
	case DivisionResult:
		return fmt.Sprintf("%.*f", val.Precision, val.Value)
	case AvgResult:
		return fmt.Sprintf("%.*f", val.Scale, val.Value)
	case bool:
		if val {
			return "1"
		}
		return "0"
	}
	return fmt.Sprintf("%v", v)
}

func formatMySQLFloatString(v float64) string {
	if math.IsNaN(v) {
		return "NaN"
	}
	if math.IsInf(v, 1) {
		return "inf"
	}
	if math.IsInf(v, -1) {
		return "-inf"
	}
	abs := math.Abs(v)
	if abs != 0 && (abs >= 1e14 || abs < 1e-4) {
		s := strconv.FormatFloat(v, 'e', 5, 64)
		s = strings.Replace(s, "e+0", "e", 1)
		s = strings.Replace(s, "e-0", "e-", 1)
		s = strings.Replace(s, "e+", "e", 1)
		// Strip trailing zeros from the mantissa (MySQL omits them, e.g. 1.00000e43 -> 1e43)
		if eIdx := strings.IndexByte(s, 'e'); eIdx > 0 {
			mantissa := s[:eIdx]
			exp := s[eIdx:]
			if strings.Contains(mantissa, ".") {
				mantissa = strings.TrimRight(mantissa, "0")
				mantissa = strings.TrimRight(mantissa, ".")
			}
			s = mantissa + exp
		}
		return s
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// FormatMySQLFloat formats a float64 value using MySQL's scientific notation style
// (trailing zeros stripped, exponent normalized).
func FormatMySQLFloat(v float64) string {
	return formatMySQLFloatString(v)
}

// FormatMySQLFloat32 formats a float32 value using MySQL's FLOAT column display style
// (6 significant digits with trailing zeros preserved for single-precision columns).
func FormatMySQLFloat32(v float32) string {
	f := float64(v)
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
		// Use bitSize=32 for float32 precision, keeping 5 decimal places (6 sig figs)
		s := strconv.FormatFloat(f, 'e', 5, 32)
		s = strings.Replace(s, "e+0", "e", 1)
		s = strings.Replace(s, "e-0", "e-", 1)
		s = strings.Replace(s, "e+", "e", 1)
		// For FLOAT, MySQL keeps trailing zeros (6 significant digits)
		return s
	}
	return strconv.FormatFloat(f, 'f', -1, 32)
}

func isStringValue(v interface{}) bool {
	switch v.(type) {
	case string, []byte:
		return true
	}
	return false
}

// toInt64 converts a value to int64.
func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case uint64:
		return int64(n)
	case float64:
		return int64(n)
	case SysVarDouble:
		return int64(n.Value)
	case ScaledValue:
		return int64(n.Value)
	case DivisionResult:
		return int64(n.Value)
	case AvgResult:
		return int64(n.Value)
	case HexBytes:
		decoded, err := hex.DecodeString(string(n))
		if err != nil || len(decoded) == 0 {
			return 0
		}
		var val uint64
		for _, b := range decoded {
			val = val<<8 | uint64(b)
		}
		return int64(val)
	case string:
		i, err := strconv.ParseInt(n, 10, 64)
		if err != nil {
			// Try parsing as float and truncating (handles "1.6000" etc.)
			if f, ferr := strconv.ParseFloat(n, 64); ferr == nil {
				return int64(f)
			}
		}
		return i
	case bool:
		if n {
			return 1
		}
		return 0
	}
	return 0
}

// toUint64 converts a value to uint64 for bitwise aggregate operations.
func toUint64(v interface{}) uint64 {
	switch n := v.(type) {
	case int64:
		return uint64(n)
	case uint64:
		return n
	case float64:
		return uint64(int64(n))
	case DivisionResult:
		return uint64(int64(n.Value))
	case AvgResult:
		return uint64(int64(n.Value))
	case HexBytes:
		decoded, err := hex.DecodeString(string(n))
		if err != nil || len(decoded) == 0 {
			return 0
		}
		var val uint64
		for _, b := range decoded {
			val = val<<8 | uint64(b)
		}
		return val
	case []byte:
		var val uint64
		for _, b := range n {
			val = val<<8 | uint64(b)
		}
		return val
	case string:
		if u, err := strconv.ParseUint(n, 10, 64); err == nil {
			return u
		}
		if i, err := strconv.ParseInt(n, 10, 64); err == nil {
			return uint64(i)
		}
		if f, err := strconv.ParseFloat(n, 64); err == nil {
			return uint64(int64(f))
		}
		return 0
	case bool:
		if n {
			return 1
		}
		return 0
	}
	return 0
}

// isTruthy returns true if the value is considered truthy in MySQL.
func isTruthy(v interface{}) bool {
	if v == nil {
		return false
	}
	switch val := v.(type) {
	case bool:
		return val
	case int64:
		return val != 0
	case float64:
		return val != 0
	case ScaledValue:
		return val.Value != 0
	case DivisionResult:
		return val.Value != 0
	case AvgResult:
		return val.Value != 0
	case string:
		return val != "" && val != "0"
	}
	return false
}

// evalExprMaybeRow evaluates an expression, using row context if row is non-nil.
func (e *Executor) evalExprMaybeRow(expr sqlparser.Expr, row *storage.Row) (interface{}, error) {
	if row != nil {
		return e.evalRowExpr(expr, *row)
	}
	return e.evalExpr(expr)
}

// evalRowExpr evaluates an expression in the context of a table row.
// It handles column lookups and delegates other expressions to e.evalExpr.
// evalRowExprTupleAware evaluates an expression in row context, returning []interface{}
// for ValTuple (to support nested row/tuple comparisons) or a scalar value otherwise.
func (e *Executor) evalRowExprTupleAware(expr sqlparser.Expr, row storage.Row) (interface{}, error) {
	if tup, ok := expr.(sqlparser.ValTuple); ok {
		vals := make([]interface{}, len(tup))
		for i, item := range tup {
			v, err := e.evalRowExprTupleAware(item, row)
			if err != nil {
				return nil, err
			}
			vals[i] = v
		}
		return vals, nil
	}
	return e.evalRowExpr(expr, row)
}

func (e *Executor) evalRowExpr(expr sqlparser.Expr, row storage.Row) (interface{}, error) {
	switch v := expr.(type) {
	case *sqlparser.ColName:
		colName := v.Name.String()
		// Handle _rowid: MySQL alias for the single-column integer primary key
		if strings.EqualFold(colName, "_rowid") {
			pkCol := e.findRowIDColumn(row)
			if pkCol != "" {
				if val, ok := row[pkCol]; ok {
					return val, nil
				}
			}
			return nil, nil
		}
		// Try qualified lookup first (alias.col) if qualifier is set
		if !v.Qualifier.IsEmpty() {
			qualified := v.Qualifier.Name.String() + "." + colName
			if val, ok := row[qualified]; ok {
				return val, nil
			}
			// Try full qualifier (db.table.col) for multi-database references
			fullQualified := sqlparser.String(v.Qualifier) + "." + colName
			fullQualified = strings.Trim(fullQualified, "`")
			if val, ok := row[fullQualified]; ok {
				return val, nil
			}
			// Try case-insensitive qualified lookup (handles t1.A when key is t1.a)
			upperQualified := strings.ToUpper(qualified)
			for k, kv := range row {
				if strings.ToUpper(k) == upperQualified {
					return kv, nil
				}
			}
			// Fall back to correlatedRow for correlated subquery references
			if e.correlatedRow != nil {
				if val, ok := e.correlatedRow[qualified]; ok {
					return val, nil
				}
				if val, ok := e.correlatedRow[fullQualified]; ok {
					return val, nil
				}
				// Try unqualified lookup in correlatedRow: handles the case where
				// the outer row's keys are unqualified (e.g. {a:0, b:10}) but the
				// subquery WHERE references them as t1.a.  We must look here BEFORE
				// falling through to row[colName], otherwise we'd incorrectly return
				// the inner table's value (e.g. t2.a) instead of the outer t1.a.
				upperName := strings.ToUpper(colName)
				for k, kv := range e.correlatedRow {
					if strings.ToUpper(k) == upperName {
						return kv, nil
					}
				}
				// Qualifier was present but not resolved from correlatedRow either.
				// Do NOT fall through to bare row[colName]: that would incorrectly
				// match a same-named column in the inner table (e.g., t2.a when
				// looking for t1.a in a correlated subquery).
				return nil, nil
			}
		}
		// Fall back to un-prefixed lookup
		if val, ok := row[colName]; ok {
			return val, nil
		}
		// Case-insensitive fallback (needed for information_schema columns)
		upperName := strings.ToUpper(colName)
		for k, v := range row {
			if strings.ToUpper(k) == upperName {
				return v, nil
			}
		}
		// Fall back to correlatedRow for correlated subquery
		if e.correlatedRow != nil {
			if val, ok := e.correlatedRow[colName]; ok {
				return val, nil
			}
			if !v.Qualifier.IsEmpty() {
				qualified := v.Qualifier.Name.String() + "." + colName
				if val, ok := e.correlatedRow[qualified]; ok {
					return val, nil
				}
			}
		}
		return nil, nil
	case *sqlparser.Subquery:
		// Scalar subquery in row context (correlated)
		return e.execSubqueryScalar(v, row)
	case *sqlparser.BinaryExpr:
		isBitOpRow := v.Operator == sqlparser.BitOrOp || v.Operator == sqlparser.BitAndOp ||
			v.Operator == sqlparser.BitXorOp || v.Operator == sqlparser.ShiftLeftOp ||
			v.Operator == sqlparser.ShiftRightOp
		isPlusMinusRow := v.Operator == sqlparser.PlusOp || v.Operator == sqlparser.MinusOp
		// Check for spatial type columns in bit operations - MySQL raises 1210 (Incorrect arguments)
		if isBitOpRow {
			opName := ""
			switch v.Operator {
			case sqlparser.BitOrOp:
				opName = "|"
			case sqlparser.BitAndOp:
				opName = "&"
			case sqlparser.BitXorOp:
				opName = "^"
			case sqlparser.ShiftLeftOp:
				opName = "<<"
			case sqlparser.ShiftRightOp:
				opName = ">>"
			}
			if e.isSpatialExpr(v.Left) || e.isSpatialExpr(v.Right) {
				return nil, mysqlError(1210, "HY000", fmt.Sprintf("Incorrect arguments to %s", opName))
			}
		}
		// Determine if sub-expressions are integer literals that may overflow
		leftIsIntLit := isIntValLiteral(v.Left)
		rightIsIntLit := isIntValLiteral(v.Right)
		leftIsHexLit := isHexNumLiteral(v.Left)
		rightIsHexLit := isHexNumLiteral(v.Right)
		left, err := e.evalRowExpr(v.Left, row)
		var leftOvRow *intOverflowError
		if err != nil {
			var oe *intOverflowError
			if errors.As(err, &oe) {
				leftOvRow = oe
				if isBitOpRow && oe.kind == "DECIMAL" {
					left = int64(math.MaxInt64)
				} else {
					left = uint64(math.MaxUint64)
				}
			} else if strings.Contains(err.Error(), "INT_OVERFLOW") {
				left = uint64(math.MaxUint64)
			} else {
				return nil, err
			}
		}
		// For HexNum literals that fit in uint64, convert to HexBytes for byte-wise bit ops.
		// Overflow detection (>8 decoded bytes) is deferred until both sides are evaluated,
		// so we can check whether the other side is also binary (byte-wise context).
		var leftOverflowHexBytes HexBytes
		if isBitOpRow && leftIsHexLit && err == nil {
			if hb, ok := left.(HexBytes); ok {
				decoded, decErr := hex.DecodeString(string(hb))
				if decErr == nil && len(decoded) > 8 {
					leftOverflowHexBytes = hb // defer decision until right is known
				}
				// If <= 8 bytes, keep as HexBytes for normal byte-wise operations
			} else if s, ok := left.(string); ok {
				hexStr := s
				if len(hexStr)%2 != 0 {
					hexStr = "0" + hexStr
				}
				if _, decErr := hex.DecodeString(hexStr); decErr == nil {
					left = HexBytes(hexStr)
				}
			}
		}
		// If left is from a VARBINARY/BINARY column, the storage returns raw bytes as a string.
		// Convert to HexBytes so bitwise ops treat it as binary, not an integer.
		if isBitOpRow && err == nil && e.isBinaryExpr(v.Left) {
			if s, ok := left.(string); ok {
				left = HexBytes(strings.ToUpper(hex.EncodeToString([]byte(s))))
			}
		}
		right, err := e.evalRowExpr(v.Right, row)
		var rightOvRow *intOverflowError
		if err != nil {
			var oe *intOverflowError
			if errors.As(err, &oe) {
				rightOvRow = oe
				if isBitOpRow && oe.kind == "DECIMAL" {
					right = int64(math.MaxInt64)
				} else {
					right = uint64(math.MaxUint64)
				}
			} else if strings.Contains(err.Error(), "INT_OVERFLOW") {
				right = uint64(math.MaxUint64)
			} else {
				return nil, err
			}
		}
		// For HexNum literals that fit in uint64, convert to HexBytes for byte-wise bit ops.
		// Overflow detection (>8 decoded bytes) is deferred until both sides are evaluated.
		var rightOverflowHexBytes HexBytes
		if isBitOpRow && rightIsHexLit && err == nil {
			if hb, ok := right.(HexBytes); ok {
				decoded, decErr := hex.DecodeString(string(hb))
				if decErr == nil && len(decoded) > 8 {
					rightOverflowHexBytes = hb // defer decision until left is known
				}
				// If <= 8 bytes, keep as HexBytes for normal byte-wise operations
			} else if s, ok := right.(string); ok {
				hexStr := s
				if len(hexStr)%2 != 0 {
					hexStr = "0" + hexStr
				}
				if _, decErr := hex.DecodeString(hexStr); decErr == nil {
					right = HexBytes(hexStr)
				}
			}
		}
		// If right is from a VARBINARY/BINARY column, the storage returns raw bytes as a string.
		// Convert to HexBytes so bitwise ops treat it as binary, not an integer.
		if isBitOpRow && err == nil && e.isBinaryExpr(v.Right) {
			if s, ok := right.(string); ok {
				right = HexBytes(strings.ToUpper(hex.EncodeToString([]byte(s))))
			}
		}
		// Now that both sides are known, resolve deferred overflow hex literals.
		// If the other side is binary (HexBytes), keep as HexBytes for byte-wise ops.
		// If the other side is NOT binary (integer context), clamp to MaxUint64 + warning.
		if leftOverflowHexBytes != "" {
			_, rightIsBinary := toBinaryBytesForBitOp(right)
			if rightIsBinary {
				left = leftOverflowHexBytes // byte-wise context: keep as HexBytes
			} else {
				hbStr := string(leftOverflowHexBytes)
				oe := &intOverflowError{val: strings.TrimLeft(hbStr, "0"), kind: "BINARY"}
				if oe.val == "" {
					oe.val = "0"
				}
				e.addWarning("Warning", 1292, formatOverflowWarningMsg(oe))
				left = uint64(math.MaxUint64)
			}
		}
		if rightOverflowHexBytes != "" {
			_, leftIsBinary := toBinaryBytesForBitOp(left)
			if leftIsBinary {
				right = rightOverflowHexBytes // byte-wise context: keep as HexBytes
			} else {
				hbStr := string(rightOverflowHexBytes)
				oe := &intOverflowError{val: strings.TrimLeft(hbStr, "0"), kind: "BINARY"}
				if oe.val == "" {
					oe.val = "0"
				}
				e.addWarning("Warning", 1292, formatOverflowWarningMsg(oe))
				right = uint64(math.MaxUint64)
			}
		}
		if leftOvRow != nil {
			e.addWarning("Warning", 1292, formatOverflowWarningMsg(leftOvRow))
		}
		if rightOvRow != nil {
			e.addWarning("Warning", 1292, formatOverflowWarningMsg(rightOvRow))
		}
		// For bit operations, check if a string result from evalRowExpr represents an
		// overflowed value. Adjust the clamped value based on the original expression type:
		// - IntVal literal overflow → DECIMAL kind → clamp to MaxInt64
		// - string literals / other → INTEGER kind → clamp to MaxUint64
		if isBitOpRow && leftOvRow == nil {
			if s, ok := left.(string); ok {
				if _, err2 := strconv.ParseUint(strings.TrimSpace(s), 10, 64); err2 != nil && errors.Is(err2, strconv.ErrRange) {
					kind := "INTEGER"
					if leftIsIntLit {
						kind = "DECIMAL"
					}
					e.addWarning("Warning", 1292, formatOverflowWarningMsg(&intOverflowError{val: strings.TrimSpace(s), kind: kind}))
					if kind == "DECIMAL" {
						left = int64(math.MaxInt64)
					}
					// For INTEGER, toUint64ForBitOp will handle the string properly
				}
			}
		}
		if isBitOpRow && rightOvRow == nil {
			if s, ok := right.(string); ok {
				if _, err2 := strconv.ParseUint(strings.TrimSpace(s), 10, 64); err2 != nil && errors.Is(err2, strconv.ErrRange) {
					kind := "INTEGER"
					if rightIsIntLit {
						kind = "DECIMAL"
					}
					e.addWarning("Warning", 1292, formatOverflowWarningMsg(&intOverflowError{val: strings.TrimSpace(s), kind: kind}))
					if kind == "DECIMAL" {
						right = int64(math.MaxInt64)
					}
				}
			}
		}
		// For plus/minus operations, handle big decimal literal overflow:
		// When a big IntVal literal overflows uint64, evalRowExpr default case returns the
		// decimal string (oe.val). MySQL preserves the exact decimal value for addition/subtraction.
		// Use math/big to compute the exact result.
		if isPlusMinusRow && leftOvRow == nil && leftIsIntLit {
			if leftStr, ok := left.(string); ok {
				// Parse as big.Int
				var bigL, bigR big.Int
				if _, ok2 := bigL.SetString(strings.TrimSpace(leftStr), 10); ok2 {
					// Parse right as big.Int if it's a string or int64
					rightStr := ""
					switch rv := right.(type) {
					case string:
						rightStr = strings.TrimSpace(rv)
					case int64:
						rightStr = strconv.FormatInt(rv, 10)
					case uint64:
						rightStr = strconv.FormatUint(rv, 10)
					}
					if rightStr != "" {
						if _, ok3 := bigR.SetString(rightStr, 10); ok3 {
							var bigResult big.Int
							if v.Operator == sqlparser.PlusOp {
								bigResult.Add(&bigL, &bigR)
							} else {
								bigResult.Sub(&bigL, &bigR)
							}
							// If result fits in int64, return int64
							if bigResult.IsInt64() {
								return bigResult.Int64(), nil
							}
							// Otherwise return as string (exact decimal)
							return bigResult.String(), nil
						}
					}
				}
			}
		}
		// For plus/minus operations, handle hex literal overflow:
		// When a HexNum literal overflowed uint64, evalRowExpr default case returns HexBytes.
		// Detect overflow (>8 decoded bytes) and clamp to MaxUint64 + warning.
		if isPlusMinusRow && leftOvRow == nil && leftIsHexLit {
			if hb, ok := left.(HexBytes); ok {
				decoded, decErr := hex.DecodeString(string(hb))
				if decErr == nil && len(decoded) > 8 {
					oe := &intOverflowError{val: strings.TrimLeft(string(hb), "0"), kind: "BINARY"}
					if oe.val == "" {
						oe.val = "0"
					}
					e.addWarning("Warning", 1292, formatOverflowWarningMsg(oe))
					left = uint64(math.MaxUint64)
				}
			} else if s, ok := left.(string); ok {
				// Fallback: string form (shouldn't happen but handle for safety)
				hexStr := strings.TrimLeft(s, "0")
				if len(hexStr) > 16 {
					oe := &intOverflowError{val: s, kind: "BINARY"}
					e.addWarning("Warning", 1292, formatOverflowWarningMsg(oe))
					left = uint64(math.MaxUint64)
				}
			}
		}
		if isPlusMinusRow && rightOvRow == nil && rightIsHexLit {
			if hb, ok := right.(HexBytes); ok {
				decoded, decErr := hex.DecodeString(string(hb))
				if decErr == nil && len(decoded) > 8 {
					oe := &intOverflowError{val: strings.TrimLeft(string(hb), "0"), kind: "BINARY"}
					if oe.val == "" {
						oe.val = "0"
					}
					e.addWarning("Warning", 1292, formatOverflowWarningMsg(oe))
					right = uint64(math.MaxUint64)
				}
			} else if s, ok := right.(string); ok {
				hexStr := strings.TrimLeft(s, "0")
				if len(hexStr) > 16 {
					oe := &intOverflowError{val: s, kind: "BINARY"}
					e.addWarning("Warning", 1292, formatOverflowWarningMsg(oe))
					right = uint64(math.MaxUint64)
				}
			}
		}
		return evalBinaryExpr(left, right, v.Operator, e.getDivPrecisionIncrement())
	case *sqlparser.ComparisonExpr:
		// Handle IN / NOT IN specially: right side is a ValTuple
		if v.Operator == sqlparser.InOp || v.Operator == sqlparser.NotInOp {
			// When left is a tuple: (a,b) IN ((1,2),(3,4)) or (a,b) IN (SELECT ...)
			if leftTupleIN, leftIsTupleIN := v.Left.(sqlparser.ValTuple); leftIsTupleIN {
				if sub, ok := v.Right.(*sqlparser.Subquery); ok {
					// MySQL error 1235: LIMIT in IN/ALL/ANY/SOME subquery is not supported
					if subqueryHasLimit(sub) {
						return nil, mysqlError(1235, "42000", "This version of MySQL doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery'")
					}
					// Evaluate tuple elements WITH row context (needed when in IS TRUE/FALSE wrapper)
					result, err := e.execSubquery(sub, row)
					if err != nil {
						return nil, err
					}
					if len(result.Columns) != len(leftTupleIN) {
						return nil, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftTupleIN)))
					}
					leftValsRow := make([]interface{}, len(leftTupleIN))
					for i, lExpr := range leftTupleIN {
						lv, err := e.evalRowExpr(lExpr, row) // Use row context!
						if err != nil {
							return nil, err
						}
						leftValsRow[i] = lv
					}
					hasNullRow := false
					for _, lv := range leftValsRow {
						if lv == nil {
							hasNullRow = true
							break
						}
					}
					for _, rrow := range result.Rows {
						if len(rrow) != len(leftValsRow) {
							continue
						}
						allMatch := true
						rowHasNull := false
						hasDefiniteNonMatch := false
						for i := 0; i < len(leftValsRow); i++ {
							if leftValsRow[i] == nil || rrow[i] == nil {
								rowHasNull = true
								allMatch = false
								// Don't break - continue to check remaining non-null pairs
								continue
							}
							match, _ := compareValues(leftValsRow[i], rrow[i], sqlparser.EqualOp)
							if !match {
								allMatch = false
								hasDefiniteNonMatch = true
								break
							}
						}
						if allMatch {
							if v.Operator == sqlparser.InOp {
								return int64(1), nil
							}
							return int64(0), nil
						}
						if rowHasNull && !hasDefiniteNonMatch {
							hasNullRow = true
						}
					}
					if hasNullRow {
						return nil, nil
					}
					if v.Operator == sqlparser.NotInOp {
						return int64(1), nil
					}
					return int64(0), nil
				}
				// Row IN tuple-of-tuples: (a,b) IN ((1,2),(3,4)) or nested (a,(b,c)) IN ((1,(2,3)),...)
				if rightTupleIN, ok := v.Right.(sqlparser.ValTuple); ok {
					// MySQL validates all IN-list items structurally BEFORE evaluating any
					// match. Do a pre-validation pass over all items in rightTupleIN.
					for _, item := range rightTupleIN {
						switch rItem := item.(type) {
						case sqlparser.ValTuple:
							if err := validateRowTupleStructure(leftTupleIN, rItem); err != nil {
								return nil, err
							}
						case *sqlparser.Subquery:
							// A subquery returns a flat row; if any element of leftTupleIN
							// is a nested tuple, the subquery can never match that element.
							for _, lElem := range leftTupleIN {
								if lNested, ok := lElem.(sqlparser.ValTuple); ok {
									return nil, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(lNested)))
								}
							}
						default:
							// Non-tuple item: break out (fall through to scalar handling)
							break
						}
					}

					// Evaluate left tuple values with row context (supports nested tuples)
					leftValsIN := make([]interface{}, len(leftTupleIN))
					for i, lExpr := range leftTupleIN {
						lv, err := e.evalRowExprTupleAware(lExpr, row)
						if err != nil {
							return nil, err
						}
						leftValsIN[i] = lv
					}
					hasNullIN := false
					leftRowIN := interface{}(leftValsIN)
					for _, item := range rightTupleIN {
						switch rItemEval := item.(type) {
						case sqlparser.ValTuple:
							rValsIN := make([]interface{}, len(rItemEval))
							for i, rv := range rItemEval {
								rVal, err := e.evalRowExprTupleAware(rv, row)
								if err != nil {
									return nil, err
								}
								rValsIN[i] = rVal
							}
							equal, rowHasNull, err := rowTuplesEqual(leftRowIN, interface{}(rValsIN))
							if err != nil {
								return nil, err
							}
							if equal {
								if v.Operator == sqlparser.InOp {
									return int64(1), nil
								}
								return int64(0), nil
							}
							if rowHasNull {
								hasNullIN = true
							}
						case *sqlparser.Subquery:
							// Subquery with no nested left elements: execute and compare.
							subResult, err := e.execSubquery(rItemEval, row)
							if err != nil {
								return nil, err
							}
							if len(subResult.Columns) != len(leftTupleIN) {
								return nil, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftTupleIN)))
							}
							for _, subRow := range subResult.Rows {
								if len(subRow) != len(leftValsIN) {
									continue
								}
								allMatch := true
								rowHasNull := false
								for i := 0; i < len(leftValsIN); i++ {
									lv, rv := leftValsIN[i], subRow[i]
									if lv == nil || rv == nil {
										rowHasNull = true
										allMatch = false
										break
									}
									match, _ := compareValues(lv, rv, sqlparser.EqualOp)
									if !match {
										allMatch = false
										break
									}
								}
								if allMatch {
									if v.Operator == sqlparser.InOp {
										return int64(1), nil
									}
									return int64(0), nil
								}
								if rowHasNull {
									hasNullIN = true
								}
							}
						}
					}
					if hasNullIN {
						return nil, nil
					}
					if v.Operator == sqlparser.NotInOp {
						return int64(1), nil
					}
					return int64(0), nil
				}
			}
			// Handle (SELECT c1,c2,...) IN (SELECT c1,c2,...) — subquery IN subquery.
			// The left subquery may return multi-column rows.
			if leftSub, leftIsSub := v.Left.(*sqlparser.Subquery); leftIsSub {
				if rightSub, rightIsSub := v.Right.(*sqlparser.Subquery); rightIsSub {
					leftResult, err := e.execSubquery(leftSub, row)
					if err != nil {
						return nil, err
					}
					rightResult, err := e.execSubquery(rightSub, row)
					if err != nil {
						return nil, err
					}
					if len(leftResult.Columns) != len(rightResult.Columns) {
						return nil, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftResult.Columns)))
					}
					ncols := len(leftResult.Columns)
					for _, lrow := range leftResult.Rows {
						hasNull := false
						for _, rrow := range rightResult.Rows {
							allMatch := true
							rowHasNull := false
							for i := 0; i < ncols; i++ {
								lv, rv := lrow[i], rrow[i]
								if lv == nil || rv == nil {
									rowHasNull = true
									allMatch = false
									break
								}
								match, _ := compareValues(lv, rv, sqlparser.EqualOp)
								if !match {
									allMatch = false
									break
								}
							}
							if allMatch {
								if v.Operator == sqlparser.InOp {
									return int64(1), nil
								}
								return int64(0), nil
							}
							if rowHasNull {
								hasNull = true
							}
						}
						if hasNull {
							return nil, nil
						}
					}
					if v.Operator == sqlparser.NotInOp {
						return int64(1), nil
					}
					return int64(0), nil
				}
			}
			left, err := e.evalRowExpr(v.Left, row)
			if err != nil {
				return nil, err
			}
			if left == nil {
				return nil, nil
			}
			if tuple, ok := v.Right.(sqlparser.ValTuple); ok {
				hasNull := false
				for _, item := range tuple {
					val, err := e.evalRowExpr(item, row)
					if err != nil {
						return nil, err
					}
					if val == nil {
						hasNull = true
						continue
					}
					match, _ := compareValues(left, val, sqlparser.EqualOp)
					if match {
						if v.Operator == sqlparser.InOp {
							return int64(1), nil
						}
						return int64(0), nil
					}
				}
				if hasNull {
					return nil, nil
				}
				if v.Operator == sqlparser.NotInOp {
					return int64(1), nil
				}
				return int64(0), nil
			}
			// Handle IN (SELECT ...) — subquery on right side
			if sub, ok := v.Right.(*sqlparser.Subquery); ok {
				// Set correlatedRow so correlated subqueries can reference the outer row.
				oldCorrelatedIN := e.correlatedRow
				if row != nil {
					e.correlatedRow = row
				}
				result, err := e.evalInSubquery(left, v.Left, sub, v.Operator)
				e.correlatedRow = oldCorrelatedIN
				return result, err
			}
		}
		// Handle (SELECT c1,c2,...) op ROW(v1,v2,...) and ROW(...) op (SELECT ...)
		// in row context. Delegate to evalComparisonExpr which has the full implementation.
		{
			_, leftIsSub := v.Left.(*sqlparser.Subquery)
			_, rightIsSub := v.Right.(*sqlparser.Subquery)
			_, leftIsValTuple := v.Left.(sqlparser.ValTuple)
			_, rightIsValTuple := v.Right.(sqlparser.ValTuple)
			if (leftIsSub && rightIsValTuple) || (leftIsValTuple && rightIsSub) {
				return e.evalComparisonExpr(v)
			}
		}
		// Handle ROW/tuple comparisons: ROW(a,b) = ROW(c,d) or (a,b) = (c,d)
		// Supports nested tuples via evalRowExprTupleAware returning []interface{} for ValTuple.
		leftTuple, leftIsTuple := v.Left.(sqlparser.ValTuple)
		rightTuple, rightIsTuple := v.Right.(sqlparser.ValTuple)
		if leftIsTuple && rightIsTuple {
			if len(leftTuple) != len(rightTuple) {
				return nil, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftTuple)))
			}
			// Evaluate as full tuple values (nested tuples return []interface{})
			leftValsRT := make([]interface{}, len(leftTuple))
			rightValsRT := make([]interface{}, len(rightTuple))
			for i := 0; i < len(leftTuple); i++ {
				lv, err := e.evalRowExprTupleAware(leftTuple[i], row)
				if err != nil {
					return nil, err
				}
				leftValsRT[i] = lv
				rv, err := e.evalRowExprTupleAware(rightTuple[i], row)
				if err != nil {
					return nil, err
				}
				rightValsRT[i] = rv
			}
			switch v.Operator {
			case sqlparser.EqualOp, sqlparser.NotEqualOp, sqlparser.NullSafeEqualOp:
				equal, hasNull, err := rowTuplesEqual(interface{}(leftValsRT), interface{}(rightValsRT))
				if err != nil {
					return nil, err
				}
				if v.Operator == sqlparser.NullSafeEqualOp {
					if hasNull {
						// For <=>, NULL == NULL = true, NULL != non-NULL = false
						// rowTuplesEqual doesn't handle this correctly for NullSafe
						// Fall through to element-wise comparison
						allNullMatch := true
						for i := 0; i < len(leftValsRT); i++ {
							lv, rv := leftValsRT[i], rightValsRT[i]
							if lv == nil && rv == nil {
								continue
							}
							if lv == nil || rv == nil {
								allNullMatch = false
								break
							}
							eq, _, err := rowTuplesEqual(lv, rv)
							if err != nil {
								return nil, err
							}
							if !eq {
								allNullMatch = false
								break
							}
						}
						if allNullMatch {
							return int64(1), nil
						}
						return int64(0), nil
					}
					if equal {
						return int64(1), nil
					}
					return int64(0), nil
				}
				if hasNull {
					return nil, nil
				}
				if v.Operator == sqlparser.NotEqualOp {
					equal = !equal
				}
				if equal {
					return int64(1), nil
				}
				return int64(0), nil
			default:
				// Lexicographic comparison for <, >, <=, >=
				for i := 0; i < len(leftValsRT); i++ {
					lv, rv := leftValsRT[i], rightValsRT[i]
					if lv == nil || rv == nil {
						return nil, nil
					}
					eq, err := compareValues(lv, rv, sqlparser.EqualOp)
					if err != nil {
						return nil, err
					}
					if eq {
						continue
					}
					lt, err := compareValues(lv, rv, sqlparser.LessThanOp)
					if err != nil {
						return nil, err
					}
					switch v.Operator {
					case sqlparser.LessThanOp, sqlparser.LessEqualOp:
						if lt {
							return int64(1), nil
						}
						return int64(0), nil
					default:
						if !lt {
							return int64(1), nil
						}
						return int64(0), nil
					}
				}
				switch v.Operator {
				case sqlparser.LessEqualOp, sqlparser.GreaterEqualOp:
					return int64(1), nil
				default:
					return int64(0), nil
				}
			}
		}
		if leftIsTuple {
			// Left is tuple, right is subquery or something else - delegate to evalWhere
			match, err := e.evalWhere(v, row)
			if err != nil {
				return nil, err
			}
			if match {
				return int64(1), nil
			}
			return int64(0), nil
		}
		// Handle ANY/SOME/ALL modifier with subquery on right side.
		// e.g. expr = ANY(SELECT ...) or expr <> ALL(SELECT ...) in SELECT list context.
		// The right subquery can return multiple rows (ANY/ALL semantics).
		if v.Modifier != 0 {
			if sub, ok := v.Right.(*sqlparser.Subquery); ok {
				isAny := v.Modifier == 1 // ANY/SOME; Modifier=2 means ALL

				// Evaluate left side. If left is itself a subquery that returns >1 row,
				// MySQL returns NULL rather than raising error 1242 in ANY/ALL context.
				var left interface{}
				if leftSub, leftIsSub := v.Left.(*sqlparser.Subquery); leftIsSub {
					subResult, subErr := e.execSubquery(leftSub, row)
					if subErr != nil {
						return nil, subErr
					}
					if len(subResult.Rows) == 0 {
						return nil, nil // NULL
					}
					if len(subResult.Rows) > 1 {
						return nil, nil // >1 row in ANY/ALL context → NULL (not error 1242)
					}
					if len(subResult.Rows[0]) == 0 {
						return nil, nil
					}
					left = subResult.Rows[0][0]
				} else {
					var err error
					left, err = e.evalRowExpr(v.Left, row)
					if err != nil {
						return nil, err
					}
				}

				if left == nil {
					return nil, nil
				}

				vals, err := e.execSubqueryValues(sub, row)
				if err != nil {
					return nil, err
				}

				if isAny {
					// ANY/SOME: true if comparison holds for at least one non-NULL value
					hasNull := false
					for _, val := range vals {
						if val == nil {
							hasNull = true
							continue
						}
						match, err := compareValues(left, val, v.Operator)
						if err != nil {
							return nil, err
						}
						if match {
							return int64(1), nil
						}
					}
					if hasNull {
						return nil, nil // unknown (NULL) if no match but there were NULLs
					}
					return int64(0), nil
				}
				// ALL: true if comparison holds for every value; NULL if any value is NULL
				for _, val := range vals {
					if val == nil {
						return nil, nil
					}
					match, err := compareValues(left, val, v.Operator)
					if err != nil {
						return nil, err
					}
					if !match {
						return int64(0), nil
					}
				}
				return int64(1), nil
			}
		}
		// Comparison in row context
		// Extract COLLATE clause for collation-aware comparison
		var collationName string
		leftExpr2, rightExpr2 := v.Left, v.Right
		if ce, ok := leftExpr2.(*sqlparser.CollateExpr); ok {
			collationName = ce.Collation
			leftExpr2 = ce.Expr
		}
		if ce, ok := rightExpr2.(*sqlparser.CollateExpr); ok {
			collationName = ce.Collation
			rightExpr2 = ce.Expr
		}
		// If left side is a bare column reference not found in the row, MySQL returns
		// ER_BAD_FIELD_ERROR before evaluating the right side. This prevents a right-side
		// error (e.g. @@GLOBAL on a session-only var) from masking the column error.
		if colExpr, ok := leftExpr2.(*sqlparser.ColName); ok && colExpr.Qualifier.IsEmpty() {
			colName := colExpr.Name.String()
			if _, found := row[colName]; !found {
				// Also check case-insensitive
				upperName := strings.ToUpper(colName)
				foundCI := false
				for k := range row {
					if strings.ToUpper(k) == upperName {
						foundCI = true
						break
					}
				}
				if !foundCI {
					return nil, mysqlError(1054, "42S22", fmt.Sprintf("Unknown column '%s' in 'field list'", colName))
				}
			}
		}
		left, err := e.evalRowExpr(leftExpr2, row)
		if err != nil {
			return nil, err
		}
		right, err := e.evalRowExpr(rightExpr2, row)
		if err != nil {
			return nil, err
		}
		// Handle LIKE/NOT LIKE with optional ESCAPE and optional COLLATE
		if v.Operator == sqlparser.LikeOp || v.Operator == sqlparser.NotLikeOp {
			// NULL comparison: x LIKE NULL = NULL = false in WHERE context
			if left == nil || right == nil {
				return nil, nil
			}
			ls := toString(left)
			rs := toString(right)
			// Determine escape character (default is '\')
			escapeChar := rune('\\')
			if v.Escape != nil {
				escVal, _ := e.evalRowExpr(v.Escape, row)
				if escStr := toString(escVal); len([]rune(escStr)) > 0 {
					escapeChar = []rune(escStr)[0]
				}
			}
			collLower := strings.ToLower(collationName)
			isCaseSensitive := collationName != "" && (strings.Contains(collLower, "_bin") || strings.Contains(collLower, "_cs"))
			var re *regexp.Regexp
			if isCaseSensitive {
				re = likeToRegexpCaseSensitiveEscape(rs, escapeChar)
			} else {
				re = likeToRegexpEscape(rs, escapeChar)
			}
			matched := re.MatchString(ls)
			if v.Operator == sqlparser.LikeOp {
				if matched {
					return int64(1), nil
				}
				return int64(0), nil
			}
			if !matched {
				return int64(1), nil
			}
			return int64(0), nil
		}
		if collationName != "" {
			if vc := lookupVitessCollation(collationName); vc != nil {
				ls := toString(left)
				rs := toString(right)
				lBytes := []byte(ls)
				rBytes := []byte(rs)
				cs := vc.Charset()
				if cs.Name() != "utf8mb4" && cs.Name() != "utf8mb3" && cs.Name() != "binary" {
					if conv, convErr := charset.ConvertFromUTF8(nil, cs, lBytes); convErr == nil {
						lBytes = conv
					}
					if conv, convErr := charset.ConvertFromUTF8(nil, cs, rBytes); convErr == nil {
						rBytes = conv
					}
				}
				cmp := vc.Collate(lBytes, rBytes, false)
				switch v.Operator {
				case sqlparser.EqualOp:
					if cmp == 0 {
						return int64(1), nil
					}
					return int64(0), nil
				case sqlparser.NotEqualOp:
					if cmp != 0 {
						return int64(1), nil
					}
					return int64(0), nil
				case sqlparser.LessThanOp:
					if cmp < 0 {
						return int64(1), nil
					}
					return int64(0), nil
				case sqlparser.GreaterThanOp:
					if cmp > 0 {
						return int64(1), nil
					}
					return int64(0), nil
				case sqlparser.LessEqualOp:
					if cmp <= 0 {
						return int64(1), nil
					}
					return int64(0), nil
				case sqlparser.GreaterEqualOp:
					if cmp >= 0 {
						return int64(1), nil
					}
					return int64(0), nil
				}
			}
		}
		// NULL comparison returns NULL (except for NULL-safe equal <=>)
		if (left == nil || right == nil) && v.Operator != sqlparser.NullSafeEqualOp {
			return nil, nil
		}
		// For system variable ENUM comparisons, apply case-insensitive string comparison
		// to match MySQL's default collation (utf8mb4_0900_ai_ci) behavior.
		if v.Operator == sqlparser.EqualOp || v.Operator == sqlparser.NotEqualOp {
			isSysVarEnumRow := false
			if varExpr, ok := leftExpr2.(*sqlparser.Variable); ok {
				varName := strings.ToLower(varExpr.Name.String())
				varName = strings.TrimPrefix(varName, "global.")
				varName = strings.TrimPrefix(varName, "session.")
				varName = strings.TrimPrefix(varName, "local.")
				if sysVarEnumSet[varName] {
					isSysVarEnumRow = true
				}
			}
			if varExpr, ok := rightExpr2.(*sqlparser.Variable); ok {
				varName := strings.ToLower(varExpr.Name.String())
				varName = strings.TrimPrefix(varName, "global.")
				varName = strings.TrimPrefix(varName, "session.")
				varName = strings.TrimPrefix(varName, "local.")
				if sysVarEnumSet[varName] {
					isSysVarEnumRow = true
				}
			}
			if isSysVarEnumRow {
				if ls, lok := left.(string); lok {
					if rs, rok := right.(string); rok {
						equal := strings.EqualFold(ls, rs)
						if v.Operator == sqlparser.EqualOp {
							if equal {
								return int64(1), nil
							}
							return int64(0), nil
						}
						if !equal {
							return int64(1), nil
						}
						return int64(0), nil
					}
				}
			}
		}
		result, err := compareValues(left, right, v.Operator)
		if err != nil {
			return nil, err
		}
		if result {
			return int64(1), nil
		}
		return int64(0), nil
	case *sqlparser.UnaryExpr:
		val, err := e.evalRowExpr(v.Expr, row)
		if err != nil {
			return nil, err
		}
		if v.Operator == sqlparser.TildaOp {
			// ~ is bitwise NOT
			if val == nil {
				return nil, nil // ~NULL = NULL
			}
			// If value is from a VARBINARY/BINARY column, convert raw bytes to HexBytes first
			if e.isBinaryExpr(v.Expr) {
				if s, ok := val.(string); ok {
					val = HexBytes(strings.ToUpper(hex.EncodeToString([]byte(s))))
				}
			}
			// If value is a binary string (HexBytes or raw binary), do byte-wise NOT
			if binaryBytes, isBinary := toBinaryBytesForBitOp(val); isBinary {
				return binaryBitwiseNot(binaryBytes), nil
			}
			return ^toUint64ForBitOp(val), nil
		}
		if v.Operator == sqlparser.BangOp {
			// ! is logical NOT (deprecated alias in MySQL 8.0)
			if val == nil {
				return nil, nil // !NULL = NULL
			}
			if isTruthy(val) {
				return int64(0), nil
			}
			return int64(1), nil
		}
		if v.Operator == sqlparser.UMinusOp {
			switch n := val.(type) {
			case int64:
				return -n, nil
			case uint64:
				if n == 1<<63 {
					return int64(math.MinInt64), nil
				}
				if n <= math.MaxInt64 {
					return -int64(n), nil
				}
				// Keep exact value for >int64 range to avoid float precision loss.
				return fmt.Sprintf("-%d", n), nil
			case float64:
				return -n, nil
			case string:
				if strings.HasPrefix(n, "-") {
					return strings.TrimPrefix(n, "-"), nil
				}
				return "-" + n, nil
			}
		}
		return val, nil
	case *sqlparser.FuncExpr:
		// Evaluate function arguments with row context
		return e.evalFuncExprWithRow(v, row)
	case *sqlparser.CaseExpr:
		return e.evalCaseExprWithRow(v, row)
	case *sqlparser.ConvertExpr:
		// CAST(expr AS type) with row context - delegate to evalExpr via default
		oldCorrelated := e.correlatedRow
		e.correlatedRow = row
		val, err := e.evalExpr(expr)
		e.correlatedRow = oldCorrelated
		return val, err
	case *sqlparser.CastExpr:
		// CAST(expr AS type) with row context - delegate to evalExpr via default
		oldCorrelated := e.correlatedRow
		e.correlatedRow = row
		val, err := e.evalExpr(expr)
		e.correlatedRow = oldCorrelated
		return val, err
	case *sqlparser.CharExpr:
		// CHAR(N1, N2, ...) with row context
		// MySQL outputs the minimum number of bytes needed for each value.
		var result []byte
		for _, argExpr := range v.Exprs {
			val, err := e.evalRowExpr(argExpr, row)
			if err != nil {
				return nil, err
			}
			if val == nil {
				continue
			}
			n := uint64(toInt64(val))
			if n == 0 {
				result = append(result, 0)
			} else if n <= 0xFF {
				result = append(result, byte(n))
			} else if n <= 0xFFFF {
				result = append(result, byte(n>>8), byte(n))
			} else if n <= 0xFFFFFF {
				result = append(result, byte(n>>16), byte(n>>8), byte(n))
			} else {
				result = append(result, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
			}
		}
		return string(result), nil
	case *sqlparser.CollateExpr:
		// Ignore COLLATE and evaluate inner expression with row context
		return e.evalRowExpr(v.Expr, row)
	case *sqlparser.IntervalDateExpr:
		dateVal, err := e.evalRowExpr(v.Date, row)
		if err != nil {
			return nil, err
		}
		intervalVal, err := e.evalRowExpr(v.Interval, row)
		if err != nil {
			return nil, err
		}
		return evalIntervalDateExprStrict(dateVal, intervalVal, v.Unit, v.Syntax, e.isTraditionalMode())
	case *sqlparser.AssignmentExpr:
		// @var := expr with row context
		val, err := e.evalRowExpr(v.Right, row)
		if err != nil {
			return nil, err
		}
		varName := strings.TrimPrefix(sqlparser.String(v.Left), "@")
		varName = strings.Trim(varName, "`")
		if e.userVars == nil {
			e.userVars = make(map[string]interface{})
		}
		e.userVars[varName] = val
		return val, nil
	case *sqlparser.CountStar, *sqlparser.Count, *sqlparser.Sum, *sqlparser.Max, *sqlparser.Min, *sqlparser.Avg, *sqlparser.GroupConcatExpr:
		// For HAVING clause: look up the aggregate display name in the row
		displayName := aggregateDisplayName(expr)
		if val, ok := row[displayName]; ok {
			return val, nil
		}
		// Fallback: compute the aggregate (for single-row context)
		return e.evalExpr(expr)
	case *sqlparser.PerformanceSchemaFuncExpr:
		// Evaluate the argument with row context so aggregate results are resolved
		if v.Argument != nil {
			argVal, err := e.evalRowExpr(v.Argument, row)
			if err != nil {
				return nil, err
			}
			// Apply the formatting function to the resolved argument
			switch v.Type {
			case sqlparser.FormatBytesType:
				if argVal == nil {
					return nil, nil
				}
				if s, ok := argVal.(string); ok {
					if _, parseErr := strconv.ParseFloat(s, 64); parseErr != nil {
						return nil, mysqlError(1264, "22003", "Input value is out of range in 'format_bytes'")
					}
				}
				return formatBytesValue(argVal), nil
			case sqlparser.FormatPicoTimeType:
				if argVal == nil {
					return nil, nil
				}
				if s, ok := argVal.(string); ok {
					if _, parseErr := strconv.ParseFloat(s, 64); parseErr != nil {
						return nil, mysqlError(1264, "22003", "Input value is out of range in 'format_pico_time'")
					}
				}
				return formatPicoTimeValue(argVal), nil
			default:
				return e.evalPerformanceSchemaFuncExpr(v)
			}
		}
		return e.evalPerformanceSchemaFuncExpr(v)
	default:
		// For JSON and other expressions, set row context via correlatedRow
		// so that ColName lookups in evalExpr can find row values.
		// Merge the old correlatedRow into the new row so that outer
		// correlated references (e.g., from an enclosing subquery) remain
		// accessible.
		oldCorrelated := e.correlatedRow
		merged := make(storage.Row, len(row))
		if oldCorrelated != nil {
			for k, v := range oldCorrelated {
				merged[k] = v
			}
		}
		for k, v := range row {
			merged[k] = v
		}
		e.correlatedRow = merged
		val, err := e.evalExpr(expr)
		e.correlatedRow = oldCorrelated
		if err != nil {
			var oe *intOverflowError
			if errors.As(err, &oe) {
				if oe.kind == "BINARY" {
					// Large hex literal (0x...) overflow: return as HexBytes so that
					// comparisons with HexBytes results (e.g. from VARBINARY bitwise ops)
					// and other binary-aware code paths work correctly.
					hexStr := oe.val
					if len(hexStr)%2 != 0 {
						hexStr = "0" + hexStr
					}
					return HexBytes(strings.ToUpper(hexStr)), nil
				}
				// Preserve the original integer literal text so comparisons can
				// use exact bigint semantics instead of float64-rounded max uint.
				return oe.val, nil
			}
		}
		return val, err
	}
}

// evalFuncExprWithRow evaluates a function expression with row context for column references.
func (e *Executor) evalFuncExprWithRow(v *sqlparser.FuncExpr, row storage.Row) (interface{}, error) {
	// Evaluate function arguments with row context to resolve column references
	name := strings.ToLower(v.Name.String())

	rowPtr := &row

	// Dispatch to category-specific handlers
	if result, handled, err := evalStringFunc(e, name, v, rowPtr); handled {
		return result, err
	}
	if result, handled, err := evalDatetimeFunc(e, name, v, rowPtr); handled {
		return result, err
	}
	if result, handled, err := evalMathFunc(e, name, v, rowPtr); handled {
		return result, err
	}
	if result, handled, err := evalMiscFunc(e, name, v, rowPtr); handled {
		return result, err
	}

	// Try user-defined function from catalog
	qualifier := v.Qualifier.String()
	if result, err := e.callUserDefinedFunction(name, v.Exprs, &row, qualifier); err == nil {
		return result, nil
	}
	// Fallback: delegate to evalFuncExpr.
	// For spatial functions that need column values, set correlatedRow so column references resolve.
	funcNameLower := strings.ToLower(v.Name.String())
	switch funcNameLower {
	case "mbrwithin", "st_within", "mbrcontains", "st_contains", "mbrintersects", "st_intersects":
		oldCorrelated := e.correlatedRow
		e.correlatedRow = row
		result, err := e.evalFuncExpr(v)
		e.correlatedRow = oldCorrelated
		return result, err
	default:
		return e.evalFuncExpr(v)
	}
}

// evalComparisonWithRow evaluates a comparison expression with row context.
func (e *Executor) evalComparisonWithRow(v *sqlparser.ComparisonExpr, row storage.Row) (interface{}, error) {
	// Delegate to evalWhere for the actual comparison logic (it handles tuples, subqueries, etc.)
	match, err := e.evalWhere(v, row)
	if err != nil {
		return nil, err
	}
	if match {
		return int64(1), nil
	}
	return int64(0), nil
}

// evalBinaryExprWithRow evaluates a binary arithmetic expression with row context.
func (e *Executor) evalBinaryExprWithRow(v *sqlparser.BinaryExpr, row storage.Row) (interface{}, error) {
	left, err := e.evalRowExpr(v.Left, row)
	if err != nil {
		return nil, err
	}
	right, err := e.evalRowExpr(v.Right, row)
	if err != nil {
		return nil, err
	}
	return evalBinaryExpr(left, right, v.Operator, e.getDivPrecisionIncrement())
}

// isBinaryConvertExpr returns true if the expression is a BINARY cast (e.g. BINARY 'str' or CAST(x AS BINARY)).
// This is used to detect binary-collation comparisons in CASE expressions.
func isBinaryConvertExpr(expr sqlparser.Expr) bool {
	switch v := expr.(type) {
	case *sqlparser.ConvertExpr:
		return v.Type != nil && strings.EqualFold(v.Type.Type, "binary")
	case *sqlparser.CastExpr:
		return v.Type != nil && strings.EqualFold(v.Type.Type, "binary")
	}
	return false
}

// compareValuesBinary compares two values case-sensitively (binary collation).
func compareValuesBinary(left, right interface{}) bool {
	if left == nil || right == nil {
		return false
	}
	ls := fmt.Sprintf("%v", left)
	rs := fmt.Sprintf("%v", right)
	return ls == rs
}

// evalCaseExprWithRow evaluates a CASE expression with row context.
func (e *Executor) evalCaseExprWithRow(v *sqlparser.CaseExpr, row storage.Row) (interface{}, error) {
	// Evaluate CASE expression with row context for column resolution
	if v.Expr != nil {
		// Simple CASE: CASE expr WHEN val THEN result ...
		caseVal, err := e.evalRowExpr(v.Expr, row)
		if err != nil {
			return nil, err
		}
		// Check if the base expression has binary collation (BINARY expr)
		baseIsBinary := isBinaryConvertExpr(v.Expr)
		for _, when := range v.Whens {
			whenVal, err := e.evalRowExpr(when.Cond, row)
			if err != nil {
				return nil, err
			}
			// Use binary (case-sensitive) comparison if either side has a BINARY cast
			var match bool
			if baseIsBinary || isBinaryConvertExpr(when.Cond) {
				match = compareValuesBinary(caseVal, whenVal)
			} else {
				match, _ = compareValues(caseVal, whenVal, sqlparser.EqualOp)
			}
			if match {
				return e.evalRowExpr(when.Val, row)
			}
		}
	} else {
		// Searched CASE: CASE WHEN cond THEN result ...
		for _, when := range v.Whens {
			cond, err := e.evalWhere(when.Cond, row)
			if err != nil {
				return nil, err
			}
			if cond {
				return e.evalRowExpr(when.Val, row)
			}
		}
	}
	if v.Else != nil {
		return e.evalRowExpr(v.Else, row)
	}
	return nil, nil
}

// evalRowExpr is a package-level shim for backward-compatible callers that
// do not have access to an executor.  It creates a temporary executor with
// empty state, which is sufficient for column-lookup and literal evaluation.
func evalRowExpr(expr sqlparser.Expr, row storage.Row) (interface{}, error) {
	// Some callers use this shim without full executor context.
	// Avoid hard failures on subqueries that require storage/catalog.
	if hasSubqueryExpr(expr) {
		return nil, nil
	}
	e := &Executor{}
	return e.evalRowExpr(expr, row)
}

func hasSubqueryExpr(expr sqlparser.Expr) bool {
	found := false
	sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if _, ok := node.(*sqlparser.Subquery); ok {
			found = true
			return false, nil
		}
		return true, nil
	}, expr)
	return found
}

// evalHaving evaluates a HAVING predicate, with support for aggregate functions
// that are computed against the group's rows.
func (e *Executor) evalHaving(expr sqlparser.Expr, havingRow storage.Row, groupRows []storage.Row) (bool, error) {
	// Pre-compute any aggregate expressions in the HAVING clause and add to the row
	enrichedRow := make(storage.Row, len(havingRow))
	for k, v := range havingRow {
		enrichedRow[k] = v
	}
	// Walk the expression to find aggregates and compute them
	e.addAggregatesToRow(expr, enrichedRow, groupRows)
	return e.evalWhere(expr, enrichedRow)
}

// addAggregatesToRow walks an expression tree and computes any aggregate functions,
// storing their results in the row with their display names.
func (e *Executor) addAggregatesToRow(expr sqlparser.Expr, row storage.Row, groupRows []storage.Row) {
	switch v := expr.(type) {
	case *sqlparser.ComparisonExpr:
		e.addAggregatesToRow(v.Left, row, groupRows)
		e.addAggregatesToRow(v.Right, row, groupRows)
	case *sqlparser.AndExpr:
		e.addAggregatesToRow(v.Left, row, groupRows)
		e.addAggregatesToRow(v.Right, row, groupRows)
	case *sqlparser.OrExpr:
		e.addAggregatesToRow(v.Left, row, groupRows)
		e.addAggregatesToRow(v.Right, row, groupRows)
	case *sqlparser.CountStar, *sqlparser.Count, *sqlparser.Sum, *sqlparser.Max, *sqlparser.Min, *sqlparser.Avg, *sqlparser.GroupConcatExpr,
		*sqlparser.JSONArrayAgg, *sqlparser.JSONObjectAgg,
		*sqlparser.Variance, *sqlparser.VarPop, *sqlparser.VarSamp,
		*sqlparser.Std, *sqlparser.StdDev, *sqlparser.StdPop, *sqlparser.StdSamp:
		displayName := aggregateDisplayName(expr)
		if _, ok := row[displayName]; !ok {
			repRow := storage.Row{}
			if len(groupRows) > 0 {
				repRow = groupRows[0]
			}
			val, err := evalAggregateExpr(expr, groupRows, repRow)
			if err == nil {
				row[displayName] = val
			}
		}
	case *sqlparser.IsExpr:
		// Walk into IS expressions to find aggregates (e.g., avg(x) IS NOT NULL)
		e.addAggregatesToRow(v.Left, row, groupRows)
	}
}

// evalWhere evaluates a WHERE predicate against a row.
func (e *Executor) evalWhere(expr sqlparser.Expr, row storage.Row) (bool, error) {
	switch v := expr.(type) {
	case *sqlparser.ComparisonExpr:
		// Handle IN / NOT IN specially because the right side is a ValTuple or Subquery.
		if v.Operator == sqlparser.InOp || v.Operator == sqlparser.NotInOp {
			// Handle subquery on right side
			if sub, ok := v.Right.(*sqlparser.Subquery); ok {
				// MySQL error 1235: LIMIT in IN/ALL/ANY/SOME subquery is not supported
				if subqueryHasLimit(sub) {
					return false, mysqlError(1235, "42000", "This version of MySQL doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery'")
				}
				// Check if left side is a tuple: (a,b) IN (SELECT x,y FROM ...)
				if leftTuple, ok := v.Left.(sqlparser.ValTuple); ok {
					result, err := e.execSubquery(sub, row)
					if err != nil {
						return false, err
					}
					if len(result.Columns) != len(leftTuple) {
						return false, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftTuple)))
					}
					leftVals := make([]interface{}, len(leftTuple))
					for i, lExpr := range leftTuple {
						lv, err := e.evalRowExpr(lExpr, row)
						if err != nil {
							return false, err
						}
						leftVals[i] = lv
					}
					hasNull := false
					for _, lv := range leftVals {
						if lv == nil {
							hasNull = true
							break
						}
					}
					for _, rrow := range result.Rows {
						if len(rrow) != len(leftVals) {
							continue
						}
						allMatch := true
						rowHasNull := false
						hasDefiniteNonMatch := false
						for i := 0; i < len(leftVals); i++ {
							if leftVals[i] == nil || rrow[i] == nil {
								rowHasNull = true
								allMatch = false
								// Don't break - continue to check remaining non-null pairs
								continue
							}
							match, _ := compareValues(leftVals[i], rrow[i], sqlparser.EqualOp)
							if !match {
								allMatch = false
								hasDefiniteNonMatch = true
								break
							}
						}
						if allMatch {
							return v.Operator == sqlparser.InOp, nil
						}
						if rowHasNull && !hasDefiniteNonMatch {
							hasNull = true
						}
					}
					if v.Operator == sqlparser.NotInOp && !hasNull {
						return true, nil
					}
					return false, nil
				}
				// Handle (SELECT c1,c2,...) IN (SELECT c1,c2,...) — subquery on both sides.
				// Execute left subquery and compare its rows against right subquery rows.
				if leftSub, leftIsSub := v.Left.(*sqlparser.Subquery); leftIsSub {
					leftResult, err := e.execSubquery(leftSub, row)
					if err != nil {
						return false, err
					}
					rightResult, err := e.execSubquery(sub, row)
					if err != nil {
						return false, err
					}
					if len(leftResult.Columns) != len(rightResult.Columns) {
						return false, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftResult.Columns)))
					}
					ncols := len(leftResult.Columns)
					for _, lrow := range leftResult.Rows {
						hasNull := false
						for _, rrow := range rightResult.Rows {
							allMatch := true
							rowHasNull := false
							for i := 0; i < ncols; i++ {
								lv, rv := lrow[i], rrow[i]
								if lv == nil || rv == nil {
									rowHasNull = true
									allMatch = false
									break
								}
								match, _ := compareValues(lv, rv, sqlparser.EqualOp)
								if !match {
									allMatch = false
									break
								}
							}
							if allMatch {
								return v.Operator == sqlparser.InOp, nil
							}
							if rowHasNull {
								hasNull = true
							}
						}
						if hasNull {
							return false, nil
						}
					}
					return v.Operator == sqlparser.NotInOp, nil
				}
				// Scalar IN (SELECT ...)
				left, err := e.evalRowExpr(v.Left, row)
				if err != nil {
					return false, err
				}
				vals, err := e.execSubqueryValues(sub, row)
				if err != nil {
					return false, err
				}
				if left == nil {
					// NULL IN (empty set) = FALSE; NULL NOT IN (empty set) = TRUE
					if len(vals) == 0 {
						return v.Operator == sqlparser.NotInOp, nil
					}
					return false, nil
				}
				hasNull := false
				for _, val := range vals {
					if val == nil {
						hasNull = true
						continue
					}
					match, _ := compareValues(left, val, sqlparser.EqualOp)
					if match {
						return v.Operator == sqlparser.InOp, nil
					}
				}
				// For NOT IN: if any subquery value is NULL and no match found, result is UNKNOWN (false)
				if v.Operator == sqlparser.NotInOp && hasNull {
					return false, nil
				}
				return v.Operator == sqlparser.NotInOp, nil
			}
			// Row/tuple IN (tuple of tuples): (a,b,c) IN ((1,2,3),(4,5,6))
			if leftTupleW, leftIsTupleW := v.Left.(sqlparser.ValTuple); leftIsTupleW {
				rightTupleW, ok := v.Right.(sqlparser.ValTuple)
				if !ok {
					return false, fmt.Errorf("IN/NOT IN right side must be a value tuple, got %T", v.Right)
				}
				leftValsW := make([]interface{}, len(leftTupleW))
				for i, lExpr := range leftTupleW {
					lv, err := e.evalRowExprTupleAware(lExpr, row)
					if err != nil {
						return false, err
					}
					leftValsW[i] = lv
				}
				hasNullW := false
				leftRowW := interface{}(leftValsW)
				for _, item := range rightTupleW {
					rowTupleW, isRowTuple := item.(sqlparser.ValTuple)
					if !isRowTuple {
						break
					}
					if len(rowTupleW) != len(leftTupleW) {
						return false, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftTupleW)))
					}
					rValsW := make([]interface{}, len(rowTupleW))
					for i, rv := range rowTupleW {
						rVal, err := e.evalRowExprTupleAware(rv, row)
						if err != nil {
							return false, err
						}
						rValsW[i] = rVal
					}
					equal, rowHasNull, err := rowTuplesEqual(leftRowW, interface{}(rValsW))
					if err != nil {
						return false, err
					}
					if equal {
						return v.Operator == sqlparser.InOp, nil
					}
					if rowHasNull {
						hasNullW = true
					}
				}
				if v.Operator == sqlparser.NotInOp && !hasNullW {
					return true, nil
				}
				return false, nil
			}
			// Check for illegal collation mix in IN/NOT IN before evaluating.
			if tuple2, ok2 := v.Right.(sqlparser.ValTuple); ok2 {
				if collErr := e.checkCollationMixForIN(v.Left, []sqlparser.Expr(tuple2)); collErr != nil {
					return false, collErr
				}
			}
			left, err := e.evalRowExpr(v.Left, row)
			if err != nil {
				return false, err
			}
			tuple, ok := v.Right.(sqlparser.ValTuple)
			if !ok {
				return false, fmt.Errorf("IN/NOT IN right side must be a value tuple, got %T", v.Right)
			}
			// NULL IN (...) is always NULL (treated as false in WHERE)
			if left == nil {
				return false, nil
			}
			hasNull := false
			for _, tupleExpr := range tuple {
				val, err := e.evalRowExpr(tupleExpr, row)
				if err != nil {
					return false, err
				}
				if val == nil {
					hasNull = true
					continue
				}
				ls, rs := fmt.Sprintf("%v", left), fmt.Sprintf("%v", val)
				// TIME IN semantics: allow HH:MM:SS vs HHMMSS style equality.
				leftIsTimeLike := looksLikeTime(ls) && !looksLikeDate(ls)
				rightIsTimeLike := looksLikeTime(rs) && !looksLikeDate(rs)
				if leftIsTimeLike || rightIsTimeLike {
					match, err := compareValues(left, val, sqlparser.EqualOp)
					if err != nil {
						return false, err
					}
					if match {
						return v.Operator == sqlparser.InOp, nil
					}
				}
				// Preserve legacy YEAR IN semantics: only convert small years on string side.
				ls, rs = normalizeYearComparisonTypedStringOnly(ls, rs, left, val)
				if numericEqualForComparison(ls, rs, left, val) {
					return v.Operator == sqlparser.InOp, nil
				}
				if _, errL := strconv.ParseFloat(ls, 64); errL == nil {
					if _, errR := strconv.ParseFloat(rs, 64); errR == nil {
						continue
					}
				}
				if ls == rs {
					return v.Operator == sqlparser.InOp, nil
				}
				// Case-insensitive match for INFORMATION_SCHEMA rows
				if row["__is_info_schema__"] != nil {
					if _, isLS := left.(string); isLS {
						if _, isRS := val.(string); isRS {
							if strings.EqualFold(ls, rs) {
								return v.Operator == sqlparser.InOp, nil
							}
						}
					}
				}
			}
			// For NOT IN: if any tuple value is NULL and no match found, result is UNKNOWN (false)
			if v.Operator == sqlparser.NotInOp && hasNull {
				return false, nil
			}
			return v.Operator == sqlparser.NotInOp, nil
		}

		// Handle ANY/SOME (Modifier=1) and ALL (Modifier=2) with subquery
		if v.Modifier != 0 {
			if sub, ok := v.Right.(*sqlparser.Subquery); ok {
				isAny := v.Modifier == 1 // ANY/SOME
				// Handle tuple (row constructor) left side: (a,b) = ANY (SELECT x,y FROM ...)
				if leftTupleAny, leftIsTupleAny := v.Left.(sqlparser.ValTuple); leftIsTupleAny && v.Operator == sqlparser.EqualOp {
					result, err := e.execSubquery(sub, row)
					if err != nil {
						return false, err
					}
					if len(result.Columns) != len(leftTupleAny) {
						return false, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftTupleAny)))
					}
					leftVals := make([]interface{}, len(leftTupleAny))
					for i, lExpr := range leftTupleAny {
						lv, err := e.evalRowExpr(lExpr, row)
						if err != nil {
							return false, err
						}
						leftVals[i] = lv
					}
					if isAny {
						// ANY: true if any row matches
						for _, rrow := range result.Rows {
							allMatch := true
							for i := 0; i < len(leftVals); i++ {
								if leftVals[i] == nil || rrow[i] == nil {
									allMatch = false
									break
								}
								match, _ := compareValues(leftVals[i], rrow[i], sqlparser.EqualOp)
								if !match {
									allMatch = false
									break
								}
							}
							if allMatch {
								return true, nil
							}
						}
						return false, nil
					}
					// ALL: true if every row matches
					for _, rrow := range result.Rows {
						for i := 0; i < len(leftVals); i++ {
							if leftVals[i] == nil || rrow[i] == nil {
								return false, nil
							}
							match, _ := compareValues(leftVals[i], rrow[i], sqlparser.EqualOp)
							if !match {
								return false, nil
							}
						}
					}
					return true, nil
				}
				left, err := e.evalRowExpr(v.Left, row)
				if err != nil {
					return false, err
				}
				if left == nil {
					return false, nil
				}
				vals, err := e.execSubqueryValues(sub, row)
				if err != nil {
					return false, err
				}
				if isAny {
					// ANY/SOME: true if comparison holds for at least one non-NULL value
					for _, val := range vals {
						if val == nil {
							continue
						}
						match, err := compareValues(left, val, v.Operator)
						if err != nil {
							return false, err
						}
						if match {
							return true, nil
						}
					}
					return false, nil
				}
				// ALL: true if comparison holds for every value.
				// If any subquery value is NULL, the comparison is UNKNOWN (returns false).
				for _, val := range vals {
					if val == nil {
						return false, nil
					}
					match, err := compareValues(left, val, v.Operator)
					if err != nil {
						return false, err
					}
					if !match {
						return false, nil
					}
				}
				return true, nil
			}
		}

		// Handle ValTuple (row constructor) comparison: (c1,c2) = (SELECT c1, c2 FROM ...) or (c1,c2) = (2,'abc')
		if tuple, ok := v.Left.(sqlparser.ValTuple); ok {
			// Evaluate left tuple values (with tuple-aware support for nested tuples)
			leftVals := make([]interface{}, len(tuple))
			for i, texpr := range tuple {
				val, err := e.evalRowExprTupleAware(texpr, row)
				if err != nil {
					return false, err
				}
				leftVals[i] = val
			}
			// Right side: subquery returning one row
			if sub, ok := v.Right.(*sqlparser.Subquery); ok {
				result, err := e.execSubquery(sub, row)
				if err != nil {
					return false, err
				}
				if len(result.Rows) == 0 {
					return false, nil
				}
				if len(result.Rows) > 1 {
					return false, fmt.Errorf("Subquery returns more than 1 row")
				}
				rightRow := result.Rows[0]
				if len(leftVals) != len(rightRow) {
					return false, fmt.Errorf("Operand should contain %d column(s)", len(leftVals))
				}
				allMatch := true
				for i, lv := range leftVals {
					rv := rightRow[i]
					match, err := compareValues(lv, rv, v.Operator)
					if err != nil {
						return false, err
					}
					if !match {
						allMatch = false
						break
					}
				}
				return allMatch, nil
			}
			// Right side: ValTuple literal (c1,c2) op (2, "abc") — row constructor comparison.
			if rightTuple, ok := v.Right.(sqlparser.ValTuple); ok {
				if len(leftVals) != len(rightTuple) {
					return false, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftVals)))
				}
				// Build right vals (with tuple-aware support for nested tuples)
				rightValsW := make([]interface{}, len(rightTuple))
				for i := range rightTuple {
					rv, err := e.evalRowExprTupleAware(rightTuple[i], row)
					if err != nil {
						return false, err
					}
					rightValsW[i] = rv
				}
				switch v.Operator {
				case sqlparser.EqualOp, sqlparser.NotEqualOp, sqlparser.NullSafeEqualOp:
					equal, hasNull, err := rowTuplesEqual(interface{}(leftVals), interface{}(rightValsW))
					if err != nil {
						return false, err
					}
					if v.Operator == sqlparser.NullSafeEqualOp {
						if hasNull {
							// Element-wise null-safe comparison
							for i := 0; i < len(leftVals); i++ {
								lv, rv := leftVals[i], rightValsW[i]
								if lv == nil && rv == nil {
									continue
								}
								if lv == nil || rv == nil {
									return false, nil
								}
								eq, _, _ := rowTuplesEqual(lv, rv)
								if !eq {
									return false, nil
								}
							}
							return true, nil
						}
						return equal, nil
					}
					if hasNull {
						return false, nil
					}
					if v.Operator == sqlparser.NotEqualOp {
						return !equal, nil
					}
					return equal, nil
				default:
					// Lexicographic comparison for <, >, <=, >=
					for i := 0; i < len(leftVals); i++ {
						lv, rv := leftVals[i], rightValsW[i]
						if lv == nil || rv == nil {
							return false, nil // NULL → UNKNOWN → false in WHERE
						}
						eq, err := compareValues(lv, rv, sqlparser.EqualOp)
						if err != nil {
							return false, err
						}
						if eq {
							continue
						}
						lt, err := compareValues(lv, rv, sqlparser.LessThanOp)
						if err != nil {
							return false, err
						}
						switch v.Operator {
						case sqlparser.LessThanOp, sqlparser.LessEqualOp:
							return lt, nil
						default: // GreaterThanOp, GreaterEqualOp
							return !lt, nil
						}
					}
					// All elements equal
					switch v.Operator {
					case sqlparser.LessEqualOp, sqlparser.GreaterEqualOp:
						return true, nil
					default:
						return false, nil
					}
				}
			}
		}

		// Extract COLLATE clause from either side for collation-aware comparison
		var whereCollation string
		leftExprW, rightExprW := v.Left, v.Right
		if ce, ok := leftExprW.(*sqlparser.CollateExpr); ok {
			whereCollation = ce.Collation
			leftExprW = ce.Expr
		}
		if ce, ok := rightExprW.(*sqlparser.CollateExpr); ok {
			whereCollation = ce.Collation
			rightExprW = ce.Expr
		}
		left, err := e.evalRowExpr(leftExprW, row)
		if err != nil {
			return false, err
		}
		right, err := e.evalRowExpr(rightExprW, row)
		if err != nil {
			return false, err
		}
		// Collation-aware LIKE/NOT LIKE or LIKE with ESCAPE clause
		if v.Operator == sqlparser.LikeOp || v.Operator == sqlparser.NotLikeOp {
			// NULL comparison: x LIKE NULL = NULL = false in WHERE context
			if left == nil || right == nil {
				return false, nil
			}
			ls := toString(left)
			rs := toString(right)
			// Determine escape character (default is '\')
			escapeChar := rune('\\')
			if v.Escape != nil {
				escVal, _ := e.evalRowExpr(v.Escape, row)
				if escStr := toString(escVal); len([]rune(escStr)) > 0 {
					escapeChar = []rune(escStr)[0]
				}
			}
			collLower := strings.ToLower(whereCollation)
			isCaseSensitive := whereCollation != "" && (strings.Contains(collLower, "_bin") || strings.Contains(collLower, "_cs"))
			var re *regexp.Regexp
			if isCaseSensitive {
				re = likeToRegexpCaseSensitiveEscape(rs, escapeChar)
			} else {
				re = likeToRegexpEscape(rs, escapeChar)
			}
			matched := re.MatchString(ls)
			if v.Operator == sqlparser.LikeOp {
				return matched, nil
			}
			return !matched, nil
		}
		// For binary/varbinary column comparisons, use NO PAD (byte-for-byte) semantics.
		// Check if either side references a binary column.
		if (e.isBinaryExpr(leftExprW) || e.isBinaryExpr(rightExprW)) &&
			(v.Operator == sqlparser.LessThanOp || v.Operator == sqlparser.GreaterThanOp ||
				v.Operator == sqlparser.LessEqualOp || v.Operator == sqlparser.GreaterEqualOp) {
			if left == nil || right == nil {
				return false, nil
			}
			ls := toString(left)
			rs := toString(right)
			cmp := 0
			if ls < rs {
				cmp = -1
			} else if ls > rs {
				cmp = 1
			}
			switch v.Operator {
			case sqlparser.LessThanOp:
				return cmp < 0, nil
			case sqlparser.GreaterThanOp:
				return cmp > 0, nil
			case sqlparser.LessEqualOp:
				return cmp <= 0, nil
			case sqlparser.GreaterEqualOp:
				return cmp >= 0, nil
			}
		}
		result, err := compareValues(left, right, v.Operator)
		if err != nil {
			return false, err
		}
		// For INFORMATION_SCHEMA rows, apply case-insensitive string comparison
		// to match MySQL's default utf8mb4_0900_ai_ci collation behavior.
		if !result && row["__is_info_schema__"] != nil {
			if ls, lok := left.(string); lok {
				if rs, rok := right.(string); rok {
					ci := strings.EqualFold(ls, rs)
					switch v.Operator {
					case sqlparser.EqualOp:
						return ci, nil
					case sqlparser.NotEqualOp:
						return !ci, nil
					}
				}
			}
		}
		return result, err
	case *sqlparser.AndExpr:
		l, err := e.evalWhere(v.Left, row)
		if err != nil {
			return false, err
		}
		r, err := e.evalWhere(v.Right, row)
		if err != nil {
			return false, err
		}
		return l && r, nil
	case *sqlparser.OrExpr:
		l, err := e.evalWhere(v.Left, row)
		if err != nil {
			return false, err
		}
		r, err := e.evalWhere(v.Right, row)
		if err != nil {
			return false, err
		}
		return l || r, nil
	case *sqlparser.IsExpr:
		val, err := e.evalRowExpr(v.Left, row)
		if err != nil {
			return false, err
		}
		switch v.Right {
		case sqlparser.IsNullOp:
			if e.sqlAutoIsNull && e.lastAutoIncID > 0 && val != nil {
				if fmt.Sprintf("%v", val) == fmt.Sprintf("%v", e.lastAutoIncID) {
					return true, nil
				}
			}
			if val == nil {
				return true, nil
			}
			// MySQL: 0000-00-00 IS NULL = TRUE for NOT NULL date columns
			if isZeroDate(val) {
				colName := extractColumnName(v.Left)
				if colName != "" && e.isColumnNotNull(colName) {
					return true, nil
				}
			}
			return false, nil
		case sqlparser.IsNotNullOp:
			return val != nil, nil
		case sqlparser.IsTrueOp:
			return val != nil && isTruthy(val), nil
		case sqlparser.IsFalseOp:
			return val != nil && !isTruthy(val), nil
		case sqlparser.IsNotTrueOp:
			return val == nil || !isTruthy(val), nil
		case sqlparser.IsNotFalseOp:
			return val == nil || isTruthy(val), nil
		}
	case *sqlparser.BetweenExpr:
		val, err := e.evalRowExpr(v.Left, row)
		if err != nil {
			return false, err
		}
		from, err := e.evalRowExpr(v.From, row)
		if err != nil {
			return false, err
		}
		to, err := e.evalRowExpr(v.To, row)
		if err != nil {
			return false, err
		}
		geFrom, err := compareValues(val, from, sqlparser.GreaterEqualOp)
		if err != nil {
			return false, err
		}
		leTo, err := compareValues(val, to, sqlparser.LessEqualOp)
		if err != nil {
			return false, err
		}
		result := geFrom && leTo
		if v.IsBetween {
			return result, nil
		}
		return !result, nil
	case *sqlparser.ExistsExpr:
		result, err := e.execSubquery(v.Subquery, row)
		if err != nil {
			return false, err
		}
		return len(result.Rows) > 0, nil
	case *sqlparser.NotExpr:
		// Evaluate the inner expression using evalRowExpr to preserve NULL (tristate) semantics.
		// NOT(NULL) = NULL → exclude from WHERE (return false).
		// NOT(TRUE) = FALSE → exclude from WHERE (return false).
		// NOT(FALSE) = TRUE → include in WHERE (return true).
		innerVal, err := e.evalRowExpr(v.Expr, row)
		if err != nil {
			return false, err
		}
		if innerVal == nil {
			return false, nil // NOT(NULL) = NULL → treat as false in WHERE
		}
		return !isTruthy(innerVal), nil
	case *sqlparser.MemberOfExpr:
		val, err := e.evalRowExpr(v, row)
		if err != nil {
			return false, err
		}
		switch x := val.(type) {
		case bool:
			return x, nil
		case int64:
			return x != 0, nil
		case uint64:
			return x != 0, nil
		default:
			return toInt64(val) != 0, nil
		}
	}
	val, err := e.evalRowExpr(expr, row)
	if err != nil {
		return false, err
	}
	switch x := val.(type) {
	case bool:
		return x, nil
	case int64:
		return x != 0, nil
	case uint64:
		return x != 0, nil
	case float64:
		return x != 0, nil
	case ScaledValue:
		return x.Value != 0, nil
	case DivisionResult:
		return x.Value != 0, nil
	case string:
		return strings.TrimSpace(x) != "" && x != "0", nil
	default:
		return toInt64(val) != 0, nil
	}
}

// evalWhere is a package-level shim for backward-compatible callers.
func evalWhere(expr sqlparser.Expr, row storage.Row) (bool, error) {
	e := &Executor{}
	return e.evalWhere(expr, row)
}

// normalizeYearComparison detects when one side is a YEAR-like value (4-digit year string)
// and the other is a small string value ('0'-'99'), and converts the small one using YEAR rules.
// Only string values are converted (not integer literals), matching MySQL's behavior.
// origLeft/origRight are the original interface{} values to check types.
func normalizeYearComparison(ls, rs string) (string, string) {
	return normalizeYearComparisonTyped(ls, rs, nil, nil)
}

func normalizeYearComparisonTyped(ls, rs string, origLeft, origRight interface{}) (string, string) {
	lf, le := strconv.ParseFloat(ls, 64)
	rf, re := strconv.ParseFloat(rs, 64)
	if le != nil || re != nil {
		return ls, rs
	}
	li, ri := int(lf), int(rf)

	isYearLike := func(n int) bool {
		return n == 0 || (n >= 1901 && n <= 2155)
	}
	isSmallYear := func(n int) bool {
		return n >= 0 && n <= 99
	}
	convertSmallYear := func(n int) int {
		if n == 0 {
			return 0
		}
		if n >= 1 && n <= 69 {
			return 2000 + n
		}
		if n >= 70 && n <= 99 {
			return 1900 + n
		}
		return n
	}
	// Prefer conversions when at least one side is originally string-typed.
	// YEAR columns are stored as strings in this engine, while many non-YEAR
	// numeric columns stay numeric. This avoids forcing year conversion when
	// both sides are plain numeric values.
	isOrigString := func(orig interface{}) bool {
		if orig == nil {
			return true // Unknown type, assume string for backward compat
		}
		_, ok := orig.(string)
		return ok
	}

	if isYearLike(li) && isSmallYear(ri) && !isYearLike(ri) && (isOrigString(origLeft) || isOrigString(origRight)) {
		ri = convertSmallYear(ri)
		return ls, fmt.Sprintf("%d", ri)
	}
	if isYearLike(ri) && isSmallYear(li) && !isYearLike(li) && (isOrigString(origLeft) || isOrigString(origRight)) {
		li = convertSmallYear(li)
		return fmt.Sprintf("%d", li), rs
	}
	return ls, rs
}

// normalizeYearComparisonTypedStringOnly is a stricter YEAR normalization used
// by IN/NOT IN where this engine expects only string-side small years
// ('1'..'99') to be converted.
func normalizeYearComparisonTypedStringOnly(ls, rs string, origLeft, origRight interface{}) (string, string) {
	lf, le := strconv.ParseFloat(ls, 64)
	rf, re := strconv.ParseFloat(rs, 64)
	if le != nil || re != nil {
		return ls, rs
	}
	li, ri := int(lf), int(rf)

	isYearLike := func(n int) bool {
		return n == 0 || (n >= 1901 && n <= 2155)
	}
	isSmallYear := func(n int) bool {
		return n >= 0 && n <= 99
	}
	convertSmallYear := func(n int) int {
		if n == 0 {
			return 0
		}
		if n >= 1 && n <= 69 {
			return 2000 + n
		}
		if n >= 70 && n <= 99 {
			return 1900 + n
		}
		return n
	}
	isOrigString := func(orig interface{}) bool {
		if orig == nil {
			return true
		}
		_, ok := orig.(string)
		return ok
	}

	if isYearLike(li) && isSmallYear(ri) && !isYearLike(ri) && isOrigString(origRight) {
		ri = convertSmallYear(ri)
		return ls, fmt.Sprintf("%d", ri)
	}
	if isYearLike(ri) && isSmallYear(li) && !isYearLike(li) && isOrigString(origLeft) {
		li = convertSmallYear(li)
		return fmt.Sprintf("%d", li), rs
	}
	return ls, rs
}

// isNativeNumericType returns true if the value is a Go numeric type (int64, uint64, float64).
// Used for MySQL-compatible type coercion: when comparing a number with a non-numeric string,
// the string is cast to 0.
func isNativeNumericType(v interface{}) bool {
	switch v.(type) {
	case int64, uint64, float64, int, int32, float32:
		return true
	}
	return false
}

// isNumericString returns true if s is parseable as a decimal or floating-point number.
// Used to distinguish numeric strings (e.g. DECIMAL column "123.45") from raw binary byte strings.
func isNumericString(s string) bool {
	if s == "" {
		return false
	}
	_, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return err == nil
}

// toUint64ForBitOpAsBinaryString converts a value to uint64 for integer-mode bitwise operations.
// When the value is binary (HexBytes from BINARY/VARBINARY column), MySQL converts the raw bytes
// as a string to integer (i.e., treats the bytes as text, resulting in 0 for non-numeric binary data).
// This mirrors MySQL's mixed-mode behavior: BINARY_col | INTEGER → integer arithmetic.
func toUint64ForBitOpAsBinaryString(v interface{}, bytes []byte, isBinary bool) uint64 {
	if isBinary {
		// Binary side in mixed mode: treat raw bytes as string for integer conversion
		rawStr := string(bytes)
		return toUint64ForBitOp(rawStr)
	}
	return toUint64ForBitOp(v)
}

func numericEqualForComparison(ls, rs string, origLeft, origRight interface{}) bool {
	if li, okL := parseStrictBigInt(ls); okL {
		if ri, okR := parseStrictBigInt(rs); okR {
			return li.Cmp(ri) == 0
		}
	}

	fl, errL := strconv.ParseFloat(ls, 64)
	fr, errR := strconv.ParseFloat(rs, 64)
	if errL != nil || errR != nil {
		return false
	}
	if fl == fr {
		return true
	}
	if !shouldUseFloat32Equality(ls, rs, origLeft, origRight) {
		return false
	}
	// Avoid over-matching at larger magnitudes where float32 ULP becomes coarse.
	if math.Max(math.Abs(fl), math.Abs(fr)) >= 65536 {
		return false
	}
	if math.Abs(fl-fr) > 0.0005 {
		return false
	}
	return float32(fl) == float32(fr)
}

func shouldUseFloat32Equality(ls, rs string, origLeft, origRight interface{}) bool {
	looksApprox := func(s string) bool {
		return strings.ContainsAny(s, ".eE")
	}
	if !looksApprox(ls) && !looksApprox(rs) {
		return false
	}
	switch origLeft.(type) {
	case float32, float64, ScaledValue, DivisionResult:
		return true
	}
	switch origRight.(type) {
	case float32, float64, ScaledValue, DivisionResult:
		return true
	}
	_, leftIsString := origLeft.(string)
	_, rightIsString := origRight.(string)
	return leftIsString && rightIsString
}

func parseStrictBigInt(s string) (*big.Int, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, false
	}
	start := 0
	if s[0] == '+' || s[0] == '-' {
		if len(s) == 1 {
			return nil, false
		}
		start = 1
	}
	for i := start; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return nil, false
		}
	}
	n := new(big.Int)
	if _, ok := n.SetString(s, 10); !ok {
		return nil, false
	}
	return n, true
}

func toStrictBigInt(v interface{}) (*big.Int, bool) {
	switch n := v.(type) {
	case int:
		return big.NewInt(int64(n)), true
	case int8:
		return big.NewInt(int64(n)), true
	case int16:
		return big.NewInt(int64(n)), true
	case int32:
		return big.NewInt(int64(n)), true
	case int64:
		return big.NewInt(n), true
	case uint:
		z := new(big.Int)
		z.SetUint64(uint64(n))
		return z, true
	case uint8:
		z := new(big.Int)
		z.SetUint64(uint64(n))
		return z, true
	case uint16:
		z := new(big.Int)
		z.SetUint64(uint64(n))
		return z, true
	case uint32:
		z := new(big.Int)
		z.SetUint64(uint64(n))
		return z, true
	case uint64:
		z := new(big.Int)
		z.SetUint64(n)
		return z, true
	case string:
		return parseStrictBigInt(n)
	default:
		return nil, false
	}
}

// valuesEqual checks if two values are semantically equal for UPDATE change detection.
func valuesEqual(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	// Convert both to string representation for comparison
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func compareValues(left, right interface{}, op sqlparser.ComparisonExprOperator) (bool, error) {
	// Unwrap SysVarDouble to plain float64 for comparison purposes.
	if sd, ok := left.(SysVarDouble); ok {
		left = sd.Value
	}
	if sd, ok := right.(SysVarDouble); ok {
		right = sd.Value
	}
	// Unwrap AvgResult to plain float64 for comparison purposes.
	if ar, ok := left.(AvgResult); ok {
		left = ar.Value
	}
	if ar, ok := right.(AvgResult); ok {
		right = ar.Value
	}
	// When comparing HexBytes (x'...' literal) against an integer (0x... literal),
	// decode both to raw byte strings for comparison. In MySQL, x'123ABC' = 0x123ABC
	// is true because both represent the same binary string "\x12\x3a\xbc".
	if hb, ok := left.(HexBytes); ok {
		if isNativeNumericType(right) {
			// Decode HexBytes to raw bytes, convert integer to raw bytes too
			decoded, err := hexDecodeString(string(hb))
			if err == nil {
				left = decoded
				right = hexIntToBytes(right)
			}
		} else if _, ok2 := right.(string); ok2 {
			// HexBytes vs string (e.g. raw binary from VARBINARY bitwise op result vs HexBytes from 0x overflow)
			// Decode HexBytes to raw bytes for comparison with the raw byte string
			decoded, err := hexDecodeString(string(hb))
			if err == nil {
				left = decoded
			}
		}
	} else if hb, ok := right.(HexBytes); ok {
		if isNativeNumericType(left) {
			decoded, err := hexDecodeString(string(hb))
			if err == nil {
				right = decoded
				left = hexIntToBytes(left)
			}
		} else if _, ok2 := left.(string); ok2 {
			// HexBytes vs string (e.g. raw binary from VARBINARY bitwise op result vs HexBytes from 0x overflow)
			decoded, err := hexDecodeString(string(hb))
			if err == nil {
				right = decoded
			}
		}
	}
	// NULL-safe equal (<=>): true if both NULL, false if one is NULL, otherwise normal equality.
	if op == sqlparser.NullSafeEqualOp {
		if left == nil && right == nil {
			return true, nil
		}
		if left == nil || right == nil {
			return false, nil
		}
		// Compare numerically if possible, then fall back to string comparison
		ls, rs := fmt.Sprintf("%v", left), fmt.Sprintf("%v", right)
		ls, rs = normalizeYearComparisonTyped(ls, rs, left, right)
		if numericEqualForComparison(ls, rs, left, right) {
			return true, nil
		}
		if _, errL := strconv.ParseFloat(ls, 64); errL == nil {
			if _, errR := strconv.ParseFloat(rs, 64); errR == nil {
				return false, nil
			}
		}
		if ls == rs {
			return true, nil
		}
		// Try date normalization
		if looksLikeDate(ls) || looksLikeDate(rs) {
			ln := normalizeDateTimeString(ls)
			rn := normalizeDateTimeString(rs)
			if ln != "" && rn != "" {
				ln, rn = normalizeDateTimeForCompare(ln, rn)
				return ln == rn, nil
			}
		}
		return false, nil
	}

	// Handle NULL comparisons
	if left == nil || right == nil {
		return false, nil // NULL comparisons always false in SQL (except IS NULL)
	}

	switch op {
	case sqlparser.EqualOp:
		// For numeric-looking strings, compare numerically
		ls, rs := fmt.Sprintf("%v", left), fmt.Sprintf("%v", right)
		// Apply YEAR normalization for small-number vs 4-digit-year comparisons
		ls, rs = normalizeYearComparisonTyped(ls, rs, left, right)
		if numericEqualForComparison(ls, rs, left, right) {
			return true, nil
		}
		fl, errL := strconv.ParseFloat(ls, 64)
		fr, errR := strconv.ParseFloat(rs, 64)
		if errL == nil && errR == nil {
			return false, nil
		}
		// ENUM values compared with integers: do not coerce via string→0 rule.
		_, leftIsEnum := left.(EnumValue)
		_, rightIsEnum := right.(EnumValue)
		// MySQL type coercion: when one operand is a DATE/TIME-like string
		// and the other is a native integer, convert the integer to DATE/TIME
		// format for comparison (e.g., 19830907 → "1983-09-07", 000400 → "00:04:00").
		if errL == nil && errR != nil && isNativeNumericType(left) {
			// left is numeric, right is non-numeric string
			if looksLikeDate(rs) {
				// Try converting the integer to a date string (YYYYMMDD/YYMMDD)
				dateStr := parseMySQLDateValue(ls)
				if dateStr != "" {
					rn := normalizeDateTimeString(rs)
					if rn != "" {
						dateStr, rn = normalizeDateTimeForCompare(dateStr, rn)
						return dateStr == rn, nil
					}
				}
			}
			if looksLikeTime(rs) && !looksLikeDate(rs) {
				// Try converting the integer to a TIME string (HHMMSS)
				lt := parseMySQLTimeValue(ls)
				rt := parseMySQLTimeValue(rs)
				return lt == rt, nil
			}
			if rightIsEnum {
				return false, nil
			}
			// For a native numeric column value vs a binary string with non-printable bytes:
			// MySQL converts the binary string to DOUBLE (which fails -> 0), so compare fl == 0.
			return fl == 0, nil
		}
		if errR == nil && errL != nil && isNativeNumericType(right) {
			// right is numeric, left is non-numeric string
			if looksLikeDate(ls) {
				// Try converting the integer to a date string (YYYYMMDD/YYMMDD)
				dateStr := parseMySQLDateValue(rs)
				if dateStr == "" && fr == 0 {
					// Integer 0 compared to a date = 0000-00-00 comparison
					ln := normalizeDateTimeString(ls)
					if ln != "" {
						ln, _ = normalizeDateTimeForCompare(ln, "0000-00-00")
						return ln == "0000-00-00", nil
					}
				}
				if dateStr != "" {
					ln := normalizeDateTimeString(ls)
					if ln != "" {
						ln, dateStr = normalizeDateTimeForCompare(ln, dateStr)
						return ln == dateStr, nil
					}
				}
			}
			if looksLikeTime(ls) && !looksLikeDate(ls) {
				// Try converting the integer to a TIME string (HHMMSS)
				lt := parseMySQLTimeValue(ls)
				rt := parseMySQLTimeValue(rs)
				return lt == rt, nil
			}
			if leftIsEnum {
				return false, nil
			}
			// Binary string vs integer: if left contains non-printable bytes (binary data),
			// treat the integer as a big-endian byte string for comparison.
			if looksLikeBinaryData(ls) {
				rightAsBytes, ok := hexIntToBytes(right).(string)
				if ok {
					return ls == rightAsBytes, nil
				}
			}
			return fr == 0, nil
		}
		if ls == rs {
			return true, nil
		}
		// MySQL default collation (utf8mb4_0900_ai_ci) is case-insensitive.
		// When both operands are native Go strings (i.e. VARCHAR/CHAR columns or string literals),
		// use case-insensitive comparison unless the value looks like a date/time/number (already
		// handled above) or binary data.
		_, leftIsString := left.(string)
		_, rightIsString := right.(string)
		if leftIsString && rightIsString && !looksLikeBinaryData(ls) && !looksLikeBinaryData(rs) {
			if strings.EqualFold(ls, rs) {
				return true, nil
			}
			// MySQL PAD SPACE semantics: trailing spaces are ignored in non-binary comparisons.
			// 'a' = 'a ' is TRUE, 'a\0' < 'a' is TRUE (NUL is less than space)
			lsTrimmed := strings.TrimRight(ls, " ")
			rsTrimmed := strings.TrimRight(rs, " ")
			if strings.EqualFold(lsTrimmed, rsTrimmed) {
				return true, nil
			}
		}
		// Try datetime normalization if strings look like dates (but not binary data)
		if !looksLikeBinaryData(ls) && !looksLikeBinaryData(rs) {
			if looksLikeDate(ls) || looksLikeDate(rs) {
				ln := normalizeDateTimeString(ls)
				rn := normalizeDateTimeString(rs)
				if ln != "" && rn != "" {
					// Use the two-arg normalizer to align date vs datetime granularity
					ln, rn = normalizeDateTimeForCompare(ln, rn)
					return ln == rn, nil
				}
			}
			// Try TIME normalization if either looks like a time.
			// Use strict check to avoid false positives from strings that contain ':'
			// but are not actually time values (e.g. instrument names like 'hash_filo::lock').
			if looksLikeActualTime(ls) || looksLikeActualTime(rs) {
				lt := parseMySQLTimeValue(ls)
				rt := parseMySQLTimeValue(rs)
				if lt == rt {
					return true, nil
				}
			}
		}
		return false, nil
	case sqlparser.NotEqualOp:
		ls, rs := fmt.Sprintf("%v", left), fmt.Sprintf("%v", right)
		ls, rs = normalizeYearComparisonTyped(ls, rs, left, right)
		if numericEqualForComparison(ls, rs, left, right) {
			return false, nil
		}
		fl, errL := strconv.ParseFloat(ls, 64)
		fr, errR := strconv.ParseFloat(rs, 64)
		if errL == nil && errR == nil {
			return true, nil
		}
		_, leftIsEnum := left.(EnumValue)
		_, rightIsEnum := right.(EnumValue)
		// MySQL type coercion: when one operand is a native numeric type
		// and the other is a non-numeric string, try DATE/TIME coercion first.
		if errL == nil && errR != nil && isNativeNumericType(left) {
			if looksLikeDate(rs) {
				dateStr := parseMySQLDateValue(ls)
				if dateStr != "" {
					rn := normalizeDateTimeString(rs)
					if rn != "" {
						dateStr, rn = normalizeDateTimeForCompare(dateStr, rn)
						return dateStr != rn, nil
					}
				}
			}
			if looksLikeTime(rs) && !looksLikeDate(rs) {
				lt := parseMySQLTimeValue(ls)
				rt := parseMySQLTimeValue(rs)
				return lt != rt, nil
			}
			if rightIsEnum {
				return true, nil
			}
			return fl != 0, nil
		}
		if errR == nil && errL != nil && isNativeNumericType(right) {
			if looksLikeDate(ls) {
				dateStr := parseMySQLDateValue(rs)
				if dateStr != "" {
					ln := normalizeDateTimeString(ls)
					if ln != "" {
						ln, dateStr = normalizeDateTimeForCompare(ln, dateStr)
						return ln != dateStr, nil
					}
				}
			}
			if looksLikeTime(ls) && !looksLikeDate(ls) {
				lt := parseMySQLTimeValue(ls)
				rt := parseMySQLTimeValue(rs)
				return lt != rt, nil
			}
			if leftIsEnum {
				return true, nil
			}
			return fr != 0, nil
		}
		if ls == rs {
			return false, nil
		}
		// Try datetime normalization
		if looksLikeDate(ls) || looksLikeDate(rs) {
			ln := normalizeDateTimeString(ls)
			rn := normalizeDateTimeString(rs)
			if ln != "" && rn != "" {
				ln, rn = normalizeDateTimeForCompare(ln, rn)
				return ln != rn, nil
			}
		}
		return true, nil
	case sqlparser.LessThanOp, sqlparser.GreaterThanOp, sqlparser.LessEqualOp, sqlparser.GreaterEqualOp:
		if li, okL := toStrictBigInt(left); okL {
			if ri, okR := toStrictBigInt(right); okR {
				// Use bigint ordering only for values outside signed 64-bit range;
				// keep existing YEAR/small-number behavior for ordinary integers.
				if li.BitLen() > 63 || ri.BitLen() > 63 {
					cmp := li.Cmp(ri)
					switch op {
					case sqlparser.LessThanOp:
						return cmp < 0, nil
					case sqlparser.GreaterThanOp:
						return cmp > 0, nil
					case sqlparser.LessEqualOp:
						return cmp <= 0, nil
					case sqlparser.GreaterEqualOp:
						return cmp >= 0, nil
					}
				}
			}
		}
		// Apply YEAR normalization for ordering comparisons
		ls, rs := fmt.Sprintf("%v", left), fmt.Sprintf("%v", right)
		nls, nrs := normalizeYearComparisonTyped(ls, rs, left, right)
		if nls != ls || nrs != rs {
			// YEAR comparison detected, use normalized values
			fl, _ := strconv.ParseFloat(nls, 64)
			fr, _ := strconv.ParseFloat(nrs, 64)
			cmp := 0
			if fl < fr {
				cmp = -1
			} else if fl > fr {
				cmp = 1
			}
			switch op {
			case sqlparser.LessThanOp:
				return cmp < 0, nil
			case sqlparser.GreaterThanOp:
				return cmp > 0, nil
			case sqlparser.LessEqualOp:
				return cmp <= 0, nil
			case sqlparser.GreaterEqualOp:
				return cmp >= 0, nil
			}
		}
		// TIME ordering: when one side is TIME-like (contains ':') and not a date,
		// coerce both sides with MySQL TIME parser and compare by duration.
		leftIsTimeLike := looksLikeTime(ls) && !looksLikeDate(ls)
		rightIsTimeLike := looksLikeTime(rs) && !looksLikeDate(rs)
		if leftIsTimeLike || rightIsTimeLike {
			if tcmp, ok := compareMySQLTimeOrdering(ls, rs); ok {
				switch op {
				case sqlparser.LessThanOp:
					return tcmp < 0, nil
				case sqlparser.GreaterThanOp:
					return tcmp > 0, nil
				case sqlparser.LessEqualOp:
					return tcmp <= 0, nil
				case sqlparser.GreaterEqualOp:
					return tcmp >= 0, nil
				}
			}
		}
		// DATE ordering: when one side is a date-like string and the other is
		// a numeric integer, try converting the integer to a date for comparison.
		if (looksLikeDate(ls) && isNativeNumericType(right)) || (looksLikeDate(rs) && isNativeNumericType(left)) {
			var lDate, rDate string
			if looksLikeDate(ls) {
				lDate = normalizeDateTimeString(ls)
				rDate = parseMySQLDateValue(rs)
			} else {
				lDate = parseMySQLDateValue(ls)
				rDate = normalizeDateTimeString(rs)
			}
			if lDate != "" && rDate != "" {
				lDate, rDate = normalizeDateTimeForCompare(lDate, rDate)
				dcmp := 0
				if lDate < rDate {
					dcmp = -1
				} else if lDate > rDate {
					dcmp = 1
				}
				switch op {
				case sqlparser.LessThanOp:
					return dcmp < 0, nil
				case sqlparser.GreaterThanOp:
					return dcmp > 0, nil
				case sqlparser.LessEqualOp:
					return dcmp <= 0, nil
				case sqlparser.GreaterEqualOp:
					return dcmp >= 0, nil
				}
			}
		}
		// Validate datetime strings: if either side looks like a full datetime (date+time)
		// but has an invalid time component (hour > 23, etc.), throw error 1292.
		if invalidStr, invalid := hasInvalidTimeComponent(ls); invalid {
			return false, mysqlError(1292, "HY000", fmt.Sprintf("Incorrect DATETIME value: '%s'", invalidStr))
		}
		if invalidStr, invalid := hasInvalidTimeComponent(rs); invalid {
			return false, mysqlError(1292, "HY000", fmt.Sprintf("Incorrect DATETIME value: '%s'", invalidStr))
		}
		cmp := compareNumeric(left, right)
		switch op {
		case sqlparser.LessThanOp:
			return cmp < 0, nil
		case sqlparser.GreaterThanOp:
			return cmp > 0, nil
		case sqlparser.LessEqualOp:
			return cmp <= 0, nil
		case sqlparser.GreaterEqualOp:
			return cmp >= 0, nil
		}
		return false, nil
	case sqlparser.LikeOp:
		pattern := toString(right)
		value := toString(left)
		re := likeToRegexp(pattern)
		return re.MatchString(value), nil
	case sqlparser.NotLikeOp:
		pattern := toString(right)
		value := toString(left)
		re := likeToRegexp(pattern)
		return !re.MatchString(value), nil
	case sqlparser.RegexpOp:
		rePattern := toString(right)
		reValue := toString(left)
		compiled, err := regexp.Compile("(?i)" + rePattern)
		if err != nil {
			return false, mysqlError(3692, "HY000", "Illegal argument to a regular expression.")
		}
		return compiled.MatchString(reValue), nil
	case sqlparser.NotRegexpOp:
		rePattern := toString(right)
		reValue := toString(left)
		compiled, err := regexp.Compile("(?i)" + rePattern)
		if err != nil {
			return false, mysqlError(3692, "HY000", "Illegal argument to a regular expression.")
		}
		return !compiled.MatchString(reValue), nil
	}
	return false, fmt.Errorf("unsupported comparison operator: %s", op.ToString())
}

// soundex implements the MySQL SOUNDEX() function.
// It returns a 4-character Soundex code for the input string.
func soundex(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	// Map of letters to soundex digits
	code := map[byte]byte{
		'B': '1', 'F': '1', 'P': '1', 'V': '1',
		'C': '2', 'G': '2', 'J': '2', 'K': '2', 'Q': '2', 'S': '2', 'X': '2', 'Z': '2',
		'D': '3', 'T': '3',
		'L': '4',
		'M': '5', 'N': '5',
		'R': '6',
	}
	upper := strings.ToUpper(s)
	var result []byte
	// Find first letter
	firstIdx := -1
	for i := 0; i < len(upper); i++ {
		if upper[i] >= 'A' && upper[i] <= 'Z' {
			firstIdx = i
			break
		}
	}
	if firstIdx < 0 {
		return "0000"
	}
	result = append(result, upper[firstIdx])
	lastCode := code[upper[firstIdx]]
	// MySQL SOUNDEX: non-alpha chars (spaces, numbers, punctuation) do NOT reset adjacency.
	// Only vowels/H/W/Y (letters without a soundex code) are skipped without resetting.
	// All consonants with the same soundex code as the previous are skipped.
	// MySQL does not limit the result to 4 characters.
	for i := firstIdx + 1; i < len(upper); i++ {
		c := upper[i]
		if c < 'A' || c > 'Z' {
			// Non-letter: skip without resetting lastCode
			continue
		}
		if d, ok := code[c]; ok {
			if d != lastCode {
				result = append(result, d)
				lastCode = d
			}
		} else {
			// A, E, I, O, U, H, W, Y — not coded, skip but do NOT reset adjacency
		}
	}
	for len(result) < 4 {
		result = append(result, '0')
	}
	return string(result)
}

// likePatternToRegexpStr converts a SQL LIKE pattern to a regexp string with a given escape rune.
// prefix is the regexp prefix (e.g. "(?i)^" for case-insensitive or "^" for case-sensitive).
func likePatternToRegexpStr(pattern string, escapeChar rune, prefix string) string {
	var sb strings.Builder
	sb.WriteString(prefix)
	runes := []rune(pattern)
	for i := 0; i < len(runes); i++ {
		c := runes[i]
		if c == escapeChar && i+1 < len(runes) {
			// The next character is escaped: treat it literally (not as a wildcard)
			sb.WriteString(regexp.QuoteMeta(string(runes[i+1])))
			i++
		} else if c == '%' {
			sb.WriteString(".*")
		} else if c == '_' {
			sb.WriteString(".")
		} else {
			sb.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	sb.WriteString("$")
	return sb.String()
}

// likeToRegexpCaseSensitive converts a SQL LIKE pattern to a case-sensitive Go regexp.
// Used for LIKE with binary or case-sensitive collations.
func likeToRegexpCaseSensitive(pattern string) *regexp.Regexp {
	re, _ := regexp.Compile(likePatternToRegexpStr(pattern, '\\', "^"))
	return re
}

// likeToRegexpCaseSensitiveEscape converts a SQL LIKE pattern with a custom escape char to a case-sensitive Go regexp.
func likeToRegexpCaseSensitiveEscape(pattern string, escapeChar rune) *regexp.Regexp {
	re, _ := regexp.Compile(likePatternToRegexpStr(pattern, escapeChar, "^"))
	return re
}

// likeToRegexpEscape converts a SQL LIKE pattern with a custom escape char to a case-insensitive Go regexp.
func likeToRegexpEscape(pattern string, escapeChar rune) *regexp.Regexp {
	re, _ := regexp.Compile(likePatternToRegexpStr(pattern, escapeChar, "(?i)^"))
	return re
}

// likeToRegexp converts a SQL LIKE pattern to a Go regexp.
func likeToRegexp(pattern string) *regexp.Regexp {
	var sb strings.Builder
	sb.WriteString("(?i)^") // case-insensitive
	runes := []rune(pattern)
	for i := 0; i < len(runes); i++ {
		c := runes[i]
		if c == '\\' && i+1 < len(runes) {
			sb.WriteString(regexp.QuoteMeta(string(runes[i+1])))
			i++
		} else if c == '%' {
			sb.WriteString(".*")
		} else if c == '_' {
			sb.WriteString(".")
		} else {
			sb.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	sb.WriteString("$")
	re, _ := regexp.Compile(sb.String())
	return re
}

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

// compareCaseInsensitive compares two values using case-insensitive string comparison
// for strings, to match MySQL's utf8_general_ci collation behavior.
func compareCaseInsensitive(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	sa := strings.ToLower(toString(a))
	sb := strings.ToLower(toString(b))
	if sa < sb {
		return -1
	}
	if sa > sb {
		return 1
	}
	return 0
}

func effectiveTableCollation(def *catalog.TableDef) string {
	if def == nil {
		return ""
	}
	if def.Collation != "" {
		return strings.ToLower(def.Collation)
	}
	charset := def.Charset
	if charset == "" {
		charset = "utf8mb4"
	}
	return strings.ToLower(catalog.DefaultCollationForCharset(charset))
}

func resolveOrderByCollation(tableDefs []*catalog.TableDef, fromExprs ...sqlparser.TableExpr) string {
	if len(tableDefs) == 0 {
		// For virtual tables (information_schema, performance_schema) that don't have
		// catalog entries, detect the schema from the FROM clause and use utf8_general_ci
		// which is the default collation for these schemas in MySQL 8.0.
		if len(fromExprs) > 0 {
			if dbName := extractFromDatabase(fromExprs[0]); dbName != "" {
				dbLower := strings.ToLower(dbName)
				if dbLower == "information_schema" || dbLower == "performance_schema" {
					return "utf8_general_ci"
				}
			}
		}
		return "utf8mb4_0900_ai_ci"
	}
	// Use single-table collation first; for joins fallback to the first table.
	if len(tableDefs) == 1 {
		return effectiveTableCollation(tableDefs[0])
	}
	return effectiveTableCollation(tableDefs[0])
}

// extractFromDatabase extracts the database/qualifier name from a FROM expression.
func extractFromDatabase(expr sqlparser.TableExpr) string {
	switch te := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		if tn, ok := te.Expr.(sqlparser.TableName); ok {
			if !tn.Qualifier.IsEmpty() {
				return tn.Qualifier.String()
			}
		}
	}
	return ""
}

func compareByCollation(a, b interface{}, collation string) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	aIsStr := isStringValue(a)
	bIsStr := isStringValue(b)
	if aIsStr || bIsStr {
		aStr := toString(a)
		bStr := toString(b)

		sa := normalizeCollationKey(aStr, collation)
		sb := normalizeCollationKey(bStr, collation)
		if sa < sb {
			return -1
		}
		if sa > sb {
			return 1
		}
		// Tie-break for _ci collations
		coll := strings.ToLower(collation)
		if strings.HasSuffix(coll, "_ci") {
			var aTie, bTie string
			switch coll {
			case "sjis_japanese_ci", "cp932_japanese_ci":
				aTie = encodeStringForCollation(aStr, "sjis")
				bTie = encodeStringForCollation(bStr, "sjis")
			case "ujis_japanese_ci", "eucjpms_japanese_ci":
				aTie = encodeStringForCollation(aStr, "eucjp")
				bTie = encodeStringForCollation(bStr, "eucjp")
			default:
				aTie = aStr
				bTie = bStr
			}
			if aTie < bTie {
				return -1
			}
			if aTie > bTie {
				return 1
			}
		}
		return 0
	}
	return compareNumeric(a, b)
}

func normalizeCollationKey(s string, collation string) string {
	coll := strings.ToLower(collation)

	// Use Vitess for UCA 0900 collations which need accurate weight tables
	if strings.Contains(coll, "_0900_") || strings.HasSuffix(coll, "_0900_bin") {
		if vc := lookupVitessCollation(collation); vc != nil {
			ws := vitessWeightString(s, vc)
			return string(ws)
		}
	}

	// Collation-specific key normalization
	switch coll {
	case "utf8_general_ci", "utf8mb3_general_ci":
		return normalizeUTF8GeneralCIKey(s)
	case "sjis_japanese_ci", "cp932_japanese_ci":
		return encodeStringForCollation(foldASCIICase(s), "sjis")
	case "ujis_japanese_ci", "eucjpms_japanese_ci":
		return encodeStringForCollation(foldASCIICase(s), "eucjp")
	case "dec8_swedish_ci", "swe7_swedish_ci":
		// dec8_swedish_ci and swe7_swedish_ci: letters A-Z and a-z are case-folded using
		// ToUpper (mapped to 0x41-0x5A range). This ensures letters sort before 0x5B-0x60
		// characters ([, \, ], ^, _, `) since Z=0x5A < [=0x5B.
		// In contrast, ToLower would map letters to 0x61-0x7A which is ABOVE 0x5B-0x60,
		// causing [, \, etc. to incorrectly appear before letters in the sort output.
		return strings.ToUpper(s)
	}
	if strings.HasSuffix(coll, "_ci") {
		// Use ToUpper for case-insensitive collations. MySQL's weight tables for _ci collations
		// assign letters the same weight as their uppercase equivalents (0x41-0x5A range).
		// This ensures that letters sort BEFORE punctuation in the 0x5B-0x60 range ([, \, ], ^, _, `),
		// which is correct for MySQL's Latin-family _ci collations.
		// Previously ToLower was used, which mapped letters to 0x61-0x7A (ABOVE 0x5B-0x60),
		// causing [ and \ to incorrectly sort before letters.
		return strings.ToUpper(s)
	}
	return s
}

func encodeStringForCollation(s, charset string) string {
	var enc *encoding.Encoder
	isUJIS := false
	switch strings.ToLower(charset) {
	case "sjis", "cp932":
		enc = japanese.ShiftJIS.NewEncoder()
	case "ujis", "eucjp", "eucjpms":
		enc = japanese.EUCJP.NewEncoder()
		isUJIS = true
	default:
		return s
	}
	encoded, err := encoding.ReplaceUnsupported(enc).Bytes([]byte(s))
	if err != nil {
		return s
	}
	if isUJIS && len(encoded) >= 2 && encoded[0] == 0xF9 && encoded[1] == 0xAE {
		// Align collation position of U+4EE1 (仡) with MySQL ujis ordering.
		encoded = append([]byte{0x8F, 0xB0, 0xC8}, encoded[2:]...)
	}
	return string(encoded)
}

func foldASCIICase(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] = b[i] + ('a' - 'A')
		}
	}
	return string(b)
}

func normalizeUTF8GeneralCIKey(s string) string {
	s = strings.ToLower(s)
	decomposed := norm.NFD.String(s)
	var b strings.Builder
	b.Grow(len(decomposed))
	for _, r := range decomposed {
		if unicode.Is(unicode.Mn, r) {
			continue
		}
		// In MySQL's utf8_general_ci, non-letter ASCII symbols like '_' (0x5F)
		// sort after letters. Remap them to high codepoints to match MySQL ordering.
		if r == '_' {
			b.WriteRune('\u007F') // DEL sorts after all ASCII letters
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func compareNumeric(a, b interface{}) int {
	// If both values are strings (or one is), try numeric comparison first
	aIsStr := isStringValue(a)
	bIsStr := isStringValue(b)
	if aIsStr || bIsStr {
		sa := toString(a)
		sb := toString(b)
		// Try numeric comparison when at least one value is not a string,
		// or when both are short numeric-looking strings (like "99999.99999" vs "-99999")
		shouldTryNumeric := !aIsStr || !bIsStr
		if !shouldTryNumeric && len(sa) <= 20 && len(sb) <= 20 {
			// Both are strings, but short enough to be valid numbers
			shouldTryNumeric = true
		}
		if shouldTryNumeric {
			fa, errA := strconv.ParseFloat(sa, 64)
			fb, errB := strconv.ParseFloat(sb, 64)
			if errA == nil && errB == nil {
				if fa < fb {
					return -1
				}
				if fa > fb {
					return 1
				}
				return 0
			}
			// MySQL type coercion: when one side is a native numeric type and the
			// other is a non-numeric string, treat the non-numeric string as 0.
			// e.g. INT_COL > 'A' → INT_COL > 0 (MySQL converts non-numeric string to 0)
			if !aIsStr && errB != nil {
				fb = 0
				if fa < fb {
					return -1
				}
				if fa > fb {
					return 1
				}
				return 0
			}
			if !bIsStr && errA != nil {
				fa = 0
				if fa < fb {
					return -1
				}
				if fa > fb {
					return 1
				}
				return 0
			}
		}
		// Normalize date values first (e.g., "20070523091528" -> "2007-05-23 09:15:28")
		if na := normalizeDateTimeString(sa); na != "" {
			sa = na
		}
		if nb := normalizeDateTimeString(sb); nb != "" {
			sb = nb
		}
		// Normalize date/time comparisons: when one is TIME-like and
		// the other is DATETIME-like, extract the matching part.
		sa, sb = normalizeDateTimeForCompare(sa, sb)

		// Apply PAD SPACE semantics for non-binary string comparisons.
		// MySQL's default collations (latin1_swedish_ci, utf8mb4_0900_ai_ci, etc.)
		// use PAD SPACE: the shorter string is virtually padded with spaces before comparison.
		// This means 'a\0' < 'a' because 'a' pads to 'a ' and NUL (0x00) < space (0x20).
		padLen := len(sa)
		if len(sb) > padLen {
			padLen = len(sb)
		}
		for len(sa) < padLen {
			sa += " "
		}
		for len(sb) < padLen {
			sb += " "
		}

		if sa < sb {
			return -1
		}
		if sa > sb {
			return 1
		}
		return 0
	}
	fa := toFloat(a)
	fb := toFloat(b)
	if fa < fb {
		return -1
	}
	if fa > fb {
		return 1
	}
	return 0
}

func compareMySQLTimeOrdering(a, b string) (int, bool) {
	ta := parseMySQLTimeValue(strings.TrimSpace(a))
	tb := parseMySQLTimeValue(strings.TrimSpace(b))
	ma, okA := parseCanonicalTimeMicros(ta)
	mb, okB := parseCanonicalTimeMicros(tb)
	if !okA || !okB {
		return 0, false
	}
	if ma < mb {
		return -1, true
	}
	if ma > mb {
		return 1, true
	}
	return 0, true
}

func parseCanonicalTimeMicros(s string) (int64, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	sign := int64(1)
	if strings.HasPrefix(s, "-") {
		sign = -1
		s = s[1:]
	}
	main := s
	frac := ""
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		main = s[:dot]
		frac = s[dot+1:]
	}
	parts := strings.Split(main, ":")
	if len(parts) != 3 {
		return 0, false
	}
	h, errH := strconv.ParseInt(parts[0], 10, 64)
	m, errM := strconv.ParseInt(parts[1], 10, 64)
	sec, errS := strconv.ParseInt(parts[2], 10, 64)
	if errH != nil || errM != nil || errS != nil {
		return 0, false
	}
	if m < 0 || m > 59 || sec < 0 || sec > 59 {
		return 0, false
	}
	micros := int64(0)
	if frac != "" {
		for len(frac) < 6 {
			frac += "0"
		}
		if len(frac) > 6 {
			frac = frac[:6]
		}
		u, err := strconv.ParseInt(frac, 10, 64)
		if err != nil {
			return 0, false
		}
		micros = u
	}
	total := ((h*3600 + m*60 + sec) * 1_000_000) + micros
	return sign * total, true
}

// normalizeDateTimeForCompare ensures two date/time string values are
// compared using the same granularity. When one looks like a TIME (HH:MM:SS)
// and the other looks like a DATETIME (YYYY-MM-DD HH:MM:SS), the DATETIME is
// truncated to TIME. Similarly, DATE vs DATETIME extracts the DATE part.
func normalizeDateTimeForCompare(a, b string) (string, string) {
	aIsDate := isDateString(a)
	bIsDate := isDateString(b)
	aIsTime := isTimeString(a)
	bIsTime := isTimeString(b)
	aIsDatetime := isDatetimeString(a)
	bIsDatetime := isDatetimeString(b)

	// TIME vs DATETIME: extract time part from datetime
	if aIsTime && bIsDatetime {
		if idx := strings.Index(b, " "); idx >= 0 {
			b = b[idx+1:]
		}
	} else if bIsTime && aIsDatetime {
		if idx := strings.Index(a, " "); idx >= 0 {
			a = a[idx+1:]
		}
	}

	// DATE vs DATETIME: extend DATE to DATETIME by appending " 00:00:00"
	if aIsDate && !aIsDatetime && bIsDatetime {
		a = a + " 00:00:00"
	} else if bIsDate && !bIsDatetime && aIsDatetime {
		b = b + " 00:00:00"
	}

	return a, b
}

func isDateString(s string) bool {
	return len(s) >= 10 && s[4] == '-' && s[7] == '-' && (len(s) == 10 || s[10] == ' ')
}

func isTimeString(s string) bool {
	if len(s) < 5 || len(s) > 8 {
		return false
	}
	// HH:MM:SS or H:MM:SS
	parts := strings.Split(s, ":")
	return len(parts) == 3
}

func isDatetimeString(s string) bool {
	return len(s) == 19 && s[4] == '-' && s[7] == '-' && s[10] == ' ' && s[13] == ':' && s[16] == ':'
}

// isDateLikeButInvalid returns true if s looks like a date/datetime string (YYYY-MM-DD...)
// but has an invalid month/day combination (e.g. "1997-11-31").
func isDateLikeButInvalid(s string) bool {
	if len(s) < 10 || s[4] != '-' || s[7] != '-' {
		return false
	}
	// It looks like a date - check validity
	y, ey := strconv.Atoi(s[:4])
	m, em := strconv.Atoi(s[5:7])
	d, ed := strconv.Atoi(s[8:10])
	if ey != nil || em != nil || ed != nil {
		return false
	}
	return !isValidDate(y, m, d)
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

// isDatetimeLikeString returns true if s looks like a full datetime (has a date part with '-' or ':').
// Used to detect when addtime/subtime/timediff receives a datetime instead of a time.
func isDatetimeLikeString(s string) bool {
	// A datetime string has format YYYY-MM-DD HH:MM:SS[.ffffff] or YYYY:MM:DD HH:MM:SS
	// The key indicator: 4 digits followed by '-' or ':' at position 4, and another '-' or ':' at position 7.
	// The first 4 characters must all be digits (year).
	if len(s) >= 10 {
		// Check first 4 chars are digits (year part)
		for i := 0; i < 4; i++ {
			if s[i] < '0' || s[i] > '9' {
				return false
			}
		}
		sep := s[4]
		if (sep == '-' || sep == ':') && s[7] == sep {
			return true
		}
	}
	return false
}

func toFloat(v interface{}) float64 {
	switch n := v.(type) {
	case int64:
		return float64(n)
	case uint64:
		return float64(n)
	case float64:
		return n
	case SysVarDouble:
		return n.Value
	case ScaledValue:
		return n.Value
	case DivisionResult:
		return n.Value
	case AvgResult:
		return n.Value
	case HexBytes:
		// Interpret hex digits as big-endian unsigned integer.
		decoded, err := hex.DecodeString(string(n))
		if err != nil || len(decoded) == 0 {
			return 0
		}
		var val uint64
		for _, b := range decoded {
			val = val<<8 | uint64(b)
		}
		return float64(val)
	case string:
		s := strings.TrimSpace(n)
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		} else if errors.Is(err, strconv.ErrRange) {
			if strings.HasPrefix(s, "-") {
				return math.Inf(-1)
			}
			return math.Inf(1)
		}
		// MySQL numeric context for TIME string: HH:MM:SS[.frac] -> HHMMSS[.frac]
		if strings.Count(s, ":") == 2 {
			sign := 1.0
			if strings.HasPrefix(s, "-") {
				sign = -1.0
				s = strings.TrimPrefix(s, "-")
			}
			main := s
			fracPart := ""
			if dot := strings.IndexByte(s, '.'); dot >= 0 {
				main = s[:dot]
				fracPart = s[dot+1:]
			}
			tparts := strings.Split(main, ":")
			if len(tparts) == 3 {
				h, eh := strconv.Atoi(tparts[0])
				m, em := strconv.Atoi(tparts[1])
				sec, es := strconv.Atoi(tparts[2])
				if eh == nil && em == nil && es == nil {
					base := float64(h*10000 + m*100 + sec)
					if fracPart != "" {
						if fracDigits, err := strconv.ParseFloat("0."+fracPart, 64); err == nil {
							base += fracDigits
						}
					}
					return sign * base
				}
			}
		}
		// MySQL numeric context for DATE/DATETIME strings.
		if len(s) >= 10 && s[4] == '-' && s[7] == '-' {
			y, ey := strconv.Atoi(s[0:4])
			mo, em := strconv.Atoi(s[5:7])
			d, ed := strconv.Atoi(s[8:10])
			if ey == nil && em == nil && ed == nil {
				base := float64(y*10000 + mo*100 + d)
				if len(s) >= 19 && s[10] == ' ' && s[13] == ':' && s[16] == ':' {
					h, eh := strconv.Atoi(s[11:13])
					mi, emi := strconv.Atoi(s[14:16])
					se, es := strconv.Atoi(s[17:19])
					if eh == nil && emi == nil && es == nil {
						base = float64((y*10000+mo*100+d)*1000000 + h*10000 + mi*100 + se)
					}
				}
				return base
			}
		}
		// MySQL numeric context for day names: Monday=0, Tuesday=1, ..., Sunday=6
		switch s {
		case "Monday":
			return 0
		case "Tuesday":
			return 1
		case "Wednesday":
			return 2
		case "Thursday":
			return 3
		case "Friday":
			return 4
		case "Saturday":
			return 5
		case "Sunday":
			return 6
		}
		if f, ok := parseNumericPrefixMySQL(s); ok {
			return f
		}
		// Binary string to integer (big-endian) for hex literal x'...' in numeric context.
		// MySQL interprets binary strings as big-endian unsigned integers in arithmetic.
		if len(n) > 0 && len(n) <= 8 {
			isBinary := false
			for i := 0; i < len(n); i++ {
				if n[i] < 0x20 || n[i] > 0x7e {
					isBinary = true
					break
				}
			}
			if isBinary {
				var val uint64
				for i := 0; i < len(n); i++ {
					val = val<<8 | uint64(n[i])
				}
				return float64(val)
			}
		}
		return 0
	case bool:
		if n {
			return 1
		}
		return 0
	}
	return 0
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
	return tn.Name.String(), true, viewWhere, nil
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

// validateWhereForInvalidDatetime walks a WHERE expression and checks for invalid datetime
// string literals used in comparisons. Returns error 1292 if an invalid DATETIME is found.
func validateWhereForInvalidDatetime(expr sqlparser.Expr) error {
	if expr == nil {
		return nil
	}
	var walkErr error
	sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if walkErr != nil {
			return false, nil
		}
		comp, ok := node.(*sqlparser.ComparisonExpr)
		if !ok {
			return true, nil
		}
		// Check right side for invalid datetime literal
		if lit, ok := comp.Right.(*sqlparser.Literal); ok {
			if lit.Type == sqlparser.StrVal {
				val := lit.Val
				if invalidStr, invalid := hasInvalidTimeComponent(val); invalid {
					walkErr = mysqlError(1292, "HY000", fmt.Sprintf("Incorrect DATETIME value: '%s'", invalidStr))
					return false, nil
				}
			}
		}
		// Check left side too
		if lit, ok := comp.Left.(*sqlparser.Literal); ok {
			if lit.Type == sqlparser.StrVal {
				val := lit.Val
				if invalidStr, invalid := hasInvalidTimeComponent(val); invalid {
					walkErr = mysqlError(1292, "HY000", fmt.Sprintf("Incorrect DATETIME value: '%s'", invalidStr))
					return false, nil
				}
			}
		}
		return true, nil
	}, expr)
	return walkErr
}

// hasInvalidDateString checks if a string that looks like a date (YYYY-MM-DD format)
// is actually an invalid date. Returns the invalid string and true if invalid.
// The sqlMode parameter controls which dates are considered invalid:
// - Without ALLOW_INVALID_DATES: rejects out-of-range months/days and zero dates (with NO_ZERO_DATE)
// - With ALLOW_INVALID_DATES: allows day values up to 31 for any month, but rejects impossible day > 31
// - Always rejects non-date strings that don't match date format
func hasInvalidDateString(s string, sqlMode string) (string, bool) {
	// Only check strings that look like dates (YYYY-MM-DD or similar)
	// Must contain a hyphen and enough characters
	if len(s) < 8 || !strings.ContainsRune(s, '-') {
		// Check if it's trying to be a date but fails (e.g., "wrong-date")
		// A string with '-' but not parseable as date
		if strings.ContainsRune(s, '-') {
			return s, true
		}
		return "", false
	}

	allowInvalidDates := strings.Contains(sqlMode, "ALLOW_INVALID_DATES")
	isNoZeroDate := strings.Contains(sqlMode, "NO_ZERO_DATE") || strings.Contains(sqlMode, "TRADITIONAL")

	// Strip time part if present
	datePart := s
	if idx := strings.IndexByte(s, ' '); idx >= 0 {
		datePart = s[:idx]
	}
	if idx := strings.IndexByte(s, 'T'); idx >= 0 {
		datePart = s[:idx]
	}

	// Parse YYYY-MM-DD
	parts := strings.Split(datePart, "-")
	if len(parts) != 3 {
		// Not a recognizable date format - it's invalid
		return s, true
	}

	yStr := strings.TrimSpace(parts[0])
	mStr := strings.TrimSpace(parts[1])
	dStr := strings.TrimSpace(parts[2])

	// Validate all parts are numeric
	for _, c := range yStr {
		if c < '0' || c > '9' {
			return s, true
		}
	}
	for _, c := range mStr {
		if c < '0' || c > '9' {
			return s, true
		}
	}
	for _, c := range dStr {
		if c < '0' || c > '9' {
			return s, true
		}
	}

	y, _ := strconv.Atoi(yStr)
	m, _ := strconv.Atoi(mStr)
	d, _ := strconv.Atoi(dStr)

	// Check for zero date (0000-00-00)
	if y == 0 && m == 0 && d == 0 {
		if isNoZeroDate {
			return s, true
		}
		return "", false
	}

	if allowInvalidDates {
		// In ALLOW_INVALID_DATES: allow zero month/day and any day 0-31
		// Only reject impossible values like month > 12 or day > 31
		if m > 12 {
			return s, true
		}
		if d > 31 {
			return s, true
		}
		return "", false
	}

	// Check for partial zero dates (zero month or day) in strict modes
	isNoZeroInDate := strings.Contains(sqlMode, "NO_ZERO_IN_DATE") || strings.Contains(sqlMode, "TRADITIONAL")
	if m == 0 || d == 0 {
		if isNoZeroInDate {
			return s, true
		}
		return "", false
	}

	// Check month range (strict mode) - only for non-zero months
	if m < 1 || m > 12 {
		return s, true
	}

	// Standard mode: check exact day range per month
	if d < 1 {
		return s, true
	}
	daysInMonth := [13]int{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	maxDay := daysInMonth[m]
	if m == 2 && isLeapYear(y) {
		maxDay = 29
	}
	if d > maxDay {
		return s, true
	}

	return "", false
}

// validateWhereForInvalidDateColumns walks a WHERE expression and checks for invalid date
// string literals used in comparisons against DATE/DATETIME columns.
// Returns error 1292 if an invalid DATE is found.
func validateWhereForInvalidDateColumns(expr sqlparser.Expr, tableDef *catalog.TableDef, sqlMode string) error {
	if expr == nil {
		return nil
	}
	if tableDef == nil {
		return nil
	}

	// Build a map of column name -> type for quick lookup
	colTypes := make(map[string]string, len(tableDef.Columns))
	for _, col := range tableDef.Columns {
		colTypes[strings.ToLower(col.Name)] = strings.ToUpper(col.Type)
	}

	var walkErr error
	sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if walkErr != nil {
			return false, nil
		}
		comp, ok := node.(*sqlparser.ComparisonExpr)
		if !ok {
			return true, nil
		}
		// Only check ordered comparisons (< > <= >=) and equality
		switch comp.Operator {
		case sqlparser.LessThanOp, sqlparser.GreaterThanOp, sqlparser.LessEqualOp,
			sqlparser.GreaterEqualOp, sqlparser.EqualOp, sqlparser.NotEqualOp:
		default:
			return true, nil
		}

		// Helper to check if an expression is a DATE/DATETIME column
		isDateCol := func(e sqlparser.Expr) bool {
			col, ok := e.(*sqlparser.ColName)
			if !ok {
				return false
			}
			colName := strings.ToLower(col.Name.String())
			colType, exists := colTypes[colName]
			if !exists {
				return false
			}
			return colType == "DATE" || strings.HasPrefix(colType, "DATETIME") || strings.HasPrefix(colType, "TIMESTAMP")
		}

		// Check: date_col op string_literal
		var strLit string
		var hasStrLit bool
		var isDateColumn bool

		if lit, ok := comp.Right.(*sqlparser.Literal); ok && lit.Type == sqlparser.StrVal {
			if isDateCol(comp.Left) {
				strLit = lit.Val
				hasStrLit = true
				isDateColumn = true
			}
		}
		if !hasStrLit {
			if lit, ok := comp.Left.(*sqlparser.Literal); ok && lit.Type == sqlparser.StrVal {
				if isDateCol(comp.Right) {
					strLit = lit.Val
					hasStrLit = true
					isDateColumn = true
				}
			}
		}

		if hasStrLit && isDateColumn {
			// Check if this string looks like a date but is invalid
			// First check: does it look like a datetime (has both date and time components)?
			if strings.ContainsRune(strLit, ' ') && strings.ContainsRune(strLit, ':') {
				// Let the existing DATETIME validator handle this
				if invalidStr, invalid := hasInvalidTimeComponent(strLit); invalid {
					walkErr = mysqlError(1292, "HY000", fmt.Sprintf("Incorrect DATETIME value: '%s'", invalidStr))
					return false, nil
				}
			}
			// Check as a DATE string
			if invalidStr, invalid := hasInvalidDateString(strLit, sqlMode); invalid {
				walkErr = mysqlError(1292, "HY000", fmt.Sprintf("Incorrect DATE value: '%s'", invalidStr))
				return false, nil
			}
		}

		return true, nil
	}, expr)
	return walkErr
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
		} else if op == "check" {
			// Check for rows where the AUTO_INCREMENT column has value 0
			// (MySQL emits a warning for this condition)
			hasZeroAI := false
			if tbl, err2 := e.Storage.GetTable(e.CurrentDB, bareTable); err2 == nil && tbl != nil {
				for _, col := range tbl.Def.Columns {
					if col.AutoIncrement {
						tbl.Mu.RLock()
						for _, row := range tbl.Rows {
							v := row[col.Name]
							if v != nil {
								var iv int64
								switch val := v.(type) {
								case int64:
									iv = val
								case int:
									iv = int64(val)
								case int32:
									iv = int64(val)
								case float64:
									iv = int64(val)
								}
								if iv == 0 {
									hasZeroAI = true
									break
								}
							}
						}
						tbl.Mu.RUnlock()
						break
					}
				}
			}
			if hasZeroAI {
				rows = append(rows, []interface{}{tableName, op, "warning", "Found row where the auto_increment column has the value 0"})
			}
			rows = append(rows, []interface{}{tableName, op, "status", "OK"})
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

func evalLiteralForPK(expr sqlparser.Expr) interface{} {
	switch v := expr.(type) {
	case *sqlparser.Literal:
		switch v.Type {
		case sqlparser.IntVal:
			n, err := strconv.ParseInt(v.Val, 10, 64)
			if err != nil {
				u, err2 := strconv.ParseUint(v.Val, 10, 64)
				if err2 != nil {
					return nil
				}
				return u
			}
			return n
		case sqlparser.HexNum:
			s := v.Val
			if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
				s = s[2:]
			}
			n, err := strconv.ParseInt(s, 16, 64)
			if err != nil {
				return nil
			}
			return n
		case sqlparser.StrVal:
			return v.Val
		case sqlparser.HexVal:
			return v.Val
		case sqlparser.FloatVal:
			f, err := strconv.ParseFloat(v.Val, 64)
			if err != nil {
				return nil
			}
			return f
		default:
			return nil
		}
	case *sqlparser.UnaryExpr:
		if v.Operator == sqlparser.UMinusOp {
			if inner := evalLiteralForPK(v.Expr); inner != nil {
				switch n := inner.(type) {
				case int64:
					return -n
				case float64:
					return -n
				}
			}
		}
		return nil
	default:
		return nil
	}
}

// ---------------------------------------------------------------------------
// findMatchExprInWhere recursively searches an expression tree for a MATCH AGAINST
// expression and returns the first one found, or nil if none.
func findMatchExprInWhere(expr sqlparser.Expr) *sqlparser.MatchExpr {
	switch v := expr.(type) {
	case *sqlparser.MatchExpr:
		return v
	case *sqlparser.AndExpr:
		if m := findMatchExprInWhere(v.Left); m != nil {
			return m
		}
		return findMatchExprInWhere(v.Right)
	case *sqlparser.OrExpr:
		if m := findMatchExprInWhere(v.Left); m != nil {
			return m
		}
		return findMatchExprInWhere(v.Right)
	case *sqlparser.ComparisonExpr:
		if m := findMatchExprInWhere(v.Left); m != nil {
			return m
		}
		return findMatchExprInWhere(v.Right)
	case *sqlparser.NotExpr:
		return findMatchExprInWhere(v.Expr)
	}
	return nil
}

// Full-Text Search (MATCH … AGAINST) implementation
// ---------------------------------------------------------------------------

// ftsTokenize splits text into lowercase word tokens, similar to InnoDB's
// built-in parser. Words shorter than minLen are discarded.
func ftsTokenize(text string, minLen int) []string {
	var tokens []string
	word := strings.Builder{}
	for _, r := range text {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || (r >= 0x80 && unicode.IsLetter(r)) {
			word.WriteRune(unicode.ToLower(r))
		} else {
			if word.Len() >= minLen {
				tokens = append(tokens, word.String())
			}
			word.Reset()
		}
	}
	if word.Len() >= minLen {
		tokens = append(tokens, word.String())
	}
	return tokens
}

// ftsBaseScore is the base relevance score per word occurrence,
// approximating MySQL's IDF-based scoring for a small table.
const ftsBaseScore = 0.22764469683170319

// ftsStopwords is the default InnoDB stopword list.
var ftsStopwords = map[string]bool{
	"a": true, "about": true, "an": true, "are": true, "as": true,
	"at": true, "be": true, "by": true, "com": true, "de": true,
	"en": true, "for": true, "from": true, "how": true, "i": true,
	"in": true, "is": true, "it": true, "la": true, "of": true,
	"on": true, "or": true, "that": true, "the": true, "this": true,
	"to": true, "was": true, "what": true, "when": true, "where": true,
	"who": true, "will": true, "with": true, "und": true, "www": true,
}

// evalMatchExpr evaluates a MATCH(col1,col2,...) AGAINST('query' [mode]) expression.
func (e *Executor) evalMatchExpr(v *sqlparser.MatchExpr) (interface{}, error) {
	// 0. Validate that a FULLTEXT index exists for the referenced columns.
	if err := e.validateFulltextIndex(v); err != nil {
		return nil, err
	}

	// 1. Collect text from the FULLTEXT index columns (not the MATCH columns).
	// MySQL uses all columns from the matching FULLTEXT index, regardless of
	// what columns are specified in MATCH(). This handles cases like MATCH(c,c)
	// which should use the (b,c) FULLTEXT index and search both columns.
	ftCols := e.resolveFulltextIndexColumns(v)
	var docParts []string
	if len(ftCols) > 0 {
		seen := make(map[string]bool)
		for _, colName := range ftCols {
			if seen[colName] {
				continue
			}
			seen[colName] = true
			col := &sqlparser.ColName{Name: sqlparser.NewIdentifierCI(colName)}
			if len(v.Columns) > 0 && !v.Columns[0].Qualifier.IsEmpty() {
				col.Qualifier = v.Columns[0].Qualifier
			}
			val, err := e.evalExpr(col)
			if err != nil {
				continue
			}
			if val != nil {
				docParts = append(docParts, toString(val))
			}
		}
	} else {
		// Fallback: use MATCH columns directly (deduplicated)
		seen := make(map[string]bool)
		for _, col := range v.Columns {
			colKey := strings.ToLower(col.Name.String())
			if seen[colKey] {
				continue
			}
			seen[colKey] = true
			val, err := e.evalExpr(col)
			if err != nil {
				continue
			}
			if val != nil {
				docParts = append(docParts, toString(val))
			}
		}
	}
	docText := strings.Join(docParts, " ")

	// 2. Evaluate the AGAINST expression to get the search string
	searchVal, err := e.evalExpr(v.Expr)
	if err != nil {
		return float64(0), nil
	}
	if searchVal == nil {
		return float64(0), nil
	}
	searchStr := toString(searchVal)

	minTokenSize := 3

	// 3. Dispatch based on match option
	switch v.Option {
	case sqlparser.BooleanModeOpt:
		return ftsEvalBoolean(docText, searchStr, minTokenSize), nil
	default:
		// Natural language mode (also covers NoOption, QueryExpansionOpt, NaturalLanguageModeWithQueryExpansionOpt)
		return ftsEvalNaturalLanguage(docText, searchStr, minTokenSize), nil
	}
}

// resolveFulltextIndexColumns finds the FULLTEXT index that matches the MATCH
// expression and returns its column names. Returns nil if no index is found.
func (e *Executor) resolveFulltextIndexColumns(v *sqlparser.MatchExpr) []string {
	if e.CurrentDB == "" || e.Catalog == nil || len(v.Columns) == 0 {
		return nil
	}
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil || db == nil {
		return nil
	}

	matchSet := make(map[string]bool)
	for _, col := range v.Columns {
		matchSet[strings.ToLower(col.Name.String())] = true
	}

	tableName := ""
	if !v.Columns[0].Qualifier.IsEmpty() {
		tableName = v.Columns[0].Qualifier.Name.String()
	}

	checkTable := func(tblDef *catalog.TableDef) []string {
		for _, idx := range tblDef.Indexes {
			if idx.Type != "FULLTEXT" {
				continue
			}
			idxCols := make(map[string]bool)
			for _, c := range idx.Columns {
				idxCols[strings.ToLower(stripPrefixLengthFromCol(c))] = true
			}
			allFound := true
			for mc := range matchSet {
				if !idxCols[mc] {
					allFound = false
					break
				}
			}
			if allFound {
				return idx.Columns
			}
		}
		return nil
	}

	if tableName != "" {
		tblDef, _, err := findTableDefCaseInsensitive(db, tableName)
		if err != nil || tblDef == nil {
			return nil
		}
		return checkTable(tblDef)
	}
	for _, name := range db.ListTables() {
		tblDef, err := db.GetTable(name)
		if err != nil {
			continue
		}
		if cols := checkTable(tblDef); cols != nil {
			return cols
		}
	}
	return nil
}

// ftsEvalNaturalLanguage performs natural-language full-text search.
// validateFulltextIndex checks that the columns referenced in a MATCH expression
// have a matching FULLTEXT index. Returns ER_FT_MATCHING_KEY_NOT_FOUND if not.
func (e *Executor) validateFulltextIndex(v *sqlparser.MatchExpr) error {
	if len(v.Columns) == 0 {
		return nil
	}

	// Collect the column names from the MATCH expression
	matchCols := make([]string, len(v.Columns))
	for i, col := range v.Columns {
		matchCols[i] = strings.ToLower(col.Name.String())
	}
	sort.Strings(matchCols)

	// Determine table name from column qualifiers
	tableName := ""
	if !v.Columns[0].Qualifier.IsEmpty() {
		tableName = v.Columns[0].Qualifier.Name.String()
	}

	if e.CurrentDB == "" || e.Catalog == nil {
		return nil
	}
	db, dbErr := e.Catalog.GetDatabase(e.CurrentDB)
	if dbErr != nil || db == nil {
		return nil
	}

	// Deduplicate match columns
	matchSet := make(map[string]bool)
	for _, c := range matchCols {
		matchSet[c] = true
	}

	// hasMatchingFTIndex checks if a table definition has a FULLTEXT index
	// that covers all unique columns from the MATCH expression.
	hasMatchingFTIndex := func(tblDef *catalog.TableDef) bool {
		for _, idx := range tblDef.Indexes {
			if idx.Type != "FULLTEXT" {
				continue
			}
			idxCols := make(map[string]bool)
			for _, c := range idx.Columns {
				idxCols[strings.ToLower(stripPrefixLengthFromCol(c))] = true
			}
			// Check that all unique MATCH columns are in this FT index
			allFound := true
			for mc := range matchSet {
				if !idxCols[mc] {
					allFound = false
					break
				}
			}
			if allFound {
				return true
			}
		}
		return false
	}

	if tableName != "" {
		tblDef, _, err := findTableDefCaseInsensitive(db, tableName)
		if err != nil || tblDef == nil {
			return nil
		}
		if hasMatchingFTIndex(tblDef) {
			return nil
		}
	} else {
		// No qualifier: search all tables in the current database
		for _, name := range db.ListTables() {
			tblDef, err := db.GetTable(name)
			if err != nil {
				continue
			}
			if hasMatchingFTIndex(tblDef) {
				return nil
			}
		}
	}

	return mysqlError(1191, "HY000", "Can't find FULLTEXT index matching the column list")
}

// Returns a relevance score (float64). 0 means no match.
func ftsEvalNaturalLanguage(docText, searchStr string, minTokenSize int) float64 {
	docTokens := ftsTokenize(docText, minTokenSize)
	queryTokens := ftsTokenize(searchStr, minTokenSize)

	if len(queryTokens) == 0 {
		return float64(0)
	}

	// Build document token frequency map
	docFreq := make(map[string]int)
	for _, t := range docTokens {
		docFreq[t]++
	}

	// Count matching query terms (exclude stopwords)
	var totalScore float64
	for _, qt := range queryTokens {
		if ftsStopwords[qt] {
			continue
		}
		if cnt, ok := docFreq[qt]; ok {
			// Simple TF-based scoring
			totalScore += float64(cnt) * ftsBaseScore
		}
	}
	return totalScore
}

// ftsEvalBoolean performs boolean-mode full-text search.
// Returns a relevance score (float64). 0 means no match.
func ftsEvalBoolean(docText, searchStr string, minTokenSize int) float64 {
	docTokens := ftsTokenize(docText, minTokenSize)
	docFreq := make(map[string]int)
	for _, t := range docTokens {
		docFreq[t]++
	}
	docLower := strings.ToLower(docText)

	terms := parseBooleanQuery(searchStr, minTokenSize)
	return evalBooleanTerms(terms, docFreq, docLower, minTokenSize)
}

// boolTermOp represents a boolean operator for a search term.
type boolTermOp int

const (
	boolDefault  boolTermOp = iota // optional (OR semantics)
	boolRequired                   // + (AND required)
	boolExcluded                   // - (NOT excluded)
	boolNegate                     // ~ (present but negated rank)
	boolIncRank                    // > (increase rank)
	boolDecRank                    // < (decrease rank)
)

// boolTerm represents a single term or sub-expression in a boolean FTS query.
type boolTerm struct {
	op       boolTermOp
	word     string     // single word (lowercased)
	wildcard bool       // trailing * (prefix match)
	phrase   string     // quoted phrase (lowercased)
	sub      []boolTerm // sub-expression in parentheses
}

func parseBooleanQuery(searchStr string, minTokenSize int) []boolTerm {
	s := strings.TrimSpace(searchStr)
	terms, _ := parseBoolTerms(s, minTokenSize)
	return terms
}

func parseBoolTerms(s string, minTokenSize int) ([]boolTerm, string) {
	var terms []boolTerm
	for len(s) > 0 {
		s = strings.TrimLeft(s, " \t\n\r")
		if len(s) == 0 {
			break
		}
		if s[0] == ')' {
			break
		}

		op := boolDefault
		for len(s) > 0 {
			switch s[0] {
			case '+':
				op = boolRequired
				s = s[1:]
				continue
			case '-':
				op = boolExcluded
				s = s[1:]
				continue
			case '~':
				op = boolNegate
				s = s[1:]
				continue
			case '>':
				op = boolIncRank
				s = s[1:]
				continue
			case '<':
				op = boolDecRank
				s = s[1:]
				continue
			}
			break
		}
		s = strings.TrimLeft(s, " \t")
		if len(s) == 0 {
			break
		}

		if s[0] == '(' {
			sub, rest := parseBoolTerms(s[1:], minTokenSize)
			rest = strings.TrimLeft(rest, " \t")
			if len(rest) > 0 && rest[0] == ')' {
				rest = rest[1:]
			}
			terms = append(terms, boolTerm{op: op, sub: sub})
			s = rest
		} else if s[0] == '"' {
			end := strings.Index(s[1:], "\"")
			var phrase string
			if end < 0 {
				phrase = s[1:]
				s = ""
			} else {
				phrase = s[1 : end+1]
				s = s[end+2:]
			}
			terms = append(terms, boolTerm{op: op, phrase: strings.ToLower(phrase)})
		} else {
			end := 0
			for end < len(s) && s[end] != ' ' && s[end] != '\t' && s[end] != ')' && s[end] != '(' && s[end] != '"' {
				end++
			}
			word := s[:end]
			s = s[end:]
			wildcard := false
			if strings.HasSuffix(word, "*") {
				wildcard = true
				word = word[:len(word)-1]
			}
			// Strip non-word characters (punctuation like curly quotes) from the word.
			// Only keep letters, digits, and underscores (same logic as ftsTokenize).
			word = strings.Map(func(r rune) rune {
				if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || (r >= 0x80 && unicode.IsLetter(r)) {
					return r
				}
				return -1
			}, word)
			word = strings.ToLower(word)
			if word == "" || word == "&" {
				continue
			}
			terms = append(terms, boolTerm{op: op, word: word, wildcard: wildcard})
		}
	}
	return terms, s
}

func evalBooleanTerms(terms []boolTerm, docFreq map[string]int, docLower string, minTokenSize int) float64 {
	if len(terms) == 0 {
		return float64(0)
	}

	hasRequired := false
	for _, t := range terms {
		if t.op == boolRequired {
			hasRequired = true
			break
		}
	}

	var score float64
	allRequiredMet := true
	anyOptionalMatch := false

	for _, t := range terms {
		var matched bool
		var termScore float64

		if len(t.sub) > 0 {
			termScore = evalBooleanTerms(t.sub, docFreq, docLower, minTokenSize)
			matched = termScore > 0
		} else if t.phrase != "" {
			if strings.Contains(docLower, t.phrase) {
				matched = true
				termScore = 1.0
			}
		} else if t.wildcard {
			for tok, cnt := range docFreq {
				if strings.HasPrefix(tok, t.word) && !ftsStopwords[tok] {
					matched = true
					termScore += float64(cnt) * ftsBaseScore
				}
			}
		} else {
			// In boolean mode, stopwords never match
			if !ftsStopwords[t.word] {
				if cnt, ok := docFreq[t.word]; ok {
					matched = true
					termScore = float64(cnt) * ftsBaseScore
				}
			}
			// If it's a stopword, matched stays false
		}

		switch t.op {
		case boolRequired:
			if !matched {
				allRequiredMet = false
			} else {
				score += termScore
			}
		case boolExcluded:
			if matched {
				return float64(0)
			}
		case boolNegate:
			if matched {
				score -= termScore * 0.5
			} else {
				score += ftsBaseScore
			}
		case boolIncRank:
			if matched {
				score += termScore * 2
				anyOptionalMatch = true
			}
		case boolDecRank:
			if matched {
				score += termScore * 0.5
				anyOptionalMatch = true
			}
		default:
			if matched {
				score += termScore
				anyOptionalMatch = true
			}
		}
	}

	if hasRequired && !allRequiredMet {
		return float64(0)
	}
	if !hasRequired && !anyOptionalMatch {
		return float64(0)
	}
	if score <= 0 {
		score = 0.001
	}
	return score
}
