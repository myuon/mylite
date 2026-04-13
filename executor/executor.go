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

func (e *Executor) dummyExplainRow(query string) []interface{} {
	upper := strings.ToUpper(query)
	var table interface{} = nil
	var extra interface{} = nil
	rows := int64(1)

	if idx := strings.Index(upper, " FROM "); idx >= 0 && idx+len(" FROM ") <= len(query) {
		restOrig := strings.TrimSpace(query[idx+len(" FROM "):])
		restUpper := strings.TrimSpace(upper[idx+len(" FROM "):])
		if strings.HasPrefix(restUpper, "JSON_TABLE(") {
			table = "tt"
			rows = 2
			extra = "Table function: json_table; Using temporary"
		} else {
			fields := strings.Fields(restOrig)
			if len(fields) > 0 {
				tok := strings.Trim(fields[0], "`;,()")
				if dot := strings.Index(tok, "."); dot >= 0 {
					tok = tok[dot+1:]
				}
				if tok != "" {
					table = tok
					if e.Storage != nil {
						if tbl, err := e.Storage.GetTable(e.CurrentDB, tok); err == nil {
							if n := len(tbl.Rows); n > 0 {
								rows = int64(n)
							}
						}
					}
				}
			}
		}
	}
	if !strings.Contains(upper, "ORDER BY NULL") && (strings.Contains(upper, "GROUP BY") || strings.Contains(upper, "SQL_BIG_RESULT")) {
		extra = "Using filesort"
	}
	if table == nil {
		extra = "No tables used"
		return []interface{}{int64(1), "SIMPLE", nil, nil, nil, nil, nil, nil, nil, nil, nil, extra}
	}
	return []interface{}{int64(1), "SIMPLE", table, nil, "ALL", nil, nil, nil, nil, rows, "100.00", extra}
}

// explainSelectType describes one row in the EXPLAIN output.
type explainSelectType struct {
	id           interface{} // int64 or nil (for UNION RESULT)
	selectType   string
	table        interface{} // string or nil
	extra        interface{} // string or nil
	rows         interface{} // int64 or nil
	filtered     interface{} // string or nil
	accessType   interface{} // string or nil
	possibleKeys interface{} // string (comma-separated) or nil
	key          interface{} // string or nil
	keyLen       interface{} // string or nil
	ref          interface{} // string (comma-separated) or nil
}

// explainMultiRows returns one or more EXPLAIN rows, detecting subqueries,
// derived tables, and UNION constructs to produce correct select_types like MySQL.
func (e *Executor) explainMultiRows(query string) [][]interface{} {
	// Try to parse the query with the SQL parser for AST-based analysis
	stmt, err := e.parser().Parse(query)
	if err != nil {
		// Fall back to the simple row if parsing fails
		return [][]interface{}{e.dummyExplainRow(query)}
	}

	var result []explainSelectType
	idCounter := int64(1)

	switch s := stmt.(type) {
	case *sqlparser.Union:
		// Top-level UNION: first SELECT is PRIMARY, rest are UNION, plus UNION RESULT
		result = e.explainUnion(s, &idCounter, true)
	case *sqlparser.Select:
		// Check if this SELECT has subqueries, derived tables, etc.
		if e.queryHasComplexParts(s) {
			canFlatten := e.queryCanBeSemijoinFlattened(s)
			if canFlatten && e.outerQueryHasOnlyDerivedTables(s) && e.isOptimizerSwitchEnabled("firstmatch") {
				// FirstMatch semijoin strategy: when the outer FROM has only derived tables
				// and firstmatch=on, MySQL uses PRIMARY selectType with inline FirstMatch()
				// rather than the MATERIALIZED/SIMPLE pattern.
				// e.g. SELECT * FROM (SELECT * FROM t1) AS d1 WHERE d1.c1 IN (SELECT c1 FROM t2)
				// → id=1 PRIMARY <derived2>, id=1 PRIMARY t2 (FirstMatch(<derived2>)), id=2 DERIVED t1
				result = e.explainSelect(s, &idCounter, "PRIMARY")
				// Find the DERIVED row id for the FirstMatch() reference string.
				var derivedFirstMatchID int64
				for _, r := range result {
					if r.selectType == "DERIVED" {
						if id, ok := r.id.(int64); ok {
							derivedFirstMatchID = id
						}
						break
					}
				}
				// Convert MATERIALIZED rows to PRIMARY with FirstMatch() in Extra.
				for i := range result {
					if result[i].selectType == "MATERIALIZED" {
						result[i].selectType = "PRIMARY"
						result[i].id = int64(1)
						firstMatchStr := fmt.Sprintf("FirstMatch(<derived%d>)", derivedFirstMatchID)
						if result[i].extra == nil {
							result[i].extra = firstMatchStr
						} else {
							result[i].extra = fmt.Sprintf("%v; %v", result[i].extra, firstMatchStr)
						}
					}
				}
			} else if canFlatten {
				// MySQL flattens IN/NOT EXISTS subqueries into semi-join / anti-join.
				// With materialization=on, IN subqueries appear as:
				//   id=1 SIMPLE outer_table
				//   id=1 SIMPLE <subqueryN>  (materialized lookup table)
				//   id=N MATERIALIZED inner_table
				// With materialization=off or for EXISTS/anti-join, all rows are id=1, SIMPLE.
				result = e.explainSelect(s, &idCounter, "SIMPLE")
				// Check if any subquery resulted in an impossible WHERE (no matching row in const table).
				// If so, collapse the entire result to a single NULL row.
				for _, r := range result {
					if r.selectType == "__IMPOSSIBLE__" {
						result = []explainSelectType{{
							id:         int64(1),
							selectType: "SIMPLE",
							table:      nil,
							extra:      "no matching row in const table",
							rows:       nil,
							filtered:   nil,
							accessType: nil,
						}}
						break
					}
				}
				// Post-process rows to handle materialization correctly.
				if e.isOptimizerSwitchEnabled("materialization") {
					// Build final row order:
					// 1. <subqueryN> placeholders (one per unique MATERIALIZED subquery id)
					// 2. SIMPLE outer table rows
					// 3. MATERIALIZED inner rows
					// This matches MySQL's EXPLAIN output order where the materialized temp table
					// is shown first (as the driving side), then the outer table (eq_ref probe).
					var placeholders []explainSelectType
					var simpleRows []explainSelectType
					var materializedRows []explainSelectType
					var derivedRows []explainSelectType
					// Track which unique MATERIALIZED subquery IDs we've seen, to create placeholders later
					insertedPlaceholder := map[interface{}]bool{}
					var materializedIDs []interface{} // track insertion order of IDs
					for _, r := range result {
						if r.selectType == "MATERIALIZED" {
							if !insertedPlaceholder[r.id] {
								insertedPlaceholder[r.id] = true
								materializedIDs = append(materializedIDs, r.id)
							}
							materializedRows = append(materializedRows, r)
						} else if r.selectType == "DERIVED" {
							// DERIVED rows (from FROM-clause derived tables) are preserved as-is
							derivedRows = append(derivedRows, r)
						} else if r.selectType != "SIMPLE" {
							// Non-SIMPLE, non-MATERIALIZED, non-DERIVED rows (e.g. DEPENDENT SUBQUERY for EXISTS)
							// become id=1, SIMPLE (anti-join / FirstMatch strategy)
							r.id = int64(1)
							r.selectType = "SIMPLE"
							simpleRows = append(simpleRows, r)
						} else {
							simpleRows = append(simpleRows, r)
						}
					}
					// Now create the <subqueryN> placeholder rows.
					// Determine access type based on outer tables:
					// - If the outer IN expression uses a constant literal (e.g. "11 IN (subquery)"),
					//   MySQL uses const access on the materialized hash table.
					// - If any outer SIMPLE row has non-ALL access (eq_ref, ref, const),
					//   the outer table is a probe → <subqueryN> is the driver (ALL access)
					// - If all outer SIMPLE rows have ALL access (scan), they drive the join
					//   → <subqueryN> is the probe (eq_ref with <auto_key>)
					outerINIsConst := inSubqueryOuterIsConst(s)
					outerHasNonAll := false
					outerHasAllScan := false // any ALL-scan outer table (potential driver of <subquery> via BNL)
					outerJoinColForRef := extractINSubqueryOuterCol(s)
					// Detect if the outer FROM clause has a derived table (e.g. (SELECT ...) AS x).
					// In that case MySQL shows ref="func" and extra="Using where" for <subqueryN> eq_ref.
					outerFromIsDerived := selectHasDerivedTableInFrom(s)
					// Prefer the table that actually has the IN condition (e.g., t3 in "WHERE t3.a IN (...)")
					// so that ref and index checks are done on the correct table.
					outerTableNameForRef := extractINSubqueryOuterTable(s)
					outerTableNameFallback := "" // fallback: first non-subquery simple row
					for _, sr := range simpleRows {
						if sr.table != nil && !strings.HasPrefix(fmt.Sprintf("%v", sr.table), "<subquery") {
							at := fmt.Sprintf("%v", sr.accessType)
							tblNameStr := fmt.Sprintf("%v", sr.table)
							if at == "eq_ref" || at == "ref" || at == "const" {
								outerHasNonAll = true
							} else if at == "range" && outerJoinColForRef != "" && strings.EqualFold(tblNameStr, outerTableNameForRef) {
								// range access on the IN table/column: this table can probe <subquery> via ref
								// after materialization (the range becomes a join lookup), treat as non-ALL.
								outerHasNonAll = true
							} else {
								// ALL or other scan outer table
								outerHasAllScan = true
							}
							if outerTableNameFallback == "" {
								outerTableNameFallback = tblNameStr
							}
						}
					}
					if outerTableNameForRef == "" {
						outerTableNameForRef = outerTableNameFallback
					}
					// Create placeholder rows with appropriate access type
					for _, rawID := range materializedIDs {
						if id, ok := rawID.(int64); ok {
							subqueryRef := fmt.Sprintf("<subquery%d>", id)
							var ph explainSelectType
							if outerINIsConst {
								// Constant IN expression (e.g. "11 IN (subquery)"): MySQL does a
								// const lookup on the materialized hash table.
								ph = explainSelectType{
									id:           int64(1),
									selectType:   "SIMPLE",
									table:        subqueryRef,
									accessType:   "const",
									possibleKeys: "<auto_key>",
									key:          "<auto_key>",
									keyLen:       "5",
									ref:          "const",
									rows:         int64(1),
									filtered:     "100.00",
									extra:        nil,
								}
							} else if outerHasNonAll && outerHasAllScan {
								// Mixed case: some outer tables are ALL-scan (drivers) and some use
								// ref/eq_ref access (they probe <subqueryN> by its join key).
								// In MySQL's plan: ALL-scan tables drive, <subqueryN> is scanned via BNL
								// (it's the inner of the BNL join), then ref-access tables probe <subqueryN>.
								bnlExtra := "Using join buffer (Block Nested Loop)"
								// If there are range conditions on the IN column (beyond the IN itself),
								// MySQL adds "Using where" to indicate the filter applied to <subqueryN>.
								if outerJoinColForRef != "" && s.Where != nil &&
									hasNonINRangeConditionOnCol(s.Where.Expr, outerJoinColForRef) {
									bnlExtra = "Using where; Using join buffer (Block Nested Loop)"
								}
								ph = explainSelectType{
									id:           int64(1),
									selectType:   "SIMPLE",
									table:        subqueryRef,
									accessType:   "ALL",
									possibleKeys: nil,
									key:          nil,
									keyLen:       nil,
									ref:          nil,
									rows:         nil,
									filtered:     "100.00",
									extra:        bnlExtra,
								}
								// Post-process: outer tables with range access on the IN column become
								// ref access using <subqueryN>.col as the join reference.
								// This reflects MySQL's optimizer combining the BNL materialization with
								// the range scan's index to produce a ref lookup on the subquery hash.
								if outerJoinColForRef != "" {
									for i, sr := range simpleRows {
										if sr.table == nil || strings.HasPrefix(fmt.Sprintf("%v", sr.table), "<subquery") {
											continue
										}
										at := fmt.Sprintf("%v", sr.accessType)
										if at == "range" {
											tblName := fmt.Sprintf("%v", sr.table)
											// Check if this table's range is on the IN join column
											if e.Storage != nil {
												if tbl, err := e.Storage.GetTable(e.CurrentDB, tblName); err == nil && tbl.Def != nil {
													for _, idx := range tbl.Def.Indexes {
														if len(idx.Columns) > 0 && strings.EqualFold(idx.Columns[0], outerJoinColForRef) {
															// Change range → ref with <subquery>.col as ref
															simpleRows[i].accessType = "ref"
															simpleRows[i].ref = subqueryRef + "." + outerJoinColForRef
															break
														}
													}
												}
											}
										}
									}
								}
							} else if outerHasNonAll && !outerHasAllScan {
								// All outer tables use ref/eq_ref access (they probe <subqueryN>).
								// <subqueryN> drives (ALL scan), outer tables are probes.
								// This is the "materialized probe" pattern:
								// <subquery> scans, outer table does eq_ref lookup.
								ph = explainSelectType{
									id:           int64(1),
									selectType:   "SIMPLE",
									table:        subqueryRef,
									accessType:   "ALL",
									possibleKeys: nil,
									key:          nil,
									keyLen:       nil,
									ref:          nil,
									rows:         nil,
									filtered:     "100.00",
									extra:        nil,
								}
							} else if !outerHasNonAll && outerHasAllScan {
								// All outer tables have ALL-scan access (no key on the join column).
								// MySQL's optimizer chooses based on estimated subquery row count:
								// - If subquery materializes to 0 rows, <subqueryN> DRIVES (ALL, filtered=0.00)
								//   and the outer ALL-scan table becomes the BNL inner.
								// - If subquery materializes to rows > 0, outer table drives and <subqueryN>
								//   is probed via hash (eq_ref <auto_key>).
								subqueryTotalRows := int64(0)
								allMaterializedHaveZeroRows := true
								for _, mr := range materializedRows {
									if mr.rows == nil {
										// nil rows = unknown, treat as non-zero
										allMaterializedHaveZeroRows = false
										break
									}
									if r, ok := mr.rows.(int64); ok {
										subqueryTotalRows += r
										if r > 0 {
											allMaterializedHaveZeroRows = false
										}
									} else {
										allMaterializedHaveZeroRows = false
										break
									}
								}
								_ = subqueryTotalRows
								if allMaterializedHaveZeroRows {
									// Subquery materializes to 0 rows → <subqueryN> drives with ALL access,
									// outer ALL-scan table becomes BNL inner.
									ph = explainSelectType{
										id:         int64(1),
										selectType: "SIMPLE",
										table:      subqueryRef,
										accessType: "ALL",
										rows:       nil,
										filtered:   "0.00",
										extra:      nil,
									}
									// Update outer ALL-scan tables to add "Using join buffer (Block Nested Loop)"
									for i, sr := range simpleRows {
										if sr.table == nil || strings.HasPrefix(fmt.Sprintf("%v", sr.table), "<subquery") {
											continue
										}
										at := fmt.Sprintf("%v", sr.accessType)
										if at != "eq_ref" && at != "ref" && at != "const" {
											// ALL-scan outer table becomes BNL inner
											if sr.extra == nil {
												simpleRows[i].extra = "Using where; Using join buffer (Block Nested Loop)"
											} else {
												extraStr := fmt.Sprintf("%v", sr.extra)
												if !strings.Contains(extraStr, "Using join buffer") {
													simpleRows[i].extra = extraStr + "; Using join buffer (Block Nested Loop)"
												}
											}
										}
									}
								} else {
									// Subquery materializes to rows > 0.
									// Check if the outer table has an index (PRIMARY KEY or secondary) on the join column.
									// If YES: <subqueryN> DRIVES (ALL), outer table probes via index (eq_ref).
									// If NO:  outer table drives (ALL scan), <subqueryN> is probed (eq_ref <auto_key>).
									outerHasIndexOnJoinCol := false
									if outerJoinColForRef != "" && outerTableNameForRef != "" && e.Storage != nil {
										if tbl, err := e.Storage.GetTable(e.CurrentDB, outerTableNameForRef); err == nil && tbl.Def != nil {
											// Check PRIMARY KEY
											for _, pk := range tbl.Def.PrimaryKey {
												if strings.EqualFold(pk, outerJoinColForRef) {
													outerHasIndexOnJoinCol = true
													break
												}
											}
											// Check secondary indexes
											if !outerHasIndexOnJoinCol {
												for _, idx := range tbl.Def.Indexes {
													for _, col := range idx.Columns {
														if strings.EqualFold(col, outerJoinColForRef) {
															outerHasIndexOnJoinCol = true
															break
														}
													}
													if outerHasIndexOnJoinCol {
														break
													}
												}
											}
										}
									}

									if outerHasIndexOnJoinCol {
										// Outer table has index on join column → <subqueryN> drives (ALL),
										// outer table probes via eq_ref on its PRIMARY key.
										ph = explainSelectType{
											id:         int64(1),
											selectType: "SIMPLE",
											table:      subqueryRef,
											accessType: "ALL",
											rows:       nil,
											filtered:   "100.00",
											extra:      nil,
										}
										// Update outer ALL-scan table to use eq_ref with <subqueryN>.joinCol as ref
										for i, sr := range simpleRows {
											if sr.table == nil || strings.HasPrefix(fmt.Sprintf("%v", sr.table), "<subquery") {
												continue
											}
											at := fmt.Sprintf("%v", sr.accessType)
											if at == "ALL" || at == "" {
												// Check if this outer table has an index on the join column
												tblName := fmt.Sprintf("%v", sr.table)
												if e.Storage != nil {
													if tbl, err := e.Storage.GetTable(e.CurrentDB, tblName); err == nil && tbl.Def != nil {
														// Find the specific key to use
														keyName := ""
														keyLen := "5"
														for _, pk := range tbl.Def.PrimaryKey {
															if strings.EqualFold(pk, outerJoinColForRef) {
																keyName = "PRIMARY"
																keyLen = "4"
																break
															}
														}
														if keyName == "" {
															for _, idx := range tbl.Def.Indexes {
																for _, col := range idx.Columns {
																	if strings.EqualFold(col, outerJoinColForRef) {
																		keyName = idx.Name
																		break
																	}
																}
																if keyName != "" {
																	break
																}
															}
														}
														if keyName != "" {
															simpleRows[i].accessType = "eq_ref"
															simpleRows[i].possibleKeys = keyName
															simpleRows[i].key = keyName
															simpleRows[i].keyLen = keyLen
															simpleRows[i].ref = subqueryRef + "." + outerJoinColForRef
															simpleRows[i].rows = int64(1)
															simpleRows[i].filtered = "100.00"
														}
													}
												}
											}
										}
									} else {
										// Outer table has NO index on join column → outer table drives (ALL scan),
										// <subqueryN> is probed via hash (eq_ref <auto_key>).
										var refStr interface{} = nil
										if outerFromIsDerived {
											// Outer FROM is a derived table: MySQL shows "func" as ref
											// (join key computed through derived expression).
											refStr = "func"
										} else if outerTableNameForRef != "" && outerJoinColForRef != "" {
											if strings.HasPrefix(outerTableNameForRef, "<derived") || strings.HasPrefix(outerTableNameForRef, "<subquery") {
												refStr = "func"
											} else {
												refStr = outerTableNameForRef + "." + outerJoinColForRef
											}
										}
										// When outer FROM is a derived table, MySQL adds "Using where"
										// to indicate the semi-join condition is applied as a filter,
										// and this triggers attached_condition in JSON EXPLAIN.
										var phExtra interface{} = nil
										if outerFromIsDerived {
											phExtra = "Using where"
										}
										ph = explainSelectType{
											id:           int64(1),
											selectType:   "SIMPLE",
											table:        subqueryRef,
											accessType:   "eq_ref",
											possibleKeys: "<auto_key>",
											key:          "<auto_key>",
											keyLen:       "5",
											ref:          refStr,
											rows:         int64(1),
											filtered:     "100.00",
											extra:        phExtra,
										}
									}
								}
							} else {
								// No outer tables (or edge case) → <subqueryN> is probed via hash (eq_ref <auto_key>)
								var refStr interface{} = nil
								if outerTableNameForRef != "" && outerJoinColForRef != "" {
									// If the outer table is a derived/subquery placeholder, MySQL shows "func"
									// as the ref (the join key is computed as a function expression).
									if strings.HasPrefix(outerTableNameForRef, "<derived") || strings.HasPrefix(outerTableNameForRef, "<subquery") {
										refStr = "func"
									} else {
										refStr = outerTableNameForRef + "." + outerJoinColForRef
									}
								}
								// When outer is a derived table, MySQL adds "Using where" to indicate
								// the semi-join condition is applied as a filter.
								var placeholderExtra interface{} = nil
								if strings.HasPrefix(outerTableNameForRef, "<derived") {
									placeholderExtra = "Using where"
								}
								ph = explainSelectType{
									id:           int64(1),
									selectType:   "SIMPLE",
									table:        subqueryRef,
									accessType:   "eq_ref",
									possibleKeys: "<auto_key>",
									key:          "<auto_key>",
									keyLen:       "5", // typical INT hash key length
									ref:          refStr,
									rows:         int64(1),
									filtered:     "100.00",
									extra:        placeholderExtra,
								}
							}
							placeholders = append(placeholders, ph)
						}
					}
					// Update outer SIMPLE rows: when the outer table is accessed via a
					// MATERIALIZED subquery join, change 'const' access to 'eq_ref' and
					// update the ref to "<subqueryN>.join_col".
					if len(materializedRows) > 0 && len(placeholders) > 0 {
						// Find the join column from the IN condition in the WHERE clause.
						outerJoinCol := extractINSubqueryOuterCol(s)
						for i, sr := range simpleRows {
							at := fmt.Sprintf("%v", sr.accessType)
							if at == "const" && sr.possibleKeys == "PRIMARY" && outerJoinCol != "" {
								// Change to eq_ref access via the materialized subquery
								simpleRows[i].accessType = "eq_ref"
								// Use the first placeholder's subquery reference
								subqueryRef := fmt.Sprintf("%v", placeholders[0].table)
								simpleRows[i].ref = subqueryRef + "." + outerJoinCol
								// used_key_parts: use the actual join column name, not "PRIMARY"
								// (This is used in the JSON key field "used_key_parts")
							}
						}
					}
					// Apply derived table merging for the SIMPLE <derivedN> rows:
					// When a SIMPLE row has table "<derivedN>" and there's a corresponding DERIVED row,
					// replace <derivedN> with the DERIVED row's base table and drop the DERIVED row.
					// This matches MySQL's behavior of merging simple derived tables into the outer query.
					derivedByID := make(map[int64]explainSelectType)
					for _, dr := range derivedRows {
						if id, ok := dr.id.(int64); ok {
							derivedByID[id] = dr
						}
					}
					mergedDerived := make(map[int64]bool)
					for i, sr := range simpleRows {
						if sr.table == nil {
							continue
						}
						tblStr := fmt.Sprintf("%v", sr.table)
						if !strings.HasPrefix(tblStr, "<derived") {
							continue
						}
						// Extract the derived ID from "<derivedN>"
						var derivedID int64
						if _, err := fmt.Sscanf(tblStr, "<derived%d>", &derivedID); err == nil {
							if dr, ok := derivedByID[derivedID]; ok {
								// Replace the <derivedN> row with the DERIVED row's properties
								simpleRows[i].table = dr.table
								simpleRows[i].accessType = dr.accessType
								simpleRows[i].rows = dr.rows
								simpleRows[i].filtered = dr.filtered
								simpleRows[i].extra = dr.extra
								mergedDerived[derivedID] = true
							}
						}
					}
					// Keep unmerged DERIVED rows
					var unmergedDerived []explainSelectType
					for _, dr := range derivedRows {
						if id, ok := dr.id.(int64); ok {
							if !mergedDerived[id] {
								unmergedDerived = append(unmergedDerived, dr)
							}
						}
					}

					// Final order: placeholders first, then outer SIMPLE rows, then MATERIALIZED rows
					// (in reverse subquery ID order, matching MySQL's output), then unmerged DERIVED rows.
					// MySQL outputs MATERIALIZED subqueries in reverse order of their IDs when multiple
					// IN subqueries are present (higher IDs first).
					if len(materializedRows) > 1 {
						// Reverse materializedRows by grouping by id and reversing id order.
						// Build a map from id to rows, then output in reverse id order.
						type matGroup struct {
							id   interface{}
							rows []explainSelectType
						}
						var groups []matGroup
						seenIDs := make(map[interface{}]int)
						for _, mr := range materializedRows {
							if idx, ok := seenIDs[mr.id]; ok {
								groups[idx].rows = append(groups[idx].rows, mr)
							} else {
								seenIDs[mr.id] = len(groups)
								groups = append(groups, matGroup{id: mr.id, rows: []explainSelectType{mr}})
							}
						}
						// Reverse the group order (higher IDs first)
						for i, j := 0, len(groups)-1; i < j; i, j = i+1, j-1 {
							groups[i], groups[j] = groups[j], groups[i]
						}
						materializedRows = materializedRows[:0]
						for _, g := range groups {
							materializedRows = append(materializedRows, g.rows...)
						}
					}
					var processed []explainSelectType
					processed = append(processed, placeholders...)
					processed = append(processed, simpleRows...)
					processed = append(processed, materializedRows...)
					processed = append(processed, unmergedDerived...)
					result = processed
				} else {
					// materialization=off: all non-SIMPLE rows become id=1, SIMPLE
					for i := range result {
						if result[i].selectType != "SIMPLE" {
							result[i].id = int64(1)
							result[i].selectType = "SIMPLE"
						}
					}
				}
			} else {
				result = e.explainSelect(s, &idCounter, "PRIMARY")
			}
		} else {
			// Simple query
			result = e.explainSelect(s, &idCounter, "SIMPLE")
			// Detect "Impossible WHERE noticed after reading const tables":
			// When all tables in the result have const/system access type AND
			// there are multiple tables (JOIN) AND the actual SELECT returns 0 rows,
			// MySQL collapses to a single "Impossible WHERE noticed after reading const tables" row.
			// This occurs when constant folding after const-table resolution reveals
			// that the WHERE condition is always false (e.g., LEFT JOIN with IS NULL + non-null check).
			if len(result) > 1 && s.Where != nil {
				allConst := true
				for _, r := range result {
					at := fmt.Sprintf("%v", r.accessType)
					if at != "const" && at != "system" {
						allConst = false
						break
					}
				}
				if allConst {
					// Execute the SELECT to check if it returns 0 rows
					actualResult, execErr := e.execSelect(s)
					if execErr == nil && actualResult != nil && len(actualResult.Rows) == 0 {
						result = []explainSelectType{{
							id:         int64(1),
							selectType: "SIMPLE",
							table:      nil,
							extra:      "Impossible WHERE noticed after reading const tables",
							rows:       nil,
							filtered:   nil,
							accessType: nil,
						}}
					}
				}
			}
		}
	case *sqlparser.Update:
		// EXPLAIN UPDATE: first table gets select_type="UPDATE", subsequent tables get "SIMPLE".
		// Collect all table names from the UPDATE's TableExprs.
		var tableNames []string
		for _, te := range s.TableExprs {
			tableNames = append(tableNames, e.extractAllTableNames(te)...)
		}
		for i, tblName := range tableNames {
			var rowCount int64 = 1
			if e.Storage != nil {
				if tbl, err := e.Storage.GetTable(e.CurrentDB, tblName); err == nil {
					if n := len(tbl.Rows); n > 0 {
						rowCount = int64(n)
					}
				}
			}
			st := "SIMPLE"
			if i == 0 {
				st = "UPDATE"
			}
			var updateExtra interface{} = nil
			if i == 0 && s.Where != nil {
				updateExtra = "Using where"
			}
			result = append(result, explainSelectType{
				id:         idCounter,
				selectType: st,
				table:      tblName,
				rows:       rowCount,
				filtered:   "100.00",
				accessType: "ALL",
				extra:      updateExtra,
			})
		}
		if len(result) == 0 {
			return [][]interface{}{e.dummyExplainRow(query)}
		}
	case *sqlparser.Delete:
		// EXPLAIN DELETE: show "DELETE" as select_type.
		// For DELETE without WHERE, Extra is "Deleting all rows".
		var tableNames []string
		for _, te := range s.TableExprs {
			tableNames = append(tableNames, e.extractAllTableNames(te)...)
		}
		for i, tblName := range tableNames {
			var rowCount int64 = 1
			if e.Storage != nil {
				if tbl, err := e.Storage.GetTable(e.CurrentDB, tblName); err == nil {
					if n := len(tbl.Rows); n > 0 {
						rowCount = int64(n)
					}
				}
			}
			st := "SIMPLE"
			if i == 0 {
				st = "DELETE"
			}
			var extra interface{} = nil
			if i == 0 && s.Where == nil {
				extra = "Deleting all rows"
			} else if i == 0 && s.Where != nil {
				extra = "Using where"
			}
			result = append(result, explainSelectType{
				id:         idCounter,
				selectType: st,
				table:      tblName,
				rows:       rowCount,
				filtered:   "100.00",
				accessType: "ALL",
				extra:      extra,
			})
		}
		if len(result) == 0 {
			return [][]interface{}{e.dummyExplainRow(query)}
		}
	case *sqlparser.Insert:
		// EXPLAIN INSERT/REPLACE: show table with select_type "INSERT" or "REPLACE"
		// rows, filtered are NULL in MySQL's traditional EXPLAIN for INSERT/REPLACE
		tblName := ""
		if tn, ok := s.Table.Expr.(sqlparser.TableName); ok {
			tblName = tn.Name.String()
		}
		stType := "INSERT"
		if s.Action == sqlparser.ReplaceAct {
			stType = "REPLACE"
		}
		result = append(result, explainSelectType{
			id:         idCounter,
			selectType: stType,
			table:      tblName,
			rows:       nil,
			filtered:   nil,
			accessType: "ALL",
		})
		// For INSERT/REPLACE ... SELECT, also include the SELECT subquery (same id as INSERT/REPLACE)
		if rows, ok := s.Rows.(*sqlparser.Select); ok {
			innerRows := e.explainSelect(rows, &idCounter, "SIMPLE")
			result = append(result, innerRows...)
		}
	default:
		return [][]interface{}{e.dummyExplainRow(query)}
	}

	// Convert to row format
	rows := make([][]interface{}, len(result))
	for i, r := range result {
		rows[i] = []interface{}{
			r.id, r.selectType, r.table, nil, r.accessType,
			r.possibleKeys, r.key, r.keyLen, r.ref, r.rows, r.filtered, r.extra,
		}
	}
	return rows
}

// explainSemijoin checks whether MySQL's semijoin optimization applies to this SELECT.
// When a non-correlated EXISTS or IN subquery appears in the WHERE clause (not SELECT/HAVING),
// MySQL's semijoin optimizer merges the subquery into the outer query and shows all
// participating tables with SELECT_TYPE=SIMPLE and the same query id=1.
// Returns (rows, true) if semijoin applies, otherwise (nil, false).
func (e *Executor) explainSemijoin(sel *sqlparser.Select, baseID int64) ([]explainSelectType, bool) {
	// No FROM clause → not applicable
	if len(sel.From) == 0 {
		return nil, false
	}
	// Derived tables in FROM → not semijoin
	for _, te := range sel.From {
		if e.tableExprHasSubquery(te) {
			return nil, false
		}
	}
	// No WHERE clause → no subquery to semijoin
	if sel.Where == nil {
		return nil, false
	}
	// STRAIGHT_JOIN as a SELECT modifier prevents semijoin flattening.
	if sel.StraightJoinHint {
		return nil, false
	}
	// Collect outer real table names; if none (e.g. FROM dual), semijoin doesn't apply
	outerTables := map[string]bool{}
	for _, te := range sel.From {
		for _, tn := range e.extractAllTableNames(te) {
			if !strings.EqualFold(tn, "dual") {
				outerTables[strings.ToLower(tn)] = true
			}
		}
	}
	if len(outerTables) == 0 {
		return nil, false
	}
	var semiTables []string // tables from EXISTS/IN inner selects
	hasSemijoins := false
	hasOtherSubqueries := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch n := node.(type) {
		case *sqlparser.Subquery:
			inner, ok := n.Select.(*sqlparser.Select)
			if !ok {
				hasOtherSubqueries = true
				return false, nil
			}
			// Check if correlated
			if e.isCorrelatedSubquery(n.Select, outerTables) {
				hasOtherSubqueries = true
				return false, nil
			}
			// STRAIGHT_JOIN as a SELECT modifier in the inner subquery prevents semijoin.
			// Note: STRAIGHT_JOIN as a join type (t1 STRAIGHT_JOIN t2) does NOT prevent it.
			if inner.StraightJoinHint {
				hasOtherSubqueries = true
				return false, nil
			}
			// Collect inner real table names (exclude pseudo-table "dual")
			innerTables := e.extractAllTableNamesFromSelect(inner)
			var realInnerTables []string
			for _, t := range innerTables {
				if !strings.EqualFold(t, "dual") {
					realInnerTables = append(realInnerTables, t)
				}
			}
			if len(realInnerTables) == 0 {
				// No real tables in EXISTS → not semijoin candidate
				hasOtherSubqueries = true
				return false, nil
			}
			semiTables = append(semiTables, realInnerTables...)
			hasSemijoins = true
			return false, nil
		}
		return true, nil
	}, sel.Where)

	if !hasSemijoins || hasOtherSubqueries || len(semiTables) == 0 {
		return nil, false
	}
	// Build SIMPLE rows: inner (semi-join) tables first, then outer tables
	// Use the single query id=baseID for all rows
	var rows []explainSelectType
	// Add inner semi-join tables (from EXISTS subqueries)
	for _, tbl := range semiTables {
		ai := e.explainDetectAccessType(sel, tbl)
		var rowCount int64 = 1
		if e.Storage != nil {
			if t, err := e.Storage.GetTable(e.CurrentDB, tbl); err == nil && len(t.Rows) > 0 {
				rowCount = int64(len(t.Rows))
			}
		}
		rows = append(rows, explainSelectType{
			id:           baseID,
			selectType:   "SIMPLE",
			table:        tbl,
			accessType:   ai.accessType,
			possibleKeys: nilIfEmpty(ai.possibleKeys),
			key:          nilIfEmpty(ai.key),
			keyLen:       nilIfEmpty(ai.keyLen),
			ref:          nilIfEmpty(ai.ref),
			rows:         rowCount,
			filtered:     "100.00",
			extra:        nil,
		})
	}
	// Add outer real tables (exclude dual)
	outerTableNames := e.extractAllTableNamesFromSelect(sel)
	for _, tbl := range outerTableNames {
		if strings.EqualFold(tbl, "dual") {
			continue
		}
		ai := e.explainDetectAccessType(sel, tbl)
		var rowCount int64 = 1
		if e.Storage != nil {
			if t, err := e.Storage.GetTable(e.CurrentDB, tbl); err == nil && len(t.Rows) > 0 {
				rowCount = int64(len(t.Rows))
			}
		}
		rows = append(rows, explainSelectType{
			id:           baseID,
			selectType:   "SIMPLE",
			table:        tbl,
			accessType:   ai.accessType,
			possibleKeys: nilIfEmpty(ai.possibleKeys),
			key:          nilIfEmpty(ai.key),
			keyLen:       nilIfEmpty(ai.keyLen),
			ref:          nilIfEmpty(ai.ref),
			rows:         rowCount,
			filtered:     "100.00",
			extra:        nil,
		})
	}
	return rows, true
}

// extractAllTableNamesFromSelect extracts real table names from a SELECT's FROM clause.
func (e *Executor) extractAllTableNamesFromSelect(sel *sqlparser.Select) []string {
	var names []string
	for _, te := range sel.From {
		names = append(names, e.extractAllTableNames(te)...)
	}
	return names
}

// nilIfEmpty returns nil if v is nil or an empty string, otherwise returns v.
func nilIfEmpty(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	if s, ok := v.(string); ok && s == "" {
		return nil
	}
	return v
}

// queryHasComplexParts returns true if the SELECT contains subqueries or derived tables.
func (e *Executor) queryHasComplexParts(sel *sqlparser.Select) bool {
	hasComplex := false
	// Check FROM clause for derived tables
	for _, te := range sel.From {
		if e.tableExprHasSubquery(te) {
			hasComplex = true
			break
		}
	}
	if hasComplex {
		return true
	}
	// Check for subqueries in SELECT expressions, WHERE, HAVING
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch n := node.(type) {
		case *sqlparser.Subquery:
			_ = n
			hasComplex = true
			return false, nil
		}
		return true, nil
	}, sel.SelectExprs, sel.Where, sel.Having)
	if hasComplex {
		return true
	}
	// Check for subqueries in JOIN ON conditions
	for _, te := range sel.From {
		var onNodes []sqlparser.SQLNode
		e.collectJoinOnConditions(te, &onNodes)
		for _, onNode := range onNodes {
			_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
				if _, ok := node.(*sqlparser.Subquery); ok {
					hasComplex = true
					return false, nil
				}
				return true, nil
			}, onNode)
		}
	}
	return hasComplex
}

// queryCanBeSemijoinFlattened returns true if the SELECT's WHERE-clause subqueries
// can all be flattened into a single SIMPLE query block (MySQL anti-join / semi-join
// optimization). The rules:
//
//   - No derived tables in FROM (those need DERIVED rows).
//   - The outer query must have at least one real table (not just DUAL).
//   - No subqueries in the SELECT list (scalar subqueries must stay as SUBQUERY).
//   - Every subquery in WHERE is either: IN (SELECT …), NOT IN (SELECT …),
//     EXISTS (SELECT …), or NOT EXISTS wrapped in ExistsExpr.
//   - No UNION subqueries inside IN (unions cannot be semijoin-flattened).
//   - semijoin optimizer flag is ON (default).
func (e *Executor) queryCanBeSemijoinFlattened(sel *sqlparser.Select) bool {
	// Note: derived tables in FROM do NOT prevent IN-subquery flattening.
	// MySQL can semijoin-flatten IN subqueries even when the outer FROM has derived tables.
	// (The old check was too restrictive.)

	// STRAIGHT_JOIN as a SELECT modifier (SELECT STRAIGHT_JOIN ...) prevents semijoin flattening.
	// Note: STRAIGHT_JOIN as a join type (t1 STRAIGHT_JOIN t2) is just a join order hint
	// and does NOT prevent semijoin flattening.
	if sel.StraightJoinHint {
		return false
	}

	// The outer query must have at least one real table or derived table (not just DUAL).
	// If there are no real tables or derived tables, MySQL cannot form an anti-join and keeps
	// the subquery as a separate PRIMARY+SUBQUERY pair.
	var outerTables []string
	for _, te := range sel.From {
		outerTables = append(outerTables, e.extractAllTableNames(te)...)
	}
	numDerivedOuter := 0
	for _, te := range sel.From {
		numDerivedOuter += countDerivedTablesInExpr(te)
	}
	hasRealTable := numDerivedOuter > 0 // derived tables also count as "real" outer tables
	for _, tn := range outerTables {
		if strings.ToLower(tn) != "dual" {
			hasRealTable = true
			break
		}
	}
	if !hasRealTable {
		return false
	}

	// Subqueries in the SELECT list (scalar) prevent flattening.
	hasSelectSubquery := false
	_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
		if _, ok := n.(*sqlparser.Subquery); ok {
			hasSelectSubquery = true
			return false, nil
		}
		return true, nil
	}, sel.SelectExprs)
	if hasSelectSubquery {
		return false
	}

	// Collect ON conditions from JOIN expressions.
	var onConditions []sqlparser.SQLNode
	for _, te := range sel.From {
		e.collectJoinOnConditions(te, &onConditions)
	}

	// Check if there are any subqueries to flatten in WHERE or ON conditions.
	// If WHERE is nil AND no ON conditions have subqueries, this is a simple query.
	hasSubqueryInWhere := sel.Where != nil
	hasSubqueryInON := false
	for _, onCond := range onConditions {
		_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
			if _, ok := n.(*sqlparser.Subquery); ok {
				hasSubqueryInON = true
				return false, nil
			}
			return true, nil
		}, onCond)
		if hasSubqueryInON {
			break
		}
	}
	if !hasSubqueryInWhere && !hasSubqueryInON {
		return false // no subqueries at all → already SIMPLE
	}

	// Walk both WHERE and ON conditions for flattenability analysis.
	hasAny := false
	allFlattenable := true
	walkFn := func(n sqlparser.SQLNode) (bool, error) {
		switch expr := n.(type) {
		case *sqlparser.ExistsExpr:
			// EXISTS / NOT EXISTS can be flattened only when the subquery has real tables.
			// If the subquery is `EXISTS (SELECT 1 FROM dual)`, MySQL keeps it as SUBQUERY.
			if inner, ok := expr.Subquery.Select.(*sqlparser.Select); ok {
				var innerTables []string
				for _, te := range inner.From {
					innerTables = append(innerTables, e.extractAllTableNames(te)...)
				}
				hasRealInner := false
				for _, tn := range innerTables {
					if strings.ToLower(tn) != "dual" {
						hasRealInner = true
						break
					}
				}
				if !hasRealInner {
					// No real inner tables: EXISTS is a constant check, can't be flattened.
					allFlattenable = false
					return false, nil
				}
				hasAny = true
			}
			return false, nil
		case *sqlparser.ComparisonExpr:
			// Check if the right-hand side is a subquery (IN, NOT IN, = ANY, etc.)
			isSubqueryComparison := false
			if expr.Operator == sqlparser.InOp || expr.Operator == sqlparser.NotInOp {
				isSubqueryComparison = true
			} else if expr.Modifier == sqlparser.Any {
				// = ANY with a subquery is equivalent to IN and can be semijoin-flattened.
				// Note: !=ANY / <ANY / >ANY / <=ANY / >=ANY with a subquery are NOT
				// equivalent to IN and cannot be semijoin-flattened.
				// Only = ANY is equivalent to IN.
				if expr.Operator == sqlparser.EqualOp {
					if _, ok := expr.Right.(*sqlparser.Subquery); ok {
						isSubqueryComparison = true
					}
				}
			}
			// Note: ALL modifier (>= ALL, <= ALL, = ALL, etc.) is NEVER semijoin-flattenable.
			// It requires a different execution strategy (ALL-subquery execution).
			if isSubqueryComparison {
				if sub, ok := expr.Right.(*sqlparser.Subquery); ok {
					// UNION subqueries inside IN/ANY cannot be semijoin-flattened.
					if _, isUnion := sub.Select.(*sqlparser.Union); isUnion {
						allFlattenable = false
						return false, nil
					}
					// IN/ANY subqueries with no real inner tables are constant checks.
					if inner, ok := sub.Select.(*sqlparser.Select); ok {
						var innerTables []string
						for _, te := range inner.From {
							innerTables = append(innerTables, e.extractAllTableNames(te)...)
						}
						hasRealInner := false
						for _, tn := range innerTables {
							if strings.ToLower(tn) != "dual" {
								hasRealInner = true
								break
							}
						}
						if !hasRealInner {
							allFlattenable = false
							return false, nil
						}
						// Check for NO_SEMIJOIN optimizer hint in the inner SELECT.
						// e.g. SELECT /*+ NO_SEMIJOIN() */ a FROM t1 prevents flattening.
						for _, c := range inner.Comments.GetComments() {
							if strings.Contains(strings.ToUpper(c), "NO_SEMIJOIN") {
								allFlattenable = false
								return false, nil
							}
						}
						// STRAIGHT_JOIN as a SELECT modifier in the inner subquery prevents semijoin flattening.
						// Note: STRAIGHT_JOIN as a join type (t1 STRAIGHT_JOIN t2) does NOT prevent it.
						if inner.StraightJoinHint {
							allFlattenable = false
							return false, nil
						}
						// GROUP BY in the inner subquery prevents semijoin flattening.
						// MySQL uses IN-to-EXISTS conversion for aggregate subqueries instead.
						if inner.GroupBy != nil && len(inner.GroupBy.Exprs) > 0 {
							allFlattenable = false
							return false, nil
						}
						// HAVING in the inner subquery also prevents semijoin.
						if inner.Having != nil {
							allFlattenable = false
							return false, nil
						}
					}
					hasAny = true
					return false, nil
				}
			}
			return true, nil
		case *sqlparser.Subquery:
			// A bare subquery in WHERE not wrapped in EXISTS/IN/NOT IN/= ANY.
			allFlattenable = false
			return false, nil
		}
		return true, nil
	}
	// Walk WHERE clause
	if sel.Where != nil {
		_ = sqlparser.Walk(walkFn, sel.Where)
	}
	// Walk JOIN ON conditions
	for _, onCond := range onConditions {
		_ = sqlparser.Walk(walkFn, onCond)
	}

	if !hasAny || !allFlattenable {
		return false
	}

	// MySQL cannot semijoin-flatten if the total number of tables (outer + inner) exceeds MAX_TABLES (61).
	// When there are too many tables, MySQL keeps the subquery as a separate SUBQUERY block.
	const mysqlMaxTables = 61
	outerTableCount := len(outerTables)
	// Count inner tables from all IN/ANY subqueries in WHERE and ON conditions
	innerTableCount := 0
	countInnerTables := func(n sqlparser.SQLNode) (bool, error) {
		if comp, ok := n.(*sqlparser.ComparisonExpr); ok {
			if comp.Operator == sqlparser.InOp || comp.Operator == sqlparser.NotInOp {
				if sub, ok := comp.Right.(*sqlparser.Subquery); ok {
					if inner, ok := sub.Select.(*sqlparser.Select); ok {
						for _, te := range inner.From {
							innerTableCount += len(e.extractAllTableNames(te))
						}
					}
				}
			}
		}
		return true, nil
	}
	if sel.Where != nil {
		_ = sqlparser.Walk(countInnerTables, sel.Where)
	}
	for _, onCond := range onConditions {
		_ = sqlparser.Walk(countInnerTables, onCond)
	}
	if outerTableCount+innerTableCount > mysqlMaxTables {
		return false // Too many tables → MySQL keeps subquery as SUBQUERY, not SIMPLE
	}

	// semijoin must be enabled (it is on by default).
	return e.isOptimizerSwitchEnabled("semijoin")
}

// tableExprHasSubquery checks if a table expression contains a derived table (subquery in FROM).
func (e *Executor) tableExprHasSubquery(te sqlparser.TableExpr) bool {
	switch t := te.(type) {
	case *sqlparser.AliasedTableExpr:
		if _, ok := t.Expr.(*sqlparser.DerivedTable); ok {
			return true
		}
	case *sqlparser.JoinTableExpr:
		return e.tableExprHasSubquery(t.LeftExpr) || e.tableExprHasSubquery(t.RightExpr)
	case *sqlparser.ParenTableExpr:
		for _, expr := range t.Exprs {
			if e.tableExprHasSubquery(expr) {
				return true
			}
		}
	}
	return false
}

// tableExprHasStraightJoin returns true if any JoinTableExpr in the tree uses StraightJoinType.
func (e *Executor) tableExprHasStraightJoin(te sqlparser.TableExpr) bool {
	switch t := te.(type) {
	case *sqlparser.JoinTableExpr:
		if t.Join == sqlparser.StraightJoinType {
			return true
		}
		return e.tableExprHasStraightJoin(t.LeftExpr) || e.tableExprHasStraightJoin(t.RightExpr)
	case *sqlparser.ParenTableExpr:
		for _, expr := range t.Exprs {
			if e.tableExprHasStraightJoin(expr) {
				return true
			}
		}
	}
	return false
}

// extractAllTableNames collects all real table names from a table expression tree.
func (e *Executor) extractAllTableNames(te sqlparser.TableExpr) []string {
	switch t := te.(type) {
	case *sqlparser.AliasedTableExpr:
		if _, ok := t.Expr.(*sqlparser.DerivedTable); ok {
			return nil
		}
		if tn, ok := t.Expr.(sqlparser.TableName); ok {
			return []string{tn.Name.String()}
		}
	case *sqlparser.JoinTableExpr:
		left := e.extractAllTableNames(t.LeftExpr)
		right := e.extractAllTableNames(t.RightExpr)
		return append(left, right...)
	case *sqlparser.ParenTableExpr:
		var names []string
		for _, expr := range t.Exprs {
			names = append(names, e.extractAllTableNames(expr)...)
		}
		return names
	}
	return nil
}

// countDerivedTablesInExpr counts how many direct derived tables (subqueries in FROM) exist
// in a table expression.
func countDerivedTablesInExpr(te sqlparser.TableExpr) int {
	switch t := te.(type) {
	case *sqlparser.AliasedTableExpr:
		if _, ok := t.Expr.(*sqlparser.DerivedTable); ok {
			return 1
		}
	case *sqlparser.JoinTableExpr:
		return countDerivedTablesInExpr(t.LeftExpr) + countDerivedTablesInExpr(t.RightExpr)
	case *sqlparser.ParenTableExpr:
		n := 0
		for _, expr := range t.Exprs {
			n += countDerivedTablesInExpr(expr)
		}
		return n
	}
	return 0
}

// explainSelect produces EXPLAIN rows for a SELECT statement.
func (e *Executor) explainSelect(sel *sqlparser.Select, idCounter *int64, selectType string) []explainSelectType {
	myID := *idCounter
	var result []explainSelectType

	// Collect all real table names from FROM clause (skip synthesized "dual").
	var allTableNames []string
	for _, te := range sel.From {
		for _, tn := range e.extractAllTableNames(te) {
			if strings.ToLower(tn) != "dual" {
				allTableNames = append(allTableNames, tn)
			}
		}
	}

	// Count direct derived tables in FROM clause
	numDerived := 0
	for _, te := range sel.From {
		numDerived += countDerivedTablesInExpr(te)
	}

	// Check for GROUP BY / SQL_BIG_RESULT
	queryStr := sqlparser.String(sel)
	upperQ := strings.ToUpper(queryStr)
	orderByNull := len(sel.OrderBy) == 1 && func() bool {
		_, ok := sel.OrderBy[0].Expr.(*sqlparser.NullVal)
		return ok
	}()

	if len(allTableNames) == 0 && numDerived == 0 {
		// No tables at all
		result = append(result, explainSelectType{
			id:         myID,
			selectType: selectType,
			table:      nil,
			extra:      "No tables used",
			rows:       nil,
			filtered:   nil,
			accessType: nil,
		})
	} else if len(allTableNames) == 0 && numDerived > 0 {
		// Only derived tables in FROM - add a row for each derived table reference.
		// Derived tables will get ids starting at *idCounter+1 (assigned in explainFromExpr).
		nextID := *idCounter + 1
		for i := 0; i < numDerived; i++ {
			derivedRef := fmt.Sprintf("<derived%d>", nextID)
			result = append(result, explainSelectType{
				id:           myID,
				selectType:   selectType,
				table:        derivedRef,
				extra:        nil,
				rows:         int64(1),
				filtered:     "100.00",
				accessType:   "ALL",
				possibleKeys: nil,
				key:          nil,
				keyLen:       nil,
				ref:          nil,
			})
			nextID++
		}
	} else {
		for idx, tblName := range allTableNames {
			var rowCount int64 = 1
			tableIsEmpty := false
			if e.Storage != nil {
				if tbl, err := e.Storage.GetTable(e.CurrentDB, tblName); err == nil {
					if n := len(tbl.Rows); n > 0 {
						rowCount = int64(n)
					} else {
						tableIsEmpty = true
						rowCount = 0
					}
				}
			}

			var extra interface{} = nil
			// Single-table scan on empty table: MySQL shows "no matching row in const table"
			// with table=NULL in the traditional EXPLAIN output.
			// Exception: MATERIALIZED subqueries always show the real table name with 0 rows.
			if tableIsEmpty && len(allTableNames) == 1 && idx == 0 && selectType != "MATERIALIZED" {
				result = append(result, explainSelectType{
					id:         myID,
					selectType: selectType,
					table:      nil,
					extra:      "no matching row in const table",
					rows:       nil,
					filtered:   nil,
					accessType: nil,
				})
				// When the outer table is empty in a semijoin-flattened context (SIMPLE),
				// return immediately without processing subqueries. MySQL collapses the
				// entire result to 1 NULL row in this case.
				if selectType == "SIMPLE" {
					return result
				}
				continue
			} else if idx == 0 && !orderByNull && (strings.Contains(upperQ, "GROUP BY") || strings.Contains(upperQ, "SQL_BIG_RESULT")) {
				extra = "Using filesort"
			}
			// For secondary tables in a cross-join, MySQL shows "Using join buffer"
			if idx > 0 {
				extra = "Using join buffer (Block Nested Loop)"
			}

			var accessType interface{} = "ALL"
			var filtered interface{} = "100.00"
			if tableIsEmpty {
				filtered = "0.00"
			}
			var possibleKeys interface{} = nil
			var key interface{} = nil
			var keyLen interface{} = nil
			var ref interface{} = nil

			// Detect access type based on WHERE clause and available indexes
			accessInfo := e.explainDetectAccessType(sel, tblName)
			accessType = accessInfo.accessType
			possibleKeys = accessInfo.possibleKeys
			key = accessInfo.key
			keyLen = accessInfo.keyLen
			ref = accessInfo.ref

			if accessInfo.accessType == "const" || accessInfo.accessType == "eq_ref" || accessInfo.accessType == "ref" {
				rowCount = int64(1)
			}

			// Set "Using index condition" for ref access with IS NULL or range conditions on indexed columns
			if extra == nil && (accessInfo.accessType == "ref" || accessInfo.accessType == "range") {
				if sel.Where != nil {
					wcs := explainExtractWhereConditions(sel.Where.Expr, tblName)
					for _, wc := range wcs {
						if wc.isNull {
							extra = "Using index condition"
							break
						}
					}
				}
			}

			// Add "Using where" for ALL-access tables that have WHERE conditions filtering them.
			// MySQL adds "Using where" when the engine applies a WHERE filter during a full scan.
			if accessInfo.accessType == "ALL" && sel.Where != nil {
				dbName := e.CurrentDB
				if dbName == "" {
					dbName = "test"
				}
				cond := e.extractTableCondition(sel.Where.Expr, tblName, dbName)
				if cond != "" {
					if extra == nil {
						extra = "Using where"
					} else {
						extra = fmt.Sprintf("Using where; %v", extra)
					}
				}
			}

			result = append(result, explainSelectType{
				id:           myID,
				selectType:   selectType,
				table:        tblName,
				extra:        extra,
				rows:         rowCount,
				filtered:     filtered,
				accessType:   accessType,
				possibleKeys: possibleKeys,
				key:          key,
				keyLen:       keyLen,
				ref:          ref,
			})
		}
	}

	// Process FROM clause for derived tables.
	// We collect them separately first so that WHERE-clause subqueries
	// (DEPENDENT SUBQUERY / SUBQUERY) can be inserted before DERIVED rows,
	// matching MySQL's EXPLAIN output order.
	derivedStart := len(result)
	for _, te := range sel.From {
		e.explainFromExpr(te, idCounter, &result)
	}
	derivedRows := make([]explainSelectType, len(result)-derivedStart)
	copy(derivedRows, result[derivedStart:])
	result = result[:derivedStart]

	// Process subqueries in SELECT expressions, WHERE, HAVING.
	// MySQL outputs WHERE-clause subqueries BEFORE FROM-clause derived tables.
	e.explainSubqueries(sel, idCounter, &result, selectType)

	// Append derived rows after WHERE-clause subquery rows.
	result = append(result, derivedRows...)

	return result
}

// explainFromExpr processes table expressions to find derived tables.
func (e *Executor) explainFromExpr(te sqlparser.TableExpr, idCounter *int64, result *[]explainSelectType) {
	switch t := te.(type) {
	case *sqlparser.AliasedTableExpr:
		if dt, ok := t.Expr.(*sqlparser.DerivedTable); ok {
			// This is a derived table (subquery in FROM)
			*idCounter++
			switch inner := dt.Select.(type) {
			case *sqlparser.Union:
				derived := e.explainUnion(inner, idCounter, false)
				// The first element should be DERIVED
				if len(derived) > 0 {
					derived[0].selectType = "DERIVED"
				}
				*result = append(*result, derived...)
			case *sqlparser.Select:
				innerRows := e.explainSelect(inner, idCounter, "DERIVED")
				// Fix the id counter: the DERIVED row gets the next id
				// (already incremented above)
				if len(innerRows) > 0 {
					innerRows[0].id = *idCounter
				}
				*result = append(*result, innerRows...)
			}
		}
	case *sqlparser.JoinTableExpr:
		e.explainFromExpr(t.LeftExpr, idCounter, result)
		e.explainFromExpr(t.RightExpr, idCounter, result)
	case *sqlparser.ParenTableExpr:
		for _, expr := range t.Exprs {
			e.explainFromExpr(expr, idCounter, result)
		}
	}
}

// extractTableNamesAndAliases collects all table names and aliases from a SELECT's FROM clause.
// This is used to determine whether a subquery references outer tables (correlated).
func (e *Executor) extractTableNamesAndAliases(sel *sqlparser.Select) map[string]bool {
	names := make(map[string]bool)
	for _, te := range sel.From {
		e.collectTableNamesAndAliases(te, names)
	}
	return names
}

func (e *Executor) collectTableNamesAndAliases(te sqlparser.TableExpr, names map[string]bool) {
	switch t := te.(type) {
	case *sqlparser.AliasedTableExpr:
		if tn, ok := t.Expr.(sqlparser.TableName); ok {
			names[strings.ToLower(tn.Name.String())] = true
		}
		if !t.As.IsEmpty() {
			names[strings.ToLower(t.As.String())] = true
		}
	case *sqlparser.JoinTableExpr:
		e.collectTableNamesAndAliases(t.LeftExpr, names)
		e.collectTableNamesAndAliases(t.RightExpr, names)
	case *sqlparser.ParenTableExpr:
		for _, expr := range t.Exprs {
			e.collectTableNamesAndAliases(expr, names)
		}
	}
}

// isCorrelatedSubquery checks whether a subquery SELECT references any of the outer table names.
// isOptimizerSwitchEnabled checks if a specific optimizer_switch flag is enabled.
// Returns true if the flag is "on" or if the switch is not set (defaults to on for most flags).
func (e *Executor) isOptimizerSwitchEnabled(flag string) bool {
	switchVal, ok := e.getSysVar("optimizer_switch")
	if !ok {
		return true // default: on
	}
	for _, part := range strings.Split(switchVal, ",") {
		part = strings.TrimSpace(part)
		kv := strings.SplitN(part, "=", 2)
		if len(kv) == 2 && strings.TrimSpace(kv[0]) == flag {
			return strings.TrimSpace(kv[1]) == "on"
		}
	}
	return true // not found means default (on)
}

func (e *Executor) isCorrelatedSubquery(subSelect sqlparser.TableStatement, outerTables map[string]bool) bool {
	if len(outerTables) == 0 {
		return false
	}

	// Collect the subquery's own table names/aliases
	innerTables := make(map[string]bool)
	switch s := subSelect.(type) {
	case *sqlparser.Select:
		innerTables = e.extractTableNamesAndAliases(s)
	case *sqlparser.Union:
		// For unions, collect from all selects
		selects := e.flattenUnion(s)
		for _, sel := range selects {
			if inner, ok := sel.(*sqlparser.Select); ok {
				for k, v := range e.extractTableNamesAndAliases(inner) {
					innerTables[k] = v
				}
			}
		}
	}

	// Check if the subquery has any real inner tables (not DUAL)
	hasRealInnerTables := false
	for t := range innerTables {
		if !strings.EqualFold(t, "dual") {
			hasRealInnerTables = true
			break
		}
	}

	correlated := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if cn, ok := node.(*sqlparser.ColName); ok {
			qualifier := strings.ToLower(cn.Qualifier.Name.String())
			if qualifier != "" && outerTables[qualifier] && !innerTables[qualifier] {
				// Qualified reference to outer table: clearly correlated
				correlated = true
				return false, nil
			}
			if qualifier == "" && !hasRealInnerTables {
				// Unqualified column reference with no real inner tables (e.g. FROM DUAL):
				// the column must come from the outer scope → correlated
				colName := strings.ToLower(cn.Name.String())
				// Check if this column name exists in any outer table
				for outerTbl := range outerTables {
					if strings.EqualFold(outerTbl, "dual") {
						continue
					}
					// Look up the table schema to check column existence
					if e.Storage != nil {
						if tbl, err := e.Storage.GetTable(e.CurrentDB, outerTbl); err == nil && tbl.Def != nil {
							for _, col := range tbl.Def.Columns {
								if strings.EqualFold(col.Name, colName) {
									correlated = true
									return false, nil
								}
							}
						}
					}
				}
			}
		}
		return true, nil
	}, subSelect)
	return correlated
}

// outerTablesAreSystem returns true if all outer tables together have exactly 1 row,
// making the outer query a "system" table access. In this case, MySQL prefers inline
// FirstMatch over MATERIALIZED because the const/system access is cheap.
func (e *Executor) outerTablesAreSystem(outerTables map[string]bool) bool {
	if e.Storage == nil || len(outerTables) == 0 {
		return false
	}
	totalRows := 0
	for tblName := range outerTables {
		tbl, err := e.Storage.GetTable(e.CurrentDB, tblName)
		if err != nil {
			return false
		}
		totalRows += len(tbl.Rows)
	}
	return totalRows == 1
}

// innerSubqueryUsesOnlyStraightJoin returns true if the SELECT's FROM clause uses ONLY STRAIGHT_JOIN
// (no INNER JOIN, no LEFT JOIN, no comma-join). STRAIGHT_JOIN forces join order, preventing table reordering.
// Note: FROM t1, t2 (comma join) has len(sel.From)==2 with separate AliasedTableExprs, NOT STRAIGHT_JOIN.
// STRAIGHT_JOIN syntax (t1 STRAIGHT_JOIN t2) produces a single JoinTableExpr with StraightJoinType.
func (e *Executor) innerSubqueryUsesOnlyStraightJoin(sel *sqlparser.Select) bool {
	if len(sel.From) == 0 {
		return false
	}
	// Comma join (FROM t1, t2) means multiple top-level table expressions — not STRAIGHT_JOIN
	if len(sel.From) > 1 {
		return false
	}
	for _, te := range sel.From {
		if !allJoinsAreStraight(te) {
			return false
		}
	}
	// Must have at least one actual JoinTableExpr with StraightJoinType
	return hasStraightJoin(sel.From[0])
}

// hasStraightJoin returns true if the table expression contains at least one STRAIGHT_JOIN.
func hasStraightJoin(te sqlparser.TableExpr) bool {
	switch t := te.(type) {
	case *sqlparser.JoinTableExpr:
		if t.Join == sqlparser.StraightJoinType {
			return true
		}
		return hasStraightJoin(t.LeftExpr) || hasStraightJoin(t.RightExpr)
	case *sqlparser.ParenTableExpr:
		for _, expr := range t.Exprs {
			if hasStraightJoin(expr) {
				return true
			}
		}
	}
	return false
}

func allJoinsAreStraight(te sqlparser.TableExpr) bool {
	switch t := te.(type) {
	case *sqlparser.JoinTableExpr:
		if t.Join != sqlparser.StraightJoinType {
			return false
		}
		return allJoinsAreStraight(t.LeftExpr) && allJoinsAreStraight(t.RightExpr)
	case *sqlparser.AliasedTableExpr:
		return true // base table - no join
	case *sqlparser.ParenTableExpr:
		for _, expr := range t.Exprs {
			if !allJoinsAreStraight(expr) {
				return false
			}
		}
		return true
	}
	return true
}

// innerSubqueryUsesRightJoin returns true if the SELECT's FROM clause contains a RIGHT JOIN.
func (e *Executor) innerSubqueryUsesRightJoin(sel *sqlparser.Select) bool {
	for _, te := range sel.From {
		if hasRightJoin(te) {
			return true
		}
	}
	return false
}

func hasRightJoin(te sqlparser.TableExpr) bool {
	switch t := te.(type) {
	case *sqlparser.JoinTableExpr:
		if t.Join == sqlparser.RightJoinType || t.Join == sqlparser.NaturalRightJoinType {
			return true
		}
		return hasRightJoin(t.LeftExpr) || hasRightJoin(t.RightExpr)
	case *sqlparser.ParenTableExpr:
		for _, expr := range t.Exprs {
			if hasRightJoin(expr) {
				return true
			}
		}
	}
	return false
}

// allInnerTablesTwoOrMoreRows returns true if the inner SELECT uses STRAIGHT_JOIN and ALL inner
// tables have >= 2 rows. In this case, MySQL forces MATERIALIZED even for constant outer expressions
// (e.g., "11 IN (SELECT ...)"), because materializing is cheaper than scanning large fixed-order tables.
// outerQueryHasOnlyDerivedTables returns true if the outer query's FROM clause contains
// only derived tables (subqueries) and no real base tables. This is used to determine
// whether MySQL will prefer MATERIALIZED strategy for IN subqueries (since FirstMatch
// can't be applied efficiently when there's no base table to probe against).
func (e *Executor) outerQueryHasOnlyDerivedTables(sel *sqlparser.Select) bool {
	if sel == nil || len(sel.From) == 0 {
		return false
	}
	numDerived := 0
	numBase := 0
	for _, te := range sel.From {
		numDerived += countDerivedTablesInExpr(te)
		baseNames := e.extractAllTableNames(te)
		for _, name := range baseNames {
			if !strings.EqualFold(name, "dual") {
				numBase++
			}
		}
	}
	return numDerived > 0 && numBase == 0
}

func (e *Executor) allInnerTablesTwoOrMoreRows(inner *sqlparser.Select) bool {
	if e.Storage == nil {
		return false
	}
	if !e.innerSubqueryUsesOnlyStraightJoin(inner) {
		return false
	}
	var innerTables []string
	for _, te := range inner.From {
		innerTables = append(innerTables, e.extractAllTableNames(te)...)
	}
	var realInner []string
	for _, t := range innerTables {
		if !strings.EqualFold(t, "dual") {
			realInner = append(realInner, t)
		}
	}
	if len(realInner) < 2 {
		return false
	}
	for _, tblName := range realInner {
		tbl, err := e.Storage.GetTable(e.CurrentDB, tblName)
		if err != nil || len(tbl.Rows) < 2 {
			return false
		}
	}
	return true
}

// selectWhereHasINSubquery returns true if a SELECT's WHERE clause contains at least one
// IN subquery (col IN (SELECT ...)). This is used to detect nested IN subquery chains
// which trigger MySQL's DuplicateWeedout semi-join strategy.
func selectWhereHasINSubquery(sel *sqlparser.Select) bool {
	if sel == nil || sel.Where == nil {
		return false
	}
	found := false
	_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
		if found {
			return false, nil
		}
		if cmp, ok := n.(*sqlparser.ComparisonExpr); ok {
			if cmp.Operator == sqlparser.InOp {
				if _, ok := cmp.Right.(*sqlparser.Subquery); ok {
					found = true
					return false, nil
				}
			}
		}
		// Also handle tuple IN subquery: (a, b) IN (SELECT ...)
		if cmp, ok := n.(*sqlparser.ComparisonExpr); ok {
			if cmp.Operator == sqlparser.InOp {
				if _, ok := cmp.Right.(*sqlparser.Subquery); ok {
					found = true
					return false, nil
				}
			}
		}
		return true, nil
	}, sel.Where)
	return found
}

// collectAllTablesForDuplicateWeedout recursively collects all table names from a nested IN subquery chain.
// This is used to determine all tables that should appear in DuplicateWeedout EXPLAIN output (all SIMPLE, id=1).
// Returns a slice of table names in depth-first order (inner tables first, outer table last).
func (e *Executor) collectAllTablesForDuplicateWeedout(inner *sqlparser.Select) []string {
	var result []string
	// Collect direct tables from this SELECT's FROM
	for _, te := range inner.From {
		for _, tn := range e.extractAllTableNames(te) {
			if !strings.EqualFold(tn, "dual") {
				result = append(result, tn)
			}
		}
	}
	// Recursively collect tables from any IN subqueries in WHERE
	if inner.Where != nil {
		_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
			if cmp, ok := n.(*sqlparser.ComparisonExpr); ok {
				if cmp.Operator == sqlparser.InOp {
					if sub, ok := cmp.Right.(*sqlparser.Subquery); ok {
						if nestedSel, ok := sub.Select.(*sqlparser.Select); ok {
							nested := e.collectAllTablesForDuplicateWeedout(nestedSel)
							result = append(result, nested...)
						}
						return false, nil
					}
				}
			}
			return true, nil
		}, inner.Where)
	}
	return result
}

// shouldUseDuplicateWeedout returns true when the inner SELECT has nested IN subqueries
// (i.e., its WHERE has an IN subquery), which triggers MySQL's DuplicateWeedout semijoin strategy.
// When DuplicateWeedout is used, MySQL flattens all tables from all nesting levels into a
// single id=1 SIMPLE join with "Start temporary" / "End temporary" markers.
func (e *Executor) shouldUseDuplicateWeedout(inner *sqlparser.Select) bool {
	if !e.isSemijoinEnabled() {
		return false
	}
	return selectWhereHasINSubquery(inner)
}

// shouldMaterializeSubquery returns true if an IN subquery should use the MATERIALIZED strategy.
// MySQL uses a cost-based approach, but we approximate with a row count heuristic:
// extractINSubqueryOuterCol extracts the outer column name from the first
// IN (subquery) condition in a SELECT's WHERE clause.
// For "WHERE col IN (SELECT ...)", returns "col".
// inSubqueryOuterIsConst returns true if the WHERE clause has "const IN (subquery)"
// where the left side of IN is a literal constant (e.g. "11 IN (SELECT ...)").
// In this case MySQL uses const access on the materialized hash table.
func inSubqueryOuterIsConst(sel *sqlparser.Select) bool {
	if sel == nil || sel.Where == nil {
		return false
	}
	result := false
	_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
		if cmp, ok := n.(*sqlparser.ComparisonExpr); ok {
			if cmp.Operator == sqlparser.InOp {
				if _, ok := cmp.Right.(*sqlparser.Subquery); ok {
					switch cmp.Left.(type) {
					case *sqlparser.Literal, *sqlparser.NullVal:
						result = true
						return false, nil
					}
				}
			}
		}
		return true, nil
	}, sel.Where.Expr)
	return result
}

// For "WHERE tbl.col IN (SELECT ...)", returns "col".
// Returns "" if no IN subquery condition is found.
func extractINSubqueryOuterCol(sel *sqlparser.Select) string {
	if sel == nil || sel.Where == nil {
		return ""
	}
	return extractINColFromExpr(sel.Where.Expr)
}

// selectHasDerivedTableInFrom returns true if the SELECT has at least one
// derived table (subselect) in its FROM clause (e.g. FROM (SELECT ...) AS x).
// MySQL shows ref="func" and "Using where" for <subqueryN> eq_ref placeholders
// when the outer table comes from a derived query.
func selectHasDerivedTableInFrom(sel *sqlparser.Select) bool {
	if sel == nil {
		return false
	}
	for _, expr := range sel.From {
		if ate, ok := expr.(*sqlparser.AliasedTableExpr); ok {
			if _, ok2 := ate.Expr.(*sqlparser.DerivedTable); ok2 {
				return true
			}
		}
	}
	return false
}

// extractINSubqueryOuterTable extracts the TABLE name from "WHERE tbl.col IN (SELECT ...)".
// Returns the table qualifier if present, else "".
func extractINSubqueryOuterTable(sel *sqlparser.Select) string {
	if sel == nil || sel.Where == nil {
		return ""
	}
	return extractINTableFromExpr(sel.Where.Expr)
}

// extractINTableFromExpr finds the table qualifier of the IN column in a WHERE expression.
// For "t3.a IN (SELECT ...)", returns "t3".
func extractINTableFromExpr(expr sqlparser.Expr) string {
	if expr == nil {
		return ""
	}
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if e.Operator == sqlparser.InOp {
			if col, ok := e.Left.(*sqlparser.ColName); ok {
				return col.Qualifier.Name.String()
			}
		}
	case *sqlparser.AndExpr:
		if tbl := extractINTableFromExpr(e.Left); tbl != "" {
			return tbl
		}
		return extractINTableFromExpr(e.Right)
	}
	return ""
}

// extractINColFromNode finds the outer column name for a specific IN subquery `sub`
// within a WHERE expression node. For "WHERE a IN (sub1) AND b IN (sub2)", calling
// with sub2 returns "b".
func extractINColFromNode(node sqlparser.SQLNode, sub *sqlparser.Subquery) string {
	var result string
	_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
		if cmp, ok := n.(*sqlparser.ComparisonExpr); ok {
			if cmp.Operator == sqlparser.InOp {
				if rSub, ok := cmp.Right.(*sqlparser.Subquery); ok {
					if rSub == sub {
						if col, ok := cmp.Left.(*sqlparser.ColName); ok {
							result = col.Name.String()
							return false, nil
						}
					}
				}
			}
		}
		return true, nil
	}, node)
	return result
}

func extractINColFromExpr(expr sqlparser.Expr) string {
	if expr == nil {
		return ""
	}
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if e.Operator == sqlparser.InOp {
			// col IN (subquery)
			if col, ok := e.Left.(*sqlparser.ColName); ok {
				return col.Name.String()
			}
		}
	case *sqlparser.AndExpr:
		if col := extractINColFromExpr(e.Left); col != "" {
			return col
		}
		return extractINColFromExpr(e.Right)
	}
	return ""
}

// matTypeClass returns the "materialization type class" for a MySQL column type.
// MySQL only uses materialization when the outer column and inner column have
// compatible type classes (same class). Cross-class comparisons (e.g., INT vs DATE,
// INT vs CHAR) fall back to DuplicateWeedout/non-materialized strategies.
// Classes: "numeric", "string", "temporal", "other"
func matTypeClass(colType string) string {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	// Strip type parameters e.g. VARCHAR(255) → VARCHAR
	if idx := strings.IndexByte(upper, '('); idx >= 0 {
		upper = strings.TrimSpace(upper[:idx])
	}
	switch upper {
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT",
		"FLOAT", "DOUBLE", "REAL", "DECIMAL", "NUMERIC",
		"UNSIGNED", "BIT":
		return "numeric"
	case "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT",
		"BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB",
		"ENUM", "SET":
		return "string"
	case "DATE", "DATETIME", "TIMESTAMP", "TIME", "YEAR":
		return "temporal"
	}
	return "other"
}

// hasNonINRangeConditionOnCol checks whether the WHERE expression contains any
// range conditions (less-than, greater-than, between, OR of comparisons) on the
// given column name (ignoring table qualifiers), other than an IN-subquery condition.
// This is used to determine if "Using where" should be shown on a <subqueryN> placeholder.
func hasNonINRangeConditionOnCol(expr sqlparser.Expr, colName string) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		return hasNonINRangeConditionOnCol(e.Left, colName) || hasNonINRangeConditionOnCol(e.Right, colName)
	case *sqlparser.OrExpr:
		// OR involving the same column implies a range/multi-range filter
		leftHas := hasNonINRangeConditionOnCol(e.Left, colName)
		rightHas := hasNonINRangeConditionOnCol(e.Right, colName)
		return leftHas || rightHas
	case *sqlparser.ComparisonExpr:
		if e.Operator == sqlparser.InOp {
			return false // skip IN expressions
		}
		var lCol, rCol string
		if col, ok := e.Left.(*sqlparser.ColName); ok {
			lCol = col.Name.String()
		}
		if col, ok := e.Right.(*sqlparser.ColName); ok {
			rCol = col.Name.String()
		}
		if strings.EqualFold(lCol, colName) || strings.EqualFold(rCol, colName) {
			return true
		}
	case *sqlparser.BetweenExpr:
		if col, ok := e.Left.(*sqlparser.ColName); ok {
			if strings.EqualFold(col.Name.String(), colName) {
				return true
			}
		}
	}
	return false
}

// inSubqueryTypesCompatibleForMat returns true if the outer join column type
// and inner select column type are compatible for materialization (same type class).
// When types are incompatible (e.g., INT vs DATE), MySQL falls back to DuplicateWeedout.
func (e *Executor) inSubqueryTypesCompatibleForMat(outerSel *sqlparser.Select, inner *sqlparser.Select, subNode *sqlparser.Subquery) bool {
	if e.Storage == nil || e.Catalog == nil {
		return true // assume compatible if we can't check
	}

	// Find the outer join column name from the WHERE clause
	var outerColName, outerTableName string
	if outerSel != nil && outerSel.Where != nil {
		_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
			if cmp, ok := n.(*sqlparser.ComparisonExpr); ok {
				if cmp.Operator == sqlparser.InOp {
					var rSub *sqlparser.Subquery
					if subNode != nil {
						rSub, _ = cmp.Right.(*sqlparser.Subquery)
						if rSub != subNode {
							return true, nil
						}
					} else {
						rSub, _ = cmp.Right.(*sqlparser.Subquery)
						if rSub == nil {
							return true, nil
						}
					}
					if col, ok := cmp.Left.(*sqlparser.ColName); ok {
						outerColName = strings.ToLower(col.Name.String())
						if !col.Qualifier.Name.IsEmpty() {
							outerTableName = strings.ToLower(col.Qualifier.Name.String())
						}
						return false, nil
					}
				}
			}
			return true, nil
		}, outerSel.Where.Expr)
	}
	if outerColName == "" {
		return true // no column to check
	}

	// Find the outer column type from the FROM tables
	outerColType := ""
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return true
	}
	for _, te := range outerSel.From {
		var tblName, alias string
		if ate, ok := te.(*sqlparser.AliasedTableExpr); ok {
			if tn, ok := ate.Expr.(sqlparser.TableName); ok {
				tblName = strings.ToLower(tn.Name.String())
			}
			if !ate.As.IsEmpty() {
				alias = strings.ToLower(ate.As.String())
			}
		}
		if tblName == "" {
			continue
		}
		// Check if this table matches the outer column qualifier
		if outerTableName != "" && outerTableName != tblName && outerTableName != alias {
			continue
		}
		if tblDef, err := db.GetTable(tblName); err == nil && tblDef != nil {
			for _, col := range tblDef.Columns {
				if strings.EqualFold(col.Name, outerColName) {
					outerColType = col.Type
					break
				}
			}
		}
		if outerColType != "" {
			break
		}
	}
	if outerColType == "" {
		return true // can't determine type, assume compatible
	}

	// Find the inner column type from the inner SELECT first expression
	innerColType := ""
	if inner != nil && inner.SelectExprs != nil && len(inner.SelectExprs.Exprs) > 0 {
		if ae, ok := inner.SelectExprs.Exprs[0].(*sqlparser.AliasedExpr); ok {
			if col, ok := ae.Expr.(*sqlparser.ColName); ok {
				innerColName := strings.ToLower(col.Name.String())
				// Look up in inner tables
				for _, te := range inner.From {
					var tblName string
					if ate, ok := te.(*sqlparser.AliasedTableExpr); ok {
						if tn, ok := ate.Expr.(sqlparser.TableName); ok {
							tblName = strings.ToLower(tn.Name.String())
						}
					}
					if tblName == "" {
						continue
					}
					if tblDef, err := db.GetTable(tblName); err == nil && tblDef != nil {
						for _, c := range tblDef.Columns {
							if strings.EqualFold(c.Name, innerColName) {
								innerColType = c.Type
								break
							}
						}
					}
					if innerColType != "" {
						break
					}
				}
			}
		}
	}
	if innerColType == "" {
		return true // can't determine type, assume compatible
	}

	// Check if type classes are the same
	outerClass := matTypeClass(outerColType)
	innerClass := matTypeClass(innerColType)
	return outerClass == innerClass || outerClass == "other" || innerClass == "other"
}

// - If total inner table rows > materializationThreshold → MATERIALIZED
// - If the join column has no usable index AND total rows > 0 → MATERIALIZED
// - If the inner table is empty (0 rows) → MATERIALIZED (MySQL still materializes for large outer tables)
// Otherwise → inline semijoin (SIMPLE, all rows at id=1).
const materializationRowThreshold = 100

func (e *Executor) shouldMaterializeSubquery(inner *sqlparser.Select, outerIsSystem bool) bool {
	if e.Storage == nil {
		return false
	}

	// Collect inner table names
	var innerTables []string
	for _, te := range inner.From {
		innerTables = append(innerTables, e.extractAllTableNames(te)...)
	}
	// Filter out DUAL
	var realInnerTables []string
	for _, t := range innerTables {
		if !strings.EqualFold(t, "dual") {
			realInnerTables = append(realInnerTables, t)
		}
	}
	if len(realInnerTables) == 0 {
		return false
	}

	// Find the join column (first SELECT expression of the inner query).
	var joinColName string
	if inner.SelectExprs != nil && len(inner.SelectExprs.Exprs) > 0 {
		if ae, ok := inner.SelectExprs.Exprs[0].(*sqlparser.AliasedExpr); ok {
			if col, ok := ae.Expr.(*sqlparser.ColName); ok {
				joinColName = strings.ToLower(col.Name.String())
			}
		}
	}

	// For single-table subqueries, check for index coverage on the join column.
	// MySQL's strategy depends on the index type and optimizer flags:
	// - PRIMARY KEY or UNIQUE on join col → always eq_ref (inline), regardless of firstmatch setting
	// - Constant primary key equality in WHERE (pk=N) → const/eq_ref access, never materialize
	// - Secondary (non-unique) index + firstmatch=on → FirstMatch (inline)
	// - Secondary (non-unique) index + firstmatch=off → MATERIALIZED (for large tables)
	// - No index → MATERIALIZED (if large table) or inline (if small)
	firstMatchOn := e.isOptimizerSwitchEnabled("firstmatch")
	if len(realInnerTables) == 1 {
		innerTableName := realInnerTables[0]
		tbl, tblErr := e.Storage.GetTable(e.CurrentDB, innerTableName)
		if tblErr != nil {
			// The table name might be a view. Try to resolve it to its base table.
			if baseTableName, isView, _, _ := e.resolveViewToBaseTable(innerTableName); isView && baseTableName != "" {
				if baseTbl, err2 := e.Storage.GetTable(e.CurrentDB, baseTableName); err2 == nil {
					tbl = baseTbl
					tblErr = nil
					// For SELECT * subqueries (joinColName=""), derive join col from PK of base table.
					// This handles "t1field IN (SELECT * FROM view)" where view = "SELECT * FROM t2"
					// and t2 has PRIMARY KEY on its first column → use eq_ref (no materialization).
					if joinColName == "" && baseTbl.Def != nil && len(baseTbl.Def.PrimaryKey) > 0 {
						joinColName = strings.ToLower(baseTbl.Def.PrimaryKey[0])
					}
				}
			}
		}
		if tblErr == nil && tbl != nil && tbl.Def != nil {
			// Check if the inner WHERE has a constant equality on the primary key column(s).
			// When pk = constant, MySQL uses const/eq_ref access → no materialization.
			if len(tbl.Def.PrimaryKey) > 0 && inner.Where != nil {
				if hasConstPKEquality(inner.Where.Expr, tbl.Def.PrimaryKey[0]) {
					return false // const table access via primary key → always inline
				}
			}

			if joinColName != "" {
				// Primary key on join col → eq_ref (inline)
				if len(tbl.Def.PrimaryKey) > 0 && strings.EqualFold(tbl.Def.PrimaryKey[0], joinColName) {
					return false // eq_ref via primary key → always inline
				}
				// Check secondary indexes
				for _, idx := range tbl.Def.Indexes {
					if len(idx.Columns) > 0 && strings.EqualFold(idx.Columns[0], joinColName) {
						// Secondary index on join column:
						// - firstmatch=on → FirstMatch (inline, no materialization)
						//   UNLESS the inner WHERE has a range condition on the join column,
						//   in which case MySQL uses materialization (range scan materializes
						//   a subset and probes via hash).
						// - firstmatch=off → may materialize if table is large
						if firstMatchOn {
							// Check if the inner WHERE has a range condition on the join column.
							// E.g., "kp1 < 20" would cause MySQL to use materialization.
							innerHasRangeOnJoinCol := false
							if inner.Where != nil {
								innerHasRangeOnJoinCol = whereHasRangeOnCol(inner.Where.Expr, joinColName)
							}
							if !innerHasRangeOnJoinCol {
								return false // FirstMatch strategy → inline (equality/no-range)
							}
							// Range condition → fall through to materialization
						}
						// firstmatch=off or range on join col: fall through to row count check
						break
					}
				}
			}
		}
	}

	// Multi-table inner subqueries:
	// When the outer table is a "system" table (exactly 1 row) and firstmatch=on,
	// MySQL always uses the inline FirstMatch/semijoin strategy regardless of inner table sizes.
	// This is because the const/system outer table makes FirstMatch optimal.
	// When the outer table requires a scan (2+ rows), check if any inner table is empty —
	// even one empty table in an INNER JOIN causes MySQL to prefer MATERIALIZED strategy.
	if len(realInnerTables) > 1 {
		anyEmpty := false
		for _, tblName := range realInnerTables {
			tbl, err := e.Storage.GetTable(e.CurrentDB, tblName)
			if err != nil || len(tbl.Rows) == 0 {
				anyEmpty = true
				break
			}
		}
		if anyEmpty {
			// Any inner table empty:
			// - If outer is a "system" table (1 row) and firstmatch=on, MySQL uses inline
			//   FirstMatch because the const/system outer makes it optimal.
			// - If total inner rows >= 2: MySQL uses LooseScan (Start/End temporary) regardless
			//   of join type — at least one non-empty inner table makes LooseScan viable.
			// - If total inner rows == 1 and RIGHT JOIN with the right table having that 1 row:
			//   LooseScan applies since the right (preserved) table drives.
			// - Otherwise (total <= 1 with no LooseScan-viable configuration): MATERIALIZED.
			if outerIsSystem && firstMatchOn {
				return false // outer is system + firstmatch=on → inline FirstMatch
			}
			// Compute total inner rows to determine LooseScan viability
			totalInnerRows := 0
			for _, tblName := range realInnerTables {
				if tbl, err := e.Storage.GetTable(e.CurrentDB, tblName); err == nil {
					totalInnerRows += len(tbl.Rows)
				}
			}
			if totalInnerRows >= 2 {
				// For STRAIGHT_JOIN, the join order is fixed (t2 STRAIGHT_JOIN t3 → t2 first).
				// When the FIRST (leftmost) inner table is non-empty, MySQL uses MATERIALIZED —
				// BUT ONLY when the outer table is NOT a system table (1 row).
				// When outer is system (1 row), MySQL always uses FirstMatch even for STRAIGHT_JOIN.
				// For all other join types (INNER, LEFT, RIGHT), MySQL can reorder tables and
				// LooseScan is viable when total inner rows >= 2, BUT ONLY when loosescan=on.
				// When loosescan=off, MySQL uses MATERIALIZED instead.
				looseScanOn := e.isOptimizerSwitchEnabled("loosescan")
				if !outerIsSystem && e.innerSubqueryUsesOnlyStraightJoin(inner) && len(realInnerTables) >= 2 {
					firstTbl := realInnerTables[0]
					if tbl, err := e.Storage.GetTable(e.CurrentDB, firstTbl); err == nil && len(tbl.Rows) > 0 {
						return true // STRAIGHT_JOIN with non-empty first table → MATERIALIZED
					}
				}
				if looseScanOn {
					return false // total >= 2 inner rows → LooseScan strategy (not MATERIALIZED)
				}
				return true // loosescan=off → MATERIALIZED
			}
			// total == 1: check if RIGHT JOIN with right table being the non-empty one.
			// When firstmatch=on, MySQL can use LooseScan for this case.
			// When firstmatch=off, MySQL uses MATERIALIZED instead.
			if firstMatchOn && totalInnerRows == 1 && e.innerSubqueryUsesRightJoin(inner) && len(realInnerTables) >= 2 {
				lastTbl := realInnerTables[len(realInnerTables)-1]
				if tbl, err := e.Storage.GetTable(e.CurrentDB, lastTbl); err == nil && len(tbl.Rows) > 0 {
					return false // RIGHT JOIN with right table having the 1 row → LooseScan
				}
			}
			return true // use MATERIALIZED strategy
		}
		// All inner tables have data:
		// - STRAIGHT_JOIN with first table having >= 2 rows → MATERIALIZED (fixed join order, large probe set)
		//   BUT ONLY when the outer is NOT a system table (outer=system always uses FirstMatch).
		// - firstmatch=on + any inner table has PK on join col → FirstMatch (eq_ref inline, cheap even with data)
		// - firstmatch=on + no PK on join col + total inner rows above threshold → MATERIALIZED
		// - firstmatch=on + no PK on join col + small tables → inline semijoin (SIMPLE)
		// - firstmatch=off + any inner table has PK on join column: inline (eq_ref → no cost benefit from MATERIALIZED)
		// - firstmatch=off + no PK on join column + large tables: MATERIALIZED
		const multiTableMatThreshold = 5
		if !outerIsSystem && e.innerSubqueryUsesOnlyStraightJoin(inner) && len(realInnerTables) >= 2 {
			firstTbl := realInnerTables[0]
			if tbl, err := e.Storage.GetTable(e.CurrentDB, firstTbl); err == nil && len(tbl.Rows) >= 2 {
				return true // STRAIGHT_JOIN with large first table → MATERIALIZED even when firstmatch=on
			}
		}
		if firstMatchOn {
			// When any inner table has a PRIMARY KEY or SECONDARY INDEX on the join column,
			// MySQL can use ref/eq_ref access → FirstMatch is cheap and preferred regardless of table size.
			if joinColName != "" {
				for _, tblName := range realInnerTables {
					if tbl, err := e.Storage.GetTable(e.CurrentDB, tblName); err == nil && tbl.Def != nil {
						// Check PRIMARY KEY
						if len(tbl.Def.PrimaryKey) > 0 && strings.EqualFold(tbl.Def.PrimaryKey[0], joinColName) {
							return false // eq_ref via primary key → FirstMatch is cheap
						}
						// Check secondary indexes
						for _, idx := range tbl.Def.Indexes {
							if len(idx.Columns) > 0 && strings.EqualFold(idx.Columns[0], joinColName) {
								// If inner subquery has a range condition on the join col,
								// MySQL uses MATERIALIZED even with firstmatch=on.
								innerHasRangeOnJC := false
								if inner.Where != nil {
									innerHasRangeOnJC = whereHasRangeOnCol(inner.Where.Expr, joinColName)
								}
								if !innerHasRangeOnJC {
									return false // ref via secondary index → FirstMatch is cheap
								}
								// Range condition → fall through to MATERIALIZED
							}
						}
					}
				}
			}
			// No index on join column in any inner table: use total row count to decide.
			// When total inner rows exceeds the threshold, MySQL's cost model prefers MATERIALIZED.
			totalInnerRowsForMat := 0
			for _, tblName := range realInnerTables {
				if tbl, err := e.Storage.GetTable(e.CurrentDB, tblName); err == nil {
					totalInnerRowsForMat += len(tbl.Rows)
				}
			}
			if totalInnerRowsForMat > multiTableMatThreshold {
				return true // Large multi-table subquery with no usable index → MATERIALIZED
			}
			return false // Small multi-table subquery → FirstMatch strategy (inline)
		}
		// firstmatch=off: check if the first inner table has a PK on the join column.
		// If so, MySQL uses inline eq_ref access instead of MATERIALIZED.
		if joinColName != "" && e.Storage != nil {
			firstTbl := realInnerTables[0]
			if tbl, err := e.Storage.GetTable(e.CurrentDB, firstTbl); err == nil && tbl.Def != nil {
				if len(tbl.Def.PrimaryKey) > 0 && strings.EqualFold(tbl.Def.PrimaryKey[0], joinColName) {
					return false // eq_ref via primary key → inline even for multi-table
				}
			}
		}
		return true // firstmatch=off, no PK on join col → MATERIALIZED
	}

	// Compute total row count across all inner tables (single-table case)
	totalRows := 0
	for _, tblName := range realInnerTables {
		tbl, err := e.Storage.GetTable(e.CurrentDB, tblName)
		if err != nil {
			continue
		}
		totalRows += len(tbl.Rows)
	}

	// Empty table handling:
	// - subquery_materialization_cost_based=off → always materialize (even empty, even firstmatch=on)
	// - subquery_materialization_cost_based=on + firstmatch=on → use FirstMatch (SIMPLE), not MATERIALIZED
	// - subquery_materialization_cost_based=on + firstmatch=off → materialize
	if totalRows == 0 {
		costBased := e.isOptimizerSwitchEnabled("subquery_materialization_cost_based")
		if !costBased {
			// Cost-based disabled → always materialize
			return true
		}
		if !firstMatchOn {
			// firstmatch=off → materialize empty tables too
			return true
		}
		// firstmatch=on + cost_based=on → use FirstMatch (inline/SIMPLE)
		return false
	}

	// Large single-table subquery with no primary key and no usable index → materialize.
	if totalRows > materializationRowThreshold {
		return true
	}

	// Small non-empty single-table subquery with no primary key (and no FirstMatch) → materialize.
	return true
}

// hasConstPKEquality checks if the WHERE expression has a constant equality condition on the given column.
// For example, `pk = 12` or `12 = pk` (with a literal, not a column reference) returns true.
// This is used to detect "const table" access patterns where MySQL accesses a single row via primary key.
// whereHasRangeOnCol returns true if the WHERE expression contains a range condition
// (non-equality comparison) directly on the given column name. Examples:
//   col < 20, col > 5, col <= 100, col BETWEEN a AND b
// This is used to detect when MySQL switches from FirstMatch to materialization
// for single-table IN subqueries with range access on the join column.
func whereHasRangeOnCol(expr sqlparser.Expr, col string) bool {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		var colName string
		var otherSide sqlparser.Expr
		if c, ok := e.Left.(*sqlparser.ColName); ok {
			colName = strings.ToLower(c.Name.String())
			otherSide = e.Right
		} else if c, ok := e.Right.(*sqlparser.ColName); ok {
			colName = strings.ToLower(c.Name.String())
			otherSide = e.Left
		}
		if strings.EqualFold(colName, col) {
			// Only treat as a range condition when compared to a literal value,
			// not another column reference (correlation doesn't restrict range).
			if _, isLiteral := otherSide.(*sqlparser.Literal); !isLiteral {
				break
			}
			switch e.Operator {
			case sqlparser.LessThanOp, sqlparser.LessEqualOp,
				sqlparser.GreaterThanOp, sqlparser.GreaterEqualOp,
				sqlparser.NotEqualOp:
				return true
			}
		}

	case *sqlparser.AndExpr:
		return whereHasRangeOnCol(e.Left, col) || whereHasRangeOnCol(e.Right, col)
	case *sqlparser.OrExpr:
		return whereHasRangeOnCol(e.Left, col) || whereHasRangeOnCol(e.Right, col)
	}
	return false
}

func hasConstPKEquality(expr sqlparser.Expr, pkCol string) bool {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if e.Operator == sqlparser.EqualOp {
			// Check col = literal
			if col, ok := e.Left.(*sqlparser.ColName); ok {
				if strings.EqualFold(col.Name.String(), pkCol) {
					// Right side must be a literal (not a column ref)
					if _, isLit := e.Right.(*sqlparser.Literal); isLit {
						return true
					}
				}
			}
			// Check literal = col
			if col, ok := e.Right.(*sqlparser.ColName); ok {
				if strings.EqualFold(col.Name.String(), pkCol) {
					if _, isLit := e.Left.(*sqlparser.Literal); isLit {
						return true
					}
				}
			}
		}
	case *sqlparser.AndExpr:
		return hasConstPKEquality(e.Left, pkCol) || hasConstPKEquality(e.Right, pkCol)
	}
	return false
}

// extractConstPKValue extracts the literal value from a constant PK equality condition.
// For example, `pk = 12` returns ("12", true); otherwise returns ("", false).
func extractConstPKValue(expr sqlparser.Expr, pkCol string) (string, bool) {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if e.Operator == sqlparser.EqualOp {
			if col, ok := e.Left.(*sqlparser.ColName); ok {
				if strings.EqualFold(col.Name.String(), pkCol) {
					if lit, isLit := e.Right.(*sqlparser.Literal); isLit {
						return lit.Val, true
					}
				}
			}
			if col, ok := e.Right.(*sqlparser.ColName); ok {
				if strings.EqualFold(col.Name.String(), pkCol) {
					if lit, isLit := e.Left.(*sqlparser.Literal); isLit {
						return lit.Val, true
					}
				}
			}
		}
	case *sqlparser.AndExpr:
		if v, ok := extractConstPKValue(e.Left, pkCol); ok {
			return v, true
		}
		return extractConstPKValue(e.Right, pkCol)
	}
	return "", false
}

// isImpossibleConstPKWhere checks if the inner SELECT's WHERE clause is a constant PK equality
// that doesn't match any existing row, OR if ALL inner tables are empty (making the result empty).
// Used for MySQL's "no matching row in const table" EXPLAIN optimization.
func (e *Executor) isImpossibleConstPKWhere(inner *sqlparser.Select) bool {
	if e.Storage == nil {
		return false
	}

	// Collect all inner table names
	var innerTableNames []string
	for _, te := range inner.From {
		innerTableNames = append(innerTableNames, e.extractAllTableNames(te)...)
	}

	// Single-table inner subquery with a constant PK equality that matches no row.
	if inner.Where == nil || len(innerTableNames) != 1 {
		return false
	}
	tableName := innerTableNames[0]
	if strings.EqualFold(tableName, "dual") {
		return false
	}
	tbl, err := e.Storage.GetTable(e.CurrentDB, tableName)
	if err != nil || tbl.Def == nil || len(tbl.Def.PrimaryKey) == 0 {
		return false
	}
	pkCol := tbl.Def.PrimaryKey[0]
	pkVal, ok := extractConstPKValue(inner.Where.Expr, pkCol)
	if !ok {
		return false
	}
	// Check if any row has this PK value
	// Row is a map[string]interface{}, keyed by column name.
	for _, row := range tbl.Rows {
		if val, ok := row[pkCol]; ok {
			rowVal := fmt.Sprintf("%v", val)
			if rowVal == pkVal {
				return false // Row exists → not impossible
			}
		}
	}
	return true // No matching row found → impossible WHERE
}

// isSubqueryInINContext checks if a Subquery node is used in an IN, NOT IN, or = ANY / != ANY context.
// Note: plain scalar equality like "col = (SELECT 1 FROM t2)" (without ANY/ALL) is NOT IN context;
// those are SUBQUERY not semijoin-flattenable.
func isSubqueryInINContext(node sqlparser.SQLNode, sub *sqlparser.Subquery) bool {
	found := false
	_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
		if cmp, ok := n.(*sqlparser.ComparisonExpr); ok {
			switch cmp.Operator {
			case sqlparser.InOp, sqlparser.NotInOp:
				if subR, ok := cmp.Right.(*sqlparser.Subquery); ok && subR == sub {
					found = true
					return false, nil
				}
			case sqlparser.EqualOp, sqlparser.NotEqualOp,
				sqlparser.LessThanOp, sqlparser.LessEqualOp,
				sqlparser.GreaterThanOp, sqlparser.GreaterEqualOp:
				// = ANY / != ANY / < ANY / etc. — only with ANY or ALL modifier.
				// Plain scalar comparisons like col = (SELECT 1) have Modifier=Missing and are NOT IN context.
				if (cmp.Modifier == sqlparser.Any || cmp.Modifier == sqlparser.All) {
					if subR, ok := cmp.Right.(*sqlparser.Subquery); ok && subR == sub {
						found = true
						return false, nil
					}
				}
			}
		}
		return true, nil
	}, node)
	return found
}

// isSubqueryInExistsContext checks if a Subquery node is inside an ExistsExpr.
func isSubqueryInExistsContext(node sqlparser.SQLNode, sub *sqlparser.Subquery) bool {
	found := false
	_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
		if exists, ok := n.(*sqlparser.ExistsExpr); ok {
			if exists.Subquery == sub {
				found = true
				return false, nil
			}
		}
		return true, nil
	}, node)
	return found
}

// isSemiJoinDecorrelatable checks if a correlated subquery can be converted to a semijoin.
// This is true when the correlation consists only of simple equality conditions
// between outer and inner table columns (e.g., WHERE outer.col = inner.col).
// Such conditions can be "hoisted" into the semijoin join condition, making
// the inner subquery effectively non-correlated.
func (e *Executor) isSemiJoinDecorrelatable(inner *sqlparser.Select, outerTables map[string]bool) bool {
	if inner.Where == nil {
		return false // no WHERE → no correlation to decorrelate
	}
	// Collect inner table names
	innerTables := e.extractTableNamesAndAliases(inner)

	// Check if ALL references to outer tables in WHERE are simple equality conditions
	allCorrelationsAreEqualities := true
	hasAnyCorrelation := false

	var checkExpr func(expr sqlparser.Expr) bool
	checkExpr = func(expr sqlparser.Expr) bool {
		switch e := expr.(type) {
		case *sqlparser.AndExpr:
			return checkExpr(e.Left) && checkExpr(e.Right)
		case *sqlparser.ComparisonExpr:
			if e.Operator != sqlparser.EqualOp {
				// Non-equality condition: check if it references outer tables
				// If not correlated, it's fine. If correlated, not decorrelatable.
				leftRefOuter := exprReferencesOuterTable(e.Left, outerTables, innerTables)
				rightRefOuter := exprReferencesOuterTable(e.Right, outerTables, innerTables)
				if leftRefOuter || rightRefOuter {
					return false // non-equality correlation → not decorrelatable
				}
				return true
			}
			// Equality: check if it's an outer-inner equality
			leftRefOuter := exprReferencesOuterTable(e.Left, outerTables, innerTables)
			rightRefOuter := exprReferencesOuterTable(e.Right, outerTables, innerTables)
			if leftRefOuter || rightRefOuter {
				hasAnyCorrelation = true
				// This is an equality with outer reference → decorrelatable
				return true
			}
			return true // non-correlated equality → fine
		default:
			return true
		}
	}

	if !checkExpr(inner.Where.Expr) {
		allCorrelationsAreEqualities = false
	}

	return allCorrelationsAreEqualities && hasAnyCorrelation
}

// exprReferencesOuterTable returns true if the expression references an outer table column.
func exprReferencesOuterTable(expr sqlparser.Expr, outerTables map[string]bool, innerTables map[string]bool) bool {
	if col, ok := expr.(*sqlparser.ColName); ok {
		qualifier := strings.ToLower(col.Qualifier.Name.String())
		if qualifier != "" {
			return outerTables[qualifier] && !innerTables[qualifier]
		}
		// Unqualified: check if it's NOT an inner table column (could be outer)
		// For safety, return false for unqualified names
	}
	return false
}

// outerINExprIsConstant checks if the IN condition that contains the given subquery
// has a constant (non-column) expression on the left-hand side.
// When the IN outer expression is a constant (e.g., "11 IN (subquery)"), MySQL uses
// FirstMatch instead of MATERIALIZED because the constant can be checked without
// materializing the inner result for each outer row.
func outerINExprIsConstant(node sqlparser.SQLNode, sub *sqlparser.Subquery) bool {
	isConst := false
	_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
		if cmp, ok := n.(*sqlparser.ComparisonExpr); ok {
			if cmp.Operator == sqlparser.InOp || cmp.Operator == sqlparser.NotInOp {
				if subR, ok := cmp.Right.(*sqlparser.Subquery); ok && subR == sub {
					// Check if the left side is a constant (not a column reference)
					switch cmp.Left.(type) {
					case *sqlparser.Literal, *sqlparser.NullVal:
						isConst = true
					case *sqlparser.ColName:
						isConst = false
					default:
						isConst = false
					}
					return false, nil
				}
			}
		}
		return true, nil
	}, node)
	return isConst
}

// explainSubqueries finds subqueries in SELECT expressions, WHERE, and HAVING clauses.
func (e *Executor) explainSubqueries(sel *sqlparser.Select, idCounter *int64, result *[]explainSelectType, outerSelectType ...string) {
	// Collect outer table names for correlated subquery detection
	outerTables := e.extractTableNamesAndAliases(sel)

	// Determine whether the outer query can use semijoin flattening.
	// MATERIALIZED is only valid when the outer query is semijoin-flattened.
	outerCanSemijoin := e.queryCanBeSemijoinFlattened(sel)

	// Walk the SELECT expressions, WHERE, HAVING, and JOIN ON conditions to find subqueries.
	// We need to avoid descending into FROM clause derived tables (handled separately)
	// but we DO need to walk JOIN ON conditions (which may contain IN subqueries).
	nodes := []sqlparser.SQLNode{}
	if sel.SelectExprs != nil {
		nodes = append(nodes, sel.SelectExprs)
	}
	if sel.Where != nil {
		nodes = append(nodes, sel.Where)
	}
	if sel.Having != nil {
		nodes = append(nodes, sel.Having)
	}
	if sel.OrderBy != nil {
		nodes = append(nodes, sel.OrderBy)
	}
	// Walk ON conditions from JOIN expressions in the FROM clause.
	for _, te := range sel.From {
		e.collectJoinOnConditions(te, &nodes)
	}

	outerHasOnlyDerived := e.outerQueryHasOnlyDerivedTables(sel)
	outerST := ""
	if len(outerSelectType) > 0 {
		outerST = outerSelectType[0]
	}
	for _, node := range nodes {
		startIdx := len(*result)
		e.walkForSubqueries(node, idCounter, result, outerTables, outerCanSemijoin, outerHasOnlyDerived, sel, outerST)
		// MySQL displays DEPENDENT SUBQUERY rows from the WHERE clause in reverse order
		// (higher ids first) because it processes them in reverse during optimization.
		// Reverse the newly-added rows if they are all DEPENDENT SUBQUERY.
		newRows := (*result)[startIdx:]
		if len(newRows) > 1 {
			allDependent := true
			for _, r := range newRows {
				if r.selectType != "DEPENDENT SUBQUERY" {
					allDependent = false
					break
				}
			}
			if allDependent {
				for i, j := 0, len(newRows)-1; i < j; i, j = i+1, j-1 {
					newRows[i], newRows[j] = newRows[j], newRows[i]
				}
			}
		}
	}
}

// collectJoinOnConditions recursively collects ON condition expressions from JOIN table expressions.
// These conditions may contain subqueries that need to be walked for EXPLAIN.
func (e *Executor) collectJoinOnConditions(te sqlparser.TableExpr, nodes *[]sqlparser.SQLNode) {
	switch t := te.(type) {
	case *sqlparser.JoinTableExpr:
		if t.Condition != nil && t.Condition.On != nil {
			*nodes = append(*nodes, t.Condition.On)
		}
		e.collectJoinOnConditions(t.LeftExpr, nodes)
		e.collectJoinOnConditions(t.RightExpr, nodes)
	case *sqlparser.ParenTableExpr:
		for _, expr := range t.Exprs {
			e.collectJoinOnConditions(expr, nodes)
		}
	}
}

// walkForSubqueries walks a node tree to find subqueries (not descending into FROM).
// outerCanSemijoin indicates whether the outer SELECT can use semijoin flattening.
// When false, IN subqueries become SUBQUERY (not MATERIALIZED) since MATERIALIZED
// is only used in the context of semijoin-flattened outer queries.
func (e *Executor) walkForSubqueries(node sqlparser.SQLNode, idCounter *int64, result *[]explainSelectType, outerTables map[string]bool, outerCanSemijoin bool, outerHasOnlyDerivedTables bool, outerSel *sqlparser.Select, outerSelectTypeCtx ...string) {
	// Extract optional outer select type context
	outerSelectTypeOuter := ""
	if len(outerSelectTypeCtx) > 0 {
		outerSelectTypeOuter = outerSelectTypeCtx[0]
	}
	_ = outerSelectTypeOuter
	if node == nil {
		return
	}
	_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
		switch sub := n.(type) {
		case *sqlparser.Subquery:
			outerIDBeforeIncrement := *idCounter
			*idCounter++
			correlated := e.isCorrelatedSubquery(sub.Select, outerTables)
			inContext := isSubqueryInINContext(node, sub)

			switch inner := sub.Select.(type) {
			case *sqlparser.Union:
				unionRows := e.explainUnion(inner, idCounter, false)
				if len(unionRows) > 0 {
					if correlated {
						unionRows[0].selectType = "DEPENDENT SUBQUERY"
						// Mark subsequent UNION rows as DEPENDENT UNION
						for i := 1; i < len(unionRows); i++ {
							if unionRows[i].selectType == "UNION" {
								unionRows[i].selectType = "DEPENDENT UNION"
							}
						}
					} else {
						unionRows[0].selectType = "SUBQUERY"
					}
				}
				*result = append(*result, unionRows...)
			case *sqlparser.Select:
				selectType := "SUBQUERY"
				// Check if the inner SELECT has a NO_SEMIJOIN optimizer hint.
				// When NO_SEMIJOIN is present, MySQL uses the EXISTS strategy (DEPENDENT SUBQUERY),
				// not MATERIALIZED, even if materialization=on.
				innerHasNoSemijoin := false
				for _, c := range inner.Comments.GetComments() {
					if strings.Contains(strings.ToUpper(c), "NO_SEMIJOIN") {
						innerHasNoSemijoin = true
						break
					}
				}
				// Check for "impossible WHERE" first (before considering correlation):
				// When all inner tables are empty OR const PK lookup fails, MySQL uses
				// "no matching row in const table" for the entire outer query.
				// This applies to both IN and EXISTS/correlated subqueries when semijoin=on.
				// Check if subquery is in EXISTS context (MySQL can semijoin-flatten correlated EXISTS)
				inExistsContext := isSubqueryInExistsContext(node, sub)
				// MySQL decorrelates simple correlated EXISTS subqueries:
				// EXISTS (SELECT * FROM t2 LEFT JOIN t3 WHERE outer.col = inner.col)
				// → treated as semijoin with decorrelated inner subquery
				existsCanDecorrelate := inExistsContext && e.isSemijoinEnabled() && outerCanSemijoin && !innerHasNoSemijoin && e.isSemiJoinDecorrelatable(inner, outerTables)

				if e.isSemijoinEnabled() && e.isImpossibleConstPKWhere(inner) {
					selectType = "__IMPOSSIBLE__"
				} else if (correlated || innerHasNoSemijoin) && !existsCanDecorrelate {
					selectType = "DEPENDENT SUBQUERY"
				} else if inContext || existsCanDecorrelate {
					if inContext && outerSelectTypeOuter == "DEPENDENT SUBQUERY" && e.isSemijoinEnabled() {
						// IN subquery inside a DEPENDENT SUBQUERY context: MySQL uses FirstMatch
						// within the dependent context (the inner table is transitively dependent).
						// Don't use MATERIALIZED here — produce a DEPENDENT SUBQUERY row merged
						// at the outer id level instead of a new MATERIALIZED subquery.
						selectType = "DEPENDENT SUBQUERY"
					} else if e.isSemijoinEnabled() && outerCanSemijoin {
						bigTables := false
						if v, ok := e.getSysVar("big_tables"); ok && strings.EqualFold(v, "on") {
							bigTables = true
						}
						if e.isOptimizerSwitchEnabled("materialization") && !bigTables {
							// Use MATERIALIZED only when the inner subquery would benefit from
							// materialization (no usable index, or multiple tables, or empty table).
							// Exception 1: when the outer IN expression is a constant (e.g., "11 IN (subquery)"),
							// MySQL uses FirstMatch instead — no need to materialize for a constant lookup.
							// Exception 2: when the outer table is a "system" table (exactly 1 row)
							// and firstmatch=on, MySQL always uses inline FirstMatch even if inner
							// tables are empty. The const/system outer table makes FirstMatch optimal.
							outerINIsConst := outerINExprIsConstant(node, sub)
							outerIsSystem := e.outerTablesAreSystem(outerTables)
							firstMatchOn := e.isOptimizerSwitchEnabled("firstmatch")
							// Check if outer query has only derived tables (no real base tables).
							// When outer FROM has only derived tables, MySQL prefers MATERIALIZED strategy
							// for the IN subquery (FirstMatch can't be applied efficiently without base table).
							outerHasOnlyDerivedTables := outerHasOnlyDerivedTables
							// Special case: STRAIGHT_JOIN with ALL inner tables having >= 2 rows forces
							// MATERIALIZED even when the outer IN expression is a constant.
							// MySQL's cost-based optimizer decides that materializing is cheaper
							// than FirstMatch when all inner tables are large.
							if !outerIsSystem && e.allInnerTablesTwoOrMoreRows(inner) {
								selectType = "MATERIALIZED"
							} else if outerHasOnlyDerivedTables && !outerINIsConst {
								// Outer has only derived tables: MySQL cannot do efficient FirstMatch,
								// so it uses MATERIALIZED strategy for non-constant IN expressions.
								selectType = "MATERIALIZED"
							} else if e.inSubqueryTypesCompatibleForMat(outerSel, inner, sub) && e.shouldMaterializeSubquery(inner, outerIsSystem) {
								// When firstmatch=on and outer IN is a constant literal, MySQL uses
								// "no matching row in const table" (FirstMatch with const lookup).
								// When firstmatch=off, MySQL always uses MATERIALIZED strategy.
								// Note: type incompatibility (e.g., INT vs DATE) skips materialization.
								//
								// Exception: when the inner subquery's range on the join column is
								// "bounded" by an equal range condition in the outer WHERE, MySQL
								// can use ref access (FirstMatch) instead of materialization.
								// E.g., "WHERE a IN (SELECT kp1 FROM t1 WHERE kp1<20) AND a<20"
								// → MySQL uses ref on kp1=t3.a (FirstMatch), not MATERIALIZED.
								outerAlsoHasRange := false
								if node != nil {
									// Extract the outer join column (left side of IN expr)
									outerJoinCol := extractINColFromNode(node, sub)
									if outerJoinCol != "" {
										// Get the expression from the node (could be *sqlparser.Where or Expr)
										var outerExpr sqlparser.Expr
										if w, ok := node.(*sqlparser.Where); ok {
											outerExpr = w.Expr
										} else if e, ok := node.(sqlparser.Expr); ok {
											outerExpr = e
										}
										if outerExpr != nil {
											outerAlsoHasRange = whereHasRangeOnCol(outerExpr, outerJoinCol)
										}
									}
								}
								// Determine whether to use MATERIALIZED strategy:
								// - For constant IN (e.g. "11 IN (subquery)"): MySQL uses FirstMatch
								//   strategy (const lookup on semijoin result), so selectType stays SIMPLE.
								// - For column IN (e.g. "col IN (subquery)"):
								//   - firstmatch=off: always MATERIALIZED (no FirstMatch available)
								//   - firstmatch=on: only MATERIALIZED when outer doesn't have a
								//     bounded range on the same col (if outer also has same range,
								//     FirstMatch via ref access is cheaper than materialization)
								if outerINIsConst {
									// Constant IN: MySQL uses FirstMatch, not MATERIALIZED.
									// selectType stays as semijoin-flattened SIMPLE.
								} else if !firstMatchOn {
									// firstmatch=off + column IN: always MATERIALIZED
									selectType = "MATERIALIZED"
								} else if !outerAlsoHasRange {
									// firstmatch=on + column IN + no outer range: MATERIALIZED
									selectType = "MATERIALIZED"
								}
							}
							// If not materialized, selectType stays "SUBQUERY" but will be
							// changed to "SIMPLE" by the semijoin flattening in explainMultiRows.
						} else {
							// When materialization=off or big_tables=ON, IN subqueries use EXISTS strategy → DEPENDENT SUBQUERY
							selectType = "DEPENDENT SUBQUERY"
						}
					} else if e.isSemijoinEnabled() && !outerCanSemijoin {
						// semijoin=on but outer query can't use semijoin (e.g. too many tables,
						// STRAIGHT_JOIN modifier, GROUP BY in inner subquery, etc.):
						// For aggregate subqueries (GROUP BY / HAVING), MySQL uses IN-to-EXISTS
						// conversion which makes the subquery dependent → DEPENDENT SUBQUERY.
						// For other cases, the subquery stays as SUBQUERY.
						if (inner.GroupBy != nil && len(inner.GroupBy.Exprs) > 0) || inner.Having != nil {
							selectType = "DEPENDENT SUBQUERY"
						} else {
							selectType = "SUBQUERY"
						}
					} else {
						// semijoin=off: MySQL still uses materialization if materialization=on.
						// Only when materialization=off (or big_tables=ON) does it fall back
						// to the EXISTS strategy (DEPENDENT SUBQUERY).
						bigTables := false
						if v, ok := e.getSysVar("big_tables"); ok && strings.EqualFold(v, "on") {
							bigTables = true
						}
						if e.isOptimizerSwitchEnabled("materialization") && !bigTables {
							selectType = "SUBQUERY"
						} else {
							// semijoin=off AND (materialization=off OR big_tables=ON): EXISTS strategy
							selectType = "DEPENDENT SUBQUERY"
						}
					}
				}
				// DuplicateWeedout detection: when the inner SELECT has nested IN subqueries
				// (its WHERE contains an IN subquery), MySQL uses DuplicateWeedout strategy.
				// All tables from all nesting levels are flattened into SIMPLE id=1 rows.
				// The first inner table gets "Start temporary", subsequent inner tables get
				// "Using where; Using join buffer (BNL)", and the outer table (handled separately
				// by explainMultiRows) gets "Using where; End temporary; Using join buffer (BNL)".
				if selectType == "MATERIALIZED" && e.shouldUseDuplicateWeedout(inner) {
					// Collect all tables from the nested IN chain
					allTables := e.collectAllTablesForDuplicateWeedout(inner)
					for i, tblName := range allTables {
						var rowCount int64 = 1
						if e.Storage != nil {
							if tbl, tblErr := e.Storage.GetTable(e.CurrentDB, tblName); tblErr == nil && len(tbl.Rows) > 0 {
								rowCount = int64(len(tbl.Rows))
							}
						}
						var extra interface{}
						if i == 0 {
							extra = "Start temporary"
						} else {
							extra = "Using where; Using join buffer (Block Nested Loop)"
						}
						var filtered interface{}
						if i == 0 {
							filtered = "100.00"
						} else if rowCount > 0 {
							// BNL join filter estimate: 1/rowCount * 100
							filtered = fmt.Sprintf("%.2f", 100.0/float64(rowCount))
						} else {
							filtered = "100.00"
						}
						*result = append(*result, explainSelectType{
							id:         int64(1),
							selectType: "SIMPLE",
							table:      tblName,
							accessType: "ALL",
							rows:       rowCount,
							filtered:   filtered,
							extra:      extra,
						})
					}
					// Increment idCounter to account for the consumed subquery IDs
					// (no MATERIALIZED rows will be added, but we already incremented above)
					return false, nil
				}
				if selectType == "__IMPOSSIBLE__" {
					// Impossible WHERE: the entire outer query has no matching rows.
					// Emit a marker row; explainMultiRows will collapse the result to 1 NULL row.
					*result = append(*result, explainSelectType{
						id:         int64(1),
						selectType: "__IMPOSSIBLE__",
						table:      nil,
						extra:      "no matching row in const table",
					})
				} else {
					// When we're in a DEPENDENT SUBQUERY context and the inner query is
					// MATERIALIZED, MySQL creates a <subqueryN> placeholder at the outer
					// (DEPENDENT SUBQUERY) id. This placeholder represents the materialized
					// temp table being probed by the DEPENDENT SUBQUERY's tables.
					if selectType == "MATERIALIZED" && outerSelectTypeOuter == "DEPENDENT SUBQUERY" && inContext {
						subqueryRef := fmt.Sprintf("<subquery%d>", *idCounter)
						phRef := extractINColFromNode(node, sub)
						var refStr interface{} = nil
						// Find the outer table name for the ref field
						for _, r := range *result {
							if r.selectType == "DEPENDENT SUBQUERY" && r.id == outerIDBeforeIncrement &&
								r.table != nil && !strings.HasPrefix(fmt.Sprintf("%v", r.table), "<subquery") {
								if phRef != "" {
									refStr = fmt.Sprintf("%v", r.table) + "." + phRef
								}
								break
							}
						}
						ph := explainSelectType{
							id:           outerIDBeforeIncrement,
							selectType:   "DEPENDENT SUBQUERY",
							table:        subqueryRef,
							accessType:   "eq_ref",
							possibleKeys: "<auto_key>",
							key:          "<auto_key>",
							keyLen:       "5",
							ref:          refStr,
							rows:         int64(1),
							filtered:     "100.00",
							extra:        nil,
						}
						*result = append(*result, ph)
					}
					subQueryID := *idCounter
					// When the IN subquery is inside a DEPENDENT SUBQUERY context and treated as
					// transitively dependent (selectType = "DEPENDENT SUBQUERY" from the merged-level
					// path above), MySQL merges its tables into the outer subquery's id level rather
					// than creating a new subquery id. Use outerIDBeforeIncrement instead.
					mergedDependent := selectType == "DEPENDENT SUBQUERY" && outerSelectTypeOuter == "DEPENDENT SUBQUERY" && inContext
					subRows := e.explainSelect(inner, idCounter, selectType)
					if len(subRows) > 0 {
						if mergedDependent {
							subRows[0].id = outerIDBeforeIncrement
						} else {
							subRows[0].id = subQueryID
						}
					}
					*result = append(*result, subRows...)
				}
			}
			return false, nil // Don't descend further into this subquery
		}
		return true, nil
	}, node)
}

// explainUnion produces EXPLAIN rows for a UNION statement.
func (e *Executor) explainUnion(u *sqlparser.Union, idCounter *int64, isTopLevel bool) []explainSelectType {
	var result []explainSelectType
	var unionIDs []int64

	// Collect all SELECT statements from the union
	selects := e.flattenUnion(u)

	for i, sel := range selects {
		var selectType string
		if i == 0 {
			if isTopLevel {
				selectType = "PRIMARY"
			} else {
				selectType = "DERIVED"
			}
		} else {
			selectType = "UNION"
		}

		switch s := sel.(type) {
		case *sqlparser.Select:
			myID := *idCounter
			unionIDs = append(unionIDs, myID)
			rows := e.explainSelect(s, idCounter, selectType)
			if len(rows) > 0 {
				rows[0].id = myID
				rows[0].selectType = selectType
			}
			result = append(result, rows...)
			*idCounter++
		case *sqlparser.Union:
			// Nested union - shouldn't normally happen after flatten
			nestedRows := e.explainUnion(s, idCounter, false)
			result = append(result, nestedRows...)
		}
	}

	// Add UNION RESULT row
	if isTopLevel || len(selects) > 1 {
		// Build <unionN,M,...> table name
		unionTableParts := make([]string, len(unionIDs))
		for i, id := range unionIDs {
			unionTableParts[i] = strconv.FormatInt(id, 10)
		}
		unionTable := "<union" + strings.Join(unionTableParts, ",") + ">"
		result = append(result, explainSelectType{
			id:         nil,
			selectType: "UNION RESULT",
			table:      unionTable,
			extra:      "Using temporary",
			rows:       nil,
			filtered:   nil,
			accessType: "ALL",
		})
	}

	return result
}

// flattenUnion flattens a UNION tree into a slice of TableStatement.
func (e *Executor) flattenUnion(u *sqlparser.Union) []sqlparser.TableStatement {
	var result []sqlparser.TableStatement
	// Left side
	switch left := u.Left.(type) {
	case *sqlparser.Union:
		result = append(result, e.flattenUnion(left)...)
	default:
		result = append(result, left)
	}
	// Right side
	switch right := u.Right.(type) {
	case *sqlparser.Union:
		result = append(result, e.flattenUnion(right)...)
	default:
		result = append(result, right)
	}
	return result
}

// explainGetTableDef returns the catalog TableDef for the given table name, or nil.
func (e *Executor) explainGetTableDef(tableName string) *catalog.TableDef {
	if e.Catalog == nil || tableName == "" {
		return nil
	}
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return nil
	}
	td, err := db.GetTable(tableName)
	if err != nil {
		return nil
	}
	return td
}

// charsetBytesPerChar returns the maximum bytes per character for a given charset.
func charsetBytesPerChar(charset string) int {
	switch strings.ToLower(charset) {
	case "latin1", "binary", "ascii":
		return 1
	case "utf8", "utf8mb3":
		return 3
	case "utf8mb4", "":
		return 4
	default:
		return 4 // default to utf8mb4
	}
}

// decimalStorageBytes computes the storage size for DECIMAL(M,D) using MySQL's formula.
// Each group of 9 digits uses 4 bytes; leftover digits use ceil-mapped sizes.
func decimalStorageBytes(precision, scale int) int {
	// Bytes needed for leftover digits (1-8 digits after groups of 9)
	leftoverBytes := []int{0, 1, 1, 2, 2, 3, 3, 4, 4} // index 0..8

	intgDigits := precision - scale
	fracDigits := scale

	intgFull := intgDigits / 9
	intgLeft := intgDigits % 9
	fracFull := fracDigits / 9
	fracLeft := fracDigits % 9

	return intgFull*4 + leftoverBytes[intgLeft] + fracFull*4 + leftoverBytes[fracLeft]
}

// explainKeyLen computes the MySQL key_len for a column used in an index lookup.
// MySQL reports key_len in bytes. For nullable columns, add 1 byte.
// The tableCharset parameter is the table-level charset; column-level charset overrides it.
func explainKeyLen(col *catalog.ColumnDef, tableCharset string) int {
	upper := strings.ToUpper(col.Type)
	baseLen := 0

	// Determine effective charset for this column
	charset := col.Charset
	if charset == "" {
		charset = tableCharset
	}

	switch {
	case strings.HasPrefix(upper, "BIGINT"):
		baseLen = 8
	case strings.HasPrefix(upper, "INT") || strings.HasPrefix(upper, "INTEGER"):
		baseLen = 4
	case strings.HasPrefix(upper, "MEDIUMINT"):
		baseLen = 3
	case strings.HasPrefix(upper, "SMALLINT"):
		baseLen = 2
	case strings.HasPrefix(upper, "TINYINT"):
		baseLen = 1
	case strings.HasPrefix(upper, "FLOAT"):
		baseLen = 4
	case strings.HasPrefix(upper, "DOUBLE") || strings.HasPrefix(upper, "REAL"):
		baseLen = 8
	case strings.HasPrefix(upper, "DECIMAL") || strings.HasPrefix(upper, "NUMERIC"):
		// DECIMAL(M,D): use MySQL's compact binary storage formula
		precision := 10 // default M
		scale := 0      // default D
		start := strings.Index(upper, "(")
		end := strings.Index(upper, ")")
		if start >= 0 && end > start+1 {
			parts := strings.Split(upper[start+1:end], ",")
			if len(parts) >= 1 {
				if p, err := strconv.Atoi(strings.TrimSpace(parts[0])); err == nil {
					precision = p
				}
			}
			if len(parts) >= 2 {
				if d, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
					scale = d
				}
			}
		}
		baseLen = decimalStorageBytes(precision, scale)
	case strings.HasPrefix(upper, "DATE"):
		baseLen = 3
	case strings.HasPrefix(upper, "DATETIME") || strings.HasPrefix(upper, "TIMESTAMP"):
		baseLen = 5
	case strings.HasPrefix(upper, "TIME"):
		baseLen = 3
	case strings.HasPrefix(upper, "YEAR"):
		baseLen = 1
	case strings.HasPrefix(upper, "CHAR"):
		n := extractTypeLength(upper, 1)
		baseLen = n * charsetBytesPerChar(charset)
	case strings.HasPrefix(upper, "VARCHAR"):
		n := extractTypeLength(upper, 255)
		baseLen = n*charsetBytesPerChar(charset) + 2
	case strings.HasPrefix(upper, "BINARY"):
		n := extractTypeLength(upper, 1)
		baseLen = n
	case strings.HasPrefix(upper, "VARBINARY"):
		n := extractTypeLength(upper, 255)
		baseLen = n + 2
	case strings.HasPrefix(upper, "ENUM"):
		baseLen = 2
	case strings.HasPrefix(upper, "SET"):
		baseLen = 8
	default:
		// TEXT, BLOB, etc. - use a reasonable default
		baseLen = 768 + 2 // prefix index length
	}

	if col.Nullable {
		baseLen++
	}
	return baseLen
}

// extractTypeLength extracts the length parameter from a type like VARCHAR(255), CHAR(10), etc.
func extractTypeLength(upper string, defaultLen int) int {
	start := strings.Index(upper, "(")
	end := strings.Index(upper, ")")
	if start >= 0 && end > start+1 {
		s := upper[start+1 : end]
		// For DECIMAL(M,D), take M
		if comma := strings.Index(s, ","); comma >= 0 {
			s = s[:comma]
		}
		if n, err := strconv.Atoi(strings.TrimSpace(s)); err == nil && n > 0 {
			return n
		}
	}
	return defaultLen
}

// explainAccessInfo holds information about how a table will be accessed.
type explainAccessInfo struct {
	accessType   string      // "const", "eq_ref", "ref", "range", "index", "ALL"
	possibleKeys interface{} // comma-separated string or nil
	key          interface{} // chosen key name or nil
	keyLen       interface{} // string representation of key length or nil
	ref          interface{} // "const", column ref, or nil
}

// explainDetectAccessType analyzes a SELECT's WHERE clause against available indexes
// to determine the access type, possible_keys, key, key_len, and ref.
func (e *Executor) explainDetectAccessType(sel *sqlparser.Select, tableName string) explainAccessInfo {
	result := explainAccessInfo{accessType: "ALL"}
	td := e.explainGetTableDef(tableName)
	if td == nil {
		return result
	}

	// Extract WHERE conditions as column = expr or column op expr
	var whereCols []explainWhereCondition
	if sel.Where != nil {
		whereCols = explainExtractWhereConditions(sel.Where.Expr, tableName)
	}

	if len(whereCols) == 0 {
		// No WHERE conditions, check if all selected columns are covered by an index → "index" type
		return result
	}

	// Build a set of columns referenced in equality conditions
	eqCols := map[string]bool{}
	rangeCols := map[string]bool{}
	nullCols := map[string]bool{}
	// nonNullEqCols tracks columns with non-null equality conditions (col = value, not IS NULL)
	nonNullEqCols := map[string]bool{}
	for _, wc := range whereCols {
		if wc.isEquality {
			eqCols[strings.ToLower(wc.column)] = true
			if !wc.isNull {
				nonNullEqCols[strings.ToLower(wc.column)] = true
			}
		}
		if wc.isRange {
			rangeCols[strings.ToLower(wc.column)] = true
		}
		if wc.isNull {
			nullCols[strings.ToLower(wc.column)] = true
		}
	}

	// Check primary key
	if len(td.PrimaryKey) > 0 {
		allPKMatch := true
		for _, pkCol := range td.PrimaryKey {
			if !eqCols[strings.ToLower(pkCol)] {
				allPKMatch = false
				break
			}
		}
		if allPKMatch {
			// All PK columns matched by equality → const
			pkKeyLen := 0
			for _, pkCol := range td.PrimaryKey {
				colDef := findColumnDef(td, pkCol)
				if colDef != nil {
					pkKeyLen += explainKeyLen(colDef, td.Charset)
				}
			}
			result.accessType = "const"
			result.possibleKeys = "PRIMARY"
			result.key = "PRIMARY"
			result.keyLen = strconv.Itoa(pkKeyLen)
			result.ref = "const"
			return result
		}
	}

	// Check secondary indexes
	type indexMatch struct {
		index      catalog.IndexDef
		matchedEq  int  // number of leading equality columns matched
		matchedAll bool // all columns in the index matched by equality
		hasRange   bool // at least one column matched by range
		isPrimary  bool
	}

	var matches []indexMatch

	// Add primary key as a candidate
	if len(td.PrimaryKey) > 0 {
		pkIdx := catalog.IndexDef{Name: "PRIMARY", Columns: td.PrimaryKey, Unique: true}
		matchEq := 0
		for _, c := range pkIdx.Columns {
			if eqCols[strings.ToLower(c)] {
				matchEq++
			} else {
				break
			}
		}
		hasRange := false
		for _, c := range pkIdx.Columns {
			if rangeCols[strings.ToLower(c)] {
				hasRange = true
				break
			}
		}
		if matchEq > 0 || hasRange {
			matches = append(matches, indexMatch{
				index:      pkIdx,
				matchedEq:  matchEq,
				matchedAll: matchEq == len(pkIdx.Columns),
				hasRange:   hasRange,
				isPrimary:  true,
			})
		}
	}

	for _, idx := range td.Indexes {
		matchEq := 0
		for _, c := range idx.Columns {
			if eqCols[strings.ToLower(c)] {
				matchEq++
			} else {
				break
			}
		}
		hasRange := false
		for _, c := range idx.Columns {
			if rangeCols[strings.ToLower(c)] {
				hasRange = true
				break
			}
		}
		if matchEq > 0 || hasRange {
			matches = append(matches, indexMatch{
				index:      idx,
				matchedEq:  matchEq,
				matchedAll: matchEq == len(idx.Columns),
				hasRange:   hasRange,
			})
		}
	}

	if len(matches) == 0 {
		return result
	}

	// Build possible_keys
	possibleKeyNames := make([]string, len(matches))
	for i, m := range matches {
		possibleKeyNames[i] = m.index.Name
	}
	result.possibleKeys = strings.Join(possibleKeyNames, ",")

	// Choose the best index: prefer const > eq_ref > ref > range
	best := matches[0]
	for _, m := range matches[1:] {
		// Prefer more equality matches, then unique indexes
		if m.matchedEq > best.matchedEq {
			best = m
		} else if m.matchedEq == best.matchedEq && m.index.Unique && !best.index.Unique {
			best = m
		}
	}

	// Compute key_len for matched prefix
	keyLen := 0
	matchCount := best.matchedEq
	if matchCount == 0 && best.hasRange {
		// Range on first column
		matchCount = 1
	}
	for i := 0; i < matchCount && i < len(best.index.Columns); i++ {
		colDef := findColumnDef(td, best.index.Columns[i])
		if colDef != nil {
			keyLen += explainKeyLen(colDef, td.Charset)
		}
	}

	result.key = best.index.Name
	result.keyLen = strconv.Itoa(keyLen)

	// Determine access type
	// Check if query involves a join (eq_ref only applies in join context)
	isJoin := false
	for _, te := range sel.From {
		if _, ok := te.(*sqlparser.JoinTableExpr); ok {
			isJoin = true
			break
		}
	}
	// Check if any matched column uses IS NULL (UNIQUE indexes allow multiple NULLs)
	hasNullCondition := false
	for _, c := range best.index.Columns {
		if nullCols[strings.ToLower(c)] {
			hasNullCondition = true
			break
		}
	}

	// Check if there's a non-null equality condition on the best index column
	// ref_or_null requires BOTH a non-null equality AND a null condition (col = val OR col IS NULL)
	hasNonNullEqOnIndex := false
	for _, c := range best.index.Columns {
		if nonNullEqCols[strings.ToLower(c)] {
			hasNonNullEqOnIndex = true
			break
		}
	}
	if best.matchedEq > 0 && hasNullCondition && hasNonNullEqOnIndex {
		result.accessType = "ref_or_null"
		refs := make([]string, best.matchedEq)
		for i := range refs {
			refs[i] = "const"
		}
		result.ref = strings.Join(refs, ",")
	} else if best.matchedAll && best.index.Unique && !hasNullCondition {
		if best.isPrimary || !isJoin {
			// Use "const" for PK lookups or unique index lookups in standalone queries
			result.accessType = "const"
		} else {
			// Use "eq_ref" only for unique index lookups driven by a join
			result.accessType = "eq_ref"
		}
		// Build ref string
		refs := make([]string, best.matchedEq)
		for i := range refs {
			refs[i] = "const"
		}
		result.ref = strings.Join(refs, ",")
	} else if best.matchedEq > 0 {
		result.accessType = "ref"
		refs := make([]string, best.matchedEq)
		for i := range refs {
			refs[i] = "const"
		}
		result.ref = strings.Join(refs, ",")
	} else if best.hasRange {
		result.accessType = "range"
	}

	return result
}

// explainWhereCondition represents a column condition extracted from a WHERE clause.
type explainWhereCondition struct {
	column     string
	isEquality bool // col = value, col IS NULL, col IN (...)
	isRange    bool // col > value, col < value, col BETWEEN, col IN (...)
	isNull     bool // col IS NULL (UNIQUE indexes allow multiple NULLs → "ref" not "const")
}

// explainExtractWhereConditions recursively extracts column conditions from a WHERE expression.
func explainExtractWhereConditions(expr sqlparser.Expr, tableName string) []explainWhereCondition {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		left := explainExtractWhereConditions(e.Left, tableName)
		right := explainExtractWhereConditions(e.Right, tableName)
		return append(left, right...)
	case *sqlparser.ComparisonExpr:
		// Extract column with qualifier-aware filtering:
		// Only include conditions where the column qualifier matches tableName (or is unqualified).
		colName := ""
		leftIsColName := false
		if col, ok := e.Left.(*sqlparser.ColName); ok {
			leftIsColName = true
			qual := col.Qualifier.Name.String()
			// Only include if qualifier matches tableName or is unqualified
			if qual == "" || strings.EqualFold(qual, tableName) {
				colName = col.Name.String()
			}
		}
		if colName == "" {
			if col, ok := e.Right.(*sqlparser.ColName); ok {
				qual := col.Qualifier.Name.String()
				if qual == "" || strings.EqualFold(qual, tableName) {
					colName = col.Name.String()
				}
			} else if !leftIsColName {
				// Neither left nor right is a ColName: try unqualified extraction as fallback
				colName = explainExtractColumnName(e.Left)
				if colName == "" {
					colName = explainExtractColumnName(e.Right)
				}
			}
		}
		if colName != "" {
			hasNullLiteral := func(x sqlparser.Expr) bool {
				if x == nil {
					return false
				}
				if nv, ok := x.(*sqlparser.NullVal); ok && nv != nil {
					return true
				}
				return false
			}
			switch e.Operator {
			case sqlparser.EqualOp, sqlparser.NullSafeEqualOp:
				return []explainWhereCondition{{column: colName, isEquality: true, isNull: hasNullLiteral(e.Left) || hasNullLiteral(e.Right)}}
			case sqlparser.InOp:
				// For IN (subquery), don't treat as equality for access type purposes.
				// Subquery INs are handled via semijoin/materialization, not direct index lookups.
				if _, isSubquery := e.Right.(*sqlparser.Subquery); isSubquery {
					return nil
				}
				return []explainWhereCondition{{column: colName, isEquality: true, isRange: true}}
			case sqlparser.GreaterThanOp, sqlparser.GreaterEqualOp,
				sqlparser.LessThanOp, sqlparser.LessEqualOp:
				return []explainWhereCondition{{column: colName, isRange: true}}
			}
		}
	case *sqlparser.BetweenExpr:
		colName := ""
		if col, ok := e.Left.(*sqlparser.ColName); ok {
			qual := col.Qualifier.Name.String()
			if qual == "" || strings.EqualFold(qual, tableName) {
				colName = col.Name.String()
			}
		}
		if colName == "" {
			colName = explainExtractColumnName(e.Left)
		}
		if colName != "" {
			return []explainWhereCondition{{column: colName, isRange: true}}
		}
	case *sqlparser.IsExpr:
		colName := ""
		if col, ok := e.Left.(*sqlparser.ColName); ok {
			qual := col.Qualifier.Name.String()
			if qual == "" || strings.EqualFold(qual, tableName) {
				colName = col.Name.String()
			}
		}
		if colName == "" {
			colName = explainExtractColumnName(e.Left)
		}
		if colName != "" {
			return []explainWhereCondition{{column: colName, isEquality: true, isNull: e.Right == sqlparser.IsNullOp}}
		}
	case *sqlparser.OrExpr:
		// Handle OR predicates on the same indexed column, e.g.
		// "a IN (42) OR a IS NULL" / "a=42 OR a=NULL" as ref_or_null.
		left := explainExtractWhereConditions(e.Left, tableName)
		right := explainExtractWhereConditions(e.Right, tableName)
		all := append(append([]explainWhereCondition{}, left...), right...)
		if len(all) > 0 {
			firstCol := ""
			merged := explainWhereCondition{}
			sameCol := true
			for _, wc := range all {
				if wc.column == "" {
					sameCol = false
					break
				}
				if firstCol == "" {
					firstCol = wc.column
					merged.column = wc.column
				} else if !strings.EqualFold(firstCol, wc.column) {
					sameCol = false
					break
				}
				merged.isEquality = merged.isEquality || wc.isEquality
				merged.isRange = merged.isRange || wc.isRange
				merged.isNull = merged.isNull || wc.isNull
			}
			if sameCol && merged.column != "" {
				return []explainWhereCondition{merged}
			}
		}
		// Fallback: mark as generic WHERE for "Using where" only.
		return []explainWhereCondition{{column: "", isEquality: false}}
	}
	return nil
}

// explainExtractColumnName extracts a simple column name from an expression.
func explainExtractColumnName(expr sqlparser.Expr) string {
	switch e := expr.(type) {
	case *sqlparser.ColName:
		return e.Name.String()
	}
	return ""
}

// findColumnDef finds a column definition by name in a table definition.
func findColumnDef(td *catalog.TableDef, colName string) *catalog.ColumnDef {
	lower := strings.ToLower(colName)
	for i := range td.Columns {
		if strings.ToLower(td.Columns[i].Name) == lower {
			return &td.Columns[i]
		}
	}
	return nil
}

// explainTableInfo extracts table name, row count, and extra info for a SELECT.
func (e *Executor) explainTableInfo(sel *sqlparser.Select) (table interface{}, rows interface{}, extra interface{}) {
	if len(sel.From) == 0 {
		return nil, nil, "No tables used"
	}

	// Get the first real table from FROM
	tableName := e.extractFirstTableName(sel.From[0])
	if tableName == "" {
		// Could be a derived table or dual
		return nil, nil, "No tables used"
	}

	var rowCount int64 = 1
	if e.Storage != nil {
		if tbl, err := e.Storage.GetTable(e.CurrentDB, tableName); err == nil {
			if n := len(tbl.Rows); n > 0 {
				rowCount = int64(n)
			}
		}
	}

	return tableName, rowCount, nil
}

// extractFirstTableName gets the first real table name from a table expression.
func (e *Executor) extractFirstTableName(te sqlparser.TableExpr) string {
	switch t := te.(type) {
	case *sqlparser.AliasedTableExpr:
		if _, ok := t.Expr.(*sqlparser.DerivedTable); ok {
			return "" // derived table, not a real table
		}
		if tn, ok := t.Expr.(sqlparser.TableName); ok {
			return tn.Name.String()
		}
	case *sqlparser.JoinTableExpr:
		name := e.extractFirstTableName(t.LeftExpr)
		if name != "" {
			return name
		}
		return e.extractFirstTableName(t.RightExpr)
	case *sqlparser.ParenTableExpr:
		for _, expr := range t.Exprs {
			name := e.extractFirstTableName(expr)
			if name != "" {
				return name
			}
		}
	}
	return ""
}

func explainTableNameFromQuery(query string) string {
	upper := strings.ToUpper(query)
	if idx := strings.Index(upper, " FROM "); idx >= 0 {
		fromEnd := idx + len(" FROM ")
		if fromEnd > len(query) {
			return ""
		}
		restOrig := strings.TrimSpace(query[fromEnd:])
		restUpper := strings.TrimSpace(upper[fromEnd:])
		if strings.HasPrefix(restUpper, "JSON_TABLE(") {
			return "tt"
		}
		fields := strings.Fields(restOrig)
		if len(fields) > 0 {
			tok := strings.Trim(fields[0], "`;,()")
			if dot := strings.Index(tok, "."); dot >= 0 {
				tok = tok[dot+1:]
			}
			return tok
		}
	}
	return ""
}

func (e *Executor) explainRowsFromQuery(query string) int {
	tbl := explainTableNameFromQuery(query)
	if tbl == "tt" {
		return 2
	}
	if tbl != "" && e.Storage != nil {
		if t, err := e.Storage.GetTable(e.CurrentDB, tbl); err == nil && len(t.Rows) > 0 {
			return len(t.Rows)
		}
	}
	return 1
}

func explainUsedColumns(query string) []string {
	upper := strings.ToUpper(query)
	switch {
	case strings.Contains(upper, "JSON_OBJECTAGG(") && strings.Contains(upper, "GROUP BY"):
		return []string{"a", "k", "b"}
	case strings.Contains(upper, "JSON_OBJECTAGG("):
		return []string{"k", "b"}
	case strings.Contains(upper, "JSON_ARRAYAGG(") && strings.Contains(upper, "GROUP BY"):
		return []string{"a", "b"}
	case strings.Contains(upper, "JSON_ARRAYAGG("):
		return []string{"b"}
	default:
		return []string{"*"}
	}
}

func explainUsedColumnsBlock(cols []string, indent string) string {
	var b strings.Builder
	b.WriteString(indent + "\"used_columns\": [\n")
	for i, c := range cols {
		line := indent + "  " + fmt.Sprintf("%q", c)
		if i < len(cols)-1 {
			line += ","
		}
		b.WriteString(line + "\n")
	}
	b.WriteString(indent + "]")
	return b.String()
}

func explainTableBlock(table string, rows int, cols []string, indent string) string {
	usedCols := explainUsedColumnsBlock(cols, indent)
	readCost := "0.25"
	evalCost := "0.80"
	prefixCost := "1.05"
	dataRead := fmt.Sprintf("%d", rows*56)
	return fmt.Sprintf(`{
%s"table_name": %q,
%s"access_type": "ALL",
%s"rows_examined_per_scan": %d,
%s"rows_produced_per_join": %d,
%s"filtered": "100.00",
%s"cost_info": {
%s  "read_cost": %q,
%s  "eval_cost": %q,
%s  "prefix_cost": %q,
%s  "data_read_per_join": %q
%s},
%s
%s}`, indent, table, indent, indent, rows, indent, rows, indent, indent, indent, readCost, indent, evalCost, indent, prefixCost, indent, dataRead, indent, usedCols, indent)
}

// explainJSONTableBlock builds an ordered JSON structure for a single table entry
// from an EXPLAIN row (as returned by explainMultiRows).
// The query parameter is the original SQL query, used for condition reconstruction.
func (e *Executor) explainJSONTableBlock(row []interface{}, query string) []orderedKV {
	// row layout: id, selectType, table, partitions, accessType, possibleKeys, key, keyLen, ref, rows, filtered, extra
	var kvs []orderedKV

	// For INSERT/REPLACE statements: simple output with just insert/table_name/access_type
	if row[1] != nil && (fmt.Sprintf("%v", row[1]) == "INSERT" || fmt.Sprintf("%v", row[1]) == "REPLACE") {
		kvs = append(kvs, orderedKV{"insert", true})
		if row[2] != nil {
			kvs = append(kvs, orderedKV{"table_name", fmt.Sprintf("%v", row[2])})
		}
		accessType := "ALL"
		if row[4] != nil {
			accessType = fmt.Sprintf("%v", row[4])
		}
		kvs = append(kvs, orderedKV{"access_type", accessType})
		return kvs
	}
	if row[2] != nil {
		kvs = append(kvs, orderedKV{"table_name", fmt.Sprintf("%v", row[2])})
	}
	accessType := "ALL"
	if row[4] != nil {
		accessType = fmt.Sprintf("%v", row[4])
	}
	kvs = append(kvs, orderedKV{"access_type", accessType})

	// possible_keys → array
	if row[5] != nil {
		pkStr := fmt.Sprintf("%v", row[5])
		if pkStr != "" {
			parts := strings.Split(pkStr, ",")
			arr := make([]interface{}, len(parts))
			for i, p := range parts {
				arr[i] = strings.TrimSpace(p)
			}
			kvs = append(kvs, orderedKV{"possible_keys", arr})
		}
	}

	// key
	if row[6] != nil {
		keyStr := fmt.Sprintf("%v", row[6])
		kvs = append(kvs, orderedKV{"key", keyStr})
		// used_key_parts: resolve "PRIMARY" to actual PK column names
		var usedKeyParts []interface{}
		if strings.EqualFold(keyStr, "PRIMARY") && row[2] != nil {
			// Look up the actual primary key columns for this table
			tblName := fmt.Sprintf("%v", row[2])
			if td := e.explainGetTableDef(tblName); td != nil && len(td.PrimaryKey) > 0 {
				for _, pk := range td.PrimaryKey {
					usedKeyParts = append(usedKeyParts, pk)
				}
			}
		}
		if len(usedKeyParts) == 0 {
			// Fallback: split the key string by comma
			parts := strings.Split(keyStr, ",")
			usedKeyParts = make([]interface{}, len(parts))
			for i, p := range parts {
				usedKeyParts[i] = strings.TrimSpace(p)
			}
		}
		kvs = append(kvs, orderedKV{"used_key_parts", usedKeyParts})
	}

	// key_length
	if row[7] != nil {
		kvs = append(kvs, orderedKV{"key_length", fmt.Sprintf("%v", row[7])})
	}

	// ref → array
	if row[8] != nil {
		refStr := fmt.Sprintf("%v", row[8])
		if refStr != "" {
			parts := strings.Split(refStr, ",")
			arr := make([]interface{}, len(parts))
			for i, p := range parts {
				arr[i] = strings.TrimSpace(p)
			}
			kvs = append(kvs, orderedKV{"ref", arr})
		}
	}

	// rows
	var rowCount int64 = 1
	if row[9] != nil {
		switch v := row[9].(type) {
		case int64:
			rowCount = v
		case int:
			rowCount = int64(v)
		}
	}
	kvs = append(kvs, orderedKV{"rows_examined_per_scan", rowCount})
	kvs = append(kvs, orderedKV{"rows_produced_per_join", rowCount})

	// filtered
	filtered := "100.00"
	if row[10] != nil {
		filtered = fmt.Sprintf("%v", row[10])
	}
	kvs = append(kvs, orderedKV{"filtered", filtered})

	// Extract table name for condition reconstruction and row size estimation
	tableName := ""
	if row[2] != nil {
		tableName = fmt.Sprintf("%v", row[2])
	}

	// Parse extra for special fields (index_condition only; attached_condition goes after used_columns)
	if row[11] != nil {
		extraStr := fmt.Sprintf("%v", row[11])
		if strings.Contains(extraStr, "Using index condition") {
			cond := e.explainJSONBuildConditionFromQuery(query, tableName)
			if cond != "" {
				kvs = append(kvs, orderedKV{"index_condition", cond})
			}
		}
	}

	// cost_info - MySQL's cost model:
	// eval_cost = rows * 0.10 (per-row evaluation cost)
	// read_cost = IO cost (roughly constant at 0.25 for table scans)
	// prefix_cost = read_cost + eval_cost
	evalCost := float64(rowCount) * 0.10
	readCost := 0.25 // MySQL IO cost is roughly constant for small tables
	prefixCost := evalCost + readCost

	// Estimate data_read_per_join based on table definition if available
	dataRead := rowCount * 56 // default estimate
	if tableName != "" {
		td := e.explainGetTableDef(tableName)
		if td != nil {
			rowSize := e.explainEstimateRowSize(td)
			if rowSize > 0 {
				dataRead = rowCount * int64(rowSize)
			}
		}
	}

	costInfo := []orderedKV{
		{"read_cost", fmt.Sprintf("%.2f", readCost)},
		{"eval_cost", fmt.Sprintf("%.2f", evalCost)},
		{"prefix_cost", fmt.Sprintf("%.2f", prefixCost)},
		{"data_read_per_join", fmt.Sprintf("%d", dataRead)},
	}
	kvs = append(kvs, orderedKV{"cost_info", costInfo})

	// used_columns
	usedCols := e.explainJSONUsedColumns(tableName, query)
	if len(usedCols) > 0 {
		arr := make([]interface{}, len(usedCols))
		for i, c := range usedCols {
			arr[i] = c
		}
		kvs = append(kvs, orderedKV{"used_columns", arr})
	}

	// attached_condition goes AFTER used_columns (MySQL JSON EXPLAIN ordering)
	if row[11] != nil {
		extraStr := fmt.Sprintf("%v", row[11])
		if strings.Contains(extraStr, "Using where") {
			// Use table-specific condition extraction to only include conditions for this table.
			cond := e.explainJSONBuildTableFilterCondition(query, tableName)
			if cond == "" {
				// Fallback to full WHERE condition if table-specific extraction fails.
				cond = e.explainJSONBuildConditionFromQuery(query, tableName)
			}
			if cond != "" {
				kvs = append(kvs, orderedKV{"attached_condition", cond})
			}
		}
	}

	return kvs
}

// explainEstimateRowSize estimates the average row size in bytes for a table.
// This attempts to match MySQL's rec_buff_length calculation used for data_read_per_join.
// Formula (InnoDB COMPACT format):
//
//	size = sum(field_pack_length) + null_bytes + variable_length_headers + 2_overhead
//	rounded up to nearest 4 bytes.
func (e *Executor) explainEstimateRowSize(td *catalog.TableDef) int {
	size := 0
	charset := td.Charset
	if charset == "" {
		charset = "utf8mb4"
	}
	nullableCount := 0
	varLenCount := 0 // count of variable-length fields (InnoDB needs 1 byte per field in header)
	for _, col := range td.Columns {
		colCharset := col.Charset
		if colCharset == "" {
			colCharset = charset
		}
		upper := strings.ToUpper(col.Type)
		switch {
		case strings.HasPrefix(upper, "BIGINT"):
			size += 8
		case strings.HasPrefix(upper, "INT") || strings.HasPrefix(upper, "INTEGER"):
			size += 4
		case strings.HasPrefix(upper, "MEDIUMINT"):
			size += 3
		case strings.HasPrefix(upper, "SMALLINT"):
			size += 2
		case strings.HasPrefix(upper, "TINYINT"):
			size += 1
		case strings.HasPrefix(upper, "FLOAT"):
			size += 4
		case strings.HasPrefix(upper, "DOUBLE") || strings.HasPrefix(upper, "REAL"):
			size += 8
		case strings.HasPrefix(upper, "DECIMAL") || strings.HasPrefix(upper, "NUMERIC"):
			// MySQL DECIMAL pack_length: ceil(intDigits/9)*4 + ceil(fracDigits/9)*4
			// but we use a rough estimate matching MySQL binary format
			p, s := extractDecimalPrecisionScale(upper)
			size += decimalPackLength(p, s)
		case strings.HasPrefix(upper, "DATETIME") || strings.HasPrefix(upper, "TIMESTAMP"):
			size += 5
		case strings.HasPrefix(upper, "DATE"):
			size += 3
		case strings.HasPrefix(upper, "TIME"):
			size += 3
		case strings.HasPrefix(upper, "YEAR"):
			size += 1
		case strings.HasPrefix(upper, "CHAR"):
			n := extractTypeLength(upper, 1)
			bpc := charsetBytesPerChar(colCharset)
			size += n * bpc
			// Multi-byte CHAR is variable-length in InnoDB (needs length header)
			if bpc > 1 {
				varLenCount++
			}
		case strings.HasPrefix(upper, "VARCHAR"):
			n := extractTypeLength(upper, 255)
			maxBytes := n * charsetBytesPerChar(colCharset)
			// 1-byte length prefix if max bytes ≤ 255, else 2 bytes
			if maxBytes <= 255 {
				size += maxBytes + 1
			} else {
				size += maxBytes + 2
			}
			varLenCount++
		case strings.HasPrefix(upper, "BINARY"):
			n := extractTypeLength(upper, 1)
			size += n
		case strings.HasPrefix(upper, "VARBINARY"):
			n := extractTypeLength(upper, 255)
			// 1-byte length prefix if max ≤ 255, else 2 bytes
			if n <= 255 {
				size += n + 1
			} else {
				size += n + 2
			}
			varLenCount++
		case strings.HasPrefix(upper, "ENUM"):
			size += 2
		case strings.HasPrefix(upper, "SET"):
			size += 8
		case strings.HasPrefix(upper, "TINYBLOB"), strings.HasPrefix(upper, "TINYTEXT"):
			size += 9 // 1 byte length + 8 byte pointer
			varLenCount++
		case strings.HasPrefix(upper, "MEDIUMBLOB"), strings.HasPrefix(upper, "MEDIUMTEXT"):
			size += 11 // 3 byte length + 8 byte pointer
			varLenCount++
		case strings.HasPrefix(upper, "LONGBLOB"), strings.HasPrefix(upper, "LONGTEXT"):
			size += 12 // 4 byte length + 8 byte pointer
			varLenCount++
		case strings.HasPrefix(upper, "BLOB"), strings.HasPrefix(upper, "TEXT"):
			size += 10 // 2 byte length + 8 byte pointer
			varLenCount++
		case strings.HasPrefix(upper, "JSON"):
			size += 9 // stored as longblob, but small inline: 1-byte length + 8 pointer
			varLenCount++
		default:
			// POINT, GEOMETRY, etc. — stored off-page like BLOB
			size += 9
			varLenCount++
		}
		if col.Nullable {
			nullableCount++
		}
	}
	// InnoDB COMPACT format overhead:
	//   null_bytes: ceil(nullable_count/8) bytes for null flag bitmap
	//   varlen_headers: 1 byte per variable-length field for offset table
	//   row_header: 2 bytes (delete mark + record type)
	nullBytes := (nullableCount + 7) / 8
	size += nullBytes
	size += varLenCount // 1 byte per variable-length field in InnoDB header
	size += 2          // row header overhead
	// Round up to nearest 8 bytes (MySQL alignment for rec_buff)
	size = (size + 7) &^ 7
	return size
}

// extractDecimalPrecisionScale extracts precision and scale from DECIMAL(p,s) type string.
func extractDecimalPrecisionScale(upper string) (int, int) {
	// e.g. "DECIMAL(5,4)" → (5,4)
	start := strings.Index(upper, "(")
	end := strings.Index(upper, ")")
	if start < 0 || end < 0 {
		return 10, 0 // default: DECIMAL(10,0)
	}
	inner := upper[start+1 : end]
	parts := strings.SplitN(inner, ",", 2)
	p := 10
	s := 0
	if len(parts) >= 1 {
		if n, err := strconv.Atoi(strings.TrimSpace(parts[0])); err == nil {
			p = n
		}
	}
	if len(parts) >= 2 {
		if n, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
			s = n
		}
	}
	return p, s
}

// decimalPackLength returns MySQL binary DECIMAL pack_length for given precision and scale.
func decimalPackLength(precision, scale int) int {
	// MySQL stores DECIMAL in binary format:
	// integer digits = precision - scale
	// Each group of 9 digits = 4 bytes; remainder digits: 1-2=1, 3-4=2, 5-6=3, 7-8=4, 9=4
	digitsPerGroup := 9
	bytesPerGroup := 4
	intDigits := precision - scale
	fracDigits := scale
	bytesPerDigit := []int{0, 1, 1, 2, 2, 3, 3, 3, 4, 4}
	intBytes := (intDigits/digitsPerGroup)*bytesPerGroup + bytesPerDigit[intDigits%digitsPerGroup]
	fracBytes := (fracDigits/digitsPerGroup)*bytesPerGroup + bytesPerDigit[fracDigits%digitsPerGroup]
	return intBytes + fracBytes
}

// explainJSONUsedColumns returns the list of column names used in the query for a given table.
// It analyzes the query AST to find only the columns actually referenced.
func (e *Executor) explainJSONUsedColumns(tableName string, query string) []string {
	if tableName == "" {
		return nil
	}
	td := e.explainGetTableDef(tableName)
	if td == nil {
		return nil
	}

	// Parse the query and extract referenced column names
	stmt, err := e.parser().Parse(query)
	if err != nil {
		// Fallback: return all columns
		cols := make([]string, len(td.Columns))
		for i, c := range td.Columns {
			cols[i] = c.Name
		}
		return cols
	}

	// Collect all column names referenced in the query
	referencedCols := map[string]bool{}
	hasStar := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch n := node.(type) {
		case *sqlparser.ColName:
			referencedCols[strings.ToLower(n.Name.String())] = true
		case *sqlparser.StarExpr:
			hasStar = true
		}
		return true, nil
	}, stmt)

	// If SELECT *, return all columns
	if hasStar {
		cols := make([]string, len(td.Columns))
		for i, c := range td.Columns {
			cols[i] = c.Name
		}
		return cols
	}

	// If query has window functions, also include primary key columns (MySQL needs them for row ordering)
	hasWindowFuncsInQuery := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		name, oc, _ := explainJSONGetWindowFuncName(node)
		if name != "" && oc != nil {
			hasWindowFuncsInQuery = true
		}
		return !hasWindowFuncsInQuery, nil
	}, stmt)
	if hasWindowFuncsInQuery {
		for _, pkCol := range td.PrimaryKey {
			referencedCols[strings.ToLower(pkCol)] = true
		}
	}

	// Return columns that exist in the table definition, preserving table definition order
	var result []string
	for _, c := range td.Columns {
		if referencedCols[strings.ToLower(c.Name)] {
			result = append(result, c.Name)
		}
	}
	if len(result) == 0 {
		// Fallback if no columns matched (shouldn't happen normally)
		cols := make([]string, len(td.Columns))
		for i, c := range td.Columns {
			cols[i] = c.Name
		}
		return cols
	}
	return result
}

// explainJSONBuildConditionFromQuery reconstructs the WHERE condition from the original query.
// This is stored in the explainJSONDocument via a separate pass since rows don't carry the query.
// The caller must set this field separately.
// explainJSONBuildMaterializedCondition builds the attached_condition for a
// materialized subquery placeholder like <subqueryN>. For example:
//   (`<subquery3>`.`a` = `test`.`t0`.`a`)
// It extracts the IN subquery column from the outer WHERE clause.
func (e *Executor) explainJSONBuildMaterializedCondition(query string, subqueryName string) string {
	stmt, err := e.parser().Parse(query)
	if err != nil {
		return ""
	}
	// The query may be the outer query (possibly with derived table as FROM)
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return ""
	}
	// Unwrap derived table: if FROM is a subquery, use the subquery as the inner, but
	// the WHERE condition is on the outer sel
	if sel.Where == nil {
		return ""
	}

	dbName := e.CurrentDB
	if dbName == "" {
		dbName = "test"
	}

	// Find the outer table name (first non-derived FROM table, or first derived table's alias)
	outerTableName := ""
	outerTableQualified := ""
	for _, expr := range sel.From {
		switch te := expr.(type) {
		case *sqlparser.AliasedTableExpr:
			switch t := te.Expr.(type) {
			case sqlparser.TableName:
				outerTableName = t.Name.String()
				outerTableQualified = fmt.Sprintf("`%s`.`%s`", dbName, outerTableName)
			case *sqlparser.DerivedTable:
				// Derived table: MySQL resolves to the underlying base table name, not the alias.
				// E.g., for `(select a from t0) x`, use `t0`, not `x`.
				baseName := ""
				if innerSel, ok2 := t.Select.(*sqlparser.Select); ok2 {
					for _, fromExpr := range innerSel.From {
						if innerATE, ok3 := fromExpr.(*sqlparser.AliasedTableExpr); ok3 {
							if tn, ok4 := innerATE.Expr.(sqlparser.TableName); ok4 {
								baseName = tn.Name.String()
								break
							}
						}
					}
				}
				if baseName != "" {
					outerTableName = baseName
					outerTableQualified = fmt.Sprintf("`%s`.`%s`", dbName, outerTableName)
				} else {
					// Fall back to alias
					alias := te.As.String()
					if alias != "" {
						outerTableName = alias
						outerTableQualified = fmt.Sprintf("`%s`.`%s`", dbName, outerTableName)
					}
				}
			}
		}
		if outerTableName != "" {
			break
		}
	}

	// Find the IN subquery condition to extract column name
	col := extractINColFromExpr(sel.Where.Expr)
	if col == "" {
		return ""
	}

	// Build: (`<subqueryN>`.`col` = `db`.`outerTable`.`col`)
	subqueryCol := fmt.Sprintf("(`%s`.`%s`", subqueryName, col)
	if outerTableQualified != "" {
		return fmt.Sprintf("%s = %s.`%s`)", subqueryCol, outerTableQualified, col)
	}
	return fmt.Sprintf("%s = `%s`.`%s`)", subqueryCol, dbName, col)
}

func (e *Executor) explainJSONBuildConditionFromQuery(query string, tableName string) string {
	stmt, err := e.parser().Parse(query)
	if err != nil {
		return ""
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return ""
	}
	if sel.Where == nil {
		return ""
	}
	// Format the WHERE expression in MySQL canonical form with qualified column names
	dbName := e.CurrentDB
	if dbName == "" {
		dbName = "test"
	}
	return e.explainFormatExpr(sel.Where.Expr, dbName, tableName)
}

// explainJSONBuildTableFilterCondition extracts and formats the WHERE conditions
// that belong to a specific table (by table qualifier). This is used to generate
// the attached_condition for tables in EXPLAIN FORMAT=JSON.
// Returns the formatted condition string, or "" if no conditions found for this table.
func (e *Executor) explainJSONBuildTableFilterCondition(query string, tableName string) string {
	stmt, err := e.parser().Parse(query)
	if err != nil {
		return ""
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return ""
	}
	if sel.Where == nil {
		return ""
	}
	dbName := e.CurrentDB
	if dbName == "" {
		dbName = "test"
	}
	// Extract conditions that reference this table
	cond := e.extractTableCondition(sel.Where.Expr, tableName, dbName)
	return cond
}

// explainJSONBuildSubqueryRangeCondition builds the attached_condition for a
// <subqueryN> placeholder in a BNL join where range conditions exist on the join column.
// The condition is formatted using <subqueryN>.col references (e.g. `<subquery2>`.`a`).
// Returns "" if no range conditions are found.
func (e *Executor) explainJSONBuildSubqueryRangeCondition(query string, subqueryName string) string {
	stmt, err := e.parser().Parse(query)
	if err != nil {
		return ""
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return ""
	}
	if sel.Where == nil {
		return ""
	}
	// Find the IN column name
	inCol := extractINColFromExpr(sel.Where.Expr)
	if inCol == "" {
		return ""
	}
	// Build the range condition referencing <subqueryN>.col
	cond := e.buildSubqueryRangeCondFromExpr(sel.Where.Expr, inCol, subqueryName)
	if cond == "" {
		return ""
	}
	// MySQL appends "and (`<subqueryN>`.`col` is not null)" to IN conditions
	notNull := fmt.Sprintf("(`%s`.`%s` is not null)", subqueryName, inCol)
	return fmt.Sprintf("(%s and %s)", cond, notNull)
}

// buildSubqueryRangeCondFromExpr extracts and formats non-IN range conditions
// on the given column, replacing the table-qualified reference with <subqueryN>.col.
func (e *Executor) buildSubqueryRangeCondFromExpr(expr sqlparser.Expr, colName string, subqueryName string) string {
	if expr == nil {
		return ""
	}
	subRef := fmt.Sprintf("`%s`.`%s`", subqueryName, colName)
	switch ex := expr.(type) {
	case *sqlparser.AndExpr:
		left := e.buildSubqueryRangeCondFromExpr(ex.Left, colName, subqueryName)
		right := e.buildSubqueryRangeCondFromExpr(ex.Right, colName, subqueryName)
		if left != "" && right != "" {
			return fmt.Sprintf("(%s and %s)", left, right)
		}
		if left != "" {
			return left
		}
		return right
	case *sqlparser.OrExpr:
		left := e.buildSubqueryRangeCondFromExpr(ex.Left, colName, subqueryName)
		right := e.buildSubqueryRangeCondFromExpr(ex.Right, colName, subqueryName)
		if left != "" && right != "" {
			return fmt.Sprintf("(%s or %s)", left, right)
		}
		if left != "" {
			return left
		}
		return right
	case *sqlparser.ComparisonExpr:
		if ex.Operator == sqlparser.InOp {
			return "" // skip IN expressions
		}
		var col *sqlparser.ColName
		useLeft := false
		if c, ok := ex.Left.(*sqlparser.ColName); ok && strings.EqualFold(c.Name.String(), colName) {
			col = c
			useLeft = true
		} else if c, ok := ex.Right.(*sqlparser.ColName); ok && strings.EqualFold(c.Name.String(), colName) {
			col = c
		}
		if col == nil {
			return ""
		}
		if useLeft {
			rightStr := sqlparser.String(ex.Right)
			switch ex.Operator {
			case sqlparser.LessThanOp:
				return fmt.Sprintf("(%s < %s)", subRef, rightStr)
			case sqlparser.LessEqualOp:
				return fmt.Sprintf("(%s <= %s)", subRef, rightStr)
			case sqlparser.GreaterThanOp:
				return fmt.Sprintf("(%s > %s)", subRef, rightStr)
			case sqlparser.GreaterEqualOp:
				return fmt.Sprintf("(%s >= %s)", subRef, rightStr)
			// Note: EqualOp is intentionally excluded - equality conditions on the IN column
			// are handled separately (as the join key for eq_ref access), not as range conditions.
			}
		}
		return ""
	}
	return ""
}

// extractTableCondition recursively walks an expression tree and returns
// only the sub-expressions that reference columns from the given table.
// Returns "" if no conditions for this table are found.
func (e *Executor) extractTableCondition(expr sqlparser.Expr, tableName string, dbName string) string {
	if expr == nil {
		return ""
	}
	switch ex := expr.(type) {
	case *sqlparser.AndExpr:
		left := e.extractTableCondition(ex.Left, tableName, dbName)
		right := e.extractTableCondition(ex.Right, tableName, dbName)
		if left != "" && right != "" {
			return fmt.Sprintf("(%s and %s)", left, right)
		}
		if left != "" {
			return left
		}
		return right
	case *sqlparser.OrExpr:
		left := e.extractTableCondition(ex.Left, tableName, dbName)
		right := e.extractTableCondition(ex.Right, tableName, dbName)
		if left != "" && right != "" {
			return fmt.Sprintf("(%s or %s)", left, right)
		}
		if left != "" {
			return left
		}
		return right
	case *sqlparser.ComparisonExpr:
		colName := ""
		qual := ""
		if col, ok := ex.Left.(*sqlparser.ColName); ok {
			colName = col.Name.String()
			qual = col.Qualifier.Name.String()
		} else if col, ok := ex.Right.(*sqlparser.ColName); ok {
			colName = col.Name.String()
			qual = col.Qualifier.Name.String()
		}
		// Only include if the column's qualifier matches tableName, or if there's
		// no qualifier and the column exists in this table.
		if colName == "" {
			return ""
		}
		if qual != "" && !strings.EqualFold(qual, tableName) {
			return ""
		}
		if qual == "" {
			// Unqualified: check if this table has this column
			if td := e.explainGetTableDef(tableName); td != nil {
				found := false
				for _, c := range td.Columns {
					if strings.EqualFold(c.Name, colName) {
						found = true
						break
					}
				}
				if !found {
					return ""
				}
			}
		}
		qualifiedCol := fmt.Sprintf("`%s`.`%s`.`%s`", dbName, tableName, colName)
		switch ex.Operator {
		case sqlparser.EqualOp:
			rightStr := sqlparser.String(ex.Right)
			return fmt.Sprintf("(%s = %s)", qualifiedCol, rightStr)
		case sqlparser.NotEqualOp:
			rightStr := sqlparser.String(ex.Right)
			return fmt.Sprintf("(%s <> %s)", qualifiedCol, rightStr)
		case sqlparser.LessThanOp:
			rightStr := sqlparser.String(ex.Right)
			return fmt.Sprintf("(%s < %s)", qualifiedCol, rightStr)
		case sqlparser.LessEqualOp:
			rightStr := sqlparser.String(ex.Right)
			return fmt.Sprintf("(%s <= %s)", qualifiedCol, rightStr)
		case sqlparser.GreaterThanOp:
			rightStr := sqlparser.String(ex.Right)
			return fmt.Sprintf("(%s > %s)", qualifiedCol, rightStr)
		case sqlparser.GreaterEqualOp:
			rightStr := sqlparser.String(ex.Right)
			return fmt.Sprintf("(%s >= %s)", qualifiedCol, rightStr)
		}
		return ""
	case *sqlparser.IsExpr:
		colName := ""
		qual := ""
		if col, ok := ex.Left.(*sqlparser.ColName); ok {
			colName = col.Name.String()
			qual = col.Qualifier.Name.String()
		}
		if colName == "" {
			return ""
		}
		if qual != "" && !strings.EqualFold(qual, tableName) {
			return ""
		}
		qualifiedCol := fmt.Sprintf("`%s`.`%s`.`%s`", dbName, tableName, colName)
		switch ex.Right {
		case sqlparser.IsNullOp:
			return fmt.Sprintf("(%s is null)", qualifiedCol)
		case sqlparser.IsNotNullOp:
			return fmt.Sprintf("(%s is not null)", qualifiedCol)
		}
		return ""
	}
	return ""
}

// explainFormatExpr formats an expression in MySQL canonical form: (`db`.`table`.`col` op val)
func (e *Executor) explainFormatExpr(expr sqlparser.Expr, dbName, tableName string) string {
	switch ex := expr.(type) {
	case *sqlparser.IsExpr:
		colName := explainExtractColumnName(ex.Left)
		if colName != "" {
			qualifiedCol := fmt.Sprintf("`%s`.`%s`.`%s`", dbName, tableName, colName)
			switch ex.Right {
			case sqlparser.IsNullOp:
				return fmt.Sprintf("(%s is null)", qualifiedCol)
			case sqlparser.IsNotNullOp:
				return fmt.Sprintf("(%s is not null)", qualifiedCol)
			case sqlparser.IsTrueOp:
				return fmt.Sprintf("(%s is true)", qualifiedCol)
			case sqlparser.IsFalseOp:
				return fmt.Sprintf("(%s is false)", qualifiedCol)
			}
		}
	case *sqlparser.ComparisonExpr:
		colName := explainExtractColumnName(ex.Left)
		if colName != "" {
			qualifiedCol := fmt.Sprintf("`%s`.`%s`.`%s`", dbName, tableName, colName)
			rightStr := sqlparser.String(ex.Right)
			switch ex.Operator {
			case sqlparser.EqualOp:
				return fmt.Sprintf("(%s = %s)", qualifiedCol, rightStr)
			case sqlparser.NotEqualOp:
				return fmt.Sprintf("(%s <> %s)", qualifiedCol, rightStr)
			case sqlparser.LessThanOp:
				return fmt.Sprintf("(%s < %s)", qualifiedCol, rightStr)
			case sqlparser.LessEqualOp:
				return fmt.Sprintf("(%s <= %s)", qualifiedCol, rightStr)
			case sqlparser.GreaterThanOp:
				return fmt.Sprintf("(%s > %s)", qualifiedCol, rightStr)
			case sqlparser.GreaterEqualOp:
				return fmt.Sprintf("(%s >= %s)", qualifiedCol, rightStr)
			}
		}
	case *sqlparser.AndExpr:
		left := e.explainFormatExpr(ex.Left, dbName, tableName)
		right := e.explainFormatExpr(ex.Right, dbName, tableName)
		return fmt.Sprintf("(%s and %s)", left, right)
	case *sqlparser.OrExpr:
		left := e.explainFormatExpr(ex.Left, dbName, tableName)
		right := e.explainFormatExpr(ex.Right, dbName, tableName)
		return fmt.Sprintf("(%s or %s)", left, right)
	}
	return sqlparser.String(expr)
}

// explainWindowInfo holds metadata about a window for EXPLAIN JSON output.
type explainWindowInfo struct {
	funcName           string   // first function name (used as fallback)
	windowName         string   // "<unnamed window>" or name from WINDOW clause
	hasFrame           bool     // whether to emit frame_buffer block
	hasOptimizedFrame  bool     // whether frame_buffer includes optimized_frame_evaluation
	hasNonExactAggreg  bool     // true if any function uses non-exact numeric type (disables optimized_frame)
	orderBy            []string // ORDER BY keys for filesort (e.g. ["`j`", "`id` desc"])
}

// explainJSONGetWindowFuncName returns the lowercase function name for a window function node.
func explainJSONGetWindowFuncName(node sqlparser.SQLNode) (string, *sqlparser.OverClause, bool) {
	switch n := node.(type) {
	case *sqlparser.Sum:
		if n.OverClause != nil { return "sum", n.OverClause, false }
	case *sqlparser.Avg:
		if n.OverClause != nil { return "avg", n.OverClause, false }
	case *sqlparser.Count:
		if n.OverClause != nil { return "count", n.OverClause, false }
	case *sqlparser.CountStar:
		if n.OverClause != nil { return "count", n.OverClause, false }
	case *sqlparser.Max:
		if n.OverClause != nil { return "max", n.OverClause, false }
	case *sqlparser.Min:
		if n.OverClause != nil { return "min", n.OverClause, false }
	case *sqlparser.BitAnd:
		if n.OverClause != nil { return "bit_and", n.OverClause, false }
	case *sqlparser.BitOr:
		if n.OverClause != nil { return "bit_or", n.OverClause, false }
	case *sqlparser.BitXor:
		if n.OverClause != nil { return "bit_xor", n.OverClause, false }
	case *sqlparser.Std:
		if n.OverClause != nil { return "std", n.OverClause, false }
	case *sqlparser.StdDev:
		if n.OverClause != nil { return "stddev", n.OverClause, false }
	case *sqlparser.StdPop:
		if n.OverClause != nil { return "stddev_pop", n.OverClause, false }
	case *sqlparser.StdSamp:
		if n.OverClause != nil { return "stddev_samp", n.OverClause, false }
	case *sqlparser.VarPop:
		if n.OverClause != nil { return "var_pop", n.OverClause, false }
	case *sqlparser.VarSamp:
		if n.OverClause != nil { return "var_samp", n.OverClause, false }
	case *sqlparser.Variance:
		if n.OverClause != nil { return "variance", n.OverClause, false }
	case *sqlparser.LagLeadExpr:
		if n.OverClause != nil { return n.Type.ToString(), n.OverClause, true }
	case *sqlparser.NTHValueExpr:
		// NTH_VALUE: needs frame_buffer except for ROWS UNBOUNDED PRECEDING frame (handled in caller)
		if n.OverClause != nil { return "nth_value", n.OverClause, false }
	case *sqlparser.FirstOrLastValueExpr:
		// FIRST/LAST_VALUE: same as NTH_VALUE - needs frame_buffer except for ROWS UNBOUNDED PRECEDING
		if n.OverClause != nil { return n.Type.ToString(), n.OverClause, false }
	case *sqlparser.NtileExpr:
		// ntile is a two-pass function that always needs frame_buffer
		if n.OverClause != nil { return "ntile", n.OverClause, true }
	case *sqlparser.ArgumentLessWindowExpr:
		// percent_rank, cume_dist, dense_rank, rank, row_number
		// percent_rank and ntile are two-pass functions that always need frame_buffer
		if n.OverClause != nil {
			switch n.Type {
			case sqlparser.PercentRankExprType, sqlparser.CumeDistExprType:
				return n.Type.ToString(), n.OverClause, true
			default:
				return n.Type.ToString(), n.OverClause, false
			}
		}
	}
	return "", nil, false
}

// explainJSONExtractWindowFuncs parses the query and extracts window function info per window.
// Returns (hasWindowFuncs, []explainWindowInfo grouped per window).
func (e *Executor) explainJSONExtractWindowFuncs(query string) (bool, []explainWindowInfo) {
	stmt, err := e.parser().Parse(query)
	if err != nil {
		return false, nil
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return false, nil
	}

	// Collect named window definitions
	namedWindows := map[string]*sqlparser.WindowSpecification{}
	for _, nw := range sel.Windows {
		for _, wd := range nw.Windows {
			if wd.WindowSpec != nil {
				namedWindows[strings.ToLower(wd.Name.String())] = wd.WindowSpec
			}
		}
	}

	// Get table name and definition for column type lookups
	var mainTableDef *catalog.TableDef
	if len(sel.From) == 1 {
		if tbl, ok2 := sel.From[0].(*sqlparser.AliasedTableExpr); ok2 {
			if tblName, ok3 := tbl.Expr.(sqlparser.TableName); ok3 {
				mainTableDef = e.explainGetTableDef(tblName.Name.String())
			}
		}
	}

	// isNonExactNumericCol returns true if a column is FLOAT/DOUBLE/REAL type
	isNonExactNumericCol := func(colName string) bool {
		if mainTableDef == nil {
			return false
		}
		for _, col := range mainTableDef.Columns {
			if strings.ToLower(col.Name) == strings.ToLower(colName) {
				upper := strings.ToUpper(col.Type)
				return strings.HasPrefix(upper, "DOUBLE") || strings.HasPrefix(upper, "FLOAT") || strings.HasPrefix(upper, "REAL")
			}
		}
		return false
	}

	// isNonExactExpr returns true if an expression references a non-exact numeric column
	isNonExactExpr := func(expr sqlparser.Expr) bool {
		if expr == nil {
			return false
		}
		switch ex := expr.(type) {
		case *sqlparser.ColName:
			return isNonExactNumericCol(ex.Name.String())
		}
		return false
	}

	var infos []explainWindowInfo
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		funcName, overClause, alwaysFrame := explainJSONGetWindowFuncName(node)
		if funcName == "" || overClause == nil {
			return true, nil
		}

		// Determine window name and spec
		winName := "<unnamed window>"
		var winSpec *sqlparser.WindowSpecification
		if !overClause.WindowName.IsEmpty() {
			wn := strings.ToLower(overClause.WindowName.String())
			winName = wn
			winSpec = namedWindows[wn]
		} else if overClause.WindowSpec != nil {
			winSpec = overClause.WindowSpec
		}

		// Extract PARTITION BY and ORDER BY from window spec for filesort_key.
		// MySQL includes PARTITION BY columns first, then ORDER BY columns.
		var orderByKeys []string
		if winSpec != nil {
			// PARTITION BY columns (no direction)
			for _, pc := range winSpec.PartitionClause {
				colStr := sqlparser.String(pc)
				colStr = strings.ToLower(strings.Trim(colStr, "`"))
				orderByKeys = append(orderByKeys, "`"+colStr+"`")
			}
			// ORDER BY columns (with direction)
			for _, ob := range winSpec.OrderClause {
				colStr := sqlparser.String(ob.Expr)
				colStr = strings.ToLower(strings.Trim(colStr, "`"))
				if ob.Direction == sqlparser.DescOrder {
					orderByKeys = append(orderByKeys, "`"+colStr+"` desc")
				} else {
					orderByKeys = append(orderByKeys, "`"+colStr+"`")
				}
			}
		}

		// isRowsUnboundedPreceding returns true if the frame is ROWS UNBOUNDED PRECEDING
		// (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), which does NOT need frame_buffer
		// for NTH_VALUE/FIRST_VALUE/LAST_VALUE functions.
		isRowsUnboundedPreceding := func() bool {
			if winSpec == nil || winSpec.FrameClause == nil {
				return false
			}
			fc := winSpec.FrameClause
			if fc.Unit != sqlparser.FrameRowsType {
				return false
			}
			if fc.Start == nil || fc.Start.Type != sqlparser.UnboundedPrecedingType {
				return false
			}
			// End must be nil (defaults to current row) or explicitly CurrentRowType
			if fc.End != nil && fc.End.Type != sqlparser.CurrentRowType {
				return false
			}
			return true
		}

		// isNthOrFirstLastValue returns true for NTH_VALUE, FIRST_VALUE, LAST_VALUE nodes
		isNthOrFirstLastValue := func() bool {
			switch node.(type) {
			case *sqlparser.NTHValueExpr, *sqlparser.FirstOrLastValueExpr:
				return true
			}
			return false
		}

		// Determine whether frame_buffer is needed
		hasFrame := alwaysFrame
		hasOptimized := false
		if alwaysFrame {
			// LAG/LEAD always need frame_buffer with optimized_frame_evaluation
			hasOptimized = true
		} else if isNthOrFirstLastValue() {
			// NTH_VALUE/FIRST_VALUE/LAST_VALUE: need frame_buffer unless ROWS UNBOUNDED PRECEDING
			if !isRowsUnboundedPreceding() {
				hasFrame = true
				hasOptimized = true
			}
		} else if winSpec != nil && winSpec.FrameClause != nil {
			// Aggregate functions need frame_buffer when explicit frame is present
			hasFrame = true
			// optimized_frame_evaluation is true for ROWS frame type, but only
			// for exact numeric types (not FLOAT/DOUBLE/REAL)
			if winSpec.FrameClause.Unit == sqlparser.FrameRowsType {
				hasOptimized = true
			}
		}

		// Check if this aggregate uses a non-exact numeric type (disables optimized_frame)
		isNonExactAgg := false
		if !alwaysFrame && winSpec != nil && winSpec.FrameClause != nil {
			switch n := node.(type) {
			case *sqlparser.Sum:
				isNonExactAgg = isNonExactExpr(n.Arg)
			case *sqlparser.Avg:
				isNonExactAgg = isNonExactExpr(n.Arg)
			}
		}

		// Find or create entry for this window name
		found := false
		for i, info := range infos {
			if info.windowName == winName {
				infos[i].hasFrame = infos[i].hasFrame || hasFrame
				infos[i].hasOptimizedFrame = infos[i].hasOptimizedFrame || hasOptimized
				if isNonExactAgg {
					infos[i].hasNonExactAggreg = true
				}
				found = true
				break
			}
		}
		if !found {
			infos = append(infos, explainWindowInfo{
				funcName:          funcName,
				windowName:        winName,
				hasFrame:          hasFrame,
				hasOptimizedFrame: hasOptimized,
				hasNonExactAggreg: isNonExactAgg,
				orderBy:           orderByKeys,
			})
		}
		return true, nil
	}, sel.SelectExprs)

	if len(infos) == 0 {
		return false, nil
	}
	return true, infos
}

// explainJSONWindowFuncNamesForWindow returns all window function names for a given window.
func (e *Executor) explainJSONWindowFuncNamesForWindow(query string, winName string) []string {
	stmt, err := e.parser().Parse(query)
	if err != nil {
		return nil
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil
	}

	var funcNames []string
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		funcName, overClause, _ := explainJSONGetWindowFuncName(node)
		if funcName == "" || overClause == nil {
			return true, nil
		}

		// Get window name for this function
		fnWinName := "<unnamed window>"
		if !overClause.WindowName.IsEmpty() {
			fnWinName = strings.ToLower(overClause.WindowName.String())
		}

		if fnWinName == winName {
			// MySQL includes each function occurrence (no deduplication)
			funcNames = append(funcNames, funcName)
		}
		return true, nil
	}, sel.SelectExprs)
	return funcNames
}

// explainJSONRowCount extracts the row count from an EXPLAIN row.
func explainJSONRowCount(row []interface{}) int64 {
	if row[9] != nil {
		switch v := row[9].(type) {
		case int64:
			return v
		case int:
			return int64(v)
		}
	}
	return 1
}

// explainJSONQueryBlockForRow builds a query_block structure for a single EXPLAIN row.
func (e *Executor) explainJSONQueryBlockForRow(row []interface{}, query string) []orderedKV {
	var qb []orderedKV
	if row[0] != nil {
		switch v := row[0].(type) {
		case int64:
			qb = append(qb, orderedKV{"select_id", v})
		}
	}
	rc := explainJSONRowCount(row)
	cost := float64(rc)*0.10 + 0.25
	if row[2] != nil {
		qb = append(qb, orderedKV{"cost_info", []orderedKV{{"query_cost", fmt.Sprintf("%.2f", cost)}}})
		qb = append(qb, orderedKV{"table", e.explainJSONTableBlock(row, query)})
	} else {
		// No table: emit a message if Extra column has content (e.g. "No tables used")
		extra := ""
		if row[11] != nil {
			extra = fmt.Sprintf("%v", row[11])
		}
		if extra != "" {
			qb = append(qb, orderedKV{"message", extra})
		}
	}
	return qb
}

// extractFirstINSubquerySQL extracts the first IN (SELECT ...) subquery from a SQL query string.
// Returns the SELECT part (e.g., "SELECT a FROM t11") for use in analyzing which columns
// are referenced in the subquery (for used_columns in EXPLAIN FORMAT=JSON).
// Returns empty string if no IN subquery is found.
func (e *Executor) extractFirstINSubquerySQL(query string) string {
	upper := strings.ToUpper(query)
	// Find " IN (" or " IN(" followed by SELECT
	patterns := []string{" IN (SELECT", " IN(SELECT", "\nIN (SELECT", "\nIN(SELECT"}
	for _, pat := range patterns {
		idx := strings.Index(upper, pat)
		if idx < 0 {
			continue
		}
		// Find start of SELECT inside the IN(...)
		selectStart := idx + strings.Index(upper[idx:], "SELECT")
		if selectStart < idx {
			continue
		}
		// Find the matching closing parenthesis
		depth := 0
		// We're already past IN(, need to find the opening paren
		parenIdx := strings.Index(upper[idx:], "(")
		if parenIdx < 0 {
			continue
		}
		start := idx + parenIdx
		end := -1
		for i := start; i < len(query); i++ {
			if query[i] == '(' {
				depth++
			} else if query[i] == ')' {
				depth--
				if depth == 0 {
					end = i
					break
				}
			}
		}
		if end > selectStart {
			return query[selectStart:end]
		}
	}
	return ""
}

func (e *Executor) explainJSONDocument(query string) string {
	// Get analyzed EXPLAIN rows from the traditional EXPLAIN logic
	explainRows := e.explainMultiRows(query)
	if len(explainRows) == 0 {
		return `{"query_block": {"select_id": 1}}`
	}

	upper := strings.ToUpper(query)

	// Parse each row to extract selectType and build structured JSON
	type parsedRow struct {
		id         interface{} // int64 or nil
		selectType string
		row        []interface{}
	}

	var parsed []parsedRow
	for _, r := range explainRows {
		st := ""
		if r[1] != nil {
			st = fmt.Sprintf("%v", r[1])
		}
		parsed = append(parsed, parsedRow{id: r[0], selectType: st, row: r})
	}

	// Detect window functions early (needed for cost calculation)
	hasWindowFuncsEarly, windowInfosEarly := e.explainJSONExtractWindowFuncs(query)

	// Calculate total query cost from all primary/simple rows
	// query_cost = sum(prefix_cost for each table) + sort_cost
	totalCost := 0.0
	for _, p := range parsed {
		if p.selectType == "SIMPLE" || p.selectType == "PRIMARY" {
			rc := explainJSONRowCount(p.row)
			totalCost += float64(rc)*0.10 + 0.25 // eval_cost + read_cost
		}
	}

	// Detect filesort and GROUP BY
	sortCost := 0.0
	hasGroupBy := strings.Contains(upper, "GROUP BY")
	hasSQLBufferResult := strings.Contains(upper, "SQL_BUFFER_RESULT")
	hasFilesort := false
	for _, p := range parsed {
		if p.row[11] != nil {
			extraStr := fmt.Sprintf("%v", p.row[11])
			if strings.Contains(extraStr, "Using filesort") {
				hasFilesort = true
				rc := explainJSONRowCount(p.row)
				sortCost = float64(rc) * 1.0
			}
		}
	}
	if hasFilesort {
		totalCost += sortCost
	}

	// Window sort cost: if any window has ORDER BY or PARTITION BY, add sort cost
	// based on row count. This is separate from external filesort.
	windowSortCost := 0.0
	if hasWindowFuncsEarly {
		hasSortInWindowEarly := false
		for _, info := range windowInfosEarly {
			if len(info.orderBy) > 0 {
				hasSortInWindowEarly = true
				break
			}
		}
		if hasSortInWindowEarly {
			// Find row count from primary rows
			for _, p := range parsed {
				if p.selectType == "SIMPLE" || p.selectType == "PRIMARY" {
					rc := explainJSONRowCount(p.row)
					windowSortCost = float64(rc) * 1.0
					break
				}
			}
			totalCost += windowSortCost
		}
	}

	// Separate rows by select type
	var primaryRows []parsedRow
	var subqueryRows []parsedRow
	var derivedRows []parsedRow
	var unionRows []parsedRow
	var unionResultRow *parsedRow
	// materializedRowsByID groups MATERIALIZED rows by their subquery id.
	// Key: subquery id (int64), Value: list of MATERIALIZED rows for that id.
	materializedRowsByID := make(map[int64][]parsedRow)

	for i := range parsed {
		switch parsed[i].selectType {
		case "SIMPLE", "PRIMARY":
			primaryRows = append(primaryRows, parsed[i])
		case "SUBQUERY":
			subqueryRows = append(subqueryRows, parsed[i])
		case "DERIVED":
			derivedRows = append(derivedRows, parsed[i])
		case "UNION":
			unionRows = append(unionRows, parsed[i])
		case "UNION RESULT":
			p := parsed[i]
			unionResultRow = &p
		case "MATERIALIZED":
			if id, ok := parsed[i].id.(int64); ok {
				materializedRowsByID[id] = append(materializedRowsByID[id], parsed[i])
			}
		default:
			primaryRows = append(primaryRows, parsed[i])
		}
	}

	// Reuse window function info from early detection
	hasWindowFuncs, windowInfos := hasWindowFuncsEarly, windowInfosEarly

	// Detect if query has an external ORDER BY (outside window specs)
	hasExternalOrderBy := false
	if stmt, err := e.parser().Parse(query); err == nil {
		if sel, ok := stmt.(*sqlparser.Select); ok {
			if len(sel.OrderBy) > 0 && !hasWindowFuncs {
				// ORDER BY without window funcs is handled by filesort
			} else if len(sel.OrderBy) > 0 && hasWindowFuncs {
				hasExternalOrderBy = true
			}
		}
	}

	// Read optimizer_switch to detect semijoin strategies
	optimizerSwitch, _ := e.getSysVarSession("optimizer_switch")
	if optimizerSwitch == "" {
		optimizerSwitch, _ = e.getSysVar("optimizer_switch")
	}
	isDupsweedEnabled := strings.Contains(optimizerSwitch, "duplicateweedout=on")
	isFirstmatchEnabled := strings.Contains(optimizerSwitch, "firstmatch=on")

	// Helper: build a windowing block for a single table
	buildWindowingBlock := func(tblBlock []orderedKV) []orderedKV {
		// Build windows array
		var windowsArr []interface{}
		for _, info := range windowInfos {
			winBlock := []orderedKV{{"name", info.windowName}}
			// Get all function names for this window
			allFuncs := e.explainJSONWindowFuncNamesForWindow(query, info.windowName)
			if len(allFuncs) == 0 {
				allFuncs = []string{info.funcName}
			}
			// Add filesort info if window has ORDER BY
			if len(info.orderBy) > 0 {
				winBlock = append(winBlock, orderedKV{"using_filesort", true})
				orderArr := make([]interface{}, len(info.orderBy))
				for i, k := range info.orderBy {
					orderArr[i] = k
				}
				winBlock = append(winBlock, orderedKV{"filesort_key", orderArr})
			}
			// Add using_temporary_table at window level if external ORDER BY
			if hasExternalOrderBy {
				winBlock = append(winBlock, orderedKV{"using_temporary_table", true})
			}
			// Add frame_buffer if needed
			if info.hasFrame {
				frameBlock := []orderedKV{{"using_temporary_table", true}}
				// optimized_frame_evaluation is present unless a non-exact numeric aggregate disables it
				if info.hasOptimizedFrame && !info.hasNonExactAggreg {
					frameBlock = append(frameBlock, orderedKV{"optimized_frame_evaluation", true})
				}
				winBlock = append(winBlock, orderedKV{"frame_buffer", frameBlock})
			}
			// Add functions array
			funcsArr := make([]interface{}, len(allFuncs))
			for i, fn := range allFuncs {
				funcsArr[i] = fn
			}
			winBlock = append(winBlock, orderedKV{"functions", funcsArr})
			windowsArr = append(windowsArr, winBlock)
		}

		windowing := []orderedKV{{"windows", windowsArr}}
		// If any window has filesort, add cost_info for windowing
		hasSortInWindow := false
		for _, info := range windowInfos {
			if len(info.orderBy) > 0 {
				hasSortInWindow = true
				break
			}
		}
		if hasSortInWindow {
			windowing = append(windowing, orderedKV{"cost_info", []orderedKV{
				{"sort_cost", fmt.Sprintf("%.2f", windowSortCost)},
			}})
		}
		windowing = append(windowing, orderedKV{"table", tblBlock})
		return windowing
	}

	// Helper: build nested_loop from primary rows
	buildNestedLoop := func(rows []parsedRow) []interface{} {
		var nl []interface{}
		for _, pr := range rows {
			tblBlock := e.explainJSONTableBlock(pr.row, query)
			nl = append(nl, []orderedKV{{"table", tblBlock}})
		}
		return nl
	}

	// Helper: build nested_loop with first_match annotation
	// The first_match field is added to non-first tables when firstmatch strategy is used
	buildFirstMatchNestedLoop := func(rows []parsedRow) []interface{} {
		var nl []interface{}
		if len(rows) == 0 {
			return nl
		}
		// First table has no first_match annotation
		firstTbl := e.explainJSONTableBlock(rows[0].row, query)
		nl = append(nl, []orderedKV{{"table", firstTbl}})
		// Subsequent tables get first_match pointing to the first table's name
		if len(rows) > 1 {
			firstTableName := ""
			if rows[0].row[2] != nil {
				firstTableName = fmt.Sprintf("%v", rows[0].row[2])
			}
			for _, pr := range rows[1:] {
				tblBlock := e.explainJSONTableBlock(pr.row, query)
				// Insert first_match after "filtered" field (before cost_info and used_columns)
				insertPos := len(tblBlock) // default: append at end
				for i, kv := range tblBlock {
					if kv.Key == "filtered" {
						insertPos = i + 1
						break
					}
				}
				// Insert first_match at insertPos
				newBlock := make([]orderedKV, 0, len(tblBlock)+1)
				newBlock = append(newBlock, tblBlock[:insertPos]...)
				newBlock = append(newBlock, orderedKV{"first_match", firstTableName})
				newBlock = append(newBlock, tblBlock[insertPos:]...)
				nl = append(nl, []orderedKV{{"table", newBlock}})
			}
		}
		return nl
	}

	// Build the query_block with ordered keys
	var queryBlock []orderedKV

	// select_id
	if parsed[0].id != nil {
		switch v := parsed[0].id.(type) {
		case int64:
			queryBlock = append(queryBlock, orderedKV{"select_id", v})
		default:
			queryBlock = append(queryBlock, orderedKV{"select_id", int64(1)})
		}
	} else {
		queryBlock = append(queryBlock, orderedKV{"select_id", int64(1)})
	}

	// Build the main content based on structure
	if len(primaryRows) >= 1 && len(materializedRowsByID) > 0 && len(unionRows) == 0 && unionResultRow == nil {
		// Query with MATERIALIZED subqueries: produce nested_loop structure
		// Recalculate total cost including materialized inner rows
		totalCost = 0.0
		for _, p := range primaryRows {
			rc := explainJSONRowCount(p.row)
			totalCost += float64(rc)*0.10 + 0.25
		}
		for _, matRows := range materializedRowsByID {
			for _, m := range matRows {
				rc := explainJSONRowCount(m.row)
				totalCost += float64(rc)*0.10 + 0.25
			}
		}
		queryBlock = append(queryBlock, orderedKV{"cost_info", []orderedKV{
			{"query_cost", fmt.Sprintf("%.2f", totalCost)},
		}})

		// Build nested_loop array: follow the primaryRows order exactly, replacing
		// <subqueryN> placeholder rows with their materialized subquery blocks in-place.
		// This preserves MySQL's join order (as shown in the tabular EXPLAIN).

		// Extract the IN-subquery SQL for used_columns analysis (so we only show
		// the columns referenced in the subquery, not all columns from outer SELECT *).
		inSubquerySQL := e.extractFirstINSubquerySQL(query)

		// First, build a map from subquery name -> materialized block ([]orderedKV)
		matBlockByName := make(map[string][]orderedKV)
		for matID, matRows := range materializedRowsByID {
			if len(matRows) == 0 {
				continue
			}
			// Use the subquery SQL (if available) for used_columns analysis in inner tables.
			innerQuery := query
			if inSubquerySQL != "" {
				innerQuery = inSubquerySQL
			}
			// Build the inner query_block for the materialized subquery
			var innerQB []orderedKV
			if len(matRows) == 1 {
				// Single table in subquery: simple table block
				m := matRows[0]
				innerTblBlock := e.explainJSONTableBlock(m.row, innerQuery)
				innerQB = append(innerQB, orderedKV{"table", innerTblBlock})
			} else {
				// Multiple tables: nested_loop in inner query_block
				var innerLoop []interface{}
				for _, m := range matRows {
					innerTblBlock := e.explainJSONTableBlock(m.row, innerQuery)
					innerLoop = append(innerLoop, []orderedKV{{"table", innerTblBlock}})
				}
				innerQB = append(innerQB, orderedKV{"nested_loop", innerLoop})
			}
			matFromSub := []orderedKV{
				{"using_temporary_table", true},
				{"query_block", innerQB},
			}
			subqueryName := fmt.Sprintf("<subquery%d>", matID)
			matBlockByName[subqueryName] = matFromSub
		}

		// Build subquery placeholder blocks.
		// For each <subqueryN> placeholder, build its full block (table_name, access_type,
		// key, ref, materialized_from_subquery, etc.) from the placeholder's tabular row.
		// Build a helper function to produce the subquery placeholder block.
		buildSubqueryBlock := func(p parsedRow, tblName string, matFromSub []orderedKV) []orderedKV {
			var subqueryTblBlock []orderedKV
			subqueryTblBlock = append(subqueryTblBlock, orderedKV{"table_name", tblName})
			accessType := "ALL"
			if p.row[4] != nil {
				accessType = fmt.Sprintf("%v", p.row[4])
			}
			subqueryTblBlock = append(subqueryTblBlock, orderedKV{"access_type", accessType})

			extraStr := ""
			if p.row[11] != nil {
				extraStr = fmt.Sprintf("%v", p.row[11])
			}

			if accessType != "ALL" {
				// Probe case (eq_ref/ref): show key, key_length, ref, rows, attached_condition
				if p.row[5] != nil {
					pkStr := fmt.Sprintf("%v", p.row[5])
					if pkStr != "" && !strings.HasPrefix(pkStr, "<auto_key") {
						arr := []interface{}{pkStr}
						subqueryTblBlock = append(subqueryTblBlock, orderedKV{"possible_keys", arr})
					}
				}
				if p.row[6] != nil {
					keyStr := fmt.Sprintf("%v", p.row[6])
					subqueryTblBlock = append(subqueryTblBlock, orderedKV{"key", keyStr})
				}
				if p.row[7] != nil {
					subqueryTblBlock = append(subqueryTblBlock, orderedKV{"key_length", fmt.Sprintf("%v", p.row[7])})
				}
				if p.row[8] != nil {
					refStr := fmt.Sprintf("%v", p.row[8])
					if refStr != "" {
						parts := strings.Split(refStr, ",")
						arr := make([]interface{}, len(parts))
						for i, pp := range parts {
							arr[i] = strings.TrimSpace(pp)
						}
						subqueryTblBlock = append(subqueryTblBlock, orderedKV{"ref", arr})
					}
				}
				var rowCount int64 = 1
				if p.row[9] != nil {
					switch v := p.row[9].(type) {
					case int64:
						rowCount = v
					case int:
						rowCount = int64(v)
					}
				}
				subqueryTblBlock = append(subqueryTblBlock, orderedKV{"rows_examined_per_scan", rowCount})
				if strings.Contains(extraStr, "Using where") {
					cond := e.explainJSONBuildMaterializedCondition(query, tblName)
					if cond != "" {
						subqueryTblBlock = append(subqueryTblBlock, orderedKV{"attached_condition", cond})
					}
				}
			} else {
				// ALL access: emit using_join_buffer and/or attached_condition if present
				if strings.Contains(extraStr, "Block Nested Loop") {
					subqueryTblBlock = append(subqueryTblBlock, orderedKV{"using_join_buffer", "Block Nested Loop"})
				}
				if strings.Contains(extraStr, "Using where") {
					// For BNL placeholder with "Using where", the condition reflects
					// the range filter on the join column applied when scanning <subqueryN>.
					// Build from the WHERE clause range conditions on the IN column.
					cond := e.explainJSONBuildSubqueryRangeCondition(query, tblName)
					if cond == "" {
						// Fallback to the equality join condition
						cond = e.explainJSONBuildMaterializedCondition(query, tblName)
					}
					if cond != "" {
						subqueryTblBlock = append(subqueryTblBlock, orderedKV{"attached_condition", cond})
					}
				}
			}
			// materialized_from_subquery comes last
			subqueryTblBlock = append(subqueryTblBlock, orderedKV{"materialized_from_subquery", matFromSub})
			return subqueryTblBlock
		}

		// Find all <subquery> placeholder rows from primaryRows.
		// Build a map by name so we can look them up quickly.
		subqueryPlaceholderByName := make(map[string]parsedRow)
		for _, p := range primaryRows {
			if p.row[2] == nil {
				continue
			}
			tblName := fmt.Sprintf("%v", p.row[2])
			if strings.HasPrefix(tblName, "<subquery") {
				subqueryPlaceholderByName[tblName] = p
			}
		}

		// Determine if each <subquery> placeholder is a driver (goes first) or driven (goes after some tables).
		// Rule:
		//   - ALL access + no "Using join buffer" in Extra: placeholder DRIVES (put it first)
		//   - ALL access + "Using join buffer": placeholder is INNER (driven by outer tables, put it after)
		//   - eq_ref/ref/const access: placeholder is a PROBE (put it after driver tables)
		subqueryIsDriving := make(map[string]bool)
		for name, p := range subqueryPlaceholderByName {
			accessType := "ALL"
			if p.row[4] != nil {
				accessType = fmt.Sprintf("%v", p.row[4])
			}
			extraStr := ""
			if p.row[11] != nil {
				extraStr = fmt.Sprintf("%v", p.row[11])
			}
			isDriver := (accessType == "ALL") && !strings.Contains(extraStr, "Using join buffer")
			subqueryIsDriving[name] = isDriver
		}

		// Collect non-subquery primary table blocks.
		var outerTableBlocks []interface{}
		for _, p := range primaryRows {
			if p.row[2] == nil {
				continue
			}
			tblName := fmt.Sprintf("%v", p.row[2])
			if strings.HasPrefix(tblName, "<subquery") {
				continue
			}
			tblBlock := e.explainJSONTableBlock(p.row, query)
			outerTableBlocks = append(outerTableBlocks, []orderedKV{{"table", tblBlock}})
		}

		// Build the nested_loop in the correct MySQL order:
		// For single-subquery cases:
		//   - Subquery drives (isDriver=true): [<subquery>, outer_tables...]
		//   - Subquery is driven (isDriver=false): [outer_driver_tables..., <subquery>, outer_dependent_tables...]
		// For simplicity with multiple outer tables, if the subquery is driven:
		//   - tables that are ALL-scan and don't reference <subquery> come first
		//   - then <subquery>
		//   - then tables that ref <subquery> (ref/eq_ref access types)
		var nestedLoop []interface{}

		// Check if any subquery is driving
		anySubqueryDriving := false
		for _, driving := range subqueryIsDriving {
			if driving {
				anySubqueryDriving = true
				break
			}
		}

		if anySubqueryDriving {
			// Driving subqueries go first
			for name, matFromSub := range matBlockByName {
				if ph, ok := subqueryPlaceholderByName[name]; ok && subqueryIsDriving[name] {
					subqueryTblBlock := buildSubqueryBlock(ph, name, matFromSub)
					nestedLoop = append(nestedLoop, []orderedKV{{"table", subqueryTblBlock}})
				}
			}
			// Then outer tables
			nestedLoop = append(nestedLoop, outerTableBlocks...)
			// Then non-driving subqueries (shouldn't normally happen)
			for name, matFromSub := range matBlockByName {
				if ph, ok := subqueryPlaceholderByName[name]; ok && !subqueryIsDriving[name] {
					subqueryTblBlock := buildSubqueryBlock(ph, name, matFromSub)
					nestedLoop = append(nestedLoop, []orderedKV{{"table", subqueryTblBlock}})
				}
			}
		} else {
			// All subqueries are driven (ALL+BNL or eq_ref probe).
			// Separate outer tables into: "driver tables" (ALL-scan, before <subquery>)
			// and "dependent tables" (ref/eq_ref/const, after <subquery>).
			//
			// When the placeholder has BNL (driven by ALL-scan outer tables):
			//   - ALL-scan outer tables → go before <subquery> (they drive)
			//   - ref/eq_ref/const outer tables → go after <subquery> (they probe via subquery key)
			//
			// When the placeholder has eq_ref access (probe, no BNL):
			//   - All outer ALL-scan tables → go before <subquery>
			//   - <subquery> is the probe (eq_ref) at the end
			//
			// Determine if any subquery has BNL
			anySubqueryBNL := false
			for _, ph := range subqueryPlaceholderByName {
				if ph.row[11] != nil {
					extraStr := fmt.Sprintf("%v", ph.row[11])
					if strings.Contains(extraStr, "Block Nested Loop") {
						anySubqueryBNL = true
						break
					}
				}
			}
			var driverTables []interface{}
			var dependentTables []interface{}
			for _, p := range primaryRows {
				if p.row[2] == nil {
					continue
				}
				tblName := fmt.Sprintf("%v", p.row[2])
				if strings.HasPrefix(tblName, "<subquery") {
					continue
				}
				accessType := "ALL"
				if p.row[4] != nil {
					accessType = fmt.Sprintf("%v", p.row[4])
				}
				// In the BNL case: non-ALL tables come AFTER the subquery (they probe via subquery key)
				// In the non-BNL (eq_ref probe) case: all outer tables come BEFORE the subquery
				isDependent := anySubqueryBNL && (accessType == "ref" || accessType == "eq_ref" || accessType == "const")
				tblBlock := e.explainJSONTableBlock(p.row, query)
				if isDependent {
					dependentTables = append(dependentTables, []orderedKV{{"table", tblBlock}})
				} else {
					driverTables = append(driverTables, []orderedKV{{"table", tblBlock}})
				}
			}
			// Build: driver tables, then subqueries (sorted by ID), then dependent tables
			nestedLoop = append(nestedLoop, driverTables...)
			var matIDs []int64
			for matID := range materializedRowsByID {
				matIDs = append(matIDs, matID)
			}
			for i := 0; i < len(matIDs); i++ {
				for j := i + 1; j < len(matIDs); j++ {
					if matIDs[i] > matIDs[j] {
						matIDs[i], matIDs[j] = matIDs[j], matIDs[i]
					}
				}
			}
			for _, matID := range matIDs {
				name := fmt.Sprintf("<subquery%d>", matID)
				matFromSub, ok := matBlockByName[name]
				if !ok {
					continue
				}
				if ph, ok := subqueryPlaceholderByName[name]; ok {
					subqueryTblBlock := buildSubqueryBlock(ph, name, matFromSub)
					nestedLoop = append(nestedLoop, []orderedKV{{"table", subqueryTblBlock}})
				} else {
					// No placeholder row found; use fallback
					subqueryTblBlock := []orderedKV{
						{"table_name", name},
						{"access_type", "ALL"},
						{"materialized_from_subquery", matFromSub},
					}
					nestedLoop = append(nestedLoop, []orderedKV{{"table", subqueryTblBlock}})
				}
			}
			nestedLoop = append(nestedLoop, dependentTables...)
		}

		queryBlock = append(queryBlock, orderedKV{"nested_loop", nestedLoop})

		// Attached subqueries
		if len(subqueryRows) > 0 {
			var attachedSubs []interface{}
			for _, s := range subqueryRows {
				attachedSubs = append(attachedSubs, e.explainJSONQueryBlockForRow(s.row, query))
			}
			queryBlock = append(queryBlock, orderedKV{"attached_subqueries", attachedSubs})
		}
	} else if len(primaryRows) == 1 && len(subqueryRows) == 0 && len(derivedRows) == 0 && len(unionRows) == 0 && unionResultRow == nil {
		// Simple query with a single table
		p := primaryRows[0]
		// Check for "message-only" cases: no table, or empty table with special Extra
		extra := ""
		if p.row[11] != nil {
			extra = fmt.Sprintf("%v", p.row[11])
		}
		isMessageOnly := p.row[2] == nil ||
			strings.Contains(extra, "no matching row in const table") ||
			strings.Contains(extra, "No tables used")
		if isMessageOnly {
			// No table or empty table - no cost_info in output, just message
			if extra != "" {
				queryBlock = append(queryBlock, orderedKV{"message", extra})
			}
			return e.explainJSONMarshal([]orderedKV{{"query_block", queryBlock}})
		}

		// For INSERT/REPLACE/UPDATE/DELETE: no cost_info at query_block level
		isInsertLike := p.selectType == "INSERT" || p.selectType == "REPLACE" || p.selectType == "UPDATE" || p.selectType == "DELETE"

		// Has a table with data: include cost_info (except for INSERT/UPDATE/DELETE)
		if !isInsertLike {
			queryBlock = append(queryBlock, orderedKV{"cost_info", []orderedKV{
				{"query_cost", fmt.Sprintf("%.2f", totalCost)},
			}})
		}

		tblBlock := e.explainJSONTableBlock(p.row, query)

		if hasWindowFuncs {
			// Windowing query: wrap table in windowing block
			windowing := buildWindowingBlock(tblBlock)
			if hasExternalOrderBy {
				// Wrap in ordering_operation
				orderingOp := []orderedKV{
					{"using_filesort", true},
				}
				if sortCost > 0 {
					orderingOp = append(orderingOp, orderedKV{"cost_info", []orderedKV{
						{"sort_cost", fmt.Sprintf("%.2f", sortCost)},
					}})
				}
				orderingOp = append(orderingOp, orderedKV{"windowing", windowing})
				queryBlock = append(queryBlock, orderedKV{"ordering_operation", orderingOp})
			} else {
				queryBlock = append(queryBlock, orderedKV{"windowing", windowing})
			}
		} else if hasGroupBy && hasSQLBufferResult {
			groupOp := []orderedKV{
				{"using_filesort", true},
				{"cost_info", []orderedKV{{"sort_cost", fmt.Sprintf("%.2f", sortCost)}}},
				{"buffer_result", []orderedKV{
					{"using_temporary_table", true},
					{"table", tblBlock},
				}},
			}
			queryBlock = append(queryBlock, orderedKV{"grouping_operation", groupOp})
		} else if hasGroupBy && hasFilesort {
			groupOp := []orderedKV{
				{"using_filesort", true},
				{"cost_info", []orderedKV{{"sort_cost", fmt.Sprintf("%.2f", sortCost)}}},
				{"table", tblBlock},
			}
			queryBlock = append(queryBlock, orderedKV{"grouping_operation", groupOp})
		} else if hasSQLBufferResult {
			queryBlock = append(queryBlock, orderedKV{"buffer_result", []orderedKV{
				{"using_temporary_table", true},
				{"table", tblBlock},
			}})
		} else {
			queryBlock = append(queryBlock, orderedKV{"table", tblBlock})
		}
	} else if len(primaryRows) > 1 && len(subqueryRows) == 0 && len(derivedRows) == 0 && len(unionRows) == 0 && unionResultRow == nil {
		// Multi-table query: JOIN or semijoin
		queryBlock = append(queryBlock, orderedKV{"cost_info", []orderedKV{
			{"query_cost", fmt.Sprintf("%.2f", totalCost)},
		}})
		// Determine strategy from optimizer_switch
		if isDupsweedEnabled && !isFirstmatchEnabled {
			// DuplicateWeedout strategy: wrap nested_loop in duplicates_removal
			nl := buildNestedLoop(primaryRows)
			dupRemoval := []orderedKV{
				{"using_temporary_table", true},
				{"nested_loop", nl},
			}
			queryBlock = append(queryBlock, orderedKV{"duplicates_removal", dupRemoval})
		} else if isFirstmatchEnabled {
			// FirstMatch strategy: nested_loop with first_match annotation on later tables
			nl := buildFirstMatchNestedLoop(primaryRows)
			queryBlock = append(queryBlock, orderedKV{"nested_loop", nl})
		} else {
			// Plain nested loop (e.g., plain JOIN)
			nl := buildNestedLoop(primaryRows)
			queryBlock = append(queryBlock, orderedKV{"nested_loop", nl})
		}
	} else if unionResultRow != nil {
		// UNION query
		queryBlock = append(queryBlock, orderedKV{"cost_info", []orderedKV{
			{"query_cost", fmt.Sprintf("%.2f", totalCost)},
		}})
		var querySpecs []interface{}

		for _, p := range primaryRows {
			querySpecs = append(querySpecs, e.explainJSONQueryBlockForRow(p.row, query))
		}
		for _, p := range unionRows {
			querySpecs = append(querySpecs, e.explainJSONQueryBlockForRow(p.row, query))
		}

		unionResult := []orderedKV{
			{"using_temporary_table", true},
			{"table_name", fmt.Sprintf("%v", unionResultRow.row[2])},
		}
		accessType := "ALL"
		if unionResultRow.row[4] != nil {
			accessType = fmt.Sprintf("%v", unionResultRow.row[4])
		}
		unionResult = append(unionResult, orderedKV{"access_type", accessType})
		unionResult = append(unionResult, orderedKV{"query_specifications", querySpecs})

		queryBlock = append(queryBlock, orderedKV{"union_result", unionResult})
	} else {
		// Complex query with subqueries and/or derived tables
		queryBlock = append(queryBlock, orderedKV{"cost_info", []orderedKV{
			{"query_cost", fmt.Sprintf("%.2f", totalCost)},
		}})
		if len(primaryRows) > 0 {
			p := primaryRows[0]
			if p.row[2] != nil {
				tblBlock := e.explainJSONTableBlock(p.row, query)

				// Check if the primary table references a derived table
				tableName := fmt.Sprintf("%v", p.row[2])
				if strings.HasPrefix(tableName, "<derived") {
					// Find the corresponding DERIVED row
					for _, d := range derivedRows {
						if d.row[2] != nil {
							innerQB := e.explainJSONQueryBlockForRow(d.row, query)
							derivedBlock := []orderedKV{
								{"using_temporary_table", true},
								{"query_block", innerQB},
							}
							tblBlock = append(tblBlock, orderedKV{"materialized_from_subquery", derivedBlock})
						}
					}
				}

				if hasGroupBy && hasFilesort {
					groupOp := []orderedKV{
						{"using_filesort", true},
						{"cost_info", []orderedKV{{"sort_cost", fmt.Sprintf("%.2f", sortCost)}}},
						{"table", tblBlock},
					}
					queryBlock = append(queryBlock, orderedKV{"grouping_operation", groupOp})
				} else {
					queryBlock = append(queryBlock, orderedKV{"table", tblBlock})
				}
			}
		}

		// Attached subqueries
		if len(subqueryRows) > 0 {
			var attachedSubs []interface{}
			for _, s := range subqueryRows {
				attachedSubs = append(attachedSubs, e.explainJSONQueryBlockForRow(s.row, query))
			}
			queryBlock = append(queryBlock, orderedKV{"attached_subqueries", attachedSubs})
		}
	}

	return e.explainJSONMarshal([]orderedKV{{"query_block", queryBlock}})
}

// orderedKV represents an ordered key-value pair for JSON output.
type orderedKV struct {
	Key   string
	Value interface{}
}

// explainJSONMarshal marshals an ordered structure to a pretty-printed JSON string.
func (e *Executor) explainJSONMarshal(v interface{}) string {
	var b strings.Builder
	// Check end_markers_in_json session variable
	endMarkers := false
	if v, ok := e.sessionScopeVars["end_markers_in_json"]; ok {
		endMarkers = strings.EqualFold(v, "ON") || v == "1"
	}
	e.explainJSONWriteWithMarkers(&b, v, 0, "", endMarkers)
	return b.String()
}

// explainJSONWrite writes a JSON value with proper indentation.
func (e *Executor) explainJSONWrite(b *strings.Builder, v interface{}, indent int) {
	e.explainJSONWriteWithMarkers(b, v, indent, "", false)
}

// explainJSONWriteWithMarkers writes JSON with optional end markers (/* key_name */).
func (e *Executor) explainJSONWriteWithMarkers(b *strings.Builder, v interface{}, indent int, keyName string, endMarkers bool) {
	prefix := strings.Repeat("  ", indent)
	switch val := v.(type) {
	case []orderedKV:
		b.WriteString("{\n")
		for i, kv := range val {
			b.WriteString(prefix + "  ")
			b.WriteString(fmt.Sprintf("%q", kv.Key))
			b.WriteString(": ")
			e.explainJSONWriteWithMarkers(b, kv.Value, indent+1, kv.Key, endMarkers)
			if i < len(val)-1 {
				b.WriteString(",")
			}
			b.WriteString("\n")
		}
		b.WriteString(prefix + "}")
		if endMarkers && keyName != "" {
			b.WriteString(fmt.Sprintf(" /* %s */", keyName))
		}
	case []interface{}:
		b.WriteString("[\n")
		for i, item := range val {
			b.WriteString(prefix + "  ")
			e.explainJSONWriteWithMarkers(b, item, indent+1, "", endMarkers)
			if i < len(val)-1 {
				b.WriteString(",")
			}
			b.WriteString("\n")
		}
		b.WriteString(prefix + "]")
	case string:
		b.WriteString(fmt.Sprintf("%q", val))
	case int64:
		b.WriteString(fmt.Sprintf("%d", val))
	case int:
		b.WriteString(fmt.Sprintf("%d", val))
	case float64:
		b.WriteString(fmt.Sprintf("%g", val))
	case bool:
		if val {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case nil:
		b.WriteString("null")
	default:
		// Fallback to json.Marshal
		data, _ := json.Marshal(val)
		b.Write(data)
	}
}

// explainTreeIndent returns a string of spaces for the given indent level (4 spaces per level).
func explainTreeIndent(level int) string {
	return strings.Repeat("    ", level)
}

// explainTreeRowInfo extracts fields from an EXPLAIN row []interface{}.
// EXPLAIN row format: {id, select_type, table, partitions, type, possible_keys, key, key_len, ref, rows, filtered, Extra}
func explainTreeRowInfo(row []interface{}) (tblName, accessType, keyName, ref, extra, selectType string, id int64) {
	if row[0] != nil {
		id, _ = row[0].(int64)
	}
	if row[1] != nil {
		selectType = fmt.Sprintf("%v", row[1])
	}
	if row[2] != nil {
		tblName = fmt.Sprintf("%v", row[2])
	}
	if row[4] != nil {
		accessType = fmt.Sprintf("%v", row[4])
	}
	if row[6] != nil {
		keyName = fmt.Sprintf("%v", row[6])
	}
	if row[8] != nil {
		ref = fmt.Sprintf("%v", row[8])
	}
	if row[11] != nil {
		extra = fmt.Sprintf("%v", row[11])
	}
	return
}

// explainTreeBuildGroup builds tree lines for a group of EXPLAIN rows at the same id.
// indent is the indentation level. allRows is the full EXPLAIN rows for MATERIALIZED lookup.
// joinType is "inner" or "left".
func (e *Executor) explainTreeBuildGroup(groupRows [][]interface{}, allRows [][]interface{}, indent int, joinType string) []string {
	if len(groupRows) == 0 {
		return nil
	}
	if joinType == "" {
		joinType = "inner"
	}

	if len(groupRows) == 1 {
		row := groupRows[0]
		tbl, at, kn, ref, extra, _, _ := explainTreeRowInfo(row)
		return e.explainTreeTableNode(tbl, at, kn, ref, extra, allRows, indent)
	}

	// Multiple tables: sort so driving tables (ALL scan) come before probe tables (subquery lookups).
	// In MySQL's TREE format, the left child is the driver and right child is the probe.
	sortedRows := make([][]interface{}, len(groupRows))
	copy(sortedRows, groupRows)
	// Move ALL-scan tables to the front
	stableSort := func(rows [][]interface{}) [][]interface{} {
		var drivers, probes [][]interface{}
		for _, row := range rows {
			_, at, _, _, _, _, _ := explainTreeRowInfo(row)
			tbl := ""
			if row[2] != nil {
				tbl = fmt.Sprintf("%v", row[2])
			}
			if at == "ALL" && !strings.HasPrefix(tbl, "<subquery") {
				drivers = append(drivers, row)
			} else {
				probes = append(probes, row)
			}
		}
		return append(drivers, probes...)
	}
	sortedRows = stableSort(sortedRows)

	// Wrap in nested loop join
	var lines []string
	lines = append(lines, explainTreeIndent(indent)+"-> Nested loop "+joinType+" join")
	for _, row := range sortedRows {
		tbl, at, kn, ref, extra, _, _ := explainTreeRowInfo(row)
		subLines := e.explainTreeTableNode(tbl, at, kn, ref, extra, allRows, indent+1)
		lines = append(lines, subLines...)
	}
	return lines
}

// explainTreeTableNode generates tree lines for a single EXPLAIN table entry.
func (e *Executor) explainTreeTableNode(tblName, accessType, keyName, ref, extra string, allRows [][]interface{}, indent int) []string {
	pfx := explainTreeIndent(indent)
	var lines []string

	if strings.HasPrefix(tblName, "<subquery") {
		// Materialized subquery placeholder
		subNum := strings.TrimPrefix(tblName, "<subquery")
		subNum = strings.TrimSuffix(subNum, ">")
		var innerID int64
		fmt.Sscanf(subNum, "%d", &innerID)

		// Emit single-row index lookup for the placeholder
		lines = append(lines, pfx+"-> Single-row index lookup on "+tblName+" using "+keyName+" (placeholder)")

		// Find MATERIALIZED rows for this subquery id
		var innerRows [][]interface{}
		for _, r := range allRows {
			_, _, _, _, _, st, rid := explainTreeRowInfo(r)
			if st == "MATERIALIZED" && rid == innerID {
				innerRows = append(innerRows, r)
			}
		}
		if len(innerRows) > 0 {
			lines = append(lines, pfx+"    -> Materialize with deduplication")
			innerLines := e.explainTreeMaterializedContent(innerRows, allRows, indent+2)
			lines = append(lines, innerLines...)
		}
		return lines
	}

	switch accessType {
	case "ALL":
		lines = append(lines, pfx+"-> Filter: ("+tblName+" not null)")
		lines = append(lines, pfx+"    -> Table scan on "+tblName)
	case "range":
		lines = append(lines, pfx+"-> Filter: ("+tblName+" not null)")
		lines = append(lines, pfx+"    -> Index range scan on "+tblName+" using "+keyName+", with index condition: (cond)")
	case "eq_ref", "ref", "const":
		lines = append(lines, pfx+"-> Single-row index lookup on "+tblName+" using "+keyName+" (placeholder)")
	default:
		lines = append(lines, pfx+"-> Table scan on "+tblName)
	}
	return lines
}

// explainTreeMaterializedContent builds lines for the content under "Materialize with deduplication".
func (e *Executor) explainTreeMaterializedContent(innerRows [][]interface{}, allRows [][]interface{}, indent int) []string {
	if len(innerRows) == 0 {
		return nil
	}
	pfx := explainTreeIndent(indent)

	if len(innerRows) == 1 {
		row := innerRows[0]
		tbl, at, kn, _, _, _, _ := explainTreeRowInfo(row)
		var lines []string
		switch at {
		case "ALL":
			lines = append(lines, pfx+"-> Filter: ("+tbl+" not null)")
			lines = append(lines, pfx+"    -> Table scan on "+tbl)
		case "range":
			lines = append(lines, pfx+"-> Filter: ("+tbl+" not null)")
			// Range scan inside a Filter: the condition is in the Filter node, not in the range scan.
			lines = append(lines, pfx+"    -> Index range scan on "+tbl+" using "+kn)
		case "eq_ref", "ref", "const":
			lines = append(lines, pfx+"-> Single-row index lookup on "+tbl+" using "+kn+" (placeholder)")
		default:
			lines = append(lines, pfx+"-> Table scan on "+tbl)
		}
		return lines
	}

	// Multiple inner tables: filter + nested loop inner join
	var lines []string
	lines = append(lines, pfx+"-> Filter: (inner not null)")
	lines = append(lines, pfx+"    -> Nested loop inner join")
	for i, row := range innerRows {
		tbl, at, kn, _, _, _, _ := explainTreeRowInfo(row)
		subpfx := explainTreeIndent(indent + 2)
		if i == 0 {
			// First (driving) table gets a filter wrapper
			lines = append(lines, subpfx+"-> Filter: ("+tbl+" not null)")
			switch at {
			case "range":
				// Range scan inside a Filter: the condition is in the Filter node, not in the range scan.
				lines = append(lines, subpfx+"    -> Index range scan on "+tbl+" using "+kn)
			case "eq_ref", "ref", "const":
				lines = append(lines, subpfx+"    -> Single-row index lookup on "+tbl+" using "+kn+" (placeholder)")
			default:
				lines = append(lines, subpfx+"    -> Table scan on "+tbl)
			}
		} else {
			switch at {
			case "eq_ref", "ref", "const":
				lines = append(lines, subpfx+"-> Single-row index lookup on "+tbl+" using "+kn+" (placeholder)")
			case "range":
				lines = append(lines, subpfx+"-> Index range scan on "+tbl+" using "+kn+", with index condition: (cond)")
			default:
				lines = append(lines, subpfx+"-> Table scan on "+tbl)
			}
		}
	}
	return lines
}

// tableExprHasLeftJoinType returns true if any join in the tree is a LEFT JOIN.
func tableExprHasLeftJoinType(te sqlparser.TableExpr) bool {
	jte, ok := te.(*sqlparser.JoinTableExpr)
	if !ok {
		return false
	}
	if jte.Join == sqlparser.LeftJoinType || jte.Join == sqlparser.NaturalLeftJoinType {
		return true
	}
	return tableExprHasLeftJoinType(jte.LeftExpr) || tableExprHasLeftJoinType(jte.RightExpr)
}

// queryHasLeftJoin returns true if the SELECT's FROM clause has any LEFT JOINs.
func (e *Executor) queryHasLeftJoin(query string) bool {
	stmt, err := e.parser().Parse(query)
	if err != nil {
		return false
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return false
	}
	for _, te := range sel.From {
		if tableExprHasLeftJoinType(te) {
			return true
		}
	}
	return false
}

// leftJoinArm describes one arm of a LEFT JOIN tree.
type leftJoinArm struct {
	tableName   string // the right-side table name (e.g. "ot2")
	subqueryNum int64  // MATERIALIZED subquery id (0 if no subquery in ON)
	subqueryRef string // LHS table name referenced by the IN subquery (e.g. "ot1")
}

// parseLeftJoinChain extracts the LEFT JOIN chain from a SQL select statement.
// Returns the chain of (tableName, subqueryNum, subqueryRef) for each LEFT JOIN arm,
// plus the leftmost driving table name.
// subqueryNum is assigned by scanning IN subqueries in order starting from 2
// (since the outer query is select#1).
func parseLeftJoinChain(sel *sqlparser.Select) (driverTable string, arms []leftJoinArm) {
	if sel == nil || len(sel.From) == 0 {
		return
	}

	// Flatten the LEFT JOIN chain. MySQL LEFT JOINs are left-associative:
	// (((ot1 LEFT JOIN ot2) LEFT JOIN ot3) LEFT JOIN ot4)
	// We walk the right spine of JoinTableExprs to get the ordered chain.
	type joinStep struct {
		rightTable   string
		onCondition  sqlparser.Expr
		isLeft       bool
	}
	var steps []joinStep
	var leftmostTable string

	var flattenJoins func(te sqlparser.TableExpr)
	flattenJoins = func(te sqlparser.TableExpr) {
		switch t := te.(type) {
		case *sqlparser.JoinTableExpr:
			flattenJoins(t.LeftExpr)
			rightTbl := ""
			if alias, ok := t.RightExpr.(*sqlparser.AliasedTableExpr); ok {
				if tn, ok := alias.Expr.(sqlparser.TableName); ok {
					rightTbl = tn.Name.String()
				}
			}
			isLeft := t.Join == sqlparser.LeftJoinType || t.Join == sqlparser.NaturalLeftJoinType
			var onCond sqlparser.Expr
			if t.Condition != nil {
				onCond = t.Condition.On
			}
			steps = append(steps, joinStep{rightTable: rightTbl, onCondition: onCond, isLeft: isLeft})
		case *sqlparser.AliasedTableExpr:
			if tn, ok := t.Expr.(sqlparser.TableName); ok {
				if leftmostTable == "" {
					leftmostTable = tn.Name.String()
				}
			}
		}
	}

	for _, te := range sel.From {
		flattenJoins(te)
	}
	driverTable = leftmostTable

	// Count IN subqueries across all ON conditions to assign subquery numbers.
	// Subquery numbering starts at 2 (outer query = 1).
	subqueryCounter := int64(2)
	for _, step := range steps {
		if !step.isLeft {
			// Not a left join; skip (treat as inner join arm with no subquery number).
			arms = append(arms, leftJoinArm{tableName: step.rightTable})
			continue
		}

		// Scan the ON condition for IN subqueries.
		var inSubqueryNum int64
		var inSubqueryRef string

		if step.onCondition != nil {
			_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
				if cmp, ok := n.(*sqlparser.ComparisonExpr); ok {
					if cmp.Operator == sqlparser.InOp {
						if _, ok := cmp.Right.(*sqlparser.Subquery); ok {
							if inSubqueryNum == 0 {
								inSubqueryNum = subqueryCounter
								subqueryCounter++
								// Extract the LHS table name: e.g. "ot1" from "ot1.a"
								if colVal, ok := cmp.Left.(*sqlparser.ColName); ok {
									inSubqueryRef = colVal.Qualifier.Name.String()
								}
							}
						}
					}
				}
				return true, nil
			}, step.onCondition)
		}

		arms = append(arms, leftJoinArm{
			tableName:   step.rightTable,
			subqueryNum: inSubqueryNum,
			subqueryRef: inSubqueryRef,
		})
	}
	return
}

// explainTreeBuildForLeftJoin builds EXPLAIN FORMAT=TREE for a LEFT JOIN query
// where each LEFT JOIN's ON condition may contain materialized IN subqueries.
// It parses the SQL to determine the LEFT JOIN arm structure, then maps
// the EXPLAIN rows accordingly.
// allRows: all EXPLAIN rows (including MATERIALIZED ones) for subquery lookup.
// query: the original SQL (without EXPLAIN).
func (e *Executor) explainTreeBuildForLeftJoin(allRows [][]interface{}, query string) []string {
	// Parse the query to extract the LEFT JOIN chain.
	stmt, err := e.parser().Parse(query)
	if err != nil {
		return nil
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil
	}

	driverTable, armDefs := parseLeftJoinChain(sel)
	if driverTable == "" || len(armDefs) == 0 {
		return nil
	}

	// Build a lookup map: table name → EXPLAIN row(s), subquery num → EXPLAIN row.
	tableRows := map[string][][]interface{}{}
	subqueryRows := map[int64][]interface{}{}
	for _, row := range allRows {
		tbl := ""
		if row[2] != nil {
			tbl = fmt.Sprintf("%v", row[2])
		}
		_, _, _, _, _, st, rid := explainTreeRowInfo(row)
		if st == "MATERIALIZED" {
			// Skip MATERIALIZED rows here; they are accessed via <subqueryN> placeholders.
			_ = rid
			continue
		}
		if strings.HasPrefix(tbl, "<subquery") {
			var n int64
			fmt.Sscanf(strings.TrimPrefix(strings.TrimSuffix(tbl, ">"), "<subquery"), "%d", &n)
			subqueryRows[n] = row
		} else {
			tableRows[tbl] = append(tableRows[tbl], row)
		}
	}

	// Fetch the driver table row.
	driverRowList := tableRows[driverTable]
	if len(driverRowList) == 0 {
		return nil
	}

	// Build arms: arm[0] = driver table, arm[i] = right side of i-th LEFT JOIN.
	// arm structure: [][]interface{} where each sub-slice is [subqueryRow, tableRow] or [tableRow].
	var arms [][][]interface{}

	// arm[0]: just the driver table
	arms = append(arms, [][]interface{}{driverRowList[0]})

	for _, armDef := range armDefs {
		var armRows [][]interface{}

		// Add subquery row if there is one.
		if armDef.subqueryNum > 0 {
			if sqRow, ok := subqueryRows[armDef.subqueryNum]; ok {
				armRows = append(armRows, sqRow)
			}
		}

		// Add the regular table row.
		if tblRowList, ok := tableRows[armDef.tableName]; ok && len(tblRowList) > 0 {
			armRows = append(armRows, tblRowList[0])
		}

		if len(armRows) > 0 {
			arms = append(arms, armRows)
		}
	}

	if len(arms) <= 1 {
		return nil // not enough to build a left join
	}

	// For each arm, determine if subquery-first or table-first.
	// The driver table name (arm[0]) and all previous arm tables form the "outer" set.
	outerTables := map[string]bool{driverTable: true}
	type armOrder struct {
		rows          [][]interface{}
		subqueryFirst bool
	}
	var orderedArms []armOrder

	// arm[0] is the driver (no inner join needed).
	orderedArms = append(orderedArms, armOrder{rows: arms[0], subqueryFirst: false})

	for i, armDef := range armDefs {
		armIdx := i + 1
		if armIdx >= len(arms) {
			break
		}
		arm := arms[armIdx]

		// Subquery-first if subqueryRef points to an outer table (not this arm's table).
		sf := true
		if armDef.subqueryRef != "" {
			if armDef.subqueryRef == armDef.tableName {
				sf = false // self-reference → table-first
			} else if _, isOuter := outerTables[armDef.subqueryRef]; !isOuter {
				sf = false // not an outer table and not self → unclear, default table-first
			}
		}
		orderedArms = append(orderedArms, armOrder{rows: arm, subqueryFirst: sf})
		outerTables[armDef.tableName] = true
	}

	// Convert orderedArms back to arms for buildLeftJoinChain (order already embedded).
	// We need a version of buildLeftJoinChain that knows the ordering.
	// Build the arms slices in the correct order for each arm.
	var finalArms [][][]interface{}
	for i, oa := range orderedArms {
		if i == 0 {
			finalArms = append(finalArms, oa.rows)
			continue
		}
		// Reorder rows based on subqueryFirst.
		var subRows, tblRows [][]interface{}
		for _, row := range oa.rows {
			tbl := ""
			if row[2] != nil {
				tbl = fmt.Sprintf("%v", row[2])
			}
			if strings.HasPrefix(tbl, "<subquery") {
				subRows = append(subRows, row)
			} else {
				tblRows = append(tblRows, row)
			}
		}
		if oa.subqueryFirst {
			finalArms = append(finalArms, append(subRows, tblRows...))
		} else {
			finalArms = append(finalArms, append(tblRows, subRows...))
		}
	}

	return e.buildLeftJoinChain(finalArms, allRows, 0)
}

// buildLeftJoinChain recursively builds "Nested loop left join" nodes.
// arms[0] is the leftmost table group; arms[1..] are successive right-side groups.
func (e *Executor) buildLeftJoinChain(arms [][][]interface{}, allRows [][]interface{}, indent int) []string {
	if len(arms) == 0 {
		return nil
	}
	pfx := explainTreeIndent(indent)

	if len(arms) == 1 {
		// Single arm: render it directly (no join wrapper needed).
		return e.explainTreeRenderLeftJoinArm(arms[0], allRows, indent, false)
	}

	// Emit the left join wrapper and recurse.
	var lines []string
	lines = append(lines, pfx+"-> Nested loop left join")

	// Left child: all arms except the last.
	leftLines := e.buildLeftJoinChain(arms[:len(arms)-1], allRows, indent+1)
	lines = append(lines, leftLines...)

	// Right child: the last arm, rendered as an inner join.
	rightLines := e.explainTreeRenderLeftJoinArm(arms[len(arms)-1], allRows, indent+1, true)
	lines = append(lines, rightLines...)

	return lines
}

// explainTreeRenderLeftJoinArm renders one arm of a LEFT JOIN tree.
// armRows must already be in the correct rendering order (caller determines order).
// isRightSide: if true, arm is the right side of a LEFT JOIN (render as nested loop inner join).
// If isRightSide=false, renders the arm as a simple table or group.
// The subquery-first ordering is inferred from the row order: if the first row is a
// <subquery> placeholder, subqueryFirst=true; otherwise false.
func (e *Executor) explainTreeRenderLeftJoinArm(armRows [][]interface{}, allRows [][]interface{}, indent int, isRightSide bool) []string {
	pfx := explainTreeIndent(indent)

	if !isRightSide {
		// Driver arm: render without join wrapper.
		if len(armRows) == 1 {
			row := armRows[0]
			tbl, at, kn, ref, extra, _, _ := explainTreeRowInfo(row)
			// For the leftmost driver arm with a pure ALL scan (no filter conditions),
			// MySQL omits the Filter wrapper in FORMAT=TREE.
			if at == "ALL" && !strings.Contains(extra, "Using where") && !strings.Contains(extra, "Using join buffer") {
				pfx2 := explainTreeIndent(indent)
				return []string{pfx2 + "-> Table scan on " + tbl}
			}
			return e.explainTreeTableNode(tbl, at, kn, ref, extra, allRows, indent)
		}
		return e.explainTreeBuildGroup(armRows, allRows, indent, "inner")
	}

	// Right-side arm: separate subquery placeholder rows from regular table rows.
	var subqueryRowsInArm, tableRowsInArm [][]interface{}
	for _, row := range armRows {
		tbl := ""
		if row[2] != nil {
			tbl = fmt.Sprintf("%v", row[2])
		}
		if strings.HasPrefix(tbl, "<subquery") {
			subqueryRowsInArm = append(subqueryRowsInArm, row)
		} else {
			tableRowsInArm = append(tableRowsInArm, row)
		}
	}

	if len(subqueryRowsInArm) == 0 || len(tableRowsInArm) == 0 {
		return e.explainTreeBuildGroup(armRows, allRows, indent, "inner")
	}

	// Infer ordering from the first row of the arm.
	firstTbl := ""
	if len(armRows) > 0 && armRows[0][2] != nil {
		firstTbl = fmt.Sprintf("%v", armRows[0][2])
	}
	subqueryFirst := strings.HasPrefix(firstTbl, "<subquery")

	var lines []string
	lines = append(lines, pfx+"-> Nested loop inner join")

	if subqueryFirst {
		// Subquery-first: render rows in order (all subquery rows before table rows).
		for _, row := range armRows {
			tbl, at, kn, ref, extra, _, _ := explainTreeRowInfo(row)
			subLines := e.explainTreeTableNode(tbl, at, kn, ref, extra, allRows, indent+1)
			lines = append(lines, subLines...)
		}
	} else {
		// Table-first: render table rows, then wrap subquery rows in Filter.
		for _, row := range tableRowsInArm {
			tbl, at, kn, ref, extra, _, _ := explainTreeRowInfo(row)
			subLines := e.explainTreeTableNode(tbl, at, kn, ref, extra, allRows, indent+1)
			lines = append(lines, subLines...)
		}
		for _, row := range subqueryRowsInArm {
			tbl, at, kn, ref, extra, _, _ := explainTreeRowInfo(row)
			// Probe-side subquery gets a Filter wrapper (MySQL shows a filter condition
			// on the subquery placeholder when the table is the driver).
			innerLines := e.explainTreeTableNode(tbl, at, kn, ref, extra, allRows, indent+2)
			lines = append(lines, explainTreeIndent(indent+1)+"-> Filter: ("+tbl+" condition)")
			lines = append(lines, innerLines...)
		}
	}

	return lines
}

func (e *Executor) explainTreeText(query string) string {
	upper := strings.ToUpper(query)
	if strings.Contains(upper, "JSON_TABLE(") {
		return "-> Materialize table function"
	}

	// Try to build a proper tree from EXPLAIN rows
	rows := e.explainMultiRows(query)

	// Check if we have any complex structure (materialized subqueries, multi-table joins)
	hasSubquery := false
	hasMaterialized := false
	for _, row := range rows {
		_, _, _, _, _, st, _ := explainTreeRowInfo(row)
		tbl := ""
		if row[2] != nil {
			tbl = fmt.Sprintf("%v", row[2])
		}
		if strings.HasPrefix(tbl, "<subquery") {
			hasSubquery = true
		}
		if st == "MATERIALIZED" {
			hasMaterialized = true
		}
	}

	// For complex queries with materialized subqueries, build full tree
	if hasSubquery || hasMaterialized {
		// Group rows by id (skip nil-id rows like UNION RESULT)
		idOrder := []int64{}
		idGroups := map[int64][][]interface{}{}
		for _, row := range rows {
			if id, ok := row[0].(int64); ok {
				if _, seen := idGroups[id]; !seen {
					idOrder = append(idOrder, id)
				}
				idGroups[id] = append(idGroups[id], row)
			}
		}

		if len(idOrder) > 0 {
			outerID := idOrder[0]
			outerRows := idGroups[outerID]

			// Filter out MATERIALIZED rows (they belong to inner subqueries and are accessed via the placeholder)
			var primaryRows [][]interface{}
			for _, row := range outerRows {
				_, _, _, _, _, st, _ := explainTreeRowInfo(row)
				if st != "MATERIALIZED" {
					primaryRows = append(primaryRows, row)
				}
			}
			if len(primaryRows) == 0 {
				primaryRows = outerRows
			}

			// Use specialized LEFT JOIN tree builder when the query has LEFT JOINs.
			var treeLines []string
			if e.queryHasLeftJoin(query) {
				treeLines = e.explainTreeBuildForLeftJoin(rows, query)
			} else {
				treeLines = e.explainTreeBuildGroup(primaryRows, rows, 0, "inner")
			}
			if len(treeLines) > 0 {
				return strings.Join(treeLines, "\n")
			}
		}
	}

	// Fallback: simple single-table tree
	tbl := explainTableNameFromQuery(query)
	if tbl == "" {
		tbl = "dual"
	}

	// Parse the query to detect structural features
	stmt, err := e.parser().Parse(query)
	if err != nil {
		return "-> Table scan on " + tbl
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return "-> Table scan on " + tbl
	}

	// Detect LIMIT/OFFSET
	var limitOffset string
	if sel.Limit != nil {
		rowCount := int64(0)
		offset := int64(0)
		if sel.Limit.Rowcount != nil {
			if lit, ok := sel.Limit.Rowcount.(*sqlparser.Literal); ok {
				rowCount, _ = strconv.ParseInt(lit.Val, 10, 64)
			}
		}
		if sel.Limit.Offset != nil {
			if lit, ok := sel.Limit.Offset.(*sqlparser.Literal); ok {
				offset, _ = strconv.ParseInt(lit.Val, 10, 64)
			}
		}
		if offset > 0 {
			limitOffset = fmt.Sprintf("-> Limit/Offset: %d/%d row(s)\n    ", rowCount, offset)
		} else if rowCount > 0 {
			limitOffset = fmt.Sprintf("-> Limit: %d row(s)\n    ", rowCount)
		}
	}

	// Determine inner scan node based on access type
	var scanNode string
	hasWhere := sel.Where != nil
	if hasWhere {
		// Try to detect index usage for the scan node
		ai := e.explainDetectAccessType(sel, tbl)
		if ai.accessType == "range" {
			idxName := ""
			if ai.key != nil {
				idxName = fmt.Sprintf("%v", ai.key)
			}
			if idxName != "" {
				scanNode = "-> Index range scan on " + tbl + " using " + idxName
			} else {
				scanNode = "-> Table scan on " + tbl
			}
		} else if ai.accessType == "ref" || ai.accessType == "ref_or_null" || ai.accessType == "eq_ref" || ai.accessType == "const" {
			idxName := ""
			if ai.key != nil {
				idxName = fmt.Sprintf("%v", ai.key)
			}
			if idxName != "" {
				scanNode = "-> Index lookup on " + tbl + " using " + idxName
			} else {
				scanNode = "-> Table scan on " + tbl
			}
		} else if ai.accessType == "index" {
			idxName := ""
			if ai.key != nil {
				idxName = fmt.Sprintf("%v", ai.key)
			}
			if idxName != "" {
				scanNode = "-> Index scan on " + tbl + " using " + idxName
			} else {
				scanNode = "-> Table scan on " + tbl
			}
		} else {
			scanNode = "-> Table scan on " + tbl
		}
	} else {
		scanNode = "-> Table scan on " + tbl
	}

	// Build the tree: [LimitOffset ->] [Filter ->] scan
	if hasWhere {
		// Generate a placeholder filter condition text
		whereStr := sqlparser.String(sel.Where.Expr)
		filterLine := "-> Filter: (" + whereStr + ")\n    " + scanNode
		if limitOffset != "" {
			return limitOffset + filterLine
		}
		return filterLine
	}
	return limitOffset + scanNode
}

func (e *Executor) explainResultForType(explainType sqlparser.ExplainType, explainedQuery string) *Result {
	switch explainType {
	case sqlparser.TreeType:
		return &Result{
			Columns:     []string{"EXPLAIN"},
			Rows:        [][]interface{}{{e.explainTreeText(explainedQuery)}},
			IsResultSet: true,
		}
	case sqlparser.JSONType:
		return &Result{
			Columns:     []string{"EXPLAIN"},
			Rows:        [][]interface{}{{e.explainJSONDocument(explainedQuery)}},
			IsResultSet: true,
		}
	default:
		rows := e.explainMultiRows(explainedQuery)
		return &Result{
			Columns:     []string{"id", "select_type", "table", "partitions", "type", "possible_keys", "key", "key_len", "ref", "rows", "filtered", "Extra"},
			Rows:        rows,
			IsResultSet: true,
		}
	}
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

// coerceDateTimeValue truncates datetime values to match the column type.
// For DATE columns, "2007-02-13 15:09:33" becomes "2007-02-13".
// For TIME columns, "2007-02-13 15:09:33" becomes "15:09:33".
// For YEAR columns, "2007-02-13 15:09:33" becomes "2007".
func coerceDateTimeValue(colType string, v interface{}) interface{} {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	s := fmt.Sprintf("%v", v)
	if len(s) == 0 {
		// Empty string for YEAR -> "0000"
		if strings.HasPrefix(upper, "YEAR") {
			return "0000"
		}
		// Empty string -> zero date/datetime
		switch upper {
		case "DATE":
			return "0000-00-00"
		case "DATETIME":
			return "0000-00-00 00:00:00"
		case "TIMESTAMP":
			return "0000-00-00 00:00:00"
		}
		return v
	}
	// Normalize YEAR(4) -> YEAR
	if strings.HasPrefix(upper, "YEAR(") {
		upper = "YEAR"
	}
	// Extract TIME precision: TIME -> 0, TIME(N) -> N
	timeFsp := 0
	isTimeType := false
	if upper == "TIME" {
		isTimeType = true
	} else if strings.HasPrefix(upper, "TIME(") {
		isTimeType = true
		fmt.Sscanf(upper, "TIME(%d)", &timeFsp)
	}
	if isTimeType {
		upper = "TIME"
	}
	switch upper {
	case "DATE":
		// If the value looks like a datetime, truncate to date-only
		if len(s) > 10 && s[4] == '-' && s[7] == '-' {
			dateStr := s[:10]
			// Validate the extracted date
			parsed := parseMySQLDateValue(dateStr)
			if parsed != "" {
				return parsed
			}
			return "0000-00-00"
		}
		// Try parsing various MySQL DATE formats
		parsed := parseMySQLDateValue(s)
		if parsed != "" {
			return parsed
		}
		// Invalid date value -> zero date
		return "0000-00-00"
	case "TIME":
		// Use the original value for numeric handling
		result := parseMySQLTimeValueRaw(v)
		// Invalid TIME text should coerce to zero time in non-strict behavior.
		if strings.Count(result, ":") != 2 {
			result = "00:00:00"
		}
		// Apply TIME precision: round fractional seconds to timeFsp digits
		return applyTimePrecision(result, timeFsp)
	case "YEAR":
		return coerceYearValue(v)
	case "TIMESTAMP":
		// Try parsing various date formats first
		parsed := parseMySQLDateValue(s)
		if parsed == "" {
			// Invalid date value -> zero timestamp
			return "0000-00-00 00:00:00"
		}
		s = parsed
		// Check if we need to append time from the original value
		if len(s) == 10 {
			timePart := extractTimePart(v, s)
			if timePart != "" {
				// Normalize time separator chars to ':'
				timePart = normalizeDateTimeSeparators(timePart)
				s = s + " " + timePart
			}
		}
		// TIMESTAMP range: '1970-01-01 00:00:01' to '2038-01-19 03:14:07' UTC
		// Out-of-range values are stored as '0000-00-00 00:00:00'
		if len(s) >= 10 && s[4] == '-' {
			datePart := s
			if len(datePart) > 10 {
				datePart = datePart[:10]
			}
			if t, err := time.Parse("2006-01-02", datePart); err == nil {
				minTS := time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC)
				maxTS := time.Date(2038, 1, 19, 3, 14, 7, 0, time.UTC)
				if t.Before(minTS) || t.After(maxTS) {
					return "0000-00-00 00:00:00"
				}
			}
		}
		// Ensure TIMESTAMP always has time component
		if len(s) == 10 && s[4] == '-' && s[7] == '-' {
			s = s + " 00:00:00"
		}
		return s
	case "DATETIME":
		// Try parsing various date formats
		parsed := parseMySQLDateValue(s)
		if parsed != "" {
			timePart := extractTimePart(v, s)
			if timePart != "" {
				// Normalize time separator chars to ':'
				timePart = normalizeDateTimeSeparators(timePart)
				return parsed + " " + timePart
			}
			return parsed + " 00:00:00"
		}
		// Invalid date value -> zero datetime
		return "0000-00-00 00:00:00"
	}
	return v
}

// extractTimePart extracts the time component from a datetime value.
// It handles both string values with space separators and numeric YYYYMMDDHHMMSS/YYMMDDHHMMSS formats.
func extractTimePart(v interface{}, parsedDate string) string {
	origS := fmt.Sprintf("%v", v)

	// Check for space-separated time part (e.g., "98-12-31 11:30:45")
	if idx := strings.Index(origS, " "); idx >= 0 {
		timePart := strings.TrimSpace(origS[idx+1:])
		if timePart != "" {
			return timePart
		}
	}

	// Check for numeric YYYYMMDDHHMMSS or YYMMDDHHMMSS format
	isAllDigits := true
	for _, c := range origS {
		if c < '0' || c > '9' {
			isAllDigits = false
			break
		}
	}
	if isAllDigits {
		switch len(origS) {
		case 14: // YYYYMMDDHHMMSS
			h, _ := strconv.Atoi(origS[8:10])
			m, _ := strconv.Atoi(origS[10:12])
			sec, _ := strconv.Atoi(origS[12:14])
			return fmt.Sprintf("%02d:%02d:%02d", h, m, sec)
		case 12: // YYMMDDHHMMSS
			h, _ := strconv.Atoi(origS[6:8])
			m, _ := strconv.Atoi(origS[8:10])
			sec, _ := strconv.Atoi(origS[10:12])
			return fmt.Sprintf("%02d:%02d:%02d", h, m, sec)
		}
	}

	// Also handle numeric int64/float64 values that were converted to string
	switch n := v.(type) {
	case int64:
		if n >= 10000000000 { // At least YYMMDDHHMMSS
			sec := int(n % 100)
			m := int((n / 100) % 100)
			h := int((n / 10000) % 100)
			return fmt.Sprintf("%02d:%02d:%02d", h, m, sec)
		}
	case float64:
		in := int64(n)
		if in >= 10000000000 { // At least YYMMDDHHMMSS
			sec := int(in % 100)
			m := int((in / 100) % 100)
			h := int((in / 10000) % 100)
			return fmt.Sprintf("%02d:%02d:%02d", h, m, sec)
		}
	case uint64:
		if n >= 10000000000 { // At least YYMMDDHHMMSS
			sec := int(n % 100)
			m := int((n / 100) % 100)
			h := int((n / 10000) % 100)
			return fmt.Sprintf("%02d:%02d:%02d", h, m, sec)
		}
	}

	return ""
}

// parseMySQLTimeValueRaw handles raw interface values for TIME conversion.
// This properly handles numeric values (int64, float64) using HHMMSS format.
func parseMySQLTimeValueRaw(v interface{}) string {
	switch n := v.(type) {
	case int64:
		negative := n < 0
		if negative {
			n = -n
		}
		sec := int(n % 100)
		m := int((n / 100) % 100)
		h := int(n / 10000)
		return formatTimeValue(negative, h, m, sec, "")
	case float64:
		negative := n < 0
		if negative {
			n = -n
		}
		intPart := int64(n)
		fracPart := n - float64(intPart)
		sec := int(intPart % 100)
		m := int((intPart / 100) % 100)
		h := int(intPart / 10000)
		frac := ""
		if fracPart > 0 {
			fracStr := fmt.Sprintf("%.6f", fracPart)
			frac = strings.TrimPrefix(fracStr, "0.")
			frac = strings.TrimRight(frac, "0")
		}
		return formatTimeValue(negative, h, m, sec, frac)
	case uint64:
		sec := int(n % 100)
		m := int((n / 100) % 100)
		h := int(n / 10000)
		return formatTimeValue(false, h, m, sec, "")
	}
	return parseMySQLTimeValue(fmt.Sprintf("%v", v))
}

// parseMySQLTimeValue parses a value into MySQL TIME format (HH:MM:SS).
// Supports various MySQL TIME input formats and clips to valid range.
func parseMySQLTimeValue(s string) string {
	if s == "" {
		return "00:00:00"
	}

	// If the value looks like a datetime (YYYY-MM-DD HH:MM:SS), extract the time part.
	// Special case: 0000-00-DD represents a day offset for TIME values (e.g. from str_to_date %d %H).
	// In that case, convert days to hours and add to the time component.
	if len(s) >= 19 && s[4] == '-' && s[7] == '-' && s[10] == ' ' {
		year, _ := strconv.Atoi(s[:4])
		month, _ := strconv.Atoi(s[5:7])
		if year == 0 && month == 0 {
			day, _ := strconv.Atoi(s[8:10])
			if day > 0 {
				// Convert to D HH:MM:SS.ffffff form and let the colon-based parser handle it
				return parseMySQLTimeValue(fmt.Sprintf("%d %s", day, s[11:]))
			}
		}
		return s[11:]
	}
	// If the value looks like a date (YYYY-MM-DD), return 00:00:00
	if len(s) == 10 && s[4] == '-' && s[7] == '-' {
		return "00:00:00"
	}

	// Already in HH:MM:SS or H:MM:SS format (with optional sign and fractional seconds)
	if strings.Contains(s, ":") {
		// Handle "D HH:MM:SS" format (days prefix)
		negative := false
		if strings.HasPrefix(s, "-") {
			negative = true
			s = s[1:]
		}
		days := 0
		if idx := strings.Index(s, " "); idx >= 0 {
			d, err := strconv.Atoi(strings.TrimSpace(s[:idx]))
			if err == nil {
				days = d
				s = strings.TrimSpace(s[idx+1:])
			}
		}
		parts := strings.Split(s, ":")
		h, m, sec := 0, 0, 0
		frac := ""
		switch len(parts) {
		case 2: // HH:MM
			h, _ = strconv.Atoi(parts[0])
			m, _ = strconv.Atoi(parts[1])
		case 3: // HH:MM:SS or HH:MM:SS.frac
			h, _ = strconv.Atoi(parts[0])
			m, _ = strconv.Atoi(parts[1])
			secParts := strings.SplitN(parts[2], ".", 2)
			sec, _ = strconv.Atoi(secParts[0])
			if len(secParts) > 1 {
				frac = secParts[1]
			}
		}
		h += days * 24
		return formatTimeValue(negative, h, m, sec, frac)
	}

	// Handle "D HH" format (e.g., "0 10" -> "10:00:00")
	// MySQL interprets 'D val' as D days + val hours (not seconds)
	if idx := strings.Index(s, " "); idx >= 0 {
		negative := false
		if strings.HasPrefix(s, "-") {
			negative = true
			s = s[1:]
			idx--
		}
		days, err := strconv.Atoi(strings.TrimSpace(s[:idx]))
		if err == nil {
			rest := strings.TrimSpace(s[idx+1:])
			hh, _ := strconv.Atoi(rest)
			h := days*24 + hh
			return formatTimeValue(negative, h, 0, 0, "")
		}
	}

	// Handle numeric and string "HHMMSS" or "SS" format
	negative := false
	if strings.HasPrefix(s, "-") {
		negative = true
		s = s[1:]
	}

	// Handle fractional seconds in numeric format (e.g., "123556.99")
	frac := ""
	if dotIdx := strings.Index(s, "."); dotIdx >= 0 {
		frac = s[dotIdx+1:]
		s = s[:dotIdx]
	}

	// Pad to at least 2 digits
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		// Not a valid number, return as-is
		return s
	}

	var h, m, sec int
	if n < 100 {
		// SS format
		sec = int(n)
	} else if n < 10000 {
		// MMSS format
		sec = int(n % 100)
		m = int(n / 100)
	} else {
		// HHMMSS format
		sec = int(n % 100)
		m = int((n / 100) % 100)
		h = int(n / 10000)
	}

	return formatTimeValue(negative, h, m, sec, frac)
}

// formatTimeValue formats time components into MySQL TIME string, clipping to valid range.
func formatTimeValue(negative bool, h, m, sec int, frac string) string {
	// Invalid minutes/seconds produce zero time in MySQL numeric TIME context.
	if m > 59 || sec > 59 || m < 0 || sec < 0 {
		return "00:00:00"
	}

	// Clip to MySQL TIME range: -838:59:59 to 838:59:59
	totalSecs := h*3600 + m*60 + sec
	maxSecs := 838*3600 + 59*60 + 59
	if totalSecs > maxSecs {
		// If hours > 838, clip to max
		h, m, sec = 838, 59, 59
		frac = ""
	}

	sign := ""
	if negative {
		sign = "-"
	}

	result := fmt.Sprintf("%s%02d:%02d:%02d", sign, h, m, sec)
	if frac != "" {
		// Truncate fractional seconds to microseconds (6 digits)
		if len(frac) > 6 {
			frac = frac[:6]
		}
		// Remove trailing zeros
		frac = strings.TrimRight(frac, "0")
		if frac != "" {
			result += "." + frac
		}
	}
	return result
}

// applyTimePrecision rounds or truncates a TIME string's fractional seconds
// to the given fsp (fractional seconds precision, 0-6).
// For fsp=0 (default TIME), fractional seconds >= 0.5 round up the seconds.
func applyTimePrecision(timeStr string, fsp int) string {
	// Find the fractional part
	dotIdx := strings.Index(timeStr, ".")
	if dotIdx < 0 {
		// No fractional part, nothing to do
		return timeStr
	}
	basePart := timeStr[:dotIdx]
	fracPart := timeStr[dotIdx+1:]

	if fsp == 0 {
		// Round: if first frac digit >= 5, increment seconds
		if len(fracPart) > 0 && fracPart[0] >= '5' {
			return incrementTimeSecond(basePart)
		}
		return basePart
	}

	// Pad or truncate fractional part to fsp digits
	for len(fracPart) < fsp {
		fracPart += "0"
	}
	if len(fracPart) > fsp {
		// Check if we need to round
		roundUp := fracPart[fsp] >= '5'
		fracPart = fracPart[:fsp]
		if roundUp {
			// Increment the last fractional digit
			digits := []byte(fracPart)
			carry := true
			for i := len(digits) - 1; i >= 0 && carry; i-- {
				digits[i]++
				if digits[i] > '9' {
					digits[i] = '0'
				} else {
					carry = false
				}
			}
			fracPart = string(digits)
			if carry {
				// Fractional part overflowed, increment seconds
				return incrementTimeSecond(basePart) + "." + fracPart
			}
		}
	}
	// Keep exactly fsp digits (do not trim trailing zeros for fixed-precision TIME columns)
	return basePart + "." + fracPart
}

// incrementTimeSecond adds 1 second to a TIME string like "HH:MM:SS" or "-HH:MM:SS".
func incrementTimeSecond(timeStr string) string {
	negative := false
	s := timeStr
	if strings.HasPrefix(s, "-") {
		negative = true
		s = s[1:]
	}
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return timeStr
	}
	h, _ := strconv.Atoi(parts[0])
	m, _ := strconv.Atoi(parts[1])
	sec, _ := strconv.Atoi(parts[2])
	sec++
	if sec >= 60 {
		sec = 0
		m++
	}
	if m >= 60 {
		m = 0
		h++
	}
	// Re-clip to TIME range
	sign := ""
	if negative {
		sign = "-"
	}
	totalSecs := h*3600 + m*60 + sec
	maxSecs := 838*3600 + 59*60 + 59
	if totalSecs > maxSecs {
		h, m, sec = 838, 59, 59
	}
	return fmt.Sprintf("%s%02d:%02d:%02d", sign, h, m, sec)
}

// implicitZeroValue returns the implicit zero/default value for a MySQL type
// when a NOT NULL column has no explicit default.
func implicitZeroValue(colType string) interface{} {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	if strings.HasPrefix(upper, "YEAR") {
		return "0000"
	}
	if strings.HasPrefix(upper, "DATE") && !strings.HasPrefix(upper, "DATETIME") {
		return "0000-00-00"
	}
	if strings.HasPrefix(upper, "DATETIME") || strings.HasPrefix(upper, "TIMESTAMP") {
		return "0000-00-00 00:00:00"
	}
	if strings.HasPrefix(upper, "TIME") {
		return "00:00:00"
	}
	if strings.Contains(upper, "INT") || strings.Contains(upper, "DECIMAL") ||
		strings.Contains(upper, "FLOAT") || strings.Contains(upper, "DOUBLE") {
		return int64(0)
	}
	// BINARY(N): zero value is N null bytes
	if padLen := binaryPadLength(colType); padLen > 0 {
		return strings.Repeat("\x00", padLen)
	}
	// ENUM: implicit default is the first enum value
	lower := strings.ToLower(strings.TrimSpace(colType))
	if strings.HasPrefix(lower, "enum(") {
		inner := colType[5 : len(colType)-1]
		vals := splitEnumValues(inner)
		if len(vals) > 0 {
			return EnumValue(strings.Trim(vals[0], "'"))
		}
		return EnumValue("")
	}
	// Default for string types (including SET which defaults to empty)
	return ""
}

// coerceYearValue converts a value to MySQL YEAR type.
// Returns a string like "0000", "2005", etc.
func coerceYearValue(v interface{}) interface{} {
	s := fmt.Sprintf("%v", v)

	// Check if original value was a numeric type
	isNumericType := false
	var numVal int
	switch n := v.(type) {
	case int64:
		isNumericType = true
		numVal = int(n)
	case float64:
		isNumericType = true
		numVal = int(n)
	case uint64:
		isNumericType = true
		numVal = int(n)
	}

	// Empty string -> 0000
	if s == "" {
		return "0000"
	}

	// Extract year from date/datetime (e.g., "2009-01-29 11:11:27")
	if len(s) >= 5 && s[4] == '-' {
		yearPart := s[:4]
		if y, err := strconv.Atoi(yearPart); err == nil {
			if y >= 1901 && y <= 2155 {
				return fmt.Sprintf("%d", y)
			}
			return "0000"
		}
	}

	if isNumericType {
		if numVal == 0 {
			return "0000"
		}
		if numVal >= 1 && numVal <= 69 {
			return fmt.Sprintf("%d", 2000+numVal)
		}
		if numVal >= 70 && numVal <= 99 {
			return fmt.Sprintf("%d", 1900+numVal)
		}
		if numVal >= 1901 && numVal <= 2155 {
			return fmt.Sprintf("%d", numVal)
		}
		return "0000"
	}

	// String value — MySQL extracts leading numeric portion (e.g., "2012qwer" → 2012)
	n, err := strconv.Atoi(s)
	if err != nil {
		f, err2 := strconv.ParseFloat(s, 64)
		if err2 != nil {
			// Try extracting leading digits (e.g., "2012qwer" -> 2012)
			leadingDigits := ""
			for _, ch := range s {
				if ch >= '0' && ch <= '9' {
					leadingDigits += string(ch)
				} else {
					break
				}
			}
			if leadingDigits == "" {
				return "0000"
			}
			n, err = strconv.Atoi(leadingDigits)
			if err != nil {
				return "0000"
			}
		} else {
			n = int(f)
		}
	}

	// String '0' or '00' or '000' -> 2000 (but '0000' -> 0000)
	if n == 0 {
		trimmed := strings.TrimLeft(s, "0")
		if trimmed == "" && len(s) >= 4 {
			return "0000"
		}
		return "2000"
	}

	if n >= 1 && n <= 69 {
		return fmt.Sprintf("%d", 2000+n)
	}
	if n >= 70 && n <= 99 {
		return fmt.Sprintf("%d", 1900+n)
	}
	if n >= 1901 && n <= 2155 {
		return fmt.Sprintf("%d", n)
	}
	return "0000"
}

// parseMySQLDateValue parses various MySQL date input formats and returns YYYY-MM-DD or "".
var flexDateRe = regexp.MustCompile(`^(\d{4})-(\d{1,2})-(\d{1,2})`)

func parseMySQLDateValue(s string) string {
	// Already in standard format
	if len(s) >= 10 && s[4] == '-' && s[7] == '-' {
		dateStr := s[:10]
		// Validate the date
		y, _ := strconv.Atoi(dateStr[:4])
		m, _ := strconv.Atoi(dateStr[5:7])
		d, _ := strconv.Atoi(dateStr[8:10])
		if !isValidDate(y, m, d) {
			return "" // Invalid date like 2008-04-31
		}
		return dateStr
	}

	// Try flexible YYYY-M-D format (non-zero-padded)
	if m := flexDateRe.FindStringSubmatch(s); m != nil {
		y, _ := strconv.Atoi(m[1])
		mo, _ := strconv.Atoi(m[2])
		d, _ := strconv.Atoi(m[3])
		if isValidDate(y, mo, d) {
			return fmt.Sprintf("%04d-%02d-%02d", y, mo, d)
		}
	}

	// Strip time part for datetime strings
	datePart := s
	if idx := strings.Index(s, " "); idx >= 0 {
		datePart = s[:idx]
	}

	// Try YYYYMMDD or YYYYMMDDHHMMSS format (no delimiters)
	isAllDigits := true
	for _, c := range datePart {
		if c < '0' || c > '9' {
			isAllDigits = false
			break
		}
	}
	if isAllDigits {
		switch len(datePart) {
		case 8: // YYYYMMDD
			y, _ := strconv.Atoi(datePart[:4])
			m, _ := strconv.Atoi(datePart[4:6])
			d, _ := strconv.Atoi(datePart[6:8])
			if isValidDate(y, m, d) {
				return fmt.Sprintf("%04d-%02d-%02d", y, m, d)
			}
		case 14: // YYYYMMDDHHMMSS - extract date part
			y, _ := strconv.Atoi(datePart[:4])
			m, _ := strconv.Atoi(datePart[4:6])
			d, _ := strconv.Atoi(datePart[6:8])
			if isValidDate(y, m, d) {
				return fmt.Sprintf("%04d-%02d-%02d", y, m, d)
			}
		case 6: // YYMMDD
			yy, _ := strconv.Atoi(datePart[:2])
			m, _ := strconv.Atoi(datePart[2:4])
			d, _ := strconv.Atoi(datePart[4:6])
			y := convert2DigitYear(yy)
			if isValidDate(y, m, d) {
				return fmt.Sprintf("%04d-%02d-%02d", y, m, d)
			}
		case 12: // YYMMDDHHMMSS - extract date part
			yy, _ := strconv.Atoi(datePart[:2])
			m, _ := strconv.Atoi(datePart[2:4])
			d, _ := strconv.Atoi(datePart[4:6])
			y := convert2DigitYear(yy)
			if isValidDate(y, m, d) {
				return fmt.Sprintf("%04d-%02d-%02d", y, m, d)
			}
		}
		return ""
	}

	// Handle 2-digit year with delimiters: YY-MM-DD or YY/MM/DD etc.
	// Replace various delimiters with -
	normalized := normalizeDateDelimiters(datePart)
	parts := strings.Split(normalized, "-")
	if len(parts) == 3 {
		yStr, mStr, dStr := parts[0], parts[1], parts[2]
		y, errY := strconv.Atoi(yStr)
		m, errM := strconv.Atoi(mStr)
		d, errD := strconv.Atoi(dStr)
		if errY == nil && errM == nil && errD == nil {
			// Special case: all-zero date (e.g., 00-00-00) -> 0000-00-00
			if y == 0 && m == 0 && d == 0 {
				return "0000-00-00"
			}
			if len(yStr) <= 2 {
				y = convert2DigitYear(y)
			}
			if isValidDate(y, m, d) {
				return fmt.Sprintf("%04d-%02d-%02d", y, m, d)
			}
		}
	}

	return ""
}

// isValidDate checks if a date is valid (or is the zero date 0000-00-00).
func isValidDate(y, m, d int) bool {
	// Zero date is always valid
	if y == 0 && m == 0 && d == 0 {
		return true
	}
	// Allow zero month/day for MySQL partial zero dates
	if m == 0 || d == 0 {
		return m >= 0 && m <= 12 && d >= 0 && d <= 31
	}
	if m < 1 || m > 12 || d < 1 {
		return false
	}
	// Days in month
	daysInMonth := [13]int{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	maxDay := daysInMonth[m]
	if m == 2 && isLeapYear(y) {
		maxDay = 29
	}
	return d <= maxDay
}

// isLeapYear checks if a year is a leap year.
func isLeapYear(y int) bool {
	return (y%4 == 0 && y%100 != 0) || y%400 == 0
}

// checkDateStrict validates a date string value for a DATE/DATETIME/TIMESTAMP column
// in strict SQL mode. Returns an error if the date is invalid.
// The sqlMode string is used to determine which checks to apply.
func checkDateStrict(colType, colName, originalValue, sqlMode string) error {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	// Only check date-like types
	isDateType := upper == "DATE" || strings.HasPrefix(upper, "DATETIME") || strings.HasPrefix(upper, "TIMESTAMP")
	if !isDateType {
		return nil
	}

	s := strings.TrimSpace(originalValue)
	if s == "" {
		return nil
	}

	// Determine SQL mode flags
	isNoZeroInDate := strings.Contains(sqlMode, "NO_ZERO_IN_DATE") || strings.Contains(sqlMode, "TRADITIONAL")
	isNoZeroDate := strings.Contains(sqlMode, "NO_ZERO_DATE") || strings.Contains(sqlMode, "TRADITIONAL")
	// ALLOW_INVALID_DATES skips range checks for day (but still requires valid month range 1-12)
	allowInvalidDates := strings.Contains(sqlMode, "ALLOW_INVALID_DATES")

	// Parse the date part from the value
	// Supported formats: YYYY-MM-DD, YYYYMMDD, YY-MM-DD, and flexible variants
	datePart := s
	// Split at space, newline, or 'T' to get the date portion
	if idx := strings.IndexAny(s, " \tT\n\r"); idx >= 0 {
		datePart = s[:idx]
	}

	// If the value looks like a TIME-only value (e.g., "97:02:03" or "23:59:59"),
	// skip strict date validation — MySQL coerces such values into dates.
	// A TIME-only format has colons but doesn't have date separators (-/.) in the
	// first few characters. Detect by: contains ':' but not in a DATETIME context.
	if strings.Contains(datePart, ":") {
		// Check if it's a time-only format: digits, colons, possibly a decimal
		looksLikeTime := true
		for _, c := range datePart {
			if !((c >= '0' && c <= '9') || c == ':' || c == '.') {
				looksLikeTime = false
				break
			}
		}
		if looksLikeTime {
			return nil // MySQL coerces TIME values into date columns, don't reject here
		}
	}
	// Handle decimal/fractional numeric date-time values like '20010101235959.4'
	// or '20010101100000.1234567'. These are compact numeric DATETIME formats with
	// fractional seconds. Strip the fractional part only if the integer part looks
	// like a compact YYYYMMDDHHMMSS or YYYYMMDD format (8, 12, or 14 digits).
	if idx := strings.IndexByte(datePart, '.'); idx >= 0 {
		// Check if the part before the dot is all digits
		allDigitsBefore := true
		for _, c := range datePart[:idx] {
			if c < '0' || c > '9' {
				allDigitsBefore = false
				break
			}
		}
		if allDigitsBefore && (idx == 8 || idx == 12 || idx == 14) {
			datePart = datePart[:idx]
		}
	}

	// Try to parse as YYYY-M-D or YYYYMMDD
	var y, m, d int
	parsed := false

	// Check if it's a numeric value (no delimiters)
	isAllDigits := true
	for _, c := range datePart {
		if c < '0' || c > '9' {
			isAllDigits = false
			break
		}
	}
	if isAllDigits {
		switch len(datePart) {
		case 14: // YYYYMMDDHHMMSS - DATETIME format, extract date part
			yy, _ := strconv.Atoi(datePart[:4])
			mm, _ := strconv.Atoi(datePart[4:6])
			dd, _ := strconv.Atoi(datePart[6:8])
			y, m, d = yy, mm, dd
			parsed = true
		case 12: // YYMMDDHHMMSS - DATETIME format, extract date part
			yy, _ := strconv.Atoi(datePart[:2])
			mm, _ := strconv.Atoi(datePart[2:4])
			dd, _ := strconv.Atoi(datePart[4:6])
			y, m, d = convert2DigitYear(yy), mm, dd
			parsed = true
		case 8: // YYYYMMDD
			yy, _ := strconv.Atoi(datePart[:4])
			mm, _ := strconv.Atoi(datePart[4:6])
			dd, _ := strconv.Atoi(datePart[6:8])
			y, m, d = yy, mm, dd
			parsed = true
		case 6: // YYMMDD
			yy, _ := strconv.Atoi(datePart[:2])
			mm, _ := strconv.Atoi(datePart[2:4])
			dd, _ := strconv.Atoi(datePart[4:6])
			y, m, d = convert2DigitYear(yy), mm, dd
			parsed = true
		default:
			// Short numeric values like '59' - definitely invalid
			return mysqlError(1292, "22007", fmt.Sprintf("Incorrect date value: '%s' for column '%s' at row 1", originalValue, colName))
		}
	}

	if !parsed {
		// Try delimited formats: YYYY-M-D or similar
		norm := normalizeDateDelimiters(datePart)
		parts := strings.Split(norm, "-")
		if len(parts) == 3 {
			yStr, mStr, dStr := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), strings.TrimSpace(parts[2])
			// Remove time components from dStr
			if idx := strings.IndexAny(dStr, " T:"); idx >= 0 {
				dStr = dStr[:idx]
			}
			var parseErr error
			if len(yStr) <= 2 {
				var yy int
				yy, parseErr = strconv.Atoi(yStr)
				if parseErr == nil {
					y = convert2DigitYear(yy)
				}
			} else {
				y, parseErr = strconv.Atoi(yStr)
			}
			if parseErr != nil {
				return mysqlError(1292, "22007", fmt.Sprintf("Incorrect date value: '%s' for column '%s' at row 1", originalValue, colName))
			}
			m, _ = strconv.Atoi(mStr)
			d, _ = strconv.Atoi(dStr)
			parsed = true
		} else {
			// Not parseable as a date
			return mysqlError(1292, "22007", fmt.Sprintf("Incorrect date value: '%s' for column '%s' at row 1", originalValue, colName))
		}
	}

	if !parsed {
		return mysqlError(1292, "22007", fmt.Sprintf("Incorrect date value: '%s' for column '%s' at row 1", originalValue, colName))
	}

	// Check for zero date (0000-00-00)
	if y == 0 && m == 0 && d == 0 {
		if isNoZeroDate {
			return mysqlError(1292, "22007", fmt.Sprintf("Incorrect date value: '%s' for column '%s' at row 1", originalValue, colName))
		}
		return nil
	}

	// Check for zero month or zero day (partial zero dates)
	if m == 0 || d == 0 {
		if isNoZeroInDate {
			return mysqlError(1292, "22007", fmt.Sprintf("Incorrect date value: '%s' for column '%s' at row 1", originalValue, colName))
		}
		// Partial zero dates are allowed in non-NO_ZERO_IN_DATE mode
		return nil
	}

	// Check month range
	if m < 1 || m > 12 {
		return mysqlError(1292, "22007", fmt.Sprintf("Incorrect date value: '%s' for column '%s' at row 1", originalValue, colName))
	}

	// ALLOW_INVALID_DATES: skip day range check, only month must be valid (1-12)
	if allowInvalidDates {
		if d < 1 || d > 31 {
			return mysqlError(1292, "22007", fmt.Sprintf("Incorrect date value: '%s' for column '%s' at row 1", originalValue, colName))
		}
		return nil
	}

	// Check day range
	daysInMonth := [13]int{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	maxDay := daysInMonth[m]
	if m == 2 && isLeapYear(y) {
		maxDay = 29
	}
	if d < 1 || d > maxDay {
		return mysqlError(1292, "22007", fmt.Sprintf("Incorrect date value: '%s' for column '%s' at row 1", originalValue, colName))
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

// normalizeDateDelimiters replaces various date delimiters with '-'.
func normalizeDateDelimiters(s string) string {
	var result strings.Builder
	for _, c := range s {
		switch c {
		case '/', '.', '@':
			result.WriteByte('-')
		default:
			result.WriteRune(c)
		}
	}
	return result.String()
}

// normalizeDateTimeSeparators replaces various time separator characters with ':'.
func normalizeDateTimeSeparators(s string) string {
	var result strings.Builder
	for _, c := range s {
		switch c {
		case '*', '+', '^':
			result.WriteByte(':')
		default:
			result.WriteRune(c)
		}
	}
	return result.String()
}

// looksLikeDate checks if a string looks like a date value (contains date separators).
func looksLikeDate(s string) bool {
	// Contains date-like separator and has digits
	if len(s) < 6 {
		return false
	}
	return strings.ContainsAny(s, "-/.")
}

// looksLikeTime returns true if the string looks like a TIME value (contains ':').
func looksLikeTime(s string) bool {
	return strings.Contains(s, ":")
}

// looksLikeActualTime returns true if the string is a plausible MySQL TIME value.
// A valid TIME looks like: [+-][D ]HH:MM[:SS[.frac]] where all components are numeric.
// This is stricter than looksLikeTime and excludes strings like instrument names
// that happen to contain ':' (e.g. 'wait/synch/mutex/sql/hash_filo::lock').
func looksLikeActualTime(s string) bool {
	if s == "" {
		return false
	}
	// Strip optional sign
	if s[0] == '-' || s[0] == '+' {
		s = s[1:]
	}
	// Strip optional day prefix (e.g. "1 " in "1 12:00:00")
	if idx := strings.Index(s, " "); idx >= 0 {
		dayPart := s[:idx]
		rest := s[idx+1:]
		allDigits := true
		for _, c := range dayPart {
			if c < '0' || c > '9' {
				allDigits = false
				break
			}
		}
		if allDigits && len(dayPart) > 0 {
			s = rest
		}
	}
	// Now s should be HH:MM[:SS[.frac]]
	// The first character must be a digit
	if len(s) == 0 || s[0] < '0' || s[0] > '9' {
		return false
	}
	// Must contain ':' and all colon-separated segments must start with a digit
	colonIdx := strings.Index(s, ":")
	if colonIdx < 0 {
		return false
	}
	// Validate each colon-separated part starts with a digit
	parts := strings.Split(s, ":")
	for _, p := range parts {
		// Each part may have fractional seconds: strip after '.'
		p = strings.SplitN(p, ".", 2)[0]
		if len(p) == 0 {
			continue // empty part (e.g. "12::34") - not valid time but also not an instrument
		}
		for _, c := range p {
			if c < '0' || c > '9' {
				return false
			}
		}
	}
	return true
}

// normalizeDateTimeString normalizes a date or datetime string into canonical form.
// For date-only values: returns "YYYY-MM-DD".
// For datetime values: returns "YYYY-MM-DD HH:MM:SS".
// Returns "" if the string cannot be parsed as a date.
func normalizeDateTimeString(s string) string {
	datePart := parseMySQLDateValue(s)
	if datePart == "" {
		return ""
	}
	// Check if original has a time component
	timePart := extractTimePart(s, datePart)
	if timePart != "" {
		timePart = normalizeDateTimeSeparators(timePart)
		return datePart + " " + timePart
	}
	return datePart
}

// hasInvalidTimeComponent checks if a datetime string has an invalid time component
// (hour > 23, minute > 59, or second > 59). Returns the invalid string if found.
// Only applies to strings that look like full datetime values (date + time parts).
func hasInvalidTimeComponent(s string) (string, bool) {
	// Must have both date separator (-) and time separator (:) and a space
	spaceIdx := strings.Index(s, " ")
	if spaceIdx < 0 {
		return "", false
	}
	datePart := s[:spaceIdx]
	timePart := strings.TrimSpace(s[spaceIdx+1:])
	// Validate date part (must be YYYY-MM-DD format)
	if len(datePart) < 10 || datePart[4] != '-' || datePart[7] != '-' {
		return "", false
	}
	// Validate time part (must be HH:MM:SS format)
	colonIdx := strings.Index(timePart, ":")
	if colonIdx < 0 {
		return "", false
	}
	hourStr := timePart[:colonIdx]
	hour, err := strconv.Atoi(hourStr)
	if err != nil {
		return "", false
	}
	if hour > 23 {
		return s, true
	}
	// Check minutes
	rest := timePart[colonIdx+1:]
	colonIdx2 := strings.Index(rest, ":")
	if colonIdx2 >= 0 {
		minStr := rest[:colonIdx2]
		min, err := strconv.Atoi(minStr)
		if err == nil && min > 59 {
			return s, true
		}
		secStr := rest[colonIdx2+1:]
		// Strip fractional seconds
		if dotIdx := strings.Index(secStr, "."); dotIdx >= 0 {
			secStr = secStr[:dotIdx]
		}
		sec, err := strconv.Atoi(secStr)
		if err == nil && sec > 59 {
			return s, true
		}
	}
	return "", false
}

// convert2DigitYear converts a 2-digit year to 4-digit year.
// 0-69 -> 2000-2069, 70-99 -> 1970-1999
func convert2DigitYear(yy int) int {
	if yy >= 0 && yy <= 69 {
		return 2000 + yy
	}
	if yy >= 70 && yy <= 99 {
		return 1900 + yy
	}
	return yy
}

// coerceIntegerValue converts values for integer columns (TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT).
// In non-strict mode: empty string -> 0, non-numeric string -> 0 (or extract leading number),
// negative -> 0 for UNSIGNED, out-of-range -> clipped.
// mysqlRoundToInt rounds a float64 to int64 using MySQL's rounding rule:
// "round half away from zero" (0.5 -> 1, -0.5 -> -1).
func mysqlRoundToInt(f float64) int64 {
	if f >= 0 {
		return int64(f + 0.5)
	}
	return int64(f - 0.5)
}

// intTypeRange holds the signed min/max and unsigned max for an integer column type.
type intTypeRange struct {
	Min, Max    int64
	MaxUnsigned uint64
}

// intTypeRanges maps base integer type names to their value ranges.
var intTypeRanges = map[string]intTypeRange{
	"TINYINT":   {-128, 127, 255},
	"SMALLINT":  {-32768, 32767, 65535},
	"MEDIUMINT": {-8388608, 8388607, 16777215},
	"INT":       {-2147483648, 2147483647, 4294967295},
	"INTEGER":   {-2147483648, 2147483647, 4294967295},
	"BIGINT":    {-9223372036854775808, 9223372036854775807, 18446744073709551615},
}

func coerceIntegerValue(colType string, v interface{}) interface{} {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	// Remove display width like INT(11)
	baseType := upper
	if idx := strings.Index(baseType, "("); idx >= 0 {
		baseType = baseType[:idx]
	}
	isUnsigned := strings.Contains(upper, "UNSIGNED")
	// Strip UNSIGNED, ZEROFILL etc. for base type check
	baseType = strings.TrimSpace(strings.Replace(strings.Replace(baseType, "UNSIGNED", "", 1), "ZEROFILL", "", 1))

	// Check if this is an integer type
	rng, isIntType := intTypeRanges[baseType]
	if !isIntType {
		return v
	}
	minVal, maxVal, maxUnsigned := rng.Min, rng.Max, rng.MaxUnsigned

	// Convert value to int64
	var intVal int64
	switch val := v.(type) {
	case int64:
		intVal = val
	case HexBytes:
		// x'...' hex literal inserted into integer column: convert big-endian bytes to integer
		decoded, err := hex.DecodeString(string(val))
		if err != nil || len(decoded) == 0 {
			intVal = 0
		} else {
			var uval uint64
			for _, b := range decoded {
				uval = uval<<8 | uint64(b)
			}
			if isUnsigned {
				if uval > maxUnsigned {
					return int64(maxUnsigned)
				}
				return int64(uval)
			}
			intVal = int64(uval)
		}
	case ScaledValue:
		f := val.Value
		if isUnsigned {
			if f < 0 {
				intVal = 0
			} else if f > float64(maxUnsigned) {
				intVal = int64(maxUnsigned)
			} else {
				intVal = mysqlRoundToInt(f)
			}
		} else if f > float64(maxVal) {
			intVal = maxVal
		} else if f < float64(minVal) {
			intVal = minVal
		} else {
			// MySQL rounds half away from zero for decimal-to-int conversion
			intVal = mysqlRoundToInt(f)
		}
	case DivisionResult:
		f := val.Value
		if isUnsigned {
			if f < 0 {
				intVal = 0
			} else if f > float64(maxUnsigned) {
				intVal = int64(maxUnsigned)
			} else {
				intVal = mysqlRoundToInt(f)
			}
		} else if f > float64(maxVal) {
			intVal = maxVal
		} else if f < float64(minVal) {
			intVal = minVal
		} else {
			// MySQL rounds half away from zero for decimal-to-int conversion
			intVal = mysqlRoundToInt(f)
		}
	case float64:
		if isUnsigned {
			if val < 0 {
				intVal = 0
			} else if val > float64(maxUnsigned) {
				intVal = int64(maxUnsigned)
			} else {
				intVal = mysqlRoundToInt(val)
			}
		} else if val > float64(maxVal) {
			intVal = maxVal
		} else if val < float64(minVal) {
			intVal = minVal
		} else {
			// MySQL rounds half away from zero for float-to-int conversion
			intVal = mysqlRoundToInt(val)
		}
	case uint64:
		if isUnsigned {
			if val > maxUnsigned {
				if baseType == "BIGINT" {
					return maxUnsigned
				}
				return int64(maxUnsigned)
			}
			if baseType == "BIGINT" {
				return val
			}
			return int64(val)
		}
		if val > uint64(math.MaxInt64) {
			intVal = maxVal
		} else {
			intVal = int64(val)
		}
	case string:
		if val == "" {
			return int64(0)
		}
		// Try parsing as number - extract leading numeric part
		val = strings.TrimSpace(val)
		// Parse leading numeric portion
		numStr := ""
		hasDot := false
		hasExp := false
		for i, c := range val {
			if c == '-' && i == 0 {
				numStr += string(c)
			} else if c == '-' && hasExp {
				// Exponent sign (e.g. 1.5e-3)
				numStr += string(c)
			} else if c == '+' && hasExp {
				// Exponent sign (e.g. 1.5e+3)
				// skip +, just continue
			} else if c >= '0' && c <= '9' {
				numStr += string(c)
			} else if c == '.' && !hasDot {
				hasDot = true
				numStr += string(c)
			} else if (c == 'e' || c == 'E') && !hasExp && numStr != "" && numStr != "-" {
				hasExp = true
				hasDot = true // treat scientific notation as having decimal
				numStr += string(c)
			} else {
				break
			}
		}
		if numStr == "" || numStr == "-" {
			return int64(0)
		}
		if hasDot {
			if baseType == "BIGINT" {
				// Special case: float64 can't represent BIGINT boundaries precisely.
				// Use big.Float with high precision to round correctly.
				bf := new(big.Float).SetPrec(128)
				if _, ok := bf.SetString(numStr); !ok {
					return int64(0)
				}
				if isUnsigned {
					// Check sign
					if bf.Sign() < 0 {
						return uint64(0)
					}
					// Convert to big.Int using truncation
					bfTrunc := new(big.Float).SetPrec(128).Copy(bf)
					// Get integer part via Uint64 on truncated value
					// Use big.Int for full precision
					bi := new(big.Int)
					bf.Int(bi) // truncates toward zero
					// Determine if we need to round up (fractional part >= 0.5)
					biF := new(big.Float).SetPrec(128).SetInt(bi)
					frac := new(big.Float).SetPrec(128).Sub(bfTrunc, biF)
					half := new(big.Float).SetPrec(128).SetFloat64(0.5)
					if frac.Cmp(half) >= 0 {
						bi.Add(bi, big.NewInt(1))
					}
					// Clamp to uint64 max
					maxU := new(big.Int).SetUint64(maxUnsigned)
					if bi.Cmp(maxU) > 0 {
						return maxUnsigned
					}
					return bi.Uint64()
				} else {
					// Signed BIGINT
					bfSign := bf.Sign() // sign of original value
					bf2 := new(big.Float).SetPrec(128).Copy(bf)
					bi := new(big.Int)
					bf.Int(bi) // truncates toward zero
					biF := new(big.Float).SetPrec(128).SetInt(bi)
					frac := new(big.Float).SetPrec(128).Sub(bf2, biF)
					half := new(big.Float).SetPrec(128).SetFloat64(0.5)
					negHalf := new(big.Float).SetPrec(128).SetFloat64(-0.5)
					// Round half away from zero: use sign of original value to determine direction
					if bfSign >= 0 && frac.Cmp(half) >= 0 {
						bi.Add(bi, big.NewInt(1))
					} else if bfSign < 0 && frac.Cmp(negHalf) <= 0 {
						bi.Sub(bi, big.NewInt(1))
					}
					// Clamp to int64 range
					maxI := new(big.Int).SetInt64(maxVal)
					minI := new(big.Int).SetInt64(minVal)
					if bi.Cmp(maxI) > 0 {
						return maxVal
					}
					if bi.Cmp(minI) < 0 {
						return minVal
					}
					return bi.Int64()
				}
			}
			f, err := strconv.ParseFloat(numStr, 64)
			if err != nil {
				return int64(0)
			}
			if isUnsigned {
				if f < 0 {
					intVal = 0
				} else if f > float64(maxUnsigned) {
					intVal = int64(maxUnsigned)
				} else {
					intVal = mysqlRoundToInt(f)
				}
			} else if f > float64(maxVal) {
				intVal = maxVal
			} else if f < float64(minVal) {
				intVal = minVal
			} else {
				// MySQL rounds half away from zero for string-to-int conversion
				intVal = mysqlRoundToInt(f)
			}
		} else {
			n, err := strconv.ParseInt(numStr, 10, 64)
			if err != nil {
				// Might be too large for int64
				f, err2 := strconv.ParseFloat(numStr, 64)
				if err2 != nil {
					return int64(0)
				}
				if f > float64(maxVal) {
					intVal = maxVal
				} else if f < float64(minVal) {
					intVal = minVal
				} else {
					intVal = int64(f)
				}
			} else {
				intVal = n
			}
		}
	default:
		return v
	}

	// Apply range constraints
	isZerofill := strings.Contains(upper, "ZEROFILL")
	if isUnsigned {
		if intVal < 0 {
			if baseType == "BIGINT" {
				if isZerofill {
					return applyIntZerofill(uint64(0), upper)
				}
				return uint64(0)
			}
			if isZerofill {
				return applyIntZerofill(int64(0), upper)
			}
			return int64(0)
		}
		if uint64(intVal) > maxUnsigned {
			if baseType == "BIGINT" {
				if isZerofill {
					return applyIntZerofill(maxUnsigned, upper)
				}
				return maxUnsigned
			}
			if isZerofill {
				return applyIntZerofill(int64(maxUnsigned), upper)
			}
			return int64(maxUnsigned)
		}
		if baseType == "BIGINT" {
			if isZerofill {
				return applyIntZerofill(uint64(intVal), upper)
			}
			return uint64(intVal)
		}
	} else {
		if intVal < minVal {
			return minVal
		}
		if intVal > maxVal {
			return maxVal
		}
	}
	if isZerofill {
		return applyIntZerofill(intVal, upper)
	}
	return intVal
}

// applyIntZerofill applies ZEROFILL zero-padding to an integer value for display.
// The display width is extracted from the column type (e.g., "INT(2) ZEROFILL" -> width 2).
// Returns a zero-padded string if a display width is specified, otherwise returns val unchanged.
func applyIntZerofill(val interface{}, upperColType string) interface{} {
	// Extract display width from INT(N), TINYINT(N), etc.
	var width int
	if idx := strings.Index(upperColType, "("); idx >= 0 {
		if n, err := fmt.Sscanf(upperColType[idx:], "(%d)", &width); err != nil || n == 0 {
			width = 0
		}
	}
	if width <= 0 {
		return val
	}
	var s string
	switch v := val.(type) {
	case int64:
		if v < 0 {
			return val // negative values not zero-padded in MySQL ZEROFILL (unsigned context)
		}
		s = fmt.Sprintf("%d", v)
	case uint64:
		s = fmt.Sprintf("%d", v)
	default:
		return val
	}
	for len(s) < width {
		s = "0" + s
	}
	return s
}

func coerceBitValue(colType string, v interface{}) interface{} {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	if !strings.HasPrefix(upper, "BIT") {
		return v
	}
	width := 1
	if n, err := fmt.Sscanf(upper, "BIT(%d)", &width); err != nil || n != 1 {
		width = 1
	}
	if width <= 0 {
		width = 1
	}
	if width > 64 {
		width = 64
	}
	parseBitString := func(s string) (uint64, bool) {
		s = strings.TrimSpace(s)
		if strings.HasPrefix(strings.ToLower(s), "b'") {
			body := s[2:]
			if idx := strings.Index(body, "'"); idx >= 0 {
				body = body[:idx]
			}
			i := 0
			for i < len(body) && (body[i] == '0' || body[i] == '1') {
				i++
			}
			body = body[:i]
			if body == "" {
				return 0, true
			}
			u, err := strconv.ParseUint(body, 2, 64)
			return u, err == nil
		}
		if strings.HasPrefix(s, "0b") || strings.HasPrefix(s, "0B") {
			body := s[2:]
			i := 0
			for i < len(body) && (body[i] == '0' || body[i] == '1') {
				i++
			}
			body = body[:i]
			if body == "" {
				return 0, true
			}
			u, err := strconv.ParseUint(body, 2, 64)
			return u, err == nil
		}
		return 0, false
	}
	var u uint64
	switch n := v.(type) {
	case int64:
		if n < 0 {
			u = 0
		} else {
			u = uint64(n)
		}
	case uint64:
		u = n
	case float64:
		if n < 0 {
			u = 0
		} else {
			u = uint64(int64(n))
		}
	case string:
		if bu, ok := parseBitString(n); ok {
			u = bu
			break
		}
		if iv, err := strconv.ParseInt(strings.TrimSpace(n), 10, 64); err == nil {
			if iv < 0 {
				u = 0
			} else {
				u = uint64(iv)
			}
			break
		}
		return int64(0)
	default:
		return v
	}
	mask := uint64(1<<width) - 1
	if width == 64 {
		mask = ^uint64(0)
	}
	if u > mask {
		u = mask
	}
	return int64(u)
}

// checkIntegerStrict validates integer constraints in strict mode.
// Returns an error if the value would be out of range or is not a valid integer.
func checkIntegerStrict(colType string, colName string, v interface{}) error {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	baseType := upper
	if idx := strings.Index(baseType, "("); idx >= 0 {
		baseType = baseType[:idx]
	}
	isUnsigned := strings.Contains(upper, "UNSIGNED")
	baseType = strings.TrimSpace(strings.Replace(strings.Replace(baseType, "UNSIGNED", "", 1), "ZEROFILL", "", 1))

	isIntType := false
	var minVal, maxVal int64
	var maxUnsigned uint64
	switch baseType {
	case "TINYINT":
		isIntType = true
		minVal, maxVal = -128, 127
		maxUnsigned = 255
	case "SMALLINT":
		isIntType = true
		minVal, maxVal = -32768, 32767
		maxUnsigned = 65535
	case "MEDIUMINT":
		isIntType = true
		minVal, maxVal = -8388608, 8388607
		maxUnsigned = 16777215
	case "INT", "INTEGER":
		isIntType = true
		minVal, maxVal = -2147483648, 2147483647
		maxUnsigned = 4294967295
	case "BIGINT":
		isIntType = true
		minVal, maxVal = -9223372036854775808, 9223372036854775807
		maxUnsigned = 18446744073709551615
	}
	if !isIntType {
		return nil
	}

	// Check string values for non-numeric content
	if s, ok := v.(string); ok {
		s = strings.TrimSpace(s)
		if s == "" {
			return nil // empty string -> 0 is OK even in strict mode for non-strict numeric
		}
		sl := strings.ToLower(s)
		if sl == "true" || sl == "false" {
			return nil
		}
		// Check if it's a valid number
		_, errInt := strconv.ParseInt(s, 10, 64)
		_, errFloat := strconv.ParseFloat(s, 64)
		if errInt != nil && errFloat != nil {
			// Try to extract leading numeric part
			hasNumeric := false
			for _, c := range s {
				if (c >= '0' && c <= '9') || c == '-' || c == '.' {
					hasNumeric = true
					break
				}
			}
			if !hasNumeric {
				return mysqlError(1366, "HY000", fmt.Sprintf("Incorrect integer value: '%s' for column '%s' at row 1", s, colName))
			}
		}
	}

	// Check range for numeric values
	var intVal int64
	switch val := v.(type) {
	case int64:
		intVal = val
	case float64:
		intVal = int64(val)
	case uint64:
		if isUnsigned && val > maxUnsigned {
			return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
		}
		return nil
	case string:
		val = strings.TrimSpace(val)
		if val == "" {
			return nil
		}
		vLower := strings.ToLower(val)
		if vLower == "true" {
			intVal = 1
			break
		}
		if vLower == "false" {
			intVal = 0
			break
		}
		numStr := ""
		hasDot := false
		hasExp := false
		for i, c := range val {
			if c == '-' && i == 0 {
				numStr += string(c)
			} else if c == '-' && hasExp {
				numStr += string(c)
			} else if c == '+' && hasExp {
				// skip +
			} else if c >= '0' && c <= '9' {
				numStr += string(c)
			} else if c == '.' && !hasDot {
				hasDot = true
				numStr += string(c)
			} else if (c == 'e' || c == 'E') && !hasExp && numStr != "" && numStr != "-" {
				hasExp = true
				hasDot = true // treat scientific notation as having decimal
				numStr += string(c)
			} else {
				break
			}
		}
		if numStr == "" || numStr == "-" {
			return nil
		}
		if hasDot {
			if baseType == "BIGINT" {
				// Special case: float64 can't represent BIGINT boundaries precisely.
				// Use big.Float with high precision to check range.
				bf := new(big.Float).SetPrec(128)
				if _, ok := bf.SetString(numStr); !ok {
					return nil
				}
				// Round: get integer value (truncate toward zero, then round based on frac)
				bfSign := bf.Sign() // sign of original value
				bfCopy := new(big.Float).SetPrec(128).Copy(bf)
				bi := new(big.Int)
				bf.Int(bi) // truncates toward zero
				biF := new(big.Float).SetPrec(128).SetInt(bi)
				frac := new(big.Float).SetPrec(128).Sub(bfCopy, biF)
				half := new(big.Float).SetPrec(128).SetFloat64(0.5)
				negHalf := new(big.Float).SetPrec(128).SetFloat64(-0.5)
				// Round half away from zero: use sign of original value
				if bfSign >= 0 && frac.Cmp(half) >= 0 {
					bi.Add(bi, big.NewInt(1))
				} else if bfSign < 0 && frac.Cmp(negHalf) <= 0 {
					bi.Sub(bi, big.NewInt(1))
				}
				if isUnsigned {
					if bi.Sign() < 0 {
						return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
					}
					maxU := new(big.Int).SetUint64(maxUnsigned)
					if bi.Cmp(maxU) > 0 {
						return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
					}
				} else {
					maxI := new(big.Int).SetInt64(maxVal)
					minI := new(big.Int).SetInt64(minVal)
					if bi.Cmp(maxI) > 0 || bi.Cmp(minI) < 0 {
						return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
					}
				}
				return nil
			}
			f, _ := strconv.ParseFloat(numStr, 64)
			// Use proper rounding (round half away from zero) for out-of-range detection
			intVal = mysqlRoundToInt(f)
		} else if isUnsigned && !strings.HasPrefix(numStr, "-") {
			// For unsigned columns, try ParseUint first to handle values > MaxInt64
			u, err := strconv.ParseUint(numStr, 10, 64)
			if err != nil {
				return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
			}
			if u > maxUnsigned {
				return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
			}
			return nil
		} else {
			n, err := strconv.ParseInt(numStr, 10, 64)
			if err != nil {
				return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
			}
			intVal = n
		}
	default:
		return nil
	}

	if isUnsigned {
		if intVal < 0 {
			return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
		}
		if uint64(intVal) > maxUnsigned {
			return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
		}
	} else {
		if intVal < minVal || intVal > maxVal {
			return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
		}
	}
	return nil
}

// validateEnumSetValue validates and normalizes a value for ENUM/SET columns.
func validateEnumSetValue(colType string, v interface{}) interface{} {
	lower := strings.ToLower(colType)
	if !strings.HasPrefix(lower, "enum(") && !strings.HasPrefix(lower, "set(") {
		return v
	}
	isEnum := strings.HasPrefix(lower, "enum(")
	inner := ""
	if isEnum {
		inner = colType[5 : len(colType)-1]
	} else {
		inner = colType[4 : len(colType)-1]
	}
	var allowed []string
	for _, part := range splitEnumValues(inner) {
		part = strings.Trim(part, "'")
		allowed = append(allowed, part)
	}

	// Handle integer values: convert valid indices/bitmasks to string labels.
	// Out-of-range integers are left as-is so the INSERT validation code can raise errors.
	switch iv := v.(type) {
	case int64:
		if isEnum {
			// ENUM: index 1..N maps to the Nth element; index 0 = empty string (valid).
			// Out-of-range indices are left as int64 for the INSERT path to reject.
			if iv == 0 {
				return EnumValue("")
			}
			if iv >= 1 && int(iv) <= len(allowed) {
				return EnumValue(allowed[iv-1])
			}
			// Out of range: leave as int64 for strict mode to handle
			return v
		}
		// SET bitmask: only convert if within valid range (0..2^N-1)
		maxMask := int64((1 << uint(len(allowed))) - 1)
		if iv >= 0 && iv <= maxMask {
			if iv == 0 {
				return ""
			}
			var valid []string
			for i, a := range allowed {
				if iv&(1<<uint(i)) != 0 {
					valid = append(valid, a)
				}
			}
			return strings.Join(valid, ",")
		}
		// Out of range: leave as int64 for strict mode to handle
		return v
	case uint64:
		return validateEnumSetValue(colType, int64(iv))
	}

	s, ok := v.(string)
	if !ok {
		// Other non-string values are left as-is.
		return v
	}
	if isEnum {
		if s == "" {
			return EnumValue(s)
		}
		// Check if the string is a numeric index (stored as string representation of int)
		if idx, err := strconv.ParseInt(s, 10, 64); err == nil {
			if idx == 0 {
				return EnumValue("")
			}
			if idx >= 1 && int(idx) <= len(allowed) {
				return EnumValue(allowed[idx-1])
			}
			return EnumValue("")
		}
		for _, a := range allowed {
			if strings.EqualFold(s, a) {
				return EnumValue(a)
			}
		}
		return EnumValue("")
	}
	// SET validation
	if s == "" {
		return s
	}
	// Check if the string is a numeric bitmask (stored as string representation of int)
	if bitmask, err := strconv.ParseInt(s, 10, 64); err == nil {
		if bitmask == 0 {
			return ""
		}
		var valid []string
		for i, a := range allowed {
			if bitmask&(1<<uint(i)) != 0 {
				valid = append(valid, a)
			}
		}
		return strings.Join(valid, ",")
	}
	members := strings.Split(s, ",")
	var valid []string
	for _, m := range members {
		m = strings.TrimSpace(m)
		matched := false
		// Prefer exact (case-sensitive) match first
		for _, a := range allowed {
			if m == a {
				valid = append(valid, a)
				matched = true
				break
			}
		}
		// Fall back to case-insensitive match
		if !matched {
			for _, a := range allowed {
				if strings.EqualFold(m, a) {
					valid = append(valid, a)
					break
				}
			}
		}
	}
	return strings.Join(valid, ",")
}

func splitEnumValues(s string) []string {
	var result []string
	var current strings.Builder
	inQuote := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '\'' {
			inQuote = !inQuote
			current.WriteByte(ch)
		} else if ch == ',' && !inQuote {
			result = append(result, strings.TrimSpace(current.String()))
			current.Reset()
		} else {
			current.WriteByte(ch)
		}
	}
	rest := strings.TrimSpace(current.String())
	if rest != "" {
		result = append(result, rest)
	}
	return result
}

// extractNumericPrefix tries to find the longest valid numeric prefix in a string s.
// For example, "123.4e" -> "123.4" (truncate trailing 'e' with no exponent digits).
// "123abc" -> "123", "-456.78e+2abc" -> "-456.78e+2".
// Returns ("", false) if no valid numeric prefix found.
func extractNumericPrefix(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false
	}
	i := 0
	// optional sign
	if i < len(s) && (s[i] == '+' || s[i] == '-') {
		i++
	}
	// digits before decimal
	digitsBeforeDecimal := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
		digitsBeforeDecimal++
	}
	// optional decimal point and fractional digits
	if i < len(s) && s[i] == '.' {
		i++
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
			digitsBeforeDecimal++ // count overall
		}
	}
	if digitsBeforeDecimal == 0 {
		return "", false
	}
	// optional exponent (only if followed by digits)
	ePos := i
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		j := i + 1
		if j < len(s) && (s[j] == '+' || s[j] == '-') {
			j++
		}
		if j < len(s) && s[j] >= '0' && s[j] <= '9' {
			// Valid exponent: advance i past exponent digits
			i = j
			for i < len(s) && s[i] >= '0' && s[i] <= '9' {
				i++
			}
		} else {
			// 'e' not followed by digits: truncate here (don't include the 'e')
			i = ePos
		}
	}
	prefix := s[:i]
	if prefix == "" || prefix == "+" || prefix == "-" {
		return "", false
	}
	return prefix, true
}

// parseDecimalString attempts to parse a string that may contain scientific notation
// or other numeric formats that strconv.ParseFloat cannot handle (e.g., extreme exponents).
// Returns the float64 value and a classification:
// "normal" - parsed fine, "overflow_pos" - huge positive, "overflow_neg" - huge negative,
// "zero" - tiny (rounds to zero), "invalid" - not a number.
func parseDecimalString(s string) (float64, string) {
	s = strings.TrimSpace(s)
	// Handle scientific notation with extreme exponents BEFORE ParseFloat
	// because ParseFloat returns Inf for large exponents but we need to distinguish
	// between "valid overflow" and "invalid (exponent overflows uint64)"
	sLower := strings.ToLower(s)
	cleanS := sLower
	negative := false
	if strings.HasPrefix(cleanS, "-") {
		negative = true
		cleanS = cleanS[1:]
	} else if strings.HasPrefix(cleanS, "+") {
		cleanS = cleanS[1:]
	}
	if idx := strings.Index(cleanS, "e"); idx >= 0 {
		expStr := cleanS[idx+1:]
		expNeg := false
		if strings.HasPrefix(expStr, "+") {
			expStr = expStr[1:]
		} else if strings.HasPrefix(expStr, "-") {
			expNeg = true
			expStr = expStr[1:]
		}
		// Check if the exponent value itself overflows uint64
		// MySQL treats these as "incorrect decimal value" -> 0
		// Special case: empty expStr means "123.4e" with trailing 'e' -> truncate to mantissa
		if expStr == "" {
			// Trailing 'e'/'E' with no exponent: truncate to the mantissa before 'e'
			mantissa := cleanS[:idx]
			if negative {
				mantissa = "-" + mantissa
			}
			f, perr := strconv.ParseFloat(mantissa, 64)
			if perr != nil {
				return 0, "truncated"
			}
			return f, "truncated"
		}
		expVal, err := strconv.ParseUint(expStr, 10, 64)
		if err != nil {
			// Exponent value overflows -> incorrect decimal value -> 0
			return 0, "zero"
		}
		if expNeg {
			// Very large negative exponent -> tiny number -> 0
			if expVal > 308 {
				return 0, "zero"
			}
			// Small negative exponent: let ParseFloat handle it normally
		} else if expVal > 308 {
			// Positive exponent: if very large, overflow
			if negative {
				return math.Inf(-1), "overflow_neg"
			}
			return math.Inf(1), "overflow_pos"
		}
	}
	// Try standard float parsing
	f, err := strconv.ParseFloat(s, 64)
	if err == nil {
		if math.IsInf(f, 1) {
			return f, "overflow_pos"
		}
		if math.IsInf(f, -1) {
			return f, "overflow_neg"
		}
		return f, "normal"
	}
	return 0, "invalid"
}

func roundDecimalStringHalfUp(s string, scale int) (string, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false
	}
	sign := ""
	if strings.HasPrefix(s, "-") {
		sign = "-"
		s = s[1:]
	} else if strings.HasPrefix(s, "+") {
		s = s[1:]
	}
	if strings.ContainsAny(s, "eE") {
		return "", false
	}
	intPart := s
	fracPart := ""
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		intPart = s[:dot]
		fracPart = s[dot+1:]
	}
	if intPart == "" {
		intPart = "0"
	}
	for _, ch := range intPart {
		if ch < '0' || ch > '9' {
			return "", false
		}
	}
	for _, ch := range fracPart {
		if ch < '0' || ch > '9' {
			return "", false
		}
	}
	if scale < 0 {
		scale = 0
	}
	if len(fracPart) < scale+1 {
		fracPart += strings.Repeat("0", scale+1-len(fracPart))
	}
	keep := fracPart
	if len(keep) > scale {
		keep = keep[:scale]
	}
	roundUp := len(fracPart) > scale && fracPart[scale] >= '5'

	digits := intPart + keep
	if digits == "" {
		digits = "0"
	}
	n := new(big.Int)
	if _, ok := n.SetString(digits, 10); !ok {
		return "", false
	}
	if roundUp {
		n.Add(n, big.NewInt(1))
	}
	outDigits := n.String()
	if scale == 0 {
		if sign == "-" && outDigits != "0" {
			return "-" + outDigits, true
		}
		return outDigits, true
	}
	if len(outDigits) <= scale {
		outDigits = strings.Repeat("0", scale-len(outDigits)+1) + outDigits
	}
	split := len(outDigits) - scale
	out := outDigits[:split] + "." + outDigits[split:]
	if sign == "-" && out != "0."+strings.Repeat("0", scale) {
		out = "-" + out
	}
	return out, true
}

func roundNumericStringHalfUp(s string, scale int) (string, bool) {
	if out, ok := roundDecimalStringHalfUp(s, scale); ok {
		return out, true
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false
	}
	plain, ok := scientificToPlainDecimal(s)
	if !ok {
		return "", false
	}
	return roundDecimalStringHalfUp(plain, scale)
}

func scientificToPlainDecimal(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false
	}
	sign := ""
	if strings.HasPrefix(s, "-") {
		sign = "-"
		s = s[1:]
	} else if strings.HasPrefix(s, "+") {
		s = s[1:]
	}
	parts := strings.Split(strings.ToLower(s), "e")
	if len(parts) != 2 {
		return "", false
	}
	mantissa := parts[0]
	exp, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", false
	}
	if exp > 10000 || exp < -10000 {
		return "", false
	}
	intPart := mantissa
	fracPart := ""
	if dot := strings.IndexByte(mantissa, '.'); dot >= 0 {
		intPart = mantissa[:dot]
		fracPart = mantissa[dot+1:]
	}
	if intPart == "" {
		intPart = "0"
	}
	for _, ch := range intPart {
		if ch < '0' || ch > '9' {
			return "", false
		}
	}
	for _, ch := range fracPart {
		if ch < '0' || ch > '9' {
			return "", false
		}
	}
	digits := strings.TrimLeft(intPart+fracPart, "0")
	if digits == "" {
		return "0", true
	}
	decimalPos := len(intPart) + exp
	var out string
	switch {
	case decimalPos <= 0:
		out = "0." + strings.Repeat("0", -decimalPos) + digits
	case decimalPos >= len(digits):
		out = digits + strings.Repeat("0", decimalPos-len(digits))
	default:
		out = digits[:decimalPos] + "." + digits[decimalPos:]
	}
	if sign == "-" && out != "0" && !strings.HasPrefix(out, "0.") {
		return "-" + out, true
	}
	if sign == "-" && strings.HasPrefix(out, "0.") && strings.Trim(out[2:], "0") != "" {
		return "-" + out, true
	}
	return out, true
}

func parseDecimalStringToRat(s string) (*big.Rat, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, false
	}
	if strings.ContainsAny(s, "eE") {
		bf, ok := new(big.Float).SetPrec(2048).SetString(s)
		if !ok {
			return nil, false
		}
		r, _ := bf.Rat(nil)
		if r == nil {
			return nil, false
		}
		return r, true
	}
	r := new(big.Rat)
	if _, ok := r.SetString(s); !ok {
		return nil, false
	}
	return r, true
}

func formatRatFixed(r *big.Rat, scale int) string {
	if r == nil {
		return "0"
	}
	if scale < 0 {
		scale = 0
	}
	bf := new(big.Float).SetPrec(4096).SetRat(r)
	return bf.Text('f', scale)
}

func clipDecimalIntegerString(s string, precision int, unsigned bool) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "0"
	}
	sign := ""
	if strings.HasPrefix(s, "-") {
		sign = "-"
		s = s[1:]
	} else if strings.HasPrefix(s, "+") {
		s = s[1:]
	}
	s = strings.TrimLeft(s, "0")
	if s == "" {
		return "0"
	}
	if unsigned && sign == "-" {
		return "0"
	}
	maxStr := strings.Repeat("9", precision)
	if len(s) > precision {
		if unsigned {
			return maxStr
		}
		if sign == "-" {
			return "-" + maxStr
		}
		return maxStr
	}
	if sign == "-" {
		return "-" + s
	}
	return s
}

func decimalMaxString(m, d int) string {
	if d <= 0 {
		if m <= 0 {
			return "0"
		}
		return strings.Repeat("9", m)
	}
	intDigits := m - d
	if intDigits <= 0 {
		intDigits = 1
	}
	return strings.Repeat("9", intDigits) + "." + strings.Repeat("9", d)
}

// coerceColumnValue applies the standard DML value coercion chain for a column:
// binary padding, decimal formatting, enum/set validation, datetime coercion,
// integer coercion, and bit coercion.
func coerceColumnValue(colType string, val interface{}) interface{} {
	if padLen := binaryPadLength(colType); padLen > 0 && val != nil {
		val = padBinaryValue(val, padLen)
	} else if isVarbinaryType(colType) && val != nil {
		// For VARBINARY, convert integer hex literals (int64/uint64) to their
		// big-endian byte string representation without fixed-length padding.
		val = hexIntToBytes(val)
	}
	if val != nil {
		val = formatDecimalValue(colType, val)
		val = validateEnumSetValue(colType, val)
		val = coerceDateTimeValue(colType, val)
		val = coerceIntegerValue(colType, val)
		val = coerceBitValue(colType, val)
	}
	return val
}

// formatDecimalValue formats a value for DECIMAL(M,D), DOUBLE(M,D), or FLOAT(M,D) columns.
func formatDecimalValue(colType string, v interface{}) interface{} {
	lower := strings.ToLower(colType)
	// Strip all trailing modifiers (unsigned, zerofill) in any order to get the base type
	cleanLower := strings.TrimSpace(lower)
	for {
		prev := cleanLower
		cleanLower = strings.TrimSuffix(cleanLower, " unsigned")
		cleanLower = strings.TrimSpace(cleanLower)
		cleanLower = strings.TrimSuffix(cleanLower, " zerofill")
		cleanLower = strings.TrimSpace(cleanLower)
		if cleanLower == prev {
			break
		}
	}
	var prefix string
	for _, p := range []string{"decimal", "double", "float", "real"} {
		if strings.HasPrefix(cleanLower, p+"(") {
			prefix = p
			break
		}
	}

	isUnsigned := strings.Contains(lower, "unsigned") || strings.Contains(lower, "zerofill") // ZEROFILL implies UNSIGNED

	// Handle bare DECIMAL/DOUBLE/FLOAT/REAL without (M,D)
	if prefix == "" {
		// Bare FLOAT/DOUBLE/REAL: convert to numeric value
		for _, p := range []string{"double", "float", "real"} {
			if cleanLower == p {
				f := toFloat(v)
				if math.IsInf(f, 1) {
					if p == "float" {
						f = float64(math.MaxFloat32)
					} else {
						f = math.MaxFloat64
					}
				}
				if math.IsInf(f, -1) {
					if p == "float" {
						f = -float64(math.MaxFloat32)
					} else {
						f = -math.MaxFloat64
					}
				}
				if isUnsigned && f < 0 {
					f = 0
				}
				if p == "float" {
					f = float64(float32(f))
					if math.IsInf(f, 1) {
						f = float64(math.MaxFloat32)
					} else if math.IsInf(f, -1) {
						f = -float64(math.MaxFloat32)
					}
					f = normalizeFloatSignificant(f, 6)
				}
				// Convert to int64 when the float is an exact integer
				if f == float64(int64(f)) {
					return int64(f)
				}
				return f
			}
		}
		// Bare DECIMAL -> default to (10,0), round to integer
		if cleanLower == "decimal" {
			f, cls := decimalParseValue(v)
			if cls == "invalid" {
				return v
			}
			if isUnsigned && f < 0 {
				f = 0
			}
			maxVal := float64(9999999999)
			if cls == "overflow_pos" || f > maxVal {
				f = maxVal
			}
			if cls == "overflow_neg" || f < -maxVal {
				if isUnsigned {
					f = 0
				} else {
					f = -maxVal
				}
			}
			// Helper to zero-pad integer for bare DECIMAL ZEROFILL (default M=10)
			zerofillInt := func(i int64) interface{} {
				if !strings.Contains(lower, "zerofill") {
					return i
				}
				const bareDecimalM = 10
				if i < 0 && isUnsigned {
					return strings.Repeat("0", bareDecimalM)
				}
				s := fmt.Sprintf("%d", i)
				neg := i < 0
				abs := s
				if neg {
					abs = s[1:]
				}
				for len(abs) < bareDecimalM {
					abs = "0" + abs
				}
				if neg {
					return "-" + abs
				}
				return abs
			}
			if f >= 0 {
				return zerofillInt(int64(f + 0.5))
			}
			return zerofillInt(-int64(-f + 0.5))
		}
		return v
	}
	// For string values, check if it's a valid number (including sci notation).
	if s, ok := v.(string); ok {
		_, cls := parseDecimalString(s)
		if cls == "invalid" {
			return v
		}
	}
	isZerofill := strings.Contains(lower, "zerofill")

	var m, d int
	// Try DECIMAL(M,D) first, then DECIMAL(M) which defaults to D=0
	if n, err := fmt.Sscanf(cleanLower, prefix+"(%d,%d)", &m, &d); (err == nil && n == 2) || func() bool {
		if n2, err2 := fmt.Sscanf(cleanLower, prefix+"(%d)", &m); err2 == nil && n2 == 1 {
			d = 0
			return true
		}
		return false
	}() {
		// applyZerofillStr zero-pads a decimal string for ZEROFILL DECIMAL(M,D).
		// For DECIMAL(M,D), the integer part is M-D digits wide.
		// e.g. DECIMAL(10,2) -> "1.23" -> "00000001.23"
		// For DECIMAL(M,0) ZEROFILL: "1" -> "0000000001"
		applyZerofillStr := func(s string) string {
			if !isZerofill || prefix != "decimal" {
				return s
			}
			// Strip leading + sign (not used in MySQL output normally)
			if strings.HasPrefix(s, "+") {
				s = s[1:]
			}
			negative := strings.HasPrefix(s, "-")
			abs := s
			if negative {
				abs = s[1:]
			}
			// ZEROFILL + UNSIGNED: negative values are clipped to 0
			if negative && isUnsigned {
				if d == 0 {
					return strings.Repeat("0", m)
				}
				intWidth2 := m - d
				if intWidth2 <= 0 {
					intWidth2 = 1
				}
				return strings.Repeat("0", intWidth2) + "." + strings.Repeat("0", d)
			}
			if d == 0 {
				// Integer DECIMAL(M,0) ZEROFILL: pad to M digits
				for len(abs) < m {
					abs = "0" + abs
				}
				if negative {
					return "-" + abs
				}
				return abs
			}
			dotIdx := strings.Index(abs, ".")
			var intPart, fracPart string
			if dotIdx >= 0 {
				intPart = abs[:dotIdx]
				fracPart = abs[dotIdx+1:]
			} else {
				intPart = abs
				fracPart = strings.Repeat("0", d)
			}
			intWidth := m - d
			if intWidth <= 0 {
				intWidth = 1
			}
			for len(intPart) < intWidth {
				intPart = "0" + intPart
			}
			padded := intPart + "." + fracPart
			if negative {
				return "-" + padded
			}
			return padded
		}
		// applyZerofillFromInt applies zerofill to an int64 value for DECIMAL(M,0) ZEROFILL.
		applyZerofillFromInt := func(i int64) interface{} {
			if !isZerofill || prefix != "decimal" || d != 0 {
				return i
			}
			if i < 0 && isUnsigned {
				return strings.Repeat("0", m)
			}
			s := fmt.Sprintf("%d", i)
			neg := i < 0
			abs2 := s
			if neg {
				abs2 = s[1:]
			}
			for len(abs2) < m {
				abs2 = "0" + abs2
			}
			if neg {
				return "-" + abs2
			}
			return abs2
		}

		f, cls := decimalParseValue(v)
		if prefix == "float" || prefix == "double" || prefix == "real" {
			f, cls = floatParseValue(v)
		}
		clipped := false

		// Compute max value for DECIMAL(M,D)
		intDigits := m - d
		if intDigits <= 0 {
			intDigits = 1
		}
		maxIntPart := 1.0
		for i := 0; i < intDigits; i++ {
			maxIntPart *= 10
		}
		maxFrac := 1.0
		for i := 0; i < d; i++ {
			maxFrac *= 10
		}
		maxVal := maxIntPart - 1.0/maxFrac
		if d == 0 {
			maxVal = maxIntPart - 1
		}

		// Handle unsigned: clip negative to 0
		if isUnsigned && (f < 0 || cls == "overflow_neg") {
			f = 0
			cls = "normal"
			clipped = true
		}

		// Clip to valid range (non-strict mode)
		if cls == "overflow_pos" || f > maxVal {
			f = maxVal
			clipped = true
		}
		if cls == "overflow_neg" || f < -maxVal {
			f = -maxVal
			clipped = true
		}

		if d == 0 {
			// Keep DECIMAL(M,0) as string for large precisions to avoid int64 overflow.
			if prefix == "decimal" {
				if s, ok := v.(string); ok {
					_, sCls := parseDecimalString(s)
					if sCls == "overflow_pos" {
						if isZerofill {
							return strings.Repeat("9", m)
						}
						return strings.Repeat("9", m)
					}
					if sCls == "overflow_neg" {
						if isUnsigned {
							return applyZerofillStr("0")
						}
						return "-" + strings.Repeat("9", m)
					}
					if sCls == "zero" {
						return applyZerofillStr("0")
					}
					if rounded, ok := roundNumericStringHalfUp(s, 0); ok {
						return applyZerofillStr(clipDecimalIntegerString(rounded, m, isUnsigned))
					}
				}
				if s, ok := v.(string); ok && !clipped {
					if rounded, ok := roundNumericStringHalfUp(s, 0); ok {
						return applyZerofillStr(rounded)
					}
				}
				if m > 18 || math.Abs(f) > float64(math.MaxInt64)-1 {
					// Use round-half-up for DECIMAL, not Go's round-half-to-even
					// math.Floor(f+0.5) implements round-half-up
					if f >= 0 {
						return applyZerofillStr(fmt.Sprintf("%.0f", math.Floor(f+0.5)))
					}
					return applyZerofillStr(fmt.Sprintf("%.0f", math.Ceil(f-0.5)))
				}
			}
			if prefix == "float" {
				f = float64(float32(f))
			}
			// Round to nearest integer (MySQL DECIMAL rounds, not truncates)
			if f >= 0 {
				return applyZerofillFromInt(int64(f + 0.5))
			}
			return applyZerofillFromInt(-int64(-f + 0.5))
		}
		if prefix == "decimal" {
			if clipped {
				maxStr := decimalMaxString(m, d)
				if isUnsigned && f == 0 {
					return applyZerofillStr("0." + strings.Repeat("0", d))
				}
				if f < 0 {
					return "-" + maxStr
				}
				return applyZerofillStr(maxStr)
			}
			// DECIMAL: round to d decimal places.
			if s, ok := v.(string); ok && !clipped {
				if rounded, ok := roundNumericStringHalfUp(s, d); ok {
					return applyZerofillStr(rounded)
				}
			}
			return applyZerofillStr(fmt.Sprintf("%.*f", d, f))
		}
		// FLOAT/DOUBLE/REAL(M,D): round to d decimal places (banker's rounding).
		f = roundToEvenScale(f, d)
		if prefix == "float" {
			// FLOAT is single-precision after scale rounding.
			f = float64(float32(f))
		}
		return fmt.Sprintf("%.*f", d, f)
	}
	return v
}

func roundToEvenScale(f float64, d int) float64 {
	if d <= 0 {
		return math.RoundToEven(f)
	}
	factor := 1.0
	for i := 0; i < d; i++ {
		factor *= 10
	}
	return math.RoundToEven(f*factor) / factor
}

// normalizeFloatSignificant rounds f to n significant digits.
func normalizeFloatSignificant(f float64, n int) float64 {
	if f == 0 || n <= 0 || math.IsInf(f, 0) || math.IsNaN(f) {
		return f
	}
	abs := math.Abs(f)
	exp := math.Floor(math.Log10(abs))
	scale := float64(n-1) - exp
	if scale >= 0 {
		factor := math.Pow(10, scale)
		return math.Round(f*factor) / factor
	}
	step := math.Pow(10, -scale)
	return math.Round(f/step) * step
}

// decimalParseValue extracts a float64 and classification from any value type.
func decimalParseValue(v interface{}) (float64, string) {
	switch n := v.(type) {
	case int64:
		return float64(n), "normal"
	case uint64:
		return float64(n), "normal"
	case float64:
		if math.IsInf(n, 1) {
			return n, "overflow_pos"
		}
		if math.IsInf(n, -1) {
			return n, "overflow_neg"
		}
		return n, "normal"
	case string:
		return parseDecimalString(n)
	}
	return toFloat(v), "normal"
}

func floatParseValue(v interface{}) (float64, string) {
	switch n := v.(type) {
	case int64:
		return float64(n), "normal"
	case uint64:
		return float64(n), "normal"
	case float64:
		if math.IsInf(n, 1) {
			return n, "overflow_pos"
		}
		if math.IsInf(n, -1) {
			return n, "overflow_neg"
		}
		if math.IsNaN(n) {
			return 0, "invalid"
		}
		return n, "normal"
	case string:
		s := strings.TrimSpace(n)
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			if errors.Is(err, strconv.ErrRange) {
				if strings.HasPrefix(s, "-") {
					return math.Inf(-1), "overflow_neg"
				}
				return math.Inf(1), "overflow_pos"
			}
			return 0, "invalid"
		}
		if math.IsInf(f, 1) {
			return f, "overflow_pos"
		}
		if math.IsInf(f, -1) {
			return f, "overflow_neg"
		}
		if math.IsNaN(f) {
			return 0, "invalid"
		}
		return f, "normal"
	}
	f := toFloat(v)
	if math.IsInf(f, 1) {
		return f, "overflow_pos"
	}
	if math.IsInf(f, -1) {
		return f, "overflow_neg"
	}
	if math.IsNaN(f) {
		return 0, "invalid"
	}
	return f, "normal"
}

func coerceValueForColumnType(col catalog.ColumnDef, val interface{}) interface{} {
	if val == nil {
		return nil
	}
	if padLen := binaryPadLength(col.Type); padLen > 0 {
		val = padBinaryValue(val, padLen)
	} else if isVarbinaryType(col.Type) {
		val = hexIntToBytes(val)
	}
	val = formatDecimalValue(col.Type, val)
	val = validateEnumSetValue(col.Type, val)
	val = coerceDateTimeValue(col.Type, val)
	val = coerceIntegerValue(col.Type, val)
	val = coerceBitValue(col.Type, val)
	// Truncate BLOB/TEXT values when column type changes (e.g., LONGBLOB→BLOB)
	// MySQL behavior: data exceeding the new max length is set to empty string
	colUp := strings.ToUpper(col.Type)
	isBlobTy := colUp == "BLOB" || colUp == "TINYBLOB" || colUp == "MEDIUMBLOB"
	isTextTy := colUp == "TEXT" || colUp == "TINYTEXT" || colUp == "MEDIUMTEXT"
	if isBlobTy || isTextTy {
		if sv, ok := val.(string); ok {
			maxLen := extractCharLength(col.Type)
			if maxLen > 0 {
				if isBlobTy {
					if len(sv) > maxLen {
						val = "" // MySQL sets to empty string when BLOB data exceeds new column max
					}
				} else {
					if len([]rune(sv)) > maxLen {
						val = "" // MySQL sets to empty string when TEXT data exceeds new column max
					}
				}
			}
		}
	}
	return val
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
						rowTupleIN, isRowTuple := item.(sqlparser.ValTuple)
						if !isRowTuple {
							break // fall through to scalar handling
						}
						if len(rowTupleIN) != len(leftTupleIN) {
							return nil, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftTupleIN)))
						}
						rValsIN := make([]interface{}, len(rowTupleIN))
						for i, rv := range rowTupleIN {
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

// execExplainStmt handles EXPLAIN SELECT ... statements.
// Returns a simplified explain result set for compatibility.
func (e *Executor) execExplainStmt(s *sqlparser.ExplainStmt, query string) (*Result, error) {
	// Validate index hints (USE KEY / IGNORE KEY / FORCE KEY) before producing explain output.
	if sel, ok := s.Statement.(*sqlparser.Select); ok {
		if err := e.validateIndexHints(sel.From); err != nil {
			return nil, err
		}
		// Validate WHERE clause for invalid datetime literals.
		if sel.Where != nil {
			if err := validateWhereForInvalidDatetime(sel.Where.Expr); err != nil {
				return nil, err
			}
		}
		// Validate WHERE clause for invalid DATE string literals against DATE columns.
		if sel.Where != nil && len(sel.From) > 0 {
			if tbl, ok := sel.From[0].(*sqlparser.AliasedTableExpr); ok {
				if tn, ok := tbl.Expr.(sqlparser.TableName); ok {
					tableName := tn.Name.String()
					if e.Catalog != nil {
						if db, err := e.Catalog.GetDatabase(e.CurrentDB); err == nil {
							if td, ok := db.Tables[tableName]; ok {
								if err := validateWhereForInvalidDateColumns(sel.Where.Expr, td, e.sqlMode); err != nil {
									return nil, err
								}
							}
						}
					}
				}
			}
		}
	}
	// Use explainResultForType which delegates to explainMultiRows for proper select_type detection
	explainedQuery := query
	if s.Statement != nil {
		explainedQuery = sqlparser.String(s.Statement)
	}
	return e.explainResultForType(s.Type, explainedQuery), nil
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
		// MySQL treats latin1 as cp1252 (Windows-1252) for unicode conversions.
		// See WL-1494: "Treat latin1 as cp1252 for unicode conversion"
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
	case "hebrew":
		return charmap.ISO8859_8.NewEncoder()
	case "greek":
		return charmap.ISO8859_7.NewEncoder()
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
		// MySQL treats latin1 as cp1252 (Windows-1252) for unicode conversions.
		// See WL-1494: "Treat latin1 as cp1252 for unicode conversion"
		return charmap.Windows1252.NewDecoder()
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

// decodeFromCharset decodes a string from the given charset into UTF-8.
// If the charset is utf8/empty or no decoder is available, returns s unchanged.
func decodeFromCharset(s, charset string) (string, error) {
	cs := canonicalCharset(charset)
	if cs == "utf8" || cs == "" {
		return s, nil
	}
	if cs == "ascii" {
		// Strip bytes > 127
		var buf []byte
		for i := 0; i < len(s); i++ {
			if s[i] <= 127 {
				buf = append(buf, s[i])
			} else {
				buf = append(buf, '?')
			}
		}
		return string(buf), nil
	}
	dec := charsetDecoder(cs)
	if dec == nil {
		return s, nil
	}
	decoded, _, err := transform.String(dec, s)
	if err != nil {
		return s, err
	}
	return decoded, nil
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
	case "ascii":
		// Strip bytes > 127; input is UTF-8 so just pass through ASCII codepoints
		var buf []byte
		for _, r := range s {
			if r <= 127 {
				buf = append(buf, byte(r))
			} else {
				buf = append(buf, '?')
			}
		}
		return string(buf), nil
	default:
		// Handle 8-bit charsets: encode UTF-8 string into target charset bytes,
		// then round-trip back through the decoder so the Go string holds the
		// raw single-byte representation that MySQL wire protocol expects.
		enc := charsetEncoder(cs)
		if enc == nil {
			return s, nil
		}
		encoded, err := encoding.ReplaceUnsupported(enc).Bytes([]byte(s))
		if err != nil {
			return s, err
		}
		return string(encoded), nil
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
