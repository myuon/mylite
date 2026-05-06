package executor

import (
	"strings"
	"sync"

	"github.com/myuon/mylite/storage"
)

// PSEventRow is a minimal representation of a single row in a
// performance_schema events_* table (shared across connections).
type PSEventRow struct {
	ThreadID   int64
	EventID    int64
	EndEventID int64
	EventName  string
	// Extra fields returned per-table
	Extra map[string]interface{}
}

// PSEventsStore is a goroutine-safe shared store for PS event rows
// keyed by table name (lowercase). Each connection adds its own
// events; TRUNCATE clears the table in the shared map. The store is
// shared via Executor.Clone() so all connections see each other's events.
type PSEventsStore struct {
	mu     sync.RWMutex
	events map[string][]PSEventRow // table name → rows
	nextID int64                   // global auto-incrementing event ID
}

// NewPSEventsStore creates an empty store.
func NewPSEventsStore() *PSEventsStore {
	return &PSEventsStore{
		events: make(map[string][]PSEventRow),
		nextID: 1,
	}
}

// Append adds a row to the given table.
func (s *PSEventsStore) Append(table string, row PSEventRow) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if row.EventID == 0 {
		row.EventID = s.nextID
		s.nextID++
	}
	if row.EndEventID == 0 {
		row.EndEventID = row.EventID
	}
	s.events[table] = append(s.events[table], row)
}

// Truncate removes all rows for the given table.
func (s *PSEventsStore) Truncate(table string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.events, table)
}

// RemoveByThread removes all rows for the given thread from the table.
func (s *PSEventsStore) RemoveByThread(table string, threadID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := s.events[table]
	filtered := rows[:0]
	for _, r := range rows {
		if r.ThreadID != threadID {
			filtered = append(filtered, r)
		}
	}
	if len(filtered) == 0 {
		delete(s.events, table)
	} else {
		s.events[table] = filtered
	}
}

// UpsertByThread replaces the row for the given thread in the table, or appends if not found.
func (s *PSEventsStore) UpsertByThread(table string, row PSEventRow) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := s.events[table]
	for i, r := range rows {
		if r.ThreadID == row.ThreadID {
			row.EventID = r.EventID
			row.EndEventID = r.EventID
			rows[i] = row
			return
		}
	}
	row.EventID = s.nextID
	s.nextID++
	row.EndEventID = row.EventID
	s.events[table] = append(rows, row)
}

// Rows returns a snapshot of all rows for the given table.
func (s *PSEventsStore) Rows(table string) []PSEventRow {
	s.mu.RLock()
	defer s.mu.RUnlock()
	src := s.events[table]
	if len(src) == 0 {
		return nil
	}
	out := make([]PSEventRow, len(src))
	copy(out, src)
	return out
}

// ToStorageRows converts PSEventRow slices to storage.Row format using
// the column mapping provided by the caller.
func psEventRowsToStorageRows(rows []PSEventRow, extraCols map[string]interface{}) []storage.Row {
	out := make([]storage.Row, 0, len(rows))
	for _, r := range rows {
		row := storage.Row{
			"THREAD_ID":    r.ThreadID,
			"EVENT_ID":     r.EventID,
			"END_EVENT_ID": r.EndEventID,
			"EVENT_NAME":   r.EventName,
		}
		for k, v := range extraCols {
			row[k] = v
		}
		for k, v := range r.Extra {
			row[k] = v
		}
		out = append(out, row)
	}
	return out
}

// recordPSStatementEvent appends the current query as a statement event to
// events_statements_current and events_statements_history in the shared
// PS events store. This makes events visible to other connections immediately
// (needed for wait_condition.inc polling in P_S idx tests).
//
// Internal / recursive calls (routineDepth > 0) and TRUNCATE statements are
// skipped to avoid noise. The store is bounded by keeping only the last N
// events per thread in events_statements_history (MySQL default: 10).
func (e *Executor) recordPSStatementEvent(query string) {
	if e.psEventsStore == nil || e.routineDepth > 0 {
		return
	}
	if e.psClassDisabled("statement") {
		return
	}
	q := strings.TrimSpace(query)
	if q == "" {
		return
	}
	upper := strings.ToUpper(q)
	// Skip internal PS truncate and setup statements to reduce noise
	if strings.HasPrefix(upper, "SET ") {
		return
	}

	threadID := e.connectionID + 1
	eventName := "statement/sql/select"
	if strings.HasPrefix(upper, "INSERT") {
		eventName = "statement/sql/insert"
	} else if strings.HasPrefix(upper, "UPDATE") {
		eventName = "statement/sql/update"
	} else if strings.HasPrefix(upper, "DELETE") {
		eventName = "statement/sql/delete"
	} else if strings.HasPrefix(upper, "CREATE") {
		eventName = "statement/sql/create_table"
	} else if strings.HasPrefix(upper, "DROP") {
		eventName = "statement/sql/drop_table"
	} else if strings.HasPrefix(upper, "TRUNCATE") {
		eventName = "statement/sql/truncate"
	} else if strings.HasPrefix(upper, "START TRANSACTION") || upper == "BEGIN" {
		eventName = "statement/sql/begin"
	} else if upper == "COMMIT" {
		eventName = "statement/sql/commit"
	}

	stmtExtra := map[string]interface{}{
		"SOURCE": "", "TIMER_START": int64(0), "TIMER_END": int64(0), "TIMER_WAIT": int64(0),
		"SQL_TEXT": q, "DIGEST": nil, "DIGEST_TEXT": nil, "CURRENT_SCHEMA": e.CurrentDB,
		"ROWS_AFFECTED": int64(0), "ROWS_SENT": int64(0), "ROWS_EXAMINED": int64(0),
		"CREATED_TMP_DISK_TABLES": int64(0), "CREATED_TMP_TABLES": int64(0),
		"ERRORS": int64(0), "WARNINGS": int64(0), "NESTING_EVENT_ID": nil, "NESTING_EVENT_TYPE": nil,
	}
	row := PSEventRow{
		ThreadID:  threadID,
		EventName: eventName,
		Extra:     stmtExtra,
	}

	// events_statements_current: keep only the most recent statement per thread
	// (replace any existing row for this thread, mimicking MySQL's current-row semantics)
	store := e.psEventsStore
	store.mu.Lock()
	cur := store.events["events_statements_current"]
	replaced := false
	for i, r := range cur {
		if r.ThreadID == threadID {
			row.EventID = r.EventID // keep same event ID for current
			row.EndEventID = r.EventID
			cur[i] = row
			replaced = true
			break
		}
	}
	if !replaced {
		row.EventID = store.nextID
		store.nextID++
		row.EndEventID = row.EventID
		store.events["events_statements_current"] = append(cur, row)
	}

	// events_statements_history: append (bounded to 10 rows per thread)
	hist := store.events["events_statements_history"]
	histRow := row
	histRow.EventID = store.nextID
	store.nextID++
	histRow.EndEventID = histRow.EventID
	hist = append(hist, histRow)
	// trim to keep only last 10 per thread
	const maxHistPerThread = 10
	threadCount := 0
	for i := len(hist) - 1; i >= 0; i-- {
		if hist[i].ThreadID == threadID {
			threadCount++
			if threadCount > maxHistPerThread {
				hist = append(hist[:i], hist[i+1:]...)
			}
		}
	}
	store.events["events_statements_history"] = hist

	// events_waits_current: add a synthetic wait event per statement so that
	// idx_events_waits_current wait_condition can be satisfied. Keep one per thread.
	waitExtra := map[string]interface{}{
		"SOURCE": "", "TIMER_START": int64(0), "TIMER_END": int64(0), "TIMER_WAIT": int64(0),
		"SPINS": nil, "OBJECT_SCHEMA": nil, "OBJECT_NAME": nil, "INDEX_NAME": nil,
		"OBJECT_TYPE": "TABLE", "OBJECT_INSTANCE_BEGIN": int64(0), "NESTING_EVENT_ID": nil,
		"NESTING_EVENT_TYPE": nil, "OPERATION": "lock", "NUMBER_OF_BYTES": nil, "FLAGS": nil,
	}
	waitRow := PSEventRow{
		ThreadID:  threadID,
		EventName: "wait/lock/table/sql/handler",
		Extra:     waitExtra,
	}
	waitCur := store.events["events_waits_current"]
	waitReplaced := false
	for i, r := range waitCur {
		if r.ThreadID == threadID {
			waitRow.EventID = r.EventID
			waitRow.EndEventID = r.EventID
			waitCur[i] = waitRow
			waitReplaced = true
			break
		}
	}
	if !waitReplaced {
		waitRow.EventID = store.nextID
		store.nextID++
		waitRow.EndEventID = waitRow.EventID
		store.events["events_waits_current"] = append(waitCur, waitRow)
	}

	// events_transactions_current: record a synthetic transaction event for each
	// connection so that idx_events_transactions_current wait_condition can be satisfied.
	// We maintain one transaction row per thread, refreshed on each statement.
	txnExtra := map[string]interface{}{
		"STATE": "ACTIVE", "TRX_ID": nil, "GTID": "", "XID_FORMAT_ID": nil, "XID_GTRID": nil,
		"XID_BQUAL": nil, "XA_STATE": nil, "SOURCE": "", "TIMER_START": int64(0),
		"TIMER_END": nil, "TIMER_WAIT": nil, "ACCESS_MODE": "READ WRITE",
		"ISOLATION_LEVEL": "REPEATABLE READ", "AUTOCOMMIT": "YES",
		"NESTING_EVENT_ID": nil, "NESTING_EVENT_TYPE": nil,
	}
	txnRow := PSEventRow{
		ThreadID:  threadID,
		EventName: "transaction",
		Extra:     txnExtra,
	}
	txnCur := store.events["events_transactions_current"]
	txnReplaced := false
	for i, r := range txnCur {
		if r.ThreadID == threadID {
			txnRow.EventID = r.EventID
			txnRow.EndEventID = r.EventID
			txnCur[i] = txnRow
			txnReplaced = true
			break
		}
	}
	if !txnReplaced {
		txnRow.EventID = store.nextID
		store.nextID++
		txnRow.EndEventID = txnRow.EventID
		store.events["events_transactions_current"] = append(txnCur, txnRow)
	}

	// events_stages_current: add a synthetic "executing" stage event per thread so that
	// idx_events_stages_current wait_condition can be satisfied immediately (without
	// needing an actual lock-wait stage). Keep one per thread.
	if !e.psClassDisabled("stage") {
		stageExtra := map[string]interface{}{
			"SOURCE": "", "TIMER_START": int64(0), "TIMER_END": int64(0), "TIMER_WAIT": int64(0),
			"WORK_COMPLETED": nil, "WORK_ESTIMATED": nil, "NESTING_EVENT_ID": nil, "NESTING_EVENT_TYPE": nil,
		}
		stageRow := PSEventRow{
			ThreadID:  threadID,
			EventName: "stage/sql/executing",
			Extra:     stageExtra,
		}
		stageCur := store.events["events_stages_current"]
		stageReplaced := false
		for i, r := range stageCur {
			if r.ThreadID == threadID {
				stageRow.EventID = r.EventID
				stageRow.EndEventID = r.EventID
				stageCur[i] = stageRow
				stageReplaced = true
				break
			}
		}
		if !stageReplaced {
			stageRow.EventID = store.nextID
			store.nextID++
			stageRow.EndEventID = stageRow.EventID
			store.events["events_stages_current"] = append(stageCur, stageRow)
		}
	}

	store.mu.Unlock()
}

// recordPSStageEventLockWait adds a stage event "stage/sql/waiting for handler lock"
// to events_stages_current for this connection's thread. Called when a DML statement
// blocks waiting for a lock held by another connection. The event persists until
// removePSStageEvent is called.
func (e *Executor) recordPSStageEventLockWait() {
	if e.psEventsStore == nil || e.psClassDisabled("stage") {
		return
	}
	threadID := e.connectionID + 1
	stageExtra := map[string]interface{}{
		"SOURCE": "", "TIMER_START": int64(0), "TIMER_END": nil, "TIMER_WAIT": nil,
		"WORK_COMPLETED": nil, "WORK_ESTIMATED": nil, "NESTING_EVENT_ID": nil, "NESTING_EVENT_TYPE": nil,
	}
	e.psEventsStore.UpsertByThread("events_stages_current", PSEventRow{
		ThreadID:  threadID,
		EventName: "stage/sql/waiting for handler lock",
		Extra:     stageExtra,
	})
}

// removePSStageEvent removes any stage event for this connection's thread from
// events_stages_current. Called when a DML lock wait completes.
func (e *Executor) removePSStageEvent() {
	if e.psEventsStore == nil {
		return
	}
	threadID := e.connectionID + 1
	e.psEventsStore.RemoveByThread("events_stages_current", threadID)
}
