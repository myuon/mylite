package executor

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// recordFKError stores the FK constraint message from err into the active
// transaction's metadata (trx_last_foreign_key_error in INNODB_TRX).
// The error message format is:
//
//	Cannot add or update a child row: a foreign key constraint fails (`db`.`tbl`, CONSTRAINT ...)
//
// or
//
//	Cannot delete or update a parent row: a foreign key constraint fails (`db`.`tbl`, CONSTRAINT ...)
//
// We extract and store only the content inside the outermost parentheses (after "constraint fails ").
func (e *Executor) recordFKError(err error) {
	if err == nil || e.txnActiveSet == nil || !e.inTransaction {
		return
	}
	msg := err.Error()
	// Find "constraint fails (" to locate the start of the constraint description.
	const marker = "constraint fails ("
	idx := strings.Index(strings.ToLower(msg), marker)
	if idx < 0 {
		return
	}
	start := idx + len(marker)
	part := msg[start:]
	// Remove the trailing ')' (the matching close of "constraint fails (...")
	if strings.HasSuffix(part, ")") {
		part = part[:len(part)-1]
	}
	e.txnActiveSet.SetFKError(e.connectionID, part)
}

// txSavepoint holds the catalog and storage state captured at BEGIN time.
type txSavepoint struct {
	// Storage snapshot per database name.
	storageSnap map[string]*storage.DatabaseSnapshot
	// Catalog snapshot: db name -> table name -> *catalog.TableDef (shallow copy is fine;
	// TableDef itself is not mutated after creation).
	catalogSnap map[string]map[string]*catalog.TableDef
}

// fullSnapshot holds a complete snapshot of all databases for MYLITE SNAPSHOT commands.
type fullSnapshot struct {
	storageSnap map[string]*storage.DatabaseSnapshot
	catalogSnap map[string]map[string]*catalog.TableDef
}

// undoEntry records a single DML mutation for transaction rollback.
type undoEntry struct {
	op       string // "INSERT", "DELETE", "UPDATE"
	db       string
	table    string
	rowIndex int         // row index at time of operation (for INSERT: index of inserted row)
	oldRow   storage.Row // for DELETE/UPDATE: the original row
}

func (e *Executor) captureSnapshot() *txSavepoint {
	sp := &txSavepoint{
		storageSnap: make(map[string]*storage.DatabaseSnapshot),
		catalogSnap: make(map[string]map[string]*catalog.TableDef),
	}
	// Snapshot all databases currently in the catalog.
	for dbName, db := range e.Catalog.Databases {
		sp.storageSnap[dbName] = e.Storage.SnapshotDatabase(dbName)
		tablesCopy := make(map[string]*catalog.TableDef, len(db.Tables))
		for tName, tDef := range db.Tables {
			tablesCopy[tName] = tDef
		}
		sp.catalogSnap[dbName] = tablesCopy
	}
	return sp
}

// ddlImplicitCommit simulates the implicit commit that MySQL performs before DDL statements.
// This clears named savepoints so that subsequent ROLLBACK TO SAVEPOINT fails.
func (e *Executor) ddlImplicitCommit() {
	if e.namedSavepoints != nil {
		e.namedSavepoints = make(map[string]bool)
	}
}

// waitForInstanceBackupLock blocks until no other connection holds LOCK INSTANCE FOR BACKUP,
// or until lock_wait_timeout expires. Sets the processlist state to "Waiting for backup lock"
// while blocked. Returns nil if safe to proceed, error on timeout.
// Connections that hold the backup lock themselves may still run DDL.
func (e *Executor) waitForInstanceBackupLock() error {
	if e.instanceBackupLock == nil {
		return nil
	}
	held, _ := e.instanceBackupLock.IsHeldByOther(e.connectionID)
	if !held {
		return nil
	}
	// Determine timeout from lock_wait_timeout session variable.
	timeoutSec := 31536000.0 // default: essentially infinite
	if e.sessionScopeVars != nil {
		if v, ok := e.sessionScopeVars["lock_wait_timeout"]; ok {
			if t, err := strconv.ParseFloat(v, 64); err == nil {
				timeoutSec = t
			}
		}
	}
	// Set process state while waiting.
	if e.processList != nil {
		e.processList.SetState(e.connectionID, "Waiting for backup lock")
	}
	err := e.instanceBackupLock.WaitUntilFreeForDDL(e.connectionID, timeoutSec)
	if e.processList != nil {
		e.processList.SetState(e.connectionID, "")
	}
	if err != nil {
		return mysqlError(1205, "HY000", "Lock wait timeout exceeded; try restarting transaction")
	}
	return nil
}

func (e *Executor) execBegin() (*Result, error) {
	if e.inTransaction {
		// Implicit commit of previous transaction before starting a new one.
		// Release row locks held by the previous transaction (MySQL behavior).
		if e.rowLockManager != nil {
			e.rowLockManager.ReleaseRowLocks(e.connectionID)
		}
		e.savepoint = nil
		e.txnUndoLog = nil
		e.txnHasWrites = false
		if e.txnActiveSet != nil {
			e.txnActiveSet.End(e.connectionID)
		}
	}
	e.savepoint = e.captureSnapshot()
	e.txnUndoLog = nil
	e.txnHasWrites = false
	e.namedSavepoints = make(map[string]bool)
	e.inTransaction = true
	if e.txnActiveSet != nil {
		iso, uq, fk := e.txnSessionMeta()
		e.txnActiveSet.Begin(e.connectionID, iso, uq, fk)
	}
	// nextTxnIsolationPrev stays set until the transaction ends (execCommit/execRollback),
	// at which point we restore sessionScopeVars["transaction_isolation"] to the saved
	// previous value. While the transaction is active, sessionScopeVars still holds the
	// NextTxScope value so all isolation-level reads (gap locks, SERIALIZABLE checks, etc.)
	// see the correct isolation level.
	return &Result{}, nil
}

// ensureImplicitTxnTracked registers the current connection in TxnActiveSet when
// autocommit=0 is active. MySQL treats autocommit=0 as starting an implicit
// transaction on the first DML, so we need it to show in INNODB_TRX.
// This is a no-op when already in an explicit transaction or when autocommit is ON.
func (e *Executor) ensureImplicitTxnTracked() {
	if e.txnActiveSet == nil {
		return
	}
	if e.inTransaction {
		// Already tracked via execBegin.
		return
	}
	// Check autocommit=0
	v, ok := e.getSysVar("autocommit")
	if !ok {
		return
	}
	upper := strings.ToUpper(v)
	if upper != "0" && upper != "OFF" {
		return
	}
	// autocommit=0: register in TxnActiveSet if not already there.
	e.txnActiveSet.mu.RLock()
	_, alreadyActive := e.txnActiveSet.active[e.connectionID]
	e.txnActiveSet.mu.RUnlock()
	if alreadyActive {
		return
	}
	iso, uq, fk := e.txnSessionMeta()
	e.txnActiveSet.Begin(e.connectionID, iso, uq, fk)
}

// endImplicitTxnTracked removes the connection from TxnActiveSet after an
// autocommit=0 implicit transaction ends (COMMIT/ROLLBACK when !inTransaction).
func (e *Executor) endImplicitTxnTracked() {
	if e.txnActiveSet == nil || e.inTransaction {
		return
	}
	e.txnActiveSet.End(e.connectionID)
}

// restoreNextTxnIsolation restores the session isolation level after a transaction
// that was started with a NextTxScope (SET TRANSACTION ISOLATION LEVEL) override.
// If nextTxnIsolationPrev is set, it means the current session's transaction_isolation
// was temporarily overridden for the last transaction. We restore it here.
func (e *Executor) restoreNextTxnIsolation() {
	if e.nextTxnIsolationPrev == "" {
		return
	}
	e.sessionScopeVars["transaction_isolation"] = e.nextTxnIsolationPrev
	e.nextTxnIsolationPrev = ""
}

// txnSessionMeta returns the current session's isolation level, unique_checks,
// and foreign_key_checks values for INNODB_TRX metadata.
func (e *Executor) txnSessionMeta() (isolationLevel string, uniqueChecks, foreignKeyChecks int64) {
	isolationLevel = "REPEATABLE-READ"
	uniqueChecks = 1
	foreignKeyChecks = 1
	if iso, ok := e.getSysVar("transaction_isolation"); ok {
		isolationLevel = iso
	} else if iso, ok := e.getSysVar("tx_isolation"); ok {
		isolationLevel = iso
	}
	if uq, ok := e.getSysVar("unique_checks"); ok {
		if uq == "0" || strings.EqualFold(uq, "OFF") {
			uniqueChecks = 0
		}
	}
	if fk, ok := e.getSysVar("foreign_key_checks"); ok {
		if fk == "0" || strings.EqualFold(fk, "OFF") {
			foreignKeyChecks = 0
		}
	}
	return
}

func (e *Executor) execCommit() (*Result, error) {
	// Always release row locks on COMMIT (covers autocommit=0 implicit transactions)
	if e.rowLockManager != nil {
		e.rowLockManager.ReleaseRowLocks(e.connectionID)
	}
	if !e.inTransaction {
		// End implicit autocommit=0 transaction tracking if present.
		e.endImplicitTxnTracked()
		e.restoreNextTxnIsolation()
		return &Result{}, nil
	}
	// Block COMMIT if another connection holds FLUSH TABLES WITH READ LOCK,
	// but only for write transactions (INSERT/UPDATE/DELETE). Read-only transactions
	// (BEGIN; SELECT; COMMIT) are never blocked since they have no dirty pages.
	if e.globalReadLock != nil && e.txnHasWrites {
		if e.processList != nil {
			e.processList.SetState(e.connectionID, "Waiting for commit lock")
		}
		if err := e.globalReadLock.WaitIfHeldByOther(e.connectionID, 31536000); err != nil {
			if e.processList != nil {
				e.processList.SetState(e.connectionID, "")
			}
			return nil, fmt.Errorf("Lock wait timeout exceeded; try restarting transaction")
		}
		if e.processList != nil {
			e.processList.SetState(e.connectionID, "")
		}
	}
	// Remove transaction tags from rows inserted by this connection
	e.clearTxnRowTags()
	e.inTransaction = false
	e.savepoint = nil
	e.txnUndoLog = nil
	e.txnHasWrites = false
	if e.txnActiveSet != nil {
		e.txnActiveSet.End(e.connectionID)
	}
	// Restore session isolation level if it was temporarily overridden by a NextTxScope set.
	e.restoreNextTxnIsolation()
	// Release GTID ownership acquired by SET SESSION gtid_next = 'ANONYMOUS'.
	delete(e.sessionScopeVars, "__owns_anonymous_gtid")
	return &Result{}, nil
}

// filterUncommittedRows removes rows that were inserted by other connections'
// uncommitted transactions (transaction isolation for reads).
func (e *Executor) filterUncommittedRows(rows []storage.Row) []storage.Row {
	if e.txnActiveSet == nil {
		return rows
	}
	e.txnActiveSet.mu.RLock()
	hasActive := len(e.txnActiveSet.active) > 0
	e.txnActiveSet.mu.RUnlock()
	if !hasActive {
		// No active transactions; strip any leftover tags and return all rows
		return rows
	}

	result := make([]storage.Row, 0, len(rows))
	for _, row := range rows {
		connIDVal, hasTxnTag := row["__txn_conn_id__"]
		if !hasTxnTag {
			// Row was not inserted in a transaction (committed data)
			result = append(result, row)
			continue
		}
		connID, ok := connIDVal.(int64)
		if !ok {
			result = append(result, row)
			continue
		}
		if connID == e.connectionID {
			// Row was inserted by this connection -- visible
			result = append(result, row)
			continue
		}
		// Row was inserted by another connection -- check if that connection
		// is still in an active transaction
		e.txnActiveSet.mu.RLock()
		otherActive := e.txnActiveSet.active[connID]
		e.txnActiveSet.mu.RUnlock()
		if otherActive {
			// Other connection's uncommitted row -- filter out
			continue
		}
		// Other connection already committed -- visible
		result = append(result, row)
	}
	return result
}

// clearTxnRowTags removes the __txn_conn_id__ metadata from all rows
// that were inserted by this connection during the transaction.
func (e *Executor) clearTxnRowTags() {
	if e.txnUndoLog == nil {
		return
	}
	// Collect unique db:table pairs from the undo log
	tables := make(map[string]bool)
	for _, entry := range e.txnUndoLog {
		if entry.op == "INSERT" {
			tables[entry.db+":"+entry.table] = true
		}
	}
	for key := range tables {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			continue
		}
		tbl, err := e.Storage.GetTable(parts[0], parts[1])
		if err != nil {
			continue
		}
		tbl.Lock()
		for _, row := range tbl.Rows {
			if connID, ok := row["__txn_conn_id__"]; ok {
				if cid, ok := connID.(int64); ok && cid == e.connectionID {
					delete(row, "__txn_conn_id__")
				}
			}
		}
		tbl.Unlock()
	}
}

func (e *Executor) execRollback() (*Result, error) {
	// Always release row locks on ROLLBACK (covers autocommit=0 implicit transactions)
	if e.rowLockManager != nil {
		e.rowLockManager.ReleaseRowLocks(e.connectionID)
	}
	if !e.inTransaction {
		// End implicit autocommit=0 transaction tracking if present.
		e.endImplicitTxnTracked()
		e.restoreNextTxnIsolation()
		// Release GTID ownership acquired by SET SESSION gtid_next = 'ANONYMOUS'.
		delete(e.sessionScopeVars, "__owns_anonymous_gtid")
		return &Result{}, nil
	}
	sp := e.savepoint
	undoLog := e.txnUndoLog
	e.inTransaction = false
	e.savepoint = nil
	e.txnUndoLog = nil
	e.txnHasWrites = false
	if e.txnActiveSet != nil {
		e.txnActiveSet.End(e.connectionID)
	}
	// Restore session isolation level if it was temporarily overridden by a NextTxScope set.
	e.restoreNextTxnIsolation()
	// Release GTID ownership acquired by SET SESSION gtid_next = 'ANONYMOUS'.
	delete(e.sessionScopeVars, "__owns_anonymous_gtid")

	// If we have an undo log, use it for precise per-connection rollback
	// instead of the snapshot-based approach which can clobber other connections' data.
	if len(undoLog) > 0 {
		// Check if any tables involved in the transaction are non-transactional (MyISAM/MEMORY).
		// If so, emit Warning 1196 and skip rollback for those tables.
		hasNonTransactional := false
		for _, entry := range undoLog {
			if db, ok := e.Catalog.Databases[entry.db]; ok {
				if tblDef, ok2 := db.Tables[entry.table]; ok2 {
					eng := strings.ToUpper(tblDef.Engine)
					if eng == "MYISAM" || eng == "MEMORY" || eng == "HEAP" {
						hasNonTransactional = true
						break
					}
				}
			}
		}
		if hasNonTransactional {
			// Non-transactional tables cannot be rolled back; emit warning and skip undo.
			e.addWarning("Warning", 1196, "Some non-transactional changed tables couldn't be rolled back")
			return &Result{}, nil
		}
		e.replayUndoLog(undoLog)
		return &Result{}, nil
	}

	if sp == nil {
		return &Result{}, nil
	}

	// Restore catalog: replace each database's table map with the snapshot.
	// First, remove databases that were created during the transaction.
	for dbName := range e.Catalog.Databases {
		if _, existed := sp.catalogSnap[dbName]; !existed {
			delete(e.Catalog.Databases, dbName)
			e.Storage.DropDatabase(dbName)
		}
	}
	// Restore tables in each snapshotted database.
	for dbName, tables := range sp.catalogSnap {
		db, ok := e.Catalog.Databases[dbName]
		if !ok {
			// Database was dropped during the transaction; recreate it.
			e.Catalog.Databases[dbName] = &catalog.Database{
				Name:   dbName,
				Tables: make(map[string]*catalog.TableDef),
			}
			db = e.Catalog.Databases[dbName]
		}
		// Replace the table map wholesale.
		db.Tables = tables
		// Restore storage.
		e.Storage.RestoreDatabase(dbName, sp.storageSnap[dbName])
	}

	return &Result{}, nil
}

// replayUndoLog undoes DML mutations in reverse order.
func (e *Executor) replayUndoLog(log []undoEntry) {
	for i := len(log) - 1; i >= 0; i-- {
		entry := log[i]
		tbl, err := e.Storage.GetTable(entry.db, entry.table)
		if err != nil {
			continue
		}
		tbl.Lock()
		switch entry.op {
		case "INSERT":
			// Remove the row that was inserted.
			// We need to find and remove the row by matching the old row data.
			if entry.oldRow != nil {
				newRows := make([]storage.Row, 0, len(tbl.Rows))
				removed := false
				for _, r := range tbl.Rows {
					if !removed && rowsEqualByMap(r, entry.oldRow) {
						removed = true
						continue
					}
					newRows = append(newRows, r)
				}
				if removed {
					tbl.Rows = newRows
					tbl.InvalidateIndexes()
				}
			}
		case "DELETE":
			// Re-insert the deleted row.
			if entry.oldRow != nil {
				// Insert at the original index if possible
				if entry.rowIndex >= 0 && entry.rowIndex <= len(tbl.Rows) {
					newRows := make([]storage.Row, 0, len(tbl.Rows)+1)
					newRows = append(newRows, tbl.Rows[:entry.rowIndex]...)
					newRows = append(newRows, entry.oldRow)
					newRows = append(newRows, tbl.Rows[entry.rowIndex:]...)
					tbl.Rows = newRows
				} else {
					tbl.Rows = append(tbl.Rows, entry.oldRow)
				}
				tbl.InvalidateIndexes()
			}
		case "UPDATE":
			// Restore the old row at the given index.
			if entry.oldRow != nil && entry.rowIndex >= 0 && entry.rowIndex < len(tbl.Rows) {
				tbl.Rows[entry.rowIndex] = entry.oldRow
				tbl.InvalidateIndexes()
			}
		}
		tbl.Unlock()
	}
}

// rowsEqualByMap checks if two storage rows have the same key-value pairs.
func rowsEqualByMap(a, b storage.Row) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		bv, ok := b[k]
		if !ok {
			return false
		}
		if fmt.Sprintf("%v", v) != fmt.Sprintf("%v", bv) {
			return false
		}
	}
	return true
}
