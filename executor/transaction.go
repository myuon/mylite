package executor

import (
	"fmt"
	"strings"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

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

func (e *Executor) execBegin() (*Result, error) {
	if e.inTransaction {
		// Implicit commit of previous transaction before starting a new one.
		e.savepoint = nil
		e.txnUndoLog = nil
		if e.txnActiveSet != nil {
			e.txnActiveSet.End(e.connectionID)
		}
	}
	e.savepoint = e.captureSnapshot()
	e.txnUndoLog = nil
	e.inTransaction = true
	if e.txnActiveSet != nil {
		e.txnActiveSet.Begin(e.connectionID)
	}
	return &Result{}, nil
}

func (e *Executor) execCommit() (*Result, error) {
	// Always release row locks on COMMIT (covers autocommit=0 implicit transactions)
	if e.rowLockManager != nil {
		e.rowLockManager.ReleaseRowLocks(e.connectionID)
	}
	if !e.inTransaction {
		return &Result{}, nil
	}
	// Remove transaction tags from rows inserted by this connection
	e.clearTxnRowTags()
	e.inTransaction = false
	e.savepoint = nil
	e.txnUndoLog = nil
	if e.txnActiveSet != nil {
		e.txnActiveSet.End(e.connectionID)
	}
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
		return &Result{}, nil
	}
	sp := e.savepoint
	undoLog := e.txnUndoLog
	e.inTransaction = false
	e.savepoint = nil
	e.txnUndoLog = nil
	if e.txnActiveSet != nil {
		e.txnActiveSet.End(e.connectionID)
	}

	// If we have an undo log, use it for precise per-connection rollback
	// instead of the snapshot-based approach which can clobber other connections' data.
	if len(undoLog) > 0 {
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
