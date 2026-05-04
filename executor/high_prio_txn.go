package executor

import (
	"strconv"
	"strings"
	"sync"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// HighPrioTxnManager manages state for high-priority transactions (MySQL DEBUG facility).
//
// MySQL's `SET GLOBAL DEBUG='+d,dbug_set_high_prio_trx'` marks the next START TRANSACTION
// as high-priority. High-priority transactions preempt lower-priority transactions that hold
// conflicting row locks by immediately rolling back the blocker instead of waiting.
//
// This is shared across all executor instances (connections).
type HighPrioTxnManager struct {
	mu sync.Mutex

	// debugFlags is the current set of active debug flags (e.g. "dbug_set_high_prio_trx").
	debugFlags map[string]bool

	// killedConns tracks connections whose transactions have been killed by a
	// high-priority transaction. When such a connection tries to COMMIT, it returns
	// ER_ERROR_DURING_COMMIT (MySQL error 1180, "Got error 149").
	killedConns map[int64]bool

	// executors stores references to active executors by connectionID,
	// enabling cross-connection rollback for high-priority preemption.
	executors map[int64]*Executor
}

// NewHighPrioTxnManager creates a new HighPrioTxnManager.
func NewHighPrioTxnManager() *HighPrioTxnManager {
	return &HighPrioTxnManager{
		debugFlags:  make(map[string]bool),
		killedConns: make(map[int64]bool),
		executors:   make(map[int64]*Executor),
	}
}

// SetDebugFlag adds or removes a debug flag.
// spec is in MySQL format: "+d,flag1,flag2" or "-d,flag1,flag2"
func (m *HighPrioTxnManager) SetDebugFlag(spec string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	spec = strings.TrimSpace(spec)
	if len(spec) < 3 {
		return
	}

	op := spec[0] // '+' or '-'
	if spec[1] != 'd' && spec[1] != 'D' {
		// Not a 'd' (debug) type flag, ignore
		return
	}
	if len(spec) < 3 || spec[2] != ',' {
		return
	}
	flags := strings.Split(spec[3:], ",")
	for _, flag := range flags {
		flag = strings.TrimSpace(flag)
		if flag == "" {
			continue
		}
		if op == '+' {
			m.debugFlags[flag] = true
		} else if op == '-' {
			delete(m.debugFlags, flag)
		}
	}
}

// IsDebugFlagSet returns true if the given debug flag is currently active.
func (m *HighPrioTxnManager) IsDebugFlagSet(flag string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.debugFlags[flag]
}

// RegisterExecutor registers an executor so it can be accessed by other connections
// for high-priority preemption. Call at transaction start.
func (m *HighPrioTxnManager) RegisterExecutor(connID int64, e *Executor) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.executors[connID] = e
}

// UnregisterExecutor removes the executor registration. Call at disconnect or transaction end.
func (m *HighPrioTxnManager) UnregisterExecutor(connID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.executors, connID)
}

// MarkKilled marks a connection's transaction as killed by a high-priority transaction.
func (m *HighPrioTxnManager) MarkKilled(connID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.killedConns[connID] = true
}

// ClearKilled removes the killed status for a connection (after rollback).
func (m *HighPrioTxnManager) ClearKilled(connID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.killedConns, connID)
}

// IsKilled returns true if the connection's transaction has been killed.
func (m *HighPrioTxnManager) IsKilled(connID int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.killedConns[connID]
}

// PreemptConnection forces a rollback of the given connection's transaction
// and releases its row locks. Returns true if preemption was performed.
// Called by high-priority transactions when they need a lock held by connID.
func (m *HighPrioTxnManager) PreemptConnection(connID int64, rlm *RowLockManager) bool {
	m.mu.Lock()
	e, ok := m.executors[connID]
	m.mu.Unlock()
	if !ok || e == nil {
		return false
	}

	// Mark the connection as killed so its COMMIT returns ER_ERROR_DURING_COMMIT
	m.MarkKilled(connID)

	// Perform undo of the connection's transaction
	e.replayUndoLogExternal()

	// Release all row locks held by the killed connection
	if rlm != nil {
		rlm.ReleaseRowLocks(connID)
	}

	return true
}

// PreemptTableConflicts finds all connections holding row locks on the given table
// and preempts them. This is called before matching rows in high-priority DML.
func (m *HighPrioTxnManager) PreemptTableConflicts(dbName, tableName string, selfConnID int64, rlm *RowLockManager) {
	if rlm == nil {
		return
	}
	prefix := dbName + ":" + tableName + ":"
	blockers := rlm.GetBlockersByTablePrefix(selfConnID, prefix)
	for _, blockerID := range blockers {
		m.PreemptConnection(blockerID, rlm)
	}
}

// acquireRowLocksForRowsHighPrio acquires row-level locks for a high-priority transaction.
// If any row is locked by another connection, that connection is preempted first.
// This is the high-priority version of acquireRowLocksForRows (defined in select.go).
func (e *Executor) acquireRowLocksForRowsHighPrio(dbName, tableName string, def *catalog.TableDef, rows []storage.Row, indices []int) error {
	timeout := 50.0
	if v, ok := e.getSysVar("innodb_lock_wait_timeout"); ok {
		if t, err := strconv.ParseFloat(v, 64); err == nil {
			timeout = t
		}
	}

	pkCols := def.PrimaryKey

	for _, idx := range indices {
		if idx < 0 || idx >= len(rows) {
			continue
		}
		var lockKey string
		if len(pkCols) > 0 {
			lockKey = buildRowLockKey(dbName, tableName, pkCols, rows[idx])
		} else {
			lockKey = buildRowLockKeyByIndex(dbName, tableName, idx)
		}
		var err error
		if e.isHighPriority && e.highPrioTxn != nil {
			err = e.rowLockManager.AcquireRowLockHighPrio(e.connectionID, lockKey, timeout, e.highPrioTxn)
		} else {
			err = e.rowLockManager.AcquireRowLock(e.connectionID, lockKey, timeout)
		}
		if err != nil {
			return mysqlError(1205, "HY000", "Lock wait timeout exceeded; try restarting transaction")
		}
	}
	return nil
}

