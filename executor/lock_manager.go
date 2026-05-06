package executor

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// LockManager manages MySQL user-level locks (GET_LOCK/RELEASE_LOCK).
// Lock names are case-insensitive (stored as lowercase) and support re-entrant
// acquisition: the same connection can acquire the same name multiple times,
// requiring an equal number of RELEASE_LOCK calls to fully release it.
type LockManager struct {
	mu    sync.Mutex
	locks map[string]*userLock // lock name (lowercase) -> lock info
}

type userLock struct {
	ownerID int64        // connection ID that owns the lock
	count   int          // re-entrant acquisition count (>= 1 when held)
	ch      chan struct{} // closed when the lock is fully released
}

// NewLockManager creates a new LockManager.
func NewLockManager() *LockManager {
	return &LockManager{
		locks: make(map[string]*userLock),
	}
}

// normalizeLockName returns the case-folded (lowercase) lock name for storage/lookup.
func normalizeLockName(name string) string {
	return strings.ToLower(name)
}

// GetLock tries to acquire a named lock with a timeout (in seconds).
// Returns 1 if acquired, 0 if timed out.
// The connID identifies the connection requesting the lock.
// Lock names are case-insensitive. The same connection may acquire the same lock
// multiple times (re-entrant); each acquisition increments the internal counter.
// If setStateFn is non-nil, it is called with "User lock" before blocking
// and with "" after acquiring.
func (lm *LockManager) GetLock(name string, timeout float64, connID int64, setStateFn func(string)) int64 {
	key := normalizeLockName(name)
	lm.mu.Lock()

	existing, exists := lm.locks[key]
	if exists && existing.ownerID == connID {
		// Re-entrant: increment count
		existing.count++
		lm.mu.Unlock()
		return 1
	}

	if !exists {
		// Lock is free, acquire it
		lm.locks[key] = &userLock{
			ownerID: connID,
			count:   1,
			ch:      make(chan struct{}),
		}
		lm.mu.Unlock()
		return 1
	}

	// Lock is held by another connection - we need to wait
	waitCh := existing.ch
	lm.mu.Unlock()

	// Set state to "User lock" while waiting
	if setStateFn != nil {
		setStateFn("User lock")
	}

	if timeout <= 0 {
		// No wait
		if setStateFn != nil {
			setStateFn("")
		}
		return 0
	}

	timer := time.NewTimer(time.Duration(timeout * float64(time.Second)))
	defer timer.Stop()

	select {
	case <-waitCh:
		// Lock was released, try to acquire
		if setStateFn != nil {
			setStateFn("")
		}
		lm.mu.Lock()
		// Check again - someone else might have grabbed it
		if _, stillExists := lm.locks[key]; !stillExists {
			lm.locks[key] = &userLock{
				ownerID: connID,
				count:   1,
				ch:      make(chan struct{}),
			}
			lm.mu.Unlock()
			return 1
		}
		lm.mu.Unlock()
		return 0
	case <-timer.C:
		if setStateFn != nil {
			setStateFn("")
		}
		return 0
	}
}

// ReleaseLock releases one acquisition of a named lock.
// Returns 1 if released (count decremented or fully removed), 0 if not owned by
// this connection, nil if lock does not exist.
// For re-entrant locks, the lock is only fully released after count reaches zero.
func (lm *LockManager) ReleaseLock(name string, connID int64) interface{} {
	key := normalizeLockName(name)
	lm.mu.Lock()
	defer lm.mu.Unlock()

	existing, exists := lm.locks[key]
	if !exists {
		return nil
	}
	if existing.ownerID != connID {
		return int64(0)
	}

	existing.count--
	if existing.count <= 0 {
		close(existing.ch)
		delete(lm.locks, key)
	}
	return int64(1)
}

// ReleaseAllLocks releases all locks held by a connection, counting each
// re-entrant acquisition separately. Returns total count of acquisitions released.
func (lm *LockManager) ReleaseAllLocks(connID int64) int64 {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	var count int64
	for name, lock := range lm.locks {
		if lock.ownerID == connID {
			count += int64(lock.count)
			close(lock.ch)
			delete(lm.locks, name)
		}
	}
	return count
}

// IsFreeLock checks if a lock name is free. Returns 1 if free, 0 if in use.
// Lock name comparison is case-insensitive.
func (lm *LockManager) IsFreeLock(name string) int64 {
	key := normalizeLockName(name)
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if _, exists := lm.locks[key]; exists {
		return 0
	}
	return 1
}

// IsUsedLock checks if a lock is in use. Returns the connection ID of the owner,
// or nil if the lock is not held. Lock name comparison is case-insensitive.
func (lm *LockManager) IsUsedLock(name string) interface{} {
	key := normalizeLockName(name)
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lock, exists := lm.locks[key]; exists {
		return lock.ownerID
	}
	return nil
}

// RowLockManager manages InnoDB-style row-level locks for SELECT ... FOR UPDATE / FOR SHARE.
// Row locks are keyed by "db:table:pkValue" and held until COMMIT/ROLLBACK.
// Supports both shared (S) and exclusive (X) lock modes:
//   - Multiple shared locks from different connections are compatible.
//   - An exclusive lock is incompatible with any other lock from a different connection.
type RowLockManager struct {
	mu                   sync.Mutex
	locks                map[string]*rowLockEntry // lockKey -> entry
	waitingFor           map[int64]map[int64]bool // waiter connID -> set of blocker connIDs
	waitStarted          map[int64]time.Time      // waiter connID -> time when wait began
	deadlockDetectEnabled atomic.Int32             // 1 = enabled (default), 0 = disabled
}

// errDeadlock is returned when a deadlock cycle is detected.
var errDeadlock = fmt.Errorf("Deadlock found when trying to get lock; try restarting transaction")

type rowLockEntry struct {
	exclusive bool           // true = X lock, false = S lock
	owners    map[int64]bool // connectionIDs that hold the lock
	ch        chan struct{}   // closed when the lock state changes (released or downgraded)
}

// NewRowLockManager creates a new RowLockManager.
func NewRowLockManager() *RowLockManager {
	rlm := &RowLockManager{
		locks:       make(map[string]*rowLockEntry),
		waitingFor:  make(map[int64]map[int64]bool),
		waitStarted: make(map[int64]time.Time),
	}
	rlm.deadlockDetectEnabled.Store(1) // enabled by default
	return rlm
}

// IsWaiting reports whether connID is currently waiting for a row lock.
// If waiting, it also returns the time at which the wait began.
func (rlm *RowLockManager) IsWaiting(connID int64) (bool, time.Time) {
	rlm.mu.Lock()
	defer rlm.mu.Unlock()
	if t, ok := rlm.waitStarted[connID]; ok {
		return true, t
	}
	return false, time.Time{}
}

// WaitingConnIDsSnapshot returns a snapshot of all connection IDs that are
// currently waiting for a row lock, along with the time each wait started.
func (rlm *RowLockManager) WaitingConnIDsSnapshot() map[int64]time.Time {
	rlm.mu.Lock()
	defer rlm.mu.Unlock()
	if len(rlm.waitStarted) == 0 {
		return nil
	}
	out := make(map[int64]time.Time, len(rlm.waitStarted))
	for id, t := range rlm.waitStarted {
		out[id] = t
	}
	return out
}

// SetDeadlockDetect enables or disables deadlock detection (innodb_deadlock_detect).
func (rlm *RowLockManager) SetDeadlockDetect(enabled bool) {
	if enabled {
		rlm.deadlockDetectEnabled.Store(1)
	} else {
		rlm.deadlockDetectEnabled.Store(0)
	}
}

// AcquireRowLock tries to acquire an exclusive lock on a row identified by key.
// If the lock is held by another connection, it blocks until the lock is released
// or the timeout expires. Returns nil on success, error on timeout.
func (rlm *RowLockManager) AcquireRowLock(connID int64, key string, timeoutSec float64) error {
	return rlm.acquireRowLockInner(connID, key, true, timeoutSec)
}

// AcquireSharedRowLock tries to acquire a shared lock on a row identified by key.
// Shared locks are compatible with other shared locks but block on exclusive locks.
func (rlm *RowLockManager) AcquireSharedRowLock(connID int64, key string, timeoutSec float64) error {
	return rlm.acquireRowLockInner(connID, key, false, timeoutSec)
}

func (rlm *RowLockManager) acquireRowLockInner(connID int64, key string, exclusive bool, timeoutSec float64) error {
	deadline := time.Now().Add(time.Duration(timeoutSec * float64(time.Second)))

	for {
		rlm.mu.Lock()
		existing, exists := rlm.locks[key]

		if !exists {
			// Lock is free, acquire it
			rlm.locks[key] = &rowLockEntry{
				exclusive: exclusive,
				owners:    map[int64]bool{connID: true},
				ch:        make(chan struct{}),
			}
			delete(rlm.waitingFor, connID)
			delete(rlm.waitStarted, connID)
			rlm.mu.Unlock()
			return nil
		}

		// Check if we already own it
		if existing.owners[connID] {
			// Upgrade shared -> exclusive if needed
			if exclusive && !existing.exclusive {
				if len(existing.owners) == 1 {
					// Only we hold it, upgrade
					existing.exclusive = true
				}
				// If others hold shared locks too, we need to wait
				// (fall through to wait below only if len > 1)
				if len(existing.owners) == 1 {
					rlm.mu.Unlock()
					return nil
				}
			} else {
				// Already own compatible lock (re-entrant)
				rlm.mu.Unlock()
				return nil
			}
		}

		// Check compatibility
		if !exclusive && !existing.exclusive {
			// Shared + shared = compatible, add ourselves
			existing.owners[connID] = true
			delete(rlm.waitingFor, connID)
			delete(rlm.waitStarted, connID)
			rlm.mu.Unlock()
			return nil
		}

		// Record wait-for relationship and check for deadlock
		blockers := make(map[int64]bool)
		for ownerID := range existing.owners {
			if ownerID != connID {
				blockers[ownerID] = true
			}
		}
		if len(blockers) > 0 {
			rlm.waitingFor[connID] = blockers
			// Record when the wait started (only on the first iteration).
			if _, alreadyWaiting := rlm.waitStarted[connID]; !alreadyWaiting {
				rlm.waitStarted[connID] = time.Now()
			}
			if rlm.deadlockDetectEnabled.Load() != 0 && rlm.detectDeadlock(connID) {
				delete(rlm.waitingFor, connID)
				delete(rlm.waitStarted, connID)
				rlm.mu.Unlock()
				return errDeadlock
			}
		}

		// Incompatible: wait for lock state change
		waitCh := existing.ch
		rlm.mu.Unlock()

		remaining := time.Until(deadline)
		if remaining <= 0 {
			select {
			case <-waitCh:
				continue
			default:
				rlm.mu.Lock()
				delete(rlm.waitingFor, connID)
				delete(rlm.waitStarted, connID)
				rlm.mu.Unlock()
				return errLockWaitTimeout
			}
		}

		timer := time.NewTimer(remaining)
		select {
		case <-waitCh:
			timer.Stop()
			continue
		case <-timer.C:
			select {
			case <-waitCh:
				continue
			default:
				rlm.mu.Lock()
				delete(rlm.waitingFor, connID)
				delete(rlm.waitStarted, connID)
				rlm.mu.Unlock()
				return errLockWaitTimeout
			}
		}
	}
}

// detectDeadlock checks if connID is involved in a deadlock cycle using DFS.
// Must be called with rlm.mu held.
func (rlm *RowLockManager) detectDeadlock(startConn int64) bool {
	visited := make(map[int64]bool)
	return rlm.dfsDeadlock(startConn, startConn, visited)
}

// dfsDeadlock performs a DFS traversal of the wait-for graph.
// Returns true if a cycle back to target is found.
func (rlm *RowLockManager) dfsDeadlock(current, target int64, visited map[int64]bool) bool {
	if visited[current] {
		return false
	}
	visited[current] = true
	for blocker := range rlm.waitingFor[current] {
		if blocker == target {
			return true
		}
		if rlm.dfsDeadlock(blocker, target, visited) {
			return true
		}
	}
	return false
}

// TryAcquireRowLock attempts to acquire an exclusive row lock without blocking.
// Returns (true, nil) if the lock was acquired, (false, nil) if the row is
// locked by another connection (used for SKIP LOCKED), or (false, error) on error.
func (rlm *RowLockManager) TryAcquireRowLock(connID int64, key string, exclusive bool) (acquired bool, err error) {
	rlm.mu.Lock()
	defer rlm.mu.Unlock()

	existing, exists := rlm.locks[key]
	if !exists {
		// Lock is free, acquire it
		rlm.locks[key] = &rowLockEntry{
			exclusive: exclusive,
			owners:    map[int64]bool{connID: true},
			ch:        make(chan struct{}),
		}
		return true, nil
	}

	// Already own it?
	if existing.owners[connID] {
		if exclusive && !existing.exclusive && len(existing.owners) > 1 {
			// Can't upgrade with other shared holders
			return false, nil
		}
		if exclusive && !existing.exclusive && len(existing.owners) == 1 {
			existing.exclusive = true
		}
		return true, nil
	}

	// Shared + shared = compatible
	if !exclusive && !existing.exclusive {
		existing.owners[connID] = true
		return true, nil
	}

	// Incompatible
	return false, nil
}

// ReleaseRowLocks releases all row locks held by a connection.
func (rlm *RowLockManager) ReleaseRowLocks(connID int64) {
	rlm.mu.Lock()
	defer rlm.mu.Unlock()

	delete(rlm.waitingFor, connID)
	delete(rlm.waitStarted, connID)

	for key, entry := range rlm.locks {
		if entry.owners[connID] {
			delete(entry.owners, connID)
			if len(entry.owners) == 0 {
				close(entry.ch)
				delete(rlm.locks, key)
			} else {
				// Signal waiters that state changed (e.g. one shared holder left)
				close(entry.ch)
				entry.ch = make(chan struct{})
			}
		}
	}
}

// HasOtherLocks checks if any other connection holds a lock on the exact key.
// Used to detect potential blocking before calling AcquireRowLock.
func (rlm *RowLockManager) HasOtherLocks(connID int64, key string) bool {
	rlm.mu.Lock()
	defer rlm.mu.Unlock()
	entry, exists := rlm.locks[key]
	if !exists {
		return false
	}
	for ownerID := range entry.owners {
		if ownerID != connID {
			return true
		}
	}
	return false
}

// HasOtherLocksWithPrefix checks if any other connection holds locks with keys
// matching the given prefix. Used for gap lock simulation.
func (rlm *RowLockManager) HasOtherLocksWithPrefix(connID int64, prefix string) bool {
	rlm.mu.Lock()
	defer rlm.mu.Unlock()

	for key, entry := range rlm.locks {
		if strings.HasPrefix(key, prefix) {
			for ownerID := range entry.owners {
				if ownerID != connID {
					return true
				}
			}
		}
	}
	return false
}

// GetOtherLockedKeysWithPrefix returns all lock keys held by other connections
// that match the given prefix.
func (rlm *RowLockManager) GetOtherLockedKeysWithPrefix(connID int64, prefix string) []string {
	rlm.mu.Lock()
	defer rlm.mu.Unlock()

	var keys []string
	for key, entry := range rlm.locks {
		if strings.HasPrefix(key, prefix) {
			for ownerID := range entry.owners {
				if ownerID != connID {
					keys = append(keys, key)
					break
				}
			}
		}
	}
	return keys
}

// GetRowLockStats returns the number of row locks and distinct tables locked
// by the given connection. Lock keys have the form "db:table:pkValue".
func (rlm *RowLockManager) GetRowLockStats(connID int64) (rowsLocked int64, tablesLocked int64) {
	rlm.mu.Lock()
	defer rlm.mu.Unlock()

	tables := make(map[string]bool)
	for key, entry := range rlm.locks {
		if !entry.owners[connID] {
			continue
		}
		rowsLocked++
		// Extract "db:table" prefix (first two colon-separated segments).
		first := strings.Index(key, ":")
		if first < 0 {
			continue
		}
		second := strings.Index(key[first+1:], ":")
		var tablePrefix string
		if second < 0 {
			tablePrefix = key
		} else {
			tablePrefix = key[:first+1+second]
		}
		tables[tablePrefix] = true
	}
	tablesLocked = int64(len(tables))
	return
}

// errLockWaitTimeout is a sentinel used internally; the executor wraps it with
// the proper MySQL error code when returning to the client.
var errLockWaitTimeout = fmt.Errorf("lock_wait_timeout")

// InstanceBackupLock implements LOCK INSTANCE FOR BACKUP / UNLOCK INSTANCE.
//
// MySQL semantics:
//   - Multiple connections can hold the backup lock simultaneously (shared holders).
//   - DDL statements must wait until all backup lock holders have released the lock.
//   - The DDL waiter's processlist state is set to "Waiting for backup lock".
//   - Connections that hold the backup lock can still run DDL (the lock only blocks
//     DDL from *other* connections that don't hold the lock, i.e. DDL tries exclusive).
//
// Implementation: holders is a set of connIDs. When a DDL from a non-holder wants to
// proceed, it must wait for holders to become empty. When the set transitions from
// non-empty to empty, ch is closed and re-created so waiters are notified.
type InstanceBackupLock struct {
	mu      sync.Mutex
	holders map[int64]bool // connIDs currently holding the backup lock
	ch      chan struct{}   // closed when holders becomes empty; recreated on next acquire
}

// NewInstanceBackupLock creates a new InstanceBackupLock.
func NewInstanceBackupLock() *InstanceBackupLock {
	return &InstanceBackupLock{
		holders: make(map[int64]bool),
		ch:      make(chan struct{}),
	}
}

// Acquire adds connID to the backup lock holder set.
// Multiple connections may hold the lock concurrently.
func (ibl *InstanceBackupLock) Acquire(connID int64) {
	ibl.mu.Lock()
	defer ibl.mu.Unlock()
	if ibl.ch == nil {
		ibl.ch = make(chan struct{})
	}
	ibl.holders[connID] = true
}

// Release removes connID from the holder set.
// If no holders remain, waiting DDL statements are unblocked.
func (ibl *InstanceBackupLock) Release(connID int64) {
	ibl.mu.Lock()
	defer ibl.mu.Unlock()
	delete(ibl.holders, connID)
	if len(ibl.holders) == 0 && ibl.ch != nil {
		close(ibl.ch)
		ibl.ch = nil
	}
}

// IsHeldByOther returns true if any connection OTHER than connID holds the backup lock,
// along with the current wait channel. When the wait channel is closed, the caller
// should re-check IsHeldByOther to see if the lock has truly been released.
func (ibl *InstanceBackupLock) IsHeldByOther(connID int64) (bool, chan struct{}) {
	ibl.mu.Lock()
	defer ibl.mu.Unlock()
	for id := range ibl.holders {
		if id != connID {
			return true, ibl.ch
		}
	}
	return false, nil
}

// WaitUntilFreeForDDL blocks until no other connection holds the backup lock,
// or until the timeout expires. Returns nil if safe to proceed, error on timeout.
// While waiting, the caller is responsible for updating processlist state.
func (ibl *InstanceBackupLock) WaitUntilFreeForDDL(connID int64, timeoutSec float64) error {
	deadline := time.Now().Add(time.Duration(timeoutSec * float64(time.Second)))

	for {
		held, waitCh := ibl.IsHeldByOther(connID)
		if !held {
			return nil
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			select {
			case <-waitCh:
				continue
			default:
				return errLockWaitTimeout
			}
		}

		timer := time.NewTimer(remaining)
		select {
		case <-waitCh:
			timer.Stop()
			// Holders may have changed; re-check
			continue
		case <-timer.C:
			select {
			case <-waitCh:
				continue
			default:
				return errLockWaitTimeout
			}
		}
	}
}

// TableLockManager manages LOCK TABLE READ/WRITE per connection.
// When a connection holds LOCK TABLE, only those tables are accessible and
// the lock mode (READ vs WRITE) restricts operations.
type TableLockManager struct {
	mu sync.Mutex
	// locks maps connID -> table (lowercase "db.table") -> lock mode ("READ" or "WRITE")
	locks map[int64]map[string]string
}

// NewTableLockManager creates a new TableLockManager.
func NewTableLockManager() *TableLockManager {
	return &TableLockManager{
		locks: make(map[int64]map[string]string),
	}
}

// LockTable records a table lock for the given connection.
func (tlm *TableLockManager) LockTable(connID int64, dbTable string, mode string) {
	tlm.mu.Lock()
	defer tlm.mu.Unlock()
	if tlm.locks[connID] == nil {
		tlm.locks[connID] = make(map[string]string)
	}
	tlm.locks[connID][strings.ToLower(dbTable)] = strings.ToUpper(mode)
}

// UnlockAll releases all table locks for a connection.
func (tlm *TableLockManager) UnlockAll(connID int64) {
	tlm.mu.Lock()
	defer tlm.mu.Unlock()
	delete(tlm.locks, connID)
}

// UnlockTable releases the lock for a specific table for a connection.
// If no locks remain for the connection after this, the connection entry is removed.
func (tlm *TableLockManager) UnlockTable(connID int64, dbTable string) {
	tlm.mu.Lock()
	defer tlm.mu.Unlock()
	m, ok := tlm.locks[connID]
	if !ok {
		return
	}
	delete(m, strings.ToLower(dbTable))
	if len(m) == 0 {
		delete(tlm.locks, connID)
	}
}

// HasLocks returns true if the connection currently holds any LOCK TABLE locks.
func (tlm *TableLockManager) HasLocks(connID int64) bool {
	tlm.mu.Lock()
	defer tlm.mu.Unlock()
	return len(tlm.locks[connID]) > 0
}

// GetLockMode returns the lock mode for a table, or "" if no lock is held.
func (tlm *TableLockManager) GetLockMode(connID int64, dbTable string) string {
	tlm.mu.Lock()
	defer tlm.mu.Unlock()
	if m, ok := tlm.locks[connID]; ok {
		return m[strings.ToLower(dbTable)]
	}
	return ""
}

// IsLocked checks if a table is locked by the given connection and returns
// whether the table is accessible and the lock mode.
func (tlm *TableLockManager) IsLocked(connID int64, dbTable string) (locked bool, mode string) {
	tlm.mu.Lock()
	defer tlm.mu.Unlock()
	m, hasLocks := tlm.locks[connID]
	if !hasLocks {
		// No active LOCK TABLE session - all tables are accessible
		return false, ""
	}
	mode, ok := m[strings.ToLower(dbTable)]
	if !ok {
		return false, ""
	}
	return true, mode
}

// GetLocks returns a copy of all locks held by the given connection.
// The returned map keys are lowercase "db.table" strings, values are lock modes.
func (tlm *TableLockManager) GetLocks(connID int64) map[string]string {
	tlm.mu.Lock()
	defer tlm.mu.Unlock()
	m, ok := tlm.locks[connID]
	if !ok {
		return nil
	}
	result := make(map[string]string, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

// RenameTableLock transfers the lock for oldKey to newKey for the given connection.
// This implements MySQL's behavior where RENAME TABLE under LOCK TABLES transfers
// the table lock to the new name.
func (tlm *TableLockManager) RenameTableLock(connID int64, oldDBTable, newDBTable string) {
	tlm.mu.Lock()
	defer tlm.mu.Unlock()
	m, ok := tlm.locks[connID]
	if !ok {
		return
	}
	oldKey := strings.ToLower(oldDBTable)
	mode, exists := m[oldKey]
	if !exists {
		return
	}
	delete(m, oldKey)
	m[strings.ToLower(newDBTable)] = mode
}

// IsLockedByOther checks if a table is locked by any connection OTHER than connID.
// Returns (true, mode) if locked by another connection, where mode is the strongest
// lock mode held (WRITE > READ). Returns (false, "") if not locked by others.
func (tlm *TableLockManager) IsLockedByOther(connID int64, dbTable string) (locked bool, mode string) {
	tlm.mu.Lock()
	defer tlm.mu.Unlock()
	key := strings.ToLower(dbTable)
	for otherID, m := range tlm.locks {
		if otherID == connID {
			continue
		}
		if lockMode, ok := m[key]; ok {
			if lockMode == "WRITE" {
				return true, "WRITE"
			}
			locked = true
			mode = lockMode
		}
	}
	return locked, mode
}

// GlobalReadLock implements FLUSH TABLES WITH READ LOCK (FTWRL).
//
// MySQL FTWRL semantics:
//   - Multiple connections can each independently hold FTWRL simultaneously.
//   - Each connection goes through two phases to acquire:
//     Phase 1: Add this connection to the holder set (no blocking needed —
//              multiple holders are allowed).
//     Phase 2: Wait for all other connections' active queries to complete,
//              so that no query is running concurrently with the lock.
//   - Once a connection has fully acquired FTWRL (phase 2 complete), it is
//     "ready". COMMITs from connections that are NOT in the holder set and were
//     NOT part of an active query during phase 2 are blocked until holders release.
//   - UNLOCK TABLES on the connection removes it from the holder set. When the
//     last holder releases, the commit-blocked connections are unblocked.
//
// Important: connections that had active queries during phase 2 (and thus caused
// the wait) are exempted from COMMIT blocking. This matches MySQL's behavior where
// transactions that were in-progress before FTWRL became ready can still commit.
//
// Implementation note:
//   - holders: connIDs that have fully acquired FTWRL (ready set).
//   - pendingHolders: connIDs in phase 2 (not yet ready; COMMITs not yet blocked).
//   - exemptFromBlock: connIDs whose active queries we waited on during phase 2.
//     These connections are exempt from the COMMIT block.
//   - ch: closed when the last holder releases; recreated on the next acquire.
type GlobalReadLock struct {
	mu             sync.Mutex
	holders        map[int64]bool // connections with fully-acquired FTWRL (ready)
	pendingHolders map[int64]bool // connections in phase 2
	exemptFromBlock map[int64]bool // connections exempted from COMMIT block
	ch             chan struct{}   // closed when holders becomes empty; nil if no holders
}

// NewGlobalReadLock creates a new GlobalReadLock.
func NewGlobalReadLock() *GlobalReadLock {
	return &GlobalReadLock{
		holders:         make(map[int64]bool),
		pendingHolders:  make(map[int64]bool),
		exemptFromBlock: make(map[int64]bool),
	}
}

// Acquire adds connID to FTWRL holders.
// Phase 1: register as pending (doesn't block other connections from acquiring).
// Phase 2: wait for all other connections' active queries to complete.
// Returns nil on success, error on timeout.
func (grl *GlobalReadLock) Acquire(connID int64, timeoutSec float64, pl *ProcessList) error {
	deadline := time.Now().Add(time.Duration(timeoutSec * float64(time.Second)))

	// Phase 1: Register as a pending holder (does not block other connections).
	grl.mu.Lock()
	if grl.holders[connID] || grl.pendingHolders[connID] {
		// Already hold it (re-entrant)
		grl.mu.Unlock()
		return nil
	}
	if grl.ch == nil {
		grl.ch = make(chan struct{})
	}
	grl.pendingHolders[connID] = true
	grl.mu.Unlock()

	// Phase 2: Wait for all other connections' active queries to complete.
	// During this phase, COMMITs are not yet blocked by this connection.
	// Track which connections we had to wait on (they are exempt from COMMIT blocking).
	if pl != nil {
		for {
			grl.mu.Lock()
			// Collect all holder connIDs (both ready and pending) that belong to this acquisition.
			holderSet := make(map[int64]bool)
			for id := range grl.holders {
				holderSet[id] = true
			}
			for id := range grl.pendingHolders {
				holderSet[id] = true
			}
			grl.mu.Unlock()

			entries := pl.Snapshot()
			hasActiveQuery := false
			for _, entry := range entries {
				// Ignore queries from connections that also hold FTWRL (they're allowed).
				if holderSet[entry.ID] {
					continue
				}
				if entry.Command == "Query" {
					hasActiveQuery = true
					// Track this connection as exempt (it had a query during our wait).
					grl.mu.Lock()
					grl.exemptFromBlock[entry.ID] = true
					grl.mu.Unlock()
				}
			}

			if !hasActiveQuery {
				break
			}

			remaining := time.Until(deadline)
			if remaining <= 0 {
				// Remove from pending since we couldn't fully acquire
				grl.mu.Lock()
				delete(grl.pendingHolders, connID)
				grl.mu.Unlock()
				return errLockWaitTimeout
			}

			// Poll every 10ms
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Phase 2 complete: move from pending to ready holders.
	grl.mu.Lock()
	delete(grl.pendingHolders, connID)
	grl.holders[connID] = true
	grl.mu.Unlock()

	return nil
}

// Release removes connID from the holder set.
// If no holders remain, waiting COMMITs are unblocked.
func (grl *GlobalReadLock) Release(connID int64) {
	grl.mu.Lock()
	defer grl.mu.Unlock()
	delete(grl.holders, connID)
	delete(grl.pendingHolders, connID)
	if len(grl.holders) == 0 && len(grl.pendingHolders) == 0 && grl.ch != nil {
		close(grl.ch)
		grl.ch = nil
		// Clear exemptions when no holders remain
		for id := range grl.exemptFromBlock {
			delete(grl.exemptFromBlock, id)
		}
	}
}

// IsHeld returns true if any connection holds the global read lock (ready or pending).
func (grl *GlobalReadLock) IsHeld() bool {
	grl.mu.Lock()
	defer grl.mu.Unlock()
	return len(grl.holders) > 0 || len(grl.pendingHolders) > 0
}

// IsHeldByConn returns true if connID is a ready FTWRL holder.
func (grl *GlobalReadLock) IsHeldByConn(connID int64) bool {
	grl.mu.Lock()
	defer grl.mu.Unlock()
	return grl.holders[connID]
}

// IsHeldByOther returns true if any connection OTHER than connID has a
// fully-acquired (ready) FTWRL AND connID is not exempt from blocking.
// If held and not exempt, also returns the wait channel.
func (grl *GlobalReadLock) IsHeldByOther(connID int64) (bool, chan struct{}) {
	grl.mu.Lock()
	defer grl.mu.Unlock()
	// If connID is exempt (had an active query during phase 2 wait), don't block it.
	if grl.exemptFromBlock[connID] {
		return false, nil
	}
	for id := range grl.holders {
		if id != connID {
			return true, grl.ch
		}
	}
	return false, nil
}

// WaitIfHeldByOther blocks until all other connections' FTWRL is released,
// or until the timeout expires. Returns nil if no blocking needed.
func (grl *GlobalReadLock) WaitIfHeldByOther(connID int64, timeoutSec float64) error {
	held, waitCh := grl.IsHeldByOther(connID)
	if !held {
		return nil
	}

	timer := time.NewTimer(time.Duration(timeoutSec * float64(time.Second)))
	defer timer.Stop()

	select {
	case <-waitCh:
		return nil
	case <-timer.C:
		return errLockWaitTimeout
	}
}

