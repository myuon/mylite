package executor

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// LockManager manages MySQL user-level locks (GET_LOCK/RELEASE_LOCK).
type LockManager struct {
	mu    sync.Mutex
	locks map[string]*userLock // lock name -> lock info
}

type userLock struct {
	ownerID int64         // connection ID that owns the lock
	ch      chan struct{} // closed when the lock is released
}

// NewLockManager creates a new LockManager.
func NewLockManager() *LockManager {
	return &LockManager{
		locks: make(map[string]*userLock),
	}
}

// GetLock tries to acquire a named lock with a timeout (in seconds).
// Returns 1 if acquired, 0 if timed out, nil if error.
// The connID identifies the connection requesting the lock.
// If setStateFn is non-nil, it is called with "User lock" before blocking
// and with "" after acquiring.
func (lm *LockManager) GetLock(name string, timeout float64, connID int64, setStateFn func(string)) int64 {
	lm.mu.Lock()

	existing, exists := lm.locks[name]
	if exists && existing.ownerID == connID {
		// Already own it - re-entrant
		lm.mu.Unlock()
		return 1
	}

	if !exists {
		// Lock is free, acquire it
		lm.locks[name] = &userLock{
			ownerID: connID,
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
		if _, stillExists := lm.locks[name]; !stillExists {
			lm.locks[name] = &userLock{
				ownerID: connID,
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

// ReleaseLock releases a named lock. Returns 1 if released, 0 if not owned by this connection, nil if not exists.
func (lm *LockManager) ReleaseLock(name string, connID int64) interface{} {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	existing, exists := lm.locks[name]
	if !exists {
		return nil
	}
	if existing.ownerID != connID {
		return int64(0)
	}

	close(existing.ch)
	delete(lm.locks, name)
	return int64(1)
}

// ReleaseAllLocks releases all locks held by a connection. Returns the count released.
func (lm *LockManager) ReleaseAllLocks(connID int64) int64 {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	var count int64
	for name, lock := range lm.locks {
		if lock.ownerID == connID {
			close(lock.ch)
			delete(lm.locks, name)
			count++
		}
	}
	return count
}

// IsFreeLock checks if a lock name is free. Returns 1 if free, 0 if in use.
func (lm *LockManager) IsFreeLock(name string) int64 {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if _, exists := lm.locks[name]; exists {
		return 0
	}
	return 1
}

// IsUsedLock checks if a lock is in use. Returns the connection ID of the owner, or nil.
func (lm *LockManager) IsUsedLock(name string) interface{} {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lock, exists := lm.locks[name]; exists {
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
	mu    sync.Mutex
	locks map[string]*rowLockEntry // lockKey -> entry
}

type rowLockEntry struct {
	exclusive bool           // true = X lock, false = S lock
	owners    map[int64]bool // connectionIDs that hold the lock
	ch        chan struct{}   // closed when the lock state changes (released or downgraded)
}

// NewRowLockManager creates a new RowLockManager.
func NewRowLockManager() *RowLockManager {
	return &RowLockManager{
		locks: make(map[string]*rowLockEntry),
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
			rlm.mu.Unlock()
			return nil
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
				return errLockWaitTimeout
			}
		}
	}
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

// errLockWaitTimeout is a sentinel used internally; the executor wraps it with
// the proper MySQL error code when returning to the client.
var errLockWaitTimeout = fmt.Errorf("lock_wait_timeout")

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

