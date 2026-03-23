package executor

import (
	"fmt"
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

// RowLockManager manages InnoDB-style row-level locks for SELECT ... FOR UPDATE.
// Row locks are keyed by "db:table:pkValue" and held until COMMIT/ROLLBACK.
type RowLockManager struct {
	mu    sync.Mutex
	locks map[string]*rowLockEntry // lockKey -> entry
}

type rowLockEntry struct {
	owner int64        // connectionID that holds the lock
	ch    chan struct{} // closed when the lock is released
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
	deadline := time.Now().Add(time.Duration(timeoutSec * float64(time.Second)))

	for {
		rlm.mu.Lock()
		existing, exists := rlm.locks[key]

		if exists && existing.owner == connID {
			// Already own it (re-entrant)
			rlm.mu.Unlock()
			return nil
		}

		if !exists {
			// Lock is free, acquire it
			rlm.locks[key] = &rowLockEntry{
				owner: connID,
				ch:    make(chan struct{}),
			}
			rlm.mu.Unlock()
			return nil
		}

		// Lock held by another connection - wait
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

// ReleaseRowLocks releases all row locks held by a connection.
func (rlm *RowLockManager) ReleaseRowLocks(connID int64) {
	rlm.mu.Lock()
	defer rlm.mu.Unlock()

	for key, entry := range rlm.locks {
		if entry.owner == connID {
			close(entry.ch)
			delete(rlm.locks, key)
		}
	}
}

// errLockWaitTimeout is a sentinel used internally; the executor wraps it with
// the proper MySQL error code when returning to the client.
var errLockWaitTimeout = fmt.Errorf("lock_wait_timeout")

