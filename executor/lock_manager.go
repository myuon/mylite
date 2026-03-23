package executor

import (
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
