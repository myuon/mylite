package executor

import (
	"sync"
	"time"
)

// LockManager manages user-level named locks (GET_LOCK, RELEASE_LOCK, etc.).
type LockManager struct {
	mu     sync.Mutex
	named  map[string]*namedLockEntry
	byConn map[int64]map[string]bool
}

type namedLockEntry struct {
	owner int64
	ch    chan struct{} // closed when released
}

// NewLockManager creates a new LockManager.
func NewLockManager() *LockManager {
	return &LockManager{
		named:  make(map[string]*namedLockEntry),
		byConn: make(map[int64]map[string]bool),
	}
}

// GetLock acquires a named lock. Returns 1 on success, 0 on timeout.
func (lm *LockManager) GetLock(connID int64, name string, timeoutSec float64) interface{} {
	// Truncate name to 64 chars (MySQL behavior)
	if len(name) > 64 {
		name = name[:64]
	}

	for {
		lm.mu.Lock()
		entry := lm.named[name]

		if entry == nil || entry.owner == 0 {
			// Lock is free, acquire it
			lm.named[name] = &namedLockEntry{owner: connID, ch: make(chan struct{})}
			if lm.byConn[connID] == nil {
				lm.byConn[connID] = make(map[string]bool)
			}
			lm.byConn[connID][name] = true
			lm.mu.Unlock()
			return int64(1)
		}

		if entry.owner == connID {
			// Re-entrant: same connection already holds this lock
			lm.mu.Unlock()
			return int64(1)
		}

		// Lock held by another connection, wait
		waitCh := entry.ch
		lm.mu.Unlock()

		if timeoutSec <= 0 {
			return int64(0)
		}

		timer := time.NewTimer(time.Duration(timeoutSec * float64(time.Second)))
		select {
		case <-waitCh:
			timer.Stop()
			// Lock was released, try to acquire (loop back)
			timeoutSec = 0 // Only try once more without waiting
			continue
		case <-timer.C:
			return int64(0)
		}
	}
}

// ReleaseLock releases a named lock. Returns 1 if released, 0 if not held by this conn, nil if doesn't exist.
func (lm *LockManager) ReleaseLock(connID int64, name string) interface{} {
	if len(name) > 64 {
		name = name[:64]
	}
	lm.mu.Lock()
	defer lm.mu.Unlock()

	entry := lm.named[name]
	if entry == nil {
		return nil
	}
	if entry.owner != connID {
		return int64(0)
	}

	close(entry.ch)
	entry.owner = 0
	delete(lm.byConn[connID], name)
	delete(lm.named, name)
	return int64(1)
}

// IsFreeLock returns 1 if the named lock is free, 0 if in use.
func (lm *LockManager) IsFreeLock(name string) int64 {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	entry := lm.named[name]
	if entry == nil || entry.owner == 0 {
		return 1
	}
	return 0
}

// IsUsedLock returns the connectionID holding the lock, or nil if free.
func (lm *LockManager) IsUsedLock(name string) interface{} {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	entry := lm.named[name]
	if entry == nil || entry.owner == 0 {
		return nil
	}
	return entry.owner
}

// ReleaseAllLocks releases all named locks held by a connection. Returns count released.
func (lm *LockManager) ReleaseAllLocks(connID int64) int64 {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	locks := lm.byConn[connID]
	count := int64(0)
	for name := range locks {
		if entry := lm.named[name]; entry != nil && entry.owner == connID {
			close(entry.ch)
			delete(lm.named, name)
			count++
		}
	}
	delete(lm.byConn, connID)
	return count
}
