package executor

import (
	"sync"
	"time"
)

// ProcessEntry represents a single connection's state in the process list.
type ProcessEntry struct {
	ID        int64
	User      string
	Host      string
	DB        string
	Command   string // "Query", "Sleep", etc.
	StartTime time.Time
	State     string // e.g. "User lock", "executing", ""
	Info      string // current query text, or ""
}

// ProcessList is a shared registry of all active connections and their states.
// It is safe for concurrent use.
type ProcessList struct {
	mu      sync.RWMutex
	entries map[int64]*ProcessEntry
}

// NewProcessList creates a new ProcessList.
func NewProcessList() *ProcessList {
	return &ProcessList{
		entries: make(map[int64]*ProcessEntry),
	}
}

// RegisterWithID adds a connection with the given ID to the process list.
func (pl *ProcessList) RegisterWithID(id int64, user, host, db string) {
	pl.mu.Lock()
	pl.entries[id] = &ProcessEntry{
		ID:        id,
		User:      user,
		Host:      host,
		DB:        db,
		Command:   "Sleep",
		StartTime: time.Now(),
	}
	pl.mu.Unlock()
}

// Unregister removes a connection from the process list.
func (pl *ProcessList) Unregister(id int64) {
	pl.mu.Lock()
	delete(pl.entries, id)
	pl.mu.Unlock()
}

// SetQuery sets the current query and state for a connection.
func (pl *ProcessList) SetQuery(id int64, query, state string) {
	pl.mu.Lock()
	if e, ok := pl.entries[id]; ok {
		e.Command = "Query"
		e.Info = query
		e.State = state
		e.StartTime = time.Now()
	}
	pl.mu.Unlock()
}

// SetState updates only the state for a connection (e.g., "User lock").
func (pl *ProcessList) SetState(id int64, state string) {
	pl.mu.Lock()
	if e, ok := pl.entries[id]; ok {
		e.State = state
	}
	pl.mu.Unlock()
}

// ClearQuery clears the current query info after execution completes.
func (pl *ProcessList) ClearQuery(id int64) {
	pl.mu.Lock()
	if e, ok := pl.entries[id]; ok {
		e.Command = "Sleep"
		e.Info = ""
		e.State = ""
		e.StartTime = time.Now()
	}
	pl.mu.Unlock()
}

// SetDB updates the current database for a connection.
func (pl *ProcessList) SetDB(id int64, db string) {
	pl.mu.Lock()
	if e, ok := pl.entries[id]; ok {
		e.DB = db
	}
	pl.mu.Unlock()
}

// Snapshot returns a copy of all current entries.
func (pl *ProcessList) Snapshot() []ProcessEntry {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	result := make([]ProcessEntry, 0, len(pl.entries))
	for _, e := range pl.entries {
		result = append(result, *e)
	}
	return result
}
