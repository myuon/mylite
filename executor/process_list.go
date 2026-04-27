package executor

import (
	"net"
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
	conn      net.Conn // underlying TCP connection for KILL support; may be nil
}

// accountKey groups connections by (user, host) for the accounts table.
type accountKey struct {
	user string
	host string
}

// AccountRow holds (user, host, current, total) for performance_schema.accounts.
type AccountRow struct {
	User    string
	Host    string
	Current int64
	Total   int64
}

// ProcessList is a shared registry of all active connections and their states.
// It tracks historical connection totals for performance_schema accounts/users/hosts tables.
// It is safe for concurrent use.
type ProcessList struct {
	mu      sync.RWMutex
	entries map[int64]*ProcessEntry
	// Separate historical total counters for accounts, users, and hosts tables.
	// Each is independently reset on TRUNCATE of the respective table.
	acctTotals map[accountKey]int64 // (user,host) → total connections for accounts table
	userTotals map[string]int64     // user → total connections for users table
	hostTotals map[string]int64     // host → total connections for hosts table
}

// NewProcessList creates a new ProcessList.
func NewProcessList() *ProcessList {
	return &ProcessList{
		entries:    make(map[int64]*ProcessEntry),
		acctTotals: make(map[accountKey]int64),
		userTotals: make(map[string]int64),
		hostTotals: make(map[string]int64),
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
	pl.acctTotals[accountKey{user, host}]++
	pl.userTotals[user]++
	pl.hostTotals[host]++
	pl.mu.Unlock()
}

// RegisterWithConn adds a connection with its underlying net.Conn so KILL can close it.
func (pl *ProcessList) RegisterWithConn(id int64, user, host, db string, conn net.Conn) {
	pl.mu.Lock()
	pl.entries[id] = &ProcessEntry{
		ID:        id,
		User:      user,
		Host:      host,
		DB:        db,
		Command:   "Sleep",
		StartTime: time.Now(),
		conn:      conn,
	}
	pl.acctTotals[accountKey{user, host}]++
	pl.userTotals[user]++
	pl.hostTotals[host]++
	pl.mu.Unlock()
}

// Unregister removes a connection from the process list.
func (pl *ProcessList) Unregister(id int64) {
	pl.mu.Lock()
	delete(pl.entries, id)
	pl.mu.Unlock()
}

// Kill removes the connection from the process list AND closes the underlying TCP connection.
// This causes the server goroutine to exit, so the next connection from the pool is truly new.
func (pl *ProcessList) Kill(id int64) {
	pl.mu.Lock()
	entry := pl.entries[id]
	delete(pl.entries, id)
	pl.mu.Unlock()
	if entry != nil && entry.conn != nil {
		entry.conn.Close() //nolint:errcheck
	}
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

// SetUser updates the user name for a connection and adjusts historical total counters.
func (pl *ProcessList) SetUser(id int64, user string) {
	pl.mu.Lock()
	if e, ok := pl.entries[id]; ok {
		oldUser := e.User
		host := e.Host
		if oldUser != user {
			oldKey := accountKey{oldUser, host}
			newKey := accountKey{user, host}
			// Move account totals: remove from old, credit new.
			if pl.acctTotals[oldKey] > 0 {
				pl.acctTotals[oldKey]--
				if pl.acctTotals[oldKey] == 0 {
					delete(pl.acctTotals, oldKey)
				}
			}
			pl.acctTotals[newKey]++
			// Move user totals.
			if pl.userTotals[oldUser] > 0 {
				pl.userTotals[oldUser]--
				if pl.userTotals[oldUser] == 0 {
					delete(pl.userTotals, oldUser)
				}
			}
			pl.userTotals[user]++
			e.User = user
		}
	}
	pl.mu.Unlock()
}

// ResetAccountTotals resets historical totals for the accounts table: after reset,
// TOTAL_CONNECTIONS equals CURRENT_CONNECTIONS for each (user, host) pair.
func (pl *ProcessList) ResetAccountTotals() {
	pl.mu.Lock()
	newTotals := make(map[accountKey]int64)
	for _, e := range pl.entries {
		newTotals[accountKey{e.User, e.Host}]++
	}
	pl.acctTotals = newTotals
	pl.mu.Unlock()
}

// ResetUserTotals resets historical totals for the users table.
func (pl *ProcessList) ResetUserTotals() {
	pl.mu.Lock()
	newTotals := make(map[string]int64)
	for _, e := range pl.entries {
		newTotals[e.User]++
	}
	pl.userTotals = newTotals
	pl.mu.Unlock()
}

// ResetHostTotals resets historical totals for the hosts table.
// As a side effect, also purges zero-current-connection entries from the accounts table
// for hosts that are being reset, matching MySQL 8.0 behaviour where truncating the hosts
// table removes stale zero-current entries from the accounts table.
func (pl *ProcessList) ResetHostTotals() {
	pl.mu.Lock()
	// Count current connections per host.
	currentByHost := make(map[string]int64)
	for _, e := range pl.entries {
		currentByHost[e.Host]++
	}
	// Reset host totals to current only.
	newHostTotals := make(map[string]int64)
	for host, cur := range currentByHost {
		newHostTotals[host] = cur
	}
	pl.hostTotals = newHostTotals
	// Also purge zero-current-connection entries from accounts that belong to hosts
	// whose counts are being reset (mirrors MySQL 8.0 cross-table cleanup).
	currentByAcct := make(map[accountKey]int64)
	for _, e := range pl.entries {
		currentByAcct[accountKey{e.User, e.Host}]++
	}
	for k := range pl.acctTotals {
		if currentByAcct[k] == 0 {
			delete(pl.acctTotals, k)
		}
	}
	pl.mu.Unlock()
}

// AccountsFull returns rows for performance_schema.accounts.
func (pl *ProcessList) AccountsFull() []AccountRow {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	// Count current connections per (user, host)
	current := make(map[accountKey]int64)
	for _, e := range pl.entries {
		current[accountKey{e.User, e.Host}]++
	}
	// Build result from totals (includes entries with 0 current but nonzero history)
	result := make([]AccountRow, 0, len(pl.acctTotals))
	seen := make(map[accountKey]bool)
	for k, total := range pl.acctTotals {
		seen[k] = true
		result = append(result, AccountRow{
			User:    k.user,
			Host:    k.host,
			Current: current[k],
			Total:   total,
		})
	}
	// Also include entries that are current but not in totals (edge case)
	for k, cur := range current {
		if !seen[k] {
			result = append(result, AccountRow{User: k.user, Host: k.host, Current: cur, Total: cur})
		}
	}
	return result
}

// UsersFull returns rows for performance_schema.users.
type UserRow struct {
	User    string
	Current int64
	Total   int64
}

// UsersFull returns all user rows.
func (pl *ProcessList) UsersFull() []UserRow {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	// Count current connections per user
	current := make(map[string]int64)
	for _, e := range pl.entries {
		current[e.User]++
	}
	result := make([]UserRow, 0, len(pl.userTotals))
	seen := make(map[string]bool)
	for user, total := range pl.userTotals {
		seen[user] = true
		result = append(result, UserRow{User: user, Current: current[user], Total: total})
	}
	for user, cur := range current {
		if !seen[user] {
			result = append(result, UserRow{User: user, Current: cur, Total: cur})
		}
	}
	return result
}

// HostRow holds (host, current, total) for performance_schema.hosts.
type HostRow struct {
	Host    string
	Current int64
	Total   int64
}

// HostsFull returns all host rows.
func (pl *ProcessList) HostsFull() []HostRow {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	// Count current connections per host
	current := make(map[string]int64)
	for _, e := range pl.entries {
		current[e.Host]++
	}
	result := make([]HostRow, 0, len(pl.hostTotals))
	seen := make(map[string]bool)
	for host, total := range pl.hostTotals {
		seen[host] = true
		result = append(result, HostRow{Host: host, Current: current[host], Total: total})
	}
	for host, cur := range current {
		if !seen[host] {
			result = append(result, HostRow{Host: host, Current: cur, Total: cur})
		}
	}
	return result
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
