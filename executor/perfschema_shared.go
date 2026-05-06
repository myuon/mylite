package executor

import (
	"sync"
)

// connectAttr represents a single session connection attribute (e.g. _os, program_name).
type connectAttr struct {
	Name     string
	Value    string
	Ordinal  int
}

// PerfSchemaShared holds multi-connection state for performance_schema tables.
// It is created once (in New()) and shared via Clone() across all connections.
type PerfSchemaShared struct {
	mu sync.RWMutex

	// userVars maps connectionID → variable name → value for user_variables_by_thread.
	userVars map[int64]map[string]interface{}

	// connectAttrs maps connectionID → list of connect attributes for
	// session_connect_attrs / session_account_connect_attrs.
	connectAttrs map[int64][]connectAttr

	// connectAttrUser maps connectionID → username for session_account_connect_attrs filtering.
	connectAttrUser map[int64]string
}

// NewPerfSchemaShared creates an empty shared state.
func NewPerfSchemaShared() *PerfSchemaShared {
	return &PerfSchemaShared{
		userVars:        make(map[int64]map[string]interface{}),
		connectAttrs:    make(map[int64][]connectAttr),
		connectAttrUser: make(map[int64]string),
	}
}

// SetUserVar updates a single user variable for the given connection.
// Internal variables (names starting with "__") are not tracked.
func (ps *PerfSchemaShared) SetUserVar(connID int64, name string, value interface{}) {
	if len(name) >= 2 && name[0] == '_' && name[1] == '_' {
		return // skip internal variables
	}
	ps.mu.Lock()
	if ps.userVars[connID] == nil {
		ps.userVars[connID] = make(map[string]interface{})
	}
	ps.userVars[connID][name] = value
	ps.mu.Unlock()
}

// AllUserVars returns a snapshot of all user variables across all connections,
// returned as (connID, varName, value) triples.
func (ps *PerfSchemaShared) AllUserVars() []struct {
	ConnID int64
	Name   string
	Value  interface{}
} {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	var out []struct {
		ConnID int64
		Name   string
		Value  interface{}
	}
	for connID, vars := range ps.userVars {
		for name, val := range vars {
			out = append(out, struct {
				ConnID int64
				Name   string
				Value  interface{}
			}{connID, name, val})
		}
	}
	return out
}

// RegisterConnection registers a new connection with its connect attributes and username.
func (ps *PerfSchemaShared) RegisterConnection(connID int64, attrs []connectAttr, username string) {
	ps.mu.Lock()
	ps.connectAttrs[connID] = attrs
	ps.connectAttrUser[connID] = username
	ps.mu.Unlock()
}

// UpdateConnectionUser updates the stored username for a connection (called on SET __current_user).
func (ps *PerfSchemaShared) UpdateConnectionUser(connID int64, username string) {
	ps.mu.Lock()
	ps.connectAttrUser[connID] = username
	ps.mu.Unlock()
}

// AllConnectAttrs returns a snapshot of all connect attributes across all connections.
func (ps *PerfSchemaShared) AllConnectAttrs() []struct {
	ConnID   int64
	Attrs    []connectAttr
	Username string
} {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	var out []struct {
		ConnID   int64
		Attrs    []connectAttr
		Username string
	}
	for connID, attrs := range ps.connectAttrs {
		out = append(out, struct {
			ConnID   int64
			Attrs    []connectAttr
			Username string
		}{connID, attrs, ps.connectAttrUser[connID]})
	}
	return out
}

// UnregisterConnection removes all state for a connection on disconnect.
func (ps *PerfSchemaShared) UnregisterConnection(connID int64) {
	ps.mu.Lock()
	delete(ps.userVars, connID)
	delete(ps.connectAttrs, connID)
	delete(ps.connectAttrUser, connID)
	ps.mu.Unlock()
}

// defaultConnectAttrs returns the standard set of 6 connection attributes
// that MySQL reports for every connection in session_connect_attrs.
func defaultConnectAttrs() []connectAttr {
	return []connectAttr{
		{Name: "_os", Value: "Linux", Ordinal: 0},
		{Name: "_client_name", Value: "libmysql", Ordinal: 1},
		{Name: "_pid", Value: "1", Ordinal: 2},
		{Name: "_client_version", Value: "8.0.0", Ordinal: 3},
		{Name: "_platform", Value: "x86_64", Ordinal: 4},
		{Name: "program_name", Value: "mysql", Ordinal: 5},
	}
}
