package executor

import (
	"strings"
	"sync"
)

// ViewStore holds view definitions shared across all connections (executors).
// It stores the view's SELECT SQL, display SQL, check options, and CREATE VIEW
// statement, keyed by "db.viewname" (lower-cased database + "." + view name).
type ViewStore struct {
	mu sync.RWMutex
	// selectSQL maps "db.viewname" -> SELECT SQL (for execution).
	selectSQL map[string]string
	// displaySQL maps "db.viewname" -> canonical SELECT SQL (for IS.VIEWS).
	displaySQL map[string]string
	// checkOptions maps "db.viewname" -> check option ("cascaded", "local", or "").
	checkOptions map[string]string
	// createSQL maps "db.viewname" -> full CREATE VIEW SQL (for SHOW CREATE VIEW).
	createSQL map[string]string
	// security maps "db.viewname" -> "definer" or "invoker" (SQL SECURITY clause).
	security map[string]string
}

// NewViewStore creates a new empty ViewStore.
func NewViewStore() *ViewStore {
	return &ViewStore{
		selectSQL:    make(map[string]string),
		displaySQL:   make(map[string]string),
		checkOptions: make(map[string]string),
		createSQL:    make(map[string]string),
		security:     make(map[string]string),
	}
}

func viewKey(db, name string) string {
	if db == "" {
		return name
	}
	return db + "." + name
}

// Set stores all view definition components.
// security is "definer" or "invoker" (SQL SECURITY clause).
func (vs *ViewStore) Set(db, name, selectSQL, displaySQL, checkOption, createSQL, security string) {
	key := viewKey(db, name)
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.selectSQL[key] = selectSQL
	vs.displaySQL[key] = displaySQL
	vs.checkOptions[key] = checkOption
	vs.createSQL[key] = createSQL
	if security != "" {
		vs.security[key] = strings.ToLower(security)
	}
}

// IsInvokerSecurity returns true if the view uses SQL SECURITY INVOKER.
func (vs *ViewStore) IsInvokerSecurity(db, name string) bool {
	key := viewKey(db, name)
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return strings.ToLower(vs.security[key]) == "invoker"
}

// Delete removes a view definition.
func (vs *ViewStore) Delete(db, name string) {
	key := viewKey(db, name)
	vs.mu.Lock()
	defer vs.mu.Unlock()
	delete(vs.selectSQL, key)
	delete(vs.displaySQL, key)
	delete(vs.checkOptions, key)
	delete(vs.createSQL, key)
}

// Lookup looks up a view by db+name (case-insensitive for the name part).
// Returns (selectSQL, ok).
func (vs *ViewStore) Lookup(db, name string) (selectSQL string, ok bool) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	key := viewKey(db, name)
	if sql, found := vs.selectSQL[key]; found {
		return sql, true
	}
	// Try case-insensitive match on the name part
	prefix := db + "."
	for k, sql := range vs.selectSQL {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			if equalFoldASCII(k[len(prefix):], name) {
				return sql, true
			}
		} else if db == "" && equalFoldASCII(k, name) {
			return sql, true
		}
	}
	return "", false
}

// LookupDisplaySQL returns the display SQL for IS.VIEWS.VIEW_DEFINITION.
func (vs *ViewStore) LookupDisplaySQL(db, name string) (string, bool) {
	key := viewKey(db, name)
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	v, ok := vs.displaySQL[key]
	return v, ok
}

// LookupCheckOption returns the check option for a view.
func (vs *ViewStore) LookupCheckOption(db, name string) string {
	key := viewKey(db, name)
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.checkOptions[key]
}

// LookupCreateSQL returns the full CREATE VIEW SQL.
func (vs *ViewStore) LookupCreateSQL(db, name string) (string, bool) {
	key := viewKey(db, name)
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	v, ok := vs.createSQL[key]
	return v, ok
}

// Rename renames a view from oldName to newName within the given database.
// Returns true if the view was found and renamed.
func (vs *ViewStore) Rename(db, oldName, newName string) bool {
	oldKey := viewKey(db, oldName)
	newKey := viewKey(db, newName)
	vs.mu.Lock()
	defer vs.mu.Unlock()
	sql, ok := vs.selectSQL[oldKey]
	if !ok {
		return false
	}
	vs.selectSQL[newKey] = sql
	delete(vs.selectSQL, oldKey)
	if v, ok2 := vs.displaySQL[oldKey]; ok2 {
		vs.displaySQL[newKey] = v
		delete(vs.displaySQL, oldKey)
	}
	if v, ok2 := vs.checkOptions[oldKey]; ok2 {
		vs.checkOptions[newKey] = v
		delete(vs.checkOptions, oldKey)
	}
	if v, ok2 := vs.createSQL[oldKey]; ok2 {
		vs.createSQL[newKey] = v
		delete(vs.createSQL, oldKey)
	}
	if v, ok2 := vs.security[oldKey]; ok2 {
		vs.security[newKey] = v
		delete(vs.security, oldKey)
	}
	return true
}

// AllViews returns a snapshot of all view entries as (db, name, selectSQL) tuples.
func (vs *ViewStore) AllViews() []viewEntry {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	result := make([]viewEntry, 0, len(vs.selectSQL))
	for key, sql := range vs.selectSQL {
		result = append(result, viewEntry{key: key, selectSQL: sql})
	}
	return result
}

type viewEntry struct {
	key       string // "db.name" or just "name"
	selectSQL string
}

// AllViewCreateSQLs returns all view names and their CREATE VIEW SQL.
func (vs *ViewStore) AllViewCreateSQLs() map[string]string {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	result := make(map[string]string, len(vs.createSQL))
	for k, v := range vs.createSQL {
		result[k] = v
	}
	return result
}

// equalFoldASCII is a fast ASCII case-insensitive comparison.
func equalFoldASCII(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}
