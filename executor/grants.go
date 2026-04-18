package executor

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// GrantType represents the type of grant.
type GrantType int

const (
	GrantTypeGlobal  GrantType = iota // GRANT privs ON *.* TO user
	GrantTypeDB                       // GRANT privs ON db.* TO user
	GrantTypeTable                    // GRANT privs ON db.table TO user
	GrantTypeRole                     // GRANT role TO user
)

// GrantEntry holds one GRANT record for a user.
type GrantEntry struct {
	// Privs is the list of privilege names in uppercase, e.g. ["SELECT", "INSERT"]
	// For role grants this is nil.
	Privs []string
	// Object is the target: "*.*", "db.*", "db.table", or role name for role grants.
	Object string
	// GrantOption is true when WITH GRANT OPTION is specified.
	GrantOption bool
	// Type identifies what kind of grant this is.
	Type GrantType
	// RoleName is set for GrantTypeRole grants; it's the granted role name.
	RoleName string
	// RoleHost is the host of the granted role (default "%").
	RoleHost string
}

// GrantStore is a thread-safe in-memory store for user grants.
// It is shared across all connections (like superUsers).
type GrantStore struct {
	mu sync.RWMutex
	// entries maps "user@host" (lowercase) -> list of grant entries.
	entries map[string][]GrantEntry
	// roles maps role name (lowercase) -> list of grant entries for that role.
	roles map[string][]GrantEntry
	// defaultRoles maps "user@host" -> list of default role names (lowercase).
	defaultRoles map[string][]string
}

// NewGrantStore creates a new empty GrantStore.
func NewGrantStore() *GrantStore {
	return &GrantStore{
		entries:      make(map[string][]GrantEntry),
		roles:        make(map[string][]GrantEntry),
		defaultRoles: make(map[string][]string),
	}
}

func userKey(user, host string) string {
	return strings.ToLower(user) + "@" + strings.ToLower(host)
}

// isRole returns true if the name looks like a role (no @ in a user context).
// In MySQL, roles created with CREATE ROLE have host=% by default.
// We check if the user@host entry is registered as a role.
func (gs *GrantStore) isRole(name, host string) bool {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	_, ok := gs.roles[userKey(name, host)]
	return ok
}

// SetDefaultRoles stores the default roles for a user (called on ALTER USER ... DEFAULT ROLE ...).
// roles is a list of role names (without host). Pass nil or empty slice to clear.
func (gs *GrantStore) SetDefaultRoles(user, host string, roles []string) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	key := userKey(user, host)
	if len(roles) == 0 {
		gs.defaultRoles[key] = nil
	} else {
		normalized := make([]string, 0, len(roles))
		for _, r := range roles {
			normalized = append(normalized, strings.ToLower(strings.Trim(r, "`'\"")))
		}
		gs.defaultRoles[key] = normalized
	}
}

// GetDefaultRoles returns the default roles for a user.
func (gs *GrantStore) GetDefaultRoles(user, host string) []string {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	key := userKey(user, host)
	return gs.defaultRoles[key]
}

// GetActiveDefaultRoles returns the default roles for a user that are still actually granted to them.
// This filters out roles that have been revoked since DEFAULT ROLE was set.
func (gs *GrantStore) GetActiveDefaultRoles(user, host string) []string {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	key := userKey(user, host)
	defaultRoles := gs.defaultRoles[key]
	if len(defaultRoles) == 0 {
		return nil
	}
	// Build set of currently granted roles for this user
	grantedRoles := make(map[string]bool)
	for _, e := range gs.entries[key] {
		if e.Type == GrantTypeRole {
			grantedRoles[strings.ToLower(e.RoleName)] = true
		}
	}
	var active []string
	for _, r := range defaultRoles {
		if grantedRoles[strings.ToLower(r)] {
			active = append(active, r)
		}
	}
	return active
}

// RegisterRole marks a name as a role (called on CREATE ROLE).
func (gs *GrantStore) RegisterRole(name string) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	key := userKey(name, "%")
	if gs.roles[key] == nil {
		gs.roles[key] = []GrantEntry{}
	}
}

// AddPrivGrant adds a privilege grant on an object for a user.
// privs is like "SELECT,INSERT" or "ALL PRIVILEGES" etc.
// object is like "*.*", "db.*", "db.table".
func (gs *GrantStore) AddPrivGrant(user, host, privs, object string, grantOption bool) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	key := userKey(user, host)

	privList := normalizePrivList(privs)
	gtype := objectToGrantType(object)

	// Check if role key exists
	roleKey := userKey(user, "%")
	isRole := false
	if _, ok := gs.roles[roleKey]; ok && host == "%" {
		isRole = true
	}

	// Find existing entry for same object
	var targetMap map[string][]GrantEntry
	if isRole {
		targetMap = gs.roles
	} else {
		targetMap = gs.entries
	}

	entries := targetMap[key]
	for i, e := range entries {
		if e.Object == object && e.Type == gtype {
			// Merge privileges
			merged := mergePrivs(e.Privs, privList)
			entries[i].Privs = merged
			if grantOption {
				entries[i].GrantOption = true
			}
			targetMap[key] = entries
			return
		}
	}
	// New entry
	entries = append(entries, GrantEntry{
		Privs:       privList,
		Object:      object,
		GrantOption: grantOption,
		Type:        gtype,
	})
	targetMap[key] = entries
}

// AddRoleGrant records that a role was granted to a user (GRANT role TO user).
func (gs *GrantStore) AddRoleGrant(roleName, roleHost, toUser, toHost string, adminOption bool) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	key := userKey(toUser, toHost)
	entries := gs.entries[key]
	// Check for duplicate
	for i, e := range entries {
		if e.Type == GrantTypeRole && strings.EqualFold(e.RoleName, roleName) {
			if adminOption {
				entries[i].GrantOption = true
			}
			gs.entries[key] = entries
			return
		}
	}
	entries = append(entries, GrantEntry{
		Type:        GrantTypeRole,
		RoleName:    roleName,
		RoleHost:    roleHost,
		GrantOption: adminOption,
	})
	gs.entries[key] = entries
}

// RevokePrivGrant removes a privilege grant for a user on an object.
func (gs *GrantStore) RevokePrivGrant(user, host, privs, object string) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	key := userKey(user, host)
	privList := normalizePrivList(privs)
	gtype := objectToGrantType(object)

	// Check both entries and roles
	for _, targetMap := range []map[string][]GrantEntry{gs.entries, gs.roles} {
		entries := targetMap[key]
		for i, e := range entries {
			if e.Object == object && e.Type == gtype {
				remaining := removePrivs(e.Privs, privList)
				if len(remaining) == 0 {
					// Remove the entry entirely
					targetMap[key] = append(entries[:i], entries[i+1:]...)
				} else {
					entries[i].Privs = remaining
					targetMap[key] = entries
				}
				return
			}
		}
	}
}

// RevokeAllPrivGrants removes all privilege grants for a user.
func (gs *GrantStore) RevokeAllPrivGrants(user, host string) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	key := userKey(user, host)
	delete(gs.entries, key)
}

// RevokeRoleGrant removes a role membership grant for a user.
// roleName may include @host (e.g. "r1@%") or just the role name (e.g. "r1").
func (gs *GrantStore) RevokeRoleGrant(user, host, roleName string) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	key := userKey(user, host)
	// Parse role name, stripping host if present
	rn := roleName
	if atIdx := strings.LastIndex(rn, "@"); atIdx >= 0 {
		rn = strings.TrimSpace(rn[:atIdx])
	}
	rn = strings.Trim(strings.TrimSpace(rn), "`'\"")
	entries := gs.entries[key]
	newEntries := entries[:0]
	for _, e := range entries {
		if e.Type == GrantTypeRole && strings.EqualFold(e.RoleName, rn) {
			continue // remove this role grant
		}
		newEntries = append(newEntries, e)
	}
	if len(newEntries) == 0 {
		delete(gs.entries, key)
	} else {
		gs.entries[key] = newEntries
	}
}

// GetGrants returns all grant entries for a user.
func (gs *GrantStore) GetGrants(user, host string) []GrantEntry {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	key := userKey(user, host)
	result := make([]GrantEntry, len(gs.entries[key]))
	copy(result, gs.entries[key])
	return result
}

// GetRoleGrants returns all grant entries for a role.
func (gs *GrantStore) GetRoleGrants(roleName string) []GrantEntry {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	key := userKey(roleName, "%")
	result := make([]GrantEntry, len(gs.roles[key]))
	copy(result, gs.roles[key])
	return result
}

// HasPrivilege checks if a user (with active roles) has a specific privilege on db.table.
// priv is like "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", etc.
// db and table are the target (empty table means DB-level check, both empty means global check).
// activeRoles is the list of currently active role names.
func (gs *GrantStore) HasPrivilege(user, host, priv, db, table string, activeRoles []string) bool {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	priv = strings.ToUpper(priv)

	// checkEntries checks a list of entries for the given privilege on db.table
	var checkEntries func(entries []GrantEntry) bool
	checkEntries = func(entries []GrantEntry) bool {
		for _, e := range entries {
			if e.Type == GrantTypeRole {
				continue
			}
			// Check if this entry covers the required privilege
			hasPriv := false
			for _, p := range e.Privs {
				if p == "ALL PRIVILEGES" || strings.EqualFold(p, priv) {
					hasPriv = true
					break
				}
				// Handle column-level grants: "SELECT(c1)" counts as SELECT privilege
				// at the table level (column enforcement would be separate)
				if parenIdx := strings.Index(p, "("); parenIdx > 0 {
					basePriv := strings.TrimSpace(p[:parenIdx])
					if strings.EqualFold(basePriv, priv) {
						hasPriv = true
						break
					}
				}
			}
			if !hasPriv {
				continue
			}
			// Check scope: does this entry cover db.table?
			switch e.Type {
			case GrantTypeGlobal: // *.* covers everything
				return true
			case GrantTypeDB: // db.* covers this db
				entryDB := strings.TrimSuffix(e.Object, ".*")
				if db != "" && dbNameMatches(entryDB, db) {
					return true
				}
			case GrantTypeTable: // db.tbl covers this specific table
				parts := strings.SplitN(e.Object, ".", 2)
				if len(parts) == 2 && db != "" && table != "" &&
					dbNameMatches(parts[0], db) && strings.EqualFold(parts[1], table) {
					return true
				}
			}
		}
		return false
	}

	// Check user's own grants (exact host, then wildcard host)
	uk := userKey(user, host)
	if checkEntries(gs.entries[uk]) {
		return true
	}
	// Also check grants stored with wildcard host "%"
	if host != "%" {
		ukWild := userKey(user, "%")
		if checkEntries(gs.entries[ukWild]) {
			return true
		}
	}

	// Expand active roles recursively
	var checkRole func(roleName, roleHost string, visited map[string]bool) bool
	checkRole = func(roleName, roleHost string, visited map[string]bool) bool {
		rk := userKey(roleName, roleHost)
		if visited[rk] {
			return false
		}
		visited[rk] = true
		// Check role's privilege grants
		if checkEntries(gs.roles[rk]) {
			return true
		}
		// Also check gs.entries for this role (role-to-role grants are in entries)
		allEntries := append(gs.roles[rk], gs.entries[rk]...)
		for _, e := range allEntries {
			if e.Type == GrantTypeRole {
				nestedHost := e.RoleHost
				if nestedHost == "" {
					nestedHost = "%"
				}
				if checkRole(e.RoleName, nestedHost, visited) {
					return true
				}
			}
		}
		return false
	}

	visited := make(map[string]bool)
	for _, roleName := range activeRoles {
		if checkRole(roleName, "%", visited) {
			return true
		}
	}

	return false
}

// tablePrivileges is the set of privileges that affect table-level access.
// We only enforce table-level checks when the user has explicit table-level privilege grants.
var tablePrivileges = map[string]bool{
	"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true,
	"CREATE": true, "DROP": true, "ALTER": true, "INDEX": true,
	"ALL PRIVILEGES": true, "ALL": true,
	"CREATE TEMPORARY TABLES": true,
}

// UserHasAnyTablePrivGrant returns true if the given user (at exact host or wildcard %) has any
// table-level privilege grant entries (not just role membership or non-table grants like EXECUTE/FILE).
// This is used to determine if privilege enforcement should be applied.
// Users with only EXECUTE, FILE, PROCESS etc. or role memberships don't get table-level enforcement.
func (gs *GrantStore) UserHasAnyTablePrivGrant(user, host string) bool {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	hasTablePrivEntry := func(entries []GrantEntry) bool {
		for _, e := range entries {
			if e.Type == GrantTypeRole {
				continue
			}
			for _, p := range e.Privs {
				basePriv := strings.ToUpper(strings.TrimSpace(p))
				if idx := strings.Index(basePriv, "("); idx >= 0 {
					basePriv = strings.TrimSpace(basePriv[:idx])
				}
				if tablePrivileges[basePriv] {
					return true
				}
			}
		}
		return false
	}

	uk := userKey(user, host)
	if hasTablePrivEntry(gs.entries[uk]) {
		return true
	}
	if host != "%" {
		ukWild := userKey(user, "%")
		if hasTablePrivEntry(gs.entries[ukWild]) {
			return true
		}
	}
	return false
}

// dbNameMatches checks if a stored DB name pattern matches the actual DB name.
// Supports MySQL LIKE patterns (% and _) for database name matching.
func dbNameMatches(pattern, dbName string) bool {
	if strings.EqualFold(pattern, dbName) {
		return true
	}
	// Check if pattern contains wildcard characters
	if strings.ContainsAny(pattern, "%_") {
		return matchLike(strings.ToLower(dbName), strings.ToLower(pattern))
	}
	return false
}

// BuildShowGrants builds SHOW GRANTS output rows for a user.
// usingRoles is the list of role names to expand (from USING clause).
// Returns rows like ["GRANT SELECT ON *.* TO `u`@`h`"].
func (gs *GrantStore) BuildShowGrants(user, host string, usingRoles []string) []string {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	key := userKey(user, host)
	entries := gs.entries[key]

	// Collect all privilege grants, possibly with role expansion
	// globalPrivs accumulates global privileges, dbPrivs[db] etc.
	type privKey struct {
		object string
		gtype  GrantType
	}

	type privSet struct {
		privs       map[string]bool
		grantOption bool
	}

	privMap := make(map[privKey]*privSet)

	addPrivEntries := func(es []GrantEntry) {
		for _, e := range es {
			if e.Type == GrantTypeRole {
				continue // role grants handled separately
			}
			k := privKey{e.Object, e.Type}
			if privMap[k] == nil {
				privMap[k] = &privSet{privs: make(map[string]bool)}
			}
			for _, p := range e.Privs {
				privMap[k].privs[p] = true
			}
			if e.GrantOption {
				privMap[k].grantOption = true
			}
		}
	}

	// expandRole recursively expands a role's privilege grants into privMap.
	// visited prevents infinite recursion on circular role grants.
	// Roles store privilege grants in gs.roles, but role-to-role grants go into gs.entries.
	var expandRole func(roleName, roleHost string, visited map[string]bool)
	expandRole = func(roleName, roleHost string, visited map[string]bool) {
		rk := userKey(roleName, roleHost)
		if visited[rk] {
			return
		}
		visited[rk] = true
		// Privilege grants for this role are in gs.roles
		rolePrivEntries := gs.roles[rk]
		addPrivEntries(rolePrivEntries)
		// Nested role grants for this role may be in gs.entries (role-to-role grants)
		// or in gs.roles (if AddRoleGrant was corrected for roles)
		allEntries := append(rolePrivEntries, gs.entries[rk]...)
		for _, e := range allEntries {
			if e.Type == GrantTypeRole {
				nestedHost := e.RoleHost
				if nestedHost == "" {
					nestedHost = "%"
				}
				expandRole(e.RoleName, nestedHost, visited)
			}
		}
	}

	addPrivEntries(entries)

	// Expand using roles (with recursive nested role expansion)
	visited := make(map[string]bool)
	for _, roleName := range usingRoles {
		expandRole(roleName, "%", visited)
	}

	// Check if there's a global privilege entry
	hasGlobal := false
	for k := range privMap {
		if k.gtype == GrantTypeGlobal {
			hasGlobal = true
			break
		}
	}

	var rows []string

	// Always first: GRANT USAGE ON *.* (or ALL PRIVILEGES if they have everything)
	globalKey := privKey{"*.*", GrantTypeGlobal}
	if ps := privMap[globalKey]; ps != nil && len(ps.privs) > 0 {
		privStr := formatPrivList(ps.privs)
		suffix := ""
		if ps.grantOption {
			suffix = " WITH GRANT OPTION"
		}
		rows = append(rows, fmt.Sprintf("GRANT %s ON *.* TO `%s`@`%s`%s", privStr, user, host, suffix))
		hasGlobal = true
	}
	if !hasGlobal {
		rows = append(rows, fmt.Sprintf("GRANT USAGE ON *.* TO `%s`@`%s`", user, host))
	}

	// DB-level grants
	var dbKeys []privKey
	for k := range privMap {
		if k.gtype == GrantTypeDB {
			dbKeys = append(dbKeys, k)
		}
	}
	sort.Slice(dbKeys, func(i, j int) bool { return dbKeys[i].object < dbKeys[j].object })
	for _, k := range dbKeys {
		ps := privMap[k]
		privStr := formatPrivList(ps.privs)
		dbName := strings.TrimSuffix(k.object, ".*")
		suffix := ""
		if ps.grantOption {
			suffix = " WITH GRANT OPTION"
		}
		rows = append(rows, fmt.Sprintf("GRANT %s ON `%s`.* TO `%s`@`%s`%s", privStr, dbName, user, host, suffix))
	}

	// Table-level grants
	var tableKeys []privKey
	for k := range privMap {
		if k.gtype == GrantTypeTable {
			tableKeys = append(tableKeys, k)
		}
	}
	sort.Slice(tableKeys, func(i, j int) bool { return tableKeys[i].object < tableKeys[j].object })
	for _, k := range tableKeys {
		ps := privMap[k]
		privStr := formatPrivList(ps.privs)
		parts := strings.SplitN(k.object, ".", 2)
		suffix := ""
		if ps.grantOption {
			suffix = " WITH GRANT OPTION"
		}
		rows = append(rows, fmt.Sprintf("GRANT %s ON `%s`.`%s` TO `%s`@`%s`%s", privStr, parts[0], parts[1], user, host, suffix))
	}

	// Role membership grants: group by admin option, sort, and format as comma-separated
	// Also collect WITH ADMIN OPTION roles inherited from USING roles (recursively).
	var roleGrantsNoAdmin []GrantEntry
	var roleGrantsWithAdmin []GrantEntry
	// Track inherited admin roles to avoid duplicates
	inheritedAdminRoles := make(map[string]bool) // "roleName@host" -> true

	// Collect inherited WITH ADMIN OPTION roles from USING roles (recursively)
	var collectInheritedAdminRoles func(roleName, roleHost string, visitedAdm map[string]bool)
	collectInheritedAdminRoles = func(roleName, roleHost string, visitedAdm map[string]bool) {
		rk := userKey(roleName, roleHost)
		if visitedAdm[rk] {
			return
		}
		visitedAdm[rk] = true
		// Look in both gs.roles and gs.entries for this role's nested role grants
		allEntries := append(gs.roles[rk], gs.entries[rk]...)
		for _, e := range allEntries {
			if e.Type == GrantTypeRole && e.GrantOption {
				nestedHost := e.RoleHost
				if nestedHost == "" {
					nestedHost = "%"
				}
				inheritedKey := userKey(e.RoleName, nestedHost)
				if !inheritedAdminRoles[inheritedKey] {
					inheritedAdminRoles[inheritedKey] = true
					roleGrantsWithAdmin = append(roleGrantsWithAdmin, GrantEntry{
						Type:        GrantTypeRole,
						RoleName:    e.RoleName,
						RoleHost:    nestedHost,
						GrantOption: true,
					})
				}
				// Recurse
				collectInheritedAdminRoles(e.RoleName, nestedHost, visitedAdm)
			}
		}
	}

	for _, roleName := range usingRoles {
		collectInheritedAdminRoles(roleName, "%", make(map[string]bool))
	}

	for _, e := range entries {
		if e.Type == GrantTypeRole {
			if e.GrantOption {
				// Only add if not already in inherited list
				rk := userKey(e.RoleName, e.RoleHost)
				if !inheritedAdminRoles[rk] {
					roleGrantsWithAdmin = append(roleGrantsWithAdmin, e)
				}
			} else {
				roleGrantsNoAdmin = append(roleGrantsNoAdmin, e)
			}
		}
	}
	sort.Slice(roleGrantsNoAdmin, func(i, j int) bool {
		return strings.ToLower(roleGrantsNoAdmin[i].RoleName) < strings.ToLower(roleGrantsNoAdmin[j].RoleName)
	})
	sort.Slice(roleGrantsWithAdmin, func(i, j int) bool {
		return strings.ToLower(roleGrantsWithAdmin[i].RoleName) < strings.ToLower(roleGrantsWithAdmin[j].RoleName)
	})
	if len(roleGrantsNoAdmin) > 0 {
		var parts []string
		for _, e := range roleGrantsNoAdmin {
			rh := e.RoleHost
			if rh == "" {
				rh = "%"
			}
			parts = append(parts, fmt.Sprintf("`%s`@`%s`", e.RoleName, rh))
		}
		rows = append(rows, fmt.Sprintf("GRANT %s TO `%s`@`%s`", strings.Join(parts, ","), user, host))
	}
	if len(roleGrantsWithAdmin) > 0 {
		var parts []string
		for _, e := range roleGrantsWithAdmin {
			rh := e.RoleHost
			if rh == "" {
				rh = "%"
			}
			parts = append(parts, fmt.Sprintf("`%s`@`%s`", e.RoleName, rh))
		}
		rows = append(rows, fmt.Sprintf("GRANT %s TO `%s`@`%s` WITH ADMIN OPTION", strings.Join(parts, ","), user, host))
	}

	return rows
}

// objectToGrantType determines the GrantType from an object string.
func objectToGrantType(object string) GrantType {
	if object == "*.*" {
		return GrantTypeGlobal
	}
	if strings.HasSuffix(object, ".*") {
		return GrantTypeDB
	}
	return GrantTypeTable
}

// normalizePrivList converts a comma-separated privilege string into a sorted uppercase list.
// For column-level grants like "SELECT(c1)", the priv name is uppercased but column names
// are kept lowercase (MySQL stores them lowercase).
func normalizePrivList(privs string) []string {
	// Expand ALL / ALL PRIVILEGES
	upper := strings.ToUpper(strings.TrimSpace(privs))
	if upper == "ALL" || upper == "ALL PRIVILEGES" {
		return []string{"ALL PRIVILEGES"}
	}
	parts := strings.Split(privs, ",")
	var result []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		// Handle column-level grants: "SELECT(c1)" -> "SELECT(c1)" with uppercase priv name
		if parenIdx := strings.Index(p, "("); parenIdx > 0 {
			basePriv := strings.ToUpper(strings.TrimSpace(p[:parenIdx]))
			colPart := p[parenIdx:] // "(c1)" - preserve as-is but lowercase col names
			// Lowercase the column names inside parens
			colPart = "(" + strings.ToLower(strings.Trim(strings.TrimSpace(colPart), "()")) + ")"
			result = append(result, basePriv+colPart)
		} else {
			result = append(result, strings.ToUpper(p))
		}
	}
	sort.Strings(result)
	return result
}

// mergePrivs merges two privilege lists, removing duplicates.
// If either contains "ALL PRIVILEGES", return that.
func mergePrivs(existing, newPrivs []string) []string {
	for _, p := range existing {
		if p == "ALL PRIVILEGES" {
			return existing
		}
	}
	for _, p := range newPrivs {
		if p == "ALL PRIVILEGES" {
			return []string{"ALL PRIVILEGES"}
		}
	}
	seen := make(map[string]bool)
	for _, p := range existing {
		seen[p] = true
	}
	result := make([]string, len(existing))
	copy(result, existing)
	for _, p := range newPrivs {
		if !seen[p] {
			seen[p] = true
			result = append(result, p)
		}
	}
	sort.Strings(result)
	return result
}

// removePrivs removes privs from existing. Returns remaining.
func removePrivs(existing, toRemove []string) []string {
	// If removing ALL PRIVILEGES, clear everything
	for _, p := range toRemove {
		if p == "ALL PRIVILEGES" {
			return nil
		}
	}
	remove := make(map[string]bool)
	for _, p := range toRemove {
		remove[p] = true
	}
	var result []string
	for _, p := range existing {
		if !remove[p] {
			result = append(result, p)
		}
	}
	return result
}

// formatPrivList formats a set of privileges for display.
// MySQL canonical order: SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, SHUTDOWN,
// PROCESS, FILE, REFERENCES, INDEX, ALTER, SHOW DATABASES, SUPER, CREATE TEMPORARY TABLES,
// LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW,
// CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, CREATE TABLESPACE, ...
var privOrder = []string{
	"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "RELOAD",
	"SHUTDOWN", "PROCESS", "FILE", "REFERENCES", "INDEX", "ALTER",
	"SHOW DATABASES", "SUPER", "CREATE TEMPORARY TABLES", "LOCK TABLES",
	"EXECUTE", "REPLICATION SLAVE", "REPLICATION CLIENT", "CREATE VIEW",
	"SHOW VIEW", "CREATE ROUTINE", "ALTER ROUTINE", "CREATE USER",
	"EVENT", "TRIGGER", "CREATE TABLESPACE",
}

func formatPrivList(privs map[string]bool) string {
	if privs["ALL PRIVILEGES"] {
		return "ALL PRIVILEGES"
	}
	var ordered []string
	seen := make(map[string]bool)
	for _, p := range privOrder {
		if privs[p] {
			ordered = append(ordered, p)
			seen[p] = true
		}
		// Also check for column-level grants (e.g., "SELECT(c1)")
		for pv := range privs {
			if !seen[pv] {
				if parenIdx := strings.Index(pv, "("); parenIdx > 0 {
					basePriv := strings.ToUpper(strings.TrimSpace(pv[:parenIdx]))
					if basePriv == p {
						// Format as: "SELECT (`c1`)"
						colPart := strings.Trim(pv[parenIdx:], "()")
						// Backtick each column
						cols := strings.Split(colPart, ",")
						var quotedCols []string
						for _, c := range cols {
							c = strings.TrimSpace(c)
							quotedCols = append(quotedCols, "`"+c+"`")
						}
						ordered = append(ordered, fmt.Sprintf("%s (%s)", p, strings.Join(quotedCols, ", ")))
						seen[pv] = true
					}
				}
			}
		}
	}
	// Add any remaining privs not in our order list
	var rest []string
	for p := range privs {
		if !seen[p] {
			// Check if it's a column-level grant
			if parenIdx := strings.Index(p, "("); parenIdx > 0 {
				basePriv := strings.ToUpper(strings.TrimSpace(p[:parenIdx]))
				colPart := strings.Trim(p[parenIdx:], "()")
				cols := strings.Split(colPart, ",")
				var quotedCols []string
				for _, c := range cols {
					c = strings.TrimSpace(c)
					quotedCols = append(quotedCols, "`"+c+"`")
				}
				rest = append(rest, fmt.Sprintf("%s (%s)", basePriv, strings.Join(quotedCols, ", ")))
			} else {
				rest = append(rest, p)
			}
		}
	}
	sort.Strings(rest)
	ordered = append(ordered, rest...)
	return strings.Join(ordered, ", ")
}

// ParseGrantStatement parses a GRANT statement and returns its components.
// Returns: privs, object, toUser, toHost, isRoleGrant, grantOption, adminOption
func ParseGrantStatement(query string) (privs, object, toUser, toHost string, isRoleGrant bool, grantOption, adminOption bool) {
	// Normalize
	q := strings.TrimRight(strings.TrimSpace(query), ";")
	upper := strings.ToUpper(q)

	// Find " TO " separator
	toIdx := strings.Index(upper, " TO ")
	if toIdx < 0 {
		return
	}

	grantPart := strings.TrimSpace(q[6:toIdx]) // strip "GRANT "
	toPart := strings.TrimSpace(q[toIdx+4:])

	// Check for WITH GRANT OPTION / WITH ADMIN OPTION
	upperTo := strings.ToUpper(toPart)
	if wi := strings.Index(upperTo, " WITH GRANT OPTION"); wi >= 0 {
		grantOption = true
		toPart = strings.TrimSpace(toPart[:wi])
	} else if wi := strings.Index(upperTo, " WITH ADMIN OPTION"); wi >= 0 {
		adminOption = true
		toPart = strings.TrimSpace(toPart[:wi])
	}

	// Parse toUser@toHost
	toPart = strings.TrimRight(toPart, ";")
	// Handle multiple targets - just take first for now
	// toUser@toHost format: user@host, `user`@`host`, 'user'@'host'
	toUser, toHost = parseUserHost(toPart)

	// Check for " ON " to distinguish privilege grant from role grant
	onIdx := strings.Index(strings.ToUpper(grantPart), " ON ")
	if onIdx < 0 {
		// No ON clause -> this is a role grant: GRANT role TO user
		// grantPart is the role name(s), possibly comma-separated
		// For now handle single role
		rolePart := strings.TrimSpace(grantPart)
		// Could be multiple roles: "r1, r2" - just use first
		if commaIdx := strings.Index(rolePart, ","); commaIdx >= 0 {
			rolePart = strings.TrimSpace(rolePart[:commaIdx])
		}
		privs = rolePart // role name stored in privs for caller to check
		isRoleGrant = true
		return
	}

	// GRANT privs ON object TO user
	privs = strings.TrimSpace(grantPart[:onIdx])
	object = strings.TrimSpace(grantPart[onIdx+4:])

	// Normalize object: remove backticks and normalize
	object = normalizeGrantObject(object)
	return
}

// ParseRevokeStatement parses a REVOKE statement.
// Returns: privs, object, fromUser, fromHost, isRoleRevoke
func ParseRevokeStatement(query string) (privs, object, fromUser, fromHost string, isRoleRevoke bool) {
	q := strings.TrimRight(strings.TrimSpace(query), ";")
	upper := strings.ToUpper(q)

	// REVOKE ALL PRIVILEGES, GRANT OPTION FROM user (no ON clause — revoke everything globally)
	// Distinguish from "REVOKE ALL PRIVILEGES ON db.tbl FROM user" which has an ON clause.
	if strings.HasPrefix(upper, "REVOKE ALL PRIVILEGES") && strings.Contains(upper, " FROM ") {
		// Check if there is an ON clause between "REVOKE ALL PRIVILEGES" and "FROM"
		fromIdx := strings.LastIndex(upper, " FROM ")
		revokePart := strings.TrimSpace(upper[len("REVOKE "):fromIdx])
		if !strings.Contains(revokePart, " ON ") {
			// No ON clause → this is the global REVOKE ALL PRIVILEGES, GRANT OPTION FROM user form
			fromPart := strings.TrimSpace(q[fromIdx+6:])
			fromUser, fromHost = parseUserHost(fromPart)
			privs = "ALL PRIVILEGES"
			object = "*.*"
			return
		}
		// Has ON clause → fall through to the general parsing below
	}

	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx < 0 {
		return
	}

	revokePart := strings.TrimSpace(q[7:fromIdx]) // strip "REVOKE "
	fromPart := strings.TrimSpace(q[fromIdx+6:])
	fromUser, fromHost = parseUserHost(fromPart)

	onIdx := strings.Index(strings.ToUpper(revokePart), " ON ")
	if onIdx < 0 {
		// Role revoke
		privs = strings.TrimSpace(revokePart)
		isRoleRevoke = true
		return
	}

	privs = strings.TrimSpace(revokePart[:onIdx])
	object = normalizeGrantObject(strings.TrimSpace(revokePart[onIdx+4:]))
	return
}

// parseUserHost parses "user@host" or "'user'@'host'" or "`user`@`host`".
func parseUserHost(s string) (user, host string) {
	s = strings.TrimSpace(s)
	s = strings.TrimRight(s, ";")

	// Find the @ separator - use last @ for safety
	atIdx := strings.LastIndex(s, "@")
	if atIdx < 0 {
		// No host - might be a role name (host = %)
		user = strings.Trim(s, "`'\"")
		host = "%"
		return
	}
	userPart := s[:atIdx]
	hostPart := s[atIdx+1:]
	user = strings.Trim(strings.TrimSpace(userPart), "`'\"")
	host = strings.Trim(strings.TrimSpace(hostPart), "`'\"")
	return
}

// normalizeGrantObject converts ON clause object to canonical form.
// e.g. "`db`.`tbl`" -> "db.tbl", "MySQLtest.*" -> "mysqltest.*"
func normalizeGrantObject(obj string) string {
	// Remove backticks and quotes
	obj = strings.TrimSpace(obj)
	// Split on dot
	parts := strings.SplitN(obj, ".", 2)
	if len(parts) != 2 {
		return obj
	}
	db := strings.Trim(parts[0], "`'\"")
	table := strings.Trim(parts[1], "`'\"")

	// Lowercase DB name (MySQL lower_case_table_names=1 behavior)
	db = strings.ToLower(db)

	if table == "*" {
		if db == "*" {
			return "*.*"
		}
		return db + ".*"
	}
	return db + "." + strings.ToLower(table)
}
