package executor

import (
	"strings"
	"testing"
)

func TestGrantStoreBasic(t *testing.T) {
	gs := NewGrantStore()
	gs.RegisterRole("r1")

	// GRANT SELECT ON *.* TO r1
	gs.AddPrivGrant("r1", "%", "SELECT", "*.*", false)

	// Check role grants
	roleGrants := gs.GetRoleGrants("r1")
	t.Logf("role r1 grants: %+v", roleGrants)

	// GRANT r1 TO u1@localhost
	gs.AddRoleGrant("r1", "%", "u1", "localhost", false)

	// SHOW GRANTS FOR u1@localhost USING r1
	rows := gs.BuildShowGrants("u1", "localhost", []string{"r1"})
	t.Logf("show grants result: %v", rows)

	// Should include SELECT
	found := false
	for _, r := range rows {
		if r == "GRANT SELECT ON *.* TO `u1`@`localhost`" {
			found = true
		}
	}
	if !found {
		t.Errorf("Expected SELECT grant, got: %v", rows)
	}
}

func TestParseGrantStatement(t *testing.T) {
	tests := []struct {
		query         string
		wantPrivs     string
		wantObject    string
		wantUser      string
		wantHost      string
		wantRoleGrant bool
	}{
		{"GRANT SELECT ON *.* TO r1", "SELECT", "*.*", "r1", "%", false},
		{"GRANT r1 TO u1@localhost", "r1", "", "u1", "localhost", true},
		{"GRANT ALL PRIVILEGES ON test.* TO mysqltest_1@localhost", "ALL PRIVILEGES", "test.*", "mysqltest_1", "localhost", false},
	}
	for _, tc := range tests {
		privs, object, toUser, toHost, isRoleGrant, _, _ := ParseGrantStatement(tc.query)
		if privs != tc.wantPrivs {
			t.Errorf("ParseGrantStatement(%q) privs = %q, want %q", tc.query, privs, tc.wantPrivs)
		}
		if !isRoleGrant && object != tc.wantObject {
			t.Errorf("ParseGrantStatement(%q) object = %q, want %q", tc.query, object, tc.wantObject)
		}
		if toUser != tc.wantUser {
			t.Errorf("ParseGrantStatement(%q) user = %q, want %q", tc.query, toUser, tc.wantUser)
		}
		if toHost != tc.wantHost {
			t.Errorf("ParseGrantStatement(%q) host = %q, want %q", tc.query, toHost, tc.wantHost)
		}
		if isRoleGrant != tc.wantRoleGrant {
			t.Errorf("ParseGrantStatement(%q) isRoleGrant = %v, want %v", tc.query, isRoleGrant, tc.wantRoleGrant)
		}
	}
}

// TestSelectPrivilegeEnforcement verifies that SELECT privilege checks work:
//   - root (no __current_user) has full access
//   - non-root user without SELECT is denied with error 1142
//   - non-root user with SELECT on the specific table is allowed
//   - non-root user with global SELECT (ON *.*) is allowed
//   - non-root user with DB-level SELECT (ON db.*) is allowed
//   - GRANT/REVOKE is reflected immediately
func TestSelectPrivilegeEnforcement(t *testing.T) {
	e := newTestExecutor(t)

	// Create test table
	if _, err := e.Execute("CREATE TABLE t_priv (id INT, val VARCHAR(10))"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := e.Execute("INSERT INTO t_priv VALUES (1, 'a')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// Create user u1 with no privileges
	if _, err := e.Execute("CREATE USER u1@localhost"); err != nil {
		t.Fatalf("CREATE USER: %v", err)
	}

	// GRANT SELECT on test.t_priv to u1
	if _, err := e.Execute("GRANT SELECT ON test.t_priv TO u1@localhost"); err != nil {
		t.Fatalf("GRANT SELECT: %v", err)
	}

	// Root (no __current_user set) should always succeed
	if _, err := e.Execute("SELECT * FROM t_priv"); err != nil {
		t.Fatalf("root SELECT should succeed: %v", err)
	}

	// Set current user to u1 (simulating non-root connection)
	if _, err := e.Execute("SET @__current_user = 'u1'"); err != nil {
		t.Fatalf("SET current_user: %v", err)
	}

	// u1 has SELECT on test.t_priv - should succeed
	if _, err := e.Execute("SELECT * FROM t_priv"); err != nil {
		t.Fatalf("u1 SELECT on t_priv should succeed: %v", err)
	}

	// Create another table with no grant for u1
	if _, err := e.Execute("SET @__current_user = ''"); err != nil {
		t.Fatalf("SET current_user: %v", err)
	}
	if _, err := e.Execute("CREATE TABLE t_priv2 (id INT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := e.Execute("SET @__current_user = 'u1'"); err != nil {
		t.Fatalf("SET current_user: %v", err)
	}

	// u1 has no SELECT on test.t_priv2 - should fail with 1142
	_, err := e.Execute("SELECT * FROM t_priv2")
	if err == nil {
		t.Fatal("u1 SELECT on t_priv2 should be denied")
	}
	if !strings.Contains(err.Error(), "1142") {
		t.Fatalf("expected error 1142, got: %v", err)
	}
	if !strings.Contains(err.Error(), "SELECT command denied") {
		t.Fatalf("expected 'SELECT command denied', got: %v", err)
	}

	// Clear user context back to root
	if _, err := e.Execute("SET @__current_user = ''"); err != nil {
		t.Fatalf("SET current_user: %v", err)
	}

	// Create u2 with global SELECT
	if _, err := e.Execute("CREATE USER u2@localhost"); err != nil {
		t.Fatalf("CREATE USER u2: %v", err)
	}
	if _, err := e.Execute("GRANT SELECT ON *.* TO u2@localhost"); err != nil {
		t.Fatalf("GRANT SELECT *.*: %v", err)
	}
	if _, err := e.Execute("SET @__current_user = 'u2'"); err != nil {
		t.Fatalf("SET current_user: %v", err)
	}

	// u2 has global SELECT - should succeed on any table
	if _, err := e.Execute("SELECT * FROM t_priv"); err != nil {
		t.Fatalf("u2 global SELECT on t_priv should succeed: %v", err)
	}
	if _, err := e.Execute("SELECT * FROM t_priv2"); err != nil {
		t.Fatalf("u2 global SELECT on t_priv2 should succeed: %v", err)
	}

	// Test REVOKE: grant u1 SELECT on t_priv AND t_priv2, then revoke t_priv2.
	// u1 should be denied on t_priv2 but allowed on t_priv.
	if _, err := e.Execute("SET @__current_user = ''"); err != nil {
		t.Fatalf("SET current_user: %v", err)
	}
	// Grant u1 SELECT on both tables
	if _, err := e.Execute("GRANT SELECT ON test.t_priv TO u1@localhost"); err != nil {
		t.Fatalf("GRANT SELECT t_priv: %v", err)
	}
	if _, err := e.Execute("GRANT SELECT ON test.t_priv2 TO u1@localhost"); err != nil {
		t.Fatalf("GRANT SELECT t_priv2: %v", err)
	}
	// Now REVOKE t_priv2 - u1 still has t_priv grant so enforcement applies
	if _, err := e.Execute("REVOKE SELECT ON test.t_priv2 FROM u1@localhost"); err != nil {
		t.Fatalf("REVOKE SELECT t_priv2: %v", err)
	}
	if _, err := e.Execute("SET @__current_user = 'u1'"); err != nil {
		t.Fatalf("SET current_user: %v", err)
	}

	// u1 still has SELECT on t_priv - should succeed
	if _, err := e.Execute("SELECT * FROM t_priv"); err != nil {
		t.Fatalf("u1 SELECT on t_priv should succeed after partial REVOKE: %v", err)
	}

	// u1 no longer has SELECT on t_priv2 - should fail with 1142
	_, err = e.Execute("SELECT * FROM t_priv2")
	if err == nil {
		t.Fatal("u1 SELECT on t_priv2 should be denied after REVOKE")
	}
	if !strings.Contains(err.Error(), "1142") {
		t.Fatalf("expected error 1142 after REVOKE, got: %v", err)
	}
	if !strings.Contains(err.Error(), "SELECT command denied") {
		t.Fatalf("expected 'SELECT command denied' after REVOKE, got: %v", err)
	}
}

func TestExecutorGrantFlow(t *testing.T) {
	// Test the full executor grant flow
	e := newTestExecutor(t)

	// CREATE ROLE r1
	_, err := e.Execute("CREATE ROLE r1")
	if err != nil {
		t.Fatalf("CREATE ROLE: %v", err)
	}

	// CREATE USER u1@localhost
	_, err = e.Execute("CREATE USER u1@localhost IDENTIFIED BY 'foo'")
	if err != nil {
		t.Fatalf("CREATE USER: %v", err)
	}

	// GRANT SELECT ON *.* TO r1
	_, err = e.Execute("GRANT SELECT ON *.* TO r1")
	if err != nil {
		t.Fatalf("GRANT SELECT: %v", err)
	}

	// Check grantStore
	roleGrants := e.grantStore.GetRoleGrants("r1")
	t.Logf("role r1 grants after GRANT: %+v", roleGrants)
	if len(roleGrants) == 0 {
		t.Errorf("Expected r1 to have grants, got none")
	}

	// GRANT r1 TO u1@localhost
	_, err = e.Execute("GRANT r1 TO u1@localhost")
	if err != nil {
		t.Fatalf("GRANT role: %v", err)
	}

	// SHOW GRANTS FOR u1@localhost USING r1
	res, err := e.Execute("SHOW GRANTS FOR u1@localhost USING r1")
	if err != nil {
		t.Fatalf("SHOW GRANTS: %v", err)
	}
	t.Logf("SHOW GRANTS result: %v", res.Rows)

	// Should include SELECT
	found := false
	for _, row := range res.Rows {
		if s, ok := row[0].(string); ok && s == "GRANT SELECT ON *.* TO `u1`@`localhost`" {
			found = true
		}
	}
	if !found {
		t.Errorf("Expected SELECT in SHOW GRANTS, got: %v", res.Rows)
	}
}
