package executor

import (
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
