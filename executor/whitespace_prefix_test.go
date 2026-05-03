package executor

import (
	"strings"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

func TestCollapseUpperWhitespace(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"ALTER USER foo", "ALTER USER foo"},
		{"ALTER  USER foo", "ALTER USER foo"},
		{"ALTER\tUSER foo", "ALTER USER foo"},
		{"ALTER\nUSER foo", "ALTER USER foo"},
		{"ALTER \t\nUSER foo", "ALTER USER foo"},
		{"FLUSH PRIVILEGES", "FLUSH PRIVILEGES"},
		{"", ""},
	}
	for _, tc := range tests {
		got := collapseUpperWhitespace(tc.in)
		if got != tc.want {
			t.Errorf("collapseUpperWhitespace(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestAlterUserDoubleSpace verifies that ALTER USER with multiple internal
// spaces (which Vitess parser doesn't accept) is still recognized by the
// fallback prefix-match path and treated as a no-op.
func TestAlterUserDoubleSpace(t *testing.T) {
	cat := catalog.New()
	stor := storage.NewEngine()
	exec := New(cat, stor)

	// First create the user (this should work without issue).
	if _, err := exec.Execute("CREATE USER test_user_dbl IDENTIFIED BY 'pwd'"); err != nil {
		t.Fatalf("CREATE USER failed: %v", err)
	}

	// Single-space variant should succeed (baseline).
	if _, err := exec.Execute("ALTER USER test_user_dbl IDENTIFIED BY 'newpwd'"); err != nil {
		t.Fatalf("ALTER USER (single space) failed: %v", err)
	}

	// Double-space variant should also succeed via the fallback prefix-match.
	if _, err := exec.Execute("ALTER  USER test_user_dbl IDENTIFIED BY 'newerpwd'"); err != nil {
		t.Fatalf("ALTER  USER (double space) failed: %v", err)
	}

	// Tab and newline variants should also succeed.
	if _, err := exec.Execute("ALTER\tUSER test_user_dbl IDENTIFIED BY 'tabpwd'"); err != nil {
		t.Fatalf("ALTER\\tUSER failed: %v", err)
	}
	if _, err := exec.Execute("ALTER\nUSER test_user_dbl IDENTIFIED BY 'nlpwd'"); err != nil {
		t.Fatalf("ALTER\\nUSER failed: %v", err)
	}
}

// TestCollapsePerformance verifies the fast path works (no allocation when
// the input has no extra whitespace runs).
func TestCollapseUpperWhitespaceFastPath(t *testing.T) {
	// Strings with no double-space and no tab/newline should not allocate
	// (returned unchanged). This is checked by reference equality on the
	// underlying data via strings.Builder behavior; we approximate by length.
	in := "SELECT * FROM t WHERE a = 1"
	out := collapseUpperWhitespace(in)
	if out != in || len(out) != len(in) {
		t.Errorf("expected fast-path passthrough; got %q -> %q", in, out)
	}
	// Sanity: no leading/trailing spaces collapsed (caller's responsibility).
	if strings.HasPrefix(out, " ") || strings.HasSuffix(out, " ") {
		t.Errorf("unexpected leading/trailing whitespace in %q", out)
	}
}
