package executor

import (
	"strings"
	"testing"
)

func TestNormalizeStatementDigest_Basic(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"select_literal", "SELECT 1 FROM t1", "SELECT ? FROM `t1`"},
		{"select_quoted_ident", "SELECT 1 FROM `t1`", "SELECT ? FROM `t1`"},
		{"select_literal_list", "SELECT 1, 2, 3, 4 FROM t1", "SELECT ?, ... FROM `t1`"},
		{"insert_one_value", "INSERT INTO t1 VALUES (1)", "INSERT INTO `t1` VALUES (?)"},
		{"insert_multi_cols", "INSERT INTO t3 VALUES (1, 2)", "INSERT INTO `t3` VALUES (...)"},
		{"insert_multi_rows", "INSERT INTO t1 VALUES (1), (2), (3)", "INSERT INTO `t1` VALUES (?) /* , ... */"},
		{"insert_paired_rows", "INSERT INTO t3 VALUES (1, 2), (3, 4)", "INSERT INTO `t3` VALUES (...) /* , ... */"},
		{"in_list", "SELECT * FROM t1 WHERE a IN (1, 2, 3)", "SELECT * FROM `t1` WHERE `a` IN (...)"},
		{"in_list_one", "SELECT * FROM t1 WHERE a IN (1)", "SELECT * FROM `t1` WHERE `a` IN (?)"},
		{"truncate_ps_table", "TRUNCATE TABLE performance_schema.events_statements_summary_by_digest",
			"TRUNCATE TABLE `performance_schema` . `events_statements_summary_by_digest`"},
		{"whitespace_collapsed", "SELECT       1     +    1", "SELECT ? + ?"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, got := normalizeStatementDigest(tc.in)
			if got != tc.want {
				t.Errorf("normalize(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestNormalizeStatementDigest_StableHash(t *testing.T) {
	a, _ := normalizeStatementDigest("SELECT 1 FROM t1")
	b, _ := normalizeStatementDigest("SELECT   42  FROM   t1")
	if a != b {
		t.Errorf("hashes should match for queries that normalize identically, got %q vs %q", a, b)
	}
	if len(a) != 64 {
		t.Errorf("digest hex should be 64 chars (SHA-256), got %d", len(a))
	}
}

func TestNormalizeStatementDigest_UnaryOperators(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		// Unary +/- before literal at top-level SELECT (stripped)
		{"select_pos1", "select 1 from expect_unary", "SELECT ? FROM `expect_unary`"},
		{"select_pos2", "select +1 from expect_unary", "SELECT ? FROM `expect_unary`"},
		{"select_neg", "select -1 from expect_unary", "SELECT ? FROM `expect_unary`"},
		{"select_many_plus", "select ++++++1 from expect_unary", "SELECT ? FROM `expect_unary`"},
		{"select_many_minus", "select ------1 from expect_unary", "SELECT ? FROM `expect_unary`"},
		// Binary context (preceded by literal): operators kept
		{"select_binary_plus", "select 0+1 from expect_binary", "SELECT ? + ? FROM `expect_binary`"},
		{"select_binary_minus", "select 0-1 from expect_binary", "SELECT ? - ? FROM `expect_binary`"},
		// Identifier context: unary operator kept (not stripped)
		{"select_unary_ident", "select -a, +b from t", "SELECT - `a` , + `b` FROM `t`"},
		// Inside tuple: unary stripped (context is '(' or ',')
		{"insert_unary_tuple", "insert into t values (-1, +2, -3)", "INSERT INTO `t` VALUES (...)"},
		// Full unary_digest test case for expect_unchanged: -a, +b kept
		{"select_unchanged", "select a-b, a+b, -a, -b, +a, +b from expect_unchanged",
			"SELECT `a` - `b` , `a` + `b` , - `a` , - `b` , + `a` , + `b` FROM `expect_unchanged`"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, got := normalizeStatementDigest(tc.in)
			if got != tc.want {
				t.Errorf("normalize(%q)\n got  %q\nwant %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestNormalizeStatementDigest_UnaryDigestAllVariants verifies that all 5 variants
// of the unary_digest test normalize to the same DIGEST_TEXT.
func TestNormalizeStatementDigest_UnaryDigestAllVariants(t *testing.T) {
	queries := []string{
		"select 1 from expect_unary",
		"select +1 from expect_unary",
		"select -1 from expect_unary",
		"select ++++++++++++++++++++++++++++++++++++++++++++++++1 from expect_unary",
		"select ------------------------------------------------1 from expect_unary",
	}
	want := "SELECT ? FROM `expect_unary`"
	for _, q := range queries {
		_, got := normalizeStatementDigest(q)
		if got != want {
			t.Errorf("normalize(%q)\n got  %q\nwant %q", q, got, want)
		}
	}
}

func TestNormalizeStatementDigest_CommentsStripped(t *testing.T) {
	_, got := normalizeStatementDigest("SELECT 1 /* hello */ + 1")
	if strings.Contains(got, "hello") {
		t.Errorf("block comment should be stripped: %q", got)
	}
	_, got = normalizeStatementDigest("SELECT 1; # trailing\n")
	if strings.Contains(got, "trailing") {
		t.Errorf("hash comment should be stripped: %q", got)
	}
}
