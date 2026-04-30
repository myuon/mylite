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
