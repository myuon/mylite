package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// 1. directive skip: include ファイル名 → category
var directiveIncludeCategory = map[string]string{
	// environmental (OS/build/resource)
	"windows.inc":                 "environmental",
	"not_windows.inc":             "environmental",
	"linux.inc":                   "environmental",
	"linux-version.inc":           "environmental",
	"freebsd.inc":                 "environmental",
	"have_mac_os.inc":             "environmental",
	"have_32bit.inc":              "environmental",
	"no_valgrind_without_big.inc": "environmental",
	"not_valgrind.inc":            "environmental",
	"have_innodb_16k.inc":         "environmental",
	"have_innodb_max_16k.inc":     "environmental",
	"count_sessions.inc":          "environmental",
	"have_outfile.inc":            "environmental",
	"have_thread_prio.inc":        "environmental",
	"have_not_thread_prio.inc":    "environmental",
	"have_nodebug.inc":            "environmental",
	"not_threadpool.inc":          "environmental",
	"not_as_root.inc":             "environmental",
	"no_protocol.inc":             "environmental",
	"no_ps_protocol.inc":          "environmental",

	// out_of_scope (方針外機能)
	"have_ndb.inc":                          "out_of_scope",
	"have_federated_db.inc":                 "out_of_scope",
	"force_myisam_default.inc":              "out_of_scope",
	"have_myisam.inc":                       "out_of_scope",
	"have_archive.inc":                      "out_of_scope",
	"master-slave.inc":                      "out_of_scope",
	"have_slave_repository_type_table.inc":  "out_of_scope",
	"have_only_innodb.inc":                  "out_of_scope",
	"have_log_bin.inc":                      "out_of_scope",
	"force_binlog_format_statement.inc":     "out_of_scope",

	// infra_lifecycle (server再起動・kill系)
	"restart_mysqld.inc":          "infra_lifecycle",
	"shutdown_mysqld.inc":         "infra_lifecycle",
	"kill_and_restart_mysqld.inc": "infra_lifecycle",
	"wait_until_disconnected.inc": "infra_lifecycle",

	// deferred (将来実装したい)
	"have_plugin_auth.inc":    "deferred",
	"have_plugin_interface.inc": "deferred",
	"have_plugin_server.inc":  "deferred",
	"resource_group_init.inc": "deferred",
	"have_ngram.inc":          "deferred",
	"wait_condition.inc":      "deferred",
	"wait_condition_sp.inc":   "deferred",
	"idx_explain_test.inc":    "deferred",
	"import.inc":              "deferred",
	"deadlock.inc":            "deferred",
}

// 2. skiplist skip: suite名 → category (デフォルト)
var skiplistSuiteCategory = map[string]string{
	"perfschema":  "deferred",
	"innodb":      "deferred",
	"sys_vars":    "deferred",
	"sysschema":   "deferred",
	"funcs_1":     "deferred",
	"parts":       "deferred",
	"innodb_fts":  "deferred",
	"gcol":        "deferred",
	"innodb_zip":  "deferred",
	"max_parts":   "deferred",
	"json":        "deferred",
	"innodb_undo": "deferred",
	"collations":  "deferred",
	"engine_funcs": "deferred",
	"engine_iuds": "deferred",
	"other":       "deferred",

	"auth_sec":               "out_of_scope",
	"binlog_nogtid":          "out_of_scope",
	"encryption":             "out_of_scope",
	"gcol_ndb":               "out_of_scope",
	"innodb_gis":             "out_of_scope",
	"innodb_stress":          "out_of_scope",
	"opt_trace":              "out_of_scope",
	"query_rewrite_plugins":  "out_of_scope",
	"stress":                 "out_of_scope",
	"gis":                    "out_of_scope",
	"x":                      "out_of_scope",
	"rpl":                    "out_of_scope",
	"rpl_gtid":               "out_of_scope",
	"rpl_nogtid":             "out_of_scope",
	"binlog":                 "out_of_scope",
	"binlog_gtid":            "out_of_scope",
	"group_replication":      "out_of_scope",
	"secondary_engine":       "out_of_scope",

	"large_tests": "environmental",
}

// 3. test pattern (glob) overrides for skiplist
// 例: funcs_1/myisam_* tests are out_of_scope despite suite default
var skiplistPatternOverride = []struct {
	pattern  string
	category string
}{
	{"funcs_1/myisam_*", "out_of_scope"},
	{"funcs_1/memory_*", "out_of_scope"}, // MEMORY engine
	// 他の明確な例外があれば追加
}

// Matches both "--source foo.inc" and "-- source foo.inc" (MTR allows a space after --)
var sourceDirectiveRe = regexp.MustCompile(`(?m)^\s*--\s*source\s+(\S+)`)

// Matches "--skip <message>" or bare "skip <message>" lines
var skipDirectiveRe = regexp.MustCompile(`(?im)^\s*--?\s*skip\s+(.+)`)

// infraDirectiveRe matches MTR directives that require filesystem/OS infra.
// These tests are "infra_lifecycle" even if they have no explicit --skip.
var infraDirectiveRe = regexp.MustCompile(`(?im)^\s*--\s*(exec|copy_file|move_file|force-rmdir|rmdir)\b`)

// skipMessageCategory maps known --skip message substrings to categories.
// Matched case-insensitively via strings.Contains.
var skipMessageCategory = []struct {
	substr   string
	category string
}{
	{"No plan to support sockets", "infra_lifecycle"},
	{"Need the plugin", "deferred"},
	{"need the plugin", "deferred"},
	{"plugin", "deferred"},
}

// classifySkipCategory determines a skip category from the test path and skip_reason.
// Returns "" if no rule matches.
func classifySkipCategory(suite, name, reason, suiteRoot string) string {
	switch reason {
	case "unsupported":
		return "deferred"
	case "infra":
		return "environmental"
	case "directive":
		// Scan .test file for first --source include/X.inc or --skip <message>
		path := filepath.Join(suiteRoot, suite, "t", name+".test")
		data, err := os.ReadFile(path)
		if err != nil {
			return ""
		}
		// Limit to first 32KB (covers most .test files that have --source skips deep in the file)
		head := data
		if len(head) > 32768 {
			head = head[:32768]
		}
		headStr := string(head)

		// Try --source include matches first
		sourceMatches := sourceDirectiveRe.FindAllStringSubmatch(headStr, -1)
		for _, m := range sourceMatches {
			base := filepath.Base(m[1])
			if cat, ok := directiveIncludeCategory[base]; ok {
				return cat
			}
		}

		// Try --skip message matches
		skipMatches := skipDirectiveRe.FindAllStringSubmatch(headStr, -1)
		for _, m := range skipMatches {
			msg := m[1]
			for _, entry := range skipMessageCategory {
				if strings.Contains(msg, entry.substr) {
					return entry.category
				}
			}
		}

		// Try infra directive detection (exec/copy_file/move_file etc.)
		if infraDirectiveRe.MatchString(headStr) {
			return "infra_lifecycle"
		}

		return ""
	case "skiplist":
		testPath := suite + "/" + name
		// pattern overrides first
		for _, ov := range skiplistPatternOverride {
			if matched, _ := filepath.Match(ov.pattern, testPath); matched {
				return ov.category
			}
		}
		// suite default
		if cat, ok := skiplistSuiteCategory[suite]; ok {
			return cat
		}
		return ""
	}
	return ""
}
