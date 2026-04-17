package mtrrunner

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/myuon/mylite/executor"
)

// testRecord mirrors the per-test entry in the JSON result log.
type testRecord struct {
	Suite  string `json:"suite"`
	Name   string `json:"name"`
	Status string `json:"status"` // pass | fail | error | skip | timeout
}

// resultLog mirrors the top-level structure of result JSON files.
type resultLog struct {
	Tests []testRecord `json:"tests"`
}

// featureTestPatterns maps a Feature.Name to a list of substrings that,
// when found in "suite/testname" (lower-cased), indicate the test exercises
// that feature.  The match is substring-based (strings.Contains).
var featureTestPatterns = map[string][]string{
	"ENGINE=MyISAM":      {"_myisam", "myisam_", "force_myisam"},
	"ENGINE=MEMORY":      {"_memory", "_heap", "heap_"},
	"ENGINE=FEDERATED":   {"federated"},
	"ENGINE=NDB":         {"ndb", "gcol_ndb"},
	"ENGINE=BLACKHOLE":   {"blackhole"},
	"ENGINE=CSV":         {"csv"},
	"ENGINE=ARCHIVE":     {"archive"},
	"HANDLER":            {"handler_", "/handler"},
	"FULLTEXT Index":     {"fulltext", "innodb_fts/"},
	"Spatial Index":      {"spatial", "gis", "innodb_gis"},
	"PARTITION BY":       {"partition", "/parts/", "max_parts"},
	"CREATE VIEW":        {"view"},
	"CREATE TRIGGER":     {"trigger"},
	"CREATE EVENT":       {"event"},
	"CREATE PROCEDURE":   {"procedure", "proc_"},
	"CREATE FUNCTION":    {"func_"},
	"Window Functions":   {"window"},
	"Window Functions (built-in)": {"window"},
	"WITH RECURSIVE":              {"with_recursive", "recursive"},
	"WITH (non-recursive CTE)":    {"with_non_recursive", "cte"},
	"GIS / Spatial Functions":     {"gis", "spatial", "innodb_gis"},
	"GEOMETRY / Spatial types":    {"gis", "spatial", "innodb_gis"},
	"EXPLAIN":                     {"explain", "opt_trace"},
	"PREPARE/EXECUTE":             {"ps_", "prepare"},
	"Binary Log":                  {"binlog", "binlog_nogtid"},
	"GTID":                        {"gtid"},
	"Roles":                       {"roles"},
	"GRANT/REVOKE":                {"grant"},
	"Collations":                  {"collat", "collisions", "charset"},
	"JSON":                        {"json"},
	"JSON Functions":              {"json"},
	"System Variables":            {"sys_vars"},
	"performance_schema":          {"perfschema"},
	"sys schema":                  {"sysschema"},
	"INFORMATION_SCHEMA":          {"information_schema"},
	"Generated Columns":           {"gcol", "gcol_ndb"},
	"Foreign Keys":                {"foreign_key", "fk_"},
	"Transactions (BEGIN/COMMIT/ROLLBACK)": {"transaction", "innodb_undo", "innodb_stress"},
	"SAVEPOINT":              {"savepoint", "sp_savepoint"},
	"LOAD DATA INFILE":       {"load_data", "loaddata"},
	"Date/Time Functions":    {"func_datetime", "datetime_", "date_"},
	"Encryption Functions":   {"encryption", "aes_"},
	"Aggregate Functions":    {"funcs_1", "funcs_2"},
	"String Functions":       {"func_string", "funcs_1", "funcs_2"},
	"Math Functions":         {"func_math", "funcs_1", "funcs_2"},
	"Date / DATETIME / TIMESTAMP": {"func_datetime", "engine_funcs"},
	"CHECK Constraints":      {"check_constraint"},
	"AUTO_INCREMENT":         {"auto_increment"},
	"SELECT FOR UPDATE":      {"innodb_stress"},
}

// featureCounts accumulates pass/fail/error/skip/timeout counts per feature.
type featureCounts struct {
	Pass    int
	Fail    int
	Error   int
	Skip    int
	Timeout int
}

func (fc featureCounts) Total() int {
	return fc.Pass + fc.Fail + fc.Error + fc.Skip + fc.Timeout
}

func (fc featureCounts) Bad() int { return fc.Fail + fc.Error }

// ValidateFeatures loads a result JSON file and prints a feature-validation report.
func ValidateFeatures(resultPath string) error {
	data, err := os.ReadFile(resultPath)
	if err != nil {
		return fmt.Errorf("cannot read result file %q: %w", resultPath, err)
	}
	var log resultLog
	if err := json.Unmarshal(data, &log); err != nil {
		return fmt.Errorf("cannot parse result file %q: %w", resultPath, err)
	}

	// Build lookup: "suite/testname" -> status
	type stat struct{ pass, fail, err, skip, timeout int }
	counts := map[string]*featureCounts{}
	for _, feat := range executor.Features {
		counts[feat.Name] = &featureCounts{}
	}

	// For each test, find matching features and tally
	for _, t := range log.Tests {
		key := strings.ToLower(t.Suite + "/" + t.Name)
		for _, feat := range executor.Features {
			patterns, ok := featureTestPatterns[feat.Name]
			if !ok {
				continue
			}
			matched := false
			for _, p := range patterns {
				if strings.Contains(key, strings.ToLower(p)) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
			fc := counts[feat.Name]
			switch t.Status {
			case "pass":
				fc.Pass++
			case "fail":
				fc.Fail++
			case "error":
				fc.Error++
			case "skip":
				fc.Skip++
			case "timeout":
				fc.Timeout++
			}
		}
	}

	// Print report
	fmt.Println("\n=== Feature Validation ===")

	// Sort features by category then name for readable output
	features := make([]executor.Feature, len(executor.Features))
	copy(features, executor.Features)
	sort.Slice(features, func(i, j int) bool {
		if features[i].Category != features[j].Category {
			return features[i].Category < features[j].Category
		}
		return features[i].Name < features[j].Name
	})

	var consistent, warnings, mismatches, noData int

	for _, feat := range features {
		fc := counts[feat.Name]
		total := fc.Total()
		runTotal := fc.Pass + fc.Fail + fc.Error + fc.Timeout // excludes skip

		if total == 0 {
			fmt.Printf("ℹ %-40s (%s): no related tests\n", feat.Name, feat.Status)
			noData++
			continue
		}

		label := fmt.Sprintf("%-40s (%s)", feat.Name, feat.Status)
		summary := fmt.Sprintf("%d/%d pass", fc.Pass, runTotal)
		if fc.Skip > 0 {
			summary += fmt.Sprintf(", %d skipped", fc.Skip)
		}

		switch feat.Status {
		case executor.Supported:
			// Expectation: high pass rate
			if runTotal == 0 {
				// All skipped
				fmt.Printf("✓ %s: all skipped — no data\n", label)
				noData++
			} else {
				passRate := float64(fc.Pass) / float64(runTotal)
				if passRate < 0.5 && fc.Bad() > 2 {
					fmt.Printf("✗ %s: %s — investigate (%.0f%% pass)\n", label, summary, passRate*100)
					mismatches++
				} else {
					fmt.Printf("✓ %s: %s — consistent\n", label, summary)
					consistent++
				}
			}

		case executor.Unsupported:
			// Expectation: few/no passes (mostly skip or fail)
			if runTotal == 0 {
				fmt.Printf("✓ %s: %s — consistent (all skipped)\n", label, summary)
				consistent++
			} else if fc.Pass > 0 && float64(fc.Pass)/float64(runTotal) > 0.5 {
				fmt.Printf("⚠ %s: %s → consider upgrading to \"partial\" or \"supported\"\n", label, summary)
				warnings++
			} else {
				fmt.Printf("✓ %s: %s — consistent\n", label, summary)
				consistent++
			}

		case executor.Partial:
			// Show pass/fail ratio; flag extreme cases
			if runTotal == 0 {
				fmt.Printf("ℹ %s: %s — no executable tests\n", label, summary)
				noData++
			} else {
				passRate := float64(fc.Pass) / float64(runTotal)
				detail := fmt.Sprintf("%s (%.0f%% pass)", summary, passRate*100)
				if passRate >= 0.9 {
					fmt.Printf("⚠ %s: %s → consider upgrading to \"supported\"\n", label, detail)
					warnings++
				} else if fc.Pass == 0 && runTotal >= 3 {
					fmt.Printf("⚠ %s: %s → consider downgrading to \"unsupported\"\n", label, detail)
					warnings++
				} else {
					fmt.Printf("~ %s: %s\n", label, detail)
					consistent++
				}
			}
		}
	}

	fmt.Println("---")
	fmt.Printf("Summary: %d consistent, %d warnings, %d mismatches, %d no-data\n",
		consistent, warnings, mismatches, noData)

	return nil
}
