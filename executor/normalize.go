package executor

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

// normalizeEngineName returns the canonical MySQL engine name for a given input.
func normalizeEngineName(name string) string {
	switch strings.ToUpper(name) {
	case "INNODB":
		return "InnoDB"
	case "MYISAM":
		return "MyISAM"
	case "MERGE", "MRG_MYISAM":
		return "MRG_MYISAM"
	case "MEMORY", "HEAP":
		return "MEMORY"
	case "CSV":
		return "CSV"
	case "ARCHIVE":
		return "ARCHIVE"
	case "BLACKHOLE":
		return "BLACKHOLE"
	case "FEDERATED":
		return "FEDERATED"
	case "NDB", "NDBCLUSTER":
		return "ndbcluster"
	case "PERFORMANCE_SCHEMA":
		return "PERFORMANCE_SCHEMA"
	default:
		return name
	}
}

// normalizeTypeAliases replaces MySQL type aliases that the vitess parser
// doesn't support with their canonical equivalents.
func normalizeTypeAliases(query string) string {
	upper := strings.ToUpper(query)
	// Only apply to CREATE TABLE (including TEMPORARY) or ALTER TABLE statements
	if !strings.Contains(upper, "CREATE TABLE") && !strings.Contains(upper, "CREATE TEMPORARY TABLE") && !strings.Contains(upper, "ALTER TABLE") {
		return query
	}
	// Replace DOUBLE PRECISION with DOUBLE (case-insensitive, word-boundary aware)
	result := replaceTypeWord(query, "DOUBLE PRECISION", "DOUBLE")
	result = replaceTypeWord(result, "DEC", "DECIMAL")
	result = replaceTypeWord(result, "FIXED", "DECIMAL")
	result = replaceTypeWord(result, "NUMERIC", "DECIMAL")
	result = replaceTypeWord(result, "SERIAL", "BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE")
	result = replaceTypeWord(result, "INT1", "TINYINT")
	result = replaceTypeWord(result, "INT2", "SMALLINT")
	result = replaceTypeWord(result, "INT3", "MEDIUMINT")
	result = replaceTypeWord(result, "INT4", "INT")
	result = replaceTypeWord(result, "INT8", "BIGINT")
	result = replaceTypeWord(result, "MIDDLEINT", "MEDIUMINT")
	result = replaceTypeWord(result, "FLOAT4", "FLOAT")
	result = replaceTypeWord(result, "FLOAT8", "DOUBLE")
	result = replaceTypeWord(result, "LONG VARBINARY", "MEDIUMBLOB")
	result = replaceTypeWord(result, "LONG VARCHAR", "MEDIUMTEXT")
	// NCHAR VARYING and NCHAR VARCHAR must come before NCHAR to avoid partial replacement.
	// These are DDL-only normalizations. We add CHARACTER SET utf8 AFTER the size specifier
	// to preserve the charset in SHOW CREATE TABLE output (NATIONAL/NCHAR types imply UTF8).
	result = regexp.MustCompile(`(?i)\bNCHAR\s+VARYING(\s*\([^)]*\))?`).ReplaceAllStringFunc(result, func(m string) string {
		re := regexp.MustCompile(`(?i)\bNCHAR\s+VARYING`)
		return re.ReplaceAllString(m, "VARCHAR") + " CHARACTER SET utf8"
	})
	result = regexp.MustCompile(`(?i)\bNCHAR\s+VARCHAR(\s*\([^)]*\))?`).ReplaceAllStringFunc(result, func(m string) string {
		re := regexp.MustCompile(`(?i)\bNCHAR\s+VARCHAR`)
		return re.ReplaceAllString(m, "VARCHAR") + " CHARACTER SET utf8"
	})
	result = regexp.MustCompile(`(?i)\bNVARCHAR(\s*\([^)]*\))?`).ReplaceAllStringFunc(result, func(m string) string {
		re := regexp.MustCompile(`(?i)\bNVARCHAR`)
		return re.ReplaceAllString(m, "VARCHAR") + " CHARACTER SET utf8"
	})
	result = regexp.MustCompile(`(?i)\bNCHAR(\s*\([^)]*\))?`).ReplaceAllStringFunc(result, func(m string) string {
		re := regexp.MustCompile(`(?i)\bNCHAR`)
		return re.ReplaceAllString(m, "CHAR") + " CHARACTER SET utf8"
	})
	return result
}

// replaceTypeWord replaces a type keyword in a SQL query case-insensitively,
// only when it appears as a whole word (not part of a larger identifier)
// and not inside a quoted string.
func replaceTypeWord(query, old, replacement string) string {
	// Use byte-level case folding to ensure upper and query have the same length.
	// strings.ToUpper can change length for multi-byte characters (e.g. ß → SS).
	upper := []byte(query)
	for i, b := range upper {
		if b >= 'a' && b <= 'z' {
			upper[i] = b - 32
		}
	}
	upperStr := string(upper)
	oldUpper := strings.ToUpper(old)
	idx := 0
	for {
		pos := strings.Index(upperStr[idx:], oldUpper)
		if pos == -1 {
			break
		}
		absPos := idx + pos
		endPos := absPos + len(old)

		// Check if we're inside a quoted string
		inQuote := false
		quoteChar := byte(0)
		for i := 0; i < absPos; i++ {
			ch := query[i]
			if !inQuote && (ch == '\'' || ch == '"') {
				inQuote = true
				quoteChar = ch
			} else if inQuote && ch == quoteChar {
				inQuote = false
			}
		}
		if inQuote {
			idx = endPos
			continue
		}

		// Check word boundaries
		if absPos > 0 {
			ch := query[absPos-1]
			if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '_' {
				idx = endPos
				continue
			}
			// Skip if preceded by '=' (e.g. ROW_FORMAT=FIXED — not a column type)
			if ch == '=' {
				idx = endPos
				continue
			}
		}
		// Skip if the preceding non-space token is a table option keyword like ROW_FORMAT
		// or COLUMN_FORMAT (e.g. "ROW_FORMAT FIXED", "COLUMN_FORMAT FIXED" — value, not a column type)
		{
			pre := strings.TrimRight(upperStr[:absPos], " \t\n\r")
			if strings.HasSuffix(pre, "ROW_FORMAT") || strings.HasSuffix(pre, "COLUMN_FORMAT") {
				idx = endPos
				continue
			}
		}
		if endPos < len(query) {
			ch := query[endPos]
			if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '_' || (ch >= '0' && ch <= '9') {
				idx = endPos
				continue
			}
		}
		query = query[:absPos] + replacement + query[endPos:]
		upper2 := []byte(query)
		for i, b := range upper2 {
			if b >= 'a' && b <= 'z' {
				upper2[i] = b - 32
			}
		}
		upperStr = string(upper2)
		idx = absPos + len(replacement)
	}
	return query
}

// normalizeInlineCheckConstraints converts column-level CHECK constraints to
// table-level constraints. The vitess parser produces a nil TableSpec when a
// column definition contains an inline CHECK (expr), but handles table-level
// CHECK constraints correctly.
// Example: CREATE TABLE t1(f1 INT CHECK (f1 < 10))
// becomes: CREATE TABLE t1(f1 INT, CHECK (f1 < 10))
func normalizeInlineCheckConstraints(query string) string {
	upper := strings.ToUpper(query)
	// Only normalize inline CHECK constraints for CREATE TABLE.
	// ALTER TABLE with ADD CONSTRAINT ... CHECK is valid Vitess syntax as-is.
	if !strings.Contains(upper, "CREATE TABLE") {
		return query
	}
	if !strings.Contains(upper, "CHECK") {
		return query
	}

	// Find inline CHECK constraints in column definitions and move them
	// to table-level constraints.
	// We scan for patterns like: ... CHECK (...) ... within column definitions.
	// An inline CHECK is preceded by a column type/options (not by a comma).
	var result []byte
	var extractedChecks []string
	i := 0
	for i < len(query) {
		// Skip string literals
		if query[i] == '\'' {
			result = append(result, query[i])
			i++
			for i < len(query) {
				result = append(result, query[i])
				if query[i] == '\'' {
					i++
					break
				}
				if query[i] == '\\' && i+1 < len(query) {
					i++
					result = append(result, query[i])
				}
				i++
			}
			continue
		}
		if query[i] == '"' {
			result = append(result, query[i])
			i++
			for i < len(query) {
				result = append(result, query[i])
				if query[i] == '"' {
					i++
					break
				}
				i++
			}
			continue
		}
		if query[i] == '`' {
			result = append(result, query[i])
			i++
			for i < len(query) {
				result = append(result, query[i])
				if query[i] == '`' {
					i++
					break
				}
				i++
			}
			continue
		}

		// Look for CONSTRAINT name CHECK or CHECK keyword
		remaining := query[i:]
		upperRemaining := strings.ToUpper(remaining)

		// Match optional "CONSTRAINT name" before CHECK
		constraintPrefix := ""
		checkStart := i
		matched := false

		if strings.HasPrefix(upperRemaining, "CONSTRAINT ") {
			// Could be "CONSTRAINT name CHECK ..."
			j := len("CONSTRAINT ")
			// Skip whitespace
			for j < len(remaining) && (remaining[j] == ' ' || remaining[j] == '\t' || remaining[j] == '\n' || remaining[j] == '\r') {
				j++
			}
			// Read name (possibly backtick-quoted)
			nameStart := j
			_ = nameStart
			if j < len(remaining) && remaining[j] == '`' {
				j++
				for j < len(remaining) && remaining[j] != '`' {
					j++
				}
				if j < len(remaining) {
					j++ // skip closing backtick
				}
			} else {
				for j < len(remaining) && remaining[j] != ' ' && remaining[j] != '\t' && remaining[j] != '\n' {
					j++
				}
			}
			nameEnd := j
			// Skip whitespace
			for j < len(remaining) && (remaining[j] == ' ' || remaining[j] == '\t' || remaining[j] == '\n' || remaining[j] == '\r') {
				j++
			}
			if j+5 <= len(remaining) && strings.EqualFold(remaining[j:j+5], "CHECK") &&
				(j+5 == len(remaining) || remaining[j+5] == ' ' || remaining[j+5] == '(' || remaining[j+5] == '\t') {
				constraintPrefix = remaining[:nameEnd]
				checkStart = i + j
				matched = true
			}
		}

		if !matched && strings.HasPrefix(upperRemaining, "CHECK") &&
			(len(upperRemaining) == 5 || upperRemaining[5] == ' ' || upperRemaining[5] == '(' || upperRemaining[5] == '\t') {
			matched = true
			checkStart = i
		}

		if matched {
			// Determine if this CHECK is inline (part of column def) or already at table level.
			// Table-level CHECK is preceded by a comma (possibly with whitespace).
			// Inline CHECK is preceded by column type/options (letter, digit, paren).
			// When a CONSTRAINT prefix is present, we must check the character before
			// CONSTRAINT (not before CHECK), since CHECK follows the constraint name.
			scanFrom := checkStart
			if constraintPrefix != "" {
				scanFrom = i // i points to the start of CONSTRAINT keyword
			}
			prevNonSpace := 0
			prevNonSpacePos := -1
			for p := scanFrom - 1; p >= 0; p-- {
				ch := query[p]
				if ch != ' ' && ch != '\t' && ch != '\n' && ch != '\r' {
					prevNonSpace = int(ch)
					prevNonSpacePos = p
					break
				}
			}
			// If the prevNonSpace is a letter, it might be the end of the word
			// "CONSTRAINT". In that case we should look before CONSTRAINT to determine
			// if the check is at table level (preceded by ',' or '(').
			if prevNonSpace > 0 && ((prevNonSpace >= 'a' && prevNonSpace <= 'z') || (prevNonSpace >= 'A' && prevNonSpace <= 'Z')) && prevNonSpacePos >= 0 {
				// Find the start of this word
				wordEnd := prevNonSpacePos
				wordStart := wordEnd
				for wordStart > 0 {
					ch := query[wordStart-1]
					if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' {
						wordStart--
					} else {
						break
					}
				}
				word := strings.ToUpper(query[wordStart : wordEnd+1])
				if word == "CONSTRAINT" {
					// Look before CONSTRAINT
					for p := wordStart - 1; p >= 0; p-- {
						ch := query[p]
						if ch != ' ' && ch != '\t' && ch != '\n' && ch != '\r' {
							prevNonSpace = int(ch)
							break
						}
					}
				}
			}
			isTableLevel := prevNonSpace == ',' || prevNonSpace == '('
			if isTableLevel {
				// This is a table-level CHECK constraint. Skip past the entire
				// CHECK (...) clause (and optional ENFORCED/NOT ENFORCED) so we
				// don't re-encounter the CHECK keyword on subsequent iterations.
				j := checkStart + 5 // past "CHECK"
				for j < len(query) && query[j] != '(' {
					j++
				}
				if j < len(query) {
					depth := 0
					for j < len(query) {
						if query[j] == '(' {
							depth++
						} else if query[j] == ')' {
							depth--
							if depth == 0 {
								j++
								break
							}
						} else if query[j] == '\'' {
							j++
							for j < len(query) && query[j] != '\'' {
								if query[j] == '\\' {
									j++
								}
								j++
							}
						}
						j++
					}
					// Also skip optional NOT ENFORCED / ENFORCED
					rest := strings.TrimLeft(query[j:], " \t\n\r")
					upperRest := strings.ToUpper(rest)
					if strings.HasPrefix(upperRest, "NOT ENFORCED") {
						j += (len(query[j:]) - len(rest)) + len("NOT ENFORCED")
					} else if strings.HasPrefix(upperRest, "ENFORCED") {
						j += (len(query[j:]) - len(rest)) + len("ENFORCED")
					}
					// Copy the entire table-level CHECK clause as-is
					result = append(result, query[i:j]...)
					i = j
					continue
				}
			} else {
				// This is an inline CHECK. Extract the full CHECK clause.
				// Find the opening '(' after CHECK — must be only whitespace between
				// CHECK and '('. If other characters appear first, this is invalid
				// CHECK syntax (e.g. "CHECK something (...)") that MySQL rejects;
				// skip normalization so the executor can return a parse error.
				j := checkStart + 5 // past "CHECK"
				for j < len(query) && (query[j] == ' ' || query[j] == '\t' || query[j] == '\n' || query[j] == '\r') {
					j++
				}
				if j >= len(query) || query[j] != '(' {
					// Not a valid inline CHECK — pass through without normalizing
					result = append(result, query[i])
					i++
					continue
				}
				if j < len(query) {
					// Find matching closing ')'
					depth := 0
					start := j
					for j < len(query) {
						if query[j] == '(' {
							depth++
						} else if query[j] == ')' {
							depth--
							if depth == 0 {
								j++
								break
							}
						} else if query[j] == '\'' {
							j++
							for j < len(query) && query[j] != '\'' {
								if query[j] == '\\' {
									j++
								}
								j++
							}
						}
						j++
					}
					checkExpr := query[start:j]
					// Also consume optional NOT ENFORCED / ENFORCED after the check
					rest := strings.TrimLeft(query[j:], " \t\n\r")
					upperRest := strings.ToUpper(rest)
					suffix := ""
					consumed := j
					if strings.HasPrefix(upperRest, "NOT ENFORCED") {
						suffix = " NOT ENFORCED"
						consumed = j + (len(query[j:]) - len(rest)) + len("NOT ENFORCED")
					} else if strings.HasPrefix(upperRest, "ENFORCED") {
						suffix = " ENFORCED"
						consumed = j + (len(query[j:]) - len(rest)) + len("ENFORCED")
					}
					// Build the table-level constraint
					checkClause := "CHECK " + checkExpr + suffix
					if constraintPrefix != "" {
						checkClause = constraintPrefix + " " + checkClause
					}
					extractedChecks = append(extractedChecks, checkClause)
					// Remove from current position, trim trailing whitespace
					// Also remove the CONSTRAINT prefix if present
					removeFrom := i
					if constraintPrefix != "" {
						removeFrom = i
					}
					// Strip whitespace before the removed CHECK
					for removeFrom > 0 && (query[removeFrom-1] == ' ' || query[removeFrom-1] == '\t') {
						removeFrom--
					}
					result = result[:len(result)-(i-removeFrom)]
					i = consumed
					continue
				}
			}
		}

		result = append(result, query[i])
		i++
	}

	if len(extractedChecks) == 0 {
		return query
	}

	// Insert extracted checks before the closing ')' of the column definition list.
	// Find the last ')' that closes the column definitions.
	out := string(result)
	// Find the position of the closing paren of column list.
	// It's the last ')' before table options or end of statement.
	lastParen := strings.LastIndex(out, ")")
	if lastParen >= 0 {
		// Insert checks as table-level constraints
		insert := ""
		for _, c := range extractedChecks {
			insert += ",\n" + c
		}
		out = out[:lastParen] + insert + out[lastParen:]
	}

	return out
}

// normalizeStorageClause strips "STORAGE DISK" and "STORAGE MEMORY" from
// column definitions. The vitess parser does not recognise these MySQL column
// attributes and silently produces a nil TableSpec for CREATE TABLE statements
// that contain them. Since mylite is an in-memory engine the storage attribute
// is irrelevant.
func normalizeStorageClause(query string) string {
	upper := strings.ToUpper(query)
	if !strings.Contains(upper, "STORAGE DISK") && !strings.Contains(upper, "STORAGE MEMORY") {
		return query
	}
	re := regexp.MustCompile(`(?i)\bSTORAGE\s+(DISK|MEMORY)\b`)
	return re.ReplaceAllString(query, "")
}

// normalizeStatsSamplePages strips STATS_SAMPLE_PAGES=default from table
// options. The vitess parser cannot handle "default" as a table option value.
// For numeric values, only strip valid ones (1-65535) so that invalid values
// still trigger parse errors.
func normalizeStatsSamplePages(query string) string {
	re := regexp.MustCompile(`(?i)\bSTATS_SAMPLE_PAGES\s*=\s*default\b`)
	return re.ReplaceAllString(query, "")
}

// normalizeStartTransaction strips "START TRANSACTION" from CREATE TABLE
// statements. This is an atomic DDL option in MySQL 8.0 that the vitess
// parser cannot handle. It's safe to ignore since mylite doesn't support
// crash recovery.
func normalizeStartTransaction(query string) string {
	upper := strings.ToUpper(query)
	if !strings.Contains(upper, "CREATE TABLE") || !strings.Contains(upper, "START TRANSACTION") {
		return query
	}
	re := regexp.MustCompile(`(?i)\bSTART\s+TRANSACTION\b`)
	return re.ReplaceAllString(query, "")
}

// normalizeAutoextendSize converts AUTOEXTEND_SIZE values with size suffixes
// (K, M, G, T) to their byte equivalents, since the vitess parser cannot
// handle the suffix notation. E.g., AUTOEXTEND_SIZE=64M -> AUTOEXTEND_SIZE=67108864.
func normalizeAutoextendSize(query string) string {
	upper := strings.ToUpper(query)
	if !strings.Contains(upper, "AUTOEXTEND_SIZE") {
		return query
	}
	re := regexp.MustCompile(`(?i)\bAUTOEXTEND_SIZE\s*=\s*(\d+)([KMGT])\b`)
	return re.ReplaceAllStringFunc(query, func(match string) string {
		m := re.FindStringSubmatch(match)
		if len(m) < 3 {
			return match
		}
		val, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil {
			return match
		}
		switch strings.ToUpper(m[2]) {
		case "K":
			val *= 1024
		case "M":
			val *= 1024 * 1024
		case "G":
			val *= 1024 * 1024 * 1024
		case "T":
			val *= 1024 * 1024 * 1024 * 1024
		}
		return fmt.Sprintf("AUTOEXTEND_SIZE=%d", val)
	})
}

// normalizeSecondaryEngine strips SECONDARY_ENGINE=value from CREATE/ALTER TABLE
// statements. The vitess parser produces a nil TableSpec when this option is present.
// Since mylite doesn't support secondary engines, we simply ignore it.
func normalizeSecondaryEngine(query string) string {
	upper := strings.ToUpper(query)
	if !strings.Contains(upper, "SECONDARY_ENGINE") {
		return query
	}
	// Strip SECONDARY_ENGINE=value (value may be quoted or unquoted)
	re := regexp.MustCompile(`(?i)\bSECONDARY_ENGINE\s*=\s*(?:'[^']*'|"[^"]*"|` + "`[^`]*`" + `|\S+)`)
	return re.ReplaceAllString(query, "")
}

// normalizeAddIndexUsing rewrites "ADD KEY USING BTREE (" and similar forms
// to "ADD KEY (" since the vitess parser does not handle USING before column list.
func normalizeAddIndexUsing(query string) string {
	upper := strings.ToUpper(query)
	if !strings.Contains(upper, "ALTER TABLE") {
		return query
	}
	// Move USING before column list to after it (parser can handle it after)
	// Pattern: ADD KEY [name] USING BTREE (col) -> ADD KEY [name] (col) USING BTREE
	// Without name:
	re1 := regexp.MustCompile(`(?i)(ADD\s+(UNIQUE\s+)?(KEY|INDEX))\s+USING\s+(\w+)\s*(\([^)]*\))`)
	query = re1.ReplaceAllString(query, "${1} ${5} USING ${4}")
	// With name: ADD KEY i1 USING BTREE (col) -> ADD KEY i1 (col) USING BTREE
	re2 := regexp.MustCompile(`(?i)(ADD\s+(UNIQUE\s+)?(KEY|INDEX)\s+` + "`?" + `\w+` + "`?" + `)\s+USING\s+(\w+)\s*(\([^)]*\))`)
	query = re2.ReplaceAllString(query, "${1} ${5} USING ${4}")
	return query
}

// isIdentChar returns true if the byte could be part of a SQL identifier.
func isIdentChar(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_' || b == '$'
}

// normalizeCreateTableParenSelect rewrites CREATE TABLE t (SELECT ...) ORDER BY ...
// into CREATE TABLE t SELECT ... ORDER BY ... so the vitess parser can handle it.
// When an outer ORDER BY is present, the inner ORDER BY is dropped since the
// outer one takes precedence in MySQL.
func normalizeCreateTableParenSelect(query string) string {
	upper := strings.ToUpper(strings.TrimSpace(query))
	if !strings.HasPrefix(upper, "CREATE TABLE") && !strings.HasPrefix(upper, "CREATE TEMPORARY TABLE") {
		return query
	}
	// Skip normalization if WITH clause (CTE) is present to avoid mistaking CTE subqueries
	// for the parenthesized SELECT pattern. A CTE WITH clause has the form "WITH name AS (".
	if regexp.MustCompile(`(?i)\bWITH\s+\w+\s+AS\s*\(`).MatchString(query) {
		return query
	}
	trimmed := strings.TrimSpace(query)
	for i := 0; i < len(trimmed); i++ {
		if trimmed[i] == '\'' {
			for i++; i < len(trimmed) && trimmed[i] != '\''; i++ {
				if trimmed[i] == '\\' {
					i++
				}
			}
			continue
		}
		if trimmed[i] != '(' {
			continue
		}
		j := i + 1
		for j < len(trimmed) && (trimmed[j] == ' ' || trimmed[j] == '\t' || trimmed[j] == '\n') {
			j++
		}
		if j+6 >= len(trimmed) || !strings.EqualFold(trimmed[j:j+7], "SELECT ") {
			break
		}
		depth := 1
		k := i + 1
		for k < len(trimmed) && depth > 0 {
			switch trimmed[k] {
			case '(':
				depth++
			case ')':
				depth--
			case '\'':
				for k++; k < len(trimmed) && trimmed[k] != '\''; k++ {
					if trimmed[k] == '\\' {
						k++
					}
				}
			}
			k++
		}
		if depth != 0 {
			return query
		}
		innerSelect := strings.TrimSpace(trimmed[i+1 : k-1])
		prefix := trimmed[:i]
		suffix := strings.TrimSpace(trimmed[k:])

		// If there is an outer ORDER BY, strip the inner ORDER BY
		if len(suffix) > 0 && strings.HasPrefix(strings.ToUpper(suffix), "ORDER BY") {
			upperInner := strings.ToUpper(innerSelect)
			lastOB := -1
			d := 0
			for m := 0; m < len(innerSelect)-7; m++ {
				switch innerSelect[m] {
				case '(':
					d++
				case ')':
					d--
				case '\'':
					for m++; m < len(innerSelect) && innerSelect[m] != '\''; m++ {
						if innerSelect[m] == '\\' {
							m++
						}
					}
				}
				if d == 0 && strings.HasPrefix(upperInner[m:], "ORDER BY") {
					lastOB = m
				}
			}
			if lastOB > 0 {
				innerSelect = strings.TrimSpace(innerSelect[:lastOB])
			}
		}

		result := strings.TrimSpace(prefix) + " " + innerSelect
		if len(suffix) > 0 {
			result += " " + suffix
		}
		return result
	}
	return query
}

// normalizeCreateTableEngineSelect strips table options (ENGINE=, CHARSET=, etc.)
// between the table name and SELECT clause in CREATE TABLE statements so the
// vitess parser can handle them. The engine is ignored since mylite uses its
// own storage engine for all tables.
func normalizeCreateTableEngineSelect(query string) string {
	upper := strings.ToUpper(query)
	if !strings.HasPrefix(upper, "CREATE TABLE") && !strings.HasPrefix(upper, "CREATE TEMPORARY TABLE") {
		return query
	}
	// Find SELECT keyword (not inside parentheses)
	depth := 0
	selectIdx := -1
	for i := 0; i < len(query)-6; i++ {
		switch query[i] {
		case '(':
			depth++
		case ')':
			depth--
		case '\'':
			// Skip string literals
			for i++; i < len(query) && query[i] != '\''; i++ {
				if query[i] == '\\' {
					i++
				}
			}
		}
		if depth == 0 && (query[i] == 's' || query[i] == 'S') {
			// SELECT can be followed by space, newline, or tab as whitespace separator.
			if i+6 <= len(query) && strings.EqualFold(query[i:i+6], "SELECT") &&
				(i+6 == len(query) || query[i+6] == ' ' || query[i+6] == '\n' || query[i+6] == '\t' || query[i+6] == '\r') {
				// Ensure SELECT is a standalone keyword (preceded by space/newline, not part of identifier)
				if i == 0 || !isIdentChar(query[i-1]) {
					selectIdx = i
					break
				}
			}
		}
	}
	if selectIdx < 0 {
		return query
	}
	// Check if there are table options between the table name and SELECT
	// CREATE [TEMPORARY] TABLE name [options] SELECT ...
	// or CREATE [TEMPORARY] TABLE name (col defs) [options] SELECT ...
	prefix := query[:selectIdx]
	// Find the position after the column definitions closing paren (depth 0).
	// Table-level options only appear after the closing ')' of the column list.
	// We must not strip CHARACTER SET / CHARSET inside the column definitions.
	colDefsEnd := -1
	{
		d := 0
		inStr := false
		for i := 0; i < len(prefix); i++ {
			if inStr {
				if prefix[i] == '\'' {
					if i+1 < len(prefix) && prefix[i+1] == '\'' {
						i++ // skip escaped quote
					} else {
						inStr = false
					}
				} else if prefix[i] == '\\' {
					i++
				}
				continue
			}
			switch prefix[i] {
			case '\'':
				inStr = true
			case '(':
				d++
			case ')':
				d--
				if d == 0 {
					colDefsEnd = i
				}
			}
		}
	}
	// Only strip table-level options from the part after the closing paren.
	// If there's no paren (rare: CREATE TABLE name ENGINE=x SELECT ...), strip from whole prefix.
	colDefsPart := prefix
	tableOptsPart := ""
	if colDefsEnd >= 0 && colDefsEnd < len(prefix)-1 {
		colDefsPart = prefix[:colDefsEnd+1]
		tableOptsPart = prefix[colDefsEnd+1:]
	} else if colDefsEnd < 0 {
		// No parentheses - all of prefix is table options (CREATE TABLE name ENGINE=x SELECT)
		colDefsPart = ""
		tableOptsPart = prefix
	}
	// Strip known table options from the table-options portion only.
	// Match both "OPTION=value" and "OPTION value" (without equals sign).
	reOpts := regexp.MustCompile(`(?i)\b(?:ENGINE|TYPE|ROW_FORMAT|KEY_BLOCK_SIZE|AVG_ROW_LENGTH|MIN_ROWS|MAX_ROWS|PACK_KEYS|CHECKSUM|DELAY_KEY_WRITE|DATA\s+DIRECTORY|INDEX\s+DIRECTORY|AUTO_INCREMENT|INSERT_METHOD|STATS_AUTO_RECALC|STATS_PERSISTENT|STATS_SAMPLE_PAGES)\s*=?\s*\S+`)
	cleaned := reOpts.ReplaceAllString(tableOptsPart, " ")
	reCharset := regexp.MustCompile(`(?i)\bDEFAULT\s+(?:CHARSET|CHARACTER\s+SET)\s*=?\s*\S+`)
	cleaned = reCharset.ReplaceAllString(cleaned, " ")
	reCharset2 := regexp.MustCompile(`(?i)\b(?:CHARSET|CHARACTER\s+SET)\s*=?\s*\S+`)
	cleaned = reCharset2.ReplaceAllString(cleaned, " ")
	reCollate := regexp.MustCompile(`(?i)\bCOLLATE\s*=?\s*\S+`)
	cleaned = reCollate.ReplaceAllString(cleaned, " ")
	reComment := regexp.MustCompile(`(?i)\bCOMMENT\s*=?\s*'[^']*'`)
	cleaned = reComment.ReplaceAllString(cleaned, " ")
	// Collapse multiple spaces in table opts part
	cleaned = regexp.MustCompile(`\s+`).ReplaceAllString(cleaned, " ")
	fullPrefix := colDefsPart + cleaned
	fullPrefix = regexp.MustCompile(`\s+`).ReplaceAllString(fullPrefix, " ")
	return strings.TrimSpace(fullPrefix) + " " + query[selectIdx:]
}

// normalizeEngineWithoutEquals rewrites "ENGINE <value>" to "ENGINE=<value>"
// in CREATE TABLE / ALTER TABLE statements. MySQL allows both forms but the
// vitess parser requires the equals sign.
func normalizeEngineWithoutEquals(query string) string {
	upper := strings.ToUpper(query)
	if !strings.Contains(upper, "ENGINE") {
		return query
	}
	if !strings.HasPrefix(upper, "CREATE ") && !strings.HasPrefix(upper, "ALTER ") {
		return query
	}
	re := regexp.MustCompile(`(?i)\bENGINE\s+(InnoDB|MyISAM|MEMORY|HEAP|ARCHIVE|CSV|BLACKHOLE|NDB|MERGE|FEDERATED|EXAMPLE)\b`)
	return re.ReplaceAllString(query, "ENGINE=$1")
}

// normalizeCreateTableIndexUsing rewrites "PRIMARY KEY USING BTREE (cols)" and
// "KEY name USING BTREE (cols)" in CREATE TABLE to move USING after the column list
// so the vitess parser can handle it.
func normalizeCreateTableIndexUsing(query string) string {
	upper := strings.ToUpper(query)
	if !strings.Contains(upper, " USING ") {
		return query
	}
	if !strings.Contains(upper, "CREATE TABLE") && !strings.Contains(upper, "CREATE TEMPORARY TABLE") {
		return query
	}
	// Strip "USING BTREE/HASH" that appears immediately before a column list "(".
	// This handles cases like "UNIQUE KEY name USING HASH (col(N))" where the
	// column list contains nested parentheses that confuse the [^)]* approach.
	// Vitess can parse the index type when it appears after the column list, but
	// since mylite ignores index types entirely, we simply strip them when they
	// appear before the column list.
	reUsingBeforeParen := regexp.MustCompile(`(?i)\bUSING\s+(?:BTREE|HASH)\s+\(`)
	if reUsingBeforeParen.MatchString(query) {
		query = reUsingBeforeParen.ReplaceAllStringFunc(query, func(_ string) string {
			return "("
		})
		return query
	}
	// PRIMARY KEY USING BTREE/HASH (cols) -> PRIMARY KEY (cols) USING BTREE/HASH
	re1 := regexp.MustCompile(`(?i)(PRIMARY\s+KEY)\s+USING\s+(\w+)\s*(\([^)]*\))`)
	query = re1.ReplaceAllString(query, "${1} ${3} USING ${2}")
	// UNIQUE KEY [name] USING BTREE (cols) -> UNIQUE KEY [name] (cols) USING BTREE
	re2 := regexp.MustCompile("(?i)(UNIQUE\\s+(?:KEY|INDEX))\\s+USING\\s+(\\w+)\\s*(\\([^)]*\\))")
	query = re2.ReplaceAllString(query, "${1} ${3} USING ${2}")
	re3 := regexp.MustCompile("(?i)(UNIQUE\\s+(?:KEY|INDEX)\\s+`?\\w+`?)\\s+USING\\s+(\\w+)\\s*(\\([^)]*\\))")
	query = re3.ReplaceAllString(query, "${1} ${3} USING ${2}")
	// KEY/INDEX [name] USING BTREE (cols) -> KEY [name] (cols) USING BTREE
	re4 := regexp.MustCompile("(?i)((?:KEY|INDEX))\\s+USING\\s+(\\w+)\\s*(\\([^)]*\\))")
	query = re4.ReplaceAllString(query, "${1} ${3} USING ${2}")
	re5 := regexp.MustCompile("(?i)((?:KEY|INDEX)\\s+`?\\w+`?)\\s+USING\\s+(\\w+)\\s*(\\([^)]*\\))")
	query = re5.ReplaceAllString(query, "${1} ${3} USING ${2}")
	// UNIQUE name USING BTREE (cols) -> UNIQUE KEY name (cols) USING BTREE
	re6 := regexp.MustCompile("(?i)(UNIQUE)\\s+(`?\\w+`?)\\s+USING\\s+(\\w+)\\s*(\\([^)]*\\))")
	query = re6.ReplaceAllString(query, "${1} KEY ${2} ${4} USING ${3}")
	// UNIQUE USING BTREE (cols) -> UNIQUE KEY (cols) USING BTREE (no name)
	re7 := regexp.MustCompile("(?i)(UNIQUE)\\s+USING\\s+(\\w+)\\s*(\\([^)]*\\))")
	query = re7.ReplaceAllString(query, "${1} KEY ${3} USING ${2}")
	return query
}

// normalizeEnumHexValues converts hex literals in ENUM and SET column type values
// to quoted string literals so the vitess parser can handle them.
// e.g. ENUM(0x9353,0x9373) -> ENUM('0x9353','0x9373')
func normalizeEnumHexValues(query string) string {
	upper := strings.ToUpper(query)
	if !strings.Contains(upper, "ENUM") && !strings.Contains(upper, " SET") {
		return query
	}
	reHex := regexp.MustCompile(`(?i)\b(ENUM|SET)\s*\(((?:[^()]*0x[0-9a-fA-F]+[^()]*)+)\)`)
	return reHex.ReplaceAllStringFunc(query, func(match string) string {
		parenIdx := strings.IndexByte(match, '(')
		keyword := match[:parenIdx]
		valPart := match[parenIdx+1 : len(match)-1]
		parts := strings.Split(valPart, ",")
		newParts := make([]string, 0, len(parts))
		for _, p := range parts {
			trimmed := strings.TrimSpace(p)
			if strings.HasPrefix(strings.ToLower(trimmed), "0x") {
				newParts = append(newParts, "'"+trimmed+"'")
			} else {
				newParts = append(newParts, p)
			}
		}
		return keyword + "(" + strings.Join(newParts, ", ") + ")"
	})
}

// normalizeWeightString strips extra numeric args from weight_string() calls
// that vitess can't parse, e.g. weight_string(expr, 1, 2, 0xC0) -> weight_string(expr)
func normalizeWeightString(query string) string {
	uq := strings.ToUpper(query)
	wsIdx := strings.Index(uq, "WEIGHT_STRING")
	if wsIdx < 0 {
		return query
	}
	// Find the opening paren
	pIdx := strings.Index(query[wsIdx:], "(")
	if pIdx < 0 {
		return query
	}
	pIdx += wsIdx
	// Find matching closing paren, tracking depth
	depth := 1
	firstComma := -1
	for wi := pIdx + 1; wi < len(query); wi++ {
		switch query[wi] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				if firstComma > 0 {
					// Strip everything from first comma to before closing paren
					return query[:firstComma] + query[wi:]
				}
				return query
			}
		case ',':
			if depth == 1 && firstComma < 0 {
				firstComma = wi
			}
		case '\'':
			// Skip string literals
			for wi++; wi < len(query) && query[wi] != '\''; wi++ {
				if query[wi] == '\\' {
					wi++
				}
			}
		}
	}
	return query
}

// normalizeForShareOf strips "OF table_name" and "SKIP LOCKED" / "NOWAIT" from
// FOR SHARE / FOR UPDATE clauses since the vitess parser can't handle the extended syntax.
func normalizeForShareOf(query string) string {
	uq := strings.ToUpper(query)
	if !strings.Contains(uq, "FOR SHARE") && !strings.Contains(uq, "FOR UPDATE") {
		return query
	}
	// Strip "OF table1[, table2...]" after FOR SHARE/FOR UPDATE
	fsRe := regexp.MustCompile(`(?i)(FOR\s+(?:SHARE|UPDATE))\s+OF\s+[\w` + "`" + `]+(?:\s*,\s*[\w` + "`" + `]+)*`)
	query = fsRe.ReplaceAllString(query, "${1}")
	// Strip SKIP LOCKED / NOWAIT
	fsRe2 := regexp.MustCompile(`(?i)(FOR\s+(?:SHARE|UPDATE))\s+(?:SKIP\s+LOCKED|NOWAIT)`)
	query = fsRe2.ReplaceAllString(query, "${1}")
	// Strip standalone SKIP LOCKED / NOWAIT (may remain after stripping OF)
	query = regexp.MustCompile(`(?i)\s+SKIP\s+LOCKED`).ReplaceAllString(query, "")
	query = regexp.MustCompile(`(?i)\s+NOWAIT`).ReplaceAllString(query, "")
	// Strip duplicate FOR SHARE/FOR UPDATE
	fsRe3 := regexp.MustCompile(`(?i)(FOR\s+(?:SHARE|UPDATE))\s+FOR\s+(?:SHARE|UPDATE)`)
	for fsRe3.MatchString(query) {
		query = fsRe3.ReplaceAllString(query, "${1}")
	}
	return query
}

// parseSelectLockClauses extracts per-table locking info from
// "FOR SHARE OF t1 SKIP LOCKED FOR UPDATE OF t2 NOWAIT" style clauses.
// Returns nil if no "OF table" clauses are present.
func parseSelectLockClauses(query string) []selectLockClause {
	uq := strings.ToUpper(query)
	if !strings.Contains(uq, "FOR SHARE") && !strings.Contains(uq, "FOR UPDATE") {
		return nil
	}
	// Match: FOR (SHARE|UPDATE) [OF table[,table...]] [SKIP LOCKED|NOWAIT]
	re := regexp.MustCompile(`(?i)FOR\s+(SHARE|UPDATE)(?:\s+OF\s+([\w` + "`" + `]+(?:\s*,\s*[\w` + "`" + `]+)*))?(?:\s+(SKIP\s+LOCKED|NOWAIT))?`)
	matches := re.FindAllStringSubmatch(query, -1)
	if len(matches) == 0 {
		return nil
	}
	// Check if any clause has "OF table" - if none do, return nil (simple lock)
	hasOfClause := false
	for _, m := range matches {
		if m[2] != "" {
			hasOfClause = true
			break
		}
	}
	if !hasOfClause {
		return nil
	}
	var clauses []selectLockClause
	for _, m := range matches {
		lockType := strings.ToUpper(m[1])
		exclusive := lockType == "UPDATE"
		tablesStr := strings.TrimSpace(m[2])
		modifier := strings.ToUpper(strings.TrimSpace(m[3]))
		skipLocked := strings.Contains(modifier, "SKIP")
		nowait := modifier == "NOWAIT"

		if tablesStr == "" {
			// "FOR SHARE" / "FOR UPDATE" without OF - applies to all tables
			clauses = append(clauses, selectLockClause{
				tableName:  "*",
				exclusive:  exclusive,
				skipLocked: skipLocked,
				nowait:     nowait,
			})
		} else {
			// Split comma-separated table names
			tables := strings.Split(tablesStr, ",")
			for _, t := range tables {
				t = strings.TrimSpace(t)
				t = strings.Trim(t, "`")
				if t != "" {
					clauses = append(clauses, selectLockClause{
						tableName:  t,
						exclusive:  exclusive,
						skipLocked: skipLocked,
						nowait:     nowait,
					})
				}
			}
		}
	}
	return clauses
}

// normalizeMemberOperator rewrites legacy "expr MEMBER (json_doc)" to
// "expr MEMBER OF (json_doc)" for parser compatibility.
func normalizeMemberOperator(query string) string {
	re := regexp.MustCompile(`(?i)\bMEMBER\s*\(`)
	return re.ReplaceAllString(query, "MEMBER OF (")
}

// normalizeJSONTableDefaultOrder rewrites JSON_TABLE path-column clauses where
// ON ERROR appears before ON EMPTY, since the parser expects ON EMPTY first.
func normalizeJSONTableDefaultOrder(query string) string {
	re := regexp.MustCompile(`(?is)(default\s+'[^']*'\s+on\s+error)\s+(default\s+'[^']*'\s+on\s+empty)`)
	return re.ReplaceAllString(query, "${2} ${1}")
}

// quoteNonASCIIIdentifiers wraps bare words containing non-ASCII characters
// with backticks so the vitess SQL parser can handle them.
func quoteNonASCIIIdentifiers(query string) string {
	// Quick check: if no non-ASCII bytes, nothing to do.
	hasNonASCII := false
	for i := 0; i < len(query); i++ {
		if query[i] > 127 {
			hasNonASCII = true
			break
		}
	}
	if !hasNonASCII {
		return query
	}

	var buf strings.Builder
	buf.Grow(len(query) + 16)
	i := 0
	for i < len(query) {
		b := query[i]
		// Skip string literals
		if b == '\'' || b == '"' {
			quote := b
			buf.WriteByte(b)
			i++
			for i < len(query) {
				if query[i] == '\\' && i+1 < len(query) {
					buf.WriteByte(query[i])
					buf.WriteByte(query[i+1])
					i += 2
					continue
				}
				if query[i] == quote {
					buf.WriteByte(query[i])
					i++
					if i < len(query) && query[i] == quote {
						buf.WriteByte(query[i])
						i++
						continue
					}
					break
				}
				buf.WriteByte(query[i])
				i++
			}
			continue
		}
		// Skip backtick-quoted identifiers
		if b == '`' {
			buf.WriteByte(b)
			i++
			for i < len(query) && query[i] != '`' {
				buf.WriteByte(query[i])
				i++
			}
			if i < len(query) {
				buf.WriteByte(query[i])
				i++
			}
			continue
		}
		// Check for identifier-like token that contains non-ASCII
		if isIdentStart(rune(b)) || b > 127 {
			start := i
			for i < len(query) {
				r, size := utf8.DecodeRuneInString(query[i:])
				if r == utf8.RuneError && size <= 1 {
					// Invalid UTF-8 byte: include it as part of the
					// identifier (latin1 / cp1252 column names) and
					// advance past it so we don't loop forever.
					if query[i] > 127 {
						i++
						continue
					}
					break
				}
				if isIdentPart(r) || r > 127 {
					i += size
				} else {
					break
				}
			}
			word := query[start:i]
			// Only quote if it contains non-ASCII
			wordHasNonASCII := false
			for j := 0; j < len(word); j++ {
				if word[j] > 127 {
					wordHasNonASCII = true
					break
				}
			}
			if wordHasNonASCII {
				buf.WriteByte('`')
				buf.WriteString(word)
				buf.WriteByte('`')
			} else {
				buf.WriteString(word)
			}
			continue
		}
		buf.WriteByte(b)
		i++
	}
	return buf.String()
}
