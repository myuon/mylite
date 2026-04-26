package executor

import (
	"regexp"
	"strings"
)

func matchLike(s, pattern string) bool {
	// Convert to runes for proper multibyte character handling
	sr := []rune(strings.ToLower(s))
	pr := []rune(strings.ToLower(pattern))
	return matchLikeHelper(sr, pr, 0, 0)
}

func matchLikeHelper(s, p []rune, si, pi int) bool {
	for pi < len(p) {
		if p[pi] == '\\' && pi+1 < len(p) {
			// Backslash escape: next character is treated as literal
			pi++
			if si >= len(s) || s[si] != p[pi] {
				return false
			}
			si++
			pi++
		} else if p[pi] == '%' {
			pi++
			for si <= len(s) {
				if matchLikeHelper(s, p, si, pi) {
					return true
				}
				si++
			}
			return false
		} else if p[pi] == '_' {
			if si >= len(s) {
				return false
			}
			si++
			pi++
		} else {
			if si >= len(s) || s[si] != p[pi] {
				return false
			}
			si++
			pi++
		}
	}
	return si == len(s)
}

// stripBlockComments removes /* ... */ block comments from a string,
// but only when they appear at the end of the expression (trailing comments).
// Embedded comments like 1+2/*hello*/+3 are preserved.
// Used to clean up column names where MySQL strips trailing block comments.
func stripBlockComments(s string) string {
	// Only strip if the expression ends with */
	trimmed := strings.TrimSpace(s)
	if !strings.HasSuffix(trimmed, "*/") {
		return s
	}
	// Find the start of the last trailing block comment
	// We want to find the rightmost /* such that everything after */ is whitespace
	result := trimmed
	for strings.HasSuffix(result, "*/") {
		// Find the matching /*
		end := strings.LastIndex(result, "*/")
		start := strings.LastIndex(result[:end], "/*")
		if start < 0 {
			break
		}
		// Check that what's after the */ is only whitespace (already trimmed, so end is at len-2)
		// and that what's before /* is meaningful
		candidate := strings.TrimSpace(result[:start])
		if candidate == "" {
			break
		}
		result = candidate
	}
	return result
}

// normalizeSQLDisplayName converts SQL keywords in a string to uppercase and
// normalizes operator spacing to match MySQL's column display name behavior.
func normalizeSQLDisplayName(s string) string {
	s = uppercaseSQLKeywords(s)
	// Compact operators only in nested subexpressions, while keeping top-level
	// spacing (e.g. "a = b") used by some result headers.
	s = compactOperatorsInSubexpressions(s)
	// MySQL displays function arguments without space after comma: LEFT(`c1`,0) not LEFT(`c1`, 0)
	if !strings.HasPrefix(s, "JSON_SCHEMA_VALID(") &&
		!strings.HasPrefix(s, "JSON_SCHEMA_VALIDATION_REPORT(") &&
		!strings.HasPrefix(s, "JSON_MERGE_PRESERVE(") {
		s = normalizeFuncArgSpaces(s)
	}
	// MySQL displays SUBSTRING, not SUBSTR in column headers
	if strings.HasPrefix(s, "SUBSTR(") && !strings.HasPrefix(s, "SUBSTRING(") {
		s = "SUBSTRING" + s[6:]
	}
	if strings.HasPrefix(s, "substr(") && !strings.HasPrefix(s, "substring(") {
		s = "substring" + s[6:]
	}
	// Unescape literal \n and \t in string literals back to actual newlines/tabs
	// (vitess sqlparser.String() escapes these in string literals)
	s = unescapeStringLiterals(s)
	s = normalizeSelectedFunctionArgDisplaySpacing(s)
	// MySQL displays string literal column headers without quotes:
	// SELECT 'hello' -> column name is "hello" not "'hello'"
	// But only strip if it's a simple string literal (no operators like ||).
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' && isSimpleStringLiteral(s) {
		s = s[1 : len(s)-1]
	}
	// Normalize _utf8mb3 charset introducer to _utf8 (MySQL displays _utf8, not _utf8mb3)
	// Also remove the space that sqlparser inserts between introducer and literal
	s = normalizeCharsetIntroducers(s)
	return s
}

// normalizeCharsetIntroducers normalizes charset introducers in column display names.
// The sqlparser converts _utf8 to _utf8mb3 and adds a space before the literal.
// MySQL displays these as _utf8'...' without space.
func normalizeCharsetIntroducers(s string) string {
	// Replace _utf8mb3 ' with _utf8'
	s = strings.ReplaceAll(s, "_utf8mb3 '", "_utf8'")
	s = strings.ReplaceAll(s, "_utf8mb3'", "_utf8'")
	// Also handle other common alias pairs
	s = strings.ReplaceAll(s, "_utf8mb4 '", "_utf8mb4'")
	return s
}

// stripCharsetIntroducerForColName strips charset introducers from an expression
// when used as a column name. In MySQL, _utf8'abc' and n'abc' display as 'abc' (then
// the outer quote-strip will produce 'abc'). So we strip the _charset prefix,
// leaving just the quoted string. National charset introducer n'' is also stripped.
// Examples: _utf8'abc' -> 'abc', n'abc' -> 'abc', _utf8mb4'abc' -> 'abc'
func stripCharsetIntroducerForColName(s string) string {
	// Handle national charset: n'...' or N'...'
	if len(s) >= 3 && (s[0] == 'n' || s[0] == 'N') && s[1] == '\'' && s[len(s)-1] == '\'' {
		return s[1:]
	}
	// Handle _charset'...' pattern (e.g. _utf8'abc', _latin1'hello')
	// A charset introducer has no spaces or operators between _ and the quote.
	// Expressions like "_bit | x'cafebabe'" start with _ but are not charset introducers.
	if len(s) >= 3 && s[0] == '_' {
		// Find the quote, but only if all chars between _ and ' are identifier chars
		isCharsetIntroducer := false
		quotePos := -1
		for i := 1; i < len(s); i++ {
			ch := s[i]
			if ch == '\'' {
				quotePos = i
				break
			}
			// Charset names are letters, digits, underscore only (e.g. utf8mb4, latin1)
			if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_') {
				break // space, operator, etc. → not a charset introducer
			}
		}
		if quotePos > 0 && s[len(s)-1] == '\'' {
			isCharsetIntroducer = true
		}
		if isCharsetIntroducer {
			return s[quotePos:]
		}
	}
	return s
}

// isSimpleStringLiteral returns true if s is a simple quoted string literal
// (starts and ends with ' and contains no unescaped ' in the middle and no operators outside quotes).
// Used to distinguish SELECT 'hello' (simple literal) from SELECT 'A' || 'B' (expression).
func isSimpleStringLiteral(s string) bool {
	if len(s) < 2 || s[0] != '\'' || s[len(s)-1] != '\'' {
		return false
	}
	// Check that there is no unescaped ' between position 1 and len-1
	inner := s[1 : len(s)-1]
	for i := 0; i < len(inner); i++ {
		if inner[i] == '\'' && (i == 0 || inner[i-1] != '\\') {
			return false
		}
	}
	return true
}

func normalizeSelectedFunctionArgDisplaySpacing(s string) string {
	for _, fn := range []string{"JSON_SCHEMA_VALID", "JSON_SCHEMA_VALIDATION_REPORT", "JSON_MERGE_PRESERVE"} {
		prefix := fn + "("
		if !strings.HasPrefix(s, prefix) {
			continue
		}
		inner := s[len(prefix):]
		depth := 1
		inQuote := byte(0)
		var b strings.Builder
		for i := 0; i < len(inner); i++ {
			ch := inner[i]
			if inQuote != 0 {
				b.WriteByte(ch)
				if ch == inQuote {
					inQuote = 0
				}
				continue
			}
			if ch == '\'' || ch == '"' || ch == '`' {
				inQuote = ch
				b.WriteByte(ch)
				continue
			}
			if ch == '(' {
				depth++
				b.WriteByte(ch)
				continue
			}
			if ch == ')' {
				depth--
				if depth == 0 {
					return prefix + b.String() + ")"
				}
				b.WriteByte(ch)
				continue
			}
			if ch == ',' && depth == 1 {
				b.WriteString(", ")
				for i+1 < len(inner) && inner[i+1] == ' ' {
					i++
				}
				continue
			}
			b.WriteByte(ch)
		}
	}
	return s
}

// compactOperatorsInDisplayName removes spaces around comparison and arithmetic
// operators in a SQL display name string to match MySQL column header format.
func compactOperatorsInDisplayName(s string) string {
	var result strings.Builder
	inQuote := byte(0)
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inQuote != 0 {
			result.WriteByte(ch)
			if ch == inQuote {
				// Check for escaped quote ('' or \')
				if i+1 < len(s) && s[i+1] == inQuote {
					i++
					result.WriteByte(s[i])
					continue
				}
				inQuote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			inQuote = ch
			result.WriteByte(ch)
			continue
		}
		// Skip spaces adjacent to operators
		if ch == ' ' {
			// Look ahead past spaces for operator
			j := i + 1
			for j < len(s) && s[j] == ' ' {
				j++
			}
			if j < len(s) && isComparisonOrArithOp(s[j]) {
				continue
			}
			// Look behind for operator
			if result.Len() > 0 {
				prev := result.String()
				if isComparisonOrArithOp(prev[len(prev)-1]) {
					continue
				}
			}
		}
		result.WriteByte(ch)
	}
	return result.String()
}

func isComparisonOrArithOp(ch byte) bool {
	return ch == '=' || ch == '<' || ch == '>' || ch == '!' || ch == '+' || ch == '-' || ch == '*' || ch == '/'
}

// compactOperatorsInSubexpressions removes spaces around operators inside function calls and
// subqueries (parenthesized expressions), matching MySQL's column display name behavior.
// At the top level (depth 0), operators are only compacted when both sides are expressions
// (i.e., the left side ends with ')' and the right side starts with a function call).
func compactOperatorsInSubexpressions(s string) string {
	var result strings.Builder
	parenDepth := 0
	inQuote := byte(0)
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inQuote != 0 {
			result.WriteByte(ch)
			if ch == inQuote {
				inQuote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			inQuote = ch
			result.WriteByte(ch)
			continue
		}
		if ch == '(' {
			parenDepth++
			result.WriteByte(ch)
			continue
		}
		if ch == ')' {
			parenDepth--
			result.WriteByte(ch)
			continue
		}
		if ch == ' ' {
			// Compact arithmetic operators at all depths: MySQL displays "3*a" not "3 * a".
			for _, op := range []string{" * ", " / ", " + ", " - "} {
				if i+len(op) <= len(s) && s[i:i+len(op)] == op {
					compact := strings.TrimSpace(op)
					result.WriteString(compact)
					i += len(op) - 1
					goto nextChar
				}
			}
			if parenDepth > 0 {
				// Inside parentheses: compact comparison operators too
				for _, op := range []string{" = ", " != ", " <> ", " >= ", " <= ", " > ", " < "} {
					if i+len(op) <= len(s) && s[i:i+len(op)] == op {
						compact := strings.TrimSpace(op)
						result.WriteString(compact)
						i += len(op) - 1
						goto nextChar
					}
				}
				// Also compact ", " -> ","  inside parentheses only
				if i+2 <= len(s) && s[i:i+2] == ", " {
					result.WriteByte(',')
					i++ // skip the space
					continue
				}
			} else {
				// At top level (depth 0): only compact " = " when both sides are expressions.
				// Left side must end with ')' (i.e., result ends with ')'), and
				// right side must start with a function call (identifier followed by '(').
				if i+3 <= len(s) && s[i:i+3] == " = " {
					// Check left side: result must end with ')'
					resultStr := result.String()
					leftEndsWithParen := len(resultStr) > 0 && resultStr[len(resultStr)-1] == ')'
					// Check right side: must be a function call (word chars then '(')
					rightStart := i + 3
					rightIsFuncCall := false
					if rightStart < len(s) {
						c0 := s[rightStart]
						if c0 >= 'A' && c0 <= 'Z' || c0 >= 'a' && c0 <= 'z' || c0 == '_' {
							// Look ahead to see if there is a '(' before any space or operator
							for j := rightStart + 1; j < len(s); j++ {
								c := s[j]
								if c == '(' {
									rightIsFuncCall = true
									break
								}
								if c == ' ' || c == '=' || c == '<' || c == '>' || c == '!' {
									break
								}
							}
						}
					}
					if leftEndsWithParen && rightIsFuncCall {
						result.WriteByte('=')
						i += 2 // skip " = ": current i is at ' ', advance past '=' and ' '
						goto nextChar
					}
				}
			}
		}
		result.WriteByte(ch)
	nextChar:
	}
	return result.String()
}

// unescapeStringLiterals replaces escaped \n and \t inside quoted strings with actual newlines/tabs.
func unescapeStringLiterals(s string) string {
	if !strings.Contains(s, "\\") {
		return s
	}
	var result strings.Builder
	inQuote := byte(0)
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inQuote != 0 {
			if ch == '\\' && i+1 < len(s) {
				next := s[i+1]
				switch next {
				case '0':
					result.WriteByte(0x00) // \0 = null byte (ASCII 0)
					i++
					continue
				case 'n':
					result.WriteByte('\n')
					i++
					continue
				case 't':
					result.WriteByte('\t')
					i++
					continue
				case 'r':
					result.WriteByte('\r')
					i++
					continue
				case 'b':
					result.WriteByte('\b')
					i++
					continue
				case '\\':
					result.WriteByte('\\')
					i++
					continue
				case '\'':
					result.WriteByte('\'')
					i++
					continue
				case '"':
					result.WriteByte('"')
					i++
					continue
				}
			}
			if ch == inQuote {
				inQuote = 0
			}
			result.WriteByte(ch)
			continue
		}
		if ch == '\'' || ch == '"' {
			inQuote = ch
		}
		result.WriteByte(ch)
	}
	return result.String()
}

// normalizeFuncArgSpaces removes spaces after commas inside function calls,
// matching MySQL's column display name format (e.g., "LEFT(`c1`,0)" not "LEFT(`c1`, 0)").
func normalizeFuncArgSpaces(s string) string {
	var result strings.Builder
	inQuote := byte(0)
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inQuote != 0 {
			result.WriteByte(ch)
			if ch == inQuote {
				inQuote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			inQuote = ch
			result.WriteByte(ch)
			continue
		}
		if ch == ',' && i+1 < len(s) && s[i+1] == ' ' {
			result.WriteByte(',')
			i++ // skip the space after comma
			continue
		}
		result.WriteByte(ch)
	}
	return result.String()
}

// normalizeAggColNameNulls replaces lowercase "null" with "NULL" in aggregate
// column names (outside of quoted strings), matching MySQL display behavior.
// MySQL preserves original keyword case inside CASE expressions, so null is not
// uppercased when the expression contains a CASE...END construct.
func normalizeAggColNameNulls(s string) string {
	// If the expression contains a CASE construct, MySQL preserves original case
	// of keywords including null. Skip uppercasing null in that case.
	lowerS := strings.ToLower(s)
	if strings.Contains(lowerS, "case ") || strings.Contains(lowerS, "(case ") {
		return s
	}
	var result strings.Builder
	inQuote := byte(0)
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inQuote != 0 {
			result.WriteByte(ch)
			if ch == inQuote {
				inQuote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			inQuote = ch
			result.WriteByte(ch)
			continue
		}
		// Check for "null" (case-insensitive) at word boundary
		if (ch == 'n' || ch == 'N') && i+4 <= len(s) && strings.EqualFold(s[i:i+4], "null") {
			// Check word boundaries
			prevOK := i == 0 || s[i-1] == '(' || s[i-1] == ',' || s[i-1] == ' '
			nextOK := i+4 == len(s) || s[i+4] == ')' || s[i+4] == ',' || s[i+4] == ' '
			if prevOK && nextOK {
				result.WriteString("NULL")
				i += 3
				continue
			}
		}
		result.WriteByte(ch)
	}
	return result.String()
}

// uppercaseAggInnerKeywords applies uppercaseSQLKeywords to the inner arguments
// of an aggregate function, preserving the outer function name.
func uppercaseAggInnerKeywords(s string) string {
	// GROUP_CONCAT is intentionally excluded: MySQL preserves lowercase keywords
	// (order by, separator, etc.) in GROUP_CONCAT column display names.
	knownUpper := []string{"JSON_ARRAYAGG(", "JSON_OBJECTAGG(", "COUNT(", "SUM(", "AVG(", "MIN(", "MAX("}
	prefixEnd := 0
	for _, p := range knownUpper {
		if strings.HasPrefix(s, p) {
			prefixEnd = len(p)
			break
		}
	}
	if prefixEnd == 0 {
		return s
	}
	// Find matching close paren
	depth := 1
	inQuote := byte(0)
	end := len(s)
	for i := prefixEnd; i < len(s); i++ {
		ch := s[i]
		if inQuote != 0 {
			if ch == inQuote {
				inQuote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			inQuote = ch
			continue
		}
		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
			if depth == 0 {
				end = i
				break
			}
		}
	}
	inner := s[prefixEnd:end]
	inner = uppercaseSQLKeywords(inner)
	return s[:prefixEnd] + inner + s[end:]
}

// normalizeAggColNameFunctions lowercases non-SQL-keyword function names in aggregate
// column names to match MySQL behavior (e.g. ST_PointFromText → st_pointfromtext).
func normalizeAggColNameFunctions(s string) string {
	// Known aggregate prefixes that should stay uppercase
	knownUpper := []string{"JSON_ARRAYAGG(", "JSON_OBJECTAGG(", "COUNT(", "SUM(", "AVG(", "MIN(", "MAX(", "GROUP_CONCAT("}
	// Find the aggregate prefix end
	prefixEnd := 0
	for _, p := range knownUpper {
		if strings.HasPrefix(s, p) {
			prefixEnd = len(p)
			break
		}
	}
	if prefixEnd == 0 {
		return s
	}
	// Find matching close paren for the outer aggregate
	depth := 1
	innerStart := prefixEnd
	innerEnd := len(s)
	inQuote := byte(0)
	for i := innerStart; i < len(s); i++ {
		ch := s[i]
		if inQuote != 0 {
			if ch == inQuote {
				inQuote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			inQuote = ch
			continue
		}
		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
			if depth == 0 {
				innerEnd = i
				break
			}
		}
	}
	inner := s[innerStart:innerEnd]
	// Lowercase function-like identifiers in the inner part (word followed by '(')
	var result strings.Builder
	inQuote = 0
	for i := 0; i < len(inner); i++ {
		ch := inner[i]
		if inQuote != 0 {
			result.WriteByte(ch)
			if ch == inQuote {
				inQuote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			inQuote = ch
			result.WriteByte(ch)
			continue
		}
		// Check for function name pattern: alphabetic/underscore chars followed by '('
		if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '_' {
			j := i
			for j < len(inner) && ((inner[j] >= 'A' && inner[j] <= 'Z') || (inner[j] >= 'a' && inner[j] <= 'z') || (inner[j] >= '0' && inner[j] <= '9') || inner[j] == '_') {
				j++
			}
			word := inner[i:j]
			if j < len(inner) && inner[j] == '(' {
				// It's a function call - lowercase non-SQL function names
				wordUpper := strings.ToUpper(word)
				if isKnownSQLFunction(wordUpper) {
					result.WriteString(wordUpper)
				} else {
					result.WriteString(strings.ToLower(word))
				}
			} else {
				result.WriteString(word)
			}
			i = j - 1
			continue
		}
		result.WriteByte(ch)
	}
	return s[:prefixEnd] + result.String() + s[innerEnd:]
}

// isKnownSQLFunction returns true for SQL/JSON built-in function names that
// should remain uppercase in column headers.
func isKnownSQLFunction(name string) bool {
	known := map[string]bool{
		"DISTINCT": true,
		"CAST": true, "CONVERT": true, "COALESCE": true, "IF": true, "IFNULL": true,
		"NULLIF": true, "CONCAT": true, "CONCAT_WS": true, "LENGTH": true, "CHAR_LENGTH": true,
		"UPPER": true, "LOWER": true, "TRIM": true, "LTRIM": true, "RTRIM": true,
		"REPLACE": true, "SUBSTRING": true, "LEFT": true, "RIGHT": true, "REVERSE": true,
		"REPEAT": true, "LPAD": true, "RPAD": true, "INSTR": true, "LOCATE": true,
		"ABS": true, "CEIL": true, "CEILING": true, "FLOOR": true, "ROUND": true, "TRUNCATE": true,
		"MOD": true, "POWER": true, "SQRT": true, "RAND": true,
		"NOW": true, "CURDATE": true, "CURTIME": true, "DATE": true, "TIME": true,
		"YEAR": true, "MONTH": true, "DAY": true, "HOUR": true, "MINUTE": true, "SECOND": true,
		"DATE_FORMAT": true, "DATE_ADD": true, "DATE_SUB": true, "DATEDIFF": true,
		"COUNT": true, "SUM": true, "AVG": true, "MIN": true, "MAX": true,
		"BIT_AND": true, "BIT_OR": true, "BIT_XOR": true,
		"GROUP_CONCAT": true, "JSON_ARRAYAGG": true, "JSON_OBJECTAGG": true,
		"JSON_EXTRACT": true, "JSON_VALID": true, "JSON_TYPE": true,
		"JSON_DEPTH": true, "JSON_LENGTH": true, "JSON_KEYS": true,
		"JSON_ARRAY": true, "JSON_OBJECT": true,
		"JSON_MERGE_PRESERVE": true, "JSON_MERGE_PATCH": true,
		"JSON_CONTAINS": true, "JSON_CONTAINS_PATH": true,
		"JSON_SET": true, "JSON_INSERT": true, "JSON_REPLACE": true,
		"JSON_REMOVE": true, "JSON_UNQUOTE": true, "JSON_QUOTE": true,
		"JSON_PRETTY": true, "JSON_STORAGE_SIZE": true, "JSON_STORAGE_FREE": true,
		"JSON_OVERLAPS": true, "JSON_SEARCH": true, "JSON_VALUE": true,
		"JSON_SCHEMA_VALID": true, "JSON_SCHEMA_VALIDATION_REPORT": true,
		"JSON_ARRAY_APPEND": true, "JSON_ARRAY_INSERT": true,
		"HEX": true, "UNHEX": true, "BIN": true, "OCT": true,
		"CHARSET": true, "COLLATION": true,
	}
	return known[name]
}

// normalizeAggColNameSubselect adds "FROM dual" to subselects without FROM in
// aggregate column names, matching MySQL display behavior.
func normalizeAggColNameSubselect(s string) string {
	// Look for "(SELECT ... )" patterns without FROM
	upper := strings.ToUpper(s)
	idx := strings.Index(upper, "(SELECT ")
	if idx < 0 {
		return s
	}
	// Find the matching close paren
	start := idx + 1 // after '('
	depth := 1
	inQuote := byte(0)
	end := len(s)
	for i := idx + 1; i < len(s); i++ {
		ch := s[i]
		if inQuote != 0 {
			if ch == inQuote {
				inQuote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			inQuote = ch
			continue
		}
		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
			if depth == 0 {
				end = i
				break
			}
		}
	}
	subquery := s[start:end]
	subqueryUpper := strings.ToUpper(subquery)
	// Remove spaces after commas in the subquery to match MySQL compact display
	subquery = normalizeFuncArgSpaces(subquery)
	// If subquery has no FROM clause, add " FROM dual"
	if !strings.Contains(subqueryUpper, " FROM ") {
		subquery = subquery + " FROM dual"
	}
	return s[:start] + subquery + s[end:]
}

// uppercaseSQLKeywords converts SQL keywords in a string to uppercase to match MySQL's
// column display name behavior for subquery expressions.
func uppercaseSQLKeywords(s string) string {
	keywords := []string{
		"select", "from", "where", "and", "or", "not", "in", "exists",
		"any", "some", "all", "as", "on", "join", "left", "right", "inner",
		"outer", "cross", "group", "by", "order", "having", "limit", "offset",
		"union", "except", "intersect", "between", "like", "is",
		"true", "false",
		"asc", "desc", "count", "sum", "avg", "min", "max", "upper", "lower",
		"row", "with", "cast", "convert", "json_extract", "json_valid", "json_type",
		"json_depth", "json_length", "json_keys", "json_array", "json_object",
		"json_merge_preserve", "json_merge_patch", "json_contains", "json_contains_path",
		"json_set", "json_insert", "json_replace", "json_remove", "json_unquote",
		"json_quote", "json_pretty", "json_storage_size", "json_storage_free",
		"json_overlaps", "json_search", "json_value", "json_arrayagg", "json_objectagg",
		"json_schema_valid", "json_schema_validation_report",
	}
	result := []byte(s)
	for _, kw := range keywords {
		kwBytes := []byte(kw)
		upper := []byte(strings.ToUpper(kw))
		i := 0
		for i < len(result) {
			// Skip quoted strings
			if result[i] == '\'' || result[i] == '"' || result[i] == '`' {
				q := result[i]
				i++
				for i < len(result) && result[i] != q {
					i++
				}
				if i < len(result) {
					i++
				}
				continue
			}
			// Check word boundary at start
			if i > 0 {
				ch := result[i-1]
				if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' || (ch >= '0' && ch <= '9') {
					i++
					continue
				}
			}
			// Check if keyword matches
			if i+len(kw) <= len(result) {
				match := true
				for j := 0; j < len(kw); j++ {
					if result[i+j] != kwBytes[j] {
						match = false
						break
					}
				}
				if match {
					// Check word boundary at end
					end := i + len(kw)
					if end < len(result) {
						ch := result[end]
						if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' || (ch >= '0' && ch <= '9') {
							i++
							continue
						}
					}
					copy(result[i:i+len(kw)], upper)
					i += len(kw)
					continue
				}
			}
			i++
		}
	}
	return string(result)
}

// stripLeadingCStyleComments removes leading /* ... */ comment blocks and
// surrounding whitespace.
func stripLeadingCStyleComments(s string) string {
	for {
		trimmed := strings.TrimSpace(s)
		if !strings.HasPrefix(trimmed, "/*") {
			return trimmed
		}
		// Preserve MySQL versioned comments that should be executed (version <= 80040).
		// Format: /*!NNNNN content */ where NNNNN is the minimum MySQL version.
		// If version <= our server version (8.0.40 = 80040), the content should be
		// executed, so let vitess handle it. High version comments (e.g. /*!99999 ... */)
		// are effectively no-ops and can be stripped.
		if strings.HasPrefix(trimmed, "/*!") {
			// Extract version number
			verStr := ""
			for i := 3; i < len(trimmed) && i < 8; i++ {
				if trimmed[i] >= '0' && trimmed[i] <= '9' {
					verStr += string(trimmed[i])
				} else {
					break
				}
			}
			if len(verStr) == 5 {
				ver := 0
				for _, ch := range verStr {
					ver = ver*10 + int(ch-'0')
				}
				if ver <= 80040 {
					// This versioned comment should be executed - let vitess handle it
					return trimmed
				}
			}
			// High version or malformed - strip like a regular comment
		}
		end := strings.Index(trimmed, "*/")
		if end < 0 {
			return trimmed
		}
		s = trimmed[end+2:]
	}
}

// soundex implements the MySQL SOUNDEX() function.
// It returns a 4-character Soundex code for the input string.
func soundex(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	// Map of letters to soundex digits
	code := map[byte]byte{
		'B': '1', 'F': '1', 'P': '1', 'V': '1',
		'C': '2', 'G': '2', 'J': '2', 'K': '2', 'Q': '2', 'S': '2', 'X': '2', 'Z': '2',
		'D': '3', 'T': '3',
		'L': '4',
		'M': '5', 'N': '5',
		'R': '6',
	}
	upper := strings.ToUpper(s)
	var result []byte
	// Find first letter
	firstIdx := -1
	for i := 0; i < len(upper); i++ {
		if upper[i] >= 'A' && upper[i] <= 'Z' {
			firstIdx = i
			break
		}
	}
	if firstIdx < 0 {
		return "0000"
	}
	result = append(result, upper[firstIdx])
	lastCode := code[upper[firstIdx]]
	// MySQL SOUNDEX: non-alpha chars (spaces, numbers, punctuation) do NOT reset adjacency.
	// Only vowels/H/W/Y (letters without a soundex code) are skipped without resetting.
	// All consonants with the same soundex code as the previous are skipped.
	// MySQL does not limit the result to 4 characters.
	for i := firstIdx + 1; i < len(upper); i++ {
		c := upper[i]
		if c < 'A' || c > 'Z' {
			// Non-letter: skip without resetting lastCode
			continue
		}
		if d, ok := code[c]; ok {
			if d != lastCode {
				result = append(result, d)
				lastCode = d
			}
		} else {
			// A, E, I, O, U, H, W, Y — not coded, skip but do NOT reset adjacency
		}
	}
	for len(result) < 4 {
		result = append(result, '0')
	}
	return string(result)
}

// likePatternToRegexpStr converts a SQL LIKE pattern to a regexp string with a given escape rune.
// prefix is the regexp prefix (e.g. "(?i)^" for case-insensitive or "^" for case-sensitive).
func likePatternToRegexpStr(pattern string, escapeChar rune, prefix string) string {
	var sb strings.Builder
	sb.WriteString(prefix)
	runes := []rune(pattern)
	for i := 0; i < len(runes); i++ {
		c := runes[i]
		if c == escapeChar && i+1 < len(runes) {
			// The next character is escaped: treat it literally (not as a wildcard)
			sb.WriteString(regexp.QuoteMeta(string(runes[i+1])))
			i++
		} else if c == '%' {
			sb.WriteString(".*")
		} else if c == '_' {
			sb.WriteString(".")
		} else {
			sb.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	sb.WriteString("$")
	return sb.String()
}

// likeToRegexpCaseSensitive converts a SQL LIKE pattern to a case-sensitive Go regexp.
// Used for LIKE with binary or case-sensitive collations.
func likeToRegexpCaseSensitive(pattern string) *regexp.Regexp {
	re, _ := regexp.Compile(likePatternToRegexpStr(pattern, '\\', "^"))
	return re
}

// likeToRegexpCaseSensitiveEscape converts a SQL LIKE pattern with a custom escape char to a case-sensitive Go regexp.
func likeToRegexpCaseSensitiveEscape(pattern string, escapeChar rune) *regexp.Regexp {
	re, _ := regexp.Compile(likePatternToRegexpStr(pattern, escapeChar, "^"))
	return re
}

// likeToRegexpEscape converts a SQL LIKE pattern with a custom escape char to a case-insensitive Go regexp.
func likeToRegexpEscape(pattern string, escapeChar rune) *regexp.Regexp {
	re, _ := regexp.Compile(likePatternToRegexpStr(pattern, escapeChar, "(?i)^"))
	return re
}

// likeToRegexp converts a SQL LIKE pattern to a Go regexp.
func likeToRegexp(pattern string) *regexp.Regexp {
	var sb strings.Builder
	sb.WriteString("(?i)^") // case-insensitive
	runes := []rune(pattern)
	for i := 0; i < len(runes); i++ {
		c := runes[i]
		if c == '\\' && i+1 < len(runes) {
			sb.WriteString(regexp.QuoteMeta(string(runes[i+1])))
			i++
		} else if c == '%' {
			sb.WriteString(".*")
		} else if c == '_' {
			sb.WriteString(".")
		} else {
			sb.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	sb.WriteString("$")
	re, _ := regexp.Compile(sb.String())
	return re
}
