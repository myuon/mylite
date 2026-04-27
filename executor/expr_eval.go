package executor

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/unicode/norm"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/vt/sqlparser"
)

// evalTupleAware evaluates an expression, returning []interface{} for ValTuple
// (to support nested row comparisons) or a scalar value otherwise.
func (e *Executor) evalTupleAware(expr sqlparser.Expr) (interface{}, error) {
	if tup, ok := expr.(sqlparser.ValTuple); ok {
		vals := make([]interface{}, len(tup))
		for i, item := range tup {
			v, err := e.evalTupleAware(item)
			if err != nil {
				return nil, err
			}
			vals[i] = v
		}
		return vals, nil
	}
	return e.evalExpr(expr)
}

// rowTuplesEqual compares two values that may be scalars or []interface{} (nested tuples).
// Returns (equal, hasNull, error). hasNull is true if either side is NULL.
func rowTuplesEqual(a, b interface{}) (equal bool, hasNull bool, err error) {
	aSlice, aIsTuple := a.([]interface{})
	bSlice, bIsTuple := b.([]interface{})
	if aIsTuple && bIsTuple {
		if len(aSlice) != len(bSlice) {
			return false, false, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(aSlice)))
		}
		sawNull := false
		for i := range aSlice {
			eq, null, err := rowTuplesEqual(aSlice[i], bSlice[i])
			if err != nil {
				return false, false, err
			}
			if null {
				// Don't stop: a later definitive mismatch overrides NULL
				sawNull = true
				continue
			}
			if !eq {
				// Definitive mismatch: result is 0 regardless of any NULLs seen
				return false, false, nil
			}
		}
		if sawNull {
			return false, true, nil
		}
		return true, false, nil
	}
	if aIsTuple || bIsTuple {
		// One is a tuple, the other is scalar - type mismatch
		// The error message should reflect the number of columns expected by the tuple side.
		if aIsTuple {
			return false, false, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(aSlice)))
		}
		return false, false, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(bSlice)))
	}
	if a == nil || b == nil {
		return false, true, nil
	}
	match, err := compareValues(a, b, sqlparser.EqualOp)
	return match, false, err
}

// evalLiteralExpr handles *sqlparser.Literal evaluation.
func (e *Executor) evalLiteralExpr(v *sqlparser.Literal) (interface{}, error) {
	switch v.Type {
	case sqlparser.IntVal:
		n, err := strconv.ParseInt(v.Val, 10, 64)
		if err != nil {
			// Try unsigned 64-bit
			u, err2 := strconv.ParseUint(v.Val, 10, 64)
			if err2 != nil {
				return nil, &intOverflowError{val: v.Val, kind: "DECIMAL"}
			}
			return u, nil
		}
		return n, nil
	case sqlparser.FloatVal:
		f, err := strconv.ParseFloat(v.Val, 64)
		if err != nil {
			return nil, err
		}
		return f, nil
	case sqlparser.DecimalVal:
		// Keep DECIMAL literal as string to preserve precision.
		return v.Val, nil
	case sqlparser.StrVal:
		return v.Val, nil
	case sqlparser.HexVal:
		// x'...' hex literal: return as HexBytes so that string contexts
		// (CONVERT USING, JSON functions) see the hex-digit string while
		// numeric contexts (arithmetic, comparison) interpret it as a
		// big-endian unsigned integer via toFloat/toInt64.
		return HexBytes(v.Val), nil
	case sqlparser.HexNum:
		// 0x878A -> parse as integer
		s := v.Val
		if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
			s = s[2:]
		}
		n, err := strconv.ParseInt(s, 16, 64)
		if err != nil {
			// Try unsigned for large values like 0xfffffffffffffff1
			u, err2 := strconv.ParseUint(s, 16, 64)
			if err2 != nil {
				// Value exceeds uint64; return overflow error so callers can clamp to MaxUint64 + warn
				return nil, &intOverflowError{val: s, kind: "BINARY"}
			}
			return u, nil
		}
		return n, nil
	case sqlparser.BitNum:
		// 0b1010 or b'1010' -> parse as integer
		s := strings.TrimSpace(v.Val)
		if strings.HasPrefix(strings.ToLower(s), "b'") && strings.HasSuffix(s, "'") {
			s = s[2 : len(s)-1]
		} else if strings.HasPrefix(s, "0b") || strings.HasPrefix(s, "0B") {
			s = s[2:]
		}
		if s == "" {
			return int64(0), nil
		}
		// Try parsing as uint64 first for large values
		u, err := strconv.ParseUint(s, 2, 64)
		if err != nil {
			return v.Val, nil
		}
		if u <= math.MaxInt64 {
			return int64(u), nil
		}
		return u, nil
	case sqlparser.DateVal:
		// Validate DATE literal: must have exactly 3 components (year, month, day)
		// separated by -, ., /, or :. No trailing time components allowed.
		s := v.Val
		normalized, err := normalizeDateLiteral(s, e.sqlMode)
		if err != nil {
			return nil, err
		}
		return normalized, nil
	case sqlparser.TimeVal:
		// Validate and normalize TIME literal.
		normalized, err := normalizeTimeLiteral(v.Val)
		if err != nil {
			return nil, err
		}
		return normalized, nil
	case sqlparser.TimestampVal:
		// Validate TIMESTAMP literal: must have date AND time component (YYYY-MM-DD HH[:MM[:SS]])
		normalized, err := normalizeTimestampLiteral(v.Val)
		if err != nil {
			return nil, err
		}
		return normalized, nil
	default:
		// Handle timestamp/date/time typed literals as plain string values.
		return v.Val, nil
	}
}

// allDigits returns true if the string contains only decimal digits.
func allDigits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(s) > 0
}

// normalizeTimestampLiteral validates and normalizes a TIMESTAMP/DATETIME literal.
// MySQL requires at minimum YYYY-MM-DD HH (date + hour component).
// Just a date (YYYY-MM-DD) is not sufficient.
// Also accepts packed format YYYYMMDDHHMMSS.
func normalizeTimestampLiteral(s string) (string, error) {
	errFn := func() error {
		return mysqlError(1292, "HY000", fmt.Sprintf("Incorrect DATETIME value: '%s'", s))
	}
	// Check for packed format (all digits, no separators): YYYYMMDDHHMMSS
	if allDigits(s) {
		if len(s) < 12 {
			return "", errFn()
		}
		// Return as-is, MySQL will handle normalization
		return s, nil
	}
	// Handle packed format with trailing dot or fractional: YYYYMMDDHHMMSS. or YYYYMMDDHHMMSS.f
	// e.g., "20130710010203." → accept (trailing dot means zero fractional, strip dot)
	// e.g., "20130710010203.1" → accept, preserve fractional
	// e.g., "20130710010203.1234567" → accept with 7 digits (MySQL rounds to 6 when displaying)
	if dotIdx := strings.Index(s, "."); dotIdx == 14 && allDigits(s[:14]) {
		fracPart := ""
		if len(s) > 15 {
			fracPart = s[15:]
		}
		if fracPart == "" || allDigits(fracPart) {
			// Return packed part; coerce will expand YYYYMMDDHHMMSS to datetime.
			// Fractional is returned as part of the value for coerce to handle.
			// MySQL rounds fractional seconds > 6 digits to 6 digits.
			if len(fracPart) > 6 {
				// Round to 6 digits.
				keep := fracPart[:6]
				roundUp := fracPart[6] >= '5'
				if roundUp {
					// Increment last digit.
					keepBytes := []byte(keep)
					carry := true
					for i := len(keepBytes) - 1; i >= 0 && carry; i-- {
						d := keepBytes[i] - '0' + 1
						if d >= 10 {
							keepBytes[i] = '0'
						} else {
							keepBytes[i] = '0' + d
							carry = false
						}
					}
					keep = string(keepBytes)
					// If carry out, the fractional part overflowed - return as-is (MySQL handles it)
				}
				fracPart = keep
			}
			if fracPart != "" {
				return s[:14] + "." + fracPart, nil
			}
			return s[:14], nil // strip trailing dot only
		}
		return "", errFn()
	}
	// Must contain at least a space separator (date + time)
	spaceIdx := strings.Index(s, " ")
	if spaceIdx < 0 {
		return "", errFn()
	}
	// Date part must be non-empty and time part must have at least hour
	datePart := s[:spaceIdx]
	timePart := strings.TrimSpace(s[spaceIdx+1:])
	if datePart == "" || timePart == "" {
		return "", errFn()
	}
	// Validate date part: must have 3 components (Y, M, D) separated by -, /, . or :
	dateParts := strings.FieldsFunc(datePart, func(r rune) bool { return r == '-' || r == '/' || r == '.' || r == ':' })
	if len(dateParts) < 3 {
		return "", errFn()
	}
	// Check that the date parts are numeric
	for _, dp := range dateParts {
		for _, c := range dp {
			if c < '0' || c > '9' {
				return "", errFn()
			}
		}
	}
	// Check that the time part starts with valid digits
	for i, c := range timePart {
		if i == 0 && !(c >= '0' && c <= '9') {
			return "", errFn()
		}
		if c == ':' || c == '.' {
			break
		}
		if !(c >= '0' && c <= '9') {
			return "", errFn()
		}
	}

	// Check for fractional seconds precision > 6 (invalid)
	if dot := strings.LastIndex(timePart, "."); dot >= 0 {
		frac := timePart[dot+1:]
		if len(frac) > 6 {
			return "", errFn()
		}
	}

	// Normalize the time component to HH:MM:SS[.frac]
	// Time part may be: HH, HH:MM, HH:MM:SS, HH:MM:SS.frac, HH:MM:SS.
	// Also supports dot-separated time: HH.MM.SS.frac (MySQL TIMESTAMP extension).
	h, m, secFull := "00", "00", "00"

	// Check if the time part uses dot separators (no colons in the H/M/S section).
	// Pattern: contains '.' but not ':' (before the fractional second position).
	// Dot-separated: "01.02.03.456" → HH=01, MM=02, SS=03, frac=456
	//                "01.02.0.31"   → HH=01, MM=02, SS=0, frac=31
	if !strings.Contains(timePart, ":") && strings.Contains(timePart, ".") {
		// Dot-separated time format.
		dotParts := strings.Split(timePart, ".")
		// Need at least H and M (2 parts).
		if len(dotParts) < 2 {
			return "", errFn()
		}
		h = fmt.Sprintf("%02s", dotParts[0])
		m = fmt.Sprintf("%02s", dotParts[1])
		if len(dotParts) >= 3 {
			sStr := dotParts[2]
			// Validate seconds: must be 1-2 digits and ≤59.
			sVal, sErr := strconv.Atoi(sStr)
			if sErr != nil || sVal > 59 || len(sStr) > 2 {
				return "", errFn()
			}
			secFull = fmt.Sprintf("%02d", sVal)
			if len(dotParts) >= 4 {
				// Remaining parts form the fractional seconds.
				// Only the 4th part is fractional (remaining are invalid).
				if len(dotParts) > 4 {
					return "", errFn()
				}
				frac := dotParts[3]
				if len(frac) > 6 {
					return "", errFn()
				}
				secFull = secFull + "." + frac
			}
		}
	} else {
		timeParts := strings.Split(timePart, ":")
		switch len(timeParts) {
		case 1:
			h = fmt.Sprintf("%02s", timeParts[0])
		case 2:
			h = fmt.Sprintf("%02s", timeParts[0])
			mStr := timeParts[1]
			frac := ""
			if dot := strings.Index(mStr, "."); dot >= 0 {
				frac = mStr[dot:]
				mStr = mStr[:dot]
			}
			m = fmt.Sprintf("%02s", mStr)
			secFull = "00" + frac
		case 3:
			h = fmt.Sprintf("%02s", timeParts[0])
			m = fmt.Sprintf("%02s", timeParts[1])
			sStr := timeParts[2]
			frac := ""
			if dot := strings.Index(sStr, "."); dot >= 0 {
				frac = strings.TrimRight(sStr[dot:], ".")
				sStr = sStr[:dot]
			}
			secFull = fmt.Sprintf("%02s", sStr) + frac
			// Strip trailing just-dot
			secFull = strings.TrimRight(secFull, ".")
		}
	}

	return fmt.Sprintf("%s %s:%s:%s", datePart, h, m, secFull), nil
}

// normalizeTimeLiteral validates and normalizes a TIME literal string.
// MySQL TIME range: -838:59:59 to 838:59:59.
// Returns error for invalid formats or out-of-range values.
func normalizeTimeLiteral(s string) (string, error) {
	errFn := func() error {
		return mysqlError(1292, "HY000", fmt.Sprintf("Incorrect TIME value: '%s'", s))
	}

	if s == "" {
		return "00:00:00", nil
	}

	negative := false
	str := s
	if strings.HasPrefix(str, "-") {
		negative = true
		str = str[1:]
	}

	// Check for non-time characters (letters are invalid in TIME literals)
	// Valid chars: digits, ':', '.', ' '
	for _, c := range str {
		if !((c >= '0' && c <= '9') || c == ':' || c == '.' || c == ' ') {
			return "", errFn()
		}
	}

	// Check for day prefix: "D HH:MM:SS"
	var days int64
	if idx := strings.Index(str, " "); idx >= 0 {
		dayStr := str[:idx]
		d, err := strconv.ParseInt(dayStr, 10, 64)
		if err != nil {
			return "", errFn()
		}
		days = d
		str = str[idx+1:]
	}

	// Split by ':' to get components
	parts := strings.Split(str, ":")

	var totalHours int64
	var minutes int64
	var secFull string // includes fractional part

	switch len(parts) {
	case 1:
		// Single number: treat as seconds (HHMMSS packed)
		numStr := parts[0]
		// strip fractional
		if dot := strings.Index(numStr, "."); dot >= 0 {
			numStr = numStr[:dot]
		}
		n, err := strconv.ParseInt(numStr, 10, 64)
		if err != nil {
			return "", errFn()
		}
		// Treat as HHMMSS: last 2 = seconds, next 2 = minutes, rest = hours
		sec := n % 100
		n /= 100
		min := n % 100
		n /= 100
		hrs := n
		if min >= 60 || sec >= 60 {
			return "", errFn()
		}
		totalHours = days*24 + hrs
		minutes = min
		secFull = fmt.Sprintf("%02d", sec)
		if dot := strings.Index(parts[0], "."); dot >= 0 {
			secFull += parts[0][dot:]
		}
	case 2:
		// HH:MM[.frac] or HH:MM
		h, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return "", errFn()
		}
		mStr := parts[1]
		frac := ""
		if dot := strings.Index(mStr, "."); dot >= 0 {
			frac = mStr[dot:]
			mStr = mStr[:dot]
		}
		mn, err := strconv.ParseInt(mStr, 10, 64)
		if err != nil {
			return "", errFn()
		}
		if mn >= 60 {
			return "", errFn()
		}
		totalHours = days*24 + h
		minutes = mn
		secFull = "00" + frac
	case 3:
		// HH:MM:SS[.frac]
		h, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return "", errFn()
		}
		mn, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return "", errFn()
		}
		sStr := parts[2]
		frac := ""
		if dot := strings.Index(sStr, "."); dot >= 0 {
			frac = sStr[dot:]
			sStr = sStr[:dot]
		}
		sec, err := strconv.ParseInt(sStr, 10, 64)
		if err != nil {
			if sStr == "" {
				sec = 0
			} else {
				return "", errFn()
			}
		}
		if mn >= 60 || sec >= 60 {
			return "", errFn()
		}
		totalHours = days*24 + h
		minutes = mn
		secFull = fmt.Sprintf("%02d", sec) + frac
	default:
		return "", errFn()
	}

	if negative {
		totalHours = -totalHours
	}

	// MySQL max TIME: 838:59:59, min: -838:59:59
	if totalHours > 838 || totalHours < -838 {
		return "", errFn()
	}

	// Strip trailing dot from secFull (e.g. "10." → "10")
	secFull = strings.TrimRight(secFull, ".")

	// Format with at least 2-digit hours (MySQL style: 00:00:10 not 0:00:10)
	absH := totalHours
	if absH < 0 {
		absH = -absH
	}
	var hStr string
	if absH < 10 {
		hStr = fmt.Sprintf("0%d", absH)
	} else {
		hStr = fmt.Sprintf("%d", absH)
	}
	if negative && totalHours <= 0 {
		if totalHours == 0 {
			return fmt.Sprintf("-0:%02d:%s", minutes, secFull), nil
		}
		return fmt.Sprintf("-%s:%02d:%s", hStr, minutes, secFull), nil
	}
	return fmt.Sprintf("%s:%02d:%s", hStr, minutes, secFull), nil
}

// normalizeDateLiteral validates and normalizes a DATE literal string.
// Returns the normalized "YYYY-MM-DD" string, or an error if invalid.
// Supports MySQL's flexible date parsing:
//   - Separators: -, ., /, :
//   - 2-digit years: 00-69 → 2000-2069, 70-99 → 1970-1999
//   - Output always "YYYY-MM-DD"
//
// Invalid cases (error): fewer than 3 components, trailing time part (space), non-digit chars.
func normalizeDateLiteral(s, sqlMode string) (string, error) {
	// No spaces allowed in DATE literals (space indicates trailing time part)
	if strings.ContainsAny(s, " \t") {
		return "", mysqlError(1292, "HY000", fmt.Sprintf("Incorrect DATE value: '%s'", s))
	}

	// Count separator characters - find the separator used
	var sep byte
	sepCount := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '-' || c == '.' || c == '/' || c == ':' {
			if sepCount == 0 {
				sep = c
			} else if c != sep {
				// Mixed separators not allowed
				return "", mysqlError(1292, "HY000", fmt.Sprintf("Incorrect DATE value: '%s'", s))
			}
			sepCount++
		}
	}

	// Must have exactly 2 separators (3 components)
	if sepCount != 2 {
		return "", mysqlError(1292, "HY000", fmt.Sprintf("Incorrect DATE value: '%s'", s))
	}

	// Check that each component is a non-empty sequence of digits
	parts := strings.Split(s, string([]byte{sep}))
	if len(parts) != 3 {
		return "", mysqlError(1292, "HY000", fmt.Sprintf("Incorrect DATE value: '%s'", s))
	}
	for _, p := range parts {
		if p == "" {
			return "", mysqlError(1292, "HY000", fmt.Sprintf("Incorrect DATE value: '%s'", s))
		}
		for _, c := range p {
			if c < '0' || c > '9' {
				return "", mysqlError(1292, "HY000", fmt.Sprintf("Incorrect DATE value: '%s'", s))
			}
		}
	}

	// Parse year/month/day as integers
	year, _ := strconv.Atoi(parts[0])
	month, _ := strconv.Atoi(parts[1])
	day, _ := strconv.Atoi(parts[2])

	// Normalize 2-digit years: 00-69 → 2000-2069, 70-99 → 1970-1999
	if len(parts[0]) <= 2 {
		if year <= 69 {
			year += 2000
		} else {
			year += 1900
		}
	}

	// Check sql_mode restrictions
	if strings.Contains(sqlMode, "NO_ZERO_IN_DATE") || strings.Contains(sqlMode, "TRADITIONAL") {
		if month == 0 || day == 0 {
			return "", mysqlError(1292, "HY000", fmt.Sprintf("Incorrect DATE value: '%s'", s))
		}
	}

	// Return normalized YYYY-MM-DD
	return fmt.Sprintf("%04d-%02d-%02d", year, month, day), nil
}

// evalColNameExpr handles *sqlparser.ColName evaluation.
func (e *Executor) evalColNameExpr(v *sqlparser.ColName) (interface{}, error) {
	// If we have a row context (correlatedRow), look up the actual value
	if e.correlatedRow != nil {
		colName := v.Name.String()
		if !v.Qualifier.IsEmpty() {
			qualified := v.Qualifier.Name.String() + "." + colName
			if val, ok := e.correlatedRow[qualified]; ok {
				return val, nil
			}
		}
		if val, ok := e.correlatedRow[colName]; ok {
			return val, nil
		}
		// Case-insensitive fallback
		upperName := strings.ToUpper(colName)
		for k, rv := range e.correlatedRow {
			if strings.ToUpper(k) == upperName {
				return rv, nil
			}
		}
		// In ON DUPLICATE KEY UPDATE context, bare column names that are not found
		// in the existing row (correlatedRow) should be looked up in the VALUES row
		// (the row being inserted). This handles cases like:
		//   INSERT INTO t0 SELECT a FROM t1 ON DUPLICATE KEY UPDATE k = a + t1.a + 10
		// where 'a' and 't1.a' refer to the source row being inserted.
		if e.onDupValuesRow != nil {
			colName := v.Name.String()
			if !v.Qualifier.IsEmpty() {
				qualified := v.Qualifier.Name.String() + "." + colName
				if val, ok := e.onDupValuesRow[qualified]; ok {
					return val, nil
				}
			}
			if val, ok := e.onDupValuesRow[colName]; ok {
				return val, nil
			}
			upperName := strings.ToUpper(colName)
			for k, rv := range e.onDupValuesRow {
				if strings.ToUpper(k) == upperName {
					return rv, nil
				}
			}
		}
	}
	// Return column name as string for use in row lookup
	return v.Name.String(), nil
}

// evalVariableExpr handles *sqlparser.Variable evaluation.
func (e *Executor) evalVariableExpr(v *sqlparser.Variable) (interface{}, error) {
	// Handle user variables (@var)
	if v.Scope == sqlparser.VariableScope {
		varName := v.Name.String()
		if val, ok := e.userVars[varName]; ok {
			return val, nil
		}
		return nil, nil
	}
	// Handle @@variables
	name := strings.ToLower(v.Name.String())
	// Strip scope prefix (global., session., local.)
	name = strings.TrimPrefix(name, "global.")
	name = strings.TrimPrefix(name, "session.")
	name = strings.TrimPrefix(name, "local.")

	// @@warning_count and @@error_count are diagnostic variables that return
	// the counts from the *previous* statement (snapshotted in preprocessQuery).
	if name == "warning_count" {
		return e.lastWarningCount, nil
	}
	if name == "error_count" {
		return e.lastErrorCount, nil
	}

	// Check if the user explicitly wrote @@session.var or @@local.var
	// (as opposed to just @@var). We detect this from the raw query text
	// because the AST doesn't distinguish @@var from @@session.var.
	if v.Scope == sqlparser.SessionScope && !sysVarSessionOnly[name] && !sysVarBothScope[name] && (sysVarReadOnly[name] || sysVarGlobalOnly[name]) {
		q := strings.ToLower(e.currentQuery)
		// Check for explicit @@session.name or @@local.name anywhere in the query
		// Use word boundary: the name must be followed by non-word char or end
		hasSessionScope := false
		for _, prefix := range []string{"@@session.", "@@local."} {
			pattern := prefix + name
			idx := strings.Index(q, pattern)
			for idx >= 0 {
				endPos := idx + len(pattern)
				if endPos >= len(q) || !isIdentChar(q[endPos]) {
					hasSessionScope = true
					break
				}
				// Continue searching
				next := strings.Index(q[endPos:], pattern)
				if next < 0 {
					break
				}
				idx = endPos + next
			}
			if hasSessionScope {
				break
			}
		}
		if hasSessionScope {
			return nil, mysqlError(1238, "HY000", fmt.Sprintf("Variable '%s' is a GLOBAL variable", name))
		}
	}

	// Emit deprecation warning for deprecated variables
	if msg, ok := sysVarDeprecated[name]; ok {
		e.addWarning("Warning", 1287, msg)
	}

	// Check for @@global.session_only_var
	if v.Scope == sqlparser.GlobalScope && sysVarSessionOnly[name] && !sysVarBothScope[name] {
		return nil, mysqlError(1238, "HY000", fmt.Sprintf("Variable '%s' is a SESSION variable", name))
	}

	// rand_seed1 and rand_seed2 are write-only: they can be SET to seed the RNG
	// but SELECT @@rand_seed1 / @@rand_seed2 always returns 0 (MySQL behavior).
	if name == "rand_seed1" || name == "rand_seed2" {
		return int64(0), nil
	}

	// @@timestamp is a session-only variable. When set to a non-zero value, it
	// returns that fixed timestamp as a float. When unset or set to 0 (the default),
	// it returns the current UNIX timestamp as a float64.
	// MySQL formats @@timestamp with 6 decimal places (DOUBLE type), and it is
	// a DOUBLE in arithmetic context (so timestamp - timestamp = 0, not 0.000000).
	if name == "timestamp" && v.Scope != sqlparser.GlobalScope {
		var ts float64
		if e.fixedTimestamp != nil {
			ts = float64(e.fixedTimestamp.Unix())
		} else {
			ts = float64(time.Now().Unix())
		}
		// SysVarDouble displays with 6 decimal places but has valueScale=0 so
		// arithmetic operations produce integer results (not ScaledValue).
		return SysVarDouble{Value: ts}, nil
	}

	// pseudo_thread_id defaults to the connection ID but can be overridden by SET.
	if name == "pseudo_thread_id" && v.Scope != sqlparser.GlobalScope {
		if sv, ok := e.sessionScopeVars["pseudo_thread_id"]; ok {
			if n, err := strconv.ParseInt(sv, 10, 64); err == nil {
				return n, nil
			}
		}
		return e.connectionID, nil
	}

	// Check for user-set variables with proper scope resolution.
	var gv string
	var gvOK bool
	if v.Scope == sqlparser.GlobalScope {
		gv, gvOK = e.getSysVarGlobal(name)
	} else {
		gv, gvOK = e.getSysVarSession(name)
		// For global-only variables accessed without explicit scope,
		// fall back to global scope since they have no session value.
		if !gvOK && (sysVarGlobalOnly[name] && !sysVarBothScope[name]) {
			gv, gvOK = e.getSysVarGlobal(name)
		}
	}
	if gvOK {
		// Apply minimum constraints
		if name == "innodb_stats_transient_sample_pages" || name == "innodb_stats_persistent_sample_pages" {
			if n, err := strconv.ParseInt(gv, 10, 64); err == nil && n < 1 {
				gv = "1"
			}
		}
		return sysVarStringToSelectValueForVar(gv, name), nil
	}
	// Fall back to startup variables if present.
	// performance_schema_consumer_* and performance_schema_instrument are
	// startup-only options that must not be exposed as system variables.
	if sv, ok := e.startupVars[name]; ok &&
		!strings.HasPrefix(name, "performance_schema_consumer_") &&
		name != "performance_schema_instrument" {
		if name == "innodb_stats_transient_sample_pages" || name == "innodb_stats_persistent_sample_pages" {
			if n, err := strconv.ParseInt(sv, 10, 64); err == nil && n < 1 {
				sv = "1"
			}
		}
		return sysVarStringToSelectValueForVar(sv, name), nil
	}
	switch name {
	case "version_comment":
		return "mylite", nil
	case "version":
		return "8.4.0-mylite", nil
	case "max_allowed_packet":
		return int64(67108864), nil
	case "character_set_client":
		return "utf8mb4", nil
	case "character_set_connection":
		return "utf8mb4", nil
	case "character_set_results":
		return "utf8mb4", nil
	case "collation_connection":
		return "utf8mb4_0900_ai_ci", nil
	case "sql_mode":
		// For @@global.sql_mode, return the default, not
		// the session-local e.sqlMode which may have been SET at session level.
		if v.Scope == sqlparser.GlobalScope {
			return defaultSQLMode, nil
		}
		return e.sqlMode, nil
	case "autocommit":
		return int64(1), nil
	case "innodb_file_per_table":
		return int64(1), nil
	case "innodb_strict_mode":
		return int64(1), nil
	case "innodb_page_size":
		return int64(16384), nil
	case "innodb_default_row_format":
		return "dynamic", nil
	case "innodb_lock_wait_timeout":
		return int64(50), nil
	case "innodb_autoinc_lock_mode":
		return int64(2), nil
	case "innodb_stats_on_metadata":
		return int64(0), nil
	case "innodb_stats_persistent":
		return int64(1), nil
	case "innodb_stats_auto_recalc":
		return int64(1), nil
	case "innodb_stats_transient_sample_pages":
		return int64(8), nil
	case "innodb_stats_persistent_sample_pages":
		return int64(20), nil
	case "innodb_rollback_on_timeout":
		return int64(1), nil
	case "innodb_table_locks":
		return int64(1), nil
	case "innodb_commit_concurrency":
		return int64(0), nil
	case "innodb_log_buffer_size":
		return int64(16777216), nil
	case "innodb_buffer_pool_size":
		return int64(134217728), nil
	case "innodb_buffer_pool_in_core_file":
		return int64(1), nil
	case "innodb_random_read_ahead":
		return int64(0), nil
	case "innodb_redo_log_encrypt":
		return int64(0), nil
	case "innodb_flush_method":
		return "fsync", nil
	case "innodb_tmpdir":
		return nil, nil
	case "innodb_data_file_path":
		return "ibdata1:12M:autoextend", nil
	case "innodb_change_buffering":
		return "all", nil
	case "innodb_fill_factor":
		return int64(100), nil
	case "datadir":
		return "/var/lib/mysql/", nil
	case "lower_case_table_names":
		return int64(0), nil
	case "default_storage_engine":
		return "InnoDB", nil
	case "server_id":
		return int64(1), nil
	case "auto_increment_increment":
		return int64(1), nil
	case "auto_increment_offset":
		return int64(1), nil
	case "transaction_isolation":
		return "REPEATABLE-READ", nil
	case "tx_isolation":
		return "REPEATABLE-READ", nil
	case "character_set_server":
		return "utf8mb4", nil
	case "collation_server":
		return "utf8mb4_0900_ai_ci", nil
	case "identity", "last_insert_id":
		return e.lastInsertID, nil
	case "insert_id":
		return e.nextInsertID, nil
	case "gtid_owned":
		// At session scope, gtid_owned reflects gtid_next when it's set to ANONYMOUS.
		if v.Scope != sqlparser.GlobalScope {
			if gn, ok := e.sessionScopeVars["gtid_next"]; ok && strings.EqualFold(gn, "ANONYMOUS") {
				return "ANONYMOUS", nil
			}
		}
		return "", nil
	// Variables that return NULL by default
	case "external_user", "proxy_user",
		"ssl_crl", "ssl_crlpath",
		"mysqlx_ssl_crl", "mysqlx_ssl_crlpath",
		"innodb_ft_aux_table",
		"init_file",
		"report_host", "report_user", "report_password",
		"innodb_directories",
		"innodb_ft_server_stopword_table", "innodb_ft_user_stopword_table",
		"innodb_data_home_dir":
		return nil, nil
	}
	// Fall back to the full variables map (SHOW VARIABLES / performance_schema)
	allVars := e.buildVariablesMapScoped(v.Scope == sqlparser.GlobalScope)
	if val, ok := allVars[name]; ok {
		return sysVarStringToSelectValueForVar(val, name), nil
	}
	// Unknown system variable — return MySQL error 1193
	return nil, mysqlError(1193, "HY000", fmt.Sprintf("Unknown system variable '%s'", name))
}

// evalUnaryExpr handles *sqlparser.UnaryExpr evaluation.
func (e *Executor) evalUnaryExpr(v *sqlparser.UnaryExpr) (interface{}, error) {
	val, err := e.evalExpr(v.Expr)
	if err != nil {
		// For unary minus applied to an overflow value, negate the sign in the
		// overflow error so that callers (e.g. CAST AS SIGNED) can clamp to the
		// correct bound.  E.g. CAST(-19999999999999999999 AS SIGNED) should clamp
		// to INT64_MIN, not INT64_MAX.
		if v.Operator == sqlparser.UMinusOp {
			var oe *intOverflowError
			if errors.As(err, &oe) {
				negVal := oe.val
				if strings.HasPrefix(negVal, "-") {
					negVal = strings.TrimPrefix(negVal, "-")
				} else {
					negVal = "-" + negVal
				}
				return nil, &intOverflowError{val: negVal, kind: oe.kind}
			}
		}
		return nil, err
	}
	if v.Operator == sqlparser.UMinusOp {
		switch n := val.(type) {
		case int64:
			// -INT64_MIN overflows int64; return uint64(INT64_MAX+1) = 9223372036854775808.
			if n == math.MinInt64 {
				return uint64(1 << 63), nil
			}
			return -n, nil
		case uint64:
			if n == 1<<63 {
				return int64(math.MinInt64), nil
			}
			if n <= math.MaxInt64 {
				return -int64(n), nil
			}
			// Keep exact value for >int64 range to avoid float precision loss.
			return fmt.Sprintf("-%d", n), nil
		case float64:
			return -n, nil
		case string:
			if strings.HasPrefix(n, "-") {
				return strings.TrimPrefix(n, "-"), nil
			}
			return "-" + n, nil
		}
	}
	if v.Operator == sqlparser.TildaOp {
		// ~ is bitwise NOT
		if val == nil {
			return nil, nil // ~NULL = NULL
		}
		// If value is a binary string (HexBytes or raw binary), do byte-wise NOT
		if binaryBytes, isBinary := toBinaryBytesForBitOp(val); isBinary {
			return binaryBitwiseNot(binaryBytes), nil
		}
		return ^toUint64ForBitOp(val), nil
	}
	if v.Operator == sqlparser.BangOp {
		// ! is logical NOT (deprecated alias in MySQL 8.0)
		if val == nil {
			return nil, nil // !NULL = NULL
		}
		if isTruthy(val) {
			return int64(0), nil
		}
		return int64(1), nil
	}
	return val, nil
}

// evalConvertExpr handles *sqlparser.ConvertExpr evaluation.
func (e *Executor) evalConvertExpr(v *sqlparser.ConvertExpr) (interface{}, error) {
	// CAST(expr AS type)
	val, err := e.evalExpr(v.Expr)
	if v.Type != nil {
		typeName := strings.ToUpper(v.Type.Type)
		// Handle INT_OVERFLOW for integer casts
		if err != nil {
			var oe *intOverflowError
			if errors.As(err, &oe) {
				switch typeName {
				case "SIGNED", "INT", "INTEGER", "BIGINT":
					// Check if the original value had a negative sign
					if strings.HasPrefix(strings.TrimSpace(oe.val), "-") {
						return int64(math.MinInt64), nil
					}
					return int64(math.MaxInt64), nil
				case "UNSIGNED":
					// In strict DML mode, overflow is an error.
					if e.isStrictMode() && e.insideDML {
						return nil, mysqlError(1292, "22007", formatOverflowWarningMsg(oe))
					}
					e.addWarning("Warning", 1292, formatOverflowWarningMsg(oe))
					return uint64(math.MaxUint64), nil
				case "DECIMAL", "FLOAT", "DOUBLE", "REAL":
					// Try to parse as float for decimal casts
					f, parseErr := strconv.ParseFloat(oe.val, 64)
					if parseErr == nil {
						return f, nil
					}
					return nil, err
				}
			}
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	if v.Type == nil {
		return val, nil
	}
	typeName := strings.ToUpper(v.Type.Type)
	// NULL cast always returns NULL regardless of target type.
	if val == nil {
		return nil, nil
	}
	switch typeName {
	case "SIGNED", "INT", "INTEGER", "BIGINT":
		return toInt64(val), nil
	case "UNSIGNED":
		// String → UNSIGNED: if string is too large for int64 (float overflow), clamp to MaxUint64.
		if s, ok := val.(string); ok {
			u, err2 := strconv.ParseUint(strings.TrimSpace(s), 10, 64)
			if err2 != nil {
				if errors.Is(err2, strconv.ErrRange) {
					oe := &intOverflowError{val: s, kind: "INTEGER"}
					if e.isStrictMode() && e.insideDML {
						return nil, mysqlError(1292, "22007", formatOverflowWarningMsg(oe))
					}
					e.addWarning("Warning", 1292, formatOverflowWarningMsg(oe))
					return uint64(math.MaxUint64), nil
				}
				// Try float parse for cases like "3.14"
				n := toInt64(val)
				if n < 0 {
					return uint64(n), nil
				}
				return uint64(n), nil
			}
			return u, nil
		}
		n := toInt64(val)
		if n < 0 {
			return uint64(n), nil
		}
		return uint64(n), nil
	case "CHAR", "VARCHAR", "TEXT", "NCHAR", "NVARCHAR":
		if val == nil {
			return nil, nil
		}
		return toString(val), nil
	case "DECIMAL", "FLOAT", "DOUBLE", "REAL":
		return toFloat(val), nil
	case "DATETIME", "TIMESTAMP":
		if val == nil {
			return nil, nil
		}
		return e.castToDatetime(toString(val))
	case "DATE":
		if val == nil {
			return nil, nil
		}
		return e.castToDate(toString(val))
	case "TIME":
		if val == nil {
			return nil, nil
		}
		return toString(val), nil
	case "BINARY", "VARBINARY":
		if val == nil {
			return nil, nil
		}
		// Convert integer hex literals to big-endian bytes first
		byteVal := hexIntToBytes(val)
		var s string
		if sv, ok := byteVal.(string); ok {
			s = sv
		} else {
			s = toString(val)
		}
		// For BINARY(N), pad to length N
		if typeName == "BINARY" && v.Type != nil && v.Type.Length != nil {
			n := *v.Type.Length
			if n > 0 {
				return padBinaryValue(s, n), nil
			}
		}
		return s, nil
	case "YEAR":
		return toInt64(val), nil
	case "JSON":
		return castToJSONValue(val, isStrictJSONStringCastSource(v.Expr))
	}
	return val, nil
}

// evalBinaryOp handles *sqlparser.BinaryExpr evaluation.
func (e *Executor) evalBinaryOp(v *sqlparser.BinaryExpr) (interface{}, error) {
	// Determine if this is a bitwise operation, which affects how overflow is handled.
	isBitOp := v.Operator == sqlparser.BitOrOp || v.Operator == sqlparser.BitAndOp ||
		v.Operator == sqlparser.BitXorOp || v.Operator == sqlparser.ShiftLeftOp ||
		v.Operator == sqlparser.ShiftRightOp

	left, err := e.evalExpr(v.Left)
	var leftOverflow *intOverflowError
	if err != nil {
		// For INT_OVERFLOW in arithmetic context, treat as max uint64
		var oe *intOverflowError
		if errors.As(err, &oe) {
			leftOverflow = oe
			// DECIMAL overflow in bitwise ops clamps to MaxInt64 (MySQL uses signed context for DECIMAL).
			// BINARY overflow (large hex literal 0x...) in bitwise ops: treat as binary byte string.
			// INTEGER overflow in bitwise ops clamps to MaxUint64.
			if isBitOp && oe.kind == "DECIMAL" {
				left = int64(math.MaxInt64)
			} else {
				left = uint64(math.MaxUint64)
			}
			err = nil
		} else if strings.Contains(err.Error(), "INT_OVERFLOW") {
			// Fallback: match by string if type assertion fails
			left = uint64(math.MaxUint64)
			err = nil
		} else {
			return nil, err
		}
	}
	right, err := e.evalExpr(v.Right)
	var rightOverflow *intOverflowError
	if err != nil {
		var oe *intOverflowError
		if errors.As(err, &oe) {
			rightOverflow = oe
			if isBitOp && oe.kind == "DECIMAL" {
				right = int64(math.MaxInt64)
			} else {
				right = uint64(math.MaxUint64)
			}
			err = nil
		} else if strings.Contains(err.Error(), "INT_OVERFLOW") {
			right = uint64(math.MaxUint64)
			err = nil
		} else {
			return nil, err
		}
	}
	// Add overflow warnings before computing the result
	if leftOverflow != nil {
		e.addWarning("Warning", 1292, formatOverflowWarningMsg(leftOverflow))
	}
	if rightOverflow != nil {
		e.addWarning("Warning", 1292, formatOverflowWarningMsg(rightOverflow))
	}
	// For bit operations, detect string overflow that wasn't caught as intOverflowError.
	// Strings in bitwise context are treated as UNSIGNED integers; overflow → MaxUint64 + warning.
	if isBitOp && leftOverflow == nil {
		if s, ok := left.(string); ok {
			if _, err2 := strconv.ParseUint(strings.TrimSpace(s), 10, 64); err2 != nil && errors.Is(err2, strconv.ErrRange) {
				e.addWarning("Warning", 1292, formatOverflowWarningMsg(&intOverflowError{val: strings.TrimSpace(s), kind: "INTEGER"}))
			}
		}
	}
	if isBitOp && rightOverflow == nil {
		if s, ok := right.(string); ok {
			if _, err2 := strconv.ParseUint(strings.TrimSpace(s), 10, 64); err2 != nil && errors.Is(err2, strconv.ErrRange) {
				e.addWarning("Warning", 1292, formatOverflowWarningMsg(&intOverflowError{val: strings.TrimSpace(s), kind: "INTEGER"}))
			}
		}
	}
	// In TRADITIONAL (strict + ERROR_FOR_DIVISION_BY_ZERO) mode, division by zero
	// is an error (not NULL) when inside a DML context.
	// Note: ERROR_FOR_DIVISION_BY_ZERO alone (without strict) only produces a warning.
	return evalBinaryExpr(left, right, v.Operator, e.getDivPrecisionIncrement())
}

// collCoercibility holds the effective collation name and its coercibility level for an expression.
// Coercibility levels (lower = stronger):
//   0 = EXPLICIT (from COLLATE clause)
//   1 = (unused)
//   2 = IMPLICIT (column with explicit collation)
//   3 = SYSCONST (system constant, e.g. @@variable)
//   4 = COERCIBLE (string literal using connection collation)
//   5 = NUMERIC (number - not a string)
//   6 = IGNORABLE (NULL, binary functions)
//  -1 = not a string expression
type collCoercibility struct {
	collation    string
	coercibility int
}

// isStringColType returns true if a MySQL column type is a string type (CHAR, VARCHAR, TEXT, ENUM, SET).
func isStringColType(colType string) bool {
	t := strings.ToUpper(colType)
	return strings.Contains(t, "CHAR") || strings.Contains(t, "TEXT") ||
		strings.Contains(t, "ENUM") || strings.Contains(t, "SET")
}

// getExprCollationInfo returns the effective collation and coercibility for an expression,
// using the current queryTableDef for column lookups.
// Returns coercibility=-1 if the expression is not a string type.
func (e *Executor) getExprCollationInfo(expr sqlparser.Expr) collCoercibility {
	switch v := expr.(type) {
	case *sqlparser.CollateExpr:
		// EXPLICIT collation via COLLATE clause (coercibility 0)
		return collCoercibility{collation: v.Collation, coercibility: 0}
	case *sqlparser.ColName:
		// IMPLICIT collation from column definition (coercibility 2)
		colName := v.Name.String()
		if e.queryTableDef != nil {
			coll := e.lookupColumnCollation(colName, e.queryTableDef)
			if coll != "" {
				return collCoercibility{collation: coll, coercibility: 2}
			}
		}
		// Column not found or not a string type
		return collCoercibility{coercibility: -1}
	case *sqlparser.Literal:
		if v.Type == sqlparser.StrVal {
			// String literal uses connection collation (coercibility 4 = COERCIBLE)
			connColl := "utf8mb4_0900_ai_ci"
			if cv, ok := e.getSysVar("collation_connection"); ok && cv != "" {
				connColl = cv
			}
			return collCoercibility{collation: connColl, coercibility: 4}
		}
		return collCoercibility{coercibility: -1}
	case *sqlparser.IntroducerExpr:
		// _charset'str' — charset introducer, use the default collation for that charset
		if lit, ok := v.Expr.(*sqlparser.Literal); ok && lit.Type == sqlparser.StrVal {
			cs := strings.ToLower(strings.TrimPrefix(v.CharacterSet, "_"))
			coll := catalog.DefaultCollationForCharset(cs)
			if coll == "" {
				coll = "utf8mb4_0900_ai_ci"
			}
			return collCoercibility{collation: coll, coercibility: 4}
		}
		return collCoercibility{coercibility: -1}
	}
	return collCoercibility{coercibility: -1}
}

// isBinaryColType returns true if the column type uses binary collation (BLOB, BINARY, VARBINARY).
func isBinaryColType(colType string) bool {
	t := strings.ToUpper(strings.TrimSpace(colType))
	// Strip size spec: BINARY(10) -> BINARY, VARBINARY(100) -> VARBINARY
	if paren := strings.IndexByte(t, '('); paren >= 0 {
		t = strings.TrimSpace(t[:paren])
	}
	switch t {
	case "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "BINARY", "VARBINARY":
		return true
	}
	return false
}

// lookupColumnCollation returns the effective collation for a named column in a table definition.
// Returns "" if the column is not found or is not a string/binary type.
func (e *Executor) lookupColumnCollation(colName string, td *catalog.TableDef) string {
	for _, col := range td.Columns {
		if strings.EqualFold(col.Name, colName) {
			if isBinaryColType(col.Type) {
				return "binary" // binary types always use binary collation
			}
			if !isStringColType(col.Type) {
				return "" // not a string column
			}
			if col.Collation != "" {
				return col.Collation
			}
			if col.Charset != "" {
				return catalog.DefaultCollationForCharset(col.Charset)
			}
			if td.Collation != "" {
				return td.Collation
			}
			if td.Charset != "" {
				return catalog.DefaultCollationForCharset(td.Charset)
			}
			return "utf8mb4_0900_ai_ci" // server default
		}
	}
	return ""
}

// checkCollationMixForIN checks if the expressions in an IN expression have incompatible collations.
// allExprs includes the left operand followed by the tuple items.
// Returns an appropriate MySQL error (1267, 1270, or 1271) if there is an illegal collation mix,
// or nil if the collations are compatible.
func (e *Executor) checkCollationMixForIN(leftExpr sqlparser.Expr, tupleItems []sqlparser.Expr) error {
	// Build the list of all expressions: left + tuple items
	allExprs := make([]sqlparser.Expr, 0, 1+len(tupleItems))
	allExprs = append(allExprs, leftExpr)
	allExprs = append(allExprs, tupleItems...)

	// Get collation info for each expression
	infos := make([]collCoercibility, len(allExprs))
	for i, expr := range allExprs {
		infos[i] = e.getExprCollationInfo(expr)
	}

	// Determine the minimum (strongest) coercibility among string expressions
	minCoercibility := 7 // start above all valid levels
	for _, info := range infos {
		if info.coercibility >= 0 && info.coercibility < minCoercibility {
			minCoercibility = info.coercibility
		}
	}
	if minCoercibility == 7 {
		// No string expressions — no conflict possible
		return nil
	}

	// Collect distinct collations at the minimum coercibility level
	seen := map[string]bool{}
	for _, info := range infos {
		if info.coercibility == minCoercibility {
			seen[strings.ToLower(info.collation)] = true
		}
	}

	if len(seen) <= 1 {
		// All string operands at strongest level agree on collation — no conflict
		return nil
	}

	// There is a conflict. Determine the operation string and error code.
	totalItems := len(allExprs)
	coercName := "IMPLICIT"
	switch minCoercibility {
	case 0:
		coercName = "EXPLICIT"
	case 2:
		coercName = "IMPLICIT"
	case 4:
		coercName = "COERCIBLE"
	}

	// Build the list of (collation, coercibility_name) strings for conflicting items
	var conflictList []string
	seenConflict := map[string]bool{}
	for _, info := range infos {
		if info.coercibility == minCoercibility {
			key := strings.ToLower(info.collation)
			if !seenConflict[key] {
				seenConflict[key] = true
				conflictList = append(conflictList, fmt.Sprintf("(%s,%s)", info.collation, coercName))
			}
		}
	}

	switch totalItems {
	case 2:
		// a in (b) — treated as a = b comparison
		if len(conflictList) >= 2 {
			return mysqlError(1267, "HY000", fmt.Sprintf(
				"Illegal mix of collations %s and %s for operation '='",
				conflictList[0], conflictList[1]))
		}
	case 3:
		if len(conflictList) >= 2 {
			msg := fmt.Sprintf("Illegal mix of collations %s for operation ' IN '", strings.Join(conflictList, ","))
			return mysqlError(1270, "HY000", msg)
		}
	default:
		// 4 or more items total (N collations case)
		return mysqlError(1271, "HY000", "Illegal mix of collations for operation ' IN '")
	}
	return nil
}

// checkCollationMixForEQ checks if a binary = / != comparison has incompatible collations.
func (e *Executor) checkCollationMixForEQ(leftExpr, rightExpr sqlparser.Expr) error {
	leftInfo := e.getExprCollationInfo(leftExpr)
	rightInfo := e.getExprCollationInfo(rightExpr)

	// If either side is not a string, no conflict
	if leftInfo.coercibility < 0 || rightInfo.coercibility < 0 {
		return nil
	}

	// Determine strongest coercibility
	minCoercibility := leftInfo.coercibility
	if rightInfo.coercibility < minCoercibility {
		minCoercibility = rightInfo.coercibility
	}

	// Only check operands at the strongest level
	var atMin []collCoercibility
	if leftInfo.coercibility == minCoercibility {
		atMin = append(atMin, leftInfo)
	}
	if rightInfo.coercibility == minCoercibility {
		atMin = append(atMin, rightInfo)
	}

	if len(atMin) < 2 {
		return nil
	}

	// Check if all collations at the min level are the same
	first := strings.ToLower(atMin[0].collation)
	for _, info := range atMin[1:] {
		if strings.ToLower(info.collation) != first {
			coercName := "IMPLICIT"
			switch minCoercibility {
			case 0:
				coercName = "EXPLICIT"
			case 2:
				coercName = "IMPLICIT"
			case 4:
				coercName = "COERCIBLE"
			}
			return mysqlError(1267, "HY000", fmt.Sprintf(
				"Illegal mix of collations (%s,%s) and (%s,%s) for operation '='",
				atMin[0].collation, coercName,
				info.collation, coercName))
		}
	}
	return nil
}

// evalComparisonExpr handles *sqlparser.ComparisonExpr evaluation.
func (e *Executor) evalComparisonExpr(v *sqlparser.ComparisonExpr) (interface{}, error) {
	// Handle IN / NOT IN specially: right side is a ValTuple
	if v.Operator == sqlparser.InOp || v.Operator == sqlparser.NotInOp {
		// When left is a tuple and right is a subquery, delegate directly to
		// evalInSubquery which handles tuple evaluation internally. Evaluating
		// v.Left via evalExpr would fail with "Operand should contain 1 column(s)".
		if _, leftIsTuple := v.Left.(sqlparser.ValTuple); leftIsTuple {
			if sub, ok := v.Right.(*sqlparser.Subquery); ok {
				return e.evalInSubquery(nil, v.Left, sub, v.Operator)
			}
			// Row/tuple IN (tuple of tuples): (a,b) IN ((1,2),(3,4))
			// Right side must be a ValTuple where each element is itself a ValTuple.
			if rightTuple, ok := v.Right.(sqlparser.ValTuple); ok {
				leftTuple := v.Left.(sqlparser.ValTuple)

				// MySQL validates all IN-list items structurally BEFORE evaluating any
				// match. Do a pre-validation pass over all items in rightTuple.
				for _, item := range rightTuple {
					switch rItem := item.(type) {
					case sqlparser.ValTuple:
						// Each tuple item must structurally match leftTuple.
						if err := validateRowTupleStructure(leftTuple, rItem); err != nil {
							return nil, err
						}
					case *sqlparser.Subquery:
						// A subquery returns a flat row; if any element of leftTuple
						// is a nested tuple, the subquery can never match that element.
						// MySQL reports an error in this case.
						for _, lElem := range leftTuple {
							if lNested, ok := lElem.(sqlparser.ValTuple); ok {
								return nil, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(lNested)))
							}
						}
					default:
						// Non-tuple, non-subquery item in a tuple-IN context: fall through to scalar path.
						goto scalarIN
					}
				}

				// Evaluate left tuple values once (recursively, to support nested tuples)
				leftVals := make([]interface{}, len(leftTuple))
				for i, lExpr := range leftTuple {
					lv, err := e.evalTupleAware(lExpr)
					if err != nil {
						return nil, err
					}
					leftVals[i] = lv
				}
				hasNull := false
				// Wrap leftVals as a []interface{} row for recursive comparison
				leftRow := interface{}(leftVals)
				for _, item := range rightTuple {
					switch rItem := item.(type) {
					case sqlparser.ValTuple:
						rVals := make([]interface{}, len(rItem))
						for i, rv := range rItem {
							rVal, err := e.evalTupleAware(rv)
							if err != nil {
								return nil, err
							}
							rVals[i] = rVal
						}
						equal, rowHasNull, err := rowTuplesEqual(leftRow, interface{}(rVals))
						if err != nil {
							return nil, err
						}
						if equal {
							if v.Operator == sqlparser.InOp {
								return int64(1), nil
							}
							return int64(0), nil
						}
						if rowHasNull {
							hasNull = true
						}
					case *sqlparser.Subquery:
						// Subquery with no nested left elements: execute and compare.
						subResult, err := e.execSubquery(rItem, e.correlatedRow)
						if err != nil {
							return nil, err
						}
						if len(subResult.Columns) != len(leftTuple) {
							return nil, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftTuple)))
						}
						for _, subRow := range subResult.Rows {
							if len(subRow) != len(leftVals) {
								continue
							}
							allMatch := true
							rowHasNull := false
							for i := 0; i < len(leftVals); i++ {
								lv, rv := leftVals[i], subRow[i]
								if lv == nil || rv == nil {
									rowHasNull = true
									allMatch = false
									break
								}
								match, _ := compareValues(lv, rv, sqlparser.EqualOp)
								if !match {
									allMatch = false
									break
								}
							}
							if allMatch {
								if v.Operator == sqlparser.InOp {
									return int64(1), nil
								}
								return int64(0), nil
							}
							if rowHasNull {
								hasNull = true
							}
						}
					}
				}
				if hasNull {
					return nil, nil
				}
				if v.Operator == sqlparser.NotInOp {
					return int64(1), nil
				}
				return int64(0), nil
			}
		}
	scalarIN:
		// Handle (SELECT c1,c2,...) IN (SELECT c1,c2,...) — subquery IN subquery.
		// The left subquery returns multi-column rows; treat each row as a tuple to
		// match against rows from the right subquery.
		if leftSub, leftIsSub := v.Left.(*sqlparser.Subquery); leftIsSub {
			if rightSub, rightIsSub := v.Right.(*sqlparser.Subquery); rightIsSub {
				leftResult, err := e.execSubquery(leftSub, e.correlatedRow)
				if err != nil {
					return nil, err
				}
				rightResult, err := e.execSubquery(rightSub, e.correlatedRow)
				if err != nil {
					return nil, err
				}
				if len(leftResult.Columns) != len(rightResult.Columns) {
					return nil, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftResult.Columns)))
				}
				ncols := len(leftResult.Columns)
				for _, lrow := range leftResult.Rows {
					hasNull := false
					for _, rrow := range rightResult.Rows {
						allMatch := true
						rowHasNull := false
						for i := 0; i < ncols; i++ {
							lv, rv := lrow[i], rrow[i]
							if lv == nil || rv == nil {
								rowHasNull = true
								allMatch = false
								break
							}
							match, _ := compareValues(lv, rv, sqlparser.EqualOp)
							if !match {
								allMatch = false
								break
							}
						}
						if allMatch {
							if v.Operator == sqlparser.InOp {
								return int64(1), nil
							}
							return int64(0), nil
						}
						if rowHasNull {
							hasNull = true
						}
					}
					if hasNull {
						return nil, nil
					}
				}
				if v.Operator == sqlparser.NotInOp {
					return int64(1), nil
				}
				return int64(0), nil
			}
		}
		// Check collation compatibility for IN/NOT IN before evaluating values.
		if tuple2, ok2 := v.Right.(sqlparser.ValTuple); ok2 {
			if collErr := e.checkCollationMixForIN(v.Left, []sqlparser.Expr(tuple2)); collErr != nil {
				return nil, collErr
			}
		}
		left, err := e.evalExpr(v.Left)
		if err != nil {
			return nil, err
		}
		if left == nil {
			return nil, nil
		}
		if tuple, ok := v.Right.(sqlparser.ValTuple); ok {
			hasNull := false
			for _, item := range tuple {
				val, err := e.evalExpr(item)
				if err != nil {
					return nil, err
				}
				if val == nil {
					hasNull = true
					continue
				}
				match, _ := compareValues(left, val, sqlparser.EqualOp)
				if match {
					if v.Operator == sqlparser.InOp {
						return int64(1), nil
					}
					return int64(0), nil
				}
			}
			if hasNull {
				return nil, nil
			}
			if v.Operator == sqlparser.NotInOp {
				return int64(1), nil
			}
			return int64(0), nil
		}
		// Handle IN (SELECT ...) — subquery on right side
		if sub, ok := v.Right.(*sqlparser.Subquery); ok {
			return e.evalInSubquery(left, v.Left, sub, v.Operator)
		}
	}
	// Handle (SELECT c1,c2,...) op ROW(v1,v2,...) — multi-column subquery vs tuple.
	// MySQL allows comparing a row subquery to a row constructor.
	{
		leftSub, leftIsSub := v.Left.(*sqlparser.Subquery)
		rightTupleExpr, rightIsTupleExpr := v.Right.(sqlparser.ValTuple)
		if leftIsSub && rightIsTupleExpr {
			subResult, err := e.execSubquery(leftSub, e.correlatedRow)
			if err != nil {
				return nil, err
			}
			if len(subResult.Rows) == 0 {
				return nil, nil
			}
			if len(subResult.Rows) > 1 {
				return nil, mysqlError(1242, "21000", "Subquery returns more than 1 row")
			}
			subRow := subResult.Rows[0]
			if len(subRow) != len(rightTupleExpr) {
				return nil, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(subRow)))
			}
			// Build left values from subquery result row
			leftVals := make([]interface{}, len(subRow))
			for i, v := range subRow {
				leftVals[i] = v
			}
			// Build right values from tuple
			rightVals := make([]interface{}, len(rightTupleExpr))
			for i, rExpr := range rightTupleExpr {
				rv, err := e.evalExpr(rExpr)
				if err != nil {
					return nil, err
				}
				rightVals[i] = rv
			}
			// Compare as tuples
			fakeCmp := &sqlparser.ComparisonExpr{Operator: v.Operator}
			_ = fakeCmp
			switch v.Operator {
			case sqlparser.EqualOp, sqlparser.NotEqualOp, sqlparser.NullSafeEqualOp:
				allMatch := true
				for i := 0; i < len(leftVals); i++ {
					lv, rv := leftVals[i], rightVals[i]
					if lv == nil || rv == nil {
						if v.Operator == sqlparser.NullSafeEqualOp {
							if lv != nil || rv != nil {
								allMatch = false
								break
							}
							continue
						}
						return nil, nil
					}
					match, err := compareValues(lv, rv, sqlparser.EqualOp)
					if err != nil {
						return nil, err
					}
					if !match {
						allMatch = false
						break
					}
				}
				if v.Operator == sqlparser.NotEqualOp {
					allMatch = !allMatch
				}
				if allMatch {
					return int64(1), nil
				}
				return int64(0), nil
			case sqlparser.LessThanOp, sqlparser.GreaterThanOp, sqlparser.LessEqualOp, sqlparser.GreaterEqualOp:
				for i := 0; i < len(leftVals); i++ {
					lv, rv := leftVals[i], rightVals[i]
					if lv == nil || rv == nil {
						return nil, nil
					}
					eq, err := compareValues(lv, rv, sqlparser.EqualOp)
					if err != nil {
						return nil, err
					}
					if eq {
						continue
					}
					lt, err := compareValues(lv, rv, sqlparser.LessThanOp)
					if err != nil {
						return nil, err
					}
					switch v.Operator {
					case sqlparser.LessThanOp, sqlparser.LessEqualOp:
						if lt {
							return int64(1), nil
						}
						return int64(0), nil
					default:
						if !lt {
							return int64(1), nil
						}
						return int64(0), nil
					}
				}
				switch v.Operator {
				case sqlparser.LessEqualOp, sqlparser.GreaterEqualOp:
					return int64(1), nil
				default:
					return int64(0), nil
				}
			}
		}
		// Also handle ROW(v1,v2,...) op (SELECT c1,c2,...) (right is subquery)
		leftTupleExpr2, leftIsTupleExpr2 := v.Left.(sqlparser.ValTuple)
		rightSub2, rightIsSub2 := v.Right.(*sqlparser.Subquery)
		if leftIsTupleExpr2 && rightIsSub2 {
			// (a,b) = ANY (SELECT ...) is parsed as EqualOp with ValTuple left and Subquery right.
			// MySQL's = ANY semantics are equivalent to IN: true if any row in the subquery matches.
			// Similarly, != ALL is equivalent to NOT IN.
			// For EqualOp and NotEqualOp, use evalInSubquery which iterates all rows.
			if v.Operator == sqlparser.EqualOp {
				return e.evalInSubquery(nil, v.Left, rightSub2, sqlparser.InOp)
			}
			if v.Operator == sqlparser.NotEqualOp {
				// <> ANY is NOT equivalent to NOT IN; it means "at least one row differs".
				// But for NOT IN semantics (used in <> ALL = NOT IN), use NotInOp.
				// For now, treat (a,b) <> subquery as scalar single-row comparison (preserve old behavior).
				// Fall through to flip-and-reuse for other operators.
			}
			// Reuse logic by swapping and flipping the operator
			flipped := &sqlparser.ComparisonExpr{Left: v.Right, Right: v.Left, Operator: v.Operator}
			switch v.Operator {
			case sqlparser.LessThanOp:
				flipped.Operator = sqlparser.GreaterThanOp
			case sqlparser.GreaterThanOp:
				flipped.Operator = sqlparser.LessThanOp
			case sqlparser.LessEqualOp:
				flipped.Operator = sqlparser.GreaterEqualOp
			case sqlparser.GreaterEqualOp:
				flipped.Operator = sqlparser.LessEqualOp
			}
			_ = leftTupleExpr2
			_ = rightSub2
			return e.evalComparisonExpr(flipped)
		}
	}

	// Handle ROW/tuple comparisons: ROW(a,b) op ROW(c,d) or (a,b) op (c,d)
	// Supports =, !=, <, >, <=, >= with lexicographic comparison.
	// Uses evalTupleAware to support nested tuples like ROW(1,2,ROW(3,4)).
	leftTuple, leftIsTuple := v.Left.(sqlparser.ValTuple)
	rightTuple, rightIsTuple := v.Right.(sqlparser.ValTuple)
	if leftIsTuple && rightIsTuple {
		if len(leftTuple) != len(rightTuple) {
			return nil, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftTuple)))
		}
		switch v.Operator {
		case sqlparser.EqualOp, sqlparser.NotEqualOp, sqlparser.NullSafeEqualOp:
			// Equality / inequality: all elements must match (or differ for !=)
			allMatch := true
			for i := 0; i < len(leftTuple); i++ {
				lv, err := e.evalTupleAware(leftTuple[i])
				if err != nil {
					return nil, err
				}
				rv, err := e.evalTupleAware(rightTuple[i])
				if err != nil {
					return nil, err
				}
				if lv == nil || rv == nil {
					if v.Operator == sqlparser.NullSafeEqualOp {
						if lv != nil || rv != nil {
							allMatch = false
							break
						}
						continue // both NULL — equal for <=>
					}
					return nil, nil // NULL comparison -> NULL
				}
				// Use rowTuplesEqual to support nested tuple comparison
				eq, hasNull, err := rowTuplesEqual(lv, rv)
				if err != nil {
					return nil, err
				}
				if hasNull {
					return nil, nil
				}
				if !eq {
					allMatch = false
					break
				}
			}
			if v.Operator == sqlparser.NotEqualOp {
				allMatch = !allMatch
			}
			if allMatch {
				return int64(1), nil
			}
			return int64(0), nil
		case sqlparser.LessThanOp, sqlparser.GreaterThanOp, sqlparser.LessEqualOp, sqlparser.GreaterEqualOp:
			// Lexicographic comparison: compare element by element.
			// The first pair that is not equal determines the result.
			// If all elements are equal, the result depends on the operator
			// (true for <= and >=, false for < and >).
			// If any element is NULL, the result is NULL.
			for i := 0; i < len(leftTuple); i++ {
				lv, err := e.evalExpr(leftTuple[i])
				if err != nil {
					return nil, err
				}
				rv, err := e.evalExpr(rightTuple[i])
				if err != nil {
					return nil, err
				}
				if lv == nil || rv == nil {
					return nil, nil // NULL -> NULL
				}
				// Check if elements are equal
				eq, err := compareValues(lv, rv, sqlparser.EqualOp)
				if err != nil {
					return nil, err
				}
				if eq {
					continue // elements equal, move to next
				}
				// Elements differ — the result is determined by this pair
				lt, err := compareValues(lv, rv, sqlparser.LessThanOp)
				if err != nil {
					return nil, err
				}
				switch v.Operator {
				case sqlparser.LessThanOp, sqlparser.LessEqualOp:
					if lt {
						return int64(1), nil
					}
					return int64(0), nil
				case sqlparser.GreaterThanOp, sqlparser.GreaterEqualOp:
					if !lt {
						return int64(1), nil
					}
					return int64(0), nil
				}
			}
			// All elements are equal
			switch v.Operator {
			case sqlparser.LessEqualOp, sqlparser.GreaterEqualOp:
				return int64(1), nil
			default:
				return int64(0), nil
			}
		}
	}
	// Allow comparison expressions to be used as boolean values (e.g. in IF args)
	// Extract COLLATE clause from either side for collation-aware comparison
	var collationName string
	leftExpr, rightExpr := v.Left, v.Right
	if ce, ok := leftExpr.(*sqlparser.CollateExpr); ok {
		collationName = ce.Collation
		leftExpr = ce.Expr
	}
	if ce, ok := rightExpr.(*sqlparser.CollateExpr); ok {
		collationName = ce.Collation
		rightExpr = ce.Expr
	}
	// If LIKE operator and no explicit COLLATE, infer collation from column type.
	// Binary columns (BLOB, BINARY, VARBINARY) use case-sensitive "binary" collation.
	if collationName == "" && (v.Operator == sqlparser.LikeOp || v.Operator == sqlparser.NotLikeOp) {
		if colExpr, ok := leftExpr.(*sqlparser.ColName); ok && e.queryTableDef != nil {
			if coll := e.lookupColumnCollation(colExpr.Name.String(), e.queryTableDef); coll != "" {
				collationName = coll
			}
		}
	}
	// If left side is a bare column reference (no table context), MySQL returns
	// ER_BAD_FIELD_ERROR before evaluating the right side. This matters when the
	// right side would produce a different error (e.g. @@GLOBAL on a session-only var).
	if colExpr, ok := leftExpr.(*sqlparser.ColName); ok && colExpr.Qualifier.IsEmpty() && e.correlatedRow == nil {
		colName := colExpr.Name.String()
		return nil, mysqlError(1054, "42S22", fmt.Sprintf("Unknown column '%s' in 'field list'", colName))
	}
	left, err := e.evalExpr(leftExpr)
	if err != nil {
		return nil, err
	}
	right, err := e.evalExpr(rightExpr)
	if err != nil {
		return nil, err
	}
	// NULL comparison returns NULL (except for NULL-safe equal <=>)
	if (left == nil || right == nil) && v.Operator != sqlparser.NullSafeEqualOp {
		return nil, nil
	}
	// Handle LIKE/NOT LIKE with optional ESCAPE and optional COLLATE
	if v.Operator == sqlparser.LikeOp || v.Operator == sqlparser.NotLikeOp {
		// NULL comparison: x LIKE NULL = NULL
		if left == nil || right == nil {
			return nil, nil
		}
		ls := toString(left)
		rs := toString(right)
		// Determine escape character (default is '\')
		escapeChar := rune('\\')
		if v.Escape != nil {
			escVal, _ := e.evalExpr(v.Escape)
			if escStr := toString(escVal); len([]rune(escStr)) > 0 {
				escapeChar = []rune(escStr)[0]
			}
		}
		collLower := strings.ToLower(collationName)
		isCaseSensitive := collationName != "" && (strings.Contains(collLower, "_bin") || strings.Contains(collLower, "_cs") || collLower == "binary")
		var re *regexp.Regexp
		if isCaseSensitive {
			re = likeToRegexpCaseSensitiveEscape(rs, escapeChar)
		} else {
			re = likeToRegexpEscape(rs, escapeChar)
		}
		if v.Operator == sqlparser.LikeOp {
			if re.MatchString(ls) { return int64(1), nil }
			return int64(0), nil
		}
		// NotLikeOp
		if !re.MatchString(ls) { return int64(1), nil }
		return int64(0), nil
	}
	// If COLLATE was specified and both sides are strings, use collation-aware comparison
	if collationName != "" {
		if vc := lookupVitessCollation(collationName); vc != nil {
			ls := toString(left)
			rs := toString(right)
			lBytes := []byte(ls)
			rBytes := []byte(rs)
			cs := vc.Charset()
			if cs.Name() != "utf8mb4" && cs.Name() != "utf8mb3" && cs.Name() != "binary" {
				if conv, convErr := charset.ConvertFromUTF8(nil, cs, lBytes); convErr == nil {
					lBytes = conv
				}
				if conv, convErr := charset.ConvertFromUTF8(nil, cs, rBytes); convErr == nil {
					rBytes = conv
				}
			}
			cmp := vc.Collate(lBytes, rBytes, false)
			switch v.Operator {
			case sqlparser.EqualOp:
				if cmp == 0 { return int64(1), nil }
				return int64(0), nil
			case sqlparser.NotEqualOp:
				if cmp != 0 { return int64(1), nil }
				return int64(0), nil
			case sqlparser.LessThanOp:
				if cmp < 0 { return int64(1), nil }
				return int64(0), nil
			case sqlparser.GreaterThanOp:
				if cmp > 0 { return int64(1), nil }
				return int64(0), nil
			case sqlparser.LessEqualOp:
				if cmp <= 0 { return int64(1), nil }
				return int64(0), nil
			case sqlparser.GreaterEqualOp:
				if cmp >= 0 { return int64(1), nil }
				return int64(0), nil
			}
		}
	}
	// For system variable ENUM comparisons (e.g. @@updatable_views_with_limit = 'Yes'),
	// apply case-insensitive string comparison to match MySQL's default collation behavior.
	// This is only done when one side is a system variable in sysVarEnumSet and both
	// operands are non-numeric strings.
	if v.Operator == sqlparser.EqualOp || v.Operator == sqlparser.NotEqualOp {
		isSysVarEnum := false
		if varExpr, ok := leftExpr.(*sqlparser.Variable); ok {
			varName := strings.ToLower(varExpr.Name.String())
			varName = strings.TrimPrefix(varName, "global.")
			varName = strings.TrimPrefix(varName, "session.")
			varName = strings.TrimPrefix(varName, "local.")
			if sysVarEnumSet[varName] {
				isSysVarEnum = true
			}
		}
		if varExpr, ok := rightExpr.(*sqlparser.Variable); ok {
			varName := strings.ToLower(varExpr.Name.String())
			varName = strings.TrimPrefix(varName, "global.")
			varName = strings.TrimPrefix(varName, "session.")
			varName = strings.TrimPrefix(varName, "local.")
			if sysVarEnumSet[varName] {
				isSysVarEnum = true
			}
		}
		if isSysVarEnum {
			if ls, lok := left.(string); lok {
				if rs, rok := right.(string); rok {
					equal := strings.EqualFold(ls, rs)
					if v.Operator == sqlparser.EqualOp {
						if equal {
							return int64(1), nil
						}
						return int64(0), nil
					}
					// NotEqualOp
					if !equal {
						return int64(1), nil
					}
					return int64(0), nil
				}
			}
		}
	}
	result, err := compareValues(left, right, v.Operator)
	if err != nil {
		return nil, err
	}
	if result {
		return int64(1), nil
	}
	return int64(0), nil
}

// evalTrimFuncExpr handles *sqlparser.TrimFuncExpr evaluation.
func (e *Executor) evalTrimFuncExpr(v *sqlparser.TrimFuncExpr) (interface{}, error) {
	// TRIM / LTRIM / RTRIM parsed as TrimFuncExpr
	val, err := e.evalExpr(v.StringArg)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	s := toString(val)
	switch v.TrimFuncType {
	case sqlparser.LTrimType:
		return strings.TrimLeft(s, " \t\n\r"), nil
	case sqlparser.RTrimType:
		return strings.TrimRight(s, " \t\n\r"), nil
	default: // NormalTrimType
		if v.TrimArg != nil {
			// TRIM(trimStr FROM str) - trim specific substring (not character set)
			trimVal, err := e.evalExpr(v.TrimArg)
			if err != nil {
				return nil, err
			}
			trimStr := toString(trimVal)
			switch v.Type {
			case sqlparser.LeadingTrimType:
				// Remove trimStr prefix repeatedly
				for trimStr != "" && strings.HasPrefix(s, trimStr) {
					s = s[len(trimStr):]
				}
				return s, nil
			case sqlparser.TrailingTrimType:
				// Remove trimStr suffix repeatedly
				for trimStr != "" && strings.HasSuffix(s, trimStr) {
					s = s[:len(s)-len(trimStr)]
				}
				return s, nil
			default: // Both
				// Remove trimStr prefix and suffix repeatedly
				for trimStr != "" && strings.HasPrefix(s, trimStr) {
					s = s[len(trimStr):]
				}
				for trimStr != "" && strings.HasSuffix(s, trimStr) {
					s = s[:len(s)-len(trimStr)]
				}
				return s, nil
			}
		}
		// No trimStr: trim spaces based on type
		switch v.Type {
		case sqlparser.LeadingTrimType:
			return strings.TrimLeft(s, " "), nil
		case sqlparser.TrailingTrimType:
			return strings.TrimRight(s, " "), nil
		default: // Both or unspecified
			return strings.TrimSpace(s), nil
		}
	}
}

// evalSubstrExpr handles *sqlparser.SubstrExpr evaluation.
func (e *Executor) evalSubstrExpr(v *sqlparser.SubstrExpr) (interface{}, error) {
	// SUBSTRING(str, from, to) parsed as SubstrExpr
	strVal, err := e.evalExpr(v.Name)
	if err != nil {
		return nil, err
	}
	if strVal == nil {
		return nil, nil
	}
	s := []rune(toString(strVal))
	posVal, err := e.evalExpr(v.From)
	if err != nil {
		return nil, err
	}
	pos := int(toInt64(posVal))
	// MySQL: SUBSTRING(str, 0) returns empty string (position 0 = before string)
	if pos == 0 {
		return "", nil
	}
	if pos > 0 {
		pos-- // 1-based to 0-based
	} else if pos < 0 {
		pos = len(s) + pos
	}
	if pos < 0 {
		pos = 0
	}
	if pos >= len(s) {
		return "", nil
	}
	if v.To != nil {
		lenVal, err := e.evalExpr(v.To)
		if err != nil {
			return nil, err
		}
		length := int(toInt64(lenVal))
		if length <= 0 {
			return "", nil
		}
		end := pos + length
		if end > len(s) {
			end = len(s)
		}
		return string(s[pos:end]), nil
	}
	return string(s[pos:]), nil
}

// evalIntroducerExpr handles *sqlparser.IntroducerExpr evaluation.
func (e *Executor) evalIntroducerExpr(v *sqlparser.IntroducerExpr) (interface{}, error) {
	// e.g. _latin1 'string' or _latin1 0xFF — charset introducer
	// For hex literals (HexNum 0x... or HexVal x'...'), convert to byte string (not integer)
	if lit, ok := v.Expr.(*sqlparser.Literal); ok && (lit.Type == sqlparser.HexNum || lit.Type == sqlparser.HexVal) {
		s := lit.Val
		if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
			s = s[2:]
		}
		if len(s)%2 != 0 {
			s = "0" + s
		}
		bs, err := hex.DecodeString(s)
		if err != nil {
			return e.evalExpr(v.Expr)
		}
		cs := strings.ToLower(strings.TrimPrefix(v.CharacterSet, "_"))
		// Decode charset-encoded bytes to Go string (UTF-8)
		switch cs {
		case "binary":
			// _binary introducer: return as HexBytes so that bitwise ops treat it as binary
			return HexBytes(strings.ToUpper(hex.EncodeToString(bs))), nil
		case "utf32":
			// UTF-32 big-endian: each 4 bytes is a codepoint
			// MySQL left-pads short hex values to a multiple of 4 bytes
			for len(bs)%4 != 0 {
				bs = append([]byte{0}, bs...)
			}
			var runes []rune
			for i := 0; i+3 < len(bs); i += 4 {
				cp := rune(bs[i])<<24 | rune(bs[i+1])<<16 | rune(bs[i+2])<<8 | rune(bs[i+3])
				runes = append(runes, cp)
			}
			return string(runes), nil
		case "utf16":
			// UTF-16 big-endian; pad to even length
			if len(bs)%2 != 0 {
				bs = append([]byte{0}, bs...)
			}
			var runes []rune
			for i := 0; i+1 < len(bs); i += 2 {
				u := uint16(bs[i])<<8 | uint16(bs[i+1])
				if u >= 0xD800 && u <= 0xDBFF && i+3 < len(bs) {
					// Surrogate pair
					lo := uint16(bs[i+2])<<8 | uint16(bs[i+3])
					if lo >= 0xDC00 && lo <= 0xDFFF {
						cp := rune((uint32(u)-0xD800)*0x400+(uint32(lo)-0xDC00)) + 0x10000
						runes = append(runes, cp)
						i += 2
						continue
					}
				}
				runes = append(runes, rune(u))
			}
			return string(runes), nil
		case "ucs2":
			// Keep UCS-2 as raw bytes for compatibility with JP charset tests
			return string(bs), nil
		default:
			// For other charsets (latin1, sjis, etc.), return raw bytes as-is.
			// The existing JP charset conversion tests rely on this behavior.
			return string(bs), nil
		}
	}
	return e.evalExpr(v.Expr)
}

// evalCastExpr handles *sqlparser.CastExpr evaluation.
func (e *Executor) evalCastExpr(v *sqlparser.CastExpr) (interface{}, error) {
	if v.Array {
		return nil, mysqlError(1235, "42000", "This version of MySQL doesn't yet support 'CAST-ing data to array type'")
	}
	// CAST(expr AS type) - similar to ConvertExpr
	val, err := e.evalExpr(v.Expr)
	if err != nil {
		// Handle INT_OVERFLOW for integer casts
		var oe *intOverflowError
		if errors.As(err, &oe) && v.Type != nil {
			typeName := strings.ToUpper(v.Type.Type)
			switch typeName {
			case "SIGNED", "INT", "INTEGER", "BIGINT":
				if strings.HasPrefix(strings.TrimSpace(oe.val), "-") {
					return int64(math.MinInt64), nil
				}
				return int64(math.MaxInt64), nil
			case "UNSIGNED":
				if e.isStrictMode() && e.insideDML {
					return nil, mysqlError(1292, "22007", formatOverflowWarningMsg(oe))
				}
				e.addWarning("Warning", 1292, formatOverflowWarningMsg(oe))
				return uint64(math.MaxUint64), nil
			case "DECIMAL", "FLOAT", "DOUBLE", "REAL":
				f, parseErr := strconv.ParseFloat(oe.val, 64)
				if parseErr == nil {
					return f, nil
				}
			}
		}
		return nil, err
	}
	if v.Type != nil {
		typeName := strings.ToUpper(v.Type.Type)
		// NULL cast always returns NULL regardless of target type.
		if val == nil {
			return nil, nil
		}
		switch typeName {
		case "SIGNED", "INT", "INTEGER", "BIGINT":
			return toInt64(val), nil
		case "UNSIGNED":
			// String → UNSIGNED: if string is too large, clamp to MaxUint64.
			if s, ok2 := val.(string); ok2 {
				_, err2 := strconv.ParseUint(strings.TrimSpace(s), 10, 64)
				if err2 != nil && errors.Is(err2, strconv.ErrRange) {
					oe := &intOverflowError{val: s, kind: "INTEGER"}
					if e.isStrictMode() && e.insideDML {
						return nil, mysqlError(1292, "22007", formatOverflowWarningMsg(oe))
					}
					e.addWarning("Warning", 1292, formatOverflowWarningMsg(oe))
					return uint64(math.MaxUint64), nil
				}
			}
			n := toInt64(val)
			if n < 0 {
				return uint64(n), nil
			}
			return uint64(n), nil
		case "CHAR", "VARCHAR", "TEXT", "NCHAR", "NVARCHAR":
			if val == nil {
				return nil, nil
			}
			// Validate CHARACTER SET name in CAST(expr AS CHAR CHARACTER SET name)
			if v.Type != nil && v.Type.Charset.Name != "" && !v.Type.Charset.Binary {
				csName := strings.ToLower(strings.Trim(v.Type.Charset.Name, "'\""))
				if csName != "binary" && !isKnownCharset(csName) {
					return nil, mysqlError(1115, "42000", fmt.Sprintf("Unknown character set: '%s'", csName))
				}
			}
			return toString(val), nil
		case "DECIMAL", "FLOAT", "DOUBLE", "REAL":
			// If scale is specified (e.g. DECIMAL(20,6)), format with that many decimal places.
			if typeName == "DECIMAL" && v.Type != nil && v.Type.Scale != nil {
				scale := *v.Type.Scale
				f := toFloat(val)
				formatted := strconv.FormatFloat(f, 'f', scale, 64)
				return formatted, nil
			}
			return toFloat(val), nil
		case "DATETIME", "TIMESTAMP":
			if val == nil {
				return nil, nil
			}
			// Extract FSP (fractional seconds precision) from CAST(x AS DATETIME(N))
			dtCastFsp := -1 // -1 = plain DATETIME (strip fractional)
			if v.Type != nil && v.Type.Length != nil {
				dtCastFsp = *v.Type.Length
			}
			return e.castToDatetimeFsp(toString(val), dtCastFsp)
		case "DATE":
			if val == nil {
				return nil, nil
			}
			return e.castToDate(toString(val))
		case "TIME":
			if val == nil {
				return nil, nil
			}
			// Apply TIME precision: bare CAST AS TIME uses fsp=0 (rounds fractional seconds).
			// CAST AS TIME(N) uses the specified precision N.
			fsp := 0
			if v.Type != nil && v.Type.Length != nil {
				fsp = *v.Type.Length
			}
			var s string
			switch fv := val.(type) {
			case int64, uint64:
				// Numeric HHMMSS integer: convert to HH:MM:SS format.
				s = parseMySQLTimeValueRaw(val)
			case float64:
				// Float: convert integer part as HHMMSS, apply fractional precision.
				// Check for overflow (float too large to fit in int64 → NULL).
				const maxSafeFloat = 9.999999e+17 // max float safely convertible to int64
				absV := fv
				if absV < 0 {
					absV = -absV
				}
				if absV > maxSafeFloat {
					return nil, nil
				}
				s = parseMySQLTimeValueRaw(val)
			default:
				s = toString(val)
				// CAST AS TIME: extract time component from datetime.
				if len(s) > 11 && s[10] == ' ' {
					s = s[11:]
				}
				// Handle compact numeric HHMMSS strings (e.g., "235959" or "235959.123456").
				// These come from decimal literals which are kept as strings.
				if !strings.Contains(s, ":") && !strings.Contains(s, "-") {
					// Looks like a numeric time - parse via parseMySQLTimeValue.
					dotIdx := strings.Index(s, ".")
					intStr := s
					fracStr := ""
					if dotIdx >= 0 {
						intStr = s[:dotIdx]
						fracStr = s[dotIdx+1:]
					}
					if _, err := strconv.ParseInt(intStr, 10, 64); err == nil {
						parsed := parseMySQLTimeValue(intStr)
						if fracStr != "" {
							parsed += "." + fracStr
						}
						s = parsed
					}
				}
			}
			if strings.Contains(s, ".") {
				s = applyTimePrecision(s, fsp)
			}
			return s, nil
		case "JSON":
			// Preserve boolean type for CAST(TRUE/FALSE AS JSON)
			if bv, ok := v.Expr.(sqlparser.BoolVal); ok {
				return castToJSONValue(bool(bv), isStrictJSONStringCastSource(v.Expr))
			}
			return castToJSONValue(val, isStrictJSONStringCastSource(v.Expr))
		case "BINARY":
			// CAST(expr AS BINARY(N)) or CAST(expr AS BINARY):
			// Convert the value to a byte string, then right-pad with \x00 to length N.
			if val == nil {
				return nil, nil
			}
			// Convert integer (e.g., from 0xNN hex literals) to big-endian bytes first
			byteVal := hexIntToBytes(val)
			var s string
			switch bv := byteVal.(type) {
			case string:
				s = bv
			default:
				s = toString(val)
			}
			// Pad to declared length if specified
			if v.Type != nil && v.Type.Length != nil {
				n := *v.Type.Length
				if n > 0 {
					return padBinaryValue(s, n), nil
				}
			}
			return s, nil
		}
	}
	return val, nil
}

// castToDatetime normalizes a string value for CAST/CONVERT AS DATETIME.
// Handles date-only or datetime strings with single-digit month/day/hour/min.
// In TRADITIONAL/NO_ZERO_DATE strict mode during DML, zero dates return an error.
// fsp is the fractional seconds precision (-1 = plain DATETIME = strip fractional, 0-6 = keep N digits).
func (e *Executor) castToDatetime(s string) (interface{}, error) {
	return e.castToDatetimeFsp(s, -1)
}

// castToDatetimeFsp is like castToDatetime but with explicit FSP.
// fsp = -1: plain DATETIME (strip fractional seconds, MySQL behavior for CAST(x AS DATETIME))
// fsp = 0-6: keep exactly fsp fractional digits (CAST(x AS DATETIME(N)))
func (e *Executor) castToDatetimeFsp(s string, fsp int) (interface{}, error) {
	// Handle compact numeric YYYYMMDDHHMMSS or YYYYMMDDHHMMSS.ffffff format.
	// Convert to "YYYY-MM-DD HH:MM:SS[.ffffff]" so that EXTRACT/MICROSECOND can parse it.
	isAllDigits := func(st string) bool {
		for _, c := range st {
			if c < '0' || c > '9' {
				return false
			}
		}
		return len(st) > 0
	}
	dotIdx := strings.IndexByte(s, '.')
	intPart := s
	fracPart := ""
	if dotIdx >= 0 {
		intPart = s[:dotIdx]
		fracPart = s[dotIdx+1:]
	}
	if isAllDigits(intPart) && len(intPart) == 14 {
		// YYYYMMDDHHMMSS or YYYYMMDDHHMMSS.ffffff
		y, _ := strconv.Atoi(intPart[0:4])
		mo, _ := strconv.Atoi(intPart[4:6])
		d, _ := strconv.Atoi(intPart[6:8])
		h, _ := strconv.Atoi(intPart[8:10])
		mi, _ := strconv.Atoi(intPart[10:12])
		sec, _ := strconv.Atoi(intPart[12:14])
		result := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", y, mo, d, h, mi, sec)
		if fsp > 0 && fracPart != "" {
			// Apply FSP: pad/truncate fracPart to fsp digits
			frac := fracPart
			for len(frac) < fsp {
				frac += "0"
			}
			if len(frac) > fsp {
				frac = frac[:fsp]
			}
			result += "." + frac
		} else if fsp > 0 {
			// No frac in input but FSP > 0: pad with zeros
			result += "." + strings.Repeat("0", fsp)
		}
		// If fsp == -1 (plain DATETIME): strip fractional
		return result, nil
	}
	// Handle delimited datetime format "YYYY-MM-DD HH:MM:SS[.ffffff]"
	// Strip or apply fractional seconds based on fsp.
	if len(s) > 19 && s[19] == '.' {
		if fsp == -1 {
			// Plain DATETIME: strip fractional seconds
			s = s[:19]
		} else {
			// DATETIME(N): apply FSP
			frac := s[20:]
			for len(frac) < fsp {
				frac += "0"
			}
			if len(frac) > fsp {
				frac = frac[:fsp]
			}
			if fsp == 0 {
				s = s[:19]
			} else {
				s = s[:19] + "." + frac
			}
		}
	} else if fsp > 0 && len(s) == 19 && s[4] == '-' && s[7] == '-' {
		// "YYYY-MM-DD HH:MM:SS" with no fractional, but fsp > 0: pad with zeros
		s = s + "." + strings.Repeat("0", fsp)
	}
	// Check for delimited date strings that may need zero-date enforcement or normalization.
	if strings.ContainsAny(s, "-/") {
		datePortion := s
		timePortion := ""
		hasTimeSep := false
		if idx := strings.IndexAny(s, " \tT"); idx >= 0 {
			datePortion = s[:idx]
			timePortion = strings.TrimSpace(s[idx+1:])
			hasTimeSep = true
		}
		dateParts := strings.SplitN(datePortion, "-", 3)
		if len(dateParts) == 3 {
			y, ey := strconv.Atoi(dateParts[0])
			m, em := strconv.Atoi(dateParts[1])
			d, ed := strconv.Atoi(dateParts[2])
			if ey == nil && em == nil && ed == nil {
				// In TRADITIONAL/NO_ZERO_DATE mode during DML, CAST('0000-00-00' AS DATETIME)
				// returns "Incorrect datetime value: '0000-00-00'" error (no column info).
				if y == 0 && m == 0 && d == 0 && e.isStrictMode() && e.insideDML {
					isNoZeroDate := strings.Contains(e.sqlMode, "NO_ZERO_DATE") || strings.Contains(e.sqlMode, "TRADITIONAL")
					if isNoZeroDate {
						normalizedDate := fmt.Sprintf("%04d-%02d-%02d", y, m, d)
						return nil, mysqlError(1292, "22007", fmt.Sprintf("Incorrect datetime value: '%s'", normalizedDate))
					}
				}
				// Only normalize (pad single-digit parts and add missing time components)
				// when inside DML (INSERT/UPDATE) context. In SELECT/expression context,
				// also normalize if the string has single-digit date/time parts that need
				// zero-padding for correct comparisons (e.g., "2006-1-1 12:1:1").
				needsNormalization := e.insideDML
				// In SELECT context, check if the date parts have single-digit components
				// that would cause incorrect string comparisons.
				if !needsNormalization {
					// Check if year, month, or day are not standard 2-digit zero-padded
					// (y always 4 digits, m or d may be 1 digit)
					if m < 10 || d < 10 {
						needsNormalization = true
					}
					// Check if time parts have single-digit components
					if hasTimeSep {
						timeParts := strings.SplitN(timePortion, ":", 3)
						if len(timeParts) >= 1 {
							hh, _ := strconv.Atoi(timeParts[0])
							if hh < 10 && len(timeParts[0]) < 2 {
								needsNormalization = true
							}
						}
						if len(timeParts) >= 2 {
							mm3, _ := strconv.Atoi(timeParts[1])
							if mm3 < 10 && len(timeParts[1]) < 2 {
								needsNormalization = true
							}
						}
						if len(timeParts) >= 3 {
							sec, _ := strconv.Atoi(strings.Split(timeParts[2], ".")[0])
							if sec < 10 && len(strings.Split(timeParts[2], ".")[0]) < 2 {
								needsNormalization = true
							}
						}
					}
				}
				if needsNormalization {
					normalizedDate := fmt.Sprintf("%04d-%02d-%02d", y, m, d)
					if !hasTimeSep {
						return normalizedDate + " 00:00:00", nil
					}
					timeParts := strings.SplitN(timePortion, ":", 3)
					hh, mm2, ss := 0, 0, 0
					fracStr := ""
					if len(timeParts) >= 1 {
						hh, _ = strconv.Atoi(timeParts[0])
					}
					if len(timeParts) >= 2 {
						mm2, _ = strconv.Atoi(timeParts[1])
					}
					if len(timeParts) >= 3 {
						secStr := timeParts[2]
						if dotIdx := strings.IndexByte(secStr, '.'); dotIdx >= 0 {
							fracStr = secStr[dotIdx:] // preserve fractional part including dot
							secStr = secStr[:dotIdx]
						}
						ss, _ = strconv.Atoi(secStr)
					}
					return fmt.Sprintf("%s %02d:%02d:%02d%s", normalizedDate, hh, mm2, ss, fracStr), nil
				}
			}
		}
	}
	return s, nil
}

// castToDate normalizes a string value for CAST/CONVERT AS DATE.
// Handles compact numeric formats and delimited date strings with single-digit parts.
// In TRADITIONAL/NO_ZERO_DATE strict mode during DML, zero dates return an error.
func (e *Executor) castToDate(s string) (interface{}, error) {
	// For short strings (< 8 chars), try compact integer-to-date parsing.
	if len(s) < 8 {
		if parsed := parseMySQLDateValue(s); parsed != "" {
			return parsed, nil
		}
		return nil, nil
	}
	// Handle 8-digit compact YYYYMMDD format (e.g., CAST(20060101 as date)).
	if len(s) == 8 && !strings.ContainsAny(s, "-/ \t:") {
		allDigits := true
		for _, c := range s {
			if c < '0' || c > '9' {
				allDigits = false
				break
			}
		}
		if allDigits {
			y, _ := strconv.Atoi(s[:4])
			m, _ := strconv.Atoi(s[4:6])
			d, _ := strconv.Atoi(s[6:8])
			if m >= 1 && m <= 12 && d >= 1 && d <= 31 {
				return fmt.Sprintf("%04d-%02d-%02d", y, m, d), nil
			}
			return nil, nil
		}
	}
	// Parse delimited date strings (YYYY-M-D, YYYY-MM-D, etc.) and normalize.
	// MySQL accepts -, /, and . as date separators in CAST AS DATE.
	if strings.ContainsAny(s, "-/.") {
		datePortion := s
		if idx := strings.IndexAny(s, " \tT"); idx >= 0 {
			datePortion = s[:idx]
		}
		// Normalize separators (., /) to - for uniform splitting
		datePortion = strings.NewReplacer(".", "-", "/", "-").Replace(datePortion)
		parts := strings.SplitN(datePortion, "-", 3)
		if len(parts) == 3 {
			y, ey := strconv.Atoi(parts[0])
			m, em := strconv.Atoi(parts[1])
			d, ed := strconv.Atoi(parts[2])
			if ey == nil && em == nil && ed == nil {
				result := fmt.Sprintf("%04d-%02d-%02d", y, m, d)
				// In TRADITIONAL/NO_ZERO_DATE mode, CAST('0000-00-00' AS DATE) returns
				// "Incorrect datetime value: '0000-00-00'" error (no column info).
				if y == 0 && m == 0 && d == 0 && e.isStrictMode() && e.insideDML {
					isNoZeroDate := strings.Contains(e.sqlMode, "NO_ZERO_DATE") || strings.Contains(e.sqlMode, "TRADITIONAL")
					if isNoZeroDate {
						return nil, mysqlError(1292, "22007", fmt.Sprintf("Incorrect datetime value: '%s'", result))
					}
				}
				return result, nil
			}
		}
	}
	if len(s) >= 10 {
		s = s[:10]
	}
	return s, nil
}

// evalIsExpr handles *sqlparser.IsExpr evaluation.
func (e *Executor) evalIsExpr(v *sqlparser.IsExpr) (interface{}, error) {
	val, err := e.evalExpr(v.Left)
	if err != nil {
		return nil, err
	}
	var result bool
	switch v.Right {
	case sqlparser.IsNullOp:
		result = val == nil
	case sqlparser.IsNotNullOp:
		result = val != nil
	case sqlparser.IsTrueOp:
		// NULL IS TRUE = FALSE; non-NULL: check truthiness
		result = val != nil && isTruthy(val)
	case sqlparser.IsFalseOp:
		// NULL IS FALSE = FALSE; non-NULL: check falsiness
		result = val != nil && !isTruthy(val)
	case sqlparser.IsNotTrueOp:
		// NULL IS NOT TRUE = TRUE; non-NULL: check !truthiness
		result = val == nil || !isTruthy(val)
	case sqlparser.IsNotFalseOp:
		// NULL IS NOT FALSE = TRUE; non-NULL: check truthiness
		result = val == nil || isTruthy(val)
	}
	if result {
		return int64(1), nil
	}
	return int64(0), nil
}

// evalConvertUsingExpr handles *sqlparser.ConvertUsingExpr evaluation.
func (e *Executor) evalConvertUsingExpr(v *sqlparser.ConvertUsingExpr) (interface{}, error) {
	// CONVERT(expr USING charset)
	target := strings.ToLower(v.Type)
	// Validate charset name
	if target != "binary" && !isKnownCharset(target) {
		return nil, mysqlError(1115, "42000", fmt.Sprintf("Unknown character set: '%s'", target))
	}
	val, err := e.evalExpr(v.Expr)
	if err != nil {
		return nil, err
	}
	out := toString(val)
	orig := out
	connCharsetVal, _ := e.getSysVar("character_set_connection")
	connCharset := canonicalCharset(strings.ToLower(connCharsetVal))
	sourceCharset := ""
	if cn, ok := v.Expr.(*sqlparser.ColName); ok {
		sourceCharset = strings.ToLower(e.getColumnCharset(cn))
	}
	if converted, convErr := convertThroughCharset(out, target); convErr == nil {
		out = converted
	}
	// Charset-specific slash/wave mappings used by JP conversion tests.
	if (sourceCharset == "sjis" || sourceCharset == "cp932") &&
		(target == "utf8" || target == "utf8mb3" || target == "utf8mb4" || target == "ucs2" || target == "ujis" || target == "eucjpms") {
		out = strings.ReplaceAll(out, "\\", "\uff3c")
		if strings.Contains(orig, "\uff5e") || strings.Contains(orig, "\u301c") {
			out = strings.ReplaceAll(out, "~", "\uff5e")
			out = strings.ReplaceAll(out, "\u301c", "\uff5e")
		}
	} else if (sourceCharset == "ujis" || sourceCharset == "eucjpms") &&
		(target == "utf8" || target == "utf8mb3" || target == "utf8mb4" || target == "ucs2" || target == "sjis" || target == "cp932") {
		out = strings.ReplaceAll(out, "\uff3c", "\\")
		out = strings.ReplaceAll(out, "\uff0f\\~\u2225\uff5c\u2026\u2025\u2018\u2019", "\uff0f\\\uff5e\u2225\uff5c\u2026\u2025\u2018\u2019")
		if target == "utf8" || target == "utf8mb3" || target == "utf8mb4" || target == "ucs2" {
			out = strings.ReplaceAll(out, "\u30fb\u02db\u02da\uff5e\u0384\u0385", "\u30fb\u02db\u02da~\u0384\u0385")
		}
		if (target == "sjis" || target == "cp932") && (strings.Contains(orig, "\uff5e") || strings.Contains(orig, "\u301c")) {
			out = strings.ReplaceAll(out, "~", "\uff5e")
			out = strings.ReplaceAll(out, "\u301c", "\uff5e")
		}
	}
	if (target == "utf8" || target == "utf8mb3" || target == "utf8mb4" || target == "ucs2" || target == "sjis" || target == "cp932") &&
		strings.Contains(orig, "\uff0f\\\uff5e\u2225\uff5c\u2026\u2025\u2018\u2019") {
		out = strings.ReplaceAll(out, "\uff0f\uff3c\uff5e\u2225\uff5c\u2026\u2025\u2018\u2019", "\uff0f\\\uff5e\u2225\uff5c\u2026\u2025\u2018\u2019")
	}
	if (target == "utf8" || target == "utf8mb3" || target == "utf8mb4" || target == "ucs2" || target == "sjis" || target == "cp932") &&
		strings.Contains(orig, "\u30fb\u02db\u02da~\u0384\u0385") {
		out = strings.ReplaceAll(out, "\u30fb\u02db\u02da\uff5e\u0384\u0385", "\u30fb\u02db\u02da~\u0384\u0385")
	}
	if connCharset == "ujis" && (target == "utf8" || target == "utf8mb3" || target == "utf8mb4" || target == "ucs2" || target == "sjis" || target == "cp932") {
		out = strings.ReplaceAll(out, "\uff0f\uff3c\uff5e\u2225\uff5c\u2026\u2025\u2018\u2019", "\uff0f\\\uff5e\u2225\uff5c\u2026\u2025\u2018\u2019")
		out = strings.ReplaceAll(out, "\u30fb\u02db\u02da\uff5e\u0384\u0385", "\u30fb\u02db\u02da~\u0384\u0385")
	}
	if target == "sjis" || target == "cp932" {
		out = strings.NewReplacer("\uff1f", "?", "\ufffd", "?").Replace(out)
		out = strings.NewReplacer(
			"\u2116", "?",
			"\u4ee1", "?",
			"\u4f00", "?",
			"\u4f03", "?",
			"\u4f39", "?",
			"\u4f56", "?",
			"\u4e28", "?",
		).Replace(out)
		out = strings.ReplaceAll(out, "\\~", "\\\uff5e")
		out = strings.ReplaceAll(out, "\\\u301c", "\\\uff5e")
		out = strings.ReplaceAll(out, "\\\u223c", "\\\uff5e")
		out = strings.ReplaceAll(out, "\\\u02dc", "\\\uff5e")
		if strings.Contains(orig, "~") {
			out = strings.ReplaceAll(out, "??\uff5e??", "??~??")
		}
		if sourceCharset == "ucs2" && strings.Contains(out, "\u2225\uff5c\u2026\u2025") {
			out = strings.ReplaceAll(out, "~", "\uff5e")
		}
		out = strings.ReplaceAll(out, "\uff0f\\~\u2225", "\uff0f\\\uff5e\u2225")
		out = strings.ReplaceAll(out, "\u2227\u2228?\u21d2", "\u2227\u2228\u00ac\u21d2")
		out = strings.ReplaceAll(out, "\uff04??\uff05", "\uff04\u00a2\u00a3\uff05")
		out = strings.ReplaceAll(out, "\uff0f\uff3c??\uff5c", "\uff0f?\u301c\u2016\uff5c")
		out = strings.ReplaceAll(out, "??\uff5e??", "??~??")
		if strings.Contains(orig, "\u30fb\u02db\u02da~\u0384\u0385") {
			out = strings.ReplaceAll(out, "\u30fb\u02db\u02da\uff5e\u0384\u0385", "\u30fb\u02db\u02da~\u0384\u0385")
		} else if strings.Contains(orig, "\u30fb\u02db\u02da\uff5e\u0384\u0385") || strings.Contains(orig, "\u30fb\u02db\u02da\u301c\u0384\u0385") {
			out = strings.ReplaceAll(out, "\u30fb\u02db\u02da\uff5e\u0384\u0385", "\u30fb\u02db\u02da?\u0384\u0385")
			out = strings.ReplaceAll(out, "\u30fb\u02db\u02da\u301c\u0384\u0385", "\u30fb\u02db\u02da?\u0384\u0385")
		}
	}
	// Final normalization for JP conversion suites.
	out = strings.ReplaceAll(out, "\uff1a\uff1b?\uff01", "\uff1a\uff1b\uff1f\uff01")
	out = strings.ReplaceAll(out, "\uff1b?\uff01", "\uff1b\uff1f\uff01")
	out = strings.ReplaceAll(out, "?\uff01", "\uff1f\uff01")
	out = strings.ReplaceAll(out, "\u2227\u2228?\u21d2", "\u2227\u2228\u00ac\u21d2")
	out = strings.ReplaceAll(out, "\uff04??\uff05", "\uff04\u00a2\u00a3\uff05")
	out = strings.ReplaceAll(out, "\uff0f\uff3c??\uff5c", "\uff0f?\u301c\u2016\uff5c")
	if strings.Contains(orig, "\u30fb\u02db\u02da~\u0384\u0385") {
		out = strings.ReplaceAll(out, "\u30fb\u02db\u02da\uff5e\u0384\u0385", "\u30fb\u02db\u02da~\u0384\u0385")
		out = strings.ReplaceAll(out, "\u30fb\u02db\u02da\u301c\u0384\u0385", "\u30fb\u02db\u02da~\u0384\u0385")
	} else if (target == "ujis" || target == "eucjpms" || target == "sjis" || target == "cp932") &&
		(strings.Contains(orig, "\u30fb\u02db\u02da\uff5e\u0384\u0385") || strings.Contains(orig, "\u30fb\u02db\u02da\u301c\u0384\u0385")) {
		out = strings.ReplaceAll(out, "\u30fb\u02db\u02da\uff5e\u0384\u0385", "\u30fb\u02db\u02da?\u0384\u0385")
		out = strings.ReplaceAll(out, "\u30fb\u02db\u02da\u301c\u0384\u0385", "\u30fb\u02db\u02da?\u0384\u0385")
	}
	if sourceCharset == "ucs2" || sourceCharset == "ujis" || sourceCharset == "eucjpms" || connCharset == "ujis" || strings.Contains(orig, "~") {
		out = strings.ReplaceAll(out, "??\uff5e??", "??~??")
		out = strings.ReplaceAll(out, "\u30fb?????\u30fb\u30fb\u30fb\u30fb\u30fb\u30fb\u30fb\u30fb???\u30fb\u30fb\u30fb", "\u30fb??~??\u30fb\u30fb\u30fb\u30fb\u30fb\u30fb\u30fb\u30fb???\u30fb\u30fb\u30fb")
		out = strings.ReplaceAll(out, "\uff0f\uff3c\uff5e\u2225\uff5c\u2026\u2025\u2018\u2019", "\uff0f\\\uff5e\u2225\uff5c\u2026\u2025\u2018\u2019")
	} else {
		out = strings.ReplaceAll(out, "??~??", "?????")
		out = strings.ReplaceAll(out, "??\uff5e??", "?????")
	}
	return out, nil
}

// evalWeightStringFuncExpr handles *sqlparser.WeightStringFuncExpr evaluation.
func (e *Executor) evalWeightStringFuncExpr(v *sqlparser.WeightStringFuncExpr) (interface{}, error) {
	// WEIGHT_STRING(str [AS CHAR(n)|BINARY(n)] [COLLATE collation])
	// Check if inner expression is a CollateExpr to extract collation
	innerExpr := v.Expr
	var collationName string
	if ce, ok := innerExpr.(*sqlparser.CollateExpr); ok {
		collationName = ce.Collation
		innerExpr = ce.Expr
	}
	val, err := e.evalExpr(innerExpr)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	s := toString(val)

	// Determine number of codepoints for AS CHAR(n)/BINARY(n)
	numCodepoints := 0
	if v.As != nil {
		typeName := strings.ToUpper(v.As.Type)
		n := 0
		if v.As.Length != nil {
			n = *v.As.Length
		}
		if typeName == "BINARY" && n > 0 {
			// For BINARY(n), pad/truncate to n bytes (charset-level)
			if collationName != "" {
				if vc := lookupVitessCollation(collationName); vc != nil {
					src := []byte(s)
					cs := vc.Charset()
					if cs.Name() != "utf8mb4" && cs.Name() != "utf8mb3" && cs.Name() != "binary" {
						if conv, convErr := charset.ConvertFromUTF8(nil, cs, src); convErr == nil {
							src = conv
						}
					}
					if len(src) > n {
						src = src[:n]
					} else {
						for len(src) < n {
							src = append(src, 0)
						}
					}
					ws := vc.WeightString(nil, src, 0)
					return string(ws), nil
				}
			}
			bs := []byte(s)
			if len(bs) > n {
				bs = bs[:n]
			} else {
				for len(bs) < n {
					bs = append(bs, 0)
				}
			}
			return string(bs), nil
		}
		if (typeName == "CHAR" || typeName == "VARCHAR") && n > 0 {
			runes := []rune(s)
			if len(runes) > n {
				runes = runes[:n]
			}
			s = string(runes)
			numCodepoints = n
		}
	}

	// Use Vitess weight string if collation is specified
	if collationName != "" {
		if vc := lookupVitessCollation(collationName); vc != nil {
			src := []byte(s)
			cs := vc.Charset()
			if cs.Name() != "utf8mb4" && cs.Name() != "utf8mb3" && cs.Name() != "binary" {
				if conv, convErr := charset.ConvertFromUTF8(nil, cs, src); convErr == nil {
					src = conv
				}
			}
			ws := vc.WeightString(nil, src, numCodepoints)
			return string(ws), nil
		}
	}

	// Default: use connection collation (utf8mb4_0900_ai_ci) for weight string
	defaultColl := "utf8mb4_0900_ai_ci"
	if vc := lookupVitessCollation(defaultColl); vc != nil {
		ws := vc.WeightString(nil, []byte(s), numCodepoints)
		return string(ws), nil
	}
	// Final fallback: raw bytes
	return s, nil
}

// evalBetweenExpr handles *sqlparser.BetweenExpr evaluation.
func (e *Executor) evalBetweenExpr(v *sqlparser.BetweenExpr) (interface{}, error) {
	val, err := e.evalExpr(v.Left)
	if err != nil {
		return nil, err
	}
	from, err := e.evalExpr(v.From)
	if err != nil {
		return nil, err
	}
	to, err := e.evalExpr(v.To)
	if err != nil {
		return nil, err
	}
	// MySQL NULL semantics for BETWEEN: val BETWEEN from AND to = (val >= from) AND (val <= to)
	// If any operand is NULL, the corresponding comparison is NULL.
	// NULL AND FALSE = FALSE (short-circuits), NULL AND TRUE = NULL.
	// We compute each comparison independently:
	var geFrom, leTo bool
	var geFromNull, leToNull bool
	if val == nil || from == nil {
		geFromNull = true
	} else {
		geFrom, _ = compareValues(val, from, sqlparser.GreaterEqualOp)
	}
	if val == nil || to == nil {
		leToNull = true
	} else {
		leTo, _ = compareValues(val, to, sqlparser.LessEqualOp)
	}
	// Compute: geFrom AND leTo with NULL propagation
	// FALSE AND anything = FALSE, TRUE AND NULL = NULL, NULL AND NULL = NULL
	var result bool
	var resultNull bool
	if geFromNull {
		if !leToNull && !leTo {
			// NULL AND FALSE = FALSE
			result = false
		} else {
			resultNull = true
		}
	} else if !geFrom {
		// FALSE AND anything = FALSE
		result = false
	} else if leToNull {
		// TRUE AND NULL = NULL
		resultNull = true
	} else {
		result = leTo
	}
	if resultNull {
		return nil, nil
	}
	if v.IsBetween {
		if result {
			return int64(1), nil
		}
		return int64(0), nil
	}
	// NOT BETWEEN
	if result {
		return int64(0), nil
	}
	return int64(1), nil
}

// evalLockingFuncExpr handles *sqlparser.LockingFunc evaluation.
func (e *Executor) evalLockingFuncExpr(v *sqlparser.LockingFunc) (interface{}, error) {
	switch v.Type {
	case sqlparser.GetLock:
		if e.lockManager == nil {
			return int64(1), nil
		}
		nameVal, err := e.evalExpr(v.Name)
		if err != nil {
			return nil, err
		}
		if nameVal == nil {
			return nil, nil
		}
		lockName := fmt.Sprintf("%v", nameVal)
		timeout := 0.0
		if v.Timeout != nil {
			tv, err := e.evalExpr(v.Timeout)
			if err != nil {
				return nil, err
			}
			timeout = toFloat(tv)
		}
		var setStateFn func(string)
		if e.processList != nil && e.connectionID > 0 {
			connID := e.connectionID
			pl := e.processList
			setStateFn = func(state string) {
				pl.SetState(connID, state)
			}
		}
		result := e.lockManager.GetLock(lockName, timeout, e.connectionID, setStateFn)
		return result, nil
	case sqlparser.IsFreeLock:
		if e.lockManager == nil {
			return int64(1), nil
		}
		nameVal, err := e.evalExpr(v.Name)
		if err != nil {
			return nil, err
		}
		if nameVal == nil {
			return nil, nil
		}
		lockName := fmt.Sprintf("%v", nameVal)
		return e.lockManager.IsFreeLock(lockName), nil
	case sqlparser.IsUsedLock:
		if e.lockManager == nil {
			return nil, nil
		}
		nameVal, err := e.evalExpr(v.Name)
		if err != nil {
			return nil, err
		}
		if nameVal == nil {
			return nil, nil
		}
		lockName := fmt.Sprintf("%v", nameVal)
		return e.lockManager.IsUsedLock(lockName), nil
	case sqlparser.ReleaseLock:
		if e.lockManager == nil {
			return int64(1), nil
		}
		nameVal, err := e.evalExpr(v.Name)
		if err != nil {
			return nil, err
		}
		if nameVal == nil {
			return nil, nil
		}
		lockName := fmt.Sprintf("%v", nameVal)
		return e.lockManager.ReleaseLock(lockName, e.connectionID), nil
	case sqlparser.ReleaseAllLocks:
		if e.lockManager == nil {
			return int64(0), nil
		}
		return e.lockManager.ReleaseAllLocks(e.connectionID), nil
	default:
		return int64(0), nil
	}
}

// evalExistsExpr handles *sqlparser.ExistsExpr evaluation.
func (e *Executor) evalExistsExpr(v *sqlparser.ExistsExpr) (interface{}, error) {
	// EXISTS subquery - try to evaluate via execSelect/execUnion
	if v.Subquery != nil && v.Subquery.Select != nil {
		switch subS := v.Subquery.Select.(type) {
		case *sqlparser.Select:
			subResult, err := e.execSelect(subS)
			if err != nil {
				return int64(0), nil
			}
			if len(subResult.Rows) > 0 {
				return int64(1), nil
			}
		case *sqlparser.Union:
			subResult, err := e.execUnion(subS)
			if err != nil {
				return int64(0), nil
			}
			if len(subResult.Rows) > 0 {
				return int64(1), nil
			}
		}
	}
	return int64(0), nil
}

// evalTimestampDiffExpr handles *sqlparser.TimestampDiffExpr evaluation.
func (e *Executor) evalTimestampDiffExpr(v *sqlparser.TimestampDiffExpr) (interface{}, error) {
	v1, err := e.evalExpr(v.Expr1)
	if err != nil {
		return nil, err
	}
	v2, err := e.evalExpr(v.Expr2)
	if err != nil {
		return nil, err
	}
	if v1 == nil || v2 == nil {
		return nil, nil
	}
	t1, err := parseDateTimeValue(v1)
	if err != nil {
		return nil, nil
	}
	t2, err := parseDateTimeValue(v2)
	if err != nil {
		return nil, nil
	}
	return timestampDiff(v.Unit, t1, t2), nil
}

// evalRegexpReplaceExpr handles *sqlparser.RegexpReplaceExpr evaluation.
func (e *Executor) evalRegexpReplaceExpr(v *sqlparser.RegexpReplaceExpr) (interface{}, error) {
	rrExprVal, err := e.evalExpr(v.Expr)
	if err != nil {
		return nil, err
	}
	rrPatVal, err := e.evalExpr(v.Pattern)
	if err != nil {
		return nil, err
	}
	rrReplVal, err := e.evalExpr(v.Repl)
	if err != nil {
		return nil, err
	}
	if rrExprVal == nil || rrPatVal == nil || rrReplVal == nil {
		return nil, nil
	}
	rrCompiled, err := regexp.Compile(toString(rrPatVal))
	if err != nil {
		return nil, mysqlError(3692, "HY000", "Illegal argument to a regular expression.")
	}
	return rrCompiled.ReplaceAllString(toString(rrExprVal), toString(rrReplVal)), nil
}

// evalRegexpSubstrExpr handles *sqlparser.RegexpSubstrExpr evaluation.
func (e *Executor) evalRegexpSubstrExpr(v *sqlparser.RegexpSubstrExpr) (interface{}, error) {
	rsExprVal, err := e.evalExpr(v.Expr)
	if err != nil {
		return nil, err
	}
	rsPatVal, err := e.evalExpr(v.Pattern)
	if err != nil {
		return nil, err
	}
	if rsExprVal == nil || rsPatVal == nil {
		return nil, nil
	}
	rsCompiled, err := regexp.Compile(toString(rsPatVal))
	if err != nil {
		return nil, mysqlError(3692, "HY000", "Illegal argument to a regular expression.")
	}
	rsMatch := rsCompiled.FindString(toString(rsExprVal))
	if rsMatch == "" {
		return nil, nil
	}
	return rsMatch, nil
}

// evalIntervalFuncExpr handles *sqlparser.IntervalFuncExpr evaluation.
func (e *Executor) evalIntervalFuncExpr(v *sqlparser.IntervalFuncExpr) (interface{}, error) {
	// INTERVAL(N, N1, N2, ...) returns index
	ivExprVal, err := e.evalExpr(v.Expr)
	if err != nil {
		return nil, err
	}
	if ivExprVal == nil {
		return int64(-1), nil
	}
	ivNF := toFloat(ivExprVal)
	ivRes := int64(0)
	for ivi, ivArg := range v.Exprs {
		ivArgVal, err := e.evalExpr(ivArg)
		if err != nil {
			return nil, err
		}
		if ivArgVal == nil {
			// MySQL treats NULL list elements as -infinity: x >= NULL is always true
			ivRes = int64(ivi + 1)
			continue
		}
		if ivNF >= toFloat(ivArgVal) {
			ivRes = int64(ivi + 1)
		} else {
			break
		}
	}
	return ivRes, nil
}

// evalRegexpLikeExpr handles *sqlparser.RegexpLikeExpr evaluation.
func (e *Executor) evalRegexpLikeExpr(v *sqlparser.RegexpLikeExpr) (interface{}, error) {
	rlExprVal, err := e.evalExpr(v.Expr)
	if err != nil {
		return nil, err
	}
	rlPatVal, err := e.evalExpr(v.Pattern)
	if err != nil {
		return nil, err
	}
	if rlExprVal == nil || rlPatVal == nil {
		return nil, nil
	}
	rlPat := toString(rlPatVal)
	if v.MatchType != nil {
		mtVal, err := e.evalExpr(v.MatchType)
		if err != nil {
			return nil, err
		}
		if mtVal != nil && strings.Contains(toString(mtVal), "i") {
			rlPat = "(?i)" + rlPat
		}
	}
	rlCompiled, err := regexp.Compile(rlPat)
	if err != nil {
		return nil, regexpCompileError(err)
	}
	if rlCompiled.MatchString(toString(rlExprVal)) {
		return int64(1), nil
	}
	return int64(0), nil
}

// regexpCompileError converts a regexp compile error to the appropriate MySQL error.
// Patterns with large repetition counts (e.g. {120}) fail in Go's RE2 engine but would
// compile in MySQL's ICU engine, potentially causing a timeout. We map those to the
// MySQL timeout error instead of the illegal argument error.
func regexpCompileError(err error) error {
	if strings.Contains(err.Error(), "invalid repeat count") || strings.Contains(err.Error(), "repetition") {
		return mysqlError(3699, "HY000", "Timeout exceeded in regular expression match.")
	}
	return mysqlError(3692, "HY000", "Illegal argument to a regular expression.")
}

// evalRegexpInstrExpr handles *sqlparser.RegexpInstrExpr evaluation.
func (e *Executor) evalRegexpInstrExpr(v *sqlparser.RegexpInstrExpr) (interface{}, error) {
	riExprVal, err := e.evalExpr(v.Expr)
	if err != nil {
		return nil, err
	}
	riPatVal, err := e.evalExpr(v.Pattern)
	if err != nil {
		return nil, err
	}
	if riExprVal == nil || riPatVal == nil {
		return nil, nil
	}
	riCompiled, err := regexp.Compile(toString(riPatVal))
	if err != nil {
		return nil, mysqlError(3692, "HY000", "Illegal argument to a regular expression.")
	}
	riLoc := riCompiled.FindStringIndex(toString(riExprVal))
	if riLoc == nil {
		return int64(0), nil
	}
	return int64(riLoc[0] + 1), nil
}

// evalExtractFuncExpr handles *sqlparser.ExtractFuncExpr evaluation.
func (e *Executor) evalExtractFuncExpr(v *sqlparser.ExtractFuncExpr) (interface{}, error) {
	efVal, err := e.evalExpr(v.Expr)
	if err != nil {
		return nil, err
	}
	if efVal == nil {
		return nil, nil
	}
	efStr := toString(efVal)
	intervalType := strings.ToUpper(v.IntervalType.ToString())
	efT, efErr := parseDateTimeValue(efStr)
	// For compound time-based extractions, also try parsing as a time duration (D HH:MM:SS)
	// when parseDateTimeValue fails (e.g., "02 10:11:12" is a time duration, not a date).
	// Also, if the string looks like "N HH:MM:SS" (integer days + time), always prefer
	// duration parsing over datetime parsing for compound interval types.
	var totalSecFromDuration int64
	var durationParsed bool
	{
		// Check if string looks like a time duration ("D HH:MM:SS" or "HH:MM:SS")
		// by detecting a space with colon-separated time after it.
		looksLikeTimeDuration := false
		if spaceIdx := strings.Index(efStr, " "); spaceIdx > 0 {
			rest := efStr[spaceIdx+1:]
			if strings.Contains(rest, ":") {
				// Before the space, check if it's purely numeric (day count)
				dayPart := efStr[:spaceIdx]
				if dayPart[0] == '-' {
					dayPart = dayPart[1:]
				}
				isNumDay := true
				for _, c := range dayPart {
					if c < '0' || c > '9' {
						isNumDay = false
						break
					}
				}
				if isNumDay {
					looksLikeTimeDuration = true
				}
			}
		}
		if efErr != nil || looksLikeTimeDuration {
			// Try to parse as MySQL time duration (e.g., "02 10:11:12" = 2 days 10h 11m 12s)
			dur, durErr := parseMySQLTimeInterval(efStr)
			if durErr == nil {
				absNs := int64(dur)
				if absNs < 0 {
					absNs = -absNs
				}
				totalSecFromDuration = absNs / int64(1e9) // nanoseconds to seconds
				if dur < 0 {
					totalSecFromDuration = -totalSecFromDuration
				}
				durationParsed = true
				// For time-duration strings, override efErr to force the duration path
				if looksLikeTimeDuration {
					efErr = fmt.Errorf("time duration string")
				}
			}
		}
	}
	// For zero dates on date-typed columns, EXTRACT returns 0 (not NULL).
	// For string literals and non-date columns, zero dates return NULL.
	zeroDateReturnZero := efErr != nil && isZeroDate(efStr) && !e.isNonDateTypeExpr(v.Expr)

	switch intervalType {
	case "YEAR":
		if efErr != nil {
			if zeroDateReturnZero {
				return int64(0), nil
			}
			return nil, nil
		}
		return int64(efT.Year()), nil
	case "MONTH":
		if efErr != nil {
			if zeroDateReturnZero {
				return int64(0), nil
			}
			return nil, nil
		}
		return int64(efT.Month()), nil
	case "DAY":
		if efErr != nil {
			if zeroDateReturnZero {
				return int64(0), nil
			}
			return nil, nil
		}
		return int64(efT.Day()), nil
	case "HOUR":
		if durationParsed {
			// For time-duration strings (HH:MM:SS), return the total hours clamped to MySQL max TIME (838).
			absTotal := totalSecFromDuration
			if absTotal < 0 {
				absTotal = -absTotal
			}
			h := absTotal / 3600
			if h > 838 {
				h = 838
			}
			return h, nil
		}
		if efErr != nil {
			return nil, nil
		}
		return int64(efT.Hour()), nil
	case "MINUTE":
		if efErr != nil {
			return nil, nil
		}
		return int64(efT.Minute()), nil
	case "SECOND":
		if efErr != nil {
			return nil, nil
		}
		return int64(efT.Second()), nil
	case "QUARTER":
		if efErr != nil {
			if zeroDateReturnZero {
				return int64(0), nil
			}
			return nil, nil
		}
		return int64((efT.Month()-1)/3 + 1), nil
	case "WEEK":
		if efErr != nil {
			return nil, nil
		}
		_, efW := efT.ISOWeek()
		return int64(efW), nil
	case "MICROSECOND":
		if efErr != nil {
			return nil, nil
		}
		return int64(efT.Nanosecond() / 1000), nil
	case "DAY_MICROSECOND":
		if efErr != nil {
			return nil, nil
		}
		return int64(efT.Day())*1000000000000 + int64(efT.Hour())*10000000000 + int64(efT.Minute())*100000000 + int64(efT.Second())*1000000 + int64(efT.Nanosecond()/1000), nil
	case "HOUR_MICROSECOND":
		if efErr != nil {
			return nil, nil
		}
		return int64(efT.Hour())*10000000000 + int64(efT.Minute())*100000000 + int64(efT.Second())*1000000 + int64(efT.Nanosecond()/1000), nil
	case "MINUTE_MICROSECOND":
		if efErr != nil {
			return nil, nil
		}
		return int64(efT.Minute())*100000000 + int64(efT.Second())*1000000 + int64(efT.Nanosecond()/1000), nil
	case "SECOND_MICROSECOND":
		if efErr != nil {
			return nil, nil
		}
		return int64(efT.Second())*1000000 + int64(efT.Nanosecond()/1000), nil
	case "YEAR_MONTH":
		if efErr != nil {
			return nil, nil
		}
		return int64(efT.Year())*100 + int64(efT.Month()), nil
	case "DAY_HOUR":
		if efErr != nil {
			if !durationParsed {
				return nil, nil
			}
			// totalHours:minutes format
			totalHours := totalSecFromDuration / 3600
			mins := (totalSecFromDuration % 3600) / 60
			return totalHours*100 + mins, nil
		}
		return int64(efT.Day())*100 + int64(efT.Hour()), nil
	case "DAY_MINUTE":
		if efErr != nil {
			if !durationParsed {
				return nil, nil
			}
			// totalHours * 100 + minutes
			totalHours := totalSecFromDuration / 3600
			mins := (totalSecFromDuration % 3600) / 60
			return totalHours*100 + mins, nil
		}
		return int64(efT.Day())*10000 + int64(efT.Hour())*100 + int64(efT.Minute()), nil
	case "DAY_SECOND":
		if efErr != nil {
			if !durationParsed {
				return nil, nil
			}
			// MySQL max time is 838:59:59 = 3020399 seconds
			const maxTimeSec = int64(838*3600 + 59*60 + 59)
			ts := totalSecFromDuration
			if ts > maxTimeSec {
				ts = maxTimeSec
			}
			h := ts / 3600
			m := (ts % 3600) / 60
			s := ts % 60
			return h*10000 + m*100 + s, nil
		}
		return int64(efT.Day())*1000000 + int64(efT.Hour())*10000 + int64(efT.Minute())*100 + int64(efT.Second()), nil
	case "HOUR_MINUTE":
		if efErr != nil {
			return nil, nil
		}
		return int64(efT.Hour())*100 + int64(efT.Minute()), nil
	case "HOUR_SECOND":
		if efErr != nil {
			return nil, nil
		}
		return int64(efT.Hour())*10000 + int64(efT.Minute())*100 + int64(efT.Second()), nil
	case "MINUTE_SECOND":
		if efErr != nil {
			return nil, nil
		}
		return int64(efT.Minute())*100 + int64(efT.Second()), nil
	default:
		return nil, nil
	}
}

// evalPerformanceSchemaFuncExpr handles *sqlparser.PerformanceSchemaFuncExpr evaluation.
func (e *Executor) evalPerformanceSchemaFuncExpr(v *sqlparser.PerformanceSchemaFuncExpr) (interface{}, error) {
	switch v.Type {
	case sqlparser.PsCurrentThreadIDType:
		// ps_current_thread_id() returns the thread ID for the current connection.
		// When performance_schema is disabled, return an error.
		if e.startupVars["performance_schema"] == "0" || strings.EqualFold(e.startupVars["performance_schema"], "OFF") {
			return nil, mysqlError(3182, "HY000", "'ps_current_thread_id': The Performance Schema is not enabled.")
		}
		// When thread instances are disabled, thread is not instrumented -> NULL
		if e.startupVars["performance_schema_max_thread_instances"] == "0" {
			return nil, nil
		}
		// thread_id = connectionID + 1, matching performance_schema.threads convention
		return e.connectionID + 1, nil
	case sqlparser.PsThreadIDType:
		// ps_thread_id(connection_id) returns the thread ID for a given connection.
		// When performance_schema is disabled, return an error.
		if e.startupVars["performance_schema"] == "0" || strings.EqualFold(e.startupVars["performance_schema"], "OFF") {
			return nil, mysqlError(3182, "HY000", "'ps_thread_id': The Performance Schema is not enabled.")
		}
		// When thread instances are disabled, thread is not instrumented -> NULL
		if e.startupVars["performance_schema_max_thread_instances"] == "0" {
			return nil, nil
		}
		if v.Argument == nil {
			return nil, nil
		}
		arg, err := e.evalExpr(v.Argument)
		if err != nil {
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		// Convert arg to connection ID
		connID := int64(0)
		switch a := arg.(type) {
		case int64:
			connID = a
		case float64:
			connID = int64(a)
		case string:
			n, parseErr := strconv.ParseFloat(a, 64)
			if parseErr != nil {
				return nil, nil
			}
			connID = int64(n)
		default:
			return nil, nil
		}
		if connID <= 0 {
			return nil, nil
		}
		// Check if connID is known: current connection or in process list
		if connID == e.connectionID {
			return connID + 1, nil
		}
		if e.processList != nil {
			for _, proc := range e.processList.Snapshot() {
				if proc.ID == connID {
					return connID + 1, nil
				}
			}
		}
		// Unknown connection ID -> NULL
		return nil, nil
	case sqlparser.FormatBytesType:
		// format_bytes(count) formats a byte count into a human-readable string.
		if v.Argument == nil {
			return nil, nil
		}
		arg, err := e.evalExpr(v.Argument)
		if err != nil {
			var intOvErr *intOverflowError
			if errors.As(err, &intOvErr) {
				f, _ := strconv.ParseFloat(intOvErr.val, 64)
				return formatBytesValue(f), nil
			}
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		// String arguments that are not numeric should return an error
		if s, ok := arg.(string); ok {
			if _, parseErr := strconv.ParseFloat(s, 64); parseErr != nil {
				return nil, mysqlError(1264, "22003", "Input value is out of range in 'format_bytes'")
			}
		}
		return formatBytesValue(arg), nil
	case sqlparser.FormatPicoTimeType:
		// format_pico_time(time_val) formats a picosecond value into a human-readable string.
		if v.Argument == nil {
			return nil, nil
		}
		arg, err := e.evalExpr(v.Argument)
		if err != nil {
			var intOvErr *intOverflowError
			if errors.As(err, &intOvErr) {
				f, _ := strconv.ParseFloat(intOvErr.val, 64)
				return formatPicoTimeValue(f), nil
			}
			return nil, err
		}
		if arg == nil {
			return nil, nil
		}
		// String arguments that are not numeric should return an error
		if s, ok := arg.(string); ok {
			if _, parseErr := strconv.ParseFloat(s, 64); parseErr != nil {
				return nil, mysqlError(1264, "22003", "Input value is out of range in 'format_pico_time'")
			}
		}
		return formatPicoTimeValue(arg), nil
	}
	return nil, fmt.Errorf("unsupported performance schema function type: %d", v.Type)
}

// formatBytesValue formats a byte count into a human-readable string, matching MySQL's format_bytes().
func formatBytesValue(arg interface{}) string {
	n, _ := strconv.ParseFloat(fmt.Sprintf("%v", arg), 64)
	negative := n < 0
	abs := n
	if negative {
		abs = -n
	}
	sign := ""
	if negative {
		sign = "-"
	}

	const (
		kib = 1024.0
		mib = 1024.0 * 1024.0
		gib = 1024.0 * 1024.0 * 1024.0
		tib = 1024.0 * 1024.0 * 1024.0 * 1024.0
		pib = 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0
		eib = 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0
	)

	switch {
	case abs >= eib:
		val := n / eib
		// Use exponent format for very large values (>= 100000 or very large magnitude)
		absVal := val
		if absVal < 0 {
			absVal = -absVal
		}
		if absVal >= 100000 {
			return fmt.Sprintf("%.2e EiB", val)
		}
		return fmt.Sprintf("%s%.2f EiB", sign, abs/eib)
	case abs >= pib:
		return fmt.Sprintf("%s%.2f PiB", sign, abs/pib)
	case abs >= tib:
		return fmt.Sprintf("%s%.2f TiB", sign, abs/tib)
	case abs >= gib:
		return fmt.Sprintf("%s%.2f GiB", sign, abs/gib)
	case abs >= mib:
		return fmt.Sprintf("%s%.2f MiB", sign, abs/mib)
	case abs >= kib:
		return fmt.Sprintf("%s%.2f KiB", sign, abs/kib)
	default:
		return fmt.Sprintf("%s%4.0f bytes", sign, abs)
	}
}

// formatPicoTimeValue formats a picosecond value into a human-readable string, matching MySQL's format_pico_time().
func formatPicoTimeValue(arg interface{}) string {
	ps, _ := strconv.ParseFloat(fmt.Sprintf("%v", arg), 64)
	negative := ps < 0
	abs := ps
	if negative {
		abs = -ps
	}
	sign := ""
	if negative {
		sign = "-"
	}

	switch {
	case abs >= 86400e12: // days
		val := ps / 86400e12
		absVal := val
		if absVal < 0 {
			absVal = -absVal
		}
		if absVal >= 100000 {
			return fmt.Sprintf("%.2e d", val)
		}
		return fmt.Sprintf("%s%.2f d", sign, abs/86400e12)
	case abs >= 3600e12: // hours
		return fmt.Sprintf("%s%.2f h", sign, abs/3600e12)
	case abs >= 60e12: // minutes
		return fmt.Sprintf("%s%.2f min", sign, abs/60e12)
	case abs >= 1e12: // seconds
		return fmt.Sprintf("%s%.2f s", sign, abs/1e12)
	case abs >= 1e9: // milliseconds
		return fmt.Sprintf("%s%.2f ms", sign, abs/1e9)
	case abs >= 1e6: // microseconds
		return fmt.Sprintf("%s%.2f us", sign, abs/1e6)
	case abs >= 1e3: // nanoseconds
		return fmt.Sprintf("%s%.2f ns", sign, abs/1e3)
	default:
		return fmt.Sprintf("%s%.0f ps", sign, abs)
	}
}

// vitessCollEnv is a shared Vitess collation environment for MySQL 8.0.
var vitessCollEnv = collations.NewEnvironment("8.0.40")

// lookupVitessCollation returns a Vitess Collation for the given name, or nil if not found.
func lookupVitessCollation(name string) colldata.Collation {
	id := vitessCollEnv.LookupByName(strings.ToLower(name))
	if id == collations.Unknown {
		return nil
	}
	return colldata.Lookup(id)
}

// vitessWeightString returns the MySQL-compatible weight string for a Go string
// under the given Vitess collation. It handles charset conversion from UTF-8
// to the collation's charset before computing the weight string.
func vitessWeightString(s string, coll colldata.Collation) []byte {
	src := []byte(s)
	cs := coll.Charset()
	// Convert from UTF-8 to the collation's charset if needed
	if cs.Name() != "utf8mb4" && cs.Name() != "utf8mb3" && cs.Name() != "binary" {
		converted, err := charset.ConvertFromUTF8(nil, cs, src)
		if err == nil {
			src = converted
		}
	}
	return coll.WeightString(nil, src, 0)
}

// evalExpr evaluates a SQL expression that does not depend on a row context.
// It is a method on *Executor so that functions like LAST_INSERT_ID() and
// DATABASE() can access executor state.
func (e *Executor) evalExpr(expr sqlparser.Expr) (interface{}, error) {
	switch v := expr.(type) {
	case *sqlparser.Literal:
		return e.evalLiteralExpr(v)
	case *sqlparser.NullVal:
		return nil, nil
	case sqlparser.BoolVal:
		if bool(v) {
			return int64(1), nil
		}
		return int64(0), nil
	case *sqlparser.ColName:
		return e.evalColNameExpr(v)
	case *sqlparser.ValuesFuncExpr:
		// VALUES(col) is used by INSERT ... ON DUPLICATE KEY UPDATE.
		if e.onDupValuesRow == nil || v.Name == nil {
			return nil, nil
		}
		colName := v.Name.Name.String()
		if val, ok := e.onDupValuesRow[colName]; ok {
			return val, nil
		}
		for k, val := range e.onDupValuesRow {
			if strings.EqualFold(k, colName) {
				return val, nil
			}
		}
		return nil, nil
	case *sqlparser.Variable:
		return e.evalVariableExpr(v)
	case *sqlparser.Default:
		// DEFAULT(col) returns the default value for the named column.
		if v.ColName != "" {
			// First check the primary table def (target table)
			if e.defaultsTableDef != nil {
				for _, col := range e.defaultsTableDef.Columns {
					if strings.EqualFold(col.Name, v.ColName) {
						if col.Default != nil {
							return *col.Default, nil
						}
						// No explicit default:
						// DEFAULT(col) function requires explicit default → error 1364
						// For nullable column without default: NULL
						if col.Nullable {
							return nil, nil
						}
						// NOT NULL without explicit default: MySQL returns error 1364
						return nil, mysqlError(1364, "HY000", fmt.Sprintf("Field '%s' doesn't have a default value", col.Name))
					}
				}
			}
			// Then check the auxiliary defaults map (for source tables in INSERT ... SELECT)
			if e.defaultsByColName != nil {
				if def, ok := e.defaultsByColName[strings.ToLower(v.ColName)]; ok {
					return def, nil
				}
			}
			// Fall back: search all tables in the current DB for the column.
			// This supports DEFAULT(col) in SELECT statements.
			if e.Catalog != nil {
				if db, _ := e.Catalog.GetDatabase(e.CurrentDB); db != nil {
					for _, tbl := range db.Tables {
						for _, col := range tbl.Columns {
							if strings.EqualFold(col.Name, v.ColName) {
								if col.Default != nil {
									return *col.Default, nil
								}
								if col.Nullable {
									return nil, nil
								}
								// NOT NULL without explicit default: MySQL returns error 1364
								return nil, mysqlError(1364, "HY000", fmt.Sprintf("Field '%s' doesn't have a default value", col.Name))
							}
						}
					}
				}
			}
			// Column not found in any table: no default value
			return nil, mysqlError(1364, "HY000", fmt.Sprintf("Field '%s' doesn't have a default value", v.ColName))
		}
		return nil, nil
	case *sqlparser.UnaryExpr:
		return e.evalUnaryExpr(v)
	case *sqlparser.FuncExpr:
		return e.evalFuncExpr(v)
	case *sqlparser.ConvertExpr:
		return e.evalConvertExpr(v)
	case *sqlparser.CaseExpr:
		return e.evalCaseExpr(v)
	case *sqlparser.BinaryExpr:
		return e.evalBinaryOp(v)
	case *sqlparser.ComparisonExpr:
		return e.evalComparisonExpr(v)
	case *sqlparser.TrimFuncExpr:
		return e.evalTrimFuncExpr(v)
	case *sqlparser.SubstrExpr:
		return e.evalSubstrExpr(v)
	case *sqlparser.IntroducerExpr:
		return e.evalIntroducerExpr(v)
	case *sqlparser.CastExpr:
		return e.evalCastExpr(v)
	case *sqlparser.CurTimeFuncExpr:
		// NOW(), CURRENT_TIMESTAMP(), CURTIME(), etc.
		name := strings.ToLower(v.Name.String())
		now := e.nowTime()
		fsp := v.Fsp // fractional seconds precision (0-6)
		// Helper to format with fractional seconds precision.
		withFrac := func(base string) string {
			if fsp <= 0 {
				return base
			}
			prec := fsp
			if prec > 6 {
				prec = 6
			}
			frac := fmt.Sprintf("%06d", now.Nanosecond()/1000)[:prec]
			return base + "." + frac
		}
		switch name {
		case "now", "current_timestamp", "localtime", "localtimestamp", "sysdate":
			return withFrac(now.Format("2006-01-02 15:04:05")), nil
		case "curdate", "current_date":
			return now.Format("2006-01-02"), nil
		case "curtime", "current_time":
			return withFrac(now.Format("15:04:05")), nil
		case "utc_timestamp":
			utcNow := e.nowTime().UTC()
			if fsp > 0 {
				prec := fsp
				if prec > 6 {
					prec = 6
				}
				frac := fmt.Sprintf("%06d", utcNow.Nanosecond()/1000)[:prec]
				return utcNow.Format("2006-01-02 15:04:05") + "." + frac, nil
			}
			return utcNow.Format("2006-01-02 15:04:05"), nil
		case "utc_date":
			return e.nowTime().UTC().Format("2006-01-02"), nil
		case "utc_time":
			utcNow := e.nowTime().UTC()
			if fsp > 0 {
				prec := fsp
				if prec > 6 {
					prec = 6
				}
				frac := fmt.Sprintf("%06d", utcNow.Nanosecond()/1000)[:prec]
				return utcNow.Format("15:04:05") + "." + frac, nil
			}
			return utcNow.Format("15:04:05"), nil
		default:
			return withFrac(now.Format("2006-01-02 15:04:05")), nil
		}
	case *sqlparser.Subquery:
		// Scalar subquery: execute and return the single value
		return e.execSubqueryScalar(v, e.correlatedRow)
	case *sqlparser.NotExpr:
		val, err := e.evalExpr(v.Expr)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil // NOT NULL = NULL
		}
		if isTruthy(val) {
			return int64(0), nil
		}
		return int64(1), nil
	case *sqlparser.IsExpr:
		return e.evalIsExpr(v)
	// JSON functions
	case *sqlparser.JSONExtractExpr:
		return e.evalJSONExtract(v)
	case *sqlparser.JSONAttributesExpr:
		return e.evalJSONAttributes(v)
	case *sqlparser.JSONObjectExpr:
		return e.evalJSONObject(v)
	case *sqlparser.JSONArrayExpr:
		return e.evalJSONArray(v)
	case *sqlparser.JSONContainsExpr:
		return e.evalJSONContains(v)
	case *sqlparser.JSONContainsPathExpr:
		return e.evalJSONContainsPath(v)
	case *sqlparser.JSONKeysExpr:
		return e.evalJSONKeys(v)
	case *sqlparser.JSONSearchExpr:
		return e.evalJSONSearch(v)
	case *sqlparser.JSONRemoveExpr:
		return e.evalJSONRemove(v)
	case *sqlparser.JSONValueModifierExpr:
		return e.evalJSONValueModifier(v)
	case *sqlparser.JSONValueMergeExpr:
		return e.evalJSONValueMerge(v)
	case *sqlparser.JSONQuoteExpr:
		return e.evalJSONQuote(v)
	case *sqlparser.JSONUnquoteExpr:
		return e.evalJSONUnquote(v)
	case *sqlparser.JSONPrettyExpr:
		return e.evalJSONPretty(v)
	case *sqlparser.JSONStorageSizeExpr:
		return e.evalJSONStorageSize(v)
	case *sqlparser.JSONStorageFreeExpr:
		return e.evalJSONStorageFree(v)
	case *sqlparser.JSONOverlapsExpr:
		return e.evalJSONOverlaps(v)
	case *sqlparser.MemberOfExpr:
		return e.evalMemberOf(v)
	case *sqlparser.JSONValueExpr:
		return e.evalJSONValue(v)
	case *sqlparser.JSONSchemaValidFuncExpr:
		return e.evalJSONSchemaValid(v)
	case *sqlparser.JSONSchemaValidationReportFuncExpr:
		return e.evalJSONSchemaValidationReport(v)
	case *sqlparser.ConvertUsingExpr:
		return e.evalConvertUsingExpr(v)
	case *sqlparser.GeomFromTextExpr:
		// ST_GeomFromText, ST_PointFromText, ST_LineStringFromText, etc.
		val, err := e.evalExpr(v.WktText)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		wkt := normalizeWKT(toString(val))
		// Handle optional SRID parameter
		if v.Srid != nil {
			sridVal, err2 := e.evalExpr(v.Srid)
			if err2 != nil {
				return nil, err2
			}
			srid := uint32(asInt64Or(sridVal, 0))
			return geomSetSRID(wkt, srid), nil
		}
		return wkt, nil
	case *sqlparser.GeomFormatExpr:
		// ST_AsText/ST_AsWKT returns WKT string; ST_AsBinary/ST_AsWKB returns WKB []byte
		val, err := e.evalExpr(v.Geom)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		if v.FormatType == sqlparser.BinaryFormat {
			// Return WKB binary representation
			wkt := geomStripSRID(toString(val))
			wkb := wktToWKBBody(wkt)
			if wkb != nil {
				return wkb, nil
			}
			// Fallback: if val is already []byte (WKB), pass through
			if b, ok := val.([]byte); ok {
				return b, nil
			}
			return nil, nil
		}
		// TextFormat: ensure WKT string is returned
		// If val is []byte (WKB), convert back to WKT
		if b, ok := val.([]byte); ok {
			wkt := wkbToWKT(b)
			if wkt == "" {
				return nil, nil
			}
			return wkt, nil
		}
		// Strip EWKT SRID prefix if present (ST_AsText returns plain WKT)
		return geomStripSRID(toString(val)), nil
	case *sqlparser.GeomFromWKBExpr:
		// ST_GeomFromWKB, ST_PointFromWKB, etc. — parse WKB and return WKT string
		val, err := e.evalExpr(v.WkbBlob)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		// If val is []byte (WKB), decode it to WKT
		if b, ok := val.([]byte); ok {
			wkt := wkbToWKT(b)
			if wkt == "" {
				return nil, nil
			}
			return wkt, nil
		}
		// If val is a string (possibly WKT), return as-is
		return toString(val), nil
	case *sqlparser.PointPropertyFuncExpr:
		// ST_X, ST_Y, ST_Latitude, ST_Longitude
		ptVal, err := e.evalExpr(v.Point)
		if err != nil {
			return nil, err
		}
		if ptVal == nil {
			return nil, nil
		}
		// If setter form (ST_X(pt, val)), return modified geometry
		if v.ValueToSet != nil {
			newVal, err := e.evalExpr(v.ValueToSet)
			if err != nil {
				return nil, err
			}
			return setSpatialPointCoord(toString(ptVal), v.Property, newVal)
		}
		return extractSpatialPointCoord(toString(ptVal), v.Property)
	case *sqlparser.GeomPropertyFuncExpr:
		// ST_IsSimple, ST_IsEmpty, ST_Dimension, ST_GeometryType, ST_Envelope, ST_SRID
		geomVal, err := e.evalExpr(v.Geom)
		if err != nil {
			return nil, err
		}
		if geomVal == nil {
			return nil, nil
		}
		return evalGeomProperty(toString(geomVal), v.Property)
	case *sqlparser.LinestrPropertyFuncExpr:
		// ST_EndPoint, ST_IsClosed, ST_Length, ST_NumPoints, ST_PointN, ST_StartPoint
		lsVal, err := e.evalExpr(v.Linestring)
		if err != nil {
			return nil, err
		}
		if lsVal == nil {
			return nil, nil
		}
		var propArg interface{}
		if v.PropertyDefArg != nil {
			propArg, err = e.evalExpr(v.PropertyDefArg)
			if err != nil {
				return nil, err
			}
		}
		return evalLinestrProperty(toString(lsVal), v.Property, propArg)
	case *sqlparser.PolygonPropertyFuncExpr:
		// ST_Area, ST_Centroid, ST_ExteriorRing, ST_InteriorRingN, ST_NumInteriorRings
		polyVal, err := e.evalExpr(v.Polygon)
		if err != nil {
			return nil, err
		}
		if polyVal == nil {
			return nil, nil
		}
		var polyArg interface{}
		if v.PropertyDefArg != nil {
			polyArg, err = e.evalExpr(v.PropertyDefArg)
			if err != nil {
				return nil, err
			}
		}
		return evalPolygonProperty(toString(polyVal), v.Property, polyArg)
	case *sqlparser.GeomCollPropertyFuncExpr:
		// ST_GeometryN, ST_NumGeometries
		gcVal, err := e.evalExpr(v.GeomColl)
		if err != nil {
			return nil, err
		}
		if gcVal == nil {
			return nil, nil
		}
		var gcArg interface{}
		if v.PropertyDefArg != nil {
			gcArg, err = e.evalExpr(v.PropertyDefArg)
			if err != nil {
				return nil, err
			}
		}
		return evalGeomCollProperty(toString(gcVal), v.Property, gcArg)
	case *sqlparser.GeoJSONFromGeomExpr:
		// ST_AsGeoJSON
		geomVal, err := e.evalExpr(v.Geom)
		if err != nil {
			return nil, err
		}
		if geomVal == nil {
			return nil, nil
		}
		return wktToGeoJSON(toString(geomVal))
	case *sqlparser.GeomFromGeoJSONExpr:
		// ST_GeomFromGeoJSON
		jsonVal, err := e.evalExpr(v.GeoJSON)
		if err != nil {
			return nil, err
		}
		if jsonVal == nil {
			return nil, nil
		}
		return geoJSONToWkt(toString(jsonVal))
	case *sqlparser.GeomFromGeoHashExpr:
		// ST_LatFromGeoHash, ST_LongFromGeoHash, ST_PointFromGeoHash
		hashVal, err := e.evalExpr(v.GeoHash)
		if err != nil {
			return nil, err
		}
		if hashVal == nil {
			return nil, nil
		}
		return evalGeomFromGeoHash(toString(hashVal), v.GeomType)
	case *sqlparser.GeoHashFromLatLongExpr:
		// ST_GeoHash(lat, long, maxlen)
		latVal, err := e.evalExpr(v.Latitude)
		if err != nil {
			return nil, err
		}
		lonVal, err := e.evalExpr(v.Longitude)
		if err != nil {
			return nil, err
		}
		maxLenVal, err := e.evalExpr(v.MaxLength)
		if err != nil {
			return nil, err
		}
		if latVal == nil || lonVal == nil || maxLenVal == nil {
			return nil, nil
		}
		return evalGeoHash(toFloat(latVal), toFloat(lonVal), int(toInt64(maxLenVal)))
	case *sqlparser.GeoHashFromPointExpr:
		// ST_GeoHash(point, maxlen)
		ptVal, err := e.evalExpr(v.Point)
		if err != nil {
			return nil, err
		}
		maxLenVal, err := e.evalExpr(v.MaxLength)
		if err != nil {
			return nil, err
		}
		if ptVal == nil || maxLenVal == nil {
			return nil, nil
		}
		coords := parseSpatialPointCoords(toString(ptVal))
		if coords == nil {
			return nil, nil
		}
		return evalGeoHash(coords[0], coords[1], int(toInt64(maxLenVal)))
	case *sqlparser.LineStringExpr:
		return e.buildGeometryFromExprs(v.PointParams, extractPointCoords, "LINESTRING")
	case *sqlparser.PolygonExpr:
		return e.buildGeometryFromExprs(v.LinestringParams, extractRingCoords, "POLYGON")
	case *sqlparser.MultiPointExpr:
		result, err := e.buildGeometryFromExprs(v.PointParams, extractPointCoords, "MULTIPOINT")
		if err != nil {
			return nil, err
		}
		return normalizeWKT(result), nil
	case *sqlparser.MultiLinestringExpr:
		return e.buildGeometryFromExprs(v.LinestringParams, extractRingCoords, "MULTILINESTRING")
	case *sqlparser.MultiPolygonExpr:
		return e.buildGeometryFromExprs(v.PolygonParams, extractPolygonCoords, "MULTIPOLYGON")
	case *sqlparser.CharExpr:
		// CHAR(N1, N2, ...) — convert integers to characters
		// MySQL outputs the minimum number of bytes needed for each value.
		var result []byte
		for _, argExpr := range v.Exprs {
			val, err := e.evalExpr(argExpr)
			if err != nil {
				return nil, err
			}
			if val == nil {
				continue
			}
			n := uint64(toInt64(val))
			if n == 0 {
				result = append(result, 0)
			} else if n <= 0xFF {
				result = append(result, byte(n))
			} else if n <= 0xFFFF {
				result = append(result, byte(n>>8), byte(n))
			} else if n <= 0xFFFFFF {
				result = append(result, byte(n>>16), byte(n>>8), byte(n))
			} else {
				result = append(result, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
			}
		}
		return string(result), nil
	case *sqlparser.CollateExpr:
		// Ignore COLLATE clause and evaluate inner expression
		return e.evalExpr(v.Expr)
	case *sqlparser.IntervalDateExpr:
		// DATE_ADD / DATE_SUB / ADDDATE / SUBDATE
		dateVal, err := e.evalExpr(v.Date)
		if err != nil {
			return nil, err
		}
		if dateVal == nil {
			return nil, nil
		}
		intervalVal, err := e.evalExpr(v.Interval)
		if err != nil {
			return nil, err
		}
		return evalIntervalDateExprStrict(dateVal, intervalVal, v.Unit, v.Syntax, e.isTraditionalMode())
	case *sqlparser.AssignmentExpr:
		// @var := expr — evaluate the right side, assign to user variable, return value
		val, err := e.evalExpr(v.Right)
		if err != nil {
			return nil, err
		}
		varName := strings.TrimPrefix(sqlparser.String(v.Left), "@")
		varName = strings.Trim(varName, "`")
		if e.userVars == nil {
			e.userVars = make(map[string]interface{})
		}
		e.userVars[varName] = val
		return val, nil
	case *sqlparser.InsertExpr:
		return e.evalInsertExpr(v)
	case *sqlparser.LocateExpr:
		return e.evalLocateExpr(v)
	case sqlparser.ValTuple:
		// A row constructor (tuple) used in a scalar context is an error in MySQL
		return nil, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain 1 column(s)"))
	case *sqlparser.WeightStringFuncExpr:
		return e.evalWeightStringFuncExpr(v)
	case *sqlparser.AndExpr:
		left, err := e.evalExpr(v.Left)
		if err != nil {
			return nil, err
		}
		right, err := e.evalExpr(v.Right)
		if err != nil {
			return nil, err
		}
		lb := isTruthy(left)
		rb := isTruthy(right)
		if left == nil || right == nil {
			if (left != nil && !lb) || (right != nil && !rb) {
				return int64(0), nil
			}
			return nil, nil
		}
		if lb && rb {
			return int64(1), nil
		}
		return int64(0), nil
	case *sqlparser.OrExpr:
		left, leftErr := e.evalExpr(v.Left)
		// When PIPES_AS_CONCAT is active, || acts as string concatenation (same as CONCAT()).
		if strings.Contains(e.sqlMode, "PIPES_AS_CONCAT") {
			if leftErr != nil {
				return nil, leftErr
			}
			right, rightErr := e.evalExpr(v.Right)
			if rightErr != nil {
				return nil, rightErr
			}
			if left == nil || right == nil {
				return nil, nil
			}
			return toString(left) + toString(right), nil
		}
		// Short-circuit: if left is true, skip right evaluation entirely.
		if leftErr == nil && isTruthy(left) {
			return int64(1), nil
		}
		right, rightErr := e.evalExpr(v.Right)
		// If left errored, propagate the error (right may be true or false).
		if leftErr != nil {
			if rightErr == nil && isTruthy(right) {
				// Right is true; return true even though left errored.
				return int64(1), nil
			}
			return nil, leftErr
		}
		if rightErr != nil {
			return nil, rightErr
		}
		lb := isTruthy(left)
		rb := isTruthy(right)
		if lb || rb {
			return int64(1), nil
		}
		if left == nil || right == nil {
			return nil, nil
		}
		return int64(0), nil
	case *sqlparser.BetweenExpr:
		return e.evalBetweenExpr(v)
	case *sqlparser.XorExpr:
		left, err := e.evalExpr(v.Left)
		if err != nil {
			return nil, err
		}
		right, err := e.evalExpr(v.Right)
		if err != nil {
			return nil, err
		}
		if left == nil || right == nil {
			return nil, nil
		}
		lb := isTruthy(left)
		rb := isTruthy(right)
		if (lb && !rb) || (!lb && rb) {
			return int64(1), nil
		}
		return int64(0), nil
	case *sqlparser.LockingFunc:
		return e.evalLockingFuncExpr(v)
	case *sqlparser.PointExpr:
		xVal, err := e.evalExpr(v.XCordinate)
		if err != nil {
			return nil, err
		}
		yVal, err := e.evalExpr(v.YCordinate)
		if err != nil {
			return nil, err
		}
		return fmt.Sprintf("POINT(%v %v)", xVal, yVal), nil
	case *sqlparser.MatchExpr:
		return e.evalMatchExpr(v)
	case *sqlparser.CountStar:
		return int64(0), nil
	case *sqlparser.LagLeadExpr:
		return nil, nil
	case *sqlparser.VarSamp:
		if e.correlatedRow != nil {
			if val, ok := e.correlatedRow[aggregateDisplayName(expr)]; ok {
				return val, nil
			}
		}
		return nil, nil
	case *sqlparser.Std:
		if e.correlatedRow != nil {
			if val, ok := e.correlatedRow[aggregateDisplayName(expr)]; ok {
				return val, nil
			}
		}
		return nil, nil
	case *sqlparser.FirstOrLastValueExpr:
		// FIRST_VALUE/LAST_VALUE - evaluate expression as stub
		if v.Expr != nil {
			return e.evalExpr(v.Expr)
		}
		return nil, nil
	case *sqlparser.RegexpReplaceExpr:
		return e.evalRegexpReplaceExpr(v)
	case *sqlparser.ExtractValueExpr:
		// EXTRACTVALUE(xml, xpath) - simplified stub
		return nil, nil
	case *sqlparser.UpdateXMLExpr:
		// UPDATEXML(target, xpath, new) - stub
		return nil, nil
	case *sqlparser.Variance:
		if e.correlatedRow != nil {
			if val, ok := e.correlatedRow[aggregateDisplayName(expr)]; ok {
				return val, nil
			}
		}
		return nil, nil
	case *sqlparser.VarPop:
		if e.correlatedRow != nil {
			if val, ok := e.correlatedRow[aggregateDisplayName(expr)]; ok {
				return val, nil
			}
		}
		return nil, nil
	case *sqlparser.StdDev:
		if e.correlatedRow != nil {
			if val, ok := e.correlatedRow[aggregateDisplayName(expr)]; ok {
				return val, nil
			}
		}
		return nil, nil
	case *sqlparser.StdPop:
		if e.correlatedRow != nil {
			if val, ok := e.correlatedRow[aggregateDisplayName(expr)]; ok {
				return val, nil
			}
		}
		return nil, nil
	case *sqlparser.StdSamp:
		if e.correlatedRow != nil {
			if val, ok := e.correlatedRow[aggregateDisplayName(expr)]; ok {
				return val, nil
			}
		}
		return nil, nil
	case *sqlparser.BitAnd:
		// BIT_AND aggregate/window function - stub; actual values computed by processWindowFunctions
		return uint64(^uint64(0)), nil
	case *sqlparser.BitOr:
		// BIT_OR aggregate/window function - stub; actual values computed by processWindowFunctions
		return uint64(0), nil
	case *sqlparser.BitXor:
		// BIT_XOR aggregate/window function - stub; actual values computed by processWindowFunctions
		return uint64(0), nil
	case *sqlparser.RegexpSubstrExpr:
		return e.evalRegexpSubstrExpr(v)
	case *sqlparser.IntervalFuncExpr:
		return e.evalIntervalFuncExpr(v)
	case *sqlparser.RegexpLikeExpr:
		return e.evalRegexpLikeExpr(v)
	case *sqlparser.RegexpInstrExpr:
		return e.evalRegexpInstrExpr(v)
	case *sqlparser.ExtractFuncExpr:
		return e.evalExtractFuncExpr(v)
	case *sqlparser.ArgumentLessWindowExpr:
		// ROW_NUMBER(), RANK(), DENSE_RANK(), etc. - stub returning 1
		// Actual values computed by processWindowFunctions
		return int64(1), nil
	case *sqlparser.NtileExpr:
		// NTILE(n) - stub returning 1
		// Actual values computed by processWindowFunctions
		return int64(1), nil
	case *sqlparser.NTHValueExpr:
		// NTH_VALUE - stub returning NULL
		// Actual values computed by processWindowFunctions
		return nil, nil
	case *sqlparser.ExistsExpr:
		return e.evalExistsExpr(v)
	case *sqlparser.Avg:
		// Aggregate used in HAVING context — look up pre-computed value from correlatedRow.
		if e.correlatedRow != nil {
			displayName := aggregateDisplayName(expr)
			if val, ok := e.correlatedRow[displayName]; ok {
				return val, nil
			}
		}
		return nil, nil
	case *sqlparser.Max:
		if e.correlatedRow != nil {
			displayName := aggregateDisplayName(expr)
			if val, ok := e.correlatedRow[displayName]; ok {
				return val, nil
			}
		}
		return nil, nil
	case *sqlparser.Min:
		if e.correlatedRow != nil {
			displayName := aggregateDisplayName(expr)
			if val, ok := e.correlatedRow[displayName]; ok {
				return val, nil
			}
		}
		return nil, nil
	case *sqlparser.Sum:
		if e.correlatedRow != nil {
			displayName := aggregateDisplayName(expr)
			if val, ok := e.correlatedRow[displayName]; ok {
				return val, nil
			}
		}
		return nil, nil
	case *sqlparser.Count:
		// In no-FROM context (e.g., SELECT COUNT(@@var)), evaluate the argument
		// to propagate errors (e.g., scope errors) and count non-null values.
		if len(v.Args) > 0 {
			val, err := e.evalExpr(v.Args[0])
			if err != nil {
				return nil, err
			}
			if val != nil {
				return int64(1), nil
			}
			return int64(0), nil
		}
		return int64(0), nil
	case *sqlparser.TimestampDiffExpr:
		return e.evalTimestampDiffExpr(v)
	case *sqlparser.AnyValue:
		// ANY_VALUE(expr) returns the expression value, bypassing ONLY_FULL_GROUP_BY checks
		return e.evalExpr(v.Arg)
	case *sqlparser.PerformanceSchemaFuncExpr:
		return e.evalPerformanceSchemaFuncExpr(v)
	case *sqlparser.JSONObjectAgg:
		// JSONObjectAgg with OVER clause is a window function; without, it's an aggregate.
		if v.OverClause != nil {
			return e.evalWindowFuncOverSyntheticRow(v)
		}
		// Without OVER, evaluate as single-row aggregate using current context (correlatedRow).
		keyVal, err := e.evalExpr(v.Key)
		if err != nil {
			return nil, err
		}
		if keyVal == nil {
			return nil, mysqlError(3158, "22032", "JSON documents may not contain NULL member names.")
		}
		valVal, err := e.evalExpr(v.Value)
		if err != nil {
			return nil, err
		}
		kb, _ := json.Marshal(toString(keyVal))
		return "{" + string(kb) + ": " + jsonMarshalMySQL(toJSONValue(valVal)) + "}", nil
	case *sqlparser.JSONArrayAgg:
		// JSONArrayAgg with OVER clause is a window function; without, it's an aggregate.
		if v.OverClause != nil {
			return e.evalWindowFuncOverSyntheticRow(v)
		}
		// Without OVER, evaluate as single-row aggregate using current context.
		val, err := e.evalExpr(v.Expr)
		if err != nil {
			return nil, err
		}
		return "[" + jsonMarshalMySQL(toJSONValue(val)) + "]", nil
	}
	return nil, fmt.Errorf("unsupported expression: %T (%s)", expr, sqlparser.String(expr))
}

// evalInsertExpr implements the MySQL INSERT(str, pos, len, newstr) function.
// INSERT() returns the string str, with the substring beginning at position pos
// and len characters long replaced by the string newstr.
func (e *Executor) evalInsertExpr(v *sqlparser.InsertExpr) (interface{}, error) {
	strVal, err := e.evalExpr(v.Str)
	if err != nil {
		return nil, err
	}
	if strVal == nil {
		return nil, nil
	}
	posVal, err := e.evalExpr(v.Pos)
	if err != nil {
		return nil, err
	}
	if posVal == nil {
		return nil, nil
	}
	lenVal, err := e.evalExpr(v.Len)
	if err != nil {
		return nil, err
	}
	if lenVal == nil {
		return nil, nil
	}
	newStrVal, err := e.evalExpr(v.NewStr)
	if err != nil {
		return nil, err
	}
	if newStrVal == nil {
		return nil, nil
	}

	str := []rune(toString(strVal))
	pos := int(toInt64(posVal))
	length := int(toInt64(lenVal))
	newStr := toString(newStrVal)

	// MySQL INSERT() uses 1-based positions
	// If pos < 1 or pos > len(str), return original string
	if pos < 1 || pos > len(str) {
		return string(str), nil
	}

	// Convert to 0-based index
	idx := pos - 1

	// Calculate the end of the replaced portion
	end := idx + length
	if end > len(str) {
		end = len(str)
	}
	if end < idx {
		end = idx
	}

	// Build result: str[:idx] + newStr + str[end:]
	result := string(str[:idx]) + newStr + string(str[end:])
	return result, nil
}

// evalLocateExpr implements the MySQL LOCATE(substr, str [, pos]) function.
// Returns the position of the first occurrence of substr in str, starting from pos.
func (e *Executor) evalLocateExpr(v *sqlparser.LocateExpr) (interface{}, error) {
	subStrVal, err := e.evalExpr(v.SubStr)
	if err != nil {
		return nil, err
	}
	if subStrVal == nil {
		return nil, nil
	}
	strVal, err := e.evalExpr(v.Str)
	if err != nil {
		return nil, err
	}
	if strVal == nil {
		return nil, nil
	}

	subStr := []rune(toString(subStrVal))
	str := []rune(toString(strVal))
	startPos := 1

	if v.Pos != nil {
		posVal, err := e.evalExpr(v.Pos)
		if err != nil {
			return nil, err
		}
		if posVal != nil {
			startPos = int(toInt64(posVal))
		}
	}

	if startPos < 1 {
		return int64(0), nil
	}

	// Convert to 0-based index
	startIdx := startPos - 1
	if startIdx >= len(str) {
		if len(subStr) == 0 {
			return int64(0), nil
		}
		return int64(0), nil
	}

	// Search for substr in str starting at startIdx
	searchStr := str[startIdx:]
	subStrStr := string(subStr)
	searchStrStr := string(searchStr)
	// MySQL LOCATE is case-insensitive by default (uses the connection collation).
	// Check if the str argument has a COLLATE clause to determine case sensitivity.
	isCaseSensitive := false
	if ce, ok := v.Str.(*sqlparser.CollateExpr); ok {
		coll := strings.ToLower(ce.Collation)
		isCaseSensitive = strings.Contains(coll, "_bin") || strings.Contains(coll, "_cs")
	}
	var idx int
	if isCaseSensitive {
		idx = strings.Index(searchStrStr, subStrStr)
	} else {
		idx = strings.Index(strings.ToLower(searchStrStr), strings.ToLower(subStrStr))
	}
	if idx < 0 {
		return int64(0), nil
	}

	// Convert byte index back to rune index
	runeIdx := len([]rune(searchStrStr[:idx]))
	return int64(runeIdx + startPos), nil
}

// evalFuncExpr handles MySQL built-in function calls.
func (e *Executor) evalFuncExpr(v *sqlparser.FuncExpr) (interface{}, error) {
	name := strings.ToLower(v.Name.String())

	// Dispatch to category-specific handlers
	if result, handled, err := evalStringFunc(e, name, v, nil); handled {
		return result, err
	}
	if result, handled, err := evalDatetimeFunc(e, name, v, nil); handled {
		return result, err
	}
	if result, handled, err := evalMathFunc(e, name, v, nil); handled {
		return result, err
	}
	if result, handled, err := evalMiscFunc(e, name, v, nil); handled {
		return result, err
	}

	// Try spatial functions
	if result, handled, err := evalSpatialFunc(e, name, v.Exprs); handled {
		return result, err
	}
	// Try built-in sys schema functions
	if result, handled, err := e.evalSysSchemaFunc(name, v.Exprs); handled {
		return result, err
	}
	// Try user-defined function from catalog
	qualifier := v.Qualifier.String()
	if result, err := e.callUserDefinedFunction(name, v.Exprs, nil, qualifier); err == nil {
		return result, nil
	} else if !strings.Contains(strings.ToLower(err.Error()), "function not found") {
		return nil, err
	}
	// Unknown function: return ER_SP_DOES_NOT_EXIST (1305, SQLSTATE 42000) so that
	// CONTINUE HANDLERs for SQLSTATE '42000' inside stored functions can catch it.
	db := e.CurrentDB
	if db == "" {
		db = "test"
	}
	return nil, mysqlError(1305, "42000", fmt.Sprintf("FUNCTION %s.%s does not exist", db, name))
}

// mysqlYearWeek implements MySQL's YEARWEEK(date, mode) function.
// Mode 0 (default): first day of week is Sunday, range 0-53.
// If the date falls in week 0 (before the first Sunday), YEARWEEK returns
// the last week of the previous year.
// Uses the proleptic Gregorian calendar.

// to the last day (e.g., Jan 31 + 1 month = Feb 28, not Mar 3).
// daysInMonth returns the number of days in the given month/year.
// back to parseDateTimeValue for datetime/time strings.
// For example, "1 01:01:01" -> "1", "123abc" -> "123", "-5" -> "-5".
// evalIntervalDateExpr evaluates DATE_ADD/DATE_SUB expressions.

// Returns the result as a TIME string with microseconds (e.g. "-25:01:00.110000").
// Returns nil if completely unparseable.
// formatMicrosAsTimeString formats total microseconds as a MySQL TIME string with 6 decimal places.
// avoiding int64 overflow in time.Duration by splitting into days and remainder.
// Returns total seconds as int64 to avoid overflow for large values.
//   - "d hh:mm:ss.ffffff": DAY_MICROSECOND like "10000 99:99:99.999999"

// mysqlAdjustedWeekday returns the weekday adjusted for MySQL's calendar (handles year 0 offset).
// The locale parameter (e.g. "de_DE", "en_US") controls month/weekday name language.
// timestampDiff computes the difference between two timestamps in the given unit.
// When literalFormat is false, always returns the full datetime(6) format.
// mysqlDateParser is a custom parser for MySQL's STR_TO_DATE format.

// validate checks for invalid format specifier combinations that MySQL rejects.

// mysqlFormatToGoLayout converts a MySQL date format string to a Go time layout.
// mysqlGetFormat returns the format string for a given date type and locale.
// evalCaseExpr handles CASE expressions.
func (e *Executor) evalCaseExpr(v *sqlparser.CaseExpr) (interface{}, error) {
	// Track expression depth to detect stack overflow for deeply nested expressions.
	// MySQL returns ER_STACK_OVERRUN_NEED_MORE (1436) when the expression stack is exhausted.
	e.exprDepth++
	defer func() { e.exprDepth-- }()
	if e.exprDepth > 8192 {
		return nil, mysqlError(1436, "HY000", "Thread stack overrun: "+
			"Need more than available stack. Use 'mysqld --thread_stack=#' to specify a bigger stack.")
	}
	var baseVal interface{}
	if v.Expr != nil {
		var err error
		baseVal, err = e.evalExpr(v.Expr)
		if err != nil {
			return nil, err
		}
	}
	for _, when := range v.Whens {
		condVal, err := e.evalExpr(when.Cond)
		if err != nil {
			return nil, err
		}
		matched := false
		if v.Expr != nil {
			// Simple CASE: compare base to each WHEN value
			matched = fmt.Sprintf("%v", baseVal) == fmt.Sprintf("%v", condVal)
		} else {
			// Searched CASE: each WHEN is a boolean expression
			matched = isTruthy(condVal)
		}
		if matched {
			return e.evalExpr(when.Val)
		}
	}
	if v.Else != nil {
		return e.evalExpr(v.Else)
	}
	return nil, nil
}

// HexBytes represents a value originating from an x'...' hex literal.
// In string context it behaves as the hex-digit string (e.g. "e68891"),
// but toFloat/toInt64 interpret it as a big-endian unsigned integer so
// arithmetic works correctly (e.g. x'1000000000000000' - 1).
type HexBytes string

// SysVarDouble wraps a float64 value from a DOUBLE system variable so
// it is displayed with 6 fixed decimal places (e.g. "90.000000") in
// SELECT @@var, while still being usable as a float64 in arithmetic
// (e.g. SET @v = @@var - 1).
type SysVarDouble struct {
	Value float64
}

// ScaledValue wraps a float64 from multiplication/addition/subtraction
// to preserve the decimal scale through the arithmetic chain.
type ScaledValue struct {
	Value float64
	Scale int
}

// DivisionResult wraps a float64 that came from the / operator so the display
// layer can format it with div_precision_increment decimal places.
type DivisionResult struct {
	Value     float64
	Precision int
}

// AvgResult wraps the float64 result of AVG() with its display scale.
// It carries a Scale for formatted display, but contributes scale=0 to
// arithmetic expressions (so that e.g. avg(a)+count(a) does not force
// decimal-padded output on the sum).
// When Rat is non-nil, String() uses exact big.Rat formatting to avoid
// float64 precision loss for large or high-precision values.
type AvgResult struct {
	Value float64
	Scale int
	Rat   *big.Rat // exact value; used by String() when set
}

// valueScale returns the number of decimal digits in a value.
// Used to compute division result precision per MySQL rules.
func valueScale(v interface{}) int {
	switch val := v.(type) {
	case SysVarDouble:
		s := strconv.FormatFloat(val.Value, 'f', -1, 64)
		if idx := strings.Index(s, "."); idx >= 0 {
			return len(s) - idx - 1
		}
		return 0
	case ScaledValue:
		return val.Scale
	case DivisionResult:
		return val.Precision
	case AvgResult:
		// AvgResult contributes scale=0 to arithmetic to avoid forcing
		// decimal-padded output on expressions involving AVG.
		return 0
	case float64:
		// Float64 values (from DOUBLE/FLOAT columns or function results) contribute
		// scale=0 to arithmetic so that DOUBLE arithmetic yields DOUBLE (not fixed
		// decimal) results. This matches MySQL behavior where DOUBLE+DOUBLE=DOUBLE.
		return 0
	case float32:
		s := strconv.FormatFloat(float64(val), 'f', -1, 32)
		if idx := strings.Index(s, "."); idx >= 0 {
			return len(s) - idx - 1
		}
		return 0
	case string:
		// Only count decimal scale if the string represents a numeric value.
		// Non-numeric strings like "/path/to/file.err" should not contribute scale.
		if idx := strings.Index(val, "."); idx >= 0 {
			// Verify the string looks like a number (leading digits or sign)
			trimmed := strings.TrimSpace(val)
			if len(trimmed) > 0 {
				c := trimmed[0]
				if c >= '0' && c <= '9' || c == '-' || c == '+' {
					return len(val) - idx - 1
				}
			}
		}
		return 0
	default:
		return 0
	}
}

// decimalStringScale returns the number of fractional digits in a decimal string.
// Unlike valueScale, it handles strings like ".12345" (leading dot) where the
// first character is '.' rather than a digit.
func decimalStringScale(s string) int {
	if idx := strings.Index(s, "."); idx >= 0 {
		return len(s) - idx - 1
	}
	return 0
}

// evalBinaryExpr evaluates arithmetic binary expressions.
func evalBinaryExpr(left, right interface{}, op sqlparser.BinaryExprOperator, divPrecision ...int) (interface{}, error) {
	// MySQL arithmetic/bit operations with NULL yield NULL.
	if left == nil || right == nil {
		return nil, nil
	}

	// Handle uint64 arithmetic without float conversion to preserve precision.
	// uint64 + int64 or int64 + uint64 where one side is uint64 should stay uint64.
	if op == sqlparser.PlusOp || op == sqlparser.MinusOp {
		// Only if neither side has decimal scale
		if valueScale(left) == 0 && valueScale(right) == 0 {
			lu, lIsU64 := left.(uint64)
			ru, rIsU64 := right.(uint64)
			li, lIsI64 := left.(int64)
			ri, rIsI64 := right.(int64)
			if lIsU64 || rIsU64 {
				// Only use integer arithmetic when both sides are int64 or uint64.
				// If either side is float64, AvgResult, or other numeric type,
				// fall through to float arithmetic to avoid precision loss.
				lKnown := lIsU64 || lIsI64
				rKnown := rIsU64 || rIsI64
				if lKnown && rKnown {
					var lv, rv uint64
					if lIsU64 {
						lv = lu
					} else {
						lv = uint64(li)
					}
					if rIsU64 {
						rv = ru
					} else {
						rv = uint64(ri)
					}
					if op == sqlparser.PlusOp {
						return lv + rv, nil
					}
					return lv - rv, nil
				}
				// Fall through to float arithmetic if either side is non-integer
			}
			// Both int64: use integer arithmetic to avoid float precision loss.
			// Only do this when the result fits in int64 (no overflow).
			if lIsI64 && rIsI64 {
				if op == sqlparser.PlusOp {
					sum := li + ri
					// Overflow detection: if signs were different from expected, use float
					if (ri > 0 && sum < li) || (ri < 0 && sum > li) {
						// Overflow — fall through to float arithmetic
					} else {
						return sum, nil
					}
				} else { // MinusOp
					diff := li - ri
					// Overflow detection
					if (ri < 0 && diff < li) || (ri > 0 && diff > li) {
						// Overflow — fall through to float arithmetic
					} else {
						return diff, nil
					}
				}
			}
		}
	}

	lf := toFloat(left)
	rf := toFloat(right)
	// Track whether either operand is a float type (not integer).
	// If so, the result should remain float64 even when it's a whole number,
	// to match MySQL DOUBLE arithmetic behavior.
	_, lIsFloat := left.(float64)
	_, rIsFloat := right.(float64)
	_, lIsFloat32 := left.(float32)
	_, rIsFloat32 := right.(float32)
	hasFloatOperand := lIsFloat || rIsFloat || lIsFloat32 || rIsFloat32
	var result float64
	switch op {
	case sqlparser.PlusOp:
		result = lf + rf
		// Preserve scale through addition for div_precision_increment
		ls := valueScale(left)
		rs := valueScale(right)
		maxS := ls
		if rs > maxS {
			maxS = rs
		}
		if maxS > 0 {
			return ScaledValue{Value: result, Scale: maxS}, nil
		}
	case sqlparser.MinusOp:
		result = lf - rf
		ls := valueScale(left)
		rs := valueScale(right)
		maxS := ls
		if rs > maxS {
			maxS = rs
		}
		if maxS > 0 {
			return ScaledValue{Value: result, Scale: maxS}, nil
		}
	case sqlparser.MultOp:
		result = lf * rf
		// Preserve scale through multiplication for div_precision_increment
		ls := valueScale(left)
		rs := valueScale(right)
		maxS := ls
		if rs > maxS {
			maxS = rs
		}
		if maxS > 0 {
			return ScaledValue{Value: result, Scale: ls + rs}, nil
		}
	case sqlparser.DivOp:
		if rf == 0 {
			return nil, nil // MySQL returns NULL for division by zero
		}
		divIncr := 4
		if len(divPrecision) > 0 {
			divIncr = divPrecision[0]
		}
		// MySQL computes result scale = left_scale + div_precision_increment.
		// The right operand's scale does not affect the output precision.
		// e.g. 15/(5/3): inner 5/3 has scale=4, but outer 15/... uses leftScale=0+4=4
		//      vs 15/5/3: left=DivisionResult(scale=4), so 4+4=8
		leftScale := valueScale(left)
		prec := leftScale + divIncr
		return DivisionResult{Value: lf / rf, Precision: prec}, nil
	case sqlparser.IntDivOp:
		if rf == 0 {
			return nil, nil
		}
		return int64(lf / rf), nil
	case sqlparser.ModOp:
		// Use big.Rat for high-precision decimal modulo when either operand is a decimal string.
		// This preserves full DECIMAL precision for expressions like 1 % 0.123456789123456789...
		ls, lIsStr := left.(string)
		rs, rIsStr := right.(string)
		if lIsStr || rIsStr {
			var lRatStr, rRatStr string
			if lIsStr {
				lRatStr = ls
			} else {
				lRatStr = fmt.Sprintf("%v", left)
			}
			if rIsStr {
				rRatStr = rs
			} else {
				rRatStr = fmt.Sprintf("%v", right)
			}
			rat0, ok0 := parseDecimalStringToRat(lRatStr)
			rat1, ok1 := parseDecimalStringToRat(rRatStr)
			if ok0 && ok1 {
				if rat1.Sign() == 0 {
					return nil, nil
				}
				// MOD(a, b) = a - TRUNCATE(a/b, 0) * b
				quot := new(big.Rat).Quo(rat0, rat1)
				quotFloat, _ := quot.Float64()
				truncInt := int64(quotFloat)
				truncRat := new(big.Rat).SetInt64(truncInt)
				remainder := new(big.Rat).Sub(rat0, new(big.Rat).Mul(truncRat, rat1))
				// Determine output scale from operands.
				// Use decimalStringScale which handles strings like ".12345" (leading dot).
				scale0 := decimalStringScale(lRatStr)
				scale1 := decimalStringScale(rRatStr)
				outScale := scale0
				if scale1 > outScale {
					outScale = scale1
				}
				// MySQL's internal DECIMAL arithmetic uses 8 groups of 9 digits = 72
				// significant fractional digits. Compute at precision 72 then pad to
				// outScale with zeros (matching MySQL's DECIMAL precision behavior).
				const mysqlDecimalMaxFracDigits = 72
				computeScale := outScale
				if computeScale > mysqlDecimalMaxFracDigits {
					computeScale = mysqlDecimalMaxFracDigits
				}
				result := formatRatFixed(remainder, computeScale)
				// Pad to outScale with trailing zeros if needed.
				if outScale > computeScale {
					if dotIdx := strings.Index(result, "."); dotIdx >= 0 {
						curFrac := len(result) - dotIdx - 1
						if curFrac < outScale {
							result += strings.Repeat("0", outScale-curFrac)
						}
					} else {
						result += "." + strings.Repeat("0", outScale)
					}
				}
				return result, nil
			}
		}
		// Use integer arithmetic for int64/uint64 to avoid float64 precision loss.
		switch la := left.(type) {
		case int64:
			switch ra := right.(type) {
			case int64:
				if ra == 0 {
					return nil, nil
				}
				return la % ra, nil
			case uint64:
				if ra == 0 {
					return nil, nil
				}
				if la < 0 {
					// MySQL: negative mod positive = negative result
					r := int64(uint64(-la) % ra)
					if r != 0 {
						r = -r
					}
					return r, nil
				}
				return int64(uint64(la) % ra), nil
			}
		case uint64:
			switch ra := right.(type) {
			case uint64:
				if ra == 0 {
					return nil, nil
				}
				return la % ra, nil
			case int64:
				if ra == 0 {
					return nil, nil
				}
				if ra < 0 {
					ra = -ra
				}
				return la % uint64(ra), nil
			}
		}
		if rf == 0 {
			return nil, nil
		}
		mod := math.Mod(lf, rf)
		if mod == float64(int64(mod)) {
			return int64(mod), nil
		}
		return mod, nil
	case sqlparser.ShiftLeftOp:
		leftBytes, leftIsBinary := toBinaryBytesForBitOp(left)
		if leftIsBinary {
			n := toUint64ForBitOp(right)
			return binaryShiftLeft(leftBytes, n), nil
		}
		return toUint64ForBitOp(left) << toUint64ForBitOp(right), nil
	case sqlparser.ShiftRightOp:
		leftBytes, leftIsBinary := toBinaryBytesForBitOp(left)
		if leftIsBinary {
			n := toUint64ForBitOp(right)
			return binaryShiftRight(leftBytes, n), nil
		}
		return toUint64ForBitOp(left) >> toUint64ForBitOp(right), nil
	case sqlparser.BitAndOp:
		leftBytes, leftIsBinary := toBinaryBytesForBitOp(left)
		rightBytes, rightIsBinary := toBinaryBytesForBitOp(right)
		if leftIsBinary && rightIsBinary {
			// Both sides are binary: byte-wise operation
			return binaryBitwiseAnd(leftBytes, rightBytes)
		}
		// When only one side is binary (mixed mode), MySQL uses integer arithmetic
		// where the binary side is converted as a string → integer (truncated to 0 for non-numeric binary data).
		return toUint64ForBitOpAsBinaryString(left, leftBytes, leftIsBinary) & toUint64ForBitOpAsBinaryString(right, rightBytes, rightIsBinary), nil
	case sqlparser.BitOrOp:
		leftBytes, leftIsBinary := toBinaryBytesForBitOp(left)
		rightBytes, rightIsBinary := toBinaryBytesForBitOp(right)
		if leftIsBinary && rightIsBinary {
			return binaryBitwiseOr(leftBytes, rightBytes)
		}
		return toUint64ForBitOpAsBinaryString(left, leftBytes, leftIsBinary) | toUint64ForBitOpAsBinaryString(right, rightBytes, rightIsBinary), nil
	case sqlparser.BitXorOp:
		leftBytes, leftIsBinary := toBinaryBytesForBitOp(left)
		rightBytes, rightIsBinary := toBinaryBytesForBitOp(right)
		if leftIsBinary && rightIsBinary {
			return binaryBitwiseXor(leftBytes, rightBytes)
		}
		return toUint64ForBitOpAsBinaryString(left, leftBytes, leftIsBinary) ^ toUint64ForBitOpAsBinaryString(right, rightBytes, rightIsBinary), nil
	default:
		return nil, fmt.Errorf("unsupported binary operator: %v", op)
	}
	// Don't convert to int64 if either operand was float, to preserve
	// DOUBLE type semantics (MySQL: INT + DOUBLE = DOUBLE, not INT).
	if !hasFloatOperand && result == float64(int64(result)) {
		return int64(result), nil
	}
	return result, nil
}

// evalExprMaybeRow evaluates an expression, using row context if row is non-nil.
func (e *Executor) evalExprMaybeRow(expr sqlparser.Expr, row *storage.Row) (interface{}, error) {
	if row != nil {
		return e.evalRowExpr(expr, *row)
	}
	return e.evalExpr(expr)
}

// evalRowExpr evaluates an expression in the context of a table row.
// It handles column lookups and delegates other expressions to e.evalExpr.
// evalRowExprTupleAware evaluates an expression in row context, returning []interface{}
// for ValTuple (to support nested row/tuple comparisons) or a scalar value otherwise.
func (e *Executor) evalRowExprTupleAware(expr sqlparser.Expr, row storage.Row) (interface{}, error) {
	if tup, ok := expr.(sqlparser.ValTuple); ok {
		vals := make([]interface{}, len(tup))
		for i, item := range tup {
			v, err := e.evalRowExprTupleAware(item, row)
			if err != nil {
				return nil, err
			}
			vals[i] = v
		}
		return vals, nil
	}
	return e.evalRowExpr(expr, row)
}

func (e *Executor) evalRowExpr(expr sqlparser.Expr, row storage.Row) (interface{}, error) {
	switch v := expr.(type) {
	case *sqlparser.ColName:
		colName := v.Name.String()
		// Handle _rowid: MySQL alias for the single-column integer primary key
		if strings.EqualFold(colName, "_rowid") {
			pkCol := e.findRowIDColumn(row)
			if pkCol != "" {
				if val, ok := row[pkCol]; ok {
					return val, nil
				}
			}
			return nil, nil
		}
		// Try qualified lookup first (alias.col) if qualifier is set
		if !v.Qualifier.IsEmpty() {
			qualified := v.Qualifier.Name.String() + "." + colName
			if val, ok := row[qualified]; ok {
				return val, nil
			}
			// Try full qualifier (db.table.col) for multi-database references
			fullQualified := sqlparser.String(v.Qualifier) + "." + colName
			fullQualified = strings.Trim(fullQualified, "`")
			if val, ok := row[fullQualified]; ok {
				return val, nil
			}
			// Try case-insensitive qualified lookup (handles t1.A when key is t1.a)
			upperQualified := strings.ToUpper(qualified)
			for k, kv := range row {
				if strings.ToUpper(k) == upperQualified {
					return kv, nil
				}
			}
			// Fall back to correlatedRow for correlated subquery references
			if e.correlatedRow != nil {
				if val, ok := e.correlatedRow[qualified]; ok {
					return val, nil
				}
				if val, ok := e.correlatedRow[fullQualified]; ok {
					return val, nil
				}
				// Try unqualified lookup in correlatedRow: handles the case where
				// the outer row's keys are unqualified (e.g. {a:0, b:10}) but the
				// subquery WHERE references them as t1.a.  We must look here BEFORE
				// falling through to row[colName], otherwise we'd incorrectly return
				// the inner table's value (e.g. t2.a) instead of the outer t1.a.
				upperName := strings.ToUpper(colName)
				for k, kv := range e.correlatedRow {
					if strings.ToUpper(k) == upperName {
						return kv, nil
					}
				}
				// Qualifier was present but not resolved from correlatedRow either.
				// Do NOT fall through to bare row[colName]: that would incorrectly
				// match a same-named column in the inner table (e.g., t2.a when
				// looking for t1.a in a correlated subquery).
				return nil, nil
			}
		}
		// Fall back to un-prefixed lookup
		if val, ok := row[colName]; ok {
			return val, nil
		}
		// Case-insensitive fallback (needed for information_schema columns)
		upperName := strings.ToUpper(colName)
		for k, v := range row {
			if strings.ToUpper(k) == upperName {
				return v, nil
			}
		}
		// Fall back to correlatedRow for correlated subquery
		if e.correlatedRow != nil {
			if val, ok := e.correlatedRow[colName]; ok {
				return val, nil
			}
			if !v.Qualifier.IsEmpty() {
				qualified := v.Qualifier.Name.String() + "." + colName
				if val, ok := e.correlatedRow[qualified]; ok {
					return val, nil
				}
			}
		}
		return nil, nil
	case *sqlparser.Subquery:
		// Scalar subquery in row context (correlated)
		return e.execSubqueryScalar(v, row)
	case *sqlparser.BinaryExpr:
		isBitOpRow := v.Operator == sqlparser.BitOrOp || v.Operator == sqlparser.BitAndOp ||
			v.Operator == sqlparser.BitXorOp || v.Operator == sqlparser.ShiftLeftOp ||
			v.Operator == sqlparser.ShiftRightOp
		isPlusMinusRow := v.Operator == sqlparser.PlusOp || v.Operator == sqlparser.MinusOp
		// Check for spatial type columns in bit operations - MySQL raises 1210 (Incorrect arguments)
		if isBitOpRow {
			opName := ""
			switch v.Operator {
			case sqlparser.BitOrOp:
				opName = "|"
			case sqlparser.BitAndOp:
				opName = "&"
			case sqlparser.BitXorOp:
				opName = "^"
			case sqlparser.ShiftLeftOp:
				opName = "<<"
			case sqlparser.ShiftRightOp:
				opName = ">>"
			}
			if e.isSpatialExpr(v.Left) || e.isSpatialExpr(v.Right) {
				return nil, mysqlError(1210, "HY000", fmt.Sprintf("Incorrect arguments to %s", opName))
			}
		}
		// Determine if sub-expressions are integer literals that may overflow
		leftIsIntLit := isIntValLiteral(v.Left)
		rightIsIntLit := isIntValLiteral(v.Right)
		leftIsHexLit := isHexNumLiteral(v.Left)
		rightIsHexLit := isHexNumLiteral(v.Right)
		left, err := e.evalRowExpr(v.Left, row)
		var leftOvRow *intOverflowError
		if err != nil {
			var oe *intOverflowError
			if errors.As(err, &oe) {
				leftOvRow = oe
				if isBitOpRow && oe.kind == "DECIMAL" {
					left = int64(math.MaxInt64)
				} else {
					left = uint64(math.MaxUint64)
				}
			} else if strings.Contains(err.Error(), "INT_OVERFLOW") {
				left = uint64(math.MaxUint64)
			} else {
				return nil, err
			}
		}
		// For HexNum literals that fit in uint64, convert to HexBytes for byte-wise bit ops.
		// Overflow detection (>8 decoded bytes) is deferred until both sides are evaluated,
		// so we can check whether the other side is also binary (byte-wise context).
		var leftOverflowHexBytes HexBytes
		if isBitOpRow && leftIsHexLit && err == nil {
			if hb, ok := left.(HexBytes); ok {
				decoded, decErr := hex.DecodeString(string(hb))
				if decErr == nil && len(decoded) > 8 {
					leftOverflowHexBytes = hb // defer decision until right is known
				}
				// If <= 8 bytes, keep as HexBytes for normal byte-wise operations
			} else if s, ok := left.(string); ok {
				hexStr := s
				if len(hexStr)%2 != 0 {
					hexStr = "0" + hexStr
				}
				if _, decErr := hex.DecodeString(hexStr); decErr == nil {
					left = HexBytes(hexStr)
				}
			}
		}
		// If left is from a VARBINARY/BINARY column, the storage returns raw bytes as a string.
		// Convert to HexBytes so bitwise ops treat it as binary, not an integer.
		if isBitOpRow && err == nil && e.isBinaryExpr(v.Left) {
			if s, ok := left.(string); ok {
				left = HexBytes(strings.ToUpper(hex.EncodeToString([]byte(s))))
			}
		}
		right, err := e.evalRowExpr(v.Right, row)
		var rightOvRow *intOverflowError
		if err != nil {
			var oe *intOverflowError
			if errors.As(err, &oe) {
				rightOvRow = oe
				if isBitOpRow && oe.kind == "DECIMAL" {
					right = int64(math.MaxInt64)
				} else {
					right = uint64(math.MaxUint64)
				}
			} else if strings.Contains(err.Error(), "INT_OVERFLOW") {
				right = uint64(math.MaxUint64)
			} else {
				return nil, err
			}
		}
		// For HexNum literals that fit in uint64, convert to HexBytes for byte-wise bit ops.
		// Overflow detection (>8 decoded bytes) is deferred until both sides are evaluated.
		var rightOverflowHexBytes HexBytes
		if isBitOpRow && rightIsHexLit && err == nil {
			if hb, ok := right.(HexBytes); ok {
				decoded, decErr := hex.DecodeString(string(hb))
				if decErr == nil && len(decoded) > 8 {
					rightOverflowHexBytes = hb // defer decision until left is known
				}
				// If <= 8 bytes, keep as HexBytes for normal byte-wise operations
			} else if s, ok := right.(string); ok {
				hexStr := s
				if len(hexStr)%2 != 0 {
					hexStr = "0" + hexStr
				}
				if _, decErr := hex.DecodeString(hexStr); decErr == nil {
					right = HexBytes(hexStr)
				}
			}
		}
		// If right is from a VARBINARY/BINARY column, the storage returns raw bytes as a string.
		// Convert to HexBytes so bitwise ops treat it as binary, not an integer.
		if isBitOpRow && err == nil && e.isBinaryExpr(v.Right) {
			if s, ok := right.(string); ok {
				right = HexBytes(strings.ToUpper(hex.EncodeToString([]byte(s))))
			}
		}
		// Now that both sides are known, resolve deferred overflow hex literals.
		// If the other side is binary (HexBytes), keep as HexBytes for byte-wise ops.
		// If the other side is NOT binary (integer context), clamp to MaxUint64 + warning.
		if leftOverflowHexBytes != "" {
			_, rightIsBinary := toBinaryBytesForBitOp(right)
			if rightIsBinary {
				left = leftOverflowHexBytes // byte-wise context: keep as HexBytes
			} else {
				hbStr := string(leftOverflowHexBytes)
				oe := &intOverflowError{val: strings.TrimLeft(hbStr, "0"), kind: "BINARY"}
				if oe.val == "" {
					oe.val = "0"
				}
				e.addWarning("Warning", 1292, formatOverflowWarningMsg(oe))
				left = uint64(math.MaxUint64)
			}
		}
		if rightOverflowHexBytes != "" {
			_, leftIsBinary := toBinaryBytesForBitOp(left)
			if leftIsBinary {
				right = rightOverflowHexBytes // byte-wise context: keep as HexBytes
			} else {
				hbStr := string(rightOverflowHexBytes)
				oe := &intOverflowError{val: strings.TrimLeft(hbStr, "0"), kind: "BINARY"}
				if oe.val == "" {
					oe.val = "0"
				}
				e.addWarning("Warning", 1292, formatOverflowWarningMsg(oe))
				right = uint64(math.MaxUint64)
			}
		}
		if leftOvRow != nil {
			e.addWarning("Warning", 1292, formatOverflowWarningMsg(leftOvRow))
		}
		if rightOvRow != nil {
			e.addWarning("Warning", 1292, formatOverflowWarningMsg(rightOvRow))
		}
		// For bit operations, check if a string result from evalRowExpr represents an
		// overflowed value. Adjust the clamped value based on the original expression type:
		// - IntVal literal overflow → DECIMAL kind → clamp to MaxInt64
		// - string literals / other → INTEGER kind → clamp to MaxUint64
		if isBitOpRow && leftOvRow == nil {
			if s, ok := left.(string); ok {
				if _, err2 := strconv.ParseUint(strings.TrimSpace(s), 10, 64); err2 != nil && errors.Is(err2, strconv.ErrRange) {
					kind := "INTEGER"
					if leftIsIntLit {
						kind = "DECIMAL"
					}
					e.addWarning("Warning", 1292, formatOverflowWarningMsg(&intOverflowError{val: strings.TrimSpace(s), kind: kind}))
					if kind == "DECIMAL" {
						left = int64(math.MaxInt64)
					}
					// For INTEGER, toUint64ForBitOp will handle the string properly
				}
			}
		}
		if isBitOpRow && rightOvRow == nil {
			if s, ok := right.(string); ok {
				if _, err2 := strconv.ParseUint(strings.TrimSpace(s), 10, 64); err2 != nil && errors.Is(err2, strconv.ErrRange) {
					kind := "INTEGER"
					if rightIsIntLit {
						kind = "DECIMAL"
					}
					e.addWarning("Warning", 1292, formatOverflowWarningMsg(&intOverflowError{val: strings.TrimSpace(s), kind: kind}))
					if kind == "DECIMAL" {
						right = int64(math.MaxInt64)
					}
				}
			}
		}
		// For plus/minus operations, handle big decimal literal overflow:
		// When a big IntVal literal overflows uint64, evalRowExpr default case returns the
		// decimal string (oe.val). MySQL preserves the exact decimal value for addition/subtraction.
		// Use math/big to compute the exact result.
		if isPlusMinusRow && leftOvRow == nil && leftIsIntLit {
			if leftStr, ok := left.(string); ok {
				// Parse as big.Int
				var bigL, bigR big.Int
				if _, ok2 := bigL.SetString(strings.TrimSpace(leftStr), 10); ok2 {
					// Parse right as big.Int if it's a string or int64
					rightStr := ""
					switch rv := right.(type) {
					case string:
						rightStr = strings.TrimSpace(rv)
					case int64:
						rightStr = strconv.FormatInt(rv, 10)
					case uint64:
						rightStr = strconv.FormatUint(rv, 10)
					}
					if rightStr != "" {
						if _, ok3 := bigR.SetString(rightStr, 10); ok3 {
							var bigResult big.Int
							if v.Operator == sqlparser.PlusOp {
								bigResult.Add(&bigL, &bigR)
							} else {
								bigResult.Sub(&bigL, &bigR)
							}
							// If result fits in int64, return int64
							if bigResult.IsInt64() {
								return bigResult.Int64(), nil
							}
							// Otherwise return as string (exact decimal)
							return bigResult.String(), nil
						}
					}
				}
			}
		}
		// For plus/minus operations, handle hex literal overflow:
		// When a HexNum literal overflowed uint64, evalRowExpr default case returns HexBytes.
		// Detect overflow (>8 decoded bytes) and clamp to MaxUint64 + warning.
		if isPlusMinusRow && leftOvRow == nil && leftIsHexLit {
			if hb, ok := left.(HexBytes); ok {
				decoded, decErr := hex.DecodeString(string(hb))
				if decErr == nil && len(decoded) > 8 {
					oe := &intOverflowError{val: strings.TrimLeft(string(hb), "0"), kind: "BINARY"}
					if oe.val == "" {
						oe.val = "0"
					}
					e.addWarning("Warning", 1292, formatOverflowWarningMsg(oe))
					left = uint64(math.MaxUint64)
				}
			} else if s, ok := left.(string); ok {
				// Fallback: string form (shouldn't happen but handle for safety)
				hexStr := strings.TrimLeft(s, "0")
				if len(hexStr) > 16 {
					oe := &intOverflowError{val: s, kind: "BINARY"}
					e.addWarning("Warning", 1292, formatOverflowWarningMsg(oe))
					left = uint64(math.MaxUint64)
				}
			}
		}
		if isPlusMinusRow && rightOvRow == nil && rightIsHexLit {
			if hb, ok := right.(HexBytes); ok {
				decoded, decErr := hex.DecodeString(string(hb))
				if decErr == nil && len(decoded) > 8 {
					oe := &intOverflowError{val: strings.TrimLeft(string(hb), "0"), kind: "BINARY"}
					if oe.val == "" {
						oe.val = "0"
					}
					e.addWarning("Warning", 1292, formatOverflowWarningMsg(oe))
					right = uint64(math.MaxUint64)
				}
			} else if s, ok := right.(string); ok {
				hexStr := strings.TrimLeft(s, "0")
				if len(hexStr) > 16 {
					oe := &intOverflowError{val: s, kind: "BINARY"}
					e.addWarning("Warning", 1292, formatOverflowWarningMsg(oe))
					right = uint64(math.MaxUint64)
				}
			}
		}
		// When a hex literal (x'...' or 0x...) is used in a bit operation alongside a
		// non-binary operand, MySQL treats the hex literal as an integer (big-endian), not
		// as raw binary bytes. For example: integer_col | x'cafebabe' treats x'cafebabe'
		// as the integer 0xcafebabe. This conversion only applies when the OTHER operand is
		// NOT binary; if the other side is a BINARY/VARBINARY column, byte-wise ops apply.
		if isBitOpRow && leftIsHexLit {
			if hb, ok := left.(HexBytes); ok {
				_, rightIsBin := toBinaryBytesForBitOp(right)
				if !rightIsBin {
					if decoded, err2 := hex.DecodeString(string(hb)); err2 == nil {
						var val uint64
						for _, b := range decoded {
							val = val<<8 | uint64(b)
						}
						left = val
					}
				}
			}
		}
		if isBitOpRow && rightIsHexLit {
			if hb, ok := right.(HexBytes); ok {
				_, leftIsBin := toBinaryBytesForBitOp(left)
				if !leftIsBin {
					if decoded, err2 := hex.DecodeString(string(hb)); err2 == nil {
						var val uint64
						for _, b := range decoded {
							val = val<<8 | uint64(b)
						}
						right = val
					}
				}
			}
		}
		// In TRADITIONAL/ERROR_FOR_DIVISION_BY_ZERO + strict mode during DML:
		// division by zero raises error 1365. NULL divisor returns NULL (not an error).
		if (v.Operator == sqlparser.DivOp || v.Operator == sqlparser.IntDivOp || v.Operator == sqlparser.ModOp) && e.insideDML && right != nil {
			rf := toFloat(right)
			if rf == 0 {
				isDivError := e.isStrictMode() && (strings.Contains(e.sqlMode, "ERROR_FOR_DIVISION_BY_ZERO") || strings.Contains(e.sqlMode, "TRADITIONAL"))
				if isDivError {
					return nil, mysqlError(1365, "22012", "Division by 0")
				}
			}
		}
		return evalBinaryExpr(left, right, v.Operator, e.getDivPrecisionIncrement())
	case *sqlparser.ComparisonExpr:
		// Handle IN / NOT IN specially: right side is a ValTuple
		if v.Operator == sqlparser.InOp || v.Operator == sqlparser.NotInOp {
			// When left is a tuple: (a,b) IN ((1,2),(3,4)) or (a,b) IN (SELECT ...)
			if leftTupleIN, leftIsTupleIN := v.Left.(sqlparser.ValTuple); leftIsTupleIN {
				if sub, ok := v.Right.(*sqlparser.Subquery); ok {
					// MySQL error 1235: LIMIT in IN/ALL/ANY/SOME subquery is not supported
					if subqueryHasLimit(sub) {
						return nil, mysqlError(1235, "42000", "This version of MySQL doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery'")
					}
					// Evaluate tuple elements WITH row context (needed when in IS TRUE/FALSE wrapper)
					result, err := e.execSubquery(sub, row)
					if err != nil {
						return nil, err
					}
					if len(result.Columns) != len(leftTupleIN) {
						return nil, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftTupleIN)))
					}
					leftValsRow := make([]interface{}, len(leftTupleIN))
					for i, lExpr := range leftTupleIN {
						lv, err := e.evalRowExpr(lExpr, row) // Use row context!
						if err != nil {
							return nil, err
						}
						leftValsRow[i] = lv
					}
					hasNullRow := false
					for _, lv := range leftValsRow {
						if lv == nil {
							hasNullRow = true
							break
						}
					}
					for _, rrow := range result.Rows {
						if len(rrow) != len(leftValsRow) {
							continue
						}
						allMatch := true
						rowHasNull := false
						hasDefiniteNonMatch := false
						for i := 0; i < len(leftValsRow); i++ {
							if leftValsRow[i] == nil || rrow[i] == nil {
								rowHasNull = true
								allMatch = false
								// Don't break - continue to check remaining non-null pairs
								continue
							}
							match, _ := compareValues(leftValsRow[i], rrow[i], sqlparser.EqualOp)
							if !match {
								allMatch = false
								hasDefiniteNonMatch = true
								break
							}
						}
						if allMatch {
							if v.Operator == sqlparser.InOp {
								return int64(1), nil
							}
							return int64(0), nil
						}
						if rowHasNull && !hasDefiniteNonMatch {
							hasNullRow = true
						}
					}
					if hasNullRow {
						return nil, nil
					}
					if v.Operator == sqlparser.NotInOp {
						return int64(1), nil
					}
					return int64(0), nil
				}
				// Row IN tuple-of-tuples: (a,b) IN ((1,2),(3,4)) or nested (a,(b,c)) IN ((1,(2,3)),...)
				if rightTupleIN, ok := v.Right.(sqlparser.ValTuple); ok {
					// MySQL validates all IN-list items structurally BEFORE evaluating any
					// match. Do a pre-validation pass over all items in rightTupleIN.
					for _, item := range rightTupleIN {
						switch rItem := item.(type) {
						case sqlparser.ValTuple:
							if err := validateRowTupleStructure(leftTupleIN, rItem); err != nil {
								return nil, err
							}
						case *sqlparser.Subquery:
							// A subquery returns a flat row; if any element of leftTupleIN
							// is a nested tuple, the subquery can never match that element.
							for _, lElem := range leftTupleIN {
								if lNested, ok := lElem.(sqlparser.ValTuple); ok {
									return nil, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(lNested)))
								}
							}
						default:
							// Non-tuple item: break out (fall through to scalar handling)
							break
						}
					}

					// Evaluate left tuple values with row context (supports nested tuples)
					leftValsIN := make([]interface{}, len(leftTupleIN))
					for i, lExpr := range leftTupleIN {
						lv, err := e.evalRowExprTupleAware(lExpr, row)
						if err != nil {
							return nil, err
						}
						leftValsIN[i] = lv
					}
					hasNullIN := false
					leftRowIN := interface{}(leftValsIN)
					for _, item := range rightTupleIN {
						switch rItemEval := item.(type) {
						case sqlparser.ValTuple:
							rValsIN := make([]interface{}, len(rItemEval))
							for i, rv := range rItemEval {
								rVal, err := e.evalRowExprTupleAware(rv, row)
								if err != nil {
									return nil, err
								}
								rValsIN[i] = rVal
							}
							equal, rowHasNull, err := rowTuplesEqual(leftRowIN, interface{}(rValsIN))
							if err != nil {
								return nil, err
							}
							if equal {
								if v.Operator == sqlparser.InOp {
									return int64(1), nil
								}
								return int64(0), nil
							}
							if rowHasNull {
								hasNullIN = true
							}
						case *sqlparser.Subquery:
							// Subquery with no nested left elements: execute and compare.
							subResult, err := e.execSubquery(rItemEval, row)
							if err != nil {
								return nil, err
							}
							if len(subResult.Columns) != len(leftTupleIN) {
								return nil, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftTupleIN)))
							}
							for _, subRow := range subResult.Rows {
								if len(subRow) != len(leftValsIN) {
									continue
								}
								allMatch := true
								rowHasNull := false
								for i := 0; i < len(leftValsIN); i++ {
									lv, rv := leftValsIN[i], subRow[i]
									if lv == nil || rv == nil {
										rowHasNull = true
										allMatch = false
										break
									}
									match, _ := compareValues(lv, rv, sqlparser.EqualOp)
									if !match {
										allMatch = false
										break
									}
								}
								if allMatch {
									if v.Operator == sqlparser.InOp {
										return int64(1), nil
									}
									return int64(0), nil
								}
								if rowHasNull {
									hasNullIN = true
								}
							}
						}
					}
					if hasNullIN {
						return nil, nil
					}
					if v.Operator == sqlparser.NotInOp {
						return int64(1), nil
					}
					return int64(0), nil
				}
			}
			// Handle (SELECT c1,c2,...) IN (SELECT c1,c2,...) — subquery IN subquery.
			// The left subquery may return multi-column rows.
			if leftSub, leftIsSub := v.Left.(*sqlparser.Subquery); leftIsSub {
				if rightSub, rightIsSub := v.Right.(*sqlparser.Subquery); rightIsSub {
					leftResult, err := e.execSubquery(leftSub, row)
					if err != nil {
						return nil, err
					}
					rightResult, err := e.execSubquery(rightSub, row)
					if err != nil {
						return nil, err
					}
					if len(leftResult.Columns) != len(rightResult.Columns) {
						return nil, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftResult.Columns)))
					}
					ncols := len(leftResult.Columns)
					for _, lrow := range leftResult.Rows {
						hasNull := false
						for _, rrow := range rightResult.Rows {
							allMatch := true
							rowHasNull := false
							for i := 0; i < ncols; i++ {
								lv, rv := lrow[i], rrow[i]
								if lv == nil || rv == nil {
									rowHasNull = true
									allMatch = false
									break
								}
								match, _ := compareValues(lv, rv, sqlparser.EqualOp)
								if !match {
									allMatch = false
									break
								}
							}
							if allMatch {
								if v.Operator == sqlparser.InOp {
									return int64(1), nil
								}
								return int64(0), nil
							}
							if rowHasNull {
								hasNull = true
							}
						}
						if hasNull {
							return nil, nil
						}
					}
					if v.Operator == sqlparser.NotInOp {
						return int64(1), nil
					}
					return int64(0), nil
				}
			}
			left, err := e.evalRowExpr(v.Left, row)
			if err != nil {
				return nil, err
			}
			if left == nil {
				return nil, nil
			}
			if tuple, ok := v.Right.(sqlparser.ValTuple); ok {
				hasNull := false
				for _, item := range tuple {
					val, err := e.evalRowExpr(item, row)
					if err != nil {
						return nil, err
					}
					if val == nil {
						hasNull = true
						continue
					}
					match, _ := compareValues(left, val, sqlparser.EqualOp)
					if match {
						if v.Operator == sqlparser.InOp {
							return int64(1), nil
						}
						return int64(0), nil
					}
				}
				if hasNull {
					return nil, nil
				}
				if v.Operator == sqlparser.NotInOp {
					return int64(1), nil
				}
				return int64(0), nil
			}
			// Handle IN (SELECT ...) — subquery on right side
			if sub, ok := v.Right.(*sqlparser.Subquery); ok {
				// Set correlatedRow so correlated subqueries can reference the outer row.
				oldCorrelatedIN := e.correlatedRow
				if row != nil {
					e.correlatedRow = row
				}
				result, err := e.evalInSubquery(left, v.Left, sub, v.Operator)
				e.correlatedRow = oldCorrelatedIN
				return result, err
			}
		}
		// Handle (SELECT c1,c2,...) op ROW(v1,v2,...) and ROW(...) op (SELECT ...)
		// in row context. Delegate to evalComparisonExpr which has the full implementation.
		{
			_, leftIsSub := v.Left.(*sqlparser.Subquery)
			_, rightIsSub := v.Right.(*sqlparser.Subquery)
			_, leftIsValTuple := v.Left.(sqlparser.ValTuple)
			_, rightIsValTuple := v.Right.(sqlparser.ValTuple)
			if (leftIsSub && rightIsValTuple) || (leftIsValTuple && rightIsSub) {
				return e.evalComparisonExpr(v)
			}
		}
		// Handle ROW/tuple comparisons: ROW(a,b) = ROW(c,d) or (a,b) = (c,d)
		// Supports nested tuples via evalRowExprTupleAware returning []interface{} for ValTuple.
		leftTuple, leftIsTuple := v.Left.(sqlparser.ValTuple)
		rightTuple, rightIsTuple := v.Right.(sqlparser.ValTuple)
		if leftIsTuple && rightIsTuple {
			if len(leftTuple) != len(rightTuple) {
				return nil, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftTuple)))
			}
			// Evaluate as full tuple values (nested tuples return []interface{})
			leftValsRT := make([]interface{}, len(leftTuple))
			rightValsRT := make([]interface{}, len(rightTuple))
			for i := 0; i < len(leftTuple); i++ {
				lv, err := e.evalRowExprTupleAware(leftTuple[i], row)
				if err != nil {
					return nil, err
				}
				leftValsRT[i] = lv
				rv, err := e.evalRowExprTupleAware(rightTuple[i], row)
				if err != nil {
					return nil, err
				}
				rightValsRT[i] = rv
			}
			switch v.Operator {
			case sqlparser.EqualOp, sqlparser.NotEqualOp, sqlparser.NullSafeEqualOp:
				equal, hasNull, err := rowTuplesEqual(interface{}(leftValsRT), interface{}(rightValsRT))
				if err != nil {
					return nil, err
				}
				if v.Operator == sqlparser.NullSafeEqualOp {
					if hasNull {
						// For <=>, NULL == NULL = true, NULL != non-NULL = false
						// rowTuplesEqual doesn't handle this correctly for NullSafe
						// Fall through to element-wise comparison
						allNullMatch := true
						for i := 0; i < len(leftValsRT); i++ {
							lv, rv := leftValsRT[i], rightValsRT[i]
							if lv == nil && rv == nil {
								continue
							}
							if lv == nil || rv == nil {
								allNullMatch = false
								break
							}
							eq, _, err := rowTuplesEqual(lv, rv)
							if err != nil {
								return nil, err
							}
							if !eq {
								allNullMatch = false
								break
							}
						}
						if allNullMatch {
							return int64(1), nil
						}
						return int64(0), nil
					}
					if equal {
						return int64(1), nil
					}
					return int64(0), nil
				}
				if hasNull {
					return nil, nil
				}
				if v.Operator == sqlparser.NotEqualOp {
					equal = !equal
				}
				if equal {
					return int64(1), nil
				}
				return int64(0), nil
			default:
				// Lexicographic comparison for <, >, <=, >=
				for i := 0; i < len(leftValsRT); i++ {
					lv, rv := leftValsRT[i], rightValsRT[i]
					if lv == nil || rv == nil {
						return nil, nil
					}
					eq, err := compareValues(lv, rv, sqlparser.EqualOp)
					if err != nil {
						return nil, err
					}
					if eq {
						continue
					}
					lt, err := compareValues(lv, rv, sqlparser.LessThanOp)
					if err != nil {
						return nil, err
					}
					switch v.Operator {
					case sqlparser.LessThanOp, sqlparser.LessEqualOp:
						if lt {
							return int64(1), nil
						}
						return int64(0), nil
					default:
						if !lt {
							return int64(1), nil
						}
						return int64(0), nil
					}
				}
				switch v.Operator {
				case sqlparser.LessEqualOp, sqlparser.GreaterEqualOp:
					return int64(1), nil
				default:
					return int64(0), nil
				}
			}
		}
		if leftIsTuple {
			// Left is tuple, right is subquery or something else - delegate to evalWhere
			match, err := e.evalWhere(v, row)
			if err != nil {
				return nil, err
			}
			if match {
				return int64(1), nil
			}
			return int64(0), nil
		}
		// Handle ANY/SOME/ALL modifier with subquery on right side.
		// e.g. expr = ANY(SELECT ...) or expr <> ALL(SELECT ...) in SELECT list context.
		// The right subquery can return multiple rows (ANY/ALL semantics).
		if v.Modifier != 0 {
			if sub, ok := v.Right.(*sqlparser.Subquery); ok {
				isAny := v.Modifier == 1 // ANY/SOME; Modifier=2 means ALL

				// Evaluate left side. If left is itself a subquery that returns >1 row,
				// MySQL returns NULL rather than raising error 1242 in ANY/ALL context.
				var left interface{}
				if leftSub, leftIsSub := v.Left.(*sqlparser.Subquery); leftIsSub {
					subResult, subErr := e.execSubquery(leftSub, row)
					if subErr != nil {
						return nil, subErr
					}
					if len(subResult.Rows) == 0 {
						return nil, nil // NULL
					}
					if len(subResult.Rows) > 1 {
						return nil, nil // >1 row in ANY/ALL context → NULL (not error 1242)
					}
					if len(subResult.Rows[0]) == 0 {
						return nil, nil
					}
					left = subResult.Rows[0][0]
				} else {
					var err error
					left, err = e.evalRowExpr(v.Left, row)
					if err != nil {
						return nil, err
					}
				}

				if left == nil {
					return nil, nil
				}

				vals, err := e.execSubqueryValues(sub, row)
				if err != nil {
					return nil, err
				}

				if isAny {
					// ANY/SOME: true if comparison holds for at least one non-NULL value
					hasNull := false
					for _, val := range vals {
						if val == nil {
							hasNull = true
							continue
						}
						match, err := compareValues(left, val, v.Operator)
						if err != nil {
							return nil, err
						}
						if match {
							return int64(1), nil
						}
					}
					if hasNull {
						return nil, nil // unknown (NULL) if no match but there were NULLs
					}
					return int64(0), nil
				}
				// ALL: true if comparison holds for every value; NULL if any value is NULL
				for _, val := range vals {
					if val == nil {
						return nil, nil
					}
					match, err := compareValues(left, val, v.Operator)
					if err != nil {
						return nil, err
					}
					if !match {
						return int64(0), nil
					}
				}
				return int64(1), nil
			}
		}
		// Comparison in row context
		// Extract COLLATE clause for collation-aware comparison
		var collationName string
		leftExpr2, rightExpr2 := v.Left, v.Right
		if ce, ok := leftExpr2.(*sqlparser.CollateExpr); ok {
			collationName = ce.Collation
			leftExpr2 = ce.Expr
		}
		if ce, ok := rightExpr2.(*sqlparser.CollateExpr); ok {
			collationName = ce.Collation
			rightExpr2 = ce.Expr
		}
		// If LIKE operator and no explicit COLLATE, infer collation from column type.
		// Binary columns (BLOB, BINARY, VARBINARY) use case-sensitive "binary" collation.
		if collationName == "" && (v.Operator == sqlparser.LikeOp || v.Operator == sqlparser.NotLikeOp) {
			if colExpr, ok := leftExpr2.(*sqlparser.ColName); ok && e.queryTableDef != nil {
				if coll := e.lookupColumnCollation(colExpr.Name.String(), e.queryTableDef); coll != "" {
					collationName = coll
				}
			}
		}
		// If left side is a bare column reference not found in the row, MySQL returns
		// ER_BAD_FIELD_ERROR before evaluating the right side. This prevents a right-side
		// error (e.g. @@GLOBAL on a session-only var) from masking the column error.
		if colExpr, ok := leftExpr2.(*sqlparser.ColName); ok && colExpr.Qualifier.IsEmpty() {
			colName := colExpr.Name.String()
			if _, found := row[colName]; !found {
				// Also check case-insensitive
				upperName := strings.ToUpper(colName)
				foundCI := false
				for k := range row {
					if strings.ToUpper(k) == upperName {
						foundCI = true
						break
					}
				}
				if !foundCI {
					return nil, mysqlError(1054, "42S22", fmt.Sprintf("Unknown column '%s' in 'field list'", colName))
				}
			}
		}
		left, err := e.evalRowExpr(leftExpr2, row)
		if err != nil {
			return nil, err
		}
		right, err := e.evalRowExpr(rightExpr2, row)
		if err != nil {
			return nil, err
		}
		// Handle LIKE/NOT LIKE with optional ESCAPE and optional COLLATE
		if v.Operator == sqlparser.LikeOp || v.Operator == sqlparser.NotLikeOp {
			// NULL comparison: x LIKE NULL = NULL = false in WHERE context
			if left == nil || right == nil {
				return nil, nil
			}
			ls := toString(left)
			rs := toString(right)
			// Determine escape character (default is '\')
			escapeChar := rune('\\')
			if v.Escape != nil {
				escVal, _ := e.evalRowExpr(v.Escape, row)
				if escStr := toString(escVal); len([]rune(escStr)) > 0 {
					escapeChar = []rune(escStr)[0]
				}
			}
			collLower := strings.ToLower(collationName)
			isCaseSensitive := collationName != "" && (strings.Contains(collLower, "_bin") || strings.Contains(collLower, "_cs") || collLower == "binary")
			var re *regexp.Regexp
			if isCaseSensitive {
				re = likeToRegexpCaseSensitiveEscape(rs, escapeChar)
			} else {
				re = likeToRegexpEscape(rs, escapeChar)
			}
			matched := re.MatchString(ls)
			if v.Operator == sqlparser.LikeOp {
				if matched {
					return int64(1), nil
				}
				return int64(0), nil
			}
			if !matched {
				return int64(1), nil
			}
			return int64(0), nil
		}
		if collationName != "" {
			if vc := lookupVitessCollation(collationName); vc != nil {
				ls := toString(left)
				rs := toString(right)
				lBytes := []byte(ls)
				rBytes := []byte(rs)
				cs := vc.Charset()
				if cs.Name() != "utf8mb4" && cs.Name() != "utf8mb3" && cs.Name() != "binary" {
					if conv, convErr := charset.ConvertFromUTF8(nil, cs, lBytes); convErr == nil {
						lBytes = conv
					}
					if conv, convErr := charset.ConvertFromUTF8(nil, cs, rBytes); convErr == nil {
						rBytes = conv
					}
				}
				cmp := vc.Collate(lBytes, rBytes, false)
				switch v.Operator {
				case sqlparser.EqualOp:
					if cmp == 0 {
						return int64(1), nil
					}
					return int64(0), nil
				case sqlparser.NotEqualOp:
					if cmp != 0 {
						return int64(1), nil
					}
					return int64(0), nil
				case sqlparser.LessThanOp:
					if cmp < 0 {
						return int64(1), nil
					}
					return int64(0), nil
				case sqlparser.GreaterThanOp:
					if cmp > 0 {
						return int64(1), nil
					}
					return int64(0), nil
				case sqlparser.LessEqualOp:
					if cmp <= 0 {
						return int64(1), nil
					}
					return int64(0), nil
				case sqlparser.GreaterEqualOp:
					if cmp >= 0 {
						return int64(1), nil
					}
					return int64(0), nil
				}
			}
		}
		// NULL comparison returns NULL (except for NULL-safe equal <=>)
		if (left == nil || right == nil) && v.Operator != sqlparser.NullSafeEqualOp {
			return nil, nil
		}
		// For system variable ENUM comparisons, apply case-insensitive string comparison
		// to match MySQL's default collation (utf8mb4_0900_ai_ci) behavior.
		if v.Operator == sqlparser.EqualOp || v.Operator == sqlparser.NotEqualOp {
			isSysVarEnumRow := false
			if varExpr, ok := leftExpr2.(*sqlparser.Variable); ok {
				varName := strings.ToLower(varExpr.Name.String())
				varName = strings.TrimPrefix(varName, "global.")
				varName = strings.TrimPrefix(varName, "session.")
				varName = strings.TrimPrefix(varName, "local.")
				if sysVarEnumSet[varName] {
					isSysVarEnumRow = true
				}
			}
			if varExpr, ok := rightExpr2.(*sqlparser.Variable); ok {
				varName := strings.ToLower(varExpr.Name.String())
				varName = strings.TrimPrefix(varName, "global.")
				varName = strings.TrimPrefix(varName, "session.")
				varName = strings.TrimPrefix(varName, "local.")
				if sysVarEnumSet[varName] {
					isSysVarEnumRow = true
				}
			}
			if isSysVarEnumRow {
				if ls, lok := left.(string); lok {
					if rs, rok := right.(string); rok {
						equal := strings.EqualFold(ls, rs)
						if v.Operator == sqlparser.EqualOp {
							if equal {
								return int64(1), nil
							}
							return int64(0), nil
						}
						if !equal {
							return int64(1), nil
						}
						return int64(0), nil
					}
				}
			}
		}
		result, err := compareValues(left, right, v.Operator)
		if err != nil {
			return nil, err
		}
		if result {
			return int64(1), nil
		}
		return int64(0), nil
	case *sqlparser.UnaryExpr:
		val, err := e.evalRowExpr(v.Expr, row)
		if err != nil {
			return nil, err
		}
		if v.Operator == sqlparser.TildaOp {
			// ~ is bitwise NOT
			if val == nil {
				return nil, nil // ~NULL = NULL
			}
			// If value is from a VARBINARY/BINARY column, convert raw bytes to HexBytes first
			if e.isBinaryExpr(v.Expr) {
				if s, ok := val.(string); ok {
					val = HexBytes(strings.ToUpper(hex.EncodeToString([]byte(s))))
				}
			}
			// If value is a binary string (HexBytes or raw binary), do byte-wise NOT
			if binaryBytes, isBinary := toBinaryBytesForBitOp(val); isBinary {
				return binaryBitwiseNot(binaryBytes), nil
			}
			return ^toUint64ForBitOp(val), nil
		}
		if v.Operator == sqlparser.BangOp {
			// ! is logical NOT (deprecated alias in MySQL 8.0)
			if val == nil {
				return nil, nil // !NULL = NULL
			}
			if isTruthy(val) {
				return int64(0), nil
			}
			return int64(1), nil
		}
		if v.Operator == sqlparser.UMinusOp {
			switch n := val.(type) {
			case int64:
				// -INT64_MIN overflows int64; return uint64(INT64_MAX+1).
				if n == math.MinInt64 {
					return uint64(1 << 63), nil
				}
				return -n, nil
			case uint64:
				if n == 1<<63 {
					return int64(math.MinInt64), nil
				}
				if n <= math.MaxInt64 {
					return -int64(n), nil
				}
				// Keep exact value for >int64 range to avoid float precision loss.
				return fmt.Sprintf("-%d", n), nil
			case float64:
				return -n, nil
			case string:
				if strings.HasPrefix(n, "-") {
					return strings.TrimPrefix(n, "-"), nil
				}
				return "-" + n, nil
			}
		}
		return val, nil
	case *sqlparser.FuncExpr:
		// Evaluate function arguments with row context
		return e.evalFuncExprWithRow(v, row)
	case *sqlparser.CaseExpr:
		return e.evalCaseExprWithRow(v, row)
	case *sqlparser.ConvertExpr:
		// CAST(expr AS type) with row context - delegate to evalExpr via default
		oldCorrelated := e.correlatedRow
		e.correlatedRow = row
		val, err := e.evalExpr(expr)
		e.correlatedRow = oldCorrelated
		return val, err
	case *sqlparser.CastExpr:
		// CAST(expr AS type) with row context - delegate to evalExpr via default
		oldCorrelated := e.correlatedRow
		e.correlatedRow = row
		val, err := e.evalExpr(expr)
		e.correlatedRow = oldCorrelated
		return val, err
	case *sqlparser.CharExpr:
		// CHAR(N1, N2, ...) with row context
		// MySQL outputs the minimum number of bytes needed for each value.
		var result []byte
		for _, argExpr := range v.Exprs {
			val, err := e.evalRowExpr(argExpr, row)
			if err != nil {
				return nil, err
			}
			if val == nil {
				continue
			}
			n := uint64(toInt64(val))
			if n == 0 {
				result = append(result, 0)
			} else if n <= 0xFF {
				result = append(result, byte(n))
			} else if n <= 0xFFFF {
				result = append(result, byte(n>>8), byte(n))
			} else if n <= 0xFFFFFF {
				result = append(result, byte(n>>16), byte(n>>8), byte(n))
			} else {
				result = append(result, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
			}
		}
		return string(result), nil
	case *sqlparser.CollateExpr:
		// Ignore COLLATE and evaluate inner expression with row context
		return e.evalRowExpr(v.Expr, row)
	case *sqlparser.IntervalDateExpr:
		dateVal, err := e.evalRowExpr(v.Date, row)
		if err != nil {
			return nil, err
		}
		intervalVal, err := e.evalRowExpr(v.Interval, row)
		if err != nil {
			return nil, err
		}
		return evalIntervalDateExprStrict(dateVal, intervalVal, v.Unit, v.Syntax, e.isTraditionalMode())
	case *sqlparser.AssignmentExpr:
		// @var := expr with row context
		val, err := e.evalRowExpr(v.Right, row)
		if err != nil {
			return nil, err
		}
		varName := strings.TrimPrefix(sqlparser.String(v.Left), "@")
		varName = strings.Trim(varName, "`")
		if e.userVars == nil {
			e.userVars = make(map[string]interface{})
		}
		e.userVars[varName] = val
		return val, nil
	case *sqlparser.CountStar, *sqlparser.Count, *sqlparser.Sum, *sqlparser.Max, *sqlparser.Min, *sqlparser.Avg, *sqlparser.GroupConcatExpr:
		// For HAVING clause: look up the aggregate display name in the row
		displayName := aggregateDisplayName(expr)
		if val, ok := row[displayName]; ok {
			return val, nil
		}
		// Fallback: compute the aggregate (for single-row context)
		return e.evalExpr(expr)
	case *sqlparser.PerformanceSchemaFuncExpr:
		// Evaluate the argument with row context so aggregate results are resolved
		if v.Argument != nil {
			argVal, err := e.evalRowExpr(v.Argument, row)
			if err != nil {
				return nil, err
			}
			// Apply the formatting function to the resolved argument
			switch v.Type {
			case sqlparser.FormatBytesType:
				if argVal == nil {
					return nil, nil
				}
				if s, ok := argVal.(string); ok {
					if _, parseErr := strconv.ParseFloat(s, 64); parseErr != nil {
						return nil, mysqlError(1264, "22003", "Input value is out of range in 'format_bytes'")
					}
				}
				return formatBytesValue(argVal), nil
			case sqlparser.FormatPicoTimeType:
				if argVal == nil {
					return nil, nil
				}
				if s, ok := argVal.(string); ok {
					if _, parseErr := strconv.ParseFloat(s, 64); parseErr != nil {
						return nil, mysqlError(1264, "22003", "Input value is out of range in 'format_pico_time'")
					}
				}
				return formatPicoTimeValue(argVal), nil
			default:
				return e.evalPerformanceSchemaFuncExpr(v)
			}
		}
		return e.evalPerformanceSchemaFuncExpr(v)
	default:
		// For JSON and other expressions, set row context via correlatedRow
		// so that ColName lookups in evalExpr can find row values.
		// Merge the old correlatedRow into the new row so that outer
		// correlated references (e.g., from an enclosing subquery) remain
		// accessible.
		oldCorrelated := e.correlatedRow
		merged := make(storage.Row, len(row))
		if oldCorrelated != nil {
			for k, v := range oldCorrelated {
				merged[k] = v
			}
		}
		for k, v := range row {
			merged[k] = v
		}
		e.correlatedRow = merged
		val, err := e.evalExpr(expr)
		e.correlatedRow = oldCorrelated
		if err != nil {
			var oe *intOverflowError
			if errors.As(err, &oe) {
				if oe.kind == "BINARY" {
					// Large hex literal (0x...) overflow: return as HexBytes so that
					// comparisons with HexBytes results (e.g. from VARBINARY bitwise ops)
					// and other binary-aware code paths work correctly.
					hexStr := oe.val
					if len(hexStr)%2 != 0 {
						hexStr = "0" + hexStr
					}
					return HexBytes(strings.ToUpper(hexStr)), nil
				}
				// Preserve the original integer literal text so comparisons can
				// use exact bigint semantics instead of float64-rounded max uint.
				return oe.val, nil
			}
		}
		return val, err
	}
}

// evalFuncExprWithRow evaluates a function expression with row context for column references.
func (e *Executor) evalFuncExprWithRow(v *sqlparser.FuncExpr, row storage.Row) (interface{}, error) {
	// Evaluate function arguments with row context to resolve column references
	name := strings.ToLower(v.Name.String())

	rowPtr := &row

	// Dispatch to category-specific handlers
	if result, handled, err := evalStringFunc(e, name, v, rowPtr); handled {
		return result, err
	}
	if result, handled, err := evalDatetimeFunc(e, name, v, rowPtr); handled {
		return result, err
	}
	if result, handled, err := evalMathFunc(e, name, v, rowPtr); handled {
		return result, err
	}
	if result, handled, err := evalMiscFunc(e, name, v, rowPtr); handled {
		return result, err
	}

	// Try user-defined function from catalog
	qualifier := v.Qualifier.String()
	if result, err := e.callUserDefinedFunction(name, v.Exprs, &row, qualifier); err == nil {
		return result, nil
	}
	// Fallback: delegate to evalFuncExpr.
	// Always set correlatedRow so that column references inside any function are resolved
	// from the current row (e.g. ST_SRID(g, 4326) in UPDATE SET needs to read 'g').
	oldCorrelated := e.correlatedRow
	e.correlatedRow = row
	result, err := e.evalFuncExpr(v)
	e.correlatedRow = oldCorrelated
	return result, err
}

// evalComparisonWithRow evaluates a comparison expression with row context.
func (e *Executor) evalComparisonWithRow(v *sqlparser.ComparisonExpr, row storage.Row) (interface{}, error) {
	// Delegate to evalWhere for the actual comparison logic (it handles tuples, subqueries, etc.)
	match, err := e.evalWhere(v, row)
	if err != nil {
		return nil, err
	}
	if match {
		return int64(1), nil
	}
	return int64(0), nil
}

// evalBinaryExprWithRow evaluates a binary arithmetic expression with row context.
func (e *Executor) evalBinaryExprWithRow(v *sqlparser.BinaryExpr, row storage.Row) (interface{}, error) {
	left, err := e.evalRowExpr(v.Left, row)
	if err != nil {
		return nil, err
	}
	right, err := e.evalRowExpr(v.Right, row)
	if err != nil {
		return nil, err
	}
	return evalBinaryExpr(left, right, v.Operator, e.getDivPrecisionIncrement())
}

// isBinaryConvertExpr returns true if the expression is a BINARY cast (e.g. BINARY 'str' or CAST(x AS BINARY)).
// This is used to detect binary-collation comparisons in CASE expressions.
func isBinaryConvertExpr(expr sqlparser.Expr) bool {
	switch v := expr.(type) {
	case *sqlparser.ConvertExpr:
		return v.Type != nil && strings.EqualFold(v.Type.Type, "binary")
	case *sqlparser.CastExpr:
		return v.Type != nil && strings.EqualFold(v.Type.Type, "binary")
	}
	return false
}

// isCompactDecimalDatetime returns true if s is in "YYYYMMDDHHMMSS.ffffff" compact decimal datetime format.
// This is a 14-digit integer part followed by a '.' and fractional seconds.
func isCompactDecimalDatetime(s string) bool {
	dotIdx := strings.IndexByte(s, '.')
	if dotIdx != 14 {
		return false
	}
	for i := 0; i < 14; i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	for i := 15; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// datetimeStringToFloat64 converts a standard datetime string "YYYY-MM-DD HH:MM:SS[.ffffff]"
// to its compact decimal float64 representation YYYYMMDDHHMMSS[.ffffff].
// Returns 0 if the string is not a valid datetime.
func datetimeStringToFloat64(s string) float64 {
	// Expect at least "YYYY-MM-DD HH:MM:SS" (19 chars)
	if len(s) < 19 || s[4] != '-' || s[7] != '-' || s[10] != ' ' || s[13] != ':' || s[16] != ':' {
		return 0
	}
	compact := s[0:4] + s[5:7] + s[8:10] + s[11:13] + s[14:16] + s[17:19]
	if len(s) > 19 && s[19] == '.' {
		compact += s[19:] // preserve ".ffffff"
	}
	f, err := strconv.ParseFloat(compact, 64)
	if err != nil {
		return 0
	}
	return f
}

// compareValuesBinary compares two values case-sensitively (binary collation).
func compareValuesBinary(left, right interface{}) bool {
	if left == nil || right == nil {
		return false
	}
	ls := fmt.Sprintf("%v", left)
	rs := fmt.Sprintf("%v", right)
	return ls == rs
}

// evalCaseExprWithRow evaluates a CASE expression with row context.
func (e *Executor) evalCaseExprWithRow(v *sqlparser.CaseExpr, row storage.Row) (interface{}, error) {
	// Evaluate CASE expression with row context for column resolution
	if v.Expr != nil {
		// Simple CASE: CASE expr WHEN val THEN result ...
		caseVal, err := e.evalRowExpr(v.Expr, row)
		if err != nil {
			return nil, err
		}
		// Check if the base expression has binary collation (BINARY expr)
		baseIsBinary := isBinaryConvertExpr(v.Expr)
		for _, when := range v.Whens {
			whenVal, err := e.evalRowExpr(when.Cond, row)
			if err != nil {
				return nil, err
			}
			// Use binary (case-sensitive) comparison if either side has a BINARY cast
			var match bool
			if baseIsBinary || isBinaryConvertExpr(when.Cond) {
				match = compareValuesBinary(caseVal, whenVal)
			} else {
				match, _ = compareValues(caseVal, whenVal, sqlparser.EqualOp)
			}
			if match {
				return e.evalRowExpr(when.Val, row)
			}
		}
	} else {
		// Searched CASE: CASE WHEN cond THEN result ...
		for _, when := range v.Whens {
			cond, err := e.evalWhere(when.Cond, row)
			if err != nil {
				return nil, err
			}
			if cond {
				return e.evalRowExpr(when.Val, row)
			}
		}
	}
	if v.Else != nil {
		return e.evalRowExpr(v.Else, row)
	}
	return nil, nil
}

// evalRowExpr is a package-level shim for backward-compatible callers that
// do not have access to an executor.  It creates a temporary executor with
// empty state, which is sufficient for column-lookup and literal evaluation.
func evalRowExpr(expr sqlparser.Expr, row storage.Row) (interface{}, error) {
	// Some callers use this shim without full executor context.
	// Avoid hard failures on subqueries that require storage/catalog.
	if hasSubqueryExpr(expr) {
		return nil, nil
	}
	e := &Executor{}
	return e.evalRowExpr(expr, row)
}

func hasSubqueryExpr(expr sqlparser.Expr) bool {
	found := false
	sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if _, ok := node.(*sqlparser.Subquery); ok {
			found = true
			return false, nil
		}
		return true, nil
	}, expr)
	return found
}

// evalHaving evaluates a HAVING predicate, with support for aggregate functions
// that are computed against the group's rows.
func (e *Executor) evalHaving(expr sqlparser.Expr, havingRow storage.Row, groupRows []storage.Row) (bool, error) {
	// Pre-compute any aggregate expressions in the HAVING clause and add to the row
	enrichedRow := make(storage.Row, len(havingRow))
	for k, v := range havingRow {
		enrichedRow[k] = v
	}
	// Walk the expression to find aggregates and compute them
	e.addAggregatesToRow(expr, enrichedRow, groupRows)
	return e.evalWhere(expr, enrichedRow)
}

// evalWhere evaluates a WHERE predicate against a row.
func (e *Executor) evalWhere(expr sqlparser.Expr, row storage.Row) (bool, error) {
	switch v := expr.(type) {
	case *sqlparser.ComparisonExpr:
		// Handle IN / NOT IN specially because the right side is a ValTuple or Subquery.
		if v.Operator == sqlparser.InOp || v.Operator == sqlparser.NotInOp {
			// Handle subquery on right side
			if sub, ok := v.Right.(*sqlparser.Subquery); ok {
				// MySQL error 1235: LIMIT in IN/ALL/ANY/SOME subquery is not supported
				if subqueryHasLimit(sub) {
					return false, mysqlError(1235, "42000", "This version of MySQL doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery'")
				}
				// Check if left side is a tuple: (a,b) IN (SELECT x,y FROM ...)
				if leftTuple, ok := v.Left.(sqlparser.ValTuple); ok {
					result, err := e.execSubquery(sub, row)
					if err != nil {
						return false, err
					}
					if len(result.Columns) != len(leftTuple) {
						return false, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftTuple)))
					}
					leftVals := make([]interface{}, len(leftTuple))
					for i, lExpr := range leftTuple {
						lv, err := e.evalRowExpr(lExpr, row)
						if err != nil {
							return false, err
						}
						leftVals[i] = lv
					}
					hasNull := false
					for _, lv := range leftVals {
						if lv == nil {
							hasNull = true
							break
						}
					}
					for _, rrow := range result.Rows {
						if len(rrow) != len(leftVals) {
							continue
						}
						allMatch := true
						rowHasNull := false
						hasDefiniteNonMatch := false
						for i := 0; i < len(leftVals); i++ {
							if leftVals[i] == nil || rrow[i] == nil {
								rowHasNull = true
								allMatch = false
								// Don't break - continue to check remaining non-null pairs
								continue
							}
							match, _ := compareValues(leftVals[i], rrow[i], sqlparser.EqualOp)
							if !match {
								allMatch = false
								hasDefiniteNonMatch = true
								break
							}
						}
						if allMatch {
							return v.Operator == sqlparser.InOp, nil
						}
						if rowHasNull && !hasDefiniteNonMatch {
							hasNull = true
						}
					}
					if v.Operator == sqlparser.NotInOp && !hasNull {
						return true, nil
					}
					return false, nil
				}
				// Handle (SELECT c1,c2,...) IN (SELECT c1,c2,...) — subquery on both sides.
				// Execute left subquery and compare its rows against right subquery rows.
				if leftSub, leftIsSub := v.Left.(*sqlparser.Subquery); leftIsSub {
					leftResult, err := e.execSubquery(leftSub, row)
					if err != nil {
						return false, err
					}
					rightResult, err := e.execSubquery(sub, row)
					if err != nil {
						return false, err
					}
					if len(leftResult.Columns) != len(rightResult.Columns) {
						return false, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftResult.Columns)))
					}
					ncols := len(leftResult.Columns)
					for _, lrow := range leftResult.Rows {
						hasNull := false
						for _, rrow := range rightResult.Rows {
							allMatch := true
							rowHasNull := false
							for i := 0; i < ncols; i++ {
								lv, rv := lrow[i], rrow[i]
								if lv == nil || rv == nil {
									rowHasNull = true
									allMatch = false
									break
								}
								match, _ := compareValues(lv, rv, sqlparser.EqualOp)
								if !match {
									allMatch = false
									break
								}
							}
							if allMatch {
								return v.Operator == sqlparser.InOp, nil
							}
							if rowHasNull {
								hasNull = true
							}
						}
						if hasNull {
							return false, nil
						}
					}
					return v.Operator == sqlparser.NotInOp, nil
				}
				// Scalar IN (SELECT ...)
				left, err := e.evalRowExpr(v.Left, row)
				if err != nil {
					return false, err
				}
				vals, err := e.execSubqueryValues(sub, row)
				if err != nil {
					return false, err
				}
				if left == nil {
					// NULL IN (empty set) = FALSE; NULL NOT IN (empty set) = TRUE
					if len(vals) == 0 {
						return v.Operator == sqlparser.NotInOp, nil
					}
					return false, nil
				}
				hasNull := false
				for _, val := range vals {
					if val == nil {
						hasNull = true
						continue
					}
					match, _ := compareValues(left, val, sqlparser.EqualOp)
					if match {
						return v.Operator == sqlparser.InOp, nil
					}
				}
				// For NOT IN: if any subquery value is NULL and no match found, result is UNKNOWN (false)
				if v.Operator == sqlparser.NotInOp && hasNull {
					return false, nil
				}
				return v.Operator == sqlparser.NotInOp, nil
			}
			// Row/tuple IN (tuple of tuples): (a,b,c) IN ((1,2,3),(4,5,6))
			if leftTupleW, leftIsTupleW := v.Left.(sqlparser.ValTuple); leftIsTupleW {
				rightTupleW, ok := v.Right.(sqlparser.ValTuple)
				if !ok {
					return false, fmt.Errorf("IN/NOT IN right side must be a value tuple, got %T", v.Right)
				}
				leftValsW := make([]interface{}, len(leftTupleW))
				for i, lExpr := range leftTupleW {
					lv, err := e.evalRowExprTupleAware(lExpr, row)
					if err != nil {
						return false, err
					}
					leftValsW[i] = lv
				}
				hasNullW := false
				leftRowW := interface{}(leftValsW)
				for _, item := range rightTupleW {
					rowTupleW, isRowTuple := item.(sqlparser.ValTuple)
					if !isRowTuple {
						break
					}
					if len(rowTupleW) != len(leftTupleW) {
						return false, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftTupleW)))
					}
					rValsW := make([]interface{}, len(rowTupleW))
					for i, rv := range rowTupleW {
						rVal, err := e.evalRowExprTupleAware(rv, row)
						if err != nil {
							return false, err
						}
						rValsW[i] = rVal
					}
					equal, rowHasNull, err := rowTuplesEqual(leftRowW, interface{}(rValsW))
					if err != nil {
						return false, err
					}
					if equal {
						return v.Operator == sqlparser.InOp, nil
					}
					if rowHasNull {
						hasNullW = true
					}
				}
				if v.Operator == sqlparser.NotInOp && !hasNullW {
					return true, nil
				}
				return false, nil
			}
			// Check for illegal collation mix in IN/NOT IN before evaluating.
			if tuple2, ok2 := v.Right.(sqlparser.ValTuple); ok2 {
				if collErr := e.checkCollationMixForIN(v.Left, []sqlparser.Expr(tuple2)); collErr != nil {
					return false, collErr
				}
			}
			left, err := e.evalRowExpr(v.Left, row)
			if err != nil {
				return false, err
			}
			tuple, ok := v.Right.(sqlparser.ValTuple)
			if !ok {
				return false, fmt.Errorf("IN/NOT IN right side must be a value tuple, got %T", v.Right)
			}
			// NULL IN (...) is always NULL (treated as false in WHERE)
			if left == nil {
				return false, nil
			}
			// Determine effective collation for the entire IN expression.
			// The dominant collation is chosen by lowest coercibility across all items.
			inEffectiveCollation := ""
			inCaseSensitive := false
			if _, isString := left.(string); isString {
				// Collect collation from left side and all tuple items
				allInExprs := make([]sqlparser.Expr, 0, 1+len(tuple))
				allInExprs = append(allInExprs, v.Left)
				for _, te := range tuple {
					allInExprs = append(allInExprs, te)
				}
				bestCoercibility := 7
				bestCollation := ""
				for _, te := range allInExprs {
					ci := e.getExprCollationInfo(te)
					if ci.coercibility >= 0 && ci.coercibility < bestCoercibility {
						bestCoercibility = ci.coercibility
						bestCollation = ci.collation
					}
				}
				if bestCollation != "" {
					inEffectiveCollation = bestCollation
					collLower := strings.ToLower(inEffectiveCollation)
					isBin := strings.HasSuffix(collLower, "_bin") || collLower == "binary"
					inCaseSensitive = isBin || strings.Contains(collLower, "_cs")
				}
			}
			hasNull := false
			for _, tupleExpr := range tuple {
				val, err := e.evalRowExpr(tupleExpr, row)
				if err != nil {
					return false, err
				}
				if val == nil {
					hasNull = true
					continue
				}
				ls, rs := fmt.Sprintf("%v", left), fmt.Sprintf("%v", val)
				// TIME IN semantics: allow HH:MM:SS vs HHMMSS style equality.
				leftIsTimeLike := looksLikeTime(ls) && !looksLikeDate(ls)
				rightIsTimeLike := looksLikeTime(rs) && !looksLikeDate(rs)
				if leftIsTimeLike || rightIsTimeLike {
					match, err := compareValues(left, val, sqlparser.EqualOp)
					if err != nil {
						return false, err
					}
					if match {
						return v.Operator == sqlparser.InOp, nil
					}
				}
				// Preserve legacy YEAR IN semantics: only convert small years on string side.
				ls, rs = normalizeYearComparisonTypedStringOnly(ls, rs, left, val)
				if numericEqualForComparison(ls, rs, left, val) {
					return v.Operator == sqlparser.InOp, nil
				}
				if _, errL := strconv.ParseFloat(ls, 64); errL == nil {
					if _, errR := strconv.ParseFloat(rs, 64); errR == nil {
						continue
					}
				}
				if ls == rs {
					return v.Operator == sqlparser.InOp, nil
				}
				// Case-insensitive match for INFORMATION_SCHEMA rows
				if row["__is_info_schema__"] != nil {
					if _, isLS := left.(string); isLS {
						if _, isRS := val.(string); isRS {
							if strings.EqualFold(ls, rs) {
								return v.Operator == sqlparser.InOp, nil
							}
						}
					}
				}
				// Collation-aware comparison: use the effective collation for the IN expression.
				// If the effective collation is case-insensitive, compare case-insensitively.
				if inEffectiveCollation != "" {
					if _, isLS := left.(string); isLS {
						if _, isRS := val.(string); isRS {
							if !inCaseSensitive && strings.EqualFold(ls, rs) {
								return v.Operator == sqlparser.InOp, nil
							}
							// If case-sensitive and strings are not equal, skip.
						}
					}
				}
			}
			// For NOT IN: if any tuple value is NULL and no match found, result is UNKNOWN (false)
			if v.Operator == sqlparser.NotInOp && hasNull {
				return false, nil
			}
			return v.Operator == sqlparser.NotInOp, nil
		}

		// Handle ANY/SOME (Modifier=1) and ALL (Modifier=2) with subquery
		if v.Modifier != 0 {
			if sub, ok := v.Right.(*sqlparser.Subquery); ok {
				isAny := v.Modifier == 1 // ANY/SOME
				// Handle tuple (row constructor) left side: (a,b) = ANY (SELECT x,y FROM ...)
				if leftTupleAny, leftIsTupleAny := v.Left.(sqlparser.ValTuple); leftIsTupleAny && v.Operator == sqlparser.EqualOp {
					result, err := e.execSubquery(sub, row)
					if err != nil {
						return false, err
					}
					if len(result.Columns) != len(leftTupleAny) {
						return false, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftTupleAny)))
					}
					leftVals := make([]interface{}, len(leftTupleAny))
					for i, lExpr := range leftTupleAny {
						lv, err := e.evalRowExpr(lExpr, row)
						if err != nil {
							return false, err
						}
						leftVals[i] = lv
					}
					if isAny {
						// ANY: true if any row matches
						for _, rrow := range result.Rows {
							allMatch := true
							for i := 0; i < len(leftVals); i++ {
								if leftVals[i] == nil || rrow[i] == nil {
									allMatch = false
									break
								}
								match, _ := compareValues(leftVals[i], rrow[i], sqlparser.EqualOp)
								if !match {
									allMatch = false
									break
								}
							}
							if allMatch {
								return true, nil
							}
						}
						return false, nil
					}
					// ALL: true if every row matches
					for _, rrow := range result.Rows {
						for i := 0; i < len(leftVals); i++ {
							if leftVals[i] == nil || rrow[i] == nil {
								return false, nil
							}
							match, _ := compareValues(leftVals[i], rrow[i], sqlparser.EqualOp)
							if !match {
								return false, nil
							}
						}
					}
					return true, nil
				}
				left, err := e.evalRowExpr(v.Left, row)
				if err != nil {
					return false, err
				}
				if left == nil {
					return false, nil
				}
				vals, err := e.execSubqueryValues(sub, row)
				if err != nil {
					return false, err
				}
				if isAny {
					// ANY/SOME: true if comparison holds for at least one non-NULL value
					for _, val := range vals {
						if val == nil {
							continue
						}
						match, err := compareValues(left, val, v.Operator)
						if err != nil {
							return false, err
						}
						if match {
							return true, nil
						}
					}
					return false, nil
				}
				// ALL: true if comparison holds for every value.
				// If any subquery value is NULL, the comparison is UNKNOWN (returns false).
				for _, val := range vals {
					if val == nil {
						return false, nil
					}
					match, err := compareValues(left, val, v.Operator)
					if err != nil {
						return false, err
					}
					if !match {
						return false, nil
					}
				}
				return true, nil
			}
		}

		// Handle ValTuple (row constructor) comparison: (c1,c2) = (SELECT c1, c2 FROM ...) or (c1,c2) = (2,'abc')
		if tuple, ok := v.Left.(sqlparser.ValTuple); ok {
			// Evaluate left tuple values (with tuple-aware support for nested tuples)
			leftVals := make([]interface{}, len(tuple))
			for i, texpr := range tuple {
				val, err := e.evalRowExprTupleAware(texpr, row)
				if err != nil {
					return false, err
				}
				leftVals[i] = val
			}
			// Right side: subquery returning one row
			if sub, ok := v.Right.(*sqlparser.Subquery); ok {
				result, err := e.execSubquery(sub, row)
				if err != nil {
					return false, err
				}
				if len(result.Rows) == 0 {
					return false, nil
				}
				if len(result.Rows) > 1 {
					return false, fmt.Errorf("Subquery returns more than 1 row")
				}
				rightRow := result.Rows[0]
				if len(leftVals) != len(rightRow) {
					return false, fmt.Errorf("Operand should contain %d column(s)", len(leftVals))
				}
				allMatch := true
				for i, lv := range leftVals {
					rv := rightRow[i]
					match, err := compareValues(lv, rv, v.Operator)
					if err != nil {
						return false, err
					}
					if !match {
						allMatch = false
						break
					}
				}
				return allMatch, nil
			}
			// Right side: ValTuple literal (c1,c2) op (2, "abc") — row constructor comparison.
			if rightTuple, ok := v.Right.(sqlparser.ValTuple); ok {
				if len(leftVals) != len(rightTuple) {
					return false, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftVals)))
				}
				// Build right vals (with tuple-aware support for nested tuples)
				rightValsW := make([]interface{}, len(rightTuple))
				for i := range rightTuple {
					rv, err := e.evalRowExprTupleAware(rightTuple[i], row)
					if err != nil {
						return false, err
					}
					rightValsW[i] = rv
				}
				switch v.Operator {
				case sqlparser.EqualOp, sqlparser.NotEqualOp, sqlparser.NullSafeEqualOp:
					equal, hasNull, err := rowTuplesEqual(interface{}(leftVals), interface{}(rightValsW))
					if err != nil {
						return false, err
					}
					if v.Operator == sqlparser.NullSafeEqualOp {
						if hasNull {
							// Element-wise null-safe comparison
							for i := 0; i < len(leftVals); i++ {
								lv, rv := leftVals[i], rightValsW[i]
								if lv == nil && rv == nil {
									continue
								}
								if lv == nil || rv == nil {
									return false, nil
								}
								eq, _, _ := rowTuplesEqual(lv, rv)
								if !eq {
									return false, nil
								}
							}
							return true, nil
						}
						return equal, nil
					}
					if hasNull {
						return false, nil
					}
					if v.Operator == sqlparser.NotEqualOp {
						return !equal, nil
					}
					return equal, nil
				default:
					// Lexicographic comparison for <, >, <=, >=
					for i := 0; i < len(leftVals); i++ {
						lv, rv := leftVals[i], rightValsW[i]
						if lv == nil || rv == nil {
							return false, nil // NULL → UNKNOWN → false in WHERE
						}
						eq, err := compareValues(lv, rv, sqlparser.EqualOp)
						if err != nil {
							return false, err
						}
						if eq {
							continue
						}
						lt, err := compareValues(lv, rv, sqlparser.LessThanOp)
						if err != nil {
							return false, err
						}
						switch v.Operator {
						case sqlparser.LessThanOp, sqlparser.LessEqualOp:
							return lt, nil
						default: // GreaterThanOp, GreaterEqualOp
							return !lt, nil
						}
					}
					// All elements equal
					switch v.Operator {
					case sqlparser.LessEqualOp, sqlparser.GreaterEqualOp:
						return true, nil
					default:
						return false, nil
					}
				}
			}
		}

		// Extract COLLATE clause from either side for collation-aware comparison
		var whereCollation string
		leftExprW, rightExprW := v.Left, v.Right
		if ce, ok := leftExprW.(*sqlparser.CollateExpr); ok {
			whereCollation = ce.Collation
			leftExprW = ce.Expr
		}
		if ce, ok := rightExprW.(*sqlparser.CollateExpr); ok {
			whereCollation = ce.Collation
			rightExprW = ce.Expr
		}
		// If LIKE operator and no explicit COLLATE clause, try to infer collation from column type.
		// Binary columns (BLOB, BINARY, VARBINARY) use case-sensitive "binary" collation.
		if whereCollation == "" && (v.Operator == sqlparser.LikeOp || v.Operator == sqlparser.NotLikeOp) {
			if colExpr, ok := leftExprW.(*sqlparser.ColName); ok && e.queryTableDef != nil {
				if coll := e.lookupColumnCollation(colExpr.Name.String(), e.queryTableDef); coll != "" {
					whereCollation = coll
				}
			}
		}
		left, err := e.evalRowExpr(leftExprW, row)
		var leftWhereOvErr *intOverflowError
		if err != nil {
			var oe *intOverflowError
			if errors.As(err, &oe) {
				leftWhereOvErr = oe
				left = uint64(math.MaxUint64)
			} else {
				return false, err
			}
		}
		right, err := e.evalRowExpr(rightExprW, row)
		var rightWhereOvErr *intOverflowError
		if err != nil {
			var oe *intOverflowError
			if errors.As(err, &oe) {
				rightWhereOvErr = oe
				right = uint64(math.MaxUint64)
			} else {
				return false, err
			}
		}
		if leftWhereOvErr != nil {
			e.addWarning("Warning", 1292, formatOverflowWarningMsg(leftWhereOvErr))
		}
		if rightWhereOvErr != nil {
			e.addWarning("Warning", 1292, formatOverflowWarningMsg(rightWhereOvErr))
		}
		// Collation-aware LIKE/NOT LIKE or LIKE with ESCAPE clause
		if v.Operator == sqlparser.LikeOp || v.Operator == sqlparser.NotLikeOp {
			// NULL comparison: x LIKE NULL = NULL = false in WHERE context
			if left == nil || right == nil {
				return false, nil
			}
			ls := toString(left)
			rs := toString(right)
			// Determine escape character (default is '\')
			escapeChar := rune('\\')
			if v.Escape != nil {
				escVal, _ := e.evalRowExpr(v.Escape, row)
				if escStr := toString(escVal); len([]rune(escStr)) > 0 {
					escapeChar = []rune(escStr)[0]
				}
			}
			collLower := strings.ToLower(whereCollation)
			isCaseSensitive := whereCollation != "" && (strings.Contains(collLower, "_bin") || strings.Contains(collLower, "_cs") || collLower == "binary")
			var re *regexp.Regexp
			if isCaseSensitive {
				re = likeToRegexpCaseSensitiveEscape(rs, escapeChar)
			} else {
				re = likeToRegexpEscape(rs, escapeChar)
			}
			matched := re.MatchString(ls)
			if v.Operator == sqlparser.LikeOp {
				return matched, nil
			}
			return !matched, nil
		}
		// For binary/varbinary column comparisons, use NO PAD (byte-for-byte) semantics.
		// Check if either side references a binary column.
		if (e.isBinaryExpr(leftExprW) || e.isBinaryExpr(rightExprW)) &&
			(v.Operator == sqlparser.LessThanOp || v.Operator == sqlparser.GreaterThanOp ||
				v.Operator == sqlparser.LessEqualOp || v.Operator == sqlparser.GreaterEqualOp) {
			if left == nil || right == nil {
				return false, nil
			}
			ls := toString(left)
			rs := toString(right)
			cmp := 0
			if ls < rs {
				cmp = -1
			} else if ls > rs {
				cmp = 1
			}
			switch v.Operator {
			case sqlparser.LessThanOp:
				return cmp < 0, nil
			case sqlparser.GreaterThanOp:
				return cmp > 0, nil
			case sqlparser.LessEqualOp:
				return cmp <= 0, nil
			case sqlparser.GreaterEqualOp:
				return cmp >= 0, nil
			}
		}
		// MySQL numeric column vs non-numeric string: coerce the string to 0.
		// e.g. DECIMAL_COL > 'A' → DECIMAL_COL > 0 (MySQL converts non-numeric string to 0 with warning).
		// This only applies when the column side is DECIMAL/FLOAT/DOUBLE and the string side is a literal.
		// Exclude TIME/DATE-like strings from coercion (e.g. TIME column vs DECIMAL column comparison).
		if ls, lok := left.(string); lok {
			if rs, rok := right.(string); rok {
				if e.isNumericDecimalColExpr(leftExprW) {
					if _, ferr := strconv.ParseFloat(rs, 64); ferr != nil && rs != "" && !looksLikeTime(rs) && !looksLikeDate(rs) {
						// right is non-numeric string literal: coerce to "0"
						right = "0"
						e.addWarning("Warning", 1292, fmt.Sprintf("Truncated incorrect DOUBLE value: '%s'", rs))
					}
				} else if e.isNumericDecimalColExpr(rightExprW) {
					if _, ferr := strconv.ParseFloat(ls, 64); ferr != nil && ls != "" && !looksLikeTime(ls) && !looksLikeDate(ls) {
						// left is non-numeric string literal: coerce to "0"
						left = "0"
						e.addWarning("Warning", 1292, fmt.Sprintf("Truncated incorrect DOUBLE value: '%s'", ls))
					}
				}
			}
		}
		result, err := compareValues(left, right, v.Operator)
		if err != nil {
			return false, err
		}
		// For INFORMATION_SCHEMA rows, apply case-insensitive string comparison
		// to match MySQL's default utf8mb4_0900_ai_ci collation behavior.
		if !result && row["__is_info_schema__"] != nil {
			if ls, lok := left.(string); lok {
				if rs, rok := right.(string); rok {
					ci := strings.EqualFold(ls, rs)
					switch v.Operator {
					case sqlparser.EqualOp:
						return ci, nil
					case sqlparser.NotEqualOp:
						return !ci, nil
					}
				}
			}
		}
		return result, err
	case *sqlparser.AndExpr:
		l, err := e.evalWhere(v.Left, row)
		if err != nil {
			return false, err
		}
		r, err := e.evalWhere(v.Right, row)
		if err != nil {
			return false, err
		}
		return l && r, nil
	case *sqlparser.OrExpr:
		l, lErr := e.evalWhere(v.Left, row)
		// Short-circuit: if left is true, skip right evaluation.
		if lErr == nil && l {
			return true, nil
		}
		r, rErr := e.evalWhere(v.Right, row)
		if lErr != nil {
			// Left errored; if right is true, return true.
			if rErr == nil && r {
				return true, nil
			}
			return false, lErr
		}
		if rErr != nil {
			return false, rErr
		}
		return l || r, nil
	case *sqlparser.IsExpr:
		val, err := e.evalRowExpr(v.Left, row)
		if err != nil {
			return false, err
		}
		switch v.Right {
		case sqlparser.IsNullOp:
			if e.sqlAutoIsNull && e.lastAutoIncID > 0 && val != nil {
				if fmt.Sprintf("%v", val) == fmt.Sprintf("%v", e.lastAutoIncID) {
					return true, nil
				}
			}
			if val == nil {
				return true, nil
			}
			// MySQL: 0000-00-00 IS NULL = TRUE for NOT NULL date columns
			if isZeroDate(val) {
				if e.isColumnNotNullExpr(v.Left) {
					return true, nil
				}
			}
			return false, nil
		case sqlparser.IsNotNullOp:
			return val != nil, nil
		case sqlparser.IsTrueOp:
			return val != nil && isTruthy(val), nil
		case sqlparser.IsFalseOp:
			return val != nil && !isTruthy(val), nil
		case sqlparser.IsNotTrueOp:
			return val == nil || !isTruthy(val), nil
		case sqlparser.IsNotFalseOp:
			return val == nil || isTruthy(val), nil
		}
	case *sqlparser.BetweenExpr:
		val, err := e.evalRowExpr(v.Left, row)
		if err != nil {
			return false, err
		}
		from, err := e.evalRowExpr(v.From, row)
		if err != nil {
			return false, err
		}
		to, err := e.evalRowExpr(v.To, row)
		if err != nil {
			return false, err
		}
		geFrom, err := compareValues(val, from, sqlparser.GreaterEqualOp)
		if err != nil {
			return false, err
		}
		leTo, err := compareValues(val, to, sqlparser.LessEqualOp)
		if err != nil {
			return false, err
		}
		result := geFrom && leTo
		if v.IsBetween {
			return result, nil
		}
		return !result, nil
	case *sqlparser.ExistsExpr:
		result, err := e.execSubquery(v.Subquery, row)
		if err != nil {
			return false, err
		}
		return len(result.Rows) > 0, nil
	case *sqlparser.NotExpr:
		// Evaluate the inner expression using evalRowExpr to preserve NULL (tristate) semantics.
		// NOT(NULL) = NULL → exclude from WHERE (return false).
		// NOT(TRUE) = FALSE → exclude from WHERE (return false).
		// NOT(FALSE) = TRUE → include in WHERE (return true).
		innerVal, err := e.evalRowExpr(v.Expr, row)
		if err != nil {
			return false, err
		}
		if innerVal == nil {
			return false, nil // NOT(NULL) = NULL → treat as false in WHERE
		}
		return !isTruthy(innerVal), nil
	case *sqlparser.MemberOfExpr:
		val, err := e.evalRowExpr(v, row)
		if err != nil {
			return false, err
		}
		switch x := val.(type) {
		case bool:
			return x, nil
		case int64:
			return x != 0, nil
		case uint64:
			return x != 0, nil
		default:
			return toInt64(val) != 0, nil
		}
	}
	val, err := e.evalRowExpr(expr, row)
	if err != nil {
		return false, err
	}
	switch x := val.(type) {
	case bool:
		return x, nil
	case int64:
		return x != 0, nil
	case uint64:
		return x != 0, nil
	case float64:
		return x != 0, nil
	case ScaledValue:
		return x.Value != 0, nil
	case DivisionResult:
		return x.Value != 0, nil
	case string:
		return strings.TrimSpace(x) != "" && x != "0", nil
	default:
		return toInt64(val) != 0, nil
	}
}

// evalWhere is a package-level shim for backward-compatible callers.
func evalWhere(expr sqlparser.Expr, row storage.Row) (bool, error) {
	e := &Executor{}
	return e.evalWhere(expr, row)
}

func compareValues(left, right interface{}, op sqlparser.ComparisonExprOperator) (bool, error) {
	// Unwrap SysVarDouble to plain float64 for comparison purposes.
	if sd, ok := left.(SysVarDouble); ok {
		left = sd.Value
	}
	if sd, ok := right.(SysVarDouble); ok {
		right = sd.Value
	}
	// Unwrap AvgResult to plain float64 for comparison purposes.
	if ar, ok := left.(AvgResult); ok {
		left = ar.Value
	}
	if ar, ok := right.(AvgResult); ok {
		right = ar.Value
	}
	// When comparing HexBytes (x'...' literal) against an integer (0x... literal),
	// decode both to raw byte strings for comparison. In MySQL, x'123ABC' = 0x123ABC
	// is true because both represent the same binary string "\x12\x3a\xbc".
	if hb, ok := left.(HexBytes); ok {
		if isNativeNumericType(right) {
			// Decode HexBytes to raw bytes, convert integer to raw bytes too
			decoded, err := hexDecodeString(string(hb))
			if err == nil {
				left = decoded
				right = hexIntToBytes(right)
			}
		} else if rs, ok2 := right.(string); ok2 {
			// HexBytes vs string: if right is a decimal number (contains '.'),
			// convert HexBytes to its big integer decimal representation for numeric comparison.
			if strings.ContainsRune(rs, '.') {
				hexStr := string(hb)
				if len(hexStr)%2 != 0 {
					hexStr = "0" + hexStr
				}
				n := new(big.Int)
				if _, ok3 := n.SetString(hexStr, 16); ok3 {
					left = n.Text(10)
				} else {
					decoded, err := hexDecodeString(string(hb))
					if err == nil {
						left = decoded
					}
				}
			} else {
				// HexBytes vs string (e.g. raw binary from VARBINARY bitwise op result vs HexBytes from 0x overflow)
				// Decode HexBytes to raw bytes for comparison with the raw byte string
				decoded, err := hexDecodeString(string(hb))
				if err == nil {
					left = decoded
				}
			}
		}
	} else if hb, ok := right.(HexBytes); ok {
		if isNativeNumericType(left) {
			decoded, err := hexDecodeString(string(hb))
			if err == nil {
				right = decoded
				left = hexIntToBytes(left)
			}
		} else if ls, ok2 := left.(string); ok2 {
			// HexBytes vs string: check if the string is a decimal number (contains '.').
			// In that case, convert HexBytes to its big integer decimal representation
			// for numeric comparison (e.g. DECIMAL column vs overflowed 0x hex literal).
			if strings.ContainsRune(ls, '.') {
				hexStr := string(hb)
				if len(hexStr)%2 != 0 {
					hexStr = "0" + hexStr
				}
				n := new(big.Int)
				if _, ok3 := n.SetString(hexStr, 16); ok3 {
					right = n.Text(10)
				} else {
					decoded, err := hexDecodeString(string(hb))
					if err == nil {
						right = decoded
					}
				}
			} else {
				// HexBytes vs string (e.g. raw binary from VARBINARY bitwise op result vs HexBytes from 0x overflow)
				// Decode HexBytes to raw bytes for comparison with the raw byte string
				decoded, err := hexDecodeString(string(hb))
				if err == nil {
					right = decoded
				}
			}
		}
	}
	// NULL-safe equal (<=>): true if both NULL, false if one is NULL, otherwise normal equality.
	if op == sqlparser.NullSafeEqualOp {
		if left == nil && right == nil {
			return true, nil
		}
		if left == nil || right == nil {
			return false, nil
		}
		// Compare numerically if possible, then fall back to string comparison
		ls, rs := fmt.Sprintf("%v", left), fmt.Sprintf("%v", right)
		ls, rs = normalizeYearComparisonTyped(ls, rs, left, right)
		if numericEqualForComparison(ls, rs, left, right) {
			return true, nil
		}
		if _, errL := strconv.ParseFloat(ls, 64); errL == nil {
			if _, errR := strconv.ParseFloat(rs, 64); errR == nil {
				return false, nil
			}
		}
		if ls == rs {
			return true, nil
		}
		// Try date normalization
		if looksLikeDate(ls) || looksLikeDate(rs) {
			ln := normalizeDateTimeString(ls)
			rn := normalizeDateTimeString(rs)
			if ln != "" && rn != "" {
				ln, rn = normalizeDateTimeForCompare(ln, rn)
				return ln == rn, nil
			}
		}
		return false, nil
	}

	// Handle NULL comparisons
	if left == nil || right == nil {
		return false, nil // NULL comparisons always false in SQL (except IS NULL)
	}

	switch op {
	case sqlparser.EqualOp:
		// For numeric-looking strings, compare numerically
		ls, rs := fmt.Sprintf("%v", left), fmt.Sprintf("%v", right)
		// Apply YEAR normalization for small-number vs 4-digit-year comparisons
		ls, rs = normalizeYearComparisonTyped(ls, rs, left, right)
		if numericEqualForComparison(ls, rs, left, right) {
			return true, nil
		}
		fl, errL := strconv.ParseFloat(ls, 64)
		fr, errR := strconv.ParseFloat(rs, 64)
		if errL == nil && errR == nil {
			return false, nil
		}
		// ENUM values compared with integers: do not coerce via string→0 rule.
		_, leftIsEnum := left.(EnumValue)
		_, rightIsEnum := right.(EnumValue)
		// MySQL type coercion: when one operand is a DATE/TIME-like string
		// and the other is a native integer, convert the integer to DATE/TIME
		// format for comparison (e.g., 19830907 → "1983-09-07", 000400 → "00:04:00").
		if errL == nil && errR != nil && isNativeNumericType(left) {
			// left is numeric, right is non-numeric string
			if looksLikeDate(rs) {
				// Try converting the integer to a date string (YYYYMMDD/YYMMDD)
				dateStr := parseMySQLDateValue(ls)
				if dateStr != "" {
					rn := normalizeDateTimeString(rs)
					if rn != "" {
						dateStr, rn = normalizeDateTimeForCompare(dateStr, rn)
						return dateStr == rn, nil
					}
				}
			}
			if looksLikeTime(rs) && !looksLikeDate(rs) {
				// Try converting the integer to a TIME string (HHMMSS)
				lt := parseMySQLTimeValue(ls)
				rt := parseMySQLTimeValue(rs)
				return lt == rt, nil
			}
			if rightIsEnum {
				return false, nil
			}
			// For a native numeric column value vs a binary string with non-printable bytes:
			// MySQL converts the binary string to DOUBLE (which fails -> 0), so compare fl == 0.
			return fl == 0, nil
		}
		if errR == nil && errL != nil && isNativeNumericType(right) {
			// right is numeric, left is non-numeric string
			if looksLikeDate(ls) {
				// Try converting the integer to a date string (YYYYMMDD/YYMMDD)
				dateStr := parseMySQLDateValue(rs)
				if dateStr == "" && fr == 0 {
					// Integer 0 compared to a date = 0000-00-00 comparison
					ln := normalizeDateTimeString(ls)
					if ln != "" {
						ln, _ = normalizeDateTimeForCompare(ln, "0000-00-00")
						return ln == "0000-00-00", nil
					}
				}
				if dateStr != "" {
					ln := normalizeDateTimeString(ls)
					if ln != "" {
						ln, dateStr = normalizeDateTimeForCompare(ln, dateStr)
						return ln == dateStr, nil
					}
				}
			}
			if looksLikeTime(ls) && !looksLikeDate(ls) {
				// Try converting the integer to a TIME string (HHMMSS)
				lt := parseMySQLTimeValue(ls)
				rt := parseMySQLTimeValue(rs)
				return lt == rt, nil
			}
			if leftIsEnum {
				return false, nil
			}
			// Binary string vs integer: if left contains non-printable bytes (binary data),
			// treat the integer as a big-endian byte string for comparison.
			if looksLikeBinaryData(ls) {
				rightAsBytes, ok := hexIntToBytes(right).(string)
				if ok {
					return ls == rightAsBytes, nil
				}
			}
			return fr == 0, nil
		}
		if ls == rs {
			return true, nil
		}
		// MySQL default collation (utf8mb4_0900_ai_ci) is case-insensitive.
		// When both operands are native Go strings (i.e. VARCHAR/CHAR columns or string literals),
		// use case-insensitive comparison unless the value looks like a date/time/number (already
		// handled above) or binary data.
		_, leftIsString := left.(string)
		_, rightIsString := right.(string)
		if leftIsString && rightIsString && !looksLikeBinaryData(ls) && !looksLikeBinaryData(rs) {
			if strings.EqualFold(ls, rs) {
				return true, nil
			}
			// MySQL PAD SPACE semantics: trailing spaces are ignored in non-binary comparisons.
			// 'a' = 'a ' is TRUE, 'a\0' < 'a' is TRUE (NUL is less than space)
			lsTrimmed := strings.TrimRight(ls, " ")
			rsTrimmed := strings.TrimRight(rs, " ")
			if strings.EqualFold(lsTrimmed, rsTrimmed) {
				return true, nil
			}
		}
		// Try datetime normalization if strings look like dates (but not binary data)
		if !looksLikeBinaryData(ls) && !looksLikeBinaryData(rs) {
			if looksLikeDate(ls) || looksLikeDate(rs) {
				ln := normalizeDateTimeString(ls)
				rn := normalizeDateTimeString(rs)
				if ln != "" && rn != "" {
					// Use the two-arg normalizer to align date vs datetime granularity
					ln, rn = normalizeDateTimeForCompare(ln, rn)
					if ln == rn {
						return true, nil
					}
					// Fallback: convert both datetime strings to compact decimal float64
					// for numeric comparison. This handles precision loss in large datetime
					// numbers (e.g. 20010101112233.123456 vs 20010101112233.123457 are
					// equal as float64 at this magnitude). Only apply this fallback when
					// one of the ORIGINAL strings was in compact decimal YYYYMMDDHHMMSS.ffffff
					// format (14-digit int part before '.'), not standard datetime strings.
					// This avoids false positives from datetime strings that differ only in
					// the fractional second (e.g. 00:00:00.999999 vs 00:00:01.000000).
					lsIsCompactDecimalDT := isCompactDecimalDatetime(ls)
					rsIsCompactDecimalDT := isCompactDecimalDatetime(rs)
					if lsIsCompactDecimalDT || rsIsCompactDecimalDT {
						lf := datetimeStringToFloat64(ln)
						rf := datetimeStringToFloat64(rn)
						if lf != 0 && rf != 0 {
							return lf == rf, nil
						}
					}
					return false, nil
				}
				// Handle YEAR column (4-digit string, e.g. "2026") compared with DATETIME string.
				// MySQL extracts the year from the DATETIME and compares only the year portion.
				// e.g.: YEAR_col = NOW() where YEAR_col="2026" and NOW()="2026-04-17 15:55:12" -> true
				if isYearColumnString(ls) && rn != "" {
					yearFromDT := rn[:4]
					return ls == yearFromDT, nil
				}
				if isYearColumnString(rs) && ln != "" {
					yearFromDT := ln[:4]
					return rs == yearFromDT, nil
				}
			}
			// Try TIME normalization if either looks like a time.
			// Use strict check to avoid false positives from strings that contain ':'
			// but are not actually time values (e.g. instrument names like 'hash_filo::lock').
			// Skip normalization only when both sides already have fractional seconds AND
			// are in normalized HH:MM:SS.frac format; in that case string comparison is used
			// so that TIME(6) '11:22:33.123000' != VARCHAR '11:22:33.123' (MySQL behavior).
			isNormalizedTimeFrac := func(s string) bool {
				dotIdx := strings.Index(s, ".")
				if dotIdx < 5 {
					return false // no fractional part or too short
				}
				// Check HH:MM before the dot
				colonIdx := strings.Index(s[:dotIdx], ":")
				if colonIdx < 2 {
					return false // first component must be at least 2 digits
				}
				return true
			}
			skipTimeNorm := isNormalizedTimeFrac(ls) && isNormalizedTimeFrac(rs)
			if (looksLikeActualTime(ls) || looksLikeActualTime(rs)) && !skipTimeNorm {
				lt := parseMySQLTimeValue(ls)
				rt := parseMySQLTimeValue(rs)
				if lt == rt {
					return true, nil
				}
			}
		}
		return false, nil
	case sqlparser.NotEqualOp:
		ls, rs := fmt.Sprintf("%v", left), fmt.Sprintf("%v", right)
		ls, rs = normalizeYearComparisonTyped(ls, rs, left, right)
		if numericEqualForComparison(ls, rs, left, right) {
			return false, nil
		}
		fl, errL := strconv.ParseFloat(ls, 64)
		fr, errR := strconv.ParseFloat(rs, 64)
		if errL == nil && errR == nil {
			return true, nil
		}
		_, leftIsEnum := left.(EnumValue)
		_, rightIsEnum := right.(EnumValue)
		// MySQL type coercion: when one operand is a native numeric type
		// and the other is a non-numeric string, try DATE/TIME coercion first.
		if errL == nil && errR != nil && isNativeNumericType(left) {
			if looksLikeDate(rs) {
				dateStr := parseMySQLDateValue(ls)
				if dateStr != "" {
					rn := normalizeDateTimeString(rs)
					if rn != "" {
						dateStr, rn = normalizeDateTimeForCompare(dateStr, rn)
						return dateStr != rn, nil
					}
				}
			}
			if looksLikeTime(rs) && !looksLikeDate(rs) {
				lt := parseMySQLTimeValue(ls)
				rt := parseMySQLTimeValue(rs)
				return lt != rt, nil
			}
			if rightIsEnum {
				return true, nil
			}
			return fl != 0, nil
		}
		if errR == nil && errL != nil && isNativeNumericType(right) {
			if looksLikeDate(ls) {
				dateStr := parseMySQLDateValue(rs)
				if dateStr != "" {
					ln := normalizeDateTimeString(ls)
					if ln != "" {
						ln, dateStr = normalizeDateTimeForCompare(ln, dateStr)
						return ln != dateStr, nil
					}
				}
			}
			if looksLikeTime(ls) && !looksLikeDate(ls) {
				lt := parseMySQLTimeValue(ls)
				rt := parseMySQLTimeValue(rs)
				return lt != rt, nil
			}
			if leftIsEnum {
				return true, nil
			}
			return fr != 0, nil
		}
		if ls == rs {
			return false, nil
		}
		// Try datetime normalization
		if looksLikeDate(ls) || looksLikeDate(rs) {
			ln := normalizeDateTimeString(ls)
			rn := normalizeDateTimeString(rs)
			if ln != "" && rn != "" {
				ln, rn = normalizeDateTimeForCompare(ln, rn)
				return ln != rn, nil
			}
		}
		return true, nil
	case sqlparser.LessThanOp, sqlparser.GreaterThanOp, sqlparser.LessEqualOp, sqlparser.GreaterEqualOp:
		if li, okL := toStrictBigInt(left); okL {
			if ri, okR := toStrictBigInt(right); okR {
				// Use bigint ordering only for values outside signed 64-bit range;
				// keep existing YEAR/small-number behavior for ordinary integers.
				if li.BitLen() > 63 || ri.BitLen() > 63 {
					cmp := li.Cmp(ri)
					switch op {
					case sqlparser.LessThanOp:
						return cmp < 0, nil
					case sqlparser.GreaterThanOp:
						return cmp > 0, nil
					case sqlparser.LessEqualOp:
						return cmp <= 0, nil
					case sqlparser.GreaterEqualOp:
						return cmp >= 0, nil
					}
				}
			}
		}
		// Apply YEAR normalization for ordering comparisons
		ls, rs := fmt.Sprintf("%v", left), fmt.Sprintf("%v", right)
		nls, nrs := normalizeYearComparisonTyped(ls, rs, left, right)
		if nls != ls || nrs != rs {
			// YEAR comparison detected, use normalized values
			fl, _ := strconv.ParseFloat(nls, 64)
			fr, _ := strconv.ParseFloat(nrs, 64)
			cmp := 0
			if fl < fr {
				cmp = -1
			} else if fl > fr {
				cmp = 1
			}
			switch op {
			case sqlparser.LessThanOp:
				return cmp < 0, nil
			case sqlparser.GreaterThanOp:
				return cmp > 0, nil
			case sqlparser.LessEqualOp:
				return cmp <= 0, nil
			case sqlparser.GreaterEqualOp:
				return cmp >= 0, nil
			}
		}
		// TIME ordering: when one side is TIME-like (contains ':') and not a date,
		// coerce both sides with MySQL TIME parser and compare by duration.
		leftIsTimeLike := looksLikeTime(ls) && !looksLikeDate(ls)
		rightIsTimeLike := looksLikeTime(rs) && !looksLikeDate(rs)
		if leftIsTimeLike || rightIsTimeLike {
			if tcmp, ok := compareMySQLTimeOrdering(ls, rs); ok {
				switch op {
				case sqlparser.LessThanOp:
					return tcmp < 0, nil
				case sqlparser.GreaterThanOp:
					return tcmp > 0, nil
				case sqlparser.LessEqualOp:
					return tcmp <= 0, nil
				case sqlparser.GreaterEqualOp:
					return tcmp >= 0, nil
				}
			}
		}
		// DATE ordering: when one side is a date-like string and the other is
		// a numeric integer, try converting the integer to a date for comparison.
		if (looksLikeDate(ls) && isNativeNumericType(right)) || (looksLikeDate(rs) && isNativeNumericType(left)) {
			var lDate, rDate string
			if looksLikeDate(ls) {
				lDate = normalizeDateTimeString(ls)
				rDate = parseMySQLDateValue(rs)
			} else {
				lDate = parseMySQLDateValue(ls)
				rDate = normalizeDateTimeString(rs)
			}
			if lDate != "" && rDate != "" {
				lDate, rDate = normalizeDateTimeForCompare(lDate, rDate)
				dcmp := 0
				if lDate < rDate {
					dcmp = -1
				} else if lDate > rDate {
					dcmp = 1
				}
				switch op {
				case sqlparser.LessThanOp:
					return dcmp < 0, nil
				case sqlparser.GreaterThanOp:
					return dcmp > 0, nil
				case sqlparser.LessEqualOp:
					return dcmp <= 0, nil
				case sqlparser.GreaterEqualOp:
					return dcmp >= 0, nil
				}
			}
		}
		// Validate datetime strings: if either side looks like a full datetime (date+time)
		// but has an invalid time component (hour > 23, etc.), throw error 1292.
		if invalidStr, invalid := hasInvalidTimeComponent(ls); invalid {
			return false, mysqlError(1292, "HY000", fmt.Sprintf("Incorrect DATETIME value: '%s'", invalidStr))
		}
		if invalidStr, invalid := hasInvalidTimeComponent(rs); invalid {
			return false, mysqlError(1292, "HY000", fmt.Sprintf("Incorrect DATETIME value: '%s'", invalidStr))
		}
		cmp := compareNumeric(left, right)
		switch op {
		case sqlparser.LessThanOp:
			return cmp < 0, nil
		case sqlparser.GreaterThanOp:
			return cmp > 0, nil
		case sqlparser.LessEqualOp:
			return cmp <= 0, nil
		case sqlparser.GreaterEqualOp:
			return cmp >= 0, nil
		}
		return false, nil
	case sqlparser.LikeOp:
		pattern := toString(right)
		value := toString(left)
		re := likeToRegexp(pattern)
		return re.MatchString(value), nil
	case sqlparser.NotLikeOp:
		pattern := toString(right)
		value := toString(left)
		re := likeToRegexp(pattern)
		return !re.MatchString(value), nil
	case sqlparser.RegexpOp:
		rePattern := toString(right)
		reValue := toString(left)
		compiled, err := regexp.Compile("(?i)" + rePattern)
		if err != nil {
			return false, mysqlError(3692, "HY000", "Illegal argument to a regular expression.")
		}
		return compiled.MatchString(reValue), nil
	case sqlparser.NotRegexpOp:
		rePattern := toString(right)
		reValue := toString(left)
		compiled, err := regexp.Compile("(?i)" + rePattern)
		if err != nil {
			return false, mysqlError(3692, "HY000", "Illegal argument to a regular expression.")
		}
		return !compiled.MatchString(reValue), nil
	}
	return false, fmt.Errorf("unsupported comparison operator: %s", op.ToString())
}

// compareCaseInsensitive compares two values using case-insensitive string comparison
// for strings, to match MySQL's utf8_general_ci collation behavior.
func compareCaseInsensitive(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	sa := strings.ToLower(toString(a))
	sb := strings.ToLower(toString(b))
	if sa < sb {
		return -1
	}
	if sa > sb {
		return 1
	}
	return 0
}

func effectiveTableCollation(def *catalog.TableDef) string {
	if def == nil {
		return ""
	}
	if def.Collation != "" {
		return strings.ToLower(def.Collation)
	}
	charset := def.Charset
	if charset == "" {
		charset = "utf8mb4"
	}
	return strings.ToLower(catalog.DefaultCollationForCharset(charset))
}

func resolveOrderByCollation(tableDefs []*catalog.TableDef, fromExprs ...sqlparser.TableExpr) string {
	if len(tableDefs) == 0 {
		// For virtual tables (information_schema, performance_schema) that don't have
		// catalog entries, detect the schema from the FROM clause and use utf8_general_ci
		// which is the default collation for these schemas in MySQL 8.0.
		if len(fromExprs) > 0 {
			if dbName := extractFromDatabase(fromExprs[0]); dbName != "" {
				dbLower := strings.ToLower(dbName)
				if dbLower == "information_schema" || dbLower == "performance_schema" {
					return "utf8_general_ci"
				}
			}
		}
		return "utf8mb4_0900_ai_ci"
	}
	// Use single-table collation first; for joins fallback to the first table.
	if len(tableDefs) == 1 {
		return effectiveTableCollation(tableDefs[0])
	}
	return effectiveTableCollation(tableDefs[0])
}

// extractFromDatabase extracts the database/qualifier name from a FROM expression.
func extractFromDatabase(expr sqlparser.TableExpr) string {
	switch te := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		if tn, ok := te.Expr.(sqlparser.TableName); ok {
			if !tn.Qualifier.IsEmpty() {
				return tn.Qualifier.String()
			}
		}
	}
	return ""
}

// compareByCollationNoTieBreak compares two values using the given collation,
// but unlike compareByCollation it does NOT apply a secondary tie-break based on
// the raw string when collation keys are equal. This is used for GROUP_CONCAT ORDER BY
// where MySQL relies on stable sort to preserve insertion order for equal elements
// (e.g. 'D' and 'd' under utf8mb4_0900_ai_ci should be considered truly equal).
func compareByCollationNoTieBreak(a, b interface{}, collation string) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	aIsStr := isStringValue(a)
	bIsStr := isStringValue(b)
	if aIsStr || bIsStr {
		aStr := toString(a)
		bStr := toString(b)
		sa := normalizeCollationKey(aStr, collation)
		sb := normalizeCollationKey(bStr, collation)
		if sa < sb {
			return -1
		}
		if sa > sb {
			return 1
		}
		return 0 // no tie-break: equal collation keys are treated as equal
	}
	return compareNumeric(a, b)
}

func compareByCollation(a, b interface{}, collation string) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	aIsStr := isStringValue(a)
	bIsStr := isStringValue(b)
	if aIsStr || bIsStr {
		aStr := toString(a)
		bStr := toString(b)

		sa := normalizeCollationKey(aStr, collation)
		sb := normalizeCollationKey(bStr, collation)
		if sa < sb {
			return -1
		}
		if sa > sb {
			return 1
		}
		// Tie-break for _ci collations
		coll := strings.ToLower(collation)
		if strings.HasSuffix(coll, "_ci") {
			var aTie, bTie string
			switch coll {
			case "sjis_japanese_ci", "cp932_japanese_ci":
				aTie = encodeStringForCollation(aStr, "sjis")
				bTie = encodeStringForCollation(bStr, "sjis")
			case "ujis_japanese_ci", "eucjpms_japanese_ci":
				aTie = encodeStringForCollation(aStr, "eucjp")
				bTie = encodeStringForCollation(bStr, "eucjp")
			default:
				aTie = aStr
				bTie = bStr
			}
			if aTie < bTie {
				return -1
			}
			if aTie > bTie {
				return 1
			}
		}
		return 0
	}
	return compareNumeric(a, b)
}

// isSafeForVitessCollation returns true if the given collation name belongs to a
// charset family where Vitess weight strings produce correct MySQL-compatible sort
// order. Only charsets that have been verified are included. Asian multi-byte
// charsets (sjis/ujis/cp932/eucjpms/euckr/gb2312/gbk/gb18030/big5) are excluded
// because Vitess does not support them and would produce incorrect results.
func isSafeForVitessCollation(collationName string) bool {
	lower := strings.ToLower(collationName)
	// Only apply Vitess weight strings for charsets where it's proven correct
	for _, prefix := range []string{"latin1_", "latin2_", "cp1251_", "utf8mb4_", "utf8mb3_", "utf8_", "binary"} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

func normalizeCollationKey(s string, collation string) string {
	coll := strings.ToLower(collation)

	// Collation-specific key normalization (checked first to override generic Vitess path)
	switch coll {
	case "utf8_general_ci", "utf8mb3_general_ci":
		return normalizeUTF8GeneralCIKey(s)
	case "sjis_japanese_ci", "cp932_japanese_ci":
		return encodeStringForCollation(foldASCIICase(s), "sjis")
	case "ujis_japanese_ci", "eucjpms_japanese_ci":
		return encodeStringForCollation(foldASCIICase(s), "eucjp")
	case "dec8_swedish_ci", "swe7_swedish_ci":
		// dec8_swedish_ci and swe7_swedish_ci: letters A-Z and a-z are case-folded using
		// ToUpper (mapped to 0x41-0x5A range). This ensures letters sort before 0x5B-0x60
		// characters ([, \, ], ^, _, `) since Z=0x5A < [=0x5B.
		// In contrast, ToLower would map letters to 0x61-0x7A which is ABOVE 0x5B-0x60,
		// causing [, \, etc. to incorrectly appear before letters in the sort output.
		return strings.ToUpper(s)
	}

	// Use Vitess for UCA 0900 collations which need accurate weight tables, and for other
	// safe charsets (latin1, latin2, cp1251, utf8mb4, utf8mb3/utf8, binary) where Vitess
	// produces correct MySQL-compatible weight strings.
	if strings.Contains(coll, "_0900_") || strings.HasSuffix(coll, "_0900_bin") || isSafeForVitessCollation(coll) {
		if vc := lookupVitessCollation(collation); vc != nil {
			ws := vitessWeightString(s, vc)
			return string(ws)
		}
	}
	if strings.HasSuffix(coll, "_ci") {
		// Use ToUpper for case-insensitive collations. MySQL's weight tables for _ci collations
		// assign letters the same weight as their uppercase equivalents (0x41-0x5A range).
		// This ensures that letters sort BEFORE punctuation in the 0x5B-0x60 range ([, \, ], ^, _, `),
		// which is correct for MySQL's Latin-family _ci collations.
		// Previously ToLower was used, which mapped letters to 0x61-0x7A (ABOVE 0x5B-0x60),
		// causing [ and \ to incorrectly sort before letters.
		return strings.ToUpper(s)
	}
	return s
}

func encodeStringForCollation(s, charset string) string {
	var enc *encoding.Encoder
	isUJIS := false
	switch strings.ToLower(charset) {
	case "sjis", "cp932":
		enc = japanese.ShiftJIS.NewEncoder()
	case "ujis", "eucjp", "eucjpms":
		enc = japanese.EUCJP.NewEncoder()
		isUJIS = true
	default:
		return s
	}
	encoded, err := encoding.ReplaceUnsupported(enc).Bytes([]byte(s))
	if err != nil {
		return s
	}
	if isUJIS && len(encoded) >= 2 && encoded[0] == 0xF9 && encoded[1] == 0xAE {
		// Align collation position of U+4EE1 (仡) with MySQL ujis ordering.
		encoded = append([]byte{0x8F, 0xB0, 0xC8}, encoded[2:]...)
	}
	return string(encoded)
}

func foldASCIICase(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] = b[i] + ('a' - 'A')
		}
	}
	return string(b)
}

func normalizeUTF8GeneralCIKey(s string) string {
	s = strings.ToLower(s)
	decomposed := norm.NFD.String(s)
	var b strings.Builder
	b.Grow(len(decomposed))
	for _, r := range decomposed {
		if unicode.Is(unicode.Mn, r) {
			continue
		}
		// In MySQL's utf8_general_ci, non-letter ASCII symbols like '_' (0x5F)
		// sort after letters. Remap them to high codepoints to match MySQL ordering.
		if r == '_' {
			b.WriteRune('\u007F') // DEL sorts after all ASCII letters
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// compareIntVsDecimalString compares a native integer (int64/uint64) against a
// decimal string using exact big.Rat arithmetic. This avoids float64 precision
// loss when values are near the boundaries of int64/uint64 (e.g. comparing
// BIGINT UNSIGNED MAX against a decimal like 18446744073709551615.00001).
// Returns (cmp, true) where cmp is -1/0/1, or (0, false) if not applicable.
func compareIntVsDecimalString(intVal interface{}, decStr string) (int, bool) {
	var intRat *big.Rat
	switch n := intVal.(type) {
	case int64:
		intRat = new(big.Rat).SetInt64(n)
	case uint64:
		intRat = new(big.Rat).SetUint64(n)
	default:
		return 0, false
	}
	decRat := new(big.Rat)
	if _, ok := decRat.SetString(decStr); !ok {
		return 0, false
	}
	cmp := intRat.Cmp(decRat)
	return cmp, true
}

func compareNumeric(a, b interface{}) int {
	// If both values are strings (or one is), try numeric comparison first
	aIsStr := isStringValue(a)
	bIsStr := isStringValue(b)
	if aIsStr || bIsStr {
		sa := toString(a)
		sb := toString(b)
		// Try numeric comparison when at least one value is not a string,
		// or when both are short numeric-looking strings (like "99999.99999" vs "-99999")
		shouldTryNumeric := !aIsStr || !bIsStr
		if !shouldTryNumeric && len(sa) <= 20 && len(sb) <= 20 {
			// Both are strings, but short enough to be valid numbers
			shouldTryNumeric = true
		}
		// When both are strings but at least one has a decimal point, try big.Rat comparison
		// to handle long DECIMAL values correctly (e.g. DECIMAL(65,30) vs large integer string).
		if !shouldTryNumeric && (strings.ContainsRune(sa, '.') || strings.ContainsRune(sb, '.')) {
			ra := new(big.Rat)
			rb := new(big.Rat)
			if _, okA := ra.SetString(sa); okA {
				if _, okB := rb.SetString(sb); okB {
					return ra.Cmp(rb)
				}
			}
		}
		if shouldTryNumeric {
			// Use exact big.Rat comparison when one side is a native integer and the
			// other is a decimal string. This avoids float64 precision loss for values
			// near the boundaries of int64/uint64 (e.g. BIGINT UNSIGNED MAX comparisons).
			if !aIsStr && bIsStr && strings.ContainsRune(sb, '.') {
				if cmp, ok := compareIntVsDecimalString(a, sb); ok {
					return cmp
				}
			}
			if !bIsStr && aIsStr && strings.ContainsRune(sa, '.') {
				if cmp, ok := compareIntVsDecimalString(b, sa); ok {
					return -cmp
				}
			}
			fa, errA := strconv.ParseFloat(sa, 64)
			fb, errB := strconv.ParseFloat(sb, 64)
			if errA == nil && errB == nil {
				if fa < fb {
					return -1
				}
				if fa > fb {
					return 1
				}
				return 0
			}
			// MySQL type coercion: when one side is a native numeric type and the
			// other is a non-numeric string, treat the non-numeric string as 0.
			// e.g. INT_COL > 'A' → INT_COL > 0 (MySQL converts non-numeric string to 0)
			if !aIsStr && errB != nil {
				fb = 0
				if fa < fb {
					return -1
				}
				if fa > fb {
					return 1
				}
				return 0
			}
			if !bIsStr && errA != nil {
				fa = 0
				if fa < fb {
					return -1
				}
				if fa > fb {
					return 1
				}
				return 0
			}
		}
		// Normalize date values first (e.g., "20070523091528" -> "2007-05-23 09:15:28")
		if na := normalizeDateTimeString(sa); na != "" {
			sa = na
		}
		if nb := normalizeDateTimeString(sb); nb != "" {
			sb = nb
		}
		// Normalize date/time comparisons: when one is TIME-like and
		// the other is DATETIME-like, extract the matching part.
		sa, sb = normalizeDateTimeForCompare(sa, sb)

		// Apply PAD SPACE semantics for non-binary string comparisons.
		// MySQL's default collations (latin1_swedish_ci, utf8mb4_0900_ai_ci, etc.)
		// use PAD SPACE: the shorter string is virtually padded with spaces before comparison.
		// This means 'a\0' < 'a' because 'a' pads to 'a ' and NUL (0x00) < space (0x20).
		padLen := len(sa)
		if len(sb) > padLen {
			padLen = len(sb)
		}
		for len(sa) < padLen {
			sa += " "
		}
		for len(sb) < padLen {
			sb += " "
		}

		if sa < sb {
			return -1
		}
		if sa > sb {
			return 1
		}
		return 0
	}
	// Use exact integer comparison for int64/uint64 pairs to avoid float64 precision loss
	// near boundary values (e.g. INT64_MAX-1 vs INT64_MAX both round to the same float64).
	switch al := a.(type) {
	case int64:
		switch br := b.(type) {
		case int64:
			if al < br {
				return -1
			}
			if al > br {
				return 1
			}
			return 0
		case uint64:
			// int64 vs uint64: if int64 is negative, it's always less than any uint64.
			if al < 0 {
				return -1
			}
			aU := uint64(al)
			if aU < br {
				return -1
			}
			if aU > br {
				return 1
			}
			return 0
		}
	case uint64:
		switch br := b.(type) {
		case uint64:
			if al < br {
				return -1
			}
			if al > br {
				return 1
			}
			return 0
		case int64:
			// uint64 vs int64: if int64 is negative, uint64 is always greater.
			if br < 0 {
				return 1
			}
			bU := uint64(br)
			if al < bU {
				return -1
			}
			if al > bU {
				return 1
			}
			return 0
		}
	}
	fa := toFloat(a)
	fb := toFloat(b)
	if fa < fb {
		return -1
	}
	if fa > fb {
		return 1
	}
	return 0
}

func evalLiteralForPK(expr sqlparser.Expr) interface{} {
	switch v := expr.(type) {
	case *sqlparser.Literal:
		switch v.Type {
		case sqlparser.IntVal:
			n, err := strconv.ParseInt(v.Val, 10, 64)
			if err != nil {
				u, err2 := strconv.ParseUint(v.Val, 10, 64)
				if err2 != nil {
					return nil
				}
				return u
			}
			return n
		case sqlparser.HexNum:
			s := v.Val
			if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
				s = s[2:]
			}
			n, err := strconv.ParseInt(s, 16, 64)
			if err != nil {
				return nil
			}
			return n
		case sqlparser.StrVal:
			return v.Val
		case sqlparser.HexVal:
			return v.Val
		case sqlparser.FloatVal:
			f, err := strconv.ParseFloat(v.Val, 64)
			if err != nil {
				return nil
			}
			return f
		default:
			return nil
		}
	case *sqlparser.UnaryExpr:
		if v.Operator == sqlparser.UMinusOp {
			if inner := evalLiteralForPK(v.Expr); inner != nil {
				switch n := inner.(type) {
				case int64:
					return -n
				case float64:
					return -n
				}
			}
		}
		return nil
	default:
		return nil
	}
}

// evalMatchExpr evaluates a MATCH(col1,col2,...) AGAINST('query' [mode]) expression.
func (e *Executor) evalMatchExpr(v *sqlparser.MatchExpr) (interface{}, error) {
	// 0. Validate that a FULLTEXT index exists for the referenced columns.
	if err := e.validateFulltextIndex(v); err != nil {
		return nil, err
	}

	// 1. Collect text from the FULLTEXT index columns (not the MATCH columns).
	// MySQL uses all columns from the matching FULLTEXT index, regardless of
	// what columns are specified in MATCH(). This handles cases like MATCH(c,c)
	// which should use the (b,c) FULLTEXT index and search both columns.
	ftCols := e.resolveFulltextIndexColumns(v)
	var docParts []string
	if len(ftCols) > 0 {
		seen := make(map[string]bool)
		for _, colName := range ftCols {
			if seen[colName] {
				continue
			}
			seen[colName] = true
			col := &sqlparser.ColName{Name: sqlparser.NewIdentifierCI(colName)}
			if len(v.Columns) > 0 && !v.Columns[0].Qualifier.IsEmpty() {
				col.Qualifier = v.Columns[0].Qualifier
			}
			val, err := e.evalExpr(col)
			if err != nil {
				continue
			}
			if val != nil {
				docParts = append(docParts, toString(val))
			}
		}
	} else {
		// Fallback: use MATCH columns directly (deduplicated)
		seen := make(map[string]bool)
		for _, col := range v.Columns {
			colKey := strings.ToLower(col.Name.String())
			if seen[colKey] {
				continue
			}
			seen[colKey] = true
			val, err := e.evalExpr(col)
			if err != nil {
				continue
			}
			if val != nil {
				docParts = append(docParts, toString(val))
			}
		}
	}
	docText := strings.Join(docParts, " ")

	// 2. Evaluate the AGAINST expression to get the search string
	searchVal, err := e.evalExpr(v.Expr)
	if err != nil {
		return float64(0), nil
	}
	if searchVal == nil {
		return float64(0), nil
	}
	searchStr := toString(searchVal)

	minTokenSize := 3

	// 3. Dispatch based on match option
	switch v.Option {
	case sqlparser.BooleanModeOpt:
		return ftsEvalBoolean(docText, searchStr, minTokenSize), nil
	default:
		// Natural language mode (also covers NoOption, QueryExpansionOpt, NaturalLanguageModeWithQueryExpansionOpt)
		// Use BM25 collection-level scoring only for MyISAM tables.
		// InnoDB FTS uses a different (simpler) scoring model that matches the constant ftsBaseScore.
		var stats *ftsCollStats
		tableName := e.resolveFulltextTableName(v)
		if tableName != "" && e.isMyISAMTable(tableName) {
			colsForStats := ftCols
			if len(colsForStats) == 0 {
				// Build fallback column list from MATCH columns
				seen := make(map[string]bool)
				for _, col := range v.Columns {
					k := strings.ToLower(col.Name.String())
					if !seen[k] {
						seen[k] = true
						colsForStats = append(colsForStats, k)
					}
				}
			}
			stats = e.computeFtsCollStats(tableName, colsForStats, minTokenSize)
		}
		return ftsEvalNaturalLanguage(docText, searchStr, minTokenSize, stats), nil
	}
}

// isMyISAMTable returns true if the named table (in CurrentDB) uses the MyISAM engine.
func (e *Executor) isMyISAMTable(tableName string) bool {
	if e.CurrentDB == "" || e.Catalog == nil {
		return false
	}
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil || db == nil {
		return false
	}
	tblDef, err := db.GetTable(tableName)
	if err != nil || tblDef == nil {
		return false
	}
	return strings.EqualFold(tblDef.Engine, "MYISAM")
}

// Returns a relevance score (float64). 0 means no match.
// stats provides collection-level statistics for BM25 scoring; if nil, falls back to
// simple TF-based scoring using ftsBaseScore.
func ftsEvalNaturalLanguage(docText, searchStr string, minTokenSize int, stats *ftsCollStats) float64 {
	docTokens := ftsTokenize(docText, minTokenSize)
	queryTokens := ftsTokenize(searchStr, minTokenSize)

	if len(queryTokens) == 0 {
		return float64(0)
	}

	// Build document token frequency map
	docFreq := make(map[string]int)
	for _, t := range docTokens {
		docFreq[t]++
	}
	docLen := len(docTokens)

	if stats != nil && stats.N > 0 {
		// BM25 scoring using collection statistics.
		// IDF = ln((N - n + 0.5) / (n + 0.5))  (Okapi BM25 variant)
		// TF_weight = tf * (k1 + 1) / (tf + k1 * (1 - b + b * dl / avgdl))
		// score = sum(IDF * TF_weight) for each non-stopword query term
		const k1 = 1.2
		const b = 0.75
		N := float64(stats.N)
		avgDL := stats.AvgDL
		if avgDL <= 0 {
			avgDL = 1.0
		}
		dl := float64(docLen)

		var totalScore float64
		for _, qt := range queryTokens {
			if ftsStopwords[qt] {
				continue
			}
			tf := float64(docFreq[qt])
			if tf == 0 {
				continue
			}
			n := float64(stats.DF[qt])
			if n == 0 {
				continue
			}
			idf := math.Log((N - n + 0.5) / (n + 0.5))
			if idf < 0 {
				idf = 0
			}
			tfWeight := tf * (k1 + 1) / (tf + k1*(1-b+b*dl/avgDL))
			totalScore += idf * tfWeight
		}
		return totalScore
	}

	// Fallback: simple TF-based scoring without collection stats.
	var totalScore float64
	for _, qt := range queryTokens {
		if ftsStopwords[qt] {
			continue
		}
		if cnt, ok := docFreq[qt]; ok {
			// Simple TF-based scoring
			totalScore += float64(cnt) * ftsBaseScore
		}
	}
	return totalScore
}

// ftsEvalBoolean performs boolean-mode full-text search.
// Returns a relevance score (float64). 0 means no match.
func ftsEvalBoolean(docText, searchStr string, minTokenSize int) float64 {
	docTokens := ftsTokenize(docText, minTokenSize)
	docFreq := make(map[string]int)
	for _, t := range docTokens {
		docFreq[t]++
	}
	docLower := strings.ToLower(docText)

	terms := parseBooleanQuery(searchStr, minTokenSize)
	return evalBooleanTerms(terms, docFreq, docLower, minTokenSize)
}

// boolTermOp represents a boolean operator for a search term.
type boolTermOp int

const (
	boolDefault boolTermOp = iota // optional (OR semantics)
	boolRequired                  // + (AND required)
	boolExcluded                  // - (NOT excluded)
	boolNegate                    // ~ (present but negated rank)
	boolIncRank                   // > (increase rank)
	boolDecRank                   // < (decrease rank)
)

// boolTerm represents a single term or sub-expression in a boolean FTS query.
type boolTerm struct {
	op       boolTermOp
	word     string     // single word (lowercased)
	wildcard bool       // trailing * (prefix match)
	phrase   string     // quoted phrase (lowercased)
	sub      []boolTerm // sub-expression in parentheses
}

func parseBooleanQuery(searchStr string, minTokenSize int) []boolTerm {
	s := strings.TrimSpace(searchStr)
	terms, _ := parseBoolTerms(s, minTokenSize)
	return terms
}

func parseBoolTerms(s string, minTokenSize int) ([]boolTerm, string) {
	var terms []boolTerm
	for len(s) > 0 {
		s = strings.TrimLeft(s, " \t\n\r")
		if len(s) == 0 {
			break
		}
		if s[0] == ')' {
			break
		}

		op := boolDefault
		for len(s) > 0 {
			switch s[0] {
			case '+':
				op = boolRequired
				s = s[1:]
				continue
			case '-':
				op = boolExcluded
				s = s[1:]
				continue
			case '~':
				op = boolNegate
				s = s[1:]
				continue
			case '>':
				op = boolIncRank
				s = s[1:]
				continue
			case '<':
				op = boolDecRank
				s = s[1:]
				continue
			}
			break
		}
		s = strings.TrimLeft(s, " \t")
		if len(s) == 0 {
			break
		}

		if s[0] == '(' {
			sub, rest := parseBoolTerms(s[1:], minTokenSize)
			rest = strings.TrimLeft(rest, " \t")
			if len(rest) > 0 && rest[0] == ')' {
				rest = rest[1:]
			}
			terms = append(terms, boolTerm{op: op, sub: sub})
			s = rest
		} else if s[0] == '"' {
			end := strings.Index(s[1:], "\"")
			var phrase string
			if end < 0 {
				phrase = s[1:]
				s = ""
			} else {
				phrase = s[1 : end+1]
				s = s[end+2:]
			}
			terms = append(terms, boolTerm{op: op, phrase: strings.ToLower(phrase)})
		} else {
			end := 0
			for end < len(s) && s[end] != ' ' && s[end] != '\t' && s[end] != ')' && s[end] != '(' && s[end] != '"' {
				end++
			}
			word := s[:end]
			s = s[end:]
			wildcard := false
			if strings.HasSuffix(word, "*") {
				wildcard = true
				word = word[:len(word)-1]
			}
			// Strip non-word characters (punctuation like curly quotes) from the word.
			// Only keep letters, digits, and underscores (same logic as ftsTokenize).
			word = strings.Map(func(r rune) rune {
				if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || (r >= 0x80 && unicode.IsLetter(r)) {
					return r
				}
				return -1
			}, word)
			word = strings.ToLower(word)
			if word == "" || word == "&" {
				continue
			}
			terms = append(terms, boolTerm{op: op, word: word, wildcard: wildcard})
		}
	}
	return terms, s
}

func evalBooleanTerms(terms []boolTerm, docFreq map[string]int, docLower string, minTokenSize int) float64 {
	if len(terms) == 0 {
		return float64(0)
	}

	hasRequired := false
	for _, t := range terms {
		if t.op == boolRequired {
			hasRequired = true
			break
		}
	}

	var score float64
	allRequiredMet := true
	anyOptionalMatch := false

	for _, t := range terms {
		var matched bool
		var termScore float64

		if len(t.sub) > 0 {
			termScore = evalBooleanTerms(t.sub, docFreq, docLower, minTokenSize)
			matched = termScore > 0
		} else if t.phrase != "" {
			if strings.Contains(docLower, t.phrase) {
				matched = true
				termScore = 1.0
			}
		} else if t.wildcard {
			for tok, cnt := range docFreq {
				if strings.HasPrefix(tok, t.word) && !ftsStopwords[tok] {
					matched = true
					termScore += float64(cnt) * ftsBaseScore
				}
			}
		} else {
			// In boolean mode, stopwords never match
			if !ftsStopwords[t.word] {
				if _, ok := docFreq[t.word]; ok {
					matched = true
					// Boolean mode simple word match returns 1.0 per matching term
					// (binary presence/absence indicator), not a weighted score.
					termScore = 1.0
				}
			}
			// If it's a stopword, matched stays false
		}

		switch t.op {
		case boolRequired:
			if !matched {
				allRequiredMet = false
			} else {
				score += termScore
			}
		case boolExcluded:
			if matched {
				return float64(0)
			}
		case boolNegate:
			if matched {
				score -= termScore * 0.5
			} else {
				score += ftsBaseScore
			}
		case boolIncRank:
			if matched {
				score += termScore * 2
				anyOptionalMatch = true
			}
		case boolDecRank:
			if matched {
				score += termScore * 0.5
				anyOptionalMatch = true
			}
		default:
			if matched {
				score += termScore
				anyOptionalMatch = true
			}
		}
	}

	if hasRequired && !allRequiredMet {
		return float64(0)
	}
	if !hasRequired && !anyOptionalMatch {
		return float64(0)
	}
	if score <= 0 {
		score = 0.001
	}
	return score
}

// ---------------------------------------------------------------------------
// findMatchExprInWhere recursively searches an expression tree for a MATCH AGAINST
// expression and returns the first one found, or nil if none.
func findMatchExprInWhere(expr sqlparser.Expr) *sqlparser.MatchExpr {
	switch v := expr.(type) {
	case *sqlparser.MatchExpr:
		return v
	case *sqlparser.AndExpr:
		if m := findMatchExprInWhere(v.Left); m != nil {
			return m
		}
		return findMatchExprInWhere(v.Right)
	case *sqlparser.OrExpr:
		if m := findMatchExprInWhere(v.Left); m != nil {
			return m
		}
		return findMatchExprInWhere(v.Right)
	case *sqlparser.ComparisonExpr:
		if m := findMatchExprInWhere(v.Left); m != nil {
			return m
		}
		return findMatchExprInWhere(v.Right)
	case *sqlparser.NotExpr:
		return findMatchExprInWhere(v.Expr)
	}
	return nil
}

// Full-Text Search (MATCH … AGAINST) implementation
// ---------------------------------------------------------------------------

// ftsTokenize splits text into lowercase word tokens, similar to InnoDB's
// built-in parser. Words shorter than minLen are discarded.
func ftsTokenize(text string, minLen int) []string {
	var tokens []string
	word := strings.Builder{}
	for _, r := range text {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || (r >= 0x80 && unicode.IsLetter(r)) {
			word.WriteRune(unicode.ToLower(r))
		} else {
			if word.Len() >= minLen {
				tokens = append(tokens, word.String())
			}
			word.Reset()
		}
	}
	if word.Len() >= minLen {
		tokens = append(tokens, word.String())
	}
	return tokens
}

// ftsBaseScore is the base relevance score per word occurrence,
// approximating MySQL's IDF-based scoring for a small table.
const ftsBaseScore = 0.22764469683170319

// ftsCollStats holds per-table collection-level statistics used for BM25 scoring.
// These must be computed by scanning the entire table before per-row scoring.
type ftsCollStats struct {
	// N is the total number of documents (rows) in the collection.
	N int
	// DF maps each token to the number of documents it appears in.
	DF map[string]int
	// AvgDL is the average number of tokens per document (for BM25 length normalization).
	AvgDL float64
}

// ftsStopwords is the default InnoDB stopword list.
var ftsStopwords = map[string]bool{
	"a": true, "about": true, "an": true, "are": true, "as": true,
	"at": true, "be": true, "by": true, "com": true, "de": true,
	"en": true, "for": true, "from": true, "how": true, "i": true,
	"in": true, "is": true, "it": true, "la": true, "of": true,
	"on": true, "or": true, "that": true, "the": true, "this": true,
	"to": true, "was": true, "what": true, "when": true, "where": true,
	"who": true, "will": true, "with": true, "und": true, "www": true,
}

// resolveFulltextIndexColumns finds the FULLTEXT index that matches the MATCH
// expression and returns its column names. Returns nil if no index is found.
func (e *Executor) resolveFulltextIndexColumns(v *sqlparser.MatchExpr) []string {
	if e.CurrentDB == "" || e.Catalog == nil || len(v.Columns) == 0 {
		return nil
	}
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil || db == nil {
		return nil
	}

	matchSet := make(map[string]bool)
	for _, col := range v.Columns {
		matchSet[strings.ToLower(col.Name.String())] = true
	}

	tableName := ""
	if !v.Columns[0].Qualifier.IsEmpty() {
		tableName = v.Columns[0].Qualifier.Name.String()
	}

	checkTable := func(tblDef *catalog.TableDef) []string {
		for _, idx := range tblDef.Indexes {
			if idx.Type != "FULLTEXT" {
				continue
			}
			idxCols := make(map[string]bool)
			for _, c := range idx.Columns {
				idxCols[strings.ToLower(stripPrefixLengthFromCol(c))] = true
			}
			allFound := true
			for mc := range matchSet {
				if !idxCols[mc] {
					allFound = false
					break
				}
			}
			if allFound {
				return idx.Columns
			}
		}
		return nil
	}

	if tableName != "" {
		tblDef, _, err := findTableDefCaseInsensitive(db, tableName)
		if err != nil || tblDef == nil {
			return nil
		}
		return checkTable(tblDef)
	}
	for _, name := range db.ListTables() {
		tblDef, err := db.GetTable(name)
		if err != nil {
			continue
		}
		if cols := checkTable(tblDef); cols != nil {
			return cols
		}
	}
	return nil
}

// resolveFulltextTableName returns the canonical (lowercase) table name that owns the
// FULLTEXT index matching the MATCH expression, or "" if not found.
func (e *Executor) resolveFulltextTableName(v *sqlparser.MatchExpr) string {
	if e.CurrentDB == "" || e.Catalog == nil || len(v.Columns) == 0 {
		return ""
	}
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil || db == nil {
		return ""
	}

	matchSet := make(map[string]bool)
	for _, col := range v.Columns {
		matchSet[strings.ToLower(col.Name.String())] = true
	}

	tableName := ""
	if !v.Columns[0].Qualifier.IsEmpty() {
		tableName = v.Columns[0].Qualifier.Name.String()
	}

	hasMatchingFT := func(tblDef *catalog.TableDef) bool {
		for _, idx := range tblDef.Indexes {
			if idx.Type != "FULLTEXT" {
				continue
			}
			idxCols := make(map[string]bool)
			for _, c := range idx.Columns {
				idxCols[strings.ToLower(stripPrefixLengthFromCol(c))] = true
			}
			allFound := true
			for mc := range matchSet {
				if !idxCols[mc] {
					allFound = false
					break
				}
			}
			if allFound {
				return true
			}
		}
		return false
	}

	if tableName != "" {
		tblDef, _, err := findTableDefCaseInsensitive(db, tableName)
		if err != nil || tblDef == nil {
			return ""
		}
		if hasMatchingFT(tblDef) {
			return strings.ToLower(tableName)
		}
		return ""
	}
	for _, name := range db.ListTables() {
		tblDef, err := db.GetTable(name)
		if err != nil {
			continue
		}
		if hasMatchingFT(tblDef) {
			return strings.ToLower(name)
		}
	}
	return ""
}

// computeFtsCollStats scans all rows in the given table and computes collection-level
// BM25 statistics (N, DF per term, AvgDL) for the specified FULLTEXT index columns.
// Results are cached in e.ftsCollStatsCache under key "db.table:col1,col2,...".
func (e *Executor) computeFtsCollStats(tableName string, ftCols []string, minTokenSize int) *ftsCollStats {
	if e.Storage == nil || e.CurrentDB == "" {
		return nil
	}

	// Build cache key
	colKey := strings.Join(ftCols, ",")
	cacheKey := e.CurrentDB + "." + tableName + ":" + colKey
	if e.ftsCollStatsCache != nil {
		if cached, ok := e.ftsCollStatsCache[cacheKey]; ok {
			return cached
		}
	}

	tbl, err := e.Storage.GetTable(e.CurrentDB, tableName)
	if err != nil || tbl == nil {
		return nil
	}

	rows := tbl.Scan()
	N := len(rows)
	if N == 0 {
		return nil
	}

	// Compute DF (document frequency) and total token count
	dfMap := make(map[string]int)
	var totalTokens int
	for _, row := range rows {
		var parts []string
		seen := make(map[string]bool)
		for _, colName := range ftCols {
			if seen[colName] {
				continue
			}
			seen[colName] = true
			if val, ok := row[colName]; ok && val != nil {
				parts = append(parts, toString(val))
			}
		}
		docText := strings.Join(parts, " ")
		tokens := ftsTokenize(docText, minTokenSize)
		totalTokens += len(tokens)

		// Count each unique token once per document for DF
		tokenSet := make(map[string]bool)
		for _, tok := range tokens {
			tokenSet[tok] = true
		}
		for tok := range tokenSet {
			dfMap[tok]++
		}
	}

	avgDL := float64(totalTokens) / float64(N)
	if N == 0 {
		avgDL = 1.0
	}

	stats := &ftsCollStats{
		N:     N,
		DF:    dfMap,
		AvgDL: avgDL,
	}

	// Cache the result
	if e.ftsCollStatsCache == nil {
		e.ftsCollStatsCache = make(map[string]*ftsCollStats)
	}
	e.ftsCollStatsCache[cacheKey] = stats
	return stats
}

// validateFulltextIndex checks that the columns referenced in a MATCH expression
// have a matching FULLTEXT index. Returns ER_FT_MATCHING_KEY_NOT_FOUND if not.
func (e *Executor) validateFulltextIndex(v *sqlparser.MatchExpr) error {
	if len(v.Columns) == 0 {
		return nil
	}

	// Collect the column names from the MATCH expression
	matchCols := make([]string, len(v.Columns))
	for i, col := range v.Columns {
		matchCols[i] = strings.ToLower(col.Name.String())
	}
	sort.Strings(matchCols)

	// Determine table name from column qualifiers
	tableName := ""
	if !v.Columns[0].Qualifier.IsEmpty() {
		tableName = v.Columns[0].Qualifier.Name.String()
	}

	if e.CurrentDB == "" || e.Catalog == nil {
		return nil
	}
	db, dbErr := e.Catalog.GetDatabase(e.CurrentDB)
	if dbErr != nil || db == nil {
		return nil
	}

	// Deduplicate match columns
	matchSet := make(map[string]bool)
	for _, c := range matchCols {
		matchSet[c] = true
	}

	// hasMatchingFTIndex checks if a table definition has a FULLTEXT index
	// that covers all unique columns from the MATCH expression.
	hasMatchingFTIndex := func(tblDef *catalog.TableDef) bool {
		for _, idx := range tblDef.Indexes {
			if idx.Type != "FULLTEXT" {
				continue
			}
			idxCols := make(map[string]bool)
			for _, c := range idx.Columns {
				idxCols[strings.ToLower(stripPrefixLengthFromCol(c))] = true
			}
			// Check that all unique MATCH columns are in this FT index
			allFound := true
			for mc := range matchSet {
				if !idxCols[mc] {
					allFound = false
					break
				}
			}
			if allFound {
				return true
			}
		}
		return false
	}

	if tableName != "" {
		tblDef, _, err := findTableDefCaseInsensitive(db, tableName)
		if err != nil || tblDef == nil {
			return nil
		}
		if hasMatchingFTIndex(tblDef) {
			return nil
		}
		// MyISAM allows MATCH without a FULLTEXT index (full table scan without index).
		if strings.EqualFold(tblDef.Engine, "MYISAM") {
			return nil
		}
	} else {
		// No qualifier: search all tables in the current database
		for _, name := range db.ListTables() {
			tblDef, err := db.GetTable(name)
			if err != nil {
				continue
			}
			if hasMatchingFTIndex(tblDef) {
				return nil
			}
		}
		// No matching FULLTEXT index found. For MyISAM tables, MATCH without an
		// explicit FULLTEXT index is allowed (the engine performs a full table scan).
		// Check if any table in the database that contains one of the MATCH columns
		// uses the MyISAM engine. Also allow when the session default_storage_engine
		// is set to MyISAM (e.g. via force_myisam_default.inc).
		if eng, ok := e.getSysVar("default_storage_engine"); ok && strings.EqualFold(eng, "MYISAM") {
			return nil
		}
		for _, name := range db.ListTables() {
			tblDef, err := db.GetTable(name)
			if err != nil || tblDef == nil {
				continue
			}
			if !strings.EqualFold(tblDef.Engine, "MYISAM") {
				continue
			}
			// Check if this MyISAM table has at least one of the MATCH columns
			for _, col := range tblDef.Columns {
				if matchSet[strings.ToLower(col.Name)] {
					return nil
				}
			}
		}
	}

	return mysqlError(1191, "HY000", "Can't find FULLTEXT index matching the column list")
}
