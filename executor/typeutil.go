package executor

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"

	"github.com/myuon/mylite/storage"
	"vitess.io/vitess/go/vt/sqlparser"
)

// checkDecimalRange checks if a value fits within a DECIMAL(M,D) column's range.
func checkDecimalRange(colType string, v interface{}) error {
	lower := strings.ToLower(colType)
	lower = strings.TrimSuffix(strings.TrimSpace(lower), " unsigned")
	lower = strings.TrimSpace(lower)
	var m, d int
	if n, err := fmt.Sscanf(lower, "decimal(%d,%d)", &m, &d); (err == nil && n == 2) || func() bool {
		if n2, err2 := fmt.Sscanf(lower, "decimal(%d)", &m); err2 == nil && n2 == 1 {
			d = 0
			return true
		}
		return false
	}() {
		f := toFloat(v)
		if f < 0 {
			f = -f
		}
		intDigits := m - d
		if intDigits <= 0 {
			intDigits = 1
		}
		maxVal := 1.0
		for i := 0; i < intDigits; i++ {
			maxVal *= 10
		}
		if f >= maxVal {
			return fmt.Errorf("out of range")
		}
	}
	return nil
}

// isIntValLiteral returns true if the expression is an IntVal literal
// (a decimal integer literal, not a hex or string literal).
func isIntValLiteral(expr sqlparser.Expr) bool {
	if lit, ok := expr.(*sqlparser.Literal); ok {
		return lit.Type == sqlparser.IntVal
	}
	return false
}

// isHexNumLiteral returns true if the expression is a HexNum literal (0x...).
func isHexNumLiteral(expr sqlparser.Expr) bool {
	if lit, ok := expr.(*sqlparser.Literal); ok {
		return lit.Type == sqlparser.HexNum
	}
	return false
}

// hexDecodeString decodes a hex string (e.g. "123ABC") to raw bytes (e.g. "\x12\x3a\xbc").
// Returns error if the input is not valid hex.
func hexDecodeString(s string) (string, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// toBinaryBytesForBitOp extracts raw bytes from a value if it should be treated as
// a binary string for bitwise operations. Returns (bytes, true) if binary, or (nil, false) if not.
// A value is "binary" if it is:
//   - HexBytes (from x'...' hex literal or stored in BINARY/VARBINARY column)
//   - string with non-printable/null bytes (raw binary data from _binary introducer or stored binary)
//
// Pure integer/float values are NOT binary.
func toBinaryBytesForBitOp(v interface{}) ([]byte, bool) {
	switch n := v.(type) {
	case HexBytes:
		// x'...' literal or BINARY/VARBINARY column value: hex digits string, decode to raw bytes
		hexStr := string(n)
		if len(hexStr)%2 != 0 {
			hexStr = "0" + hexStr
		}
		decoded, err := hex.DecodeString(hexStr)
		if err != nil {
			return nil, false
		}
		return decoded, true
	}
	return nil, false
}

// binaryBitwiseAnd performs byte-wise AND on two binary byte slices of equal length.
// Returns error if lengths differ.
func binaryBitwiseAnd(left, right []byte) (string, error) {
	if len(left) != len(right) {
		return "", mysqlError(3513, "HY000", "Binary operands of bitwise operators must be of equal length")
	}
	result := make([]byte, len(left))
	for i := range left {
		result[i] = left[i] & right[i]
	}
	return string(result), nil
}

// binaryBitwiseOr performs byte-wise OR on two binary byte slices of equal length.
func binaryBitwiseOr(left, right []byte) (string, error) {
	if len(left) != len(right) {
		return "", mysqlError(3513, "HY000", "Binary operands of bitwise operators must be of equal length")
	}
	result := make([]byte, len(left))
	for i := range left {
		result[i] = left[i] | right[i]
	}
	return string(result), nil
}

// binaryBitwiseXor performs byte-wise XOR on two binary byte slices of equal length.
func binaryBitwiseXor(left, right []byte) (string, error) {
	if len(left) != len(right) {
		return "", mysqlError(3513, "HY000", "Binary operands of bitwise operators must be of equal length")
	}
	result := make([]byte, len(left))
	for i := range left {
		result[i] = left[i] ^ right[i]
	}
	return string(result), nil
}

// binaryBitwiseNot performs byte-wise NOT (flip all bits) on a binary byte slice.
func binaryBitwiseNot(b []byte) string {
	result := make([]byte, len(b))
	for i := range b {
		result[i] = ^b[i]
	}
	return string(result)
}

// binaryShiftLeft shifts a binary byte slice left by n bits, returning the same-length result.
func binaryShiftLeft(b []byte, n uint64) string {
	if len(b) == 0 {
		return ""
	}
	byteShift := int(n / 8)
	bitShift := uint(n % 8)
	result := make([]byte, len(b))
	if byteShift >= len(b) {
		return string(result) // all zeros
	}
	for i := 0; i < len(b)-byteShift; i++ {
		result[i] = b[i+byteShift] << bitShift
		if bitShift > 0 && i+byteShift+1 < len(b) {
			result[i] |= b[i+byteShift+1] >> (8 - bitShift)
		}
	}
	return string(result)
}

// toBinaryBytesFromInt converts a uint64 integer to a big-endian byte slice of the given length.
// Used when one operand is binary and the other is an integer in a binary bitwise op.
func toBinaryBytesFromInt(v uint64, length int) []byte {
	if length <= 0 {
		length = 8
	}
	buf := make([]byte, 8)
	buf[0] = byte(v >> 56)
	buf[1] = byte(v >> 48)
	buf[2] = byte(v >> 40)
	buf[3] = byte(v >> 32)
	buf[4] = byte(v >> 24)
	buf[5] = byte(v >> 16)
	buf[6] = byte(v >> 8)
	buf[7] = byte(v)
	if length >= 8 {
		// Right-align 8 bytes within longer buffer
		result := make([]byte, length)
		copy(result[length-8:], buf)
		return result
	}
	// Take rightmost 'length' bytes
	return buf[8-length:]
}

// binaryShiftRight shifts a binary byte slice right by n bits, returning the same-length result.
func binaryShiftRight(b []byte, n uint64) string {
	if len(b) == 0 {
		return ""
	}
	byteShift := int(n / 8)
	bitShift := uint(n % 8)
	result := make([]byte, len(b))
	if byteShift >= len(b) {
		return string(result) // all zeros
	}
	for i := len(b) - 1; i >= byteShift; i-- {
		result[i] = b[i-byteShift] >> bitShift
		if bitShift > 0 && i-byteShift-1 >= 0 {
			result[i] |= b[i-byteShift-1] << (8 - bitShift)
		}
	}
	return string(result)
}

// toDateTimeStringAsFloat converts a DATE/DATETIME/TIME string to a float value
// using MySQL's numeric conversion rules (e.g., "2015-10-24 12:00:00" → 20151024120000).
// Returns 0 if the string is not a recognized date/time format.
// Does NOT handle binary byte strings (unlike toFloat).
func toDateTimeStringAsFloat(s string) float64 {
	// TIME format: HH:MM:SS
	if strings.Count(s, ":") == 2 {
		sign := 1.0
		main := s
		if strings.HasPrefix(s, "-") {
			sign = -1.0
			main = s[1:]
		}
		fracPart := ""
		if dot := strings.IndexByte(main, '.'); dot >= 0 {
			fracPart = main[dot+1:]
			main = main[:dot]
		}
		tparts := strings.Split(main, ":")
		if len(tparts) == 3 {
			h, eh := strconv.Atoi(tparts[0])
			m, em := strconv.Atoi(tparts[1])
			sec, es := strconv.Atoi(tparts[2])
			if eh == nil && em == nil && es == nil {
				base := float64(h*10000 + m*100 + sec)
				if fracPart != "" {
					if fracDigits, err := strconv.ParseFloat("0."+fracPart, 64); err == nil {
						base += fracDigits
					}
				}
				return sign * base
			}
		}
	}
	// DATE/DATETIME format: YYYY-MM-DD or YYYY-MM-DD HH:MM:SS
	if len(s) >= 10 && s[4] == '-' && s[7] == '-' {
		y, ey := strconv.Atoi(s[0:4])
		mo, em := strconv.Atoi(s[5:7])
		d, ed := strconv.Atoi(s[8:10])
		if ey == nil && em == nil && ed == nil {
			base := float64(y*10000 + mo*100 + d)
			if len(s) >= 19 && s[10] == ' ' && s[13] == ':' && s[16] == ':' {
				h, eh := strconv.Atoi(s[11:13])
				mi, emi := strconv.Atoi(s[14:16])
				se, es := strconv.Atoi(s[17:19])
				if eh == nil && emi == nil && es == nil {
					base = float64((y*10000+mo*100+d)*1000000 + h*10000 + mi*100 + se)
				}
			}
			return base
		}
	}
	return 0
}

// toUint64ForBitOp converts a value to uint64 for bitwise operations,
// preserving the full uint64 range without float precision loss.
func toUint64ForBitOp(v interface{}) uint64 {
	switch n := v.(type) {
	case int64:
		return uint64(n)
	case uint64:
		return n
	case HexBytes:
		decoded, err := hex.DecodeString(string(n))
		if err != nil || len(decoded) == 0 {
			return 0
		}
		var val uint64
		for _, b := range decoded {
			val = val<<8 | uint64(b)
		}
		return val
	case string:
		s := strings.TrimSpace(n)
		// Try uint64 first, then int64, then float
		if u, err := strconv.ParseUint(s, 10, 64); err == nil {
			return u
		}
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return uint64(i)
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			if f < 0 {
				return uint64(int64(f))
			}
			if f >= float64(math.MaxUint64) {
				return math.MaxUint64
			}
			return uint64(f)
		}
		// Handle DATE/DATETIME string formats (e.g., "2015-10-24 12:00:00" → 20151024120000).
		// Only for printable ASCII strings that look like date/time formats.
		f := toDateTimeStringAsFloat(s)
		if f != 0 {
			if f < 0 {
				return uint64(int64(f))
			}
			if f >= float64(math.MaxUint64) {
				return math.MaxUint64
			}
			return uint64(f)
		}
		return 0
	default:
		f := toFloat(v)
		if f < 0 {
			return uint64(int64(f))
		}
		if f >= float64(math.MaxUint64) {
			return math.MaxUint64
		}
		return uint64(f)
	}
}

// toString converts a value to string.
func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case HexBytes:
		return string(val)
	case EnumValue:
		return string(val)
	case []byte:
		return string(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case uint64:
		return strconv.FormatUint(val, 10)
	case float64:
		return formatMySQLFloatString(val)
	case ScaledValue:
		return formatMySQLFloatString(val.Value)
	case DivisionResult:
		return fmt.Sprintf("%.*f", val.Precision, val.Value)
	case AvgResult:
		return fmt.Sprintf("%.*f", val.Scale, val.Value)
	case bool:
		if val {
			return "1"
		}
		return "0"
	}
	return fmt.Sprintf("%v", v)
}

func formatMySQLFloatString(v float64) string {
	if math.IsNaN(v) {
		return "NaN"
	}
	if math.IsInf(v, 1) {
		return "inf"
	}
	if math.IsInf(v, -1) {
		return "-inf"
	}
	abs := math.Abs(v)
	if abs != 0 && (abs >= 1e14 || abs < 1e-4) {
		s := strconv.FormatFloat(v, 'e', 5, 64)
		s = strings.Replace(s, "e+0", "e", 1)
		s = strings.Replace(s, "e-0", "e-", 1)
		s = strings.Replace(s, "e+", "e", 1)
		// Strip trailing zeros from the mantissa (MySQL omits them, e.g. 1.00000e43 -> 1e43)
		if eIdx := strings.IndexByte(s, 'e'); eIdx > 0 {
			mantissa := s[:eIdx]
			exp := s[eIdx:]
			if strings.Contains(mantissa, ".") {
				mantissa = strings.TrimRight(mantissa, "0")
				mantissa = strings.TrimRight(mantissa, ".")
			}
			s = mantissa + exp
		}
		return s
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// FormatMySQLFloat formats a float64 value using MySQL's scientific notation style
// (trailing zeros stripped, exponent normalized).
func FormatMySQLFloat(v float64) string {
	return formatMySQLFloatString(v)
}

// FormatMySQLFloat32 formats a float32 value using MySQL's FLOAT column display style
// (6 significant digits with trailing zeros preserved for single-precision columns).
func FormatMySQLFloat32(v float32) string {
	f := float64(v)
	if math.IsNaN(f) {
		return "NaN"
	}
	if math.IsInf(f, 1) {
		return "inf"
	}
	if math.IsInf(f, -1) {
		return "-inf"
	}
	abs := math.Abs(f)
	if abs != 0 && (abs >= 1e14 || abs < 1e-4) {
		// Use bitSize=32 for float32 precision, keeping 5 decimal places (6 sig figs)
		s := strconv.FormatFloat(f, 'e', 5, 32)
		s = strings.Replace(s, "e+0", "e", 1)
		s = strings.Replace(s, "e-0", "e-", 1)
		s = strings.Replace(s, "e+", "e", 1)
		// Strip trailing zeros from the mantissa (MySQL omits them for FLOAT scientific notation too)
		if eIdx := strings.IndexByte(s, 'e'); eIdx > 0 {
			mantissa := s[:eIdx]
			exp := s[eIdx:]
			if strings.Contains(mantissa, ".") {
				mantissa = strings.TrimRight(mantissa, "0")
				mantissa = strings.TrimRight(mantissa, ".")
			}
			s = mantissa + exp
		}
		return s
	}
	return strconv.FormatFloat(f, 'f', -1, 32)
}

func isStringValue(v interface{}) bool {
	switch v.(type) {
	case string, []byte:
		return true
	}
	return false
}

// toInt64 converts a value to int64.
func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case uint64:
		return int64(n)
	case float64:
		return int64(n)
	case SysVarDouble:
		return int64(n.Value)
	case ScaledValue:
		return int64(n.Value)
	case DivisionResult:
		return int64(n.Value)
	case AvgResult:
		return int64(n.Value)
	case HexBytes:
		decoded, err := hex.DecodeString(string(n))
		if err != nil || len(decoded) == 0 {
			return 0
		}
		var val uint64
		for _, b := range decoded {
			val = val<<8 | uint64(b)
		}
		return int64(val)
	case string:
		i, err := strconv.ParseInt(n, 10, 64)
		if err != nil {
			// Try parsing as float and truncating (handles "1.6000" etc.)
			if f, ferr := strconv.ParseFloat(n, 64); ferr == nil {
				return int64(f)
			}
		}
		return i
	case bool:
		if n {
			return 1
		}
		return 0
	}
	return 0
}

// toUint64 converts a value to uint64 for bitwise aggregate operations.
func toUint64(v interface{}) uint64 {
	switch n := v.(type) {
	case int64:
		return uint64(n)
	case uint64:
		return n
	case float64:
		return uint64(int64(n))
	case DivisionResult:
		return uint64(int64(n.Value))
	case AvgResult:
		return uint64(int64(n.Value))
	case HexBytes:
		decoded, err := hex.DecodeString(string(n))
		if err != nil || len(decoded) == 0 {
			return 0
		}
		var val uint64
		for _, b := range decoded {
			val = val<<8 | uint64(b)
		}
		return val
	case []byte:
		var val uint64
		for _, b := range n {
			val = val<<8 | uint64(b)
		}
		return val
	case string:
		if u, err := strconv.ParseUint(n, 10, 64); err == nil {
			return u
		}
		if i, err := strconv.ParseInt(n, 10, 64); err == nil {
			return uint64(i)
		}
		if f, err := strconv.ParseFloat(n, 64); err == nil {
			return uint64(int64(f))
		}
		return 0
	case bool:
		if n {
			return 1
		}
		return 0
	}
	return 0
}

// isTruthy returns true if the value is considered truthy in MySQL.
func isTruthy(v interface{}) bool {
	if v == nil {
		return false
	}
	switch val := v.(type) {
	case bool:
		return val
	case int64:
		return val != 0
	case float64:
		return val != 0
	case ScaledValue:
		return val.Value != 0
	case DivisionResult:
		return val.Value != 0
	case AvgResult:
		return val.Value != 0
	case string:
		return val != "" && val != "0"
	}
	return false
}

// addAggregatesToRow walks an expression tree and computes any aggregate functions,
// storing their results in the row with their display names.
func (e *Executor) addAggregatesToRow(expr sqlparser.Expr, row storage.Row, groupRows []storage.Row) {
	switch v := expr.(type) {
	case *sqlparser.ComparisonExpr:
		e.addAggregatesToRow(v.Left, row, groupRows)
		e.addAggregatesToRow(v.Right, row, groupRows)
	case *sqlparser.AndExpr:
		e.addAggregatesToRow(v.Left, row, groupRows)
		e.addAggregatesToRow(v.Right, row, groupRows)
	case *sqlparser.OrExpr:
		e.addAggregatesToRow(v.Left, row, groupRows)
		e.addAggregatesToRow(v.Right, row, groupRows)
	case *sqlparser.CountStar, *sqlparser.Count, *sqlparser.Sum, *sqlparser.Max, *sqlparser.Min, *sqlparser.Avg, *sqlparser.GroupConcatExpr,
		*sqlparser.JSONArrayAgg, *sqlparser.JSONObjectAgg,
		*sqlparser.Variance, *sqlparser.VarPop, *sqlparser.VarSamp,
		*sqlparser.Std, *sqlparser.StdDev, *sqlparser.StdPop, *sqlparser.StdSamp:
		displayName := aggregateDisplayName(expr)
		if _, ok := row[displayName]; !ok {
			repRow := storage.Row{}
			if len(groupRows) > 0 {
				repRow = groupRows[0]
			}
			val, err := evalAggregateExpr(expr, groupRows, repRow)
			if err == nil {
				row[displayName] = val
			}
		}
	case *sqlparser.IsExpr:
		// Walk into IS expressions to find aggregates (e.g., avg(x) IS NOT NULL)
		e.addAggregatesToRow(v.Left, row, groupRows)
	case *sqlparser.FuncExpr:
		// Walk into function arguments to find nested aggregates (e.g., INSTR(GROUP_CONCAT(x), y))
		for _, arg := range v.Exprs {
			e.addAggregatesToRow(arg, row, groupRows)
		}
	case *sqlparser.BinaryExpr:
		e.addAggregatesToRow(v.Left, row, groupRows)
		e.addAggregatesToRow(v.Right, row, groupRows)
	case *sqlparser.UnaryExpr:
		e.addAggregatesToRow(v.Expr, row, groupRows)
	case *sqlparser.NotExpr:
		e.addAggregatesToRow(v.Expr, row, groupRows)
	}
}

// normalizeYearComparison normalizes a comparison between a YEAR value
// and the other is a small string value ('0'-'99'), and converts the small one using YEAR rules.
// Only string values are converted (not integer literals), matching MySQL's behavior.
// origLeft/origRight are the original interface{} values to check types.
func normalizeYearComparison(ls, rs string) (string, string) {
	return normalizeYearComparisonTyped(ls, rs, nil, nil)
}

func normalizeYearComparisonTyped(ls, rs string, origLeft, origRight interface{}) (string, string) {
	lf, le := strconv.ParseFloat(ls, 64)
	rf, re := strconv.ParseFloat(rs, 64)
	if le != nil || re != nil {
		return ls, rs
	}
	li, ri := int(lf), int(rf)

	isYearLike := func(n int) bool {
		return n == 0 || (n >= 1901 && n <= 2155)
	}
	isSmallYear := func(n int) bool {
		return n >= 0 && n <= 99
	}
	convertSmallYear := func(n int) int {
		if n == 0 {
			return 0
		}
		if n >= 1 && n <= 69 {
			return 2000 + n
		}
		if n >= 70 && n <= 99 {
			return 1900 + n
		}
		return n
	}
	// Prefer conversions when at least one side is originally string-typed.
	// YEAR columns are stored as strings in this engine, while many non-YEAR
	// numeric columns stay numeric. This avoids forcing year conversion when
	// both sides are plain numeric values.
	isOrigString := func(orig interface{}) bool {
		if orig == nil {
			return true // Unknown type, assume string for backward compat
		}
		_, ok := orig.(string)
		return ok
	}

	if isYearLike(li) && isSmallYear(ri) && !isYearLike(ri) && (isOrigString(origLeft) || isOrigString(origRight)) {
		ri = convertSmallYear(ri)
		return ls, fmt.Sprintf("%d", ri)
	}
	if isYearLike(ri) && isSmallYear(li) && !isYearLike(li) && (isOrigString(origLeft) || isOrigString(origRight)) {
		li = convertSmallYear(li)
		return fmt.Sprintf("%d", li), rs
	}
	return ls, rs
}

// normalizeYearComparisonTypedStringOnly is a stricter YEAR normalization used
// by IN/NOT IN where this engine expects only string-side small years
// ('1'..'99') to be converted.
func normalizeYearComparisonTypedStringOnly(ls, rs string, origLeft, origRight interface{}) (string, string) {
	lf, le := strconv.ParseFloat(ls, 64)
	rf, re := strconv.ParseFloat(rs, 64)
	if le != nil || re != nil {
		return ls, rs
	}
	li, ri := int(lf), int(rf)

	isYearLike := func(n int) bool {
		return n == 0 || (n >= 1901 && n <= 2155)
	}
	isSmallYear := func(n int) bool {
		return n >= 0 && n <= 99
	}
	convertSmallYear := func(n int) int {
		if n == 0 {
			return 0
		}
		if n >= 1 && n <= 69 {
			return 2000 + n
		}
		if n >= 70 && n <= 99 {
			return 1900 + n
		}
		return n
	}
	isOrigString := func(orig interface{}) bool {
		if orig == nil {
			return true
		}
		_, ok := orig.(string)
		return ok
	}

	if isYearLike(li) && isSmallYear(ri) && !isYearLike(ri) && isOrigString(origRight) {
		ri = convertSmallYear(ri)
		return ls, fmt.Sprintf("%d", ri)
	}
	if isYearLike(ri) && isSmallYear(li) && !isYearLike(li) && isOrigString(origLeft) {
		li = convertSmallYear(li)
		return fmt.Sprintf("%d", li), rs
	}
	return ls, rs
}

// isNativeNumericType returns true if the value is a Go numeric type (int64, uint64, float64).
// Used for MySQL-compatible type coercion: when comparing a number with a non-numeric string,
// the string is cast to 0.
func isNativeNumericType(v interface{}) bool {
	switch v.(type) {
	case int64, uint64, float64, int, int32, float32:
		return true
	}
	return false
}

// isNumericString returns true if s is parseable as a decimal or floating-point number.
// Used to distinguish numeric strings (e.g. DECIMAL column "123.45") from raw binary byte strings.
func isNumericString(s string) bool {
	if s == "" {
		return false
	}
	_, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return err == nil
}

// toUint64ForBitOpAsBinaryString converts a value to uint64 for integer-mode bitwise operations.
// When the value is binary (HexBytes from BINARY/VARBINARY column), MySQL converts the raw bytes
// as a string to integer (i.e., treats the bytes as text, resulting in 0 for non-numeric binary data).
// This mirrors MySQL's mixed-mode behavior: BINARY_col | INTEGER → integer arithmetic.
func toUint64ForBitOpAsBinaryString(v interface{}, bytes []byte, isBinary bool) uint64 {
	if isBinary {
		// Binary side in mixed mode: treat raw bytes as string for integer conversion
		rawStr := string(bytes)
		return toUint64ForBitOp(rawStr)
	}
	return toUint64ForBitOp(v)
}

func numericEqualForComparison(ls, rs string, origLeft, origRight interface{}) bool {
	if li, okL := parseStrictBigInt(ls); okL {
		if ri, okR := parseStrictBigInt(rs); okR {
			return li.Cmp(ri) == 0
		}
		// ls is an exact integer, rs is a decimal (has fractional part).
		// Use exact big.Rat comparison to avoid float64 precision loss near
		// int64/uint64 boundaries (e.g. 18446744073709551615 == 18446744073709551615.00001).
		if strings.ContainsRune(rs, '.') {
			rr := new(big.Rat)
			if _, ok := rr.SetString(rs); ok {
				lr := new(big.Rat).SetInt(li)
				return lr.Cmp(rr) == 0
			}
		}
	}
	if ri, okR := parseStrictBigInt(rs); okR {
		// rs is an exact integer, ls is a decimal (has fractional part).
		if strings.ContainsRune(ls, '.') {
			lr := new(big.Rat)
			if _, ok := lr.SetString(ls); ok {
				rr := new(big.Rat).SetInt(ri)
				return lr.Cmp(rr) == 0
			}
		}
	}

	fl, errL := strconv.ParseFloat(ls, 64)
	fr, errR := strconv.ParseFloat(rs, 64)
	if errL != nil || errR != nil {
		return false
	}
	if fl == fr {
		return true
	}
	if !shouldUseFloat32Equality(ls, rs, origLeft, origRight) {
		return false
	}
	// Avoid over-matching at larger magnitudes where float32 ULP becomes coarse.
	if math.Max(math.Abs(fl), math.Abs(fr)) >= 65536 {
		return false
	}
	if math.Abs(fl-fr) > 0.0005 {
		return false
	}
	// Don't use float32 comparison if one value is exactly zero and the other is not.
	// float32(1e-308) underflows to 0, causing false positives (e.g. 0 == '1E-308').
	if (fl == 0) != (fr == 0) {
		return false
	}
	return float32(fl) == float32(fr)
}

func shouldUseFloat32Equality(ls, rs string, origLeft, origRight interface{}) bool {
	looksApprox := func(s string) bool {
		return strings.ContainsAny(s, ".eE")
	}
	if !looksApprox(ls) && !looksApprox(rs) {
		return false
	}
	switch origLeft.(type) {
	case float32, float64, ScaledValue, DivisionResult:
		return true
	}
	switch origRight.(type) {
	case float32, float64, ScaledValue, DivisionResult:
		return true
	}
	_, leftIsString := origLeft.(string)
	_, rightIsString := origRight.(string)
	return leftIsString && rightIsString
}

func parseStrictBigInt(s string) (*big.Int, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, false
	}
	start := 0
	if s[0] == '+' || s[0] == '-' {
		if len(s) == 1 {
			return nil, false
		}
		start = 1
	}
	for i := start; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return nil, false
		}
	}
	n := new(big.Int)
	if _, ok := n.SetString(s, 10); !ok {
		return nil, false
	}
	return n, true
}

func toStrictBigInt(v interface{}) (*big.Int, bool) {
	switch n := v.(type) {
	case int:
		return big.NewInt(int64(n)), true
	case int8:
		return big.NewInt(int64(n)), true
	case int16:
		return big.NewInt(int64(n)), true
	case int32:
		return big.NewInt(int64(n)), true
	case int64:
		return big.NewInt(n), true
	case uint:
		z := new(big.Int)
		z.SetUint64(uint64(n))
		return z, true
	case uint8:
		z := new(big.Int)
		z.SetUint64(uint64(n))
		return z, true
	case uint16:
		z := new(big.Int)
		z.SetUint64(uint64(n))
		return z, true
	case uint32:
		z := new(big.Int)
		z.SetUint64(uint64(n))
		return z, true
	case uint64:
		z := new(big.Int)
		z.SetUint64(n)
		return z, true
	case string:
		return parseStrictBigInt(n)
	default:
		return nil, false
	}
}

// valuesEqual checks if two values are semantically equal for UPDATE change detection.
func valuesEqual(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	// Convert both to string representation for comparison
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func toFloat(v interface{}) float64 {
	switch n := v.(type) {
	case int64:
		return float64(n)
	case uint64:
		return float64(n)
	case float64:
		return n
	case SysVarDouble:
		return n.Value
	case ScaledValue:
		return n.Value
	case DivisionResult:
		return n.Value
	case AvgResult:
		return n.Value
	case HexBytes:
		// Interpret hex digits as big-endian unsigned integer.
		decoded, err := hex.DecodeString(string(n))
		if err != nil || len(decoded) == 0 {
			return 0
		}
		var val uint64
		for _, b := range decoded {
			val = val<<8 | uint64(b)
		}
		return float64(val)
	case string:
		s := strings.TrimSpace(n)
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		} else if errors.Is(err, strconv.ErrRange) {
			if strings.HasPrefix(s, "-") {
				return math.Inf(-1)
			}
			return math.Inf(1)
		}
		// MySQL numeric context for TIME string: HH:MM:SS[.frac] -> HHMMSS[.frac]
		if strings.Count(s, ":") == 2 {
			sign := 1.0
			if strings.HasPrefix(s, "-") {
				sign = -1.0
				s = strings.TrimPrefix(s, "-")
			}
			main := s
			fracPart := ""
			if dot := strings.IndexByte(s, '.'); dot >= 0 {
				main = s[:dot]
				fracPart = s[dot+1:]
			}
			tparts := strings.Split(main, ":")
			if len(tparts) == 3 {
				h, eh := strconv.Atoi(tparts[0])
				m, em := strconv.Atoi(tparts[1])
				sec, es := strconv.Atoi(tparts[2])
				if eh == nil && em == nil && es == nil {
					base := float64(h*10000 + m*100 + sec)
					if fracPart != "" {
						if fracDigits, err := strconv.ParseFloat("0."+fracPart, 64); err == nil {
							base += fracDigits
						}
					}
					return sign * base
				}
			}
		}
		// MySQL numeric context for DATE/DATETIME strings.
		if len(s) >= 10 && s[4] == '-' && s[7] == '-' {
			y, ey := strconv.Atoi(s[0:4])
			mo, em := strconv.Atoi(s[5:7])
			d, ed := strconv.Atoi(s[8:10])
			if ey == nil && em == nil && ed == nil {
				base := float64(y*10000 + mo*100 + d)
				if len(s) >= 19 && s[10] == ' ' && s[13] == ':' && s[16] == ':' {
					h, eh := strconv.Atoi(s[11:13])
					mi, emi := strconv.Atoi(s[14:16])
					se, es := strconv.Atoi(s[17:19])
					if eh == nil && emi == nil && es == nil {
						base = float64((y*10000+mo*100+d)*1000000 + h*10000 + mi*100 + se)
					}
				}
				return base
			}
		}
		// MySQL numeric context for day names: Monday=0, Tuesday=1, ..., Sunday=6
		switch s {
		case "Monday":
			return 0
		case "Tuesday":
			return 1
		case "Wednesday":
			return 2
		case "Thursday":
			return 3
		case "Friday":
			return 4
		case "Saturday":
			return 5
		case "Sunday":
			return 6
		}
		if f, ok := parseNumericPrefixMySQL(s); ok {
			return f
		}
		// Binary string to integer (big-endian) for hex literal x'...' in numeric context.
		// MySQL interprets binary strings as big-endian unsigned integers in arithmetic.
		if len(n) > 0 && len(n) <= 8 {
			isBinary := false
			for i := 0; i < len(n); i++ {
				if n[i] < 0x20 || n[i] > 0x7e {
					isBinary = true
					break
				}
			}
			if isBinary {
				var val uint64
				for i := 0; i < len(n); i++ {
					val = val<<8 | uint64(n[i])
				}
				return float64(val)
			}
		}
		return 0
	case bool:
		if n {
			return 1
		}
		return 0
	}
	return 0
}
