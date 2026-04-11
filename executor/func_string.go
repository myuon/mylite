package executor

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/myuon/mylite/storage"
	"vitess.io/vitess/go/vt/sqlparser"
)

// evalStringFunc dispatches string-related functions.
// When row is non-nil, expressions are evaluated with row context.
// Returns (result, handled, error). If handled is false, the caller should try other dispatchers.
func evalStringFunc(e *Executor, name string, v *sqlparser.FuncExpr, row *storage.Row) (interface{}, bool, error) {
	switch name {
	case "concat":
		var sb strings.Builder
		for _, argExpr := range v.Exprs {
			val, err := e.evalExprMaybeRow(argExpr, row)
			if err != nil {
				return nil, true, err
			}
			if val == nil {
				return nil, true, nil // CONCAT with NULL returns NULL
			}
			sb.WriteString(toString(val))
		}
		result := sb.String()
		// Check if result exceeds max_allowed_packet (MySQL: error 1301)
		maxPkt := int64(67108864)
		if sv, ok := e.getSysVar("max_allowed_packet"); ok {
			if n, err2 := strconv.ParseInt(sv, 10, 64); err2 == nil {
				maxPkt = n
			}
		}
		if int64(len(result)) > maxPkt {
			msg := fmt.Sprintf("Result of concat() was larger than max_allowed_packet (%d) - truncated", maxPkt)
			if e.inUpdateSetContext {
				return nil, true, mysqlError(1301, "HY000", msg)
			}
			// SELECT/INSERT context: return NULL with warning
			e.addWarning("Warning", 1301, msg)
			return nil, true, nil
		}
		return result, true, nil
	case "md5":
		val, isNull, err := e.evalArg1(v.Exprs, "MD5", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		sum := md5.Sum([]byte(toString(val)))
		return hex.EncodeToString(sum[:]), true, nil
	case "concat_ws":
		if len(v.Exprs) < 1 {
			return nil, true, nil
		}
		sepVal, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if sepVal == nil {
			return nil, true, nil
		}
		sep := toString(sepVal)
		var parts []string
		for _, argExpr := range v.Exprs[1:] {
			val, err := e.evalExprMaybeRow(argExpr, row)
			if err != nil {
				return nil, true, err
			}
			if val == nil {
				continue // CONCAT_WS skips NULLs
			}
			parts = append(parts, toString(val))
		}
		return strings.Join(parts, sep), true, nil
	case "upper", "ucase":
		val, isNull, err := e.evalArg1(v.Exprs, "UPPER", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		s := toString(val)
		if strings.ContainsRune(s, '\x00') || e.isBinaryExpr(v.Exprs[0]) {
			return s, true, nil
		}
		return strings.ToUpper(s), true, nil
	case "lower", "lcase":
		val, isNull, err := e.evalArg1(v.Exprs, "LOWER", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		s := toString(val)
		if strings.ContainsRune(s, '\x00') || e.isBinaryExpr(v.Exprs[0]) {
			return s, true, nil
		}
		return strings.ToLower(s), true, nil
	case "length", "octet_length":
		val, isNull, err := e.evalArg1(v.Exprs, "LENGTH", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		s := toString(val)
		if colName, ok := v.Exprs[0].(*sqlparser.ColName); ok {
			cs := e.getColumnCharset(colName)
			if byteLen, err2 := charsetByteLength(s, cs); err2 == nil {
				return byteLen, true, nil
			}
		}
		return int64(len(s)), true, nil
	case "char_length", "character_length":
		val, isNull, err := e.evalArg1(v.Exprs, "CHAR_LENGTH", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		s := toString(val)
		cs := ""
		if colName, ok := v.Exprs[0].(*sqlparser.ColName); ok {
			cs = e.getColumnCharset(colName)
		}
		return int64(mysqlCharLenCharset(s, cs)), true, nil
	case "ascii", "ord":
		val, isNull, err := e.evalArg1(v.Exprs, "ASCII", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		s := toString(val)
		if len(s) == 0 {
			return int64(0), true, nil
		}
		return int64(s[0]), true, nil
	case "load_file":
		val, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		filePath := toString(val)
		if !filepath.IsAbs(filePath) && len(e.SearchPaths) > 0 {
			for _, sp := range e.SearchPaths {
				candidate := filepath.Join(sp, filePath)
				if _, statErr := os.Stat(candidate); statErr == nil {
					filePath = candidate
					break
				}
			}
		}
		data, readErr := os.ReadFile(filePath)
		if readErr != nil {
			return nil, true, nil // MySQL returns NULL if file cannot be read
		}
		return string(data), true, nil
	case "char":
		var result []byte
		for _, argExpr := range v.Exprs {
			val, err := e.evalExprMaybeRow(argExpr, row)
			if err != nil {
				return nil, true, err
			}
			if val == nil {
				continue
			}
			n := uint64(toInt64(val))
			// MySQL CHAR() outputs the minimum number of bytes needed for the value
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
		return string(result), true, nil
	case "substring", "substr", "mid":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("SUBSTRING requires at least 2 arguments")
		}
		strVal, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if strVal == nil {
			return nil, true, nil
		}
		s := []rune(toString(strVal))
		posVal, err := e.evalExprMaybeRow(v.Exprs[1], row)
		if err != nil {
			return nil, true, err
		}
		pos := int(toInt64(posVal))
		if pos == 0 {
			return "", true, nil
		}
		if pos > 0 {
			pos--
		} else if pos < 0 {
			pos = len(s) + pos
		}
		if pos < 0 {
			pos = 0
		}
		if pos >= len(s) {
			return "", true, nil
		}
		if len(v.Exprs) >= 3 {
			lenVal, err := e.evalExprMaybeRow(v.Exprs[2], row)
			if err != nil {
				return nil, true, err
			}
			length := int(toInt64(lenVal))
			if length <= 0 {
				return "", true, nil
			}
			end := pos + length
			if end > len(s) {
				end = len(s)
			}
			return string(s[pos:end]), true, nil
		}
		return string(s[pos:]), true, nil
	case "trim":
		val, isNull, err := e.evalArg1(v.Exprs, "TRIM", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		return strings.TrimSpace(toString(val)), true, nil
	case "ltrim":
		val, isNull, err := e.evalArg1(v.Exprs, "LTRIM", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		return strings.TrimLeft(toString(val), " \t\n\r"), true, nil
	case "rtrim":
		val, isNull, err := e.evalArg1(v.Exprs, "RTRIM", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		return strings.TrimRight(toString(val), " \t\n\r"), true, nil
	case "replace":
		if len(v.Exprs) < 3 {
			return nil, true, fmt.Errorf("REPLACE requires 3 arguments")
		}
		strVal, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		fromVal, err := e.evalExprMaybeRow(v.Exprs[1], row)
		if err != nil {
			return nil, true, err
		}
		toVal, err := e.evalExprMaybeRow(v.Exprs[2], row)
		if err != nil {
			return nil, true, err
		}
		if strVal == nil || fromVal == nil || toVal == nil {
			return nil, true, nil
		}
		fromStr := toString(fromVal)
		if fromStr == "" {
			// MySQL REPLACE returns original string when search string is empty
			return toString(strVal), true, nil
		}
		return strings.ReplaceAll(toString(strVal), fromStr, toString(toVal)), true, nil
	case "left":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("LEFT requires 2 arguments")
		}
		strVal, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if strVal == nil {
			return nil, true, nil
		}
		lenVal, err := e.evalExprMaybeRow(v.Exprs[1], row)
		if err != nil {
			var ov *intOverflowError
			if errors.As(err, &ov) {
				return nil, true, nil
			}
			return nil, true, err
		}
		if lenVal == nil {
			return nil, true, nil
		}
		s := []rune(toString(strVal))
		n := int(toInt64(lenVal))
		if n <= 0 {
			return "", true, nil
		}
		if n > len(s) {
			n = len(s)
		}
		return string(s[:n]), true, nil
	case "right":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("RIGHT requires 2 arguments")
		}
		strVal, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if strVal == nil {
			return nil, true, nil
		}
		lenVal, err := e.evalExprMaybeRow(v.Exprs[1], row)
		if err != nil {
			var ov *intOverflowError
			if errors.As(err, &ov) {
				return nil, true, nil
			}
			return nil, true, err
		}
		if lenVal == nil {
			return nil, true, nil
		}
		s := []rune(toString(strVal))
		n := int(toInt64(lenVal))
		if n <= 0 {
			return "", true, nil
		}
		if n > len(s) {
			n = len(s)
		}
		return string(s[len(s)-n:]), true, nil
	case "hex":
		val, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		switch tv := val.(type) {
		case int64:
			// Use unsigned format to avoid negative hex output for large values
			return strings.ToUpper(fmt.Sprintf("%X", uint64(tv))), true, nil
		case float64:
			return strings.ToUpper(fmt.Sprintf("%X", uint64(int64(tv)))), true, nil
		default:
			s := toString(val)
			return strings.ToUpper(hex.EncodeToString([]byte(s))), true, nil
		}
	case "unhex":
		val, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		hexStr := toString(val)
		// MySQL pads odd-length hex strings with a leading zero
		if len(hexStr)%2 != 0 {
			hexStr = "0" + hexStr
		}
		decoded, err := hex.DecodeString(hexStr)
		if err != nil {
			return nil, true, nil
		}
		return string(decoded), true, nil
	case "strcmp":
		v0, v1, hasNull, err := e.evalArgs2(v.Exprs, "STRCMP", row)
		if err != nil {
			return nil, true, err
		}
		if hasNull {
			return nil, true, nil
		}
		// PAD SPACE: compare with space-padding semantics (MySQL non-binary collation).
		// When one string is shorter, it is virtually padded with spaces for comparison.
		// This means 'a\0' < 'a' because 'a' pads to 'a ' and '\0' (0x00) < ' ' (0x20).
		s0 := strings.ToLower(toString(v0))
		s1 := strings.ToLower(toString(v1))
		cmpLen := len(s0)
		if len(s1) > cmpLen {
			cmpLen = len(s1)
		}
		for len(s0) < cmpLen {
			s0 += " "
		}
		for len(s1) < cmpLen {
			s1 += " "
		}
		if s0 < s1 {
			return int64(-1), true, nil
		} else if s0 > s1 {
			return int64(1), true, nil
		}
		return int64(0), true, nil
	case "reverse":
		val, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		runes := []rune(toString(val))
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return string(runes), true, nil
	case "oct":
		val, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		n := toInt64(val)
		if n < 0 {
			return fmt.Sprintf("%o", uint64(n)), true, nil
		}
		return fmt.Sprintf("%o", n), true, nil
	case "repeat":
		if len(v.Exprs) < 2 {
			return nil, true, nil
		}
		sVal, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		nVal, err := e.evalExprMaybeRow(v.Exprs[1], row)
		if err != nil {
			return nil, true, err
		}
		count := int(math.Round(toFloat(nVal)))
		if count <= 0 || sVal == nil {
			return "", true, nil
		}
		// For HexBytes (from x'...' literals), decode to raw bytes
		// so that REPEAT operates on the actual byte sequence.
		if hb, ok := sVal.(HexBytes); ok {
			decoded, err := hex.DecodeString(string(hb))
			if err != nil {
				return nil, true, nil
			}
			str := string(decoded)
			if int64(count)*int64(len(str)) > 67108864 {
				return nil, true, nil
			}
			return strings.Repeat(str, count), true, nil
		}
		// For 0x... hex number literals, the value is int64/uint64 but in
		// string context MySQL treats them as binary byte sequences.
		if lit, ok := v.Exprs[0].(*sqlparser.Literal); ok && lit.Type == sqlparser.HexNum {
			s := lit.Val
			if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
				s = s[2:]
			}
			if len(s)%2 != 0 {
				s = "0" + s
			}
			decoded, err := hex.DecodeString(s)
			if err != nil {
				return nil, true, nil
			}
			str := string(decoded)
			if int64(count)*int64(len(str)) > 67108864 {
				return nil, true, nil
			}
			return strings.Repeat(str, count), true, nil
		}
		str := toString(sVal)
		maxAllowed := int64(67108864) // default 64 MB
		if mapStr, ok := e.getSysVar("max_allowed_packet"); ok && mapStr != "" {
			if parsed, err := strconv.ParseInt(mapStr, 10, 64); err == nil && parsed > maxAllowed {
				maxAllowed = parsed
			}
		}
		if int64(count)*int64(len(str)) > maxAllowed {
			return nil, true, nil
		}
		return strings.Repeat(str, count), true, nil
	case "instr":
		strVal, subVal, hasNull, err := e.evalArgs2(v.Exprs, "INSTR", row)
		if err != nil {
			return nil, true, err
		}
		if hasNull {
			return nil, true, nil
		}
		// INSTR is case-insensitive by default (uses connection collation)
		s := strings.ToLower(toString(strVal))
		sub := strings.ToLower(toString(subVal))
		if len(sub) == 0 {
			return int64(1), true, nil
		}
		sRunes := []rune(s)
		subRunes := []rune(sub)
		for i := 0; i <= len(sRunes)-len(subRunes); i++ {
			match := true
			for j := 0; j < len(subRunes); j++ {
				if sRunes[i+j] != subRunes[j] {
					match = false
					break
				}
			}
			if match {
				// Return position in original (non-lowercased) string
				origRunes := []rune(toString(strVal))
				_ = origRunes
				return int64(i + 1), true, nil
			}
		}
		return int64(0), true, nil
	case "lpad":
		if len(v.Exprs) < 3 {
			return nil, true, fmt.Errorf("LPAD requires 3 arguments")
		}
		strVal, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		lenVal, err := e.evalExprMaybeRow(v.Exprs[1], row)
		if err != nil {
			var ov *intOverflowError
			if errors.As(err, &ov) {
				if lit, ok := v.Exprs[1].(*sqlparser.Literal); ok && lit.Type == sqlparser.IntVal {
					if bi, ok := new(big.Int).SetString(lit.Val, 10); ok && bi.Sign() >= 0 && bi.Cmp(big.NewInt(67108864)) > 0 {
						return nil, true, nil
					}
				}
				return nil, true, nil
			}
			return nil, true, err
		}
		padVal, err := e.evalExprMaybeRow(v.Exprs[2], row)
		if err != nil {
			return nil, true, err
		}
		if strVal == nil || lenVal == nil || padVal == nil {
			return nil, true, nil
		}
		s := []rune(toString(strVal))
		targetLen64 := toInt64(lenVal)
		if targetLen64 < 0 {
			return nil, true, nil
		}
		if targetLen64 > 67108864 {
			return nil, true, nil
		}
		targetLen := int(targetLen64)
		padStr := []rune(toString(padVal))
		if targetLen <= len(s) {
			return string(s[:targetLen]), true, nil
		}
		if len(padStr) == 0 {
			return "", true, nil
		}
		needed := targetLen - len(s)
		var pad []rune
		for len(pad) < needed {
			pad = append(pad, padStr...)
		}
		pad = pad[:needed]
		return string(append(pad, s...)), true, nil
	case "rpad":
		if len(v.Exprs) < 3 {
			return nil, true, fmt.Errorf("RPAD requires 3 arguments")
		}
		strVal, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		lenVal, err := e.evalExprMaybeRow(v.Exprs[1], row)
		if err != nil {
			var ov *intOverflowError
			if errors.As(err, &ov) {
				if lit, ok := v.Exprs[1].(*sqlparser.Literal); ok && lit.Type == sqlparser.IntVal {
					if bi, ok := new(big.Int).SetString(lit.Val, 10); ok && bi.Sign() >= 0 && bi.Cmp(big.NewInt(67108864)) > 0 {
						return nil, true, nil
					}
				}
				return nil, true, nil
			}
			return nil, true, err
		}
		padVal, err := e.evalExprMaybeRow(v.Exprs[2], row)
		if err != nil {
			return nil, true, err
		}
		if strVal == nil || lenVal == nil || padVal == nil {
			return nil, true, nil
		}
		s := []rune(toString(strVal))
		targetLen64 := toInt64(lenVal)
		if targetLen64 < 0 {
			return nil, true, nil
		}
		if targetLen64 > 67108864 {
			return nil, true, nil
		}
		targetLen := int(targetLen64)
		padStr := []rune(toString(padVal))
		if targetLen <= len(s) {
			return string(s[:targetLen]), true, nil
		}
		if len(padStr) == 0 {
			return "", true, nil
		}
		needed := targetLen - len(s)
		var pad []rune
		for len(pad) < needed {
			pad = append(pad, padStr...)
		}
		pad = pad[:needed]
		return string(append(s, pad...)), true, nil
	case "space":
		spVal, isNull, err := e.evalArg1(v.Exprs, "SPACE", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		spN := int(toInt64(spVal))
		if spN < 0 {
			return nil, true, nil
		}
		if spN > 1048576 {
			spN = 1048576
		}
		return strings.Repeat(" ", spN), true, nil
	case "substring_index":
		if len(v.Exprs) < 3 {
			return nil, true, fmt.Errorf("SUBSTRING_INDEX requires 3 arguments")
		}
		siStr, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		siDelim, err := e.evalExprMaybeRow(v.Exprs[1], row)
		if err != nil {
			return nil, true, err
		}
		siCnt, err := e.evalExprMaybeRow(v.Exprs[2], row)
		if err != nil {
			return nil, true, err
		}
		if siStr == nil || siDelim == nil || siCnt == nil {
			return nil, true, nil
		}
		siS := toString(siStr)
		siD := toString(siDelim)
		siN64 := toInt64(siCnt)
		if siD == "" {
			// MySQL returns empty string when delimiter is empty
			return "", true, nil
		}
		if siN64 >= 0 {
			// Positive count: find N-th occurrence of delimiter from left
			count := int(siN64)
			if count == 0 {
				return "", true, nil
			}
			pos := 0
			for i := 0; i < count; i++ {
				idx := strings.Index(siS[pos:], siD)
				if idx < 0 {
					// Fewer than count occurrences: return the whole string
					return siS, true, nil
				}
				pos += idx + len(siD)
			}
			// Return everything before the N-th delimiter
			return siS[:pos-len(siD)], true, nil
		}
		// Negative count: scan from the right
		// Find the |N|-th occurrence from the right by reverse scanning
		siAbs := -int(siN64)
		if siN64 == -1<<63 {
			siAbs = len(siS) // overflow guard
		}
		pos := len(siS)
		for i := 0; i < siAbs; i++ {
			// Search for delimiter ending at pos
			idx := strings.LastIndex(siS[:pos], siD)
			if idx < 0 {
				return siS, true, nil
			}
			pos = idx
		}
		return siS[pos+len(siD):], true, nil
	case "soundex":
		val, isNull, err := e.evalArg1(v.Exprs, "SOUNDEX", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		return soundex(toString(val)), true, nil
	case "bit_length":
		val, isNull, err := e.evalArg1(v.Exprs, "BIT_LENGTH", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		return int64(len(toString(val))) * 8, true, nil
	case "field":
		if len(v.Exprs) < 2 {
			return int64(0), true, nil
		}
		fieldTarget, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if fieldTarget == nil {
			return int64(0), true, nil
		}
		fieldTS := toString(fieldTarget)
		for fi := 1; fi < len(v.Exprs); fi++ {
			fieldVal, err := e.evalExprMaybeRow(v.Exprs[fi], row)
			if err != nil {
				return nil, true, err
			}
			if fieldVal != nil && strings.EqualFold(toString(fieldVal), fieldTS) {
				return int64(fi), true, nil
			}
		}
		return int64(0), true, nil
	case "find_in_set":
		fisNeedle, fisHaystack, hasNull, err := e.evalArgs2(v.Exprs, "FIND_IN_SET", row)
		if err != nil {
			return nil, true, err
		}
		if hasNull {
			return nil, true, nil
		}
		fisNS := toString(fisNeedle)
		fisHS := toString(fisHaystack)
		if fisHS == "" {
			// Empty haystack: no elements to search
			return int64(0), true, nil
		}
		for fi, part := range strings.Split(fisHS, ",") {
			if part == fisNS {
				return int64(fi + 1), true, nil
			}
		}
		return int64(0), true, nil
	case "elt":
		if len(v.Exprs) < 2 {
			return nil, true, nil
		}
		eltIdx, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if eltIdx == nil {
			return nil, true, nil
		}
		eltN := int(toInt64(eltIdx))
		if eltN < 1 || eltN >= len(v.Exprs) {
			return nil, true, nil
		}
		r, err := e.evalExprMaybeRow(v.Exprs[eltN], row)
		return r, true, err
	case "make_set":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("MAKE_SET requires at least 2 arguments")
		}
		msB, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if msB == nil {
			return nil, true, nil
		}
		msBits := uint64(toInt64(msB))
		var msParts []string
		for msi := 1; msi < len(v.Exprs); msi++ {
			if msBits&(1<<uint(msi-1)) != 0 {
				msVal, err := e.evalExprMaybeRow(v.Exprs[msi], row)
				if err != nil {
					return nil, true, err
				}
				if msVal != nil {
					msParts = append(msParts, toString(msVal))
				}
			}
		}
		return strings.Join(msParts, ","), true, nil
	case "export_set":
		if len(v.Exprs) < 3 {
			return nil, true, fmt.Errorf("EXPORT_SET requires at least 3 arguments")
		}
		esBits, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		esOn, err := e.evalExprMaybeRow(v.Exprs[1], row)
		if err != nil {
			return nil, true, err
		}
		esOff, err := e.evalExprMaybeRow(v.Exprs[2], row)
		if err != nil {
			return nil, true, err
		}
		if esBits == nil || esOn == nil || esOff == nil {
			return nil, true, nil
		}
		esSep := ","
		esCount := 64
		if len(v.Exprs) >= 4 {
			esSepVal, err := e.evalExprMaybeRow(v.Exprs[3], row)
			if err != nil {
				return nil, true, err
			}
			if esSepVal != nil {
				esSep = toString(esSepVal)
			}
		}
		if len(v.Exprs) >= 5 {
			esCntVal, err := e.evalExprMaybeRow(v.Exprs[4], row)
			if err != nil {
				return nil, true, err
			}
			if esCntVal != nil {
				esCount = int(toInt64(esCntVal))
			}
		}
		esB := uint64(toInt64(esBits))
		esOnStr := toString(esOn)
		esOffStr := toString(esOff)
		var esParts []string
		for esi := 0; esi < esCount; esi++ {
			if esB&(1<<uint(esi)) != 0 {
				esParts = append(esParts, esOnStr)
			} else {
				esParts = append(esParts, esOffStr)
			}
		}
		return strings.Join(esParts, esSep), true, nil
	case "quote":
		qVal, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return "NULL", true, nil
		}
		qSrc := []byte(toString(qVal))
		var qBuf strings.Builder
		qBuf.WriteByte('\'')
		for _, b := range qSrc {
			switch b {
			case '\\':
				qBuf.WriteString("\\\\")
			case '\'':
				qBuf.WriteString("\\'")
			case 0x00:
				qBuf.WriteString("\\0")
			case 0x1A: // ctrl-Z
				qBuf.WriteString("\\Z")
			case '\n':
				qBuf.WriteString("\\n")
			case '\r':
				qBuf.WriteString("\\r")
			default:
				qBuf.WriteByte(b)
			}
		}
		qBuf.WriteByte('\'')
		return qBuf.String(), true, nil
	case "weight_string":
		wsVal, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		return toString(wsVal), true, nil
	case "format":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("FORMAT requires 2 arguments")
		}
		fmtVal, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		fmtDec, err := e.evalExprMaybeRow(v.Exprs[1], row)
		if err != nil {
			return nil, true, err
		}
		if fmtVal == nil {
			return nil, true, nil
		}
		fmtD := int(toInt64(fmtDec))
		if fmtD < 0 {
			fmtD = 0
		}
		fmtF := toFloat(fmtVal)
		fmtFormatted := fmt.Sprintf("%.*f", fmtD, fmtF)
		fmtParts := strings.SplitN(fmtFormatted, ".", 2)
		fmtIntPart := fmtParts[0]
		fmtNeg := false
		if strings.HasPrefix(fmtIntPart, "-") {
			fmtNeg = true
			fmtIntPart = fmtIntPart[1:]
		}
		var fmtBuf []byte
		for fi, ch := range fmtIntPart {
			if fi > 0 && (len(fmtIntPart)-fi)%3 == 0 {
				fmtBuf = append(fmtBuf, ',')
			}
			fmtBuf = append(fmtBuf, byte(ch))
		}
		fmtS := string(fmtBuf)
		if fmtNeg {
			fmtS = "-" + fmtS
		}
		if len(fmtParts) > 1 {
			fmtS = fmtS + "." + fmtParts[1]
		}
		return fmtS, true, nil
	default:
		return nil, false, nil
	}
}
