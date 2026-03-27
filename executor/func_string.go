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
	"strings"

	"github.com/myuon/mylite/storage"
	"vitess.io/vitess/go/vt/sqlparser"
)

// evalStringFunc dispatches string-related functions from evalFuncExpr.
// Returns (result, handled, error). If handled is false, the caller should try other dispatchers.
func evalStringFunc(e *Executor, name string, v *sqlparser.FuncExpr) (interface{}, bool, error) {
	switch name {
	case "concat":
		var sb strings.Builder
		for _, argExpr := range v.Exprs {
			val, err := e.evalExpr(argExpr)
			if err != nil {
				return nil, true, err
			}
			if val == nil {
				return nil, true, nil // CONCAT with NULL returns NULL
			}
			sb.WriteString(toString(val))
		}
		return sb.String(), true, nil
	case "md5":
		val, isNull, err := e.evalArg1(v.Exprs, "MD5")
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
		sepVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if sepVal == nil {
			return nil, true, nil
		}
		sep := toString(sepVal)
		var parts []string
		for _, argExpr := range v.Exprs[1:] {
			val, err := e.evalExpr(argExpr)
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
		val, isNull, err := e.evalArg1(v.Exprs, "UPPER")
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		s := toString(val)
		if strings.ContainsRune(s, '\x00') {
			return s, true, nil
		}
		if e.isBinaryExpr(v.Exprs[0]) {
			return s, true, nil
		}
		return strings.ToUpper(s), true, nil
	case "lower", "lcase":
		val, isNull, err := e.evalArg1(v.Exprs, "LOWER")
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		if e.isBinaryExpr(v.Exprs[0]) {
			return toString(val), true, nil
		}
		return strings.ToLower(toString(val)), true, nil
	case "length", "octet_length":
		val, isNull, err := e.evalArg1(v.Exprs, "LENGTH")
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		s := toString(val)
		if colName, ok := v.Exprs[0].(*sqlparser.ColName); ok {
			cs := e.getColumnCharset(colName)
			if byteLen, err := charsetByteLength(s, cs); err == nil {
				return byteLen, true, nil
			}
		}
		return int64(len(s)), true, nil
	case "char_length", "character_length":
		val, isNull, err := e.evalArg1(v.Exprs, "CHAR_LENGTH")
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		return int64(mysqlCharLen(toString(val))), true, nil
	case "ascii", "ord":
		val, isNull, err := e.evalArg1(v.Exprs, "ASCII")
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
		val, isNull, err := e.evalArg1Quiet(v.Exprs)
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
				if _, err := os.Stat(candidate); err == nil {
					filePath = candidate
					break
				}
			}
		}
		data, err := os.ReadFile(filePath)
		if err != nil {
			return nil, true, nil // MySQL returns NULL if file cannot be read
		}
		return string(data), true, nil
	case "char":
		var sb strings.Builder
		for _, argExpr := range v.Exprs {
			val, err := e.evalExpr(argExpr)
			if err != nil {
				return nil, true, err
			}
			if val == nil {
				continue
			}
			n := toInt64(val)
			if n >= 0 && n <= 255 {
				sb.WriteByte(byte(n))
			}
		}
		return sb.String(), true, nil
	case "substring", "substr", "mid":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("SUBSTRING requires at least 2 arguments")
		}
		strVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if strVal == nil {
			return nil, true, nil
		}
		s := []rune(toString(strVal))
		posVal, err := e.evalExpr(v.Exprs[1])
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
			lenVal, err := e.evalExpr(v.Exprs[2])
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
		val, isNull, err := e.evalArg1(v.Exprs, "TRIM")
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		return strings.TrimSpace(toString(val)), true, nil
	case "ltrim":
		val, isNull, err := e.evalArg1(v.Exprs, "LTRIM")
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		return strings.TrimLeft(toString(val), " \t\n\r"), true, nil
	case "rtrim":
		val, isNull, err := e.evalArg1(v.Exprs, "RTRIM")
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
		strVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		fromVal, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, true, err
		}
		toVal, err := e.evalExpr(v.Exprs[2])
		if err != nil {
			return nil, true, err
		}
		if strVal == nil {
			return nil, true, nil
		}
		return strings.ReplaceAll(toString(strVal), toString(fromVal), toString(toVal)), true, nil
	case "left":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("LEFT requires 2 arguments")
		}
		strVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if strVal == nil {
			return nil, true, nil
		}
		lenVal, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			var ov *intOverflowError
			if errors.As(err, &ov) {
				return nil, true, nil
			}
			return nil, true, err
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
		strVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if strVal == nil {
			return nil, true, nil
		}
		lenVal, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			var ov *intOverflowError
			if errors.As(err, &ov) {
				return nil, true, nil
			}
			return nil, true, err
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
		val, isNull, err := e.evalArg1Quiet(v.Exprs)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		switch tv := val.(type) {
		case int64:
			return strings.ToUpper(fmt.Sprintf("%X", tv)), true, nil
		case float64:
			return strings.ToUpper(fmt.Sprintf("%X", int64(tv))), true, nil
		default:
			s := toString(val)
			var hexBuf strings.Builder
			for _, b := range []byte(s) {
				fmt.Fprintf(&hexBuf, "%02X", b)
			}
			return hexBuf.String(), true, nil
		}
	case "unhex":
		val, isNull, err := e.evalArg1Quiet(v.Exprs)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		decoded, err := hex.DecodeString(toString(val))
		if err != nil {
			return nil, true, nil
		}
		return string(decoded), true, nil
	case "strcmp":
		v0, v1, hasNull, err := e.evalArgs2(v.Exprs, "STRCMP")
		if err != nil {
			return nil, true, err
		}
		if hasNull {
			return nil, true, nil
		}
		s0 := strings.ToLower(toString(v0))
		s1 := strings.ToLower(toString(v1))
		if s0 < s1 {
			return int64(-1), true, nil
		} else if s0 > s1 {
			return int64(1), true, nil
		}
		return int64(0), true, nil
	case "reverse":
		val, isNull, err := e.evalArg1Quiet(v.Exprs)
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
		val, isNull, err := e.evalArg1Quiet(v.Exprs)
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
		s, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		n, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, true, err
		}
		count := int(math.Round(toFloat(n)))
		if count <= 0 || s == nil {
			return "", true, nil
		}
		str := toString(s)
		if int64(count)*int64(len(str)) > 67108864 {
			return nil, true, nil
		}
		return strings.Repeat(str, count), true, nil
	case "instr":
		strVal, subVal, hasNull, err := e.evalArgs2(v.Exprs, "INSTR")
		if err != nil {
			return nil, true, err
		}
		if hasNull {
			return nil, true, nil
		}
		s := []rune(toString(strVal))
		sub := []rune(toString(subVal))
		if len(sub) == 0 {
			return int64(1), true, nil
		}
		for i := 0; i <= len(s)-len(sub); i++ {
			match := true
			for j := 0; j < len(sub); j++ {
				if s[i+j] != sub[j] {
					match = false
					break
				}
			}
			if match {
				return int64(i + 1), true, nil
			}
		}
		return int64(0), true, nil
	case "lpad":
		if len(v.Exprs) < 3 {
			return nil, true, fmt.Errorf("LPAD requires 3 arguments")
		}
		strVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		lenVal, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			var ov *intOverflowError
			if errors.As(err, &ov) {
				if lit, ok := v.Exprs[1].(*sqlparser.Literal); ok && lit.Type == sqlparser.IntVal {
					if bi, ok := new(big.Int).SetString(lit.Val, 10); ok && bi.Sign() >= 0 && bi.Cmp(big.NewInt(67108864)) > 0 {
						return nil, true, nil
					}
				}
			}
			return nil, true, err
		}
		padVal, err := e.evalExpr(v.Exprs[2])
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
		strVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		lenVal, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			var ov *intOverflowError
			if errors.As(err, &ov) {
				if lit, ok := v.Exprs[1].(*sqlparser.Literal); ok && lit.Type == sqlparser.IntVal {
					if bi, ok := new(big.Int).SetString(lit.Val, 10); ok && bi.Sign() >= 0 && bi.Cmp(big.NewInt(67108864)) > 0 {
						return nil, true, nil
					}
				}
			}
			return nil, true, err
		}
		padVal, err := e.evalExpr(v.Exprs[2])
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
		spVal, isNull, err := e.evalArg1(v.Exprs, "SPACE")
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
		siStr, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		siDelim, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, true, err
		}
		siCnt, err := e.evalExpr(v.Exprs[2])
		if err != nil {
			return nil, true, err
		}
		if siStr == nil || siDelim == nil || siCnt == nil {
			return nil, true, nil
		}
		siS := toString(siStr)
		siD := toString(siDelim)
		siN := int(toInt64(siCnt))
		siParts := strings.Split(siS, siD)
		if siN >= 0 {
			if siN >= len(siParts) {
				return siS, true, nil
			}
			return strings.Join(siParts[:siN], siD), true, nil
		}
		siN = -siN
		if siN >= len(siParts) {
			return siS, true, nil
		}
		return strings.Join(siParts[len(siParts)-siN:], siD), true, nil
	case "soundex":
		val, isNull, err := e.evalArg1(v.Exprs, "SOUNDEX")
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		return soundex(toString(val)), true, nil
	case "bit_length":
		val, isNull, err := e.evalArg1(v.Exprs, "BIT_LENGTH")
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
		fieldTarget, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if fieldTarget == nil {
			return int64(0), true, nil
		}
		fieldTS := toString(fieldTarget)
		for fi := 1; fi < len(v.Exprs); fi++ {
			fieldVal, err := e.evalExpr(v.Exprs[fi])
			if err != nil {
				return nil, true, err
			}
			if fieldVal != nil && strings.EqualFold(toString(fieldVal), fieldTS) {
				return int64(fi), true, nil
			}
		}
		return int64(0), true, nil
	case "find_in_set":
		fisNeedle, fisHaystack, hasNull, err := e.evalArgs2(v.Exprs, "FIND_IN_SET")
		if err != nil {
			return nil, true, err
		}
		if hasNull {
			return nil, true, nil
		}
		fisNS := toString(fisNeedle)
		fisHS := toString(fisHaystack)
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
		eltIdx, err := e.evalExpr(v.Exprs[0])
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
		r, err := e.evalExpr(v.Exprs[eltN])
		return r, true, err
	case "make_set":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("MAKE_SET requires at least 2 arguments")
		}
		msB, err := e.evalExpr(v.Exprs[0])
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
				msVal, err := e.evalExpr(v.Exprs[msi])
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
		esBits, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		esOn, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, true, err
		}
		esOff, err := e.evalExpr(v.Exprs[2])
		if err != nil {
			return nil, true, err
		}
		if esBits == nil || esOn == nil || esOff == nil {
			return nil, true, nil
		}
		esSep := ","
		esCount := 64
		if len(v.Exprs) >= 4 {
			esSepVal, err := e.evalExpr(v.Exprs[3])
			if err != nil {
				return nil, true, err
			}
			if esSepVal != nil {
				esSep = toString(esSepVal)
			}
		}
		if len(v.Exprs) >= 5 {
			esCntVal, err := e.evalExpr(v.Exprs[4])
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
		qVal, isNull, err := e.evalArg1Quiet(v.Exprs)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return "NULL", true, nil
		}
		qStr := toString(qVal)
		qStr = strings.ReplaceAll(qStr, "\\", "\\\\")
		qStr = strings.ReplaceAll(qStr, "'", "\\'")
		return "'" + qStr + "'", true, nil
	case "weight_string":
		wsVal, isNull, err := e.evalArg1Quiet(v.Exprs)
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
		fmtVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		fmtDec, err := e.evalExpr(v.Exprs[1])
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

// evalStringFuncWithRow dispatches string-related functions from evalFuncExprWithRow.
func evalStringFuncWithRow(e *Executor, name string, v *sqlparser.FuncExpr, row storage.Row, evalArgs func() ([]interface{}, error)) (interface{}, bool, error) {
	switch name {
	case "upper", "ucase":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		s := toString(args[0])
		if strings.ContainsRune(s, '\x00') || (len(v.Exprs) > 0 && e.isBinaryExpr(v.Exprs[0])) {
			return s, true, nil
		}
		return strings.ToUpper(s), true, nil
	case "lower", "lcase":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		s := toString(args[0])
		if strings.ContainsRune(s, '\x00') || (len(v.Exprs) > 0 && e.isBinaryExpr(v.Exprs[0])) {
			return s, true, nil
		}
		return strings.ToLower(s), true, nil
	case "repeat":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 2 || args[0] == nil {
			return nil, true, nil
		}
		count := int(math.Round(toFloat(args[1])))
		if count <= 0 {
			return "", true, nil
		}
		str := toString(args[0])
		if int64(count)*int64(len(str)) > 67108864 {
			return nil, true, nil
		}
		return strings.Repeat(str, count), true, nil
	case "concat":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		var sb strings.Builder
		for _, a := range args {
			if a == nil {
				return nil, true, nil
			}
			sb.WriteString(toString(a))
		}
		return sb.String(), true, nil
	case "md5":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		sum := md5.Sum([]byte(toString(args[0])))
		return hex.EncodeToString(sum[:]), true, nil
	case "length", "octet_length":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		s := toString(args[0])
		if colName, ok := v.Exprs[0].(*sqlparser.ColName); ok {
			cs := e.getColumnCharset(colName)
			if byteLen, err2 := charsetByteLength(s, cs); err2 == nil {
				return byteLen, true, nil
			}
		}
		return int64(len(s)), true, nil
	case "char_length", "character_length":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		return int64(mysqlCharLen(toString(args[0]))), true, nil
	case "replace":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 3 || args[0] == nil {
			return nil, true, nil
		}
		return strings.ReplaceAll(toString(args[0]), toString(args[1]), toString(args[2])), true, nil
	case "left":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 2 || args[0] == nil {
			return nil, true, nil
		}
		s := []rune(toString(args[0]))
		n := int(toInt64(args[1]))
		if n <= 0 {
			return "", true, nil
		}
		if n > len(s) {
			n = len(s)
		}
		return string(s[:n]), true, nil
	case "right":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 2 || args[0] == nil {
			return nil, true, nil
		}
		s := []rune(toString(args[0]))
		n := int(toInt64(args[1]))
		if n <= 0 {
			return "", true, nil
		}
		if n > len(s) {
			n = len(s)
		}
		return string(s[len(s)-n:]), true, nil
	case "hex":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		switch tv := args[0].(type) {
		case int64:
			return strings.ToUpper(fmt.Sprintf("%X", tv)), true, nil
		case float64:
			return strings.ToUpper(fmt.Sprintf("%X", int64(tv))), true, nil
		default:
			s := toString(args[0])
			return strings.ToUpper(hex.EncodeToString([]byte(s))), true, nil
		}
	case "instr":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 2 || args[0] == nil || args[1] == nil {
			return nil, true, nil
		}
		s := []rune(toString(args[0]))
		sub := []rune(toString(args[1]))
		if len(sub) == 0 {
			return int64(1), true, nil
		}
		for i := 0; i <= len(s)-len(sub); i++ {
			match := true
			for j := 0; j < len(sub); j++ {
				if s[i+j] != sub[j] {
					match = false
					break
				}
			}
			if match {
				return int64(i + 1), true, nil
			}
		}
		return int64(0), true, nil
	case "reverse":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		runes := []rune(toString(args[0]))
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return string(runes), true, nil
	case "lpad":
		args, err := evalArgs()
		if err != nil {
			var ov *intOverflowError
			if errors.As(err, &ov) {
				return nil, true, nil
			}
			return nil, true, err
		}
		if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
			return nil, true, nil
		}
		s := []rune(toString(args[0]))
		targetLen64 := toInt64(args[1])
		if targetLen64 < 0 {
			return nil, true, nil
		}
		if targetLen64 > 67108864 {
			return nil, true, nil
		}
		targetLen := int(targetLen64)
		padStr := []rune(toString(args[2]))
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
		args, err := evalArgs()
		if err != nil {
			var ov *intOverflowError
			if errors.As(err, &ov) {
				return nil, true, nil
			}
			return nil, true, err
		}
		if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
			return nil, true, nil
		}
		s := []rune(toString(args[0]))
		targetLen64 := toInt64(args[1])
		if targetLen64 < 0 {
			return nil, true, nil
		}
		if targetLen64 > 67108864 {
			return nil, true, nil
		}
		targetLen := int(targetLen64)
		padStr := []rune(toString(args[2]))
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
	case "substring", "substr", "mid":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 2 || args[0] == nil {
			return nil, true, nil
		}
		s := []rune(toString(args[0]))
		pos := int(toInt64(args[1]))
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
		if len(args) >= 3 {
			length := int(toInt64(args[2]))
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
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		return strings.TrimSpace(toString(args[0])), true, nil
	case "ltrim":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		return strings.TrimLeft(toString(args[0]), " \t\n\r"), true, nil
	case "rtrim":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		return strings.TrimRight(toString(args[0]), " \t\n\r"), true, nil
	case "ascii", "ord":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		s := toString(args[0])
		if len(s) == 0 {
			return int64(0), true, nil
		}
		return int64(s[0]), true, nil
	case "char":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		var sb strings.Builder
		for _, arg := range args {
			if arg == nil {
				continue
			}
			n := toInt64(arg)
			if n >= 0 && n <= 255 {
				sb.WriteByte(byte(n))
			}
		}
		return sb.String(), true, nil
	case "strcmp":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 2 || args[0] == nil || args[1] == nil {
			return nil, true, nil
		}
		s0, s1 := strings.ToLower(toString(args[0])), strings.ToLower(toString(args[1]))
		if s0 < s1 {
			return int64(-1), true, nil
		} else if s0 > s1 {
			return int64(1), true, nil
		}
		return int64(0), true, nil
	case "oct":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		n := toInt64(args[0])
		if n < 0 {
			return fmt.Sprintf("%o", uint64(n)), true, nil
		}
		return fmt.Sprintf("%o", n), true, nil
	case "load_file":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		filePath := toString(args[0])
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
			return nil, true, nil
		}
		return string(data), true, nil
	default:
		return nil, false, nil
	}
}
