package executor

import (
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"strconv"
	"strings"

	"github.com/myuon/mylite/storage"
	"vitess.io/vitess/go/vt/sqlparser"
)

// evalMathFunc dispatches math-related functions.
// When row is non-nil, expressions are evaluated with row context.
// Returns (result, handled, error).
func evalMathFunc(e *Executor, name string, v *sqlparser.FuncExpr, row *storage.Row) (interface{}, bool, error) {
	switch name {
	case "rand":
		if len(v.Exprs) == 0 {
			return rand.Float64(), true, nil
		}
		seedVal, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if seedVal == nil {
			return rand.Float64(), true, nil
		}
		r := rand.New(rand.NewSource(toInt64(seedVal)))
		return r.Float64(), true, nil
	case "abs":
		val, isNull, err := e.evalArg1(v.Exprs, "ABS", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		f := toFloat(val)
		if f < 0 {
			f = -f
		}
		if f == float64(int64(f)) {
			return int64(f), true, nil
		}
		return f, true, nil
	case "pi":
		if len(v.Exprs) != 0 {
			return nil, true, mysqlError(1582, "42000", "Incorrect parameter count in the call to native function 'pi'")
		}
		return math.Pi, true, nil
	case "floor":
		val, isNull, err := e.evalArg1(v.Exprs, "FLOOR", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		f := toFloat(val)
		return int64(f), true, nil
	case "ceil", "ceiling":
		val, isNull, err := e.evalArg1(v.Exprs, "CEIL", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		f := toFloat(val)
		n := int64(f)
		if float64(n) < f {
			n++
		}
		return n, true, nil
	case "round":
		val, isNull, err := e.evalArg1(v.Exprs, "ROUND", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		decimals := int64(0)
		if len(v.Exprs) >= 2 {
			dv, err := e.evalExprMaybeRow(v.Exprs[1], row)
			if err != nil {
				return nil, true, err
			}
			decimals = toInt64(dv)
		}
		// Clamp very large negative decimals: MySQL returns 0 for very large negative values.
		if decimals < -30 {
			return int64(0), true, nil
		}
		// Determine whether the second argument is a literal integer constant.
		// When it is a literal, MySQL uses exactly `decimals` decimal places in the result.
		// When it is a column/expression reference, MySQL preserves the source DECIMAL column's scale
		// (result scale = max(decimals, origScale)).
		decimalsIsLiteral := false
		if len(v.Exprs) >= 2 {
			switch arg2 := v.Exprs[1].(type) {
			case *sqlparser.Literal:
				if arg2.Type == sqlparser.IntVal {
					decimalsIsLiteral = true
				}
			case *sqlparser.UnaryExpr:
				if lit, ok2 := arg2.Expr.(*sqlparser.Literal); ok2 && lit.Type == sqlparser.IntVal {
					decimalsIsLiteral = true
				}
			}
		}
		// For exact integer types (int64, uint64) with 0 decimals, return as-is to avoid precision loss
		if decimals == 0 {
			switch tv := val.(type) {
			case int64:
				return tv, true, nil
			case uint64:
				return tv, true, nil
			}
		}
		// Convert non-string numeric types to string so the exact decimal path can handle them.
		// This avoids float64 overflow when `decimals` is large (e.g. 40 or 100).
		var valStr string
		switch tv := val.(type) {
		case string:
			valStr = tv
		case int64:
			valStr = strconv.FormatInt(tv, 10)
		case uint64:
			valStr = strconv.FormatUint(tv, 10)
		case float64:
			valStr = strconv.FormatFloat(tv, 'f', -1, 64)
		case ScaledValue:
			valStr = strconv.FormatFloat(tv.Value, 'f', -1, 64)
		default:
			valStr = ""
		}
		// For decimal strings (including those converted above), use exact decimal rounding.
		if valStr != "" {
			// Cap displayScale at MySQL's DECIMAL max precision (30) to avoid unreasonably long output.
			displayDecimals := decimals
			if displayDecimals > 30 {
				displayDecimals = 30
			}
			if out, ok2 := roundDecimalStringHalfUp(valStr, int(displayDecimals)); ok2 {
				// Determine the display scale:
				// - literal N arg: show exactly N decimal places
				// - column/expression arg: preserve the source column's scale (max of decimals and origScale)
				displayScale := int(displayDecimals)
				if !decimalsIsLiteral {
					if dotIdx := strings.IndexByte(valStr, '.'); dotIdx >= 0 {
						origScale := len(valStr) - dotIdx - 1
						if origScale > displayScale {
							displayScale = origScale
						}
					}
				}
				if displayScale > 0 {
					outDotIdx := strings.IndexByte(out, '.')
					if outDotIdx < 0 {
						out += "." + strings.Repeat("0", displayScale)
					} else {
						curScale := len(out) - outDotIdx - 1
						if curScale < displayScale {
							out += strings.Repeat("0", displayScale-curScale)
						}
					}
				}
				return out, true, nil
			}
		}
		// Float fallback — only used when string conversion was not possible.
		// Clamp decimals to a safe range to avoid int64 overflow in factor multiplication.
		if decimals > 30 {
			decimals = 30
		}
		f := toFloat(val)
		if decimals == 0 {
			return int64(f + 0.5), true, nil
		}
		var rounded float64
		if decimals > 0 {
			factor := 1.0
			for i := int64(0); i < decimals; i++ {
				factor *= 10
			}
			rounded = float64(int64(f*factor+0.5)) / factor
		} else {
			// Negative decimals: round to nearest 10^|decimals|
			factor := 1.0
			for i := decimals; i < 0; i++ {
				factor *= 10
			}
			rounded = float64(int64(f/factor+0.5)) * factor
		}
		outScale := int(decimals)
		if outScale < 0 {
			outScale = 0
		}
		return fmt.Sprintf("%.*f", outScale, rounded), true, nil
	case "truncate":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("TRUNCATE requires 2 arguments")
		}
		val, isNull, err := e.evalArg1(v.Exprs, "TRUNCATE", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		dv, err := e.evalExprMaybeRow(v.Exprs[1], row)
		if err != nil {
			return nil, true, err
		}
		decimals := toInt64(dv)
		if decimals == 0 {
			f := toFloat(val)
			if f >= 0 {
				return int64(f), true, nil
			}
			return -int64(-f), true, nil
		}
		if decimals > 0 {
			// Convert value to string for exact decimal truncation, avoiding float64
			// overflow when decimals is large (e.g. 40 or 100).
			var valStr string
			switch tv := val.(type) {
			case string:
				valStr = tv
			case int64:
				valStr = strconv.FormatInt(tv, 10)
			case uint64:
				valStr = strconv.FormatUint(tv, 10)
			case float64:
				valStr = strconv.FormatFloat(tv, 'f', -1, 64)
			case ScaledValue:
				valStr = strconv.FormatFloat(tv.Value, 'f', -1, 64)
			}
			if valStr != "" {
				// Cap display scale at MySQL's practical maximum of 30.
				dispDecimals := decimals
				if dispDecimals > 30 {
					dispDecimals = 30
				}
				// Truncate: keep only dispDecimals decimal places (no rounding).
				if out, ok2 := truncateDecimalString(valStr, int(dispDecimals)); ok2 {
					outScale := int(dispDecimals)
					// Pad with trailing zeros if needed.
					if outScale > 0 {
						outDotIdx := strings.IndexByte(out, '.')
						if outDotIdx < 0 {
							out += "." + strings.Repeat("0", outScale)
						} else {
							curScale := len(out) - outDotIdx - 1
							if curScale < outScale {
								out += strings.Repeat("0", outScale-curScale)
							}
						}
					}
					return out, true, nil
				}
			}
			// Float fallback for small decimals.
			if decimals > 30 {
				decimals = 30
			}
			factor := 1.0
			for j := int64(0); j < decimals; j++ {
				factor *= 10
			}
			f := toFloat(val)
			outScale := int(decimals)
			if f >= 0 {
				trunc := float64(int64(f*factor)) / factor
				return fmt.Sprintf("%.*f", outScale, trunc), true, nil
			}
			trunc := -float64(int64(-f*factor)) / factor
			return fmt.Sprintf("%.*f", outScale, trunc), true, nil
		}
		// Negative decimals: truncate to the left of decimal point
		factor := 1.0
		for j := int64(0); j < -decimals; j++ {
			factor *= 10
		}
		f := toFloat(val)
		return int64(f/factor) * int64(factor), true, nil
	case "mod":
		v0, v1, hasNull, err := e.evalArgs2(v.Exprs, "MOD", row)
		if err != nil {
			return nil, true, err
		}
		if hasNull {
			return nil, true, nil
		}
		// Use big.Rat for high-precision decimal modulo when either argument is a decimal string.
		// This avoids float64 precision loss for values like MOD(1, 0.123456789123456789...).
		s0, s0IsStr := v0.(string)
		s1, s1IsStr := v1.(string)
		if s0IsStr || s1IsStr {
			var r0Str, r1Str string
			if s0IsStr {
				r0Str = s0
			} else {
				r0Str = fmt.Sprintf("%v", v0)
			}
			if s1IsStr {
				r1Str = s1
			} else {
				r1Str = fmt.Sprintf("%v", v1)
			}
			rat0, ok0 := parseDecimalStringToRat(r0Str)
			rat1, ok1 := parseDecimalStringToRat(r1Str)
			if ok0 && ok1 {
				if rat1.Sign() == 0 {
					return nil, true, nil
				}
				// MOD(a, b) = a - TRUNCATE(a/b, 0) * b  (using big.Rat)
				quot := new(big.Rat).Quo(rat0, rat1)
				// Truncate towards zero: take the integer part of the quotient.
				quotFloat, _ := quot.Float64()
				truncInt := int64(quotFloat)
				truncRat := new(big.Rat).SetInt64(truncInt)
				remainder := new(big.Rat).Sub(rat0, new(big.Rat).Mul(truncRat, rat1))
				// Determine output scale from the input strings.
				scale0 := valueScale(r0Str)
				scale1 := valueScale(r1Str)
				outScale := scale0
				if scale1 > outScale {
					outScale = scale1
				}
				result := formatRatFixed(remainder, outScale)
				return result, true, nil
			}
		}
		// For integer or float operands, use float64 modulo.
		rf0 := toFloat(v0)
		rf1 := toFloat(v1)
		if rf1 == 0 {
			return nil, true, nil
		}
		mod := math.Mod(rf0, rf1)
		if mod == float64(int64(mod)) {
			return int64(mod), true, nil
		}
		return mod, true, nil
	case "sqrt":
		sqrtVal, isNull, err := e.evalArg1(v.Exprs, "SQRT", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		sqrtF := toFloat(sqrtVal)
		if sqrtF < 0 {
			return nil, true, nil
		}
		return math.Sqrt(sqrtF), true, nil
	case "sign":
		signVal, isNull, err := e.evalArg1(v.Exprs, "SIGN", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		signF := toFloat(signVal)
		if signF > 0 {
			return int64(1), true, nil
		} else if signF < 0 {
			return int64(-1), true, nil
		}
		return int64(0), true, nil
	case "ln":
		lnVal, isNull, err := e.evalArg1(v.Exprs, "LN", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		lnF := toFloat(lnVal)
		if lnF <= 0 {
			return nil, true, nil
		}
		return math.Log(lnF), true, nil
	case "log":
		if len(v.Exprs) == 1 {
			logVal, err := e.evalExprMaybeRow(v.Exprs[0], row)
			if err != nil {
				return nil, true, err
			}
			if logVal == nil {
				return nil, true, nil
			}
			logF := toFloat(logVal)
			if logF <= 0 {
				return nil, true, nil
			}
			return math.Log(logF), true, nil
		} else if len(v.Exprs) == 2 {
			logBase, err := e.evalExprMaybeRow(v.Exprs[0], row)
			if err != nil {
				return nil, true, err
			}
			logVal, err := e.evalExprMaybeRow(v.Exprs[1], row)
			if err != nil {
				return nil, true, err
			}
			if logBase == nil || logVal == nil {
				return nil, true, nil
			}
			logBF := toFloat(logBase)
			logVF := toFloat(logVal)
			if logBF <= 0 || logBF == 1 || logVF <= 0 {
				return nil, true, nil
			}
			return math.Log(logVF) / math.Log(logBF), true, nil
		}
		return nil, true, fmt.Errorf("LOG requires 1 or 2 arguments")
	case "log2":
		log2Val, isNull, err := e.evalArg1(v.Exprs, "LOG2", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		log2F := toFloat(log2Val)
		if log2F <= 0 {
			return nil, true, nil
		}
		return math.Log2(log2F), true, nil
	case "log10":
		log10Val, isNull, err := e.evalArg1(v.Exprs, "LOG10", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		log10F := toFloat(log10Val)
		if log10F <= 0 {
			return nil, true, nil
		}
		return math.Log10(log10F), true, nil
	case "exp":
		expVal, isNull, err := e.evalArg1(v.Exprs, "EXP", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		return math.Exp(toFloat(expVal)), true, nil
	case "pow", "power":
		powBase, powExp, hasNull, err := e.evalArgs2(v.Exprs, "POW", row)
		if err != nil {
			return nil, true, err
		}
		if hasNull {
			return nil, true, nil
		}
		return math.Pow(toFloat(powBase), toFloat(powExp)), true, nil
	case "crc32":
		crcVal, isNull, err := e.evalArg1(v.Exprs, "CRC32", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		crcS := toString(crcVal)
		var crcResult uint32 = 0xFFFFFFFF
		for ci := 0; ci < len(crcS); ci++ {
			crcResult ^= uint32(crcS[ci])
			for cj := 0; cj < 8; cj++ {
				if crcResult&1 != 0 {
					crcResult = (crcResult >> 1) ^ 0xEDB88320
				} else {
					crcResult >>= 1
				}
			}
		}
		return int64(crcResult ^ 0xFFFFFFFF), true, nil
	case "degrees":
		degVal, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		return toFloat(degVal) * 180 / math.Pi, true, nil
	case "radians":
		radVal, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		return toFloat(radVal) * math.Pi / 180, true, nil
	case "acos":
		acosVal, isNull, err := e.evalArg1(v.Exprs, "ACOS", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		acosF := toFloat(acosVal)
		if acosF < -1 || acosF > 1 {
			return nil, true, nil
		}
		return math.Acos(acosF), true, nil
	case "asin":
		asinVal, isNull, err := e.evalArg1(v.Exprs, "ASIN", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		asinF := toFloat(asinVal)
		if asinF < -1 || asinF > 1 {
			return nil, true, nil
		}
		return math.Asin(asinF), true, nil
	case "atan", "atan2":
		atanVal, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		if len(v.Exprs) >= 2 {
			atanVal2, err := e.evalExprMaybeRow(v.Exprs[1], row)
			if err != nil {
				return nil, true, err
			}
			if atanVal2 == nil {
				return nil, true, nil
			}
			return math.Atan2(toFloat(atanVal), toFloat(atanVal2)), true, nil
		}
		return math.Atan(toFloat(atanVal)), true, nil
	case "sin":
		sinVal, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		return math.Sin(toFloat(sinVal)), true, nil
	case "cos":
		cosVal, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		return math.Cos(toFloat(cosVal)), true, nil
	case "tan":
		tanVal, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		return math.Tan(toFloat(tanVal)), true, nil
	case "cot":
		cotVal, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		cotF := toFloat(cotVal)
		sinV := math.Sin(cotF)
		if sinV == 0 {
			return nil, true, nil
		}
		return math.Cos(cotF) / sinV, true, nil
	case "bit_count":
		bcVal, isNull, err := e.evalArg1(v.Exprs, "BIT_COUNT", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		// For HexBytes (BINARY/VARBINARY), count bits across all bytes
		if hb, ok := bcVal.(HexBytes); ok {
			hexStr := string(hb)
			if len(hexStr)%2 != 0 {
				hexStr = "0" + hexStr
			}
			decoded, decErr := hex.DecodeString(hexStr)
			if decErr == nil {
				bcCount := int64(0)
				for _, b := range decoded {
					for b != 0 {
						bcCount += int64(b & 1)
						b >>= 1
					}
				}
				return bcCount, true, nil
			}
		}
		bcU := uint64(toInt64(bcVal))
		bcCount := int64(0)
		for bcU != 0 {
			bcCount += int64(bcU & 1)
			bcU >>= 1
		}
		return bcCount, true, nil
	case "conv":
		if len(v.Exprs) < 3 {
			return nil, true, nil
		}
		val, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		fromBaseVal, err := e.evalExprMaybeRow(v.Exprs[1], row)
		if err != nil {
			return nil, true, err
		}
		toBaseVal, err := e.evalExprMaybeRow(v.Exprs[2], row)
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
		}
		s := toString(val)
		fromBase := int(toInt64(fromBaseVal))
		toBase := int(toInt64(toBaseVal))
		// MySQL allows negative to_base (means signed output); normalize for validation
		absFromBase := fromBase
		if absFromBase < 0 {
			absFromBase = -absFromBase
		}
		absToBase := toBase
		if absToBase < 0 {
			absToBase = -absToBase
		}
		if absFromBase < 2 || absFromBase > 36 || absToBase < 2 || absToBase > 36 {
			return nil, true, nil
		}
		// MySQL CONV() treats values as unsigned 64-bit integers.
		// Try unsigned parse first to handle large hex values like e251273eb74a8ee3.
		un, parseErr := strconv.ParseUint(s, absFromBase, 64)
		if parseErr != nil {
			// Fall back to signed parse (for negative values)
			n, signedErr := strconv.ParseInt(s, absFromBase, 64)
			if signedErr != nil {
				return nil, true, nil
			}
			un = uint64(n)
		}
		if toBase < 0 {
			// Negative toBase means interpret as signed and output signed
			return strings.ToUpper(strconv.FormatInt(int64(un), absToBase)), true, nil
		}
		return strings.ToUpper(strconv.FormatUint(un, toBase)), true, nil
	case "bin":
		val, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		n := toInt64(val)
		if n < 0 {
			return fmt.Sprintf("%b", uint64(n)), true, nil
		}
		return fmt.Sprintf("%b", n), true, nil
	default:
		return nil, false, nil
	}
}
