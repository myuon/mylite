package executor

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"

	"github.com/myuon/mylite/storage"
	"vitess.io/vitess/go/vt/sqlparser"
)

// evalMathFunc dispatches math-related functions from evalFuncExpr.
// Returns (result, handled, error).
func evalMathFunc(e *Executor, name string, v *sqlparser.FuncExpr) (interface{}, bool, error) {
	switch name {
	case "rand":
		if len(v.Exprs) == 0 {
			return rand.Float64(), true, nil
		}
		seedVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if seedVal == nil {
			return rand.Float64(), true, nil
		}
		r := rand.New(rand.NewSource(toInt64(seedVal)))
		return r.Float64(), true, nil
	case "abs":
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("ABS requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if val == nil {
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
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("FLOOR requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
		}
		f := toFloat(val)
		return int64(f), true, nil
	case "ceil", "ceiling":
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("CEIL requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
		}
		f := toFloat(val)
		n := int64(f)
		if float64(n) < f {
			n++
		}
		return n, true, nil
	case "round":
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("ROUND requires at least 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
		}
		f := toFloat(val)
		decimals := int64(0)
		if len(v.Exprs) >= 2 {
			dv, err := e.evalExpr(v.Exprs[1])
			if err != nil {
				return nil, true, err
			}
			decimals = toInt64(dv)
		}
		if decimals == 0 {
			return int64(f + 0.5), true, nil
		}
		factor := 1.0
		for i := int64(0); i < decimals; i++ {
			factor *= 10
		}
		rounded := float64(int64(f*factor+0.5)) / factor
		outScale := int(decimals)
		if s, ok := val.(string); ok {
			if dot := strings.IndexByte(s, '.'); dot >= 0 {
				inScale := len(s) - dot - 1
				if inScale > outScale {
					outScale = inScale
				}
			}
		}
		return fmt.Sprintf("%.*f", outScale, rounded), true, nil
	case "truncate":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("TRUNCATE requires 2 arguments")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
		}
		dv, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, true, err
		}
		f := toFloat(val)
		decimals := toInt64(dv)
		if decimals == 0 {
			if f >= 0 {
				return int64(f), true, nil
			}
			return -int64(-f), true, nil
		}
		if decimals > 0 {
			factor := 1.0
			for j := int64(0); j < decimals; j++ {
				factor *= 10
			}
			outScale := int(decimals)
			if s, ok := val.(string); ok {
				if dot := strings.IndexByte(s, '.'); dot >= 0 {
					inScale := len(s) - dot - 1
					if inScale > outScale {
						outScale = inScale
					}
				}
			}
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
		return int64(f/factor) * int64(factor), true, nil
	case "mod":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("MOD requires 2 arguments")
		}
		v0, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		v1, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, true, err
		}
		if v0 == nil || v1 == nil {
			return nil, true, nil
		}
		d := toInt64(v1)
		if d == 0 {
			return nil, true, nil
		}
		return toInt64(v0) % d, true, nil
	case "sqrt":
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("SQRT requires 1 argument")
		}
		sqrtVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if sqrtVal == nil {
			return nil, true, nil
		}
		sqrtF := toFloat(sqrtVal)
		if sqrtF < 0 {
			return nil, true, nil
		}
		return math.Sqrt(sqrtF), true, nil
	case "sign":
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("SIGN requires 1 argument")
		}
		signVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if signVal == nil {
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
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("LN requires 1 argument")
		}
		lnVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if lnVal == nil {
			return nil, true, nil
		}
		lnF := toFloat(lnVal)
		if lnF <= 0 {
			return nil, true, nil
		}
		return math.Log(lnF), true, nil
	case "log":
		if len(v.Exprs) == 1 {
			logVal, err := e.evalExpr(v.Exprs[0])
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
			logBase, err := e.evalExpr(v.Exprs[0])
			if err != nil {
				return nil, true, err
			}
			logVal, err := e.evalExpr(v.Exprs[1])
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
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("LOG2 requires 1 argument")
		}
		log2Val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if log2Val == nil {
			return nil, true, nil
		}
		log2F := toFloat(log2Val)
		if log2F <= 0 {
			return nil, true, nil
		}
		return math.Log2(log2F), true, nil
	case "log10":
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("LOG10 requires 1 argument")
		}
		log10Val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if log10Val == nil {
			return nil, true, nil
		}
		log10F := toFloat(log10Val)
		if log10F <= 0 {
			return nil, true, nil
		}
		return math.Log10(log10F), true, nil
	case "exp":
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("EXP requires 1 argument")
		}
		expVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if expVal == nil {
			return nil, true, nil
		}
		return math.Exp(toFloat(expVal)), true, nil
	case "pow", "power":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("POW requires 2 arguments")
		}
		powBase, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		powExp, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, true, err
		}
		if powBase == nil || powExp == nil {
			return nil, true, nil
		}
		return math.Pow(toFloat(powBase), toFloat(powExp)), true, nil
	case "crc32":
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("CRC32 requires 1 argument")
		}
		crcVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if crcVal == nil {
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
		if len(v.Exprs) < 1 {
			return nil, true, nil
		}
		degVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if degVal == nil {
			return nil, true, nil
		}
		return toFloat(degVal) * 180 / math.Pi, true, nil
	case "radians":
		if len(v.Exprs) < 1 {
			return nil, true, nil
		}
		radVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if radVal == nil {
			return nil, true, nil
		}
		return toFloat(radVal) * math.Pi / 180, true, nil
	case "acos":
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("ACOS requires 1 argument")
		}
		acosVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if acosVal == nil {
			return nil, true, nil
		}
		acosF := toFloat(acosVal)
		if acosF < -1 || acosF > 1 {
			return nil, true, nil
		}
		return math.Acos(acosF), true, nil
	case "asin":
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("ASIN requires 1 argument")
		}
		asinVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if asinVal == nil {
			return nil, true, nil
		}
		asinF := toFloat(asinVal)
		if asinF < -1 || asinF > 1 {
			return nil, true, nil
		}
		return math.Asin(asinF), true, nil
	case "atan", "atan2":
		if len(v.Exprs) < 1 {
			return nil, true, nil
		}
		atanVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if atanVal == nil {
			return nil, true, nil
		}
		if len(v.Exprs) >= 2 {
			atanVal2, err := e.evalExpr(v.Exprs[1])
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
		if len(v.Exprs) < 1 {
			return nil, true, nil
		}
		sinVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if sinVal == nil {
			return nil, true, nil
		}
		return math.Sin(toFloat(sinVal)), true, nil
	case "cos":
		if len(v.Exprs) < 1 {
			return nil, true, nil
		}
		cosVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if cosVal == nil {
			return nil, true, nil
		}
		return math.Cos(toFloat(cosVal)), true, nil
	case "tan":
		if len(v.Exprs) < 1 {
			return nil, true, nil
		}
		tanVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if tanVal == nil {
			return nil, true, nil
		}
		return math.Tan(toFloat(tanVal)), true, nil
	case "cot":
		if len(v.Exprs) < 1 {
			return nil, true, nil
		}
		cotVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if cotVal == nil {
			return nil, true, nil
		}
		cotF := toFloat(cotVal)
		sinV := math.Sin(cotF)
		if sinV == 0 {
			return nil, true, nil
		}
		return math.Cos(cotF) / sinV, true, nil
	case "bit_count":
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("BIT_COUNT requires 1 argument")
		}
		bcVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if bcVal == nil {
			return nil, true, nil
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
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		fromBaseVal, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, true, err
		}
		toBaseVal, err := e.evalExpr(v.Exprs[2])
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
		}
		s := toString(val)
		fromBase := int(toInt64(fromBaseVal))
		toBase := int(toInt64(toBaseVal))
		n, parseErr := strconv.ParseInt(s, fromBase, 64)
		if parseErr != nil {
			return nil, true, nil
		}
		return strings.ToUpper(strconv.FormatInt(n, toBase)), true, nil
	case "bin":
		if len(v.Exprs) < 1 {
			return nil, true, nil
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
		}
		n := toInt64(val)
		return fmt.Sprintf("%b", n), true, nil
	default:
		return nil, false, nil
	}
}

// evalMathFuncWithRow dispatches math-related functions from evalFuncExprWithRow.
func evalMathFuncWithRow(e *Executor, name string, v *sqlparser.FuncExpr, row storage.Row, evalArgs func() ([]interface{}, error)) (interface{}, bool, error) {
	switch name {
	case "rand":
		if len(v.Exprs) == 0 {
			return rand.Float64(), true, nil
		}
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return rand.Float64(), true, nil
		}
		r := rand.New(rand.NewSource(toInt64(args[0])))
		return r.Float64(), true, nil
	case "abs":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		f := toFloat(args[0])
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
	case "mod":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 2 || args[0] == nil || args[1] == nil {
			return nil, true, nil
		}
		d := toInt64(args[1])
		if d == 0 {
			return nil, true, nil
		}
		return toInt64(args[0]) % d, true, nil
	case "conv":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 3 || args[0] == nil {
			return nil, true, nil
		}
		s := toString(args[0])
		fromBase := int(toInt64(args[1]))
		toBase := int(toInt64(args[2]))
		if fromBase < 2 || fromBase > 36 || toBase < 2 || toBase > 36 {
			return nil, true, nil
		}
		n, parseErr := strconv.ParseInt(s, fromBase, 64)
		if parseErr != nil {
			return nil, true, nil
		}
		return strings.ToUpper(strconv.FormatInt(n, toBase)), true, nil
	case "bin":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		n := toInt64(args[0])
		if n < 0 {
			return fmt.Sprintf("%b", uint64(n)), true, nil
		}
		return fmt.Sprintf("%b", n), true, nil
	case "truncate":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 2 || args[0] == nil {
			return nil, true, nil
		}
		f := toFloat(args[0])
		decimals := toInt64(args[1])
		if decimals == 0 {
			if f >= 0 {
				return int64(f), true, nil
			}
			return -int64(-f), true, nil
		}
		if decimals > 0 {
			factor := 1.0
			for j := int64(0); j < decimals; j++ {
				factor *= 10
			}
			outScale := int(decimals)
			if s, ok := args[0].(string); ok {
				if dot := strings.IndexByte(s, '.'); dot >= 0 {
					inScale := len(s) - dot - 1
					if inScale > outScale {
						outScale = inScale
					}
				}
			}
			if f >= 0 {
				trunc := float64(int64(f*factor)) / factor
				return fmt.Sprintf("%.*f", outScale, trunc), true, nil
			}
			trunc := -float64(int64(-f*factor)) / factor
			return fmt.Sprintf("%.*f", outScale, trunc), true, nil
		}
		factor := 1.0
		for j := int64(0); j < -decimals; j++ {
			factor *= 10
		}
		return int64(f/factor) * int64(factor), true, nil
	case "round":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		f := toFloat(args[0])
		decimals := int64(0)
		if len(args) >= 2 {
			decimals = toInt64(args[1])
		}
		if decimals == 0 {
			return int64(f + 0.5), true, nil
		}
		factor := 1.0
		for j := int64(0); j < decimals; j++ {
			factor *= 10
		}
		rounded := float64(int64(f*factor+0.5)) / factor
		outScale := int(decimals)
		if s, ok := args[0].(string); ok {
			if dot := strings.IndexByte(s, '.'); dot >= 0 {
				inScale := len(s) - dot - 1
				if inScale > outScale {
					outScale = inScale
				}
			}
		}
		return fmt.Sprintf("%.*f", outScale, rounded), true, nil
	case "bit_count":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 {
			return nil, true, fmt.Errorf("BIT_COUNT requires 1 argument")
		}
		if args[0] == nil {
			return nil, true, nil
		}
		bcU := uint64(toInt64(args[0]))
		bcCount := int64(0)
		for bcU != 0 {
			bcCount += int64(bcU & 1)
			bcU >>= 1
		}
		return bcCount, true, nil
	default:
		return nil, false, nil
	}
}
