package executor

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math/rand"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/myuon/mylite/storage"
	"vitess.io/vitess/go/vt/sqlparser"
)

// evalMiscFunc dispatches miscellaneous functions.
// When row is non-nil, expressions are evaluated with row context.
// Returns (result, handled, error).
func evalMiscFunc(e *Executor, name string, v *sqlparser.FuncExpr, row *storage.Row) (interface{}, bool, error) {
	switch name {
	case "last_insert_id":
		if len(v.Exprs) > 0 {
			val, err := e.evalExprMaybeRow(v.Exprs[0], row)
			if err != nil {
				return nil, true, err
			}
			e.lastInsertID = toInt64(val)
			return e.lastInsertID, true, nil
		}
		return e.lastInsertID, true, nil
	case "database", "schema":
		return e.CurrentDB, true, nil
	case "version":
		return "8.4.0-mylite", true, nil
	case "ifnull", "nvl":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("IFNULL requires 2 arguments")
		}
		val, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if val != nil {
			return val, true, nil
		}
		r, err := e.evalExprMaybeRow(v.Exprs[1], row)
		return r, true, err
	case "coalesce":
		for _, argExpr := range v.Exprs {
			val, err := e.evalExprMaybeRow(argExpr, row)
			if err != nil {
				return nil, true, err
			}
			if val != nil {
				return val, true, nil
			}
		}
		return nil, true, nil
	case "if":
		if len(v.Exprs) < 3 {
			return nil, true, fmt.Errorf("IF requires 3 arguments")
		}
		cond, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if isTruthy(cond) {
			r, err := e.evalExprMaybeRow(v.Exprs[1], row)
			return r, true, err
		}
		r, err := e.evalExprMaybeRow(v.Exprs[2], row)
		return r, true, err
	case "isnull":
		val, isNull, err := e.evalArg1(v.Exprs, "ISNULL", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return int64(1), true, nil
		}
		_ = val
		return int64(0), true, nil
	case "nullif":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("NULLIF requires 2 arguments")
		}
		v0, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		v1, err := e.evalExprMaybeRow(v.Exprs[1], row)
		if err != nil {
			return nil, true, err
		}
		if fmt.Sprintf("%v", v0) == fmt.Sprintf("%v", v1) {
			return nil, true, nil
		}
		return v0, true, nil
	case "cast", "convert":
		if len(v.Exprs) >= 1 {
			r, err := e.evalExprMaybeRow(v.Exprs[0], row)
			return r, true, err
		}
		return nil, true, nil
	case "charset":
		if len(v.Exprs) < 1 {
			return nil, true, nil
		}
		if cue, ok := v.Exprs[0].(*sqlparser.ConvertUsingExpr); ok {
			return strings.ToLower(cue.Type), true, nil
		}
		// Handle charset introducer: _utf8'a' → charset is "utf8"
		if intro, ok := v.Exprs[0].(*sqlparser.IntroducerExpr); ok {
			cs := strings.ToLower(intro.CharacterSet)
			// Normalize utf8mb3 → utf8 for display
			if cs == "_utf8mb3" || cs == "utf8mb3" {
				cs = "utf8"
			} else {
				cs = strings.TrimPrefix(cs, "_")
			}
			return cs, true, nil
		}
		val, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return "binary", true, nil
		}
		if colName, ok := v.Exprs[0].(*sqlparser.ColName); ok {
			cs := e.getColumnCharset(colName)
			if cs != "" {
				return cs, true, nil
			}
		}
		colStr := ""
		if colName, ok := v.Exprs[0].(*sqlparser.ColName); ok {
			colStr = colName.Name.String()
		}
		if colStr != "" {
			if db, err2 := e.Catalog.GetDatabase(e.CurrentDB); err2 == nil {
				for _, tblDef := range db.Tables {
					if tblDef.Charset != "" {
						for _, col := range tblDef.Columns {
							if strings.EqualFold(col.Name, colStr) {
								return tblDef.Charset, true, nil
							}
						}
					}
				}
			}
		}
		if cs, ok := e.getSysVar("character_set_connection"); ok && cs != "" {
			return strings.ToLower(cs), true, nil
		}
		return "utf8", true, nil
	case "collation":
		if len(v.Exprs) < 1 {
			return nil, true, nil
		}
		if cue, ok := v.Exprs[0].(*sqlparser.ConvertUsingExpr); ok {
			cs := strings.ToLower(cue.Type)
			switch cs {
			case "utf8", "utf8mb3":
				return "utf8mb3_general_ci", true, nil
			case "utf8mb4":
				return "utf8mb4_0900_ai_ci", true, nil
			case "latin1":
				return "latin1_swedish_ci", true, nil
			case "binary":
				return "binary", true, nil
			default:
				return cs + "_general_ci", true, nil
			}
		}
		// Handle charset introducer: _utf8'a' → collation is "utf8_general_ci"
		if intro, ok := v.Exprs[0].(*sqlparser.IntroducerExpr); ok {
			cs := strings.ToLower(strings.TrimPrefix(intro.CharacterSet, "_"))
			switch cs {
			case "utf8", "utf8mb3":
				return "utf8_general_ci", true, nil
			case "utf8mb4":
				return "utf8mb4_0900_ai_ci", true, nil
			case "latin1":
				return "latin1_swedish_ci", true, nil
			case "binary":
				return "binary", true, nil
			default:
				return cs + "_general_ci", true, nil
			}
		}
		val, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return "binary", true, nil
		}
		if colName, ok := v.Exprs[0].(*sqlparser.ColName); ok {
			cs := e.getColumnCharset(colName)
			if cs != "" {
				switch cs {
				case "utf8", "utf8mb3":
					return "utf8mb3_general_ci", true, nil
				case "utf8mb4":
					return "utf8mb4_0900_ai_ci", true, nil
				case "latin1":
					return "latin1_swedish_ci", true, nil
				case "binary":
					return "binary", true, nil
				default:
					return cs + "_general_ci", true, nil
				}
			}
		}
		// Functions that return utf8 strings: charset(), collation(), user(), database(), version(), etc.
		if innerFunc, ok := v.Exprs[0].(*sqlparser.FuncExpr); ok {
			funcName := strings.ToLower(innerFunc.Name.String())
			switch funcName {
			case "charset", "collation", "user", "current_user", "session_user",
				"system_user", "database", "schema", "version":
				return "utf8_general_ci", true, nil
			}
		}
		return "utf8mb4_0900_ai_ci", true, nil
	case "least":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("LEAST requires at least 2 arguments")
		}
		var result interface{}
		allNull := true
		for _, argExpr := range v.Exprs {
			val, err := e.evalExprMaybeRow(argExpr, row)
			if err != nil {
				return nil, true, err
			}
			if val == nil {
				return nil, true, nil
			}
			allNull = false
			if result == nil {
				result = val
			} else {
				cmp := compareNumeric(val, result)
				if cmp < 0 {
					result = val
				}
			}
		}
		if allNull {
			return nil, true, nil
		}
		return result, true, nil
	case "greatest":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("GREATEST requires at least 2 arguments")
		}
		var result interface{}
		allNull := true
		for _, argExpr := range v.Exprs {
			val, err := e.evalExprMaybeRow(argExpr, row)
			if err != nil {
				return nil, true, err
			}
			if val == nil {
				return nil, true, nil
			}
			allNull = false
			if result == nil {
				result = val
			} else {
				cmp := compareNumeric(val, result)
				if cmp > 0 {
					result = val
				}
			}
		}
		if allNull {
			return nil, true, nil
		}
		return result, true, nil
	case "current_user":
		if cu, ok := e.userVars["__current_user"]; ok {
			if cuStr, ok := cu.(string); ok && cuStr != "" {
				// If the username doesn't include @host, append @localhost
				if !strings.Contains(cuStr, "@") {
					return cuStr + "@localhost", true, nil
				}
				return cuStr, true, nil
			}
		}
		return "root@localhost", true, nil
	case "connection_id":
		return e.connectionID, true, nil
	case "found_rows":
		return e.lastFoundRows, true, nil
	case "interval":
		if len(v.Exprs) < 2 {
			return int64(-1), true, nil
		}
		ivN, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if ivN == nil {
			return int64(-1), true, nil
		}
		ivNF := toFloat(ivN)
		ivResult := int64(0)
		for ivi := 1; ivi < len(v.Exprs); ivi++ {
			ivVal, err := e.evalExprMaybeRow(v.Exprs[ivi], row)
			if err != nil {
				return nil, true, err
			}
			if ivVal == nil {
				continue
			}
			if ivNF >= toFloat(ivVal) {
				ivResult = int64(ivi)
			} else {
				break
			}
		}
		return ivResult, true, nil
	case "sleep":
		if len(v.Exprs) > 0 {
			dur, err := e.evalExprMaybeRow(v.Exprs[0], row)
			if err != nil {
				return nil, true, err
			}
			secs := toFloat(dur)
			if secs > 0 && secs <= 300 {
				time.Sleep(time.Duration(secs * float64(time.Second)))
			}
		}
		return int64(0), true, nil
	case "user", "session_user", "system_user":
		if cu, ok := e.userVars["__current_user"]; ok {
			if cuStr, ok := cu.(string); ok && cuStr != "" {
				if !strings.Contains(cuStr, "@") {
					return cuStr + "@localhost", true, nil
				}
				return cuStr, true, nil
			}
		}
		return "root@localhost", true, nil
	case "regexp_like":
		rlVal, rlPat, hasNull, err := e.evalArgs2(v.Exprs, "REGEXP_LIKE", row)
		if err != nil {
			return nil, true, err
		}
		if hasNull {
			return nil, true, nil
		}
		rlFlags := ""
		if len(v.Exprs) >= 3 {
			rlFv, err := e.evalExprMaybeRow(v.Exprs[2], row)
			if err != nil {
				return nil, true, err
			}
			if rlFv != nil {
				rlFlags = toString(rlFv)
			}
		}
		rlPattern := toString(rlPat)
		if strings.Contains(rlFlags, "i") {
			rlPattern = "(?i)" + rlPattern
		}
		rlRe, err := regexp.Compile(rlPattern)
		if err != nil {
			// If the pattern has large repetition counts (e.g. {120}, {80}), MySQL's ICU
			// engine compiles it but then times out during matching. Map to timeout error.
			if strings.Contains(err.Error(), "invalid repeat count") {
				return nil, true, mysqlError(3699, "HY000", "Timeout exceeded in regular expression match.")
			}
			return nil, true, mysqlError(3692, "HY000", "Illegal argument to a regular expression.")
		}
		if rlRe.MatchString(toString(rlVal)) {
			return int64(1), true, nil
		}
		return int64(0), true, nil
	case "is_ipv4":
		ipVal, isNull, err := e.evalArg1(v.Exprs, "IS_IPV4", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return int64(0), true, nil
		}
		ipStr := toString(ipVal)
		ipParts := strings.Split(ipStr, ".")
		if len(ipParts) != 4 {
			return int64(0), true, nil
		}
		ipValid := true
		for _, ipP := range ipParts {
			ipN, ipErr := strconv.Atoi(ipP)
			if ipErr != nil || ipN < 0 || ipN > 255 {
				ipValid = false
				break
			}
		}
		if ipValid {
			return int64(1), true, nil
		}
		return int64(0), true, nil
	case "current_role":
		return "NONE", true, nil
	case "inet_ntoa":
		inVal, isNull, err := e.evalArg1(v.Exprs, "INET_NTOA", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		inN := uint32(toInt64(inVal))
		return fmt.Sprintf("%d.%d.%d.%d", (inN>>24)&0xFF, (inN>>16)&0xFF, (inN>>8)&0xFF, inN&0xFF), true, nil
	case "inet_aton":
		iaVal, isNull, err := e.evalArg1(v.Exprs, "INET_ATON", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		iaParts := strings.Split(toString(iaVal), ".")
		if len(iaParts) != 4 {
			return nil, true, nil
		}
		var iaResult uint32
		for _, iaP := range iaParts {
			iaN, err := strconv.Atoi(iaP)
			if err != nil || iaN < 0 || iaN > 255 {
				return nil, true, nil
			}
			iaResult = iaResult*256 + uint32(iaN)
		}
		return int64(iaResult), true, nil
	case "coercibility":
		return int64(4), true, nil
	case "st_astext", "st_aswkt":
		stVal, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		return toString(stVal), true, nil
	case "st_equals":
		steA, steB, hasNull, err := e.evalArgs2(v.Exprs, "ST_EQUALS", row)
		if err != nil {
			return nil, true, err
		}
		if hasNull {
			return nil, true, nil
		}
		if toString(steA) == toString(steB) {
			return int64(1), true, nil
		}
		return int64(0), true, nil
	case "mbrintersects", "st_intersects", "mbrwithin", "st_within", "mbrcontains", "st_contains":
		g1Val, g2Val, hasNull, err := e.evalArgs2(v.Exprs, strings.ToUpper(name), row)
		if err != nil {
			return nil, true, err
		}
		if hasNull {
			return nil, true, nil
		}
		g1Str := toString(g1Val)
		g2Str := toString(g2Val)
		mbr1 := wktBoundingBox(g1Str)
		mbr2 := wktBoundingBox(g2Str)
		if mbr1 == nil || mbr2 == nil {
			return nil, true, nil
		}
		if name == "mbrintersects" || name == "st_intersects" {
			if mbr1[2] >= mbr2[0] && mbr2[2] >= mbr1[0] && mbr1[3] >= mbr2[1] && mbr2[3] >= mbr1[1] {
				return int64(1), true, nil
			}
			return int64(0), true, nil
		}
		if name == "mbrwithin" || name == "st_within" {
			if mbr1[0] >= mbr2[0] && mbr1[1] >= mbr2[1] && mbr1[2] <= mbr2[2] && mbr1[3] <= mbr2[3] {
				return int64(1), true, nil
			}
			return int64(0), true, nil
		}
		if mbr2[0] >= mbr1[0] && mbr2[1] >= mbr1[1] && mbr2[2] <= mbr1[2] && mbr2[3] <= mbr1[3] {
			return int64(1), true, nil
		}
		return int64(0), true, nil
	case "roles_graphml":
		return "<graphml/>", true, nil
	case "uuid":
		uuidB := make([]byte, 16)
		rand.Read(uuidB)
		uuidB[6] = (uuidB[6] & 0x0f) | 0x40
		uuidB[8] = (uuidB[8] & 0x3f) | 0x80
		return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
			uint32(uuidB[0])<<24|uint32(uuidB[1])<<16|uint32(uuidB[2])<<8|uint32(uuidB[3]),
			uint16(uuidB[4])<<8|uint16(uuidB[5]),
			uint16(uuidB[6])<<8|uint16(uuidB[7]),
			uint16(uuidB[8])<<8|uint16(uuidB[9]),
			uint64(uuidB[10])<<40|uint64(uuidB[11])<<32|uint64(uuidB[12])<<24|uint64(uuidB[13])<<16|uint64(uuidB[14])<<8|uint64(uuidB[15])), true, nil
	case "is_ipv6":
		ip6Val, isNull, err := e.evalArg1(v.Exprs, "IS_IPV6", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return int64(0), true, nil
		}
		ip6Str := toString(ip6Val)
		if strings.Contains(ip6Str, ":") && !strings.Contains(ip6Str, " ") {
			return int64(1), true, nil
		}
		return int64(0), true, nil
	case "is_ipv4_mapped":
		imVal, isNull, err := e.evalArg1(v.Exprs, "IS_IPV4_MAPPED", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return int64(0), true, nil
		}
		imBytes := []byte(toString(imVal))
		if len(imBytes) == 16 {
			isMapped := true
			for bi := 0; bi < 10; bi++ {
				if imBytes[bi] != 0 {
					isMapped = false
					break
				}
			}
			if isMapped && imBytes[10] == 0xff && imBytes[11] == 0xff {
				return int64(1), true, nil
			}
		}
		return int64(0), true, nil
	case "is_ipv4_compat":
		icVal, isNull, err := e.evalArg1(v.Exprs, "IS_IPV4_COMPAT", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return int64(0), true, nil
		}
		icBytes := []byte(toString(icVal))
		if len(icBytes) == 16 {
			isCompat := true
			for bi := 0; bi < 12; bi++ {
				if icBytes[bi] != 0 {
					isCompat = false
					break
				}
			}
			if isCompat && (icBytes[12] != 0 || icBytes[13] != 0 || icBytes[14] != 0 || icBytes[15] != 0) {
				return int64(1), true, nil
			}
		}
		return int64(0), true, nil
	case "sha", "sha1":
		shaVal, isNull, err := e.evalArg1(v.Exprs, "SHA", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		return fmt.Sprintf("%x", md5.Sum([]byte(toString(shaVal)))), true, nil
	case "benchmark":
		return int64(0), true, nil
	case "master_pos_wait":
		return int64(0), true, nil
	case "statement_digest":
		sdVal, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		sdHash := md5.Sum([]byte(toString(sdVal)))
		return hex.EncodeToString(sdHash[:]), true, nil
	case "name_const":
		if len(v.Exprs) < 2 {
			return nil, true, nil
		}
		r, err := e.evalExprMaybeRow(v.Exprs[1], row)
		return r, true, err
	case "aes_encrypt":
		result, err := e.evalAESEncrypt(v, row)
		return result, true, err
	case "aes_decrypt":
		result, err := e.evalAESDecrypt(v, row)
		return result, true, err
	case "uuid_to_bin":
		if len(v.Exprs) == 0 {
			return nil, true, nil
		}
		utbVal, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		uuidStr := toString(utbVal)
		// Strip surrounding braces if present
		uuidStr = strings.TrimLeft(uuidStr, "{")
		uuidStr = strings.TrimRight(uuidStr, "}")
		// Remove dashes
		hexStr := strings.ReplaceAll(uuidStr, "-", "")
		// Check for swap_flag (second argument)
		swapFlag := false
		if len(v.Exprs) >= 2 {
			swapVal, swapErr := e.evalExprMaybeRow(v.Exprs[1], row)
			if swapErr == nil && swapVal != nil {
				swapFlag = toInt64(swapVal) != 0
			}
		}
		// Decode hex to 16 binary bytes
		decoded, decErr := hex.DecodeString(hexStr)
		if decErr != nil || len(decoded) != 16 {
			return nil, true, fmt.Errorf("incorrect string value for uuid_to_bin")
		}
		// If swap_flag is set, swap time-low and time-high fields (MySQL time-ordered UUID)
		if swapFlag {
			// Swap: [0-3] (time_low) ↔ [6-7] (time_hi_and_version)
			// Original: time_low(4) time_mid(2) time_hi(2) ...
			// Swapped:  time_hi(2) time_mid(2) time_low(4) ...
			swapped := make([]byte, 16)
			copy(swapped[0:2], decoded[6:8])  // time_hi
			copy(swapped[2:4], decoded[4:6])  // time_mid
			copy(swapped[4:8], decoded[0:4])  // time_low
			copy(swapped[8:16], decoded[8:16]) // rest
			decoded = swapped
		}
		return string(decoded), true, nil
	case "bin_to_uuid":
		if len(v.Exprs) == 0 {
			return nil, true, nil
		}
		btuVal, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		// Decode binary input: HexBytes (x'...' literals) need to be decoded to raw bytes
		var btuBytes []byte
		if hb, ok := btuVal.(HexBytes); ok {
			decoded, decErr := hex.DecodeString(string(hb))
			if decErr != nil {
				return nil, true, fmt.Errorf("incorrect string value for bin_to_uuid")
			}
			btuBytes = decoded
		} else {
			btuBytes = []byte(toString(btuVal))
		}
		// Expect exactly 16 bytes
		if len(btuBytes) != 16 {
			return nil, true, fmt.Errorf("incorrect string value for bin_to_uuid")
		}
		btuStr := string(btuBytes)
		// Check for swap_flag (second argument)
		btuSwapFlag := false
		if len(v.Exprs) >= 2 {
			btuSwapVal, btuSwapErr := e.evalExprMaybeRow(v.Exprs[1], row)
			if btuSwapErr == nil && btuSwapVal != nil {
				btuSwapFlag = toInt64(btuSwapVal) != 0
			}
		}
		b := []byte(btuStr)
		if btuSwapFlag {
			// Reverse the time-ordered swap: time_hi(2) time_mid(2) time_low(4) → time_low(4) time_mid(2) time_hi(2)
			unswapped := make([]byte, 16)
			copy(unswapped[0:4], b[4:8])  // time_low
			copy(unswapped[4:6], b[2:4])  // time_mid
			copy(unswapped[6:8], b[0:2])  // time_hi
			copy(unswapped[8:16], b[8:16]) // rest
			b = unswapped
		}
		uuidStr := fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
			b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
		return uuidStr, true, nil
	case "to_base64":
		tb64Val, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		tb64Src := []byte(toString(tb64Val))
		const tb64Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
		var tb64Buf []byte
		for i := 0; i < len(tb64Src); i += 3 {
			var b0, b1, b2 byte
			b0 = tb64Src[i]
			if i+1 < len(tb64Src) {
				b1 = tb64Src[i+1]
			}
			if i+2 < len(tb64Src) {
				b2 = tb64Src[i+2]
			}
			tb64Buf = append(tb64Buf, tb64Chars[(b0>>2)&0x3F])
			tb64Buf = append(tb64Buf, tb64Chars[((b0<<4)|(b1>>4))&0x3F])
			if i+1 < len(tb64Src) {
				tb64Buf = append(tb64Buf, tb64Chars[((b1<<2)|(b2>>6))&0x3F])
			} else {
				tb64Buf = append(tb64Buf, '=')
			}
			if i+2 < len(tb64Src) {
				tb64Buf = append(tb64Buf, tb64Chars[b2&0x3F])
			} else {
				tb64Buf = append(tb64Buf, '=')
			}
		}
		return string(tb64Buf), true, nil
	case "from_base64":
		fb64Val, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		return toString(fb64Val), true, nil // simplified stub
	case "random_bytes":
		rbVal, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		rbN := int(toInt64(rbVal))
		if rbN <= 0 || rbN > 1024 {
			return nil, true, nil
		}
		rbBytes := make([]byte, rbN)
		rand.Read(rbBytes)
		return string(rbBytes), true, nil
	case "uncompress":
		return nil, true, nil
	case "compress":
		return nil, true, nil
	case "uncompressed_length":
		return int64(0), true, nil
	case "row_count":
		return int64(-1), true, nil
	case "uuid_short":
		return int64(rand.Int63()), true, nil
	case "is_uuid":
		iuVal, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return int64(0), true, nil
		}
		iuStr := toString(iuVal)
		iuRe := regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
		if iuRe.MatchString(iuStr) {
			return int64(1), true, nil
		}
		return int64(0), true, nil
	case "inet6_aton":
		i6aVal, i6aIsNull, i6aErr := e.evalArg1Quiet(v.Exprs, row)
		if i6aErr != nil {
			return nil, true, i6aErr
		}
		if i6aIsNull || i6aVal == nil {
			return nil, true, nil
		}
		i6aStr := toString(i6aVal)
		// Try parsing as IPv4-in-IPv6 (::ffff:x.x.x.x) or pure IPv6 or IPv4
		i6aIP := net.ParseIP(i6aStr)
		if i6aIP == nil {
			return nil, true, nil
		}
		// MySQL INET6_ATON returns 4-byte binary for IPv4 addresses,
		// 16-byte binary for IPv6 addresses.
		if i6aIP4 := i6aIP.To4(); i6aIP4 != nil {
			return string(i6aIP4), true, nil
		}
		i6aIP = i6aIP.To16()
		if i6aIP == nil {
			return nil, true, nil
		}
		return string(i6aIP), true, nil
	case "inet6_ntoa":
		i6nVal, i6nIsNull, i6nErr := e.evalArg1Quiet(v.Exprs, row)
		if i6nErr != nil {
			return nil, true, i6nErr
		}
		if i6nIsNull || i6nVal == nil {
			return nil, true, nil
		}
		i6nStr := toString(i6nVal)
		b := []byte(i6nStr)
		if len(b) == 4 {
			// IPv4
			return fmt.Sprintf("%d.%d.%d.%d", b[0], b[1], b[2], b[3]), true, nil
		} else if len(b) == 16 {
			// IPv6
			ip := net.IP(b)
			return ip.String(), true, nil
		}
		return nil, true, nil
	default:
		return nil, false, nil
	}
}

// parseBlockEncryptionMode parses the block_encryption_mode system variable.
// Returns (algorithm, keyLen, mode) e.g. ("aes", 128, "ecb").
func parseBlockEncryptionMode(modeStr string) (int, string) {
	// Format: "aes-<keylen>-<mode>" e.g. "aes-128-ecb"
	parts := strings.Split(strings.ToLower(modeStr), "-")
	if len(parts) != 3 {
		return 128, "ecb"
	}
	keyLen := 128
	switch parts[1] {
	case "192":
		keyLen = 192
	case "256":
		keyLen = 256
	}
	return keyLen, parts[2]
}

// aesBlockModeRequiresIV returns true if the block cipher mode requires an IV.
func aesBlockModeRequiresIV(mode string) bool {
	switch mode {
	case "cbc", "cfb1", "cfb8", "cfb128", "ofb":
		return true
	}
	return false
}

// aesKeySchedule derives the AES key from the user-provided key string,
// matching MySQL's key schedule algorithm (repeated XOR folding).
func aesKeySchedule(key []byte, keyLen int) []byte {
	keyBytes := keyLen / 8
	result := make([]byte, keyBytes)
	for i, b := range key {
		result[i%keyBytes] ^= b
	}
	return result
}

// pkcs7Pad pads data to blockSize using PKCS#7.
func pkcs7Pad(data []byte, blockSize int) []byte {
	padding := blockSize - len(data)%blockSize
	pad := make([]byte, padding)
	for i := range pad {
		pad[i] = byte(padding)
	}
	return append(data, pad...)
}

// pkcs7Unpad removes PKCS#7 padding.
func pkcs7Unpad(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty data")
	}
	padding := int(data[len(data)-1])
	if padding == 0 || padding > len(data) || padding > aes.BlockSize {
		return nil, fmt.Errorf("invalid padding")
	}
	for i := len(data) - padding; i < len(data); i++ {
		if data[i] != byte(padding) {
			return nil, fmt.Errorf("invalid padding")
		}
	}
	return data[:len(data)-padding], nil
}

func (e *Executor) evalAESEncrypt(v *sqlparser.FuncExpr, row *storage.Row) (interface{}, error) {
	modeStr, _ := e.getSysVar("block_encryption_mode")
	if modeStr == "" {
		modeStr = "aes-128-ecb"
	}
	keyLen, mode := parseBlockEncryptionMode(modeStr)

	// CFB/OFB modes require exactly 3 args (plaintext, key, iv)
	if aesBlockModeRequiresIV(mode) && len(v.Exprs) < 3 {
		return nil, mysqlError(1582, "42000",
			"Incorrect parameter count in the call to native function 'aes_encrypt'")
	}
	if len(v.Exprs) < 2 {
		return nil, mysqlError(1582, "42000",
			"Incorrect parameter count in the call to native function 'aes_encrypt'")
	}

	plainVal, err := e.evalExprMaybeRow(v.Exprs[0], row)
	if err != nil {
		return nil, err
	}
	keyVal, err := e.evalExprMaybeRow(v.Exprs[1], row)
	if err != nil {
		return nil, err
	}
	if plainVal == nil || keyVal == nil {
		return nil, nil
	}

	var iv []byte
	if len(v.Exprs) >= 3 {
		ivVal, err := e.evalExprMaybeRow(v.Exprs[2], row)
		if err != nil {
			return nil, err
		}
		if ivVal != nil {
			iv = []byte(toString(ivVal))
		}
	}

	plaintext := []byte(toString(plainVal))
	key := aesKeySchedule([]byte(toString(keyVal)), keyLen)

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, nil
	}

	var result string
	switch mode {
	case "ecb":
		padded := pkcs7Pad(plaintext, aes.BlockSize)
		ciphertext := make([]byte, len(padded))
		for i := 0; i < len(padded); i += aes.BlockSize {
			block.Encrypt(ciphertext[i:i+aes.BlockSize], padded[i:i+aes.BlockSize])
		}
		result = string(ciphertext)
	case "cbc":
		if len(iv) < aes.BlockSize {
			return nil, nil
		}
		padded := pkcs7Pad(plaintext, aes.BlockSize)
		ciphertext := make([]byte, len(padded))
		cbc := cipher.NewCBCEncrypter(block, iv[:aes.BlockSize])
		cbc.CryptBlocks(ciphertext, padded)
		result = string(ciphertext)
	case "cfb1":
		if len(iv) < aes.BlockSize {
			return nil, nil
		}
		ciphertext := aesCFB1Encrypt(block, iv[:aes.BlockSize], plaintext)
		result = string(ciphertext)
	case "cfb8":
		if len(iv) < aes.BlockSize {
			return nil, nil
		}
		ciphertext := aesCFB8Encrypt(block, iv[:aes.BlockSize], plaintext)
		result = string(ciphertext)
	case "cfb128":
		if len(iv) < aes.BlockSize {
			return nil, nil
		}
		ciphertext := make([]byte, len(plaintext))
		stream := cipher.NewCFBEncrypter(block, iv[:aes.BlockSize])
		stream.XORKeyStream(ciphertext, plaintext)
		result = string(ciphertext)
	case "ofb":
		if len(iv) < aes.BlockSize {
			return nil, nil
		}
		ciphertext := make([]byte, len(plaintext))
		stream := cipher.NewOFB(block, iv[:aes.BlockSize])
		stream.XORKeyStream(ciphertext, plaintext)
		result = string(ciphertext)
	default:
		return nil, nil
	}
	return result, nil
}

func (e *Executor) evalAESDecrypt(v *sqlparser.FuncExpr, row *storage.Row) (interface{}, error) {
	modeStr, _ := e.getSysVar("block_encryption_mode")
	if modeStr == "" {
		modeStr = "aes-128-ecb"
	}
	keyLen, mode := parseBlockEncryptionMode(modeStr)

	if aesBlockModeRequiresIV(mode) && len(v.Exprs) < 3 {
		return nil, mysqlError(1582, "42000",
			"Incorrect parameter count in the call to native function 'aes_decrypt'")
	}
	if len(v.Exprs) < 2 {
		return nil, mysqlError(1582, "42000",
			"Incorrect parameter count in the call to native function 'aes_decrypt'")
	}

	cipherVal, err := e.evalExprMaybeRow(v.Exprs[0], row)
	if err != nil {
		return nil, err
	}
	keyVal, err := e.evalExprMaybeRow(v.Exprs[1], row)
	if err != nil {
		return nil, err
	}
	if cipherVal == nil || keyVal == nil {
		return nil, nil
	}

	var iv []byte
	if len(v.Exprs) >= 3 {
		ivVal, err := e.evalExprMaybeRow(v.Exprs[2], row)
		if err != nil {
			return nil, err
		}
		if ivVal != nil {
			iv = []byte(toString(ivVal))
		}
	}

	ciphertext := []byte(toString(cipherVal))
	key := aesKeySchedule([]byte(toString(keyVal)), keyLen)

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, nil
	}

	switch mode {
	case "ecb":
		if len(ciphertext) == 0 || len(ciphertext)%aes.BlockSize != 0 {
			return nil, nil
		}
		plaintext := make([]byte, len(ciphertext))
		for i := 0; i < len(ciphertext); i += aes.BlockSize {
			block.Decrypt(plaintext[i:i+aes.BlockSize], ciphertext[i:i+aes.BlockSize])
		}
		unpadded, err := pkcs7Unpad(plaintext)
		if err != nil {
			return nil, nil
		}
		return string(unpadded), nil
	case "cbc":
		if len(iv) < aes.BlockSize {
			return nil, nil
		}
		if len(ciphertext) == 0 || len(ciphertext)%aes.BlockSize != 0 {
			return nil, nil
		}
		plaintext := make([]byte, len(ciphertext))
		cbc := cipher.NewCBCDecrypter(block, iv[:aes.BlockSize])
		cbc.CryptBlocks(plaintext, ciphertext)
		unpadded, err := pkcs7Unpad(plaintext)
		if err != nil {
			return nil, nil
		}
		return string(unpadded), nil
	case "cfb1":
		if len(iv) < aes.BlockSize {
			return nil, nil
		}
		plaintext := aesCFB1Decrypt(block, iv[:aes.BlockSize], ciphertext)
		return string(plaintext), nil
	case "cfb8":
		if len(iv) < aes.BlockSize {
			return nil, nil
		}
		plaintext := aesCFB8Decrypt(block, iv[:aes.BlockSize], ciphertext)
		return string(plaintext), nil
	case "cfb128":
		if len(iv) < aes.BlockSize {
			return nil, nil
		}
		plaintext := make([]byte, len(ciphertext))
		stream := cipher.NewCFBDecrypter(block, iv[:aes.BlockSize])
		stream.XORKeyStream(plaintext, ciphertext)
		return string(plaintext), nil
	case "ofb":
		if len(iv) < aes.BlockSize {
			return nil, nil
		}
		plaintext := make([]byte, len(ciphertext))
		stream := cipher.NewOFB(block, iv[:aes.BlockSize])
		stream.XORKeyStream(plaintext, ciphertext)
		return string(plaintext), nil
	default:
		return nil, nil
	}
}

// aesCFB1Encrypt implements CFB-1 mode encryption (1 bit at a time).
func aesCFB1Encrypt(block cipher.Block, iv, plaintext []byte) []byte {
	shift := make([]byte, aes.BlockSize)
	copy(shift, iv)
	ciphertext := make([]byte, len(plaintext))
	encrypted := make([]byte, aes.BlockSize)

	for i := 0; i < len(plaintext); i++ {
		var outByte byte
		for bit := 7; bit >= 0; bit-- {
			block.Encrypt(encrypted, shift)
			plaintextBit := (plaintext[i] >> uint(bit)) & 1
			cipherBit := plaintextBit ^ (encrypted[0] >> 7)
			outByte |= cipherBit << uint(bit)
			// Shift left by 1 bit, insert cipherBit at the end
			for j := 0; j < aes.BlockSize-1; j++ {
				shift[j] = (shift[j] << 1) | (shift[j+1] >> 7)
			}
			shift[aes.BlockSize-1] = (shift[aes.BlockSize-1] << 1) | cipherBit
		}
		ciphertext[i] = outByte
	}
	return ciphertext
}

// aesCFB1Decrypt implements CFB-1 mode decryption (1 bit at a time).
func aesCFB1Decrypt(block cipher.Block, iv, ciphertext []byte) []byte {
	shift := make([]byte, aes.BlockSize)
	copy(shift, iv)
	plaintext := make([]byte, len(ciphertext))
	encrypted := make([]byte, aes.BlockSize)

	for i := 0; i < len(ciphertext); i++ {
		var outByte byte
		for bit := 7; bit >= 0; bit-- {
			block.Encrypt(encrypted, shift)
			cipherBit := (ciphertext[i] >> uint(bit)) & 1
			plaintextBit := cipherBit ^ (encrypted[0] >> 7)
			outByte |= plaintextBit << uint(bit)
			// Shift left by 1 bit, insert cipherBit at the end
			for j := 0; j < aes.BlockSize-1; j++ {
				shift[j] = (shift[j] << 1) | (shift[j+1] >> 7)
			}
			shift[aes.BlockSize-1] = (shift[aes.BlockSize-1] << 1) | cipherBit
		}
		plaintext[i] = outByte
	}
	return plaintext
}

// aesCFB8Encrypt implements CFB-8 mode encryption (1 byte at a time).
func aesCFB8Encrypt(block cipher.Block, iv, plaintext []byte) []byte {
	shift := make([]byte, aes.BlockSize)
	copy(shift, iv)
	ciphertext := make([]byte, len(plaintext))
	encrypted := make([]byte, aes.BlockSize)

	for i := 0; i < len(plaintext); i++ {
		block.Encrypt(encrypted, shift)
		ciphertext[i] = plaintext[i] ^ encrypted[0]
		// Shift left by 1 byte, append ciphertext byte
		copy(shift, shift[1:])
		shift[aes.BlockSize-1] = ciphertext[i]
	}
	return ciphertext
}

// aesCFB8Decrypt implements CFB-8 mode decryption (1 byte at a time).
func aesCFB8Decrypt(block cipher.Block, iv, ciphertext []byte) []byte {
	shift := make([]byte, aes.BlockSize)
	copy(shift, iv)
	plaintext := make([]byte, len(ciphertext))
	encrypted := make([]byte, aes.BlockSize)

	for i := 0; i < len(ciphertext); i++ {
		block.Encrypt(encrypted, shift)
		plaintext[i] = ciphertext[i] ^ encrypted[0]
		// Shift left by 1 byte, append ciphertext byte
		copy(shift, shift[1:])
		shift[aes.BlockSize-1] = ciphertext[i]
	}
	return plaintext
}
