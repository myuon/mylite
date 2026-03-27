package executor

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/myuon/mylite/storage"
	"vitess.io/vitess/go/vt/sqlparser"
)

// evalMiscFunc dispatches miscellaneous functions from evalFuncExpr.
// Returns (result, handled, error).
func evalMiscFunc(e *Executor, name string, v *sqlparser.FuncExpr) (interface{}, bool, error) {
	switch name {
	case "last_insert_id":
		if len(v.Exprs) > 0 {
			val, err := e.evalExpr(v.Exprs[0])
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
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if val != nil {
			return val, true, nil
		}
		r, err := e.evalExpr(v.Exprs[1])
		return r, true, err
	case "coalesce":
		for _, argExpr := range v.Exprs {
			val, err := e.evalExpr(argExpr)
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
		cond, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if isTruthy(cond) {
			r, err := e.evalExpr(v.Exprs[1])
			return r, true, err
		}
		r, err := e.evalExpr(v.Exprs[2])
		return r, true, err
	case "isnull":
		val, isNull, err := e.evalArg1(v.Exprs, "ISNULL")
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
		v0, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		v1, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, true, err
		}
		if fmt.Sprintf("%v", v0) == fmt.Sprintf("%v", v1) {
			return nil, true, nil
		}
		return v0, true, nil
	case "cast", "convert":
		if len(v.Exprs) >= 1 {
			r, err := e.evalExpr(v.Exprs[0])
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
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return "binary", true, nil
		}
		colStr := ""
		if colName, ok := v.Exprs[0].(*sqlparser.ColName); ok {
			colStr = colName.Name.String()
			cs := e.getColumnCharset(colName)
			if cs != "" {
				return cs, true, nil
			}
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
		val, err := e.evalExpr(v.Exprs[0])
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
		return "utf8mb4_0900_ai_ci", true, nil
	case "least":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("LEAST requires at least 2 arguments")
		}
		var result interface{}
		allNull := true
		for _, argExpr := range v.Exprs {
			val, err := e.evalExpr(argExpr)
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
			val, err := e.evalExpr(argExpr)
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
		return "root@localhost", true, nil
	case "connection_id":
		return e.connectionID, true, nil
	case "found_rows":
		return e.lastFoundRows, true, nil
	case "interval":
		if len(v.Exprs) < 2 {
			return int64(-1), true, nil
		}
		ivN, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if ivN == nil {
			return int64(-1), true, nil
		}
		ivNF := toFloat(ivN)
		ivResult := int64(0)
		for ivi := 1; ivi < len(v.Exprs); ivi++ {
			ivVal, err := e.evalExpr(v.Exprs[ivi])
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
			dur, err := e.evalExpr(v.Exprs[0])
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
		return "root@localhost", true, nil
	case "regexp_like":
		rlVal, rlPat, hasNull, err := e.evalArgs2(v.Exprs, "REGEXP_LIKE")
		if err != nil {
			return nil, true, err
		}
		if hasNull {
			return nil, true, nil
		}
		rlFlags := ""
		if len(v.Exprs) >= 3 {
			rlFv, err := e.evalExpr(v.Exprs[2])
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
			return nil, true, mysqlError(3692, "HY000", "Illegal argument to a regular expression.")
		}
		if rlRe.MatchString(toString(rlVal)) {
			return int64(1), true, nil
		}
		return int64(0), true, nil
	case "is_ipv4":
		ipVal, isNull, err := e.evalArg1(v.Exprs, "IS_IPV4")
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
		inVal, isNull, err := e.evalArg1(v.Exprs, "INET_NTOA")
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		inN := uint32(toInt64(inVal))
		return fmt.Sprintf("%d.%d.%d.%d", (inN>>24)&0xFF, (inN>>16)&0xFF, (inN>>8)&0xFF, inN&0xFF), true, nil
	case "inet_aton":
		iaVal, isNull, err := e.evalArg1(v.Exprs, "INET_ATON")
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
		stVal, isNull, err := e.evalArg1Quiet(v.Exprs)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		return toString(stVal), true, nil
	case "st_equals":
		steA, steB, hasNull, err := e.evalArgs2(v.Exprs, "ST_EQUALS")
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
		g1Val, g2Val, hasNull, err := e.evalArgs2(v.Exprs, strings.ToUpper(name))
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
		ip6Val, isNull, err := e.evalArg1(v.Exprs, "IS_IPV6")
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
		imVal, isNull, err := e.evalArg1(v.Exprs, "IS_IPV4_MAPPED")
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
		icVal, isNull, err := e.evalArg1(v.Exprs, "IS_IPV4_COMPAT")
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
		shaVal, isNull, err := e.evalArg1(v.Exprs, "SHA")
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
		sdVal, isNull, err := e.evalArg1Quiet(v.Exprs)
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
		r, err := e.evalExpr(v.Exprs[1])
		return r, true, err
	case "aes_encrypt", "aes_decrypt":
		return nil, true, nil
	case "uuid_to_bin":
		utbVal, isNull, err := e.evalArg1Quiet(v.Exprs)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		return strings.ReplaceAll(toString(utbVal), "-", ""), true, nil
	case "bin_to_uuid":
		btuVal, isNull, err := e.evalArg1Quiet(v.Exprs)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		return toString(btuVal), true, nil
	case "to_base64":
		tb64Val, isNull, err := e.evalArg1Quiet(v.Exprs)
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
		fb64Val, isNull, err := e.evalArg1Quiet(v.Exprs)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		return toString(fb64Val), true, nil // simplified stub
	case "random_bytes":
		rbVal, isNull, err := e.evalArg1Quiet(v.Exprs)
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
		iuVal, isNull, err := e.evalArg1Quiet(v.Exprs)
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
		return nil, true, nil
	case "inet6_ntoa":
		return nil, true, nil
	default:
		return nil, false, nil
	}
}

// evalMiscFuncWithRow dispatches miscellaneous functions from evalFuncExprWithRow.
func evalMiscFuncWithRow(e *Executor, name string, v *sqlparser.FuncExpr, row storage.Row, evalArgs func() ([]interface{}, error)) (interface{}, bool, error) {
	switch name {
	case "last_insert_id":
		if len(v.Exprs) > 0 {
			args, err := evalArgs()
			if err != nil {
				return nil, true, err
			}
			e.lastInsertID = toInt64(args[0])
			return e.lastInsertID, true, nil
		}
		return e.lastInsertID, true, nil
	case "if":
		if len(v.Exprs) < 3 {
			return nil, true, fmt.Errorf("IF requires 3 arguments")
		}
		cond, err := e.evalRowExpr(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if isTruthy(cond) {
			r, err := e.evalRowExpr(v.Exprs[1], row)
			return r, true, err
		}
		r, err := e.evalRowExpr(v.Exprs[2], row)
		return r, true, err
	case "ifnull", "nvl":
		if len(v.Exprs) < 2 {
			return nil, true, nil
		}
		val, err := e.evalRowExpr(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if val != nil {
			return val, true, nil
		}
		r, err := e.evalRowExpr(v.Exprs[1], row)
		return r, true, err
	case "coalesce":
		for _, argExpr := range v.Exprs {
			val, err := e.evalRowExpr(argExpr, row)
			if err != nil {
				return nil, true, err
			}
			if val != nil {
				return val, true, nil
			}
		}
		return nil, true, nil
	case "isnull":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return int64(1), true, nil
		}
		return int64(0), true, nil
	case "charset":
		if len(v.Exprs) < 1 {
			return nil, true, nil
		}
		if cue, ok := v.Exprs[0].(*sqlparser.ConvertUsingExpr); ok {
			return strings.ToLower(cue.Type), true, nil
		}
		val, err := e.evalRowExpr(v.Exprs[0], row)
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
		if cn, ok := v.Exprs[0].(*sqlparser.ColName); ok {
			colStr = cn.Name.String()
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
	default:
		return nil, false, nil
	}
}
