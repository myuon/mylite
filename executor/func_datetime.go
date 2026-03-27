package executor

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/myuon/mylite/storage"
	"vitess.io/vitess/go/vt/sqlparser"
)

// evalDatetimeFunc dispatches date/time-related functions from evalFuncExpr.
// Returns (result, handled, error).
func evalDatetimeFunc(e *Executor, name string, v *sqlparser.FuncExpr) (interface{}, bool, error) {
	switch name {
	case "now", "current_timestamp", "sysdate":
		return e.nowTime().Format("2006-01-02 15:04:05"), true, nil
	case "curdate", "current_date":
		return e.nowTime().Format("2006-01-02"), true, nil
	case "curtime", "current_time":
		return e.nowTime().Format("15:04:05"), true, nil
	case "utc_date":
		return e.nowTime().UTC().Format("2006-01-02"), true, nil
	case "utc_time":
		return e.nowTime().UTC().Format("15:04:05"), true, nil
	case "utc_timestamp":
		return e.nowTime().UTC().Format("2006-01-02 15:04:05"), true, nil
	case "unix_timestamp":
		if len(v.Exprs) == 0 {
			return int64(e.nowTime().Unix()), true, nil
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, true, nil
		}
		return int64(t.Unix()), true, nil
	case "from_unixtime":
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("FROM_UNIXTIME requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		ts := toInt64(val)
		t := time.Unix(ts, 0)
		if len(v.Exprs) >= 2 {
			fmtVal, err := e.evalExpr(v.Exprs[1])
			if err != nil {
				return nil, true, err
			}
			return mysqlDateFormat(t, toString(fmtVal)), true, nil
		}
		return t.Format("2006-01-02 15:04:05"), true, nil
	case "year":
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("YEAR requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, true, nil
		}
		return int64(t.Year()), true, nil
	case "month":
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("MONTH requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, true, nil
		}
		return int64(t.Month()), true, nil
	case "day", "dayofmonth":
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("DAY requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, true, nil
		}
		return int64(t.Day()), true, nil
	case "hour":
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("HOUR requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, true, nil
		}
		return int64(t.Hour()), true, nil
	case "minute":
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("MINUTE requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, true, nil
		}
		return int64(t.Minute()), true, nil
	case "second":
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("SECOND requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, true, nil
		}
		return int64(t.Second()), true, nil
	case "date":
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("DATE requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
		}
		if isZeroDate(val) {
			return "0000-00-00", true, nil
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, true, nil
		}
		return t.Format("2006-01-02"), true, nil
	case "time":
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("TIME requires 1 argument")
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, true, nil
		}
		return t.Format("15:04:05"), true, nil
	case "datediff":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("DATEDIFF requires 2 arguments")
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
		t0, err := parseDateTimeValue(v0)
		if err != nil {
			return nil, true, nil
		}
		t1, err := parseDateTimeValue(v1)
		if err != nil {
			return nil, true, nil
		}
		d0 := time.Date(t0.Year(), t0.Month(), t0.Day(), 0, 0, 0, 0, time.UTC)
		d1 := time.Date(t1.Year(), t1.Month(), t1.Day(), 0, 0, 0, 0, time.UTC)
		diff := int64(d0.Sub(d1).Hours() / 24)
		return diff, true, nil
	case "date_format":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("DATE_FORMAT requires 2 arguments")
		}
		dateVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		fmtVal, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, true, err
		}
		t, err := parseDateTimeValue(dateVal)
		if err != nil {
			return nil, true, nil
		}
		return mysqlDateFormat(t, toString(fmtVal)), true, nil
	case "str_to_date":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("STR_TO_DATE requires 2 arguments")
		}
		strVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		fmtVal2, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, true, err
		}
		if strVal == nil || fmtVal2 == nil {
			return nil, true, nil
		}
		parsed := mysqlStrToDate(toString(strVal), toString(fmtVal2))
		if parsed == nil {
			return nil, true, nil
		}
		return *parsed, true, nil
	case "get_format":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("GET_FORMAT requires 2 arguments")
		}
		typeVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		localeVal, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, true, err
		}
		if typeVal == nil || localeVal == nil {
			return nil, true, nil
		}
		return mysqlGetFormat(strings.ToUpper(toString(typeVal)), strings.ToUpper(toString(localeVal))), true, nil
	case "dayname":
		if len(v.Exprs) < 1 {
			return nil, true, nil
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if isZeroDate(val) {
			return nil, true, nil
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, true, nil
		}
		return t.Format("Monday"), true, nil
	case "dayofweek":
		if len(v.Exprs) < 1 {
			return nil, true, nil
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if isZeroDate(val) {
			return nil, true, nil
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, true, nil
		}
		return int64(t.Weekday()) + 1, true, nil
	case "weekday":
		if len(v.Exprs) < 1 {
			return nil, true, nil
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if isZeroDate(val) {
			return nil, true, nil
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, true, nil
		}
		wd := int64(t.Weekday()) - 1
		if wd < 0 {
			wd = 6
		}
		return wd, true, nil
	case "dayofyear":
		if len(v.Exprs) < 1 {
			return nil, true, nil
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if isZeroDate(val) {
			return nil, true, nil
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, true, nil
		}
		return int64(t.YearDay()), true, nil
	case "monthname":
		if len(v.Exprs) < 1 {
			return nil, true, nil
		}
		val, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, true, nil
		}
		return t.Format("January"), true, nil
	case "addtime":
		if len(v.Exprs) < 2 {
			return nil, true, nil
		}
		base, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		interval, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, true, err
		}
		t, err := parseDateTimeValue(base)
		if err != nil {
			return toString(base), true, nil
		}
		dur, err := parseMySQLTimeInterval(toString(interval))
		if err != nil {
			return toString(base), true, nil
		}
		return t.Add(dur).Format("2006-01-02 15:04:05"), true, nil
	case "subtime":
		if len(v.Exprs) < 2 {
			return nil, true, nil
		}
		base, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		interval, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, true, err
		}
		t, err := parseDateTimeValue(base)
		if err != nil {
			return toString(base), true, nil
		}
		dur, err := parseMySQLTimeInterval(toString(interval))
		if err != nil {
			return toString(base), true, nil
		}
		return t.Add(-dur).Format("2006-01-02 15:04:05"), true, nil
	case "from_days":
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
		days := int(toInt64(val))
		if days <= 0 {
			return "0000-00-00", true, nil
		}
		t := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, days-1)
		return t.Format("2006-01-02"), true, nil
	case "to_days":
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
		if isZeroDate(val) {
			return nil, true, nil
		}
		t, parseErr := parseDateTimeValue(val)
		if parseErr != nil {
			return nil, true, nil
		}
		return mysqlToDays(t), true, nil
	case "last_day":
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
		t, parseErr := parseDateTimeValue(val)
		if parseErr != nil {
			return nil, true, nil
		}
		firstOfNextMonth := time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, time.UTC)
		lastDay := firstOfNextMonth.AddDate(0, 0, -1)
		return lastDay.Format("2006-01-02"), true, nil
	case "quarter":
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
		t, parseErr := parseDateTimeValue(val)
		if parseErr != nil {
			return nil, true, nil
		}
		return int64((t.Month()-1)/3 + 1), true, nil
	case "week":
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
		if isZeroDate(val) {
			return nil, true, nil
		}
		t, parseErr := parseDateTimeValue(val)
		if parseErr != nil {
			return nil, true, nil
		}
		mode := int64(0)
		if len(v.Exprs) >= 2 {
			if modeVal, modeErr := e.evalExpr(v.Exprs[1]); modeErr == nil && modeVal != nil {
				mode = toInt64(modeVal)
			}
		}
		if mode == 0 {
			return mysqlWeekMode0(t), true, nil
		}
		_, wk := t.ISOWeek()
		return int64(wk), true, nil
	case "weekofyear":
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
		if isZeroDate(val) {
			return nil, true, nil
		}
		t, parseErr := parseDateTimeValue(val)
		if parseErr != nil {
			return nil, true, nil
		}
		_, wk := t.ISOWeek()
		return int64(wk), true, nil
	case "yearweek":
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
		t, parseErr := parseDateTimeValue(val)
		if parseErr != nil {
			return nil, true, nil
		}
		yr, wk := mysqlYearWeek(t, 0)
		return int64(yr*100 + wk), true, nil
	case "timestamp":
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
		t, parseErr := parseDateTimeValue(val)
		if parseErr != nil {
			return nil, true, nil
		}
		return t.Format("2006-01-02 15:04:05"), true, nil
	case "sec_to_time":
		if len(v.Exprs) < 1 {
			return nil, true, nil
		}
		arg, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if arg == nil {
			return nil, true, nil
		}
		return secToTimeValue(arg), true, nil
	case "time_to_sec":
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("TIME_TO_SEC requires 1 argument")
		}
		ttsVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if ttsVal == nil {
			return nil, true, nil
		}
		ttsS := toString(ttsVal)
		ttsNeg := false
		if strings.HasPrefix(ttsS, "-") {
			ttsNeg = true
			ttsS = ttsS[1:]
		}
		ttsParts := strings.Split(ttsS, ":")
		var ttsSecs int64
		switch len(ttsParts) {
		case 3:
			ttsH, _ := strconv.ParseInt(ttsParts[0], 10, 64)
			ttsM, _ := strconv.ParseInt(ttsParts[1], 10, 64)
			ttsSec, _ := strconv.ParseFloat(ttsParts[2], 64)
			ttsSecs = ttsH*3600 + ttsM*60 + int64(ttsSec)
		case 2:
			ttsM, _ := strconv.ParseInt(ttsParts[0], 10, 64)
			ttsSec, _ := strconv.ParseFloat(ttsParts[1], 64)
			ttsSecs = ttsM*60 + int64(ttsSec)
		default:
			ttsSec, _ := strconv.ParseFloat(ttsS, 64)
			ttsSecs = int64(ttsSec)
		}
		if ttsNeg {
			ttsSecs = -ttsSecs
		}
		return ttsSecs, true, nil
	case "period_add":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("PERIOD_ADD requires 2 arguments")
		}
		paP, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		paN, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, true, err
		}
		if paP == nil || paN == nil {
			return nil, true, nil
		}
		paPeriod := toInt64(paP)
		paMonths := toInt64(paN)
		if paPeriod <= 0 {
			return int64(0), true, nil
		}
		paYear := paPeriod / 100
		paMonth := paPeriod % 100
		if paYear < 70 {
			paYear += 2000
		} else if paYear < 100 {
			paYear += 1900
		}
		paTotalM := paYear*12 + paMonth - 1 + paMonths
		paNewY := paTotalM / 12
		paNewM := paTotalM%12 + 1
		return paNewY*100 + paNewM, true, nil
	case "period_diff":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("PERIOD_DIFF requires 2 arguments")
		}
		pdP1, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		pdP2, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, true, err
		}
		if pdP1 == nil || pdP2 == nil {
			return nil, true, nil
		}
		pdToMonths := func(period int64) int64 {
			pdY := period / 100
			pdM := period % 100
			if pdY < 70 {
				pdY += 2000
			} else if pdY < 100 {
				pdY += 1900
			}
			return pdY*12 + pdM
		}
		return pdToMonths(toInt64(pdP1)) - pdToMonths(toInt64(pdP2)), true, nil
	case "maketime":
		if len(v.Exprs) < 3 {
			return nil, true, fmt.Errorf("MAKETIME requires 3 arguments")
		}
		mtH, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		mtM, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, true, err
		}
		mtSec, err := e.evalExpr(v.Exprs[2])
		if err != nil {
			return nil, true, err
		}
		if mtH == nil || mtM == nil || mtSec == nil {
			return nil, true, nil
		}
		mtHi := toInt64(mtH)
		mtMi := toInt64(mtM)
		mtSi := toInt64(mtSec)
		if mtMi < 0 || mtMi > 59 || mtSi < 0 || mtSi > 59 {
			return nil, true, nil
		}
		mtNeg := ""
		if mtHi < 0 {
			mtNeg = "-"
			mtHi = -mtHi
		}
		return fmt.Sprintf("%s%02d:%02d:%02d", mtNeg, mtHi, mtMi, mtSi), true, nil
	case "microsecond":
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("MICROSECOND requires 1 argument")
		}
		usVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if usVal == nil {
			return nil, true, nil
		}
		usStr := toString(usVal)
		if usDot := strings.LastIndex(usStr, "."); usDot >= 0 {
			usFrac := usStr[usDot+1:]
			for len(usFrac) < 6 {
				usFrac += "0"
			}
			usN, _ := strconv.ParseInt(usFrac[:6], 10, 64)
			return usN, true, nil
		}
		return int64(0), true, nil
	case "time_format":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("TIME_FORMAT requires 2 arguments")
		}
		tfTime, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		tfFmt, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, true, err
		}
		if tfTime == nil || tfFmt == nil {
			return nil, true, nil
		}
		tfStr := toString(tfTime)
		tfParts := strings.Split(tfStr, ":")
		tfH, tfM, tfSec := 0, 0, 0
		if len(tfParts) >= 1 {
			tfH, _ = strconv.Atoi(tfParts[0])
		}
		if len(tfParts) >= 2 {
			tfM, _ = strconv.Atoi(tfParts[1])
		}
		if len(tfParts) >= 3 {
			tfSec, _ = strconv.Atoi(tfParts[2])
		}
		tfResult := toString(tfFmt)
		tfResult = strings.ReplaceAll(tfResult, "%H", fmt.Sprintf("%02d", tfH))
		tfResult = strings.ReplaceAll(tfResult, "%k", fmt.Sprintf("%d", tfH))
		tfResult = strings.ReplaceAll(tfResult, "%i", fmt.Sprintf("%02d", tfM))
		tfResult = strings.ReplaceAll(tfResult, "%S", fmt.Sprintf("%02d", tfSec))
		tfResult = strings.ReplaceAll(tfResult, "%s", fmt.Sprintf("%02d", tfSec))
		tfH12 := tfH % 12
		if tfH12 == 0 {
			tfH12 = 12
		}
		tfResult = strings.ReplaceAll(tfResult, "%h", fmt.Sprintf("%02d", tfH12))
		tfResult = strings.ReplaceAll(tfResult, "%l", fmt.Sprintf("%d", tfH12))
		if tfH < 12 {
			tfResult = strings.ReplaceAll(tfResult, "%p", "AM")
		} else {
			tfResult = strings.ReplaceAll(tfResult, "%p", "PM")
		}
		return tfResult, true, nil
	case "convert_tz":
		if len(v.Exprs) < 3 {
			return nil, true, fmt.Errorf("CONVERT_TZ requires 3 arguments")
		}
		ctzVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if ctzVal == nil {
			return nil, true, nil
		}
		return toString(ctzVal), true, nil
	case "timediff":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("TIMEDIFF requires 2 arguments")
		}
		tdA, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		tdB, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, true, err
		}
		if tdA == nil || tdB == nil {
			return nil, true, nil
		}
		tdTA, tdErr1 := parseDateTimeValue(toString(tdA))
		tdTB, tdErr2 := parseDateTimeValue(toString(tdB))
		if tdErr1 != nil || tdErr2 != nil {
			return nil, true, nil
		}
		tdDiff := tdTA.Sub(tdTB)
		tdNeg := ""
		if tdDiff < 0 {
			tdNeg = "-"
			tdDiff = -tdDiff
		}
		tdH := int(tdDiff.Hours())
		tdM := int(tdDiff.Minutes()) % 60
		tdS := int(tdDiff.Seconds()) % 60
		return fmt.Sprintf("%s%02d:%02d:%02d", tdNeg, tdH, tdM, tdS), true, nil
	case "to_seconds":
		if len(v.Exprs) < 1 {
			return nil, true, nil
		}
		tsVal, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		if tsVal == nil {
			return nil, true, nil
		}
		tsT, tsErr := parseDateTimeValue(toString(tsVal))
		if tsErr != nil {
			return nil, true, nil
		}
		tsDays := int64(tsT.Year())*365 + int64(tsT.YearDay()) + int64(tsT.Year())/4 - int64(tsT.Year())/100 + int64(tsT.Year())/400
		return tsDays*86400 + int64(tsT.Hour())*3600 + int64(tsT.Minute())*60 + int64(tsT.Second()), true, nil
	case "makedate":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("MAKEDATE requires 2 arguments")
		}
		mdYear, err := e.evalExpr(v.Exprs[0])
		if err != nil {
			return nil, true, err
		}
		mdDay, err := e.evalExpr(v.Exprs[1])
		if err != nil {
			return nil, true, err
		}
		if mdYear == nil || mdDay == nil {
			return nil, true, nil
		}
		mdY := int(toInt64(mdYear))
		mdD := int(toInt64(mdDay))
		if mdD <= 0 || mdY < 0 || mdY > 9999 {
			return nil, true, nil
		}
		mdT := time.Date(mdY, 1, mdD, 0, 0, 0, 0, time.UTC)
		return mdT.Format("2006-01-02"), true, nil
	default:
		return nil, false, nil
	}
}

// evalDatetimeFuncWithRow dispatches date/time-related functions from evalFuncExprWithRow.
func evalDatetimeFuncWithRow(e *Executor, name string, v *sqlparser.FuncExpr, row storage.Row, evalArgs func() ([]interface{}, error)) (interface{}, bool, error) {
	switch name {
	case "date":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		if isZeroDate(args[0]) {
			return "0000-00-00", true, nil
		}
		t, err := parseDateTimeValue(args[0])
		if err != nil {
			return nil, true, nil
		}
		return t.Format("2006-01-02"), true, nil
	case "year":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		if isZeroDate(args[0]) {
			return int64(0), true, nil
		}
		t, err := parseDateTimeValue(args[0])
		if err != nil {
			return nil, true, nil
		}
		return int64(t.Year()), true, nil
	case "month":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		if isZeroDate(args[0]) {
			return int64(0), true, nil
		}
		t, err := parseDateTimeValue(args[0])
		if err != nil {
			return nil, true, nil
		}
		return int64(t.Month()), true, nil
	case "day", "dayofmonth":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		if isZeroDate(args[0]) {
			return int64(0), true, nil
		}
		t, err := parseDateTimeValue(args[0])
		if err != nil {
			return nil, true, nil
		}
		return int64(t.Day()), true, nil
	case "hour":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		t, err := parseDateTimeValue(args[0])
		if err != nil {
			return nil, true, nil
		}
		return int64(t.Hour()), true, nil
	case "minute":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		t, err := parseDateTimeValue(args[0])
		if err != nil {
			return nil, true, nil
		}
		return int64(t.Minute()), true, nil
	case "second":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		t, err := parseDateTimeValue(args[0])
		if err != nil {
			return nil, true, nil
		}
		return int64(t.Second()), true, nil
	case "dayname":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		if isZeroDate(args[0]) {
			return nil, true, nil
		}
		t, err := parseDateTimeValue(args[0])
		if err != nil {
			return nil, true, nil
		}
		return t.Format("Monday"), true, nil
	case "dayofweek":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		if isZeroDate(args[0]) {
			return nil, true, nil
		}
		t, err := parseDateTimeValue(args[0])
		if err != nil {
			return nil, true, nil
		}
		return int64(t.Weekday()) + 1, true, nil
	case "dayofyear":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		if isZeroDate(args[0]) {
			return nil, true, nil
		}
		t, err := parseDateTimeValue(args[0])
		if err != nil {
			return nil, true, nil
		}
		return int64(t.YearDay()), true, nil
	case "monthname":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		if isZeroDate(args[0]) {
			return nil, true, nil
		}
		t, err := parseDateTimeValue(args[0])
		if err != nil {
			return nil, true, nil
		}
		return t.Format("January"), true, nil
	case "weekday":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		if isZeroDate(args[0]) {
			return nil, true, nil
		}
		t, err := parseDateTimeValue(args[0])
		if err != nil {
			return nil, true, nil
		}
		wd := int64(t.Weekday()) - 1
		if wd < 0 {
			wd = 6
		}
		return wd, true, nil
	case "time":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		t, err := parseDateTimeValue(args[0])
		if err != nil {
			return nil, true, nil
		}
		return t.Format("15:04:05"), true, nil
	case "from_days":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		days := int(toInt64(args[0]))
		if days <= 0 {
			return "0000-00-00", true, nil
		}
		t := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, days-1)
		return t.Format("2006-01-02"), true, nil
	case "to_days":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		if isZeroDate(args[0]) {
			return nil, true, nil
		}
		t, parseErr := parseDateTimeValue(args[0])
		if parseErr != nil {
			return nil, true, nil
		}
		return mysqlToDays(t), true, nil
	case "last_day":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		if isZeroDate(args[0]) {
			return nil, true, nil
		}
		t, parseErr := parseDateTimeValue(args[0])
		if parseErr != nil {
			return nil, true, nil
		}
		firstOfNextMonth := time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, time.UTC)
		lastDay := firstOfNextMonth.AddDate(0, 0, -1)
		return lastDay.Format("2006-01-02"), true, nil
	case "quarter":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		if isZeroDate(args[0]) {
			return int64(0), true, nil
		}
		t, parseErr := parseDateTimeValue(args[0])
		if parseErr != nil {
			return nil, true, nil
		}
		return int64((t.Month()-1)/3 + 1), true, nil
	case "week":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		if isZeroDate(args[0]) {
			return nil, true, nil
		}
		t, parseErr := parseDateTimeValue(args[0])
		if parseErr != nil {
			return nil, true, nil
		}
		mode := int64(0)
		if len(args) >= 2 && args[1] != nil {
			mode = toInt64(args[1])
		}
		if mode == 0 {
			return mysqlWeekMode0(t), true, nil
		}
		_, wk := t.ISOWeek()
		return int64(wk), true, nil
	case "weekofyear":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		if isZeroDate(args[0]) {
			return nil, true, nil
		}
		t, parseErr := parseDateTimeValue(args[0])
		if parseErr != nil {
			return nil, true, nil
		}
		_, wk := t.ISOWeek()
		return int64(wk), true, nil
	case "yearweek":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		if isZeroDate(args[0]) {
			return nil, true, nil
		}
		t, parseErr := parseDateTimeValue(args[0])
		if parseErr != nil {
			return nil, true, nil
		}
		yr, wk := mysqlYearWeek(t, 0)
		return int64(yr*100 + wk), true, nil
	case "timestamp":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		if isZeroDate(args[0]) {
			return "0000-00-00 00:00:00", true, nil
		}
		t, parseErr := parseDateTimeValue(args[0])
		if parseErr != nil {
			return nil, true, nil
		}
		return t.Format("2006-01-02 15:04:05"), true, nil
	case "sec_to_time":
		args, err := evalArgs()
		if err != nil {
			return nil, true, err
		}
		if len(args) < 1 || args[0] == nil {
			return nil, true, nil
		}
		return secToTimeValue(args[0]), true, nil
	default:
		return nil, false, nil
	}
}
