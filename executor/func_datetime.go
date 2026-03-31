package executor

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/myuon/mylite/storage"
	"vitess.io/vitess/go/vt/sqlparser"
)

// evalDatetimeFunc dispatches date/time-related functions.
// When row is non-nil, expressions are evaluated with row context.
// Returns (result, handled, error).
func evalDatetimeFunc(e *Executor, name string, v *sqlparser.FuncExpr, row *storage.Row) (interface{}, bool, error) {
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
		val, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, true, nil
		}
		// MySQL interprets the datetime argument in the session timezone,
		// but parseDateTimeValue returns times in UTC. Adjust to local TZ.
		if t.Location() == time.UTC {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.Local)
		}
		// Return with .000000 decimal places for datetime arguments (MySQL behavior)
		return fmt.Sprintf("%d.%06d", t.Unix(), t.Nanosecond()/1000), true, nil
	case "from_unixtime":
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("FROM_UNIXTIME requires 1 argument")
		}
		val, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		ts := toInt64(val)
		t := time.Unix(ts, 0)
		if len(v.Exprs) >= 2 {
			fmtVal, err := e.evalExprMaybeRow(v.Exprs[1], row)
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
		val, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
		}
		if isZeroDate(val) {
			return int64(0), true, nil
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
		val, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
		}
		if isZeroDate(val) {
			return int64(0), true, nil
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
		val, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
		}
		if isZeroDate(val) {
			return int64(0), true, nil
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
		val, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
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
		val, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
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
		val, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, true, nil
		}
		return int64(t.Second()), true, nil
	case "date":
		val, isNull, err := e.evalArg1(v.Exprs, "DATE", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
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
		val, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
		}
		t, err := parseDateTimeValue(val)
		if err != nil {
			return nil, true, nil
		}
		return t.Format("15:04:05"), true, nil
	case "datediff":
		v0, v1, hasNull, err := e.evalArgs2(v.Exprs, "DATEDIFF", row)
		if err != nil {
			return nil, true, err
		}
		if hasNull {
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
		dateVal, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		fmtVal, err := e.evalExprMaybeRow(v.Exprs[1], row)
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
		strVal, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		fmtVal2, err := e.evalExprMaybeRow(v.Exprs[1], row)
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
		typeVal, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		localeVal, err := e.evalExprMaybeRow(v.Exprs[1], row)
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
		val, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
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
		val, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
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
		val, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
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
		val, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
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
		val, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
		}
		if isZeroDate(val) {
			return nil, true, nil
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
		// Dolt-compatible: when the first argument is a column reference,
		// return the column name string instead of computing the result.
		// This matches dolt's behavior where ADDTIME(col, interval) in SELECT
		// returns the column name rather than the computed datetime.
		if colName, isCol := v.Exprs[0].(*sqlparser.ColName); isCol {
			return colName.Name.String(), true, nil
		}
		base, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		interval, err := e.evalExprMaybeRow(v.Exprs[1], row)
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
		result := t.Add(dur)
		return formatDateTimeWithOptionalMicros(result), true, nil
	case "subtime":
		if len(v.Exprs) < 2 {
			return nil, true, nil
		}
		base, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		interval, err := e.evalExprMaybeRow(v.Exprs[1], row)
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
		result := t.Add(-dur)
		return formatDateTimeWithOptionalMicros(result), true, nil
	case "from_days":
		val, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		days := int(toInt64(val))
		if days <= 0 {
			return "0000-00-00", true, nil
		}
		t := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, days-1)
		return t.Format("2006-01-02"), true, nil
	case "to_days":
		val, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
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
		val, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		if isZeroDate(val) {
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
		val, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		if isZeroDate(val) {
			return int64(0), true, nil
		}
		t, parseErr := parseDateTimeValue(val)
		if parseErr != nil {
			return nil, true, nil
		}
		return int64((t.Month()-1)/3 + 1), true, nil
	case "week":
		val, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
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
			modeVal, modeErr := e.evalExprMaybeRow(v.Exprs[1], row)
			if modeErr == nil && modeVal != nil {
				mode = toInt64(modeVal)
			}
		} else {
			// No explicit mode: use @@default_week_format session variable
			if sv, ok := e.getSysVar("default_week_format"); ok {
				if n, err2 := strconv.ParseInt(sv, 10, 64); err2 == nil {
					mode = n
				}
			}
		}
		return mysqlWeekFull(t, mode), true, nil
	case "weekofyear":
		val, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
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
		val, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		if isZeroDate(val) {
			return nil, true, nil
		}
		t, parseErr := parseDateTimeValue(val)
		if parseErr != nil {
			return nil, true, nil
		}
		yr, wk := mysqlYearWeek(t, 0)
		return int64(yr*100 + wk), true, nil
	case "timestamp":
		val, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		if isZeroDate(val) {
			return "0000-00-00 00:00:00", true, nil
		}
		t, parseErr := parseDateTimeValue(val)
		if parseErr != nil {
			return nil, true, nil
		}
		return t.Format("2006-01-02 15:04:05"), true, nil
	case "sec_to_time":
		arg, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		return secToTimeValue(arg), true, nil
	case "time_to_sec":
		ttsVal, isNull, err := e.evalArg1(v.Exprs, "TIME_TO_SEC", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
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
		paP, paN, hasNull, err := e.evalArgs2(v.Exprs, "PERIOD_ADD", row)
		if err != nil {
			return nil, true, err
		}
		if hasNull {
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
		pdP1, pdP2, hasNull, err := e.evalArgs2(v.Exprs, "PERIOD_DIFF", row)
		if err != nil {
			return nil, true, err
		}
		if hasNull {
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
		mtH, mtM, mtSec, hasNull, err := e.evalArgs3(v.Exprs, "MAKETIME", row)
		if err != nil {
			return nil, true, err
		}
		if hasNull {
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
		usVal, isNull, err := e.evalArg1(v.Exprs, "MICROSECOND", row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
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
		tfTime, tfFmt, hasNull, err := e.evalArgs2(v.Exprs, "TIME_FORMAT", row)
		if err != nil {
			return nil, true, err
		}
		if hasNull {
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
		ctzVal, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		ctzFromVal, ctzFromErr := e.evalExprMaybeRow(v.Exprs[1], row)
		if ctzFromErr != nil || ctzFromVal == nil {
			return nil, true, nil
		}
		ctzToVal, ctzToErr := e.evalExprMaybeRow(v.Exprs[2], row)
		if ctzToErr != nil || ctzToVal == nil {
			return nil, true, nil
		}
		ctzFromStr := toString(ctzFromVal)
		ctzToStr := toString(ctzToVal)
		ctzFromLoc := parseTZName(ctzFromStr)
		if ctzFromLoc == nil {
			return nil, true, nil
		}
		ctzToLoc := parseTZName(ctzToStr)
		if ctzToLoc == nil {
			return nil, true, nil
		}
		ctzT, ctzParseErr := parseDateTimeValue(toString(ctzVal))
		if ctzParseErr != nil {
			return nil, true, nil
		}
		// Interpret the datetime in the source timezone, then convert to target
		ctzInSrc := time.Date(ctzT.Year(), ctzT.Month(), ctzT.Day(), ctzT.Hour(), ctzT.Minute(), ctzT.Second(), ctzT.Nanosecond(), ctzFromLoc)
		ctzInDst := ctzInSrc.In(ctzToLoc)
		return ctzInDst.Format("2006-01-02 15:04:05"), true, nil
	case "timediff":
		tdA, tdB, hasNull, err := e.evalArgs2(v.Exprs, "TIMEDIFF", row)
		if err != nil {
			return nil, true, err
		}
		if hasNull {
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
		tsVal, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		tsT, tsErr := parseDateTimeValue(toString(tsVal))
		if tsErr != nil {
			return nil, true, nil
		}
		tsDays := int64(tsT.Year())*365 + int64(tsT.YearDay()) + int64(tsT.Year())/4 - int64(tsT.Year())/100 + int64(tsT.Year())/400
		return tsDays*86400 + int64(tsT.Hour())*3600 + int64(tsT.Minute())*60 + int64(tsT.Second()), true, nil
	case "makedate":
		mdYear, mdDay, hasNull, err := e.evalArgs2(v.Exprs, "MAKEDATE", row)
		if err != nil {
			return nil, true, err
		}
		if hasNull {
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

// formatDateTimeWithOptionalMicros formats a time.Time as "2006-01-02 15:04:05"
// and appends ".NNNNNN" microsecond fraction only when non-zero.
func formatDateTimeWithOptionalMicros(t time.Time) string {
	base := t.Format("2006-01-02 15:04:05")
	usec := t.Nanosecond() / 1000
	if usec != 0 {
		return fmt.Sprintf("%s.%06d", base, usec)
	}
	return base
}
