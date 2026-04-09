package executor

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/myuon/mylite/storage"
	"vitess.io/vitess/go/vt/sqlparser"
)

// parseTimeMicroseconds parses a time string like "HH:MM:SS[.ffffff]"
// and returns the total microseconds. Returns (microseconds, ok).
func parseTimeMicroseconds(s string) (int64, bool) {
	s = strings.TrimSpace(s)
	neg := false
	if strings.HasPrefix(s, "-") {
		neg = true
		s = strings.TrimSpace(s[1:])
	}
	// Handle "D HH:MM:SS" format
	var days int64
	if idx := strings.Index(s, " "); idx >= 0 {
		d, err := strconv.ParseInt(s[:idx], 10, 64)
		if err != nil {
			return 0, false
		}
		days = d
		s = s[idx+1:]
	}
	// Parse HH:MM:SS[.ffffff] or HH:MM:SS using colons (may also be colon-separated date like "2000:01:01")
	parts := strings.Split(s, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, false
	}
	h, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, false
	}
	m, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || m > 59 {
		return 0, false
	}
	var sec int64
	var usec int64
	if len(parts) == 3 {
		secStr := parts[2]
		if dotIdx := strings.Index(secStr, "."); dotIdx >= 0 {
			sec, err = strconv.ParseInt(secStr[:dotIdx], 10, 64)
			if err != nil {
				return 0, false
			}
			frac := secStr[dotIdx+1:]
			for len(frac) < 6 {
				frac += "0"
			}
			if len(frac) > 6 {
				frac = frac[:6]
			}
			usec, _ = strconv.ParseInt(frac, 10, 64)
		} else {
			sec, err = strconv.ParseInt(secStr, 10, 64)
			if err != nil {
				return 0, false
			}
		}
		if sec > 59 {
			return 0, false
		}
	}
	totalUs := (days*86400+h*3600+m*60+sec)*1000000 + usec
	if neg {
		totalUs = -totalUs
	}
	return totalUs, true
}

// formatTimeDiffMicros formats a time difference in microseconds as MySQL TIME string.
// Clamps to MySQL's max TIME value of 838:59:59.000000 or min of -838:59:59.000000.
// Adds warning if clamped. origHadMicros indicates if inputs had fractional seconds.
func formatTimeDiffMicros(diffUs int64, e *Executor) string {
	return formatTimeDiffMicrosOpt(diffUs, e, false)
}

func formatTimeDiffMicrosOpt(diffUs int64, e *Executor, origHadMicros bool) string {
	const maxTimeMicros = int64(838*3600+59*60+59) * 1000000
	neg := diffUs < 0
	absDiffUs := diffUs
	if neg {
		absDiffUs = -diffUs
	}
	if absDiffUs > maxTimeMicros {
		// Add warning about truncation
		unclamped := absDiffUs
		absDiffUs = maxTimeMicros
		if e != nil {
			// Format original value for warning
			uh := unclamped / (1000000 * 3600)
			um := (unclamped / (1000000 * 60)) % 60
			us2 := (unclamped / 1000000) % 60
			uus := unclamped % 1000000
			var original string
			if neg {
				original = fmt.Sprintf("-%02d:%02d:%02d.%06d", uh, um, us2, uus)
			} else {
				original = fmt.Sprintf("%02d:%02d:%02d.%06d", uh, um, us2, uus)
			}
			e.addWarning("Warning", 1292, "Truncated incorrect time value: '"+original+"'")
		}
	}
	h := absDiffUs / (1000000 * 3600)
	m := (absDiffUs / (1000000 * 60)) % 60
	s := (absDiffUs / 1000000) % 60
	us := absDiffUs % 1000000
	sign := ""
	if neg {
		sign = "-"
	}
	// Show microseconds if: result has fractional microseconds, OR inputs had microseconds
	if us != 0 || origHadMicros {
		return fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, h, m, s, us)
	}
	return fmt.Sprintf("%s%02d:%02d:%02d", sign, h, m, s)
}

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
			// Return as integer when called with no arguments
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
		// Re-interpret the parsed wall-clock time in the session timezone so that
		// UNIX_TIMESTAMP('1998-09-16 09:26:00') with time_zone='+03:00' correctly
		// treats the datetime as being in +03:00, not UTC.
		loc := e.timeZone
		if loc == nil {
			loc = time.Local
		}
		tInZone := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), loc)
		unixSec := tInZone.Unix()
		// MySQL returns 0 for timestamps outside the valid UNIX_TIMESTAMP range:
		// [1970-01-01 00:00:01 UTC, 2038-01-19 03:14:07 UTC]
		const mysqlMaxUnixTimestamp = int64(2147483647)
		if unixSec < 0 || unixSec > mysqlMaxUnixTimestamp {
			return int64(0), true, nil
		}
		// Detect whether argument has fractional seconds to determine return type.
		// MySQL returns DECIMAL(16,6) when the argument is a CONCAT() or similar string expression,
		// and an integer when the argument is a datetime expression or plain string literal without fractional seconds.
		micros := float64(tInZone.Nanosecond()) / 1e3
		result := float64(unixSec) + micros/1e6
		if tInZone.Nanosecond() != 0 {
			return DivisionResult{Value: result, Precision: 6}, true, nil
		}
		// No fractional seconds: return DECIMAL(16,6) only for CONCAT/string-building expressions
		if isStringBuildingExpr(v.Exprs[0]) {
			return DivisionResult{Value: result, Precision: 6}, true, nil
		}
		return int64(unixSec), true, nil
	case "from_unixtime":
		if len(v.Exprs) < 1 {
			return nil, true, fmt.Errorf("FROM_UNIXTIME requires 1 argument")
		}
		val, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		tsFloat := toFloat(val)
		// MySQL FROM_UNIXTIME returns NULL for negative timestamps or timestamps > 2147483647.
		const mysqlMaxUnixTimestamp = float64(2147483647)
		if tsFloat < 0 || tsFloat > mysqlMaxUnixTimestamp {
			return nil, true, nil
		}
		ts := int64(tsFloat)
		ns := int64((tsFloat - float64(ts)) * 1e9)
		t := time.Unix(ts, ns)
		// Apply session timezone so FROM_UNIXTIME(0) respects SET time_zone
		if e.timeZone != nil {
			t = t.In(e.timeZone)
		}
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
		t, err := parseTimeExtractionValue(val)
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
		t, err := parseTimeExtractionValue(val)
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
		t, err := parseTimeExtractionValue(val)
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
		valStr := toString(val)
		// Handle integer input: TIME(0) = '00:00:00', TIME(10112) = '01:01:12' (HHMMSS)
		if _, isInt := val.(int64); isInt {
			n := toInt64(val)
			neg := n < 0
			if neg {
				n = -n
			}
			h := n / 10000
			m := (n % 10000) / 100
			s := n % 100
			result := fmt.Sprintf("%02d:%02d:%02d", h, m, s)
			if neg {
				result = "-" + result
			}
			return result, true, nil
		}
		// Handle "0000-00-00 HH:MM:SS..." format directly (str_to_date time-only result)
		var timeStr string
		var fracStr string
		if strings.HasPrefix(valStr, "0000-00-00 ") {
			rest := valStr[len("0000-00-00 "):]
			if dotIdx := strings.LastIndex(rest, "."); dotIdx >= 0 {
				fracStr = rest[dotIdx+1:]
				timeStr = rest[:dotIdx]
			} else {
				timeStr = rest
			}
		} else {
			// If input is a datetime string (has date + time), extract the time part directly
			// to properly detect invalid hours (>23 should return NULL).
			if isDatetimeLikeString(valStr) {
				// Find the space separating date and time
				spaceIdx := strings.Index(valStr, " ")
				if spaceIdx >= 0 {
					timePart := strings.TrimSpace(valStr[spaceIdx+1:])
					// Parse hour from time part
					colonIdx := strings.Index(timePart, ":")
					if colonIdx < 0 {
						return nil, true, nil
					}
					hourStr := timePart[:colonIdx]
					hour, err := strconv.Atoi(hourStr)
					if err != nil || hour > 23 || hour < 0 {
						return nil, true, nil
					}
					// Extract fractional part
					if dotIdx := strings.LastIndex(timePart, "."); dotIdx >= 0 {
						fracStr = timePart[dotIdx+1:]
						timeStr = timePart[:dotIdx]
					} else {
						timeStr = timePart
					}
					// Validate timeStr looks like HH:MM:SS
					timeParts := strings.Split(timeStr, ":")
					if len(timeParts) != 3 {
						return nil, true, nil
					}
				} else {
					// No time part in datetime string
					timeStr = "00:00:00"
				}
			} else {
				t, terr := parseDateTimeValue(val)
				if terr != nil {
					// Try to parse as a TIME string (e.g. "-73:42:12", "838:59:59")
					us, ok := parseTimeMicroseconds(valStr)
					if !ok {
						return nil, true, nil
					}
					return formatMicrosAsTimeString(us), true, nil
				}
				timeStr = t.Format("15:04:05")
				// Extract fractional part from input string
				if dotIdx := strings.LastIndex(valStr, "."); dotIdx >= 0 {
					fracStr = valStr[dotIdx+1:]
				}
			}
		}
		// Include fractional seconds if present in input
		if fracStr != "" {
			for len(fracStr) < 6 {
				fracStr += "0"
			}
			if len(fracStr) > 6 {
				fracStr = fracStr[:6]
			}
			return fmt.Sprintf("%s.%s", timeStr, fracStr), true, nil
		}
		return timeStr, true, nil
	case "datediff":
		v0, v1, hasNull, err := e.evalArgs2(v.Exprs, "DATEDIFF", row)
		if err != nil {
			return nil, true, err
		}
		if hasNull {
			return nil, true, nil
		}
		// Check for invalid dates (e.g. Nov 31) - parseMySQLDateValue returns "" for invalid dates.
		// But if ALLOW_INVALID_DATES is set, skip this check.
		s0 := toString(v0)
		s1 := toString(v1)
		allowInvalidDates := strings.Contains(e.sqlMode, "ALLOW_INVALID_DATES")
		if !allowInvalidDates {
			if parsed := parseMySQLDateValue(s0); parsed == "" && isDateLikeButInvalid(s0) {
				return nil, true, nil
			}
			if parsed := parseMySQLDateValue(s1); parsed == "" && isDateLikeButInvalid(s1) {
				return nil, true, nil
			}
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
		lcLocale, _ := e.getSysVar("lc_time_names")
		return mysqlDateFormat(t, toString(fmtVal), lcLocale), true, nil
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
		// Use smart (type-aware) output format only when both arguments are string literals.
		// When either arg is a column reference or expression, MySQL returns datetime(6) format.
		_, fmtIsLit := v.Exprs[1].(*sqlparser.Literal)
		parsed := mysqlStrToDate(toString(strVal), toString(fmtVal2), fmtIsLit)
		if parsed == nil {
			return nil, true, nil
		}
		return *parsed, true, nil
	case "get_format":
		if len(v.Exprs) < 2 {
			return nil, true, fmt.Errorf("GET_FORMAT requires 2 arguments")
		}
		// The first argument is a type keyword (DATE, TIME, DATETIME, TIMESTAMP).
		// It may be parsed as a ColName. Extract it directly from AST if possible.
		var typeStr string
		if col, ok := v.Exprs[0].(*sqlparser.ColName); ok {
			typeStr = strings.ToUpper(col.Name.String())
		} else {
			typeVal, err := e.evalExprMaybeRow(v.Exprs[0], row)
			if err != nil {
				return nil, true, err
			}
			if typeVal == nil {
				return nil, true, nil
			}
			typeStr = strings.ToUpper(toString(typeVal))
		}
		localeVal, err := e.evalExprMaybeRow(v.Exprs[1], row)
		if err != nil {
			return nil, true, err
		}
		if localeVal == nil {
			return nil, true, nil
		}
		result := mysqlGetFormat(typeStr, strings.ToUpper(toString(localeVal)))
		if result == "" {
			return nil, true, nil
		}
		return result, true, nil
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
		base, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if base == nil {
			return nil, true, nil
		}
		interval, err := e.evalExprMaybeRow(v.Exprs[1], row)
		if err != nil {
			return nil, true, err
		}
		if interval == nil {
			return nil, true, nil
		}
		baseStr := toString(base)
		intervalStr := toString(interval)
		// If the interval looks like a datetime (has a date part like YYYY-MM-DD),
		// MySQL returns NULL for addtime/subtime.
		if isDatetimeLikeString(intervalStr) {
			return nil, true, nil
		}
		// If base is a datetime (has YYYY-MM-DD), use datetime arithmetic.
		// Otherwise (TIME string), use time string arithmetic.
		if isDatetimeLikeString(baseStr) {
			t, dtErr := parseDateTimeValue(base)
			if dtErr == nil {
				dur, err := parseMySQLTimeInterval(intervalStr)
				if err != nil {
					return baseStr, true, nil
				}
				result := t.Add(dur)
				return formatDateTimeWithOptionalMicros(result), true, nil
			}
		}
		// Base is a TIME string (possibly with garbage at end) - use time arithmetic
		return addTimeStrings(baseStr, intervalStr, false), true, nil
	case "subtime":
		if len(v.Exprs) < 2 {
			return nil, true, nil
		}
		base, err := e.evalExprMaybeRow(v.Exprs[0], row)
		if err != nil {
			return nil, true, err
		}
		if base == nil {
			return nil, true, nil
		}
		interval, err := e.evalExprMaybeRow(v.Exprs[1], row)
		if err != nil {
			return nil, true, err
		}
		if interval == nil {
			return nil, true, nil
		}
		baseStr := toString(base)
		intervalStr := toString(interval)
		// If the interval looks like a datetime (has a date part), MySQL returns NULL.
		if isDatetimeLikeString(intervalStr) {
			return nil, true, nil
		}
		// If base is a datetime (has YYYY-MM-DD), use datetime arithmetic.
		// Otherwise (TIME string), use time string arithmetic.
		if isDatetimeLikeString(baseStr) {
			t, dtErr := parseDateTimeValue(base)
			if dtErr == nil {
				dur, err := parseMySQLTimeInterval(intervalStr)
				if err != nil {
					return baseStr, true, nil
				}
				result := t.Add(-dur)
				return formatDateTimeWithOptionalMicros(result), true, nil
			}
		}
		// Base is a TIME string - use time arithmetic
		return addTimeStrings(baseStr, intervalStr, true), true, nil
	case "from_days":
		val, isNull, err := e.evalArg1Quiet(v.Exprs, row)
		if err != nil {
			return nil, true, err
		}
		if isNull {
			return nil, true, nil
		}
		days := toInt64(val)
		if days <= 0 {
			return "0000-00-00", true, nil
		}
		// MySQL's day 1 epoch: 1970-01-01 is MySQL day 719528
		const mysqlUnixEpochDay = int64(719528)
		delta := days - mysqlUnixEpochDay
		t := time.Unix(delta*86400, 0).UTC()
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
		// For modes with range 1-53 (bit1 set: modes 2,3,6,7), use mysqlWeekYearFull
		// to correctly handle year-boundary cases (e.g., Dec 31 in next year's week 1).
		// For modes with range 0-53 (bit1 clear: modes 0,1,4,5), use mysqlWeekFull
		// directly so that dates before the first week return 0.
		if (mode & 2) != 0 {
			_, week := mysqlWeekYearFull(t, mode)
			return week, true, nil
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
		mode := int64(0)
		if len(v.Exprs) >= 2 {
			modeVal, modeErr := e.evalExprMaybeRow(v.Exprs[1], row)
			if modeErr == nil && modeVal != nil {
				mode = toInt64(modeVal)
			}
		}
		yr, wk := mysqlWeekYearFull(t, mode)
		return int64(yr)*100 + wk, true, nil
	case "timestamp":
		if len(v.Exprs) < 1 {
			return nil, true, nil
		}
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
		valStr := toString(val)
		t, parseErr := parseDateTimeValue(val)
		if parseErr != nil {
			return nil, true, nil
		}
		if len(v.Exprs) == 1 {
			// Single arg: return datetime (preserving microseconds from input string)
			fracStr := ""
			if dotIdx := strings.LastIndex(valStr, "."); dotIdx >= 0 {
				fracStr = valStr[dotIdx+1:]
				for len(fracStr) < 6 {
					fracStr += "0"
				}
				if len(fracStr) > 6 {
					fracStr = fracStr[:6]
				}
			}
			if fracStr != "" {
				return t.Format("2006-01-02 15:04:05") + "." + fracStr, true, nil
			}
			return t.Format("2006-01-02 15:04:05"), true, nil
		}
		// Two args: TIMESTAMP(date, time) - add time to the date
		timeArg, err := e.evalExprMaybeRow(v.Exprs[1], row)
		if err != nil {
			return nil, true, err
		}
		if timeArg == nil {
			return nil, true, nil
		}
		timeArgStr := toString(timeArg)
		dur, durErr := parseMySQLTimeInterval(timeArgStr)
		if durErr != nil {
			return nil, true, nil
		}
		result := t.Add(dur)
		// Preserve microseconds if the time argument has fractional seconds
		if dotIdx := strings.LastIndex(timeArgStr, "."); dotIdx >= 0 {
			fracStr := timeArgStr[dotIdx+1:]
			for len(fracStr) < 6 {
				fracStr += "0"
			}
			if len(fracStr) > 6 {
				fracStr = fracStr[:6]
			}
			// Check if result has its own nanoseconds from the duration
			if result.Nanosecond() != 0 {
				us := result.Nanosecond() / 1000
				return fmt.Sprintf("%s.%06d", result.Format("2006-01-02 15:04:05"), us), true, nil
			}
			return result.Format("2006-01-02 15:04:05") + "." + fracStr, true, nil
		}
		// No fractional seconds
		return result.Format("2006-01-02 15:04:05"), true, nil
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
			// Truncate at first non-digit (MySQL ignores trailing garbage)
			digitEnd := 0
			for digitEnd < len(usFrac) && usFrac[digitEnd] >= '0' && usFrac[digitEnd] <= '9' {
				digitEnd++
			}
			usFrac = usFrac[:digitEnd]
			for len(usFrac) < 6 {
				usFrac += "0"
			}
			if len(usFrac) > 6 {
				usFrac = usFrac[:6]
			}
			usN, _ := strconv.ParseInt(usFrac, 10, 64)
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
		// Parse the time value - handle numeric datetime format (YYYYMMDDHHMMSS etc.)
		tfH, tfM, tfSec := 0, 0, 0
		// First try to parse as a datetime to extract time components
		if t, parseErr := parseDateTimeValue(tfTime); parseErr == nil {
			tfH = t.Hour()
			tfM = t.Minute()
			tfSec = t.Second()
		} else {
			// Fallback: split by ':' for HH:MM:SS format
			tfStr := toString(tfTime)
			tfParts := strings.Split(tfStr, ":")
			if len(tfParts) >= 1 {
				tfH, _ = strconv.Atoi(tfParts[0])
			}
			if len(tfParts) >= 2 {
				tfM, _ = strconv.Atoi(tfParts[1])
			}
			if len(tfParts) >= 3 {
				tfSec, _ = strconv.Atoi(tfParts[2])
			}
		}
		// For AM/PM and 12-hour clock, normalize using h mod 24
		tfH24 := tfH % 24 // hour within a day (0-23)
		if tfH24 < 0 {
			tfH24 += 24
		}
		tfAmPm := "AM"
		if tfH24 >= 12 {
			tfAmPm = "PM"
		}
		tfH12 := tfH24 % 12
		if tfH12 == 0 {
			tfH12 = 12
		}

		tfFmtStr := toString(tfFmt)
		var tfSb strings.Builder
		for i := 0; i < len(tfFmtStr); i++ {
			if tfFmtStr[i] == '%' && i+1 < len(tfFmtStr) {
				i++
				switch tfFmtStr[i] {
				case 'H':
					tfSb.WriteString(fmt.Sprintf("%02d", tfH24))
				case 'k':
					tfSb.WriteString(fmt.Sprintf("%d", tfH24))
				case 'h', 'I':
					tfSb.WriteString(fmt.Sprintf("%02d", tfH12))
				case 'l':
					tfSb.WriteString(fmt.Sprintf("%d", tfH12))
				case 'i':
					tfSb.WriteString(fmt.Sprintf("%02d", tfM))
				case 's', 'S':
					tfSb.WriteString(fmt.Sprintf("%02d", tfSec))
				case 'p':
					tfSb.WriteString(tfAmPm)
				case 'r':
					// 12-hour clock with AM/PM
					tfSb.WriteString(fmt.Sprintf("%02d:%02d:%02d %s", tfH12, tfM, tfSec, tfAmPm))
				case 'T':
					tfSb.WriteString(fmt.Sprintf("%02d:%02d:%02d", tfH24, tfM, tfSec))
				case '%':
					tfSb.WriteByte('%')
				default:
					tfSb.WriteByte('%')
					tfSb.WriteByte(tfFmtStr[i])
				}
			} else {
				tfSb.WriteByte(tfFmtStr[i])
			}
		}
		return tfSb.String(), true, nil
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
		tdStrA := toString(tdA)
		tdStrB := toString(tdB)
		tdTA, tdErr1 := parseDateTimeValue(tdStrA)
		tdTB, tdErr2 := parseDateTimeValue(tdStrB)
		// Determine if each arg is a datetime or a time string.
		// A string is a "datetime" if it has a date component (YYYY-MM-DD or similar with '-').
		tdAIsDatetime := isDatetimeLikeString(tdStrA)
		tdBIsDatetime := isDatetimeLikeString(tdStrB)
		// MySQL returns NULL if the argument types differ (datetime vs time).
		if tdAIsDatetime != tdBIsDatetime {
			return nil, true, nil
		}
		if tdErr1 != nil || tdErr2 != nil {
			// Try parsing both as time strings if datetime parse fails
			var totalUsA, totalUsB int64
			var parseOkA, parseOkB bool
			if tdErr1 != nil {
				us, ok := parseTimeMicroseconds(tdStrA)
				totalUsA = us
				parseOkA = ok
			} else {
				parseOkA = true
				totalUsA = (int64(tdTA.Hour())*3600+int64(tdTA.Minute())*60+int64(tdTA.Second()))*1000000 + int64(tdTA.Nanosecond())/1000
			}
			if tdErr2 != nil {
				us, ok := parseTimeMicroseconds(tdStrB)
				totalUsB = us
				parseOkB = ok
			} else {
				parseOkB = true
				totalUsB = (int64(tdTB.Hour())*3600+int64(tdTB.Minute())*60+int64(tdTB.Second()))*1000000 + int64(tdTB.Nanosecond())/1000
			}
			if !parseOkA || !parseOkB {
				return nil, true, nil
			}
			diffUs := totalUsA - totalUsB
			origHadMicros := strings.Contains(tdStrA, ".") || strings.Contains(tdStrB, ".")
			return formatTimeDiffMicrosOpt(diffUs, e, origHadMicros), true, nil
		}
		tdDiff := tdTA.Sub(tdTB)
		tdDiffUs := int64(tdDiff) / 1000 // nanoseconds to microseconds
		origHadMicros := tdTA.Nanosecond() != 0 || tdTB.Nanosecond() != 0 || strings.Contains(tdStrA, ".") || strings.Contains(tdStrB, ".")
		return formatTimeDiffMicrosOpt(tdDiffUs, e, origHadMicros), true, nil
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
		// MySQL converts 2-digit years: 0-69 → 2000-2069, 70-99 → 1970-1999
		if mdY >= 0 && mdY <= 99 {
			mdY = convert2DigitYear(mdY)
		}
		if mdD <= 0 || mdY < 0 || mdY > 9999 {
			return nil, true, nil
		}
		mdT := time.Date(mdY, 1, mdD, 0, 0, 0, 0, time.UTC)
		// MySQL returns NULL if the resulting date is out of range (year > 9999)
		if mdT.Year() > 9999 || mdT.Year() < 0 {
			return nil, true, nil
		}
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
