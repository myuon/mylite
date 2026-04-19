package executor

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/myuon/mylite/catalog"
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
		t := e.nowTime()
		if len(v.Exprs) > 0 {
			precVal, _, _ := e.evalArg1(v.Exprs, name, row)
			prec := int(toFloat(precVal))
			if prec > 0 && prec <= 6 {
				frac := fmt.Sprintf("%06d", t.Nanosecond()/1000)[:prec]
				return t.Format("2006-01-02 15:04:05") + "." + frac, true, nil
			}
		}
		return t.Format("2006-01-02 15:04:05"), true, nil
	case "curdate", "current_date":
		return e.nowTime().Format("2006-01-02"), true, nil
	case "curtime", "current_time":
		t := e.nowTime()
		if len(v.Exprs) > 0 {
			precVal, _, _ := e.evalArg1(v.Exprs, name, row)
			prec := int(toFloat(precVal))
			if prec > 0 && prec <= 6 {
				frac := fmt.Sprintf("%06d", t.Nanosecond()/1000)[:prec]
				return t.Format("15:04:05") + "." + frac, true, nil
			}
		}
		return t.Format("15:04:05"), true, nil
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
			if e.isNonDateTypeExpr(v.Exprs[0]) {
				return nil, true, nil // string literal or non-date column → NULL
			}
			return int64(0), true, nil // date-typed column → 0
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
			if e.isNonDateTypeExpr(v.Exprs[0]) {
				return nil, true, nil // string literal or non-date column → NULL
			}
			return int64(0), true, nil // date-typed column → 0
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
			if e.isNonDateTypeExpr(v.Exprs[0]) {
				return nil, true, nil // string literal or non-date column → NULL
			}
			return int64(0), true, nil // date-typed column → 0
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
		parsed, err := mysqlStrToDate(toString(strVal), toString(fmtVal2), fmtIsLit, e.isStrictMode())
		if err != nil {
			if isMySQLError(err, 1411) {
				// In DML context (INSERT/UPDATE) with strict mode, propagate error 1411 as an error.
				// In SELECT context, MySQL converts it to a warning and returns NULL.
				if e.insideDML && e.isStrictMode() {
					return nil, true, err
				}
				// Extract the message from the error and add as a warning
				msg := strings.TrimPrefix(err.Error(), "ERROR 1411 (HY000): ")
				e.addWarning("Warning", 1411, msg)
				return nil, true, nil
			}
			return nil, true, err
		}
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
		// If base is a full DATETIME (has YYYY-MM-DD HH:MM:SS), use datetime arithmetic.
		// Plain DATE strings (YYYY-MM-DD only) are treated as TIME strings by MySQL.
		if isDatetimeWithTimeComponent(baseStr) {
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
		// Base is a TIME string (possibly with garbage at end) - use time arithmetic.
		// If base looks like a date string (YYYY-MM-DD), MySQL emits a warning.
		if isDatetimeLikeString(baseStr) {
			e.addWarning("Warning", 1292, fmt.Sprintf("Truncated incorrect time value: '%s'", baseStr))
		}
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
		// If base is a full DATETIME (has YYYY-MM-DD HH:MM:SS), use datetime arithmetic.
		// Plain DATE strings (YYYY-MM-DD only) are treated as TIME strings by MySQL.
		if isDatetimeWithTimeComponent(baseStr) {
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
		// Base is a TIME string - use time arithmetic.
		// If base looks like a date string (YYYY-MM-DD), MySQL emits a warning.
		if isDatetimeLikeString(baseStr) {
			e.addWarning("Warning", 1292, fmt.Sprintf("Truncated incorrect time value: '%s'", baseStr))
		}
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
		// MySQL returns 0000-00-00 (zero date) for day numbers that result in year 0
		// (days < 366). Day 366 = 0001-01-01, day 365 = 0000-12-31 which MySQL
		// treats as a zero/invalid date and displays as 0000-00-00.
		if days < 366 {
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
		// Check if the date string has a zero month component (e.g., '2005-00-00', '2005-00-01')
		// MySQL treats month=0 as invalid for LAST_DAY and returns NULL with a warning.
		valStr := toString(val)
		if len(valStr) >= 7 && valStr[4] == '-' {
			mo, moErr := strconv.Atoi(valStr[5:7])
			if moErr == nil && mo == 0 {
				return nil, true, nil
			}
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
			if len(v.Exprs) > 0 && e.isNonDateTypeExpr(v.Exprs[0]) {
				return nil, true, nil // string literal or non-date column → NULL
			}
			return int64(0), true, nil // date-typed column → 0
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
		// If the value is a TIME-only string (no date component), combine with current date.
		// A TIME-only value parses with year=0 when format "15:04:05" matches.
		// Detect TIME-only: contains ':' but no '-' date separator.
		if isTimeOnlyStr(valStr) {
			now := e.nowTime()
			t = time.Date(now.Year(), now.Month(), now.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), now.Location())
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
		// MySQL TIME_TO_SEC clamps to max TIME value 838:59:59 = 3020399 seconds
		const maxTTSSec = 838*3600 + 59*60 + 59
		if ttsSecs > maxTTSSec {
			ttsSecs = maxTTSSec
		} else if ttsSecs < -maxTTSSec {
			ttsSecs = -maxTTSSec
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
		// Extract fractional seconds from the second argument.
		// Use string representation to avoid float64 rounding artifacts.
		// MySQL MAKETIME result precision = min(fracDigitsOfSecondArg, 6).
		// Trailing zeros ARE preserved to fill the declared precision.
		var mtFrac string
		mtSecStr := toString(mtSec)
		if dot := strings.IndexByte(mtSecStr, '.'); dot >= 0 {
			rawFrac := mtSecStr[dot+1:]
			// Determine precision: min(len(rawFrac), 6)
			prec := len(rawFrac)
			if prec > 6 {
				prec = 6
			}
			// Truncate to prec digits (no rounding in time_truncate_fractional mode)
			frac := rawFrac[:prec]
			// Pad to prec if shorter (shouldn't happen but be safe)
			for len(frac) < prec {
				frac += "0"
			}
			// Only include if non-zero (but keep trailing zeros within the precision)
			// Exception: if all zeros, still include (MySQL shows .000000 for 59.0000005 truncated)
			mtFrac = "." + frac
		}
		// Clamp to MySQL TIME maximum value: 838:59:59
		const maxMakeTimeH = int64(838)
		clampedH := mtHi
		clampedM := mtMi
		clampedS := mtSi
		if mtHi > maxMakeTimeH || (mtHi == maxMakeTimeH && (mtMi > 59 || mtSi > 59)) {
			clampedH = maxMakeTimeH
			clampedM = 59
			clampedS = 59
			mtFrac = ""
		}
		timeStr := fmt.Sprintf("%s%02d:%02d:%02d%s", mtNeg, clampedH, clampedM, clampedS, mtFrac)
		return timeStr, true, nil
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

// isTimeOnlyStr returns true if s looks like a TIME-only value (HH:MM:SS) with no date component.
// A TIME-only string has ':' but no '-' date separator.
func isTimeOnlyStr(s string) bool {
	s = strings.TrimSpace(s)
	if strings.Contains(s, "-") {
		return false // has date separator → not time-only
	}
	return strings.Contains(s, ":")
}

// isNonDateTypeExpr returns true if expr is a string literal or refers to a non-DATE/DATETIME/TIMESTAMP column.
// Used to determine if zero dates should return NULL (non-date context) vs 0 (date column context).
func (e *Executor) isNonDateTypeExpr(expr sqlparser.Expr) bool {
	// String literals always use NULL semantics for zero dates
	if lit, ok := expr.(*sqlparser.Literal); ok {
		return lit.Type == sqlparser.StrVal
	}
	// For column references, check the column type
	if col, ok := expr.(*sqlparser.ColName); ok {
		colName := col.Name.String()
		if e.queryTableDef != nil {
			for _, c := range e.queryTableDef.Columns {
				if strings.EqualFold(c.Name, colName) {
					ct := strings.ToUpper(strings.TrimSpace(c.Type))
					if paren := strings.IndexByte(ct, '('); paren >= 0 {
						ct = strings.TrimSpace(ct[:paren])
					}
					switch ct {
					case "DATE", "DATETIME", "TIMESTAMP":
						return false // date-typed column → return 0
					}
					return true // non-date column (CHAR, VARCHAR, TEXT, etc.) → return NULL
				}
			}
		} else {
			// queryTableDef is nil: try to look up from catalog using current DB
			// This handles cases where queryTableDef was not set (e.g., complex queries)
			if e.Catalog != nil && e.CurrentDB != "" {
				if db, err := e.Catalog.GetDatabase(e.CurrentDB); err == nil {
					for _, td := range db.Tables {
						for _, c := range td.Columns {
							if strings.EqualFold(c.Name, colName) {
								ct := strings.ToUpper(strings.TrimSpace(c.Type))
								if paren := strings.IndexByte(ct, '('); paren >= 0 {
									ct = strings.TrimSpace(ct[:paren])
								}
								switch ct {
								case "DATE", "DATETIME", "TIMESTAMP":
									return false
								}
								return true
							}
						}
					}
				}
			}
		}
	}
	// Default: treat as non-date (return NULL for zero dates)
	return true
}

func isZeroDate(val interface{}) bool {
	if val == nil {
		return false
	}
	s := toString(val)
	return strings.HasPrefix(s, "0000-00-00")
}

func secToTimeValue(v interface{}) string {
	// Determine the precision for fractional seconds.
	// DivisionResult carries a Precision that limits decimal places.
	fracPrec := 6
	if dr, ok := v.(DivisionResult); ok {
		if dr.Precision < fracPrec {
			fracPrec = dr.Precision
		}
	}

	// When the input is a string (e.g. a DECIMAL literal like "3661.9999999"), extract
	// the fractional part directly from the string representation to avoid float64 rounding.
	// This preserves precision for truncation (time_truncate_fractional mode).
	var strFracPart string
	if sv, ok := v.(string); ok {
		sv = strings.TrimSpace(sv)
		if strings.HasPrefix(sv, "-") {
			sv = sv[1:]
		}
		if dot := strings.IndexByte(sv, '.'); dot >= 0 {
			strFracPart = sv[dot+1:] // raw fractional digits from string
		}
	}

	f := toFloat(v)
	sign := ""
	if f < 0 {
		sign = "-"
		f = -f
	}
	// MySQL SEC_TO_TIME clamps to max TIME value 838:59:59
	const maxTimeSec = 838*3600 + 59*60 + 59 // 3020399
	totalSec := int64(f)
	clamped := false
	if totalSec > maxTimeSec {
		totalSec = maxTimeSec
		clamped = true
	}
	h := totalSec / 3600
	m := (totalSec % 3600) / 60
	s := totalSec % 60

	if fracPrec > 0 && !clamped {
		var fracStr string
		if strFracPart != "" {
			// Use string-based truncation (no float rounding artifacts).
			frac := strFracPart
			// Pad to at least fracPrec digits
			for len(frac) < fracPrec {
				frac += "0"
			}
			// Truncate to fracPrec digits (do not round)
			frac = frac[:fracPrec]
			frac = strings.TrimRight(frac, "0")
			if frac != "" {
				fracStr = "." + frac
			}
		} else {
			// Float-based fallback: format and strip trailing zeros
			frac := f - float64(totalSec)
			if frac > 1e-9 {
				fs := fmt.Sprintf("%."+strconv.Itoa(fracPrec)+"f", frac)[1:] // e.g., ".4235"
				fs = strings.TrimRight(fs, "0")
				if fs != "." {
					fracStr = fs
				}
			}
		}
		if fracStr != "" {
			return fmt.Sprintf("%s%02d:%02d:%02d%s", sign, h, m, s, fracStr)
		}
	}
	return fmt.Sprintf("%s%02d:%02d:%02d", sign, h, m, s)
}

// mysqlWeekMode0 calculates MySQL's WEEK(date) with default mode 0.

func mysqlWeekMode0(t time.Time) int64 {
	yday := t.YearDay() // 1-based
	// Find the weekday of Jan 1 (0=Sunday, ..., 6=Saturday)
	jan1 := time.Date(t.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
	wdJan1 := int(jan1.Weekday()) // 0=Sunday
	// First Sunday of the year is at day (7-wdJan1)%7 + 1
	// If Jan 1 is Sunday, first Sunday is day 1
	firstSunday := (7 - wdJan1) % 7
	if yday <= firstSunday {
		return 0
	}
	return int64((yday-firstSunday-1)/7 + 1)
}

// mysqlWeekFull calculates MySQL's WEEK(date, mode) for all 8 modes.
// MySQL WEEK() mode semantics:
//
//	Mode 0: first=Sun, range 0-53, week1=starts at first Sunday
//	Mode 1: first=Mon, range 0-53, week1=4+ days in year
//	Mode 2: first=Sun, range 1-53, week1=starts at first Sunday (or prev year last week)
//	Mode 3: first=Mon, range 1-53, ISO week
//	Mode 4: first=Sun, range 0-53, week1=4+ days in year
//	Mode 5: first=Mon, range 0-53, week1=starts at first Monday
//	Mode 6: first=Sun, range 1-53, week1=4+ days in year

func mysqlWeekFull(t time.Time, mode int64) int64 {
	mode = mode & 7 // clamp to 0-7
	mondayFirst := (mode & 1) != 0
	weekRange1to53 := (mode & 2) != 0
	useWeek4Rule := (mode & 4) != 0

	year := t.Year()

	if mondayFirst {
		// Monday-first modes
		if useWeek4Rule {
			// Modes 5, 7: week 1 starts at first Monday (no 4-day rule)
			jan1 := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
			wdJan1 := int(jan1.Weekday()) // 0=Sun, 1=Mon, ..., 6=Sat
			// firstMonday: days from Jan 1 to first Monday (0-based offset)
			firstMonday := (8 - wdJan1) % 7 // 0 if Jan 1 is Monday
			yday := t.YearDay() - 1         // 0-based
			if yday < firstMonday {
				if weekRange1to53 {
					// Return last week of previous year
					prevDec31 := time.Date(year-1, 12, 31, 0, 0, 0, 0, time.UTC)
					return mysqlWeekFull(prevDec31, mode)
				}
				return 0
			}
			return int64((yday-firstMonday)/7 + 1)
		}
		// Mode 3: straight ISO week (1-53)
		if weekRange1to53 {
			_, isoWeek := t.ISOWeek()
			return int64(isoWeek)
		}
		// Mode 1: Monday-first, range 0-53, week 1 has 4+ days in year.
		// Week 1 starts: the Monday on or before Jan 4 (the week containing Jan 4 is week 1).
		// wdJan1 in Mon=1..Sun=7 system:
		jan1 := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
		wdJan1Mon := int(jan1.Weekday()) // 0=Sun, 1=Mon..6=Sat
		if wdJan1Mon == 0 {
			wdJan1Mon = 7 // Sunday = 7 in Mon-first system
		}
		// Offset from Jan 1 to Monday of week 1:
		// If wdJan1Mon <= 4 (Mon=1,Tue=2,Wed=3,Thu=4): week 1 starts at -(wdJan1Mon-1)
		// If wdJan1Mon >= 5 (Fri=5,Sat=6,Sun=7): week 1 starts at 8-wdJan1Mon
		var week1StartOffset int
		if wdJan1Mon <= 4 {
			week1StartOffset = -(wdJan1Mon - 1) // negative: in Dec of prev year
		} else {
			week1StartOffset = 8 - wdJan1Mon // positive: in Jan
		}
		yday := t.YearDay() - 1 // 0-based day of year
		if yday < week1StartOffset {
			// Date is before week 1 of current year: return 0
			return 0
		}
		return int64((yday-week1StartOffset)/7 + 1)
	}

	// Sunday-first modes (modes 0, 2, 4, 6)
	jan1 := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
	wdJan1 := int(jan1.Weekday()) // 0=Sun

	if useWeek4Rule {
		// Modes 4, 6: week 1 has 4+ days in year (Sun-first variant)
		// First Sunday of the week that contains Jan 1 with 4+ days
		// The week containing Jan 1 starts on the Sunday on or before Jan 1.
		// Jan 1 is day wdJan1 of its week (0=Sun). The week starts at Jan 1 - wdJan1.
		// If this week has >= 4 days in the year (i.e. wdJan1 <= 3), it's week 1.
		// Otherwise week 1 starts next Sunday.
		yday := t.YearDay() - 1 // 0-based
		// Days before Jan 1 in the partial week containing Jan 1
		daysInFirstWeek := 7 - wdJan1
		if wdJan1 <= 3 {
			// Jan 1's week has >= 4 days -> it's week 1 starting at offset -wdJan1
			weekStart := -wdJan1 // can be negative (days in Dec of prev year)
			week := (yday - weekStart) / 7
			if week == 0 && weekRange1to53 {
				// Shouldn't happen with useWeek4Rule when wdJan1<=3
				return 1
			}
			return int64(week + 1)
		}
		// First partial week has < 4 days, so week 1 starts at daysInFirstWeek
		if yday < daysInFirstWeek {
			if weekRange1to53 {
				// Return last week of previous year
				prevDec31 := time.Date(year-1, 12, 31, 0, 0, 0, 0, time.UTC)
				return mysqlWeekFull(prevDec31, mode)
			}
			return 0
		}
		return int64((yday-daysInFirstWeek)/7 + 1)
	}

	// Modes 0, 2: week 1 starts at first Sunday
	// firstSunday: 0-based offset of first Sunday
	firstSunday := (7 - wdJan1) % 7
	yday := t.YearDay() - 1 // 0-based
	if yday < firstSunday {
		if weekRange1to53 {
			// Return last week of previous year
			prevDec31 := time.Date(year-1, 12, 31, 0, 0, 0, 0, time.UTC)
			return mysqlWeekFull(prevDec31, mode)
		}
		return 0
	}
	return int64((yday-firstSunday)/7 + 1)
}

// mysqlWeekYearFull returns (year, week) for a given mode.

func mysqlWeekYearFull(t time.Time, mode int64) (int, int64) {
	week := mysqlWeekFull(t, mode)
	year := t.Year()
	if week == 0 {
		// Week 0 means date belongs to last week of previous year
		year--
		prevDec31 := time.Date(year, 12, 31, 0, 0, 0, 0, time.UTC)
		week = mysqlWeekFull(prevDec31, mode)
		return year, week
	}
	// Mode 3 (ISO): use Go's ISOWeek for accurate year boundary
	if mode == 3 {
		isoYear, _ := t.ISOWeek()
		return isoYear, week
	}
	// For dates in late December, check if the week actually belongs to next year's week 1.
	// This happens with 4-day rule modes (1, 4, 6) and first-day rule modes (2, 5, 7).
	// If Jan 1 of next year falls in week 1, and our date is in the same week as Jan 1,
	// then our date belongs to next year's week 1.
	if int(t.Month()) == 12 && t.Day() >= 29 {
		nextYear := year + 1
		jan1Next := time.Date(nextYear, 1, 1, 0, 0, 0, 0, time.UTC)
		week1OfNextYear := mysqlWeekFull(jan1Next, mode)
		if week1OfNextYear == 1 {
			// Jan 1 of next year is in week 1. Check if our date t is in the same week.
			// Two dates are in the same week if their day-of-week difference (Mon or Sun first) <= 6
			// and the earlier is the week start.
			mondayFirst := (mode & 1) != 0
			var weekdayT, weekdayJ int
			if mondayFirst {
				weekdayT = (int(t.Weekday()) + 6) % 7 // 0=Mon..6=Sun
				weekdayJ = (int(jan1Next.Weekday()) + 6) % 7
			} else {
				weekdayT = int(t.Weekday()) // 0=Sun..6=Sat
				weekdayJ = int(jan1Next.Weekday())
			}
			// Check if t and jan1Next are in the same week:
			// They are in the same week if their week-start date is the same.
			weekStartT := t.AddDate(0, 0, -weekdayT)
			weekStartJ := jan1Next.AddDate(0, 0, -weekdayJ)
			if weekStartT.Equal(weekStartJ) {
				return nextYear, 1
			}
		}
	}
	return year, week
}

// isBinaryExpr checks whether an expression references a BINARY or VARBINARY column.

func mysqlYearWeek(t time.Time, mode int) (int, int) {
	if mode != 0 {
		// For non-zero modes, fall back to ISO week
		return t.ISOWeek()
	}
	wk := mysqlWeekMode0(t)
	yr := t.Year()
	if wk == 0 {
		// Date is before the first Sunday of the year; belongs to last week of previous year.
		yr--
		// Calculate week number of Dec 31 of previous year
		dec31 := time.Date(yr, 12, 31, 0, 0, 0, 0, time.UTC)
		wk = mysqlWeekMode0(dec31)
	}
	return yr, int(wk)
}

// mysqlToDays calculates MySQL's TO_DAYS value for a given time.

func mysqlToDays(t time.Time) int64 {
	y := int64(t.Year())
	m := int64(t.Month())
	d := int64(t.Day())
	if m <= 2 {
		y--
		m += 12
	}
	days := 365*y + y/4 - y/100 + y/400 + (153*(m-3)+2)/5 + d + 1721119 - 1
	return days - 1721059
}

func parseDateTimeValue(val interface{}) (time.Time, error) {
	if val == nil {
		return time.Time{}, fmt.Errorf("NULL date value")
	}
	s := toString(val)
	// Handle zero dates: return a sentinel zero time
	if strings.HasPrefix(s, "0000-00-00") {
		return time.Time{}, fmt.Errorf("zero date")
	}
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02",
		"15:04:05",
		"2006-01-02 15:04:05.999999999",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}
	// Try to parse YYYYMMDDHHMMSS or YYMMDDHHMMSS format (all-digit, no separators)
	isAllDigitsStr := func(st string) bool {
		for _, c := range st {
			if c < '0' || c > '9' {
				return false
			}
		}
		return len(st) > 0
	}
	isAllDigits := true
	for _, c := range s {
		if c < '0' || c > '9' {
			isAllDigits = false
			break
		}
	}
	// Handle YYYYMMDDHHMMSS.ffffff (compact numeric with fractional seconds)
	if !isAllDigits {
		if dotIdx := strings.IndexByte(s, '.'); dotIdx == 14 && isAllDigitsStr(s[:14]) && isAllDigitsStr(s[15:]) {
			intPart := s[:14]
			fracPart := s[15:]
			y, _ := strconv.Atoi(intPart[0:4])
			mo, _ := strconv.Atoi(intPart[4:6])
			d, _ := strconv.Atoi(intPart[6:8])
			h, _ := strconv.Atoi(intPart[8:10])
			mi, _ := strconv.Atoi(intPart[10:12])
			sec, _ := strconv.Atoi(intPart[12:14])
			if mo >= 1 && mo <= 12 && d >= 1 && d <= 31 {
				// Convert fractional to nanoseconds (up to 6 digits = microseconds)
				frac := fracPart
				if len(frac) > 6 {
					frac = frac[:6]
				}
				for len(frac) < 6 {
					frac += "0"
				}
				usec, _ := strconv.Atoi(frac)
				nsec := usec * 1000
				return time.Date(y, time.Month(mo), d, h, mi, sec, nsec, time.UTC), nil
			}
		}
	}
	if isAllDigits {
		switch len(s) {
		case 14: // YYYYMMDDHHMMSS
			y, _ := strconv.Atoi(s[:4])
			mo, _ := strconv.Atoi(s[4:6])
			d, _ := strconv.Atoi(s[6:8])
			h, _ := strconv.Atoi(s[8:10])
			mi, _ := strconv.Atoi(s[10:12])
			sec, _ := strconv.Atoi(s[12:14])
			if mo >= 1 && mo <= 12 && d >= 1 && d <= 31 {
				return time.Date(y, time.Month(mo), d, h, mi, sec, 0, time.UTC), nil
			}
		case 12: // YYMMDDHHMMSS
			yy, _ := strconv.Atoi(s[:2])
			mo, _ := strconv.Atoi(s[2:4])
			d, _ := strconv.Atoi(s[4:6])
			h, _ := strconv.Atoi(s[6:8])
			mi, _ := strconv.Atoi(s[8:10])
			sec, _ := strconv.Atoi(s[10:12])
			y := convert2DigitYear(yy)
			if mo >= 1 && mo <= 12 && d >= 1 && d <= 31 {
				return time.Date(y, time.Month(mo), d, h, mi, sec, 0, time.UTC), nil
			}
		}
	}
	// Try to parse using parseMySQLDateValue for various formats (2-digit year, delimiters, etc.)
	parsed := parseMySQLDateValue(s)
	if parsed != "" {
		// Check if there's a time part after the date
		timePart := ""
		if idx := strings.Index(s, " "); idx >= 0 {
			timePart = strings.TrimSpace(s[idx+1:])
			timePart = normalizeDateTimeSeparators(timePart)
		}
		dateStr := parsed
		if timePart != "" {
			dateStr = parsed + " " + timePart
		}
		for _, f := range formats {
			if t, err := time.Parse(f, dateStr); err == nil {
				return t, nil
			}
		}
		// At least try parsing the date portion
		if t, err := time.Parse("2006-01-02", parsed); err == nil {
			return t, nil
		}
	}
	// Fallback: manually parse YYYY-MM-DD (handles invalid dates like 2009-04-31, 2010-00-01)
	if len(s) >= 10 && (s[4] == '-') && (s[7] == '-') {
		y, ey := strconv.Atoi(s[:4])
		m, em := strconv.Atoi(s[5:7])
		d, ed := strconv.Atoi(s[8:10])
		if ey == nil && em == nil && ed == nil && m >= 0 && m <= 12 && d >= 0 && d <= 31 {
			// Clamp invalid month/day for Go's time.Date (which normalizes)
			if m == 0 {
				m = 1
			}
			if d == 0 {
				d = 1
			}
			// Create approximate time (may not be exact for invalid dates)
			t := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)
			// If there's a time part, parse it
			if len(s) > 10 && s[10] == ' ' {
				parts := strings.Split(s[11:], ":")
				if len(parts) >= 3 {
					h, _ := strconv.Atoi(parts[0])
					mi, _ := strconv.Atoi(parts[1])
					sec, _ := strconv.Atoi(strings.Split(parts[2], ".")[0])
					t = time.Date(y, time.Month(m), d, h, mi, sec, 0, time.UTC)
				}
			}
			return t, nil
		}
	}
	// Handle "YYYY:MM:DD HH:MM:SS[.ffffff]" format (MySQL allows colons as date separators)
	if len(s) >= 10 && s[4] == ':' && s[7] == ':' {
		// Replace first two colons with dashes to get YYYY-MM-DD format
		normalized := s[:4] + "-" + s[5:7] + "-" + s[8:]
		t, err := parseDateTimeValue(normalized)
		if err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse date/time value: %q", s)
}

// addDateMonths adds years and months to a date using MySQL semantics:
// if the resulting day exceeds the last day of the target month, it is clamped

func addDateMonths(t time.Time, years, months int) time.Time {
	// Calculate target year/month
	y := t.Year() + years
	m := int(t.Month()) + months
	// Normalize months
	for m > 12 {
		m -= 12
		y++
	}
	for m < 1 {
		m += 12
		y--
	}
	// Clamp day to last day of target month
	d := t.Day()
	lastDay := daysInMonth(y, time.Month(m))
	if d > lastDay {
		d = lastDay
	}
	return time.Date(y, time.Month(m), d, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), t.Location())
}

func daysInMonth(year int, month time.Month) int {
	return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// parseTimeExtractionValue parses a value for HOUR/MINUTE/SECOND extraction.
// It first tries HHMMSS interpretation for 6-digit integers, then falls

func parseTimeExtractionValue(val interface{}) (time.Time, error) {
	s := toString(val)
	// For all-digit strings, try HHMMSS first for 6 digits (e.g., 230322 = 23:03:22)
	isAllDigits := true
	for _, c := range s {
		if c < '0' || c > '9' {
			isAllDigits = false
			break
		}
	}
	if isAllDigits && len(s) == 6 {
		h, _ := strconv.Atoi(s[:2])
		mi, _ := strconv.Atoi(s[2:4])
		sec, _ := strconv.Atoi(s[4:6])
		if h <= 23 && mi <= 59 && sec <= 59 {
			return time.Date(0, 1, 1, h, mi, sec, 0, time.UTC), nil
		}
	}
	return parseDateTimeValue(val)
}

// extractLeadingInt extracts the leading integer portion of a string.

func extractLeadingInt(s string) string {
	s = strings.TrimSpace(s)
	end := 0
	for i, c := range s {
		if c == '-' && i == 0 {
			end = 1
			continue
		}
		if c >= '0' && c <= '9' {
			end = i + 1
		} else {
			break
		}
	}
	if end == 0 {
		return "0"
	}
	return s[:end]
}

func evalIntervalDateExpr(dateVal, intervalVal interface{}, unit sqlparser.IntervalType, syntax sqlparser.IntervalExprSyntax) (interface{}, error) {
	return evalIntervalDateExprStrict(dateVal, intervalVal, unit, syntax, false)
}

func evalIntervalDateExprStrict(dateVal, intervalVal interface{}, unit sqlparser.IntervalType, syntax sqlparser.IntervalExprSyntax, strict bool) (interface{}, error) {
	// If date or interval value is NULL, result is NULL
	if dateVal == nil || intervalVal == nil {
		return nil, nil
	}
	// Zero dates produce NULL (with a warning in strict mode)
	if isZeroDate(dateVal) {
		if strict {
			return nil, mysqlError(1292, "22007", "Incorrect datetime value: '"+toString(dateVal)+"'")
		}
		return nil, nil
	}
	t, err := parseDateTimeValue(dateVal)
	if err != nil {
		return toString(dateVal), nil
	}
	iStr := toString(intervalVal)
	isSubtract := syntax == sqlparser.IntervalDateExprDateSub || syntax == sqlparser.IntervalDateExprSubdate || syntax == sqlparser.IntervalDateExprBinarySub

	// parseIntervalInt parses an interval integer safely, returning (value, overflow).
	// If the string is too large for int, overflow=true and the date would overflow.
	parseIntervalInt := func(s string) (int, bool) {
		s = extractLeadingInt(s)
		n, err := strconv.Atoi(s)
		if err != nil {
			// overflow or invalid - treat as overflow
			return 0, true
		}
		return n, false
	}

	switch unit {
	case sqlparser.IntervalDay:
		n, ov := parseIntervalInt(iStr)
		if ov {
			if strict {
				return nil, mysqlError(1441, "22008", "Datetime function: datetime field overflow")
			}
			return nil, nil
		}
		if isSubtract {
			n = -n
		}
		t = t.AddDate(0, 0, n)
	case sqlparser.IntervalMonth:
		n, ov := parseIntervalInt(iStr)
		if ov {
			if strict {
				return nil, mysqlError(1441, "22008", "Datetime function: datetime field overflow")
			}
			return nil, nil
		}
		if isSubtract {
			n = -n
		}
		t = addDateMonths(t, 0, n)
	case sqlparser.IntervalYear:
		n, ov := parseIntervalInt(iStr)
		if ov {
			if strict {
				return nil, mysqlError(1441, "22008", "Datetime function: datetime field overflow")
			}
			return nil, nil
		}
		if isSubtract {
			n = -n
		}
		t = addDateMonths(t, n, 0)
	case sqlparser.IntervalHour:
		n, ov := parseIntervalInt(iStr)
		if ov {
			if strict {
				return nil, mysqlError(1441, "22008", "Datetime function: datetime field overflow")
			}
			return nil, nil
		}
		if isSubtract {
			n = -n
		}
		t = addSecondsToTime(t, int64(n)*3600)
	case sqlparser.IntervalMinute:
		n, ov := parseIntervalInt(iStr)
		if ov {
			if strict {
				return nil, mysqlError(1441, "22008", "Datetime function: datetime field overflow")
			}
			return nil, nil
		}
		if isSubtract {
			n = -n
		}
		t = addSecondsToTime(t, int64(n)*60)
	case sqlparser.IntervalSecond:
		n, ov := parseIntervalInt(iStr)
		if ov {
			if strict {
				return nil, mysqlError(1441, "22008", "Datetime function: datetime field overflow")
			}
			return nil, nil
		}
		if isSubtract {
			n = -n
		}
		// Use Unix epoch arithmetic to avoid time.Duration int64 overflow for large values.
		days := n / 86400
		rem := n % 86400
		t = t.AddDate(0, 0, int(days)).Add(time.Duration(rem) * time.Second)
	case sqlparser.IntervalWeek:
		n, ov := parseIntervalInt(iStr)
		if ov {
			if strict {
				return nil, mysqlError(1441, "22008", "Datetime function: datetime field overflow")
			}
			return nil, nil
		}
		if isSubtract {
			n = -n
		}
		t = t.AddDate(0, 0, n*7)
	case sqlparser.IntervalQuarter:
		n, ov := parseIntervalInt(iStr)
		if ov {
			if strict {
				return nil, mysqlError(1441, "22008", "Datetime function: datetime field overflow")
			}
			return nil, nil
		}
		if isSubtract {
			n = -n
		}
		t = addDateMonths(t, 0, n*3)
	case sqlparser.IntervalYearMonth:
		// Format: 'Y M' or 'Y:M' or 'Y-M'
		s := strings.TrimSpace(iStr)
		neg := false
		if strings.HasPrefix(s, "-") {
			neg = true
			s = strings.TrimSpace(s[1:])
		}
		// Split on any separator: space, colon, or dash
		sep := strings.IndexAny(s, " :-")
		var years, months int
		if sep >= 0 {
			years, _ = strconv.Atoi(s[:sep])
			months, _ = strconv.Atoi(strings.TrimSpace(s[sep+1:]))
		} else {
			years, _ = strconv.Atoi(s)
		}
		if neg {
			years = -years
			months = -months
		}
		if isSubtract {
			years = -years
			months = -months
		}
		t = addDateMonths(t, years, months)
	case sqlparser.IntervalHourMinute:
		// Format: 'HH:MM'
		totalSec := parseCompoundInterval(iStr, "hh:mm")
		if isSubtract {
			totalSec = -totalSec
		}
		t = addSecondsToTime(t, totalSec)
	case sqlparser.IntervalHourSecond:
		// Format: 'HH:MM:SS'
		totalSec := parseCompoundInterval(iStr, "hh:mm:ss")
		if isSubtract {
			totalSec = -totalSec
		}
		t = addSecondsToTime(t, totalSec)
	case sqlparser.IntervalMinuteSecond:
		// Format: 'MM:SS'
		totalSec := parseCompoundInterval(iStr, "mm:ss")
		if isSubtract {
			totalSec = -totalSec
		}
		t = addSecondsToTime(t, totalSec)
	case sqlparser.IntervalDayHour:
		// Format: 'D HH'
		totalSec := parseCompoundInterval(iStr, "d hh")
		if isSubtract {
			totalSec = -totalSec
		}
		t = addSecondsToTime(t, totalSec)
	case sqlparser.IntervalDayMinute:
		// Format: 'D HH:MM'
		totalSec := parseCompoundInterval(iStr, "d hh:mm")
		if isSubtract {
			totalSec = -totalSec
		}
		t = addSecondsToTime(t, totalSec)
	case sqlparser.IntervalDaySecond:
		// Format: 'D HH:MM:SS' or 'D H:M:S' - use totalSec to avoid overflow
		totalSec := parseCompoundInterval(iStr, "d hh:mm:ss")
		if isSubtract {
			totalSec = -totalSec
		}
		t = addSecondsToTime(t, totalSec)
	case sqlparser.IntervalMicrosecond:
		// Format: just a number (microseconds)
		n, _ := strconv.ParseInt(strings.TrimSpace(iStr), 10, 64)
		dur := time.Duration(n) * time.Microsecond
		if isSubtract {
			dur = -dur
		}
		t = t.Add(dur)
	case sqlparser.IntervalSecondMicrosecond:
		// Format: 'SS.ffffff'
		dur := parseMicrosecondInterval(iStr, "ss.ffffff")
		if isSubtract {
			dur = -dur
		}
		t = t.Add(dur)
	case sqlparser.IntervalMinuteMicrosecond:
		// Format: 'MM:SS.ffffff'
		dur := parseMicrosecondInterval(iStr, "mm:ss.ffffff")
		if isSubtract {
			dur = -dur
		}
		t = t.Add(dur)
	case sqlparser.IntervalHourMicrosecond:
		// Format: 'HH:MM:SS.ffffff'
		dur := parseMicrosecondInterval(iStr, "hh:mm:ss.ffffff")
		if isSubtract {
			dur = -dur
		}
		t = t.Add(dur)
	case sqlparser.IntervalDayMicrosecond:
		// Format: 'D HH:MM:SS.ffffff'
		dur := parseMicrosecondInterval(iStr, "d hh:mm:ss.ffffff")
		if isSubtract {
			dur = -dur
		}
		t = t.Add(dur)
	default:
		// For ADDDATE/SUBDATE shorthand (IntervalNone), the interval is in days
		if unit == sqlparser.IntervalNone && (syntax == sqlparser.IntervalDateExprAdddate || syntax == sqlparser.IntervalDateExprSubdate) {
			n, ov := parseIntervalInt(iStr)
			if ov {
				if strict {
					return nil, mysqlError(1441, "22008", "Datetime function: datetime field overflow")
				}
				return nil, nil
			}
			if isSubtract {
				n = -n
			}
			t = t.AddDate(0, 0, n)
		} else {
			// Try to parse as a time interval for unsupported types
			dur, _ := parseMySQLTimeInterval(iStr)
			if isSubtract {
				dur = -dur
			}
			t = t.Add(dur)
		}
	}

	// Format output based on whether the original had time component
	ds := toString(dateVal)

	// If the input was a pure TIME value (no date component, format HH:MM:SS),
	// return the result as a TIME string rather than a datetime.
	isPureTime := func(s string) bool {
		// A pure time string has colons but no dashes or spaces before the colon.
		// e.g. "06:07:08", "06:07:08.123456" but not "2003-01-02 06:07:08"
		if dashIdx := strings.IndexByte(s, '-'); dashIdx >= 0 {
			return false
		}
		if spaceIdx := strings.IndexByte(s, ' '); spaceIdx >= 0 {
			return false
		}
		return strings.IndexByte(s, ':') >= 0
	}
	if isPureTime(ds) {
		usec := t.Nanosecond() / 1000
		base := t.Format("15:04:05")
		if usec != 0 {
			base = fmt.Sprintf("%s.%06d", base, usec)
		}
		return base, nil
	}

	// Check for datetime overflow (year out of MySQL's valid range 1000-9999)
	if t.Year() < 1000 || t.Year() > 9999 {
		if strict {
			return nil, mysqlError(1441, "22008", "Datetime function: datetime field overflow")
		}
		return nil, nil
	}

	usec := t.Nanosecond() / 1000
	if strings.Contains(ds, " ") || strings.Contains(ds, ":") || t.Hour() != 0 || t.Minute() != 0 || t.Second() != 0 || usec != 0 {
		base := t.Format("2006-01-02 15:04:05")
		if usec != 0 {
			base = fmt.Sprintf("%s.%06d", base, usec)
		}
		return base, nil
	}
	return t.Format("2006-01-02"), nil
}

// parseMySQLTimeInterval parses MySQL time interval strings like "1 01:01:01" or "01:01:01".
// addTimeStrings adds (or subtracts if subtract=true) two TIME strings using microsecond arithmetic.
// Handles garbage at end of base string (truncates to valid time part).

func addTimeStrings(base, interval string, subtract bool) interface{} {
	// Parse base time - truncate at first non-time character after a valid time
	baseTime := parseTimeStringToMicros(base)
	if baseTime == nil {
		return base // unparseable
	}
	// Parse interval
	intervalTime := parseTimeStringToMicros(interval)
	if intervalTime == nil {
		return base
	}
	baseMicros := *baseTime
	// MySQL clamps the base TIME value to [-838:59:59, 838:59:59] before arithmetic.
	if baseMicros > maxTimeMicros {
		baseMicros = maxTimeMicros
	} else if baseMicros < -maxTimeMicros {
		baseMicros = -maxTimeMicros
	}
	intervalMicros := *intervalTime
	var resultMicros int64
	if subtract {
		resultMicros = baseMicros - intervalMicros
	} else {
		resultMicros = baseMicros + intervalMicros
	}
	return formatMicrosAsTimeString(resultMicros)
}

// timeStringToNumericString converts a TIME string like "01:02:03.456" to its numeric HHMMSS.fff form "10203.456".
// Returns "" if the string doesn't look like a TIME value.
// MySQL stores TIME literals in REAL/FLOAT/DOUBLE columns as the numeric HHMMSS.fff representation.
func timeStringToNumericString(s string) string {
	neg := ""
	if strings.HasPrefix(s, "-") {
		neg = "-"
		s = s[1:]
	}
	// Match HH:MM:SS[.fff] or HHH:MM:SS[.fff]
	parts := strings.SplitN(s, ":", 3)
	if len(parts) != 3 {
		return ""
	}
	h, errH := strconv.Atoi(parts[0])
	m, errM := strconv.Atoi(parts[1])
	if errH != nil || errM != nil || h < 0 || m < 0 || m > 59 {
		return ""
	}
	// Third part may have fractional seconds
	secStr := parts[2]
	fracStr := ""
	if dotIdx := strings.Index(secStr, "."); dotIdx >= 0 {
		fracStr = "." + secStr[dotIdx+1:]
		secStr = secStr[:dotIdx]
	}
	sec, errS := strconv.Atoi(secStr)
	if errS != nil || sec < 0 || sec > 59 {
		return ""
	}
	// Format as HHMMSS[.fff]
	return fmt.Sprintf("%s%d%02d%02d%s", neg, h, m, sec, fracStr)
}

// parseTimeStringToMicros parses a MySQL TIME string to total microseconds.
// Handles negative signs, D HH:MM:SS, HH:MM:SS.ffffff, and garbage at end.

func parseTimeStringToMicros(s string) *int64 {
	s = strings.TrimSpace(s)
	negative := false
	if strings.HasPrefix(s, "-") {
		negative = true
		s = s[1:]
	}
	// Extract valid time chars: digits, colon, dot, space
	end := 0
	for end < len(s) {
		c := s[end]
		if (c >= '0' && c <= '9') || c == ':' || c == '.' || c == ' ' {
			end++
		} else {
			break
		}
	}
	if end == 0 {
		return nil
	}
	s = strings.TrimSpace(s[:end])
	// Parse D HH:MM:SS.ffffff
	days := 0
	if idx := strings.Index(s, " "); idx >= 0 {
		d, err := strconv.Atoi(s[:idx])
		if err == nil {
			days = d
			s = s[idx+1:]
		}
	}
	var h, m, sec, usecs int
	parts := strings.SplitN(s, ":", 3)
	switch len(parts) {
	case 3:
		h, _ = strconv.Atoi(parts[0])
		m, _ = strconv.Atoi(parts[1])
		secStr := parts[2]
		if dotIdx := strings.Index(secStr, "."); dotIdx >= 0 {
			sec, _ = strconv.Atoi(secStr[:dotIdx])
			frac := secStr[dotIdx+1:]
			for len(frac) < 6 {
				frac += "0"
			}
			if len(frac) > 6 {
				frac = frac[:6]
			}
			usecs, _ = strconv.Atoi(frac)
		} else {
			sec, _ = strconv.Atoi(secStr)
		}
	case 2:
		h, _ = strconv.Atoi(parts[0])
		m, _ = strconv.Atoi(parts[1])
	case 1:
		// No colon: parse as right-aligned HHMMSS numeric string.
		// MySQL treats bare numeric time strings with HHMMSS right-alignment:
		//   1-2 digits → SS, 3-4 digits → MMSS (00:MM:SS), 5-6 digits → HHMMSS.
		p := parts[0]
		// Strip fractional part if present
		if dotIdx := strings.Index(p, "."); dotIdx >= 0 {
			fracStr := p[dotIdx+1:]
			p = p[:dotIdx]
			for len(fracStr) < 6 {
				fracStr += "0"
			}
			if len(fracStr) > 6 {
				fracStr = fracStr[:6]
			}
			usecs, _ = strconv.Atoi(fracStr)
		}
		switch {
		case len(p) <= 2:
			sec, _ = strconv.Atoi(p)
		case len(p) <= 4:
			// MMSS
			m, _ = strconv.Atoi(p[:len(p)-2])
			sec, _ = strconv.Atoi(p[len(p)-2:])
		default:
			// HHMMSS (take last 4 as MMSS, rest as HH)
			secPart := p[len(p)-2:]
			minPart := p[len(p)-4 : len(p)-2]
			hourPart := p[:len(p)-4]
			sec, _ = strconv.Atoi(secPart)
			m, _ = strconv.Atoi(minPart)
			h, _ = strconv.Atoi(hourPart)
		}
	default:
		return nil
	}
	total := int64(days)*24*int64(time.Hour/time.Microsecond) +
		int64(h)*int64(time.Hour/time.Microsecond) +
		int64(m)*int64(time.Minute/time.Microsecond) +
		int64(sec)*int64(time.Second/time.Microsecond) +
		int64(usecs)
	if negative {
		total = -total
	}
	return &total
}

// maxTimeMicros is the maximum MySQL TIME value in microseconds: 838:59:59 (no fractional)
// MySQL clamps overflow TIME results to exactly 838:59:59 (without fractional seconds).
const maxTimeMicros int64 = (838*3600 + 59*60 + 59) * 1_000_000

func formatMicrosAsTimeString(micros int64) string {
	negative := micros < 0
	if negative {
		micros = -micros
	}
	// Clamp to MySQL TIME maximum value (838:59:59)
	if micros > maxTimeMicros {
		micros = maxTimeMicros
	}
	us := micros % int64(time.Second/time.Microsecond)
	micros /= int64(time.Second / time.Microsecond)
	sec := int(micros % 60)
	micros /= 60
	mins := int(micros % 60)
	hours := int(micros / 60)
	sign := ""
	if negative {
		sign = "-"
	}
	if us != 0 {
		return fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, hours, mins, sec, us)
	}
	return fmt.Sprintf("%s%02d:%02d:%02d", sign, hours, mins, sec)
}

// addDurationToTime adds a duration in seconds (possibly large) to a time,

func addSecondsToTime(t time.Time, totalSec int64) time.Time {
	days := totalSec / 86400
	rem := totalSec % 86400
	return t.AddDate(0, 0, int(days)).Add(time.Duration(rem) * time.Second)
}

// parseCompoundInterval parses MySQL compound (non-microsecond) interval formats.
// Handles leading negative sign: applies to all components.
// format: "hh:mm", "hh:mm:ss", "mm:ss", "d hh", "d hh:mm"

func parseCompoundInterval(s, format string) int64 {
	s = strings.TrimSpace(s)
	neg := false
	if strings.HasPrefix(s, "-") {
		neg = true
		s = strings.TrimSpace(s[1:])
	}

	var days, hours, mins, secs int64

	switch format {
	case "d hh:mm:ss":
		// D HH:MM:SS - space then colons
		sep := strings.IndexAny(s, " :")
		if sep >= 0 {
			d, _ := strconv.ParseInt(s[:sep], 10, 64)
			days = d
			rest := s[sep+1:]
			parts := strings.SplitN(rest, ":", 3)
			if len(parts) == 3 {
				h, _ := strconv.ParseInt(parts[0], 10, 64)
				m, _ := strconv.ParseInt(parts[1], 10, 64)
				sc, _ := strconv.ParseInt(parts[2], 10, 64)
				hours, mins, secs = h, m, sc
			} else if len(parts) == 2 {
				h, _ := strconv.ParseInt(parts[0], 10, 64)
				m, _ := strconv.ParseInt(parts[1], 10, 64)
				hours, mins = h, m
			} else {
				h, _ := strconv.ParseInt(rest, 10, 64)
				hours = h
			}
		}
	case "d hh:mm":
		// D HH:MM - separator can be space or colon
		sep := strings.IndexAny(s, " :")
		if sep >= 0 {
			d, _ := strconv.ParseInt(s[:sep], 10, 64)
			days = d
			rest := s[sep+1:]
			parts := strings.SplitN(rest, ":", 2)
			if len(parts) == 2 {
				h, _ := strconv.ParseInt(parts[0], 10, 64)
				m, _ := strconv.ParseInt(parts[1], 10, 64)
				hours, mins = h, m
			} else {
				h, _ := strconv.ParseInt(rest, 10, 64)
				hours = h
			}
		}
	case "d hh":
		// D HH - separator can be space or colon
		sep := strings.IndexAny(s, " :")
		if sep >= 0 {
			d, _ := strconv.ParseInt(s[:sep], 10, 64)
			h, _ := strconv.ParseInt(strings.TrimSpace(s[sep+1:]), 10, 64)
			days, hours = d, h
		} else {
			h, _ := strconv.ParseInt(s, 10, 64)
			hours = h
		}
	case "hh:mm:ss":
		// HH:MM:SS
		parts := strings.SplitN(s, ":", 3)
		if len(parts) == 3 {
			h, _ := strconv.ParseInt(parts[0], 10, 64)
			m, _ := strconv.ParseInt(parts[1], 10, 64)
			sc, _ := strconv.ParseInt(parts[2], 10, 64)
			hours, mins, secs = h, m, sc
		}
	case "hh:mm":
		// HH:MM
		parts := strings.SplitN(s, ":", 2)
		if len(parts) == 2 {
			h, _ := strconv.ParseInt(parts[0], 10, 64)
			m, _ := strconv.ParseInt(parts[1], 10, 64)
			hours, mins = h, m
		}
	case "mm:ss":
		// MM:SS (colon separates minutes and seconds)
		parts := strings.SplitN(s, ":", 2)
		if len(parts) == 2 {
			m, _ := strconv.ParseInt(parts[0], 10, 64)
			sc, _ := strconv.ParseInt(parts[1], 10, 64)
			mins, secs = m, sc
		} else {
			m, _ := strconv.ParseInt(s, 10, 64)
			mins = m
		}
	}

	totalSec := days*86400 + hours*3600 + mins*60 + secs
	if neg {
		totalSec = -totalSec
	}
	return totalSec
}

// parseMicrosecondInterval parses MySQL compound microsecond interval formats.
// format parameter indicates the expected format type:
//   - "ss.ffffff": SECOND_MICROSECOND like "10000.999999"
//   - "mm:ss.ffffff": MINUTE_MICROSECOND like "10000:99.999999"
//   - "hh:mm:ss.ffffff": HOUR_MICROSECOND like "10000:99:99.999999"

func parseMicrosecondInterval(s, format string) time.Duration {
	s = strings.TrimSpace(s)
	var days, hours, mins, secs, usecs int64

	switch format {
	case "d hh:mm:ss.ffffff":
		// D HH:MM:SS.ffffff
		if idx := strings.Index(s, " "); idx >= 0 {
			d, _ := strconv.ParseInt(s[:idx], 10, 64)
			days = d
			s = s[idx+1:]
		}
		fallthrough
	case "hh:mm:ss.ffffff":
		// HH:MM:SS.ffffff
		parts := strings.SplitN(s, ":", 3)
		if len(parts) == 3 {
			h, _ := strconv.ParseInt(parts[0], 10, 64)
			m, _ := strconv.ParseInt(parts[1], 10, 64)
			secStr := parts[2]
			hours, mins = h, m
			if dotIdx := strings.Index(secStr, "."); dotIdx >= 0 {
				sec, _ := strconv.ParseInt(secStr[:dotIdx], 10, 64)
				secs = sec
				frac := secStr[dotIdx+1:]
				for len(frac) < 6 {
					frac += "0"
				}
				usecs, _ = strconv.ParseInt(frac[:6], 10, 64)
			} else {
				sc, _ := strconv.ParseInt(secStr, 10, 64)
				secs = sc
			}
		}
	case "mm:ss.ffffff":
		// MM:SS.ffffff (colon separates minutes and seconds)
		parts := strings.SplitN(s, ":", 2)
		if len(parts) == 2 {
			m, _ := strconv.ParseInt(parts[0], 10, 64)
			secStr := parts[1]
			mins = m
			if dotIdx := strings.Index(secStr, "."); dotIdx >= 0 {
				sec, _ := strconv.ParseInt(secStr[:dotIdx], 10, 64)
				secs = sec
				frac := secStr[dotIdx+1:]
				for len(frac) < 6 {
					frac += "0"
				}
				usecs, _ = strconv.ParseInt(frac[:6], 10, 64)
			} else {
				sc, _ := strconv.ParseInt(secStr, 10, 64)
				secs = sc
			}
		} else if len(parts) == 1 {
			// Just seconds.microseconds
			if dotIdx := strings.Index(s, "."); dotIdx >= 0 {
				sec, _ := strconv.ParseInt(s[:dotIdx], 10, 64)
				secs = sec
				frac := s[dotIdx+1:]
				for len(frac) < 6 {
					frac += "0"
				}
				usecs, _ = strconv.ParseInt(frac[:6], 10, 64)
			}
		}
	case "ss.ffffff":
		// SS.ffffff (dot separates seconds and microseconds)
		if dotIdx := strings.Index(s, "."); dotIdx >= 0 {
			sec, _ := strconv.ParseInt(s[:dotIdx], 10, 64)
			secs = sec
			frac := s[dotIdx+1:]
			for len(frac) < 6 {
				frac += "0"
			}
			usecs, _ = strconv.ParseInt(frac[:6], 10, 64)
		} else {
			sec, _ := strconv.ParseInt(s, 10, 64)
			secs = sec
		}
	}

	return time.Duration(days)*24*time.Hour +
		time.Duration(hours)*time.Hour +
		time.Duration(mins)*time.Minute +
		time.Duration(secs)*time.Second +
		time.Duration(usecs)*time.Microsecond
}

func parseMySQLTimeInterval(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	// Handle negative sign: "-01:01:01" means -1h-1m-1s
	negative := false
	if strings.HasPrefix(s, "-") {
		negative = true
		s = s[1:]
	}
	var days, hours, mins, secs, usecs int

	// Format: "D HH:MM:SS" or "HH:MM:SS" or "D"
	if idx := strings.Index(s, " "); idx >= 0 {
		d, err := strconv.Atoi(s[:idx])
		if err != nil {
			return 0, err
		}
		days = d
		s = s[idx+1:]
	}

	parts := strings.Split(s, ":")
	switch len(parts) {
	case 3:
		h, _ := strconv.Atoi(parts[0])
		m, _ := strconv.Atoi(parts[1])
		secPart := parts[2]
		if dotIdx := strings.Index(secPart, "."); dotIdx >= 0 {
			intPart := secPart[:dotIdx]
			fracPart := secPart[dotIdx+1:]
			sc, _ := strconv.Atoi(intPart)
			secs = sc
			// Pad or truncate to 6 digits (microseconds)
			for len(fracPart) < 6 {
				fracPart += "0"
			}
			fracPart = fracPart[:6]
			usecs, _ = strconv.Atoi(fracPart)
		} else {
			sc, _ := strconv.Atoi(secPart)
			secs = sc
		}
		hours, mins = h, m
	case 2:
		h, _ := strconv.Atoi(parts[0])
		m, _ := strconv.Atoi(parts[1])
		hours, mins = h, m
	case 1:
		if parts[0] != "" {
			// A bare integer in TIME context is parsed as HHMMSS format.
			// e.g. "1" → 0:00:01, "100" → 0:01:00, "10000" → 1:00:00.
			// Extract fractional part if present.
			numStr := parts[0]
			var fracStr string
			if dotIdx := strings.Index(numStr, "."); dotIdx >= 0 {
				fracStr = numStr[dotIdx+1:]
				numStr = numStr[:dotIdx]
			}
			// Pad to at least 1 digit for HHMMSS interpretation
			n, _ := strconv.Atoi(numStr)
			// Interpret n as HHMMSS: last 2 digits = seconds, next 2 = minutes, rest = hours
			secs = n % 100
			mins = (n / 100) % 100
			hours = n / 10000
			if fracStr != "" {
				for len(fracStr) < 6 {
					fracStr += "0"
				}
				fracStr = fracStr[:6]
				usecs, _ = strconv.Atoi(fracStr)
			}
		}
	}

	dur := time.Duration(days)*24*time.Hour +
		time.Duration(hours)*time.Hour +
		time.Duration(mins)*time.Minute +
		time.Duration(secs)*time.Second +
		time.Duration(usecs)*time.Microsecond
	if negative {
		dur = -dur
	}
	return dur, nil
}

// mysqlWeekdayNames maps Go weekday to MySQL weekday name.
var mysqlWeekdayNamesDF = [7]string{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"}
var mysqlWeekdayAbbrDF = [7]string{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"}

// Locale-specific month/weekday name tables for date_format.
var mysqlLocaleMonthNames = map[string][12]string{
	"de_DE": {"Januar", "Februar", "März", "April", "Mai", "Juni", "Juli", "August", "September", "Oktober", "November", "Dezember"},
	"ru_RU": {"Января", "Февраля", "Марта", "Апреля", "Мая", "Июня", "Июля", "Августа", "Сентября", "Октября", "Ноября", "Декабря"},
}
var mysqlLocaleMonthAbbr = map[string][12]string{
	"de_DE": {"Jan", "Feb", "Mär", "Apr", "Mai", "Jun", "Jul", "Aug", "Sep", "Okt", "Nov", "Dez"},
	"ru_RU": {"Янв", "Фев", "Мар", "Апр", "Май", "Июн", "Июл", "Авг", "Сен", "Окт", "Ноя", "Дек"},
}
var mysqlLocaleWeekdayNames = map[string][7]string{
	"de_DE": {"Sonntag", "Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag"},
	"ru_RU": {"Воскресенье", "Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота"},
}
var mysqlLocaleWeekdayAbbr = map[string][7]string{
	"de_DE": {"So", "Mo", "Di", "Mi", "Do", "Fr", "Sa"},
	"ru_RU": {"Вск", "Пнд", "Втр", "Срд", "Чтв", "Птн", "Сбт"},
}

func mysqlAdjustedWeekday(t time.Time) time.Weekday {
	wd := t.Weekday()
	// For year 0 dates, Go's proleptic Gregorian calendar is off by 1 from MySQL.
	// MySQL considers year 0 weekdays shifted by +1.
	if t.Year() == 0 {
		wd = (wd + 1) % 7
	}
	return wd
}

// mysqlDateFormat converts a MySQL DATE_FORMAT format string (e.g. "%Y-%m-%d") to a Go time.Time string.

func mysqlDateFormat(t time.Time, format string, locale ...string) string {
	lcLocale := "en_US"
	if len(locale) > 0 && locale[0] != "" {
		lcLocale = locale[0]
	}
	// Helper to get weekday name
	getWeekdayName := func(wd time.Weekday) string {
		if names, ok := mysqlLocaleWeekdayNames[lcLocale]; ok {
			return names[wd]
		}
		return mysqlWeekdayNamesDF[wd]
	}
	getWeekdayAbbr := func(wd time.Weekday) string {
		if abbr, ok := mysqlLocaleWeekdayAbbr[lcLocale]; ok {
			return abbr[wd]
		}
		return mysqlWeekdayAbbrDF[wd]
	}
	getMonthName := func(m time.Month) string {
		if names, ok := mysqlLocaleMonthNames[lcLocale]; ok {
			return names[m-1]
		}
		return t.Format("January")
	}
	getMonthAbbr := func(m time.Month) string {
		if abbr, ok := mysqlLocaleMonthAbbr[lcLocale]; ok {
			return abbr[m-1]
		}
		return t.Format("Jan")
	}

	var sb strings.Builder
	i := 0
	for i < len(format) {
		if format[i] == '%' && i+1 < len(format) {
			i++
			switch format[i] {
			case 'Y':
				sb.WriteString(t.Format("2006"))
			case 'y':
				sb.WriteString(t.Format("06"))
			case 'm':
				sb.WriteString(t.Format("01"))
			case 'c':
				sb.WriteString(fmt.Sprintf("%d", t.Month()))
			case 'd':
				sb.WriteString(t.Format("02"))
			case 'e':
				sb.WriteString(fmt.Sprintf("%d", t.Day()))
			case 'H':
				sb.WriteString(t.Format("15"))
			case 'h', 'I':
				sb.WriteString(t.Format("03"))
			case 'i':
				sb.WriteString(t.Format("04"))
			case 's', 'S':
				sb.WriteString(t.Format("05"))
			case 'p':
				sb.WriteString(t.Format("PM"))
			case 'a':
				sb.WriteString(getWeekdayAbbr(mysqlAdjustedWeekday(t)))
			case 'W':
				sb.WriteString(getWeekdayName(mysqlAdjustedWeekday(t)))
			case 'w':
				sb.WriteString(fmt.Sprintf("%d", mysqlAdjustedWeekday(t)))
			case 'D':
				// Day of month with ordinal suffix (1st, 2nd, 3rd, 4th, ...)
				day := t.Day()
				var suffix string
				switch day {
				case 11, 12, 13:
					suffix = "th"
				default:
					switch day % 10 {
					case 1:
						suffix = "st"
					case 2:
						suffix = "nd"
					case 3:
						suffix = "rd"
					default:
						suffix = "th"
					}
				}
				sb.WriteString(fmt.Sprintf("%d%s", day, suffix))
			case 'f':
				// Microseconds (000000-999999)
				sb.WriteString(fmt.Sprintf("%06d", t.Nanosecond()/1000))
			case 'j':
				sb.WriteString(fmt.Sprintf("%03d", t.YearDay()))
			case 'k':
				// Hour in 24h format without leading zero (0-23)
				sb.WriteString(fmt.Sprintf("%d", t.Hour()))
			case 'l':
				// Hour in 12h format without leading zero (1-12)
				h := t.Hour() % 12
				if h == 0 {
					h = 12
				}
				sb.WriteString(fmt.Sprintf("%d", h))
			case 'M':
				sb.WriteString(getMonthName(t.Month()))
			case 'b':
				sb.WriteString(getMonthAbbr(t.Month()))
			case 'T':
				sb.WriteString(t.Format("15:04:05"))
			case 'r':
				sb.WriteString(t.Format("03:04:05 PM"))
			case 'U':
				// Week number (00-53), Sunday is first day of week (mode 0)
				sb.WriteString(fmt.Sprintf("%02d", mysqlWeekFull(t, 0)))
			case 'u':
				// Week number (00-53), Monday is first day of week (mode 1)
				sb.WriteString(fmt.Sprintf("%02d", mysqlWeekFull(t, 1)))
			case 'V':
				// Week number (01-53), Sunday is first day of week (mode 2)
				sb.WriteString(fmt.Sprintf("%02d", mysqlWeekFull(t, 2)))
			case 'v':
				// Week number (01-53), Monday is first day of week (mode 3, ISO)
				sb.WriteString(fmt.Sprintf("%02d", mysqlWeekFull(t, 3)))
			case 'X':
				// Year for the week where Sunday is first day (mode 2)
				isoY, _ := mysqlWeekYearFull(t, 2)
				sb.WriteString(fmt.Sprintf("%04d", isoY))
			case 'x':
				// Year for the week where Monday is first day (mode 3, ISO)
				isoY, _ := mysqlWeekYearFull(t, 3)
				sb.WriteString(fmt.Sprintf("%04d", isoY))
			case '%':
				sb.WriteByte('%')
			default:
				sb.WriteByte('%')
				sb.WriteByte(format[i])
			}
		} else {
			sb.WriteByte(format[i])
		}
		i++
	}
	return sb.String()
}

func timestampDiff(unit sqlparser.IntervalType, t1, t2 time.Time) int64 {
	switch unit {
	case sqlparser.IntervalMicrosecond:
		return t2.Sub(t1).Microseconds()
	case sqlparser.IntervalSecond:
		return int64(t2.Sub(t1).Seconds())
	case sqlparser.IntervalMinute:
		return int64(t2.Sub(t1).Minutes())
	case sqlparser.IntervalHour:
		return int64(t2.Sub(t1).Hours())
	case sqlparser.IntervalDay:
		return int64(t2.Sub(t1).Hours() / 24)
	case sqlparser.IntervalWeek:
		return int64(t2.Sub(t1).Hours() / (24 * 7))
	case sqlparser.IntervalMonth:
		y1, m1, _ := t1.Date()
		y2, m2, d2 := t2.Date()
		months := int64((y2-y1)*12 + int(m2-m1))
		// Antisymmetric day+time adjustment: if not a complete month, adjust toward zero.
		_, _, d1 := t1.Date()
		// When days are equal, also compare intraday time.
		t1IntraDay := int64(t1.Hour())*3600 + int64(t1.Minute())*60 + int64(t1.Second())
		t2IntraDay := int64(t2.Hour())*3600 + int64(t2.Minute())*60 + int64(t2.Second())
		t2LessThanT1 := d2 < d1 || (d2 == d1 && t2IntraDay < t1IntraDay)
		t2GreaterThanT1 := d2 > d1 || (d2 == d1 && t2IntraDay > t1IntraDay)
		if months > 0 && t2LessThanT1 {
			months--
		} else if months < 0 && t2GreaterThanT1 {
			months++
		}
		return months
	case sqlparser.IntervalQuarter:
		y1, m1, _ := t1.Date()
		y2, m2, d2 := t2.Date()
		months := int64((y2-y1)*12 + int(m2-m1))
		_, _, d1 := t1.Date()
		t1IntraDay := int64(t1.Hour())*3600 + int64(t1.Minute())*60 + int64(t1.Second())
		t2IntraDay := int64(t2.Hour())*3600 + int64(t2.Minute())*60 + int64(t2.Second())
		t2LessThanT1 := d2 < d1 || (d2 == d1 && t2IntraDay < t1IntraDay)
		t2GreaterThanT1 := d2 > d1 || (d2 == d1 && t2IntraDay > t1IntraDay)
		if months > 0 && t2LessThanT1 {
			months--
		} else if months < 0 && t2GreaterThanT1 {
			months++
		}
		return months / 3
	case sqlparser.IntervalYear:
		y1, m1, _ := t1.Date()
		y2, m2, d2 := t2.Date()
		months := int64((y2-y1)*12 + int(m2-m1))
		_, _, d1 := t1.Date()
		t1IntraDay := int64(t1.Hour())*3600 + int64(t1.Minute())*60 + int64(t1.Second())
		t2IntraDay := int64(t2.Hour())*3600 + int64(t2.Minute())*60 + int64(t2.Second())
		t2LessThanT1 := d2 < d1 || (d2 == d1 && t2IntraDay < t1IntraDay)
		t2GreaterThanT1 := d2 > d1 || (d2 == d1 && t2IntraDay > t1IntraDay)
		if months > 0 && t2LessThanT1 {
			months--
		} else if months < 0 && t2GreaterThanT1 {
			months++
		}
		return months / 12
	}
	return 0
}

// mysqlStrToDate parses a date string using a MySQL format string (like STR_TO_DATE).
// Returns (nil, nil) if the string cannot be parsed (format mismatch → NULL result).
// In strict mode, returns (nil, err) with MySQL error 1411 if the string is parsed but
// contains an invalid date component (zero year/month/day, or out-of-range month/day).
// In non-strict mode, invalid date components return (nil, nil) instead of an error.
// When literalFormat is true, returns a smart type-aware format (date, time, or datetime).

func mysqlStrToDate(dateStr, format string, literalFormat bool, strictMode bool) (*string, error) {
	// Use a custom MySQL-compatible parser rather than Go's time.Parse,
	// because MySQL supports specifiers that Go doesn't (e.g. %D, %#, %j, %U, %W).
	p := &mysqlDateParser{s: dateStr, f: format, literalMode: literalFormat}
	if !p.parse() {
		return nil, nil
	}
	if !p.validate() {
		return nil, nil
	}
	// After successful parsing, check for invalid date component values.
	// MySQL error 1411 (HY000): "Incorrect datetime value: '%s' for function str_to_date"
	// In strict mode: return error. In non-strict mode: return NULL silently.
	if p.hasDate || p.hasYear || p.hasMonth || p.hasDay {
		yr, mo, dy := p.resolveDate()
		invalid := false
		if p.hasYear && yr == 0 {
			invalid = true
		} else if p.hasMonth && mo == 0 {
			invalid = true
		} else if p.hasDay && dy == 0 {
			invalid = true
		} else if p.hasMonth && mo > 12 {
			invalid = true
		} else if p.hasDay && dy > 31 {
			invalid = true
		} else if p.hasMonth && !p.hasYear && yr == 0 {
			// Month is present but year was not specified: the implicit year 0000 makes
			// the resulting date invalid (e.g., str_to_date(1, '%m') → 0000-01-00 is invalid).
			invalid = true
		}
		if invalid {
			if strictMode {
				return nil, mysqlError(1411, "HY000", fmt.Sprintf("Incorrect datetime value: '%s' for function str_to_date", dateStr))
			}
			// Non-strict mode: return NULL
			return nil, nil
		}
	}
	result := p.format()
	return &result, nil
}

type mysqlDateParser struct {
	s           string // input string
	f           string // format string
	si          int    // position in s
	fi          int    // position in f
	literalMode bool   // when true, use smart type-aware output format
	// parsed components
	year, month, day           int
	hour, minute, second       int
	microsecond                int
	ampm                       int // 0=unset, 1=AM, 2=PM
	weekday                    int // 0=Sunday..6=Saturday, -1=unset
	yearday                    int // day of year 1..366, 0=unset
	weekU, weeku               int // week number for %U/%u, -1=unset
	weekV, weekv               int // week number for %V/%v, -1=unset
	yearX, yearx               int // year for %X/%x
	hasDate, hasTime, hasMicro bool
	hasYear, hasMonth, hasDay  bool // true if %Y/%y or %m/%c/%M/%b or %d/%e/%D present
	hasAMPM                    bool
	has24hHour                 bool // true if %H or %k was used (24-hour format)
	hasWeekday                 bool // true if %W or %w was used
	hasWeekV                   bool // true if %V was used
	hasWeekv                   bool // true if %v was used
	hasWeekU                   bool // true if %U was used
	hasWeeku                   bool // true if %u was used
	hasYearX                   bool // true if %X was used
	hasYearx                   bool // true if %x was used
}

var mysqlMonthNames = []string{
	"", "January", "February", "March", "April", "May", "June",
	"July", "August", "September", "October", "November", "December",
}
var mysqlMonthAbbr = []string{
	"", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
	"Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
}
var mysqlWeekdayNames = []string{
	"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday",
}
var mysqlWeekdayAbbr = []string{
	"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat",
}

func (p *mysqlDateParser) readInt(maxDigits int) (int, bool) {
	start := p.si
	count := 0
	for p.si < len(p.s) && p.s[p.si] >= '0' && p.s[p.si] <= '9' && count < maxDigits {
		p.si++
		count++
	}
	if count == 0 {
		return 0, false
	}
	n, err := strconv.Atoi(p.s[start:p.si])
	return n, err == nil
}

// readExactInt reads exactly n digits. Returns false if fewer than n digits are available.
// Used for fixed-width format specifiers like %m (month 01-12), %d (day 01-31), etc.
// If there are more digits than n following, they are left unconsumed (same as readInt).
func (p *mysqlDateParser) readExactInt(n int) (int, bool) {
	start := p.si
	count := 0
	for p.si < len(p.s) && p.s[p.si] >= '0' && p.s[p.si] <= '9' && count < n {
		p.si++
		count++
	}
	if count < n {
		// Not enough digits: reset and fail
		p.si = start
		return 0, false
	}
	val, err := strconv.Atoi(p.s[start:p.si])
	return val, err == nil
}

func (p *mysqlDateParser) readFrac() (int, bool) {
	// Read up to 6 digits, pad to 6
	start := p.si
	count := 0
	for p.si < len(p.s) && p.s[p.si] >= '0' && p.s[p.si] <= '9' && count < 6 {
		p.si++
		count++
	}
	// skip extra digits
	for p.si < len(p.s) && p.s[p.si] >= '0' && p.s[p.si] <= '9' {
		p.si++
	}
	if count == 0 {
		return 0, false
	}
	frac := p.s[start : start+count]
	for len(frac) < 6 {
		frac += "0"
	}
	n, err := strconv.Atoi(frac)
	return n, err == nil
}

func (p *mysqlDateParser) matchLiteral(lit string) bool {
	if p.si+len(lit) > len(p.s) {
		return false
	}
	if strings.EqualFold(p.s[p.si:p.si+len(lit)], lit) {
		p.si += len(lit)
		return true
	}
	return false
}

func (p *mysqlDateParser) skipNonDigits() {
	for p.si < len(p.s) && (p.s[p.si] < '0' || p.s[p.si] > '9') {
		p.si++
	}
}

func (p *mysqlDateParser) parse() bool {
	p.year, p.month, p.day = 0, 0, 0
	p.hour, p.minute, p.second, p.microsecond = 0, 0, 0, 0
	p.ampm = 0
	p.weekday = -1
	p.yearday = 0
	p.weekU, p.weeku, p.weekV, p.weekv = -1, -1, -1, -1
	p.yearX, p.yearx = 0, 0

	for p.fi < len(p.f) {
		if p.f[p.fi] == '%' && p.fi+1 < len(p.f) {
			p.fi++
			spec := p.f[p.fi]
			p.fi++
			switch spec {
			case 'Y': // 4-digit year (MySQL also handles 2-digit with century expansion)
				startSI := p.si
				n, ok := p.readInt(4)
				if !ok {
					return false
				}
				// If only 2 digits were read, apply MySQL 2-digit year rule
				if p.si-startSI <= 2 && n < 100 {
					if n < 70 {
						p.year = 2000 + n
					} else {
						p.year = 1900 + n
					}
				} else {
					p.year = n
				}
				p.hasDate = true
				p.hasYear = true
			case 'y': // 2-digit year
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				if n < 70 {
					p.year = 2000 + n
				} else {
					p.year = 1900 + n
				}
				p.hasDate = true
				p.hasYear = true
			case 'm': // month 01-12
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.month = n
				p.hasDate = true
				p.hasMonth = true
			case 'c': // month 1-12 (no leading zero)
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.month = n
				p.hasDate = true
				p.hasMonth = true
			case 'd': // day 01-31
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.day = n
				p.hasDate = true
				p.hasDay = true
			case 'e': // day 1-31 (no leading zero)
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.day = n
				p.hasDate = true
				p.hasDay = true
			case 'D': // day with ordinal suffix (1st, 2nd, 3rd...)
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.day = n
				p.hasDate = true
				p.hasDay = true
				// Skip ordinal suffix (st, nd, rd, th)
				for p.si < len(p.s) && p.s[p.si] >= 'a' && p.s[p.si] <= 'z' {
					p.si++
				}
			case 'H': // hour 00-23 (optional at end of input, 24-hour)
				p.has24hHour = true
				if p.si >= len(p.s) {
					p.hasTime = true
					break
				}
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.hour = n
				p.hasTime = true
			case 'h', 'I': // hour 01-12 (optional at end of input)
				if p.si >= len(p.s) {
					p.hasTime = true
					break
				}
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.hour = n
				p.hasTime = true
			case 'k': // hour 0-23 (no leading zero, 24-hour)
				p.has24hHour = true
				if p.si >= len(p.s) {
					p.hasTime = true
					break
				}
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.hour = n
				p.hasTime = true
			case 'l': // hour 1-12 (no leading zero)
				if p.si >= len(p.s) {
					p.hasTime = true
					break
				}
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.hour = n
				p.hasTime = true
			case 'i': // minutes (optional at end of input)
				if p.si >= len(p.s) {
					break
				}
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.minute = n
				p.hasTime = true
			case 's', 'S': // seconds (optional at end of input)
				if p.si >= len(p.s) {
					break
				}
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.second = n
				p.hasTime = true
			case 'f': // microseconds (optional - if not present, default to 0)
				p.hasMicro = true // %f in format always indicates microsecond output
				if p.si < len(p.s) && p.s[p.si] >= '0' && p.s[p.si] <= '9' {
					n, ok := p.readFrac()
					if ok {
						p.microsecond = n
					}
				}
				p.hasTime = true
			case 'p': // AM/PM
				// skip optional whitespace
				for p.si < len(p.s) && p.s[p.si] == ' ' {
					p.si++
				}
				if p.si+2 <= len(p.s) {
					am := strings.ToUpper(p.s[p.si : p.si+2])
					if am == "AM" {
						p.ampm = 1
						p.si += 2
						p.hasAMPM = true
					} else if am == "PM" {
						p.ampm = 2
						p.si += 2
						p.hasAMPM = true
					} else {
						return false
					}
				} else {
					return false
				}
			case 'T': // HH:MM:SS
				h, ok1 := p.readInt(2)
				if !ok1 || p.si >= len(p.s) || p.s[p.si] != ':' {
					return false
				}
				p.si++
				m, ok2 := p.readInt(2)
				if !ok2 || p.si >= len(p.s) || p.s[p.si] != ':' {
					return false
				}
				p.si++
				s, ok3 := p.readInt(2)
				if !ok3 {
					return false
				}
				p.hour, p.minute, p.second = h, m, s
				p.hasTime = true
			case 'r': // HH:MM:SS AM/PM
				h, ok1 := p.readInt(2)
				if !ok1 || p.si >= len(p.s) || p.s[p.si] != ':' {
					return false
				}
				p.si++
				m, ok2 := p.readInt(2)
				if !ok2 || p.si >= len(p.s) || p.s[p.si] != ':' {
					return false
				}
				p.si++
				s, ok3 := p.readInt(2)
				if !ok3 {
					return false
				}
				p.hour, p.minute, p.second = h, m, s
				p.hasTime = true
				// read space and AM/PM
				for p.si < len(p.s) && p.s[p.si] == ' ' {
					p.si++
				}
				if p.si+2 <= len(p.s) {
					am := strings.ToUpper(p.s[p.si : p.si+2])
					if am == "AM" {
						p.ampm = 1
						p.si += 2
						p.hasAMPM = true
					} else if am == "PM" {
						p.ampm = 2
						p.si += 2
						p.hasAMPM = true
					}
				}
			case 'M': // full month name
				matched := false
				for i := 1; i <= 12; i++ {
					if p.matchLiteral(mysqlMonthNames[i]) {
						p.month = i
						matched = true
						p.hasDate = true
						p.hasMonth = true
						break
					}
					// also try 3-letter prefix match for abbreviated
				}
				if !matched {
					// try abbreviated match (first 3-4 chars)
					for i := 1; i <= 12; i++ {
						for l := 3; l <= len(mysqlMonthNames[i]); l++ {
							if p.si+l <= len(p.s) && strings.EqualFold(p.s[p.si:p.si+l], mysqlMonthNames[i][:l]) {
								if l == len(mysqlMonthNames[i]) || (p.si+l < len(p.s) && (p.s[p.si+l] < 'a' || p.s[p.si+l] > 'z') && (p.s[p.si+l] < 'A' || p.s[p.si+l] > 'Z')) {
									p.month = i
									p.si += l
									matched = true
									p.hasDate = true
									p.hasMonth = true
									break
								}
							}
						}
						if matched {
							break
						}
					}
				}
				if !matched {
					return false
				}
			case 'b': // abbreviated month name
				matched := false
				for i := 1; i <= 12; i++ {
					if p.matchLiteral(mysqlMonthAbbr[i]) {
						p.month = i
						matched = true
						p.hasDate = true
						p.hasMonth = true
						break
					}
				}
				if !matched {
					// try case-insensitive match of full month up to 3 chars
					for i := 1; i <= 12; i++ {
						for l := 3; l <= len(mysqlMonthNames[i]); l++ {
							if p.si+l <= len(p.s) && strings.EqualFold(p.s[p.si:p.si+l], mysqlMonthNames[i][:l]) {
								if l == len(mysqlMonthNames[i]) || (p.si+l < len(p.s) && !((p.s[p.si+l] >= 'a' && p.s[p.si+l] <= 'z') || (p.s[p.si+l] >= 'A' && p.s[p.si+l] <= 'Z'))) {
									p.month = i
									p.si += l
									matched = true
									p.hasDate = true
									p.hasMonth = true
									break
								}
							}
						}
						if matched {
							break
						}
					}
				}
				if !matched {
					return false
				}
			case 'W': // full weekday name
				p.hasWeekday = true
				matched := false
				for i, name := range mysqlWeekdayNames {
					// try full match first
					if p.matchLiteral(name) {
						p.weekday = i
						matched = true
						break
					}
				}
				if !matched {
					// try abbreviated (3 char min)
					for i, name := range mysqlWeekdayNames {
						for l := 3; l <= len(name); l++ {
							if p.si+l <= len(p.s) && strings.EqualFold(p.s[p.si:p.si+l], name[:l]) {
								if l == len(name) || (p.si+l < len(p.s) && !((p.s[p.si+l] >= 'a' && p.s[p.si+l] <= 'z') || (p.s[p.si+l] >= 'A' && p.s[p.si+l] <= 'Z'))) {
									p.weekday = i
									p.si += l
									matched = true
									break
								}
							}
						}
						if matched {
							break
						}
					}
				}
				if !matched {
					// MySQL silently ignores unrecognized weekday names
				}
			case 'a': // abbreviated weekday name
				for _, name := range mysqlWeekdayAbbr {
					if p.matchLiteral(name) {
						break
					}
				}
			case 'j': // day of year 001-366
				n, ok := p.readInt(3)
				if !ok {
					return false
				}
				p.yearday = n
				p.hasDate = true
			case 'U': // week 00-53 (first day=Sunday)
				p.hasWeekU = true
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.weekU = n
			case 'u': // week 00-53 (first day=Monday, ISO)
				p.hasWeeku = true
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.weeku = n
			case 'V': // week 01-53 (first day=Sunday, used with %X)
				p.hasWeekV = true
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.weekV = n
			case 'v': // week 01-53 (first day=Monday, used with %x)
				p.hasWeekv = true
				n, ok := p.readInt(2)
				if !ok {
					return false
				}
				p.weekv = n
			case 'X': // year for %V (4 digits, Sunday-based)
				p.hasYearX = true
				n, ok := p.readInt(4)
				if !ok {
					return false
				}
				p.yearX = n
				p.hasDate = true
				p.hasYear = true
			case 'x': // year for %v (4 digits, Monday-based ISO)
				p.hasYearx = true
				n, ok := p.readInt(4)
				if !ok {
					return false
				}
				p.yearx = n
				p.hasDate = true
				p.hasYear = true
			case 'w': // weekday 0=Sunday..6=Saturday
				p.hasWeekday = true
				n, ok := p.readInt(1)
				if !ok {
					return false
				}
				if n > 6 { // MySQL: 0=Sunday..6=Saturday, 7 is invalid
					return false
				}
				p.weekday = n
			case '#', '@', '.': // skip non-numeric chars (%#, %@, and %. all skip non-digits)
				p.skipNonDigits()
			case '%': // literal %
				if p.si >= len(p.s) || p.s[p.si] != '%' {
					return false
				}
				p.si++
			default:
				// unknown specifier, skip char in input
				if p.si < len(p.s) {
					p.si++
				}
			}
		} else {
			// literal char in format must match char in input.
			// MySQL is lenient: if a non-alphanumeric separator doesn't match,
			// parsing stops but returns what was parsed so far (not a failure).
			fc := p.f[p.fi]
			p.fi++
			if p.si >= len(p.s) {
				// MySQL is lenient with trailing format chars
				continue
			}
			if p.s[p.si] != fc {
				isAlpha := func(c byte) bool {
					return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
				}
				isNonAlnum := func(c byte) bool {
					return (c < '0' || c > '9') && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z')
				}
				if isNonAlnum(fc) {
					// Format expects a separator but input has something different.
					// Check what comes NEXT in the format (after this separator).
					restFormat := p.f[p.fi:]
					// If the next format item is %f (microseconds), and the input at the
					// current position is alpha (like 'A' for AM), that means the fractional
					// part is completely absent. The separator before %f is required when
					// %p follows %f (because otherwise AM/PM can't be parsed correctly).
					if len(restFormat) >= 2 && restFormat[0] == '%' && restFormat[1] == 'f' {
						// Check if there's a %p after %f
						afterF := restFormat[2:]
						// strip the optional space between %f and %p
						afterFtrimmed := strings.TrimLeft(afterF, " \t")
						if len(afterFtrimmed) >= 2 && afterFtrimmed[0] == '%' && (afterFtrimmed[1] == 'p' || afterFtrimmed[1] == 'P') {
							// If input is alphabetic (like A of AM), the `.` separator before %f
							// didn't match and %f+%p combination would be ambiguous -> return false
							if isAlpha(p.s[p.si]) {
								return false
							}
						}
					}
					// Try to skip the format separator and continue (MySQL leniency).
					// Don't advance input - the next format specifier will try to match.
					continue
				}
				// Alphanumeric literal mismatch - MySQL stops parsing here
				// and returns what was parsed so far (not NULL).
				// We break out of the loop and return true.
				p.fi = len(p.f) // stop processing format
				continue
			}
			p.si++
		}
	}
	return true
}

func (p *mysqlDateParser) validate() bool {
	// %H or %k (24-hour) combined with %p (AM/PM) is invalid in MySQL
	if p.has24hHour && p.hasAMPM {
		return false
	}
	// %V (Sunday-based week) requires %X (Sunday-based year), not %x or plain %Y
	if p.hasWeekV && !p.hasYearX {
		return false
	}
	// %v (ISO/Monday-based week) requires %x (ISO year), not %X or plain %Y
	if p.hasWeekv && !p.hasYearx {
		return false
	}
	// %X (Sunday-based year) should be used with %V, not %v
	if p.hasYearX && p.hasWeekv && !p.hasWeekV {
		return false
	}
	// %x (ISO year) should be used with %v, not %V
	if p.hasYearx && p.hasWeekV && !p.hasWeekv {
		return false
	}
	// %u (Monday-based week) should use %Y (not %x which is for %v/ISO)
	// Mixing %u with %x is invalid
	if p.hasWeeku && p.hasYearx {
		return false
	}
	return true
}

func (p *mysqlDateParser) resolveDate() (yr, mo, dy int) {
	yr, mo, dy = p.year, p.month, p.day

	// If we have week+year info, compute date from that
	if p.weekU >= 0 && yr > 0 {
		// %U + %Y: week number (Sunday-based)
		// Find Jan 1 of year, then compute week
		jan1 := time.Date(yr, 1, 1, 0, 0, 0, 0, time.UTC)
		// First Sunday on or before Jan 1
		offset := int(jan1.Weekday()) // 0=Sunday
		firstSunday := jan1.AddDate(0, 0, -offset)
		// Week 0 starts on firstSunday (if Jan 1 is not Sunday, week 0 = last week of prev year)
		// Week 1 starts on first Sunday >= Jan 1
		if offset == 0 {
			// Jan 1 is Sunday, week 0 = that week
			firstSunday = jan1
		}
		target := firstSunday.AddDate(0, 0, p.weekU*7)
		if p.weekday >= 0 {
			// adjust to the correct weekday
			diff := p.weekday - int(target.Weekday())
			target = target.AddDate(0, 0, diff)
		}
		yr, mo, dy = target.Year(), int(target.Month()), target.Day()
		return
	}
	if p.weeku >= 0 && yr > 0 {
		// %u + %Y: ISO-like week number (Monday-based)
		jan1 := time.Date(yr, 1, 1, 0, 0, 0, 0, time.UTC)
		offset := int(jan1.Weekday()) // 0=Sunday
		// Week 1 starts on first Monday
		// Adjust: Monday=0 for ISO
		mondayOffset := (offset + 6) % 7 // days from last Monday to Jan 1
		firstMonday := jan1.AddDate(0, 0, -mondayOffset)
		if mondayOffset == 0 {
			firstMonday = jan1
		}
		// Week 0 = the week before the first Monday
		target := firstMonday.AddDate(0, 0, (p.weeku-1)*7)
		if p.weeku == 0 {
			target = firstMonday.AddDate(0, 0, -7)
		} else {
			target = firstMonday.AddDate(0, 0, (p.weeku-1)*7)
		}
		if p.weekday >= 0 {
			diff := p.weekday - int(target.Weekday())
			if diff < 0 {
				diff += 7
			}
			target = target.AddDate(0, 0, diff)
		}
		yr, mo, dy = target.Year(), int(target.Month()), target.Day()
		return
	}
	if p.weekV >= 0 && p.yearX > 0 {
		// %V + %X: Sunday-based week, year from %X
		jan1 := time.Date(p.yearX, 1, 1, 0, 0, 0, 0, time.UTC)
		offset := int(jan1.Weekday())
		var firstSunday time.Time
		if offset == 0 {
			firstSunday = jan1
		} else {
			firstSunday = jan1.AddDate(0, 0, 7-offset)
		}
		target := firstSunday.AddDate(0, 0, (p.weekV-1)*7)
		if p.weekday >= 0 {
			diff := p.weekday - int(target.Weekday())
			target = target.AddDate(0, 0, diff)
		}
		yr, mo, dy = target.Year(), int(target.Month()), target.Day()
		return
	}
	if p.weekv >= 0 && p.yearx > 0 {
		// %v + %x: ISO week, year from %x
		// Find the first Monday of the first ISO week of yearx
		jan1 := time.Date(p.yearx, 1, 1, 0, 0, 0, 0, time.UTC)
		_, isoWeek1 := jan1.ISOWeek()
		var isoFirstMonday time.Time
		if isoWeek1 == 1 {
			// Jan 1 is in ISO week 1, find the Monday of that week
			wd := int(jan1.Weekday())
			if wd == 0 {
				wd = 7
			}
			isoFirstMonday = jan1.AddDate(0, 0, 1-wd)
		} else {
			// Jan 1 is in last week of previous year, find first ISO week 1 Monday
			isoFirstMonday = jan1.AddDate(0, 0, 8-int(jan1.Weekday()))
			if jan1.Weekday() == 0 {
				isoFirstMonday = jan1.AddDate(0, 0, 1)
			}
		}
		target := isoFirstMonday.AddDate(0, 0, (p.weekv-1)*7)
		if p.weekday >= 0 {
			diff := p.weekday - int(target.Weekday())
			if diff < 0 {
				diff += 7
			}
			target = target.AddDate(0, 0, diff)
		}
		yr, mo, dy = target.Year(), int(target.Month()), target.Day()
		return
	}
	if p.yearday > 0 && yr > 0 {
		// %j + %Y: day of year
		t := time.Date(yr, 1, p.yearday, 0, 0, 0, 0, time.UTC)
		yr, mo, dy = t.Year(), int(t.Month()), t.Day()
	}
	return
}

func (p *mysqlDateParser) format() string {
	// Apply AM/PM adjustment
	h := p.hour
	if p.hasAMPM {
		if p.ampm == 1 { // AM
			if h == 12 {
				h = 0
			}
		} else if p.ampm == 2 { // PM
			if h != 12 {
				h += 12
			}
		}
	}

	yr, mo, dy := p.resolveDate()

	// Determine the result format based on which specifiers were used.
	hasFullDate := p.hasYear || p.hasMonth    // has year or month info → calendar date context
	hasDayOffset := p.hasDate && !hasFullDate // %d/%j but no year/month → day-offset for time

	if p.literalMode {
		// Smart type-aware output: use the minimal format suggested by the format string.
		if hasFullDate && p.hasTime {
			// Full datetime
			if p.hasMicro {
				return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d",
					yr, mo, dy, h, p.minute, p.second, p.microsecond)
			}
			return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
				yr, mo, dy, h, p.minute, p.second)
		}
		if hasFullDate {
			// Date only
			return fmt.Sprintf("%04d-%02d-%02d", yr, mo, dy)
		}
		if p.hasTime {
			// Time only or day-offset + time → return as time string
			dayOffset := 0
			if hasDayOffset {
				dayOffset = dy
			}
			totalH := h + dayOffset*24
			frac := ""
			if p.hasMicro {
				frac = fmt.Sprintf(".%06d", p.microsecond)
			}
			if totalH < 0 {
				return fmt.Sprintf("-%02d:%02d:%02d%s", -totalH, p.minute, p.second, frac)
			}
			return fmt.Sprintf("%02d:%02d:%02d%s", totalH, p.minute, p.second, frac)
		}
		if hasDayOffset {
			// Day only (no time specifiers) → return as date with 0000-00-DD
			return fmt.Sprintf("0000-00-%02d", dy)
		}
	}

	// Default mode (or fallback for literal mode): return full datetime(6) format.
	// When time-only (no date): use 0000-00-00 prefix.
	if !p.hasDate && p.hasTime {
		// Time-only with no date: use 0000-00-00 as date prefix
		return fmt.Sprintf("0000-00-00 %02d:%02d:%02d.%06d",
			h, p.minute, p.second, p.microsecond)
	}
	// Day+time with no full date: keep day in date position (non-literal mode)
	// Full datetime or date-only: return YYYY-MM-DD HH:MM:SS.ffffff
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d",
		yr, mo, dy, h, p.minute, p.second, p.microsecond)
}

func mysqlFormatToGoLayout(format string) string {
	var sb strings.Builder
	i := 0
	for i < len(format) {
		if format[i] == '%' && i+1 < len(format) {
			i++
			switch format[i] {
			case 'Y':
				sb.WriteString("2006")
			case 'y':
				sb.WriteString("06")
			case 'm':
				sb.WriteString("01")
			case 'c':
				sb.WriteString("1")
			case 'd':
				sb.WriteString("02")
			case 'e':
				sb.WriteString("2")
			case 'H':
				sb.WriteString("15")
			case 'h', 'I':
				sb.WriteString("03")
			case 'k':
				sb.WriteString("15") // 24-hour unpadded, Go doesn't have exact match
			case 'l':
				sb.WriteString("3")
			case 'i':
				sb.WriteString("04")
			case 's', 'S':
				sb.WriteString("05")
			case 'p':
				sb.WriteString("PM")
			case 'T':
				sb.WriteString("15:04:05")
			case 'r':
				sb.WriteString("03:04:05 PM")
			case 'f':
				sb.WriteString("000000")
			case 'W':
				sb.WriteString("Monday")
			case 'M':
				sb.WriteString("January")
			case 'b':
				sb.WriteString("Jan")
			case 'a':
				sb.WriteString("Mon")
			case '%':
				sb.WriteByte('%')
			default:
				sb.WriteByte('%')
				sb.WriteByte(format[i])
			}
		} else {
			sb.WriteByte(format[i])
		}
		i++
	}
	return sb.String()
}

func mysqlGetFormat(dateType, locale string) string {
	switch dateType {
	case "DATE":
		switch locale {
		case "USA":
			return "%m.%d.%Y"
		case "JIS", "ISO":
			return "%Y-%m-%d"
		case "EUR":
			return "%d.%m.%Y"
		case "INTERNAL":
			return "%Y%m%d"
		}
	case "DATETIME", "TIMESTAMP":
		switch locale {
		case "USA":
			return "%Y-%m-%d %H.%i.%s"
		case "JIS", "ISO":
			return "%Y-%m-%d %H:%i:%s"
		case "EUR":
			return "%Y-%m-%d %H.%i.%s"
		case "INTERNAL":
			return "%Y%m%d%H%i%s"
		}
	case "TIME":
		switch locale {
		case "USA":
			return "%h:%i:%s %p"
		case "JIS", "ISO":
			return "%H:%i:%s"
		case "EUR":
			return "%H.%i.%s"
		case "INTERNAL":
			return "%H%i%s"
		}
	}
	return ""
}

func compareMySQLTimeOrdering(a, b string) (int, bool) {
	ta := parseMySQLTimeValue(strings.TrimSpace(a))
	tb := parseMySQLTimeValue(strings.TrimSpace(b))
	ma, okA := parseCanonicalTimeMicros(ta)
	mb, okB := parseCanonicalTimeMicros(tb)
	if !okA || !okB {
		return 0, false
	}
	if ma < mb {
		return -1, true
	}
	if ma > mb {
		return 1, true
	}
	return 0, true
}

func parseCanonicalTimeMicros(s string) (int64, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	sign := int64(1)
	if strings.HasPrefix(s, "-") {
		sign = -1
		s = s[1:]
	}
	main := s
	frac := ""
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		main = s[:dot]
		frac = s[dot+1:]
	}
	parts := strings.Split(main, ":")
	if len(parts) != 3 {
		return 0, false
	}
	h, errH := strconv.ParseInt(parts[0], 10, 64)
	m, errM := strconv.ParseInt(parts[1], 10, 64)
	sec, errS := strconv.ParseInt(parts[2], 10, 64)
	if errH != nil || errM != nil || errS != nil {
		return 0, false
	}
	if m < 0 || m > 59 || sec < 0 || sec > 59 {
		return 0, false
	}
	micros := int64(0)
	if frac != "" {
		for len(frac) < 6 {
			frac += "0"
		}
		if len(frac) > 6 {
			frac = frac[:6]
		}
		u, err := strconv.ParseInt(frac, 10, 64)
		if err != nil {
			return 0, false
		}
		micros = u
	}
	total := ((h*3600 + m*60 + sec) * 1_000_000) + micros
	return sign * total, true
}

// normalizeDateTimeForCompare ensures two date/time string values are
// compared using the same granularity. When one looks like a TIME (HH:MM:SS)
// and the other looks like a DATETIME (YYYY-MM-DD HH:MM:SS), the DATETIME is

func normalizeDateTimeForCompare(a, b string) (string, string) {
	aIsDate := isDateString(a)
	bIsDate := isDateString(b)
	aIsTime := isTimeString(a)
	bIsTime := isTimeString(b)
	aIsDatetime := isDatetimeString(a)
	bIsDatetime := isDatetimeString(b)

	// TIME vs DATETIME: extract time part from datetime
	if aIsTime && bIsDatetime {
		if idx := strings.Index(b, " "); idx >= 0 {
			b = b[idx+1:]
		}
	} else if bIsTime && aIsDatetime {
		if idx := strings.Index(a, " "); idx >= 0 {
			a = a[idx+1:]
		}
	}

	// DATE vs DATETIME: extend DATE to DATETIME by appending " 00:00:00"
	if aIsDate && !aIsDatetime && bIsDatetime {
		a = a + " 00:00:00"
	} else if bIsDate && !bIsDatetime && aIsDatetime {
		b = b + " 00:00:00"
	}

	// Datetime fractional precision alignment: when both are datetime strings
	// with different fractional digit counts, truncate the longer one to the
	// shorter one's length. This matches MySQL's TIME_TRUNCATE_FRACTIONAL behavior
	// where literals are truncated to match the column's display precision.
	// E.g.: '2001-01-01 00:00:00.999999' (stored in DATETIME(6)) compared with
	//       '2001-01-01 00:00:00.9999998' (7-digit literal) → truncate to 6 digits.
	aDotIdx := strings.LastIndex(a, ".")
	bDotIdx := strings.LastIndex(b, ".")
	if aDotIdx > 10 && bDotIdx > 10 { // both have fractional parts in a datetime context
		aFrac := a[aDotIdx+1:]
		bFrac := b[bDotIdx+1:]
		if len(aFrac) != len(bFrac) {
			// Truncate the longer to match the shorter
			if len(aFrac) > len(bFrac) {
				a = a[:aDotIdx+1+len(bFrac)]
			} else {
				b = b[:bDotIdx+1+len(aFrac)]
			}
		}
	}

	return a, b
}

func isDateString(s string) bool {
	return len(s) >= 10 && s[4] == '-' && s[7] == '-' && (len(s) == 10 || s[10] == ' ')
}

func isTimeString(s string) bool {
	if len(s) < 5 || len(s) > 8 {
		return false
	}
	// HH:MM:SS or H:MM:SS
	parts := strings.Split(s, ":")
	return len(parts) == 3
}

func isDatetimeString(s string) bool {
	return len(s) == 19 && s[4] == '-' && s[7] == '-' && s[10] == ' ' && s[13] == ':' && s[16] == ':'
}

// isDatetimeWithTimeComponent returns true if s is a full DATETIME string with a time component
// (e.g., '2006-07-16 10:30:00'), as opposed to a plain DATE string ('2006-07-16').
// For ADDTIME/SUBTIME, only DATETIME (not DATE) strings should use datetime arithmetic.
func isDatetimeWithTimeComponent(s string) bool {
	if !isDatetimeLikeString(s) {
		return false
	}
	// Must have space or 'T' separator after date part (position 10)
	return len(s) > 10 && (s[10] == ' ' || s[10] == 'T')
}

// isDateLikeButInvalid returns true if s looks like a date/datetime string (YYYY-MM-DD...)

func isDateLikeButInvalid(s string) bool {
	if len(s) < 10 || s[4] != '-' || s[7] != '-' {
		return false
	}
	// It looks like a date - check validity
	y, ey := strconv.Atoi(s[:4])
	m, em := strconv.Atoi(s[5:7])
	d, ed := strconv.Atoi(s[8:10])
	if ey != nil || em != nil || ed != nil {
		return false
	}
	return !isValidDate(y, m, d)
}

// isStringBuildingExpr returns true when the SQL expression is a CONCAT or similar operation
// that produces a string value. Used to detect when UNIX_TIMESTAMP should return DECIMAL.

func isDatetimeLikeString(s string) bool {
	// A datetime string has format YYYY-MM-DD HH:MM:SS[.ffffff] or YYYY:MM:DD HH:MM:SS
	// The key indicator: 4 digits followed by '-' or ':' at position 4, and another '-' or ':' at position 7.
	// The first 4 characters must all be digits (year).
	if len(s) >= 10 {
		// Check first 4 chars are digits (year part)
		for i := 0; i < 4; i++ {
			if s[i] < '0' || s[i] > '9' {
				return false
			}
		}
		sep := s[4]
		if (sep == '-' || sep == ':') && s[7] == sep {
			return true
		}
	}
	return false
}

func validateWhereForInvalidDatetime(expr sqlparser.Expr) error {
	if expr == nil {
		return nil
	}
	var walkErr error
	sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if walkErr != nil {
			return false, nil
		}
		comp, ok := node.(*sqlparser.ComparisonExpr)
		if !ok {
			return true, nil
		}
		// Check right side for invalid datetime literal
		if lit, ok := comp.Right.(*sqlparser.Literal); ok {
			if lit.Type == sqlparser.StrVal {
				val := lit.Val
				if invalidStr, invalid := hasInvalidTimeComponent(val); invalid {
					walkErr = mysqlError(1292, "HY000", fmt.Sprintf("Incorrect DATETIME value: '%s'", invalidStr))
					return false, nil
				}
			}
		}
		// Check left side too
		if lit, ok := comp.Left.(*sqlparser.Literal); ok {
			if lit.Type == sqlparser.StrVal {
				val := lit.Val
				if invalidStr, invalid := hasInvalidTimeComponent(val); invalid {
					walkErr = mysqlError(1292, "HY000", fmt.Sprintf("Incorrect DATETIME value: '%s'", invalidStr))
					return false, nil
				}
			}
		}
		return true, nil
	}, expr)
	return walkErr
}

// hasInvalidDateString checks if a string that looks like a date (YYYY-MM-DD format)
// is actually an invalid date. Returns the invalid string and true if invalid.
// The sqlMode parameter controls which dates are considered invalid:
// - Without ALLOW_INVALID_DATES: rejects out-of-range months/days and zero dates (with NO_ZERO_DATE)
// - With ALLOW_INVALID_DATES: allows day values up to 31 for any month, but rejects impossible day > 31

func hasInvalidDateString(s string, sqlMode string) (string, bool) {
	// Only check strings that look like dates (YYYY-MM-DD or similar)
	// Must contain a hyphen and enough characters
	if len(s) < 8 || !strings.ContainsRune(s, '-') {
		// Check if it's trying to be a date but fails (e.g., "wrong-date")
		// A string with '-' but not parseable as date
		if strings.ContainsRune(s, '-') {
			return s, true
		}
		return "", false
	}

	allowInvalidDates := strings.Contains(sqlMode, "ALLOW_INVALID_DATES")
	isNoZeroDate := strings.Contains(sqlMode, "NO_ZERO_DATE") || strings.Contains(sqlMode, "TRADITIONAL")

	// Strip time part if present
	datePart := s
	if idx := strings.IndexByte(s, ' '); idx >= 0 {
		datePart = s[:idx]
	}
	if idx := strings.IndexByte(s, 'T'); idx >= 0 {
		datePart = s[:idx]
	}

	// Parse YYYY-MM-DD
	parts := strings.Split(datePart, "-")
	if len(parts) != 3 {
		// Not a recognizable date format - it's invalid
		return s, true
	}

	yStr := strings.TrimSpace(parts[0])
	mStr := strings.TrimSpace(parts[1])
	dStr := strings.TrimSpace(parts[2])

	// Validate all parts are numeric
	for _, c := range yStr {
		if c < '0' || c > '9' {
			return s, true
		}
	}
	for _, c := range mStr {
		if c < '0' || c > '9' {
			return s, true
		}
	}
	for _, c := range dStr {
		if c < '0' || c > '9' {
			return s, true
		}
	}

	y, _ := strconv.Atoi(yStr)
	m, _ := strconv.Atoi(mStr)
	d, _ := strconv.Atoi(dStr)

	// Check for zero date (0000-00-00)
	if y == 0 && m == 0 && d == 0 {
		if isNoZeroDate {
			return s, true
		}
		return "", false
	}

	if allowInvalidDates {
		// In ALLOW_INVALID_DATES: allow zero month/day and any day 0-31
		// Only reject impossible values like month > 12 or day > 31
		if m > 12 {
			return s, true
		}
		if d > 31 {
			return s, true
		}
		return "", false
	}

	// Check for partial zero dates (zero month or day) in strict modes
	isNoZeroInDate := strings.Contains(sqlMode, "NO_ZERO_IN_DATE") || strings.Contains(sqlMode, "TRADITIONAL")
	if m == 0 || d == 0 {
		if isNoZeroInDate {
			return s, true
		}
		return "", false
	}

	// Check month range (strict mode) - only for non-zero months
	if m < 1 || m > 12 {
		return s, true
	}

	// Standard mode: check exact day range per month
	if d < 1 {
		return s, true
	}
	daysInMonth := [13]int{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	maxDay := daysInMonth[m]
	if m == 2 && isLeapYear(y) {
		maxDay = 29
	}
	if d > maxDay {
		return s, true
	}

	return "", false
}

// validateWhereForInvalidDateColumns walks a WHERE expression and checks for invalid date
// string literals used in comparisons against DATE/DATETIME columns.

func validateWhereForInvalidDateColumns(expr sqlparser.Expr, tableDef *catalog.TableDef, sqlMode string) error {
	if expr == nil {
		return nil
	}
	if tableDef == nil {
		return nil
	}

	// Build a map of column name -> type for quick lookup
	colTypes := make(map[string]string, len(tableDef.Columns))
	for _, col := range tableDef.Columns {
		colTypes[strings.ToLower(col.Name)] = strings.ToUpper(col.Type)
	}

	var walkErr error
	sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if walkErr != nil {
			return false, nil
		}
		comp, ok := node.(*sqlparser.ComparisonExpr)
		if !ok {
			return true, nil
		}
		// Only check ordered comparisons (< > <= >=) and equality
		switch comp.Operator {
		case sqlparser.LessThanOp, sqlparser.GreaterThanOp, sqlparser.LessEqualOp,
			sqlparser.GreaterEqualOp, sqlparser.EqualOp, sqlparser.NotEqualOp:
		default:
			return true, nil
		}

		// Helper to check if an expression is a DATE/DATETIME column
		isDateCol := func(e sqlparser.Expr) bool {
			col, ok := e.(*sqlparser.ColName)
			if !ok {
				return false
			}
			colName := strings.ToLower(col.Name.String())
			colType, exists := colTypes[colName]
			if !exists {
				return false
			}
			return colType == "DATE" || strings.HasPrefix(colType, "DATETIME") || strings.HasPrefix(colType, "TIMESTAMP")
		}

		// Check: date_col op string_literal
		var strLit string
		var hasStrLit bool
		var isDateColumn bool

		if lit, ok := comp.Right.(*sqlparser.Literal); ok && lit.Type == sqlparser.StrVal {
			if isDateCol(comp.Left) {
				strLit = lit.Val
				hasStrLit = true
				isDateColumn = true
			}
		}
		if !hasStrLit {
			if lit, ok := comp.Left.(*sqlparser.Literal); ok && lit.Type == sqlparser.StrVal {
				if isDateCol(comp.Right) {
					strLit = lit.Val
					hasStrLit = true
					isDateColumn = true
				}
			}
		}

		if hasStrLit && isDateColumn {
			// Check if this string looks like a date but is invalid
			// First check: does it look like a datetime (has both date and time components)?
			if strings.ContainsRune(strLit, ' ') && strings.ContainsRune(strLit, ':') {
				// Let the existing DATETIME validator handle this
				if invalidStr, invalid := hasInvalidTimeComponent(strLit); invalid {
					walkErr = mysqlError(1292, "HY000", fmt.Sprintf("Incorrect DATETIME value: '%s'", invalidStr))
					return false, nil
				}
			}
			// Check as a DATE string
			if invalidStr, invalid := hasInvalidDateString(strLit, sqlMode); invalid {
				walkErr = mysqlError(1292, "HY000", fmt.Sprintf("Incorrect DATE value: '%s'", invalidStr))
				return false, nil
			}
		}

		return true, nil
	}, expr)
	return walkErr
}

// validateIndexHints checks that all index names referenced in USE KEY / IGNORE KEY / FORCE KEY

