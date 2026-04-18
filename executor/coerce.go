package executor

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/myuon/mylite/catalog"
)

// coerceDateTimeValue truncates datetime values to match the column type.
// For DATE columns, "2007-02-13 15:09:33" becomes "2007-02-13".
// For TIME columns, "2007-02-13 15:09:33" becomes "15:09:33".
// For YEAR columns, "2007-02-13 15:09:33" becomes "2007".
func coerceDateTimeValue(colType string, v interface{}) interface{} {
	return coerceDateTimeValueWithSession(colType, v, nil)
}

// coerceDateTimeValueWithSession is like coerceDateTimeValue but uses sessionTime to
// determine the current date when converting TIME values to TIMESTAMP columns.
// If sessionTime is nil, TIME -> TIMESTAMP conversion returns "0000-00-00 00:00:00".
// timeTruncateFractional should be true when the TIME_TRUNCATE_FRACTIONAL SQL mode is active;
// in that mode, a TIME-format string like "10:10:10.999999" stored into TIMESTAMP gets the
// session date prepended. Without that mode, such strings are treated as invalid datetimes (→ zero).
func coerceDateTimeValueWithSession(colType string, v interface{}, sessionTime *time.Time) interface{} {
	return coerceDateTimeValueWithSessionEx(colType, v, sessionTime, false)
}

func coerceDateTimeValueWithSessionEx(colType string, v interface{}, sessionTime *time.Time, timeTruncateFractional bool) interface{} {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	// Convert ScaledValue to its decimal string representation so downstream
	// date/time parsing can handle it correctly (e.g. DECIMAL(30,6) column value).
	if sv, ok := v.(ScaledValue); ok {
		v = fmt.Sprintf("%."+strconv.Itoa(sv.Scale)+"f", sv.Value)
	}
	s := fmt.Sprintf("%v", v)
	if len(s) == 0 {
		// Empty string for YEAR -> "0000"
		if strings.HasPrefix(upper, "YEAR") {
			return "0000"
		}
		// Empty string -> zero date/datetime
		switch upper {
		case "DATE":
			return "0000-00-00"
		case "DATETIME":
			return "0000-00-00 00:00:00"
		case "TIMESTAMP":
			return "0000-00-00 00:00:00"
		}
		return v
	}
	// Normalize YEAR(4) -> YEAR
	if strings.HasPrefix(upper, "YEAR(") {
		upper = "YEAR"
	}
	// Extract TIME precision: TIME -> 0, TIME(N) -> N
	timeFsp := 0
	isTimeType := false
	if upper == "TIME" {
		isTimeType = true
	} else if strings.HasPrefix(upper, "TIME(") {
		isTimeType = true
		fmt.Sscanf(upper, "TIME(%d)", &timeFsp)
	}
	if isTimeType {
		upper = "TIME"
	}
	// Extract DATETIME(N) / TIMESTAMP(N) precision and normalize to base type.
	dtFsp := -1 // -1 means no precision specified (plain DATETIME/TIMESTAMP)
	if strings.HasPrefix(upper, "DATETIME(") {
		fmt.Sscanf(upper, "DATETIME(%d)", &dtFsp)
		upper = "DATETIME"
	} else if strings.HasPrefix(upper, "TIMESTAMP(") {
		fmt.Sscanf(upper, "TIMESTAMP(%d)", &dtFsp)
		upper = "TIMESTAMP"
	}
	switch upper {
	case "DATE":
		// If the value looks like a datetime, truncate to date-only
		if len(s) > 10 && s[4] == '-' && s[7] == '-' {
			dateStr := s[:10]
			// Validate the extracted date
			parsed := parseMySQLDateValue(dateStr)
			if parsed != "" {
				return parsed
			}
			return "0000-00-00"
		}
		// Try parsing various MySQL DATE formats
		parsed := parseMySQLDateValue(s)
		if parsed != "" {
			return parsed
		}
		// Invalid date value -> zero date
		return "0000-00-00"
	case "TIME":
		// Use the original value for numeric handling
		result := parseMySQLTimeValueRaw(v)
		// Invalid TIME text should coerce to zero time in non-strict behavior.
		if strings.Count(result, ":") != 2 {
			result = "00:00:00"
		}
		// Apply TIME precision: round fractional seconds to timeFsp digits
		return applyTimePrecision(result, timeFsp)
	case "YEAR":
		return coerceYearValue(v)
	case "TIMESTAMP":
		// Handle TIME-only values (HH:MM:SS[.ffffff]) being stored into a TIMESTAMP column.
		// This behavior (prepend session date) is only active in TIME_TRUNCATE_FRACTIONAL mode.
		// Without that mode, a TIME-like string (e.g. '10:45:15') stored into TIMESTAMP is
		// treated as an invalid datetime and becomes '0000-00-00 00:00:00'.
		if timeTruncateFractional {
			isTimeOnlyStr := func(st string) bool {
				// Must contain ':' and not contain '-' (which would indicate a date part)
				return strings.Contains(st, ":") && !strings.Contains(st, "-")
			}
			if isTimeOnlyStr(s) {
				// s is a TIME-only string like "10:10:10.999999"
				var datePrefix string
				if sessionTime != nil {
					datePrefix = sessionTime.Format("2006-01-02")
				} else {
					return "0000-00-00 00:00:00"
				}
				s = datePrefix + " " + s
			}
		}
		// Handle compact numeric YYYYMMDDHHMMSS[.ffffff] format by converting to
		// standard "YYYY-MM-DD HH:MM:SS[.ffffff]" before further processing.
		// This handles DECIMAL values like 20010101101010.999999 stored in TIMESTAMP.
		{
			isAllDigitStr := func(st string) bool {
				for _, c := range st {
					if c < '0' || c > '9' {
						return false
					}
				}
				return len(st) > 0
			}
			tsDotIdx := strings.IndexByte(s, '.')
			tsIntPart, tsFracPart := s, ""
			if tsDotIdx >= 0 {
				tsIntPart = s[:tsDotIdx]
				tsFracPart = s[tsDotIdx+1:]
			}
			if isAllDigitStr(tsIntPart) && len(tsIntPart) == 14 {
				y, _ := strconv.Atoi(tsIntPart[0:4])
				mo, _ := strconv.Atoi(tsIntPart[4:6])
				d, _ := strconv.Atoi(tsIntPart[6:8])
				h, _ := strconv.Atoi(tsIntPart[8:10])
				mi, _ := strconv.Atoi(tsIntPart[10:12])
				sec, _ := strconv.Atoi(tsIntPart[12:14])
				s = fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", y, mo, d, h, mi, sec)
				if tsFracPart != "" {
					s += "." + tsFracPart
				}
			}
		}
		// Apply TIMESTAMP(N) fractional precision if specified.
		if dtFsp >= 0 {
			if dotIdx2 := strings.IndexByte(s, '.'); dotIdx2 > 10 {
				if dtFsp == 0 {
					s = s[:dotIdx2]
				} else {
					frac := s[dotIdx2+1:]
					for len(frac) < dtFsp {
						frac += "0"
					}
					if len(frac) > dtFsp {
						frac = frac[:dtFsp]
					}
					s = s[:dotIdx2+1] + frac
				}
			} else if dtFsp > 0 && len(s) >= 19 && s[4] == '-' {
				// No fractional part but dtFsp > 0: pad with zeros
				s = s + "." + strings.Repeat("0", dtFsp)
			}
		} else {
			// Plain TIMESTAMP: strip fractional seconds
			if dotIdx2 := strings.IndexByte(s, '.'); dotIdx2 > 10 {
				s = s[:dotIdx2]
			}
		}
		// Save the full time part (including fractional) before parseMySQLDateValue strips it.
		// parseMySQLDateValue only returns the date (10 chars), so we extract the time here.
		fullTimeForTS := ""
		if spIdx := strings.IndexAny(s, " \tT"); spIdx >= 0 {
			fullTimeForTS = strings.TrimSpace(s[spIdx+1:])
		} else if len(s) >= 19 && s[4] == '-' && s[7] == '-' {
			// no space found but could be edge case - skip
		}
		// Try parsing various date formats first
		parsed := parseMySQLDateValue(s)
		if parsed == "" {
			// Invalid date value -> zero timestamp
			return "0000-00-00 00:00:00"
		}
		s = parsed
		// Append the saved time part (which includes fractional seconds)
		if fullTimeForTS != "" {
			timePart := normalizeDateTimeSeparators(fullTimeForTS)
			s = s + " " + timePart
		} else if len(s) == 10 {
			timePart := extractTimePart(v, s)
			if timePart != "" {
				timePart = normalizeDateTimeSeparators(timePart)
				s = s + " " + timePart
			}
		}
		// TIMESTAMP range: '1970-01-01 00:00:01' to '2038-01-19 03:14:07' UTC
		// Out-of-range values are stored as '0000-00-00 00:00:00'
		if len(s) >= 10 && s[4] == '-' {
			// Try to parse full datetime for accurate range check.
			// Strip fractional seconds for parsing (time.Parse doesn't handle them in the base format).
			checkStr := s
			if len(checkStr) > 19 && checkStr[19] == '.' {
				checkStr = checkStr[:19]
			}
			var t time.Time
			var parseErr error
			if len(checkStr) >= 19 && checkStr[10] == ' ' {
				t, parseErr = time.Parse("2006-01-02 15:04:05", checkStr[:19])
			} else {
				t, parseErr = time.Parse("2006-01-02", checkStr[:10])
			}
			if parseErr == nil {
				minTS := time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC)
				maxTS := time.Date(2038, 1, 19, 3, 14, 7, 0, time.UTC)
				if t.Before(minTS) || t.After(maxTS) {
					return "0000-00-00 00:00:00"
				}
			}
		}
		// Ensure TIMESTAMP always has time component
		if len(s) == 10 && s[4] == '-' && s[7] == '-' {
			s = s + " 00:00:00"
		}
		return s
	case "DATETIME":
		// Determine if the raw value is numeric (integer type)
		isNumericV := false
		switch v.(type) {
		case int64, uint64, float64:
			isNumericV = true
		}
		isAllDigitStr := func(st string) bool {
			for _, c := range st {
				if c < '0' || c > '9' {
					return false
				}
			}
			return len(st) > 0
		}

		// Track if the original value was a compact numeric string (all digits or numeric type).
		// We only validate time-part range for compact numeric inputs; delimited strings (with
		// separators like '-' or ':') are stored verbatim for out-of-range time components in
		// non-strict mode to match MySQL/Dolt behavior.
		wasCompactNumeric := isNumericV || isAllDigitStr(s)

		// Handle ISO-8601 T-separator without dashes: "20030102T131415" (15 chars) -> "20030102131415"
		if len(s) == 15 && s[8] == 'T' && isAllDigitStr(s[:8]) && isAllDigitStr(s[9:]) {
			s = s[:8] + s[9:]
			wasCompactNumeric = true
		}

		// Handle compact numeric datetime with fractional seconds: "YYYYMMDDHHMMSS.ffffff"
		// Split at decimal point, remember fractional part for later.
		var compactFrac string
		if dotIdx := strings.IndexByte(s, '.'); dotIdx >= 0 && isAllDigitStr(s[:dotIdx]) && len(s[:dotIdx]) == 14 {
			compactFrac = s[dotIdx+1:]
			s = s[:dotIdx]
			wasCompactNumeric = true
		}

		// Pad compact numeric strings to nearest parseable length.
		// Integers: left-pad (no leading digits in numeric value).
		// Strings: right-pad (zero-fill trailing fields).
		if isAllDigitStr(s) {
			needPadLen := 0
			switch len(s) {
			case 7, 9, 10, 11:
				needPadLen = 12
			case 13:
				needPadLen = 14
			}
			if needPadLen > 0 {
				if isNumericV {
					s = strings.Repeat("0", needPadLen-len(s)) + s
				} else {
					s = s + strings.Repeat("0", needPadLen-len(s))
				}
			}
		}

		// Try parsing various date formats
		parsed := parseMySQLDateValue(s)
		if parsed != "" {
			// Check for year > 9999 (parseMySQLDateValue may return "10000-12-02")
			if dashIdx := strings.Index(parsed, "-"); dashIdx > 0 {
				if y, err := strconv.Atoi(parsed[:dashIdx]); err == nil && y > 9999 {
					return "0000-00-00 00:00:00"
				}
			}
			timePart := extractTimePart(v, s)
			// For compact decimal datetime "YYYYMMDDHHMMSS.ffffff", extractTimePart may fail
			// because the original v has a dot. Extract time directly from stripped s in this case.
			if timePart == "" && compactFrac != "" && len(s) == 14 && isAllDigitStr(s) {
				h, _ := strconv.Atoi(s[8:10])
				m2, _ := strconv.Atoi(s[10:12])
				sec2, _ := strconv.Atoi(s[12:14])
				timePart = fmt.Sprintf("%02d:%02d:%02d", h, m2, sec2)
			}
			if timePart != "" {
				// Normalize time separator chars to ':'
				timePart = normalizeDateTimeSeparators(timePart)
				// Expand compact 6-digit time "131415" -> "13:14:15"
				timePart = expandCompactTime(timePart)
				// Zero-pad single-digit hour: "1:01:01" -> "01:01:01"
				if colonCount := strings.Count(timePart, ":"); colonCount == 2 {
					parts := strings.SplitN(timePart, ":", 3)
					if len(parts[0]) == 1 {
						timePart = "0" + timePart
					}
				}
				// Extract fractional seconds from timePart if present.
				var fracPart string
				if dotIdx := strings.Index(timePart, "."); dotIdx >= 0 {
					fracPart = timePart[dotIdx+1:]
					timePart = timePart[:dotIdx]
				}
				// Use compactFrac (from "YYYYMMDDHHMMSS.ffffff" splitting) if no frac in timePart.
				if fracPart == "" && compactFrac != "" {
					fracPart = compactFrac
				}
				// Validate: H<=23, M<=59, S<=59.
				// Only apply this check for compact numeric inputs (all-digit strings, integers).
				// Delimited strings like "2009-01-01 23:59:60" are stored verbatim in non-strict mode.
				if wasCompactNumeric && !isValidTimePart(timePart) {
					return "0000-00-00 00:00:00"
				}
				// Special case: if parsed is "0000-00-00" but has a non-zero time,
				// and original string has a 2-digit year like "00-00-00 HH:MM:SS",
				// re-interpret the 2-digit "00" year as 2000.
				if parsed == "0000-00-00" && timePart != "00:00:00" {
					origDatePart := s
					if spIdx := strings.IndexAny(s, " T"); spIdx >= 0 {
						origDatePart = s[:spIdx]
					}
					origNorm := normalizeDateDelimiters(origDatePart)
					origParts := strings.Split(origNorm, "-")
					if len(origParts) == 3 {
						yStr := origParts[0]
						if len(yStr) <= 2 {
							yy, _ := strconv.Atoi(yStr)
							y2 := convert2DigitYear(yy)
							if y2 != 0 {
								parsed = fmt.Sprintf("%04d-00-00", y2)
							}
						}
					}
				}
				// Apply fractional seconds precision.
				if dtFsp >= 0 {
					// DATETIME(N): keep exactly N fractional digits (pad with zeros or truncate).
					// MySQL always displays DATETIME(N) with exactly N fractional digits.
					if dtFsp == 0 {
						// DATETIME(0) or plain DATETIME: drop fractional part.
						return parsed + " " + timePart
					}
					frac := fracPart
					for len(frac) < dtFsp {
						frac += "0"
					}
					if len(frac) > dtFsp {
						frac = frac[:dtFsp]
					}
					return parsed + " " + timePart + "." + frac
				} else if dtFsp < 0 {
					// Plain DATETIME (no precision specified): strip fractional seconds.
					// fracPart is ignored.
				}
				return parsed + " " + timePart
			}
			return parsed + " 00:00:00"
		}
		// Invalid date value -> zero datetime
		return "0000-00-00 00:00:00"
	}
	return v
}

// expandCompactTime converts a 6-digit all-numeric string "HHMMSS" to "HH:MM:SS".
func expandCompactTime(s string) string {
	if len(s) != 6 {
		return s
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return s
		}
	}
	return s[:2] + ":" + s[2:4] + ":" + s[4:]
}

// isValidTimePart validates HH:MM:SS time for DATETIME coercion.
// Returns false if H>23, M>59, or S>59.
func isValidTimePart(timePart string) bool {
	parts := strings.SplitN(timePart, ":", 3)
	if len(parts) != 3 {
		return true
	}
	h, err := strconv.Atoi(parts[0])
	if err != nil || h > 23 {
		return false
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil || m > 59 {
		return false
	}
	secStr := parts[2]
	if dotIdx := strings.Index(secStr, "."); dotIdx >= 0 {
		secStr = secStr[:dotIdx]
	}
	sec, err := strconv.Atoi(secStr)
	if err != nil || sec > 59 {
		return false
	}
	return true
}

// extractTimePart extracts the time component from a datetime value.
// It handles both string values with space/T separators and numeric YYYYMMDDHHMMSS/YYMMDDHHMMSS formats.
func extractTimePart(v interface{}, parsedDate string) string {
	origS := fmt.Sprintf("%v", v)

	// Check for space- or T-separated time part (e.g., "98-12-31 11:30:45" or "2001-01-01T01:01:01")
	for _, sep := range []byte{' ', 'T'} {
		if idx := strings.IndexByte(origS, sep); idx >= 0 {
			timePart := strings.TrimSpace(origS[idx+1:])
			if timePart != "" {
				// Only strip trailing non-time characters if the time part already uses
				// proper colon separators (HH:MM:SS). If it uses other separators like
				// 11*30*45 or 11+30+45, let normalizeDateTimeSeparators handle it.
				if strings.Contains(timePart, ":") {
					end := 0
					for end < len(timePart) {
						c := timePart[end]
						if (c >= '0' && c <= '9') || c == ':' || c == '.' {
							end++
						} else {
							break
						}
					}
					timePart = timePart[:end]
				}
				if timePart != "" {
					return timePart
				}
			}
		}
	}

	// Check for numeric YYYYMMDDHHMMSS or YYMMDDHHMMSS format
	isAllDigits := true
	for _, c := range origS {
		if c < '0' || c > '9' {
			isAllDigits = false
			break
		}
	}
	if isAllDigits {
		switch len(origS) {
		case 14: // YYYYMMDDHHMMSS
			h, _ := strconv.Atoi(origS[8:10])
			m, _ := strconv.Atoi(origS[10:12])
			sec, _ := strconv.Atoi(origS[12:14])
			return fmt.Sprintf("%02d:%02d:%02d", h, m, sec)
		case 12: // YYMMDDHHMMSS
			h, _ := strconv.Atoi(origS[6:8])
			m, _ := strconv.Atoi(origS[8:10])
			sec, _ := strconv.Atoi(origS[10:12])
			return fmt.Sprintf("%02d:%02d:%02d", h, m, sec)
		}
	}

	// Also handle numeric int64/float64 values that were converted to string
	switch n := v.(type) {
	case int64:
		if n >= 10000000000 { // At least YYMMDDHHMMSS
			sec := int(n % 100)
			m := int((n / 100) % 100)
			h := int((n / 10000) % 100)
			return fmt.Sprintf("%02d:%02d:%02d", h, m, sec)
		}
	case float64:
		in := int64(n)
		if in >= 10000000000 { // At least YYMMDDHHMMSS
			sec := int(in % 100)
			m := int((in / 100) % 100)
			h := int((in / 10000) % 100)
			return fmt.Sprintf("%02d:%02d:%02d", h, m, sec)
		}
	case uint64:
		if n >= 10000000000 { // At least YYMMDDHHMMSS
			sec := int(n % 100)
			m := int((n / 100) % 100)
			h := int((n / 10000) % 100)
			return fmt.Sprintf("%02d:%02d:%02d", h, m, sec)
		}
	}

	return ""
}

// parseMySQLTimeValueRaw handles raw interface values for TIME conversion.
// This properly handles numeric values (int64, float64) using HHMMSS format.
func parseMySQLTimeValueRaw(v interface{}) string {
	const maxTimeHours = 838
	switch n := v.(type) {
	case int64:
		negative := n < 0
		if negative {
			n = -n
		}
		sec := int(n % 100)
		m := int((n / 100) % 100)
		h := int(n / 10000)
		// If hours exceed TIME max, clamp to 838:59:59 regardless of m/sec validity.
		if h > maxTimeHours {
			sign := ""
			if negative {
				sign = "-"
			}
			return sign + "838:59:59"
		}
		return formatTimeValue(negative, h, m, sec, "")
	case float64:
		negative := n < 0
		if negative {
			n = -n
		}
		intPart := int64(n)
		fracPart := n - float64(intPart)
		sec := int(intPart % 100)
		m := int((intPart / 100) % 100)
		h := int(intPart / 10000)
		frac := ""
		if fracPart > 0 {
			fracStr := fmt.Sprintf("%.6f", fracPart)
			frac = strings.TrimPrefix(fracStr, "0.")
			frac = strings.TrimRight(frac, "0")
		}
		// If hours exceed TIME max, clamp to 838:59:59 regardless of m/sec validity.
		if h > maxTimeHours {
			sign := ""
			if negative {
				sign = "-"
			}
			return sign + "838:59:59"
		}
		return formatTimeValue(negative, h, m, sec, frac)
	case uint64:
		sec := int(n % 100)
		m := int((n / 100) % 100)
		h := int(n / 10000)
		// If hours exceed TIME max, clamp to 838:59:59 regardless of m/sec validity.
		if h > maxTimeHours {
			return "838:59:59"
		}
		return formatTimeValue(false, h, m, sec, "")
	}
	return parseMySQLTimeValue(fmt.Sprintf("%v", v))
}

// parseMySQLTimeValue parses a value into MySQL TIME format (HH:MM:SS).
// Supports various MySQL TIME input formats and clips to valid range.
func parseMySQLTimeValue(s string) string {
	if s == "" {
		return "00:00:00"
	}

	// If the value looks like a datetime (YYYY-MM-DD HH:MM:SS), extract the time part.
	// Special case: 0000-00-DD represents a day offset for TIME values (e.g. from str_to_date %d %H).
	// In that case, convert days to hours and add to the time component.
	if len(s) >= 19 && s[4] == '-' && s[7] == '-' && s[10] == ' ' {
		year, _ := strconv.Atoi(s[:4])
		month, _ := strconv.Atoi(s[5:7])
		if year == 0 && month == 0 {
			day, _ := strconv.Atoi(s[8:10])
			if day > 0 {
				// Convert to D HH:MM:SS.ffffff form and let the colon-based parser handle it
				return parseMySQLTimeValue(fmt.Sprintf("%d %s", day, s[11:]))
			}
		}
		return s[11:]
	}
	// If the value looks like a date (YYYY-MM-DD), return 00:00:00
	if len(s) == 10 && s[4] == '-' && s[7] == '-' {
		return "00:00:00"
	}

	// Already in HH:MM:SS or H:MM:SS format (with optional sign and fractional seconds)
	if strings.Contains(s, ":") {
		// Handle "D HH:MM:SS" format (days prefix)
		negative := false
		if strings.HasPrefix(s, "-") {
			negative = true
			s = s[1:]
		}
		days := 0
		if idx := strings.Index(s, " "); idx >= 0 {
			d, err := strconv.Atoi(strings.TrimSpace(s[:idx]))
			if err == nil {
				days = d
				s = strings.TrimSpace(s[idx+1:])
			}
		}
		parts := strings.Split(s, ":")
		h, m, sec := 0, 0, 0
		frac := ""
		switch len(parts) {
		case 2: // HH:MM
			h, _ = strconv.Atoi(parts[0])
			m, _ = strconv.Atoi(parts[1])
		case 3: // HH:MM:SS or HH:MM:SS.frac
			h, _ = strconv.Atoi(parts[0])
			m, _ = strconv.Atoi(parts[1])
			secParts := strings.SplitN(parts[2], ".", 2)
			sec, _ = strconv.Atoi(secParts[0])
			if len(secParts) > 1 {
				frac = secParts[1]
			}
		}
		h += days * 24
		return formatTimeValue(negative, h, m, sec, frac)
	}

	// Handle "D HH" format (e.g., "0 10" -> "10:00:00")
	// MySQL interprets 'D val' as D days + val hours (not seconds)
	if idx := strings.Index(s, " "); idx >= 0 {
		negative := false
		if strings.HasPrefix(s, "-") {
			negative = true
			s = s[1:]
			idx--
		}
		days, err := strconv.Atoi(strings.TrimSpace(s[:idx]))
		if err == nil {
			rest := strings.TrimSpace(s[idx+1:])
			hh, _ := strconv.Atoi(rest)
			h := days*24 + hh
			return formatTimeValue(negative, h, 0, 0, "")
		}
	}

	// Handle numeric and string "HHMMSS" or "SS" format
	negative := false
	if strings.HasPrefix(s, "-") {
		negative = true
		s = s[1:]
	}

	// Handle fractional seconds in numeric format (e.g., "123556.99")
	frac := ""
	if dotIdx := strings.Index(s, "."); dotIdx >= 0 {
		frac = s[dotIdx+1:]
		s = s[:dotIdx]
	}

	// Pad to at least 2 digits
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		// Not a valid number, return as-is
		return s
	}

	var h, m, sec int
	if n < 100 {
		// SS format
		sec = int(n)
	} else if n < 10000 {
		// MMSS format
		sec = int(n % 100)
		m = int(n / 100)
	} else {
		// HHMMSS format
		sec = int(n % 100)
		m = int((n / 100) % 100)
		h = int(n / 10000)
		// If hours exceed TIME max, clamp to 838:59:59 regardless of m/sec validity.
		if h > 838 {
			sign := ""
			if negative {
				sign = "-"
			}
			return sign + "838:59:59"
		}
	}

	return formatTimeValue(negative, h, m, sec, frac)
}

// formatTimeValue formats time components into MySQL TIME string, clipping to valid range.
func formatTimeValue(negative bool, h, m, sec int, frac string) string {
	// Invalid minutes/seconds produce zero time in MySQL numeric TIME context.
	if m > 59 || sec > 59 || m < 0 || sec < 0 {
		return "00:00:00"
	}

	// Clip to MySQL TIME range: -838:59:59 to 838:59:59
	totalSecs := h*3600 + m*60 + sec
	maxSecs := 838*3600 + 59*60 + 59
	if totalSecs > maxSecs {
		// If hours > 838, clip to max
		h, m, sec = 838, 59, 59
		frac = ""
	} else if totalSecs == maxSecs && frac != "" && strings.TrimRight(frac, "0") != "" {
		// 838:59:59.xxx where xxx > 0 exceeds the max TIME value (838:59:59); clamp by dropping fractional part.
		// 838:59:59.000000 is exactly at max and is valid (no clamping).
		frac = ""
	}

	sign := ""
	if negative {
		sign = "-"
	}

	result := fmt.Sprintf("%s%02d:%02d:%02d", sign, h, m, sec)
	if frac != "" {
		// Truncate fractional seconds to microseconds (6 digits)
		if len(frac) > 6 {
			frac = frac[:6]
		}
		// Remove trailing zeros
		frac = strings.TrimRight(frac, "0")
		if frac != "" {
			result += "." + frac
		}
	}
	return result
}

// applyTimePrecision rounds or truncates a TIME string's fractional seconds
// to the given fsp (fractional seconds precision, 0-6).
// For fsp=0 (default TIME), fractional seconds >= 0.5 round up the seconds.
func applyTimePrecision(timeStr string, fsp int) string {
	// Find the fractional part
	dotIdx := strings.Index(timeStr, ".")
	if dotIdx < 0 {
		if fsp == 0 {
			// No fractional part for non-fractional TIME, nothing to do
			return timeStr
		}
		// TIME(N) with fsp>0 but no fractional part: pad with zeros (e.g. "838:59:59" -> "838:59:59.000000")
		return timeStr + "." + strings.Repeat("0", fsp)
	}
	basePart := timeStr[:dotIdx]
	fracPart := timeStr[dotIdx+1:]

	if fsp == 0 {
		// Round: if first frac digit >= 5, increment seconds
		if len(fracPart) > 0 && fracPart[0] >= '5' {
			return incrementTimeSecond(basePart)
		}
		return basePart
	}

	// Pad or truncate fractional part to fsp digits
	for len(fracPart) < fsp {
		fracPart += "0"
	}
	if len(fracPart) > fsp {
		// Check if we need to round
		roundUp := fracPart[fsp] >= '5'
		fracPart = fracPart[:fsp]
		if roundUp {
			// Increment the last fractional digit
			digits := []byte(fracPart)
			carry := true
			for i := len(digits) - 1; i >= 0 && carry; i-- {
				digits[i]++
				if digits[i] > '9' {
					digits[i] = '0'
				} else {
					carry = false
				}
			}
			fracPart = string(digits)
			if carry {
				// Fractional part overflowed, increment seconds
				return incrementTimeSecond(basePart) + "." + fracPart
			}
		}
	}
	// Keep exactly fsp digits (do not trim trailing zeros for fixed-precision TIME columns)
	return basePart + "." + fracPart
}

// incrementTimeSecond adds 1 second to a TIME string like "HH:MM:SS" or "-HH:MM:SS".
func incrementTimeSecond(timeStr string) string {
	negative := false
	s := timeStr
	if strings.HasPrefix(s, "-") {
		negative = true
		s = s[1:]
	}
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return timeStr
	}
	h, _ := strconv.Atoi(parts[0])
	m, _ := strconv.Atoi(parts[1])
	sec, _ := strconv.Atoi(parts[2])
	sec++
	if sec >= 60 {
		sec = 0
		m++
	}
	if m >= 60 {
		m = 0
		h++
	}
	// Re-clip to TIME range
	sign := ""
	if negative {
		sign = "-"
	}
	totalSecs := h*3600 + m*60 + sec
	maxSecs := 838*3600 + 59*60 + 59
	if totalSecs > maxSecs {
		h, m, sec = 838, 59, 59
	}
	return fmt.Sprintf("%s%02d:%02d:%02d", sign, h, m, sec)
}

// truncateTimePrecision truncates (without rounding) a TIME or DATETIME string's
// fractional seconds to the given fsp (0-6). Used with TIME_TRUNCATE_FRACTIONAL mode.
// For fsp=0 the fractional part is dropped entirely.
func truncateTimePrecision(timeStr string, fsp int) string {
	dotIdx := strings.Index(timeStr, ".")
	if dotIdx < 0 {
		return timeStr
	}
	basePart := timeStr[:dotIdx]
	fracPart := timeStr[dotIdx+1:]

	if fsp == 0 {
		return basePart
	}

	for len(fracPart) < fsp {
		fracPart += "0"
	}
	if len(fracPart) > fsp {
		fracPart = fracPart[:fsp]
	}
	return basePart + "." + fracPart
}

// truncateFracPartOfValue applies fractional-second truncation to the string
// representation of a TIME, DATETIME, or TIMESTAMP value.
// Handles "HH:MM:SS.fff" and "YYYY-MM-DD HH:MM:SS.fff" formats.
func truncateFracPartOfValue(s string, fsp int) string {
	if !strings.Contains(s, ".") {
		return s
	}
	// DATETIME/TIMESTAMP: "YYYY-MM-DD HH:MM:SS.ffffff"
	if len(s) > 19 && s[4] == '-' && s[7] == '-' && s[10] == ' ' {
		timePart := s[11:]
		return s[:11] + truncateTimePrecision(timePart, fsp)
	}
	// TIME: "HH:MM:SS.ffffff" or "-HH:MM:SS.ffffff"
	return truncateTimePrecision(s, fsp)
}

// extractTimeFspFromType returns the fractional seconds precision for TIME/DATETIME/TIMESTAMP
// column types. Returns (fsp, true) for temporal types, (0, false) otherwise.
func extractTimeFspFromType(colType string) (int, bool) {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	if upper == "TIME" {
		return 0, true
	}
	if strings.HasPrefix(upper, "TIME(") {
		fsp := 0
		fmt.Sscanf(upper, "TIME(%d)", &fsp)
		return fsp, true
	}
	if upper == "DATETIME" {
		return 0, true
	}
	if strings.HasPrefix(upper, "DATETIME(") {
		fsp := 0
		fmt.Sscanf(upper, "DATETIME(%d)", &fsp)
		return fsp, true
	}
	if upper == "TIMESTAMP" {
		return 0, true
	}
	if strings.HasPrefix(upper, "TIMESTAMP(") {
		fsp := 0
		fmt.Sscanf(upper, "TIMESTAMP(%d)", &fsp)
		return fsp, true
	}
	return 0, false
}

// preTruncateTimeValue converts val to a string and applies fractional-second truncation
// for TIME/DATETIME/TIMESTAMP columns when TIME_TRUNCATE_FRACTIONAL is active.
// For float64 values it uses the exact decimal string representation to avoid rounding
// artifacts from float arithmetic (e.g. 101010.9999999 → "10:10:10.9999999" string path).
// Returns the possibly-modified value (string) and whether a conversion was made.
func preTruncateTimeValue(val interface{}, fsp int) interface{} {
	switch v := val.(type) {
	case string:
		return truncateFracPartOfValue(v, fsp)
	case float64:
		// Convert float to exact decimal string so that the downstream string parser
		// sees all fractional digits without float-rounding artifacts.
		s := strconv.FormatFloat(v, 'f', -1, 64)
		// For TIME-like floats (HHMMSS.frac), truncate at the decimal point.
		// The string will be parsed by parseMySQLTimeValue later.
		return truncateFracPartOfValue(s, fsp)
	}
	return val
}

// coerceColumnValueForWrite applies the standard coercion chain for a column during
// DML write operations (INSERT/UPDATE). When TIME_TRUNCATE_FRACTIONAL is active in
// sqlMode, fractional seconds are truncated (not rounded) before the value is stored.
func (e *Executor) coerceColumnValueForWrite(colType string, val interface{}) interface{} {
	if val != nil && strings.Contains(e.sqlMode, "TIME_TRUNCATE_FRACTIONAL") {
		if fsp, ok := extractTimeFspFromType(colType); ok {
			val = preTruncateTimeValue(val, fsp)
		}
	}
	return e.coerceColumnValueWithSession(colType, val)
}

// coerceColumnValueWithSession is like coerceColumnValue but passes the session time
// for TIME -> TIMESTAMP conversion.
func (e *Executor) coerceColumnValueWithSession(colType string, val interface{}) interface{} {
	if padLen := binaryPadLength(colType); padLen > 0 && val != nil {
		val = padBinaryValue(val, padLen)
	} else if isVarbinaryType(colType) && val != nil {
		val = hexIntToBytes(val)
	}
	if val != nil {
		val = formatDecimalValue(colType, val)
		val = validateEnumSetValue(colType, val)
		st := e.nowTime()
		ttf := strings.Contains(e.sqlMode, "TIME_TRUNCATE_FRACTIONAL")
		val = coerceDateTimeValueWithSessionEx(colType, val, &st, ttf)
		val = coerceIntegerValue(colType, val)
		val = coerceBitValue(colType, val)
	}
	return val
}

// coerceValueForColumnTypeForWrite is like coerceValueForColumnType but applies
// fractional-second truncation when TIME_TRUNCATE_FRACTIONAL is active.
func (e *Executor) coerceValueForColumnTypeForWrite(col catalog.ColumnDef, val interface{}) interface{} {
	if val != nil && strings.Contains(e.sqlMode, "TIME_TRUNCATE_FRACTIONAL") {
		if fsp, ok := extractTimeFspFromType(col.Type); ok {
			val = preTruncateTimeValue(val, fsp)
		}
	}
	return e.coerceValueForColumnTypeWithSession(col, val)
}

// coerceValueForColumnTypeWithSession is like coerceValueForColumnType but passes
// the session time for TIME -> TIMESTAMP conversion.
func (e *Executor) coerceValueForColumnTypeWithSession(col catalog.ColumnDef, val interface{}) interface{} {
	if val == nil {
		return nil
	}
	if padLen := binaryPadLength(col.Type); padLen > 0 {
		val = padBinaryValue(val, padLen)
	} else if isVarbinaryType(col.Type) {
		val = hexIntToBytes(val)
	}
	val = formatDecimalValue(col.Type, val)
	val = validateEnumSetValue(col.Type, val)
	st := e.nowTime()
	ttf := strings.Contains(e.sqlMode, "TIME_TRUNCATE_FRACTIONAL")
	val = coerceDateTimeValueWithSessionEx(col.Type, val, &st, ttf)
	val = coerceIntegerValue(col.Type, val)
	val = coerceBitValue(col.Type, val)
	// Truncate BLOB/TEXT values when column type changes (e.g., LONGBLOB→BLOB)
	colUp := strings.ToUpper(col.Type)
	isBlobTy := colUp == "BLOB" || colUp == "TINYBLOB" || colUp == "MEDIUMBLOB"
	isTextTy := colUp == "TEXT" || colUp == "TINYTEXT" || colUp == "MEDIUMTEXT"
	if isBlobTy || isTextTy {
		if sv, ok := val.(string); ok {
			maxLen := extractCharLength(col.Type)
			if maxLen > 0 {
				if isBlobTy {
					if len(sv) > maxLen {
						val = "" // MySQL sets to empty string when BLOB data exceeds new column max
					}
				} else {
					if len([]rune(sv)) > maxLen {
						val = "" // MySQL sets to empty string when TEXT data exceeds new column max
					}
				}
			}
		}
	}
	return val
}

// implicitZeroValue returns the implicit zero/default value for a MySQL type
// when a NOT NULL column has no explicit default.
func implicitZeroValue(colType string) interface{} {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	if strings.HasPrefix(upper, "YEAR") {
		return "0000"
	}
	if strings.HasPrefix(upper, "DATE") && !strings.HasPrefix(upper, "DATETIME") {
		return "0000-00-00"
	}
	if strings.HasPrefix(upper, "DATETIME") || strings.HasPrefix(upper, "TIMESTAMP") {
		return "0000-00-00 00:00:00"
	}
	if strings.HasPrefix(upper, "TIME") {
		return "00:00:00"
	}
	if strings.Contains(upper, "INT") || strings.Contains(upper, "DECIMAL") ||
		strings.Contains(upper, "FLOAT") || strings.Contains(upper, "DOUBLE") {
		return int64(0)
	}
	// BINARY(N): zero value is N null bytes
	if padLen := binaryPadLength(colType); padLen > 0 {
		return strings.Repeat("\x00", padLen)
	}
	// ENUM: implicit default is the first enum value
	lower := strings.ToLower(strings.TrimSpace(colType))
	if strings.HasPrefix(lower, "enum(") {
		inner := colType[5 : len(colType)-1]
		vals := splitEnumValues(inner)
		if len(vals) > 0 {
			return EnumValue(strings.Trim(vals[0], "'"))
		}
		return EnumValue("")
	}
	// Default for string types (including SET which defaults to empty)
	return ""
}

// coerceYearValue converts a value to MySQL YEAR type.
// Returns a string like "0000", "2005", etc.
func coerceYearValue(v interface{}) interface{} {
	s := fmt.Sprintf("%v", v)

	// Check if original value was a numeric type
	isNumericType := false
	var numVal int
	switch n := v.(type) {
	case int64:
		isNumericType = true
		numVal = int(n)
	case float64:
		isNumericType = true
		numVal = int(math.Round(n))
	case uint64:
		isNumericType = true
		numVal = int(n)
	}

	// Empty string -> 0000
	if s == "" {
		return "0000"
	}

	// Extract year from date/datetime (e.g., "2009-01-29 11:11:27")
	if len(s) >= 5 && s[4] == '-' {
		yearPart := s[:4]
		if y, err := strconv.Atoi(yearPart); err == nil {
			if y >= 1901 && y <= 2155 {
				return fmt.Sprintf("%d", y)
			}
			return "0000"
		}
	}

	if isNumericType {
		if numVal == 0 {
			return "0000"
		}
		if numVal >= 1 && numVal <= 69 {
			return fmt.Sprintf("%d", 2000+numVal)
		}
		if numVal >= 70 && numVal <= 99 {
			return fmt.Sprintf("%d", 1900+numVal)
		}
		if numVal >= 1901 && numVal <= 2155 {
			return fmt.Sprintf("%d", numVal)
		}
		return "0000"
	}

	// String value — MySQL extracts leading numeric portion (e.g., "2012qwer" → 2012)
	n, err := strconv.Atoi(s)
	if err != nil {
		f, err2 := strconv.ParseFloat(s, 64)
		if err2 != nil {
			// Try extracting leading digits (e.g., "2012qwer" -> 2012)
			leadingDigits := ""
			for _, ch := range s {
				if ch >= '0' && ch <= '9' {
					leadingDigits += string(ch)
				} else {
					break
				}
			}
			if leadingDigits == "" {
				return "0000"
			}
			n, err = strconv.Atoi(leadingDigits)
			if err != nil {
				return "0000"
			}
		} else {
			// MySQL rounds float YEAR values (e.g., 2000.5 → 2001)
			n = int(math.Round(f))
		}
	}

	// String '0' or '00' or '000' -> 2000 (but '0000' -> 0000)
	if n == 0 {
		trimmed := strings.TrimLeft(s, "0")
		if trimmed == "" && len(s) >= 4 {
			return "0000"
		}
		return "2000"
	}

	if n >= 1 && n <= 69 {
		return fmt.Sprintf("%d", 2000+n)
	}
	if n >= 70 && n <= 99 {
		return fmt.Sprintf("%d", 1900+n)
	}
	if n >= 1901 && n <= 2155 {
		return fmt.Sprintf("%d", n)
	}
	return "0000"
}

// parseMySQLDateValue parses various MySQL date input formats and returns YYYY-MM-DD or "".
var flexDateRe = regexp.MustCompile(`^(\d{4})-(\d{1,2})-(\d{1,2})`)

// parseMySQLDateValueAllowInvalid parses a date string like parseMySQLDateValue but
// allows invalid day values (e.g. 2004-02-30) as required by ALLOW_INVALID_DATES mode.
// Returns the formatted date string, or "" if the value can't be parsed at all.
func parseMySQLDateValueAllowInvalid(s string) string {
	// Try YYYY-M-D format (may have a time component after a space)
	if m := flexDateRe.FindStringSubmatch(s); m != nil {
		y, _ := strconv.Atoi(m[1])
		mo, _ := strconv.Atoi(m[2])
		d, _ := strconv.Atoi(m[3])
		// Allow any day 1-31 as long as month is 1-12 (or 0 for partial zero)
		if mo >= 0 && mo <= 12 && d >= 0 && d <= 31 {
			datePart := fmt.Sprintf("%04d-%02d-%02d", y, mo, d)
			// Preserve time component if present (for DATETIME values)
			if idx := strings.IndexAny(s, " \tT"); idx >= 0 {
				return datePart + " " + strings.TrimSpace(s[idx+1:])
			}
			return datePart
		}
	}
	// Try standard YYYY-MM-DD format
	if len(s) >= 10 && s[4] == '-' && s[7] == '-' {
		y, _ := strconv.Atoi(s[:4])
		mo, _ := strconv.Atoi(s[5:7])
		d, _ := strconv.Atoi(s[8:10])
		if mo >= 0 && mo <= 12 && d >= 0 && d <= 31 {
			datePart := fmt.Sprintf("%04d-%02d-%02d", y, mo, d)
			// Preserve time component if present (for DATETIME values)
			if len(s) > 10 {
				if idx := strings.IndexAny(s[10:], " \tT"); idx >= 0 {
					return datePart + " " + strings.TrimSpace(s[10+idx+1:])
				}
			}
			return datePart
		}
	}
	// Fall through to the normal parser for numeric formats etc.
	return parseMySQLDateValue(s)
}

func parseMySQLDateValue(s string) string {
	// Already in standard format
	if len(s) >= 10 && s[4] == '-' && s[7] == '-' {
		dateStr := s[:10]
		// Validate the date
		y, _ := strconv.Atoi(dateStr[:4])
		m, _ := strconv.Atoi(dateStr[5:7])
		d, _ := strconv.Atoi(dateStr[8:10])
		if !isValidDate(y, m, d) {
			return "" // Invalid date like 2008-04-31
		}
		return dateStr
	}

	// Try flexible YYYY-M-D format (non-zero-padded)
	if m := flexDateRe.FindStringSubmatch(s); m != nil {
		y, _ := strconv.Atoi(m[1])
		mo, _ := strconv.Atoi(m[2])
		d, _ := strconv.Atoi(m[3])
		if isValidDate(y, mo, d) {
			return fmt.Sprintf("%04d-%02d-%02d", y, mo, d)
		}
	}

	// Strip time part for datetime strings
	datePart := s
	if idx := strings.Index(s, " "); idx >= 0 {
		datePart = s[:idx]
	}

	// isValidCompactDate rejects partial zeros (m=0 or d=0 with non-all-zero date)
	// for compact numeric formats (YYMMDD, YYYYMMDD, etc.).
	// In delimited formats, partial zeros like "2003-01-00" are allowed.
	isValidCompactDate := func(y, m, d int) bool {
		if y == 0 && m == 0 && d == 0 {
			return true // Zero date is always valid
		}
		if m == 0 || d == 0 {
			return false // Partial zeros invalid in compact form
		}
		return isValidDate(y, m, d)
	}

	// Try YYYYMMDD or YYYYMMDDHHMMSS format (no delimiters)
	isAllDigits := true
	for _, c := range datePart {
		if c < '0' || c > '9' {
			isAllDigits = false
			break
		}
	}
	if isAllDigits {
		switch len(datePart) {
		case 8: // YYYYMMDD
			y, _ := strconv.Atoi(datePart[:4])
			m, _ := strconv.Atoi(datePart[4:6])
			d, _ := strconv.Atoi(datePart[6:8])
			if isValidCompactDate(y, m, d) {
				return fmt.Sprintf("%04d-%02d-%02d", y, m, d)
			}
		case 14: // YYYYMMDDHHMMSS - extract date part
			y, _ := strconv.Atoi(datePart[:4])
			m, _ := strconv.Atoi(datePart[4:6])
			d, _ := strconv.Atoi(datePart[6:8])
			if isValidCompactDate(y, m, d) {
				return fmt.Sprintf("%04d-%02d-%02d", y, m, d)
			}
		case 6: // YYMMDD
			yy, _ := strconv.Atoi(datePart[:2])
			m, _ := strconv.Atoi(datePart[2:4])
			d, _ := strconv.Atoi(datePart[4:6])
			y := convert2DigitYear(yy)
			if isValidCompactDate(y, m, d) {
				return fmt.Sprintf("%04d-%02d-%02d", y, m, d)
			}
		case 12: // YYMMDDHHMMSS - extract date part
			yy, _ := strconv.Atoi(datePart[:2])
			m, _ := strconv.Atoi(datePart[2:4])
			d, _ := strconv.Atoi(datePart[4:6])
			y := convert2DigitYear(yy)
			if isValidCompactDate(y, m, d) {
				return fmt.Sprintf("%04d-%02d-%02d", y, m, d)
			}
		default:
			// 1-5 digits: left-pad to 6 digits with zeros and parse as YYMMDD.
			// Integer 0 is special: it means the zero date 0000-00-00 (not 2000-00-00).
			if len(datePart) >= 1 && len(datePart) <= 5 {
				n, err := strconv.Atoi(datePart)
				if err == nil {
					if n == 0 {
						return "0000-00-00"
					}
					padded := fmt.Sprintf("%06d", n)
					yy, _ := strconv.Atoi(padded[:2])
					m, _ := strconv.Atoi(padded[2:4])
					d, _ := strconv.Atoi(padded[4:6])
					y := convert2DigitYear(yy)
					if isValidDate(y, m, d) {
						return fmt.Sprintf("%04d-%02d-%02d", y, m, d)
					}
				}
			}
		}
		return ""
	}

	// Handle 2-digit year with delimiters: YY-MM-DD or YY/MM/DD etc.
	// Replace various delimiters with -
	normalized := normalizeDateDelimiters(datePart)
	parts := strings.Split(normalized, "-")
	if len(parts) == 3 {
		yStr, mStr, dStr := parts[0], parts[1], parts[2]
		y, errY := strconv.Atoi(yStr)
		m, errM := strconv.Atoi(mStr)
		d, errD := strconv.Atoi(dStr)
		if errY == nil && errM == nil && errD == nil {
			// Special case: all parts zero (e.g., 00-00-00) -> 0000-00-00 (zero date)
			if y == 0 && m == 0 && d == 0 {
				return "0000-00-00"
			}
			if len(yStr) <= 2 {
				y = convert2DigitYear(y)
			}
			if isValidDate(y, m, d) {
				return fmt.Sprintf("%04d-%02d-%02d", y, m, d)
			}
		}
	}

	return ""
}

// isValidDate checks if a date is valid (or is the zero date 0000-00-00).
func isValidDate(y, m, d int) bool {
	// Zero date is always valid
	if y == 0 && m == 0 && d == 0 {
		return true
	}
	// Allow zero month/day for MySQL partial zero dates
	if m == 0 || d == 0 {
		return m >= 0 && m <= 12 && d >= 0 && d <= 31
	}
	if m < 1 || m > 12 || d < 1 {
		return false
	}
	// Days in month
	daysInMonth := [13]int{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	maxDay := daysInMonth[m]
	if m == 2 && isLeapYear(y) {
		maxDay = 29
	}
	return d <= maxDay
}

// isLeapYear checks if a year is a leap year.
func isLeapYear(y int) bool {
	return (y%4 == 0 && y%100 != 0) || y%400 == 0
}

// checkTimeStrict validates a TIME string value in strict SQL mode.
// Returns error 1292 if the time value exceeds the MySQL TIME range [-838:59:59.999999, 838:59:59.999999]
// after rounding to the column's fractional-seconds precision.
func checkTimeStrict(colType, colName, originalValue string, rowNum int) error {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	isTimeType := upper == "TIME" || strings.HasPrefix(upper, "TIME(")
	if !isTimeType {
		return nil
	}

	s := strings.TrimSpace(originalValue)
	if s == "" {
		return nil
	}

	// DATETIME-formatted values (e.g. "2007-02-13 09:09:33") are valid for TIME columns;
	// MySQL extracts the time portion. Skip strict range check for these.
	if len(s) >= 10 && s[4] == '-' && s[7] == '-' {
		return nil
	}

	// Extract fractional-seconds precision from column type, e.g. TIME(6) -> 6
	fsp := 0
	if strings.HasPrefix(upper, "TIME(") {
		fmt.Sscanf(upper, "TIME(%d)", &fsp)
	}

	// Parse the time value. Strip leading '-' for sign.
	sign := 1
	rest := s
	if strings.HasPrefix(rest, "-") {
		sign = -1
		rest = rest[1:]
	}

	// Parse HH:MM:SS[.fraction]
	var hours, mins, secs int
	var fracStr string
	colonCount := strings.Count(rest, ":")
	if colonCount == 2 {
		if dotIdx := strings.Index(rest, "."); dotIdx >= 0 {
			fracStr = rest[dotIdx+1:]
			rest = rest[:dotIdx]
		}
		parts := strings.Split(rest, ":")
		if len(parts) == 3 {
			fmt.Sscanf(parts[0], "%d", &hours)
			fmt.Sscanf(parts[1], "%d", &mins)
			fmt.Sscanf(parts[2], "%d", &secs)
		}
	} else if colonCount == 1 {
		// MM:SS format
		if dotIdx := strings.Index(rest, "."); dotIdx >= 0 {
			fracStr = rest[dotIdx+1:]
			rest = rest[:dotIdx]
		}
		parts := strings.Split(rest, ":")
		if len(parts) == 2 {
			fmt.Sscanf(parts[0], "%d", &mins)
			fmt.Sscanf(parts[1], "%d", &secs)
		}
	} else {
		// Just digits - not a typical time format we need to check
		return nil
	}

	// Check if fractional part rounds up when truncated to fsp digits
	// If fracStr has more digits than fsp, check if it rounds up
	if len(fracStr) > fsp {
		// Check if rounding causes carry: digit at position fsp >= 5
		roundDigit := 0
		if fsp < len(fracStr) {
			d := fracStr[fsp] - '0'
			roundDigit = int(d)
		}
		if roundDigit >= 5 {
			// Rounding up: carry into seconds
			secs++
			if secs >= 60 {
				secs = 0
				mins++
				if mins >= 60 {
					mins = 0
					hours++
				}
			}
		}
	}

	// Check range: max is 838:59:59
	maxHours := 838
	_ = sign
	if hours > maxHours || (hours == maxHours && (mins > 59 || secs > 59)) {
		return mysqlError(1292, "22007", fmt.Sprintf("Incorrect time value: '%s' for column '%s' at row %d", originalValue, colName, rowNum))
	}

	return nil
}

// checkDateStrict validates a date string value for a DATE/DATETIME/TIMESTAMP column
// in strict SQL mode. Returns an error if the date is invalid.
// The sqlMode string is used to determine which checks to apply.
func checkDateStrict(colType, colName, originalValue, sqlMode string, rowNum int) error {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	// Only check date-like types
	isDateType := upper == "DATE" || strings.HasPrefix(upper, "DATETIME") || strings.HasPrefix(upper, "TIMESTAMP")
	if !isDateType {
		return nil
	}

	// Determine the type label for error messages
	typeLabel := "date"
	if strings.HasPrefix(upper, "DATETIME") {
		typeLabel = "datetime"
	} else if strings.HasPrefix(upper, "TIMESTAMP") {
		typeLabel = "datetime"
	}
	_ = typeLabel // used below

	s := strings.TrimSpace(originalValue)
	if s == "" {
		return nil
	}

	// Determine SQL mode flags
	isNoZeroInDate := strings.Contains(sqlMode, "NO_ZERO_IN_DATE") || strings.Contains(sqlMode, "TRADITIONAL")
	isNoZeroDate := strings.Contains(sqlMode, "NO_ZERO_DATE") || strings.Contains(sqlMode, "TRADITIONAL")
	// ALLOW_INVALID_DATES skips range checks for day (but still requires valid month range 1-12)
	allowInvalidDates := strings.Contains(sqlMode, "ALLOW_INVALID_DATES")

	// Parse the date part from the value
	// Supported formats: YYYY-MM-DD, YYYYMMDD, YY-MM-DD, and flexible variants
	datePart := s
	// Split at space, newline, or 'T' to get the date portion
	if idx := strings.IndexAny(s, " \tT\n\r"); idx >= 0 {
		datePart = s[:idx]
	}

	// If the value looks like a TIME-only value (e.g., "97:02:03" or "23:59:59"),
	// skip strict date validation — MySQL coerces such values into dates.
	// A TIME-only format has colons but doesn't have date separators (-/.) in the
	// first few characters. Detect by: contains ':' but not in a DATETIME context.
	if strings.Contains(datePart, ":") {
		// Check if it's a time-only format: digits, colons, possibly a decimal
		looksLikeTime := true
		for _, c := range datePart {
			if !((c >= '0' && c <= '9') || c == ':' || c == '.') {
				looksLikeTime = false
				break
			}
		}
		if looksLikeTime {
			return nil // MySQL coerces TIME values into date columns, don't reject here
		}
	}
	// Handle decimal/fractional numeric date-time values like '20010101235959.4'
	// or '20010101100000.1234567'. These are compact numeric DATETIME formats with
	// fractional seconds. Strip the fractional part only if the integer part looks
	// like a compact YYYYMMDDHHMMSS or YYYYMMDD format (8, 12, or 14 digits).
	if idx := strings.IndexByte(datePart, '.'); idx >= 0 {
		// Check if the part before the dot is all digits
		allDigitsBefore := true
		for _, c := range datePart[:idx] {
			if c < '0' || c > '9' {
				allDigitsBefore = false
				break
			}
		}
		if allDigitsBefore && (idx == 8 || idx == 12 || idx == 14) {
			datePart = datePart[:idx]
		}
	}

	// Try to parse as YYYY-M-D or YYYYMMDD
	var y, m, d int
	parsed := false

	// Check if it's a numeric value (no delimiters)
	isAllDigits := true
	for _, c := range datePart {
		if c < '0' || c > '9' {
			isAllDigits = false
			break
		}
	}
	if isAllDigits {
		// Pad 7-11 digit strings to 12, 13-digit to 14 (right-pad, string coercion)
		switch len(datePart) {
		case 7, 9, 10, 11:
			datePart = datePart + strings.Repeat("0", 12-len(datePart))
		case 13:
			datePart = datePart + "0"
		}
		switch len(datePart) {
		case 14: // YYYYMMDDHHMMSS - DATETIME format, extract date part
			yy, _ := strconv.Atoi(datePart[:4])
			mm, _ := strconv.Atoi(datePart[4:6])
			dd, _ := strconv.Atoi(datePart[6:8])
			y, m, d = yy, mm, dd
			parsed = true
		case 12: // YYMMDDHHMMSS - DATETIME format, extract date part
			yy, _ := strconv.Atoi(datePart[:2])
			mm, _ := strconv.Atoi(datePart[2:4])
			dd, _ := strconv.Atoi(datePart[4:6])
			y, m, d = convert2DigitYear(yy), mm, dd
			parsed = true
		case 8: // YYYYMMDD
			yy, _ := strconv.Atoi(datePart[:4])
			mm, _ := strconv.Atoi(datePart[4:6])
			dd, _ := strconv.Atoi(datePart[6:8])
			y, m, d = yy, mm, dd
			parsed = true
		case 6: // YYMMDD
			yy, _ := strconv.Atoi(datePart[:2])
			mm, _ := strconv.Atoi(datePart[2:4])
			dd, _ := strconv.Atoi(datePart[4:6])
			y, m, d = convert2DigitYear(yy), mm, dd
			parsed = true
		default:
			// Short numeric values like '59' - definitely invalid
			return mysqlError(1292, "22007", fmt.Sprintf("Incorrect %s value: '%s' for column '%s' at row %d", typeLabel, originalValue, colName, rowNum))
		}
	}

	if !parsed {
		// Try delimited formats: YYYY-M-D or similar
		norm := normalizeDateDelimiters(datePart)
		parts := strings.Split(norm, "-")
		if len(parts) == 3 {
			yStr, mStr, dStr := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), strings.TrimSpace(parts[2])
			// Remove time components from dStr
			if idx := strings.IndexAny(dStr, " T:"); idx >= 0 {
				dStr = dStr[:idx]
			}
			var parseErr error
			if len(yStr) <= 2 {
				var yy int
				yy, parseErr = strconv.Atoi(yStr)
				if parseErr == nil {
					y = convert2DigitYear(yy)
				}
			} else {
				y, parseErr = strconv.Atoi(yStr)
			}
			if parseErr != nil {
				return mysqlError(1292, "22007", fmt.Sprintf("Incorrect %s value: '%s' for column '%s' at row %d", typeLabel, originalValue, colName, rowNum))
			}
			m, _ = strconv.Atoi(mStr)
			d, _ = strconv.Atoi(dStr)
			parsed = true
		} else {
			// Not parseable as a date
			return mysqlError(1292, "22007", fmt.Sprintf("Incorrect %s value: '%s' for column '%s' at row %d", typeLabel, originalValue, colName, rowNum))
		}
	}

	if !parsed {
		return mysqlError(1292, "22007", fmt.Sprintf("Incorrect %s value: '%s' for column '%s' at row %d", typeLabel, originalValue, colName, rowNum))
	}

	// Check for zero date (0000-00-00)
	if y == 0 && m == 0 && d == 0 {
		if isNoZeroDate {
			return mysqlError(1292, "22007", fmt.Sprintf("Incorrect %s value: '%s' for column '%s' at row %d", typeLabel, originalValue, colName, rowNum))
		}
		return nil
	}

	// Check for zero month or zero day (partial zero dates)
	if m == 0 || d == 0 {
		// TIMESTAMP can never have month=0 or day=0 (out of valid range 1970-2038)
		isTimestamp := strings.HasPrefix(upper, "TIMESTAMP")
		if isNoZeroInDate || isTimestamp {
			return mysqlError(1292, "22007", fmt.Sprintf("Incorrect %s value: '%s' for column '%s' at row %d", typeLabel, originalValue, colName, rowNum))
		}
		// Partial zero dates are allowed in non-NO_ZERO_IN_DATE mode for DATE/DATETIME
		return nil
	}

	// Check month range
	if m < 1 || m > 12 {
		return mysqlError(1292, "22007", fmt.Sprintf("Incorrect %s value: '%s' for column '%s' at row %d", typeLabel, originalValue, colName, rowNum))
	}

	// ALLOW_INVALID_DATES: skip day range check for DATE/DATETIME only.
	// TIMESTAMP still requires a valid calendar date (because it must also be in 1970-2038 range).
	isTimestampForAllow := strings.HasPrefix(upper, "TIMESTAMP")
	if allowInvalidDates && !isTimestampForAllow {
		if d < 1 || d > 31 {
			return mysqlError(1292, "22007", fmt.Sprintf("Incorrect %s value: '%s' for column '%s' at row %d", typeLabel, originalValue, colName, rowNum))
		}
		return nil
	}

	// Check day range
	daysInMonth := [13]int{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	maxDay := daysInMonth[m]
	if m == 2 && isLeapYear(y) {
		maxDay = 29
	}
	if d < 1 || d > maxDay {
		return mysqlError(1292, "22007", fmt.Sprintf("Incorrect %s value: '%s' for column '%s' at row %d", typeLabel, originalValue, colName, rowNum))
	}

	// For DATETIME/TIMESTAMP columns: validate the time component (HH:MM:SS) if present.
	// Hours must be 0-23, minutes 0-59, seconds 0-59.
	if strings.HasPrefix(upper, "DATETIME") || strings.HasPrefix(upper, "TIMESTAMP") {
		// Extract time portion after the space/T separator
		timePart := ""
		if idx := strings.IndexAny(s, " \tT"); idx >= 0 {
			timePart = strings.TrimSpace(s[idx+1:])
		}
		if timePart != "" {
			timeParts := strings.Split(timePart, ":")
			// Accept HH:MM or HH:MM:SS[.fraction] — 2 or 3 parts
			if len(timeParts) == 2 || len(timeParts) == 3 {
				hh, errH := strconv.Atoi(timeParts[0])
				mm2, errM := strconv.Atoi(timeParts[1])
				ss := 0
				var errS error
				if len(timeParts) == 3 {
					secStr := timeParts[2]
					if dotIdx := strings.IndexByte(secStr, '.'); dotIdx >= 0 {
						secStr = secStr[:dotIdx]
					}
					ss, errS = strconv.Atoi(secStr)
				}
				if errH != nil || errM != nil || errS != nil {
					return mysqlError(1292, "22007", fmt.Sprintf("Incorrect %s value: '%s' for column '%s' at row %d", typeLabel, originalValue, colName, rowNum))
				}
				if hh < 0 || hh > 23 || mm2 < 0 || mm2 > 59 || ss < 0 || ss > 59 {
					return mysqlError(1292, "22007", fmt.Sprintf("Incorrect %s value: '%s' for column '%s' at row %d", typeLabel, originalValue, colName, rowNum))
				}
			}
			// len==1 means no colon (e.g. "001500" compact form) — skip validation here
		}
	}

	// For TIMESTAMP columns: check that the value is within the valid range
	// (1970-01-01 00:00:01 to 2038-01-19 03:14:07 UTC). Values outside this range
	// are invalid even if the date itself is valid.
	if strings.HasPrefix(upper, "TIMESTAMP") {
		// TIMESTAMP range: any date before 1970 or after 2038 is out of range.
		// Also 0000-xx-xx is invalid.
		isOutOfRange := false
		if y < 1970 {
			isOutOfRange = true
		} else if y > 2038 {
			isOutOfRange = true
		} else if y == 2038 && (m > 1 || (m == 1 && d > 19)) {
			isOutOfRange = true
		}
		if isOutOfRange {
			return mysqlError(1292, "22007", fmt.Sprintf("Incorrect %s value: '%s' for column '%s' at row %d", typeLabel, originalValue, colName, rowNum))
		}
	}

	return nil
}

// zeroDateTimeValue returns the zero value string for a DATE/DATETIME/TIMESTAMP/TIME column type.
// Used when a date/time value is invalid and IGNORE suppresses the error.
func zeroDateTimeValue(colType string) string {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	if upper == "DATE" {
		return "0000-00-00"
	}
	if strings.HasPrefix(upper, "DATETIME") {
		return "0000-00-00 00:00:00"
	}
	if strings.HasPrefix(upper, "TIMESTAMP") {
		return "0000-00-00 00:00:00"
	}
	if upper == "TIME" || strings.HasPrefix(upper, "TIME(") {
		return "00:00:00"
	}
	return ""
}

// normalizeDateDelimiters replaces various date delimiters with '-'.
func normalizeDateDelimiters(s string) string {
	var result strings.Builder
	for _, c := range s {
		switch c {
		case '/', '.', '@':
			result.WriteByte('-')
		default:
			result.WriteRune(c)
		}
	}
	return result.String()
}

// normalizeDateTimeSeparators replaces various time separator characters with ':'.
func normalizeDateTimeSeparators(s string) string {
	var result strings.Builder
	for _, c := range s {
		switch c {
		case '*', '+', '^':
			result.WriteByte(':')
		default:
			result.WriteRune(c)
		}
	}
	return result.String()
}

// isYearColumnString returns true if the string represents a YEAR column value:
// exactly 4 digits, in the range 0000 or 1901-2155.
func isYearColumnString(s string) bool {
	if len(s) != 4 {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	// "0000" is valid YEAR zero value
	if s == "0000" {
		return true
	}
	y, err := strconv.Atoi(s)
	if err != nil {
		return false
	}
	return y >= 1901 && y <= 2155
}

// looksLikeDate checks if a string looks like a date value (contains date separators).
func looksLikeDate(s string) bool {
	// Contains date-like separator and has digits
	if len(s) < 6 {
		return false
	}
	return strings.ContainsAny(s, "-/.")
}

// looksLikeTime returns true if the string looks like a TIME value (contains ':').
func looksLikeTime(s string) bool {
	return strings.Contains(s, ":")
}

// looksLikeActualTime returns true if the string is a plausible MySQL TIME value.
// A valid TIME looks like: [+-][D ]HH:MM[:SS[.frac]] where all components are numeric.
// This is stricter than looksLikeTime and excludes strings like instrument names
// that happen to contain ':' (e.g. 'wait/synch/mutex/sql/hash_filo::lock').
func looksLikeActualTime(s string) bool {
	if s == "" {
		return false
	}
	// Strip optional sign
	if s[0] == '-' || s[0] == '+' {
		s = s[1:]
	}
	// Strip optional day prefix (e.g. "1 " in "1 12:00:00")
	if idx := strings.Index(s, " "); idx >= 0 {
		dayPart := s[:idx]
		rest := s[idx+1:]
		allDigits := true
		for _, c := range dayPart {
			if c < '0' || c > '9' {
				allDigits = false
				break
			}
		}
		if allDigits && len(dayPart) > 0 {
			s = rest
		}
	}
	// Now s should be HH:MM[:SS[.frac]]
	// The first character must be a digit
	if len(s) == 0 || s[0] < '0' || s[0] > '9' {
		return false
	}
	// Must contain ':' and all colon-separated segments must start with a digit
	colonIdx := strings.Index(s, ":")
	if colonIdx < 0 {
		return false
	}
	// Validate each colon-separated part starts with a digit
	parts := strings.Split(s, ":")
	for _, p := range parts {
		// Each part may have fractional seconds: strip after '.'
		p = strings.SplitN(p, ".", 2)[0]
		if len(p) == 0 {
			continue // empty part (e.g. "12::34") - not valid time but also not an instrument
		}
		for _, c := range p {
			if c < '0' || c > '9' {
				return false
			}
		}
	}
	return true
}

// normalizeDateTimeString normalizes a date or datetime string into canonical form.
// For date-only values: returns "YYYY-MM-DD".
// For datetime values: returns "YYYY-MM-DD HH:MM:SS".
// Returns "" if the string cannot be parsed as a date.
func normalizeDateTimeString(s string) string {
	// Handle compact decimal YYYYMMDDHHMMSS.ffffff format (e.g. "20010101112233.000000").
	// parseMySQLDateValue can't handle the '.' separator, so we convert it manually.
	if dotIdx := strings.IndexByte(s, '.'); dotIdx == 14 {
		intPart := s[:14]
		fracPart := s[15:]
		isAllDigitsFn := func(st string) bool {
			for _, c := range st {
				if c < '0' || c > '9' {
					return false
				}
			}
			return len(st) > 0
		}
		if isAllDigitsFn(intPart) && isAllDigitsFn(fracPart) {
			y, _ := strconv.Atoi(intPart[0:4])
			mo, _ := strconv.Atoi(intPart[4:6])
			d, _ := strconv.Atoi(intPart[6:8])
			h, _ := strconv.Atoi(intPart[8:10])
			mi, _ := strconv.Atoi(intPart[10:12])
			sec, _ := strconv.Atoi(intPart[12:14])
			if mo >= 1 && mo <= 12 && d >= 1 && d <= 31 {
				return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%s", y, mo, d, h, mi, sec, fracPart)
			}
		}
	}
	datePart := parseMySQLDateValue(s)
	if datePart == "" {
		return ""
	}
	// Check if original has a time component
	timePart := extractTimePart(s, datePart)
	if timePart != "" {
		timePart = normalizeDateTimeSeparators(timePart)
		return datePart + " " + timePart
	}
	return datePart
}

// hasInvalidTimeComponent checks if a datetime string has an invalid time component
// (hour > 23, minute > 59, or second > 59). Returns the invalid string if found.
// Only applies to strings that look like full datetime values (date + time parts).
func hasInvalidTimeComponent(s string) (string, bool) {
	// Must have both date separator (-) and time separator (:) and a space
	spaceIdx := strings.Index(s, " ")
	if spaceIdx < 0 {
		return "", false
	}
	datePart := s[:spaceIdx]
	timePart := strings.TrimSpace(s[spaceIdx+1:])
	// Validate date part (must be YYYY-MM-DD format)
	if len(datePart) < 10 || datePart[4] != '-' || datePart[7] != '-' {
		return "", false
	}
	// Validate time part (must be HH:MM:SS format)
	colonIdx := strings.Index(timePart, ":")
	if colonIdx < 0 {
		return "", false
	}
	hourStr := timePart[:colonIdx]
	hour, err := strconv.Atoi(hourStr)
	if err != nil {
		return "", false
	}
	if hour > 23 {
		return s, true
	}
	// Check minutes
	rest := timePart[colonIdx+1:]
	colonIdx2 := strings.Index(rest, ":")
	if colonIdx2 >= 0 {
		minStr := rest[:colonIdx2]
		min, err := strconv.Atoi(minStr)
		if err == nil && min > 59 {
			return s, true
		}
		secStr := rest[colonIdx2+1:]
		// Strip fractional seconds
		if dotIdx := strings.Index(secStr, "."); dotIdx >= 0 {
			secStr = secStr[:dotIdx]
		}
		sec, err := strconv.Atoi(secStr)
		if err == nil && sec > 59 {
			return s, true
		}
	}
	return "", false
}

// convert2DigitYear converts a 2-digit year to 4-digit year.
// 0-69 -> 2000-2069, 70-99 -> 1970-1999
func convert2DigitYear(yy int) int {
	if yy >= 0 && yy <= 69 {
		return 2000 + yy
	}
	if yy >= 70 && yy <= 99 {
		return 1900 + yy
	}
	return yy
}

// coerceIntegerValue converts values for integer columns (TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT).
// In non-strict mode: empty string -> 0, non-numeric string -> 0 (or extract leading number),
// negative -> 0 for UNSIGNED, out-of-range -> clipped.
// mysqlRoundToInt rounds a float64 to int64 using MySQL's rounding rule:
// "round half away from zero" (0.5 -> 1, -0.5 -> -1).
func mysqlRoundToInt(f float64) int64 {
	if f >= 0 {
		return int64(f + 0.5)
	}
	return int64(f - 0.5)
}

// intTypeRange holds the signed min/max and unsigned max for an integer column type.
type intTypeRange struct {
	Min, Max    int64
	MaxUnsigned uint64
}

// intTypeRanges maps base integer type names to their value ranges.
var intTypeRanges = map[string]intTypeRange{
	"TINYINT":   {-128, 127, 255},
	"SMALLINT":  {-32768, 32767, 65535},
	"MEDIUMINT": {-8388608, 8388607, 16777215},
	"INT":       {-2147483648, 2147483647, 4294967295},
	"INTEGER":   {-2147483648, 2147483647, 4294967295},
	"BIGINT":    {-9223372036854775808, 9223372036854775807, 18446744073709551615},
}

func coerceIntegerValue(colType string, v interface{}) interface{} {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	// Remove display width like INT(11)
	baseType := upper
	if idx := strings.Index(baseType, "("); idx >= 0 {
		baseType = baseType[:idx]
	}
	isUnsigned := strings.Contains(upper, "UNSIGNED")
	// Strip UNSIGNED, ZEROFILL etc. for base type check
	baseType = strings.TrimSpace(strings.Replace(strings.Replace(baseType, "UNSIGNED", "", 1), "ZEROFILL", "", 1))

	// Check if this is an integer type
	rng, isIntType := intTypeRanges[baseType]
	if !isIntType {
		return v
	}
	minVal, maxVal, maxUnsigned := rng.Min, rng.Max, rng.MaxUnsigned

	// Convert value to int64
	var intVal int64
	switch val := v.(type) {
	case int64:
		intVal = val
	case HexBytes:
		// x'...' hex literal inserted into integer column: convert big-endian bytes to integer
		decoded, err := hex.DecodeString(string(val))
		if err != nil || len(decoded) == 0 {
			intVal = 0
		} else {
			var uval uint64
			for _, b := range decoded {
				uval = uval<<8 | uint64(b)
			}
			if isUnsigned {
				if uval > maxUnsigned {
					return int64(maxUnsigned)
				}
				return int64(uval)
			}
			intVal = int64(uval)
		}
	case ScaledValue:
		f := val.Value
		if isUnsigned {
			if f < 0 {
				intVal = 0
			} else if f > float64(maxUnsigned) {
				intVal = int64(maxUnsigned)
			} else {
				intVal = mysqlRoundToInt(f)
			}
		} else if f > float64(maxVal) {
			intVal = maxVal
		} else if f < float64(minVal) {
			intVal = minVal
		} else {
			// MySQL rounds half away from zero for decimal-to-int conversion
			intVal = mysqlRoundToInt(f)
		}
	case DivisionResult:
		f := val.Value
		if isUnsigned {
			if f < 0 {
				intVal = 0
			} else if f > float64(maxUnsigned) {
				intVal = int64(maxUnsigned)
			} else {
				intVal = mysqlRoundToInt(f)
			}
		} else if f > float64(maxVal) {
			intVal = maxVal
		} else if f < float64(minVal) {
			intVal = minVal
		} else {
			// MySQL rounds half away from zero for decimal-to-int conversion
			intVal = mysqlRoundToInt(f)
		}
	case float64:
		if isUnsigned {
			if val < 0 {
				intVal = 0
			} else if val > float64(maxUnsigned) {
				intVal = int64(maxUnsigned)
			} else {
				intVal = mysqlRoundToInt(val)
			}
		} else if val > float64(maxVal) {
			intVal = maxVal
		} else if val < float64(minVal) {
			intVal = minVal
		} else {
			// MySQL rounds half away from zero for float-to-int conversion
			intVal = mysqlRoundToInt(val)
		}
	case uint64:
		if isUnsigned {
			if val > maxUnsigned {
				if baseType == "BIGINT" {
					return maxUnsigned
				}
				return int64(maxUnsigned)
			}
			if baseType == "BIGINT" {
				return val
			}
			return int64(val)
		}
		if val > uint64(math.MaxInt64) {
			intVal = maxVal
		} else {
			intVal = int64(val)
		}
	case string:
		if val == "" {
			return int64(0)
		}
		// Try parsing as number - extract leading numeric part
		val = strings.TrimSpace(val)
		// Detect DATETIME format "YYYY-MM-DD HH:MM:SS[.frac]" and convert to YYYYMMDDHHMMSS integer.
		// MySQL converts DATETIME to integer as YYYYMMDDHHMMSS (e.g., "2001-01-01 10:10:59" -> 20010101101059).
		if len(val) >= 19 && val[4] == '-' && val[7] == '-' && val[10] == ' ' && val[13] == ':' && val[16] == ':' {
			y, _ := strconv.ParseInt(val[0:4], 10, 64)
			mo, _ := strconv.ParseInt(val[5:7], 10, 64)
			d, _ := strconv.ParseInt(val[8:10], 10, 64)
			h, _ := strconv.ParseInt(val[11:13], 10, 64)
			mi, _ := strconv.ParseInt(val[14:16], 10, 64)
			sec, _ := strconv.ParseInt(val[17:19], 10, 64)
			datetimeInt := y*10000000000 + mo*100000000 + d*1000000 + h*10000 + mi*100 + sec
			if isUnsigned {
				if datetimeInt < 0 {
					return uint64(0)
				}
				if uint64(datetimeInt) > maxUnsigned {
					return maxUnsigned
				}
				return uint64(datetimeInt)
			}
			if datetimeInt > maxVal {
				return maxVal
			}
			if datetimeInt < minVal {
				return minVal
			}
			return int64(datetimeInt)
		}
		// Detect TIME format "HH:MM:SS[.frac]" or "-HH:MM:SS[.frac]" and convert to HHMMSS integer.
		// MySQL converts TIME to integer as HHMMSS (e.g., "10:10:59" -> 101059).
		{
			negative := false
			rest := val
			if strings.HasPrefix(rest, "-") {
				negative = true
				rest = rest[1:]
			}
			// Match HH:MM:SS pattern (colons at positions 2 and 5, or similar)
			colonCount := strings.Count(rest, ":")
			if colonCount == 2 {
				parts := strings.SplitN(rest, ":", 3)
				// parts[2] may have fractional seconds - strip them (MySQL truncates)
				secPart := parts[2]
				if dotIdx := strings.Index(secPart, "."); dotIdx >= 0 {
					secPart = secPart[:dotIdx]
				}
				// Parse each component as integer
				h, errH := strconv.ParseInt(parts[0], 10, 64)
				m, errM := strconv.ParseInt(parts[1], 10, 64)
				s, errS := strconv.ParseInt(secPart, 10, 64)
				if errH == nil && errM == nil && errS == nil {
					hhmmss := h*10000 + m*100 + s
					// MySQL truncates the fractional part when converting TIME to integer (no rounding)
					// So "10:10:59.5" -> 101059, not 101060
					if negative {
						hhmmss = -hhmmss
					}
					// Clamp to type range
					if isUnsigned {
						if hhmmss < 0 {
							return uint64(0)
						}
						if uint64(hhmmss) > maxUnsigned {
							return maxUnsigned
						}
						return uint64(hhmmss)
					}
					if hhmmss > maxVal {
						return maxVal
					}
					if hhmmss < minVal {
						return minVal
					}
					return hhmmss
				}
			}
		}
		// Parse leading numeric portion
		numStr := ""
		hasDot := false
		hasExp := false
		for i, c := range val {
			if c == '-' && i == 0 {
				numStr += string(c)
			} else if c == '-' && hasExp {
				// Exponent sign (e.g. 1.5e-3)
				numStr += string(c)
			} else if c == '+' && hasExp {
				// Exponent sign (e.g. 1.5e+3)
				// skip +, just continue
			} else if c >= '0' && c <= '9' {
				numStr += string(c)
			} else if c == '.' && !hasDot {
				hasDot = true
				numStr += string(c)
			} else if (c == 'e' || c == 'E') && !hasExp && numStr != "" && numStr != "-" {
				hasExp = true
				hasDot = true // treat scientific notation as having decimal
				numStr += string(c)
			} else {
				break
			}
		}
		if numStr == "" || numStr == "-" {
			return int64(0)
		}
		if hasDot {
			if baseType == "BIGINT" {
				// Special case: float64 can't represent BIGINT boundaries precisely.
				// Use big.Float with high precision to round correctly.
				bf := new(big.Float).SetPrec(128)
				if _, ok := bf.SetString(numStr); !ok {
					return int64(0)
				}
				if isUnsigned {
					// Check sign
					if bf.Sign() < 0 {
						return uint64(0)
					}
					// Convert to big.Int using truncation
					bfTrunc := new(big.Float).SetPrec(128).Copy(bf)
					// Get integer part via Uint64 on truncated value
					// Use big.Int for full precision
					bi := new(big.Int)
					bf.Int(bi) // truncates toward zero
					// Determine if we need to round up (fractional part >= 0.5)
					biF := new(big.Float).SetPrec(128).SetInt(bi)
					frac := new(big.Float).SetPrec(128).Sub(bfTrunc, biF)
					half := new(big.Float).SetPrec(128).SetFloat64(0.5)
					if frac.Cmp(half) >= 0 {
						bi.Add(bi, big.NewInt(1))
					}
					// Clamp to uint64 max
					maxU := new(big.Int).SetUint64(maxUnsigned)
					if bi.Cmp(maxU) > 0 {
						return maxUnsigned
					}
					return bi.Uint64()
				} else {
					// Signed BIGINT
					bfSign := bf.Sign() // sign of original value
					bf2 := new(big.Float).SetPrec(128).Copy(bf)
					bi := new(big.Int)
					bf.Int(bi) // truncates toward zero
					biF := new(big.Float).SetPrec(128).SetInt(bi)
					frac := new(big.Float).SetPrec(128).Sub(bf2, biF)
					half := new(big.Float).SetPrec(128).SetFloat64(0.5)
					negHalf := new(big.Float).SetPrec(128).SetFloat64(-0.5)
					// Round half away from zero: use sign of original value to determine direction
					if bfSign >= 0 && frac.Cmp(half) >= 0 {
						bi.Add(bi, big.NewInt(1))
					} else if bfSign < 0 && frac.Cmp(negHalf) <= 0 {
						bi.Sub(bi, big.NewInt(1))
					}
					// Clamp to int64 range
					maxI := new(big.Int).SetInt64(maxVal)
					minI := new(big.Int).SetInt64(minVal)
					if bi.Cmp(maxI) > 0 {
						return maxVal
					}
					if bi.Cmp(minI) < 0 {
						return minVal
					}
					return bi.Int64()
				}
			}
			f, err := strconv.ParseFloat(numStr, 64)
			if err != nil {
				return int64(0)
			}
			if isUnsigned {
				if f < 0 {
					intVal = 0
				} else if f > float64(maxUnsigned) {
					intVal = int64(maxUnsigned)
				} else {
					intVal = mysqlRoundToInt(f)
				}
			} else if f > float64(maxVal) {
				intVal = maxVal
			} else if f < float64(minVal) {
				intVal = minVal
			} else {
				// MySQL rounds half away from zero for string-to-int conversion
				intVal = mysqlRoundToInt(f)
			}
		} else {
			n, err := strconv.ParseInt(numStr, 10, 64)
			if err != nil {
				// For signed BIGINT, use big.Int to avoid float64 precision issues at boundary.
				if !isUnsigned && baseType == "BIGINT" {
					bi := new(big.Int)
					if _, ok := bi.SetString(numStr, 10); ok {
						maxIBig := new(big.Int).SetInt64(maxVal)
						minIBig := new(big.Int).SetInt64(minVal)
						if bi.Cmp(maxIBig) > 0 {
							return maxVal
						}
						if bi.Cmp(minIBig) < 0 {
							return minVal
						}
						return bi.Int64()
					}
					return maxVal
				}
				// For unsigned types, try ParseUint first to handle values > MaxInt64.
				if isUnsigned && !strings.HasPrefix(numStr, "-") && baseType == "BIGINT" {
					u, uerr := strconv.ParseUint(numStr, 10, 64)
					if uerr == nil {
						if u > maxUnsigned {
							return maxUnsigned
						}
						return u
					}
					// ParseUint failed: number overflows uint64. Use big.Float for accurate comparison.
					bf := new(big.Float).SetPrec(128)
					if _, ok := bf.SetString(numStr); ok {
						maxUBig := new(big.Float).SetPrec(128).SetUint64(maxUnsigned)
						if bf.Sign() < 0 {
							return uint64(0)
						}
						if bf.Cmp(maxUBig) > 0 {
							return maxUnsigned
						}
						// Value is within range, extract as uint64
						u64, _ := bf.Uint64()
						return u64
					}
					return maxUnsigned
				}
				// Might be too large for int64
				f, err2 := strconv.ParseFloat(numStr, 64)
				if err2 != nil {
					return int64(0)
				}
				if isUnsigned {
					if f < 0 {
						intVal = 0
					} else if f > float64(maxUnsigned) {
						if baseType == "BIGINT" {
							return maxUnsigned
						}
						intVal = int64(maxUnsigned)
					} else {
						intVal = mysqlRoundToInt(f)
					}
				} else if f > float64(maxVal) {
					intVal = maxVal
				} else if f < float64(minVal) {
					intVal = minVal
				} else {
					intVal = int64(f)
				}
			} else {
				intVal = n
			}
		}
	default:
		return v
	}

	// Apply range constraints
	isZerofill := strings.Contains(upper, "ZEROFILL")
	if isUnsigned {
		if intVal < 0 {
			if baseType == "BIGINT" {
				if isZerofill {
					return applyIntZerofill(uint64(0), upper)
				}
				return uint64(0)
			}
			if isZerofill {
				return applyIntZerofill(int64(0), upper)
			}
			return int64(0)
		}
		if uint64(intVal) > maxUnsigned {
			if baseType == "BIGINT" {
				if isZerofill {
					return applyIntZerofill(maxUnsigned, upper)
				}
				return maxUnsigned
			}
			if isZerofill {
				return applyIntZerofill(int64(maxUnsigned), upper)
			}
			return int64(maxUnsigned)
		}
		if baseType == "BIGINT" {
			if isZerofill {
				return applyIntZerofill(uint64(intVal), upper)
			}
			return uint64(intVal)
		}
	} else {
		if intVal < minVal {
			return minVal
		}
		if intVal > maxVal {
			return maxVal
		}
	}
	if isZerofill {
		return applyIntZerofill(intVal, upper)
	}
	return intVal
}

// applyIntZerofill applies ZEROFILL zero-padding to an integer value for display.
// The display width is extracted from the column type (e.g., "INT(2) ZEROFILL" -> width 2).
// Returns a zero-padded string if a display width is specified, otherwise returns val unchanged.
func applyIntZerofill(val interface{}, upperColType string) interface{} {
	// Extract display width from INT(N), TINYINT(N), etc.
	var width int
	if idx := strings.Index(upperColType, "("); idx >= 0 {
		if n, err := fmt.Sscanf(upperColType[idx:], "(%d)", &width); err != nil || n == 0 {
			width = 0
		}
	}
	if width <= 0 {
		return val
	}
	var s string
	switch v := val.(type) {
	case int64:
		if v < 0 {
			return val // negative values not zero-padded in MySQL ZEROFILL (unsigned context)
		}
		s = fmt.Sprintf("%d", v)
	case uint64:
		s = fmt.Sprintf("%d", v)
	default:
		return val
	}
	for len(s) < width {
		s = "0" + s
	}
	return s
}

func coerceBitValue(colType string, v interface{}) interface{} {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	if !strings.HasPrefix(upper, "BIT") {
		return v
	}
	width := 1
	if n, err := fmt.Sscanf(upper, "BIT(%d)", &width); err != nil || n != 1 {
		width = 1
	}
	if width <= 0 {
		width = 1
	}
	if width > 64 {
		width = 64
	}
	parseBitString := func(s string) (uint64, bool) {
		s = strings.TrimSpace(s)
		if strings.HasPrefix(strings.ToLower(s), "b'") {
			body := s[2:]
			if idx := strings.Index(body, "'"); idx >= 0 {
				body = body[:idx]
			}
			i := 0
			for i < len(body) && (body[i] == '0' || body[i] == '1') {
				i++
			}
			body = body[:i]
			if body == "" {
				return 0, true
			}
			u, err := strconv.ParseUint(body, 2, 64)
			return u, err == nil
		}
		if strings.HasPrefix(s, "0b") || strings.HasPrefix(s, "0B") {
			body := s[2:]
			i := 0
			for i < len(body) && (body[i] == '0' || body[i] == '1') {
				i++
			}
			body = body[:i]
			if body == "" {
				return 0, true
			}
			u, err := strconv.ParseUint(body, 2, 64)
			return u, err == nil
		}
		return 0, false
	}
	var u uint64
	switch n := v.(type) {
	case int64:
		if n < 0 {
			u = 0
		} else {
			u = uint64(n)
		}
	case uint64:
		u = n
	case float64:
		if n < 0 {
			u = 0
		} else {
			u = uint64(int64(n))
		}
	case string:
		if bu, ok := parseBitString(n); ok {
			u = bu
			break
		}
		if iv, err := strconv.ParseInt(strings.TrimSpace(n), 10, 64); err == nil {
			if iv < 0 {
				u = 0
			} else {
				u = uint64(iv)
			}
			break
		}
		return int64(0)
	default:
		return v
	}
	mask := uint64(1<<width) - 1
	if width == 64 {
		mask = ^uint64(0)
	}
	if u > mask {
		u = mask
	}
	return int64(u)
}

// checkIntegerStrict validates integer constraints in strict mode.
// Returns an error if the value would be out of range or is not a valid integer.
func checkIntegerStrict(colType string, colName string, v interface{}) error {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	baseType := upper
	if idx := strings.Index(baseType, "("); idx >= 0 {
		baseType = baseType[:idx]
	}
	isUnsigned := strings.Contains(upper, "UNSIGNED")
	baseType = strings.TrimSpace(strings.Replace(strings.Replace(baseType, "UNSIGNED", "", 1), "ZEROFILL", "", 1))

	isIntType := false
	var minVal, maxVal int64
	var maxUnsigned uint64
	switch baseType {
	case "TINYINT":
		isIntType = true
		minVal, maxVal = -128, 127
		maxUnsigned = 255
	case "SMALLINT":
		isIntType = true
		minVal, maxVal = -32768, 32767
		maxUnsigned = 65535
	case "MEDIUMINT":
		isIntType = true
		minVal, maxVal = -8388608, 8388607
		maxUnsigned = 16777215
	case "INT", "INTEGER":
		isIntType = true
		minVal, maxVal = -2147483648, 2147483647
		maxUnsigned = 4294967295
	case "BIGINT":
		isIntType = true
		minVal, maxVal = -9223372036854775808, 9223372036854775807
		maxUnsigned = 18446744073709551615
	}
	if !isIntType {
		return nil
	}

	// Check string values for non-numeric content
	if s, ok := v.(string); ok {
		s = strings.TrimSpace(s)
		if s == "" {
			// Empty string → integer column: error in strict mode (MySQL error 1366).
			return mysqlError(1366, "HY000", fmt.Sprintf("Incorrect integer value: '' for column '%s' at row 1", colName))
		}
		sl := strings.ToLower(s)
		if sl == "true" || sl == "false" {
			return nil
		}
		// TIME-like strings (HH:MM:SS[.ffffff]) and DATETIME-like strings are converted
		// to their numeric HHMMSS or YYYYMMDDHHMMSS representation. MySQL allows this
		// even in strict mode (no error is raised).
		if strings.Count(s, ":") == 2 || (len(s) >= 19 && s[4] == '-' && s[7] == '-' && s[10] == ' ') {
			return nil
		}
		// Check if it's a valid number
		_, errInt := strconv.ParseInt(s, 10, 64)
		_, errFloat := strconv.ParseFloat(s, 64)
		if errInt != nil && errFloat != nil {
			// Check if the FIRST character starts a numeric sequence (leading numeric prefix).
			// MySQL: 'a59b' → error 1366 (no leading numeric); '1a' → error 1265 Data truncated (has leading numeric but trailing garbage).
			firstChar := rune(0)
			for _, c := range s {
				firstChar = c
				break
			}
			hasLeadingNumeric := (firstChar >= '0' && firstChar <= '9') || firstChar == '-' || firstChar == '.'
			if !hasLeadingNumeric {
				return mysqlError(1366, "HY000", fmt.Sprintf("Incorrect integer value: '%s' for column '%s' at row 1", s, colName))
			}
			// Has leading numeric but also trailing non-numeric → Data truncated.
			return mysqlError(1265, "01000", fmt.Sprintf("Data truncated for column '%s' at row 1", colName))
		}
	}

	// Check range for numeric values
	var intVal int64
	switch val := v.(type) {
	case int64:
		intVal = val
	case float64:
		// Avoid undefined behavior when float64 is out of int64 range.
		// Use explicit out-of-range check before converting to int64.
		if isUnsigned {
			if val < 0 {
				return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
			}
			if val > float64(maxUnsigned) {
				return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
			}
			// val is in unsigned range, convert safely via big.Float for BIGINT to avoid precision issues
			if baseType == "BIGINT" {
				bf := new(big.Float).SetPrec(64).SetFloat64(val)
				u, _ := bf.Uint64()
				if u > maxUnsigned {
					return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
				}
				return nil
			}
			intVal = int64(val)
		} else {
			if val > float64(maxVal) {
				return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
			}
			if val < float64(minVal) {
				return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
			}
			intVal = int64(val)
		}
	case uint64:
		if isUnsigned && val > maxUnsigned {
			return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
		}
		if !isUnsigned && val > uint64(maxVal) {
			// e.g. uint64(9223372036854775808) into BIGINT (maxVal=9223372036854775807)
			return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
		}
		return nil
	case string:
		val = strings.TrimSpace(val)
		if val == "" {
			// Empty string → integer column: error in strict mode.
			return mysqlError(1366, "HY000", fmt.Sprintf("Incorrect integer value: '' for column '%s' at row 1", colName))
		}
		vLower := strings.ToLower(val)
		if vLower == "true" {
			intVal = 1
			break
		}
		if vLower == "false" {
			intVal = 0
			break
		}
		numStr := ""
		hasDot := false
		hasExp := false
		for i, c := range val {
			if c == '-' && i == 0 {
				numStr += string(c)
			} else if c == '-' && hasExp {
				numStr += string(c)
			} else if c == '+' && hasExp {
				// skip +
			} else if c >= '0' && c <= '9' {
				numStr += string(c)
			} else if c == '.' && !hasDot {
				hasDot = true
				numStr += string(c)
			} else if (c == 'e' || c == 'E') && !hasExp && numStr != "" && numStr != "-" {
				hasExp = true
				hasDot = true // treat scientific notation as having decimal
				numStr += string(c)
			} else {
				break
			}
		}
		if numStr == "" || numStr == "-" {
			return nil
		}
		if hasDot {
			if baseType == "BIGINT" {
				// Special case: float64 can't represent BIGINT boundaries precisely.
				// Use big.Float with high precision to check range.
				bf := new(big.Float).SetPrec(128)
				if _, ok := bf.SetString(numStr); !ok {
					return nil
				}
				// Round: get integer value (truncate toward zero, then round based on frac)
				bfSign := bf.Sign() // sign of original value
				bfCopy := new(big.Float).SetPrec(128).Copy(bf)
				bi := new(big.Int)
				bf.Int(bi) // truncates toward zero
				biF := new(big.Float).SetPrec(128).SetInt(bi)
				frac := new(big.Float).SetPrec(128).Sub(bfCopy, biF)
				half := new(big.Float).SetPrec(128).SetFloat64(0.5)
				negHalf := new(big.Float).SetPrec(128).SetFloat64(-0.5)
				// Round half away from zero: use sign of original value
				if bfSign >= 0 && frac.Cmp(half) >= 0 {
					bi.Add(bi, big.NewInt(1))
				} else if bfSign < 0 && frac.Cmp(negHalf) <= 0 {
					bi.Sub(bi, big.NewInt(1))
				}
				if isUnsigned {
					if bi.Sign() < 0 {
						return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
					}
					maxU := new(big.Int).SetUint64(maxUnsigned)
					if bi.Cmp(maxU) > 0 {
						return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
					}
				} else {
					maxI := new(big.Int).SetInt64(maxVal)
					minI := new(big.Int).SetInt64(minVal)
					if bi.Cmp(maxI) > 0 || bi.Cmp(minI) < 0 {
						return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
					}
				}
				return nil
			}
			f, _ := strconv.ParseFloat(numStr, 64)
			// Use proper rounding (round half away from zero) for out-of-range detection
			intVal = mysqlRoundToInt(f)
		} else if isUnsigned && !strings.HasPrefix(numStr, "-") {
			// For unsigned columns, try ParseUint first to handle values > MaxInt64
			u, err := strconv.ParseUint(numStr, 10, 64)
			if err != nil {
				return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
			}
			if u > maxUnsigned {
				return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
			}
			return nil
		} else {
			n, err := strconv.ParseInt(numStr, 10, 64)
			if err != nil {
				return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
			}
			intVal = n
		}
	default:
		return nil
	}

	if isUnsigned {
		if intVal < 0 {
			return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
		}
		if uint64(intVal) > maxUnsigned {
			return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
		}
	} else {
		if intVal < minVal || intVal > maxVal {
			return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
		}
	}
	return nil
}

// validateEnumSetValue validates and normalizes a value for ENUM/SET columns.
func validateEnumSetValue(colType string, v interface{}) interface{} {
	lower := strings.ToLower(colType)
	if !strings.HasPrefix(lower, "enum(") && !strings.HasPrefix(lower, "set(") {
		return v
	}
	isEnum := strings.HasPrefix(lower, "enum(")
	inner := ""
	if isEnum {
		inner = colType[5 : len(colType)-1]
	} else {
		inner = colType[4 : len(colType)-1]
	}
	var allowed []string
	for _, part := range splitEnumValues(inner) {
		part = strings.Trim(part, "'")
		allowed = append(allowed, part)
	}

	// Handle integer values: convert valid indices/bitmasks to string labels.
	// Out-of-range integers are left as-is so the INSERT validation code can raise errors.
	switch iv := v.(type) {
	case int64:
		if isEnum {
			// ENUM: index 1..N maps to the Nth element; index 0 = empty string (valid).
			// Out-of-range indices are left as int64 for the INSERT path to reject.
			if iv == 0 {
				return EnumValue("")
			}
			if iv >= 1 && int(iv) <= len(allowed) {
				return EnumValue(allowed[iv-1])
			}
			// Out of range: leave as int64 for strict mode to handle
			return v
		}
		// SET bitmask: only convert if within valid range (0..2^N-1)
		maxMask := int64((1 << uint(len(allowed))) - 1)
		if iv >= 0 && iv <= maxMask {
			if iv == 0 {
				return ""
			}
			var valid []string
			for i, a := range allowed {
				if iv&(1<<uint(i)) != 0 {
					valid = append(valid, a)
				}
			}
			return strings.Join(valid, ",")
		}
		// Out of range: leave as int64 for strict mode to handle
		return v
	case uint64:
		return validateEnumSetValue(colType, int64(iv))
	}

	s, ok := v.(string)
	if !ok {
		// Other non-string values are left as-is.
		return v
	}
	if isEnum {
		if s == "" {
			return EnumValue(s)
		}
		// Check if the string is a numeric index (stored as string representation of int)
		if idx, err := strconv.ParseInt(s, 10, 64); err == nil {
			if idx == 0 {
				return EnumValue("")
			}
			if idx >= 1 && int(idx) <= len(allowed) {
				return EnumValue(allowed[idx-1])
			}
			return EnumValue("")
		}
		for _, a := range allowed {
			if strings.EqualFold(s, a) {
				return EnumValue(a)
			}
		}
		return EnumValue("")
	}
	// SET validation
	if s == "" {
		return s
	}
	// Check if the string is a numeric bitmask (stored as string representation of int)
	if bitmask, err := strconv.ParseInt(s, 10, 64); err == nil {
		if bitmask == 0 {
			return ""
		}
		var valid []string
		for i, a := range allowed {
			if bitmask&(1<<uint(i)) != 0 {
				valid = append(valid, a)
			}
		}
		return strings.Join(valid, ",")
	}
	members := strings.Split(s, ",")
	var valid []string
	for _, m := range members {
		m = strings.TrimSpace(m)
		matched := false
		// Prefer exact (case-sensitive) match first
		for _, a := range allowed {
			if m == a {
				valid = append(valid, a)
				matched = true
				break
			}
		}
		// Fall back to case-insensitive match
		if !matched {
			for _, a := range allowed {
				if strings.EqualFold(m, a) {
					valid = append(valid, a)
					break
				}
			}
		}
	}
	return strings.Join(valid, ",")
}

func splitEnumValues(s string) []string {
	var result []string
	var current strings.Builder
	inQuote := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '\'' {
			inQuote = !inQuote
			current.WriteByte(ch)
		} else if ch == ',' && !inQuote {
			result = append(result, strings.TrimSpace(current.String()))
			current.Reset()
		} else {
			current.WriteByte(ch)
		}
	}
	rest := strings.TrimSpace(current.String())
	if rest != "" {
		result = append(result, rest)
	}
	return result
}

// extractNumericPrefix tries to find the longest valid numeric prefix in a string s.
// For example, "123.4e" -> "123.4" (truncate trailing 'e' with no exponent digits).
// "123abc" -> "123", "-456.78e+2abc" -> "-456.78e+2".
// Returns ("", false) if no valid numeric prefix found.
func extractNumericPrefix(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false
	}
	i := 0
	// optional sign
	if i < len(s) && (s[i] == '+' || s[i] == '-') {
		i++
	}
	// digits before decimal
	digitsBeforeDecimal := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
		digitsBeforeDecimal++
	}
	// optional decimal point and fractional digits
	if i < len(s) && s[i] == '.' {
		i++
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
			digitsBeforeDecimal++ // count overall
		}
	}
	if digitsBeforeDecimal == 0 {
		return "", false
	}
	// optional exponent (only if followed by digits)
	ePos := i
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		j := i + 1
		if j < len(s) && (s[j] == '+' || s[j] == '-') {
			j++
		}
		if j < len(s) && s[j] >= '0' && s[j] <= '9' {
			// Valid exponent: advance i past exponent digits
			i = j
			for i < len(s) && s[i] >= '0' && s[i] <= '9' {
				i++
			}
		} else {
			// 'e' not followed by digits: truncate here (don't include the 'e')
			i = ePos
		}
	}
	prefix := s[:i]
	if prefix == "" || prefix == "+" || prefix == "-" {
		return "", false
	}
	return prefix, true
}

// parseDecimalString attempts to parse a string that may contain scientific notation
// or other numeric formats that strconv.ParseFloat cannot handle (e.g., extreme exponents).
// Returns the float64 value and a classification:
// "normal" - parsed fine, "overflow_pos" - huge positive, "overflow_neg" - huge negative,
// "zero" - tiny (rounds to zero), "invalid" - not a number.
func parseDecimalString(s string) (float64, string) {
	s = strings.TrimSpace(s)
	// Handle scientific notation with extreme exponents BEFORE ParseFloat
	// because ParseFloat returns Inf for large exponents but we need to distinguish
	// between "valid overflow" and "invalid (exponent overflows uint64)"
	sLower := strings.ToLower(s)
	cleanS := sLower
	negative := false
	if strings.HasPrefix(cleanS, "-") {
		negative = true
		cleanS = cleanS[1:]
	} else if strings.HasPrefix(cleanS, "+") {
		cleanS = cleanS[1:]
	}
	if idx := strings.Index(cleanS, "e"); idx >= 0 {
		expStr := cleanS[idx+1:]
		expNeg := false
		if strings.HasPrefix(expStr, "+") {
			expStr = expStr[1:]
		} else if strings.HasPrefix(expStr, "-") {
			expNeg = true
			expStr = expStr[1:]
		}
		// Check if the exponent value itself overflows uint64
		// MySQL treats these as "incorrect decimal value" -> 0
		// Special case: empty expStr means "123.4e" with trailing 'e' -> truncate to mantissa
		if expStr == "" {
			// Trailing 'e'/'E' with no exponent: truncate to the mantissa before 'e'
			mantissa := cleanS[:idx]
			if negative {
				mantissa = "-" + mantissa
			}
			f, perr := strconv.ParseFloat(mantissa, 64)
			if perr != nil {
				return 0, "truncated"
			}
			return f, "truncated"
		}
		expVal, err := strconv.ParseUint(expStr, 10, 64)
		if err != nil {
			// Exponent value overflows -> incorrect decimal value -> 0
			return 0, "zero"
		}
		if expNeg {
			// Very large negative exponent -> tiny number -> 0
			if expVal > 308 {
				return 0, "zero"
			}
			// Small negative exponent: let ParseFloat handle it normally
		} else if expVal > 308 {
			// Positive exponent: if very large, overflow
			if negative {
				return math.Inf(-1), "overflow_neg"
			}
			return math.Inf(1), "overflow_pos"
		}
	}
	// Try standard float parsing
	f, err := strconv.ParseFloat(s, 64)
	if err == nil {
		if math.IsInf(f, 1) {
			return f, "overflow_pos"
		}
		if math.IsInf(f, -1) {
			return f, "overflow_neg"
		}
		return f, "normal"
	}
	return 0, "invalid"
}

func roundDecimalStringHalfUp(s string, scale int) (string, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false
	}
	sign := ""
	if strings.HasPrefix(s, "-") {
		sign = "-"
		s = s[1:]
	} else if strings.HasPrefix(s, "+") {
		s = s[1:]
	}
	if strings.ContainsAny(s, "eE") {
		return "", false
	}
	intPart := s
	fracPart := ""
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		intPart = s[:dot]
		fracPart = s[dot+1:]
	}
	if intPart == "" {
		intPart = "0"
	}
	for _, ch := range intPart {
		if ch < '0' || ch > '9' {
			return "", false
		}
	}
	for _, ch := range fracPart {
		if ch < '0' || ch > '9' {
			return "", false
		}
	}
	if scale < 0 {
		scale = 0
	}
	if len(fracPart) < scale+1 {
		fracPart += strings.Repeat("0", scale+1-len(fracPart))
	}
	keep := fracPart
	if len(keep) > scale {
		keep = keep[:scale]
	}
	roundUp := len(fracPart) > scale && fracPart[scale] >= '5'

	digits := intPart + keep
	if digits == "" {
		digits = "0"
	}
	n := new(big.Int)
	if _, ok := n.SetString(digits, 10); !ok {
		return "", false
	}
	if roundUp {
		n.Add(n, big.NewInt(1))
	}
	outDigits := n.String()
	if scale == 0 {
		if sign == "-" && outDigits != "0" {
			return "-" + outDigits, true
		}
		return outDigits, true
	}
	if len(outDigits) <= scale {
		outDigits = strings.Repeat("0", scale-len(outDigits)+1) + outDigits
	}
	split := len(outDigits) - scale
	out := outDigits[:split] + "." + outDigits[split:]
	if sign == "-" && out != "0."+strings.Repeat("0", scale) {
		out = "-" + out
	}
	return out, true
}

// truncateDecimalString truncates (not rounds) a decimal string to `scale` decimal places.
// Returns ("", false) if the string is not a valid decimal number.
func truncateDecimalString(s string, scale int) (string, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false
	}
	sign := ""
	if strings.HasPrefix(s, "-") {
		sign = "-"
		s = s[1:]
	} else if strings.HasPrefix(s, "+") {
		s = s[1:]
	}
	if strings.ContainsAny(s, "eE") {
		return "", false
	}
	intPart := s
	fracPart := ""
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		intPart = s[:dot]
		fracPart = s[dot+1:]
	}
	if intPart == "" {
		intPart = "0"
	}
	for _, ch := range intPart {
		if ch < '0' || ch > '9' {
			return "", false
		}
	}
	for _, ch := range fracPart {
		if ch < '0' || ch > '9' {
			return "", false
		}
	}
	if scale < 0 {
		scale = 0
	}
	// Truncate fracPart to `scale` digits (no rounding).
	keep := fracPart
	if len(keep) > scale {
		keep = keep[:scale]
	}
	if scale == 0 {
		if sign == "-" && intPart != "0" {
			return "-" + intPart, true
		}
		return intPart, true
	}
	// Pad fracPart to exactly `scale` digits if shorter.
	if len(keep) < scale {
		keep += strings.Repeat("0", scale-len(keep))
	}
	out := intPart + "." + keep
	if sign == "-" && out != "0."+strings.Repeat("0", scale) {
		out = "-" + out
	}
	return out, true
}

func roundNumericStringHalfUp(s string, scale int) (string, bool) {
	if out, ok := roundDecimalStringHalfUp(s, scale); ok {
		return out, true
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false
	}
	plain, ok := scientificToPlainDecimal(s)
	if !ok {
		return "", false
	}
	return roundDecimalStringHalfUp(plain, scale)
}

func scientificToPlainDecimal(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false
	}
	sign := ""
	if strings.HasPrefix(s, "-") {
		sign = "-"
		s = s[1:]
	} else if strings.HasPrefix(s, "+") {
		s = s[1:]
	}
	parts := strings.Split(strings.ToLower(s), "e")
	if len(parts) != 2 {
		return "", false
	}
	mantissa := parts[0]
	exp, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", false
	}
	if exp > 10000 || exp < -10000 {
		return "", false
	}
	intPart := mantissa
	fracPart := ""
	if dot := strings.IndexByte(mantissa, '.'); dot >= 0 {
		intPart = mantissa[:dot]
		fracPart = mantissa[dot+1:]
	}
	if intPart == "" {
		intPart = "0"
	}
	for _, ch := range intPart {
		if ch < '0' || ch > '9' {
			return "", false
		}
	}
	for _, ch := range fracPart {
		if ch < '0' || ch > '9' {
			return "", false
		}
	}
	digits := strings.TrimLeft(intPart+fracPart, "0")
	if digits == "" {
		return "0", true
	}
	decimalPos := len(intPart) + exp
	var out string
	switch {
	case decimalPos <= 0:
		out = "0." + strings.Repeat("0", -decimalPos) + digits
	case decimalPos >= len(digits):
		out = digits + strings.Repeat("0", decimalPos-len(digits))
	default:
		out = digits[:decimalPos] + "." + digits[decimalPos:]
	}
	if sign == "-" && out != "0" && !strings.HasPrefix(out, "0.") {
		return "-" + out, true
	}
	if sign == "-" && strings.HasPrefix(out, "0.") && strings.Trim(out[2:], "0") != "" {
		return "-" + out, true
	}
	return out, true
}

func parseDecimalStringToRat(s string) (*big.Rat, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, false
	}
	if strings.ContainsAny(s, "eE") {
		bf, ok := new(big.Float).SetPrec(2048).SetString(s)
		if !ok {
			return nil, false
		}
		r, _ := bf.Rat(nil)
		if r == nil {
			return nil, false
		}
		return r, true
	}
	r := new(big.Rat)
	if _, ok := r.SetString(s); !ok {
		return nil, false
	}
	return r, true
}

func formatRatFixed(r *big.Rat, scale int) string {
	if r == nil {
		return "0"
	}
	if scale < 0 {
		scale = 0
	}
	bf := new(big.Float).SetPrec(4096).SetRat(r)
	return bf.Text('f', scale)
}

func clipDecimalIntegerString(s string, precision int, unsigned bool) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "0"
	}
	sign := ""
	if strings.HasPrefix(s, "-") {
		sign = "-"
		s = s[1:]
	} else if strings.HasPrefix(s, "+") {
		s = s[1:]
	}
	s = strings.TrimLeft(s, "0")
	if s == "" {
		return "0"
	}
	if unsigned && sign == "-" {
		return "0"
	}
	maxStr := strings.Repeat("9", precision)
	if len(s) > precision {
		if unsigned {
			return maxStr
		}
		if sign == "-" {
			return "-" + maxStr
		}
		return maxStr
	}
	if sign == "-" {
		return "-" + s
	}
	return s
}

func decimalMaxString(m, d int) string {
	if d <= 0 {
		if m <= 0 {
			return "0"
		}
		return strings.Repeat("9", m)
	}
	intDigits := m - d
	if intDigits <= 0 {
		intDigits = 1
	}
	return strings.Repeat("9", intDigits) + "." + strings.Repeat("9", d)
}

// coerceColumnValue applies the standard DML value coercion chain for a column:
// binary padding, decimal formatting, enum/set validation, datetime coercion,
// integer coercion, and bit coercion.
func coerceColumnValue(colType string, val interface{}) interface{} {
	if padLen := binaryPadLength(colType); padLen > 0 && val != nil {
		val = padBinaryValue(val, padLen)
	} else if isVarbinaryType(colType) && val != nil {
		// For VARBINARY, convert integer hex literals (int64/uint64) to their
		// big-endian byte string representation without fixed-length padding.
		val = hexIntToBytes(val)
	}
	if val != nil {
		val = formatDecimalValue(colType, val)
		val = validateEnumSetValue(colType, val)
		val = coerceDateTimeValue(colType, val)
		val = coerceIntegerValue(colType, val)
		val = coerceBitValue(colType, val)
	}
	return val
}

// formatDecimalValue formats a value for DECIMAL(M,D), DOUBLE(M,D), or FLOAT(M,D) columns.
func formatDecimalValue(colType string, v interface{}) interface{} {
	lower := strings.ToLower(colType)
	// Strip all trailing modifiers (unsigned, zerofill) in any order to get the base type
	cleanLower := strings.TrimSpace(lower)
	for {
		prev := cleanLower
		cleanLower = strings.TrimSuffix(cleanLower, " unsigned")
		cleanLower = strings.TrimSpace(cleanLower)
		cleanLower = strings.TrimSuffix(cleanLower, " zerofill")
		cleanLower = strings.TrimSpace(cleanLower)
		if cleanLower == prev {
			break
		}
	}
	var prefix string
	for _, p := range []string{"decimal", "double", "float", "real"} {
		if strings.HasPrefix(cleanLower, p+"(") {
			prefix = p
			break
		}
	}

	isUnsigned := strings.Contains(lower, "unsigned") || strings.Contains(lower, "zerofill") // ZEROFILL implies UNSIGNED

	// Handle bare DECIMAL/DOUBLE/FLOAT/REAL without (M,D)
	if prefix == "" {
		// Bare FLOAT/DOUBLE/REAL: convert to numeric value
		for _, p := range []string{"double", "float", "real"} {
			if cleanLower == p {
				f := toFloat(v)
				if math.IsInf(f, 1) {
					if p == "float" {
						f = float64(math.MaxFloat32)
					} else {
						f = math.MaxFloat64
					}
				}
				if math.IsInf(f, -1) {
					if p == "float" {
						f = -float64(math.MaxFloat32)
					} else {
						f = -math.MaxFloat64
					}
				}
				if isUnsigned && f < 0 {
					f = 0
				}
				if p == "float" {
					f = float64(float32(f))
					if math.IsInf(f, 1) {
						f = float64(math.MaxFloat32)
					} else if math.IsInf(f, -1) {
						f = -float64(math.MaxFloat32)
					}
					f = normalizeFloatSignificant(f, 6)
				}
				// Convert to int64 when the float is an exact integer
				if f == float64(int64(f)) {
					return int64(f)
				}
				return f
			}
		}
		// Bare DECIMAL -> default to (10,0), round to integer
		if cleanLower == "decimal" {
			f, cls := decimalParseValue(v)
			if cls == "invalid" {
				return v
			}
			if isUnsigned && f < 0 {
				f = 0
			}
			maxVal := float64(9999999999)
			if cls == "overflow_pos" || f > maxVal {
				f = maxVal
			}
			if cls == "overflow_neg" || f < -maxVal {
				if isUnsigned {
					f = 0
				} else {
					f = -maxVal
				}
			}
			// Helper to zero-pad integer for bare DECIMAL ZEROFILL (default M=10)
			zerofillInt := func(i int64) interface{} {
				if !strings.Contains(lower, "zerofill") {
					return i
				}
				const bareDecimalM = 10
				if i < 0 && isUnsigned {
					return strings.Repeat("0", bareDecimalM)
				}
				s := fmt.Sprintf("%d", i)
				neg := i < 0
				abs := s
				if neg {
					abs = s[1:]
				}
				for len(abs) < bareDecimalM {
					abs = "0" + abs
				}
				if neg {
					return "-" + abs
				}
				return abs
			}
			if f >= 0 {
				return zerofillInt(int64(f + 0.5))
			}
			return zerofillInt(-int64(-f + 0.5))
		}
		return v
	}
	// For string values, check if it's a valid number (including sci notation).
	if s, ok := v.(string); ok {
		_, cls := parseDecimalString(s)
		if cls == "invalid" {
			return v
		}
	}
	isZerofill := strings.Contains(lower, "zerofill")

	var m, d int
	// Try DECIMAL(M,D) first, then DECIMAL(M) which defaults to D=0
	if n, err := fmt.Sscanf(cleanLower, prefix+"(%d,%d)", &m, &d); (err == nil && n == 2) || func() bool {
		if n2, err2 := fmt.Sscanf(cleanLower, prefix+"(%d)", &m); err2 == nil && n2 == 1 {
			d = 0
			return true
		}
		return false
	}() {
		// applyZerofillStr zero-pads a decimal string for ZEROFILL DECIMAL(M,D).
		// For DECIMAL(M,D), the integer part is M-D digits wide.
		// e.g. DECIMAL(10,2) -> "1.23" -> "00000001.23"
		// For DECIMAL(M,0) ZEROFILL: "1" -> "0000000001"
		applyZerofillStr := func(s string) string {
			if !isZerofill || prefix != "decimal" {
				return s
			}
			// Strip leading + sign (not used in MySQL output normally)
			if strings.HasPrefix(s, "+") {
				s = s[1:]
			}
			negative := strings.HasPrefix(s, "-")
			abs := s
			if negative {
				abs = s[1:]
			}
			// ZEROFILL + UNSIGNED: negative values are clipped to 0
			if negative && isUnsigned {
				if d == 0 {
					return strings.Repeat("0", m)
				}
				intWidth2 := m - d
				if intWidth2 <= 0 {
					intWidth2 = 1
				}
				return strings.Repeat("0", intWidth2) + "." + strings.Repeat("0", d)
			}
			if d == 0 {
				// Integer DECIMAL(M,0) ZEROFILL: pad to M digits
				for len(abs) < m {
					abs = "0" + abs
				}
				if negative {
					return "-" + abs
				}
				return abs
			}
			dotIdx := strings.Index(abs, ".")
			var intPart, fracPart string
			if dotIdx >= 0 {
				intPart = abs[:dotIdx]
				fracPart = abs[dotIdx+1:]
			} else {
				intPart = abs
				fracPart = strings.Repeat("0", d)
			}
			intWidth := m - d
			if intWidth <= 0 {
				intWidth = 1
			}
			for len(intPart) < intWidth {
				intPart = "0" + intPart
			}
			padded := intPart + "." + fracPart
			if negative {
				return "-" + padded
			}
			return padded
		}
		// applyZerofillFromInt applies zerofill to an int64 value for DECIMAL(M,0) ZEROFILL.
		applyZerofillFromInt := func(i int64) interface{} {
			if !isZerofill || prefix != "decimal" || d != 0 {
				return i
			}
			if i < 0 && isUnsigned {
				return strings.Repeat("0", m)
			}
			s := fmt.Sprintf("%d", i)
			neg := i < 0
			abs2 := s
			if neg {
				abs2 = s[1:]
			}
			for len(abs2) < m {
				abs2 = "0" + abs2
			}
			if neg {
				return "-" + abs2
			}
			return abs2
		}

		f, cls := decimalParseValue(v)
		if prefix == "float" || prefix == "double" || prefix == "real" {
			f, cls = floatParseValue(v)
		}
		clipped := false

		// Compute max value for DECIMAL(M,D)
		intDigits := m - d
		if intDigits <= 0 {
			intDigits = 1
		}
		maxIntPart := 1.0
		for i := 0; i < intDigits; i++ {
			maxIntPart *= 10
		}
		maxFrac := 1.0
		for i := 0; i < d; i++ {
			maxFrac *= 10
		}
		maxVal := maxIntPart - 1.0/maxFrac
		if d == 0 {
			maxVal = maxIntPart - 1
		}

		// Handle unsigned: clip negative to 0
		if isUnsigned && (f < 0 || cls == "overflow_neg") {
			f = 0
			cls = "normal"
			clipped = true
		}

		// Clip to valid range (non-strict mode)
		if cls == "overflow_pos" || f > maxVal {
			f = maxVal
			clipped = true
		}
		if cls == "overflow_neg" || f < -maxVal {
			f = -maxVal
			clipped = true
		}

		if d == 0 {
			// Keep DECIMAL(M,0) as string for large precisions to avoid int64 overflow.
			if prefix == "decimal" {
				if s, ok := v.(string); ok {
					_, sCls := parseDecimalString(s)
					if sCls == "overflow_pos" {
						if isZerofill {
							return strings.Repeat("9", m)
						}
						return strings.Repeat("9", m)
					}
					if sCls == "overflow_neg" {
						if isUnsigned {
							return applyZerofillStr("0")
						}
						return "-" + strings.Repeat("9", m)
					}
					if sCls == "zero" {
						return applyZerofillStr("0")
					}
					if rounded, ok := roundNumericStringHalfUp(s, 0); ok {
						return applyZerofillStr(clipDecimalIntegerString(rounded, m, isUnsigned))
					}
				}
				if s, ok := v.(string); ok && !clipped {
					if rounded, ok := roundNumericStringHalfUp(s, 0); ok {
						return applyZerofillStr(rounded)
					}
				}
				if m > 18 || math.Abs(f) > float64(math.MaxInt64)-1 {
					// Use round-half-up for DECIMAL, not Go's round-half-to-even
					// math.Floor(f+0.5) implements round-half-up
					if f >= 0 {
						return applyZerofillStr(fmt.Sprintf("%.0f", math.Floor(f+0.5)))
					}
					return applyZerofillStr(fmt.Sprintf("%.0f", math.Ceil(f-0.5)))
				}
			}
			if prefix == "float" {
				f = float64(float32(f))
			}
			// Round to nearest integer (MySQL DECIMAL rounds, not truncates)
			if f >= 0 {
				return applyZerofillFromInt(int64(f + 0.5))
			}
			return applyZerofillFromInt(-int64(-f + 0.5))
		}
		if prefix == "decimal" {
			if clipped {
				maxStr := decimalMaxString(m, d)
				if isUnsigned && f == 0 {
					return applyZerofillStr("0." + strings.Repeat("0", d))
				}
				if f < 0 {
					return "-" + maxStr
				}
				return applyZerofillStr(maxStr)
			}
			// DECIMAL: round to d decimal places.
			if s, ok := v.(string); ok && !clipped {
				if rounded, ok := roundNumericStringHalfUp(s, d); ok {
					return applyZerofillStr(rounded)
				}
			}
			return applyZerofillStr(fmt.Sprintf("%.*f", d, f))
		}
		// FLOAT/DOUBLE/REAL(M,D): round to d decimal places.
		// For d <= 2, use fmt.Sprintf directly on the float64 value, which correctly
		// handles tie-breaking (e.g. 999.985 → 999.99) by using the full float64 binary
		// representation rather than scaled multiplication which loses precision.
		// For d > 2, use banker's rounding (RoundToEven) which matches MySQL behavior
		// for values like FLOAT(5,4) with -9.12345 → -9.1234 (RoundToEven(-91234.5) = -91234).
		if d <= 2 {
			return fmt.Sprintf("%.*f", d, f)
		}
		f = roundToEvenScale(f, d)
		if prefix == "float" {
			f = float64(float32(f))
		}
		return fmt.Sprintf("%.*f", d, f)
	}
	return v
}


func roundToEvenScale(f float64, d int) float64 {
	if d <= 0 {
		return math.RoundToEven(f)
	}
	factor := 1.0
	for i := 0; i < d; i++ {
		factor *= 10
	}
	return math.RoundToEven(f*factor) / factor
}

// normalizeFloatSignificant rounds f to n significant digits.
func normalizeFloatSignificant(f float64, n int) float64 {
	if f == 0 || n <= 0 || math.IsInf(f, 0) || math.IsNaN(f) {
		return f
	}
	abs := math.Abs(f)
	exp := math.Floor(math.Log10(abs))
	scale := float64(n-1) - exp
	if scale >= 0 {
		factor := math.Pow(10, scale)
		return math.Round(f*factor) / factor
	}
	step := math.Pow(10, -scale)
	return math.Round(f/step) * step
}

// decimalParseValue extracts a float64 and classification from any value type.
func decimalParseValue(v interface{}) (float64, string) {
	switch n := v.(type) {
	case int64:
		return float64(n), "normal"
	case uint64:
		return float64(n), "normal"
	case float64:
		if math.IsInf(n, 1) {
			return n, "overflow_pos"
		}
		if math.IsInf(n, -1) {
			return n, "overflow_neg"
		}
		return n, "normal"
	case string:
		return parseDecimalString(n)
	}
	return toFloat(v), "normal"
}

func floatParseValue(v interface{}) (float64, string) {
	switch n := v.(type) {
	case int64:
		return float64(n), "normal"
	case uint64:
		return float64(n), "normal"
	case float64:
		if math.IsInf(n, 1) {
			return n, "overflow_pos"
		}
		if math.IsInf(n, -1) {
			return n, "overflow_neg"
		}
		if math.IsNaN(n) {
			return 0, "invalid"
		}
		return n, "normal"
	case string:
		s := strings.TrimSpace(n)
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			if errors.Is(err, strconv.ErrRange) {
				if strings.HasPrefix(s, "-") {
					return math.Inf(-1), "overflow_neg"
				}
				return math.Inf(1), "overflow_pos"
			}
			return 0, "invalid"
		}
		if math.IsInf(f, 1) {
			return f, "overflow_pos"
		}
		if math.IsInf(f, -1) {
			return f, "overflow_neg"
		}
		if math.IsNaN(f) {
			return 0, "invalid"
		}
		return f, "normal"
	}
	f := toFloat(v)
	if math.IsInf(f, 1) {
		return f, "overflow_pos"
	}
	if math.IsInf(f, -1) {
		return f, "overflow_neg"
	}
	if math.IsNaN(f) {
		return 0, "invalid"
	}
	return f, "normal"
}

func coerceValueForColumnType(col catalog.ColumnDef, val interface{}) interface{} {
	if val == nil {
		return nil
	}
	if padLen := binaryPadLength(col.Type); padLen > 0 {
		val = padBinaryValue(val, padLen)
	} else if isVarbinaryType(col.Type) {
		val = hexIntToBytes(val)
	}
	val = formatDecimalValue(col.Type, val)
	val = validateEnumSetValue(col.Type, val)
	val = coerceDateTimeValue(col.Type, val)
	val = coerceIntegerValue(col.Type, val)
	val = coerceBitValue(col.Type, val)
	// Truncate BLOB/TEXT values when column type changes (e.g., LONGBLOB→BLOB)
	// MySQL behavior: data exceeding the new max length is set to empty string
	colUp := strings.ToUpper(col.Type)
	isBlobTy := colUp == "BLOB" || colUp == "TINYBLOB" || colUp == "MEDIUMBLOB"
	isTextTy := colUp == "TEXT" || colUp == "TINYTEXT" || colUp == "MEDIUMTEXT"
	if isBlobTy || isTextTy {
		if sv, ok := val.(string); ok {
			maxLen := extractCharLength(col.Type)
			if maxLen > 0 {
				if isBlobTy {
					if len(sv) > maxLen {
						val = "" // MySQL sets to empty string when BLOB data exceeds new column max
					}
				} else {
					if len([]rune(sv)) > maxLen {
						val = "" // MySQL sets to empty string when TEXT data exceeds new column max
					}
				}
			}
		}
	}
	return val
}

// checkIntegerRangeForColumn checks if the value is out of range for the given integer column type.
// Returns an error (MySQL 1264/22003) if out of range, nil otherwise.
// Only applies to integer types (TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT).
func checkIntegerRangeForColumn(colType string, colName string, v interface{}) error {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	// Strip generated column expression: everything from "GENERATED" onward
	if genIdx := strings.Index(upper, " GENERATED"); genIdx >= 0 {
		upper = strings.TrimSpace(upper[:genIdx])
	}
	isUnsigned := strings.Contains(upper, "UNSIGNED") || strings.Contains(upper, "ZEROFILL")
	baseType := upper
	// Remove display width like SMALLINT(6)
	if idx := strings.Index(baseType, "("); idx >= 0 {
		baseType = baseType[:idx]
	}
	baseType = strings.TrimSpace(strings.Replace(strings.Replace(baseType, "UNSIGNED", "", 1), "ZEROFILL", "", 1))
	baseType = strings.TrimSpace(baseType)

	rng, isIntType := intTypeRanges[baseType]
	if !isIntType {
		return nil
	}

	var intVal int64
	switch val := v.(type) {
	case int64:
		intVal = val
	case uint64:
		if isUnsigned {
			if val > rng.MaxUnsigned {
				return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
			}
			return nil
		}
		if val > uint64(rng.Max) {
			return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
		}
		return nil
	default:
		return nil
	}

	if isUnsigned {
		if intVal < 0 || uint64(intVal) > rng.MaxUnsigned {
			return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
		}
	} else {
		if intVal < rng.Min || intVal > rng.Max {
			return mysqlError(1264, "22003", fmt.Sprintf("Out of range value for column '%s' at row 1", colName))
		}
	}
	return nil
}
