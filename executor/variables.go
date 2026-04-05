package executor

import (
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/myuon/mylite/catalog"
	"vitess.io/vitess/go/vt/sqlparser"
)

const defaultSQLMode = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"

// execSet handles parsed SET statements.
func (e *Executor) execSet(stmt *sqlparser.Set) (*Result, error) {
	for _, expr := range stmt.Exprs {
		// Handle user variables (@var)
		if expr.Var.Scope == sqlparser.VariableScope {
			varName := expr.Var.Name.String()
			val, err := e.evalExpr(expr.Expr)
			if err != nil {
				// Fallback: use the string representation
				val = strings.Trim(sqlparser.String(expr.Expr), "'\"")
			}
			// Unwrap SysVarDouble to plain float64 so user variables
			// display as normal numbers (e.g. 90, not 90.000000).
			if sd, ok := val.(SysVarDouble); ok {
				val = sd.Value
			}
			e.userVars[varName] = val
			continue
		}
		name := strings.ToLower(expr.Var.Name.String())
		// Strip scope prefix for variable name lookup
		cleanVarName := strings.TrimPrefix(name, "global.")
		cleanVarName = strings.TrimPrefix(cleanVarName, "session.")
		cleanVarName = strings.TrimPrefix(cleanVarName, "local.")
		// performance_schema_consumer_* are startup-only options, not settable variables.
		if strings.HasPrefix(cleanVarName, "performance_schema_consumer_") ||
			cleanVarName == "performance_schema_instrument" {
			return nil, mysqlError(1193, "HY000", fmt.Sprintf("Unknown system variable '%s'", cleanVarName))
		}
		// Check if variable is read-only
		if sysVarReadOnly[cleanVarName] {
			return nil, mysqlError(1238, "HY000", fmt.Sprintf("Variable '%s' is a read only variable", cleanVarName))
		}
		// Check if GLOBAL-only variable is being set at SESSION scope
		scope := expr.Var.Scope
		if sysVarGlobalOnly[cleanVarName] && !sysVarBothScope[cleanVarName] && scope != sqlparser.GlobalScope {
			return nil, mysqlError(1228, "HY000", fmt.Sprintf("Variable '%s' is a GLOBAL variable and should be set with SET GLOBAL", cleanVarName))
		}
		// Check if SESSION-only variable is being set at GLOBAL scope
		if sysVarSessionOnly[cleanVarName] && scope == sqlparser.GlobalScope {
			return nil, mysqlError(1229, "HY000", fmt.Sprintf("Variable '%s' is a SESSION variable and can't be used with SET GLOBAL", cleanVarName))
		}
		// Type validation for int-range and float-range variables must happen BEFORE
		// scope checks (MySQL checks type before scope for these variables).
		if _, isIntRange := sysVarIntRange[cleanVarName]; isIntRange {
			if typeErr := checkIntVarType(cleanVarName, expr.Expr); typeErr != nil {
				return nil, typeErr
			}
		} else if _, isFltRange := sysVarFloatRange[cleanVarName]; isFltRange {
			if typeErr := checkFloatVarType(cleanVarName, expr.Expr); typeErr != nil {
				return nil, typeErr
			}
		}
		// Check if session-read-only variable is being set at SESSION scope
		if sysVarSessionReadOnly[cleanVarName] && scope != sqlparser.GlobalScope {
			return nil, mysqlError(1238, "HY000", fmt.Sprintf("SESSION variable '%s' is read-only. Use SET GLOBAL to assign the value", cleanVarName))
		}
		// Emit deprecation warning for deprecated variables
		if msg, ok := sysVarDeprecated[cleanVarName]; ok {
			e.addWarning("Warning", 1287, msg)
		}
		// Reject NULL for string-type system variables (MySQL error 1231)
		if _, isNull := expr.Expr.(*sqlparser.NullVal); isNull {
			if sysVarStringType[cleanVarName] {
				return nil, mysqlError(1231, "42000", fmt.Sprintf("Variable '%s' can't be set to the value of 'NULL'", cleanVarName))
			}
		}
		// Reject numeric literals or bare identifiers for pure-string system variables (MySQL error 1232)
		if sysVarPureStringType[cleanVarName] && isInvalidStringVarExpr(cleanVarName, expr.Expr) {
			return nil, mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", cleanVarName))
		}
		val := sqlparser.String(expr.Expr)
		val = strings.Trim(val, "'\"")
		// Detect bare-identifier arithmetic expressions like SET @@var = name1 + name2.
		// MySQL treats bare identifiers in SET values as column references; if they appear
		// in an arithmetic expression (BinaryExpr with ColName operands) and there's no
		// table context, MySQL returns ER_BAD_FIELD_ERROR.
		if binExpr, ok := expr.Expr.(*sqlparser.BinaryExpr); ok {
			if setExprContainsColName(binExpr) {
				// Extract the first ColName for the error message
				colName := setExprFirstColName(binExpr)
				return nil, mysqlError(1054, "42S22", fmt.Sprintf("Unknown column '%s' in 'field list'", colName))
			}
		}
		// Try evaluating the expression (handles @user_var, @@system_var references)
		if evalVal, evalErr := e.evalExpr(expr.Expr); evalErr != nil {
			// If the expression itself fails, propagate ER_BAD_FIELD_ERROR
			// unless this variable accepts unquoted identifiers (e.g., innodb_monitor_*)
			if isMySQLError(evalErr, 1054) && !sysVarAcceptIdentifier[cleanVarName] { // ER_BAD_FIELD_ERROR
				return nil, evalErr
			}
		} else if evalVal != nil {
			val = fmt.Sprintf("%v", evalVal)
		}
		switch name {
		case "names":
			charset := strings.ToLower(val)
			if charset == "default" {
				delete(e.sessionScopeVars, "character_set_client")
				delete(e.sessionScopeVars, "character_set_connection")
				delete(e.sessionScopeVars, "character_set_results")
				delete(e.sessionScopeVars, "collation_connection")
			} else if charset != "binary" && !isKnownCharset(charset) {
				return nil, mysqlError(1115, "42000", fmt.Sprintf("Unknown character set: '%s'", charset))
			} else {
				e.sessionScopeVars["character_set_client"] = charset
				e.sessionScopeVars["character_set_connection"] = charset
				e.sessionScopeVars["character_set_results"] = charset
			}
		case "sql_mode":
			if _, isNull := expr.Expr.(*sqlparser.NullVal); isNull {
				return nil, mysqlError(1231, "42000", "Variable 'sql_mode' can't be set to the value of 'NULL'")
			}
			upperVal := strings.ToUpper(val)
			// Reject boolean-like values that are not valid SQL modes
			if upperVal == "OFF" || upperVal == "ON" || upperVal == "TRUE" || upperVal == "FALSE" {
				return nil, mysqlError(1231, "42000", fmt.Sprintf("Variable 'sql_mode' can't be set to the value of '%s'", strings.ToLower(val)))
			}
			var modeVal string
			if upperVal == "DEFAULT" {
				modeVal = defaultSQLMode
			} else {
				modeVal = expandSQLMode(upperVal)
			}
			if scope == sqlparser.GlobalScope {
				e.setGlobalVar("sql_mode", modeVal)
			} else {
				e.sqlMode = modeVal
				e.sessionScopeVars["sql_mode"] = modeVal
			}
			// Emit warning 3135 when NO_ZERO_DATE/NO_ZERO_IN_DATE/ERROR_FOR_DIVISION_BY_ZERO
			// are explicitly set (MySQL deprecation warning — these will merge into strict mode).
			// Warning is emitted unless TRADITIONAL mode is used (which already includes these).
			if modeVal != defaultSQLMode && !strings.Contains(modeVal, "TRADITIONAL") {
				hasSensitiveMode := strings.Contains(modeVal, "NO_ZERO_DATE") ||
					strings.Contains(modeVal, "NO_ZERO_IN_DATE") ||
					strings.Contains(modeVal, "ERROR_FOR_DIVISION_BY_ZERO")
				if hasSensitiveMode {
					e.addWarning("Warning", 3135, "'NO_ZERO_DATE', 'NO_ZERO_IN_DATE' and 'ERROR_FOR_DIVISION_BY_ZERO' sql modes should be used with strict mode. They will be merged with strict mode in a future release.")
				}
			}
		case "sql_auto_is_null":
			isOn := val == "1" || strings.ToUpper(val) == "ON" || strings.ToUpper(val) == "TRUE"
			boolStr := "OFF"
			if isOn {
				boolStr = "ON"
			}
			if scope == sqlparser.GlobalScope {
				e.setGlobalVar("sql_auto_is_null", boolStr)
			} else {
				e.sqlAutoIsNull = isOn
				e.sessionScopeVars["sql_auto_is_null"] = boolStr
			}
		case "timestamp":
			n, err := strconv.ParseFloat(val, 64)
			if err == nil {
				if n == 0 {
					e.fixedTimestamp = nil
				} else {
					t := time.Unix(int64(n), 0)
					if e.timeZone != nil {
						t = t.In(e.timeZone)
					}
					e.fixedTimestamp = &t
				}
			}
		case "time_zone":
			if err := e.parseTimeZone(val); err != nil {
				return nil, mysqlError(1298, "HY000", err.Error())
			}
			// Update session/global scope vars so @@time_zone reads back correctly
			if e.timeZone != nil {
				tzStr := e.timeZone.String()
				// time.FixedZone preserves the original string we passed
				if scope == sqlparser.GlobalScope {
					e.setGlobalVar("time_zone", tzStr)
				} else {
					e.sessionScopeVars["time_zone"] = tzStr
				}
			} else {
				if scope == sqlparser.GlobalScope {
					e.setGlobalVar("time_zone", "SYSTEM")
				} else {
					delete(e.sessionScopeVars, "time_zone")
				}
			}
		case "insert_id":
			// insert_id only accepts integers (not floats or strings)
			if strings.ContainsAny(val, ".eE") {
				return nil, mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable 'insert_id'"))
			}
			if n, err := strconv.ParseInt(val, 10, 64); err == nil {
				if n < 0 {
					// Negative values are truncated to 0 (insert_id is unsigned)
					e.addWarning("Warning", 1292, fmt.Sprintf("Truncated incorrect insert_id value: '%s'", val))
					n = 0
				}
				e.nextInsertID = n
				e.sessionScopeVars["insert_id"] = fmt.Sprintf("%d", n)
			} else {
				return nil, mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable 'insert_id'"))
			}
		case "identity", "last_insert_id":
			// identity/last_insert_id only accepts integers (not floats or strings)
			if strings.ContainsAny(val, ".eE") {
				return nil, mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", name))
			}
			if n, err := strconv.ParseInt(val, 10, 64); err == nil {
				if n < 0 {
					// Negative values are truncated to 0 (unsigned)
					e.addWarning("Warning", 1292, fmt.Sprintf("Truncated incorrect %s value: '%s'", name, val))
					n = 0
				}
				e.lastInsertID = n
			} else {
				return nil, mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", name))
			}
		case "character_set_client", "character_set_connection", "character_set_results",
			"character_set_filesystem", "character_set_server", "character_set_database":
			isGlobal := scope == sqlparser.GlobalScope
			// MySQL allows SET @@character_set_results = NULL to disable result conversion.
			if _, isNull := expr.Expr.(*sqlparser.NullVal); isNull {
				if name == "character_set_results" {
					if isGlobal {
						e.deleteSysVar(name, true)
					} else {
						delete(e.sessionScopeVars, name)
					}
				}
				break
			}
			if strings.ToUpper(val) == "NULL" {
				if name == "character_set_results" {
					if isGlobal {
						e.deleteSysVar(name, true)
					} else {
						delete(e.sessionScopeVars, name)
					}
				}
				break
			}
			if strings.ToUpper(val) == "DEFAULT" {
				if isGlobal {
					e.deleteSysVar(name, true)
				} else {
					if gv, ok := e.getGlobalVar(name); ok {
						e.sessionScopeVars[name] = gv
					} else {
						delete(e.sessionScopeVars, name)
					}
				}
			} else {
				csVal := strings.ToLower(val)
				// Float values are an incorrect argument type
				if strings.ContainsAny(csVal, ".eE") {
					if _, fErr := strconv.ParseFloat(csVal, 64); fErr == nil {
						return nil, mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", name))
					}
				}
				// Resolve numeric charset ID to name
				if id, err := strconv.ParseInt(csVal, 10, 64); err == nil {
					if csName, ok := resolveCharsetID(id); ok {
						csVal = csName
					} else {
						return nil, mysqlError(1115, "42000", fmt.Sprintf("Unknown character set: '%s'", truncateErrVal(val)))
					}
				}
				// Normalize utf8mb3 alias
				if csVal == "utf8mb3" {
					csVal = "utf8"
				}
				// Validate charset name
				if !isKnownCharset(csVal) {
					return nil, mysqlError(1115, "42000", fmt.Sprintf("Unknown character set: '%s'", truncateErrVal(val)))
				}
				// Reject non-client-safe charsets for character_set_client and character_set_connection
				if (name == "character_set_client" || name == "character_set_connection") && isNonClientCharset(csVal) {
					return nil, mysqlError(1231, "42000", fmt.Sprintf("Variable '%s' can't be set to the value of '%s'", name, csVal))
				}
				e.setSysVar(name, csVal, isGlobal)
			}
		case "collation_connection", "collation_server", "collation_database":
			isGlobal := scope == sqlparser.GlobalScope
			if strings.ToUpper(val) == "DEFAULT" {
				if isGlobal {
					e.deleteSysVar(name, true)
				} else {
					if gv, ok := e.getGlobalVar(name); ok {
						e.sessionScopeVars[name] = gv
					} else {
						delete(e.sessionScopeVars, name)
					}
				}
			} else {
				collVal := strings.ToLower(val)
				// Float values are an incorrect argument type
				if strings.ContainsAny(collVal, ".eE") {
					if _, fErr := strconv.ParseFloat(collVal, 64); fErr == nil {
						return nil, mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", name))
					}
				}
				// Resolve numeric collation ID to name
				if id, err := strconv.ParseInt(collVal, 10, 64); err == nil {
					if collName, ok := resolveCollationID(id); ok {
						collVal = collName
					} else {
						return nil, mysqlError(1273, "HY000", fmt.Sprintf("Unknown collation: '%s'", truncateErrVal(val)))
					}
				}
				// Normalize utf8mb3_ prefix
				if strings.HasPrefix(collVal, "utf8mb3_") {
					collVal = "utf8_" + strings.TrimPrefix(collVal, "utf8mb3_")
				}
				// Validate collation name
				if !isKnownCollation(collVal) {
					return nil, mysqlError(1273, "HY000", fmt.Sprintf("Unknown collation: '%s'", truncateErrVal(val)))
				}
				e.setSysVar(name, collVal, isGlobal)
			}
		default:
			// Store any SET GLOBAL/SESSION variable for later retrieval
			if name != "" {
				// Strip scope prefix
				cleanName := strings.TrimPrefix(name, "global.")
				cleanName = strings.TrimPrefix(cleanName, "session.")
				cleanName = strings.TrimPrefix(cleanName, "local.")
				isGlobal := scope == sqlparser.GlobalScope
				// Enforce SUPER privilege for setting security-sensitive global variables.
				// Non-root (non-SUPER) users get ER_SPECIFIC_ACCESS_DENIED_ERROR (1227).
				if isGlobal && superOnlyGlobalVars[cleanName] {
					isNonRoot := false
					if cu, ok := e.userVars["__current_user"]; ok {
						if cuStr, ok2 := cu.(string); ok2 && cuStr != "" && !strings.EqualFold(cuStr, "root") {
							isNonRoot = true
						}
					}
					if isNonRoot {
						return nil, mysqlError(1227, "42000", "Access denied; you need (at least one of) the SUPER or SYSTEM_VARIABLES_ADMIN privilege(s) for this operation")
					}
				}
				// Save previous session value before SET GLOBAL overwrites it.
				savedSessionVal := map[string]string{}
				if isGlobal {
					if prev, ok := e.sessionScopeVars[cleanName]; ok {
						savedSessionVal[cleanName] = prev
					}
				}
				// Handle DEFAULT: restore to default value (or emulate known MySQL special defaults).
				if _, isDefault := expr.Expr.(*sqlparser.Default); isDefault {
					if cleanName == "connect_timeout" {
						e.setSysVar(cleanName, "10", isGlobal)
					} else if cleanName == "innodb_io_capacity_max" {
						e.setSysVar(cleanName, "4294967295", isGlobal)
					} else if cleanName == "foreign_key_checks" || cleanName == "unique_checks" {
						// DEFAULT for these boolean session variables is OFF (0)
						e.setSysVar(cleanName, "0", isGlobal)
					} else if isGlobal {
						// For SET GLOBAL var = DEFAULT, reset to compiled default.
						// If startupVars has an override for this variable, we must
						// explicitly set globalScopeVars to the compiled default so
						// it takes precedence over the startup value.
						// Exception: innodb_commit_concurrency uses startup value as
						// its DEFAULT because the zero<->non-zero transition rule
						// would block resetting to the compiled default (0).
						if cleanName == "innodb_commit_concurrency" {
							if sv, hasSV := e.startupVars[cleanName]; hasSV {
								e.setGlobalVar(cleanName, sv)
							} else {
								e.deleteSysVar(cleanName, true)
							}
						} else if _, hasStartup := e.startupVars[cleanName]; hasStartup {
							if compiled, ok := e.getCompiledDefault(cleanName); ok {
								e.setGlobalVar(cleanName, compiled)
							} else {
								e.deleteSysVar(cleanName, true)
							}
						} else {
							e.deleteSysVar(cleanName, true)
						}
					} else {
						// For SET SESSION var = DEFAULT, set session to current global value.
						// In MySQL, DEFAULT for session means "current global value".
						if gv, ok := e.getGlobalVar(cleanName); ok {
							e.sessionScopeVars[cleanName] = gv
						} else {
							// No global override, fall back to compiled default
							delete(e.sessionScopeVars, cleanName)
						}
					}
					// When max_join_size is reset to DEFAULT, sql_big_selects goes back to ON.
					if cleanName == "max_join_size" {
						if isGlobal {
							e.setGlobalVar("sql_big_selects", "ON")
						} else {
							e.sessionScopeVars["sql_big_selects"] = "ON"
						}
					}
					// Skip the isGlobal move-to-global block below since DEFAULT
					// already handled the proper scope placement.
					continue
				} else if fltRng, isFlt := sysVarFloatRange[cleanName]; isFlt {
					clamped, err := e.clampFloatVar(cleanName, expr.Expr, fltRng.Min, fltRng.Max)
					if err != nil {
						return nil, err
					}
					// Cross-variable constraint: lwm cannot exceed innodb_max_dirty_pages_pct
					if cleanName == "innodb_max_dirty_pages_pct_lwm" {
						if f, parseErr := strconv.ParseFloat(clamped, 64); parseErr == nil {
							maxPct := 90.0 // default
							if v, ok := e.getSysVar("innodb_max_dirty_pages_pct"); ok && v != "" {
								if parsed, err := strconv.ParseFloat(v, 64); err == nil {
									maxPct = parsed
								}
							}
							if f > maxPct {
								e.addWarning("Warning", 1210, "innodb_max_dirty_pages_pct_lwm cannot be set higher than innodb_max_dirty_pages_pct.")
								e.addWarning("Warning", 1210, fmt.Sprintf("Setting innodb_max_dirty_page_pct_lwm to %s", strconv.FormatFloat(maxPct, 'f', 6, 64)))
								clamped = strconv.FormatFloat(maxPct, 'f', 6, 64)
							}
						}
					}
					e.sessionScopeVars[cleanName] = clamped
				} else if rng, ok := sysVarIntRange[cleanName]; ok {
					var clamped string
					var err error
					if rng.BlockSize > 0 {
						clamped, err = e.clampIntVar(cleanName, expr.Expr, rng.Min, rng.Max, rng.IsUnsigned, rng.BlockSize)
					} else {
						clamped, err = e.clampIntVar(cleanName, expr.Expr, rng.Min, rng.Max, rng.IsUnsigned)
					}
					if err != nil {
						return nil, err
					}
					// innodb_log_write_ahead_size must be a power of 2
					if cleanName == "innodb_log_write_ahead_size" {
						if n, parseErr := strconv.ParseUint(clamped, 10, 64); parseErr == nil && n > 0 {
							// Check if n is a power of 2
							if n&(n-1) != 0 {
								// Round up to nearest power of 2
								rounded := uint64(1)
								for rounded < n {
									rounded <<= 1
								}
								if rounded > 16384 {
									rounded = 16384
								}
								e.addWarning("Warning", 1210, fmt.Sprintf("innodb_log_write_ahead_size should be set to power of 2, in range [512,16384]"))
								e.addWarning("Warning", 1210, fmt.Sprintf("Setting innodb_log_write_ahead_size to %d", rounded))
								clamped = strconv.FormatUint(rounded, 10)
							}
						}
					}
					// For parser_max_mem_size, the global value caps session values
					// for non-superuser connections. Root/superuser can exceed the
					// global value.
					if !isGlobal && cleanName == "parser_max_mem_size" {
						isSuper := true
						if cu, ok := e.userVars["__current_user"]; ok {
							if cuStr, ok := cu.(string); ok && cuStr != "" && !strings.EqualFold(cuStr, "root") {
								isSuper = false
							}
						}
						if !isSuper {
							if gv, ok := e.getGlobalVar(cleanName); ok {
								if gMax, err := strconv.ParseUint(gv, 10, 64); err == nil {
									if cv, err := strconv.ParseUint(clamped, 10, 64); err == nil && cv > gMax {
										e.addWarning("Warning", 1292, fmt.Sprintf("Truncated incorrect %s value: '%s'", cleanName, clamped))
										clamped = strconv.FormatUint(gMax, 10)
									}
								}
							}
						}
					}
					e.sessionScopeVars[cleanName] = clamped
					// When max_join_size is set, update sql_big_selects accordingly.
					// Non-default value -> sql_big_selects = OFF; default -> ON.
					if cleanName == "max_join_size" && !isGlobal {
						if clamped == "18446744073709551615" {
							e.sessionScopeVars["sql_big_selects"] = "ON"
						} else {
							e.sessionScopeVars["sql_big_selects"] = "OFF"
						}
					}
				} else if cleanName == "innodb_io_capacity_max" {
					// INTEGER only, minimum 100, and cannot be set lower than innodb_io_capacity.
					if lit, isLit := expr.Expr.(*sqlparser.Literal); isLit {
						litStr := strings.TrimSpace(sqlparser.String(lit))
						if strings.HasPrefix(litStr, "'") || strings.HasPrefix(litStr, "\"") || strings.ContainsAny(litStr, ".eE") {
							return nil, mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", cleanName))
						}
					}
					evalVal, err := e.evalExpr(expr.Expr)
					if err != nil || evalVal == nil {
						return nil, mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", cleanName))
					}
					n := toInt64(evalVal)
					if n < 100 {
						e.warnings = append(e.warnings, Warning{
							Level:   "Warning",
							Code:    1292,
							Message: fmt.Sprintf("Truncated incorrect %s value: '%d'", cleanName, n),
						})
						n = 100
					}
					minVal := int64(200)
					if curMin, ok := e.getSysVar("innodb_io_capacity"); ok && curMin != "" {
						if parsed, err := strconv.ParseInt(curMin, 10, 64); err == nil {
							minVal = parsed
						}
					} else if startupMin := e.startupVars["innodb_io_capacity"]; startupMin != "" {
						if parsed, err := strconv.ParseInt(startupMin, 10, 64); err == nil {
							minVal = parsed
						}
					}
					if n < minVal {
						e.warnings = append(e.warnings,
							Warning{Level: "Warning", Code: 1210, Message: "innodb_io_capacity_max cannot be set lower than innodb_io_capacity."},
							Warning{Level: "Warning", Code: 1210, Message: fmt.Sprintf("Setting innodb_io_capacity_max to %d", minVal)},
						)
						n = minVal
					}
					e.sessionScopeVars[cleanName] = fmt.Sprintf("%d", n)
				} else if cleanName == "innodb_io_capacity" {
					// INTEGER only, minimum 100, and cannot exceed innodb_io_capacity_max.
					if lit, isLit := expr.Expr.(*sqlparser.Literal); isLit {
						litStr := strings.TrimSpace(sqlparser.String(lit))
						if strings.HasPrefix(litStr, "'") || strings.HasPrefix(litStr, "\"") || strings.ContainsAny(litStr, ".eE") {
							return nil, mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", cleanName))
						}
					}
					evalVal, err := e.evalExpr(expr.Expr)
					if err != nil || evalVal == nil {
						return nil, mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", cleanName))
					}
					n := toInt64(evalVal)
					if n < 100 {
						e.warnings = append(e.warnings, Warning{
							Level:   "Warning",
							Code:    1292,
							Message: fmt.Sprintf("Truncated incorrect %s value: '%d'", cleanName, n),
						})
						n = 100
					}
					maxVal := int64(2000)
					if curMax, ok := e.getSysVar("innodb_io_capacity_max"); ok && curMax != "" {
						if parsed, err := strconv.ParseInt(curMax, 10, 64); err == nil {
							maxVal = parsed
						}
					} else if startupMax := e.startupVars["innodb_io_capacity_max"]; startupMax != "" {
						if parsed, err := strconv.ParseInt(startupMax, 10, 64); err == nil {
							maxVal = parsed
						}
					}
					if n > maxVal {
						e.warnings = append(e.warnings,
							Warning{Level: "Warning", Code: 1210, Message: "innodb_io_capacity cannot be set higher than innodb_io_capacity_max."},
							Warning{Level: "Warning", Code: 1210, Message: fmt.Sprintf("Setting innodb_io_capacity to %d", maxVal)},
						)
						n = maxVal
					}
					e.sessionScopeVars[cleanName] = fmt.Sprintf("%d", n)
				} else if cleanName == "innodb_fsync_threshold" {
					// Must be non-negative and aligned to InnoDB page size (4KB); otherwise 0.
					evalVal, err := e.evalExpr(expr.Expr)
					if err != nil || evalVal == nil {
						return nil, mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", cleanName))
					}
					n := toInt64(evalVal)
					if n < 0 || (n != 0 && n%4096 != 0) {
						n = 0
					}
					e.sessionScopeVars[cleanName] = fmt.Sprintf("%d", n)
				} else if cleanName == "innodb_tmpdir" {
					// innodb_tmpdir must be a valid path
					evalVal, _ := e.evalExpr(expr.Expr)
					tmpPath := toString(evalVal)
					if tmpPath != "" {
						if _, err := os.Stat(tmpPath); os.IsNotExist(err) {
							errMsg := fmt.Sprintf("Variable 'innodb_tmpdir' can't be set to the value of '%s'", tmpPath)
							e.warnings = append(e.warnings,
								Warning{Level: "Warning", Code: 1210, Message: "InnoDB: Path doesn't exist."},
								Warning{Level: "Error", Code: 1231, Message: errMsg},
							)
							return nil, mysqlError(1231, "42000", errMsg)
						}
					}
					e.sessionScopeVars[cleanName] = tmpPath
				} else if cleanName == "innodb_commit_concurrency" {
					// innodb_commit_concurrency cannot transition between 0 and non-zero.
					// Type validation: reject non-integer literals (strings, floats)
					if lit, isLit := expr.Expr.(*sqlparser.Literal); isLit {
						litStr := strings.TrimSpace(sqlparser.String(lit))
						if strings.HasPrefix(litStr, "'") || strings.HasPrefix(litStr, "\"") || strings.ContainsAny(litStr, ".eE") {
							return nil, mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", cleanName))
						}
					}
					evalVal, _ := e.evalExpr(expr.Expr)
					// Reject string and float values from evaluated expressions
					switch evalVal.(type) {
					case string:
						s := evalVal.(string)
						if _, err := strconv.ParseInt(s, 10, 64); err != nil {
							return nil, mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", cleanName))
						}
					case float32, float64:
						return nil, mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", cleanName))
					}
					newVal := toInt64(evalVal)
					currentVal, _ := e.getSysVar(cleanName)
					if currentVal == "" {
						currentVal = e.startupVars[cleanName]
					}
					curIsZero := currentVal == "0"
					newIsZero := newVal == 0
					if curIsZero != newIsZero {
						return nil, mysqlError(1231, "42000", fmt.Sprintf("Variable 'innodb_commit_concurrency' can't be set to the value of '%v'", evalVal))
					}
					if isGlobal {
						e.setGlobalVar(cleanName, fmt.Sprintf("%d", newVal))
						if prevVal, had := savedSessionVal[cleanName]; had {
							e.sessionScopeVars[cleanName] = prevVal
						}
						continue
					} else {
						e.sessionScopeVars[cleanName] = fmt.Sprintf("%d", newVal)
					}
				} else if cleanName == "default_storage_engine" || cleanName == "default_tmp_storage_engine" || cleanName == "storage_engine" {
					// Normalize engine names for storage engine variables
					if _, isNull := expr.Expr.(*sqlparser.NullVal); isNull {
						return nil, mysqlError(1231, "42000", fmt.Sprintf("Variable '%s' can't be set to the value of 'NULL'", cleanName))
					}
					isNumeric := false
					if _, isUnary := expr.Expr.(*sqlparser.UnaryExpr); isUnary {
						isNumeric = true
					}
					if lit, isLit := expr.Expr.(*sqlparser.Literal); isLit {
						litStr := strings.TrimSpace(sqlparser.String(lit))
						unquoted := strings.Trim(litStr, "'\"")
						if _, numErr := strconv.ParseFloat(unquoted, 64); numErr == nil {
							isNumeric = true
						}
					}
					if !isNumeric {
						testVal := strings.Trim(val, "'\"")
						if _, numErr := strconv.ParseFloat(testVal, 64); numErr == nil && testVal != "" {
							isNumeric = true
						}
					}
					if isNumeric {
						return nil, mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", cleanName))
					}
					normalized := normalizeEngineName(val)
					knownEngines := map[string]bool{
						"InnoDB": true, "MyISAM": true, "MRG_MYISAM": true, "MEMORY": true,
						"CSV": true, "ARCHIVE": true, "BLACKHOLE": true, "FEDERATED": true,
						"PERFORMANCE_SCHEMA": true, "ndbcluster": true, "TEMPTABLE": true,
					}
					if !knownEngines[normalized] {
						return nil, mysqlError(1286, "42000", fmt.Sprintf("Unknown storage engine '%s'", val))
					}
					if isGlobal {
						e.setGlobalVar(cleanName, normalized)
						if prevVal, had := savedSessionVal[cleanName]; had {
							e.sessionScopeVars[cleanName] = prevVal
						}
						continue
					} else {
						e.sessionScopeVars[cleanName] = normalized
					}
				} else if cleanName == "innodb_ft_aux_table" || cleanName == "innodb_ft_server_stopword_table" || cleanName == "innodb_ft_user_stopword_table" {
					// These variables require either empty string or "schema/table" format.
					evalVal, _ := e.evalExpr(expr.Expr)
					ftVal := strings.TrimSpace(toString(evalVal))
					if ftVal == "" {
						// NULL or empty string clears the variable
						delete(e.sessionScopeVars, cleanName)
					} else {
						// Validate schema/table format: must be "schema/table" with exactly one '/'
						// and non-empty parts on each side.
						slashIdx := strings.Index(ftVal, "/")
						if slashIdx < 0 || slashIdx == 0 || slashIdx == len(ftVal)-1 || strings.Contains(ftVal[slashIdx+1:], "/") {
							// MySQL truncates the value to 200 chars in this error message
							displayVal := ftVal
							if len(displayVal) > 200 {
								displayVal = displayVal[:200]
							}
							return nil, mysqlError(1231, "42000", fmt.Sprintf("Variable '%s' can't be set to the value of '%s'", cleanName, displayVal))
						}
						e.sessionScopeVars[cleanName] = strings.ToLower(ftVal)
					}
				} else {
					// Evaluate expression
					evalVal, err := e.evalExpr(expr.Expr)
					if enumVals, isEnum := sysVarEnumValues[cleanName]; isEnum && len(enumVals) > 0 {
						enumVal, enumErr := normalizeEnumSetValue(cleanName, expr.Expr, evalVal)
						if enumErr != nil {
							return nil, enumErr
						}
						e.sessionScopeVars[cleanName] = enumVal
					} else if isBooleanVariable(cleanName) {
						boolVal, bErr := normalizeBooleanSetValue(cleanName, expr.Expr, evalVal)
						if bErr != nil {
							return nil, bErr
						}
						e.sessionScopeVars[cleanName] = boolVal
					} else if err == nil && evalVal != nil {
						sv := fmt.Sprintf("%v", evalVal)
						if sysVarEnumSet[cleanName] {
							switch strings.ToUpper(sv) {
							case "TRUE":
								sv = "ON"
							case "FALSE":
								sv = "OFF"
							}
						}
						e.sessionScopeVars[cleanName] = sv
					} else if err == nil && evalVal == nil {
						// nil means NULL - store empty or delete
						delete(e.sessionScopeVars, cleanName)
					} else {
						e.sessionScopeVars[cleanName] = val
					}
				}
				// If SET GLOBAL was used, move the newly-stored value to global scope
				// while preserving the previous session value (SET GLOBAL should not
				// affect the current session's variable value).
				if isGlobal {
					if v, ok := e.sessionScopeVars[cleanName]; ok {
						globalVal := v
						// Trigger variables reset immediately after being set
						if triggerSysVars[cleanName] {
							globalVal = "OFF"
						}
						e.setGlobalVar(cleanName, globalVal)
						// When super_read_only is set to ON, also set read_only to ON.
						// (Setting super_read_only OFF does NOT reset read_only.)
						if cleanName == "super_read_only" && (v == "1" || strings.ToUpper(v) == "ON") {
							// Setting super_read_only ON also sets read_only ON.
							e.setGlobalVar("read_only", "1")
						}
						// Setting read_only OFF also sets super_read_only OFF (MySQL behavior).
						if cleanName == "read_only" && (v == "0" || strings.ToUpper(v) == "OFF") {
							e.setGlobalVar("super_read_only", "0")
						}
						// When max_join_size is set globally, update global sql_big_selects.
						if cleanName == "max_join_size" {
							if v == "18446744073709551615" {
								e.setGlobalVar("sql_big_selects", "ON")
							} else {
								e.setGlobalVar("sql_big_selects", "OFF")
							}
						}
						// Restore the previous session value or remove if there wasn't one.
						if prevVal, had := savedSessionVal[cleanName]; had {
							e.sessionScopeVars[cleanName] = prevVal
						} else {
							delete(e.sessionScopeVars, cleanName)
						}
					}
				}
			}
		}
	}
	return &Result{}, nil
}

// resolveSystemVarInValue resolves @@system_var and @user_var references in a value string.
func (e *Executor) resolveSystemVarInValue(val string) string {
	trimVal := strings.TrimSpace(val)
	if strings.HasPrefix(trimVal, "@@") {
		// Resolve system variable
		varName := strings.TrimPrefix(trimVal, "@@")
		varName = strings.TrimPrefix(varName, "SESSION.")
		varName = strings.TrimPrefix(varName, "GLOBAL.")
		varName = strings.TrimPrefix(varName, "session.")
		varName = strings.TrimPrefix(varName, "global.")
		resolved, err := e.evalExpr(&sqlparser.ColName{Name: sqlparser.NewIdentifierCI(varName)})
		if err == nil && resolved != nil {
			return fmt.Sprintf("%v", resolved)
		}
		// Try known variables
		switch strings.ToLower(varName) {
		case "sql_mode":
			return e.sqlMode
		}
	} else if strings.HasPrefix(trimVal, "@") {
		// Resolve user variable
		uvName := strings.TrimPrefix(trimVal, "@")
		if uv, ok := e.userVars[uvName]; ok {
			return fmt.Sprintf("%v", uv)
		}
	}
	return val
}

// handleRawSet handles SET statements that the parser couldn't parse.
func (e *Executor) handleRawSet(raw string) error {
	// Handle user variables: SET @var = value or SET @var := value
	trimmed := strings.TrimSpace(raw)
	upperTrimmed := strings.ToUpper(trimmed)
	if strings.HasPrefix(upperTrimmed, "SET STARTUP ") {
		rest := strings.TrimSpace(trimmed[len("SET STARTUP "):])
		if eqIdx := strings.Index(rest, "="); eqIdx > 0 {
			varName := strings.TrimSpace(strings.ToLower(rest[:eqIdx]))
			val := strings.TrimSpace(rest[eqIdx+1:])
			val = strings.TrimSuffix(val, ";")
			val = strings.TrimSpace(val)
			val = strings.Trim(val, "'\"")
			// For charset/collation options, MySQL accepts comma-separated values
			// but only uses the first value (e.g. --character-set-server=utf16,latin1 -> utf16).
			if varName == "character_set_server" || varName == "collation_server" {
				if commaIdx := strings.Index(val, ","); commaIdx >= 0 {
					val = strings.TrimSpace(val[:commaIdx])
				}
			}
			// SET STARTUP bypasses read-only and scope checks.
			// Normalize boolean variables to ON/OFF.
			if isBooleanVariable(varName) {
				upperVal := strings.ToUpper(val)
				if upperVal == "1" || upperVal == "ON" || upperVal == "TRUE" || upperVal == "YES" {
					val = "ON"
				} else if upperVal == "0" || upperVal == "OFF" || upperVal == "FALSE" || upperVal == "NO" {
					val = "OFF"
				}
			}
			// performance_schema_instrument patterns are accumulated, not overwritten.
			// Each call to SET STARTUP performance_schema_instrument='pattern=value'
			// appends to the list stored in __ps_instrument_patterns__.
			if varName == "performance_schema_instrument" {
				existing := e.startupVars["__ps_instrument_patterns__"]
				if existing == "" {
					e.startupVars["__ps_instrument_patterns__"] = val
				} else {
					e.startupVars["__ps_instrument_patterns__"] = existing + "\n" + val
				}
				return nil
			}
			// Store directly in both globalScopeVars and startupVars.
			e.setGlobalVar(varName, val)
			e.startupVars[varName] = val
			// When character_set_server is set and collation_server was not explicitly
			// set in startup options, update collation_server to the default collation
			// for that charset (MySQL behavior).
			if varName == "character_set_server" {
				if _, collationAlreadySet := e.startupVars["collation_server"]; !collationAlreadySet {
					defaultCollation := catalog.DefaultCollationForCharset(val)
					if defaultCollation != "" {
						e.setGlobalVar("collation_server", defaultCollation)
						e.startupVars["collation_server"] = defaultCollation
					}
				}
			}
			// Special handling: --timezone= master.opt option sets the server timezone.
			// POSIX TZ convention (GMT+N = UTC-N) is used, so we store it for Clone()
			// to pick up as the default session timezone.
			if varName == "timezone" || varName == "time_zone" {
				// Parse as POSIX TZ offset (e.g. "GMT+10" = UTC-10, "GMT-3" = UTC+3)
				posixVal := val
				// Convert POSIX "GMT+N" to MySQL "+N:00" format (sign is inverted)
				if strings.HasPrefix(strings.ToUpper(posixVal), "GMT") {
					offsetStr := posixVal[3:] // e.g. "+10" or "-3"
					if len(offsetStr) > 0 && (offsetStr[0] == '+' || offsetStr[0] == '-') {
						// Invert the sign for MySQL convention
						sign := offsetStr[0]
						absStr := offsetStr[1:]
						hours, parseErr := strconv.Atoi(absStr)
						if parseErr == nil {
							mysqlSign := byte('+')
							if sign == '+' {
								mysqlSign = '-'
							}
							posixVal = fmt.Sprintf("%c%02d:00", mysqlSign, hours)
						}
					}
				}
				if tzErr := e.parseTimeZone(posixVal); tzErr == nil {
					e.startupVars["timezone_parsed"] = posixVal
				}
			}
			return nil
		}
	}
	if strings.HasPrefix(strings.ToUpper(trimmed), "SET ") {
		rest := strings.TrimSpace(trimmed[4:])
		if strings.HasPrefix(rest, "@") && !strings.HasPrefix(rest, "@@") {
			// Find = or :=
			eqIdx := strings.Index(rest, ":=")
			if eqIdx < 0 {
				eqIdx = strings.Index(rest, "=")
			} else {
				// For :=, the value starts after :=
				varName := strings.TrimSpace(rest[1:eqIdx])
				val := strings.TrimSpace(rest[eqIdx+2:])
				val = strings.TrimSuffix(val, ";")
				val = strings.TrimSpace(val)
				val = strings.Trim(val, "'\"")
				val = e.resolveSystemVarInValue(val)
				e.userVars[varName] = val
				return nil
			}
			if eqIdx > 0 {
				varName := strings.TrimSpace(rest[1:eqIdx])
				val := strings.TrimSpace(rest[eqIdx+1:])
				val = strings.TrimSuffix(val, ";")
				val = strings.TrimSpace(val)
				val = strings.Trim(val, "'\"")
				val = e.resolveSystemVarInValue(val)
				e.userVars[varName] = val
				return nil
			}
		}
	}
	upper := strings.ToUpper(raw)
	if strings.Contains(upper, "SQL_MODE") {
		if idx := strings.Index(upper, "="); idx >= 0 {
			val := strings.TrimSpace(raw[idx+1:])
			val = strings.Trim(val, "'\"")
			val = strings.TrimSuffix(val, ";")
			val = strings.TrimSpace(val)
			// Resolve @user_var and @@system_var references
			val = e.resolveSystemVarInValue(val)
			var modeVal string
			if strings.ToUpper(val) == "DEFAULT" {
				modeVal = defaultSQLMode
			} else {
				modeVal = strings.ToUpper(val)
			}
			isGlobal := strings.Contains(upper, "GLOBAL")
			if isGlobal {
				e.setGlobalVar("sql_mode", modeVal)
			} else {
				e.sqlMode = modeVal
				e.sessionScopeVars["sql_mode"] = modeVal
			}
			// Emit warning 3135 for deprecated sensitive modes (same as execSet path)
			if modeVal != defaultSQLMode && !strings.Contains(modeVal, "TRADITIONAL") {
				hasSensitiveMode := strings.Contains(modeVal, "NO_ZERO_DATE") ||
					strings.Contains(modeVal, "NO_ZERO_IN_DATE") ||
					strings.Contains(modeVal, "ERROR_FOR_DIVISION_BY_ZERO")
				if hasSensitiveMode {
					e.addWarning("Warning", 3135, "'NO_ZERO_DATE', 'NO_ZERO_IN_DATE' and 'ERROR_FOR_DIVISION_BY_ZERO' sql modes should be used with strict mode. They will be merged with strict mode in a future release.")
				}
			}
		}
	}
	if strings.Contains(upper, "SQL_AUTO_IS_NULL") {
		if idx := strings.Index(upper, "="); idx >= 0 {
			val := strings.TrimSpace(raw[idx+1:])
			val = strings.Trim(val, "'\"")
			val = strings.TrimSuffix(val, ";")
			val = strings.TrimSpace(val)
			e.sqlAutoIsNull = val == "1" || strings.ToUpper(val) == "ON"
			if e.sqlAutoIsNull {
				e.sessionScopeVars["sql_auto_is_null"] = "ON"
			} else {
				e.sessionScopeVars["sql_auto_is_null"] = "OFF"
			}
		}
	}
	if strings.Contains(upper, "TIMESTAMP") && !strings.Contains(upper, "SQL_MODE") {
		if idx := strings.Index(upper, "="); idx >= 0 {
			val := strings.TrimSpace(raw[idx+1:])
			val = strings.Trim(val, "'\"")
			val = strings.TrimSuffix(val, ";")
			val = strings.TrimSpace(val)
			n, err := strconv.ParseFloat(val, 64)
			if err == nil {
				if n == 0 {
					e.fixedTimestamp = nil
				} else {
					t := time.Unix(int64(n), 0)
					if e.timeZone != nil {
						t = t.In(e.timeZone)
					}
					e.fixedTimestamp = &t
				}
			}
		}
	}
	if strings.Contains(upper, "TIME_ZONE") && !strings.Contains(upper, "TIMESTAMP") {
		if idx := strings.Index(upper, "="); idx >= 0 {
			val := strings.TrimSpace(raw[idx+1:])
			val = strings.Trim(val, "'\"")
			val = strings.TrimSuffix(val, ";")
			val = strings.TrimSpace(val)
			_ = e.parseTimeZone(val)
		}
	}
	// Detect invalid SET local.X / SET global.X / SET session.X (without @@)
	{
		restCheck := strings.TrimSpace(trimmed[4:])
		restCheckLower := strings.ToLower(restCheck)
		for _, prefix := range []string{"local.", "global.", "session."} {
			if strings.HasPrefix(restCheckLower, prefix) && !strings.HasPrefix(restCheckLower, "@@") {
				// This is SET local.X = Y which is a syntax error in MySQL
				return mysqlError(1064, "42000", fmt.Sprintf("You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '%s' at line 1", restCheck))
			}
		}
		// Detect SET @@SESSION varname / SET @@GLOBAL varname (space instead of dot)
		for _, prefix := range []string{"@@global ", "@@session ", "@@local "} {
			if strings.HasPrefix(restCheckLower, prefix) {
				nearText := restCheck[len(prefix):]
				return mysqlError(1064, "42000", fmt.Sprintf("You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '%s' at line 1", nearText))
			}
		}
	}
	// Store any SET GLOBAL/SESSION variable generically
	rest := strings.TrimSpace(trimmed[4:])
	restUpper := strings.ToUpper(rest)
	isPersistOnly := strings.HasPrefix(restUpper, "PERSIST_ONLY ")
	isGlobalScope := strings.HasPrefix(restUpper, "GLOBAL ") || strings.HasPrefix(restUpper, "@@GLOBAL.") || strings.HasPrefix(restUpper, "PERSIST ") || isPersistOnly
	// Use case-insensitive prefix stripping for scope keywords
	if strings.HasPrefix(restUpper, "GLOBAL ") {
		rest = rest[len("GLOBAL "):]
	} else if strings.HasPrefix(restUpper, "SESSION ") {
		rest = rest[len("SESSION "):]
	} else if strings.HasPrefix(restUpper, "LOCAL ") {
		rest = rest[len("LOCAL "):]
	} else if strings.HasPrefix(restUpper, "PERSIST_ONLY ") {
		rest = rest[len("PERSIST_ONLY "):]
	} else if strings.HasPrefix(restUpper, "PERSIST ") {
		rest = rest[len("PERSIST "):]
	}
	rest = strings.TrimPrefix(rest, "@@global.")
	rest = strings.TrimPrefix(rest, "@@session.")
	rest = strings.TrimPrefix(rest, "@@local.")
	rest = strings.TrimPrefix(rest, "@@")
	_ = restUpper
	if eqIdx := strings.Index(rest, "="); eqIdx > 0 {
		varName := strings.TrimSpace(strings.ToLower(rest[:eqIdx]))
		// Check read-only
		if sysVarReadOnly[varName] {
			if isPersistOnly {
				return mysqlError(1238, "HY000", fmt.Sprintf("Variable '%s' is a non persistent read only variable", varName))
			}
			return mysqlError(1238, "HY000", fmt.Sprintf("Variable '%s' is a read only variable", varName))
		}
		// Check GLOBAL-only
		if sysVarGlobalOnly[varName] && !sysVarBothScope[varName] && !isGlobalScope {
			return mysqlError(1228, "HY000", fmt.Sprintf("Variable '%s' is a GLOBAL variable and should be set with SET GLOBAL", varName))
		}
		// Check SESSION-only
		if sysVarSessionOnly[varName] && isGlobalScope {
			return mysqlError(1229, "HY000", fmt.Sprintf("Variable '%s' is a SESSION variable and can't be used with SET GLOBAL", varName))
		}
		// Check session-read-only
		if sysVarSessionReadOnly[varName] && !isGlobalScope {
			return mysqlError(1238, "HY000", fmt.Sprintf("SESSION variable '%s' is read-only. Use SET GLOBAL to assign the value", varName))
		}
		// Emit deprecation warning for deprecated variables
		if msg, ok := sysVarDeprecated[varName]; ok {
			e.addWarning("Warning", 1287, msg)
		}
		val := strings.TrimSpace(rest[eqIdx+1:])
		val = strings.TrimSuffix(val, ";")
		val = strings.TrimSpace(val)
		// Reject NULL for string-type system variables
		if strings.ToUpper(val) == "NULL" && sysVarStringType[varName] {
			return mysqlError(1231, "42000", fmt.Sprintf("Variable '%s' can't be set to the value of 'NULL'", varName))
		}
		// If the value is an unquoted SQL reserved keyword, vitess rejected it for good reason.
		// Propagate as syntax error, but only for non-enum variables (enum vars often use reserved words as values).
		if !strings.HasPrefix(val, "'") && !strings.HasPrefix(val, "\"") && sqlReservedKeywords[strings.ToUpper(val)] && !sysVarEnumSet[varName] && !sysVarAcceptIdentifier[varName] {
			return mysqlError(1064, "42000", fmt.Sprintf("You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '%s' at line 1", val))
		}
		val = strings.Trim(val, "'\"")
		// Handle identity/last_insert_id special cases (type checking + update e.lastInsertID)
		if varName == "identity" || varName == "last_insert_id" {
			if strings.ContainsAny(val, ".eE") {
				return mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", varName))
			}
			if n, nerr := strconv.ParseInt(val, 10, 64); nerr == nil {
				if n < 0 {
					e.addWarning("Warning", 1292, fmt.Sprintf("Truncated incorrect %s value: '%s'", varName, val))
					n = 0
				}
				e.lastInsertID = n
			} else {
				return mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", varName))
			}
			return nil
		}
		// Handle insert_id special cases (type checking + update e.nextInsertID)
		if varName == "insert_id" {
			if strings.ContainsAny(val, ".eE") {
				return mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable 'insert_id'"))
			}
			if n, nerr := strconv.ParseInt(val, 10, 64); nerr == nil {
				if n < 0 {
					e.addWarning("Warning", 1292, fmt.Sprintf("Truncated incorrect insert_id value: '%s'", val))
					n = 0
				}
				e.nextInsertID = n
			} else {
				return mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable 'insert_id'"))
			}
			return nil
		}
		// Normalize engine names for storage engine variables
		if varName == "default_storage_engine" || varName == "default_tmp_storage_engine" || varName == "storage_engine" {
			if _, err := strconv.ParseFloat(val, 64); err == nil {
				return mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", varName))
			}
			val = normalizeEngineName(val)
		}
		// Resolve numeric IDs and validate charset/collation variables
		if isCharsetVar(varName) {
			csVal := strings.ToLower(val)
			if strings.ContainsAny(csVal, ".eE") {
				if _, fErr := strconv.ParseFloat(csVal, 64); fErr == nil {
					return mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", varName))
				}
			}
			if id, parseErr := strconv.ParseInt(csVal, 10, 64); parseErr == nil {
				if csName, ok := resolveCharsetID(id); ok {
					val = csName
				} else {
					return mysqlError(1115, "42000", fmt.Sprintf("Unknown character set: '%s'", truncateErrVal(val)))
				}
			} else {
				if csVal == "utf8mb3" {
					val = "utf8"
				}
				if csVal != "default" && !isKnownCharset(csVal) {
					return mysqlError(1115, "42000", fmt.Sprintf("Unknown character set: '%s'", truncateErrVal(val)))
				}
			}
		}
		if isCollationVar(varName) {
			collVal := strings.ToLower(val)
			if strings.ContainsAny(collVal, ".eE") {
				if _, fErr := strconv.ParseFloat(collVal, 64); fErr == nil {
					return mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", varName))
				}
			}
			if id, parseErr := strconv.ParseInt(collVal, 10, 64); parseErr == nil {
				if collName, ok := resolveCollationID(id); ok {
					val = collName
				} else {
					return mysqlError(1273, "HY000", fmt.Sprintf("Unknown collation: '%s'", truncateErrVal(val)))
				}
			} else {
				if strings.HasPrefix(collVal, "utf8mb3_") {
					val = "utf8_" + strings.TrimPrefix(collVal, "utf8mb3_")
				}
				if collVal != "default" && !isKnownCollation(collVal) {
					return mysqlError(1273, "HY000", fmt.Sprintf("Unknown collation: '%s'", truncateErrVal(val)))
				}
			}
		}
		if strings.ToUpper(val) != "DEFAULT" {
			e.setSysVar(varName, val, isGlobalScope)
		} else {
			// For SET GLOBAL var = DEFAULT, use the compiled default when
			// startupVars would otherwise shadow it.
			// Exception: innodb_commit_concurrency uses startup value as DEFAULT.
			if isGlobalScope {
				if varName == "innodb_commit_concurrency" {
					if sv, hasSV := e.startupVars[varName]; hasSV {
						e.setGlobalVar(varName, sv)
					} else {
						e.deleteSysVar(varName, true)
					}
				} else if _, hasStartup := e.startupVars[varName]; hasStartup {
					if compiled, ok := e.getCompiledDefault(varName); ok {
						e.setGlobalVar(varName, compiled)
					} else {
						e.deleteSysVar(varName, true)
					}
				} else {
					e.deleteSysVar(varName, true)
				}
			} else {
				e.deleteSysVar(varName, false)
			}
		}
		// Trigger variables reset to OFF immediately after being set
		if triggerSysVars[varName] {
			e.setSysVar(varName, "OFF", isGlobalScope)
		}
	}
	if strings.HasPrefix(upper, "SET NAMES ") {
		rawVal := strings.TrimSpace(raw[len("SET NAMES "):])
		fields := strings.Fields(rawVal)
		if len(fields) > 0 {
			charset := strings.ToLower(strings.Trim(fields[0], "'\";"))
			if charset != "default" && charset != "binary" && !isKnownCharset(charset) {
				return mysqlError(1115, "42000", fmt.Sprintf("Unknown character set: '%s'", charset))
			}
			if charset == "default" {
				delete(e.sessionScopeVars, "character_set_client")
				delete(e.sessionScopeVars, "character_set_connection")
				delete(e.sessionScopeVars, "character_set_results")
				delete(e.sessionScopeVars, "collation_connection")
			} else {
				e.sessionScopeVars["character_set_client"] = charset
				e.sessionScopeVars["character_set_connection"] = charset
				e.sessionScopeVars["character_set_results"] = charset
			}
		}
	}
	return nil
}

// showVariables handles SHOW [GLOBAL|SESSION] VARIABLES [LIKE '...']
// buildVariablesMap returns the full map of system variable name -> value,
// including any overrides from SET GLOBAL/SESSION.
// sysVarReadOnly contains system variables that cannot be SET.
var sysVarReadOnly = map[string]bool{
	"basedir": true, "character_set_system": true, "character_sets_dir": true,
	"datadir": true, "have_compress": true, "have_dynamic_loading": true,
	"have_geometry": true, "have_openssl": true, "have_profiling": true,
	"have_query_cache": true, "have_rtree_keys": true, "have_ssl": true,
	"have_symlink": true, "have_statement_timeout": true,
	"hostname": true, "innodb_page_size": true, "innodb_read_only": true,
	"innodb_version": true, "large_files_support": true, "large_page_size": true,
	"lc_messages_dir": true, "license": true, "locked_in_memory": true,
	"log_bin": true, "log_bin_basename": true, "log_bin_index": true,
	"lower_case_file_system": true, "lower_case_table_names": true,
	"max_digest_length": true, "mysqlx_port": true, "open_files_limit": true,
	"pid_file": true, "plugin_dir": true, "port": true, "protocol_version": true,
	"server_uuid": true, "skip_networking": true, "socket": true,
	"system_time_zone": true, "thread_stack": true, "tmpdir": true,
	"version": true, "version_comment": true, "version_compile_machine": true,
	"version_compile_os": true, "innodb_buffer_pool_chunk_size": true,
	"innodb_buffer_pool_load_at_startup": true,
	"innodb_data_file_path":              true, "innodb_data_home_dir": true,
	"innodb_dedicated_server": true, "innodb_force_recovery": true,
	"innodb_log_file_size": true, "innodb_temp_data_file_path": true,
	"innodb_undo_directory": true, "init_file": true, "myisam_mmap_size": true, "old": true,
	"performance_schema": true, "skip_show_database": true,
	"skip_external_locking":            true,
	"performance_schema_accounts_size": true, "performance_schema_digests_size": true,
	"performance_schema_error_size":                            true,
	"performance_schema_events_stages_history_size":            true,
	"performance_schema_events_stages_history_long_size":       true,
	"performance_schema_events_statements_history_size":        true,
	"performance_schema_events_statements_history_long_size":   true,
	"performance_schema_events_transactions_history_size":      true,
	"performance_schema_events_transactions_history_long_size": true,
	"performance_schema_events_waits_history_size":             true,
	"performance_schema_events_waits_history_long_size":        true,
	"performance_schema_hosts_size":                            true,
	"performance_schema_max_cond_classes":                      true,
	"performance_schema_max_cond_instances":                    true,
	"performance_schema_max_digest_length":                     true,
	// performance_schema_max_digest_sample_age is settable in MySQL
	"performance_schema_max_file_classes":                  true,
	"performance_schema_max_file_handles":                  true,
	"performance_schema_max_file_instances":                true,
	"performance_schema_max_index_stat":                    true,
	"performance_schema_max_memory_classes":                true,
	"performance_schema_max_metadata_locks":                true,
	"performance_schema_max_mutex_classes":                 true,
	"performance_schema_max_mutex_instances":               true,
	"performance_schema_max_prepared_statements_instances": true,
	"performance_schema_max_program_instances":             true,
	"performance_schema_max_rwlock_classes":                true,
	"performance_schema_max_rwlock_instances":              true,
	"performance_schema_max_socket_classes":                true,
	"performance_schema_max_socket_instances":              true,
	"performance_schema_max_sql_text_length":               true,
	"performance_schema_max_stage_classes":                 true,
	"performance_schema_max_statement_classes":             true,
	"performance_schema_max_statement_stack":               true,
	"performance_schema_max_table_handles":                 true,
	"performance_schema_max_table_instances":               true,
	"performance_schema_max_table_lock_stat":               true,
	"performance_schema_max_thread_classes":                true,
	"performance_schema_max_thread_instances":              true,
	"performance_schema_session_connect_attrs_size":        true,
	"performance_schema_setup_actors_size":                 true,
	"performance_schema_setup_objects_size":                true,
	"performance_schema_users_size":                        true,
	"persisted_globals_load":                               true,
	"large_pages":                                          true,
	"version_compile_zlib":                                 true,
	"innodb_log_files_in_group":                            true, "innodb_log_group_home_dir": true,
	"innodb_page_cleaners": true,
	"thread_handling":      true,
	// server_id_bits is settable (removed from read-only)
	"back_log": true, "bind_address": true, "admin_address": true,
	"admin_port": true, "core_file": true,
	"disabled_storage_engines": true, "ft_max_word_len": true,
	"ft_min_word_len": true, "ft_query_expansion_limit": true,
	"ft_stopword_file": true, "innodb_autoinc_lock_mode": true,
	"innodb_buffer_pool_instances": true, "innodb_doublewrite_dir": true,
	"innodb_doublewrite_files": true, "innodb_doublewrite_pages": true,
	"innodb_adaptive_hash_index_parts": true,
	"innodb_api_disable_rowlock":       true, "innodb_api_enable_binlog": true, "innodb_api_enable_mdl": true,
	"innodb_force_load_corrupted": true, "innodb_ft_cache_size": true,
	"innodb_ft_max_token_size": true, "innodb_ft_min_token_size": true,
	"innodb_ft_sort_pll_degree": true, "innodb_ft_total_cache_size": true,
	"innodb_numa_interleave": true,
	"innodb_open_files":      true, "innodb_purge_threads": true,
	"innodb_read_io_threads":  true,
	"innodb_sort_buffer_size": true, "innodb_sync_array_size": true,
	"innodb_temp_tablespaces_dir": true,
	"innodb_use_native_aio":       true, "innodb_validate_tablespace_paths": true,
	"innodb_write_io_threads": true,
	"ngram_token_size":        true, "secure_file_priv": true,
	"binlog_gtid_simple_recovery": true, "binlog_row_event_max_size": true,
	"binlog_rotate_encryption_master_key_at_startup": true,
	"create_admin_listener_thread":                   true,
	// external_user and proxy_user are session-only (removed from read-only)
	"tls_version": true, "tls_ciphersuites": true,
	"ssl_ca": true, "ssl_capath": true, "ssl_cert": true, "ssl_cipher": true,
	"ssl_key": true, "ssl_crl": true, "ssl_crlpath": true,
	"mysqlx_ssl_ca": true, "mysqlx_ssl_capath": true, "mysqlx_ssl_cert": true,
	"mysqlx_ssl_cipher": true, "mysqlx_ssl_key": true,
	"mysqlx_ssl_crl": true, "mysqlx_ssl_crlpath": true,
	"mysqlx_socket": true, "mysqlx_bind_address": true, "mysqlx_port_open_timeout": true,
	"auto_generate_certs":                          true,
	"sha256_password_auto_generate_rsa_keys":       true,
	"caching_sha2_password_auto_generate_rsa_keys": true,
	"sha256_password_private_key_path":             true,
	"sha256_password_public_key_path":              true,
	"caching_sha2_password_private_key_path":       true,
	"caching_sha2_password_public_key_path":        true,
	"error_count":                                  true,
	"warning_count":                                true,
	"innodb_redo_log_capacity":                     true,
	"innodb_doublewrite":                           true,
	"innodb_rollback_on_timeout":                   true,
	"default_authentication_plugin":                true,
	"disconnect_on_expired_password":               true,
	"gtid_executed":                                true,
	"gtid_owned":                                   true,
	"relay_log":                                    true,
	"relay_log_info_file":                          true,
	"relay_log_recovery":                           true,
	"relay_log_space_limit":                        true,
	"relay_log_basename":                           true,
	"relay_log_index":                              true,
	"slave_skip_errors":                            true,
	"table_open_cache_instances":                   true,
	"report_host":                                  true,
	"report_port":                                  true,
	"report_user":                                  true,
	"report_password":                              true,
	"skip_name_resolve":                            true,
	"innodb_directories":                           true,
	"myisam_recover_options":                       true,
	"lock_order":                                   true,
	"lock_order_debug_loop":                        true,
	"lock_order_debug_missing_arc":                 true,
	"lock_order_debug_missing_key":                 true,
	"lock_order_debug_missing_unlock":              true,
	"lock_order_dependencies":                      true,
	"lock_order_extra_dependencies":                true,
	"lock_order_output_directory":                  true,
	"lock_order_print_txt":                         true,
	"lock_order_trace_loop":                        true,
	"lock_order_trace_missing_arc":                 true,
	"lock_order_trace_missing_key":                 true,
	"lock_order_trace_missing_unlock":              true,
	"log_error":                                    true,
	"innodb_flush_method":                          true,
}

// sysVarGlobalOnly contains system variables that can only be SET at GLOBAL scope.
// Only includes variables where we are 100% certain they are GLOBAL-only in MySQL 8.0.
// superOnlyGlobalVars lists global system variables that require SUPER (or
// SYSTEM_VARIABLES_ADMIN) privilege to set. Non-root connections attempting
// to set these receive ER_SPECIFIC_ACCESS_DENIED_ERROR (1227).
var superOnlyGlobalVars = map[string]bool{
	"read_only":       true,
	"super_read_only": true,
}

var sysVarGlobalOnly = map[string]bool{
	"automatic_sp_privileges":                 true,
	"avoid_temporal_upgrade":                  true,
	"relay_log_purge":                         true,
	"stored_program_cache":                    true,
	"slow_query_log":                          true,
	"slow_query_log_file":                     true,
	"binlog_cache_size":                       true,
	"binlog_error_action":                     true,
	"binlog_expire_logs_seconds":              true,
	"binlog_group_commit_sync_delay":          true,
	"binlog_group_commit_sync_no_delay_count": true,
	"binlog_max_flush_queue_time":             true,
	"binlog_order_commits":                    true,
	"binlog_stmt_cache_size":                  true,
	"check_proxy_users":                       true,
	"concurrent_insert":                       true,
	"connect_timeout":                         true,
	"innodb_undo_tablespaces":                 true,
	// "max_allowed_packet":                    true, // both scope (session + global)
	"max_prepared_stmt_count":                  true,
	"default_authentication_plugin":            true,
	"default_password_lifetime":                true,
	"disconnect_on_expired_password":           true,
	"expire_logs_days":                         true,
	"flush":                                    true,
	"flush_time":                               true,
	"general_log":                              true,
	"general_log_file":                         true,
	"gtid_executed_compression_period":         true,
	"innodb_adaptive_flushing":                 true,
	"innodb_adaptive_flushing_lwm":             true,
	"innodb_adaptive_hash_index":               true,
	"innodb_adaptive_hash_index_parts":         true,
	"innodb_adaptive_max_sleep_delay":          true,
	"innodb_buffer_pool_dump_at_shutdown":      true,
	"innodb_buffer_pool_dump_now":              true,
	"innodb_buffer_pool_dump_pct":              true,
	"innodb_buffer_pool_filename":              true,
	"innodb_buffer_pool_in_core_file":          true,
	"innodb_buffer_pool_load_abort":            true,
	"innodb_buffer_pool_load_now":              true,
	"innodb_buffer_pool_size":                  true,
	"innodb_change_buffer_max_size":            true,
	"innodb_cmp_per_index_enabled":             true,
	"innodb_commit_concurrency":                true,
	"innodb_compression_level":                 true,
	"innodb_compression_failure_threshold_pct": true,
	"innodb_compression_pad_pct_max":           true,
	"innodb_concurrency_tickets":               true,
	"innodb_deadlock_detect":                   true,
	"innodb_disable_sort_file_cache":           true,
	"innodb_fast_shutdown":                     true,
	"innodb_flush_log_at_timeout":              true,
	"innodb_flush_neighbors":                   true,
	"innodb_flush_sync":                        true,
	"innodb_fill_factor":                       true,
	"innodb_flushing_avg_loops":                true,
	"innodb_ft_enable_diag_print":              true,
	"innodb_ft_num_word_optimize":              true,
	"innodb_ft_result_cache_limit":             true,
	"innodb_io_capacity":                       true,
	"innodb_io_capacity_max":                   true,
	"innodb_lru_scan_depth":                    true,
	"innodb_max_dirty_pages_pct":               true,
	"innodb_max_dirty_pages_pct_lwm":           true,
	"innodb_max_purge_lag":                     true,
	"innodb_max_purge_lag_delay":               true,
	"innodb_max_undo_log_size":                 true,
	"innodb_monitor_disable":                   true,
	"innodb_monitor_enable":                    true,
	"innodb_monitor_reset":                     true,
	"innodb_monitor_reset_all":                 true,
	"innodb_old_blocks_pct":                    true,
	"innodb_old_blocks_time":                   true,
	"innodb_online_alter_log_max_size":         true,
	"innodb_optimize_fulltext_only":            true,
	"innodb_print_all_deadlocks":               true,
	"innodb_print_ddl_log":                     true,
	"innodb_purge_batch_size":                  true,
	"innodb_purge_rseg_truncate_frequency":     true,
	"innodb_random_read_ahead":                 true,
	"innodb_read_ahead_threshold":              true,
	"innodb_redo_log_encrypt":                  true,
	"innodb_replication_delay":                 true,
	"innodb_spin_wait_delay":                   true,
	"innodb_spin_wait_pause_multiplier":        true,
	"innodb_stats_auto_recalc":                 true,
	"innodb_stats_include_delete_marked":       true,
	"innodb_stats_on_metadata":                 true,
	"innodb_stats_persistent":                  true,
	"innodb_stats_persistent_sample_pages":     true,
	"innodb_stats_transient_sample_pages":      true,
	"innodb_status_output":                     true,
	"innodb_status_output_locks":               true,
	"innodb_sync_spin_loops":                   true,
	"innodb_thread_concurrency":                true,
	"innodb_thread_sleep_delay":                true,
	"innodb_undo_log_encrypt":                  true,
	"log_error_verbosity":                      true,
	"log_error_suppression_list":               true,
	"log_error_services":                       true,
	"log_output":                               true,
	"log_bin_trust_function_creators":          true,
	"log_throttle_queries_not_using_indexes":   true,
	"max_connections":                          true,
	"max_connect_errors":                       true,
	"max_write_lock_count":                     true,
	"mysql_native_password_proxy_users":        true,
	"sha256_password_proxy_users":              true,
	"mysqlx_connect_timeout":                   true,
	"mysqlx_document_id_unique_prefix":         true,
	"mysqlx_enable_hello_notice":               true,
	"mysqlx_idle_worker_thread_timeout":        true,
	"mysqlx_max_allowed_packet":                true,
	"mysqlx_max_connections":                   true,
	"mysqlx_min_worker_threads":                true,
	"password_history":                         true,
	"password_reuse_interval":                  true,
	"password_require_current":                 true,
	"sync_binlog":                              true,
	"table_open_cache":                         true,
	"table_open_cache_instances":               true,
	"table_definition_cache":                   true,
	"thread_cache_size":                        true,
	"delayed_insert_limit":                     true,
	"delayed_insert_timeout":                   true,
	"delayed_queue_size":                       true,
	"host_cache_size":                          true,
	"lock_order":                               true,
	"lock_order_debug_loop":                    true,
	"lock_order_debug_missing_arc":             true,
	"lock_order_debug_missing_key":             true,
	"lock_order_debug_missing_unlock":          true,
	"lock_order_dependencies":                  true,
	"lock_order_extra_dependencies":            true,
	"lock_order_output_directory":              true,
	"lock_order_print_txt":                     true,
	"lock_order_trace_loop":                    true,
	"lock_order_trace_missing_arc":             true,
	"lock_order_trace_missing_key":             true,
	"lock_order_trace_missing_unlock":          true,
	"init_slave":                               true,
	"master_info_repository":                   true,
	"master_verify_checksum":                   true,
	"max_binlog_cache_size":                    true,
	"max_binlog_size":                          true,
	"max_binlog_stmt_cache_size":               true,
	"max_relay_log_size":                       true,
	"offline_mode":                             true,
	"relay_log_info_repository":                true,
	"report_host":                              true,
	"report_password":                          true,
	"report_port":                              true,
	"report_user":                              true,
	"rpl_read_size":                            true,
	"rpl_stop_slave_timeout":                   true,
	"schema_definition_cache":                  true,
	"slave_allow_batching":                     true,
	"slave_checkpoint_group":                   true,
	"slave_checkpoint_period":                  true,
	"slave_compressed_protocol":                true,
	"slave_max_allowed_packet":                 true,
	"slave_net_timeout":                        true,
	"slave_parallel_type":                      true,
	"slave_parallel_workers":                   true,
	"slave_pending_jobs_size_max":              true,
	"slave_preserve_commit_order":              true,
	"slave_rows_search_algorithms":             true,
	"slave_sql_verify_checksum":                true,
	"slave_transaction_retries":                true,
	"slow_launch_time":                         true,
	"super_read_only":                          true,
	"sync_master_info":                         true,
	"sync_relay_log":                           true,
	"sync_relay_log_info":                      true,
	"table_encryption_privilege_check":         true,
	"tablespace_definition_cache":              true,
	"innodb_redo_log_archive_dirs":             true,
	"innodb_fsync_threshold":                   true,
	"sql_slave_skip_counter":                   true,
	// Additional global-only variables discovered from test failures
	"binlog_checksum":                          true,
	"binlog_row_metadata":                      true,
	"enforce_gtid_consistency":                 true,
	"gtid_executed":                            true,
	"init_file":                                true,
	"innodb_api_bk_commit_interval":            true,
	"innodb_api_trx_level":                     true,
	"innodb_autoextend_increment":              true,
	"innodb_change_buffering":                  true,
	"innodb_file_per_table":                    true,
	"innodb_flush_log_at_trx_commit":           true,
	"innodb_ft_aux_table":                      true,
	"innodb_ft_server_stopword_table":          true,
	"innodb_log_buffer_size":                   true,
	"innodb_log_checksums":                     true,
	"innodb_log_spin_cpu_abs_lwm":              true,
	"innodb_log_spin_cpu_pct_hwm":              true,
	"innodb_log_wait_for_flush_spin_hwm":       true,
	"innodb_log_writer_threads":                true,
	"innodb_log_compressed_pages":              true,
	"innodb_log_write_ahead_size":              true,
	"innodb_rollback_segments":                 true,
	"innodb_stats_method":                      true,
	"innodb_undo_log_truncate":                 true,
	"local_infile":                             true,
	"log_bin_use_v1_row_events":                true,
	"log_slow_extra":                           true,
	"log_timestamps":                           true,
	"myisam_data_pointer_size":                 true,
	"myisam_mmap_size":                         true,
	"myisam_use_mmap":                          true,
	"mysqlx_interactive_timeout":               true,
	"old":                                      true,
	"performance_schema_max_digest_sample_age": true,
	"read_only":                                true,
	"relay_log":                                true,
	"relay_log_info_file":                      true,
	"relay_log_recovery":                       true,
	"relay_log_space_limit":                    true,
	"require_secure_transport":                 true,
	"server_id":                                true,
	"server_id_bits":                           true,
	"slave_skip_errors":                        true,
	"stored_program_definition_cache":          true,
	"temptable_max_ram":                        true,
	"log_queries_not_using_indexes":            true,
	"log_slow_admin_statements":                true,
	"log_slow_slave_statements":                true,
	"log_statements_unsafe_for_binlog":         true,
	"delay_key_write":                          true,
	"ft_boolean_syntax":                        true,
	"init_connect":                             true,
	"innodb_log_file_size":                     true,
	"innodb_log_files_in_group":                true,
	"key_buffer_size":                          true,
	"key_cache_age_threshold":                  true,
	"key_cache_block_size":                     true,
	"key_cache_division_limit":                 true,
	"innodb_flush_method":                      true,
	"disabled_storage_engines":                 true,
}

// sysVarEnumSet contains system variables that are ENUM types where ON/OFF
// should be returned as strings, not converted to 0/1.
var sysVarEnumSet = map[string]bool{
	"delay_key_write":                 true,
	"enforce_gtid_consistency":        true,
	"event_scheduler":                 true,
	"gtid_mode":                       true,
	"myisam_recover_options":          true,
	"session_track_transaction_info":  true,
	"slave_skip_errors":               true,
	"replica_skip_errors":             true,
	"concurrent_insert":               true,
	"completion_type":                 true,
	"binlog_format":                   true,
	"binlog_row_image":                true,
	"binlog_row_metadata":             true,
	"binlog_error_action":             true,
	"binlog_checksum":                 true,
	"innodb_change_buffering":         true,
	"innodb_default_row_format":       true,
	"innodb_checksum_algorithm":       true,
	"innodb_flush_method":             true,
	"innodb_doublewrite":              true,
	"transaction_isolation":           true,
	"tx_isolation":                    true,
	"default_storage_engine":          true,
	"default_tmp_storage_engine":      true,
	"block_encryption_mode":           true,
	"internal_tmp_mem_storage_engine": true,
	"optimizer_switch":                true,
	"optimizer_trace":                 true,
	"optimizer_trace_features":        true,
	"sql_mode":                        true,
	"default_authentication_plugin":   true,
	"innodb_stats_method":             true,
	"myisam_stats_method":             true,
	"lc_messages":                     true,
	"lc_time_names":                   true,
	"log_output":                      true,
	"log_error_services":              true,
	"log_timestamps":                  true,
	"tls_version":                     true,
	"use_secondary_engine":            true,
	"session_track_system_variables":  true,
	"have_compress":                   true,
	"have_dynamic_loading":            true,
	"have_geometry":                   true,
	"have_openssl":                    true,
	"have_profiling":                  true,
	"have_query_cache":                true,
	"have_rtree_keys":                 true,
	"have_ssl":                        true,
	"have_symlink":                    true,
	"have_statement_timeout":          true,
	"secure_file_priv":                true,
	"updatable_views_with_limit":      true,
	"slave_parallel_type":             true,
	"slave_rows_search_algorithms":    true,
	"slave_type_conversions":          true,
	"rbr_exec_mode":                   true,
	"thread_handling":                 true,
	"master_info_repository":          true,
	"relay_log_info_repository":       true,
}

// sysVarCommaSeparatedEnum contains enum variables that accept comma-separated values (SET type).
var sysVarCommaSeparatedEnum = map[string]bool{
	"log_output":                   true,
	"optimizer_switch":             true,
	"optimizer_trace":              true,
	"sql_mode":                     true,
	"tls_version":                  true,
	"slave_rows_search_algorithms": true,
	"slave_type_conversions":       true,
}

// sysVarAcceptIdentifier contains variables that accept unquoted identifiers as string values.
// These are typically innodb_monitor_* variables that accept counter names like "All", "module_cpu", etc.
var sysVarAcceptIdentifier = map[string]bool{
	"innodb_monitor_disable":   true,
	"innodb_monitor_enable":    true,
	"innodb_monitor_reset":     true,
	"innodb_monitor_reset_all": true,
}

// sysVarStringType contains system variables that are string types and reject NULL values.
var sysVarStringType = map[string]bool{
	"log_error_services":         true,
	"log_error_suppression_list": true,
	"sql_mode":                   true,
	"optimizer_switch":           true,
	"optimizer_trace":            true,
	"optimizer_trace_features":   true,
	"log_output":                 true,
	"log_timestamps":             true,
	"tls_version":                true,
	"block_encryption_mode":      true,
	"lc_messages":                true,
	"lc_time_names":              true,
	"character_set_client":       true,
	"character_set_connection":   true,
	// Note: character_set_results is NOT here because MySQL allows NULL
	// (NULL means "don't convert results")
	"character_set_filesystem": true,
	"character_set_server":     true,
	"character_set_database":   true,
	"collation_connection":     true,
	"collation_server":         true,
	"collation_database":       true,
	// Filename/path string variables that reject numeric literals
	"innodb_ft_aux_table":             true,
	"innodb_ft_server_stopword_table": true,
	"innodb_ft_user_stopword_table":   true,
	"general_log_file":                true,
	"slow_query_log_file":             true,
}

// isNumericLiteralExpr returns true if the expression is a numeric literal
// (integer or float), including negative numbers via UnaryExpr.
func isNumericLiteralExpr(expr sqlparser.Expr) bool {
	switch e := expr.(type) {
	case *sqlparser.Literal:
		switch e.Type {
		case sqlparser.IntVal, sqlparser.FloatVal, sqlparser.DecimalVal, sqlparser.HexNum:
			return true
		}
	case *sqlparser.UnaryExpr:
		if e.Operator == sqlparser.UMinusOp || e.Operator == sqlparser.UPlusOp {
			return isNumericLiteralExpr(e.Expr)
		}
	}
	return false
}

// isInvalidStringVarExpr returns true if the expression is invalid for a
// string-type system variable. MySQL rejects numeric literals with ER_WRONG_TYPE_FOR_VAR.
// For filename-type variables, bare identifiers (ColName) are also rejected.
func isInvalidStringVarExpr(varName string, expr sqlparser.Expr) bool {
	if isNumericLiteralExpr(expr) {
		return true
	}
	// Bare identifier like `mytest.log` parses as ColName; only reject for
	// filename-type variables (not for enum-like vars that accept bare identifiers).
	if sysVarFilenameType[varName] {
		if _, isCol := expr.(*sqlparser.ColName); isCol {
			return true
		}
	}
	return false
}

// sysVarFilenameType contains string-type system variables that are file paths/names
// and should reject bare identifiers (ColName) as ER_WRONG_TYPE_FOR_VAR.
var sysVarFilenameType = map[string]bool{
	"general_log_file":    true,
	"slow_query_log_file": true,
}

// sysVarPureStringType contains system variables that are purely string-typed
// (file paths, table names) and reject ALL numeric literals AND bare identifiers
// with ER_WRONG_TYPE_FOR_VAR (error 1232). Unlike enum/charset string vars,
// these do not accept numeric values as indices.
var sysVarPureStringType = map[string]bool{
	"innodb_ft_aux_table":             true,
	"innodb_ft_server_stopword_table": true,
	"innodb_ft_user_stopword_table":   true,
	"general_log_file":                true,
	"slow_query_log_file":             true,
}

// checkIntVarType checks if the expression is a valid integer type for integer-range
// system variables. Returns ER_WRONG_TYPE_FOR_VAR if invalid.
func checkIntVarType(name string, expr sqlparser.Expr) error {
	switch e := expr.(type) {
	case *sqlparser.Literal:
		litStr := strings.TrimSpace(sqlparser.String(e))
		// String literals and float literals are wrong type
		if strings.HasPrefix(litStr, "'") || strings.HasPrefix(litStr, "\"") || strings.ContainsAny(litStr, ".eE") {
			return mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", name))
		}
	case *sqlparser.UnaryExpr:
		return checkIntVarType(name, e.Expr)
	case *sqlparser.ColName:
		// Bare identifier (e.g., SET var = test) is wrong type
		return mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", name))
	}
	return nil
}

// checkFloatVarType checks if the expression is a valid type for float-range
// system variables. Returns ER_WRONG_TYPE_FOR_VAR if invalid.
func checkFloatVarType(name string, expr sqlparser.Expr) error {
	switch e := expr.(type) {
	case *sqlparser.Literal:
		litStr := strings.TrimSpace(sqlparser.String(e))
		// String literals are wrong type
		if strings.HasPrefix(litStr, "'") || strings.HasPrefix(litStr, "\"") {
			return mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", name))
		}
	case *sqlparser.UnaryExpr:
		return checkFloatVarType(name, e.Expr)
	case *sqlparser.ColName:
		return mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", name))
	}
	return nil
}

// truncateErrVal truncates a value to 64 characters for MySQL error messages.
func truncateErrVal(val string) string {
	if len(val) > 64 {
		return val[:64]
	}
	return val
}

// isNonClientCharset returns true for charsets that cannot be used as client charsets.
// These are multi-byte-only charsets that can't represent ASCII in a single byte.
func isNonClientCharset(name string) bool {
	switch strings.ToLower(name) {
	case "ucs2", "utf16", "utf16le", "utf32":
		return true
	}
	return false
}

// isCharsetVar returns true if the variable name is a character_set_* variable.
func isCharsetVar(name string) bool {
	switch name {
	case "character_set_client", "character_set_connection", "character_set_results",
		"character_set_filesystem", "character_set_server", "character_set_database":
		return true
	}
	return false
}

// isCollationVar returns true if the variable name is a collation_* variable.
func isCollationVar(name string) bool {
	switch name {
	case "collation_connection", "collation_server", "collation_database":
		return true
	}
	return false
}

// sysVarEnumValues maps numeric/string assignments to canonical enum names.
var sysVarEnumValues = map[string]map[string]string{
	"binlog_format": {
		"0":         "MIXED",
		"1":         "STATEMENT",
		"2":         "ROW",
		"MIXED":     "MIXED",
		"STATEMENT": "STATEMENT",
		"ROW":       "ROW",
	},
	"binlog_row_image": {
		"0":       "MINIMAL",
		"1":       "NOBLOB",
		"2":       "FULL",
		"MINIMAL": "MINIMAL",
		"NOBLOB":  "NOBLOB",
		"FULL":    "FULL",
	},
	"binlog_row_metadata": {
		"0":       "MINIMAL",
		"1":       "FULL",
		"MINIMAL": "MINIMAL",
		"FULL":    "FULL",
	},
	"binlog_checksum": {
		"0":     "NONE",
		"1":     "CRC32",
		"NONE":  "NONE",
		"CRC32": "CRC32",
	},
	"binlog_error_action": {
		"0":            "IGNORE_ERROR",
		"1":            "ABORT_SERVER",
		"IGNORE_ERROR": "IGNORE_ERROR",
		"ABORT_SERVER": "ABORT_SERVER",
	},
	"innodb_stats_method": {
		"0":             "nulls_equal",
		"1":             "nulls_unequal",
		"2":             "nulls_ignored",
		"NULLS_EQUAL":   "nulls_equal",
		"NULLS_UNEQUAL": "nulls_unequal",
		"NULLS_IGNORED": "nulls_ignored",
	},
	"myisam_stats_method": {
		"0":             "nulls_unequal",
		"1":             "nulls_equal",
		"2":             "nulls_ignored",
		"NULLS_UNEQUAL": "nulls_unequal",
		"NULLS_EQUAL":   "nulls_equal",
		"NULLS_IGNORED": "nulls_ignored",
	},
	"completion_type": {
		"0":        "NO_CHAIN",
		"1":        "CHAIN",
		"2":        "RELEASE",
		"NO_CHAIN": "NO_CHAIN",
		"CHAIN":    "CHAIN",
		"RELEASE":  "RELEASE",
	},
	"concurrent_insert": {
		"0":      "NEVER",
		"1":      "AUTO",
		"2":      "ALWAYS",
		"NEVER":  "NEVER",
		"AUTO":   "AUTO",
		"ALWAYS": "ALWAYS",
	},
	"delay_key_write": {
		"0":   "OFF",
		"1":   "ON",
		"2":   "ALL",
		"OFF": "OFF",
		"ON":  "ON",
		"ALL": "ALL",
	},
	"enforce_gtid_consistency": {
		"0":    "OFF",
		"1":    "ON",
		"2":    "WARN",
		"OFF":  "OFF",
		"ON":   "ON",
		"WARN": "WARN",
	},
	"innodb_change_buffering": {
		"0":       "none",
		"1":       "inserts",
		"2":       "deletes",
		"3":       "changes",
		"4":       "purges",
		"5":       "all",
		"NONE":    "none",
		"INSERTS": "inserts",
		"DELETES": "deletes",
		"CHANGES": "changes",
		"PURGES":  "purges",
		"ALL":     "all",
	},
	"innodb_checksum_algorithm": {
		"0":             "crc32",
		"1":             "strict_crc32",
		"2":             "innodb",
		"3":             "strict_innodb",
		"4":             "none",
		"5":             "strict_none",
		"CRC32":         "crc32",
		"STRICT_CRC32":  "strict_crc32",
		"INNODB":        "innodb",
		"STRICT_INNODB": "strict_innodb",
		"NONE":          "none",
		"STRICT_NONE":   "strict_none",
	},
	"innodb_default_row_format": {
		"0":         "redundant",
		"1":         "compact",
		"2":         "dynamic",
		"REDUNDANT": "redundant",
		"COMPACT":   "compact",
		"DYNAMIC":   "dynamic",
	},
	"innodb_flush_method": {
		"FSYNC":             "fsync",
		"O_DSYNC":           "O_DSYNC",
		"LITTLESYNC":        "littlesync",
		"NOSYNC":            "nosync",
		"O_DIRECT":          "O_DIRECT",
		"O_DIRECT_NO_FSYNC": "O_DIRECT_NO_FSYNC",
	},
	"log_timestamps": {
		"0":      "UTC",
		"1":      "SYSTEM",
		"UTC":    "UTC",
		"SYSTEM": "SYSTEM",
	},
	"log_error_verbosity": {
		"1": "1",
		"2": "2",
		"3": "3",
	},
	"use_secondary_engine": {
		"0":      "OFF",
		"1":      "ON",
		"2":      "FORCED",
		"OFF":    "OFF",
		"ON":     "ON",
		"FORCED": "FORCED",
	},
	"session_track_transaction_info": {
		"0":               "OFF",
		"1":               "STATE",
		"2":               "CHARACTERISTICS",
		"OFF":             "OFF",
		"STATE":           "STATE",
		"CHARACTERISTICS": "CHARACTERISTICS",
	},
	"transaction_isolation": {
		"0":                "READ-UNCOMMITTED",
		"1":                "READ-COMMITTED",
		"2":                "REPEATABLE-READ",
		"3":                "SERIALIZABLE",
		"READ-UNCOMMITTED": "READ-UNCOMMITTED",
		"READ-COMMITTED":   "READ-COMMITTED",
		"REPEATABLE-READ":  "REPEATABLE-READ",
		"SERIALIZABLE":     "SERIALIZABLE",
	},
	"internal_tmp_mem_storage_engine": {
		"TEMPTABLE": "TempTable",
		"MEMORY":    "MEMORY",
	},
	"event_scheduler": {
		"0":        "OFF",
		"1":        "ON",
		"ON":       "ON",
		"OFF":      "OFF",
		"DISABLED": "DISABLED",
	},
	"gtid_mode": {
		"0":              "OFF",
		"1":              "OFF_PERMISSIVE",
		"2":              "ON_PERMISSIVE",
		"3":              "ON",
		"OFF":            "OFF",
		"OFF_PERMISSIVE": "OFF_PERMISSIVE",
		"ON_PERMISSIVE":  "ON_PERMISSIVE",
		"ON":             "ON",
	},
	"innodb_doublewrite": {
		"0":   "OFF",
		"1":   "ON",
		"ON":  "ON",
		"OFF": "OFF",
	},
	"rbr_exec_mode": {
		"STRICT":     "STRICT",
		"IDEMPOTENT": "IDEMPOTENT",
	},
	"log_output": {
		"TABLE": "TABLE",
		"FILE":  "FILE",
		"NONE":  "NONE",
		"1":     "NONE",
		"2":     "FILE",
		"3":     "NONE,FILE",
		"4":     "TABLE",
		"5":     "NONE,TABLE",
		"6":     "FILE,TABLE",
		"7":     "NONE,FILE,TABLE",
	},
	"updatable_views_with_limit": {
		"0":   "NO",
		"1":   "YES",
		"YES": "YES",
		"NO":  "NO",
	},
	"block_encryption_mode": {
		"0":              "aes-128-ecb",
		"1":              "aes-192-ecb",
		"2":              "aes-256-ecb",
		"3":              "aes-128-cbc",
		"4":              "aes-192-cbc",
		"5":              "aes-256-cbc",
		"AES-128-ECB":    "aes-128-ecb",
		"AES-192-ECB":    "aes-192-ecb",
		"AES-256-ECB":    "aes-256-ecb",
		"AES-128-CBC":    "aes-128-cbc",
		"AES-192-CBC":    "aes-192-cbc",
		"AES-256-CBC":    "aes-256-cbc",
		"AES-128-CFB1":   "aes-128-cfb1",
		"AES-192-CFB1":   "aes-192-cfb1",
		"AES-256-CFB1":   "aes-256-cfb1",
		"AES-128-CFB8":   "aes-128-cfb8",
		"AES-192-CFB8":   "aes-192-cfb8",
		"AES-256-CFB8":   "aes-256-cfb8",
		"AES-128-CFB128": "aes-128-cfb128",
		"AES-192-CFB128": "aes-192-cfb128",
		"AES-256-CFB128": "aes-256-cfb128",
		"AES-128-OFB":    "aes-128-ofb",
		"AES-192-OFB":    "aes-192-ofb",
		"AES-256-OFB":    "aes-256-ofb",
	},
}

// sysVarSessionOnly contains system variables that only exist at SESSION scope.
// SELECT @@global.var for these should return error 1238.
var sysVarSessionOnly = map[string]bool{
	"identity":                   true,
	"last_insert_id":             true,
	"insert_id":                  true,
	"pseudo_thread_id":           true,
	"rand_seed1":                 true,
	"rand_seed2":                 true,
	"gtid_next":                  true,
	"pseudo_slave_mode":          true,
	"transaction_allow_batching": true,
	"sql_log_bin":                true,
	"error_count":                true,
	"warning_count":              true,
	"original_commit_timestamp":  true,
	"original_server_version":    true,
	"immediate_server_version":   true,
	"timestamp":                  true,
	"rbr_exec_mode":              true,
}

// sysVarBothScope contains system variables that exist at both SESSION and GLOBAL scope.
// These should not be blocked by sysVarGlobalOnly even if they appear there.
var sysVarBothScope = map[string]bool{
	"gtid_owned":           true,
	"max_allowed_packet":   true,
	"rbr_exec_mode":        true,
	"max_user_connections": true,
	"net_buffer_length":    true,
}

// sysVarSessionReadOnly contains system variables that are read-only at SESSION scope.
// SET SESSION var = val should return error 1238 with "SESSION variable 'var' is read-only. Use SET GLOBAL to assign the value".
var sysVarSessionReadOnly = map[string]bool{
	"net_buffer_length":    true,
	"max_user_connections": true,
	"max_allowed_packet":   true,
}

// sysVarDeprecated maps deprecated system variable names to their deprecation warning message.
// When these variables are read (SELECT @@var), a deprecation warning (code 1287) is emitted.
var sysVarDeprecated = map[string]string{
	"expire_logs_days":                 "'@@expire_logs_days' is deprecated and will be removed in a future release. Please use binlog_expire_logs_seconds instead.",
	"max_delayed_threads":              "'@@max_delayed_threads' is deprecated and will be removed in a future release.",
	"max_insert_delayed_threads":       "'@@max_insert_delayed_threads' is deprecated and will be removed in a future release.",
	"binlog_max_flush_queue_time":      "'@@binlog_max_flush_queue_time' is deprecated and will be removed in a future release.",
	"show_old_temporals":               "'@@show_old_temporals' is deprecated and will be removed in a future release.",
	"avoid_temporal_upgrade":           "'@@avoid_temporal_upgrade' is deprecated and will be removed in a future release.",
	"log_bin_use_v1_row_events":        "'@@log_bin_use_v1_row_events' is deprecated and will be removed in a future release.",
	"log_slow_slave_statements":        "'@@log_slow_slave_statements' is deprecated and will be removed in a future release. Please use log_slow_replica_statements instead.",
	"relay_log_info_file":              "'@@relay_log_info_file' is deprecated and will be removed in a future release.",
	"master_info_repository":           "'@@master_info_repository' is deprecated and will be removed in a future release.",
	"relay_log_info_repository":        "'@@relay_log_info_repository' is deprecated and will be removed in a future release.",
	"master_verify_checksum":           "'@@master_verify_checksum' is deprecated and will be removed in a future release. Please use source_verify_checksum instead.",
	"slave_compressed_protocol":        "'@@slave_compressed_protocol' is deprecated and will be removed in a future release.",
	"log_statements_unsafe_for_binlog": "'@@log_statements_unsafe_for_binlog' is deprecated and will be removed in a future release.",
	"init_slave":                       "'@@init_slave' is deprecated and will be removed in a future release. Please use init_replica instead.",
	"slave_rows_search_algorithms":     "'@@slave_rows_search_algorithms' is deprecated and will be removed in a future release.",
	"slave_type_conversions":           "'@@slave_type_conversions' is deprecated and will be removed in a future release.",
	"slave_allow_batching":             "'@@slave_allow_batching' is deprecated and will be removed in a future release.",
	"slave_checkpoint_group":           "'@@slave_checkpoint_group' is deprecated and will be removed in a future release.",
	"slave_checkpoint_period":          "'@@slave_checkpoint_period' is deprecated and will be removed in a future release.",
	"slave_max_allowed_packet":         "'@@slave_max_allowed_packet' is deprecated and will be removed in a future release.",
	"slave_net_timeout":                "'@@slave_net_timeout' is deprecated and will be removed in a future release.",
	"slave_parallel_type":              "'@@slave_parallel_type' is deprecated and will be removed in a future release.",
	"slave_parallel_workers":           "'@@slave_parallel_workers' is deprecated and will be removed in a future release.",
	"slave_pending_jobs_size_max":      "'@@slave_pending_jobs_size_max' is deprecated and will be removed in a future release.",
	"slave_preserve_commit_order":      "'@@slave_preserve_commit_order' is deprecated and will be removed in a future release.",
	"slave_sql_verify_checksum":        "'@@slave_sql_verify_checksum' is deprecated and will be removed in a future release.",
	"slave_transaction_retries":        "'@@slave_transaction_retries' is deprecated and will be removed in a future release.",
	"slave_skip_errors":                "'@@slave_skip_errors' is deprecated and will be removed in a future release.",
	"slave_exec_mode":                  "'@@slave_exec_mode' is deprecated and will be removed in a future release.",
	"rpl_stop_slave_timeout":           "'@@rpl_stop_slave_timeout' is deprecated and will be removed in a future release. Please use rpl_stop_replica_timeout instead.",
	"sync_master_info":                 "'@@sync_master_info' is deprecated and will be removed in a future release. Please use sync_source_info instead.",
	"sql_slave_skip_counter":           "'@@sql_slave_skip_counter' is deprecated and will be removed in a future release. Please use sql_replica_skip_counter instead.",
}

// sysVarBoolean contains system variables that are boolean type (ON/OFF).
// For these, float/scientific notation values are rejected with "Incorrect argument type".
var sysVarBoolean = map[string]bool{
	"innodb_adaptive_flushing": true, "innodb_adaptive_hash_index": true,
	"innodb_disable_sort_file_cache": true, "innodb_flush_sync": true,
	"innodb_ft_enable_diag_print": true, "innodb_ft_enable_stopword": true,
	"innodb_optimize_fulltext_only": true, "innodb_print_all_deadlocks": true,
	"innodb_print_ddl_log": true, "innodb_random_read_ahead": true,
	"innodb_redo_log_encrypt": true, "innodb_stats_include_delete_marked": true,
	"innodb_stats_on_metadata": true, "innodb_status_output": true,
	"innodb_status_output_locks": true, "innodb_strict_mode": true,
	"innodb_table_locks": true, "innodb_undo_log_encrypt": true,
	"innodb_cmp_per_index_enabled": true, "innodb_file_per_table": true,
	"innodb_stats_persistent": true, "innodb_stats_auto_recalc": true,
	"innodb_rollback_on_timeout": true, "innodb_deadlock_detect": true,
	"innodb_buffer_pool_dump_at_shutdown": true, "innodb_buffer_pool_dump_now": true,
	"innodb_buffer_pool_in_core_file": true, "innodb_buffer_pool_load_abort": true,
	"innodb_buffer_pool_load_now": true, "innodb_log_checksums": true,
	"innodb_log_compressed_pages": true, "innodb_log_writer_threads": true,
	"big_tables": true, "foreign_key_checks": true, "unique_checks": true,
	"sql_auto_is_null": true, "sql_big_selects": true, "sql_buffer_result": true,
	"sql_log_bin": true, "sql_notes": true, "sql_quote_show_create": true,
	"sql_safe_updates": true, "sql_warnings": true, "autocommit": true,
	"end_markers_in_json": true, "explicit_defaults_for_timestamp": true,
	"general_log": true, "slow_query_log": true, "profiling": true,
	"low_priority_updates": true, "flush": true, "new": true, "old": true,
	"old_alter_table": true, "keep_files_on_create": true,
	"log_queries_not_using_indexes": true, "log_slow_admin_statements": true,
	"log_slow_extra": true, "log_slow_replica_statements": true,
	"log_slow_slave_statements": true, "log_bin_trust_function_creators": true,
	"binlog_order_commits": true, "binlog_rows_query_log_events": true,
	"binlog_direct_non_transactional_updates": true, "binlog_encryption": true,
	"binlog_transaction_compression": true,
	"check_proxy_users":              true, "mysql_native_password_proxy_users": true,
	"sha256_password_proxy_users": true, "require_secure_transport": true,
	"disconnect_on_expired_password": true, "password_require_current": true,
	"avoid_temporal_upgrade": true, "show_create_table_verbosity": true,
	"show_old_temporals": true, "windowing_use_high_precision": true,
	"session_track_schema": true, "session_track_state_change": true,
	"select_into_disk_sync": true, "transaction_read_only": true,
	"read_only": true, "super_read_only": true, "offline_mode": true,
	"relay_log_purge": true, "relay_log_recovery": true,
	"master_verify_checksum": true, "slave_allow_batching": true,
	"slave_compressed_protocol": true, "slave_preserve_commit_order": true,
	"slave_sql_verify_checksum": true, "print_identified_with_as_hex": true,
	"default_table_encryption": true, "table_encryption_privilege_check": true,
	"local_infile": true, "myisam_use_mmap": true,
	"temptable_use_mmap": true, "sql_log_off": true,
	"innodb_doublewrite":         true,
	"automatic_sp_privileges":    true,
	"mysqlx_enable_hello_notice": true,
}

func isBooleanVariable(name string) bool {
	return sysVarBoolean[name]
}

// sysVarBoolAcceptNegative contains boolean variables that accept negative integers
// as ON due to MySQL BUG#50643. Normal boolean variables reject negatives.
var sysVarBoolAcceptNegative = map[string]bool{
	"innodb_adaptive_flushing":            true,
	"innodb_adaptive_hash_index":          true,
	"innodb_disable_sort_file_cache":      true,
	"innodb_flush_sync":                   true,
	"innodb_ft_enable_diag_print":         true,
	"innodb_ft_enable_stopword":           true,
	"innodb_optimize_fulltext_only":       true,
	"innodb_parallel_read_threads":        true,
	"innodb_print_all_deadlocks":          true,
	"innodb_random_read_ahead":            true,
	"innodb_stats_on_metadata":            true,
	"innodb_status_output":                true,
	"innodb_status_output_locks":          true,
	"innodb_strict_mode":                  true,
	"innodb_cmp_per_index_enabled":        true,
	"innodb_redo_log_encrypt":             true,
	"innodb_undo_log_encrypt":             true,
	"innodb_table_locks":                  true,
	"innodb_deadlock_detect":              true,
	"innodb_log_checksums":                true,
	"innodb_log_compressed_pages":         true,
	"innodb_log_writer_threads":           true,
	"innodb_print_ddl_log":                true,
	"innodb_stats_include_delete_marked":  true,
	"innodb_buffer_pool_dump_at_shutdown": true,
	"innodb_buffer_pool_dump_now":         true,
	"innodb_buffer_pool_in_core_file":     true,
	"innodb_buffer_pool_load_abort":       true,
	"innodb_buffer_pool_load_now":         true,
}

// sqlReservedKeywords contains SQL reserved keywords that cannot be used as unquoted
// values in SET statements. When vitess rejects a SET statement with one of these
// as a value, we propagate the syntax error instead of silently accepting it.
var sqlReservedKeywords = map[string]bool{
	"OF": true, "FROM": true, "TO": true, "BY": true, "AS": true,
	"IN": true, "IS": true, "OR": true, "AND": true, "NOT": true,
	"FOR": true, "WITH": true, "SELECT": true, "INSERT": true,
	"UPDATE": true, "DELETE": true, "WHERE": true, "SET": true,
	"TABLE": true, "INTO": true, "VALUES": true, "ORDER": true,
	"GROUP": true, "HAVING": true, "LIMIT": true, "JOIN": true,
	"INNER": true, "OUTER": true, "LEFT": true, "RIGHT": true,
	"CROSS": true, "UNION": true, "ALL": true, "ANY": true,
	"EXISTS": true, "BETWEEN": true, "LIKE": true, "CASE": true,
	"WHEN": true, "THEN": true, "ELSE": true, "END": true,
	"IF": true, "WHILE": true, "DO": true, "REPEAT": true,
	"UNTIL": true, "LOOP": true, "LEAVE": true, "ITERATE": true,
	"RETURN": true, "CALL": true, "DROP": true, "CREATE": true,
	"ALTER": true, "INDEX": true, "PRIMARY": true, "KEY": true,
	"FOREIGN": true, "REFERENCES": true, "CHECK": true,
	"CONSTRAINT": true, "UNIQUE": true, "ADD": true,
	"COLUMN": true, "CHANGE": true, "MODIFY": true, "RENAME": true,
	"EACH": true, "ROW": true, "ROWS": true, "OVER": true,
	"PARTITION": true, "WINDOW": true, "RANGE": true,
	"BOTH": true, "LEADING": true, "TRAILING": true,
	"SIGNAL": true, "RESIGNAL": true, "UNDO": true,
	"USAGE": true, "OPTION": true, "GRANT": true, "REVOKE": true,
}

// triggerSysVars are variables that act as triggers: setting them to ON
// performs an action, and then the variable immediately resets to OFF.
var triggerSysVars = map[string]bool{
	"innodb_buffer_pool_load_abort": true,
	"innodb_buffer_pool_load_now":   true,
	"innodb_buffer_pool_dump_now":   true,
	"innodb_undo_log_encrypt":       true,
	"innodb_redo_log_encrypt":       true,
}

type intVarRange struct {
	Min        int64
	Max        uint64
	IsUnsigned bool
	BlockSize  uint64 // if non-zero, value is rounded down to nearest multiple
}

var sysVarIntRange = map[string]intVarRange{
	"auto_increment_increment":                 {Min: 1, Max: 65535, IsUnsigned: true},
	"auto_increment_offset":                    {Min: 1, Max: 65535, IsUnsigned: true},
	"connect_timeout":                          {Min: 2, Max: 31536000, IsUnsigned: true},
	"default_week_format":                      {Min: 0, Max: 7, IsUnsigned: true},
	"delayed_insert_timeout":                   {Min: 1, Max: 31536000, IsUnsigned: true},
	"div_precision_increment":                  {Min: 0, Max: 30, IsUnsigned: true},
	"innodb_compression_failure_threshold_pct": {Min: 0, Max: 100, IsUnsigned: true},
	"innodb_compression_pad_pct_max":           {Min: 0, Max: 75, IsUnsigned: true},
	"innodb_flush_log_at_timeout":              {Min: 0, Max: 2700, IsUnsigned: true},
	"innodb_flushing_avg_loops":                {Min: 1, Max: 1000, IsUnsigned: true},
	"innodb_max_purge_lag":                     {Min: 0, Max: 4294967295, IsUnsigned: true},
	"innodb_max_purge_lag_delay":               {Min: 0, Max: 4294967295, IsUnsigned: true},
	"innodb_purge_batch_size":                  {Min: 1, Max: 5000, IsUnsigned: true},
	"innodb_purge_rseg_truncate_frequency":     {Min: 1, Max: 128, IsUnsigned: true},
	"innodb_sync_spin_loops":                   {Min: 0, Max: 4294967295, IsUnsigned: true},
	"innodb_thread_concurrency":                {Min: 0, Max: 1000, IsUnsigned: true},
	"lock_wait_timeout":                        {Min: 1, Max: 31536000, IsUnsigned: true},
	"max_connections":                          {Min: 1, Max: 100000, IsUnsigned: true},
	"mysqlx_connect_timeout":                   {Min: 1, Max: 31536000, IsUnsigned: true},
	"mysqlx_max_connections":                   {Min: 1, Max: 100000, IsUnsigned: true},
	"rpl_stop_slave_timeout":                   {Min: 2, Max: 31536000, IsUnsigned: true},
	"sync_binlog":                              {Min: 0, Max: 4294967295, IsUnsigned: true},
	// Additional integer variables with ranges
	"bulk_insert_buffer_size":                    {Min: 0, Max: 18446744073709551615, IsUnsigned: true},
	"binlog_cache_size":                          {Min: 4096, Max: 18446744073709547520, IsUnsigned: true, BlockSize: 4096},
	"binlog_stmt_cache_size":                     {Min: 4096, Max: 18446744073709547520, IsUnsigned: true, BlockSize: 4096},
	"binlog_expire_logs_seconds":                 {Min: 0, Max: 4294967295, IsUnsigned: true},
	"binlog_group_commit_sync_delay":             {Min: 0, Max: 1000000, IsUnsigned: true},
	"binlog_group_commit_sync_no_delay_count":    {Min: 0, Max: 1000000, IsUnsigned: true},
	"binlog_max_flush_queue_time":                {Min: 0, Max: 10000, IsUnsigned: true},
	"binlog_transaction_dependency_history_size": {Min: 1, Max: 1000000, IsUnsigned: true},
	"cte_max_recursion_depth":                    {Min: 0, Max: 4294967295, IsUnsigned: true},
	"default_password_lifetime":                  {Min: 0, Max: 65535, IsUnsigned: true},
	"delayed_insert_limit":                       {Min: 1, Max: 18446744073709551615, IsUnsigned: true},
	"delayed_queue_size":                         {Min: 1, Max: 18446744073709551615, IsUnsigned: true},
	"eq_range_index_dive_limit":                  {Min: 0, Max: 4294967295, IsUnsigned: true},
	"expire_logs_days":                           {Min: 0, Max: 99, IsUnsigned: true},
	"flush_time":                                 {Min: 0, Max: 31536000, IsUnsigned: true},
	"group_concat_max_len":                       {Min: 4, Max: 18446744073709551615, IsUnsigned: true},
	"gtid_executed_compression_period":           {Min: 0, Max: 4294967295, IsUnsigned: true},
	"histogram_generation_max_mem_size":          {Min: 1000000, Max: 18446744073709551615, IsUnsigned: true},
	"host_cache_size":                            {Min: 0, Max: 65536, IsUnsigned: true},
	"information_schema_stats_expiry":            {Min: 0, Max: 31536000, IsUnsigned: true},
	"innodb_adaptive_flushing_lwm":               {Min: 0, Max: 70, IsUnsigned: true},
	"innodb_adaptive_hash_index_parts":           {Min: 1, Max: 512, IsUnsigned: true},
	"innodb_adaptive_max_sleep_delay":            {Min: 0, Max: 1000000, IsUnsigned: true},
	"innodb_buffer_pool_dump_pct":                {Min: 1, Max: 100, IsUnsigned: true},
	"innodb_buffer_pool_size":                    {Min: 5242880, Max: 18446744073709551615, IsUnsigned: true},
	"innodb_change_buffer_max_size":              {Min: 0, Max: 50, IsUnsigned: true},
	"innodb_concurrency_tickets":                 {Min: 1, Max: 4294967295, IsUnsigned: true},
	"innodb_fast_shutdown":                       {Min: 0, Max: 2, IsUnsigned: true},
	"innodb_flush_neighbors":                     {Min: 0, Max: 2, IsUnsigned: true},
	// innodb_max_dirty_pages_pct_lwm is a DOUBLE variable; handled in sysVarFloatRange.
	"innodb_max_undo_log_size":               {Min: 10485760, Max: 18446744073709551615, IsUnsigned: true},
	"innodb_online_alter_log_max_size":       {Min: 65536, Max: 18446744073709551615, IsUnsigned: true},
	"join_buffer_size":                       {Min: 128, Max: 18446744073709551615, IsUnsigned: true, BlockSize: 128},
	"key_buffer_size":                        {Min: 0, Max: 18446744073709551615, IsUnsigned: true},
	"key_cache_age_threshold":                {Min: 100, Max: 18446744073709551615, IsUnsigned: true, BlockSize: 100},
	"key_cache_block_size":                   {Min: 512, Max: 16384, IsUnsigned: true, BlockSize: 512},
	"key_cache_division_limit":               {Min: 1, Max: 100, IsUnsigned: true},
	"log_error_verbosity":                    {Min: 1, Max: 3, IsUnsigned: true},
	"log_throttle_queries_not_using_indexes": {Min: 0, Max: 4294967295, IsUnsigned: true},
	// Note: long_query_time is a DOUBLE variable; handled separately (not in int range map).
	"max_binlog_cache_size":                    {Min: 4096, Max: 18446744073709547520, IsUnsigned: true, BlockSize: 4096},
	"max_binlog_size":                          {Min: 4096, Max: 1073741824, IsUnsigned: true, BlockSize: 4096},
	"max_binlog_stmt_cache_size":               {Min: 4096, Max: 18446744073709547520, IsUnsigned: true, BlockSize: 4096},
	"max_connect_errors":                       {Min: 1, Max: 18446744073709551615, IsUnsigned: true},
	"max_delayed_threads":                      {Min: 0, Max: 16384, IsUnsigned: true},
	"max_error_count":                          {Min: 0, Max: 65535, IsUnsigned: true},
	"max_execution_time":                       {Min: 0, Max: 4294967295, IsUnsigned: true},
	"max_heap_table_size":                      {Min: 16384, Max: 18446744073709550592, IsUnsigned: true, BlockSize: 1024},
	"max_insert_delayed_threads":               {Min: 0, Max: 16384, IsUnsigned: true},
	"max_join_size":                            {Min: 1, Max: 18446744073709551615, IsUnsigned: true},
	"max_length_for_sort_data":                 {Min: 4, Max: 8388608, IsUnsigned: true},
	"max_points_in_geometry":                   {Min: 3, Max: 1048576, IsUnsigned: true},
	"max_prepared_stmt_count":                  {Min: 0, Max: 1048576, IsUnsigned: true},
	"max_relay_log_size":                       {Min: 0, Max: 1073741824, IsUnsigned: true, BlockSize: 4096},
	"max_seeks_for_key":                        {Min: 1, Max: 18446744073709551615, IsUnsigned: true},
	"max_sort_length":                          {Min: 4, Max: 8388608, IsUnsigned: true},
	"max_sp_recursion_depth":                   {Min: 0, Max: 255, IsUnsigned: true},
	"max_user_connections":                     {Min: 0, Max: 4294967295, IsUnsigned: true},
	"max_write_lock_count":                     {Min: 1, Max: 18446744073709551615, IsUnsigned: true},
	"min_examined_row_limit":                   {Min: 0, Max: 18446744073709551615, IsUnsigned: true},
	"myisam_max_sort_file_size":                {Min: 0, Max: 18446744073709551615, IsUnsigned: true},
	"myisam_repair_threads":                    {Min: 1, Max: 18446744073709551615, IsUnsigned: true},
	"myisam_sort_buffer_size":                  {Min: 4096, Max: 18446744073709551615, IsUnsigned: true},
	"mysqlx_document_id_unique_prefix":         {Min: 0, Max: 65535, IsUnsigned: true},
	"mysqlx_idle_worker_thread_timeout":        {Min: 0, Max: 3600, IsUnsigned: true},
	"mysqlx_max_allowed_packet":                {Min: 512, Max: 1073741824, IsUnsigned: true},
	"mysqlx_min_worker_threads":                {Min: 1, Max: 100, IsUnsigned: true},
	"mysqlx_write_timeout":                     {Min: 1, Max: 2147483, IsUnsigned: true},
	"mysqlx_read_timeout":                      {Min: 1, Max: 2147483, IsUnsigned: true},
	"mysqlx_wait_timeout":                      {Min: 1, Max: 2147483, IsUnsigned: true},
	"mysqlx_interactive_timeout":               {Min: 1, Max: 2147483, IsUnsigned: true},
	"net_buffer_length":                        {Min: 1024, Max: 1048576, IsUnsigned: true, BlockSize: 1024},
	"net_read_timeout":                         {Min: 1, Max: 31536000, IsUnsigned: true},
	"net_retry_count":                          {Min: 1, Max: 18446744073709551615, IsUnsigned: true},
	"net_write_timeout":                        {Min: 1, Max: 31536000, IsUnsigned: true},
	"ngram_token_size":                         {Min: 1, Max: 10, IsUnsigned: true},
	"optimizer_prune_level":                    {Min: 0, Max: 1, IsUnsigned: true},
	"optimizer_search_depth":                   {Min: 0, Max: 62, IsUnsigned: true},
	"optimizer_trace_limit":                    {Min: 0, Max: 2147483647, IsUnsigned: false},
	"optimizer_trace_max_mem_size":             {Min: 0, Max: 4294967295, IsUnsigned: true},
	"optimizer_trace_offset":                   {Min: -2147483648, Max: 2147483647, IsUnsigned: false},
	"parser_max_mem_size":                      {Min: 10000000, Max: 18446744073709551615, IsUnsigned: true},
	"password_history":                         {Min: 0, Max: 4294967295, IsUnsigned: true},
	"password_reuse_interval":                  {Min: 0, Max: 4294967295, IsUnsigned: true},
	"preload_buffer_size":                      {Min: 1024, Max: 1073741824, IsUnsigned: true},
	"profiling_history_size":                   {Min: 0, Max: 100, IsUnsigned: true},
	"query_alloc_block_size":                   {Min: 1024, Max: 4294967295, IsUnsigned: true, BlockSize: 1024},
	"query_prealloc_size":                      {Min: 8192, Max: 18446744073709551615, IsUnsigned: true, BlockSize: 1024},
	"range_alloc_block_size":                   {Min: 4096, Max: 4294966272, IsUnsigned: true, BlockSize: 1024},
	"range_optimizer_max_mem_size":             {Min: 0, Max: 18446744073709551615, IsUnsigned: true},
	"read_buffer_size":                         {Min: 8192, Max: 2147483647, IsUnsigned: true, BlockSize: 4096},
	"read_rnd_buffer_size":                     {Min: 1, Max: 2147483647, IsUnsigned: true},
	"regexp_stack_limit":                       {Min: 0, Max: 2147483647, IsUnsigned: true},
	"regexp_time_limit":                        {Min: 0, Max: 2147483647, IsUnsigned: true},
	"rpl_read_size":                            {Min: 8192, Max: 4294967295, IsUnsigned: true},
	"schema_definition_cache":                  {Min: 256, Max: 524288, IsUnsigned: true},
	"select_into_buffer_size":                  {Min: 8192, Max: 2147483647, IsUnsigned: true},
	"select_into_disk_sync_delay":              {Min: 0, Max: 31536000, IsUnsigned: true},
	"slave_checkpoint_group":                   {Min: 32, Max: 524280, IsUnsigned: true},
	"slave_checkpoint_period":                  {Min: 1, Max: 4294967295, IsUnsigned: true},
	"slave_max_allowed_packet":                 {Min: 1024, Max: 1073741824, IsUnsigned: true, BlockSize: 1024},
	"slave_net_timeout":                        {Min: 1, Max: 31536000, IsUnsigned: true},
	"slave_parallel_workers":                   {Min: 0, Max: 1024, IsUnsigned: true},
	"slave_pending_jobs_size_max":              {Min: 1024, Max: 18446744073709551615, IsUnsigned: true, BlockSize: 1024},
	"slave_transaction_retries":                {Min: 0, Max: 18446744073709551615, IsUnsigned: true},
	"slow_launch_time":                         {Min: 0, Max: 31536000, IsUnsigned: true},
	"sort_buffer_size":                         {Min: 32768, Max: 18446744073709551615, IsUnsigned: true},
	"sql_select_limit":                         {Min: 0, Max: 18446744073709551615, IsUnsigned: true},
	"stored_program_cache":                     {Min: 16, Max: 524288, IsUnsigned: true},
	"stored_program_definition_cache":          {Min: 256, Max: 524288, IsUnsigned: true},
	"sync_master_info":                         {Min: 0, Max: 4294967295, IsUnsigned: true},
	"sync_relay_log":                           {Min: 0, Max: 4294967295, IsUnsigned: true},
	"sync_relay_log_info":                      {Min: 0, Max: 4294967295, IsUnsigned: true},
	"table_definition_cache":                   {Min: 400, Max: 524288, IsUnsigned: true},
	"table_open_cache":                         {Min: 1, Max: 524288, IsUnsigned: true},
	"table_open_cache_instances":               {Min: 1, Max: 64, IsUnsigned: true},
	"tablespace_definition_cache":              {Min: 256, Max: 524288, IsUnsigned: true},
	"temptable_max_mmap":                       {Min: 0, Max: 18446744073709551615, IsUnsigned: true},
	"temptable_max_ram":                        {Min: 2097152, Max: 18446744073709551615, IsUnsigned: true},
	"thread_cache_size":                        {Min: 0, Max: 16384, IsUnsigned: true},
	"tmp_table_size":                           {Min: 1024, Max: 18446744073709551615, IsUnsigned: true},
	"transaction_alloc_block_size":             {Min: 1024, Max: 131072, IsUnsigned: true, BlockSize: 1024},
	"transaction_prealloc_size":                {Min: 1024, Max: 131072, IsUnsigned: true, BlockSize: 1024},
	"wait_timeout":                             {Min: 1, Max: 31536000, IsUnsigned: true},
	"interactive_timeout":                      {Min: 1, Max: 31536000, IsUnsigned: true},
	"innodb_stats_transient_sample_pages":      {Min: 1, Max: 18446744073709551615, IsUnsigned: true},
	"innodb_stats_persistent_sample_pages":     {Min: 1, Max: 18446744073709551615, IsUnsigned: true},
	"max_allowed_packet":                       {Min: 1024, Max: 1073741824, IsUnsigned: true, BlockSize: 1024},
	"performance_schema_max_digest_sample_age": {Min: 0, Max: 1048576, IsUnsigned: true},
	"server_id":                                {Min: 0, Max: 4294967295, IsUnsigned: true},
	"innodb_api_bk_commit_interval":            {Min: 1, Max: 1073741824, IsUnsigned: true},
	"innodb_lock_wait_timeout":                 {Min: 1, Max: 1073741824, IsUnsigned: true},
	"innodb_api_trx_level":                     {Min: 0, Max: 3, IsUnsigned: true},
	"innodb_autoextend_increment":              {Min: 1, Max: 1000, IsUnsigned: true},
	"innodb_flush_log_at_trx_commit":           {Min: 0, Max: 2, IsUnsigned: true},
	"innodb_log_write_ahead_size":              {Min: 512, Max: 16384, IsUnsigned: true},
	"innodb_rollback_segments":                 {Min: 1, Max: 128, IsUnsigned: true},
	// innodb_max_dirty_pages_pct is a DOUBLE variable; handled in sysVarFloatRange.
	"myisam_data_pointer_size":           {Min: 2, Max: 7, IsUnsigned: true},
	"myisam_mmap_size":                   {Min: 7, Max: 18446744073709551615, IsUnsigned: true},
	"relay_log_space_limit":              {Min: 0, Max: 18446744073709551615, IsUnsigned: true},
	"innodb_undo_tablespaces":            {Min: 2, Max: 127, IsUnsigned: true},
	"sql_slave_skip_counter":             {Min: 0, Max: 4294967295, IsUnsigned: true},
	"innodb_lru_scan_depth":              {Min: 100, Max: 18446744073709551615, IsUnsigned: true},
	"innodb_old_blocks_pct":              {Min: 5, Max: 95, IsUnsigned: true},
	"innodb_old_blocks_time":             {Min: 0, Max: 18446744073709551615, IsUnsigned: true},
	"innodb_spin_wait_delay":             {Min: 0, Max: 1000, IsUnsigned: true},
	"innodb_spin_wait_pause_multiplier":  {Min: 0, Max: 100, IsUnsigned: true},
	"innodb_thread_sleep_delay":          {Min: 0, Max: 1000000, IsUnsigned: true},
	"innodb_read_ahead_threshold":        {Min: 0, Max: 64, IsUnsigned: true},
	"innodb_compression_level":           {Min: 0, Max: 9, IsUnsigned: true},
	"innodb_ft_num_word_optimize":        {Min: 1000, Max: 18446744073709551615, IsUnsigned: true},
	"innodb_ft_result_cache_limit":       {Min: 1000000, Max: 4294967295, IsUnsigned: true},
	"innodb_fill_factor":                 {Min: 10, Max: 100, IsUnsigned: true},
	"innodb_replication_delay":           {Min: 0, Max: 18446744073709551615, IsUnsigned: true},
	"innodb_log_buffer_size":             {Min: 262144, Max: 4294967295, IsUnsigned: true},
	"innodb_log_spin_cpu_abs_lwm":        {Min: 0, Max: 4294967295, IsUnsigned: true},
	"innodb_log_spin_cpu_pct_hwm":        {Min: 0, Max: 100, IsUnsigned: true},
	"innodb_log_wait_for_flush_spin_hwm": {Min: 0, Max: 18446744073709551615, IsUnsigned: true},
	"innodb_parallel_read_threads":       {Min: 1, Max: 256, IsUnsigned: true},
	"innodb_sort_buffer_size":            {Min: 65536, Max: 67108864, IsUnsigned: true},
	// innodb_commit_concurrency: special behavior in MySQL (cannot change from 0 to non-zero)
	// Not adding to generic range check to avoid regression with innodb_bug42101
	"server_id_bits":            {Min: 0, Max: 32, IsUnsigned: true},
	"pseudo_thread_id":          {Min: 0, Max: 18446744073709551615, IsUnsigned: true},
	"rand_seed1":                {Min: 0, Max: 18446744073709551615, IsUnsigned: true},
	"rand_seed2":                {Min: 0, Max: 18446744073709551615, IsUnsigned: true},
	"original_commit_timestamp": {Min: 0, Max: 18446744073709551615, IsUnsigned: true},
	"original_server_version":   {Min: 0, Max: 18446744073709551615, IsUnsigned: true},
	"immediate_server_version":  {Min: 0, Max: 18446744073709551615, IsUnsigned: true},
}

func parseStrictIntegerAssignment(expr sqlparser.Expr, evalVal interface{}) (int64, uint64, bool, string, error) {
	if lit, isLit := expr.(*sqlparser.Literal); isLit {
		litStr := strings.TrimSpace(sqlparser.String(lit))
		raw := strings.Trim(litStr, "'\"")
		if strings.HasPrefix(litStr, "'") || strings.HasPrefix(litStr, "\"") || strings.ContainsAny(litStr, ".eE") {
			return 0, 0, false, raw, errors.New("non-integer literal")
		}
		if strings.HasPrefix(litStr, "-") {
			n, err := strconv.ParseInt(litStr, 10, 64)
			if err != nil {
				return 0, 0, false, raw, errors.New("invalid integer")
			}
			return n, 0, true, raw, nil
		}
		u, err := strconv.ParseUint(litStr, 10, 64)
		if err != nil {
			return 0, 0, false, raw, errors.New("invalid integer")
		}
		return 0, u, false, raw, nil
	}
	switch v := evalVal.(type) {
	case int:
		n := int64(v)
		return n, uint64(n), n < 0, strconv.FormatInt(n, 10), nil
	case int8:
		n := int64(v)
		return n, uint64(n), n < 0, strconv.FormatInt(n, 10), nil
	case int16:
		n := int64(v)
		return n, uint64(n), n < 0, strconv.FormatInt(n, 10), nil
	case int32:
		n := int64(v)
		return n, uint64(n), n < 0, strconv.FormatInt(n, 10), nil
	case int64:
		return v, uint64(v), v < 0, strconv.FormatInt(v, 10), nil
	case uint:
		u := uint64(v)
		return int64(u), u, false, strconv.FormatUint(u, 10), nil
	case uint8:
		u := uint64(v)
		return int64(u), u, false, strconv.FormatUint(u, 10), nil
	case uint16:
		u := uint64(v)
		return int64(u), u, false, strconv.FormatUint(u, 10), nil
	case uint32:
		u := uint64(v)
		return int64(u), u, false, strconv.FormatUint(u, 10), nil
	case uint64:
		return int64(v), v, false, strconv.FormatUint(v, 10), nil
	case bool:
		if v {
			return 1, 1, false, "1", nil
		}
		return 0, 0, false, "0", nil
	case string:
		s := strings.TrimSpace(v)
		if s == "" {
			return 0, 0, false, s, errors.New("non-integer string")
		}
		up := strings.ToUpper(s)
		if up == "TRUE" {
			return 1, 1, false, "1", nil
		}
		if up == "FALSE" {
			return 0, 0, false, "0", nil
		}
		if strings.ContainsAny(s, ".eE") {
			return 0, 0, false, s, errors.New("non-integer string")
		}
		if strings.HasPrefix(s, "-") {
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return 0, 0, false, s, errors.New("invalid integer")
			}
			return n, 0, true, s, nil
		}
		u, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return 0, 0, false, s, errors.New("invalid integer")
		}
		return 0, u, false, s, nil
	default:
		return 0, 0, false, fmt.Sprintf("%v", evalVal), errors.New("unsupported type")
	}
}

func (e *Executor) clampIntVar(name string, expr sqlparser.Expr, min int64, max uint64, isUnsigned bool, blockSize ...uint64) (string, error) {
	evalVal, err := e.evalExpr(expr)
	if err != nil {
		evalVal = nil
	}
	n, u, isNegative, raw, parseErr := parseStrictIntegerAssignment(expr, evalVal)
	if parseErr != nil {
		return "", mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", name))
	}
	// Check for --maximum-<varname> startup option that caps the max value.
	// For signed variables, the maximum also sets a symmetric negative minimum
	// (e.g., --maximum-optimizer-trace-offset=50 means range is [-50, 50]).
	var maximumCapMin int64
	var hasMaximumCap bool
	if maxStr, ok := e.startupVars["maximum_"+name]; ok {
		if maxVal, err := strconv.ParseUint(maxStr, 10, 64); err == nil && maxVal < max {
			max = maxVal
			if !isUnsigned {
				maximumCapMin = -int64(maxVal)
				hasMaximumCap = true
			}
		}
	}
	warn := func() {
		e.warnings = append(e.warnings, Warning{
			Level:   "Warning",
			Code:    1292,
			Message: fmt.Sprintf("Truncated incorrect %s value: '%s'", name, raw),
		})
	}
	if isUnsigned {
		var out uint64
		if isNegative {
			out = uint64(min)
			warn()
		} else {
			out = u
		}
		if out < uint64(min) {
			out = uint64(min)
			warn()
		}
		if out > max {
			out = max
			warn()
		}
		// Apply block_size alignment (round down)
		if len(blockSize) > 0 && blockSize[0] > 0 && out > 0 {
			out = (out / blockSize[0]) * blockSize[0]
		}
		return strconv.FormatUint(out, 10), nil
	}
	maxI := int64(max)
	out := n
	if !isNegative {
		if u > math.MaxInt64 {
			out = math.MaxInt64
			warn()
		} else {
			out = int64(u)
		}
	}
	// Apply symmetric negative minimum from --maximum-<varname> startup option
	effectiveMin := min
	if hasMaximumCap && maximumCapMin > min {
		effectiveMin = maximumCapMin
	}
	if out < effectiveMin {
		out = effectiveMin
		warn()
	}
	if out > maxI {
		out = maxI
		warn()
	}
	return strconv.FormatInt(out, 10), nil
}

// floatVarRange describes the valid range for a DOUBLE system variable.
type floatVarRange struct {
	Min float64
	Max float64
}

// sysVarFloatRange contains system variables that are DOUBLE type with min/max bounds.
var sysVarFloatRange = map[string]floatVarRange{
	"innodb_max_dirty_pages_pct":      {Min: 0, Max: 99.999},
	"innodb_max_dirty_pages_pct_lwm":  {Min: 0, Max: 99.999},
	"long_query_time":                 {Min: 0, Max: 31536000},
	"secondary_engine_cost_threshold": {Min: 0, Max: math.MaxFloat64},
}

// clampFloatVar validates and clamps a DOUBLE system variable assignment.
func (e *Executor) clampFloatVar(name string, expr sqlparser.Expr, min, max float64) (string, error) {
	evalVal, err := e.evalExpr(expr)
	if err != nil {
		evalVal = nil
	}
	var f float64
	var raw string
	if lit, isLit := expr.(*sqlparser.Literal); isLit {
		litStr := strings.TrimSpace(sqlparser.String(lit))
		unquoted := strings.Trim(litStr, "'\"")
		// Quoted non-numeric strings are type errors
		isQuoted := strings.HasPrefix(litStr, "'") || strings.HasPrefix(litStr, "\"")
		if isQuoted {
			if _, parseErr := strconv.ParseFloat(unquoted, 64); parseErr != nil {
				return "", mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", name))
			}
		}
		parsed, parseErr := strconv.ParseFloat(unquoted, 64)
		if parseErr != nil {
			return "", mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", name))
		}
		f = parsed
		raw = unquoted
	} else if evalVal != nil {
		switch v := evalVal.(type) {
		case float64:
			f = v
			raw = strconv.FormatFloat(v, 'f', -1, 64)
		case float32:
			f = float64(v)
			raw = strconv.FormatFloat(f, 'f', -1, 64)
		case int64:
			f = float64(v)
			raw = strconv.FormatInt(v, 10)
		case int:
			f = float64(v)
			raw = strconv.Itoa(v)
		case uint64:
			f = float64(v)
			raw = strconv.FormatUint(v, 10)
		case SysVarDouble:
			f = v.Value
			raw = strconv.FormatFloat(v.Value, 'f', -1, 64)
		case ScaledValue:
			f = v.Value
			raw = strconv.FormatFloat(v.Value, 'f', -1, 64)
		case bool:
			if v {
				f = 1
			}
			raw = fmt.Sprintf("%v", v)
		case string:
			parsed, parseErr := strconv.ParseFloat(v, 64)
			if parseErr != nil {
				return "", mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", name))
			}
			f = parsed
			raw = v
		default:
			return "", mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", name))
		}
	} else {
		return "", mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", name))
	}
	if f < min {
		e.addWarning("Warning", 1292, fmt.Sprintf("Truncated incorrect %s value: '%s'", name, raw))
		f = min
	}
	if f > max {
		e.addWarning("Warning", 1292, fmt.Sprintf("Truncated incorrect %s value: '%s'", name, raw))
		f = max
	}
	return strconv.FormatFloat(f, 'f', 6, 64), nil
}

func normalizeBooleanToken(raw string) (int64, bool, bool) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, false, false
	}
	isQuoted := strings.HasPrefix(s, "'") || strings.HasPrefix(s, "\"")
	up := strings.ToUpper(strings.Trim(s, "'\""))
	if isQuoted {
		// Quoted strings: only accept "ON", "OFF", "0", "1"
		switch up {
		case "ON":
			return 1, true, false
		case "OFF":
			return 0, true, false
		case "0":
			return 0, true, false
		case "1":
			return 1, true, false
		}
		return 0, false, false
	}
	switch up {
	case "ON", "TRUE":
		return 1, true, false
	case "OFF", "FALSE":
		return 0, true, false
	}
	if strings.Contains(s, ".") {
		if _, err := strconv.ParseFloat(s, 64); err == nil {
			return 0, false, true
		}
	}
	if strings.ContainsAny(s, "eE") {
		if _, err := strconv.ParseFloat(s, 64); err == nil {
			return 0, false, true
		}
	}
	if isQuoted {
		return 0, false, false
	}
	if strings.HasPrefix(s, "-") {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, false, false
		}
		if n == 0 || n == 1 {
			return n, true, false
		}
		// Negative integers: not normally accepted. BUG#50643 handling is in normalizeBooleanSetValue.
		return n, false, false
	}
	if u, err := strconv.ParseUint(s, 10, 64); err == nil {
		if u == 0 || u == 1 {
			return int64(u), true, false
		}
		// Positive integers > 1 are rejected
		return int64(u), false, false
	}
	return 0, false, false
}

func normalizeEnumSetValue(name string, expr sqlparser.Expr, evalVal interface{}) (string, error) {
	enumMap, ok := sysVarEnumValues[name]
	if !ok {
		return "", fmt.Errorf("unknown enum variable")
	}
	// badPart captures the specific invalid part in comma-separated values
	var badPart string
	normalize := func(s string) (string, bool) {
		up := strings.ToUpper(strings.TrimSpace(strings.Trim(s, "'\"")))
		v, ok := enumMap[up]
		if ok {
			return v, true
		}
		// Support comma-separated values for SET-type enum variables (e.g., log_output = 'FILE,TABLE')
		// Use the unquoted (but not trimmed) version for comma splitting to preserve spaces
		rawUnquoted := strings.Trim(s, "'\"")
		upRaw := strings.ToUpper(rawUnquoted)
		if sysVarCommaSeparatedEnum[name] && strings.Contains(upRaw, ",") {
			rawParts := strings.Split(rawUnquoted, ",")
			upParts := strings.Split(upRaw, ",")
			seen := map[string]bool{}
			normalized := make([]string, 0, len(upParts))
			for i, p := range upParts {
				// For log_output, spaces within parts are NOT trimmed.
				// MySQL rejects " FILE" as invalid.
				if name == "log_output" {
					if p == "" {
						continue
					}
				} else {
					p = strings.TrimSpace(p)
					if p == "" {
						continue
					}
				}
				if mv, mok := enumMap[p]; mok {
					if !seen[mv] {
						seen[mv] = true
						normalized = append(normalized, mv)
					}
				} else {
					// Store the original-case bad part for error messages
					if i < len(rawParts) {
						badPart = rawParts[i]
					} else {
						badPart = p
					}
					return "", false
				}
			}
			if len(normalized) > 0 {
				// Sort in canonical order for log_output: NONE, FILE, TABLE
				if name == "log_output" {
					sort.Slice(normalized, func(i, j int) bool {
						order := map[string]int{"NONE": 0, "FILE": 1, "TABLE": 2}
						return order[normalized[i]] < order[normalized[j]]
					})
				}
				return strings.Join(normalized, ","), true
			}
		}
		return "", false
	}
	if lit, isLit := expr.(*sqlparser.Literal); isLit {
		litStr := strings.TrimSpace(sqlparser.String(lit))
		// First try to normalize the value as an enum (handles quoted strings like 'RELEASE')
		if v, ok := normalize(litStr); ok {
			return v, nil
		}
		// Only reject float/scientific notation for unquoted numeric-looking values
		isQuoted := strings.HasPrefix(litStr, "'") || strings.HasPrefix(litStr, "\"")
		if !isQuoted && strings.ContainsAny(litStr, ".eE") {
			return "", mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", name))
		}
		displayVal := strings.Trim(litStr, "'\"")
		// If we have a specific bad part from comma-separated validation, use it
		if badPart != "" {
			return "", mysqlError(1231, "42000", fmt.Sprintf("Variable '%s' can't be set to the value of '%s'", name, badPart))
		}
		// The parser lowercases unquoted keywords like OFF/ON to 'off'/'on'.
		// MySQL shows them uppercase in error messages, so restore the case
		// for well-known SQL keywords.
		switch strings.ToUpper(displayVal) {
		case "ON", "OFF", "TRUE", "FALSE", "YES", "NO":
			displayVal = strings.ToUpper(displayVal)
		}
		return "", mysqlError(1231, "42000", fmt.Sprintf("Variable '%s' can't be set to the value of '%s'", name, displayVal))
	}
	switch v := evalVal.(type) {
	case float32, float64:
		return "", mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", name))
	case bool:
		// TRUE/FALSE → "1"/"0" for enum lookup
		boolStr := "0"
		if v {
			boolStr = "1"
		}
		if mapped, ok := normalize(boolStr); ok {
			return mapped, nil
		}
		return "", mysqlError(1231, "42000", fmt.Sprintf("Variable '%s' can't be set to the value of '%s'", name, boolStr))
	default:
		if mapped, ok := normalize(fmt.Sprintf("%v", v)); ok {
			return mapped, nil
		}
		valStr := "NULL"
		if v != nil {
			valStr = fmt.Sprintf("%v", v)
		}
		return "", mysqlError(1231, "42000", fmt.Sprintf("Variable '%s' can't be set to the value of '%s'", name, valStr))
	}
}

func normalizeBooleanSetValue(name string, expr sqlparser.Expr, evalVal interface{}) (string, error) {
	acceptNeg := sysVarBoolAcceptNegative[name]
	if lit, isLit := expr.(*sqlparser.Literal); isLit {
		litStr := strings.TrimSpace(sqlparser.String(lit))
		n, ok, typeErr := normalizeBooleanToken(litStr)
		if typeErr {
			return "", mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", name))
		}
		if !ok {
			// Check if it's a negative integer for BUG#50643 variables
			if acceptNeg && strings.HasPrefix(litStr, "-") {
				if _, err := strconv.ParseInt(litStr, 10, 64); err == nil {
					return "1", nil // negative → ON
				}
			}
			unq := strings.Trim(litStr, "'\"")
			return "", mysqlError(1231, "42000", fmt.Sprintf("Variable '%s' can't be set to the value of '%s'", name, unq))
		}
		return strconv.FormatInt(n, 10), nil
	}
	switch v := evalVal.(type) {
	case bool:
		if v {
			return "1", nil
		}
		return "0", nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		n, ok, _ := normalizeBooleanToken(fmt.Sprintf("%v", v))
		if !ok {
			// Check if it's a negative integer for BUG#50643 variables
			if acceptNeg {
				s := fmt.Sprintf("%v", v)
				if strings.HasPrefix(s, "-") {
					return "1", nil // negative → ON
				}
			}
			return "", mysqlError(1231, "42000", fmt.Sprintf("Variable '%s' can't be set to the value of '%v'", name, v))
		}
		return strconv.FormatInt(n, 10), nil
	case float32, float64:
		return "", mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", name))
	case string:
		// When the value comes from a user variable, treat it as a quoted string
		// (only "ON", "OFF", "0", "1" are valid, not "TRUE"/"FALSE").
		checkVal := v
		if _, isVar := expr.(*sqlparser.Variable); isVar {
			checkVal = "'" + v + "'"
		}
		n, ok, typeErr := normalizeBooleanToken(checkVal)
		if typeErr {
			return "", mysqlError(1232, "42000", fmt.Sprintf("Incorrect argument type to variable '%s'", name))
		}
		if !ok {
			if acceptNeg && strings.HasPrefix(v, "-") {
				if _, err := strconv.ParseInt(v, 10, 64); err == nil {
					return "1", nil
				}
			}
			return "", mysqlError(1231, "42000", fmt.Sprintf("Variable '%s' can't be set to the value of '%s'", name, strings.Trim(v, "'\"")))
		}
		return strconv.FormatInt(n, 10), nil
	default:
		valStr := "NULL"
		if evalVal != nil {
			valStr = fmt.Sprintf("%v", evalVal)
		}
		return "", mysqlError(1231, "42000", fmt.Sprintf("Variable '%s' can't be set to the value of '%s'", name, valStr))
	}
}

// sysVarStringToSelectValue converts a system variable string value to the value
// returned by SELECT @@var. Boolean ON/OFF become 1/0, numeric strings become int64/float64.
func sysVarStringToSelectValue(val string) interface{} {
	return sysVarStringToSelectValueForVar(val, "")
}

// sysVarStringToSelectValueForVar converts with variable name awareness.
// For enum-type variables, ON/OFF strings are preserved; for booleans they become 0/1.
func sysVarStringToSelectValueForVar(val string, varName string) interface{} {
	upper := strings.ToUpper(val)
	// For enum variables, don't convert ON/OFF to 0/1
	if !sysVarEnumSet[varName] {
		if upper == "ON" || upper == "YES" || upper == "TRUE" {
			return int64(1)
		}
		if upper == "OFF" || upper == "NO" || upper == "FALSE" {
			return int64(0)
		}
	}
	// For DOUBLE system variables, return SysVarDouble so the display
	// layer formats with 6 fixed decimal places (e.g. "90.000000"),
	// while arithmetic still works (toFloat extracts the float64).
	if _, isFlt := sysVarFloatRange[varName]; isFlt {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return SysVarDouble{Value: f}
		}
	}
	if n, err := strconv.ParseInt(val, 10, 64); err == nil {
		return n
	}
	// Try unsigned int before float to preserve precision for large values like max uint64
	if u, err := strconv.ParseUint(val, 10, 64); err == nil {
		return u
	}
	if f, err := strconv.ParseFloat(val, 64); err == nil {
		return f
	}
	return val
}

func (e *Executor) buildVariablesMap() map[string]string {
	return e.buildVariablesMapScoped(false)
}

func (e *Executor) buildVariablesMapScoped(globalOnly bool) map[string]string {
	vars := map[string]string{
		// InnoDB variables
		"innodb_rollback_on_timeout":               "ON",
		"innodb_file_per_table":                    "ON",
		"innodb_strict_mode":                       "ON",
		"innodb_page_size":                         "16384",
		"innodb_buffer_pool_size":                  "134217728",
		"innodb_default_row_format":                "dynamic",
		"innodb_lock_wait_timeout":                 "50",
		"innodb_autoinc_lock_mode":                 "2",
		"innodb_stats_persistent":                  "ON",
		"innodb_stats_auto_recalc":                 "ON",
		"innodb_stats_persistent_sample_pages":     "20",
		"innodb_stats_transient_sample_pages":      "8",
		"innodb_log_file_size":                     "50331648",
		"innodb_ft_enable_stopword":                "ON",
		"innodb_ft_server_stopword_table":          "",
		"innodb_large_prefix":                      "ON",
		"innodb_fill_factor":                       "100",
		"innodb_sort_buffer_size":                  "1048576",
		"innodb_online_alter_log_max_size":         "134217728",
		"innodb_optimize_fulltext_only":            "OFF",
		"innodb_max_dirty_pages_pct":               "90.000000",
		"innodb_max_dirty_pages_pct_lwm":           "10.000000",
		"innodb_change_buffering":                  "all",
		"innodb_change_buffer_max_size":            "25",
		"innodb_flush_log_at_trx_commit":           "1",
		"innodb_doublewrite":                       "ON",
		"innodb_flush_method":                      "fsync",
		"innodb_tmpdir":                            "",
		"innodb_checksum_algorithm":                "crc32",
		"innodb_ft_max_token_size":                 "84",
		"innodb_ft_min_token_size":                 "3",
		"innodb_compression_level":                 "6",
		"innodb_data_file_path":                    "ibdata1:12M:autoextend",
		"innodb_adaptive_flushing":                 "ON",
		"innodb_adaptive_flushing_lwm":             "10",
		"innodb_adaptive_hash_index":               "ON",
		"innodb_adaptive_hash_index_parts":         "8",
		"innodb_adaptive_max_sleep_delay":          "150000",
		"innodb_api_bk_commit_interval":            "5",
		"innodb_api_disable_rowlock":               "OFF",
		"innodb_api_enable_binlog":                 "OFF",
		"innodb_api_enable_mdl":                    "OFF",
		"innodb_api_trx_level":                     "0",
		"innodb_autoextend_increment":              "64",
		"innodb_buffer_pool_chunk_size":            "134217728",
		"innodb_buffer_pool_dump_at_shutdown":      "ON",
		"innodb_buffer_pool_dump_now":              "OFF",
		"innodb_buffer_pool_dump_pct":              "25",
		"innodb_buffer_pool_filename":              "ib_buffer_pool",
		"innodb_buffer_pool_in_core_file":          "ON",
		"innodb_buffer_pool_instances":             "1",
		"innodb_buffer_pool_load_abort":            "OFF",
		"innodb_buffer_pool_load_at_startup":       "ON",
		"innodb_buffer_pool_load_now":              "OFF",
		"innodb_cmp_per_index_enabled":             "OFF",
		"innodb_commit_concurrency":                "0",
		"innodb_compression_failure_threshold_pct": "5",
		"innodb_compression_pad_pct_max":           "50",
		"innodb_concurrency_tickets":               "5000",
		"innodb_data_home_dir":                     "",
		"innodb_deadlock_detect":                   "ON",
		"innodb_dedicated_server":                  "OFF",
		"innodb_disable_sort_file_cache":           "OFF",
		"innodb_doublewrite_batch_size":            "0",
		"innodb_doublewrite_dir":                   "",
		"innodb_doublewrite_files":                 "2",
		"innodb_doublewrite_pages":                 "4",
		"innodb_fast_shutdown":                     "1",
		"innodb_flush_log_at_timeout":              "1",
		"innodb_flush_neighbors":                   "0",
		"innodb_flush_sync":                        "ON",
		"innodb_flushing_avg_loops":                "30",
		"innodb_force_load_corrupted":              "OFF",
		"innodb_force_recovery":                    "0",
		"innodb_ft_aux_table":                      "",
		"innodb_ft_cache_size":                     "8000000",
		"innodb_ft_enable_diag_print":              "OFF",
		"innodb_ft_num_word_optimize":              "2000",
		"innodb_ft_result_cache_limit":             "2000000000",
		"innodb_ft_sort_pll_degree":                "2",
		"innodb_ft_total_cache_size":               "640000000",
		"innodb_ft_user_stopword_table":            "",
		"innodb_io_capacity":                       "200",
		"innodb_io_capacity_max":                   "2000",
		"innodb_log_buffer_size":                   "16777216",
		"innodb_log_checksums":                     "ON",
		"innodb_log_compressed_pages":              "ON",
		"innodb_log_spin_cpu_abs_lwm":              "80",
		"innodb_log_spin_cpu_pct_hwm":              "50",
		"innodb_log_wait_for_flush_spin_hwm":       "400",
		"innodb_log_write_ahead_size":              "8192",
		"innodb_log_writer_threads":                "ON",
		"innodb_lru_scan_depth":                    "1024",
		"innodb_max_purge_lag":                     "0",
		"innodb_max_purge_lag_delay":               "0",
		"innodb_max_undo_log_size":                 "1073741824",
		"innodb_monitor_disable":                   "",
		"innodb_monitor_enable":                    "",
		"innodb_monitor_reset":                     "",
		"innodb_monitor_reset_all":                 "",
		"innodb_numa_interleave":                   "OFF",
		"innodb_old_blocks_pct":                    "37",
		"innodb_old_blocks_time":                   "1000",
		"innodb_open_files":                        "4000",
		"innodb_parallel_read_threads":             "4",
		"innodb_print_all_deadlocks":               "OFF",
		"innodb_print_ddl_log":                     "OFF",
		"innodb_purge_batch_size":                  "300",
		"innodb_purge_rseg_truncate_frequency":     "128",
		"innodb_purge_threads":                     "4",
		"innodb_random_read_ahead":                 "OFF",
		"innodb_read_ahead_threshold":              "56",
		"innodb_read_io_threads":                   "4",
		"innodb_read_only":                         "OFF",
		"innodb_redo_log_capacity":                 "104857600",
		"innodb_redo_log_encrypt":                  "OFF",
		"innodb_replication_delay":                 "0",
		"innodb_rollback_segments":                 "128",
		"innodb_spin_wait_delay":                   "6",
		"innodb_spin_wait_pause_multiplier":        "50",
		"innodb_stats_include_delete_marked":       "OFF",
		"innodb_stats_method":                      "nulls_equal",
		"innodb_stats_on_metadata":                 "OFF",
		"innodb_status_output":                     "OFF",
		"innodb_status_output_locks":               "OFF",
		"innodb_sync_array_size":                   "1",
		"innodb_sync_spin_loops":                   "30",
		"innodb_table_locks":                       "ON",
		"innodb_temp_data_file_path":               "ibtmp1:12M:autoextend",
		"innodb_temp_tablespaces_dir":              "./#innodb_temp/",
		"innodb_thread_concurrency":                "0",
		"innodb_thread_sleep_delay":                "10000",
		"innodb_undo_directory":                    "./",
		"innodb_undo_log_encrypt":                  "OFF",
		"innodb_undo_log_truncate":                 "ON",
		"innodb_undo_tablespaces":                  "2",
		"innodb_use_native_aio":                    "ON",
		"innodb_validate_tablespace_paths":         "ON",
		"innodb_write_io_threads":                  "4",

		// Auto increment
		"auto_increment_increment": "1",
		"auto_increment_offset":    "1",

		// Character set / collation
		"character_set_server":     "utf8mb4",
		"character_set_database":   "utf8mb4",
		"character_set_client":     "utf8mb4",
		"character_set_connection": "utf8mb4",
		"character_set_results":    "utf8mb4",
		"character_set_filesystem": "binary",
		"character_set_system":     "utf8mb3",
		"character_sets_dir":       "/usr/share/mysql/charsets/",
		"collation_server":         "utf8mb4_0900_ai_ci",
		"collation_database":       "utf8mb4_0900_ai_ci",
		"collation_connection":     "utf8mb4_0900_ai_ci",

		// Server identity
		"version":                 "8.4.0-mylite",
		"version_comment":         "mylite",
		"version_compile_machine": "x86_64",
		"version_compile_os":      "Linux",
		"server_id":               "1",
		"server_uuid":             "00000000-0000-0000-0000-000000000000",
		"hostname":                "localhost",
		"port":                    "3306",
		"socket":                  "/var/run/mysqld/mysqld.sock",
		"pid_file":                "/var/run/mysqld/mysqld.pid",
		"basedir":                 "/usr/",
		"datadir":                 "/var/lib/mysql/",
		"tmpdir":                  "/tmp",
		"plugin_dir":              "/usr/lib/mysql/plugin/",
		"log_error":               "stderr",

		// SQL modes and behavior
		"sql_mode":                   "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
		"lower_case_table_names":     "0",
		"lower_case_file_system":     "OFF",
		"license":                    "GPL",
		"core_file":                  "ON",
		"max_allowed_packet":         "67108864",
		"default_storage_engine":     "InnoDB",
		"default_tmp_storage_engine": "InnoDB",

		// Connection and timeout
		"connect_timeout":      "60",
		"wait_timeout":         "28800",
		"interactive_timeout":  "28800",
		"net_read_timeout":     "30",
		"net_write_timeout":    "60",
		"net_retry_count":      "10",
		"net_buffer_length":    "16384",
		"max_connections":      "151",
		"max_connect_errors":   "100",
		"max_user_connections": "0",
		"back_log":             "151",
		"thread_cache_size":    "9",
		"thread_stack":         "262144",

		// Query and buffer sizes
		"max_heap_table_size":      "16777216",
		"tmp_table_size":           "16777216",
		"sort_buffer_size":         "262144",
		"join_buffer_size":         "262144",
		"read_buffer_size":         "131072",
		"read_rnd_buffer_size":     "262144",
		"bulk_insert_buffer_size":  "8388608",
		"max_sort_length":          "1024",
		"max_length_for_sort_data": "4096",
		"group_concat_max_len":     "1024",
		"preload_buffer_size":      "32768",

		// Logging
		"general_log":                     "OFF",
		"general_log_file":                "localhost.log",
		"slow_query_log":                  "OFF",
		"slow_query_log_file":             "localhost-slow.log",
		"long_query_time":                 "10.000000",
		"log_output":                      "FILE",
		"log_queries_not_using_indexes":   "OFF",
		"log_slow_admin_statements":       "OFF",
		"log_slow_extra":                  "OFF",
		"log_slow_replica_statements":     "OFF",
		"log_slow_slave_statements":       "OFF",
		"log_timestamps":                  "UTC",
		"log_error_verbosity":             "2",
		"log_error_suppression_list":      "",
		"log_error_services":              "log_filter_internal; log_sink_internal",
		"log_bin":                         "OFF",
		"log_bin_basename":                "",
		"log_bin_index":                   "",
		"log_bin_trust_function_creators": "OFF",

		// Binary log
		"binlog_cache_size":                              "32768",
		"binlog_stmt_cache_size":                         "32768",
		"binlog_format":                                  "ROW",
		"binlog_row_image":                               "FULL",
		"binlog_row_metadata":                            "MINIMAL",
		"binlog_rows_query_log_events":                   "OFF",
		"binlog_direct_non_transactional_updates":        "OFF",
		"binlog_order_commits":                           "ON",
		"binlog_error_action":                            "ABORT_SERVER",
		"binlog_expire_logs_seconds":                     "2592000",
		"binlog_group_commit_sync_delay":                 "0",
		"binlog_group_commit_sync_no_delay_count":        "0",
		"binlog_gtid_simple_recovery":                    "ON",
		"binlog_max_flush_queue_time":                    "0",
		"binlog_checksum":                                "CRC32",
		"binlog_encryption":                              "OFF",
		"binlog_rotate_encryption_master_key_at_startup": "OFF",
		"binlog_row_event_max_size":                      "8192",
		"binlog_transaction_compression":                 "OFF",
		"binlog_transaction_compression_level_zstd":      "3",
		"binlog_transaction_dependency_history_size":     "25000",
		"binlog_transaction_dependency_tracking":         "COMMIT_ORDER",

		// GTID
		"gtid_mode":                        "OFF",
		"gtid_executed":                    "",
		"gtid_purged":                      "",
		"gtid_owned":                       "",
		"enforce_gtid_consistency":         "OFF",
		"gtid_executed_compression_period": "1000",
		"gtid_next":                        "AUTOMATIC",

		// Replication
		"relay_log":                "localhost-relay-bin",
		"relay_log_basename":       "MYSQLTEST_VARDIR/mysqld.1/data/localhost-relay-bin",
		"relay_log_index":          "MYSQLTEST_VARDIR/mysqld.1/data/localhost-relay-bin.index",
		"relay_log_info_file":      "relay-log.info",
		"relay_log_purge":          "ON",
		"relay_log_recovery":       "OFF",
		"relay_log_space_limit":    "0",
		"replica_net_timeout":      "60",
		"replica_skip_errors":      "OFF",
		"replica_type_conversions": "",
		"sync_binlog":              "1",

		// Transaction
		"transaction_isolation": "REPEATABLE-READ",
		"transaction_read_only": "OFF",
		"autocommit":            "ON",
		"completion_type":       "NO_CHAIN",

		// Security
		"secure_file_priv":                  "",
		"require_secure_transport":          "OFF",
		"default_authentication_plugin":     "caching_sha2_password",
		"default_password_lifetime":         "0",
		"disconnect_on_expired_password":    "ON",
		"password_history":                  "0",
		"password_reuse_interval":           "0",
		"password_require_current":          "OFF",
		"check_proxy_users":                 "OFF",
		"mysql_native_password_proxy_users": "OFF",
		"sha256_password_proxy_users":       "OFF",
		"automatic_sp_privileges":           "ON",
		"skip_name_resolve":                 "ON",
		"skip_networking":                   "OFF",
		"skip_show_database":                "OFF",
		"local_infile":                      "OFF",

		// Optimizer
		"optimizer_switch":             "index_merge=on,index_merge_union=on,index_merge_sort_union=on,index_merge_intersection=on,engine_condition_pushdown=on,index_condition_pushdown=on,mrr=on,mrr_cost_based=on,block_nested_loop=on,batched_key_access=off,materialization=on,semijoin=on,loosescan=on,firstmatch=on,duplicateweedout=on,subquery_materialization_cost_based=on,use_index_extensions=on,condition_fanout_filter=on,derived_merge=on,use_invisible_indexes=off,skip_scan=on",
		"optimizer_trace":              "enabled=off,one_line=off",
		"optimizer_trace_features":     "greedy_search=on,range_optimizer=on,dynamic_range=on,repeated_subselect=on",
		"optimizer_trace_limit":        "1",
		"optimizer_trace_max_mem_size": "1048576",
		"optimizer_trace_offset":       "-1",
		"optimizer_prune_level":        "1",
		"optimizer_search_depth":       "62",
		"eq_range_index_dive_limit":    "200",
		"range_optimizer_max_mem_size": "8388608",

		// Keys and indexes
		"key_buffer_size":           "8388608",
		"key_cache_age_threshold":   "300",
		"key_cache_block_size":      "1024",
		"key_cache_division_limit":  "100",
		"myisam_sort_buffer_size":   "8388608",
		"myisam_max_sort_file_size": "9223372036853727232",
		"myisam_repair_threads":     "1",
		"myisam_recover_options":    "OFF",
		"myisam_use_mmap":           "OFF",
		"myisam_stats_method":       "nulls_unequal",
		"myisam_data_pointer_size":  "6",

		// Table and file settings
		"table_open_cache":                "4000",
		"table_open_cache_instances":      "16",
		"table_definition_cache":          "2000",
		"open_files_limit":                "1048576",
		"max_tmp_tables":                  "32",
		"max_write_lock_count":            "18446744073709551615",
		"concurrent_insert":               "AUTO",
		"delay_key_write":                 "ON",
		"flush":                           "OFF",
		"flush_time":                      "0",
		"lock_wait_timeout":               "31536000",
		"lock_order":                      "ON",
		"lock_order_debug_loop":           "OFF",
		"lock_order_debug_missing_arc":    "OFF",
		"lock_order_debug_missing_key":    "OFF",
		"lock_order_debug_missing_unlock": "OFF",
		"lock_order_dependencies":         "",
		"lock_order_extra_dependencies":   "",
		"lock_order_output_directory":     "",
		"lock_order_print_txt":            "OFF",
		"lock_order_trace_loop":           "OFF",
		"lock_order_trace_missing_arc":    "OFF",
		"lock_order_trace_missing_key":    "OFF",
		"lock_order_trace_missing_unlock": "OFF",
		"low_priority_updates":            "OFF",
		"max_delayed_threads":             "20",
		"delayed_insert_limit":            "100",
		"delayed_insert_timeout":          "300",
		"delayed_queue_size":              "1000",

		// Full-text search
		"ft_boolean_syntax":        "+ -><()~*:\"\"&|",
		"ft_max_word_len":          "84",
		"ft_min_word_len":          "4",
		"ft_query_expansion_limit": "20",
		"ft_stopword_file":         "(built-in)",

		// Misc variables
		"big_tables":                        "OFF",
		"block_encryption_mode":             "aes-128-ecb",
		"div_precision_increment":           "4",
		"end_markers_in_json":               "OFF",
		"explicit_defaults_for_timestamp":   "ON",
		"foreign_key_checks":                "ON",
		"unique_checks":                     "ON",
		"sql_auto_is_null":                  "OFF",
		"sql_big_selects":                   "ON",
		"sql_buffer_result":                 "OFF",
		"sql_log_bin":                       "ON",
		"sql_notes":                         "ON",
		"sql_quote_show_create":             "ON",
		"sql_safe_updates":                  "OFF",
		"sql_select_limit":                  "18446744073709551615",
		"sql_warnings":                      "OFF",
		"updatable_views_with_limit":        "YES",
		"max_error_count":                   "1024",
		"max_execution_time":                "0",
		"max_join_size":                     "18446744073709551615",
		"max_points_in_geometry":            "65536",
		"max_seeks_for_key":                 "18446744073709551615",
		"max_sp_recursion_depth":            "0",
		"max_prepared_stmt_count":           "16382",
		"stored_program_cache":              "256",
		"stored_program_definition_cache":   "256",
		"event_scheduler":                   "ON",
		"default_week_format":               "0",
		"default_collation_for_utf8mb4":     "utf8mb4_0900_ai_ci",
		"information_schema_stats_expiry":   "86400",
		"ngram_token_size":                  "2",
		"regexp_stack_limit":                "8000000",
		"regexp_time_limit":                 "32",
		"cte_max_recursion_depth":           "1000",
		"internal_tmp_mem_storage_engine":   "TempTable",
		"temptable_max_mmap":                "1073741824",
		"temptable_max_ram":                 "1073741824",
		"temptable_use_mmap":                "ON",
		"windowing_use_high_precision":      "ON",
		"histogram_generation_max_mem_size": "20000000",
		"use_secondary_engine":              "ON",
		"secondary_engine_cost_threshold":   "100000.000000",
		"show_create_table_verbosity":       "OFF",
		"show_old_temporals":                "OFF",
		"session_track_gtids":               "OFF",
		"session_track_schema":              "ON",
		"session_track_state_change":        "OFF",
		"session_track_system_variables":    "time_zone,autocommit,character_set_client,character_set_results,character_set_connection",
		"session_track_transaction_info":    "OFF",
		"avoid_temporal_upgrade":            "OFF",
		"show_compatibility_56":             "OFF",
		"lc_messages":                       "en_US",
		"lc_messages_dir":                   "/usr/share/mysql/",
		"lc_time_names":                     "en_US",

		// Performance schema
		"performance_schema": "ON",

		// Admin
		"admin_address":                "",
		"admin_port":                   "33062",
		"create_admin_listener_thread": "OFF",

		// Bind address
		"bind_address": "*",

		// Profiling
		"profiling":              "OFF",
		"profiling_history_size": "15",

		// Error handling
		"error_count":   "0",
		"warning_count": "0",

		// Timestamps and time
		"timestamp":        "0",
		"time_zone":        "SYSTEM",
		"system_time_zone": "UTC",

		// SSL/TLS
		"have_ssl":                               "YES",
		"have_openssl":                           "YES",
		"ssl_ca":                                 "",
		"ssl_capath":                             "",
		"ssl_cert":                               "",
		"ssl_cipher":                             "",
		"ssl_crl":                                "",
		"ssl_crlpath":                            "",
		"ssl_key":                                "",
		"tls_version":                            "TLSv1.2,TLSv1.3",
		"tls_ciphersuites":                       "",
		"auto_generate_certs":                    "ON",
		"sha256_password_auto_generate_rsa_keys": "ON",
		"caching_sha2_password_auto_generate_rsa_keys": "ON",
		"sha256_password_private_key_path":             "private_key.pem",
		"sha256_password_public_key_path":              "public_key.pem",
		"caching_sha2_password_private_key_path":       "private_key.pem",
		"caching_sha2_password_public_key_path":        "public_key.pem",

		// Query cache (removed in 8.0 but variables may still appear)
		"have_query_cache": "NO",
		"query_cache_type": "OFF",

		// Various features
		"have_compress":            "YES",
		"have_dynamic_loading":     "YES",
		"have_geometry":            "YES",
		"have_profiling":           "YES",
		"have_rtree_keys":          "YES",
		"have_symlink":             "DISABLED",
		"have_statement_timeout":   "YES",
		"disabled_storage_engines": "",
		"init_connect":             "",
		"init_file":                "",

		// MySQLX
		"mysqlx_bind_address":                         "0.0.0.0",
		"mysqlx_connect_timeout":                      "30",
		"mysqlx_document_id_unique_prefix":            "0",
		"mysqlx_enable_hello_notice":                  "ON",
		"mysqlx_idle_worker_thread_timeout":           "60",
		"mysqlx_interactive_timeout":                  "28800",
		"mysqlx_max_allowed_packet":                   "67108864",
		"mysqlx_max_connections":                      "100",
		"mysqlx_min_worker_threads":                   "2",
		"mysqlx_port":                                 "33060",
		"mysqlx_port_open_timeout":                    "0",
		"mysqlx_read_timeout":                         "30",
		"mysqlx_socket":                               "/var/run/mysqld/mysqlx.sock",
		"mysqlx_ssl_ca":                               "",
		"mysqlx_ssl_capath":                           "",
		"mysqlx_ssl_cert":                             "",
		"mysqlx_ssl_cipher":                           "",
		"mysqlx_ssl_crl":                              "",
		"mysqlx_ssl_crlpath":                          "",
		"mysqlx_ssl_key":                              "",
		"mysqlx_wait_timeout":                         "28800",
		"mysqlx_write_timeout":                        "60",
		"mysqlx_compression_algorithms":               "deflate_stream,lz4_message,zstd_stream",
		"mysqlx_deflate_default_compression_level":    "3",
		"mysqlx_deflate_max_client_compression_level": "5",
		"mysqlx_lz4_default_compression_level":        "2",
		"mysqlx_lz4_max_client_compression_level":     "8",
		"mysqlx_zstd_default_compression_level":       "3",
		"mysqlx_zstd_max_client_compression_level":    "11",

		// Expire logs
		"expire_logs_days": "0",

		// Misc
		"max_digest_length":           "1024",
		"max_insert_delayed_threads":  "20",
		"min_examined_row_limit":      "0",
		"new":                         "OFF",
		"old":                         "OFF",
		"old_alter_table":             "OFF",
		"protocol_version":            "10",
		"select_into_buffer_size":     "131072",
		"select_into_disk_sync":       "OFF",
		"select_into_disk_sync_delay": "0",

		// Missing variables referenced by sys_vars tests
		"default_table_encryption":               "OFF",
		"host_cache_size":                        "128",
		"identity":                               "0",
		"insert_id":                              "0",
		"immediate_server_version":               "999999",
		"original_server_version":                "999999",
		"original_commit_timestamp":              "36028797018963968",
		"init_slave":                             "",
		"keep_files_on_create":                   "OFF",
		"large_files_support":                    "ON",
		"large_pages":                            "OFF",
		"large_page_size":                        "0",
		"locked_in_memory":                       "OFF",
		"log_bin_use_v1_row_events":              "OFF",
		"log_statements_unsafe_for_binlog":       "ON",
		"log_throttle_queries_not_using_indexes": "0",
		"master_info_repository":                 "TABLE",
		"master_verify_checksum":                 "OFF",
		"max_binlog_cache_size":                  "18446744073709547520",
		"max_binlog_size":                        "1073741824",
		"max_binlog_stmt_cache_size":             "18446744073709547520",
		"max_relay_log_size":                     "0",
		"myisam_mmap_size":                       "18446744073709551615",
		"offline_mode":                           "OFF",
		"parser_max_mem_size":                    "18446744073709551615",
		"persisted_globals_load":                 "ON",
		"print_identified_with_as_hex":           "OFF",
		"pseudo_slave_mode":                      "OFF",
		"pseudo_thread_id":                       "0",
		"query_alloc_block_size":                 "8192",
		"query_prealloc_size":                    "8192",
		"rand_seed1":                             "0",
		"rand_seed2":                             "0",
		"range_alloc_block_size":                 "4096",
		"rbr_exec_mode":                          "STRICT",
		"read_only":                              "OFF",
		"relay_log_info_repository":              "TABLE",
		"report_host":                            "",
		"report_password":                        "",
		"report_port":                            "0",
		"report_user":                            "",
		"rpl_read_size":                          "8192",
		"rpl_stop_slave_timeout":                 "31536000",
		"schema_definition_cache":                "256",
		"server_id_bits":                         "32",
		"skip_external_locking":                  "ON",
		"slave_allow_batching":                   "OFF",
		"slave_checkpoint_group":                 "512",
		"slave_checkpoint_period":                "300",
		"slave_compressed_protocol":              "OFF",
		"slave_max_allowed_packet":               "1073741824",
		"slave_net_timeout":                      "60",
		"slave_parallel_type":                    "DATABASE",
		"slave_parallel_workers":                 "0",
		"slave_pending_jobs_size_max":            "134217728",
		"slave_preserve_commit_order":            "OFF",
		"slave_rows_search_algorithms":           "INDEX_SCAN,HASH_SCAN",
		"slave_skip_errors":                      "OFF",
		"slave_sql_verify_checksum":              "ON",
		"slave_transaction_retries":              "10",
		"slave_type_conversions":                 "",
		"slow_launch_time":                       "2",
		"sql_log_off":                            "OFF",
		"sql_slave_skip_counter":                 "0",
		"super_read_only":                        "OFF",
		"sync_master_info":                       "10000",
		"sync_relay_log":                         "10000",
		"sync_relay_log_info":                    "10000",
		"table_encryption_privilege_check":       "OFF",
		"tablespace_definition_cache":            "256",
		"thread_handling":                        "one-thread-per-connection",
		"transaction_alloc_block_size":           "8192",
		"transaction_allow_batching":             "OFF",
		"transaction_prealloc_size":              "4096",
		"version_compile_zlib":                   "1.2.13",
		"innodb_directories":                     "",
		"innodb_log_files_in_group":              "2",
		"innodb_log_group_home_dir":              "./",
		"innodb_page_cleaners":                   "1",
		"innodb_redo_log_archive_dirs":           "",
		"innodb_fsync_threshold":                 "0",
		"innodb_version":                         "8.0.32",

		// Performance schema sizing variables (pfs_*)
		"performance_schema_accounts_size":                         "-1",
		"performance_schema_digests_size":                          "10000",
		"performance_schema_error_size":                            "5124",
		"performance_schema_events_stages_history_size":            "10",
		"performance_schema_events_stages_history_long_size":       "10000",
		"performance_schema_events_statements_history_size":        "10",
		"performance_schema_events_statements_history_long_size":   "10000",
		"performance_schema_events_transactions_history_size":      "10",
		"performance_schema_events_transactions_history_long_size": "10000",
		"performance_schema_events_waits_history_size":             "10",
		"performance_schema_events_waits_history_long_size":        "10000",
		"performance_schema_hosts_size":                            "-1",
		"performance_schema_max_cond_classes":                      "100",
		"performance_schema_max_cond_instances":                    "-1",
		"performance_schema_max_digest_length":                     "1024",
		"performance_schema_max_digest_sample_age":                 "60",
		"performance_schema_max_file_classes":                      "80",
		"performance_schema_max_file_handles":                      "32768",
		"performance_schema_max_file_instances":                    "-1",
		"performance_schema_max_index_stat":                        "-1",
		"performance_schema_max_memory_classes":                    "450",
		"performance_schema_max_metadata_locks":                    "-1",
		"performance_schema_max_mutex_classes":                     "300",
		"performance_schema_max_mutex_instances":                   "-1",
		"performance_schema_max_prepared_statements_instances":     "-1",
		"performance_schema_max_program_instances":                 "-1",
		"performance_schema_max_rwlock_classes":                    "60",
		"performance_schema_max_rwlock_instances":                  "-1",
		"performance_schema_max_socket_classes":                    "123",
		"performance_schema_max_socket_instances":                  "123",
		"performance_schema_max_sql_text_length":                   "1024",
		"performance_schema_max_stage_classes":                     "175",
		"performance_schema_max_statement_classes":                 "218",
		"performance_schema_max_statement_stack":                   "10",
		"performance_schema_max_table_handles":                     "-1",
		"performance_schema_max_table_instances":                   "-1",
		"performance_schema_max_table_lock_stat":                   "-1",
		"performance_schema_max_thread_classes":                    "100",
		"performance_schema_max_thread_instances":                  "-1",
		"performance_schema_session_connect_attrs_size":            "512",
		"performance_schema_setup_actors_size":                     "-1",
		"performance_schema_setup_objects_size":                    "-1",
		"performance_schema_users_size":                            "-1",
	}

	// Override with any SET GLOBAL/SESSION values
	for name, val := range e.startupVars {
		// performance_schema_consumer_* and performance_schema_instrument are
		// startup-only options that configure performance_schema.setup_consumers
		// and setup_instruments tables. They are NOT exposed as system variables.
		if strings.HasPrefix(name, "performance_schema_consumer_") ||
			name == "performance_schema_instrument" {
			continue
		}
		// Apply minimum/maximum constraints for known variables
		if name == "innodb_stats_transient_sample_pages" || name == "innodb_stats_persistent_sample_pages" {
			if n, err := strconv.ParseInt(val, 10, 64); err == nil && n < 1 {
				val = "1"
			}
		}
		if defaultVal, ok := vars[name]; ok {
			upperDefault := strings.ToUpper(defaultVal)
			if upperDefault == "ON" || upperDefault == "OFF" {
				upperVal := strings.ToUpper(val)
				if upperVal == "1" || upperVal == "ON" || upperVal == "TRUE" || upperVal == "YES" {
					val = "ON"
				} else if upperVal == "0" || upperVal == "OFF" || upperVal == "FALSE" || upperVal == "NO" {
					val = "OFF"
				}
			}
		}
		vars[name] = val
	}

	// Override with SET GLOBAL values.
	// For session scope (!globalOnly), only apply global overrides for global-only
	// variables because SET @@global.var should not affect @@session.var.
	globalVarsCopy := make(map[string]string, len(e.globalScopeVars))
	if e.globalVarsMu != nil {
		e.globalVarsMu.RLock()
	}
	for k, v := range e.globalScopeVars {
		globalVarsCopy[k] = v
	}
	if e.globalVarsMu != nil {
		e.globalVarsMu.RUnlock()
	}
	for name, val := range globalVarsCopy {
		// performance_schema_consumer_* are startup-only, not system variables
		if strings.HasPrefix(name, "performance_schema_consumer_") ||
			name == "performance_schema_instrument" {
			continue
		}
		if !globalOnly && !sysVarGlobalOnly[name] && !sysVarBothScope[name] {
			continue
		}
		if name == "innodb_stats_transient_sample_pages" || name == "innodb_stats_persistent_sample_pages" {
			if n, err := strconv.ParseInt(val, 10, 64); err == nil && n < 1 {
				val = "1"
			}
		}
		if defaultVal, ok := vars[name]; ok {
			upperDefault := strings.ToUpper(defaultVal)
			if upperDefault == "ON" || upperDefault == "OFF" {
				upperVal := strings.ToUpper(val)
				if upperVal == "1" || upperVal == "ON" || upperVal == "TRUE" || upperVal == "YES" {
					val = "ON"
				} else if upperVal == "0" || upperVal == "OFF" || upperVal == "FALSE" || upperVal == "NO" {
					val = "OFF"
				}
			}
		}
		vars[name] = val
	}

	// Override with SET SESSION values (skip for SHOW GLOBAL VARIABLES)
	if globalOnly {
		return vars
	}
	for name, val := range e.sessionScopeVars {
		// Apply minimum/maximum constraints for known variables
		if name == "innodb_stats_transient_sample_pages" || name == "innodb_stats_persistent_sample_pages" {
			if n, err := strconv.ParseInt(val, 10, 64); err == nil && n < 1 {
				val = "1"
			}
		}
		// For boolean variables (default is ON/OFF), normalize 1/0/on/off/true/false
		// to ON/OFF for SHOW VARIABLES / performance_schema display.
		if defaultVal, ok := vars[name]; ok {
			upperDefault := strings.ToUpper(defaultVal)
			if upperDefault == "ON" || upperDefault == "OFF" {
				upperVal := strings.ToUpper(val)
				if upperVal == "1" || upperVal == "ON" || upperVal == "TRUE" || upperVal == "YES" {
					val = "ON"
				} else if upperVal == "0" || upperVal == "OFF" || upperVal == "FALSE" || upperVal == "NO" {
					val = "OFF"
				}
			}
		}
		vars[name] = val
	}
	// Override dynamic runtime values
	vars["insert_id"] = strconv.FormatInt(e.nextInsertID, 10)
	vars["identity"] = strconv.FormatInt(e.lastInsertID, 10)
	vars["last_insert_id"] = strconv.FormatInt(e.lastInsertID, 10)
	// pseudo_thread_id defaults to the connection ID (if not explicitly SET)
	if _, hasExplicit := e.sessionScopeVars["pseudo_thread_id"]; !hasExplicit {
		vars["pseudo_thread_id"] = strconv.FormatInt(e.connectionID, 10)
	}
	// rand_seed1 and rand_seed2 always read as 0 (write-only variables)
	vars["rand_seed1"] = "0"
	vars["rand_seed2"] = "0"
	return vars
}

func (e *Executor) showVariables(upper string) (*Result, error) {
	likePattern := ""
	if idx := strings.Index(upper, "LIKE '"); idx >= 0 {
		rest := upper[idx+6:]
		if end := strings.Index(rest, "'"); end >= 0 {
			likePattern = strings.ToLower(rest[:end])
		}
	}
	if likePattern == "" {
		if idx := strings.Index(upper, `LIKE "`); idx >= 0 {
			rest := upper[idx+6:]
			if end := strings.Index(rest, `"`); end >= 0 {
				likePattern = strings.ToLower(rest[:end])
			}
		}
	}
	if likePattern == "" {
		if idx := strings.Index(upper, "VARIABLE_NAME"); idx >= 0 {
			rest := upper[idx+len("VARIABLE_NAME"):]
			if eq := strings.Index(rest, "="); eq >= 0 {
				rest = strings.TrimSpace(rest[eq+1:])
				if strings.HasPrefix(rest, "'") {
					rest = rest[1:]
					if end := strings.Index(rest, "'"); end >= 0 {
						likePattern = strings.ToLower(rest[:end])
					}
				}
			}
		}
	}

	isGlobalShow := strings.Contains(upper, "GLOBAL")
	vars := e.buildVariablesMapScoped(isGlobalShow)

	var rows [][]interface{}
	for name, val := range vars {
		// For SHOW GLOBAL VARIABLES, exclude session-only variables
		if isGlobalShow && sysVarSessionOnly[name] {
			continue
		}
		if likePattern != "" && !matchLike(name, likePattern) {
			continue
		}
		rows = append(rows, []interface{}{name, val})
	}
	// Sort by variable name
	sort.Slice(rows, func(i, j int) bool {
		return rows[i][0].(string) < rows[j][0].(string)
	})

	return &Result{
		Columns:     []string{"Variable_name", "Value"},
		Rows:        rows,
		IsResultSet: true,
	}, nil
}

// showStatus handles SHOW [GLOBAL|SESSION] STATUS [LIKE '...']
func (e *Executor) showStatus(upper string) (*Result, error) {
	likePattern := ""
	if idx := strings.Index(upper, "LIKE '"); idx >= 0 {
		rest := upper[idx+6:]
		if end := strings.Index(rest, "'"); end >= 0 {
			likePattern = strings.ToLower(rest[:end])
		}
	}
	if likePattern == "" {
		if idx := strings.Index(upper, `LIKE "`); idx >= 0 {
			rest := upper[idx+6:]
			if end := strings.Index(rest, `"`); end >= 0 {
				likePattern = strings.ToLower(rest[:end])
			}
		}
	}
	statusVars := []struct {
		Name  string
		Value string
	}{
		{Name: "Aborted_clients", Value: "0"},
		{Name: "Aborted_connects", Value: "0"},
		{Name: "Bytes_received", Value: "0"},
		{Name: "Bytes_sent", Value: "0"},
		{Name: "Com_select", Value: "1"},
		{Name: "Connections", Value: func() string {
			if e.nextConnID != nil {
				return fmt.Sprintf("%d", e.nextConnID.Load())
			}
			return "1"
		}()},
		{Name: "Handler_commit", Value: "0"},
		{Name: "Handler_delete", Value: "0"},
		{Name: "Handler_discover", Value: "0"},
		{Name: "Handler_external_lock", Value: "0"},
		{Name: "Handler_mrr_init", Value: "0"},
		{Name: "Handler_prepare", Value: "0"},
		{Name: "Handler_read_first", Value: fmt.Sprintf("%d", e.handlerReadFirst)},
		{Name: "Handler_read_key", Value: fmt.Sprintf("%d", e.handlerReadKey)},
		{Name: "Handler_read_last", Value: fmt.Sprintf("%d", e.handlerReadLast)},
		{Name: "Handler_read_next", Value: fmt.Sprintf("%d", e.handlerReadNext)},
		{Name: "Handler_read_prev", Value: fmt.Sprintf("%d", e.handlerReadPrev)},
		{Name: "Handler_read_rnd", Value: fmt.Sprintf("%d", e.handlerReadRnd)},
		{Name: "Handler_read_rnd_next", Value: fmt.Sprintf("%d", e.handlerReadRndNext)},
		{Name: "Handler_rollback", Value: "0"},
		{Name: "Handler_savepoint", Value: "0"},
		{Name: "Handler_savepoint_rollback", Value: "0"},
		{Name: "Handler_update", Value: "0"},
		{Name: "Handler_write", Value: "0"},
		{Name: "Max_used_connections", Value: "1"},
		{Name: "Open_tables", Value: "0"},
		{Name: "Opened_tables", Value: "0"},
		{Name: "Queries", Value: "1"},
		{Name: "Questions", Value: "1"},
		{Name: "Slow_queries", Value: "0"},
		{Name: "Sort_merge_passes", Value: "0"},
		{Name: "Sort_range", Value: fmt.Sprintf("%d", e.sortRange)},
		{Name: "Sort_rows", Value: fmt.Sprintf("%d", e.sortRows)},
		{Name: "Sort_scan", Value: fmt.Sprintf("%d", e.sortScan)},
		{Name: "Threads_cached", Value: "0"},
		{Name: "Threads_connected", Value: "1"},
		{Name: "Threads_created", Value: "1"},
		{Name: "Threads_running", Value: "1"},
		{Name: "Uptime", Value: "1"},
		// InnoDB status variables
		{Name: "Innodb_page_size", Value: "16384"},
		// Performance Schema status variables
		{Name: "Performance_schema_accounts_lost", Value: "0"},
		{Name: "Performance_schema_cond_classes_lost", Value: "0"},
		{Name: "Performance_schema_cond_instances_lost", Value: "0"},
		{Name: "Performance_schema_digest_lost", Value: "0"},
		{Name: "Performance_schema_file_classes_lost", Value: "0"},
		{Name: "Performance_schema_file_handles_lost", Value: "0"},
		{Name: "Performance_schema_file_instances_lost", Value: "0"},
		{Name: "Performance_schema_hosts_lost", Value: "0"},
		{Name: "Performance_schema_index_stat_lost", Value: "0"},
		{Name: "Performance_schema_locker_lost", Value: "0"},
		{Name: "Performance_schema_memory_classes_lost", Value: "0"},
		{Name: "Performance_schema_metadata_lock_lost", Value: "0"},
		{Name: "Performance_schema_mutex_classes_lost", Value: "0"},
		{Name: "Performance_schema_mutex_instances_lost", Value: "0"},
		{Name: "Performance_schema_nested_statement_lost", Value: "0"},
		{Name: "Performance_schema_prepared_statements_lost", Value: "0"},
		{Name: "Performance_schema_program_lost", Value: "0"},
		{Name: "Performance_schema_rwlock_classes_lost", Value: "0"},
		{Name: "Performance_schema_rwlock_instances_lost", Value: "0"},
		{Name: "Performance_schema_session_connect_attrs_longest_seen", Value: "0"},
		{Name: "Performance_schema_session_connect_attrs_lost", Value: "0"},
		{Name: "Performance_schema_socket_classes_lost", Value: "0"},
		{Name: "Performance_schema_socket_instances_lost", Value: "0"},
		{Name: "Performance_schema_stage_classes_lost", Value: "0"},
		{Name: "Performance_schema_statement_classes_lost", Value: "0"},
		{Name: "Performance_schema_table_handles_lost", Value: "0"},
		{Name: "Performance_schema_table_instances_lost", Value: "0"},
		{Name: "Performance_schema_table_lock_stat_lost", Value: "0"},
		{Name: "Performance_schema_thread_classes_lost", Value: "0"},
		{Name: "Performance_schema_thread_instances_lost", Value: "0"},
		{Name: "Performance_schema_users_lost", Value: "0"},
		// SSL / OpenSSL status variables (stubs for MTR have_openssl.inc)
		{Name: "Rsa_public_key", Value: ""},
	}
	rows := make([][]interface{}, 0, len(statusVars))
	for _, sv := range statusVars {
		if likePattern != "" {
			nameLower := strings.ToLower(sv.Name)
			patLower := strings.ToLower(likePattern)
			if strings.Contains(patLower, "%") || strings.Contains(patLower, "_") {
				if !matchLike(nameLower, patLower) {
					continue
				}
			} else if !strings.EqualFold(sv.Name, likePattern) {
				continue
			}
		}
		rows = append(rows, []interface{}{sv.Name, sv.Value})
	}
	return &Result{
		Columns:     []string{"Variable_name", "Value"},
		Rows:        rows,
		IsResultSet: true,
	}, nil
}

// setExprContainsColName reports whether a BinaryExpr has at least one ColName operand.
// Used to detect bare-identifier arithmetic in SET variable expressions.
func setExprContainsColName(e sqlparser.Expr) bool {
	switch v := e.(type) {
	case *sqlparser.ColName:
		return true
	case *sqlparser.BinaryExpr:
		return setExprContainsColName(v.Left) || setExprContainsColName(v.Right)
	}
	return false
}

// setExprFirstColName returns the name of the first ColName found in the expression tree.
func setExprFirstColName(e sqlparser.Expr) string {
	switch v := e.(type) {
	case *sqlparser.ColName:
		return v.Name.String()
	case *sqlparser.BinaryExpr:
		if name := setExprFirstColName(v.Left); name != "" {
			return name
		}
		return setExprFirstColName(v.Right)
	}
	return ""
}
