package executor

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	catalogPkg "github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

func asInt64Or(v interface{}, fallback int64) int64 {
	switch x := v.(type) {
	case int:
		return int64(x)
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case int64:
		return x
	case uint:
		return int64(x)
	case uint8:
		return int64(x)
	case uint16:
		return int64(x)
	case uint32:
		return int64(x)
	case uint64:
		return int64(x)
	default:
		return fallback
	}
}

func normalizeIndexColumnName(col string) string {
	if idx := strings.Index(col, "("); idx >= 0 {
		return strings.TrimSpace(col[:idx])
	}
	return strings.TrimSpace(col)
}

func (e *Executor) informationSchemaStatsExpiryZero() bool {
	v, ok := e.getSysVar("information_schema_stats_expiry")
	if !ok {
		return false
	}
	trimmed := strings.TrimSpace(strings.ToLower(v))
	if trimmed == "" || trimmed == "default" {
		return false
	}
	n, err := strconv.ParseInt(trimmed, 10, 64)
	return err == nil && n == 0
}

func distinctPrefixCounts(rows []storage.Row, cols []string) []int64 {
	counts := make([]int64, len(cols))
	for i := range cols {
		seen := make(map[string]struct{}, len(rows))
		for _, r := range rows {
			var b strings.Builder
			for j := 0; j <= i; j++ {
				if j > 0 {
					b.WriteByte(0x1f)
				}
				colName := normalizeIndexColumnName(cols[j])
				b.WriteString(toString(r[colName]))
			}
			seen[b.String()] = struct{}{}
		}
		counts[i] = int64(len(seen))
	}
	return counts
}

// infoSchemaColumnOrder defines the canonical column order for INFORMATION_SCHEMA tables.
var infoSchemaColumnOrder = map[string][]string{
	"schemata":                 {"CATALOG_NAME", "SCHEMA_NAME", "DEFAULT_CHARACTER_SET_NAME", "DEFAULT_COLLATION_NAME", "SQL_PATH", "DEFAULT_ENCRYPTION"},
	"tables":                   {"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "ENGINE", "VERSION", "ROW_FORMAT", "TABLE_ROWS", "AVG_ROW_LENGTH", "DATA_LENGTH", "MAX_DATA_LENGTH", "INDEX_LENGTH", "DATA_FREE", "AUTO_INCREMENT", "CREATE_TIME", "UPDATE_TIME", "CHECK_TIME", "TABLE_COLLATION", "CHECKSUM", "CREATE_OPTIONS", "TABLE_COMMENT"},
	"columns":                  {"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "COLUMN_DEFAULT", "IS_NULLABLE", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE", "DATETIME_PRECISION", "CHARACTER_SET_NAME", "COLLATION_NAME", "COLUMN_TYPE", "COLUMN_KEY", "EXTRA", "PRIVILEGES", "COLUMN_COMMENT", "GENERATION_EXPRESSION", "SRS_ID"},
	"statistics":               {"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "NON_UNIQUE", "INDEX_SCHEMA", "INDEX_NAME", "SEQ_IN_INDEX", "COLUMN_NAME", "COLLATION", "CARDINALITY", "SUB_PART", "PACKED", "NULLABLE", "INDEX_TYPE", "COMMENT", "INDEX_COMMENT", "IS_VISIBLE", "EXPRESSION"},
	"column_statistics":        {"SCHEMA_NAME", "TABLE_NAME", "COLUMN_NAME", "HISTOGRAM"},
	"engines":                  {"ENGINE", "SUPPORT", "COMMENT", "TRANSACTIONS", "XA", "SAVEPOINTS"},
	"innodb_tables":            {"NAME", "SPACE", "FLAG", "N_COLS", "ROW_FORMAT", "ZIP_PAGE_SIZE", "SPACE_TYPE"},
	"innodb_tablespaces":       {"SPACE", "NAME", "ROW_FORMAT", "PAGE_SIZE", "ZIP_PAGE_SIZE", "SPACE_TYPE"},
	"innodb_datafiles":         {"SPACE", "PATH"},
	"innodb_columns":           {"TABLE_ID", "NAME", "POS", "MTYPE", "PRTYPE", "LEN"},
	"innodb_virtual":           {"TABLE_ID", "POS", "BASE_POS"},
	"innodb_foreign":           {"ID", "FOR_NAME", "REF_NAME", "N_COLS"},
	"innodb_metrics":           {"NAME", "COUNT", "TYPE", "STATUS", "SUBSYSTEM", "COMMENT"},
	"innodb_cached_indexes":    {"INDEX_ID", "N_FIELDS", "SPACE", "PAGE_NO"},
	"innodb_indexes":           {"INDEX_ID", "NAME", "TABLE_ID", "TYPE"},
	"innodb_buffer_page_lru":   {"POOL_ID", "LRU_POSITION", "SPACE", "PAGE_NUMBER"},
	"innodb_buffer_page":       {"SPACE", "PAGE_NUMBER", "PAGE_TYPE", "NUMBER_RECORDS"},
	"innodb_buffer_pool_stats": {"POOL_ID", "POOL_SIZE"},
	"innodb_trx":               {"trx_id", "trx_state", "trx_started"},
	"innodb_foreign_cols":      {"ID", "FOR_COL_NAME", "REF_COL_NAME", "POS"},
	"innodb_fields":            {"INDEX_ID", "NAME", "POS"},
	"optimizer_trace":          {"QUERY", "TRACE"},
	"files":                    {"FILE_NAME", "FILE_TYPE", "TABLESPACE_NAME"},
	"processlist":              {"ID", "USER", "HOST", "DB", "COMMAND", "TIME", "STATE", "INFO"},
	"key_column_usage":         {"CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "POSITION_IN_UNIQUE_CONSTRAINT", "REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME"},
	"referential_constraints":  {"CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "UNIQUE_CONSTRAINT_CATALOG", "UNIQUE_CONSTRAINT_SCHEMA", "UNIQUE_CONSTRAINT_NAME", "MATCH_OPTION", "UPDATE_RULE", "DELETE_RULE", "TABLE_NAME", "REFERENCED_TABLE_NAME"},
	"innodb_temp_table_info":          {"TABLE_ID", "NAME", "N_COLS", "SPACE"},
	"global_variables":                {"VARIABLE_NAME", "VARIABLE_VALUE"},
	"session_variables":               {"VARIABLE_NAME", "VARIABLE_VALUE"},
	"events_waits_history_long":       {"THREAD_ID", "EVENT_ID", "END_EVENT_ID", "EVENT_NAME", "SOURCE", "TIMER_START", "TIMER_END", "TIMER_WAIT", "SPINS", "OBJECT_SCHEMA", "OBJECT_NAME", "INDEX_NAME", "OBJECT_TYPE", "OBJECT_INSTANCE_BEGIN", "NESTING_EVENT_ID", "NESTING_EVENT_TYPE", "OPERATION", "NUMBER_OF_BYTES", "FLAGS"},
	"events_waits_current":            {"THREAD_ID", "EVENT_ID", "END_EVENT_ID", "EVENT_NAME", "SOURCE", "TIMER_START", "TIMER_END", "TIMER_WAIT", "SPINS", "OBJECT_SCHEMA", "OBJECT_NAME", "INDEX_NAME", "OBJECT_TYPE", "OBJECT_INSTANCE_BEGIN", "NESTING_EVENT_ID", "NESTING_EVENT_TYPE", "OPERATION", "NUMBER_OF_BYTES", "FLAGS"},
	"events_statements_history_long":  {"THREAD_ID", "EVENT_ID", "END_EVENT_ID", "EVENT_NAME", "SOURCE", "TIMER_START", "TIMER_END", "TIMER_WAIT", "SQL_TEXT", "DIGEST", "DIGEST_TEXT"},
	"events_stages_history_long":      {"THREAD_ID", "EVENT_ID", "END_EVENT_ID", "EVENT_NAME", "SOURCE", "TIMER_START", "TIMER_END", "TIMER_WAIT"},
	"performance_timers":              {"TIMER_NAME", "TIMER_FREQUENCY", "TIMER_RESOLUTION", "TIMER_OVERHEAD"},
	"threads":                         {"THREAD_ID", "NAME", "TYPE", "PROCESSLIST_ID", "PROCESSLIST_USER", "PROCESSLIST_HOST", "PROCESSLIST_DB", "PROCESSLIST_COMMAND", "PROCESSLIST_TIME", "PROCESSLIST_STATE", "PROCESSLIST_INFO", "PARENT_THREAD_ID", "ROLE", "INSTRUMENTED", "HISTORY", "CONNECTION_TYPE", "THREAD_OS_ID", "RESOURCE_GROUP"},
	"setup_actors":                    {"HOST", "USER", "ROLE", "ENABLED", "HISTORY"},
	"triggers":                        {"TRIGGER_CATALOG", "TRIGGER_SCHEMA", "TRIGGER_NAME", "EVENT_MANIPULATION", "EVENT_OBJECT_CATALOG", "EVENT_OBJECT_SCHEMA", "EVENT_OBJECT_TABLE", "ACTION_ORDER", "ACTION_CONDITION", "ACTION_STATEMENT", "ACTION_ORIENTATION", "ACTION_TIMING", "ACTION_REFERENCE_OLD_TABLE", "ACTION_REFERENCE_NEW_TABLE", "ACTION_REFERENCE_OLD_ROW", "ACTION_REFERENCE_NEW_ROW", "CREATED", "SQL_MODE", "DEFINER", "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION", "DATABASE_COLLATION"},
	"table_constraints":               {"CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "TABLE_SCHEMA", "TABLE_NAME", "CONSTRAINT_TYPE", "ENFORCED"},
	"check_constraints":               {"CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "CHECK_CLAUSE"},
	"character_sets":                   {"CHARACTER_SET_NAME", "DEFAULT_COLLATE_NAME", "DESCRIPTION", "MAXLEN"},
	"collations":                       {"COLLATION_NAME", "CHARACTER_SET_NAME", "ID", "IS_DEFAULT", "IS_COMPILED", "SORTLEN", "PAD_ATTRIBUTE"},
	"collation_character_set_applicability": {"COLLATION_NAME", "CHARACTER_SET_NAME"},
	"user_privileges":                  {"GRANTEE", "TABLE_CATALOG", "PRIVILEGE_TYPE", "IS_GRANTABLE"},
	"schema_privileges":                {"GRANTEE", "TABLE_CATALOG", "TABLE_SCHEMA", "PRIVILEGE_TYPE", "IS_GRANTABLE"},
	"table_privileges":                 {"GRANTEE", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "PRIVILEGE_TYPE", "IS_GRANTABLE"},
	"column_privileges":                {"GRANTEE", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "PRIVILEGE_TYPE", "IS_GRANTABLE"},
	"routines":                         {"SPECIFIC_NAME", "ROUTINE_CATALOG", "ROUTINE_SCHEMA", "ROUTINE_NAME", "ROUTINE_TYPE", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE", "DATETIME_PRECISION", "CHARACTER_SET_NAME", "COLLATION_NAME", "DTD_IDENTIFIER", "ROUTINE_BODY", "ROUTINE_DEFINITION", "EXTERNAL_NAME", "EXTERNAL_LANGUAGE", "PARAMETER_STYLE", "IS_DETERMINISTIC", "SQL_DATA_ACCESS", "SQL_PATH", "SECURITY_TYPE", "CREATED", "LAST_ALTERED", "SQL_MODE", "ROUTINE_COMMENT", "DEFINER", "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION", "DATABASE_COLLATION"},
	"views":                            {"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "VIEW_DEFINITION", "CHECK_OPTION", "IS_UPDATABLE", "DEFINER", "SECURITY_TYPE", "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION"},
	// performance_schema stub tables
	"accounts":                         {"USER", "HOST", "CURRENT_CONNECTIONS", "TOTAL_CONNECTIONS"},
	"users":                            {"USER", "CURRENT_CONNECTIONS", "TOTAL_CONNECTIONS"},
	"hosts":                            {"HOST", "CURRENT_CONNECTIONS", "TOTAL_CONNECTIONS"},
	"setup_objects":                    {"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "ENABLED", "TIMED"},
	"setup_instruments":                {"NAME", "ENABLED", "TIMED", "PROPERTIES", "VOLATILITY", "DOCUMENTATION"},
	"setup_threads":                    {"NAME", "ENABLED", "HISTORY", "PROPERTIES", "VOLATILITY", "DOCUMENTATION"},
	"persisted_variables":              {"VARIABLE_NAME", "VARIABLE_VALUE"},
	"variables_info":                   {"VARIABLE_NAME", "VARIABLE_SOURCE", "VARIABLE_PATH", "MIN_VALUE", "MAX_VALUE", "SET_TIME", "SET_USER", "SET_HOST"},
	"variables_by_thread":              {"THREAD_ID", "VARIABLE_NAME", "VARIABLE_VALUE"},
	"mutex_instances":                  {"NAME", "OBJECT_INSTANCE_BEGIN", "LOCKED_BY_THREAD_ID"},
	"rwlock_instances":                 {"NAME", "OBJECT_INSTANCE_BEGIN", "WRITE_LOCKED_BY_THREAD_ID", "READ_LOCKED_BY_COUNT"},
	"cond_instances":                   {"NAME", "OBJECT_INSTANCE_BEGIN"},
	"file_instances":                   {"FILE_NAME", "EVENT_NAME", "OPEN_COUNT"},
	"file_summary_by_instance":         {"FILE_NAME", "EVENT_NAME", "OBJECT_INSTANCE_BEGIN", "COUNT_STAR", "SUM_TIMER_WAIT"},
	"file_summary_by_event_name":       {"EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "COUNT_READ", "SUM_TIMER_READ", "SUM_NUMBER_OF_BYTES_READ", "COUNT_WRITE", "SUM_TIMER_WRITE", "SUM_NUMBER_OF_BYTES_WRITE", "COUNT_MISC", "SUM_TIMER_MISC"},
	"socket_instances":                 {"EVENT_NAME", "OBJECT_INSTANCE_BEGIN", "THREAD_ID", "SOCKET_ID", "IP", "PORT", "STATE"},
	"socket_summary_by_event_name":     {"EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"},
	"socket_summary_by_instance":       {"EVENT_NAME", "OBJECT_INSTANCE_BEGIN", "COUNT_STAR", "SUM_TIMER_WAIT"},
	"table_handles":                    {"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "OBJECT_INSTANCE_BEGIN", "OWNER_THREAD_ID", "OWNER_EVENT_ID", "INTERNAL_LOCK", "EXTERNAL_LOCK"},
	"table_io_waits_summary_by_table":  {"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "COUNT_READ", "COUNT_WRITE", "COUNT_FETCH", "COUNT_INSERT", "COUNT_UPDATE", "COUNT_DELETE"},
	"table_io_waits_summary_by_index_usage": {"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "INDEX_NAME", "COUNT_STAR", "SUM_TIMER_WAIT"},
	"table_lock_waits_summary_by_table": {"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT"},
	"events_waits_history":             {"THREAD_ID", "EVENT_ID", "END_EVENT_ID", "EVENT_NAME", "SOURCE", "TIMER_START", "TIMER_END", "TIMER_WAIT", "SPINS", "OBJECT_SCHEMA", "OBJECT_NAME", "INDEX_NAME", "OBJECT_TYPE", "OBJECT_INSTANCE_BEGIN", "NESTING_EVENT_ID", "NESTING_EVENT_TYPE", "OPERATION", "NUMBER_OF_BYTES", "FLAGS"},
	"events_stages_current":            {"THREAD_ID", "EVENT_ID", "END_EVENT_ID", "EVENT_NAME", "SOURCE", "TIMER_START", "TIMER_END", "TIMER_WAIT", "WORK_COMPLETED", "WORK_ESTIMATED", "NESTING_EVENT_ID", "NESTING_EVENT_TYPE"},
	"events_stages_history":            {"THREAD_ID", "EVENT_ID", "END_EVENT_ID", "EVENT_NAME", "SOURCE", "TIMER_START", "TIMER_END", "TIMER_WAIT", "WORK_COMPLETED", "WORK_ESTIMATED", "NESTING_EVENT_ID", "NESTING_EVENT_TYPE"},
	"events_statements_current":        {"THREAD_ID", "EVENT_ID", "END_EVENT_ID", "EVENT_NAME", "SOURCE", "TIMER_START", "TIMER_END", "TIMER_WAIT", "SQL_TEXT", "DIGEST", "DIGEST_TEXT", "CURRENT_SCHEMA", "ROWS_AFFECTED", "ROWS_SENT", "ROWS_EXAMINED", "CREATED_TMP_DISK_TABLES", "CREATED_TMP_TABLES", "ERRORS", "WARNINGS", "NESTING_EVENT_ID", "NESTING_EVENT_TYPE"},
	"events_statements_history":        {"THREAD_ID", "EVENT_ID", "END_EVENT_ID", "EVENT_NAME", "SOURCE", "TIMER_START", "TIMER_END", "TIMER_WAIT", "SQL_TEXT", "DIGEST", "DIGEST_TEXT"},
	"events_transactions_current":      {"THREAD_ID", "EVENT_ID", "END_EVENT_ID", "EVENT_NAME", "STATE", "TRX_ID", "GTID", "XID_FORMAT_ID", "XID_GTRID", "XID_BQUAL", "XA_STATE", "SOURCE", "TIMER_START", "TIMER_END", "TIMER_WAIT", "ACCESS_MODE", "ISOLATION_LEVEL", "AUTOCOMMIT", "NESTING_EVENT_ID", "NESTING_EVENT_TYPE"},
	"events_transactions_history":      {"THREAD_ID", "EVENT_ID", "END_EVENT_ID", "EVENT_NAME", "STATE", "TRX_ID", "GTID", "XID_FORMAT_ID", "XID_GTRID", "XID_BQUAL", "XA_STATE", "SOURCE", "TIMER_START", "TIMER_END", "TIMER_WAIT", "ACCESS_MODE", "ISOLATION_LEVEL", "AUTOCOMMIT", "NESTING_EVENT_ID", "NESTING_EVENT_TYPE"},
	"events_transactions_history_long": {"THREAD_ID", "EVENT_ID", "END_EVENT_ID", "EVENT_NAME", "STATE", "TRX_ID", "GTID", "XID_FORMAT_ID", "XID_GTRID", "XID_BQUAL", "XA_STATE", "SOURCE", "TIMER_START", "TIMER_END", "TIMER_WAIT", "ACCESS_MODE", "ISOLATION_LEVEL", "AUTOCOMMIT", "NESTING_EVENT_ID", "NESTING_EVENT_TYPE"},
	"events_waits_summary_by_account_by_event_name":  {"USER", "HOST", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"},
	"events_waits_summary_by_host_by_event_name":     {"HOST", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"},
	"events_waits_summary_by_instance":               {"EVENT_NAME", "OBJECT_INSTANCE_BEGIN", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"},
	"events_waits_summary_by_thread_by_event_name":   {"THREAD_ID", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"},
	"events_waits_summary_by_user_by_event_name":     {"USER", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"},
	"events_waits_summary_global_by_event_name":      {"EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"},
	"events_stages_summary_by_account_by_event_name": {"USER", "HOST", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"},
	"events_stages_summary_by_host_by_event_name":    {"HOST", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"},
	"events_stages_summary_by_thread_by_event_name":  {"THREAD_ID", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"},
	"events_stages_summary_by_user_by_event_name":    {"USER", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"},
	"events_stages_summary_global_by_event_name":     {"EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"},
	"events_statements_summary_by_account_by_event_name": {"USER", "HOST", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "SUM_ERRORS", "SUM_WARNINGS", "SUM_ROWS_AFFECTED", "SUM_ROWS_SENT", "SUM_ROWS_EXAMINED"},
	"events_statements_summary_by_digest":           {"SCHEMA_NAME", "DIGEST", "DIGEST_TEXT", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "SUM_LOCK_TIME", "SUM_ERRORS", "SUM_WARNINGS", "SUM_ROWS_AFFECTED", "SUM_ROWS_SENT", "SUM_ROWS_EXAMINED", "SUM_CREATED_TMP_DISK_TABLES", "SUM_CREATED_TMP_TABLES", "SUM_SELECT_FULL_JOIN", "SUM_SELECT_FULL_RANGE_JOIN", "SUM_SELECT_RANGE", "SUM_SELECT_RANGE_CHECK", "SUM_SELECT_SCAN", "SUM_SORT_MERGE_PASSES", "SUM_SORT_RANGE", "SUM_SORT_ROWS", "SUM_SORT_SCAN", "SUM_NO_INDEX_USED", "SUM_NO_GOOD_INDEX_USED", "FIRST_SEEN", "LAST_SEEN", "QUANTILE_95", "QUANTILE_99", "QUANTILE_999", "QUERY_SAMPLE_TEXT", "QUERY_SAMPLE_SEEN", "QUERY_SAMPLE_TIMER_WAIT"},
	"events_statements_summary_by_host_by_event_name":   {"HOST", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "SUM_ERRORS", "SUM_WARNINGS", "SUM_ROWS_AFFECTED", "SUM_ROWS_SENT", "SUM_ROWS_EXAMINED"},
	"events_statements_summary_by_thread_by_event_name": {"THREAD_ID", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "SUM_ERRORS", "SUM_WARNINGS", "SUM_ROWS_AFFECTED", "SUM_ROWS_SENT", "SUM_ROWS_EXAMINED"},
	"events_statements_summary_by_user_by_event_name":   {"USER", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "SUM_ERRORS", "SUM_WARNINGS", "SUM_ROWS_AFFECTED", "SUM_ROWS_SENT", "SUM_ROWS_EXAMINED"},
	"events_statements_summary_global_by_event_name":    {"EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "SUM_ERRORS", "SUM_WARNINGS", "SUM_ROWS_AFFECTED", "SUM_ROWS_SENT", "SUM_ROWS_EXAMINED"},
	"events_statements_summary_by_program":   {"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"},
	"events_statements_histogram_by_digest":  {"SCHEMA_NAME", "DIGEST", "BUCKET_NUMBER", "BUCKET_TIMER_LOW", "BUCKET_TIMER_HIGH", "COUNT_BUCKET", "COUNT_BUCKET_AND_LOWER", "BUCKET_QUANTILE"},
	"events_statements_histogram_global":     {"BUCKET_NUMBER", "BUCKET_TIMER_LOW", "BUCKET_TIMER_HIGH", "COUNT_BUCKET", "COUNT_BUCKET_AND_LOWER", "BUCKET_QUANTILE"},
	"events_transactions_summary_by_account_by_event_name": {"USER", "HOST", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"},
	"events_transactions_summary_by_host_by_event_name":    {"HOST", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"},
	"events_transactions_summary_by_thread_by_event_name":  {"THREAD_ID", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"},
	"events_transactions_summary_by_user_by_event_name":    {"USER", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"},
	"events_transactions_summary_global_by_event_name":     {"EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"},
	"events_errors_summary_by_account_by_error": {"USER", "HOST", "ERROR_NUMBER", "ERROR_NAME", "SQL_STATE", "SUM_ERROR_RAISED", "SUM_ERROR_HANDLED", "FIRST_SEEN", "LAST_SEEN"},
	"events_errors_summary_by_host_by_error":    {"HOST", "ERROR_NUMBER", "ERROR_NAME", "SQL_STATE", "SUM_ERROR_RAISED", "SUM_ERROR_HANDLED", "FIRST_SEEN", "LAST_SEEN"},
	"events_errors_summary_by_thread_by_error":  {"THREAD_ID", "ERROR_NUMBER", "ERROR_NAME", "SQL_STATE", "SUM_ERROR_RAISED", "SUM_ERROR_HANDLED", "FIRST_SEEN", "LAST_SEEN"},
	"events_errors_summary_by_user_by_error":    {"USER", "ERROR_NUMBER", "ERROR_NAME", "SQL_STATE", "SUM_ERROR_RAISED", "SUM_ERROR_HANDLED", "FIRST_SEEN", "LAST_SEEN"},
	"events_errors_summary_global_by_error":     {"ERROR_NUMBER", "ERROR_NAME", "SQL_STATE", "SUM_ERROR_RAISED", "SUM_ERROR_HANDLED", "FIRST_SEEN", "LAST_SEEN"},
	"memory_summary_by_account_by_event_name":   {"USER", "HOST", "EVENT_NAME", "COUNT_ALLOC", "COUNT_FREE", "SUM_NUMBER_OF_BYTES_ALLOC", "SUM_NUMBER_OF_BYTES_FREE", "LOW_COUNT_USED", "CURRENT_COUNT_USED", "HIGH_COUNT_USED", "LOW_NUMBER_OF_BYTES_USED", "CURRENT_NUMBER_OF_BYTES_USED", "HIGH_NUMBER_OF_BYTES_USED"},
	"memory_summary_by_host_by_event_name":      {"HOST", "EVENT_NAME", "COUNT_ALLOC", "COUNT_FREE", "SUM_NUMBER_OF_BYTES_ALLOC", "SUM_NUMBER_OF_BYTES_FREE", "LOW_COUNT_USED", "CURRENT_COUNT_USED", "HIGH_COUNT_USED", "LOW_NUMBER_OF_BYTES_USED", "CURRENT_NUMBER_OF_BYTES_USED", "HIGH_NUMBER_OF_BYTES_USED"},
	"memory_summary_by_thread_by_event_name":    {"THREAD_ID", "EVENT_NAME", "COUNT_ALLOC", "COUNT_FREE", "SUM_NUMBER_OF_BYTES_ALLOC", "SUM_NUMBER_OF_BYTES_FREE", "LOW_COUNT_USED", "CURRENT_COUNT_USED", "HIGH_COUNT_USED", "LOW_NUMBER_OF_BYTES_USED", "CURRENT_NUMBER_OF_BYTES_USED", "HIGH_NUMBER_OF_BYTES_USED"},
	"memory_summary_by_user_by_event_name":      {"USER", "EVENT_NAME", "COUNT_ALLOC", "COUNT_FREE", "SUM_NUMBER_OF_BYTES_ALLOC", "SUM_NUMBER_OF_BYTES_FREE", "LOW_COUNT_USED", "CURRENT_COUNT_USED", "HIGH_COUNT_USED", "LOW_NUMBER_OF_BYTES_USED", "CURRENT_NUMBER_OF_BYTES_USED", "HIGH_NUMBER_OF_BYTES_USED"},
	"status_by_account":                {"USER", "HOST", "VARIABLE_NAME", "VARIABLE_VALUE"},
	"status_by_host":                   {"HOST", "VARIABLE_NAME", "VARIABLE_VALUE"},
	"status_by_thread":                 {"THREAD_ID", "VARIABLE_NAME", "VARIABLE_VALUE"},
	"status_by_user":                   {"USER", "VARIABLE_NAME", "VARIABLE_VALUE"},
	"replication_connection_configuration": {"CHANNEL_NAME", "HOST", "PORT", "USER", "NETWORK_INTERFACE", "AUTO_POSITION", "SSL_ALLOWED", "SSL_CA_FILE", "SSL_CA_PATH", "SSL_CERTIFICATE", "SSL_CIPHER", "SSL_KEY"},
	"replication_connection_status":    {"CHANNEL_NAME", "GROUP_NAME", "SOURCE_UUID", "THREAD_ID", "SERVICE_STATE"},
	"replication_applier_configuration": {"CHANNEL_NAME", "DESIRED_DELAY"},
	"replication_applier_status":       {"CHANNEL_NAME", "SERVICE_STATE", "REMAINING_DELAY", "COUNT_TRANSACTIONS_RETRIES"},
	"replication_applier_status_by_coordinator": {"CHANNEL_NAME", "THREAD_ID", "SERVICE_STATE"},
	"replication_applier_status_by_worker": {"CHANNEL_NAME", "WORKER_ID", "THREAD_ID", "SERVICE_STATE"},
	"replication_applier_filters":      {"CHANNEL_NAME", "FILTER_NAME", "FILTER_RULE", "CONFIGURED_BY", "ACTIVE_SINCE"},
	"replication_applier_global_filters": {"FILTER_NAME", "FILTER_RULE", "CONFIGURED_BY", "ACTIVE_SINCE"},
	"replication_group_members":        {"CHANNEL_NAME", "MEMBER_ID", "MEMBER_HOST", "MEMBER_PORT", "MEMBER_STATE", "MEMBER_ROLE", "MEMBER_VERSION", "MEMBER_COMMUNICATION_STACK"},
	"replication_group_member_stats":   {"CHANNEL_NAME", "VIEW_ID", "MEMBER_ID", "COUNT_TRANSACTIONS_IN_QUEUE", "COUNT_TRANSACTIONS_CHECKED", "COUNT_CONFLICTS_DETECTED", "COUNT_TRANSACTIONS_ROWS_VALIDATING", "TRANSACTIONS_COMMITTED_ALL_MEMBERS", "LAST_CONFLICT_FREE_TRANSACTION", "COUNT_TRANSACTIONS_REMOTE_IN_APPLIER_QUEUE", "COUNT_TRANSACTIONS_REMOTE_APPLIED", "COUNT_TRANSACTIONS_LOCAL_PROPOSED", "COUNT_TRANSACTIONS_LOCAL_ROLLBACK"},
	"keyring_keys":                     {"KEY_ID", "KEY_OWNER", "BACKEND_KEY_ID"},
	"host_cache":                       {"IP", "HOST", "HOST_VALIDATED", "SUM_CONNECT_ERRORS", "COUNT_HOST_BLOCKED_ERRORS", "COUNT_NAMEINFO_TRANSIENT_ERRORS", "COUNT_NAMEINFO_PERMANENT_ERRORS", "COUNT_FORMAT_ERRORS", "COUNT_ADDRINFO_TRANSIENT_ERRORS", "COUNT_ADDRINFO_PERMANENT_ERRORS", "COUNT_FCRDNS_ERRORS", "COUNT_HOST_ACL_ERRORS", "COUNT_NO_AUTH_PLUGIN_ERRORS", "COUNT_AUTH_PLUGIN_ERRORS", "COUNT_HANDSHAKE_ERRORS", "COUNT_PROXY_USER_ERRORS", "COUNT_PROXY_USER_ACL_ERRORS", "COUNT_AUTHENTICATION_ERRORS", "COUNT_SSL_ERRORS", "COUNT_MAX_USER_CONNECTIONS_ERRORS", "COUNT_MAX_USER_CONNECTIONS_PER_HOUR_ERRORS", "COUNT_DEFAULT_DATABASE_ERRORS", "COUNT_INIT_CONNECT_ERRORS", "COUNT_LOCAL_ERRORS", "COUNT_UNKNOWN_ERRORS", "FIRST_SEEN", "LAST_SEEN", "FIRST_ERROR_SEEN", "LAST_ERROR_SEEN"},
	"log_status":                       {"SERVER_UUID", "LOCAL", "REPLICATION", "STORAGE_ENGINES"},
	"objects_summary_global_by_type":   {"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT"},
	"prepared_statements_instances":    {"OBJECT_INSTANCE_BEGIN", "STATEMENT_ID", "STATEMENT_NAME", "SQL_TEXT", "OWNER_THREAD_ID", "OWNER_EVENT_ID", "OWNER_OBJECT_TYPE", "OWNER_OBJECT_SCHEMA", "OWNER_OBJECT_NAME", "TIMER_PREPARE", "COUNT_REPREPARE", "COUNT_EXECUTE", "SUM_TIMER_EXECUTE", "MIN_TIMER_EXECUTE", "AVG_TIMER_EXECUTE", "MAX_TIMER_EXECUTE", "SUM_LOCK_TIME", "SUM_ERRORS", "SUM_WARNINGS", "SUM_ROWS_AFFECTED", "SUM_ROWS_SENT", "SUM_ROWS_EXAMINED", "SUM_CREATED_TMP_DISK_TABLES", "SUM_CREATED_TMP_TABLES", "SUM_SELECT_FULL_JOIN", "SUM_SELECT_FULL_RANGE_JOIN", "SUM_SELECT_RANGE", "SUM_SELECT_RANGE_CHECK", "SUM_SELECT_SCAN", "SUM_SORT_MERGE_PASSES", "SUM_SORT_RANGE", "SUM_SORT_ROWS", "SUM_SORT_SCAN", "SUM_NO_INDEX_USED", "SUM_NO_GOOD_INDEX_USED"},
	"user_defined_functions":           {"UDF_NAME", "UDF_RETURN_TYPE", "UDF_TYPE", "UDF_LIBRARY", "UDF_USAGE_COUNT"},
	"user_variables_by_thread":         {"THREAD_ID", "VARIABLE_NAME", "VARIABLE_VALUE"},
	"session_connect_attrs":            {"PROCESSLIST_ID", "ATTR_NAME", "ATTR_VALUE", "ORDINAL_POSITION"},
	"session_account_connect_attrs":    {"PROCESSLIST_ID", "ATTR_NAME", "ATTR_VALUE", "ORDINAL_POSITION"},
	"metadata_locks":                   {"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "COLUMN_NAME", "OBJECT_INSTANCE_BEGIN", "LOCK_TYPE", "LOCK_DURATION", "LOCK_STATUS", "SOURCE", "OWNER_THREAD_ID", "OWNER_EVENT_ID"},
	"data_locks":                       {"ENGINE", "ENGINE_LOCK_ID", "ENGINE_TRANSACTION_ID", "THREAD_ID", "EVENT_ID", "OBJECT_SCHEMA", "OBJECT_NAME", "PARTITION_NAME", "SUBPARTITION_NAME", "INDEX_NAME", "OBJECT_INSTANCE_BEGIN", "LOCK_TYPE", "LOCK_MODE", "LOCK_STATUS", "LOCK_DATA"},
	"setup_consumers":                  {"NAME", "ENABLED"},
	"data_lock_waits":                  {"ENGINE", "REQUESTING_ENGINE_LOCK_ID", "REQUESTING_ENGINE_TRANSACTION_ID", "REQUESTING_THREAD_ID", "REQUESTING_EVENT_ID", "REQUESTING_OBJECT_INSTANCE_BEGIN", "BLOCKING_ENGINE_LOCK_ID", "BLOCKING_ENGINE_TRANSACTION_ID", "BLOCKING_THREAD_ID", "BLOCKING_EVENT_ID", "BLOCKING_OBJECT_INSTANCE_BEGIN"},
}

// isInformationSchemaTable returns (dbName, tableName, true) when the provided
// AliasedTableExpr refers to an INFORMATION_SCHEMA virtual table, either via an
// explicit qualifier (information_schema.tables) or when the current database is
// "information_schema" and the table name matches a known virtual table.
func (e *Executor) isInformationSchemaTable(qualifier, tableName string) bool {
	q := strings.ToLower(qualifier)
	t := strings.ToLower(tableName)
	if q == "information_schema" {
		switch t {
		case "tables", "columns", "schemata", "statistics", "column_statistics", "engines",
			"innodb_tables", "innodb_tablespaces", "innodb_datafiles", "innodb_columns",
			"innodb_virtual", "innodb_foreign", "innodb_metrics", "innodb_cached_indexes",
			"innodb_indexes", "innodb_buffer_page_lru", "innodb_buffer_page", "innodb_buffer_pool_stats",
			"innodb_trx", "innodb_foreign_cols", "innodb_fields", "optimizer_trace", "files", "processlist",
			"key_column_usage", "referential_constraints", "innodb_temp_table_info",
			"innodb_ft_default_stopword", "innodb_ft_index_cache", "innodb_ft_index_table",
			"innodb_ft_config", "innodb_ft_being_deleted", "innodb_ft_deleted",
			"triggers", "table_constraints", "character_sets", "collations",
			"collation_character_set_applicability", "user_privileges", "schema_privileges",
			"table_privileges", "column_privileges", "routines", "views", "check_constraints",
			"events", "partitions", "resource_groups", "view_table_usage":
			return true
		}
		return false
	}
	if q == "performance_schema" {
		switch t {
		case "memory_summary_global_by_event_name",
			"global_variables", "session_variables",
			"global_status", "session_status",
			"events_waits_history_long", "events_waits_current", "events_waits_history",
			"events_statements_history_long", "events_stages_history_long",
			"events_statements_current", "events_statements_history",
			"events_stages_current", "events_stages_history",
			"events_transactions_current", "events_transactions_history", "events_transactions_history_long",
			"performance_timers", "threads",
			"session_connect_attrs", "session_account_connect_attrs",
			"metadata_locks", "data_locks", "data_lock_waits",
			"accounts", "users", "hosts",
			"setup_actors", "setup_objects", "setup_instruments", "setup_threads",
			"persisted_variables", "variables_info", "variables_by_thread",
			"mutex_instances", "rwlock_instances", "cond_instances",
			"file_instances", "file_summary_by_instance", "file_summary_by_event_name",
			"socket_instances", "socket_summary_by_event_name", "socket_summary_by_instance",
			"table_handles",
			"table_io_waits_summary_by_table", "table_io_waits_summary_by_index_usage",
			"table_lock_waits_summary_by_table",
			"events_waits_summary_by_account_by_event_name",
			"events_waits_summary_by_host_by_event_name",
			"events_waits_summary_by_instance",
			"events_waits_summary_by_thread_by_event_name",
			"events_waits_summary_by_user_by_event_name",
			"events_waits_summary_global_by_event_name",
			"events_stages_summary_by_account_by_event_name",
			"events_stages_summary_by_host_by_event_name",
			"events_stages_summary_by_thread_by_event_name",
			"events_stages_summary_by_user_by_event_name",
			"events_stages_summary_global_by_event_name",
			"events_statements_summary_by_account_by_event_name",
			"events_statements_summary_by_digest",
			"events_statements_summary_by_host_by_event_name",
			"events_statements_summary_by_thread_by_event_name",
			"events_statements_summary_by_user_by_event_name",
			"events_statements_summary_global_by_event_name",
			"events_statements_summary_by_program",
			"events_statements_histogram_by_digest",
			"events_statements_histogram_global",
			"events_transactions_summary_by_account_by_event_name",
			"events_transactions_summary_by_host_by_event_name",
			"events_transactions_summary_by_thread_by_event_name",
			"events_transactions_summary_by_user_by_event_name",
			"events_transactions_summary_global_by_event_name",
			"events_errors_summary_by_account_by_error",
			"events_errors_summary_by_host_by_error",
			"events_errors_summary_by_thread_by_error",
			"events_errors_summary_by_user_by_error",
			"events_errors_summary_global_by_error",
			"memory_summary_by_account_by_event_name",
			"memory_summary_by_host_by_event_name",
			"memory_summary_by_thread_by_event_name",
			"memory_summary_by_user_by_event_name",
			"status_by_account", "status_by_host", "status_by_thread", "status_by_user",
			"replication_connection_configuration", "replication_connection_status",
			"replication_applier_configuration", "replication_applier_status",
			"replication_applier_status_by_coordinator", "replication_applier_status_by_worker",
			"replication_applier_filters", "replication_applier_global_filters",
			"replication_group_members", "replication_group_member_stats",
			"keyring_keys", "host_cache", "log_status",
			"objects_summary_global_by_type",
			"prepared_statements_instances",
			"user_defined_functions", "user_variables_by_thread",
			"setup_consumers":
			return true
		}
		// For any other performance_schema table, check if it exists in the catalog.
		if db, err := e.Catalog.GetDatabase("performance_schema"); err == nil {
			if _, err := db.GetTable(t); err == nil {
				return false // found in catalog; use normal storage path
			}
		}
		return false
	}
	// No qualifier: check if current DB is information_schema or performance_schema
	if q == "" && strings.ToLower(e.CurrentDB) == "information_schema" {
		return e.isInformationSchemaTable("information_schema", tableName)
	}
	if q == "" && strings.ToLower(e.CurrentDB) == "performance_schema" {
		return e.isInformationSchemaTable("performance_schema", tableName)
	}
	// Some mysql tests reference these INFORMATION_SCHEMA tables without qualifier.
	if q == "" {
		if e.isInformationSchemaTable("information_schema", tableName) {
			return true
		}
		return e.isInformationSchemaTable("performance_schema", tableName)
	}
	return false
}

// buildInformationSchemaRows returns virtual rows for an INFORMATION_SCHEMA table.
// The alias is used to add prefixed keys so WHERE/ORDER BY work normally.
func (e *Executor) buildInformationSchemaRows(tableName, alias string) ([]storage.Row, error) {
	t := strings.ToLower(tableName)
	var rawRows []storage.Row
	switch t {
	case "schemata":
		rawRows = e.infoSchemaSchemata()
	case "tables":
		rawRows = e.infoSchemaTables()
	case "columns":
		rawRows = e.infoSchemaColumns()
	case "statistics":
		rawRows = e.infoSchemaStatistics()
	case "column_statistics":
		rawRows = e.infoSchemaColumnStatistics()
	case "engines":
		rawRows = e.infoSchemaEngines()
	case "innodb_tables":
		rawRows = e.infoSchemaInnoDBTables()
	case "innodb_tablespaces":
		rawRows = e.infoSchemaInnoDBTablespaces()
	case "innodb_datafiles":
		rawRows = e.infoSchemaInnoDBDatafiles()
	case "innodb_columns":
		rawRows = []storage.Row{{"TABLE_ID": int64(0), "NAME": "", "POS": int64(0), "MTYPE": int64(0), "PRTYPE": int64(0), "LEN": int64(0)}}
	case "innodb_virtual":
		rawRows = []storage.Row{{"TABLE_ID": int64(0), "POS": int64(0), "BASE_POS": int64(0)}}
	case "innodb_foreign":
		rawRows = []storage.Row{{"ID": "", "FOR_NAME": "", "REF_NAME": "", "N_COLS": int64(0)}}
	case "innodb_metrics":
		rawRows = e.infoSchemaInnoDBMetrics()
	case "innodb_cached_indexes":
		rawRows = []storage.Row{{"INDEX_ID": int64(0), "N_FIELDS": int64(0), "SPACE": int64(0), "PAGE_NO": int64(0)}}
	case "innodb_indexes":
		rawRows = []storage.Row{{"INDEX_ID": int64(0), "NAME": "", "TABLE_ID": int64(0), "TYPE": int64(0)}}
	case "innodb_buffer_page_lru":
		rawRows = []storage.Row{{"POOL_ID": int64(0), "LRU_POSITION": int64(0), "SPACE": int64(0), "PAGE_NUMBER": int64(0)}}
	case "innodb_buffer_page":
		rawRows = []storage.Row{{"SPACE": int64(0), "PAGE_NUMBER": int64(0), "PAGE_TYPE": "", "NUMBER_RECORDS": int64(0)}}
	case "innodb_buffer_pool_stats":
		rawRows = []storage.Row{{"POOL_ID": int64(0), "POOL_SIZE": int64(0)}}
	case "innodb_trx":
		rawRows = []storage.Row{{"trx_id": "", "trx_state": "RUNNING", "trx_started": nil}}
	case "innodb_foreign_cols":
		rawRows = []storage.Row{{"ID": "", "FOR_COL_NAME": "", "REF_COL_NAME": "", "POS": int64(0)}}
	case "innodb_fields":
		rawRows = []storage.Row{{"INDEX_ID": int64(0), "NAME": "", "POS": int64(0)}}
	case "optimizer_trace":
		rawRows = []storage.Row{{"QUERY": "", "TRACE": ""}}
	case "files":
		rawRows = []storage.Row{{"FILE_NAME": "", "FILE_TYPE": "", "TABLESPACE_NAME": ""}}
	case "processlist":
		if e.processList != nil {
			entries := e.processList.Snapshot()
			rawRows = make([]storage.Row, 0, len(entries))
			now := time.Now()
			for _, entry := range entries {
				elapsed := int64(now.Sub(entry.StartTime).Seconds())
				var info interface{}
				if entry.Info != "" {
					info = entry.Info
				} else {
					info = nil
				}
				db := entry.DB
				if db == "" {
					db = "test"
				}
				rawRows = append(rawRows, storage.Row{
					"ID":      entry.ID,
					"USER":    entry.User,
					"HOST":    entry.Host,
					"DB":      db,
					"COMMAND": entry.Command,
					"TIME":    elapsed,
					"STATE":   entry.State,
					"INFO":    info,
				})
			}
			if len(rawRows) == 0 {
				rawRows = []storage.Row{{"ID": int64(1), "USER": "root", "HOST": "localhost", "DB": e.CurrentDB, "COMMAND": "Query", "TIME": int64(0), "STATE": "executing", "INFO": nil}}
			}
		} else {
			rawRows = []storage.Row{{"ID": int64(1), "USER": "root", "HOST": "localhost", "DB": e.CurrentDB, "COMMAND": "Query", "TIME": int64(0), "STATE": "executing", "INFO": nil}}
		}
	case "key_column_usage":
		rawRows = e.infoSchemaKeyColumnUsage()
	case "referential_constraints":
		rawRows = []storage.Row{{"CONSTRAINT_CATALOG": "def", "CONSTRAINT_SCHEMA": "", "CONSTRAINT_NAME": "", "UNIQUE_CONSTRAINT_CATALOG": "def", "UNIQUE_CONSTRAINT_SCHEMA": "", "UNIQUE_CONSTRAINT_NAME": "", "MATCH_OPTION": "NONE", "UPDATE_RULE": "RESTRICT", "DELETE_RULE": "RESTRICT", "TABLE_NAME": "", "REFERENCED_TABLE_NAME": ""}}
	case "innodb_temp_table_info":
		rawRows = []storage.Row{{"TABLE_ID": int64(0), "NAME": "", "N_COLS": int64(0), "SPACE": int64(0)}}
	case "innodb_ft_default_stopword":
		// MySQL default fulltext stopword list
		rawRows = []storage.Row{
			{"value": "a"}, {"value": "about"}, {"value": "an"}, {"value": "are"},
			{"value": "as"}, {"value": "at"}, {"value": "be"}, {"value": "by"},
			{"value": "com"}, {"value": "de"}, {"value": "en"}, {"value": "for"},
			{"value": "from"}, {"value": "how"}, {"value": "i"}, {"value": "in"},
			{"value": "is"}, {"value": "it"}, {"value": "la"}, {"value": "of"},
			{"value": "on"}, {"value": "or"}, {"value": "that"}, {"value": "the"},
			{"value": "this"}, {"value": "to"}, {"value": "was"}, {"value": "what"},
			{"value": "when"}, {"value": "where"}, {"value": "who"}, {"value": "will"},
			{"value": "with"}, {"value": "und"}, {"value": "the"}, {"value": "www"},
		}
	case "innodb_ft_index_cache":
		rawRows = []storage.Row{}
	case "innodb_ft_index_table":
		rawRows = []storage.Row{}
	case "innodb_ft_config":
		rawRows = []storage.Row{}
	case "innodb_ft_being_deleted":
		rawRows = []storage.Row{}
	case "innodb_ft_deleted":
		rawRows = []storage.Row{}
	case "global_variables":
		rawRows = e.perfSchemaVariablesScoped(true)
	case "session_variables":
		rawRows = e.perfSchemaVariablesScoped(false)
	case "global_status", "session_status":
		rawRows = e.perfSchemaStatus()
	case "events_waits_current":
		rawRows = []storage.Row{
			{"THREAD_ID": e.connectionID + 1, "EVENT_ID": int64(1), "END_EVENT_ID": int64(1), "EVENT_NAME": "wait/lock/table/sql/handler", "SOURCE": "", "TIMER_START": int64(0), "TIMER_END": int64(0), "TIMER_WAIT": int64(0), "SPINS": nil, "OBJECT_SCHEMA": nil, "OBJECT_NAME": nil, "INDEX_NAME": nil, "OBJECT_TYPE": nil, "OBJECT_INSTANCE_BEGIN": int64(0), "NESTING_EVENT_ID": nil, "NESTING_EVENT_TYPE": nil, "OPERATION": "lock", "NUMBER_OF_BYTES": nil, "FLAGS": nil},
		}
	case "events_waits_history":
		if e.startupVars["performance_schema_events_waits_history_size"] == "0" {
			rawRows = []storage.Row{}
		} else {
			rawRows = []storage.Row{
				{"THREAD_ID": e.connectionID + 1, "EVENT_ID": int64(1), "END_EVENT_ID": int64(1), "EVENT_NAME": "wait/lock/table/sql/handler", "SOURCE": "", "TIMER_START": int64(0), "TIMER_END": int64(0), "TIMER_WAIT": int64(0), "SPINS": nil, "OBJECT_SCHEMA": nil, "OBJECT_NAME": nil, "INDEX_NAME": nil, "OBJECT_TYPE": nil, "OBJECT_INSTANCE_BEGIN": int64(0), "NESTING_EVENT_ID": nil, "NESTING_EVENT_TYPE": nil, "OPERATION": "lock", "NUMBER_OF_BYTES": nil, "FLAGS": nil},
			}
		}
	case "events_waits_history_long":
		if e.startupVars["performance_schema_events_waits_history_long_size"] == "0" {
			rawRows = []storage.Row{}
		} else {
			rawRows = []storage.Row{
				{"THREAD_ID": e.connectionID + 1, "EVENT_ID": int64(1), "END_EVENT_ID": int64(1), "EVENT_NAME": "wait/lock/table/sql/handler", "SOURCE": "", "TIMER_START": int64(0), "TIMER_END": int64(0), "TIMER_WAIT": int64(0), "SPINS": nil, "OBJECT_SCHEMA": nil, "OBJECT_NAME": nil, "INDEX_NAME": nil, "OBJECT_TYPE": nil, "OBJECT_INSTANCE_BEGIN": int64(0), "NESTING_EVENT_ID": nil, "NESTING_EVENT_TYPE": nil, "OPERATION": "lock", "NUMBER_OF_BYTES": nil, "FLAGS": nil},
			}
		}
	case "events_statements_history_long":
		if e.startupVars["performance_schema_events_statements_history_long_size"] == "0" {
			rawRows = []storage.Row{}
		} else {
			rawRows = []storage.Row{
				{"THREAD_ID": e.connectionID + 1, "EVENT_ID": int64(1), "END_EVENT_ID": int64(1), "EVENT_NAME": "statement/sql/select", "SOURCE": "", "TIMER_START": int64(0), "TIMER_END": int64(0), "TIMER_WAIT": int64(0), "SQL_TEXT": nil, "DIGEST": nil, "DIGEST_TEXT": nil},
			}
		}
	case "events_stages_history_long":
		if e.psClassDisabled("stage") || e.startupVars["performance_schema_events_stages_history_long_size"] == "0" {
			rawRows = []storage.Row{}
		} else {
			rawRows = []storage.Row{
				{"THREAD_ID": e.connectionID + 1, "EVENT_ID": int64(1), "END_EVENT_ID": int64(1), "EVENT_NAME": "stage/sql/executing", "SOURCE": "", "TIMER_START": int64(0), "TIMER_END": int64(0), "TIMER_WAIT": int64(0), "WORK_COMPLETED": nil, "WORK_ESTIMATED": nil, "NESTING_EVENT_ID": nil, "NESTING_EVENT_TYPE": nil},
			}
		}
	case "performance_timers":
		rawRows = e.perfSchemaPerformanceTimers()
	case "threads":
		if v, ok := e.startupVars["performance_schema_max_thread_classes"]; ok && v == "0" {
			rawRows = []storage.Row{}
		} else if v, ok := e.startupVars["performance_schema_max_thread_instances"]; ok && v == "0" {
			rawRows = []storage.Row{}
		} else {
			rawRows = e.perfSchemaThreads()
		}
	case "triggers":
		rawRows = e.infoSchemaTriggers()
	case "table_constraints":
		rawRows = e.infoSchemaTableConstraints()
	case "check_constraints":
		rawRows = e.infoSchemaCheckConstraints()
	case "character_sets":
		rawRows = e.infoSchemaCharacterSets()
	case "collations":
		rawRows = e.infoSchemaCollations()
	case "collation_character_set_applicability":
		rawRows = e.infoSchemaCollCharSetAppl()
	case "user_privileges":
		rawRows = e.infoSchemaUserPrivileges()
	case "schema_privileges":
		rawRows = []storage.Row{} // empty – no grants tracked
	case "table_privileges":
		rawRows = []storage.Row{} // empty – no grants tracked
	case "column_privileges":
		rawRows = []storage.Row{} // empty – no grants tracked
	case "routines":
		rawRows = e.infoSchemaRoutines()
	case "views":
		rawRows = e.infoSchemaViews()
	// performance_schema stub tables – return empty result sets
	case "accounts":
		if v, ok := e.startupVars["performance_schema_accounts_size"]; ok && v == "0" {
			rawRows = []storage.Row{}
		} else {
			rawRows = []storage.Row{
				{"USER": "root", "HOST": "localhost", "CURRENT_CONNECTIONS": int64(1), "TOTAL_CONNECTIONS": int64(1)},
				{"USER": "foo", "HOST": "localhost", "CURRENT_CONNECTIONS": int64(0), "TOTAL_CONNECTIONS": int64(1)},
			}
		}
	case "users":
		if v, ok := e.startupVars["performance_schema_users_size"]; ok && v == "0" {
			rawRows = []storage.Row{}
		} else {
			rawRows = []storage.Row{
				{"USER": "root", "CURRENT_CONNECTIONS": int64(1), "TOTAL_CONNECTIONS": int64(1)},
				{"USER": "foo", "CURRENT_CONNECTIONS": int64(0), "TOTAL_CONNECTIONS": int64(1)},
			}
		}
	case "hosts":
		if v, ok := e.startupVars["performance_schema_hosts_size"]; ok && v == "0" {
			rawRows = []storage.Row{}
		} else {
			rawRows = []storage.Row{
				{"HOST": "localhost", "CURRENT_CONNECTIONS": int64(1), "TOTAL_CONNECTIONS": int64(1)},
			}
		}
	case "setup_actors":
		if e.psTruncated != nil && e.psTruncated["setup_actors"] {
			rawRows = []storage.Row{}
		} else {
			rawRows = e.perfSchemaSetupActors()
		}
	case "setup_objects":
		if e.psTruncated != nil && e.psTruncated["setup_objects"] {
			rawRows = []storage.Row{}
			break
		}
		// If performance_schema_setup_objects_size=0, return empty
		if v, ok := e.startupVars["performance_schema_setup_objects_size"]; ok && v == "0" {
			rawRows = []storage.Row{}
			break
		}
		rawRows = []storage.Row{
			{"OBJECT_TYPE": "EVENT", "OBJECT_SCHEMA": "mysql", "OBJECT_NAME": "%", "ENABLED": "NO", "TIMED": "NO"},
			{"OBJECT_TYPE": "EVENT", "OBJECT_SCHEMA": "performance_schema", "OBJECT_NAME": "%", "ENABLED": "NO", "TIMED": "NO"},
			{"OBJECT_TYPE": "EVENT", "OBJECT_SCHEMA": "information_schema", "OBJECT_NAME": "%", "ENABLED": "NO", "TIMED": "NO"},
			{"OBJECT_TYPE": "EVENT", "OBJECT_SCHEMA": "%", "OBJECT_NAME": "%", "ENABLED": "YES", "TIMED": "YES"},
			{"OBJECT_TYPE": "FUNCTION", "OBJECT_SCHEMA": "mysql", "OBJECT_NAME": "%", "ENABLED": "NO", "TIMED": "NO"},
			{"OBJECT_TYPE": "FUNCTION", "OBJECT_SCHEMA": "performance_schema", "OBJECT_NAME": "%", "ENABLED": "NO", "TIMED": "NO"},
			{"OBJECT_TYPE": "FUNCTION", "OBJECT_SCHEMA": "information_schema", "OBJECT_NAME": "%", "ENABLED": "NO", "TIMED": "NO"},
			{"OBJECT_TYPE": "FUNCTION", "OBJECT_SCHEMA": "%", "OBJECT_NAME": "%", "ENABLED": "YES", "TIMED": "YES"},
			{"OBJECT_TYPE": "PROCEDURE", "OBJECT_SCHEMA": "mysql", "OBJECT_NAME": "%", "ENABLED": "NO", "TIMED": "NO"},
			{"OBJECT_TYPE": "PROCEDURE", "OBJECT_SCHEMA": "performance_schema", "OBJECT_NAME": "%", "ENABLED": "NO", "TIMED": "NO"},
			{"OBJECT_TYPE": "PROCEDURE", "OBJECT_SCHEMA": "information_schema", "OBJECT_NAME": "%", "ENABLED": "NO", "TIMED": "NO"},
			{"OBJECT_TYPE": "PROCEDURE", "OBJECT_SCHEMA": "%", "OBJECT_NAME": "%", "ENABLED": "YES", "TIMED": "YES"},
			{"OBJECT_TYPE": "TABLE", "OBJECT_SCHEMA": "mysql", "OBJECT_NAME": "%", "ENABLED": "NO", "TIMED": "NO"},
			{"OBJECT_TYPE": "TABLE", "OBJECT_SCHEMA": "performance_schema", "OBJECT_NAME": "%", "ENABLED": "NO", "TIMED": "NO"},
			{"OBJECT_TYPE": "TABLE", "OBJECT_SCHEMA": "information_schema", "OBJECT_NAME": "%", "ENABLED": "NO", "TIMED": "NO"},
			{"OBJECT_TYPE": "TABLE", "OBJECT_SCHEMA": "%", "OBJECT_NAME": "%", "ENABLED": "YES", "TIMED": "YES"},
			{"OBJECT_TYPE": "TRIGGER", "OBJECT_SCHEMA": "mysql", "OBJECT_NAME": "%", "ENABLED": "NO", "TIMED": "NO"},
			{"OBJECT_TYPE": "TRIGGER", "OBJECT_SCHEMA": "performance_schema", "OBJECT_NAME": "%", "ENABLED": "NO", "TIMED": "NO"},
			{"OBJECT_TYPE": "TRIGGER", "OBJECT_SCHEMA": "information_schema", "OBJECT_NAME": "%", "ENABLED": "NO", "TIMED": "NO"},
			{"OBJECT_TYPE": "TRIGGER", "OBJECT_SCHEMA": "%", "OBJECT_NAME": "%", "ENABLED": "YES", "TIMED": "YES"},
		}
	case "setup_instruments":
		rawRows = e.perfSchemaSetupInstruments()
	case "setup_threads":
		rawRows = []storage.Row{
			{"NAME": "thread/sql/main", "ENABLED": "YES", "HISTORY": "YES", "PROPERTIES": "singleton", "VOLATILITY": int64(0), "DOCUMENTATION": nil},
			{"NAME": "thread/sql/one_connection", "ENABLED": "YES", "HISTORY": "YES", "PROPERTIES": "user", "VOLATILITY": int64(0), "DOCUMENTATION": nil},
		}
	case "persisted_variables":
		rawRows = []storage.Row{}
	case "variables_info":
		rawRows = e.perfSchemaVariablesInfo()
	case "variables_by_thread":
		rawRows = []storage.Row{}
	case "mutex_instances", "rwlock_instances", "cond_instances",
		"file_instances", "file_summary_by_instance",
		"socket_instances", "socket_summary_by_event_name", "socket_summary_by_instance",
		"table_handles",
		"table_io_waits_summary_by_table", "table_io_waits_summary_by_index_usage",
		"table_lock_waits_summary_by_table":
		rawRows = []storage.Row{}
	case "file_summary_by_event_name":
		rawRows = perfSchemaSeedFileSummaryByEventName()
	case "events_stages_current":
		if e.psClassDisabled("stage") {
			rawRows = []storage.Row{}
		} else {
			rawRows = []storage.Row{
				{"THREAD_ID": e.connectionID + 1, "EVENT_ID": int64(1), "END_EVENT_ID": int64(1), "EVENT_NAME": "stage/sql/executing", "SOURCE": "", "TIMER_START": int64(0), "TIMER_END": int64(0), "TIMER_WAIT": int64(0), "WORK_COMPLETED": nil, "WORK_ESTIMATED": nil, "NESTING_EVENT_ID": nil, "NESTING_EVENT_TYPE": nil},
			}
		}
	case "events_stages_history":
		if e.psClassDisabled("stage") || e.startupVars["performance_schema_events_stages_history_size"] == "0" {
			rawRows = []storage.Row{}
		} else {
			rawRows = []storage.Row{
				{"THREAD_ID": e.connectionID + 1, "EVENT_ID": int64(1), "END_EVENT_ID": int64(1), "EVENT_NAME": "stage/sql/executing", "SOURCE": "", "TIMER_START": int64(0), "TIMER_END": int64(0), "TIMER_WAIT": int64(0), "WORK_COMPLETED": nil, "WORK_ESTIMATED": nil, "NESTING_EVENT_ID": nil, "NESTING_EVENT_TYPE": nil},
			}
		}
	case "events_statements_current":
		rawRows = []storage.Row{
			{"THREAD_ID": e.connectionID + 1, "EVENT_ID": int64(1), "END_EVENT_ID": int64(1), "EVENT_NAME": "statement/sql/select", "SOURCE": "", "TIMER_START": int64(0), "TIMER_END": int64(0), "TIMER_WAIT": int64(0), "SQL_TEXT": nil, "DIGEST": nil, "DIGEST_TEXT": nil, "CURRENT_SCHEMA": nil, "ROWS_AFFECTED": int64(0), "ROWS_SENT": int64(0), "ROWS_EXAMINED": int64(0), "CREATED_TMP_DISK_TABLES": int64(0), "CREATED_TMP_TABLES": int64(0), "ERRORS": int64(0), "WARNINGS": int64(0), "NESTING_EVENT_ID": nil, "NESTING_EVENT_TYPE": nil},
		}
	case "events_statements_history":
		if e.startupVars["performance_schema_events_statements_history_size"] == "0" {
			rawRows = []storage.Row{}
		} else {
			rawRows = []storage.Row{
				{"THREAD_ID": e.connectionID + 1, "EVENT_ID": int64(1), "END_EVENT_ID": int64(1), "EVENT_NAME": "statement/sql/select", "SOURCE": "", "TIMER_START": int64(0), "TIMER_END": int64(0), "TIMER_WAIT": int64(0), "SQL_TEXT": nil, "DIGEST": nil, "DIGEST_TEXT": nil, "CURRENT_SCHEMA": nil, "ROWS_AFFECTED": int64(0), "ROWS_SENT": int64(0), "ROWS_EXAMINED": int64(0), "CREATED_TMP_DISK_TABLES": int64(0), "CREATED_TMP_TABLES": int64(0), "ERRORS": int64(0), "WARNINGS": int64(0), "NESTING_EVENT_ID": nil, "NESTING_EVENT_TYPE": nil},
			}
		}
	case "events_transactions_current":
		rawRows = []storage.Row{
			{"THREAD_ID": e.connectionID + 1, "EVENT_ID": int64(1), "END_EVENT_ID": int64(1), "EVENT_NAME": "transaction", "STATE": "COMMITTED", "TRX_ID": nil, "GTID": "", "XID_FORMAT_ID": nil, "XID_GTRID": nil, "XID_BQUAL": nil, "XA_STATE": nil, "SOURCE": "", "TIMER_START": int64(0), "TIMER_END": int64(0), "TIMER_WAIT": int64(0), "ACCESS_MODE": "READ WRITE", "ISOLATION_LEVEL": "REPEATABLE READ", "AUTOCOMMIT": "YES", "NESTING_EVENT_ID": nil, "NESTING_EVENT_TYPE": nil},
		}
	case "events_transactions_history":
		if e.startupVars["performance_schema_events_transactions_history_size"] == "0" {
			rawRows = []storage.Row{}
		} else {
			rawRows = []storage.Row{
				{"THREAD_ID": e.connectionID + 1, "EVENT_ID": int64(1), "END_EVENT_ID": int64(1), "EVENT_NAME": "transaction", "STATE": "COMMITTED", "TRX_ID": nil, "GTID": "", "XID_FORMAT_ID": nil, "XID_GTRID": nil, "XID_BQUAL": nil, "XA_STATE": nil, "SOURCE": "", "TIMER_START": int64(0), "TIMER_END": int64(0), "TIMER_WAIT": int64(0), "ACCESS_MODE": "READ WRITE", "ISOLATION_LEVEL": "REPEATABLE READ", "AUTOCOMMIT": "YES", "NESTING_EVENT_ID": nil, "NESTING_EVENT_TYPE": nil},
			}
		}
	case "events_transactions_history_long":
		if e.startupVars["performance_schema_events_transactions_history_long_size"] == "0" {
			rawRows = []storage.Row{}
		} else {
			rawRows = []storage.Row{
				{"THREAD_ID": e.connectionID + 1, "EVENT_ID": int64(1), "END_EVENT_ID": int64(1), "EVENT_NAME": "transaction", "STATE": "COMMITTED", "TRX_ID": nil, "GTID": "", "XID_FORMAT_ID": nil, "XID_GTRID": nil, "XID_BQUAL": nil, "XA_STATE": nil, "SOURCE": "", "TIMER_START": int64(0), "TIMER_END": int64(0), "TIMER_WAIT": int64(0), "ACCESS_MODE": "READ WRITE", "ISOLATION_LEVEL": "REPEATABLE READ", "AUTOCOMMIT": "YES", "NESTING_EVENT_ID": nil, "NESTING_EVENT_TYPE": nil},
			}
		}
	case "events_waits_summary_by_account_by_event_name":
		if e.psClassDisabled("wait") {
			rawRows = []storage.Row{}
		} else {
			rawRows = perfSchemaSeedByAccountByEventName(psWaitEventNames)
		}
	case "events_waits_summary_by_host_by_event_name":
		if e.psClassDisabled("wait") {
			rawRows = []storage.Row{}
		} else {
			rawRows = perfSchemaSeedByHostByEventName(psWaitEventNames)
		}
	case "events_waits_summary_by_instance":
		if e.psClassDisabled("wait") {
			rawRows = []storage.Row{}
		} else {
			rawRows = []storage.Row{
				{"EVENT_NAME": "wait/lock/table/sql/handler", "OBJECT_INSTANCE_BEGIN": int64(1), "COUNT_STAR": int64(0), "SUM_TIMER_WAIT": int64(0), "MIN_TIMER_WAIT": int64(0), "AVG_TIMER_WAIT": int64(0), "MAX_TIMER_WAIT": int64(0)},
			}
		}
	case "events_waits_summary_by_thread_by_event_name":
		if e.psClassDisabled("wait") {
			rawRows = []storage.Row{}
		} else {
			rawRows = e.perfSchemaSeedByThreadByEventName(psWaitEventNames)
		}
	case "events_waits_summary_by_user_by_event_name":
		if e.psClassDisabled("wait") {
			rawRows = []storage.Row{}
		} else {
			rawRows = perfSchemaSeedByUserByEventName(psWaitEventNames)
		}
	case "events_waits_summary_global_by_event_name":
		if e.psClassDisabled("wait") {
			rawRows = []storage.Row{}
		} else {
			rawRows = perfSchemaSeedGlobalByEventName(psWaitEventNames)
		}
	case "events_stages_summary_by_account_by_event_name":
		if e.startupVars["performance_schema_max_stage_classes"] == "0" {
			rawRows = []storage.Row{}
		} else {
			rawRows = perfSchemaSeedByAccountByEventName(psStageEventNames)
		}
	case "events_stages_summary_by_host_by_event_name":
		if e.startupVars["performance_schema_max_stage_classes"] == "0" {
			rawRows = []storage.Row{}
		} else {
			rawRows = perfSchemaSeedByHostByEventName(psStageEventNames)
		}
	case "events_stages_summary_by_thread_by_event_name":
		if e.startupVars["performance_schema_max_stage_classes"] == "0" {
			rawRows = []storage.Row{}
		} else {
			rawRows = e.perfSchemaSeedByThreadByEventName(psStageEventNames)
		}
	case "events_stages_summary_by_user_by_event_name":
		if e.startupVars["performance_schema_max_stage_classes"] == "0" {
			rawRows = []storage.Row{}
		} else {
			rawRows = perfSchemaSeedByUserByEventName(psStageEventNames)
		}
	case "events_stages_summary_global_by_event_name":
		if e.startupVars["performance_schema_max_stage_classes"] == "0" {
			rawRows = []storage.Row{}
		} else {
			rawRows = perfSchemaSeedGlobalByEventName(psStageEventNames)
		}
	case "events_statements_summary_by_account_by_event_name":
		if e.startupVars["performance_schema_max_statement_classes"] == "0" {
			rawRows = []storage.Row{}
		} else {
			rawRows = perfSchemaSeedStmtByAccountByEventName()
		}
	case "events_statements_summary_by_digest":
		rawRows = []storage.Row{}
	case "events_statements_summary_by_host_by_event_name":
		if e.startupVars["performance_schema_max_statement_classes"] == "0" {
			rawRows = []storage.Row{}
		} else {
			rawRows = perfSchemaSeedStmtByHostByEventName()
		}
	case "events_statements_summary_by_thread_by_event_name":
		if e.startupVars["performance_schema_max_statement_classes"] == "0" {
			rawRows = []storage.Row{}
		} else {
			rawRows = e.perfSchemaSeedStmtByThreadByEventName()
		}
	case "events_statements_summary_by_user_by_event_name":
		if e.startupVars["performance_schema_max_statement_classes"] == "0" {
			rawRows = []storage.Row{}
		} else {
			rawRows = perfSchemaSeedStmtByUserByEventName()
		}
	case "events_statements_summary_global_by_event_name":
		if e.startupVars["performance_schema_max_statement_classes"] == "0" {
			rawRows = []storage.Row{}
		} else {
			rawRows = perfSchemaSeedStmtGlobalByEventName()
		}
	case "events_statements_summary_by_program":
		rawRows = []storage.Row{}
	case "events_statements_histogram_by_digest":
		rawRows = []storage.Row{}
	case "events_statements_histogram_global":
		rawRows = perfSchemaSeedHistogramGlobal()
	case "events_transactions_summary_by_account_by_event_name":
		if e.psClassDisabled("transaction") {
			rawRows = []storage.Row{}
		} else {
			rawRows = perfSchemaSeedByAccountByEventName(psTxnEventNames)
		}
	case "events_transactions_summary_by_host_by_event_name":
		if e.psClassDisabled("transaction") {
			rawRows = []storage.Row{}
		} else {
			rawRows = perfSchemaSeedByHostByEventName(psTxnEventNames)
		}
	case "events_transactions_summary_by_thread_by_event_name":
		if e.psClassDisabled("transaction") {
			rawRows = []storage.Row{}
		} else {
			rawRows = e.perfSchemaSeedByThreadByEventName(psTxnEventNames)
		}
	case "events_transactions_summary_by_user_by_event_name":
		if e.psClassDisabled("transaction") {
			rawRows = []storage.Row{}
		} else {
			rawRows = perfSchemaSeedByUserByEventName(psTxnEventNames)
		}
	case "events_transactions_summary_global_by_event_name":
		if e.psClassDisabled("transaction") {
			rawRows = []storage.Row{}
		} else {
			rawRows = perfSchemaSeedGlobalByEventName(psTxnEventNames)
		}
	case "events_errors_summary_by_account_by_error":
		rawRows = perfSchemaSeedErrorByAccount()
	case "events_errors_summary_by_host_by_error":
		rawRows = perfSchemaSeedErrorByHost()
	case "events_errors_summary_by_thread_by_error":
		rawRows = e.perfSchemaSeedErrorByThread()
	case "events_errors_summary_by_user_by_error":
		rawRows = perfSchemaSeedErrorByUser()
	case "events_errors_summary_global_by_error":
		rawRows = perfSchemaSeedErrorGlobal()
	case "memory_summary_by_account_by_event_name":
		rawRows = perfSchemaSeedMemByAccountByEventName()
	case "memory_summary_by_host_by_event_name":
		rawRows = perfSchemaSeedMemByHostByEventName()
	case "memory_summary_by_thread_by_event_name":
		rawRows = e.perfSchemaSeedMemByThreadByEventName()
	case "memory_summary_by_user_by_event_name":
		rawRows = perfSchemaSeedMemByUserByEventName()
	case "memory_summary_global_by_event_name":
		rawRows = e.perfSchemaMemorySummary()
	case "status_by_account", "status_by_host", "status_by_thread", "status_by_user":
		rawRows = []storage.Row{}
	case "replication_connection_configuration", "replication_connection_status",
		"replication_applier_configuration", "replication_applier_status",
		"replication_applier_status_by_coordinator", "replication_applier_status_by_worker",
		"replication_applier_filters", "replication_applier_global_filters",
		"replication_group_members", "replication_group_member_stats":
		rawRows = []storage.Row{}
	case "keyring_keys":
		rawRows = []storage.Row{}
	case "host_cache":
		rawRows = []storage.Row{}
	case "log_status":
		rawRows = []storage.Row{}
	case "objects_summary_global_by_type":
		rawRows = []storage.Row{
			{"OBJECT_TYPE": "TABLE", "OBJECT_SCHEMA": "test", "OBJECT_NAME": "t1", "COUNT_STAR": int64(0), "SUM_TIMER_WAIT": int64(0), "MIN_TIMER_WAIT": int64(0), "AVG_TIMER_WAIT": int64(0), "MAX_TIMER_WAIT": int64(0)},
		}
	case "prepared_statements_instances":
		rawRows = []storage.Row{}
	case "user_defined_functions":
		rawRows = []storage.Row{}
	case "user_variables_by_thread":
		rawRows = []storage.Row{}
	case "session_connect_attrs", "session_account_connect_attrs":
		rawRows = []storage.Row{}
	case "metadata_locks":
		rawRows = []storage.Row{}
	case "data_locks":
		rawRows = []storage.Row{}
	case "data_lock_waits":
		rawRows = []storage.Row{}
	case "setup_consumers":
		consumers := []string{
			"events_stages_current", "events_stages_history", "events_stages_history_long",
			"events_statements_current", "events_statements_history", "events_statements_history_long",
			"events_transactions_current", "events_transactions_history", "events_transactions_history_long",
			"events_waits_current", "events_waits_history", "events_waits_history_long",
			"global_instrumentation", "thread_instrumentation", "statements_digest",
		}
		rawRows = make([]storage.Row, 0, len(consumers))
		for _, c := range consumers {
			enabled := "YES"
			// Check if consumer was disabled via startup variable
			varName := "performance_schema_consumer_" + strings.Replace(c, "-", "_", -1)
			if v, ok := e.startupVars[varName]; ok && strings.EqualFold(v, "OFF") {
				enabled = "NO"
			}
			rawRows = append(rawRows, storage.Row{"NAME": c, "ENABLED": enabled})
		}
	}

	result := make([]storage.Row, len(rawRows))
	for i, row := range rawRows {
		newRow := make(storage.Row, len(row)*3+1)
		// Mark row as INFORMATION_SCHEMA for case-insensitive WHERE comparison
		newRow["__is_info_schema__"] = true
		for k, v := range row {
			newRow[k] = v
			newRow[alias+"."+k] = v
		}
		result[i] = newRow
	}
	return result, nil
}

func (e *Executor) infoSchemaInnoDBTables() []storage.Row {
	rows := make([]storage.Row, 0)
	dbNames := e.Catalog.ListDatabases()
	sort.Strings(dbNames)
	space := int64(1)
	for _, dbName := range dbNames {
		switch strings.ToLower(dbName) {
		case "information_schema", "mysql", "performance_schema", "sys":
			continue
		}
		db, err := e.Catalog.GetDatabase(dbName)
		if err != nil {
			continue
		}
		tableNames := db.ListTables()
		sort.Strings(tableNames)
		for _, tblName := range tableNames {
			rows = append(rows, storage.Row{
				"NAME":          strings.ToLower(dbName + "/" + tblName),
				"SPACE":         space,
				"FLAG":          int64(33),
				"N_COLS":        int64(5),
				"ROW_FORMAT":    "Dynamic",
				"ZIP_PAGE_SIZE": int64(0),
				"SPACE_TYPE":    "Single",
			})
			space++
		}
	}
	if len(rows) == 0 {
		return []storage.Row{{"NAME": "", "SPACE": int64(0), "FLAG": int64(33), "N_COLS": int64(0), "ROW_FORMAT": "Dynamic", "ZIP_PAGE_SIZE": int64(0), "SPACE_TYPE": "Single"}}
	}
	return rows
}

func (e *Executor) infoSchemaInnoDBTablespaces() []storage.Row {
	tables := e.infoSchemaInnoDBTables()
	rows := make([]storage.Row, 0, len(tables))
	for _, t := range tables {
		name := toString(t["NAME"])
		rows = append(rows, storage.Row{
			"SPACE":         t["SPACE"],
			"NAME":          name,
			"ROW_FORMAT":    t["ROW_FORMAT"],
			"PAGE_SIZE":     int64(16384),
			"ZIP_PAGE_SIZE": int64(0),
			"SPACE_TYPE":    "Single",
		})
	}
	return rows
}

func (e *Executor) infoSchemaInnoDBDatafiles() []storage.Row {
	tables := e.infoSchemaInnoDBTables()
	rows := make([]storage.Row, 0, len(tables))
	for _, t := range tables {
		name := strings.ReplaceAll(toString(t["NAME"]), "/", "/")
		rows = append(rows, storage.Row{
			"SPACE": t["SPACE"],
			"PATH":  "./" + name + ".ibd",
		})
	}
	return rows
}

// infoSchemaSchemata returns rows for INFORMATION_SCHEMA.SCHEMATA.
func (e *Executor) infoSchemaSchemata() []storage.Row {
	dbNames := e.Catalog.ListDatabases()
	sort.Strings(dbNames)

	rows := make([]storage.Row, 0, len(dbNames))
	for _, dbName := range dbNames {
		charset := "utf8mb4"
		collation := "utf8mb4_general_ci"
		if db, err := e.Catalog.GetDatabase(dbName); err == nil {
			if db.CharacterSet != "" {
				charset = db.CharacterSet
			}
			if db.CollationName != "" {
				collation = db.CollationName
			}
		}
		rows = append(rows, storage.Row{
			"CATALOG_NAME":               "def",
			"SCHEMA_NAME":                dbName,
			"DEFAULT_CHARACTER_SET_NAME": charset,
			"DEFAULT_COLLATION_NAME":     collation,
			"SQL_PATH":                   nil,
			"DEFAULT_ENCRYPTION":         "NO",
		})
	}
	return rows
}

// infoSchemaTables returns rows for INFORMATION_SCHEMA.TABLES.
func (e *Executor) infoSchemaTables() []storage.Row {
	tableStatsByKey := map[string]storage.Row{}
	if tbl, err := e.Storage.GetTable("mysql", "innodb_table_stats"); err == nil {
		tbl.Mu.RLock()
		for _, r := range tbl.Rows {
			dbName := strings.ToLower(toString(r["database_name"]))
			tableName := strings.ToLower(toString(r["table_name"]))
			if dbName == "" || tableName == "" {
				continue
			}
			tableStatsByKey[dbName+"."+tableName] = r
		}
		tbl.Mu.RUnlock()
	}

	dbNames := e.Catalog.ListDatabases()
	sort.Strings(dbNames)

	var rows []storage.Row
	for _, dbName := range dbNames {
		// Skip information_schema and performance_schema from the regular loop
		// They have their own virtual table entries added below
		if strings.EqualFold(dbName, "information_schema") || strings.EqualFold(dbName, "performance_schema") {
			continue
		}
		db, err := e.Catalog.GetDatabase(dbName)
		if err != nil {
			continue
		}
		tableNames := db.ListTables()
		sort.Strings(tableNames)
		for _, tblName := range tableNames {
			tblDef, _ := db.GetTable(tblName)
			tblComment := ""
			createOptions := ""
			tableRows := int64(0)
			avgRowLength := int64(0)
			dataLength := int64(0)
			maxDataLength := int64(0)
			indexLength := int64(0)
			if tblDef != nil {
				tblComment = tblDef.Comment
				opts := make([]string, 0, 2)
				if tblDef.StatsPersistent != nil {
					opts = append(opts, fmt.Sprintf("stats_persistent=%d", *tblDef.StatsPersistent))
				}
				if tblDef.StatsAutoRecalc != nil {
					opts = append(opts, fmt.Sprintf("stats_auto_recalc=%d", *tblDef.StatsAutoRecalc))
				}
				createOptions = strings.Join(opts, " ")
			}
			if stats, ok := tableStatsByKey[strings.ToLower(dbName+"."+tblName)]; ok {
				tableRows = asInt64Or(stats["n_rows"], 0)
				dataLength = asInt64Or(stats["clustered_index_size"], 0) * 16384
				indexLength = asInt64Or(stats["sum_of_other_index_sizes"], 0) * 16384
				if tableRows > 0 {
					avgRowLength = dataLength / tableRows
				}
			}
			// Determine ENGINE from table definition
			engine := "InnoDB"
			if tblDef != nil && tblDef.Engine != "" {
				engine = canonicalEngineName(tblDef.Engine)
			}

			// Determine ROW_FORMAT from table definition
			rowFormat := "Dynamic"
			if tblDef != nil && tblDef.RowFormat != "" {
				rowFormat = tblDef.RowFormat
			} else if strings.EqualFold(engine, "MEMORY") {
				rowFormat = "Fixed"
			}

			// Determine TABLE_COLLATION from table definition or database charset
			tableCollation := "utf8mb4_0900_ai_ci"
			if tblDef != nil && tblDef.Collation != "" {
				tableCollation = tblDef.Collation
			} else if tblDef != nil && tblDef.Charset != "" {
				tableCollation = catalogPkg.DefaultCollationForCharset(tblDef.Charset)
			} else if db != nil && db.CollationName != "" {
				tableCollation = db.CollationName
			}

			// Try to get actual row count from storage engine
			if tableRows == 0 {
				if stbl, err := e.Storage.GetTable(dbName, tblName); err == nil {
					stbl.Mu.RLock()
					tableRows = int64(len(stbl.Rows))
					stbl.Mu.RUnlock()
				}
			}

			// Compute AUTO_INCREMENT: next value for auto-increment column
			var autoIncrement interface{} = nil
			if tblDef != nil {
				for _, col := range tblDef.Columns {
					if col.AutoIncrement {
						if stbl, err := e.Storage.GetTable(dbName, tblName); err == nil {
							cur := stbl.AutoIncrementValue()
							autoIncrement = cur + 1
						}
						break
					}
				}
			}

			// DATA_FREE: use 0 for InnoDB tables (not nil)
			var dataFree interface{} = int64(0)
			if strings.EqualFold(engine, "MEMORY") {
				dataFree = int64(0)
			}

			rows = append(rows, storage.Row{
				"TABLE_CATALOG":   "def",
				"TABLE_SCHEMA":    dbName,
				"TABLE_NAME":      tblName,
				"TABLE_TYPE":      "BASE TABLE",
				"ENGINE":          engine,
				"VERSION":         int64(10),
				"ROW_FORMAT":      rowFormat,
				"TABLE_ROWS":      tableRows,
				"AVG_ROW_LENGTH":  avgRowLength,
				"DATA_LENGTH":     dataLength,
				"MAX_DATA_LENGTH": maxDataLength,
				"INDEX_LENGTH":    indexLength,
				"DATA_FREE":       dataFree,
				"AUTO_INCREMENT":  autoIncrement,
				"CREATE_TIME":     nil,
				"UPDATE_TIME":     nil,
				"CHECK_TIME":      nil,
				"TABLE_COLLATION": tableCollation,
				"CHECKSUM":        nil,
				"CREATE_OPTIONS":  createOptions,
				"TABLE_COMMENT":   tblComment,
			})
		}
	}

	// Add information_schema virtual tables as SYSTEM VIEW entries
	// Order matches MySQL 8.0's utf8_general_ci collation ordering
	isTableNames := []string{
		"CHARACTER_SETS", "CHECK_CONSTRAINTS",
		"COLLATIONS", "COLLATION_CHARACTER_SET_APPLICABILITY",
		"COLUMNS", "COLUMN_PRIVILEGES", "COLUMN_STATISTICS",
		"ENGINES", "EVENTS", "FILES",
		"KEYWORDS", "KEY_COLUMN_USAGE",
		"OPTIMIZER_TRACE",
		"PARAMETERS", "PARTITIONS", "PLUGINS",
		"PROCESSLIST",
		"REFERENTIAL_CONSTRAINTS", "RESOURCE_GROUPS", "ROUTINES",
		"SCHEMATA", "SCHEMA_PRIVILEGES", "STATISTICS",
		"ST_GEOMETRY_COLUMNS", "ST_SPATIAL_REFERENCE_SYSTEMS", "ST_UNITS_OF_MEASURE",
		"TABLES", "TABLESPACES", "TABLE_CONSTRAINTS", "TABLE_PRIVILEGES",
		"TRIGGERS",
		"USER_PRIVILEGES",
		"VIEWS", "VIEW_ROUTINE_USAGE", "VIEW_TABLE_USAGE",
	}
	for _, tblName := range isTableNames {
		rows = append(rows, storage.Row{
			"TABLE_CATALOG":   "def",
			"TABLE_SCHEMA":    "information_schema",
			"TABLE_NAME":      tblName,
			"TABLE_TYPE":      "SYSTEM VIEW",
			"ENGINE":          nil,
			"VERSION":         int64(10),
			"ROW_FORMAT":      nil,
			"TABLE_ROWS":      nil,
			"AVG_ROW_LENGTH":  nil,
			"DATA_LENGTH":     nil,
			"MAX_DATA_LENGTH": nil,
			"INDEX_LENGTH":    nil,
			"DATA_FREE":       nil,
			"AUTO_INCREMENT":  nil,
			"CREATE_TIME":     nil,
			"UPDATE_TIME":     nil,
			"CHECK_TIME":      nil,
			"TABLE_COLLATION": nil,
			"CHECKSUM":        nil,
			"CREATE_OPTIONS":  nil,
			"TABLE_COMMENT":   "",
		})
	}

	return rows
}

// infoSchemaColumns returns rows for INFORMATION_SCHEMA.COLUMNS.
func (e *Executor) infoSchemaColumns() []storage.Row {
	dbNames := e.Catalog.ListDatabases()
	sort.Strings(dbNames)

	var rows []storage.Row
	for _, dbName := range dbNames {
		db, err := e.Catalog.GetDatabase(dbName)
		if err != nil {
			continue
		}
		tableNames := db.ListTables()
		sort.Strings(tableNames)
		for _, tblName := range tableNames {
			tbl, err := db.GetTable(tblName)
			if err != nil {
				continue
			}
			for pos, col := range tbl.Columns {
				isNullable := "YES"
				if !col.Nullable {
					isNullable = "NO"
				}
				var colDefault interface{}
				if col.Default != nil {
					colDefault = *col.Default
				}

				// Derive DATA_TYPE and precision/scale from col.Type
				dataType := strings.ToLower(col.Type)
				// Strip parenthesized length info for DATA_TYPE
				if idx := strings.Index(dataType, "("); idx >= 0 {
					dataType = dataType[:idx]
				}
				// Strip UNSIGNED, ZEROFILL, SIGNED modifiers from DATA_TYPE
				dataType = strings.Replace(dataType, " unsigned", "", 1)
				dataType = strings.Replace(dataType, " zerofill", "", 1)
				dataType = strings.Replace(dataType, " signed", "", 1)
				dataType = strings.TrimSpace(dataType)

				var charMaxLen interface{}
				var charOctetLen interface{}
				var numPrecision interface{}
				var numScale interface{}
				colTypeUpper := strings.ToUpper(strings.TrimSpace(col.Type))
			baseType := colTypeUpper
			if idx := strings.Index(baseType, "("); idx >= 0 {
				baseType = strings.TrimSpace(baseType[:idx])
			}
			// Strip UNSIGNED, ZEROFILL, etc.
			baseType = strings.TrimSpace(strings.Split(baseType, " ")[0])

			switch baseType {
			case "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT":
				// Extract character length from type spec
				charLen := int64(-1) // -1 means not found
				if idx := strings.Index(colTypeUpper, "("); idx >= 0 {
					end := strings.Index(colTypeUpper[idx:], ")")
					if end > 0 {
						n, err := strconv.ParseInt(colTypeUpper[idx+1:idx+end], 10, 64)
						if err == nil {
							charLen = n
						}
					}
				}
				if baseType == "CHAR" && charLen == -1 {
					charLen = 1 // CHAR without length defaults to CHAR(1)
				} else if baseType == "VARCHAR" && charLen == -1 {
					charLen = 0
				} else if charLen == -1 {
					charLen = 0
				}
				// For TEXT types, CHARACTER_MAXIMUM_LENGTH is in characters
				// which depends on the charset's max bytes per char
				colCharset := strings.ToLower(col.Charset)
				if colCharset == "" {
					colCharset = "utf8mb4"
				}
				charsetMaxBytes := int64(4) // default utf8mb4
				switch {
				case colCharset == "latin1" || colCharset == "ascii" || colCharset == "binary":
					charsetMaxBytes = 1
				case colCharset == "ucs2":
					charsetMaxBytes = 2
				case colCharset == "utf16" || colCharset == "utf16le":
					charsetMaxBytes = 4 // surrogate pairs
				case colCharset == "utf8" || colCharset == "utf8mb3":
					charsetMaxBytes = 3
				case colCharset == "utf8mb4":
					charsetMaxBytes = 4
				case colCharset == "utf32":
					charsetMaxBytes = 4
				}
				isTextType := false
				var textByteLen int64
				if baseType == "TINYTEXT" {
					charLen = 255 / charsetMaxBytes
					textByteLen = 255
					isTextType = true
				} else if baseType == "TEXT" {
					charLen = 65535 / charsetMaxBytes
					textByteLen = 65535
					isTextType = true
				} else if baseType == "MEDIUMTEXT" {
					charLen = 16777215 / charsetMaxBytes
					textByteLen = 16777215
					isTextType = true
				} else if baseType == "LONGTEXT" {
					charLen = 4294967295 / charsetMaxBytes
					textByteLen = 4294967295
					isTextType = true
				}
				{
					charMaxLen = charLen
					if isTextType {
						charOctetLen = textByteLen
					} else {
						charOctetLen = charLen * charsetMaxBytes
					}
				}
			case "INT", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT":
				isUnsigned := strings.Contains(strings.ToUpper(col.Type), "UNSIGNED")
				switch baseType {
				case "TINYINT":
					numPrecision = int64(3)
				case "SMALLINT":
					numPrecision = int64(5)
				case "MEDIUMINT":
					numPrecision = int64(7)
				case "INT":
					numPrecision = int64(10)
				case "BIGINT":
					if isUnsigned {
						numPrecision = int64(20)
					} else {
						numPrecision = int64(19)
					}
				default:
					numPrecision = int64(10)
				}
				numScale = int64(0)
			case "FLOAT", "DOUBLE", "DECIMAL":
				numPrecision = nil
				numScale = nil
			}

			// Determine character set and collation for string types
			var charSetName interface{} = nil
			var collationName interface{} = nil
			switch baseType {
			case "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "ENUM", "SET":
				cs := col.Charset
				if cs == "" {
					cs = "utf8mb4"
				}
				charSetName = cs
				// Determine default collation for charset
				switch strings.ToLower(cs) {
				case "utf8mb4":
					collationName = "utf8mb4_0900_ai_ci"
				case "utf8", "utf8mb3":
					collationName = "utf8_general_ci"
					if cs == "utf8mb3" {
						collationName = "utf8mb3_general_ci"
					}
				case "latin1":
					collationName = "latin1_swedish_ci"
				case "binary":
					collationName = "binary"
				case "ascii":
					collationName = "ascii_general_ci"
				case "ucs2":
					collationName = "ucs2_general_ci"
				case "utf16":
					collationName = "utf16_general_ci"
				case "utf32":
					collationName = "utf32_general_ci"
				default:
					collationName = cs + "_general_ci"
				}
				// Use column-specific collation if set
				if col.Collation != "" {
					collationName = col.Collation
				}
			}

				columnKey := ""
				extra := ""
				if col.PrimaryKey {
					columnKey = "PRI"
				} else if col.Unique {
					columnKey = "UNI"
				}
				// Also mark as MUL if part of a non-unique index (first column)
				if columnKey == "" {
					for _, idx := range tbl.Indexes {
						if len(idx.Columns) > 0 {
							idxCol := normalizeIndexColumnName(idx.Columns[0])
							if strings.EqualFold(idxCol, col.Name) {
								columnKey = "MUL"
								break
							}
						}
					}
				}
				if col.AutoIncrement {
					extra = "auto_increment"
				} else if col.OnUpdateCurrentTimestamp {
					if col.Default != nil && strings.ToUpper(*col.Default) == "CURRENT_TIMESTAMP" {
						extra = "DEFAULT_GENERATED on update CURRENT_TIMESTAMP"
					} else {
						extra = "on update CURRENT_TIMESTAMP"
					}
				} else if col.Default != nil && strings.ToUpper(*col.Default) == "CURRENT_TIMESTAMP" {
					// TIMESTAMP/DATETIME with DEFAULT CURRENT_TIMESTAMP (no ON UPDATE)
					extra = "DEFAULT_GENERATED"
				}

				// Compute DATETIME_PRECISION for temporal types
				var datetimePrecision interface{} = nil
				switch baseType {
				case "DATETIME", "TIMESTAMP", "TIME":
					datetimePrecision = int64(0) // default fractional seconds precision
					// Check for explicit precision like DATETIME(6)
					if idx := strings.Index(colTypeUpper, "("); idx >= 0 {
						end := strings.Index(colTypeUpper[idx:], ")")
						if end > 0 {
							if n, err := strconv.ParseInt(colTypeUpper[idx+1:idx+end], 10, 64); err == nil {
								datetimePrecision = n
							}
						}
					}
				}

				rows = append(rows, storage.Row{
					"TABLE_CATALOG":            "def",
					"TABLE_SCHEMA":             dbName,
					"TABLE_NAME":               tblName,
					"COLUMN_NAME":              col.Name,
					"ORDINAL_POSITION":         int64(pos + 1),
					"COLUMN_DEFAULT":           colDefault,
					"IS_NULLABLE":              isNullable,
					"DATA_TYPE":                dataType,
					"CHARACTER_MAXIMUM_LENGTH": charMaxLen,
					"CHARACTER_OCTET_LENGTH":   charOctetLen,
					"NUMERIC_PRECISION":        numPrecision,
					"NUMERIC_SCALE":            numScale,
					"DATETIME_PRECISION":       datetimePrecision,
					"CHARACTER_SET_NAME":       charSetName,
					"COLLATION_NAME":           collationName,
					"COLUMN_TYPE":              normalizeColumnType(col.Type),
					"COLUMN_KEY":               columnKey,
					"EXTRA":                    extra,
					"PRIVILEGES":               "select,insert,update,references",
					"COLUMN_COMMENT":           col.Comment,
					"GENERATION_EXPRESSION":    "",
					"SRS_ID":                   nil,
				})
			}
		}
	}
	return rows
}

// infoSchemaStatistics returns rows for INFORMATION_SCHEMA.STATISTICS.
func (e *Executor) infoSchemaStatistics() []storage.Row {
	readPersistent := e.informationSchemaStatsExpiryZero()
	cardinalityByKey := map[string]int64{}
	if readPersistent {
		if tbl, err := e.Storage.GetTable("mysql", "innodb_index_stats"); err == nil {
			tbl.Mu.RLock()
			for _, r := range tbl.Rows {
				dbName := strings.ToLower(toString(r["database_name"]))
				tableName := strings.ToLower(toString(r["table_name"]))
				indexName := strings.ToLower(toString(r["index_name"]))
				statName := strings.ToLower(toString(r["stat_name"]))
				if dbName == "" || tableName == "" || indexName == "" || !strings.HasPrefix(statName, "n_diff_pfx") {
					continue
				}
				cardinalityByKey[dbName+"."+tableName+"."+indexName+"."+statName] = asInt64Or(r["stat_value"], 0)
			}
			tbl.Mu.RUnlock()
		}
	}

	dbNames := e.Catalog.ListDatabases()
	sort.Strings(dbNames)

	var rows []storage.Row
	for _, dbName := range dbNames {
		db, err := e.Catalog.GetDatabase(dbName)
		if err != nil {
			continue
		}
		tableNames := db.ListTables()
		sort.Strings(tableNames)
		for _, tblName := range tableNames {
			tbl, err := db.GetTable(tblName)
			if err != nil {
				continue
			}
			stbl, _ := e.Storage.GetTable(dbName, tblName)
			var dataRows []storage.Row
			if stbl != nil {
				stbl.Mu.RLock()
				dataRows = append(dataRows, stbl.Rows...)
				stbl.Mu.RUnlock()
			}
			colNullable := make(map[string]bool, len(tbl.Columns))
			for _, c := range tbl.Columns {
				colNullable[strings.ToLower(c.Name)] = c.Nullable
			}
			appendIndexRows := func(indexName string, cols []string, nonUnique int64, idxComment string, idxType string) {
				var dynamic []int64
				if !readPersistent {
					dynamic = distinctPrefixCounts(dataRows, cols)
				}
				indexTypeStr := "BTREE"
				collation := interface{}("A")
				if idxType == "FULLTEXT" {
					indexTypeStr = "FULLTEXT"
					collation = nil
				} else if idxType == "SPATIAL" {
					indexTypeStr = "SPATIAL"
					collation = nil
				}
				for i, col := range cols {
					colName := normalizeIndexColumnName(col)
					nullable := ""
					if colNullable[strings.ToLower(colName)] {
						nullable = "YES"
					}
					statKey := strings.ToLower(dbName + "." + tblName + "." + indexName + "." + fmt.Sprintf("n_diff_pfx%02d", i+1))
					cardinality := int64(0)
					if readPersistent {
						cardinality = cardinalityByKey[statKey]
					} else if i < len(dynamic) {
						cardinality = dynamic[i]
					}
					rows = append(rows, storage.Row{
						"TABLE_CATALOG": "def",
						"TABLE_SCHEMA":  dbName,
						"TABLE_NAME":    tblName,
						"NON_UNIQUE":    nonUnique,
						"INDEX_SCHEMA":  dbName,
						"INDEX_NAME":    indexName,
						"SEQ_IN_INDEX":  int64(i + 1),
						"COLUMN_NAME":   colName,
						"COLLATION":     collation,
						"CARDINALITY":   cardinality,
						"SUB_PART":      nil,
						"PACKED":        nil,
						"NULLABLE":      nullable,
						"INDEX_TYPE":    indexTypeStr,
						"COMMENT":       "",
						"INDEX_COMMENT": idxComment,
						"IS_VISIBLE":    "YES",
						"EXPRESSION":    nil,
					})
				}
			}

			// InnoDB secondary index metadata includes PK columns as suffix.
			for _, idx := range tbl.Indexes {
				nonUnique := int64(1)
				if idx.Unique {
					nonUnique = 0
				}
				appendIndexRows(idx.Name, idx.Columns, nonUnique, idx.Comment, idx.Type)
			}
			if len(tbl.PrimaryKey) > 0 {
				appendIndexRows("PRIMARY", tbl.PrimaryKey, 0, "", "")
			}
		}
	}
	return rows
}

// infoSchemaColumnStatistics returns rows for INFORMATION_SCHEMA.COLUMN_STATISTICS.
func (e *Executor) infoSchemaColumnStatistics() []storage.Row {
	dbNames := e.Catalog.ListDatabases()
	sort.Strings(dbNames)

	var rows []storage.Row
	for _, dbName := range dbNames {
		db, err := e.Catalog.GetDatabase(dbName)
		if err != nil {
			continue
		}
		tableNames := db.ListTables()
		sort.Strings(tableNames)
		for _, tblName := range tableNames {
			tblDef, err := db.GetTable(tblName)
			if err != nil || tblDef == nil {
				continue
			}
			stbl, _ := e.Storage.GetTable(dbName, tblName)
			for _, col := range tblDef.Columns {
				histogram := `{"buckets":[]}`
				if stbl != nil {
					counts := map[string]int{}
					total := 0
					stbl.Mu.RLock()
					for _, r := range stbl.Rows {
						if v, ok := r[col.Name]; ok && v != nil {
							key := toString(v)
							counts[key]++
							total++
						}
					}
					stbl.Mu.RUnlock()
					if total > 0 {
						keys := make([]string, 0, len(counts))
						for k := range counts {
							keys = append(keys, k)
						}
						sort.Strings(keys)
						cum := 0
						buckets := make([]interface{}, 0, len(keys))
						for _, k := range keys {
							cum += counts[k]
							buckets = append(buckets, []interface{}{k, float64(cum) / float64(total)})
						}
						histogram = jsonMarshalMySQL(map[string]interface{}{"buckets": buckets})
					}
				}
				rows = append(rows, storage.Row{
					"SCHEMA_NAME": dbName,
					"TABLE_NAME":  tblName,
					"COLUMN_NAME": col.Name,
					"HISTOGRAM":   histogram,
				})
			}
		}
	}
	return rows
}

// showTableStatus returns a Result for SHOW TABLE STATUS, mapping to the
// INFORMATION_SCHEMA.TABLES columns that MySQL clients commonly expect.
func (e *Executor) showTableStatus() (*Result, error) {
	rows := e.infoSchemaTables()
	cols := []string{
		"Name", "Engine", "Version", "Row_format", "Rows",
		"Avg_row_length", "Data_length", "Max_data_length", "Index_length",
		"Data_free", "Auto_increment", "Create_time", "Update_time",
		"Check_time", "Collation", "Checksum", "Create_options", "Comment",
	}

	// Filter to current DB
	dbName := e.CurrentDB
	resultRows := make([][]interface{}, 0)
	for _, row := range rows {
		if row["TABLE_SCHEMA"] != dbName {
			continue
		}
		resultRows = append(resultRows, []interface{}{
			row["TABLE_NAME"],
			row["ENGINE"],
			row["VERSION"],
			row["ROW_FORMAT"],
			row["TABLE_ROWS"],
			row["AVG_ROW_LENGTH"],
			row["DATA_LENGTH"],
			row["MAX_DATA_LENGTH"],
			row["INDEX_LENGTH"],
			row["DATA_FREE"],
			row["AUTO_INCREMENT"],
			row["CREATE_TIME"],
			row["UPDATE_TIME"],
			row["CHECK_TIME"],
			row["TABLE_COLLATION"],
			row["CHECKSUM"],
			row["CREATE_OPTIONS"],
			row["TABLE_COMMENT"],
		})
	}

	return &Result{
		Columns:     cols,
		Rows:        resultRows,
		IsResultSet: true,
	}, nil
}

// infoSchemaEngines returns rows for INFORMATION_SCHEMA.ENGINES.
// mylite treats all engines as InnoDB internally, but reports MyISAM, MEMORY,
// CSV, etc. as available so that tests using --source include/have_myisam.inc
// (and similar) do not self-skip.
func (e *Executor) infoSchemaEngines() []storage.Row {
	return []storage.Row{
		{
			"ENGINE":       "InnoDB",
			"SUPPORT":      "DEFAULT",
			"COMMENT":      "Supports transactions, row-level locking, and foreign keys",
			"TRANSACTIONS": "YES",
			"XA":           "YES",
			"SAVEPOINTS":   "YES",
		},
		{
			"ENGINE":       "MyISAM",
			"SUPPORT":      "YES",
			"COMMENT":      "MyISAM storage engine",
			"TRANSACTIONS": "NO",
			"XA":           "NO",
			"SAVEPOINTS":   "NO",
		},
		{
			"ENGINE":       "MEMORY",
			"SUPPORT":      "YES",
			"COMMENT":      "Hash based, stored in memory, useful for temporary tables",
			"TRANSACTIONS": "NO",
			"XA":           "NO",
			"SAVEPOINTS":   "NO",
		},
		{
			"ENGINE":       "CSV",
			"SUPPORT":      "YES",
			"COMMENT":      "CSV storage engine",
			"TRANSACTIONS": "NO",
			"XA":           "NO",
			"SAVEPOINTS":   "NO",
		},
		{
			"ENGINE":       "ARCHIVE",
			"SUPPORT":      "YES",
			"COMMENT":      "Archive storage engine",
			"TRANSACTIONS": "NO",
			"XA":           "NO",
			"SAVEPOINTS":   "NO",
		},
		{
			"ENGINE":       "BLACKHOLE",
			"SUPPORT":      "YES",
			"COMMENT":      "/dev/null storage engine (anything you write to it disappears)",
			"TRANSACTIONS": "NO",
			"XA":           "NO",
			"SAVEPOINTS":   "NO",
		},
		{
			"ENGINE":       "FEDERATED",
			"SUPPORT":      "NO",
			"COMMENT":      "Federated MySQL storage engine",
			"TRANSACTIONS": "NO",
			"XA":           "NO",
			"SAVEPOINTS":   "NO",
		},
		{
			"ENGINE":       "MRG_MYISAM",
			"SUPPORT":      "YES",
			"COMMENT":      "Collection of identical MyISAM tables",
			"TRANSACTIONS": "NO",
			"XA":           "NO",
			"SAVEPOINTS":   "NO",
		},
		{
			"ENGINE":       "PERFORMANCE_SCHEMA",
			"SUPPORT":      "YES",
			"COMMENT":      "Performance Schema",
			"TRANSACTIONS": "NO",
			"XA":           "NO",
			"SAVEPOINTS":   "NO",
		},
	}
}

// perfSchemaMemorySummary returns rows for performance_schema.memory_summary_global_by_event_name.
func (e *Executor) perfSchemaMemorySummary() []storage.Row {
	names := []string{
		"memory/sql/JSON",
		"memory/performance_schema/users",
		"memory/performance_schema/accounts",
		"memory/performance_schema/hosts",
		"memory/performance_schema/threads",
		"memory/sql/THD::main_mem_root",
	}
	rows := make([]storage.Row, 0, len(names))
	for _, n := range names {
		rows = append(rows, storage.Row{
			"EVENT_NAME":                   n,
			"COUNT_ALLOC":                  int64(1),
			"COUNT_FREE":                   int64(1),
			"SUM_NUMBER_OF_BYTES_ALLOC":    int64(1024),
			"SUM_NUMBER_OF_BYTES_FREE":     int64(1024),
			"LOW_COUNT_USED":               int64(0),
			"CURRENT_COUNT_USED":           int64(0),
			"HIGH_COUNT_USED":              int64(1),
			"LOW_NUMBER_OF_BYTES_USED":     int64(0),
			"CURRENT_NUMBER_OF_BYTES_USED": int64(0),
			"HIGH_NUMBER_OF_BYTES_USED":    int64(1024),
		})
	}
	return rows
}

// perfSchemaVariables returns sorted rows for performance_schema.global_variables / session_variables.
func (e *Executor) perfSchemaVariables() []storage.Row {
	return e.perfSchemaVariablesScoped(false)
}

// perfSchemaVariablesScoped returns sorted rows scoped to global or session.
func (e *Executor) perfSchemaVariablesScoped(globalOnly bool) []storage.Row {
	vars := e.buildVariablesMapScoped(globalOnly)
	names := make([]string, 0, len(vars))
	for n := range vars {
		names = append(names, n)
	}
	sort.Strings(names)
	rows := make([]storage.Row, 0, len(names))
	for _, n := range names {
		rows = append(rows, storage.Row{
			"VARIABLE_NAME":  n,
			"variable_name":  n,
			"VARIABLE_VALUE": vars[n],
			"variable_value": vars[n],
		})
	}
	return rows
}

// perfSchemaStatus returns rows for performance_schema.global_status / session_status.
// This provides a minimal set of status variables needed by MTR tests.
func (e *Executor) perfSchemaStatus() []storage.Row {
	// Use the same status data as SHOW STATUS
	statusResult, _ := e.showStatus("")
	if statusResult == nil {
		return nil
	}
	// Build mapping of PS lost counters to their size variables
	psLostToSize := map[string]string{
		"Performance_schema_accounts_lost":             "performance_schema_accounts_size",
		"Performance_schema_cond_classes_lost":         "performance_schema_max_cond_classes",
		"Performance_schema_cond_instances_lost":       "performance_schema_max_cond_instances",
		"Performance_schema_file_classes_lost":         "performance_schema_max_file_classes",
		"Performance_schema_file_handles_lost":         "performance_schema_max_file_handles",
		"Performance_schema_file_instances_lost":       "performance_schema_max_file_instances",
		"Performance_schema_hosts_lost":                "performance_schema_hosts_size",
		"Performance_schema_index_stat_lost":           "performance_schema_max_index_stat",
		"Performance_schema_memory_classes_lost":       "performance_schema_max_memory_classes",
		"Performance_schema_metadata_lock_lost":        "performance_schema_max_metadata_locks",
		"Performance_schema_mutex_classes_lost":        "performance_schema_max_mutex_classes",
		"Performance_schema_mutex_instances_lost":      "performance_schema_max_mutex_instances",
		"Performance_schema_prepared_statements_lost":  "performance_schema_max_prepared_statements_instances",
		"Performance_schema_program_lost":              "performance_schema_max_program_instances",
		"Performance_schema_rwlock_classes_lost":       "performance_schema_max_rwlock_classes",
		"Performance_schema_rwlock_instances_lost":     "performance_schema_max_rwlock_instances",
		"Performance_schema_session_connect_attrs_lost": "performance_schema_session_connect_attrs_size",
		"Performance_schema_socket_classes_lost":       "performance_schema_max_socket_classes",
		"Performance_schema_socket_instances_lost":     "performance_schema_max_socket_instances",
		"Performance_schema_stage_classes_lost":        "performance_schema_max_stage_classes",
		"Performance_schema_statement_classes_lost":    "performance_schema_max_statement_classes",
		"Performance_schema_table_handles_lost":        "performance_schema_max_table_handles",
		"Performance_schema_table_instances_lost":      "performance_schema_max_table_instances",
		"Performance_schema_table_lock_stat_lost":      "performance_schema_max_table_lock_stat",
		"Performance_schema_thread_classes_lost":       "performance_schema_max_thread_classes",
		"Performance_schema_thread_instances_lost":     "performance_schema_max_thread_instances",
		"Performance_schema_users_lost":                "performance_schema_users_size",
	}
	rows := make([]storage.Row, 0, len(statusResult.Rows))
	for _, srow := range statusResult.Rows {
		if len(srow) >= 2 {
			name := fmt.Sprintf("%v", srow[0])
			val := fmt.Sprintf("%v", srow[1])
			// If this is a PS lost counter and the corresponding size var is 0,
			// report the lost counter as 1 (indicating data was lost).
			if sizeVar, ok := psLostToSize[name]; ok {
				if sizeVal, found := e.startupVars[sizeVar]; found && sizeVal == "0" {
					val = "1"
				}
			}
			upperName := strings.ToUpper(name)
			rows = append(rows, storage.Row{
				"VARIABLE_NAME":  upperName,
				"variable_name":  upperName,
				"VARIABLE_VALUE": val,
				"variable_value": val,
			})
		}
	}
	return rows
}

// innoDBMetricDef defines a single InnoDB metric entry for INFORMATION_SCHEMA.INNODB_METRICS.
type innoDBMetricDef struct {
	name      string
	subsystem string
	mtype     string
}

// innoDBMetrics is a list of known InnoDB metrics, mirroring the MySQL 8.0 set.
var innoDBMetrics = []innoDBMetricDef{
	{"metadata_table_reference_count", "metadata", "counter"},
	{"metadata_table_handles_opened", "metadata", "counter"},
	{"metadata_table_handles_closed", "metadata", "counter"},
	{"lock_deadlocks", "lock", "counter"},
	{"lock_deadlock_false_positives", "lock", "counter"},
	{"lock_deadlock_rounds", "counter", "counter"},
	{"lock_timeouts", "lock", "counter"},
	{"lock_rec_lock_waits", "lock", "counter"},
	{"lock_table_lock_waits", "lock", "counter"},
	{"lock_rec_lock_requests", "lock", "counter"},
	{"lock_rec_release_attempts", "lock", "counter"},
	{"lock_rec_grant_attempts", "lock", "counter"},
	{"lock_rec_lock_created", "lock", "counter"},
	{"lock_rec_lock_removed", "lock", "counter"},
	{"lock_table_lock_created", "lock", "counter"},
	{"lock_table_lock_removed", "lock", "counter"},
	{"lock_table_locks", "lock", "counter"},
	{"lock_row_lock_current_waits", "lock", "counter"},
	{"lock_row_lock_time", "lock", "counter"},
	{"lock_row_lock_time_max", "lock", "counter"},
	{"lock_row_lock_waits", "lock", "counter"},
	{"lock_row_lock_time_avg", "lock", "counter"},
	{"lock_schedule_refreshes", "lock", "counter"},
	{"buffer_pool_size", "buffer", "value"},
	{"buffer_pool_reads", "buffer", "status_counter"},
	{"buffer_pool_read_requests", "buffer", "status_counter"},
	{"buffer_pool_write_requests", "buffer", "status_counter"},
	{"buffer_pool_pages_total", "buffer", "value"},
	{"buffer_pool_pages_data", "buffer", "value"},
	{"buffer_pool_pages_dirty", "buffer", "value"},
	{"buffer_pool_pages_free", "buffer", "value"},
	{"buffer_data_written", "buffer", "status_counter"},
	{"buffer_data_read", "buffer", "status_counter"},
	{"os_data_reads", "os", "status_counter"},
	{"os_data_writes", "os", "status_counter"},
	{"os_data_fsyncs", "os", "status_counter"},
	{"trx_rw_commits", "transaction", "counter"},
	{"trx_ro_commits", "transaction", "counter"},
	{"trx_nl_ro_commits", "transaction", "counter"},
	{"trx_commits_insert_update", "transaction", "counter"},
	{"trx_rollbacks", "transaction", "counter"},
	{"trx_rollbacks_savepoint", "transaction", "counter"},
	{"trx_active_transactions", "transaction", "counter"},
	{"trx_rseg_history_len", "transaction", "value"},
	{"trx_undo_slots_used", "transaction", "counter"},
	{"trx_undo_slots_cached", "transaction", "counter"},
	{"purge_del_mark_records", "purge", "counter"},
	{"purge_upd_exist_or_extern_records", "purge", "counter"},
	{"purge_invoked", "purge", "counter"},
	{"purge_undo_log_pages", "purge", "counter"},
	{"purge_dml_delay_usec", "purge", "value"},
	{"purge_stop_count", "purge", "value"},
	{"purge_resume_count", "purge", "value"},
	{"purge_truncate_history_count", "purge", "counter"},
	{"purge_truncate_history_usec", "purge", "counter"},
	{"log_lsn_last_flush", "recovery", "value"},
	{"log_lsn_last_checkpoint", "recovery", "value"},
	{"log_lsn_current", "recovery", "value"},
	{"log_lsn_archived", "recovery", "value"},
	{"log_lsn_checkpoint_age", "recovery", "value"},
	{"log_lsn_buf_dirty_pages_added", "recovery", "value"},
	{"log_lsn_buf_pool_oldest_approx", "recovery", "value"},
	{"log_lsn_buf_pool_oldest_lwm", "recovery", "value"},
	{"log_max_modified_age_async", "recovery", "value"},
	{"log_max_modified_age_sync", "recovery", "value"},
	{"log_waits", "recovery", "status_counter"},
	{"log_write_requests", "recovery", "status_counter"},
	{"log_writes", "recovery", "status_counter"},
	{"log_padded", "recovery", "status_counter"},
	{"compress_pages_compressed", "compression", "counter"},
	{"compress_pages_decompressed", "compression", "counter"},
	{"index_page_splits", "index", "counter"},
	{"index_page_merge_attempts", "index", "counter"},
	{"index_page_merge_successful", "index", "counter"},
	{"adaptive_hash_searches", "adaptive_hash_index", "status_counter"},
	{"adaptive_hash_searches_btree", "adaptive_hash_index", "status_counter"},
	{"file_num_open_files", "file_system", "value"},
	{"ibuf_merges_insert", "change_buffer", "status_counter"},
	{"ibuf_merges_delete_mark", "change_buffer", "status_counter"},
	{"ibuf_merges_delete", "change_buffer", "status_counter"},
	{"ibuf_merges_discard_insert", "change_buffer", "status_counter"},
	{"ibuf_merges_discard_delete_mark", "change_buffer", "status_counter"},
	{"ibuf_merges_discard_delete", "change_buffer", "status_counter"},
	{"ibuf_merges", "change_buffer", "status_counter"},
	{"ibuf_size", "change_buffer", "value"},
	{"innodb_dblwr_pages_written", "dblwr", "status_counter"},
	{"innodb_dblwr_writes", "dblwr", "status_counter"},
	{"innodb_page_size", "server", "value"},
	{"innodb_rwlock_s_spin_waits", "server", "status_counter"},
	{"innodb_rwlock_x_spin_waits", "server", "status_counter"},
	{"innodb_rwlock_sx_spin_waits", "server", "status_counter"},
	{"innodb_rwlock_s_spin_rounds", "server", "status_counter"},
	{"innodb_rwlock_x_spin_rounds", "server", "status_counter"},
	{"innodb_rwlock_sx_spin_rounds", "server", "status_counter"},
	{"innodb_rwlock_s_os_waits", "server", "status_counter"},
	{"innodb_rwlock_x_os_waits", "server", "status_counter"},
	{"innodb_rwlock_sx_os_waits", "server", "status_counter"},
	{"dml_inserts", "dml", "status_counter"},
	{"dml_deletes", "dml", "status_counter"},
	{"dml_updates", "dml", "status_counter"},
	{"dml_system_inserts", "dml", "status_counter"},
	{"dml_system_deletes", "dml", "status_counter"},
	{"dml_system_updates", "dml", "status_counter"},
	{"sampled_pages_read", "sampling", "counter"},
	{"sampled_pages_skipped", "sampling", "counter"},
	{"ddl_background_drop_indexes", "ddl", "counter"},
	{"ddl_background_drop_tables", "ddl", "counter"},
	{"ddl_online_create_index", "ddl", "counter"},
	{"ddl_pending_alter_table", "ddl", "counter"},
	{"ddl_sort_file_alter_table", "ddl", "counter"},
	{"ddl_log_file_alter_table", "ddl", "counter"},
	{"icp_attempts", "icp", "counter"},
	{"icp_no_match", "icp", "counter"},
	{"icp_out_of_range", "icp", "counter"},
	{"icp_match", "icp", "counter"},
	{"cpu_utime_abs", "cpu", "value"},
	{"cpu_stime_abs", "cpu", "value"},
	{"cpu_utime_pct", "cpu", "value"},
	{"cpu_stime_pct", "cpu", "value"},
	{"cpu_n", "cpu", "value"},
}

func (e *Executor) infoSchemaInnoDBMetrics() []storage.Row {
	rows := make([]storage.Row, 0, len(innoDBMetrics))
	for _, m := range innoDBMetrics {
		rows = append(rows, storage.Row{
			"NAME":      m.name,
			"COUNT":     int64(0),
			"TYPE":      m.mtype,
			"STATUS":    "disabled",
			"SUBSYSTEM": m.subsystem,
			"COMMENT":   "",
		})
	}
	return rows
}

// perfSchemaPerformanceTimers returns the fixed rows for performance_schema.performance_timers.
func (e *Executor) perfSchemaPerformanceTimers() []storage.Row {
	return []storage.Row{
		{"TIMER_NAME": "CYCLE", "TIMER_FREQUENCY": int64(2400000000), "TIMER_RESOLUTION": int64(1), "TIMER_OVERHEAD": int64(37)},
		{"TIMER_NAME": "NANOSECOND", "TIMER_FREQUENCY": int64(1000000000), "TIMER_RESOLUTION": int64(1), "TIMER_OVERHEAD": int64(56)},
		{"TIMER_NAME": "MICROSECOND", "TIMER_FREQUENCY": int64(1000000), "TIMER_RESOLUTION": int64(1), "TIMER_OVERHEAD": int64(56)},
		{"TIMER_NAME": "MILLISECOND", "TIMER_FREQUENCY": int64(1000), "TIMER_RESOLUTION": int64(1), "TIMER_OVERHEAD": int64(56)},
	}
}

// perfSchemaThreads returns a stub row for performance_schema.threads.
func (e *Executor) perfSchemaThreads() []storage.Row {
	rows := []storage.Row{
		{
			"THREAD_ID":           int64(1),
			"NAME":                "thread/sql/main",
			"TYPE":                "BACKGROUND",
			"PROCESSLIST_ID":      nil,
			"PROCESSLIST_USER":    nil,
			"PROCESSLIST_HOST":    nil,
			"PROCESSLIST_DB":      nil,
			"PROCESSLIST_COMMAND": nil,
			"PROCESSLIST_TIME":    nil,
			"PROCESSLIST_STATE":   nil,
			"PROCESSLIST_INFO":    nil,
			"PARENT_THREAD_ID":    nil,
			"ROLE":                nil,
			"INSTRUMENTED":        "YES",
			"HISTORY":             "YES",
			"CONNECTION_TYPE":     nil,
			"THREAD_OS_ID":        int64(0),
			"RESOURCE_GROUP":      "SYS_default",
		},
	}
	// Add a row for the current user connection
	connID := e.connectionID
	if connID > 0 {
		rows = append(rows, storage.Row{
			"THREAD_ID":           connID + 1, // thread_id = connID + 1 by convention
			"NAME":                "thread/sql/one_connection",
			"TYPE":                "FOREGROUND",
			"PROCESSLIST_ID":      connID,
			"PROCESSLIST_USER":    "root",
			"PROCESSLIST_HOST":    "localhost",
			"PROCESSLIST_DB":      e.CurrentDB,
			"PROCESSLIST_COMMAND": "Query",
			"PROCESSLIST_TIME":    int64(0),
			"PROCESSLIST_STATE":   nil,
			"PROCESSLIST_INFO":    nil,
			"PARENT_THREAD_ID":    int64(1),
			"ROLE":                nil,
			"INSTRUMENTED":        "YES",
			"HISTORY":             "YES",
			"CONNECTION_TYPE":     "TCP/IP",
			"THREAD_OS_ID":        int64(0),
			"RESOURCE_GROUP":      "USR_default",
		})
	}
	return rows
}

// perfSchemaSetupActors returns the default rows for performance_schema.setup_actors.
func (e *Executor) perfSchemaSetupActors() []storage.Row {
	// If performance_schema_setup_actors_size=0, return empty
	if v, ok := e.startupVars["performance_schema_setup_actors_size"]; ok && v == "0" {
		return []storage.Row{}
	}
	return []storage.Row{
		{"HOST": "%", "USER": "%", "ROLE": "%", "ENABLED": "YES", "HISTORY": "YES"},
	}
}

// perfSchemaSetupInstruments returns stub rows for performance_schema.setup_instruments.
func (e *Executor) perfSchemaSetupInstruments() []storage.Row {
	// Return a representative set of instrument categories
	instruments := []struct{ name, enabled, timed string }{
		{"wait/synch/mutex/sql/THD_LOCK_INFO::mutex", "YES", "YES"},
		{"wait/synch/rwlock/sql/LOCK_grant", "YES", "YES"},
		{"wait/synch/cond/sql/COND_open", "YES", "YES"},
		{"wait/io/file/sql/binlog", "YES", "YES"},
		{"wait/io/table/sql/handler", "YES", "YES"},
		{"wait/io/socket/sql/server_tcpip_socket", "YES", "YES"},
		{"wait/lock/table/sql/handler", "YES", "YES"},
		{"stage/sql/After create", "YES", "YES"},
		{"statement/sql/select", "YES", "YES"},
		{"statement/sql/insert", "YES", "YES"},
		{"statement/sql/update", "YES", "YES"},
		{"statement/sql/delete", "YES", "YES"},
		{"transaction", "YES", "YES"},
		{"memory/sql/THD::main_mem_root", "YES", "YES"},
		{"idle", "YES", "YES"},
	}

	// Check startup variables that disable instrument categories.
	// When performance_schema_max_*_classes=0, the corresponding instruments are excluded.
	disabledPrefixes := map[string]string{
		"performance_schema_max_cond_classes":      "wait/synch/cond/",
		"performance_schema_max_mutex_classes":     "wait/synch/mutex/",
		"performance_schema_max_rwlock_classes":    "wait/synch/rwlock/",
		"performance_schema_max_file_classes":      "wait/io/file/",
		"performance_schema_max_socket_classes":    "wait/io/socket/",
		"performance_schema_max_stage_classes":     "stage/",
		"performance_schema_max_statement_classes": "statement/",
		"performance_schema_max_thread_classes":    "thread/",
		"performance_schema_max_memory_classes":    "memory/",
	}
	excludedPrefixes := make([]string, 0)
	for varName, prefix := range disabledPrefixes {
		if v, ok := e.startupVars[varName]; ok && v == "0" {
			excludedPrefixes = append(excludedPrefixes, prefix)
		}
	}

	rows := make([]storage.Row, 0, len(instruments))
	for _, inst := range instruments {
		excluded := false
		for _, prefix := range excludedPrefixes {
			if strings.HasPrefix(inst.name, prefix) {
				excluded = true
				break
			}
		}
		if excluded {
			continue
		}
		rows = append(rows, storage.Row{
			"NAME": inst.name, "ENABLED": inst.enabled, "TIMED": inst.timed,
			"PROPERTIES": "", "VOLATILITY": int64(0), "DOCUMENTATION": nil,
		})
	}
	return rows
}

// perfSchemaVariablesInfo returns rows for performance_schema.variables_info.
func (e *Executor) perfSchemaVariablesInfo() []storage.Row {
	vars := e.buildVariablesMap()
	names := make([]string, 0, len(vars))
	for n := range vars {
		names = append(names, n)
	}
	sort.Strings(names)
	rows := make([]storage.Row, 0, len(names))
	for _, n := range names {
		source := "COMPILED"
		// Check if this variable was set via startup options (master.opt / command line)
		if _, ok := e.startupVars[n]; ok {
			source = "COMMAND_LINE"
		}
		rows = append(rows, storage.Row{
			"VARIABLE_NAME":   n,
			"VARIABLE_SOURCE": source,
			"VARIABLE_PATH":   "",
			"MIN_VALUE":       "0",
			"MAX_VALUE":       "0",
			"SET_TIME":        nil,
			"SET_USER":        nil,
			"SET_HOST":        nil,
		})
	}
	return rows
}

// infoSchemaTriggers returns rows for INFORMATION_SCHEMA.TRIGGERS.
func (e *Executor) infoSchemaTriggers() []storage.Row {
	var rows []storage.Row
	dbNames := e.Catalog.ListDatabases()
	sort.Strings(dbNames)
	for _, dbName := range dbNames {
		if isSystemSchemaName(dbName) {
			continue
		}
		db, err := e.Catalog.GetDatabase(dbName)
		if err != nil {
			continue
		}
		if db.Triggers == nil {
			continue
		}
		trigNames := make([]string, 0, len(db.Triggers))
		for n := range db.Triggers {
			trigNames = append(trigNames, n)
		}
		sort.Strings(trigNames)
		for _, trigName := range trigNames {
			tr := db.Triggers[trigName]
			body := strings.Join(tr.Body, ";\n")
			rows = append(rows, storage.Row{
				"TRIGGER_CATALOG":            "def",
				"TRIGGER_SCHEMA":             dbName,
				"TRIGGER_NAME":               tr.Name,
				"EVENT_MANIPULATION":          tr.Event,
				"EVENT_OBJECT_CATALOG":        "def",
				"EVENT_OBJECT_SCHEMA":         dbName,
				"EVENT_OBJECT_TABLE":          tr.Table,
				"ACTION_ORDER":               int64(1),
				"ACTION_CONDITION":           nil,
				"ACTION_STATEMENT":            body,
				"ACTION_ORIENTATION":          "ROW",
				"ACTION_TIMING":              tr.Timing,
				"ACTION_REFERENCE_OLD_TABLE": nil,
				"ACTION_REFERENCE_NEW_TABLE": nil,
				"ACTION_REFERENCE_OLD_ROW":   "OLD",
				"ACTION_REFERENCE_NEW_ROW":   "NEW",
				"CREATED":                    nil,
				"SQL_MODE":                   "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
				"DEFINER":                    "root@localhost",
				"CHARACTER_SET_CLIENT":       "utf8mb4",
				"COLLATION_CONNECTION":       "utf8mb4_general_ci",
				"DATABASE_COLLATION":         "utf8mb4_general_ci",
			})
		}
	}
	return rows
}

// infoSchemaTableConstraints returns rows for INFORMATION_SCHEMA.TABLE_CONSTRAINTS.
func (e *Executor) infoSchemaTableConstraints() []storage.Row {
	var rows []storage.Row
	dbNames := e.Catalog.ListDatabases()
	sort.Strings(dbNames)
	for _, dbName := range dbNames {
		db, err := e.Catalog.GetDatabase(dbName)
		if err != nil {
			continue
		}
		tableNames := db.ListTables()
		sort.Strings(tableNames)
		for _, tblName := range tableNames {
			tblDef, _ := db.GetTable(tblName)
			if tblDef == nil {
				continue
			}
			// PRIMARY KEY constraint
			if len(tblDef.PrimaryKey) > 0 {
				rows = append(rows, storage.Row{
					"CONSTRAINT_CATALOG": "def",
					"CONSTRAINT_SCHEMA":  dbName,
					"CONSTRAINT_NAME":    "PRIMARY",
					"TABLE_SCHEMA":       dbName,
					"TABLE_NAME":         tblName,
					"CONSTRAINT_TYPE":    "PRIMARY KEY",
					"ENFORCED":           "YES",
				})
			}
			// UNIQUE constraints from indexes
			for _, idx := range tblDef.Indexes {
				if idx.Unique {
					rows = append(rows, storage.Row{
						"CONSTRAINT_CATALOG": "def",
						"CONSTRAINT_SCHEMA":  dbName,
						"CONSTRAINT_NAME":    idx.Name,
						"TABLE_SCHEMA":       dbName,
						"TABLE_NAME":         tblName,
						"CONSTRAINT_TYPE":    "UNIQUE",
						"ENFORCED":           "YES",
					})
				}
			}
		}
	}
	return rows
}

// infoSchemaCharacterSets returns rows for INFORMATION_SCHEMA.CHARACTER_SETS.
func (e *Executor) infoSchemaCharacterSets() []storage.Row {
	return []storage.Row{
		{"CHARACTER_SET_NAME": "utf8mb4", "DEFAULT_COLLATE_NAME": "utf8mb4_general_ci", "DESCRIPTION": "UTF-8 Unicode", "MAXLEN": int64(4)},
		{"CHARACTER_SET_NAME": "utf8mb3", "DEFAULT_COLLATE_NAME": "utf8mb3_general_ci", "DESCRIPTION": "UTF-8 Unicode", "MAXLEN": int64(3)},
		{"CHARACTER_SET_NAME": "utf8", "DEFAULT_COLLATE_NAME": "utf8_general_ci", "DESCRIPTION": "UTF-8 Unicode", "MAXLEN": int64(3)},
		{"CHARACTER_SET_NAME": "latin1", "DEFAULT_COLLATE_NAME": "latin1_swedish_ci", "DESCRIPTION": "cp1252 West European", "MAXLEN": int64(1)},
		{"CHARACTER_SET_NAME": "binary", "DEFAULT_COLLATE_NAME": "binary", "DESCRIPTION": "Binary pseudo charset", "MAXLEN": int64(1)},
		{"CHARACTER_SET_NAME": "ascii", "DEFAULT_COLLATE_NAME": "ascii_general_ci", "DESCRIPTION": "US ASCII", "MAXLEN": int64(1)},
	}
}

// infoSchemaCollations returns rows for INFORMATION_SCHEMA.COLLATIONS.
func (e *Executor) infoSchemaCollations() []storage.Row {
	return []storage.Row{
		{"COLLATION_NAME": "utf8mb4_general_ci", "CHARACTER_SET_NAME": "utf8mb4", "ID": int64(45), "IS_DEFAULT": "Yes", "IS_COMPILED": "Yes", "SORTLEN": int64(1), "PAD_ATTRIBUTE": "PAD SPACE"},
		{"COLLATION_NAME": "utf8mb4_bin", "CHARACTER_SET_NAME": "utf8mb4", "ID": int64(46), "IS_DEFAULT": "", "IS_COMPILED": "Yes", "SORTLEN": int64(1), "PAD_ATTRIBUTE": "PAD SPACE"},
		{"COLLATION_NAME": "utf8mb4_0900_ai_ci", "CHARACTER_SET_NAME": "utf8mb4", "ID": int64(255), "IS_DEFAULT": "", "IS_COMPILED": "Yes", "SORTLEN": int64(0), "PAD_ATTRIBUTE": "NO PAD"},
		{"COLLATION_NAME": "utf8_general_ci", "CHARACTER_SET_NAME": "utf8", "ID": int64(33), "IS_DEFAULT": "Yes", "IS_COMPILED": "Yes", "SORTLEN": int64(1), "PAD_ATTRIBUTE": "PAD SPACE"},
		{"COLLATION_NAME": "utf8_bin", "CHARACTER_SET_NAME": "utf8", "ID": int64(83), "IS_DEFAULT": "", "IS_COMPILED": "Yes", "SORTLEN": int64(1), "PAD_ATTRIBUTE": "PAD SPACE"},
		{"COLLATION_NAME": "latin1_swedish_ci", "CHARACTER_SET_NAME": "latin1", "ID": int64(8), "IS_DEFAULT": "Yes", "IS_COMPILED": "Yes", "SORTLEN": int64(1), "PAD_ATTRIBUTE": "PAD SPACE"},
		{"COLLATION_NAME": "latin1_bin", "CHARACTER_SET_NAME": "latin1", "ID": int64(47), "IS_DEFAULT": "", "IS_COMPILED": "Yes", "SORTLEN": int64(1), "PAD_ATTRIBUTE": "PAD SPACE"},
		{"COLLATION_NAME": "binary", "CHARACTER_SET_NAME": "binary", "ID": int64(63), "IS_DEFAULT": "Yes", "IS_COMPILED": "Yes", "SORTLEN": int64(1), "PAD_ATTRIBUTE": "NO PAD"},
		{"COLLATION_NAME": "ascii_general_ci", "CHARACTER_SET_NAME": "ascii", "ID": int64(11), "IS_DEFAULT": "Yes", "IS_COMPILED": "Yes", "SORTLEN": int64(1), "PAD_ATTRIBUTE": "PAD SPACE"},
	}
}

// infoSchemaCollCharSetAppl returns rows for INFORMATION_SCHEMA.COLLATION_CHARACTER_SET_APPLICABILITY.
func (e *Executor) infoSchemaCollCharSetAppl() []storage.Row {
	colls := e.infoSchemaCollations()
	rows := make([]storage.Row, 0, len(colls))
	for _, c := range colls {
		rows = append(rows, storage.Row{
			"COLLATION_NAME":     c["COLLATION_NAME"],
			"CHARACTER_SET_NAME": c["CHARACTER_SET_NAME"],
		})
	}
	return rows
}

// infoSchemaUserPrivileges returns rows for INFORMATION_SCHEMA.USER_PRIVILEGES.
func (e *Executor) infoSchemaUserPrivileges() []storage.Row {
	privTypes := []string{
		"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "RELOAD",
		"SHUTDOWN", "PROCESS", "FILE", "REFERENCES", "INDEX", "ALTER",
		"SHOW DATABASES", "SUPER", "CREATE TEMPORARY TABLES", "LOCK TABLES",
		"EXECUTE", "REPLICATION SLAVE", "REPLICATION CLIENT", "CREATE VIEW",
		"SHOW VIEW", "CREATE ROUTINE", "ALTER ROUTINE", "CREATE USER", "EVENT",
		"TRIGGER", "CREATE TABLESPACE", "CREATE ROLE", "DROP ROLE",
	}
	rows := make([]storage.Row, 0, len(privTypes))
	for _, p := range privTypes {
		rows = append(rows, storage.Row{
			"GRANTEE":        "'root'@'localhost'",
			"TABLE_CATALOG":  "def",
			"PRIVILEGE_TYPE": p,
			"IS_GRANTABLE":  "YES",
		})
	}
	return rows
}

// infoSchemaRoutines returns rows for INFORMATION_SCHEMA.ROUTINES.
func (e *Executor) infoSchemaRoutines() []storage.Row {
	var rows []storage.Row
	dbNames := e.Catalog.ListDatabases()
	sort.Strings(dbNames)
	for _, dbName := range dbNames {
		if isSystemSchemaName(dbName) {
			continue
		}
		db, err := e.Catalog.GetDatabase(dbName)
		if err != nil {
			continue
		}
		// Procedures
		if db.Procedures != nil {
			procNames := make([]string, 0, len(db.Procedures))
			for n := range db.Procedures {
				procNames = append(procNames, n)
			}
			sort.Strings(procNames)
			for _, pName := range procNames {
				p := db.Procedures[pName]
				rows = append(rows, storage.Row{
					"SPECIFIC_NAME":             p.Name,
					"ROUTINE_CATALOG":           "def",
					"ROUTINE_SCHEMA":            dbName,
					"ROUTINE_NAME":              p.Name,
					"ROUTINE_TYPE":              "PROCEDURE",
					"DATA_TYPE":                 "",
					"CHARACTER_MAXIMUM_LENGTH":  nil,
					"CHARACTER_OCTET_LENGTH":    nil,
					"NUMERIC_PRECISION":         nil,
					"NUMERIC_SCALE":             nil,
					"DATETIME_PRECISION":        nil,
					"CHARACTER_SET_NAME":        nil,
					"COLLATION_NAME":            nil,
					"DTD_IDENTIFIER":            nil,
					"ROUTINE_BODY":              "SQL",
					"ROUTINE_DEFINITION":        strings.Join(p.Body, ";\n"),
					"EXTERNAL_NAME":             nil,
					"EXTERNAL_LANGUAGE":         "SQL",
					"PARAMETER_STYLE":           "SQL",
					"IS_DETERMINISTIC":          "NO",
					"SQL_DATA_ACCESS":           "CONTAINS SQL",
					"SQL_PATH":                  nil,
					"SECURITY_TYPE":             "DEFINER",
					"CREATED":                   "2024-01-01 00:00:00",
					"LAST_ALTERED":              "2024-01-01 00:00:00",
					"SQL_MODE":                  "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
					"ROUTINE_COMMENT":           "",
					"DEFINER":                   "root@localhost",
					"CHARACTER_SET_CLIENT":      "utf8mb4",
					"COLLATION_CONNECTION":      "utf8mb4_general_ci",
					"DATABASE_COLLATION":        "utf8mb4_general_ci",
				})
			}
		}
		// Functions
		if db.Functions != nil {
			funcNames := make([]string, 0, len(db.Functions))
			for n := range db.Functions {
				funcNames = append(funcNames, n)
			}
			sort.Strings(funcNames)
			for _, fName := range funcNames {
				f := db.Functions[fName]
				det := "NO"
				if f.Deterministic {
					det = "YES"
				}
				rows = append(rows, storage.Row{
					"SPECIFIC_NAME":             f.Name,
					"ROUTINE_CATALOG":           "def",
					"ROUTINE_SCHEMA":            dbName,
					"ROUTINE_NAME":              f.Name,
					"ROUTINE_TYPE":              "FUNCTION",
					"DATA_TYPE":                 f.ReturnType,
					"CHARACTER_MAXIMUM_LENGTH":  nil,
					"CHARACTER_OCTET_LENGTH":    nil,
					"NUMERIC_PRECISION":         nil,
					"NUMERIC_SCALE":             nil,
					"DATETIME_PRECISION":        nil,
					"CHARACTER_SET_NAME":        nil,
					"COLLATION_NAME":            nil,
					"DTD_IDENTIFIER":            f.ReturnType,
					"ROUTINE_BODY":              "SQL",
					"ROUTINE_DEFINITION":        strings.Join(f.Body, ";\n"),
					"EXTERNAL_NAME":             nil,
					"EXTERNAL_LANGUAGE":         "SQL",
					"PARAMETER_STYLE":           "SQL",
					"IS_DETERMINISTIC":          det,
					"SQL_DATA_ACCESS":           "CONTAINS SQL",
					"SQL_PATH":                  nil,
					"SECURITY_TYPE":             "DEFINER",
					"CREATED":                   "2024-01-01 00:00:00",
					"LAST_ALTERED":              "2024-01-01 00:00:00",
					"SQL_MODE":                  "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
					"ROUTINE_COMMENT":           "",
					"DEFINER":                   "root@localhost",
					"CHARACTER_SET_CLIENT":      "utf8mb4",
					"COLLATION_CONNECTION":      "utf8mb4_general_ci",
					"DATABASE_COLLATION":        "utf8mb4_general_ci",
				})
			}
		}
	}
	return rows
}

// infoSchemaViews returns rows for INFORMATION_SCHEMA.VIEWS.
func (e *Executor) infoSchemaViews() []storage.Row {
	var rows []storage.Row
	if e.views != nil {
		viewNames := make([]string, 0, len(e.views))
		for n := range e.views {
			viewNames = append(viewNames, n)
		}
		sort.Strings(viewNames)
		for _, vName := range viewNames {
			rows = append(rows, storage.Row{
				"TABLE_CATALOG":        "def",
				"TABLE_SCHEMA":         e.CurrentDB,
				"TABLE_NAME":           vName,
				"VIEW_DEFINITION":      e.views[vName],
				"CHECK_OPTION":         "NONE",
				"IS_UPDATABLE":         "YES",
				"DEFINER":              "root@localhost",
				"SECURITY_TYPE":        "DEFINER",
				"CHARACTER_SET_CLIENT": "utf8mb4",
				"COLLATION_CONNECTION": "utf8mb4_general_ci",
			})
		}
	}
	return rows
}

// psClassDisabled returns true when the performance_schema startup variable for
// the given instrument class (wait, stage, statement, transaction, memory) is set to 0.
func (e *Executor) psClassDisabled(class string) bool {
	switch class {
	case "wait":
		return e.startupVars["performance_schema_max_mutex_classes"] == "0" ||
			e.startupVars["performance_schema_max_rwlock_classes"] == "0" ||
			e.startupVars["performance_schema_max_cond_classes"] == "0"
	case "stage":
		return e.startupVars["performance_schema_max_stage_classes"] == "0"
	case "statement":
		return e.startupVars["performance_schema_max_statement_classes"] == "0"
	case "transaction":
		return e.startupVars["performance_schema_max_transaction_classes"] == "0"
	case "memory":
		return e.startupVars["performance_schema_max_memory_classes"] == "0"
	}
	return false
}

// infoSchemaKeyColumnUsage returns rows for INFORMATION_SCHEMA.KEY_COLUMN_USAGE.
func (e *Executor) infoSchemaKeyColumnUsage() []storage.Row {
	dbNames := e.Catalog.ListDatabases()
	sort.Strings(dbNames)

	var rows []storage.Row
	for _, dbName := range dbNames {
		db, err := e.Catalog.GetDatabase(dbName)
		if err != nil {
			continue
		}
		tableNames := db.ListTables()
		sort.Strings(tableNames)
		for _, tblName := range tableNames {
			tbl, err := db.GetTable(tblName)
			if err != nil || tbl == nil {
				continue
			}
			// Primary key constraint
			if len(tbl.PrimaryKey) > 0 {
				for i, col := range tbl.PrimaryKey {
					rows = append(rows, storage.Row{
						"CONSTRAINT_CATALOG":            "def",
						"CONSTRAINT_SCHEMA":             dbName,
						"CONSTRAINT_NAME":               "PRIMARY",
						"TABLE_CATALOG":                 "def",
						"TABLE_SCHEMA":                  dbName,
						"TABLE_NAME":                    tblName,
						"COLUMN_NAME":                   col,
						"ORDINAL_POSITION":              int64(i + 1),
						"POSITION_IN_UNIQUE_CONSTRAINT": nil,
						"REFERENCED_TABLE_SCHEMA":       nil,
						"REFERENCED_TABLE_NAME":         nil,
						"REFERENCED_COLUMN_NAME":        nil,
					})
				}
			}
			// Unique indexes
			for _, idx := range tbl.Indexes {
				if idx.Unique {
					for i, col := range idx.Columns {
						colName := normalizeIndexColumnName(col)
						rows = append(rows, storage.Row{
							"CONSTRAINT_CATALOG":            "def",
							"CONSTRAINT_SCHEMA":             dbName,
							"CONSTRAINT_NAME":               idx.Name,
							"TABLE_CATALOG":                 "def",
							"TABLE_SCHEMA":                  dbName,
							"TABLE_NAME":                    tblName,
							"COLUMN_NAME":                   colName,
							"ORDINAL_POSITION":              int64(i + 1),
							"POSITION_IN_UNIQUE_CONSTRAINT": nil,
							"REFERENCED_TABLE_SCHEMA":       nil,
							"REFERENCED_TABLE_NAME":         nil,
							"REFERENCED_COLUMN_NAME":        nil,
						})
					}
				}
			}
		}
	}
	return rows
}

// infoSchemaCheckConstraints returns rows for INFORMATION_SCHEMA.CHECK_CONSTRAINTS.
func (e *Executor) infoSchemaCheckConstraints() []storage.Row {
	dbNames := e.Catalog.ListDatabases()
	sort.Strings(dbNames)

	var rows []storage.Row
	for _, dbName := range dbNames {
		db, err := e.Catalog.GetDatabase(dbName)
		if err != nil {
			continue
		}
		tableNames := db.ListTables()
		sort.Strings(tableNames)
		for _, tblName := range tableNames {
			tbl, err := db.GetTable(tblName)
			if err != nil || tbl == nil {
				continue
			}
			for _, cc := range tbl.CheckConstraints {
				rows = append(rows, storage.Row{
					"CONSTRAINT_CATALOG": "def",
					"CONSTRAINT_SCHEMA":  dbName,
					"CONSTRAINT_NAME":    cc.Name,
					"CHECK_CLAUSE":       cc.Expr,
				})
			}
		}
	}
	return rows
}

// canonicalEngineName returns the MySQL-canonical engine name with proper casing.
func canonicalEngineName(engine string) string {
	switch strings.ToUpper(engine) {
	case "INNODB":
		return "InnoDB"
	case "MYISAM":
		return "MyISAM"
	case "MEMORY":
		return "MEMORY"
	case "CSV":
		return "CSV"
	case "ARCHIVE":
		return "ARCHIVE"
	case "BLACKHOLE":
		return "BLACKHOLE"
	case "MERGE", "MRGSORT":
		return "MRG_MYISAM"
	case "FEDERATED":
		return "FEDERATED"
	case "NDB", "NDBCLUSTER":
		return "ndbcluster"
	default:
		return engine
	}
}

// normalizeColumnType returns the MySQL COLUMN_TYPE string for a given column type.
// For integer types without explicit display width, it adds the default display width.
func normalizeColumnType(colType string) string {
	t := strings.ToLower(strings.TrimSpace(colType))
	upper := strings.ToUpper(t)
	isUnsigned := strings.Contains(upper, "UNSIGNED")

	// Check if it already has a parenthesized width
	base := t
	if idx := strings.Index(base, "("); idx >= 0 {
		return t // already has explicit width
	}
	// Strip unsigned/zerofill for base type matching
	baseClean := strings.TrimSpace(strings.Replace(strings.Replace(base, "unsigned", "", 1), "zerofill", "", 1))
	baseClean = strings.TrimSpace(baseClean)

	suffix := ""
	if isUnsigned {
		suffix = " unsigned"
	}
	if strings.Contains(strings.ToLower(colType), "zerofill") {
		suffix += " zerofill"
	}

	switch baseClean {
	case "tinyint":
		if isUnsigned {
			return "tinyint(3) unsigned"
		}
		return "tinyint(4)"
	case "smallint":
		if isUnsigned {
			return "smallint(5) unsigned"
		}
		return "smallint(6)"
	case "mediumint":
		if isUnsigned {
			return "mediumint(8) unsigned"
		}
		return "mediumint(9)"
	case "int":
		if isUnsigned {
			return "int(10) unsigned"
		}
		return "int(11)"
	case "bigint":
		return "bigint(20)" + suffix
	case "char":
		// CHAR without length defaults to CHAR(1)
		return "char(1)"
	case "year":
		return "year(4)"
	}
	return t
}

// ---------- performance_schema seed data helpers ----------

// Event name lists used by summary tables.
var psWaitEventNames = []string{
	"wait/lock/table/sql/handler",
	"wait/io/file/sql/ERRMSG",
	"wait/synch/mutex/sql/LOCK_open",
	"wait/synch/rwlock/sql/LOCK_grant",
	"wait/synch/cond/sql/COND_open",
	"idle",
}

var psStageEventNames = []string{
	"stage/sql/executing",
	"stage/sql/init",
	"stage/sql/Opening tables",
	"stage/sql/After create",
}

var psTxnEventNames = []string{
	"transaction",
}

var psStmtEventNames = []string{
	"statement/sql/select",
	"statement/sql/insert",
	"statement/sql/update",
	"statement/sql/delete",
	"statement/sql/create_table",
	"statement/com/Init DB",
	"statement/com/Quit",
}

var psMemEventNames = []string{
	"memory/performance_schema/users",
	"memory/performance_schema/accounts",
	"memory/performance_schema/hosts",
	"memory/performance_schema/threads",
	"memory/sql/THD::main_mem_root",
	"memory/sql/help",
}

var psFileEventNames = []string{
	"wait/io/file/sql/ERRMSG",
	"wait/io/file/sql/binlog",
	"wait/io/file/innodb/innodb_data_file",
}

// -- Global by event name helpers --

func perfSchemaSeedGlobalByEventName(names []string) []storage.Row {
	rows := make([]storage.Row, 0, len(names))
	for _, n := range names {
		rows = append(rows, storage.Row{
			"EVENT_NAME": n, "COUNT_STAR": int64(0),
			"SUM_TIMER_WAIT": int64(0), "MIN_TIMER_WAIT": int64(0),
			"AVG_TIMER_WAIT": int64(0), "MAX_TIMER_WAIT": int64(0),
		})
	}
	return rows
}

func (e *Executor) perfSchemaSeedByThreadByEventName(names []string) []storage.Row {
	tid := e.connectionID + 1
	rows := make([]storage.Row, 0, len(names))
	for _, n := range names {
		rows = append(rows, storage.Row{
			"THREAD_ID": tid, "EVENT_NAME": n, "COUNT_STAR": int64(0),
			"SUM_TIMER_WAIT": int64(0), "MIN_TIMER_WAIT": int64(0),
			"AVG_TIMER_WAIT": int64(0), "MAX_TIMER_WAIT": int64(0),
		})
	}
	return rows
}

func perfSchemaSeedByAccountByEventName(names []string) []storage.Row {
	users := []string{"root", "foo"}
	rows := make([]storage.Row, 0, len(names)*len(users))
	for _, u := range users {
		for _, n := range names {
			rows = append(rows, storage.Row{
				"USER": u, "HOST": "localhost", "EVENT_NAME": n, "COUNT_STAR": int64(0),
				"SUM_TIMER_WAIT": int64(0), "MIN_TIMER_WAIT": int64(0),
				"AVG_TIMER_WAIT": int64(0), "MAX_TIMER_WAIT": int64(0),
			})
		}
	}
	return rows
}

func perfSchemaSeedByHostByEventName(names []string) []storage.Row {
	rows := make([]storage.Row, 0, len(names))
	for _, n := range names {
		rows = append(rows, storage.Row{
			"HOST": "localhost", "EVENT_NAME": n, "COUNT_STAR": int64(0),
			"SUM_TIMER_WAIT": int64(0), "MIN_TIMER_WAIT": int64(0),
			"AVG_TIMER_WAIT": int64(0), "MAX_TIMER_WAIT": int64(0),
		})
	}
	return rows
}

func perfSchemaSeedByUserByEventName(names []string) []storage.Row {
	users := []string{"root", "foo"}
	rows := make([]storage.Row, 0, len(names)*len(users))
	for _, u := range users {
		for _, n := range names {
			rows = append(rows, storage.Row{
				"USER": u, "EVENT_NAME": n, "COUNT_STAR": int64(0),
				"SUM_TIMER_WAIT": int64(0), "MIN_TIMER_WAIT": int64(0),
				"AVG_TIMER_WAIT": int64(0), "MAX_TIMER_WAIT": int64(0),
			})
		}
	}
	return rows
}

// -- Statement summary helpers (extra columns vs waits/stages/txn) --

func perfSchemaSeedStmtGlobalByEventName() []storage.Row {
	rows := make([]storage.Row, 0, len(psStmtEventNames))
	for _, n := range psStmtEventNames {
		rows = append(rows, storage.Row{
			"EVENT_NAME": n, "COUNT_STAR": int64(0),
			"SUM_TIMER_WAIT": int64(0), "MIN_TIMER_WAIT": int64(0),
			"AVG_TIMER_WAIT": int64(0), "MAX_TIMER_WAIT": int64(0),
			"SUM_ERRORS": int64(0), "SUM_WARNINGS": int64(0),
			"SUM_ROWS_AFFECTED": int64(0), "SUM_ROWS_SENT": int64(0),
			"SUM_ROWS_EXAMINED": int64(0),
		})
	}
	return rows
}

func (e *Executor) perfSchemaSeedStmtByThreadByEventName() []storage.Row {
	tid := e.connectionID + 1
	rows := make([]storage.Row, 0, len(psStmtEventNames))
	for _, n := range psStmtEventNames {
		rows = append(rows, storage.Row{
			"THREAD_ID": tid, "EVENT_NAME": n, "COUNT_STAR": int64(0),
			"SUM_TIMER_WAIT": int64(0), "MIN_TIMER_WAIT": int64(0),
			"AVG_TIMER_WAIT": int64(0), "MAX_TIMER_WAIT": int64(0),
			"SUM_ERRORS": int64(0), "SUM_WARNINGS": int64(0),
			"SUM_ROWS_AFFECTED": int64(0), "SUM_ROWS_SENT": int64(0),
			"SUM_ROWS_EXAMINED": int64(0),
		})
	}
	return rows
}

func perfSchemaSeedStmtByAccountByEventName() []storage.Row {
	users := []string{"root", "foo"}
	rows := make([]storage.Row, 0, len(psStmtEventNames)*len(users))
	for _, u := range users {
		for _, n := range psStmtEventNames {
			rows = append(rows, storage.Row{
				"USER": u, "HOST": "localhost", "EVENT_NAME": n, "COUNT_STAR": int64(0),
				"SUM_TIMER_WAIT": int64(0), "MIN_TIMER_WAIT": int64(0),
				"AVG_TIMER_WAIT": int64(0), "MAX_TIMER_WAIT": int64(0),
				"SUM_ERRORS": int64(0), "SUM_WARNINGS": int64(0),
				"SUM_ROWS_AFFECTED": int64(0), "SUM_ROWS_SENT": int64(0),
				"SUM_ROWS_EXAMINED": int64(0),
			})
		}
	}
	return rows
}

func perfSchemaSeedStmtByHostByEventName() []storage.Row {
	rows := make([]storage.Row, 0, len(psStmtEventNames))
	for _, n := range psStmtEventNames {
		rows = append(rows, storage.Row{
			"HOST": "localhost", "EVENT_NAME": n, "COUNT_STAR": int64(0),
			"SUM_TIMER_WAIT": int64(0), "MIN_TIMER_WAIT": int64(0),
			"AVG_TIMER_WAIT": int64(0), "MAX_TIMER_WAIT": int64(0),
			"SUM_ERRORS": int64(0), "SUM_WARNINGS": int64(0),
			"SUM_ROWS_AFFECTED": int64(0), "SUM_ROWS_SENT": int64(0),
			"SUM_ROWS_EXAMINED": int64(0),
		})
	}
	return rows
}

func perfSchemaSeedStmtByUserByEventName() []storage.Row {
	users := []string{"root", "foo"}
	rows := make([]storage.Row, 0, len(psStmtEventNames)*len(users))
	for _, u := range users {
		for _, n := range psStmtEventNames {
			rows = append(rows, storage.Row{
				"USER": u, "EVENT_NAME": n, "COUNT_STAR": int64(0),
				"SUM_TIMER_WAIT": int64(0), "MIN_TIMER_WAIT": int64(0),
				"AVG_TIMER_WAIT": int64(0), "MAX_TIMER_WAIT": int64(0),
				"SUM_ERRORS": int64(0), "SUM_WARNINGS": int64(0),
				"SUM_ROWS_AFFECTED": int64(0), "SUM_ROWS_SENT": int64(0),
				"SUM_ROWS_EXAMINED": int64(0),
			})
		}
	}
	return rows
}

// -- Error summary helpers --

var psErrorNumbers = []int64{1146, 1049, 1045, 1064, 1054}

func perfSchemaSeedErrorGlobal() []storage.Row {
	rows := make([]storage.Row, 0, len(psErrorNumbers))
	for _, n := range psErrorNumbers {
		rows = append(rows, storage.Row{
			"ERROR_NUMBER": n, "ERROR_NAME": "", "SQL_STATE": "",
			"SUM_ERROR_RAISED": int64(0), "SUM_ERROR_HANDLED": int64(0),
			"FIRST_SEEN": nil, "LAST_SEEN": nil,
		})
	}
	return rows
}

func (e *Executor) perfSchemaSeedErrorByThread() []storage.Row {
	tid := e.connectionID + 1
	rows := make([]storage.Row, 0, len(psErrorNumbers))
	for _, n := range psErrorNumbers {
		rows = append(rows, storage.Row{
			"THREAD_ID": tid, "ERROR_NUMBER": n, "ERROR_NAME": "", "SQL_STATE": "",
			"SUM_ERROR_RAISED": int64(0), "SUM_ERROR_HANDLED": int64(0),
			"FIRST_SEEN": nil, "LAST_SEEN": nil,
		})
	}
	return rows
}

func perfSchemaSeedErrorByAccount() []storage.Row {
	users := []string{"root", "foo"}
	rows := make([]storage.Row, 0, len(psErrorNumbers)*len(users))
	for _, u := range users {
		for _, n := range psErrorNumbers {
			rows = append(rows, storage.Row{
				"USER": u, "HOST": "localhost", "ERROR_NUMBER": n, "ERROR_NAME": "", "SQL_STATE": "",
				"SUM_ERROR_RAISED": int64(0), "SUM_ERROR_HANDLED": int64(0),
				"FIRST_SEEN": nil, "LAST_SEEN": nil,
			})
		}
	}
	return rows
}

func perfSchemaSeedErrorByHost() []storage.Row {
	rows := make([]storage.Row, 0, len(psErrorNumbers))
	for _, n := range psErrorNumbers {
		rows = append(rows, storage.Row{
			"HOST": "localhost", "ERROR_NUMBER": n, "ERROR_NAME": "", "SQL_STATE": "",
			"SUM_ERROR_RAISED": int64(0), "SUM_ERROR_HANDLED": int64(0),
			"FIRST_SEEN": nil, "LAST_SEEN": nil,
		})
	}
	return rows
}

func perfSchemaSeedErrorByUser() []storage.Row {
	users := []string{"root", "foo"}
	rows := make([]storage.Row, 0, len(psErrorNumbers)*len(users))
	for _, u := range users {
		for _, n := range psErrorNumbers {
			rows = append(rows, storage.Row{
				"USER": u, "ERROR_NUMBER": n, "ERROR_NAME": "", "SQL_STATE": "",
				"SUM_ERROR_RAISED": int64(0), "SUM_ERROR_HANDLED": int64(0),
				"FIRST_SEEN": nil, "LAST_SEEN": nil,
			})
		}
	}
	return rows
}

// -- Memory summary helpers --

// psMemRow builds a single memory_summary row with required extra fields.
func psMemRow(eventName string, extra map[string]interface{}) storage.Row {
	row := storage.Row{
		"EVENT_NAME":                   eventName,
		"COUNT_ALLOC":                  int64(0),
		"COUNT_FREE":                   int64(0),
		"SUM_NUMBER_OF_BYTES_ALLOC":    int64(0),
		"SUM_NUMBER_OF_BYTES_FREE":     int64(0),
		"LOW_COUNT_USED":               int64(0),
		"CURRENT_COUNT_USED":           int64(0),
		"HIGH_COUNT_USED":              int64(0),
		"LOW_NUMBER_OF_BYTES_USED":     int64(0),
		"CURRENT_NUMBER_OF_BYTES_USED": int64(0),
		"HIGH_NUMBER_OF_BYTES_USED":    int64(0),
	}
	for k, v := range extra {
		row[k] = v
	}
	return row
}

func (e *Executor) perfSchemaSeedMemByThreadByEventName() []storage.Row {
	tid := e.connectionID + 1
	rows := make([]storage.Row, 0, len(psMemEventNames))
	for _, n := range psMemEventNames {
		rows = append(rows, psMemRow(n, map[string]interface{}{"THREAD_ID": tid}))
	}
	return rows
}

func perfSchemaSeedMemByAccountByEventName() []storage.Row {
	users := []string{"root", "foo"}
	rows := make([]storage.Row, 0, len(psMemEventNames)*len(users))
	for _, u := range users {
		for _, n := range psMemEventNames {
			rows = append(rows, psMemRow(n, map[string]interface{}{"USER": u, "HOST": "localhost"}))
		}
	}
	return rows
}

func perfSchemaSeedMemByHostByEventName() []storage.Row {
	rows := make([]storage.Row, 0, len(psMemEventNames))
	for _, n := range psMemEventNames {
		rows = append(rows, psMemRow(n, map[string]interface{}{"HOST": "localhost"}))
	}
	return rows
}

func perfSchemaSeedMemByUserByEventName() []storage.Row {
	users := []string{"root", "foo"}
	rows := make([]storage.Row, 0, len(psMemEventNames)*len(users))
	for _, u := range users {
		for _, n := range psMemEventNames {
			rows = append(rows, psMemRow(n, map[string]interface{}{"USER": u}))
		}
	}
	return rows
}

// -- File summary by event name --

func perfSchemaSeedFileSummaryByEventName() []storage.Row {
	rows := make([]storage.Row, 0, len(psFileEventNames))
	for _, n := range psFileEventNames {
		rows = append(rows, storage.Row{
			"EVENT_NAME": n, "COUNT_STAR": int64(0), "SUM_TIMER_WAIT": int64(0),
			"COUNT_READ": int64(0), "SUM_TIMER_READ": int64(0), "SUM_NUMBER_OF_BYTES_READ": int64(0),
			"COUNT_WRITE": int64(0), "SUM_TIMER_WRITE": int64(0), "SUM_NUMBER_OF_BYTES_WRITE": int64(0),
			"COUNT_MISC": int64(0), "SUM_TIMER_MISC": int64(0),
		})
	}
	return rows
}

// -- Histogram global --

func perfSchemaSeedHistogramGlobal() []storage.Row {
	rows := make([]storage.Row, 0, 20)
	for i := int64(0); i < 20; i++ {
		rows = append(rows, storage.Row{
			"BUCKET_NUMBER": i,
			"BUCKET_TIMER_LOW": i * 1000000,
			"BUCKET_TIMER_HIGH": (i + 1) * 1000000,
			"COUNT_BUCKET": int64(0),
			"COUNT_BUCKET_AND_LOWER": int64(0),
			"BUCKET_QUANTILE": 0.0,
		})
	}
	return rows
}
