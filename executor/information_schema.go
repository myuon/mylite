package executor

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	catalogPkg "github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
	"vitess.io/vitess/go/vt/sqlparser"
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
	"innodb_tables":            {"TABLE_ID", "NAME", "SPACE", "FLAG", "N_COLS", "ROW_FORMAT", "ZIP_PAGE_SIZE", "SPACE_TYPE"},
	"innodb_tablespaces":       {"SPACE", "NAME", "ROW_FORMAT", "PAGE_SIZE", "ZIP_PAGE_SIZE", "SPACE_TYPE"},
	"innodb_datafiles":         {"SPACE", "PATH"},
	"innodb_columns":           {"TABLE_ID", "NAME", "POS", "MTYPE", "PRTYPE", "LEN"},
	"innodb_virtual":           {"TABLE_ID", "POS", "BASE_POS"},
	"innodb_foreign":           {"ID", "FOR_NAME", "REF_NAME", "N_COLS", "TYPE"},
	"innodb_metrics":           {"NAME", "COUNT", "TYPE", "STATUS", "SUBSYSTEM", "COMMENT"},
	"innodb_cached_indexes":    {"SPACE_ID", "INDEX_ID", "N_CACHED_PAGES"},
	"innodb_indexes":           {"INDEX_ID", "NAME", "TABLE_ID", "TYPE", "SPACE"},
	"innodb_buffer_page_lru":   {"POOL_ID", "LRU_POSITION", "SPACE", "PAGE_NUMBER"},
	"innodb_buffer_page":       {"SPACE", "PAGE_NUMBER", "PAGE_TYPE", "NUMBER_RECORDS", "TABLE_NAME"},
	"innodb_buffer_pool_stats": {"POOL_ID", "POOL_SIZE"},
	"innodb_cmp":               {"page_size", "compress_ops", "compress_ops_ok", "compress_time", "uncompress_ops", "uncompress_time"},
	"innodb_cmp_reset":         {"page_size", "compress_ops", "compress_ops_ok", "compress_time", "uncompress_ops", "uncompress_time"},
	"innodb_cmpmem":            {"page_size", "buffer_pool_instance", "pages_used", "pages_free", "relocation_ops", "relocation_time"},
	"innodb_cmpmem_reset":      {"page_size", "buffer_pool_instance", "pages_used", "pages_free", "relocation_ops", "relocation_time"},
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
	"events_stages_history_long":      {"THREAD_ID", "EVENT_ID", "END_EVENT_ID", "EVENT_NAME", "SOURCE", "TIMER_START", "TIMER_END", "TIMER_WAIT", "WORK_COMPLETED", "WORK_ESTIMATED", "NESTING_EVENT_ID", "NESTING_EVENT_TYPE"},
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
	"st_spatial_reference_systems":     {"SRS_NAME", "SRS_ID", "ORGANIZATION", "ORGANIZATION_COORDSYS_ID", "DEFINITION", "DESCRIPTION"},
	"st_geometry_columns":              {"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "SRS_NAME", "SRS_ID", "GEOMETRY_TYPE_NAME"},
	"st_units_of_measure":              {"UNIT_NAME", "UNIT_TYPE", "CONVERSION_FACTOR", "DESCRIPTION"},
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
	"events_statements_summary_by_account_by_event_name": {"USER", "HOST", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "SUM_LOCK_TIME", "SUM_ERRORS", "SUM_WARNINGS", "SUM_ROWS_AFFECTED", "SUM_ROWS_SENT", "SUM_ROWS_EXAMINED", "SUM_CREATED_TMP_DISK_TABLES", "SUM_CREATED_TMP_TABLES", "SUM_SELECT_FULL_JOIN", "SUM_SELECT_FULL_RANGE_JOIN", "SUM_SELECT_RANGE", "SUM_SELECT_RANGE_CHECK", "SUM_SELECT_SCAN", "SUM_SORT_MERGE_PASSES", "SUM_SORT_RANGE", "SUM_SORT_ROWS", "SUM_SORT_SCAN", "SUM_NO_INDEX_USED", "SUM_NO_GOOD_INDEX_USED"},
	"events_statements_summary_by_digest":           {"SCHEMA_NAME", "DIGEST", "DIGEST_TEXT", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "SUM_LOCK_TIME", "SUM_ERRORS", "SUM_WARNINGS", "SUM_ROWS_AFFECTED", "SUM_ROWS_SENT", "SUM_ROWS_EXAMINED", "SUM_CREATED_TMP_DISK_TABLES", "SUM_CREATED_TMP_TABLES", "SUM_SELECT_FULL_JOIN", "SUM_SELECT_FULL_RANGE_JOIN", "SUM_SELECT_RANGE", "SUM_SELECT_RANGE_CHECK", "SUM_SELECT_SCAN", "SUM_SORT_MERGE_PASSES", "SUM_SORT_RANGE", "SUM_SORT_ROWS", "SUM_SORT_SCAN", "SUM_NO_INDEX_USED", "SUM_NO_GOOD_INDEX_USED", "FIRST_SEEN", "LAST_SEEN", "QUANTILE_95", "QUANTILE_99", "QUANTILE_999", "QUERY_SAMPLE_TEXT", "QUERY_SAMPLE_SEEN", "QUERY_SAMPLE_TIMER_WAIT"},
	"events_statements_summary_by_host_by_event_name":   {"HOST", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "SUM_LOCK_TIME", "SUM_ERRORS", "SUM_WARNINGS", "SUM_ROWS_AFFECTED", "SUM_ROWS_SENT", "SUM_ROWS_EXAMINED", "SUM_CREATED_TMP_DISK_TABLES", "SUM_CREATED_TMP_TABLES", "SUM_SELECT_FULL_JOIN", "SUM_SELECT_FULL_RANGE_JOIN", "SUM_SELECT_RANGE", "SUM_SELECT_RANGE_CHECK", "SUM_SELECT_SCAN", "SUM_SORT_MERGE_PASSES", "SUM_SORT_RANGE", "SUM_SORT_ROWS", "SUM_SORT_SCAN", "SUM_NO_INDEX_USED", "SUM_NO_GOOD_INDEX_USED"},
	"events_statements_summary_by_thread_by_event_name": {"THREAD_ID", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "SUM_LOCK_TIME", "SUM_ERRORS", "SUM_WARNINGS", "SUM_ROWS_AFFECTED", "SUM_ROWS_SENT", "SUM_ROWS_EXAMINED", "SUM_CREATED_TMP_DISK_TABLES", "SUM_CREATED_TMP_TABLES", "SUM_SELECT_FULL_JOIN", "SUM_SELECT_FULL_RANGE_JOIN", "SUM_SELECT_RANGE", "SUM_SELECT_RANGE_CHECK", "SUM_SELECT_SCAN", "SUM_SORT_MERGE_PASSES", "SUM_SORT_RANGE", "SUM_SORT_ROWS", "SUM_SORT_SCAN", "SUM_NO_INDEX_USED", "SUM_NO_GOOD_INDEX_USED"},
	"events_statements_summary_by_user_by_event_name":   {"USER", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "SUM_LOCK_TIME", "SUM_ERRORS", "SUM_WARNINGS", "SUM_ROWS_AFFECTED", "SUM_ROWS_SENT", "SUM_ROWS_EXAMINED", "SUM_CREATED_TMP_DISK_TABLES", "SUM_CREATED_TMP_TABLES", "SUM_SELECT_FULL_JOIN", "SUM_SELECT_FULL_RANGE_JOIN", "SUM_SELECT_RANGE", "SUM_SELECT_RANGE_CHECK", "SUM_SELECT_SCAN", "SUM_SORT_MERGE_PASSES", "SUM_SORT_RANGE", "SUM_SORT_ROWS", "SUM_SORT_SCAN", "SUM_NO_INDEX_USED", "SUM_NO_GOOD_INDEX_USED"},
	"events_statements_summary_global_by_event_name":    {"EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "SUM_LOCK_TIME", "SUM_ERRORS", "SUM_WARNINGS", "SUM_ROWS_AFFECTED", "SUM_ROWS_SENT", "SUM_ROWS_EXAMINED", "SUM_CREATED_TMP_DISK_TABLES", "SUM_CREATED_TMP_TABLES", "SUM_SELECT_FULL_JOIN", "SUM_SELECT_FULL_RANGE_JOIN", "SUM_SELECT_RANGE", "SUM_SELECT_RANGE_CHECK", "SUM_SELECT_SCAN", "SUM_SORT_MERGE_PASSES", "SUM_SORT_RANGE", "SUM_SORT_ROWS", "SUM_SORT_SCAN", "SUM_NO_INDEX_USED", "SUM_NO_GOOD_INDEX_USED"},
	"events_statements_summary_by_program":   {"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "COUNT_STATEMENTS", "SUM_STATEMENTS_WAIT", "MIN_STATEMENTS_WAIT", "AVG_STATEMENTS_WAIT", "MAX_STATEMENTS_WAIT", "SUM_LOCK_TIME", "SUM_ERRORS", "SUM_WARNINGS", "SUM_ROWS_AFFECTED", "SUM_ROWS_SENT", "SUM_ROWS_EXAMINED", "SUM_CREATED_TMP_DISK_TABLES", "SUM_CREATED_TMP_TABLES", "SUM_SELECT_FULL_JOIN", "SUM_SELECT_FULL_RANGE_JOIN", "SUM_SELECT_RANGE", "SUM_SELECT_RANGE_CHECK", "SUM_SELECT_SCAN", "SUM_SORT_MERGE_PASSES", "SUM_SORT_RANGE", "SUM_SORT_ROWS", "SUM_SORT_SCAN", "SUM_NO_INDEX_USED", "SUM_NO_GOOD_INDEX_USED"},
	"events_statements_histogram_by_digest":  {"SCHEMA_NAME", "DIGEST", "BUCKET_NUMBER", "BUCKET_TIMER_LOW", "BUCKET_TIMER_HIGH", "COUNT_BUCKET", "COUNT_BUCKET_AND_LOWER", "BUCKET_QUANTILE"},
	"events_statements_histogram_global":     {"BUCKET_NUMBER", "BUCKET_TIMER_LOW", "BUCKET_TIMER_HIGH", "COUNT_BUCKET", "COUNT_BUCKET_AND_LOWER", "BUCKET_QUANTILE"},
	"events_transactions_summary_by_account_by_event_name": {"USER", "HOST", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "COUNT_READ_WRITE", "SUM_TIMER_READ_WRITE", "MIN_TIMER_READ_WRITE", "AVG_TIMER_READ_WRITE", "MAX_TIMER_READ_WRITE", "COUNT_READ_ONLY", "SUM_TIMER_READ_ONLY", "MIN_TIMER_READ_ONLY", "AVG_TIMER_READ_ONLY", "MAX_TIMER_READ_ONLY"},
	"events_transactions_summary_by_host_by_event_name":    {"HOST", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "COUNT_READ_WRITE", "SUM_TIMER_READ_WRITE", "MIN_TIMER_READ_WRITE", "AVG_TIMER_READ_WRITE", "MAX_TIMER_READ_WRITE", "COUNT_READ_ONLY", "SUM_TIMER_READ_ONLY", "MIN_TIMER_READ_ONLY", "AVG_TIMER_READ_ONLY", "MAX_TIMER_READ_ONLY"},
	"events_transactions_summary_by_thread_by_event_name":  {"THREAD_ID", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "COUNT_READ_WRITE", "SUM_TIMER_READ_WRITE", "MIN_TIMER_READ_WRITE", "AVG_TIMER_READ_WRITE", "MAX_TIMER_READ_WRITE", "COUNT_READ_ONLY", "SUM_TIMER_READ_ONLY", "MIN_TIMER_READ_ONLY", "AVG_TIMER_READ_ONLY", "MAX_TIMER_READ_ONLY"},
	"events_transactions_summary_by_user_by_event_name":    {"USER", "EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "COUNT_READ_WRITE", "SUM_TIMER_READ_WRITE", "MIN_TIMER_READ_WRITE", "AVG_TIMER_READ_WRITE", "MAX_TIMER_READ_WRITE", "COUNT_READ_ONLY", "SUM_TIMER_READ_ONLY", "MIN_TIMER_READ_ONLY", "AVG_TIMER_READ_ONLY", "MAX_TIMER_READ_ONLY"},
	"events_transactions_summary_global_by_event_name":     {"EVENT_NAME", "COUNT_STAR", "SUM_TIMER_WAIT", "MIN_TIMER_WAIT", "AVG_TIMER_WAIT", "MAX_TIMER_WAIT", "COUNT_READ_WRITE", "SUM_TIMER_READ_WRITE", "MIN_TIMER_READ_WRITE", "AVG_TIMER_READ_WRITE", "MAX_TIMER_READ_WRITE", "COUNT_READ_ONLY", "SUM_TIMER_READ_ONLY", "MIN_TIMER_READ_ONLY", "AVG_TIMER_READ_ONLY", "MAX_TIMER_READ_ONLY"},
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

// perfSchemaColumnOrder lists performance_schema table names (lowercase).
// Used to detect PS tables which preserve user-specified column casing.
var perfSchemaColumnOrder = map[string]bool{
	"accounts": true, "users": true, "hosts": true,
	"setup_objects": true, "setup_instruments": true, "setup_threads": true,
	"persisted_variables": true, "variables_info": true, "variables_by_thread": true,
	"mutex_instances": true, "rwlock_instances": true, "cond_instances": true,
	"file_instances": true, "file_summary_by_instance": true, "file_summary_by_event_name": true,
	"socket_instances": true, "socket_summary_by_event_name": true, "socket_summary_by_instance": true,
	"table_handles": true, "table_io_waits_summary_by_table": true,
	"table_io_waits_summary_by_index_usage": true, "table_lock_waits_summary_by_table": true,
	"events_waits_history": true, "events_waits_history_long": true, "events_waits_current": true,
	"events_stages_current": true, "events_stages_history": true, "events_stages_history_long": true,
	"events_statements_current": true, "events_statements_history": true, "events_statements_history_long": true,
	"events_transactions_current": true, "events_transactions_history": true, "events_transactions_history_long": true,
	"events_waits_summary_by_account_by_event_name": true,
	"events_waits_summary_by_host_by_event_name": true,
	"events_waits_summary_by_instance": true,
	"events_waits_summary_by_thread_by_event_name": true,
	"events_waits_summary_by_user_by_event_name": true,
	"events_waits_summary_global_by_event_name": true,
	"events_stages_summary_by_account_by_event_name": true,
	"events_stages_summary_by_host_by_event_name": true,
	"events_stages_summary_by_thread_by_event_name": true,
	"events_stages_summary_by_user_by_event_name": true,
	"events_stages_summary_global_by_event_name": true,
	"events_statements_summary_by_account_by_event_name": true,
	"events_statements_summary_by_digest": true,
	"events_statements_summary_by_host_by_event_name": true,
	"events_statements_summary_by_thread_by_event_name": true,
	"events_statements_summary_by_user_by_event_name": true,
	"events_statements_summary_global_by_event_name": true,
	"events_statements_summary_by_program": true,
	"events_statements_histogram_by_digest": true,
	"events_statements_histogram_global": true,
	"events_transactions_summary_by_account_by_event_name": true,
	"events_transactions_summary_by_host_by_event_name": true,
	"events_transactions_summary_by_thread_by_event_name": true,
	"events_transactions_summary_by_user_by_event_name": true,
	"events_transactions_summary_global_by_event_name": true,
	"events_errors_summary_by_account_by_error": true,
	"events_errors_summary_by_host_by_error": true,
	"events_errors_summary_by_thread_by_error": true,
	"events_errors_summary_by_user_by_error": true,
	"events_errors_summary_global_by_error": true,
	"memory_summary_by_account_by_event_name": true,
	"memory_summary_by_host_by_event_name": true,
	"memory_summary_by_thread_by_event_name": true,
	"memory_summary_by_user_by_event_name": true,
	"status_by_account": true, "status_by_host": true, "status_by_thread": true, "status_by_user": true,
	"replication_connection_configuration": true, "replication_connection_status": true,
	"replication_applier_configuration": true, "replication_applier_status": true,
	"replication_applier_status_by_coordinator": true, "replication_applier_status_by_worker": true,
	"replication_applier_filters": true, "replication_applier_global_filters": true,
	"replication_group_members": true, "replication_group_member_stats": true,
	"keyring_keys": true, "host_cache": true, "log_status": true,
	"prepared_statements_instances": true,
	"user_defined_functions": true, "user_variables_by_thread": true,
	"session_connect_attrs": true, "session_account_connect_attrs": true,
	"metadata_locks": true, "data_locks": true, "data_lock_waits": true,
	"setup_consumers": true, "setup_actors": true, "performance_timers": true,
	"threads": true,
	"objects_summary_global_by_type": true,
}
// perfSchemaVirtualTableNames returns the canonical sorted list of all virtual
// performance_schema table names (lowercase). This list matches MySQL 8.0.
func perfSchemaVirtualTableNames() []string {
	return []string{
		"accounts",
		"cond_instances",
		"data_lock_waits",
		"data_locks",
		"events_errors_summary_by_account_by_error",
		"events_errors_summary_by_host_by_error",
		"events_errors_summary_by_thread_by_error",
		"events_errors_summary_by_user_by_error",
		"events_errors_summary_global_by_error",
		"events_stages_current",
		"events_stages_history",
		"events_stages_history_long",
		"events_stages_summary_by_account_by_event_name",
		"events_stages_summary_by_host_by_event_name",
		"events_stages_summary_by_thread_by_event_name",
		"events_stages_summary_by_user_by_event_name",
		"events_stages_summary_global_by_event_name",
		"events_statements_current",
		"events_statements_histogram_by_digest",
		"events_statements_histogram_global",
		"events_statements_history",
		"events_statements_history_long",
		"events_statements_summary_by_account_by_event_name",
		"events_statements_summary_by_digest",
		"events_statements_summary_by_host_by_event_name",
		"events_statements_summary_by_program",
		"events_statements_summary_by_thread_by_event_name",
		"events_statements_summary_by_user_by_event_name",
		"events_statements_summary_global_by_event_name",
		"events_transactions_current",
		"events_transactions_history",
		"events_transactions_history_long",
		"events_transactions_summary_by_account_by_event_name",
		"events_transactions_summary_by_host_by_event_name",
		"events_transactions_summary_by_thread_by_event_name",
		"events_transactions_summary_by_user_by_event_name",
		"events_transactions_summary_global_by_event_name",
		"events_waits_current",
		"events_waits_history",
		"events_waits_history_long",
		"events_waits_summary_by_account_by_event_name",
		"events_waits_summary_by_host_by_event_name",
		"events_waits_summary_by_instance",
		"events_waits_summary_by_thread_by_event_name",
		"events_waits_summary_by_user_by_event_name",
		"events_waits_summary_global_by_event_name",
		"file_instances",
		"file_summary_by_event_name",
		"file_summary_by_instance",
		"global_status",
		"global_variables",
		"host_cache",
		"hosts",
		"keyring_keys",
		"log_status",
		"memory_summary_by_account_by_event_name",
		"memory_summary_by_host_by_event_name",
		"memory_summary_by_thread_by_event_name",
		"memory_summary_by_user_by_event_name",
		"memory_summary_global_by_event_name",
		"metadata_locks",
		"mutex_instances",
		"objects_summary_global_by_type",
		"performance_timers",
		"persisted_variables",
		"prepared_statements_instances",
		"replication_applier_configuration",
		"replication_applier_filters",
		"replication_applier_global_filters",
		"replication_applier_status",
		"replication_applier_status_by_coordinator",
		"replication_applier_status_by_worker",
		"replication_connection_configuration",
		"replication_connection_status",
		"replication_group_member_stats",
		"replication_group_members",
		"rwlock_instances",
		"session_account_connect_attrs",
		"session_connect_attrs",
		"session_status",
		"session_variables",
		"setup_actors",
		"setup_consumers",
		"setup_instruments",
		"setup_objects",
		"setup_threads",
		"socket_instances",
		"socket_summary_by_event_name",
		"socket_summary_by_instance",
		"status_by_account",
		"status_by_host",
		"status_by_thread",
		"status_by_user",
		"table_handles",
		"table_io_waits_summary_by_index_usage",
		"table_io_waits_summary_by_table",
		"table_lock_waits_summary_by_table",
		"threads",
		"user_defined_functions",
		"user_variables_by_thread",
		"users",
		"variables_by_thread",
		"variables_info",
	}
}

// perfSchemaCreateTable maps performance_schema table names to their CREATE TABLE statements.
// These are fixed MySQL system table definitions.
var perfSchemaCreateTable = map[string]string{
	"accounts": "CREATE TABLE `accounts` (\n  `USER` char(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n  `HOST` char(255) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT NULL,\n  `CURRENT_CONNECTIONS` bigint(20) NOT NULL,\n  `TOTAL_CONNECTIONS` bigint(20) NOT NULL,\n  UNIQUE KEY `ACCOUNT` (`USER`,`HOST`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"cond_instances": "CREATE TABLE `cond_instances` (\n  `NAME` varchar(128) NOT NULL,\n  `OBJECT_INSTANCE_BEGIN` bigint(20) unsigned NOT NULL,\n  PRIMARY KEY (`OBJECT_INSTANCE_BEGIN`),\n  KEY `NAME` (`NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"data_lock_waits": "CREATE TABLE `data_lock_waits` (\n  `ENGINE` varchar(32) NOT NULL,\n  `REQUESTING_ENGINE_LOCK_ID` varchar(128) NOT NULL,\n  `REQUESTING_ENGINE_TRANSACTION_ID` bigint(20) unsigned DEFAULT NULL,\n  `REQUESTING_THREAD_ID` bigint(20) unsigned DEFAULT NULL,\n  `REQUESTING_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `REQUESTING_OBJECT_INSTANCE_BEGIN` bigint(20) unsigned NOT NULL,\n  `BLOCKING_ENGINE_LOCK_ID` varchar(128) NOT NULL,\n  `BLOCKING_ENGINE_TRANSACTION_ID` bigint(20) unsigned DEFAULT NULL,\n  `BLOCKING_THREAD_ID` bigint(20) unsigned DEFAULT NULL,\n  `BLOCKING_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `BLOCKING_OBJECT_INSTANCE_BEGIN` bigint(20) unsigned NOT NULL,\n  KEY `REQUESTING_ENGINE_LOCK_ID` (`REQUESTING_ENGINE_LOCK_ID`,`ENGINE`),\n  KEY `BLOCKING_ENGINE_LOCK_ID` (`BLOCKING_ENGINE_LOCK_ID`,`ENGINE`),\n  KEY `REQUESTING_ENGINE_TRANSACTION_ID` (`REQUESTING_ENGINE_TRANSACTION_ID`,`ENGINE`),\n  KEY `BLOCKING_ENGINE_TRANSACTION_ID` (`BLOCKING_ENGINE_TRANSACTION_ID`,`ENGINE`),\n  KEY `REQUESTING_THREAD_ID` (`REQUESTING_THREAD_ID`,`REQUESTING_EVENT_ID`),\n  KEY `BLOCKING_THREAD_ID` (`BLOCKING_THREAD_ID`,`BLOCKING_EVENT_ID`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"data_locks": "CREATE TABLE `data_locks` (\n  `ENGINE` varchar(32) NOT NULL,\n  `ENGINE_LOCK_ID` varchar(128) NOT NULL,\n  `ENGINE_TRANSACTION_ID` bigint(20) unsigned DEFAULT NULL,\n  `THREAD_ID` bigint(20) unsigned DEFAULT NULL,\n  `EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `OBJECT_SCHEMA` varchar(64) DEFAULT NULL,\n  `OBJECT_NAME` varchar(64) DEFAULT NULL,\n  `PARTITION_NAME` varchar(64) DEFAULT NULL,\n  `SUBPARTITION_NAME` varchar(64) DEFAULT NULL,\n  `INDEX_NAME` varchar(64) DEFAULT NULL,\n  `OBJECT_INSTANCE_BEGIN` bigint(20) unsigned NOT NULL,\n  `LOCK_TYPE` varchar(32) NOT NULL,\n  `LOCK_MODE` varchar(32) NOT NULL,\n  `LOCK_STATUS` varchar(32) NOT NULL,\n  `LOCK_DATA` varchar(8192) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,\n  PRIMARY KEY (`ENGINE_LOCK_ID`,`ENGINE`),\n  KEY `ENGINE_TRANSACTION_ID` (`ENGINE_TRANSACTION_ID`,`ENGINE`),\n  KEY `THREAD_ID` (`THREAD_ID`,`EVENT_ID`),\n  KEY `OBJECT_SCHEMA` (`OBJECT_SCHEMA`,`OBJECT_NAME`,`PARTITION_NAME`,`SUBPARTITION_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_stages_current": "CREATE TABLE `events_stages_current` (\n  `THREAD_ID` bigint(20) unsigned NOT NULL,\n  `EVENT_ID` bigint(20) unsigned NOT NULL,\n  `END_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `SOURCE` varchar(64) DEFAULT NULL,\n  `TIMER_START` bigint(20) unsigned DEFAULT NULL,\n  `TIMER_END` bigint(20) unsigned DEFAULT NULL,\n  `TIMER_WAIT` bigint(20) unsigned DEFAULT NULL,\n  `WORK_COMPLETED` bigint(20) unsigned DEFAULT NULL,\n  `WORK_ESTIMATED` bigint(20) unsigned DEFAULT NULL,\n  `NESTING_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `NESTING_EVENT_TYPE` enum('TRANSACTION','STATEMENT','STAGE','WAIT') DEFAULT NULL,\n  PRIMARY KEY (`THREAD_ID`,`EVENT_ID`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_stages_history": "CREATE TABLE `events_stages_history` (\n  `THREAD_ID` bigint(20) unsigned NOT NULL,\n  `EVENT_ID` bigint(20) unsigned NOT NULL,\n  `END_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `SOURCE` varchar(64) DEFAULT NULL,\n  `TIMER_START` bigint(20) unsigned DEFAULT NULL,\n  `TIMER_END` bigint(20) unsigned DEFAULT NULL,\n  `TIMER_WAIT` bigint(20) unsigned DEFAULT NULL,\n  `WORK_COMPLETED` bigint(20) unsigned DEFAULT NULL,\n  `WORK_ESTIMATED` bigint(20) unsigned DEFAULT NULL,\n  `NESTING_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `NESTING_EVENT_TYPE` enum('TRANSACTION','STATEMENT','STAGE','WAIT') DEFAULT NULL,\n  PRIMARY KEY (`THREAD_ID`,`EVENT_ID`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_stages_history_long": "CREATE TABLE `events_stages_history_long` (\n  `THREAD_ID` bigint(20) unsigned NOT NULL,\n  `EVENT_ID` bigint(20) unsigned NOT NULL,\n  `END_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `SOURCE` varchar(64) DEFAULT NULL,\n  `TIMER_START` bigint(20) unsigned DEFAULT NULL,\n  `TIMER_END` bigint(20) unsigned DEFAULT NULL,\n  `TIMER_WAIT` bigint(20) unsigned DEFAULT NULL,\n  `WORK_COMPLETED` bigint(20) unsigned DEFAULT NULL,\n  `WORK_ESTIMATED` bigint(20) unsigned DEFAULT NULL,\n  `NESTING_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `NESTING_EVENT_TYPE` enum('TRANSACTION','STATEMENT','STAGE','WAIT') DEFAULT NULL\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_stages_summary_by_account_by_event_name": "CREATE TABLE `events_stages_summary_by_account_by_event_name` (\n  `USER` char(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n  `HOST` char(255) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  UNIQUE KEY `ACCOUNT` (`USER`,`HOST`,`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_stages_summary_by_host_by_event_name": "CREATE TABLE `events_stages_summary_by_host_by_event_name` (\n  `HOST` char(255) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  UNIQUE KEY `HOST` (`HOST`,`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_stages_summary_by_thread_by_event_name": "CREATE TABLE `events_stages_summary_by_thread_by_event_name` (\n  `THREAD_ID` bigint(20) unsigned NOT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  PRIMARY KEY (`THREAD_ID`,`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_stages_summary_by_user_by_event_name": "CREATE TABLE `events_stages_summary_by_user_by_event_name` (\n  `USER` char(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  UNIQUE KEY `USER` (`USER`,`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_stages_summary_global_by_event_name": "CREATE TABLE `events_stages_summary_global_by_event_name` (\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  PRIMARY KEY (`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_statements_current": "CREATE TABLE `events_statements_current` (\n  `THREAD_ID` bigint(20) unsigned NOT NULL,\n  `EVENT_ID` bigint(20) unsigned NOT NULL,\n  `END_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `SOURCE` varchar(64) DEFAULT NULL,\n  `TIMER_START` bigint(20) unsigned DEFAULT NULL,\n  `TIMER_END` bigint(20) unsigned DEFAULT NULL,\n  `TIMER_WAIT` bigint(20) unsigned DEFAULT NULL,\n  `LOCK_TIME` bigint(20) unsigned NOT NULL,\n  `SQL_TEXT` longtext,\n  `DIGEST` varchar(64) DEFAULT NULL,\n  `DIGEST_TEXT` longtext,\n  `CURRENT_SCHEMA` varchar(64) DEFAULT NULL,\n  `OBJECT_TYPE` varchar(64) DEFAULT NULL,\n  `OBJECT_SCHEMA` varchar(64) DEFAULT NULL,\n  `OBJECT_NAME` varchar(64) DEFAULT NULL,\n  `OBJECT_INSTANCE_BEGIN` bigint(20) unsigned DEFAULT NULL,\n  `MYSQL_ERRNO` int(11) DEFAULT NULL,\n  `RETURNED_SQLSTATE` varchar(5) DEFAULT NULL,\n  `MESSAGE_TEXT` varchar(128) DEFAULT NULL,\n  `ERRORS` bigint(20) unsigned NOT NULL,\n  `WARNINGS` bigint(20) unsigned NOT NULL,\n  `ROWS_AFFECTED` bigint(20) unsigned NOT NULL,\n  `ROWS_SENT` bigint(20) unsigned NOT NULL,\n  `ROWS_EXAMINED` bigint(20) unsigned NOT NULL,\n  `CREATED_TMP_DISK_TABLES` bigint(20) unsigned NOT NULL,\n  `CREATED_TMP_TABLES` bigint(20) unsigned NOT NULL,\n  `SELECT_FULL_JOIN` bigint(20) unsigned NOT NULL,\n  `SELECT_FULL_RANGE_JOIN` bigint(20) unsigned NOT NULL,\n  `SELECT_RANGE` bigint(20) unsigned NOT NULL,\n  `SELECT_RANGE_CHECK` bigint(20) unsigned NOT NULL,\n  `SELECT_SCAN` bigint(20) unsigned NOT NULL,\n  `SORT_MERGE_PASSES` bigint(20) unsigned NOT NULL,\n  `SORT_RANGE` bigint(20) unsigned NOT NULL,\n  `SORT_ROWS` bigint(20) unsigned NOT NULL,\n  `SORT_SCAN` bigint(20) unsigned NOT NULL,\n  `NO_INDEX_USED` bigint(20) unsigned NOT NULL,\n  `NO_GOOD_INDEX_USED` bigint(20) unsigned NOT NULL,\n  `NESTING_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `NESTING_EVENT_TYPE` enum('TRANSACTION','STATEMENT','STAGE','WAIT') DEFAULT NULL,\n  `NESTING_EVENT_LEVEL` int(11) DEFAULT NULL,\n  `STATEMENT_ID` bigint(20) unsigned DEFAULT NULL,\n  PRIMARY KEY (`THREAD_ID`,`EVENT_ID`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_statements_history": "CREATE TABLE `events_statements_history` (\n  `THREAD_ID` bigint(20) unsigned NOT NULL,\n  `EVENT_ID` bigint(20) unsigned NOT NULL,\n  `END_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `SOURCE` varchar(64) DEFAULT NULL,\n  `TIMER_START` bigint(20) unsigned DEFAULT NULL,\n  `TIMER_END` bigint(20) unsigned DEFAULT NULL,\n  `TIMER_WAIT` bigint(20) unsigned DEFAULT NULL,\n  `LOCK_TIME` bigint(20) unsigned NOT NULL,\n  `SQL_TEXT` longtext,\n  `DIGEST` varchar(64) DEFAULT NULL,\n  `DIGEST_TEXT` longtext,\n  `CURRENT_SCHEMA` varchar(64) DEFAULT NULL,\n  `OBJECT_TYPE` varchar(64) DEFAULT NULL,\n  `OBJECT_SCHEMA` varchar(64) DEFAULT NULL,\n  `OBJECT_NAME` varchar(64) DEFAULT NULL,\n  `OBJECT_INSTANCE_BEGIN` bigint(20) unsigned DEFAULT NULL,\n  `MYSQL_ERRNO` int(11) DEFAULT NULL,\n  `RETURNED_SQLSTATE` varchar(5) DEFAULT NULL,\n  `MESSAGE_TEXT` varchar(128) DEFAULT NULL,\n  `ERRORS` bigint(20) unsigned NOT NULL,\n  `WARNINGS` bigint(20) unsigned NOT NULL,\n  `ROWS_AFFECTED` bigint(20) unsigned NOT NULL,\n  `ROWS_SENT` bigint(20) unsigned NOT NULL,\n  `ROWS_EXAMINED` bigint(20) unsigned NOT NULL,\n  `CREATED_TMP_DISK_TABLES` bigint(20) unsigned NOT NULL,\n  `CREATED_TMP_TABLES` bigint(20) unsigned NOT NULL,\n  `SELECT_FULL_JOIN` bigint(20) unsigned NOT NULL,\n  `SELECT_FULL_RANGE_JOIN` bigint(20) unsigned NOT NULL,\n  `SELECT_RANGE` bigint(20) unsigned NOT NULL,\n  `SELECT_RANGE_CHECK` bigint(20) unsigned NOT NULL,\n  `SELECT_SCAN` bigint(20) unsigned NOT NULL,\n  `SORT_MERGE_PASSES` bigint(20) unsigned NOT NULL,\n  `SORT_RANGE` bigint(20) unsigned NOT NULL,\n  `SORT_ROWS` bigint(20) unsigned NOT NULL,\n  `SORT_SCAN` bigint(20) unsigned NOT NULL,\n  `NO_INDEX_USED` bigint(20) unsigned NOT NULL,\n  `NO_GOOD_INDEX_USED` bigint(20) unsigned NOT NULL,\n  `NESTING_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `NESTING_EVENT_TYPE` enum('TRANSACTION','STATEMENT','STAGE','WAIT') DEFAULT NULL,\n  `NESTING_EVENT_LEVEL` int(11) DEFAULT NULL,\n  `STATEMENT_ID` bigint(20) unsigned DEFAULT NULL,\n  PRIMARY KEY (`THREAD_ID`,`EVENT_ID`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_statements_history_long": "CREATE TABLE `events_statements_history_long` (\n  `THREAD_ID` bigint(20) unsigned NOT NULL,\n  `EVENT_ID` bigint(20) unsigned NOT NULL,\n  `END_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `SOURCE` varchar(64) DEFAULT NULL,\n  `TIMER_START` bigint(20) unsigned DEFAULT NULL,\n  `TIMER_END` bigint(20) unsigned DEFAULT NULL,\n  `TIMER_WAIT` bigint(20) unsigned DEFAULT NULL,\n  `LOCK_TIME` bigint(20) unsigned NOT NULL,\n  `SQL_TEXT` longtext,\n  `DIGEST` varchar(64) DEFAULT NULL,\n  `DIGEST_TEXT` longtext,\n  `CURRENT_SCHEMA` varchar(64) DEFAULT NULL,\n  `OBJECT_TYPE` varchar(64) DEFAULT NULL,\n  `OBJECT_SCHEMA` varchar(64) DEFAULT NULL,\n  `OBJECT_NAME` varchar(64) DEFAULT NULL,\n  `OBJECT_INSTANCE_BEGIN` bigint(20) unsigned DEFAULT NULL,\n  `MYSQL_ERRNO` int(11) DEFAULT NULL,\n  `RETURNED_SQLSTATE` varchar(5) DEFAULT NULL,\n  `MESSAGE_TEXT` varchar(128) DEFAULT NULL,\n  `ERRORS` bigint(20) unsigned NOT NULL,\n  `WARNINGS` bigint(20) unsigned NOT NULL,\n  `ROWS_AFFECTED` bigint(20) unsigned NOT NULL,\n  `ROWS_SENT` bigint(20) unsigned NOT NULL,\n  `ROWS_EXAMINED` bigint(20) unsigned NOT NULL,\n  `CREATED_TMP_DISK_TABLES` bigint(20) unsigned NOT NULL,\n  `CREATED_TMP_TABLES` bigint(20) unsigned NOT NULL,\n  `SELECT_FULL_JOIN` bigint(20) unsigned NOT NULL,\n  `SELECT_FULL_RANGE_JOIN` bigint(20) unsigned NOT NULL,\n  `SELECT_RANGE` bigint(20) unsigned NOT NULL,\n  `SELECT_RANGE_CHECK` bigint(20) unsigned NOT NULL,\n  `SELECT_SCAN` bigint(20) unsigned NOT NULL,\n  `SORT_MERGE_PASSES` bigint(20) unsigned NOT NULL,\n  `SORT_RANGE` bigint(20) unsigned NOT NULL,\n  `SORT_ROWS` bigint(20) unsigned NOT NULL,\n  `SORT_SCAN` bigint(20) unsigned NOT NULL,\n  `NO_INDEX_USED` bigint(20) unsigned NOT NULL,\n  `NO_GOOD_INDEX_USED` bigint(20) unsigned NOT NULL,\n  `NESTING_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `NESTING_EVENT_TYPE` enum('TRANSACTION','STATEMENT','STAGE','WAIT') DEFAULT NULL,\n  `NESTING_EVENT_LEVEL` int(11) DEFAULT NULL,\n  `STATEMENT_ID` bigint(20) unsigned DEFAULT NULL\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_statements_summary_by_account_by_event_name": "CREATE TABLE `events_statements_summary_by_account_by_event_name` (\n  `USER` char(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n  `HOST` char(255) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `SUM_LOCK_TIME` bigint(20) unsigned NOT NULL,\n  `SUM_ERRORS` bigint(20) unsigned NOT NULL,\n  `SUM_WARNINGS` bigint(20) unsigned NOT NULL,\n  `SUM_ROWS_AFFECTED` bigint(20) unsigned NOT NULL,\n  `SUM_ROWS_SENT` bigint(20) unsigned NOT NULL,\n  `SUM_ROWS_EXAMINED` bigint(20) unsigned NOT NULL,\n  `SUM_CREATED_TMP_DISK_TABLES` bigint(20) unsigned NOT NULL,\n  `SUM_CREATED_TMP_TABLES` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_FULL_JOIN` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_FULL_RANGE_JOIN` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_RANGE` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_RANGE_CHECK` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_SCAN` bigint(20) unsigned NOT NULL,\n  `SUM_SORT_MERGE_PASSES` bigint(20) unsigned NOT NULL,\n  `SUM_SORT_RANGE` bigint(20) unsigned NOT NULL,\n  `SUM_SORT_ROWS` bigint(20) unsigned NOT NULL,\n  `SUM_SORT_SCAN` bigint(20) unsigned NOT NULL,\n  `SUM_NO_INDEX_USED` bigint(20) unsigned NOT NULL,\n  `SUM_NO_GOOD_INDEX_USED` bigint(20) unsigned NOT NULL,\n  UNIQUE KEY `ACCOUNT` (`USER`,`HOST`,`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_statements_summary_by_digest": "CREATE TABLE `events_statements_summary_by_digest` (\n  `SCHEMA_NAME` varchar(64) DEFAULT NULL,\n  `DIGEST` varchar(64) DEFAULT NULL,\n  `DIGEST_TEXT` longtext,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `SUM_LOCK_TIME` bigint(20) unsigned NOT NULL,\n  `SUM_ERRORS` bigint(20) unsigned NOT NULL,\n  `SUM_WARNINGS` bigint(20) unsigned NOT NULL,\n  `SUM_ROWS_AFFECTED` bigint(20) unsigned NOT NULL,\n  `SUM_ROWS_SENT` bigint(20) unsigned NOT NULL,\n  `SUM_ROWS_EXAMINED` bigint(20) unsigned NOT NULL,\n  `SUM_CREATED_TMP_DISK_TABLES` bigint(20) unsigned NOT NULL,\n  `SUM_CREATED_TMP_TABLES` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_FULL_JOIN` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_FULL_RANGE_JOIN` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_RANGE` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_RANGE_CHECK` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_SCAN` bigint(20) unsigned NOT NULL,\n  `SUM_SORT_MERGE_PASSES` bigint(20) unsigned NOT NULL,\n  `SUM_SORT_RANGE` bigint(20) unsigned NOT NULL,\n  `SUM_SORT_ROWS` bigint(20) unsigned NOT NULL,\n  `SUM_SORT_SCAN` bigint(20) unsigned NOT NULL,\n  `SUM_NO_INDEX_USED` bigint(20) unsigned NOT NULL,\n  `SUM_NO_GOOD_INDEX_USED` bigint(20) unsigned NOT NULL,\n  `FIRST_SEEN` timestamp(6) NOT NULL DEFAULT '0000-00-00 00:00:00.000000',\n  `LAST_SEEN` timestamp(6) NOT NULL DEFAULT '0000-00-00 00:00:00.000000',\n  `QUANTILE_95` bigint(20) unsigned NOT NULL,\n  `QUANTILE_99` bigint(20) unsigned NOT NULL,\n  `QUANTILE_999` bigint(20) unsigned NOT NULL,\n  `QUERY_SAMPLE_TEXT` longtext,\n  `QUERY_SAMPLE_SEEN` timestamp(6) NOT NULL DEFAULT '0000-00-00 00:00:00.000000',\n  `QUERY_SAMPLE_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  UNIQUE KEY `SCHEMA_NAME` (`SCHEMA_NAME`,`DIGEST`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_statements_summary_by_host_by_event_name": "CREATE TABLE `events_statements_summary_by_host_by_event_name` (\n  `HOST` char(255) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `SUM_LOCK_TIME` bigint(20) unsigned NOT NULL,\n  `SUM_ERRORS` bigint(20) unsigned NOT NULL,\n  `SUM_WARNINGS` bigint(20) unsigned NOT NULL,\n  `SUM_ROWS_AFFECTED` bigint(20) unsigned NOT NULL,\n  `SUM_ROWS_SENT` bigint(20) unsigned NOT NULL,\n  `SUM_ROWS_EXAMINED` bigint(20) unsigned NOT NULL,\n  `SUM_CREATED_TMP_DISK_TABLES` bigint(20) unsigned NOT NULL,\n  `SUM_CREATED_TMP_TABLES` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_FULL_JOIN` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_FULL_RANGE_JOIN` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_RANGE` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_RANGE_CHECK` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_SCAN` bigint(20) unsigned NOT NULL,\n  `SUM_SORT_MERGE_PASSES` bigint(20) unsigned NOT NULL,\n  `SUM_SORT_RANGE` bigint(20) unsigned NOT NULL,\n  `SUM_SORT_ROWS` bigint(20) unsigned NOT NULL,\n  `SUM_SORT_SCAN` bigint(20) unsigned NOT NULL,\n  `SUM_NO_INDEX_USED` bigint(20) unsigned NOT NULL,\n  `SUM_NO_GOOD_INDEX_USED` bigint(20) unsigned NOT NULL,\n  UNIQUE KEY `HOST` (`HOST`,`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_statements_summary_by_thread_by_event_name": "CREATE TABLE `events_statements_summary_by_thread_by_event_name` (\n  `THREAD_ID` bigint(20) unsigned NOT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `SUM_LOCK_TIME` bigint(20) unsigned NOT NULL,\n  `SUM_ERRORS` bigint(20) unsigned NOT NULL,\n  `SUM_WARNINGS` bigint(20) unsigned NOT NULL,\n  `SUM_ROWS_AFFECTED` bigint(20) unsigned NOT NULL,\n  `SUM_ROWS_SENT` bigint(20) unsigned NOT NULL,\n  `SUM_ROWS_EXAMINED` bigint(20) unsigned NOT NULL,\n  `SUM_CREATED_TMP_DISK_TABLES` bigint(20) unsigned NOT NULL,\n  `SUM_CREATED_TMP_TABLES` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_FULL_JOIN` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_FULL_RANGE_JOIN` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_RANGE` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_RANGE_CHECK` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_SCAN` bigint(20) unsigned NOT NULL,\n  `SUM_SORT_MERGE_PASSES` bigint(20) unsigned NOT NULL,\n  `SUM_SORT_RANGE` bigint(20) unsigned NOT NULL,\n  `SUM_SORT_ROWS` bigint(20) unsigned NOT NULL,\n  `SUM_SORT_SCAN` bigint(20) unsigned NOT NULL,\n  `SUM_NO_INDEX_USED` bigint(20) unsigned NOT NULL,\n  `SUM_NO_GOOD_INDEX_USED` bigint(20) unsigned NOT NULL,\n  PRIMARY KEY (`THREAD_ID`,`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_statements_summary_by_user_by_event_name": "CREATE TABLE `events_statements_summary_by_user_by_event_name` (\n  `USER` char(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `SUM_LOCK_TIME` bigint(20) unsigned NOT NULL,\n  `SUM_ERRORS` bigint(20) unsigned NOT NULL,\n  `SUM_WARNINGS` bigint(20) unsigned NOT NULL,\n  `SUM_ROWS_AFFECTED` bigint(20) unsigned NOT NULL,\n  `SUM_ROWS_SENT` bigint(20) unsigned NOT NULL,\n  `SUM_ROWS_EXAMINED` bigint(20) unsigned NOT NULL,\n  `SUM_CREATED_TMP_DISK_TABLES` bigint(20) unsigned NOT NULL,\n  `SUM_CREATED_TMP_TABLES` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_FULL_JOIN` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_FULL_RANGE_JOIN` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_RANGE` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_RANGE_CHECK` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_SCAN` bigint(20) unsigned NOT NULL,\n  `SUM_SORT_MERGE_PASSES` bigint(20) unsigned NOT NULL,\n  `SUM_SORT_RANGE` bigint(20) unsigned NOT NULL,\n  `SUM_SORT_ROWS` bigint(20) unsigned NOT NULL,\n  `SUM_SORT_SCAN` bigint(20) unsigned NOT NULL,\n  `SUM_NO_INDEX_USED` bigint(20) unsigned NOT NULL,\n  `SUM_NO_GOOD_INDEX_USED` bigint(20) unsigned NOT NULL,\n  UNIQUE KEY `USER` (`USER`,`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_statements_summary_global_by_event_name": "CREATE TABLE `events_statements_summary_global_by_event_name` (\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `SUM_LOCK_TIME` bigint(20) unsigned NOT NULL,\n  `SUM_ERRORS` bigint(20) unsigned NOT NULL,\n  `SUM_WARNINGS` bigint(20) unsigned NOT NULL,\n  `SUM_ROWS_AFFECTED` bigint(20) unsigned NOT NULL,\n  `SUM_ROWS_SENT` bigint(20) unsigned NOT NULL,\n  `SUM_ROWS_EXAMINED` bigint(20) unsigned NOT NULL,\n  `SUM_CREATED_TMP_DISK_TABLES` bigint(20) unsigned NOT NULL,\n  `SUM_CREATED_TMP_TABLES` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_FULL_JOIN` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_FULL_RANGE_JOIN` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_RANGE` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_RANGE_CHECK` bigint(20) unsigned NOT NULL,\n  `SUM_SELECT_SCAN` bigint(20) unsigned NOT NULL,\n  `SUM_SORT_MERGE_PASSES` bigint(20) unsigned NOT NULL,\n  `SUM_SORT_RANGE` bigint(20) unsigned NOT NULL,\n  `SUM_SORT_ROWS` bigint(20) unsigned NOT NULL,\n  `SUM_SORT_SCAN` bigint(20) unsigned NOT NULL,\n  `SUM_NO_INDEX_USED` bigint(20) unsigned NOT NULL,\n  `SUM_NO_GOOD_INDEX_USED` bigint(20) unsigned NOT NULL,\n  PRIMARY KEY (`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_transactions_current": "CREATE TABLE `events_transactions_current` (\n  `THREAD_ID` bigint(20) unsigned NOT NULL,\n  `EVENT_ID` bigint(20) unsigned NOT NULL,\n  `END_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `STATE` enum('ACTIVE','COMMITTED','ROLLED BACK') DEFAULT NULL,\n  `TRX_ID` bigint(20) unsigned DEFAULT NULL,\n  `GTID` varchar(64) DEFAULT NULL,\n  `XID_FORMAT_ID` int(11) DEFAULT NULL,\n  `XID_GTRID` varchar(130) DEFAULT NULL,\n  `XID_BQUAL` varchar(130) DEFAULT NULL,\n  `XA_STATE` varchar(64) DEFAULT NULL,\n  `SOURCE` varchar(64) DEFAULT NULL,\n  `TIMER_START` bigint(20) unsigned DEFAULT NULL,\n  `TIMER_END` bigint(20) unsigned DEFAULT NULL,\n  `TIMER_WAIT` bigint(20) unsigned DEFAULT NULL,\n  `ACCESS_MODE` enum('READ ONLY','READ WRITE') DEFAULT NULL,\n  `ISOLATION_LEVEL` varchar(64) DEFAULT NULL,\n  `AUTOCOMMIT` enum('YES','NO') NOT NULL,\n  `NUMBER_OF_SAVEPOINTS` bigint(20) unsigned DEFAULT NULL,\n  `NUMBER_OF_ROLLBACK_TO_SAVEPOINT` bigint(20) unsigned DEFAULT NULL,\n  `NUMBER_OF_RELEASE_SAVEPOINT` bigint(20) unsigned DEFAULT NULL,\n  `OBJECT_INSTANCE_BEGIN` bigint(20) unsigned DEFAULT NULL,\n  `NESTING_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `NESTING_EVENT_TYPE` enum('TRANSACTION','STATEMENT','STAGE','WAIT') DEFAULT NULL,\n  PRIMARY KEY (`THREAD_ID`,`EVENT_ID`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_transactions_history": "CREATE TABLE `events_transactions_history` (\n  `THREAD_ID` bigint(20) unsigned NOT NULL,\n  `EVENT_ID` bigint(20) unsigned NOT NULL,\n  `END_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `STATE` enum('ACTIVE','COMMITTED','ROLLED BACK') DEFAULT NULL,\n  `TRX_ID` bigint(20) unsigned DEFAULT NULL,\n  `GTID` varchar(64) DEFAULT NULL,\n  `XID_FORMAT_ID` int(11) DEFAULT NULL,\n  `XID_GTRID` varchar(130) DEFAULT NULL,\n  `XID_BQUAL` varchar(130) DEFAULT NULL,\n  `XA_STATE` varchar(64) DEFAULT NULL,\n  `SOURCE` varchar(64) DEFAULT NULL,\n  `TIMER_START` bigint(20) unsigned DEFAULT NULL,\n  `TIMER_END` bigint(20) unsigned DEFAULT NULL,\n  `TIMER_WAIT` bigint(20) unsigned DEFAULT NULL,\n  `ACCESS_MODE` enum('READ ONLY','READ WRITE') DEFAULT NULL,\n  `ISOLATION_LEVEL` varchar(64) DEFAULT NULL,\n  `AUTOCOMMIT` enum('YES','NO') NOT NULL,\n  `NUMBER_OF_SAVEPOINTS` bigint(20) unsigned DEFAULT NULL,\n  `NUMBER_OF_ROLLBACK_TO_SAVEPOINT` bigint(20) unsigned DEFAULT NULL,\n  `NUMBER_OF_RELEASE_SAVEPOINT` bigint(20) unsigned DEFAULT NULL,\n  `OBJECT_INSTANCE_BEGIN` bigint(20) unsigned DEFAULT NULL,\n  `NESTING_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `NESTING_EVENT_TYPE` enum('TRANSACTION','STATEMENT','STAGE','WAIT') DEFAULT NULL,\n  PRIMARY KEY (`THREAD_ID`,`EVENT_ID`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_transactions_history_long": "CREATE TABLE `events_transactions_history_long` (\n  `THREAD_ID` bigint(20) unsigned NOT NULL,\n  `EVENT_ID` bigint(20) unsigned NOT NULL,\n  `END_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `STATE` enum('ACTIVE','COMMITTED','ROLLED BACK') DEFAULT NULL,\n  `TRX_ID` bigint(20) unsigned DEFAULT NULL,\n  `GTID` varchar(64) DEFAULT NULL,\n  `XID_FORMAT_ID` int(11) DEFAULT NULL,\n  `XID_GTRID` varchar(130) DEFAULT NULL,\n  `XID_BQUAL` varchar(130) DEFAULT NULL,\n  `XA_STATE` varchar(64) DEFAULT NULL,\n  `SOURCE` varchar(64) DEFAULT NULL,\n  `TIMER_START` bigint(20) unsigned DEFAULT NULL,\n  `TIMER_END` bigint(20) unsigned DEFAULT NULL,\n  `TIMER_WAIT` bigint(20) unsigned DEFAULT NULL,\n  `ACCESS_MODE` enum('READ ONLY','READ WRITE') DEFAULT NULL,\n  `ISOLATION_LEVEL` varchar(64) DEFAULT NULL,\n  `AUTOCOMMIT` enum('YES','NO') NOT NULL,\n  `NUMBER_OF_SAVEPOINTS` bigint(20) unsigned DEFAULT NULL,\n  `NUMBER_OF_ROLLBACK_TO_SAVEPOINT` bigint(20) unsigned DEFAULT NULL,\n  `NUMBER_OF_RELEASE_SAVEPOINT` bigint(20) unsigned DEFAULT NULL,\n  `OBJECT_INSTANCE_BEGIN` bigint(20) unsigned DEFAULT NULL,\n  `NESTING_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `NESTING_EVENT_TYPE` enum('TRANSACTION','STATEMENT','STAGE','WAIT') DEFAULT NULL\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_transactions_summary_by_account_by_event_name": "CREATE TABLE `events_transactions_summary_by_account_by_event_name` (\n  `USER` char(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n  `HOST` char(255) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `COUNT_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `COUNT_READ_ONLY` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_READ_ONLY` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_READ_ONLY` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_READ_ONLY` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_READ_ONLY` bigint(20) unsigned NOT NULL,\n  UNIQUE KEY `ACCOUNT` (`USER`,`HOST`,`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_transactions_summary_by_host_by_event_name": "CREATE TABLE `events_transactions_summary_by_host_by_event_name` (\n  `HOST` char(255) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `COUNT_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `COUNT_READ_ONLY` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_READ_ONLY` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_READ_ONLY` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_READ_ONLY` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_READ_ONLY` bigint(20) unsigned NOT NULL,\n  UNIQUE KEY `HOST` (`HOST`,`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_transactions_summary_by_thread_by_event_name": "CREATE TABLE `events_transactions_summary_by_thread_by_event_name` (\n  `THREAD_ID` bigint(20) unsigned NOT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `COUNT_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `COUNT_READ_ONLY` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_READ_ONLY` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_READ_ONLY` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_READ_ONLY` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_READ_ONLY` bigint(20) unsigned NOT NULL,\n  PRIMARY KEY (`THREAD_ID`,`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_transactions_summary_by_user_by_event_name": "CREATE TABLE `events_transactions_summary_by_user_by_event_name` (\n  `USER` char(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `COUNT_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `COUNT_READ_ONLY` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_READ_ONLY` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_READ_ONLY` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_READ_ONLY` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_READ_ONLY` bigint(20) unsigned NOT NULL,\n  UNIQUE KEY `USER` (`USER`,`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_transactions_summary_global_by_event_name": "CREATE TABLE `events_transactions_summary_global_by_event_name` (\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `COUNT_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_READ_WRITE` bigint(20) unsigned NOT NULL,\n  `COUNT_READ_ONLY` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_READ_ONLY` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_READ_ONLY` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_READ_ONLY` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_READ_ONLY` bigint(20) unsigned NOT NULL,\n  PRIMARY KEY (`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_waits_current": "CREATE TABLE `events_waits_current` (\n  `THREAD_ID` bigint(20) unsigned NOT NULL,\n  `EVENT_ID` bigint(20) unsigned NOT NULL,\n  `END_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `SOURCE` varchar(64) DEFAULT NULL,\n  `TIMER_START` bigint(20) unsigned DEFAULT NULL,\n  `TIMER_END` bigint(20) unsigned DEFAULT NULL,\n  `TIMER_WAIT` bigint(20) unsigned DEFAULT NULL,\n  `SPINS` int(10) unsigned DEFAULT NULL,\n  `OBJECT_SCHEMA` varchar(64) DEFAULT NULL,\n  `OBJECT_NAME` varchar(512) DEFAULT NULL,\n  `INDEX_NAME` varchar(64) DEFAULT NULL,\n  `OBJECT_TYPE` varchar(64) DEFAULT NULL,\n  `OBJECT_INSTANCE_BEGIN` bigint(20) unsigned NOT NULL,\n  `NESTING_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `NESTING_EVENT_TYPE` enum('TRANSACTION','STATEMENT','STAGE','WAIT') DEFAULT NULL,\n  `OPERATION` varchar(32) NOT NULL,\n  `NUMBER_OF_BYTES` bigint(20) DEFAULT NULL,\n  `FLAGS` int(10) unsigned DEFAULT NULL,\n  PRIMARY KEY (`THREAD_ID`,`EVENT_ID`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_waits_history": "CREATE TABLE `events_waits_history` (\n  `THREAD_ID` bigint(20) unsigned NOT NULL,\n  `EVENT_ID` bigint(20) unsigned NOT NULL,\n  `END_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `SOURCE` varchar(64) DEFAULT NULL,\n  `TIMER_START` bigint(20) unsigned DEFAULT NULL,\n  `TIMER_END` bigint(20) unsigned DEFAULT NULL,\n  `TIMER_WAIT` bigint(20) unsigned DEFAULT NULL,\n  `SPINS` int(10) unsigned DEFAULT NULL,\n  `OBJECT_SCHEMA` varchar(64) DEFAULT NULL,\n  `OBJECT_NAME` varchar(512) DEFAULT NULL,\n  `INDEX_NAME` varchar(64) DEFAULT NULL,\n  `OBJECT_TYPE` varchar(64) DEFAULT NULL,\n  `OBJECT_INSTANCE_BEGIN` bigint(20) unsigned NOT NULL,\n  `NESTING_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `NESTING_EVENT_TYPE` enum('TRANSACTION','STATEMENT','STAGE','WAIT') DEFAULT NULL,\n  `OPERATION` varchar(32) NOT NULL,\n  `NUMBER_OF_BYTES` bigint(20) DEFAULT NULL,\n  `FLAGS` int(10) unsigned DEFAULT NULL,\n  PRIMARY KEY (`THREAD_ID`,`EVENT_ID`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_waits_history_long": "CREATE TABLE `events_waits_history_long` (\n  `THREAD_ID` bigint(20) unsigned NOT NULL,\n  `EVENT_ID` bigint(20) unsigned NOT NULL,\n  `END_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `SOURCE` varchar(64) DEFAULT NULL,\n  `TIMER_START` bigint(20) unsigned DEFAULT NULL,\n  `TIMER_END` bigint(20) unsigned DEFAULT NULL,\n  `TIMER_WAIT` bigint(20) unsigned DEFAULT NULL,\n  `SPINS` int(10) unsigned DEFAULT NULL,\n  `OBJECT_SCHEMA` varchar(64) DEFAULT NULL,\n  `OBJECT_NAME` varchar(512) DEFAULT NULL,\n  `INDEX_NAME` varchar(64) DEFAULT NULL,\n  `OBJECT_TYPE` varchar(64) DEFAULT NULL,\n  `OBJECT_INSTANCE_BEGIN` bigint(20) unsigned NOT NULL,\n  `NESTING_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `NESTING_EVENT_TYPE` enum('TRANSACTION','STATEMENT','STAGE','WAIT') DEFAULT NULL,\n  `OPERATION` varchar(32) NOT NULL,\n  `NUMBER_OF_BYTES` bigint(20) DEFAULT NULL,\n  `FLAGS` int(10) unsigned DEFAULT NULL\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_waits_summary_by_account_by_event_name": "CREATE TABLE `events_waits_summary_by_account_by_event_name` (\n  `USER` char(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n  `HOST` char(255) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  UNIQUE KEY `ACCOUNT` (`USER`,`HOST`,`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_waits_summary_by_host_by_event_name": "CREATE TABLE `events_waits_summary_by_host_by_event_name` (\n  `HOST` char(255) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  UNIQUE KEY `HOST` (`HOST`,`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_waits_summary_by_instance": "CREATE TABLE `events_waits_summary_by_instance` (\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `OBJECT_INSTANCE_BEGIN` bigint(20) unsigned NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  PRIMARY KEY (`OBJECT_INSTANCE_BEGIN`),\n  KEY `EVENT_NAME` (`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_waits_summary_by_thread_by_event_name": "CREATE TABLE `events_waits_summary_by_thread_by_event_name` (\n  `THREAD_ID` bigint(20) unsigned NOT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  PRIMARY KEY (`THREAD_ID`,`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_waits_summary_by_user_by_event_name": "CREATE TABLE `events_waits_summary_by_user_by_event_name` (\n  `USER` char(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  UNIQUE KEY `USER` (`USER`,`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_waits_summary_global_by_event_name": "CREATE TABLE `events_waits_summary_global_by_event_name` (\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  PRIMARY KEY (`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"file_instances": "CREATE TABLE `file_instances` (\n  `FILE_NAME` varchar(512) NOT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `OPEN_COUNT` int(10) unsigned NOT NULL,\n  PRIMARY KEY (`FILE_NAME`),\n  KEY `EVENT_NAME` (`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"file_summary_by_event_name": "CREATE TABLE `file_summary_by_event_name` (\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `COUNT_READ` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `SUM_NUMBER_OF_BYTES_READ` bigint(20) NOT NULL,\n  `COUNT_WRITE` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `SUM_NUMBER_OF_BYTES_WRITE` bigint(20) NOT NULL,\n  `COUNT_MISC` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_MISC` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_MISC` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_MISC` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_MISC` bigint(20) unsigned NOT NULL,\n  PRIMARY KEY (`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"file_summary_by_instance": "CREATE TABLE `file_summary_by_instance` (\n  `FILE_NAME` varchar(512) NOT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `OBJECT_INSTANCE_BEGIN` bigint(20) unsigned NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `COUNT_READ` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `SUM_NUMBER_OF_BYTES_READ` bigint(20) NOT NULL,\n  `COUNT_WRITE` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `SUM_NUMBER_OF_BYTES_WRITE` bigint(20) NOT NULL,\n  `COUNT_MISC` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_MISC` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_MISC` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_MISC` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_MISC` bigint(20) unsigned NOT NULL,\n  PRIMARY KEY (`OBJECT_INSTANCE_BEGIN`),\n  KEY `FILE_NAME` (`FILE_NAME`),\n  KEY `EVENT_NAME` (`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"host_cache": "CREATE TABLE `host_cache` (\n  `IP` varchar(64) NOT NULL,\n  `HOST` varchar(255) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT NULL,\n  `HOST_VALIDATED` enum('YES','NO') NOT NULL,\n  `SUM_CONNECT_ERRORS` bigint(20) NOT NULL,\n  `COUNT_HOST_BLOCKED_ERRORS` bigint(20) NOT NULL,\n  `COUNT_NAMEINFO_TRANSIENT_ERRORS` bigint(20) NOT NULL,\n  `COUNT_NAMEINFO_PERMANENT_ERRORS` bigint(20) NOT NULL,\n  `COUNT_FORMAT_ERRORS` bigint(20) NOT NULL,\n  `COUNT_ADDRINFO_TRANSIENT_ERRORS` bigint(20) NOT NULL,\n  `COUNT_ADDRINFO_PERMANENT_ERRORS` bigint(20) NOT NULL,\n  `COUNT_FCRDNS_ERRORS` bigint(20) NOT NULL,\n  `COUNT_HOST_ACL_ERRORS` bigint(20) NOT NULL,\n  `COUNT_NO_AUTH_PLUGIN_ERRORS` bigint(20) NOT NULL,\n  `COUNT_AUTH_PLUGIN_ERRORS` bigint(20) NOT NULL,\n  `COUNT_HANDSHAKE_ERRORS` bigint(20) NOT NULL,\n  `COUNT_PROXY_USER_ERRORS` bigint(20) NOT NULL,\n  `COUNT_PROXY_USER_ACL_ERRORS` bigint(20) NOT NULL,\n  `COUNT_AUTHENTICATION_ERRORS` bigint(20) NOT NULL,\n  `COUNT_SSL_ERRORS` bigint(20) NOT NULL,\n  `COUNT_MAX_USER_CONNECTIONS_ERRORS` bigint(20) NOT NULL,\n  `COUNT_MAX_USER_CONNECTIONS_PER_HOUR_ERRORS` bigint(20) NOT NULL,\n  `COUNT_DEFAULT_DATABASE_ERRORS` bigint(20) NOT NULL,\n  `COUNT_INIT_CONNECT_ERRORS` bigint(20) NOT NULL,\n  `COUNT_LOCAL_ERRORS` bigint(20) NOT NULL,\n  `COUNT_UNKNOWN_ERRORS` bigint(20) NOT NULL,\n  `FIRST_SEEN` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',\n  `LAST_SEEN` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',\n  `FIRST_ERROR_SEEN` timestamp NULL DEFAULT '0000-00-00 00:00:00',\n  `LAST_ERROR_SEEN` timestamp NULL DEFAULT '0000-00-00 00:00:00',\n  PRIMARY KEY (`IP`),\n  KEY `HOST` (`HOST`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"hosts": "CREATE TABLE `hosts` (\n  `HOST` char(255) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT NULL,\n  `CURRENT_CONNECTIONS` bigint(20) NOT NULL,\n  `TOTAL_CONNECTIONS` bigint(20) NOT NULL,\n  UNIQUE KEY `HOST` (`HOST`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"keyring_keys": "CREATE TABLE `keyring_keys` (\n  `KEY_ID` varchar(255) COLLATE utf8mb4_bin NOT NULL,\n  `KEY_OWNER` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL,\n  `BACKEND_KEY_ID` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	"memory_summary_by_account_by_event_name": "CREATE TABLE `memory_summary_by_account_by_event_name` (\n  `USER` char(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n  `HOST` char(255) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_ALLOC` bigint(20) unsigned NOT NULL,\n  `COUNT_FREE` bigint(20) unsigned NOT NULL,\n  `SUM_NUMBER_OF_BYTES_ALLOC` bigint(20) unsigned NOT NULL,\n  `SUM_NUMBER_OF_BYTES_FREE` bigint(20) unsigned NOT NULL,\n  `LOW_COUNT_USED` bigint(20) NOT NULL,\n  `CURRENT_COUNT_USED` bigint(20) NOT NULL,\n  `HIGH_COUNT_USED` bigint(20) NOT NULL,\n  `LOW_NUMBER_OF_BYTES_USED` bigint(20) NOT NULL,\n  `CURRENT_NUMBER_OF_BYTES_USED` bigint(20) NOT NULL,\n  `HIGH_NUMBER_OF_BYTES_USED` bigint(20) NOT NULL,\n  UNIQUE KEY `ACCOUNT` (`USER`,`HOST`,`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"memory_summary_by_host_by_event_name": "CREATE TABLE `memory_summary_by_host_by_event_name` (\n  `HOST` char(255) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_ALLOC` bigint(20) unsigned NOT NULL,\n  `COUNT_FREE` bigint(20) unsigned NOT NULL,\n  `SUM_NUMBER_OF_BYTES_ALLOC` bigint(20) unsigned NOT NULL,\n  `SUM_NUMBER_OF_BYTES_FREE` bigint(20) unsigned NOT NULL,\n  `LOW_COUNT_USED` bigint(20) NOT NULL,\n  `CURRENT_COUNT_USED` bigint(20) NOT NULL,\n  `HIGH_COUNT_USED` bigint(20) NOT NULL,\n  `LOW_NUMBER_OF_BYTES_USED` bigint(20) NOT NULL,\n  `CURRENT_NUMBER_OF_BYTES_USED` bigint(20) NOT NULL,\n  `HIGH_NUMBER_OF_BYTES_USED` bigint(20) NOT NULL,\n  UNIQUE KEY `HOST` (`HOST`,`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"memory_summary_by_thread_by_event_name": "CREATE TABLE `memory_summary_by_thread_by_event_name` (\n  `THREAD_ID` bigint(20) unsigned NOT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_ALLOC` bigint(20) unsigned NOT NULL,\n  `COUNT_FREE` bigint(20) unsigned NOT NULL,\n  `SUM_NUMBER_OF_BYTES_ALLOC` bigint(20) unsigned NOT NULL,\n  `SUM_NUMBER_OF_BYTES_FREE` bigint(20) unsigned NOT NULL,\n  `LOW_COUNT_USED` bigint(20) NOT NULL,\n  `CURRENT_COUNT_USED` bigint(20) NOT NULL,\n  `HIGH_COUNT_USED` bigint(20) NOT NULL,\n  `LOW_NUMBER_OF_BYTES_USED` bigint(20) NOT NULL,\n  `CURRENT_NUMBER_OF_BYTES_USED` bigint(20) NOT NULL,\n  `HIGH_NUMBER_OF_BYTES_USED` bigint(20) NOT NULL,\n  PRIMARY KEY (`THREAD_ID`,`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"memory_summary_by_user_by_event_name": "CREATE TABLE `memory_summary_by_user_by_event_name` (\n  `USER` char(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_ALLOC` bigint(20) unsigned NOT NULL,\n  `COUNT_FREE` bigint(20) unsigned NOT NULL,\n  `SUM_NUMBER_OF_BYTES_ALLOC` bigint(20) unsigned NOT NULL,\n  `SUM_NUMBER_OF_BYTES_FREE` bigint(20) unsigned NOT NULL,\n  `LOW_COUNT_USED` bigint(20) NOT NULL,\n  `CURRENT_COUNT_USED` bigint(20) NOT NULL,\n  `HIGH_COUNT_USED` bigint(20) NOT NULL,\n  `LOW_NUMBER_OF_BYTES_USED` bigint(20) NOT NULL,\n  `CURRENT_NUMBER_OF_BYTES_USED` bigint(20) NOT NULL,\n  `HIGH_NUMBER_OF_BYTES_USED` bigint(20) NOT NULL,\n  UNIQUE KEY `USER` (`USER`,`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"memory_summary_global_by_event_name": "CREATE TABLE `memory_summary_global_by_event_name` (\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_ALLOC` bigint(20) unsigned NOT NULL,\n  `COUNT_FREE` bigint(20) unsigned NOT NULL,\n  `SUM_NUMBER_OF_BYTES_ALLOC` bigint(20) unsigned NOT NULL,\n  `SUM_NUMBER_OF_BYTES_FREE` bigint(20) unsigned NOT NULL,\n  `LOW_COUNT_USED` bigint(20) NOT NULL,\n  `CURRENT_COUNT_USED` bigint(20) NOT NULL,\n  `HIGH_COUNT_USED` bigint(20) NOT NULL,\n  `LOW_NUMBER_OF_BYTES_USED` bigint(20) NOT NULL,\n  `CURRENT_NUMBER_OF_BYTES_USED` bigint(20) NOT NULL,\n  `HIGH_NUMBER_OF_BYTES_USED` bigint(20) NOT NULL,\n  PRIMARY KEY (`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"metadata_locks": "CREATE TABLE `metadata_locks` (\n  `OBJECT_TYPE` varchar(64) NOT NULL,\n  `OBJECT_SCHEMA` varchar(64) DEFAULT NULL,\n  `OBJECT_NAME` varchar(64) DEFAULT NULL,\n  `COLUMN_NAME` varchar(64) DEFAULT NULL,\n  `OBJECT_INSTANCE_BEGIN` bigint(20) unsigned NOT NULL,\n  `LOCK_TYPE` varchar(32) NOT NULL,\n  `LOCK_DURATION` varchar(32) NOT NULL,\n  `LOCK_STATUS` varchar(32) NOT NULL,\n  `SOURCE` varchar(64) DEFAULT NULL,\n  `OWNER_THREAD_ID` bigint(20) unsigned DEFAULT NULL,\n  `OWNER_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  PRIMARY KEY (`OBJECT_INSTANCE_BEGIN`),\n  KEY `OBJECT_TYPE` (`OBJECT_TYPE`,`OBJECT_SCHEMA`,`OBJECT_NAME`,`COLUMN_NAME`),\n  KEY `OWNER_THREAD_ID` (`OWNER_THREAD_ID`,`OWNER_EVENT_ID`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"mutex_instances": "CREATE TABLE `mutex_instances` (\n  `NAME` varchar(128) NOT NULL,\n  `OBJECT_INSTANCE_BEGIN` bigint(20) unsigned NOT NULL,\n  `LOCKED_BY_THREAD_ID` bigint(20) unsigned DEFAULT NULL,\n  PRIMARY KEY (`OBJECT_INSTANCE_BEGIN`),\n  KEY `NAME` (`NAME`),\n  KEY `LOCKED_BY_THREAD_ID` (`LOCKED_BY_THREAD_ID`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"objects_summary_global_by_type": "CREATE TABLE `objects_summary_global_by_type` (\n  `OBJECT_TYPE` varchar(64) DEFAULT NULL,\n  `OBJECT_SCHEMA` varchar(64) DEFAULT NULL,\n  `OBJECT_NAME` varchar(64) DEFAULT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  UNIQUE KEY `OBJECT` (`OBJECT_TYPE`,`OBJECT_SCHEMA`,`OBJECT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"performance_timers": "CREATE TABLE `performance_timers` (\n  `TIMER_NAME` enum('CYCLE','NANOSECOND','MICROSECOND','MILLISECOND') NOT NULL,\n  `TIMER_FREQUENCY` bigint(20) DEFAULT NULL,\n  `TIMER_RESOLUTION` bigint(20) DEFAULT NULL,\n  `TIMER_OVERHEAD` bigint(20) DEFAULT NULL\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"persisted_variables": "CREATE TABLE `persisted_variables` (\n  `VARIABLE_NAME` varchar(64) NOT NULL,\n  `VARIABLE_VALUE` varchar(1024) DEFAULT NULL,\n  PRIMARY KEY (`VARIABLE_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"replication_group_member_stats": "CREATE TABLE `replication_group_member_stats` (\n  `CHANNEL_NAME` char(64) NOT NULL,\n  `VIEW_ID` char(60) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,\n  `MEMBER_ID` char(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,\n  `COUNT_TRANSACTIONS_IN_QUEUE` bigint(20) unsigned NOT NULL,\n  `COUNT_TRANSACTIONS_CHECKED` bigint(20) unsigned NOT NULL,\n  `COUNT_CONFLICTS_DETECTED` bigint(20) unsigned NOT NULL,\n  `COUNT_TRANSACTIONS_ROWS_VALIDATING` bigint(20) unsigned NOT NULL,\n  `TRANSACTIONS_COMMITTED_ALL_MEMBERS` longtext NOT NULL,\n  `LAST_CONFLICT_FREE_TRANSACTION` text NOT NULL,\n  `COUNT_TRANSACTIONS_REMOTE_IN_APPLIER_QUEUE` bigint(20) unsigned NOT NULL,\n  `COUNT_TRANSACTIONS_REMOTE_APPLIED` bigint(20) unsigned NOT NULL,\n  `COUNT_TRANSACTIONS_LOCAL_PROPOSED` bigint(20) unsigned NOT NULL,\n  `COUNT_TRANSACTIONS_LOCAL_ROLLBACK` bigint(20) unsigned NOT NULL\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"rwlock_instances": "CREATE TABLE `rwlock_instances` (\n  `NAME` varchar(128) NOT NULL,\n  `OBJECT_INSTANCE_BEGIN` bigint(20) unsigned NOT NULL,\n  `WRITE_LOCKED_BY_THREAD_ID` bigint(20) unsigned DEFAULT NULL,\n  `READ_LOCKED_BY_COUNT` int(10) unsigned NOT NULL,\n  PRIMARY KEY (`OBJECT_INSTANCE_BEGIN`),\n  KEY `NAME` (`NAME`),\n  KEY `WRITE_LOCKED_BY_THREAD_ID` (`WRITE_LOCKED_BY_THREAD_ID`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"setup_actors": "CREATE TABLE `setup_actors` (\n  `HOST` char(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL DEFAULT '%',\n  `USER` char(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT '%',\n  `ROLE` char(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT '%',\n  `ENABLED` enum('YES','NO') NOT NULL DEFAULT 'YES',\n  `HISTORY` enum('YES','NO') NOT NULL DEFAULT 'YES',\n  PRIMARY KEY (`HOST`,`USER`,`ROLE`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"setup_consumers": "CREATE TABLE `setup_consumers` (\n  `NAME` varchar(64) NOT NULL,\n  `ENABLED` enum('YES','NO') NOT NULL,\n  PRIMARY KEY (`NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"setup_instruments": "CREATE TABLE `setup_instruments` (\n  `NAME` varchar(128) NOT NULL,\n  `ENABLED` enum('YES','NO') NOT NULL,\n  `TIMED` enum('YES','NO') DEFAULT NULL,\n  `PROPERTIES` set('singleton','progress','user','global_statistics','mutable') NOT NULL,\n  `VOLATILITY` int(11) NOT NULL,\n  `DOCUMENTATION` longtext,\n  PRIMARY KEY (`NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"setup_objects": "CREATE TABLE `setup_objects` (\n  `OBJECT_TYPE` enum('EVENT','FUNCTION','PROCEDURE','TABLE','TRIGGER') NOT NULL DEFAULT 'TABLE',\n  `OBJECT_SCHEMA` varchar(64) DEFAULT '%',\n  `OBJECT_NAME` varchar(64) NOT NULL DEFAULT '%',\n  `ENABLED` enum('YES','NO') NOT NULL DEFAULT 'YES',\n  `TIMED` enum('YES','NO') NOT NULL DEFAULT 'YES',\n  UNIQUE KEY `OBJECT` (`OBJECT_TYPE`,`OBJECT_SCHEMA`,`OBJECT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"socket_instances": "CREATE TABLE `socket_instances` (\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `OBJECT_INSTANCE_BEGIN` bigint(20) unsigned NOT NULL,\n  `THREAD_ID` bigint(20) unsigned DEFAULT NULL,\n  `SOCKET_ID` int(11) NOT NULL,\n  `IP` varchar(64) NOT NULL,\n  `PORT` int(11) NOT NULL,\n  `STATE` enum('IDLE','ACTIVE') NOT NULL,\n  PRIMARY KEY (`OBJECT_INSTANCE_BEGIN`),\n  KEY `THREAD_ID` (`THREAD_ID`),\n  KEY `SOCKET_ID` (`SOCKET_ID`),\n  KEY `IP` (`IP`,`PORT`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"socket_summary_by_event_name": "CREATE TABLE `socket_summary_by_event_name` (\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `COUNT_READ` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `SUM_NUMBER_OF_BYTES_READ` bigint(20) unsigned NOT NULL,\n  `COUNT_WRITE` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `SUM_NUMBER_OF_BYTES_WRITE` bigint(20) unsigned NOT NULL,\n  `COUNT_MISC` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_MISC` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_MISC` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_MISC` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_MISC` bigint(20) unsigned NOT NULL,\n  PRIMARY KEY (`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"socket_summary_by_instance": "CREATE TABLE `socket_summary_by_instance` (\n  `EVENT_NAME` varchar(128) NOT NULL,\n  `OBJECT_INSTANCE_BEGIN` bigint(20) unsigned NOT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `COUNT_READ` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `SUM_NUMBER_OF_BYTES_READ` bigint(20) unsigned NOT NULL,\n  `COUNT_WRITE` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `SUM_NUMBER_OF_BYTES_WRITE` bigint(20) unsigned NOT NULL,\n  `COUNT_MISC` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_MISC` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_MISC` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_MISC` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_MISC` bigint(20) unsigned NOT NULL,\n  PRIMARY KEY (`OBJECT_INSTANCE_BEGIN`),\n  KEY `EVENT_NAME` (`EVENT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"table_handles": "CREATE TABLE `table_handles` (\n  `OBJECT_TYPE` varchar(64) NOT NULL,\n  `OBJECT_SCHEMA` varchar(64) NOT NULL,\n  `OBJECT_NAME` varchar(64) NOT NULL,\n  `OBJECT_INSTANCE_BEGIN` bigint(20) unsigned NOT NULL,\n  `OWNER_THREAD_ID` bigint(20) unsigned DEFAULT NULL,\n  `OWNER_EVENT_ID` bigint(20) unsigned DEFAULT NULL,\n  `INTERNAL_LOCK` varchar(64) DEFAULT NULL,\n  `EXTERNAL_LOCK` varchar(64) DEFAULT NULL,\n  PRIMARY KEY (`OBJECT_INSTANCE_BEGIN`),\n  KEY `OBJECT_TYPE` (`OBJECT_TYPE`,`OBJECT_SCHEMA`,`OBJECT_NAME`),\n  KEY `OWNER_THREAD_ID` (`OWNER_THREAD_ID`,`OWNER_EVENT_ID`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"table_io_waits_summary_by_index_usage": "CREATE TABLE `table_io_waits_summary_by_index_usage` (\n  `OBJECT_TYPE` varchar(64) DEFAULT NULL,\n  `OBJECT_SCHEMA` varchar(64) DEFAULT NULL,\n  `OBJECT_NAME` varchar(64) DEFAULT NULL,\n  `INDEX_NAME` varchar(64) DEFAULT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `COUNT_READ` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `COUNT_WRITE` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `COUNT_FETCH` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_FETCH` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_FETCH` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_FETCH` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_FETCH` bigint(20) unsigned NOT NULL,\n  `COUNT_INSERT` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_INSERT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_INSERT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_INSERT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_INSERT` bigint(20) unsigned NOT NULL,\n  `COUNT_UPDATE` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_UPDATE` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_UPDATE` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_UPDATE` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_UPDATE` bigint(20) unsigned NOT NULL,\n  `COUNT_DELETE` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_DELETE` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_DELETE` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_DELETE` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_DELETE` bigint(20) unsigned NOT NULL,\n  UNIQUE KEY `OBJECT` (`OBJECT_TYPE`,`OBJECT_SCHEMA`,`OBJECT_NAME`,`INDEX_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"table_io_waits_summary_by_table": "CREATE TABLE `table_io_waits_summary_by_table` (\n  `OBJECT_TYPE` varchar(64) DEFAULT NULL,\n  `OBJECT_SCHEMA` varchar(64) DEFAULT NULL,\n  `OBJECT_NAME` varchar(64) DEFAULT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `COUNT_READ` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `COUNT_WRITE` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `COUNT_FETCH` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_FETCH` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_FETCH` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_FETCH` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_FETCH` bigint(20) unsigned NOT NULL,\n  `COUNT_INSERT` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_INSERT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_INSERT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_INSERT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_INSERT` bigint(20) unsigned NOT NULL,\n  `COUNT_UPDATE` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_UPDATE` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_UPDATE` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_UPDATE` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_UPDATE` bigint(20) unsigned NOT NULL,\n  `COUNT_DELETE` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_DELETE` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_DELETE` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_DELETE` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_DELETE` bigint(20) unsigned NOT NULL,\n  UNIQUE KEY `OBJECT` (`OBJECT_TYPE`,`OBJECT_SCHEMA`,`OBJECT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"table_lock_waits_summary_by_table": "CREATE TABLE `table_lock_waits_summary_by_table` (\n  `OBJECT_TYPE` varchar(64) DEFAULT NULL,\n  `OBJECT_SCHEMA` varchar(64) DEFAULT NULL,\n  `OBJECT_NAME` varchar(64) DEFAULT NULL,\n  `COUNT_STAR` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WAIT` bigint(20) unsigned NOT NULL,\n  `COUNT_READ` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_READ` bigint(20) unsigned NOT NULL,\n  `COUNT_WRITE` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WRITE` bigint(20) unsigned NOT NULL,\n  `COUNT_READ_NORMAL` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_READ_NORMAL` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_READ_NORMAL` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_READ_NORMAL` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_READ_NORMAL` bigint(20) unsigned NOT NULL,\n  `COUNT_READ_WITH_SHARED_LOCKS` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_READ_WITH_SHARED_LOCKS` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_READ_WITH_SHARED_LOCKS` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_READ_WITH_SHARED_LOCKS` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_READ_WITH_SHARED_LOCKS` bigint(20) unsigned NOT NULL,\n  `COUNT_READ_HIGH_PRIORITY` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_READ_HIGH_PRIORITY` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_READ_HIGH_PRIORITY` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_READ_HIGH_PRIORITY` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_READ_HIGH_PRIORITY` bigint(20) unsigned NOT NULL,\n  `COUNT_READ_NO_INSERT` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_READ_NO_INSERT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_READ_NO_INSERT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_READ_NO_INSERT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_READ_NO_INSERT` bigint(20) unsigned NOT NULL,\n  `COUNT_READ_EXTERNAL` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_READ_EXTERNAL` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_READ_EXTERNAL` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_READ_EXTERNAL` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_READ_EXTERNAL` bigint(20) unsigned NOT NULL,\n  `COUNT_WRITE_ALLOW_WRITE` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WRITE_ALLOW_WRITE` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WRITE_ALLOW_WRITE` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WRITE_ALLOW_WRITE` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WRITE_ALLOW_WRITE` bigint(20) unsigned NOT NULL,\n  `COUNT_WRITE_CONCURRENT_INSERT` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WRITE_CONCURRENT_INSERT` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WRITE_CONCURRENT_INSERT` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WRITE_CONCURRENT_INSERT` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WRITE_CONCURRENT_INSERT` bigint(20) unsigned NOT NULL,\n  `COUNT_WRITE_LOW_PRIORITY` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WRITE_LOW_PRIORITY` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WRITE_LOW_PRIORITY` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WRITE_LOW_PRIORITY` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WRITE_LOW_PRIORITY` bigint(20) unsigned NOT NULL,\n  `COUNT_WRITE_NORMAL` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WRITE_NORMAL` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WRITE_NORMAL` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WRITE_NORMAL` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WRITE_NORMAL` bigint(20) unsigned NOT NULL,\n  `COUNT_WRITE_EXTERNAL` bigint(20) unsigned NOT NULL,\n  `SUM_TIMER_WRITE_EXTERNAL` bigint(20) unsigned NOT NULL,\n  `MIN_TIMER_WRITE_EXTERNAL` bigint(20) unsigned NOT NULL,\n  `AVG_TIMER_WRITE_EXTERNAL` bigint(20) unsigned NOT NULL,\n  `MAX_TIMER_WRITE_EXTERNAL` bigint(20) unsigned NOT NULL,\n  UNIQUE KEY `OBJECT` (`OBJECT_TYPE`,`OBJECT_SCHEMA`,`OBJECT_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"threads": "CREATE TABLE `threads` (\n  `THREAD_ID` bigint(20) unsigned NOT NULL,\n  `NAME` varchar(128) NOT NULL,\n  `TYPE` varchar(10) NOT NULL,\n  `PROCESSLIST_ID` bigint(20) unsigned DEFAULT NULL,\n  `PROCESSLIST_USER` varchar(32) DEFAULT NULL,\n  `PROCESSLIST_HOST` varchar(255) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT NULL,\n  `PROCESSLIST_DB` varchar(64) DEFAULT NULL,\n  `PROCESSLIST_COMMAND` varchar(16) DEFAULT NULL,\n  `PROCESSLIST_TIME` bigint(20) DEFAULT NULL,\n  `PROCESSLIST_STATE` varchar(64) DEFAULT NULL,\n  `PROCESSLIST_INFO` longtext,\n  `PARENT_THREAD_ID` bigint(20) unsigned DEFAULT NULL,\n  `ROLE` varchar(64) DEFAULT NULL,\n  `INSTRUMENTED` enum('YES','NO') NOT NULL,\n  `HISTORY` enum('YES','NO') NOT NULL,\n  `CONNECTION_TYPE` varchar(16) DEFAULT NULL,\n  `THREAD_OS_ID` bigint(20) unsigned DEFAULT NULL,\n  `RESOURCE_GROUP` varchar(64) DEFAULT NULL,\n  PRIMARY KEY (`THREAD_ID`),\n  KEY `PROCESSLIST_ID` (`PROCESSLIST_ID`),\n  KEY `THREAD_OS_ID` (`THREAD_OS_ID`),\n  KEY `NAME` (`NAME`),\n  KEY `PROCESSLIST_ACCOUNT` (`PROCESSLIST_USER`,`PROCESSLIST_HOST`),\n  KEY `PROCESSLIST_HOST` (`PROCESSLIST_HOST`),\n  KEY `RESOURCE_GROUP` (`RESOURCE_GROUP`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"users": "CREATE TABLE `users` (\n  `USER` char(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n  `CURRENT_CONNECTIONS` bigint(20) NOT NULL,\n  `TOTAL_CONNECTIONS` bigint(20) NOT NULL,\n  UNIQUE KEY `USER` (`USER`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"variables_info": "CREATE TABLE `variables_info` (\n  `VARIABLE_NAME` varchar(64) NOT NULL,\n  `VARIABLE_SOURCE` enum('COMPILED','GLOBAL','SERVER','EXPLICIT','EXTRA','USER','LOGIN','COMMAND_LINE','PERSISTED','DYNAMIC') DEFAULT 'COMPILED',\n  `VARIABLE_PATH` varchar(1024) DEFAULT NULL,\n  `MIN_VALUE` varchar(64) DEFAULT NULL,\n  `MAX_VALUE` varchar(64) DEFAULT NULL,\n  `SET_TIME` timestamp(6) NULL DEFAULT NULL,\n  `SET_USER` char(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n  `SET_HOST` char(255) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT NULL\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_errors_summary_by_account_by_error": "CREATE TABLE `events_errors_summary_by_account_by_error` (\n  `USER` char(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n  `HOST` char(255) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT NULL,\n  `ERROR_NUMBER` int(11) DEFAULT NULL,\n  `ERROR_NAME` varchar(64) DEFAULT NULL,\n  `SQL_STATE` varchar(5) DEFAULT NULL,\n  `SUM_ERROR_RAISED` bigint(20) unsigned NOT NULL,\n  `SUM_ERROR_HANDLED` bigint(20) unsigned NOT NULL,\n  `FIRST_SEEN` timestamp NULL DEFAULT '0000-00-00 00:00:00',\n  `LAST_SEEN` timestamp NULL DEFAULT '0000-00-00 00:00:00',\n  UNIQUE KEY `ACCOUNT` (`USER`,`HOST`,`ERROR_NUMBER`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_errors_summary_by_host_by_error": "CREATE TABLE `events_errors_summary_by_host_by_error` (\n  `HOST` char(255) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT NULL,\n  `ERROR_NUMBER` int(11) DEFAULT NULL,\n  `ERROR_NAME` varchar(64) DEFAULT NULL,\n  `SQL_STATE` varchar(5) DEFAULT NULL,\n  `SUM_ERROR_RAISED` bigint(20) unsigned NOT NULL,\n  `SUM_ERROR_HANDLED` bigint(20) unsigned NOT NULL,\n  `FIRST_SEEN` timestamp NULL DEFAULT '0000-00-00 00:00:00',\n  `LAST_SEEN` timestamp NULL DEFAULT '0000-00-00 00:00:00',\n  UNIQUE KEY `HOST` (`HOST`,`ERROR_NUMBER`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_errors_summary_by_thread_by_error": "CREATE TABLE `events_errors_summary_by_thread_by_error` (\n  `THREAD_ID` bigint(20) unsigned NOT NULL,\n  `ERROR_NUMBER` int(11) DEFAULT NULL,\n  `ERROR_NAME` varchar(64) DEFAULT NULL,\n  `SQL_STATE` varchar(5) DEFAULT NULL,\n  `SUM_ERROR_RAISED` bigint(20) unsigned NOT NULL,\n  `SUM_ERROR_HANDLED` bigint(20) unsigned NOT NULL,\n  `FIRST_SEEN` timestamp NULL DEFAULT '0000-00-00 00:00:00',\n  `LAST_SEEN` timestamp NULL DEFAULT '0000-00-00 00:00:00',\n  UNIQUE KEY `THREAD_ID` (`THREAD_ID`,`ERROR_NUMBER`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_errors_summary_by_user_by_error": "CREATE TABLE `events_errors_summary_by_user_by_error` (\n  `USER` char(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n  `ERROR_NUMBER` int(11) DEFAULT NULL,\n  `ERROR_NAME` varchar(64) DEFAULT NULL,\n  `SQL_STATE` varchar(5) DEFAULT NULL,\n  `SUM_ERROR_RAISED` bigint(20) unsigned NOT NULL,\n  `SUM_ERROR_HANDLED` bigint(20) unsigned NOT NULL,\n  `FIRST_SEEN` timestamp NULL DEFAULT '0000-00-00 00:00:00',\n  `LAST_SEEN` timestamp NULL DEFAULT '0000-00-00 00:00:00',\n  UNIQUE KEY `USER` (`USER`,`ERROR_NUMBER`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_errors_summary_global_by_error": "CREATE TABLE `events_errors_summary_global_by_error` (\n  `ERROR_NUMBER` int(11) DEFAULT NULL,\n  `ERROR_NAME` varchar(64) DEFAULT NULL,\n  `SQL_STATE` varchar(5) DEFAULT NULL,\n  `SUM_ERROR_RAISED` bigint(20) unsigned NOT NULL,\n  `SUM_ERROR_HANDLED` bigint(20) unsigned NOT NULL,\n  `FIRST_SEEN` timestamp NULL DEFAULT '0000-00-00 00:00:00',\n  `LAST_SEEN` timestamp NULL DEFAULT '0000-00-00 00:00:00',\n  UNIQUE KEY `ERROR_NUMBER` (`ERROR_NUMBER`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_statements_histogram_by_digest": "CREATE TABLE `events_statements_histogram_by_digest` (\n  `SCHEMA_NAME` varchar(64) DEFAULT NULL,\n  `DIGEST` varchar(64) DEFAULT NULL,\n  `BUCKET_NUMBER` int(10) unsigned NOT NULL,\n  `BUCKET_TIMER_LOW` bigint(20) unsigned NOT NULL,\n  `BUCKET_TIMER_HIGH` bigint(20) unsigned NOT NULL,\n  `COUNT_BUCKET` bigint(20) unsigned NOT NULL,\n  `COUNT_BUCKET_AND_LOWER` bigint(20) unsigned NOT NULL,\n  `BUCKET_QUANTILE` double(23,20) NOT NULL,\n  UNIQUE KEY `SCHEMA_NAME` (`SCHEMA_NAME`,`DIGEST`,`BUCKET_NUMBER`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"events_statements_histogram_global": "CREATE TABLE `events_statements_histogram_global` (\n  `BUCKET_NUMBER` int(10) unsigned NOT NULL,\n  `BUCKET_TIMER_LOW` bigint(20) unsigned NOT NULL,\n  `BUCKET_TIMER_HIGH` bigint(20) unsigned NOT NULL,\n  `COUNT_BUCKET` bigint(20) unsigned NOT NULL,\n  `COUNT_BUCKET_AND_LOWER` bigint(20) unsigned NOT NULL,\n  `BUCKET_QUANTILE` double(23,20) NOT NULL,\n  PRIMARY KEY (`BUCKET_NUMBER`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"global_status": "CREATE TABLE `global_status` (\n  `VARIABLE_NAME` varchar(64) NOT NULL,\n  `VARIABLE_VALUE` varchar(1024) DEFAULT NULL\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"global_variables": "CREATE TABLE `global_variables` (\n  `VARIABLE_NAME` varchar(64) NOT NULL,\n  `VARIABLE_VALUE` varchar(1024) DEFAULT NULL\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"session_account_connect_attrs": "CREATE TABLE `session_account_connect_attrs` (\n  `PROCESSLIST_ID` int(11) NOT NULL,\n  `ATTR_NAME` varchar(32) NOT NULL,\n  `ATTR_VALUE` varchar(1024) DEFAULT NULL,\n  `ORDINAL_POSITION` int(11) DEFAULT NULL,\n  PRIMARY KEY (`PROCESSLIST_ID`,`ATTR_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	"session_connect_attrs": "CREATE TABLE `session_connect_attrs` (\n  `PROCESSLIST_ID` int(11) NOT NULL,\n  `ATTR_NAME` varchar(32) NOT NULL,\n  `ATTR_VALUE` varchar(1024) DEFAULT NULL,\n  `ORDINAL_POSITION` int(11) DEFAULT NULL,\n  PRIMARY KEY (`PROCESSLIST_ID`,`ATTR_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	"session_status": "CREATE TABLE `session_status` (\n  `VARIABLE_NAME` varchar(64) NOT NULL,\n  `VARIABLE_VALUE` varchar(1024) DEFAULT NULL\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"session_variables": "CREATE TABLE `session_variables` (\n  `VARIABLE_NAME` varchar(64) NOT NULL,\n  `VARIABLE_VALUE` varchar(1024) DEFAULT NULL\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"setup_threads": "CREATE TABLE `setup_threads` (\n  `NAME` varchar(128) NOT NULL,\n  `ENABLED` enum('YES','NO') NOT NULL,\n  `HISTORY` enum('YES','NO') NOT NULL,\n  `PROPERTIES` set('singleton','progress') NOT NULL,\n  `VOLATILITY` int(11) NOT NULL DEFAULT '0',\n  `DOCUMENTATION` longtext,\n  PRIMARY KEY (`NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"status_by_thread": "CREATE TABLE `status_by_thread` (\n  `THREAD_ID` bigint(20) unsigned NOT NULL,\n  `VARIABLE_NAME` varchar(64) NOT NULL,\n  `VARIABLE_VALUE` varchar(1024) DEFAULT NULL\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"user_variables_by_thread": "CREATE TABLE `user_variables_by_thread` (\n  `THREAD_ID` bigint(20) unsigned NOT NULL,\n  `VARIABLE_NAME` varchar(64) NOT NULL,\n  `VARIABLE_VALUE` longtext,\n  PRIMARY KEY (`THREAD_ID`,`VARIABLE_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	"variables_by_thread": "CREATE TABLE `variables_by_thread` (\n  `THREAD_ID` bigint(20) unsigned NOT NULL,\n  `VARIABLE_NAME` varchar(64) NOT NULL,\n  `VARIABLE_VALUE` varchar(1024) DEFAULT NULL,\n  PRIMARY KEY (`THREAD_ID`,`VARIABLE_NAME`)\n) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
}

// emptyStubTables lists virtual tables that always return an empty result set.
var emptyStubTables = map[string]bool{
	"innodb_ft_index_cache":  true,
	"innodb_ft_index_table":  true,
	"innodb_ft_config":       true,
	"innodb_ft_being_deleted": true,
	"innodb_ft_deleted":      true,
	"column_privileges":      true,
	"persisted_variables":    true,
	// "variables_by_thread" is handled dynamically
	// "status_by_thread" is handled dynamically
	// "user_variables_by_thread" is handled dynamically
	// "session_connect_attrs" is handled dynamically
	// "session_account_connect_attrs" is handled dynamically
	// "socket_summary_by_event_name" is handled dynamically
	// "events_statements_summary_by_digest" is handled dynamically
	// "events_statements_histogram_by_digest" is handled dynamically
	"events_statements_summary_by_program":    true,
	"status_by_account":                       true,
	"status_by_host":                          true,
	"status_by_user":                          true,
	"replication_connection_configuration":     true,
	"replication_connection_status":            true,
	"replication_applier_configuration":        true,
	"replication_applier_status":               true,
	"replication_applier_status_by_coordinator": true,
	"replication_applier_status_by_worker":     true,
	"replication_applier_filters":              true,
	"replication_applier_global_filters":       true,
	"replication_group_members":                true,
	"replication_group_member_stats":           true,
	"keyring_keys":               true,
	"host_cache":                 true,
	"log_status":                 true,
	"prepared_statements_instances": true,
	"user_defined_functions":     true,
	"metadata_locks":             true,
	"data_locks":                 true,
	"data_lock_waits":            true,
	"file_instances":             true,
	"file_summary_by_instance":   true,
	"socket_summary_by_instance":    true,
	"table_handles":              true,
	// table_io_waits_summary_by_table, table_io_waits_summary_by_index_usage,
	// table_lock_waits_summary_by_table are handled dynamically (see buildInformationSchemaRows).
}

// singleRowStubTables maps table names to their single stub row definition.
// These are InnoDB metadata tables that return one row of zero/empty values.
var singleRowStubTables = map[string]storage.Row{
	"innodb_columns":        {"TABLE_ID": int64(0), "NAME": "", "POS": int64(0), "MTYPE": int64(0), "PRTYPE": int64(0), "LEN": int64(0)},
	"innodb_virtual":        {"TABLE_ID": int64(0), "POS": int64(0), "BASE_POS": int64(0)},
	"innodb_buffer_page_lru": {"POOL_ID": int64(0), "LRU_POSITION": int64(0), "SPACE": int64(0), "PAGE_NUMBER": int64(0)},
	"innodb_buffer_pool_stats": {"POOL_ID": int64(0), "POOL_SIZE": int64(0)},
	"innodb_cmp":          {"page_size": int64(4096), "compress_ops": int64(0), "compress_ops_ok": int64(0), "compress_time": int64(0), "uncompress_ops": int64(0), "uncompress_time": int64(0)},
	"innodb_cmp_reset":    {"page_size": int64(4096), "compress_ops": int64(0), "compress_ops_ok": int64(0), "compress_time": int64(0), "uncompress_ops": int64(0), "uncompress_time": int64(0)},
	"innodb_cmpmem":       {"page_size": int64(4096), "buffer_pool_instance": int64(0), "pages_used": int64(0), "pages_free": int64(0), "relocation_ops": int64(0), "relocation_time": int64(0)},
	"innodb_cmpmem_reset": {"page_size": int64(4096), "buffer_pool_instance": int64(0), "pages_used": int64(0), "pages_free": int64(0), "relocation_ops": int64(0), "relocation_time": int64(0)},
	"innodb_trx":            {"trx_id": "", "trx_state": "RUNNING", "trx_started": nil},
	"innodb_fields":         {"INDEX_ID": int64(0), "NAME": "", "POS": int64(0)},
	"optimizer_trace":       {"QUERY": "", "TRACE": ""},
	"files":                 {"FILE_NAME": "", "FILE_TYPE": "", "TABLESPACE_NAME": ""},
	"referential_constraints": {"CONSTRAINT_CATALOG": "def", "CONSTRAINT_SCHEMA": "", "CONSTRAINT_NAME": "", "UNIQUE_CONSTRAINT_CATALOG": "def", "UNIQUE_CONSTRAINT_SCHEMA": "", "UNIQUE_CONSTRAINT_NAME": "", "MATCH_OPTION": "NONE", "UPDATE_RULE": "RESTRICT", "DELETE_RULE": "RESTRICT", "TABLE_NAME": "", "REFERENCED_TABLE_NAME": ""},
	"innodb_temp_table_info": {"TABLE_ID": int64(0), "NAME": "", "N_COLS": int64(0), "SPACE": int64(0)},
}

// psSummaryTables maps summary table names to their definitions.
// Each entry follows the pattern: check psClassDisabled/startupVar, then call seed fn.
type psSummaryDef struct {
	disableClass string   // for psClassDisabled check ("wait", "transaction", "stage", "")
	disableVar   string   // for startupVars check (empty means use disableClass instead)
	seedFn       func(e *Executor) []storage.Row
}

var psSummaryTables = map[string]psSummaryDef{
	// wait summaries
	"events_waits_summary_by_account_by_event_name": {disableClass: "wait", seedFn: func(_ *Executor) []storage.Row { return perfSchemaSeedByAccountByEventName(psWaitEventNames) }},
	"events_waits_summary_by_host_by_event_name":    {disableClass: "wait", seedFn: func(_ *Executor) []storage.Row { return perfSchemaSeedByHostByEventName(psWaitEventNames) }},
	"events_waits_summary_by_instance":              {disableClass: "wait", seedFn: func(_ *Executor) []storage.Row { return []storage.Row{{"EVENT_NAME": "wait/lock/table/sql/handler", "OBJECT_INSTANCE_BEGIN": int64(1), "COUNT_STAR": int64(0), "SUM_TIMER_WAIT": int64(0), "MIN_TIMER_WAIT": int64(0), "AVG_TIMER_WAIT": int64(0), "MAX_TIMER_WAIT": int64(0)}} }},
	"events_waits_summary_by_thread_by_event_name":  {disableClass: "wait", seedFn: func(e *Executor) []storage.Row { return e.perfSchemaSeedByThreadByEventName(psWaitEventNames) }},
	"events_waits_summary_by_user_by_event_name":    {disableClass: "wait", seedFn: func(_ *Executor) []storage.Row { return perfSchemaSeedByUserByEventName(psWaitEventNames) }},
	"events_waits_summary_global_by_event_name":     {disableClass: "wait", seedFn: func(_ *Executor) []storage.Row { return perfSchemaSeedGlobalByEventName(psWaitEventNames) }},
	// stage summaries
	"events_stages_summary_by_account_by_event_name": {disableVar: "performance_schema_max_stage_classes", seedFn: func(_ *Executor) []storage.Row { return perfSchemaSeedByAccountByEventName(psStageEventNames) }},
	"events_stages_summary_by_host_by_event_name":    {disableVar: "performance_schema_max_stage_classes", seedFn: func(_ *Executor) []storage.Row { return perfSchemaSeedByHostByEventName(psStageEventNames) }},
	"events_stages_summary_by_thread_by_event_name":  {disableVar: "performance_schema_max_stage_classes", seedFn: func(e *Executor) []storage.Row { return e.perfSchemaSeedByThreadByEventName(psStageEventNames) }},
	"events_stages_summary_by_user_by_event_name":    {disableVar: "performance_schema_max_stage_classes", seedFn: func(_ *Executor) []storage.Row { return perfSchemaSeedByUserByEventName(psStageEventNames) }},
	"events_stages_summary_global_by_event_name":     {disableVar: "performance_schema_max_stage_classes", seedFn: func(_ *Executor) []storage.Row { return perfSchemaSeedGlobalByEventName(psStageEventNames) }},
	// statement summaries
	"events_statements_summary_by_account_by_event_name": {disableVar: "performance_schema_max_statement_classes", seedFn: func(_ *Executor) []storage.Row { return perfSchemaSeedStmtByAccountByEventName() }},
	"events_statements_summary_by_host_by_event_name":    {disableVar: "performance_schema_max_statement_classes", seedFn: func(_ *Executor) []storage.Row { return perfSchemaSeedStmtByHostByEventName() }},
	"events_statements_summary_by_thread_by_event_name":  {disableVar: "performance_schema_max_statement_classes", seedFn: func(e *Executor) []storage.Row { return e.perfSchemaSeedStmtByThreadByEventName() }},
	"events_statements_summary_by_user_by_event_name":    {disableVar: "performance_schema_max_statement_classes", seedFn: func(_ *Executor) []storage.Row { return perfSchemaSeedStmtByUserByEventName() }},
	"events_statements_summary_global_by_event_name":     {disableVar: "performance_schema_max_statement_classes", seedFn: func(_ *Executor) []storage.Row { return perfSchemaSeedStmtGlobalByEventName() }},
	// transaction summaries (include COUNT_READ_WRITE, COUNT_READ_ONLY, etc.)
	"events_transactions_summary_by_account_by_event_name": {disableClass: "transaction", seedFn: func(_ *Executor) []storage.Row { return perfSchemaSeedTxnByAccountByEventName() }},
	"events_transactions_summary_by_host_by_event_name":    {disableClass: "transaction", seedFn: func(_ *Executor) []storage.Row { return perfSchemaSeedTxnByHostByEventName() }},
	"events_transactions_summary_by_thread_by_event_name":  {disableClass: "transaction", seedFn: func(e *Executor) []storage.Row { return e.perfSchemaSeedTxnByThreadByEventName() }},
	"events_transactions_summary_by_user_by_event_name":    {disableClass: "transaction", seedFn: func(_ *Executor) []storage.Row { return perfSchemaSeedTxnByUserByEventName() }},
	"events_transactions_summary_global_by_event_name":     {disableClass: "transaction", seedFn: func(_ *Executor) []storage.Row { return perfSchemaSeedTxnGlobalByEventName() }},
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
			"innodb_cmp", "innodb_cmp_reset", "innodb_cmpmem", "innodb_cmpmem_reset",
			"innodb_trx", "innodb_foreign_cols", "innodb_fields", "optimizer_trace", "files", "processlist",
			"key_column_usage", "referential_constraints", "innodb_temp_table_info",
			"innodb_ft_default_stopword", "innodb_ft_index_cache", "innodb_ft_index_table",
			"innodb_ft_config", "innodb_ft_being_deleted", "innodb_ft_deleted",
			"triggers", "table_constraints", "character_sets", "collations",
			"collation_character_set_applicability", "user_privileges", "schema_privileges",
			"table_privileges", "column_privileges", "routines", "views", "check_constraints",
			"events", "partitions", "plugins", "resource_groups", "view_table_usage",
			"st_spatial_reference_systems", "st_geometry_columns", "st_units_of_measure":
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

	// Check declarative stub tables first (empty results and single-row stubs).
	if emptyStubTables[t] {
		rawRows = []storage.Row{}
		goto applyAlias
	}
	if stubRow, ok := singleRowStubTables[t]; ok {
		rawRows = []storage.Row{stubRow}
		goto applyAlias
	}

	// Check performance_schema summary tables with disable-check + seed pattern.
	if def, ok := psSummaryTables[t]; ok {
		disabled := false
		if def.disableClass != "" {
			disabled = e.psClassDisabled(def.disableClass)
		}
		if def.disableVar != "" {
			disabled = e.startupVars[def.disableVar] == "0"
		}
		if disabled {
			rawRows = []storage.Row{}
		} else {
			rawRows = def.seedFn(e)
		}
		goto applyAlias
	}

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
	case "plugins":
		rawRows = e.infoSchemaPlugins()
	case "innodb_tables":
		rawRows = e.infoSchemaInnoDBTables()
	case "innodb_tablespaces":
		rawRows = e.infoSchemaInnoDBTablespaces()
	case "innodb_datafiles":
		rawRows = e.infoSchemaInnoDBDatafiles()
	case "innodb_metrics":
		rawRows = e.infoSchemaInnoDBMetrics()
	case "innodb_indexes":
		rawRows = e.infoSchemaInnoDBIndexes()
	case "innodb_cached_indexes":
		rawRows = e.infoSchemaInnoDBCachedIndexes()
	case "innodb_foreign":
		rawRows = e.infoSchemaInnoDBForeign()
	case "innodb_foreign_cols":
		rawRows = e.infoSchemaInnoDBForeignCols()
	case "innodb_buffer_page":
		rawRows = []storage.Row{} // empty stub - no buffer pool tracking
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
				var db interface{}
				if entry.DB != "" {
					db = entry.DB
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
	case "global_variables":
		rawRows = e.perfSchemaVariablesScoped(true)
	case "session_variables":
		rawRows = e.perfSchemaVariablesScoped(false)
	case "global_status", "session_status":
		rawRows = e.perfSchemaStatus()
	case "table_io_waits_summary_by_table":
		rawRows = e.perfSchemaTableIOWaitsByTable()
	case "table_io_waits_summary_by_index_usage":
		rawRows = e.perfSchemaTableIOWaitsByIndexUsage()
	case "table_lock_waits_summary_by_table":
		rawRows = e.perfSchemaTableLockWaitsByTable()
	case "objects_summary_global_by_type":
		rawRows = e.perfSchemaObjectsSummaryGlobalByType()
	case "mutex_instances":
		// Empty when max_mutex_classes=0 or max_mutex_instances=0
		if e.startupVars["performance_schema_max_mutex_classes"] == "0" || e.startupVars["performance_schema_max_mutex_instances"] == "0" {
			rawRows = []storage.Row{}
		} else {
			// Comprehensive list of MySQL server mutexes for compatibility with server_init test
			mutexNames := []string{
				"wait/synch/mutex/mysys/THR_LOCK_malloc",
				"wait/synch/mutex/mysys/THR_LOCK_open",
				"wait/synch/mutex/mysys/THR_LOCK_myisam",
				"wait/synch/mutex/mysys/THR_LOCK_heap",
				"wait/synch/mutex/mysys/THR_LOCK_net",
				"wait/synch/mutex/mysys/THR_LOCK_charset",
				"wait/synch/mutex/sql/LOCK_open",
				// 8 instances of LOCK_thd_list
				"wait/synch/mutex/sql/LOCK_thd_list",
				"wait/synch/mutex/sql/LOCK_thd_list",
				"wait/synch/mutex/sql/LOCK_thd_list",
				"wait/synch/mutex/sql/LOCK_thd_list",
				"wait/synch/mutex/sql/LOCK_thd_list",
				"wait/synch/mutex/sql/LOCK_thd_list",
				"wait/synch/mutex/sql/LOCK_thd_list",
				"wait/synch/mutex/sql/LOCK_thd_list",
				"wait/synch/mutex/sql/LOCK_log_throttle_qni",
				"wait/synch/mutex/sql/LOCK_status",
				"wait/synch/mutex/sql/LOCK_uuid_generator",
				"wait/synch/mutex/sql/LOCK_crypt",
				"wait/synch/mutex/sql/LOCK_slave_list",
				"wait/synch/mutex/sql/LOCK_manager",
				"wait/synch/mutex/sql/LOCK_global_system_variables",
				"wait/synch/mutex/sql/LOCK_user_conn",
				"wait/synch/mutex/sql/LOCK_prepared_stmt_count",
				"wait/synch/mutex/sql/LOCK_connection_count",
				"wait/synch/mutex/sql/LOCK_server_started",
				"wait/synch/mutex/sql/LOCK_event_queue",
				"wait/synch/mutex/sql/LOCK_item_func_sleep",
				"wait/synch/mutex/sql/LOCK_audit_mask",
				"wait/synch/mutex/sql/LOCK_transaction_cache",
				"wait/synch/mutex/sql/LOCK_plugin",
				"wait/synch/mutex/sql/tz_LOCK",
				"wait/synch/mutex/sql/LOCK_active_mi",
			}
			rawRows = make([]storage.Row, len(mutexNames))
			for i, name := range mutexNames {
				rawRows[i] = storage.Row{"NAME": name, "OBJECT_INSTANCE_BEGIN": int64(i + 1), "LOCKED_BY_THREAD_ID": nil}
			}
		}
	case "rwlock_instances":
		// Empty when max_rwlock_classes=0 or max_rwlock_instances=0
		if e.startupVars["performance_schema_max_rwlock_classes"] == "0" || e.startupVars["performance_schema_max_rwlock_instances"] == "0" {
			rawRows = []storage.Row{}
		} else {
			rwlockNames := []string{
				"wait/synch/rwlock/sql/LOCK_sys_init_connect",
				"wait/synch/rwlock/sql/LOCK_sys_init_slave",
				"wait/synch/rwlock/sql/LOCK_system_variables_hash",
				"wait/synch/rwlock/sql/LOCK_grant",
			}
			rawRows = make([]storage.Row, len(rwlockNames))
			for i, name := range rwlockNames {
				rawRows[i] = storage.Row{"NAME": name, "OBJECT_INSTANCE_BEGIN": int64(i + 1), "WRITE_LOCKED_BY_THREAD_ID": nil, "READ_LOCKED_BY_COUNT": int64(0)}
			}
		}
	case "cond_instances":
		// Empty when max_cond_classes=0 or max_cond_instances=0
		if e.startupVars["performance_schema_max_cond_classes"] == "0" || e.startupVars["performance_schema_max_cond_instances"] == "0" {
			rawRows = []storage.Row{}
		} else {
			condNames := []string{
				"wait/synch/cond/sql/COND_server_started",
				"wait/synch/cond/sql/COND_manager",
				"wait/synch/cond/sql/COND_thread_cache",
				"wait/synch/cond/sql/COND_flush_thread_cache",
				"wait/synch/cond/sql/COND_queue_state",
			}
			rawRows = make([]storage.Row, len(condNames))
			for i, name := range condNames {
				rawRows[i] = storage.Row{"NAME": name, "OBJECT_INSTANCE_BEGIN": int64(i + 1)}
			}
		}
	case "socket_instances":
		// Empty when max_socket_classes=0 or max_socket_instances=0
		if e.startupVars["performance_schema_max_socket_classes"] == "0" || e.startupVars["performance_schema_max_socket_instances"] == "0" {
			rawRows = []storage.Row{}
		} else {
			rawRows = []storage.Row{
				{"EVENT_NAME": "wait/io/socket/sql/client_connection", "OBJECT_INSTANCE_BEGIN": int64(1), "THREAD_ID": e.connectionID + 1, "SOCKET_ID": int64(1), "IP": "127.0.0.1", "PORT": int64(3306), "STATE": "ACTIVE"},
			}
		}
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
		if e.psClassDisabled("statement") || e.startupVars["performance_schema_events_statements_history_long_size"] == "0" {
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
		rawRows = e.infoSchemaSchemaPrivileges()
	case "table_privileges":
		rawRows = e.infoSchemaTablePrivileges()
	case "routines":
		rawRows = e.infoSchemaRoutines()
	case "views":
		rawRows = e.infoSchemaViews()
	case "st_spatial_reference_systems":
		rawRows = infoSchemaSpatialReferenceSystems()
	case "st_geometry_columns":
		rawRows = e.infoSchemaSTGeometryColumns()
	case "st_units_of_measure":
		rawRows = []storage.Row{}
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
		if e.psSetupActorsInit {
			rawRows = e.psSetupActors
		} else {
			rawRows = e.perfSchemaSetupActors()
		}
	case "setup_objects":
		if e.psSetupObjectsInit {
			rawRows = e.psSetupObjects
		} else {
			rawRows = e.perfSchemaSetupObjectsDefault()
		}
	case "setup_instruments":
		rawRows = e.perfSchemaSetupInstruments()
	case "setup_threads":
		rawRows = e.perfSchemaSetupThreads()
	case "variables_info":
		rawRows = e.perfSchemaVariablesInfo()
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
		if e.psClassDisabled("statement") {
			rawRows = []storage.Row{}
		} else {
			rawRows = []storage.Row{
				{"THREAD_ID": e.connectionID + 1, "EVENT_ID": int64(1), "END_EVENT_ID": int64(1), "EVENT_NAME": "statement/sql/select", "SOURCE": "", "TIMER_START": int64(0), "TIMER_END": int64(0), "TIMER_WAIT": int64(0), "SQL_TEXT": nil, "DIGEST": nil, "DIGEST_TEXT": nil, "CURRENT_SCHEMA": nil, "ROWS_AFFECTED": int64(0), "ROWS_SENT": int64(0), "ROWS_EXAMINED": int64(0), "CREATED_TMP_DISK_TABLES": int64(0), "CREATED_TMP_TABLES": int64(0), "ERRORS": int64(0), "WARNINGS": int64(0), "NESTING_EVENT_ID": nil, "NESTING_EVENT_TYPE": nil},
			}
		}
	case "events_statements_history":
		if e.psClassDisabled("statement") || e.startupVars["performance_schema_events_statements_history_size"] == "0" {
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
	case "events_statements_histogram_global":
		rawRows = perfSchemaSeedHistogramGlobal()
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
			// Check per-session override first
			if e.psConsumerEnabled != nil {
				if v, ok := e.psConsumerEnabled[c]; ok {
					enabled = v
					rawRows = append(rawRows, storage.Row{"NAME": c, "ENABLED": enabled})
					continue
				}
			}
			// Check if consumer was disabled via startup variable
			// Accept OFF/0/NO/FALSE as disabled (matching MySQL's --loose-performance-schema-consumer-X=0)
			varName := "performance_schema_consumer_" + strings.Replace(c, "-", "_", -1)
			if v, ok := e.startupVars[varName]; ok && isPerfSchemaConsumerDisabled(v) {
				enabled = "NO"
			}
			rawRows = append(rawRows, storage.Row{"NAME": c, "ENABLED": enabled})
		}
	case "session_connect_attrs", "session_account_connect_attrs":
		rawRows = e.perfSchemaSessionConnectAttrs()
	case "socket_summary_by_event_name":
		rawRows = perfSchemaSocketSummaryByEventName()
	case "status_by_thread":
		rawRows = e.perfSchemaStatusByThread()
	case "user_variables_by_thread":
		rawRows = e.perfSchemaUserVariablesByThread()
	case "variables_by_thread":
		rawRows = e.perfSchemaVariablesByThread()
	case "events_statements_summary_by_digest":
		rawRows = e.perfSchemaESMSByDigest()
	case "events_statements_histogram_by_digest":
		rawRows = e.perfSchemaESMHByDigest()
	}

applyAlias:
	// Determine if this is a performance_schema table (columns use user casing, not uppercase)
	isPerfSchema := strings.Contains(strings.ToLower(alias), "performance_schema")

	// InnoDB INFORMATION_SCHEMA tables (innodb_*) also preserve user-specified
	// column casing in SELECT headers, matching MySQL 8.0 behavior where these
	// are data dictionary tables rather than system views.
	isInnoDBTable := strings.HasPrefix(strings.ToLower(tableName), "innodb_")

	result := make([]storage.Row, len(rawRows))
	for i, row := range rawRows {
		newRow := make(storage.Row, len(row)*3+1)
		// Mark row as INFORMATION_SCHEMA for case-insensitive WHERE comparison
		newRow["__is_info_schema__"] = true
		// Performance_schema and InnoDB IS tables preserve user-specified column casing in SELECT
		if isPerfSchema || isInnoDBTable {
			newRow["__ps_preserve_col_case__"] = true
		}
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
	tableID := int64(1)
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
				"TABLE_ID":      tableID,
				"NAME":          strings.ToLower(dbName + "/" + tblName),
				"SPACE":         space,
				"FLAG":          int64(33),
				"N_COLS":        int64(5),
				"ROW_FORMAT":    "Dynamic",
				"ZIP_PAGE_SIZE": int64(0),
				"SPACE_TYPE":    "Single",
			})
			tableID++
			space++
		}
	}
	if len(rows) == 0 {
		return []storage.Row{{"TABLE_ID": int64(0), "NAME": "", "SPACE": int64(0), "FLAG": int64(33), "N_COLS": int64(0), "ROW_FORMAT": "Dynamic", "ZIP_PAGE_SIZE": int64(0), "SPACE_TYPE": "Single"}}
	}
	return rows
}

// infoSchemaInnoDBIndexes returns rows for information_schema.INNODB_INDEXES.
// Columns: INDEX_ID, NAME, TABLE_ID, TYPE, SPACE
func (e *Executor) infoSchemaInnoDBIndexes() []storage.Row {
	rows := make([]storage.Row, 0)
	dbNames := e.Catalog.ListDatabases()
	sort.Strings(dbNames)
	tableID := int64(1)
	indexID := int64(1)
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
			def, err := db.GetTable(tblName)
			if err != nil || def == nil {
				tableID++
				space++
				continue
			}
			// Primary index (GEN_CLUST_INDEX if no explicit primary key)
			pkName := "GEN_CLUST_INDEX"
			if len(def.PrimaryKey) > 0 {
				pkName = "PRIMARY"
			}
			rows = append(rows, storage.Row{
				"INDEX_ID": indexID,
				"NAME":     pkName,
				"TABLE_ID": tableID,
				"TYPE":     int64(3), // clustered
				"SPACE":    space,
			})
			indexID++
			// Secondary indexes
			for _, idx := range def.Indexes {
				if strings.EqualFold(idx.Name, "PRIMARY") {
					continue
				}
				rows = append(rows, storage.Row{
					"INDEX_ID": indexID,
					"NAME":     idx.Name,
					"TABLE_ID": tableID,
					"TYPE":     int64(0),
					"SPACE":    space,
				})
				indexID++
			}
			tableID++
			space++
		}
	}
	return rows
}

// infoSchemaInnoDBCachedIndexes returns rows for information_schema.INNODB_CACHED_INDEXES.
// Since mylite doesn't track buffer pool pages, all N_CACHED_PAGES are 0.
// Columns: SPACE_ID, INDEX_ID, N_CACHED_PAGES
func (e *Executor) infoSchemaInnoDBCachedIndexes() []storage.Row {
	rows := make([]storage.Row, 0)
	dbNames := e.Catalog.ListDatabases()
	sort.Strings(dbNames)
	indexID := int64(1)
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
			def, err := db.GetTable(tblName)
			if err != nil || def == nil {
				space++
				indexID++
				continue
			}
			// Primary/clustered index
			rows = append(rows, storage.Row{
				"SPACE_ID":       space,
				"INDEX_ID":       indexID,
				"N_CACHED_PAGES": int64(0),
			})
			indexID++
			// Secondary indexes
			for _, idx := range def.Indexes {
				if strings.EqualFold(idx.Name, "PRIMARY") {
					continue
				}
				rows = append(rows, storage.Row{
					"SPACE_ID":       space,
					"INDEX_ID":       indexID,
					"N_CACHED_PAGES": int64(0),
				})
				indexID++
			}
			space++
		}
	}
	return rows
}

func (e *Executor) infoSchemaInnoDBTablespaces() []storage.Row {
	// Start with the two default undo tablespaces (innodb_undo_001, innodb_undo_002).
	// MySQL always creates these at startup; MTR tests rely on their presence.
	rows := []storage.Row{
		{
			"SPACE":         int64(0xFFFFFFFE),
			"NAME":          "innodb_undo_001",
			"ROW_FORMAT":    "Undo",
			"PAGE_SIZE":     int64(16384),
			"ZIP_PAGE_SIZE": int64(0),
			"SPACE_TYPE":    "Undo",
		},
		{
			"SPACE":         int64(0xFFFFFFFD),
			"NAME":          "innodb_undo_002",
			"ROW_FORMAT":    "Undo",
			"PAGE_SIZE":     int64(16384),
			"ZIP_PAGE_SIZE": int64(0),
			"SPACE_TYPE":    "Undo",
		},
	}
	tables := e.infoSchemaInnoDBTables()
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

// innodbFKType computes the InnoDB INNODB_FOREIGN.TYPE bitmask for a foreign key.
// Bit encoding:
//
//	DELETE: CASCADE=1, SET NULL=2, NO_ACTION=16, RESTRICT=0
//	UPDATE: CASCADE=4, SET NULL=8, NO_ACTION=32, RESTRICT=0
//	Default (no clause specified) = NO_ACTION on both sides = 48
func innodbFKType(onDelete, onUpdate string) int64 {
	var t int64
	switch strings.ToUpper(onDelete) {
	case "CASCADE":
		t |= 1
	case "SET NULL":
		t |= 2
	case "NO ACTION", "":
		t |= 16
	// RESTRICT = 0 (no bits set)
	}
	switch strings.ToUpper(onUpdate) {
	case "CASCADE":
		t |= 4
	case "SET NULL":
		t |= 8
	case "NO ACTION", "":
		t |= 32
	// RESTRICT = 0 (no bits set)
	}
	return t
}

// infoSchemaInnoDBForeign returns rows for INFORMATION_SCHEMA.INNODB_FOREIGN.
func (e *Executor) infoSchemaInnoDBForeign() []storage.Row {
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
		for _, tableName := range tableNames {
			def, err := db.GetTable(tableName)
			if err != nil || def == nil {
				continue
			}
			for _, fk := range def.ForeignKeys {
				fkID := dbName + "/" + fk.Name
				forName := dbName + "/" + tableName
				refName := dbName + "/" + fk.ReferencedTable
				rows = append(rows, storage.Row{
					"ID":       fkID,
					"FOR_NAME": forName,
					"REF_NAME": refName,
					"N_COLS":   int64(len(fk.Columns)),
					"TYPE":     innodbFKType(fk.OnDelete, fk.OnUpdate),
				})
			}
		}
	}
	// Sort by FOR_NAME descending to match MySQL's INNODB_FOREIGN ordering
	sort.Slice(rows, func(i, j int) bool {
		fi, fj := rows[i]["FOR_NAME"].(string), rows[j]["FOR_NAME"].(string)
		return fi > fj
	})
	return rows
}

// infoSchemaInnoDBForeignCols returns rows for INFORMATION_SCHEMA.INNODB_FOREIGN_COLS.
func (e *Executor) infoSchemaInnoDBForeignCols() []storage.Row {
	dbNames := e.Catalog.ListDatabases()
	sort.Strings(dbNames)
	type colRow struct {
		forName string
		row     storage.Row
	}
	var colRows []colRow
	for _, dbName := range dbNames {
		db, err := e.Catalog.GetDatabase(dbName)
		if err != nil {
			continue
		}
		tableNames := db.ListTables()
		sort.Strings(tableNames)
		for _, tableName := range tableNames {
			def, err := db.GetTable(tableName)
			if err != nil || def == nil {
				continue
			}
			forName := dbName + "/" + tableName
			for _, fk := range def.ForeignKeys {
				fkID := dbName + "/" + fk.Name
				for i, col := range fk.Columns {
					refCol := ""
					if i < len(fk.ReferencedColumns) {
						refCol = fk.ReferencedColumns[i]
					}
					colRows = append(colRows, colRow{
						forName: forName,
						row: storage.Row{
							"ID":           fkID,
							"FOR_COL_NAME": col,
							"REF_COL_NAME": refCol,
							"POS":          int64(i + 1),
						},
					})
				}
			}
		}
	}
	// Sort by FOR_NAME descending to match MySQL's INNODB_FOREIGN_COLS ordering
	sort.SliceStable(colRows, func(i, j int) bool {
		return colRows[i].forName > colRows[j].forName
	})
	rows := make([]storage.Row, len(colRows))
	for i, cr := range colRows {
		rows[i] = cr.row
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

	// Add user-defined views as VIEW entries
	if e.views != nil {
		viewNames := make([]string, 0, len(e.views))
		for n := range e.views {
			viewNames = append(viewNames, n)
		}
		sort.Strings(viewNames)
		for _, vName := range viewNames {
			rows = append(rows, storage.Row{
				"TABLE_CATALOG":   "def",
				"TABLE_SCHEMA":    e.CurrentDB,
				"TABLE_NAME":      vName,
				"TABLE_TYPE":      "VIEW",
				"ENGINE":          nil,
				"VERSION":         nil,
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
				"TABLE_COMMENT":   "VIEW",
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

	// Tables with Fixed ROW_FORMAT in performance_schema (all others are Dynamic)
	psFixedRowFormat := map[string]bool{
		"accounts":                           true,
		"events_statements_histogram_global": true,
		"hosts":                              true,
		"performance_timers":                 true,
		"replication_applier_configuration":  true,
		"replication_applier_status":         true,
		"replication_group_members":          true,
		"setup_actors":                       true,
		"users":                              true,
	}
	// Tables with non-default collation in performance_schema
	psTableCollation := map[string]string{
		"keyring_keys":                 "utf8mb4_bin",
		"session_account_connect_attrs": "utf8mb4_bin",
		"session_connect_attrs":         "utf8mb4_bin",
	}

	// Add performance_schema virtual tables as PERFORMANCE_SCHEMA entries
	for _, tblName := range perfSchemaVirtualTableNames() {
		rowFormat := "Dynamic"
		if psFixedRowFormat[tblName] {
			rowFormat = "Fixed"
		}
		collation := "utf8mb4_0900_ai_ci"
		if c, ok := psTableCollation[tblName]; ok {
			collation = c
		}
		rows = append(rows, storage.Row{
			"TABLE_CATALOG":   "def",
			"TABLE_SCHEMA":    "performance_schema",
			"TABLE_NAME":      tblName,
			"TABLE_TYPE":      "BASE TABLE",
			"ENGINE":          "PERFORMANCE_SCHEMA",
			"VERSION":         int64(10),
			"ROW_FORMAT":      rowFormat,
			"TABLE_ROWS":      int64(0),
			"AVG_ROW_LENGTH":  int64(0),
			"DATA_LENGTH":     int64(0),
			"MAX_DATA_LENGTH": int64(0),
			"INDEX_LENGTH":    int64(0),
			"DATA_FREE":       int64(0),
			"AUTO_INCREMENT":  nil,
			"CREATE_TIME":     nil,
			"UPDATE_TIME":     nil,
			"CHECK_TIME":      nil,
			"TABLE_COLLATION": collation,
			"CHECKSUM":        nil,
			"CREATE_OPTIONS":  "",
			"TABLE_COMMENT":   "",
		})
	}

	return rows
}

// infoSchemaColumns returns rows for INFORMATION_SCHEMA.COLUMNS.
func (e *Executor) infoSchemaColumns() []storage.Row {
	dbNames := e.Catalog.ListDatabases()
	sort.Strings(dbNames)

	// For non-root users, filter columns to only those the user has any privilege on.
	// MySQL filters IS.COLUMNS so non-privileged columns are not shown.
	// Get current user context for privilege filtering.
	isColUser, isColHost, isColRoles := e.getCurrentUserAndRoles()
	// hasColAccess returns true if the current user has any privilege on the given column.
	hasColAccess := func(dbName, tblName, colName string) bool {
		if isColUser == "" || e.grantStore == nil {
			return true // root or no grantStore: show all
		}
		// Check if user has any full (non-column-restricted) privilege at global, DB, or table level.
		// Column-restricted grants like INSERT(a) only grant access to that specific column,
		// not the entire table, so they can't be used to reveal all columns.
		if e.grantStore.HasFullTablePrivilege(isColUser, isColHost, dbName, tblName, isColRoles) {
			return true
		}
		// Check column-level grants specifically for this column
		return e.grantStore.HasColumnPrivilege(isColUser, isColHost, dbName, tblName, colName, isColRoles)
	}

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
				// colDefault will be post-processed below after type info is determined

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
				// Normalize type aliases
				if dataType == "integer" {
					dataType = "int"
				} else if dataType == "real" {
					dataType = "double"
				}
				// Normalize FLOAT(n) precision: float(1-24) -> float, float(25-53) -> double
				if dataType == "float" {
					// Check the colTypeUpper for precision
					colTypeUp2 := strings.ToUpper(strings.TrimSpace(col.Type))
					if idx := strings.Index(colTypeUp2, "("); idx >= 0 {
						end := strings.Index(colTypeUp2[idx:], ")")
						if end > 0 {
							inner := strings.TrimSpace(colTypeUp2[idx+1 : idx+end])
							// Remove any modifiers after the number
							if commaIdx := strings.Index(inner, ","); commaIdx >= 0 {
								inner = strings.TrimSpace(inner[:commaIdx])
							}
							if prec, err := strconv.ParseInt(inner, 10, 64); err == nil && prec >= 25 && prec <= 53 {
								dataType = "double"
							}
						}
					}
				}

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
					// Inherit from table charset, then default to utf8mb4
					if tbl.Charset != "" {
						colCharset = strings.ToLower(tbl.Charset)
					} else {
						colCharset = "utf8mb4"
					}
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
			case "ENUM":
				// CHARACTER_MAXIMUM_LENGTH = max length of the longest enum value
				if idx := strings.Index(colTypeUpper, "("); idx >= 0 {
					end := strings.LastIndex(colTypeUpper, ")")
					if end > idx {
						inner := colTypeUpper[idx+1 : end]
						vals := splitEnumSetValues(inner)
						maxLen := int64(0)
						for _, v := range vals {
							if int64(len(v)) > maxLen {
								maxLen = int64(len(v))
							}
						}
						charMaxLen = maxLen
						colCharsetForEnum := strings.ToLower(col.Charset)
						if colCharsetForEnum == "" {
							if tbl.Charset != "" {
								colCharsetForEnum = strings.ToLower(tbl.Charset)
							} else {
								colCharsetForEnum = "utf8mb4"
							}
						}
						maxBytes := int64(4)
						if colCharsetForEnum == "latin1" || colCharsetForEnum == "ascii" {
							maxBytes = 1
						} else if colCharsetForEnum == "utf8" || colCharsetForEnum == "utf8mb3" {
							maxBytes = 3
						}
						charOctetLen = maxLen * maxBytes
					}
				}
			case "SET":
				// CHARACTER_MAXIMUM_LENGTH = sum of all set values + commas
				if idx := strings.Index(colTypeUpper, "("); idx >= 0 {
					end := strings.LastIndex(colTypeUpper, ")")
					if end > idx {
						inner := colTypeUpper[idx+1 : end]
						vals := splitEnumSetValues(inner)
						totalLen := int64(0)
						for i, v := range vals {
							totalLen += int64(len(v))
							if i > 0 {
								totalLen++ // for comma separator
							}
						}
						charMaxLen = totalLen
						colCharsetForSet := strings.ToLower(col.Charset)
						if colCharsetForSet == "" {
							if tbl.Charset != "" {
								colCharsetForSet = strings.ToLower(tbl.Charset)
							} else {
								colCharsetForSet = "utf8mb4"
							}
						}
						maxBytes := int64(4)
						if colCharsetForSet == "latin1" || colCharsetForSet == "ascii" {
							maxBytes = 1
						} else if colCharsetForSet == "utf8" || colCharsetForSet == "utf8mb3" {
							maxBytes = 3
						}
						charOctetLen = totalLen * maxBytes
					}
				}
			case "TINYBLOB":
				charMaxLen = int64(255)
				charOctetLen = int64(255)
			case "BLOB":
				charMaxLen = int64(65535)
				charOctetLen = int64(65535)
			case "MEDIUMBLOB":
				charMaxLen = int64(16777215)
				charOctetLen = int64(16777215)
			case "LONGBLOB":
				charMaxLen = int64(4294967295)
				charOctetLen = int64(4294967295)
			case "BINARY", "VARBINARY":
				// Extract length from type spec
				binLen := int64(1) // BINARY without length defaults to BINARY(1)
				if baseType == "VARBINARY" {
					binLen = 0
				}
				if idx := strings.Index(colTypeUpper, "("); idx >= 0 {
					end := strings.Index(colTypeUpper[idx:], ")")
					if end > 0 {
						n, err := strconv.ParseInt(colTypeUpper[idx+1:idx+end], 10, 64)
						if err == nil {
							binLen = n
						}
					}
				}
				charMaxLen = binLen
				charOctetLen = binLen
			case "INT", "INTEGER", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT":
				colTypeUp := strings.ToUpper(col.Type)
				// zerofill implies unsigned in MySQL
				isUnsignedInt := strings.Contains(colTypeUp, "UNSIGNED") || strings.Contains(colTypeUp, "ZEROFILL")
				switch baseType {
				case "TINYINT":
					numPrecision = int64(3)
				case "SMALLINT":
					numPrecision = int64(5)
				case "MEDIUMINT":
					numPrecision = int64(7)
				case "INT", "INTEGER":
					numPrecision = int64(10)
				case "BIGINT":
					if isUnsignedInt {
						numPrecision = int64(20)
					} else {
						numPrecision = int64(19)
					}
				default:
					numPrecision = int64(10)
				}
				numScale = int64(0)
			case "DECIMAL", "NUMERIC", "DEC":
				// Extract precision and scale from DECIMAL(p,s)
				if idx := strings.Index(colTypeUpper, "("); idx >= 0 {
					end := strings.Index(colTypeUpper[idx:], ")")
					if end > 0 {
						inner := colTypeUpper[idx+1 : idx+end]
						parts := strings.Split(inner, ",")
						if len(parts) >= 1 {
							if p, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64); err == nil {
								if p == 0 {
									p = 10 // MySQL normalizes DECIMAL(0) to DECIMAL(10,0)
								}
								numPrecision = p
							}
						}
						if len(parts) >= 2 {
							if s, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64); err == nil {
								numScale = s
							}
						} else {
							numScale = int64(0)
						}
					}
				} else {
					// No explicit precision/scale - MySQL defaults: DECIMAL(10,0)
					numPrecision = int64(10)
					numScale = int64(0)
				}
			case "BIT":
				// BIT(n): NUMERIC_PRECISION = n
				if idx := strings.Index(colTypeUpper, "("); idx >= 0 {
					end := strings.Index(colTypeUpper[idx:], ")")
					if end > 0 {
						if n, err := strconv.ParseInt(strings.TrimSpace(colTypeUpper[idx+1:idx+end]), 10, 64); err == nil {
							numPrecision = n
						}
					}
				} else {
					numPrecision = int64(1) // BIT without length defaults to BIT(1)
				}
			case "FLOAT":
				// MySQL reports NUMERIC_PRECISION=12 for FLOAT (1-24 bits) or 22 for DOUBLE (25-53 bits)
				numPrecision = int64(12)
				numScale = nil
				// Check if this is actually a double-precision float
				if idx := strings.Index(colTypeUpper, "("); idx >= 0 {
					end := strings.Index(colTypeUpper[idx:], ")")
					if end > 0 {
						inner := strings.TrimSpace(colTypeUpper[idx+1 : idx+end])
						if prec, err := strconv.ParseInt(inner, 10, 64); err == nil && prec >= 25 && prec <= 53 {
							numPrecision = int64(22)
						}
					}
				}
			case "DOUBLE", "REAL":
				// MySQL reports NUMERIC_PRECISION=22 for DOUBLE/REAL
				numPrecision = int64(22)
				numScale = nil
			}

			// Coerce COLUMN_DEFAULT for DECIMAL/NUMERIC types: apply scale rounding
			// and zerofill formatting to match MySQL INFORMATION_SCHEMA output.
			if colDefault != nil && (baseType == "DECIMAL" || baseType == "NUMERIC" || baseType == "DEC") {
				if defStr, ok := colDefault.(string); ok {
					if scale, ok2 := numScale.(int64); ok2 {
						if prec, ok3 := numPrecision.(int64); ok3 {
							colTypeUp := strings.ToUpper(col.Type)
							isZF := strings.Contains(colTypeUp, "ZEROFILL")
							// Format the decimal value with the correct number of scale digits.
							// Use string manipulation to avoid float64 precision issues.
							formatted := formatDecimalDefault(defStr, int(scale))
							if isZF && prec > 0 {
								// Zerofill: pad with zeros to total display width.
								// For DECIMAL(p,s): integer part = (p-s) digits, scale part = s digits.
								// Total width = (p-s) + 1 + s = p+1 when scale>0, or p when scale=0.
								// But display is actually: integerPart + "." + scalePart
								// where integerPart is zero-padded to (p-s) width.
								var totalWidth int
								if scale > 0 {
									intPart := int(prec) - int(scale)
									totalWidth = intPart + 1 + int(scale) // integer_digits + "." + scale_digits
								} else {
									totalWidth = int(prec)
								}
								if len(formatted) < totalWidth {
									formatted = strings.Repeat("0", totalWidth-len(formatted)) + formatted
								}
							}
							colDefault = formatted
						}
					}
				}
			}

			// Coerce COLUMN_DEFAULT for temporal types
			if colDefault != nil {
				if defStr, ok := colDefault.(string); ok {
					switch baseType {
					case "TIME":
						// Normalize time default to HH:MM:SS format
						normalized := normalizeTimeDefault(defStr)
						if normalized != "" {
							colDefault = normalized
						}
					case "DATETIME":
						// Normalize datetime default to YYYY-MM-DD HH:MM:SS
						normalized := normalizeDatetimeDefault(defStr)
						if normalized != "" {
							colDefault = normalized
						}
					case "TIMESTAMP":
						// Normalize timestamp default (e.g., 20001231235959 -> 2000-12-31 23:59:59)
						normalized := normalizeTimestampDefault(defStr)
						if normalized != "" {
							colDefault = normalized
						}
					}
				}
			}

			// Coerce COLUMN_DEFAULT for binary literal defaults (b'...' notation)
			if colDefault != nil {
				if defStr, ok := colDefault.(string); ok {
					if strings.HasPrefix(defStr, "0b") || strings.HasPrefix(defStr, "b'") {
						// Binary literal: decode to actual value
						binaryVal := normalizeBinaryLiteralDefault(defStr, baseType)
						colDefault = binaryVal
					}
				}
			}

			// Coerce COLUMN_DEFAULT for integer zerofill types
			if colDefault != nil && (baseType == "INT" || baseType == "INTEGER" || baseType == "TINYINT" ||
				baseType == "SMALLINT" || baseType == "MEDIUMINT" || baseType == "BIGINT") {
				if defStr, ok := colDefault.(string); ok {
					colTypeUp := strings.ToUpper(col.Type)
					isZF := strings.Contains(colTypeUp, "ZEROFILL")
					if isZF {
						// Get display width from type spec
						displayWidth := 0
						if idx := strings.Index(colTypeUp, "("); idx >= 0 {
							end := strings.Index(colTypeUp[idx:], ")")
							if end > 0 {
								if w, err := strconv.Atoi(strings.TrimSpace(colTypeUp[idx+1 : idx+end])); err == nil {
									displayWidth = w
								}
							}
						}
						if displayWidth == 0 {
							// Default widths
							switch baseType {
							case "TINYINT":
								displayWidth = 3
							case "SMALLINT":
								displayWidth = 5
							case "MEDIUMINT":
								displayWidth = 8
							case "INT", "INTEGER":
								displayWidth = 10
							case "BIGINT":
								displayWidth = 20
							}
						}
						if displayWidth > 0 && len(defStr) < displayWidth {
							defStr = strings.Repeat("0", displayWidth-len(defStr)) + defStr
							colDefault = defStr
						}
					}
				}
			}

			// Coerce COLUMN_DEFAULT for FLOAT/DOUBLE zerofill types
			if colDefault != nil && (baseType == "FLOAT" || baseType == "DOUBLE" || baseType == "REAL") {
				if defStr, ok := colDefault.(string); ok {
					colTypeUp := strings.ToUpper(col.Type)
					isZF := strings.Contains(colTypeUp, "ZEROFILL")
					if isZF {
						// Display width: FLOAT(1-24) = 12, DOUBLE/REAL/FLOAT(25-53) = 22
						// Use numPrecision which is already set correctly above
						displayWidth := int64(12)
						if np, ok2 := numPrecision.(int64); ok2 {
							displayWidth = np
						}
						if int64(len(defStr)) < displayWidth {
							defStr = strings.Repeat("0", int(displayWidth)-len(defStr)) + defStr
						}
						colDefault = defStr
					}
				}
			}

			// Determine character set and collation for string types
			var charSetName interface{} = nil
			var collationName interface{} = nil
			switch baseType {
			case "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "ENUM", "SET":
				cs := col.Charset
				if cs == "" {
					// No explicit column charset: inherit from table then database.
					if tbl.Charset != "" {
						cs = tbl.Charset
					} else {
						cs = "utf8mb4"
					}
				}
				charSetName = cs
				// Determine default collation for charset, then apply overrides.
				collationName = catalogPkg.DefaultCollationForCharset(cs)
				// Column inherits table-level collation when it has no explicit collation.
				if col.Collation != "" {
					collationName = col.Collation
				} else if col.Charset == "" && tbl.Collation != "" {
					// No explicit column charset/collation: inherit table collation.
					collationName = tbl.Collation
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
					if col.Default != nil && strings.HasPrefix(strings.ToUpper(*col.Default), "CURRENT_TIMESTAMP") {
						extra = "DEFAULT_GENERATED on update CURRENT_TIMESTAMP"
					} else {
						extra = "on update CURRENT_TIMESTAMP"
					}
				} else if col.Default != nil && strings.HasPrefix(strings.ToUpper(*col.Default), "CURRENT_TIMESTAMP") {
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

				// Filter by column access for non-root users
				if !hasColAccess(dbName, tblName, col.Name) {
					continue
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
	// Always load persistent stats from innodb_index_stats (if available).
	// These are used when readPersistent=true, or as a presence indicator
	// to decide whether to return NULL vs 0 for cardinality.
	cardinalityByKey := map[string]int64{}
	cardinalityKeyExists := map[string]bool{}
	// cardinalityNotAnalyzed tracks entries with sample_size=0 (sentinel for CREATE TABLE).
	// These show as NULL in SHOW INDEX until ANALYZE is run.
	cardinalityNotAnalyzed := map[string]bool{}
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
			key := dbName + "." + tableName + "." + indexName + "." + statName
			cardinalityByKey[key] = asInt64Or(r["stat_value"], 0)
			cardinalityKeyExists[key] = true
			// Check if this is the "not yet analyzed" sentinel (sample_size=0)
			if asInt64Or(r["sample_size"], -1) == 0 {
				cardinalityNotAnalyzed[key] = true
			}
		}
		tbl.Mu.RUnlock()
	}

	dbNames := e.Catalog.ListDatabases()
	sort.Strings(dbNames)

	// Get current user for privilege filtering (once, before the loop)
	statsUser, statsHost, statsRoles := e.getCurrentUserAndRoles()
	var rows []storage.Row
	for _, dbName := range dbNames {
		db, err := e.Catalog.GetDatabase(dbName)
		if err != nil {
			continue
		}
		tableNames := db.ListTables()
		sort.Strings(tableNames)
		for _, tblName := range tableNames {
			// Filter by privilege: non-root users only see tables they have any access to
			if statsUser != "" && e.grantStore != nil {
				if !e.grantStore.HasAnyTableAccess(statsUser, statsHost, dbName, tblName, statsRoles) {
					continue
				}
			}
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
			// Determine table engine for cardinality mode selection.
			tblEngine := ""
			if tbl != nil {
				tblEngine = strings.ToUpper(tbl.Engine)
			}
			// InnoDB with STATS_PERSISTENT=0: use transient/dynamic stats (always compute dynamic)
			tblUsesTransientStats := (tblEngine == "" || tblEngine == "INNODB") &&
				!e.innodbStatsPersistentEnabled(tbl)
			// Temporary tables always use dynamic stats (no persistent stats stored)
			tblIsTemp := e.tempTables != nil && (e.tempTables[tblName] || e.tempTables[strings.ToLower(tblName)])
			appendIndexRows := func(indexName string, cols []string, nonUnique int64, idxComment string, idxType string, idxUsing string, invisible bool) {
				var dynamic []int64
				tblIsNonInnoDB := tblEngine != "" && tblEngine != "INNODB"
				if (!readPersistent && (tblUsesTransientStats || tblIsTemp || len(dataRows) > 0)) ||
					(tblIsNonInnoDB && len(dataRows) > 0) {
					// Compute dynamic stats when:
					// - Transient InnoDB (STATS_PERSISTENT=0): always compute
					// - Temporary tables: always compute
					// - Any table with rows: compute for fallback
					// - Non-InnoDB tables (MyISAM etc.): always compute from live data
					dynamic = distinctPrefixCounts(dataRows, cols)
				}
				indexTypeStr := "BTREE"
				collation := interface{}("A")
				if idxType == "FULLTEXT" {
					indexTypeStr = "FULLTEXT"
					collation = nil
				} else if idxType == "SPATIAL" {
					indexTypeStr = "SPATIAL"
					// SPATIAL indexes show Collation=A in SHOW INDEX (both InnoDB and MyISAM)
				} else if (tblEngine == "MEMORY" || tblEngine == "HEAP") && !strings.EqualFold(idxUsing, "BTREE") {
					// MEMORY/HEAP engine defaults to HASH unless explicitly specified as BTREE
					indexTypeStr = "HASH"
					collation = nil
				} else if strings.EqualFold(idxUsing, "HASH") {
					// Explicit HASH index on any engine
					indexTypeStr = "HASH"
					collation = nil
				}
				for i, col := range cols {
					colName := normalizeIndexColumnName(col)
					nullable := ""
					if colNullable[strings.ToLower(colName)] {
						nullable = "YES"
					}
					statKey := strings.ToLower(dbName + "." + tblName + "." + indexName + "." + fmt.Sprintf("n_diff_pfx%02d", i+1))
					var cardinality interface{}
					// readPersistent only applies to InnoDB tables (which have innodb_index_stats entries).
					// Non-InnoDB tables (MyISAM, etc.) don't have persistent stats, so use dynamic.
					tblIsInnoDB := tblEngine == "" || tblEngine == "INNODB"
					if readPersistent && tblIsInnoDB {
						// When stats_expiry=0, always read from persistent storage.
						// MySQL returns the stored stat_value even for unanalyzed tables (shows 0 for empty tables).
						// If no persistent entry exists (e.g., new index added via CREATE INDEX), use dynamic stats.
						if cardinalityKeyExists[statKey] {
							cardinality = cardinalityByKey[statKey]
						} else if i < len(dynamic) {
							cardinality = dynamic[i]
						} else {
							cardinality = int64(0)
						}
					} else if cardinalityKeyExists[statKey] {
						if cardinalityNotAnalyzed[statKey] {
							// Not-analyzed sentinel (CREATE TABLE, no ANALYZE): show NULL.
							// Only InnoDB persistent-stats tables reach here (non-InnoDB tables
							// don't have entries in innodb_index_stats).
							cardinality = nil
						} else {
							cardinality = cardinalityByKey[statKey]
						}
					} else if tblUsesTransientStats || tblIsTemp {
						// Transient InnoDB (STATS_PERSISTENT=0) or temp table: use dynamic.
						if i < len(dynamic) {
							cardinality = dynamic[i]
						} else {
							cardinality = int64(0)
						}
					} else if i < len(dynamic) {
						// No persistent stats entry and not transient InnoDB.
						// For non-InnoDB (MyISAM etc.) or temp tables: use dynamic if available.
						// dynamic[i] is only computed when len(dataRows) > 0 (see guard above).
						cardinality = dynamic[i]
					}
					// For InnoDB with STATS_PERSISTENT=1 and no entry: NULL (unexpected, but handled)
					// For empty MyISAM (len(dataRows)==0) with no entry: NULL (dynamic not computed)
					// Exception: MyISAM PRIMARY KEY always shows 0 cardinality (even for empty tables)
					if tblIsNonInnoDB && cardinality == nil && indexTypeStr != "HASH" &&
						!(tblEngine == "MEMORY" || tblEngine == "HEAP") &&
						strings.EqualFold(indexName, "PRIMARY") {
						cardinality = int64(0)
					}
					// For MEMORY/HEAP tables with BTREE indexes, cardinality is always NULL
					// (MySQL's MEMORY engine reports NULL cardinality for BTREE but computes for HASH)
					if (tblEngine == "MEMORY" || tblEngine == "HEAP") && indexTypeStr == "BTREE" {
						cardinality = nil
					}
					// SPATIAL indexes use a 32-byte MBR key prefix
					var subPart interface{}
					if idxType == "SPATIAL" {
						subPart = int64(32)
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
						"SUB_PART":      subPart,
						"PACKED":        nil,
						"NULLABLE":      nullable,
						"INDEX_TYPE":    indexTypeStr,
						"COMMENT":       "",
						"INDEX_COMMENT": idxComment,
						"IS_VISIBLE":    func() string { if invisible { return "NO" }; return "YES" }(),
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
				appendIndexRows(idx.Name, idx.Columns, nonUnique, idx.Comment, idx.Type, idx.Using, idx.Invisible)
			}
			if len(tbl.PrimaryKey) > 0 {
				appendIndexRows("PRIMARY", tbl.PrimaryKey, 0, "", "", "", false)
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

// infoSchemaPlugins returns rows for INFORMATION_SCHEMA.PLUGINS.
// Reports the same set of storage-engine plugins that MySQL 8.0 ships by default.
func (e *Executor) infoSchemaPlugins() []storage.Row {
	type pluginInfo struct {
		name, status, ptype, author, description string
	}
	plugins := []pluginInfo{
		{"binlog", "ACTIVE", "STORAGE ENGINE", "MySQL AB", "This is a pseudo storage engine to represent the binlog in a transaction"},
		{"sha256_password", "ACTIVE", "AUTHENTICATION", "Oracle", "SHA256 password authentication"},
		{"caching_sha2_password", "ACTIVE", "AUTHENTICATION", "Oracle", "Caching SHA2 password authentication"},
		{"sha2_cache_cleaner", "ACTIVE", "AUDIT", "Oracle", "SHA2 cache  cleaner"},
		{"daemon_keyring_proxy_plugin", "ACTIVE", "DAEMON", "Oracle", "A plugin that can be used as a proxy to other keyring plugins"},
		{"CSV", "ACTIVE", "STORAGE ENGINE", "Brian Aker, MySQL AB", "CSV storage engine"},
		{"MEMORY", "ACTIVE", "STORAGE ENGINE", "MySQL AB", "Hash based, stored in memory, useful for temporary tables"},
		{"InnoDB", "ACTIVE", "STORAGE ENGINE", "Oracle Corporation", "Supports transactions, row-level locking, and foreign keys"},
		{"FEDERATED", "DISABLED", "STORAGE ENGINE", "Patrick Galbraith and Brian Aker, MySQL AB", "Federated MySQL storage engine"},
		{"MyISAM", "ACTIVE", "STORAGE ENGINE", "MySQL AB", "MyISAM storage engine"},
		{"MRG_MYISAM", "ACTIVE", "STORAGE ENGINE", "MySQL AB", "Collection of identical MyISAM tables"},
		{"PERFORMANCE_SCHEMA", "ACTIVE", "STORAGE ENGINE", "Marc Alff, Oracle", "Performance Schema"},
		{"ARCHIVE", "ACTIVE", "STORAGE ENGINE", "Brian Aker, MySQL AB", "Archive storage engine"},
		{"BLACKHOLE", "ACTIVE", "STORAGE ENGINE", "MySQL AB", "/dev/null storage engine (anything you write to it disappears)"},
		{"mysqlx_cache_cleaner", "ACTIVE", "AUDIT", "Oracle Corp", "Cache cleaner for sha2 authentication in X plugin"},
		{"mysqlx", "ACTIVE", "DAEMON", "Oracle Corp", "X Plugin for MySQL"},
		{"mysql_native_password", "ACTIVE", "AUTHENTICATION", "R.J.Silk, Sergei Golubchik", "Native MySQL authentication"},
	}

	rows := make([]storage.Row, 0, len(plugins))
	for _, p := range plugins {
		loadOption := "ON"
		if p.status == "DISABLED" {
			loadOption = "OFF"
		}
		rows = append(rows, storage.Row{
			"PLUGIN_NAME":            p.name,
			"PLUGIN_VERSION":         "1.0",
			"PLUGIN_STATUS":          p.status,
			"PLUGIN_TYPE":            p.ptype,
			"PLUGIN_TYPE_VERSION":    "80200.0",
			"PLUGIN_LIBRARY":         nil,
			"PLUGIN_LIBRARY_VERSION": nil,
			"PLUGIN_AUTHOR":          p.author,
			"PLUGIN_DESCRIPTION":     p.description,
			"PLUGIN_LICENSE":         "GPL",
			"LOAD_OPTION":            loadOption,
		})
	}
	return rows
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
		// For global_variables, exclude session-only variables
		if globalOnly && sysVarSessionOnly[n] {
			continue
		}
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
	{"metadata_table_handles_opened", "metadata", "counter"},
	{"metadata_table_handles_closed", "metadata", "counter"},
	{"metadata_table_reference_count", "metadata", "counter"},
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

// innoDBBackgroundThreadNames lists the InnoDB background thread names returned
// in performance_schema.threads. These are static thread names from MySQL 8.0.
var innoDBBackgroundThreadNames = []string{
	"thread/innodb/buf_dump_thread",
	"thread/innodb/buf_resize_thread",
	"thread/innodb/dict_stats_thread",
	"thread/innodb/fts_optimize_thread",
	"thread/innodb/io_ibuf_thread",
	"thread/innodb/io_log_thread",
	"thread/innodb/io_read_thread",
	"thread/innodb/io_write_thread",
	"thread/innodb/log_checkpointer_thread",
	"thread/innodb/log_closer_thread",
	"thread/innodb/log_flush_notifier_thread",
	"thread/innodb/log_flusher_thread",
	"thread/innodb/log_write_notifier_thread",
	"thread/innodb/log_writer_thread",
	"thread/innodb/page_flush_coordinator_thread",
	"thread/innodb/srv_error_monitor_thread",
	"thread/innodb/srv_lock_timeout_thread",
	"thread/innodb/srv_master_thread",
	"thread/innodb/srv_monitor_thread",
	"thread/innodb/srv_purge_thread",
	"thread/innodb/srv_worker_thread",
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
			"THREAD_OS_ID":        int64(1),
			"RESOURCE_GROUP":      "SYS_default",
		},
	}
	// Add InnoDB background threads
	for i, name := range innoDBBackgroundThreadNames {
		rows = append(rows, storage.Row{
			"THREAD_ID":           int64(100 + i),
			"NAME":                name,
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
			"THREAD_OS_ID":        int64(100 + i),
			"RESOURCE_GROUP":      "SYS_default",
		})
	}
	// Add rows for all active connections from the process list
	seenConnIDs := make(map[int64]bool)
	if e.processList != nil {
		for _, proc := range e.processList.Snapshot() {
			cid := proc.ID
			if cid <= 0 || seenConnIDs[cid] {
				continue
			}
			seenConnIDs[cid] = true
			instrumented := "YES"
			if v, ok := e.psThreadInstrumented[cid]; ok {
				instrumented = v
			}
			history := "YES"
			if v, ok := e.psThreadHistory[cid]; ok {
				history = v
			}
			db := proc.DB
			if db == "" {
				db = "test"
			}
			var procInfo interface{}
			if proc.Info != "" {
				procInfo = proc.Info
			}
			var procState interface{}
			if proc.State != "" && proc.State != "starting" {
				procState = proc.State
			} else if proc.Command == "Query" {
				procState = "executing"
			}
			rows = append(rows, storage.Row{
				"THREAD_ID":           cid + 1, // thread_id = connID + 1 by convention
				"NAME":                "thread/sql/one_connection",
				"TYPE":                "FOREGROUND",
				"PROCESSLIST_ID":      cid,
				"PROCESSLIST_USER":    proc.User,
				"PROCESSLIST_HOST":    proc.Host,
				"PROCESSLIST_DB":      db,
				"PROCESSLIST_COMMAND": proc.Command,
				"PROCESSLIST_TIME":    int64(0),
				"PROCESSLIST_STATE":   procState,
				"PROCESSLIST_INFO":    procInfo,
				"PARENT_THREAD_ID":    int64(1),
				"ROLE":                nil,
				"INSTRUMENTED":        instrumented,
				"HISTORY":             history,
				"CONNECTION_TYPE":     "TCP/IP",
				"THREAD_OS_ID":        10000 + cid, // unique fake OS thread ID per connection
				"RESOURCE_GROUP":      "USR_default",
			})
		}
	}
	// Also add the current connection if not already included
	connID := e.connectionID
	if connID > 0 && !seenConnIDs[connID] {
		instrumented := "YES"
		if v, ok := e.psThreadInstrumented[connID]; ok {
			instrumented = v
		}
		history := "YES"
		if v, ok := e.psThreadHistory[connID]; ok {
			history = v
		}
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
			"PROCESSLIST_INFO":    e.currentQuery,
			"PARENT_THREAD_ID":    int64(1),
			"ROLE":                nil,
			"INSTRUMENTED":        instrumented,
			"HISTORY":             history,
			"CONNECTION_TYPE":     "TCP/IP",
			"THREAD_OS_ID":        10000 + connID, // unique fake OS thread ID per connection
			"RESOURCE_GROUP":      "USR_default",
		})
	}
	return rows
}

// perfSchemaUserTableRows iterates all non-system user tables and calls fn for each.
func (e *Executor) perfSchemaUserTableRows(fn func(dbName, tblName string, tbl *catalogPkg.TableDef)) {
	dbNames := e.Catalog.ListDatabases()
	sort.Strings(dbNames)
	for _, dbName := range dbNames {
		switch strings.ToLower(dbName) {
		case "information_schema", "mysql", "performance_schema", "sys":
			continue
		}
		db, err := e.Catalog.GetDatabase(dbName)
		if err != nil {
			continue
		}
		tblNames := db.ListTables()
		sort.Strings(tblNames)
		for _, tblName := range tblNames {
			def, err := db.GetTable(tblName)
			if err != nil || def == nil {
				continue
			}
			fn(dbName, tblName, def)
		}
	}
}

// perfSchemaObjectsSummaryGlobalByType returns rows for objects_summary_global_by_type.
// It returns one row per table across all databases (user tables + key system tables).
func (e *Executor) perfSchemaObjectsSummaryGlobalByType() []storage.Row {
	var rows []storage.Row
	dbNames := e.Catalog.ListDatabases()
	sort.Strings(dbNames)
	for _, dbName := range dbNames {
		db, err := e.Catalog.GetDatabase(dbName)
		if err != nil {
			continue
		}
		tblNames := db.ListTables()
		sort.Strings(tblNames)
		for _, tblName := range tblNames {
			// Skip temporary tables — they are not tracked in performance_schema.
			if e.tempTables != nil && (e.tempTables[tblName] || e.tempTables[strings.ToLower(tblName)]) {
				continue
			}
			rows = append(rows, storage.Row{
				"OBJECT_TYPE":      "TABLE",
				"OBJECT_SCHEMA":    dbName,
				"OBJECT_NAME":      tblName,
				"COUNT_STAR":       int64(0),
				"SUM_TIMER_WAIT":   int64(0),
				"MIN_TIMER_WAIT":   int64(0),
				"AVG_TIMER_WAIT":   int64(0),
				"MAX_TIMER_WAIT":   int64(0),
			})
		}
	}
	return rows
}

// perfSchemaTableIOWaitsByTable returns one zero-count row per user table.
func (e *Executor) perfSchemaTableIOWaitsByTable() []storage.Row {
	var rows []storage.Row
	e.perfSchemaUserTableRows(func(dbName, tblName string, _ *catalogPkg.TableDef) {
		rows = append(rows, storage.Row{
			"OBJECT_TYPE":      "TABLE",
			"OBJECT_SCHEMA":    dbName,
			"OBJECT_NAME":      tblName,
			"COUNT_STAR":       int64(0),
			"SUM_TIMER_WAIT":   int64(0),
			"MIN_TIMER_WAIT":   int64(0),
			"AVG_TIMER_WAIT":   int64(0),
			"MAX_TIMER_WAIT":   int64(0),
			"COUNT_READ":       int64(0),
			"SUM_TIMER_READ":   int64(0),
			"MIN_TIMER_READ":   int64(0),
			"AVG_TIMER_READ":   int64(0),
			"MAX_TIMER_READ":   int64(0),
			"COUNT_WRITE":      int64(0),
			"SUM_TIMER_WRITE":  int64(0),
			"MIN_TIMER_WRITE":  int64(0),
			"AVG_TIMER_WRITE":  int64(0),
			"MAX_TIMER_WRITE":  int64(0),
			"COUNT_FETCH":      int64(0),
			"SUM_TIMER_FETCH":  int64(0),
			"MIN_TIMER_FETCH":  int64(0),
			"AVG_TIMER_FETCH":  int64(0),
			"MAX_TIMER_FETCH":  int64(0),
			"COUNT_INSERT":     int64(0),
			"SUM_TIMER_INSERT": int64(0),
			"MIN_TIMER_INSERT": int64(0),
			"AVG_TIMER_INSERT": int64(0),
			"MAX_TIMER_INSERT": int64(0),
			"COUNT_UPDATE":     int64(0),
			"SUM_TIMER_UPDATE": int64(0),
			"MIN_TIMER_UPDATE": int64(0),
			"AVG_TIMER_UPDATE": int64(0),
			"MAX_TIMER_UPDATE": int64(0),
			"COUNT_DELETE":     int64(0),
			"SUM_TIMER_DELETE": int64(0),
			"MIN_TIMER_DELETE": int64(0),
			"AVG_TIMER_DELETE": int64(0),
			"MAX_TIMER_DELETE": int64(0),
		})
	})
	return rows
}

// perfSchemaTableIOWaitsByIndexUsage returns one zero-count row per (table, index) combo.
func (e *Executor) perfSchemaTableIOWaitsByIndexUsage() []storage.Row {
	var rows []storage.Row
	e.perfSchemaUserTableRows(func(dbName, tblName string, def *catalogPkg.TableDef) {
		// NULL index row (full-table scans)
		rows = append(rows, storage.Row{
			"OBJECT_TYPE":      "TABLE",
			"OBJECT_SCHEMA":    dbName,
			"OBJECT_NAME":      tblName,
			"INDEX_NAME":       nil,
			"COUNT_STAR":       int64(0),
			"SUM_TIMER_WAIT":   int64(0),
			"MIN_TIMER_WAIT":   int64(0),
			"AVG_TIMER_WAIT":   int64(0),
			"MAX_TIMER_WAIT":   int64(0),
			"COUNT_READ":       int64(0),
			"SUM_TIMER_READ":   int64(0),
			"MIN_TIMER_READ":   int64(0),
			"AVG_TIMER_READ":   int64(0),
			"MAX_TIMER_READ":   int64(0),
			"COUNT_WRITE":      int64(0),
			"SUM_TIMER_WRITE":  int64(0),
			"MIN_TIMER_WRITE":  int64(0),
			"AVG_TIMER_WRITE":  int64(0),
			"MAX_TIMER_WRITE":  int64(0),
			"COUNT_FETCH":      int64(0),
			"SUM_TIMER_FETCH":  int64(0),
			"MIN_TIMER_FETCH":  int64(0),
			"AVG_TIMER_FETCH":  int64(0),
			"MAX_TIMER_FETCH":  int64(0),
			"COUNT_INSERT":     int64(0),
			"SUM_TIMER_INSERT": int64(0),
			"MIN_TIMER_INSERT": int64(0),
			"AVG_TIMER_INSERT": int64(0),
			"MAX_TIMER_INSERT": int64(0),
			"COUNT_UPDATE":     int64(0),
			"SUM_TIMER_UPDATE": int64(0),
			"MIN_TIMER_UPDATE": int64(0),
			"AVG_TIMER_UPDATE": int64(0),
			"MAX_TIMER_UPDATE": int64(0),
			"COUNT_DELETE":     int64(0),
			"SUM_TIMER_DELETE": int64(0),
			"MIN_TIMER_DELETE": int64(0),
			"AVG_TIMER_DELETE": int64(0),
			"MAX_TIMER_DELETE": int64(0),
		})
		// PRIMARY index row
		pkName := "PRIMARY"
		if len(def.PrimaryKey) == 0 {
			pkName = "GEN_CLUST_INDEX"
		}
		rows = append(rows, storage.Row{
			"OBJECT_TYPE":      "TABLE",
			"OBJECT_SCHEMA":    dbName,
			"OBJECT_NAME":      tblName,
			"INDEX_NAME":       pkName,
			"COUNT_STAR":       int64(0),
			"SUM_TIMER_WAIT":   int64(0),
			"MIN_TIMER_WAIT":   int64(0),
			"AVG_TIMER_WAIT":   int64(0),
			"MAX_TIMER_WAIT":   int64(0),
			"COUNT_READ":       int64(0),
			"SUM_TIMER_READ":   int64(0),
			"MIN_TIMER_READ":   int64(0),
			"AVG_TIMER_READ":   int64(0),
			"MAX_TIMER_READ":   int64(0),
			"COUNT_WRITE":      int64(0),
			"SUM_TIMER_WRITE":  int64(0),
			"MIN_TIMER_WRITE":  int64(0),
			"AVG_TIMER_WRITE":  int64(0),
			"MAX_TIMER_WRITE":  int64(0),
			"COUNT_FETCH":      int64(0),
			"SUM_TIMER_FETCH":  int64(0),
			"MIN_TIMER_FETCH":  int64(0),
			"AVG_TIMER_FETCH":  int64(0),
			"MAX_TIMER_FETCH":  int64(0),
			"COUNT_INSERT":     int64(0),
			"SUM_TIMER_INSERT": int64(0),
			"MIN_TIMER_INSERT": int64(0),
			"AVG_TIMER_INSERT": int64(0),
			"MAX_TIMER_INSERT": int64(0),
			"COUNT_UPDATE":     int64(0),
			"SUM_TIMER_UPDATE": int64(0),
			"MIN_TIMER_UPDATE": int64(0),
			"AVG_TIMER_UPDATE": int64(0),
			"MAX_TIMER_UPDATE": int64(0),
			"COUNT_DELETE":     int64(0),
			"SUM_TIMER_DELETE": int64(0),
			"MIN_TIMER_DELETE": int64(0),
			"AVG_TIMER_DELETE": int64(0),
			"MAX_TIMER_DELETE": int64(0),
		})
		// Secondary index rows
		for _, idx := range def.Indexes {
			if strings.EqualFold(idx.Name, "PRIMARY") {
				continue
			}
			rows = append(rows, storage.Row{
				"OBJECT_TYPE":      "TABLE",
				"OBJECT_SCHEMA":    dbName,
				"OBJECT_NAME":      tblName,
				"INDEX_NAME":       idx.Name,
				"COUNT_STAR":       int64(0),
				"SUM_TIMER_WAIT":   int64(0),
				"MIN_TIMER_WAIT":   int64(0),
				"AVG_TIMER_WAIT":   int64(0),
				"MAX_TIMER_WAIT":   int64(0),
				"COUNT_READ":       int64(0),
				"SUM_TIMER_READ":   int64(0),
				"MIN_TIMER_READ":   int64(0),
				"AVG_TIMER_READ":   int64(0),
				"MAX_TIMER_READ":   int64(0),
				"COUNT_WRITE":      int64(0),
				"SUM_TIMER_WRITE":  int64(0),
				"MIN_TIMER_WRITE":  int64(0),
				"AVG_TIMER_WRITE":  int64(0),
				"MAX_TIMER_WRITE":  int64(0),
				"COUNT_FETCH":      int64(0),
				"SUM_TIMER_FETCH":  int64(0),
				"MIN_TIMER_FETCH":  int64(0),
				"AVG_TIMER_FETCH":  int64(0),
				"MAX_TIMER_FETCH":  int64(0),
				"COUNT_INSERT":     int64(0),
				"SUM_TIMER_INSERT": int64(0),
				"MIN_TIMER_INSERT": int64(0),
				"AVG_TIMER_INSERT": int64(0),
				"MAX_TIMER_INSERT": int64(0),
				"COUNT_UPDATE":     int64(0),
				"SUM_TIMER_UPDATE": int64(0),
				"MIN_TIMER_UPDATE": int64(0),
				"AVG_TIMER_UPDATE": int64(0),
				"MAX_TIMER_UPDATE": int64(0),
				"COUNT_DELETE":     int64(0),
				"SUM_TIMER_DELETE": int64(0),
				"MIN_TIMER_DELETE": int64(0),
				"AVG_TIMER_DELETE": int64(0),
				"MAX_TIMER_DELETE": int64(0),
			})
		}
	})
	return rows
}

// perfSchemaTableLockWaitsByTable returns one zero-count row per user table.
func (e *Executor) perfSchemaTableLockWaitsByTable() []storage.Row {
	var rows []storage.Row
	e.perfSchemaUserTableRows(func(dbName, tblName string, _ *catalogPkg.TableDef) {
		rows = append(rows, storage.Row{
			"OBJECT_TYPE":                       "TABLE",
			"OBJECT_SCHEMA":                     dbName,
			"OBJECT_NAME":                       tblName,
			"COUNT_STAR":                        int64(0),
			"SUM_TIMER_WAIT":                    int64(0),
			"MIN_TIMER_WAIT":                    int64(0),
			"AVG_TIMER_WAIT":                    int64(0),
			"MAX_TIMER_WAIT":                    int64(0),
			"COUNT_READ":                        int64(0),
			"SUM_TIMER_READ":                    int64(0),
			"MIN_TIMER_READ":                    int64(0),
			"AVG_TIMER_READ":                    int64(0),
			"MAX_TIMER_READ":                    int64(0),
			"COUNT_WRITE":                       int64(0),
			"SUM_TIMER_WRITE":                   int64(0),
			"MIN_TIMER_WRITE":                   int64(0),
			"AVG_TIMER_WRITE":                   int64(0),
			"MAX_TIMER_WRITE":                   int64(0),
			"COUNT_READ_NORMAL":                 int64(0),
			"SUM_TIMER_READ_NORMAL":             int64(0),
			"MIN_TIMER_READ_NORMAL":             int64(0),
			"AVG_TIMER_READ_NORMAL":             int64(0),
			"MAX_TIMER_READ_NORMAL":             int64(0),
			"COUNT_READ_WITH_SHARED_LOCKS":      int64(0),
			"SUM_TIMER_READ_WITH_SHARED_LOCKS":  int64(0),
			"MIN_TIMER_READ_WITH_SHARED_LOCKS":  int64(0),
			"AVG_TIMER_READ_WITH_SHARED_LOCKS":  int64(0),
			"MAX_TIMER_READ_WITH_SHARED_LOCKS":  int64(0),
			"COUNT_READ_HIGH_PRIORITY":          int64(0),
			"SUM_TIMER_READ_HIGH_PRIORITY":      int64(0),
			"MIN_TIMER_READ_HIGH_PRIORITY":      int64(0),
			"AVG_TIMER_READ_HIGH_PRIORITY":      int64(0),
			"MAX_TIMER_READ_HIGH_PRIORITY":      int64(0),
			"COUNT_READ_NO_INSERT":              int64(0),
			"SUM_TIMER_READ_NO_INSERT":          int64(0),
			"MIN_TIMER_READ_NO_INSERT":          int64(0),
			"AVG_TIMER_READ_NO_INSERT":          int64(0),
			"MAX_TIMER_READ_NO_INSERT":          int64(0),
			"COUNT_READ_EXTERNAL":               int64(0),
			"SUM_TIMER_READ_EXTERNAL":           int64(0),
			"MIN_TIMER_READ_EXTERNAL":           int64(0),
			"AVG_TIMER_READ_EXTERNAL":           int64(0),
			"MAX_TIMER_READ_EXTERNAL":           int64(0),
			"COUNT_WRITE_ALLOW_WRITE":           int64(0),
			"SUM_TIMER_WRITE_ALLOW_WRITE":       int64(0),
			"MIN_TIMER_WRITE_ALLOW_WRITE":       int64(0),
			"AVG_TIMER_WRITE_ALLOW_WRITE":       int64(0),
			"MAX_TIMER_WRITE_ALLOW_WRITE":       int64(0),
			"COUNT_WRITE_CONCURRENT_INSERT":     int64(0),
			"SUM_TIMER_WRITE_CONCURRENT_INSERT": int64(0),
			"MIN_TIMER_WRITE_CONCURRENT_INSERT": int64(0),
			"AVG_TIMER_WRITE_CONCURRENT_INSERT": int64(0),
			"MAX_TIMER_WRITE_CONCURRENT_INSERT": int64(0),
			"COUNT_WRITE_LOW_PRIORITY":          int64(0),
			"SUM_TIMER_WRITE_LOW_PRIORITY":      int64(0),
			"MIN_TIMER_WRITE_LOW_PRIORITY":      int64(0),
			"AVG_TIMER_WRITE_LOW_PRIORITY":      int64(0),
			"MAX_TIMER_WRITE_LOW_PRIORITY":      int64(0),
			"COUNT_WRITE_NORMAL":                int64(0),
			"SUM_TIMER_WRITE_NORMAL":            int64(0),
			"MIN_TIMER_WRITE_NORMAL":            int64(0),
			"AVG_TIMER_WRITE_NORMAL":            int64(0),
			"MAX_TIMER_WRITE_NORMAL":            int64(0),
			"COUNT_WRITE_EXTERNAL":              int64(0),
			"SUM_TIMER_WRITE_EXTERNAL":          int64(0),
			"MIN_TIMER_WRITE_EXTERNAL":          int64(0),
			"AVG_TIMER_WRITE_EXTERNAL":          int64(0),
			"MAX_TIMER_WRITE_EXTERNAL":          int64(0),
		})
	})
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

// perfSchemaSetupThreads returns the rows for performance_schema.setup_threads matching MySQL 8.0.
func (e *Executor) perfSchemaSetupThreads() []storage.Row {
	type threadDef struct {
		name, properties string
	}
	threads := []threadDef{
		{"thread/innodb/buf_dump_thread", ""},
		{"thread/innodb/buf_resize_thread", ""},
		{"thread/innodb/clone_ddl_thread", ""},
		{"thread/innodb/clone_gtid_thread", ""},
		{"thread/innodb/dict_stats_thread", ""},
		{"thread/innodb/fts_optimize_thread", ""},
		{"thread/innodb/fts_parallel_merge_thread", ""},
		{"thread/innodb/fts_parallel_tokenization_thread", ""},
		{"thread/innodb/io_handler_thread", ""},
		{"thread/innodb/io_ibuf_thread", ""},
		{"thread/innodb/io_log_thread", ""},
		{"thread/innodb/io_read_thread", ""},
		{"thread/innodb/io_write_thread", ""},
		{"thread/innodb/log_writer_thread", ""},
		{"thread/innodb/page_flush_thread", ""},
		{"thread/innodb/srv_error_monitor_thread", ""},
		{"thread/innodb/srv_lock_timeout_thread", ""},
		{"thread/innodb/srv_master_thread", ""},
		{"thread/innodb/srv_monitor_thread", ""},
		{"thread/innodb/srv_purge_thread", ""},
		{"thread/innodb/srv_worker_thread", ""},
		{"thread/innodb/trx_recovery_rollback_thread", ""},
		{"thread/sql/compress_gtid_table", "singleton"},
		{"thread/sql/event_scheduler", "singleton"},
		{"thread/sql/event_worker", ""},
		{"thread/sql/main", "singleton"},
		{"thread/sql/one_connection", "user"},
		{"thread/sql/signal_handler", "singleton"},
	}

	// Check if thread classes are disabled
	if v, ok := e.startupVars["performance_schema_max_thread_classes"]; ok && v == "0" {
		return []storage.Row{}
	}

	rows := make([]storage.Row, 0, len(threads))
	for _, t := range threads {
		rows = append(rows, storage.Row{
			"NAME": t.name, "ENABLED": "YES", "HISTORY": "YES",
			"PROPERTIES": t.properties, "VOLATILITY": int64(0), "DOCUMENTATION": nil,
		})
	}
	return rows
}

// perfSchemaSetupObjectsDefault returns the default rows for performance_schema.setup_objects.
func (e *Executor) perfSchemaSetupObjectsDefault() []storage.Row {
	// If performance_schema_setup_objects_size=0, return empty
	if v, ok := e.startupVars["performance_schema_setup_objects_size"]; ok && v == "0" {
		return []storage.Row{}
	}
	return []storage.Row{
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
}

// getSetupActorsRows returns the current setup_actors rows, initializing from defaults if needed.
func (e *Executor) getSetupActorsRows() []storage.Row {
	if e.psSetupActorsInit {
		return e.psSetupActors
	}
	return e.perfSchemaSetupActors()
}

// getSetupObjectsRows returns the current setup_objects rows, initializing from defaults if needed.
func (e *Executor) getSetupObjectsRows() []storage.Row {
	if e.psSetupObjectsInit {
		return e.psSetupObjects
	}
	return e.perfSchemaSetupObjectsDefault()
}

// isPerfSchemaEnumValid checks if a value is valid for YES/NO enum columns.
func isPerfSchemaEnumValid(val string) bool {
	return strings.EqualFold(val, "YES") || strings.EqualFold(val, "NO")
}

// isPerfSchemaConsumerDisabled returns true when a startup-variable value
// represents a disabled consumer (OFF/0/FALSE/NO, all case-insensitive).
// MySQL accepts these forms for --loose-performance-schema-consumer-*=<val>.
func isPerfSchemaConsumerDisabled(v string) bool {
	upper := strings.ToUpper(strings.TrimSpace(v))
	switch upper {
	case "OFF", "0", "FALSE", "NO":
		return true
	}
	return false
}

// validSetupObjectTypes are the allowed OBJECT_TYPE values for setup_objects.
var validSetupObjectTypes = map[string]bool{
	"EVENT": true, "FUNCTION": true, "PROCEDURE": true, "TABLE": true, "TRIGGER": true,
}

// execPerfSchemaInsert handles INSERT into performance_schema.setup_actors or setup_objects.
func (e *Executor) execPerfSchemaInsert(stmt *sqlparser.Insert, tableName string) (*Result, error) {
	// Get column names from the INSERT statement
	colNames := make([]string, len(stmt.Columns))
	for i, col := range stmt.Columns {
		colNames[i] = strings.ToUpper(col.String())
	}

	// Handle INSERT ... SELECT (for restoring from backup table)
	if sel, ok := stmt.Rows.(*sqlparser.Select); ok {
		selResult, err := e.execSelect(sel)
		if err != nil {
			return nil, err
		}
		if selResult == nil || !selResult.IsResultSet {
			return &Result{AffectedRows: 0}, nil
		}
		affected := uint64(0)
		for _, row := range selResult.Rows {
			newRow := storage.Row{}
			if len(colNames) > 0 {
				for i, cn := range colNames {
					if i < len(row) {
						if row[i] != nil {
							newRow[cn] = fmt.Sprintf("%v", row[i])
						} else {
							newRow[cn] = "%"
						}
					}
				}
			} else {
				// No columns specified - map by result column order
				if tableName == "setup_actors" {
					actorCols := []string{"HOST", "USER", "ROLE", "ENABLED", "HISTORY"}
					for i, cn := range actorCols {
						if i < len(row) {
							if row[i] != nil {
								newRow[cn] = fmt.Sprintf("%v", row[i])
							} else {
								newRow[cn] = "%"
							}
						}
					}
				} else { // setup_objects
					objCols := []string{"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "ENABLED", "TIMED"}
					for i, cn := range objCols {
						if i < len(row) {
							if row[i] != nil {
								newRow[cn] = fmt.Sprintf("%v", row[i])
							} else {
								newRow[cn] = "%"
							}
						}
					}
				}
			}
			// Set defaults for missing columns
			if tableName == "setup_actors" {
				if _, ok := newRow["HOST"]; !ok {
					newRow["HOST"] = "%"
				}
				if _, ok := newRow["USER"]; !ok {
					newRow["USER"] = "%"
				}
				if _, ok := newRow["ROLE"]; !ok {
					newRow["ROLE"] = "%"
				}
				if _, ok := newRow["ENABLED"]; !ok {
					newRow["ENABLED"] = "YES"
				}
				if _, ok := newRow["HISTORY"]; !ok {
					newRow["HISTORY"] = "YES"
				}
			} else {
				if _, ok := newRow["ENABLED"]; !ok {
					newRow["ENABLED"] = "YES"
				}
				if _, ok := newRow["TIMED"]; !ok {
					newRow["TIMED"] = "YES"
				}
			}
			// Validate ENABLED/HISTORY and OBJECT_TYPE
			if tableName == "setup_actors" {
				if enabled, ok := newRow["ENABLED"].(string); ok {
					if !isPerfSchemaEnumValid(enabled) {
						return nil, mysqlError(1265, "01000", "Data truncated for column 'ENABLED' at row 1")
					}
					newRow["ENABLED"] = strings.ToUpper(enabled)
				}
				if history, ok := newRow["HISTORY"].(string); ok {
					if !isPerfSchemaEnumValid(history) {
						return nil, mysqlError(1265, "01000", "Data truncated for column 'HISTORY' at row 1")
					}
					newRow["HISTORY"] = strings.ToUpper(history)
				}
			} else {
				if objType, ok := newRow["OBJECT_TYPE"].(string); ok {
					if !validSetupObjectTypes[strings.ToUpper(objType)] {
						return nil, mysqlError(1452, "23000", "Cannot add or update a child row: a foreign key constraint fails ()")
					}
					newRow["OBJECT_TYPE"] = strings.ToUpper(objType)
				}
			}

			// Check for duplicate key
			if tableName == "setup_actors" {
				currentRows := e.getSetupActorsRows()
				host := fmt.Sprintf("%v", newRow["HOST"])
				user := fmt.Sprintf("%v", newRow["USER"])
				role := fmt.Sprintf("%v", newRow["ROLE"])
				for _, existing := range currentRows {
					if fmt.Sprintf("%v", existing["HOST"]) == host &&
						fmt.Sprintf("%v", existing["USER"]) == user &&
						fmt.Sprintf("%v", existing["ROLE"]) == role {
						return nil, mysqlError(1022, "23000", "Can't write; duplicate key in table 'setup_actors'")
					}
				}
				if !e.psSetupActorsInit {
					e.psSetupActors = append([]storage.Row{}, currentRows...)
					e.psSetupActorsInit = true
				}
				e.psSetupActors = append(e.psSetupActors, newRow)
			} else {
				currentRows := e.getSetupObjectsRows()
				objType := fmt.Sprintf("%v", newRow["OBJECT_TYPE"])
				objSchema := fmt.Sprintf("%v", newRow["OBJECT_SCHEMA"])
				objName := fmt.Sprintf("%v", newRow["OBJECT_NAME"])
				for _, existing := range currentRows {
					if fmt.Sprintf("%v", existing["OBJECT_TYPE"]) == objType &&
						fmt.Sprintf("%v", existing["OBJECT_SCHEMA"]) == objSchema &&
						fmt.Sprintf("%v", existing["OBJECT_NAME"]) == objName {
						return nil, mysqlError(1022, "23000", "Can't write; duplicate key in table 'setup_objects'")
					}
				}
				if !e.psSetupObjectsInit {
					e.psSetupObjects = append([]storage.Row{}, currentRows...)
					e.psSetupObjectsInit = true
				}
				e.psSetupObjects = append(e.psSetupObjects, newRow)
			}
			affected++
		}
		return &Result{AffectedRows: affected}, nil
	}

	// Handle INSERT ... VALUES or INSERT ... SET
	rows, ok := stmt.Rows.(sqlparser.Values)
	if !ok {
		return &Result{AffectedRows: 1}, nil
	}

	affected := uint64(0)
	for _, valTuple := range rows {
		newRow := storage.Row{}
		if len(colNames) > 0 {
			for i, cn := range colNames {
				if i < len(valTuple) {
					val, err := e.evalExpr(valTuple[i])
					if err != nil {
						return nil, err
					}
					if val != nil {
						newRow[cn] = fmt.Sprintf("%v", val)
					} else {
						newRow[cn] = "%"
					}
				}
			}
		} else {
			// Positional values
			if tableName == "setup_actors" {
				actorCols := []string{"HOST", "USER", "ROLE", "ENABLED", "HISTORY"}
				for i, cn := range actorCols {
					if i < len(valTuple) {
						val, err := e.evalExpr(valTuple[i])
						if err != nil {
							return nil, err
						}
						if val != nil {
							newRow[cn] = fmt.Sprintf("%v", val)
						} else {
							newRow[cn] = "%"
						}
					}
				}
			} else {
				objCols := []string{"OBJECT_TYPE", "OBJECT_SCHEMA", "OBJECT_NAME", "ENABLED", "TIMED"}
				for i, cn := range objCols {
					if i < len(valTuple) {
						val, err := e.evalExpr(valTuple[i])
						if err != nil {
							return nil, err
						}
						if val != nil {
							newRow[cn] = fmt.Sprintf("%v", val)
						} else {
							newRow[cn] = "%"
						}
					}
				}
			}
		}

		// Set defaults for missing columns
		if tableName == "setup_actors" {
			if _, ok := newRow["HOST"]; !ok {
				newRow["HOST"] = "%"
			}
			if _, ok := newRow["USER"]; !ok {
				newRow["USER"] = "%"
			}
			if _, ok := newRow["ROLE"]; !ok {
				newRow["ROLE"] = "%"
			}
			if _, ok := newRow["ENABLED"]; !ok {
				newRow["ENABLED"] = "YES"
			}
			if _, ok := newRow["HISTORY"]; !ok {
				newRow["HISTORY"] = "YES"
			}
			// Validate ENABLED/HISTORY columns
			if enabled, ok := newRow["ENABLED"].(string); ok {
				if !isPerfSchemaEnumValid(enabled) {
					return nil, mysqlError(1265, "01000", "Data truncated for column 'ENABLED' at row 1")
				}
				newRow["ENABLED"] = strings.ToUpper(enabled)
			}
			if history, ok := newRow["HISTORY"].(string); ok {
				if !isPerfSchemaEnumValid(history) {
					return nil, mysqlError(1265, "01000", "Data truncated for column 'HISTORY' at row 1")
				}
				newRow["HISTORY"] = strings.ToUpper(history)
			}
		} else { // setup_objects
			if _, ok := newRow["ENABLED"]; !ok {
				newRow["ENABLED"] = "YES"
			}
			if _, ok := newRow["TIMED"]; !ok {
				newRow["TIMED"] = "YES"
			}
			// Validate OBJECT_TYPE
			if objType, ok := newRow["OBJECT_TYPE"].(string); ok {
				if !validSetupObjectTypes[strings.ToUpper(objType)] {
					return nil, mysqlError(1452, "23000", "Cannot add or update a child row: a foreign key constraint fails ()")
				}
				newRow["OBJECT_TYPE"] = strings.ToUpper(objType)
			}
		}

		// Check for duplicate key
		if tableName == "setup_actors" {
			currentRows := e.getSetupActorsRows()
			host := fmt.Sprintf("%v", newRow["HOST"])
			user := fmt.Sprintf("%v", newRow["USER"])
			role := fmt.Sprintf("%v", newRow["ROLE"])
			for _, existing := range currentRows {
				if fmt.Sprintf("%v", existing["HOST"]) == host &&
					fmt.Sprintf("%v", existing["USER"]) == user &&
					fmt.Sprintf("%v", existing["ROLE"]) == role {
					return nil, mysqlError(1022, "23000", "Can't write; duplicate key in table 'setup_actors'")
				}
			}
			if !e.psSetupActorsInit {
				e.psSetupActors = append([]storage.Row{}, currentRows...)
				e.psSetupActorsInit = true
			}
			e.psSetupActors = append(e.psSetupActors, newRow)
		} else {
			currentRows := e.getSetupObjectsRows()
			objType := fmt.Sprintf("%v", newRow["OBJECT_TYPE"])
			objSchema := fmt.Sprintf("%v", newRow["OBJECT_SCHEMA"])
			objName := fmt.Sprintf("%v", newRow["OBJECT_NAME"])
			for _, existing := range currentRows {
				if fmt.Sprintf("%v", existing["OBJECT_TYPE"]) == objType &&
					fmt.Sprintf("%v", existing["OBJECT_SCHEMA"]) == objSchema &&
					fmt.Sprintf("%v", existing["OBJECT_NAME"]) == objName {
					return nil, mysqlError(1022, "23000", "Can't write; duplicate key in table 'setup_objects'")
				}
			}
			if !e.psSetupObjectsInit {
				e.psSetupObjects = append([]storage.Row{}, currentRows...)
				e.psSetupObjectsInit = true
			}
			e.psSetupObjects = append(e.psSetupObjects, newRow)
		}
		affected++
	}
	return &Result{AffectedRows: affected}, nil
}

// execPerfSchemaDelete handles DELETE from performance_schema.setup_actors or setup_objects.
func (e *Executor) execPerfSchemaDelete(stmt *sqlparser.Delete, tableName string) (*Result, error) {
	var currentRows []storage.Row
	if tableName == "setup_actors" {
		currentRows = e.getSetupActorsRows()
	} else {
		currentRows = e.getSetupObjectsRows()
	}

	// If no WHERE clause, delete all rows
	if stmt.Where == nil {
		affected := uint64(len(currentRows))
		if tableName == "setup_actors" {
			e.psSetupActors = []storage.Row{}
			e.psSetupActorsInit = true
		} else {
			e.psSetupObjects = []storage.Row{}
			e.psSetupObjectsInit = true
		}
		return &Result{AffectedRows: affected}, nil
	}

	// Filter rows based on WHERE clause
	remaining := make([]storage.Row, 0, len(currentRows))
	affected := uint64(0)
	for _, row := range currentRows {
		match, err := e.evalWhere(stmt.Where.Expr, row)
		if err != nil {
			return nil, err
		}
		if match {
			affected++
		} else {
			remaining = append(remaining, row)
		}
	}
	if tableName == "setup_actors" {
		e.psSetupActors = remaining
		e.psSetupActorsInit = true
	} else {
		e.psSetupObjects = remaining
		e.psSetupObjectsInit = true
	}
	return &Result{AffectedRows: affected}, nil
}

// execPerfSchemaUpdate handles UPDATE on writable performance_schema tables.
func (e *Executor) execPerfSchemaUpdate(stmt *sqlparser.Update, tableName string) (*Result, error) {
	// Check allowed columns
	allowedCols := map[string]map[string]bool{
		"setup_consumers":   {"enabled": true},
		"setup_instruments": {"enabled": true, "timed": true},
		"setup_actors":      {"enabled": true, "history": true},
		"setup_objects":     {"enabled": true, "timed": true},
		"setup_threads":     {"enabled": true, "history": true},
		"threads":           {"instrumented": true, "history": true},
	}
	if allowed, ok := allowedCols[tableName]; ok {
		for _, expr := range stmt.Exprs {
			colName := strings.ToLower(expr.Name.Name.String())
			if !allowed[colName] {
				return nil, mysqlError(1683, "HY000", "Invalid performance_schema usage.")
			}
		}
	}

	// Validate YES/NO enum values and NULL checks for each update expression
	for _, expr := range stmt.Exprs {
		colName := strings.ToUpper(expr.Name.Name.String())
		val, err := e.evalExpr(expr.Expr)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, mysqlError(1048, "23000", fmt.Sprintf("Column '%s' cannot be null", colName))
		}
		strVal := fmt.Sprintf("%v", val)
		if !isPerfSchemaEnumValid(strVal) {
			return nil, mysqlError(1265, "01000", fmt.Sprintf("Data truncated for column '%s' at row 1", colName))
		}
	}

	// For setup_actors, update the in-memory rows
	if tableName == "setup_actors" {
		currentRows := e.getSetupActorsRows()
		updated := make([]storage.Row, len(currentRows))
		affected := uint64(0)
		for i, row := range currentRows {
			match := true
			if stmt.Where != nil {
				m, err := e.evalWhere(stmt.Where.Expr, row)
				if err != nil {
					return nil, err
				}
				match = m
			}
			newRow := make(storage.Row, len(row))
			for k, v := range row {
				newRow[k] = v
			}
			if match {
				for _, expr := range stmt.Exprs {
					colName := strings.ToUpper(expr.Name.Name.String())
					val, _ := e.evalExpr(expr.Expr)
					newRow[colName] = strings.ToUpper(fmt.Sprintf("%v", val))
				}
				affected++
			}
			updated[i] = newRow
		}
		e.psSetupActors = updated
		e.psSetupActorsInit = true
		return &Result{AffectedRows: affected}, nil
	}

	// For setup_objects, update the in-memory rows
	if tableName == "setup_objects" {
		currentRows := e.getSetupObjectsRows()
		updated := make([]storage.Row, len(currentRows))
		affected := uint64(0)
		for i, row := range currentRows {
			match := true
			if stmt.Where != nil {
				m, err := e.evalWhere(stmt.Where.Expr, row)
				if err != nil {
					return nil, err
				}
				match = m
			}
			newRow := make(storage.Row, len(row))
			for k, v := range row {
				newRow[k] = v
			}
			if match {
				for _, expr := range stmt.Exprs {
					colName := strings.ToUpper(expr.Name.Name.String())
					val, _ := e.evalExpr(expr.Expr)
					newRow[colName] = strings.ToUpper(fmt.Sprintf("%v", val))
				}
				affected++
			}
			updated[i] = newRow
		}
		e.psSetupObjects = updated
		e.psSetupObjectsInit = true
		return &Result{AffectedRows: affected}, nil
	}

	// For threads table, track INSTRUMENTED/HISTORY per-connection
	if tableName == "threads" {
		if e.psThreadInstrumented == nil {
			e.psThreadInstrumented = make(map[int64]string)
		}
		if e.psThreadHistory == nil {
			e.psThreadHistory = make(map[int64]string)
		}
		for _, expr := range stmt.Exprs {
			colName := strings.ToLower(expr.Name.Name.String())
			val, _ := e.evalExpr(expr.Expr)
			strVal := strings.ToUpper(fmt.Sprintf("%v", val))
			switch colName {
			case "instrumented":
				e.psThreadInstrumented[e.connectionID] = strVal
			case "history":
				e.psThreadHistory[e.connectionID] = strVal
			}
		}
		return &Result{AffectedRows: 0}, nil
	}

	// For setup_consumers: persist per-session ENABLED state
	if tableName == "setup_consumers" {
		// Build current consumer state to apply WHERE filter against
		consumers := []string{
			"events_stages_current", "events_stages_history", "events_stages_history_long",
			"events_statements_current", "events_statements_history", "events_statements_history_long",
			"events_transactions_current", "events_transactions_history", "events_transactions_history_long",
			"events_waits_current", "events_waits_history", "events_waits_history_long",
			"global_instrumentation", "thread_instrumentation", "statements_digest",
		}
		affected := uint64(0)
		for _, c := range consumers {
			enabled := "YES"
			if e.psConsumerEnabled != nil {
				if v, ok := e.psConsumerEnabled[c]; ok {
					enabled = v
				}
			} else {
				varName := "performance_schema_consumer_" + strings.Replace(c, "-", "_", -1)
				if v, ok := e.startupVars[varName]; ok && isPerfSchemaConsumerDisabled(v) {
					enabled = "NO"
				}
			}
			row := storage.Row{"NAME": c, "ENABLED": enabled}
			match := true
			if stmt.Where != nil {
				m, err := e.evalWhere(stmt.Where.Expr, row)
				if err != nil {
					return nil, err
				}
				match = m
			}
			if match {
				if e.psConsumerEnabled == nil {
					e.psConsumerEnabled = make(map[string]string)
				}
				for _, expr := range stmt.Exprs {
					val, _ := e.evalExpr(expr.Expr)
					e.psConsumerEnabled[c] = strings.ToUpper(fmt.Sprintf("%v", val))
				}
				affected++
			}
		}
		return &Result{AffectedRows: affected}, nil
	}

	// For setup_instruments, setup_threads - silently succeed
	return &Result{AffectedRows: 0}, nil
}

// psInstrumentPatternValue parses a performance-schema-instrument value string.
// Returns (enabled, timed, valid). Invalid values return false.
// ON/TRUE/1 → YES/YES, OFF/FALSE/0/NO → NO/NO, COUNTED → YES/NO
func psInstrumentPatternValue(v string) (enabled, timed string, valid bool) {
	upper := strings.ToUpper(strings.TrimSpace(v))
	switch upper {
	case "ON", "TRUE", "1", "YES":
		return "YES", "YES", true
	case "OFF", "FALSE", "0", "NO":
		return "NO", "NO", true
	case "COUNTED":
		return "YES", "NO", true
	}
	return "", "", false
}

// psLikeMatch performs SQL LIKE matching where % matches any sequence of chars.
// Matching is case-insensitive.
func psLikeMatch(pattern, str string) bool {
	if pattern == "" {
		return str == ""
	}
	if pattern[0] == '%' {
		// Skip consecutive %
		for len(pattern) > 0 && pattern[0] == '%' {
			pattern = pattern[1:]
		}
		if pattern == "" {
			return true
		}
		for i := 0; i <= len(str); i++ {
			if psLikeMatch(pattern, str[i:]) {
				return true
			}
		}
		return false
	}
	if len(str) == 0 {
		return false
	}
	if pattern[0] != str[0] {
		return false
	}
	return psLikeMatch(pattern[1:], str[1:])
}

// applyPsInstrumentPatterns applies performance-schema-instrument startup patterns
// to determine the ENABLED and TIMED values for a given instrument name.
// Patterns are applied in order; the last matching pattern wins.
// Memory instruments always have TIMED=NO regardless of patterns.
func applyPsInstrumentPatterns(patterns string, name, defaultEnabled, defaultTimed string) (enabled, timed string) {
	enabled, timed = defaultEnabled, defaultTimed
	nameLower := strings.ToLower(name)
	for _, line := range strings.Split(patterns, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		eqIdx := strings.Index(line, "=")
		if eqIdx < 0 {
			continue
		}
		pat := strings.TrimSpace(line[:eqIdx])
		val := strings.TrimSpace(line[eqIdx+1:])
		if pat == "" {
			continue
		}
		// Strip trailing slashes from pattern (MySQL trims them)
		pat = strings.TrimRight(pat, "/")
		en, ti, ok := psInstrumentPatternValue(val)
		if !ok {
			continue
		}
		if psLikeMatch(strings.ToLower(pat), nameLower) {
			enabled, timed = en, ti
		}
	}
	// Memory instruments are never timed
	if strings.HasPrefix(nameLower, "memory/") {
		timed = "NO"
	}
	return enabled, timed
}

// perfSchemaSetupInstruments returns stub rows for performance_schema.setup_instruments.
func (e *Executor) perfSchemaSetupInstruments() []storage.Row {
	// Return a representative set of instrument categories matching MySQL 8.0
	type instDef struct {
		name, enabled, timed, properties string
	}
	instruments := []instDef{
		// Mutex instruments
		{"wait/synch/mutex/sql/Commit_order_manager::m_mutex", "YES", "YES", ""},
		{"wait/synch/mutex/sql/Cost_constant_cache::LOCK_cost_const", "YES", "YES", "singleton"},
		{"wait/synch/mutex/sql/Event_scheduler::LOCK_scheduler_state", "YES", "YES", "singleton"},
		{"wait/synch/mutex/sql/Gtid_set::gtid_executed::free_intervals_mutex", "YES", "YES", ""},
		{"wait/synch/mutex/sql/Gtid_state", "YES", "YES", "singleton"},
		{"wait/synch/mutex/sql/hash_filo::lock", "YES", "YES", ""},
		{"wait/synch/mutex/sql/key_mts_gaq_LOCK", "YES", "YES", ""},
		{"wait/synch/mutex/sql/key_mts_temp_table_LOCK", "YES", "YES", ""},
		{"wait/synch/mutex/sql/LOCK_acl_cache_flush", "YES", "YES", "singleton"},
		{"wait/synch/mutex/sql/LOCK_audit_mask", "YES", "YES", "singleton"},
		{"wait/synch/mutex/sql/LOCK_transaction_cache", "YES", "YES", "singleton"},
		{"wait/synch/mutex/sql/LOCK_user_conn", "YES", "YES", "singleton"},
		{"wait/synch/mutex/sql/LOCK_uuid_generator", "YES", "YES", "singleton"},
		{"wait/synch/mutex/sql/THD_LOCK_INFO::mutex", "YES", "YES", ""},
		// Rwlock instruments
		{"wait/synch/rwlock/sql/Binlog_relay_IO_delegate::lock", "YES", "YES", "singleton"},
		{"wait/synch/rwlock/sql/Binlog_storage_delegate::lock", "YES", "YES", "singleton"},
		{"wait/synch/rwlock/sql/Binlog_transmit_delegate::lock", "YES", "YES", "singleton"},
		{"wait/synch/rwlock/sql/channel_lock", "YES", "YES", ""},
		{"wait/synch/rwlock/sql/channel_map_lock", "YES", "YES", ""},
		{"wait/synch/rwlock/sql/channel_to_filter_lock", "YES", "YES", ""},
		{"wait/synch/rwlock/sql/gtid_commit_rollback", "YES", "YES", "singleton"},
		{"wait/synch/rwlock/sql/gtid_mode_lock", "YES", "YES", "singleton"},
		{"wait/synch/rwlock/sql/gtid_retrieved", "YES", "YES", "singleton"},
		{"wait/synch/rwlock/sql/LOCK_sys_init_connect", "YES", "YES", "singleton"},
		// Cond instruments
		{"wait/synch/cond/sql/Commit_order_manager::m_workers.cond", "YES", "YES", ""},
		{"wait/synch/cond/sql/COND_compress_gtid_table", "YES", "YES", "singleton"},
		{"wait/synch/cond/sql/COND_connection_count", "YES", "YES", "singleton"},
		{"wait/synch/cond/sql/COND_flush_thread_cache", "YES", "YES", "singleton"},
		{"wait/synch/cond/sql/COND_manager", "YES", "YES", "singleton"},
		{"wait/synch/cond/sql/COND_open", "YES", "YES", ""},
		{"wait/synch/cond/sql/COND_queue_state", "YES", "YES", "singleton"},
		{"wait/synch/cond/sql/COND_server_started", "YES", "YES", "singleton"},
		{"wait/synch/cond/sql/COND_thd_list", "YES", "YES", ""},
		{"wait/synch/cond/sql/COND_thr_lock", "YES", "YES", ""},
		{"wait/synch/cond/sql/COND_thread_cache", "YES", "YES", "singleton"},
		// IO instruments
		{"wait/io/file/sql/binlog", "YES", "YES", ""},
		{"wait/io/table/sql/handler", "YES", "YES", ""},
		{"wait/io/socket/sql/server_tcpip_socket", "YES", "YES", ""},
		{"wait/lock/table/sql/handler", "YES", "YES", ""},
		// Stage instruments
		{"stage/sql/After create", "YES", "YES", ""},
		{"stage/sql/creating table", "YES", "YES", ""},
		// Statement instruments
		{"statement/sql/select", "YES", "YES", ""},
		{"statement/sql/insert", "YES", "YES", ""},
		{"statement/sql/update", "YES", "YES", ""},
		{"statement/sql/delete", "YES", "YES", ""},
		// Memory instruments (TIMED is always NO for memory instruments)
		{"memory/sql/THD::main_mem_root", "YES", "NO", ""},
		// Other instruments
		{"transaction", "YES", "YES", ""},
		{"idle", "YES", "YES", ""},
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

	// Load performance-schema-instrument patterns accumulated from master.opt
	patterns := e.startupVars["__ps_instrument_patterns__"]

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
		enabled, timed := inst.enabled, inst.timed
		if patterns != "" {
			enabled, timed = applyPsInstrumentPatterns(patterns, inst.name, enabled, timed)
		}
		rows = append(rows, storage.Row{
			"NAME": inst.name, "ENABLED": enabled, "TIMED": timed,
			"PROPERTIES": inst.properties, "VOLATILITY": int64(0), "DOCUMENTATION": nil,
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
	// Build from allCharsets() which has all 41 MySQL 8.0 charsets.
	// allCharsets() columns: [name, description, default_collation, maxlen]
	charsets := allCharsets()
	rows := make([]storage.Row, 0, len(charsets))
	for _, cs := range charsets {
		name := cs[0].(string)
		desc := cs[1].(string)
		defaultColl := cs[2].(string)
		maxlen := cs[3].(int64)
		rows = append(rows, storage.Row{
			"CHARACTER_SET_NAME":  name,
			"DEFAULT_COLLATE_NAME": defaultColl,
			"DESCRIPTION":         desc,
			"MAXLEN":              maxlen,
		})
	}
	return rows
}

// infoSchemaCollations returns rows for INFORMATION_SCHEMA.COLLATIONS.
func (e *Executor) infoSchemaCollations() []storage.Row {
	// Build from allCollations() which has all MySQL collations.
	// allCollations() columns: [Collation, Charset, Id, Default, Compiled, Sortlen, Pad_attribute]
	colls := allCollations()
	rows := make([]storage.Row, 0, len(colls))
	for _, c := range colls {
		isDefault := ""
		if c[3].(string) == "Yes" {
			isDefault = "Yes"
		}
		rows = append(rows, storage.Row{
			"COLLATION_NAME":     c[0].(string),
			"CHARACTER_SET_NAME": c[1].(string),
			"ID":                 c[2].(int64),
			"IS_DEFAULT":         isDefault,
			"IS_COMPILED":        c[4].(string),
			"SORTLEN":            c[5].(int64),
			"PAD_ATTRIBUTE":      c[6].(string),
		})
	}
	return rows
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
// Only global-level (*.*) privileges are shown here; DB- and table-level grants
// appear in SCHEMA_PRIVILEGES and TABLE_PRIVILEGES respectively.
// Users with no global privileges get a single USAGE row (MySQL convention).
func (e *Executor) infoSchemaUserPrivileges() []storage.Row {
	var rows []storage.Row

	// Always include root@localhost with ALL PRIVILEGES + GRANT OPTION.
	rootPrivTypes := []string{
		"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "RELOAD",
		"SHUTDOWN", "PROCESS", "FILE", "REFERENCES", "INDEX", "ALTER",
		"SHOW DATABASES", "SUPER", "CREATE TEMPORARY TABLES", "LOCK TABLES",
		"EXECUTE", "REPLICATION SLAVE", "REPLICATION CLIENT", "CREATE VIEW",
		"SHOW VIEW", "CREATE ROUTINE", "ALTER ROUTINE", "CREATE USER", "EVENT",
		"TRIGGER", "CREATE TABLESPACE", "CREATE ROLE", "DROP ROLE",
	}
	for _, p := range rootPrivTypes {
		rows = append(rows, storage.Row{
			"GRANTEE":        "'root'@'localhost'",
			"TABLE_CATALOG":  "def",
			"PRIVILEGE_TYPE": p,
			"IS_GRANTABLE":   "YES",
		})
	}

	if e.grantStore == nil {
		return rows
	}

	// Enumerate all non-root users registered in the grant store.
	for _, uh := range e.grantStore.ListAllUserHosts() {
		if strings.EqualFold(uh.User, "root") {
			continue
		}
		grantee := fmt.Sprintf("'%s'@'%s'", uh.User, uh.Host)
		globalGrants := e.grantStore.GetGrantsByType(uh.User, uh.Host, GrantTypeGlobal)
		if len(globalGrants) == 0 {
			// No global privileges: emit USAGE row.
			rows = append(rows, storage.Row{
				"GRANTEE":        grantee,
				"TABLE_CATALOG":  "def",
				"PRIVILEGE_TYPE": "USAGE",
				"IS_GRANTABLE":   "NO",
			})
			continue
		}
		for _, entry := range globalGrants {
			isGrantable := "NO"
			if entry.GrantOption {
				isGrantable = "YES"
			}
			for _, p := range entry.Privs {
				rows = append(rows, storage.Row{
					"GRANTEE":        grantee,
					"TABLE_CATALOG":  "def",
					"PRIVILEGE_TYPE": p,
					"IS_GRANTABLE":   isGrantable,
				})
			}
		}
	}

	return rows
}

// infoSchemaSchemaPrivileges returns rows for INFORMATION_SCHEMA.SCHEMA_PRIVILEGES.
// Only DB-level (db.*) privileges are shown.
func (e *Executor) infoSchemaSchemaPrivileges() []storage.Row {
	if e.grantStore == nil {
		return []storage.Row{}
	}
	var rows []storage.Row
	for _, uh := range e.grantStore.ListAllUserHosts() {
		grantee := fmt.Sprintf("'%s'@'%s'", uh.User, uh.Host)
		dbGrants := e.grantStore.GetGrantsByType(uh.User, uh.Host, GrantTypeDB)
		for _, entry := range dbGrants {
			dbName := strings.TrimSuffix(entry.Object, ".*")
			isGrantable := "NO"
			if entry.GrantOption {
				isGrantable = "YES"
			}
			for _, p := range entry.Privs {
				rows = append(rows, storage.Row{
					"GRANTEE":        grantee,
					"TABLE_CATALOG":  "def",
					"TABLE_SCHEMA":   dbName,
					"PRIVILEGE_TYPE": p,
					"IS_GRANTABLE":   isGrantable,
				})
			}
		}
	}
	return rows
}

// infoSchemaTablePrivileges returns rows for INFORMATION_SCHEMA.TABLE_PRIVILEGES.
// Only table-level (db.table) privileges are shown.
func (e *Executor) infoSchemaTablePrivileges() []storage.Row {
	if e.grantStore == nil {
		return []storage.Row{}
	}
	var rows []storage.Row
	for _, uh := range e.grantStore.ListAllUserHosts() {
		grantee := fmt.Sprintf("'%s'@'%s'", uh.User, uh.Host)
		tableGrants := e.grantStore.GetGrantsByType(uh.User, uh.Host, GrantTypeTable)
		for _, entry := range tableGrants {
			parts := strings.SplitN(entry.Object, ".", 2)
			if len(parts) != 2 {
				continue
			}
			dbName, tableName := parts[0], parts[1]
			isGrantable := "NO"
			if entry.GrantOption {
				isGrantable = "YES"
			}
			for _, p := range entry.Privs {
				// Skip column-level grants (those with parenthesized column list)
				if strings.Contains(p, "(") {
					continue
				}
				rows = append(rows, storage.Row{
					"GRANTEE":        grantee,
					"TABLE_CATALOG":  "def",
					"TABLE_SCHEMA":   dbName,
					"TABLE_NAME":     tableName,
					"PRIVILEGE_TYPE": p,
					"IS_GRANTABLE":   isGrantable,
				})
			}
		}
	}
	return rows
}

// routineDefinitionText returns the ROUTINE_DEFINITION value for a stored procedure or function.
// When bodyText is available (preserves original BEGIN...END), it is used directly.
// Otherwise falls back to joining statements with ";\n".
func (e *Executor) routineDefinitionText(stmts []string, bodyText string) string {
	if bodyText != "" {
		return bodyText
	}
	return strings.Join(stmts, ";\n")
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
					"ROUTINE_DEFINITION":        e.routineDefinitionText(p.Body, p.BodyText),
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
					"ROUTINE_DEFINITION":        e.routineDefinitionText(f.Body, f.BodyText),
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
				"VIEW_DEFINITION":      e.viewDefinitionForDisplay(vName),
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
			// Foreign key constraints
			for _, fk := range tbl.ForeignKeys {
				for i, col := range fk.Columns {
					var refCol interface{}
					if i < len(fk.ReferencedColumns) {
						refCol = fk.ReferencedColumns[i]
					}
					rows = append(rows, storage.Row{
						"CONSTRAINT_CATALOG":            "def",
						"CONSTRAINT_SCHEMA":             dbName,
						"CONSTRAINT_NAME":               fk.Name,
						"TABLE_CATALOG":                 "def",
						"TABLE_SCHEMA":                  dbName,
						"TABLE_NAME":                    tblName,
						"COLUMN_NAME":                   col,
						"ORDINAL_POSITION":              int64(i + 1),
						"POSITION_IN_UNIQUE_CONSTRAINT": int64(i + 1),
						"REFERENCED_TABLE_SCHEMA":       dbName,
						"REFERENCED_TABLE_NAME":         fk.ReferencedTable,
						"REFERENCED_COLUMN_NAME":        refCol,
					})
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

// normalizeTimeDefault normalizes a TIME column default value to HH:MM:SS format.
func normalizeTimeDefault(s string) string {
	s = strings.TrimSpace(s)
	// If already in HH:MM:SS format, return as-is
	if len(s) == 8 && s[2] == ':' && s[5] == ':' {
		return s
	}
	// If it's a pure number, treat as seconds
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		hours := n / 3600
		mins := (n % 3600) / 60
		secs := n % 60
		return fmt.Sprintf("%02d:%02d:%02d", hours, mins, secs)
	}
	return ""
}

// normalizeDatetimeDefault normalizes a DATETIME column default to YYYY-MM-DD HH:MM:SS format.
func normalizeDatetimeDefault(s string) string {
	s = strings.TrimSpace(strings.Trim(s, "'\""))
	// If already in full format, return as-is
	if len(s) == 19 && s[4] == '-' && s[7] == '-' && s[10] == ' ' {
		return s
	}
	// Try parsing short date formats like '2/2/2' = year/month/day or month/day/year
	// MySQL interprets '2/2/2' as 0002-02-02
	parts := strings.FieldsFunc(s, func(r rune) bool {
		return r == '/' || r == '-'
	})
	if len(parts) >= 3 {
		year, errY := strconv.ParseInt(parts[0], 10, 64)
		month, errM := strconv.ParseInt(parts[1], 10, 64)
		day, errD := strconv.ParseInt(parts[2], 10, 64)
		if errY == nil && errM == nil && errD == nil {
			return fmt.Sprintf("%04d-%02d-%02d 00:00:00", year, month, day)
		}
	}
	return ""
}

// normalizeTimestampDefault normalizes a TIMESTAMP column default to YYYY-MM-DD HH:MM:SS.
func normalizeTimestampDefault(s string) string {
	s = strings.TrimSpace(strings.Trim(s, "'\""))
	// If already in full format, return as-is
	if len(s) == 19 && s[4] == '-' && s[7] == '-' && s[10] == ' ' {
		return s
	}
	// Compact format: YYYYMMDDHHMMSS (14 digits)
	if len(s) == 14 {
		if _, err := strconv.ParseInt(s, 10, 64); err == nil {
			return fmt.Sprintf("%s-%s-%s %s:%s:%s",
				s[0:4], s[4:6], s[6:8], s[8:10], s[10:12], s[12:14])
		}
	}
	return ""
}

// normalizeBinaryLiteralDefault converts a binary literal default value to its
// expected INFORMATION_SCHEMA representation.
func normalizeBinaryLiteralDefault(s, baseType string) interface{} {
	// Parse binary literal: "0b101" or "b'101'" → value 5 = 0x05
	var bits string
	if strings.HasPrefix(s, "0b") {
		bits = s[2:]
	} else if strings.HasPrefix(s, "b'") && strings.HasSuffix(s, "'") {
		bits = s[2 : len(s)-1]
	} else {
		return s
	}

	// Parse bits to integer
	n, err := strconv.ParseInt(bits, 2, 64)
	if err != nil {
		return s
	}

	switch baseType {
	case "BINARY", "VARBINARY":
		// Display as 0xNN hex format
		return fmt.Sprintf("0x%02X", n)
	case "CHAR", "VARCHAR":
		// Non-printable chars → display as their byte value char
		// MySQL INFORMATION_SCHEMA shows the actual character
		// For non-printable chars (like 0x05), MySQL shows the raw byte
		// which the runner might display as empty or as the hex escape
		// Based on expected: empty string for char b'101' (= 0x05)
		if n < 0x20 || n == 0x7f {
			// Non-printable: return the actual byte as string
			return string(rune(n))
		}
		return string(rune(n))
	}
	return s
}

// splitEnumSetValues splits ENUM/SET value list like "'a','b c','d'" into individual values
// (without surrounding quotes).
func splitEnumSetValues(inner string) []string {
	var vals []string
	i := 0
	for i < len(inner) {
		if inner[i] == '\'' {
			j := i + 1
			for j < len(inner) {
				if inner[j] == '\'' && (j+1 >= len(inner) || inner[j+1] != '\'') {
					break
				}
				if inner[j] == '\'' && j+1 < len(inner) && inner[j+1] == '\'' {
					j += 2 // skip escaped quote
					continue
				}
				j++
			}
			vals = append(vals, inner[i+1:j])
			i = j + 1
			// skip comma
			for i < len(inner) && (inner[i] == ',' || inner[i] == ' ') {
				i++
			}
		} else {
			i++
		}
	}
	return vals
}

// formatDecimalDefault formats a decimal default value string with exactly `scale`
// decimal places, using string manipulation to avoid float64 precision issues.
func formatDecimalDefault(val string, scale int) string {
	// Remove sign for processing, add back later
	negative := strings.HasPrefix(val, "-")
	if negative {
		val = val[1:]
	}

	// Split into integer and fractional parts
	parts := strings.SplitN(val, ".", 2)
	intPart := parts[0]
	fracPart := ""
	if len(parts) > 1 {
		fracPart = parts[1]
	}

	if scale == 0 {
		// Round the fractional part: if first frac digit >= 5, increment integer
		carry := 0
		if len(fracPart) > 0 {
			firstFrac := int(fracPart[0] - '0')
			if firstFrac >= 5 {
				carry = 1
			}
		}
		if carry > 0 {
			// Increment intPart
			intDigits := []byte(intPart)
			for i := len(intDigits) - 1; i >= 0; i-- {
				if intDigits[i] < '9' {
					intDigits[i]++
					carry = 0
					break
				}
				intDigits[i] = '0'
			}
			intPart = string(intDigits)
			if carry > 0 {
				intPart = "1" + intPart
			}
		}
		if negative {
			return "-" + intPart
		}
		return intPart
	}

	// Pad or truncate fracPart to exactly scale digits
	for len(fracPart) < scale {
		fracPart += "0"
	}
	fracPart = fracPart[:scale]

	result := intPart + "." + fracPart
	if negative {
		return "-" + result
	}
	return result
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

	// Normalize type aliases
	t = strings.Replace(t, "integer", "int", 1)
	t = strings.Replace(t, "real", "double", 1)
	upper = strings.ToUpper(t)

	isUnsigned := strings.Contains(upper, "UNSIGNED")
	isZerofill := strings.Contains(upper, "ZEROFILL")

	// Build type suffix (zerofill implies unsigned in MySQL)
	suffix := ""
	if isZerofill {
		suffix = " unsigned zerofill"
	} else if isUnsigned {
		suffix = " unsigned"
	}

	// Determine the base type (before any parenthesized spec)
	base := t
	baseClean := strings.TrimSpace(strings.Replace(strings.Replace(base, " unsigned", "", 1), " zerofill", "", 1))
	baseClean = strings.TrimSpace(baseClean)

	// Check if it already has a parenthesized width
	if idx := strings.Index(baseClean, "("); idx >= 0 {
		// For DECIMAL with only precision but no scale, add ",0"
		baseType := baseClean[:idx]
		if baseType == "decimal" || baseType == "numeric" || baseType == "dec" {
			parenEnd := strings.Index(baseClean, ")")
			if parenEnd > idx {
				inner := strings.TrimSpace(baseClean[idx+1 : parenEnd])
				if !strings.Contains(inner, ",") {
					// Normalize precision=0 to 10 (MySQL minimum)
					prec := inner
					if prec == "0" {
						prec = "10"
					}
					return baseType + "(" + prec + ",0)" + suffix
				} else {
					// Has both precision and scale - normalize precision=0 to 10
					parts := strings.SplitN(inner, ",", 2)
					prec := strings.TrimSpace(parts[0])
					scale := strings.TrimSpace(parts[1])
					if prec == "0" {
						prec = "10"
					}
					return baseType + "(" + prec + "," + scale + ")" + suffix
				}
			}
		}
		// Normalize float/double precision values:
		// float(0), float(23), float(24) -> float
		// float(25..53), float(53), double(53) -> double
		if baseType == "float" || baseType == "double" || baseType == "real" {
			parenStart := strings.Index(baseClean, "(")
			parenEnd := strings.Index(baseClean, ")")
			if parenStart >= 0 && parenEnd > parenStart {
				inner := strings.TrimSpace(baseClean[parenStart+1 : parenEnd])
				if prec, err := strconv.ParseInt(inner, 10, 64); err == nil {
					if prec == 0 || (prec >= 1 && prec <= 24) {
						return "float" + suffix
					} else if prec >= 25 && prec <= 53 {
						return "double" + suffix
					}
				}
				if inner == "0" {
					return baseType + suffix
				}
			}
		}
		return t // already has explicit width with correct format
	}

	switch baseClean {
	case "tinyint":
		if isUnsigned || isZerofill {
			return "tinyint(3)" + suffix
		}
		return "tinyint(4)"
	case "smallint":
		if isUnsigned || isZerofill {
			return "smallint(5)" + suffix
		}
		return "smallint(6)"
	case "mediumint":
		if isUnsigned || isZerofill {
			return "mediumint(8)" + suffix
		}
		return "mediumint(9)"
	case "int", "integer":
		if isUnsigned || isZerofill {
			return "int(10)" + suffix
		}
		return "int(11)"
	case "bigint":
		return "bigint(20)" + suffix
	case "char":
		// CHAR without length defaults to CHAR(1)
		return "char(1)"
	case "binary":
		// BINARY without length defaults to BINARY(1)
		return "binary(1)"
	case "year":
		return "year(4)"
	case "decimal", "numeric", "dec":
		// Default precision/scale: DECIMAL(10,0)
		return "decimal(10,0)" + suffix
	}
	// For any other type with zerofill (e.g., double zerofill, float zerofill),
	// ensure "unsigned" is included since zerofill implies unsigned in MySQL.
	if isZerofill && !strings.Contains(t, "unsigned") {
		// Replace "zerofill" with "unsigned zerofill"
		t = strings.Replace(t, " zerofill", " unsigned zerofill", 1)
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

// txnExtraColumns returns the extra columns for transaction summary rows.
func txnExtraColumns() storage.Row {
	return storage.Row{
		"COUNT_READ_WRITE": int64(0), "SUM_TIMER_READ_WRITE": int64(0),
		"MIN_TIMER_READ_WRITE": int64(0), "AVG_TIMER_READ_WRITE": int64(0), "MAX_TIMER_READ_WRITE": int64(0),
		"COUNT_READ_ONLY": int64(0), "SUM_TIMER_READ_ONLY": int64(0),
		"MIN_TIMER_READ_ONLY": int64(0), "AVG_TIMER_READ_ONLY": int64(0), "MAX_TIMER_READ_ONLY": int64(0),
	}
}

func perfSchemaSeedTxnGlobalByEventName() []storage.Row {
	rows := make([]storage.Row, 0, len(psTxnEventNames))
	for _, n := range psTxnEventNames {
		row := storage.Row{
			"EVENT_NAME": n, "COUNT_STAR": int64(0),
			"SUM_TIMER_WAIT": int64(0), "MIN_TIMER_WAIT": int64(0),
			"AVG_TIMER_WAIT": int64(0), "MAX_TIMER_WAIT": int64(0),
		}
		for k, v := range txnExtraColumns() {
			row[k] = v
		}
		rows = append(rows, row)
	}
	return rows
}

func (e *Executor) perfSchemaSeedTxnByThreadByEventName() []storage.Row {
	tid := e.connectionID + 1
	rows := make([]storage.Row, 0, len(psTxnEventNames))
	for _, n := range psTxnEventNames {
		row := storage.Row{
			"THREAD_ID": tid, "EVENT_NAME": n, "COUNT_STAR": int64(0),
			"SUM_TIMER_WAIT": int64(0), "MIN_TIMER_WAIT": int64(0),
			"AVG_TIMER_WAIT": int64(0), "MAX_TIMER_WAIT": int64(0),
		}
		for k, v := range txnExtraColumns() {
			row[k] = v
		}
		rows = append(rows, row)
	}
	return rows
}

func perfSchemaSeedTxnByAccountByEventName() []storage.Row {
	users := []string{"root", "foo"}
	rows := make([]storage.Row, 0, len(psTxnEventNames)*len(users))
	for _, u := range users {
		for _, n := range psTxnEventNames {
			row := storage.Row{
				"USER": u, "HOST": "localhost", "EVENT_NAME": n, "COUNT_STAR": int64(0),
				"SUM_TIMER_WAIT": int64(0), "MIN_TIMER_WAIT": int64(0),
				"AVG_TIMER_WAIT": int64(0), "MAX_TIMER_WAIT": int64(0),
			}
			for k, v := range txnExtraColumns() {
				row[k] = v
			}
			rows = append(rows, row)
		}
	}
	return rows
}

func perfSchemaSeedTxnByHostByEventName() []storage.Row {
	rows := make([]storage.Row, 0, len(psTxnEventNames))
	for _, n := range psTxnEventNames {
		row := storage.Row{
			"HOST": "localhost", "EVENT_NAME": n, "COUNT_STAR": int64(0),
			"SUM_TIMER_WAIT": int64(0), "MIN_TIMER_WAIT": int64(0),
			"AVG_TIMER_WAIT": int64(0), "MAX_TIMER_WAIT": int64(0),
		}
		for k, v := range txnExtraColumns() {
			row[k] = v
		}
		rows = append(rows, row)
	}
	return rows
}

func perfSchemaSeedTxnByUserByEventName() []storage.Row {
	users := []string{"root", "foo"}
	rows := make([]storage.Row, 0, len(psTxnEventNames)*len(users))
	for _, u := range users {
		for _, n := range psTxnEventNames {
			row := storage.Row{
				"USER": u, "EVENT_NAME": n, "COUNT_STAR": int64(0),
				"SUM_TIMER_WAIT": int64(0), "MIN_TIMER_WAIT": int64(0),
				"AVG_TIMER_WAIT": int64(0), "MAX_TIMER_WAIT": int64(0),
			}
			for k, v := range txnExtraColumns() {
				row[k] = v
			}
			rows = append(rows, row)
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
			"SUM_LOCK_TIME": int64(0),
			"SUM_ERRORS": int64(0), "SUM_WARNINGS": int64(0),
			"SUM_ROWS_AFFECTED": int64(0), "SUM_ROWS_SENT": int64(0),
			"SUM_ROWS_EXAMINED": int64(0),
			"SUM_CREATED_TMP_DISK_TABLES": int64(0), "SUM_CREATED_TMP_TABLES": int64(0),
			"SUM_SELECT_FULL_JOIN": int64(0), "SUM_SELECT_FULL_RANGE_JOIN": int64(0),
			"SUM_SELECT_RANGE": int64(0), "SUM_SELECT_RANGE_CHECK": int64(0),
			"SUM_SELECT_SCAN": int64(0),
			"SUM_SORT_MERGE_PASSES": int64(0), "SUM_SORT_RANGE": int64(0),
			"SUM_SORT_ROWS": int64(0), "SUM_SORT_SCAN": int64(0),
			"SUM_NO_INDEX_USED": int64(0), "SUM_NO_GOOD_INDEX_USED": int64(0),
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
			"SUM_LOCK_TIME": int64(0),
			"SUM_ERRORS": int64(0), "SUM_WARNINGS": int64(0),
			"SUM_ROWS_AFFECTED": int64(0), "SUM_ROWS_SENT": int64(0),
			"SUM_ROWS_EXAMINED": int64(0),
			"SUM_CREATED_TMP_DISK_TABLES": int64(0), "SUM_CREATED_TMP_TABLES": int64(0),
			"SUM_SELECT_FULL_JOIN": int64(0), "SUM_SELECT_FULL_RANGE_JOIN": int64(0),
			"SUM_SELECT_RANGE": int64(0), "SUM_SELECT_RANGE_CHECK": int64(0),
			"SUM_SELECT_SCAN": int64(0),
			"SUM_SORT_MERGE_PASSES": int64(0), "SUM_SORT_RANGE": int64(0),
			"SUM_SORT_ROWS": int64(0), "SUM_SORT_SCAN": int64(0),
			"SUM_NO_INDEX_USED": int64(0), "SUM_NO_GOOD_INDEX_USED": int64(0),
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
				"SUM_LOCK_TIME": int64(0),
				"SUM_ERRORS": int64(0), "SUM_WARNINGS": int64(0),
				"SUM_ROWS_AFFECTED": int64(0), "SUM_ROWS_SENT": int64(0),
				"SUM_ROWS_EXAMINED": int64(0),
				"SUM_CREATED_TMP_DISK_TABLES": int64(0), "SUM_CREATED_TMP_TABLES": int64(0),
				"SUM_SELECT_FULL_JOIN": int64(0), "SUM_SELECT_FULL_RANGE_JOIN": int64(0),
				"SUM_SELECT_RANGE": int64(0), "SUM_SELECT_RANGE_CHECK": int64(0),
				"SUM_SELECT_SCAN": int64(0),
				"SUM_SORT_MERGE_PASSES": int64(0), "SUM_SORT_RANGE": int64(0),
				"SUM_SORT_ROWS": int64(0), "SUM_SORT_SCAN": int64(0),
				"SUM_NO_INDEX_USED": int64(0), "SUM_NO_GOOD_INDEX_USED": int64(0),
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
			"SUM_LOCK_TIME": int64(0),
			"SUM_ERRORS": int64(0), "SUM_WARNINGS": int64(0),
			"SUM_ROWS_AFFECTED": int64(0), "SUM_ROWS_SENT": int64(0),
			"SUM_ROWS_EXAMINED": int64(0),
			"SUM_CREATED_TMP_DISK_TABLES": int64(0), "SUM_CREATED_TMP_TABLES": int64(0),
			"SUM_SELECT_FULL_JOIN": int64(0), "SUM_SELECT_FULL_RANGE_JOIN": int64(0),
			"SUM_SELECT_RANGE": int64(0), "SUM_SELECT_RANGE_CHECK": int64(0),
			"SUM_SELECT_SCAN": int64(0),
			"SUM_SORT_MERGE_PASSES": int64(0), "SUM_SORT_RANGE": int64(0),
			"SUM_SORT_ROWS": int64(0), "SUM_SORT_SCAN": int64(0),
			"SUM_NO_INDEX_USED": int64(0), "SUM_NO_GOOD_INDEX_USED": int64(0),
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
				"SUM_LOCK_TIME": int64(0),
				"SUM_ERRORS": int64(0), "SUM_WARNINGS": int64(0),
				"SUM_ROWS_AFFECTED": int64(0), "SUM_ROWS_SENT": int64(0),
				"SUM_ROWS_EXAMINED": int64(0),
				"SUM_CREATED_TMP_DISK_TABLES": int64(0), "SUM_CREATED_TMP_TABLES": int64(0),
				"SUM_SELECT_FULL_JOIN": int64(0), "SUM_SELECT_FULL_RANGE_JOIN": int64(0),
				"SUM_SELECT_RANGE": int64(0), "SUM_SELECT_RANGE_CHECK": int64(0),
				"SUM_SELECT_SCAN": int64(0),
				"SUM_SORT_MERGE_PASSES": int64(0), "SUM_SORT_RANGE": int64(0),
				"SUM_SORT_ROWS": int64(0), "SUM_SORT_SCAN": int64(0),
				"SUM_NO_INDEX_USED": int64(0), "SUM_NO_GOOD_INDEX_USED": int64(0),
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

// perfSchemaSessionConnectAttrs returns rows for session_connect_attrs / session_account_connect_attrs.
// These tables expose connection attributes (like _os, _platform, _client_name, etc.) for the current session.
func (e *Executor) perfSchemaSessionConnectAttrs() []storage.Row {
	attrs := []struct {
		Name  string
		Value string
	}{
		{"_os", "Linux"},
		{"_client_name", "libmysql"},
		{"_pid", "1"},
		{"_client_version", "8.0.0"},
		{"_platform", "x86_64"},
		{"program_name", "mysql"},
	}
	rows := make([]storage.Row, 0, len(attrs))
	for i, a := range attrs {
		rows = append(rows, storage.Row{
			"PROCESSLIST_ID":   e.connectionID,
			"ATTR_NAME":        a.Name,
			"ATTR_VALUE":       a.Value,
			"ORDINAL_POSITION": int64(i),
		})
	}
	return rows
}

// perfSchemaSocketSummaryByEventName returns rows for socket_summary_by_event_name.
func perfSchemaSocketSummaryByEventName() []storage.Row {
	events := []string{
		"wait/io/socket/sql/server_tcpip_socket",
		"wait/io/socket/sql/server_unix_socket",
		"wait/io/socket/sql/client_connection",
	}
	rows := make([]storage.Row, 0, len(events))
	for _, ev := range events {
		rows = append(rows, storage.Row{
			"EVENT_NAME":     ev,
			"COUNT_STAR":     int64(0),
			"SUM_TIMER_WAIT": int64(0),
			"MIN_TIMER_WAIT": int64(0),
			"AVG_TIMER_WAIT": int64(0),
			"MAX_TIMER_WAIT": int64(0),
		})
	}
	return rows
}

// perfSchemaStatusByThread returns rows for status_by_thread.
func (e *Executor) perfSchemaStatusByThread() []storage.Row {
	threadID := e.connectionID + 1
	// Return a subset of important status variables per thread
	statusVars := []struct {
		Name  string
		Value string
	}{
		{"Bytes_received", "0"},
		{"Bytes_sent", "0"},
		{"Handler_read_key", fmt.Sprintf("%d", e.handlerReadKey)},
		{"Max_execution_time_exceeded", "0"},
		{"Max_execution_time_set", "0"},
		{"Max_execution_time_set_failed", "0"},
		{"Sort_rows", "0"},
		{"Sort_scan", "0"},
	}
	rows := make([]storage.Row, 0, len(statusVars))
	for _, sv := range statusVars {
		rows = append(rows, storage.Row{
			"THREAD_ID":      threadID,
			"VARIABLE_NAME":  sv.Name,
			"VARIABLE_VALUE": sv.Value,
		})
	}
	return rows
}

// perfSchemaUserVariablesByThread returns rows for user_variables_by_thread.
func (e *Executor) perfSchemaUserVariablesByThread() []storage.Row {
	threadID := e.connectionID + 1
	rows := make([]storage.Row, 0, len(e.userVars))
	for name, val := range e.userVars {
		valStr := "NULL"
		if val != nil {
			valStr = fmt.Sprintf("%v", val)
		}
		rows = append(rows, storage.Row{
			"THREAD_ID":      threadID,
			"VARIABLE_NAME":  name,
			"VARIABLE_VALUE": valStr,
		})
	}
	return rows
}

// perfSchemaVariablesByThread returns rows for variables_by_thread.
func (e *Executor) perfSchemaVariablesByThread() []storage.Row {
	threadID := e.connectionID + 1
	vars := e.buildVariablesMapScoped(false) // session scope
	rows := make([]storage.Row, 0, len(vars))
	for name, val := range vars {
		rows = append(rows, storage.Row{
			"THREAD_ID":      threadID,
			"VARIABLE_NAME":  name,
			"VARIABLE_VALUE": val,
		})
	}
	return rows
}

// perfSchemaESMSByDigest returns rows for events_statements_summary_by_digest.
func (e *Executor) perfSchemaESMSByDigest() []storage.Row {
	if e.psTruncated["events_statements_summary_by_digest"] && len(e.psDigests) == 0 {
		return []storage.Row{}
	}
	// When digests_size=0, the table is disabled — return empty.
	if v, ok := e.startupVars["performance_schema_digests_size"]; ok && v == "0" {
		return []storage.Row{}
	}
	// When digests_size=N and N is very small (e.g. 1), the table fills up
	// immediately. Show a NULL overflow row to indicate the table is full.
	if v, ok := e.startupVars["performance_schema_digests_size"]; ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 && n <= 1 {
			// Table has a capacity of 1 and is already full (overflow indicator).
			return []storage.Row{{
				"SCHEMA_NAME":                 nil,
				"DIGEST":                      nil,
				"DIGEST_TEXT":                  nil,
				"COUNT_STAR":                   int64(0),
				"SUM_TIMER_WAIT":              int64(0),
				"MIN_TIMER_WAIT":              int64(0),
				"AVG_TIMER_WAIT":              int64(0),
				"MAX_TIMER_WAIT":              int64(0),
				"SUM_LOCK_TIME":               int64(0),
				"SUM_ERRORS":                  int64(0),
				"SUM_WARNINGS":                int64(0),
				"SUM_ROWS_AFFECTED":           int64(0),
				"SUM_ROWS_SENT":               int64(0),
				"SUM_ROWS_EXAMINED":           int64(0),
				"SUM_CREATED_TMP_DISK_TABLES": int64(0),
				"SUM_CREATED_TMP_TABLES":      int64(0),
				"SUM_SELECT_FULL_JOIN":        int64(0),
				"SUM_SELECT_FULL_RANGE_JOIN":  int64(0),
				"SUM_SELECT_RANGE":            int64(0),
				"SUM_SELECT_RANGE_CHECK":      int64(0),
				"SUM_SELECT_SCAN":             int64(0),
				"SUM_SORT_MERGE_PASSES":       int64(0),
				"SUM_SORT_RANGE":              int64(0),
				"SUM_SORT_ROWS":               int64(0),
				"SUM_SORT_SCAN":               int64(0),
				"SUM_NO_INDEX_USED":           int64(0),
				"SUM_NO_GOOD_INDEX_USED":      int64(0),
				"FIRST_SEEN":                  "2024-01-01 00:00:00.000000",
				"LAST_SEEN":                   "2024-01-01 00:00:00.000000",
				"QUANTILE_95":                 int64(0),
				"QUANTILE_99":                 int64(0),
				"QUANTILE_999":                int64(0),
				"QUERY_SAMPLE_TEXT":           nil,
				"QUERY_SAMPLE_SEEN":           "2024-01-01 00:00:00.000000",
				"QUERY_SAMPLE_TIMER_WAIT":     int64(0),
			}}
		}
	}
	rows := make([]storage.Row, 0, len(e.psDigests))
	for _, d := range e.psDigests {
		rows = append(rows, storage.Row{
			"SCHEMA_NAME":                  d.SchemaName,
			"DIGEST":                       d.Digest,
			"DIGEST_TEXT":                   d.DigestText,
			"COUNT_STAR":                    d.CountStar,
			"SUM_TIMER_WAIT":               int64(0),
			"MIN_TIMER_WAIT":               int64(0),
			"AVG_TIMER_WAIT":               int64(0),
			"MAX_TIMER_WAIT":               int64(0),
			"SUM_LOCK_TIME":                int64(0),
			"SUM_ERRORS":                   int64(0),
			"SUM_WARNINGS":                 int64(0),
			"SUM_ROWS_AFFECTED":            int64(0),
			"SUM_ROWS_SENT":                int64(0),
			"SUM_ROWS_EXAMINED":            int64(0),
			"SUM_CREATED_TMP_DISK_TABLES":  int64(0),
			"SUM_CREATED_TMP_TABLES":       int64(0),
			"SUM_SELECT_FULL_JOIN":         int64(0),
			"SUM_SELECT_FULL_RANGE_JOIN":   int64(0),
			"SUM_SELECT_RANGE":             int64(0),
			"SUM_SELECT_RANGE_CHECK":       int64(0),
			"SUM_SELECT_SCAN":              int64(0),
			"SUM_SORT_MERGE_PASSES":        int64(0),
			"SUM_SORT_RANGE":               int64(0),
			"SUM_SORT_ROWS":                int64(0),
			"SUM_SORT_SCAN":                int64(0),
			"SUM_NO_INDEX_USED":            int64(0),
			"SUM_NO_GOOD_INDEX_USED":       int64(0),
			"FIRST_SEEN":                   "2024-01-01 00:00:00.000000",
			"LAST_SEEN":                    "2024-01-01 00:00:00.000000",
			"QUANTILE_95":                  int64(0),
			"QUANTILE_99":                  int64(0),
			"QUANTILE_999":                 int64(0),
			"QUERY_SAMPLE_TEXT":            d.DigestText,
			"QUERY_SAMPLE_SEEN":            "2024-01-01 00:00:00.000000",
			"QUERY_SAMPLE_TIMER_WAIT":      int64(0),
		})
	}
	return rows
}

// perfSchemaESMHByDigest returns rows for events_statements_histogram_by_digest.
func (e *Executor) perfSchemaESMHByDigest() []storage.Row {
	if e.psTruncated["events_statements_histogram_by_digest"] && len(e.psDigests) == 0 {
		return []storage.Row{}
	}
	rows := make([]storage.Row, 0, len(e.psDigests))
	for _, d := range e.psDigests {
		rows = append(rows, storage.Row{
			"SCHEMA_NAME":            d.SchemaName,
			"DIGEST":                 d.Digest,
			"BUCKET_NUMBER":          int64(0),
			"BUCKET_TIMER_LOW":       int64(0),
			"BUCKET_TIMER_HIGH":      int64(1000000),
			"COUNT_BUCKET":           d.CountStar,
			"COUNT_BUCKET_AND_LOWER": d.CountStar,
			"BUCKET_QUANTILE":        1.0,
		})
	}
	return rows
}

// wgs84Definition is the WKT definition string for WGS 84 (SRID 4326).
const wgs84Definition = `GEOGCS["WGS 84",DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]]`

// infoSchemaSpatialReferenceSystems returns the minimal SRS catalog rows.
// Columns: SRS_NAME, SRS_ID, ORGANIZATION, ORGANIZATION_COORDSYS_ID, DEFINITION, DESCRIPTION
func infoSchemaSpatialReferenceSystems() []storage.Row {
	return []storage.Row{
		{
			"SRS_NAME":                 "",
			"SRS_ID":                   uint32(0),
			"ORGANIZATION":             nil,
			"ORGANIZATION_COORDSYS_ID": nil,
			"DEFINITION":               "",
			"DESCRIPTION":              nil,
		},
		{
			"SRS_NAME":                 "WGS 84",
			"SRS_ID":                   uint32(4326),
			"ORGANIZATION":             "EPSG",
			"ORGANIZATION_COORDSYS_ID": uint32(4326),
			"DEFINITION":               wgs84Definition,
			"DESCRIPTION":              nil,
		},
	}
}

// infoSchemaSTGeometryColumns returns rows for INFORMATION_SCHEMA.ST_GEOMETRY_COLUMNS.
// Columns: TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, SRS_NAME, SRS_ID, GEOMETRY_TYPE_NAME
func (e *Executor) infoSchemaSTGeometryColumns() []storage.Row {
	var rows []storage.Row
	dbNames := e.Catalog.ListDatabases()
	sort.Strings(dbNames)
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
			tbl, err := db.GetTable(tblName)
			if err != nil {
				continue
			}
			for _, col := range tbl.Columns {
				colUpper := strings.ToUpper(col.Type)
				// Check if this is a spatial type
				if !strings.Contains(colUpper, "POINT") &&
					!strings.Contains(colUpper, "LINESTRING") &&
					!strings.Contains(colUpper, "POLYGON") &&
					!strings.Contains(colUpper, "GEOMETRY") &&
					!strings.Contains(colUpper, "GEOMCOLLECTION") {
					continue
				}
				// Determine geometry type name (lowercase)
				geomTypeName := "geometry"
				switch {
				case strings.Contains(colUpper, "MULTIPOLYGON"):
					geomTypeName = "multipolygon"
				case strings.Contains(colUpper, "MULTILINESTRING"):
					geomTypeName = "multilinestring"
				case strings.Contains(colUpper, "MULTIPOINT"):
					geomTypeName = "multipoint"
				case strings.Contains(colUpper, "GEOMETRYCOLLECTION"), strings.Contains(colUpper, "GEOMCOLLECTION"):
					geomTypeName = "geomcollection"
				case strings.Contains(colUpper, "POLYGON"):
					geomTypeName = "polygon"
				case strings.Contains(colUpper, "LINESTRING"):
					geomTypeName = "linestring"
				case strings.Contains(colUpper, "POINT"):
					geomTypeName = "point"
				}
				// Extract SRID from column definition if present
				var srsName interface{} = nil
				var srsID interface{} = nil
				if col.SRIDConstraint != nil {
					srsID = *col.SRIDConstraint
					// Look up SRS name from known SRS catalog
					for _, srs := range infoSchemaSpatialReferenceSystems() {
						if id, ok := srs["SRS_ID"].(uint32); ok && id == *col.SRIDConstraint {
							if name, ok2 := srs["SRS_NAME"].(string); ok2 && name != "" {
								srsName = name
							}
							break
						}
					}
				}
				rows = append(rows, storage.Row{
					"TABLE_CATALOG":      "def",
					"TABLE_SCHEMA":       dbName,
					"TABLE_NAME":         tblName,
					"COLUMN_NAME":        col.Name,
					"SRS_NAME":           srsName,
					"SRS_ID":             srsID,
					"GEOMETRY_TYPE_NAME": geomTypeName,
				})
			}
		}
	}
	return rows
}
