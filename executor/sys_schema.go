package executor

import (
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/myuon/mylite/catalog"
	"vitess.io/vitess/go/vt/sqlparser"
)

// initSysSchema populates the sys database with stub table/view definitions
// so that DESC sys.<table> works and INFORMATION_SCHEMA.TABLES lists them.
func initSysSchema(db *catalog.Database) {
	db.Tables["host_summary"] = &catalog.TableDef{
		Name: "host_summary",
		Columns: []catalog.ColumnDef{
			{Name: "host", Type: "VARCHAR(255)", Nullable: true, Default: nil,},
			{Name: "statements", Type: "DECIMAL(64,0)", Nullable: true, Default: nil,},
			{Name: "statement_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "statement_avg_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "table_scans", Type: "DECIMAL(65,0)", Nullable: true, Default: nil,},
			{Name: "file_ios", Type: "DECIMAL(64,0)", Nullable: true, Default: nil,},
			{Name: "file_io_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "current_connections", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "total_connections", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "unique_users", Type: "BIGINT(21)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "current_memory", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "total_memory_allocated", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["host_summary_by_file_io"] = &catalog.TableDef{
		Name: "host_summary_by_file_io",
		Columns: []catalog.ColumnDef{
			{Name: "host", Type: "VARCHAR(255)", Nullable: true, Default: nil,},
			{Name: "ios", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "io_latency", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["host_summary_by_file_io_type"] = &catalog.TableDef{
		Name: "host_summary_by_file_io_type",
		Columns: []catalog.ColumnDef{
			{Name: "host", Type: "VARCHAR(255)", Nullable: true, Default: nil,},
			{Name: "event_name", Type: "VARCHAR(128)", Nullable: false, Default: nil,},
			{Name: "total", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "max_latency", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["host_summary_by_stages"] = &catalog.TableDef{
		Name: "host_summary_by_stages",
		Columns: []catalog.ColumnDef{
			{Name: "host", Type: "VARCHAR(255)", Nullable: true, Default: nil,},
			{Name: "event_name", Type: "VARCHAR(128)", Nullable: false, Default: nil,},
			{Name: "total", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "avg_latency", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["host_summary_by_statement_latency"] = &catalog.TableDef{
		Name: "host_summary_by_statement_latency",
		Columns: []catalog.ColumnDef{
			{Name: "host", Type: "VARCHAR(255)", Nullable: true, Default: nil,},
			{Name: "total", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "total_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "max_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "lock_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "rows_sent", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "rows_examined", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "rows_affected", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "full_scans", Type: "DECIMAL(43,0)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["host_summary_by_statement_type"] = &catalog.TableDef{
		Name: "host_summary_by_statement_type",
		Columns: []catalog.ColumnDef{
			{Name: "host", Type: "VARCHAR(255)", Nullable: true, Default: nil,},
			{Name: "statement", Type: "VARCHAR(128)", Nullable: true, Default: nil,},
			{Name: "total", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "max_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "lock_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "rows_sent", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_examined", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_affected", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "full_scans", Type: "BIGINT(21) UNSIGNED", Nullable: false, Default: sysStrPtr("0"),},
		},
	}
	db.Tables["innodb_buffer_stats_by_schema"] = &catalog.TableDef{
		Name: "innodb_buffer_stats_by_schema",
		Columns: []catalog.ColumnDef{
			{Name: "object_schema", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "allocated", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "data", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "pages", Type: "BIGINT(21)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "pages_hashed", Type: "BIGINT(21)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "pages_old", Type: "BIGINT(21)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "rows_cached", Type: "DECIMAL(44,0)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["innodb_buffer_stats_by_table"] = &catalog.TableDef{
		Name: "innodb_buffer_stats_by_table",
		Columns: []catalog.ColumnDef{
			{Name: "object_schema", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "object_name", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "allocated", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "data", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "pages", Type: "BIGINT(21)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "pages_hashed", Type: "BIGINT(21)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "pages_old", Type: "BIGINT(21)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "rows_cached", Type: "DECIMAL(44,0)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["innodb_lock_waits"] = &catalog.TableDef{
		Name: "innodb_lock_waits",
		Columns: []catalog.ColumnDef{
			{Name: "wait_started", Type: "DATETIME", Nullable: true, Default: nil,},
			{Name: "wait_age", Type: "TIME", Nullable: true, Default: nil,},
			{Name: "wait_age_secs", Type: "BIGINT(21)", Nullable: true, Default: nil,},
			{Name: "locked_table", Type: "MEDIUMTEXT", Nullable: true, Default: nil,},
			{Name: "locked_table_schema", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "locked_table_name", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "locked_table_partition", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "locked_table_subpartition", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "locked_index", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "locked_type", Type: "VARCHAR(32)", Nullable: false, Default: nil,},
			{Name: "waiting_trx_id", Type: "VARCHAR(18)", Nullable: false, Default: sysStrPtr(""),},
			{Name: "waiting_trx_started", Type: "DATETIME", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00"),},
			{Name: "waiting_trx_age", Type: "TIME", Nullable: true, Default: nil,},
			{Name: "waiting_trx_rows_locked", Type: "BIGINT(21) UNSIGNED", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "waiting_trx_rows_modified", Type: "BIGINT(21) UNSIGNED", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "waiting_pid", Type: "BIGINT(21) UNSIGNED", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "waiting_query", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "waiting_lock_id", Type: "VARCHAR(128)", Nullable: false, Default: nil,},
			{Name: "waiting_lock_mode", Type: "VARCHAR(32)", Nullable: false, Default: nil,},
			{Name: "blocking_trx_id", Type: "VARCHAR(18)", Nullable: false, Default: sysStrPtr(""),},
			{Name: "blocking_pid", Type: "BIGINT(21) UNSIGNED", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "blocking_query", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "blocking_lock_id", Type: "VARCHAR(128)", Nullable: false, Default: nil,},
			{Name: "blocking_lock_mode", Type: "VARCHAR(32)", Nullable: false, Default: nil,},
			{Name: "blocking_trx_started", Type: "DATETIME", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00"),},
			{Name: "blocking_trx_age", Type: "TIME", Nullable: true, Default: nil,},
			{Name: "blocking_trx_rows_locked", Type: "BIGINT(21) UNSIGNED", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "blocking_trx_rows_modified", Type: "BIGINT(21) UNSIGNED", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "sql_kill_blocking_query", Type: "VARCHAR(32)", Nullable: false, Default: sysStrPtr(""),},
			{Name: "sql_kill_blocking_connection", Type: "VARCHAR(26)", Nullable: false, Default: sysStrPtr(""),},
		},
	}
	db.Tables["io_by_thread_by_latency"] = &catalog.TableDef{
		Name: "io_by_thread_by_latency",
		Columns: []catalog.ColumnDef{
			{Name: "user", Type: "VARCHAR(288)", Nullable: true, Default: nil,},
			{Name: "total", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "total_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "min_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "avg_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "max_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "thread_id", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "processlist_id", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
		},
	}
	db.Tables["io_global_by_file_by_bytes"] = &catalog.TableDef{
		Name: "io_global_by_file_by_bytes",
		Columns: []catalog.ColumnDef{
			{Name: "file", Type: "VARCHAR(512)", Nullable: true, Default: nil,},
			{Name: "count_read", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_read", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "avg_read", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "count_write", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_written", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "avg_write", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "total", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "write_pct", Type: "DECIMAL(26,2)", Nullable: false, Default: sysStrPtr("0.00"),},
		},
	}
	db.Tables["io_global_by_file_by_latency"] = &catalog.TableDef{
		Name: "io_global_by_file_by_latency",
		Columns: []catalog.ColumnDef{
			{Name: "file", Type: "VARCHAR(512)", Nullable: true, Default: nil,},
			{Name: "total", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "count_read", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "read_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "count_write", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "write_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "count_misc", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "misc_latency", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["io_global_by_wait_by_bytes"] = &catalog.TableDef{
		Name: "io_global_by_wait_by_bytes",
		Columns: []catalog.ColumnDef{
			{Name: "event_name", Type: "VARCHAR(128)", Nullable: true, Default: nil,},
			{Name: "total", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "min_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "avg_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "max_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "count_read", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_read", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "avg_read", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "count_write", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_written", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "avg_written", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "total_requested", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["io_global_by_wait_by_latency"] = &catalog.TableDef{
		Name: "io_global_by_wait_by_latency",
		Columns: []catalog.ColumnDef{
			{Name: "event_name", Type: "VARCHAR(128)", Nullable: true, Default: nil,},
			{Name: "total", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "avg_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "max_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "read_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "write_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "misc_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "count_read", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_read", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "avg_read", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "count_write", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_written", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "avg_written", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["latest_file_io"] = &catalog.TableDef{
		Name: "latest_file_io",
		Columns: []catalog.ColumnDef{
			{Name: "thread", Type: "VARCHAR(316)", Nullable: true, Default: nil,},
			{Name: "file", Type: "VARCHAR(512)", Nullable: true, Default: nil,},
			{Name: "latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "operation", Type: "VARCHAR(32)", Nullable: false, Default: nil,},
			{Name: "requested", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["memory_by_host_by_current_bytes"] = &catalog.TableDef{
		Name: "memory_by_host_by_current_bytes",
		Columns: []catalog.ColumnDef{
			{Name: "host", Type: "VARCHAR(255)", Nullable: true, Default: nil,},
			{Name: "current_count_used", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "current_allocated", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "current_avg_alloc", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "current_max_alloc", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "total_allocated", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["memory_by_thread_by_current_bytes"] = &catalog.TableDef{
		Name: "memory_by_thread_by_current_bytes",
		Columns: []catalog.ColumnDef{
			{Name: "thread_id", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "user", Type: "VARCHAR(288)", Nullable: true, Default: nil,},
			{Name: "current_count_used", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "current_allocated", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "current_avg_alloc", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "current_max_alloc", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "total_allocated", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["memory_by_user_by_current_bytes"] = &catalog.TableDef{
		Name: "memory_by_user_by_current_bytes",
		Columns: []catalog.ColumnDef{
			{Name: "user", Type: "VARCHAR(32)", Nullable: true, Default: nil,},
			{Name: "current_count_used", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "current_allocated", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "current_avg_alloc", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "current_max_alloc", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "total_allocated", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["memory_global_by_current_bytes"] = &catalog.TableDef{
		Name: "memory_global_by_current_bytes",
		Columns: []catalog.ColumnDef{
			{Name: "event_name", Type: "VARCHAR(128)", Nullable: false, Default: nil,},
			{Name: "current_count", Type: "BIGINT(20)", Nullable: false, Default: nil,},
			{Name: "current_alloc", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "current_avg_alloc", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "high_count", Type: "BIGINT(20)", Nullable: false, Default: nil,},
			{Name: "high_alloc", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "high_avg_alloc", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["memory_global_total"] = &catalog.TableDef{
		Name: "memory_global_total",
		Columns: []catalog.ColumnDef{
			{Name: "total_allocated", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["metrics"] = &catalog.TableDef{
		Name: "metrics",
		Columns: []catalog.ColumnDef{
			{Name: "Variable_name", Type: "VARCHAR(64)", Nullable: false, Default: sysStrPtr(""),},
			{Name: "Variable_value", Type: "VARCHAR(1024)", Nullable: true, Default: nil,},
			{Name: "Type", Type: "VARCHAR(13)", Nullable: false, Default: sysStrPtr(""),},
			{Name: "Enabled", Type: "VARCHAR(3)", Nullable: false, Default: sysStrPtr(""),},
		},
	}
	db.Tables["processlist"] = &catalog.TableDef{
		Name: "processlist",
		Columns: []catalog.ColumnDef{
			{Name: "thd_id", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "conn_id", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "user", Type: "VARCHAR(288)", Nullable: true, Default: nil,},
			{Name: "db", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "command", Type: "VARCHAR(16)", Nullable: true, Default: nil,},
			{Name: "state", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "time", Type: "BIGINT(20)", Nullable: true, Default: nil,},
			{Name: "current_statement", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "statement_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "progress", Type: "DECIMAL(26,2)", Nullable: true, Default: nil,},
			{Name: "lock_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "rows_examined", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "rows_sent", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "rows_affected", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "tmp_tables", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "tmp_disk_tables", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "full_scan", Type: "VARCHAR(3)", Nullable: false, Default: sysStrPtr(""),},
			{Name: "last_statement", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "last_statement_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "current_memory", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "last_wait", Type: "VARCHAR(128)", Nullable: true, Default: nil,},
			{Name: "last_wait_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "source", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "trx_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "trx_state", Type: "ENUM('ACTIVE','COMMITTED','ROLLED BACK')", Nullable: true, Default: nil,},
			{Name: "trx_autocommit", Type: "ENUM('YES','NO')", Nullable: true, Default: nil,},
			{Name: "pid", Type: "VARCHAR(1024)", Nullable: true, Default: nil,},
			{Name: "program_name", Type: "VARCHAR(1024)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["ps_check_lost_instrumentation"] = &catalog.TableDef{
		Name: "ps_check_lost_instrumentation",
		Columns: []catalog.ColumnDef{
			{Name: "variable_name", Type: "VARCHAR(64)", Nullable: false, Default: nil,},
			{Name: "variable_value", Type: "VARCHAR(1024)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["schema_auto_increment_columns"] = &catalog.TableDef{
		Name: "schema_auto_increment_columns",
		Columns: []catalog.ColumnDef{
			{Name: "table_schema", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "table_name", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "column_name", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "data_type", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "column_type", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "is_signed", Type: "INT", Nullable: false, Default: nil,},
			{Name: "is_unsigned", Type: "INT", Nullable: false, Default: nil,},
			{Name: "max_value", Type: "BIGINT UNSIGNED", Nullable: true, Default: nil,},
			{Name: "auto_increment", Type: "BIGINT UNSIGNED", Nullable: true, Default: nil,},
			{Name: "auto_increment_ratio", Type: "DECIMAL(25,4)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["schema_index_statistics"] = &catalog.TableDef{
		Name: "schema_index_statistics",
		Columns: []catalog.ColumnDef{
			{Name: "table_schema", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "table_name", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "index_name", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "rows_selected", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "select_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "rows_inserted", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "insert_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "rows_updated", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "update_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "rows_deleted", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "delete_latency", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["schema_object_overview"] = &catalog.TableDef{
		Name: "schema_object_overview",
		Columns: []catalog.ColumnDef{
			{Name: "db", Type: "VARCHAR(64)", Nullable: false, Default: nil,},
			{Name: "object_type", Type: "ENUM('FUNCTION','PROCEDURE')", Nullable: false, Default: nil,},
			{Name: "count", Type: "BIGINT(21)", Nullable: false, Default: sysStrPtr("0"),},
		},
	}
	db.Tables["schema_redundant_indexes"] = &catalog.TableDef{
		Name: "schema_redundant_indexes",
		Columns: []catalog.ColumnDef{
			{Name: "table_schema", Type: "VARCHAR(64)", Nullable: false, Default: nil,},
			{Name: "table_name", Type: "VARCHAR(64)", Nullable: false, Default: nil,},
			{Name: "redundant_index_name", Type: "VARCHAR(64)", Nullable: false, Default: nil,},
			{Name: "redundant_index_columns", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "redundant_index_non_unique", Type: "BIGINT", Nullable: true, Default: nil,},
			{Name: "dominant_index_name", Type: "VARCHAR(64)", Nullable: false, Default: nil,},
			{Name: "dominant_index_columns", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "dominant_index_non_unique", Type: "BIGINT", Nullable: true, Default: nil,},
			{Name: "subpart_exists", Type: "INT", Nullable: false, Default: nil,},
			{Name: "sql_drop_index", Type: "VARCHAR(223)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["schema_table_lock_waits"] = &catalog.TableDef{
		Name: "schema_table_lock_waits",
		Columns: []catalog.ColumnDef{
			{Name: "object_schema", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "object_name", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "waiting_thread_id", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "waiting_pid", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "waiting_account", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "waiting_lock_type", Type: "VARCHAR(32)", Nullable: false, Default: nil,},
			{Name: "waiting_lock_duration", Type: "VARCHAR(32)", Nullable: false, Default: nil,},
			{Name: "waiting_query", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "waiting_query_secs", Type: "BIGINT(20)", Nullable: true, Default: nil,},
			{Name: "waiting_query_rows_affected", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "waiting_query_rows_examined", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "blocking_thread_id", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "blocking_pid", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "blocking_account", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "blocking_lock_type", Type: "VARCHAR(32)", Nullable: false, Default: nil,},
			{Name: "blocking_lock_duration", Type: "VARCHAR(32)", Nullable: false, Default: nil,},
			{Name: "sql_kill_blocking_query", Type: "VARCHAR(31)", Nullable: true, Default: nil,},
			{Name: "sql_kill_blocking_connection", Type: "VARCHAR(25)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["schema_table_statistics"] = &catalog.TableDef{
		Name: "schema_table_statistics",
		Columns: []catalog.ColumnDef{
			{Name: "table_schema", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "table_name", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "total_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "rows_fetched", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "fetch_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "rows_inserted", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "insert_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "rows_updated", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "update_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "rows_deleted", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "delete_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "io_read_requests", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "io_read", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "io_read_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "io_write_requests", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "io_write", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "io_write_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "io_misc_requests", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "io_misc_latency", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["schema_table_statistics_with_buffer"] = &catalog.TableDef{
		Name: "schema_table_statistics_with_buffer",
		Columns: []catalog.ColumnDef{
			{Name: "table_schema", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "table_name", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "rows_fetched", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "fetch_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "rows_inserted", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "insert_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "rows_updated", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "update_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "rows_deleted", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "delete_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "io_read_requests", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "io_read", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "io_read_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "io_write_requests", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "io_write", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "io_write_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "io_misc_requests", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "io_misc_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "innodb_buffer_allocated", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "innodb_buffer_data", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "innodb_buffer_free", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "innodb_buffer_pages", Type: "BIGINT(21)", Nullable: true, Default: sysStrPtr("0"),},
			{Name: "innodb_buffer_pages_hashed", Type: "BIGINT(21)", Nullable: true, Default: sysStrPtr("0"),},
			{Name: "innodb_buffer_pages_old", Type: "BIGINT(21)", Nullable: true, Default: sysStrPtr("0"),},
			{Name: "innodb_buffer_rows_cached", Type: "DECIMAL(44,0)", Nullable: true, Default: sysStrPtr("0"),},
		},
	}
	db.Tables["schema_tables_with_full_table_scans"] = &catalog.TableDef{
		Name: "schema_tables_with_full_table_scans",
		Columns: []catalog.ColumnDef{
			{Name: "object_schema", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "object_name", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "rows_full_scanned", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "latency", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["schema_unused_indexes"] = &catalog.TableDef{
		Name: "schema_unused_indexes",
		Columns: []catalog.ColumnDef{
			{Name: "object_schema", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "object_name", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "index_name", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["session"] = &catalog.TableDef{
		Name: "session",
		Columns: []catalog.ColumnDef{
			{Name: "thd_id", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "conn_id", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "user", Type: "VARCHAR(288)", Nullable: true, Default: nil,},
			{Name: "db", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "command", Type: "VARCHAR(16)", Nullable: true, Default: nil,},
			{Name: "state", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "time", Type: "BIGINT(20)", Nullable: true, Default: nil,},
			{Name: "current_statement", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "statement_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "progress", Type: "DECIMAL(26,2)", Nullable: true, Default: nil,},
			{Name: "lock_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "rows_examined", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "rows_sent", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "rows_affected", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "tmp_tables", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "tmp_disk_tables", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "full_scan", Type: "VARCHAR(3)", Nullable: false, Default: sysStrPtr(""),},
			{Name: "last_statement", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "last_statement_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "current_memory", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "last_wait", Type: "VARCHAR(128)", Nullable: true, Default: nil,},
			{Name: "last_wait_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "source", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "trx_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "trx_state", Type: "ENUM('ACTIVE','COMMITTED','ROLLED BACK')", Nullable: true, Default: nil,},
			{Name: "trx_autocommit", Type: "ENUM('YES','NO')", Nullable: true, Default: nil,},
			{Name: "pid", Type: "VARCHAR(1024)", Nullable: true, Default: nil,},
			{Name: "program_name", Type: "VARCHAR(1024)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["session_ssl_status"] = &catalog.TableDef{
		Name: "session_ssl_status",
		Columns: []catalog.ColumnDef{
			{Name: "thread_id", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "ssl_version", Type: "VARCHAR(1024)", Nullable: true, Default: nil,},
			{Name: "ssl_cipher", Type: "VARCHAR(1024)", Nullable: true, Default: nil,},
			{Name: "ssl_sessions_reused", Type: "VARCHAR(1024)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["statement_analysis"] = &catalog.TableDef{
		Name: "statement_analysis",
		Columns: []catalog.ColumnDef{
			{Name: "query", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "db", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "full_scan", Type: "VARCHAR(1)", Nullable: false, Default: sysStrPtr(""),},
			{Name: "exec_count", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "err_count", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "warn_count", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "max_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "avg_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "lock_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "rows_sent", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_sent_avg", Type: "DECIMAL(21,0)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "rows_examined", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_examined_avg", Type: "DECIMAL(21,0)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "rows_affected", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_affected_avg", Type: "DECIMAL(21,0)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "tmp_tables", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "tmp_disk_tables", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_sorted", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "sort_merge_passes", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "digest", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "first_seen", Type: "TIMESTAMP(6)", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00.000000"),},
			{Name: "last_seen", Type: "TIMESTAMP(6)", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00.000000"),},
		},
	}
	db.Tables["statements_with_errors_or_warnings"] = &catalog.TableDef{
		Name: "statements_with_errors_or_warnings",
		Columns: []catalog.ColumnDef{
			{Name: "query", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "db", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "exec_count", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "errors", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "error_pct", Type: "DECIMAL(27,4)", Nullable: false, Default: sysStrPtr("0.0000"),},
			{Name: "warnings", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "warning_pct", Type: "DECIMAL(27,4)", Nullable: false, Default: sysStrPtr("0.0000"),},
			{Name: "first_seen", Type: "TIMESTAMP(6)", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00.000000"),},
			{Name: "last_seen", Type: "TIMESTAMP(6)", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00.000000"),},
			{Name: "digest", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["statements_with_full_table_scans"] = &catalog.TableDef{
		Name: "statements_with_full_table_scans",
		Columns: []catalog.ColumnDef{
			{Name: "query", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "db", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "exec_count", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "no_index_used_count", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "no_good_index_used_count", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "no_index_used_pct", Type: "DECIMAL(24,0)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "rows_sent", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_examined", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_sent_avg", Type: "DECIMAL(21,0) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "rows_examined_avg", Type: "DECIMAL(21,0) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "first_seen", Type: "TIMESTAMP(6)", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00.000000"),},
			{Name: "last_seen", Type: "TIMESTAMP(6)", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00.000000"),},
			{Name: "digest", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["statements_with_runtimes_in_95th_percentile"] = &catalog.TableDef{
		Name: "statements_with_runtimes_in_95th_percentile",
		Columns: []catalog.ColumnDef{
			{Name: "query", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "db", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "full_scan", Type: "VARCHAR(1)", Nullable: false, Default: sysStrPtr(""),},
			{Name: "exec_count", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "err_count", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "warn_count", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "max_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "avg_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "rows_sent", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_sent_avg", Type: "DECIMAL(21,0)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "rows_examined", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_examined_avg", Type: "DECIMAL(21,0)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "first_seen", Type: "TIMESTAMP(6)", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00.000000"),},
			{Name: "last_seen", Type: "TIMESTAMP(6)", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00.000000"),},
			{Name: "digest", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["statements_with_sorting"] = &catalog.TableDef{
		Name: "statements_with_sorting",
		Columns: []catalog.ColumnDef{
			{Name: "query", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "db", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "exec_count", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "sort_merge_passes", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "avg_sort_merges", Type: "DECIMAL(21,0)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "sorts_using_scans", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "sort_using_range", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_sorted", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "avg_rows_sorted", Type: "DECIMAL(21,0)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "first_seen", Type: "TIMESTAMP(6)", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00.000000"),},
			{Name: "last_seen", Type: "TIMESTAMP(6)", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00.000000"),},
			{Name: "digest", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["statements_with_temp_tables"] = &catalog.TableDef{
		Name: "statements_with_temp_tables",
		Columns: []catalog.ColumnDef{
			{Name: "query", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "db", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "exec_count", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "memory_tmp_tables", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "disk_tmp_tables", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "avg_tmp_tables_per_query", Type: "DECIMAL(21,0)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "tmp_tables_to_disk_pct", Type: "DECIMAL(24,0)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "first_seen", Type: "TIMESTAMP(6)", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00.000000"),},
			{Name: "last_seen", Type: "TIMESTAMP(6)", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00.000000"),},
			{Name: "digest", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["sys_config"] = &catalog.TableDef{
		Name: "sys_config",
		Columns: []catalog.ColumnDef{
			{Name: "variable", Type: "VARCHAR(128)", Nullable: false, Default: nil, PrimaryKey: true,},
			{Name: "value", Type: "VARCHAR(128)", Nullable: true, Default: nil,},
			{Name: "set_time", Type: "TIMESTAMP", Nullable: true, Default: sysStrPtr("CURRENT_TIMESTAMP"), OnUpdateCurrentTimestamp: true},
			{Name: "set_by", Type: "VARCHAR(128)", Nullable: true, Default: nil,},
		},
		PrimaryKey: []string{"variable"},
	}
	db.Tables["user_summary"] = &catalog.TableDef{
		Name: "user_summary",
		Columns: []catalog.ColumnDef{
			{Name: "user", Type: "VARCHAR(32)", Nullable: true, Default: nil,},
			{Name: "statements", Type: "DECIMAL(64,0)", Nullable: true, Default: nil,},
			{Name: "statement_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "statement_avg_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "table_scans", Type: "DECIMAL(65,0)", Nullable: true, Default: nil,},
			{Name: "file_ios", Type: "DECIMAL(64,0)", Nullable: true, Default: nil,},
			{Name: "file_io_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "current_connections", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "total_connections", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "unique_hosts", Type: "BIGINT(21)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "current_memory", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "total_memory_allocated", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["user_summary_by_file_io"] = &catalog.TableDef{
		Name: "user_summary_by_file_io",
		Columns: []catalog.ColumnDef{
			{Name: "user", Type: "VARCHAR(32)", Nullable: true, Default: nil,},
			{Name: "ios", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "io_latency", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["user_summary_by_file_io_type"] = &catalog.TableDef{
		Name: "user_summary_by_file_io_type",
		Columns: []catalog.ColumnDef{
			{Name: "user", Type: "VARCHAR(32)", Nullable: true, Default: nil,},
			{Name: "event_name", Type: "VARCHAR(128)", Nullable: false, Default: nil,},
			{Name: "total", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "max_latency", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["user_summary_by_stages"] = &catalog.TableDef{
		Name: "user_summary_by_stages",
		Columns: []catalog.ColumnDef{
			{Name: "user", Type: "VARCHAR(32)", Nullable: true, Default: nil,},
			{Name: "event_name", Type: "VARCHAR(128)", Nullable: false, Default: nil,},
			{Name: "total", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "avg_latency", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["user_summary_by_statement_latency"] = &catalog.TableDef{
		Name: "user_summary_by_statement_latency",
		Columns: []catalog.ColumnDef{
			{Name: "user", Type: "VARCHAR(32)", Nullable: true, Default: nil,},
			{Name: "total", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "total_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "max_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "lock_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "rows_sent", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "rows_examined", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "rows_affected", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "full_scans", Type: "DECIMAL(43,0)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["user_summary_by_statement_type"] = &catalog.TableDef{
		Name: "user_summary_by_statement_type",
		Columns: []catalog.ColumnDef{
			{Name: "user", Type: "VARCHAR(32)", Nullable: true, Default: nil,},
			{Name: "statement", Type: "VARCHAR(128)", Nullable: true, Default: nil,},
			{Name: "total", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "max_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "lock_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "rows_sent", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_examined", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_affected", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "full_scans", Type: "BIGINT(21) UNSIGNED", Nullable: false, Default: sysStrPtr("0"),},
		},
	}
	db.Tables["version"] = &catalog.TableDef{
		Name: "version",
		Columns: []catalog.ColumnDef{
			{Name: "sys_version", Type: "VARCHAR(5)", Nullable: false, Default: nil,},
			{Name: "mysql_version", Type: "VARCHAR(20)", Nullable: false, Default: nil,},
		},
	}
	db.Tables["wait_classes_global_by_avg_latency"] = &catalog.TableDef{
		Name: "wait_classes_global_by_avg_latency",
		Columns: []catalog.ColumnDef{
			{Name: "event_class", Type: "VARCHAR(128)", Nullable: false, Default: sysStrPtr(""),},
			{Name: "total", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "total_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "min_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "avg_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "max_latency", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["wait_classes_global_by_latency"] = &catalog.TableDef{
		Name: "wait_classes_global_by_latency",
		Columns: []catalog.ColumnDef{
			{Name: "event_class", Type: "VARCHAR(128)", Nullable: true, Default: nil,},
			{Name: "total", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "total_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "min_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "avg_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "max_latency", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["waits_by_host_by_latency"] = &catalog.TableDef{
		Name: "waits_by_host_by_latency",
		Columns: []catalog.ColumnDef{
			{Name: "host", Type: "VARCHAR(255)", Nullable: true, Default: nil,},
			{Name: "event", Type: "VARCHAR(128)", Nullable: false, Default: nil,},
			{Name: "total", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "avg_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "max_latency", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["waits_by_user_by_latency"] = &catalog.TableDef{
		Name: "waits_by_user_by_latency",
		Columns: []catalog.ColumnDef{
			{Name: "user", Type: "VARCHAR(32)", Nullable: true, Default: nil,},
			{Name: "event", Type: "VARCHAR(128)", Nullable: false, Default: nil,},
			{Name: "total", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "avg_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "max_latency", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["waits_global_by_latency"] = &catalog.TableDef{
		Name: "waits_global_by_latency",
		Columns: []catalog.ColumnDef{
			{Name: "events", Type: "VARCHAR(128)", Nullable: false, Default: nil,},
			{Name: "total", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "avg_latency", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "max_latency", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$host_summary"] = &catalog.TableDef{
		Name: "x$host_summary",
		Columns: []catalog.ColumnDef{
			{Name: "host", Type: "VARCHAR(255)", Nullable: true, Default: nil,},
			{Name: "statements", Type: "DECIMAL(64,0)", Nullable: true, Default: nil,},
			{Name: "statement_latency", Type: "DECIMAL(64,0)", Nullable: true, Default: nil,},
			{Name: "statement_avg_latency", Type: "DECIMAL(65,4)", Nullable: true, Default: nil,},
			{Name: "table_scans", Type: "DECIMAL(65,0)", Nullable: true, Default: nil,},
			{Name: "file_ios", Type: "DECIMAL(64,0)", Nullable: true, Default: nil,},
			{Name: "file_io_latency", Type: "DECIMAL(64,0)", Nullable: true, Default: nil,},
			{Name: "current_connections", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "total_connections", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "unique_users", Type: "BIGINT(21)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "current_memory", Type: "DECIMAL(63,0)", Nullable: true, Default: nil,},
			{Name: "total_memory_allocated", Type: "DECIMAL(64,0)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$host_summary_by_file_io"] = &catalog.TableDef{
		Name: "x$host_summary_by_file_io",
		Columns: []catalog.ColumnDef{
			{Name: "host", Type: "VARCHAR(255)", Nullable: true, Default: nil,},
			{Name: "ios", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "io_latency", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$host_summary_by_file_io_type"] = &catalog.TableDef{
		Name: "x$host_summary_by_file_io_type",
		Columns: []catalog.ColumnDef{
			{Name: "host", Type: "VARCHAR(255)", Nullable: true, Default: nil,},
			{Name: "event_name", Type: "VARCHAR(128)", Nullable: false, Default: nil,},
			{Name: "total", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "max_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
		},
	}
	db.Tables["x$host_summary_by_stages"] = &catalog.TableDef{
		Name: "x$host_summary_by_stages",
		Columns: []catalog.ColumnDef{
			{Name: "host", Type: "VARCHAR(255)", Nullable: true, Default: nil,},
			{Name: "event_name", Type: "VARCHAR(128)", Nullable: false, Default: nil,},
			{Name: "total", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "avg_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
		},
	}
	db.Tables["x$host_summary_by_statement_latency"] = &catalog.TableDef{
		Name: "x$host_summary_by_statement_latency",
		Columns: []catalog.ColumnDef{
			{Name: "host", Type: "VARCHAR(255)", Nullable: true, Default: nil,},
			{Name: "total", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "total_latency", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "max_latency", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "lock_latency", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "rows_sent", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "rows_examined", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "rows_affected", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "full_scans", Type: "DECIMAL(43,0)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$host_summary_by_statement_type"] = &catalog.TableDef{
		Name: "x$host_summary_by_statement_type",
		Columns: []catalog.ColumnDef{
			{Name: "host", Type: "VARCHAR(255)", Nullable: true, Default: nil,},
			{Name: "statement", Type: "VARCHAR(128)", Nullable: true, Default: nil,},
			{Name: "total", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "max_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "lock_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_sent", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_examined", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_affected", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "full_scans", Type: "BIGINT(21) UNSIGNED", Nullable: false, Default: sysStrPtr("0"),},
		},
	}
	db.Tables["x$innodb_buffer_stats_by_schema"] = &catalog.TableDef{
		Name: "x$innodb_buffer_stats_by_schema",
		Columns: []catalog.ColumnDef{
			{Name: "object_schema", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "allocated", Type: "DECIMAL(43,0)", Nullable: true, Default: nil,},
			{Name: "data", Type: "DECIMAL(43,0)", Nullable: true, Default: nil,},
			{Name: "pages", Type: "BIGINT(21)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "pages_hashed", Type: "BIGINT(21)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "pages_old", Type: "BIGINT(21)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "rows_cached", Type: "DECIMAL(44,0)", Nullable: false, Default: sysStrPtr("0"),},
		},
	}
	db.Tables["x$innodb_buffer_stats_by_table"] = &catalog.TableDef{
		Name: "x$innodb_buffer_stats_by_table",
		Columns: []catalog.ColumnDef{
			{Name: "object_schema", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "object_name", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "allocated", Type: "DECIMAL(43,0)", Nullable: true, Default: nil,},
			{Name: "data", Type: "DECIMAL(43,0)", Nullable: true, Default: nil,},
			{Name: "pages", Type: "BIGINT(21)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "pages_hashed", Type: "BIGINT(21)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "pages_old", Type: "BIGINT(21)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "rows_cached", Type: "DECIMAL(44,0)", Nullable: false, Default: sysStrPtr("0"),},
		},
	}
	db.Tables["x$innodb_lock_waits"] = &catalog.TableDef{
		Name: "x$innodb_lock_waits",
		Columns: []catalog.ColumnDef{
			{Name: "wait_started", Type: "DATETIME", Nullable: true, Default: nil,},
			{Name: "wait_age", Type: "TIME", Nullable: true, Default: nil,},
			{Name: "wait_age_secs", Type: "BIGINT(21)", Nullable: true, Default: nil,},
			{Name: "locked_table", Type: "MEDIUMTEXT", Nullable: true, Default: nil,},
			{Name: "locked_table_schema", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "locked_table_name", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "locked_table_partition", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "locked_table_subpartition", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "locked_index", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "locked_type", Type: "VARCHAR(32)", Nullable: false, Default: nil,},
			{Name: "waiting_trx_id", Type: "VARCHAR(18)", Nullable: false, Default: sysStrPtr(""),},
			{Name: "waiting_trx_started", Type: "DATETIME", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00"),},
			{Name: "waiting_trx_age", Type: "TIME", Nullable: true, Default: nil,},
			{Name: "waiting_trx_rows_locked", Type: "BIGINT(21) UNSIGNED", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "waiting_trx_rows_modified", Type: "BIGINT(21) UNSIGNED", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "waiting_pid", Type: "BIGINT(21) UNSIGNED", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "waiting_query", Type: "VARCHAR(1024)", Nullable: true, Default: nil,},
			{Name: "waiting_lock_id", Type: "VARCHAR(128)", Nullable: false, Default: nil,},
			{Name: "waiting_lock_mode", Type: "VARCHAR(32)", Nullable: false, Default: nil,},
			{Name: "blocking_trx_id", Type: "VARCHAR(18)", Nullable: false, Default: sysStrPtr(""),},
			{Name: "blocking_pid", Type: "BIGINT(21) UNSIGNED", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "blocking_query", Type: "VARCHAR(1024)", Nullable: true, Default: nil,},
			{Name: "blocking_lock_id", Type: "VARCHAR(128)", Nullable: false, Default: nil,},
			{Name: "blocking_lock_mode", Type: "VARCHAR(32)", Nullable: false, Default: nil,},
			{Name: "blocking_trx_started", Type: "DATETIME", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00"),},
			{Name: "blocking_trx_age", Type: "TIME", Nullable: true, Default: nil,},
			{Name: "blocking_trx_rows_locked", Type: "BIGINT(21) UNSIGNED", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "blocking_trx_rows_modified", Type: "BIGINT(21) UNSIGNED", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "sql_kill_blocking_query", Type: "VARCHAR(32)", Nullable: false, Default: sysStrPtr(""),},
			{Name: "sql_kill_blocking_connection", Type: "VARCHAR(26)", Nullable: false, Default: sysStrPtr(""),},
		},
	}
	db.Tables["x$io_by_thread_by_latency"] = &catalog.TableDef{
		Name: "x$io_by_thread_by_latency",
		Columns: []catalog.ColumnDef{
			{Name: "user", Type: "VARCHAR(288)", Nullable: true, Default: nil,},
			{Name: "total", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "total_latency", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "min_latency", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "avg_latency", Type: "DECIMAL(24,4)", Nullable: true, Default: nil,},
			{Name: "max_latency", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "thread_id", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "processlist_id", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$io_global_by_file_by_bytes"] = &catalog.TableDef{
		Name: "x$io_global_by_file_by_bytes",
		Columns: []catalog.ColumnDef{
			{Name: "file", Type: "VARCHAR(512)", Nullable: false, Default: nil,},
			{Name: "count_read", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_read", Type: "BIGINT(20)", Nullable: false, Default: nil,},
			{Name: "avg_read", Type: "DECIMAL(23,4)", Nullable: false, Default: sysStrPtr("0.0000"),},
			{Name: "count_write", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_written", Type: "BIGINT(20)", Nullable: false, Default: nil,},
			{Name: "avg_write", Type: "DECIMAL(23,4)", Nullable: false, Default: sysStrPtr("0.0000"),},
			{Name: "total", Type: "BIGINT(21)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "write_pct", Type: "DECIMAL(26,2)", Nullable: false, Default: sysStrPtr("0.00"),},
		},
	}
	db.Tables["x$io_global_by_file_by_latency"] = &catalog.TableDef{
		Name: "x$io_global_by_file_by_latency",
		Columns: []catalog.ColumnDef{
			{Name: "file", Type: "VARCHAR(512)", Nullable: false, Default: nil,},
			{Name: "total", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "count_read", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "read_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "count_write", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "write_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "count_misc", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "misc_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
		},
	}
	db.Tables["x$io_global_by_wait_by_bytes"] = &catalog.TableDef{
		Name: "x$io_global_by_wait_by_bytes",
		Columns: []catalog.ColumnDef{
			{Name: "event_name", Type: "VARCHAR(128)", Nullable: true, Default: nil,},
			{Name: "total", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "min_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "avg_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "max_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "count_read", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_read", Type: "BIGINT(20)", Nullable: false, Default: nil,},
			{Name: "avg_read", Type: "DECIMAL(23,4)", Nullable: false, Default: sysStrPtr("0.0000"),},
			{Name: "count_write", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_written", Type: "BIGINT(20)", Nullable: false, Default: nil,},
			{Name: "avg_written", Type: "DECIMAL(23,4)", Nullable: false, Default: sysStrPtr("0.0000"),},
			{Name: "total_requested", Type: "BIGINT(21)", Nullable: false, Default: sysStrPtr("0"),},
		},
	}
	db.Tables["x$io_global_by_wait_by_latency"] = &catalog.TableDef{
		Name: "x$io_global_by_wait_by_latency",
		Columns: []catalog.ColumnDef{
			{Name: "event_name", Type: "VARCHAR(128)", Nullable: true, Default: nil,},
			{Name: "total", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "avg_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "max_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "read_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "write_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "misc_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "count_read", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_read", Type: "BIGINT(20)", Nullable: false, Default: nil,},
			{Name: "avg_read", Type: "DECIMAL(23,4)", Nullable: false, Default: sysStrPtr("0.0000"),},
			{Name: "count_write", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_written", Type: "BIGINT(20)", Nullable: false, Default: nil,},
			{Name: "avg_written", Type: "DECIMAL(23,4)", Nullable: false, Default: sysStrPtr("0.0000"),},
		},
	}
	db.Tables["x$latest_file_io"] = &catalog.TableDef{
		Name: "x$latest_file_io",
		Columns: []catalog.ColumnDef{
			{Name: "thread", Type: "VARCHAR(316)", Nullable: true, Default: nil,},
			{Name: "file", Type: "VARCHAR(512)", Nullable: true, Default: nil,},
			{Name: "latency", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "operation", Type: "VARCHAR(32)", Nullable: false, Default: nil,},
			{Name: "requested", Type: "BIGINT(20)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$memory_by_host_by_current_bytes"] = &catalog.TableDef{
		Name: "x$memory_by_host_by_current_bytes",
		Columns: []catalog.ColumnDef{
			{Name: "host", Type: "VARCHAR(255)", Nullable: true, Default: nil,},
			{Name: "current_count_used", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "current_allocated", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "current_avg_alloc", Type: "DECIMAL(45,4)", Nullable: false, Default: sysStrPtr("0.0000"),},
			{Name: "current_max_alloc", Type: "BIGINT(20)", Nullable: true, Default: nil,},
			{Name: "total_allocated", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$memory_by_thread_by_current_bytes"] = &catalog.TableDef{
		Name: "x$memory_by_thread_by_current_bytes",
		Columns: []catalog.ColumnDef{
			{Name: "thread_id", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "user", Type: "VARCHAR(288)", Nullable: true, Default: nil,},
			{Name: "current_count_used", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "current_allocated", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "current_avg_alloc", Type: "DECIMAL(45,4)", Nullable: false, Default: sysStrPtr("0.0000"),},
			{Name: "current_max_alloc", Type: "BIGINT(20)", Nullable: true, Default: nil,},
			{Name: "total_allocated", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$memory_by_user_by_current_bytes"] = &catalog.TableDef{
		Name: "x$memory_by_user_by_current_bytes",
		Columns: []catalog.ColumnDef{
			{Name: "user", Type: "VARCHAR(32)", Nullable: true, Default: nil,},
			{Name: "current_count_used", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "current_allocated", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "current_avg_alloc", Type: "DECIMAL(45,4)", Nullable: false, Default: sysStrPtr("0.0000"),},
			{Name: "current_max_alloc", Type: "BIGINT(20)", Nullable: true, Default: nil,},
			{Name: "total_allocated", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$memory_global_by_current_bytes"] = &catalog.TableDef{
		Name: "x$memory_global_by_current_bytes",
		Columns: []catalog.ColumnDef{
			{Name: "event_name", Type: "VARCHAR(128)", Nullable: false, Default: nil,},
			{Name: "current_count", Type: "BIGINT(20)", Nullable: false, Default: nil,},
			{Name: "current_alloc", Type: "BIGINT(20)", Nullable: false, Default: nil,},
			{Name: "current_avg_alloc", Type: "DECIMAL(23,4)", Nullable: false, Default: sysStrPtr("0.0000"),},
			{Name: "high_count", Type: "BIGINT(20)", Nullable: false, Default: nil,},
			{Name: "high_alloc", Type: "BIGINT(20)", Nullable: false, Default: nil,},
			{Name: "high_avg_alloc", Type: "DECIMAL(23,4)", Nullable: false, Default: sysStrPtr("0.0000"),},
		},
	}
	db.Tables["x$memory_global_total"] = &catalog.TableDef{
		Name: "x$memory_global_total",
		Columns: []catalog.ColumnDef{
			{Name: "total_allocated", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$processlist"] = &catalog.TableDef{
		Name: "x$processlist",
		Columns: []catalog.ColumnDef{
			{Name: "thd_id", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "conn_id", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "user", Type: "VARCHAR(288)", Nullable: true, Default: nil,},
			{Name: "db", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "command", Type: "VARCHAR(16)", Nullable: true, Default: nil,},
			{Name: "state", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "time", Type: "BIGINT(20)", Nullable: true, Default: nil,},
			{Name: "current_statement", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "statement_latency", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "progress", Type: "DECIMAL(26,2)", Nullable: true, Default: nil,},
			{Name: "lock_latency", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "rows_examined", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "rows_sent", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "rows_affected", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "tmp_tables", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "tmp_disk_tables", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "full_scan", Type: "VARCHAR(3)", Nullable: false, Default: sysStrPtr(""),},
			{Name: "last_statement", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "last_statement_latency", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "current_memory", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "last_wait", Type: "VARCHAR(128)", Nullable: true, Default: nil,},
			{Name: "last_wait_latency", Type: "VARCHAR(20)", Nullable: true, Default: nil,},
			{Name: "source", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "trx_latency", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "trx_state", Type: "ENUM('ACTIVE','COMMITTED','ROLLED BACK')", Nullable: true, Default: nil,},
			{Name: "trx_autocommit", Type: "ENUM('YES','NO')", Nullable: true, Default: nil,},
			{Name: "pid", Type: "VARCHAR(1024)", Nullable: true, Default: nil,},
			{Name: "program_name", Type: "VARCHAR(1024)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$ps_digest_95th_percentile_by_avg_us"] = &catalog.TableDef{
		Name: "x$ps_digest_95th_percentile_by_avg_us",
		Columns: []catalog.ColumnDef{
			{Name: "avg_us", Type: "DECIMAL(21,0)", Nullable: true, Default: nil,},
			{Name: "percentile", Type: "DECIMAL(46,4)", Nullable: false, Default: sysStrPtr("0.0000"),},
		},
	}
	db.Tables["x$ps_digest_avg_latency_distribution"] = &catalog.TableDef{
		Name: "x$ps_digest_avg_latency_distribution",
		Columns: []catalog.ColumnDef{
			{Name: "cnt", Type: "BIGINT(21)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "avg_us", Type: "DECIMAL(21,0)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$ps_schema_table_statistics_io"] = &catalog.TableDef{
		Name: "x$ps_schema_table_statistics_io",
		Columns: []catalog.ColumnDef{
			{Name: "table_schema", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "table_name", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "count_read", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "sum_number_of_bytes_read", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "sum_timer_read", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "count_write", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "sum_number_of_bytes_write", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "sum_timer_write", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "count_misc", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "sum_timer_misc", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$schema_flattened_keys"] = &catalog.TableDef{
		Name: "x$schema_flattened_keys",
		Columns: []catalog.ColumnDef{
			{Name: "table_schema", Type: "VARCHAR(64)", Nullable: false, Default: nil,},
			{Name: "table_name", Type: "VARCHAR(64)", Nullable: false, Default: nil,},
			{Name: "index_name", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "non_unique", Type: "BIGINT", Nullable: true, Default: nil,},
			{Name: "subpart_exists", Type: "INT", Nullable: false, Default: nil,},
			{Name: "index_columns", Type: "TEXT", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$schema_index_statistics"] = &catalog.TableDef{
		Name: "x$schema_index_statistics",
		Columns: []catalog.ColumnDef{
			{Name: "table_schema", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "table_name", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "index_name", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "rows_selected", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "select_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_inserted", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "insert_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_updated", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "update_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_deleted", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "delete_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
		},
	}
	db.Tables["x$schema_table_lock_waits"] = &catalog.TableDef{
		Name: "x$schema_table_lock_waits",
		Columns: []catalog.ColumnDef{
			{Name: "object_schema", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "object_name", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "waiting_thread_id", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "waiting_pid", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "waiting_account", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "waiting_lock_type", Type: "VARCHAR(32)", Nullable: false, Default: nil,},
			{Name: "waiting_lock_duration", Type: "VARCHAR(32)", Nullable: false, Default: nil,},
			{Name: "waiting_query", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "waiting_query_secs", Type: "BIGINT(20)", Nullable: true, Default: nil,},
			{Name: "waiting_query_rows_affected", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "waiting_query_rows_examined", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "blocking_thread_id", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "blocking_pid", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "blocking_account", Type: "TEXT", Nullable: true, Default: nil,},
			{Name: "blocking_lock_type", Type: "VARCHAR(32)", Nullable: false, Default: nil,},
			{Name: "blocking_lock_duration", Type: "VARCHAR(32)", Nullable: false, Default: nil,},
			{Name: "sql_kill_blocking_query", Type: "VARCHAR(31)", Nullable: true, Default: nil,},
			{Name: "sql_kill_blocking_connection", Type: "VARCHAR(25)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$schema_table_statistics"] = &catalog.TableDef{
		Name: "x$schema_table_statistics",
		Columns: []catalog.ColumnDef{
			{Name: "table_schema", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "table_name", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "total_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_fetched", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "fetch_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_inserted", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "insert_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_updated", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "update_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_deleted", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "delete_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "io_read_requests", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "io_read", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "io_read_latency", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "io_write_requests", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "io_write", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "io_write_latency", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "io_misc_requests", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "io_misc_latency", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$schema_table_statistics_with_buffer"] = &catalog.TableDef{
		Name: "x$schema_table_statistics_with_buffer",
		Columns: []catalog.ColumnDef{
			{Name: "table_schema", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "table_name", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "rows_fetched", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "fetch_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_inserted", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "insert_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_updated", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "update_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_deleted", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "delete_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "io_read_requests", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "io_read", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "io_read_latency", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "io_write_requests", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "io_write", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "io_write_latency", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "io_misc_requests", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "io_misc_latency", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "innodb_buffer_allocated", Type: "DECIMAL(43,0)", Nullable: true, Default: nil,},
			{Name: "innodb_buffer_data", Type: "DECIMAL(43,0)", Nullable: true, Default: nil,},
			{Name: "innodb_buffer_free", Type: "DECIMAL(44,0)", Nullable: true, Default: nil,},
			{Name: "innodb_buffer_pages", Type: "BIGINT(21)", Nullable: true, Default: sysStrPtr("0"),},
			{Name: "innodb_buffer_pages_hashed", Type: "BIGINT(21)", Nullable: true, Default: sysStrPtr("0"),},
			{Name: "innodb_buffer_pages_old", Type: "BIGINT(21)", Nullable: true, Default: sysStrPtr("0"),},
			{Name: "innodb_buffer_rows_cached", Type: "DECIMAL(44,0)", Nullable: true, Default: sysStrPtr("0"),},
		},
	}
	db.Tables["x$schema_tables_with_full_table_scans"] = &catalog.TableDef{
		Name: "x$schema_tables_with_full_table_scans",
		Columns: []catalog.ColumnDef{
			{Name: "object_schema", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "object_name", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "rows_full_scanned", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
		},
	}
	db.Tables["x$session"] = &catalog.TableDef{
		Name: "x$session",
		Columns: []catalog.ColumnDef{
			{Name: "thd_id", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "conn_id", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "user", Type: "VARCHAR(288)", Nullable: true, Default: nil,},
			{Name: "db", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "command", Type: "VARCHAR(16)", Nullable: true, Default: nil,},
			{Name: "state", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "time", Type: "BIGINT(20)", Nullable: true, Default: nil,},
			{Name: "current_statement", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "statement_latency", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "progress", Type: "DECIMAL(26,2)", Nullable: true, Default: nil,},
			{Name: "lock_latency", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "rows_examined", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "rows_sent", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "rows_affected", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "tmp_tables", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "tmp_disk_tables", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "full_scan", Type: "VARCHAR(3)", Nullable: false, Default: sysStrPtr(""),},
			{Name: "last_statement", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "last_statement_latency", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "current_memory", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "last_wait", Type: "VARCHAR(128)", Nullable: true, Default: nil,},
			{Name: "last_wait_latency", Type: "VARCHAR(20)", Nullable: true, Default: nil,},
			{Name: "source", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "trx_latency", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "trx_state", Type: "ENUM('ACTIVE','COMMITTED','ROLLED BACK')", Nullable: true, Default: nil,},
			{Name: "trx_autocommit", Type: "ENUM('YES','NO')", Nullable: true, Default: nil,},
			{Name: "pid", Type: "VARCHAR(1024)", Nullable: true, Default: nil,},
			{Name: "program_name", Type: "VARCHAR(1024)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$statement_analysis"] = &catalog.TableDef{
		Name: "x$statement_analysis",
		Columns: []catalog.ColumnDef{
			{Name: "query", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "db", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "full_scan", Type: "VARCHAR(1)", Nullable: false, Default: sysStrPtr(""),},
			{Name: "exec_count", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "err_count", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "warn_count", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "max_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "avg_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "lock_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_sent", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_sent_avg", Type: "DECIMAL(21,0)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "rows_examined", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_examined_avg", Type: "DECIMAL(21,0)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "rows_affected", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_affected_avg", Type: "DECIMAL(21,0)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "tmp_tables", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "tmp_disk_tables", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_sorted", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "sort_merge_passes", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "digest", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "first_seen", Type: "TIMESTAMP(6)", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00.000000"),},
			{Name: "last_seen", Type: "TIMESTAMP(6)", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00.000000"),},
		},
	}
	db.Tables["x$statements_with_errors_or_warnings"] = &catalog.TableDef{
		Name: "x$statements_with_errors_or_warnings",
		Columns: []catalog.ColumnDef{
			{Name: "query", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "db", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "exec_count", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "errors", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "error_pct", Type: "DECIMAL(27,4)", Nullable: false, Default: sysStrPtr("0.0000"),},
			{Name: "warnings", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "warning_pct", Type: "DECIMAL(27,4)", Nullable: false, Default: sysStrPtr("0.0000"),},
			{Name: "first_seen", Type: "TIMESTAMP(6)", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00.000000"),},
			{Name: "last_seen", Type: "TIMESTAMP(6)", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00.000000"),},
			{Name: "digest", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$statements_with_full_table_scans"] = &catalog.TableDef{
		Name: "x$statements_with_full_table_scans",
		Columns: []catalog.ColumnDef{
			{Name: "query", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "db", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "exec_count", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "no_index_used_count", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "no_good_index_used_count", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "no_index_used_pct", Type: "DECIMAL(24,0)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "rows_sent", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_examined", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_sent_avg", Type: "DECIMAL(21,0) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "rows_examined_avg", Type: "DECIMAL(21,0) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "first_seen", Type: "TIMESTAMP(6)", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00.000000"),},
			{Name: "last_seen", Type: "TIMESTAMP(6)", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00.000000"),},
			{Name: "digest", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$statements_with_runtimes_in_95th_percentile"] = &catalog.TableDef{
		Name: "x$statements_with_runtimes_in_95th_percentile",
		Columns: []catalog.ColumnDef{
			{Name: "query", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "db", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "full_scan", Type: "VARCHAR(1)", Nullable: false, Default: sysStrPtr(""),},
			{Name: "exec_count", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "err_count", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "warn_count", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "max_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "avg_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_sent", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_sent_avg", Type: "DECIMAL(21,0)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "rows_examined", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_examined_avg", Type: "DECIMAL(21,0)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "first_seen", Type: "TIMESTAMP(6)", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00.000000"),},
			{Name: "last_seen", Type: "TIMESTAMP(6)", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00.000000"),},
			{Name: "digest", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$statements_with_sorting"] = &catalog.TableDef{
		Name: "x$statements_with_sorting",
		Columns: []catalog.ColumnDef{
			{Name: "query", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "db", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "exec_count", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "sort_merge_passes", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "avg_sort_merges", Type: "DECIMAL(21,0)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "sorts_using_scans", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "sort_using_range", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_sorted", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "avg_rows_sorted", Type: "DECIMAL(21,0)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "first_seen", Type: "TIMESTAMP(6)", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00.000000"),},
			{Name: "last_seen", Type: "TIMESTAMP(6)", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00.000000"),},
			{Name: "digest", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$statements_with_temp_tables"] = &catalog.TableDef{
		Name: "x$statements_with_temp_tables",
		Columns: []catalog.ColumnDef{
			{Name: "query", Type: "LONGTEXT", Nullable: true, Default: nil,},
			{Name: "db", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
			{Name: "exec_count", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "memory_tmp_tables", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "disk_tmp_tables", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "avg_tmp_tables_per_query", Type: "DECIMAL(21,0)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "tmp_tables_to_disk_pct", Type: "DECIMAL(24,0)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "first_seen", Type: "TIMESTAMP(6)", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00.000000"),},
			{Name: "last_seen", Type: "TIMESTAMP(6)", Nullable: false, Default: sysStrPtr("0000-00-00 00:00:00.000000"),},
			{Name: "digest", Type: "VARCHAR(64)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$user_summary"] = &catalog.TableDef{
		Name: "x$user_summary",
		Columns: []catalog.ColumnDef{
			{Name: "user", Type: "VARCHAR(32)", Nullable: true, Default: nil,},
			{Name: "statements", Type: "DECIMAL(64,0)", Nullable: true, Default: nil,},
			{Name: "statement_latency", Type: "DECIMAL(64,0)", Nullable: true, Default: nil,},
			{Name: "statement_avg_latency", Type: "DECIMAL(65,4)", Nullable: false, Default: sysStrPtr("0.0000"),},
			{Name: "table_scans", Type: "DECIMAL(65,0)", Nullable: true, Default: nil,},
			{Name: "file_ios", Type: "DECIMAL(64,0)", Nullable: true, Default: nil,},
			{Name: "file_io_latency", Type: "DECIMAL(64,0)", Nullable: true, Default: nil,},
			{Name: "current_connections", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "total_connections", Type: "DECIMAL(41,0)", Nullable: true, Default: nil,},
			{Name: "unique_hosts", Type: "BIGINT(21)", Nullable: false, Default: sysStrPtr("0"),},
			{Name: "current_memory", Type: "DECIMAL(63,0)", Nullable: true, Default: nil,},
			{Name: "total_memory_allocated", Type: "DECIMAL(64,0)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$user_summary_by_file_io"] = &catalog.TableDef{
		Name: "x$user_summary_by_file_io",
		Columns: []catalog.ColumnDef{
			{Name: "user", Type: "VARCHAR(32)", Nullable: true, Default: nil,},
			{Name: "ios", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "io_latency", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$user_summary_by_file_io_type"] = &catalog.TableDef{
		Name: "x$user_summary_by_file_io_type",
		Columns: []catalog.ColumnDef{
			{Name: "user", Type: "VARCHAR(32)", Nullable: true, Default: nil,},
			{Name: "event_name", Type: "VARCHAR(128)", Nullable: false, Default: nil,},
			{Name: "total", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "max_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
		},
	}
	db.Tables["x$user_summary_by_stages"] = &catalog.TableDef{
		Name: "x$user_summary_by_stages",
		Columns: []catalog.ColumnDef{
			{Name: "user", Type: "VARCHAR(32)", Nullable: true, Default: nil,},
			{Name: "event_name", Type: "VARCHAR(128)", Nullable: false, Default: nil,},
			{Name: "total", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "avg_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
		},
	}
	db.Tables["x$user_summary_by_statement_latency"] = &catalog.TableDef{
		Name: "x$user_summary_by_statement_latency",
		Columns: []catalog.ColumnDef{
			{Name: "user", Type: "VARCHAR(32)", Nullable: true, Default: nil,},
			{Name: "total", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "total_latency", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "max_latency", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "lock_latency", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "rows_sent", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "rows_examined", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "rows_affected", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "full_scans", Type: "DECIMAL(43,0)", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$user_summary_by_statement_type"] = &catalog.TableDef{
		Name: "x$user_summary_by_statement_type",
		Columns: []catalog.ColumnDef{
			{Name: "user", Type: "VARCHAR(32)", Nullable: true, Default: nil,},
			{Name: "statement", Type: "VARCHAR(128)", Nullable: true, Default: nil,},
			{Name: "total", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "max_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "lock_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_sent", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_examined", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "rows_affected", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "full_scans", Type: "BIGINT(21) UNSIGNED", Nullable: false, Default: sysStrPtr("0"),},
		},
	}
	db.Tables["x$wait_classes_global_by_avg_latency"] = &catalog.TableDef{
		Name: "x$wait_classes_global_by_avg_latency",
		Columns: []catalog.ColumnDef{
			{Name: "event_class", Type: "VARCHAR(128)", Nullable: false, Default: sysStrPtr(""),},
			{Name: "total", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "total_latency", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "min_latency", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "avg_latency", Type: "DECIMAL(46,4)", Nullable: false, Default: sysStrPtr("0.0000"),},
			{Name: "max_latency", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$wait_classes_global_by_latency"] = &catalog.TableDef{
		Name: "x$wait_classes_global_by_latency",
		Columns: []catalog.ColumnDef{
			{Name: "event_class", Type: "VARCHAR(128)", Nullable: true, Default: nil,},
			{Name: "total", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "total_latency", Type: "DECIMAL(42,0)", Nullable: true, Default: nil,},
			{Name: "min_latency", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
			{Name: "avg_latency", Type: "DECIMAL(46,4)", Nullable: false, Default: sysStrPtr("0.0000"),},
			{Name: "max_latency", Type: "BIGINT(20) UNSIGNED", Nullable: true, Default: nil,},
		},
	}
	db.Tables["x$waits_by_host_by_latency"] = &catalog.TableDef{
		Name: "x$waits_by_host_by_latency",
		Columns: []catalog.ColumnDef{
			{Name: "host", Type: "VARCHAR(255)", Nullable: true, Default: nil,},
			{Name: "event", Type: "VARCHAR(128)", Nullable: false, Default: nil,},
			{Name: "total", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "avg_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "max_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
		},
	}
	db.Tables["x$waits_by_user_by_latency"] = &catalog.TableDef{
		Name: "x$waits_by_user_by_latency",
		Columns: []catalog.ColumnDef{
			{Name: "user", Type: "VARCHAR(32)", Nullable: true, Default: nil,},
			{Name: "event", Type: "VARCHAR(128)", Nullable: false, Default: nil,},
			{Name: "total", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "avg_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "max_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
		},
	}
	db.Tables["x$waits_global_by_latency"] = &catalog.TableDef{
		Name: "x$waits_global_by_latency",
		Columns: []catalog.ColumnDef{
			{Name: "events", Type: "VARCHAR(128)", Nullable: false, Default: nil,},
			{Name: "total", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "total_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "avg_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
			{Name: "max_latency", Type: "BIGINT(20) UNSIGNED", Nullable: false, Default: nil,},
		},
	}

	// Register sys schema functions (stubs for INFORMATION_SCHEMA.ROUTINES)
	sysFunctions := []string{
		"extract_schema_from_file_name",
		"extract_table_from_file_name",
		"format_bytes",
		"format_path",
		"format_statement",
		"format_time",
		"list_add",
		"list_drop",
		"ps_is_account_enabled",
		"ps_is_consumer_enabled",
		"ps_is_instrument_default_enabled",
		"ps_is_instrument_default_timed",
		"ps_is_thread_instrumented",
		"ps_thread_account",
		"ps_thread_id",
		"ps_thread_stack",
		"ps_thread_trx_info",
		"quote_identifier",
		"sys_get_config",
		"version_major",
		"version_minor",
		"version_patch",
	}
	for _, name := range sysFunctions {
		db.CreateFunction(&catalog.FunctionDef{
			Name:       name,
			ReturnType: "VARCHAR(255)",
			Body:       []string{"SELECT NULL"},
		})
	}

	// Register sys schema procedures (stubs)
	sysProcedures := []string{
		"create_synonym_db",
		"diagnostics",
		"execute_prepared_stmt",
		"ps_setup_disable_background_threads",
		"ps_setup_disable_consumer",
		"ps_setup_disable_instrument",
		"ps_setup_disable_thread",
		"ps_setup_enable_background_threads",
		"ps_setup_enable_consumer",
		"ps_setup_enable_instrument",
		"ps_setup_enable_thread",
		"ps_setup_reload_saved",
		"ps_setup_reset_to_default",
		"ps_setup_save",
		"ps_setup_show_disabled",
		"ps_setup_show_disabled_consumers",
		"ps_setup_show_disabled_instruments",
		"ps_setup_show_enabled",
		"ps_setup_show_enabled_consumers",
		"ps_setup_show_enabled_instruments",
		"ps_statement_avg_latency_histogram",
		"ps_trace_statement_digest",
		"ps_trace_thread",
		"ps_truncate_all_tables",
		"statement_performance_analyzer",
		"table_exists",
	}
	for _, name := range sysProcedures {
		db.CreateProcedure(&catalog.ProcedureDef{
			Name: name,
			Body: []string{"SELECT 1"},
		})
	}

	// Register sys schema triggers (stubs)
	db.CreateTrigger(&catalog.TriggerDef{
		Name:   "sys_config_insert_set_user",
		Timing: "BEFORE",
		Event:  "INSERT",
		Table:  "sys_config",
		Body:   []string{"SET NEW.set_by = 'root@localhost'"},
	})
	db.CreateTrigger(&catalog.TriggerDef{
		Name:   "sys_config_update_set_user",
		Timing: "BEFORE",
		Event:  "UPDATE",
		Table:  "sys_config",
		Body:   []string{"SET NEW.set_by = 'root@localhost'"},
	})
}

func sysStrPtr(s string) *string {
	return &s
}

// evalSysSchemaFunc evaluates built-in sys schema functions.
// Returns (result, handled, error). If handled is false, the caller should try other dispatch paths.
func (e *Executor) evalSysSchemaFunc(name string, args []sqlparser.Expr) (interface{}, bool, error) {
	switch strings.ToLower(name) {
	case "quote_identifier":
		if len(args) < 1 {
			return nil, true, nil
		}
		val, err := e.evalExpr(args[0])
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
		}
		s := toString(val)
		escaped := strings.ReplaceAll(s, "`", "``")
		return "`" + escaped + "`", true, nil

	case "format_bytes":
		if len(args) < 1 {
			return nil, true, nil
		}
		val, err := e.evalExpr(args[0])
		if err != nil {
			var intOvErr *intOverflowError
			if errors.As(err, &intOvErr) {
				// Large integer overflow: parse the string value as float
				f, _ := strconv.ParseFloat(intOvErr.val, 64)
				return sysFormatBytes(f), true, nil
			}
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
		}
		bytes := toFloat(val)
		return sysFormatBytes(bytes), true, nil

	case "format_time":
		if len(args) < 1 {
			return nil, true, nil
		}
		val, err := e.evalExpr(args[0])
		if err != nil {
			var intOvErr *intOverflowError
			if errors.As(err, &intOvErr) {
				// Large integer overflow: convert to float64 to match MySQL's float arithmetic
				bf := new(big.Float).SetPrec(256)
				bf.SetString(intOvErr.val)
				f, _ := bf.Float64()
				return sysFormatTime(f), true, nil
			}
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
		}
		picos := toFloat(val)
		return sysFormatTime(picos), true, nil

	case "extract_schema_from_file_name":
		if len(args) < 1 {
			return nil, true, nil
		}
		val, err := e.evalExpr(args[0])
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
		}
		s := toString(val)
		parts := strings.Split(strings.ReplaceAll(s, "\\", "/"), "/")
		if len(parts) >= 2 {
			return parts[len(parts)-2], true, nil
		}
		return nil, true, nil

	case "extract_table_from_file_name":
		if len(args) < 1 {
			return nil, true, nil
		}
		val, err := e.evalExpr(args[0])
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
		}
		s := toString(val)
		parts := strings.Split(strings.ReplaceAll(s, "\\", "/"), "/")
		if len(parts) >= 1 {
			tablePart := parts[len(parts)-1]
			if idx := strings.LastIndex(tablePart, "."); idx >= 0 {
				tablePart = tablePart[:idx]
			}
			return tablePart, true, nil
		}
		return nil, true, nil

	case "version_major":
		major, _, _ := e.parseMySQLVersion()
		return int64(major), true, nil
	case "version_minor":
		_, minor, _ := e.parseMySQLVersion()
		return int64(minor), true, nil
	case "version_patch":
		_, _, patch := e.parseMySQLVersion()
		return int64(patch), true, nil

	case "sys_get_config":
		// sys_get_config(variable_name, default_value): look up sys.sys_config table.
		// Return the configured value if found, otherwise return the default.
		if len(args) < 1 {
			return nil, true, nil
		}
		nameVal, err := e.evalExpr(args[0])
		if err != nil {
			return nil, true, err
		}
		var defaultVal interface{}
		if len(args) >= 2 {
			defaultVal, err = e.evalExpr(args[1])
			if err != nil {
				return nil, true, err
			}
		}
		paramName := toString(nameVal)
		// Hardcoded defaults matching MySQL's sys_config table defaults
		sysConfigDefaults := map[string]interface{}{
			"diagnostics.allow_i_s_tables":               "OFF",
			"diagnostics.include_raw":                    "OFF",
			"ps_thread_trx_info.max_length":              int64(65535),
			"statement_performance_analyzer.limit":       int64(100),
			"statement_performance_analyzer.view":        nil,
			"statement_truncate_len":                     int64(64),
			"sys.diagnostics.allow_i_s_tables":           "OFF",
			"sys.diagnostics.include_raw":                "OFF",
			"sys.ps_thread_trx_info.max_length":          int64(65535),
			"sys.statement_performance_analyzer.limit":   int64(100),
			"sys.statement_performance_analyzer.view":    nil,
			"sys.statement_truncate_len":                 int64(64),
		}
		if val, ok := sysConfigDefaults[paramName]; ok {
			return val, true, nil
		}
		return defaultVal, true, nil

	default:
		return nil, false, nil
	}
}

func (e *Executor) parseMySQLVersion() (int, int, int) {
	ver, _ := e.getSysVar("version")
	if ver == "" {
		return 8, 4, 0
	}
	if idx := strings.IndexByte(ver, '-'); idx >= 0 {
		ver = ver[:idx]
	}
	parts := strings.SplitN(ver, ".", 3)
	var major, minor, patch int
	if len(parts) >= 1 {
		fmt.Sscanf(parts[0], "%d", &major)
	}
	if len(parts) >= 2 {
		fmt.Sscanf(parts[1], "%d", &minor)
	}
	if len(parts) >= 3 {
		fmt.Sscanf(parts[2], "%d", &patch)
	}
	return major, minor, patch
}

func sysFormatBytes(bytes float64) string {
	if bytes < 0 {
		return fmt.Sprintf("%.0f bytes", bytes)
	}
	units := []struct {
		threshold float64
		unit      string
	}{
		{1125899906842624, "PiB"},
		{1099511627776, "TiB"},
		{1073741824, "GiB"},
		{1048576, "MiB"},
		{1024, "KiB"},
	}
	for _, u := range units {
		if bytes >= u.threshold {
			return fmt.Sprintf("%.2f %s", bytes/u.threshold, u.unit)
		}
	}
	return fmt.Sprintf("%.0f bytes", bytes)
}

// formatFloat2 formats a float64 value with 2 decimal places using MySQL-compatible
// precision (17 significant digits), which matches how MySQL's libc formats large floats.
func formatFloat2(f float64) string {
	if f == 0 {
		return "0.00"
	}
	negative := f < 0
	if negative {
		f = -f
	}
	// Use 17 significant digits (prec=16 in 'e' format) to match MySQL's double formatting
	s := strconv.FormatFloat(f, 'e', 16, 64)
	parts := strings.SplitN(s, "e", 2)
	if len(parts) != 2 {
		result := fmt.Sprintf("%.2f", f)
		if negative {
			return "-" + result
		}
		return result
	}
	mantStr := parts[0]
	exp, err := strconv.Atoi(parts[1])
	if err != nil {
		result := fmt.Sprintf("%.2f", f)
		if negative {
			return "-" + result
		}
		return result
	}
	// Remove decimal point from mantissa to get all digits
	mantDigits := strings.Replace(mantStr, ".", "", 1)
	// intDigits = number of digits before the decimal point
	intDigits := exp + 1
	var intStr, fracStr string
	if intDigits >= len(mantDigits) {
		intStr = mantDigits + strings.Repeat("0", intDigits-len(mantDigits))
		fracStr = "00"
	} else if intDigits > 0 {
		intStr = mantDigits[:intDigits]
		remaining := mantDigits[intDigits:]
		if len(remaining) >= 2 {
			d1 := remaining[0] - '0'
			d2 := remaining[1] - '0'
			carry := byte(0)
			if len(remaining) > 2 && remaining[2]-'0' >= 5 {
				carry = 1
			}
			d2 += carry
			if d2 >= 10 {
				d2 -= 10
				d1++
			}
			if d1 >= 10 {
				// Carry overflows into integer part, fall back to standard formatting
				result := fmt.Sprintf("%.2f", f)
				if negative {
					return "-" + result
				}
				return result
			}
			fracStr = fmt.Sprintf("%d%d", d1, d2)
		} else if len(remaining) == 1 {
			fracStr = string(remaining[0]) + "0"
		} else {
			fracStr = "00"
		}
	} else {
		// |f| < 1
		zeros := -intDigits
		allFrac := strings.Repeat("0", zeros) + mantDigits
		if len(allFrac) >= 2 {
			fracStr = allFrac[:2]
		} else {
			fracStr = allFrac + strings.Repeat("0", 2-len(allFrac))
		}
		intStr = "0"
	}
	result := intStr + "." + fracStr
	if negative {
		return "-" + result
	}
	return result
}

func sysFormatTime(picos float64) string {
	if picos < 0 {
		return fmt.Sprintf("%.0f ps", picos)
	}
	units := []struct {
		threshold float64
		unit      string
	}{
		{604800000000000000, "w"},
		{86400000000000000, "d"},
		{3600000000000000, "h"},
		{60000000000000, "m"},
		{1000000000000, "s"},
		{1000000000, "ms"},
		{1000000, "us"},
		{1000, "ns"},
	}
	for _, u := range units {
		if picos >= u.threshold {
			return formatFloat2(picos/u.threshold) + " " + u.unit
		}
	}
	return fmt.Sprintf("%.0f ps", picos)
}

func sysFormatTimeBig(picos *big.Float) string {
	units := []struct {
		threshold string
		unit      string
	}{
		{"604800000000000000", "w"},
		{"86400000000000000", "d"},
		{"3600000000000000", "h"},
		{"60000000000000", "m"},
		{"1000000000000", "s"},
		{"1000000000", "ms"},
		{"1000000", "us"},
		{"1000", "ns"},
	}
	for _, u := range units {
		thresh := new(big.Float).SetPrec(256)
		thresh.SetString(u.threshold)
		if picos.Cmp(thresh) >= 0 {
			result := new(big.Float).SetPrec(256).Quo(picos, thresh)
			// Format with 2 decimal places using big.Float precision
			// Split into integer and fractional parts
			intPart, _ := result.Int(nil)
			intBig := new(big.Float).SetPrec(256).SetInt(intPart)
			frac := new(big.Float).SetPrec(256).Sub(result, intBig)
			fracF, _ := frac.Float64()
			// Format: integer part + 2 decimal places
			fracStr := fmt.Sprintf("%.2f", fracF)
			decStr := fracStr[1:] // ".xx"
			return fmt.Sprintf("%s%s %s", intPart.String(), decStr, u.unit)
		}
	}
	f, _ := picos.Float64()
	return fmt.Sprintf("%.0f ps", f)
}

