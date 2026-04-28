package executor

import (
	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

func (e *Executor) initSystemTables() {
	if e.Catalog == nil || e.Storage == nil {
		return
	}

	// Initialize sys schema: register all stub tables/views, functions, procedures, triggers
	// and seed initial data rows.
	if sysDB, err := e.Catalog.GetDatabase("sys"); err == nil {
		if len(sysDB.Tables) == 0 {
			initSysSchema(sysDB)
			e.Storage.EnsureDatabase("sys")
			for _, tbl := range sysDB.Tables {
				e.Storage.CreateTable("sys", tbl)
			}
		}
		// Seed sys.version (idempotent: only insert if empty)
		if vt, vtErr := e.Storage.GetTable("sys", "version"); vtErr == nil {
			if e.tableRowCount("sys", "version") == 0 {
				vt.Insert(storage.Row{"sys_version": "2.1.0", "mysql_version": "8.0.36"}) //nolint:errcheck
			}
		}
		// Seed sys.sys_config (idempotent: only insert if empty)
		if e.tableRowCount("sys", "sys_config") == 0 {
			_, _ = e.Execute(`INSERT INTO sys.sys_config (variable, value) VALUES` +
				` ('diagnostics.allow_i_s_tables', 'OFF'),` +
				` ('diagnostics.include_raw', 'OFF'),` +
				` ('ps_thread_trx_info.max_length', '65535'),` +
				` ('statement_performance_analyzer.limit', '100'),` +
				` ('statement_performance_analyzer.view', NULL),` +
				` ('statement_truncate_len', '64')`)
		}
	}

	ensure := func(dbName string, def *catalog.TableDef) {
		e.Storage.EnsureDatabase(dbName)
		db, err := e.Catalog.GetDatabase(dbName)
		if err != nil {
			return
		}
		if _, err := db.GetTable(def.Name); err != nil {
			db.CreateTable(def) //nolint:errcheck
			e.Storage.CreateTable(dbName, def)
		}
	}

	ensure("mysql", &catalog.TableDef{
		Name:      "innodb_table_stats",
		Charset:   "utf8",
		Collation: "utf8_bin",
		Columns: func() []catalog.ColumnDef {
			defCT := "CURRENT_TIMESTAMP"
			return []catalog.ColumnDef{
				{Name: "database_name", Type: "VARCHAR(64)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true},
				{Name: "table_name", Type: "VARCHAR(199)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true},
				{Name: "last_update", Type: "TIMESTAMP", Default: &defCT, OnUpdateCurrentTimestamp: true},
				{Name: "n_rows", Type: "BIGINT(20) UNSIGNED"},
				{Name: "clustered_index_size", Type: "BIGINT(20) UNSIGNED"},
				{Name: "sum_of_other_index_sizes", Type: "BIGINT(20) UNSIGNED"},
			}
		}(),
	})

	ensure("mysql", &catalog.TableDef{
		Name:      "innodb_index_stats",
		Charset:   "utf8",
		Collation: "utf8_bin",
		Columns: func() []catalog.ColumnDef {
			defCT := "CURRENT_TIMESTAMP"
			return []catalog.ColumnDef{
				{Name: "database_name", Type: "VARCHAR(64)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true},
				{Name: "table_name", Type: "VARCHAR(199)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true},
				{Name: "index_name", Type: "VARCHAR(64)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true},
				{Name: "last_update", Type: "TIMESTAMP", Default: &defCT, OnUpdateCurrentTimestamp: true},
				{Name: "stat_name", Type: "VARCHAR(64)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true},
				{Name: "stat_value", Type: "BIGINT(20) UNSIGNED"},
				{Name: "sample_size", Type: "BIGINT(20) UNSIGNED", Nullable: true},
				{Name: "stat_description", Type: "VARCHAR(1024)", Charset: "utf8", Collation: "utf8_bin"},
			}
		}(),
	})

	ensure("performance_schema", &catalog.TableDef{
		Name: "setup_instruments",
		Columns: []catalog.ColumnDef{
			{Name: "NAME", Type: "VARCHAR(128)"},
			{Name: "ENABLED", Type: "VARCHAR(8)"},
			{Name: "TIMED", Type: "VARCHAR(8)"},
			{Name: "PROPERTIES", Type: "VARCHAR(256)"},
			{Name: "VOLATILITY", Type: "INT"},
			{Name: "DOCUMENTATION", Type: "TEXT"},
		},
	})
	ensure("performance_schema", &catalog.TableDef{
		Name: "setup_consumers",
		Columns: []catalog.ColumnDef{
			{Name: "NAME", Type: "VARCHAR(128)"},
			{Name: "ENABLED", Type: "VARCHAR(8)"},
		},
	})
	ensure("performance_schema", &catalog.TableDef{
		Name: "session_status",
		Columns: []catalog.ColumnDef{
			{Name: "VARIABLE_NAME", Type: "VARCHAR(64)"},
			{Name: "VARIABLE_VALUE", Type: "VARCHAR(1024)"},
		},
	})
	ensure("performance_schema", &catalog.TableDef{
		Name: "global_status",
		Columns: []catalog.ColumnDef{
			{Name: "VARIABLE_NAME", Type: "VARCHAR(64)"},
			{Name: "VARIABLE_VALUE", Type: "VARCHAR(1024)"},
		},
	})
	ensure("performance_schema", &catalog.TableDef{
		Name: "file_summary_by_event_name",
		Columns: []catalog.ColumnDef{
			{Name: "EVENT_NAME", Type: "VARCHAR(128)"},
			{Name: "COUNT_STAR", Type: "BIGINT"},
		},
	})
	ensure("performance_schema", &catalog.TableDef{
		Name: "events_stages_history",
		Columns: []catalog.ColumnDef{
			{Name: "THREAD_ID", Type: "BIGINT UNSIGNED"},
			{Name: "EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "END_EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "EVENT_NAME", Type: "VARCHAR(128)"},
			{Name: "SOURCE", Type: "VARCHAR(64)"},
			{Name: "TIMER_START", Type: "BIGINT UNSIGNED"},
			{Name: "TIMER_END", Type: "BIGINT UNSIGNED"},
			{Name: "TIMER_WAIT", Type: "BIGINT UNSIGNED"},
			{Name: "WORK_COMPLETED", Type: "BIGINT UNSIGNED", Nullable: true},
			{Name: "WORK_ESTIMATED", Type: "BIGINT UNSIGNED", Nullable: true},
			{Name: "NESTING_EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "NESTING_EVENT_TYPE", Type: "ENUM('TRANSACTION','STATEMENT','STAGE','WAIT')"},
		},
	})
	ensure("performance_schema", &catalog.TableDef{
		Name: "events_stages_current",
		Columns: []catalog.ColumnDef{
			{Name: "THREAD_ID", Type: "BIGINT UNSIGNED"},
			{Name: "EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "END_EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "EVENT_NAME", Type: "VARCHAR(128)"},
			{Name: "SOURCE", Type: "VARCHAR(64)"},
			{Name: "TIMER_START", Type: "BIGINT UNSIGNED"},
			{Name: "TIMER_END", Type: "BIGINT UNSIGNED"},
			{Name: "TIMER_WAIT", Type: "BIGINT UNSIGNED"},
			{Name: "WORK_COMPLETED", Type: "BIGINT UNSIGNED", Nullable: true},
			{Name: "WORK_ESTIMATED", Type: "BIGINT UNSIGNED", Nullable: true},
			{Name: "NESTING_EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "NESTING_EVENT_TYPE", Type: "ENUM('TRANSACTION','STATEMENT','STAGE','WAIT')"},
		},
	})
	ensure("performance_schema", &catalog.TableDef{
		Name: "global_variables",
		Columns: []catalog.ColumnDef{
			{Name: "VARIABLE_NAME", Type: "VARCHAR(64)"},
			{Name: "VARIABLE_VALUE", Type: "VARCHAR(1024)"},
		},
	})
	ensure("performance_schema", &catalog.TableDef{
		Name: "session_variables",
		Columns: []catalog.ColumnDef{
			{Name: "VARIABLE_NAME", Type: "VARCHAR(64)"},
			{Name: "VARIABLE_VALUE", Type: "VARCHAR(1024)"},
		},
	})
	ensure("performance_schema", &catalog.TableDef{
		Name: "events_waits_history_long",
		Columns: []catalog.ColumnDef{
			{Name: "THREAD_ID", Type: "BIGINT UNSIGNED"},
			{Name: "EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "END_EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "EVENT_NAME", Type: "VARCHAR(128)"},
			{Name: "SOURCE", Type: "VARCHAR(64)"},
			{Name: "TIMER_START", Type: "BIGINT UNSIGNED"},
			{Name: "TIMER_END", Type: "BIGINT UNSIGNED"},
			{Name: "TIMER_WAIT", Type: "BIGINT UNSIGNED"},
			{Name: "SPINS", Type: "INT UNSIGNED"},
			{Name: "OBJECT_SCHEMA", Type: "VARCHAR(64)"},
			{Name: "OBJECT_NAME", Type: "VARCHAR(512)"},
			{Name: "INDEX_NAME", Type: "VARCHAR(64)"},
			{Name: "OBJECT_TYPE", Type: "VARCHAR(64)"},
			{Name: "OBJECT_INSTANCE_BEGIN", Type: "BIGINT UNSIGNED"},
			{Name: "NESTING_EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "NESTING_EVENT_TYPE", Type: "VARCHAR(64)"},
			{Name: "OPERATION", Type: "VARCHAR(32)"},
			{Name: "NUMBER_OF_BYTES", Type: "BIGINT"},
			{Name: "FLAGS", Type: "INT UNSIGNED"},
		},
	})
	ensure("performance_schema", &catalog.TableDef{
		Name: "events_waits_current",
		Columns: []catalog.ColumnDef{
			{Name: "THREAD_ID", Type: "BIGINT UNSIGNED"},
			{Name: "EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "END_EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "EVENT_NAME", Type: "VARCHAR(128)"},
			{Name: "SOURCE", Type: "VARCHAR(64)"},
			{Name: "TIMER_START", Type: "BIGINT UNSIGNED"},
			{Name: "TIMER_END", Type: "BIGINT UNSIGNED"},
			{Name: "TIMER_WAIT", Type: "BIGINT UNSIGNED"},
			{Name: "SPINS", Type: "INT UNSIGNED"},
			{Name: "OBJECT_SCHEMA", Type: "VARCHAR(64)"},
			{Name: "OBJECT_NAME", Type: "VARCHAR(512)"},
			{Name: "INDEX_NAME", Type: "VARCHAR(64)"},
			{Name: "OBJECT_TYPE", Type: "VARCHAR(64)"},
			{Name: "OBJECT_INSTANCE_BEGIN", Type: "BIGINT UNSIGNED"},
			{Name: "NESTING_EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "NESTING_EVENT_TYPE", Type: "VARCHAR(64)"},
			{Name: "OPERATION", Type: "VARCHAR(32)"},
			{Name: "NUMBER_OF_BYTES", Type: "BIGINT"},
			{Name: "FLAGS", Type: "INT UNSIGNED"},
		},
	})
	ensure("performance_schema", &catalog.TableDef{
		Name: "events_statements_history_long",
		Columns: []catalog.ColumnDef{
			{Name: "THREAD_ID", Type: "BIGINT UNSIGNED"},
			{Name: "EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "END_EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "EVENT_NAME", Type: "VARCHAR(128)"},
			{Name: "SOURCE", Type: "VARCHAR(64)"},
			{Name: "TIMER_START", Type: "BIGINT UNSIGNED"},
			{Name: "TIMER_END", Type: "BIGINT UNSIGNED"},
			{Name: "TIMER_WAIT", Type: "BIGINT UNSIGNED"},
			{Name: "SQL_TEXT", Type: "LONGTEXT"},
			{Name: "DIGEST", Type: "VARCHAR(64)"},
			{Name: "DIGEST_TEXT", Type: "LONGTEXT"},
		},
	})
	ensure("performance_schema", &catalog.TableDef{
		Name: "events_stages_history_long",
		Columns: []catalog.ColumnDef{
			{Name: "THREAD_ID", Type: "BIGINT UNSIGNED"},
			{Name: "EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "END_EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "EVENT_NAME", Type: "VARCHAR(128)"},
			{Name: "SOURCE", Type: "VARCHAR(64)"},
			{Name: "TIMER_START", Type: "BIGINT UNSIGNED"},
			{Name: "TIMER_END", Type: "BIGINT UNSIGNED"},
			{Name: "TIMER_WAIT", Type: "BIGINT UNSIGNED"},
			{Name: "WORK_COMPLETED", Type: "BIGINT UNSIGNED", Nullable: true},
			{Name: "WORK_ESTIMATED", Type: "BIGINT UNSIGNED", Nullable: true},
			{Name: "NESTING_EVENT_ID", Type: "BIGINT UNSIGNED"},
			{Name: "NESTING_EVENT_TYPE", Type: "ENUM('TRANSACTION','STATEMENT','STAGE','WAIT')"},
		},
	})

	ensure("mtr", &catalog.TableDef{
		Name:   "global_suppressions",
		Engine: "InnoDB",
		Columns: []catalog.ColumnDef{
			{Name: "pattern", Type: "VARCHAR(255)"},
		},
	})

	ensure("mtr", &catalog.TableDef{
		Name:   "test_suppressions",
		Engine: "InnoDB",
		Columns: []catalog.ColumnDef{
			{Name: "pattern", Type: "VARCHAR(255)"},
		},
	})

	ensure("information_schema", &catalog.TableDef{
		Name: "INNODB_TRX",
		Columns: []catalog.ColumnDef{
			{Name: "trx_id", Type: "VARCHAR(18)"},
			{Name: "trx_state", Type: "VARCHAR(13)"},
			{Name: "trx_started", Type: "DATETIME"},
			{Name: "trx_requested_lock_id", Type: "VARCHAR(105)", Nullable: true},
			{Name: "trx_wait_started", Type: "DATETIME", Nullable: true},
			{Name: "trx_weight", Type: "BIGINT(21) UNSIGNED"},
			{Name: "trx_mysql_thread_id", Type: "BIGINT(21) UNSIGNED"},
			{Name: "trx_query", Type: "VARCHAR(1024)", Nullable: true},
			{Name: "trx_operation_state", Type: "VARCHAR(64)", Nullable: true},
			{Name: "trx_tables_in_use", Type: "BIGINT(21) UNSIGNED"},
			{Name: "trx_tables_locked", Type: "BIGINT(21) UNSIGNED"},
			{Name: "trx_lock_structs", Type: "BIGINT(21) UNSIGNED"},
			{Name: "trx_lock_memory_bytes", Type: "BIGINT(21) UNSIGNED"},
			{Name: "trx_rows_locked", Type: "BIGINT(21) UNSIGNED"},
			{Name: "trx_rows_modified", Type: "BIGINT(21) UNSIGNED"},
			{Name: "trx_concurrency_tickets", Type: "BIGINT(21) UNSIGNED"},
			{Name: "trx_isolation_level", Type: "VARCHAR(16)"},
			{Name: "trx_unique_checks", Type: "INT(1)"},
			{Name: "trx_foreign_key_checks", Type: "INT(1)"},
			{Name: "trx_last_foreign_key_error", Type: "VARCHAR(256)", Nullable: true},
			{Name: "trx_adaptive_hash_latched", Type: "INT(1)"},
			{Name: "trx_adaptive_hash_timeout", Type: "BIGINT(21) UNSIGNED"},
			{Name: "trx_is_read_only", Type: "INT(1)"},
			{Name: "trx_autocommit_non_locking", Type: "INT(1)"},
		},
	})
	ensure("information_schema", &catalog.TableDef{
		Name: "INNODB_BUFFER_POOL_STATS",
		Columns: []catalog.ColumnDef{
			{Name: "POOL_ID", Type: "BIGINT"},
			{Name: "POOL_SIZE", Type: "BIGINT"},
		},
	})
	ensure("information_schema", &catalog.TableDef{
		Name: "INNODB_FOREIGN_COLS",
		Columns: []catalog.ColumnDef{
			{Name: "ID", Type: "VARCHAR(255)"},
			{Name: "FOR_COL_NAME", Type: "VARCHAR(64)"},
			{Name: "REF_COL_NAME", Type: "VARCHAR(64)"},
			{Name: "POS", Type: "BIGINT"},
		},
	})
	ensure("information_schema", &catalog.TableDef{
		Name: "INNODB_INDEXES",
		Columns: []catalog.ColumnDef{
			{Name: "INDEX_ID", Type: "BIGINT"},
			{Name: "NAME", Type: "VARCHAR(255)"},
			{Name: "TABLE_ID", Type: "BIGINT"},
			{Name: "TYPE", Type: "BIGINT"},
		},
	})
	ensure("information_schema", &catalog.TableDef{
		Name: "INNODB_BUFFER_PAGE",
		Columns: []catalog.ColumnDef{
			{Name: "SPACE", Type: "BIGINT"},
			{Name: "PAGE_NUMBER", Type: "BIGINT"},
			{Name: "PAGE_TYPE", Type: "VARCHAR(64)"},
			{Name: "NUMBER_RECORDS", Type: "BIGINT"},
		},
	})
	ensure("information_schema", &catalog.TableDef{
		Name: "OPTIMIZER_TRACE",
		Columns: []catalog.ColumnDef{
			{Name: "QUERY", Type: "LONGTEXT"},
			{Name: "TRACE", Type: "LONGTEXT"},
			{Name: "MISSING_BYTES_BEYOND_MAX_MEM_SIZE", Type: "BIGINT"},
			{Name: "INSUFFICIENT_PRIVILEGES", Type: "TINYINT"},
		},
	})
	ensure("information_schema", &catalog.TableDef{
		Name: "INNODB_CMP_PER_INDEX",
		Columns: []catalog.ColumnDef{
			{Name: "database_name", Type: "VARCHAR(192)"},
			{Name: "table_name", Type: "VARCHAR(192)"},
			{Name: "index_name", Type: "VARCHAR(192)"},
			{Name: "compress_ops", Type: "INT"},
			{Name: "compress_ops_ok", Type: "INT"},
			{Name: "compress_time", Type: "INT"},
			{Name: "uncompress_ops", Type: "INT"},
			{Name: "uncompress_time", Type: "INT"},
		},
	})
	ensure("information_schema", &catalog.TableDef{
		Name: "PLUGINS",
		Columns: []catalog.ColumnDef{
			{Name: "PLUGIN_NAME", Type: "VARCHAR(64)"},
			{Name: "PLUGIN_VERSION", Type: "VARCHAR(20)"},
			{Name: "PLUGIN_STATUS", Type: "VARCHAR(10)"},
			{Name: "PLUGIN_TYPE", Type: "VARCHAR(80)"},
			{Name: "PLUGIN_TYPE_VERSION", Type: "VARCHAR(20)"},
			{Name: "PLUGIN_LIBRARY", Type: "VARCHAR(64)"},
			{Name: "PLUGIN_LIBRARY_VERSION", Type: "VARCHAR(20)"},
			{Name: "PLUGIN_AUTHOR", Type: "VARCHAR(64)"},
			{Name: "PLUGIN_DESCRIPTION", Type: "LONGTEXT"},
			{Name: "PLUGIN_LICENSE", Type: "VARCHAR(80)"},
			{Name: "LOAD_OPTION", Type: "VARCHAR(64)"},
		},
	})
	ensure("information_schema", &catalog.TableDef{
		Name: "INNODB_TEMP_TABLE_INFO",
		Columns: []catalog.ColumnDef{
			{Name: "TABLE_ID", Type: "BIGINT(21) UNSIGNED"},
			{Name: "NAME", Type: "VARCHAR(255)"},
			{Name: "N_COLS", Type: "BIGINT"},
			{Name: "SPACE", Type: "BIGINT"},
		},
	})

	// INFORMATION_SCHEMA tables referenced by MTR tests
	ensure("information_schema", &catalog.TableDef{
		Name: "EVENTS",
		Columns: []catalog.ColumnDef{
			{Name: "EVENT_CATALOG", Type: "VARCHAR(64)"},
			{Name: "EVENT_SCHEMA", Type: "VARCHAR(64)"},
			{Name: "EVENT_NAME", Type: "VARCHAR(64)"},
			{Name: "DEFINER", Type: "VARCHAR(288)"},
			{Name: "TIME_ZONE", Type: "VARCHAR(64)"},
			{Name: "EVENT_BODY", Type: "VARCHAR(8)"},
			{Name: "EVENT_DEFINITION", Type: "LONGTEXT"},
			{Name: "EVENT_TYPE", Type: "VARCHAR(9)"},
			{Name: "EXECUTE_AT", Type: "DATETIME"},
			{Name: "INTERVAL_VALUE", Type: "VARCHAR(256)"},
			{Name: "INTERVAL_FIELD", Type: "VARCHAR(18)"},
			{Name: "SQL_MODE", Type: "VARCHAR(8192)"},
			{Name: "STARTS", Type: "DATETIME"},
			{Name: "ENDS", Type: "DATETIME"},
			{Name: "STATUS", Type: "VARCHAR(18)"},
			{Name: "ON_COMPLETION", Type: "VARCHAR(12)"},
			{Name: "CREATED", Type: "DATETIME"},
			{Name: "LAST_ALTERED", Type: "DATETIME"},
			{Name: "LAST_EXECUTED", Type: "DATETIME"},
			{Name: "EVENT_COMMENT", Type: "VARCHAR(2048)"},
			{Name: "ORIGINATOR", Type: "BIGINT"},
			{Name: "CHARACTER_SET_CLIENT", Type: "VARCHAR(32)"},
			{Name: "COLLATION_CONNECTION", Type: "VARCHAR(32)"},
			{Name: "DATABASE_COLLATION", Type: "VARCHAR(32)"},
		},
	})
	ensure("information_schema", &catalog.TableDef{
		Name: "PARTITIONS",
		Columns: []catalog.ColumnDef{
			{Name: "TABLE_CATALOG", Type: "VARCHAR(64)"},
			{Name: "TABLE_SCHEMA", Type: "VARCHAR(64)"},
			{Name: "TABLE_NAME", Type: "VARCHAR(64)"},
			{Name: "PARTITION_NAME", Type: "VARCHAR(64)"},
			{Name: "SUBPARTITION_NAME", Type: "VARCHAR(64)"},
			{Name: "PARTITION_ORDINAL_POSITION", Type: "BIGINT"},
			{Name: "SUBPARTITION_ORDINAL_POSITION", Type: "BIGINT"},
			{Name: "PARTITION_METHOD", Type: "VARCHAR(18)"},
			{Name: "SUBPARTITION_METHOD", Type: "VARCHAR(12)"},
			{Name: "PARTITION_EXPRESSION", Type: "VARCHAR(2048)"},
			{Name: "SUBPARTITION_EXPRESSION", Type: "VARCHAR(2048)"},
			{Name: "PARTITION_DESCRIPTION", Type: "TEXT"},
			{Name: "TABLE_ROWS", Type: "BIGINT"},
			{Name: "AVG_ROW_LENGTH", Type: "BIGINT"},
			{Name: "DATA_LENGTH", Type: "BIGINT"},
			{Name: "MAX_DATA_LENGTH", Type: "BIGINT"},
			{Name: "INDEX_LENGTH", Type: "BIGINT"},
			{Name: "DATA_FREE", Type: "BIGINT"},
			{Name: "CREATE_TIME", Type: "DATETIME"},
			{Name: "UPDATE_TIME", Type: "DATETIME"},
			{Name: "CHECK_TIME", Type: "DATETIME"},
			{Name: "CHECKSUM", Type: "BIGINT"},
			{Name: "PARTITION_COMMENT", Type: "TEXT"},
			{Name: "NODEGROUP", Type: "VARCHAR(256)"},
			{Name: "TABLESPACE_NAME", Type: "VARCHAR(64)"},
		},
	})
	ensure("information_schema", &catalog.TableDef{
		Name: "RESOURCE_GROUPS",
		Columns: []catalog.ColumnDef{
			{Name: "RESOURCE_GROUP_NAME", Type: "VARCHAR(64)"},
			{Name: "RESOURCE_GROUP_TYPE", Type: "VARCHAR(4)"},
			{Name: "RESOURCE_GROUP_ENABLED", Type: "TINYINT"},
			{Name: "VCPU_IDS", Type: "TEXT"},
			{Name: "THREAD_PRIORITY", Type: "INT"},
		},
	})
	ensure("information_schema", &catalog.TableDef{
		Name: "VIEW_TABLE_USAGE",
		Columns: []catalog.ColumnDef{
			{Name: "VIEW_CATALOG", Type: "VARCHAR(64)"},
			{Name: "VIEW_SCHEMA", Type: "VARCHAR(64)"},
			{Name: "VIEW_NAME", Type: "VARCHAR(64)"},
			{Name: "TABLE_CATALOG", Type: "VARCHAR(64)"},
			{Name: "TABLE_SCHEMA", Type: "VARCHAR(64)"},
			{Name: "TABLE_NAME", Type: "VARCHAR(64)"},
		},
	})

	// MySQL system tables referenced by MTR tests
	// mysql.user column order matches canonical MySQL 8.0 layout (as shown in SELECT * output).
	ensure("mysql", &catalog.TableDef{
		Name: "user",
		Columns: func() []catalog.ColumnDef {
			defEmpty := ""
			defN := "N"
			def0 := "0"
			defPlugin := "caching_sha2_password"
			return []catalog.ColumnDef{
				{Name: "Host", Type: "CHAR(255)", Charset: "ascii", Collation: "ascii_general_ci", PrimaryKey: true, Default: &defEmpty},
				{Name: "User", Type: "CHAR(32)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true, Default: &defEmpty},
				{Name: "Select_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Insert_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Update_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Delete_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Create_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Drop_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Reload_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Shutdown_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Process_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "File_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Grant_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "References_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Index_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Alter_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Show_db_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Super_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Create_tmp_table_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Lock_tables_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Execute_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Repl_slave_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Repl_client_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Create_view_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Show_view_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Create_routine_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Alter_routine_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Create_user_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Event_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Trigger_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Create_tablespace_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "ssl_type", Type: "ENUM('','ANY','X509','SPECIFIED')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defEmpty},
				{Name: "ssl_cipher", Type: "BLOB"},
				{Name: "x509_issuer", Type: "BLOB"},
				{Name: "x509_subject", Type: "BLOB"},
				{Name: "max_questions", Type: "INT(11) UNSIGNED", Default: &def0},
				{Name: "max_updates", Type: "INT(11) UNSIGNED", Default: &def0},
				{Name: "max_connections", Type: "INT(11) UNSIGNED", Default: &def0},
				{Name: "max_user_connections", Type: "INT(11) UNSIGNED", Default: &def0},
				{Name: "plugin", Type: "CHAR(64)", Charset: "utf8", Collation: "utf8_bin", Default: &defPlugin},
				{Name: "authentication_string", Type: "TEXT", Charset: "utf8", Collation: "utf8_bin", Nullable: true},
				{Name: "password_expired", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "password_last_changed", Type: "TIMESTAMP", Nullable: true},
				{Name: "password_lifetime", Type: "SMALLINT UNSIGNED", Nullable: true},
				{Name: "account_locked", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Create_role_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Drop_role_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Password_reuse_history", Type: "SMALLINT UNSIGNED", Nullable: true},
				{Name: "Password_reuse_time", Type: "SMALLINT UNSIGNED", Nullable: true},
				{Name: "Password_require_current", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Nullable: true},
				{Name: "User_attributes", Type: "JSON", Nullable: true},
			}
		}(),
	})
	ensure("mysql", &catalog.TableDef{
		Name: "db",
		Columns: func() []catalog.ColumnDef {
			defEmpty := ""
			defN := "N"
			return []catalog.ColumnDef{
				{Name: "Host", Type: "CHAR(255)", Charset: "ascii", Collation: "ascii_general_ci", PrimaryKey: true, Default: &defEmpty},
				{Name: "Db", Type: "CHAR(64)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true, Default: &defEmpty},
				{Name: "User", Type: "CHAR(32)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true, Default: &defEmpty},
				{Name: "Select_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Insert_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Update_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Delete_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Create_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Drop_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Grant_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "References_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Index_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Alter_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Create_tmp_table_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Lock_tables_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Create_view_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Show_view_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Create_routine_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Alter_routine_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Execute_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Event_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
				{Name: "Trigger_priv", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
			}
		}(),
	})
	logDefaultTS := "CURRENT_TIMESTAMP(6)"
	ensure("mysql", &catalog.TableDef{
		Name:    "general_log",
		Engine:  "CSV",
		Charset: "utf8",
		Collation: "utf8_general_ci",
		Comment: "General log",
		Columns: []catalog.ColumnDef{
			{Name: "event_time", Type: "TIMESTAMP(6)", Default: &logDefaultTS, OnUpdateCurrentTimestamp: true},
			{Name: "user_host", Type: "MEDIUMTEXT", Charset: "utf8", Collation: "utf8_general_ci"},
			{Name: "thread_id", Type: "BIGINT(21) UNSIGNED"},
			{Name: "server_id", Type: "INT(10) UNSIGNED"},
			{Name: "command_type", Type: "VARCHAR(64)", Charset: "utf8", Collation: "utf8_general_ci"},
			{Name: "argument", Type: "MEDIUMBLOB"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name:    "slow_log",
		Engine:  "CSV",
		Charset: "utf8",
		Collation: "utf8_general_ci",
		Comment: "Slow log",
		Columns: []catalog.ColumnDef{
			{Name: "start_time", Type: "TIMESTAMP(6)", Default: &logDefaultTS, OnUpdateCurrentTimestamp: true},
			{Name: "user_host", Type: "MEDIUMTEXT", Charset: "utf8", Collation: "utf8_general_ci"},
			{Name: "query_time", Type: "TIME(6)"},
			{Name: "lock_time", Type: "TIME(6)"},
			{Name: "rows_sent", Type: "INT(11)"},
			{Name: "rows_examined", Type: "INT(11)"},
			{Name: "db", Type: "VARCHAR(512)", Charset: "utf8", Collation: "utf8_general_ci"},
			{Name: "last_insert_id", Type: "INT(11)"},
			{Name: "insert_id", Type: "INT(11)"},
			{Name: "server_id", Type: "INT(10) UNSIGNED"},
			{Name: "sql_text", Type: "MEDIUMBLOB"},
			{Name: "thread_id", Type: "BIGINT(21) UNSIGNED"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "server_cost",
		Columns: func() []catalog.ColumnDef {
			defCT := "CURRENT_TIMESTAMP"
			return []catalog.ColumnDef{
				{Name: "cost_name", Type: "VARCHAR(64)", Charset: "utf8", Collation: "utf8_general_ci", PrimaryKey: true},
				{Name: "cost_value", Type: "FLOAT", Nullable: true},
				{Name: "last_update", Type: "TIMESTAMP", Default: &defCT, OnUpdateCurrentTimestamp: true},
				{Name: "comment", Type: "VARCHAR(1024)", Charset: "utf8", Collation: "utf8_general_ci", Nullable: true},
				{Name: "default_value", Type: "FLOAT GENERATED ALWAYS AS (case cost_name when 'disk_temptable_create_cost' then 20.0 when 'disk_temptable_row_cost' then 0.5 when 'key_compare_cost' then 0.05 when 'memory_temptable_create_cost' then 1.0 when 'memory_temptable_row_cost' then 0.1 when 'row_evaluate_cost' then 0.1 else NULL end) VIRTUAL", Nullable: true},
			}
		}(),
	})
	if e.tableRowCount("mysql", "server_cost") == 0 {
		_, _ = e.Execute(`INSERT INTO mysql.server_cost (cost_name, cost_value, last_update, comment) VALUES` +
			` ('disk_temptable_create_cost', NULL, CURRENT_TIMESTAMP, NULL),` +
			` ('disk_temptable_row_cost', NULL, CURRENT_TIMESTAMP, NULL),` +
			` ('key_compare_cost', NULL, CURRENT_TIMESTAMP, NULL),` +
			` ('memory_temptable_create_cost', NULL, CURRENT_TIMESTAMP, NULL),` +
			` ('memory_temptable_row_cost', NULL, CURRENT_TIMESTAMP, NULL),` +
			` ('row_evaluate_cost', NULL, CURRENT_TIMESTAMP, NULL)`)
	}
	ensure("mysql", &catalog.TableDef{
		Name: "engine_cost",
		Columns: func() []catalog.ColumnDef {
			defCT := "CURRENT_TIMESTAMP"
			return []catalog.ColumnDef{
				{Name: "engine_name", Type: "VARCHAR(64)", Charset: "utf8", Collation: "utf8_general_ci", PrimaryKey: true},
				{Name: "device_type", Type: "INT", PrimaryKey: true},
				{Name: "cost_name", Type: "VARCHAR(64)", Charset: "utf8", Collation: "utf8_general_ci", PrimaryKey: true},
				{Name: "cost_value", Type: "FLOAT", Nullable: true},
				{Name: "last_update", Type: "TIMESTAMP", Default: &defCT, OnUpdateCurrentTimestamp: true},
				{Name: "comment", Type: "VARCHAR(1024)", Charset: "utf8", Collation: "utf8_general_ci", Nullable: true},
				{Name: "default_value", Type: "FLOAT GENERATED ALWAYS AS (case cost_name when 'io_block_read_cost' then 1.0 when 'memory_block_read_cost' then 0.25 else NULL end) VIRTUAL", Nullable: true},
			}
		}(),
	})
	if e.tableRowCount("mysql", "engine_cost") == 0 {
		_, _ = e.Execute(`INSERT INTO mysql.engine_cost (engine_name, device_type, cost_name, cost_value, last_update, comment) VALUES` +
			` ('default', 0, 'io_block_read_cost', NULL, CURRENT_TIMESTAMP, NULL),` +
			` ('default', 0, 'memory_block_read_cost', NULL, CURRENT_TIMESTAMP, NULL)`)
	}
	ensure("mysql", &catalog.TableDef{
		Name: "tables_priv",
		Indexes: []catalog.IndexDef{
			{Name: "Grantor", Columns: []string{"Grantor"}, Unique: false},
		},
		Columns: func() []catalog.ColumnDef {
			defEmpty := ""
			defCT := "CURRENT_TIMESTAMP"
			return []catalog.ColumnDef{
				{Name: "Host", Type: "CHAR(255)", Charset: "ascii", Collation: "ascii_general_ci", PrimaryKey: true, Default: &defEmpty},
				{Name: "Db", Type: "CHAR(64)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true, Default: &defEmpty},
				{Name: "User", Type: "CHAR(32)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true, Default: &defEmpty},
				{Name: "Table_name", Type: "CHAR(64)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true, Default: &defEmpty},
				{Name: "Grantor", Type: "VARCHAR(288)", Charset: "utf8", Collation: "utf8_bin", Default: &defEmpty},
				{Name: "Timestamp", Type: "TIMESTAMP", Default: &defCT, OnUpdateCurrentTimestamp: true},
				{Name: "Table_priv", Type: "SET('Select','Insert','Update','Delete','Create','Drop','Grant','References','Index','Alter','Create View','Show view','Trigger')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defEmpty},
				{Name: "Column_priv", Type: "SET('Select','Insert','Update','References')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defEmpty},
			}
		}(),
	})
	ensure("mysql", &catalog.TableDef{
		Name: "columns_priv",
		Columns: func() []catalog.ColumnDef {
			defEmpty := ""
			defCT := "CURRENT_TIMESTAMP"
			return []catalog.ColumnDef{
				{Name: "Host", Type: "CHAR(255)", Charset: "ascii", Collation: "ascii_general_ci", PrimaryKey: true, Default: &defEmpty},
				{Name: "Db", Type: "CHAR(64)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true, Default: &defEmpty},
				{Name: "User", Type: "CHAR(32)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true, Default: &defEmpty},
				{Name: "Table_name", Type: "CHAR(64)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true, Default: &defEmpty},
				{Name: "Column_name", Type: "CHAR(64)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true, Default: &defEmpty},
				{Name: "Timestamp", Type: "TIMESTAMP", Default: &defCT, OnUpdateCurrentTimestamp: true},
				{Name: "Column_priv", Type: "SET('Select','Insert','Update','References')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defEmpty},
			}
		}(),
	})
	ensure("mysql", &catalog.TableDef{
		Name: "role_edges",
		Columns: func() []catalog.ColumnDef {
			defEmpty := ""
			defN := "N"
			return []catalog.ColumnDef{
				{Name: "FROM_HOST", Type: "CHAR(255)", Charset: "ascii", Collation: "ascii_general_ci", PrimaryKey: true, Default: &defEmpty},
				{Name: "FROM_USER", Type: "CHAR(32)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true, Default: &defEmpty},
				{Name: "TO_HOST", Type: "CHAR(255)", Charset: "ascii", Collation: "ascii_general_ci", PrimaryKey: true, Default: &defEmpty},
				{Name: "TO_USER", Type: "CHAR(32)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true, Default: &defEmpty},
				{Name: "WITH_ADMIN_OPTION", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
			}
		}(),
	})
	ensure("mysql", &catalog.TableDef{
		Name: "default_roles",
		Columns: func() []catalog.ColumnDef {
			defPercent := "%"
			defEmpty := ""
			return []catalog.ColumnDef{
				{Name: "HOST", Type: "CHAR(255)", Charset: "ascii", Collation: "ascii_general_ci", PrimaryKey: true, Default: &defEmpty},
				{Name: "USER", Type: "CHAR(32)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true, Default: &defEmpty},
				{Name: "DEFAULT_ROLE_HOST", Type: "CHAR(255)", Charset: "ascii", Collation: "ascii_general_ci", PrimaryKey: true, Default: &defPercent},
				{Name: "DEFAULT_ROLE_USER", Type: "CHAR(32)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true, Default: &defEmpty},
			}
		}(),
	})
	ensure("mysql", &catalog.TableDef{
		Name: "func",
		Columns: func() []catalog.ColumnDef {
			def0 := "0"
			defEmpty := ""
			return []catalog.ColumnDef{
				{Name: "name", Type: "CHAR(64)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true, Default: &defEmpty},
				{Name: "ret", Type: "TINYINT(1)", Default: &def0},
				{Name: "dl", Type: "CHAR(128)", Charset: "utf8", Collation: "utf8_bin", Default: &defEmpty},
				{Name: "type", Type: "ENUM('function','aggregate')", Charset: "utf8", Collation: "utf8_general_ci"},
			}
		}(),
	})
	ensure("mysql", &catalog.TableDef{
		Name: "procs_priv",
		Indexes: []catalog.IndexDef{
			{Name: "Grantor", Columns: []string{"Grantor"}, Unique: false},
		},
		Columns: func() []catalog.ColumnDef {
			defEmpty := ""
			defCT := "CURRENT_TIMESTAMP"
			return []catalog.ColumnDef{
				{Name: "Host", Type: "CHAR(255)", Charset: "ascii", Collation: "ascii_general_ci", PrimaryKey: true, Default: &defEmpty},
				{Name: "Db", Type: "CHAR(64)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true, Default: &defEmpty},
				{Name: "User", Type: "CHAR(32)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true, Default: &defEmpty},
				{Name: "Routine_name", Type: "CHAR(64)", Charset: "utf8", Collation: "utf8_general_ci", PrimaryKey: true, Default: &defEmpty},
				{Name: "Routine_type", Type: "ENUM('FUNCTION','PROCEDURE')", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true},
				{Name: "Grantor", Type: "VARCHAR(288)", Charset: "utf8", Collation: "utf8_bin", Default: &defEmpty},
				{Name: "Proc_priv", Type: "SET('Execute','Alter Routine','Grant')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defEmpty},
				{Name: "Timestamp", Type: "TIMESTAMP", Default: &defCT, OnUpdateCurrentTimestamp: true},
			}
		}(),
	})
	ensure("mysql", &catalog.TableDef{
		Name: "component",
		Columns: []catalog.ColumnDef{
			{Name: "component_id", Type: "INT(10) UNSIGNED", AutoIncrement: true, PrimaryKey: true},
			{Name: "component_group_id", Type: "INT(10) UNSIGNED"},
			{Name: "component_urn", Type: "TEXT", Charset: "utf8", Collation: "utf8_general_ci"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "global_grants",
		Columns: func() []catalog.ColumnDef {
			defEmpty := ""
			defN := "N"
			return []catalog.ColumnDef{
				{Name: "USER", Type: "CHAR(32)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true, Default: &defEmpty},
				{Name: "HOST", Type: "CHAR(255)", Charset: "ascii", Collation: "ascii_general_ci", PrimaryKey: true, Default: &defEmpty},
				{Name: "PRIV", Type: "CHAR(32)", Charset: "utf8", Collation: "utf8_general_ci", PrimaryKey: true, Default: &defEmpty},
				{Name: "WITH_GRANT_OPTION", Type: "ENUM('N','Y')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
			}
		}(),
	})
	ensure("mysql", &catalog.TableDef{
		Name: "gtid_executed",
		Columns: []catalog.ColumnDef{
			{Name: "source_uuid", Type: "CHAR(36)", Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci", PrimaryKey: true, Comment: "uuid of the source where the transaction was originally executed."},
			{Name: "interval_start", Type: "BIGINT(20)", PrimaryKey: true, Comment: "First number of interval."},
			{Name: "interval_end", Type: "BIGINT(20)", Comment: "Last number of interval."},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "help_category",
		Columns: []catalog.ColumnDef{
			{Name: "help_category_id", Type: "SMALLINT(5) UNSIGNED", PrimaryKey: true},
			{Name: "name", Type: "CHAR(64)", Charset: "utf8", Collation: "utf8_general_ci", Unique: true},
			{Name: "parent_category_id", Type: "SMALLINT(5) UNSIGNED", Nullable: true},
			{Name: "url", Type: "TEXT", Charset: "utf8", Collation: "utf8_general_ci"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "help_keyword",
		Columns: []catalog.ColumnDef{
			{Name: "help_keyword_id", Type: "INT(10) UNSIGNED", PrimaryKey: true},
			{Name: "name", Type: "CHAR(64)", Charset: "utf8", Collation: "utf8_general_ci", Unique: true},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "help_relation",
		Columns: []catalog.ColumnDef{
			{Name: "help_topic_id", Type: "INT(10) UNSIGNED", PrimaryKey: true},
			{Name: "help_keyword_id", Type: "INT(10) UNSIGNED", PrimaryKey: true},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "help_topic",
		Columns: []catalog.ColumnDef{
			{Name: "help_topic_id", Type: "INT(10) UNSIGNED", PrimaryKey: true},
			{Name: "name", Type: "CHAR(64)", Charset: "utf8", Collation: "utf8_general_ci", Unique: true},
			{Name: "help_category_id", Type: "SMALLINT(5) UNSIGNED"},
			{Name: "description", Type: "TEXT", Charset: "utf8", Collation: "utf8_general_ci"},
			{Name: "example", Type: "TEXT", Charset: "utf8", Collation: "utf8_general_ci"},
			{Name: "url", Type: "TEXT", Charset: "utf8", Collation: "utf8_general_ci"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "password_history",
		Columns: func() []catalog.ColumnDef {
			defEmpty := ""
			defCT6 := "CURRENT_TIMESTAMP(6)"
			return []catalog.ColumnDef{
				{Name: "Host", Type: "CHAR(255)", Charset: "ascii", Collation: "ascii_general_ci", PrimaryKey: true, Default: &defEmpty},
				{Name: "User", Type: "CHAR(32)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true, Default: &defEmpty},
				{Name: "Password_timestamp", Type: "TIMESTAMP(6)", PrimaryKey: true, Default: &defCT6},
				{Name: "Password", Type: "TEXT", Charset: "utf8", Collation: "utf8_bin", Nullable: true},
			}
		}(),
	})
	ensure("mysql", &catalog.TableDef{
		Name: "plugin",
		Columns: func() []catalog.ColumnDef {
			defEmpty := ""
			return []catalog.ColumnDef{
				{Name: "name", Type: "VARCHAR(64)", Charset: "utf8", Collation: "utf8_general_ci", PrimaryKey: true, Default: &defEmpty},
				{Name: "dl", Type: "VARCHAR(128)", Charset: "utf8", Collation: "utf8_general_ci", Default: &defEmpty},
			}
		}(),
	})
	ensure("mysql", &catalog.TableDef{
		Name: "proxies_priv",
		Indexes: []catalog.IndexDef{
			{Name: "Grantor", Columns: []string{"Grantor"}, Unique: false},
		},
		Columns: func() []catalog.ColumnDef {
			def0 := "0"
			defEmpty := ""
			defCT := "CURRENT_TIMESTAMP"
			return []catalog.ColumnDef{
				{Name: "Host", Type: "CHAR(255)", Charset: "ascii", Collation: "ascii_general_ci", PrimaryKey: true, Default: &defEmpty},
				{Name: "User", Type: "CHAR(32)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true, Default: &defEmpty},
				{Name: "Proxied_host", Type: "CHAR(255)", Charset: "ascii", Collation: "ascii_general_ci", PrimaryKey: true, Default: &defEmpty},
				{Name: "Proxied_user", Type: "CHAR(32)", Charset: "utf8", Collation: "utf8_bin", PrimaryKey: true, Default: &defEmpty},
				{Name: "With_grant", Type: "TINYINT(1)", Default: &def0},
				{Name: "Grantor", Type: "VARCHAR(288)", Charset: "utf8", Collation: "utf8_bin", Default: &defEmpty},
				{Name: "Timestamp", Type: "TIMESTAMP", Default: &defCT, OnUpdateCurrentTimestamp: true},
			}
		}(),
	})
	ensure("mysql", &catalog.TableDef{
		Name: "servers",
		Columns: func() []catalog.ColumnDef {
			defEmpty := ""
			def0 := "0"
			return []catalog.ColumnDef{
				{Name: "Server_name", Type: "CHAR(64)", Charset: "utf8", Collation: "utf8_general_ci", PrimaryKey: true, Default: &defEmpty},
				{Name: "Host", Type: "CHAR(255)", Charset: "ascii", Collation: "ascii_general_ci", Default: &defEmpty},
				{Name: "Db", Type: "CHAR(64)", Charset: "utf8", Collation: "utf8_general_ci", Default: &defEmpty},
				{Name: "Username", Type: "CHAR(64)", Charset: "utf8", Collation: "utf8_general_ci", Default: &defEmpty},
				{Name: "Password", Type: "CHAR(64)", Charset: "utf8", Collation: "utf8_general_ci", Default: &defEmpty},
				{Name: "Port", Type: "INT(4)", Default: &def0},
				{Name: "Socket", Type: "CHAR(64)", Charset: "utf8", Collation: "utf8_general_ci", Default: &defEmpty},
				{Name: "Wrapper", Type: "CHAR(64)", Charset: "utf8", Collation: "utf8_general_ci", Default: &defEmpty},
				{Name: "Owner", Type: "CHAR(64)", Charset: "utf8", Collation: "utf8_general_ci", Default: &defEmpty},
			}
		}(),
	})
	ensure("mysql", &catalog.TableDef{
		Name: "slave_master_info",
		Columns: []catalog.ColumnDef{
			{Name: "Number_of_lines", Type: "INT(10) UNSIGNED", Comment: "Number of lines in the file."},
			{Name: "Master_log_name", Type: "TEXT", Charset: "utf8", Collation: "utf8_bin", Comment: "The name of the master binary log currently being read from the master."},
			{Name: "Master_log_pos", Type: "BIGINT(20) UNSIGNED", Comment: "The master log position of the last read event."},
			{Name: "Host", Type: "CHAR(255)", Charset: "ascii", Collation: "ascii_general_ci", Nullable: true, Comment: "The host name of the master."},
			{Name: "User_name", Type: "TEXT", Charset: "utf8", Collation: "utf8_bin", Nullable: true, Comment: "The user name used to connect to the master."},
			{Name: "User_password", Type: "TEXT", Charset: "utf8", Collation: "utf8_bin", Nullable: true, Comment: "The password used to connect to the master."},
			{Name: "Port", Type: "INT(10) UNSIGNED", Comment: "The network port used to connect to the master."},
			{Name: "Connect_retry", Type: "INT(10) UNSIGNED", Comment: "The period (in seconds) that the slave will wait before trying to reconnect to the master."},
			{Name: "Enabled_ssl", Type: "TINYINT(1)", Comment: "Indicates whether the server supports SSL connections."},
			{Name: "Ssl_ca", Type: "TEXT", Charset: "utf8", Collation: "utf8_bin", Nullable: true, Comment: "The file used for the Certificate Authority (CA) certificate."},
			{Name: "Ssl_capath", Type: "TEXT", Charset: "utf8", Collation: "utf8_bin", Nullable: true, Comment: "The path to the Certificate Authority (CA) certificates."},
			{Name: "Ssl_cert", Type: "TEXT", Charset: "utf8", Collation: "utf8_bin", Nullable: true, Comment: "The name of the SSL certificate file."},
			{Name: "Ssl_cipher", Type: "TEXT", Charset: "utf8", Collation: "utf8_bin", Nullable: true, Comment: "The name of the cipher in use for the SSL connection."},
			{Name: "Ssl_key", Type: "TEXT", Charset: "utf8", Collation: "utf8_bin", Nullable: true, Comment: "The name of the SSL key file."},
			{Name: "Ssl_verify_server_cert", Type: "TINYINT(1)", Comment: "Whether to verify the server certificate."},
			{Name: "Heartbeat", Type: "FLOAT"},
			{Name: "Bind", Type: "TEXT", Charset: "utf8", Collation: "utf8_bin", Nullable: true, Comment: "Displays which interface is employed when connecting to the MySQL server"},
			{Name: "Ignored_server_ids", Type: "TEXT", Charset: "utf8", Collation: "utf8_bin", Nullable: true, Comment: "The number of server IDs to be ignored, followed by the actual server IDs"},
			{Name: "Uuid", Type: "TEXT", Charset: "utf8", Collation: "utf8_bin", Nullable: true, Comment: "The master server uuid."},
			{Name: "Retry_count", Type: "BIGINT(20) UNSIGNED", Comment: "Number of reconnect attempts, to the master, before giving up."},
			{Name: "Ssl_crl", Type: "TEXT", Charset: "utf8", Collation: "utf8_bin", Nullable: true, Comment: "The file used for the Certificate Revocation List (CRL)"},
			{Name: "Ssl_crlpath", Type: "TEXT", Charset: "utf8", Collation: "utf8_bin", Nullable: true, Comment: "The path used for Certificate Revocation List (CRL) files"},
			{Name: "Enabled_auto_position", Type: "TINYINT(1)", Comment: "Indicates whether GTIDs will be used to retrieve events from the master."},
			{Name: "Channel_name", Type: "CHAR(64)", Charset: "utf8", Collation: "utf8_general_ci", PrimaryKey: true, Comment: "The channel on which the slave is connected to a source. Used in Multisource Replication"},
			{Name: "Tls_version", Type: "TEXT", Charset: "utf8", Collation: "utf8_bin", Nullable: true, Comment: "Tls version"},
			{Name: "Public_key_path", Type: "TEXT", Charset: "utf8", Collation: "utf8_bin", Nullable: true, Comment: "The file containing public key of master server."},
			{Name: "Get_public_key", Type: "TINYINT(1)", Comment: "Preference to get public key from master."},
			{Name: "Network_namespace", Type: "TEXT", Charset: "utf8", Collation: "utf8_bin", Nullable: true, Comment: "Network namespace used for communication with the master server."},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "slave_relay_log_info",
		Columns: []catalog.ColumnDef{
			{Name: "Number_of_lines", Type: "INT(10) UNSIGNED", Comment: "Number of lines in the file or rows in the table. Used to version table definitions."},
			{Name: "Relay_log_name", Type: "TEXT", Charset: "utf8", Collation: "utf8_bin", Comment: "The name of the current relay log file."},
			{Name: "Relay_log_pos", Type: "BIGINT(20) UNSIGNED", Comment: "The relay log position of the last executed event."},
			{Name: "Master_log_name", Type: "TEXT", Charset: "utf8", Collation: "utf8_bin", Comment: "The name of the master binary log file from which the events in the relay log file were read."},
			{Name: "Master_log_pos", Type: "BIGINT(20) UNSIGNED", Comment: "The master log position of the last executed event."},
			{Name: "Sql_delay", Type: "INT(11)", Comment: "The number of seconds that the slave must lag behind the master."},
			{Name: "Number_of_workers", Type: "INT(10) UNSIGNED"},
			{Name: "Id", Type: "INT(10) UNSIGNED", Comment: "Internal Id that uniquely identifies this record."},
			{Name: "Channel_name", Type: "CHAR(64)", Charset: "utf8", Collation: "utf8_general_ci", PrimaryKey: true, Comment: "The channel on which the slave is connected to a source. Used in Multisource Replication"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "slave_worker_info",
		Columns: []catalog.ColumnDef{
			{Name: "Id", Type: "INT(10) UNSIGNED", PrimaryKey: true},
			{Name: "Relay_log_name", Type: "TEXT", Charset: "utf8", Collation: "utf8_bin"},
			{Name: "Relay_log_pos", Type: "BIGINT(20) UNSIGNED"},
			{Name: "Master_log_name", Type: "TEXT", Charset: "utf8", Collation: "utf8_bin"},
			{Name: "Master_log_pos", Type: "BIGINT(20) UNSIGNED"},
			{Name: "Checkpoint_relay_log_name", Type: "TEXT", Charset: "utf8", Collation: "utf8_bin"},
			{Name: "Checkpoint_relay_log_pos", Type: "BIGINT(20) UNSIGNED"},
			{Name: "Checkpoint_master_log_name", Type: "TEXT", Charset: "utf8", Collation: "utf8_bin"},
			{Name: "Checkpoint_master_log_pos", Type: "BIGINT(20) UNSIGNED"},
			{Name: "Checkpoint_seqno", Type: "INT(10) UNSIGNED"},
			{Name: "Checkpoint_group_size", Type: "INT(10) UNSIGNED"},
			{Name: "Checkpoint_group_bitmap", Type: "BLOB"},
			{Name: "Channel_name", Type: "CHAR(64)", Charset: "utf8", Collation: "utf8_general_ci", PrimaryKey: true, Comment: "The channel on which the slave is connected to a source. Used in Multisource Replication"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "time_zone",
		Columns: func() []catalog.ColumnDef {
			defN := "N"
			return []catalog.ColumnDef{
				{Name: "Time_zone_id", Type: "INT(10) UNSIGNED", AutoIncrement: true, PrimaryKey: true},
				{Name: "Use_leap_seconds", Type: "ENUM('Y','N')", Charset: "utf8", Collation: "utf8_general_ci", Default: &defN},
			}
		}(),
	})
	ensure("mysql", &catalog.TableDef{
		Name: "time_zone_leap_second",
		Columns: []catalog.ColumnDef{
			{Name: "Transition_time", Type: "BIGINT(20)", PrimaryKey: true},
			{Name: "Correction", Type: "INT(11)"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "time_zone_name",
		Columns: []catalog.ColumnDef{
			{Name: "Name", Type: "CHAR(64)", Charset: "utf8", Collation: "utf8_general_ci", PrimaryKey: true},
			{Name: "Time_zone_id", Type: "INT(10) UNSIGNED"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "time_zone_transition",
		Columns: []catalog.ColumnDef{
			{Name: "Time_zone_id", Type: "INT(10) UNSIGNED", PrimaryKey: true},
			{Name: "Transition_time", Type: "BIGINT(20)", PrimaryKey: true},
			{Name: "Transition_type_id", Type: "INT(10) UNSIGNED"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "time_zone_transition_type",
		Columns: func() []catalog.ColumnDef {
			def0 := "0"
			defEmpty := ""
			return []catalog.ColumnDef{
				{Name: "Time_zone_id", Type: "INT(10) UNSIGNED", PrimaryKey: true},
				{Name: "Transition_type_id", Type: "INT(10) UNSIGNED", PrimaryKey: true},
				{Name: "Offset", Type: "INT(11)", Default: &def0},
				{Name: "Is_DST", Type: "TINYINT(3) UNSIGNED", Default: &def0},
				{Name: "Abbreviation", Type: "CHAR(8)", Charset: "utf8", Collation: "utf8_general_ci", Default: &defEmpty},
			}
		}(),
	})
}
