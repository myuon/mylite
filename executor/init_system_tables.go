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
		Charset:   "utf8mb4",
		Collation: "utf8mb4_bin",
		Columns: []catalog.ColumnDef{
			{Name: "database_name", Type: "VARCHAR(64)"},
			{Name: "table_name", Type: "VARCHAR(199)"},
			{Name: "last_update", Type: "TIMESTAMP"},
			{Name: "n_rows", Type: "BIGINT"},
			{Name: "clustered_index_size", Type: "BIGINT"},
			{Name: "sum_of_other_index_sizes", Type: "BIGINT"},
		},
	})

	ensure("mysql", &catalog.TableDef{
		Name:      "innodb_index_stats",
		Charset:   "utf8mb4",
		Collation: "utf8mb4_bin",
		Columns: []catalog.ColumnDef{
			{Name: "database_name", Type: "VARCHAR(64)"},
			{Name: "table_name", Type: "VARCHAR(199)"},
			{Name: "index_name", Type: "VARCHAR(64)"},
			{Name: "last_update", Type: "TIMESTAMP"},
			{Name: "stat_name", Type: "VARCHAR(64)"},
			{Name: "stat_value", Type: "BIGINT"},
			{Name: "sample_size", Type: "BIGINT", Nullable: true},
			{Name: "stat_description", Type: "VARCHAR(1024)"},
		},
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
	ensure("mysql", &catalog.TableDef{
		Name: "user",
		Columns: func() []catalog.ColumnDef {
			defEmpty := ""
			defN := "N"
			return []catalog.ColumnDef{
				{Name: "Host", Type: "VARCHAR(255)", Default: &defEmpty},
				{Name: "User", Type: "VARCHAR(32)", Default: &defEmpty},
				{Name: "Select_priv", Type: "VARCHAR(1)", Default: &defN},
				{Name: "Insert_priv", Type: "VARCHAR(1)", Default: &defN},
				{Name: "Update_priv", Type: "VARCHAR(1)", Default: &defN},
				{Name: "Delete_priv", Type: "VARCHAR(1)", Default: &defN},
				{Name: "Create_priv", Type: "VARCHAR(1)", Default: &defN},
				{Name: "Drop_priv", Type: "VARCHAR(1)", Default: &defN},
				{Name: "Grant_priv", Type: "VARCHAR(1)", Default: &defN},
				{Name: "Shutdown_priv", Type: "VARCHAR(1)", Default: &defN},
				{Name: "authentication_string", Type: "TEXT", Default: &defEmpty},
				{Name: "plugin", Type: "VARCHAR(64)", Default: &defEmpty},
				{Name: "account_locked", Type: "VARCHAR(1)", Default: &defN},
				{Name: "ssl_cipher", Type: "BLOB", Default: &defEmpty},
				{Name: "x509_issuer", Type: "BLOB", Default: &defEmpty},
				{Name: "x509_subject", Type: "BLOB", Default: &defEmpty},
				{Name: "password_last_changed", Type: "TIMESTAMP", Nullable: true},
				{Name: "Password_reuse_history", Type: "SMALLINT UNSIGNED", Nullable: true},
				{Name: "Password_reuse_time", Type: "SMALLINT UNSIGNED", Nullable: true},
				{Name: "create_role_priv", Type: "ENUM('N','Y')", Default: &defN},
				{Name: "drop_role_priv", Type: "ENUM('N','Y')", Default: &defN},
			}
		}(),
	})
	ensure("mysql", &catalog.TableDef{
		Name: "db",
		Columns: func() []catalog.ColumnDef {
			defEmpty := ""
			defN := "N"
			return []catalog.ColumnDef{
				{Name: "Host", Type: "VARCHAR(255)", Default: &defEmpty},
				{Name: "Db", Type: "VARCHAR(64)", Default: &defEmpty},
				{Name: "User", Type: "VARCHAR(32)", Default: &defEmpty},
				{Name: "Select_priv", Type: "ENUM('N','Y')", Default: &defN},
				{Name: "Insert_priv", Type: "ENUM('N','Y')", Default: &defN},
				{Name: "Update_priv", Type: "ENUM('N','Y')", Default: &defN},
				{Name: "Delete_priv", Type: "ENUM('N','Y')", Default: &defN},
				{Name: "Create_priv", Type: "ENUM('N','Y')", Default: &defN},
				{Name: "Drop_priv", Type: "ENUM('N','Y')", Default: &defN},
				{Name: "Grant_priv", Type: "ENUM('N','Y')", Default: &defN},
				{Name: "References_priv", Type: "ENUM('N','Y')", Default: &defN},
				{Name: "Index_priv", Type: "ENUM('N','Y')", Default: &defN},
				{Name: "Alter_priv", Type: "ENUM('N','Y')", Default: &defN},
				{Name: "Create_tmp_table_priv", Type: "ENUM('N','Y')", Default: &defN},
				{Name: "Lock_tables_priv", Type: "ENUM('N','Y')", Default: &defN},
				{Name: "Create_view_priv", Type: "ENUM('N','Y')", Default: &defN},
				{Name: "Show_view_priv", Type: "ENUM('N','Y')", Default: &defN},
				{Name: "Create_routine_priv", Type: "ENUM('N','Y')", Default: &defN},
				{Name: "Alter_routine_priv", Type: "ENUM('N','Y')", Default: &defN},
				{Name: "Execute_priv", Type: "ENUM('N','Y')", Default: &defN},
				{Name: "Event_priv", Type: "ENUM('N','Y')", Default: &defN},
				{Name: "Trigger_priv", Type: "ENUM('N','Y')", Default: &defN},
			}
		}(),
	})
	logDefaultTS := "CURRENT_TIMESTAMP(6)"
	ensure("mysql", &catalog.TableDef{
		Name:    "general_log",
		Engine:  "CSV",
		Charset: "utf8",
		Comment: "General log",
		Columns: []catalog.ColumnDef{
			{Name: "event_time", Type: "TIMESTAMP(6)", Default: &logDefaultTS, OnUpdateCurrentTimestamp: true},
			{Name: "user_host", Type: "MEDIUMTEXT"},
			{Name: "thread_id", Type: "BIGINT(21) UNSIGNED"},
			{Name: "server_id", Type: "INT(10) UNSIGNED"},
			{Name: "command_type", Type: "VARCHAR(64)"},
			{Name: "argument", Type: "MEDIUMBLOB"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name:    "slow_log",
		Engine:  "CSV",
		Charset: "utf8",
		Comment: "Slow log",
		Columns: []catalog.ColumnDef{
			{Name: "start_time", Type: "TIMESTAMP(6)", Default: &logDefaultTS, OnUpdateCurrentTimestamp: true},
			{Name: "user_host", Type: "MEDIUMTEXT"},
			{Name: "query_time", Type: "TIME(6)"},
			{Name: "lock_time", Type: "TIME(6)"},
			{Name: "rows_sent", Type: "INT(11)"},
			{Name: "rows_examined", Type: "INT(11)"},
			{Name: "db", Type: "VARCHAR(512)"},
			{Name: "last_insert_id", Type: "INT(11)"},
			{Name: "insert_id", Type: "INT(11)"},
			{Name: "server_id", Type: "INT(10) UNSIGNED"},
			{Name: "sql_text", Type: "MEDIUMBLOB"},
			{Name: "thread_id", Type: "BIGINT(21) UNSIGNED"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "server_cost",
		Columns: []catalog.ColumnDef{
			{Name: "cost_name", Type: "VARCHAR(64)"},
			{Name: "cost_value", Type: "FLOAT", Nullable: true},
			{Name: "last_update", Type: "TIMESTAMP"},
			{Name: "comment", Type: "VARCHAR(1024)", Nullable: true},
			{Name: "default_value", Type: "FLOAT", Nullable: true},
		},
	})
	if e.tableRowCount("mysql", "server_cost") == 0 {
		_, _ = e.Execute(`INSERT INTO mysql.server_cost (cost_name, cost_value, last_update, comment, default_value) VALUES` +
			` ('disk_temptable_create_cost', NULL, CURRENT_TIMESTAMP, NULL, 20.0),` +
			` ('disk_temptable_row_cost', NULL, CURRENT_TIMESTAMP, NULL, 0.5),` +
			` ('key_compare_cost', NULL, CURRENT_TIMESTAMP, NULL, 0.05),` +
			` ('memory_temptable_create_cost', NULL, CURRENT_TIMESTAMP, NULL, 1.0),` +
			` ('memory_temptable_row_cost', NULL, CURRENT_TIMESTAMP, NULL, 0.1),` +
			` ('row_evaluate_cost', NULL, CURRENT_TIMESTAMP, NULL, 0.1)`)
	}
	ensure("mysql", &catalog.TableDef{
		Name: "engine_cost",
		Columns: []catalog.ColumnDef{
			{Name: "engine_name", Type: "VARCHAR(64)"},
			{Name: "device_type", Type: "INT"},
			{Name: "cost_name", Type: "VARCHAR(64)"},
			{Name: "cost_value", Type: "FLOAT", Nullable: true},
			{Name: "last_update", Type: "TIMESTAMP"},
			{Name: "comment", Type: "VARCHAR(1024)", Nullable: true},
			{Name: "default_value", Type: "FLOAT", Nullable: true},
		},
	})
	if e.tableRowCount("mysql", "engine_cost") == 0 {
		_, _ = e.Execute(`INSERT INTO mysql.engine_cost (engine_name, device_type, cost_name, cost_value, last_update, comment, default_value) VALUES` +
			` ('default', 0, 'io_block_read_cost', NULL, CURRENT_TIMESTAMP, NULL, 1.0),` +
			` ('default', 0, 'memory_block_read_cost', NULL, CURRENT_TIMESTAMP, NULL, 0.25)`)
	}
	ensure("mysql", &catalog.TableDef{
		Name: "tables_priv",
		Columns: []catalog.ColumnDef{
			{Name: "Host", Type: "VARCHAR(255)"},
			{Name: "Db", Type: "VARCHAR(64)"},
			{Name: "User", Type: "VARCHAR(32)"},
			{Name: "Table_name", Type: "VARCHAR(64)"},
			{Name: "Grantor", Type: "VARCHAR(288)"},
			{Name: "Timestamp", Type: "TIMESTAMP"},
			{Name: "Table_priv", Type: "VARCHAR(200)"},
			{Name: "Column_priv", Type: "VARCHAR(200)"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "columns_priv",
		Columns: []catalog.ColumnDef{
			{Name: "Host", Type: "VARCHAR(255)"},
			{Name: "Db", Type: "VARCHAR(64)"},
			{Name: "User", Type: "VARCHAR(32)"},
			{Name: "Table_name", Type: "VARCHAR(64)"},
			{Name: "Column_name", Type: "VARCHAR(64)"},
			{Name: "Timestamp", Type: "TIMESTAMP"},
			{Name: "Column_priv", Type: "VARCHAR(200)"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "role_edges",
		Columns: []catalog.ColumnDef{
			{Name: "FROM_HOST", Type: "VARCHAR(255)"},
			{Name: "FROM_USER", Type: "VARCHAR(32)"},
			{Name: "TO_HOST", Type: "VARCHAR(255)"},
			{Name: "TO_USER", Type: "VARCHAR(32)"},
			{Name: "WITH_ADMIN_OPTION", Type: "VARCHAR(1)"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "default_roles",
		Columns: []catalog.ColumnDef{
			{Name: "HOST", Type: "VARCHAR(255)"},
			{Name: "USER", Type: "VARCHAR(32)"},
			{Name: "DEFAULT_ROLE_HOST", Type: "VARCHAR(255)"},
			{Name: "DEFAULT_ROLE_USER", Type: "VARCHAR(32)"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "func",
		Columns: []catalog.ColumnDef{
			{Name: "name", Type: "VARCHAR(64)"},
			{Name: "ret", Type: "INT"},
			{Name: "dl", Type: "VARCHAR(128)"},
			{Name: "type", Type: "VARCHAR(10)"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "procs_priv",
		Columns: []catalog.ColumnDef{
			{Name: "Host", Type: "VARCHAR(255)"},
			{Name: "Db", Type: "VARCHAR(64)"},
			{Name: "User", Type: "VARCHAR(32)"},
			{Name: "Routine_name", Type: "VARCHAR(64)"},
			{Name: "Routine_type", Type: "VARCHAR(20)"},
			{Name: "Grantor", Type: "VARCHAR(288)"},
			{Name: "Proc_priv", Type: "VARCHAR(200)"},
			{Name: "Timestamp", Type: "TIMESTAMP"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "component",
		Columns: []catalog.ColumnDef{
			{Name: "component_id", Type: "INT UNSIGNED"},
			{Name: "component_group_id", Type: "INT UNSIGNED"},
			{Name: "component_urn", Type: "TEXT"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "global_grants",
		Columns: []catalog.ColumnDef{
			{Name: "USER", Type: "VARCHAR(32)"},
			{Name: "HOST", Type: "VARCHAR(255)"},
			{Name: "PRIV", Type: "VARCHAR(32)"},
			{Name: "WITH_GRANT_OPTION", Type: "VARCHAR(1)"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "gtid_executed",
		Columns: []catalog.ColumnDef{
			{Name: "source_uuid", Type: "CHAR(36)"},
			{Name: "interval_start", Type: "BIGINT"},
			{Name: "interval_end", Type: "BIGINT"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "help_category",
		Columns: []catalog.ColumnDef{
			{Name: "help_category_id", Type: "SMALLINT UNSIGNED"},
			{Name: "name", Type: "VARCHAR(64)"},
			{Name: "parent_category_id", Type: "SMALLINT UNSIGNED"},
			{Name: "url", Type: "TEXT"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "help_keyword",
		Columns: []catalog.ColumnDef{
			{Name: "help_keyword_id", Type: "INT UNSIGNED"},
			{Name: "name", Type: "VARCHAR(64)"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "help_relation",
		Columns: []catalog.ColumnDef{
			{Name: "help_topic_id", Type: "INT UNSIGNED"},
			{Name: "help_keyword_id", Type: "INT UNSIGNED"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "help_topic",
		Columns: []catalog.ColumnDef{
			{Name: "help_topic_id", Type: "INT UNSIGNED"},
			{Name: "name", Type: "VARCHAR(64)"},
			{Name: "help_category_id", Type: "SMALLINT UNSIGNED"},
			{Name: "description", Type: "TEXT"},
			{Name: "example", Type: "TEXT"},
			{Name: "url", Type: "TEXT"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "password_history",
		Columns: []catalog.ColumnDef{
			{Name: "Host", Type: "VARCHAR(255)"},
			{Name: "User", Type: "VARCHAR(32)"},
			{Name: "Password_timestamp", Type: "TIMESTAMP(6)"},
			{Name: "Password", Type: "TEXT"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "plugin",
		Columns: []catalog.ColumnDef{
			{Name: "name", Type: "VARCHAR(64)"},
			{Name: "dl", Type: "VARCHAR(128)"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "proxies_priv",
		Columns: []catalog.ColumnDef{
			{Name: "Host", Type: "VARCHAR(255)"},
			{Name: "User", Type: "VARCHAR(32)"},
			{Name: "Proxied_host", Type: "VARCHAR(255)"},
			{Name: "Proxied_user", Type: "VARCHAR(32)"},
			{Name: "With_grant", Type: "TINYINT"},
			{Name: "Grantor", Type: "VARCHAR(288)"},
			{Name: "Timestamp", Type: "TIMESTAMP"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "servers",
		Columns: []catalog.ColumnDef{
			{Name: "Server_name", Type: "VARCHAR(64)"},
			{Name: "Host", Type: "VARCHAR(255)"},
			{Name: "Db", Type: "VARCHAR(64)"},
			{Name: "Username", Type: "VARCHAR(64)"},
			{Name: "Password", Type: "VARCHAR(64)"},
			{Name: "Port", Type: "INT"},
			{Name: "Socket", Type: "VARCHAR(64)"},
			{Name: "Wrapper", Type: "VARCHAR(64)"},
			{Name: "Owner", Type: "VARCHAR(64)"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "slave_master_info",
		Columns: []catalog.ColumnDef{
			{Name: "Number_of_lines", Type: "INT UNSIGNED"},
			{Name: "Master_log_name", Type: "TEXT"},
			{Name: "Master_log_pos", Type: "BIGINT UNSIGNED"},
			{Name: "Host", Type: "VARCHAR(255)"},
			{Name: "User_name", Type: "TEXT"},
			{Name: "User_password", Type: "TEXT"},
			{Name: "Port", Type: "INT UNSIGNED"},
			{Name: "Connect_retry", Type: "INT UNSIGNED"},
			{Name: "Enabled_ssl", Type: "TINYINT UNSIGNED"},
			{Name: "Ssl_ca", Type: "TEXT"},
			{Name: "Ssl_capath", Type: "TEXT"},
			{Name: "Ssl_cert", Type: "TEXT"},
			{Name: "Ssl_cipher", Type: "TEXT"},
			{Name: "Ssl_key", Type: "TEXT"},
			{Name: "Ssl_verify_server_cert", Type: "TINYINT UNSIGNED"},
			{Name: "Heartbeat", Type: "FLOAT"},
			{Name: "Bind", Type: "TEXT"},
			{Name: "Ignored_server_ids", Type: "TEXT"},
			{Name: "Uuid", Type: "TEXT"},
			{Name: "Retry_count", Type: "BIGINT UNSIGNED"},
			{Name: "Ssl_crl", Type: "TEXT"},
			{Name: "Ssl_crlpath", Type: "TEXT"},
			{Name: "Enabled_auto_position", Type: "TINYINT UNSIGNED"},
			{Name: "Channel_name", Type: "VARCHAR(64)"},
			{Name: "Tls_version", Type: "TEXT"},
			{Name: "Public_key_path", Type: "TEXT"},
			{Name: "Get_public_key", Type: "TINYINT UNSIGNED"},
			{Name: "Network_namespace", Type: "TEXT"},
			{Name: "Master_compression_algorithm", Type: "VARCHAR(64)"},
			{Name: "Master_zstd_compression_level", Type: "INT UNSIGNED"},
			{Name: "Tls_ciphersuites", Type: "TEXT"},
			{Name: "Source_connection_auto_failover", Type: "TINYINT UNSIGNED"},
			{Name: "Gtid_only", Type: "TINYINT UNSIGNED"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "slave_relay_log_info",
		Columns: []catalog.ColumnDef{
			{Name: "Number_of_lines", Type: "INT UNSIGNED"},
			{Name: "Relay_log_name", Type: "TEXT"},
			{Name: "Relay_log_pos", Type: "BIGINT UNSIGNED"},
			{Name: "Master_log_name", Type: "TEXT"},
			{Name: "Master_log_pos", Type: "BIGINT UNSIGNED"},
			{Name: "Sql_delay", Type: "INT"},
			{Name: "Number_of_workers", Type: "INT UNSIGNED"},
			{Name: "Id", Type: "INT UNSIGNED"},
			{Name: "Channel_name", Type: "VARCHAR(64)"},
			{Name: "Privilege_checks_username", Type: "TEXT"},
			{Name: "Privilege_checks_hostname", Type: "TEXT"},
			{Name: "Require_row_format", Type: "TINYINT UNSIGNED"},
			{Name: "Require_table_primary_key_check", Type: "ENUM('STREAM','ON','OFF','GENERATE')"},
			{Name: "Assign_gtids_to_anonymous_transactions_type", Type: "ENUM('OFF','LOCAL','UUID')"},
			{Name: "Assign_gtids_to_anonymous_transactions_value", Type: "TEXT"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "slave_worker_info",
		Columns: []catalog.ColumnDef{
			{Name: "Id", Type: "INT UNSIGNED"},
			{Name: "Relay_log_name", Type: "TEXT"},
			{Name: "Relay_log_pos", Type: "BIGINT UNSIGNED"},
			{Name: "Master_log_name", Type: "TEXT"},
			{Name: "Master_log_pos", Type: "BIGINT UNSIGNED"},
			{Name: "Checkpoint_relay_log_name", Type: "TEXT"},
			{Name: "Checkpoint_relay_log_pos", Type: "BIGINT UNSIGNED"},
			{Name: "Checkpoint_master_log_name", Type: "TEXT"},
			{Name: "Checkpoint_master_log_pos", Type: "BIGINT UNSIGNED"},
			{Name: "Checkpoint_seqno", Type: "INT UNSIGNED"},
			{Name: "Checkpoint_group_size", Type: "INT UNSIGNED"},
			{Name: "Checkpoint_group_bitmap", Type: "BLOB"},
			{Name: "Channel_name", Type: "VARCHAR(64)"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "time_zone",
		Columns: []catalog.ColumnDef{
			{Name: "Time_zone_id", Type: "INT UNSIGNED"},
			{Name: "Use_leap_seconds", Type: "VARCHAR(1)"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "time_zone_leap_second",
		Columns: []catalog.ColumnDef{
			{Name: "Transition_time", Type: "BIGINT"},
			{Name: "Correction", Type: "INT"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "time_zone_name",
		Columns: []catalog.ColumnDef{
			{Name: "Name", Type: "VARCHAR(64)"},
			{Name: "Time_zone_id", Type: "INT UNSIGNED"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "time_zone_transition",
		Columns: []catalog.ColumnDef{
			{Name: "Time_zone_id", Type: "INT UNSIGNED"},
			{Name: "Transition_time", Type: "BIGINT"},
			{Name: "Transition_type_id", Type: "INT UNSIGNED"},
		},
	})
	ensure("mysql", &catalog.TableDef{
		Name: "time_zone_transition_type",
		Columns: []catalog.ColumnDef{
			{Name: "Time_zone_id", Type: "INT UNSIGNED"},
			{Name: "Transition_type_id", Type: "INT UNSIGNED"},
			{Name: "Offset", Type: "INT"},
			{Name: "Is_DST", Type: "TINYINT UNSIGNED"},
			{Name: "Abbreviation", Type: "VARCHAR(8)"},
		},
	})
}
