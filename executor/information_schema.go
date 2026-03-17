package executor

import (
	"sort"
	"strings"

	"github.com/myuon/mylite/storage"
)

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
	"innodb_metrics":           {"NAME", "COUNT", "TYPE"},
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
	"innodb_temp_table_info":   {"TABLE_ID", "NAME", "N_COLS", "SPACE"},
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
			"key_column_usage", "referential_constraints", "innodb_temp_table_info":
			return true
		}
		return false
	}
	if q == "performance_schema" {
		return t == "memory_summary_global_by_event_name"
	}
	// No qualifier: check if current DB is information_schema
	if q == "" && strings.ToLower(e.CurrentDB) == "information_schema" {
		return e.isInformationSchemaTable("information_schema", tableName)
	}
	// Some mysql tests reference these INFORMATION_SCHEMA tables without qualifier.
	if q == "" {
		return e.isInformationSchemaTable("information_schema", tableName)
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
		rawRows = []storage.Row{{"NAME": "", "COUNT": int64(0), "TYPE": "counter"}}
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
		rawRows = []storage.Row{{"ID": int64(1), "USER": "root", "HOST": "localhost", "DB": e.CurrentDB, "COMMAND": "Sleep", "TIME": int64(0), "STATE": "", "INFO": nil}}
	case "key_column_usage":
		rawRows = []storage.Row{{"CONSTRAINT_CATALOG": "def", "CONSTRAINT_SCHEMA": "", "CONSTRAINT_NAME": "", "TABLE_CATALOG": "def", "TABLE_SCHEMA": "", "TABLE_NAME": "", "COLUMN_NAME": "", "ORDINAL_POSITION": int64(1), "POSITION_IN_UNIQUE_CONSTRAINT": nil, "REFERENCED_TABLE_SCHEMA": nil, "REFERENCED_TABLE_NAME": nil, "REFERENCED_COLUMN_NAME": nil}}
	case "referential_constraints":
		rawRows = []storage.Row{{"CONSTRAINT_CATALOG": "def", "CONSTRAINT_SCHEMA": "", "CONSTRAINT_NAME": "", "UNIQUE_CONSTRAINT_CATALOG": "def", "UNIQUE_CONSTRAINT_SCHEMA": "", "UNIQUE_CONSTRAINT_NAME": "", "MATCH_OPTION": "NONE", "UPDATE_RULE": "RESTRICT", "DELETE_RULE": "RESTRICT", "TABLE_NAME": "", "REFERENCED_TABLE_NAME": ""}}
	case "innodb_temp_table_info":
		rawRows = []storage.Row{{"TABLE_ID": int64(0), "NAME": "", "N_COLS": int64(0), "SPACE": int64(0)}}
	case "memory_summary_global_by_event_name":
		rawRows = e.perfSchemaMemorySummary()
	}

	result := make([]storage.Row, len(rawRows))
	for i, row := range rawRows {
		newRow := make(storage.Row, len(row)*2)
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
			tblDef, _ := db.GetTable(tblName)
			tblComment := ""
			if tblDef != nil {
				tblComment = tblDef.Comment
			}
			rows = append(rows, storage.Row{
				"TABLE_CATALOG":   "def",
				"TABLE_SCHEMA":    dbName,
				"TABLE_NAME":      tblName,
				"TABLE_TYPE":      "BASE TABLE",
				"ENGINE":          "InnoDB",
				"VERSION":         int64(10),
				"ROW_FORMAT":      "Dynamic",
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
				"TABLE_COLLATION": "utf8mb4_general_ci",
				"CHECKSUM":        nil,
				"CREATE_OPTIONS":  "",
				"TABLE_COMMENT":   tblComment,
			})
		}
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
				dataType = strings.TrimSpace(dataType)

				var charMaxLen interface{}
				var charOctetLen interface{}
				var numPrecision interface{}
				var numScale interface{}
				switch strings.ToUpper(strings.TrimSpace(col.Type[:min(len(col.Type), 10)])) {
				case "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT":
					charMaxLen = nil
					charOctetLen = nil
				case "INT", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT":
					numPrecision = int64(10)
					numScale = int64(0)
				case "FLOAT", "DOUBLE", "DECIMAL":
					numPrecision = nil
					numScale = nil
				}

				columnKey := ""
				extra := ""
				if col.PrimaryKey {
					columnKey = "PRI"
				} else if col.Unique {
					columnKey = "UNI"
				}
				if col.AutoIncrement {
					extra = "auto_increment"
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
					"DATETIME_PRECISION":       nil,
					"CHARACTER_SET_NAME":       nil,
					"COLLATION_NAME":           nil,
					"COLUMN_TYPE":              strings.ToLower(col.Type),
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
			seqInIndex := int64(0)
			// Add primary key entries
			for i, pkCol := range tbl.PrimaryKey {
				seqInIndex = int64(i + 1)
				rows = append(rows, storage.Row{
					"TABLE_CATALOG": "def",
					"TABLE_SCHEMA":  dbName,
					"TABLE_NAME":    tblName,
					"NON_UNIQUE":    int64(0),
					"INDEX_SCHEMA":  dbName,
					"INDEX_NAME":    "PRIMARY",
					"SEQ_IN_INDEX":  seqInIndex,
					"COLUMN_NAME":   pkCol,
					"COLLATION":     "A",
					"CARDINALITY":   nil,
					"SUB_PART":      nil,
					"PACKED":        nil,
					"NULLABLE":      "",
					"INDEX_TYPE":    "BTREE",
					"COMMENT":       "",
					"INDEX_COMMENT": "",
					"IS_VISIBLE":    "YES",
					"EXPRESSION":    nil,
				})
			}
			// Add index entries
			for _, idx := range tbl.Indexes {
				nonUnique := int64(1)
				if idx.Unique {
					nonUnique = 0
				}
				for i, col := range idx.Columns {
					rows = append(rows, storage.Row{
						"TABLE_CATALOG": "def",
						"TABLE_SCHEMA":  dbName,
						"TABLE_NAME":    tblName,
						"NON_UNIQUE":    nonUnique,
						"INDEX_SCHEMA":  dbName,
						"INDEX_NAME":    idx.Name,
						"SEQ_IN_INDEX":  int64(i + 1),
						"COLUMN_NAME":   col,
						"COLLATION":     "A",
						"CARDINALITY":   nil,
						"SUB_PART":      nil,
						"PACKED":        nil,
						"NULLABLE":      "",
						"INDEX_TYPE":    "BTREE",
						"COMMENT":       "",
						"INDEX_COMMENT": idx.Comment,
						"IS_VISIBLE":    "YES",
						"EXPRESSION":    nil,
					})
				}
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
// mylite only supports InnoDB (as a compatibility layer).
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
	}
}

// perfSchemaMemorySummary returns rows for performance_schema.memory_summary_global_by_event_name.
func (e *Executor) perfSchemaMemorySummary() []storage.Row {
	return []storage.Row{
		{
			"EVENT_NAME":                   "memory/sql/JSON",
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
			"event_name":                   "memory/sql/JSON",
			"count_alloc":                  int64(1),
			"count_free":                   int64(1),
			"sum_number_of_bytes_alloc":    int64(1024),
			"sum_number_of_bytes_free":     int64(1024),
		},
	}
}
