package executor

import (
	"sort"
	"strings"

	"github.com/myuon/mylite/storage"
)

// infoSchemaColumnOrder defines the canonical column order for INFORMATION_SCHEMA tables.
var infoSchemaColumnOrder = map[string][]string{
	"schemata": {"CATALOG_NAME", "SCHEMA_NAME", "DEFAULT_CHARACTER_SET_NAME", "DEFAULT_COLLATION_NAME", "SQL_PATH"},
	"tables": {"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "ENGINE", "VERSION", "ROW_FORMAT", "TABLE_ROWS", "AVG_ROW_LENGTH", "DATA_LENGTH", "MAX_DATA_LENGTH", "INDEX_LENGTH", "DATA_FREE", "AUTO_INCREMENT", "CREATE_TIME", "UPDATE_TIME", "CHECK_TIME", "TABLE_COLLATION", "CHECKSUM", "CREATE_OPTIONS", "TABLE_COMMENT"},
	"columns":    {"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "COLUMN_DEFAULT", "IS_NULLABLE", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE", "DATETIME_PRECISION", "CHARACTER_SET_NAME", "COLLATION_NAME", "COLUMN_TYPE", "COLUMN_KEY", "EXTRA", "PRIVILEGES", "COLUMN_COMMENT", "GENERATION_EXPRESSION", "SRS_ID"},
	"statistics": {"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "NON_UNIQUE", "INDEX_SCHEMA", "INDEX_NAME", "SEQ_IN_INDEX", "COLUMN_NAME", "COLLATION", "CARDINALITY", "SUB_PART", "PACKED", "NULLABLE", "INDEX_TYPE", "COMMENT", "INDEX_COMMENT", "IS_VISIBLE", "EXPRESSION"},
}

// isInformationSchemaTable returns (dbName, tableName, true) when the provided
// AliasedTableExpr refers to an INFORMATION_SCHEMA virtual table, either via an
// explicit qualifier (information_schema.tables) or when the current database is
// "information_schema" and the table name matches a known virtual table.
func (e *Executor) isInformationSchemaTable(qualifier, tableName string) bool {
	q := strings.ToLower(qualifier)
	t := strings.ToLower(tableName)
	if q == "information_schema" {
		return t == "tables" || t == "columns" || t == "schemata" || t == "statistics"
	}
	// No qualifier: check if current DB is information_schema
	if q == "" && strings.ToLower(e.CurrentDB) == "information_schema" {
		return t == "tables" || t == "columns" || t == "schemata" || t == "statistics"
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

// infoSchemaSchemata returns rows for INFORMATION_SCHEMA.SCHEMATA.
func (e *Executor) infoSchemaSchemata() []storage.Row {
	dbNames := e.Catalog.ListDatabases()
	sort.Strings(dbNames)

	rows := make([]storage.Row, 0, len(dbNames))
	for _, db := range dbNames {
		rows = append(rows, storage.Row{
			"CATALOG_NAME":              "def",
			"SCHEMA_NAME":               db,
			"DEFAULT_CHARACTER_SET_NAME": "utf8mb4",
			"DEFAULT_COLLATION_NAME":    "utf8mb4_general_ci",
			"SQL_PATH":                  nil,
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
					"COLUMN_COMMENT":           "",
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
						"INDEX_COMMENT": "",
						"IS_VISIBLE":    "YES",
						"EXPRESSION":    nil,
					})
				}
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
