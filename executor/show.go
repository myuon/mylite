package executor

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
	"vitess.io/vitess/go/vt/sqlparser"
)

// execDescribe handles DESCRIBE <table> and DESC <table> (parsed as *sqlparser.ExplainTab).
func (e *Executor) execDescribe(stmt *sqlparser.ExplainTab) (*Result, error) {
	tableName := stmt.Table.Name.String()
	if q := stmt.Table.Qualifier.String(); q != "" {
		tableName = q + "." + tableName
	}
	return e.describeTable(tableName)
}

func findTableDefCaseInsensitive(db *catalog.Database, tableName string) (*catalog.TableDef, string, error) {
	if def, err := db.GetTable(tableName); err == nil {
		return def, tableName, nil
	}
	for _, name := range db.ListTables() {
		if strings.EqualFold(name, tableName) {
			def, err := db.GetTable(name)
			if err == nil {
				return def, name, nil
			}
		}
	}
	return nil, tableName, fmt.Errorf("table '%s' doesn't exist", tableName)
}

func findDatabaseCaseInsensitive(cat *catalog.Catalog, dbName string) (*catalog.Database, string, error) {
	if db, err := cat.GetDatabase(dbName); err == nil {
		return db, dbName, nil
	}
	for _, name := range cat.ListDatabases() {
		if strings.EqualFold(name, dbName) {
			db, err := cat.GetDatabase(name)
			if err == nil {
				return db, name, nil
			}
		}
	}
	return nil, dbName, fmt.Errorf("unknown database '%s'", dbName)
}

func isInfoSchemaTable(dbName string) bool {
	return strings.EqualFold(dbName, "information_schema")
}

// isColMeta holds column metadata for DESCRIBE output of IS tables.
type isColMeta struct {
	colType  string
	nullable string // "YES" or "NO"
	defVal   interface{}
}

// infoSchemaColumnMeta maps IS table+column to metadata for DESCRIBE.
var infoSchemaColumnMeta = map[string]map[string]isColMeta{
	"character_sets": {
		"CHARACTER_SET_NAME":   {colType: "varchar(64)", nullable: "NO", defVal: nil},
		"DEFAULT_COLLATE_NAME": {colType: "varchar(64)", nullable: "NO", defVal: nil},
		"DESCRIPTION":          {colType: "varchar(2048)", nullable: "NO", defVal: nil},
		"MAXLEN":               {colType: "int(10) unsigned", nullable: "NO", defVal: nil},
	},
	"collations": {
		"COLLATION_NAME":     {colType: "varchar(64)", nullable: "NO", defVal: nil},
		"CHARACTER_SET_NAME": {colType: "varchar(64)", nullable: "NO", defVal: nil},
		"ID":                 {colType: "bigint(20) unsigned", nullable: "NO", defVal: "0"},
		"IS_DEFAULT":         {colType: "varchar(3)", nullable: "NO", defVal: ""},
		"IS_COMPILED":        {colType: "varchar(3)", nullable: "NO", defVal: ""},
		"SORTLEN":            {colType: "int(10) unsigned", nullable: "NO", defVal: nil},
		"PAD_ATTRIBUTE":      {colType: "enum('PAD SPACE','NO PAD')", nullable: "NO", defVal: nil},
	},
	"collation_character_set_applicability": {
		"COLLATION_NAME":     {colType: "varchar(64)", nullable: "NO", defVal: nil},
		"CHARACTER_SET_NAME": {colType: "varchar(64)", nullable: "NO", defVal: nil},
	},
	"engines": {
		"ENGINE":       {colType: "varchar(64)", nullable: "NO", defVal: ""},
		"SUPPORT":      {colType: "varchar(8)", nullable: "NO", defVal: ""},
		"COMMENT":      {colType: "varchar(80)", nullable: "NO", defVal: ""},
		"TRANSACTIONS": {colType: "varchar(3)", nullable: "YES", defVal: ""},
		"XA":           {colType: "varchar(3)", nullable: "YES", defVal: ""},
		"SAVEPOINTS":   {colType: "varchar(3)", nullable: "YES", defVal: ""},
	},
	"user_privileges": {
		"GRANTEE":        {colType: "varchar(292)", nullable: "NO", defVal: ""},
		"TABLE_CATALOG":  {colType: "varchar(512)", nullable: "NO", defVal: ""},
		"PRIVILEGE_TYPE": {colType: "varchar(64)", nullable: "NO", defVal: ""},
		"IS_GRANTABLE":   {colType: "varchar(3)", nullable: "NO", defVal: ""},
	},
	"schema_privileges": {
		"GRANTEE":        {colType: "varchar(292)", nullable: "NO", defVal: ""},
		"TABLE_CATALOG":  {colType: "varchar(512)", nullable: "NO", defVal: ""},
		"TABLE_SCHEMA":   {colType: "varchar(64)", nullable: "NO", defVal: ""},
		"PRIVILEGE_TYPE": {colType: "varchar(64)", nullable: "NO", defVal: ""},
		"IS_GRANTABLE":   {colType: "varchar(3)", nullable: "NO", defVal: ""},
	},
	"table_privileges": {
		"GRANTEE":        {colType: "varchar(292)", nullable: "NO", defVal: ""},
		"TABLE_CATALOG":  {colType: "varchar(512)", nullable: "NO", defVal: ""},
		"TABLE_SCHEMA":   {colType: "varchar(64)", nullable: "NO", defVal: ""},
		"TABLE_NAME":     {colType: "varchar(64)", nullable: "NO", defVal: ""},
		"PRIVILEGE_TYPE": {colType: "varchar(64)", nullable: "NO", defVal: ""},
		"IS_GRANTABLE":   {colType: "varchar(3)", nullable: "NO", defVal: ""},
	},
	"column_privileges": {
		"GRANTEE":        {colType: "varchar(292)", nullable: "NO", defVal: ""},
		"TABLE_CATALOG":  {colType: "varchar(512)", nullable: "NO", defVal: ""},
		"TABLE_SCHEMA":   {colType: "varchar(64)", nullable: "NO", defVal: ""},
		"TABLE_NAME":     {colType: "varchar(64)", nullable: "NO", defVal: ""},
		"COLUMN_NAME":    {colType: "varchar(64)", nullable: "NO", defVal: ""},
		"PRIVILEGE_TYPE": {colType: "varchar(64)", nullable: "NO", defVal: ""},
		"IS_GRANTABLE":   {colType: "varchar(3)", nullable: "NO", defVal: ""},
	},
}

// infoSchemaCreateView maps IS table names to their MySQL view definitions (returned as VIEW).
var infoSchemaCreateView = map[string]string{
	"character_sets": "CREATE ALGORITHM=UNDEFINED DEFINER=`mysql.infoschema`@`localhost` SQL SECURITY DEFINER VIEW `information_schema`.`CHARACTER_SETS` AS select `cs`.`name` AS `CHARACTER_SET_NAME`,`col`.`name` AS `DEFAULT_COLLATE_NAME`,`cs`.`comment` AS `DESCRIPTION`,`cs`.`mb_max_length` AS `MAXLEN` from (`mysql`.`character_sets` `cs` join `mysql`.`collations` `col` on((`cs`.`default_collation_id` = `col`.`id`)))",
	"collations": "CREATE ALGORITHM=UNDEFINED DEFINER=`mysql.infoschema`@`localhost` SQL SECURITY DEFINER VIEW `information_schema`.`COLLATIONS` AS select `col`.`name` AS `COLLATION_NAME`,`cs`.`name` AS `CHARACTER_SET_NAME`,`col`.`id` AS `ID`,if(exists(select 1 from `mysql`.`character_sets` where (`mysql`.`character_sets`.`default_collation_id` = `col`.`id`)),'Yes','') AS `IS_DEFAULT`,if(`col`.`is_compiled`,'Yes','') AS `IS_COMPILED`,`col`.`sort_length` AS `SORTLEN`,`col`.`pad_attribute` AS `PAD_ATTRIBUTE` from (`mysql`.`collations` `col` join `mysql`.`character_sets` `cs` on((`col`.`character_set_id` = `cs`.`id`)))",
	"collation_character_set_applicability": "CREATE ALGORITHM=UNDEFINED DEFINER=`mysql.infoschema`@`localhost` SQL SECURITY DEFINER VIEW `information_schema`.`COLLATION_CHARACTER_SET_APPLICABILITY` AS select `col`.`name` AS `COLLATION_NAME`,`cs`.`name` AS `CHARACTER_SET_NAME` from (`mysql`.`character_sets` `cs` join `mysql`.`collations` `col` on((`cs`.`id` = `col`.`character_set_id`)))",
}

// infoSchemaCreateTable maps IS table names to CREATE TEMPORARY TABLE statements (for older IS tables).
var infoSchemaCreateTable = map[string]string{
	"engines":           "CREATE TEMPORARY TABLE `ENGINES` (\n  `ENGINE` varchar(64) NOT NULL DEFAULT '',\n  `SUPPORT` varchar(8) NOT NULL DEFAULT '',\n  `COMMENT` varchar(80) NOT NULL DEFAULT '',\n  `TRANSACTIONS` varchar(3) DEFAULT NULL,\n  `XA` varchar(3) DEFAULT NULL,\n  `SAVEPOINTS` varchar(3) DEFAULT NULL\n) ENGINE=MEMORY DEFAULT CHARSET=utf8",
	"user_privileges":   "CREATE TEMPORARY TABLE `USER_PRIVILEGES` (\n  `GRANTEE` varchar(292) NOT NULL DEFAULT '',\n  `TABLE_CATALOG` varchar(512) NOT NULL DEFAULT '',\n  `PRIVILEGE_TYPE` varchar(64) NOT NULL DEFAULT '',\n  `IS_GRANTABLE` varchar(3) NOT NULL DEFAULT ''\n) ENGINE=MEMORY DEFAULT CHARSET=utf8",
	"schema_privileges": "CREATE TEMPORARY TABLE `SCHEMA_PRIVILEGES` (\n  `GRANTEE` varchar(292) NOT NULL DEFAULT '',\n  `TABLE_CATALOG` varchar(512) NOT NULL DEFAULT '',\n  `TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT '',\n  `PRIVILEGE_TYPE` varchar(64) NOT NULL DEFAULT '',\n  `IS_GRANTABLE` varchar(3) NOT NULL DEFAULT ''\n) ENGINE=MEMORY DEFAULT CHARSET=utf8",
	"table_privileges":  "CREATE TEMPORARY TABLE `TABLE_PRIVILEGES` (\n  `GRANTEE` varchar(292) NOT NULL DEFAULT '',\n  `TABLE_CATALOG` varchar(512) NOT NULL DEFAULT '',\n  `TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT '',\n  `TABLE_NAME` varchar(64) NOT NULL DEFAULT '',\n  `PRIVILEGE_TYPE` varchar(64) NOT NULL DEFAULT '',\n  `IS_GRANTABLE` varchar(3) NOT NULL DEFAULT ''\n) ENGINE=MEMORY DEFAULT CHARSET=utf8",
	"column_privileges": "CREATE TEMPORARY TABLE `COLUMN_PRIVILEGES` (\n  `GRANTEE` varchar(292) NOT NULL DEFAULT '',\n  `TABLE_CATALOG` varchar(512) NOT NULL DEFAULT '',\n  `TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT '',\n  `TABLE_NAME` varchar(64) NOT NULL DEFAULT '',\n  `COLUMN_NAME` varchar(64) NOT NULL DEFAULT '',\n  `PRIVILEGE_TYPE` varchar(64) NOT NULL DEFAULT '',\n  `IS_GRANTABLE` varchar(3) NOT NULL DEFAULT ''\n) ENGINE=MEMORY DEFAULT CHARSET=utf8",
}

// describeInfoSchemaTable returns DESCRIBE output for an information_schema virtual table.
func describeInfoSchemaTable(tableName string, cols []string) *Result {
	lowerName := strings.ToLower(tableName)
	metaMap := infoSchemaColumnMeta[lowerName]
	rows := make([][]interface{}, 0, len(cols))
	for _, col := range cols {
		colType := "varchar(64)"
		nullable := "NO"
		var defVal interface{} = nil
		if metaMap != nil {
			if m, ok := metaMap[col]; ok {
				colType = m.colType
				nullable = m.nullable
				defVal = m.defVal
			}
		}
		rows = append(rows, []interface{}{col, colType, nullable, "", defVal, ""})
	}
	return &Result{
		Columns:     []string{"Field", "Type", "Null", "Key", "Default", "Extra"},
		Rows:        rows,
		IsResultSet: true,
	}
}

// describeTable returns column metadata for a table, matching MySQL DESCRIBE output.
func (e *Executor) describeTable(tableName string) (*Result, error) {
	descDB := e.CurrentDB
	if strings.Contains(tableName, ".") {
		descDB, tableName = resolveTableNameDB(tableName, e.CurrentDB)
	}
	db, resolvedDBName, err := findDatabaseCaseInsensitive(e.Catalog, descDB)
	if err != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", descDB))
	}
	descDB = resolvedDBName
	tblDef, resolvedTableName, err := findTableDefCaseInsensitive(db, tableName)
	if err != nil {
		// Check if it's a view — derive column info from executing the view's SELECT.
		if e.views != nil {
			viewLookup := tableName
			if _, ok := e.views[viewLookup]; !ok {
				// Try case-insensitive lookup
				for vn := range e.views {
					if strings.EqualFold(vn, viewLookup) {
						viewLookup = vn
						break
					}
				}
			}
			if viewSQL, ok := e.views[viewLookup]; ok {
				return e.describeView(viewSQL)
			}
		}
		// For information_schema virtual tables, use infoSchemaColumnOrder to build DESCRIBE output.
		if isInfoSchemaTable(descDB) {
			lowerName := strings.ToLower(tableName)
			if cols, ok := infoSchemaColumnOrder[lowerName]; ok {
				return describeInfoSchemaTable(tableName, cols), nil
			}
		}
		return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", descDB, tableName))
	}
	tableName = resolvedTableName

	cols := []string{"Field", "Type", "Null", "Key", "Default", "Extra"}
	rows := make([][]interface{}, 0, len(tblDef.Columns))
	for _, col := range tblDef.Columns {
		nullable := "YES"
		if !col.Nullable {
			nullable = "NO"
		}
		key := ""
		isPK := col.PrimaryKey
		if !isPK {
			for _, pk := range tblDef.PrimaryKey {
				if strings.EqualFold(stripPrefixLengthFromCol(pk), col.Name) {
					isPK = true
					break
				}
			}
		}
		if isPK {
			key = "PRI"
		} else if col.Unique {
			key = "UNI"
		} else {
			// Check indexes for this column
			for _, idx := range tblDef.Indexes {
				if len(idx.Columns) > 0 {
					// Strip length suffix from index column name (e.g. "col(10)" -> "col")
					idxCol := idx.Columns[0]
					if parenIdx := strings.Index(idxCol, "("); parenIdx >= 0 {
						idxCol = idxCol[:parenIdx]
					}
					if strings.EqualFold(idxCol, col.Name) {
						if idx.Unique && len(idx.Columns) == 1 {
							key = "UNI"
						} else {
							key = "MUL"
						}
						break
					}
				}
			}
		}
		var defVal interface{}
		if col.Default != nil {
			defVal = *col.Default
		} else if isInfoSchemaTable(descDB) {
			defVal = "" // empty for INFORMATION_SCHEMA columns
		} else {
			defVal = nil // NULL for columns without explicit default (user tables)
		}
		var extra interface{}
		extra = ""
		if col.AutoIncrement {
			extra = "auto_increment"
		} else if isGeneratedColumnType(col.Type) {
			if strings.Contains(strings.ToUpper(col.Type), "STORED") {
				extra = "STORED GENERATED"
			} else {
				extra = "VIRTUAL GENERATED"
			}
		}
		// For TEMPORARY tables, MySQL returns NULL for the Extra column when empty
		if e.tempTables[tableName] && extra == "" {
			extra = nil
		}
		rows = append(rows, []interface{}{col.Name, mysqlDisplayType(col.Type), nullable, key, defVal, extra})
	}

	return &Result{
		Columns:     cols,
		Rows:        rows,
		IsResultSet: true,
	}, nil
}

// describeTableFull returns column metadata for SHOW FULL COLUMNS, including
// Collation, Privileges, and Comment columns.
func (e *Executor) describeTableFull(tableName string) (*Result, error) {
	// Get the basic describe result first
	basic, err := e.describeTable(tableName)
	if err != nil {
		return nil, err
	}
	// Extend columns: Field, Type, Collation, Null, Key, Default, Extra, Privileges, Comment
	cols := []string{"Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"}
	rows := make([][]interface{}, 0, len(basic.Rows))
	for _, row := range basic.Rows {
		// row is [Field, Type, Null, Key, Default, Extra]
		colType := ""
		if len(row) > 1 && row[1] != nil {
			colType = toString(row[1])
		}
		collation := interface{}(nil)
		colTypeUpper := strings.ToUpper(colType)
		// String types get a collation
		if strings.Contains(colTypeUpper, "CHAR") || strings.Contains(colTypeUpper, "TEXT") ||
			strings.Contains(colTypeUpper, "ENUM") || strings.Contains(colTypeUpper, "SET") {
			collation = "utf8mb4_0900_ai_ci"
			// Try to extract charset/collation from column definition
			fieldName := ""
			if row[0] != nil {
				fieldName = toString(row[0])
			}
			descDB := e.CurrentDB
			descTbl := tableName
			if strings.Contains(descTbl, ".") {
				descDB, descTbl = resolveTableNameDB(descTbl, e.CurrentDB)
			}
			if dbObj, err2 := e.Catalog.GetDatabase(descDB); err2 == nil {
				if tblDef, err2 := dbObj.GetTable(descTbl); err2 == nil && tblDef != nil {
					for _, cd := range tblDef.Columns {
						if strings.EqualFold(cd.Name, fieldName) {
							// Check for CHARACTER SET in column type
							cdUpper := strings.ToUpper(cd.Type)
							if idx := strings.Index(cdUpper, "CHARACTER SET "); idx >= 0 {
								rest := cd.Type[idx+len("CHARACTER SET "):]
								parts := strings.Fields(rest)
								if len(parts) > 0 {
									cs := strings.ToLower(parts[0])
									// Map charset to default collation
									switch cs {
									case "latin1":
										collation = "latin1_swedish_ci"
									case "utf8", "utf8mb3":
										collation = "utf8_general_ci"
									case "binary":
										collation = "binary"
									case "gb2312":
										collation = "gb2312_chinese_ci"
									case "gbk":
										collation = "gbk_chinese_ci"
									case "gb18030":
										collation = "gb18030_chinese_ci"
									case "cp1250":
										collation = "cp1250_general_ci"
									case "ascii":
										collation = "ascii_general_ci"
									case "latin2":
										collation = "latin2_general_ci"
									case "cp932":
										collation = "cp932_japanese_ci"
									case "ujis":
										collation = "ujis_japanese_ci"
									case "ucs2":
										collation = "ucs2_general_ci"
									case "utf16":
										collation = "utf16_general_ci"
									case "utf32":
										collation = "utf32_general_ci"
									case "tis620":
										collation = "tis620_thai_ci"
									default:
										collation = cs + "_general_ci"
									}
								}
							}
							// Check for COLLATE in column type
							if idx := strings.Index(cdUpper, "COLLATE "); idx >= 0 {
								rest := cd.Type[idx+len("COLLATE "):]
								parts := strings.Fields(rest)
								if len(parts) > 0 {
									collation = strings.ToLower(parts[0])
								}
							}
							break
						}
					}
				}
			}
		}
		privileges := "select,insert,update,references"
		comment := ""
		newRow := []interface{}{
			row[0],     // Field
			row[1],     // Type
			collation,  // Collation
			row[2],     // Null
			row[3],     // Key
			row[4],     // Default
			row[5],     // Extra
			privileges, // Privileges
			comment,    // Comment
		}
		rows = append(rows, newRow)
	}
	return &Result{
		Columns:     cols,
		Rows:        rows,
		IsResultSet: true,
	}, nil
}

// describeView derives DESCRIBE-style column info by executing the view's SELECT
// and inferring types from the result set.
func (e *Executor) describeView(viewSQL string) (*Result, error) {
	viewResult, err := e.Execute(viewSQL)
	if err != nil {
		return nil, err
	}
	cols := []string{"Field", "Type", "Null", "Key", "Default", "Extra"}
	rows := make([][]interface{}, 0, len(viewResult.Columns))
	for i, colName := range viewResult.Columns {
		// Infer type from the first non-nil value in this column
		colType := "varchar(255)" // default for unknown
		if len(viewResult.Rows) > 0 {
			for _, row := range viewResult.Rows {
				if i < len(row) && row[i] != nil {
					switch row[i].(type) {
					case int64, uint64:
						colType = "bigint"
					case float64:
						colType = "double"
					default:
						colType = "varchar(255)"
					}
					break
				}
			}
		}
		rows = append(rows, []interface{}{colName, colType, "YES", "", nil, ""})
	}
	return &Result{
		Columns:     cols,
		Rows:        rows,
		IsResultSet: true,
	}, nil
}

func (e *Executor) execShow(stmt *sqlparser.Show, query string) (*Result, error) {
	// Dispatch based on the structured ShowBasic command type when available.
	if basic, ok := stmt.Internal.(*sqlparser.ShowBasic); ok {
		likePattern := ""
		if basic.Filter != nil {
			likePattern = basic.Filter.Like
		}
		switch basic.Command {
		case sqlparser.Column:
			// SHOW COLUMNS FROM <table> / SHOW FULL COLUMNS FROM <table>
			tblName := basic.Tbl.Name.String()
			if !basic.Tbl.Qualifier.IsEmpty() {
				tblName = basic.Tbl.Qualifier.String() + "." + tblName
			}
			if basic.Full {
				return e.describeTableFull(tblName)
			}
			return e.describeTable(tblName)
		case sqlparser.TableStatus:
			// SHOW TABLE STATUS [FROM db] [LIKE ...]
			return e.showTableStatus()
		case sqlparser.Database: // SHOW DATABASES / SHOW SCHEMAS
			dbs := e.Catalog.ListDatabases()
			sort.Strings(dbs)
			rows := make([][]interface{}, 0, len(dbs))
			for _, d := range dbs {
				if likePattern != "" && !matchLike(d, likePattern) {
					continue
				}
				rows = append(rows, []interface{}{d})
			}
			colName := "Database"
			if likePattern != "" {
				colName = fmt.Sprintf("Database (%s)", likePattern)
			}
			return &Result{Columns: []string{colName}, Rows: rows, IsResultSet: true}, nil
		case sqlparser.Charset: // SHOW CHARACTER SET
			charsets := allCharsets()
			rows := make([][]interface{}, 0)
			for _, cs := range charsets {
				if likePattern == "" || matchLike(cs[0].(string), likePattern) {
					rows = append(rows, cs)
				}
			}
			return &Result{Columns: []string{"Charset", "Description", "Default collation", "Maxlen"}, Rows: rows, IsResultSet: true}, nil
		case sqlparser.Collation: // SHOW COLLATION
			collations := allCollations()
			rows := make([][]interface{}, 0)
			for _, c := range collations {
				if likePattern == "" || matchLike(c[0].(string), likePattern) {
					rows = append(rows, c)
				}
			}
			return &Result{Columns: []string{"Collation", "Charset", "Id", "Default", "Compiled", "Sortlen", "Pad_attribute"}, Rows: rows, IsResultSet: true}, nil
		case sqlparser.Table: // SHOW TABLES
			targetDB := e.CurrentDB
			if !basic.DbName.IsEmpty() {
				targetDB = basic.DbName.String()
			}
			var tables []string
			if !strings.EqualFold(targetDB, "information_schema") {
				db, err := e.Catalog.GetDatabase(targetDB)
				if err != nil {
					return nil, err
				}
				tables = db.ListTables()
			}
			// Include views in SHOW TABLES
			if e.views != nil {
				for vn := range e.views {
					tables = append(tables, vn)
				}
			}
			// For information_schema, include virtual IS tables
			if strings.EqualFold(targetDB, "information_schema") {
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
				seen := make(map[string]bool)
				for _, t := range tables {
					seen[strings.ToUpper(t)] = true
				}
				for _, t := range isTableNames {
					if !seen[t] {
						tables = append(tables, t)
					}
				}
			}
			sort.Strings(tables)
			rows := make([][]interface{}, 0, len(tables))
			for _, t := range tables {
				// Skip temporary tables in SHOW TABLES
				if e.tempTables[t] {
					continue
				}
				if likePattern != "" && !matchLike(t, likePattern) {
					continue
				}
				rows = append(rows, []interface{}{t})
			}
			colName := fmt.Sprintf("Tables_in_%s", targetDB)
			if likePattern != "" {
				colName = fmt.Sprintf("Tables_in_%s (%s)", targetDB, likePattern)
			}
			return &Result{
				Columns:     []string{colName},
				Rows:        rows,
				IsResultSet: true,
			}, nil
		}
	}

	upper := strings.ToUpper(strings.TrimSpace(query))

	if strings.HasPrefix(upper, "SHOW TABLES") {
		targetDB := e.CurrentDB
		// Parse SHOW TABLES FROM/IN <database>
		if idx := strings.Index(upper, " FROM "); idx >= 0 {
			rest := strings.TrimSpace(query[idx+6:])
			dbName := strings.Fields(rest)[0]
			dbName = strings.Trim(dbName, "`")
			targetDB = dbName
		} else if idx := strings.Index(upper, " IN "); idx >= 0 {
			rest := strings.TrimSpace(query[idx+4:])
			dbName := strings.Fields(rest)[0]
			dbName = strings.Trim(dbName, "`")
			targetDB = dbName
		}
		// Parse optional LIKE pattern
		likePattern := ""
		if idx := strings.Index(upper, " LIKE "); idx >= 0 {
			rest := strings.TrimSpace(query[idx+6:])
			likePattern = strings.Trim(strings.TrimRight(rest, ";"), "'\"")
		}
		db, err := e.Catalog.GetDatabase(targetDB)
		if err != nil {
			return nil, err
		}
		tables := db.ListTables()
		// Include views in SHOW TABLES
		if e.views != nil {
			for vn := range e.views {
				tables = append(tables, vn)
			}
		}
		// For information_schema, include virtual IS tables
		if strings.EqualFold(targetDB, "information_schema") {
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
			seen := make(map[string]bool)
			for _, t := range tables {
				seen[strings.ToUpper(t)] = true
			}
			for _, t := range isTableNames {
				if !seen[t] {
					tables = append(tables, t)
				}
			}
		}
		sort.Strings(tables)
		rows := make([][]interface{}, 0, len(tables))
		for _, t := range tables {
			if e.tempTables[t] {
				continue
			}
			if likePattern != "" && !matchLike(t, likePattern) {
				continue
			}
			rows = append(rows, []interface{}{t})
		}
		colName := fmt.Sprintf("Tables_in_%s", targetDB)
		if likePattern != "" {
			colName = fmt.Sprintf("Tables_in_%s (%s)", targetDB, likePattern)
		}
		return &Result{
			Columns:     []string{colName},
			Rows:        rows,
			IsResultSet: true,
		}, nil
	}

	if strings.HasPrefix(upper, "SHOW DATABASES") || strings.HasPrefix(upper, "SHOW SCHEMAS") {
		dbs := e.Catalog.ListDatabases()
		sort.Strings(dbs)
		// Handle LIKE pattern
		likePattern := ""
		if idx := strings.Index(upper, "LIKE "); idx >= 0 {
			rest := strings.TrimSpace(query[idx+5:])
			likePattern = strings.Trim(rest, "'\"")
		}
		rows := make([][]interface{}, 0, len(dbs))
		for _, d := range dbs {
			if likePattern != "" && !matchLike(d, likePattern) {
				continue
			}
			rows = append(rows, []interface{}{d})
		}
		colName := "Database"
		if likePattern != "" {
			colName = fmt.Sprintf("Database (%s)", likePattern)
		}
		return &Result{
			Columns:     []string{colName},
			Rows:        rows,
			IsResultSet: true,
		}, nil
	}

	// SHOW CHARACTER SET
	if strings.HasPrefix(upper, "SHOW CHARACTER SET") || strings.HasPrefix(upper, "SHOW CHARSET") {
		likePattern := ""
		if idx := strings.Index(upper, "LIKE "); idx >= 0 {
			rest := strings.TrimSpace(query[idx+5:])
			likePattern = strings.Trim(rest, "'\"")
		}
		charsets := allCharsets()
		rows := make([][]interface{}, 0)
		for _, cs := range charsets {
			if likePattern == "" || matchLike(cs[0].(string), likePattern) {
				rows = append(rows, cs)
			}
		}
		return &Result{
			Columns:     []string{"Charset", "Description", "Default collation", "Maxlen"},
			Rows:        rows,
			IsResultSet: true,
		}, nil
	}

	// SHOW COLLATION
	if strings.HasPrefix(upper, "SHOW COLLATION") {
		likePattern := ""
		if idx := strings.Index(upper, "LIKE "); idx >= 0 {
			rest := strings.TrimSpace(query[idx+5:])
			likePattern = strings.Trim(rest, "'\"")
		}
		collations := [][]interface{}{
			{"utf8mb4_0900_ai_ci", "utf8mb4", int64(255), "Yes", "Yes", int64(0), "NO PAD"},
			{"utf8mb4_general_ci", "utf8mb4", int64(45), "", "Yes", int64(1), "PAD SPACE"},
			{"utf8_general_ci", "utf8", int64(33), "Yes", "Yes", int64(1), "PAD SPACE"},
			{"latin1_swedish_ci", "latin1", int64(8), "Yes", "Yes", int64(1), "PAD SPACE"},
			{"ascii_general_ci", "ascii", int64(11), "Yes", "Yes", int64(1), "PAD SPACE"},
			{"binary", "binary", int64(63), "Yes", "Yes", int64(1), "NO PAD"},
		}
		rows := make([][]interface{}, 0)
		for _, c := range collations {
			if likePattern == "" || matchLike(c[0].(string), likePattern) {
				rows = append(rows, c)
			}
		}
		return &Result{
			Columns:     []string{"Collation", "Charset", "Id", "Default", "Compiled", "Sortlen", "Pad_attribute"},
			Rows:        rows,
			IsResultSet: true,
		}, nil
	}

	// SHOW PROCEDURE CODE / SHOW FUNCTION CODE - feature disabled (requires debug build)
	if strings.HasPrefix(upper, "SHOW PROCEDURE CODE") || strings.HasPrefix(upper, "SHOW FUNCTION CODE") {
		return nil, mysqlError(1289, "HY000", "The 'SHOW PROCEDURE|FUNCTION CODE' feature is disabled; you need MySQL built with '--with-debug' to have it working")
	}

	// SHOW CREATE DATABASE <db>
	if strings.HasPrefix(upper, "SHOW CREATE DATABASE") || strings.HasPrefix(upper, "SHOW CREATE SCHEMA") {
		parts := strings.Fields(query)
		if len(parts) >= 4 {
			dbName := parts[3]
			dbName = strings.TrimRight(dbName, ";")
			dbName = strings.ReplaceAll(dbName, "`", "")
			db, resolvedName, err := findDatabaseCaseInsensitive(e.Catalog, dbName)
			if err != nil {
				return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", dbName))
			}
			charset := "utf8mb4"
			collation := "utf8mb4_0900_ai_ci"
			if db.CharacterSet != "" {
				charset = db.CharacterSet
			}
			if db.CollationName != "" {
				collation = db.CollationName
			}
			quotedName := "`" + resolvedName + "`"
			if v, ok := e.getSysVar("sql_quote_show_create"); ok && (v == "OFF" || v == "0") {
				quotedName = resolvedName
			}
			// Show COLLATE when charset is utf8mb4 (MySQL always shows it) or
			// when collation differs from the charset's default collation.
			var createSQL string
			defaultCollation := catalog.DefaultCollationForCharset(charset)
			if charset == "utf8mb4" || (collation != "" && collation != defaultCollation) {
				if collation == "" {
					collation = defaultCollation
				}
				createSQL = fmt.Sprintf("CREATE DATABASE %s /*!40100 DEFAULT CHARACTER SET %s COLLATE %s */ /*!80016 DEFAULT ENCRYPTION='N' */", quotedName, charset, collation)
			} else {
				createSQL = fmt.Sprintf("CREATE DATABASE %s /*!40100 DEFAULT CHARACTER SET %s */ /*!80016 DEFAULT ENCRYPTION='N' */", quotedName, charset)
			}
			return &Result{
				Columns:     []string{"Database", "Create Database"},
				Rows:        [][]interface{}{{resolvedName, createSQL}},
				IsResultSet: true,
			}, nil
		}
	}

	// SHOW CREATE TABLE <table>
	if strings.HasPrefix(upper, "SHOW CREATE TABLE") {
		parts := strings.Fields(query)
		if len(parts) >= 4 {
			tableName := strings.Join(parts[3:], " ")
			tableName = strings.TrimRight(tableName, ";")
			tableName = strings.ReplaceAll(tableName, "`", "")
			return e.showCreateTable(tableName)
		}
	}

	// SHOW CREATE PROCEDURE <name>
	if strings.HasPrefix(upper, "SHOW CREATE PROCEDURE") {
		parts := strings.Fields(query)
		if len(parts) >= 4 {
			procName := strings.Join(parts[3:], " ")
			procName = strings.TrimRight(procName, ";")
			procName = strings.ReplaceAll(procName, "`", "")
			return e.showCreateProcedure(procName)
		}
	}

	// SHOW CREATE FUNCTION <name>
	if strings.HasPrefix(upper, "SHOW CREATE FUNCTION") {
		parts := strings.Fields(query)
		if len(parts) >= 4 {
			funcName := strings.Join(parts[3:], " ")
			funcName = strings.TrimRight(funcName, ";")
			funcName = strings.ReplaceAll(funcName, "`", "")
			return e.showCreateFunction(funcName)
		}
	}

	// SHOW FUNCTION STATUS [LIKE 'pattern' | WHERE expr]
	if strings.HasPrefix(upper, "SHOW FUNCTION STATUS") {
		return e.showRoutineStatus("FUNCTION", query[len("SHOW FUNCTION STATUS"):])
	}

	// SHOW PROCEDURE STATUS [LIKE 'pattern' | WHERE expr]
	if strings.HasPrefix(upper, "SHOW PROCEDURE STATUS") {
		return e.showRoutineStatus("PROCEDURE", query[len("SHOW PROCEDURE STATUS"):])
	}

	// SHOW INDEX/INDEXES/KEYS FROM <table>
	if strings.HasPrefix(upper, "SHOW INDEX ") || strings.HasPrefix(upper, "SHOW INDEXES ") || strings.HasPrefix(upper, "SHOW KEYS ") {
		showDB, showTable, ok := parseShowIndexTarget(query, e.CurrentDB)
		if ok {
			return e.showIndexes(showDB, showTable)
		}
	}

	// SHOW VARIABLES / SHOW GLOBAL VARIABLES / SHOW SESSION VARIABLES
	if strings.HasPrefix(upper, "SHOW VARIABLES") || strings.HasPrefix(upper, "SHOW GLOBAL VARIABLES") ||
		strings.HasPrefix(upper, "SHOW SESSION VARIABLES") || strings.HasPrefix(upper, "SHOW LOCAL VARIABLES") {
		return e.showVariables(upper)
	}

	// SHOW STATUS / SHOW GLOBAL STATUS
	if strings.HasPrefix(upper, "SHOW STATUS") || strings.HasPrefix(upper, "SHOW GLOBAL STATUS") ||
		strings.HasPrefix(upper, "SHOW SESSION STATUS") {
		return e.showStatus(upper)
	}

	// SHOW WARNINGS
	if strings.HasPrefix(upper, "SHOW WARNINGS") || strings.HasPrefix(upper, "SHOW COUNT(*) WARNINGS") {
		// MySQL returns warnings in severity order: Errors first, Warnings second, Notes last.
		rows := make([][]interface{}, 0, len(e.warnings))
		for _, w := range e.warnings {
			if strings.EqualFold(w.Level, "Error") {
				rows = append(rows, []interface{}{w.Level, int64(w.Code), w.Message})
			}
		}
		for _, w := range e.warnings {
			if strings.EqualFold(w.Level, "Warning") {
				rows = append(rows, []interface{}{w.Level, int64(w.Code), w.Message})
			}
		}
		for _, w := range e.warnings {
			if strings.EqualFold(w.Level, "Note") {
				rows = append(rows, []interface{}{w.Level, int64(w.Code), w.Message})
			}
		}
		return &Result{
			Columns:     []string{"Level", "Code", "Message"},
			Rows:        rows,
			IsResultSet: true,
		}, nil
	}

	// SHOW ERRORS
	if strings.HasPrefix(upper, "SHOW ERRORS") {
		rows := make([][]interface{}, 0)
		for _, w := range e.warnings {
			if strings.EqualFold(w.Level, "Error") {
				rows = append(rows, []interface{}{w.Level, int64(w.Code), w.Message})
			}
		}
		return &Result{
			Columns:     []string{"Level", "Code", "Message"},
			Rows:        rows,
			IsResultSet: true,
		}, nil
	}

	// Handle @@warning_count / @@error_count
	if strings.Contains(upper, "WARNING_COUNT") {
		return &Result{
			Columns:     []string{"@@warning_count"},
			Rows:        [][]interface{}{{e.lastWarningCount}},
			IsResultSet: true,
		}, nil
	}
	if strings.Contains(upper, "ERROR_COUNT") {
		return &Result{
			Columns:     []string{"@@error_count"},
			Rows:        [][]interface{}{{e.lastErrorCount}},
			IsResultSet: true,
		}, nil
	}

	// SHOW ENGINE INNODB STATUS
	if strings.HasPrefix(upper, "SHOW ENGINE") {
		return &Result{
			Columns:     []string{"Type", "Name", "Status"},
			Rows:        [][]interface{}{{"InnoDB", "", ""}},
			IsResultSet: true,
		}, nil
	}

	// SHOW GRANTS [FOR user@host]
	if strings.HasPrefix(upper, "SHOW GRANTS") {
		grantUser := "root"
		grantHost := "localhost"
		if forIdx := strings.Index(upper, " FOR "); forIdx >= 0 {
			forPart := strings.TrimSpace(query[forIdx+5:])
			forPart = strings.TrimRight(forPart, ";")
			if atIdx := strings.LastIndex(forPart, "@"); atIdx >= 0 {
				grantUser = strings.Trim(strings.TrimSpace(forPart[:atIdx]), "'`\"")
				grantHost = strings.Trim(strings.TrimSpace(forPart[atIdx+1:]), "'`\"")
			} else {
				grantUser = strings.Trim(strings.TrimSpace(forPart), "'`\"")
			}
		}
		grantRows := [][]interface{}{
			{fmt.Sprintf("GRANT USAGE ON *.* TO `%s`@`%s`", grantUser, grantHost)},
		}
		if grantUser == "root" {
			grantRows = [][]interface{}{
				{"GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' WITH GRANT OPTION"},
			}
		} else if e.Catalog != nil {
			for _, dbName := range e.Catalog.ListDatabases() {
				if !strings.EqualFold(dbName, "information_schema") && !strings.EqualFold(dbName, "performance_schema") &&
					!strings.EqualFold(dbName, "mysql") && !strings.EqualFold(dbName, "sys") {
					grantRows = append(grantRows, []interface{}{
						fmt.Sprintf("GRANT ALL PRIVILEGES ON `%s`.* TO `%s`@`%s`", dbName, grantUser, grantHost),
					})
				}
			}
		}
		return &Result{
			Columns:     []string{fmt.Sprintf("Grants for %s@%s", grantUser, grantHost)},
			Rows:        grantRows,
			IsResultSet: true,
		}, nil
	}

	// SHOW [FULL] PROCESSLIST
	if strings.HasPrefix(upper, "SHOW PROCESSLIST") || strings.HasPrefix(upper, "SHOW FULL PROCESSLIST") {
		cols := []string{"Id", "User", "Host", "db", "Command", "Time", "State", "Info"}
		rows := make([][]interface{}, 0)
		if e.processList != nil {
			now := time.Now()
			for _, entry := range e.processList.Snapshot() {
				elapsed := int64(now.Sub(entry.StartTime).Seconds())
				db := entry.DB
				if db == "" {
					db = e.CurrentDB
				}
				var info interface{}
				if entry.Info != "" {
					info = entry.Info
				}
				rows = append(rows, []interface{}{
					entry.ID,
					entry.User,
					entry.Host,
					db,
					entry.Command,
					elapsed,
					entry.State,
					info,
				})
			}
		}
		// Include current connection if not already in the list
		connInList := false
		for _, row := range rows {
			if row[0] == e.connectionID {
				connInList = true
				break
			}
		}
		// Add event_scheduler daemon row (MySQL always shows it)
		hasEventScheduler := false
		for _, row := range rows {
			if u, ok := row[1].(string); ok && strings.EqualFold(u, "event_scheduler") {
				hasEventScheduler = true
				break
			}
		}
		if !hasEventScheduler {
			rows = append([][]interface{}{{
				int64(1),
				"event_scheduler",
				"localhost",
				nil,
				"Daemon",
				int64(0),
				"Waiting for next activation",
				nil,
			}}, rows...)
		}
		if !connInList {
			rows = append(rows, []interface{}{
				e.connectionID,
				"root",
				"localhost",
				e.CurrentDB,
				"Query",
				int64(0),
				"starting",
				"show processlist",
			})
		}
		return &Result{Columns: cols, Rows: rows, IsResultSet: true}, nil
	}

	// Accept other SHOW statements silently
	return &Result{
		Columns:     []string{"Value"},
		Rows:        [][]interface{}{},
		IsResultSet: true,
	}, nil
}

func parseShowIndexTarget(query, currentDB string) (dbName, tableName string, ok bool) {
	trimmed := strings.TrimSpace(strings.TrimRight(query, ";"))
	fields := strings.Fields(trimmed)
	if len(fields) < 4 {
		return "", "", false
	}
	// SHOW INDEX|INDEXES|KEYS FROM|IN <table> ...
	if !strings.EqualFold(fields[0], "show") {
		return "", "", false
	}
	if !(strings.EqualFold(fields[1], "index") || strings.EqualFold(fields[1], "indexes") || strings.EqualFold(fields[1], "keys")) {
		return "", "", false
	}
	if !(strings.EqualFold(fields[2], "from") || strings.EqualFold(fields[2], "in")) {
		return "", "", false
	}
	target := strings.Trim(fields[3], "`")
	target = strings.TrimRight(target, ",")
	dbName = currentDB
	tableName = target
	if dot := strings.Index(target, "."); dot >= 0 {
		dbName = strings.Trim(target[:dot], "`")
		tableName = strings.Trim(target[dot+1:], "`")
	}
	if dbName == "" || tableName == "" {
		return "", "", false
	}
	return dbName, tableName, true
}

func (e *Executor) showIndexes(dbName, tableName string) (*Result, error) {
	db, resolvedDBName, err := findDatabaseCaseInsensitive(e.Catalog, dbName)
	if err != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", dbName))
	}
	dbName = resolvedDBName
	if _, resolvedTableName, err := findTableDefCaseInsensitive(db, tableName); err == nil {
		tableName = resolvedTableName
	} else {
		return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", dbName, tableName))
	}

	rows := make([][]interface{}, 0)
	for _, r := range e.infoSchemaStatistics() {
		if !strings.EqualFold(toString(r["TABLE_SCHEMA"]), dbName) || !strings.EqualFold(toString(r["TABLE_NAME"]), tableName) {
			continue
		}
		rows = append(rows, []interface{}{
			r["TABLE_NAME"],    // Table
			r["NON_UNIQUE"],    // Non_unique
			r["INDEX_NAME"],    // Key_name
			r["SEQ_IN_INDEX"],  // Seq_in_index
			r["COLUMN_NAME"],   // Column_name
			r["COLLATION"],     // Collation
			r["CARDINALITY"],   // Cardinality
			r["SUB_PART"],      // Sub_part
			r["PACKED"],        // Packed
			r["NULLABLE"],      // Null
			r["INDEX_TYPE"],    // Index_type
			r["COMMENT"],       // Comment
			r["INDEX_COMMENT"], // Index_comment
			r["IS_VISIBLE"],    // Visible
			r["EXPRESSION"],    // Expression
		})
	}
	// MySQL's SHOW INDEX preserves index definition order, with PRIMARY always first.
	// Use a stable sort that only moves PRIMARY to the front and orders by
	// Seq_in_index within the same index name.
	sort.SliceStable(rows, func(i, j int) bool {
		ki := toString(rows[i][2])
		kj := toString(rows[j][2])
		iPrimary := strings.EqualFold(ki, "PRIMARY")
		jPrimary := strings.EqualFold(kj, "PRIMARY")
		if iPrimary != jPrimary {
			return iPrimary
		}
		// Within the same index, order by Seq_in_index
		if strings.EqualFold(ki, kj) {
			return toInt64(rows[i][3]) < toInt64(rows[j][3])
		}
		return false
	})
	return &Result{
		Columns: []string{
			"Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name",
			"Collation", "Cardinality", "Sub_part", "Packed", "Null",
			"Index_type", "Comment", "Index_comment", "Visible", "Expression",
		},
		Rows:        rows,
		IsResultSet: true,
	}, nil
}

// populatePerfSchemaTable dynamically populates certain performance_schema tables
// with live data before they are scanned by SELECT.
func (e *Executor) populatePerfSchemaTable(tbl *storage.Table, tableName string) {
	lower := strings.ToLower(tableName)
	switch lower {
	case "global_status", "session_status":
		// Populate with the same data as SHOW STATUS
		statusResult, err := e.showStatus("")
		if err != nil || statusResult == nil {
			return
		}
		// Build a mapping of PS lost counters to their size variables.
		// If a size is set to 0, the lost counter should be > 0 (since connections exist).
		psLostToSize := map[string]string{
			"Performance_schema_accounts_lost":              "performance_schema_accounts_size",
			"Performance_schema_cond_classes_lost":          "performance_schema_max_cond_classes",
			"Performance_schema_cond_instances_lost":        "performance_schema_max_cond_instances",
			"Performance_schema_file_classes_lost":          "performance_schema_max_file_classes",
			"Performance_schema_file_handles_lost":          "performance_schema_max_file_handles",
			"Performance_schema_file_instances_lost":        "performance_schema_max_file_instances",
			"Performance_schema_hosts_lost":                 "performance_schema_hosts_size",
			"Performance_schema_index_stat_lost":            "performance_schema_max_index_stat",
			"Performance_schema_memory_classes_lost":        "performance_schema_max_memory_classes",
			"Performance_schema_metadata_lock_lost":         "performance_schema_max_metadata_locks",
			"Performance_schema_mutex_classes_lost":         "performance_schema_max_mutex_classes",
			"Performance_schema_mutex_instances_lost":       "performance_schema_max_mutex_instances",
			"Performance_schema_prepared_statements_lost":   "performance_schema_max_prepared_statements_instances",
			"Performance_schema_program_lost":               "performance_schema_max_program_instances",
			"Performance_schema_rwlock_classes_lost":        "performance_schema_max_rwlock_classes",
			"Performance_schema_rwlock_instances_lost":      "performance_schema_max_rwlock_instances",
			"Performance_schema_session_connect_attrs_lost": "performance_schema_session_connect_attrs_size",
			"Performance_schema_socket_classes_lost":        "performance_schema_max_socket_classes",
			"Performance_schema_socket_instances_lost":      "performance_schema_max_socket_instances",
			"Performance_schema_stage_classes_lost":         "performance_schema_max_stage_classes",
			"Performance_schema_statement_classes_lost":     "performance_schema_max_statement_classes",
			"Performance_schema_table_handles_lost":         "performance_schema_max_table_handles",
			"Performance_schema_table_instances_lost":       "performance_schema_max_table_instances",
			"Performance_schema_table_lock_stat_lost":       "performance_schema_max_table_lock_stat",
			"Performance_schema_thread_classes_lost":        "performance_schema_max_thread_classes",
			"Performance_schema_thread_instances_lost":      "performance_schema_max_thread_instances",
			"Performance_schema_users_lost":                 "performance_schema_users_size",
		}
		tbl.Mu.Lock()
		tbl.Rows = nil
		for _, row := range statusResult.Rows {
			if len(row) >= 2 {
				r := make(storage.Row)
				name := fmt.Sprintf("%v", row[0])
				val := fmt.Sprintf("%v", row[1])
				// If this is a PS lost counter and the corresponding size var is 0,
				// report the lost counter as 1 (indicating data was lost).
				if sizeVar, ok := psLostToSize[name]; ok {
					if sizeVal, found := e.startupVars[sizeVar]; found && sizeVal == "0" {
						val = "1"
					}
				}
				// Store with both cases for column name lookups
				upperName := strings.ToUpper(name)
				r["VARIABLE_NAME"] = upperName
				r["variable_name"] = upperName
				r["VARIABLE_VALUE"] = val
				r["variable_value"] = val
				r["variable_value"] = val
				tbl.Rows = append(tbl.Rows, r)
			}
		}
		tbl.Mu.Unlock()
	case "global_variables":
		vars := e.buildVariablesMapScoped(true)
		tbl.Mu.Lock()
		tbl.Rows = nil
		// Sort by name for deterministic output
		names := make([]string, 0, len(vars))
		for name := range vars {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			r := make(storage.Row)
			r["VARIABLE_NAME"] = name
			r["VARIABLE_VALUE"] = vars[name]
			tbl.Rows = append(tbl.Rows, r)
		}
		tbl.Mu.Unlock()
	case "session_variables":
		vars := e.buildVariablesMapScoped(false)
		tbl.Mu.Lock()
		tbl.Rows = nil
		names := make([]string, 0, len(vars))
		for name := range vars {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			r := make(storage.Row)
			r["VARIABLE_NAME"] = name
			r["VARIABLE_VALUE"] = vars[name]
			tbl.Rows = append(tbl.Rows, r)
		}
		tbl.Mu.Unlock()
	}
}

// mysqlGeneratedClause formats a generated column clause for SHOW CREATE TABLE.
// It takes the raw expression string and storage type (virtual/stored) and returns
// something like: GENERATED ALWAYS AS ((`a` + LENGTH(`d`))) STORED
func mysqlGeneratedClause(exprStr string, colType string, colCharset string) string {
	upper := strings.ToUpper(colType)
	storage := "VIRTUAL"
	if strings.HasSuffix(upper, " STORED") {
		storage = "STORED"
	}

	// Parse the expression and reformat it MySQL-style
	formattedExpr := mysqlFormatGenExpr(exprStr, colCharset)

	return fmt.Sprintf("GENERATED ALWAYS AS (%s) %s", formattedExpr, storage)
}

// mysqlFormatGenExpr formats a generated column expression in MySQL style:
// backtick-quoted column refs, no spaces after commas in function args,
// _utf8mb4 prefix for string literals, extra outer parens for non-function expressions.
// colCharset is the column-level charset (e.g. "latin1"); empty means default (no prefix needed).
func mysqlFormatGenExpr(exprStr string, colCharset string) string {
	parser := sqlparser.NewTestParser()
	stmt, err := parser.Parse("SELECT " + exprStr)
	if err != nil {
		// Fallback: return as-is
		return exprStr
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok || len(sel.SelectExprs.Exprs) != 1 {
		return exprStr
	}
	aliased, ok := sel.SelectExprs.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return exprStr
	}
	inner := mysqlGenExprNode(aliased.Expr, colCharset)
	// MySQL wraps non-function/non-call expressions in extra parens
	switch aliased.Expr.(type) {
	case *sqlparser.FuncExpr, *sqlparser.SubstrExpr,
		*sqlparser.JSONUnquoteExpr, *sqlparser.JSONExtractExpr,
		*sqlparser.CastExpr:
		// Function-like expressions: no extra parens
		return inner
	case *sqlparser.Literal:
		// When column has explicit charset, the charset introducer makes the
		// literal a complete expression (e.g. _latin1'...') — no extra parens.
		if colCharset != "" {
			return inner
		}
		return "(" + inner + ")"
	default:
		// Binary expressions, column refs, etc: wrap in parens
		return "(" + inner + ")"
	}
}

// mysqlGenExprNode recursively formats an expression node in MySQL SHOW CREATE TABLE style.
// colCharset is the column-level charset; when non-empty, string literals get a charset introducer prefix.
func mysqlGenExprNode(expr sqlparser.Expr, colCharset string) string {
	switch e := expr.(type) {
	case *sqlparser.ColName:
		return "`" + e.Name.String() + "`"
	case *sqlparser.BinaryExpr:
		return fmt.Sprintf("%s %s %s", mysqlGenExprNode(e.Left, colCharset), e.Operator.ToString(), mysqlGenExprNode(e.Right, colCharset))
	case *sqlparser.FuncExpr:
		args := make([]string, len(e.Exprs))
		for i, arg := range e.Exprs {
			args[i] = mysqlGenExprNode(arg, colCharset)
		}
		name := e.Name.String()
		// MySQL outputs function names in lowercase in SHOW CREATE TABLE
		// for generated column expressions.
		name = strings.ToLower(name)
		return name + "(" + strings.Join(args, ",") + ")"
	case *sqlparser.Literal:
		if e.Type == sqlparser.StrVal {
			if colCharset != "" {
				return "_" + colCharset + "'" + e.Val + "'"
			}
			return "'" + e.Val + "'"
		}
		return e.Val
	case *sqlparser.UnaryExpr:
		return e.Operator.ToString() + mysqlGenExprNode(e.Expr, colCharset)
	case *sqlparser.SubstrExpr:
		parts := []string{mysqlGenExprNode(e.Name, colCharset)}
		if e.From != nil {
			parts = append(parts, mysqlGenExprNode(e.From, colCharset))
		}
		if e.To != nil {
			parts = append(parts, mysqlGenExprNode(e.To, colCharset))
		}
		return "substr(" + strings.Join(parts, ",") + ")"
	case *sqlparser.JSONUnquoteExpr:
		return "json_unquote(" + mysqlGenExprNode(e.JSONValue, colCharset) + ")"
	case *sqlparser.JSONExtractExpr:
		parts := []string{mysqlGenExprNode(e.JSONDoc, colCharset)}
		for _, p := range e.PathList {
			parts = append(parts, mysqlGenExprNode(p, colCharset))
		}
		return "json_extract(" + strings.Join(parts, ",") + ")"
	case *sqlparser.CastExpr:
		return "cast(" + mysqlGenExprNode(e.Expr, colCharset) + " as " + strings.ToLower(e.Type.Type) + ")"
	case *sqlparser.IntroducerExpr:
		return e.CharacterSet + mysqlGenExprNode(e.Expr, colCharset)
	case *sqlparser.IsExpr:
		op := strings.ToLower(e.Right.ToString())
		return mysqlGenExprNode(e.Left, colCharset) + " is " + op
	case *sqlparser.CaseExpr:
		var b strings.Builder
		b.WriteString("case")
		if e.Expr != nil {
			b.WriteString(" ")
			b.WriteString(mysqlGenExprNode(e.Expr, colCharset))
		}
		for _, w := range e.Whens {
			b.WriteString(" when ")
			b.WriteString(mysqlGenExprNode(w.Cond, colCharset))
			b.WriteString(" then ")
			b.WriteString(mysqlGenExprNode(w.Val, colCharset))
		}
		if e.Else != nil {
			b.WriteString(" else ")
			b.WriteString(mysqlGenExprNode(e.Else, colCharset))
		}
		b.WriteString(" end")
		return b.String()
	case *sqlparser.ComparisonExpr:
		return mysqlGenExprNode(e.Left, colCharset) + " " + e.Operator.ToString() + " " + mysqlGenExprNode(e.Right, colCharset)
	case *sqlparser.NullVal:
		return "NULL"
	case *sqlparser.NotExpr:
		return "not(" + mysqlGenExprNode(e.Expr, colCharset) + ")"
	default:
		// Fallback to sqlparser.String for unhandled types
		return sqlparser.String(expr)
	}
}

// mysqlDisplayType returns the MySQL display type with width for SHOW CREATE TABLE.
func mysqlDisplayType(colType string) string {
	// Strip generated column clause before processing the base type
	stripped := colType
	upperCheck := strings.ToUpper(colType)
	if idx := strings.Index(upperCheck, " GENERATED ALWAYS AS "); idx >= 0 {
		stripped = strings.TrimSpace(colType[:idx])
	}

	upper := strings.ToUpper(strings.TrimSpace(stripped))
	// Extract base type and any existing parameters
	base := upper
	suffix := ""
	if idx := strings.Index(upper, "("); idx >= 0 {
		// Already has width specified, just lowercase it
		// But also normalize REAL to DOUBLE, NUMERIC to DECIMAL, INTEGER to INT
		result := strings.ToLower(stripped)
		if strings.HasPrefix(result, "real") {
			result = "double" + result[4:]
		}
		if strings.HasPrefix(result, "numeric") {
			result = "decimal" + result[7:]
		}
		if strings.HasPrefix(result, "integer") {
			result = "int" + result[7:]
		}
		return result
	}
	// Check for ZEROFILL suffix (must check before UNSIGNED since ZEROFILL implies UNSIGNED)
	if strings.HasSuffix(base, " ZEROFILL") {
		base = strings.TrimSuffix(base, " ZEROFILL")
		suffix = " zerofill"
	}
	// Check for UNSIGNED suffix
	if strings.HasSuffix(base, " UNSIGNED") {
		base = strings.TrimSuffix(base, " UNSIGNED")
		suffix = " unsigned" + suffix
	} else if strings.Contains(suffix, "zerofill") {
		// ZEROFILL implies UNSIGNED in MySQL
		suffix = " unsigned" + suffix
	}

	// Add default display widths (differ for signed vs unsigned in MySQL)
	isUnsigned := suffix != ""
	switch base {
	case "TINYINT":
		if isUnsigned {
			return "tinyint(3)" + suffix
		}
		return "tinyint(4)" + suffix
	case "SMALLINT":
		if isUnsigned {
			return "smallint(5)" + suffix
		}
		return "smallint(6)" + suffix
	case "MEDIUMINT":
		if isUnsigned {
			return "mediumint(8)" + suffix
		}
		return "mediumint(9)" + suffix
	case "INT", "INTEGER":
		if isUnsigned {
			return "int(10)" + suffix
		}
		return "int(11)" + suffix
	case "BIGINT":
		if isUnsigned {
			return "bigint(20)" + suffix
		}
		return "bigint(20)" + suffix
	case "FLOAT":
		return "float" + suffix
	case "DOUBLE", "REAL":
		return "double" + suffix
	case "DECIMAL", "NUMERIC":
		return "decimal(10,0)" + suffix
	case "CHAR":
		return "char(1)"
	case "BINARY":
		return "binary(1)"
	case "BIT":
		return "bit(1)"
	case "YEAR":
		return "year(4)"
	case "BOOL", "BOOLEAN":
		return "tinyint(1)"
	case "GEOMETRYCOLLECTION", "GEOMCOLLECTION":
		// MySQL 8.0 uses "geomcollection" as the canonical display name
		return "geomcollection"
	case "GEOMETRY", "POINT", "LINESTRING", "POLYGON",
		"MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON":
		return strings.ToLower(base)
	default:
		return strings.ToLower(colType)
	}
}

func isClusterPreferredUniqueIndex(idx catalog.IndexDef, cols []catalog.ColumnDef) bool {
	if !idx.Unique || len(idx.Columns) == 0 {
		return false
	}
	colByName := make(map[string]catalog.ColumnDef, len(cols))
	for _, c := range cols {
		colByName[strings.ToLower(c.Name)] = c
	}
	for _, raw := range idx.Columns {
		c := strings.TrimSpace(raw)
		if strings.HasPrefix(c, "(") && strings.HasSuffix(c, ")") {
			return false
		}
		if lparen := strings.Index(c, "("); lparen >= 0 {
			// Prefix-length index parts are not clustered-primary candidates.
			return false
		}
		base := c
		if fields := strings.Fields(c); len(fields) > 0 {
			base = fields[0]
		}
		def, ok := colByName[strings.ToLower(base)]
		if !ok || def.Nullable {
			return false
		}
	}
	return true
}

func (e *Executor) showCreateProcedure(procName string) (*Result, error) {
	dbName := e.CurrentDB
	if strings.Contains(procName, ".") {
		parts := strings.SplitN(procName, ".", 2)
		dbName = parts[0]
		procName = parts[1]
	}
	db, err := e.Catalog.GetDatabase(dbName)
	if err != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", dbName))
	}
	procDef := db.GetProcedure(procName)
	if procDef == nil {
		return nil, mysqlError(1305, "42000", fmt.Sprintf("PROCEDURE %s.%s does not exist", dbName, procName))
	}
	createSQL := procDef.OriginalSQL
	if createSQL == "" {
		// Reconstruct from stored definition
		var paramParts []string
		for _, p := range procDef.Params {
			paramParts = append(paramParts, fmt.Sprintf("%s %s %s", p.Mode, p.Name, p.Type))
		}
		body := strings.Join(procDef.Body, ";\n")
		createSQL = fmt.Sprintf("CREATE DEFINER=`root`@`localhost` PROCEDURE `%s`(%s)\nBEGIN\n%s;\nEND", procDef.Name, strings.Join(paramParts, ", "), body)
	} else {
		createSQL = normalizeCreateRoutine(createSQL, "PROCEDURE", procDef.Name)
	}
	return &Result{
		Columns:     []string{"Procedure", "sql_mode", "Create Procedure", "character_set_client", "collation_connection", "Database Collation"},
		Rows:        [][]interface{}{{procDef.Name, e.sqlMode, createSQL, "utf8mb4", "utf8mb4_0900_ai_ci", "utf8mb4_0900_ai_ci"}},
		IsResultSet: true,
	}, nil
}

func (e *Executor) showCreateFunction(funcName string) (*Result, error) {
	dbName := e.CurrentDB
	if strings.Contains(funcName, ".") {
		parts := strings.SplitN(funcName, ".", 2)
		dbName = parts[0]
		funcName = parts[1]
	}
	db, err := e.Catalog.GetDatabase(dbName)
	if err != nil {
		return nil, mysqlError(1049, "42000", fmt.Sprintf("Unknown database '%s'", dbName))
	}
	funcDef := db.GetFunction(funcName)
	if funcDef == nil {
		return nil, mysqlError(1305, "42000", fmt.Sprintf("FUNCTION %s.%s does not exist", dbName, funcName))
	}
	createSQL := funcDef.OriginalSQL
	if createSQL == "" {
		// Reconstruct from stored definition
		var paramParts []string
		for _, p := range funcDef.Params {
			paramParts = append(paramParts, fmt.Sprintf("%s %s", p.Name, p.Type))
		}
		body := strings.Join(funcDef.Body, ";\n")
		createSQL = fmt.Sprintf("CREATE DEFINER=`root`@`localhost` FUNCTION `%s`(%s) RETURNS %s\nBEGIN\n%s;\nEND", funcDef.Name, strings.Join(paramParts, ", "), funcDef.ReturnType, body)
	} else {
		createSQL = normalizeCreateRoutine(createSQL, "FUNCTION", funcDef.Name)
	}
	return &Result{
		Columns:     []string{"Function", "sql_mode", "Create Function", "character_set_client", "collation_connection", "Database Collation"},
		Rows:        [][]interface{}{{funcDef.Name, e.sqlMode, createSQL, "utf8mb4", "utf8mb4_0900_ai_ci", "utf8mb4_0900_ai_ci"}},
		IsResultSet: true,
	}, nil
}

// normalizeReturnType converts SQL type names to MySQL canonical form.
func normalizeReturnType(t string) string {
	switch strings.ToUpper(strings.TrimSpace(t)) {
	case "INT", "INTEGER":
		return "int(11)"
	case "TINYINT":
		return "tinyint(4)"
	case "SMALLINT":
		return "smallint(6)"
	case "MEDIUMINT":
		return "mediumint(9)"
	case "BIGINT":
		return "bigint(20)"
	case "FLOAT":
		return "float"
	case "DOUBLE", "REAL":
		return "double"
	case "DECIMAL", "NUMERIC":
		return "decimal(10,0)"
	case "VARCHAR":
		return "varchar(255)"
	case "CHAR":
		return "char(1)"
	case "TEXT":
		return "text"
	case "BLOB":
		return "blob"
	case "LONGTEXT":
		return "longtext"
	case "LONGBLOB":
		return "longblob"
	case "TINYTEXT":
		return "tinytext"
	case "TINYBLOB":
		return "tinyblob"
	case "MEDIUMTEXT":
		return "mediumtext"
	case "MEDIUMBLOB":
		return "mediumblob"
	case "DATE":
		return "date"
	case "DATETIME":
		return "datetime"
	case "TIMESTAMP":
		return "timestamp"
	case "TIME":
		return "time"
	case "YEAR":
		return "year(4)"
	case "BOOL", "BOOLEAN":
		return "tinyint(1)"
	case "JSON":
		return "json"
	}
	return strings.ToLower(t)
}

// normalizeCreateRoutine transforms a user-provided CREATE FUNCTION/PROCEDURE SQL
// into MySQL's canonical SHOW CREATE output format.
func normalizeCreateRoutine(originalSQL string, routineType string, routineName string) string {
	// Remove leading/trailing whitespace
	sql := strings.TrimSpace(originalSQL)

	// Parse whether it already has DEFINER
	upper := strings.ToUpper(sql)
	hasDefiner := strings.Contains(upper, "DEFINER")

	// Find and replace the "CREATE [DEFINER=...] FUNCTION/PROCEDURE name" header
	routineTypeUpper := strings.ToUpper(routineType)
	idx := strings.Index(upper, routineTypeUpper+" ")
	if idx < 0 {
		return sql
	}

	// Build the canonical prefix
	prefix := fmt.Sprintf("CREATE DEFINER=`root`@`localhost` %s `%s`", routineType, routineName)

	// Find what comes after the routine name in the original
	// Skip past "FUNCTION name" or "PROCEDURE name" in the original
	rest := sql[idx+len(routineType)+1:]
	// Skip the routine name
	rest = strings.TrimSpace(rest)
	// Skip quoted or unquoted name
	if strings.HasPrefix(rest, "`") {
		end := strings.Index(rest[1:], "`")
		if end >= 0 {
			rest = rest[end+2:]
		}
	} else {
		// Unquoted name: skip until whitespace, (, or end
		i := 0
		for i < len(rest) && rest[i] != ' ' && rest[i] != '\t' && rest[i] != '\n' && rest[i] != '(' {
			i++
		}
		rest = rest[i:]
	}

	// If this is a FUNCTION, normalize the RETURNS type
	if routineTypeUpper == "FUNCTION" {
		// Find "RETURNS type" in the rest (before BEGIN)
		restUpper := strings.ToUpper(rest)
		returnsIdx := strings.Index(restUpper, "RETURNS ")
		if returnsIdx >= 0 {
			beforeReturns := rest[:returnsIdx]
			afterKeyword := rest[returnsIdx+8:] // after "RETURNS "
			// Find where type ends (before BEGIN or whitespace+BEGIN)
			afterKeywordUpper := strings.ToUpper(afterKeyword)
			beginIdx := strings.Index(afterKeywordUpper, "\nBEGIN")
			if beginIdx < 0 {
				beginIdx = strings.Index(afterKeywordUpper, " BEGIN")
			}
			var returnType string
			var afterType string
			if beginIdx >= 0 {
				returnType = strings.TrimSpace(afterKeyword[:beginIdx])
				afterType = afterKeyword[beginIdx:]
			} else {
				// Single-statement function (RETURN ...)
				// Type ends at newline or specific keyword
				newlineIdx := strings.Index(afterKeyword, "\n")
				if newlineIdx >= 0 {
					returnType = strings.TrimSpace(afterKeyword[:newlineIdx])
					afterType = afterKeyword[newlineIdx:]
				} else {
					returnType = strings.TrimSpace(afterKeyword)
					afterType = ""
				}
			}
			// Normalize type
			returnType = normalizeReturnType(returnType)
			rest = beforeReturns + "RETURNS " + returnType + afterType
		}
	}

	// Normalize body: strip leading whitespace from each line
	lines := strings.Split(rest, "\n")
	var normalizedLines []string
	for _, line := range lines {
		normalizedLines = append(normalizedLines, strings.TrimLeft(line, " \t"))
	}
	rest = strings.Join(normalizedLines, "\n")
	rest = strings.TrimSpace(rest)

	_ = hasDefiner
	return prefix + rest
}

// showRoutineStatus implements SHOW FUNCTION STATUS and SHOW PROCEDURE STATUS.
// routineType is "FUNCTION" or "PROCEDURE".
// rest is the remaining part of the query after "SHOW FUNCTION STATUS" or "SHOW PROCEDURE STATUS".
func (e *Executor) showRoutineStatus(routineType string, rest string) (*Result, error) {
	rest = strings.TrimSpace(rest)
	restUpper := strings.ToUpper(rest)

	// Parse optional LIKE or WHERE clause
	var likePattern string
	var whereField string
	var whereValue string

	if strings.HasPrefix(restUpper, "LIKE ") {
		// Extract the pattern
		likeRest := strings.TrimSpace(rest[5:])
		likeRest = strings.TrimRight(likeRest, ";")
		likePattern = strings.Trim(likeRest, "'\"")
	} else if strings.HasPrefix(restUpper, "WHERE ") {
		// Simple WHERE NAME='value' or WHERE NAME LIKE 'pattern' or WHERE Db = 'value'
		whereRest := strings.TrimSpace(rest[6:])
		whereRest = strings.TrimRight(whereRest, ";")
		whereRestUpper := strings.ToUpper(whereRest)
		if strings.HasPrefix(whereRestUpper, "NAME") {
			afterName := strings.TrimSpace(whereRest[4:])
			afterNameUpper := strings.ToUpper(afterName)
			if strings.HasPrefix(afterNameUpper, "LIKE ") {
				pat := strings.TrimSpace(afterName[5:])
				likePattern = strings.Trim(pat, "'\"")
			} else if strings.HasPrefix(afterName, "=") || strings.HasPrefix(afterName, " =") {
				eqIdx := strings.Index(afterName, "=")
				val := strings.TrimSpace(afterName[eqIdx+1:])
				whereField = "NAME"
				whereValue = strings.Trim(val, "'\"")
			}
		} else if strings.HasPrefix(whereRestUpper, "DB") {
			afterDb := strings.TrimSpace(whereRest[2:])
			if strings.HasPrefix(afterDb, "=") || strings.HasPrefix(afterDb, " =") {
				eqIdx := strings.Index(afterDb, "=")
				val := strings.TrimSpace(afterDb[eqIdx+1:])
				whereField = "DB"
				whereValue = strings.Trim(val, "'\"")
			}
		}
	}

	cols := []string{"Db", "Name", "Type", "Definer", "Modified", "Created", "Security_type", "Comment", "character_set_client", "collation_connection", "Database Collation"}
	var rows [][]interface{}

	now := "2000-01-01 00:00:00" // placeholder timestamp

	// Iterate over all databases
	for _, dbName := range e.Catalog.ListDatabases() {
		// Apply DB filter
		if whereField == "DB" && whereValue != "" {
			if !strings.EqualFold(dbName, whereValue) {
				continue
			}
		}

		db, err := e.Catalog.GetDatabase(dbName)
		if err != nil {
			continue
		}

		if routineType == "FUNCTION" {
			for _, funcDef := range db.ListFunctions() {
				name := funcDef.Name
				// Apply filter
				if likePattern != "" {
					if !matchLike(strings.ToLower(name), strings.ToLower(likePattern)) {
						continue
					}
				} else if whereField == "NAME" && whereValue != "" {
					if !strings.EqualFold(name, whereValue) {
						continue
					}
				}
				rows = append(rows, []interface{}{
					dbName, name, "FUNCTION", "root@localhost",
					now, now, "DEFINER", "",
					"utf8mb4", "utf8mb4_0900_ai_ci", "utf8mb4_0900_ai_ci",
				})
			}
		} else {
			for _, procDef := range db.ListProcedures() {
				name := procDef.Name
				// Apply filter
				if likePattern != "" {
					if !matchLike(strings.ToLower(name), strings.ToLower(likePattern)) {
						continue
					}
				} else if whereField == "NAME" && whereValue != "" {
					if !strings.EqualFold(name, whereValue) {
						continue
					}
				}
				rows = append(rows, []interface{}{
					dbName, name, "PROCEDURE", "root@localhost",
					now, now, "DEFINER", "",
					"utf8mb4", "utf8mb4_0900_ai_ci", "utf8mb4_0900_ai_ci",
				})
			}
		}
	}

	return &Result{
		Columns:     cols,
		Rows:        rows,
		IsResultSet: true,
	}, nil
}

func (e *Executor) showCreateTable(tableName string) (*Result, error) {
	showDB := e.CurrentDB
	if strings.Contains(tableName, ".") {
		showDB, tableName = resolveTableNameDB(tableName, e.CurrentDB)
	}

	// Handle SHOW CREATE TABLE for information_schema virtual tables.
	if strings.ToLower(showDB) == "information_schema" {
		lowerName := strings.ToLower(tableName)
		upperName := strings.ToUpper(tableName)
		if viewDef, ok := infoSchemaCreateView[lowerName]; ok {
			return &Result{
				Columns:     []string{"View", "Create View", "character_set_client", "collation_connection"},
				Rows:        [][]interface{}{{upperName, viewDef, "utf8", "utf8_general_ci"}},
				IsResultSet: true,
			}, nil
		}
		if tblDef, ok := infoSchemaCreateTable[lowerName]; ok {
			return &Result{
				Columns:     []string{"Table", "Create Table"},
				Rows:        [][]interface{}{{upperName, tblDef}},
				IsResultSet: true,
			}, nil
		}
	}

	// Handle SHOW CREATE TABLE for performance_schema tables.
	if strings.ToLower(showDB) == "performance_schema" {
		lowerName := strings.ToLower(tableName)
		if stmt, ok := perfSchemaCreateTable[lowerName]; ok {
			return &Result{
				Columns:     []string{"Table", "Create Table"},
				Rows:        [][]interface{}{{tableName, stmt}},
				IsResultSet: true,
			}, nil
		}
		return nil, fmt.Errorf("ERROR 1146 (42S02): Table 'performance_schema.%s' doesn't exist", tableName)
	}

	db, resolvedDBName, err := findDatabaseCaseInsensitive(e.Catalog, showDB)
	if err != nil {
		return nil, err
	}
	showDB = resolvedDBName
	def, resolvedTableName, err := findTableDefCaseInsensitive(db, tableName)
	if err != nil {
		// Check if it's a view — return SHOW CREATE VIEW style output.
		if e.views != nil {
			viewLookup := tableName
			if _, ok := e.views[viewLookup]; !ok {
				for vn := range e.views {
					if strings.EqualFold(vn, viewLookup) {
						viewLookup = vn
						break
					}
				}
			}
			if viewSQL, ok := e.views[viewLookup]; ok {
				createView := fmt.Sprintf("CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `%s` AS %s", viewLookup, viewSQL)
				return &Result{
					Columns:     []string{"View", "Create View", "character_set_client", "collation_connection"},
					Rows:        [][]interface{}{{viewLookup, createView, "utf8mb4", "utf8mb4_0900_ai_ci"}},
					IsResultSet: true,
				}, nil
			}
		}
		return nil, fmt.Errorf("ERROR 1146 (42S02): Table '%s.%s' doesn't exist", showDB, tableName)
	}
	tableName = resolvedTableName

	// Check sql_quote_show_create setting
	quoteIdent := true
	if v, ok := e.getSysVar("sql_quote_show_create"); ok && (v == "OFF" || v == "0") {
		quoteIdent = false
	}
	quoteFunc := func(name string) string {
		if quoteIdent {
			return "`" + name + "`"
		}
		return name
	}

	// Get AUTO_INCREMENT value
	autoIncVal := int64(0)
	if tbl, err := e.Storage.GetTable(showDB, tableName); err == nil {
		autoIncVal = tbl.AutoIncrementValue()
	}

	var b strings.Builder
	if e.tempTables[tableName] {
		b.WriteString(fmt.Sprintf("CREATE TEMPORARY TABLE %s (\n", quoteFunc(tableName)))
	} else {
		b.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", quoteFunc(tableName)))
	}

	var colDefs []string
	var pkCols []string
	for _, col := range def.Columns {
		var parts []string
		parts = append(parts, fmt.Sprintf("  %s", quoteFunc(col.Name)))
		parts = append(parts, mysqlDisplayType(col.Type))
		// Column-level CHARACTER SET / COLLATE (when different from table default)
		if col.Charset != "" {
			tableCharset := def.Charset
			if tableCharset == "" {
				tableCharset = "utf8mb4"
			}
			charsetDiffers := !strings.EqualFold(col.Charset, tableCharset)
			// Show COLLATE when the column collation differs from the default collation for the column's charset.
			// This handles cases like VARCHAR BINARY which gets latin1_bin vs latin1's default latin1_swedish_ci.
			defaultCollForColCharset := catalog.DefaultCollationForCharset(col.Charset)
			collationDiffers := col.Collation != "" && !strings.EqualFold(col.Collation, defaultCollForColCharset)
			if charsetDiffers {
				collation := col.Collation
				if collation == "" {
					// No explicit collation: show only CHARACTER SET (MySQL behavior for
					// columns whose collation was not explicitly set, e.g. from SELECT result)
					parts = append(parts, fmt.Sprintf("CHARACTER SET %s", col.Charset))
				} else {
					parts = append(parts, fmt.Sprintf("CHARACTER SET %s COLLATE %s", col.Charset, collation))
				}
			} else if collationDiffers {
				// Same charset as table but different collation: show only COLLATE
				parts = append(parts, fmt.Sprintf("COLLATE %s", col.Collation))
			}
		}
		colTypeLower := strings.ToLower(col.Type)
		isTimestamp := strings.HasPrefix(colTypeLower, "timestamp")
		genExpr := generatedColumnExpr(col.Type)
		isGenerated := genExpr != ""
		if isGenerated {
			// Append GENERATED ALWAYS AS (...) VIRTUAL/STORED
			parts = append(parts, mysqlGeneratedClause(genExpr, col.Type, col.Charset))
		}
		if !col.Nullable {
			parts = append(parts, "NOT NULL")
		} else if isTimestamp {
			// MySQL explicitly shows NULL for nullable timestamp columns
			parts = append(parts, "NULL")
		}
		if col.AutoIncrement {
			parts = append(parts, "AUTO_INCREMENT")
		} else if !isGenerated && col.Default != nil {
			defVal := *col.Default
			// MySQL SHOW CREATE TABLE quotes default values
			if defVal == "NULL" || defVal == "null" {
				parts = append(parts, "DEFAULT NULL")
			} else if strings.HasPrefix(defVal, "'") {
				// Already quoted - pad BINARY default values.
				if padLen := binaryPadLength(col.Type); padLen > 0 {
					inner := defVal[1 : len(defVal)-1] // strip quotes
					if len(inner) < padLen {
						inner = inner + strings.Repeat("\\0", padLen-len(inner))
					}
					defVal = "'" + inner + "'"
				}
				parts = append(parts, fmt.Sprintf("DEFAULT %s", defVal))
			} else if strings.HasPrefix(strings.ToUpper(defVal), "CURRENT_TIMESTAMP") {
				// CURRENT_TIMESTAMP and CURRENT_TIMESTAMP(N) are shown unquoted
				parts = append(parts, fmt.Sprintf("DEFAULT %s", defVal))
			} else {
				// Pad BINARY default values.
				if padLen := binaryPadLength(col.Type); padLen > 0 && len(defVal) < padLen {
					defVal = defVal + strings.Repeat("\\0", padLen-len(defVal))
				}
				// Pad DECIMAL default values to the declared scale.
				defVal = padDecimalDefault(col.Type, defVal)
				parts = append(parts, fmt.Sprintf("DEFAULT '%s'", defVal))
			}
		} else if !isGenerated && col.Nullable && !col.DefaultDropped {
			// MySQL doesn't show DEFAULT NULL for BLOB/TEXT types
			isBlobOrText := strings.Contains(colTypeLower, "blob") || strings.Contains(colTypeLower, "text")
			if !isBlobOrText {
				parts = append(parts, "DEFAULT NULL")
			}
		}
		if col.OnUpdateCurrentTimestamp {
			// Show ON UPDATE CURRENT_TIMESTAMP with precision matching the column type
			fsp := ""
			if idx := strings.Index(colTypeLower, "("); idx >= 0 {
				end := strings.Index(colTypeLower[idx:], ")")
				if end >= 0 {
					fsp = colTypeLower[idx : idx+end+1]
				}
			}
			parts = append(parts, fmt.Sprintf("ON UPDATE CURRENT_TIMESTAMP%s", fsp))
		}
		if col.Comment != "" {
			parts = append(parts, fmt.Sprintf("COMMENT '%s'", col.Comment))
		}
		colDefs = append(colDefs, strings.Join(parts, " "))
	}
	pkCols = append(pkCols, def.PrimaryKey...)
	if len(pkCols) == 0 {
		for _, col := range def.Columns {
			if col.PrimaryKey {
				pkCols = append(pkCols, col.Name)
			}
		}
	}
	if len(pkCols) == 0 {
		pkCols = def.PrimaryKey
	}

	hasTrailingDefs := len(pkCols) > 0 || len(def.Indexes) > 0 || len(def.CheckConstraints) > 0 || len(def.ForeignKeys) > 0

	for i, cd := range colDefs {
		b.WriteString(cd)
		if i < len(colDefs)-1 || hasTrailingDefs {
			b.WriteString(",")
		}
		b.WriteString("\n")
	}
	if len(pkCols) > 0 {
		quotedPK := make([]string, len(pkCols))
		for i, pk := range pkCols {
			trimmed := strings.TrimSpace(pk)
			if strings.HasPrefix(trimmed, "(") && strings.HasSuffix(trimmed, ")") {
				quotedPK[i] = trimmed
			} else if lparen := strings.Index(trimmed, "("); lparen >= 0 {
				quotedPK[i] = fmt.Sprintf("%s%s", quoteFunc(trimmed[:lparen]), trimmed[lparen:])
			} else {
				quotedPK[i] = quoteFunc(trimmed)
			}
		}
		hasMore := len(def.Indexes) > 0 || len(def.CheckConstraints) > 0 || len(def.ForeignKeys) > 0
		b.WriteString(fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(quotedPK, ",")))
		if hasMore {
			b.WriteString(",")
		}
		b.WriteString("\n")
	}
	displayIndexes := make([]catalog.IndexDef, len(def.Indexes))
	copy(displayIndexes, def.Indexes)
	// hasColumnPrefix returns true if any column in the index uses a prefix (e.g. col(31)).
	hasColumnPrefix := func(idx catalog.IndexDef) bool {
		for _, c := range idx.Columns {
			if strings.Contains(c, "(") {
				return true
			}
		}
		return false
	}
	sort.SliceStable(displayIndexes, func(i, j int) bool {
		// MySQL orders UNIQUE KEY before non-unique KEY in SHOW CREATE TABLE
		iUnique := displayIndexes[i].Unique
		jUnique := displayIndexes[j].Unique
		if iUnique != jUnique {
			return iUnique && !jUnique
		}
		// Among UNIQUE keys, MySQL/InnoDB puts full-column (no prefix) indexes
		// before prefix-based indexes (InnoDB clustered index promotion logic).
		if iUnique && jUnique {
			iPrefix := hasColumnPrefix(displayIndexes[i])
			jPrefix := hasColumnPrefix(displayIndexes[j])
			if iPrefix != jPrefix {
				return !iPrefix && jPrefix
			}
		}
		return false
	})
	for i, idx := range displayIndexes {
		quotedCols := make([]string, len(idx.Columns))
		for j, c := range idx.Columns {
			direction := ""
			if j < len(idx.Orders) && strings.EqualFold(idx.Orders[j], "DESC") {
				direction = " DESC"
			} else if j < len(idx.Orders) && strings.EqualFold(idx.Orders[j], "ASC") {
				direction = " ASC"
			}
			trimmed := strings.TrimSpace(c)
			if strings.HasPrefix(trimmed, "(") && strings.HasSuffix(trimmed, ")") {
				quotedCols[j] = trimmed + direction
			} else if lparen := strings.Index(trimmed, "("); lparen >= 0 {
				// Handle column with length prefix like "c1(10)"
				quotedCols[j] = fmt.Sprintf("%s%s%s", quoteFunc(trimmed[:lparen]), trimmed[lparen:], direction)
			} else {
				quotedCols[j] = fmt.Sprintf("%s%s", quoteFunc(trimmed), direction)
			}
		}
		usingStr := ""
		if strings.EqualFold(idx.Using, "BTREE") {
			usingStr = " USING BTREE"
		}
		commentStr := ""
		if idx.Comment != "" {
			commentStr = fmt.Sprintf(" COMMENT '%s'", idx.Comment)
		}
		prefix := "KEY"
		if idx.Type == "FULLTEXT" {
			prefix = "FULLTEXT KEY"
		} else if idx.Type == "SPATIAL" {
			prefix = "SPATIAL KEY"
		} else if idx.Unique {
			prefix = "UNIQUE KEY"
		}
		invisibleStr := ""
		if idx.Invisible {
			invisibleStr = " /*!80000 INVISIBLE */"
		}
		if prefix == "UNIQUE KEY" {
			b.WriteString(fmt.Sprintf("  UNIQUE KEY %s (%s)%s%s%s", quoteFunc(idx.Name), strings.Join(quotedCols, ","), usingStr, commentStr, invisibleStr))
		} else {
			b.WriteString(fmt.Sprintf("  %s %s (%s)%s%s%s", prefix, quoteFunc(idx.Name), strings.Join(quotedCols, ","), usingStr, commentStr, invisibleStr))
		}
		if i < len(displayIndexes)-1 || len(def.CheckConstraints) > 0 || len(def.ForeignKeys) > 0 {
			b.WriteString(",")
		}
		b.WriteString("\n")
	}

	// CHECK constraints
	for i, cc := range def.CheckConstraints {
		// MySQL wraps CHECK expressions in extra parentheses and backtick-quotes
		// column names. Re-parse the stored expression to produce canonical output.
		checkExpr := cc.Expr
		if parsed, err := sqlparser.NewTestParser().ParseExpr(cc.Expr); err == nil {
			checkExpr = sqlparser.CanonicalString(parsed)
		}
		b.WriteString(fmt.Sprintf("  CONSTRAINT `%s` CHECK ((%s))", cc.Name, checkExpr))
		if i < len(def.CheckConstraints)-1 || len(def.ForeignKeys) > 0 {
			b.WriteString(",")
		}
		b.WriteString("\n")
	}

	// FOREIGN KEY constraints
	for i, fk := range def.ForeignKeys {
		quotedChildCols := make([]string, len(fk.Columns))
		for j, c := range fk.Columns {
			quotedChildCols[j] = quoteFunc(c)
		}
		quotedParentCols := make([]string, len(fk.ReferencedColumns))
		for j, c := range fk.ReferencedColumns {
			quotedParentCols[j] = quoteFunc(c)
		}
		fkStr := fmt.Sprintf("  CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
			quoteFunc(fk.Name),
			strings.Join(quotedChildCols, ","),
			quoteFunc(fk.ReferencedTable),
			strings.Join(quotedParentCols, ","))
		if strings.ToUpper(fk.OnDelete) == "CASCADE" {
			fkStr += " ON DELETE CASCADE"
		} else if strings.ToUpper(fk.OnDelete) == "SET NULL" {
			fkStr += " ON DELETE SET NULL"
		} else if strings.ToUpper(fk.OnDelete) == "NO ACTION" {
			fkStr += " ON DELETE NO ACTION"
		} else if strings.ToUpper(fk.OnDelete) == "SET DEFAULT" {
			fkStr += " ON DELETE SET DEFAULT"
		}
		if strings.ToUpper(fk.OnUpdate) == "CASCADE" {
			fkStr += " ON UPDATE CASCADE"
		} else if strings.ToUpper(fk.OnUpdate) == "SET NULL" {
			fkStr += " ON UPDATE SET NULL"
		} else if strings.ToUpper(fk.OnUpdate) == "NO ACTION" {
			fkStr += " ON UPDATE NO ACTION"
		} else if strings.ToUpper(fk.OnUpdate) == "SET DEFAULT" {
			fkStr += " ON UPDATE SET DEFAULT"
		}
		b.WriteString(fkStr)
		if i < len(def.ForeignKeys)-1 {
			b.WriteString(",")
		}
		b.WriteString("\n")
	}

	engineName := "InnoDB"
	if def.Engine != "" {
		// Normalize engine name casing to match MySQL conventions
		switch strings.ToUpper(def.Engine) {
		case "INNODB":
			engineName = "InnoDB"
		case "MYISAM":
			engineName = "MyISAM"
		case "MEMORY", "HEAP":
			engineName = "MEMORY"
		case "CSV":
			engineName = "CSV"
		case "ARCHIVE":
			engineName = "ARCHIVE"
		case "BLACKHOLE":
			engineName = "BLACKHOLE"
		case "MERGE", "MRGSORT", "MRG_MYISAM":
			engineName = "MRG_MyISAM"
		case "FEDERATED":
			engineName = "FEDERATED"
		default:
			engineName = def.Engine
		}
	}
	trailer := fmt.Sprintf(") ENGINE=%s", engineName)
	if autoIncVal > 0 {
		trailer += fmt.Sprintf(" AUTO_INCREMENT=%d", autoIncVal+1)
	}
	charset := "utf8mb4"
	collation := "utf8mb4_0900_ai_ci"
	if def.Charset != "" {
		charset = def.Charset
		collation = catalog.DefaultCollationForCharset(charset)
	}
	if def.Collation != "" {
		collation = def.Collation
	}
	trailer += fmt.Sprintf(" DEFAULT CHARSET=%s", charset)
	// Show COLLATE when charset is utf8mb4 (MySQL default behavior) or when
	// an explicit non-default collation is specified.
	defaultCollation := catalog.DefaultCollationForCharset(charset)
	if charset == "utf8mb4" || collation != defaultCollation {
		trailer += fmt.Sprintf(" COLLATE=%s", collation)
	}
	if def.Comment != "" {
		trailer += fmt.Sprintf(" COMMENT='%s'", def.Comment)
	}
	if def.RowFormat != "" {
		trailer += fmt.Sprintf(" ROW_FORMAT=%s", strings.ToUpper(def.RowFormat))
	}
	if def.KeyBlockSize != nil {
		trailer += fmt.Sprintf(" KEY_BLOCK_SIZE=%d", *def.KeyBlockSize)
	}
	if def.StatsPersistent != nil {
		trailer += fmt.Sprintf(" STATS_PERSISTENT=%d", *def.StatsPersistent)
	}
	if def.StatsAutoRecalc != nil {
		trailer += fmt.Sprintf(" STATS_AUTO_RECALC=%d", *def.StatsAutoRecalc)
	}
	if def.StatsSamplePages != nil {
		trailer += fmt.Sprintf(" STATS_SAMPLE_PAGES=%d", *def.StatsSamplePages)
	}
	if def.MaxRows != nil {
		trailer += fmt.Sprintf(" MAX_ROWS=%d", *def.MaxRows)
	}
	if def.InsertMethod != "" {
		trailer += fmt.Sprintf(" INSERT_METHOD=%s", def.InsertMethod)
	}
	if len(def.UnionTables) > 0 {
		quotedTables := make([]string, len(def.UnionTables))
		for i, t := range def.UnionTables {
			quotedTables[i] = "`" + t + "`"
		}
		trailer += fmt.Sprintf(" UNION=(%s)", strings.Join(quotedTables, ","))
	}
	b.WriteString(trailer)

	return &Result{
		Columns:     []string{"Table", "Create Table"},
		Rows:        [][]interface{}{{tableName, b.String()}},
		IsResultSet: true,
	}, nil
}
