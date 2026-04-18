package catalog

import (
	"fmt"
	"strings"
	"sync"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

// normalizeName converts a stored procedure/function name to a canonical form
// for case-insensitive and accent-insensitive comparison (matching MySQL behavior).
// MySQL uses Unicode case folding + accent stripping for routine names.
func normalizeName(name string) string {
	// Apply NFKD normalization to decompose accented characters into base+combining mark
	nfkd := norm.NFKD.String(strings.ToLower(name))
	// Remove Unicode combining marks (category Mn = Non-spacing Mark)
	var result []rune
	for _, r := range nfkd {
		if unicode.Is(unicode.Mn, r) {
			continue // skip combining/diacritic marks
		}
		result = append(result, r)
	}
	return string(result)
}

// ColumnDef represents a column definition in a table.
type ColumnDef struct {
	Name                     string
	Type                     string // MySQL type string (e.g. "INT", "VARCHAR(255)")
	Nullable                 bool
	Default                  *string
	AutoIncrement            bool
	PrimaryKey               bool
	Unique                   bool
	Comment                  string
	DefaultDropped           bool // true when ALTER TABLE ... DROP DEFAULT was used
	OnUpdateCurrentTimestamp bool   // true for TIMESTAMP/DATETIME with ON UPDATE CURRENT_TIMESTAMP
	Charset                 string // column-level charset override (e.g. "latin1"); empty means inherit table default
	Collation               string // column-level collation override; empty means inherit table default
}

// IndexDef represents an index definition.
type IndexDef struct {
	Name      string
	Columns   []string
	Orders    []string // per-column order: "", "ASC", "DESC"
	Unique    bool
	Type      string // "", "FULLTEXT", "SPATIAL"
	Using     string // Index method: BTREE, HASH, etc.
	Comment   string // COMMENT clause on index
	Invisible bool   // true when ALTER TABLE ... ADD INDEX ... INVISIBLE
}

// TableDef represents a table definition.
type TableDef struct {
	Name             string
	Columns          []ColumnDef
	PrimaryKey       []string // column names
	Indexes          []IndexDef
	Comment          string
	Charset          string // e.g. "latin1", "utf8mb4"; empty means default (utf8mb4)
	Collation        string // e.g. "latin1_swedish_ci"; empty means default
	Engine           string // e.g. "InnoDB", "MyISAM", "MEMORY"; empty means default (InnoDB)
	RowFormat        string // e.g. "DYNAMIC", "COMPACT", "REDUNDANT", "COMPRESSED"
	KeyBlockSize     *int
	PrimaryKeyOrders []string // per-PK-column order: "", "ASC", "DESC"
	StatsPersistent  *int
	StatsAutoRecalc  *int
	StatsSamplePages *int
	InsertMethod     string   // INSERT_METHOD for MERGE tables: "NO", "FIRST", "LAST"
	UnionTables      []string // UNION=(t1,t2,...) for MERGE tables
	MaxRows          *uint64  // MAX_ROWS table option (clamped to uint32 max for non-64-bit engines)
	CheckConstraints  []CheckConstraint // CHECK constraint definitions
	ForeignKeys       []ForeignKeyDef    // FOREIGN KEY constraint definitions
	PartitionType     string             // "RANGE", "LIST", "HASH", "KEY" or "" for non-partitioned
	PartitionColumns  []string           // column names used in partition expression (for ordering)
}

// ColType returns the type string for a column by name (case-insensitive).
// Returns an empty string if the column is not found.
func (td *TableDef) ColType(name string) string {
	nameLower := strings.ToLower(name)
	for _, col := range td.Columns {
		if strings.ToLower(col.Name) == nameLower {
			return col.Type
		}
	}
	return ""
}

// CheckConstraint represents a table-level CHECK constraint.
type CheckConstraint struct {
	Name string // constraint name (auto-generated if not specified)
	Expr string // SQL expression to evaluate
}

// ForeignKeyDef represents a foreign key constraint.
type ForeignKeyDef struct {
	Name              string   // constraint name
	Columns           []string // columns in the child (referencing) table
	ReferencedTable   string   // parent (referenced) table name
	ReferencedColumns []string // columns in the parent table
	OnDelete          string   // "RESTRICT", "CASCADE", "SET NULL", "NO ACTION", "" (default = RESTRICT)
	OnUpdate          string   // "RESTRICT", "CASCADE", "SET NULL", "NO ACTION", "" (default = RESTRICT)
}

// TriggerDef represents a trigger definition.
type TriggerDef struct {
	Name   string
	Timing string // "BEFORE" or "AFTER"
	Event  string // "INSERT", "UPDATE", or "DELETE"
	Table  string
	Body   []string // SQL statements in the trigger body
}

// ProcedureDef represents a stored procedure definition.
type ProcedureDef struct {
	Name        string
	Params      []ProcParam
	Body        []string // SQL statements in the procedure body
	BodyText    string   // original body text (begin...end or full body) for information_schema
	OriginalSQL string   // original CREATE PROCEDURE statement
	SqlMode     string   // sql_mode at procedure creation time
}

// ProcParam represents a parameter in a stored procedure.
type ProcParam struct {
	Mode string // "IN", "OUT", "INOUT"
	Name string
	Type string
}

// FunctionDef represents a stored function definition.
type FunctionDef struct {
	Name          string
	Params        []ProcParam
	ReturnType    string
	Body          []string // SQL statements in the function body
	BodyText      string   // original body text for information_schema
	Deterministic bool     // true if declared DETERMINISTIC
	OriginalSQL   string   // original CREATE FUNCTION statement
	SqlMode       string   // sql_mode at function creation time
}

// Database represents a database containing tables.
type Database struct {
	Name          string
	Tables        map[string]*TableDef
	Triggers      map[string]*TriggerDef   // trigger name -> trigger def
	Procedures    map[string]*ProcedureDef // procedure name -> procedure def
	Functions     map[string]*FunctionDef  // function name -> function def
	CharacterSet  string                   // e.g. "utf8mb4", "ascii", "binary"
	CollationName string                   // e.g. "utf8mb4_general_ci", "ascii_general_ci"
	mu            sync.RWMutex
}

// Catalog is the top-level catalog managing databases.
type Catalog struct {
	Databases map[string]*Database
	mu        sync.RWMutex
}

func New() *Catalog {
	c := &Catalog{
		Databases: make(map[string]*Database),
	}
	// Create default databases (matching MySQL)
	for _, name := range []string{"information_schema", "mtr", "mysql", "performance_schema", "sys", "test"} {
		charset := "utf8mb4"
		collation := "utf8mb4_0900_ai_ci"
		if name == "information_schema" {
			charset = "utf8"
			collation = "utf8_general_ci"
		}
		c.Databases[name] = &Database{
			Name:          name,
			Tables:        make(map[string]*TableDef),
			Triggers:      make(map[string]*TriggerDef),
			Procedures:    make(map[string]*ProcedureDef),
			Functions:     make(map[string]*FunctionDef),
			CharacterSet:  charset,
			CollationName: collation,
		}
	}
	return c
}

// DefaultCollationForCharset returns the default collation for a charset.
func DefaultCollationForCharset(charset string) string {
	switch strings.ToLower(charset) {
	case "utf8mb4":
		return "utf8mb4_0900_ai_ci"
	case "utf8", "utf8mb3":
		return "utf8_general_ci"
	case "latin1":
		return "latin1_swedish_ci"
	case "ascii":
		return "ascii_general_ci"
	case "binary":
		return "binary"
	case "cp1251":
		return "cp1251_general_ci"
	case "swe7":
		return "swe7_swedish_ci"
	case "armscii8":
		return "armscii8_general_ci"
	case "big5":
		return "big5_chinese_ci"
	case "cp1250":
		return "cp1250_general_ci"
	case "cp1256":
		return "cp1256_general_ci"
	case "cp1257":
		return "cp1257_general_ci"
	case "cp850":
		return "cp850_general_ci"
	case "cp852":
		return "cp852_general_ci"
	case "cp866":
		return "cp866_general_ci"
	case "cp932":
		return "cp932_japanese_ci"
	case "dec8":
		return "dec8_swedish_ci"
	case "eucjpms":
		return "eucjpms_japanese_ci"
	case "euckr":
		return "euckr_korean_ci"
	case "gb18030":
		return "gb18030_chinese_ci"
	case "gb2312":
		return "gb2312_chinese_ci"
	case "gbk":
		return "gbk_chinese_ci"
	case "geostd8":
		return "geostd8_general_ci"
	case "greek":
		return "greek_general_ci"
	case "hebrew":
		return "hebrew_general_ci"
	case "hp8":
		return "hp8_english_ci"
	case "keybcs2":
		return "keybcs2_general_ci"
	case "koi8r":
		return "koi8r_general_ci"
	case "koi8u":
		return "koi8u_general_ci"
	case "latin2":
		return "latin2_general_ci"
	case "latin5":
		return "latin5_turkish_ci"
	case "latin7":
		return "latin7_general_ci"
	case "macce":
		return "macce_general_ci"
	case "macroman":
		return "macroman_general_ci"
	case "sjis":
		return "sjis_japanese_ci"
	case "tis620":
		return "tis620_thai_ci"
	case "ucs2":
		return "ucs2_general_ci"
	case "ujis":
		return "ujis_japanese_ci"
	case "utf16":
		return "utf16_general_ci"
	case "utf16le":
		return "utf16le_general_ci"
	case "utf32":
		return "utf32_general_ci"
	default:
		return charset + "_general_ci"
	}
}

// BinaryCollationForCharset returns the binary collation for a charset.
// For example, "latin1" -> "latin1_bin", "utf8mb4" -> "utf8mb4_bin".
func BinaryCollationForCharset(charset string) string {
	switch strings.ToLower(charset) {
	case "binary":
		return "binary"
	case "utf8mb4":
		return "utf8mb4_bin"
	case "utf8", "utf8mb3":
		return "utf8_bin"
	default:
		return strings.ToLower(charset) + "_bin"
	}
}

// CharsetForCollation returns the charset that a collation belongs to.
// It returns the charset name and true if found, or "" and false if the collation is unknown.
func CharsetForCollation(collation string) (string, bool) {
	collation = strings.ToLower(collation)
	// Special case: "binary" collation belongs to "binary" charset
	if collation == "binary" {
		return "binary", true
	}
	// Special case: utf8mb3_ collations belong to "utf8" charset
	if strings.HasPrefix(collation, "utf8mb3_") {
		return "utf8", true
	}
	// The charset is the prefix before the first underscore
	idx := strings.Index(collation, "_")
	if idx < 0 {
		return "", false
	}
	charset := collation[:idx]
	// Verify the charset is known by checking if DefaultCollationForCharset returns something meaningful
	defColl := DefaultCollationForCharset(charset)
	if defColl == charset+"_general_ci" {
		// This is the fallback; check if it's a real charset
		// by checking a known list
		knownCharsets := map[string]bool{
			"utf8mb4": true, "utf8": true, "latin1": true, "ascii": true, "binary": true,
			"cp1251": true, "swe7": true, "armscii8": true, "big5": true, "cp1250": true,
			"cp1256": true, "cp1257": true, "cp850": true, "cp852": true, "cp866": true,
			"cp932": true, "dec8": true, "eucjpms": true, "euckr": true, "gb18030": true,
			"gb2312": true, "gbk": true, "geostd8": true, "greek": true, "hebrew": true,
			"hp8": true, "keybcs2": true, "koi8r": true, "koi8u": true, "latin2": true,
			"latin5": true, "latin7": true, "macce": true, "macroman": true, "sjis": true,
			"tis620": true, "ucs2": true, "ujis": true, "utf16": true, "utf16le": true,
			"utf32": true,
		}
		if !knownCharsets[charset] {
			return "", false
		}
	}
	return charset, true
}

func (c *Catalog) CreateDatabase(name string) error {
	return c.CreateDatabaseWithCharset(name, "", "")
}

// CreateDatabaseWithCharset creates a database with optional charset and collation.
func (c *Catalog) CreateDatabaseWithCharset(name, charset, collation string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.Databases[name]; exists {
		return fmt.Errorf("database '%s' already exists", name)
	}
	if charset == "" {
		charset = "utf8mb4"
	}
	if collation == "" {
		collation = DefaultCollationForCharset(charset)
	}
	c.Databases[name] = &Database{
		Name:          name,
		Tables:        make(map[string]*TableDef),
		Triggers:      make(map[string]*TriggerDef),
		Procedures:    make(map[string]*ProcedureDef),
		Functions:     make(map[string]*FunctionDef),
		CharacterSet:  charset,
		CollationName: collation,
	}
	return nil
}

func (c *Catalog) GetDatabase(name string) (*Database, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	db, ok := c.Databases[name]
	if !ok {
		return nil, fmt.Errorf("unknown database '%s'", name)
	}
	return db, nil
}

func (c *Catalog) DropDatabase(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.Databases[name]; !exists {
		return fmt.Errorf("database '%s' doesn't exist", name)
	}
	delete(c.Databases, name)
	return nil
}

func (c *Catalog) ListDatabases() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	names := make([]string, 0, len(c.Databases))
	for name := range c.Databases {
		names = append(names, name)
	}
	return names
}

func (db *Database) CreateTable(def *TableDef) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, exists := db.Tables[def.Name]; exists {
		return fmt.Errorf("table '%s' already exists", def.Name)
	}
	db.Tables[def.Name] = def
	return nil
}

func (db *Database) GetTable(name string) (*TableDef, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	t, ok := db.Tables[name]
	if !ok {
		return nil, fmt.Errorf("table '%s.%s' doesn't exist", db.Name, name)
	}
	return t, nil
}

func (db *Database) DropTable(name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, exists := db.Tables[name]; !exists {
		return fmt.Errorf("table '%s.%s' doesn't exist", db.Name, name)
	}
	delete(db.Tables, name)
	return nil
}

func (db *Database) ListTables() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	names := make([]string, 0, len(db.Tables))
	for name := range db.Tables {
		names = append(names, name)
	}
	return names
}

// AddColumn adds a new column definition to the table.
// If position is "FIRST", it prepends. If position is "AFTER <col>", it inserts after that column.
// Otherwise it appends.
func (db *Database) AddColumn(tableName string, col ColumnDef) error {
	return db.AddColumnAt(tableName, col, "", "")
}

// AddColumnAt adds a column at a specific position.
func (db *Database) AddColumnAt(tableName string, col ColumnDef, position string, afterCol string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	tbl, ok := db.Tables[tableName]
	if !ok {
		return fmt.Errorf("table '%s.%s' doesn't exist", db.Name, tableName)
	}
	for _, c := range tbl.Columns {
		if strings.EqualFold(c.Name, col.Name) {
			return fmt.Errorf("column '%s' already exists in table '%s'", col.Name, tableName)
		}
	}
	if position == "FIRST" {
		tbl.Columns = append([]ColumnDef{col}, tbl.Columns...)
	} else if position == "AFTER" && afterCol != "" {
		idx := -1
		for i, c := range tbl.Columns {
			if strings.EqualFold(c.Name, afterCol) {
				idx = i
				break
			}
		}
		if idx >= 0 {
			newCols := make([]ColumnDef, 0, len(tbl.Columns)+1)
			newCols = append(newCols, tbl.Columns[:idx+1]...)
			newCols = append(newCols, col)
			newCols = append(newCols, tbl.Columns[idx+1:]...)
			tbl.Columns = newCols
		} else {
			tbl.Columns = append(tbl.Columns, col)
		}
	} else {
		tbl.Columns = append(tbl.Columns, col)
	}
	return nil
}

// DropColumn removes a column from the table definition.
// It also removes any indexes that reference only this column,
// and removes the column from multi-column indexes.
func (db *Database) DropColumn(tableName, colName string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	tbl, ok := db.Tables[tableName]
	if !ok {
		return fmt.Errorf("table '%s.%s' doesn't exist", db.Name, tableName)
	}
	newCols := make([]ColumnDef, 0, len(tbl.Columns))
	found := false
	for _, c := range tbl.Columns {
		if strings.EqualFold(c.Name, colName) {
			found = true
			continue
		}
		newCols = append(newCols, c)
	}
	if !found {
		return fmt.Errorf("column '%s' doesn't exist in table '%s'", colName, tableName)
	}
	tbl.Columns = newCols

	// Remove indexes that reference the dropped column
	newIndexes := make([]IndexDef, 0, len(tbl.Indexes))
	for _, idx := range tbl.Indexes {
		// Remove the dropped column from the index's column list
		var remainingCols []string
		for _, c := range idx.Columns {
			// Handle column with length prefix like "c1(10)"
			bareCol := c
			if paren := strings.Index(c, "("); paren >= 0 {
				bareCol = c[:paren]
			}
			if !strings.EqualFold(bareCol, colName) {
				remainingCols = append(remainingCols, c)
			}
		}
		if len(remainingCols) > 0 {
			idx.Columns = remainingCols
			newIndexes = append(newIndexes, idx)
		}
		// If no columns remain, the index is dropped entirely
	}
	tbl.Indexes = newIndexes

	// Also remove from primary key if present
	var newPK []string
	for _, pk := range tbl.PrimaryKey {
		if !strings.EqualFold(pk, colName) {
			newPK = append(newPK, pk)
		}
	}
	tbl.PrimaryKey = newPK

	return nil
}

// ModifyColumn replaces the definition of an existing column (same name).
func (db *Database) ModifyColumn(tableName string, col ColumnDef) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	tbl, ok := db.Tables[tableName]
	if !ok {
		return fmt.Errorf("table '%s.%s' doesn't exist", db.Name, tableName)
	}
	for i, c := range tbl.Columns {
		if strings.EqualFold(c.Name, col.Name) {
			// Preserve PrimaryKey/Unique/Nullable flags from constraints
			// if the column is part of the primary key
			for _, pk := range tbl.PrimaryKey {
				if strings.EqualFold(pk, col.Name) {
					col.PrimaryKey = true
					col.Nullable = false
					break
				}
			}
			// Preserve Unique flag from indexes
			if !col.Unique && c.Unique {
				col.Unique = true
			}
			// Keep the existing canonical column name for MODIFY.
			col.Name = c.Name
			tbl.Columns[i] = col
			return nil
		}
	}
	return fmt.Errorf("column '%s' doesn't exist in table '%s'", col.Name, tableName)
}

// ChangeColumn renames a column and updates its definition.
func (db *Database) ChangeColumn(tableName, oldName string, col ColumnDef) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	tbl, ok := db.Tables[tableName]
	if !ok {
		return fmt.Errorf("table '%s.%s' doesn't exist", db.Name, tableName)
	}
	found := false
	oldCanonicalName := oldName
	for i, c := range tbl.Columns {
		if strings.EqualFold(c.Name, oldName) {
			// Preserve PrimaryKey/Unique/Nullable flags from constraints
			for _, pk := range tbl.PrimaryKey {
				if strings.EqualFold(pk, oldName) {
					col.PrimaryKey = true
					col.Nullable = false
					break
				}
			}
			if !col.Unique && c.Unique {
				col.Unique = true
			}
			oldCanonicalName = c.Name
			tbl.Columns[i] = col
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("column '%s' doesn't exist in table '%s'", oldName, tableName)
	}
	// Update index references if column name changed
	if !strings.EqualFold(oldCanonicalName, col.Name) {
		for i, idx := range tbl.Indexes {
			for j, c := range idx.Columns {
				// Strip length suffix for comparison
				colName := c
				suffix := ""
				if parenIdx := strings.Index(c, "("); parenIdx >= 0 {
					colName = c[:parenIdx]
					suffix = c[parenIdx:]
				}
				if strings.EqualFold(colName, oldCanonicalName) {
					tbl.Indexes[i].Columns[j] = col.Name + suffix
				}
			}
		}
		// Update primary key references
		for i, pk := range tbl.PrimaryKey {
			if strings.EqualFold(pk, oldCanonicalName) {
				tbl.PrimaryKey[i] = col.Name
			}
		}
	}
	return nil
}

// AddIndex adds an index definition to the table.
func (db *Database) AddIndex(tableName string, idx IndexDef) {
	db.mu.Lock()
	defer db.mu.Unlock()
	tbl, ok := db.Tables[tableName]
	if !ok {
		return
	}
	tbl.Indexes = append(tbl.Indexes, idx)
	// If this is a UNIQUE index, mark the column as unique
	if idx.Unique && len(idx.Columns) == 1 {
		colName := idx.Columns[0]
		// Strip length suffix
		if parenIdx := strings.Index(colName, "("); parenIdx >= 0 {
			colName = colName[:parenIdx]
		}
		for i, col := range tbl.Columns {
			if strings.EqualFold(col.Name, colName) {
				tbl.Columns[i].Unique = true
				break
			}
		}
	}
}

// SetPrimaryKey sets the primary key columns for the table.
func (db *Database) SetPrimaryKey(tableName string, cols []string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	tbl, ok := db.Tables[tableName]
	if !ok {
		return
	}
	tbl.PrimaryKey = cols
	// Mark columns as not-nullable (PK implies NOT NULL)
	for i, c := range tbl.Columns {
		for _, pk := range cols {
			if c.Name == pk {
				tbl.Columns[i].PrimaryKey = true
				tbl.Columns[i].Nullable = false
			}
		}
	}
}

// DropIndex removes an index by name from the table definition.
func (db *Database) DropIndex(tableName, indexName string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	tbl, ok := db.Tables[tableName]
	if !ok {
		return fmt.Errorf("table '%s.%s' doesn't exist", db.Name, tableName)
	}
	newIndexes := make([]IndexDef, 0, len(tbl.Indexes))
	found := false
	for _, idx := range tbl.Indexes {
		if strings.EqualFold(idx.Name, indexName) {
			found = true
			continue
		}
		newIndexes = append(newIndexes, idx)
	}
	if !found {
		return fmt.Errorf("Can't DROP '%s'; check that column/key exists", indexName)
	}
	tbl.Indexes = newIndexes
	return nil
}

// DropPrimaryKey removes the primary key from the table definition.
func (db *Database) DropPrimaryKey(tableName string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	tbl, ok := db.Tables[tableName]
	if !ok {
		return
	}
	for i, c := range tbl.Columns {
		if c.PrimaryKey {
			tbl.Columns[i].PrimaryKey = false
		}
	}
	tbl.PrimaryKey = nil
}

// CreateTrigger adds a trigger definition to the database.
func (db *Database) CreateTrigger(def *TriggerDef) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.Triggers == nil {
		db.Triggers = make(map[string]*TriggerDef)
	}
	db.Triggers[def.Name] = def
}

// DropTrigger removes a trigger by name.
func (db *Database) DropTrigger(name string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.Triggers, name)
}

// GetTriggersForTable returns all triggers for a given table, timing, and event.
func (db *Database) GetTriggersForTable(table, timing, event string) []*TriggerDef {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var result []*TriggerDef
	for _, tr := range db.Triggers {
		if strings.EqualFold(tr.Table, table) &&
			strings.EqualFold(tr.Timing, timing) &&
			strings.EqualFold(tr.Event, event) {
			result = append(result, tr)
		}
	}
	return result
}

// GetAllTriggersForTable returns all triggers defined on the given table (any timing/event).
func (db *Database) GetAllTriggersForTable(table string) []*TriggerDef {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var result []*TriggerDef
	for _, tr := range db.Triggers {
		if strings.EqualFold(tr.Table, table) {
			result = append(result, tr)
		}
	}
	return result
}

// CreateProcedure adds a stored procedure definition.
func (db *Database) CreateProcedure(def *ProcedureDef) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.Procedures == nil {
		db.Procedures = make(map[string]*ProcedureDef)
	}
	// Store with normalized key for case-insensitive and accent-insensitive lookup.
	// MySQL treats routine names as case and accent insensitive.
	db.Procedures[normalizeName(def.Name)] = def
}

// DropProcedure removes a stored procedure by name.
func (db *Database) DropProcedure(name string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.Procedures, normalizeName(name))
}

// GetProcedure returns a stored procedure by name (case-insensitive, accent-insensitive).
func (db *Database) GetProcedure(name string) *ProcedureDef {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.Procedures[normalizeName(name)]
}

// CreateFunction adds a stored function definition.
func (db *Database) CreateFunction(def *FunctionDef) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.Functions == nil {
		db.Functions = make(map[string]*FunctionDef)
	}
	db.Functions[normalizeName(def.Name)] = def
}

// DropFunction removes a stored function by name.
func (db *Database) DropFunction(name string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.Functions, normalizeName(name))
}

// GetFunction returns a stored function by name (case-insensitive, accent-insensitive).
func (db *Database) GetFunction(name string) *FunctionDef {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.Functions == nil {
		return nil
	}
	return db.Functions[normalizeName(name)]
}

// ListFunctions returns all stored function definitions in the database.
func (db *Database) ListFunctions() []*FunctionDef {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var result []*FunctionDef
	for _, f := range db.Functions {
		result = append(result, f)
	}
	return result
}

// ListProcedures returns all stored procedure definitions in the database.
func (db *Database) ListProcedures() []*ProcedureDef {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var result []*ProcedureDef
	for _, p := range db.Procedures {
		result = append(result, p)
	}
	return result
}
