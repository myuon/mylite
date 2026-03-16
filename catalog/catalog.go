package catalog

import (
	"fmt"
	"strings"
	"sync"
)

// ColumnDef represents a column definition in a table.
type ColumnDef struct {
	Name           string
	Type           string // MySQL type string (e.g. "INT", "VARCHAR(255)")
	Nullable       bool
	Default        *string
	AutoIncrement  bool
	PrimaryKey     bool
	Unique         bool
	Comment        string
	DefaultDropped bool // true when ALTER TABLE ... DROP DEFAULT was used
}

// IndexDef represents an index definition.
type IndexDef struct {
	Name    string
	Columns []string
	Unique  bool
	Using   string // Index method: BTREE, HASH, etc.
	Comment string // COMMENT clause on index
}

// TableDef represents a table definition.
type TableDef struct {
	Name       string
	Columns    []ColumnDef
	PrimaryKey []string // column names
	Indexes    []IndexDef
	Comment    string
	Charset    string // e.g. "latin1", "utf8mb4"; empty means default (utf8mb4)
	Collation  string // e.g. "latin1_swedish_ci"; empty means default
	Engine     string // e.g. "InnoDB", "MyISAM", "MEMORY"; empty means default (InnoDB)
	CheckConstraints []CheckConstraint // CHECK constraint definitions
}

// CheckConstraint represents a table-level CHECK constraint.
type CheckConstraint struct {
	Name string // constraint name (auto-generated if not specified)
	Expr string // SQL expression to evaluate
}

// TriggerDef represents a trigger definition.
type TriggerDef struct {
	Name    string
	Timing  string // "BEFORE" or "AFTER"
	Event   string // "INSERT", "UPDATE", or "DELETE"
	Table   string
	Body    []string // SQL statements in the trigger body
}

// ProcedureDef represents a stored procedure definition.
type ProcedureDef struct {
	Name   string
	Params []ProcParam
	Body   []string // SQL statements in the procedure body
}

// ProcParam represents a parameter in a stored procedure.
type ProcParam struct {
	Mode string // "IN", "OUT", "INOUT"
	Name string
	Type string
}

// FunctionDef represents a stored function definition.
type FunctionDef struct {
	Name       string
	Params     []ProcParam
	ReturnType string
	Body       []string // SQL statements in the function body
}

// Database represents a database containing tables.
type Database struct {
	Name           string
	Tables         map[string]*TableDef
	Triggers       map[string]*TriggerDef   // trigger name -> trigger def
	Procedures     map[string]*ProcedureDef // procedure name -> procedure def
	Functions      map[string]*FunctionDef  // function name -> function def
	CharacterSet   string // e.g. "utf8mb4", "ascii", "binary"
	CollationName  string // e.g. "utf8mb4_general_ci", "ascii_general_ci"
	mu             sync.RWMutex
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
		if c.Name == col.Name {
			return fmt.Errorf("column '%s' already exists in table '%s'", col.Name, tableName)
		}
	}
	if position == "FIRST" {
		tbl.Columns = append([]ColumnDef{col}, tbl.Columns...)
	} else if position == "AFTER" && afterCol != "" {
		idx := -1
		for i, c := range tbl.Columns {
			if c.Name == afterCol {
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
		if c.Name == colName {
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
			if bareCol != colName {
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
		if pk != colName {
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
		if c.Name == col.Name {
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
	for i, c := range tbl.Columns {
		if c.Name == oldName {
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
			tbl.Columns[i] = col
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("column '%s' doesn't exist in table '%s'", oldName, tableName)
	}
	// Update index references if column name changed
	if oldName != col.Name {
		for i, idx := range tbl.Indexes {
			for j, c := range idx.Columns {
				// Strip length suffix for comparison
				colName := c
				suffix := ""
				if parenIdx := strings.Index(c, "("); parenIdx >= 0 {
					colName = c[:parenIdx]
					suffix = c[parenIdx:]
				}
				if strings.EqualFold(colName, oldName) {
					tbl.Indexes[i].Columns[j] = col.Name + suffix
				}
			}
		}
		// Update primary key references
		for i, pk := range tbl.PrimaryKey {
			if strings.EqualFold(pk, oldName) {
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
		if idx.Name == indexName {
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

// CreateProcedure adds a stored procedure definition.
func (db *Database) CreateProcedure(def *ProcedureDef) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.Procedures == nil {
		db.Procedures = make(map[string]*ProcedureDef)
	}
	db.Procedures[def.Name] = def
}

// DropProcedure removes a stored procedure by name.
func (db *Database) DropProcedure(name string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.Procedures, name)
}

// GetProcedure returns a stored procedure by name.
func (db *Database) GetProcedure(name string) *ProcedureDef {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.Procedures[name]
}

// CreateFunction adds a stored function definition.
func (db *Database) CreateFunction(def *FunctionDef) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.Functions == nil {
		db.Functions = make(map[string]*FunctionDef)
	}
	db.Functions[def.Name] = def
}

// DropFunction removes a stored function by name.
func (db *Database) DropFunction(name string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.Functions, name)
}

// GetFunction returns a stored function by name.
func (db *Database) GetFunction(name string) *FunctionDef {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.Functions == nil {
		return nil
	}
	return db.Functions[name]
}
