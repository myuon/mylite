package catalog

import (
	"fmt"
	"sync"
)

// ColumnDef represents a column definition in a table.
type ColumnDef struct {
	Name          string
	Type          string // MySQL type string (e.g. "INT", "VARCHAR(255)")
	Nullable      bool
	Default       *string
	AutoIncrement bool
	PrimaryKey    bool
	Unique        bool
}

// TableDef represents a table definition.
type TableDef struct {
	Name       string
	Columns    []ColumnDef
	PrimaryKey []string // column names
}

// Database represents a database containing tables.
type Database struct {
	Name   string
	Tables map[string]*TableDef
	mu     sync.RWMutex
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
	// Create default "test" database
	c.Databases["test"] = &Database{
		Name:   "test",
		Tables: make(map[string]*TableDef),
	}
	return c
}

func (c *Catalog) CreateDatabase(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.Databases[name]; exists {
		return fmt.Errorf("database '%s' already exists", name)
	}
	c.Databases[name] = &Database{
		Name:   name,
		Tables: make(map[string]*TableDef),
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

// AddColumn appends a new column definition to the table.
func (db *Database) AddColumn(tableName string, col ColumnDef) error {
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
	tbl.Columns = append(tbl.Columns, col)
	return nil
}

// DropColumn removes a column from the table definition.
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
	for i, c := range tbl.Columns {
		if c.Name == oldName {
			tbl.Columns[i] = col
			return nil
		}
	}
	return fmt.Errorf("column '%s' doesn't exist in table '%s'", oldName, tableName)
}
