package storage

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/myuon/mylite/catalog"
)

// Row represents a single row as a map of column name to value.
// Values are stored as interface{} (nil for NULL).
type Row map[string]interface{}

// Table is the in-memory storage for a single table.
type Table struct {
	Def           *catalog.TableDef
	Rows          []Row
	AutoIncrement atomic.Int64
	Mu            sync.RWMutex
}

func (t *Table) Lock()   { t.Mu.Lock() }
func (t *Table) Unlock() { t.Mu.Unlock() }

// Engine is the in-memory storage engine managing all tables per database.
type Engine struct {
	databases map[string]map[string]*Table // dbName -> tableName -> Table
	mu        sync.RWMutex
}

func NewEngine() *Engine {
	e := &Engine{
		databases: make(map[string]map[string]*Table),
	}
	e.databases["test"] = make(map[string]*Table)
	return e
}

func (e *Engine) EnsureDatabase(name string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.databases[name]; !ok {
		e.databases[name] = make(map[string]*Table)
	}
}

func (e *Engine) DropDatabase(name string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.databases, name)
}

func (e *Engine) CreateTable(dbName string, def *catalog.TableDef) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.databases[dbName]; !ok {
		e.databases[dbName] = make(map[string]*Table)
	}
	t := &Table{Def: def, Rows: make([]Row, 0)}
	t.AutoIncrement.Store(1)
	e.databases[dbName][def.Name] = t
}

func (e *Engine) DropTable(dbName, tableName string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if db, ok := e.databases[dbName]; ok {
		delete(db, tableName)
	}
}

func (e *Engine) GetTable(dbName, tableName string) (*Table, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	db, ok := e.databases[dbName]
	if !ok {
		return nil, fmt.Errorf("unknown database '%s'", dbName)
	}
	t, ok := db[tableName]
	if !ok {
		return nil, fmt.Errorf("table '%s.%s' doesn't exist", dbName, tableName)
	}
	return t, nil
}

// Insert adds a row to the table. Handles AUTO_INCREMENT.
func (t *Table) Insert(row Row) (int64, error) {
	t.Mu.Lock()
	defer t.Mu.Unlock()

	// Handle AUTO_INCREMENT
	var lastInsertID int64
	for _, col := range t.Def.Columns {
		if col.AutoIncrement {
			if v, ok := row[col.Name]; !ok || v == nil {
				id := t.AutoIncrement.Add(1) - 1
				row[col.Name] = id
				lastInsertID = id
			} else {
				// If explicit value provided, update auto_increment counter if needed
				if intVal, ok := toInt64(v); ok && intVal >= t.AutoIncrement.Load() {
					t.AutoIncrement.Store(intVal + 1)
				}
				lastInsertID = toInt64Val(v)
			}
		}
	}

	t.Rows = append(t.Rows, row)
	return lastInsertID, nil
}

// Scan returns all rows (snapshot).
func (t *Table) Scan() []Row {
	t.Mu.RLock()
	defer t.Mu.RUnlock()
	result := make([]Row, len(t.Rows))
	copy(result, t.Rows)
	return result
}

// Truncate removes all rows.
func (t *Table) Truncate() {
	t.Mu.Lock()
	defer t.Mu.Unlock()
	t.Rows = make([]Row, 0)
	t.AutoIncrement.Store(1)
}

func toInt64(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int64:
		return val, true
	case float64:
		return int64(val), true
	default:
		return 0, false
	}
}

func toInt64Val(v interface{}) int64 {
	val, _ := toInt64(v)
	return val
}
