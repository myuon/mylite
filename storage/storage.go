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

func (t *Table) Lock()                   { t.Mu.Lock() }
func (t *Table) Unlock()                 { t.Mu.Unlock() }
func (t *Table) AutoIncrementValue() int64 { return t.AutoIncrement.Load() }

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
	t.AutoIncrement.Store(0)
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
				id := t.AutoIncrement.Add(1)
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

	// Check NOT NULL constraints
	for _, col := range t.Def.Columns {
		if !col.Nullable && !col.AutoIncrement {
			v, exists := row[col.Name]
			if !exists || v == nil {
				return 0, fmt.Errorf("ERROR 1048 (23000): Column '%s' cannot be null", col.Name)
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

// CloneRow returns a deep copy of a Row.
func CloneRow(row Row) Row {
	clone := make(Row, len(row))
	for k, v := range row {
		clone[k] = v
	}
	return clone
}

// TableSnapshot holds a point-in-time copy of a table's data.
type TableSnapshot struct {
	Def           *catalog.TableDef
	Rows          []Row
	AutoIncrement int64
}

// DatabaseSnapshot holds snapshots of all tables in a database.
type DatabaseSnapshot struct {
	Tables map[string]*TableSnapshot
}

// SnapshotDatabase returns a full snapshot of all tables in the given database.
// Returns nil if the database does not exist.
func (e *Engine) SnapshotDatabase(dbName string) *DatabaseSnapshot {
	e.mu.RLock()
	db, ok := e.databases[dbName]
	e.mu.RUnlock()
	if !ok {
		return nil
	}

	snap := &DatabaseSnapshot{
		Tables: make(map[string]*TableSnapshot),
	}

	// Lock each table individually to copy its data safely.
	for name, tbl := range db {
		tbl.Mu.RLock()
		rowsCopy := make([]Row, len(tbl.Rows))
		for i, r := range tbl.Rows {
			rowsCopy[i] = CloneRow(r)
		}
		snap.Tables[name] = &TableSnapshot{
			Def:           tbl.Def,
			Rows:          rowsCopy,
			AutoIncrement: tbl.AutoIncrement.Load(),
		}
		tbl.Mu.RUnlock()
	}

	return snap
}

// AddColumn sets the given key to defaultVal in every existing row of the table.
func (t *Table) AddColumn(colName string, defaultVal interface{}) {
	t.Mu.Lock()
	defer t.Mu.Unlock()
	for i := range t.Rows {
		t.Rows[i][colName] = defaultVal
	}
}

// DropColumn removes the given key from every existing row of the table.
func (t *Table) DropColumn(colName string) {
	t.Mu.Lock()
	defer t.Mu.Unlock()
	for i := range t.Rows {
		delete(t.Rows[i], colName)
	}
}

// RenameColumn renames the given key in every existing row of the table.
func (t *Table) RenameColumn(oldName, newName string) {
	t.Mu.Lock()
	defer t.Mu.Unlock()
	for i := range t.Rows {
		if v, ok := t.Rows[i][oldName]; ok {
			t.Rows[i][newName] = v
			delete(t.Rows[i], oldName)
		}
	}
}

// RestoreDatabase replaces the contents of the given database with the snapshot.
// Tables that existed in the snapshot but were dropped are recreated.
// Tables that were created after the snapshot was taken are removed.
func (e *Engine) RestoreDatabase(dbName string, snap *DatabaseSnapshot) {
	if snap == nil {
		return
	}

	e.mu.Lock()
	// Replace the entire table map with restored tables.
	restored := make(map[string]*Table, len(snap.Tables))
	for name, ts := range snap.Tables {
		rowsCopy := make([]Row, len(ts.Rows))
		for i, r := range ts.Rows {
			rowsCopy[i] = CloneRow(r)
		}
		t := &Table{
			Def:  ts.Def,
			Rows: rowsCopy,
		}
		t.AutoIncrement.Store(ts.AutoIncrement)
		restored[name] = t
	}
	e.databases[dbName] = restored
	e.mu.Unlock()
}
