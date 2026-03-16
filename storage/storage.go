package storage

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/myuon/mylite/catalog"
)

// Row represents a single row as a map of column name to value.
// Values are stored as interface{} (nil for NULL).
type Row map[string]interface{}

// displayValue formats a value for error messages, stripping trailing null bytes
// from strings (for BINARY column display).
func displayValue(v interface{}) string {
	s := fmt.Sprintf("%v", v)
	return strings.TrimRight(s, "\x00")
}

// Table is the in-memory storage for a single table.
type Table struct {
	Def             *catalog.TableDef
	Rows            []Row
	AutoIncrement   atomic.Int64
	AIExplicitlySet bool // true if AUTO_INCREMENT was explicitly set via ALTER/CREATE TABLE
	Mu              sync.RWMutex
}

func (t *Table) Lock()                     { t.Mu.Lock() }
func (t *Table) Unlock()                   { t.Mu.Unlock() }
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
			v, exists := row[col.Name]
			// In MySQL, inserting 0 into an auto-increment column is treated like NULL
			// (generates the next auto-increment value), unless NO_AUTO_VALUE_ON_ZERO is set.
			isZero := false
			if exists && v != nil {
				if intVal, ok := toInt64(v); ok && intVal == 0 {
					isZero = true
				}
			}
			if !exists || v == nil || isZero {
				// For nullable AI columns with explicitly-set AUTO_INCREMENT,
				// inserting NULL stores NULL.
				// Otherwise (NOT NULL or default AI), generate the next value.
				if col.Nullable && exists && v == nil && t.AIExplicitlySet {
					// Explicit NULL on a nullable AI column with explicit AI start: keep NULL
					lastInsertID = 0
				} else {
					maxID, hasMax, isUnsignedBigint := autoIncrementMaxForType(col.Type)
					cur := t.AutoIncrement.Load()
					if isUnsignedBigint && cur >= math.MaxInt64 {
						return 0, fmt.Errorf("Failed to read auto-increment value from storage engine")
					}
					id := cur + 1
					// Signed int64 overflow guard (BIGINT signed max case).
					if id < cur {
						id = cur
					}
					// MySQL auto_increment on bounded integer types saturates at type max;
					// next inserts then hit duplicate-key on the saturated value.
					if hasMax && id > maxID {
						id = maxID
					}
					t.AutoIncrement.Store(id)
					row[col.Name] = id
					lastInsertID = id
				}
			} else {
				// If explicit value provided, update auto_increment counter if needed
				// Store the value itself (not value+1) because Add(1) will return value+1
				if uv, ok := v.(uint64); ok {
					_, _, isUnsignedBigint := autoIncrementMaxForType(col.Type)
					if isUnsignedBigint {
						if uv > math.MaxInt64 {
							t.AutoIncrement.Store(math.MaxInt64)
						} else if int64(uv) >= t.AutoIncrement.Load() {
							t.AutoIncrement.Store(int64(uv))
						}
					} else if uv <= math.MaxInt64 && int64(uv) >= t.AutoIncrement.Load() {
						t.AutoIncrement.Store(int64(uv))
					}
					if uv <= math.MaxInt64 {
						lastInsertID = int64(uv)
					} else {
						lastInsertID = 0
					}
					continue
				}
				if intVal, ok := toInt64(v); ok && intVal >= t.AutoIncrement.Load() {
					t.AutoIncrement.Store(intVal)
				}
				lastInsertID = toInt64Val(v)
			}
		}
	}

	// Check NOT NULL constraints: in non-strict mode, insert zero value instead of error
	for _, col := range t.Def.Columns {
		if !col.Nullable && !col.AutoIncrement {
			v, exists := row[col.Name]
			if !exists || v == nil {
				// Insert type-appropriate zero value (non-strict mode behavior)
				upper := strings.ToUpper(col.Type)
				switch {
				case strings.Contains(upper, "INT") || strings.Contains(upper, "DECIMAL") ||
					strings.Contains(upper, "FLOAT") || strings.Contains(upper, "DOUBLE"):
					row[col.Name] = int64(0)
				case strings.Contains(upper, "CHAR") || strings.Contains(upper, "TEXT") ||
					strings.Contains(upper, "BLOB") || strings.Contains(upper, "ENUM"):
					row[col.Name] = ""
				case strings.Contains(upper, "DATE") || strings.Contains(upper, "TIME"):
					row[col.Name] = "0000-00-00 00:00:00"
				default:
					row[col.Name] = ""
				}
			}
		}
	}

	// Check PRIMARY KEY uniqueness
	if len(t.Def.PrimaryKey) > 0 {
		for _, existing := range t.Rows {
			match := true
			for _, pkCol := range t.Def.PrimaryKey {
				if fmt.Sprintf("%v", existing[pkCol]) != fmt.Sprintf("%v", row[pkCol]) {
					match = false
					break
				}
			}
			if match {
				pkVal := make([]string, len(t.Def.PrimaryKey))
				for i, pk := range t.Def.PrimaryKey {
					pkVal[i] = displayValue(row[pk])
				}
				return 0, fmt.Errorf("ERROR 1062 (23000): Duplicate entry '%s' for key 'PRIMARY'",
					strings.Join(pkVal, "-"))
			}
		}
	}

	// Check column-level PRIMARY KEY
	for _, col := range t.Def.Columns {
		if col.PrimaryKey {
			for _, existing := range t.Rows {
				if fmt.Sprintf("%v", existing[col.Name]) == fmt.Sprintf("%v", row[col.Name]) {
					return 0, fmt.Errorf("ERROR 1062 (23000): Duplicate entry '%s' for key 'PRIMARY'",
						displayValue(row[col.Name]))
				}
			}
		}
	}

	// Check UNIQUE constraints
	for _, idx := range t.Def.Indexes {
		if idx.Unique {
			for _, existing := range t.Rows {
				match := true
				for _, idxCol := range idx.Columns {
					ev := existing[idxCol]
					rv := row[idxCol]
					// NULL values don't violate UNIQUE constraints
					if ev == nil || rv == nil {
						match = false
						break
					}
					if fmt.Sprintf("%v", ev) != fmt.Sprintf("%v", rv) {
						match = false
						break
					}
				}
				if match {
					vals := make([]string, len(idx.Columns))
					for i, c := range idx.Columns {
						vals[i] = displayValue(row[c])
					}
					return 0, fmt.Errorf("ERROR 1062 (23000): Duplicate entry '%s' for key '%s'",
						strings.Join(vals, "-"), idx.Name)
				}
			}
		}
	}

	t.Rows = append(t.Rows, row)
	return lastInsertID, nil
}

func autoIncrementMaxForType(colType string) (int64, bool, bool) {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	base := upper
	if i := strings.IndexByte(base, '('); i >= 0 {
		base = base[:i]
	}
	isUnsigned := strings.Contains(upper, "UNSIGNED")
	base = strings.TrimSpace(strings.Replace(strings.Replace(base, "UNSIGNED", "", 1), "ZEROFILL", "", 1))
	switch base {
	case "TINYINT":
		if isUnsigned {
			return 255, true, false
		}
		return 127, true, false
	case "SMALLINT":
		if isUnsigned {
			return 65535, true, false
		}
		return 32767, true, false
	case "MEDIUMINT":
		if isUnsigned {
			return 16777215, true, false
		}
		return 8388607, true, false
	case "INT", "INTEGER":
		if isUnsigned {
			return 4294967295, true, false
		}
		return 2147483647, true, false
	case "BIGINT":
		if isUnsigned {
			// uint64 max does not fit int64 storage path.
			// Signal caller to raise ER_AUTOINC_READ_FAILED at int64 boundary.
			return math.MaxInt64, false, true
		}
		return math.MaxInt64, true, false
	}
	return 0, false, false
}

// Scan returns all rows (snapshot).
func (t *Table) Scan() []Row {
	t.Mu.RLock()
	defer t.Mu.RUnlock()
	result := make([]Row, len(t.Rows))
	copy(result, t.Rows)
	// InnoDB table scans are clustered by PRIMARY KEY.
	// Keep scan order deterministic and MySQL-compatible for tests that rely on it.
	if t.Def != nil && len(t.Def.PrimaryKey) > 0 && len(result) > 1 {
		pkCols := append([]string(nil), t.Def.PrimaryKey...)
		sort.SliceStable(result, func(i, j int) bool {
			ri, rj := result[i], result[j]
			for _, pk := range pkCols {
				cmp := compareRowValue(ri[pk], rj[pk])
				if cmp < 0 {
					return true
				}
				if cmp > 0 {
					return false
				}
			}
			return false
		})
	}
	return result
}

func compareRowValue(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	af, aok := toComparableFloat(a)
	bf, bok := toComparableFloat(b)
	if aok && bok {
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	}
	as := fmt.Sprintf("%v", a)
	bs := fmt.Sprintf("%v", b)
	if as < bs {
		return -1
	}
	if as > bs {
		return 1
	}
	return 0
}

func toComparableFloat(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(n), 64)
		if err == nil {
			return f, true
		}
	}
	return 0, false
}

// Truncate removes all rows and resets AUTO_INCREMENT.
func (t *Table) Truncate() {
	t.Mu.Lock()
	defer t.Mu.Unlock()
	t.Rows = make([]Row, 0)
	t.AutoIncrement.Store(0)
}

func toInt64(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int64:
		return val, true
	case float64:
		return int64(val), true
	case uint64:
		return int64(val), true
	case string:
		if n, err := strconv.ParseInt(val, 10, 64); err == nil {
			return n, true
		}
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return int64(f), true
		}
		return 0, false
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
