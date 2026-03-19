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

// stripPrefixLength strips the prefix length from a column name.
// e.g., "col_1_text(3072)" -> "col_1_text"
func stripPrefixLength(col string) string {
	if idx := strings.Index(col, "("); idx >= 0 {
		return col[:idx]
	}
	return col
}

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
	// pkIndex is a hash set of primary key values for O(1) uniqueness checks.
	// Lazily built on first Insert and maintained during Insert/BulkInsert.
	// Invalidated (set to nil) on delete/update/truncate operations.
	pkIndex map[string]bool
	// colPKIndex maps column-level PK column names to their value sets.
	colPKIndex map[string]map[string]bool
	// uniqueIndex maps unique index names to their value sets.
	uniqueIndex map[string]map[string]bool
}

func (t *Table) Lock()                     { t.Mu.Lock() }
func (t *Table) Unlock()                   { t.Mu.Unlock() }
func (t *Table) AutoIncrementValue() int64 { return t.AutoIncrement.Load() }

// InvalidateIndexes clears cached PK/UNIQUE indexes so they are rebuilt on next Insert.
// Must be called (or indexes will be stale) after delete/update/truncate/row-removal.
func (t *Table) InvalidateIndexes() {
	t.pkIndex = nil
	t.colPKIndex = nil
	t.uniqueIndex = nil
}

// ensureIndexes lazily builds hash-set indexes for PK and UNIQUE constraints.
// Caller must hold t.Mu (at least read lock).
func (t *Table) ensureIndexes() {
	if len(t.Def.PrimaryKey) > 0 && t.pkIndex == nil {
		t.pkIndex = make(map[string]bool, len(t.Rows))
		for _, existing := range t.Rows {
			key := bulkPKKey(existing, t.Def.PrimaryKey)
			t.pkIndex[key] = true
		}
	}
	// Column-level PK
	hasPKCol := false
	for _, col := range t.Def.Columns {
		if col.PrimaryKey {
			hasPKCol = true
			break
		}
	}
	if hasPKCol && t.colPKIndex == nil {
		t.colPKIndex = make(map[string]map[string]bool)
		for _, col := range t.Def.Columns {
			if col.PrimaryKey {
				s := make(map[string]bool, len(t.Rows))
				for _, existing := range t.Rows {
					s[fmt.Sprintf("%v", existing[col.Name])] = true
				}
				t.colPKIndex[col.Name] = s
			}
		}
	}
	// Unique indexes
	hasUnique := false
	for _, idx := range t.Def.Indexes {
		if idx.Unique {
			hasUnique = true
			break
		}
	}
	if hasUnique && t.uniqueIndex == nil {
		t.uniqueIndex = make(map[string]map[string]bool)
		for _, idx := range t.Def.Indexes {
			if idx.Unique {
				s := make(map[string]bool, len(t.Rows))
				for _, existing := range t.Rows {
					key := bulkUniqueKey(existing, idx.Columns)
					if key != "" {
						s[key] = true
					}
				}
				t.uniqueIndex[idx.Name] = s
			}
		}
	}
}

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

	// Build/ensure hash indexes for O(1) uniqueness checks
	t.ensureIndexes()

	// Check PRIMARY KEY uniqueness using hash index
	if len(t.Def.PrimaryKey) > 0 {
		key := bulkPKKey(row, t.Def.PrimaryKey)
		if t.pkIndex[key] {
			pkVal := make([]string, len(t.Def.PrimaryKey))
			for i, pk := range t.Def.PrimaryKey {
				pkVal[i] = displayValue(row[stripPrefixLength(pk)])
			}
			return 0, fmt.Errorf("ERROR 1062 (23000): Duplicate entry '%s' for key 'PRIMARY'",
				strings.Join(pkVal, "-"))
		}
	}

	// Check column-level PRIMARY KEY using hash index
	if t.colPKIndex != nil {
		for _, col := range t.Def.Columns {
			if col.PrimaryKey {
				valKey := fmt.Sprintf("%v", row[col.Name])
				if t.colPKIndex[col.Name][valKey] {
					return 0, fmt.Errorf("ERROR 1062 (23000): Duplicate entry '%s' for key 'PRIMARY'",
						displayValue(row[col.Name]))
				}
			}
		}
	}

	// Check UNIQUE constraints using hash index
	if t.uniqueIndex != nil {
		for _, idx := range t.Def.Indexes {
			if idx.Unique {
				key := bulkUniqueKey(row, idx.Columns)
				if key != "" && t.uniqueIndex[idx.Name][key] {
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

	// Maintain indexes after successful insert
	if t.pkIndex != nil && len(t.Def.PrimaryKey) > 0 {
		t.pkIndex[bulkPKKey(row, t.Def.PrimaryKey)] = true
	}
	if t.colPKIndex != nil {
		for _, col := range t.Def.Columns {
			if col.PrimaryKey {
				t.colPKIndex[col.Name][fmt.Sprintf("%v", row[col.Name])] = true
			}
		}
	}
	if t.uniqueIndex != nil {
		for _, idx := range t.Def.Indexes {
			if idx.Unique {
				key := bulkUniqueKey(row, idx.Columns)
				if key != "" {
					t.uniqueIndex[idx.Name][key] = true
				}
			}
		}
	}

	return lastInsertID, nil
}

// BulkInsert appends multiple rows under a single lock acquisition.
// It handles AUTO_INCREMENT, NOT NULL defaults, and PK/UNIQUE checks.
// Returns (lastInsertIDs slice, error). Each element corresponds to the
// auto-increment ID for that row (0 if not auto-generated).
func (t *Table) BulkInsert(rows []Row) ([]int64, error) {
	t.Mu.Lock()
	defer t.Mu.Unlock()

	// Find auto-increment column info once
	var autoCol *catalog.ColumnDef
	for i := range t.Def.Columns {
		if t.Def.Columns[i].AutoIncrement {
			autoCol = &t.Def.Columns[i]
			break
		}
	}

	var autoMaxID int64
	var autoHasMax, autoIsUnsignedBigint bool
	if autoCol != nil {
		autoMaxID, autoHasMax, autoIsUnsignedBigint = autoIncrementMaxForType(autoCol.Type)
	}

	// Determine if we need PK/UNIQUE checks
	hasPK := len(t.Def.PrimaryKey) > 0
	hasPKCol := false
	for _, col := range t.Def.Columns {
		if col.PrimaryKey {
			hasPKCol = true
			break
		}
	}
	hasUnique := false
	for _, idx := range t.Def.Indexes {
		if idx.Unique {
			hasUnique = true
			break
		}
	}

	needsUniquenessCheck := hasPK || hasPKCol || hasUnique

	// Build hash sets for PK/UNIQUE checks if needed
	var pkSet map[string]bool
	var colPKSets map[string]map[string]bool
	var uniqueSets map[string]map[string]bool // index name -> set of formatted key

	if needsUniquenessCheck {
		if hasPK {
			pkSet = make(map[string]bool, len(t.Rows))
			for _, existing := range t.Rows {
				key := bulkPKKey(existing, t.Def.PrimaryKey)
				pkSet[key] = true
			}
		}
		if hasPKCol {
			colPKSets = make(map[string]map[string]bool)
			for _, col := range t.Def.Columns {
				if col.PrimaryKey {
					s := make(map[string]bool, len(t.Rows))
					for _, existing := range t.Rows {
						s[fmt.Sprintf("%v", existing[col.Name])] = true
					}
					colPKSets[col.Name] = s
				}
			}
		}
		if hasUnique {
			uniqueSets = make(map[string]map[string]bool)
			for _, idx := range t.Def.Indexes {
				if idx.Unique {
					s := make(map[string]bool, len(t.Rows))
					for _, existing := range t.Rows {
						key := bulkUniqueKey(existing, idx.Columns)
						if key != "" { // empty means has NULL, skip
							s[key] = true
						}
					}
					uniqueSets[idx.Name] = s
				}
			}
		}
	}

	ids := make([]int64, len(rows))

	for ri, row := range rows {
		var lastInsertID int64

		// Handle AUTO_INCREMENT
		if autoCol != nil {
			v, exists := row[autoCol.Name]
			isZero := false
			if exists && v != nil {
				if intVal, ok := toInt64(v); ok && intVal == 0 {
					isZero = true
				}
			}
			if !exists || v == nil || isZero {
				if autoCol.Nullable && exists && v == nil && t.AIExplicitlySet {
					lastInsertID = 0
				} else {
					if autoIsUnsignedBigint && t.AutoIncrement.Load() >= math.MaxInt64 {
						return ids[:ri], fmt.Errorf("Failed to read auto-increment value from storage engine")
					}
					cur := t.AutoIncrement.Load()
					id := cur + 1
					if id < cur {
						id = cur
					}
					if autoHasMax && id > autoMaxID {
						id = autoMaxID
					}
					t.AutoIncrement.Store(id)
					row[autoCol.Name] = id
					lastInsertID = id
				}
			} else {
				if uv, ok := v.(uint64); ok {
					if autoIsUnsignedBigint {
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
					}
				} else {
					if intVal, ok := toInt64(v); ok && intVal >= t.AutoIncrement.Load() {
						t.AutoIncrement.Store(intVal)
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

		// PK/UNIQUE checks using hash sets
		if needsUniquenessCheck {
			if hasPK {
				key := bulkPKKey(row, t.Def.PrimaryKey)
				if pkSet[key] {
					pkVal := make([]string, len(t.Def.PrimaryKey))
					for i, pk := range t.Def.PrimaryKey {
						pkVal[i] = displayValue(row[stripPrefixLength(pk)])
					}
					return ids[:ri], fmt.Errorf("ERROR 1062 (23000): Duplicate entry '%s' for key 'PRIMARY'",
						strings.Join(pkVal, "-"))
				}
				pkSet[key] = true
			}
			if hasPKCol {
				for _, col := range t.Def.Columns {
					if col.PrimaryKey {
						key := fmt.Sprintf("%v", row[col.Name])
						if colPKSets[col.Name][key] {
							return ids[:ri], fmt.Errorf("ERROR 1062 (23000): Duplicate entry '%s' for key 'PRIMARY'",
								displayValue(row[col.Name]))
						}
						colPKSets[col.Name][key] = true
					}
				}
			}
			if hasUnique {
				for _, idx := range t.Def.Indexes {
					if idx.Unique {
						key := bulkUniqueKey(row, idx.Columns)
						if key != "" && uniqueSets[idx.Name][key] {
							vals := make([]string, len(idx.Columns))
							for i, c := range idx.Columns {
								vals[i] = displayValue(row[c])
							}
							return ids[:ri], fmt.Errorf("ERROR 1062 (23000): Duplicate entry '%s' for key '%s'",
								strings.Join(vals, "-"), idx.Name)
						}
						if key != "" {
							uniqueSets[idx.Name][key] = true
						}
					}
				}
			}
		}

		t.Rows = append(t.Rows, row)
		ids[ri] = lastInsertID
	}

	// Invalidate cached indexes since we added rows
	t.InvalidateIndexes()

	return ids, nil
}

// bulkPKKey builds a hash key for composite primary key lookup.
func bulkPKKey(row Row, pkCols []string) string {
	if len(pkCols) == 1 {
		return fmt.Sprintf("%v", row[stripPrefixLength(pkCols[0])])
	}
	parts := make([]string, len(pkCols))
	for i, pk := range pkCols {
		parts[i] = fmt.Sprintf("%v", row[stripPrefixLength(pk)])
	}
	return strings.Join(parts, "\x00")
}

// bulkUniqueKey builds a hash key for unique index lookup.
// Returns "" if any column is NULL (NULL doesn't violate UNIQUE).
func bulkUniqueKey(row Row, cols []string) string {
	if len(cols) == 1 {
		v := row[cols[0]]
		if v == nil {
			return ""
		}
		return fmt.Sprintf("%v", v)
	}
	parts := make([]string, len(cols))
	for i, c := range cols {
		v := row[c]
		if v == nil {
			return ""
		}
		parts[i] = fmt.Sprintf("%v", v)
	}
	return strings.Join(parts, "\x00")
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
	// Skip sorting for charsets where Go's byte-order comparison does not match
	// the MySQL collation order (e.g. sjis, cp932, ujis, eucjpms).
	if t.Def != nil && len(t.Def.PrimaryKey) > 0 && len(result) > 1 && !hasNonSortableCharset(t.Def.Charset) {
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

// hasNonSortableCharset returns true when the table uses a character set whose
// collation order cannot be approximated by Go's default byte-level string
// comparison.  For these charsets we fall back to insertion order instead of
// attempting an incorrect PK sort.
func hasNonSortableCharset(cs string) bool {
	switch strings.ToLower(cs) {
	case "sjis", "cp932", "ujis", "eucjpms", "euckr", "gb2312", "gbk", "gb18030", "big5":
		return true
	}
	return false
}

// Truncate removes all rows and resets AUTO_INCREMENT.
func (t *Table) Truncate() {
	t.Mu.Lock()
	defer t.Mu.Unlock()
	t.Rows = make([]Row, 0)
	t.AutoIncrement.Store(0)
	t.InvalidateIndexes()
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
	t.InvalidateIndexes()
}

// DropColumn removes the given key from every existing row of the table.
func (t *Table) DropColumn(colName string) {
	t.Mu.Lock()
	defer t.Mu.Unlock()
	for i := range t.Rows {
		for k := range t.Rows[i] {
			if strings.EqualFold(k, colName) {
				delete(t.Rows[i], k)
			}
		}
	}
	t.InvalidateIndexes()
}

// RenameColumn renames the given key in every existing row of the table.
func (t *Table) RenameColumn(oldName, newName string) {
	t.Mu.Lock()
	defer t.Mu.Unlock()
	for i := range t.Rows {
		for k, v := range t.Rows[i] {
			if strings.EqualFold(k, oldName) {
				t.Rows[i][newName] = v
				delete(t.Rows[i], k)
				break
			}
		}
	}
	t.InvalidateIndexes()
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
