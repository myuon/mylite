package storage

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/myuon/mylite/catalog"
)

// IndexAccessSpec describes how a table should be accessed via an index.
//
// This struct is the contract between the optimizer (which produces
// AccessPath information when planning a query) and the executor (which
// can use it to skip the full table scan and read only the rows the
// access path identifies).
//
// The zero value (Type="" or Type="ALL") means "fall back to the full
// table scan". Callers should detect AccessType and route to the
// appropriate Table method.
//
// Phase 5 scaffolding: this type is the public API surface for the new
// index-based execution path. Real call sites are still gated behind a
// feature flag while the executor is migrated.
type IndexAccessSpec struct {
	// Type mirrors AccessPath.Type from the planner — one of "const",
	// "eq_ref", "ref", "range", "index", "ALL".
	Type string

	// IndexName is the index used for the lookup. "PRIMARY" denotes the
	// primary key; otherwise the name matches a catalog.IndexDef.Name.
	// Empty when Type=="ALL".
	IndexName string

	// EqualityValues holds the per-column equality values for const /
	// eq_ref / ref lookups. The slice is indexed by column position in
	// the index definition (so EqualityValues[0] is the value for the
	// first column of the index). Values may be of any concrete Go type
	// stored in Row maps.
	//
	// For ref-or-null access, NULL is represented as a nil interface{}.
	EqualityValues []interface{}

	// RangeLower and RangeUpper hold the prefix-equality + final-range
	// bounds used by range-style access. RangeLower[i] / RangeUpper[i]
	// correspond to the i-th column of IndexName. A nil entry on either
	// bound means "unbounded on that side". RangeLowerInclusive /
	// RangeUpperInclusive govern the boundary semantics for the final
	// (range-typed) column; intermediate equality columns are always
	// inclusive.
	RangeLower          []interface{}
	RangeUpper          []interface{}
	RangeLowerInclusive bool
	RangeUpperInclusive bool

	// InValues is set when the optimizer chose an IN-list driven range
	// scan. Each element is one full key tuple to look up.
	InValues [][]interface{}
}

// rowMatchesEquality reports whether the given row's values for the
// columns named in idxCols equal the supplied values, using string
// formatting for comparison (matches existing PK lookup semantics).
func rowMatchesEquality(row Row, idxCols []string, values []interface{}) bool {
	if len(values) > len(idxCols) {
		return false
	}
	for i, v := range values {
		col := stripPrefixLength(idxCols[i])
		got := rowGetCI(row, col)
		if !valuesEqualLoose(got, v) {
			return false
		}
	}
	return true
}

// valuesEqualLoose compares two interface{} values with the same
// "stringify and compare" behavior as bulkPKKey / bulkUniqueKey, so that
// optimizer-built IndexAccessSpec values match rows the same way the
// existing in-memory hash indexes already do.
func valuesEqualLoose(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	// Prefer exact integer comparison when both values are integral. Float
	// conversion loses precision near the int64/uint64 limits — e.g.
	// float64(MaxInt64-1) == float64(MaxInt64), causing PK lookups for
	// large BIGINT values to either miss or alias their neighbour.
	if ai, aok := toComparableInt64(a); aok {
		if bi, bok := toComparableInt64(b); bok {
			return ai == bi
		}
	}
	// Try numeric comparison first to avoid 1 vs "1" mismatches caused
	// by literal-vs-stored type differences.
	if af, ok := toComparableFloat(a); ok {
		if bf, ok := toComparableFloat(b); ok {
			return af == bf
		}
	}
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// toComparableInt64 returns the int64 value when v is an integer
// (signed or unsigned, or a string holding an integer literal). Floats
// are rejected because they may not round-trip exactly.
func toComparableInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case uint:
		if n <= math.MaxInt64 {
			return int64(n), true
		}
	case uint8:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint64:
		if n <= math.MaxInt64 {
			return int64(n), true
		}
	case string:
		if i, err := strconv.ParseInt(strings.TrimSpace(n), 10, 64); err == nil {
			return i, true
		}
	}
	return 0, false
}

// resolveIndexColumns returns the column list for the given index name,
// or nil + false if the index isn't known. "PRIMARY" resolves to the
// table's primary key.
func (t *Table) resolveIndexColumns(indexName string) ([]string, bool) {
	if t.Def == nil {
		return nil, false
	}
	if strings.EqualFold(indexName, "PRIMARY") {
		if len(t.Def.PrimaryKey) == 0 {
			return nil, false
		}
		return append([]string(nil), t.Def.PrimaryKey...), true
	}
	for _, idx := range t.Def.Indexes {
		if strings.EqualFold(idx.Name, indexName) {
			return append([]string(nil), idx.Columns...), true
		}
	}
	return nil, false
}

// GetByPK returns the row whose primary key columns match the supplied
// values (in PrimaryKey-declaration order). The boolean return mirrors
// the standard map-lookup idiom: false when no row matches or the table
// has no primary key.
//
// This is the "const" access type: a single direct lookup.
func (t *Table) GetByPK(pkValues []interface{}) (Row, bool) {
	if t == nil || t.Def == nil || len(t.Def.PrimaryKey) == 0 {
		return nil, false
	}
	if len(pkValues) != len(t.Def.PrimaryKey) {
		return nil, false
	}
	t.Mu.RLock()
	defer t.Mu.RUnlock()
	pkCols := t.Def.PrimaryKey
	for _, row := range t.Rows {
		match := true
		for i, pk := range pkCols {
			col := stripPrefixLength(pk)
			if !valuesEqualLoose(rowGetCI(row, col), pkValues[i]) {
				match = false
				break
			}
		}
		if match {
			return cloneRowSnapshot(row), true
		}
	}
	return nil, false
}

// ScanByIndex returns the rows that match the supplied IndexAccessSpec.
// The behavior depends on spec.Type:
//
//   - "const", "eq_ref": single-row PK / unique-index lookup using
//     EqualityValues. Returns at most one row.
//   - "ref": equality lookup on a (potentially non-unique) index. May
//     return multiple rows.
//   - "range": prefix-equality + final-range scan. EqualityValues holds
//     the leading equality prefix; RangeLower/RangeUpper bound the next
//     column. When InValues is non-empty it is used instead.
//   - "index": full index scan (currently equivalent to a sorted full
//     scan of the table; index-only access is simulated by reading all
//     rows in PK order, matching the existing Scan() behaviour).
//   - "ALL" or empty: callers should fall back to Scan(); ScanByIndex
//     also handles this case by returning Scan() to keep the call site
//     simple.
//
// Returned rows are deep-copied (same semantics as Scan()).
//
// This is intentionally conservative: it scans the in-memory rows and
// filters them using the specified key columns. It does NOT yet build
// or use any sorted index data structure — the win at this stage is
// correctness of the shape (single-row for const, filtered subset for
// ref) rather than algorithmic speed-up. Future PRs can specialize the
// hot paths once the executor reliably plumbs AccessPath through.
func (t *Table) ScanByIndex(spec IndexAccessSpec) ([]Row, error) {
	if t == nil {
		return nil, fmt.Errorf("ScanByIndex: nil table")
	}
	switch strings.ToLower(spec.Type) {
	case "", "all":
		return t.Scan(), nil
	case "index":
		// Full index scan: return all rows in the order of the named
		// index. Mirrors MySQL's covering-index full-scan behaviour
		// (Extra="Using index"), where the result set is implicitly
		// sorted by the index's leading columns.
		idxCols, ok := t.resolveIndexColumns(spec.IndexName)
		if !ok || len(idxCols) == 0 {
			return t.Scan(), nil
		}
		return t.scanByIndexOrder(idxCols), nil
	}

	idxCols, ok := t.resolveIndexColumns(spec.IndexName)
	if !ok {
		// Index not found — fall back to a full scan rather than error
		// out. The optimizer may have selected an index that does not
		// (yet) materialize as a real catalog index.
		return t.Scan(), nil
	}

	switch strings.ToLower(spec.Type) {
	case "const", "eq_ref":
		if len(spec.EqualityValues) != len(idxCols) {
			return t.Scan(), nil
		}
		t.Mu.RLock()
		defer t.Mu.RUnlock()
		for _, row := range t.Rows {
			if rowMatchesEquality(row, idxCols, spec.EqualityValues) {
				return []Row{cloneRowSnapshot(row)}, nil
			}
		}
		return nil, nil

	case "ref":
		// Equality lookup on a (possibly non-unique) prefix of the index.
		t.Mu.RLock()
		defer t.Mu.RUnlock()
		var out []Row
		for _, row := range t.Rows {
			if rowMatchesEquality(row, idxCols, spec.EqualityValues) {
				out = append(out, cloneRowSnapshot(row))
			}
		}
		return out, nil

	case "range":
		t.Mu.RLock()
		defer t.Mu.RUnlock()
		// IN-list driven range: equality on each tuple, OR'd together.
		if len(spec.InValues) > 0 {
			seen := make(map[int]bool, len(spec.InValues))
			var out []Row
			for i, row := range t.Rows {
				if seen[i] {
					continue
				}
				for _, tup := range spec.InValues {
					if rowMatchesEquality(row, idxCols, tup) {
						out = append(out, cloneRowSnapshot(row))
						seen[i] = true
						break
					}
				}
			}
			return out, nil
		}
		// Prefix equality + final-column range comparison.
		eqLen := len(spec.EqualityValues)
		var out []Row
		for _, row := range t.Rows {
			if eqLen > 0 && !rowMatchesEquality(row, idxCols, spec.EqualityValues) {
				continue
			}
			if eqLen >= len(idxCols) {
				// No range column left; treat as ref.
				out = append(out, cloneRowSnapshot(row))
				continue
			}
			rangeCol := stripPrefixLength(idxCols[eqLen])
			v := rowGetCI(row, rangeCol)
			if !inRange(v, spec.RangeLower, spec.RangeUpper, eqLen,
				spec.RangeLowerInclusive, spec.RangeUpperInclusive) {
				continue
			}
			out = append(out, cloneRowSnapshot(row))
		}
		return out, nil
	}

	return t.Scan(), nil
}

// scanByIndexOrder returns all rows sorted by the supplied index
// column list (in declaration order). Used to simulate a covering
// secondary-index full scan: rows come back in index-key order rather
// than primary-key order.
//
// Ties are broken by the table's primary key (when present), matching
// InnoDB's secondary-index physical layout where each leaf entry is
// (idx_cols..., pk_cols...).
func (t *Table) scanByIndexOrder(idxCols []string) []Row {
	t.Mu.RLock()
	defer t.Mu.RUnlock()
	out := make([]Row, len(t.Rows))
	for i, row := range t.Rows {
		out[i] = cloneRowSnapshot(row)
	}
	if len(out) <= 1 {
		return out
	}
	bare := make([]string, len(idxCols))
	for i, c := range idxCols {
		bare[i] = stripPrefixLength(c)
	}
	var pkCols []string
	if t.Def != nil {
		pkCols = make([]string, len(t.Def.PrimaryKey))
		for i, c := range t.Def.PrimaryKey {
			pkCols[i] = stripPrefixLength(c)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		ri, rj := out[i], out[j]
		for _, c := range bare {
			cmp := t.compareIndexKeyColumn(rowGetCI(ri, c), rowGetCI(rj, c), c)
			if cmp < 0 {
				return true
			}
			if cmp > 0 {
				return false
			}
		}
		for _, c := range pkCols {
			cmp := t.compareIndexKeyColumn(rowGetCI(ri, c), rowGetCI(rj, c), c)
			if cmp < 0 {
				return true
			}
			if cmp > 0 {
				return false
			}
		}
		return false
	})
	return out
}

// compareIndexKeyColumn compares two index-key values the way InnoDB orders
// B-tree entries: numeric types by value, string types by table/column collation.
func (t *Table) compareIndexKeyColumn(a, b interface{}, colName string) int {
	if t.Def != nil && !hasNonSortableCharset(t.Def.Charset) &&
		isStringIndexColumnType(columnTypeForName(t.Def, colName)) {
		return compareRowValueWithCollation(a, b, effectivePKCollation(t.Def, colName))
	}
	return compareRowValue(a, b)
}

func columnTypeForName(def *catalog.TableDef, colName string) string {
	if def == nil {
		return ""
	}
	for _, col := range def.Columns {
		if strings.EqualFold(col.Name, colName) {
			return col.Type
		}
	}
	return ""
}

func isStringIndexColumnType(colType string) bool {
	upper := strings.ToUpper(colType)
	if idx := strings.Index(upper, " GENERATED"); idx >= 0 {
		upper = upper[:idx]
	}
	return strings.HasPrefix(upper, "CHAR") || strings.HasPrefix(upper, "VARCHAR") ||
		strings.HasPrefix(upper, "TEXT")
}

// inRange reports whether v lies within the lower/upper bounds for the
// range column at idxPos. nil bounds mean unbounded on that side.
func inRange(v interface{}, lower, upper []interface{}, idxPos int, lowerInclusive, upperInclusive bool) bool {
	if idxPos < len(lower) && lower[idxPos] != nil {
		cmp := compareRowValue(v, lower[idxPos])
		if cmp < 0 {
			return false
		}
		if cmp == 0 && !lowerInclusive {
			return false
		}
	}
	if idxPos < len(upper) && upper[idxPos] != nil {
		cmp := compareRowValue(v, upper[idxPos])
		if cmp > 0 {
			return false
		}
		if cmp == 0 && !upperInclusive {
			return false
		}
	}
	return true
}

// cloneRowSnapshot returns a shallow copy of the given row.
func cloneRowSnapshot(row Row) Row {
	out := make(Row, len(row))
	for k, v := range row {
		out[k] = v
	}
	return out
}

// FindIndex returns the IndexDef for the given name (PRIMARY-aware). The
// second return is false when the index does not exist on this table.
//
// Exposed so the executor can pre-resolve indexes when building an
// IndexAccessSpec without re-walking the catalog.
func (t *Table) FindIndex(name string) (catalog.IndexDef, bool) {
	if t == nil || t.Def == nil {
		return catalog.IndexDef{}, false
	}
	if strings.EqualFold(name, "PRIMARY") {
		if len(t.Def.PrimaryKey) == 0 {
			return catalog.IndexDef{}, false
		}
		return catalog.IndexDef{
			Name:    "PRIMARY",
			Columns: append([]string(nil), t.Def.PrimaryKey...),
			Unique:  true,
		}, true
	}
	for _, idx := range t.Def.Indexes {
		if strings.EqualFold(idx.Name, name) {
			return idx, true
		}
	}
	return catalog.IndexDef{}, false
}
