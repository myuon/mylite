package executor

import (
	"strconv"
	"strings"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
	"vitess.io/vitess/go/vt/sqlparser"
)

// buildIndexAccessSpec derives a storage.IndexAccessSpec for tableName
// from the given SELECT's WHERE clause. The returned spec mirrors the
// AccessPath that explainDetectAccessType would compute, but with the
// concrete key VALUES extracted from WHERE so the storage layer can
// perform the actual lookup.
//
// The second return is false when no usable index was found (caller
// should fall back to a full Scan). A returned spec with Type=="ALL"
// likewise signals the fallback path.
//
// This function is intentionally conservative — it only handles cases
// the existing planner already classifies as const / ref / range.
// Anything more elaborate (ref_or_null, index_merge, ICP, etc.)
// returns ok=false so the caller stays on the legacy scan-and-filter
// path until those cases are integrated end-to-end.
func (e *Executor) buildIndexAccessSpec(sel *sqlparser.Select, tableName string) (storage.IndexAccessSpec, bool) {
	if sel == nil {
		return storage.IndexAccessSpec{}, false
	}
	td := e.explainGetTableDef(tableName)
	if td == nil {
		return storage.IndexAccessSpec{}, false
	}

	ai := e.explainDetectAccessType(sel, tableName)
	at := strings.ToLower(ai.accessType)
	keyName := stringify(ai.key)
	// Full covering-index scan ("Using index" with no WHERE filter): the
	// planner has already verified the chosen index covers the SELECT
	// projection. We just need to plumb it through to storage so the
	// scan returns rows in index order.
	if at == "index" && keyName != "" {
		return storage.IndexAccessSpec{
			Type:      "index",
			IndexName: keyName,
		}, true
	}
	if sel.Where == nil {
		return storage.IndexAccessSpec{}, false
	}
	if keyName == "" || at == "" || at == "all" || at == "index" {
		return storage.IndexAccessSpec{}, false
	}

	// Resolve which catalog index was chosen.
	var idxCols []string
	if strings.EqualFold(keyName, "PRIMARY") {
		if len(td.PrimaryKey) == 0 {
			return storage.IndexAccessSpec{}, false
		}
		idxCols = append([]string(nil), td.PrimaryKey...)
	} else {
		var found catalog.IndexDef
		ok := false
		for _, idx := range td.Indexes {
			if strings.EqualFold(idx.Name, keyName) {
				found = idx
				ok = true
				break
			}
		}
		if !ok {
			return storage.IndexAccessSpec{}, false
		}
		idxCols = append([]string(nil), found.Columns...)
	}

	// Map column → list of binding expressions for this column from the WHERE clause.
	eqValues := map[string][]sqlparser.Expr{}
	rangeValues := map[string]struct{ lower, upper sqlparser.Expr }{}
	betweenValues := map[string]struct{ lower, upper sqlparser.Expr }{}
	inLists := map[string][]sqlparser.Expr{}

	collectBindings(sel.Where.Expr, tableName, eqValues, rangeValues, betweenValues, inLists)

	// Try to assemble per-column equality values for all idxCols.
	values := make([]interface{}, len(idxCols))
	allHaveEq := true
	for i, col := range idxCols {
		bare := stripIndexPrefixLength(col)
		bindings := eqValues[strings.ToLower(bare)]
		if len(bindings) == 0 {
			allHaveEq = false
			break
		}
		v, ok := evalLiteralExpr(bindings[0])
		if !ok {
			allHaveEq = false
			break
		}
		values[i] = v
	}

	switch at {
	case "const", "eq_ref":
		if !allHaveEq {
			return storage.IndexAccessSpec{}, false
		}
		return storage.IndexAccessSpec{
			Type:           ai.accessType,
			IndexName:      keyName,
			EqualityValues: values,
		}, true

	case "ref":
		// For ref access, we need at least the leading equality column(s)
		// to be matched. Build whatever prefix is available.
		eqPrefix := []interface{}{}
		for _, col := range idxCols {
			bare := stripIndexPrefixLength(col)
			bindings := eqValues[strings.ToLower(bare)]
			if len(bindings) == 0 {
				break
			}
			v, ok := evalLiteralExpr(bindings[0])
			if !ok {
				break
			}
			eqPrefix = append(eqPrefix, v)
		}
		if len(eqPrefix) == 0 {
			return storage.IndexAccessSpec{}, false
		}
		return storage.IndexAccessSpec{
			Type:           "ref",
			IndexName:      keyName,
			EqualityValues: eqPrefix,
		}, true

	case "range":
		// Equality prefix + final-column range (or IN list).
		// IN-list range: build full-tuple lookups.
		firstIdxCol := stripIndexPrefixLength(idxCols[0])
		if inExprs, ok := inLists[strings.ToLower(firstIdxCol)]; ok && len(inExprs) > 0 && len(idxCols) == 1 {
			tuples := make([][]interface{}, 0, len(inExprs))
			allOK := true
			for _, ex := range inExprs {
				v, ok := evalLiteralExpr(ex)
				if !ok {
					allOK = false
					break
				}
				tuples = append(tuples, []interface{}{v})
			}
			if allOK {
				return storage.IndexAccessSpec{
					Type:      "range",
					IndexName: keyName,
					InValues:  tuples,
				}, true
			}
		}

		eqPrefix := []interface{}{}
		var rangeIdxPos int
		for i, col := range idxCols {
			bare := strings.ToLower(stripIndexPrefixLength(col))
			bindings := eqValues[bare]
			if len(bindings) == 0 {
				rangeIdxPos = i
				break
			}
			v, ok := evalLiteralExpr(bindings[0])
			if !ok {
				return storage.IndexAccessSpec{}, false
			}
			eqPrefix = append(eqPrefix, v)
			rangeIdxPos = i + 1
		}
		if rangeIdxPos >= len(idxCols) {
			// No range column left — treat as ref.
			return storage.IndexAccessSpec{
				Type:           "ref",
				IndexName:      keyName,
				EqualityValues: eqPrefix,
			}, true
		}
		rangeCol := strings.ToLower(stripIndexPrefixLength(idxCols[rangeIdxPos]))

		// BETWEEN takes precedence over individual >= / <= bindings.
		lower := make([]interface{}, len(idxCols))
		upper := make([]interface{}, len(idxCols))
		lowerInclusive, upperInclusive := true, true
		if bt, ok := betweenValues[rangeCol]; ok {
			lv, lok := evalLiteralExpr(bt.lower)
			uv, uok := evalLiteralExpr(bt.upper)
			if lok && uok {
				lower[rangeIdxPos] = lv
				upper[rangeIdxPos] = uv
				return storage.IndexAccessSpec{
					Type:                "range",
					IndexName:           keyName,
					EqualityValues:      eqPrefix,
					RangeLower:          lower,
					RangeUpper:          upper,
					RangeLowerInclusive: true,
					RangeUpperInclusive: true,
				}, true
			}
		}
		if rg, ok := rangeValues[rangeCol]; ok {
			haveAny := false
			if rg.lower != nil {
				if v, ok := evalLiteralExpr(rg.lower); ok {
					lower[rangeIdxPos] = v
					haveAny = true
				}
			}
			if rg.upper != nil {
				if v, ok := evalLiteralExpr(rg.upper); ok {
					upper[rangeIdxPos] = v
					haveAny = true
				}
			}
			if haveAny {
				return storage.IndexAccessSpec{
					Type:                "range",
					IndexName:           keyName,
					EqualityValues:      eqPrefix,
					RangeLower:          lower,
					RangeUpper:          upper,
					RangeLowerInclusive: lowerInclusive,
					RangeUpperInclusive: upperInclusive,
				}, true
			}
		}
	}

	return storage.IndexAccessSpec{}, false
}

// indexColumnsAreIntegerTyped reports whether every column of the named
// index on td is declared with an integer-family type (TINYINT / SMALLINT
// / MEDIUMINT / INT / BIGINT, signed or unsigned). String / decimal /
// date columns return false so the caller can keep them on the legacy
// scan-and-filter path where executor.compareValues honours collation
// and decimal precision.
//
// This is the conservative gate used to widen ref-access activation
// without re-implementing executor's full comparison semantics in
// storage.valuesEqualLoose.
func indexColumnsAreIntegerTyped(td *catalog.TableDef, indexName string) bool {
	if td == nil {
		return false
	}
	var cols []string
	if strings.EqualFold(indexName, "PRIMARY") {
		cols = td.PrimaryKey
	} else {
		for _, idx := range td.Indexes {
			if strings.EqualFold(idx.Name, indexName) {
				cols = idx.Columns
				break
			}
		}
	}
	if len(cols) == 0 {
		return false
	}
	for _, c := range cols {
		bare := stripIndexPrefixLength(c)
		var coldef *catalog.ColumnDef
		for i := range td.Columns {
			if strings.EqualFold(td.Columns[i].Name, bare) {
				coldef = &td.Columns[i]
				break
			}
		}
		if coldef == nil {
			return false
		}
		if !isIntegerColumnType(coldef.Type) {
			return false
		}
	}
	return true
}

// isIntegerColumnType reports whether the supplied MySQL type string is
// one of the integer family types — case-insensitive, ignoring trailing
// "(N)" display widths and "UNSIGNED"/"ZEROFILL" qualifiers.
func isIntegerColumnType(typ string) bool {
	t := strings.ToUpper(strings.TrimSpace(typ))
	if i := strings.Index(t, "("); i >= 0 {
		t = t[:i]
	}
	t = strings.TrimSpace(t)
	switch t {
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT", "BOOL", "BOOLEAN":
		return true
	}
	return false
}

// stripIndexPrefixLength strips the prefix-length suffix from an index
// column name (e.g., "name(20)" -> "name"). Mirrors storage's helper
// so the executor doesn't need to import the storage internal helper.
func stripIndexPrefixLength(col string) string {
	if i := strings.Index(col, "("); i >= 0 {
		return col[:i]
	}
	return col
}

// collectBindings walks a WHERE expression and records, for each column
// belonging to tableName, the literal expressions bound by =, IN,
// BETWEEN, and range comparisons. Used by buildIndexAccessSpec to plumb
// concrete values through to storage.IndexAccessSpec.
func collectBindings(
	expr sqlparser.Expr,
	tableName string,
	eq map[string][]sqlparser.Expr,
	rng map[string]struct{ lower, upper sqlparser.Expr },
	bt map[string]struct{ lower, upper sqlparser.Expr },
	inL map[string][]sqlparser.Expr,
) {
	if expr == nil {
		return
	}
	switch x := expr.(type) {
	case *sqlparser.AndExpr:
		collectBindings(x.Left, tableName, eq, rng, bt, inL)
		collectBindings(x.Right, tableName, eq, rng, bt, inL)
	case *sqlparser.ComparisonExpr:
		colName, valExpr, swapped := extractColAndValue(x.Left, x.Right, tableName)
		if colName == "" {
			return
		}
		key := strings.ToLower(colName)
		switch x.Operator {
		case sqlparser.EqualOp, sqlparser.NullSafeEqualOp:
			eq[key] = append(eq[key], valExpr)
		case sqlparser.InOp:
			if vt, ok := x.Right.(sqlparser.ValTuple); ok {
				for _, ve := range vt {
					inL[key] = append(inL[key], ve)
				}
			}
		case sqlparser.GreaterThanOp, sqlparser.GreaterEqualOp:
			cur := rng[key]
			if !swapped {
				cur.lower = valExpr
			} else {
				cur.upper = valExpr
			}
			rng[key] = cur
		case sqlparser.LessThanOp, sqlparser.LessEqualOp:
			cur := rng[key]
			if !swapped {
				cur.upper = valExpr
			} else {
				cur.lower = valExpr
			}
			rng[key] = cur
		}
	case *sqlparser.BetweenExpr:
		col, ok := x.Left.(*sqlparser.ColName)
		if !ok {
			return
		}
		qual := col.Qualifier.Name.String()
		if qual != "" && !strings.EqualFold(qual, tableName) {
			return
		}
		key := strings.ToLower(col.Name.String())
		bt[key] = struct{ lower, upper sqlparser.Expr }{lower: x.From, upper: x.To}
	}
}

// extractColAndValue returns the column-name + literal-expr pair from a
// binary comparison's two sides, plus a "swapped" bool that's true when
// the column was on the right (so range comparators can be flipped).
func extractColAndValue(left, right sqlparser.Expr, tableName string) (string, sqlparser.Expr, bool) {
	if col, ok := left.(*sqlparser.ColName); ok {
		qual := col.Qualifier.Name.String()
		if qual == "" || strings.EqualFold(qual, tableName) {
			return col.Name.String(), right, false
		}
	}
	if col, ok := right.(*sqlparser.ColName); ok {
		qual := col.Qualifier.Name.String()
		if qual == "" || strings.EqualFold(qual, tableName) {
			return col.Name.String(), left, true
		}
	}
	return "", nil, false
}

// evalLiteralExpr extracts the Go-typed value of a literal expression
// (Literal, NullVal, BoolVal). Returns ok=false for anything else
// (column refs, expressions, subqueries) so the optimizer drops back
// to the full-scan path.
func evalLiteralExpr(expr sqlparser.Expr) (interface{}, bool) {
	switch x := expr.(type) {
	case *sqlparser.Literal:
		return literalGoValue(x), true
	case *sqlparser.NullVal:
		return nil, true
	case sqlparser.BoolVal:
		if bool(x) {
			return int64(1), true
		}
		return int64(0), true
	case *sqlparser.UnaryExpr:
		// Handle negative literals: -42, -3.14
		if x.Operator == sqlparser.UMinusOp {
			if inner, ok := evalLiteralExpr(x.Expr); ok {
				switch v := inner.(type) {
				case int64:
					return -v, true
				case float64:
					return -v, true
				case string:
					// keep as-is; storage compares numerically when possible
					return "-" + v, true
				}
			}
		}
	}
	return nil, false
}

// scanTableForFrom is the entry point used by buildFromExpr to read all
// rows for a base table. When the executor has registered an
// IndexAccessSpec for the given alias / table, this routes through
// Table.ScanByIndex (so the read mirrors the planner's chosen access
// path); otherwise it falls back to Table.Scan() — preserving the
// pre-#263 behaviour bit-for-bit.
//
// Lookup precedence:
//  1. spec keyed by lowercased alias
//  2. spec keyed by lowercased table name
//
// Falls back to Scan() if the executor has no access specs registered
// or the spec describes a full scan.
func (e *Executor) scanTableForFrom(tbl *storage.Table, alias, tableName string) []storage.Row {
	if !e.useIndexAccessSpecs || len(e.indexAccessSpecs) == 0 || tbl == nil {
		return tbl.Scan()
	}
	var spec storage.IndexAccessSpec
	found := false
	if alias != "" {
		if s, ok := e.indexAccessSpecs[strings.ToLower(alias)]; ok {
			spec = s
			found = true
		}
	}
	if !found && tableName != "" {
		if s, ok := e.indexAccessSpecs[strings.ToLower(tableName)]; ok {
			spec = s
			found = true
		}
	}
	if !found {
		return tbl.Scan()
	}
	rows, err := tbl.ScanByIndex(spec)
	if err != nil {
		return tbl.Scan()
	}
	return rows
}

// installIndexAccessSpecForSelect inspects the SELECT to decide whether the
// new index-based execution path can be safely engaged for it. When all
// guard conditions are met it builds a storage.IndexAccessSpec from the
// WHERE clause (via buildIndexAccessSpec) and stores it under the table's
// alias and bare-table name in e.indexAccessSpecs, then enables the gate.
//
// The function returns a restore closure the caller must defer to put the
// previous indexAccessSpecs / useIndexAccessSpecs state back before
// returning to its parent. This keeps subquery-driven nested execSelect
// calls isolated from each other.
//
// Guard conditions (intentionally narrow — see issue #263 follow-up):
//   - exactly one FROM expression (no JOINs, no implicit cross joins)
//   - the FROM is an AliasedTableExpr whose Expr is a plain TableName
//     (not a DerivedTable / subquery / parenthesised join)
//   - the WHERE clause contains no Subquery (correlated or otherwise)
//   - the planner-selected AccessPath is "const" (single-row PK lookup) —
//     "ref" / "range" are deferred to follow-ups
func (e *Executor) installIndexAccessSpecForSelect(sel *sqlparser.Select) func() {
	prevSpecs := e.indexAccessSpecs
	prevGate := e.useIndexAccessSpecs
	restore := func() {
		e.indexAccessSpecs = prevSpecs
		e.useIndexAccessSpecs = prevGate
	}
	if sel == nil || len(sel.From) != 1 {
		return restore
	}
	ate, ok := sel.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return restore
	}
	tn, ok := ate.Expr.(sqlparser.TableName)
	if !ok {
		return restore
	}
	tableName := tn.Name.String()
	if tableName == "" || strings.EqualFold(tableName, "dual") {
		return restore
	}
	// Decline information_schema / cross-database tables: their reads go
	// through buildInformationSchemaRows and never reach scanTableForFrom.
	if !tn.Qualifier.IsEmpty() {
		q := tn.Qualifier.String()
		if strings.EqualFold(q, "information_schema") ||
			strings.EqualFold(q, "performance_schema") ||
			strings.EqualFold(q, "sys") ||
			strings.EqualFold(q, "mysql") {
			return restore
		}
	}
	// CTE-resolved names also bypass scanTableForFrom.
	if e.cteMap != nil {
		if _, ok := e.cteMap[tableName]; ok {
			return restore
		}
	}
	if sel.Where != nil && exprHasSubquery(sel.Where.Expr) {
		return restore
	}
	// The covering-index "index" path also requires no GROUP BY / HAVING /
	// ORDER BY / DISTINCT / LIMIT — those would either re-order the
	// result or imply additional access semantics we have not validated.
	hasNoWhere := sel.Where == nil
	if hasNoWhere {
		if sel.GroupBy != nil || sel.Having != nil || sel.OrderBy != nil ||
			sel.Distinct || sel.Limit != nil {
			return restore
		}
	}
	spec, ok := e.buildIndexAccessSpec(sel, tableName)
	if !ok {
		return restore
	}
	// Narrow scope: flip the gate for "const" / "eq_ref" / "ref" / "index" only.
	// "range" remains on the legacy scan path. "index" is the
	// covering-index full-scan case; it must come from a no-WHERE SELECT
	// per the buildIndexAccessSpec contract.
	//
	// "ref" widening (Refs #263 follow-up to #304): single-table SELECTs
	// whose WHERE resolves to a non-unique-index equality lookup now
	// route through Table.ScanByIndex. The same single-table /
	// no-subquery / no-CTE / no-info-schema guard rails as const apply.
	//
	// Additionally, "ref" is restricted to integer-typed index columns:
	// storage.valuesEqualLoose does case-sensitive byte comparison for
	// strings, but executor's compareValues honours collation (varchar
	// columns are typically case-insensitive in MySQL). Restricting to
	// integer columns keeps the new path correct without re-implementing
	// the executor's collation logic in storage.
	t := strings.ToLower(spec.Type)
	if t != "const" && t != "eq_ref" && t != "ref" && t != "index" {
		return restore
	}
	if t == "index" && !hasNoWhere {
		return restore
	}
	if t == "ref" && !indexColumnsAreIntegerTyped(e.explainGetTableDef(tableName), spec.IndexName) {
		return restore
	}
	alias, _, _ := extractTableAliasFromAliased(ate)
	newSpecs := make(map[string]storage.IndexAccessSpec, 2)
	if alias != "" {
		newSpecs[strings.ToLower(alias)] = spec
	}
	newSpecs[strings.ToLower(tableName)] = spec
	e.indexAccessSpecs = newSpecs
	e.useIndexAccessSpecs = true
	return restore
}

// exprHasSubquery reports whether expr contains any subquery node, so the
// caller can refuse to engage the index access path when the WHERE clause
// has correlated / IN-subquery / EXISTS predicates that the new path
// hasn't been validated against.
func exprHasSubquery(expr sqlparser.Expr) bool {
	if expr == nil {
		return false
	}
	found := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch node.(type) {
		case *sqlparser.Subquery, *sqlparser.ExistsExpr:
			found = true
			return false, nil
		}
		return true, nil
	}, expr)
	return found
}

// literalGoValue converts a sqlparser.Literal into an int64/float64/string
// matching the broad semantics of how rows store values.
func literalGoValue(lit *sqlparser.Literal) interface{} {
	if lit == nil {
		return nil
	}
	switch lit.Type {
	case sqlparser.IntVal:
		// Best-effort int64 parse, fall back to string.
		if v, err := strconv.ParseInt(string(lit.Val), 10, 64); err == nil {
			return v
		}
		return string(lit.Val)
	case sqlparser.FloatVal, sqlparser.DecimalVal:
		if v, err := strconv.ParseFloat(string(lit.Val), 64); err == nil {
			return v
		}
		return string(lit.Val)
	default:
		return string(lit.Val)
	}
}
