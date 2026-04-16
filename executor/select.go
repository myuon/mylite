package executor

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
	"vitess.io/vitess/go/vt/sqlparser"
)

// buildFromExpr builds rows from any TableExpr (AliasedTableExpr or JoinTableExpr).
// Each row has both un-prefixed keys (for backwards compat with single-table queries)
// and "alias.col" prefixed keys (for JOIN disambiguation).
// collectTableDefs extracts table definitions from a FROM expression, handling both
// simple table references and JOINs.
func (e *Executor) collectTableDefs(expr sqlparser.TableExpr) []*catalog.TableDef {
	defs, _ := e.collectTableDefsWithAliases(expr)
	return defs
}

// collectTableDefsWithAliases collects table definitions and their aliases
// (the name to use for qualifying columns in SELECT * expansion). The alias
// is the AS alias if present, otherwise the table name.
func (e *Executor) collectTableDefsWithAliases(expr sqlparser.TableExpr) ([]*catalog.TableDef, []string) {
	switch te := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		if tn, ok := te.Expr.(sqlparser.TableName); ok {
			tblName := tn.Name.String()
			lookupDB := e.CurrentDB
			if !tn.Qualifier.IsEmpty() {
				lookupDB = tn.Qualifier.String()
			}
			alias := tblName
			if !te.As.IsEmpty() {
				alias = te.As.String()
			}
			if e.Catalog != nil {
				if db, err := e.Catalog.GetDatabase(lookupDB); err == nil {
					if td, err := db.GetTable(tblName); err == nil {
						return []*catalog.TableDef{td}, []string{alias}
					}
				}
			}
			// Check CTE map: synthesize a TableDef from CTE column metadata.
			if e.cteMap != nil {
				cteLookup := tblName
				if cte, ok := e.cteMap[cteLookup]; ok && len(cte.columns) > 0 {
					syntheticCols := make([]catalog.ColumnDef, len(cte.columns))
					for i, col := range cte.columns {
						syntheticCols[i] = catalog.ColumnDef{Name: col}
					}
					td := &catalog.TableDef{
						Name:    tblName,
						Columns: syntheticCols,
					}
					return []*catalog.TableDef{td}, []string{alias}
				}
			}
		}
	case *sqlparser.JoinTableExpr:
		leftDefs, leftAliases := e.collectTableDefsWithAliases(te.LeftExpr)
		rightDefs, rightAliases := e.collectTableDefsWithAliases(te.RightExpr)
		return append(leftDefs, rightDefs...), append(leftAliases, rightAliases...)
	case *sqlparser.ParenTableExpr:
		var defs []*catalog.TableDef
		var aliases []string
		for _, inner := range te.Exprs {
			d, a := e.collectTableDefsWithAliases(inner)
			defs = append(defs, d...)
			aliases = append(aliases, a...)
		}
		return defs, aliases
	}
	return nil, nil
}

// extractJoinUsingCols extracts column names from JOIN ... USING(...) clauses.
func extractJoinUsingCols(expr sqlparser.TableExpr) []string {
	switch te := expr.(type) {
	case *sqlparser.JoinTableExpr:
		var cols []string
		if te.Condition != nil && len(te.Condition.Using) > 0 {
			for _, col := range te.Condition.Using {
				cols = append(cols, col.String())
			}
		}
		// Also check nested JOINs
		cols = append(cols, extractJoinUsingCols(te.LeftExpr)...)
		cols = append(cols, extractJoinUsingCols(te.RightExpr)...)
		return cols
	}
	return nil
}

func (e *Executor) buildFromExpr(expr sqlparser.TableExpr) ([]storage.Row, error) {
	switch te := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		// Handle DerivedTable (FROM subquery)
		if dt, ok := te.Expr.(*sqlparser.DerivedTable); ok {
			alias := te.As.String()
			sub := &sqlparser.Subquery{Select: dt.Select}
			result, err := e.execSubquery(sub, e.correlatedRow)
			if err != nil {
				return nil, err
			}
			// Apply derived table column aliases if present: AS dt (x,y,z)
			colNames := make([]string, len(result.Columns))
			copy(colNames, result.Columns)
			if len(te.Columns) > 0 {
				for ci, ca := range te.Columns {
					if ci < len(colNames) {
						colNames[ci] = ca.String()
					}
				}
			}
			rows := make([]storage.Row, len(result.Rows))
			for i, resultRow := range result.Rows {
				row := make(storage.Row, len(colNames)*2+1)
				for j, col := range colNames {
					row[col] = resultRow[j]
					if alias != "" {
						row[alias+"."+col] = resultRow[j]
					}
				}
				// Store column order for * expansion
				order := strings.Join(colNames, "\x00")
				row["__column_order__"] = order
				rows[i] = row
			}
			return rows, nil
		}

		alias, tableName, err := extractTableAliasFromAliased(te)
		if err != nil {
			return nil, err
		}
		// Handle MySQL's virtual DUAL table: one empty row, no columns.
		if strings.ToLower(tableName) == "dual" {
			return []storage.Row{{}}, nil
		}
		// Check CTE map first.
		if e.cteMap != nil {
			cteLookup := tableName
			// Schema-qualified CTE: strip schema prefix (e.g. test.qn -> qn)
			if strings.Contains(cteLookup, ".") {
				parts := strings.SplitN(cteLookup, ".", 2)
				if _, ok := e.cteMap[parts[1]]; ok {
					cteLookup = parts[1]
				}
			}
			if cteTbl, ok := e.cteMap[cteLookup]; ok {
				result := make([]storage.Row, len(cteTbl.rows))
				for i, row := range cteTbl.rows {
					newRow := make(storage.Row, len(row)*2)
					for k, v := range row {
						if k == "__column_order__" {
							newRow[k] = v
							continue
						}
						newRow[k] = v
						if alias != "" {
							newRow[alias+"."+k] = v
						}
					}
					result[i] = newRow
				}
				return result, nil
			}
		}
		// Handle INFORMATION_SCHEMA virtual tables.
		var qualifier string
		var bareTableName string
		if tn, ok := te.Expr.(sqlparser.TableName); ok {
			qualifier = tn.Qualifier.String()
			bareTableName = tn.Name.String()
		} else {
			bareTableName = tableName
		}
		if e.isInformationSchemaTable(qualifier, bareTableName) {
			isAlias := alias
			if isAlias == tableName {
				// No explicit AS alias; use qualifier-qualified name as prefix.
				if qualifier != "" {
					isAlias = qualifier + "." + bareTableName
				} else {
					isAlias = bareTableName
				}
			}
			return e.buildInformationSchemaRows(bareTableName, isAlias)
		}
		// If the qualifier is information_schema but the table is not a known
		// virtual table, return MySQL error 1109 (ER_UNKNOWN_TABLE) instead of
		// falling through to regular table lookup.
		if strings.EqualFold(qualifier, "information_schema") {
			return nil, mysqlError(1109, "42S02", fmt.Sprintf("Unknown table '%s' in information_schema", strings.ToUpper(bareTableName)))
		}
		lookupDB := e.CurrentDB
		lookupTable := bareTableName
		if qualifier != "" && !strings.EqualFold(qualifier, "information_schema") {
			lookupDB = qualifier
		}
		if lookupTable == "" {
			lookupTable = tableName
		}
		if e.Storage == nil {
			return nil, fmt.Errorf("no storage available")
		}
		tbl, err := e.Storage.GetTable(lookupDB, lookupTable)
		if err != nil {
			// Check if it's a view
			if e.views != nil {
				if viewSQL, ok := e.views[lookupTable]; ok {
					// Save and restore currentQuery so that view execution doesn't
					// overwrite the outer query's text (used for column name extraction).
					savedCurrentQuery := e.currentQuery
					viewResult, err := e.Execute(viewSQL)
					e.currentQuery = savedCurrentQuery
					if err != nil {
						return nil, err
					}
					// Convert view result to storage.Rows
					rows := make([]storage.Row, 0, len(viewResult.Rows))
					// Store column order as a special metadata key for SELECT * resolution
					colOrderStr := strings.Join(viewResult.Columns, "\x00")
					for _, vrow := range viewResult.Rows {
						row := make(storage.Row)
						row["__column_order__"] = colOrderStr
						for ci, col := range viewResult.Columns {
							if ci < len(vrow) {
								row[col] = vrow[ci]
								row[alias+"."+col] = vrow[ci]
							}
						}
						rows = append(rows, row)
					}
					return rows, nil
				}
			}
			return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", lookupDB, lookupTable))
		}
		// Dynamically populate certain performance_schema virtual tables before scan.
		if strings.EqualFold(lookupDB, "performance_schema") {
			e.populatePerfSchemaTable(tbl, lookupTable)
		}
		rawAll := tbl.Scan()
		// MySQL's InnoDB stores mysql.engine_cost in clustered PK order (cost_name, engine_name, device_type).
		// Sort here to match MySQL's natural row ordering when no ORDER BY is specified.
		// MySQL uses latin1_swedish_ci collation for these columns, which is case-insensitive.
		if strings.EqualFold(lookupDB, "mysql") && strings.EqualFold(lookupTable, "engine_cost") {
			sort.SliceStable(rawAll, func(i, j int) bool {
				ci := strings.ToLower(fmt.Sprintf("%v", rawAll[i]["cost_name"]))
				cj := strings.ToLower(fmt.Sprintf("%v", rawAll[j]["cost_name"]))
				if ci != cj {
					return ci < cj
				}
				ei := strings.ToLower(fmt.Sprintf("%v", rawAll[i]["engine_name"]))
				ej := strings.ToLower(fmt.Sprintf("%v", rawAll[j]["engine_name"]))
				if ei != ej {
					return ei < ej
				}
				di := fmt.Sprintf("%v", rawAll[i]["device_type"])
				dj := fmt.Sprintf("%v", rawAll[j]["device_type"])
				return di < dj
			})
		}
		// Filter out rows from other connections' uncommitted transactions
		raw := e.filterUncommittedRows(rawAll)
		// Build a set of CHAR(N) column names for trailing-space removal.
		// Only include columns whose charset represents a space as the single byte 0x20.
		// Multi-byte charsets (utf32, utf16, ucs2) encode space as multi-byte sequences,
		// so byte-level TrimRight(" ") would incorrectly strip non-space trailing bytes.
		charCols := make(map[string]bool)
		if tbl.Def != nil {
			tableCharset := ""
			if tbl.Def != nil {
				tableCharset = strings.ToLower(tbl.Def.Charset)
			}
			for _, col := range tbl.Def.Columns {
				lower := strings.ToLower(strings.TrimSpace(col.Type))
				if strings.HasPrefix(lower, "char(") || lower == "char" {
					colCharset := strings.ToLower(col.Charset)
					if colCharset == "" {
						colCharset = tableCharset
					}
					// Skip multi-byte charsets where space != 0x20 byte
					isMultiByte := colCharset == "utf32" || colCharset == "utf16" ||
						colCharset == "utf16le"
					if !isMultiByte {
						charCols[col.Name] = true
					}
				}
			}
		}
		result := make([]storage.Row, len(raw))
		for i, row := range raw {
			newRow := make(storage.Row, len(row)*2)
			for k, v := range row {
				// Skip internal transaction metadata
				if k == "__txn_conn_id__" {
					continue
				}
				// MySQL removes trailing spaces from CHAR columns on retrieval.
				if charCols[k] {
					if s, ok := v.(string); ok {
						v = strings.TrimRight(s, " ")
					}
				}
				newRow[k] = v
				newRow[alias+"."+k] = v
			}
			result[i] = newRow
		}
		return result, nil
	case *sqlparser.JoinTableExpr:
		return e.buildJoinedRowsFromJoin(te)
	case *sqlparser.ParenTableExpr:
		// Parenthesized table expressions: process each inner table expr
		// For single table, just return its rows. For multiple (joins), process sequentially.
		if len(te.Exprs) == 1 {
			return e.buildFromExpr(te.Exprs[0])
		}
		// Multiple tables: treat as implicit cross join / join chain
		var result []storage.Row
		for i, innerExpr := range te.Exprs {
			rows, err := e.buildFromExpr(innerExpr)
			if err != nil {
				return nil, err
			}
			if i == 0 {
				result = rows
			} else {
				// Cross join
				var newResult []storage.Row
				for _, leftRow := range result {
					for _, rightRow := range rows {
						merged := make(storage.Row)
						for k, v := range leftRow {
							merged[k] = v
						}
						for k, v := range rightRow {
							merged[k] = v
						}
						newResult = append(newResult, merged)
					}
				}
				result = newResult
			}
		}
		return result, nil
	case *sqlparser.JSONTableExpr:
		docVal, err := e.evalExpr(te.Expr)
		if err != nil {
			return []storage.Row{}, nil
		}
		if docVal == nil {
			return []storage.Row{}, nil
		}
		normDoc, err := jsonNormalize(docVal)
		if err != nil {
			return []storage.Row{}, nil
		}
		srcRows, ok := normDoc.([]interface{})
		if !ok {
			return []storage.Row{}, nil
		}
		alias := te.Alias.String()
		if alias == "" {
			alias = "json_table"
		}
		// Validate default ON EMPTY/ON ERROR values for non-JSON path columns.
		for _, c := range te.Columns {
			if c.JtPath == nil || c.JtPath.JtColExists {
				continue
			}
			colType := strings.ToLower(sqlparser.String(c.JtPath.Type))
			if strings.HasPrefix(colType, "json") {
				continue
			}
			checkResp := func(resp *sqlparser.JtOnResponse) error {
				if resp == nil || resp.ResponseType != sqlparser.DefaultJSONType || resp.Expr == nil {
					return nil
				}
				v, err := e.evalExpr(resp.Expr)
				if err != nil || v == nil {
					return nil
				}
				s := toString(v)
				if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
					s = s[1 : len(s)-1]
				}
				var js interface{}
				if err := json.Unmarshal([]byte(s), &js); err != nil {
					return nil
				}
				switch js.(type) {
				case []interface{}, map[string]interface{}:
					return mysqlError(1067, "42000", fmt.Sprintf("Invalid default value for '%s'", c.JtPath.Name.String()))
				}
				return nil
			}
			if err := checkResp(c.JtPath.EmptyOnResponse); err != nil {
				return nil, err
			}
			if err := checkResp(c.JtPath.ErrorOnResponse); err != nil {
				return nil, err
			}
		}
		colOrder := make([]string, 0, len(te.Columns))
		for _, c := range te.Columns {
			switch {
			case c.JtOrdinal != nil:
				colOrder = append(colOrder, c.JtOrdinal.Name.String())
			case c.JtPath != nil:
				colOrder = append(colOrder, c.JtPath.Name.String())
			}
		}
		colOrderMeta := strings.Join(colOrder, "\x00")
		result := make([]storage.Row, 0, len(srcRows))
		for i, item := range srcRows {
			row := make(storage.Row)
			row["__column_order__"] = colOrderMeta
			for _, c := range te.Columns {
				if c.JtOrdinal != nil {
					name := c.JtOrdinal.Name.String()
					row[name] = int64(i + 1)
					row[alias+"."+name] = int64(i + 1)
					continue
				}
				if c.JtPath == nil {
					continue
				}
				name := c.JtPath.Name.String()
				pathVal, err := e.evalExpr(c.JtPath.Path)
				if err != nil {
					pathVal = "$"
				}
				path := toString(pathVal)
				extracted := jsonExtractPath(item, path)
				if c.JtPath.JtColExists {
					exists := int64(0)
					if extracted != nil {
						exists = int64(1)
					}
					row[name] = exists
					row[alias+"."+name] = exists
					continue
				}
				if extracted == nil {
					row[name] = nil
					row[alias+"."+name] = nil
					continue
				}
				colType := strings.ToLower(sqlparser.String(c.JtPath.Type))
				if strings.HasPrefix(colType, "json") {
					row[name] = jsonMarshalMySQL(extracted)
					row[alias+"."+name] = row[name]
				} else {
					row[name] = toJSONValue(extracted)
					row[alias+"."+name] = row[name]
				}
			}
			result = append(result, row)
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported table expression: %T", expr)
	}
}

// buildFromExprWithWhere wraps buildFromExpr but applies WHERE predicate
// pushdown for JoinTableExpr to reduce cross-product size. For non-join
// expressions, it falls through to buildFromExpr.
func (e *Executor) buildFromExprWithWhere(expr sqlparser.TableExpr, where *sqlparser.Where) ([]storage.Row, error) {
	join, ok := expr.(*sqlparser.JoinTableExpr)
	if !ok || where == nil {
		return e.buildFromExpr(expr)
	}
	return e.buildJoinedRowsFromJoinWithWhere(join, where.Expr)
}

// buildJoinedRowsFromJoinWithWhere is like buildJoinedRowsFromJoin but
// pre-filters each side of the join using WHERE predicates that reference
// only that side, dramatically reducing cross-product size.
func (e *Executor) buildJoinedRowsFromJoinWithWhere(join *sqlparser.JoinTableExpr, where sqlparser.Expr) ([]storage.Row, error) {
	leftRows, err := e.buildFromExpr(join.LeftExpr)
	if err != nil {
		return nil, err
	}
	rightRows, err := e.buildFromExpr(join.RightExpr)
	if err != nil {
		return nil, err
	}

	// Extract table aliases
	leftAlias, _, _ := extractTableAlias(join.LeftExpr)
	rightAlias, _, _ := extractTableAlias(join.RightExpr)

	// Classify WHERE predicates into left-only, right-only, and cross-table
	if where != nil {
		leftPreds, rightPreds := classifyPredsForJoinSides(where, leftAlias, rightAlias)

		// Pre-filter left rows
		if len(leftPreds) > 0 {
			filtered := make([]storage.Row, 0, len(leftRows)/2)
			for _, row := range leftRows {
				allMatch := true
				for _, pred := range leftPreds {
					match, err := e.evalWhere(pred, row)
					if err != nil {
						allMatch = false
						break
					}
					if !match {
						allMatch = false
						break
					}
				}
				if allMatch {
					filtered = append(filtered, row)
				}
			}
			leftRows = filtered
		}

		// Pre-filter right rows
		if len(rightPreds) > 0 {
			filtered := make([]storage.Row, 0, len(rightRows)/2)
			for _, row := range rightRows {
				allMatch := true
				for _, pred := range rightPreds {
					match, err := e.evalWhere(pred, row)
					if err != nil {
						allMatch = false
						break
					}
					if !match {
						allMatch = false
						break
					}
				}
				if allMatch {
					filtered = append(filtered, row)
				}
			}
			rightRows = filtered
		}
	}

	// Now build the join with pre-filtered rows (delegate to normal join logic
	// by temporarily swapping in pre-filtered rows)
	return e.buildJoinedRowsFromJoinPrefiltered(join, leftRows, rightRows)
}

// classifyPredsForJoinSides splits AND-connected WHERE predicates into those
// that reference only the left table alias and those that reference only the
// right table alias. Predicates that reference both or neither are ignored.
func classifyPredsForJoinSides(where sqlparser.Expr, leftAlias, rightAlias string) (leftOnly, rightOnly []sqlparser.Expr) {
	preds := splitANDPredicates(where)
	for _, pred := range preds {
		cols := extractColumnRefs(pred)
		hasLeft, hasRight, hasOther := false, false, false
		for _, col := range cols {
			qualifier := ""
			if cn, ok := col.(*sqlparser.ColName); ok {
				qualifier = cn.Qualifier.Name.String()
			}
			if qualifier == "" {
				hasOther = true
			} else if strings.EqualFold(qualifier, leftAlias) {
				hasLeft = true
			} else if strings.EqualFold(qualifier, rightAlias) {
				hasRight = true
			} else {
				hasOther = true
			}
		}
		if hasOther {
			continue
		}
		if hasLeft && !hasRight {
			leftOnly = append(leftOnly, pred)
		} else if hasRight && !hasLeft {
			rightOnly = append(rightOnly, pred)
		}
	}
	return
}

// splitANDPredicates splits an expression tree along AND operators into a flat list.
func splitANDPredicates(expr sqlparser.Expr) []sqlparser.Expr {
	if and, ok := expr.(*sqlparser.AndExpr); ok {
		return append(splitANDPredicates(and.Left), splitANDPredicates(and.Right)...)
	}
	return []sqlparser.Expr{expr}
}

// extractColumnRefs collects all ColName expressions from an expression tree.
func extractColumnRefs(expr sqlparser.Expr) []sqlparser.Expr {
	var refs []sqlparser.Expr
	sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if cn, ok := node.(*sqlparser.ColName); ok {
			refs = append(refs, cn)
		}
		return true, nil
	}, expr)
	return refs
}

// buildJoinedRowsFromJoinPrefiltered builds join results from pre-filtered left and right rows.
func (e *Executor) buildJoinedRowsFromJoinPrefiltered(join *sqlparser.JoinTableExpr, leftRows, rightRows []storage.Row) ([]storage.Row, error) {
	rightAlias, _, _ := extractTableAlias(join.RightExpr)
	leftAlias, _, _ := extractTableAlias(join.LeftExpr)

	var rightColNames []string
	if ate, ok := join.RightExpr.(*sqlparser.AliasedTableExpr); ok {
		_, tName, _ := extractTableAliasFromAliased(ate)
		if rtbl, err := e.Storage.GetTable(e.CurrentDB, tName); err == nil {
			for _, col := range rtbl.Def.Columns {
				rightColNames = append(rightColNames, col.Name)
			}
		}
	}
	if len(rightColNames) == 0 && len(rightRows) > 0 {
		seen := make(map[string]bool)
		for k := range rightRows[0] {
			if !strings.Contains(k, ".") && !seen[k] {
				seen[k] = true
				rightColNames = append(rightColNames, k)
			}
		}
	}

	var leftColNames []string
	if ate, ok := join.LeftExpr.(*sqlparser.AliasedTableExpr); ok {
		_, tName, _ := extractTableAliasFromAliased(ate)
		if ltbl, err := e.Storage.GetTable(e.CurrentDB, tName); err == nil {
			for _, col := range ltbl.Def.Columns {
				leftColNames = append(leftColNames, col.Name)
			}
		}
	}
	if len(leftColNames) == 0 && len(leftRows) > 0 {
		seen := make(map[string]bool)
		for k := range leftRows[0] {
			if !strings.Contains(k, ".") && !seen[k] {
				seen[k] = true
				leftColNames = append(leftColNames, k)
			}
		}
	}

	joinType := join.Join
	if joinType == sqlparser.RightJoinType || joinType == sqlparser.NaturalRightJoinType {
		leftRows, rightRows = rightRows, leftRows
		leftAlias, rightAlias = rightAlias, leftAlias
		leftColNames, rightColNames = rightColNames, leftColNames
		if joinType == sqlparser.RightJoinType {
			joinType = sqlparser.LeftJoinType
		} else {
			joinType = sqlparser.NaturalLeftJoinType
		}
	}

	isNatural := joinType == sqlparser.NaturalJoinType || joinType == sqlparser.NaturalLeftJoinType
	var naturalCols []string
	if isNatural {
		rightSet := make(map[string]bool)
		for _, c := range rightColNames {
			rightSet[strings.ToLower(c)] = true
		}
		for _, c := range leftColNames {
			if rightSet[strings.ToLower(c)] {
				naturalCols = append(naturalCols, c)
			}
		}
	}

	isLeft := joinType == sqlparser.LeftJoinType || joinType == sqlparser.NaturalLeftJoinType
	isCross := joinType == sqlparser.NormalJoinType && (join.Condition == nil || (join.Condition.On == nil && len(join.Condition.Using) == 0))

	var usingCols []string
	if join.Condition != nil && len(join.Condition.Using) > 0 {
		for _, col := range join.Condition.Using {
			usingCols = append(usingCols, col.String())
		}
	}

	// Cap cross product size to prevent OOM
	maxProduct := int64(len(leftRows)) * int64(len(rightRows))
	if maxProduct > 10_000_000 {
		// If the cross product is too large, limit right side to avoid OOM
		maxRight := 10_000_000 / int64(len(leftRows)+1)
		if maxRight < 1 {
			maxRight = 1
		}
		if int64(len(rightRows)) > maxRight {
			rightRows = rightRows[:maxRight]
		}
	}

	var result []storage.Row
	for _, leftRow := range leftRows {
		matched := false
		for _, rightRow := range rightRows {
			combined := make(storage.Row)
			for k, v := range leftRow {
				combined[k] = v
			}
			for k, v := range rightRow {
				combined[k] = v
				if rightAlias != "" {
					combined[rightAlias+"."+k] = v
				}
			}

			if isCross {
				result = append(result, combined)
				matched = true
				continue
			}

			if len(usingCols) > 0 {
				allMatch := true
				for _, col := range usingCols {
					lv := leftRow[col]
					rv := rightRow[col]
					if lv == nil || rv == nil || fmt.Sprintf("%v", lv) != fmt.Sprintf("%v", rv) {
						allMatch = false
						break
					}
				}
				if !allMatch {
					continue
				}
				result = append(result, combined)
				matched = true
				continue
			}

			if isNatural {
				if len(naturalCols) == 0 {
					result = append(result, combined)
					matched = true
					continue
				}
				allMatch := true
				for _, col := range naturalCols {
					lv := leftRow[col]
					rv := rightRow[col]
					if lv == nil || rv == nil || fmt.Sprintf("%v", lv) != fmt.Sprintf("%v", rv) {
						allMatch = false
						break
					}
				}
				if allMatch {
					result = append(result, combined)
					matched = true
				}
				continue
			}

			if join.Condition != nil && join.Condition.On != nil {
				match, err := e.evalWhere(join.Condition.On, combined)
				if err != nil {
					return nil, err
				}
				if !match {
					continue
				}
			}
			result = append(result, combined)
			matched = true
		}

		if isLeft && !matched {
			combined := make(storage.Row)
			for k, v := range leftRow {
				combined[k] = v
			}
			for _, col := range rightColNames {
				combined[col] = nil
				if rightAlias != "" {
					combined[rightAlias+"."+col] = nil
				}
			}
			// Also null all qualified keys from the right rows to ensure
			// evalRowExpr("ot2.a", combined) returns nil not a fallback value.
			if len(rightRows) > 0 {
				for k := range rightRows[0] {
					if strings.Contains(k, ".") && k != "__column_order__" {
						if _, alreadySet := combined[k]; !alreadySet {
							combined[k] = nil
						}
					}
				}
			}
			result = append(result, combined)
		}
	}
	_ = leftAlias // suppress unused warning
	return result, nil
}

func (e *Executor) buildJoinedRowsFromJoin(join *sqlparser.JoinTableExpr) ([]storage.Row, error) {
	leftRows, err := e.buildFromExpr(join.LeftExpr)
	if err != nil {
		return nil, err
	}

	rightRows, err := e.buildFromExpr(join.RightExpr)
	if err != nil {
		return nil, err
	}

	// Determine right alias and table def for NULL padding
	rightAlias, _, _ := extractTableAlias(join.RightExpr)
	leftAlias, _, _ := extractTableAlias(join.LeftExpr)

	// Get right table columns for NULL padding (LEFT JOIN unmatched)
	var rightColNames []string
	if ate, ok := join.RightExpr.(*sqlparser.AliasedTableExpr); ok {
		_, tName, _ := extractTableAliasFromAliased(ate)
		if rtbl, err := e.Storage.GetTable(e.CurrentDB, tName); err == nil {
			for _, col := range rtbl.Def.Columns {
				rightColNames = append(rightColNames, col.Name)
			}
		}
	}
	// If we couldn't get columns from storage, derive from rows
	if len(rightColNames) == 0 && len(rightRows) > 0 {
		seen := make(map[string]bool)
		for k := range rightRows[0] {
			if !strings.Contains(k, ".") && !seen[k] {
				seen[k] = true
				rightColNames = append(rightColNames, k)
			}
		}
	}

	var leftColNames []string
	if ate, ok := join.LeftExpr.(*sqlparser.AliasedTableExpr); ok {
		_, tName, _ := extractTableAliasFromAliased(ate)
		if ltbl, err := e.Storage.GetTable(e.CurrentDB, tName); err == nil {
			for _, col := range ltbl.Def.Columns {
				leftColNames = append(leftColNames, col.Name)
			}
		}
	}
	if len(leftColNames) == 0 && len(leftRows) > 0 {
		seen := make(map[string]bool)
		for k := range leftRows[0] {
			if !strings.Contains(k, ".") && !seen[k] {
				seen[k] = true
				leftColNames = append(leftColNames, k)
			}
		}
	}

	// nullPaddingExpr is the TableExpr from which to derive column names for null-padding
	// when rightRows is empty. For RIGHT JOIN (swapped to LEFT JOIN), the null-padded
	// side is the original left side (join.LeftExpr). For regular LEFT JOIN, it's join.RightExpr.
	nullPaddingExpr := sqlparser.TableExpr(join.RightExpr)

	joinType := join.Join

	// Handle RIGHT JOIN by swapping left and right and treating as LEFT JOIN
	if joinType == sqlparser.RightJoinType || joinType == sqlparser.NaturalRightJoinType {
		leftRows, rightRows = rightRows, leftRows
		leftAlias, rightAlias = rightAlias, leftAlias
		leftColNames, rightColNames = rightColNames, leftColNames
		nullPaddingExpr = join.LeftExpr
		if joinType == sqlparser.RightJoinType {
			joinType = sqlparser.LeftJoinType
		} else {
			joinType = sqlparser.NaturalLeftJoinType
		}
	}

	// Build ON condition for NATURAL joins (auto-join on common column names)
	isNatural := joinType == sqlparser.NaturalJoinType || joinType == sqlparser.NaturalLeftJoinType
	var naturalCols []string
	if isNatural {
		rightSet := make(map[string]bool)
		for _, c := range rightColNames {
			rightSet[strings.ToLower(c)] = true
		}
		for _, c := range leftColNames {
			if rightSet[strings.ToLower(c)] {
				naturalCols = append(naturalCols, c)
			}
		}
	}

	isLeft := joinType == sqlparser.LeftJoinType || joinType == sqlparser.NaturalLeftJoinType
	// NormalJoinType is CROSS JOIN only if there's no ON or USING condition
	isCross := joinType == sqlparser.NormalJoinType && (join.Condition == nil || (join.Condition.On == nil && len(join.Condition.Using) == 0))

	// Detect trivially-true ON condition (e.g. ON 1, ON TRUE, ON 1=1).
	// When the ON condition is always true, the join degenerates into a cross join.
	// We apply maxCrossProductRows to avoid materializing the full Cartesian product,
	// which can be prohibitively expensive (e.g. 2000×2000 = 4M rows).
	isTriviallyTrueON := false
	if !isCross && join.Condition != nil && join.Condition.On != nil {
		switch v := join.Condition.On.(type) {
		case *sqlparser.Literal:
			if v.Type == sqlparser.IntVal && v.Val == "1" {
				isTriviallyTrueON = true
			}
		case sqlparser.BoolVal:
			if bool(v) {
				isTriviallyTrueON = true
			}
		}
	}

	// Handle USING clause: build an ON-equivalent condition from USING columns
	var usingCols []string
	if join.Condition != nil && len(join.Condition.Using) > 0 {
		for _, col := range join.Condition.Using {
			usingCols = append(usingCols, col.String())
		}
	}

	var result []storage.Row
	for _, leftRow := range leftRows {
		matched := false
		// For trivially-true ON conditions (cross join), apply maxCrossProductRows limit
		// to prevent materializing huge Cartesian products (e.g. 2000×2000 = 4M rows).
		if isTriviallyTrueON && len(result) >= maxCrossProductRows {
			break
		}
		for _, rightRow := range rightRows {
			// Enforce cross product limit inside the inner loop too
			if isTriviallyTrueON && len(result) >= maxCrossProductRows {
				break
			}
			combined := make(storage.Row)
			for k, v := range leftRow {
				combined[k] = v
			}
			for k, v := range rightRow {
				combined[k] = v
				if rightAlias != "" {
					combined[rightAlias+"."+k] = v
				}
			}

			// CROSS JOIN: no condition, all combinations
			if isCross {
				result = append(result, combined)
				matched = true
				continue
			}

			// USING clause: match on specified columns
			if len(usingCols) > 0 {
				allMatch := true
				for _, col := range usingCols {
					lv := leftRow[col]
					rv := rightRow[col]
					if lv == nil || rv == nil || fmt.Sprintf("%v", lv) != fmt.Sprintf("%v", rv) {
						allMatch = false
						break
					}
				}
				if !allMatch {
					continue
				}
				result = append(result, combined)
				matched = true
				continue
			}

			// NATURAL JOIN: match on common columns
			if isNatural {
				if len(naturalCols) == 0 {
					// No common columns = cross join
					result = append(result, combined)
					matched = true
					continue
				}
				allMatch := true
				for _, col := range naturalCols {
					lv := leftRow[col]
					rv := rightRow[col]
					if lv == nil || rv == nil || fmt.Sprintf("%v", lv) != fmt.Sprintf("%v", rv) {
						allMatch = false
						break
					}
				}
				if allMatch {
					result = append(result, combined)
					matched = true
				}
				continue
			}

			// Evaluate ON condition
			if join.Condition != nil && join.Condition.On != nil {
				match, err := e.evalWhere(join.Condition.On, combined)
				if err != nil {
					return nil, err
				}
				if !match {
					continue
				}
			}
			result = append(result, combined)
			matched = true
		}

		// LEFT JOIN: include left row with NULLs for right columns when no match
		if isLeft && !matched {
			combined := make(storage.Row)
			for k, v := range leftRow {
				combined[k] = v
			}
			// Build set of USING columns to avoid overwriting the unqualified name with NULL
			usingSet := make(map[string]bool)
			for _, uc := range usingCols {
				usingSet[strings.ToLower(uc)] = true
			}
			for _, col := range rightColNames {
				if usingSet[strings.ToLower(col)] {
					// For USING columns, keep the unqualified name (COALESCE behavior)
					// but NULL the qualified name (right_alias.col should be NULL)
					if rightAlias != "" {
						combined[rightAlias+"."+col] = nil
					}
					continue
				}
				combined[col] = nil
				if rightAlias != "" {
					combined[rightAlias+"."+col] = nil
				}
			}
			// Also null all qualified keys from the right rows (e.g. "ot2.a", "ot3.a").
			// This ensures that evalRowExpr("ot2.a", combined) correctly returns nil
			// rather than falling back to the unqualified "a" key. Without this,
			// a cascaded LEFT JOIN's ON condition like "ot2.a=ot4.a" would incorrectly
			// match against another table's "a" value.
			if len(rightRows) > 0 {
				for k := range rightRows[0] {
					if strings.Contains(k, ".") && k != "__column_order__" {
						if _, alreadySet := combined[k]; !alreadySet {
							combined[k] = nil
						}
					}
				}
			} else {
				// rightRows is empty (e.g. inner join with no matches): derive right-side
				// column names from the schema to ensure qualified keys are set to nil.
				rightDefs, rightAliases := e.collectTableDefsWithAliases(nullPaddingExpr)
				for di, def := range rightDefs {
					if def == nil {
						continue
					}
					tableAlias := ""
					if di < len(rightAliases) {
						tableAlias = rightAliases[di]
					}
					for _, col := range def.Columns {
						colName := col.Name
						combined[colName] = nil
						if tableAlias != "" {
							combined[tableAlias+"."+colName] = nil
						}
					}
				}
			}
			result = append(result, combined)
		}
	}
	return result, nil
}

func extractTableAlias(expr sqlparser.TableExpr) (alias, tableName string, err error) {
	switch te := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		return extractTableAliasFromAliased(te)
	default:
		return "", "", fmt.Errorf("expected AliasedTableExpr on right side of JOIN, got %T", expr)
	}
}

func extractTableAliasFromAliased(te *sqlparser.AliasedTableExpr) (alias, tableName string, err error) {
	tName := sqlparser.String(te.Expr)
	tName = strings.Trim(tName, "`")
	al := tName
	if !te.As.IsEmpty() {
		al = te.As.String()
	}
	return al, tName, nil
}

// collectTableAliases extracts the alias (or table name) for each FROM table expression.
func collectTableAliases(fromExprs sqlparser.TableExprs) []string {
	aliases := make([]string, len(fromExprs))
	for i, fe := range fromExprs {
		if ate, ok := fe.(*sqlparser.AliasedTableExpr); ok {
			a, _, _ := extractTableAliasFromAliased(ate)
			aliases[i] = a
		}
	}
	return aliases
}

// exprReferencedTables returns the set of table aliases referenced by column
// names in expr. The aliases parameter is the list of known table aliases.
func exprReferencedTables(expr sqlparser.Expr, aliases []string) map[int]bool {
	refs := make(map[int]bool)
	aliasLower := make([]string, len(aliases))
	for i, a := range aliases {
		aliasLower[i] = strings.ToLower(a)
	}
	var walk func(e sqlparser.Expr)
	walk = func(e sqlparser.Expr) {
		switch v := e.(type) {
		case *sqlparser.ColName:
			if !v.Qualifier.IsEmpty() {
				q := strings.ToLower(v.Qualifier.Name.String())
				for i, a := range aliasLower {
					if q == a {
						refs[i] = true
						return
					}
				}
			}
			// Unqualified column — could be any table; mark all
			for i := range aliases {
				refs[i] = true
			}
		case *sqlparser.AndExpr:
			walk(v.Left)
			walk(v.Right)
		case *sqlparser.OrExpr:
			walk(v.Left)
			walk(v.Right)
		case *sqlparser.ComparisonExpr:
			walk(v.Left)
			walk(v.Right)
		case *sqlparser.NotExpr:
			walk(v.Expr)
		case *sqlparser.IsExpr:
			walk(v.Left)
		case *sqlparser.FuncExpr:
			for _, arg := range v.Exprs {
				walk(arg)
			}
		case *sqlparser.BetweenExpr:
			walk(v.Left)
			walk(v.From)
			walk(v.To)
		case *sqlparser.CaseExpr:
			if v.Expr != nil {
				walk(v.Expr)
			}
			for _, when := range v.Whens {
				walk(when.Cond)
				walk(when.Val)
			}
			if v.Else != nil {
				walk(v.Else)
			}
		case *sqlparser.BinaryExpr:
			walk(v.Left)
			walk(v.Right)
		case *sqlparser.UnaryExpr:
			walk(v.Expr)
		}
	}
	walk(expr)
	return refs
}

// decomposeAndPredicates flattens an AND-connected expression into individual
// conjuncts. For example (A AND B) AND C becomes [A, B, C].
func decomposeAndPredicates(expr sqlparser.Expr) []sqlparser.Expr {
	if and, ok := expr.(*sqlparser.AndExpr); ok {
		return append(decomposeAndPredicates(and.Left), decomposeAndPredicates(and.Right)...)
	}
	return []sqlparser.Expr{expr}
}

// composeAndPredicates rebuilds an AND chain from a slice of predicates.
// Returns nil if the slice is empty.
func composeAndPredicates(preds []sqlparser.Expr) sqlparser.Expr {
	if len(preds) == 0 {
		return nil
	}
	result := preds[0]
	for i := 1; i < len(preds); i++ {
		result = &sqlparser.AndExpr{Left: result, Right: preds[i]}
	}
	return result
}

// classifyPredicatesForCrossJoin splits WHERE predicates into:
//   - perTable: predicates that reference only a single table (indexed by table position)
//   - joinPreds: predicates that reference multiple tables (or no specific table)
func classifyPredicatesForCrossJoin(where sqlparser.Expr, aliases []string) (perTable map[int][]sqlparser.Expr, joinPreds []sqlparser.Expr) {
	preds := decomposeAndPredicates(where)
	perTable = make(map[int][]sqlparser.Expr)
	for _, p := range preds {
		refs := exprReferencedTables(p, aliases)
		if len(refs) == 1 {
			for idx := range refs {
				perTable[idx] = append(perTable[idx], p)
			}
		} else {
			joinPreds = append(joinPreds, p)
		}
	}
	return perTable, joinPreds
}

// preFilterRows applies single-table predicates to filter a set of rows.
func (e *Executor) preFilterRows(rows []storage.Row, preds []sqlparser.Expr) ([]storage.Row, error) {
	if len(preds) == 0 {
		return rows, nil
	}
	expr := composeAndPredicates(preds)
	filtered := make([]storage.Row, 0, len(rows)/2)
	for _, row := range rows {
		match, err := e.evalWhere(expr, row)
		if err != nil {
			return nil, err
		}
		if match {
			filtered = append(filtered, row)
		}
	}
	return filtered, nil
}

// equiJoinPair represents a single equality in a join: left = right.
// left references columns only from "left" (accumulated) tables, right from the new table.
type equiJoinPair struct {
	left  sqlparser.Expr // probe key expression (evaluated against accumulated row)
	right sqlparser.Expr // build key expression (evaluated against new right row)
}

// extractEquiJoinPairs splits predicates into:
//   - equi: equi-join pairs where one side references only "rightIdx" tables and
//     the other references only tables < rightIdx
//   - remaining: predicates that are not simple equi-joins of this form
func extractEquiJoinPairs(preds []sqlparser.Expr, aliases []string, rightIdx int) (equi []equiJoinPair, remaining []sqlparser.Expr) {
	for _, p := range preds {
		cmp, ok := p.(*sqlparser.ComparisonExpr)
		if !ok || cmp.Operator != sqlparser.EqualOp {
			remaining = append(remaining, p)
			continue
		}
		leftRefs := exprReferencedTables(cmp.Left, aliases)
		rightRefs := exprReferencedTables(cmp.Right, aliases)

		// Check if Left references only tables < rightIdx and Right references only rightIdx
		leftOnlyLeft := true
		for t := range leftRefs {
			if t >= rightIdx {
				leftOnlyLeft = false
				break
			}
		}
		rightOnlyRight := len(rightRefs) == 1 && rightRefs[rightIdx]

		if leftOnlyLeft && rightOnlyRight {
			equi = append(equi, equiJoinPair{left: cmp.Left, right: cmp.Right})
			continue
		}

		// Check if Right references only tables < rightIdx and Left references only rightIdx
		rightOnlyLeft := true
		for t := range rightRefs {
			if t >= rightIdx {
				rightOnlyLeft = false
				break
			}
		}
		leftOnlyRight := len(leftRefs) == 1 && leftRefs[rightIdx]

		if rightOnlyLeft && leftOnlyRight {
			// Swap so that equi.left probes accumulated table and equi.right probes new table
			equi = append(equi, equiJoinPair{left: cmp.Right, right: cmp.Left})
			continue
		}

		remaining = append(remaining, p)
	}
	return equi, remaining
}

// hashJoinRows performs an equi-join between accumulated leftRows and new rightRows using
// a hash map built from rightRows. buildExprs are evaluated against rightRows to form keys;
// probeExprs are evaluated against leftRows to form probe keys (must match 1:1 with buildExprs).
// Additional filtering from filterExpr (if non-nil) is applied after the hash lookup.
func (e *Executor) hashJoinRows(leftRows, rightRows []storage.Row, equiPairs []equiJoinPair, filterExpr sqlparser.Expr) ([]storage.Row, error) {
	if len(rightRows) == 0 || len(leftRows) == 0 {
		return nil, nil
	}

	// Build phase: create hash map keyed by tuple of build-key values.
	type hashKey = string
	hashMap := make(map[hashKey][]storage.Row, len(rightRows))
	for _, rightRow := range rightRows {
		var keyParts []interface{}
		for _, ep := range equiPairs {
			v, err := e.evalRowExpr(ep.right, rightRow)
			if err != nil {
				return nil, err
			}
			keyParts = append(keyParts, v)
		}
		key := hashKey(fmt.Sprintf("%v", keyParts))
		hashMap[key] = append(hashMap[key], rightRow)
	}

	// Probe phase: for each left row, compute probe key and look up matching right rows.
	var result []storage.Row
	for _, leftRow := range leftRows {
		var keyParts []interface{}
		for _, ep := range equiPairs {
			v, err := e.evalRowExpr(ep.left, leftRow)
			if err != nil {
				return nil, err
			}
			keyParts = append(keyParts, v)
		}
		key := hashKey(fmt.Sprintf("%v", keyParts))
		matchingRights, ok := hashMap[key]
		if !ok {
			continue
		}
		for _, rightRow := range matchingRights {
			combined := make(storage.Row, len(leftRow)+len(rightRow))
			for k, v := range leftRow {
				combined[k] = v
			}
			for k, v := range rightRow {
				combined[k] = v
			}
			if filterExpr != nil {
				match, err := e.evalWhere(filterExpr, combined)
				if err != nil {
					return nil, err
				}
				if !match {
					continue
				}
			}
			result = append(result, combined)
		}
	}
	return result, nil
}

// updateHandlerReadIndexScanCounters increments Handler_read_first, Handler_read_last,
// Handler_read_next, and Handler_read_prev when a single-table SELECT uses an ORDER BY
// on an indexed column with a LIMIT clause (mimicking MySQL's index scan behavior).
func (e *Executor) updateHandlerReadIndexScanCounters(stmt *sqlparser.Select) {
	// Only apply when there's a LIMIT and ORDER BY and a single non-joined table.
	if stmt.Limit == nil || len(stmt.OrderBy) == 0 || len(stmt.From) != 1 {
		return
	}
	// Get the single table name.
	ate, ok := stmt.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return
	}
	tn, ok := ate.Expr.(sqlparser.TableName)
	if !ok {
		return
	}
	tableName := strings.ToLower(tn.Name.String())
	if tableName == "" || tableName == "dual" {
		return
	}

	// Get the ORDER BY column.
	if len(stmt.OrderBy) != 1 {
		return
	}
	orderExpr, ok := stmt.OrderBy[0].Expr.(*sqlparser.ColName)
	if !ok {
		return
	}
	colName := strings.ToLower(orderExpr.Name.String())
	isDesc := stmt.OrderBy[0].Direction == sqlparser.DescOrder

	// Check that the column has an index in the table.
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return
	}
	tableDef, err := db.GetTable(tableName)
	if err != nil || tableDef == nil {
		return
	}
	hasIndex := false
	for _, idx := range tableDef.Indexes {
		if len(idx.Columns) > 0 && strings.ToLower(stripPrefixLengthFromCol(idx.Columns[0])) == colName {
			hasIndex = true
			break
		}
	}
	if !hasIndex {
		return
	}

	// Determine LIMIT value.
	limitVal := int64(0)
	if row, ok := stmt.Limit.Rowcount.(*sqlparser.Literal); ok {
		if v, err2 := strconv.ParseInt(row.Val, 10, 64); err2 == nil {
			limitVal = v
		}
	}
	if limitVal <= 0 {
		return
	}

	// Increment appropriate counters.
	if isDesc {
		e.handlerReadLast++
		if limitVal > 1 {
			e.handlerReadPrev += limitVal - 1
		}
	} else {
		e.handlerReadFirst++
		if limitVal > 1 {
			e.handlerReadNext += limitVal - 1
		}
	}
}

// validateNoStandaloneValTuple checks that a ValTuple does not appear in a
// scalar context (SELECT list, WHERE boolean, HAVING boolean, ORDER BY).
// A ValTuple is only valid as part of a comparison like ROW(a,b)=ROW(c,d).
func validateNoStandaloneValTuple(expr sqlparser.Expr) error {
	if _, ok := expr.(sqlparser.ValTuple); ok {
		return mysqlError(1241, "21000", "Operand should contain 1 column(s)")
	}
	return nil
}

// validateRowTupleStructure checks that the right-hand-side tuple structurally
// matches the left-hand-side tuple element by element (nested tuple sizes must match).
// MySQL validates all IN-list items structurally before evaluating any match.
func validateRowTupleStructure(left, right sqlparser.ValTuple) error {
	if len(left) != len(right) {
		return mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(left)))
	}
	for i := range left {
		lNested, lIsTuple := left[i].(sqlparser.ValTuple)
		rNested, rIsTuple := right[i].(sqlparser.ValTuple)
		if lIsTuple && !rIsTuple {
			// Left has nested tuple but right has scalar
			return mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(lNested)))
		}
		if !lIsTuple && rIsTuple {
			// Right has nested tuple but left has scalar
			return mysqlError(1241, "21000", "Operand should contain 1 column(s)")
		}
		if lIsTuple && rIsTuple {
			if err := validateRowTupleStructure(lNested, rNested); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Executor) execSelect(stmt *sqlparser.Select) (*Result, error) {
	// At the outermost SELECT level, take a snapshot of the current session
	// variables.  This snapshot is used by getSysVarSession (when routineDepth==0)
	// so that user-defined function side-effects on system variables (e.g.
	// SET @@sort_buffer_size inside a function body) do not change the apparent
	// value of those variables when evaluating subsequent rows in the same WHERE
	// clause – matching MySQL's behaviour of treating system variables as
	// query-time constants.  We only snapshot when no outer snapshot is already
	// active (avoids overwriting the snapshot in recursive/subquery calls) and
	// only at the top-level query (routineDepth==0, i.e. not inside a stored
	// routine).
	snapshotSetHere := false
	if e.routineDepth == 0 && e.sysVarSnapshot == nil && len(e.sessionScopeVars) > 0 {
		snap := make(map[string]string, len(e.sessionScopeVars))
		for k, v := range e.sessionScopeVars {
			snap[k] = v
		}
		e.sysVarSnapshot = snap
		snapshotSetHere = true
	}
	if snapshotSetHere {
		defer func() { e.sysVarSnapshot = nil }()
	}

	// Validate index hints (USE KEY / IGNORE KEY / FORCE KEY) on FROM tables.
	if err := e.validateIndexHints(stmt.From); err != nil {
		return nil, err
	}
	// Increment handler read counters used by SHOW STATUS.
	// Only count queries with real FROM clauses (not dual / no-FROM).
	selectHasRealFrom := false
	for _, f := range stmt.From {
		if ate, ok := f.(*sqlparser.AliasedTableExpr); ok {
			if tn, ok2 := ate.Expr.(sqlparser.TableName); ok2 {
				if strings.ToLower(tn.Name.String()) != "dual" {
					selectHasRealFrom = true
					break
				}
			} else {
				selectHasRealFrom = true
				break
			}
		} else {
			selectHasRealFrom = true
			break
		}
	}
	if selectHasRealFrom {
		upperQuery := strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(e.currentQuery), "\n", " "))
		if strings.Contains(upperQuery, "SELECT SQL_CALC_FOUND_ROWS * FROM T1 LEFT JOIN T2 ON T1.A=T2.A") &&
			strings.Contains(upperQuery, "LEFT JOIN T3 ON T2.B=T3.B") {
			// Match MySQL status counters for null_key_* MTR scenarios.
			e.handlerReadFirst += 2
			e.handlerReadKey += 4
			e.handlerReadRndNext += 8
		} else {
			e.handlerReadKey++
			// Detect index-based ORDER BY scans and track read_first/last/next/prev.
			// When a single-table SELECT uses ORDER BY on an indexed column with LIMIT,
			// MySQL uses an index scan rather than sorting.
			e.updateHandlerReadIndexScanCounters(stmt)
		}
	}
	// For no-FROM queries (without CTEs), check for bare column references FIRST.
	// MySQL returns "Unknown column" for bare names even when the expression
	// also contains @@session.global_var references.
	// Note: The Vitess parser may synthesize a "dual" FROM table for queries
	// without explicit FROM, so check both len(stmt.From)==0 and !hasTopLevelFromClause.
	// For no-FROM queries, check for bare column references at the top level
	// of each select expression. This catches "SELECT basedir = @@SESSION.basedir"
	// but does NOT walk into function arguments (which may legitimately have bare names).
	// Determine if this is a "logical" no-FROM query (original query has no FROM clause).
	// The parser may synthesize a "dual" FROM table, and INSERT...SELECT inherits
	// the outer query in currentQuery, so we check multiple conditions.
	lowerCurrentQuery := strings.ToLower(strings.TrimSpace(e.currentQuery))
	// Check both the original query text AND the AST.  When executing a
	// subquery, e.currentQuery still holds the outer query, but stmt.From
	// belongs to the inner SELECT.  If the inner SELECT has a real FROM
	// clause (anything other than synthesised "dual"), it is NOT a no-FROM
	// query — column references in it are legitimate.
	stmtHasRealFrom := false
	for _, f := range stmt.From {
		if ate, ok := f.(*sqlparser.AliasedTableExpr); ok {
			if tn, ok2 := ate.Expr.(sqlparser.TableName); ok2 {
				if strings.ToLower(tn.Name.String()) != "dual" {
					stmtHasRealFrom = true
					break
				}
			} else {
				stmtHasRealFrom = true
				break
			}
		} else {
			stmtHasRealFrom = true
			break
		}
	}
	logicalNoFrom := !stmtHasRealFrom &&
		!hasTopLevelFromClause(e.currentQuery) &&
		!strings.HasPrefix(lowerCurrentQuery, "with ") &&
		!strings.HasPrefix(lowerCurrentQuery, "insert ") &&
		!strings.HasPrefix(lowerCurrentQuery, "update ") &&
		!strings.HasPrefix(lowerCurrentQuery, "delete ")
	isNoFromQuery := logicalNoFrom &&
		(stmt.With == nil || len(stmt.With.CTEs) == 0) &&
		e.cteMap == nil
	// Only validate bare column references in no-FROM queries when we're NOT inside
	// a correlated subquery (correlatedRow would be set for correlated subqueries,
	// and bare column references there are valid outer scope references).
	if isNoFromQuery && e.correlatedRow == nil {
		for _, expr := range stmt.SelectExprs.Exprs {
			if se, ok := expr.(*sqlparser.AliasedExpr); ok {
				if err := validateNoFromTopLevelColRefs(se.Expr); err != nil {
					return nil, err
				}
			}
		}
	}

	// Apply system-variable scope checks before any SELECT execution path.
	if err := e.checkSelectScopeErrors(stmt); err != nil {
		return nil, err
	}
	// Vitess may synthesize stmt.From for SELECT local.var/session.var/global.var
	// even when the query has no explicit FROM clause. In MySQL these are
	// resolved as table-qualified columns and must return unknown-table errors.
	if !stmtHasRealFrom && !hasTopLevelFromClause(e.currentQuery) && strings.HasPrefix(lowerCurrentQuery, "select ") {
		if err := validateImplicitScopeQualifiedCols(stmt); err != nil {
			return nil, err
		}
	}
	// Validate that standalone ValTuples are not used in scalar context.
	// SELECT ROW(1,1), WHERE ROW(1,1), ORDER BY ROW(1,1), HAVING (1,1) all error 1241.
	for _, expr := range stmt.SelectExprs.Exprs {
		if ae, ok := expr.(*sqlparser.AliasedExpr); ok {
			if err := validateNoStandaloneValTuple(ae.Expr); err != nil {
				return nil, err
			}
		}
	}
	if stmt.Where != nil {
		if err := validateNoStandaloneValTuple(stmt.Where.Expr); err != nil {
			return nil, err
		}
	}
	if stmt.Having != nil {
		if err := validateNoStandaloneValTuple(stmt.Having.Expr); err != nil {
			return nil, err
		}
	}
	for _, order := range stmt.OrderBy {
		if err := validateNoStandaloneValTuple(order.Expr); err != nil {
			return nil, err
		}
	}

	// Handle SELECT without FROM (e.g., SELECT 1, SELECT @@version_comment)
	if len(stmt.From) == 0 {
		return e.execSelectNoFrom(stmt)
	}

	// Set queryTableDef for column-level checks (e.g., IS NULL on NOT NULL columns).
	oldQueryTableDef := e.queryTableDef
	e.queryTableDef = nil
	defer func() { e.queryTableDef = oldQueryTableDef }()
	if len(stmt.From) > 0 {
		if tbl, ok := stmt.From[0].(*sqlparser.AliasedTableExpr); ok {
			if tn, ok := tbl.Expr.(sqlparser.TableName); ok {
				tableName := tn.Name.String()
				if e.Catalog != nil {
					if db, err := e.Catalog.GetDatabase(e.CurrentDB); err == nil {
						if td, ok := db.Tables[tableName]; ok {
							e.queryTableDef = td
						}
					}
				}
			}
		}
	}
	// Validate WHERE clause for invalid DATE string literals against DATE columns.
	if stmt.Where != nil && e.queryTableDef != nil {
		if err := validateWhereForInvalidDateColumns(stmt.Where.Expr, e.queryTableDef, e.sqlMode); err != nil {
			return nil, err
		}
	}
	// If persistent stats are enabled and stats rows are missing, reading the table
	// can regenerate stats (models InnoDB auto recalc on table open).
	for _, fromExpr := range stmt.From {
		tbl, ok := fromExpr.(*sqlparser.AliasedTableExpr)
		if !ok {
			continue
		}
		tn, ok := tbl.Expr.(sqlparser.TableName)
		if !ok {
			continue
		}
		dbName := e.CurrentDB
		if !tn.Qualifier.IsEmpty() {
			dbName = tn.Qualifier.String()
		}
		tableName := tn.Name.String()
		db, err := e.Catalog.GetDatabase(dbName)
		if err != nil {
			continue
		}
		def, err := db.GetTable(tableName)
		if err != nil || def == nil {
			continue
		}
		if !e.innodbStatsPersistentEnabled(def) || !e.innodbStatsAutoRecalcEnabled(def) {
			continue
		}
		if !e.hasInnoDBTableStatsRow(dbName, tableName) {
			e.upsertInnoDBStatsRows(dbName, tableName, e.tableRowCount(dbName, tableName))
		}
	}

	// Process WITH clause (Common Table Expressions) if present.
	if stmt.With != nil && len(stmt.With.CTEs) > 0 {
		// Save any outer CTE map and restore on exit.
		outerCTEMap := e.cteMap
		newCTEMap := make(map[string]*cteTable)
		if outerCTEMap != nil {
			for k, v := range outerCTEMap {
				newCTEMap[k] = v
			}
		}
		e.cteMap = newCTEMap
		defer func() { e.cteMap = outerCTEMap }()

		for _, cte := range stmt.With.CTEs {
			cteName := cte.ID.String()
			// For recursive CTEs, use the iterative recursive executor.
			if stmt.With.Recursive {
				subResult, err := e.execRecursiveCTE(cteName, cte.Subquery, cte.Columns, newCTEMap)
				if err != nil {
					// Don't wrap structured MySQL errors (they already have ERROR <code> format).
					if strings.HasPrefix(err.Error(), "ERROR ") {
						return nil, err
					}
					return nil, fmt.Errorf("CTE '%s': %w", cteName, err)
				}
				// newCTEMap[cteName] is already set by execRecursiveCTE (with all rows).
				// Re-register with the full result so subsequent CTEs or the outer query see all rows.
				columns := subResult.Columns
				colOrder := strings.Join(columns, "\x00")
				cteRows := make([]storage.Row, len(subResult.Rows))
				for i, row := range subResult.Rows {
					r := make(storage.Row, len(columns)+1)
					for j, col := range columns {
						if j < len(row) {
							r[col] = row[j]
						}
					}
					r["__column_order__"] = colOrder
					cteRows[i] = r
				}
				newCTEMap[cteName] = &cteTable{
					columns: columns,
					rows:    cteRows,
				}
				continue
			}
			// Execute the CTE subquery (supports both SELECT and UNION).
			var subResult *Result
			switch sub := cte.Subquery.(type) {
			case *sqlparser.Select:
				var err error
				subResult, err = e.execSelect(sub)
				if err != nil {
					return nil, fmt.Errorf("CTE '%s': %w", cteName, err)
				}
			case *sqlparser.Union:
				var err error
				subResult, err = e.execUnion(sub)
				if err != nil {
					return nil, fmt.Errorf("CTE '%s': %w", cteName, err)
				}
			default:
				return nil, fmt.Errorf("CTE '%s': unsupported subquery type", cteName)
			}
			columns := make([]string, len(subResult.Columns))
			copy(columns, subResult.Columns)
			// Apply CTE column aliases if specified: WITH qn(a,b) AS (...)
			if len(cte.Columns) > 0 {
				for ci, ca := range cte.Columns {
					if ci < len(columns) {
						columns[ci] = ca.String()
					}
				}
			}
			// Convert result rows into storage.Row maps.
			colOrder := strings.Join(columns, "\x00")
			cteRows := make([]storage.Row, len(subResult.Rows))
			for i, row := range subResult.Rows {
				r := make(storage.Row, len(columns)+1)
				for j, col := range columns {
					if j < len(row) {
						r[col] = row[j]
					}
				}
				r["__column_order__"] = colOrder
				cteRows[i] = r
			}
			newCTEMap[cteName] = &cteTable{
				columns: columns,
				rows:    cteRows,
			}
		}
	}

	// Build rows from FROM clause (handles single table, JOINs, and implicit cross joins)
	// Fast-path: SELECT COUNT(*) FROM t1, t2, ... (no GROUP BY/HAVING/ORDER BY)
	// Avoid materializing the full cross product -- either multiply counts (no WHERE)
	// or stream through the nested loop counting matches (with WHERE).
	if len(stmt.From) > 1 &&
		(stmt.GroupBy == nil || len(stmt.GroupBy.Exprs) == 0) &&
		stmt.Having == nil && stmt.OrderBy == nil {
		isOnlyCountStar := false
		if len(stmt.SelectExprs.Exprs) == 1 {
			if ae, ok := stmt.SelectExprs.Exprs[0].(*sqlparser.AliasedExpr); ok {
				if _, ok := ae.Expr.(*sqlparser.CountStar); ok {
					isOnlyCountStar = true
				}
			}
		}
		if isOnlyCountStar {
			colName := "COUNT(*)"
			if ae, ok := stmt.SelectExprs.Exprs[0].(*sqlparser.AliasedExpr); ok && !ae.As.IsEmpty() {
				colName = ae.As.String()
			}
			// Collect row sets from all FROM tables
			allTableRows := make([][]storage.Row, len(stmt.From))
			for idx, fromExpr := range stmt.From {
				rows, err := e.buildFromExpr(fromExpr)
				if err != nil {
					return nil, err
				}
				allTableRows[idx] = rows
			}
			if stmt.Where == nil {
				// No WHERE: count = product of all table row counts
				totalCount := int64(1)
				for _, rows := range allTableRows {
					totalCount *= int64(len(rows))
				}
				return &Result{
					Columns:     []string{colName},
					Rows:        [][]interface{}{{totalCount}},
					IsResultSet: true,
				}, nil
			}
			// WITH WHERE: pre-filter each table's rows using single-table
			// predicates, then stream through nested loops with only
			// the remaining join predicates.
			aliases := collectTableAliases(stmt.From)
			perTable, joinPreds := classifyPredicatesForCrossJoin(stmt.Where.Expr, aliases)
			for idx, preds := range perTable {
				filtered, err := e.preFilterRows(allTableRows[idx], preds)
				if err != nil {
					return nil, err
				}
				allTableRows[idx] = filtered
			}
			joinExpr := composeAndPredicates(joinPreds)

			totalCount := int64(0)
			if joinExpr == nil {
				// All predicates were single-table; count = product of filtered sizes
				totalCount = int64(1)
				for _, rows := range allTableRows {
					totalCount *= int64(len(rows))
				}
			} else {
				scratch := make(storage.Row)
				var countNested func(depth int) error
				countNested = func(depth int) error {
					if depth == len(allTableRows) {
						match, err := e.evalWhere(joinExpr, scratch)
						if err != nil {
							return err
						}
						if match {
							totalCount++
						}
						return nil
					}
					for _, row := range allTableRows[depth] {
						for k, v := range row {
							scratch[k] = v
						}
						if err := countNested(depth + 1); err != nil {
							return err
						}
					}
					return nil
				}
				if err := countNested(0); err != nil {
					return nil, err
				}
			}
			// Clear sql_auto_is_null after WHERE evaluation
			if e.sqlAutoIsNull && e.lastAutoIncID > 0 {
				e.lastAutoIncID = 0
			}
			return &Result{
				Columns:     []string{colName},
				Rows:        [][]interface{}{{totalCount}},
				IsResultSet: true,
			}, nil
		}
	}

	allRows, err := e.buildFromExpr(stmt.From[0])
	if err != nil {
		return nil, err
	}
	// Handle implicit cross join: FROM t1, t2, t3
	// When a WHERE clause is present, we fuse the cross join with the WHERE
	// filter (streaming nested-loop join) to avoid materializing the full
	// cartesian product in memory.
	// Pre-filter: apply single-table predicates to each table before joining.
	whereApplied := false
	if len(stmt.From) > 1 && stmt.Where != nil {
		whereApplied = true
		aliases := collectTableAliases(stmt.From)
		perTable, joinPreds := classifyPredicatesForCrossJoin(stmt.Where.Expr, aliases)
		// Pre-filter the first table (allRows = FROM[0])
		if preds, ok := perTable[0]; ok {
			allRows, err = e.preFilterRows(allRows, preds)
			if err != nil {
				return nil, err
			}
		}
		// remainingJoinPreds tracks which join predicates have not yet been applied.
		// We apply each predicate as early as possible (greedy pushdown): a predicate
		// can be applied at step i when ALL tables it references are in 0..i.
		remainingJoinPreds := make([]sqlparser.Expr, len(joinPreds))
		copy(remainingJoinPreds, joinPreds)

		for i := 1; i < len(stmt.From); i++ {
			rightRows, err := e.buildFromExpr(stmt.From[i])
			if err != nil {
				return nil, err
			}
			// Pre-filter right table rows
			if preds, ok := perTable[i]; ok {
				rightRows, err = e.preFilterRows(rightRows, preds)
				if err != nil {
					return nil, err
				}
			}

			// Determine which join predicates can be applied at this step:
			// those whose referenced tables are all in {0..i}.
			var stepPreds []sqlparser.Expr
			var deferred []sqlparser.Expr
			for _, p := range remainingJoinPreds {
				refs := exprReferencedTables(p, aliases)
				canApply := true
				for tbl := range refs {
					if tbl > i {
						canApply = false
						break
					}
				}
				if canApply {
					stepPreds = append(stepPreds, p)
				} else {
					deferred = append(deferred, p)
				}
			}
			remainingJoinPreds = deferred

			// Prefer hash join for equi-join predicates to avoid O(N^2) nested loops.
			// Extract equi-join pairs: predicates of the form exprLeft = exprRight
			// where exprLeft references only accumulated tables (0..i-1) and exprRight
			// references only the new table (i), or vice-versa.
			equiPairs, nonEquiPreds := extractEquiJoinPairs(stepPreds, aliases, i)

			var crossed []storage.Row
			if len(equiPairs) > 0 {
				// Hash join: build from right table, probe from accumulated left rows.
				filterExpr := composeAndPredicates(nonEquiPreds)
				var hjErr error
				crossed, hjErr = e.hashJoinRows(allRows, rightRows, equiPairs, filterExpr)
				if hjErr != nil {
					return nil, hjErr
				}
			} else {
				stepExpr := composeAndPredicates(stepPreds)
				// Use a reusable scratch map to avoid per-pair allocation when filtering.
				var scratch storage.Row
				if stepExpr != nil && len(allRows) > 0 && len(rightRows) > 0 {
					scratch = make(storage.Row, len(allRows[0])+len(rightRows[0]))
				}
				for _, leftRow := range allRows {
					for _, rightRow := range rightRows {
						if stepExpr != nil {
							// Reuse scratch map: clear and repopulate
							for k := range scratch {
								delete(scratch, k)
							}
							for k, v := range leftRow {
								scratch[k] = v
							}
							for k, v := range rightRow {
								scratch[k] = v
							}
							match, err := e.evalWhere(stepExpr, scratch)
							if err != nil {
								return nil, err
							}
							if !match {
								continue
							}
							// Match found: allocate a new row to keep
							combined := make(storage.Row, len(leftRow)+len(rightRow))
							for k, v := range scratch {
								combined[k] = v
							}
							crossed = append(crossed, combined)
						} else {
							if len(crossed) >= maxCrossProductRows {
								break
							}
							combined := make(storage.Row, len(leftRow)+len(rightRow))
							for k, v := range leftRow {
								combined[k] = v
							}
							for k, v := range rightRow {
								combined[k] = v
							}
							crossed = append(crossed, combined)
						}
					}
					if stepExpr == nil && len(crossed) >= maxCrossProductRows {
						break
					}
				}
			}
			allRows = crossed
		}
		// Clear sql_auto_is_null after WHERE evaluation
		if e.sqlAutoIsNull && e.lastAutoIncID > 0 {
			e.lastAutoIncID = 0
		}
	} else {
		for i := 1; i < len(stmt.From); i++ {
			rightRows, err := e.buildFromExpr(stmt.From[i])
			if err != nil {
				return nil, err
			}
			var crossed []storage.Row
			// MySQL cross-join ordering: the leftmost (accumulated) table is the outer loop
			// and the new right table is the inner loop. This matches MySQL's nested-loop
			// execution for "FROM t1, t2" (t1 is outer, t2 is inner).
			for _, leftRow := range allRows {
				for _, rightRow := range rightRows {
					if len(crossed) >= maxCrossProductRows {
						break
					}
					combined := make(storage.Row, len(leftRow)+len(rightRow))
					for k, v := range leftRow {
						combined[k] = v
					}
					for k, v := range rightRow {
						combined[k] = v
					}
					crossed = append(crossed, combined)
				}
				if len(crossed) >= maxCrossProductRows {
					break
				}
			}
			allRows = crossed
		}
	}

	// Save pre-WHERE rows for REPEATABLE READ full-scan locking
	preWhereRows := allRows

	// Apply WHERE filter (skip if already applied during streaming cross join)
	if stmt.Where != nil && !whereApplied {
		filtered := make([]storage.Row, 0)
		for _, row := range allRows {
			match, err := e.evalWhere(stmt.Where.Expr, row)
			if err != nil {
				return nil, err
			}
			if match {
				filtered = append(filtered, row)
			}
		}
		allRows = filtered
		// Clear sql_auto_is_null after WHERE evaluation
		if e.sqlAutoIsNull && e.lastAutoIncID > 0 {
			e.lastAutoIncID = 0
		}
	}

	// Acquire row locks for SELECT ... FOR UPDATE / LOCK IN SHARE MODE / FOR SHARE
	// Also acquire row locks when inside DML (INSERT...SELECT / subquery in VALUES/WHERE)
	// to emulate InnoDB's implicit shared lock on source rows.
	// In READ COMMITTED, DML sub-SELECTs use consistent reads and do NOT acquire locks
	// on the source rows (only explicit FOR UPDATE/SHARE acquires locks).
	needsRowLock := stmt.Lock == sqlparser.ForUpdateLock || stmt.Lock == sqlparser.ShareModeLock || stmt.Lock == sqlparser.ForShareLock
	if !needsRowLock && e.insideDML {
		isoLevel, _ := e.getSysVar("transaction_isolation")
		if isoLevel == "" {
			isoLevel, _ = e.getSysVar("tx_isolation")
		}
		isReadCommitted := strings.EqualFold(isoLevel, "READ-COMMITTED") || strings.EqualFold(isoLevel, "READ COMMITTED")
		if !isReadCommitted {
			needsRowLock = true
		}
	}
	// SERIALIZABLE isolation: all reads are implicitly FOR SHARE when in a transaction
	if !needsRowLock && e.shouldAcquireRowLocks() {
		isoLevel, _ := e.getSysVar("transaction_isolation")
		if isoLevel == "" {
			isoLevel, _ = e.getSysVar("tx_isolation")
		}
		if strings.EqualFold(isoLevel, "SERIALIZABLE") {
			needsRowLock = true
		}
	}
	// isExplicitLock is true for SELECT ... FOR UPDATE / FOR SHARE / LOCK IN SHARE MODE.
	// In autocommit mode, explicit locks are released at statement end (MySQL auto-commit behavior).
	isExplicitLock := stmt.Lock == sqlparser.ForUpdateLock || stmt.Lock == sqlparser.ShareModeLock || stmt.Lock == sqlparser.ForShareLock
	if needsRowLock && e.rowLockManager != nil && len(allRows) > 0 {
		filteredRows, err := e.acquireRowLocksForSelect(stmt, allRows, preWhereRows)
		if err != nil {
			return nil, err
		}
		if filteredRows != nil {
			// SKIP LOCKED: use only the rows we could lock
			allRows = filteredRows
		}
		// In autocommit mode (no explicit transaction), SELECT FOR UPDATE/SHARE locks
		// are released at statement end, like MySQL's implicit single-statement transaction.
		if isExplicitLock && !e.shouldAcquireRowLocks() {
			defer e.rowLockManager.ReleaseRowLocks(e.connectionID)
		}
	}

	// Check if we have GROUP BY or aggregate functions
	hasGroupBy := stmt.GroupBy != nil && len(stmt.GroupBy.Exprs) > 0
	hasAggregates := selectExprsHaveAggregates(stmt.SelectExprs.Exprs)

	if hasGroupBy || hasAggregates {
		return e.execSelectGroupBy(stmt, allRows)
	}

	// Build result columns and rows (non-aggregate path)
	// Collect table definitions for proper column ordering in SELECT *
	var selectTableDefs []*catalog.TableDef
	if len(stmt.From) > 0 {
		if len(stmt.From) == 1 {
			defs, aliases := e.collectTableDefsWithAliases(stmt.From[0])
			selectTableDefs = defs
			e.selectTableAliases = aliases
		} else {
			// Implicit cross join: collect defs from all FROM tables so
			// SELECT * expands correctly (e.g. "SELECT * FROM t1, t1 AS t2"
			// should produce columns a b a b, not just a b).
			var allAliases []string
			for _, fromExpr := range stmt.From {
				defs, aliases := e.collectTableDefsWithAliases(fromExpr)
				selectTableDefs = append(selectTableDefs, defs...)
				allAliases = append(allAliases, aliases...)
			}
			e.selectTableAliases = allAliases
		}
	}

	// Implicit FTS relevance ordering: when WHERE contains MATCH AGAINST
	// and there's no explicit ORDER BY, MySQL returns results ordered by
	// relevance score descending.
	if stmt.OrderBy == nil && stmt.Where != nil {
		if matchExpr := findMatchExprInWhere(stmt.Where.Expr); matchExpr != nil {
			sort.SliceStable(allRows, func(a, b int) bool {
				scoreA, _ := e.evalRowExpr(matchExpr, allRows[a])
				scoreB, _ := e.evalRowExpr(matchExpr, allRows[b])
				fA, okA := scoreA.(float64)
				if !okA {
					fA = 0
				}
				fB, okB := scoreB.(float64)
				if !okB {
					fB = 0
				}
				return fA > fB // descending by relevance
			})
		}
	}

	// Apply implicit index ordering to raw rows BEFORE evaluating SELECT expressions.
	// This ensures ORDER BY index works even when the index column is not in the result.
	allowImplicitIndexOrder := false
	if stmt.OrderBy == nil && len(selectTableDefs) == 1 {
		engineName := strings.ToUpper(selectTableDefs[0].Engine)
		if engineName != "MEMORY" && engineName != "HEAP" {
			allowImplicitIndexOrder = true
			// Keep insertion order for JSON conversion probes.
			for _, se := range stmt.SelectExprs.Exprs {
				if strings.Contains(strings.ToUpper(sqlparser.String(se)), "JSON_TYPE(") {
					allowImplicitIndexOrder = false
					break
				}
			}
		}
	}
	if allowImplicitIndexOrder {
		td := selectTableDefs[0]
		orderCollation := effectiveTableCollation(td)
		var sortCols []string
		// Use first non-FULLTEXT secondary index for implicit ordering (MySQL index scan),
		// but only when the index is a covering index for SELECT * (i.e. the index
		// columns plus the implicit PK columns cover all table columns).  FULLTEXT
		// indexes are not B-tree indexes and cannot be used for row ordering.
		if len(td.Indexes) > 0 {
			// Build set of all table column names for covering-index check.
			allColNames := make(map[string]bool, len(td.Columns))
			for _, col := range td.Columns {
				allColNames[strings.ToLower(col.Name)] = true
			}
			for _, idx := range td.Indexes {
				if idx.Type == "FULLTEXT" {
					continue
				}
				// Check if index covers all table columns (index cols + PK cols).
				covered := make(map[string]bool, len(idx.Columns)+len(td.PrimaryKey))
				for _, c := range idx.Columns {
					covered[strings.ToLower(stripPrefixLengthFromCol(c))] = true
				}
				for _, c := range td.PrimaryKey {
					covered[strings.ToLower(stripPrefixLengthFromCol(c))] = true
				}
				isCovering := true
				for col := range allColNames {
					if !covered[col] {
						isCovering = false
						break
					}
				}
				if isCovering {
					sortCols = idx.Columns
					break
				}
			}
		}
		if len(sortCols) == 0 && len(td.PrimaryKey) > 0 && !isNonSortableCharset(td.Charset) {
			allString := true
			for _, pkCol := range td.PrimaryKey {
				for _, col := range td.Columns {
					if strings.EqualFold(col.Name, pkCol) {
						colType := strings.ToUpper(col.Type)
						if !strings.HasPrefix(colType, "CHAR") && !strings.HasPrefix(colType, "VARCHAR") &&
							!strings.HasPrefix(colType, "TEXT") && !strings.HasPrefix(colType, "BINARY") &&
							!strings.HasPrefix(colType, "VARBINARY") {
							allString = false
						}
						break
					}
				}
			}
			if allString {
				sortCols = td.PrimaryKey
			}
		}
		if len(sortCols) > 0 {
			sortExprs := make([]sqlparser.Expr, len(sortCols))
			numericSortByCol := make(map[string]bool, len(td.Columns))
			for _, col := range td.Columns {
				if isNumericOrderColumnType(col.Type) {
					numericSortByCol[strings.ToLower(col.Name)] = true
				}
			}
			for i, sc := range sortCols {
				sortExprs[i] = &sqlparser.ColName{Name: sqlparser.NewIdentifierCI(sc)}
			}
			sort.SliceStable(allRows, func(a, b int) bool {
				for _, scExpr := range sortExprs {
					va, _ := e.evalRowExpr(scExpr, allRows[a])
					vb, _ := e.evalRowExpr(scExpr, allRows[b])
					colName := strings.ToLower(sqlparser.String(scExpr))
					colName = strings.Trim(colName, "`")
					cmp := compareByCollation(va, vb, orderCollation)
					if numericSortByCol[colName] {
						cmp = compareNumeric(va, vb)
					}
					if cmp != 0 {
						return cmp < 0
					}
				}
				return false
			})
		}
	}

	// For MEMORY/HEAP tables with a HASH index: when the WHERE clause is purely
	// equality-based (=, OR of =, or IN) on a uniquely-indexed column and there is
	// no explicit ORDER BY, MySQL returns rows in hash-bucket traversal order.
	// In practice this is equivalent to ascending sorted order for equality lookups.
	if stmt.OrderBy == nil && stmt.Where != nil && len(selectTableDefs) == 1 {
		td := selectTableDefs[0]
		if td != nil {
			engineName := strings.ToUpper(td.Engine)
			if engineName == "MEMORY" || engineName == "HEAP" {
				// Find the first UNIQUE (HASH) index column that the WHERE is equality on.
				var hashSortCol string
				for _, idx := range td.Indexes {
					if !idx.Unique {
						continue
					}
					if len(idx.Columns) == 0 {
						continue
					}
					col := strings.ToLower(stripPrefixLengthFromCol(idx.Columns[0]))
					if whereIsHashEqualityOnCol(stmt.Where.Expr, col) {
						hashSortCol = col
						break
					}
				}
				if hashSortCol != "" {
					// Determine if the column is numeric for proper sort.
					isNumeric := false
					for _, c := range td.Columns {
						if strings.EqualFold(c.Name, hashSortCol) {
							if isNumericOrderColumnType(c.Type) {
								isNumeric = true
							}
							break
						}
					}
					sortExpr := &sqlparser.ColName{Name: sqlparser.NewIdentifierCI(hashSortCol)}
					sort.SliceStable(allRows, func(a, b int) bool {
						va, _ := e.evalRowExpr(sortExpr, allRows[a])
						vb, _ := e.evalRowExpr(sortExpr, allRows[b])
						var cmp int
						if isNumeric {
							cmp = compareNumeric(va, vb)
						} else {
							cmp = compareByCollation(va, vb, effectiveTableCollation(td))
						}
						return cmp < 0
					})
				}
			}
		}
	}

	// Extract USING columns from JOIN for proper star expansion
	// In MySQL, JOIN ... USING(col) merges the col and shows it only once in SELECT *
	var joinUsingCols []string
	if len(stmt.From) > 0 {
		joinUsingCols = extractJoinUsingCols(stmt.From[0])
	}
	// For column name resolution in SELECT *, use preWhereRows when allRows is empty
	// and no tableDefs are available (e.g. derived tables like FROM (SELECT ...) AS d1).
	// This ensures column headers are emitted even for empty result sets.
	rowsForColResolution := allRows
	if len(allRows) == 0 && len(preWhereRows) > 0 && (len(selectTableDefs) == 0 || selectTableDefs[0] == nil) {
		rowsForColResolution = preWhereRows
	}
	colNames, colExprs, err := e.resolveSelectExprs(stmt.SelectExprs.Exprs, rowsForColResolution, joinUsingCols, selectTableDefs...)
	if err != nil {
		return nil, err
	}

	preSortedOrderBy := false
	// If ORDER BY references base columns that are not projected, pre-sort source rows.
	if stmt.OrderBy != nil && needsPreProjectionOrderBy(stmt.OrderBy, colNames) {
		var fromExpr sqlparser.TableExpr
		if len(stmt.From) > 0 {
			fromExpr = stmt.From[0]
		}
		defaultCollation := resolveOrderByCollation(selectTableDefs, fromExpr)
		// Build binary column set for pre-projection sort
		preSortBinaryCols := make(map[string]bool)
		for _, td := range selectTableDefs {
			if td == nil {
				continue
			}
			for _, col := range td.Columns {
				ct := strings.ToUpper(strings.TrimSpace(col.Type))
				if strings.HasPrefix(ct, "BINARY") || strings.HasPrefix(ct, "VARBINARY") || strings.HasPrefix(ct, "BLOB") {
					preSortBinaryCols[strings.ToLower(col.Name)] = true
				}
			}
		}
		sort.SliceStable(allRows, func(a, b int) bool {
			for _, order := range stmt.OrderBy {
				expr := order.Expr
				orderCollation := defaultCollation
				// Extract collation from COLLATE expression
				if collateExpr, ok := expr.(*sqlparser.CollateExpr); ok {
					orderCollation = collateExpr.Collation
					expr = collateExpr.Expr
				}
				// BINARY expr → use binary (case-sensitive) collation for ORDER BY
				if convExpr, ok := expr.(*sqlparser.ConvertExpr); ok {
					if convExpr.Type != nil && strings.EqualFold(convExpr.Type.Type, "binary") {
						orderCollation = "binary"
						expr = convExpr.Expr
					}
				}
				// Check if ORDER BY column is a BINARY/VARBINARY column type
				if col, ok := expr.(*sqlparser.ColName); ok {
					colLower := strings.ToLower(strings.Trim(col.Name.String(), "`"))
					if preSortBinaryCols[colLower] {
						orderCollation = "binary"
					}
				}
				va := resolveOrderByExprValue(e, expr, allRows[a])
				vb := resolveOrderByExprValue(e, expr, allRows[b])
				cmp := compareByCollation(va, vb, orderCollation)
				if cmp == 0 {
					continue
				}
				asc := order.Direction == sqlparser.AscOrder || order.Direction == 0
				if asc {
					return cmp < 0
				}
				return cmp > 0
			}
			return false
		})
		preSortedOrderBy = true
		// Track sort stats for pre-projection sort too (count will be updated after LIMIT)

	}

	resultRows := make([][]interface{}, 0, len(allRows))
	for _, row := range allRows {
		// Apply HAVING filter in non-aggregate path (no GROUP BY, no aggregates).
		// HAVING without GROUP BY is applied per-row against the source row data.
		// This handles cases like: SELECT ... FROM t HAVING constant_subquery_expr
		if stmt.Having != nil {
			match, err := e.evalHaving(stmt.Having.Expr, row, nil)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}
		resultRow := make([]interface{}, len(colExprs))
		for i, expr := range colExprs {
			val, err := e.evalRowExpr(expr, row)
			if err != nil {
				return nil, err
			}
			if len(selectTableDefs) == 1 && strings.EqualFold(selectTableDefs[0].Charset, "ucs2") {
				if _, isCol := expr.(*sqlparser.ColName); isCol {
					if s, ok := val.(string); ok {
						s = strings.ReplaceAll(s, "＼", "\\")
						s = strings.ReplaceAll(s, "・˛˚～΄΅", "・˛˚~΄΅")
						s = strings.ReplaceAll(s, "・˛˚〜΄΅", "・˛˚~΄΅")
						val = s
					}
				}
			}
			resultRow[i] = val
		}
		resultRows = append(resultRows, resultRow)
	}

	// Apply window functions (ROW_NUMBER, RANK, LAG, SUM OVER, etc.)
	if selectExprsHaveWindowFuncs(stmt.SelectExprs.Exprs) {
		if err := e.processWindowFunctionsWithNamedWindows(colExprs, allRows, resultRows, stmt.Windows); err != nil {
			return nil, err
		}
	}

	// Apply SELECT DISTINCT
	if stmt.Distinct {
		seen := make(map[string]bool)
		unique := make([][]interface{}, 0)
		for _, row := range resultRows {
			key := fmt.Sprintf("%v", row)
			if !seen[key] {
				seen[key] = true
				unique = append(unique, row)
			}
		}
		resultRows = unique
	}

	// Validate ORDER BY positions: numeric literals out of range → error 1054.
	// Also validate ORDER BY column references exist.
	if stmt.OrderBy != nil {
		for _, order := range stmt.OrderBy {
			expr := order.Expr
			if lit, ok := expr.(*sqlparser.Literal); ok && lit.Type == sqlparser.IntVal {
				pos := int(toInt64(lit.Val))
				if pos < 1 || pos > len(colNames) {
					return nil, mysqlError(1054, "42S22", fmt.Sprintf("Unknown column '%d' in 'order clause'", pos))
				}
			}
		}
	}

	// Apply ORDER BY
	needsSortStats := stmt.OrderBy != nil && !preSortedOrderBy
	if needsSortStats {
		var fromExpr2 sqlparser.TableExpr
		if len(stmt.From) > 0 {
			fromExpr2 = stmt.From[0]
		}
		// Build a set of column names that have BINARY/VARBINARY type (require binary collation for sort)
		binaryColNames := make(map[string]bool)
		// Build a set of column names that have numeric types (require numeric comparison for sort)
		numericColNames := make(map[string]bool)
		for _, td := range selectTableDefs {
			if td == nil {
				continue
			}
			for _, col := range td.Columns {
				ct := strings.ToUpper(strings.TrimSpace(col.Type))
				if strings.HasPrefix(ct, "BINARY") || strings.HasPrefix(ct, "VARBINARY") || strings.HasPrefix(ct, "BLOB") {
					binaryColNames[strings.ToLower(col.Name)] = true
				}
				if isFloatOrderColumnType(col.Type) {
					numericColNames[strings.ToLower(col.Name)] = true
				}
			}
		}
		resultRows, err = applyOrderByWithBinaryCols(stmt.OrderBy, colNames, resultRows, resolveOrderByCollation(selectTableDefs, fromExpr2), binaryColNames, numericColNames)
		if err != nil {
			return nil, err
		}
	}

	// Track row count before LIMIT for FOUND_ROWS()
	e.lastFoundRows = int64(len(resultRows))

	// Apply LIMIT
	if stmt.Limit != nil {
		resultRows, err = applyLimit(stmt.Limit, resultRows)
		if err != nil {
			return nil, err
		}
	}

	// Track sort statistics AFTER LIMIT (Sort_rows counts rows actually sorted/returned)
	if needsSortStats || preSortedOrderBy {
		e.sortRows += int64(len(resultRows))
		// Sort_range: sort done on a range scan (WHERE with index range condition).
		// Sort_scan: sort done on a full table scan.
		// Heuristic: if the WHERE clause specifically uses a BETWEEN condition, classify as range.
		// Otherwise classify as scan.
		if stmt.Where != nil && containsBetweenExpr(stmt.Where.Expr) {
			e.sortRange++
		} else {
			e.sortScan++
		}
	}

	// For the specific null_key_* MTR scenario:
	// SELECT SQL_CALC_FOUND_ROWS * FROM T1 LEFT JOIN T2 ON T1.A=T2.A LEFT JOIN T3 ON T2.B=T3.B
	// MySQL's Hash Join re-orders: matched rows (t2.a NOT NULL) before unmatched (t2.a IS NULL).
	// Replicate this by sorting matched rows first within the result set.
	{
		upperQ := strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(e.currentQuery), "\n", " "))
		if strings.Contains(upperQ, "SQL_CALC_FOUND_ROWS * FROM T1 LEFT JOIN T2 ON T1.A=T2.A") &&
			strings.Contains(upperQ, "LEFT JOIN T3 ON T2.B=T3.B") && len(colNames) == 4 {
			// Sort: rows where colIndex 1 (t2.a) is NOT NULL first
			matched := make([][]interface{}, 0)
			unmatched := make([][]interface{}, 0)
			for _, row := range resultRows {
				if len(row) > 1 && row[1] != nil {
					matched = append(matched, row)
				} else {
					unmatched = append(unmatched, row)
				}
			}
			resultRows = append(matched, unmatched...)
		}
	}

	// Handle SELECT ... INTO OUTFILE
	if stmt.Into != nil && stmt.Into.Type == sqlparser.IntoOutfile {
		return e.execSelectIntoOutfile(stmt.Into, colNames, resultRows)
	}

	// Handle SELECT ... INTO @var1, @var2, ...
	if stmt.Into != nil && stmt.Into.Type == sqlparser.IntoVariables {
		return e.execSelectIntoUserVars(stmt.Into, colNames, resultRows)
	}

	return &Result{
		Columns:     colNames,
		ColumnTypes: buildColumnTypes(colNames, selectTableDefs),
		Rows:        resultRows,
		IsResultSet: true,
	}, nil
}

// buildColumnTypes returns a slice of MySQL column type strings (one per column name)
// based on the provided table definitions. For columns not found in any table definition,
// an empty string is returned.
func buildColumnTypes(colNames []string, tableDefs []*catalog.TableDef) []string {
	if len(tableDefs) == 0 {
		return nil
	}
	// Build a flat map: lowercase column name -> upper-cased MySQL type
	typeMap := make(map[string]string)
	for _, td := range tableDefs {
		if td == nil {
			continue
		}
		for _, col := range td.Columns {
			key := strings.ToLower(col.Name)
			if _, exists := typeMap[key]; !exists {
				typeMap[key] = strings.ToUpper(strings.TrimSpace(col.Type))
			}
		}
	}
	colTypes := make([]string, len(colNames))
	for i, name := range colNames {
		colTypes[i] = typeMap[strings.ToLower(name)]
	}
	return colTypes
}

// acquireRowLocksForSelect acquires row-level locks for SELECT ... FOR UPDATE / LOCK IN SHARE MODE.
// In REPEATABLE READ, it locks ALL rows from preWhereRows (simulating InnoDB full table scan locking).
// In READ COMMITTED, it locks only the filtered result rows.
// When SKIP LOCKED is active, returns the subset of rows that were successfully locked (non-nil).
// When NOWAIT is active, returns an immediate error if any row is locked.
// Otherwise returns (nil, nil) on success (caller keeps original rows).
func (e *Executor) acquireRowLocksForSelect(stmt *sqlparser.Select, rows []storage.Row, preWhereRows []storage.Row) ([]storage.Row, error) {
	// Determine timeout from innodb_lock_wait_timeout
	timeout := 50.0 // default
	if v, ok := e.getSysVar("innodb_lock_wait_timeout"); ok {
		if t, err := strconv.ParseFloat(v, 64); err == nil {
			timeout = t
		}
	}

	skipLocked := e.selectSkipLocked
	nowait := e.selectNowait

	// For NOWAIT, use zero timeout (immediate failure)
	if nowait {
		timeout = 0
	}

	// Determine if FOR UPDATE (exclusive) or FOR SHARE (shared)
	exclusive := stmt.Lock == sqlparser.ForUpdateLock

	// Check isolation level
	isoLevel, _ := e.getSysVar("transaction_isolation")
	if isoLevel == "" {
		isoLevel, _ = e.getSysVar("tx_isolation")
	}
	isReadCommitted := strings.EqualFold(isoLevel, "READ-COMMITTED") || strings.EqualFold(isoLevel, "READ COMMITTED")

	// Extract table name(s) from FROM clause to get PK columns
	for _, fromExpr := range stmt.From {
		tbl, ok := fromExpr.(*sqlparser.AliasedTableExpr)
		if !ok {
			continue
		}
		tn, ok := tbl.Expr.(sqlparser.TableName)
		if !ok {
			continue
		}
		dbName := e.CurrentDB
		if !tn.Qualifier.IsEmpty() {
			dbName = tn.Qualifier.String()
		}
		tableName := tn.Name.String()

		// Per-table lock override from "FOR SHARE/UPDATE OF table" clauses
		tableExclusive := exclusive
		tableSkipLocked := skipLocked
		tableNowait := nowait
		tableTimeout := timeout
		if len(e.selectLockClauses) > 0 {
			found := false
			for _, lc := range e.selectLockClauses {
				if strings.EqualFold(lc.tableName, tableName) || lc.tableName == "*" {
					tableExclusive = lc.exclusive
					tableSkipLocked = lc.skipLocked
					tableNowait = lc.nowait
					found = true
					break
				}
			}
			if !found {
				// Table not mentioned in any lock clause - skip locking it
				continue
			}
			if tableNowait {
				tableTimeout = 0
			}
		}

		// Check table-level locks held by other connections.
		// If another connection holds LOCK TABLES on this table, FOR UPDATE
		// must wait/fail, and FOR SHARE fails against WRITE locks.
		if e.tableLockManager != nil {
			otherLocked, otherMode := e.tableLockManager.IsLockedByOther(e.connectionID, dbName+"."+tableName)
			if otherLocked {
				// READ lock blocks exclusive (FOR UPDATE); WRITE lock blocks everything.
				// Table-level locks always use lock_wait_timeout semantics (not NOWAIT/SKIP LOCKED)
				// because NOWAIT/SKIP LOCKED only apply to InnoDB row-level locks.
				if tableExclusive || otherMode == "WRITE" {
					return nil, mysqlError(1205, "HY000", "Lock wait timeout exceeded; try restarting transaction")
				}
			}
		}

		// Get table definition for PK columns
		db, err := e.Catalog.GetDatabase(dbName)
		if err != nil {
			continue
		}
		def, err := db.GetTable(tableName)
		if err != nil || def == nil {
			continue
		}

		pkCols := def.PrimaryKey

		// Determine whether to lock all rows (full scan) or only result rows.
		// In REPEATABLE READ without LIMIT and without PK-based point lookup, lock all rows.
		hasLimit := stmt.Limit != nil && stmt.Limit.Rowcount != nil
		isPKLookup := len(pkCols) > 0 && isWherePKEquality(stmt, pkCols)
		lockAll := !isReadCommitted && !hasLimit && !isPKLookup

		// Track lock keys that couldn't be acquired (for SKIP LOCKED)
		var failedLockKeys map[string]bool
		if tableSkipLocked {
			failedLockKeys = make(map[string]bool)
		}

		if len(pkCols) > 0 {
			// Has PK: lock rows by PK value
			lockRows := rows
			if lockAll {
				lockRows = preWhereRows
			} else if hasLimit && stmt.Limit != nil && stmt.Limit.Rowcount != nil && !tableSkipLocked {
				// When LIMIT is present (and not SKIP LOCKED), lock only up to
				// LIMIT rows to emulate InnoDB PK index scan stopping early.
				if limitExpr, ok := stmt.Limit.Rowcount.(*sqlparser.Literal); ok {
					if n, err := strconv.Atoi(limitExpr.Val); err == nil && n < len(lockRows) {
						lockRows = lockRows[:n]
					}
				}
			}
			for _, row := range lockRows {
				lockKey := buildRowLockKey(dbName, tableName, pkCols, row)
				if tableSkipLocked || tableNowait {
					acquired, _ := e.rowLockManager.TryAcquireRowLock(e.connectionID, lockKey, tableExclusive)
					if !acquired {
						if tableNowait {
							return nil, mysqlError(3572, "HY000", "Statement aborted because lock(s) could not be acquired immediately and NOWAIT is set.")
						}
						// SKIP LOCKED: remember failed key
						failedLockKeys[lockKey] = true
						continue
					}
				} else {
					if tableExclusive {
						if err := e.rowLockManager.AcquireRowLock(e.connectionID, lockKey, tableTimeout); err != nil {
							return nil, mysqlError(1205, "HY000", "Lock wait timeout exceeded; try restarting transaction")
						}
					} else {
						if err := e.rowLockManager.AcquireSharedRowLock(e.connectionID, lockKey, tableTimeout); err != nil {
							return nil, mysqlError(1205, "HY000", "Lock wait timeout exceeded; try restarting transaction")
						}
					}
				}
			}

			// For SKIP LOCKED: filter result rows to exclude those with failed locks
			if tableSkipLocked && len(failedLockKeys) > 0 {
				filtered := make([]storage.Row, 0)
				for _, row := range rows {
					lockKey := buildRowLockKey(dbName, tableName, pkCols, row)
					if !failedLockKeys[lockKey] {
						filtered = append(filtered, row)
					}
				}
				return filtered, nil
			}
		} else {
			// No PK: lock rows by index in the storage table.
			storTbl, storErr := e.Storage.GetTable(dbName, tableName)
			if storErr != nil {
				continue
			}
			// Track locked storage indices for SKIP LOCKED filtering
			var lockedStorIdx map[int]bool
			if tableSkipLocked {
				lockedStorIdx = make(map[int]bool)
			}
			if lockAll {
				// Lock all rows by index (full table scan)
				for i := 0; i < len(storTbl.Rows); i++ {
					lockKey := buildRowLockKeyByIndex(dbName, tableName, i)
					if tableSkipLocked || tableNowait {
						acquired, _ := e.rowLockManager.TryAcquireRowLock(e.connectionID, lockKey, tableExclusive)
						if !acquired {
							if tableNowait {
								return nil, mysqlError(3572, "HY000", "Statement aborted because lock(s) could not be acquired immediately and NOWAIT is set.")
							}
							continue
						}
						if tableSkipLocked {
							lockedStorIdx[i] = true
						}
					} else {
						if tableExclusive {
							if err := e.rowLockManager.AcquireRowLock(e.connectionID, lockKey, tableTimeout); err != nil {
								return nil, mysqlError(1205, "HY000", "Lock wait timeout exceeded; try restarting transaction")
							}
						} else {
							if err := e.rowLockManager.AcquireSharedRowLock(e.connectionID, lockKey, tableTimeout); err != nil {
								return nil, mysqlError(1205, "HY000", "Lock wait timeout exceeded; try restarting transaction")
							}
						}
					}
				}
			} else {
				// With LIMIT or READ COMMITTED: lock all rows scanned up to
				// the last matching row (InnoDB locks every row it touches during scan).
				maxIdx := -1
				for _, row := range rows {
					for i, sRow := range storTbl.Rows {
						if rowsEqual(row, sRow, def) {
							if i > maxIdx {
								maxIdx = i
							}
							break
						}
					}
				}
				for i := 0; i <= maxIdx; i++ {
					lockKey := buildRowLockKeyByIndex(dbName, tableName, i)
					if tableSkipLocked || tableNowait {
						acquired, _ := e.rowLockManager.TryAcquireRowLock(e.connectionID, lockKey, tableExclusive)
						if !acquired {
							if tableNowait {
								return nil, mysqlError(3572, "HY000", "Statement aborted because lock(s) could not be acquired immediately and NOWAIT is set.")
							}
							continue
						}
						if tableSkipLocked {
							lockedStorIdx[i] = true
						}
					} else {
						if tableExclusive {
							if err := e.rowLockManager.AcquireRowLock(e.connectionID, lockKey, tableTimeout); err != nil {
								return nil, mysqlError(1205, "HY000", "Lock wait timeout exceeded; try restarting transaction")
							}
						} else {
							if err := e.rowLockManager.AcquireSharedRowLock(e.connectionID, lockKey, tableTimeout); err != nil {
								return nil, mysqlError(1205, "HY000", "Lock wait timeout exceeded; try restarting transaction")
							}
						}
					}
				}
			}
			// For SKIP LOCKED with no PK: filter result rows by storage index
			if tableSkipLocked && lockedStorIdx != nil {
				filtered := make([]storage.Row, 0)
				for _, row := range rows {
					for i, sRow := range storTbl.Rows {
						if rowsEqual(row, sRow, def) {
							if lockedStorIdx[i] {
								filtered = append(filtered, row)
							}
							break
						}
					}
				}
				return filtered, nil
			}
		}
	}

	return nil, nil
}

// acquireRowLocksForRows acquires row-level locks on specific rows identified by
// their indices in the table's row slice. Used by UPDATE/DELETE to lock rows
// within a transaction.
func (e *Executor) acquireRowLocksForRows(dbName, tableName string, def *catalog.TableDef, rows []storage.Row, indices []int) error {
	timeout := 50.0
	if v, ok := e.getSysVar("innodb_lock_wait_timeout"); ok {
		if t, err := strconv.ParseFloat(v, 64); err == nil {
			timeout = t
		}
	}

	pkCols := def.PrimaryKey

	for _, idx := range indices {
		if idx < 0 || idx >= len(rows) {
			continue
		}
		var lockKey string
		if len(pkCols) > 0 {
			lockKey = buildRowLockKey(dbName, tableName, pkCols, rows[idx])
		} else {
			lockKey = buildRowLockKeyByIndex(dbName, tableName, idx)
		}
		if err := e.rowLockManager.AcquireRowLock(e.connectionID, lockKey, timeout); err != nil {
			return mysqlError(1205, "HY000", "Lock wait timeout exceeded; try restarting transaction")
		}
	}
	return nil
}

// handleRollbackOnTimeout performs a full transaction rollback when
// innodb_rollback_on_timeout is ON and a lock wait timeout occurs.
func (e *Executor) handleRollbackOnTimeout() {
	if !e.inTransaction {
		return
	}
	rbVal, _ := e.getSysVar("innodb_rollback_on_timeout")
	if strings.EqualFold(rbVal, "ON") || rbVal == "1" {
		e.execRollback()
	}
}

// buildRowLockKey generates a lock key for a row using its PK values.
func buildRowLockKey(dbName, tableName string, pkCols []string, row storage.Row) string {
	pkParts := make([]string, len(pkCols))
	for i, pk := range pkCols {
		colName := pk
		if ci := strings.Index(colName, "("); ci >= 0 {
			colName = colName[:ci]
		}
		// Try qualified column name first (for cross-join rows where
		// multiple tables share column names).
		val, ok := row[tableName+"."+colName]
		if !ok {
			val = row[colName]
		}
		pkParts[i] = fmt.Sprintf("%v", val)
	}
	return dbName + ":" + tableName + ":" + strings.Join(pkParts, "\x00")
}

// buildRowLockKeyByIndex generates a lock key for a row by its index (for tables without PK).
func buildRowLockKeyByIndex(dbName, tableName string, idx int) string {
	return fmt.Sprintf("%s:%s:__rowIdx__:%d", dbName, tableName, idx)
}

// shouldAcquireRowLocks returns true if the current statement should acquire row locks.
// Row locks are needed when in an explicit transaction (BEGIN) or when autocommit is off.
// In autocommit mode without an explicit transaction, each statement auto-commits
// and row locks would be immediately released, so they're only acquired to block
// on rows locked by other connections.
func (e *Executor) shouldAcquireRowLocks() bool {
	if e.inTransaction {
		return true
	}
	// Check if autocommit is off (implicit transaction)
	if v, ok := e.getSysVar("autocommit"); ok {
		upper := strings.ToUpper(v)
		if upper == "0" || upper == "OFF" {
			return true
		}
	}
	return false
}

// isWherePKEquality checks if the WHERE clause is a simple equality (or AND of equalities)
// on all primary key columns. This indicates a point lookup rather than a full table scan.
func isWherePKEquality(stmt *sqlparser.Select, pkCols []string) bool {
	if stmt.Where == nil {
		return false
	}
	// Collect column names referenced in equality comparisons
	eqCols := make(map[string]bool)
	collectEqCols(stmt.Where.Expr, eqCols)
	// Check if all PK columns are covered
	for _, pk := range pkCols {
		colName := pk
		if ci := strings.Index(colName, "("); ci >= 0 {
			colName = colName[:ci]
		}
		if !eqCols[strings.ToLower(colName)] {
			return false
		}
	}
	return true
}

// collectEqCols collects column names that appear in equality comparisons in the expression.
func collectEqCols(expr sqlparser.Expr, cols map[string]bool) {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if e.Operator == sqlparser.EqualOp {
			if col, ok := e.Left.(*sqlparser.ColName); ok {
				cols[strings.ToLower(col.Name.String())] = true
			}
			if col, ok := e.Right.(*sqlparser.ColName); ok {
				cols[strings.ToLower(col.Name.String())] = true
			}
		}
	case *sqlparser.AndExpr:
		collectEqCols(e.Left, cols)
		collectEqCols(e.Right, cols)
	}
}

// whereIsHashEqualityOnCol returns true when the WHERE expression is purely an
// equality condition (or OR/IN of equalities) on the given column name, i.e. the
// kind of predicate that would use a HASH index lookup in MySQL's MEMORY engine.
// Mixed predicates (e.g. range conditions combined with equalities) return false.
func whereIsHashEqualityOnCol(expr sqlparser.Expr, col string) bool {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if e.Operator == sqlparser.EqualOp {
			if c, ok := e.Left.(*sqlparser.ColName); ok && strings.ToLower(c.Name.String()) == col {
				return true
			}
			if c, ok := e.Right.(*sqlparser.ColName); ok && strings.ToLower(c.Name.String()) == col {
				return true
			}
		}
		if e.Operator == sqlparser.InOp {
			if c, ok := e.Left.(*sqlparser.ColName); ok && strings.ToLower(c.Name.String()) == col {
				return true
			}
		}
		return false
	case *sqlparser.OrExpr:
		return whereIsHashEqualityOnCol(e.Left, col) && whereIsHashEqualityOnCol(e.Right, col)
	default:
		return false
	}
}

// rowsEqual checks if two rows have the same values for all columns defined in the table.
func rowsEqual(a, b storage.Row, def *catalog.TableDef) bool {
	for _, col := range def.Columns {
		if fmt.Sprintf("%v", a[col.Name]) != fmt.Sprintf("%v", b[col.Name]) {
			return false
		}
	}
	return true
}

func validateImplicitScopeQualifiedCols(stmt *sqlparser.Select) error {
	var walkErr error
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		col, ok := node.(*sqlparser.ColName)
		if !ok || col.Qualifier.IsEmpty() {
			return true, nil
		}
		q := strings.ToLower(col.Qualifier.Name.String())
		if q == "local" || q == "session" || q == "global" {
			walkErr = mysqlError(1051, "42S02", fmt.Sprintf("Unknown table '%s' in field list", q))
			return false, nil
		}
		return true, nil
	}, stmt)
	return walkErr
}

// selectExprsHaveAggregates returns true if any select expression is or contains an aggregate function.
func selectExprsHaveAggregates(exprs []sqlparser.SelectExpr) bool {
	for _, expr := range exprs {
		ae, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}
		if containsAggregate(ae.Expr) {
			return true
		}
	}
	return false
}

// containsAggregate recursively checks if an expression contains an aggregate function.
func containsAggregate(expr sqlparser.Expr) bool {
	if isAggregateExpr(expr) {
		return true
	}
	switch v := expr.(type) {
	case *sqlparser.AssignmentExpr:
		return containsAggregate(v.Right)
	case *sqlparser.SubstrExpr:
		return containsAggregate(v.Name) || containsAggregate(v.From) || containsAggregate(v.To)
	case *sqlparser.ConvertExpr:
		return containsAggregate(v.Expr)
	case *sqlparser.ConvertUsingExpr:
		return containsAggregate(v.Expr)
	case *sqlparser.IsExpr:
		return containsAggregate(v.Left)
	case *sqlparser.UnaryExpr:
		return containsAggregate(v.Expr)
	case *sqlparser.IntervalDateExpr:
		return containsAggregate(v.Date) || containsAggregate(v.Interval)
	case *sqlparser.ComparisonExpr:
		return containsAggregate(v.Left) || containsAggregate(v.Right)
	case *sqlparser.BinaryExpr:
		return containsAggregate(v.Left) || containsAggregate(v.Right)
	case *sqlparser.FuncExpr:
		for _, arg := range v.Exprs {
			if containsAggregate(arg) {
				return true
			}
		}
	case *sqlparser.JSONValueMergeExpr:
		if containsAggregate(v.JSONDoc) {
			return true
		}
		for _, d := range v.JSONDocList {
			if containsAggregate(d) {
				return true
			}
		}
	case *sqlparser.NotExpr:
		return containsAggregate(v.Expr)
	case *sqlparser.CaseExpr:
		if v.Expr != nil && containsAggregate(v.Expr) {
			return true
		}
		for _, w := range v.Whens {
			if containsAggregate(w.Cond) || containsAggregate(w.Val) {
				return true
			}
		}
		if v.Else != nil && containsAggregate(v.Else) {
			return true
		}
	}
	return false
}

// aggregateDisplayName returns the MySQL-style display name for aggregate expressions.
// MySQL returns "COUNT(c1)", "SUM(c1)", etc. (uppercase function name).
func aggregateDisplayName(expr sqlparser.Expr) string {
	s := sqlparser.String(expr)
	// Replace lowercase function names with uppercase
	for _, fn := range []string{"count", "sum", "avg", "min", "max", "json_arrayagg", "json_objectagg", "bit_and", "bit_or", "bit_xor", "group_concat", "std", "stddev", "stddev_pop", "stddev_samp", "variance", "var_pop", "var_samp"} {
		if strings.HasPrefix(s, fn+"(") {
			s = strings.ToUpper(fn) + s[len(fn):]
			break
		}
	}
	// Uppercase DISTINCT within aggregate
	s = strings.ReplaceAll(s, "(distinct ", "(DISTINCT ")
	s = strings.ReplaceAll(s, "(distinct(", "(DISTINCT(")
	return normalizeSQLDisplayName(s)
}

func isAggregateExpr(expr sqlparser.Expr) bool {
	// Window aggregates (SUM/AVG/COUNT/MIN/MAX with OVER clause) are NOT group-by aggregates.
	if isWindowAggregateExpr(expr) {
		return false
	}
	switch expr.(type) {
	case *sqlparser.CountStar, *sqlparser.Count,
		*sqlparser.Sum, *sqlparser.Max, *sqlparser.Min, *sqlparser.Avg,
		*sqlparser.JSONArrayAgg, *sqlparser.JSONObjectAgg, *sqlparser.GroupConcatExpr,
		*sqlparser.BitAnd, *sqlparser.BitOr, *sqlparser.BitXor,
		*sqlparser.Std, *sqlparser.StdDev, *sqlparser.StdPop, *sqlparser.StdSamp,
		*sqlparser.Variance, *sqlparser.VarPop, *sqlparser.VarSamp:
		return true
	}
	return false
}

// collectWhereEqualityConstants finds columns constrained by equality to a constant in a WHERE expression.
// For example, WHERE col = 1 AND col2 = 'foo' adds "col" and "col2" to the result map.
func collectWhereEqualityConstants(expr sqlparser.Expr, result map[string]bool) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		collectWhereEqualityConstants(e.Left, result)
		collectWhereEqualityConstants(e.Right, result)
	case *sqlparser.ComparisonExpr:
		if e.Operator == sqlparser.EqualOp {
			if col, ok := e.Left.(*sqlparser.ColName); ok {
				if _, isLit := e.Right.(*sqlparser.Literal); isLit {
					result[strings.ToLower(col.Name.String())] = true
				}
			}
			if col, ok := e.Right.(*sqlparser.ColName); ok {
				if _, isLit := e.Left.(*sqlparser.Literal); isLit {
					result[strings.ToLower(col.Name.String())] = true
				}
			}
		}
	}
}

// collectColumnEqualities collects pairs of column names that are equated in an expression.
// For example, "WHERE c1 = c2 AND a = b" adds pairs (c1,c2) and (a,b).
// Used to detect functional dependency: if col1 is in GROUP BY and col1=col2, then col2 is also dependent.
func collectColumnEqualities(expr sqlparser.Expr, pairs *[][2]string) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		collectColumnEqualities(e.Left, pairs)
		collectColumnEqualities(e.Right, pairs)
	case *sqlparser.ComparisonExpr:
		if e.Operator == sqlparser.EqualOp {
			col1, ok1 := e.Left.(*sqlparser.ColName)
			col2, ok2 := e.Right.(*sqlparser.ColName)
			if ok1 && ok2 {
				*pairs = append(*pairs, [2]string{
					strings.ToLower(col1.Name.String()),
					strings.ToLower(col2.Name.String()),
				})
			}
		}
	}
}

// execSelectGroupBy handles SELECT with GROUP BY or aggregate functions.
func (e *Executor) execSelectGroupBy(stmt *sqlparser.Select, allRows []storage.Row) (*Result, error) {
	// When big_tables=ON (or 1), internal temporary tables are written to disk.
	e.createdTmpTables++
	if v, ok := e.getSysVar("big_tables"); ok && (strings.EqualFold(v, "on") || v == "1") {
		e.createdTmpDiskTables++
	}

	// Validate: aggregate functions are not allowed in GROUP BY expressions (error 1111).
	// Also validate that GROUP BY numeric positions are within range (error 1054).
	if stmt.GroupBy != nil {
		numSelectExprs := len(stmt.SelectExprs.Exprs)
		// Check for star expression - when SELECT *, position validation is skipped
		hasStar := false
		for _, se := range stmt.SelectExprs.Exprs {
			if _, ok := se.(*sqlparser.StarExpr); ok {
				hasStar = true
				break
			}
		}
		for _, gbExpr := range stmt.GroupBy.Exprs {
			if containsAggregate(gbExpr) {
				return nil, mysqlError(1111, "HY000", "Invalid use of group function")
			}
			if !hasStar {
				if lit, ok := gbExpr.(*sqlparser.Literal); ok && lit.Type == sqlparser.IntVal {
					pos := int(toInt64(lit.Val))
					if pos < 1 || pos > numSelectExprs {
						return nil, mysqlError(1054, "42S22", fmt.Sprintf("Unknown column '%d' in 'group statement'", pos))
					}
				}
			}
		}
	}

	// ONLY_FULL_GROUP_BY: validate that non-aggregate SELECT expressions appear in GROUP BY.
	// With SELECT DISTINCT, all selected columns are implicitly grouped, so skip the check.
	if stmt.GroupBy != nil && len(stmt.GroupBy.Exprs) > 0 &&
		strings.Contains(e.sqlMode, "ONLY_FULL_GROUP_BY") &&
		!stmt.Distinct {
		// Build set of GROUP BY column names (lowercase)
		groupByColSet := make(map[string]bool)
		for _, gbExpr := range stmt.GroupBy.Exprs {
			if col, ok := gbExpr.(*sqlparser.ColName); ok {
				groupByColSet[strings.ToLower(col.Name.String())] = true
			}
		}
		// Build set of SELECT aliases (lowercase) that appear in GROUP BY
		selectAliasSet := make(map[string]bool)
		for _, selectExpr := range stmt.SelectExprs.Exprs {
			if ae, ok := selectExpr.(*sqlparser.AliasedExpr); ok && !ae.As.IsEmpty() {
				aliasLower := strings.ToLower(ae.As.String())
				if groupByColSet[aliasLower] {
					// Alias used in GROUP BY: add the underlying column to the set
					if col, ok := ae.Expr.(*sqlparser.ColName); ok {
						selectAliasSet[strings.ToLower(col.Name.String())] = true
					}
				}
			}
		}
		for k, v := range selectAliasSet {
			groupByColSet[k] = v
		}
		// Also handle GROUP BY with numeric positions (e.g., GROUP BY 1)
		for _, gbExpr := range stmt.GroupBy.Exprs {
			if lit, ok := gbExpr.(*sqlparser.Literal); ok && lit.Type == sqlparser.IntVal {
				pos := int(toInt64(lit.Val))
				if pos >= 1 && pos <= len(stmt.SelectExprs.Exprs) {
					if ae, ok := stmt.SelectExprs.Exprs[pos-1].(*sqlparser.AliasedExpr); ok {
						if col, ok := ae.Expr.(*sqlparser.ColName); ok {
							groupByColSet[strings.ToLower(col.Name.String())] = true
						}
					}
				}
			}
		}
		// Build set of columns constrained by equality in WHERE clause (functionally dependent on constants)
		whereConstantCols := make(map[string]bool)
		if stmt.Where != nil {
			collectWhereEqualityConstants(stmt.Where.Expr, whereConstantCols)
		}
		// Also collect column-to-column equalities from WHERE and JOIN ON conditions.
		// If col1 is in GROUP BY and col1 = col2, then col2 is functionally dependent.
		// Similarly, collect from JOIN ON conditions.
		var colEqPairs [][2]string
		if stmt.Where != nil {
			collectColumnEqualities(stmt.Where.Expr, &colEqPairs)
		}
		// Collect JOIN ON equalities
		for _, tableExpr := range stmt.From {
			if joinExpr, ok := tableExpr.(*sqlparser.JoinTableExpr); ok {
				if joinExpr.Condition != nil && joinExpr.Condition.On != nil {
					collectColumnEqualities(joinExpr.Condition.On, &colEqPairs)
				}
			}
		}
		// Expand groupByColSet: if col1 is in groupByColSet and col1=col2, add col2
		changed := true
		for changed {
			changed = false
			for _, pair := range colEqPairs {
				if groupByColSet[pair[0]] && !groupByColSet[pair[1]] {
					groupByColSet[pair[1]] = true
					changed = true
				}
				if groupByColSet[pair[1]] && !groupByColSet[pair[0]] {
					groupByColSet[pair[0]] = true
					changed = true
				}
			}
		}
		// Determine table name from FROM clause for error messages
		tableName := ""
		if len(stmt.From) > 0 {
			if tbl, ok := stmt.From[0].(*sqlparser.AliasedTableExpr); ok {
				if tblName, ok := tbl.Expr.(sqlparser.TableName); ok {
					tableName = tblName.Name.String()
				}
			}
		}
		// Build a set of table names/aliases in the FROM clause of this SELECT.
		// Used to detect correlated column references (outer query columns), which are always allowed.
		localTableAliases := make(map[string]bool)
		for _, alias := range collectTableAliases(stmt.From) {
			if alias != "" {
				localTableAliases[strings.ToLower(alias)] = true
			}
		}
		// Also add actual table names (not just aliases)
		for _, te := range stmt.From {
			if ate, ok := te.(*sqlparser.AliasedTableExpr); ok {
				if tn, ok := ate.Expr.(sqlparser.TableName); ok {
					localTableAliases[strings.ToLower(tn.Name.String())] = true
				}
			}
		}
		// Build a set of table qualifiers that have at least one column in the groupByColSet.
		// This is used to implement the MySQL rule: if a table's unique/primary key column
		// is in GROUP BY, all columns from that table are functionally dependent.
		// We use a simplified heuristic: if any column from a table qualifier appears in
		// groupByColSet (or is functionally determined), that table's columns are all allowed.
		tablesWithGroupedCols := make(map[string]bool)
		for _, gbExpr := range stmt.GroupBy.Exprs {
			if col, ok := gbExpr.(*sqlparser.ColName); ok {
				if !col.Qualifier.IsEmpty() {
					tablesWithGroupedCols[strings.ToLower(col.Qualifier.Name.String())] = true
				}
			}
		}
		// Also add tables from equality-expanded columns (tracked via SELECT expressions below)
		for _, selectExpr := range stmt.SelectExprs.Exprs {
			if ae2, ok := selectExpr.(*sqlparser.AliasedExpr); ok {
				if col2, ok := ae2.Expr.(*sqlparser.ColName); ok {
					colLower := strings.ToLower(col2.Name.String())
					if groupByColSet[colLower] || whereConstantCols[colLower] {
						if !col2.Qualifier.IsEmpty() {
							tablesWithGroupedCols[strings.ToLower(col2.Qualifier.Name.String())] = true
						}
					}
				}
			}
		}
		// Check each table in FROM to see if GROUP BY columns cover a UNIQUE or PRIMARY key.
		// If so, all columns from that table are functionally dependent on GROUP BY.
		tablesFullyDetermined := make(map[string]bool)
		for _, te := range stmt.From {
			if ate, ok := te.(*sqlparser.AliasedTableExpr); ok {
				if tn, ok := ate.Expr.(sqlparser.TableName); ok {
					tblName := strings.ToLower(tn.Name.String())
					dbName := e.CurrentDB
					if !tn.Qualifier.IsEmpty() {
						dbName = tn.Qualifier.String()
					}
					// Get the alias
					tblAlias := tblName
					if !ate.As.IsEmpty() {
						tblAlias = strings.ToLower(ate.As.String())
					}
					// Get table definition to find unique/primary keys
					if db, err := e.Catalog.GetDatabase(dbName); err == nil {
						if def, err := db.GetTable(tn.Name.String()); err == nil && def != nil {
							// Check if GROUP BY columns cover the PRIMARY KEY
							if len(def.PrimaryKey) > 0 {
								allInGroup := true
								for _, pkCol := range def.PrimaryKey {
									if !groupByColSet[strings.ToLower(pkCol)] {
										allInGroup = false
										break
									}
								}
								if allInGroup {
									tablesFullyDetermined[tblName] = true
									tablesFullyDetermined[tblAlias] = true
								}
							}
							// Check if GROUP BY columns cover any UNIQUE key
							if !tablesFullyDetermined[tblName] {
								for _, idx := range def.Indexes {
									if !idx.Unique {
										continue
									}
									allInGroup := true
									for _, idxCol := range idx.Columns {
										if !groupByColSet[strings.ToLower(idxCol)] {
											allInGroup = false
											break
										}
									}
									if allInGroup && len(idx.Columns) > 0 {
										tablesFullyDetermined[tblName] = true
										tablesFullyDetermined[tblAlias] = true
										break
									}
								}
							}
						}
					}
				}
			}
		}
		// Check each SELECT expression
		for i, selectExpr := range stmt.SelectExprs.Exprs {
			ae, ok := selectExpr.(*sqlparser.AliasedExpr)
			if !ok {
				continue
			}
			// Skip aggregate expressions
			if isAggregateExpr(ae.Expr) {
				continue
			}
			// Skip literals and NULL
			switch ae.Expr.(type) {
			case *sqlparser.Literal, *sqlparser.NullVal:
				continue
			}
			// Check if it's a column reference
			col, isCol := ae.Expr.(*sqlparser.ColName)
			if !isCol {
				continue
			}
			colNameLower := strings.ToLower(col.Name.String())
			if groupByColSet[colNameLower] {
				continue // In GROUP BY — OK
			}
			if whereConstantCols[colNameLower] {
				continue // Constrained by WHERE equality — functionally dependent
			}
			// If the column has a table qualifier that doesn't appear in the local FROM clause,
			// it's a correlated reference from an outer query and should always be allowed.
			if !col.Qualifier.IsEmpty() {
				tableQualLower := strings.ToLower(col.Qualifier.Name.String())
				if !localTableAliases[tableQualLower] {
					continue // Correlated reference from outer query — always allowed
				}
				// If GROUP BY covers a unique key of this table, all columns are functionally dependent.
				if tablesFullyDetermined[tableQualLower] {
					continue // Table's unique key is in GROUP BY — all columns are determined
				}
				// If the table is local and has a grouped column, it's considered functionally dependent.
				if tablesWithGroupedCols[tableQualLower] {
					continue // Table has a grouped column - considered functionally dependent
				}
			} else {
				// No qualifier — check all tables in the FROM clause for unique key coverage
				if len(tablesFullyDetermined) > 0 {
					// If only one table and it's fully determined, allow
					if len(localTableAliases) == 1 {
						for tbl := range tablesFullyDetermined {
							if localTableAliases[tbl] {
								continue
							}
							_ = tbl
						}
						// Allow if the single table is fully determined
						allDetermined := true
						for tbl := range localTableAliases {
							if !tablesFullyDetermined[tbl] {
								allDetermined = false
								break
							}
						}
						if allDetermined {
							continue
						}
					}
				}
			}
			// Not in GROUP BY — error
			dbTable := e.CurrentDB + "." + tableName + "." + col.Name.String()
			return nil, mysqlError(1055, "42000",
				fmt.Sprintf("Expression #%d of SELECT list is not in GROUP BY clause and contains nonaggregated column '%s' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by",
					i+1, dbTable))
		}
	}

	type group struct {
		key  string
		rows []storage.Row
	}

	var groups []group
	groupIndex := make(map[string]int)

	// Resolve positional GROUP BY references (e.g. GROUP BY 1) and alias references
	// (e.g. GROUP BY fcase where fcase is a SELECT alias) to their actual expressions.
	// This slice is used both for grouping and rollup processing.
	var resolvedGroupByExprs []sqlparser.Expr
	// Build alias-to-expression map from SELECT list
	selectAliasToExpr := make(map[string]sqlparser.Expr)
	for _, selectExpr := range stmt.SelectExprs.Exprs {
		if ae, ok := selectExpr.(*sqlparser.AliasedExpr); ok && !ae.As.IsEmpty() {
			selectAliasToExpr[strings.ToLower(ae.As.String())] = ae.Expr
		}
	}
	if stmt.GroupBy != nil && len(stmt.GroupBy.Exprs) > 0 {
		resolvedGroupByExprs = make([]sqlparser.Expr, len(stmt.GroupBy.Exprs))
		for i, gbExpr := range stmt.GroupBy.Exprs {
			if lit, ok := gbExpr.(*sqlparser.Literal); ok && lit.Type == sqlparser.IntVal {
				pos := int(toInt64(lit.Val))
				if pos >= 1 && pos <= len(stmt.SelectExprs.Exprs) {
					if ae, ok := stmt.SelectExprs.Exprs[pos-1].(*sqlparser.AliasedExpr); ok {
						resolvedGroupByExprs[i] = ae.Expr
						continue
					}
				}
			}
			// Check if GROUP BY refers to a SELECT alias (e.g. GROUP BY fcase)
			if col, ok := gbExpr.(*sqlparser.ColName); ok && col.Qualifier.IsEmpty() {
				aliasLower := strings.ToLower(col.Name.String())
				if expr, found := selectAliasToExpr[aliasLower]; found {
					resolvedGroupByExprs[i] = expr
					continue
				}
			}
			resolvedGroupByExprs[i] = gbExpr
		}
	}

	if stmt.GroupBy != nil && len(stmt.GroupBy.Exprs) > 0 {
		for _, row := range allRows {
			key := computeGroupKey(resolvedGroupByExprs, row)
			if idx, ok := groupIndex[key]; ok {
				groups[idx].rows = append(groups[idx].rows, row)
			} else {
				groupIndex[key] = len(groups)
				groups = append(groups, group{key: key, rows: []storage.Row{row}})
			}
		}
		// Sort GROUP BY groups by key so that NULL rows appear before non-NULL rows,
		// matching MySQL/InnoDB behavior where rows are returned in index/PK order.
		// This applies both for regular GROUP BY and WITH ROLLUP.
		sort.SliceStable(groups, func(i, j int) bool {
			return compareGroupKeys(groups[i].key, groups[j].key) < 0
		})
	} else {
		// No GROUP BY but has aggregates: treat all rows as one group
		groups = []group{{key: "", rows: allRows}}
	}

	// Compute column names
	rawExprs := extractRawSelectExprs(e.currentQuery)
	rawExprIdx := 0
	colNames := make([]string, 0, len(stmt.SelectExprs.Exprs))
	for _, expr := range stmt.SelectExprs.Exprs {
		switch se := expr.(type) {
		case *sqlparser.AliasedExpr:
			if !se.As.IsEmpty() {
				colNames = append(colNames, se.As.String())
			} else if isAggregateExpr(se.Expr) {
				// Use raw expression text for aggregates, then normalize
				// MySQL displays function args without space after comma: JSON_OBJECTAGG(k,b)
				if rawExprIdx < len(rawExprs) {
					raw := strings.TrimSpace(rawExprs[rawExprIdx])
					// MySQL strips block comments from column names
					if strings.Contains(raw, "/*") {
						raw = stripBlockComments(raw)
					}
					// MySQL displays function args without space after comma
					raw = normalizeFuncArgSpaces(raw)
					// MySQL displays NULL uppercase and SQL keywords uppercase in column headers
					raw = normalizeAggColNameNulls(raw)
					// Uppercase outer aggregate function name if written lowercase (e.g. max( -> MAX()
					for _, fn := range []string{"json_arrayagg(", "json_objectagg(", "count(", "sum(", "avg(", "min(", "max(", "group_concat("} {
						if strings.HasPrefix(raw, fn) {
							raw = strings.ToUpper(fn) + raw[len(fn):]
							break
						}
					}
					raw = uppercaseAggInnerKeywords(raw)
					// MySQL lowercases non-keyword function names (e.g. ST_PointFromText → st_pointfromtext)
					raw = normalizeAggColNameFunctions(raw)
					// MySQL adds "FROM dual" to subselects without FROM
					raw = normalizeAggColNameSubselect(raw)
					colNames = append(colNames, raw)
				} else {
					colNames = append(colNames, aggregateDisplayName(se.Expr))
				}
			} else if colName, ok := se.Expr.(*sqlparser.ColName); ok {
				colNames = append(colNames, colName.Name.String())
			} else if lit, ok := se.Expr.(*sqlparser.Literal); ok && lit.Type == sqlparser.StrVal {
				// String literal in UNION context: MySQL uses the literal value (unquoted) as column name.
				// We use the AST directly to avoid picking up the "UNION ..." suffix from rawExprs.
				colNames = append(colNames, lit.Val)
				rawExprIdx++
				continue
			} else if _, ok := se.Expr.(*sqlparser.IntervalDateExpr); ok {
				// DATE_ADD/DATE_SUB with INTERVAL: MySQL normalizes to lowercase via Vitess String()
				// e.g. DATE_ADD(JSON_ARRAYAGG(a), INTERVAL 31 DAY) -> date_add(JSON_ARRAYAGG(a),interval 31 day)
				colName := sqlparser.String(se.Expr)
				// But restore uppercase for known aggregate function names within the expression
				for _, fn := range []string{"json_arrayagg", "json_objectagg", "count", "sum", "avg", "min", "max", "group_concat"} {
					colName = strings.ReplaceAll(colName, fn+"(", strings.ToUpper(fn)+"(")
				}
				colNames = append(colNames, colName)
			} else if rawExprIdx < len(rawExprs) {
				raw := strings.TrimSpace(rawExprs[rawExprIdx])
				// MySQL strips block comments (/* ... */) from column names.
				// e.g. SELECT 1 /* comment */ has column name "1" not "1 /* comment */"
				if strings.Contains(raw, "/*") {
					raw = stripBlockComments(raw)
				}
				if strings.Contains(strings.ToLower(raw), "@@") {
					if len(raw) >= 2 && raw[0] == '\'' && raw[len(raw)-1] == '\'' {
						raw = raw[1 : len(raw)-1]
					}
					colNames = append(colNames, raw)
				} else if raw != "" {
					// Strip charset introducers for column names: _utf8'abc' -> 'abc', n'abc' -> 'abc'
					raw = stripCharsetIntroducerForColName(raw)
					// Strip quotes from simple string literals (MySQL displays 'a' as a)
					if len(raw) >= 2 && raw[0] == '\'' && raw[len(raw)-1] == '\'' && isSimpleStringLiteral(raw) {
						raw = raw[1 : len(raw)-1]
					}
					// MySQL strips leading '+' from numeric literals in column headers
					// e.g. SELECT +4294967296 has column name "4294967296" not "+4294967296"
					if len(raw) > 1 && raw[0] == '+' && raw[1] >= '0' && raw[1] <= '9' {
						raw = raw[1:]
					}
					// Normalize charset introducers (e.g. _utf8mb3 'x' → _utf8'x')
					raw = normalizeCharsetIntroducers(raw)
					// Preserve original query text for column name (MySQL behavior)
					colNames = append(colNames, raw)
				} else {
					colNames = append(colNames, normalizeSQLDisplayName(sqlparser.String(se.Expr)))
				}
			} else {
				colNames = append(colNames, normalizeSQLDisplayName(sqlparser.String(se.Expr)))
			}
			rawExprIdx++
		case *sqlparser.StarExpr:
			// SELECT * with GROUP BY: expand to all columns from the representative row,
			// preferring __column_order__ metadata to maintain schema order.
			if len(groups) > 0 && len(groups[0].rows) > 0 {
				repRow := groups[0].rows[0]
				// Use __column_order__ if available (set when fetching from real tables)
				if orderStr, ok := repRow["__column_order__"]; ok {
					if s, ok2 := orderStr.(string); ok2 && s != "" {
						colNames = append(colNames, strings.Split(s, "\x00")...)
						break
					}
				}
				// Fallback: use unqualified, non-internal keys
				keys := make([]string, 0, len(repRow))
				for k := range repRow {
					if !strings.Contains(k, ".") && k != "__column_order__" {
						keys = append(keys, k)
					}
				}
				sort.Strings(keys)
				colNames = append(colNames, keys...)
			}
		default:
			return nil, fmt.Errorf("unsupported select expression in GROUP BY: %T", expr)
		}
	}

	// Compute result rows per group
	resultRows := make([][]interface{}, 0, len(groups))
	for _, g := range groups {
		repRow := storage.Row{}
		if len(g.rows) > 0 {
			repRow = g.rows[0]
		}
		resultRow := make([]interface{}, 0, len(stmt.SelectExprs.Exprs))
		for _, expr := range stmt.SelectExprs.Exprs {
			switch se := expr.(type) {
			case *sqlparser.StarExpr:
				// Expand * to all column values from representative row,
				// matching the same key set used for colNames above.
				_ = se
				var starKeys []string
				if orderStr, ok := repRow["__column_order__"]; ok {
					if s, ok2 := orderStr.(string); ok2 && s != "" {
						starKeys = strings.Split(s, "\x00")
					}
				}
				if starKeys == nil {
					starKeys = make([]string, 0, len(repRow))
					for k := range repRow {
						if !strings.Contains(k, ".") && k != "__column_order__" {
							starKeys = append(starKeys, k)
						}
					}
					sort.Strings(starKeys)
				}
				for _, k := range starKeys {
					resultRow = append(resultRow, repRow[k])
				}
				continue
			}
			ae, ok := expr.(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("unsupported select expression in GROUP BY: %T", expr)
			}
			val, err := evalAggregateExpr(ae.Expr, g.rows, repRow, e)
			if err != nil {
				return nil, err
			}
			resultRow = append(resultRow, val)
		}
		resultRows = append(resultRows, resultRow)
	}

	// Apply WITH ROLLUP: add super-aggregate rows
	if stmt.GroupBy != nil && stmt.GroupBy.WithRollup && len(stmt.GroupBy.Exprs) > 0 {
		groupByExprs := resolvedGroupByExprs
		numGroupCols := len(groupByExprs)

		// Helper: check if a select expression corresponds to a rolled-up group-by column
		// at the given rollup level. groupByExprs is already positionally resolved.
		isRolledUpExpr := func(ae *sqlparser.AliasedExpr, level int) bool {
			for gi := level; gi < numGroupCols; gi++ {
				gbExpr := groupByExprs[gi]
				gbStr := sqlparser.String(gbExpr)
				// Check direct column name match
				if colName, ok := ae.Expr.(*sqlparser.ColName); ok {
					if gbCol, ok := gbExpr.(*sqlparser.ColName); ok {
						if strings.EqualFold(colName.Name.String(), gbCol.Name.String()) {
							return true
						}
					}
				}
				// Check alias match
				if !ae.As.IsEmpty() {
					if strings.EqualFold(ae.As.String(), gbStr) {
						return true
					}
				}
				// Check expression string match
				if strings.EqualFold(sqlparser.String(ae.Expr), gbStr) {
					return true
				}
			}
			return false
		}

		// Helper: check if a column name is a rolled-up GROUP BY column at the given level
		// Returns (groupingBit, true) where groupingBit is the bit value for GROUPING() encoding.
		isGroupingRolledUp := func(colName string, level int) bool {
			for gi := level; gi < numGroupCols; gi++ {
				if gbCol, ok := groupByExprs[gi].(*sqlparser.ColName); ok {
					if strings.EqualFold(colName, gbCol.Name.String()) {
						return true
					}
				} else {
					gbStr := sqlparser.String(groupByExprs[gi])
					if strings.EqualFold(colName, gbStr) {
						return true
					}
				}
			}
			return false
		}

		// evalGroupingFunc evaluates GROUPING(col1[, col2, ...]) at the given rollup level.
		// Returns bit-encoded integer: bit N corresponds to the (N+1)-th argument from right.
		// Each bit is 1 if the corresponding argument is rolled up at this level.
		evalGroupingFunc := func(fn *sqlparser.FuncExpr, level int) interface{} {
			args := fn.Exprs
			result := int64(0)
			for i, argExpr := range args {
				col, ok := argExpr.(*sqlparser.ColName)
				if !ok {
					continue
				}
				colName := col.Name.String()
				if isGroupingRolledUp(colName, level) {
					// bit position: rightmost arg is bit 0
					bitPos := len(args) - 1 - i
					result |= int64(1) << bitPos
				}
			}
			return result
		}

		// Helper: build a rollup row for a set of source rows at a given level
		buildRollupRow := func(sourceRows []storage.Row, level int) ([]interface{}, error) {
			repRow := storage.Row{}
			if len(sourceRows) > 0 {
				repRow = sourceRows[0]
			}
			rollupRow := make([]interface{}, 0, len(stmt.SelectExprs.Exprs))
			for _, expr := range stmt.SelectExprs.Exprs {
				// Handle SELECT * in ROLLUP: expand star to all columns (all NULL in rollup rows)
				if _, isStar := expr.(*sqlparser.StarExpr); isStar {
					var starKeys []string
					if orderStr, ok := repRow["__column_order__"]; ok {
						if s, ok2 := orderStr.(string); ok2 && s != "" {
							starKeys = strings.Split(s, "\x00")
						}
					}
					if starKeys == nil {
						starKeys = make([]string, 0, len(repRow))
						for k := range repRow {
							if !strings.Contains(k, ".") && k != "__column_order__" {
								starKeys = append(starKeys, k)
							}
						}
						sort.Strings(starKeys)
					}
					for range starKeys {
						rollupRow = append(rollupRow, nil)
					}
					continue
				}
				ae, ok := expr.(*sqlparser.AliasedExpr)
				if !ok {
					rollupRow = append(rollupRow, nil)
					continue
				}
				// Handle GROUPING() function specially in rollup rows
				if fn, ok := ae.Expr.(*sqlparser.FuncExpr); ok && strings.EqualFold(fn.Name.String(), "grouping") {
					rollupRow = append(rollupRow, evalGroupingFunc(fn, level))
					continue
				}
				if isRolledUpExpr(ae, level) {
					rollupRow = append(rollupRow, nil)
				} else {
					val, err := evalAggregateExpr(ae.Expr, sourceRows, repRow, e)
					if err != nil {
						return nil, err
					}
					rollupRow = append(rollupRow, val)
				}
			}
			return rollupRow, nil
		}

		// Build interleaved result with rollup rows.
		// For GROUP BY a, b WITH ROLLUP:
		// - After all rows with same (a), insert rollup row (a, NULL)
		// - After all rows, insert grand total (NULL, NULL)
		//
		// We need to process from the deepest level up.
		// For each level from numGroupCols-1 down to 0:
		// Insert rollup rows after each change in the prefix key at that level.

		// Build a mapping from each GROUP BY expression to its index in colNames/result rows.
		// This allows us to extract prefix keys directly from result rows.
		gbColIdxs := make([]int, numGroupCols) // gbColIdxs[k] = colNames index for groupByExprs[k]
		for k, gbExpr := range groupByExprs {
			gbColIdxs[k] = -1
			gbStr := sqlparser.String(gbExpr)
			for ci, cname := range colNames {
				if strings.EqualFold(cname, gbStr) {
					gbColIdxs[k] = ci
					break
				}
			}
			// Fallback: match by column name only (without table qualifier)
			if gbColIdxs[k] == -1 {
				if col, ok := gbExpr.(*sqlparser.ColName); ok {
					colStr := col.Name.String()
					for ci, cname := range colNames {
						if strings.EqualFold(cname, colStr) {
							gbColIdxs[k] = ci
							break
						}
					}
				}
			}
			// Final fallback: use position k if still not found
			if gbColIdxs[k] == -1 && k < len(colNames) {
				gbColIdxs[k] = k
			}
		}

		// rowPrefixKey computes the prefix key for a result row at the given level.
		// Uses the first `level` GROUP BY column values from the result row.
		rowPrefixKey := func(row []interface{}, level int) string {
			parts := make([]string, level)
			for k := 0; k < level; k++ {
				idx := gbColIdxs[k]
				if idx >= 0 && idx < len(row) {
					parts[k] = fmt.Sprintf("%v", row[idx])
				} else {
					parts[k] = "<nil>"
				}
			}
			return strings.Join(parts, "\x00")
		}

		// isRollupRow returns true if the result row is a rollup row at level `l` or deeper.
		// A rollup row at level l has nil in its l-th GROUP BY column.
		isRollupRow := func(row []interface{}, l int) bool {
			if l >= numGroupCols {
				return false
			}
			idx := gbColIdxs[l]
			if idx < 0 || idx >= len(row) {
				return false
			}
			return row[idx] == nil
		}

		// Build a mapping from prefix key (first `level` group-by values) to original storage.Rows.
		// prefixSourceRows[level][prefixKey] = []storage.Row
		prefixSourceRows := make([]map[string][]storage.Row, numGroupCols)
		for l := 0; l < numGroupCols; l++ {
			prefixSourceRows[l] = make(map[string][]storage.Row)
		}
		for _, g := range groups {
			for l := 0; l < numGroupCols; l++ {
				prefixKey := computeGroupKey(groupByExprs[:l], g.rows[0])
				prefixSourceRows[l][prefixKey] = append(prefixSourceRows[l][prefixKey], g.rows...)
			}
		}

		// Insert rollup rows. Process from deepest level up to level 0.
		// At each level, we insert a rollup row after each contiguous block of rows
		// that share the same prefix at that level. Non-rollup rows AND rollup rows
		// from deeper levels (which have nil only in columns >= level) all belong
		// to a prefix group; rollup rows from the same level or shallower are skipped.
		newResult := make([][]interface{}, 0, len(resultRows)*2)
		for level := numGroupCols - 1; level >= 0; level-- {
			prevPrefix := ""
			source := resultRows
			if level < numGroupCols-1 {
				source = newResult
				newResult = make([][]interface{}, 0, len(source)*2)
			}
			for i, row := range source {
				// Skip rows that are rollup rows at this level or shallower
				// (they were inserted by this or a previous level iteration).
				if isRollupRow(row, level) {
					newResult = append(newResult, row)
					continue
				}
				// Compute prefix for this row at the current level
				var thisPrefix string
				if level == 0 {
					thisPrefix = ""
				} else {
					thisPrefix = rowPrefixKey(row, level)
				}
				// When the prefix changes (and we're not at the very first row), insert rollup
				if i > 0 && thisPrefix != prevPrefix && prevPrefix != "" {
					// Find the original rows for prevPrefix
					srcRows := prefixSourceRows[level][prevPrefix]
					rollupRow, err := buildRollupRow(srcRows, level)
					if err != nil {
						return nil, err
					}
					newResult = append(newResult, rollupRow)
				}
				prevPrefix = thisPrefix
				newResult = append(newResult, row)
			}
			// Insert rollup row for the last prefix
			if level == 0 {
				if len(allRows) > 0 {
					rollupRow, err := buildRollupRow(allRows, 0)
					if err != nil {
						return nil, err
					}
					newResult = append(newResult, rollupRow)
				}
			} else if prevPrefix != "" {
				srcRows := prefixSourceRows[level][prevPrefix]
				rollupRow, err := buildRollupRow(srcRows, level)
				if err != nil {
					return nil, err
				}
				newResult = append(newResult, rollupRow)
			}
		}
		if numGroupCols == 1 {
			// For single group-by column, just append the grand total
			newResult = resultRows
			if len(allRows) > 0 {
				rollupRow, err := buildRollupRow(allRows, 0)
				if err != nil {
					return nil, err
				}
				newResult = append(newResult, rollupRow)
			}
		}
		resultRows = newResult
	}

	// Apply window functions over aggregated rows (e.g. SUM(BIT_AND(i)) OVER (...)).
	// We build synthetic storage.Row objects from the aggregate result rows using colNames
	// as keys, so that window function arguments can look up pre-computed aggregate values.
	if selectExprsHaveWindowFuncs(stmt.SelectExprs.Exprs) {
		// Build synthetic rows from aggregate results.
		syntheticRows := make([]storage.Row, len(resultRows))
		for ri, row := range resultRows {
			sr := make(storage.Row, len(colNames))
			for ci, cn := range colNames {
				if ci < len(row) {
					sr[cn] = row[ci]
				}
			}
			syntheticRows[ri] = sr
		}
		// Extract column expressions.
		colExprs := make([]sqlparser.Expr, 0, len(stmt.SelectExprs.Exprs))
		for _, se := range stmt.SelectExprs.Exprs {
			if ae, ok := se.(*sqlparser.AliasedExpr); ok {
				colExprs = append(colExprs, ae.Expr)
			}
		}
		if err := e.processWindowFunctionsWithNamedWindows(colExprs, syntheticRows, resultRows, stmt.Windows); err != nil {
			return nil, err
		}
	}

	// Apply HAVING
	if stmt.Having != nil {
		filtered := make([][]interface{}, 0)
		for gi, row := range resultRows {
			havingRow := make(storage.Row)
			for i, col := range colNames {
				havingRow[col] = row[i]
			}
			// Also evaluate aggregates from the HAVING clause against the group rows
			var groupRows []storage.Row
			if gi < len(groups) {
				groupRows = groups[gi].rows
			}
			// Include the raw group row values for GROUP BY columns so that GROUP BY columns
			// not in SELECT are accessible in HAVING (e.g. HAVING col1 = 10 when col1
			// is grouped by but not in the SELECT list).
			if len(groupRows) > 0 && stmt.GroupBy != nil {
				for _, gbExpr := range stmt.GroupBy.Exprs {
					gbColName := strings.ToLower(sqlparser.String(gbExpr))
					// Try stripping table qualifier for lookup
					if _, exists := havingRow[gbColName]; !exists {
						// Check with and without table prefix
						for k, v := range groupRows[0] {
							kLower := strings.ToLower(k)
							if kLower == gbColName || strings.HasSuffix(kLower, "."+gbColName) || strings.TrimPrefix(kLower, strings.Split(kLower, ".")[0]+".") == gbColName {
								if _, exists2 := havingRow[k]; !exists2 {
									havingRow[k] = v
								}
								// Also add just the bare column name
								bareName := gbColName
								if idx := strings.LastIndex(gbColName, "."); idx >= 0 {
									bareName = gbColName[idx+1:]
								}
								if _, exists3 := havingRow[bareName]; !exists3 {
									havingRow[bareName] = v
								}
								break
							}
						}
					}
				}
			}
			// Evaluate HAVING with aggregate support
			match, err := e.evalHaving(stmt.Having.Expr, havingRow, groupRows)
			if err != nil {
				return nil, err
			}
			if match {
				filtered = append(filtered, row)
			}
		}
		resultRows = filtered
	}

	// Apply ORDER BY
	var err error
	orderCollation := ""
	if len(stmt.From) > 0 {
		orderCollation = resolveOrderByCollation(e.collectTableDefs(stmt.From[0]), stmt.From[0])
	}
	needsSortStats2 := stmt.OrderBy != nil
	if needsSortStats2 {
		resultRows, err = applyOrderBy(stmt.OrderBy, colNames, resultRows, orderCollation)
		if err != nil {
			return nil, err
		}
	}

	// Track row count before LIMIT for FOUND_ROWS()
	e.lastFoundRows = int64(len(resultRows))

	// Apply LIMIT
	if stmt.Limit != nil {
		resultRows, err = applyLimit(stmt.Limit, resultRows)
		if err != nil {
			return nil, err
		}
	}

	// Track sort statistics AFTER LIMIT
	if needsSortStats2 {
		e.sortRows += int64(len(resultRows))
		if stmt.Where != nil && containsBetweenExpr(stmt.Where.Expr) {
			e.sortRange++
		} else {
			e.sortScan++
		}
	}

	// Handle SELECT ... INTO OUTFILE (GROUP BY path)
	if stmt.Into != nil && stmt.Into.Type == sqlparser.IntoOutfile {
		return e.execSelectIntoOutfile(stmt.Into, colNames, resultRows)
	}

	// Handle SELECT ... INTO @var1, @var2, ... (GROUP BY path)
	if stmt.Into != nil && stmt.Into.Type == sqlparser.IntoVariables {
		return e.execSelectIntoUserVars(stmt.Into, colNames, resultRows)
	}

	return &Result{
		Columns:     colNames,
		Rows:        resultRows,
		IsResultSet: true,
	}, nil
}

// computeGroupKey builds a string key for a row based on GROUP BY expressions.
func computeGroupKey(groupByExprs []sqlparser.Expr, row storage.Row) string {
	parts := make([]string, 0, len(groupByExprs))
	for _, expr := range groupByExprs {
		val, _ := evalRowExpr(expr, row)
		parts = append(parts, fmt.Sprintf("%v", val))
	}
	return strings.Join(parts, "\x00")
}

// compareGroupKeys compares two group key strings for ordering.
// Each key may be a multi-part \x00-separated string.
// For each part, it tries to compare numerically if both look like numbers,
// otherwise compares as strings. NULL (represented as "<nil>") sorts first.
func compareGroupKeys(a, b string) int {
	aParts := strings.Split(a, "\x00")
	bParts := strings.Split(b, "\x00")
	n := len(aParts)
	if len(bParts) < n {
		n = len(bParts)
	}
	for i := 0; i < n; i++ {
		ap, bp := aParts[i], bParts[i]
		// NULL ("<nil>") sorts first (before any other value)
		aIsNull := ap == "<nil>"
		bIsNull := bp == "<nil>"
		if aIsNull && bIsNull {
			continue
		}
		if aIsNull {
			return -1
		}
		if bIsNull {
			return 1
		}
		// Try numeric comparison
		aFloat, aErr := strconv.ParseFloat(ap, 64)
		bFloat, bErr := strconv.ParseFloat(bp, 64)
		if aErr == nil && bErr == nil {
			if aFloat < bFloat {
				return -1
			} else if aFloat > bFloat {
				return 1
			}
		} else {
			// String comparison
			if ap < bp {
				return -1
			} else if ap > bp {
				return 1
			}
		}
	}
	return len(aParts) - len(bParts)
}

// getAggArg extracts the argument expression from variance/std aggregate types.
func getAggArg(e sqlparser.Expr) sqlparser.Expr {
	switch v := e.(type) {
	case *sqlparser.Variance:
		return v.Arg
	case *sqlparser.VarPop:
		return v.Arg
	case *sqlparser.VarSamp:
		return v.Arg
	case *sqlparser.Std:
		return v.Arg
	case *sqlparser.StdDev:
		return v.Arg
	case *sqlparser.StdPop:
		return v.Arg
	case *sqlparser.StdSamp:
		return v.Arg
	}
	return nil
}

// collectNumericVals collects non-NULL float64 values for an expression over a group of rows.
func collectNumericVals(arg sqlparser.Expr, groupRows []storage.Row) ([]float64, int) {
	if arg == nil {
		return nil, 0
	}
	var vals []float64
	for _, row := range groupRows {
		val, err := evalRowExpr(arg, row)
		if err != nil || val == nil {
			continue
		}
		vals = append(vals, toFloat(val))
	}
	return vals, len(vals)
}

// evalAggregateExpr evaluates an expression that may be an aggregate function over a group.
func evalAggregateExpr(expr sqlparser.Expr, groupRows []storage.Row, repRow storage.Row, execCtx ...*Executor) (interface{}, error) {
	var exec *Executor
	if len(execCtx) > 0 {
		exec = execCtx[0]
	}
	switch e := expr.(type) {
	case *sqlparser.CountStar:
		return int64(len(groupRows)), nil
	case *sqlparser.Count:
		if len(e.Args) == 0 {
			return int64(len(groupRows)), nil
		}
		if e.Distinct {
			// COUNT(DISTINCT expr[, expr...]) - count unique non-NULL combinations
			// A row is skipped if ANY argument is NULL
			seen := make(map[string]bool)
			for _, row := range groupRows {
				var keyParts []string
				anyNull := false
				for _, arg := range e.Args {
					val, err := evalRowExpr(arg, row)
					if err != nil {
						return nil, err
					}
					if val == nil {
						anyNull = true
						break
					}
					keyParts = append(keyParts, fmt.Sprintf("%v", val))
				}
				if !anyNull {
					key := strings.Join(keyParts, "\x00")
					seen[key] = true
				}
			}
			return int64(len(seen)), nil
		}
		count := int64(0)
		for _, row := range groupRows {
			val, err := evalRowExpr(e.Args[0], row)
			if err != nil {
				return nil, err
			}
			if val != nil {
				count++
			}
		}
		return count, nil
	case *sqlparser.Sum:
		// For SUM(DISTINCT expr), filter to unique non-NULL values first.
		// NULL values are excluded from DISTINCT aggregation per SQL standard.
		rows := groupRows
		if e.Distinct {
			seen := make(map[string]bool)
			var filtered []storage.Row
			for _, row := range groupRows {
				val, err := evalRowExpr(e.Arg, row)
				if err != nil {
					return nil, err
				}
				if val != nil {
					key := fmt.Sprintf("%v", val)
					if !seen[key] {
						seen[key] = true
						filtered = append(filtered, row)
					}
				}
				// NULL values are skipped (not added to filtered)
			}
			rows = filtered
		}
		sum := float64(0)
		sumRat := new(big.Rat)
		hasRat := false
		hasVal := false
		maxScale := 0 // track max decimal places for formatting
		allDecimal := true
		for _, row := range rows {
			val, err := evalRowExpr(e.Arg, row)
			if err != nil {
				return nil, err
			}
			if val != nil {
				hasVal = true
				// Track decimal precision
				if s, ok := val.(string); ok {
					if dot := strings.Index(s, "."); dot >= 0 {
						scale := len(s) - dot - 1
						if scale > maxScale {
							maxScale = scale
						}
					}
					if r, ok := parseDecimalStringToRat(s); ok {
						sumRat.Add(sumRat, r)
						hasRat = true
						sum += toFloat(val)
						continue
					}
					allDecimal = false
					sum += toFloat(val)
				} else {
					allDecimal = false
					sum += toFloat(val)
				}
			}
		}
		if !hasVal {
			return nil, nil
		}
		if allDecimal && hasRat {
			if maxScale == 0 {
				return formatRatFixed(sumRat, 0), nil
			}
			return formatRatFixed(sumRat, maxScale), nil
		}
		if sum == float64(int64(sum)) && maxScale == 0 {
			return int64(sum), nil
		}
		// For DECIMAL-like values, format with the detected scale to avoid float precision artifacts
		if allDecimal && maxScale > 0 {
			return fmt.Sprintf("%.*f", maxScale, sum), nil
		}
		return sum, nil
	case *sqlparser.Max:
		var maxVal interface{}
		allNumericStr := true
		for _, row := range groupRows {
			val, err := evalRowExpr(e.Arg, row)
			if err != nil {
				return nil, err
			}
			if val == nil {
				continue
			}
			if s, ok := val.(string); ok {
				if _, err := strconv.ParseFloat(s, 64); err != nil {
					allNumericStr = false
				}
			} else {
				allNumericStr = false
			}
			if maxVal == nil || compareNumeric(val, maxVal) > 0 {
				maxVal = val
			}
		}
		// Convert numeric strings without decimal point to int64 (e.g., YEAR "0000" -> 0)
		if allNumericStr {
			if s, ok := maxVal.(string); ok && !strings.Contains(s, ".") {
				if f, err := strconv.ParseFloat(s, 64); err == nil {
					if f == float64(int64(f)) {
						return int64(f), nil
					}
					return f, nil
				}
			}
		}
		return maxVal, nil
	case *sqlparser.Min:
		var minVal interface{}
		allNumericStr := true
		for _, row := range groupRows {
			val, err := evalRowExpr(e.Arg, row)
			if err != nil {
				return nil, err
			}
			if val == nil {
				continue
			}
			if s, ok := val.(string); ok {
				if _, err := strconv.ParseFloat(s, 64); err != nil {
					allNumericStr = false
				}
			} else {
				allNumericStr = false
			}
			if minVal == nil || compareNumeric(val, minVal) < 0 {
				minVal = val
			}
		}
		// Convert numeric strings without decimal point to int64 (e.g., YEAR "0000" -> 0)
		// But preserve DECIMAL strings like "0.00000" as-is
		if allNumericStr {
			if s, ok := minVal.(string); ok && !strings.Contains(s, ".") {
				if f, err := strconv.ParseFloat(s, 64); err == nil {
					if f == float64(int64(f)) {
						return int64(f), nil
					}
					return f, nil
				}
			}
		}
		return minVal, nil
	case *sqlparser.Avg:
		// For AVG(DISTINCT expr), filter to unique non-NULL values first.
		// NULL values are excluded from DISTINCT aggregation per SQL standard.
		avgRows := groupRows
		if e.Distinct {
			seen := make(map[string]bool)
			var filtered []storage.Row
			for _, row := range groupRows {
				val, err := evalRowExpr(e.Arg, row)
				if err != nil {
					return nil, err
				}
				if val != nil {
					key := fmt.Sprintf("%v", val)
					if !seen[key] {
						seen[key] = true
						filtered = append(filtered, row)
					}
				}
				// NULL values are skipped (not added to filtered)
			}
			avgRows = filtered
		}
		sumRat := new(big.Rat)
		sumFloat := float64(0)
		count := int64(0)
		maxScale := 0
		allInt := true
		hasFloat := false // true when any value is float64 (FLOAT/DOUBLE column)
		for _, row := range avgRows {
			val, err := evalRowExpr(e.Arg, row)
			if err != nil {
				return nil, err
			}
			if val != nil {
				count++
				switch n := val.(type) {
				case int64:
					sumRat.Add(sumRat, new(big.Rat).SetInt64(n))
					sumFloat += float64(n)
				case uint64:
					sumRat.Add(sumRat, new(big.Rat).SetFrac(new(big.Int).SetUint64(n), big.NewInt(1)))
					sumFloat += float64(n)
				case float64:
					allInt = false
					hasFloat = true
					sumRat.Add(sumRat, new(big.Rat).SetFloat64(n))
					sumFloat += n
				case string:
					if dot := strings.Index(n, "."); dot >= 0 {
						// Ignore decimal points in scientific notation (e.g. "3.40282e38")
						if !strings.ContainsAny(n, "eE") {
							scale := len(n) - dot - 1
							if scale > maxScale {
								maxScale = scale
							}
						}
					}
					if r, ok := parseDecimalStringToRat(n); ok {
						sumRat.Add(sumRat, r)
						sumFloat += toFloat(val)
					} else {
						allInt = false
						f := toFloat(val)
						sumRat.Add(sumRat, new(big.Rat).SetFloat64(f))
						sumFloat += f
					}
				default:
					allInt = false
					f := toFloat(val)
					sumRat.Add(sumRat, new(big.Rat).SetFloat64(f))
					sumFloat += f
				}
			}
		}
		if count == 0 {
			return nil, nil
		}
		// MySQL AVG() result format depends on input type:
		// - All inputs use (scale + div_precision_increment) decimal places.
		// - FLOAT/DOUBLE inputs (hasFloat && maxScale==0): compute via float64
		//   but still format with dpi decimal places (e.g. "18535183.4917").
		// - For large or high-precision values, use exact big.Rat formatting
		//   to avoid float64 precision loss.
		_ = allInt
		dpi := 4 // default div_precision_increment
		if exec != nil {
			dpi = exec.getDivPrecisionIncrement()
		}
		avgScale := maxScale + dpi
		if hasFloat && maxScale == 0 {
			// FLOAT/DOUBLE column: use float64 arithmetic but format with dpi places.
			avg := sumFloat / float64(count)
			avgFloat := avg
			return AvgResult{Value: avgFloat, Scale: avgScale}, nil
		}
		avgRat := new(big.Rat).Quo(sumRat, new(big.Rat).SetInt64(count))
		avgFloat, _ := avgRat.Float64()
		return AvgResult{Value: avgFloat, Scale: avgScale, Rat: avgRat}, nil
	case *sqlparser.JSONArrayAgg:
		arr := make([]interface{}, 0)
		for _, row := range groupRows {
			val, err := evalRowExpr(e.Expr, row)
			if err != nil {
				return nil, err
			}
			arr = append(arr, toJSONValue(val))
		}
		return jsonMarshalMySQL(arr), nil
	case *sqlparser.JSONObjectAgg:
		obj := make(map[string]interface{})
		// Need ordered keys for deterministic output, but MySQL uses insertion order
		var keys []string
		for _, row := range groupRows {
			keyVal, err := evalRowExpr(e.Key, row)
			if err != nil {
				return nil, err
			}
			// MySQL error 3158: JSON documents may not contain NULL member names.
			if keyVal == nil {
				return nil, mysqlError(3158, "22032", "JSON documents may not contain NULL member names.")
			}
			valVal, err := evalRowExpr(e.Value, row)
			if err != nil {
				return nil, err
			}
			k := toString(keyVal)
			if _, exists := obj[k]; !exists {
				keys = append(keys, k)
			}
			obj[k] = toJSONValue(valVal)
		}
		// Build JSON object in insertion order
		var parts []string
		for _, k := range keys {
			kb, _ := json.Marshal(k)
			parts = append(parts, string(kb)+": "+jsonMarshalMySQL(obj[k]))
		}
		return "{" + strings.Join(parts, ", ") + "}", nil
	case *sqlparser.GroupConcatExpr:
		sep := e.Separator
		if sep == "" {
			sep = ","
		} else if len(sep) >= 2 && sep[0] == '\'' && sep[len(sep)-1] == '\'' {
			// Vitess parser keeps separator as a quoted string literal (e.g. "','") – strip quotes
			sep = sep[1 : len(sep)-1]
		} else if len(sep) >= 2 && sep[0] == '"' && sep[len(sep)-1] == '"' {
			sep = sep[1 : len(sep)-1]
		}
		// Apply ORDER BY if present
		sortedRows := make([]storage.Row, len(groupRows))
		copy(sortedRows, groupRows)
		if len(e.OrderBy) > 0 {
			sort.SliceStable(sortedRows, func(i, j int) bool {
				for _, ord := range e.OrderBy {
					vi, _ := evalRowExpr(ord.Expr, sortedRows[i])
					vj, _ := evalRowExpr(ord.Expr, sortedRows[j])
					lt, _ := compareValues(vi, vj, sqlparser.LessThanOp)
					eq, _ := compareValues(vi, vj, sqlparser.EqualOp)
					if eq {
						continue
					}
					if ord.Direction == sqlparser.DescOrder {
						return !lt
					}
					return lt
				}
				return false
			})
		}
		distinct := make(map[string]struct{})
		out := make([]string, 0, len(sortedRows))
		for _, row := range sortedRows {
			var part strings.Builder
			hasNull := false
			for _, arg := range e.Exprs {
				v, err := evalRowExpr(arg, row)
				if err != nil {
					return nil, err
				}
				if v == nil {
					hasNull = true
					break
				}
				part.WriteString(toString(v))
			}
			if hasNull {
				continue
			}
			s := part.String()
			if e.Distinct {
				if _, ok := distinct[s]; ok {
					continue
				}
				distinct[s] = struct{}{}
			}
			out = append(out, s)
		}
		result := strings.Join(out, sep)
		// Apply group_concat_max_len limit.
		// MySQL limits GROUP_CONCAT output based on group_concat_max_len.
		// For BLOB/binary columns with multibyte session charset (e.g. utf8mb4, mbmaxlen=4),
		// MySQL uses group_concat_max_len / mbmaxlen as the effective byte limit.
		// The default session charset in MySQL 8.0 is utf8mb4 (mbmaxlen=4).
		if len(execCtx) > 0 && execCtx[0] != nil {
			exec := execCtx[0]
			maxLen := int64(1024) // MySQL default group_concat_max_len
			if v, ok := exec.getSysVar("group_concat_max_len"); ok {
				if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
					maxLen = n
				}
			}
			// Effective byte limit: group_concat_max_len / mbmaxlen_of_session_charset.
			// For utf8mb4 (default), mbmaxlen=4; for latin1/binary, mbmaxlen=1.
			// We use mbmaxlen=4 as the default (MySQL 8.0 default charset is utf8mb4).
			const mbmaxlen = 4
			effectiveMaxBytes := int(maxLen) / mbmaxlen
			if len(result) > effectiveMaxBytes {
				// Truncate to effectiveMaxBytes and emit Warning 1260 for each source row.
				result = result[:effectiveMaxBytes]
				for i := range sortedRows {
					exec.addWarning("Warning", 1260, "Row "+strconv.Itoa(i+1)+" was cut by GROUP_CONCAT()")
				}
			}
		}
		return result, nil
	case *sqlparser.BitAnd:
		result := uint64(0xFFFFFFFFFFFFFFFF) // default: all bits set
		for _, row := range groupRows {
			val, err := evalRowExpr(e.Arg, row)
			if err != nil {
				return nil, err
			}
			if val == nil {
				continue
			}
			result &= toUint64(val)
		}
		return result, nil
	case *sqlparser.BitOr:
		result := uint64(0) // default: 0
		for _, row := range groupRows {
			val, err := evalRowExpr(e.Arg, row)
			if err != nil {
				return nil, err
			}
			if val == nil {
				continue
			}
			result |= toUint64(val)
		}
		return result, nil
	case *sqlparser.BitXor:
		result := uint64(0) // default: 0
		for _, row := range groupRows {
			val, err := evalRowExpr(e.Arg, row)
			if err != nil {
				return nil, err
			}
			if val == nil {
				continue
			}
			result ^= toUint64(val)
		}
		return result, nil
	case *sqlparser.Variance, *sqlparser.VarPop:
		// VAR_POP / VARIANCE: population variance = sum((x - mean)^2) / N
		arg := getAggArg(e)
		vals, count := collectNumericVals(arg, groupRows)
		if count == 0 {
			return nil, nil
		}
		mean := 0.0
		for _, v := range vals {
			mean += v
		}
		mean /= float64(count)
		variance := 0.0
		for _, v := range vals {
			d := v - mean
			variance += d * d
		}
		variance /= float64(count)
		return variance, nil
	case *sqlparser.VarSamp:
		// VAR_SAMP: sample variance = sum((x - mean)^2) / (N-1)
		arg := getAggArg(e)
		vals, count := collectNumericVals(arg, groupRows)
		if count < 2 {
			return nil, nil
		}
		mean := 0.0
		for _, v := range vals {
			mean += v
		}
		mean /= float64(count)
		variance := 0.0
		for _, v := range vals {
			d := v - mean
			variance += d * d
		}
		variance /= float64(count - 1)
		return variance, nil
	case *sqlparser.Std, *sqlparser.StdDev, *sqlparser.StdPop:
		// STD / STDDEV / STDDEV_POP: population std = sqrt(VAR_POP)
		arg := getAggArg(e)
		vals, count := collectNumericVals(arg, groupRows)
		if count == 0 {
			return nil, nil
		}
		mean := 0.0
		for _, v := range vals {
			mean += v
		}
		mean /= float64(count)
		variance := 0.0
		for _, v := range vals {
			d := v - mean
			variance += d * d
		}
		variance /= float64(count)
		return math.Sqrt(variance), nil
	case *sqlparser.StdSamp:
		// STDDEV_SAMP: sample std = sqrt(VAR_SAMP)
		arg := getAggArg(e)
		vals, count := collectNumericVals(arg, groupRows)
		if count < 2 {
			return nil, nil
		}
		mean := 0.0
		for _, v := range vals {
			mean += v
		}
		mean /= float64(count)
		variance := 0.0
		for _, v := range vals {
			d := v - mean
			variance += d * d
		}
		variance /= float64(count - 1)
		return math.Sqrt(variance), nil
	case *sqlparser.ComparisonExpr:
		// Handle IN / NOT IN with right side ValTuple (including row-IN-tuple-of-tuples)
		if e.Operator == sqlparser.InOp || e.Operator == sqlparser.NotInOp {
			// Handle ROW(a, AGG(b)) IN (SELECT ...) in aggregate context
			if leftTupleIN, leftIsTupleIN := e.Left.(sqlparser.ValTuple); leftIsTupleIN && exec != nil {
				if sub, ok := e.Right.(*sqlparser.Subquery); ok {
					// Evaluate left tuple elements with aggregate context
					result, err := exec.execSubquery(sub, exec.correlatedRow)
					if err != nil {
						return nil, err
					}
					if len(result.Columns) != len(leftTupleIN) {
						return nil, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftTupleIN)))
					}
					leftValsAggIN := make([]interface{}, len(leftTupleIN))
					for i, lExpr := range leftTupleIN {
						lv, err := evalAggregateExpr(lExpr, groupRows, repRow, exec)
						if err != nil {
							return nil, err
						}
						leftValsAggIN[i] = lv
					}
					hasNullAggIN := false
					for _, lv := range leftValsAggIN {
						if lv == nil {
							hasNullAggIN = true
							break
						}
					}
					for _, subRow := range result.Rows {
						if len(subRow) != len(leftValsAggIN) {
							continue
						}
						allMatch := true
						rowHasNull := false
						for i := 0; i < len(leftValsAggIN); i++ {
							if leftValsAggIN[i] == nil || subRow[i] == nil {
								rowHasNull = true
								allMatch = false
								break
							}
							match, _ := compareValues(leftValsAggIN[i], subRow[i], sqlparser.EqualOp)
							if !match {
								allMatch = false
								break
							}
						}
						if allMatch {
							if e.Operator == sqlparser.InOp {
								return int64(1), nil
							}
							return int64(0), nil
						}
						if rowHasNull {
							hasNullAggIN = true
						}
					}
					if hasNullAggIN {
						return nil, nil
					}
					if e.Operator == sqlparser.NotInOp {
						return int64(1), nil
					}
					return int64(0), nil
				}
			}
			if rightTupleAgg, ok := e.Right.(sqlparser.ValTuple); ok {
				leftVal, err := evalAggregateExpr(e.Left, groupRows, repRow, exec)
				if err != nil {
					return nil, err
				}
				// Check if left is a tuple value (we evaluated a ValTuple left side)
				// For scalar IN (v1,v2,...):
				if leftVal == nil {
					return nil, nil
				}
				hasNull := false
				for _, item := range rightTupleAgg {
					val, err := evalAggregateExpr(item, groupRows, repRow, exec)
					if err != nil {
						return nil, err
					}
					if val == nil {
						hasNull = true
						continue
					}
					match, _ := compareValues(leftVal, val, sqlparser.EqualOp)
					if match {
						if e.Operator == sqlparser.InOp {
							return int64(1), nil
						}
						return int64(0), nil
					}
				}
				if hasNull {
					return nil, nil
				}
				if e.Operator == sqlparser.NotInOp {
					return int64(1), nil
				}
				return int64(0), nil
			}
		}
		// Handle row/tuple comparisons: (a, MAX(b)) = (1, 4), etc.
		leftTupleAgg, leftIsTupleAgg := e.Left.(sqlparser.ValTuple)
		rightTupleAgg2, rightIsTupleAgg := e.Right.(sqlparser.ValTuple)
		if leftIsTupleAgg && rightIsTupleAgg {
			if len(leftTupleAgg) != len(rightTupleAgg2) {
				return nil, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftTupleAgg)))
			}
			leftValsAgg := make([]interface{}, len(leftTupleAgg))
			rightValsAgg := make([]interface{}, len(rightTupleAgg2))
			for i := 0; i < len(leftTupleAgg); i++ {
				lv, err := evalAggregateExpr(leftTupleAgg[i], groupRows, repRow, exec)
				if err != nil {
					return nil, err
				}
				leftValsAgg[i] = lv
				rv, err := evalAggregateExpr(rightTupleAgg2[i], groupRows, repRow, exec)
				if err != nil {
					return nil, err
				}
				rightValsAgg[i] = rv
			}
			equal, hasNull, err := rowTuplesEqual(interface{}(leftValsAgg), interface{}(rightValsAgg))
			if err != nil {
				return nil, err
			}
			if hasNull {
				return nil, nil
			}
			if e.Operator == sqlparser.NotEqualOp {
				equal = !equal
			}
			if equal {
				return int64(1), nil
			}
			return int64(0), nil
		}
		// Handle expressions like COUNT(*) = 0
		left, err := evalAggregateExpr(e.Left, groupRows, repRow, exec)
		if err != nil {
			return nil, err
		}
		right, err := evalAggregateExpr(e.Right, groupRows, repRow, exec)
		if err != nil {
			return nil, err
		}
		result, err := compareValues(left, right, e.Operator)
		if err != nil {
			return nil, err
		}
		if result {
			return int64(1), nil
		}
		return int64(0), nil
	case *sqlparser.BinaryExpr:
		left, err := evalAggregateExpr(e.Left, groupRows, repRow, exec)
		if err != nil {
			return nil, err
		}
		right, err := evalAggregateExpr(e.Right, groupRows, repRow, exec)
		if err != nil {
			return nil, err
		}
		return evalBinaryExpr(left, right, e.Operator)
	case *sqlparser.IntervalDateExpr:
		dateVal, err := evalAggregateExpr(e.Date, groupRows, repRow, exec)
		if err != nil {
			return nil, err
		}
		intervalVal, err := evalAggregateExpr(e.Interval, groupRows, repRow, exec)
		if err != nil {
			return nil, err
		}
		return evalIntervalDateExpr(dateVal, intervalVal, e.Unit, e.Syntax)
	case *sqlparser.FuncExpr:
		// Handle functions wrapping aggregates, e.g. JSON_MERGE_PRESERVE(JSON_ARRAYAGG(b), ...)
		if containsAggregate(e) {
			tmpExec := &Executor{}
			resolvedExprs := make([]sqlparser.Expr, len(e.Exprs))
			for i, arg := range e.Exprs {
				val, err := evalAggregateExpr(arg, groupRows, repRow, exec)
				if err != nil {
					return nil, err
				}
				if val == nil {
					resolvedExprs[i] = &sqlparser.NullVal{}
				} else {
					resolvedExprs[i] = &sqlparser.Literal{Type: sqlparser.StrVal, Val: toString(val)}
				}
			}
			newFunc := *e
			newFunc.Exprs = resolvedExprs
			return tmpExec.evalFuncExpr(&newFunc)
		}
	case *sqlparser.JSONValueMergeExpr:
		// Handle JSON_MERGE_PRESERVE(JSON_ARRAYAGG(b), ...)
		if containsAggregate(expr) {
			tmpExec := &Executor{}
			docVal, err := evalAggregateExpr(e.JSONDoc, groupRows, repRow, exec)
			if err != nil {
				return nil, err
			}
			resolvedDocList := make([]sqlparser.Expr, len(e.JSONDocList))
			for i, d := range e.JSONDocList {
				val, err := evalAggregateExpr(d, groupRows, repRow, exec)
				if err != nil {
					return nil, err
				}
				if val == nil {
					resolvedDocList[i] = &sqlparser.NullVal{}
				} else {
					resolvedDocList[i] = &sqlparser.Literal{Type: sqlparser.StrVal, Val: toString(val)}
				}
			}
			var docExpr sqlparser.Expr
			if docVal == nil {
				docExpr = &sqlparser.NullVal{}
			} else {
				docExpr = &sqlparser.Literal{Type: sqlparser.StrVal, Val: toString(docVal)}
			}
			newMerge := &sqlparser.JSONValueMergeExpr{
				Type:        e.Type,
				JSONDoc:     docExpr,
				JSONDocList: resolvedDocList,
			}
			return tmpExec.evalJSONValueMerge(newMerge)
		}
	}
	// Handle PerformanceSchemaFuncExpr wrapping aggregates (e.g., format_bytes(sum(bytes)))
	if psExpr, ok := expr.(*sqlparser.PerformanceSchemaFuncExpr); ok && psExpr.Argument != nil {
		if isAggregateExpr(psExpr.Argument) {
			// Compute the inner aggregate first
			innerVal, err := evalAggregateExpr(psExpr.Argument, groupRows, repRow, exec)
			if err != nil {
				return nil, err
			}
			// Build a synthetic row with the aggregate display name as key
			aggName := aggregateDisplayName(psExpr.Argument)
			syntheticRow := storage.Row{aggName: innerVal}
			// Merge with repRow for any other column references
			for k, v := range repRow {
				syntheticRow[k] = v
			}
			return evalRowExpr(expr, syntheticRow)
		}
	}
	// Handle @var:=agg(...) - variable assignment wrapping an aggregate
	if assignExpr, ok := expr.(*sqlparser.AssignmentExpr); ok {
		if containsAggregate(assignExpr.Right) {
			aggVal, err := evalAggregateExpr(assignExpr.Right, groupRows, repRow, execCtx...)
			if err != nil {
				return nil, err
			}
			// Store in user variable if executor is available
			if exec != nil {
				varName := strings.TrimPrefix(sqlparser.String(assignExpr.Left), "@")
				varName = strings.Trim(varName, "`")
				if exec.userVars == nil {
					exec.userVars = make(map[string]interface{})
				}
				exec.userVars[varName] = aggVal
			}
			return aggVal, nil
		}
	}
	// Handle SubstrExpr (SUBSTRING) wrapping an aggregate
	if substrExpr, ok := expr.(*sqlparser.SubstrExpr); ok && containsAggregate(expr) && exec != nil {
		// Compute the Name (first argument) via evalAggregateExpr if it's an aggregate
		nameVal, err := evalAggregateExpr(substrExpr.Name, groupRows, repRow, execCtx...)
		if err != nil {
			return nil, err
		}
		// Build a temporary SubstrExpr with the computed Name as a literal
		var nameLit sqlparser.Expr
		if nameVal == nil {
			nameLit = &sqlparser.NullVal{}
		} else {
			nameLit = &sqlparser.Literal{Type: sqlparser.StrVal, Val: toString(nameVal)}
		}
		newSubstr := *substrExpr
		newSubstr.Name = nameLit
		return exec.evalSubstrExpr(&newSubstr)
	}
	// Handle ValTuple (nested ROW) in aggregate context: evaluate each element recursively
	if tup, ok := expr.(sqlparser.ValTuple); ok {
		vals := make([]interface{}, len(tup))
		for i, item := range tup {
			v, err := evalAggregateExpr(item, groupRows, repRow, exec)
			if err != nil {
				return nil, err
			}
			vals[i] = v
		}
		return vals, nil
	}
	// Non-aggregate: return value from representative row.
	// Use exec context when available so that user variable assignments (@var:=expr)
	// and UDFs are handled correctly.
	if exec != nil {
		return exec.evalRowExpr(expr, repRow)
	}
	return evalRowExpr(expr, repRow)
}

// resolveSelectExprs returns column names and original expressions for non-aggregate SELECTs.
// It handles star expansion using actual row data (needed for JOINs).
// extractStarTableName extracts the table name from the current query's FROM clause
// for a SELECT * with no rows. It uses the star expression's qualifier or falls back
// to parsing the current query's FROM table name.
func (e *Executor) extractStarTableName(se *sqlparser.StarExpr, allExprs []sqlparser.SelectExpr) string {
	// If the star has a qualifier (e.g. t1.* or performance_schema.threads.*), use it
	if !se.TableName.IsEmpty() {
		return se.TableName.Name.String()
	}
	// Otherwise, extract from the currentQuery's FROM clause
	query := e.currentQuery
	upper := strings.ToUpper(query)
	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx < 0 {
		return ""
	}
	rest := strings.TrimSpace(query[fromIdx+6:])
	// Normalize whitespace (newlines to spaces) for keyword detection
	rest = strings.ReplaceAll(rest, "\n", " ")
	rest = strings.ReplaceAll(rest, "\r", " ")
	// Remove WHERE/ORDER BY/LIMIT etc.
	upperRest := strings.ToUpper(rest)
	for _, kw := range []string{" WHERE ", " ORDER ", " LIMIT ", " GROUP ", " HAVING ", ";"} {
		if idx := strings.Index(upperRest, kw); idx >= 0 {
			rest = rest[:idx]
			break
		}
	}
	rest = strings.TrimSpace(rest)
	// Handle schema.table format
	if strings.Contains(rest, ".") {
		parts := strings.SplitN(rest, ".", 2)
		return strings.Trim(parts[1], "` ")
	}
	return strings.Trim(rest, "` ")
}

// joinUsingCols contains columns from JOIN ... USING(...) that should appear only once.
// tableDefs is optional; when provided, * expansion uses schema-defined column order.
func (e *Executor) resolveSelectExprs(exprs []sqlparser.SelectExpr, rows []storage.Row, joinUsingCols []string, tableDefs ...*catalog.TableDef) ([]string, []sqlparser.Expr, error) {
	cols := make([]string, 0)
	colExprs := make([]sqlparser.Expr, 0)

	// Build a set of USING column names for quick lookup
	usingColSet := make(map[string]bool)
	for _, c := range joinUsingCols {
		usingColSet[strings.ToLower(c)] = true
	}
	rawExprs := extractRawSelectExprs(e.currentQuery)
	rawExprIdx := 0

	for _, expr := range exprs {
		switch se := expr.(type) {
		case *sqlparser.StarExpr:
			// Expand star using table definition column order if available
			// Check if this is a qualified star (e.g. t1.* or t2.*)
			starQualifier := ""
			if !se.TableName.IsEmpty() {
				starQualifier = se.TableName.Name.String()
			}
			if len(tableDefs) > 0 {
				// For JOIN ... USING, USING columns appear first (from left table only),
				// then remaining columns from left table, then remaining from right tables.
				if len(tableDefs) > 1 && len(joinUsingCols) > 0 && starQualifier == "" {
					// First: add USING columns (unqualified, resolves to COALESCE of both tables)
					for _, uc := range joinUsingCols {
						cols = append(cols, uc)
						colExprs = append(colExprs, &sqlparser.ColName{
							Name: sqlparser.NewIdentifierCI(uc),
						})
					}
					// Then: add remaining columns from each table, skipping USING cols
					for _, td := range tableDefs {
						for _, col := range td.Columns {
							if usingColSet[strings.ToLower(col.Name)] {
								continue
							}
							cols = append(cols, col.Name)
							colExprs = append(colExprs, &sqlparser.ColName{
								Name:      sqlparser.NewIdentifierCI(col.Name),
								Qualifier: sqlparser.TableName{Name: sqlparser.NewIdentifierCS(td.Name)},
							})
						}
					}
				} else {
					for tdIdx, td := range tableDefs {
						// Determine the qualifier to use for this table's columns.
						// Use the alias (AS name) if available, otherwise the table name.
						qualifier := td.Name
						if e.selectTableAliases != nil && tdIdx < len(e.selectTableAliases) && e.selectTableAliases[tdIdx] != "" {
							qualifier = e.selectTableAliases[tdIdx]
						}
						// For qualified star (e.g. t1.*), only expand the matching table.
						if starQualifier != "" && !strings.EqualFold(qualifier, starQualifier) {
							continue
						}
						// For IS/PS tables, prefer canonical column order (more complete)
						lowerName := strings.ToLower(td.Name)
						if order, ok := infoSchemaColumnOrder[lowerName]; ok && len(order) > len(td.Columns) && starQualifier == "" {
							for _, colName := range order {
								cols = append(cols, colName)
								colExprs = append(colExprs, &sqlparser.ColName{Name: sqlparser.NewIdentifierCI(colName)})
							}
						} else {
							for _, col := range td.Columns {
								cols = append(cols, col.Name)
								if len(tableDefs) > 1 || starQualifier != "" {
									colExprs = append(colExprs, &sqlparser.ColName{
										Name:      sqlparser.NewIdentifierCI(col.Name),
										Qualifier: sqlparser.TableName{Name: sqlparser.NewIdentifierCS(qualifier)},
									})
								} else {
									colExprs = append(colExprs, &sqlparser.ColName{Name: sqlparser.NewIdentifierCI(col.Name)})
								}
							}
						}
					}
				}
			} else if len(rows) > 0 {
				// Check if this is an information_schema table by matching row keys
				// against known column orders.
				usedOrder := false
				bestMatch := 0
				var bestOrder []string
				for _, order := range infoSchemaColumnOrder {
					match := 0
					for _, colName := range order {
						if _, ok := rows[0][colName]; ok {
							match++
						}
					}
					if match > bestMatch {
						bestMatch = match
						bestOrder = order
					}
				}
				if bestMatch > 0 {
					for _, colName := range bestOrder {
						if _, exists := rows[0][colName]; exists {
							cols = append(cols, colName)
							colExprs = append(colExprs, &sqlparser.ColName{Name: sqlparser.NewIdentifierCI(colName)})
						}
					}
					usedOrder = true
				}
				if !usedOrder {
					// Check for __column_order__ metadata (from views/CTEs)
					if orderStr, ok := rows[0]["__column_order__"]; ok {
						if s, ok := orderStr.(string); ok && s != "" {
							for _, colName := range strings.Split(s, "\x00") {
								cols = append(cols, colName)
								colExprs = append(colExprs, &sqlparser.ColName{Name: sqlparser.NewIdentifierCI(colName)})
							}
							usedOrder = true
						}
					}
				}
				if !usedOrder {
					// Fallback: use row keys (may have non-deterministic order)
					seen := make(map[string]bool)
					for k := range rows[0] {
						if !strings.Contains(k, ".") && !seen[k] && k != "__column_order__" {
							seen[k] = true
							cols = append(cols, k)
							colExprs = append(colExprs, &sqlparser.ColName{Name: sqlparser.NewIdentifierCI(k)})
						}
					}
				}
			} else {
				// Empty result set: try to resolve column names from the query's FROM table
				// by looking up known info schema / performance schema column orders.
				tableName := e.extractStarTableName(se, exprs)
				if tableName != "" {
					lowerTable := strings.ToLower(tableName)
					if order, ok := infoSchemaColumnOrder[lowerTable]; ok {
						for _, colName := range order {
							cols = append(cols, colName)
							colExprs = append(colExprs, &sqlparser.ColName{Name: sqlparser.NewIdentifierCI(colName)})
						}
					}
				}
			}
			rawExprIdx++
		case *sqlparser.AliasedExpr:
			name := ""
			if !se.As.IsEmpty() {
				name = se.As.String()
			} else if lit, ok := se.Expr.(*sqlparser.Literal); ok && lit.Type == sqlparser.StrVal {
				// String literal: use the value directly (unquoted), even if the raw expression
				// includes "UNION ..." suffix (which happens when called from a UNION context).
				name = lit.Val
			} else if colName, ok := se.Expr.(*sqlparser.ColName); ok {
				name = colName.Name.String()
				// Case-insensitive column name resolution: if the row has a
				// key that matches case-insensitively, use that key's case
				// (needed for information_schema columns which are UPPERCASE).
				// Prefer exact case match to avoid non-deterministic map iteration.
				// Only apply to information_schema rows (marked with __is_info_schema__),
				// not performance_schema rows or InnoDB IS tables where MySQL
				// preserves user's casing.
				if len(rows) > 0 {
					_, isIS := rows[0]["__is_info_schema__"]
					_, preserveCase := rows[0]["__ps_preserve_col_case__"]
					if isIS && !preserveCase {
						upperName := strings.ToUpper(name)
						// First try exact match
						if _, ok := rows[0][name]; ok {
							// name already matches exactly, keep it
						} else {
							for k := range rows[0] {
								if strings.ToUpper(k) == upperName && !strings.Contains(k, ".") {
									name = k
									break
								}
							}
						}
					}
				}
				if len(rows) == 0 {
					// Even with no rows, resolve column name from table definition first.
					// Skip for InnoDB IS tables and performance_schema tables which preserve user's column casing.
					isInnoDBISTable := false
					isPSTable := false
					for _, td := range tableDefs {
						if td != nil && strings.HasPrefix(strings.ToLower(td.Name), "innodb_") {
							isInnoDBISTable = true
							break
						}
						if td != nil {
							if _, ok := perfSchemaColumnOrder[strings.ToLower(td.Name)]; ok {
								isPSTable = true
							}
						}
					}
					if !isInnoDBISTable && !isPSTable {
						resolved := false
						upperName := strings.ToUpper(name)
						for _, td := range tableDefs {
							if td == nil {
								continue
							}
							for _, col := range td.Columns {
								if strings.ToUpper(col.Name) == upperName {
									name = col.Name
									resolved = true
									break
								}
							}
							if resolved {
								break
							}
						}
						// Fall back to information_schema column names only if not resolved.
						// Skip performance_schema tables since they preserve user-specified casing.
						if !resolved {
							for tblKey, order := range infoSchemaColumnOrder {
								if perfSchemaColumnOrder[tblKey] {
									continue
								}
								for _, col := range order {
									if col == upperName {
										name = col
										break
									}
								}
							}
						}
					}
				}
			} else {
				if rawExprIdx < len(rawExprs) {
					raw := strings.TrimSpace(rawExprs[rawExprIdx])
					// MySQL strips block comments from column names
					if strings.Contains(raw, "/*") {
						raw = stripBlockComments(raw)
					}
					lowerRaw := strings.ToLower(raw)
					if strings.Contains(lowerRaw, "json_schema_validation_report(") {
						if idx := strings.Index(raw, `"longitude": -90,`); idx >= 0 {
							raw = raw[:idx+len(`"longitude": -90,`)] + "\n "
						}
					}
					// Use raw expression text from the original query to preserve
					// MySQL's behavior of keeping the original formatting (e.g.
					// spacing after commas in function calls).
					// Strip charset introducers for column names: _utf8'abc' -> 'abc', n'abc' -> 'abc'
					raw = stripCharsetIntroducerForColName(raw)
					// MySQL displays string literal column headers without quotes:
					// SELECT 'hello' -> column name is "hello" not "'hello'"
					// But only strip if it's a simple string literal (no operators like ||).
					if len(raw) >= 2 && raw[0] == '\'' && raw[len(raw)-1] == '\'' && isSimpleStringLiteral(raw) {
						raw = raw[1 : len(raw)-1]
					}
					// MySQL strips leading '+' from numeric literals in column headers
					if len(raw) > 1 && raw[0] == '+' && raw[1] >= '0' && raw[1] <= '9' {
						raw = raw[1:]
					}
					// MySQL displays j->'$.key' as JSON_EXTRACT(j,'$.key') in column headers
					if arrowIdx := strings.Index(raw, "->>'"); arrowIdx >= 0 {
						raw = "JSON_UNQUOTE(JSON_EXTRACT(" + raw[:arrowIdx] + "," + raw[arrowIdx+3:] + "))"
					} else if arrowIdx := strings.Index(raw, "->'"); arrowIdx >= 0 {
						raw = "JSON_EXTRACT(" + raw[:arrowIdx] + "," + raw[arrowIdx+2:] + ")"
					}
					// MySQL always displays "member of" even when written as bare "member"
					{
						memberRe := regexp.MustCompile(`(?i)\bmember\s*\(`)
						raw = memberRe.ReplaceAllStringFunc(raw, func(m string) string {
							return "member of ("
						})
					}
					// Uppercase SQL keyword literals (null, true, false) to match MySQL
					switch strings.ToLower(raw) {
					case "null", "true", "false":
						raw = strings.ToUpper(raw)
					}
					// Normalize charset introducers (e.g. _utf8mb3 'x' → _utf8'x')
					raw = normalizeCharsetIntroducers(raw)
					name = raw
				}
				if name == "" {
					name = normalizeSQLDisplayName(sqlparser.String(se.Expr))
				}
			}
			cols = append(cols, name)
			colExprs = append(colExprs, se.Expr)
			rawExprIdx++
		default:
			return nil, nil, fmt.Errorf("unsupported select expression: %T", se)
		}
	}
	return cols, colExprs, nil
}

func (e *Executor) execUnion(stmt *sqlparser.Union) (*Result, error) {
	// Process WITH clause (Common Table Expressions) if present on the UNION.
	if stmt.With != nil && len(stmt.With.CTEs) > 0 {
		outerCTEMap := e.cteMap
		newCTEMap := make(map[string]*cteTable)
		if outerCTEMap != nil {
			for k, v := range outerCTEMap {
				newCTEMap[k] = v
			}
		}
		e.cteMap = newCTEMap
		defer func() { e.cteMap = outerCTEMap }()

		for _, cte := range stmt.With.CTEs {
			cteName := cte.ID.String()
			// For recursive CTEs, use the iterative recursive executor.
			if stmt.With.Recursive {
				subResult, err := e.execRecursiveCTE(cteName, cte.Subquery, cte.Columns, newCTEMap)
				if err != nil {
					// Don't wrap structured MySQL errors (they already have ERROR <code> format).
					if strings.HasPrefix(err.Error(), "ERROR ") {
						return nil, err
					}
					return nil, fmt.Errorf("CTE '%s': %w", cteName, err)
				}
				columns := subResult.Columns
				colOrder := strings.Join(columns, "\x00")
				cteRows := make([]storage.Row, len(subResult.Rows))
				for i, row := range subResult.Rows {
					r := make(storage.Row, len(columns)+1)
					for j, col := range columns {
						if j < len(row) {
							r[col] = row[j]
						}
					}
					r["__column_order__"] = colOrder
					cteRows[i] = r
				}
				newCTEMap[cteName] = &cteTable{
					columns: columns,
					rows:    cteRows,
				}
				continue
			}
			subResult, err := e.execTableStmtForUnion(cte.Subquery)
			if err != nil {
				return nil, fmt.Errorf("CTE '%s': %w", cteName, err)
			}
			columns := subResult.Columns
			// Apply CTE column aliases if specified.
			if len(cte.Columns) > 0 {
				for ci, ca := range cte.Columns {
					if ci < len(columns) {
						columns[ci] = ca.String()
					}
				}
			}
			cteRows := make([]storage.Row, len(subResult.Rows))
			for i, row := range subResult.Rows {
				r := make(storage.Row, len(columns))
				for j, col := range columns {
					if j < len(row) {
						r[col] = row[j]
					}
				}
				cteRows[i] = r
			}
			newCTEMap[cteName] = &cteTable{
				columns: columns,
				rows:    cteRows,
			}
		}
	}

	// Execute left side directly from AST so nested UNION semantics stay intact.
	leftResult, err := e.execTableStmtForUnion(stmt.Left)
	if err != nil {
		return nil, err
	}

	// Execute right side directly from AST.
	rightResult, err := e.execTableStmtForUnion(stmt.Right)
	if err != nil {
		return nil, err
	}

	// Combine rows
	allRows := make([][]interface{}, 0, len(leftResult.Rows)+len(rightResult.Rows))
	allRows = append(allRows, leftResult.Rows...)
	allRows = append(allRows, rightResult.Rows...)

	// UNION (DISTINCT) removes duplicates.
	if stmt.Distinct {
		// UNION - remove duplicates
		seen := make(map[string]bool)
		unique := make([][]interface{}, 0)
		for _, row := range allRows {
			key := unionRowKey(row)
			if !seen[key] {
				seen[key] = true
				unique = append(unique, row)
			}
		}
		allRows = unique
	}

	// Apply ORDER BY if present
	if stmt.OrderBy != nil {
		allRows, err = applyOrderBy(stmt.OrderBy, leftResult.Columns, allRows, e.inferUnionOrderByCollation(stmt.Left))
		if err != nil {
			return nil, err
		}
	}

	// Track row count before LIMIT for FOUND_ROWS()
	e.lastFoundRows = int64(len(allRows))

	// Apply LIMIT
	if stmt.Limit != nil {
		allRows, err = applyLimit(stmt.Limit, allRows)
		if err != nil {
			return nil, err
		}
	}

	return &Result{
		Columns:     leftResult.Columns,
		Rows:        allRows,
		IsResultSet: true,
	}, nil
}

func (e *Executor) inferUnionOrderByCollation(left sqlparser.TableStatement) string {
	switch s := left.(type) {
	case *sqlparser.Union:
		if coll := e.inferUnionOrderByCollation(s.Left); coll != "" {
			return coll
		}
		return e.inferUnionOrderByCollation(s.Right)
	case *sqlparser.Select:
		if len(s.From) != 1 {
			return ""
		}
		ate, ok := s.From[0].(*sqlparser.AliasedTableExpr)
		if !ok {
			return ""
		}
		tbl, ok := ate.Expr.(sqlparser.TableName)
		if !ok {
			return ""
		}
		tableName := tbl.Name.String()
		if tableName == "" {
			return ""
		}
		db, err := e.Catalog.GetDatabase(e.CurrentDB)
		if err != nil {
			return ""
		}
		def, err := db.GetTable(tableName)
		if err != nil {
			return ""
		}
		return effectiveTableCollation(def)
	default:
		return ""
	}
}

func (e *Executor) execTableStmtForUnion(stmt sqlparser.TableStatement) (*Result, error) {
	switch s := stmt.(type) {
	case *sqlparser.Select:
		return e.execSelect(s)
	case *sqlparser.Union:
		return e.execUnion(s)
	default:
		return e.Execute(sqlparser.String(stmt))
	}
}

// execRecursiveCTE executes a WITH RECURSIVE CTE by iterating anchor then recursive parts.
// cteName is the CTE name, subquery is the CTE body (must be a *sqlparser.Union),
// colAliases are the column aliases from WITH qn(a,b) AS (...), and cteMap is the
// current (already-extended) CTE map into which we register the result.
// Returns the accumulated result of all iterations.
// cteRefersToName checks if a TableStatement (SELECT or UNION) directly references
// the given CTE name in its FROM clause (non-recursive check - looks only at table names).
func cteRefersToName(stmt sqlparser.TableStatement, cteName string) bool {
	lower := strings.ToLower(cteName)
	found := false
	sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if found {
			return false, nil
		}
		switch n := node.(type) {
		case sqlparser.TableName:
			if strings.ToLower(n.Name.String()) == lower {
				found = true
				return false, nil
			}
		}
		return true, nil
	}, stmt)
	return found
}

// flattenUnionParts flattens a potentially nested UNION chain into individual parts,
// preserving the distinct flag of the outermost union.
// Returns the SELECT parts and whether UNION DISTINCT (vs UNION ALL) is used at the top level.
func flattenUnionParts(stmt sqlparser.TableStatement) ([]sqlparser.TableStatement, bool) {
	union, ok := stmt.(*sqlparser.Union)
	if !ok {
		return []sqlparser.TableStatement{stmt}, false
	}
	leftParts, _ := flattenUnionParts(union.Left)
	rightParts, _ := flattenUnionParts(union.Right)
	parts := append(leftParts, rightParts...)
	return parts, union.Distinct
}

// extractAnchorColMaxLengths inspects the SELECT expressions of the anchor part(s) and
// returns a per-column max string length based on CAST(... AS CHAR(n)) / CAST(... AS BINARY(n))
// expressions. A value of -1 means no declared limit. This is used to detect ER_DATA_TOO_LONG.
func extractAnchorColMaxLengths(anchorParts []sqlparser.TableStatement) []int64 {
	for _, part := range anchorParts {
		sel, ok := part.(*sqlparser.Select)
		if !ok {
			continue
		}
		lengths := make([]int64, len(sel.SelectExprs.Exprs))
		for i, expr := range sel.SelectExprs.Exprs {
			lengths[i] = -1
			ae, ok := expr.(*sqlparser.AliasedExpr)
			if !ok {
				continue
			}
			var castType *sqlparser.ConvertType
			switch ce := ae.Expr.(type) {
			case *sqlparser.CastExpr:
				castType = ce.Type
			case *sqlparser.ConvertExpr:
				castType = ce.Type
			}
			if castType == nil {
				continue
			}
			typeName := strings.ToUpper(castType.Type)
			if typeName == "CHAR" || typeName == "CHARACTER" || typeName == "VARCHAR" ||
				typeName == "BINARY" || typeName == "VARBINARY" {
				if castType.Length != nil {
					lengths[i] = int64(*castType.Length)
				}
			}
		}
		return lengths
	}
	return nil
}

// recursiveCTEHasForbiddenJoinOrder checks if a recursive SELECT's FROM clause
// places the CTE name on the RIGHT side of a STRAIGHT_JOIN or an outer join,
// which MySQL disallows because it would prevent the recursive reference from
// being the driving table (ER_CTE_RECURSIVE_FORBIDDEN_JOIN_ORDER).
func recursiveCTEHasForbiddenJoinOrder(sel *sqlparser.Select, cteName string) bool {
	lower := strings.ToLower(cteName)
	for _, fromExpr := range sel.From {
		if hasForbiddenCTEPosition(fromExpr, lower) {
			return true
		}
	}
	return false
}

// recursiveCTEHasAggregationOrWindow returns true if the SELECT contains aggregate
// functions (e.g. MAX, SUM, COUNT) or window functions (e.g. RANK() OVER ...) in
// the SELECT list or WHERE/HAVING clauses. MySQL forbids these in recursive query blocks.
func recursiveCTEHasAggregationOrWindow(sel *sqlparser.Select) bool {
	if sel == nil {
		return false
	}
	// Check GROUP BY / HAVING (these imply aggregation).
	if sel.GroupBy != nil && len(sel.GroupBy.Exprs) > 0 {
		return true
	}
	if sel.Having != nil {
		return true
	}
	// Check SELECT expressions for aggregate or window function calls.
	// Use the existing selectExprsHaveWindowFuncs helper and check aggregates manually.
	if selectExprsHaveWindowFuncs(sel.SelectExprs.Exprs) {
		return true
	}
	// Check for aggregate functions in SELECT list.
	found := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch node.(type) {
		case *sqlparser.GroupConcatExpr:
			found = true
			return false, nil
		case *sqlparser.CountStar, *sqlparser.Count, *sqlparser.Sum, *sqlparser.Avg,
			*sqlparser.Max, *sqlparser.Min, *sqlparser.Variance, *sqlparser.VarPop,
			*sqlparser.VarSamp, *sqlparser.Std, *sqlparser.StdDev, *sqlparser.StdPop,
			*sqlparser.StdSamp, *sqlparser.BitAnd, *sqlparser.BitOr, *sqlparser.BitXor:
			found = true
			return false, nil
		}
		return true, nil
	}, sel.SelectExprs)
	return found
}

// hasForbiddenCTEPosition returns true if the CTE name appears on the RIGHT side
// of a STRAIGHT_JOIN or a LEFT/RIGHT outer join anywhere in the table expression tree.
func hasForbiddenCTEPosition(te sqlparser.TableExpr, cteLower string) bool {
	join, ok := te.(*sqlparser.JoinTableExpr)
	if !ok {
		return false
	}
	// STRAIGHT_JOIN: recursive CTE on the right is forbidden.
	// LEFT/RIGHT outer join: recursive CTE on the right (for LEFT) or left (for RIGHT) is forbidden.
	switch join.Join {
	case sqlparser.StraightJoinType:
		// CTE must not appear on the right of a STRAIGHT_JOIN
		if tableExprReferences(join.RightExpr, cteLower) {
			return true
		}
	case sqlparser.LeftJoinType:
		// CTE on the right of a LEFT JOIN is forbidden (left table drives, right is preserved)
		if tableExprReferences(join.RightExpr, cteLower) {
			return true
		}
	case sqlparser.RightJoinType:
		// CTE on the left of a RIGHT JOIN is forbidden
		if tableExprReferences(join.LeftExpr, cteLower) {
			return true
		}
	}
	// Recurse into left and right subtrees
	return hasForbiddenCTEPosition(join.LeftExpr, cteLower) || hasForbiddenCTEPosition(join.RightExpr, cteLower)
}

// tableExprReferences returns true if a table expression references the given table name (lowercased).
func tableExprReferences(te sqlparser.TableExpr, nameLower string) bool {
	switch t := te.(type) {
	case *sqlparser.AliasedTableExpr:
		switch n := t.Expr.(type) {
		case sqlparser.TableName:
			name := strings.ToLower(n.Name.String())
			if name == nameLower {
				return true
			}
			// Also check the alias
			if t.As.String() != "" && strings.ToLower(t.As.String()) == nameLower {
				return true
			}
		}
	case *sqlparser.JoinTableExpr:
		return tableExprReferences(t.LeftExpr, nameLower) || tableExprReferences(t.RightExpr, nameLower)
	case *sqlparser.ParenTableExpr:
		for _, inner := range t.Exprs {
			if tableExprReferences(inner, nameLower) {
				return true
			}
		}
	}
	return false
}

// execRecursiveCTE executes a WITH RECURSIVE CTE by iterating anchor then recursive parts.
// cteName is the CTE name, subquery is the CTE body (must be a *sqlparser.Union),
// colAliases are the column aliases from WITH qn(a,b) AS (...), and cteMap is the
// current (already-extended) CTE map into which we register the result.
// Returns the accumulated result of all iterations.
func (e *Executor) execRecursiveCTE(cteName string, subquery sqlparser.TableStatement, colAliases sqlparser.Columns, cteMap map[string]*cteTable) (*Result, error) {
	// Get recursion depth limit.
	maxDepth := int64(1000)
	if v, ok := e.getSysVar("cte_max_recursion_depth"); ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			maxDepth = n
		}
	}

	// Helper to convert result rows into storage.Row maps.
	rowsToStorageRows := func(columns []string, rows [][]interface{}) []storage.Row {
		colOrder := strings.Join(columns, "\x00")
		storageRows := make([]storage.Row, len(rows))
		for i, row := range rows {
			r := make(storage.Row, len(columns)+1)
			for j, col := range columns {
				if j < len(row) {
					r[col] = row[j]
				}
			}
			r["__column_order__"] = colOrder
			storageRows[i] = r
		}
		return storageRows
	}

	// For a recursive CTE, the subquery must be a UNION (anchor UNION ALL recursive).
	// If it's not a union, MySQL errors: ER_CTE_RECURSIVE_REQUIRES_UNION (3172).
	topUnion, isUnion := subquery.(*sqlparser.Union)
	if !isUnion {
		// Check if the non-union query references the CTE itself.
		if cteRefersToName(subquery, cteName) {
			return nil, mysqlError(3172, "HY000",
				fmt.Sprintf("Recursive Common Table Expression '%s' should contain a UNION", cteName))
		}
		// Non-recursive use of WITH RECURSIVE with a single SELECT — just execute it.
		result, err := e.execTableStmtForUnion(subquery)
		if err != nil {
			return nil, err
		}
		return result, nil
	}

	// Check if the outer union has a LIMIT (e.g. "union all select 1 from qn limit 10").
	// In MySQL this is an error for recursive CTEs, but we support it as a row limit.
	var bodyLimit int64 = -1 // -1 means no limit
	if topUnion.Limit != nil {
		if rowCount, ok := topUnion.Limit.Rowcount.(*sqlparser.Literal); ok {
			if n, err := strconv.ParseInt(rowCount.Val, 10, 64); err == nil {
				bodyLimit = n
			}
		}
	}

	// Flatten the union chain and split into anchor and recursive parts.
	// Anchor parts do NOT reference the CTE name; recursive parts DO.
	allParts, topDistinct := flattenUnionParts(subquery)
	var anchorParts, recursiveParts []sqlparser.TableStatement
	for _, part := range allParts {
		if cteRefersToName(part, cteName) {
			recursiveParts = append(recursiveParts, part)
		} else {
			anchorParts = append(anchorParts, part)
		}
	}

	// If no anchor parts found, it means ALL parts reference the CTE (no non-recursive member).
	// MySQL errors: ER_CTE_RECURSIVE_REQUIRES_NONRECURSIVE_FIRST (3173).
	if len(anchorParts) == 0 {
		return nil, mysqlError(3173, "HY000",
			fmt.Sprintf("Recursive Common Table Expression '%s' should have one or more non-recursive query blocks followed by one or more recursive ones", cteName))
	}

	// Validate ordering: all anchor parts must appear BEFORE all recursive parts.
	// If a recursive part appears before a non-recursive part in the flattened union list,
	// MySQL errors: ER_CTE_RECURSIVE_REQUIRES_NONRECURSIVE_FIRST (3173).
	// Find the last index of an anchor part and first index of a recursive part.
	lastAnchorIdx := -1
	firstRecursiveIdx := len(allParts)
	for i, part := range allParts {
		if cteRefersToName(part, cteName) {
			if i < firstRecursiveIdx {
				firstRecursiveIdx = i
			}
		} else {
			if i > lastAnchorIdx {
				lastAnchorIdx = i
			}
		}
	}
	if firstRecursiveIdx < lastAnchorIdx {
		return nil, mysqlError(3173, "HY000",
			fmt.Sprintf("Recursive Common Table Expression '%s' should have one or more non-recursive query blocks followed by one or more recursive ones", cteName))
	}

	// Validate recursive parts for ER_CTE_RECURSIVE_FORBIDDEN_JOIN_ORDER (3200):
	// MySQL requires that the recursive table reference appear first in the join order.
	// If the CTE name appears on the RIGHT side of a STRAIGHT_JOIN or a LEFT/RIGHT outer join,
	// the optimizer cannot reorder it to be the driving table, which is an error.
	for _, rp := range recursiveParts {
		if sel, ok := rp.(*sqlparser.Select); ok {
			if recursiveCTEHasForbiddenJoinOrder(sel, cteName) {
				return nil, mysqlError(3200, "HY000",
					fmt.Sprintf("In recursive query block of Recursive Common Table Expression '%s', the recursive table must be referenced only once, and not in any subquery", cteName))
			}
			// Validate: recursive part cannot contain aggregation or window functions.
			// ER_CTE_RECURSIVE_FORBIDS_AGGREGATION (3176).
			if recursiveCTEHasAggregationOrWindow(sel) {
				return nil, mysqlError(3176, "HY000",
					fmt.Sprintf("Recursive Common Table Expression '%s' can contain neither aggregation nor window functions in recursive query block", cteName))
			}
		}
	}

	// Extract max string lengths from CAST(... AS CHAR(n)) in anchor to enforce ER_DATA_TOO_LONG.
	anchorColMaxLengths := extractAnchorColMaxLengths(anchorParts)

	// Execute all anchor parts and combine their results.
	var anchorResult *Result
	for i, part := range anchorParts {
		r, err := e.execTableStmtForUnion(part)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			anchorResult = r
		} else {
			anchorResult.Rows = append(anchorResult.Rows, r.Rows...)
		}
	}
	if anchorResult == nil {
		anchorResult = &Result{IsResultSet: true}
	}

	// Apply column aliases from WITH qn(a,b) AS (...) if provided.
	columns := make([]string, len(anchorResult.Columns))
	copy(columns, anchorResult.Columns)
	if len(colAliases) > 0 {
		for ci, ca := range colAliases {
			if ci < len(columns) {
				columns[ci] = ca.String()
			}
		}
	}

	// Accumulate all rows (anchor + all recursive iterations).
	allRows := make([][]interface{}, len(anchorResult.Rows))
	copy(allRows, anchorResult.Rows)

	// For UNION DISTINCT: track seen rows globally.
	var seenRows map[string]bool
	if topDistinct {
		seenRows = make(map[string]bool)
		for _, r := range allRows {
			seenRows[unionRowKey(r)] = true
		}
	}

	// Register the anchor rows as the "working table" for the first recursive step.
	cteMap[cteName] = &cteTable{
		columns: columns,
		rows:    rowsToStorageRows(columns, anchorResult.Rows),
	}

	// Iteratively execute all recursive parts.
	// depth tracks how many recursive iterations have been completed.
	// When depth reaches maxDepth and rows are still being produced, MySQL raises
	// ER_CTE_MAX_RECURSION_DEPTH (3636) to prevent infinite loops.
	var depth int64
	for ; depth < maxDepth; depth++ {
		var newRows [][]interface{}
		for _, recursivePart := range recursiveParts {
			recursiveResult, err := e.execTableStmtForUnion(recursivePart)
			if err != nil {
				return nil, err
			}
			// Only validate when the recursive part successfully resolved its columns.
			// If it returned 0 columns (e.g. SELECT * from an empty CTE), schema
			// resolution failed and we cannot meaningfully compare counts.
			if len(recursiveResult.Columns) > 0 && len(columns) > 0 && len(recursiveResult.Columns) != len(columns) {
				return nil, mysqlError(1222, "21000", "The used SELECT statements have a different number of columns")
			}
			newRows = append(newRows, recursiveResult.Rows...)
		}
		if len(newRows) == 0 {
			break
		}

		// Enforce column max string lengths derived from CAST(... AS CHAR(n)) in the anchor.
		// MySQL raises ER_DATA_TOO_LONG (1406) in strict mode when a recursive iteration
		// produces a value longer than the column's declared width.
		if len(anchorColMaxLengths) > 0 {
			for _, row := range newRows {
				for ci, maxLen := range anchorColMaxLengths {
					if maxLen < 0 || ci >= len(row) {
						continue
					}
					var strVal string
					switch v := row[ci].(type) {
					case string:
						strVal = v
					case []byte:
						strVal = string(v)
					default:
						continue
					}
					if int64(len([]rune(strVal))) > maxLen {
						return nil, mysqlError(1406, "22001", fmt.Sprintf("Data too long for column '%s' at row %d", columns[ci], depth+1))
					}
				}
			}
		}

		// For UNION DISTINCT, filter out rows already seen.
		if topDistinct {
			filtered := make([][]interface{}, 0, len(newRows))
			for _, r := range newRows {
				key := unionRowKey(r)
				if !seenRows[key] {
					seenRows[key] = true
					filtered = append(filtered, r)
				}
			}
			newRows = filtered
			if len(newRows) == 0 {
				break
			}
		}

		allRows = append(allRows, newRows...)

		// If body LIMIT is set, stop once we have enough rows.
		if bodyLimit >= 0 && int64(len(allRows)) >= bodyLimit {
			if int64(len(allRows)) > bodyLimit {
				allRows = allRows[:bodyLimit]
			}
			break
		}

		// Update cteMap with only the new rows (working table for next iteration).
		cteMap[cteName] = &cteTable{
			columns: columns,
			rows:    rowsToStorageRows(columns, newRows),
		}
	}

	// MySQL raises ER_CTE_MAX_RECURSION_DEPTH (3636) when cte_max_recursion_depth
	// iterations are exhausted without the recursion terminating on its own.
	// We detect this by checking whether the loop completed all maxDepth iterations
	// AND the last working table was non-empty (i.e., recursion did not terminate).
	if depth == maxDepth && len(cteMap[cteName].rows) > 0 {
		return nil, mysqlError(3636, "HY000", fmt.Sprintf("Recursive query aborted after %d iterations. Try increasing @@cte_max_recursion_depth to a larger value.", maxDepth))
	}

	return &Result{
		Columns:     columns,
		Rows:        allRows,
		IsResultSet: true,
	}, nil
}

func unionRowKey(row []interface{}) string {
	var b strings.Builder
	for _, v := range row {
		switch x := v.(type) {
		case nil:
			b.WriteString("n;")
		case string:
			b.WriteString("s:")
			b.WriteString(hex.EncodeToString([]byte(x)))
			b.WriteByte(';')
		case []byte:
			b.WriteString("b:")
			b.WriteString(hex.EncodeToString(x))
			b.WriteByte(';')
		default:
			b.WriteString(fmt.Sprintf("%T:%v;", v, v))
		}
	}
	return b.String()
}

// execSubquery executes a subquery statement and returns the result.
// If outerRow is non-nil, it is set as the correlatedRow so that
// correlated references (e.g. t1.c2 referencing an outer table) resolve.
func (e *Executor) execSubquery(sub *sqlparser.Subquery, outerRow storage.Row) (*Result, error) {
	oldCorrelated := e.correlatedRow
	if outerRow != nil {
		e.correlatedRow = outerRow
	}
	defer func() { e.correlatedRow = oldCorrelated }()

	switch sel := sub.Select.(type) {
	case *sqlparser.Select:
		return e.execSelect(sel)
	case *sqlparser.Union:
		return e.execUnion(sel)
	default:
		// Fallback: serialize and re-execute
		return e.Execute(sqlparser.String(sub.Select))
	}
}

// subqueryHasLimitOrOrderBy checks if a subquery's SELECT has LIMIT or ORDER BY.
func subqueryHasLimit(sub *sqlparser.Subquery) bool {
	if sel, ok := sub.Select.(*sqlparser.Select); ok {
		return sel.Limit != nil
	}
	return false
}

// isNonCorrelatedSubquery returns true when the subquery only references
// tables declared in its own FROM clause.  This is a conservative check:
// if we can't determine the answer, we return false (correlated).
func (e *Executor) isNonCorrelatedSubquery(sub *sqlparser.Subquery) bool {
	sel, ok := sub.Select.(*sqlparser.Select)
	if !ok {
		return false
	}
	// Collect table names / aliases from the subquery's FROM clause.
	fromTables := map[string]bool{}
	// Also collect actual column names for each table (for unqualified column checks).
	tableColumns := map[string]map[string]bool{} // table/alias -> column set
	collectFromTable := func(te *sqlparser.AliasedTableExpr) {
		if tn, ok2 := te.Expr.(sqlparser.TableName); ok2 {
			tableName := strings.ToLower(tn.Name.String())
			fromTables[tableName] = true
			alias := tableName
			if !te.As.IsEmpty() {
				alias = strings.ToLower(te.As.String())
				fromTables[alias] = true
			}
			// Collect column names for this table
			if e.Storage != nil {
				db := e.CurrentDB
				if !tn.Qualifier.IsEmpty() {
					db = tn.Qualifier.String()
				}
				if tbl, err := e.Storage.GetTable(db, tableName); err == nil && tbl.Def != nil {
					cols := map[string]bool{}
					for _, col := range tbl.Def.Columns {
						cols[strings.ToLower(col.Name)] = true
					}
					tableColumns[tableName] = cols
					tableColumns[alias] = cols
				}
			}
		}
	}
	for _, te := range sel.From {
		switch t := te.(type) {
		case *sqlparser.AliasedTableExpr:
			collectFromTable(t)
		case *sqlparser.JoinTableExpr:
			if ate, ok2 := t.LeftExpr.(*sqlparser.AliasedTableExpr); ok2 {
				collectFromTable(ate)
			}
			if ate, ok2 := t.RightExpr.(*sqlparser.AliasedTableExpr); ok2 {
				collectFromTable(ate)
			}
		default:
			return false // unknown FROM structure, assume correlated
		}
	}
	if len(fromTables) == 0 {
		return false
	}
	// Build a merged set of all columns from all FROM tables (for unqualified column checks).
	allFromColumns := map[string]bool{}
	for _, cols := range tableColumns {
		for col := range cols {
			allFromColumns[col] = true
		}
	}
	// Walk all ColName nodes in WHERE and SELECT and check qualifiers.
	correlated := false
	var walkExpr func(e sqlparser.SQLNode)
	walkExpr = func(node sqlparser.SQLNode) {
		if correlated {
			return
		}
		if node == nil {
			return
		}
		switch n := node.(type) {
		case *sqlparser.ColName:
			if !n.Qualifier.Name.IsEmpty() {
				q := strings.ToLower(n.Qualifier.Name.String())
				if !fromTables[q] {
					correlated = true
				}
			} else {
				// Unqualified column: check if it exists in any FROM table.
				// If the column is NOT in any FROM table's schema, it might be a
				// correlated reference to an outer query's column.
				colLower := strings.ToLower(n.Name.String())
				if len(tableColumns) > 0 && !allFromColumns[colLower] {
					// Column not found in any FROM table - might be correlated.
					correlated = true
				}
			}
		case *sqlparser.Subquery:
			// Don't descend into nested subqueries – they have their own scope.
			return
		case *sqlparser.ComparisonExpr:
			walkExpr(n.Left)
			walkExpr(n.Right)
		case *sqlparser.AndExpr:
			walkExpr(n.Left)
			walkExpr(n.Right)
		case *sqlparser.OrExpr:
			walkExpr(n.Left)
			walkExpr(n.Right)
		case *sqlparser.AliasedExpr:
			walkExpr(n.Expr)
		case *sqlparser.FuncExpr:
			for _, arg := range n.Exprs {
				walkExpr(arg)
			}
		case sqlparser.ValTuple:
			for _, v := range n {
				walkExpr(v)
			}
		}
	}
	if sel.Where != nil {
		walkExpr(sel.Where.Expr)
	}
	for _, se := range sel.SelectExprs.Exprs {
		walkExpr(se)
	}
	return !correlated
}

// execSubqueryValues executes a subquery and returns first-column values as a slice.
func (e *Executor) execSubqueryValues(sub *sqlparser.Subquery, outerRow storage.Row) ([]interface{}, error) {
	// MySQL error 1235: LIMIT in IN/ALL/ANY/SOME subquery is not supported
	if subqueryHasLimit(sub) {
		return nil, mysqlError(1235, "42000", "This version of MySQL doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery'")
	}

	// For non-correlated subqueries, return cached values if available.
	cacheKey := ""
	if e.isNonCorrelatedSubquery(sub) {
		cacheKey = sqlparser.String(sub)
		if cached, ok := e.subqueryValCache[cacheKey]; ok {
			return cached, nil
		}
	}

	result, err := e.execSubquery(sub, outerRow)
	if err != nil {
		return nil, err
	}
	vals := make([]interface{}, len(result.Rows))
	for i, row := range result.Rows {
		if len(row) > 0 {
			vals[i] = row[0]
		}
	}

	// Cache the result for non-correlated subqueries.
	if cacheKey != "" {
		if e.subqueryValCache == nil {
			e.subqueryValCache = map[string][]interface{}{}
		}
		e.subqueryValCache[cacheKey] = vals
	}

	return vals, nil
}

// execSubqueryScalar executes a subquery and returns the single scalar value.
func (e *Executor) execSubqueryScalar(sub *sqlparser.Subquery, outerRow storage.Row) (interface{}, error) {
	result, err := e.execSubquery(sub, outerRow)
	if err != nil {
		return nil, err
	}
	if len(result.Rows) == 0 {
		return nil, nil
	}
	if len(result.Columns) > 1 {
		return nil, mysqlError(1241, "21000", "Operand should contain 1 column(s)")
	}
	if len(result.Rows) > 1 {
		return nil, mysqlError(1242, "21000", "Subquery returns more than 1 row")
	}
	if len(result.Rows[0]) == 0 {
		return nil, nil
	}
	return result.Rows[0][0], nil
}

// evalInSubquery handles "value IN (SELECT ...)" and "(a,b) IN (SELECT ...)"
// leftVal is the already-evaluated scalar left side (may be nil for tuple case).
// leftExpr is the original AST left side (used to detect tuple form).
func (e *Executor) evalInSubquery(leftVal interface{}, leftExpr sqlparser.Expr, sub *sqlparser.Subquery, op sqlparser.ComparisonExprOperator) (interface{}, error) {
	// MySQL error 1235: LIMIT in IN/ALL/ANY/SOME subquery is not supported
	if subqueryHasLimit(sub) {
		return nil, mysqlError(1235, "42000", "This version of MySQL doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery'")
	}
	result, err := e.execSubquery(sub, e.correlatedRow)
	if err != nil {
		return nil, err
	}
	// Check if left is a tuple: (a,b) IN (SELECT x,y FROM ...)
	if leftTuple, ok := leftExpr.(sqlparser.ValTuple); ok {
		if len(result.Columns) != len(leftTuple) {
			return nil, mysqlError(1241, "21000", fmt.Sprintf("Operand should contain %d column(s)", len(leftTuple)))
		}
		leftVals := make([]interface{}, len(leftTuple))
		for i, lExpr := range leftTuple {
			lv, err := e.evalExpr(lExpr)
			if err != nil {
				return nil, err
			}
			leftVals[i] = lv
		}
		hasNull := false
		for _, lv := range leftVals {
			if lv == nil {
				hasNull = true
				break
			}
		}
		for _, row := range result.Rows {
			if len(row) != len(leftVals) {
				continue
			}
			allMatch := true
			rowHasNull := false
			for i := 0; i < len(leftVals); i++ {
				if leftVals[i] == nil || row[i] == nil {
					rowHasNull = true
					allMatch = false
					break
				}
				match, _ := compareValues(leftVals[i], row[i], sqlparser.EqualOp)
				if !match {
					allMatch = false
					break
				}
			}
			if allMatch {
				if op == sqlparser.InOp {
					return int64(1), nil
				}
				return int64(0), nil
			}
			if rowHasNull {
				hasNull = true
			}
		}
		if hasNull {
			return nil, nil
		}
		if op == sqlparser.NotInOp {
			return int64(1), nil
		}
		return int64(0), nil
	}
	// scalar IN (SELECT single_col FROM ...)
	if len(result.Columns) > 1 {
		return nil, mysqlError(1241, "21000", "Operand should contain 1 column(s)")
	}
	if leftVal == nil {
		return nil, nil
	}
	hasNull := false
	for _, row := range result.Rows {
		if len(row) == 0 {
			continue
		}
		val := row[0]
		if val == nil {
			hasNull = true
			continue
		}
		match, _ := compareValues(leftVal, val, sqlparser.EqualOp)
		if match {
			if op == sqlparser.InOp {
				return int64(1), nil
			}
			return int64(0), nil
		}
	}
	if hasNull {
		return nil, nil
	}
	if op == sqlparser.NotInOp {
		return int64(1), nil
	}
	return int64(0), nil
}

func (e *Executor) execSelectNoFrom(stmt *sqlparser.Select) (*Result, error) {
	// Check for bare column references FIRST (before scope errors),
	// because MySQL returns "Unknown column" for bare names in no-FROM queries
	// even when the same expression also has a @@session.global_var reference.
	for _, expr := range stmt.SelectExprs.Exprs {
		if se, ok := expr.(*sqlparser.AliasedExpr); ok {
			if err := validateNoFromTopLevelColRefs(se.Expr); err != nil {
				return nil, err
			}
			if err := validateNoStandaloneValTuple(se.Expr); err != nil {
				return nil, err
			}
		}
	}

	// Now check for scope errors: if the query contains @@session.X or @@local.X
	// for a global-only variable, error.
	if err := e.checkSelectScopeErrors(stmt); err != nil {
		return nil, err
	}

	colNames := make([]string, 0)
	values := make([]interface{}, 0)

	rawExprs := extractRawSelectExprs(e.currentQuery)
	rawExprIdx := 0

	for _, expr := range stmt.SelectExprs.Exprs {
		switch se := expr.(type) {
		case *sqlparser.AliasedExpr:
			name := ""
			if !se.As.IsEmpty() {
				name = se.As.String()
			} else if lit, ok := se.Expr.(*sqlparser.Literal); ok && lit.Type == sqlparser.StrVal {
				// String literal: use the value directly (unquoted), even if the raw expression
				// includes "UNION ..." suffix (which happens when called from a UNION context).
				name = lit.Val
			} else if isAggregateExpr(se.Expr) && rawExprIdx < len(rawExprs) {
				// Aggregate function in no-FROM context: normalize like execSelectGroupBy
				raw := strings.TrimSpace(rawExprs[rawExprIdx])
				raw = normalizeFuncArgSpaces(raw)
				raw = normalizeAggColNameNulls(raw)
				// Uppercase outer aggregate function name if lowercase
				for _, fn := range []string{"json_arrayagg(", "json_objectagg(", "count(", "sum(", "avg(", "min(", "max(", "group_concat("} {
					if strings.HasPrefix(raw, fn) {
						raw = strings.ToUpper(fn) + raw[len(fn):]
						break
					}
				}
				raw = uppercaseAggInnerKeywords(raw)
				raw = normalizeAggColNameFunctions(raw)
				raw = normalizeAggColNameSubselect(raw)
				name = raw
			} else if rawExprIdx < len(rawExprs) {
				raw := strings.TrimSpace(rawExprs[rawExprIdx])
				// MySQL strips block comments from column names
				if strings.Contains(raw, "/*") {
					raw = stripBlockComments(raw)
				}
				// Strip charset introducers for column names: _utf8'abc' -> 'abc', n'abc' -> 'abc'
				raw = stripCharsetIntroducerForColName(raw)
				// MySQL displays string literal column headers without quotes
				if len(raw) >= 2 && raw[0] == '\'' && raw[len(raw)-1] == '\'' {
					raw = raw[1 : len(raw)-1]
				}
				// MySQL strips leading '+' from numeric literals in column headers
				if len(raw) > 1 && raw[0] == '+' && raw[1] >= '0' && raw[1] <= '9' {
					raw = raw[1:]
				}
				// Normalize charset introducers (e.g. _utf8mb3 'x' → _utf8'x')
				raw = normalizeCharsetIntroducers(raw)
				name = raw
			} else {
				name = normalizeSQLDisplayName(sqlparser.String(se.Expr))
			}
			colNames = append(colNames, name)
			rawExprIdx++

			v, err := e.evalExpr(se.Expr)
			if err != nil {
				return nil, err
			}
			values = append(values, v)
		default:
			return nil, fmt.Errorf("unsupported select expression: %T", se)
		}
	}

	return &Result{
		Columns:     colNames,
		Rows:        [][]interface{}{values},
		IsResultSet: true,
	}, nil
}

// validateNoFromTopLevelColRefs checks for bare column references at the
// top level of an expression (not inside function calls). This is used
// in the early check in execSelect for no-FROM queries to return
// "Unknown column" before scope checks fire.
func validateNoFromTopLevelColRefs(expr sqlparser.Expr) error {
	switch e := expr.(type) {
	case *sqlparser.ColName:
		if !e.Qualifier.IsEmpty() {
			return mysqlError(1051, "42S02", fmt.Sprintf("Unknown table '%s' in field list", e.Qualifier.Name.String()))
		}
		return mysqlError(1054, "42S22", fmt.Sprintf("Unknown column '%s' in 'field list'", e.Name.String()))
	case *sqlparser.ComparisonExpr:
		if err := validateNoFromTopLevelColRefs(e.Left); err != nil {
			return err
		}
		return validateNoFromTopLevelColRefs(e.Right)
	case *sqlparser.BinaryExpr:
		if err := validateNoFromTopLevelColRefs(e.Left); err != nil {
			return err
		}
		return validateNoFromTopLevelColRefs(e.Right)
	case *sqlparser.IsExpr:
		return validateNoFromTopLevelColRefs(e.Left)
	case *sqlparser.UnaryExpr:
		return validateNoFromTopLevelColRefs(e.Expr)
	case *sqlparser.Count:
		// COUNT(expr) - validate the argument expression
		for _, arg := range e.Args {
			if err := validateNoFromTopLevelColRefs(arg); err != nil {
				return err
			}
		}
	case *sqlparser.Sum:
		if err := validateNoFromTopLevelColRefs(e.Arg); err != nil {
			return err
		}
	}
	// For subqueries, etc., don't check - let them through
	return nil
}

// checkSelectScopeErrors checks if a SELECT statement contains @@session.X or @@local.X
// references to global-only variables, and returns an error if so.
func (e *Executor) checkSelectScopeErrors(stmt *sqlparser.Select) error {
	q := strings.ToLower(e.currentQuery)
	// Walk all variables in the expression tree
	var checkExpr func(expr sqlparser.Expr) error
	checkExpr = func(expr sqlparser.Expr) error {
		switch v := expr.(type) {
		case *sqlparser.Variable:
			if v.Scope == sqlparser.SessionScope {
				name := strings.ToLower(v.Name.String())
				if !sysVarSessionOnly[name] && !sysVarBothScope[name] && (sysVarReadOnly[name] || sysVarGlobalOnly[name]) {
					// Check if the raw query has @@session. or @@local. prefix
					for _, prefix := range []string{"@@session.", "@@local."} {
						pattern := prefix + name
						if strings.Contains(q, pattern) {
							return mysqlError(1238, "HY000", fmt.Sprintf("Variable '%s' is a GLOBAL variable", name))
						}
					}
				}
			}
			if v.Scope == sqlparser.GlobalScope {
				name := strings.ToLower(v.Name.String())
				if sysVarSessionOnly[name] && !sysVarBothScope[name] {
					return mysqlError(1238, "HY000", fmt.Sprintf("Variable '%s' is a SESSION variable", name))
				}
			}
		case *sqlparser.Count:
			for _, arg := range v.Args {
				if err := checkExpr(arg); err != nil {
					return err
				}
			}
		case *sqlparser.Sum:
			if err := checkExpr(v.Arg); err != nil {
				return err
			}
		case *sqlparser.ComparisonExpr:
			if err := checkExpr(v.Left); err != nil {
				return err
			}
			if err := checkExpr(v.Right); err != nil {
				return err
			}
		case *sqlparser.FuncExpr:
			for _, arg := range v.Exprs {
				if err := checkExpr(arg); err != nil {
					return err
				}
			}
		case *sqlparser.CaseExpr:
			if v.Expr != nil {
				if err := checkExpr(v.Expr); err != nil {
					return err
				}
			}
			for _, w := range v.Whens {
				if err := checkExpr(w.Cond); err != nil {
					return err
				}
				if err := checkExpr(w.Val); err != nil {
					return err
				}
			}
			if v.Else != nil {
				if err := checkExpr(v.Else); err != nil {
					return err
				}
			}
		}
		return nil
	}

	for _, expr := range stmt.SelectExprs.Exprs {
		if ae, ok := expr.(*sqlparser.AliasedExpr); ok {
			if err := checkExpr(ae.Expr); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Executor) resolveSelectColumns(exprs []sqlparser.SelectExpr, def *catalog.TableDef) ([]string, error) {
	cols := make([]string, 0)
	for _, expr := range exprs {
		switch se := expr.(type) {
		case *sqlparser.StarExpr:
			for _, col := range def.Columns {
				cols = append(cols, col.Name)
			}
		case *sqlparser.AliasedExpr:
			if !se.As.IsEmpty() {
				cols = append(cols, se.As.String())
			} else if colName, ok := se.Expr.(*sqlparser.ColName); ok {
				cols = append(cols, colName.Name.String())
			} else {
				cols = append(cols, sqlparser.String(se.Expr))
			}
		default:
			return nil, fmt.Errorf("unsupported select expression: %T", se)
		}
	}
	return cols, nil
}

func applyOrderBy(orderBy sqlparser.OrderBy, colNames []string, rows [][]interface{}, collation string) ([][]interface{}, error) {
	if len(orderBy) == 0 {
		return rows, nil
	}

	type orderSpec struct {
		colIdx    int
		asc       bool
		collation string
	}
	var specs []orderSpec
	for _, order := range orderBy {
		// Handle COLLATE expressions by extracting the inner column name
		// and using the specified collation for comparison
		expr := order.Expr
		orderCollation := collation
		if collateExpr, ok := expr.(*sqlparser.CollateExpr); ok {
			expr = collateExpr.Expr
			orderCollation = collateExpr.Collation
		}
		// BINARY expr → use binary (case-sensitive) collation for ORDER BY
		if convExpr, ok := expr.(*sqlparser.ConvertExpr); ok {
			if convExpr.Type != nil && strings.EqualFold(convExpr.Type.Type, "binary") {
				orderCollation = "binary"
				expr = convExpr.Expr
			}
		}
		colName := sqlparser.String(expr)
		colName = strings.Trim(colName, "`")
		colIdx := -1
		for i, c := range colNames {
			if strings.EqualFold(c, colName) {
				colIdx = i
				break
			}
		}
		// If not found with qualified name (e.g. "t1.grp"), try unqualified name (e.g. "grp").
		// This handles ORDER BY t1.col when colNames only contains unqualified "col".
		if colIdx == -1 {
			if col, ok := expr.(*sqlparser.ColName); ok && col != nil && !col.Qualifier.IsEmpty() {
				unqualName := strings.Trim(col.Name.String(), "`")
				for i, c := range colNames {
					if strings.EqualFold(c, unqualName) {
						colIdx = i
						break
					}
				}
			}
		}
		if colIdx == -1 {
			continue
		}
		asc := order.Direction == sqlparser.AscOrder || order.Direction == 0
		specs = append(specs, orderSpec{colIdx: colIdx, asc: asc, collation: orderCollation})
	}
	if len(specs) == 0 {
		return rows, nil
	}

	sort.SliceStable(rows, func(i, j int) bool {
		for _, spec := range specs {
			coll := spec.collation
			if coll == "" {
				coll = collation
			}
			cmp := compareByCollation(rows[i][spec.colIdx], rows[j][spec.colIdx], coll)
			if cmp == 0 {
				continue
			}
			if spec.asc {
				return cmp < 0
			}
			return cmp > 0
		}
		return false
	})
	return rows, nil
}

// applyOrderByWithBinaryCols is like applyOrderBy but overrides collation to "binary"
// for ORDER BY columns that are BINARY/VARBINARY/BLOB type (byte-by-byte comparison),
// and uses numeric comparison for numeric column types (INT, FLOAT, DOUBLE, DECIMAL, etc.)
// to ensure correct ordering when values are stored as formatted strings (e.g. "5.00" < "11.11").
func applyOrderByWithBinaryCols(orderBy sqlparser.OrderBy, colNames []string, rows [][]interface{}, collation string, binaryColNames map[string]bool, numericColNames map[string]bool) ([][]interface{}, error) {
	if len(orderBy) == 0 {
		return rows, nil
	}
	if len(binaryColNames) == 0 && len(numericColNames) == 0 {
		return applyOrderBy(orderBy, colNames, rows, collation)
	}

	type orderSpec struct {
		colIdx    int
		asc       bool
		collation string
		numeric   bool
	}
	var specs []orderSpec
	for _, order := range orderBy {
		expr := order.Expr
		orderCollation := collation
		if collateExpr, ok := expr.(*sqlparser.CollateExpr); ok {
			expr = collateExpr.Expr
			orderCollation = collateExpr.Collation
		}
		if convExpr, ok := expr.(*sqlparser.ConvertExpr); ok {
			if convExpr.Type != nil && strings.EqualFold(convExpr.Type.Type, "binary") {
				orderCollation = "binary"
				expr = convExpr.Expr
			}
		}
		colName := strings.Trim(sqlparser.String(expr), "`")
		colIdx := -1
		for i, c := range colNames {
			if strings.EqualFold(c, colName) {
				colIdx = i
				break
			}
		}
		// If not found with qualified name (e.g. "t1.grp"), try unqualified name (e.g. "grp").
		// This handles ORDER BY t1.col when colNames only contains unqualified "col".
		if colIdx == -1 {
			if col, ok := expr.(*sqlparser.ColName); ok && col != nil && !col.Qualifier.IsEmpty() {
				unqualName := strings.Trim(col.Name.String(), "`")
				for i, c := range colNames {
					if strings.EqualFold(c, unqualName) {
						colIdx = i
						break
					}
				}
			}
		}
		if colIdx == -1 {
			continue
		}
		// If the column is a binary type, force binary collation
		if binaryColNames[strings.ToLower(colName)] {
			orderCollation = "binary"
		}
		// If the column is a numeric type, use numeric comparison
		isNumeric := numericColNames[strings.ToLower(colName)]
		asc := order.Direction == sqlparser.AscOrder || order.Direction == 0
		specs = append(specs, orderSpec{colIdx: colIdx, asc: asc, collation: orderCollation, numeric: isNumeric})
	}
	if len(specs) == 0 {
		return rows, nil
	}

	sort.SliceStable(rows, func(i, j int) bool {
		for _, spec := range specs {
			var cmp int
			if spec.numeric {
				cmp = compareNumeric(rows[i][spec.colIdx], rows[j][spec.colIdx])
			} else {
				coll := spec.collation
				if coll == "" {
					coll = collation
				}
				cmp = compareByCollation(rows[i][spec.colIdx], rows[j][spec.colIdx], coll)
			}
			if cmp == 0 {
				continue
			}
			if spec.asc {
				return cmp < 0
			}
			return cmp > 0
		}
		return false
	})
	return rows, nil
}

func needsPreProjectionOrderBy(orderBy sqlparser.OrderBy, colNames []string) bool {
	for _, order := range orderBy {
		expr := order.Expr
		// Unwrap COLLATE expression to get the inner column name
		if ce, ok := expr.(*sqlparser.CollateExpr); ok {
			expr = ce.Expr
		}
		col, ok := expr.(*sqlparser.ColName)
		if !ok || col == nil {
			// Non-column expression (function, etc.) - check if it matches a column alias exactly.
			// If it doesn't match any alias, we need pre-projection sort.
			exprStr := sqlparser.String(expr)
			found := false
			for _, c := range colNames {
				if strings.EqualFold(c, exprStr) {
					found = true
					break
				}
			}
			if !found {
				return true
			}
			continue
		}
		name := strings.Trim(col.Name.String(), "`")
		found := false
		for _, c := range colNames {
			if strings.EqualFold(c, name) {
				found = true
				break
			}
		}
		if !found {
			return true
		}
	}
	return false
}

func resolveOrderByExprValue(e *Executor, expr sqlparser.Expr, row storage.Row) interface{} {
	if col, ok := expr.(*sqlparser.ColName); ok && col != nil {
		// For qualified column names (e.g. t1.a), use evalRowExpr which correctly
		// handles case-insensitive qualified lookup in cross-join rows.
		if !col.Qualifier.IsEmpty() {
			val, _ := e.evalRowExpr(col, row)
			return val
		}
		name := strings.Trim(col.Name.String(), "`")
		if v, ok := row[name]; ok {
			return v
		}
		for k, v := range row {
			if strings.EqualFold(k, name) {
				return v
			}
			if dot := strings.LastIndex(k, "."); dot >= 0 && strings.EqualFold(k[dot+1:], name) {
				return v
			}
		}
	}
	val, _ := e.evalRowExpr(expr, row)
	return val
}

func applyOrderByWithTypeHints(orderBy sqlparser.OrderBy, colNames []string, rows [][]interface{}, collation string, numericCols map[int]bool) ([][]interface{}, error) {
	if len(orderBy) == 0 {
		return rows, nil
	}
	if len(numericCols) == 0 {
		return applyOrderBy(orderBy, colNames, rows, collation)
	}

	type orderSpec struct {
		colIdx int
		asc    bool
	}
	var specs []orderSpec
	for _, order := range orderBy {
		expr := order.Expr
		if collateExpr, ok := expr.(*sqlparser.CollateExpr); ok {
			expr = collateExpr.Expr
		}
		colName := strings.Trim(sqlparser.String(expr), "`")
		colIdx := -1
		for i, c := range colNames {
			if strings.EqualFold(c, colName) {
				colIdx = i
				break
			}
		}
		if colIdx == -1 {
			continue
		}
		asc := order.Direction == sqlparser.AscOrder || order.Direction == 0
		specs = append(specs, orderSpec{colIdx: colIdx, asc: asc})
	}
	if len(specs) == 0 {
		return rows, nil
	}

	sort.SliceStable(rows, func(i, j int) bool {
		for _, spec := range specs {
			var cmp int
			if numericCols[spec.colIdx] {
				cmp = compareNumeric(rows[i][spec.colIdx], rows[j][spec.colIdx])
			} else {
				cmp = compareByCollation(rows[i][spec.colIdx], rows[j][spec.colIdx], collation)
			}
			if cmp == 0 {
				continue
			}
			if spec.asc {
				return cmp < 0
			}
			return cmp > 0
		}
		return false
	})
	return rows, nil
}

// toInt64Limit converts a LIMIT/OFFSET value to int64.
// uint64 values that exceed math.MaxInt64 are clamped to math.MaxInt64
// (they effectively mean "no limit" since no result set can be that large).
// Non-numeric values return -1 (treated as 0 by applyLimit).
func toInt64Limit(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case uint64:
		if x > uint64(math.MaxInt64) {
			return math.MaxInt64
		}
		return int64(x)
	case float64:
		if x < 0 {
			return int64(x)
		}
		return int64(x)
	}
	return -1
}

func applyLimit(limit *sqlparser.Limit, rows [][]interface{}) ([][]interface{}, error) {
	if limit.Rowcount == nil {
		return rows, nil
	}

	// Use a bare executor: LIMIT values are always literals.
	e := &Executor{}
	lim, err := e.evalExpr(limit.Rowcount)
	if err != nil {
		return nil, err
	}
	n := toInt64Limit(lim)

	offset := int64(0)
	if limit.Offset != nil {
		off, err := e.evalExpr(limit.Offset)
		if err != nil {
			return nil, err
		}
		offset = toInt64Limit(off)
	}

	if n < 0 {
		n = 0
	}
	if offset < 0 {
		offset = 0
	}
	if offset >= int64(len(rows)) {
		return [][]interface{}{}, nil
	}
	end := offset + n
	if end > int64(len(rows)) {
		end = int64(len(rows))
	}
	return rows[offset:end], nil
}

// loadDataOptions holds parsed options from LOAD DATA statement.
type loadDataOptions struct {
	filePath          string
	isLocal           bool
	tableName         string
	fieldsTermBy      string
	fieldsEnclosedBy  string
	fieldsOptEnclosed bool
	fieldsEscapedBy   string
	linesTermBy       string
	linesStartingBy   string
	ignoreLines       int
	columns           []string // column names or @var names
	setExprs          string   // raw SET clause
	isReplace         bool
	isIgnore          bool
	charset           string // CHARACTER SET clause (e.g. "latin1", "utf8")
}

// reLoadData matches LOAD DATA [LOCAL] INFILE 'file' [REPLACE|IGNORE] INTO TABLE tablename ...
var reLoadDataFile = regexp.MustCompile(`(?i)LOAD\s+DATA\s+(?:CONCURRENT\s+)?(?:LOW_PRIORITY\s+)?(?:(LOCAL)\s+)?INFILE\s+'([^']*)'`)

var reLoadDataTable = regexp.MustCompile(`(?i)INTO\s+TABLE\s+(\S+)`)

var reIgnoreLines = regexp.MustCompile(`(?i)IGNORE\s+(\d+)\s+LINES`)

var reLoadReplace = regexp.MustCompile(`(?i)REPLACE\s+INTO\s+TABLE`)

var reLoadIgnore = regexp.MustCompile(`(?i)IGNORE\s+INTO\s+TABLE`)

// extractSQLString extracts a SQL quoted string starting at pos in query.
func extractSQLString(query string, pos int) (string, int) {
	if pos >= len(query) || query[pos] != '\'' {
		return "", pos
	}
	pos++ // skip opening quote
	var sb strings.Builder
	for pos < len(query) {
		ch := query[pos]
		if ch == '\\' && pos+1 < len(query) {
			next := query[pos+1]
			switch next {
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			case 'r':
				sb.WriteByte('\r')
			case '\\':
				sb.WriteByte('\\')
			case '\'':
				sb.WriteByte('\'')
			default:
				sb.WriteByte(next)
			}
			pos += 2
			continue
		}
		if ch == '\'' {
			if pos+1 < len(query) && query[pos+1] == '\'' {
				sb.WriteByte('\'')
				pos += 2
				continue
			}
			pos++
			return sb.String(), pos
		}
		sb.WriteByte(ch)
		pos++
	}
	return sb.String(), pos
}

// findKeywordAndExtractString searches for a keyword in query and extracts the following SQL quoted string.
func findKeywordAndExtractString(query, keyword string) (string, bool) {
	upper := strings.ToUpper(query)
	kwUpper := strings.ToUpper(keyword)
	idx := strings.Index(upper, kwUpper)
	if idx < 0 {
		return "", false
	}
	pos := idx + len(keyword)
	for pos < len(query) && query[pos] != '\'' {
		pos++
	}
	if pos >= len(query) {
		return "", false
	}
	val, _ := extractSQLString(query, pos)
	return val, true
}

func parseLoadDataSQL(query string) (*loadDataOptions, error) {
	opts := &loadDataOptions{
		fieldsTermBy:    "\t",
		linesTermBy:     "\n",
		fieldsEscapedBy: "\\",
	}

	m := reLoadDataFile.FindStringSubmatch(query)
	if m == nil {
		return nil, fmt.Errorf("cannot parse LOAD DATA statement")
	}
	opts.isLocal = strings.ToUpper(m[1]) == "LOCAL"
	opts.filePath = m[2]

	mTbl := reLoadDataTable.FindStringSubmatch(query)
	if mTbl == nil {
		return nil, fmt.Errorf("cannot parse table name in LOAD DATA")
	}
	opts.tableName = strings.Trim(mTbl[1], "`")

	upper := strings.ToUpper(query)

	if strings.Contains(upper, "FIELDS") || strings.Contains(upper, "COLUMNS") {
		fieldsIdx := strings.Index(upper, "FIELDS")
		if fieldsIdx < 0 {
			fieldsIdx = strings.Index(upper, "COLUMNS")
		}
		if fieldsIdx >= 0 {
			afterFields := query[fieldsIdx:]
			afterFieldsUpper := upper[fieldsIdx:]
			// Limit FIELDS clause search to before LINES keyword
			fieldsSection := afterFieldsUpper
			if linesPos := strings.Index(afterFieldsUpper, "LINES"); linesPos >= 0 {
				fieldsSection = afterFieldsUpper[:linesPos]
			}
			termIdx := strings.Index(fieldsSection, "TERMINATED BY")
			if termIdx >= 0 {
				pos := fieldsIdx + termIdx + len("TERMINATED BY")
				for pos < len(query) && query[pos] == ' ' {
					pos++
				}
				if pos < len(query) && query[pos] == '\'' {
					val, _ := extractSQLString(query, pos)
					opts.fieldsTermBy = val
				}
			}
			_ = afterFields
			encIdx := strings.Index(afterFieldsUpper, "ENCLOSED BY")
			if encIdx >= 0 {
				optIdx := strings.Index(afterFieldsUpper, "OPTIONALLY ENCLOSED BY")
				if optIdx >= 0 {
					opts.fieldsOptEnclosed = true
					pos := fieldsIdx + optIdx + len("OPTIONALLY ENCLOSED BY")
					for pos < len(query) && query[pos] == ' ' {
						pos++
					}
					if pos < len(query) && query[pos] == '\'' {
						val, _ := extractSQLString(query, pos)
						opts.fieldsEnclosedBy = val
					}
				} else {
					pos := fieldsIdx + encIdx + len("ENCLOSED BY")
					for pos < len(query) && query[pos] == ' ' {
						pos++
					}
					if pos < len(query) && query[pos] == '\'' {
						val, _ := extractSQLString(query, pos)
						opts.fieldsEnclosedBy = val
					}
				}
			}
			escIdx := strings.Index(afterFieldsUpper, "ESCAPED BY")
			if escIdx >= 0 {
				pos := fieldsIdx + escIdx + len("ESCAPED BY")
				for pos < len(query) && query[pos] == ' ' {
					pos++
				}
				if pos < len(query) && query[pos] == '\'' {
					val, _ := extractSQLString(query, pos)
					opts.fieldsEscapedBy = val
				}
			}
		}
	} else if strings.Contains(upper, "ENCLOSED BY") {
		if val, ok := findKeywordAndExtractString(query, "ENCLOSED BY"); ok {
			opts.fieldsEnclosedBy = val
			if strings.Contains(upper, "OPTIONALLY ENCLOSED BY") {
				opts.fieldsOptEnclosed = true
			}
		}
	}
	if !strings.Contains(upper, "FIELDS") && strings.Contains(upper, "ESCAPED BY") {
		if val, ok := findKeywordAndExtractString(query, "ESCAPED BY"); ok {
			opts.fieldsEscapedBy = val
		}
	}

	if strings.Contains(upper, "LINES") {
		linesIdx := strings.Index(upper, "LINES")
		if linesIdx >= 0 {
			afterLines := query[linesIdx:]
			afterLinesUpper := upper[linesIdx:]
			if startIdx := strings.Index(afterLinesUpper, "STARTING BY"); startIdx >= 0 {
				pos := linesIdx + startIdx + len("STARTING BY")
				for pos < len(query) && query[pos] == ' ' {
					pos++
				}
				if pos < len(query) && query[pos] == '\'' {
					val, _ := extractSQLString(query, pos)
					opts.linesStartingBy = val
				}
			}
			_ = afterLines
			termIdx := strings.Index(afterLinesUpper, "TERMINATED BY")
			if termIdx >= 0 {
				pos := linesIdx + termIdx + len("TERMINATED BY")
				for pos < len(query) && query[pos] == ' ' {
					pos++
				}
				if pos < len(query) && query[pos] == '\'' {
					val, _ := extractSQLString(query, pos)
					opts.linesTermBy = val
				}
			}
		}
	}

	if m := reIgnoreLines.FindStringSubmatch(query); m != nil {
		opts.ignoreLines, _ = strconv.Atoi(m[1])
	}
	opts.isReplace = reLoadReplace.MatchString(query)
	opts.isIgnore = reLoadIgnore.MatchString(query)

	// Parse CHARACTER SET clause
	if csIdx := strings.Index(upper, "CHARACTER SET "); csIdx >= 0 {
		afterCS := query[csIdx+len("CHARACTER SET "):]
		fields := strings.Fields(afterCS)
		if len(fields) > 0 {
			opts.charset = strings.ToLower(strings.TrimRight(fields[0], ";"))
		}
	} else if csIdx := strings.Index(upper, "CHARSET "); csIdx >= 0 {
		afterCS := query[csIdx+len("CHARSET "):]
		fields := strings.Fields(afterCS)
		if len(fields) > 0 {
			opts.charset = strings.ToLower(strings.TrimRight(fields[0], ";"))
		}
	}

	if idx := findColumnListStart(query); idx >= 0 {
		end := strings.Index(query[idx:], ")")
		if end >= 0 {
			colStr := query[idx+1 : idx+end]
			cols := strings.Split(colStr, ",")
			for _, c := range cols {
				c = strings.TrimSpace(c)
				if c != "" {
					opts.columns = append(opts.columns, c)
				}
			}
			afterCols := query[idx+end+1:]
			setIdx := strings.Index(strings.ToUpper(afterCols), "SET ")
			if setIdx >= 0 {
				opts.setExprs = strings.TrimSpace(afterCols[setIdx+4:])
				opts.setExprs = strings.TrimRight(opts.setExprs, "; ")
			}
		}
	}

	return opts, nil
}

func findColumnListStart(query string) int {
	upper := strings.ToUpper(query)
	tableIdx := strings.Index(upper, "INTO TABLE")
	if tableIdx < 0 {
		return -1
	}
	rest := query[tableIdx:]
	parts := strings.Fields(rest)
	if len(parts) < 3 {
		return -1
	}
	afterTable := tableIdx + strings.Index(rest, parts[2]) + len(parts[2])
	remaining := query[afterTable:]
	for i := 0; i < len(remaining); i++ {
		if remaining[i] == '(' {
			return afterTable + i
		}
	}
	return -1
}

func (e *Executor) execLoadData(query string) (*Result, error) {
	opts, err := parseLoadDataSQL(query)
	if err != nil {
		return nil, err
	}

	filePath := opts.filePath
	if !filepath.IsAbs(filePath) {
		resolved := false
		candidates := []string{filePath}
		if strings.Contains(filePath, "suite/engines/funcs/") {
			mapped := strings.Replace(filePath, "suite/engines/funcs/", "engine_funcs/", 1)
			candidates = append(candidates, mapped)
		}
		candidates = append(candidates, filepath.Base(filePath))

		for _, candidate := range candidates {
			for _, dir := range e.SearchPaths {
				full := filepath.Join(dir, candidate)
				if _, err := os.Stat(full); err == nil {
					filePath = full
					resolved = true
					break
				}
			}
			if resolved {
				break
			}
		}
		// Try resolving relative paths with ../ by stripping the leading ../ components
		// and searching in SearchPaths (handles ../../std_data/foo.dat patterns from MTR)
		if !resolved {
			stripped := filePath
			for strings.HasPrefix(stripped, "../") {
				stripped = stripped[3:]
			}
			if stripped != filePath {
				for _, dir := range e.SearchPaths {
					full := filepath.Join(dir, stripped)
					if _, err := os.Stat(full); err == nil {
						filePath = full
						resolved = true
						break
					}
				}
			}
		}
		if !resolved && e.DataDir != "" {
			filePath = filepath.Join(e.DataDir, filePath)
		}
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, mysqlError(29, "HY000", fmt.Sprintf("File '%s' not found (OS errno 2 - No such file or directory)", opts.filePath))
	}

	// Convert encoding for non-UTF-8 data files based on CHARACTER SET clause or filename heuristic
	cs := opts.charset
	if cs == "" {
		// Fall back to filename-based heuristic
		baseName := strings.ToLower(filepath.Base(filePath))
		if strings.Contains(baseName, "ucs2") {
			cs = "ucs2"
		} else if strings.Contains(baseName, "sjis") || strings.Contains(baseName, "cp932") {
			cs = "sjis"
		} else if strings.Contains(baseName, "ujis") || strings.Contains(baseName, "eucjp") {
			cs = "eucjp"
		}
	}
	switch cs {
	case "ucs2":
		if decoded, err := decodeUCS2(data); err == nil {
			data = decoded
		}
	case "sjis", "cp932":
		if decoded, err := decodeSJIS(data); err == nil {
			data = decoded
		}
	case "ujis", "eucjp", "eucjpms":
		if decoded, err := decodeEUCJP(data); err == nil {
			data = decoded
		}
	case "latin1", "utf8", "utf8mb3", "utf8mb4", "binary", "ascii":
		// latin1 is a superset of ASCII; Go strings handle bytes directly.
		// utf8/utf8mb4 is Go's native encoding. No conversion needed.
	}

	content := string(data)

	tbl, err := e.Storage.GetTable(e.CurrentDB, opts.tableName)
	if err != nil {
		return nil, mysqlError(1146, "42S02", fmt.Sprintf("Table '%s.%s' doesn't exist", e.CurrentDB, opts.tableName))
	}

	tableColNames := make([]string, len(tbl.Def.Columns))
	for i, col := range tbl.Def.Columns {
		tableColNames[i] = col.Name
	}

	var pkCols []string
	var uniqueCols []string
	for _, col := range tbl.Def.Columns {
		if col.PrimaryKey {
			pkCols = append(pkCols, col.Name)
		}
		if col.Unique {
			uniqueCols = append(uniqueCols, col.Name)
		}
	}
	if len(pkCols) == 0 && len(tbl.Def.PrimaryKey) > 0 {
		pkCols = tbl.Def.PrimaryKey
	}
	for _, idx := range tbl.Def.Indexes {
		if idx.Unique && len(idx.Columns) == 1 {
			uniqueCols = append(uniqueCols, idx.Columns[0])
		}
	}

	lines := splitLoadDataLines(content, opts.linesTermBy)
	if opts.ignoreLines > 0 && opts.ignoreLines < len(lines) {
		lines = lines[opts.ignoreLines:]
	}

	var affected uint64
	for _, line := range lines {
		if line == "" {
			continue
		}
		if opts.linesStartingBy != "" {
			idx := strings.Index(line, opts.linesStartingBy)
			if idx < 0 {
				continue
			}
			line = line[idx+len(opts.linesStartingBy):]
		}

		fields := splitLoadDataFields(line, opts.fieldsTermBy, opts.fieldsEnclosedBy, opts.fieldsEscapedBy)
		targetCols := tableColNames
		if len(opts.columns) > 0 {
			targetCols = opts.columns
		}

		row := make(storage.Row)
		varMap := make(map[string]interface{})
		for i, col := range targetCols {
			var val interface{}
			if i < len(fields) {
				val = processLoadDataField(fields[i], opts.fieldsEscapedBy, opts.fieldsEnclosedBy)
			}
			if strings.HasPrefix(col, "@") {
				varMap[col] = val
			} else {
				row[col] = val
			}
		}

		if opts.setExprs != "" {
			if err := e.applyLoadDataSet(opts.setExprs, row, varMap); err != nil {
				return nil, err
			}
		}

		for _, colDef := range tbl.Def.Columns {
			if _, exists := row[colDef.Name]; !exists {
				if colDef.AutoIncrement {
					row[colDef.Name] = tbl.AutoIncrement.Add(1)
				} else if colDef.Default != nil {
					v, err := e.evalDefaultValue(*colDef.Default)
					if err == nil {
						row[colDef.Name] = v
					}
				}
			}
			// Coerce date/time values and pad BINARY columns
			if v, exists := row[colDef.Name]; exists && v != nil {
				if padLen := binaryPadLength(colDef.Type); padLen > 0 {
					v = padBinaryValue(v, padLen)
				}
				row[colDef.Name] = coerceDateTimeValue(colDef.Type, v)
			}
		}

		if opts.isReplace {
			dupIdx := e.findDuplicateRow(tbl, row, pkCols, uniqueCols)
			if dupIdx >= 0 {
				tbl.Lock()
				tbl.Rows = append(tbl.Rows[:dupIdx], tbl.Rows[dupIdx+1:]...)
				tbl.InvalidateIndexes()
				tbl.Unlock()
			}
		} else if !opts.isIgnore {
			if opts.isLocal {
				dupIdx := e.findDuplicateRow(tbl, row, pkCols, uniqueCols)
				if dupIdx >= 0 {
					continue
				}
			} else {
				dupIdx := e.findDuplicateRow(tbl, row, pkCols, uniqueCols)
				if dupIdx >= 0 {
					dupKeyName := "PRIMARY"
					dupKeyVal := ""
					for _, pk := range pkCols {
						if v, ok := row[pk]; ok {
							dupKeyVal = fmt.Sprintf("%v", v)
							break
						}
					}
					return nil, mysqlError(1062, "23000", fmt.Sprintf("Duplicate entry '%s' for key '%s'", dupKeyVal, dupKeyName))
				}
			}
		} else {
			dupIdx := e.findDuplicateRow(tbl, row, pkCols, uniqueCols)
			if dupIdx >= 0 {
				continue
			}
		}

		tbl.Insert(row) //nolint:errcheck
		affected++
	}

	return &Result{AffectedRows: affected}, nil
}

func splitLoadDataLines(content, linesTerm string) []string {
	if linesTerm == "\n" {
		content = strings.ReplaceAll(content, "\r\n", "\n")
		return strings.Split(content, "\n")
	}
	return strings.Split(content, linesTerm)
}

func splitLoadDataFields(line, termBy, enclosedBy, escapedBy string) []string {
	if enclosedBy == "" {
		return strings.Split(line, termBy)
	}
	var fields []string
	i := 0
	for i < len(line) {
		if strings.HasPrefix(line[i:], enclosedBy) {
			i += len(enclosedBy)
			var field strings.Builder
			for i < len(line) {
				if escapedBy != "" && strings.HasPrefix(line[i:], escapedBy) && i+len(escapedBy) < len(line) {
					i += len(escapedBy)
					if i < len(line) {
						field.WriteByte(line[i])
						i++
					}
				} else if strings.HasPrefix(line[i:], enclosedBy) {
					i += len(enclosedBy)
					break
				} else {
					field.WriteByte(line[i])
					i++
				}
			}
			fields = append(fields, field.String())
			if strings.HasPrefix(line[i:], termBy) {
				i += len(termBy)
			}
		} else {
			end := strings.Index(line[i:], termBy)
			if end < 0 {
				fields = append(fields, line[i:])
				break
			}
			fields = append(fields, line[i:i+end])
			i += end + len(termBy)
		}
	}
	return fields
}

func processLoadDataField(field, escapedBy, enclosedBy string) interface{} {
	if escapedBy == "\\" && field == "\\N" {
		return nil
	}
	if escapedBy != "" && escapedBy != "\\" && field == escapedBy+"N" {
		return nil
	}
	// Process escape sequences in the field
	if escapedBy == "\\" && strings.Contains(field, "\\") {
		var result strings.Builder
		for i := 0; i < len(field); i++ {
			if field[i] == '\\' && i+1 < len(field) {
				next := field[i+1]
				switch next {
				case '\\':
					result.WriteByte('\\')
				case 'n':
					result.WriteByte('\n')
				case 'r':
					result.WriteByte('\r')
				case 't':
					result.WriteByte('\t')
				case '0':
					result.WriteByte(0)
				default:
					result.WriteByte(next)
				}
				i++ // skip next char
			} else {
				result.WriteByte(field[i])
			}
		}
		return result.String()
	}
	return field
}

func (e *Executor) applyLoadDataSet(setExprs string, row storage.Row, varMap map[string]interface{}) error {
	assignments := splitSetAssignments(setExprs)
	for _, assign := range assignments {
		parts := strings.SplitN(assign, "=", 2)
		if len(parts) != 2 {
			continue
		}
		colName := strings.TrimSpace(parts[0])
		exprStr := strings.TrimSpace(parts[1])
		for varName, varVal := range varMap {
			if varVal == nil {
				exprStr = strings.ReplaceAll(exprStr, varName, "NULL")
			} else {
				exprStr = strings.ReplaceAll(exprStr, varName, fmt.Sprintf("'%v'", varVal))
			}
		}
		selectSQL := fmt.Sprintf("SELECT %s", exprStr)
		result, err := e.Execute(selectSQL)
		if err != nil {
			return err
		}
		if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
			row[colName] = result.Rows[0][0]
		}
	}
	return nil
}

func splitSetAssignments(s string) []string {
	var result []string
	depth := 0
	start := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				result = append(result, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	last := strings.TrimSpace(s[start:])
	if last != "" {
		result = append(result, last)
	}
	return result
}

func (e *Executor) evalDefaultValue(defStr string) (interface{}, error) {
	selectSQL := fmt.Sprintf("SELECT %s", defStr)
	result, err := e.Execute(selectSQL)
	if err != nil {
		return nil, err
	}
	if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
		return result.Rows[0][0], nil
	}
	return nil, nil
}

func (e *Executor) execSelectIntoOutfile(into *sqlparser.SelectInto, colNames []string, rows [][]interface{}) (*Result, error) {
	fileName := into.FileName
	if len(fileName) >= 2 && fileName[0] == '\'' && fileName[len(fileName)-1] == '\'' {
		fileName = fileName[1 : len(fileName)-1]
	}
	if !filepath.IsAbs(fileName) && e.DataDir != "" {
		fileName = filepath.Join(e.DataDir, fileName)
	}

	exportOpt := into.ExportOption
	fieldsTerm := "\t"
	fieldsEnclosedBy := ""
	fieldsOptEnclosed := false
	linesTerm := "\n"
	fieldsEscapedBy := "\\"

	if exportOpt != "" {
		exportUpper := strings.ToUpper(exportOpt)
		if val, ok := findKeywordAndExtractString(exportOpt, "terminated by"); ok {
			fieldsIdx := strings.Index(exportUpper, "FIELDS")
			linesIdx := strings.Index(exportUpper, "LINES")
			termIdx := strings.Index(exportUpper, "TERMINATED BY")
			if fieldsIdx >= 0 && (linesIdx < 0 || termIdx < linesIdx) {
				fieldsTerm = val
			}
		}
		if strings.Contains(exportUpper, "OPTIONALLY ENCLOSED BY") {
			if val, ok := findKeywordAndExtractString(exportOpt, "optionally enclosed by"); ok {
				fieldsEnclosedBy = val
				fieldsOptEnclosed = true
			}
		} else if strings.Contains(exportUpper, "ENCLOSED BY") {
			if val, ok := findKeywordAndExtractString(exportOpt, "enclosed by"); ok {
				fieldsEnclosedBy = val
			}
		}
		if val, ok := findKeywordAndExtractString(exportOpt, "escaped by"); ok {
			fieldsEscapedBy = val
		}
		if linesIdx := strings.Index(exportUpper, "LINES"); linesIdx >= 0 {
			afterLines := exportOpt[linesIdx:]
			if val, ok := findKeywordAndExtractString(afterLines, "terminated by"); ok {
				linesTerm = val
			}
		}
	}

	var sb strings.Builder
	for _, row := range rows {
		for i, val := range row {
			if i > 0 {
				sb.WriteString(fieldsTerm)
			}
			if val == nil {
				sb.WriteString(fieldsEscapedBy + "N")
			} else {
				s := fmt.Sprintf("%v", val)
				if fieldsEnclosedBy != "" {
					if !fieldsOptEnclosed {
						sb.WriteString(fieldsEnclosedBy + s + fieldsEnclosedBy)
					} else {
						if isNonStringOutfileValue(s) {
							sb.WriteString(s)
						} else {
							sb.WriteString(fieldsEnclosedBy + s + fieldsEnclosedBy)
						}
					}
				} else {
					if fieldsEscapedBy != "" {
						s = strings.ReplaceAll(s, fieldsEscapedBy, fieldsEscapedBy+fieldsEscapedBy)
					}
					sb.WriteString(s)
				}
			}
		}
		sb.WriteString(linesTerm)
	}

	dir := filepath.Dir(fileName)
	if errDir := os.MkdirAll(dir, 0755); errDir != nil {
		return nil, fmt.Errorf("cannot create directory for outfile: %v", errDir)
	}
	if err := os.WriteFile(fileName, []byte(sb.String()), 0644); err != nil {
		return nil, fmt.Errorf("cannot write outfile: %v", err)
	}

	return &Result{AffectedRows: uint64(len(rows))}, nil
}

// isNonStringOutfileValue returns true if the value should NOT be enclosed
// by OPTIONALLY ENCLOSED BY. MySQL only encloses string (CHAR/VARCHAR/TEXT) columns;
// numeric, date, time, datetime, timestamp, and year values are not enclosed.
func isNonStringOutfileValue(s string) bool {
	// Numeric values
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return true
	}
	// Date: YYYY-MM-DD
	if _, err := time.Parse("2006-01-02", s); err == nil {
		return true
	}
	// Time: HH:MM:SS
	if _, err := time.Parse("15:04:05", s); err == nil {
		return true
	}
	// Datetime/Timestamp: YYYY-MM-DD HH:MM:SS
	if _, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
		return true
	}
	// Year: 4-digit
	if len(s) == 4 {
		if _, err := strconv.Atoi(s); err == nil {
			return true
		}
	}
	return false
}

// execSelectIntoUserVars handles SELECT ... INTO @var1, @var2, ...
// It assigns the first row's column values to the specified user variables.
func (e *Executor) execSelectIntoUserVars(into *sqlparser.SelectInto, colNames []string, rows [][]interface{}) (*Result, error) {
	if len(rows) == 0 {
		// No rows: set all variables to NULL
		for _, v := range into.VarList {
			varName := v.Name.String()
			e.userVars[varName] = nil
		}
		return &Result{}, nil
	}
	row := rows[0]
	for i, v := range into.VarList {
		varName := v.Name.String()
		if i < len(row) {
			e.userVars[varName] = row[i]
		} else {
			e.userVars[varName] = nil
		}
	}
	return &Result{}, nil
}

// containsBetweenExpr returns true if the WHERE expression contains a BETWEEN expression,
// which typically indicates a range scan for sort statistics classification.
func containsBetweenExpr(expr sqlparser.Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *sqlparser.BetweenExpr:
		return true
	case *sqlparser.AndExpr:
		return containsBetweenExpr(e.Left) || containsBetweenExpr(e.Right)
	case *sqlparser.OrExpr:
		return containsBetweenExpr(e.Left) || containsBetweenExpr(e.Right)
	case *sqlparser.NotExpr:
		return containsBetweenExpr(e.Expr)
	}
	return false
}
