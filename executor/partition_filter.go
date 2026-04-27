package executor

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
	"vitess.io/vitess/go/vt/sqlparser"
)

// partitionFilter builds a set of partition names (and subpartition names) that
// are included in the given PARTITION(...) clause.
// Returns (partSet, subpartToParent, error).
//   - partSet: the set of top-level partition names that are relevant
//   - subpartToParent: maps each named subpartition name to its parent partition name
func buildPartitionNameSets(td *catalog.TableDef, parts sqlparser.Partitions) (map[string]bool, map[string]string, error) {
	requested := make([]string, 0, len(parts))
	for _, p := range parts {
		requested = append(requested, p.String())
	}

	// Build a set of known partition names and subpartition name -> partition name map
	partNames := make(map[string]bool, len(td.PartitionDefs))
	subpartToParent := make(map[string]string)
	for _, pd := range td.PartitionDefs {
		partNames[strings.ToLower(pd.Name)] = true
		for _, sp := range pd.SubPartitionNames {
			subpartToParent[strings.ToLower(sp)] = strings.ToLower(pd.Name)
		}
	}

	// Resolve requested names to top-level partition names
	partSet := make(map[string]bool)
	for _, name := range requested {
		lower := strings.ToLower(name)
		if partNames[lower] {
			partSet[lower] = true
		} else if parent, ok := subpartToParent[lower]; ok {
			// Subpartition name: include its parent partition
			// We track both so we can resolve per-subpartition later
			partSet[parent] = true
		} else {
			return nil, nil, fmt.Errorf("ERROR 1735 (HY000): Unknown partition '%s' in table '%s'", name, td.Name)
		}
	}

	return partSet, subpartToParent, nil
}

// rowBelongsToPartitions checks whether a storage row belongs to any of the
// named partitions in the PARTITION() clause.
//
// partitions: raw sqlparser.Partitions from the AST
// td: table definition with partition metadata
// row: the storage row to evaluate
// evalFn: function to evaluate a SQL expression against the row
//
// For RANGE/LIST partitioning, we evaluate the partition expression and compare
// against partition value ranges.
// For HASH/KEY partitioning, we fall back to "all rows match" (conservative).
func (e *Executor) rowBelongsToPartitions(partitions sqlparser.Partitions, td *catalog.TableDef, row storage.Row) (bool, error) {
	if len(partitions) == 0 {
		return true, nil
	}
	if td == nil || td.PartitionType == "" || len(td.PartitionDefs) == 0 {
		// Not partitioned or no partition defs: allow all rows through
		return true, nil
	}

	partSet, subpartToParent, err := buildPartitionNameSets(td, partitions)
	if err != nil {
		return false, err
	}

	// Find which partition this row belongs to
	partName, err := e.resolveRowToPartition(td, row)
	if err != nil {
		// On evaluation error, conservatively include the row
		return true, nil
	}

	if partSet[strings.ToLower(partName)] {
		// The row's partition is included. Now check if the request is for specific subpartitions.
		// If all requested names are top-level partition names, just return true.
		// If any requested name is a subpartition, we need to also check that.
		return e.rowBelongsToSubpartitions(partitions, td, partName, row, subpartToParent)
	}
	return false, nil
}

// rowBelongsToSubpartitions checks subpartition membership when the PARTITION clause
// includes subpartition names. If no subpartition names are specified, returns true.
func (e *Executor) rowBelongsToSubpartitions(partitions sqlparser.Partitions, td *catalog.TableDef, partName string, row storage.Row, subpartToParent map[string]string) (bool, error) {
	// Check if any of the requested names is a subpartition name
	hasSubpartReqs := false
	reqSubparts := make(map[string]bool)
	reqTopLevel := make(map[string]bool)
	for _, p := range partitions {
		lower := strings.ToLower(p.String())
		if parent, ok := subpartToParent[lower]; ok {
			// This is a subpartition name request
			if strings.ToLower(parent) == strings.ToLower(partName) {
				hasSubpartReqs = true
				reqSubparts[lower] = true
			}
		} else {
			reqTopLevel[lower] = true
		}
	}

	if !hasSubpartReqs {
		// No subpartition specifics: all rows in the matched partition qualify
		return true, nil
	}

	// There are subpartition requests for this partition - check if top-level partition
	// is also directly requested
	if reqTopLevel[strings.ToLower(partName)] {
		return true, nil
	}

	// Need to evaluate subpartition membership
	if td.PartitionSubpartition == nil {
		return true, nil
	}

	// Find this partition's subpartitions
	var subpartNames []string
	for _, pd := range td.PartitionDefs {
		if strings.EqualFold(pd.Name, partName) {
			subpartNames = pd.SubPartitionNames
			break
		}
	}
	if len(subpartNames) == 0 {
		return true, nil
	}

	// Determine which subpartition the row belongs to
	subpartIdx, err := e.resolveRowToSubpartition(td, row, len(subpartNames))
	if err != nil {
		return true, nil
	}
	if subpartIdx < 0 || subpartIdx >= len(subpartNames) {
		return true, nil
	}

	subpartName := strings.ToLower(subpartNames[subpartIdx])
	return reqSubparts[subpartName], nil
}

// resolveRowToPartition determines which partition (by name) a row belongs to.
// Returns the partition name, or "" if it cannot be determined.
func (e *Executor) resolveRowToPartition(td *catalog.TableDef, row storage.Row) (string, error) {
	switch strings.ToUpper(td.PartitionType) {
	case "RANGE":
		return e.resolveRowToRangePartition(td, row)
	case "LIST":
		return e.resolveRowToListPartition(td, row)
	case "HASH", "KEY":
		// For HASH/KEY we'd need to compute the hash - fall back to matching all
		return "", nil
	default:
		return "", nil
	}
}

// resolveRowToRangePartition finds the RANGE partition for a row.
func (e *Executor) resolveRowToRangePartition(td *catalog.TableDef, row storage.Row) (string, error) {
	// Evaluate the partition expression
	exprVal, err := e.evalPartitionExpr(td, row)
	if err != nil {
		return "", err
	}

	intVal := toInt64(exprVal)

	// Walk partitions in order and find the first where intVal < LESS THAN bound
	for _, pd := range td.PartitionDefs {
		vr := strings.TrimSpace(pd.ValueRange)
		if strings.EqualFold(vr, "LESS THAN MAXVALUE") {
			return pd.Name, nil
		}
		if strings.HasPrefix(strings.ToUpper(vr), "LESS THAN (") {
			// Extract the bound value
			inner := vr[len("LESS THAN (") : len(vr)-1]
			inner = strings.TrimSpace(inner)
			bound, parseErr := strconv.ParseInt(inner, 10, 64)
			if parseErr != nil {
				// Try float
				if f, fErr := strconv.ParseFloat(inner, 64); fErr == nil {
					bound = int64(f)
				} else {
					// Could be an expression; try to evaluate it
					boundExpr, sqlErr := parseExprFromString(e, inner)
					if sqlErr == nil {
						boundVal, evalErr := e.evalRowExpr(boundExpr, row)
						if evalErr == nil {
							bound = toInt64(boundVal)
						}
					}
				}
			}
			if intVal < bound {
				return pd.Name, nil
			}
		}
	}
	return "", nil
}

// resolveRowToListPartition finds the LIST partition for a row.
func (e *Executor) resolveRowToListPartition(td *catalog.TableDef, row storage.Row) (string, error) {
	exprVal, err := e.evalPartitionExpr(td, row)
	if err != nil {
		return "", err
	}

	for _, pd := range td.PartitionDefs {
		vr := strings.TrimSpace(pd.ValueRange)
		if !strings.HasPrefix(strings.ToUpper(vr), "IN (") {
			continue
		}
		// Extract the list: IN (v1,v2,...)
		inner := vr[len("IN (") : len(vr)-1]
		values := splitListValues(inner)
		for _, v := range values {
			v = strings.TrimSpace(v)
			// Compare as int64 if possible, otherwise string
			if bound, err2 := strconv.ParseInt(v, 10, 64); err2 == nil {
				if toInt64(exprVal) == bound {
					return pd.Name, nil
				}
			} else {
				// String comparison (unquoted or quoted)
				strV := strings.Trim(v, "'\"")
				if fmt.Sprintf("%v", exprVal) == strV {
					return pd.Name, nil
				}
			}
		}
	}
	return "", nil
}

// splitListValues splits a comma-separated list of values, respecting nested parens.
func splitListValues(s string) []string {
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
	if start < len(s) {
		result = append(result, strings.TrimSpace(s[start:]))
	}
	return result
}

// evalPartitionExpr evaluates the table's partition expression for a given row.
func (e *Executor) evalPartitionExpr(td *catalog.TableDef, row storage.Row) (interface{}, error) {
	exprStr := td.PartitionExpression
	if exprStr == "" && len(td.PartitionExprCols) > 0 {
		// Column-list partitioning (RANGE COLUMNS / LIST COLUMNS / KEY)
		// For single column, use it directly
		exprStr = "`" + td.PartitionExprCols[0] + "`"
	}
	if exprStr == "" {
		return nil, fmt.Errorf("no partition expression")
	}

	expr, err := parseExprFromString(e, exprStr)
	if err != nil {
		return nil, fmt.Errorf("parse partition expression %q: %w", exprStr, err)
	}
	return e.evalRowExpr(expr, row)
}

// parseExprFromString parses an SQL expression string using the executor's parser.
func parseExprFromString(e *Executor, exprStr string) (sqlparser.Expr, error) {
	stmt, err := e.parser().Parse("SELECT " + exprStr)
	if err != nil {
		return nil, err
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok || len(sel.SelectExprs.Exprs) != 1 {
		return nil, fmt.Errorf("unexpected parse result for expression: %s", exprStr)
	}
	aliased, ok := sel.SelectExprs.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("unexpected select expr type for: %s", exprStr)
	}
	return aliased.Expr, nil
}

// resolveRowToSubpartition determines which subpartition index (0-based) a row
// belongs to within a partition, for SUBPARTITION BY HASH.
func (e *Executor) resolveRowToSubpartition(td *catalog.TableDef, row storage.Row, numSubparts int) (int, error) {
	if td.PartitionSubpartition == nil || numSubparts == 0 {
		return 0, nil
	}
	sp := td.PartitionSubpartition
	switch strings.ToUpper(sp.Type) {
	case "HASH":
		exprStr := sp.Expression
		if exprStr == "" {
			return 0, nil
		}
		expr, err := parseExprFromString(e, exprStr)
		if err != nil {
			return 0, nil
		}
		val, err := e.evalRowExpr(expr, row)
		if err != nil {
			return 0, nil
		}
		n := toInt64(val)
		if n < 0 {
			n = -n
		}
		return int(n % int64(numSubparts)), nil
	case "KEY":
		// For KEY subpartitioning on a single column
		if len(sp.Columns) == 0 {
			return 0, nil
		}
		colName := sp.Columns[0]
		val := row[colName]
		if val == nil {
			// Try case-insensitive lookup
			colLower := strings.ToLower(colName)
			for k, v := range row {
				if strings.ToLower(k) == colLower {
					val = v
					break
				}
			}
		}
		n := toInt64(val)
		if n < 0 {
			n = -n
		}
		return int(n % int64(numSubparts)), nil
	}
	return 0, nil
}

// filterRowsByPartitions filters a slice of rows to include only those belonging
// to the specified partitions.
func (e *Executor) filterRowsByPartitions(rows []storage.Row, partitions sqlparser.Partitions, td *catalog.TableDef) ([]storage.Row, error) {
	if len(partitions) == 0 || td == nil || td.PartitionType == "" {
		return rows, nil
	}

	// Validate partition names first (to return error if unknown partition specified)
	if _, _, err := buildPartitionNameSets(td, partitions); err != nil {
		return nil, err
	}

	result := make([]storage.Row, 0, len(rows))
	for _, row := range rows {
		match, err := e.rowBelongsToPartitions(partitions, td, row)
		if err != nil {
			return nil, err
		}
		if match {
			result = append(result, row)
		}
	}
	return result, nil
}
