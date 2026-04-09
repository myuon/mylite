package executor

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/myuon/mylite/storage"
	"vitess.io/vitess/go/vt/sqlparser"
)

// windowFuncInfo describes a window function found in a SELECT expression.
type windowFuncInfo struct {
	colIdx     int             // index in result columns
	expr       sqlparser.Expr  // the window function expression
	overClause *sqlparser.OverClause
}

// containsWindowFunc checks if an expression tree contains any window function.
func containsWindowFunc(expr sqlparser.Expr) bool {
	switch v := expr.(type) {
	case *sqlparser.ArgumentLessWindowExpr:
		return v.OverClause != nil
	case *sqlparser.LagLeadExpr:
		return v.OverClause != nil
	case *sqlparser.FirstOrLastValueExpr:
		return v.OverClause != nil
	case *sqlparser.NTHValueExpr:
		return v.OverClause != nil
	case *sqlparser.NtileExpr:
		return v.OverClause != nil
	case *sqlparser.CountStar:
		return v.OverClause != nil
	case *sqlparser.Count:
		return v.OverClause != nil
	case *sqlparser.Sum:
		return v.OverClause != nil
	case *sqlparser.Avg:
		return v.OverClause != nil
	case *sqlparser.Min:
		return v.OverClause != nil
	case *sqlparser.Max:
		return v.OverClause != nil
	case *sqlparser.Variance:
		return v.OverClause != nil
	case *sqlparser.VarPop:
		return v.OverClause != nil
	case *sqlparser.VarSamp:
		return v.OverClause != nil
	case *sqlparser.Std:
		return v.OverClause != nil
	case *sqlparser.StdDev:
		return v.OverClause != nil
	case *sqlparser.StdPop:
		return v.OverClause != nil
	case *sqlparser.StdSamp:
		return v.OverClause != nil
	case *sqlparser.BitAnd:
		return v.OverClause != nil
	case *sqlparser.BitOr:
		return v.OverClause != nil
	case *sqlparser.BitXor:
		return v.OverClause != nil
	case *sqlparser.BinaryExpr:
		return containsWindowFunc(v.Left) || containsWindowFunc(v.Right)
	case *sqlparser.UnaryExpr:
		return containsWindowFunc(v.Expr)
	case *sqlparser.CaseExpr:
		if v.Expr != nil && containsWindowFunc(v.Expr) {
			return true
		}
		for _, w := range v.Whens {
			if containsWindowFunc(w.Cond) || containsWindowFunc(w.Val) {
				return true
			}
		}
		if v.Else != nil && containsWindowFunc(v.Else) {
			return true
		}
	case *sqlparser.FuncExpr:
		for _, arg := range v.Exprs {
			if containsWindowFunc(arg) {
				return true
			}
		}
	}
	return false
}

// isWindowAggregateExpr returns true if expr is an aggregate function with an OVER clause
// (i.e., it's being used as a window function, not a GROUP BY aggregate).
func isWindowAggregateExpr(expr sqlparser.Expr) bool {
	switch v := expr.(type) {
	case *sqlparser.CountStar:
		return v.OverClause != nil
	case *sqlparser.Count:
		return v.OverClause != nil
	case *sqlparser.Sum:
		return v.OverClause != nil
	case *sqlparser.Avg:
		return v.OverClause != nil
	case *sqlparser.Min:
		return v.OverClause != nil
	case *sqlparser.Max:
		return v.OverClause != nil
	case *sqlparser.BitAnd:
		return v.OverClause != nil
	case *sqlparser.BitOr:
		return v.OverClause != nil
	case *sqlparser.BitXor:
		return v.OverClause != nil
	case *sqlparser.Variance:
		return v.OverClause != nil
	case *sqlparser.VarPop:
		return v.OverClause != nil
	case *sqlparser.VarSamp:
		return v.OverClause != nil
	case *sqlparser.Std:
		return v.OverClause != nil
	case *sqlparser.StdDev:
		return v.OverClause != nil
	case *sqlparser.StdPop:
		return v.OverClause != nil
	case *sqlparser.StdSamp:
		return v.OverClause != nil
	}
	return false
}

// selectExprsHaveWindowFuncs checks whether any SELECT expression contains window functions.
func selectExprsHaveWindowFuncs(exprs []sqlparser.SelectExpr) bool {
	for _, expr := range exprs {
		ae, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}
		if containsWindowFunc(ae.Expr) {
			return true
		}
	}
	return false
}

// getOverClause extracts the OverClause from a window function expression.
func getOverClause(expr sqlparser.Expr) *sqlparser.OverClause {
	switch v := expr.(type) {
	case *sqlparser.ArgumentLessWindowExpr:
		return v.OverClause
	case *sqlparser.LagLeadExpr:
		return v.OverClause
	case *sqlparser.FirstOrLastValueExpr:
		return v.OverClause
	case *sqlparser.NTHValueExpr:
		return v.OverClause
	case *sqlparser.NtileExpr:
		return v.OverClause
	case *sqlparser.CountStar:
		return v.OverClause
	case *sqlparser.Count:
		return v.OverClause
	case *sqlparser.Sum:
		return v.OverClause
	case *sqlparser.Avg:
		return v.OverClause
	case *sqlparser.Min:
		return v.OverClause
	case *sqlparser.Max:
		return v.OverClause
	case *sqlparser.Variance:
		return v.OverClause
	case *sqlparser.VarPop:
		return v.OverClause
	case *sqlparser.VarSamp:
		return v.OverClause
	case *sqlparser.Std:
		return v.OverClause
	case *sqlparser.StdDev:
		return v.OverClause
	case *sqlparser.StdPop:
		return v.OverClause
	case *sqlparser.StdSamp:
		return v.OverClause
	case *sqlparser.BitAnd:
		return v.OverClause
	case *sqlparser.BitOr:
		return v.OverClause
	case *sqlparser.BitXor:
		return v.OverClause
	}
	return nil
}

// findWindowFuncs finds all top-level window function expressions in SELECT expressions
// and returns their column indices and expressions.
func findWindowFuncs(colExprs []sqlparser.Expr) []windowFuncInfo {
	var result []windowFuncInfo
	for i, expr := range colExprs {
		oc := getOverClause(expr)
		if oc != nil {
			result = append(result, windowFuncInfo{
				colIdx:     i,
				expr:       expr,
				overClause: oc,
			})
		}
	}
	return result
}

// partitionKey computes a partition key string for a row given PARTITION BY expressions.
func (e *Executor) partitionKey(partitionExprs []sqlparser.Expr, row storage.Row) string {
	if len(partitionExprs) == 0 {
		return ""
	}
	parts := make([]string, len(partitionExprs))
	for i, pe := range partitionExprs {
		val, _ := e.evalRowExpr(pe, row)
		parts[i] = fmt.Sprintf("%v", val)
	}
	return strings.Join(parts, "\x00")
}

// orderByValuesForRow evaluates ORDER BY expressions for a row.
func (e *Executor) orderByValuesForRow(orderBy sqlparser.OrderBy, row storage.Row) []interface{} {
	vals := make([]interface{}, len(orderBy))
	for i, o := range orderBy {
		vals[i], _ = e.evalRowExpr(o.Expr, row)
	}
	return vals
}

// windowOrderByEqual checks if two rows have the same ORDER BY values.
func windowOrderByEqual(a, b []interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if windowCompareValues(a[i], b[i]) != 0 {
			return false
		}
	}
	return true
}

// sortRowIndices sorts row indices according to ORDER BY clause.
func (e *Executor) sortRowIndices(indices []int, allRows []storage.Row, orderBy sqlparser.OrderBy) {
	if len(orderBy) == 0 {
		return
	}
	sort.SliceStable(indices, func(a, b int) bool {
		rowA := allRows[indices[a]]
		rowB := allRows[indices[b]]
		for _, o := range orderBy {
			va, _ := e.evalRowExpr(o.Expr, rowA)
			vb, _ := e.evalRowExpr(o.Expr, rowB)
			cmp := windowCompareValues(va, vb)
			if cmp == 0 {
				continue
			}
			asc := o.Direction == sqlparser.AscOrder || o.Direction == 0
			if asc {
				return cmp < 0
			}
			return cmp > 0
		}
		return false
	})
}

// computeFrameBounds computes the start and end indices within a partition for the given row.
// Returns (start, end) as inclusive indices into the partition's ordered row list.
func (e *Executor) computeFrameBounds(frame *sqlparser.FrameClause, orderBy sqlparser.OrderBy,
	partitionRows []storage.Row, currentIdx int, orderByVals [][]interface{}) (int, int) {

	n := len(partitionRows)

	if frame == nil {
		// Default frame depends on whether there's an ORDER BY:
		// With ORDER BY: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		// Without ORDER BY: RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
		if len(orderBy) == 0 {
			return 0, n - 1
		}
		// RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		// For RANGE mode, CURRENT ROW means all rows with same ORDER BY value
		end := currentIdx
		for end+1 < n && windowOrderByEqual(orderByVals[currentIdx], orderByVals[end+1]) {
			end++
		}
		return 0, end
	}

	if frame.Unit == sqlparser.FrameRowsType {
		// ROWS mode
		start := computeFramePointRows(frame.Start, currentIdx, n, e)
		end := currentIdx // default end is current row
		if frame.End != nil {
			end = computeFramePointRows(frame.End, currentIdx, n, e)
		}
		if start < 0 {
			start = 0
		}
		if end >= n {
			end = n - 1
		}
		return start, end
	}

	// RANGE mode
	start := computeFramePointRange(frame.Start, currentIdx, n, orderByVals)
	end := currentIdx
	if frame.End != nil {
		end = computeFramePointRange(frame.End, currentIdx, n, orderByVals)
	} else {
		// Default end for RANGE start only: CURRENT ROW (peer group end)
		for end+1 < n && windowOrderByEqual(orderByVals[currentIdx], orderByVals[end+1]) {
			end++
		}
	}
	if start < 0 {
		start = 0
	}
	if end >= n {
		end = n - 1
	}
	return start, end
}

func computeFramePointRows(fp *sqlparser.FramePoint, currentIdx, n int, e *Executor) int {
	switch fp.Type {
	case sqlparser.CurrentRowType:
		return currentIdx
	case sqlparser.UnboundedPrecedingType:
		return 0
	case sqlparser.UnboundedFollowingType:
		return n - 1
	case sqlparser.ExprPrecedingType:
		offset := evalFrameOffset(fp.Expr, e)
		return currentIdx - offset
	case sqlparser.ExprFollowingType:
		offset := evalFrameOffset(fp.Expr, e)
		return currentIdx + offset
	}
	return currentIdx
}

func computeFramePointRange(fp *sqlparser.FramePoint, currentIdx, n int, orderByVals [][]interface{}) int {
	switch fp.Type {
	case sqlparser.CurrentRowType:
		return currentIdx
	case sqlparser.UnboundedPrecedingType:
		return 0
	case sqlparser.UnboundedFollowingType:
		return n - 1
	case sqlparser.ExprPrecedingType:
		// For RANGE PRECEDING, find first row where ORDER BY value >= current - offset
		return 0
	case sqlparser.ExprFollowingType:
		return n - 1
	}
	return currentIdx
}

func evalFrameOffset(expr sqlparser.Expr, e *Executor) int {
	val, err := e.evalExpr(expr)
	if err != nil || val == nil {
		return 0
	}
	return int(toInt64(val))
}

// resolveNamedWindows resolves named window references (OVER w) by looking up the named window
// definitions from the WINDOW clause and replacing the OverClause.WindowName with the full spec.
// This handles both single and multiple named windows.
func resolveNamedWindows(winFuncs []windowFuncInfo, namedWindows sqlparser.NamedWindows) []windowFuncInfo {
	if len(namedWindows) == 0 {
		return winFuncs
	}
	// Build map of name -> WindowSpecification
	windowMap := make(map[string]*sqlparser.WindowSpecification)
	for _, nw := range namedWindows {
		for _, wdef := range nw.Windows {
			windowMap[wdef.Name.Lowered()] = wdef.WindowSpec
		}
	}
	if len(windowMap) == 0 {
		return winFuncs
	}
	resolved := make([]windowFuncInfo, len(winFuncs))
	for i, wf := range winFuncs {
		resolved[i] = wf
		if wf.overClause != nil && !wf.overClause.WindowName.IsEmpty() && wf.overClause.WindowSpec == nil {
			name := wf.overClause.WindowName.Lowered()
			if spec, ok := windowMap[name]; ok {
				// Create a new OverClause with the resolved WindowSpec
				resolved[i].overClause = &sqlparser.OverClause{
					WindowSpec: spec,
				}
			}
		}
	}
	return resolved
}

// processWindowFunctions computes window function values for all rows.
// It modifies resultRows in-place, replacing stub values with correct window function results.
func (e *Executor) processWindowFunctions(
	colExprs []sqlparser.Expr,
	allRows []storage.Row,
	resultRows [][]interface{},
) error {
	return e.processWindowFunctionsWithNamedWindows(colExprs, allRows, resultRows, nil)
}

// processWindowFunctionsWithNamedWindows computes window function values with named window support.
func (e *Executor) processWindowFunctionsWithNamedWindows(
	colExprs []sqlparser.Expr,
	allRows []storage.Row,
	resultRows [][]interface{},
	namedWindows sqlparser.NamedWindows,
) error {
	winFuncs := findWindowFuncs(colExprs)
	if len(winFuncs) == 0 {
		return nil
	}

	// Resolve named window references (OVER w) to their full window specifications
	winFuncs = resolveNamedWindows(winFuncs, namedWindows)

	for _, wf := range winFuncs {
		if err := e.computeWindowFunc(wf, allRows, resultRows); err != nil {
			return err
		}
	}
	return nil
}

// computeWindowFunc computes a single window function's values for all rows.
func (e *Executor) computeWindowFunc(wf windowFuncInfo, allRows []storage.Row, resultRows [][]interface{}) error {
	oc := wf.overClause
	var ws *sqlparser.WindowSpecification
	if oc != nil {
		ws = oc.WindowSpec
	}
	if ws == nil {
		ws = &sqlparser.WindowSpecification{}
	}

	// Partition rows by PARTITION BY
	partitions := e.buildPartitions(allRows, ws.PartitionClause)

	for _, part := range partitions {
		// Sort partition by ORDER BY
		e.sortRowIndices(part.indices, allRows, ws.OrderClause)

		// Precompute ORDER BY values for each row in partition
		orderByVals := make([][]interface{}, len(part.indices))
		partRows := make([]storage.Row, len(part.indices))
		for i, idx := range part.indices {
			partRows[i] = allRows[idx]
			orderByVals[i] = e.orderByValuesForRow(ws.OrderClause, allRows[idx])
		}

		// Compute the window function value for each row in the partition
		for localIdx, globalIdx := range part.indices {
			val, err := e.evalWindowFuncForRow(wf.expr, ws, partRows, localIdx, orderByVals)
			if err != nil {
				return err
			}
			resultRows[globalIdx][wf.colIdx] = val
		}
	}

	return nil
}

type partition struct {
	key     string
	indices []int // indices into allRows
}

func (e *Executor) buildPartitions(allRows []storage.Row, partitionExprs []sqlparser.Expr) []partition {
	if len(partitionExprs) == 0 {
		// Single partition containing all rows
		indices := make([]int, len(allRows))
		for i := range allRows {
			indices[i] = i
		}
		return []partition{{key: "", indices: indices}}
	}

	partMap := make(map[string]int)
	var parts []partition
	for i, row := range allRows {
		key := e.partitionKey(partitionExprs, row)
		if idx, ok := partMap[key]; ok {
			parts[idx].indices = append(parts[idx].indices, i)
		} else {
			partMap[key] = len(parts)
			parts = append(parts, partition{key: key, indices: []int{i}})
		}
	}
	return parts
}

// evalWindowFuncForRow evaluates a window function for a specific row within its partition.
func (e *Executor) evalWindowFuncForRow(
	expr sqlparser.Expr,
	ws *sqlparser.WindowSpecification,
	partRows []storage.Row,
	localIdx int,
	orderByVals [][]interface{},
) (interface{}, error) {
	n := len(partRows)

	switch v := expr.(type) {
	case *sqlparser.ArgumentLessWindowExpr:
		switch v.Type {
		case sqlparser.RowNumberExprType:
			return int64(localIdx + 1), nil

		case sqlparser.RankExprType:
			// RANK: position of first peer in the sorted partition (1-based)
			// Find the first row with the same ORDER BY values
			rank := int64(localIdx + 1)
			for i := localIdx - 1; i >= 0; i-- {
				if windowOrderByEqual(orderByVals[i], orderByVals[localIdx]) {
					rank = int64(i + 1)
				} else {
					break
				}
			}
			return rank, nil

		case sqlparser.DenseRankExprType:
			// DENSE_RANK: like RANK but no gaps
			rank := int64(1)
			for i := 1; i <= localIdx; i++ {
				if !windowOrderByEqual(orderByVals[i-1], orderByVals[i]) {
					rank++
				}
			}
			return rank, nil

		case sqlparser.CumeDistExprType:
			// CUME_DIST: (number of rows <= current row) / total rows
			// "rows <= current row" means rows with ORDER BY value <= current
			count := 0
			for i := 0; i < n; i++ {
				if windowCompareOrderByVals(orderByVals[i], orderByVals[localIdx], ws.OrderClause) <= 0 {
					count++
				}
			}
			return float64(count) / float64(n), nil

		case sqlparser.PercentRankExprType:
			// PERCENT_RANK: (rank - 1) / (total - 1), or 0 if only 1 row
			if n <= 1 {
				return float64(0), nil
			}
			// Compute rank (same as RANK)
			rank := int64(localIdx + 1)
			for i := localIdx - 1; i >= 0; i-- {
				if windowOrderByEqual(orderByVals[i], orderByVals[localIdx]) {
					rank = int64(i + 1)
				} else {
					break
				}
			}
			return float64(rank-1) / float64(n-1), nil
		}

	case *sqlparser.NtileExpr:
		nVal, err := e.evalExpr(v.N)
		if err != nil {
			return nil, err
		}
		if nVal == nil {
			return nil, nil
		}
		buckets := toInt64(nVal)
		if buckets <= 0 {
			return nil, fmt.Errorf("NTILE argument must be positive")
		}
		// NTILE distributes rows into buckets as evenly as possible
		rowsPerBucket := int64(n) / buckets
		remainder := int64(n) % buckets
		idx64 := int64(localIdx)
		if remainder == 0 {
			return idx64/rowsPerBucket + 1, nil
		}
		// First 'remainder' buckets have (rowsPerBucket+1) rows, rest have rowsPerBucket
		bigBucketRows := remainder * (rowsPerBucket + 1)
		if idx64 < bigBucketRows {
			return idx64/(rowsPerBucket+1) + 1, nil
		}
		remaining := idx64 - bigBucketRows
		if rowsPerBucket == 0 {
			return remainder + 1, nil
		}
		return remainder + remaining/rowsPerBucket + 1, nil

	case *sqlparser.LagLeadExpr:
		offset := int64(1)
		if v.N != nil {
			nVal, err := e.evalExpr(v.N)
			if err != nil {
				return nil, err
			}
			if nVal != nil {
				offset = toInt64(nVal)
			}
		}

		var targetIdx int
		if v.Type == sqlparser.LagExprType {
			targetIdx = localIdx - int(offset)
		} else {
			targetIdx = localIdx + int(offset)
		}

		if targetIdx < 0 || targetIdx >= n {
			// Out of range - return default or NULL
			if v.Default != nil {
				return e.evalExpr(v.Default)
			}
			return nil, nil
		}
		return e.evalRowExpr(v.Expr, partRows[targetIdx])

	case *sqlparser.FirstOrLastValueExpr:
		start, end := e.computeFrameBounds(ws.FrameClause, ws.OrderClause, partRows, localIdx, orderByVals)
		if start > end || start >= n || end < 0 {
			return nil, nil
		}
		if start < 0 {
			start = 0
		}
		if end >= n {
			end = n - 1
		}
		if v.Type == sqlparser.FirstValueExprType {
			return e.evalRowExpr(v.Expr, partRows[start])
		}
		// LAST_VALUE
		return e.evalRowExpr(v.Expr, partRows[end])

	case *sqlparser.NTHValueExpr:
		start, end := e.computeFrameBounds(ws.FrameClause, ws.OrderClause, partRows, localIdx, orderByVals)
		if start < 0 {
			start = 0
		}
		if end >= n {
			end = n - 1
		}

		nVal, err := e.evalExpr(v.N)
		if err != nil {
			return nil, err
		}
		if nVal == nil {
			return nil, nil
		}
		nth := int(toInt64(nVal))
		if nth <= 0 {
			return nil, fmt.Errorf("NTH_VALUE argument must be positive")
		}

		targetIdx := start + nth - 1
		if targetIdx > end {
			return nil, nil
		}
		return e.evalRowExpr(v.Expr, partRows[targetIdx])

	case *sqlparser.CountStar:
		start, end := e.computeFrameBounds(ws.FrameClause, ws.OrderClause, partRows, localIdx, orderByVals)
		if start < 0 {
			start = 0
		}
		if end >= n {
			end = n - 1
		}
		if start > end {
			return int64(0), nil
		}
		return int64(end - start + 1), nil

	case *sqlparser.Count:
		start, end := e.computeFrameBounds(ws.FrameClause, ws.OrderClause, partRows, localIdx, orderByVals)
		if start < 0 {
			start = 0
		}
		if end >= n {
			end = n - 1
		}
		count := int64(0)
		for i := start; i <= end; i++ {
			if len(v.Args) > 0 {
				val, _ := e.evalRowExpr(v.Args[0], partRows[i])
				if val != nil {
					count++
				}
			} else {
				count++
			}
		}
		return count, nil

	case *sqlparser.Sum:
		start, end := e.computeFrameBounds(ws.FrameClause, ws.OrderClause, partRows, localIdx, orderByVals)
		if start < 0 {
			start = 0
		}
		if end >= n {
			end = n - 1
		}
		hasValue := false
		sum := float64(0)
		// Track accumulation by type for correct return type.
		var sumU64 uint64
		var sumI64 int64
		allUint64 := true
		allInt64 := true
		for i := start; i <= end; i++ {
			val, _ := e.evalRowExpr(v.Arg, partRows[i])
			if val != nil {
				if u, ok := val.(uint64); ok {
					sumU64 += u
					allInt64 = false
				} else if iv, ok := val.(int64); ok {
					sumI64 += iv
					allUint64 = false
				} else {
					allUint64 = false
					allInt64 = false
				}
				sum += windowToFloat64(val)
				hasValue = true
			}
			// NULL values are ignored in SUM and don't affect the type determination
		}
		if !hasValue {
			return nil, nil
		}
		// If all values were uint64, return uint64 to preserve precision.
		if allUint64 {
			return sumU64, nil
		}
		// If all values were int64 (integer column), return int64 (MySQL returns BIGINT for SUM of integers).
		if allInt64 {
			return sumI64, nil
		}
		// Otherwise return float/decimal
		return fmt.Sprintf("%.1f", sum), nil

	case *sqlparser.Avg:
		start, end := e.computeFrameBounds(ws.FrameClause, ws.OrderClause, partRows, localIdx, orderByVals)
		if start < 0 {
			start = 0
		}
		if end >= n {
			end = n - 1
		}
		sum := float64(0)
		count := 0
		maxScale := 0
		for i := start; i <= end; i++ {
			val, _ := e.evalRowExpr(v.Arg, partRows[i])
			if val != nil {
				sum += windowToFloat64(val)
				count++
				// Track decimal scale from string values (e.g. "1.00" from FLOAT(10,2))
				if s, ok := val.(string); ok {
					if dot := strings.Index(s, "."); dot >= 0 {
						scale := len(s) - dot - 1
						if scale > maxScale {
							maxScale = scale
						}
					}
				}
			}
		}
		if count == 0 {
			return nil, nil
		}
		avg := sum / float64(count)
		// MySQL AVG: (scale+5) decimal places (minimum 5 for integer columns)
		avgScale := maxScale + 5
		return AvgResult{Value: avg, Scale: avgScale}, nil

	case *sqlparser.Min:
		start, end := e.computeFrameBounds(ws.FrameClause, ws.OrderClause, partRows, localIdx, orderByVals)
		if start < 0 {
			start = 0
		}
		if end >= n {
			end = n - 1
		}
		var minVal interface{}
		for i := start; i <= end; i++ {
			val, _ := e.evalRowExpr(v.Arg, partRows[i])
			if val == nil {
				continue
			}
			if minVal == nil || windowCompareValues(val, minVal) < 0 {
				minVal = val
			}
		}
		return minVal, nil

	case *sqlparser.Max:
		start, end := e.computeFrameBounds(ws.FrameClause, ws.OrderClause, partRows, localIdx, orderByVals)
		if start < 0 {
			start = 0
		}
		if end >= n {
			end = n - 1
		}
		var maxVal interface{}
		for i := start; i <= end; i++ {
			val, _ := e.evalRowExpr(v.Arg, partRows[i])
			if val == nil {
				continue
			}
			if maxVal == nil || windowCompareValues(val, maxVal) > 0 {
				maxVal = val
			}
		}
		return maxVal, nil

	case *sqlparser.Variance, *sqlparser.VarPop:
		// VAR_POP / VARIANCE: population variance = sum((x-mean)^2) / n
		var arg sqlparser.Expr
		switch vv := v.(type) {
		case *sqlparser.Variance:
			arg = vv.Arg
		case *sqlparser.VarPop:
			arg = vv.Arg
		}
		start, end := e.computeFrameBounds(ws.FrameClause, ws.OrderClause, partRows, localIdx, orderByVals)
		if start < 0 {
			start = 0
		}
		if end >= n {
			end = n - 1
		}
		var vals []float64
		for i := start; i <= end; i++ {
			val, _ := e.evalRowExpr(arg, partRows[i])
			if val != nil {
				vals = append(vals, windowToFloat64(val))
			}
		}
		if len(vals) == 0 {
			return nil, nil
		}
		mean := 0.0
		for _, x := range vals {
			mean += x
		}
		mean /= float64(len(vals))
		variance := 0.0
		for _, x := range vals {
			d := x - mean
			variance += d * d
		}
		variance /= float64(len(vals))
		return variance, nil

	case *sqlparser.VarSamp:
		// VAR_SAMP: sample variance = sum((x-mean)^2) / (n-1)
		start, end := e.computeFrameBounds(ws.FrameClause, ws.OrderClause, partRows, localIdx, orderByVals)
		if start < 0 {
			start = 0
		}
		if end >= n {
			end = n - 1
		}
		var vals []float64
		for i := start; i <= end; i++ {
			val, _ := e.evalRowExpr(v.Arg, partRows[i])
			if val != nil {
				vals = append(vals, windowToFloat64(val))
			}
		}
		if len(vals) < 2 {
			return nil, nil
		}
		mean := 0.0
		for _, x := range vals {
			mean += x
		}
		mean /= float64(len(vals))
		variance := 0.0
		for _, x := range vals {
			d := x - mean
			variance += d * d
		}
		variance /= float64(len(vals) - 1)
		return variance, nil

	case *sqlparser.Std, *sqlparser.StdDev, *sqlparser.StdPop:
		// STDDEV_POP / STD / STDDEV: sqrt(VAR_POP)
		var arg sqlparser.Expr
		switch vv := v.(type) {
		case *sqlparser.Std:
			arg = vv.Arg
		case *sqlparser.StdDev:
			arg = vv.Arg
		case *sqlparser.StdPop:
			arg = vv.Arg
		}
		start, end := e.computeFrameBounds(ws.FrameClause, ws.OrderClause, partRows, localIdx, orderByVals)
		if start < 0 {
			start = 0
		}
		if end >= n {
			end = n - 1
		}
		var vals []float64
		for i := start; i <= end; i++ {
			val, _ := e.evalRowExpr(arg, partRows[i])
			if val != nil {
				vals = append(vals, windowToFloat64(val))
			}
		}
		if len(vals) == 0 {
			return nil, nil
		}
		mean := 0.0
		for _, x := range vals {
			mean += x
		}
		mean /= float64(len(vals))
		variance := 0.0
		for _, x := range vals {
			d := x - mean
			variance += d * d
		}
		variance /= float64(len(vals))
		return math.Sqrt(variance), nil

	case *sqlparser.StdSamp:
		// STDDEV_SAMP: sqrt(VAR_SAMP)
		start, end := e.computeFrameBounds(ws.FrameClause, ws.OrderClause, partRows, localIdx, orderByVals)
		if start < 0 {
			start = 0
		}
		if end >= n {
			end = n - 1
		}
		var vals []float64
		for i := start; i <= end; i++ {
			val, _ := e.evalRowExpr(v.Arg, partRows[i])
			if val != nil {
				vals = append(vals, windowToFloat64(val))
			}
		}
		if len(vals) < 2 {
			return nil, nil
		}
		mean := 0.0
		for _, x := range vals {
			mean += x
		}
		mean /= float64(len(vals))
		variance := 0.0
		for _, x := range vals {
			d := x - mean
			variance += d * d
		}
		variance /= float64(len(vals) - 1)
		return math.Sqrt(variance), nil

	case *sqlparser.BitAnd:
		start, end := e.computeFrameBounds(ws.FrameClause, ws.OrderClause, partRows, localIdx, orderByVals)
		if start < 0 {
			start = 0
		}
		if end >= n {
			end = n - 1
		}
		result := ^uint64(0) // 18446744073709551615 (all bits set)
		hasValue := false
		for i := start; i <= end; i++ {
			val, _ := e.evalRowExpr(v.Arg, partRows[i])
			if val != nil {
				result &= toUint64ForBitOp(val)
				hasValue = true
			}
		}
		if !hasValue {
			return uint64(^uint64(0)), nil
		}
		return result, nil

	case *sqlparser.BitOr:
		start, end := e.computeFrameBounds(ws.FrameClause, ws.OrderClause, partRows, localIdx, orderByVals)
		if start < 0 {
			start = 0
		}
		if end >= n {
			end = n - 1
		}
		result := uint64(0)
		for i := start; i <= end; i++ {
			val, _ := e.evalRowExpr(v.Arg, partRows[i])
			if val != nil {
				result |= toUint64ForBitOp(val)
			}
		}
		return result, nil

	case *sqlparser.BitXor:
		start, end := e.computeFrameBounds(ws.FrameClause, ws.OrderClause, partRows, localIdx, orderByVals)
		if start < 0 {
			start = 0
		}
		if end >= n {
			end = n - 1
		}
		result := uint64(0)
		for i := start; i <= end; i++ {
			val, _ := e.evalRowExpr(v.Arg, partRows[i])
			if val != nil {
				result ^= toUint64ForBitOp(val)
			}
		}
		return result, nil
	}

	return nil, nil
}

// windowCompareOrderByVals compares two sets of ORDER BY values taking direction into account.
func windowCompareOrderByVals(a, b []interface{}, orderBy sqlparser.OrderBy) int {
	for i := range a {
		if i >= len(b) {
			return 1
		}
		cmp := windowCompareValues(a[i], b[i])
		if cmp == 0 {
			continue
		}
		asc := true
		if i < len(orderBy) {
			asc = orderBy[i].Direction == sqlparser.AscOrder || orderBy[i].Direction == 0
		}
		if !asc {
			cmp = -cmp
		}
		return cmp
	}
	return 0
}

// windowToFloat64 converts a value to float64 for aggregate computations.
func windowToFloat64(val interface{}) float64 {
	if val == nil {
		return 0
	}
	switch v := val.(type) {
	case int64:
		return float64(v)
	case int:
		return float64(v)
	case float64:
		return v
	case float32:
		return float64(v)
	case string:
		var f float64
		fmt.Sscanf(v, "%f", &f)
		return f
	case []byte:
		var f float64
		fmt.Sscanf(string(v), "%f", &f)
		return f
	case uint64:
		return float64(v)
	}
	return 0
}

// windowCompareValues compares two values for ordering purposes.
// Returns -1, 0, or 1.
func windowCompareValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Try string comparison for string types
	sa, aIsStr := a.(string)
	sb, bIsStr := b.(string)
	if aIsStr && bIsStr {
		if sa < sb {
			return -1
		}
		if sa > sb {
			return 1
		}
		return 0
	}
	_, aIsBytes := a.([]byte)
	_, bIsBytes := b.([]byte)
	if aIsStr || bIsStr || aIsBytes || bIsBytes {
		aStr := fmt.Sprintf("%v", a)
		bStr := fmt.Sprintf("%v", b)
		if aStr < bStr {
			return -1
		}
		if aStr > bStr {
			return 1
		}
		return 0
	}

	fa := windowToFloat64Force(a)
	fb := windowToFloat64Force(b)

	if math.IsNaN(fa) || math.IsNaN(fb) {
		return 0
	}
	if fa < fb {
		return -1
	}
	if fa > fb {
		return 1
	}
	return 0
}

func windowToFloat64Force(val interface{}) float64 {
	if val == nil {
		return 0
	}
	switch v := val.(type) {
	case int64:
		return float64(v)
	case int:
		return float64(v)
	case float64:
		return v
	case float32:
		return float64(v)
	case uint64:
		return float64(v)
	case bool:
		if v {
			return 1
		}
		return 0
	case string:
		var f float64
		fmt.Sscanf(v, "%f", &f)
		return f
	case []byte:
		var f float64
		fmt.Sscanf(string(v), "%f", &f)
		return f
	}
	return math.NaN()
}
