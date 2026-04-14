package executor

// PlanExplainer converts a logical PlanNode tree to EXPLAIN output rows.
// This is Phase 1 scaffolding; actual output still uses the existing explain path.
type PlanExplainer struct {
	executor *Executor
	query    string // original query for access type detection
}

func newPlanExplainer(e *Executor, query string) *PlanExplainer {
	return &PlanExplainer{executor: e, query: query}
}

// ExplainTraditional walks the plan tree depth-first and returns EXPLAIN rows.
// Each row is a []interface{} matching MySQL EXPLAIN column order:
//   id, select_type, table, partitions, type, possible_keys, key, key_len, ref, rows, filtered, Extra
//
// NOTE: Phase 1 — this is scaffolding. The output may not match MySQL exactly.
// The existing explainMultiRows path is still used for actual EXPLAIN execution.
func (pe *PlanExplainer) ExplainTraditional(plan PlanNode) [][]interface{} {
	var rows [][]interface{}
	pe.collectRows(plan, &rows, nil)
	return rows
}

// collectRows recursively visits a plan node and appends EXPLAIN rows.
func (pe *PlanExplainer) collectRows(node PlanNode, rows *[][]interface{}, extraAccum []string) {
	if node == nil {
		return
	}

	switch n := node.(type) {
	case *TableScanNode:
		extra := buildExtraString(append(n.Extra, extraAccum...))

		// Use AccessPath data populated by the optimizer pass.
		accessType := n.AccessPath.Type
		if accessType == "" {
			accessType = "ALL"
		}
		var possibleKeys interface{} = nilIfEmptyStr(n.AccessPath.PossibleKeys)
		var key interface{} = nilIfEmptyStr(n.AccessPath.Key)
		var keyLen interface{} = nilIfEmptyStr(n.AccessPath.KeyLen)
		var ref interface{} = nilIfEmptyStr(n.AccessPath.Ref)

		// Row count: shrink to 1 for index lookups that guarantee 1 row.
		rowEstimate := n.RowEstimate
		if accessType == "const" || accessType == "eq_ref" || accessType == "ref" {
			rowEstimate = 1
		}

		*rows = append(*rows, []interface{}{
			n.ID,
			n.SelectType,
			n.TableName,
			nil, // partitions
			accessType,
			possibleKeys,
			key,
			keyLen,
			ref,
			rowEstimate,
			"100.00",
			extra,
		})

	case *DualNode:
		extra := buildExtraString(append([]string{"No tables used"}, extraAccum...))
		*rows = append(*rows, []interface{}{
			n.ID,
			n.SelectType,
			nil, // table
			nil, nil, nil, nil, nil, nil,
			nil, // rows
			nil, // filtered
			extra,
		})

	case *FilterNode:
		// Propagate "Using where" down to the leaf
		pe.collectRows(n.Child, rows, append(extraAccum, "Using where"))

	case *ProjectNode:
		pe.collectRows(n.Child, rows, extraAccum)

	case *SortNode:
		// Propagate "Using filesort" down to the leaf
		pe.collectRows(n.Child, rows, append(extraAccum, "Using filesort"))

	case *LimitNode:
		pe.collectRows(n.Child, rows, extraAccum)

	case *AggregateNode:
		pe.collectRows(n.Child, rows, extraAccum)

	case *JoinNode:
		pe.collectRows(n.Left, rows, extraAccum)
		pe.collectRows(n.Right, rows, extraAccum)

	case *UnionNode:
		for _, branch := range n.Branches {
			pe.collectRows(branch, rows, extraAccum)
		}
		// UNION RESULT row
		*rows = append(*rows, []interface{}{
			nil,
			"UNION RESULT",
			"<union>",
			nil, "ALL", nil, nil, nil, nil,
			nil, nil,
			"Using temporary",
		})

	case *SubqueryNode:
		pe.collectRows(n.Plan, rows, extraAccum)

	case *DerivedTableNode:
		pe.collectRows(n.Plan, rows, extraAccum)
	}
}

// nilIfEmptyStr returns nil if s is empty, otherwise returns s as interface{}.
func nilIfEmptyStr(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// buildExtraString joins a list of extra strings with "; ", deduplicating and
// filtering empty strings. Returns nil if the result is empty.
func buildExtraString(parts []string) interface{} {
	seen := make(map[string]bool)
	var out []string
	for _, p := range parts {
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil
	}
	result := ""
	for i, p := range out {
		if i > 0 {
			result += "; "
		}
		result += p
	}
	return result
}
