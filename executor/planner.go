package executor

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

// Planner converts a parsed SQL AST into a logical PlanNode tree.
type Planner struct {
	executor  *Executor
	idCounter int64
}

func newPlanner(e *Executor) *Planner {
	return &Planner{executor: e, idCounter: 1}
}

func (p *Planner) nextID() int64 {
	id := p.idCounter
	p.idCounter++
	return id
}

// BuildPlan constructs a logical plan from a parsed SQL statement.
// Currently supports SELECT and UNION.
func (p *Planner) BuildPlan(stmt sqlparser.Statement) (PlanNode, error) {
	switch s := stmt.(type) {
	case *sqlparser.Select:
		return p.buildSelectPlan(s, "SIMPLE")
	case *sqlparser.Union:
		return p.buildUnionPlan(s, true)
	default:
		return nil, fmt.Errorf("planner: unsupported statement type %T", stmt)
	}
}

// buildSelectPlan builds a logical plan for a SELECT statement.
// The returned plan tree is (bottom-up): Source → Filter → Aggregate → Project → Sort → Limit
func (p *Planner) buildSelectPlan(sel *sqlparser.Select, selectType string) (PlanNode, error) {
	id := p.nextID()

	// Promote selectType from SIMPLE to PRIMARY when query has complex parts
	// (subqueries, derived tables, etc.)
	if selectType == "SIMPLE" && p.executor.queryHasComplexParts(sel) {
		selectType = "PRIMARY"
	}

	// 1. Build FROM clause plan
	var source PlanNode
	var err error
	if len(sel.From) == 0 {
		source = &DualNode{SelectType: selectType, ID: id}
	} else {
		source, err = p.buildFromPlan(sel.From, selectType, id)
		if err != nil {
			return nil, err
		}
	}

	// 2. WHERE → FilterNode
	if sel.Where != nil {
		source = &FilterNode{
			Condition: sel.Where.Expr,
			Child:     source,
		}
	}

	// 3. GROUP BY / HAVING → AggregateNode
	hasGroupBy := sel.GroupBy != nil && len(sel.GroupBy.Exprs) > 0
	hasHaving := sel.Having != nil
	if hasGroupBy || hasHaving {
		var havingExpr sqlparser.Expr
		if sel.Having != nil {
			havingExpr = sel.Having.Expr
		}
		source = &AggregateNode{
			GroupBy: sel.GroupBy,
			Having:  havingExpr,
			Child:   source,
		}
	}

	// 4. SELECT expressions → ProjectNode
	source = &ProjectNode{
		Exprs:      sel.SelectExprs,
		IsDistinct: sel.Distinct,
		Child:      source,
	}

	// 5. ORDER BY → SortNode
	if len(sel.OrderBy) > 0 {
		source = &SortNode{
			OrderBy: sel.OrderBy,
			Child:   source,
		}
	}

	// 6. LIMIT → LimitNode
	if sel.Limit != nil {
		source = &LimitNode{
			Count:  sel.Limit.Rowcount,
			Offset: sel.Limit.Offset,
			Child:  source,
		}
	}

	return source, nil
}

// buildFromPlan builds a plan from a list of FROM table expressions.
// Multiple tables are treated as implicit CROSS JOINs.
func (p *Planner) buildFromPlan(from []sqlparser.TableExpr, selectType string, id int64) (PlanNode, error) {
	if len(from) == 0 {
		return &DualNode{SelectType: selectType, ID: id}, nil
	}

	// Build plan for the first table
	left, err := p.buildTableExprPlan(from[0], selectType, id)
	if err != nil {
		return nil, err
	}

	// Implicit cross join for additional FROM tables
	for i := 1; i < len(from); i++ {
		right, err := p.buildTableExprPlan(from[i], selectType, id)
		if err != nil {
			return nil, err
		}
		left = &JoinNode{
			JoinType:  "CROSS",
			Condition: nil,
			Left:      left,
			Right:     right,
		}
	}

	return left, nil
}

// buildTableExprPlan builds a plan node for a single table expression.
func (p *Planner) buildTableExprPlan(te sqlparser.TableExpr, selectType string, id int64) (PlanNode, error) {
	switch t := te.(type) {
	case *sqlparser.AliasedTableExpr:
		// Check if this is a derived table (subquery in FROM)
		if dt, ok := t.Expr.(*sqlparser.DerivedTable); ok {
			derivedID := p.nextID()
			var innerPlan PlanNode
			var err error
			switch inner := dt.Select.(type) {
			case *sqlparser.Select:
				innerPlan, err = p.buildSelectPlan(inner, "DERIVED")
			case *sqlparser.Union:
				innerPlan, err = p.buildUnionPlan(inner, false)
			default:
				return nil, fmt.Errorf("planner: unsupported derived table type %T", dt.Select)
			}
			if err != nil {
				return nil, err
			}
			alias := ""
			if !t.As.IsEmpty() {
				alias = t.As.String()
			}
			return &DerivedTableNode{
				Alias: alias,
				Plan:  innerPlan,
				ID:    derivedID,
			}, nil
		}

		// Regular table
		if tn, ok := t.Expr.(sqlparser.TableName); ok {
			tableName := tn.Name.String()
			// MySQL's implicit DUAL pseudo-table
			if strings.ToLower(tableName) == "dual" {
				return &DualNode{SelectType: selectType, ID: id}, nil
			}
			dbName := tn.Qualifier.String()
			alias := ""
			if !t.As.IsEmpty() {
				alias = t.As.String()
			}

			var rowEstimate int64 = 1
			var tableDef interface{} = nil
			if p.executor.Storage != nil {
				if tbl, err := p.executor.Storage.GetTable(p.executor.CurrentDB, tableName); err == nil {
					if n := len(tbl.Rows); n > 0 {
						rowEstimate = int64(n)
					}
				}
			}
			_ = tableDef

			// Look up catalog def
			var td interface{} = nil
			if p.executor.Catalog != nil {
				if db, err := p.executor.Catalog.GetDatabase(p.executor.CurrentDB); err == nil {
					if tdef, err := db.GetTable(tableName); err == nil {
						td = tdef
					}
				}
			}
			_ = td

			node := &TableScanNode{
				TableName:    tableName,
				Alias:        alias,
				DatabaseName: dbName,
				RowEstimate:  rowEstimate,
				SelectType:   selectType,
				ID:           id,
			}
			return node, nil
		}

		// Fallback for other AliasedTableExpr types (e.g. JSONTableExpr)
		return &DualNode{SelectType: selectType, ID: id}, nil

	case *sqlparser.JoinTableExpr:
		left, err := p.buildTableExprPlan(t.LeftExpr, selectType, id)
		if err != nil {
			return nil, err
		}
		right, err := p.buildTableExprPlan(t.RightExpr, selectType, id)
		if err != nil {
			return nil, err
		}

		joinType := joinTypeString(t.Join)
		var condition sqlparser.Expr
		var using []string
		if t.Condition != nil {
			condition = t.Condition.On
			for _, col := range t.Condition.Using {
				using = append(using, col.Lowered())
			}
		}

		return &JoinNode{
			JoinType:  joinType,
			Condition: condition,
			Using:     using,
			Left:      left,
			Right:     right,
		}, nil

	case *sqlparser.ParenTableExpr:
		if len(t.Exprs) == 0 {
			return &DualNode{SelectType: selectType, ID: id}, nil
		}
		return p.buildFromPlan(t.Exprs, selectType, id)

	default:
		return &DualNode{SelectType: selectType, ID: id}, nil
	}
}

// buildUnionPlan builds a plan for a UNION / UNION ALL statement.
func (p *Planner) buildUnionPlan(u *sqlparser.Union, isTopLevel bool) (PlanNode, error) {
	unionID := p.nextID()
	selects := p.executor.flattenUnion(u)

	var branches []PlanNode
	for i, sel := range selects {
		var selectType string
		if i == 0 {
			if isTopLevel {
				selectType = "PRIMARY"
			} else {
				selectType = "DERIVED"
			}
		} else {
			selectType = "UNION"
		}

		var branch PlanNode
		var err error
		switch s := sel.(type) {
		case *sqlparser.Select:
			branch, err = p.buildSelectPlan(s, selectType)
		case *sqlparser.Union:
			branch, err = p.buildUnionPlan(s, false)
		default:
			return nil, fmt.Errorf("planner: unsupported union branch type %T", sel)
		}
		if err != nil {
			return nil, err
		}
		branches = append(branches, branch)
	}

	isAll := !u.Distinct
	return &UnionNode{
		IsAll:    isAll,
		Branches: branches,
		ID:       unionID,
	}, nil
}

// joinTypeString converts a vitess JoinType to a human-readable string.
func joinTypeString(jt sqlparser.JoinType) string {
	switch jt {
	case sqlparser.LeftJoinType:
		return "LEFT"
	case sqlparser.RightJoinType:
		return "RIGHT"
	case sqlparser.StraightJoinType:
		return "STRAIGHT"
	case sqlparser.NaturalJoinType:
		return "NATURAL"
	case sqlparser.NaturalLeftJoinType:
		return "NATURAL LEFT"
	case sqlparser.NaturalRightJoinType:
		return "NATURAL RIGHT"
	default:
		return "INNER"
	}
}

// optimize runs a post-build optimization pass over the plan tree.
// It fills in AccessPath information for each TableScanNode and propagates
// Extra hints (Using where, Using filesort, Using temporary) from parent nodes.
//
// The sel parameter is the top-level SELECT (used for WHERE/index lookup).
// For UNION or complex queries this may be nil, in which case optimization is skipped.
func (p *Planner) optimize(plan PlanNode, sel *sqlparser.Select) PlanNode {
	if plan == nil {
		return plan
	}
	p.optimizeNode(plan, sel, false, false, false)
	return plan
}

// optimizeNode recursively visits nodes, accumulating context flags.
// hasSortAbove / hasAggAbove / hasFilterAbove track whether a Sort / Aggregate / Filter
// node sits above the current node in the tree.
func (p *Planner) optimizeNode(node PlanNode, sel *sqlparser.Select, hasSortAbove, hasAggAbove, hasFilterAbove bool) {
	if node == nil {
		return
	}

	switch n := node.(type) {
	case *TableScanNode:
		// Fill AccessPath from explainDetectAccessType if we have a SELECT context.
		if sel != nil {
			ai := p.executor.explainDetectAccessType(sel, n.TableName)
			n.AccessPath = AccessPath{
				Type:         ai.accessType,
				PossibleKeys: stringify(ai.possibleKeys),
				Key:          stringify(ai.key),
				KeyLen:       stringify(ai.keyLen),
				Ref:          stringify(ai.ref),
			}
			if n.AccessPath.Type == "" {
				n.AccessPath.Type = "ALL"
			}
		} else {
			if n.AccessPath.Type == "" {
				n.AccessPath.Type = "ALL"
			}
		}

		// Propagate Extra hints from above.
		if hasFilterAbove {
			n.Extra = appendUnique(n.Extra, "Using where")
		}
		if hasSortAbove {
			n.Extra = appendUnique(n.Extra, "Using filesort")
		}
		if hasAggAbove {
			n.Extra = appendUnique(n.Extra, "Using temporary")
		}

	case *FilterNode:
		p.optimizeNode(n.Child, sel, hasSortAbove, hasAggAbove, true)

	case *SortNode:
		p.optimizeNode(n.Child, sel, true, hasAggAbove, hasFilterAbove)

	case *AggregateNode:
		p.optimizeNode(n.Child, sel, hasSortAbove, true, hasFilterAbove)

	case *ProjectNode:
		p.optimizeNode(n.Child, sel, hasSortAbove, hasAggAbove, hasFilterAbove)

	case *LimitNode:
		p.optimizeNode(n.Child, sel, hasSortAbove, hasAggAbove, hasFilterAbove)

	case *JoinNode:
		// Both sides share the same context flags.
		p.optimizeNode(n.Left, sel, hasSortAbove, hasAggAbove, hasFilterAbove)
		p.optimizeNode(n.Right, sel, hasSortAbove, hasAggAbove, hasFilterAbove)

	case *UnionNode:
		for _, branch := range n.Branches {
			p.optimizeNode(branch, nil, false, false, false)
		}

	case *DerivedTableNode:
		// Derived table inner plan has its own SELECT context.
		p.optimizeNode(n.Plan, nil, false, false, false)

	case *SubqueryNode:
		p.optimizeNode(n.Plan, nil, false, false, false)

	default:
		for _, child := range node.Children() {
			p.optimizeNode(child, sel, hasSortAbove, hasAggAbove, hasFilterAbove)
		}
	}
}

// stringify converts an interface{} to string; returns "" for nil.
func stringify(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// appendUnique appends s to slice only if not already present.
func appendUnique(slice []string, s string) []string {
	for _, existing := range slice {
		if existing == s {
			return slice
		}
	}
	return append(slice, s)
}

// walkPlan visits every node in the plan tree in depth-first pre-order.
func walkPlan(node PlanNode, fn func(PlanNode)) {
	if node == nil {
		return
	}
	fn(node)
	for _, child := range node.Children() {
		walkPlan(child, fn)
	}
}

// planTableCount returns the number of TableScanNode leaves in the tree.
func planTableCount(node PlanNode) int {
	count := 0
	walkPlan(node, func(n PlanNode) {
		if _, ok := n.(*TableScanNode); ok {
			count++
		}
	})
	return count
}

// planNodeTypeNames collects NodeType strings in depth-first order.
func planNodeTypeNames(node PlanNode) []string {
	var names []string
	walkPlan(node, func(n PlanNode) {
		names = append(names, n.NodeType())
	})
	return names
}

// planString returns a compact textual representation of a plan for debugging.
func planString(node PlanNode) string {
	if node == nil {
		return "<nil>"
	}
	children := node.Children()
	if len(children) == 0 {
		switch n := node.(type) {
		case *TableScanNode:
			return fmt.Sprintf("TableScan(%s)", n.TableName)
		case *DualNode:
			return "Dual"
		}
		return node.NodeType()
	}
	parts := make([]string, len(children))
	for i, c := range children {
		parts[i] = planString(c)
	}
	return fmt.Sprintf("%s(%s)", node.NodeType(), strings.Join(parts, ", "))
}
