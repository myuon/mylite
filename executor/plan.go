package executor

import (
	"github.com/myuon/mylite/catalog"
	"vitess.io/vitess/go/vt/sqlparser"
)

// PlanNode is the interface for all logical plan tree nodes.
type PlanNode interface {
	Children() []PlanNode
	NodeType() string
}

// ---- Leaf nodes ----

// TableScanNode represents a full or index scan of a base table.
type TableScanNode struct {
	TableName    string
	Alias        string
	DatabaseName string
	RowEstimate  int64
	TableDef     *catalog.TableDef
	SelectType   string // "SIMPLE", "PRIMARY", "DERIVED", etc.
	ID           int64
	Extra        []string
}

func (n *TableScanNode) Children() []PlanNode { return nil }
func (n *TableScanNode) NodeType() string     { return "TableScan" }

// DualNode represents SELECT without a FROM clause (MySQL's DUAL pseudo-table).
type DualNode struct {
	SelectType string
	ID         int64
}

func (n *DualNode) Children() []PlanNode { return nil }
func (n *DualNode) NodeType() string     { return "Dual" }

// ---- Unary nodes ----

// FilterNode applies a predicate to its child.
type FilterNode struct {
	Condition sqlparser.Expr
	Child     PlanNode
}

func (n *FilterNode) Children() []PlanNode { return []PlanNode{n.Child} }
func (n *FilterNode) NodeType() string     { return "Filter" }

// ProjectNode selects / computes output columns.
type ProjectNode struct {
	Exprs      *sqlparser.SelectExprs
	IsDistinct bool
	Child      PlanNode
}

func (n *ProjectNode) Children() []PlanNode { return []PlanNode{n.Child} }
func (n *ProjectNode) NodeType() string     { return "Project" }

// SortNode applies ORDER BY.
type SortNode struct {
	OrderBy sqlparser.OrderBy
	Child   PlanNode
}

func (n *SortNode) Children() []PlanNode { return []PlanNode{n.Child} }
func (n *SortNode) NodeType() string     { return "Sort" }

// LimitNode applies LIMIT / OFFSET.
type LimitNode struct {
	Count  sqlparser.Expr
	Offset sqlparser.Expr
	Child  PlanNode
}

func (n *LimitNode) Children() []PlanNode { return []PlanNode{n.Child} }
func (n *LimitNode) NodeType() string     { return "Limit" }

// AggregateNode applies GROUP BY and HAVING.
type AggregateNode struct {
	GroupBy *sqlparser.GroupBy
	Having  sqlparser.Expr
	Child   PlanNode
}

func (n *AggregateNode) Children() []PlanNode { return []PlanNode{n.Child} }
func (n *AggregateNode) NodeType() string     { return "Aggregate" }

// ---- Binary / multi-child nodes ----

// JoinNode represents an explicit or implicit join between two plan nodes.
type JoinNode struct {
	JoinType  string // "INNER", "LEFT", "RIGHT", "CROSS", etc.
	Condition sqlparser.Expr
	Using     []string
	Left      PlanNode
	Right     PlanNode
}

func (n *JoinNode) Children() []PlanNode { return []PlanNode{n.Left, n.Right} }
func (n *JoinNode) NodeType() string     { return "Join" }

// UnionNode represents a UNION / UNION ALL of multiple branches.
type UnionNode struct {
	IsAll    bool
	Branches []PlanNode
	ID       int64
}

func (n *UnionNode) Children() []PlanNode { return n.Branches }
func (n *UnionNode) NodeType() string     { return "Union" }

// ---- Subquery nodes ----

// SubqueryNode wraps a subquery that appears in WHERE / SELECT etc.
type SubqueryNode struct {
	Plan         PlanNode
	SelectType   string
	ID           int64
	IsCorrelated bool
}

func (n *SubqueryNode) Children() []PlanNode { return []PlanNode{n.Plan} }
func (n *SubqueryNode) NodeType() string     { return "Subquery" }

// DerivedTableNode wraps a derived table (subquery in FROM clause).
type DerivedTableNode struct {
	Alias string
	Plan  PlanNode
	ID    int64
}

func (n *DerivedTableNode) Children() []PlanNode { return []PlanNode{n.Plan} }
func (n *DerivedTableNode) NodeType() string     { return "DerivedTable" }
