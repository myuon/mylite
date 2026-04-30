package executor

import (
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
)

// parsePlanStmt is a helper to parse a query and return the first statement.
func parsePlanStmt(t *testing.T, e *Executor, query string) sqlparser.Statement {
	t.Helper()
	stmt, err := e.parser().Parse(query)
	if err != nil {
		t.Fatalf("parse %q: %v", query, err)
	}
	return stmt
}

// TestPlanBuilder_SimpleSelect verifies that a basic SELECT builds a Project→TableScan tree.
func TestPlanBuilder_SimpleSelect(t *testing.T) {
	e := newTestExecutor(t)
	planner := newPlanner(e)

	stmt := parsePlanStmt(t, e, "SELECT id, val FROM t1 WHERE id = 1")
	plan, err := planner.BuildPlan(stmt)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if plan == nil {
		t.Fatal("expected non-nil plan")
	}

	// Top node should be Project
	if plan.NodeType() != "Project" {
		t.Errorf("expected top node Project, got %s", plan.NodeType())
	}

	// Should contain a Filter node (WHERE clause)
	found := false
	walkPlan(plan, func(n PlanNode) {
		if n.NodeType() == "Filter" {
			found = true
		}
	})
	if !found {
		t.Error("expected Filter node for WHERE clause")
	}

	// Should have at least one TableScan
	if planTableCount(plan) == 0 {
		t.Error("expected at least one TableScan")
	}
}

// TestPlanBuilder_SelectNoFrom verifies that SELECT without FROM produces a Dual leaf.
func TestPlanBuilder_SelectNoFrom(t *testing.T) {
	e := newTestExecutor(t)
	planner := newPlanner(e)

	stmt := parsePlanStmt(t, e, "SELECT 1+1")
	plan, err := planner.BuildPlan(stmt)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	found := false
	walkPlan(plan, func(n PlanNode) {
		if n.NodeType() == "Dual" {
			found = true
		}
	})
	if !found {
		t.Error("expected Dual node for SELECT without FROM")
	}
}

// TestPlanBuilder_Join verifies that a JOIN query builds a JoinNode.
func TestPlanBuilder_Join(t *testing.T) {
	e := newTestExecutor(t)
	planner := newPlanner(e)

	stmt := parsePlanStmt(t, e, "SELECT t1.id, t2.t1_id FROM t1 JOIN t2 ON t1.id = t2.t1_id")
	plan, err := planner.BuildPlan(stmt)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	found := false
	walkPlan(plan, func(n PlanNode) {
		if n.NodeType() == "Join" {
			found = true
		}
	})
	if !found {
		t.Error("expected Join node")
	}

	if planTableCount(plan) < 2 {
		t.Errorf("expected 2 TableScan nodes, got %d", planTableCount(plan))
	}
}

// TestPlanBuilder_Subquery verifies that a query with a subquery in WHERE is handled.
func TestPlanBuilder_Subquery(t *testing.T) {
	e := newTestExecutor(t)
	planner := newPlanner(e)

	stmt := parsePlanStmt(t, e, "SELECT id FROM t1 WHERE id IN (SELECT t1_id FROM t2)")
	plan, err := planner.BuildPlan(stmt)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
	// The top-level node should be QueryWithSubqueries with one Subquery child.
	qws, ok := plan.(*QueryWithSubqueries)
	if !ok {
		t.Fatalf("expected QueryWithSubqueries top node, got %T", plan)
	}
	if len(qws.Subqueries) != 1 {
		t.Fatalf("expected 1 subquery, got %d", len(qws.Subqueries))
	}
	if qws.Subqueries[0].SelectType != "SUBQUERY" {
		t.Errorf("expected SUBQUERY label, got %q", qws.Subqueries[0].SelectType)
	}
}

// TestPlanBuilder_DerivedTableIDs verifies that the derived table inner SELECT
// reuses the placeholder id (so EXPLAIN emits "id=1 PRIMARY <derived2>" plus
// "id=2 DERIVED ..." rather than allocating an extra id).
func TestPlanBuilder_DerivedTableIDs(t *testing.T) {
	e := newTestExecutor(t)
	planner := newPlanner(e)

	stmt := parsePlanStmt(t, e, "SELECT d.id FROM (SELECT id FROM t1) AS d")
	plan, err := planner.BuildPlan(stmt)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	var dt *DerivedTableNode
	walkPlan(plan, func(n PlanNode) {
		if d, ok := n.(*DerivedTableNode); ok && dt == nil {
			dt = d
		}
	})
	if dt == nil {
		t.Fatal("expected DerivedTableNode")
	}
	if dt.ParentID != 1 {
		t.Errorf("expected ParentID=1, got %d", dt.ParentID)
	}
	if dt.ID != 2 {
		t.Errorf("expected DerivedTableNode.ID=2, got %d", dt.ID)
	}
	if dt.ParentSelectType != "PRIMARY" {
		t.Errorf("expected ParentSelectType=PRIMARY, got %q", dt.ParentSelectType)
	}
	// The inner DERIVED rows must use the same id as the placeholder (2).
	walkPlan(dt.Plan, func(n PlanNode) {
		if ts, ok := n.(*TableScanNode); ok {
			if ts.ID != 2 {
				t.Errorf("expected inner TableScan ID=2, got %d", ts.ID)
			}
			if ts.SelectType != "DERIVED" {
				t.Errorf("expected inner TableScan SelectType=DERIVED, got %q", ts.SelectType)
			}
		}
	})
}

// TestPlanBuilder_SubqueryDependent verifies that a correlated subquery is
// labelled "DEPENDENT SUBQUERY" by the planner.
func TestPlanBuilder_SubqueryDependent(t *testing.T) {
	e := newTestExecutor(t)
	planner := newPlanner(e)

	stmt := parsePlanStmt(t, e, "SELECT id FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.t1_id = t1.id)")
	plan, err := planner.BuildPlan(stmt)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	qws, ok := plan.(*QueryWithSubqueries)
	if !ok {
		t.Fatalf("expected QueryWithSubqueries top node, got %T", plan)
	}
	if len(qws.Subqueries) != 1 {
		t.Fatalf("expected 1 subquery, got %d", len(qws.Subqueries))
	}
	if !qws.Subqueries[0].IsCorrelated {
		t.Error("expected correlated subquery")
	}
	if qws.Subqueries[0].SelectType != "DEPENDENT SUBQUERY" {
		t.Errorf("expected DEPENDENT SUBQUERY label, got %q", qws.Subqueries[0].SelectType)
	}
}

// TestPlanExplain_DerivedTablePlaceholder verifies that EXPLAIN output for a
// query with a derived FROM table emits a "<derivedN>" placeholder row at the
// parent's id/select_type, followed by the inner DERIVED rows.
func TestPlanExplain_DerivedTablePlaceholder(t *testing.T) {
	e := newTestExecutor(t)
	planner := newPlanner(e)

	stmt := parsePlanStmt(t, e, "SELECT d.id FROM (SELECT id FROM t1) AS d")
	plan, err := planner.BuildPlan(stmt)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	plan = planner.optimize(plan, stmt.(*sqlparser.Select))

	pe := &PlanExplainer{executor: e, query: "SELECT d.id FROM (SELECT id FROM t1) AS d"}
	rows := pe.ExplainTraditional(plan)
	if len(rows) < 2 {
		t.Fatalf("expected at least 2 rows, got %d (%v)", len(rows), rows)
	}
	// First row: id=1, select_type=PRIMARY, table=<derived2>.
	if rows[0][0].(int64) != 1 {
		t.Errorf("row 0 id: expected 1, got %v", rows[0][0])
	}
	if rows[0][1] != "PRIMARY" {
		t.Errorf("row 0 select_type: expected PRIMARY, got %v", rows[0][1])
	}
	if rows[0][2] != "<derived2>" {
		t.Errorf("row 0 table: expected <derived2>, got %v", rows[0][2])
	}
	// Second row: id=2, select_type=DERIVED, table=t1.
	if rows[1][0].(int64) != 2 {
		t.Errorf("row 1 id: expected 2, got %v", rows[1][0])
	}
	if rows[1][1] != "DERIVED" {
		t.Errorf("row 1 select_type: expected DERIVED, got %v", rows[1][1])
	}
	if rows[1][2] != "t1" {
		t.Errorf("row 1 table: expected t1, got %v", rows[1][2])
	}
}

// TestPlanExplain_SubqueryRows verifies that a SUBQUERY in WHERE produces an
// EXPLAIN row with select_type=SUBQUERY and a fresh id.
func TestPlanExplain_SubqueryRows(t *testing.T) {
	e := newTestExecutor(t)
	planner := newPlanner(e)

	stmt := parsePlanStmt(t, e, "SELECT id FROM t1 WHERE id IN (SELECT t1_id FROM t2)")
	plan, err := planner.BuildPlan(stmt)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	plan = planner.optimize(plan, stmt.(*sqlparser.Select))

	pe := &PlanExplainer{executor: e, query: "SELECT id FROM t1 WHERE id IN (SELECT t1_id FROM t2)"}
	rows := pe.ExplainTraditional(plan)
	if len(rows) < 2 {
		t.Fatalf("expected at least 2 rows, got %d (%v)", len(rows), rows)
	}
	// Last row should be the SUBQUERY one.
	last := rows[len(rows)-1]
	if last[1] != "SUBQUERY" {
		t.Errorf("last row select_type: expected SUBQUERY, got %v", last[1])
	}
	if last[0].(int64) != 2 {
		t.Errorf("last row id: expected 2, got %v", last[0])
	}
}

// TestPlanBuilder_Union verifies that a UNION query builds a UnionNode.
func TestPlanBuilder_Union(t *testing.T) {
	e := newTestExecutor(t)
	planner := newPlanner(e)

	stmt := parsePlanStmt(t, e, "SELECT id FROM t1 UNION SELECT id FROM t2")
	plan, err := planner.BuildPlan(stmt)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	found := false
	walkPlan(plan, func(n PlanNode) {
		if n.NodeType() == "Union" {
			found = true
		}
	})
	if !found {
		t.Error("expected Union node for UNION query")
	}
}

// TestPlanBuilder_DerivedTable verifies that a subquery in FROM produces a DerivedTable node.
func TestPlanBuilder_DerivedTable(t *testing.T) {
	e := newTestExecutor(t)
	planner := newPlanner(e)

	stmt := parsePlanStmt(t, e, "SELECT d.id FROM (SELECT id FROM t1) AS d")
	plan, err := planner.BuildPlan(stmt)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	found := false
	walkPlan(plan, func(n PlanNode) {
		if n.NodeType() == "DerivedTable" {
			found = true
		}
	})
	if !found {
		t.Error("expected DerivedTable node for subquery in FROM")
	}
}

// TestPlanBuilder_OrderByLimit verifies Sort and Limit nodes are produced.
func TestPlanBuilder_OrderByLimit(t *testing.T) {
	e := newTestExecutor(t)
	planner := newPlanner(e)

	stmt := parsePlanStmt(t, e, "SELECT id FROM t1 ORDER BY id LIMIT 10")
	plan, err := planner.BuildPlan(stmt)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	hasSort := false
	hasLimit := false
	walkPlan(plan, func(n PlanNode) {
		if n.NodeType() == "Sort" {
			hasSort = true
		}
		if n.NodeType() == "Limit" {
			hasLimit = true
		}
	})
	if !hasSort {
		t.Error("expected Sort node for ORDER BY")
	}
	if !hasLimit {
		t.Error("expected Limit node for LIMIT clause")
	}
}

// TestPlanBuilder_GroupBy verifies Aggregate node is produced for GROUP BY.
func TestPlanBuilder_GroupBy(t *testing.T) {
	e := newTestExecutor(t)
	planner := newPlanner(e)

	stmt := parsePlanStmt(t, e, "SELECT id, COUNT(*) FROM t1 GROUP BY id")
	plan, err := planner.BuildPlan(stmt)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	found := false
	walkPlan(plan, func(n PlanNode) {
		if n.NodeType() == "Aggregate" {
			found = true
		}
	})
	if !found {
		t.Error("expected Aggregate node for GROUP BY")
	}
}

// TestPlanString verifies that planString produces a non-empty result.
func TestPlanString(t *testing.T) {
	e := newTestExecutor(t)
	planner := newPlanner(e)

	stmt := parsePlanStmt(t, e, "SELECT id FROM t1 WHERE id = 1")
	plan, err := planner.BuildPlan(stmt)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	s := planString(plan)
	if s == "" {
		t.Error("planString should return non-empty string")
	}
}
