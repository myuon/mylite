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
	// Plan should be non-trivial (at least Project wrapping something)
	if plan.NodeType() == "" {
		t.Error("expected non-empty node type")
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
