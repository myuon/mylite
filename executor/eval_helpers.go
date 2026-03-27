package executor

import (
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
)

// evalArg1 evaluates a single required argument from a function call.
// It checks the minimum argument count, evaluates the first expression,
// and checks for NULL. Returns (value, isNull, error).
// If isNull is true, the caller should return nil, true, nil.
func (e *Executor) evalArg1(exprs []sqlparser.Expr, funcName string) (interface{}, bool, error) {
	if len(exprs) < 1 {
		return nil, false, fmt.Errorf("%s requires 1 argument", funcName)
	}
	val, err := e.evalExpr(exprs[0])
	if err != nil {
		return nil, false, err
	}
	if val == nil {
		return nil, true, nil
	}
	return val, false, nil
}

// evalArg1Quiet evaluates a single argument without error on missing args
// (returns nil, true, nil instead of error). Used by functions that return
// NULL silently when no arguments are provided.
func (e *Executor) evalArg1Quiet(exprs []sqlparser.Expr) (interface{}, bool, error) {
	if len(exprs) < 1 {
		return nil, true, nil
	}
	val, err := e.evalExpr(exprs[0])
	if err != nil {
		return nil, false, err
	}
	if val == nil {
		return nil, true, nil
	}
	return val, false, nil
}

// evalArgs2 evaluates exactly 2 required arguments.
// Returns (val0, val1, hasNull, error). If hasNull is true and error is nil,
// at least one argument was NULL.
func (e *Executor) evalArgs2(exprs []sqlparser.Expr, funcName string) (interface{}, interface{}, bool, error) {
	if len(exprs) < 2 {
		return nil, nil, false, fmt.Errorf("%s requires 2 arguments", funcName)
	}
	v0, err := e.evalExpr(exprs[0])
	if err != nil {
		return nil, nil, false, err
	}
	v1, err := e.evalExpr(exprs[1])
	if err != nil {
		return nil, nil, false, err
	}
	if v0 == nil || v1 == nil {
		return nil, nil, true, nil
	}
	return v0, v1, false, nil
}

// evalArgs3 evaluates exactly 3 required arguments.
// Returns (val0, val1, val2, hasNull, error).
func (e *Executor) evalArgs3(exprs []sqlparser.Expr, funcName string) (interface{}, interface{}, interface{}, bool, error) {
	if len(exprs) < 3 {
		return nil, nil, nil, false, fmt.Errorf("%s requires 3 arguments", funcName)
	}
	v0, err := e.evalExpr(exprs[0])
	if err != nil {
		return nil, nil, nil, false, err
	}
	v1, err := e.evalExpr(exprs[1])
	if err != nil {
		return nil, nil, nil, false, err
	}
	v2, err := e.evalExpr(exprs[2])
	if err != nil {
		return nil, nil, nil, false, err
	}
	if v0 == nil || v1 == nil || v2 == nil {
		return nil, nil, nil, true, nil
	}
	return v0, v1, v2, false, nil
}
