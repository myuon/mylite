package executor

import "testing"

// TestMysqlFormatGenExprNestedParens verifies that nested binary
// subexpressions in a GENERATED ALWAYS AS expression are wrapped in
// parens to match MySQL's SHOW CREATE TABLE output.
//
// MySQL renders e.g. ``GENERATED ALWAYS AS (((`h` * 2) + `b`))`` —
// each nested BinaryExpr operand of another BinaryExpr is wrapped in
// parens regardless of operator precedence.
func TestMysqlFormatGenExprNestedParens(t *testing.T) {
	cases := []struct {
		name string
		expr string
		want string
	}{
		{"simple_mul", "b * 2", "(`b` * 2)"},
		{"add_with_mul_left", "h * 2 + b", "((`h` * 2) + `b`)"},
		{"add_with_mul_right", "b + c * 2", "(`b` + (`c` * 2))"},
		{"with_outer_parens", "(c * 2 + b)", "((`c` * 2) + `b`)"},
		{"with_double_outer_parens", "((c * 2 + b))", "((`c` * 2) + `b`)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mysqlFormatGenExpr(tc.expr, "")
			if got != tc.want {
				t.Errorf("mysqlFormatGenExpr(%q) = %q, want %q", tc.expr, got, tc.want)
			}
		})
	}
}
