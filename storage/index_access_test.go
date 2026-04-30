package storage

import (
	"testing"

	"github.com/myuon/mylite/catalog"
)

func newIntPKTable() *Table {
	def := &catalog.TableDef{
		Name: "t1",
		Columns: []catalog.ColumnDef{
			{Name: "id", Type: "INT"},
			{Name: "v", Type: "INT"},
		},
		PrimaryKey: []string{"id"},
		Indexes: []catalog.IndexDef{
			{Name: "idx_v", Columns: []string{"v"}},
		},
	}
	t := &Table{Def: def, Rows: []Row{}}
	for i := int64(1); i <= 5; i++ {
		t.Rows = append(t.Rows, Row{"id": i, "v": i * 10})
	}
	t.Rows = append(t.Rows, Row{"id": int64(6), "v": int64(20)}) // duplicate v=20
	return t
}

func TestGetByPK_Hit(t *testing.T) {
	tbl := newIntPKTable()
	row, ok := tbl.GetByPK([]interface{}{int64(3)})
	if !ok {
		t.Fatalf("expected hit on id=3")
	}
	if v, ok := row["v"].(int64); !ok || v != 30 {
		t.Fatalf("expected v=30, got %v (%T)", row["v"], row["v"])
	}
}

func TestGetByPK_Miss(t *testing.T) {
	tbl := newIntPKTable()
	if _, ok := tbl.GetByPK([]interface{}{int64(999)}); ok {
		t.Fatalf("expected miss on id=999")
	}
}

func TestGetByPK_NoPK(t *testing.T) {
	tbl := &Table{Def: &catalog.TableDef{Name: "x", Columns: []catalog.ColumnDef{{Name: "a"}}}}
	if _, ok := tbl.GetByPK([]interface{}{int64(1)}); ok {
		t.Fatalf("expected miss when table has no PK")
	}
}

func TestScanByIndex_ConstPK(t *testing.T) {
	tbl := newIntPKTable()
	rows, err := tbl.ScanByIndex(IndexAccessSpec{
		Type:           "const",
		IndexName:      "PRIMARY",
		EqualityValues: []interface{}{int64(2)},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 1 || rows[0]["id"].(int64) != 2 {
		t.Fatalf("expected single row id=2, got %+v", rows)
	}
}

func TestScanByIndex_RefSecondary(t *testing.T) {
	tbl := newIntPKTable()
	rows, err := tbl.ScanByIndex(IndexAccessSpec{
		Type:           "ref",
		IndexName:      "idx_v",
		EqualityValues: []interface{}{int64(20)},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows for v=20, got %d (%+v)", len(rows), rows)
	}
}

func TestScanByIndex_RangeBetween(t *testing.T) {
	tbl := newIntPKTable()
	rows, err := tbl.ScanByIndex(IndexAccessSpec{
		Type:                "range",
		IndexName:           "PRIMARY",
		RangeLower:          []interface{}{int64(2)},
		RangeUpper:          []interface{}{int64(4)},
		RangeLowerInclusive: true,
		RangeUpperInclusive: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows for id BETWEEN 2 AND 4, got %d (%+v)", len(rows), rows)
	}
}

func TestScanByIndex_RangeIN(t *testing.T) {
	tbl := newIntPKTable()
	rows, err := tbl.ScanByIndex(IndexAccessSpec{
		Type:      "range",
		IndexName: "PRIMARY",
		InValues: [][]interface{}{
			{int64(1)},
			{int64(3)},
			{int64(5)},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows for id IN (1,3,5), got %d", len(rows))
	}
}

func TestScanByIndex_FallbackToScan(t *testing.T) {
	tbl := newIntPKTable()
	rows, err := tbl.ScanByIndex(IndexAccessSpec{Type: "ALL"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != len(tbl.Rows) {
		t.Fatalf("expected %d rows, got %d", len(tbl.Rows), len(rows))
	}
}

func TestScanByIndex_UnknownIndex(t *testing.T) {
	tbl := newIntPKTable()
	rows, err := tbl.ScanByIndex(IndexAccessSpec{
		Type:      "ref",
		IndexName: "no_such_idx",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != len(tbl.Rows) {
		t.Fatalf("expected fallback full scan, got %d rows", len(rows))
	}
}

func TestFindIndex(t *testing.T) {
	tbl := newIntPKTable()
	if idx, ok := tbl.FindIndex("PRIMARY"); !ok || idx.Name != "PRIMARY" || idx.Columns[0] != "id" {
		t.Fatalf("PRIMARY lookup failed: %+v ok=%v", idx, ok)
	}
	if idx, ok := tbl.FindIndex("idx_v"); !ok || idx.Name != "idx_v" {
		t.Fatalf("idx_v lookup failed: %+v ok=%v", idx, ok)
	}
	if _, ok := tbl.FindIndex("ghost"); ok {
		t.Fatalf("expected miss for unknown index")
	}
}

func TestValuesEqualLoose(t *testing.T) {
	cases := []struct {
		a, b interface{}
		want bool
	}{
		{nil, nil, true},
		{nil, int64(0), false},
		{int64(1), int64(1), true},
		{int64(1), "1", true},
		{"foo", "foo", true},
		{"foo", "bar", false},
	}
	for _, c := range cases {
		if got := valuesEqualLoose(c.a, c.b); got != c.want {
			t.Errorf("valuesEqualLoose(%v,%v) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}
