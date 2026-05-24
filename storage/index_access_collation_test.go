package storage

import (
	"testing"

	"github.com/myuon/mylite/catalog"
)

func TestScanByIndexOrder_StringCollation(t *testing.T) {
	def := &catalog.TableDef{
		Name:       "t",
		Charset:    "utf8mb4",
		Collation:  "utf8mb4_0900_ai_ci",
		Columns: []catalog.ColumnDef{
			{Name: "a", Type: "INT"},
			{Name: "p", Type: "VARCHAR(20) GENERATED ALWAYS AS (CONCAT(n, h))"},
			{Name: "n", Type: "VARCHAR(10)"},
			{Name: "h", Type: "VARCHAR(10)"},
		},
		Indexes: []catalog.IndexDef{{Name: "idx3", Columns: []string{"p"}}},
	}
	tbl := &Table{
		Def: def,
		Rows: []Row{
			{"a": int64(11), "p": "XXXAAA"},
			{"a": int64(1), "p": "uuuuu"},
			{"a": int64(3), "p": "ummuooo"},
		},
	}
	rows, err := tbl.ScanByIndex(IndexAccessSpec{Type: "index", IndexName: "idx3"})
	if err != nil {
		t.Fatalf("ScanByIndex: %v", err)
	}
	want := []string{"ummuooo", "uuuuu", "XXXAAA"}
	for i, row := range rows {
		if got := row["p"].(string); got != want[i] {
			t.Fatalf("row %d: got %q want %q (full order: %v)", i, got, want[i], rows)
		}
	}
}
