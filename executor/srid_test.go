package executor

import (
	"fmt"
	"testing"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

func TestSRIDBasic(t *testing.T) {
	cat := catalog.New()
	store := storage.NewEngine()
	e := New(cat, store)

	if _, err := e.Execute("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	e.CurrentDB = "test"
	e.sqlMode = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"

	if _, err := e.Execute("CREATE TABLE gis_point_srid (fid INTEGER NOT NULL PRIMARY KEY, g POINT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	if _, err := e.Execute("INSERT INTO gis_point_srid VALUES (101, ST_POINTFROMTEXT('POINT(0 0)'))"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Check value stored
	res, err := e.Execute("SELECT g FROM gis_point_srid WHERE fid=101")
	if err != nil {
		t.Fatalf("select g: %v", err)
	}
	t.Logf("g value before update: cols=%v rows=%v", res.Columns, res.Rows)

	// Apply ST_ASTEXT before update
	res, err = e.Execute("SELECT ST_ASTEXT(g), ST_SRID(g) FROM gis_point_srid WHERE fid=101")
	if err != nil {
		t.Fatalf("select st_astext: %v", err)
	}
	t.Logf("ST_ASTEXT/ST_SRID before update: cols=%v rows=%v", res.Columns, res.Rows)

	// Update
	if _, err := e.Execute("UPDATE gis_point_srid SET g=ST_SRID(g, 4326)"); err != nil {
		t.Fatalf("update: %v", err)
	}

	// Check stored value after update
	res, err = e.Execute("SELECT g FROM gis_point_srid WHERE fid=101")
	if err != nil {
		t.Fatalf("select g after: %v", err)
	}
	t.Logf("g value after update: cols=%v rows=%v", res.Columns, res.Rows)

	// Apply ST_ASTEXT after update
	res, err = e.Execute("SELECT ST_ASTEXT(g), ST_SRID(g) FROM gis_point_srid WHERE fid=101")
	if err != nil {
		t.Fatalf("select st_astext after: %v", err)
	}
	t.Logf("ST_ASTEXT/ST_SRID after update: cols=%v rows=%v", res.Columns, res.Rows)

	fmt.Println("Done")
}
