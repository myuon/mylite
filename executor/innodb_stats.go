package executor

import (
	"fmt"
	"strings"
	"time"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

func isSystemSchemaName(name string) bool {
	switch strings.ToLower(name) {
	case "information_schema", "mysql", "performance_schema", "sys":
		return true
	default:
		return false
	}
}

func statsIndexColName(raw string) string {
	if idx := strings.Index(raw, "("); idx >= 0 {
		return strings.TrimSpace(raw[:idx])
	}
	return strings.TrimSpace(raw)
}

func (e *Executor) innodbStatsPersistentEnabled(def *catalog.TableDef) bool {
	if def != nil && def.StatsPersistent != nil {
		return *def.StatsPersistent != 0
	}
	if v, ok := e.getSysVar("innodb_stats_persistent"); ok && v != "" {
		return v != "0" && !strings.EqualFold(v, "OFF")
	}
	return true
}

func (e *Executor) innodbStatsAutoRecalcEnabled(def *catalog.TableDef) bool {
	if def != nil && def.StatsAutoRecalc != nil {
		return *def.StatsAutoRecalc != 0
	}
	if v, ok := e.getSysVar("innodb_stats_auto_recalc"); ok && v != "" {
		return v != "0" && !strings.EqualFold(v, "OFF")
	}
	return true
}

func (e *Executor) tableRowCount(dbName, tableName string) int64 {
	tbl, err := e.Storage.GetTable(dbName, tableName)
	if err != nil {
		return 0
	}
	tbl.Mu.RLock()
	defer tbl.Mu.RUnlock()
	return int64(len(tbl.Rows))
}

func (e *Executor) hasInnoDBTableStatsRow(dbName, tableName string) bool {
	statsTbl, err := e.Storage.GetTable("mysql", "innodb_table_stats")
	if err != nil {
		return false
	}
	statsTbl.Mu.RLock()
	defer statsTbl.Mu.RUnlock()
	for _, r := range statsTbl.Rows {
		if strings.EqualFold(toString(r["database_name"]), dbName) && strings.EqualFold(toString(r["table_name"]), tableName) {
			return true
		}
	}
	return false
}

func indexDefsForStats(def *catalog.TableDef) []catalog.IndexDef {
	indexDefs := make([]catalog.IndexDef, 0, len(def.Indexes)+1)
	if len(def.PrimaryKey) > 0 {
		indexDefs = append(indexDefs, catalog.IndexDef{Name: "PRIMARY", Columns: append([]string(nil), def.PrimaryKey...)})
	}
	indexDefs = append(indexDefs, def.Indexes...)
	if len(indexDefs) == 0 {
		indexDefs = append(indexDefs, catalog.IndexDef{Name: "GEN_CLUST_INDEX", Columns: []string{"DB_ROW_ID"}})
	}
	return indexDefs
}

func (e *Executor) removeInnoDBStatsRows(dbName, tableName string) {
	statsTbl, err := e.Storage.GetTable("mysql", "innodb_table_stats")
	if err == nil {
		statsTbl.Mu.Lock()
		filtered := make([]storage.Row, 0, len(statsTbl.Rows))
		for _, r := range statsTbl.Rows {
			if strings.EqualFold(toString(r["database_name"]), dbName) && strings.EqualFold(toString(r["table_name"]), tableName) {
				continue
			}
			filtered = append(filtered, r)
		}
		statsTbl.Rows = filtered
		statsTbl.Mu.Unlock()
	}
	idxTbl, err := e.Storage.GetTable("mysql", "innodb_index_stats")
	if err == nil {
		idxTbl.Mu.Lock()
		filtered := make([]storage.Row, 0, len(idxTbl.Rows))
		for _, r := range idxTbl.Rows {
			if strings.EqualFold(toString(r["database_name"]), dbName) && strings.EqualFold(toString(r["table_name"]), tableName) {
				continue
			}
			filtered = append(filtered, r)
		}
		idxTbl.Rows = filtered
		idxTbl.Mu.Unlock()
	}
}

// maybeRecalcStats implements MySQL's InnoDB auto-recalc threshold:
// stats are recomputed only when the cumulative DML change count exceeds
// 10% of the row count at the time stats were last calculated (minimum 200).
func (e *Executor) maybeRecalcStats(dbName, tableName string, changes int64) {
	tbl, err := e.Storage.GetTable(dbName, tableName)
	if err != nil {
		return
	}
	tbl.Mu.Lock()
	tbl.DMLChangesSinceStats += changes
	pending := tbl.DMLChangesSinceStats
	lastCount := tbl.RowCountAtLastStats
	tbl.Mu.Unlock()

	threshold := lastCount / 10
	if threshold < 200 {
		threshold = 200
	}
	if pending < threshold {
		return
	}

	rowCount := e.tableRowCount(dbName, tableName)
	e.upsertInnoDBStatsRows(dbName, tableName, rowCount)

	tbl.Mu.Lock()
	tbl.DMLChangesSinceStats = 0
	tbl.RowCountAtLastStats = rowCount
	tbl.Mu.Unlock()
}

// upsertInnoDBTableStatsOnly inserts only into innodb_table_stats (not innodb_index_stats).
// Used by CREATE TABLE so that SHOW INDEX cardinality returns NULL for freshly created tables
// (no ANALYZE has been run yet). ANALYZE TABLE will call upsertInnoDBStatsRows which also
// inserts index stats, making cardinality show computed values.
func (e *Executor) upsertInnoDBTableStatsOnly(dbName, tableName string, rowCount int64) {
	db, err := e.Catalog.GetDatabase(dbName)
	if err != nil {
		return
	}
	def, err := db.GetTable(tableName)
	if err != nil || def == nil {
		return
	}
	if def.Engine != "" && !strings.EqualFold(def.Engine, "InnoDB") {
		return
	}
	if !e.innodbStatsPersistentEnabled(def) {
		e.removeInnoDBStatsRows(dbName, tableName)
		return
	}

	e.removeInnoDBStatsRows(dbName, tableName)
	lastUpdate := time.Now().UTC().Format("2006-01-02 15:04:05")

	statsTbl, err := e.Storage.GetTable("mysql", "innodb_table_stats")
	if err == nil {
		statsTbl.Mu.Lock()
		statsTbl.Rows = append(statsTbl.Rows, storage.Row{
			"database_name":            dbName,
			"table_name":               tableName,
			"last_update":              lastUpdate,
			"n_rows":                   rowCount,
			"clustered_index_size":     int64(1),
			"sum_of_other_index_sizes": int64(len(def.Indexes)),
		})
		statsTbl.Mu.Unlock()
	}
	// Note: intentionally does NOT insert into innodb_index_stats.
	// Cardinality will show as NULL in SHOW INDEX until ANALYZE TABLE is run.
}

func (e *Executor) upsertInnoDBStatsRowsFromCreate(dbName, tableName string, rowCount int64) {
	e.upsertInnoDBStatsRowsInternal(dbName, tableName, rowCount, true)
}

func (e *Executor) upsertInnoDBStatsRows(dbName, tableName string, rowCount int64) {
	e.upsertInnoDBStatsRowsInternal(dbName, tableName, rowCount, false)
}

func (e *Executor) upsertInnoDBStatsRowsInternal(dbName, tableName string, rowCount int64, notAnalyzed bool) {
	// Skip stats for temporary tables - they don't have persistent stats.
	if e.tempTables != nil && (e.tempTables[tableName] || e.tempTables[strings.ToLower(tableName)]) {
		return
	}
	db, err := e.Catalog.GetDatabase(dbName)
	if err != nil {
		return
	}
	def, err := db.GetTable(tableName)
	if err != nil || def == nil {
		return
	}
	if def.Engine != "" && !strings.EqualFold(def.Engine, "InnoDB") {
		return
	}
	if !e.innodbStatsPersistentEnabled(def) {
		e.removeInnoDBStatsRows(dbName, tableName)
		return
	}

	e.removeInnoDBStatsRows(dbName, tableName)
	lastUpdate := time.Now().UTC().Format("2006-01-02 15:04:05")

	statsTbl, err := e.Storage.GetTable("mysql", "innodb_table_stats")
	if err == nil {
		statsTbl.Mu.Lock()
		statsTbl.Rows = append(statsTbl.Rows, storage.Row{
			"database_name":            dbName,
			"table_name":               tableName,
			"last_update":              lastUpdate,
			"n_rows":                   rowCount,
			"clustered_index_size":     int64(1),
			"sum_of_other_index_sizes": int64(len(def.Indexes)),
		})
		statsTbl.Mu.Unlock()
	}

	// Load table rows for distinct count computation
	var tableRows []storage.Row
	if tbl, err := e.Storage.GetTable(dbName, tableName); err == nil {
		tbl.Mu.RLock()
		tableRows = tbl.Rows
		tbl.Mu.RUnlock()
	}

	idxTbl, err := e.Storage.GetTable("mysql", "innodb_index_stats")
	if err != nil {
		return
	}
	idxTbl.Mu.Lock()
	for _, idx := range indexDefsForStats(def) {
		indexName := idx.Name
		if indexName == "" {
			indexName = "PRIMARY"
		}
		statCols := make([]string, 0, len(idx.Columns)+1)
		statCols = append(statCols, idx.Columns...)
		if !strings.EqualFold(indexName, "PRIMARY") && !strings.EqualFold(indexName, "GEN_CLUST_INDEX") {
			if len(def.PrimaryKey) > 0 {
				statCols = append(statCols, def.PrimaryKey...)
			} else {
				statCols = append(statCols, "DB_ROW_ID")
			}
		}
		sampleSize := rowCount
		if sampleSize < 1 {
			sampleSize = 1
		}
		for i := range statCols {
			statName := fmt.Sprintf("n_diff_pfx%02d", i+1)
			descCols := make([]string, 0, i+1)
			for j := 0; j <= i; j++ {
				descCols = append(descCols, statsIndexColName(statCols[j]))
			}
			statValue := computeDistinctCount(tableRows, statCols[:i+1])
			// When notAnalyzed=true (CREATE TABLE), use sample_size=0 as a sentinel
			// meaning "stats not yet computed by ANALYZE". This makes SHOW INDEX
			// display NULL for cardinality (matching MySQL behavior for fresh tables).
			ndiffSampleSize := interface{}(int64(1))
			if notAnalyzed {
				ndiffSampleSize = int64(0)
			}
			idxTbl.Rows = append(idxTbl.Rows, storage.Row{
				"database_name":    dbName,
				"table_name":       tableName,
				"index_name":       indexName,
				"last_update":      lastUpdate,
				"stat_name":        statName,
				"stat_value":       statValue,
				"sample_size":      ndiffSampleSize,
				"stat_description": strings.Join(descCols, ","),
			})
		}
		idxTbl.Rows = append(idxTbl.Rows,
			storage.Row{
				"database_name":    dbName,
				"table_name":       tableName,
				"index_name":       indexName,
				"last_update":      lastUpdate,
				"stat_name":        "n_leaf_pages",
				"stat_value":       int64(1),
				"sample_size":      nil,
				"stat_description": "Number of leaf pages in the index",
			},
			storage.Row{
				"database_name":    dbName,
				"table_name":       tableName,
				"index_name":       indexName,
				"last_update":      lastUpdate,
				"stat_name":        "size",
				"stat_value":       int64(1),
				"sample_size":      nil,
				"stat_description": "Number of pages in the index",
			},
		)
	}
	idxTbl.Mu.Unlock()
}

// computeDistinctCount counts the number of distinct value combinations for the given
// column prefix across table rows. For DB_ROW_ID (implicit row ID), each row is unique.
func computeDistinctCount(rows []storage.Row, cols []string) int64 {
	if len(rows) == 0 {
		return int64(0)
	}
	if len(cols) > 0 && cols[len(cols)-1] == "DB_ROW_ID" {
		return int64(len(rows))
	}
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		var key strings.Builder
		for ci, col := range cols {
			if ci > 0 {
				key.WriteByte(0)
			}
			var val interface{}
			for k, v := range row {
				if strings.EqualFold(k, col) {
					val = v
					break
				}
			}
			fmt.Fprintf(&key, "%v", val)
		}
		seen[key.String()] = struct{}{}
	}
	return int64(len(seen))
}

func (e *Executor) refreshInnoDBStatsTables() {
	if e.Catalog == nil || e.Storage == nil {
		return
	}
	mysqlDB, err := e.Catalog.GetDatabase("mysql")
	if err != nil {
		return
	}
	tblStats, err := e.Storage.GetTable("mysql", "innodb_table_stats")
	if err != nil {
		return
	}
	idxStats, err := e.Storage.GetTable("mysql", "innodb_index_stats")
	if err != nil {
		return
	}

	lastUpdate := time.Now().UTC().Format("2006-01-02 15:04:05")
	tableRows := make([]storage.Row, 0, 256)
	indexRows := make([]storage.Row, 0, 1024)

	for dbName, db := range e.Catalog.Databases {
		if isSystemSchemaName(dbName) {
			continue
		}
		for tableName, def := range db.Tables {
			if def == nil {
				continue
			}
			if def.Engine != "" && !strings.EqualFold(def.Engine, "InnoDB") {
				continue
			}
			if !e.innodbStatsPersistentEnabled(def) {
				continue
			}
			var rowCount int64
			if t, err := e.Storage.GetTable(dbName, tableName); err == nil {
				t.Mu.RLock()
				rowCount = int64(len(t.Rows))
				t.Mu.RUnlock()
			}
			tableRows = append(tableRows, storage.Row{
				"database_name":            dbName,
				"table_name":               tableName,
				"last_update":              lastUpdate,
				"n_rows":                   rowCount,
				"clustered_index_size":     int64(1),
				"sum_of_other_index_sizes": int64(len(def.Indexes)),
			})

			indexDefs := make([]catalog.IndexDef, 0, len(def.Indexes)+1)
			if len(def.PrimaryKey) > 0 {
				indexDefs = append(indexDefs, catalog.IndexDef{Name: "PRIMARY", Columns: append([]string(nil), def.PrimaryKey...)})
			}
			indexDefs = append(indexDefs, def.Indexes...)
			if len(indexDefs) == 0 {
				indexDefs = append(indexDefs, catalog.IndexDef{Name: "GEN_CLUST_INDEX", Columns: []string{"DB_ROW_ID"}})
			}
			for _, idx := range indexDefs {
				indexName := idx.Name
				if indexName == "" {
					indexName = "PRIMARY"
				}
				firstCol := "id"
				if len(idx.Columns) > 0 {
					firstCol = statsIndexColName(idx.Columns[0])
				}
				indexRows = append(indexRows,
					storage.Row{
						"database_name":    dbName,
						"table_name":       tableName,
						"index_name":       indexName,
						"last_update":      lastUpdate,
						"stat_name":        "n_diff_pfx01",
						"stat_value":       rowCount,
						"sample_size":      rowCount,
						"stat_description": firstCol,
					},
					storage.Row{
						"database_name":    dbName,
						"table_name":       tableName,
						"index_name":       indexName,
						"last_update":      lastUpdate,
						"stat_name":        "n_leaf_pages",
						"stat_value":       int64(1),
						"sample_size":      nil,
						"stat_description": "Number of leaf pages in the index",
					},
					storage.Row{
						"database_name":    dbName,
						"table_name":       tableName,
						"index_name":       indexName,
						"last_update":      lastUpdate,
						"stat_name":        "size",
						"stat_value":       int64(1),
						"sample_size":      nil,
						"stat_description": "Number of pages in the index",
					},
				)
			}
		}
	}

	// Keep table definition existence checked to avoid writing into stale tables.
	if _, err := mysqlDB.GetTable("innodb_table_stats"); err == nil {
		tblStats.Mu.Lock()
		tblStats.Rows = tableRows
		tblStats.Mu.Unlock()
	}
	if _, err := mysqlDB.GetTable("innodb_index_stats"); err == nil {
		idxStats.Mu.Lock()
		idxStats.Rows = indexRows
		idxStats.Mu.Unlock()
	}
}
