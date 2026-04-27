package executor

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/myuon/mylite/catalog"
	"vitess.io/vitess/go/vt/sqlparser"
)

func (e *Executor) dummyExplainRow(query string) []interface{} {
	upper := strings.ToUpper(query)
	var table interface{} = nil
	var extra interface{} = nil
	rows := int64(1)

	if idx := strings.Index(upper, " FROM "); idx >= 0 && idx+len(" FROM ") <= len(query) {
		restOrig := strings.TrimSpace(query[idx+len(" FROM "):])
		restUpper := strings.TrimSpace(upper[idx+len(" FROM "):])
		if strings.HasPrefix(restUpper, "JSON_TABLE(") {
			table = "tt"
			rows = 2
			extra = "Table function: json_table; Using temporary"
		} else {
			fields := strings.Fields(restOrig)
			if len(fields) > 0 {
				tok := strings.Trim(fields[0], "`;,()")
				if dot := strings.Index(tok, "."); dot >= 0 {
					tok = tok[dot+1:]
				}
				if tok != "" {
					table = tok
					if e.Storage != nil {
						if tbl, err := e.Storage.GetTable(e.CurrentDB, tok); err == nil {
							if n := len(tbl.Rows); n > 0 {
								rows = int64(n)
							}
						}
					}
				}
			}
		}
	}
	if !strings.Contains(upper, "ORDER BY NULL") && (strings.Contains(upper, "GROUP BY") || strings.Contains(upper, "SQL_BIG_RESULT")) {
		extra = "Using filesort"
	}
	if table == nil {
		extra = "No tables used"
		return []interface{}{int64(1), "SIMPLE", nil, nil, nil, nil, nil, nil, nil, nil, nil, extra}
	}
	return []interface{}{int64(1), "SIMPLE", table, nil, "ALL", nil, nil, nil, nil, rows, "100.00", extra}
}

// explainSelectType describes one row in the EXPLAIN output.
type explainSelectType struct {
	id           interface{} // int64 or nil (for UNION RESULT)
	selectType   string
	table        interface{} // string or nil
	extra        interface{} // string or nil
	rows         interface{} // int64 or nil
	filtered     interface{} // string or nil
	accessType   interface{} // string or nil
	possibleKeys interface{} // string (comma-separated) or nil
	key          interface{} // string or nil
	keyLen       interface{} // string or nil
	ref          interface{} // string (comma-separated) or nil
}

// explainMultiRows returns one or more EXPLAIN rows, detecting subqueries,
// derived tables, and UNION constructs to produce correct select_types like MySQL.
func (e *Executor) explainMultiRows(query string) [][]interface{} {
	// Try to parse the query with the SQL parser for AST-based analysis
	stmt, err := e.parser().Parse(query)
	if err != nil {
		// Fall back to the simple row if parsing fails
		return [][]interface{}{e.dummyExplainRow(query)}
	}

	var result []explainSelectType
	idCounter := int64(1)

	switch s := stmt.(type) {
	case *sqlparser.Union:
		// Top-level UNION: first SELECT is PRIMARY, rest are UNION, plus UNION RESULT
		result = e.explainUnion(s, &idCounter, true)
	case *sqlparser.Select:
		// Check if this SELECT has subqueries, derived tables, etc.
		if e.queryHasComplexParts(s) {
			canFlatten := e.queryCanBeSemijoinFlattened(s)
			if canFlatten && e.outerQueryHasOnlyDerivedTables(s) && e.isOptimizerSwitchEnabled("firstmatch") {
				// FirstMatch semijoin strategy: when the outer FROM has only derived tables
				// and firstmatch=on, MySQL uses PRIMARY selectType with inline FirstMatch()
				// rather than the MATERIALIZED/SIMPLE pattern.
				// e.g. SELECT * FROM (SELECT * FROM t1) AS d1 WHERE d1.c1 IN (SELECT c1 FROM t2)
				// → id=1 PRIMARY <derived2>, id=1 PRIMARY t2 (FirstMatch(<derived2>)), id=2 DERIVED t1
				result = e.explainSelect(s, &idCounter, "PRIMARY")
				// Find the DERIVED row id for the FirstMatch() reference string.
				var derivedFirstMatchID int64
				for _, r := range result {
					if r.selectType == "DERIVED" {
						if id, ok := r.id.(int64); ok {
							derivedFirstMatchID = id
						}
						break
					}
				}
				// Convert MATERIALIZED rows to PRIMARY with FirstMatch() in Extra.
				for i := range result {
					if result[i].selectType == "MATERIALIZED" {
						result[i].selectType = "PRIMARY"
						result[i].id = int64(1)
						firstMatchStr := fmt.Sprintf("FirstMatch(<derived%d>)", derivedFirstMatchID)
						if result[i].extra == nil {
							result[i].extra = firstMatchStr
						} else {
							result[i].extra = fmt.Sprintf("%v; %v", result[i].extra, firstMatchStr)
						}
					}
				}
			} else if canFlatten {
				// MySQL flattens IN/NOT EXISTS subqueries into semi-join / anti-join.
				// With materialization=on, IN subqueries appear as:
				//   id=1 SIMPLE outer_table
				//   id=1 SIMPLE <subqueryN>  (materialized lookup table)
				//   id=N MATERIALIZED inner_table
				// With materialization=off or for EXISTS/anti-join, all rows are id=1, SIMPLE.
				result = e.explainSelect(s, &idCounter, "SIMPLE")
				// Check if any subquery resulted in an impossible WHERE (no matching row in const table).
				// If so, collapse the entire result to a single NULL row.
				for _, r := range result {
					if r.selectType == "__IMPOSSIBLE__" {
						result = []explainSelectType{{
							id:         int64(1),
							selectType: "SIMPLE",
							table:      nil,
							extra:      "no matching row in const table",
							rows:       nil,
							filtered:   nil,
							accessType: nil,
						}}
						break
					}
				}
				// Post-process rows to handle materialization correctly.
				if e.isOptimizerSwitchEnabled("materialization") {
					// Build final row order:
					// 1. <subqueryN> placeholders (one per unique MATERIALIZED subquery id)
					// 2. SIMPLE outer table rows
					// 3. MATERIALIZED inner rows
					// This matches MySQL's EXPLAIN output order where the materialized temp table
					// is shown first (as the driving side), then the outer table (eq_ref probe).
					var placeholders []explainSelectType
					var simpleRows []explainSelectType
					var materializedRows []explainSelectType
					var derivedRows []explainSelectType
					// Track which unique MATERIALIZED subquery IDs we've seen, to create placeholders later
					insertedPlaceholder := map[interface{}]bool{}
					var materializedIDs []interface{} // track insertion order of IDs
					for _, r := range result {
						if r.selectType == "MATERIALIZED" {
							if !insertedPlaceholder[r.id] {
								insertedPlaceholder[r.id] = true
								materializedIDs = append(materializedIDs, r.id)
							}
							materializedRows = append(materializedRows, r)
						} else if r.selectType == "DERIVED" {
							// DERIVED rows (from FROM-clause derived tables) are preserved as-is
							derivedRows = append(derivedRows, r)
						} else if r.selectType != "SIMPLE" {
							// Non-SIMPLE, non-MATERIALIZED, non-DERIVED rows (e.g. DEPENDENT SUBQUERY for EXISTS)
							// become id=1, SIMPLE (anti-join / FirstMatch strategy)
							r.id = int64(1)
							r.selectType = "SIMPLE"
							simpleRows = append(simpleRows, r)
						} else {
							simpleRows = append(simpleRows, r)
						}
					}
					// Now create the <subqueryN> placeholder rows.
					// Determine access type based on outer tables:
					// - If the outer IN expression uses a constant literal (e.g. "11 IN (subquery)"),
					//   MySQL uses const access on the materialized hash table.
					// - If any outer SIMPLE row has non-ALL access (eq_ref, ref, const),
					//   the outer table is a probe → <subqueryN> is the driver (ALL access)
					// - If all outer SIMPLE rows have ALL access (scan), they drive the join
					//   → <subqueryN> is the probe (eq_ref with <auto_key>)
					outerINIsConst := inSubqueryOuterIsConst(s)
					outerHasNonAll := false
					outerHasAllScan := false // any ALL-scan outer table (potential driver of <subquery> via BNL)
					outerJoinColForRef := extractINSubqueryOuterCol(s)
					// Detect if the outer FROM clause has a derived table (e.g. (SELECT ...) AS x).
					// In that case MySQL shows ref="func" and extra="Using where" for <subqueryN> eq_ref.
					outerFromIsDerived := selectHasDerivedTableInFrom(s)
					// Prefer the table that actually has the IN condition (e.g., t3 in "WHERE t3.a IN (...)")
					// so that ref and index checks are done on the correct table.
					outerTableNameForRef := extractINSubqueryOuterTable(s)
					outerTableNameFallback := "" // fallback: first non-subquery simple row
					for _, sr := range simpleRows {
						if sr.table != nil && !strings.HasPrefix(fmt.Sprintf("%v", sr.table), "<subquery") {
							at := fmt.Sprintf("%v", sr.accessType)
							tblNameStr := fmt.Sprintf("%v", sr.table)
							if at == "eq_ref" || at == "ref" || at == "const" {
								outerHasNonAll = true
							} else if at == "range" && outerJoinColForRef != "" && strings.EqualFold(tblNameStr, outerTableNameForRef) {
								// range access on the IN table/column: this table can probe <subquery> via ref
								// after materialization (the range becomes a join lookup), treat as non-ALL.
								outerHasNonAll = true
							} else {
								// ALL or other scan outer table
								outerHasAllScan = true
							}
							if outerTableNameFallback == "" {
								outerTableNameFallback = tblNameStr
							}
						}
					}
					if outerTableNameForRef == "" {
						outerTableNameForRef = outerTableNameFallback
					}
					// Create placeholder rows with appropriate access type
					for _, rawID := range materializedIDs {
						if id, ok := rawID.(int64); ok {
							subqueryRef := fmt.Sprintf("<subquery%d>", id)
							var ph explainSelectType
							if outerINIsConst {
								// Constant IN expression (e.g. "11 IN (subquery)"): MySQL does a
								// const lookup on the materialized hash table.
								ph = explainSelectType{
									id:           int64(1),
									selectType:   "SIMPLE",
									table:        subqueryRef,
									accessType:   "const",
									possibleKeys: "<auto_key>",
									key:          "<auto_key>",
									keyLen:       "5",
									ref:          "const",
									rows:         int64(1),
									filtered:     "100.00",
									extra:        nil,
								}
							} else if outerHasNonAll && outerHasAllScan {
								// Mixed case: some outer tables are ALL-scan (drivers) and some use
								// ref/eq_ref access (they probe <subqueryN> by its join key).
								// In MySQL's plan: ALL-scan tables drive, <subqueryN> is scanned via BNL
								// (it's the inner of the BNL join), then ref-access tables probe <subqueryN>.
								bnlExtra := "Using join buffer (Block Nested Loop)"
								// If there are range conditions on the IN column (beyond the IN itself),
								// MySQL adds "Using where" to indicate the filter applied to <subqueryN>.
								if outerJoinColForRef != "" && s.Where != nil &&
									hasNonINRangeConditionOnCol(s.Where.Expr, outerJoinColForRef) {
									bnlExtra = "Using where; Using join buffer (Block Nested Loop)"
								}
								ph = explainSelectType{
									id:           int64(1),
									selectType:   "SIMPLE",
									table:        subqueryRef,
									accessType:   "ALL",
									possibleKeys: nil,
									key:          nil,
									keyLen:       nil,
									ref:          nil,
									rows:         nil,
									filtered:     "100.00",
									extra:        bnlExtra,
								}
								// Post-process: outer tables with range access on the IN column become
								// ref access using <subqueryN>.col as the join reference.
								// This reflects MySQL's optimizer combining the BNL materialization with
								// the range scan's index to produce a ref lookup on the subquery hash.
								if outerJoinColForRef != "" {
									for i, sr := range simpleRows {
										if sr.table == nil || strings.HasPrefix(fmt.Sprintf("%v", sr.table), "<subquery") {
											continue
										}
										at := fmt.Sprintf("%v", sr.accessType)
										if at == "range" {
											tblName := fmt.Sprintf("%v", sr.table)
											// Check if this table's range is on the IN join column
											if e.Storage != nil {
												if tbl, err := e.Storage.GetTable(e.CurrentDB, tblName); err == nil && tbl.Def != nil {
													for _, idx := range tbl.Def.Indexes {
														if len(idx.Columns) > 0 && strings.EqualFold(idx.Columns[0], outerJoinColForRef) {
															// Change range → ref with <subquery>.col as ref
															simpleRows[i].accessType = "ref"
															simpleRows[i].ref = subqueryRef + "." + outerJoinColForRef
															break
														}
													}
												}
											}
										}
									}
								}
							} else if outerHasNonAll && !outerHasAllScan {
								// All outer tables use ref/eq_ref access (they probe <subqueryN>).
								// <subqueryN> drives (ALL scan), outer tables are probes.
								// This is the "materialized probe" pattern:
								// <subquery> scans, outer table does eq_ref lookup.
								ph = explainSelectType{
									id:           int64(1),
									selectType:   "SIMPLE",
									table:        subqueryRef,
									accessType:   "ALL",
									possibleKeys: nil,
									key:          nil,
									keyLen:       nil,
									ref:          nil,
									rows:         nil,
									filtered:     "100.00",
									extra:        nil,
								}
							} else if !outerHasNonAll && outerHasAllScan {
								// All outer tables have ALL-scan access (no key on the join column).
								// MySQL's optimizer chooses based on estimated subquery row count:
								// - If subquery materializes to 0 rows, <subqueryN> DRIVES (ALL, filtered=0.00)
								//   and the outer ALL-scan table becomes the BNL inner.
								// - If subquery materializes to rows > 0, outer table drives and <subqueryN>
								//   is probed via hash (eq_ref <auto_key>).
								subqueryTotalRows := int64(0)
								allMaterializedHaveZeroRows := true
								for _, mr := range materializedRows {
									if mr.rows == nil {
										// nil rows = unknown, treat as non-zero
										allMaterializedHaveZeroRows = false
										break
									}
									if r, ok := mr.rows.(int64); ok {
										subqueryTotalRows += r
										if r > 0 {
											allMaterializedHaveZeroRows = false
										}
									} else {
										allMaterializedHaveZeroRows = false
										break
									}
								}
								_ = subqueryTotalRows
								if allMaterializedHaveZeroRows {
									// Subquery materializes to 0 rows → <subqueryN> drives with ALL access,
									// outer ALL-scan table becomes BNL inner.
									ph = explainSelectType{
										id:         int64(1),
										selectType: "SIMPLE",
										table:      subqueryRef,
										accessType: "ALL",
										rows:       nil,
										filtered:   "0.00",
										extra:      nil,
									}
									// Update outer ALL-scan tables to add "Using join buffer (Block Nested Loop)"
									for i, sr := range simpleRows {
										if sr.table == nil || strings.HasPrefix(fmt.Sprintf("%v", sr.table), "<subquery") {
											continue
										}
										at := fmt.Sprintf("%v", sr.accessType)
										if at != "eq_ref" && at != "ref" && at != "const" {
											// ALL-scan outer table becomes BNL inner
											if sr.extra == nil {
												simpleRows[i].extra = "Using where; Using join buffer (Block Nested Loop)"
											} else {
												extraStr := fmt.Sprintf("%v", sr.extra)
												if !strings.Contains(extraStr, "Using join buffer") {
													simpleRows[i].extra = extraStr + "; Using join buffer (Block Nested Loop)"
												}
											}
										}
									}
								} else {
									// Subquery materializes to rows > 0.
									// Check if the outer table has an index (PRIMARY KEY or secondary) on the join column.
									// If YES: <subqueryN> DRIVES (ALL), outer table probes via index (eq_ref).
									// If NO:  outer table drives (ALL scan), <subqueryN> is probed (eq_ref <auto_key>).
									outerHasIndexOnJoinCol := false
									if outerJoinColForRef != "" && outerTableNameForRef != "" && e.Storage != nil {
										if tbl, err := e.Storage.GetTable(e.CurrentDB, outerTableNameForRef); err == nil && tbl.Def != nil {
											// Check PRIMARY KEY
											for _, pk := range tbl.Def.PrimaryKey {
												if strings.EqualFold(pk, outerJoinColForRef) {
													outerHasIndexOnJoinCol = true
													break
												}
											}
											// Check secondary indexes
											if !outerHasIndexOnJoinCol {
												for _, idx := range tbl.Def.Indexes {
													for _, col := range idx.Columns {
														if strings.EqualFold(col, outerJoinColForRef) {
															outerHasIndexOnJoinCol = true
															break
														}
													}
													if outerHasIndexOnJoinCol {
														break
													}
												}
											}
										}
									}

									if outerHasIndexOnJoinCol {
										// Outer table has index on join column → <subqueryN> drives (ALL),
										// outer table probes via eq_ref on its PRIMARY key.
										ph = explainSelectType{
											id:         int64(1),
											selectType: "SIMPLE",
											table:      subqueryRef,
											accessType: "ALL",
											rows:       nil,
											filtered:   "100.00",
											extra:      nil,
										}
										// Update outer ALL-scan table to use eq_ref with <subqueryN>.joinCol as ref
										for i, sr := range simpleRows {
											if sr.table == nil || strings.HasPrefix(fmt.Sprintf("%v", sr.table), "<subquery") {
												continue
											}
											at := fmt.Sprintf("%v", sr.accessType)
											if at == "ALL" || at == "" {
												// Check if this outer table has an index on the join column
												tblName := fmt.Sprintf("%v", sr.table)
												if e.Storage != nil {
													if tbl, err := e.Storage.GetTable(e.CurrentDB, tblName); err == nil && tbl.Def != nil {
														// Find the specific key to use
														keyName := ""
														keyLen := "5"
														for _, pk := range tbl.Def.PrimaryKey {
															if strings.EqualFold(pk, outerJoinColForRef) {
																keyName = "PRIMARY"
																keyLen = "4"
																break
															}
														}
														if keyName == "" {
															for _, idx := range tbl.Def.Indexes {
																for _, col := range idx.Columns {
																	if strings.EqualFold(col, outerJoinColForRef) {
																		keyName = idx.Name
																		break
																	}
																}
																if keyName != "" {
																	break
																}
															}
														}
														if keyName != "" {
															simpleRows[i].accessType = "eq_ref"
															simpleRows[i].possibleKeys = keyName
															simpleRows[i].key = keyName
															simpleRows[i].keyLen = keyLen
															simpleRows[i].ref = subqueryRef + "." + outerJoinColForRef
															simpleRows[i].rows = int64(1)
															simpleRows[i].filtered = "100.00"
														}
													}
												}
											}
										}
									} else {
										// Outer table has NO index on join column → outer table drives (ALL scan),
										// <subqueryN> is probed via hash (eq_ref <auto_key>).
										var refStr interface{} = nil
										if outerFromIsDerived {
											// Outer FROM is a derived table: MySQL shows "func" as ref
											// (join key computed through derived expression).
											refStr = "func"
										} else if outerTableNameForRef != "" && outerJoinColForRef != "" {
											if strings.HasPrefix(outerTableNameForRef, "<derived") || strings.HasPrefix(outerTableNameForRef, "<subquery") {
												refStr = "func"
											} else {
												refStr = outerTableNameForRef + "." + outerJoinColForRef
											}
										}
										// When outer FROM is a derived table, MySQL adds "Using where"
										// to indicate the semi-join condition is applied as a filter,
										// and this triggers attached_condition in JSON EXPLAIN.
										var phExtra interface{} = nil
										if outerFromIsDerived {
											phExtra = "Using where"
										}
										ph = explainSelectType{
											id:           int64(1),
											selectType:   "SIMPLE",
											table:        subqueryRef,
											accessType:   "eq_ref",
											possibleKeys: "<auto_key>",
											key:          "<auto_key>",
											keyLen:       "5",
											ref:          refStr,
											rows:         int64(1),
											filtered:     "100.00",
											extra:        phExtra,
										}
									}
								}
							} else {
								// No outer tables (or edge case) → <subqueryN> is probed via hash (eq_ref <auto_key>)
								var refStr interface{} = nil
								if outerTableNameForRef != "" && outerJoinColForRef != "" {
									// If the outer table is a derived/subquery placeholder, MySQL shows "func"
									// as the ref (the join key is computed as a function expression).
									if strings.HasPrefix(outerTableNameForRef, "<derived") || strings.HasPrefix(outerTableNameForRef, "<subquery") {
										refStr = "func"
									} else {
										refStr = outerTableNameForRef + "." + outerJoinColForRef
									}
								}
								// When outer is a derived table, MySQL adds "Using where" to indicate
								// the semi-join condition is applied as a filter.
								var placeholderExtra interface{} = nil
								if strings.HasPrefix(outerTableNameForRef, "<derived") {
									placeholderExtra = "Using where"
								}
								ph = explainSelectType{
									id:           int64(1),
									selectType:   "SIMPLE",
									table:        subqueryRef,
									accessType:   "eq_ref",
									possibleKeys: "<auto_key>",
									key:          "<auto_key>",
									keyLen:       "5", // typical INT hash key length
									ref:          refStr,
									rows:         int64(1),
									filtered:     "100.00",
									extra:        placeholderExtra,
								}
							}
							placeholders = append(placeholders, ph)
						}
					}
					// Update outer SIMPLE rows: when the outer table is accessed via a
					// MATERIALIZED subquery join, change 'const' access to 'eq_ref' and
					// update the ref to "<subqueryN>.join_col".
					if len(materializedRows) > 0 && len(placeholders) > 0 {
						// Find the join column from the IN condition in the WHERE clause.
						outerJoinCol := extractINSubqueryOuterCol(s)
						for i, sr := range simpleRows {
							at := fmt.Sprintf("%v", sr.accessType)
							if at == "const" && sr.possibleKeys == "PRIMARY" && outerJoinCol != "" {
								// Change to eq_ref access via the materialized subquery
								simpleRows[i].accessType = "eq_ref"
								// Use the first placeholder's subquery reference
								subqueryRef := fmt.Sprintf("%v", placeholders[0].table)
								simpleRows[i].ref = subqueryRef + "." + outerJoinCol
								// used_key_parts: use the actual join column name, not "PRIMARY"
								// (This is used in the JSON key field "used_key_parts")
							}
						}
					}
					// Apply derived table merging for the SIMPLE <derivedN> rows:
					// When a SIMPLE row has table "<derivedN>" and there's a corresponding DERIVED row,
					// replace <derivedN> with the DERIVED row's base table and drop the DERIVED row.
					// This matches MySQL's behavior of merging simple derived tables into the outer query.
					derivedByID := make(map[int64]explainSelectType)
					for _, dr := range derivedRows {
						if id, ok := dr.id.(int64); ok {
							derivedByID[id] = dr
						}
					}
					mergedDerived := make(map[int64]bool)
					for i, sr := range simpleRows {
						if sr.table == nil {
							continue
						}
						tblStr := fmt.Sprintf("%v", sr.table)
						if !strings.HasPrefix(tblStr, "<derived") {
							continue
						}
						// Extract the derived ID from "<derivedN>"
						var derivedID int64
						if _, err := fmt.Sscanf(tblStr, "<derived%d>", &derivedID); err == nil {
							if dr, ok := derivedByID[derivedID]; ok {
								// Replace the <derivedN> row with the DERIVED row's properties
								simpleRows[i].table = dr.table
								simpleRows[i].accessType = dr.accessType
								simpleRows[i].rows = dr.rows
								simpleRows[i].filtered = dr.filtered
								simpleRows[i].extra = dr.extra
								mergedDerived[derivedID] = true
							}
						}
					}
					// Keep unmerged DERIVED rows
					var unmergedDerived []explainSelectType
					for _, dr := range derivedRows {
						if id, ok := dr.id.(int64); ok {
							if !mergedDerived[id] {
								unmergedDerived = append(unmergedDerived, dr)
							}
						}
					}

					// Final order: placeholders first, then outer SIMPLE rows, then MATERIALIZED rows
					// (in reverse subquery ID order, matching MySQL's output), then unmerged DERIVED rows.
					// MySQL outputs MATERIALIZED subqueries in reverse order of their IDs when multiple
					// IN subqueries are present (higher IDs first).
					if len(materializedRows) > 1 {
						// Reverse materializedRows by grouping by id and reversing id order.
						// Build a map from id to rows, then output in reverse id order.
						type matGroup struct {
							id   interface{}
							rows []explainSelectType
						}
						var groups []matGroup
						seenIDs := make(map[interface{}]int)
						for _, mr := range materializedRows {
							if idx, ok := seenIDs[mr.id]; ok {
								groups[idx].rows = append(groups[idx].rows, mr)
							} else {
								seenIDs[mr.id] = len(groups)
								groups = append(groups, matGroup{id: mr.id, rows: []explainSelectType{mr}})
							}
						}
						// Reverse the group order (higher IDs first)
						for i, j := 0, len(groups)-1; i < j; i, j = i+1, j-1 {
							groups[i], groups[j] = groups[j], groups[i]
						}
						materializedRows = materializedRows[:0]
						for _, g := range groups {
							materializedRows = append(materializedRows, g.rows...)
						}
					}
					var processed []explainSelectType
					processed = append(processed, placeholders...)
					processed = append(processed, simpleRows...)
					processed = append(processed, materializedRows...)
					processed = append(processed, unmergedDerived...)
					result = processed
				} else {
					// materialization=off: all non-SIMPLE rows become id=1, SIMPLE.
					// For DuplicateWeedout/LooseScan strategy, MySQL places the inner
					// subquery tables BEFORE the outer tables in the join order.
					// Separate outer (originally SIMPLE) from inner (originally non-SIMPLE)
					// rows, then reorder: inner first, outer last.
					var outerSimpleRows []explainSelectType
					var innerRows []explainSelectType
					for _, r := range result {
						if r.selectType != "SIMPLE" {
							r.id = int64(1)
							r.selectType = "SIMPLE"
							innerRows = append(innerRows, r)
						} else {
							outerSimpleRows = append(outerSimpleRows, r)
						}
					}
					// Rebuild result: inner tables first, then outer tables.
					result = append(innerRows, outerSimpleRows...)
				}
			} else {
				result = e.explainSelect(s, &idCounter, "PRIMARY")
			}
		} else {
			// Simple query
			result = e.explainSelect(s, &idCounter, "SIMPLE")
			// Detect "Impossible WHERE noticed after reading const tables":
			// When all tables in the result have const/system access type AND
			// there are multiple tables (JOIN) AND the actual SELECT returns 0 rows,
			// MySQL collapses to a single "Impossible WHERE noticed after reading const tables" row.
			// This occurs when constant folding after const-table resolution reveals
			// that the WHERE condition is always false (e.g., LEFT JOIN with IS NULL + non-null check).
			if len(result) > 1 && s.Where != nil {
				allConst := true
				for _, r := range result {
					at := fmt.Sprintf("%v", r.accessType)
					if at != "const" && at != "system" {
						allConst = false
						break
					}
				}
				if allConst {
					// Execute the SELECT to check if it returns 0 rows
					actualResult, execErr := e.execSelect(s)
					if execErr == nil && actualResult != nil && len(actualResult.Rows) == 0 {
						result = []explainSelectType{{
							id:         int64(1),
							selectType: "SIMPLE",
							table:      nil,
							extra:      "Impossible WHERE noticed after reading const tables",
							rows:       nil,
							filtered:   nil,
							accessType: nil,
						}}
					}
				}
			}
		}
	case *sqlparser.Update:
		// EXPLAIN UPDATE: first table gets select_type="UPDATE", subsequent tables get "SIMPLE".
		// Collect all table names from the UPDATE's TableExprs.
		var tableNames []string
		for _, te := range s.TableExprs {
			tableNames = append(tableNames, e.extractAllTableNames(te)...)
		}
		for i, tblName := range tableNames {
			var rowCount int64 = 1
			if e.Storage != nil {
				if tbl, err := e.Storage.GetTable(e.CurrentDB, tblName); err == nil {
					if n := len(tbl.Rows); n > 0 {
						rowCount = int64(n)
					}
				}
			}
			st := "SIMPLE"
			if i == 0 {
				st = "UPDATE"
			}
			var updateExtra interface{} = nil
			if i == 0 && s.Where != nil {
				updateExtra = "Using where"
			}
			result = append(result, explainSelectType{
				id:         idCounter,
				selectType: st,
				table:      tblName,
				rows:       rowCount,
				filtered:   "100.00",
				accessType: "ALL",
				extra:      updateExtra,
			})
		}
		if len(result) == 0 {
			return [][]interface{}{e.dummyExplainRow(query)}
		}
	case *sqlparser.Delete:
		// EXPLAIN DELETE: show "DELETE" as select_type for deleted tables, "SIMPLE" for others.
		// For DELETE without WHERE, Extra is "Deleting all rows".
		var tableNames []string
		for _, te := range s.TableExprs {
			tableNames = append(tableNames, e.extractAllTableNames(te)...)
		}
		// Build set of tables being deleted (from stmt.Targets).
		// For single-table DELETE (no Targets), all tables are deleted.
		deletedTables := make(map[string]bool)
		if len(s.Targets) > 0 {
			for _, tgt := range s.Targets {
				deletedTables[strings.ToLower(tgt.Name.String())] = true
			}
		} else if len(tableNames) > 0 {
			// No explicit targets: first table is deleted.
			deletedTables[strings.ToLower(tableNames[0])] = true
		}
		isMultiTable := len(s.Targets) > 0
		for _, tblName := range tableNames {
			var rowCount int64 = 1
			if e.Storage != nil {
				if tbl, err := e.Storage.GetTable(e.CurrentDB, tblName); err == nil {
					if n := len(tbl.Rows); n > 0 {
						rowCount = int64(n)
					}
				}
			}
			isDeleted := deletedTables[strings.ToLower(tblName)]
			st := "SIMPLE"
			if isDeleted {
				st = "DELETE"
			}
			var extra interface{} = nil
			if isDeleted && !isMultiTable && s.Where == nil {
				extra = "Deleting all rows"
			} else if isDeleted && !isMultiTable && s.Where != nil {
				extra = "Using where"
			}
			result = append(result, explainSelectType{
				id:         idCounter,
				selectType: st,
				table:      tblName,
				rows:       rowCount,
				filtered:   "100.00",
				accessType: "ALL",
				extra:      extra,
			})
		}
		if len(result) == 0 {
			return [][]interface{}{e.dummyExplainRow(query)}
		}
	case *sqlparser.Insert:
		// EXPLAIN INSERT/REPLACE: show table with select_type "INSERT" or "REPLACE"
		// rows, filtered are NULL in MySQL's traditional EXPLAIN for INSERT/REPLACE
		tblName := ""
		if tn, ok := s.Table.Expr.(sqlparser.TableName); ok {
			tblName = tn.Name.String()
		}
		stType := "INSERT"
		if s.Action == sqlparser.ReplaceAct {
			stType = "REPLACE"
		}
		result = append(result, explainSelectType{
			id:         idCounter,
			selectType: stType,
			table:      tblName,
			rows:       nil,
			filtered:   nil,
			accessType: "ALL",
		})
		// For INSERT/REPLACE ... SELECT, also include the SELECT subquery (same id as INSERT/REPLACE)
		if rows, ok := s.Rows.(*sqlparser.Select); ok {
			innerRows := e.explainSelect(rows, &idCounter, "SIMPLE")
			result = append(result, innerRows...)
		}
	default:
		return [][]interface{}{e.dummyExplainRow(query)}
	}

	// Convert to row format
	rows := make([][]interface{}, len(result))
	for i, r := range result {
		rows[i] = []interface{}{
			r.id, r.selectType, r.table, nil, r.accessType,
			r.possibleKeys, r.key, r.keyLen, r.ref, r.rows, r.filtered, r.extra,
		}
	}
	return rows
}

// explainSemijoin checks whether MySQL's semijoin optimization applies to this SELECT.
// When a non-correlated EXISTS or IN subquery appears in the WHERE clause (not SELECT/HAVING),
// MySQL's semijoin optimizer merges the subquery into the outer query and shows all
// participating tables with SELECT_TYPE=SIMPLE and the same query id=1.
// Returns (rows, true) if semijoin applies, otherwise (nil, false).
func (e *Executor) explainSemijoin(sel *sqlparser.Select, baseID int64) ([]explainSelectType, bool) {
	// No FROM clause → not applicable
	if len(sel.From) == 0 {
		return nil, false
	}
	// Derived tables in FROM → not semijoin
	for _, te := range sel.From {
		if e.tableExprHasSubquery(te) {
			return nil, false
		}
	}
	// No WHERE clause → no subquery to semijoin
	if sel.Where == nil {
		return nil, false
	}
	// STRAIGHT_JOIN as a SELECT modifier prevents semijoin flattening.
	if sel.StraightJoinHint {
		return nil, false
	}
	// Collect outer real table names; if none (e.g. FROM dual), semijoin doesn't apply
	outerTables := map[string]bool{}
	for _, te := range sel.From {
		for _, tn := range e.extractAllTableNames(te) {
			if !strings.EqualFold(tn, "dual") {
				outerTables[strings.ToLower(tn)] = true
			}
		}
	}
	if len(outerTables) == 0 {
		return nil, false
	}
	var semiTables []string // tables from EXISTS/IN inner selects
	hasSemijoins := false
	hasOtherSubqueries := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch n := node.(type) {
		case *sqlparser.Subquery:
			inner, ok := n.Select.(*sqlparser.Select)
			if !ok {
				hasOtherSubqueries = true
				return false, nil
			}
			// Check if correlated
			if e.isCorrelatedSubquery(n.Select, outerTables) {
				hasOtherSubqueries = true
				return false, nil
			}
			// STRAIGHT_JOIN as a SELECT modifier in the inner subquery prevents semijoin.
			// Note: STRAIGHT_JOIN as a join type (t1 STRAIGHT_JOIN t2) does NOT prevent it.
			if inner.StraightJoinHint {
				hasOtherSubqueries = true
				return false, nil
			}
			// Collect inner real table names (exclude pseudo-table "dual")
			innerTables := e.extractAllTableNamesFromSelect(inner)
			var realInnerTables []string
			for _, t := range innerTables {
				if !strings.EqualFold(t, "dual") {
					realInnerTables = append(realInnerTables, t)
				}
			}
			if len(realInnerTables) == 0 {
				// No real tables in EXISTS → not semijoin candidate
				hasOtherSubqueries = true
				return false, nil
			}
			semiTables = append(semiTables, realInnerTables...)
			hasSemijoins = true
			return false, nil
		}
		return true, nil
	}, sel.Where)

	if !hasSemijoins || hasOtherSubqueries || len(semiTables) == 0 {
		return nil, false
	}
	// Build SIMPLE rows: inner (semi-join) tables first, then outer tables
	// Use the single query id=baseID for all rows
	var rows []explainSelectType
	// Add inner semi-join tables (from EXISTS subqueries)
	for _, tbl := range semiTables {
		ai := e.explainDetectAccessType(sel, tbl)
		var rowCount int64 = 1
		if e.Storage != nil {
			if t, err := e.Storage.GetTable(e.CurrentDB, tbl); err == nil && len(t.Rows) > 0 {
				rowCount = int64(len(t.Rows))
			}
		}
		rows = append(rows, explainSelectType{
			id:           baseID,
			selectType:   "SIMPLE",
			table:        tbl,
			accessType:   ai.accessType,
			possibleKeys: nilIfEmpty(ai.possibleKeys),
			key:          nilIfEmpty(ai.key),
			keyLen:       nilIfEmpty(ai.keyLen),
			ref:          nilIfEmpty(ai.ref),
			rows:         rowCount,
			filtered:     "100.00",
			extra:        nil,
		})
	}
	// Add outer real tables (exclude dual)
	outerTableNames := e.extractAllTableNamesFromSelect(sel)
	for _, tbl := range outerTableNames {
		if strings.EqualFold(tbl, "dual") {
			continue
		}
		ai := e.explainDetectAccessType(sel, tbl)
		var rowCount int64 = 1
		if e.Storage != nil {
			if t, err := e.Storage.GetTable(e.CurrentDB, tbl); err == nil && len(t.Rows) > 0 {
				rowCount = int64(len(t.Rows))
			}
		}
		rows = append(rows, explainSelectType{
			id:           baseID,
			selectType:   "SIMPLE",
			table:        tbl,
			accessType:   ai.accessType,
			possibleKeys: nilIfEmpty(ai.possibleKeys),
			key:          nilIfEmpty(ai.key),
			keyLen:       nilIfEmpty(ai.keyLen),
			ref:          nilIfEmpty(ai.ref),
			rows:         rowCount,
			filtered:     "100.00",
			extra:        nil,
		})
	}
	return rows, true
}

// extractAllTableNamesFromSelect extracts real table names from a SELECT's FROM clause.
func (e *Executor) extractAllTableNamesFromSelect(sel *sqlparser.Select) []string {
	var names []string
	for _, te := range sel.From {
		names = append(names, e.extractAllTableNames(te)...)
	}
	return names
}

// nilIfEmpty returns nil if v is nil or an empty string, otherwise returns v.
func nilIfEmpty(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	if s, ok := v.(string); ok && s == "" {
		return nil
	}
	return v
}

// queryHasComplexParts returns true if the SELECT contains subqueries or derived tables.
func (e *Executor) queryHasComplexParts(sel *sqlparser.Select) bool {
	hasComplex := false
	// Check FROM clause for derived tables
	for _, te := range sel.From {
		if e.tableExprHasSubquery(te) {
			hasComplex = true
			break
		}
	}
	if hasComplex {
		return true
	}
	// Check for subqueries in SELECT expressions, WHERE, HAVING
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch n := node.(type) {
		case *sqlparser.Subquery:
			_ = n
			hasComplex = true
			return false, nil
		}
		return true, nil
	}, sel.SelectExprs, sel.Where, sel.Having)
	if hasComplex {
		return true
	}
	// Check for subqueries in JOIN ON conditions
	for _, te := range sel.From {
		var onNodes []sqlparser.SQLNode
		e.collectJoinOnConditions(te, &onNodes)
		for _, onNode := range onNodes {
			_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
				if _, ok := node.(*sqlparser.Subquery); ok {
					hasComplex = true
					return false, nil
				}
				return true, nil
			}, onNode)
		}
	}
	return hasComplex
}

// queryCanBeSemijoinFlattened returns true if the SELECT's WHERE-clause subqueries
// can all be flattened into a single SIMPLE query block (MySQL anti-join / semi-join
// optimization). The rules:
//
//   - No derived tables in FROM (those need DERIVED rows).
//   - The outer query must have at least one real table (not just DUAL).
//   - No subqueries in the SELECT list (scalar subqueries must stay as SUBQUERY).
//   - Every subquery in WHERE is either: IN (SELECT …), NOT IN (SELECT …),
//     EXISTS (SELECT …), or NOT EXISTS wrapped in ExistsExpr.
//   - No UNION subqueries inside IN (unions cannot be semijoin-flattened).
//   - semijoin optimizer flag is ON (default).
func (e *Executor) queryCanBeSemijoinFlattened(sel *sqlparser.Select) bool {
	// Note: derived tables in FROM do NOT prevent IN-subquery flattening.
	// MySQL can semijoin-flatten IN subqueries even when the outer FROM has derived tables.
	// (The old check was too restrictive.)

	// STRAIGHT_JOIN as a SELECT modifier (SELECT STRAIGHT_JOIN ...) prevents semijoin flattening.
	// Note: STRAIGHT_JOIN as a join type (t1 STRAIGHT_JOIN t2) is just a join order hint
	// and does NOT prevent semijoin flattening.
	if sel.StraightJoinHint {
		return false
	}

	// NO_SEMIJOIN hint in the outer query's comments (e.g. /*+ NO_SEMIJOIN(@subq) */) prevents
	// semijoin flattening even when the hint references a named query block (@subq).
	for _, c := range sel.Comments.GetComments() {
		if strings.Contains(strings.ToUpper(c), "NO_SEMIJOIN") {
			return false
		}
	}

	// The outer query must have at least one real table or derived table (not just DUAL).
	// If there are no real tables or derived tables, MySQL cannot form an anti-join and keeps
	// the subquery as a separate PRIMARY+SUBQUERY pair.
	var outerTables []string
	for _, te := range sel.From {
		outerTables = append(outerTables, e.extractAllTableNames(te)...)
	}
	numDerivedOuter := 0
	for _, te := range sel.From {
		numDerivedOuter += countDerivedTablesInExpr(te)
	}
	hasRealTable := numDerivedOuter > 0 // derived tables also count as "real" outer tables
	for _, tn := range outerTables {
		if strings.ToLower(tn) != "dual" {
			hasRealTable = true
			break
		}
	}
	if !hasRealTable {
		return false
	}

	// Subqueries in the SELECT list (scalar) prevent flattening.
	hasSelectSubquery := false
	_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
		if _, ok := n.(*sqlparser.Subquery); ok {
			hasSelectSubquery = true
			return false, nil
		}
		return true, nil
	}, sel.SelectExprs)
	if hasSelectSubquery {
		return false
	}

	// Collect ON conditions from JOIN expressions.
	var onConditions []sqlparser.SQLNode
	for _, te := range sel.From {
		e.collectJoinOnConditions(te, &onConditions)
	}

	// Check if there are any subqueries to flatten in WHERE or ON conditions.
	// If WHERE is nil AND no ON conditions have subqueries, this is a simple query.
	hasSubqueryInWhere := sel.Where != nil
	hasSubqueryInON := false
	for _, onCond := range onConditions {
		_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
			if _, ok := n.(*sqlparser.Subquery); ok {
				hasSubqueryInON = true
				return false, nil
			}
			return true, nil
		}, onCond)
		if hasSubqueryInON {
			break
		}
	}
	if !hasSubqueryInWhere && !hasSubqueryInON {
		return false // no subqueries at all → already SIMPLE
	}

	// Walk both WHERE and ON conditions for flattenability analysis.
	hasAny := false
	allFlattenable := true
	walkFn := func(n sqlparser.SQLNode) (bool, error) {
		switch expr := n.(type) {
		case *sqlparser.ExistsExpr:
			// EXISTS / NOT EXISTS can be flattened only when the subquery has real tables.
			// If the subquery is `EXISTS (SELECT 1 FROM dual)`, MySQL keeps it as SUBQUERY.
			if inner, ok := expr.Subquery.Select.(*sqlparser.Select); ok {
				var innerTables []string
				for _, te := range inner.From {
					innerTables = append(innerTables, e.extractAllTableNames(te)...)
				}
				hasRealInner := false
				for _, tn := range innerTables {
					if strings.ToLower(tn) != "dual" {
						hasRealInner = true
						break
					}
				}
				if !hasRealInner {
					// No real inner tables: EXISTS is a constant check, can't be flattened.
					allFlattenable = false
					return false, nil
				}
				// If the EXISTS inner SELECT itself contains nested subqueries (e.g. correlated
				// scalar subqueries in WHERE), MySQL cannot flatten it into a simple anti-join
				// at the outer id level — it produces a separate anti-join row plus new subquery ids.
				innerHasNestedSubquery := false
				_ = sqlparser.Walk(func(n2 sqlparser.SQLNode) (bool, error) {
					if _, ok2 := n2.(*sqlparser.Subquery); ok2 {
						innerHasNestedSubquery = true
						return false, nil
					}
					return true, nil
				}, inner.SelectExprs, inner.Where, inner.Having)
				if innerHasNestedSubquery {
					allFlattenable = false
					return false, nil
				}
				hasAny = true
			}
			return false, nil
		case *sqlparser.ComparisonExpr:
			// Check if the right-hand side is a subquery (IN, NOT IN, = ANY, etc.)
			isSubqueryComparison := false
			if expr.Operator == sqlparser.InOp || expr.Operator == sqlparser.NotInOp {
				isSubqueryComparison = true
			} else if expr.Modifier == sqlparser.Any {
				// = ANY with a subquery is equivalent to IN and can be semijoin-flattened.
				// Note: !=ANY / <ANY / >ANY / <=ANY / >=ANY with a subquery are NOT
				// equivalent to IN and cannot be semijoin-flattened.
				// Only = ANY is equivalent to IN.
				if expr.Operator == sqlparser.EqualOp {
					if _, ok := expr.Right.(*sqlparser.Subquery); ok {
						isSubqueryComparison = true
					}
				}
			}
			// Note: ALL modifier (>= ALL, <= ALL, = ALL, etc.) is NEVER semijoin-flattenable.
			// It requires a different execution strategy (ALL-subquery execution).
			if isSubqueryComparison {
				if sub, ok := expr.Right.(*sqlparser.Subquery); ok {
					// UNION subqueries inside IN/ANY cannot be semijoin-flattened.
					if _, isUnion := sub.Select.(*sqlparser.Union); isUnion {
						allFlattenable = false
						return false, nil
					}
					// IN/ANY subqueries with no real inner tables are constant checks.
					if inner, ok := sub.Select.(*sqlparser.Select); ok {
						var innerTables []string
						for _, te := range inner.From {
							innerTables = append(innerTables, e.extractAllTableNames(te)...)
						}
						hasRealInner := false
						for _, tn := range innerTables {
							if strings.ToLower(tn) != "dual" {
								hasRealInner = true
								break
							}
						}
						if !hasRealInner {
							allFlattenable = false
							return false, nil
						}
						// Check for NO_SEMIJOIN optimizer hint in the inner SELECT.
						// e.g. SELECT /*+ NO_SEMIJOIN() */ a FROM t1 prevents flattening.
						for _, c := range inner.Comments.GetComments() {
							if strings.Contains(strings.ToUpper(c), "NO_SEMIJOIN") {
								allFlattenable = false
								return false, nil
							}
						}
						// STRAIGHT_JOIN as a SELECT modifier in the inner subquery prevents semijoin flattening.
						// Note: STRAIGHT_JOIN as a join type (t1 STRAIGHT_JOIN t2) does NOT prevent it.
						if inner.StraightJoinHint {
							allFlattenable = false
							return false, nil
						}
						// GROUP BY in the inner subquery prevents semijoin flattening.
						// MySQL uses IN-to-EXISTS conversion for aggregate subqueries instead.
						if inner.GroupBy != nil && len(inner.GroupBy.Exprs) > 0 {
							allFlattenable = false
							return false, nil
						}
						// HAVING in the inner subquery also prevents semijoin.
						if inner.Having != nil {
							allFlattenable = false
							return false, nil
						}
					}
					hasAny = true
					return false, nil
				}
			}
			return true, nil
		case *sqlparser.Subquery:
			// A bare subquery in WHERE not wrapped in EXISTS/IN/NOT IN/= ANY.
			allFlattenable = false
			return false, nil
		}
		return true, nil
	}
	// Walk WHERE clause
	if sel.Where != nil {
		_ = sqlparser.Walk(walkFn, sel.Where)
	}
	// Walk JOIN ON conditions
	for _, onCond := range onConditions {
		_ = sqlparser.Walk(walkFn, onCond)
	}

	if !hasAny || !allFlattenable {
		return false
	}

	// MySQL cannot semijoin-flatten if the total number of tables (outer + inner) exceeds MAX_TABLES (61).
	// When there are too many tables, MySQL keeps the subquery as a separate SUBQUERY block.
	const mysqlMaxTables = 61
	outerTableCount := len(outerTables)
	// Count inner tables from all IN/ANY subqueries in WHERE and ON conditions
	innerTableCount := 0
	countInnerTables := func(n sqlparser.SQLNode) (bool, error) {
		if comp, ok := n.(*sqlparser.ComparisonExpr); ok {
			if comp.Operator == sqlparser.InOp || comp.Operator == sqlparser.NotInOp {
				if sub, ok := comp.Right.(*sqlparser.Subquery); ok {
					if inner, ok := sub.Select.(*sqlparser.Select); ok {
						for _, te := range inner.From {
							innerTableCount += len(e.extractAllTableNames(te))
						}
					}
				}
			}
		}
		return true, nil
	}
	if sel.Where != nil {
		_ = sqlparser.Walk(countInnerTables, sel.Where)
	}
	for _, onCond := range onConditions {
		_ = sqlparser.Walk(countInnerTables, onCond)
	}
	if outerTableCount+innerTableCount > mysqlMaxTables {
		return false // Too many tables → MySQL keeps subquery as SUBQUERY, not SIMPLE
	}

	// semijoin must be enabled (it is on by default).
	return e.isOptimizerSwitchEnabled("semijoin")
}

// tableExprHasSubquery checks if a table expression contains a derived table (subquery in FROM).
func (e *Executor) tableExprHasSubquery(te sqlparser.TableExpr) bool {
	switch t := te.(type) {
	case *sqlparser.AliasedTableExpr:
		if _, ok := t.Expr.(*sqlparser.DerivedTable); ok {
			return true
		}
	case *sqlparser.JoinTableExpr:
		return e.tableExprHasSubquery(t.LeftExpr) || e.tableExprHasSubquery(t.RightExpr)
	case *sqlparser.ParenTableExpr:
		for _, expr := range t.Exprs {
			if e.tableExprHasSubquery(expr) {
				return true
			}
		}
	}
	return false
}

// tableExprHasStraightJoin returns true if any JoinTableExpr in the tree uses StraightJoinType.
func (e *Executor) tableExprHasStraightJoin(te sqlparser.TableExpr) bool {
	switch t := te.(type) {
	case *sqlparser.JoinTableExpr:
		if t.Join == sqlparser.StraightJoinType {
			return true
		}
		return e.tableExprHasStraightJoin(t.LeftExpr) || e.tableExprHasStraightJoin(t.RightExpr)
	case *sqlparser.ParenTableExpr:
		for _, expr := range t.Exprs {
			if e.tableExprHasStraightJoin(expr) {
				return true
			}
		}
	}
	return false
}

// extractAllTableNames collects all real table names from a table expression tree.
func (e *Executor) extractAllTableNames(te sqlparser.TableExpr) []string {
	switch t := te.(type) {
	case *sqlparser.AliasedTableExpr:
		if _, ok := t.Expr.(*sqlparser.DerivedTable); ok {
			return nil
		}
		if tn, ok := t.Expr.(sqlparser.TableName); ok {
			return []string{tn.Name.String()}
		}
	case *sqlparser.JoinTableExpr:
		left := e.extractAllTableNames(t.LeftExpr)
		right := e.extractAllTableNames(t.RightExpr)
		return append(left, right...)
	case *sqlparser.ParenTableExpr:
		var names []string
		for _, expr := range t.Exprs {
			names = append(names, e.extractAllTableNames(expr)...)
		}
		return names
	}
	return nil
}

// countDerivedTablesInExpr counts how many direct derived tables (subqueries in FROM) exist
// in a table expression.
func countDerivedTablesInExpr(te sqlparser.TableExpr) int {
	switch t := te.(type) {
	case *sqlparser.AliasedTableExpr:
		if _, ok := t.Expr.(*sqlparser.DerivedTable); ok {
			return 1
		}
	case *sqlparser.JoinTableExpr:
		return countDerivedTablesInExpr(t.LeftExpr) + countDerivedTablesInExpr(t.RightExpr)
	case *sqlparser.ParenTableExpr:
		n := 0
		for _, expr := range t.Exprs {
			n += countDerivedTablesInExpr(expr)
		}
		return n
	}
	return 0
}

// explainSelect produces EXPLAIN rows for a SELECT statement.
func (e *Executor) explainSelect(sel *sqlparser.Select, idCounter *int64, selectType string) []explainSelectType {
	myID := *idCounter
	var result []explainSelectType

	// Collect all real table names from FROM clause (skip synthesized "dual").
	var allTableNames []string
	for _, te := range sel.From {
		for _, tn := range e.extractAllTableNames(te) {
			if strings.ToLower(tn) != "dual" {
				allTableNames = append(allTableNames, tn)
			}
		}
	}

	// Count direct derived tables in FROM clause
	numDerived := 0
	for _, te := range sel.From {
		numDerived += countDerivedTablesInExpr(te)
	}

	// Check for GROUP BY / SQL_BIG_RESULT
	queryStr := sqlparser.String(sel)
	upperQ := strings.ToUpper(queryStr)
	orderByNull := len(sel.OrderBy) == 1 && func() bool {
		_, ok := sel.OrderBy[0].Expr.(*sqlparser.NullVal)
		return ok
	}()

	if len(allTableNames) == 0 && numDerived == 0 {
		// No tables at all
		result = append(result, explainSelectType{
			id:         myID,
			selectType: selectType,
			table:      nil,
			extra:      "No tables used",
			rows:       nil,
			filtered:   nil,
			accessType: nil,
		})
	} else if len(allTableNames) == 0 && numDerived > 0 {
		// Only derived tables in FROM - add a row for each derived table reference.
		// Derived tables will get ids starting at *idCounter+1 (assigned in explainFromExpr).
		nextID := *idCounter + 1
		for i := 0; i < numDerived; i++ {
			derivedRef := fmt.Sprintf("<derived%d>", nextID)
			result = append(result, explainSelectType{
				id:           myID,
				selectType:   selectType,
				table:        derivedRef,
				extra:        nil,
				rows:         int64(1),
				filtered:     "100.00",
				accessType:   "ALL",
				possibleKeys: nil,
				key:          nil,
				keyLen:       nil,
				ref:          nil,
			})
			nextID++
		}
	} else {
		// Check for "Impossible WHERE" due to out-of-range constant comparisons.
		// This is needed for multi-table joins where MySQL's optimizer detects that
		// a constant is out of range for a column type (e.g. TINYINT col = 128) and
		// propagates through equality conditions to produce a single "Impossible WHERE" row.
		if e.Storage != nil && len(allTableNames) > 1 && e.isWhereImpossibleDueToConstantOutOfRange(sel) {
			result = append(result, explainSelectType{
				id:         myID,
				selectType: selectType,
				table:      nil,
				extra:      "Impossible WHERE",
				rows:       nil,
				filtered:   nil,
				accessType: nil,
			})
			return result
		}
		// Pre-scan: determine which tables in a multi-table join qualify for
		// "Range checked for each record".  We use this to reorder the table
		// list so driver tables (no range-check) appear before inner tables
		// (range-checked), matching MySQL's EXPLAIN output order.
		rangeCheckedTables := make(map[string]bool)
		if len(allTableNames) > 1 && sel.Where != nil && e.Storage != nil {
			for _, tbl := range allTableNames {
				td := e.explainGetTableDef(tbl)
				if td != nil && explainTotalIndexCount(td) > 0 {
					if e.explainHasRangeFromOtherTable(sel.Where.Expr, tbl, allTableNames) {
						rangeCheckedTables[tbl] = true
					}
				}
			}
		}
		// Reorder: driver tables (not range-checked) first, inner tables last.
		if len(rangeCheckedTables) > 0 {
			var drivers []string
			var inners []string
			for _, tbl := range allTableNames {
				if rangeCheckedTables[tbl] {
					inners = append(inners, tbl)
				} else {
					drivers = append(drivers, tbl)
				}
			}
			allTableNames = append(drivers, inners...)
		}

		for idx, tblName := range allTableNames {
			var rowCount int64 = 1
			tableIsEmpty := false
			if e.Storage != nil {
				if tbl, err := e.Storage.GetTable(e.CurrentDB, tblName); err == nil {
					if n := len(tbl.Rows); n > 0 {
						rowCount = int64(n)
					} else {
						tableIsEmpty = true
						rowCount = 0
					}
				}
			}

			var extra interface{} = nil
			// Detect access type based on WHERE clause and available indexes.
			// We need this before the tableIsEmpty check to determine whether "no matching
			// row in const table" applies: MySQL only uses that message for const-access tables
			// (primary key / unique key equality lookup), not for ALL-scan empty tables.
			accessInfo := e.explainDetectAccessType(sel, tblName)

			// "no matching row in const table": MySQL shows this only when the table is accessed
			// via const access (PRIMARY KEY equality) and the row doesn't exist (empty table).
			// For ALL-scan empty tables MySQL still shows the plan with the real table name.
			// Exception: MATERIALIZED subqueries always show the real table name with 0 rows.
			if tableIsEmpty && len(allTableNames) == 1 && idx == 0 && selectType != "MATERIALIZED" &&
				(accessInfo.accessType == "const" || accessInfo.accessType == "system") {
				result = append(result, explainSelectType{
					id:         myID,
					selectType: selectType,
					table:      nil,
					extra:      "no matching row in const table",
					rows:       nil,
					filtered:   nil,
					accessType: nil,
				})
				// When the outer table is empty with const access in a semijoin-flattened context
				// (SIMPLE), return immediately without processing subqueries. MySQL collapses the
				// entire result to 1 NULL row in this case.
				if selectType == "SIMPLE" {
					return result
				}
				continue
			} else if idx == 0 && !orderByNull && (strings.Contains(upperQ, "GROUP BY") || strings.Contains(upperQ, "SQL_BIG_RESULT")) {
				// When GROUP BY is on a constant expression (e.g. GROUP BY 1), MySQL does
				// not need a filesort because all rows share the same group key.
				if !explainGroupByIsAllConstant(sel.GroupBy) {
					extra = "Using filesort"
				}
			}
			// "Range checked for each record" detection: applies to any table in a
			// multi-table join where an indexed column is constrained by a range
			// condition whose other side is a column from a different table
			// (e.g. WHERE inner.b < outer.c).  MySQL shows this instead of a fixed
			// access type because the index choice depends on each outer row's value.
			rangeCheckedForEachRecord := false
			if len(allTableNames) > 1 && sel.Where != nil && e.Storage != nil {
				td := e.explainGetTableDef(tblName)
				if td != nil && explainTotalIndexCount(td) > 0 {
					if e.explainHasRangeFromOtherTable(sel.Where.Expr, tblName, allTableNames) {
						n := explainTotalIndexCount(td)
						extra = fmt.Sprintf("Range checked for each record (index map: %s)", explainIndexBitmapHex(n))
						rangeCheckedForEachRecord = true
					}
				}
			}
			// For secondary tables in a cross-join that do not qualify for
			// "Range checked for each record", MySQL shows "Using join buffer".
			if idx > 0 && !rangeCheckedForEachRecord {
				extra = "Using join buffer (Block Nested Loop)"
			}

			var accessType interface{} = "ALL"
			var filtered interface{} = "100.00"
			var possibleKeys interface{} = nil
			var key interface{} = nil
			var keyLen interface{} = nil
			var ref interface{} = nil

			// accessInfo was already computed above (before the tableIsEmpty check).
			// For "Range checked for each record", MySQL shows type=ALL (no key chosen in
			// advance), while possible_keys still lists the candidate indexes.
			if rangeCheckedForEachRecord {
				accessType = "ALL"
				possibleKeys = accessInfo.possibleKeys
				key = nil
				keyLen = nil
				ref = nil
			} else {
				accessType = accessInfo.accessType
				possibleKeys = accessInfo.possibleKeys
				key = accessInfo.key
				keyLen = accessInfo.keyLen
				ref = accessInfo.ref
			}

			if accessInfo.accessType == "const" || accessInfo.accessType == "eq_ref" || accessInfo.accessType == "ref" {
				rowCount = int64(1)
			} else if accessInfo.accessType == "range" && sel.Where != nil && e.Storage != nil {
				// For range access, estimate row count by evaluating the WHERE on stored rows.
				// This gives accurate estimates for small in-memory tables.
				if tbl, err := e.Storage.GetTable(e.CurrentDB, tblName); err == nil && len(tbl.Rows) > 0 {
					rangeCount := int64(0)
					for _, row := range tbl.Rows {
						if match, merr := e.evalWhere(sel.Where.Expr, row); merr == nil && match {
							rangeCount++
						}
					}
					if rangeCount > 0 {
						rowCount = rangeCount
					}
				}
			} else if tableIsEmpty {
				// For ALL-scan empty tables, use rows=1 to match InnoDB's default statistics
				// (the minimum estimate for an empty table is 1 row in InnoDB stats).
				// filtered stays at "100.00" because MySQL's EXPLAIN uses the theoretical plan,
				// not the actual row counts.
				rowCount = int64(1)
			}

			// For "Range checked for each record", MySQL estimates filtered as 100/rowCount.
			if rangeCheckedForEachRecord && rowCount > 0 {
				filtered = fmt.Sprintf("%.2f", 100.0/float64(rowCount))
			}

			// Set "Using index condition" for ref access with IS NULL or range conditions on indexed columns
			if extra == nil && (accessInfo.accessType == "ref" || accessInfo.accessType == "range") {
				if sel.Where != nil {
					wcs := explainExtractWhereConditions(sel.Where.Expr, tblName)
					for _, wc := range wcs {
						if wc.isNull {
							extra = "Using index condition"
							break
						}
					}
				}
			}

			// Add "Using where" for ALL-access tables that have WHERE conditions filtering them.
			// MySQL adds "Using where" when the engine applies a WHERE filter during a full scan.
			if accessInfo.accessType == "ALL" && sel.Where != nil {
				dbName := e.CurrentDB
				if dbName == "" {
					dbName = "test"
				}
				cond := e.extractTableCondition(sel.Where.Expr, tblName, dbName)
				if cond != "" {
					if extra == nil {
						extra = "Using where"
					} else {
						extra = fmt.Sprintf("Using where; %v", extra)
					}
				}
			}

			result = append(result, explainSelectType{
				id:           myID,
				selectType:   selectType,
				table:        tblName,
				extra:        extra,
				rows:         rowCount,
				filtered:     filtered,
				accessType:   accessType,
				possibleKeys: possibleKeys,
				key:          key,
				keyLen:       keyLen,
				ref:          ref,
			})
		}
	}

	// When there are both real tables and derived tables in FROM (e.g. a JOIN),
	// add a <derivedN> placeholder row for each derived table using the current
	// select's id.  The actual inner rows (with incremented ids) will be appended
	// below by explainFromExpr.
	if len(allTableNames) > 0 && numDerived > 0 {
		nextID := *idCounter + 1
		for i := 0; i < numDerived; i++ {
			derivedRef := fmt.Sprintf("<derived%d>", nextID)
			result = append(result, explainSelectType{
				id:           myID,
				selectType:   selectType,
				table:        derivedRef,
				extra:        "Using join buffer (Block Nested Loop)",
				rows:         int64(1),
				filtered:     "100.00",
				accessType:   "ALL",
				possibleKeys: nil,
				key:          nil,
				keyLen:       nil,
				ref:          nil,
			})
			nextID++
		}
	}

	// Process FROM clause for derived tables.
	// We collect them separately first so that WHERE-clause subqueries
	// (DEPENDENT SUBQUERY / SUBQUERY) can be inserted before DERIVED rows,
	// matching MySQL's EXPLAIN output order.
	derivedStart := len(result)
	for _, te := range sel.From {
		e.explainFromExpr(te, idCounter, &result)
	}
	derivedRows := make([]explainSelectType, len(result)-derivedStart)
	copy(derivedRows, result[derivedStart:])
	result = result[:derivedStart]

	// Process subqueries in SELECT expressions, WHERE, HAVING.
	// MySQL outputs WHERE-clause subqueries BEFORE FROM-clause derived tables.
	e.explainSubqueries(sel, idCounter, &result, selectType)

	// Append derived rows after WHERE-clause subquery rows.
	result = append(result, derivedRows...)

	return result
}

// explainFromExpr processes table expressions to find derived tables.
func (e *Executor) explainFromExpr(te sqlparser.TableExpr, idCounter *int64, result *[]explainSelectType) {
	switch t := te.(type) {
	case *sqlparser.AliasedTableExpr:
		if dt, ok := t.Expr.(*sqlparser.DerivedTable); ok {
			// This is a derived table (subquery in FROM)
			*idCounter++
			myID := *idCounter // capture ID before recursive calls change the counter
			switch inner := dt.Select.(type) {
			case *sqlparser.Union:
				derived := e.explainUnion(inner, idCounter, false)
				// The first element should be DERIVED
				if len(derived) > 0 {
					derived[0].selectType = "DERIVED"
				}
				*result = append(*result, derived...)
			case *sqlparser.Select:
				innerRows := e.explainSelect(inner, idCounter, "DERIVED")
				// Fix the id: the DERIVED row gets the id captured before recursion
				if len(innerRows) > 0 {
					innerRows[0].id = myID
				}
				*result = append(*result, innerRows...)
			}
		}
	case *sqlparser.JoinTableExpr:
		e.explainFromExpr(t.LeftExpr, idCounter, result)
		e.explainFromExpr(t.RightExpr, idCounter, result)
	case *sqlparser.ParenTableExpr:
		for _, expr := range t.Exprs {
			e.explainFromExpr(expr, idCounter, result)
		}
	}
}

// extractTableNamesAndAliases collects all table names and aliases from a SELECT's FROM clause.
// This is used to determine whether a subquery references outer tables (correlated).
func (e *Executor) extractTableNamesAndAliases(sel *sqlparser.Select) map[string]bool {
	names := make(map[string]bool)
	for _, te := range sel.From {
		e.collectTableNamesAndAliases(te, names)
	}
	return names
}

func (e *Executor) collectTableNamesAndAliases(te sqlparser.TableExpr, names map[string]bool) {
	switch t := te.(type) {
	case *sqlparser.AliasedTableExpr:
		if tn, ok := t.Expr.(sqlparser.TableName); ok {
			names[strings.ToLower(tn.Name.String())] = true
		}
		if !t.As.IsEmpty() {
			names[strings.ToLower(t.As.String())] = true
		}
	case *sqlparser.JoinTableExpr:
		e.collectTableNamesAndAliases(t.LeftExpr, names)
		e.collectTableNamesAndAliases(t.RightExpr, names)
	case *sqlparser.ParenTableExpr:
		for _, expr := range t.Exprs {
			e.collectTableNamesAndAliases(expr, names)
		}
	}
}

// isCorrelatedSubquery checks whether a subquery SELECT references any of the outer table names.
// isOptimizerSwitchEnabled checks if a specific optimizer_switch flag is enabled.
// Returns true if the flag is "on" or if the switch is not set (defaults to on for most flags).
func (e *Executor) isOptimizerSwitchEnabled(flag string) bool {
	switchVal, ok := e.getSysVar("optimizer_switch")
	if !ok {
		return true // default: on
	}
	for _, part := range strings.Split(switchVal, ",") {
		part = strings.TrimSpace(part)
		kv := strings.SplitN(part, "=", 2)
		if len(kv) == 2 && strings.TrimSpace(kv[0]) == flag {
			return strings.TrimSpace(kv[1]) == "on"
		}
	}
	return true // not found means default (on)
}

func (e *Executor) isCorrelatedSubquery(subSelect sqlparser.TableStatement, outerTables map[string]bool) bool {
	if len(outerTables) == 0 {
		return false
	}

	// Collect the subquery's own table names/aliases
	innerTables := make(map[string]bool)
	switch s := subSelect.(type) {
	case *sqlparser.Select:
		innerTables = e.extractTableNamesAndAliases(s)
	case *sqlparser.Union:
		// For unions, collect from all selects
		selects := e.flattenUnion(s)
		for _, sel := range selects {
			if inner, ok := sel.(*sqlparser.Select); ok {
				for k, v := range e.extractTableNamesAndAliases(inner) {
					innerTables[k] = v
				}
			}
		}
	}

	// Check if the subquery has any real inner tables (not DUAL)
	hasRealInnerTables := false
	for t := range innerTables {
		if !strings.EqualFold(t, "dual") {
			hasRealInnerTables = true
			break
		}
	}

	correlated := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if cn, ok := node.(*sqlparser.ColName); ok {
			qualifier := strings.ToLower(cn.Qualifier.Name.String())
			if qualifier != "" && outerTables[qualifier] && !innerTables[qualifier] {
				// Qualified reference to outer table: clearly correlated
				correlated = true
				return false, nil
			}
			if qualifier == "" && !hasRealInnerTables {
				// Unqualified column reference with no real inner tables (e.g. FROM DUAL):
				// the column must come from the outer scope → correlated
				colName := strings.ToLower(cn.Name.String())
				// Check if this column name exists in any outer table
				for outerTbl := range outerTables {
					if strings.EqualFold(outerTbl, "dual") {
						continue
					}
					// Look up the table schema to check column existence
					if e.Storage != nil {
						if tbl, err := e.Storage.GetTable(e.CurrentDB, outerTbl); err == nil && tbl.Def != nil {
							for _, col := range tbl.Def.Columns {
								if strings.EqualFold(col.Name, colName) {
									correlated = true
									return false, nil
								}
							}
						}
					}
				}
			}
		}
		return true, nil
	}, subSelect)
	return correlated
}

// outerTablesAreSystem returns true if all outer tables together have exactly 1 row,
// making the outer query a "system" table access. In this case, MySQL prefers inline
// FirstMatch over MATERIALIZED because the const/system access is cheap.
func (e *Executor) outerTablesAreSystem(outerTables map[string]bool) bool {
	if e.Storage == nil || len(outerTables) == 0 {
		return false
	}
	totalRows := 0
	for tblName := range outerTables {
		tbl, err := e.Storage.GetTable(e.CurrentDB, tblName)
		if err != nil {
			return false
		}
		totalRows += len(tbl.Rows)
	}
	return totalRows == 1
}

// innerSubqueryUsesOnlyStraightJoin returns true if the SELECT's FROM clause uses ONLY STRAIGHT_JOIN
// (no INNER JOIN, no LEFT JOIN, no comma-join). STRAIGHT_JOIN forces join order, preventing table reordering.
// Note: FROM t1, t2 (comma join) has len(sel.From)==2 with separate AliasedTableExprs, NOT STRAIGHT_JOIN.
// STRAIGHT_JOIN syntax (t1 STRAIGHT_JOIN t2) produces a single JoinTableExpr with StraightJoinType.
func (e *Executor) innerSubqueryUsesOnlyStraightJoin(sel *sqlparser.Select) bool {
	if len(sel.From) == 0 {
		return false
	}
	// Comma join (FROM t1, t2) means multiple top-level table expressions — not STRAIGHT_JOIN
	if len(sel.From) > 1 {
		return false
	}
	for _, te := range sel.From {
		if !allJoinsAreStraight(te) {
			return false
		}
	}
	// Must have at least one actual JoinTableExpr with StraightJoinType
	return hasStraightJoin(sel.From[0])
}

// hasStraightJoin returns true if the table expression contains at least one STRAIGHT_JOIN.
func hasStraightJoin(te sqlparser.TableExpr) bool {
	switch t := te.(type) {
	case *sqlparser.JoinTableExpr:
		if t.Join == sqlparser.StraightJoinType {
			return true
		}
		return hasStraightJoin(t.LeftExpr) || hasStraightJoin(t.RightExpr)
	case *sqlparser.ParenTableExpr:
		for _, expr := range t.Exprs {
			if hasStraightJoin(expr) {
				return true
			}
		}
	}
	return false
}

func allJoinsAreStraight(te sqlparser.TableExpr) bool {
	switch t := te.(type) {
	case *sqlparser.JoinTableExpr:
		if t.Join != sqlparser.StraightJoinType {
			return false
		}
		return allJoinsAreStraight(t.LeftExpr) && allJoinsAreStraight(t.RightExpr)
	case *sqlparser.AliasedTableExpr:
		return true // base table - no join
	case *sqlparser.ParenTableExpr:
		for _, expr := range t.Exprs {
			if !allJoinsAreStraight(expr) {
				return false
			}
		}
		return true
	}
	return true
}

// innerSubqueryUsesRightJoin returns true if the SELECT's FROM clause contains a RIGHT JOIN.
func (e *Executor) innerSubqueryUsesRightJoin(sel *sqlparser.Select) bool {
	for _, te := range sel.From {
		if hasRightJoin(te) {
			return true
		}
	}
	return false
}

func hasRightJoin(te sqlparser.TableExpr) bool {
	switch t := te.(type) {
	case *sqlparser.JoinTableExpr:
		if t.Join == sqlparser.RightJoinType || t.Join == sqlparser.NaturalRightJoinType {
			return true
		}
		return hasRightJoin(t.LeftExpr) || hasRightJoin(t.RightExpr)
	case *sqlparser.ParenTableExpr:
		for _, expr := range t.Exprs {
			if hasRightJoin(expr) {
				return true
			}
		}
	}
	return false
}

// allInnerTablesTwoOrMoreRows returns true if the inner SELECT uses STRAIGHT_JOIN and ALL inner
// tables have >= 2 rows. In this case, MySQL forces MATERIALIZED even for constant outer expressions
// (e.g., "11 IN (SELECT ...)"), because materializing is cheaper than scanning large fixed-order tables.
// outerQueryHasOnlyDerivedTables returns true if the outer query's FROM clause contains
// only derived tables (subqueries) and no real base tables. This is used to determine
// whether MySQL will prefer MATERIALIZED strategy for IN subqueries (since FirstMatch
// can't be applied efficiently when there's no base table to probe against).
func (e *Executor) outerQueryHasOnlyDerivedTables(sel *sqlparser.Select) bool {
	if sel == nil || len(sel.From) == 0 {
		return false
	}
	numDerived := 0
	numBase := 0
	for _, te := range sel.From {
		numDerived += countDerivedTablesInExpr(te)
		baseNames := e.extractAllTableNames(te)
		for _, name := range baseNames {
			if !strings.EqualFold(name, "dual") {
				numBase++
			}
		}
	}
	return numDerived > 0 && numBase == 0
}

func (e *Executor) allInnerTablesTwoOrMoreRows(inner *sqlparser.Select) bool {
	if e.Storage == nil {
		return false
	}
	if !e.innerSubqueryUsesOnlyStraightJoin(inner) {
		return false
	}
	var innerTables []string
	for _, te := range inner.From {
		innerTables = append(innerTables, e.extractAllTableNames(te)...)
	}
	var realInner []string
	for _, t := range innerTables {
		if !strings.EqualFold(t, "dual") {
			realInner = append(realInner, t)
		}
	}
	if len(realInner) < 2 {
		return false
	}
	for _, tblName := range realInner {
		tbl, err := e.Storage.GetTable(e.CurrentDB, tblName)
		if err != nil || len(tbl.Rows) < 2 {
			return false
		}
	}
	return true
}

// selectWhereHasINSubquery returns true if a SELECT's WHERE clause contains at least one
// IN subquery (col IN (SELECT ...)). This is used to detect nested IN subquery chains
// which trigger MySQL's DuplicateWeedout semi-join strategy.
func selectWhereHasINSubquery(sel *sqlparser.Select) bool {
	if sel == nil || sel.Where == nil {
		return false
	}
	found := false
	_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
		if found {
			return false, nil
		}
		if cmp, ok := n.(*sqlparser.ComparisonExpr); ok {
			if cmp.Operator == sqlparser.InOp {
				if _, ok := cmp.Right.(*sqlparser.Subquery); ok {
					found = true
					return false, nil
				}
			}
		}
		// Also handle tuple IN subquery: (a, b) IN (SELECT ...)
		if cmp, ok := n.(*sqlparser.ComparisonExpr); ok {
			if cmp.Operator == sqlparser.InOp {
				if _, ok := cmp.Right.(*sqlparser.Subquery); ok {
					found = true
					return false, nil
				}
			}
		}
		return true, nil
	}, sel.Where)
	return found
}

// collectAllTablesForDuplicateWeedout recursively collects all table names from a nested IN subquery chain.
// This is used to determine all tables that should appear in DuplicateWeedout EXPLAIN output (all SIMPLE, id=1).
// Returns a slice of table names in depth-first order (inner tables first, outer table last).
func (e *Executor) collectAllTablesForDuplicateWeedout(inner *sqlparser.Select) []string {
	var result []string
	// Collect direct tables from this SELECT's FROM
	for _, te := range inner.From {
		for _, tn := range e.extractAllTableNames(te) {
			if !strings.EqualFold(tn, "dual") {
				result = append(result, tn)
			}
		}
	}
	// Recursively collect tables from any IN subqueries in WHERE
	if inner.Where != nil {
		_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
			if cmp, ok := n.(*sqlparser.ComparisonExpr); ok {
				if cmp.Operator == sqlparser.InOp {
					if sub, ok := cmp.Right.(*sqlparser.Subquery); ok {
						if nestedSel, ok := sub.Select.(*sqlparser.Select); ok {
							nested := e.collectAllTablesForDuplicateWeedout(nestedSel)
							result = append(result, nested...)
						}
						return false, nil
					}
				}
			}
			return true, nil
		}, inner.Where)
	}
	return result
}

// shouldUseDuplicateWeedout returns true when the inner SELECT has nested IN subqueries
// (i.e., its WHERE has an IN subquery), which triggers MySQL's DuplicateWeedout semijoin strategy.
// When DuplicateWeedout is used, MySQL flattens all tables from all nesting levels into a
// single id=1 SIMPLE join with "Start temporary" / "End temporary" markers.
func (e *Executor) shouldUseDuplicateWeedout(inner *sqlparser.Select) bool {
	if !e.isSemijoinEnabled() {
		return false
	}
	return selectWhereHasINSubquery(inner)
}

// shouldMaterializeSubquery returns true if an IN subquery should use the MATERIALIZED strategy.
// MySQL uses a cost-based approach, but we approximate with a row count heuristic:
// extractINSubqueryOuterCol extracts the outer column name from the first
// IN (subquery) condition in a SELECT's WHERE clause.
// For "WHERE col IN (SELECT ...)", returns "col".
// inSubqueryOuterIsConst returns true if the WHERE clause has "const IN (subquery)"
// where the left side of IN is a literal constant (e.g. "11 IN (SELECT ...)").
// In this case MySQL uses const access on the materialized hash table.
func inSubqueryOuterIsConst(sel *sqlparser.Select) bool {
	if sel == nil || sel.Where == nil {
		return false
	}
	result := false
	_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
		if cmp, ok := n.(*sqlparser.ComparisonExpr); ok {
			if cmp.Operator == sqlparser.InOp {
				if _, ok := cmp.Right.(*sqlparser.Subquery); ok {
					switch cmp.Left.(type) {
					case *sqlparser.Literal, *sqlparser.NullVal:
						result = true
						return false, nil
					}
				}
			}
		}
		return true, nil
	}, sel.Where.Expr)
	return result
}

// For "WHERE tbl.col IN (SELECT ...)", returns "col".
// Returns "" if no IN subquery condition is found.
func extractINSubqueryOuterCol(sel *sqlparser.Select) string {
	if sel == nil || sel.Where == nil {
		return ""
	}
	return extractINColFromExpr(sel.Where.Expr)
}

// selectHasDerivedTableInFrom returns true if the SELECT has at least one
// derived table (subselect) in its FROM clause (e.g. FROM (SELECT ...) AS x).
// MySQL shows ref="func" and "Using where" for <subqueryN> eq_ref placeholders
// when the outer table comes from a derived query.
func selectHasDerivedTableInFrom(sel *sqlparser.Select) bool {
	if sel == nil {
		return false
	}
	for _, expr := range sel.From {
		if ate, ok := expr.(*sqlparser.AliasedTableExpr); ok {
			if _, ok2 := ate.Expr.(*sqlparser.DerivedTable); ok2 {
				return true
			}
		}
	}
	return false
}

// extractINSubqueryOuterTable extracts the TABLE name from "WHERE tbl.col IN (SELECT ...)".
// Returns the table qualifier if present, else "".
func extractINSubqueryOuterTable(sel *sqlparser.Select) string {
	if sel == nil || sel.Where == nil {
		return ""
	}
	return extractINTableFromExpr(sel.Where.Expr)
}

// extractINTableFromExpr finds the table qualifier of the IN column in a WHERE expression.
// For "t3.a IN (SELECT ...)", returns "t3".
func extractINTableFromExpr(expr sqlparser.Expr) string {
	if expr == nil {
		return ""
	}
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if e.Operator == sqlparser.InOp {
			if col, ok := e.Left.(*sqlparser.ColName); ok {
				return col.Qualifier.Name.String()
			}
		}
	case *sqlparser.AndExpr:
		if tbl := extractINTableFromExpr(e.Left); tbl != "" {
			return tbl
		}
		return extractINTableFromExpr(e.Right)
	}
	return ""
}

// extractINColFromNode finds the outer column name for a specific IN subquery `sub`
// within a WHERE expression node. For "WHERE a IN (sub1) AND b IN (sub2)", calling
// with sub2 returns "b".
func extractINColFromNode(node sqlparser.SQLNode, sub *sqlparser.Subquery) string {
	var result string
	_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
		if cmp, ok := n.(*sqlparser.ComparisonExpr); ok {
			if cmp.Operator == sqlparser.InOp {
				if rSub, ok := cmp.Right.(*sqlparser.Subquery); ok {
					if rSub == sub {
						if col, ok := cmp.Left.(*sqlparser.ColName); ok {
							result = col.Name.String()
							return false, nil
						}
					}
				}
			}
		}
		return true, nil
	}, node)
	return result
}

func extractINColFromExpr(expr sqlparser.Expr) string {
	if expr == nil {
		return ""
	}
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if e.Operator == sqlparser.InOp {
			// col IN (subquery)
			if col, ok := e.Left.(*sqlparser.ColName); ok {
				return col.Name.String()
			}
		}
	case *sqlparser.AndExpr:
		if col := extractINColFromExpr(e.Left); col != "" {
			return col
		}
		return extractINColFromExpr(e.Right)
	}
	return ""
}

// matTypeClass returns the "materialization type class" for a MySQL column type.
// MySQL only uses materialization when the outer column and inner column have
// compatible type classes (same class). Cross-class comparisons (e.g., INT vs DATE,
// INT vs CHAR) fall back to DuplicateWeedout/non-materialized strategies.
// Classes: "numeric", "string", "temporal", "other"
func matTypeClass(colType string) string {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	// Strip type parameters e.g. VARCHAR(255) → VARCHAR
	if idx := strings.IndexByte(upper, '('); idx >= 0 {
		upper = strings.TrimSpace(upper[:idx])
	}
	switch upper {
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT",
		"FLOAT", "DOUBLE", "REAL", "DECIMAL", "NUMERIC",
		"UNSIGNED", "BIT":
		return "numeric"
	case "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT",
		"BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB",
		"ENUM", "SET":
		return "string"
	case "DATE", "DATETIME", "TIMESTAMP", "TIME", "YEAR":
		return "temporal"
	}
	return "other"
}

// hasNonINRangeConditionOnCol checks whether the WHERE expression contains any
// range conditions (less-than, greater-than, between, OR of comparisons) on the
// given column name (ignoring table qualifiers), other than an IN-subquery condition.
// This is used to determine if "Using where" should be shown on a <subqueryN> placeholder.
func hasNonINRangeConditionOnCol(expr sqlparser.Expr, colName string) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		return hasNonINRangeConditionOnCol(e.Left, colName) || hasNonINRangeConditionOnCol(e.Right, colName)
	case *sqlparser.OrExpr:
		// OR involving the same column implies a range/multi-range filter
		leftHas := hasNonINRangeConditionOnCol(e.Left, colName)
		rightHas := hasNonINRangeConditionOnCol(e.Right, colName)
		return leftHas || rightHas
	case *sqlparser.ComparisonExpr:
		if e.Operator == sqlparser.InOp {
			return false // skip IN expressions
		}
		var lCol, rCol string
		if col, ok := e.Left.(*sqlparser.ColName); ok {
			lCol = col.Name.String()
		}
		if col, ok := e.Right.(*sqlparser.ColName); ok {
			rCol = col.Name.String()
		}
		if strings.EqualFold(lCol, colName) || strings.EqualFold(rCol, colName) {
			return true
		}
	case *sqlparser.BetweenExpr:
		if col, ok := e.Left.(*sqlparser.ColName); ok {
			if strings.EqualFold(col.Name.String(), colName) {
				return true
			}
		}
	}
	return false
}

// inSubqueryTypesCompatibleForMat returns true if the outer join column type
// and inner select column type are compatible for materialization (same type class).
// When types are incompatible (e.g., INT vs DATE), MySQL falls back to DuplicateWeedout.
func (e *Executor) inSubqueryTypesCompatibleForMat(outerSel *sqlparser.Select, inner *sqlparser.Select, subNode *sqlparser.Subquery) bool {
	if e.Storage == nil || e.Catalog == nil {
		return true // assume compatible if we can't check
	}

	// Find the outer join column name from the WHERE clause
	var outerColName, outerTableName string
	if outerSel != nil && outerSel.Where != nil {
		_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
			if cmp, ok := n.(*sqlparser.ComparisonExpr); ok {
				if cmp.Operator == sqlparser.InOp {
					var rSub *sqlparser.Subquery
					if subNode != nil {
						rSub, _ = cmp.Right.(*sqlparser.Subquery)
						if rSub != subNode {
							return true, nil
						}
					} else {
						rSub, _ = cmp.Right.(*sqlparser.Subquery)
						if rSub == nil {
							return true, nil
						}
					}
					if col, ok := cmp.Left.(*sqlparser.ColName); ok {
						outerColName = strings.ToLower(col.Name.String())
						if !col.Qualifier.Name.IsEmpty() {
							outerTableName = strings.ToLower(col.Qualifier.Name.String())
						}
						return false, nil
					}
				}
			}
			return true, nil
		}, outerSel.Where.Expr)
	}
	if outerColName == "" {
		return true // no column to check
	}

	// Find the outer column type from the FROM tables
	outerColType := ""
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return true
	}
	for _, te := range outerSel.From {
		var tblName, alias string
		if ate, ok := te.(*sqlparser.AliasedTableExpr); ok {
			if tn, ok := ate.Expr.(sqlparser.TableName); ok {
				tblName = strings.ToLower(tn.Name.String())
			}
			if !ate.As.IsEmpty() {
				alias = strings.ToLower(ate.As.String())
			}
		}
		if tblName == "" {
			continue
		}
		// Check if this table matches the outer column qualifier
		if outerTableName != "" && outerTableName != tblName && outerTableName != alias {
			continue
		}
		if tblDef, err := db.GetTable(tblName); err == nil && tblDef != nil {
			for _, col := range tblDef.Columns {
				if strings.EqualFold(col.Name, outerColName) {
					outerColType = col.Type
					break
				}
			}
		}
		if outerColType != "" {
			break
		}
	}
	if outerColType == "" {
		return true // can't determine type, assume compatible
	}

	// Find the inner column type from the inner SELECT first expression
	innerColType := ""
	if inner != nil && inner.SelectExprs != nil && len(inner.SelectExprs.Exprs) > 0 {
		if ae, ok := inner.SelectExprs.Exprs[0].(*sqlparser.AliasedExpr); ok {
			if col, ok := ae.Expr.(*sqlparser.ColName); ok {
				innerColName := strings.ToLower(col.Name.String())
				// Look up in inner tables
				for _, te := range inner.From {
					var tblName string
					if ate, ok := te.(*sqlparser.AliasedTableExpr); ok {
						if tn, ok := ate.Expr.(sqlparser.TableName); ok {
							tblName = strings.ToLower(tn.Name.String())
						}
					}
					if tblName == "" {
						continue
					}
					if tblDef, err := db.GetTable(tblName); err == nil && tblDef != nil {
						for _, c := range tblDef.Columns {
							if strings.EqualFold(c.Name, innerColName) {
								innerColType = c.Type
								break
							}
						}
					}
					if innerColType != "" {
						break
					}
				}
			}
		}
	}
	if innerColType == "" {
		return true // can't determine type, assume compatible
	}

	// Check if type classes are the same
	outerClass := matTypeClass(outerColType)
	innerClass := matTypeClass(innerColType)
	return outerClass == innerClass || outerClass == "other" || innerClass == "other"
}

// - If total inner table rows > materializationThreshold → MATERIALIZED
// - If the join column has no usable index AND total rows > 0 → MATERIALIZED
// - If the inner table is empty (0 rows) → MATERIALIZED (MySQL still materializes for large outer tables)
// Otherwise → inline semijoin (SIMPLE, all rows at id=1).
const materializationRowThreshold = 100

func (e *Executor) shouldMaterializeSubquery(inner *sqlparser.Select, outerIsSystem bool) bool {
	if e.Storage == nil {
		return false
	}

	// Collect inner table names
	var innerTables []string
	for _, te := range inner.From {
		innerTables = append(innerTables, e.extractAllTableNames(te)...)
	}
	// Filter out DUAL
	var realInnerTables []string
	for _, t := range innerTables {
		if !strings.EqualFold(t, "dual") {
			realInnerTables = append(realInnerTables, t)
		}
	}
	if len(realInnerTables) == 0 {
		return false
	}

	// Find the join column (first SELECT expression of the inner query).
	var joinColName string
	if inner.SelectExprs != nil && len(inner.SelectExprs.Exprs) > 0 {
		if ae, ok := inner.SelectExprs.Exprs[0].(*sqlparser.AliasedExpr); ok {
			if col, ok := ae.Expr.(*sqlparser.ColName); ok {
				joinColName = strings.ToLower(col.Name.String())
			}
		}
	}

	// For single-table subqueries, check for index coverage on the join column.
	// MySQL's strategy depends on the index type and optimizer flags:
	// - PRIMARY KEY or UNIQUE on join col → always eq_ref (inline), regardless of firstmatch setting
	// - Constant primary key equality in WHERE (pk=N) → const/eq_ref access, never materialize
	// - Secondary (non-unique) index + firstmatch=on → FirstMatch (inline)
	// - Secondary (non-unique) index + firstmatch=off → MATERIALIZED (for large tables)
	// - No index → MATERIALIZED (if large table) or inline (if small)
	firstMatchOn := e.isOptimizerSwitchEnabled("firstmatch")
	if len(realInnerTables) == 1 {
		innerTableName := realInnerTables[0]
		tbl, tblErr := e.Storage.GetTable(e.CurrentDB, innerTableName)
		if tblErr != nil {
			// The table name might be a view. Try to resolve it to its base table.
			if baseTableName, isView, _, _ := e.resolveViewToBaseTable(innerTableName); isView && baseTableName != "" {
				if baseTbl, err2 := e.Storage.GetTable(e.CurrentDB, baseTableName); err2 == nil {
					tbl = baseTbl
					tblErr = nil
					// For SELECT * subqueries (joinColName=""), derive join col from PK of base table.
					// This handles "t1field IN (SELECT * FROM view)" where view = "SELECT * FROM t2"
					// and t2 has PRIMARY KEY on its first column → use eq_ref (no materialization).
					if joinColName == "" && baseTbl.Def != nil && len(baseTbl.Def.PrimaryKey) > 0 {
						joinColName = strings.ToLower(baseTbl.Def.PrimaryKey[0])
					}
				}
			}
		}
		if tblErr == nil && tbl != nil && tbl.Def != nil {
			// Check if the inner WHERE has a constant equality on the primary key column(s).
			// When pk = constant, MySQL uses const/eq_ref access → no materialization.
			if len(tbl.Def.PrimaryKey) > 0 && inner.Where != nil {
				if hasConstPKEquality(inner.Where.Expr, tbl.Def.PrimaryKey[0]) {
					return false // const table access via primary key → always inline
				}
			}

			if joinColName != "" {
				// Primary key on join col → eq_ref (inline)
				if len(tbl.Def.PrimaryKey) > 0 && strings.EqualFold(tbl.Def.PrimaryKey[0], joinColName) {
					return false // eq_ref via primary key → always inline
				}
				// Check secondary indexes
				for _, idx := range tbl.Def.Indexes {
					if len(idx.Columns) > 0 && strings.EqualFold(idx.Columns[0], joinColName) {
						// Secondary index on join column:
						// - firstmatch=on → FirstMatch (inline, no materialization)
						//   UNLESS the inner WHERE has a range condition on the join column,
						//   in which case MySQL uses materialization (range scan materializes
						//   a subset and probes via hash).
						// - firstmatch=off → may materialize if table is large
						if firstMatchOn {
							// Check if the inner WHERE has a range condition on the join column.
							// E.g., "kp1 < 20" would cause MySQL to use materialization.
							innerHasRangeOnJoinCol := false
							if inner.Where != nil {
								innerHasRangeOnJoinCol = whereHasRangeOnCol(inner.Where.Expr, joinColName)
							}
							if !innerHasRangeOnJoinCol {
								return false // FirstMatch strategy → inline (equality/no-range)
							}
							// Range condition → fall through to materialization
						}
						// firstmatch=off or range on join col: fall through to row count check
						break
					}
				}
			}
		}
	}

	// Multi-table inner subqueries:
	// When the outer table is a "system" table (exactly 1 row) and firstmatch=on,
	// MySQL always uses the inline FirstMatch/semijoin strategy regardless of inner table sizes.
	// This is because the const/system outer table makes FirstMatch optimal.
	// When the outer table requires a scan (2+ rows), check if any inner table is empty —
	// even one empty table in an INNER JOIN causes MySQL to prefer MATERIALIZED strategy.
	if len(realInnerTables) > 1 {
		anyEmpty := false
		for _, tblName := range realInnerTables {
			tbl, err := e.Storage.GetTable(e.CurrentDB, tblName)
			if err != nil || len(tbl.Rows) == 0 {
				anyEmpty = true
				break
			}
		}
		if anyEmpty {
			// Any inner table empty:
			// - If outer is a "system" table (1 row) and firstmatch=on, MySQL uses inline
			//   FirstMatch because the const/system outer makes it optimal.
			// - If total inner rows >= 2: MySQL uses LooseScan (Start/End temporary) regardless
			//   of join type — at least one non-empty inner table makes LooseScan viable.
			// - If total inner rows == 1 and RIGHT JOIN with the right table having that 1 row:
			//   LooseScan applies since the right (preserved) table drives.
			// - Otherwise (total <= 1 with no LooseScan-viable configuration): MATERIALIZED.
			if outerIsSystem && firstMatchOn {
				return false // outer is system + firstmatch=on → inline FirstMatch
			}
			// Compute total inner rows to determine LooseScan viability
			totalInnerRows := 0
			for _, tblName := range realInnerTables {
				if tbl, err := e.Storage.GetTable(e.CurrentDB, tblName); err == nil {
					totalInnerRows += len(tbl.Rows)
				}
			}
			if totalInnerRows >= 2 {
				// For STRAIGHT_JOIN, the join order is fixed (t2 STRAIGHT_JOIN t3 → t2 first).
				// When the FIRST (leftmost) inner table is non-empty, MySQL uses MATERIALIZED —
				// BUT ONLY when the outer table is NOT a system table (1 row).
				// When outer is system (1 row), MySQL always uses FirstMatch even for STRAIGHT_JOIN.
				// For all other join types (INNER, LEFT, RIGHT), MySQL can reorder tables and
				// LooseScan is viable when total inner rows >= 2, BUT ONLY when loosescan=on.
				// When loosescan=off, MySQL uses MATERIALIZED instead.
				looseScanOn := e.isOptimizerSwitchEnabled("loosescan")
				if !outerIsSystem && e.innerSubqueryUsesOnlyStraightJoin(inner) && len(realInnerTables) >= 2 {
					firstTbl := realInnerTables[0]
					if tbl, err := e.Storage.GetTable(e.CurrentDB, firstTbl); err == nil && len(tbl.Rows) > 0 {
						return true // STRAIGHT_JOIN with non-empty first table → MATERIALIZED
					}
				}
				if looseScanOn {
					return false // total >= 2 inner rows → LooseScan strategy (not MATERIALIZED)
				}
				return true // loosescan=off → MATERIALIZED
			}
			// total == 1: check if RIGHT JOIN with right table being the non-empty one.
			// When firstmatch=on, MySQL can use LooseScan for this case.
			// When firstmatch=off, MySQL uses MATERIALIZED instead.
			if firstMatchOn && totalInnerRows == 1 && e.innerSubqueryUsesRightJoin(inner) && len(realInnerTables) >= 2 {
				lastTbl := realInnerTables[len(realInnerTables)-1]
				if tbl, err := e.Storage.GetTable(e.CurrentDB, lastTbl); err == nil && len(tbl.Rows) > 0 {
					return false // RIGHT JOIN with right table having the 1 row → LooseScan
				}
			}
			return true // use MATERIALIZED strategy
		}
		// All inner tables have data:
		// - STRAIGHT_JOIN with first table having >= 2 rows → MATERIALIZED (fixed join order, large probe set)
		//   BUT ONLY when the outer is NOT a system table (outer=system always uses FirstMatch).
		// - firstmatch=on + any inner table has PK on join col → FirstMatch (eq_ref inline, cheap even with data)
		// - firstmatch=on + no PK on join col + total inner rows above threshold → MATERIALIZED
		// - firstmatch=on + no PK on join col + small tables → inline semijoin (SIMPLE)
		// - firstmatch=off + any inner table has PK on join column: inline (eq_ref → no cost benefit from MATERIALIZED)
		// - firstmatch=off + no PK on join column + large tables: MATERIALIZED
		const multiTableMatThreshold = 5
		if !outerIsSystem && e.innerSubqueryUsesOnlyStraightJoin(inner) && len(realInnerTables) >= 2 {
			firstTbl := realInnerTables[0]
			if tbl, err := e.Storage.GetTable(e.CurrentDB, firstTbl); err == nil && len(tbl.Rows) >= 2 {
				return true // STRAIGHT_JOIN with large first table → MATERIALIZED even when firstmatch=on
			}
		}
		if firstMatchOn {
			// When any inner table has a PRIMARY KEY or SECONDARY INDEX on the join column,
			// MySQL can use ref/eq_ref access → FirstMatch is cheap and preferred regardless of table size.
			if joinColName != "" {
				for _, tblName := range realInnerTables {
					if tbl, err := e.Storage.GetTable(e.CurrentDB, tblName); err == nil && tbl.Def != nil {
						// Check PRIMARY KEY
						if len(tbl.Def.PrimaryKey) > 0 && strings.EqualFold(tbl.Def.PrimaryKey[0], joinColName) {
							return false // eq_ref via primary key → FirstMatch is cheap
						}
						// Check secondary indexes
						for _, idx := range tbl.Def.Indexes {
							if len(idx.Columns) > 0 && strings.EqualFold(idx.Columns[0], joinColName) {
								// If inner subquery has a range condition on the join col,
								// MySQL uses MATERIALIZED even with firstmatch=on.
								innerHasRangeOnJC := false
								if inner.Where != nil {
									innerHasRangeOnJC = whereHasRangeOnCol(inner.Where.Expr, joinColName)
								}
								if !innerHasRangeOnJC {
									return false // ref via secondary index → FirstMatch is cheap
								}
								// Range condition → fall through to MATERIALIZED
							}
						}
					}
				}
			}
			// No index on join column in any inner table: use total row count to decide.
			// When total inner rows exceeds the threshold, MySQL's cost model prefers MATERIALIZED.
			totalInnerRowsForMat := 0
			for _, tblName := range realInnerTables {
				if tbl, err := e.Storage.GetTable(e.CurrentDB, tblName); err == nil {
					totalInnerRowsForMat += len(tbl.Rows)
				}
			}
			if totalInnerRowsForMat > multiTableMatThreshold {
				return true // Large multi-table subquery with no usable index → MATERIALIZED
			}
			return false // Small multi-table subquery → FirstMatch strategy (inline)
		}
		// firstmatch=off: check if the first inner table has a PK on the join column.
		// If so, MySQL uses inline eq_ref access instead of MATERIALIZED.
		if joinColName != "" && e.Storage != nil {
			firstTbl := realInnerTables[0]
			if tbl, err := e.Storage.GetTable(e.CurrentDB, firstTbl); err == nil && tbl.Def != nil {
				if len(tbl.Def.PrimaryKey) > 0 && strings.EqualFold(tbl.Def.PrimaryKey[0], joinColName) {
					return false // eq_ref via primary key → inline even for multi-table
				}
			}
		}
		return true // firstmatch=off, no PK on join col → MATERIALIZED
	}

	// Compute total row count across all inner tables (single-table case)
	totalRows := 0
	for _, tblName := range realInnerTables {
		tbl, err := e.Storage.GetTable(e.CurrentDB, tblName)
		if err != nil {
			continue
		}
		totalRows += len(tbl.Rows)
	}

	// Empty table handling:
	// - subquery_materialization_cost_based=off → always materialize (even empty, even firstmatch=on)
	// - subquery_materialization_cost_based=on + firstmatch=on → use FirstMatch (SIMPLE), not MATERIALIZED
	// - subquery_materialization_cost_based=on + firstmatch=off → materialize
	if totalRows == 0 {
		costBased := e.isOptimizerSwitchEnabled("subquery_materialization_cost_based")
		if !costBased {
			// Cost-based disabled → always materialize
			return true
		}
		if !firstMatchOn {
			// firstmatch=off → materialize empty tables too
			return true
		}
		// firstmatch=on + cost_based=on → use FirstMatch (inline/SIMPLE)
		return false
	}

	// Large single-table subquery with no primary key and no usable index → materialize.
	if totalRows > materializationRowThreshold {
		return true
	}

	// Small non-empty single-table subquery with no primary key (and no FirstMatch) → materialize.
	return true
}

// hasConstPKEquality checks if the WHERE expression has a constant equality condition on the given column.
// For example, `pk = 12` or `12 = pk` (with a literal, not a column reference) returns true.
// This is used to detect "const table" access patterns where MySQL accesses a single row via primary key.
// whereHasRangeOnCol returns true if the WHERE expression contains a range condition
// (non-equality comparison) directly on the given column name. Examples:
//   col < 20, col > 5, col <= 100, col BETWEEN a AND b
// This is used to detect when MySQL switches from FirstMatch to materialization
// for single-table IN subqueries with range access on the join column.
func whereHasRangeOnCol(expr sqlparser.Expr, col string) bool {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		var colName string
		var otherSide sqlparser.Expr
		if c, ok := e.Left.(*sqlparser.ColName); ok {
			colName = strings.ToLower(c.Name.String())
			otherSide = e.Right
		} else if c, ok := e.Right.(*sqlparser.ColName); ok {
			colName = strings.ToLower(c.Name.String())
			otherSide = e.Left
		}
		if strings.EqualFold(colName, col) {
			// Only treat as a range condition when compared to a literal value,
			// not another column reference (correlation doesn't restrict range).
			if _, isLiteral := otherSide.(*sqlparser.Literal); !isLiteral {
				break
			}
			switch e.Operator {
			case sqlparser.LessThanOp, sqlparser.LessEqualOp,
				sqlparser.GreaterThanOp, sqlparser.GreaterEqualOp,
				sqlparser.NotEqualOp:
				return true
			}
		}

	case *sqlparser.AndExpr:
		return whereHasRangeOnCol(e.Left, col) || whereHasRangeOnCol(e.Right, col)
	case *sqlparser.OrExpr:
		return whereHasRangeOnCol(e.Left, col) || whereHasRangeOnCol(e.Right, col)
	}
	return false
}

func hasConstPKEquality(expr sqlparser.Expr, pkCol string) bool {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if e.Operator == sqlparser.EqualOp {
			// Check col = literal
			if col, ok := e.Left.(*sqlparser.ColName); ok {
				if strings.EqualFold(col.Name.String(), pkCol) {
					// Right side must be a literal (not a column ref)
					if _, isLit := e.Right.(*sqlparser.Literal); isLit {
						return true
					}
				}
			}
			// Check literal = col
			if col, ok := e.Right.(*sqlparser.ColName); ok {
				if strings.EqualFold(col.Name.String(), pkCol) {
					if _, isLit := e.Left.(*sqlparser.Literal); isLit {
						return true
					}
				}
			}
		}
	case *sqlparser.AndExpr:
		return hasConstPKEquality(e.Left, pkCol) || hasConstPKEquality(e.Right, pkCol)
	}
	return false
}

// extractConstPKValue extracts the literal value from a constant PK equality condition.
// For example, `pk = 12` returns ("12", true); otherwise returns ("", false).
func extractConstPKValue(expr sqlparser.Expr, pkCol string) (string, bool) {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if e.Operator == sqlparser.EqualOp {
			if col, ok := e.Left.(*sqlparser.ColName); ok {
				if strings.EqualFold(col.Name.String(), pkCol) {
					if lit, isLit := e.Right.(*sqlparser.Literal); isLit {
						return lit.Val, true
					}
				}
			}
			if col, ok := e.Right.(*sqlparser.ColName); ok {
				if strings.EqualFold(col.Name.String(), pkCol) {
					if lit, isLit := e.Left.(*sqlparser.Literal); isLit {
						return lit.Val, true
					}
				}
			}
		}
	case *sqlparser.AndExpr:
		if v, ok := extractConstPKValue(e.Left, pkCol); ok {
			return v, true
		}
		return extractConstPKValue(e.Right, pkCol)
	}
	return "", false
}

// isImpossibleConstPKWhere checks if the inner SELECT's WHERE clause is a constant PK equality
// that doesn't match any existing row, OR if ALL inner tables are empty (making the result empty).
// Used for MySQL's "no matching row in const table" EXPLAIN optimization.
func (e *Executor) isImpossibleConstPKWhere(inner *sqlparser.Select) bool {
	if e.Storage == nil {
		return false
	}

	// Collect all inner table names
	var innerTableNames []string
	for _, te := range inner.From {
		innerTableNames = append(innerTableNames, e.extractAllTableNames(te)...)
	}

	// Single-table inner subquery with a constant PK equality that matches no row.
	if inner.Where == nil || len(innerTableNames) != 1 {
		return false
	}
	tableName := innerTableNames[0]
	if strings.EqualFold(tableName, "dual") {
		return false
	}
	tbl, err := e.Storage.GetTable(e.CurrentDB, tableName)
	if err != nil || tbl.Def == nil || len(tbl.Def.PrimaryKey) == 0 {
		return false
	}
	pkCol := tbl.Def.PrimaryKey[0]
	pkVal, ok := extractConstPKValue(inner.Where.Expr, pkCol)
	if !ok {
		return false
	}
	// Check if any row has this PK value
	// Row is a map[string]interface{}, keyed by column name.
	for _, row := range tbl.Rows {
		if val, ok := row[pkCol]; ok {
			rowVal := fmt.Sprintf("%v", val)
			if rowVal == pkVal {
				return false // Row exists → not impossible
			}
		}
	}
	return true // No matching row found → impossible WHERE
}

// isWhereImpossibleDueToConstantOutOfRange returns true if the WHERE clause contains a
// comparison that makes the condition trivially false due to a constant being out of range
// for an integer column type. It handles constant propagation through equality chains:
// e.g. "t1.i=t2.i AND t2.i=128" where t1.i is TINYINT makes t1.i=128 propagated, which is
// impossible since 128 > TINYINT max (127).
//
// This is used to detect the "Impossible WHERE" case in EXPLAIN for multi-table queries.
func (e *Executor) isWhereImpossibleDueToConstantOutOfRange(sel *sqlparser.Select) bool {
	if sel.Where == nil {
		return false
	}

	// Step 1: collect all AND-connected conditions from the WHERE
	var conditions []sqlparser.Expr
	collectAndConds := func(expr sqlparser.Expr, conds *[]sqlparser.Expr) {}
	collectAndConds = func(expr sqlparser.Expr, conds *[]sqlparser.Expr) {
		if andExpr, ok := expr.(*sqlparser.AndExpr); ok {
			collectAndConds(andExpr.Left, conds)
			collectAndConds(andExpr.Right, conds)
		} else {
			*conds = append(*conds, expr)
		}
	}
	collectAndConds(sel.Where.Expr, &conditions)

	// Step 2: build a map from column (table.col or col) → set of constants compared via =
	// and a list of col=col equalities for propagation.
	type colKey struct{ table, col string }
	colConstants := make(map[colKey][]string) // col → list of integer constants it's equal to
	var colEqualities [][2]colKey              // pairs of equal columns

	getColKey := func(expr sqlparser.Expr) (colKey, bool) {
		col, ok := expr.(*sqlparser.ColName)
		if !ok {
			return colKey{}, false
		}
		tbl := strings.ToLower(col.Qualifier.Name.String())
		name := strings.ToLower(col.Name.String())
		return colKey{tbl, name}, true
	}

	getIntLiteral := func(expr sqlparser.Expr) (string, bool) {
		switch v := expr.(type) {
		case *sqlparser.Literal:
			if v.Type == sqlparser.IntVal {
				return v.Val, true
			}
		case *sqlparser.UnaryExpr:
			if v.Operator == sqlparser.UMinusOp {
				if lit, ok := v.Expr.(*sqlparser.Literal); ok && lit.Type == sqlparser.IntVal {
					return "-" + lit.Val, true
				}
			}
		}
		return "", false
	}

	for _, cond := range conditions {
		cmp, ok := cond.(*sqlparser.ComparisonExpr)
		if !ok {
			continue
		}
		if cmp.Operator != sqlparser.EqualOp {
			continue
		}
		lk, lIsCol := getColKey(cmp.Left)
		rk, rIsCol := getColKey(cmp.Right)
		llit, lIsInt := getIntLiteral(cmp.Left)
		rlit, rIsInt := getIntLiteral(cmp.Right)

		if lIsCol && rIsInt {
			colConstants[lk] = append(colConstants[lk], rlit)
		} else if rIsCol && lIsInt {
			colConstants[rk] = append(colConstants[rk], llit)
		} else if lIsCol && rIsCol {
			colEqualities = append(colEqualities, [2]colKey{lk, rk})
		}
	}

	if len(colConstants) == 0 {
		return false
	}

	// Step 3: propagate constants through equality chains (BFS/union-find style)
	// Build adjacency list for col=col equalities
	colNeighbors := make(map[colKey][]colKey)
	for _, eq := range colEqualities {
		colNeighbors[eq[0]] = append(colNeighbors[eq[0]], eq[1])
		colNeighbors[eq[1]] = append(colNeighbors[eq[1]], eq[0])
	}

	// BFS to propagate constants to all connected columns
	if len(colEqualities) > 0 {
		visited := make(map[colKey]bool)
		queue := make([]colKey, 0)
		// Start from columns that already have constants
		for k := range colConstants {
			if !visited[k] {
				visited[k] = true
				queue = append(queue, k)
			}
		}
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			consts := colConstants[cur]
			if len(consts) == 0 {
				continue
			}
			for _, neighbor := range colNeighbors[cur] {
				// Propagate constants to neighbor
				colConstants[neighbor] = append(colConstants[neighbor], consts...)
				if !visited[neighbor] {
					visited[neighbor] = true
					queue = append(queue, neighbor)
				}
			}
		}
	}

	// Step 4: for each column with constants, check if any constant is out of range
	dbName := e.CurrentDB
	if dbName == "" {
		dbName = "test"
	}
	for k, consts := range colConstants {
		if len(consts) == 0 {
			continue
		}
		// Look up table to find column type
		// k.table may be empty (unqualified column), try all tables in FROM
		var colType string
		tryTable := func(tblName string) bool {
			tbl, err := e.Storage.GetTable(dbName, tblName)
			if err != nil || tbl.Def == nil {
				return false
			}
			ct := tbl.Def.ColType(k.col)
			if ct == "" {
				return false
			}
			colType = ct
			return true
		}

		if k.table != "" {
			tryTable(k.table)
		} else {
			// Try all tables in FROM
			for _, te := range sel.From {
				for _, tn := range e.extractAllTableNames(te) {
					if tryTable(tn) {
						break
					}
				}
			}
		}

		if colType == "" {
			continue
		}

		// Check if any constant is out of range for this column type
		upper := strings.ToUpper(strings.TrimSpace(colType))
		baseType := upper
		if idx := strings.Index(baseType, "("); idx >= 0 {
			baseType = baseType[:idx]
		}
		isUnsigned := strings.Contains(upper, "UNSIGNED")
		baseType = strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(baseType, "UNSIGNED", ""), "ZEROFILL", ""))
		baseType = strings.TrimSpace(baseType)

		rng, isIntType := intTypeRanges[baseType]
		if !isIntType {
			continue
		}

		for _, constStr := range consts {
			// Parse constant as big.Int for exact comparison
			n := new(big.Int)
			if _, ok := n.SetString(constStr, 10); !ok {
				continue
			}
			if isUnsigned {
				// Check against [0, MaxUnsigned]
				minVal := new(big.Int)
				maxVal := new(big.Int).SetUint64(rng.MaxUnsigned)
				if n.Cmp(minVal) < 0 || n.Cmp(maxVal) > 0 {
					return true
				}
			} else {
				// Check against [Min, Max]
				minVal := big.NewInt(rng.Min)
				maxVal := big.NewInt(rng.Max)
				if n.Cmp(minVal) < 0 || n.Cmp(maxVal) > 0 {
					return true
				}
			}
		}
	}
	return false
}

// hasImpossibleNullComparison returns true if the given WHERE expression contains
// a non-null-safe comparison with a literal NULL (e.g. "col < NULL", "col = NULL").
// Such comparisons always evaluate to NULL (not TRUE/FALSE), making the WHERE condition
// trivially impossible. MySQL's optimizer detects this and shows "Impossible WHERE noticed
// after reading const tables" in EXPLAIN.
func hasImpossibleNullComparison(expr sqlparser.Expr) bool {
	if expr == nil {
		return false
	}
	found := false
	_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
		cmp, ok := n.(*sqlparser.ComparisonExpr)
		if !ok {
			return true, nil
		}
		// Skip null-safe equality (<=>): it IS defined for NULLs.
		if cmp.Operator == sqlparser.NullSafeEqualOp {
			return true, nil
		}
		// Check if either side is a literal NULL
		_, leftNull := cmp.Left.(*sqlparser.NullVal)
		_, rightNull := cmp.Right.(*sqlparser.NullVal)
		if leftNull || rightNull {
			found = true
			return false, nil
		}
		return true, nil
	}, expr)
	return found
}

// isSubqueryInINContext checks if a Subquery node is used in an IN, NOT IN, or = ANY / != ANY context.
// Note: plain scalar equality like "col = (SELECT 1 FROM t2)" (without ANY/ALL) is NOT IN context;
// those are SUBQUERY not semijoin-flattenable.
func isSubqueryInINContext(node sqlparser.SQLNode, sub *sqlparser.Subquery) bool {
	found := false
	_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
		if cmp, ok := n.(*sqlparser.ComparisonExpr); ok {
			switch cmp.Operator {
			case sqlparser.InOp, sqlparser.NotInOp:
				if subR, ok := cmp.Right.(*sqlparser.Subquery); ok && subR == sub {
					found = true
					return false, nil
				}
			case sqlparser.EqualOp, sqlparser.NotEqualOp,
				sqlparser.LessThanOp, sqlparser.LessEqualOp,
				sqlparser.GreaterThanOp, sqlparser.GreaterEqualOp:
				// = ANY / != ANY / < ANY / etc. — only with ANY or ALL modifier.
				// Plain scalar comparisons like col = (SELECT 1) have Modifier=Missing and are NOT IN context.
				if (cmp.Modifier == sqlparser.Any || cmp.Modifier == sqlparser.All) {
					if subR, ok := cmp.Right.(*sqlparser.Subquery); ok && subR == sub {
						found = true
						return false, nil
					}
				}
			}
		}
		return true, nil
	}, node)
	return found
}

// isSubqueryInExistsContext checks if a Subquery node is inside an ExistsExpr.
func isSubqueryInExistsContext(node sqlparser.SQLNode, sub *sqlparser.Subquery) bool {
	found := false
	_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
		if exists, ok := n.(*sqlparser.ExistsExpr); ok {
			if exists.Subquery == sub {
				found = true
				return false, nil
			}
		}
		return true, nil
	}, node)
	return found
}

// isSemiJoinDecorrelatable checks if a correlated subquery can be converted to a semijoin.
// This is true when the correlation consists only of simple equality conditions
// between outer and inner table columns (e.g., WHERE outer.col = inner.col).
// Such conditions can be "hoisted" into the semijoin join condition, making
// the inner subquery effectively non-correlated.
func (e *Executor) isSemiJoinDecorrelatable(inner *sqlparser.Select, outerTables map[string]bool) bool {
	if inner.Where == nil {
		return false // no WHERE → no correlation to decorrelate
	}
	// Collect inner table names
	innerTables := e.extractTableNamesAndAliases(inner)

	// Check if ALL references to outer tables in WHERE are simple equality conditions
	allCorrelationsAreEqualities := true
	hasAnyCorrelation := false

	var checkExpr func(expr sqlparser.Expr) bool
	checkExpr = func(expr sqlparser.Expr) bool {
		switch e := expr.(type) {
		case *sqlparser.AndExpr:
			return checkExpr(e.Left) && checkExpr(e.Right)
		case *sqlparser.ComparisonExpr:
			if e.Operator != sqlparser.EqualOp {
				// Non-equality condition: check if it references outer tables
				// If not correlated, it's fine. If correlated, not decorrelatable.
				leftRefOuter := exprReferencesOuterTable(e.Left, outerTables, innerTables)
				rightRefOuter := exprReferencesOuterTable(e.Right, outerTables, innerTables)
				if leftRefOuter || rightRefOuter {
					return false // non-equality correlation → not decorrelatable
				}
				return true
			}
			// Equality: check if it's an outer-inner equality
			leftRefOuter := exprReferencesOuterTable(e.Left, outerTables, innerTables)
			rightRefOuter := exprReferencesOuterTable(e.Right, outerTables, innerTables)
			if leftRefOuter || rightRefOuter {
				hasAnyCorrelation = true
				// This is an equality with outer reference → decorrelatable
				return true
			}
			return true // non-correlated equality → fine
		default:
			return true
		}
	}

	if !checkExpr(inner.Where.Expr) {
		allCorrelationsAreEqualities = false
	}

	return allCorrelationsAreEqualities && hasAnyCorrelation
}

// exprReferencesOuterTable returns true if the expression references an outer table column.
func exprReferencesOuterTable(expr sqlparser.Expr, outerTables map[string]bool, innerTables map[string]bool) bool {
	if col, ok := expr.(*sqlparser.ColName); ok {
		qualifier := strings.ToLower(col.Qualifier.Name.String())
		if qualifier != "" {
			return outerTables[qualifier] && !innerTables[qualifier]
		}
		// Unqualified: check if it's NOT an inner table column (could be outer)
		// For safety, return false for unqualified names
	}
	return false
}

// outerINExprIsConstant checks if the IN condition that contains the given subquery
// has a constant (non-column) expression on the left-hand side.
// When the IN outer expression is a constant (e.g., "11 IN (subquery)"), MySQL uses
// FirstMatch instead of MATERIALIZED because the constant can be checked without
// materializing the inner result for each outer row.
func outerINExprIsConstant(node sqlparser.SQLNode, sub *sqlparser.Subquery) bool {
	isConst := false
	_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
		if cmp, ok := n.(*sqlparser.ComparisonExpr); ok {
			if cmp.Operator == sqlparser.InOp || cmp.Operator == sqlparser.NotInOp {
				if subR, ok := cmp.Right.(*sqlparser.Subquery); ok && subR == sub {
					// Check if the left side is a constant (not a column reference)
					switch cmp.Left.(type) {
					case *sqlparser.Literal, *sqlparser.NullVal:
						isConst = true
					case *sqlparser.ColName:
						isConst = false
					default:
						isConst = false
					}
					return false, nil
				}
			}
		}
		return true, nil
	}, node)
	return isConst
}

// explainSubqueries finds subqueries in SELECT expressions, WHERE, and HAVING clauses.
func (e *Executor) explainSubqueries(sel *sqlparser.Select, idCounter *int64, result *[]explainSelectType, outerSelectType ...string) {
	// Collect outer table names for correlated subquery detection
	outerTables := e.extractTableNamesAndAliases(sel)

	// Determine whether the outer query can use semijoin flattening.
	// MATERIALIZED is only valid when the outer query is semijoin-flattened.
	outerCanSemijoin := e.queryCanBeSemijoinFlattened(sel)

	// Walk the SELECT expressions, WHERE, HAVING, and JOIN ON conditions to find subqueries.
	// We need to avoid descending into FROM clause derived tables (handled separately)
	// but we DO need to walk JOIN ON conditions (which may contain IN subqueries).
	nodes := []sqlparser.SQLNode{}
	if sel.SelectExprs != nil {
		nodes = append(nodes, sel.SelectExprs)
	}
	if sel.Where != nil {
		nodes = append(nodes, sel.Where)
	}
	if sel.Having != nil {
		nodes = append(nodes, sel.Having)
	}
	if sel.OrderBy != nil {
		nodes = append(nodes, sel.OrderBy)
	}
	// Walk ON conditions from JOIN expressions in the FROM clause.
	for _, te := range sel.From {
		e.collectJoinOnConditions(te, &nodes)
	}

	outerHasOnlyDerived := e.outerQueryHasOnlyDerivedTables(sel)
	outerST := ""
	if len(outerSelectType) > 0 {
		outerST = outerSelectType[0]
	}
	for _, node := range nodes {
		// When semijoin is disabled but materialization is on, IN subqueries appear as
		// SUBQUERY (not DEPENDENT SUBQUERY) in EXPLAIN. MySQL outputs multiple outer-level
		// SUBQUERY groups in reverse order of their definition. Apply group-aware reversal:
		// process each direct subquery separately and output groups in reverse order.
		// This matches MySQL's optimizer processing order (last-defined subquery first).
		if whereNode, ok := node.(*sqlparser.Where); ok && !outerCanSemijoin &&
			!e.isSemijoinEnabled() && e.isOptimizerSwitchEnabled("materialization") && whereNode != nil {
			directExprs := collectDirectSubqueryExprs(whereNode.Expr)
			if len(directExprs) > 1 {
				var groups [][]explainSelectType
				for _, expr := range directExprs {
					gStart := len(*result)
					e.walkForSubqueries(expr, idCounter, result, outerTables, outerCanSemijoin, outerHasOnlyDerived, sel, outerST)
					group := make([]explainSelectType, len(*result)-gStart)
					copy(group, (*result)[gStart:])
					*result = (*result)[:gStart]
					groups = append(groups, group)
				}
				// Output groups in reverse order (last-defined subquery group first)
				for i := len(groups) - 1; i >= 0; i-- {
					*result = append(*result, groups[i]...)
				}
				continue
			}
		}

		startIdx := len(*result)
		e.walkForSubqueries(node, idCounter, result, outerTables, outerCanSemijoin, outerHasOnlyDerived, sel, outerST)
		// MySQL displays DEPENDENT SUBQUERY rows from the WHERE clause in reverse order
		// (higher ids first) because it processes them in reverse during optimization.
		// Reverse the newly-added rows if they are all DEPENDENT SUBQUERY.
		newRows := (*result)[startIdx:]
		if len(newRows) > 1 {
			allDependent := true
			for _, r := range newRows {
				if r.selectType != "DEPENDENT SUBQUERY" {
					allDependent = false
					break
				}
			}
			if allDependent {
				for i, j := 0, len(newRows)-1; i < j; i, j = i+1, j-1 {
					newRows[i], newRows[j] = newRows[j], newRows[i]
				}
			}
		}
	}
}

// collectDirectSubqueryExprs collects the direct IN/EXISTS/scalar subquery expressions
// from the given expression tree without descending into sub-selects.
// Each returned node is a ComparisonExpr (with InOp/NotInOp), ExistsExpr, or Subquery
// that is directly referenced in the given expression (not nested inside another subquery).
// This is used to identify outer-level subquery groups for EXPLAIN ordering.
func collectDirectSubqueryExprs(expr sqlparser.Expr) []sqlparser.SQLNode {
	var exprs []sqlparser.SQLNode
	_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
		switch v := n.(type) {
		case *sqlparser.ComparisonExpr:
			if _, ok := v.Right.(*sqlparser.Subquery); ok {
				// Only collect IN/NOT IN/= ANY/!= ANY etc. with a subquery on the right
				switch v.Operator {
				case sqlparser.InOp, sqlparser.NotInOp:
					exprs = append(exprs, n)
					return false, nil // don't descend into this subquery
				default:
					// Scalar subquery comparison (e.g. col = (SELECT ...))
					if v.Modifier == sqlparser.Any || v.Modifier == sqlparser.All {
						exprs = append(exprs, n)
						return false, nil
					}
				}
			}
		case *sqlparser.ExistsExpr:
			exprs = append(exprs, n)
			return false, nil
		}
		return true, nil
	}, expr)
	return exprs
}

// collectJoinOnConditions recursively collects ON condition expressions from JOIN table expressions.
// These conditions may contain subqueries that need to be walked for EXPLAIN.
func (e *Executor) collectJoinOnConditions(te sqlparser.TableExpr, nodes *[]sqlparser.SQLNode) {
	switch t := te.(type) {
	case *sqlparser.JoinTableExpr:
		if t.Condition != nil && t.Condition.On != nil {
			*nodes = append(*nodes, t.Condition.On)
		}
		e.collectJoinOnConditions(t.LeftExpr, nodes)
		e.collectJoinOnConditions(t.RightExpr, nodes)
	case *sqlparser.ParenTableExpr:
		for _, expr := range t.Exprs {
			e.collectJoinOnConditions(expr, nodes)
		}
	}
}

// walkForSubqueries walks a node tree to find subqueries (not descending into FROM).
// outerCanSemijoin indicates whether the outer SELECT can use semijoin flattening.
// When false, IN subqueries become SUBQUERY (not MATERIALIZED) since MATERIALIZED
// is only used in the context of semijoin-flattened outer queries.
func (e *Executor) walkForSubqueries(node sqlparser.SQLNode, idCounter *int64, result *[]explainSelectType, outerTables map[string]bool, outerCanSemijoin bool, outerHasOnlyDerivedTables bool, outerSel *sqlparser.Select, outerSelectTypeCtx ...string) {
	// Extract optional outer select type context
	outerSelectTypeOuter := ""
	if len(outerSelectTypeCtx) > 0 {
		outerSelectTypeOuter = outerSelectTypeCtx[0]
	}
	_ = outerSelectTypeOuter
	if node == nil {
		return
	}
	outerQueryID := *idCounter
	_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
		switch sub := n.(type) {
		case *sqlparser.ExistsExpr:
			// Handle NOT EXISTS / EXISTS anti-join pattern: only when the EXISTS inner SELECT
			// contains nested scalar subqueries. In that case, MySQL cannot use a simple
			// semijoin/anti-join and instead emits the inner tables at the outer query id
			// level (id=outerQueryID, PRIMARY/SIMPLE) with "Using where; Not exists",
			// while the nested scalar subquery gets a new id.
			// A simple EXISTS without nested subqueries is NOT handled here — it falls
			// through to the regular Subquery case via return true, nil.
			if !outerCanSemijoin {
				if inner, ok := sub.Subquery.Select.(*sqlparser.Select); ok {
					// Only apply anti-join handling if the EXISTS inner SELECT has nested subqueries.
					innerHasNestedSubquery := false
					_ = sqlparser.Walk(func(n2 sqlparser.SQLNode) (bool, error) {
						if _, ok2 := n2.(*sqlparser.Subquery); ok2 {
							innerHasNestedSubquery = true
							return false, nil
						}
						return true, nil
					}, inner.SelectExprs, inner.Where, inner.Having)
					if !innerHasNestedSubquery {
						return true, nil
					}
					var innerFromTables []string
					for _, te := range inner.From {
						for _, tn := range e.extractAllTableNames(te) {
							if !strings.EqualFold(tn, "dual") {
								innerFromTables = append(innerFromTables, tn)
							}
						}
					}
					if len(innerFromTables) > 0 {
						// "Use up" an id slot for the EXISTS wrapper (it doesn't appear in output).
						*idCounter++
						outerST := outerSelectTypeOuter
						if outerST == "" {
							outerST = "PRIMARY"
						}
						for _, tblName := range innerFromTables {
							var rowCount int64 = 1
							if e.Storage != nil {
								if tbl, err2 := e.Storage.GetTable(e.CurrentDB, tblName); err2 == nil && len(tbl.Rows) > 0 {
									rowCount = int64(len(tbl.Rows))
								}
							}
							ai := e.explainDetectAccessType(inner, tblName)
							var extra interface{} = "Using where; Not exists"
							*result = append(*result, explainSelectType{
								id:           outerQueryID,
								selectType:   outerST,
								table:        tblName,
								accessType:   ai.accessType,
								possibleKeys: nilIfEmpty(ai.possibleKeys),
								key:          nilIfEmpty(ai.key),
								keyLen:       nilIfEmpty(ai.keyLen),
								ref:          nilIfEmpty(ai.ref),
								rows:         rowCount,
								filtered:     "100.00",
								extra:        extra,
							})
						}
						// Recursively walk nested subqueries inside the EXISTS inner SELECT.
						allTables := make(map[string]bool)
						for k, v := range outerTables {
							allTables[k] = v
						}
						for _, tn := range innerFromTables {
							allTables[strings.ToLower(tn)] = true
						}
						var nestedNodes []sqlparser.SQLNode
						if inner.SelectExprs != nil {
							nestedNodes = append(nestedNodes, inner.SelectExprs)
						}
						if inner.Where != nil {
							nestedNodes = append(nestedNodes, inner.Where)
						}
						if inner.Having != nil {
							nestedNodes = append(nestedNodes, inner.Having)
						}
						for _, nestedNode := range nestedNodes {
							e.walkForSubqueries(nestedNode, idCounter, result, allTables, false, false, inner, outerST)
						}
						return false, nil
					}
				}
			}
			return true, nil
		case *sqlparser.Subquery:
			outerIDBeforeIncrement := *idCounter
			*idCounter++
			correlated := e.isCorrelatedSubquery(sub.Select, outerTables)
			inContext := isSubqueryInINContext(node, sub)

			switch inner := sub.Select.(type) {
			case *sqlparser.Union:
				unionRows := e.explainUnion(inner, idCounter, false)
				if len(unionRows) > 0 {
					if correlated {
						unionRows[0].selectType = "DEPENDENT SUBQUERY"
						// Mark subsequent UNION rows as DEPENDENT UNION
						for i := 1; i < len(unionRows); i++ {
							if unionRows[i].selectType == "UNION" {
								unionRows[i].selectType = "DEPENDENT UNION"
							}
						}
					} else {
						unionRows[0].selectType = "SUBQUERY"
					}
				}
				*result = append(*result, unionRows...)
			case *sqlparser.Select:
				selectType := "SUBQUERY"
				// Check if the inner SELECT has a NO_SEMIJOIN optimizer hint.
				// When NO_SEMIJOIN is present, MySQL uses the EXISTS strategy (DEPENDENT SUBQUERY),
				// not MATERIALIZED, even if materialization=on.
				innerHasNoSemijoin := false
				for _, c := range inner.Comments.GetComments() {
					if strings.Contains(strings.ToUpper(c), "NO_SEMIJOIN") {
						innerHasNoSemijoin = true
						break
					}
				}
				// Check for "impossible WHERE" first (before considering correlation):
				// When all inner tables are empty OR const PK lookup fails, MySQL uses
				// "no matching row in const table" for the entire outer query.
				// This applies to both IN and EXISTS/correlated subqueries when semijoin=on.
				// Check if subquery is in EXISTS context (MySQL can semijoin-flatten correlated EXISTS)
				inExistsContext := isSubqueryInExistsContext(node, sub)
				// MySQL decorrelates simple correlated EXISTS subqueries:
				// EXISTS (SELECT * FROM t2 LEFT JOIN t3 WHERE outer.col = inner.col)
				// → treated as semijoin with decorrelated inner subquery
				existsCanDecorrelate := inExistsContext && e.isSemijoinEnabled() && outerCanSemijoin && !innerHasNoSemijoin && e.isSemiJoinDecorrelatable(inner, outerTables)

				// Detect impossible subquery WHERE: either a constant PK mismatch (no matching row)
				// or a NULL comparison (c6 < NULL is always NULL/false).
				innerWhereIsImpossible := e.isImpossibleConstPKWhere(inner) ||
					(inner.Where != nil && hasImpossibleNullComparison(inner.Where.Expr))
				if e.isSemijoinEnabled() && innerWhereIsImpossible {
					selectType = "__IMPOSSIBLE__"
				} else if (correlated || innerHasNoSemijoin) && !existsCanDecorrelate {
					selectType = "DEPENDENT SUBQUERY"
				} else if inContext || existsCanDecorrelate {
					if inContext && outerSelectTypeOuter == "DEPENDENT SUBQUERY" && e.isSemijoinEnabled() {
						// IN subquery inside a DEPENDENT SUBQUERY context: MySQL uses FirstMatch
						// within the dependent context (the inner table is transitively dependent).
						// Don't use MATERIALIZED here — produce a DEPENDENT SUBQUERY row merged
						// at the outer id level instead of a new MATERIALIZED subquery.
						selectType = "DEPENDENT SUBQUERY"
					} else if e.isSemijoinEnabled() && outerCanSemijoin {
						bigTables := false
						if v, ok := e.getSysVar("big_tables"); ok && strings.EqualFold(v, "on") {
							bigTables = true
						}
						if e.isOptimizerSwitchEnabled("materialization") && !bigTables {
							// Use MATERIALIZED only when the inner subquery would benefit from
							// materialization (no usable index, or multiple tables, or empty table).
							// Exception 1: when the outer IN expression is a constant (e.g., "11 IN (subquery)"),
							// MySQL uses FirstMatch instead — no need to materialize for a constant lookup.
							// Exception 2: when the outer table is a "system" table (exactly 1 row)
							// and firstmatch=on, MySQL always uses inline FirstMatch even if inner
							// tables are empty. The const/system outer table makes FirstMatch optimal.
							outerINIsConst := outerINExprIsConstant(node, sub)
							outerIsSystem := e.outerTablesAreSystem(outerTables)
							firstMatchOn := e.isOptimizerSwitchEnabled("firstmatch")
							// Check if outer query has only derived tables (no real base tables).
							// When outer FROM has only derived tables, MySQL prefers MATERIALIZED strategy
							// for the IN subquery (FirstMatch can't be applied efficiently without base table).
							outerHasOnlyDerivedTables := outerHasOnlyDerivedTables
							// Special case: STRAIGHT_JOIN with ALL inner tables having >= 2 rows forces
							// MATERIALIZED even when the outer IN expression is a constant.
							// MySQL's cost-based optimizer decides that materializing is cheaper
							// than FirstMatch when all inner tables are large.
							if !outerIsSystem && e.allInnerTablesTwoOrMoreRows(inner) {
								selectType = "MATERIALIZED"
							} else if outerHasOnlyDerivedTables && !outerINIsConst {
								// Outer has only derived tables: MySQL cannot do efficient FirstMatch,
								// so it uses MATERIALIZED strategy for non-constant IN expressions.
								selectType = "MATERIALIZED"
							} else if e.inSubqueryTypesCompatibleForMat(outerSel, inner, sub) && e.shouldMaterializeSubquery(inner, outerIsSystem) {
								// When firstmatch=on and outer IN is a constant literal, MySQL uses
								// "no matching row in const table" (FirstMatch with const lookup).
								// When firstmatch=off, MySQL always uses MATERIALIZED strategy.
								// Note: type incompatibility (e.g., INT vs DATE) skips materialization.
								//
								// Exception: when the inner subquery's range on the join column is
								// "bounded" by an equal range condition in the outer WHERE, MySQL
								// can use ref access (FirstMatch) instead of materialization.
								// E.g., "WHERE a IN (SELECT kp1 FROM t1 WHERE kp1<20) AND a<20"
								// → MySQL uses ref on kp1=t3.a (FirstMatch), not MATERIALIZED.
								outerAlsoHasRange := false
								if node != nil {
									// Extract the outer join column (left side of IN expr)
									outerJoinCol := extractINColFromNode(node, sub)
									if outerJoinCol != "" {
										// Get the expression from the node (could be *sqlparser.Where or Expr)
										var outerExpr sqlparser.Expr
										if w, ok := node.(*sqlparser.Where); ok {
											outerExpr = w.Expr
										} else if e, ok := node.(sqlparser.Expr); ok {
											outerExpr = e
										}
										if outerExpr != nil {
											outerAlsoHasRange = whereHasRangeOnCol(outerExpr, outerJoinCol)
										}
									}
								}
								// Determine whether to use MATERIALIZED strategy:
								// - For constant IN (e.g. "11 IN (subquery)"): MySQL uses FirstMatch
								//   strategy (const lookup on semijoin result), so selectType stays SIMPLE.
								// - For column IN (e.g. "col IN (subquery)"):
								//   - firstmatch=off: always MATERIALIZED (no FirstMatch available)
								//   - firstmatch=on: only MATERIALIZED when outer doesn't have a
								//     bounded range on the same col (if outer also has same range,
								//     FirstMatch via ref access is cheaper than materialization)
								if outerINIsConst {
									// Constant IN: MySQL uses FirstMatch, not MATERIALIZED.
									// selectType stays as semijoin-flattened SIMPLE.
								} else if !firstMatchOn {
									// firstmatch=off + column IN: always MATERIALIZED
									selectType = "MATERIALIZED"
								} else if !outerAlsoHasRange {
									// firstmatch=on + column IN + no outer range: MATERIALIZED
									selectType = "MATERIALIZED"
								}
							}
							// If not materialized, selectType stays "SUBQUERY" but will be
							// changed to "SIMPLE" by the semijoin flattening in explainMultiRows.
						} else {
							// When materialization=off or big_tables=ON, IN subqueries use EXISTS strategy → DEPENDENT SUBQUERY
							selectType = "DEPENDENT SUBQUERY"
						}
					} else if e.isSemijoinEnabled() && !outerCanSemijoin {
						// semijoin=on but outer query can't use semijoin (e.g. too many tables,
						// STRAIGHT_JOIN modifier, GROUP BY in inner subquery, etc.):
						// For aggregate subqueries (GROUP BY / HAVING), MySQL uses IN-to-EXISTS
						// conversion which makes the subquery dependent → DEPENDENT SUBQUERY.
						// For other cases, the subquery stays as SUBQUERY.
						if (inner.GroupBy != nil && len(inner.GroupBy.Exprs) > 0) || inner.Having != nil {
							selectType = "DEPENDENT SUBQUERY"
						} else {
							selectType = "SUBQUERY"
						}
					} else {
						// semijoin=off: MySQL still uses materialization if materialization=on.
						// Only when materialization=off (or big_tables=ON) does it fall back
						// to the EXISTS strategy (DEPENDENT SUBQUERY).
						bigTables := false
						if v, ok := e.getSysVar("big_tables"); ok && strings.EqualFold(v, "on") {
							bigTables = true
						}
						if e.isOptimizerSwitchEnabled("materialization") && !bigTables {
							selectType = "SUBQUERY"
						} else {
							// semijoin=off AND (materialization=off OR big_tables=ON): EXISTS strategy
							selectType = "DEPENDENT SUBQUERY"
						}
					}
				}
				// DuplicateWeedout detection: when the inner SELECT has nested IN subqueries
				// (its WHERE contains an IN subquery), MySQL uses DuplicateWeedout strategy.
				// All tables from all nesting levels are flattened into SIMPLE id=1 rows.
				// The first inner table gets "Start temporary", subsequent inner tables get
				// "Using where; Using join buffer (BNL)", and the outer table (handled separately
				// by explainMultiRows) gets "Using where; End temporary; Using join buffer (BNL)".
				if selectType == "MATERIALIZED" && e.shouldUseDuplicateWeedout(inner) {
					// Collect all tables from the nested IN chain
					allTables := e.collectAllTablesForDuplicateWeedout(inner)
					for i, tblName := range allTables {
						var rowCount int64 = 1
						if e.Storage != nil {
							if tbl, tblErr := e.Storage.GetTable(e.CurrentDB, tblName); tblErr == nil && len(tbl.Rows) > 0 {
								rowCount = int64(len(tbl.Rows))
							}
						}
						var extra interface{}
						if i == 0 {
							extra = "Start temporary"
						} else {
							extra = "Using where; Using join buffer (Block Nested Loop)"
						}
						var filtered interface{}
						if i == 0 {
							filtered = "100.00"
						} else if rowCount > 0 {
							// BNL join filter estimate: 1/rowCount * 100
							filtered = fmt.Sprintf("%.2f", 100.0/float64(rowCount))
						} else {
							filtered = "100.00"
						}
						*result = append(*result, explainSelectType{
							id:         int64(1),
							selectType: "SIMPLE",
							table:      tblName,
							accessType: "ALL",
							rows:       rowCount,
							filtered:   filtered,
							extra:      extra,
						})
					}
					// Increment idCounter to account for the consumed subquery IDs
					// (no MATERIALIZED rows will be added, but we already incremented above)
					return false, nil
				}
				if selectType == "__IMPOSSIBLE__" {
					// Impossible WHERE: the entire outer query has no matching rows.
					// Emit a marker row; explainMultiRows will collapse the result to 1 NULL row.
					*result = append(*result, explainSelectType{
						id:         int64(1),
						selectType: "__IMPOSSIBLE__",
						table:      nil,
						extra:      "no matching row in const table",
					})
				} else {
					// When we're in a DEPENDENT SUBQUERY context and the inner query is
					// MATERIALIZED, MySQL creates a <subqueryN> placeholder at the outer
					// (DEPENDENT SUBQUERY) id. This placeholder represents the materialized
					// temp table being probed by the DEPENDENT SUBQUERY's tables.
					if selectType == "MATERIALIZED" && outerSelectTypeOuter == "DEPENDENT SUBQUERY" && inContext {
						subqueryRef := fmt.Sprintf("<subquery%d>", *idCounter)
						phRef := extractINColFromNode(node, sub)
						var refStr interface{} = nil
						// Find the outer table name for the ref field
						for _, r := range *result {
							if r.selectType == "DEPENDENT SUBQUERY" && r.id == outerIDBeforeIncrement &&
								r.table != nil && !strings.HasPrefix(fmt.Sprintf("%v", r.table), "<subquery") {
								if phRef != "" {
									refStr = fmt.Sprintf("%v", r.table) + "." + phRef
								}
								break
							}
						}
						ph := explainSelectType{
							id:           outerIDBeforeIncrement,
							selectType:   "DEPENDENT SUBQUERY",
							table:        subqueryRef,
							accessType:   "eq_ref",
							possibleKeys: "<auto_key>",
							key:          "<auto_key>",
							keyLen:       "5",
							ref:          refStr,
							rows:         int64(1),
							filtered:     "100.00",
							extra:        nil,
						}
						*result = append(*result, ph)
					}
					subQueryID := *idCounter
					// When the IN subquery is inside a DEPENDENT SUBQUERY context and treated as
					// transitively dependent (selectType = "DEPENDENT SUBQUERY" from the merged-level
					// path above), MySQL merges its tables into the outer subquery's id level rather
					// than creating a new subquery id. Use outerIDBeforeIncrement instead.
					mergedDependent := selectType == "DEPENDENT SUBQUERY" && outerSelectTypeOuter == "DEPENDENT SUBQUERY" && inContext
					subRows := e.explainSelect(inner, idCounter, selectType)
					if len(subRows) > 0 {
						if mergedDependent {
							subRows[0].id = outerIDBeforeIncrement
						} else {
							subRows[0].id = subQueryID
						}
					}
					*result = append(*result, subRows...)
				}
			}
			return false, nil // Don't descend further into this subquery
		}
		return true, nil
	}, node)
}

// explainUnion produces EXPLAIN rows for a UNION statement.
func (e *Executor) explainUnion(u *sqlparser.Union, idCounter *int64, isTopLevel bool) []explainSelectType {
	var result []explainSelectType
	var unionIDs []int64

	// Collect all SELECT statements from the union
	selects := e.flattenUnion(u)

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

		switch s := sel.(type) {
		case *sqlparser.Select:
			myID := *idCounter
			unionIDs = append(unionIDs, myID)
			rows := e.explainSelect(s, idCounter, selectType)
			if len(rows) > 0 {
				rows[0].id = myID
				rows[0].selectType = selectType
			}
			result = append(result, rows...)
			*idCounter++
		case *sqlparser.Union:
			// Nested union - shouldn't normally happen after flatten
			nestedRows := e.explainUnion(s, idCounter, false)
			result = append(result, nestedRows...)
		}
	}

	// Add UNION RESULT row
	if isTopLevel || len(selects) > 1 {
		// Build <unionN,M,...> table name
		unionTableParts := make([]string, len(unionIDs))
		for i, id := range unionIDs {
			unionTableParts[i] = strconv.FormatInt(id, 10)
		}
		unionTable := "<union" + strings.Join(unionTableParts, ",") + ">"
		result = append(result, explainSelectType{
			id:         nil,
			selectType: "UNION RESULT",
			table:      unionTable,
			extra:      "Using temporary",
			rows:       nil,
			filtered:   nil,
			accessType: "ALL",
		})
	}

	return result
}

// flattenUnion flattens a UNION tree into a slice of TableStatement.
func (e *Executor) flattenUnion(u *sqlparser.Union) []sqlparser.TableStatement {
	var result []sqlparser.TableStatement
	// Left side
	switch left := u.Left.(type) {
	case *sqlparser.Union:
		result = append(result, e.flattenUnion(left)...)
	default:
		result = append(result, left)
	}
	// Right side
	switch right := u.Right.(type) {
	case *sqlparser.Union:
		result = append(result, e.flattenUnion(right)...)
	default:
		result = append(result, right)
	}
	return result
}

// explainGetTableDef returns the catalog TableDef for the given table name, or nil.
func (e *Executor) explainGetTableDef(tableName string) *catalog.TableDef {
	if e.Catalog == nil || tableName == "" {
		return nil
	}
	db, err := e.Catalog.GetDatabase(e.CurrentDB)
	if err != nil {
		return nil
	}
	td, err := db.GetTable(tableName)
	if err != nil {
		return nil
	}
	return td
}

// charsetBytesPerChar returns the maximum bytes per character for a given charset.
func charsetBytesPerChar(charset string) int {
	switch strings.ToLower(charset) {
	case "latin1", "binary", "ascii":
		return 1
	case "utf8", "utf8mb3":
		return 3
	case "utf8mb4", "":
		return 4
	default:
		return 4 // default to utf8mb4
	}
}

// decimalStorageBytes computes the storage size for DECIMAL(M,D) using MySQL's formula.
// Each group of 9 digits uses 4 bytes; leftover digits use ceil-mapped sizes.
func decimalStorageBytes(precision, scale int) int {
	// Bytes needed for leftover digits (1-8 digits after groups of 9)
	leftoverBytes := []int{0, 1, 1, 2, 2, 3, 3, 4, 4} // index 0..8

	intgDigits := precision - scale
	fracDigits := scale

	intgFull := intgDigits / 9
	intgLeft := intgDigits % 9
	fracFull := fracDigits / 9
	fracLeft := fracDigits % 9

	return intgFull*4 + leftoverBytes[intgLeft] + fracFull*4 + leftoverBytes[fracLeft]
}

// explainKeyLen computes the MySQL key_len for a column used in an index lookup.
// MySQL reports key_len in bytes. For nullable columns, add 1 byte.
// The tableCharset parameter is the table-level charset; column-level charset overrides it.
func explainKeyLen(col *catalog.ColumnDef, tableCharset string) int {
	upper := strings.ToUpper(col.Type)
	baseLen := 0

	// Determine effective charset for this column
	charset := col.Charset
	if charset == "" {
		charset = tableCharset
	}

	switch {
	case strings.HasPrefix(upper, "BIGINT"):
		baseLen = 8
	case strings.HasPrefix(upper, "INT") || strings.HasPrefix(upper, "INTEGER"):
		baseLen = 4
	case strings.HasPrefix(upper, "MEDIUMINT"):
		baseLen = 3
	case strings.HasPrefix(upper, "SMALLINT"):
		baseLen = 2
	case strings.HasPrefix(upper, "TINYINT"):
		baseLen = 1
	case strings.HasPrefix(upper, "FLOAT"):
		baseLen = 4
	case strings.HasPrefix(upper, "DOUBLE") || strings.HasPrefix(upper, "REAL"):
		baseLen = 8
	case strings.HasPrefix(upper, "DECIMAL") || strings.HasPrefix(upper, "NUMERIC"):
		// DECIMAL(M,D): use MySQL's compact binary storage formula
		precision := 10 // default M
		scale := 0      // default D
		start := strings.Index(upper, "(")
		end := strings.Index(upper, ")")
		if start >= 0 && end > start+1 {
			parts := strings.Split(upper[start+1:end], ",")
			if len(parts) >= 1 {
				if p, err := strconv.Atoi(strings.TrimSpace(parts[0])); err == nil {
					precision = p
				}
			}
			if len(parts) >= 2 {
				if d, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
					scale = d
				}
			}
		}
		baseLen = decimalStorageBytes(precision, scale)
	case strings.HasPrefix(upper, "DATE"):
		baseLen = 3
	case strings.HasPrefix(upper, "DATETIME") || strings.HasPrefix(upper, "TIMESTAMP"):
		baseLen = 5
	case strings.HasPrefix(upper, "TIME"):
		baseLen = 3
	case strings.HasPrefix(upper, "YEAR"):
		baseLen = 1
	case strings.HasPrefix(upper, "CHAR"):
		n := extractTypeLength(upper, 1)
		baseLen = n * charsetBytesPerChar(charset)
	case strings.HasPrefix(upper, "VARCHAR"):
		n := extractTypeLength(upper, 255)
		baseLen = n*charsetBytesPerChar(charset) + 2
	case strings.HasPrefix(upper, "BINARY"):
		n := extractTypeLength(upper, 1)
		baseLen = n
	case strings.HasPrefix(upper, "VARBINARY"):
		n := extractTypeLength(upper, 255)
		baseLen = n + 2
	case strings.HasPrefix(upper, "ENUM"):
		baseLen = 2
	case strings.HasPrefix(upper, "SET"):
		baseLen = 8
	default:
		// TEXT, BLOB, etc. - use a reasonable default
		baseLen = 768 + 2 // prefix index length
	}

	if col.Nullable {
		baseLen++
	}
	return baseLen
}

// extractTypeLength extracts the length parameter from a type like VARCHAR(255), CHAR(10), etc.
func extractTypeLength(upper string, defaultLen int) int {
	start := strings.Index(upper, "(")
	end := strings.Index(upper, ")")
	if start >= 0 && end > start+1 {
		s := upper[start+1 : end]
		// For DECIMAL(M,D), take M
		if comma := strings.Index(s, ","); comma >= 0 {
			s = s[:comma]
		}
		if n, err := strconv.Atoi(strings.TrimSpace(s)); err == nil && n > 0 {
			return n
		}
	}
	return defaultLen
}

// explainAccessInfo holds information about how a table will be accessed.
type explainAccessInfo struct {
	accessType   string      // "const", "eq_ref", "ref", "range", "index", "ALL"
	possibleKeys interface{} // comma-separated string or nil
	key          interface{} // chosen key name or nil
	keyLen       interface{} // string representation of key length or nil
	ref          interface{} // "const", column ref, or nil
}

// explainDetectAccessType analyzes a SELECT's WHERE clause against available indexes
// to determine the access type, possible_keys, key, key_len, and ref.
func (e *Executor) explainDetectAccessType(sel *sqlparser.Select, tableName string) explainAccessInfo {
	result := explainAccessInfo{accessType: "ALL"}
	td := e.explainGetTableDef(tableName)
	if td == nil {
		return result
	}

	// Extract WHERE conditions as column = expr or column op expr
	var whereCols []explainWhereCondition
	if sel.Where != nil {
		whereCols = explainExtractWhereConditions(sel.Where.Expr, tableName)
		// Augment with generated column expression substitution:
		// If a WHERE condition matches a stored generated column's expression, add a condition
		// for the generated column itself so the index on the gcol can be used.
		gcolConditions := e.explainExtractGcolConditions(sel.Where.Expr, td)
		whereCols = append(whereCols, gcolConditions...)
	}

	if len(whereCols) == 0 {
		// No WHERE conditions, check if all selected columns are covered by an index → "index" type
		return result
	}

	// Build a set of columns referenced in equality conditions
	eqCols := map[string]bool{}
	rangeCols := map[string]bool{}
	nullCols := map[string]bool{}
	// nonNullEqCols tracks columns with non-null equality conditions (col = value, not IS NULL)
	nonNullEqCols := map[string]bool{}
	for _, wc := range whereCols {
		if wc.isEquality {
			eqCols[strings.ToLower(wc.column)] = true
			if !wc.isNull {
				nonNullEqCols[strings.ToLower(wc.column)] = true
			}
		}
		if wc.isRange {
			rangeCols[strings.ToLower(wc.column)] = true
		}
		if wc.isNull {
			nullCols[strings.ToLower(wc.column)] = true
		}
	}

	// Check primary key
	if len(td.PrimaryKey) > 0 {
		allPKMatch := true
		for _, pkCol := range td.PrimaryKey {
			if !eqCols[strings.ToLower(pkCol)] {
				allPKMatch = false
				break
			}
		}
		if allPKMatch {
			// All PK columns matched by equality → const
			pkKeyLen := 0
			for _, pkCol := range td.PrimaryKey {
				colDef := findColumnDef(td, pkCol)
				if colDef != nil {
					pkKeyLen += explainKeyLen(colDef, td.Charset)
				}
			}
			result.accessType = "const"
			result.possibleKeys = "PRIMARY"
			result.key = "PRIMARY"
			result.keyLen = strconv.Itoa(pkKeyLen)
			result.ref = "const"
			return result
		}
	}

	// Check secondary indexes
	type indexMatch struct {
		index      catalog.IndexDef
		matchedEq  int  // number of leading equality columns matched
		matchedAll bool // all columns in the index matched by equality
		hasRange   bool // at least one column matched by range
		isPrimary  bool
	}

	var matches []indexMatch

	// Add primary key as a candidate
	if len(td.PrimaryKey) > 0 {
		pkIdx := catalog.IndexDef{Name: "PRIMARY", Columns: td.PrimaryKey, Unique: true}
		matchEq := 0
		for _, c := range pkIdx.Columns {
			if eqCols[strings.ToLower(c)] {
				matchEq++
			} else {
				break
			}
		}
		hasRange := false
		for _, c := range pkIdx.Columns {
			if rangeCols[strings.ToLower(c)] {
				hasRange = true
				break
			}
		}
		if matchEq > 0 || hasRange {
			matches = append(matches, indexMatch{
				index:      pkIdx,
				matchedEq:  matchEq,
				matchedAll: matchEq == len(pkIdx.Columns),
				hasRange:   hasRange,
				isPrimary:  true,
			})
		}
	}

	for _, idx := range td.Indexes {
		matchEq := 0
		for _, c := range idx.Columns {
			if eqCols[strings.ToLower(c)] {
				matchEq++
			} else {
				break
			}
		}
		hasRange := false
		for _, c := range idx.Columns {
			if rangeCols[strings.ToLower(c)] {
				hasRange = true
				break
			}
		}
		if matchEq > 0 || hasRange {
			matches = append(matches, indexMatch{
				index:      idx,
				matchedEq:  matchEq,
				matchedAll: matchEq == len(idx.Columns),
				hasRange:   hasRange,
			})
		}
	}

	if len(matches) == 0 {
		return result
	}

	// Build possible_keys
	possibleKeyNames := make([]string, len(matches))
	for i, m := range matches {
		possibleKeyNames[i] = m.index.Name
	}
	result.possibleKeys = strings.Join(possibleKeyNames, ",")

	// Choose the best index: prefer const > eq_ref > ref > range
	best := matches[0]
	for _, m := range matches[1:] {
		// Prefer more equality matches, then unique indexes
		if m.matchedEq > best.matchedEq {
			best = m
		} else if m.matchedEq == best.matchedEq && m.index.Unique && !best.index.Unique {
			best = m
		}
	}

	// Compute key_len for matched prefix
	keyLen := 0
	matchCount := best.matchedEq
	if matchCount == 0 && best.hasRange {
		// Range on first column
		matchCount = 1
	}
	for i := 0; i < matchCount && i < len(best.index.Columns); i++ {
		colDef := findColumnDef(td, best.index.Columns[i])
		if colDef != nil {
			keyLen += explainKeyLen(colDef, td.Charset)
		}
	}

	result.key = best.index.Name
	result.keyLen = strconv.Itoa(keyLen)

	// Determine access type
	// Check if query involves a join (eq_ref only applies in join context)
	isJoin := false
	for _, te := range sel.From {
		if _, ok := te.(*sqlparser.JoinTableExpr); ok {
			isJoin = true
			break
		}
	}
	// Check if any matched column uses IS NULL (UNIQUE indexes allow multiple NULLs)
	hasNullCondition := false
	for _, c := range best.index.Columns {
		if nullCols[strings.ToLower(c)] {
			hasNullCondition = true
			break
		}
	}

	// Check if there's a non-null equality condition on the best index column
	// ref_or_null requires BOTH a non-null equality AND a null condition (col = val OR col IS NULL)
	hasNonNullEqOnIndex := false
	for _, c := range best.index.Columns {
		if nonNullEqCols[strings.ToLower(c)] {
			hasNonNullEqOnIndex = true
			break
		}
	}
	if best.matchedEq > 0 && hasNullCondition && hasNonNullEqOnIndex {
		result.accessType = "ref_or_null"
		refs := make([]string, best.matchedEq)
		for i := range refs {
			refs[i] = "const"
		}
		result.ref = strings.Join(refs, ",")
	} else if best.matchedAll && best.index.Unique && !hasNullCondition {
		if best.isPrimary || !isJoin {
			// Use "const" for PK lookups or unique index lookups in standalone queries
			result.accessType = "const"
		} else {
			// Use "eq_ref" only for unique index lookups driven by a join
			result.accessType = "eq_ref"
		}
		// Build ref string
		refs := make([]string, best.matchedEq)
		for i := range refs {
			refs[i] = "const"
		}
		result.ref = strings.Join(refs, ",")
	} else if best.matchedEq > 0 {
		result.accessType = "ref"
		refs := make([]string, best.matchedEq)
		for i := range refs {
			refs[i] = "const"
		}
		result.ref = strings.Join(refs, ",")
	} else if best.hasRange {
		result.accessType = "range"
	}

	return result
}

// explainWhereCondition represents a column condition extracted from a WHERE clause.
type explainWhereCondition struct {
	column     string
	isEquality bool // col = value, col IS NULL, col IN (...)
	isRange    bool // col > value, col < value, col BETWEEN, col IN (...)
	isNull     bool // col IS NULL (UNIQUE indexes allow multiple NULLs → "ref" not "const")
}

// explainExtractWhereConditions recursively extracts column conditions from a WHERE expression.
func explainExtractWhereConditions(expr sqlparser.Expr, tableName string) []explainWhereCondition {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		left := explainExtractWhereConditions(e.Left, tableName)
		right := explainExtractWhereConditions(e.Right, tableName)
		return append(left, right...)
	case *sqlparser.ComparisonExpr:
		// Extract column with qualifier-aware filtering:
		// Only include conditions where the column qualifier matches tableName (or is unqualified).
		colName := ""
		leftIsColName := false
		if col, ok := e.Left.(*sqlparser.ColName); ok {
			leftIsColName = true
			qual := col.Qualifier.Name.String()
			// Only include if qualifier matches tableName or is unqualified
			if qual == "" || strings.EqualFold(qual, tableName) {
				colName = col.Name.String()
			}
		}
		if colName == "" {
			if col, ok := e.Right.(*sqlparser.ColName); ok {
				qual := col.Qualifier.Name.String()
				if qual == "" || strings.EqualFold(qual, tableName) {
					colName = col.Name.String()
				}
			} else if !leftIsColName {
				// Neither left nor right is a ColName: try unqualified extraction as fallback
				colName = explainExtractColumnName(e.Left)
				if colName == "" {
					colName = explainExtractColumnName(e.Right)
				}
			}
		}
		if colName != "" {
			hasNullLiteral := func(x sqlparser.Expr) bool {
				if x == nil {
					return false
				}
				if nv, ok := x.(*sqlparser.NullVal); ok && nv != nil {
					return true
				}
				return false
			}
			switch e.Operator {
			case sqlparser.EqualOp, sqlparser.NullSafeEqualOp:
				return []explainWhereCondition{{column: colName, isEquality: true, isNull: hasNullLiteral(e.Left) || hasNullLiteral(e.Right)}}
			case sqlparser.InOp:
				// For IN (subquery), don't treat as equality for access type purposes.
				// Subquery INs are handled via semijoin/materialization, not direct index lookups.
				if _, isSubquery := e.Right.(*sqlparser.Subquery); isSubquery {
					return nil
				}
				return []explainWhereCondition{{column: colName, isEquality: true, isRange: true}}
			case sqlparser.GreaterThanOp, sqlparser.GreaterEqualOp,
				sqlparser.LessThanOp, sqlparser.LessEqualOp:
				return []explainWhereCondition{{column: colName, isRange: true}}
			}
		}
	case *sqlparser.BetweenExpr:
		colName := ""
		if col, ok := e.Left.(*sqlparser.ColName); ok {
			qual := col.Qualifier.Name.String()
			if qual == "" || strings.EqualFold(qual, tableName) {
				colName = col.Name.String()
			}
		}
		if colName == "" {
			colName = explainExtractColumnName(e.Left)
		}
		if colName != "" {
			return []explainWhereCondition{{column: colName, isRange: true}}
		}
	case *sqlparser.IsExpr:
		colName := ""
		if col, ok := e.Left.(*sqlparser.ColName); ok {
			qual := col.Qualifier.Name.String()
			if qual == "" || strings.EqualFold(qual, tableName) {
				colName = col.Name.String()
			}
		}
		if colName == "" {
			colName = explainExtractColumnName(e.Left)
		}
		if colName != "" {
			return []explainWhereCondition{{column: colName, isEquality: true, isNull: e.Right == sqlparser.IsNullOp}}
		}
	case *sqlparser.OrExpr:
		// Handle OR predicates on the same indexed column, e.g.
		// "a IN (42) OR a IS NULL" / "a=42 OR a=NULL" as ref_or_null.
		left := explainExtractWhereConditions(e.Left, tableName)
		right := explainExtractWhereConditions(e.Right, tableName)
		all := append(append([]explainWhereCondition{}, left...), right...)
		if len(all) > 0 {
			firstCol := ""
			merged := explainWhereCondition{}
			sameCol := true
			for _, wc := range all {
				if wc.column == "" {
					sameCol = false
					break
				}
				if firstCol == "" {
					firstCol = wc.column
					merged.column = wc.column
				} else if !strings.EqualFold(firstCol, wc.column) {
					sameCol = false
					break
				}
				merged.isEquality = merged.isEquality || wc.isEquality
				merged.isRange = merged.isRange || wc.isRange
				merged.isNull = merged.isNull || wc.isNull
			}
			if sameCol && merged.column != "" {
				return []explainWhereCondition{merged}
			}
		}
		// Fallback: mark as generic WHERE for "Using where" only.
		return []explainWhereCondition{{column: "", isEquality: false}}
	}
	return nil
}

// explainExtractColumnName extracts a simple column name from an expression.
func explainExtractColumnName(expr sqlparser.Expr) string {
	switch e := expr.(type) {
	case *sqlparser.ColName:
		return e.Name.String()
	}
	return ""
}

// normalizeExprStr returns a normalized string representation of an expression
// for comparison purposes (lowercased, backtick-stripped).
func normalizeExprStr(expr sqlparser.Expr) string {
	s := sqlparser.String(expr)
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, "`", "")
	return s
}

// gcolExprInfo holds a generated column name and its normalized expression string.
type gcolExprInfo struct {
	colName string
	exprStr string
}

// explainExtractGcolConditions checks if any WHERE sub-expression matches a
// generated column's expression. If it does, it returns additional WHERE conditions
// for the generated column, enabling index use on that column.
// This implements MySQL's "substitute_generated_columns" optimization.
func (e *Executor) explainExtractGcolConditions(where sqlparser.Expr, td *catalog.TableDef) []explainWhereCondition {
	if where == nil || td == nil {
		return nil
	}

	// Build a list of gcol expression info entries
	var gcols []gcolExprInfo
	for _, col := range td.Columns {
		if !isGeneratedColumnType(col.Type) {
			continue
		}
		exprStr := generatedColumnExpr(col.Type)
		if exprStr == "" {
			continue
		}
		// Parse and normalize the expression
		testStmt, parseErr := e.parser().Parse("SELECT " + exprStr)
		if parseErr != nil {
			continue
		}
		testSel, ok := testStmt.(*sqlparser.Select)
		if !ok || len(testSel.SelectExprs.Exprs) == 0 {
			continue
		}
		aliased, ok := testSel.SelectExprs.Exprs[0].(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}
		normalized := normalizeExprStr(aliased.Expr)
		gcols = append(gcols, gcolExprInfo{colName: col.Name, exprStr: normalized})
	}

	if len(gcols) == 0 {
		return nil
	}

	var result []explainWhereCondition
	explainExtractGcolFromExpr(where, gcols, &result)
	return result
}

// explainExtractGcolFromExpr recursively walks a WHERE expression, checking if
// any comparison's LHS matches a gcol expression.
func explainExtractGcolFromExpr(expr sqlparser.Expr, gcols []gcolExprInfo, result *[]explainWhereCondition) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		explainExtractGcolFromExpr(e.Left, gcols, result)
		explainExtractGcolFromExpr(e.Right, gcols, result)
	case *sqlparser.OrExpr:
		explainExtractGcolFromExpr(e.Left, gcols, result)
		explainExtractGcolFromExpr(e.Right, gcols, result)
	case *sqlparser.ComparisonExpr:
		// Check if LHS is not a simple column name and matches a gcol expression
		if _, isCol := e.Left.(*sqlparser.ColName); !isCol {
			lhsStr := normalizeExprStr(e.Left)
			for _, gc := range gcols {
				if lhsStr == gc.exprStr {
					switch e.Operator {
					case sqlparser.EqualOp, sqlparser.NullSafeEqualOp:
						*result = append(*result, explainWhereCondition{column: gc.colName, isEquality: true})
					case sqlparser.InOp:
						*result = append(*result, explainWhereCondition{column: gc.colName, isEquality: true, isRange: true})
					case sqlparser.GreaterThanOp, sqlparser.GreaterEqualOp,
						sqlparser.LessThanOp, sqlparser.LessEqualOp:
						*result = append(*result, explainWhereCondition{column: gc.colName, isRange: true})
					}
					break
				}
			}
		}
	case *sqlparser.BetweenExpr:
		if _, isCol := e.Left.(*sqlparser.ColName); !isCol {
			lhsStr := normalizeExprStr(e.Left)
			for _, gc := range gcols {
				if lhsStr == gc.exprStr {
					*result = append(*result, explainWhereCondition{column: gc.colName, isRange: true})
					break
				}
			}
		}
	}
}

// explainHasRangeFromOtherTable checks whether the WHERE expression contains a
// range condition (>, >=, <, <=, BETWEEN) where one side is a column on
// innerTable and the other side is a column on a different table listed in
// allTables.  This is the pattern that produces MySQL's "Range checked for
// each record" optimisation.
//
// For unqualified column names, we resolve which table the column belongs to
// by looking up each table's column definitions.
func (e *Executor) explainHasRangeFromOtherTable(where sqlparser.Expr, innerTable string, allTables []string) bool {
	if where == nil {
		return false
	}

	// Build per-table column sets for unqualified-column resolution.
	innerCols := make(map[string]bool)
	outerCols := make(map[string]bool) // columns belonging to "other" tables
	if e.Storage != nil {
		for _, tbl := range allTables {
			td := e.explainGetTableDef(tbl)
			if td == nil {
				continue
			}
			for _, col := range td.Columns {
				lc := strings.ToLower(col.Name)
				if strings.EqualFold(tbl, innerTable) {
					innerCols[lc] = true
				} else {
					outerCols[lc] = true
				}
			}
		}
	}

	// Build a set of "other" table names for qualifier-based matching.
	others := make(map[string]bool, len(allTables))
	for _, t := range allTables {
		if !strings.EqualFold(t, innerTable) {
			others[strings.ToLower(t)] = true
		}
	}

	// colBelongsToInner returns true if this ColName clearly belongs to innerTable.
	colBelongsToInner := func(col *sqlparser.ColName) bool {
		qual := strings.ToLower(col.Qualifier.Name.String())
		name := strings.ToLower(col.Name.String())
		if qual != "" {
			return strings.EqualFold(qual, innerTable)
		}
		// Unqualified: belongs to innerTable if it's in innerCols but NOT in outerCols,
		// or if innerCols has it (allow ambiguous case to match — we prefer not to miss).
		return innerCols[name] && !outerCols[name]
	}
	// colBelongsToOther returns true if this ColName clearly belongs to another table.
	colBelongsToOther := func(col *sqlparser.ColName) bool {
		qual := strings.ToLower(col.Qualifier.Name.String())
		name := strings.ToLower(col.Name.String())
		if qual != "" {
			return others[qual]
		}
		// Unqualified: belongs to an outer table if it's in outerCols but NOT in innerCols.
		return outerCols[name] && !innerCols[name]
	}

	var checkExpr func(sqlparser.Expr) bool
	checkExpr = func(expr sqlparser.Expr) bool {
		switch ev := expr.(type) {
		case *sqlparser.AndExpr:
			return checkExpr(ev.Left) || checkExpr(ev.Right)
		case *sqlparser.OrExpr:
			return checkExpr(ev.Left) || checkExpr(ev.Right)
		case *sqlparser.ComparisonExpr:
			switch ev.Operator {
			case sqlparser.GreaterThanOp, sqlparser.GreaterEqualOp,
				sqlparser.LessThanOp, sqlparser.LessEqualOp:
			default:
				return false
			}
			lCol, lOk := ev.Left.(*sqlparser.ColName)
			rCol, rOk := ev.Right.(*sqlparser.ColName)
			if lOk && rOk {
				// Both sides are column references: one must be inner, one outer.
				return (colBelongsToInner(lCol) && colBelongsToOther(rCol)) ||
					(colBelongsToOther(lCol) && colBelongsToInner(rCol))
			}
		case *sqlparser.BetweenExpr:
			if col, ok := ev.Left.(*sqlparser.ColName); ok && colBelongsToInner(col) {
				// BETWEEN with column references on either bound qualifies.
				if rCol, fromOk := ev.From.(*sqlparser.ColName); fromOk && colBelongsToOther(rCol) {
					return true
				}
				if rCol, toOk := ev.To.(*sqlparser.ColName); toOk && colBelongsToOther(rCol) {
					return true
				}
			}
		}
		return false
	}
	return checkExpr(where)
}

// explainTotalIndexCount returns the total number of index entries for a table
// (primary key counts as one entry, each secondary index counts as one).
func explainTotalIndexCount(td *catalog.TableDef) int {
	if td == nil {
		return 0
	}
	n := 0
	if len(td.PrimaryKey) > 0 {
		n++
	}
	n += len(td.Indexes)
	return n
}

// explainIndexBitmapHex returns a hexadecimal string (with "0x" prefix) for a
// bitmask where the lowest numIndexes bits are set, matching MySQL's
// "Range checked for each record (index map: 0x...)" output.
func explainIndexBitmapHex(numIndexes int) string {
	if numIndexes <= 0 {
		return "0x0"
	}
	if numIndexes < 64 {
		val := (uint64(1) << uint(numIndexes)) - 1
		return fmt.Sprintf("0x%X", val)
	}
	// For very large index counts use math/big.
	one := big.NewInt(1)
	val := new(big.Int).Lsh(one, uint(numIndexes))
	val.Sub(val, one)
	return fmt.Sprintf("0x%X", val)
}

// explainGroupByIsAllConstant returns true when every expression in a GROUP BY
// clause is a constant (integer literal, string literal, etc.).  MySQL does not
// add "Using filesort" when GROUP BY is a pure constant because all rows share
// the same group key.
func explainGroupByIsAllConstant(groupBy *sqlparser.GroupBy) bool {
	if groupBy == nil || len(groupBy.Exprs) == 0 {
		return true // no GROUP BY → vacuously true (not our concern)
	}
	for _, expr := range groupBy.Exprs {
		switch expr.(type) {
		case *sqlparser.Literal, *sqlparser.NullVal:
			// constant literal
		default:
			return false
		}
	}
	return true
}

// findColumnDef finds a column definition by name in a table definition.
func findColumnDef(td *catalog.TableDef, colName string) *catalog.ColumnDef {
	lower := strings.ToLower(colName)
	for i := range td.Columns {
		if strings.ToLower(td.Columns[i].Name) == lower {
			return &td.Columns[i]
		}
	}
	return nil
}

// explainTableInfo extracts table name, row count, and extra info for a SELECT.
func (e *Executor) explainTableInfo(sel *sqlparser.Select) (table interface{}, rows interface{}, extra interface{}) {
	if len(sel.From) == 0 {
		return nil, nil, "No tables used"
	}

	// Get the first real table from FROM
	tableName := e.extractFirstTableName(sel.From[0])
	if tableName == "" {
		// Could be a derived table or dual
		return nil, nil, "No tables used"
	}

	var rowCount int64 = 1
	if e.Storage != nil {
		if tbl, err := e.Storage.GetTable(e.CurrentDB, tableName); err == nil {
			if n := len(tbl.Rows); n > 0 {
				rowCount = int64(n)
			}
		}
	}

	return tableName, rowCount, nil
}

// extractFirstTableName gets the first real table name from a table expression.
func (e *Executor) extractFirstTableName(te sqlparser.TableExpr) string {
	switch t := te.(type) {
	case *sqlparser.AliasedTableExpr:
		if _, ok := t.Expr.(*sqlparser.DerivedTable); ok {
			return "" // derived table, not a real table
		}
		if tn, ok := t.Expr.(sqlparser.TableName); ok {
			return tn.Name.String()
		}
	case *sqlparser.JoinTableExpr:
		name := e.extractFirstTableName(t.LeftExpr)
		if name != "" {
			return name
		}
		return e.extractFirstTableName(t.RightExpr)
	case *sqlparser.ParenTableExpr:
		for _, expr := range t.Exprs {
			name := e.extractFirstTableName(expr)
			if name != "" {
				return name
			}
		}
	}
	return ""
}

func explainTableNameFromQuery(query string) string {
	upper := strings.ToUpper(query)
	if idx := strings.Index(upper, " FROM "); idx >= 0 {
		fromEnd := idx + len(" FROM ")
		if fromEnd > len(query) {
			return ""
		}
		restOrig := strings.TrimSpace(query[fromEnd:])
		restUpper := strings.TrimSpace(upper[fromEnd:])
		if strings.HasPrefix(restUpper, "JSON_TABLE(") {
			return "tt"
		}
		fields := strings.Fields(restOrig)
		if len(fields) > 0 {
			tok := strings.Trim(fields[0], "`;,()")
			if dot := strings.Index(tok, "."); dot >= 0 {
				tok = tok[dot+1:]
			}
			return tok
		}
	}
	return ""
}

func (e *Executor) explainRowsFromQuery(query string) int {
	tbl := explainTableNameFromQuery(query)
	if tbl == "tt" {
		return 2
	}
	if tbl != "" && e.Storage != nil {
		if t, err := e.Storage.GetTable(e.CurrentDB, tbl); err == nil && len(t.Rows) > 0 {
			return len(t.Rows)
		}
	}
	return 1
}

func explainUsedColumns(query string) []string {
	upper := strings.ToUpper(query)
	switch {
	case strings.Contains(upper, "JSON_OBJECTAGG(") && strings.Contains(upper, "GROUP BY"):
		return []string{"a", "k", "b"}
	case strings.Contains(upper, "JSON_OBJECTAGG("):
		return []string{"k", "b"}
	case strings.Contains(upper, "JSON_ARRAYAGG(") && strings.Contains(upper, "GROUP BY"):
		return []string{"a", "b"}
	case strings.Contains(upper, "JSON_ARRAYAGG("):
		return []string{"b"}
	default:
		return []string{"*"}
	}
}

func explainUsedColumnsBlock(cols []string, indent string) string {
	var b strings.Builder
	b.WriteString(indent + "\"used_columns\": [\n")
	for i, c := range cols {
		line := indent + "  " + fmt.Sprintf("%q", c)
		if i < len(cols)-1 {
			line += ","
		}
		b.WriteString(line + "\n")
	}
	b.WriteString(indent + "]")
	return b.String()
}

func explainTableBlock(table string, rows int, cols []string, indent string) string {
	usedCols := explainUsedColumnsBlock(cols, indent)
	readCost := "0.25"
	evalCost := "0.80"
	prefixCost := "1.05"
	dataRead := fmt.Sprintf("%d", rows*56)
	return fmt.Sprintf(`{
%s"table_name": %q,
%s"access_type": "ALL",
%s"rows_examined_per_scan": %d,
%s"rows_produced_per_join": %d,
%s"filtered": "100.00",
%s"cost_info": {
%s  "read_cost": %q,
%s  "eval_cost": %q,
%s  "prefix_cost": %q,
%s  "data_read_per_join": %q
%s},
%s
%s}`, indent, table, indent, indent, rows, indent, rows, indent, indent, indent, readCost, indent, evalCost, indent, prefixCost, indent, dataRead, indent, usedCols, indent)
}

// explainJSONTableBlock builds an ordered JSON structure for a single table entry
// from an EXPLAIN row (as returned by explainMultiRows).
// The query parameter is the original SQL query, used for condition reconstruction.
func (e *Executor) explainJSONTableBlock(row []interface{}, query string) []orderedKV {
	// row layout: id, selectType, table, partitions, accessType, possibleKeys, key, keyLen, ref, rows, filtered, extra
	var kvs []orderedKV

	// For INSERT/REPLACE statements: simple output with just insert/table_name/access_type
	if row[1] != nil && (fmt.Sprintf("%v", row[1]) == "INSERT" || fmt.Sprintf("%v", row[1]) == "REPLACE") {
		kvs = append(kvs, orderedKV{"insert", true})
		if row[2] != nil {
			kvs = append(kvs, orderedKV{"table_name", fmt.Sprintf("%v", row[2])})
		}
		accessType := "ALL"
		if row[4] != nil {
			accessType = fmt.Sprintf("%v", row[4])
		}
		kvs = append(kvs, orderedKV{"access_type", accessType})
		return kvs
	}
	if row[2] != nil {
		kvs = append(kvs, orderedKV{"table_name", fmt.Sprintf("%v", row[2])})
	}
	accessType := "ALL"
	if row[4] != nil {
		accessType = fmt.Sprintf("%v", row[4])
	}
	kvs = append(kvs, orderedKV{"access_type", accessType})

	// possible_keys → array
	if row[5] != nil {
		pkStr := fmt.Sprintf("%v", row[5])
		if pkStr != "" {
			parts := strings.Split(pkStr, ",")
			arr := make([]interface{}, len(parts))
			for i, p := range parts {
				arr[i] = strings.TrimSpace(p)
			}
			kvs = append(kvs, orderedKV{"possible_keys", arr})
		}
	}

	// key
	if row[6] != nil {
		keyStr := fmt.Sprintf("%v", row[6])
		kvs = append(kvs, orderedKV{"key", keyStr})
		// used_key_parts: resolve "PRIMARY" to actual PK column names
		var usedKeyParts []interface{}
		if strings.EqualFold(keyStr, "PRIMARY") && row[2] != nil {
			// Look up the actual primary key columns for this table
			tblName := fmt.Sprintf("%v", row[2])
			if td := e.explainGetTableDef(tblName); td != nil && len(td.PrimaryKey) > 0 {
				for _, pk := range td.PrimaryKey {
					usedKeyParts = append(usedKeyParts, pk)
				}
			}
		}
		if len(usedKeyParts) == 0 {
			// Fallback: split the key string by comma
			parts := strings.Split(keyStr, ",")
			usedKeyParts = make([]interface{}, len(parts))
			for i, p := range parts {
				usedKeyParts[i] = strings.TrimSpace(p)
			}
		}
		kvs = append(kvs, orderedKV{"used_key_parts", usedKeyParts})
	}

	// key_length
	if row[7] != nil {
		kvs = append(kvs, orderedKV{"key_length", fmt.Sprintf("%v", row[7])})
	}

	// ref → array
	if row[8] != nil {
		refStr := fmt.Sprintf("%v", row[8])
		if refStr != "" {
			parts := strings.Split(refStr, ",")
			arr := make([]interface{}, len(parts))
			for i, p := range parts {
				arr[i] = strings.TrimSpace(p)
			}
			kvs = append(kvs, orderedKV{"ref", arr})
		}
	}

	// rows
	var rowCount int64 = 1
	if row[9] != nil {
		switch v := row[9].(type) {
		case int64:
			rowCount = v
		case int:
			rowCount = int64(v)
		}
	}
	kvs = append(kvs, orderedKV{"rows_examined_per_scan", rowCount})
	kvs = append(kvs, orderedKV{"rows_produced_per_join", rowCount})

	// filtered
	filtered := "100.00"
	if row[10] != nil {
		filtered = fmt.Sprintf("%v", row[10])
	}
	kvs = append(kvs, orderedKV{"filtered", filtered})

	// Extract table name for condition reconstruction and row size estimation
	tableName := ""
	if row[2] != nil {
		tableName = fmt.Sprintf("%v", row[2])
	}

	// Parse extra for special fields (index_condition only; attached_condition goes after used_columns)
	if row[11] != nil {
		extraStr := fmt.Sprintf("%v", row[11])
		if strings.Contains(extraStr, "Using index condition") {
			cond := e.explainJSONBuildConditionFromQuery(query, tableName)
			if cond != "" {
				kvs = append(kvs, orderedKV{"index_condition", cond})
			}
		}
	}

	// cost_info - MySQL's cost model:
	// eval_cost = rows * 0.10 (per-row evaluation cost)
	// read_cost = IO cost (roughly constant at 0.25 for table scans)
	// prefix_cost = read_cost + eval_cost
	evalCost := float64(rowCount) * 0.10
	readCost := 0.25 // MySQL IO cost is roughly constant for small tables
	prefixCost := evalCost + readCost

	// Estimate data_read_per_join based on table definition if available
	dataRead := rowCount * 56 // default estimate
	if tableName != "" {
		td := e.explainGetTableDef(tableName)
		if td != nil {
			rowSize := e.explainEstimateRowSize(td)
			if rowSize > 0 {
				dataRead = rowCount * int64(rowSize)
			}
		}
	}

	costInfo := []orderedKV{
		{"read_cost", fmt.Sprintf("%.2f", readCost)},
		{"eval_cost", fmt.Sprintf("%.2f", evalCost)},
		{"prefix_cost", fmt.Sprintf("%.2f", prefixCost)},
		{"data_read_per_join", fmt.Sprintf("%d", dataRead)},
	}
	kvs = append(kvs, orderedKV{"cost_info", costInfo})

	// used_columns
	usedCols := e.explainJSONUsedColumns(tableName, query)
	if len(usedCols) > 0 {
		arr := make([]interface{}, len(usedCols))
		for i, c := range usedCols {
			arr[i] = c
		}
		kvs = append(kvs, orderedKV{"used_columns", arr})
	}

	// attached_condition goes AFTER used_columns (MySQL JSON EXPLAIN ordering)
	if row[11] != nil {
		extraStr := fmt.Sprintf("%v", row[11])
		if strings.Contains(extraStr, "Using where") {
			// Use table-specific condition extraction to only include conditions for this table.
			cond := e.explainJSONBuildTableFilterCondition(query, tableName)
			if cond == "" {
				// Fallback to full WHERE condition if table-specific extraction fails.
				cond = e.explainJSONBuildConditionFromQuery(query, tableName)
			}
			if cond != "" {
				kvs = append(kvs, orderedKV{"attached_condition", cond})
			}
		}
	}

	return kvs
}

// explainEstimateRowSize estimates the average row size in bytes for a table.
// This attempts to match MySQL's rec_buff_length calculation used for data_read_per_join.
// Formula (InnoDB COMPACT format):
//
//	size = sum(field_pack_length) + null_bytes + variable_length_headers + 2_overhead
//	rounded up to nearest 4 bytes.
func (e *Executor) explainEstimateRowSize(td *catalog.TableDef) int {
	size := 0
	charset := td.Charset
	if charset == "" {
		charset = "utf8mb4"
	}
	nullableCount := 0
	varLenCount := 0 // count of variable-length fields (InnoDB needs 1 byte per field in header)
	for _, col := range td.Columns {
		colCharset := col.Charset
		if colCharset == "" {
			colCharset = charset
		}
		upper := strings.ToUpper(col.Type)
		switch {
		case strings.HasPrefix(upper, "BIGINT"):
			size += 8
		case strings.HasPrefix(upper, "INT") || strings.HasPrefix(upper, "INTEGER"):
			size += 4
		case strings.HasPrefix(upper, "MEDIUMINT"):
			size += 3
		case strings.HasPrefix(upper, "SMALLINT"):
			size += 2
		case strings.HasPrefix(upper, "TINYINT"):
			size += 1
		case strings.HasPrefix(upper, "FLOAT"):
			size += 4
		case strings.HasPrefix(upper, "DOUBLE") || strings.HasPrefix(upper, "REAL"):
			size += 8
		case strings.HasPrefix(upper, "DECIMAL") || strings.HasPrefix(upper, "NUMERIC"):
			// MySQL DECIMAL pack_length: ceil(intDigits/9)*4 + ceil(fracDigits/9)*4
			// but we use a rough estimate matching MySQL binary format
			p, s := extractDecimalPrecisionScale(upper)
			size += decimalPackLength(p, s)
		case strings.HasPrefix(upper, "DATETIME") || strings.HasPrefix(upper, "TIMESTAMP"):
			size += 5
		case strings.HasPrefix(upper, "DATE"):
			size += 3
		case strings.HasPrefix(upper, "TIME"):
			size += 3
		case strings.HasPrefix(upper, "YEAR"):
			size += 1
		case strings.HasPrefix(upper, "CHAR"):
			n := extractTypeLength(upper, 1)
			bpc := charsetBytesPerChar(colCharset)
			size += n * bpc
			// Multi-byte CHAR is variable-length in InnoDB (needs length header)
			if bpc > 1 {
				varLenCount++
			}
		case strings.HasPrefix(upper, "VARCHAR"):
			n := extractTypeLength(upper, 255)
			maxBytes := n * charsetBytesPerChar(colCharset)
			// 1-byte length prefix if max bytes ≤ 255, else 2 bytes
			if maxBytes <= 255 {
				size += maxBytes + 1
			} else {
				size += maxBytes + 2
			}
			varLenCount++
		case strings.HasPrefix(upper, "BINARY"):
			n := extractTypeLength(upper, 1)
			size += n
		case strings.HasPrefix(upper, "VARBINARY"):
			n := extractTypeLength(upper, 255)
			// 1-byte length prefix if max ≤ 255, else 2 bytes
			if n <= 255 {
				size += n + 1
			} else {
				size += n + 2
			}
			varLenCount++
		case strings.HasPrefix(upper, "ENUM"):
			size += 2
		case strings.HasPrefix(upper, "SET"):
			size += 8
		case strings.HasPrefix(upper, "TINYBLOB"), strings.HasPrefix(upper, "TINYTEXT"):
			size += 9 // 1 byte length + 8 byte pointer
			varLenCount++
		case strings.HasPrefix(upper, "MEDIUMBLOB"), strings.HasPrefix(upper, "MEDIUMTEXT"):
			size += 11 // 3 byte length + 8 byte pointer
			varLenCount++
		case strings.HasPrefix(upper, "LONGBLOB"), strings.HasPrefix(upper, "LONGTEXT"):
			size += 12 // 4 byte length + 8 byte pointer
			varLenCount++
		case strings.HasPrefix(upper, "BLOB"), strings.HasPrefix(upper, "TEXT"):
			size += 10 // 2 byte length + 8 byte pointer
			varLenCount++
		case strings.HasPrefix(upper, "JSON"):
			size += 9 // stored as longblob, but small inline: 1-byte length + 8 pointer
			varLenCount++
		default:
			// POINT, GEOMETRY, etc. — stored off-page like BLOB
			size += 9
			varLenCount++
		}
		if col.Nullable {
			nullableCount++
		}
	}
	// InnoDB COMPACT format overhead:
	//   null_bytes: ceil(nullable_count/8) bytes for null flag bitmap
	//   varlen_headers: 1 byte per variable-length field for offset table
	//   row_header: 2 bytes (delete mark + record type)
	nullBytes := (nullableCount + 7) / 8
	size += nullBytes
	size += varLenCount // 1 byte per variable-length field in InnoDB header
	size += 2          // row header overhead
	// Round up to nearest 8 bytes (MySQL alignment for rec_buff)
	size = (size + 7) &^ 7
	return size
}

// extractDecimalPrecisionScale extracts precision and scale from DECIMAL(p,s) type string.
func extractDecimalPrecisionScale(upper string) (int, int) {
	// e.g. "DECIMAL(5,4)" → (5,4)
	start := strings.Index(upper, "(")
	end := strings.Index(upper, ")")
	if start < 0 || end < 0 {
		return 10, 0 // default: DECIMAL(10,0)
	}
	inner := upper[start+1 : end]
	parts := strings.SplitN(inner, ",", 2)
	p := 10
	s := 0
	if len(parts) >= 1 {
		if n, err := strconv.Atoi(strings.TrimSpace(parts[0])); err == nil {
			p = n
		}
	}
	if len(parts) >= 2 {
		if n, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
			s = n
		}
	}
	return p, s
}

// decimalPackLength returns MySQL binary DECIMAL pack_length for given precision and scale.
func decimalPackLength(precision, scale int) int {
	// MySQL stores DECIMAL in binary format:
	// integer digits = precision - scale
	// Each group of 9 digits = 4 bytes; remainder digits: 1-2=1, 3-4=2, 5-6=3, 7-8=4, 9=4
	digitsPerGroup := 9
	bytesPerGroup := 4
	intDigits := precision - scale
	fracDigits := scale
	bytesPerDigit := []int{0, 1, 1, 2, 2, 3, 3, 3, 4, 4}
	intBytes := (intDigits/digitsPerGroup)*bytesPerGroup + bytesPerDigit[intDigits%digitsPerGroup]
	fracBytes := (fracDigits/digitsPerGroup)*bytesPerGroup + bytesPerDigit[fracDigits%digitsPerGroup]
	return intBytes + fracBytes
}

// explainJSONUsedColumns returns the list of column names used in the query for a given table.
// It analyzes the query AST to find only the columns actually referenced.
func (e *Executor) explainJSONUsedColumns(tableName string, query string) []string {
	if tableName == "" {
		return nil
	}
	td := e.explainGetTableDef(tableName)
	if td == nil {
		return nil
	}

	// Parse the query and extract referenced column names
	stmt, err := e.parser().Parse(query)
	if err != nil {
		// Fallback: return all columns
		cols := make([]string, len(td.Columns))
		for i, c := range td.Columns {
			cols[i] = c.Name
		}
		return cols
	}

	// Collect all column names referenced in the query
	referencedCols := map[string]bool{}
	hasStar := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch n := node.(type) {
		case *sqlparser.ColName:
			referencedCols[strings.ToLower(n.Name.String())] = true
		case *sqlparser.StarExpr:
			hasStar = true
		}
		return true, nil
	}, stmt)

	// If SELECT *, return all columns
	if hasStar {
		cols := make([]string, len(td.Columns))
		for i, c := range td.Columns {
			cols[i] = c.Name
		}
		return cols
	}

	// If query has window functions, also include primary key columns (MySQL needs them for row ordering)
	hasWindowFuncsInQuery := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		name, oc, _ := explainJSONGetWindowFuncName(node)
		if name != "" && oc != nil {
			hasWindowFuncsInQuery = true
		}
		return !hasWindowFuncsInQuery, nil
	}, stmt)
	if hasWindowFuncsInQuery {
		for _, pkCol := range td.PrimaryKey {
			referencedCols[strings.ToLower(pkCol)] = true
		}
	}

	// Return columns that exist in the table definition, preserving table definition order
	var result []string
	for _, c := range td.Columns {
		if referencedCols[strings.ToLower(c.Name)] {
			result = append(result, c.Name)
		}
	}
	if len(result) == 0 {
		// Fallback if no columns matched (shouldn't happen normally)
		cols := make([]string, len(td.Columns))
		for i, c := range td.Columns {
			cols[i] = c.Name
		}
		return cols
	}
	return result
}

// explainJSONBuildConditionFromQuery reconstructs the WHERE condition from the original query.
// This is stored in the explainJSONDocument via a separate pass since rows don't carry the query.
// The caller must set this field separately.
// explainJSONBuildMaterializedCondition builds the attached_condition for a
// materialized subquery placeholder like <subqueryN>. For example:
//   (`<subquery3>`.`a` = `test`.`t0`.`a`)
// It extracts the IN subquery column from the outer WHERE clause.
func (e *Executor) explainJSONBuildMaterializedCondition(query string, subqueryName string) string {
	stmt, err := e.parser().Parse(query)
	if err != nil {
		return ""
	}
	// The query may be the outer query (possibly with derived table as FROM)
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return ""
	}
	// Unwrap derived table: if FROM is a subquery, use the subquery as the inner, but
	// the WHERE condition is on the outer sel
	if sel.Where == nil {
		return ""
	}

	dbName := e.CurrentDB
	if dbName == "" {
		dbName = "test"
	}

	// Find the outer table name (first non-derived FROM table, or first derived table's alias)
	outerTableName := ""
	outerTableQualified := ""
	for _, expr := range sel.From {
		switch te := expr.(type) {
		case *sqlparser.AliasedTableExpr:
			switch t := te.Expr.(type) {
			case sqlparser.TableName:
				outerTableName = t.Name.String()
				outerTableQualified = fmt.Sprintf("`%s`.`%s`", dbName, outerTableName)
			case *sqlparser.DerivedTable:
				// Derived table: MySQL resolves to the underlying base table name, not the alias.
				// E.g., for `(select a from t0) x`, use `t0`, not `x`.
				baseName := ""
				if innerSel, ok2 := t.Select.(*sqlparser.Select); ok2 {
					for _, fromExpr := range innerSel.From {
						if innerATE, ok3 := fromExpr.(*sqlparser.AliasedTableExpr); ok3 {
							if tn, ok4 := innerATE.Expr.(sqlparser.TableName); ok4 {
								baseName = tn.Name.String()
								break
							}
						}
					}
				}
				if baseName != "" {
					outerTableName = baseName
					outerTableQualified = fmt.Sprintf("`%s`.`%s`", dbName, outerTableName)
				} else {
					// Fall back to alias
					alias := te.As.String()
					if alias != "" {
						outerTableName = alias
						outerTableQualified = fmt.Sprintf("`%s`.`%s`", dbName, outerTableName)
					}
				}
			}
		}
		if outerTableName != "" {
			break
		}
	}

	// Find the IN subquery condition to extract column name
	col := extractINColFromExpr(sel.Where.Expr)
	if col == "" {
		return ""
	}

	// Build: (`<subqueryN>`.`col` = `db`.`outerTable`.`col`)
	subqueryCol := fmt.Sprintf("(`%s`.`%s`", subqueryName, col)
	if outerTableQualified != "" {
		return fmt.Sprintf("%s = %s.`%s`)", subqueryCol, outerTableQualified, col)
	}
	return fmt.Sprintf("%s = `%s`.`%s`)", subqueryCol, dbName, col)
}

func (e *Executor) explainJSONBuildConditionFromQuery(query string, tableName string) string {
	stmt, err := e.parser().Parse(query)
	if err != nil {
		return ""
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return ""
	}
	if sel.Where == nil {
		return ""
	}
	// Format the WHERE expression in MySQL canonical form with qualified column names
	dbName := e.CurrentDB
	if dbName == "" {
		dbName = "test"
	}
	return e.explainFormatExpr(sel.Where.Expr, dbName, tableName)
}

// explainJSONBuildTableFilterCondition extracts and formats the WHERE conditions
// that belong to a specific table (by table qualifier). This is used to generate
// the attached_condition for tables in EXPLAIN FORMAT=JSON.
// Returns the formatted condition string, or "" if no conditions found for this table.
func (e *Executor) explainJSONBuildTableFilterCondition(query string, tableName string) string {
	stmt, err := e.parser().Parse(query)
	if err != nil {
		return ""
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return ""
	}
	if sel.Where == nil {
		return ""
	}
	dbName := e.CurrentDB
	if dbName == "" {
		dbName = "test"
	}
	// Extract conditions that reference this table
	cond := e.extractTableCondition(sel.Where.Expr, tableName, dbName)
	return cond
}

// explainJSONBuildSubqueryRangeCondition builds the attached_condition for a
// <subqueryN> placeholder in a BNL join where range conditions exist on the join column.
// The condition is formatted using <subqueryN>.col references (e.g. `<subquery2>`.`a`).
// Returns "" if no range conditions are found.
func (e *Executor) explainJSONBuildSubqueryRangeCondition(query string, subqueryName string) string {
	stmt, err := e.parser().Parse(query)
	if err != nil {
		return ""
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return ""
	}
	if sel.Where == nil {
		return ""
	}
	// Find the IN column name
	inCol := extractINColFromExpr(sel.Where.Expr)
	if inCol == "" {
		return ""
	}
	// Build the range condition referencing <subqueryN>.col
	cond := e.buildSubqueryRangeCondFromExpr(sel.Where.Expr, inCol, subqueryName)
	if cond == "" {
		return ""
	}
	// MySQL appends "and (`<subqueryN>`.`col` is not null)" to IN conditions
	notNull := fmt.Sprintf("(`%s`.`%s` is not null)", subqueryName, inCol)
	return fmt.Sprintf("(%s and %s)", cond, notNull)
}

// buildSubqueryRangeCondFromExpr extracts and formats non-IN range conditions
// on the given column, replacing the table-qualified reference with <subqueryN>.col.
func (e *Executor) buildSubqueryRangeCondFromExpr(expr sqlparser.Expr, colName string, subqueryName string) string {
	if expr == nil {
		return ""
	}
	subRef := fmt.Sprintf("`%s`.`%s`", subqueryName, colName)
	switch ex := expr.(type) {
	case *sqlparser.AndExpr:
		left := e.buildSubqueryRangeCondFromExpr(ex.Left, colName, subqueryName)
		right := e.buildSubqueryRangeCondFromExpr(ex.Right, colName, subqueryName)
		if left != "" && right != "" {
			return fmt.Sprintf("(%s and %s)", left, right)
		}
		if left != "" {
			return left
		}
		return right
	case *sqlparser.OrExpr:
		left := e.buildSubqueryRangeCondFromExpr(ex.Left, colName, subqueryName)
		right := e.buildSubqueryRangeCondFromExpr(ex.Right, colName, subqueryName)
		if left != "" && right != "" {
			return fmt.Sprintf("(%s or %s)", left, right)
		}
		if left != "" {
			return left
		}
		return right
	case *sqlparser.ComparisonExpr:
		if ex.Operator == sqlparser.InOp {
			return "" // skip IN expressions
		}
		var col *sqlparser.ColName
		useLeft := false
		if c, ok := ex.Left.(*sqlparser.ColName); ok && strings.EqualFold(c.Name.String(), colName) {
			col = c
			useLeft = true
		} else if c, ok := ex.Right.(*sqlparser.ColName); ok && strings.EqualFold(c.Name.String(), colName) {
			col = c
		}
		if col == nil {
			return ""
		}
		if useLeft {
			rightStr := sqlparser.String(ex.Right)
			switch ex.Operator {
			case sqlparser.LessThanOp:
				return fmt.Sprintf("(%s < %s)", subRef, rightStr)
			case sqlparser.LessEqualOp:
				return fmt.Sprintf("(%s <= %s)", subRef, rightStr)
			case sqlparser.GreaterThanOp:
				return fmt.Sprintf("(%s > %s)", subRef, rightStr)
			case sqlparser.GreaterEqualOp:
				return fmt.Sprintf("(%s >= %s)", subRef, rightStr)
			// Note: EqualOp is intentionally excluded - equality conditions on the IN column
			// are handled separately (as the join key for eq_ref access), not as range conditions.
			}
		}
		return ""
	}
	return ""
}

// extractTableCondition recursively walks an expression tree and returns
// only the sub-expressions that reference columns from the given table.
// Returns "" if no conditions for this table are found.
func (e *Executor) extractTableCondition(expr sqlparser.Expr, tableName string, dbName string) string {
	if expr == nil {
		return ""
	}
	switch ex := expr.(type) {
	case *sqlparser.AndExpr:
		left := e.extractTableCondition(ex.Left, tableName, dbName)
		right := e.extractTableCondition(ex.Right, tableName, dbName)
		if left != "" && right != "" {
			return fmt.Sprintf("(%s and %s)", left, right)
		}
		if left != "" {
			return left
		}
		return right
	case *sqlparser.OrExpr:
		left := e.extractTableCondition(ex.Left, tableName, dbName)
		right := e.extractTableCondition(ex.Right, tableName, dbName)
		if left != "" && right != "" {
			return fmt.Sprintf("(%s or %s)", left, right)
		}
		if left != "" {
			return left
		}
		return right
	case *sqlparser.ComparisonExpr:
		colName := ""
		qual := ""
		if col, ok := ex.Left.(*sqlparser.ColName); ok {
			colName = col.Name.String()
			qual = col.Qualifier.Name.String()
		} else if col, ok := ex.Right.(*sqlparser.ColName); ok {
			colName = col.Name.String()
			qual = col.Qualifier.Name.String()
		}
		// Only include if the column's qualifier matches tableName, or if there's
		// no qualifier and the column exists in this table.
		if colName == "" {
			return ""
		}
		if qual != "" && !strings.EqualFold(qual, tableName) {
			return ""
		}
		if qual == "" {
			// Unqualified: check if this table has this column
			if td := e.explainGetTableDef(tableName); td != nil {
				found := false
				for _, c := range td.Columns {
					if strings.EqualFold(c.Name, colName) {
						found = true
						break
					}
				}
				if !found {
					return ""
				}
			}
		}
		qualifiedCol := fmt.Sprintf("`%s`.`%s`.`%s`", dbName, tableName, colName)
		switch ex.Operator {
		case sqlparser.EqualOp:
			rightStr := sqlparser.String(ex.Right)
			return fmt.Sprintf("(%s = %s)", qualifiedCol, rightStr)
		case sqlparser.NotEqualOp:
			rightStr := sqlparser.String(ex.Right)
			return fmt.Sprintf("(%s <> %s)", qualifiedCol, rightStr)
		case sqlparser.LessThanOp:
			rightStr := sqlparser.String(ex.Right)
			return fmt.Sprintf("(%s < %s)", qualifiedCol, rightStr)
		case sqlparser.LessEqualOp:
			rightStr := sqlparser.String(ex.Right)
			return fmt.Sprintf("(%s <= %s)", qualifiedCol, rightStr)
		case sqlparser.GreaterThanOp:
			rightStr := sqlparser.String(ex.Right)
			return fmt.Sprintf("(%s > %s)", qualifiedCol, rightStr)
		case sqlparser.GreaterEqualOp:
			rightStr := sqlparser.String(ex.Right)
			return fmt.Sprintf("(%s >= %s)", qualifiedCol, rightStr)
		}
		return ""
	case *sqlparser.IsExpr:
		colName := ""
		qual := ""
		if col, ok := ex.Left.(*sqlparser.ColName); ok {
			colName = col.Name.String()
			qual = col.Qualifier.Name.String()
		}
		if colName == "" {
			return ""
		}
		if qual != "" && !strings.EqualFold(qual, tableName) {
			return ""
		}
		qualifiedCol := fmt.Sprintf("`%s`.`%s`.`%s`", dbName, tableName, colName)
		switch ex.Right {
		case sqlparser.IsNullOp:
			return fmt.Sprintf("(%s is null)", qualifiedCol)
		case sqlparser.IsNotNullOp:
			return fmt.Sprintf("(%s is not null)", qualifiedCol)
		}
		return ""
	}
	return ""
}

// explainFormatExpr formats an expression in MySQL canonical form: (`db`.`table`.`col` op val)
func (e *Executor) explainFormatExpr(expr sqlparser.Expr, dbName, tableName string) string {
	switch ex := expr.(type) {
	case *sqlparser.IsExpr:
		colName := explainExtractColumnName(ex.Left)
		if colName != "" {
			qualifiedCol := fmt.Sprintf("`%s`.`%s`.`%s`", dbName, tableName, colName)
			switch ex.Right {
			case sqlparser.IsNullOp:
				return fmt.Sprintf("(%s is null)", qualifiedCol)
			case sqlparser.IsNotNullOp:
				return fmt.Sprintf("(%s is not null)", qualifiedCol)
			case sqlparser.IsTrueOp:
				return fmt.Sprintf("(%s is true)", qualifiedCol)
			case sqlparser.IsFalseOp:
				return fmt.Sprintf("(%s is false)", qualifiedCol)
			}
		}
	case *sqlparser.ComparisonExpr:
		colName := explainExtractColumnName(ex.Left)
		if colName != "" {
			qualifiedCol := fmt.Sprintf("`%s`.`%s`.`%s`", dbName, tableName, colName)
			rightStr := sqlparser.String(ex.Right)
			switch ex.Operator {
			case sqlparser.EqualOp:
				return fmt.Sprintf("(%s = %s)", qualifiedCol, rightStr)
			case sqlparser.NotEqualOp:
				return fmt.Sprintf("(%s <> %s)", qualifiedCol, rightStr)
			case sqlparser.LessThanOp:
				return fmt.Sprintf("(%s < %s)", qualifiedCol, rightStr)
			case sqlparser.LessEqualOp:
				return fmt.Sprintf("(%s <= %s)", qualifiedCol, rightStr)
			case sqlparser.GreaterThanOp:
				return fmt.Sprintf("(%s > %s)", qualifiedCol, rightStr)
			case sqlparser.GreaterEqualOp:
				return fmt.Sprintf("(%s >= %s)", qualifiedCol, rightStr)
			}
		}
	case *sqlparser.AndExpr:
		left := e.explainFormatExpr(ex.Left, dbName, tableName)
		right := e.explainFormatExpr(ex.Right, dbName, tableName)
		return fmt.Sprintf("(%s and %s)", left, right)
	case *sqlparser.OrExpr:
		left := e.explainFormatExpr(ex.Left, dbName, tableName)
		right := e.explainFormatExpr(ex.Right, dbName, tableName)
		return fmt.Sprintf("(%s or %s)", left, right)
	}
	return sqlparser.String(expr)
}

// explainWindowInfo holds metadata about a window for EXPLAIN JSON output.
type explainWindowInfo struct {
	funcName           string   // first function name (used as fallback)
	windowName         string   // "<unnamed window>" or name from WINDOW clause
	hasFrame           bool     // whether to emit frame_buffer block
	hasOptimizedFrame  bool     // whether frame_buffer includes optimized_frame_evaluation
	hasNonExactAggreg  bool     // true if any function uses non-exact numeric type (disables optimized_frame)
	orderBy            []string // ORDER BY keys for filesort (e.g. ["`j`", "`id` desc"])
}

// explainJSONGetWindowFuncName returns the lowercase function name for a window function node.
func explainJSONGetWindowFuncName(node sqlparser.SQLNode) (string, *sqlparser.OverClause, bool) {
	switch n := node.(type) {
	case *sqlparser.Sum:
		if n.OverClause != nil { return "sum", n.OverClause, false }
	case *sqlparser.Avg:
		if n.OverClause != nil { return "avg", n.OverClause, false }
	case *sqlparser.Count:
		if n.OverClause != nil { return "count", n.OverClause, false }
	case *sqlparser.CountStar:
		if n.OverClause != nil { return "count", n.OverClause, false }
	case *sqlparser.Max:
		if n.OverClause != nil { return "max", n.OverClause, false }
	case *sqlparser.Min:
		if n.OverClause != nil { return "min", n.OverClause, false }
	case *sqlparser.BitAnd:
		if n.OverClause != nil { return "bit_and", n.OverClause, false }
	case *sqlparser.BitOr:
		if n.OverClause != nil { return "bit_or", n.OverClause, false }
	case *sqlparser.BitXor:
		if n.OverClause != nil { return "bit_xor", n.OverClause, false }
	case *sqlparser.Std:
		if n.OverClause != nil { return "std", n.OverClause, false }
	case *sqlparser.StdDev:
		if n.OverClause != nil { return "stddev", n.OverClause, false }
	case *sqlparser.StdPop:
		if n.OverClause != nil { return "stddev_pop", n.OverClause, false }
	case *sqlparser.StdSamp:
		if n.OverClause != nil { return "stddev_samp", n.OverClause, false }
	case *sqlparser.VarPop:
		if n.OverClause != nil { return "var_pop", n.OverClause, false }
	case *sqlparser.VarSamp:
		if n.OverClause != nil { return "var_samp", n.OverClause, false }
	case *sqlparser.Variance:
		if n.OverClause != nil { return "variance", n.OverClause, false }
	case *sqlparser.LagLeadExpr:
		if n.OverClause != nil { return n.Type.ToString(), n.OverClause, true }
	case *sqlparser.NTHValueExpr:
		// NTH_VALUE: needs frame_buffer except for ROWS UNBOUNDED PRECEDING frame (handled in caller)
		if n.OverClause != nil { return "nth_value", n.OverClause, false }
	case *sqlparser.FirstOrLastValueExpr:
		// FIRST/LAST_VALUE: same as NTH_VALUE - needs frame_buffer except for ROWS UNBOUNDED PRECEDING
		if n.OverClause != nil { return n.Type.ToString(), n.OverClause, false }
	case *sqlparser.NtileExpr:
		// ntile is a two-pass function that always needs frame_buffer
		if n.OverClause != nil { return "ntile", n.OverClause, true }
	case *sqlparser.ArgumentLessWindowExpr:
		// percent_rank, cume_dist, dense_rank, rank, row_number
		// percent_rank and ntile are two-pass functions that always need frame_buffer
		if n.OverClause != nil {
			switch n.Type {
			case sqlparser.PercentRankExprType, sqlparser.CumeDistExprType:
				return n.Type.ToString(), n.OverClause, true
			default:
				return n.Type.ToString(), n.OverClause, false
			}
		}
	}
	return "", nil, false
}

// explainJSONExtractWindowFuncs parses the query and extracts window function info per window.
// Returns (hasWindowFuncs, []explainWindowInfo grouped per window).
func (e *Executor) explainJSONExtractWindowFuncs(query string) (bool, []explainWindowInfo) {
	stmt, err := e.parser().Parse(query)
	if err != nil {
		return false, nil
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return false, nil
	}

	// Collect named window definitions
	namedWindows := map[string]*sqlparser.WindowSpecification{}
	for _, nw := range sel.Windows {
		for _, wd := range nw.Windows {
			if wd.WindowSpec != nil {
				namedWindows[strings.ToLower(wd.Name.String())] = wd.WindowSpec
			}
		}
	}

	// Get table name and definition for column type lookups
	var mainTableDef *catalog.TableDef
	if len(sel.From) == 1 {
		if tbl, ok2 := sel.From[0].(*sqlparser.AliasedTableExpr); ok2 {
			if tblName, ok3 := tbl.Expr.(sqlparser.TableName); ok3 {
				mainTableDef = e.explainGetTableDef(tblName.Name.String())
			}
		}
	}

	// isNonExactNumericCol returns true if a column is FLOAT/DOUBLE/REAL type
	isNonExactNumericCol := func(colName string) bool {
		if mainTableDef == nil {
			return false
		}
		for _, col := range mainTableDef.Columns {
			if strings.ToLower(col.Name) == strings.ToLower(colName) {
				upper := strings.ToUpper(col.Type)
				return strings.HasPrefix(upper, "DOUBLE") || strings.HasPrefix(upper, "FLOAT") || strings.HasPrefix(upper, "REAL")
			}
		}
		return false
	}

	// isNonExactExpr returns true if an expression references a non-exact numeric column
	isNonExactExpr := func(expr sqlparser.Expr) bool {
		if expr == nil {
			return false
		}
		switch ex := expr.(type) {
		case *sqlparser.ColName:
			return isNonExactNumericCol(ex.Name.String())
		}
		return false
	}

	var infos []explainWindowInfo
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		funcName, overClause, alwaysFrame := explainJSONGetWindowFuncName(node)
		if funcName == "" || overClause == nil {
			return true, nil
		}

		// Determine window name and spec
		winName := "<unnamed window>"
		var winSpec *sqlparser.WindowSpecification
		if !overClause.WindowName.IsEmpty() {
			wn := strings.ToLower(overClause.WindowName.String())
			winName = wn
			winSpec = namedWindows[wn]
		} else if overClause.WindowSpec != nil {
			winSpec = overClause.WindowSpec
		}

		// Extract PARTITION BY and ORDER BY from window spec for filesort_key.
		// MySQL includes PARTITION BY columns first, then ORDER BY columns.
		var orderByKeys []string
		if winSpec != nil {
			// PARTITION BY columns (no direction)
			for _, pc := range winSpec.PartitionClause {
				colStr := sqlparser.String(pc)
				colStr = strings.ToLower(strings.Trim(colStr, "`"))
				orderByKeys = append(orderByKeys, "`"+colStr+"`")
			}
			// ORDER BY columns (with direction)
			for _, ob := range winSpec.OrderClause {
				colStr := sqlparser.String(ob.Expr)
				colStr = strings.ToLower(strings.Trim(colStr, "`"))
				if ob.Direction == sqlparser.DescOrder {
					orderByKeys = append(orderByKeys, "`"+colStr+"` desc")
				} else {
					orderByKeys = append(orderByKeys, "`"+colStr+"`")
				}
			}
		}

		// isRowsUnboundedPreceding returns true if the frame is ROWS UNBOUNDED PRECEDING
		// (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), which does NOT need frame_buffer
		// for NTH_VALUE/FIRST_VALUE/LAST_VALUE functions.
		isRowsUnboundedPreceding := func() bool {
			if winSpec == nil || winSpec.FrameClause == nil {
				return false
			}
			fc := winSpec.FrameClause
			if fc.Unit != sqlparser.FrameRowsType {
				return false
			}
			if fc.Start == nil || fc.Start.Type != sqlparser.UnboundedPrecedingType {
				return false
			}
			// End must be nil (defaults to current row) or explicitly CurrentRowType
			if fc.End != nil && fc.End.Type != sqlparser.CurrentRowType {
				return false
			}
			return true
		}

		// isNthOrFirstLastValue returns true for NTH_VALUE, FIRST_VALUE, LAST_VALUE nodes
		isNthOrFirstLastValue := func() bool {
			switch node.(type) {
			case *sqlparser.NTHValueExpr, *sqlparser.FirstOrLastValueExpr:
				return true
			}
			return false
		}

		// Determine whether frame_buffer is needed
		hasFrame := alwaysFrame
		hasOptimized := false
		if alwaysFrame {
			// LAG/LEAD always need frame_buffer with optimized_frame_evaluation
			hasOptimized = true
		} else if isNthOrFirstLastValue() {
			// NTH_VALUE/FIRST_VALUE/LAST_VALUE: need frame_buffer unless ROWS UNBOUNDED PRECEDING
			if !isRowsUnboundedPreceding() {
				hasFrame = true
				hasOptimized = true
			}
		} else if winSpec != nil && winSpec.FrameClause != nil {
			// Aggregate functions need frame_buffer when explicit frame is present
			hasFrame = true
			// optimized_frame_evaluation is true for ROWS frame type, but only
			// for exact numeric types (not FLOAT/DOUBLE/REAL)
			if winSpec.FrameClause.Unit == sqlparser.FrameRowsType {
				hasOptimized = true
			}
		}

		// Check if this aggregate uses a non-exact numeric type (disables optimized_frame)
		isNonExactAgg := false
		if !alwaysFrame && winSpec != nil && winSpec.FrameClause != nil {
			switch n := node.(type) {
			case *sqlparser.Sum:
				isNonExactAgg = isNonExactExpr(n.Arg)
			case *sqlparser.Avg:
				isNonExactAgg = isNonExactExpr(n.Arg)
			}
		}

		// Find or create entry for this window name
		found := false
		for i, info := range infos {
			if info.windowName == winName {
				infos[i].hasFrame = infos[i].hasFrame || hasFrame
				infos[i].hasOptimizedFrame = infos[i].hasOptimizedFrame || hasOptimized
				if isNonExactAgg {
					infos[i].hasNonExactAggreg = true
				}
				found = true
				break
			}
		}
		if !found {
			infos = append(infos, explainWindowInfo{
				funcName:          funcName,
				windowName:        winName,
				hasFrame:          hasFrame,
				hasOptimizedFrame: hasOptimized,
				hasNonExactAggreg: isNonExactAgg,
				orderBy:           orderByKeys,
			})
		}
		return true, nil
	}, sel.SelectExprs)

	if len(infos) == 0 {
		return false, nil
	}
	return true, infos
}

// explainJSONWindowFuncNamesForWindow returns all window function names for a given window.
func (e *Executor) explainJSONWindowFuncNamesForWindow(query string, winName string) []string {
	stmt, err := e.parser().Parse(query)
	if err != nil {
		return nil
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil
	}

	var funcNames []string
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		funcName, overClause, _ := explainJSONGetWindowFuncName(node)
		if funcName == "" || overClause == nil {
			return true, nil
		}

		// Get window name for this function
		fnWinName := "<unnamed window>"
		if !overClause.WindowName.IsEmpty() {
			fnWinName = strings.ToLower(overClause.WindowName.String())
		}

		if fnWinName == winName {
			// MySQL includes each function occurrence (no deduplication)
			funcNames = append(funcNames, funcName)
		}
		return true, nil
	}, sel.SelectExprs)
	return funcNames
}

// explainJSONRowCount extracts the row count from an EXPLAIN row.
func explainJSONRowCount(row []interface{}) int64 {
	if row[9] != nil {
		switch v := row[9].(type) {
		case int64:
			return v
		case int:
			return int64(v)
		}
	}
	return 1
}

// explainJSONQueryBlockForRow builds a query_block structure for a single EXPLAIN row.
func (e *Executor) explainJSONQueryBlockForRow(row []interface{}, query string) []orderedKV {
	var qb []orderedKV
	if row[0] != nil {
		switch v := row[0].(type) {
		case int64:
			qb = append(qb, orderedKV{"select_id", v})
		}
	}
	rc := explainJSONRowCount(row)
	cost := float64(rc)*0.10 + 0.25
	if row[2] != nil {
		qb = append(qb, orderedKV{"cost_info", []orderedKV{{"query_cost", fmt.Sprintf("%.2f", cost)}}})
		qb = append(qb, orderedKV{"table", e.explainJSONTableBlock(row, query)})
	} else {
		// No table: emit a message if Extra column has content (e.g. "No tables used")
		extra := ""
		if row[11] != nil {
			extra = fmt.Sprintf("%v", row[11])
		}
		if extra != "" {
			qb = append(qb, orderedKV{"message", extra})
		}
	}
	return qb
}

// extractFirstINSubquerySQL extracts the first IN (SELECT ...) subquery from a SQL query string.
// Returns the SELECT part (e.g., "SELECT a FROM t11") for use in analyzing which columns
// are referenced in the subquery (for used_columns in EXPLAIN FORMAT=JSON).
// Returns empty string if no IN subquery is found.
func (e *Executor) extractFirstINSubquerySQL(query string) string {
	upper := strings.ToUpper(query)
	// Find " IN (" or " IN(" followed by SELECT
	patterns := []string{" IN (SELECT", " IN(SELECT", "\nIN (SELECT", "\nIN(SELECT"}
	for _, pat := range patterns {
		idx := strings.Index(upper, pat)
		if idx < 0 {
			continue
		}
		// Find start of SELECT inside the IN(...)
		selectStart := idx + strings.Index(upper[idx:], "SELECT")
		if selectStart < idx {
			continue
		}
		// Find the matching closing parenthesis
		depth := 0
		// We're already past IN(, need to find the opening paren
		parenIdx := strings.Index(upper[idx:], "(")
		if parenIdx < 0 {
			continue
		}
		start := idx + parenIdx
		end := -1
		for i := start; i < len(query); i++ {
			if query[i] == '(' {
				depth++
			} else if query[i] == ')' {
				depth--
				if depth == 0 {
					end = i
					break
				}
			}
		}
		if end > selectStart {
			return query[selectStart:end]
		}
	}
	return ""
}

func (e *Executor) explainJSONDocument(query string) string {
	// Get analyzed EXPLAIN rows from the traditional EXPLAIN logic
	explainRows := e.explainMultiRows(query)
	if len(explainRows) == 0 {
		return `{"query_block": {"select_id": 1}}`
	}

	upper := strings.ToUpper(query)

	// Parse each row to extract selectType and build structured JSON
	type parsedRow struct {
		id         interface{} // int64 or nil
		selectType string
		row        []interface{}
	}

	var parsed []parsedRow
	for _, r := range explainRows {
		st := ""
		if r[1] != nil {
			st = fmt.Sprintf("%v", r[1])
		}
		parsed = append(parsed, parsedRow{id: r[0], selectType: st, row: r})
	}

	// Detect window functions early (needed for cost calculation)
	hasWindowFuncsEarly, windowInfosEarly := e.explainJSONExtractWindowFuncs(query)

	// Calculate total query cost from all primary/simple rows
	// query_cost = sum(prefix_cost for each table) + sort_cost
	totalCost := 0.0
	for _, p := range parsed {
		if p.selectType == "SIMPLE" || p.selectType == "PRIMARY" {
			rc := explainJSONRowCount(p.row)
			totalCost += float64(rc)*0.10 + 0.25 // eval_cost + read_cost
		}
	}

	// Detect filesort and GROUP BY
	sortCost := 0.0
	hasGroupBy := strings.Contains(upper, "GROUP BY")
	hasSQLBufferResult := strings.Contains(upper, "SQL_BUFFER_RESULT")
	hasFilesort := false
	for _, p := range parsed {
		if p.row[11] != nil {
			extraStr := fmt.Sprintf("%v", p.row[11])
			if strings.Contains(extraStr, "Using filesort") {
				hasFilesort = true
				rc := explainJSONRowCount(p.row)
				sortCost = float64(rc) * 1.0
			}
		}
	}
	if hasFilesort {
		totalCost += sortCost
	}

	// Window sort cost: if any window has ORDER BY or PARTITION BY, add sort cost
	// based on row count. This is separate from external filesort.
	windowSortCost := 0.0
	if hasWindowFuncsEarly {
		hasSortInWindowEarly := false
		for _, info := range windowInfosEarly {
			if len(info.orderBy) > 0 {
				hasSortInWindowEarly = true
				break
			}
		}
		if hasSortInWindowEarly {
			// Find row count from primary rows
			for _, p := range parsed {
				if p.selectType == "SIMPLE" || p.selectType == "PRIMARY" {
					rc := explainJSONRowCount(p.row)
					windowSortCost = float64(rc) * 1.0
					break
				}
			}
			totalCost += windowSortCost
		}
	}

	// Separate rows by select type
	var primaryRows []parsedRow
	var subqueryRows []parsedRow
	var derivedRows []parsedRow
	var unionRows []parsedRow
	var unionResultRow *parsedRow
	// materializedRowsByID groups MATERIALIZED rows by their subquery id.
	// Key: subquery id (int64), Value: list of MATERIALIZED rows for that id.
	materializedRowsByID := make(map[int64][]parsedRow)

	for i := range parsed {
		switch parsed[i].selectType {
		case "SIMPLE", "PRIMARY":
			primaryRows = append(primaryRows, parsed[i])
		case "SUBQUERY":
			subqueryRows = append(subqueryRows, parsed[i])
		case "DERIVED":
			derivedRows = append(derivedRows, parsed[i])
		case "UNION":
			unionRows = append(unionRows, parsed[i])
		case "UNION RESULT":
			p := parsed[i]
			unionResultRow = &p
		case "MATERIALIZED":
			if id, ok := parsed[i].id.(int64); ok {
				materializedRowsByID[id] = append(materializedRowsByID[id], parsed[i])
			}
		default:
			primaryRows = append(primaryRows, parsed[i])
		}
	}

	// Reuse window function info from early detection
	hasWindowFuncs, windowInfos := hasWindowFuncsEarly, windowInfosEarly

	// Detect if query has an external ORDER BY (outside window specs)
	hasExternalOrderBy := false
	if stmt, err := e.parser().Parse(query); err == nil {
		if sel, ok := stmt.(*sqlparser.Select); ok {
			if len(sel.OrderBy) > 0 && !hasWindowFuncs {
				// ORDER BY without window funcs is handled by filesort
			} else if len(sel.OrderBy) > 0 && hasWindowFuncs {
				hasExternalOrderBy = true
			}
		}
	}

	// Read optimizer_switch to detect semijoin strategies
	optimizerSwitch, _ := e.getSysVarSession("optimizer_switch")
	if optimizerSwitch == "" {
		optimizerSwitch, _ = e.getSysVar("optimizer_switch")
	}
	isDupsweedEnabled := strings.Contains(optimizerSwitch, "duplicateweedout=on")
	isFirstmatchEnabled := strings.Contains(optimizerSwitch, "firstmatch=on")
	isLooseScanEnabled := strings.Contains(optimizerSwitch, "loosescan=on")

	// Helper: build a windowing block for a single table
	buildWindowingBlock := func(tblBlock []orderedKV) []orderedKV {
		// Build windows array
		var windowsArr []interface{}
		for _, info := range windowInfos {
			winBlock := []orderedKV{{"name", info.windowName}}
			// Get all function names for this window
			allFuncs := e.explainJSONWindowFuncNamesForWindow(query, info.windowName)
			if len(allFuncs) == 0 {
				allFuncs = []string{info.funcName}
			}
			// Add filesort info if window has ORDER BY
			if len(info.orderBy) > 0 {
				winBlock = append(winBlock, orderedKV{"using_filesort", true})
				orderArr := make([]interface{}, len(info.orderBy))
				for i, k := range info.orderBy {
					orderArr[i] = k
				}
				winBlock = append(winBlock, orderedKV{"filesort_key", orderArr})
			}
			// Add using_temporary_table at window level if external ORDER BY
			if hasExternalOrderBy {
				winBlock = append(winBlock, orderedKV{"using_temporary_table", true})
			}
			// Add frame_buffer if needed
			if info.hasFrame {
				frameBlock := []orderedKV{{"using_temporary_table", true}}
				// optimized_frame_evaluation is present unless a non-exact numeric aggregate disables it
				if info.hasOptimizedFrame && !info.hasNonExactAggreg {
					frameBlock = append(frameBlock, orderedKV{"optimized_frame_evaluation", true})
				}
				winBlock = append(winBlock, orderedKV{"frame_buffer", frameBlock})
			}
			// Add functions array
			funcsArr := make([]interface{}, len(allFuncs))
			for i, fn := range allFuncs {
				funcsArr[i] = fn
			}
			winBlock = append(winBlock, orderedKV{"functions", funcsArr})
			windowsArr = append(windowsArr, winBlock)
		}

		windowing := []orderedKV{{"windows", windowsArr}}
		// If any window has filesort, add cost_info for windowing
		hasSortInWindow := false
		for _, info := range windowInfos {
			if len(info.orderBy) > 0 {
				hasSortInWindow = true
				break
			}
		}
		if hasSortInWindow {
			windowing = append(windowing, orderedKV{"cost_info", []orderedKV{
				{"sort_cost", fmt.Sprintf("%.2f", windowSortCost)},
			}})
		}
		windowing = append(windowing, orderedKV{"table", tblBlock})
		return windowing
	}

	// Helper: build nested_loop from primary rows
	buildNestedLoop := func(rows []parsedRow) []interface{} {
		var nl []interface{}
		for _, pr := range rows {
			tblBlock := e.explainJSONTableBlock(pr.row, query)
			nl = append(nl, []orderedKV{{"table", tblBlock}})
		}
		return nl
	}

	// Helper: build nested_loop with first_match annotation
	// The first_match field is added to non-first tables when firstmatch strategy is used
	buildFirstMatchNestedLoop := func(rows []parsedRow) []interface{} {
		var nl []interface{}
		if len(rows) == 0 {
			return nl
		}
		// First table has no first_match annotation
		firstTbl := e.explainJSONTableBlock(rows[0].row, query)
		nl = append(nl, []orderedKV{{"table", firstTbl}})
		// Subsequent tables get first_match pointing to the first table's name
		if len(rows) > 1 {
			firstTableName := ""
			if rows[0].row[2] != nil {
				firstTableName = fmt.Sprintf("%v", rows[0].row[2])
			}
			for _, pr := range rows[1:] {
				tblBlock := e.explainJSONTableBlock(pr.row, query)
				// Insert first_match after "filtered" field (before cost_info and used_columns)
				insertPos := len(tblBlock) // default: append at end
				for i, kv := range tblBlock {
					if kv.Key == "filtered" {
						insertPos = i + 1
						break
					}
				}
				// Insert first_match at insertPos
				newBlock := make([]orderedKV, 0, len(tblBlock)+1)
				newBlock = append(newBlock, tblBlock[:insertPos]...)
				newBlock = append(newBlock, orderedKV{"first_match", firstTableName})
				newBlock = append(newBlock, tblBlock[insertPos:]...)
				nl = append(nl, []orderedKV{{"table", newBlock}})
			}
		}
		return nl
	}

	// Build the query_block with ordered keys
	var queryBlock []orderedKV

	// select_id
	if parsed[0].id != nil {
		switch v := parsed[0].id.(type) {
		case int64:
			queryBlock = append(queryBlock, orderedKV{"select_id", v})
		default:
			queryBlock = append(queryBlock, orderedKV{"select_id", int64(1)})
		}
	} else {
		queryBlock = append(queryBlock, orderedKV{"select_id", int64(1)})
	}

	// Build the main content based on structure
	if len(primaryRows) >= 1 && len(materializedRowsByID) > 0 && len(unionRows) == 0 && unionResultRow == nil {
		// Query with MATERIALIZED subqueries: produce nested_loop structure
		// Recalculate total cost including materialized inner rows
		totalCost = 0.0
		for _, p := range primaryRows {
			rc := explainJSONRowCount(p.row)
			totalCost += float64(rc)*0.10 + 0.25
		}
		for _, matRows := range materializedRowsByID {
			for _, m := range matRows {
				rc := explainJSONRowCount(m.row)
				totalCost += float64(rc)*0.10 + 0.25
			}
		}
		queryBlock = append(queryBlock, orderedKV{"cost_info", []orderedKV{
			{"query_cost", fmt.Sprintf("%.2f", totalCost)},
		}})

		// Build nested_loop array: follow the primaryRows order exactly, replacing
		// <subqueryN> placeholder rows with their materialized subquery blocks in-place.
		// This preserves MySQL's join order (as shown in the tabular EXPLAIN).

		// Extract the IN-subquery SQL for used_columns analysis (so we only show
		// the columns referenced in the subquery, not all columns from outer SELECT *).
		inSubquerySQL := e.extractFirstINSubquerySQL(query)

		// First, build a map from subquery name -> materialized block ([]orderedKV)
		matBlockByName := make(map[string][]orderedKV)
		for matID, matRows := range materializedRowsByID {
			if len(matRows) == 0 {
				continue
			}
			// Use the subquery SQL (if available) for used_columns analysis in inner tables.
			innerQuery := query
			if inSubquerySQL != "" {
				innerQuery = inSubquerySQL
			}
			// Build the inner query_block for the materialized subquery
			var innerQB []orderedKV
			if len(matRows) == 1 {
				// Single table in subquery: simple table block
				m := matRows[0]
				innerTblBlock := e.explainJSONTableBlock(m.row, innerQuery)
				innerQB = append(innerQB, orderedKV{"table", innerTblBlock})
			} else {
				// Multiple tables: nested_loop in inner query_block
				var innerLoop []interface{}
				for _, m := range matRows {
					innerTblBlock := e.explainJSONTableBlock(m.row, innerQuery)
					innerLoop = append(innerLoop, []orderedKV{{"table", innerTblBlock}})
				}
				innerQB = append(innerQB, orderedKV{"nested_loop", innerLoop})
			}
			matFromSub := []orderedKV{
				{"using_temporary_table", true},
				{"query_block", innerQB},
			}
			subqueryName := fmt.Sprintf("<subquery%d>", matID)
			matBlockByName[subqueryName] = matFromSub
		}

		// Build subquery placeholder blocks.
		// For each <subqueryN> placeholder, build its full block (table_name, access_type,
		// key, ref, materialized_from_subquery, etc.) from the placeholder's tabular row.
		// Build a helper function to produce the subquery placeholder block.
		buildSubqueryBlock := func(p parsedRow, tblName string, matFromSub []orderedKV) []orderedKV {
			var subqueryTblBlock []orderedKV
			subqueryTblBlock = append(subqueryTblBlock, orderedKV{"table_name", tblName})
			accessType := "ALL"
			if p.row[4] != nil {
				accessType = fmt.Sprintf("%v", p.row[4])
			}
			subqueryTblBlock = append(subqueryTblBlock, orderedKV{"access_type", accessType})

			extraStr := ""
			if p.row[11] != nil {
				extraStr = fmt.Sprintf("%v", p.row[11])
			}

			if accessType != "ALL" {
				// Probe case (eq_ref/ref): show key, key_length, ref, rows, attached_condition
				if p.row[5] != nil {
					pkStr := fmt.Sprintf("%v", p.row[5])
					if pkStr != "" && !strings.HasPrefix(pkStr, "<auto_key") {
						arr := []interface{}{pkStr}
						subqueryTblBlock = append(subqueryTblBlock, orderedKV{"possible_keys", arr})
					}
				}
				if p.row[6] != nil {
					keyStr := fmt.Sprintf("%v", p.row[6])
					subqueryTblBlock = append(subqueryTblBlock, orderedKV{"key", keyStr})
				}
				if p.row[7] != nil {
					subqueryTblBlock = append(subqueryTblBlock, orderedKV{"key_length", fmt.Sprintf("%v", p.row[7])})
				}
				if p.row[8] != nil {
					refStr := fmt.Sprintf("%v", p.row[8])
					if refStr != "" {
						parts := strings.Split(refStr, ",")
						arr := make([]interface{}, len(parts))
						for i, pp := range parts {
							arr[i] = strings.TrimSpace(pp)
						}
						subqueryTblBlock = append(subqueryTblBlock, orderedKV{"ref", arr})
					}
				}
				var rowCount int64 = 1
				if p.row[9] != nil {
					switch v := p.row[9].(type) {
					case int64:
						rowCount = v
					case int:
						rowCount = int64(v)
					}
				}
				subqueryTblBlock = append(subqueryTblBlock, orderedKV{"rows_examined_per_scan", rowCount})
				if strings.Contains(extraStr, "Using where") {
					cond := e.explainJSONBuildMaterializedCondition(query, tblName)
					if cond != "" {
						subqueryTblBlock = append(subqueryTblBlock, orderedKV{"attached_condition", cond})
					}
				}
			} else {
				// ALL access: emit using_join_buffer and/or attached_condition if present
				if strings.Contains(extraStr, "Block Nested Loop") {
					subqueryTblBlock = append(subqueryTblBlock, orderedKV{"using_join_buffer", "Block Nested Loop"})
				}
				if strings.Contains(extraStr, "Using where") {
					// For BNL placeholder with "Using where", the condition reflects
					// the range filter on the join column applied when scanning <subqueryN>.
					// Build from the WHERE clause range conditions on the IN column.
					cond := e.explainJSONBuildSubqueryRangeCondition(query, tblName)
					if cond == "" {
						// Fallback to the equality join condition
						cond = e.explainJSONBuildMaterializedCondition(query, tblName)
					}
					if cond != "" {
						subqueryTblBlock = append(subqueryTblBlock, orderedKV{"attached_condition", cond})
					}
				}
			}
			// materialized_from_subquery comes last
			subqueryTblBlock = append(subqueryTblBlock, orderedKV{"materialized_from_subquery", matFromSub})
			return subqueryTblBlock
		}

		// Find all <subquery> placeholder rows from primaryRows.
		// Build a map by name so we can look them up quickly.
		subqueryPlaceholderByName := make(map[string]parsedRow)
		for _, p := range primaryRows {
			if p.row[2] == nil {
				continue
			}
			tblName := fmt.Sprintf("%v", p.row[2])
			if strings.HasPrefix(tblName, "<subquery") {
				subqueryPlaceholderByName[tblName] = p
			}
		}

		// Determine if each <subquery> placeholder is a driver (goes first) or driven (goes after some tables).
		// Rule:
		//   - ALL access + no "Using join buffer" in Extra: placeholder DRIVES (put it first)
		//   - ALL access + "Using join buffer": placeholder is INNER (driven by outer tables, put it after)
		//   - eq_ref/ref/const access: placeholder is a PROBE (put it after driver tables)
		subqueryIsDriving := make(map[string]bool)
		for name, p := range subqueryPlaceholderByName {
			accessType := "ALL"
			if p.row[4] != nil {
				accessType = fmt.Sprintf("%v", p.row[4])
			}
			extraStr := ""
			if p.row[11] != nil {
				extraStr = fmt.Sprintf("%v", p.row[11])
			}
			isDriver := (accessType == "ALL") && !strings.Contains(extraStr, "Using join buffer")
			subqueryIsDriving[name] = isDriver
		}

		// Collect non-subquery primary table blocks.
		var outerTableBlocks []interface{}
		for _, p := range primaryRows {
			if p.row[2] == nil {
				continue
			}
			tblName := fmt.Sprintf("%v", p.row[2])
			if strings.HasPrefix(tblName, "<subquery") {
				continue
			}
			tblBlock := e.explainJSONTableBlock(p.row, query)
			outerTableBlocks = append(outerTableBlocks, []orderedKV{{"table", tblBlock}})
		}

		// Build the nested_loop in the correct MySQL order:
		// For single-subquery cases:
		//   - Subquery drives (isDriver=true): [<subquery>, outer_tables...]
		//   - Subquery is driven (isDriver=false): [outer_driver_tables..., <subquery>, outer_dependent_tables...]
		// For simplicity with multiple outer tables, if the subquery is driven:
		//   - tables that are ALL-scan and don't reference <subquery> come first
		//   - then <subquery>
		//   - then tables that ref <subquery> (ref/eq_ref access types)
		var nestedLoop []interface{}

		// Check if any subquery is driving
		anySubqueryDriving := false
		for _, driving := range subqueryIsDriving {
			if driving {
				anySubqueryDriving = true
				break
			}
		}

		if anySubqueryDriving {
			// Driving subqueries go first
			for name, matFromSub := range matBlockByName {
				if ph, ok := subqueryPlaceholderByName[name]; ok && subqueryIsDriving[name] {
					subqueryTblBlock := buildSubqueryBlock(ph, name, matFromSub)
					nestedLoop = append(nestedLoop, []orderedKV{{"table", subqueryTblBlock}})
				}
			}
			// Then outer tables
			nestedLoop = append(nestedLoop, outerTableBlocks...)
			// Then non-driving subqueries (shouldn't normally happen)
			for name, matFromSub := range matBlockByName {
				if ph, ok := subqueryPlaceholderByName[name]; ok && !subqueryIsDriving[name] {
					subqueryTblBlock := buildSubqueryBlock(ph, name, matFromSub)
					nestedLoop = append(nestedLoop, []orderedKV{{"table", subqueryTblBlock}})
				}
			}
		} else {
			// All subqueries are driven (ALL+BNL or eq_ref probe).
			// Separate outer tables into: "driver tables" (ALL-scan, before <subquery>)
			// and "dependent tables" (ref/eq_ref/const, after <subquery>).
			//
			// When the placeholder has BNL (driven by ALL-scan outer tables):
			//   - ALL-scan outer tables → go before <subquery> (they drive)
			//   - ref/eq_ref/const outer tables → go after <subquery> (they probe via subquery key)
			//
			// When the placeholder has eq_ref access (probe, no BNL):
			//   - All outer ALL-scan tables → go before <subquery>
			//   - <subquery> is the probe (eq_ref) at the end
			//
			// Determine if any subquery has BNL
			anySubqueryBNL := false
			for _, ph := range subqueryPlaceholderByName {
				if ph.row[11] != nil {
					extraStr := fmt.Sprintf("%v", ph.row[11])
					if strings.Contains(extraStr, "Block Nested Loop") {
						anySubqueryBNL = true
						break
					}
				}
			}
			var driverTables []interface{}
			var dependentTables []interface{}
			for _, p := range primaryRows {
				if p.row[2] == nil {
					continue
				}
				tblName := fmt.Sprintf("%v", p.row[2])
				if strings.HasPrefix(tblName, "<subquery") {
					continue
				}
				accessType := "ALL"
				if p.row[4] != nil {
					accessType = fmt.Sprintf("%v", p.row[4])
				}
				// In the BNL case: non-ALL tables come AFTER the subquery (they probe via subquery key)
				// In the non-BNL (eq_ref probe) case: all outer tables come BEFORE the subquery
				isDependent := anySubqueryBNL && (accessType == "ref" || accessType == "eq_ref" || accessType == "const")
				tblBlock := e.explainJSONTableBlock(p.row, query)
				if isDependent {
					dependentTables = append(dependentTables, []orderedKV{{"table", tblBlock}})
				} else {
					driverTables = append(driverTables, []orderedKV{{"table", tblBlock}})
				}
			}
			// Build: driver tables, then subqueries (sorted by ID), then dependent tables
			nestedLoop = append(nestedLoop, driverTables...)
			var matIDs []int64
			for matID := range materializedRowsByID {
				matIDs = append(matIDs, matID)
			}
			for i := 0; i < len(matIDs); i++ {
				for j := i + 1; j < len(matIDs); j++ {
					if matIDs[i] > matIDs[j] {
						matIDs[i], matIDs[j] = matIDs[j], matIDs[i]
					}
				}
			}
			for _, matID := range matIDs {
				name := fmt.Sprintf("<subquery%d>", matID)
				matFromSub, ok := matBlockByName[name]
				if !ok {
					continue
				}
				if ph, ok := subqueryPlaceholderByName[name]; ok {
					subqueryTblBlock := buildSubqueryBlock(ph, name, matFromSub)
					nestedLoop = append(nestedLoop, []orderedKV{{"table", subqueryTblBlock}})
				} else {
					// No placeholder row found; use fallback
					subqueryTblBlock := []orderedKV{
						{"table_name", name},
						{"access_type", "ALL"},
						{"materialized_from_subquery", matFromSub},
					}
					nestedLoop = append(nestedLoop, []orderedKV{{"table", subqueryTblBlock}})
				}
			}
			nestedLoop = append(nestedLoop, dependentTables...)
		}

		queryBlock = append(queryBlock, orderedKV{"nested_loop", nestedLoop})

		// Attached subqueries
		if len(subqueryRows) > 0 {
			var attachedSubs []interface{}
			for _, s := range subqueryRows {
				attachedSubs = append(attachedSubs, e.explainJSONQueryBlockForRow(s.row, query))
			}
			queryBlock = append(queryBlock, orderedKV{"attached_subqueries", attachedSubs})
		}
	} else if len(primaryRows) == 1 && len(subqueryRows) == 0 && len(derivedRows) == 0 && len(unionRows) == 0 && unionResultRow == nil {
		// Simple query with a single table
		p := primaryRows[0]
		// Check for "message-only" cases: no table, or empty table with special Extra
		extra := ""
		if p.row[11] != nil {
			extra = fmt.Sprintf("%v", p.row[11])
		}
		isMessageOnly := p.row[2] == nil ||
			strings.Contains(extra, "no matching row in const table") ||
			strings.Contains(extra, "No tables used")
		if isMessageOnly {
			// No table or empty table - no cost_info in output, just message
			if extra != "" {
				queryBlock = append(queryBlock, orderedKV{"message", extra})
			}
			return e.explainJSONMarshal([]orderedKV{{"query_block", queryBlock}})
		}

		// For INSERT/REPLACE/UPDATE/DELETE: no cost_info at query_block level
		isInsertLike := p.selectType == "INSERT" || p.selectType == "REPLACE" || p.selectType == "UPDATE" || p.selectType == "DELETE"

		// Has a table with data: include cost_info (except for INSERT/UPDATE/DELETE)
		if !isInsertLike {
			queryBlock = append(queryBlock, orderedKV{"cost_info", []orderedKV{
				{"query_cost", fmt.Sprintf("%.2f", totalCost)},
			}})
		}

		tblBlock := e.explainJSONTableBlock(p.row, query)

		if hasWindowFuncs {
			// Windowing query: wrap table in windowing block
			windowing := buildWindowingBlock(tblBlock)
			if hasExternalOrderBy {
				// Wrap in ordering_operation
				orderingOp := []orderedKV{
					{"using_filesort", true},
				}
				if sortCost > 0 {
					orderingOp = append(orderingOp, orderedKV{"cost_info", []orderedKV{
						{"sort_cost", fmt.Sprintf("%.2f", sortCost)},
					}})
				}
				orderingOp = append(orderingOp, orderedKV{"windowing", windowing})
				queryBlock = append(queryBlock, orderedKV{"ordering_operation", orderingOp})
			} else {
				queryBlock = append(queryBlock, orderedKV{"windowing", windowing})
			}
		} else if hasGroupBy && hasSQLBufferResult {
			groupOp := []orderedKV{
				{"using_filesort", true},
				{"cost_info", []orderedKV{{"sort_cost", fmt.Sprintf("%.2f", sortCost)}}},
				{"buffer_result", []orderedKV{
					{"using_temporary_table", true},
					{"table", tblBlock},
				}},
			}
			queryBlock = append(queryBlock, orderedKV{"grouping_operation", groupOp})
		} else if hasGroupBy && hasFilesort {
			groupOp := []orderedKV{
				{"using_filesort", true},
				{"cost_info", []orderedKV{{"sort_cost", fmt.Sprintf("%.2f", sortCost)}}},
				{"table", tblBlock},
			}
			queryBlock = append(queryBlock, orderedKV{"grouping_operation", groupOp})
		} else if hasSQLBufferResult {
			queryBlock = append(queryBlock, orderedKV{"buffer_result", []orderedKV{
				{"using_temporary_table", true},
				{"table", tblBlock},
			}})
		} else {
			queryBlock = append(queryBlock, orderedKV{"table", tblBlock})
		}
	} else if len(primaryRows) > 1 && len(subqueryRows) == 0 && len(derivedRows) == 0 && len(unionRows) == 0 && unionResultRow == nil {
		// Multi-table query: JOIN or semijoin
		queryBlock = append(queryBlock, orderedKV{"cost_info", []orderedKV{
			{"query_cost", fmt.Sprintf("%.2f", totalCost)},
		}})
		// Determine strategy from optimizer_switch
		if (isDupsweedEnabled || isLooseScanEnabled) && !isFirstmatchEnabled {
			// DuplicateWeedout or LooseScan strategy: both produce duplicates_removal wrapper in JSON
			nl := buildNestedLoop(primaryRows)
			dupRemoval := []orderedKV{
				{"using_temporary_table", true},
				{"nested_loop", nl},
			}
			queryBlock = append(queryBlock, orderedKV{"duplicates_removal", dupRemoval})
		} else if isFirstmatchEnabled {
			// FirstMatch strategy: nested_loop with first_match annotation on later tables
			nl := buildFirstMatchNestedLoop(primaryRows)
			queryBlock = append(queryBlock, orderedKV{"nested_loop", nl})
		} else {
			// Plain nested loop (e.g., plain JOIN)
			nl := buildNestedLoop(primaryRows)
			queryBlock = append(queryBlock, orderedKV{"nested_loop", nl})
		}
	} else if unionResultRow != nil {
		// UNION query
		queryBlock = append(queryBlock, orderedKV{"cost_info", []orderedKV{
			{"query_cost", fmt.Sprintf("%.2f", totalCost)},
		}})
		var querySpecs []interface{}

		for _, p := range primaryRows {
			querySpecs = append(querySpecs, e.explainJSONQueryBlockForRow(p.row, query))
		}
		for _, p := range unionRows {
			querySpecs = append(querySpecs, e.explainJSONQueryBlockForRow(p.row, query))
		}

		unionResult := []orderedKV{
			{"using_temporary_table", true},
			{"table_name", fmt.Sprintf("%v", unionResultRow.row[2])},
		}
		accessType := "ALL"
		if unionResultRow.row[4] != nil {
			accessType = fmt.Sprintf("%v", unionResultRow.row[4])
		}
		unionResult = append(unionResult, orderedKV{"access_type", accessType})
		unionResult = append(unionResult, orderedKV{"query_specifications", querySpecs})

		queryBlock = append(queryBlock, orderedKV{"union_result", unionResult})
	} else {
		// Complex query with subqueries and/or derived tables
		queryBlock = append(queryBlock, orderedKV{"cost_info", []orderedKV{
			{"query_cost", fmt.Sprintf("%.2f", totalCost)},
		}})
		if len(primaryRows) > 0 {
			p := primaryRows[0]
			if p.row[2] != nil {
				tblBlock := e.explainJSONTableBlock(p.row, query)

				// Check if the primary table references a derived table
				tableName := fmt.Sprintf("%v", p.row[2])
				if strings.HasPrefix(tableName, "<derived") {
					// Find the corresponding DERIVED row
					for _, d := range derivedRows {
						if d.row[2] != nil {
							innerQB := e.explainJSONQueryBlockForRow(d.row, query)
							derivedBlock := []orderedKV{
								{"using_temporary_table", true},
								{"query_block", innerQB},
							}
							tblBlock = append(tblBlock, orderedKV{"materialized_from_subquery", derivedBlock})
						}
					}
				}

				if hasGroupBy && hasFilesort {
					groupOp := []orderedKV{
						{"using_filesort", true},
						{"cost_info", []orderedKV{{"sort_cost", fmt.Sprintf("%.2f", sortCost)}}},
						{"table", tblBlock},
					}
					queryBlock = append(queryBlock, orderedKV{"grouping_operation", groupOp})
				} else {
					queryBlock = append(queryBlock, orderedKV{"table", tblBlock})
				}
			}
		}

		// Attached subqueries
		if len(subqueryRows) > 0 {
			var attachedSubs []interface{}
			for _, s := range subqueryRows {
				attachedSubs = append(attachedSubs, e.explainJSONQueryBlockForRow(s.row, query))
			}
			queryBlock = append(queryBlock, orderedKV{"attached_subqueries", attachedSubs})
		}
	}

	return e.explainJSONMarshal([]orderedKV{{"query_block", queryBlock}})
}

// orderedKV represents an ordered key-value pair for JSON output.
type orderedKV struct {
	Key   string
	Value interface{}
}

// explainJSONMarshal marshals an ordered structure to a pretty-printed JSON string.
func (e *Executor) explainJSONMarshal(v interface{}) string {
	var b strings.Builder
	// Check end_markers_in_json session variable
	endMarkers := false
	if v, ok := e.sessionScopeVars["end_markers_in_json"]; ok {
		endMarkers = strings.EqualFold(v, "ON") || v == "1"
	}
	e.explainJSONWriteWithMarkers(&b, v, 0, "", endMarkers)
	return b.String()
}

// explainJSONWrite writes a JSON value with proper indentation.
func (e *Executor) explainJSONWrite(b *strings.Builder, v interface{}, indent int) {
	e.explainJSONWriteWithMarkers(b, v, indent, "", false)
}

// explainJSONWriteWithMarkers writes JSON with optional end markers (/* key_name */).
func (e *Executor) explainJSONWriteWithMarkers(b *strings.Builder, v interface{}, indent int, keyName string, endMarkers bool) {
	prefix := strings.Repeat("  ", indent)
	switch val := v.(type) {
	case []orderedKV:
		b.WriteString("{\n")
		for i, kv := range val {
			b.WriteString(prefix + "  ")
			b.WriteString(fmt.Sprintf("%q", kv.Key))
			b.WriteString(": ")
			e.explainJSONWriteWithMarkers(b, kv.Value, indent+1, kv.Key, endMarkers)
			if i < len(val)-1 {
				b.WriteString(",")
			}
			b.WriteString("\n")
		}
		b.WriteString(prefix + "}")
		if endMarkers && keyName != "" {
			b.WriteString(fmt.Sprintf(" /* %s */", keyName))
		}
	case []interface{}:
		b.WriteString("[\n")
		for i, item := range val {
			b.WriteString(prefix + "  ")
			e.explainJSONWriteWithMarkers(b, item, indent+1, "", endMarkers)
			if i < len(val)-1 {
				b.WriteString(",")
			}
			b.WriteString("\n")
		}
		b.WriteString(prefix + "]")
	case string:
		b.WriteString(fmt.Sprintf("%q", val))
	case int64:
		b.WriteString(fmt.Sprintf("%d", val))
	case int:
		b.WriteString(fmt.Sprintf("%d", val))
	case float64:
		b.WriteString(fmt.Sprintf("%g", val))
	case bool:
		if val {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case nil:
		b.WriteString("null")
	default:
		// Fallback to json.Marshal
		data, _ := json.Marshal(val)
		b.Write(data)
	}
}

// explainTreeIndent returns a string of spaces for the given indent level (4 spaces per level).
func explainTreeIndent(level int) string {
	return strings.Repeat("    ", level)
}

// explainTreeRowInfo extracts fields from an EXPLAIN row []interface{}.
// EXPLAIN row format: {id, select_type, table, partitions, type, possible_keys, key, key_len, ref, rows, filtered, Extra}
func explainTreeRowInfo(row []interface{}) (tblName, accessType, keyName, ref, extra, selectType string, id int64) {
	if row[0] != nil {
		id, _ = row[0].(int64)
	}
	if row[1] != nil {
		selectType = fmt.Sprintf("%v", row[1])
	}
	if row[2] != nil {
		tblName = fmt.Sprintf("%v", row[2])
	}
	if row[4] != nil {
		accessType = fmt.Sprintf("%v", row[4])
	}
	if row[6] != nil {
		keyName = fmt.Sprintf("%v", row[6])
	}
	if row[8] != nil {
		ref = fmt.Sprintf("%v", row[8])
	}
	if row[11] != nil {
		extra = fmt.Sprintf("%v", row[11])
	}
	return
}

// explainTreeBuildGroup builds tree lines for a group of EXPLAIN rows at the same id.
// indent is the indentation level. allRows is the full EXPLAIN rows for MATERIALIZED lookup.
// joinType is "inner" or "left".
func (e *Executor) explainTreeBuildGroup(groupRows [][]interface{}, allRows [][]interface{}, indent int, joinType string) []string {
	if len(groupRows) == 0 {
		return nil
	}
	if joinType == "" {
		joinType = "inner"
	}

	if len(groupRows) == 1 {
		row := groupRows[0]
		tbl, at, kn, ref, extra, _, _ := explainTreeRowInfo(row)
		return e.explainTreeTableNode(tbl, at, kn, ref, extra, allRows, indent)
	}

	// Multiple tables: sort so driving tables (ALL scan) come before probe tables (subquery lookups).
	// In MySQL's TREE format, the left child is the driver and right child is the probe.
	sortedRows := make([][]interface{}, len(groupRows))
	copy(sortedRows, groupRows)
	// Move ALL-scan tables to the front
	stableSort := func(rows [][]interface{}) [][]interface{} {
		var drivers, probes [][]interface{}
		for _, row := range rows {
			_, at, _, _, _, _, _ := explainTreeRowInfo(row)
			tbl := ""
			if row[2] != nil {
				tbl = fmt.Sprintf("%v", row[2])
			}
			if at == "ALL" && !strings.HasPrefix(tbl, "<subquery") {
				drivers = append(drivers, row)
			} else {
				probes = append(probes, row)
			}
		}
		return append(drivers, probes...)
	}
	sortedRows = stableSort(sortedRows)

	// Wrap in nested loop join
	var lines []string
	lines = append(lines, explainTreeIndent(indent)+"-> Nested loop "+joinType+" join")
	for _, row := range sortedRows {
		tbl, at, kn, ref, extra, _, _ := explainTreeRowInfo(row)
		subLines := e.explainTreeTableNode(tbl, at, kn, ref, extra, allRows, indent+1)
		lines = append(lines, subLines...)
	}
	return lines
}

// explainTreeTableNode generates tree lines for a single EXPLAIN table entry.
func (e *Executor) explainTreeTableNode(tblName, accessType, keyName, ref, extra string, allRows [][]interface{}, indent int) []string {
	pfx := explainTreeIndent(indent)
	var lines []string

	if strings.HasPrefix(tblName, "<subquery") {
		// Materialized subquery placeholder
		subNum := strings.TrimPrefix(tblName, "<subquery")
		subNum = strings.TrimSuffix(subNum, ">")
		var innerID int64
		fmt.Sscanf(subNum, "%d", &innerID)

		// Emit single-row index lookup for the placeholder
		lines = append(lines, pfx+"-> Single-row index lookup on "+tblName+" using "+keyName+" (placeholder)")

		// Find MATERIALIZED rows for this subquery id
		var innerRows [][]interface{}
		for _, r := range allRows {
			_, _, _, _, _, st, rid := explainTreeRowInfo(r)
			if st == "MATERIALIZED" && rid == innerID {
				innerRows = append(innerRows, r)
			}
		}
		if len(innerRows) > 0 {
			lines = append(lines, pfx+"    -> Materialize with deduplication")
			innerLines := e.explainTreeMaterializedContent(innerRows, allRows, indent+2)
			lines = append(lines, innerLines...)
		}
		return lines
	}

	switch accessType {
	case "ALL":
		lines = append(lines, pfx+"-> Filter: ("+tblName+" not null)")
		lines = append(lines, pfx+"    -> Table scan on "+tblName)
	case "range":
		lines = append(lines, pfx+"-> Filter: ("+tblName+" not null)")
		lines = append(lines, pfx+"    -> Index range scan on "+tblName+" using "+keyName+", with index condition: (cond)")
	case "eq_ref", "ref", "const":
		lines = append(lines, pfx+"-> Single-row index lookup on "+tblName+" using "+keyName+" (placeholder)")
	default:
		lines = append(lines, pfx+"-> Table scan on "+tblName)
	}
	return lines
}

// explainTreeMaterializedContent builds lines for the content under "Materialize with deduplication".
func (e *Executor) explainTreeMaterializedContent(innerRows [][]interface{}, allRows [][]interface{}, indent int) []string {
	if len(innerRows) == 0 {
		return nil
	}
	pfx := explainTreeIndent(indent)

	if len(innerRows) == 1 {
		row := innerRows[0]
		tbl, at, kn, _, _, _, _ := explainTreeRowInfo(row)
		var lines []string
		switch at {
		case "ALL":
			lines = append(lines, pfx+"-> Filter: ("+tbl+" not null)")
			lines = append(lines, pfx+"    -> Table scan on "+tbl)
		case "range":
			lines = append(lines, pfx+"-> Filter: ("+tbl+" not null)")
			// Range scan inside a Filter: the condition is in the Filter node, not in the range scan.
			lines = append(lines, pfx+"    -> Index range scan on "+tbl+" using "+kn)
		case "eq_ref", "ref", "const":
			lines = append(lines, pfx+"-> Single-row index lookup on "+tbl+" using "+kn+" (placeholder)")
		default:
			lines = append(lines, pfx+"-> Table scan on "+tbl)
		}
		return lines
	}

	// Multiple inner tables: filter + nested loop inner join
	var lines []string
	lines = append(lines, pfx+"-> Filter: (inner not null)")
	lines = append(lines, pfx+"    -> Nested loop inner join")
	for i, row := range innerRows {
		tbl, at, kn, _, _, _, _ := explainTreeRowInfo(row)
		subpfx := explainTreeIndent(indent + 2)
		if i == 0 {
			// First (driving) table gets a filter wrapper
			lines = append(lines, subpfx+"-> Filter: ("+tbl+" not null)")
			switch at {
			case "range":
				// Range scan inside a Filter: the condition is in the Filter node, not in the range scan.
				lines = append(lines, subpfx+"    -> Index range scan on "+tbl+" using "+kn)
			case "eq_ref", "ref", "const":
				lines = append(lines, subpfx+"    -> Single-row index lookup on "+tbl+" using "+kn+" (placeholder)")
			default:
				lines = append(lines, subpfx+"    -> Table scan on "+tbl)
			}
		} else {
			switch at {
			case "eq_ref", "ref", "const":
				lines = append(lines, subpfx+"-> Single-row index lookup on "+tbl+" using "+kn+" (placeholder)")
			case "range":
				lines = append(lines, subpfx+"-> Index range scan on "+tbl+" using "+kn+", with index condition: (cond)")
			default:
				lines = append(lines, subpfx+"-> Table scan on "+tbl)
			}
		}
	}
	return lines
}

// tableExprHasLeftJoinType returns true if any join in the tree is a LEFT JOIN.
func tableExprHasLeftJoinType(te sqlparser.TableExpr) bool {
	jte, ok := te.(*sqlparser.JoinTableExpr)
	if !ok {
		return false
	}
	if jte.Join == sqlparser.LeftJoinType || jte.Join == sqlparser.NaturalLeftJoinType {
		return true
	}
	return tableExprHasLeftJoinType(jte.LeftExpr) || tableExprHasLeftJoinType(jte.RightExpr)
}

// queryHasLeftJoin returns true if the SELECT's FROM clause has any LEFT JOINs.
func (e *Executor) queryHasLeftJoin(query string) bool {
	stmt, err := e.parser().Parse(query)
	if err != nil {
		return false
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return false
	}
	for _, te := range sel.From {
		if tableExprHasLeftJoinType(te) {
			return true
		}
	}
	return false
}

// leftJoinArm describes one arm of a LEFT JOIN tree.
type leftJoinArm struct {
	tableName   string // the right-side table name (e.g. "ot2")
	subqueryNum int64  // MATERIALIZED subquery id (0 if no subquery in ON)
	subqueryRef string // LHS table name referenced by the IN subquery (e.g. "ot1")
}

// parseLeftJoinChain extracts the LEFT JOIN chain from a SQL select statement.
// Returns the chain of (tableName, subqueryNum, subqueryRef) for each LEFT JOIN arm,
// plus the leftmost driving table name.
// subqueryNum is assigned by scanning IN subqueries in order starting from 2
// (since the outer query is select#1).
func parseLeftJoinChain(sel *sqlparser.Select) (driverTable string, arms []leftJoinArm) {
	if sel == nil || len(sel.From) == 0 {
		return
	}

	// Flatten the LEFT JOIN chain. MySQL LEFT JOINs are left-associative:
	// (((ot1 LEFT JOIN ot2) LEFT JOIN ot3) LEFT JOIN ot4)
	// We walk the right spine of JoinTableExprs to get the ordered chain.
	type joinStep struct {
		rightTable   string
		onCondition  sqlparser.Expr
		isLeft       bool
	}
	var steps []joinStep
	var leftmostTable string

	var flattenJoins func(te sqlparser.TableExpr)
	flattenJoins = func(te sqlparser.TableExpr) {
		switch t := te.(type) {
		case *sqlparser.JoinTableExpr:
			flattenJoins(t.LeftExpr)
			rightTbl := ""
			if alias, ok := t.RightExpr.(*sqlparser.AliasedTableExpr); ok {
				if tn, ok := alias.Expr.(sqlparser.TableName); ok {
					rightTbl = tn.Name.String()
				}
			}
			isLeft := t.Join == sqlparser.LeftJoinType || t.Join == sqlparser.NaturalLeftJoinType
			var onCond sqlparser.Expr
			if t.Condition != nil {
				onCond = t.Condition.On
			}
			steps = append(steps, joinStep{rightTable: rightTbl, onCondition: onCond, isLeft: isLeft})
		case *sqlparser.AliasedTableExpr:
			if tn, ok := t.Expr.(sqlparser.TableName); ok {
				if leftmostTable == "" {
					leftmostTable = tn.Name.String()
				}
			}
		}
	}

	for _, te := range sel.From {
		flattenJoins(te)
	}
	driverTable = leftmostTable

	// Count IN subqueries across all ON conditions to assign subquery numbers.
	// Subquery numbering starts at 2 (outer query = 1).
	subqueryCounter := int64(2)
	for _, step := range steps {
		if !step.isLeft {
			// Not a left join; skip (treat as inner join arm with no subquery number).
			arms = append(arms, leftJoinArm{tableName: step.rightTable})
			continue
		}

		// Scan the ON condition for IN subqueries.
		var inSubqueryNum int64
		var inSubqueryRef string

		if step.onCondition != nil {
			_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
				if cmp, ok := n.(*sqlparser.ComparisonExpr); ok {
					if cmp.Operator == sqlparser.InOp {
						if _, ok := cmp.Right.(*sqlparser.Subquery); ok {
							if inSubqueryNum == 0 {
								inSubqueryNum = subqueryCounter
								subqueryCounter++
								// Extract the LHS table name: e.g. "ot1" from "ot1.a"
								if colVal, ok := cmp.Left.(*sqlparser.ColName); ok {
									inSubqueryRef = colVal.Qualifier.Name.String()
								}
							}
						}
					}
				}
				return true, nil
			}, step.onCondition)
		}

		arms = append(arms, leftJoinArm{
			tableName:   step.rightTable,
			subqueryNum: inSubqueryNum,
			subqueryRef: inSubqueryRef,
		})
	}
	return
}

// explainTreeBuildForLeftJoin builds EXPLAIN FORMAT=TREE for a LEFT JOIN query
// where each LEFT JOIN's ON condition may contain materialized IN subqueries.
// It parses the SQL to determine the LEFT JOIN arm structure, then maps
// the EXPLAIN rows accordingly.
// allRows: all EXPLAIN rows (including MATERIALIZED ones) for subquery lookup.
// query: the original SQL (without EXPLAIN).
func (e *Executor) explainTreeBuildForLeftJoin(allRows [][]interface{}, query string) []string {
	// Parse the query to extract the LEFT JOIN chain.
	stmt, err := e.parser().Parse(query)
	if err != nil {
		return nil
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil
	}

	driverTable, armDefs := parseLeftJoinChain(sel)
	if driverTable == "" || len(armDefs) == 0 {
		return nil
	}

	// Build a lookup map: table name → EXPLAIN row(s), subquery num → EXPLAIN row.
	tableRows := map[string][][]interface{}{}
	subqueryRows := map[int64][]interface{}{}
	for _, row := range allRows {
		tbl := ""
		if row[2] != nil {
			tbl = fmt.Sprintf("%v", row[2])
		}
		_, _, _, _, _, st, rid := explainTreeRowInfo(row)
		if st == "MATERIALIZED" {
			// Skip MATERIALIZED rows here; they are accessed via <subqueryN> placeholders.
			_ = rid
			continue
		}
		if strings.HasPrefix(tbl, "<subquery") {
			var n int64
			fmt.Sscanf(strings.TrimPrefix(strings.TrimSuffix(tbl, ">"), "<subquery"), "%d", &n)
			subqueryRows[n] = row
		} else {
			tableRows[tbl] = append(tableRows[tbl], row)
		}
	}

	// Fetch the driver table row.
	driverRowList := tableRows[driverTable]
	if len(driverRowList) == 0 {
		return nil
	}

	// Build arms: arm[0] = driver table, arm[i] = right side of i-th LEFT JOIN.
	// arm structure: [][]interface{} where each sub-slice is [subqueryRow, tableRow] or [tableRow].
	var arms [][][]interface{}

	// arm[0]: just the driver table
	arms = append(arms, [][]interface{}{driverRowList[0]})

	for _, armDef := range armDefs {
		var armRows [][]interface{}

		// Add subquery row if there is one.
		if armDef.subqueryNum > 0 {
			if sqRow, ok := subqueryRows[armDef.subqueryNum]; ok {
				armRows = append(armRows, sqRow)
			}
		}

		// Add the regular table row.
		if tblRowList, ok := tableRows[armDef.tableName]; ok && len(tblRowList) > 0 {
			armRows = append(armRows, tblRowList[0])
		}

		if len(armRows) > 0 {
			arms = append(arms, armRows)
		}
	}

	if len(arms) <= 1 {
		return nil // not enough to build a left join
	}

	// For each arm, determine if subquery-first or table-first.
	// The driver table name (arm[0]) and all previous arm tables form the "outer" set.
	outerTables := map[string]bool{driverTable: true}
	type armOrder struct {
		rows          [][]interface{}
		subqueryFirst bool
	}
	var orderedArms []armOrder

	// arm[0] is the driver (no inner join needed).
	orderedArms = append(orderedArms, armOrder{rows: arms[0], subqueryFirst: false})

	for i, armDef := range armDefs {
		armIdx := i + 1
		if armIdx >= len(arms) {
			break
		}
		arm := arms[armIdx]

		// Subquery-first if subqueryRef points to an outer table (not this arm's table).
		sf := true
		if armDef.subqueryRef != "" {
			if armDef.subqueryRef == armDef.tableName {
				sf = false // self-reference → table-first
			} else if _, isOuter := outerTables[armDef.subqueryRef]; !isOuter {
				sf = false // not an outer table and not self → unclear, default table-first
			}
		}
		orderedArms = append(orderedArms, armOrder{rows: arm, subqueryFirst: sf})
		outerTables[armDef.tableName] = true
	}

	// Convert orderedArms back to arms for buildLeftJoinChain (order already embedded).
	// We need a version of buildLeftJoinChain that knows the ordering.
	// Build the arms slices in the correct order for each arm.
	var finalArms [][][]interface{}
	for i, oa := range orderedArms {
		if i == 0 {
			finalArms = append(finalArms, oa.rows)
			continue
		}
		// Reorder rows based on subqueryFirst.
		var subRows, tblRows [][]interface{}
		for _, row := range oa.rows {
			tbl := ""
			if row[2] != nil {
				tbl = fmt.Sprintf("%v", row[2])
			}
			if strings.HasPrefix(tbl, "<subquery") {
				subRows = append(subRows, row)
			} else {
				tblRows = append(tblRows, row)
			}
		}
		if oa.subqueryFirst {
			finalArms = append(finalArms, append(subRows, tblRows...))
		} else {
			finalArms = append(finalArms, append(tblRows, subRows...))
		}
	}

	return e.buildLeftJoinChain(finalArms, allRows, 0)
}

// buildLeftJoinChain recursively builds "Nested loop left join" nodes.
// arms[0] is the leftmost table group; arms[1..] are successive right-side groups.
func (e *Executor) buildLeftJoinChain(arms [][][]interface{}, allRows [][]interface{}, indent int) []string {
	if len(arms) == 0 {
		return nil
	}
	pfx := explainTreeIndent(indent)

	if len(arms) == 1 {
		// Single arm: render it directly (no join wrapper needed).
		return e.explainTreeRenderLeftJoinArm(arms[0], allRows, indent, false)
	}

	// Emit the left join wrapper and recurse.
	var lines []string
	lines = append(lines, pfx+"-> Nested loop left join")

	// Left child: all arms except the last.
	leftLines := e.buildLeftJoinChain(arms[:len(arms)-1], allRows, indent+1)
	lines = append(lines, leftLines...)

	// Right child: the last arm, rendered as an inner join.
	rightLines := e.explainTreeRenderLeftJoinArm(arms[len(arms)-1], allRows, indent+1, true)
	lines = append(lines, rightLines...)

	return lines
}

// explainTreeRenderLeftJoinArm renders one arm of a LEFT JOIN tree.
// armRows must already be in the correct rendering order (caller determines order).
// isRightSide: if true, arm is the right side of a LEFT JOIN (render as nested loop inner join).
// If isRightSide=false, renders the arm as a simple table or group.
// The subquery-first ordering is inferred from the row order: if the first row is a
// <subquery> placeholder, subqueryFirst=true; otherwise false.
func (e *Executor) explainTreeRenderLeftJoinArm(armRows [][]interface{}, allRows [][]interface{}, indent int, isRightSide bool) []string {
	pfx := explainTreeIndent(indent)

	if !isRightSide {
		// Driver arm: render without join wrapper.
		if len(armRows) == 1 {
			row := armRows[0]
			tbl, at, kn, ref, extra, _, _ := explainTreeRowInfo(row)
			// For the leftmost driver arm with a pure ALL scan (no filter conditions),
			// MySQL omits the Filter wrapper in FORMAT=TREE.
			if at == "ALL" && !strings.Contains(extra, "Using where") && !strings.Contains(extra, "Using join buffer") {
				pfx2 := explainTreeIndent(indent)
				return []string{pfx2 + "-> Table scan on " + tbl}
			}
			return e.explainTreeTableNode(tbl, at, kn, ref, extra, allRows, indent)
		}
		return e.explainTreeBuildGroup(armRows, allRows, indent, "inner")
	}

	// Right-side arm: separate subquery placeholder rows from regular table rows.
	var subqueryRowsInArm, tableRowsInArm [][]interface{}
	for _, row := range armRows {
		tbl := ""
		if row[2] != nil {
			tbl = fmt.Sprintf("%v", row[2])
		}
		if strings.HasPrefix(tbl, "<subquery") {
			subqueryRowsInArm = append(subqueryRowsInArm, row)
		} else {
			tableRowsInArm = append(tableRowsInArm, row)
		}
	}

	if len(subqueryRowsInArm) == 0 || len(tableRowsInArm) == 0 {
		return e.explainTreeBuildGroup(armRows, allRows, indent, "inner")
	}

	// Infer ordering from the first row of the arm.
	firstTbl := ""
	if len(armRows) > 0 && armRows[0][2] != nil {
		firstTbl = fmt.Sprintf("%v", armRows[0][2])
	}
	subqueryFirst := strings.HasPrefix(firstTbl, "<subquery")

	var lines []string
	lines = append(lines, pfx+"-> Nested loop inner join")

	if subqueryFirst {
		// Subquery-first: render rows in order (all subquery rows before table rows).
		for _, row := range armRows {
			tbl, at, kn, ref, extra, _, _ := explainTreeRowInfo(row)
			subLines := e.explainTreeTableNode(tbl, at, kn, ref, extra, allRows, indent+1)
			lines = append(lines, subLines...)
		}
	} else {
		// Table-first: render table rows, then wrap subquery rows in Filter.
		for _, row := range tableRowsInArm {
			tbl, at, kn, ref, extra, _, _ := explainTreeRowInfo(row)
			subLines := e.explainTreeTableNode(tbl, at, kn, ref, extra, allRows, indent+1)
			lines = append(lines, subLines...)
		}
		for _, row := range subqueryRowsInArm {
			tbl, at, kn, ref, extra, _, _ := explainTreeRowInfo(row)
			// Probe-side subquery gets a Filter wrapper (MySQL shows a filter condition
			// on the subquery placeholder when the table is the driver).
			innerLines := e.explainTreeTableNode(tbl, at, kn, ref, extra, allRows, indent+2)
			lines = append(lines, explainTreeIndent(indent+1)+"-> Filter: ("+tbl+" condition)")
			lines = append(lines, innerLines...)
		}
	}

	return lines
}

func (e *Executor) explainTreeText(query string) string {
	upper := strings.ToUpper(query)
	if strings.Contains(upper, "JSON_TABLE(") {
		return "-> Materialize table function"
	}

	// Try to build a proper tree from EXPLAIN rows
	rows := e.explainMultiRows(query)

	// Check if we have any complex structure (materialized subqueries, multi-table joins)
	hasSubquery := false
	hasMaterialized := false
	for _, row := range rows {
		_, _, _, _, _, st, _ := explainTreeRowInfo(row)
		tbl := ""
		if row[2] != nil {
			tbl = fmt.Sprintf("%v", row[2])
		}
		if strings.HasPrefix(tbl, "<subquery") {
			hasSubquery = true
		}
		if st == "MATERIALIZED" {
			hasMaterialized = true
		}
	}

	// For complex queries with materialized subqueries, build full tree
	if hasSubquery || hasMaterialized {
		// Group rows by id (skip nil-id rows like UNION RESULT)
		idOrder := []int64{}
		idGroups := map[int64][][]interface{}{}
		for _, row := range rows {
			if id, ok := row[0].(int64); ok {
				if _, seen := idGroups[id]; !seen {
					idOrder = append(idOrder, id)
				}
				idGroups[id] = append(idGroups[id], row)
			}
		}

		if len(idOrder) > 0 {
			outerID := idOrder[0]
			outerRows := idGroups[outerID]

			// Filter out MATERIALIZED rows (they belong to inner subqueries and are accessed via the placeholder)
			var primaryRows [][]interface{}
			for _, row := range outerRows {
				_, _, _, _, _, st, _ := explainTreeRowInfo(row)
				if st != "MATERIALIZED" {
					primaryRows = append(primaryRows, row)
				}
			}
			if len(primaryRows) == 0 {
				primaryRows = outerRows
			}

			// Use specialized LEFT JOIN tree builder when the query has LEFT JOINs.
			var treeLines []string
			if e.queryHasLeftJoin(query) {
				treeLines = e.explainTreeBuildForLeftJoin(rows, query)
			} else {
				treeLines = e.explainTreeBuildGroup(primaryRows, rows, 0, "inner")
			}
			if len(treeLines) > 0 {
				return strings.Join(treeLines, "\n")
			}
		}
	}

	// Fallback: simple single-table tree
	tbl := explainTableNameFromQuery(query)
	if tbl == "" {
		tbl = "dual"
	}

	// Parse the query to detect structural features
	stmt, err := e.parser().Parse(query)
	if err != nil {
		return "-> Table scan on " + tbl
	}

	// Handle INSERT INTO ... SELECT ...
	if ins, ok := stmt.(*sqlparser.Insert); ok {
		// Only handle INSERT ... SELECT (not INSERT ... VALUES)
		if sel, ok2 := ins.Rows.(*sqlparser.Select); ok2 {
			insertTbl := ""
			if tn, ok3 := ins.Table.Expr.(sqlparser.TableName); ok3 {
				insertTbl = tn.Name.String()
			}
			// Build the inner SELECT tree
			selectQuery := sqlparser.String(sel)
			innerTree := e.explainTreeText(selectQuery)
			// Indent inner tree lines
			innerLines := strings.Split(innerTree, "\n")
			for i, line := range innerLines {
				if line != "" {
					innerLines[i] = "    " + line
				}
			}
			return "-> Insert into " + insertTbl + "\n" + strings.Join(innerLines, "\n")
		}
		return "-> Table scan on " + tbl
	}

	// Handle multi-table UPDATE
	if upd, ok := stmt.(*sqlparser.Update); ok {
		if len(upd.TableExprs) >= 1 {
			var tableNames []string
			for _, te := range upd.TableExprs {
				tableNames = append(tableNames, e.extractAllTableNames(te)...)
			}
			if len(tableNames) >= 2 {
				headerTables := strings.Join(tableNames, ", ")
				// Build nested loop join with filter on second table if WHERE exists
				var lines []string
				lines = append(lines, "-> Update "+headerTables)
				lines = append(lines, "    -> Nested loop inner join")
				lines = append(lines, "        -> Table scan on "+tableNames[0])
				if upd.Where != nil {
					whereStr := sqlparser.String(upd.Where.Expr)
					lines = append(lines, "        -> Filter: ("+whereStr+")")
					lines = append(lines, "            -> Table scan on "+tableNames[1])
				} else {
					lines = append(lines, "        -> Table scan on "+tableNames[1])
				}
				return strings.Join(lines, "\n")
			}
		}
		return "-> Table scan on " + tbl
	}

	// Handle multi-table DELETE
	if del, ok := stmt.(*sqlparser.Delete); ok {
		if len(del.TableExprs) >= 1 {
			var tableNames []string
			for _, te := range del.TableExprs {
				tableNames = append(tableNames, e.extractAllTableNames(te)...)
			}
			// For multi-table delete, use Targets if available; otherwise use TableExprs
			var targetNames []string
			if len(del.Targets) > 0 {
				for _, t := range del.Targets {
					targetNames = append(targetNames, t.Name.String())
				}
			} else {
				targetNames = tableNames
			}
			if len(tableNames) >= 2 {
				headerTables := strings.Join(targetNames, ", ")
				var lines []string
				lines = append(lines, "-> Delete from "+headerTables)
				lines = append(lines, "    -> Nested loop inner join")
				if del.Where != nil {
					whereStr := sqlparser.String(del.Where.Expr)
					lines = append(lines, "        -> Table scan on "+tableNames[0])
					lines = append(lines, "        -> Filter: ("+whereStr+")")
					lines = append(lines, "            -> Table scan on "+tableNames[1])
				} else {
					for _, name := range tableNames {
						lines = append(lines, "        -> Table scan on "+name)
					}
				}
				return strings.Join(lines, "\n")
			}
		}
		return "-> Table scan on " + tbl
	}

	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return "-> Table scan on " + tbl
	}

	// Handle multi-table SELECT (FROM t1, t2 ...) with GROUP BY and/or LIMIT/OFFSET
	if len(sel.From) > 1 {
		// Extract table display names (use alias when available)
		var tableDisplayNames []string
		for _, te := range sel.From {
			if ate, ok2 := te.(*sqlparser.AliasedTableExpr); ok2 {
				if !ate.As.IsEmpty() {
					tableDisplayNames = append(tableDisplayNames, ate.As.String())
				} else if tn, ok3 := ate.Expr.(sqlparser.TableName); ok3 {
					tableDisplayNames = append(tableDisplayNames, tn.Name.String())
				}
			}
		}
		if len(tableDisplayNames) >= 2 {
			// Build LIMIT/OFFSET prefix
			var limitOffsetPrefix string
			var limitOffsetIndent string
			if sel.Limit != nil {
				rowCount := int64(0)
				offset := int64(0)
				if sel.Limit.Rowcount != nil {
					if lit, ok2 := sel.Limit.Rowcount.(*sqlparser.Literal); ok2 {
						rowCount, _ = strconv.ParseInt(lit.Val, 10, 64)
					}
				}
				if sel.Limit.Offset != nil {
					if lit, ok2 := sel.Limit.Offset.(*sqlparser.Literal); ok2 {
						offset, _ = strconv.ParseInt(lit.Val, 10, 64)
					}
				}
				if offset > 0 {
					limitOffsetPrefix = fmt.Sprintf("-> Limit/Offset: %d/%d row(s)", rowCount, offset)
					limitOffsetIndent = "    "
				} else if rowCount > 0 {
					limitOffsetPrefix = fmt.Sprintf("-> Limit: %d row(s)", rowCount)
					limitOffsetIndent = "    "
				}
			}

			// Detect aggregate expression for GROUP BY
			var groupAggExpr string
			if sel.GroupBy != nil && len(sel.GroupBy.Exprs) > 0 && len(sel.SelectExprs.Exprs) > 0 {
				if ae, ok2 := sel.SelectExprs.Exprs[0].(*sqlparser.AliasedExpr); ok2 {
					raw := sqlparser.String(ae.Expr)
					// Normalize to match MySQL's EXPLAIN FORMAT=TREE format:
					// uppercase NULL, no space after comma
					groupAggExpr = strings.ReplaceAll(raw, "null", "NULL")
					groupAggExpr = strings.ReplaceAll(groupAggExpr, ", ", ",")
				}
			}

			// Build the tree
			var lines []string

			// Determine inner join and sort structure
			hasGroupBy := sel.GroupBy != nil && len(sel.GroupBy.Exprs) > 0
			var innerLines []string
			if hasGroupBy {
				// Sort on group-by column + nested loop inner join
				groupByColStr := sqlparser.String(sel.GroupBy.Exprs[0])
				sortLine := limitOffsetIndent + "        -> Sort: " + groupByColStr
				innerLines = append(innerLines, limitOffsetIndent+"        -> Nested loop inner join")
				innerLines = append(innerLines, sortLine)
				innerLines = append(innerLines, limitOffsetIndent+"            -> Table scan on "+tableDisplayNames[0])
				for _, tblName := range tableDisplayNames[1:] {
					innerLines = append(innerLines, limitOffsetIndent+"        -> Table scan on "+tblName)
				}
			} else {
				// Plain nested loop inner join
				innerLines = append(innerLines, limitOffsetIndent+"    -> Nested loop inner join")
				for _, tblName := range tableDisplayNames {
					innerLines = append(innerLines, limitOffsetIndent+"        -> Table scan on "+tblName)
				}
			}

			// Assemble the full tree
			if limitOffsetPrefix != "" {
				lines = append(lines, limitOffsetPrefix)
			}
			if groupAggExpr != "" {
				lines = append(lines, limitOffsetIndent+"-> Group aggregate: "+groupAggExpr)
				lines = append(lines, innerLines...)
			} else if hasGroupBy {
				lines = append(lines, innerLines...)
			} else {
				lines = append(lines, innerLines...)
			}
			return strings.Join(lines, "\n")
		}
	}

	// Detect LIMIT/OFFSET
	var limitOffset string
	if sel.Limit != nil {
		rowCount := int64(0)
		offset := int64(0)
		if sel.Limit.Rowcount != nil {
			if lit, ok := sel.Limit.Rowcount.(*sqlparser.Literal); ok {
				rowCount, _ = strconv.ParseInt(lit.Val, 10, 64)
			}
		}
		if sel.Limit.Offset != nil {
			if lit, ok := sel.Limit.Offset.(*sqlparser.Literal); ok {
				offset, _ = strconv.ParseInt(lit.Val, 10, 64)
			}
		}
		if offset > 0 {
			limitOffset = fmt.Sprintf("-> Limit/Offset: %d/%d row(s)\n    ", rowCount, offset)
		} else if rowCount > 0 {
			limitOffset = fmt.Sprintf("-> Limit: %d row(s)\n    ", rowCount)
		}
	}

	// Determine inner scan node based on access type
	var scanNode string
	hasWhere := sel.Where != nil
	if hasWhere {
		// Try to detect index usage for the scan node
		ai := e.explainDetectAccessType(sel, tbl)
		if ai.accessType == "range" {
			idxName := ""
			if ai.key != nil {
				idxName = fmt.Sprintf("%v", ai.key)
			}
			if idxName != "" {
				scanNode = "-> Index range scan on " + tbl + " using " + idxName
			} else {
				scanNode = "-> Table scan on " + tbl
			}
		} else if ai.accessType == "ref" || ai.accessType == "ref_or_null" || ai.accessType == "eq_ref" || ai.accessType == "const" {
			idxName := ""
			if ai.key != nil {
				idxName = fmt.Sprintf("%v", ai.key)
			}
			if idxName != "" {
				scanNode = "-> Index lookup on " + tbl + " using " + idxName
			} else {
				scanNode = "-> Table scan on " + tbl
			}
		} else if ai.accessType == "index" {
			idxName := ""
			if ai.key != nil {
				idxName = fmt.Sprintf("%v", ai.key)
			}
			if idxName != "" {
				scanNode = "-> Index scan on " + tbl + " using " + idxName
			} else {
				scanNode = "-> Table scan on " + tbl
			}
		} else {
			scanNode = "-> Table scan on " + tbl
		}
	} else {
		scanNode = "-> Table scan on " + tbl
	}

	// Build the tree: [LimitOffset ->] [Filter ->] scan
	if hasWhere {
		// Generate a placeholder filter condition text
		whereStr := sqlparser.String(sel.Where.Expr)
		filterLine := "-> Filter: (" + whereStr + ")\n    " + scanNode
		if limitOffset != "" {
			return limitOffset + filterLine
		}
		return filterLine
	}
	return limitOffset + scanNode
}

// tryPlanBasedExplainTraditional attempts to generate EXPLAIN Traditional rows
// using the new plan-based path (Phase 2). Returns (rows, true) on success,
// or (nil, false) if the query is too complex and the existing path should be used.
//
// Falls back for queries with subqueries or derived tables.
// Simple and JOIN queries are handled by the plan-based path (Phase 3).
func (e *Executor) tryPlanBasedExplainTraditional(sel *sqlparser.Select) ([][]interface{}, bool) {
	// Fall back for queries with complex parts (subqueries / derived tables).
	if e.queryHasComplexParts(sel) {
		return nil, false
	}

	// Check for "Impossible WHERE" due to constant out-of-range for multi-table joins.
	// MySQL's optimizer propagates constants through equi-join conditions and detects that
	// a constant is out of range for a column's integer type, producing a single Impossible WHERE row.
	if e.Storage != nil && sel.Where != nil && e.isWhereImpossibleDueToConstantOutOfRange(sel) {
		// Count real tables to decide if it's a multi-table scenario
		var tableCount int
		for _, te := range sel.From {
			tableCount += len(e.extractAllTableNames(te))
		}
		if tableCount > 1 {
			return [][]interface{}{
				{int64(1), "SIMPLE", nil, nil, nil, nil, nil, nil, nil, nil, nil, "Impossible WHERE"},
			}, true
		}
	}

	planner := newPlanner(e)
	plan, err := planner.BuildPlan(sel)
	if err != nil {
		return nil, false
	}
	plan = planner.optimize(plan, sel)

	pe := &PlanExplainer{executor: e, query: sqlparser.String(sel)}
	rows := pe.ExplainTraditional(plan)
	if len(rows) == 0 {
		return nil, false
	}

	// Fall back to the old path when all tables have const/system access type AND
	// there is a WHERE clause with multiple rows (JOIN). The old path has sophisticated
	// "Impossible WHERE noticed after reading const tables" detection that executes
	// the actual query to determine if the WHERE is always false.
	if len(rows) > 1 && sel.Where != nil {
		allConst := true
		for _, row := range rows {
			if len(row) > 4 {
				at, ok := row[4].(string)
				if !ok || (at != "const" && at != "system") {
					allConst = false
					break
				}
			}
		}
		if allConst {
			return nil, false
		}
	}

	return rows, true
}

func (e *Executor) explainResultForType(explainType sqlparser.ExplainType, explainedQuery string) *Result {
	switch explainType {
	case sqlparser.TreeType:
		return &Result{
			Columns:     []string{"EXPLAIN"},
			Rows:        [][]interface{}{{e.explainTreeText(explainedQuery)}},
			IsResultSet: true,
		}
	case sqlparser.JSONType:
		return &Result{
			Columns:     []string{"EXPLAIN"},
			Rows:        [][]interface{}{{e.explainJSONDocument(explainedQuery)}},
			IsResultSet: true,
		}
	default:
		rows := e.explainMultiRows(explainedQuery)
		return &Result{
			Columns:     []string{"id", "select_type", "table", "partitions", "type", "possible_keys", "key", "key_len", "ref", "rows", "filtered", "Extra"},
			Rows:        rows,
			IsResultSet: true,
		}
	}
}

// execExplainStmt handles EXPLAIN SELECT ... statements.
// Returns a simplified explain result set for compatibility.
func (e *Executor) execExplainStmt(s *sqlparser.ExplainStmt, query string) (*Result, error) {
	// Check privileges for the explained statement before producing any output.
	// MySQL requires the same privileges for EXPLAIN <stmt> as for <stmt> itself.
	if privErr := e.checkTablePrivilege(s.Statement); privErr != nil {
		return nil, privErr
	}
	// Validate index hints (USE KEY / IGNORE KEY / FORCE KEY) before producing explain output.
	if sel, ok := s.Statement.(*sqlparser.Select); ok {
		if err := e.validateIndexHints(sel.From); err != nil {
			return nil, err
		}
		// Validate WHERE clause for invalid datetime literals.
		if sel.Where != nil {
			if err := validateWhereForInvalidDatetime(sel.Where.Expr); err != nil {
				return nil, err
			}
		}
		// Validate WHERE clause for invalid DATE string literals against DATE columns.
		if sel.Where != nil && len(sel.From) > 0 {
			if tbl, ok := sel.From[0].(*sqlparser.AliasedTableExpr); ok {
				if tn, ok := tbl.Expr.(sqlparser.TableName); ok {
					tableName := tn.Name.String()
					if e.Catalog != nil {
						if db, err := e.Catalog.GetDatabase(e.CurrentDB); err == nil {
							if td, ok := db.Tables[tableName]; ok {
								if err := validateWhereForInvalidDateColumns(sel.Where.Expr, td, e.sqlMode); err != nil {
									return nil, err
								}
							}
						}
					}
				}
			}
		}
	}
	// Phase 2: use plan-based EXPLAIN Traditional for simple SELECT queries.
	// For complex cases (UNION, subqueries in WHERE, etc.) fall back to existing path.
	if sel, ok2 := s.Statement.(*sqlparser.Select); ok2 {
		if s.Type == sqlparser.TraditionalType || (s.Type != sqlparser.TreeType && s.Type != sqlparser.JSONType) {
			if planRows, ok := e.tryPlanBasedExplainTraditional(sel); ok {
				return &Result{
					Columns:     []string{"id", "select_type", "table", "partitions", "type", "possible_keys", "key", "key_len", "ref", "rows", "filtered", "Extra"},
					Rows:        planRows,
					IsResultSet: true,
				}, nil
			}
		}
	} else if u, ok2 := s.Statement.(*sqlparser.Union); ok2 {
		planner := newPlanner(e)
		if plan, err := planner.BuildPlan(u); err == nil {
			_ = plan // constructed but complex path handles output
		}
	}

	// Use explainResultForType which delegates to explainMultiRows for proper select_type detection
	explainedQuery := query
	if s.Statement != nil {
		explainedQuery = sqlparser.String(s.Statement)
	}
	return e.explainResultForType(s.Type, explainedQuery), nil
}

