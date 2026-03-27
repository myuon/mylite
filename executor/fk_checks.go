package executor

import (
	"fmt"
	"strings"

	"github.com/myuon/mylite/catalog"
	"github.com/myuon/mylite/storage"
)

// foreignKeyChecksEnabled returns true if FOREIGN_KEY_CHECKS is ON (default).
func (e *Executor) foreignKeyChecksEnabled() bool {
	if v, ok := e.getSysVar("foreign_key_checks"); ok {
		return !strings.EqualFold(v, "OFF") && v != "0"
	}
	return true // default is ON
}

// checkForeignKeyOnInsert verifies that all FK constraints on the child table
// are satisfied by the row being inserted. For each FK, the referenced parent
// row must exist. If any FK column value is NULL, the constraint is satisfied
// (MySQL behavior: NULL values are not checked).
func (e *Executor) checkForeignKeyOnInsert(dbName, tableName string, row storage.Row) error {
	if !e.foreignKeyChecksEnabled() {
		return nil
	}
	db, err := e.Catalog.GetDatabase(dbName)
	if err != nil {
		return nil
	}
	def, err := db.GetTable(tableName)
	if err != nil || def == nil || len(def.ForeignKeys) == 0 {
		return nil
	}

	for _, fk := range def.ForeignKeys {
		if err := e.checkParentRowExists(dbName, fk, row); err != nil {
			return err
		}
	}
	return nil
}

// checkForeignKeyOnDelete checks that deleting a row from the parent table
// does not violate any FK constraint. If child rows reference this parent row,
// the action depends on the ON DELETE clause:
//   - RESTRICT / NO ACTION / "" (default): return error
//   - CASCADE: delete child rows
//   - SET NULL: set FK columns in child rows to NULL
func (e *Executor) checkForeignKeyOnDelete(dbName, tableName string, deletedRow storage.Row) error {
	if !e.foreignKeyChecksEnabled() {
		return nil
	}
	return e.handleParentRowRemoval(dbName, tableName, deletedRow, true)
}

// checkForeignKeyOnUpdate checks FK constraints when updating a row.
// For the child table: verify new FK values exist in the parent.
// For parent tables: check if any child references the old values of updated columns.
func (e *Executor) checkForeignKeyOnUpdate(dbName, tableName string, oldRow, newRow storage.Row) error {
	if !e.foreignKeyChecksEnabled() {
		return nil
	}

	// 1. Child-side check: if this table has FKs, verify new values exist in parent
	db, err := e.Catalog.GetDatabase(dbName)
	if err != nil {
		return nil
	}
	def, err := db.GetTable(tableName)
	if err != nil || def == nil {
		return nil
	}

	for _, fk := range def.ForeignKeys {
		// Only check if FK columns actually changed
		changed := false
		for _, col := range fk.Columns {
			oldVal := fmt.Sprintf("%v", oldRow[col])
			newVal := fmt.Sprintf("%v", newRow[col])
			if oldVal != newVal {
				changed = true
				break
			}
		}
		if changed {
			if err := e.checkParentRowExists(dbName, fk, newRow); err != nil {
				return err
			}
		}
	}

	// 2. Parent-side check: if other tables reference this table, check old values
	return e.handleParentRowUpdate(dbName, tableName, oldRow, newRow)
}

// checkParentRowExists verifies that the parent row referenced by the FK exists.
func (e *Executor) checkParentRowExists(dbName string, fk catalog.ForeignKeyDef, row storage.Row) error {
	// If any FK column is NULL, the constraint is satisfied (MySQL behavior)
	for _, col := range fk.Columns {
		if row[col] == nil {
			return nil
		}
	}

	parentTable, err := e.Storage.GetTable(dbName, fk.ReferencedTable)
	if err != nil {
		// Parent table doesn't exist - skip check (might be cross-db)
		return nil
	}

	for _, parentRow := range parentTable.Rows {
		if fkRowMatches(fk.Columns, fk.ReferencedColumns, row, parentRow) {
			return nil
		}
	}

	// Build the error message with the FK column values
	vals := make([]string, len(fk.Columns))
	for i, col := range fk.Columns {
		vals[i] = fmt.Sprintf("%v", row[col])
	}
	childRef := fmt.Sprintf("`%s`.`%s`", dbName, fk.ReferencedTable)
	return mysqlError(1452, "23000",
		fmt.Sprintf("Cannot add or update a child row: a foreign key constraint fails (`%s`.`%s`, CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES %s (%s))",
			dbName, fk.ReferencedTable, // This should be the child table but MySQL uses parent in message
			fk.Name,
			formatColumnList(fk.Columns),
			childRef,
			formatColumnList(fk.ReferencedColumns)))
}

// handleParentRowRemoval processes FK actions when a parent row is deleted.
func (e *Executor) handleParentRowRemoval(dbName, tableName string, parentRow storage.Row, isDelete bool) error {
	// Find all tables in this database that have FKs referencing this table
	db, err := e.Catalog.GetDatabase(dbName)
	if err != nil {
		return nil
	}

	for childTableName, childDef := range db.Tables {
		for _, fk := range childDef.ForeignKeys {
			if !strings.EqualFold(fk.ReferencedTable, tableName) {
				continue
			}
			// Check if any child rows reference the deleted parent row
			childTbl, err := e.Storage.GetTable(dbName, childTableName)
			if err != nil {
				continue
			}

			action := fk.OnDelete
			if !isDelete {
				action = fk.OnUpdate
			}

			hasChildren := false
			for _, childRow := range childTbl.Rows {
				if fkRowMatchesParent(fk.Columns, fk.ReferencedColumns, childRow, parentRow) {
					hasChildren = true
					break
				}
			}

			if !hasChildren {
				continue
			}

			switch strings.ToUpper(action) {
			case "CASCADE":
				if isDelete {
					childTbl.Lock()
					newRows := make([]storage.Row, 0, len(childTbl.Rows))
					for _, childRow := range childTbl.Rows {
						if !fkRowMatchesParent(fk.Columns, fk.ReferencedColumns, childRow, parentRow) {
							newRows = append(newRows, childRow)
						}
					}
					childTbl.Rows = newRows
					childTbl.InvalidateIndexes()
					childTbl.Unlock()
				}
			case "SET NULL":
				childTbl.Lock()
				for _, childRow := range childTbl.Rows {
					if fkRowMatchesParent(fk.Columns, fk.ReferencedColumns, childRow, parentRow) {
						for _, col := range fk.Columns {
							childRow[col] = nil
						}
					}
				}
				childTbl.InvalidateIndexes()
				childTbl.Unlock()
			default: // RESTRICT, NO ACTION, or empty (default = RESTRICT)
				vals := make([]string, len(fk.ReferencedColumns))
				for i, col := range fk.ReferencedColumns {
					vals[i] = fmt.Sprintf("%v", parentRow[col])
				}
				return mysqlError(1451, "23000",
					fmt.Sprintf("Cannot delete or update a parent row: a foreign key constraint fails (`%s`.`%s`, CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s` (%s))",
						dbName, childTableName,
						fk.Name,
						formatColumnList(fk.Columns),
						tableName,
						formatColumnList(fk.ReferencedColumns)))
			}
		}
	}
	return nil
}

// handleParentRowUpdate processes FK actions when a parent row is updated.
func (e *Executor) handleParentRowUpdate(dbName, tableName string, oldRow, newRow storage.Row) error {
	db, err := e.Catalog.GetDatabase(dbName)
	if err != nil {
		return nil
	}

	for childTableName, childDef := range db.Tables {
		for _, fk := range childDef.ForeignKeys {
			if !strings.EqualFold(fk.ReferencedTable, tableName) {
				continue
			}
			// Check if any referenced columns actually changed
			changed := false
			for _, refCol := range fk.ReferencedColumns {
				oldVal := fmt.Sprintf("%v", oldRow[refCol])
				newVal := fmt.Sprintf("%v", newRow[refCol])
				if oldVal != newVal {
					changed = true
					break
				}
			}
			if !changed {
				continue
			}

			childTbl, err := e.Storage.GetTable(dbName, childTableName)
			if err != nil {
				continue
			}

			hasChildren := false
			for _, childRow := range childTbl.Rows {
				if fkRowMatchesParent(fk.Columns, fk.ReferencedColumns, childRow, oldRow) {
					hasChildren = true
					break
				}
			}
			if !hasChildren {
				continue
			}

			action := fk.OnUpdate

			switch strings.ToUpper(action) {
			case "CASCADE":
				childTbl.Lock()
				for _, childRow := range childTbl.Rows {
					if fkRowMatchesParent(fk.Columns, fk.ReferencedColumns, childRow, oldRow) {
						for i, col := range fk.Columns {
							childRow[col] = newRow[fk.ReferencedColumns[i]]
						}
					}
				}
				childTbl.InvalidateIndexes()
				childTbl.Unlock()
			case "SET NULL":
				childTbl.Lock()
				for _, childRow := range childTbl.Rows {
					if fkRowMatchesParent(fk.Columns, fk.ReferencedColumns, childRow, oldRow) {
						for _, col := range fk.Columns {
							childRow[col] = nil
						}
					}
				}
				childTbl.InvalidateIndexes()
				childTbl.Unlock()
			default: // RESTRICT, NO ACTION, or empty (default = RESTRICT)
				return mysqlError(1451, "23000",
					fmt.Sprintf("Cannot delete or update a parent row: a foreign key constraint fails (`%s`.`%s`, CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s` (%s))",
						dbName, childTableName,
						fk.Name,
						formatColumnList(fk.Columns),
						tableName,
						formatColumnList(fk.ReferencedColumns)))
			}
		}
	}
	return nil
}

// fkRowMatches checks if the child row's FK columns match the parent row's referenced columns.
func fkRowMatches(childCols, parentCols []string, childRow, parentRow storage.Row) bool {
	if len(childCols) != len(parentCols) {
		return false
	}
	for i, childCol := range childCols {
		cv := childRow[childCol]
		pv := parentRow[parentCols[i]]
		if cv == nil || pv == nil {
			return false
		}
		if fmt.Sprintf("%v", cv) != fmt.Sprintf("%v", pv) {
			return false
		}
	}
	return true
}

// fkRowMatchesParent checks if a child row references a specific parent row.
func fkRowMatchesParent(childCols, parentCols []string, childRow, parentRow storage.Row) bool {
	if len(childCols) != len(parentCols) {
		return false
	}
	for i, childCol := range childCols {
		cv := childRow[childCol]
		pv := parentRow[parentCols[i]]
		if cv == nil {
			return false // NULL child FK values don't reference any parent
		}
		if pv == nil {
			return false
		}
		if fmt.Sprintf("%v", cv) != fmt.Sprintf("%v", pv) {
			return false
		}
	}
	return true
}

// formatColumnList formats a list of column names for error messages.
func formatColumnList(cols []string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = "`" + c + "`"
	}
	return strings.Join(parts, ", ")
}
