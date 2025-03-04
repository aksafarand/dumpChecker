package process

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
)

// ImportAccessQueryToSQLite imports the result of an Access query into SQLite.
// It reads all rows from rows (result of an Access query) into memory,
// creates (or replaces) a table in SQLite with name tableName,
// and then inserts all rows. All columns are stored as TEXT.
func ImportAccessQueryToSQLite(rows *sql.Rows, tableName, sourceFile string, sqliteDB *sql.DB) error {
	// Get the column names in the order returned by the query.
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}
	if len(columns) == 0 {
		return fmt.Errorf("no columns found in Access query result")
	}

	// Read all rows into a slice of maps.
	var data []map[string]interface{}
	for rows.Next() {
		// Create a slice of pointers to empty interfaces.
		// We use new(interface{}) to allow scanning any column type.
		rowValues := make([]interface{}, len(columns))
		for i := range rowValues {
			rowValues[i] = new(interface{})
		}
		if err := rows.Scan(rowValues...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert the row into a map where keys are column names.
		rowMap := make(map[string]interface{})
		for i, colName := range columns {
			// Dereference the pointer.
			valPtr := rowValues[i].(*interface{})
			// Save value as string if not nil; otherwise, nil.
			if *valPtr != nil {
				// Use fmt.Sprintf("%v", ...) to convert to string.
				rowMap[colName] = fmt.Sprintf("%v", *valPtr)
			} else {
				rowMap[colName] = nil
			}
		}
		data = append(data, rowMap)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error reading rows: %w", err)
	}

	// Open (or create) the SQLite table.
	// Drop the table if it already exists.
	dropStmt := fmt.Sprintf("DROP TABLE IF EXISTS `%s`;", tableName)
	if _, err := sqliteDB.Exec(dropStmt); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	// Build a CREATE TABLE statement based on the Access columns.
	var colDefs []string
	for _, col := range columns {
		// Use backticks to quote column names (handles spaces and special characters).
		colDefs = append(colDefs, fmt.Sprintf("`%s` TEXT", col))
	}
	createStmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (%s);", tableName, strings.Join(colDefs, ", "))
	if _, err := sqliteDB.Exec(createStmt); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Build an INSERT statement using the column order.
	var colList []string
	var placeholders []string
	for _, col := range columns {
		colList = append(colList, fmt.Sprintf("`%s`", col))
		placeholders = append(placeholders, "?")
	}
	insertStmt := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s);", tableName, strings.Join(colList, ", "), strings.Join(placeholders, ", "))

	stmt, err := sqliteDB.Prepare(insertStmt)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	// Insert each row from Access into SQLite.
	for _, row := range data {
		var values []interface{}
		// Use the column order from the Access query.
		for _, col := range columns {
			values = append(values, row[col])
		}
		if _, err := stmt.Exec(values...); err != nil {
			log.Printf("failed to insert row from %s into table %s: %v", sourceFile, tableName, err)
		}
	}

	log.Printf("Data imported successfully into table %s from source file %s", tableName, sourceFile)
	return nil
}
