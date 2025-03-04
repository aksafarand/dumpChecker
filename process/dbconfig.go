package process

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-adodb"

	// _ "github.com/mattn/go-sqlite3"
	_ "modernc.org/sqlite"
)

func ImportExcelToSQLite(xlsxPath, tableName, sheetName, sqliteDBName string) error {

	excelConnStr := fmt.Sprintf(`Provider=Microsoft.ACE.OLEDB.12.0;Data Source=%s;Extended Properties="Excel 12.0 Xml;HDR=YES;IMEX=1";`, xlsxPath)

	// Open the Excel file via sqlx using the adodb driver.
	excelDB, err := sqlx.Open("adodb", excelConnStr)
	if err != nil {
		return fmt.Errorf("failed to open Excel file: %w", err)
	}
	defer excelDB.Close()

	// Build the query.
	query := fmt.Sprintf("SELECT * FROM [%s$]", sheetName)

	// Query the Excel file using Queryx to get column order.
	rows, err := excelDB.Queryx(query)
	if err != nil {
		return fmt.Errorf("failed to query Excel sheet: %w", err)
	}
	defer rows.Close()

	// Get the column names in the order returned by the query.
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}
	if len(columns) == 0 {
		return fmt.Errorf("no columns found in Excel sheet")
	}

	// Read all rows into a slice of maps.
	var data []map[string]interface{}
	for rows.Next() {
		m := make(map[string]interface{})
		if err := rows.MapScan(m); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		data = append(data, m)
	}
	if len(data) == 0 {
		return fmt.Errorf("no data found in Excel sheet")
	}

	// Open (or create) the SQLite database.
	sqliteDB, err := sql.Open("sqlite", sqliteDBName)
	if err != nil {
		return fmt.Errorf("failed to open SQLite DB: %w", err)
	}
	defer sqliteDB.Close()

	// Build a CREATE TABLE statement based on the Excel columns.
	var colDefs []string
	for _, col := range columns {
		// We'll use TEXT for all columns; adjust as needed.
		colDefs = append(colDefs, fmt.Sprintf("`%s` TEXT", col))
	}

	dropStmt := fmt.Sprintf("DROP TABLE IF EXISTS `%s_%s`;", tableName, sheetName)
	if _, err := sqliteDB.Exec(dropStmt); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	createStmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s_%s` (%s);", tableName, sheetName, strings.Join(colDefs, ", "))
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
	insertStmt := fmt.Sprintf("INSERT INTO `%s_%s` (%s) VALUES (%s);", tableName, sheetName, strings.Join(colList, ", "), strings.Join(placeholders, ", "))
	stmt, err := sqliteDB.Prepare(insertStmt)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	// Insert each row from Excel into SQLite.
	for _, row := range data {
		var values []interface{}
		// Use the column order from the Excel query.
		for _, col := range columns {
			values = append(values, row[col])
		}
		if _, err := stmt.Exec(values...); err != nil {
			log.Printf("failed to insert row: %v", err)
		}
	}

	log.Printf("Data imported successfully into table %s_%s in %s", tableName, sheetName, sqliteDBName)
	return nil
}
