package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"parameterCheck/models"
	"parameterCheck/process"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/manifoldco/promptui"
)

func isOLEDBInstalled() bool {
	output, err := exec.Command("powershell", "-Command",
		"(New-Object -ComObject 'ADODB.Connection' -ErrorAction SilentlyContinue) -ne $null").Output()
	if err != nil {
		return false
	}

	return strings.TrimSpace(string(output)) == "True"
}

func installOLEDB() error {
	if runtime.GOARCH == "amd64" {
		return exec.Command("cmd", "/C", "AccessDatabaseEngine_X64.exe", "/quiet", "/passive", "/norestart").Run()
	}
	return exec.Command("cmd", "/C", "AccessDatabaseEngine.exe", "/quiet", "/passive", "/norestart").Run()
}

func main() {
	if !isOLEDBInstalled() {
		fmt.Println("OLEDB is missing, installing now...")
		if err := installOLEDB(); err != nil {
			log.Fatal("Failed to install OLEDB driver:", err)
		}
		fmt.Println("Installation complete. Please restart the application.")
		fmt.Println("kukuhwikartomo.ext@huawei.com - 2025")
		os.Exit(0)
	}
	start()
}

func start() {

	files, err := os.ReadDir(models.ConfigDir)
	if err != nil {
		log.Fatalf("Failed to read config directory: %v", err)
	}

	var configHuawei, configNokia bool
	var huaweiPath, nokiaPath string

	for _, file := range files {
		if !file.IsDir() {
			name := strings.ToLower(file.Name())
			fullPath := filepath.Join(models.ConfigDir, file.Name())
			if strings.Contains(name, "huawei.xlsx") {
				configHuawei = true
				huaweiPath = fullPath
			}
			if strings.Contains(name, "nokia.xlsx") {
				configNokia = true
				nokiaPath = fullPath
			}

		}
	}

	if configHuawei || configNokia {

		// if checkFileExists(filepath.Join(exeDir, "./dbconfig.db")) {
		promptDbExists := promptui.Select{
			Label: "This will re-create config db, continue?",
			Items: []string{"Yes", "No"},
		}

		_, userSel, err := promptDbExists.Run()
		if err != nil {
			log.Fatalf("Prompt failed %v\n", err)
		}

		if userSel == "Yes" {
			_ = os.Remove("./dbconfig.db")

			if configHuawei {
				err = process.ImportExcelToSQLite(huaweiPath, "Huawei", "2G", "./dbconfig.db")
				if err != nil {
					log.Fatal("No '2G' sheet found in Huawei:", err)
				}

				err = process.ImportExcelToSQLite(huaweiPath, "Huawei", "4G", "./dbconfig.db")
				if err != nil {
					log.Fatal("No '4G' sheet found in Huawei:", err)
				}

			}

			if configNokia {
				err = process.ImportExcelToSQLite(nokiaPath, "Nokia", "2G", "./dbconfig.db")
				if err != nil {
					log.Fatal("No '2G' sheet found in Nokia:", err)
				}

				err = process.ImportExcelToSQLite(nokiaPath, "Nokia", "4G", "./dbconfig.db")
				if err != nil {
					log.Fatal("No '4G' sheet found in Nokia:", err)
				}

			}

			process_dump(configHuawei, configNokia)
			return
		}

		if userSel == "No" {
			process_dump(configHuawei, configNokia)
			return
		}
		// }

	}

}

func process_dump(configHuawei, configNokia bool) {
	var queriesHw2g, queriesHw4g, queriesNok2g, queriesNok4g map[string]string

	db, err := sql.Open("sqlite", "./dbconfig.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	log.Println("Preparing Config Query")

	if configHuawei {
		queriesHw2g, err = getConfigQueries(db, "SELECT * FROM Huawei_2G")
		if err != nil {
			log.Fatal(err)
			queriesHw2g = nil
		}
		queriesHw4g, err = getConfigQueries(db, "SELECT * FROM Huawei_4G")
		if err != nil {
			log.Fatal(err)
			queriesHw4g = nil
		}
	}

	if configNokia {
		queriesNok2g, err = getConfigQueries(db, "SELECT * FROM Nokia_2G")
		if err != nil {
			log.Fatal(err)
			queriesNok2g = nil
		}
		queriesNok4g, err = getConfigQueries(db, "SELECT * FROM Nokia_4G")
		if err != nil {
			log.Fatal(err)
			queriesNok4g = nil
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		processVendorFiles(models.Huawei2gDumpDir, queriesHw2g, models.HuaweiVendorResult)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		processVendorFiles(models.Huawei4gDumpDir, queriesHw4g, models.HuaweiVendorResult)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		processVendorFiles(models.Nokia2gDumpDir, queriesNok2g, models.NokiaVendorResult)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		processVendorFiles(models.Nokia4gDumpDir, queriesNok4g, models.NokiaVendorResult)
	}()

	wg.Wait()
	log.Println("All vendor files processed.")
	log.Println("kukuhwikartomo.ext@huawei.com - 2025")
}

func generateQueries(rows *sql.Rows) (map[string]string, error) {

	querySnippets := make(map[string][]string)

	for rows.Next() {
		var rec models.ConfigRecord

		var nullProposed sql.NullString
		if err := rows.Scan(&rec.TableName, &rec.ParamName, &rec.AttributeColumn, &rec.DataType, &rec.Operator, &nullProposed); err != nil {
			return nil, fmt.Errorf("failed to scan config row: %w", err)
		}
		if nullProposed.Valid {
			rec.ProposedValue = nullProposed.String
		} else {
			rec.ProposedValue = ""
		}

		attrs := strings.Split(rec.AttributeColumn, ";")
		for i := range attrs {
			attrs[i] = fmt.Sprintf("[%s]", strings.TrimSpace(attrs[i]))
		}
		attrSelect := strings.Join(attrs, ", ")

		currentExpr := fmt.Sprintf("IIF(%s IS NULL, '', CSTR(%s))", rec.ParamName, rec.ParamName)

		// Build the ProposedValue expression based on the operator.
		var proposedExpr string
		switch strings.ToLower(rec.Operator) {
		case "between":
			parts := strings.Split(rec.ProposedValue, "to")
			if len(parts) != 2 {
				proposedExpr = currentExpr
			} else {
				lower := strings.TrimSpace(parts[0])
				upper := strings.TrimSpace(parts[1])
				proposedExpr = fmt.Sprintf("IIF(Val(CSTR(%s)) BETWEEN %s AND %s, CSTR(%s), \"%s\")", rec.ParamName, lower, upper, rec.ParamName, rec.ProposedValue)
			}
		case "multi":
			if rec.ProposedValue == "" {
				proposedExpr = currentExpr
			} else {
				// Convert "10 & 20 & 40" to "10,20,40" for the INSTR check.
				multiList := strings.ReplaceAll(rec.ProposedValue, " & ", ",")
				proposedExpr = fmt.Sprintf("IIF(INSTR(\",\" & \"%s\" & \",\", \",\" & %s & \",\") > 0, \"%s\", %s)", multiList, currentExpr, rec.ProposedValue, currentExpr)
			}
		case "=":
			if rec.ProposedValue == "" {
				proposedExpr = currentExpr
			} else {
				// For "=" operator, output the constant proposed value.
				proposedExpr = fmt.Sprintf("\"%s\"", rec.ProposedValue)
			}
		default:
			proposedExpr = currentExpr
		}

		var snippet string
		if strings.ToLower(rec.Operator) == "multi" {

			multiList := strings.ReplaceAll(rec.ProposedValue, " & ", ",")
			snippet = fmt.Sprintf(`
SELECT %s, "%s" AS Parameter, %s AS CurrentValue, "%s" AS ProposedValue,
       IIF(INSTR("," & "%s" & ",", "," & %s & ",") > 0, "Match", "NotMatched") AS Flag
FROM [%s]`,
				attrSelect,
				rec.ParamName,
				currentExpr,
				rec.ProposedValue,
				multiList,
				currentExpr,
				rec.TableName,
			)
		} else {

			snippet = fmt.Sprintf(`
SELECT sub.*, IIF(sub.CurrentValue = sub.ProposedValue, "Match", "NotMatched") AS Flag
FROM (
    SELECT %s, "%s" AS Parameter, %s AS CurrentValue, %s AS ProposedValue
    FROM [%s]
) AS sub`,
				attrSelect,
				rec.ParamName,
				currentExpr,
				proposedExpr,
				rec.TableName,
			)
		}

		querySnippets[rec.TableName] = append(querySnippets[rec.TableName], snippet)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating config rows: %w", err)
	}

	result := make(map[string]string)
	for table, snippets := range querySnippets {
		result[table] = strings.Join(snippets, " UNION ")
	}

	return result, nil
}

func getConfigQueries(db *sql.DB, query string) (map[string]string, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return generateQueries(rows)
}

func processVendorFiles(folder string, queries map[string]string, outputFolder string) {

	files, err := filepath.Glob(filepath.Join(folder, "*.mdb"))
	if err != nil {
		log.Fatalf("Error finding MDB files in %s: %v", folder, err)
	}
	filesAccdb, err := filepath.Glob(filepath.Join(folder, "*.accdb"))
	if err != nil {
		log.Fatalf("Error finding ACCDB files in %s: %v", folder, err)
	}
	files = append(files, filesAccdb...)
	var wg sync.WaitGroup
	for _, file := range files {
		wg.Add(1)
		go func(f string) {
			defer wg.Done()
			processSingleAccessFile(f, queries, outputFolder)
		}(file)
	}
	wg.Wait()
}

func processSingleAccessFile(filePath string, queries map[string]string, outputFolder string) {

	log.Printf("Processing file: %s", filePath)
	sourceConnStr := "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + filePath
	sourceDB, err := sql.Open("adodb", sourceConnStr)
	if err != nil {
		log.Printf("Failed to open Access DB %s: %v", filePath, err)
		return
	}
	defer sourceDB.Close()

	resultData := make(map[string][]map[string]interface{})
	for table, query := range queries {
		rows, err := sourceDB.Query(query)
		if err != nil {
			log.Printf("Query failed on file %s, table %s: %v", filePath, table, err)
			continue
		}
		data, err := readRowsToMap(rows)
		rows.Close()
		if err != nil {
			log.Printf("Failed to read rows from file %s, table %s: %v", filePath, table, err)
			continue
		}
		resultData[table] = data
	}

	newFile := filepath.Join(outputFolder, filepath.Base(filePath)+"_result.accdb")
	// Add to actually create new access file with path

	templateFile := "./EMPTY.accdb"
	if err := copyFile(templateFile, newFile); err != nil {
		log.Printf("Failed to copy template to new file %s: %v", newFile, err)
		return
	}

	newAccessConnStr := "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + newFile
	newAccessDB, err := sql.Open("adodb", newAccessConnStr)
	if err != nil {
		log.Printf("Failed to open new Access DB %s: %v", newFile, err)
		return
	}
	defer newAccessDB.Close()

	if err := populateNewAccessFileFromData(newAccessDB, resultData); err != nil {
		log.Printf("Failed to populate new Access DB %s: %v", newFile, err)
	}
}

func readRowsToMap(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		rowMap := make(map[string]interface{})
		for i, col := range columns {
			if values[i] != nil {
				rowMap[col] = fmt.Sprintf("%v", values[i])
			} else {
				rowMap[col] = ""
			}
		}
		results = append(results, rowMap)
	}
	return results, rows.Err()
}

func copyFile(src, dst string) error {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("failed to stat source file: %w", err)
	}

	if !sourceFileStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer destination.Close()

	if _, err := io.Copy(destination, source); err != nil {
		return fmt.Errorf("failed to copy file: %w", err)
	}

	return nil
}

func populateNewAccessFileFromData(newAccessDB *sql.DB, resultData map[string][]map[string]interface{}) error {
	for table, rowsData := range resultData {
		// Drop the table if it exists.
		dropStmt := fmt.Sprintf("DROP TABLE [%s];", table)
		_, _ = newAccessDB.Exec(dropStmt) // Ignore errors if table doesn't exist.

		// If there is no data for this table, skip creation.
		if len(rowsData) == 0 {
			log.Printf("No data for table %s; skipping creation.", table)
			continue
		}

		// Use the first row to determine the column names.
		var columns []string
		for col := range rowsData[0] {
			columns = append(columns, col)
		}

		// Build CREATE TABLE statement: All columns are defined as TEXT.
		var colDefs []string
		for _, col := range columns {
			colDefs = append(colDefs, fmt.Sprintf("[%s] TEXT", col))
		}
		createStmt := fmt.Sprintf("CREATE TABLE [%s] (%s);", table, strings.Join(colDefs, ", "))
		if _, err := newAccessDB.Exec(createStmt); err != nil {
			fmt.Println(createStmt)
			fmt.Println(err)

			return fmt.Errorf("failed to create table %s: %w", table, err)
		}

		// Build an INSERT statement based on the column order.
		var colList []string
		var placeholders []string
		for _, col := range columns {
			colList = append(colList, fmt.Sprintf("[%s]", col))
			placeholders = append(placeholders, "?")
		}
		insertStmt := fmt.Sprintf("INSERT INTO [%s] (%s) VALUES (%s);", table, strings.Join(colList, ", "), strings.Join(placeholders, ", "))
		stmt, err := newAccessDB.Prepare(insertStmt)
		if err != nil {
			return fmt.Errorf("failed to prepare insert statement for table %s: %w", table, err)
		}
		defer stmt.Close()

		// Insert each row.
		for _, row := range rowsData {
			var values []interface{}
			for _, col := range columns {
				values = append(values, row[col])
			}
			if _, err := stmt.Exec(values...); err != nil {
				log.Printf("failed to insert row into table %s: %v", table, err)
			}
		}
		log.Printf("Table [%s] created successfully with %d rows.", table, len(rowsData))
	}
	return nil
}
