package main

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// AccuracyResult holds estimation-vs-actual comparison data.
type AccuracyResult struct {
	RowCount       RowCountCheck   `json:"row_count"`
	RangeEstimates []RangeEstimate `json:"range_estimates,omitempty"`
}

// RowCountCheck compares stats_meta row count vs actual.
type RowCountCheck struct {
	StatsCount  int64   `json:"stats_count"`
	ActualCount int64   `json:"actual_count"`
	Ratio       float64 `json:"ratio"`
}

// RangeEstimate compares EXPLAIN estRows vs actual count for a range predicate.
type RangeEstimate struct {
	Column     string  `json:"column"`
	ColumnType string  `json:"column_type"`
	Predicate  string  `json:"predicate"`
	EstRows    float64 `json:"est_rows"`
	ActualRows int64   `json:"actual_rows"`
	Ratio      float64 `json:"ratio"`
}

const maxAccuracyColumns = 5

// checkAccuracy runs estimation accuracy checks after ANALYZE.
// It compares stats_meta row counts and EXPLAIN estimates against actual counts.
func checkAccuracy(db *sql.DB, cfg *Config) (*AccuracyResult, error) {
	fmt.Fprintf(os.Stderr, "\n--- Accuracy Check ---\n")
	start := time.Now()

	tableFQN := fmt.Sprintf("`%s`.`%s`", cfg.DB, cfg.Table)
	partClause := ""
	if cfg.Partition != "" {
		partClause = " PARTITION (" + cfg.Partition + ")"
	}

	result := &AccuracyResult{}

	// 1. Row count: stats_meta vs actual
	var statsCount int64
	if cfg.Partition != "" {
		err := db.QueryRow(`
			SELECT sm.count FROM mysql.stats_meta sm
			JOIN information_schema.partitions p ON sm.table_id = p.TIDB_PARTITION_ID
			WHERE p.TABLE_SCHEMA = ? AND p.TABLE_NAME = ? AND p.PARTITION_NAME = ?`,
			cfg.DB, cfg.Table, cfg.Partition).Scan(&statsCount)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  stats_meta query failed: %v\n", err)
		}
	} else {
		err := db.QueryRow(`
			SELECT sm.count FROM mysql.stats_meta sm
			JOIN information_schema.tables t ON sm.table_id = t.TIDB_TABLE_ID
			WHERE t.TABLE_SCHEMA = ? AND t.TABLE_NAME = ?`,
			cfg.DB, cfg.Table).Scan(&statsCount)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  stats_meta query failed: %v\n", err)
		}
	}

	var actualCount int64
	countSQL := fmt.Sprintf("SELECT count(*) FROM %s%s", tableFQN, partClause)
	if err := db.QueryRow(countSQL).Scan(&actualCount); err != nil {
		fmt.Fprintf(os.Stderr, "  count(*) failed: %v\n", err)
	}

	ratio := 0.0
	if actualCount > 0 {
		ratio = float64(statsCount) / float64(actualCount)
	}
	result.RowCount = RowCountCheck{
		StatsCount:  statsCount,
		ActualCount: actualCount,
		Ratio:       ratio,
	}
	fmt.Fprintf(os.Stderr, "  Row count: stats=%d actual=%d ratio=%.4f\n", statsCount, actualCount, ratio)

	// 2. Discover columns suitable for range accuracy checks
	type colInfo struct {
		name     string
		dataType string
	}

	rows, err := db.Query(`
		SELECT COLUMN_NAME, DATA_TYPE
		FROM information_schema.columns
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME != 'pk'
		ORDER BY ORDINAL_POSITION`, cfg.DB, cfg.Table)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  column discovery failed: %v\n", err)
		return result, nil
	}

	var checkCols []colInfo
	numericTypes := map[string]bool{
		"int": true, "bigint": true, "decimal": true,
		"float": true, "double": true,
		"date": true, "datetime": true, "timestamp": true,
	}
	for rows.Next() {
		var ci colInfo
		if err := rows.Scan(&ci.name, &ci.dataType); err != nil {
			continue
		}
		if numericTypes[strings.ToLower(ci.dataType)] {
			checkCols = append(checkCols, ci)
		}
	}
	rows.Close()

	if len(checkCols) > maxAccuracyColumns {
		checkCols = checkCols[:maxAccuracyColumns]
	}

	if len(checkCols) == 0 {
		fmt.Fprintf(os.Stderr, "  No numeric/date columns found for range checks\n")
		return result, nil
	}

	// 3. Get min/max for all check columns in a single scan
	minMaxParts := make([]string, 0, len(checkCols)*2)
	for _, col := range checkCols {
		minMaxParts = append(minMaxParts,
			fmt.Sprintf("MIN(`%s`)", col.name),
			fmt.Sprintf("MAX(`%s`)", col.name))
	}
	minMaxSQL := fmt.Sprintf("SELECT %s FROM %s%s", strings.Join(minMaxParts, ", "), tableFQN, partClause)
	fmt.Fprintf(os.Stderr, "  Querying min/max for %d columns...\n", len(checkCols))

	minMaxRow := db.QueryRow(minMaxSQL)
	scanDest := make([]sql.NullString, len(checkCols)*2)
	scanPtrs := make([]interface{}, len(scanDest))
	for i := range scanDest {
		scanPtrs[i] = &scanDest[i]
	}
	if err := minMaxRow.Scan(scanPtrs...); err != nil {
		fmt.Fprintf(os.Stderr, "  min/max query failed: %v\n", err)
		return result, nil
	}

	// 4. For each column, compute range predicates and run EXPLAIN + COUNT
	for i, col := range checkCols {
		minVal := scanDest[i*2]
		maxVal := scanDest[i*2+1]
		if !minVal.Valid || !maxVal.Valid {
			continue
		}

		predicates := computePredicates(col.name, col.dataType, minVal.String, maxVal.String)
		if len(predicates) == 0 {
			continue
		}

		for _, pred := range predicates {
			// Get estimated rows from EXPLAIN
			estRows, err := explainEstRows(db, fmt.Sprintf("SELECT * FROM %s%s WHERE %s", tableFQN, partClause, pred))
			if err != nil {
				fmt.Fprintf(os.Stderr, "  %s: EXPLAIN failed: %v\n", col.name, err)
				continue
			}

			// Get actual row count
			var actual int64
			if err := db.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s%s WHERE %s", tableFQN, partClause, pred)).Scan(&actual); err != nil {
				fmt.Fprintf(os.Stderr, "  %s: count failed: %v\n", col.name, err)
				continue
			}

			r := 0.0
			if actual > 0 {
				r = estRows / float64(actual)
			}
			result.RangeEstimates = append(result.RangeEstimates, RangeEstimate{
				Column:     col.name,
				ColumnType: col.dataType,
				Predicate:  pred,
				EstRows:    estRows,
				ActualRows: actual,
				Ratio:      r,
			})
			fmt.Fprintf(os.Stderr, "  %s WHERE %s: est=%.0f actual=%d ratio=%.3f\n",
				col.name, pred, estRows, actual, r)
		}
	}

	fmt.Fprintf(os.Stderr, "  Accuracy checks completed in %s\n", time.Since(start).Round(time.Millisecond))
	return result, nil
}

// computePredicates returns 3 range predicates for a column based on its min/max values.
// The predicates target approximately the 25th, 25-75th, and 75th percentile ranges.
func computePredicates(colName, dataType, minStr, maxStr string) []string {
	dt := strings.ToLower(dataType)
	switch dt {
	case "int", "bigint":
		minV, err1 := strconv.ParseFloat(minStr, 64)
		maxV, err2 := strconv.ParseFloat(maxStr, 64)
		if err1 != nil || err2 != nil || minV == maxV {
			return nil
		}
		p25 := int64(minV + (maxV-minV)*0.25)
		p75 := int64(minV + (maxV-minV)*0.75)
		return []string{
			fmt.Sprintf("`%s` <= %d", colName, p25),
			fmt.Sprintf("`%s` BETWEEN %d AND %d", colName, p25, p75),
			fmt.Sprintf("`%s` >= %d", colName, p75),
		}
	case "decimal", "float", "double":
		minV, err1 := strconv.ParseFloat(minStr, 64)
		maxV, err2 := strconv.ParseFloat(maxStr, 64)
		if err1 != nil || err2 != nil || minV == maxV {
			return nil
		}
		p25 := minV + (maxV-minV)*0.25
		p75 := minV + (maxV-minV)*0.75
		return []string{
			fmt.Sprintf("`%s` <= %f", colName, p25),
			fmt.Sprintf("`%s` BETWEEN %f AND %f", colName, p25, p75),
			fmt.Sprintf("`%s` >= %f", colName, p75),
		}
	case "date":
		minT, err1 := time.Parse("2006-01-02", minStr)
		maxT, err2 := time.Parse("2006-01-02", maxStr)
		if err1 != nil || err2 != nil || minT.Equal(maxT) {
			return nil
		}
		totalHours := maxT.Sub(minT).Hours()
		p25 := minT.Add(time.Duration(totalHours*0.25) * time.Hour).Format("2006-01-02")
		p75 := minT.Add(time.Duration(totalHours*0.75) * time.Hour).Format("2006-01-02")
		return []string{
			fmt.Sprintf("`%s` <= '%s'", colName, p25),
			fmt.Sprintf("`%s` BETWEEN '%s' AND '%s'", colName, p25, p75),
			fmt.Sprintf("`%s` >= '%s'", colName, p75),
		}
	case "datetime", "timestamp":
		minT, err1 := time.Parse("2006-01-02 15:04:05", minStr)
		maxT, err2 := time.Parse("2006-01-02 15:04:05", maxStr)
		if err1 != nil || err2 != nil || minT.Equal(maxT) {
			return nil
		}
		totalSecs := maxT.Sub(minT).Seconds()
		p25 := minT.Add(time.Duration(totalSecs*0.25) * time.Second).Format("2006-01-02 15:04:05")
		p75 := minT.Add(time.Duration(totalSecs*0.75) * time.Second).Format("2006-01-02 15:04:05")
		return []string{
			fmt.Sprintf("`%s` <= '%s'", colName, p25),
			fmt.Sprintf("`%s` BETWEEN '%s' AND '%s'", colName, p25, p75),
			fmt.Sprintf("`%s` >= '%s'", colName, p75),
		}
	default:
		return nil
	}
}

// explainEstRows runs EXPLAIN on the given query and returns the root operator's estRows.
func explainEstRows(db *sql.DB, query string) (float64, error) {
	rows, err := db.Query("EXPLAIN " + query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return 0, err
	}

	// Find estRows column index
	estRowsIdx := -1
	for i, col := range cols {
		if strings.EqualFold(col, "estRows") || strings.EqualFold(col, "estrows") || strings.EqualFold(col, "est_rows") {
			estRowsIdx = i
			break
		}
	}
	if estRowsIdx < 0 {
		return 0, fmt.Errorf("estRows column not found in EXPLAIN output (columns: %v)", cols)
	}

	if !rows.Next() {
		return 0, fmt.Errorf("no rows in EXPLAIN output")
	}

	vals := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return 0, err
	}

	s := sqlValToString(vals[estRowsIdx])
	return strconv.ParseFloat(s, 64)
}
