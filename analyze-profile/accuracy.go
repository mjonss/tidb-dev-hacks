package main

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"time"
)

// AccuracyResult holds extracted statistics data for comparison between runs.
type AccuracyResult struct {
	RowCount    RowCountCheck `json:"row_count"`
	ColumnStats []StatsEntry  `json:"column_stats,omitempty"`
	IndexStats  []StatsEntry  `json:"index_stats,omitempty"`
}

// RowCountCheck compares stats_meta row count vs actual.
type RowCountCheck struct {
	StatsCount  int64   `json:"stats_count"`
	ActualCount int64   `json:"actual_count"`
	Ratio       float64 `json:"ratio"`
}

// StatsEntry holds extracted statistics for a single column or index.
type StatsEntry struct {
	Name        string        `json:"name"`
	NDV         int64         `json:"ndv"`
	NullCount   int64         `json:"null_count"`
	AvgColSize  float64       `json:"avg_col_size,omitempty"`
	Correlation float64       `json:"correlation,omitempty"`
	TopN        []TopNEntry   `json:"topn,omitempty"`
	Buckets     []BucketEntry `json:"buckets,omitempty"`
}

// TopNEntry is a single TopN value and its frequency.
type TopNEntry struct {
	Value string `json:"value"`
	Count int64  `json:"count"`
}

// BucketEntry is a single histogram bucket.
type BucketEntry struct {
	BucketID   int64  `json:"bucket_id"`
	Count      int64  `json:"count"`
	Repeats    int64  `json:"repeats"`
	LowerBound string `json:"lower_bound"`
	UpperBound string `json:"upper_bound"`
	NDV        int64  `json:"ndv"`
}

// checkAccuracy extracts stats data (row counts, NDVs, TopN, histograms)
// for comparison between runs and against non-partitioned ground truth.
func checkAccuracy(db *sql.DB, cfg *Config) (*AccuracyResult, error) {
	fmt.Fprintf(os.Stderr, "\n--- Stats Extraction ---\n")
	start := time.Now()

	result := &AccuracyResult{}

	// 1. Row count: stats_meta vs actual
	result.RowCount = checkRowCount(db, cfg)
	fmt.Fprintf(os.Stderr, "  Row count: stats=%d actual=%d ratio=%.4f\n",
		result.RowCount.StatsCount, result.RowCount.ActualCount, result.RowCount.Ratio)

	// 2. Detect if table is partitioned
	isPartitioned := tableIsPartitioned(db, cfg)
	partFilter := ""
	if isPartitioned {
		partFilter = "global"
		fmt.Fprintf(os.Stderr, "  Table is partitioned — extracting global stats\n")
	} else {
		fmt.Fprintf(os.Stderr, "  Table is not partitioned — extracting all stats\n")
	}

	// 3. Extract column and index stats from SHOW STATS_HISTOGRAMS
	columnStats, indexStats := extractHistogramStats(db, cfg, partFilter)
	fmt.Fprintf(os.Stderr, "  Extracted %d column stats, %d index stats\n", len(columnStats), len(indexStats))

	// 4. Attach TopN entries
	attachTopN(db, cfg, partFilter, columnStats, indexStats)

	// 5. Attach histogram buckets
	attachBuckets(db, cfg, partFilter, columnStats, indexStats)

	// Flatten maps to slices
	for _, s := range columnStats {
		result.ColumnStats = append(result.ColumnStats, *s)
	}
	for _, s := range indexStats {
		result.IndexStats = append(result.IndexStats, *s)
	}

	fmt.Fprintf(os.Stderr, "  Stats extraction completed in %s\n", time.Since(start).Round(time.Millisecond))
	return result, nil
}

func checkRowCount(db *sql.DB, cfg *Config) RowCountCheck {
	tableFQN := fmt.Sprintf("`%s`.`%s`", cfg.DB, cfg.Table)
	partClause := ""
	if cfg.Partition != "" {
		partClause = " PARTITION (" + cfg.Partition + ")"
	}

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
	return RowCountCheck{
		StatsCount:  statsCount,
		ActualCount: actualCount,
		Ratio:       ratio,
	}
}

func tableIsPartitioned(db *sql.DB, cfg *Config) bool {
	var count int
	err := db.QueryRow(`
		SELECT COUNT(*) FROM information_schema.partitions
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL`,
		cfg.DB, cfg.Table).Scan(&count)
	if err != nil {
		return false
	}
	return count > 0
}

// extractHistogramStats queries SHOW STATS_HISTOGRAMS and returns maps
// keyed by column/index name for columns (is_index=0) and indexes (is_index=1).
func extractHistogramStats(db *sql.DB, cfg *Config, partFilter string) (columns, indexes map[string]*StatsEntry) {
	columns = make(map[string]*StatsEntry)
	indexes = make(map[string]*StatsEntry)

	query := fmt.Sprintf("SHOW STATS_HISTOGRAMS WHERE db_name = '%s' AND table_name = '%s'", cfg.DB, cfg.Table)
	if partFilter != "" {
		query += fmt.Sprintf(" AND partition_name = '%s'", partFilter)
	}

	rows, err := db.Query(query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  SHOW STATS_HISTOGRAMS failed: %v\n", err)
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return
	}

	// Build column index map
	colIdx := make(map[string]int)
	for i, c := range cols {
		colIdx[c] = i
	}

	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			continue
		}

		getStr := func(name string) string {
			if idx, ok := colIdx[name]; ok {
				return sqlValToString(vals[idx])
			}
			return ""
		}
		getInt := func(name string) int64 {
			s := getStr(name)
			v, _ := strconv.ParseInt(s, 10, 64)
			return v
		}
		getFloat := func(name string) float64 {
			s := getStr(name)
			v, _ := strconv.ParseFloat(s, 64)
			return v
		}

		colName := getStr("Column_name")
		isIndex := getStr("Is_index")

		entry := &StatsEntry{
			Name:        colName,
			NDV:         getInt("Distinct_count"),
			NullCount:   getInt("Null_count"),
			AvgColSize:  getFloat("Avg_col_size"),
			Correlation: getFloat("Correlation"),
		}

		if isIndex == "1" {
			indexes[colName] = entry
		} else {
			columns[colName] = entry
		}
	}
	return
}

// attachTopN queries SHOW STATS_TOPN and attaches TopN entries to the stats maps.
func attachTopN(db *sql.DB, cfg *Config, partFilter string, columns, indexes map[string]*StatsEntry) {
	query := fmt.Sprintf("SHOW STATS_TOPN WHERE db_name = '%s' AND table_name = '%s'", cfg.DB, cfg.Table)
	if partFilter != "" {
		query += fmt.Sprintf(" AND partition_name = '%s'", partFilter)
	}

	rows, err := db.Query(query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  SHOW STATS_TOPN failed: %v\n", err)
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return
	}

	colIdx := make(map[string]int)
	for i, c := range cols {
		colIdx[c] = i
	}

	totalTopN := 0
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			continue
		}

		getStr := func(name string) string {
			if idx, ok := colIdx[name]; ok {
				return sqlValToString(vals[idx])
			}
			return ""
		}

		colName := getStr("Column_name")
		isIndex := getStr("Is_index")
		value := getStr("Value")
		countStr := getStr("Count")
		count, _ := strconv.ParseInt(countStr, 10, 64)

		entry := TopNEntry{Value: value, Count: count}

		if isIndex == "1" {
			if s, ok := indexes[colName]; ok {
				s.TopN = append(s.TopN, entry)
				totalTopN++
			}
		} else {
			if s, ok := columns[colName]; ok {
				s.TopN = append(s.TopN, entry)
				totalTopN++
			}
		}
	}
	fmt.Fprintf(os.Stderr, "  Extracted %d TopN entries\n", totalTopN)
}

// attachBuckets queries SHOW STATS_BUCKETS and attaches bucket entries to the stats maps.
func attachBuckets(db *sql.DB, cfg *Config, partFilter string, columns, indexes map[string]*StatsEntry) {
	query := fmt.Sprintf("SHOW STATS_BUCKETS WHERE db_name = '%s' AND table_name = '%s'", cfg.DB, cfg.Table)
	if partFilter != "" {
		query += fmt.Sprintf(" AND partition_name = '%s'", partFilter)
	}

	rows, err := db.Query(query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  SHOW STATS_BUCKETS failed: %v\n", err)
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return
	}

	colIdx := make(map[string]int)
	for i, c := range cols {
		colIdx[c] = i
	}

	totalBuckets := 0
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			continue
		}

		getStr := func(name string) string {
			if idx, ok := colIdx[name]; ok {
				return sqlValToString(vals[idx])
			}
			return ""
		}
		getInt := func(name string) int64 {
			s := getStr(name)
			v, _ := strconv.ParseInt(s, 10, 64)
			return v
		}

		colName := getStr("Column_name")
		isIndex := getStr("Is_index")

		entry := BucketEntry{
			BucketID:   getInt("Bucket_id"),
			Count:      getInt("Count"),
			Repeats:    getInt("Repeats"),
			LowerBound: getStr("Lower_Bound"),
			UpperBound: getStr("Upper_Bound"),
			NDV:        getInt("Ndv"),
		}

		if isIndex == "1" {
			if s, ok := indexes[colName]; ok {
				s.Buckets = append(s.Buckets, entry)
				totalBuckets++
			}
		} else {
			if s, ok := columns[colName]; ok {
				s.Buckets = append(s.Buckets, entry)
				totalBuckets++
			}
		}
	}
	fmt.Fprintf(os.Stderr, "  Extracted %d histogram buckets\n", totalBuckets)
}
