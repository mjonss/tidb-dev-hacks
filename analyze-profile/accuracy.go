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

// histIDKey identifies a column or index by its internal TiDB hist_id.
type histIDKey struct {
	histID  int64
	isIndex bool
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

	// 3. Prepare raw-table fallback metadata (table_id + hist_id→name mapping)
	tableID, _ := getTableID(db, cfg)
	histMap := buildHistIDNameMap(db, cfg)

	// 4. Extract column and index stats from SHOW STATS_HISTOGRAMS (fallback: mysql.stats_histograms)
	columnStats, indexStats := extractHistogramStats(db, cfg, partFilter, tableID, histMap)
	fmt.Fprintf(os.Stderr, "  Extracted %d column stats, %d index stats\n", len(columnStats), len(indexStats))

	// 5. Attach TopN entries (fallback: mysql.stats_top_n)
	attachTopN(db, cfg, partFilter, tableID, histMap, columnStats, indexStats)

	// 6. Attach histogram buckets (fallback: mysql.stats_buckets)
	attachBuckets(db, cfg, partFilter, tableID, histMap, columnStats, indexStats)

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

// ---------------------------------------------------------------------------
// Row count check
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Table metadata helpers
// ---------------------------------------------------------------------------

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

// getTableID returns the base table_id (TIDB_TABLE_ID) used in stats tables
// for global stats of partitioned tables or the only ID for non-partitioned tables.
func getTableID(db *sql.DB, cfg *Config) (int64, error) {
	var id int64
	err := db.QueryRow(`
		SELECT TIDB_TABLE_ID FROM information_schema.tables
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
		cfg.DB, cfg.Table).Scan(&id)
	return id, err
}

// buildHistIDNameMap builds a mapping from (hist_id, is_index) to column/index name.
// For columns, hist_id == column_id which equals ordinal_position for freshly created tables.
// For indexes, uses information_schema.tidb_indexes INDEX_ID field.
func buildHistIDNameMap(db *sql.DB, cfg *Config) map[histIDKey]string {
	m := make(map[histIDKey]string)

	// Columns: ordinal_position → column_name
	rows, err := db.Query(`
		SELECT ORDINAL_POSITION, COLUMN_NAME
		FROM information_schema.columns
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION`, cfg.DB, cfg.Table)
	if err == nil {
		for rows.Next() {
			var pos int64
			var name string
			if rows.Scan(&pos, &name) == nil {
				m[histIDKey{pos, false}] = name
			}
		}
		rows.Close()
	}

	// Indexes: try tidb_indexes for INDEX_ID (TiDB-specific)
	rows, err = db.Query(`
		SELECT KEY_NAME, INDEX_ID
		FROM information_schema.tidb_indexes
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND SEQ_IN_INDEX = 1`,
		cfg.DB, cfg.Table)
	if err == nil {
		for rows.Next() {
			var name string
			var id int64
			if rows.Scan(&name, &id) == nil {
				m[histIDKey{id, true}] = name
			}
		}
		rows.Close()
	}

	return m
}

// resolveHistName looks up a hist_id in the mapping, falling back to a synthetic name.
func resolveHistName(histMap map[histIDKey]string, histID int64, isIndex bool) string {
	if name, ok := histMap[histIDKey{histID, isIndex}]; ok {
		return name
	}
	if isIndex {
		return fmt.Sprintf("idx_%d", histID)
	}
	return fmt.Sprintf("col_%d", histID)
}

// ---------------------------------------------------------------------------
// Histogram stats extraction (NDV, NullCount, AvgColSize, Correlation)
// ---------------------------------------------------------------------------

func extractHistogramStats(db *sql.DB, cfg *Config, partFilter string, tableID int64, histMap map[histIDKey]string) (columns, indexes map[string]*StatsEntry) {
	columns, indexes = showHistogramStats(db, cfg, partFilter)
	if columns != nil {
		return
	}
	fmt.Fprintf(os.Stderr, "  Falling back to mysql.stats_histograms\n")
	return rawHistogramStats(db, tableID, histMap)
}

// showHistogramStats queries SHOW STATS_HISTOGRAMS.
func showHistogramStats(db *sql.DB, cfg *Config, partFilter string) (columns, indexes map[string]*StatsEntry) {
	query := fmt.Sprintf("SHOW STATS_HISTOGRAMS WHERE db_name = '%s' AND table_name = '%s'", cfg.DB, cfg.Table)
	if partFilter != "" {
		query += fmt.Sprintf(" AND partition_name = '%s'", partFilter)
	}

	rows, err := db.Query(query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  SHOW STATS_HISTOGRAMS failed: %v\n", err)
		return nil, nil
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, nil
	}

	columns = make(map[string]*StatsEntry)
	indexes = make(map[string]*StatsEntry)

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
			v, _ := strconv.ParseInt(getStr(name), 10, 64)
			return v
		}
		getFloat := func(name string) float64 {
			v, _ := strconv.ParseFloat(getStr(name), 64)
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

// rawHistogramStats queries mysql.stats_histograms directly.
func rawHistogramStats(db *sql.DB, tableID int64, histMap map[histIDKey]string) (columns, indexes map[string]*StatsEntry) {
	columns = make(map[string]*StatsEntry)
	indexes = make(map[string]*StatsEntry)

	rows, err := db.Query(`
		SELECT hist_id, is_index, distinct_count, null_count, tot_col_size, correlation
		FROM mysql.stats_histograms
		WHERE table_id = ?`, tableID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  mysql.stats_histograms query failed: %v\n", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var histID, ndv, nullCount, totColSize int64
		var isIndex int
		var correlation float64
		if err := rows.Scan(&histID, &isIndex, &ndv, &nullCount, &totColSize, &correlation); err != nil {
			continue
		}

		name := resolveHistName(histMap, histID, isIndex == 1)
		entry := &StatsEntry{
			Name:        name,
			NDV:         ndv,
			NullCount:   nullCount,
			AvgColSize:  float64(totColSize), // raw total, not per-row average
			Correlation: correlation,
		}

		if isIndex == 1 {
			indexes[name] = entry
		} else {
			columns[name] = entry
		}
	}
	return
}

// ---------------------------------------------------------------------------
// TopN extraction
// ---------------------------------------------------------------------------

func attachTopN(db *sql.DB, cfg *Config, partFilter string, tableID int64, histMap map[histIDKey]string, columns, indexes map[string]*StatsEntry) {
	ok := showTopN(db, cfg, partFilter, columns, indexes)
	if !ok {
		fmt.Fprintf(os.Stderr, "  Falling back to mysql.stats_top_n\n")
		rawTopN(db, tableID, histMap, columns, indexes)
	}
}

// showTopN queries SHOW STATS_TOPN. Returns true on success.
func showTopN(db *sql.DB, cfg *Config, partFilter string, columns, indexes map[string]*StatsEntry) bool {
	query := fmt.Sprintf("SHOW STATS_TOPN WHERE db_name = '%s' AND table_name = '%s'", cfg.DB, cfg.Table)
	if partFilter != "" {
		query += fmt.Sprintf(" AND partition_name = '%s'", partFilter)
	}

	rows, err := db.Query(query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  SHOW STATS_TOPN failed: %v\n", err)
		return false
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return false
	}

	colIdx := make(map[string]int)
	for i, c := range cols {
		colIdx[c] = i
	}

	total := 0
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
		count, _ := strconv.ParseInt(getStr("Count"), 10, 64)

		entry := TopNEntry{Value: value, Count: count}
		if isIndex == "1" {
			if s, ok := indexes[colName]; ok {
				s.TopN = append(s.TopN, entry)
				total++
			}
		} else {
			if s, ok := columns[colName]; ok {
				s.TopN = append(s.TopN, entry)
				total++
			}
		}
	}
	fmt.Fprintf(os.Stderr, "  Extracted %d TopN entries\n", total)
	return true
}

// rawTopN queries mysql.stats_top_n directly. Values are HEX-encoded since
// the raw table stores them as binary BLOBs.
func rawTopN(db *sql.DB, tableID int64, histMap map[histIDKey]string, columns, indexes map[string]*StatsEntry) {
	rows, err := db.Query(`
		SELECT hist_id, is_index, HEX(value), count
		FROM mysql.stats_top_n
		WHERE table_id = ?
		ORDER BY hist_id, is_index, count DESC`, tableID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  mysql.stats_top_n query failed: %v\n", err)
		return
	}
	defer rows.Close()

	total := 0
	for rows.Next() {
		var histID, count int64
		var isIndex int
		var value string
		if err := rows.Scan(&histID, &isIndex, &value, &count); err != nil {
			continue
		}

		name := resolveHistName(histMap, histID, isIndex == 1)
		entry := TopNEntry{Value: value, Count: count}

		if isIndex == 1 {
			if s, ok := indexes[name]; ok {
				s.TopN = append(s.TopN, entry)
				total++
			}
		} else {
			if s, ok := columns[name]; ok {
				s.TopN = append(s.TopN, entry)
				total++
			}
		}
	}
	fmt.Fprintf(os.Stderr, "  Extracted %d TopN entries (raw, HEX-encoded values)\n", total)
}

// ---------------------------------------------------------------------------
// Bucket extraction
// ---------------------------------------------------------------------------

func attachBuckets(db *sql.DB, cfg *Config, partFilter string, tableID int64, histMap map[histIDKey]string, columns, indexes map[string]*StatsEntry) {
	ok := showBuckets(db, cfg, partFilter, columns, indexes)
	if !ok {
		fmt.Fprintf(os.Stderr, "  Falling back to mysql.stats_buckets\n")
		rawBuckets(db, tableID, histMap, columns, indexes)
	}
}

// showBuckets queries SHOW STATS_BUCKETS. Returns true on success.
func showBuckets(db *sql.DB, cfg *Config, partFilter string, columns, indexes map[string]*StatsEntry) bool {
	query := fmt.Sprintf("SHOW STATS_BUCKETS WHERE db_name = '%s' AND table_name = '%s'", cfg.DB, cfg.Table)
	if partFilter != "" {
		query += fmt.Sprintf(" AND partition_name = '%s'", partFilter)
	}

	rows, err := db.Query(query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  SHOW STATS_BUCKETS failed: %v\n", err)
		return false
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return false
	}

	colIdx := make(map[string]int)
	for i, c := range cols {
		colIdx[c] = i
	}

	total := 0
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
			v, _ := strconv.ParseInt(getStr(name), 10, 64)
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
				total++
			}
		} else {
			if s, ok := columns[colName]; ok {
				s.Buckets = append(s.Buckets, entry)
				total++
			}
		}
	}
	fmt.Fprintf(os.Stderr, "  Extracted %d histogram buckets\n", total)
	return true
}

// rawBuckets queries mysql.stats_buckets directly. Bounds are HEX-encoded
// since the raw table stores them as binary BLOBs.
func rawBuckets(db *sql.DB, tableID int64, histMap map[histIDKey]string, columns, indexes map[string]*StatsEntry) {
	rows, err := db.Query(`
		SELECT hist_id, is_index, bucket_id, count, repeats,
			HEX(lower_bound), HEX(upper_bound), ndv
		FROM mysql.stats_buckets
		WHERE table_id = ?
		ORDER BY hist_id, is_index, bucket_id`, tableID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  mysql.stats_buckets query failed: %v\n", err)
		return
	}
	defer rows.Close()

	total := 0
	for rows.Next() {
		var histID, bucketID, count, repeats, ndv int64
		var isIndex int
		var lowerBound, upperBound string
		if err := rows.Scan(&histID, &isIndex, &bucketID, &count, &repeats, &lowerBound, &upperBound, &ndv); err != nil {
			continue
		}

		name := resolveHistName(histMap, histID, isIndex == 1)
		entry := BucketEntry{
			BucketID:   bucketID,
			Count:      count,
			Repeats:    repeats,
			LowerBound: lowerBound,
			UpperBound: upperBound,
			NDV:        ndv,
		}

		if isIndex == 1 {
			if s, ok := indexes[name]; ok {
				s.Buckets = append(s.Buckets, entry)
				total++
			}
		} else {
			if s, ok := columns[name]; ok {
				s.Buckets = append(s.Buckets, entry)
				total++
			}
		}
	}
	fmt.Fprintf(os.Stderr, "  Extracted %d histogram buckets (raw, HEX-encoded bounds)\n", total)
}
