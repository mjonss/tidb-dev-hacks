package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// statsSessionVars are the session variables we capture for reference.
var statsSessionVars = []string{
	"tidb_build_stats_concurrency",
	"tidb_analyze_partition_concurrency",
	"tidb_merge_partition_stats_concurrency",
	"tidb_auto_analyze_ratio",
	"tidb_enable_async_merge_global_stats",
	"tidb_analyze_skip_column_types",
	"tidb_analyze_version",
	"tidb_partition_prune_mode",
	"tidb_build_sampling_stats_concurrency",
	"tidb_enable_sample_based_global_stats",
}

func runProfile(cfg *Config) error {
	// Create a timestamped output subdirectory to avoid overwriting previous runs
	runDir := fmt.Sprintf("%s/run_%s", cfg.OutputDir, time.Now().Format("20060102_150405"))
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}
	fmt.Fprintf(os.Stderr, "Output dir: %s\n", runDir)

	// Connect
	db, err := sql.Open("mysql", cfg.DSN())
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("ping: %w", err)
	}
	fmt.Fprintf(os.Stderr, "Connected to %s:%d\n", cfg.Host, cfg.Port)

	// Set user-requested variables on both global and session level
	for _, sv := range cfg.SetVariables {
		parts := strings.SplitN(sv, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("--set-variable %q: expected name=value", sv)
		}
		name, value := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		for _, scope := range []string{"GLOBAL", "SESSION"} {
			setSQL := fmt.Sprintf("SET @@%s.%s = '%s'", scope, name, value)
			if _, err := db.Exec(setSQL); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: %s failed: %v\n", setSQL, err)
			} else {
				fmt.Fprintf(os.Stderr, "Set %s %s = %s\n", scope, name, value)
			}
		}
	}

	// Verify table exists
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		cfg.DB, cfg.Table).Scan(&count)
	if err != nil || count == 0 {
		return fmt.Errorf("table %s.%s not found — run 'setup' first", cfg.DB, cfg.Table)
	}

	// Clear stats if requested
	if cfg.TruncateStats {
		if err := truncateStats(db); err != nil {
			return err
		}
	} else if cfg.DropStats {
		if err := dropStats(db, cfg); err != nil {
			return err
		}
	}

	// Capture session variables
	sessionVars := captureSessionVars(db)
	fmt.Fprintf(os.Stderr, "Captured %d session variables\n", len(sessionVars))

	// Start collectors
	fmt.Fprintf(os.Stderr, "Starting collectors...\n")

	pprofCollector := NewPprofCollector(cfg, runDir)

	jobsPoller := NewAnalyzeJobsPoller(db, cfg)
	jobsPoller.Start()

	metricsPoller := NewMetricsPoller(cfg)
	metricsPoller.Start()

	var logTailer *LogTailer
	if cfg.TiDBLog != "" {
		logTailer = NewLogTailer(cfg.TiDBLog)
		logTailer.Start()
		fmt.Fprintf(os.Stderr, "Tailing log: %s\n", cfg.TiDBLog)
	}

	// Start pprof loop (heap snapshot + CPU profile each iteration)
	ctx, cancel := context.WithCancel(context.Background())
	pprofCollector.StartLoop(ctx)

	// Run ANALYZE
	analyzeSQL := cfg.AnalyzeSQL()
	fmt.Fprintf(os.Stderr, "Running: %s\n", analyzeSQL)
	startTime := time.Now()

	_, analyzeErr := db.Exec(analyzeSQL)
	endTime := time.Now()
	duration := endTime.Sub(startTime)

	if analyzeErr != nil {
		fmt.Fprintf(os.Stderr, "ANALYZE failed: %v\n", analyzeErr)
		fmt.Fprintf(os.Stderr, "Collecting results anyway...\n")
	} else {
		fmt.Fprintf(os.Stderr, "ANALYZE completed in %s\n", duration.Round(time.Millisecond))
	}

	// Stop collectors
	fmt.Fprintf(os.Stderr, "Stopping collectors...\n")
	cancel()
	jobsPoller.Stop()
	metricsPoller.Stop()
	if logTailer != nil {
		logTailer.Stop()
	}
	pprofCollector.Stop()

	// Final heap snapshot after ANALYZE
	finalHeapPath := fmt.Sprintf("%s/heap_%d.pb.gz", runDir, len(pprofCollector.HeapFiles()))
	if err := pprofCollector.CaptureHeap(finalHeapPath); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: final heap snapshot failed: %v\n", err)
	} else {
		pprofCollector.mu.Lock()
		pprofCollector.heapFiles = append(pprofCollector.heapFiles, finalHeapPath)
		pprofCollector.mu.Unlock()
	}

	// Capture "after" analyze status
	analyzeStatus := captureAnalyzeStatus(db, cfg)

	// Dump table statistics (non-fatal on error)
	dumpTableStats(db, cfg, runDir)

	// Build per-partition job summaries from the final analyze status
	partitionJobs := buildPartitionJobs(analyzeStatus)

	// Query slow_query
	slowQueries := querySlowQueries(db, cfg, startTime)

	// Gather log entries
	var logEntries []LogEntry
	if logTailer != nil {
		logEntries = logTailer.Entries()
	}

	// Build result
	result := &ProfileResult{
		Config: OutputConfig{
			Host:       cfg.Host,
			Port:       cfg.Port,
			StatusPort: cfg.StatusPort,
			DB:         cfg.DB,
			Table:      cfg.Table,
		},
		RunDir:      runDir,
		SessionVars:      sessionVars,
		AnalyzeDuration:  duration.Round(time.Millisecond).String(),
		AnalyzeStartTime: startTime,
		AnalyzeEndTime:   endTime,
		PartitionJobs:    partitionJobs,
		TiDBMetrics:      metricsPoller.TiDBSamples(),
		TiKVMetrics:      metricsPoller.TiKVSamples(),
		LogEntries:       logEntries,
		SlowQueries:      slowQueries,
		PprofFiles: PprofFiles{
			HeapFiles: pprofCollector.HeapFiles(),
			CPUFiles:  pprofCollector.CPUFiles(),
		},
		AnalyzeStatus: analyzeStatus,
	}

	// Write JSON
	jsonPath := fmt.Sprintf("%s/profile_result.json", runDir)
	if err := writeJSONResult(jsonPath, result); err != nil {
		return fmt.Errorf("write JSON: %w", err)
	}
	fmt.Fprintf(os.Stderr, "Wrote %s\n", jsonPath)

	// Print summary
	printSummary(result)

	return nil
}

var statsTables = []string{
	"mysql.stats_meta",
	"mysql.stats_meta_history",
	"mysql.stats_histograms",
	"mysql.stats_buckets",
	"mysql.stats_topn",
	"mysql.stats_fm_sketch",
	"mysql.stats_extended",
	"mysql.stats_feedback",
	"mysql.stats_history",
	"mysql.analyze_options",
	"mysql.column_stats_usage",
	"mysql.stats_table_data",
	"mysql.stats_global_merge_data",
}

func truncateStats(db *sql.DB) error {
	fmt.Fprintf(os.Stderr, "Truncating stats tables...")
	truncStart := time.Now()
	for _, table := range statsTables {
		if _, err := db.Exec(fmt.Sprintf("TRUNCATE TABLE %s", table)); err != nil {
			fmt.Fprintf(os.Stderr, " %s failed: %v (skipping)\n", table, err)
		}
	}
	fmt.Fprintf(os.Stderr, " done in %s\n", time.Since(truncStart).Round(100*time.Millisecond))
	return nil
}

const dropStatsBatchSize = 8

func dropStats(db *sql.DB, cfg *Config) error {
	fmt.Fprintf(os.Stderr, "Dropping statistics for %s.%s...", cfg.DB, cfg.Table)
	dropStart := time.Now()

	// Fetch partition names.
	rows, err := db.Query(
		"SELECT PARTITION_NAME FROM information_schema.partitions WHERE table_schema = ? AND table_name = ? AND PARTITION_NAME IS NOT NULL ORDER BY PARTITION_NAME",
		cfg.DB, cfg.Table)
	if err != nil {
		fmt.Fprintln(os.Stderr)
		return fmt.Errorf("drop stats: list partitions: %w", err)
	}
	var partitions []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			fmt.Fprintln(os.Stderr)
			return fmt.Errorf("drop stats: scan partition: %w", err)
		}
		partitions = append(partitions, name)
	}
	rows.Close()

	// Drop stats in batches of partitions to avoid OOM on large tables.
	for i := 0; i < len(partitions); i += dropStatsBatchSize {
		end := i + dropStatsBatchSize
		if end > len(partitions) {
			end = len(partitions)
		}
		batch := partitions[i:end]
		dropSQL := fmt.Sprintf("DROP STATS `%s`.`%s` PARTITION %s", cfg.DB, cfg.Table, strings.Join(batch, ", "))
		if _, err := db.Exec(dropSQL); err != nil {
			fmt.Fprintln(os.Stderr)
			return fmt.Errorf("drop stats partition %s: %w", strings.Join(batch, ","), err)
		}
	}

	// Drop the global (non-partition) stats.
	if _, err := db.Exec(fmt.Sprintf("DROP STATS `%s`.`%s`", cfg.DB, cfg.Table)); err != nil {
		fmt.Fprintln(os.Stderr)
		return fmt.Errorf("drop stats (global): %w", err)
	}

	fmt.Fprintf(os.Stderr, " done in %s (%d partitions)\n", time.Since(dropStart).Round(100*time.Millisecond), len(partitions))
	return nil
}

func captureSessionVars(db *sql.DB) map[string]string {
	vars := make(map[string]string)
	for _, v := range statsSessionVars {
		var val string
		row := db.QueryRow(fmt.Sprintf("SELECT @@%s", v))
		if err := row.Scan(&val); err != nil {
			continue
		}
		vars[v] = val
	}
	return vars
}

func captureAnalyzeStatus(db *sql.DB, cfg *Config) []AnalyzeStatusEntry {
	query := `SHOW ANALYZE STATUS WHERE table_schema = ? AND table_name = ?`
	rows, err := db.Query(query, cfg.DB, cfg.Table)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: SHOW ANALYZE STATUS failed: %v\n", err)
		return nil
	}
	defer rows.Close()

	var entries []AnalyzeStatusEntry
	cols, err := rows.Columns()
	if err != nil {
		return nil
	}

	for rows.Next() {
		// SHOW ANALYZE STATUS column count varies by TiDB version, scan dynamically
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			continue
		}

		entry := AnalyzeStatusEntry{}
		for i, col := range cols {
			s := sqlValToString(vals[i])
			switch strings.ToLower(col) {
			case "table_schema":
				entry.TableSchema = s
			case "table_name":
				entry.TableName = s
			case "partition_name":
				entry.PartitionName = s
			case "job_info":
				entry.JobInfo = s
			case "state":
				entry.State = s
			case "start_time":
				if s != "" {
					entry.StartTime = &s
				}
			case "end_time":
				if s != "" {
					entry.EndTime = &s
				}
			case "processed_rows":
				entry.ProcessedRows = s
			case "fail_reason":
				entry.FailReason = s
			}
		}
		entries = append(entries, entry)
	}
	return entries
}

func sqlValToString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case []byte:
		return string(val)
	case string:
		return val
	case time.Time:
		return val.Format("2006-01-02 15:04:05")
	default:
		return fmt.Sprintf("%v", val)
	}
}

func buildPartitionJobs(status []AnalyzeStatusEntry) []PartitionJobSummary {
	var jobs []PartitionJobSummary
	for _, s := range status {
		dur := ""
		if s.StartTime != nil && s.EndTime != nil {
			t1, err1 := time.Parse("2006-01-02 15:04:05", *s.StartTime)
			t2, err2 := time.Parse("2006-01-02 15:04:05", *s.EndTime)
			if err1 == nil && err2 == nil {
				dur = t2.Sub(t1).Round(time.Millisecond).String()
			}
		}
		jobs = append(jobs, PartitionJobSummary{
			PartitionName: s.PartitionName,
			State:         s.State,
			StartTime:     s.StartTime,
			EndTime:       s.EndTime,
			Duration:      dur,
			ProcessedRows: s.ProcessedRows,
			FailReason:    s.FailReason,
		})
	}
	return jobs
}

func dumpTableStats(db *sql.DB, cfg *Config, runDir string) {
	type sqlDump struct {
		filename string
		query    string
	}

	dumps := []sqlDump{
		{"stats_topn.tsv", fmt.Sprintf("SHOW stats_topn WHERE db_name = '%s' AND table_name = '%s'", cfg.DB, cfg.Table)},
		{"stats_histograms.tsv", fmt.Sprintf("SHOW stats_histograms WHERE db_name = '%s' AND table_name = '%s'", cfg.DB, cfg.Table)},
		{"stats_buckets.tsv", fmt.Sprintf("SHOW stats_buckets WHERE db_name = '%s' AND table_name = '%s'", cfg.DB, cfg.Table)},
		{"stats_meta.tsv", fmt.Sprintf("SHOW stats_meta WHERE db_name = '%s' AND table_name = '%s'", cfg.DB, cfg.Table)},
	}

	for _, d := range dumps {
		if err := dumpQueryToTSV(db, d.query, fmt.Sprintf("%s/%s", runDir, d.filename)); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: dump %s failed: %v\n", d.filename, err)
		} else {
			fmt.Fprintf(os.Stderr, "Wrote %s/%s\n", runDir, d.filename)
		}
	}

	// HTTP stats dump
	statsURL := fmt.Sprintf("%s/stats/dump/%s/%s", cfg.StatusURL(), cfg.DB, cfg.Table)
	statsPath := fmt.Sprintf("%s/stats_dump.json", runDir)
	if err := downloadFile(statsURL, statsPath, 60*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: stats dump from %s failed: %v\n", statsURL, err)
	} else {
		fmt.Fprintf(os.Stderr, "Wrote %s\n", statsPath)
	}
}

func dumpQueryToTSV(db *sql.DB, query, path string) error {
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Header
	fmt.Fprintln(f, strings.Join(cols, "\t"))

	// Rows
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			continue
		}
		fields := make([]string, len(cols))
		for i := range cols {
			fields[i] = sqlValToString(vals[i])
		}
		fmt.Fprintln(f, strings.Join(fields, "\t"))
	}
	return nil
}

func querySlowQueries(db *sql.DB, cfg *Config, since time.Time) []SlowQueryEntry {
	query := `SELECT query, query_time, mem_max, total_keys, digest
		FROM information_schema.slow_query
		WHERE time >= ?
		  AND (query LIKE '%ANALYZE%' OR query LIKE '%analyze%')
		  AND query LIKE ?
		ORDER BY time DESC
		LIMIT 10`

	tablePattern := fmt.Sprintf("%%%s%%", cfg.Table)
	rows, err := db.Query(query, since.Format("2006-01-02 15:04:05"), tablePattern)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: slow query lookup failed: %v\n", err)
		return nil
	}
	defer rows.Close()

	var entries []SlowQueryEntry
	for rows.Next() {
		var e SlowQueryEntry
		if err := rows.Scan(&e.Query, &e.QueryTime, &e.MemMax, &e.TotalKeys, &e.Digest); err != nil {
			continue
		}
		entries = append(entries, e)
	}
	return entries
}
