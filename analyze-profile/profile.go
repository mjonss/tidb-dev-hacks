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

	// Capture session variables
	sessionVars := captureSessionVars(db)
	fmt.Fprintf(os.Stderr, "Captured %d session variables\n", len(sessionVars))

	// Capture "before" analyze status
	fmt.Fprintf(os.Stderr, "Capturing before state...\n")

	// Heap before
	pprofCollector := NewPprofCollector(cfg, runDir)
	heapBeforePath := fmt.Sprintf("%s/heap_before.pb.gz", runDir)
	if err := pprofCollector.CaptureHeap(heapBeforePath); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: heap before capture failed: %v\n", err)
		heapBeforePath = ""
	}

	// Start collectors
	fmt.Fprintf(os.Stderr, "Starting collectors...\n")

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

	// Start CPU profiling loop
	ctx, cancel := context.WithCancel(context.Background())
	pprofCollector.StartCPULoop(ctx)

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

	// Heap after
	heapAfterPath := fmt.Sprintf("%s/heap_after.pb.gz", runDir)
	if err := pprofCollector.CaptureHeap(heapAfterPath); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: heap after capture failed: %v\n", err)
		heapAfterPath = ""
	}

	// Capture "after" analyze status
	analyzeStatus := captureAnalyzeStatus(db, cfg)

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
			Partitions: cfg.Partitions,
			Rows:       cfg.Rows,
			Columns:    cfg.Columns,
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
			HeapBefore: heapBeforePath,
			HeapAfter:  heapAfterPath,
			CPUFiles:   pprofCollector.CPUFiles(),
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
			case "processed_rows", "progress":
				entry.Progress = s
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
			Progress:      s.Progress,
			FailReason:    s.FailReason,
		})
	}
	return jobs
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
