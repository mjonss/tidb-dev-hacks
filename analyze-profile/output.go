package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"
)

// ProfileResult is the top-level JSON output structure.
type ProfileResult struct {
	Config           OutputConfig          `json:"config"`
	RunDir           string                `json:"run_dir"`
	SessionVars      map[string]string     `json:"session_vars"`
	AnalyzeDuration  string                `json:"analyze_duration"`
	AnalyzeStartTime time.Time             `json:"analyze_start_time"`
	AnalyzeEndTime   time.Time             `json:"analyze_end_time"`
	PartitionJobs    []PartitionJobSummary `json:"partition_jobs"`
	TiDBMetrics      []MetricSample        `json:"tidb_metrics"`
	TiKVMetrics      []MetricSample        `json:"tikv_metrics"`
	LogEntries       []LogEntry            `json:"log_entries"`
	SlowQueries      []SlowQueryEntry      `json:"slow_queries"`
	PprofFiles       PprofFiles            `json:"pprof_files"`
	AnalyzeStatus    []AnalyzeStatusEntry  `json:"analyze_status_after"`
}

type OutputConfig struct {
	Host       string `json:"host"`
	Port       int    `json:"port"`
	StatusPort int    `json:"status_port"`
	DB         string `json:"db"`
	Table      string `json:"table"`
}

type PartitionJobSummary struct {
	PartitionName string  `json:"partition_name"`
	State         string  `json:"state"`
	StartTime     *string `json:"start_time"`
	EndTime       *string `json:"end_time"`
	Duration      string  `json:"duration,omitempty"`
	ProcessedRows string  `json:"processed_rows"`
	FailReason    string  `json:"fail_reason,omitempty"`
}

type SlowQueryEntry struct {
	Query     string  `json:"query"`
	QueryTime float64 `json:"query_time"`
	MemMax    int64   `json:"mem_max"`
	TotalKeys int64   `json:"total_keys"`
	Digest    string  `json:"digest"`
}

type PprofFiles struct {
	HeapFiles []string `json:"heap_files"`
	CPUFiles  []string `json:"cpu_files"`
}

type AnalyzeStatusEntry struct {
	TableSchema   string  `json:"table_schema"`
	TableName     string  `json:"table_name"`
	PartitionName string  `json:"partition_name"`
	JobInfo       string  `json:"job_info"`
	State         string  `json:"state"`
	StartTime     *string `json:"start_time"`
	EndTime       *string `json:"end_time"`
	ProcessedRows string  `json:"processed_rows"`
	FailReason    string  `json:"fail_reason"`
}

func writeJSONResult(path string, result *ProfileResult) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(result); err != nil {
		return fmt.Errorf("encode JSON: %w", err)
	}
	return nil
}

func printSummary(result *ProfileResult) {
	fmt.Printf("\n=== ANALYZE TABLE Profile Results ===\n")
	fmt.Printf("Table:      %s.%s\n",
		result.Config.DB, result.Config.Table)
	fmt.Printf("Duration:   %s\n", result.AnalyzeDuration)
	fmt.Println()

	// Per-partition timing
	if len(result.PartitionJobs) > 0 {
		fmt.Println("--- Per-Partition Timing ---")
		for _, pj := range result.PartitionJobs {
			durStr := ""
			if pj.Duration != "" {
				durStr = fmt.Sprintf(" (%s)", pj.Duration)
			}
			startStr := "-"
			if pj.StartTime != nil {
				startStr = *pj.StartTime
			}
			endStr := "-"
			if pj.EndTime != nil {
				endStr = *pj.EndTime
			}
			fmt.Printf("  %-20s  state=%-10s  started %s, finished %s%s\n",
				pj.PartitionName, pj.State, startStr, endStr, durStr)
		}
		fmt.Println()
	}

	// Resource usage deltas
	if len(result.TiDBMetrics) >= 2 {
		fmt.Println("--- Resource Usage (delta during ANALYZE) ---")
		first := result.TiDBMetrics[0]
		last := result.TiDBMetrics[len(result.TiDBMetrics)-1]

		cpuBefore := first.Metrics["process_cpu_seconds_total"]
		cpuAfter := last.Metrics["process_cpu_seconds_total"]
		if cpuAfter > 0 {
			fmt.Printf("  CPU time:    +%.1fs\n", cpuAfter-cpuBefore)
		}

		rssBefore := first.Metrics["process_resident_memory_bytes"]
		rssAfter := last.Metrics["process_resident_memory_bytes"]
		if rssAfter > 0 {
			fmt.Printf("  RSS memory:  %s -> %s (%+s)\n",
				formatBytes(rssBefore), formatBytes(rssAfter), formatBytesDelta(rssAfter-rssBefore))
		}

		heapBefore := first.Metrics["go_memstats_heap_alloc_bytes"]
		heapAfter := last.Metrics["go_memstats_heap_alloc_bytes"]
		if heapAfter > 0 {
			fmt.Printf("  Heap alloc:  %s -> %s (%+s)\n",
				formatBytes(heapBefore), formatBytes(heapAfter), formatBytesDelta(heapAfter-heapBefore))
		}

		gorBefore := first.Metrics["go_goroutines"]
		gorAfter := last.Metrics["go_goroutines"]
		if gorAfter > 0 {
			fmt.Printf("  Goroutines:  %.0f -> %.0f (%+.0f)\n", gorBefore, gorAfter, gorAfter-gorBefore)
		}
		fmt.Println()
	}

	// Slow query
	if len(result.SlowQueries) > 0 {
		fmt.Println("--- Slow Query ---")
		for _, sq := range result.SlowQueries {
			fmt.Printf("  Query time: %.1fs, Mem max: %s, Total keys: %s\n",
				sq.QueryTime, formatBytes(float64(sq.MemMax)), formatCount(sq.TotalKeys))
		}
		fmt.Println()
	}

	// Log entries summary
	if len(result.LogEntries) > 0 {
		fmt.Printf("--- Log Entries (%d captured) ---\n", len(result.LogEntries))
		// Show first and last few
		show := result.LogEntries
		if len(show) > 10 {
			for _, e := range show[:5] {
				fmt.Printf("  [%s] %s\n", e.Timestamp.Format("15:04:05.000"), truncate(e.Line, 120))
			}
			fmt.Printf("  ... (%d more entries) ...\n", len(show)-10)
			for _, e := range show[len(show)-5:] {
				fmt.Printf("  [%s] %s\n", e.Timestamp.Format("15:04:05.000"), truncate(e.Line, 120))
			}
		} else {
			for _, e := range show {
				fmt.Printf("  [%s] %s\n", e.Timestamp.Format("15:04:05.000"), truncate(e.Line, 120))
			}
		}
		fmt.Println()
	}

	// Session variables
	if len(result.SessionVars) > 0 {
		fmt.Println("--- Session Variables (stats-related) ---")
		keys := make([]string, 0, len(result.SessionVars))
		for k := range result.SessionVars {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Printf("  %s = %s\n", k, result.SessionVars[k])
		}
		fmt.Println()
	}

	// Output files
	fmt.Println("--- Output Files ---")
	fmt.Printf("  %s/profile_result.json\n", result.RunDir)
	for _, f := range result.PprofFiles.HeapFiles {
		fmt.Printf("  %s\n", f)
	}
	for _, f := range result.PprofFiles.CPUFiles {
		fmt.Printf("  %s\n", f)
	}
}

func formatBytes(b float64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1fGB", b/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.0fMB", b/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.0fKB", b/(1<<10))
	default:
		return fmt.Sprintf("%.0fB", b)
	}
}

func formatBytesDelta(b float64) string {
	sign := "+"
	if b < 0 {
		sign = "-"
		b = -b
	}
	return sign + formatBytes(b)
}

func formatCount(n int64) string {
	if n >= 1_000_000 {
		return fmt.Sprintf("%d,%03d,%03d", n/1_000_000, (n/1_000)%1_000, n%1_000)
	}
	if n >= 1_000 {
		return fmt.Sprintf("%d,%03d", n/1_000, n%1_000)
	}
	return fmt.Sprintf("%d", n)
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}
