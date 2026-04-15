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
	// AnalyzeDurationNs is the raw wall-clock duration as nanoseconds.
	// Use this (not AnalyzeDuration) for comparisons and averages — it
	// preserves sub-ms precision that gets lost in the string form.
	AnalyzeDurationNs int64                `json:"analyze_duration_ns"`
	AnalyzeStartTime time.Time             `json:"analyze_start_time"`
	AnalyzeEndTime   time.Time             `json:"analyze_end_time"`
	PartitionJobs    []PartitionJobSummary `json:"partition_jobs"`
	TiDBMetrics      []MetricSample        `json:"tidb_metrics"`
	TiKVMetrics      []MetricSample        `json:"tikv_metrics"`
	LogEntries       []LogEntry            `json:"log_entries"`
	SlowQueries      []SlowQueryEntry      `json:"slow_queries"`
	PprofFiles       PprofFiles            `json:"pprof_files"`
	AnalyzeStatus    []AnalyzeStatusEntry  `json:"analyze_status_after"`
	Accuracy         *AccuracyResult       `json:"accuracy,omitempty"`
	GoroutineSamples []GoroutineSample     `json:"goroutine_samples,omitempty"`
	MutexBeforeFile  string                `json:"mutex_before_file,omitempty"`
	MutexAfterFile   string                `json:"mutex_after_file,omitempty"`
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

// SlowQueryEntry mirrors the fields we pull from information_schema.slow_query.
// The breakdown columns (process/wait/backoff/cop_*) are the closest builtin
// TiDB has to a timing decomposition of a statement. See
// https://docs.pingcap.com/tidb/stable/identify-slow-queries/ for definitions.
type SlowQueryEntry struct {
	Query        string  `json:"query"`
	QueryTime    float64 `json:"query_time"`
	ParseTime    float64 `json:"parse_time,omitempty"`
	CompileTime  float64 `json:"compile_time,omitempty"`
	ProcessTime  float64 `json:"process_time,omitempty"`   // TiDB coprocessor CPU time
	WaitTime     float64 `json:"wait_time,omitempty"`      // waiting on TiKV response
	BackoffTime  float64 `json:"backoff_time,omitempty"`   // TiKV backoffs (region-not-leader, etc.)
	CopTime      float64 `json:"cop_time,omitempty"`       // total coprocessor wall
	CopProcAvg   float64 `json:"cop_proc_avg,omitempty"`   // avg per-request CPU on TiKV
	CopWaitAvg   float64 `json:"cop_wait_avg,omitempty"`   // avg per-request queue wait on TiKV
	RequestCount int64   `json:"request_count,omitempty"`
	ProcessKeys  int64   `json:"process_keys,omitempty"`
	TotalKeys    int64   `json:"total_keys"`
	MemMax       int64   `json:"mem_max"`
	DiskMax      int64   `json:"disk_max,omitempty"`
	BackoffTypes string  `json:"backoff_types,omitempty"`
	Digest       string  `json:"digest"`
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

	// Per-partition timing — summary only. Also compute the earliest start
	// and latest end across partitions to show the partition-phase wallclock
	// (which is wall = latest_end - earliest_start, typically less than the
	// total ANALYZE because of the global-merge phase after).
	if len(result.PartitionJobs) > 0 {
		stateCounts := map[string]int{}
		var durations []time.Duration
		var earliestStart, latestEnd time.Time
		const tsFmt = "2006-01-02 15:04:05"
		for _, pj := range result.PartitionJobs {
			stateCounts[pj.State]++
			if pj.Duration != "" {
				if d, err := time.ParseDuration(pj.Duration); err == nil {
					durations = append(durations, d)
				}
			}
			if pj.StartTime != nil {
				if t, err := time.Parse(tsFmt, *pj.StartTime); err == nil {
					if earliestStart.IsZero() || t.Before(earliestStart) {
						earliestStart = t
					}
				}
			}
			if pj.EndTime != nil {
				if t, err := time.Parse(tsFmt, *pj.EndTime); err == nil {
					if latestEnd.IsZero() || t.After(latestEnd) {
						latestEnd = t
					}
				}
			}
		}
		fmt.Println("--- Per-Partition Timing ---")
		fmt.Printf("  Jobs: %d", len(result.PartitionJobs))
		stateKeys := make([]string, 0, len(stateCounts))
		for k := range stateCounts {
			stateKeys = append(stateKeys, k)
		}
		sort.Strings(stateKeys)
		for _, k := range stateKeys {
			fmt.Printf("  %s=%d", k, stateCounts[k])
		}
		fmt.Println()
		if !earliestStart.IsZero() && !latestEnd.IsZero() {
			phaseWall := latestEnd.Sub(earliestStart)
			fmt.Printf("  Phase wallclock: start=%s  end=%s  wall=%s\n",
				earliestStart.Format(tsFmt), latestEnd.Format(tsFmt),
				phaseWall.Round(time.Millisecond))
		}
		if len(durations) > 0 {
			var sum time.Duration
			minD, maxD := durations[0], durations[0]
			for _, d := range durations {
				sum += d
				if d < minD {
					minD = d
				}
				if d > maxD {
					maxD = d
				}
			}
			avg := sum / time.Duration(len(durations))
			fmt.Printf("  Per-partition duration: avg=%s min=%s max=%s sum=%s\n",
				avg.Round(time.Millisecond), minD.Round(time.Millisecond),
				maxD.Round(time.Millisecond), sum.Round(time.Millisecond))
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


	// Stats extraction.
	// SHOW STATS_HISTOGRAMS reads TiDB's in-memory stats cache, which evicts
	// under pressure and returns zero buckets/TopN for evicted columns. The
	// stats_dump.json HTTP endpoint reads the authoritative on-disk tables
	// and is always complete — prefer it.
	if cols, idxs, rc, ok := loadStatsFromDump(result.RunDir); ok {
		fmt.Println("--- Stats Extraction (from stats_dump.json) ---")
		fmt.Printf("  Row count: %d\n", rc)
		if len(cols) > 0 {
			fmt.Printf("  Column stats (%d columns):\n", len(cols))
			for _, cs := range cols {
				fmt.Printf("    %-20s NDV=%-10d NullCount=%-8d TopN=%d Buckets=%d\n",
					cs.name, cs.ndv, cs.nullCount, cs.topN, cs.buckets)
			}
		}
		if len(idxs) > 0 {
			fmt.Printf("  Index stats (%d indexes):\n", len(idxs))
			for _, is := range idxs {
				fmt.Printf("    %-20s NDV=%-10d NullCount=%-8d TopN=%d Buckets=%d\n",
					is.name, is.ndv, is.nullCount, is.topN, is.buckets)
			}
		}
		fmt.Println()
	} else if result.Accuracy != nil {
		fmt.Println("--- Stats Extraction (from evictable cache — counts may under-report) ---")
		rc := result.Accuracy.RowCount
		fmt.Printf("  Row count: stats=%d actual=%d ratio=%.4f\n", rc.StatsCount, rc.ActualCount, rc.Ratio)
		if len(result.Accuracy.ColumnStats) > 0 {
			fmt.Printf("  Column stats (%d columns):\n", len(result.Accuracy.ColumnStats))
			for _, cs := range result.Accuracy.ColumnStats {
				fmt.Printf("    %-20s NDV=%-10d NullCount=%-8d TopN=%d Buckets=%d\n",
					cs.Name, cs.NDV, cs.NullCount, len(cs.TopN), len(cs.Buckets))
			}
		}
		if len(result.Accuracy.IndexStats) > 0 {
			fmt.Printf("  Index stats (%d indexes):\n", len(result.Accuracy.IndexStats))
			for _, is := range result.Accuracy.IndexStats {
				fmt.Printf("    %-20s NDV=%-10d NullCount=%-8d TopN=%d Buckets=%d\n",
					is.Name, is.NDV, is.NullCount, len(is.TopN), len(is.Buckets))
			}
		}
		fmt.Println()
	}

	fmt.Printf("Output: %s\n", result.RunDir)
}

type statsDumpSummary struct {
	name      string
	ndv       int64
	nullCount int64
	topN      int
	buckets   int
}

// loadStatsFromDump reads stats_dump.json (the HTTP /stats/dump endpoint
// output) and returns a per-column/per-index summary that isn't subject to
// TiDB's in-memory cache eviction. Returns ok=false if the file is missing.
//
// For partitioned tables the top-level columns/count are empty — the global
// stats live under partitions["global"]. Prefer that when it exists, fall
// back to the top-level entries for non-partitioned tables.
func loadStatsFromDump(runDir string) (cols, idxs []statsDumpSummary, rowCount int64, ok bool) {
	f, err := os.Open(runDir + "/stats_dump.json")
	if err != nil {
		return nil, nil, 0, false
	}
	defer f.Close()
	type scope struct {
		Count   int64                      `json:"count"`
		Columns map[string]json.RawMessage `json:"columns"`
		Indices map[string]json.RawMessage `json:"indices"`
	}
	var dump struct {
		scope
		Partitions map[string]scope `json:"partitions"`
	}
	if err := json.NewDecoder(f).Decode(&dump); err != nil {
		return nil, nil, 0, false
	}
	src := dump.scope
	if g, has := dump.Partitions["global"]; has && (g.Count > 0 || len(g.Columns) > 0 || len(g.Indices) > 0) {
		src = g
	}
	extract := func(m map[string]json.RawMessage) []statsDumpSummary {
		var out []statsDumpSummary
		for name, raw := range m {
			var entry struct {
				NullCount int64 `json:"null_count"`
				Histogram struct {
					NDV     int64             `json:"ndv"`
					Buckets []json.RawMessage `json:"buckets"`
				} `json:"histogram"`
				CMSketch struct {
					TopN []json.RawMessage `json:"top_n"`
				} `json:"cm_sketch"`
			}
			if err := json.Unmarshal(raw, &entry); err != nil {
				continue
			}
			out = append(out, statsDumpSummary{
				name:      name,
				ndv:       entry.Histogram.NDV,
				nullCount: entry.NullCount,
				topN:      len(entry.CMSketch.TopN),
				buckets:   len(entry.Histogram.Buckets),
			})
		}
		sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
		return out
	}
	cols = extract(src.Columns)
	idxs = extract(src.Indices)
	return cols, idxs, src.Count, true
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
