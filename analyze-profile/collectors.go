package main

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// AnalyzeJobSnapshot represents one row from mysql.analyze_jobs at a point in time.
type AnalyzeJobSnapshot struct {
	PollTime     time.Time `json:"poll_time"`
	JobID        int64     `json:"job_id"`
	TableSchema  string    `json:"table_schema"`
	TableName    string    `json:"table_name"`
	PartitionName string   `json:"partition_name"`
	JobInfo      string    `json:"job_info"`
	State        string    `json:"state"`
	StartTime    *string   `json:"start_time"`
	EndTime      *string   `json:"end_time"`
	Progress     string    `json:"progress"`
	FailReason   string    `json:"fail_reason"`
}

// MetricSample is one snapshot of metrics at a point in time.
type MetricSample struct {
	Timestamp time.Time          `json:"timestamp"`
	Metrics   map[string]float64 `json:"metrics"`
}

// LogEntry is a captured TiDB log line.
type LogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Line      string    `json:"line"`
}

// tidbMetricNames are the metric prefixes we look for in TiDB /metrics.
var tidbMetricNames = []string{
	"process_cpu_seconds_total",
	"process_resident_memory_bytes",
	"go_memstats_heap_alloc_bytes",
	"go_memstats_heap_inuse_bytes",
	"go_goroutines",
	"tidb_statistics_auto_analyze_total",
	"tidb_statistics_stats_inaccuracy_rate",
	"tidb_session_execute_duration_seconds_count",
	"tidb_session_execute_duration_seconds_sum",
	"tidb_distsql_handle_query_duration_seconds_count",
	"tidb_distsql_handle_query_duration_seconds_sum",
	"tidb_distsql_scan_keys_num_count",
	"tidb_distsql_scan_keys_num_sum",
}

// tikvMetricNames are the metric prefixes we look for in TiKV /metrics.
var tikvMetricNames = []string{
	"process_cpu_seconds_total",
	"process_resident_memory_bytes",
	"tikv_engine_read_served",
	"tikv_grpc_msg_duration_seconds_count",
	"tikv_grpc_msg_duration_seconds_sum",
	"tikv_coprocessor_request_duration_seconds_count",
	"tikv_coprocessor_request_duration_seconds_sum",
	"tikv_coprocessor_scan_keys_count",
	"tikv_coprocessor_scan_keys_sum",
}

// AnalyzeJobsPoller polls mysql.analyze_jobs for the target table.
type AnalyzeJobsPoller struct {
	db        *sql.DB
	cfg       *Config
	mu        sync.Mutex
	snapshots []AnalyzeJobSnapshot
	stopCh    chan struct{}
	done      chan struct{}
}

func NewAnalyzeJobsPoller(db *sql.DB, cfg *Config) *AnalyzeJobsPoller {
	return &AnalyzeJobsPoller{
		db:     db,
		cfg:    cfg,
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
}

func (p *AnalyzeJobsPoller) Start() {
	go func() {
		defer close(p.done)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-p.stopCh:
				p.poll() // final poll
				return
			case <-ticker.C:
				p.poll()
			}
		}
	}()
}

func (p *AnalyzeJobsPoller) poll() {
	query := `SELECT job_id, table_schema, table_name, partition_name, job_info, state,
		start_time, end_time, IFNULL(progress, ''), IFNULL(fail_reason, '')
		FROM mysql.analyze_jobs
		WHERE table_schema = ? AND table_name = ?
		ORDER BY job_id`

	rows, err := p.db.Query(query, p.cfg.DB, p.cfg.Table)
	if err != nil {
		// Silently skip — table may not exist or be different schema
		return
	}
	defer rows.Close()

	now := time.Now()
	for rows.Next() {
		var snap AnalyzeJobSnapshot
		snap.PollTime = now
		if err := rows.Scan(&snap.JobID, &snap.TableSchema, &snap.TableName,
			&snap.PartitionName, &snap.JobInfo, &snap.State,
			&snap.StartTime, &snap.EndTime, &snap.Progress, &snap.FailReason); err != nil {
			continue
		}
		p.mu.Lock()
		p.snapshots = append(p.snapshots, snap)
		p.mu.Unlock()
	}
}

func (p *AnalyzeJobsPoller) Stop() {
	close(p.stopCh)
	<-p.done
}

func (p *AnalyzeJobsPoller) Snapshots() []AnalyzeJobSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]AnalyzeJobSnapshot, len(p.snapshots))
	copy(out, p.snapshots)
	return out
}

// MetricsPoller scrapes /metrics endpoints periodically.
type MetricsPoller struct {
	cfg     *Config
	mu      sync.Mutex
	tidb    []MetricSample
	tikv    []MetricSample
	stopCh  chan struct{}
	done    chan struct{}
}

func NewMetricsPoller(cfg *Config) *MetricsPoller {
	return &MetricsPoller{
		cfg:    cfg,
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
}

func (m *MetricsPoller) Start() {
	go func() {
		defer close(m.done)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		m.scrape() // initial scrape
		for {
			select {
			case <-m.stopCh:
				m.scrape() // final scrape
				return
			case <-ticker.C:
				m.scrape()
			}
		}
	}()
}

func (m *MetricsPoller) scrape() {
	// TiDB metrics
	tidbSample := scrapeMetrics(m.cfg.StatusURL()+"/metrics", tidbMetricNames)
	if tidbSample != nil {
		m.mu.Lock()
		m.tidb = append(m.tidb, *tidbSample)
		m.mu.Unlock()
	}

	// TiKV metrics — discover hosts
	tikvHosts := m.discoverTiKVHosts()
	for _, host := range tikvHosts {
		url := fmt.Sprintf("http://%s/metrics", host)
		sample := scrapeMetrics(url, tikvMetricNames)
		if sample != nil {
			// Prefix metrics with the host for disambiguation
			prefixed := &MetricSample{Timestamp: sample.Timestamp, Metrics: make(map[string]float64)}
			for k, v := range sample.Metrics {
				prefixed.Metrics[fmt.Sprintf("tikv_%s_%s", host, k)] = v
			}
			m.mu.Lock()
			m.tikv = append(m.tikv, *prefixed)
			m.mu.Unlock()
		}
	}
}

func (m *MetricsPoller) discoverTiKVHosts() []string {
	// Try INFORMATION_SCHEMA.TIKV_STORE_STATUS or fall back to same host
	// For simplicity, use same host with tikv-status-port
	return []string{fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.TiKVStatusPort)}
}

func (m *MetricsPoller) Stop() {
	close(m.stopCh)
	<-m.done
}

func (m *MetricsPoller) TiDBSamples() []MetricSample {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]MetricSample, len(m.tidb))
	copy(out, m.tidb)
	return out
}

func (m *MetricsPoller) TiKVSamples() []MetricSample {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]MetricSample, len(m.tikv))
	copy(out, m.tikv)
	return out
}

func scrapeMetrics(url string, names []string) *MetricSample {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil
	}

	sample := &MetricSample{
		Timestamp: time.Now(),
		Metrics:   make(map[string]float64),
	}

	scanner := bufio.NewScanner(resp.Body)
	// Increase buffer size for large metrics pages
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		for _, name := range names {
			// Match lines starting with the metric name (exact or with labels)
			if strings.HasPrefix(line, name+" ") || strings.HasPrefix(line, name+"{") {
				// For simple metrics (no labels), extract the value directly
				if strings.HasPrefix(line, name+" ") {
					parts := strings.Fields(line)
					if len(parts) >= 2 {
						if v, err := strconv.ParseFloat(parts[1], 64); err == nil {
							sample.Metrics[name] = v
						}
					}
				} else {
					// Metric with labels — store with full label set
					// Find the closing brace
					braceEnd := strings.Index(line, "}")
					if braceEnd > 0 && braceEnd+1 < len(line) {
						key := line[:braceEnd+1]
						valStr := strings.TrimSpace(line[braceEnd+1:])
						// Remove trailing timestamp if present
						parts := strings.Fields(valStr)
						if len(parts) >= 1 {
							if v, err := strconv.ParseFloat(parts[0], 64); err == nil {
								sample.Metrics[key] = v
							}
						}
					}
				}
				break
			}
		}
	}

	if len(sample.Metrics) == 0 {
		return nil
	}
	return sample
}

// LogTailer tails a log file and captures lines matching analyze/stats patterns.
type LogTailer struct {
	path    string
	mu      sync.Mutex
	entries []LogEntry
	stopCh  chan struct{}
	done    chan struct{}
}

func NewLogTailer(path string) *LogTailer {
	return &LogTailer{
		path:   path,
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
}

func (t *LogTailer) Start() {
	go func() {
		defer close(t.done)

		f, err := os.Open(t.path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: cannot open log file %s: %v\n", t.path, err)
			return
		}
		defer f.Close()

		// Seek to end — we only want new lines
		if _, err := f.Seek(0, io.SeekEnd); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: cannot seek log file: %v\n", err)
			return
		}

		reader := bufio.NewReader(f)
		for {
			select {
			case <-t.stopCh:
				// Drain remaining lines
				t.readLines(reader)
				return
			default:
				t.readLines(reader)
				time.Sleep(200 * time.Millisecond)
			}
		}
	}()
}

var logPatterns = []string{
	"analyze",
	"statistics",
	"merge",
	"global stats",
	"GlobalStats",
	"handleAnalyze",
	"SaveStatsToStorage",
	"build stats",
}

func (t *LogTailer) readLines(reader *bufio.Reader) {
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			lower := strings.ToLower(line)
			for _, pattern := range logPatterns {
				if strings.Contains(lower, pattern) {
					t.mu.Lock()
					t.entries = append(t.entries, LogEntry{
						Timestamp: time.Now(),
						Line:      strings.TrimRight(line, "\n\r"),
					})
					t.mu.Unlock()
					break
				}
			}
		}
		if err != nil {
			return
		}
	}
}

func (t *LogTailer) Stop() {
	close(t.stopCh)
	<-t.done
}

func (t *LogTailer) Entries() []LogEntry {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]LogEntry, len(t.entries))
	copy(out, t.entries)
	return out
}

// PprofCollector captures heap and CPU profiles from TiDB's pprof endpoint.
type PprofCollector struct {
	cfg       *Config
	outputDir string
	mu        sync.Mutex
	cpuFiles  []string
	heapFiles []string
	stopCh    chan struct{}
	done      chan struct{}
}

func NewPprofCollector(cfg *Config, outputDir string) *PprofCollector {
	return &PprofCollector{
		cfg:       cfg,
		outputDir: outputDir,
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
}

// CaptureHeap captures a heap profile and saves it to the given path.
func (p *PprofCollector) CaptureHeap(path string) error {
	url := fmt.Sprintf("%s/debug/pprof/heap", p.cfg.StatusURL())
	return downloadFile(url, path, 30*time.Second)
}

// StartLoop captures CPU and heap profiles in a loop until stopped.
// A heap snapshot is taken before each CPU profile starts.
func (p *PprofCollector) StartLoop(ctx context.Context) {
	go func() {
		defer close(p.done)
		idx := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-p.stopCh:
				return
			default:
			}

			// Heap snapshot
			heapPath := fmt.Sprintf("%s/heap_%d.pb.gz", p.outputDir, idx)
			if err := p.CaptureHeap(heapPath); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: heap snapshot %d failed: %v\n", idx, err)
			} else {
				p.mu.Lock()
				p.heapFiles = append(p.heapFiles, heapPath)
				p.mu.Unlock()
			}

			// CPU profile
			cpuPath := fmt.Sprintf("%s/cpu_profile_%d.pb.gz", p.outputDir, idx)
			url := fmt.Sprintf("%s/debug/pprof/profile?seconds=%d", p.cfg.StatusURL(), p.cfg.CPUProfileSeconds)

			err := downloadFile(url, cpuPath, time.Duration(p.cfg.CPUProfileSeconds+30)*time.Second)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: CPU profile %d failed: %v\n", idx, err)
				select {
				case <-p.stopCh:
					return
				case <-ctx.Done():
					return
				default:
				}
				time.Sleep(time.Second)
				continue
			}

			p.mu.Lock()
			p.cpuFiles = append(p.cpuFiles, cpuPath)
			p.mu.Unlock()
			idx++
		}
	}()
}

func (p *PprofCollector) Stop() {
	close(p.stopCh)
	<-p.done
}

func (p *PprofCollector) CPUFiles() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.cpuFiles))
	copy(out, p.cpuFiles)
	return out
}

func (p *PprofCollector) HeapFiles() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.heapFiles))
	copy(out, p.heapFiles)
	return out
}

func downloadFile(url, path string, timeout time.Duration) error {
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, resp.Body)
	return err
}
