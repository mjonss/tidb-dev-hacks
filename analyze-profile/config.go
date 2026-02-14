package main

import (
	"flag"
	"fmt"
	"net"
)

type Config struct {
	Host       string
	Port       int
	User       string
	Password   string
	StatusPort int

	DB         string
	Table      string
	Partitions int
	Rows       int
	Columns    int
	BatchSize  int

	Partition         string
	OutputDir         string
	CPUProfileSeconds int
	TiKVStatusPort    int
	TiDBLog           string
}

func (c *Config) DSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&interpolateParams=true",
		c.User, c.Password, net.JoinHostPort(c.Host, fmt.Sprintf("%d", c.Port)), c.DB)
}

func (c *Config) DSNNoDB() string {
	return fmt.Sprintf("%s:%s@tcp(%s)/?parseTime=true&interpolateParams=true",
		c.User, c.Password, net.JoinHostPort(c.Host, fmt.Sprintf("%d", c.Port)))
}

func (c *Config) StatusURL() string {
	return fmt.Sprintf("http://%s", net.JoinHostPort(c.Host, fmt.Sprintf("%d", c.StatusPort)))
}

func (c *Config) FullTableName() string {
	return fmt.Sprintf("%s.%s", c.DB, c.Table)
}

func (c *Config) AnalyzeSQL() string {
	sql := fmt.Sprintf("ANALYZE TABLE `%s`.`%s`", c.DB, c.Table)
	if c.Partition != "" {
		sql += " PARTITION " + c.Partition
	}
	return sql
}

func (c *Config) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("--host is required")
	}
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("--port must be 1-65535")
	}
	if c.DB == "" {
		return fmt.Errorf("--db is required")
	}
	if c.Table == "" {
		return fmt.Errorf("--table is required")
	}
	if c.Partitions < 1 {
		return fmt.Errorf("--partitions must be >= 1")
	}
	if c.Rows < 1 {
		return fmt.Errorf("--rows must be >= 1")
	}
	if c.Columns < 1 || c.Columns > 500 {
		return fmt.Errorf("--columns must be 1-500")
	}
	if c.BatchSize < 1 {
		return fmt.Errorf("--batch-size must be >= 1")
	}
	if c.CPUProfileSeconds < 1 {
		return fmt.Errorf("--cpu-profile-seconds must be >= 1")
	}
	return nil
}

func RegisterFlags(fs *flag.FlagSet, cfg *Config) {
	fs.StringVar(&cfg.Host, "host", "127.0.0.1", "TiDB host")
	fs.IntVar(&cfg.Port, "port", 4000, "TiDB SQL port")
	fs.StringVar(&cfg.User, "user", "root", "DB user")
	fs.StringVar(&cfg.Password, "password", "", "DB password")
	fs.IntVar(&cfg.StatusPort, "status-port", 10080, "TiDB status port (pprof/metrics)")

	fs.StringVar(&cfg.DB, "db", "analyze_profile", "Database name")
	fs.StringVar(&cfg.Table, "table", "t_partitioned", "Table name")
	fs.IntVar(&cfg.Partitions, "partitions", 256, "Number of HASH partitions")
	fs.IntVar(&cfg.Rows, "rows", 10000000, "Number of rows to insert")
	fs.IntVar(&cfg.Columns, "columns", 50, "Number of columns")
	fs.IntVar(&cfg.BatchSize, "batch-size", 5000, "INSERT batch size")

	fs.StringVar(&cfg.Partition, "partition", "", "Comma-separated partition names to analyze (e.g. \"p0,p1\"); empty = all")
	fs.StringVar(&cfg.OutputDir, "output-dir", "./output", "Where to write profile results")
	fs.IntVar(&cfg.CPUProfileSeconds, "cpu-profile-seconds", 10, "Duration for pprof CPU profile")
	fs.IntVar(&cfg.TiKVStatusPort, "tikv-status-port", 20180, "TiKV status port (for metrics)")
	fs.StringVar(&cfg.TiDBLog, "tidb-log", "", "Path to TiDB log file (auto-detected if empty)")
}
