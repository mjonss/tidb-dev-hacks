package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Column type definitions that cycle through various types.
var columnTypes = []struct {
	typeName string
	genFunc  func(rng *rand.Rand) string
}{
	{"INT", func(rng *rand.Rand) string { return fmt.Sprintf("%d", rng.Int31()) }},
	{"BIGINT", func(rng *rand.Rand) string { return fmt.Sprintf("%d", rng.Int63()) }},
	{"CHAR(32)", func(rng *rand.Rand) string { return fmt.Sprintf("'%s'", randString(rng, 32)) }},
	{"VARCHAR(255)", func(rng *rand.Rand) string { return fmt.Sprintf("'%s'", randString(rng, 10+rng.Intn(50))) }},
	{"DECIMAL(10,2)", func(rng *rand.Rand) string { return fmt.Sprintf("%.2f", rng.Float64()*100000) }},
	{"FLOAT", func(rng *rand.Rand) string { return fmt.Sprintf("%f", rng.Float32()*1000) }},
	{"DOUBLE", func(rng *rand.Rand) string { return fmt.Sprintf("%f", rng.Float64()*100000) }},
	{"DATE", func(rng *rand.Rand) string {
		d := time.Date(2000+rng.Intn(25), time.Month(1+rng.Intn(12)), 1+rng.Intn(28), 0, 0, 0, 0, time.UTC)
		return fmt.Sprintf("'%s'", d.Format("2006-01-02"))
	}},
	{"DATETIME", func(rng *rand.Rand) string {
		d := time.Date(2000+rng.Intn(25), time.Month(1+rng.Intn(12)), 1+rng.Intn(28), rng.Intn(24), rng.Intn(60), rng.Intn(60), 0, time.UTC)
		return fmt.Sprintf("'%s'", d.Format("2006-01-02 15:04:05"))
	}},
	{"TIMESTAMP", func(rng *rand.Rand) string {
		d := time.Date(2010+rng.Intn(15), time.Month(1+rng.Intn(12)), 1+rng.Intn(28), rng.Intn(24), rng.Intn(60), rng.Intn(60), 0, time.UTC)
		return fmt.Sprintf("'%s'", d.Format("2006-01-02 15:04:05"))
	}},
}

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randString(rng *rand.Rand, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rng.Intn(len(letters))]
	}
	return string(b)
}

func runSetup(cfg *Config) error {
	start := time.Now()

	// Connect without database to create it
	db, err := sql.Open("mysql", cfg.DSNNoDB())
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("ping: %w", err)
	}
	if _, err := db.Exec("SET time_zone = '+00:00'"); err != nil {
		return fmt.Errorf("set time_zone: %w", err)
	}
	fmt.Fprintf(os.Stderr, "Connected to %s:%d\n", cfg.Host, cfg.Port)

	// Create database
	if _, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", cfg.DB)); err != nil {
		return fmt.Errorf("create database: %w", err)
	}
	fmt.Fprintf(os.Stderr, "Database: %s\n", cfg.DB)

	// Switch to target database
	if _, err := db.Exec(fmt.Sprintf("USE `%s`", cfg.DB)); err != nil {
		return fmt.Errorf("use database: %w", err)
	}

	// Drop existing table
	if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", cfg.Table)); err != nil {
		return fmt.Errorf("drop table: %w", err)
	}
	fmt.Fprintf(os.Stderr, "Dropped existing table (if any): %s\n", cfg.Table)

	// Build CREATE TABLE
	createSQL := buildCreateTable(cfg)
	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("create table: %w\nSQL: %s", err, createSQL)
	}
	fmt.Fprintf(os.Stderr, "Created table: %s (%d columns, %d partitions)\n", cfg.Table, cfg.Columns, cfg.Partitions)

	// Bulk insert
	if err := bulkInsert(db, cfg); err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	elapsed := time.Since(start)
	fmt.Printf("\n=== Setup Complete ===\n")
	fmt.Printf("  Table:      %s.%s\n", cfg.DB, cfg.Table)
	fmt.Printf("  Partitions: %d (HASH)\n", cfg.Partitions)
	fmt.Printf("  Rows:       %d\n", cfg.Rows)
	fmt.Printf("  Columns:    %d\n", cfg.Columns)
	fmt.Printf("  Elapsed:    %s\n", elapsed.Round(time.Millisecond))
	return nil
}

func buildCreateTable(cfg *Config) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE `%s` (\n", cfg.Table))
	sb.WriteString("  `pk` BIGINT NOT NULL AUTO_INCREMENT,\n")

	for i := 0; i < cfg.Columns; i++ {
		ct := columnTypes[i%len(columnTypes)]
		colName := fmt.Sprintf("c%d", i+1)
		// Cycle NULL/NOT NULL: first cycle allows NULL, next does NOT, etc.
		cycle := i / len(columnTypes)
		nullable := "NULL"
		if cycle%2 == 1 {
			nullable = "NOT NULL"
		}
		// Default value for NOT NULL columns
		defaultClause := ""
		if nullable == "NOT NULL" {
			switch {
			case strings.HasPrefix(ct.typeName, "INT"), strings.HasPrefix(ct.typeName, "BIGINT"):
				defaultClause = " DEFAULT 0"
			case strings.HasPrefix(ct.typeName, "CHAR"), strings.HasPrefix(ct.typeName, "VARCHAR"):
				defaultClause = " DEFAULT ''"
			case strings.HasPrefix(ct.typeName, "DECIMAL"), strings.HasPrefix(ct.typeName, "FLOAT"), strings.HasPrefix(ct.typeName, "DOUBLE"):
				defaultClause = " DEFAULT 0"
			case ct.typeName == "DATE":
				defaultClause = " DEFAULT '2000-01-01'"
			case ct.typeName == "DATETIME":
				defaultClause = " DEFAULT '2000-01-01 00:00:00'"
			case ct.typeName == "TIMESTAMP":
				defaultClause = " DEFAULT CURRENT_TIMESTAMP"
			}
		}
		sb.WriteString(fmt.Sprintf("  `%s` %s %s%s", colName, ct.typeName, nullable, defaultClause))
		sb.WriteString(",\n")
	}

	sb.WriteString("  PRIMARY KEY (`pk`)\n")
	sb.WriteString(fmt.Sprintf(") PARTITION BY HASH (`pk`) PARTITIONS %d", cfg.Partitions))
	return sb.String()
}

func bulkInsert(db *sql.DB, cfg *Config) error {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Build column name list
	colNames := make([]string, cfg.Columns)
	for i := 0; i < cfg.Columns; i++ {
		colNames[i] = fmt.Sprintf("`c%d`", i+1)
	}
	colList := strings.Join(colNames, ", ")

	inserted := 0
	lastReport := time.Now()
	startTime := time.Now()

	for inserted < cfg.Rows {
		batchSize := cfg.BatchSize
		if inserted+batchSize > cfg.Rows {
			batchSize = cfg.Rows - inserted
		}

		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("INSERT INTO `%s` (%s) VALUES ", cfg.Table, colList))

		for row := 0; row < batchSize; row++ {
			if row > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString("(")
			for col := 0; col < cfg.Columns; col++ {
				if col > 0 {
					sb.WriteString(", ")
				}
				ct := columnTypes[col%len(columnTypes)]
				// For nullable columns, occasionally insert NULL
				cycle := col / len(columnTypes)
				if cycle%2 == 0 && rng.Intn(20) == 0 {
					sb.WriteString("NULL")
				} else {
					sb.WriteString(ct.genFunc(rng))
				}
			}
			sb.WriteString(")")
		}

		if _, err := db.Exec(sb.String()); err != nil {
			return fmt.Errorf("batch insert at row %d: %w", inserted, err)
		}

		inserted += batchSize
		if time.Since(lastReport) > 2*time.Second {
			elapsed := time.Since(startTime)
			rate := float64(inserted) / elapsed.Seconds()
			fmt.Fprintf(os.Stderr, "  Inserted %d/%d rows (%.0f rows/s)\n", inserted, cfg.Rows, rate)
			lastReport = time.Now()
		}
	}

	fmt.Fprintf(os.Stderr, "  Inserted %d/%d rows (done)\n", inserted, cfg.Rows)
	return nil
}
