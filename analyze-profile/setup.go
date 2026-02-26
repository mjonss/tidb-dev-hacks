package main

import (
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

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

	profile, _ := ParsePartitionProfile(cfg.PartitionProfile)

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
	if err := bulkInsert(db, cfg, profile); err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	elapsed := time.Since(start)
	fmt.Printf("\n=== Setup Complete ===\n")
	fmt.Printf("  Table:      %s.%s\n", cfg.DB, cfg.Table)
	fmt.Printf("  Partitions: %d (HASH)\n", cfg.Partitions)
	fmt.Printf("  Profile:    %s\n", cfg.PartitionProfile)
	fmt.Printf("  Rows:       %d\n", cfg.Rows)
	fmt.Printf("  Columns:    %d\n", cfg.Columns)
	fmt.Printf("  Elapsed:    %s\n", elapsed.Round(time.Millisecond))
	return nil
}

func buildCreateTable(cfg *Config) string {
	tms := typeMappers()

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE `%s` (\n", cfg.Table))
	sb.WriteString("  `pk` BIGINT NOT NULL,\n")

	for i := 0; i < cfg.Columns; i++ {
		tm := tms[i%len(tms)]
		colName := fmt.Sprintf("c%d", i+1)
		// Cycle NULL/NOT NULL: first cycle allows NULL, next does NOT, etc.
		cycle := i / len(tms)
		nullable := "NULL"
		if cycle%2 == 1 {
			nullable = "NOT NULL"
		}
		// Default value for NOT NULL columns
		defaultClause := ""
		if nullable == "NOT NULL" {
			switch {
			case strings.HasPrefix(tm.TypeName, "INT"), strings.HasPrefix(tm.TypeName, "BIGINT"):
				defaultClause = " DEFAULT 0"
			case strings.HasPrefix(tm.TypeName, "CHAR"), strings.HasPrefix(tm.TypeName, "VARCHAR"):
				defaultClause = " DEFAULT ''"
			case strings.HasPrefix(tm.TypeName, "DECIMAL"), strings.HasPrefix(tm.TypeName, "FLOAT"), strings.HasPrefix(tm.TypeName, "DOUBLE"):
				defaultClause = " DEFAULT 0"
			case tm.TypeName == "DATE":
				defaultClause = " DEFAULT '2000-01-01'"
			case tm.TypeName == "DATETIME":
				defaultClause = " DEFAULT '2000-01-01 00:00:00'"
			case tm.TypeName == "TIMESTAMP":
				defaultClause = " DEFAULT CURRENT_TIMESTAMP"
			}
		}
		sb.WriteString(fmt.Sprintf("  `%s` %s %s%s", colName, tm.TypeName, nullable, defaultClause))
		sb.WriteString(",\n")
	}

	sb.WriteString("  PRIMARY KEY (`pk`)\n")
	sb.WriteString(fmt.Sprintf(") PARTITION BY HASH (`pk`) PARTITIONS %d", cfg.Partitions))
	return sb.String()
}

func bulkInsert(db *sql.DB, cfg *Config, profile PartitionProfile) error {
	tms := typeMappers()
	dists := distributions()

	// Compute per-partition row counts from weights
	weights := partitionWeights(profile, cfg.Partitions)
	partRows := make([]int, cfg.Partitions)
	assigned := 0
	for i, w := range weights {
		partRows[i] = int(math.Round(w * float64(cfg.Rows)))
		assigned += partRows[i]
	}
	// Adjust rounding error on the last non-zero partition
	diff := cfg.Rows - assigned
	if diff != 0 {
		for i := len(partRows) - 1; i >= 0; i-- {
			if partRows[i] > 0 || diff > 0 {
				partRows[i] += diff
				break
			}
		}
	}

	// Build column name list (including pk)
	colNames := make([]string, 0, cfg.Columns+1)
	colNames = append(colNames, "`pk`")
	for i := 0; i < cfg.Columns; i++ {
		colNames = append(colNames, fmt.Sprintf("`c%d`", i+1))
	}
	colList := strings.Join(colNames, ", ")

	// Determine distribution index for each column, with fallback for
	// string types + sequential distribution
	colDistIdx := make([]int, cfg.Columns)
	for i := 0; i < cfg.Columns; i++ {
		distIdx := i % len(dists)
		tm := tms[i%len(tms)]
		// Sequential distribution is meaningless for string types; fall back to Uniform
		if isSequentialDist(distIdx) && isStringType(tm) {
			distIdx = 0
		}
		colDistIdx[i] = distIdx
	}

	// Ensure the connection pool can support the desired concurrency.
	db.SetMaxOpenConns(cfg.InsertConcurrency + 2)

	var totalInserted atomic.Int64
	startTime := time.Now()

	// Progress ticker in main goroutine.
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				n := totalInserted.Load()
				elapsed := time.Since(startTime)
				rate := float64(n) / elapsed.Seconds()
				fmt.Fprintf(os.Stderr, "  Inserted %d/%d rows (%.0f rows/s)\n",
					n, cfg.Rows, rate)
			case <-done:
				return
			}
		}
	}()

	sem := make(chan struct{}, cfg.InsertConcurrency)
	var wg sync.WaitGroup
	errCh := make(chan error, 1) // first error wins

	for partID := 0; partID < cfg.Partitions; partID++ {
		rowsForPart := partRows[partID]
		if rowsForPart == 0 {
			continue
		}

		wg.Add(1)
		sem <- struct{}{} // acquire slot

		go func(partID, rowsForPart int) {
			defer wg.Done()
			defer func() { <-sem }() // release slot

			rng := rand.New(rand.NewSource(int64(partID) ^ time.Now().UnixNano()))

			inserted := 0
			for inserted < rowsForPart {
				// Check for early abort.
				select {
				case <-errCh:
					return
				default:
				}

				batchSize := cfg.BatchSize
				if inserted+batchSize > rowsForPart {
					batchSize = rowsForPart - inserted
				}

				var sb strings.Builder
				sb.WriteString(fmt.Sprintf("INSERT INTO `%s` (%s) VALUES ", cfg.Table, colList))

				for row := 0; row < batchSize; row++ {
					seqIdx := inserted + row + 1 // 1-based within partition
					if row > 0 {
						sb.WriteString(", ")
					}
					sb.WriteString("(")

					// pk
					pk := pkForPartition(seqIdx, partID, cfg.Partitions)
					sb.WriteString(fmt.Sprintf("%d", pk))

					for col := 0; col < cfg.Columns; col++ {
						sb.WriteString(", ")
						tm := tms[col%len(tms)]
						distIdx := colDistIdx[col]

						// For nullable columns, occasionally insert NULL
						cycle := col / len(tms)
						if cycle%2 == 0 && rng.Intn(20) == 0 {
							sb.WriteString("NULL")
						} else {
							v := dists[distIdx](rng, seqIdx, rowsForPart, partID, cfg.Partitions)
							sb.WriteString(tm.MapFunc(v, rng))
						}
					}
					sb.WriteString(")")
				}

				if _, err := db.Exec(sb.String()); err != nil {
					// Send error non-blocking (only first error kept).
					select {
					case errCh <- fmt.Errorf("batch insert at partition p%d, row %d: %w", partID, inserted, err):
					default:
					}
					return
				}

				inserted += batchSize
				totalInserted.Add(int64(batchSize))
			}
		}(partID, rowsForPart)
	}

	wg.Wait()
	close(done) // stop progress ticker

	// Check for error.
	select {
	case err := <-errCh:
		return err
	default:
	}

	fmt.Fprintf(os.Stderr, "  Inserted %d/%d rows (done)\n", totalInserted.Load(), cfg.Rows)
	return nil
}
