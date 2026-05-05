package main

import (
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
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

	// Build the per-column plan once and pass it through. ValidateSetup
	// already parsed --column-spec; here we materialize the full plan
	// (including the cyclic default branch).
	plan, err := columnPlan(cfg)
	if err != nil {
		return fmt.Errorf("column plan: %w", err)
	}

	// Resolve random seed.
	if cfg.Seed == 0 {
		cfg.Seed = time.Now().UnixNano()
	}
	fmt.Fprintf(os.Stderr, "Seed: %d\n", cfg.Seed)

	// Connect without database to create it
	db, err := sql.Open("mysql", cfg.DSNNoDB())
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("ping: %w", err)
	}
	fmt.Fprintf(os.Stderr, "Connected to %s:%d\n", cfg.Host, cfg.Port)

	// Create database
	if _, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", cfg.DB)); err != nil {
		return fmt.Errorf("create database: %w", err)
	}
	fmt.Fprintf(os.Stderr, "Database: %s\n", cfg.DB)

	// Reconnect with the target database in the DSN so all pooled connections
	// have the correct database selected (USE only affects a single connection).
	db.Close()
	db, err = sql.Open("mysql", cfg.DSN())
	if err != nil {
		return fmt.Errorf("reconnect: %w", err)
	}
	defer db.Close()

	// Drop existing table
	if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", cfg.Table)); err != nil {
		return fmt.Errorf("drop table: %w", err)
	}
	fmt.Fprintf(os.Stderr, "Dropped existing table (if any): %s\n", cfg.Table)

	// Build CREATE TABLE
	createSQL := buildCreateTable(cfg, plan)
	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("create table: %w\nSQL: %s", err, createSQL)
	}
	fmt.Fprintf(os.Stderr, "Created table: %s (%d columns, %d partitions)\n", cfg.Table, cfg.Columns, cfg.Partitions)
	printColumnLayout(cfg, plan)

	// Bulk insert
	if err := bulkInsert(db, cfg, plan, profile); err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	elapsed := time.Since(start)
	fmt.Printf("\n=== Setup Complete ===\n")
	fmt.Printf("  Table:      %s.%s\n", cfg.DB, cfg.Table)
	fmt.Printf("  Partitions: %d (HASH)\n", cfg.Partitions)
	fmt.Printf("  Profile:    %s\n", cfg.PartitionProfile)
	fmt.Printf("  Rows:       %d\n", cfg.Rows)
	fmt.Printf("  Columns:    %d\n", cfg.Columns)
	if len(cfg.Indexes) > 0 {
		fmt.Printf("  Indexes:    %d secondary\n", len(cfg.Indexes))
		for _, idx := range cfg.Indexes {
			fmt.Printf("              KEY (%s)\n", idx)
		}
	}
	if cfg.MaxStringLength != 60 {
		fmt.Printf("  MaxStrLen:  %d\n", cfg.MaxStringLength)
	}
	fmt.Printf("  Elapsed:    %s\n", elapsed.Round(time.Millisecond))
	return nil
}

func buildCreateTable(cfg *Config, plan []ColumnPlan) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE `%s` (\n", cfg.Table))
	sb.WriteString("  `pk` BIGINT NOT NULL,\n")

	for i, p := range plan {
		colName := fmt.Sprintf("c%d", i+1)
		nullable := "NULL"
		if !p.Nullable {
			nullable = "NOT NULL"
		}
		// Default value for NOT NULL columns
		defaultClause := ""
		if !p.Nullable {
			switch {
			case strings.HasPrefix(p.Type.TypeName, "INT"), strings.HasPrefix(p.Type.TypeName, "BIGINT"):
				defaultClause = " DEFAULT 0"
			case strings.HasPrefix(p.Type.TypeName, "CHAR"), strings.HasPrefix(p.Type.TypeName, "VARCHAR"):
				defaultClause = " DEFAULT ''"
			case strings.HasPrefix(p.Type.TypeName, "DECIMAL"), strings.HasPrefix(p.Type.TypeName, "FLOAT"), strings.HasPrefix(p.Type.TypeName, "DOUBLE"):
				defaultClause = " DEFAULT 0"
			case p.Type.TypeName == "DATE":
				defaultClause = " DEFAULT '2000-01-01'"
			case p.Type.TypeName == "DATETIME":
				defaultClause = " DEFAULT '2000-01-01 00:00:00'"
			case p.Type.TypeName == "TIMESTAMP":
				defaultClause = " DEFAULT CURRENT_TIMESTAMP"
			}
		}
		sb.WriteString(fmt.Sprintf("  `%s` %s %s%s", colName, p.Type.TypeName, nullable, defaultClause))
		sb.WriteString(",\n")
	}

	sb.WriteString("  PRIMARY KEY (`pk`)")

	// Build column name → index key byte size map for auto-prefix calculation.
	// TiDB uses utf8mb4 (4 bytes/char). Max index key = 3072 bytes.
	const maxIndexKeyBytes = 3072
	colKeyBytes := make(map[string]int)
	colKeyBytes["pk"] = 8 // BIGINT = 8 bytes
	for i, p := range plan {
		name := fmt.Sprintf("c%d", i+1)
		colKeyBytes[name] = indexKeyBytes(p.Type.TypeName)
	}

	// Secondary indexes
	// Supports explicit prefix: "c4(100)" → KEY ... (`c4`(100))
	// Auto-adds prefix when index key would exceed 3072 bytes.
	for _, idxSpec := range cfg.Indexes {
		cols := strings.Split(idxSpec, ",")

		// Parse columns and any explicit prefix lengths.
		type idxCol struct {
			name      string
			prefixLen int // 0 = no explicit prefix
			keyBytes  int // actual key bytes (may be capped)
		}
		idxCols := make([]idxCol, len(cols))
		for i, c := range cols {
			c = strings.TrimSpace(c)
			name, plen := parseColPrefix(c)
			kb := colKeyBytes[name]
			if plen > 0 {
				kb = plen * 4 // explicit prefix in chars → bytes
			}
			idxCols[i] = idxCol{name: name, prefixLen: plen, keyBytes: kb}
		}

		// Check total key size; if over limit, auto-add prefix to string columns.
		totalBytes := 0
		for _, ic := range idxCols {
			totalBytes += ic.keyBytes
		}
		if totalBytes > maxIndexKeyBytes {
			// Shrink string columns (largest first) until under limit.
			for totalBytes > maxIndexKeyBytes {
				// Find the string column with the largest keyBytes and no explicit prefix.
				best := -1
				for i, ic := range idxCols {
					if ic.prefixLen > 0 {
						continue // user set explicit prefix, don't touch
					}
					typeName := ""
					if ic.name != "pk" {
						colIdx, _ := strconv.Atoi(ic.name[1:])
						if colIdx >= 1 && colIdx <= len(plan) {
							typeName = plan[colIdx-1].Type.TypeName
						}
					}
					if !strings.HasPrefix(typeName, "VARCHAR") && !strings.HasPrefix(typeName, "CHAR") {
						continue
					}
					if best < 0 || ic.keyBytes > idxCols[best].keyBytes {
						best = i
					}
				}
				if best < 0 {
					break // no string columns left to shrink
				}
				// Budget for this column: total budget minus all other columns.
				othersBytes := 0
				for i, ic := range idxCols {
					if i != best {
						othersBytes += ic.keyBytes
					}
				}
				budgetBytes := maxIndexKeyBytes - othersBytes
				if budgetBytes < 4 {
					budgetBytes = 4 // minimum 1 char
				}
				prefixChars := budgetBytes / 4
				idxCols[best].prefixLen = prefixChars
				idxCols[best].keyBytes = prefixChars * 4
				totalBytes = 0
				for _, ic := range idxCols {
					totalBytes += ic.keyBytes
				}
			}
			// Print info about auto-prefixed columns.
			for _, ic := range idxCols {
				if ic.prefixLen > 0 {
					// Check if it was auto-set (not in the original spec).
					origCol := ""
					for _, c := range cols {
						c = strings.TrimSpace(c)
						n, _ := parseColPrefix(c)
						if n == ic.name {
							origCol = c
							break
						}
					}
					if !strings.Contains(origCol, "(") {
						fmt.Fprintf(os.Stderr, "  Auto-prefix: index column %s → %s(%d) (key would exceed %d bytes)\n",
							ic.name, ic.name, ic.prefixLen, maxIndexKeyBytes)
					}
				}
			}
		}

		quotedCols := make([]string, len(idxCols))
		nameParts := make([]string, len(idxCols))
		for i, ic := range idxCols {
			if ic.prefixLen > 0 {
				quotedCols[i] = fmt.Sprintf("`%s`(%d)", ic.name, ic.prefixLen)
			} else {
				quotedCols[i] = fmt.Sprintf("`%s`", ic.name)
			}
			nameParts[i] = ic.name
		}
		idxName := "idx_" + strings.Join(nameParts, "_")
		sb.WriteString(fmt.Sprintf(",\n  KEY `%s` (%s)", idxName, strings.Join(quotedCols, ", ")))
	}

	sb.WriteString(fmt.Sprintf("\n) PARTITION BY HASH (`pk`) PARTITIONS %d", cfg.Partitions))
	return sb.String()
}

func bulkInsert(db *sql.DB, cfg *Config, plan []ColumnPlan, profile PartitionProfile) error {
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
	colNames := make([]string, 0, len(plan)+1)
	colNames = append(colNames, "`pk`")
	for i := range plan {
		colNames = append(colNames, fmt.Sprintf("`c%d`", i+1))
	}
	colList := strings.Join(colNames, ", ")

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
				pct := float64(n) / float64(cfg.Rows) * 100
				fmt.Fprintf(os.Stderr, "\r  Inserted %.1f%% %d/%d rows, %.0f rows/s   ",
					pct, n, cfg.Rows, rate)
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

			rng := rand.New(rand.NewSource(cfg.Seed + int64(partID)))

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

					for _, p := range plan {
						sb.WriteString(", ")

						// For nullable columns, occasionally insert NULL at the configured rate.
						if p.NullRate > 0 && rng.Float64() < p.NullRate {
							sb.WriteString("NULL")
						} else {
							v := dists[p.DistIdx](rng, seqIdx, rowsForPart, partID, cfg.Partitions)
							sb.WriteString(p.Type.MapFunc(v, rng))
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

	fmt.Fprintf(os.Stderr, "\r  Inserted 100.0%% %d/%d rows (done)        \n", totalInserted.Load(), cfg.Rows)
	return nil
}

// indexKeyBytes returns the index key size in bytes for a column type (utf8mb4).
func indexKeyBytes(typeName string) int {
	switch {
	case typeName == "INT", typeName == "INT UNSIGNED":
		return 4
	case typeName == "BIGINT", typeName == "BIGINT UNSIGNED":
		return 8
	case strings.HasPrefix(typeName, "CHAR"):
		// CHAR(N) → N * 4 bytes (utf8mb4)
		return extractTypeLen(typeName) * 4
	case strings.HasPrefix(typeName, "VARCHAR"):
		// VARCHAR(N) → N * 4 + 2 bytes length prefix
		return extractTypeLen(typeName)*4 + 2
	case strings.HasPrefix(typeName, "DECIMAL"):
		return 8 // conservative
	case typeName == "FLOAT":
		return 4
	case typeName == "DOUBLE":
		return 8
	case typeName == "DATE":
		return 3
	case typeName == "DATETIME":
		return 8
	case typeName == "TIMESTAMP":
		return 4
	default:
		return 8
	}
}

// extractTypeLen extracts N from type names like "VARCHAR(255)" or "CHAR(32)".
func extractTypeLen(typeName string) int {
	start := strings.IndexByte(typeName, '(')
	end := strings.IndexByte(typeName, ')')
	if start < 0 || end < 0 || end <= start {
		return 0
	}
	n, err := strconv.Atoi(typeName[start+1 : end])
	if err != nil {
		return 0
	}
	return n
}

// parseColPrefix splits "c4(100)" into ("c4", 100). Without prefix: ("c4", 0).
func parseColPrefix(s string) (string, int) {
	idx := strings.IndexByte(s, '(')
	if idx < 0 {
		return s, 0
	}
	colName := s[:idx]
	lenStr := strings.TrimSuffix(s[idx+1:], ")")
	n, err := strconv.Atoi(lenStr)
	if err != nil || n <= 0 {
		return colName, 0
	}
	return colName, n
}

// printColumnLayout prints the column name → type + distribution mapping.
func printColumnLayout(_ *Config, plan []ColumnPlan) {
	dists := distributionNames()

	fmt.Fprintf(os.Stderr, "\nColumn layout:\n")
	fmt.Fprintf(os.Stderr, "  %-8s %-16s %-12s %s\n", "Column", "Type", "Null", "Distribution")
	fmt.Fprintf(os.Stderr, "  %-8s %-16s %-12s %s\n", "------", "----", "----", "------------")
	for i, p := range plan {
		colName := fmt.Sprintf("c%d", i+1)
		var nullable string
		switch {
		case !p.Nullable:
			nullable = "NO"
		case p.NullRate == defaultNullRate:
			nullable = "YES"
		default:
			nullable = fmt.Sprintf("YES(%.1f%%)", p.NullRate*100)
		}
		distName := dists[p.DistIdx]
		fmt.Fprintf(os.Stderr, "  %-8s %-16s %-12s %s\n", colName, p.Type.TypeName, nullable, distName)
	}
	fmt.Fprintln(os.Stderr)
}
