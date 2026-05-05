package main

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

// DistFunc generates a float64 in [0.0, 1.0) given the random source and
// contextual information about the current row and partition.
type DistFunc func(rng *rand.Rand, seqIndex, totalInPart, partitionID, numParts int) float64

// TypeMapper converts a [0,1) value to a SQL literal string for a specific column type.
type TypeMapper struct {
	TypeName string
	MapFunc  func(v float64, rng *rand.Rand) string
}

// distributions returns the 8 built-in distribution functions.
func distributions() []DistFunc {
	// Low-NDV: 100 pre-generated bucket values, shared across all calls.
	lowNDVBuckets := make([]float64, 100)
	bucketRng := rand.New(rand.NewSource(42))
	for i := range lowNDVBuckets {
		lowNDVBuckets[i] = bucketRng.Float64()
	}

	return []DistFunc{
		// 0: Uniform
		func(rng *rand.Rand, _, _, _, _ int) float64 {
			return rng.Float64()
		},
		// 1: Zipf — s=1.5, v=1, imax=10000, normalized to [0,1)
		func(rng *rand.Rand, _, _, _, _ int) float64 {
			z := rand.NewZipf(rng, 1.5, 1, 10000)
			return float64(z.Uint64()) / 10001.0
		},
		// 2: Normal — mean 0.5, stddev 0.15, clamped to [0,1)
		func(rng *rand.Rand, _, _, _, _ int) float64 {
			v := rng.NormFloat64()*0.15 + 0.5
			if v < 0 {
				v = 0
			}
			if v >= 1 {
				v = 1 - 1e-10
			}
			return v
		},
		// 3: Low-NDV — pick uniformly from 100 bucket values
		func(rng *rand.Rand, _, _, _, _ int) float64 {
			return lowNDVBuckets[rng.Intn(len(lowNDVBuckets))]
		},
		// 4: Sequential — linear ramp within the partition
		func(_ *rand.Rand, seqIndex, totalInPart, _, _ int) float64 {
			if totalInPart <= 1 {
				return 0.5
			}
			return float64(seqIndex) / float64(totalInPart)
		},
		// 5: Per-partition range — partition P maps to [P/N, (P+1)/N), uniform within
		func(rng *rand.Rand, _, _, partitionID, numParts int) float64 {
			lo := float64(partitionID) / float64(numParts)
			hi := float64(partitionID+1) / float64(numParts)
			return lo + rng.Float64()*(hi-lo)
		},
		// 6: Growth — x^0.3 where x is uniform, skews toward 1.0
		func(rng *rand.Rand, _, _, _, _ int) float64 {
			return math.Pow(rng.Float64(), 0.3)
		},
		// 7: Per-partition categorical — each partition gets a unique narrow band
		func(rng *rand.Rand, _, _, partitionID, numParts int) float64 {
			lo := float64(partitionID) / float64(numParts)
			hi := float64(partitionID+1) / float64(numParts)
			// Narrow the band to 80% of the slot to ensure zero overlap
			width := (hi - lo) * 0.8
			return lo + rng.Float64()*width
		},
	}
}

// typeMappers returns the 10 type mappers that convert [0,1) to SQL literals.
// maxStringLen controls the maximum length of generated VARCHAR values.
// If maxStringLen > 255, the VARCHAR column type is widened accordingly.
func typeMappers(maxStringLen int) []TypeMapper {
	if maxStringLen <= 0 {
		maxStringLen = 60
	}
	varcharType := "VARCHAR(255)"
	if maxStringLen > 255 {
		varcharType = fmt.Sprintf("VARCHAR(%d)", maxStringLen)
	}
	// Minimum string length is 10 or maxStringLen (whichever is smaller)
	minLen := 10
	if minLen > maxStringLen {
		minLen = maxStringLen
	}
	// The range for Intn: generate lengths from minLen to maxStringLen inclusive
	lenRange := maxStringLen - minLen + 1

	dateStart := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	dateEnd := time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)
	dateDays := dateEnd.Sub(dateStart).Hours() / 24
	dateSeconds := dateEnd.Sub(dateStart).Seconds()

	tsStart := time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC)
	tsEnd := time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)
	tsSeconds := tsEnd.Sub(tsStart).Seconds()

	return []TypeMapper{
		{"INT", func(v float64, _ *rand.Rand) string {
			return fmt.Sprintf("%d", int64(v*float64(math.MaxInt32)))
		}},
		{"BIGINT", func(v float64, _ *rand.Rand) string {
			// 2^53 for safe integer range
			return fmt.Sprintf("%d", int64(v*float64(int64(1)<<53)))
		}},
		{"CHAR(32)", func(v float64, _ *rand.Rand) string {
			// Seed a local RNG from v so same v always produces same string
			localRng := rand.New(rand.NewSource(int64(v * float64(int64(1)<<53))))
			return fmt.Sprintf("'%s'", randString(localRng, 32))
		}},
		{varcharType, func(v float64, _ *rand.Rand) string {
			localRng := rand.New(rand.NewSource(int64(v * float64(int64(1)<<53))))
			length := minLen + localRng.Intn(lenRange)
			return fmt.Sprintf("'%s'", randString(localRng, length))
		}},
		{"DECIMAL(10,2)", func(v float64, _ *rand.Rand) string {
			return fmt.Sprintf("%.2f", v*100000)
		}},
		{"FLOAT", func(v float64, _ *rand.Rand) string {
			return fmt.Sprintf("%f", v*1000)
		}},
		{"DOUBLE", func(v float64, _ *rand.Rand) string {
			return fmt.Sprintf("%f", v*100000)
		}},
		{"DATE", func(v float64, _ *rand.Rand) string {
			d := dateStart.Add(time.Duration(v*dateDays*24) * time.Hour)
			return fmt.Sprintf("'%s'", d.Format("2006-01-02"))
		}},
		{"DATETIME", func(v float64, _ *rand.Rand) string {
			d := dateStart.Add(time.Duration(v*dateSeconds) * time.Second)
			return fmt.Sprintf("'%s'", d.Format("2006-01-02 15:04:05"))
		}},
		{"TIMESTAMP", func(v float64, _ *rand.Rand) string {
			d := tsStart.Add(time.Duration(v*tsSeconds) * time.Second)
			return fmt.Sprintf("'%s'", d.Format("2006-01-02 15:04:05"))
		}},
	}
}

// intTypeMappers returns only the INT and BIGINT type mappers.
func intTypeMappers() []TypeMapper {
	all := typeMappers(60) // string length irrelevant for int-only
	return all[:2]
}

// isStringType returns true if the type mapper produces string values.
func isStringType(tm TypeMapper) bool {
	return strings.HasPrefix(tm.TypeName, "CHAR") || strings.HasPrefix(tm.TypeName, "VARCHAR")
}

// distributionNames returns human-readable names for the 8 built-in distributions.
func distributionNames() []string {
	return []string{
		"uniform",
		"zipf",
		"normal",
		"low-ndv",
		"sequential",
		"per-part-range",
		"growth",
		"per-part-categ",
	}
}

// isSequentialDist returns true for the Sequential distribution (index 4).
func isSequentialDist(distIdx int) bool {
	return distIdx == 4
}

// defaultNullRate is the fraction of inserted values that are NULL for a
// nullable column when the spec does not pin the rate. Matches the
// pre-existing 1-in-20 cyclic-default behavior.
const defaultNullRate = 0.05

// ColumnPlan describes the type, distribution, and nullability for one
// generated column.
//
// Nullable controls the DDL constraint (NULL vs NOT NULL).
// NullRate is the probability (0.0–1.0) of inserting a NULL value during
// data generation; it is forced to 0 when Nullable is false.
type ColumnPlan struct {
	Type     TypeMapper
	DistIdx  int // index into distributions() / distributionNames()
	Nullable bool
	NullRate float64
}

// columnPlan returns one ColumnPlan per generated column.
// If cfg.ColumnSpec is non-empty it is parsed (TYPE:DIST[:NULL[(rate)]|NOTNULL],...).
// Otherwise the cyclic default applies (cfg.Columns entries cycling
// types and distributions independently, alternating NULL/NOT NULL every
// len(types) columns).
func columnPlan(cfg *Config) ([]ColumnPlan, error) {
	tms := typeMappers(cfg.MaxStringLength)
	if cfg.ColumnSpec == "" && cfg.ColumnTypes == "int" {
		tms = intTypeMappers()
	}
	distNames := distributionNames()

	if cfg.ColumnSpec != "" {
		return parseColumnSpec(cfg.ColumnSpec, tms, distNames)
	}

	out := make([]ColumnPlan, cfg.Columns)
	for i := 0; i < cfg.Columns; i++ {
		tm := tms[i%len(tms)]
		d := i % len(distNames)
		if isSequentialDist(d) && isStringType(tm) {
			d = 0
		}
		nullable := (i/len(tms))%2 == 0
		rate := 0.0
		if nullable {
			rate = defaultNullRate
		}
		out[i] = ColumnPlan{Type: tm, DistIdx: d, Nullable: nullable, NullRate: rate}
	}
	return out, nil
}

// parseColumnSpec parses a "TYPE:DIST[:NULL[(rate)]|NOTNULL],..." spec string.
// Type names are case-insensitive; bare "CHAR" matches "CHAR(32)",
// "VARCHAR" matches "VARCHAR(255)", "DECIMAL" matches "DECIMAL(10,2)".
// Distribution names match distributionNames() exactly.
// Sequential distribution + string type silently falls back to uniform.
//
// Nullability token (case-insensitive, optional, default "NULL"):
//
//	NOTNULL       NOT NULL column, no NULL values inserted
//	NULL          nullable, default null rate (5%)
//	NULL(f)       nullable, f% of values inserted as NULL (0.0–100.0)
func parseColumnSpec(spec string, tms []TypeMapper, distNames []string) ([]ColumnPlan, error) {
	entries := strings.Split(spec, ",")
	out := make([]ColumnPlan, 0, len(entries))
	for i, e := range entries {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		parts := strings.Split(e, ":")
		if len(parts) < 2 || len(parts) > 3 {
			return nil, fmt.Errorf("entry %d (%q): expected TYPE:DIST[:NULL[(rate)]|NOTNULL]", i+1, e)
		}
		tm, err := lookupColumnType(tms, strings.TrimSpace(parts[0]))
		if err != nil {
			return nil, fmt.Errorf("entry %d: %w", i+1, err)
		}
		distIdx, err := lookupDistribution(distNames, strings.TrimSpace(parts[1]))
		if err != nil {
			return nil, fmt.Errorf("entry %d: %w", i+1, err)
		}
		if isSequentialDist(distIdx) && isStringType(tm) {
			distIdx = 0
		}
		nullable := true
		rate := defaultNullRate
		if len(parts) == 3 {
			nullable, rate, err = parseNullability(strings.TrimSpace(parts[2]))
			if err != nil {
				return nil, fmt.Errorf("entry %d: %w", i+1, err)
			}
		}
		out = append(out, ColumnPlan{Type: tm, DistIdx: distIdx, Nullable: nullable, NullRate: rate})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no entries parsed from %q", spec)
	}
	return out, nil
}

// parseNullability parses the nullability token. Accepts (case-insensitive):
//
//	NOTNULL                → (false, 0)
//	NULL                   → (true, defaultNullRate)
//	NULL(f) where 0<=f<=100 → (true, f/100)
func parseNullability(tok string) (bool, float64, error) {
	upper := strings.ToUpper(tok)
	if upper == "NOTNULL" || upper == "NOT_NULL" {
		return false, 0, nil
	}
	if upper == "NULL" {
		return true, defaultNullRate, nil
	}
	if strings.HasPrefix(upper, "NULL(") && strings.HasSuffix(upper, ")") {
		inner := upper[len("NULL(") : len(upper)-1]
		f, err := strconv.ParseFloat(inner, 64)
		if err != nil {
			return false, 0, fmt.Errorf("null rate %q: %w", inner, err)
		}
		if f < 0 || f > 100 {
			return false, 0, fmt.Errorf("null rate %g out of range (0.0–100.0)", f)
		}
		return true, f / 100.0, nil
	}
	return false, 0, fmt.Errorf("nullability token %q: expected NULL, NULL(rate), or NOTNULL", tok)
}

func lookupColumnType(tms []TypeMapper, name string) (TypeMapper, error) {
	upper := strings.ToUpper(name)
	for _, tm := range tms {
		u := strings.ToUpper(tm.TypeName)
		if u == upper {
			return tm, nil
		}
		if base, _, ok := strings.Cut(u, "("); ok && base == upper {
			return tm, nil
		}
	}
	avail := make([]string, len(tms))
	for i, tm := range tms {
		avail[i] = tm.TypeName
	}
	return TypeMapper{}, fmt.Errorf("unknown type %q (available: %s)", name, strings.Join(avail, ", "))
}

func lookupDistribution(names []string, name string) (int, error) {
	lower := strings.ToLower(name)
	for i, n := range names {
		if strings.ToLower(n) == lower {
			return i, nil
		}
	}
	return -1, fmt.Errorf("unknown distribution %q (available: %s)", name, strings.Join(names, ", "))
}

// PartitionProfile controls the row distribution across partitions.
type PartitionProfile int

const (
	ProfileUniform   PartitionProfile = iota
	ProfileRangeLike                  // first 2 + last 2 empty, middle grows quadratically
	ProfileSizeSkew                   // p0 gets 80%, rest share 20%
)

// ParsePartitionProfile converts a string flag value to PartitionProfile.
func ParsePartitionProfile(s string) (PartitionProfile, error) {
	switch s {
	case "uniform":
		return ProfileUniform, nil
	case "range-like":
		return ProfileRangeLike, nil
	case "size-skew":
		return ProfileSizeSkew, nil
	default:
		return ProfileUniform, fmt.Errorf("unknown partition profile %q (valid: uniform, range-like, size-skew)", s)
	}
}

// partitionWeights returns a weight slice summing to ~1.0 for distributing rows.
func partitionWeights(profile PartitionProfile, numPartitions int) []float64 {
	weights := make([]float64, numPartitions)

	switch profile {
	case ProfileRangeLike:
		if numPartitions <= 4 {
			// Not enough partitions for range-like, fall back to uniform
			for i := range weights {
				weights[i] = 1.0 / float64(numPartitions)
			}
			return weights
		}
		// First 2 and last 2 get weight 0
		total := 0.0
		for i := 2; i < numPartitions-2; i++ {
			w := float64((i-1)*(i-1)) + 1
			weights[i] = w
			total += w
		}
		// Normalize
		for i := range weights {
			weights[i] /= total
		}

	case ProfileSizeSkew:
		weights[0] = 0.80
		if numPartitions > 1 {
			share := 0.20 / float64(numPartitions-1)
			for i := 1; i < numPartitions; i++ {
				weights[i] = share
			}
		}

	default: // ProfileUniform
		for i := range weights {
			weights[i] = 1.0 / float64(numPartitions)
		}
	}

	return weights
}

// pkForPartition generates a primary key that lands in the given partition
// for HASH(pk) PARTITIONS N. seqIdx is 1-based within the partition.
func pkForPartition(seqIdx, partitionID, numPartitions int) int64 {
	return int64(seqIdx)*int64(numPartitions) + int64(partitionID)
}
