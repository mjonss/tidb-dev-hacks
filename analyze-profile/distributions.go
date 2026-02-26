package main

import (
	"fmt"
	"math"
	"math/rand"
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
func typeMappers() []TypeMapper {
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
		{"VARCHAR(255)", func(v float64, _ *rand.Rand) string {
			localRng := rand.New(rand.NewSource(int64(v * float64(int64(1)<<53))))
			length := 10 + localRng.Intn(50)
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

// isStringType returns true if the type mapper produces string values.
func isStringType(tm TypeMapper) bool {
	return tm.TypeName == "CHAR(32)" || tm.TypeName == "VARCHAR(255)"
}

// isSequentialDist returns true for the Sequential distribution (index 4).
func isSequentialDist(distIdx int) bool {
	return distIdx == 4
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
