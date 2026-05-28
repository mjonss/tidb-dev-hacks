# Singleton TopN Filter — Design Discussion

Companion to `FINDINGS-v3-topn-divergence.md`. Records the design
iterations of a filter that drops `count == 1` ("singleton") entries
from the global TopN built by `MergePartTopNAndHistToGlobal`,
the test fallout each iteration produced, and the open question.

## Motivation: Pattern 1 from the TopN-divergence study

`FINDINGS-v3-topn-divergence.md` documented three patterns in how the
combined merge produced TopN content that differed from BASE. Pattern
1 was the easiest one to motivate cleaning up:

> Uniform high-NDV columns (`c3` BIGINT UNSIGNED uniform NOTNULL, `c7`
> DOUBLE uniform, clustered `pk`) ended up with 100 `count=1` TopN
> entries on the PR side and zero on BASE. Per-partition
> `pruneTopNItem` prunes singletons in BASE, but PR's global combined
> merge re-introduces them via the bucket-Repeat path. 100 random
> singletons drawn from ~30 M distinct values carry no selectivity
> signal beyond what the histogram + NDV fallback already provides.

So the original intent was: at the global-merge step, do not admit a
candidate whose accumulated count is 1 into the global TopN, because

1. it provides no information the histogram doesn't already provide,
2. it consumes a TopN slot that could have held a signal-bearing value,
3. on a large-NDV column the specific singleton that won the heap
   race is arbitrary and contributes to plan instability.

That logic is locally correct on the bench's 30 M-row uniform data.
The rest of this document is the story of how it interacts with the
existing test suite and with one important invariant we eventually
recognized.

## Iteration 1: `>= 2` at heap admission

Initial implementation in `MergePartTopNAndHistToGlobal`:

```go
if entry.totalRepeat >= 2 {
    topNHeap.Add(entry)
}
```

This sat directly at the heap-push site in step 1c. A new test
(`TestMergePartTopNAndHistToGlobalSingletonFilter`) asserted the
post-merge invariant `m.Count >= 2` for every TopN entry.

**Behavior on the bench's Pattern 1**: 100 noise singletons no longer
admitted. Global TopN for `c3`/`c7`/`pk` becomes empty. Matches BASE.

**Fallout in the existing test suite**: several small-table
integration tests started failing — `TestGlobalStats`,
`TestGlobalIndexStatistics`, `TestGlobalStatsMergeCombined`. The
pattern was the same in every case:

- 5–7 row table, every value unique (`NDV ≈ row_count`).
- Per-partition `pruneTopNItem` prunes singletons.
- Global merge candidates: a small number of bucket-Repeat refs, each
  with totalRepeat=1.
- Filter drops them all → empty global TopN.

The tests asserted on `show stats_buckets` output for which the row
that *was* in TopN had been excluded from the histogram (Repeat=0 on
TopN-matched bucket upper). With an empty TopN, that row stays in
the bucket and the cumulative count is off by 1.

For example `TestGlobalStats` (7-row table, default `numTopN=20`):

| | Pre-filter | Post-filter |
|---|---|---|
| Estimated rows for `a > 5` | 4.00 | 5.00 |
| Bucket layout vs expectation | match | off by 1 |

So an unconditional `>= 2` was too aggressive on small data. The
filter was correct in spirit on Pattern 1 but had no way to tell
"100 noise singletons in a 30 M-row sea" from "5 unique values
making up the whole table."

## Iteration 2: conditional gate "heap was at capacity"

Move the filter from heap-admission (step 1c) to post-extraction
(step 1d), and gate it on whether the heap actually had to evict —
i.e., whether the candidate pool was larger than the heap's capacity:

```go
topNSlice := topNHeap.ToSortedSlice()
if uint32(len(topNSlice)) >= numTopN {
    // heap filled to capacity ⇒ candidates competed for slots ⇒
    // singletons that survived the race are arbitrary noise; drop.
    filtered := topNSlice[:0]
    for _, e := range topNSlice {
        if e.totalRepeat >= 2 {
            filtered = append(filtered, e)
        }
    }
    topNSlice = filtered
}
```

The motivating distinction: when the heap fills, the entries in it
are a *sample* of a larger pool, so singletons are noise. When the
heap doesn't fill, every distinct candidate fit and singletons
represent the *complete* distinct-value set — keeping them gives the
optimizer exact range information.

**Result**: `TestGlobalStats` and `TestGlobalIndexStatistics` go back
to passing because their `numTopN=20` is larger than the table's
NDV=6 — the heap doesn't fill, so the filter doesn't trigger.

**Remaining failure**: `TestGlobalStatsMergeCombined`. That test
does `analyze table t with 1 topn` on a 100 K-row table. Per-partition
TopN slot = 1 entry (the partition's most-frequent value or arbitrary
count=1). Global merge collects 7 partition-TopN candidates +
~700 bucket-Repeat candidates. Heap fills to `numTopN=1` ⇒ filter
triggers ⇒ empty TopN for column `a` and `idx_ab`.

The test author wrote the expected output to include
`"test t global a 0 1 1"` (one count=1 entry). With the filter
this becomes 0 entries. I updated the test expectations to drop
those two lines, with a comment explaining the new semantics. At
that point the merge-tests + globalstats package were green.

This was the state that got force-pushed.

## Fallout discovered after force-push

The user prompted to check
`TestAnalyzeWithDynamicPartitionPruneMode`. It failed. Then
`TestInitStatsForPartitionedTable` — also failed.

Triaging across the stats-handle test packages turned up six
failing tests in total. After bisecting each against the
pre-filter PR HEAD (origin's `63d8b20af9` before my force-push)
and against the PR base (master `e53c50c1c5`):

| Test | Master (V1) | Pre-filter PR | My HEAD | Cause |
|---|---|---|---|---|
| `TestAnalyzeWithDynamicPartitionPruneMode` | ✅ | ✅ | ❌ | **filter** |
| `TestAnalyzeGlobalStatsWithOpts1`           | ✅ | ✅ | ❌ | **filter** |
| `TestInitStatsForPartitionedTable`          | ✅ | ✅ | ❌ | **filter** |
| `TestInitStatsMemoryFullBlocksBucketsButKeepsTopN` | passes | ❌ | ❌ | pre-existing |
| `TestIncrementalModifyCountUpdate`          | passes | ❌ | ❌ | pre-existing |
| `TestStatsCacheUpdateTimeout`               | passes | ❌ | ❌ | pre-existing |
| `TestExpBackoffEstimation`                  | passes | ❌ | ❌ | pre-existing |

Three real regressions caused by my filter:

1. `TestAnalyzeWithDynamicPartitionPruneMode`. 5 rows
   `(1), (2), (3), (10), (11)`, two range partitions, `with 1 topn,
   2 buckets`. Expected bucket count was 4 (one row absorbed by
   TopN). With the filter the global TopN is empty so the row stays
   in the bucket → count=5.

2. `TestAnalyzeGlobalStatsWithOpts1`. Same shape, different table.

3. `TestInitStatsForPartitionedTable`. 6 unique-value rows, `with
   2 topn, 2 buckets`. Expected global TopN to contain 2 entries
   for the indexes. With the filter the heap fills to 2 with two
   count=1 candidates ⇒ filter triggers ⇒ empty TopN.

All three share the shape

- tiny table (5–7 rows),
- every value unique (NDV ≈ row_count),
- small explicit `numTopN` (1 or 2),
- heap fills to capacity with count=1 candidates,
- pre-filter PR (and master) keep those singletons in global TopN.

The conditional gate `len(topNSlice) >= numTopN` triggers on these
cases too, because `numTopN` is small and the candidate pool, while
also small, exceeds it.

## The constraint we missed: partitioned vs non-partitioned equivalence

The user's framing of the regression made the underlying invariant
crisp:

> Ideally the stats should look the same regardless if the table is
> partitioned or not.

Verified empirically against my filter (current HEAD):

```sql
-- Non-partitioned t_np (5 rows, all unique, with 1 topn, 2 buckets)
SHOW STATS_TOPN WHERE table_name='t_np' AND is_index=1;
-- [test t_np  a 1 1 1]   ← one TopN entry, value 1, count 1

-- Partitioned t_p, same data, same options, global stats
SHOW STATS_TOPN WHERE table_name='t_p' AND partition_name='global'
                  AND is_index=1;
-- (empty)
```

And at `numTopN=2` (6 unique values):

| | Non-partitioned `t_np` | Partitioned `t_p` global |
|---|---|---|
| `idx(b)` TopN | `[1: count=1, 2: count=1]` | empty |
| column `b` TopN | `[1: count=1, 2: count=1]` | empty |

The non-partitioned path goes through `pruneTopNItem` once at the
table level. For tiny tables it keeps singletons (the empirical
"reality" above) — presumably because with no sampling
(`sampleRows == totalRows`) the hypergeometric variance term is
zero and the threshold ends up below the count, or because of an
early-exit branch I haven't fully traced.

Either way, the empirical contract is "tiny tables keep
singletons." My global filter breaks that contract on the
partitioned side. This is a stronger constraint than any individual
test expectation: it says the global merge has to produce the
*same* TopN content as the equivalent non-partitioned analyze,
regardless of plan implementation details.

## What none of the candidate filters get right

For completeness, none of the discriminators considered so far
satisfy *all* of:

- Drop Pattern 1's 100 noise singletons on the bench.
- Preserve the small-table behavior the existing tests assert.
- Preserve the partitioned ≡ non-partitioned invariant.

| Discriminator | Pattern 1 fixed? | Small-table tests? | Equivalence? |
|---|---|---|---|
| Unconditional `>= 2` at heap.Add | ✓ | ✗ (≥ 6 fail) | ✗ |
| Conditional `len(topNSlice) >= numTopN` | ✓ | partial — `TestGlobalStats` passes, three others fail | ✗ |
| Port `pruneTopNItem` (Wald CI) at global | ✓ | ✗ — Wald threshold also prunes singletons on 5-row tables (`variance=0` ⇒ threshold ≈ 1.5 ⇒ count=1 fails) | ✗ |
| `candidateCount >= max(10 * numTopN, 100)` | ✓ | likely passes (candidates < 100 in failing tests) | needs verification — depends on whether the non-partitioned `pruneTopNItem` path ever crosses the same threshold |
| No filter (revert) | ✗ (100 noise singletons stay) | ✓ | ✓ |

The current code on the branch corresponds to row 2 — `Pattern 1
fixed, equivalence broken`.

## Resolution: mirror the per-partition `allowPruning` trigger

Re-reading `pkg/statistics/builder.go` around the
`processTopNValue` / `pruneTopNItem` call site surfaced the rule
the per-partition path already encodes:

```go
// pkg/statistics/builder.go (constants.go: DefaultTopNValue = 100)
allowPruning := true
if numTopN != DefaultTopNValue {
    allowPruning = false
}
...
if allowPruning {
    prunedTopNItems = pruneTopNItem(sortedTopNItems, ndv, nullCount, sampleNum, count)
}
```

Per-partition pruning is gated on `numTopN == DefaultTopNValue`.
Any `with N topn` clause from the user is treated as "honor what I
asked for"; `pruneTopNItem` is skipped entirely. That is exactly
why the empirical `t_np` (non-partitioned) test in the previous
section kept its singletons — `with 1 topn` and `with 2 topn` both
disable pruning.

Adding the same gate to the global filter restores the
partitioned ≡ non-partitioned invariant by construction. The
filter in `MergePartTopNAndHistToGlobal` step 1d becomes:

```go
// Drop singleton (count==1) candidates when both gates hold:
//   (a) numTopN is the default — same trigger the per-partition
//       builder uses to enable pruneTopNItem. An explicit
//       `with N topn` override turns off per-partition pruning,
//       so the global merge must leave its output alone to
//       preserve the partitioned ≡ non-partitioned invariant.
//   (b) the heap filled to capacity — singletons are then a
//       sample of a candidate pool larger than numTopN slots,
//       carrying no selectivity signal beyond the histogram +
//       NDV fallback. When the heap stays below capacity every
//       distinct candidate fit; keep them.
if numTopN == DefaultTopNValue && uint32(len(topNSlice)) >= numTopN {
    filtered := topNSlice[:0]
    for _, e := range topNSlice {
        if e.totalRepeat >= 2 {
            filtered = append(filtered, e)
        }
    }
    topNSlice = filtered
}
```

Crossing the gate against every scenario considered earlier:

| Scenario | `numTopN` | `numTopN==Default`? | Heap full? | Filter? | Equivalence? |
|---|---|---|---|---|---|
| Pattern 1 bench (`c3`, default analyze) | 100 | ✓ | ✓ (100 of ~30M) | apply | ✓ |
| `TestAnalyzeWithDynamicPartitionPruneMode` (`with 1 topn`) | 1 | ✗ | — | skip | ✓ matches `t_np` |
| `TestAnalyzeGlobalStatsWithOpts1` (`with 1 topn`) | 1 | ✗ | — | skip | ✓ |
| `TestInitStatsForPartitionedTable` (`with 2 topn`) | 2 | ✗ | — | skip | ✓ |
| `TestGlobalStatsMergeCombined` (`with 1 topn`) | 1 | ✗ | — | skip | ✓ original expectations restored |
| `TestGlobalStats` (default 20) | 20 | ✗ | — | skip | ✓ |
| Tiny table, default 100, 5 unique values | 100 | ✓ | ✗ (5 < 100) | skip | ✓ singletons kept on both sides |
| Default 100, ~200 unique values | 100 | ✓ | ✓ | apply | ✓ noise dropped |

### Result of the implementation

After installing the `numTopN == DefaultTopNValue` gate and
updating the unit test
(`TestMergePartTopNAndHistToGlobalSingletonFilter` now has three
subtests, covering each branch of the gate):

| Test | State |
|---|---|
| `TestAnalyzeWithDynamicPartitionPruneMode` | ✅ now passes |
| `TestAnalyzeGlobalStatsWithOpts1` | ✅ now passes |
| `TestInitStatsForPartitionedTable` | ✅ now passes |
| `TestGlobalStatsMergeCombined` (expectations reverted to pre-filter) | ✅ now passes |
| `TestMergePartTopNAndHistToGlobalSingletonFilter` (3 subtests) | ✅ passes |
| All other merge / globalstats tests | ✅ pass |
| `TestInitStatsMemoryFullBlocksBucketsButKeepsTopN` | ❌ pre-existing, unrelated |
| `TestIncrementalModifyCountUpdate` | ❌ pre-existing, unrelated |
| `TestStatsCacheUpdateTimeout` | ❌ pre-existing, unrelated |
| `TestExpBackoffEstimation` | ❌ pre-existing, unrelated |

The four remaining failures all fail on the pre-filter PR
(`63d8b20af9`) too, confirmed by bisecting. They are independent
of the singleton filter.

### Why the rule generalizes

The `DefaultTopNValue` gate is not a magic threshold; it is a
contract: *if the user wrote `with N topn`, neither the
per-partition nor the global pruner is allowed to drop their
entries*. The per-partition side already implements that contract.
Mirroring it at the global level makes the merge's output
indistinguishable from the equivalent non-partitioned analyze for
all explicit-`numTopN` workloads, by construction, and lets
Pattern 1 (default `numTopN`, large NDV) still be cleaned up.

The heap-at-capacity sub-gate (`len(topNSlice) >= numTopN`) is the
additional, internal-to-the-merge discriminator that separates
"sample of large pool" from "small enumeration that happens to fit
the default slot count." Together the two gates pass every
scenario considered above.
