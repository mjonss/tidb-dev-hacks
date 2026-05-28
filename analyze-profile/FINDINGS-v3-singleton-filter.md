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

## Where this leaves us

Two paths forward, both consistent with the rest of the
findings:

**Path A — revert the filter.** Restore the pre-filter `>= 0`
admission at heap.Add, drop the post-heap filter and the
`TestMergePartTopNAndHistToGlobalSingletonFilter` test, and revert
the `TestGlobalStatsMergeCombined` expectation changes to their
pre-filter values. Pattern 1's 100 noise singletons return as a
documented but-not-acted-on observation in
`FINDINGS-v3-topn-divergence.md`. The partitioned ≡
non-partitioned invariant is preserved by default.

**Path B — invest in a smarter discriminator.** Probably `candidateCount`-
gated: track the number of distinct candidates seen during the
merge walk and only apply the singleton filter when the pool is
much larger than `numTopN`. Worth doing only if we can prove that
on every input shape, the global filter's decision matches the
per-partition `pruneTopNItem`'s decision on the equivalent
non-partitioned input. That probably means actually porting the
statistical test from `pruneTopNItem` and giving it the right
"sample size" interpretation for global counts (a non-trivial
modelling question, since the global count is a sum of per-
partition observations, not a single hypergeometric draw).

The remaining engineering risk is that Path B's smarter
discriminator inherits the partitioned vs non-partitioned constraint
as a hard requirement: any filter that says "I keep this entry on a
non-partitioned 5-row table" must also say so on the partitioned
equivalent. Encoding that property in the global merge probably
needs more state than the current code carries, because the
per-partition pruner decides per partition and the global side
currently only sees the survivors.

The decision between Path A and Path B is the next conversation.
