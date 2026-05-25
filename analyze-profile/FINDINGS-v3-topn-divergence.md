# TopN divergence between PR and BASE (orthogonal to the histogram fix)

Companion to `FINDINGS-v3.md`. The histogram-bucket regression on per-
partition-categorical columns is fully resolved by the chunking fix
upstreamed into `origin/improve-global-stats-3` at `643d74138b`. This
file documents a separate phenomenon visible in the same post-fix
report: the contents of the **global TopN** still differ
substantially between PR and BASE on some columns.

All data here is from
`output-bench-v3/report.txt` (post-fix, 2026-05-25) and the
corresponding `stats_dump.json` files under
`output-bench-v3/{PR,BASE}/part-full_existing_asyncON_iter0/run_*/`.

## Headline

There are three distinct divergence patterns, of which one is benign
and two could matter for the optimizer.

| Pattern | Severity | Example columns | What's happening |
|---|---|---|---|
| 1. PR fills empty BASE TopN | low, wasteful | `c3`, `c7`, `pk` | PR puts singleton (count=1) values in TopN, BASE leaves TopN empty |
| 2. Same column, very different counts | **possibly important** | `c11`, `c15`, `c16`, `c19` | PR and BASE produce TopN sets with mostly different values; on overlapping entries the counts differ by ~100× to ~1000× |
| 3. Mostly-overlapping with small fringe | benign | `c12`, `c20` | 86–88 of 100 TopN entries match exactly with identical counts; small disagreement at the top-N boundary |

## Pattern 1 — PR adds singletons to TopN, BASE does not

For uniform / high-NDV columns where every value is essentially unique:

| Column | Type / distribution | NDV | Non-null rows | PR TopN | BASE TopN |
|---|---|---|---|---|---|
| `c3`  | BIGINT UNSIGNED, uniform NOTNULL | 29.54M | 30.0M | **100 entries, all count=1** | **empty (0 entries)** |
| `c7`  | DOUBLE, uniform                  | 27.99M | 28.5M | 100 entries, all count=1     | empty |
| `pk`  | BIGINT, uniform (clustered PK)    | 30.00M | 30.0M | 37 entries, all count=1       | empty |

For a value to be useful as a TopN entry it must occur disproportionately
often relative to other values; a value with `count=1` carries no
selectivity information that the histogram doesn't already give. PR
admits these into global TopN anyway; BASE rejects them.

This is wasteful but not incorrect — neither side will mislead the
optimizer because the entries carry no information either way. Worth
noting because it costs ~100 TopN slots per such column for no
benefit.

## Pattern 2 — same column, counts differ by ~100× to ~1000×

This is the interesting case. `c11` (DECIMAL uniform) is the
clearest illustration:

```
c11: NDV = 9,553,920   non-null rows = 28,500,374
avg occurrences per distinct value ≈ 28.5M / 9.55M ≈ 3
```

So a value that appears 5–10 times is in the Poisson tail of "most
frequent for a uniform distribution." Counts in the thousands would
imply the data is not uniform at all.

What the two algorithms produce:

|              | PR TopN[0..4]            | BASE TopN[0..4] |
|---|---|---|
| Size         | 100 entries              | 100 entries |
| **Count range** | **5 – 9**              | **7,478 – 7,969** |
| First few counts | 5, 5, 5, 5, 6        | 7583, 7737, 7904, 7943, 7947 |
| Encoded prefixes | `06 0a 02 80 00 1d…` | `06 0a 02 80 00 00…` (smaller decimal values) |

The same pattern repeats on:

| Column | Type / distribution | PR count range | BASE count range |
|---|---|---|---|
| `c15` | VARCHAR uniform NOTNULL | small (units) | thousands |
| `c16` | VARCHAR per-part-categ  | small         | thousands |
| `c19` | TIMESTAMP uniform       | small         | thousands |

The `maxΔcnt ≈ 0.9997` figures in the report's TopN distribution
table tell the same story: where a value happens to appear in BOTH
PR's and BASE's TopN, the counts differ by ~100%.

For uniformly-distributed generator-produced data, **PR's counts
(5–9) match expectations; BASE's counts (~8,000) do not**. Either
BASE is double-counting (e.g. summing per-partition TopN counts and
partition-bucket Repeats for the same value), or BASE is selecting
TopN candidates from a sketch that overestimates frequency.

**Verified against ground truth** (2026-05-25, see "Verification"
section below): the actual maximum per-value count in c11 is **15**,
and the top 10 values all have actual counts 14–15. So BASE
(reports ~7,969) overcounts by ~530×; PR (reports ~9) undercounts
by ~1.7×. PR is far closer to reality but is still ~40% low.

The reason for PR's undercount: per-partition `pruneTopNItem`
drops a value with `count=1` from any single partition's TopN. So
a globally-hot value with true count=15 spread across 15 different
partitions (count=1 each) is never admitted to any partition's
TopN, and therefore never appears in the global merge candidate
pool. PR's TopN ends up picking values that are concentrated in
1–3 partitions (count≈4–5 per partition, count≈5–9 globally), not
values that are globally hot but spread thin.

BASE's `EqualRowCount` cross-partition path *would* in principle
recover the spread-thin values' counts — but it does so via range
estimation that inflates 530× past truth, which is worse than PR's
undercount.

## Pattern 3 — small disagreement at the top-N boundary (`c12`, `c20`)

For DECIMAL and DATETIME per-part-categ columns the bulk of TopN
matches:

| Column | jaccard | common | only PR | only BASE | maxΔcnt |
|---|---|---|---|---|---|
| `c12` (DECIMAL per-part-categ)  | 0.7679 | 86 | 13 | 13 | 0.0000 |
| `c20` (DATETIME per-part-categ) | 0.8544 | 88 |  7 |  8 | 0.0000 |

86–88 entries are present in both PR and BASE with identical counts
(`maxΔcnt=0`). The disagreement is the ~13 entries at the cutoff
boundary, where ties get broken differently. This is algorithmic
noise, not a real divergence.

## Root cause

There are **two independent asymmetries** in how BASE and PR
aggregate per-value counts in the global merge. Both are present in
both code paths; which one is *visible* depends on the data shape.

### Per-partition TopN pruning (same on both)

Both BASE and PR run the same per-partition TopN builder in
`pkg/statistics/builder.go::pruneTopNItem`. It drops a candidate
unless its observed count exceeds the Wald-confidence-interval upper
bound for "a random non-TopN value":

```go
// pruneTopNItem tries to prune the least common values in the top-n
// list if it is not significantly more common than the values not in
// the list. We assume that the ones not in the top-n list's
// selectivity is 1/remained_ndv which is the internal implementation
// of EqualRowCount
if float64(topns[topNNum-1].TopNMeta.Count) > selectivity*n + 2*stddev + 0.5 {
    break  // worth keeping; stop pruning
}
topNNum--    // not significantly more frequent than a non-TopN value → drop it
```

For uniform NOTNULL columns with NDV ≈ row_count, the expected
"non-TopN value" count is ~1, and a candidate with `count=1` cannot
clear `2*stddev + 0.5`. Every candidate gets pruned, so the
per-partition TopN comes out empty for `c3`, `c7`, `pk`.

For columns where values truly repeat (e.g. `c11` with avg 3
occurrences per value), the candidates survive pruning on both BASE
and PR. So per-partition TopN is populated on both sides.

### Asymmetry A — bucket-Repeat treatment in the global TopN pool

BASE's `pkg/statistics/handle/globalstats/topn.go::MergePartTopN2GlobalTopN`
only considers values that survived per-partition pruning:

```go
for _, val := range topN.TopN { ... }
```

PR's `pkg/statistics/histogram.go::MergePartTopNAndHistToGlobal`
ALSO walks every partition bucket and pushes its `Repeat` into the
`topNHeap`:

```go
repeat := h.Buckets[bi].Repeat
entry.repeatCount += uint64(repeat)
entry.totalRepeat += uint64(repeat)
...
topNHeap.Add(entry)
```

For uniform data the per-bucket `Repeat` is 1 (the bucket upper
value occurs once in the sample). 8000 partitions × 256 buckets =
2M heap pushes, almost all with `totalRepeat=1`. The bounded
min-heap keeps 100 of them arbitrarily. Per-partition pruning is
bypassed at the global level: the global TopN admits values that
would have been considered statistically insignificant per-partition.

### Asymmetry B — cross-partition EqualRowCount estimation

BASE's global merge inflates each TopN value's count by an
estimate, derived from every other partition's histogram:

```go
for j := range partNum {
    if (j == i && version >= 2) || topNs[j].FindTopN(val.Encoded) != -1 {
        continue
    }
    count, _ := hists[j].EqualRowCount(nil, datum, isIndex)
    if count != 0 {
        counter[encodedVal] += count
        hists[j].BinarySearchRemoveVal(&datum, int64(count))
    }
}
```

`EqualRowCount` for a value `v` inside a bucket `[a,b]` with `count=N`
and `NDV=k` returns roughly `N/k` — an average-density estimate,
not an observation. For uniform DECIMAL `c11` (`N/k` ≈ 3), summed
across ~8000 other-partition histograms, this accumulates to the
~8000 figures we see in BASE's TopN.

PR's global merge does **not** do this lookup. It accumulates only
exact contributions:

1. Sums of partition-TopN counts where the value actually appears.
2. Bucket `Repeat` where the value actually equals the bucket upper.

So PR refuses to range-estimate; BASE folds in range estimates from
every partition.

### Which asymmetry surfaces depends on the data

Both asymmetries are always in effect. Whether they show up in the
output depends on what per-partition pruning produced:

| Data shape | Per-partition TopN | Asymmetry A visible? | Asymmetry B visible? |
|---|---|---|---|
| Uniform, NDV ≈ rows (`c3`, `c7`, `pk`) | empty (pruned) | **yes** — PR's bucket-Repeat=1 floodfill is the only thing in the heap; 100 win | no — BASE has no TopN values, so nothing to inflate via `EqualRowCount` |
| Uniform-with-repeats, NDV ≪ rows (`c11`, `c15`, `c19`) | populated, real counts > 1 | no — PR's bucket-Repeat=1 floodfill is outcompeted by real counts > 1 | **yes** — BASE inflates the real candidates via `EqualRowCount`; PR keeps them exact |
| per-part-categ (`c12`, `c20`) | populated, mostly disjoint per-partition value sets | partial — the per-partition cross-partition lookups in BASE find a value only in its own partition (others' histograms don't cover it), so inflation is small | small fringe disagreement only |

So Pattern 1 is "Asymmetry A in the absence of real TopN candidates",
Pattern 2 is "Asymmetry B operating on real TopN candidates", and
Pattern 3 is "neither asymmetry has much to work with — both algorithms
land on essentially the same answer."

## Verification (2026-05-25)

Restored `combined-backup-v3` and ran a direct
`GROUP BY c11 ORDER BY COUNT(*) DESC LIMIT 10` against the actual
table. Result:

```
+----------+--------------+
| c11      | actual_count |
+----------+--------------+
| 58935.54 |           15 |
| 12630.90 |           15 |
| 67853.54 |           15 |
| 71095.63 |           15 |
| 93275.21 |           15 |
| 67039.47 |           15 |
| 24471.72 |           14 |
| 83113.38 |           14 |
| 89548.09 |           14 |
| 16469.74 |           14 |
+----------+--------------+
```

The actual max per-value count is **15**.

| | Reported max count | Vs ground truth (15) |
|---|---|---|
| BASE TopN     | ~7,969 | **~530× too high** |
| PR TopN       |     ~9 | ~1.7× too low |

So BASE has been overestimating per-value selectivity on
uniform DECIMAL / VARCHAR / TIMESTAMP columns by ~500× — the
cross-partition `EqualRowCount` sum doesn't reflect anything real
about the data. PR is much closer to the truth but still
undercounts because the most globally-frequent values (true
count=15 spread across 15 partitions with count=1 each) are
rejected by each partition's `pruneTopNItem` and so never reach
the global merge candidate pool.

## Implications

- **Pattern 3**: benign, no action needed.

- **Pattern 1** (c3/c7/pk singletons): PR currently fills 100 TopN
  slots with `count=1` entries that carry no selectivity signal.
  Cleanup option: gate the bucket-Repeat heap admission on
  `entry.totalRepeat > 1`, or apply a `pruneTopNItem`-style global
  prune after heap construction. Either matches BASE's behavior on
  these columns and reduces stats noise. No optimizer accuracy
  cost — the histogram fallback gives the same estimate either way.

- **Pattern 2** (c11/c15/c19 uniform-with-repeats): PR is
  substantially more accurate than BASE (off by 1.7× vs 530×),
  but applying `pruneTopNItem` *as-is* at the global level on PR
  would over-prune, because PR's reported counts (~5–9) are
  themselves systematically below truth (~14–15). A global
  Wald-CI test calibrated on global NDV / row count would reject
  candidates whose true count would have justified inclusion.

  A better follow-up would be to make PR's counts more accurate
  first — for example, when accumulating each global TopN
  candidate's count, also walk the partition histograms for the
  same value (similar to BASE's `EqualRowCount` step) but pull
  *only the per-partition bucket Repeat*, not a range estimate.
  Then `pruneTopNItem` at global level would discriminate
  correctly.

  Even without that, the PR's current state is a strict
  improvement: 1.7× undercount is materially better than 530×
  overcount for the optimizer's cardinality estimates on these
  columns. Plans that were calibrated against BASE's inflated
  counts will see lower estimates under PR — that is the *fix*,
  not a regression, but reviewers should expect equality-predicate
  cardinality changes on workloads that touch hot values of
  uniform high-NDV columns.

## Why this didn't come up earlier

The histogram bucket regression was visible at high level (PR
produced 6–67 buckets vs BASE's 256 on per-part-categ columns), so it
dominated attention. The TopN counts were in the same accuracy diff
all along but weren't flagged because:

1. The bucket regression was a clear correctness failure (data loss
   was measurable in row totals), while the TopN divergence
   preserves total row counts.
2. Both PR and BASE produce TopN sets of plausible-looking size
   (100 entries), so the divergence isn't obvious without reading
   the actual counts inside.
3. The accuracy-diff tool's `jaccard` summary makes Pattern 2 look
   like Pattern 3 (mostly-disjoint sets), when in fact Pattern 2 is
   "the same value at two different count scales," not "different
   value sets."
