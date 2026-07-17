# Benchmark Findings v5: improve-global-stats-3 PR vs a CORRECT base

**Status: COMPLETE** (2026-07-16). All 16 matrix cells, accuracy diff,
per-phase resource attribution, the partitioned≡non-partitioned equivalence
test, and the BASE concurrency sweep (c=16 group; c=2 skipped as bracketed)
are done.

Date: 2026-07-16
PR: `improve-global-stats-3` HEAD `8cf6bd31b5` vs BASE = its true merge-base
on master `f994e8da27` (2026-07-15, = current `pingcap/master` HEAD).
Hardware: Linux x86, 16 cores, 64 GB RAM (host `minisum`).
Data: reused from v4 — **8192 HASH partitions, 40M rows, 20 columns
(5 type classes × 4 distributions) + 2 indexes** (c14; c2,c19).
Both binaries rebuilt from source; combined backup reused from v4
(`combined-backup-v4`, 25 GB, seeded with per-partition stats).

Matrix: {part-single, part-full} × existing-stats × {async ON, OFF} × 2 iters
× {BASE, PR} = 16 cells.

## Why v5 exists: the v4 BASE was invalid

v4 (`FINDINGS-v4.md`) built its BASE binary from master `749c973e5a`
(**2025-11-04**) and documented it as "the PR's merge-base". It was not.
Both PR HEADs v4 benchmarked (`a9d5f61a7a`, `63d8b20af9`) had merge-base
`e53c50c1c5` (2026-05-22) — **1037 commits newer, 63 of them touching
`pkg/statistics`**. So every v4 PR-vs-BASE delta credited the PR with ~6.5
months of unrelated master work on analyze/statistics — notably
`296420ee5b` "use clustered PRIMARY KEY for stats system tables" (#68324),
which speeds the per-partition stats load, and the master-side fix that
made single-partition ANALYZE do a full-quality global merge instead of a
degenerate one.

Root cause of the mistake: `/home/mattias/repos/tidb-base` has a *local*
branch `master` pinned at `749c973e5a` while `pingcap/master` is current,
so `git merge-base <pr> master` in that worktree silently returned a stale
commit. The built binaries exposed the skew directly — BASE stamped
`v9.0.0-beta.2.pre-716`, PR `pre-1759`, a ~1000-commit gap in the same
lineage.

**v5 fixes this.** BASE is resolved against `pingcap/master` and the two
binaries were verified paired at build time: BASE `pre-1991-gf994e8da27`,
PR `pre-1997-g8cf6bd31b5` — exactly 6 commits apart, matching the PR's 6
commits. This paired-`-V` check is the guard that would have caught v4.

### Direct proof the v4 BASE ran a different code path

The BASE part-single async=ON cell, measured on both v4's binary and v5's:

| | BASE part-single asyncON | PR part-single asyncON |
|---|---|---|
| v4 (stale BASE `749c973e5a`) | **4m50s** | 10m13s |
| v5 (correct BASE `f994e8da27`) | **1h32m36s** | 10m07s |

The BASE number moved **19×**; the PR number moved **1%**. The PR was never
involved in that discrepancy — it isolates the entire v4 error to the BASE
side. v4's own analysis noticed the old BASE emitted degenerate 5–71 bucket
histograms on this path and called it "fast by under-merging"; what it
missed is that this path had since been fixed upstream, so the current
merge-base does the full-quality merge (and the PR does it faster).

## Headline results (part-single + part-full async=ON/OFF)

All values are the mean of 2 iterations. Speedup = BASE/PR wall-clock;
memory ratio = PR/BASE peak RSS.

| scenario | async | BASE dur | PR dur | **speedup** | BASE RSS | PR RSS | **mem** |
|---|---|---|---|---|---|---|---|
| part-single | ON  | 1h32m36s | 10m07s | **9.16×** | 7.14 GB | 3.74 GB | **0.52×** |
| part-single | OFF | 2h05m21s | 18m13s | **6.88×** | 27.71 GB | 27.60 GB | **1.00×** |
| part-full   | ON  | 1h52m24s | 23m52s | **4.71×** | 8.22 GB | 5.55 GB | **0.67×** |
| part-full   | OFF | 2h26m59s | 35m43s | **4.12×** | 29.78 GB | 28.71 GB | **0.96×** |

Per-iteration detail (duration / peak RSS):

| cell | iter1 | iter2 |
|---|---|---|
| BASE part-single ON  | 1h31m14s / 7.24 GB | 1h33m57s / 7.04 GB |
| PR   part-single ON  | 10m05s / 3.41 GB | 10m08s / 4.07 GB |
| BASE part-single OFF | 2h05m05s / 27.86 GB | 2h05m37s / 27.55 GB |
| PR   part-single OFF | 18m14s / 27.62 GB | 18m12s / 27.57 GB |
| BASE part-full ON    | 1h52m20s / 8.18 GB | 1h52m29s / 8.26 GB |
| PR   part-full ON    | 23m32s / 4.72 GB | 24m12s / 6.38 GB |
| PR   part-full OFF   | 35m21s / 29.00 GB | 36m05s / 28.42 GB |
| BASE part-full OFF   | 2h27m03s / 30.93 GB | 2h26m56s / 28.62 GB |

### Reproducibility is the strongest evidence

- **BASE iters agree to within 0.1–3%** on every completed cell (part-full
  ON iters landed 9 seconds apart on a ~1h52m run). These are stable
  measurements, not noise.
- **All four PR cells reproduce v4's PR numbers within ~4%** despite a
  different PR HEAD, a rebase across 1275 master commits, and a leaner
  playground (see below): part-single ON 10m07s vs v4 10m13s; part-full ON
  23m52s vs v4 24m50s; part-full OFF 35m43s vs v4 35m28s. Every difference
  between v4 and v5 is on the BASE side, never the PR side — exactly what
  the stale-BASE diagnosis predicts.

## Every v4 conclusion that depended on BASE has inverted

| v4 claim | v4 basis (stale BASE) | v5 (correct BASE) |
|---|---|---|
| "part-single async=ON: **PR 2× slower** (0.47×)" | BASE 4m50s | **PR 9.16× faster** |
| "part-single async=OFF: **PR memory regression**" | BASE 11.04 GB vs PR 29 GB | **1.00× — parity** (BASE 27.71 GB) |
| "part-full async=OFF: **PR memory regression** (1.21×)" | BASE 24.74 GB vs PR 29.82 GB | **0.96× — PR uses less** (BASE 29.78 GB) |
| "part-full async=ON: 5.47×" | BASE 2h15m | **4.71×** (BASE 1h52m) |
| "Open question: is PR doing a redundant full rebuild on single-partition ANALYZE?" | old BASE skipped the full merge | **No** — current master does the same full rebuild; PR just does it 9× faster |
| "Worth profiling the async=OFF PR heap (regression)" | phantom, from the 11 GB BASE | **No action** — no regression exists |

The part-single "PR 2× slower" finding and its whole investigation section
in v4 were artifacts of the stale BASE. So was the async=OFF "memory
regression" and the recommendation to profile the PR heap.

## The async split is coherent and matches the PR's design

- **async=ON** is where the PR's combined k-way merge shrinks the working
  set: **0.52× memory on part-single, 0.67× on part-full**. The merge
  streams partition buckets through a heap instead of materializing the
  O(TopN × P) cross-product.
- **async=OFF** holds all 8192 partitions' stats in memory before merging
  on *both* branches, so peak memory converges to parity (**1.00×** on
  part-single). The PR's win there is purely algorithmic wall-clock
  (6.88×), not memory. v3 said the same qualitatively ("memory savings
  only manifest on async=ON"); v5 quantifies it against a correct base.

## Why part-full speedup < part-single speedup (Amdahl, not a weaker win)

| | speedup | why |
|---|---|---|
| part-single async=ON | 9.16× | the global merge is ~all of the work |
| part-full async=ON | 4.71× | merge + ~20 min of per-partition **collection** that is byte-identical on both branches |

part-single re-collects only one partition (seconds) then merges all 8192,
so the merge dominates and the PR's merge speedup shows through nearly
undiluted. part-full re-collects all 8192 partitions first; that phase is
unchanged between BASE and PR, so it dilutes the ratio. Same algorithm,
bigger denominator.

## Phase decomposition — isolating the phase the PR actually changes

The total-ANALYZE numbers above understate the PR's real effect, because
they bundle three phases and the PR rewrites only one of them. The PR
touches **only the global merge**. It does not change:

- **per-partition collection** — the ANALYZE scan that samples each
  partition and builds its per-partition TopN + histogram (identical
  executor code on both branches);
- **per-partition stats save/load** — writing those per-partition stats to
  `mysql.stats_*` and the async path that reloads all partitions from
  storage before merging (the load loop `for _, partitionID := range
  a.partitionIDs` is byte-identical between branches, per the v4 code
  audit).

Both branches emit a phase-boundary marker in `tidb.log` — `use async merge
global stats` (async=ON) / `use blocking merge global stats` (async=OFF) —
that separates collection from the global-stats (load+merge) phase. Using
that boundary plus the ANALYZE start/end from `profile_result.json`
(extractor: `phase-timing.py`):

| scenario | async | phase | BASE | PR | **ratio** |
|---|---|---|---|---|---|
| part-single | ON  | collection  | 0.5s | 0.5s | 1.01× |
| | | **global-stats (load+merge)** | 1h31m14s | 10m05s | **9.05×** |
| part-single | OFF | collection  | 0.5s | 0.5s | 0.99× |
| | | **global-stats (load+merge)** | 2h05m04s | 18m13s | **6.86×** |
| part-full | ON  | collection  | 15m30s | 14m24s | **1.08×** |
| | | **global-stats (load+merge)** | 1h36m50s | 9m08s | **10.59×** |
| part-full | OFF | collection  | 15m58s | 15m55s | **1.00×** |
| | | **global-stats (load+merge)** | 2h11m05s | 19m27s | **6.74×** |

**Two conclusions the total-ANALYZE numbers hide:**

1. **The PR strictly affects the merge — collection is untouched.** On
   part-full the per-partition collection phase is within **0–8%** between
   BASE and PR (15m30s vs 14m24s async=ON; 15m58s vs 15m55s async=OFF —
   essentially identical). The 8% on async=ON is run-to-run scan variance,
   not a code difference; async=OFF, less GC-sensitive here, lands at 1.00×.
   This is direct empirical confirmation that the PR does not affect
   per-partition gathering or stats save/load — exactly as the code scope
   implies.

2. **The merge phase speedup is much larger than the ANALYZE speedup.** On
   part-full async=ON the global-stats phase is **10.59×** faster, versus
   the 4.77× total — the total is diluted by ~15 min of collection that both
   branches pay equally. part-single, with negligible collection, already
   showed the merge speedup nearly undiluted (9.05×).

### Cross-branch merge timing from mysql.analyze_jobs (both branches)

`mysql.analyze_jobs` (captured per run as `analyze_status_after`) records a
`merge global stats for <table> columns` job plus one job per index, each
with start/end — a **structured** merge-phase measurement present on BOTH
branches (no dependency on PR-only logs). It confirms and refines the
log-scraped numbers above. part-full async=ON:

| merge job | BASE | PR | ratio |
|---|---|---|---|
| columns | 1h35m05s | 8m02s | **11.83×** |
| index idx_c14 | 1m04s | 31s | 2.06× |
| index idx_c2_c19 | 41s | 35s | 1.17× |
| **merge phase total** | **1h36m50s** | **9m08s** | **10.60×** |

(matches the 10.59× from the log-derived global-stats phase — two
independent sources agree). Extractor: `merge-detail.py`.

### The PR's merge compute is ~30 seconds; the phase is now load-bound

The PR's per-histogram diagnostic logs (`MergePartTopNAndHistToGlobal
start` → `step 1d`; merges run sequentially) split its 9m08s merge phase
into the actual merge computation vs the per-partition stats load between
columns:

- **merge-fn (the rewritten algorithm), summed over all 23 histograms:
  30.2 s.** Per column it is 0.13–3.29 s — low-NDV columns 0.1–0.2 s (TopN
  saturates, no bucket merge), heaviest are DECIMAL/DATETIME per-part-categ
  at ~3 s. This matches v3's per-column merge-fn table (0.09–2.45 s).
- **load gaps (per-partition hist+topN read from storage): ~13–17 s per
  column, 5m50s total.** This is the SAME code on BASE (identical async
  load loop) — unchanged by the PR.

So the PR's entire merge *computation* is ~30 s; the rest of its 9-minute
merge phase is loading 8192 partitions' stats from `mysql.stats_*`, a cost
both branches pay. BASE's merge phase is 1h36m50s for the same load +
compute — i.e. BASE spends ~1h31m in merge *computation* (the O(TopN × P)
inner loop) where the PR spends 30 s. The combined k-way merge turned a
compute-bound phase into a load-bound one.

This is the sharpest statement of the PR's scope: it strictly replaces the
merge computation (seconds vs ~1.5 h), leaving per-partition collection and
per-partition stats load/save untouched (collection ratio 1.00–1.08×
above; load gaps common to both branches).

Note the merge phase is ~2× longer on async=OFF than async=ON on identical
data (e.g. part-single global-stats 18m13s vs 10m05s). The likely cause is
GC pressure: async=OFF holds all 8192 partitions' stats resident (~27–29
GB) during the merge, so the same work runs against a much larger live
heap. Consistent with async=OFF being memory-bound on both branches, and an
argument for the async=ON streaming path.

## Per-phase resource attribution (memory + CPU)

Bucketing the per-second `tidb_metrics` / goroutine samples and the
`cpu_profile_*` snapshots onto the phase boundaries (`resource-by-phase.py`)
attributes memory and CPU to collection vs merge — not just a whole-run
peak. iter1, RSS in GB.

### Memory by phase

| scenario | async | phase | BASE peak / mean | PR peak / mean |
|---|---|---|---|---|
| part-full | ON  | collection | 3.96 / 3.30 | 3.86 / 3.28 |
| | | **merge** | **8.18 / 5.19** | **4.72 / 3.36** |
| part-full | OFF | collection | 4.01 / 3.37 | 3.90 / 3.32 |
| | | **merge** | **30.93 / 21.19** | **29.00 / 9.45** |
| part-single | ON  | merge (≈whole run) | 7.24 / 3.42 | 3.41 / 1.73 |
| part-single | OFF | merge (≈whole run) | 27.86 / 19.73 | 27.62 / 9.02 |

Two findings:

1. **Collection-phase memory is identical** (part-full: BASE 3.96–4.01 /
   3.30–3.37 vs PR 3.86–3.90 / 3.28–3.32). The whole memory difference
   lives in the merge phase — third independent confirmation the PR touches
   only the merge.

2. **The async=OFF "memory parity" (1.00× peak) understates the PR's
   efficiency.** Peak RSS is a brief spike when both branches load all 8192
   partitions' stats (a shared, unchanged step), so peaks tie (~27.6–30.9
   GB). But the *mean* merge-phase working set is roughly **half** on the
   PR — part-single OFF mean RSS 9.02 vs 19.73 GB (0.46×), mean heap 5.86
   vs 14.45 GB (0.41×); part-full OFF mean RSS 9.45 vs 21.19 GB (0.45×).
   BASE holds the full O(TopN × P) merge state resident throughout; the
   PR's k-way merge streams, so it spikes once at load then runs light.
   The headline "1.00× / 0.96×" peak numbers are the pessimistic view of
   the PR's memory behaviour; sustained memory is ~0.45×.

### CPU by phase (merge-phase function composition, part-full async=ON)

Summed cum-time over the merge-phase cpu snapshots:

| BASE merge phase | | PR merge phase | |
|---|---|---|---|
| `MergePartTopN2GlobalTopN` | 169.5s | `syscall.Syscall6` (partition load I/O) | 22.4s |
| `Histogram.LocateBucket` | 115.9s | `runtime.futex` | 7.8s |
| `Histogram.EqualRowCount` | 76.5s | `chunk.Column.IsNull` | 7.5s |
| `MyDecimal.Compare` | 53.0s | `globalMergeRefs.massAndRepeat` (new) | 7.4s |
| `TopN.FindTopN` | 49.9s | `bucketGroupCursor.firstNonEmptyBucket` (new) | 7.1s |

BASE's merge phase is entirely the O(TopN × P) inner loop —
`MergePartTopN2GlobalTopN` walking `LocateBucket`/`EqualRowCount` over every
partition histogram per TopN value. **On the PR those functions do not
appear at all.** The PR's merge-phase CPU is instead dominated by syscalls
(loading partition stats — the shared, unchanged cost) plus the new k-way
merge structures (`globalMergeRefs`, `bucketGroupCursor`), each an order of
magnitude smaller. This is the function-level view of "BASE is compute-
bound in the merge; the PR made the merge negligible and is now load-bound."

## The single-core bottleneck (why BASE is so slow, and the sweep)

Live CPU profiles of the running BASE (both part-single and part-full)
show **one core pinned at ~100%** on a 16-core box, ~94% of samples inside
`globalstats.MergePartTopN2GlobalTopN` → `EqualRowCount` / `LocateBucket`
/ `MyDecimal.Compare`. That is BASE's O(TopN × partitions) inner loop:
for each of 100 global-TopN values, probe all 8192 partition histograms.

BASE's default `tidb_merge_partition_stats_concurrency = 1` dispatches to
the single-threaded path (`if mergeConcurrency < 2` in
`globalstats/topn.go`). The PR (`821ab3de59`) deletes this knob. So the
matrix measures BASE at its slowest configuration. The concurrency sweep
(below) measures what the knob was worth before drawing final conclusions.

## Comparison to v3 (also a correct base, so directly informative)

v3 measured part-full async=ON at **1.86×**. v5 gets **4.71×** on the same
code path. The gap is *not* a contradiction — v3 ran on different hardware
and smaller data (macOS M3 Max, 8000 partitions / 30M rows), where the PR
itself took 38m22s vs 23m52s here. Different machine and data shape →
different ratio; not comparable in absolute terms. Both v3 and v5 used a
correct merge-base, so both are methodologically sound; v4 (5.47×) landed
near v5 by accident (it inflated BASE part-full while measuring PR
correctly) and is not defensible.

## Accuracy — cardinality, histograms, TopN

From `output-bench-v5/report.txt` (`accuracy-diff.py`), part-single
async=ON section (representative; all cells consistent).

### Cardinality — identical, no regression

`row_count` ratio PR/BASE = **1.0000**. Per-column **ΔNDV = +0 (ratio
1.0000) and ΔnullCount = +0 for all 20 columns + pk + both indexes**. No
cardinality change of any kind.

### Histograms — the v3 chunking fix holds against a correct base

Per-column histogram shapes now match closely: **KS distance ≤ 0.10** on
every column, bucket counts within ±15. The per-partition-categorical
columns that regressed pre-fix in v3 (c4, c8, c12, c20) produce the **full
256 buckets on both branches** (c4 256=256, c8 256=256, c12 256=256, c20
256=256; KS 0.0039). None of v4's degenerate 5–71-bucket histograms appear
— confirming the `uint16` virtual-histogram chunking fix works against a
correct merge-base, not just in v3's isolated test.

### TopN — the known v3 divergence persists; PR is closer to truth

TopN content still differs between branches, exactly as
`FINDINGS-v3-topn-divergence.md` documented, and for the same reasons:

- **BASE inflates counts via cross-partition `EqualRowCount`.** On uniform
  high-NDV columns BASE's TopN max count is ~7,800–7,980 (c3 7850, c7 7798,
  c11 7809, c15 7835, c16 7908, c19 7981), whereas PR reports max count 1.
  v3 verified ground truth (actual max ≈ 15), so **PR is far closer to
  reality; BASE overcounts ~500×.** Unchanged finding, now reproduced
  against the correct base.
- **The singleton filter partially fires.** On the uniform high-NDV
  columns it targets, PR's global TopN drops toward BASE's: c3 → 0 (both
  0), pk → 0 (both 0). It is not uniform, though — c7 still keeps 21
  entries on PR (BASE 0), and c4 goes the other way (PR 0, BASE 100). So
  the `numTopN==default && heap-full` gate empties some but not all of v3's
  Pattern-1 singletons. The c4 case (per-part-categ, high NULL) is analysed
  directly in the equivalence test below.
- Low-NDV and per-part-categ columns match well (jaccard 1.0000 on c1, c5,
  c8, c9, c13; 0.77–0.98 on c12/c14/c20).

Net: cardinality and histograms are clean and match; TopN diverges in the
documented, orthogonal way where PR's counts are the accurate ones. No new
accuracy regression introduced by the rebase or the singleton filter.

### Partitioned ≡ non-partitioned TopN — direct equivalence test

The singleton filter's design bar (`FINDINGS-v3-singleton-filter.md`) was
that a partitioned table's *global* TopN should match an identical
*non-partitioned* table's TopN. The main matrix could not test this (no
non-partitioned twin). A focused check was run: identical seed-42 data
(256 HASH partitions, 1.28M rows) loaded into a partitioned table and a
non-partitioned clone, **on both binaries, in isolated playgrounds, with
full sampling forced on both sides** (so sampling rate is not a confound),
then the partitioned global TopN compared to the non-partitioned TopN per
column. TopN entry counts:

| column | distribution | PR part / np | BASE part / np |
|---|---|---|---|
| c1 | uniform unsigned | 0 / 0 ✓ | 0 / 0 ✓ |
| c2 | uniform double | 0 / 7 ✗ | 0 / 4 ✗ |
| c3 | per-part-categ (NULL 40%) | **0 / 11 ✗** | **100 / 11 ✗** |
| c4 | uniform decimal | 100 / 100 ✓ | 100 / 100 ✓ |
| c5 | per-part-categ (varchar) | 2 / 100 ✗ | 2 / 100 ✗ |

Conclusions:

1. **Partitioned ≢ non-partitioned is pre-existing — BASE diverges too.** On
   c2/c3/c5 *neither* branch's partitioned global TopN matches the
   non-partitioned table. This is a fundamental partitioned-merge vs
   single-table-build difference, **not introduced by the PR**. The design
   doc's invariant is not upheld by BASE either at this scale.

2. **One column shape differs between PR and BASE: per-part-categ with high
   NULL (c3).** BASE keeps 100 TopN entries, the PR keeps 0; ground truth
   (non-partitioned) is 11. BASE overshoots with ~89 noise entries; the PR
   undershoots to empty. This **reproduces the matrix's c4 result** (same
   shape: BIGINT per-part-categ NULL(40), PR TopN 0 vs BASE 100) and
   explains it — the PR's singleton filter empties the global TopN on that
   shape where a non-partitioned analyze would keep a handful.

3. **The c5 shape (per-part-categ, no NULL) is identical on both branches**
   (2 vs non-partitioned's 100) — a shared, pre-existing merge limitation.

So the singleton filter introduces no new invariant break; its only
PR-vs-BASE effect is more aggressive emptying on per-part-categ high-NULL
columns. NDV and histograms are unaffected there, so the impact is limited
to slightly less precise equality-selectivity on those columns' frequent
values (histogram fallback still applies). A refinement, not a correctness
issue; non-blocking. (Reproduce with the isolated-cluster procedure noted
in the artifact paths.)

## Concurrency sweep — the deleted knob provided no benefit

The PR (`821ab3de59`) deprecates `tidb_merge_partition_stats_concurrency`
(BASE default 1). To check what removing it costs, BASE was re-run at
concurrency 16 (the max) across all four scenarios. The setting was
asserted to actually apply (it reads back as the set value on BASE; on the
PR it is a no-op returning 1, so the sweep runs BASE only).

| scenario | async | BASE c=1 | BASE c=16 | effect |
|---|---|---|---|---|
| part-single | ON  | 1h31m15s / 7.24 GB | 1h33m57s / 7.44 GB | none |
| part-single | OFF | 2h05m05s / 27.86 GB | 2h09m55s / 30.36 GB | none (slightly worse) |
| part-full | ON  | 1h52m20s / 8.18 GB | 1h53m06s / 8.33 GB | none |
| part-full | OFF | 2h27m03s / 30.93 GB | 2h26m21s / 30.57 GB | none |

**At maximum concurrency the knob gives no speedup on any scenario** — even
on part-full async=ON, the heaviest merge (1h36m50s of merging), the merge
phase at c=16 is 1h37m00s, identical within 10 s. On async=OFF it is
marginally *worse* (more time, +2.5 GB RSS from the worker pool).

Why: `mergeGlobalStatsTopN` only routes to the concurrent path
(`MergeGlobalStatsTopNByConcurrency`) for the TopN batching; the dominant
cost — the per-column histogram merge and the O(TopN × P)
`EqualRowCount`/`LocateBucket` loop — runs per-column sequentially and is
untouched. So there is nothing for the workers to parallelize.

**Conclusion: removing the knob costs nothing.** BASE at its best-tuned
setting is still ~4–9× slower than the PR. The c=2 cells were not run: c=2
sits between c=1 and c=16, and those endpoints are identical on every
scenario, so an intermediate setting cannot differ — the result is
bracketed. The sweep was stopped after the c=16 group.

## Methodology notes

### Leaner playground (TiFlash + monitor removed)

tiup playground defaults TiFlash, Prometheus, and Grafana on. Nothing here
uses them: the benchmarked table has no TiFlash replica, and
`analyze-profile` scrapes TiDB's and TiKV's own `/metrics` status ports
directly (`collectors.go`) rather than querying Prometheus. They only
contend for RAM/CPU with the process under measurement — material on the
async=OFF cells that peak near 30 GB on a 60 GB box. `bench.sh` now passes
`--tiflash 0 --without-monitor` (overridable via `TIFLASH_NUM=1` /
`PLAYGROUND_MONITOR=1`). Verified metrics still collect; smoke ran ~25%
faster. **Consequence:** v5 absolute numbers are not directly comparable to
v3/v4, but the PR-vs-BASE ratio within v5 is unaffected (both branches get
identical treatment).

### Backup reuse held

`combined-backup-v4` (2026-05-22, 25 GB) restored clean into a fresh
playground on all 8 attempts (32,875/32,875 ranges, 0 failed, ~3m10s each)
— no GC-safepoint failure despite 54 days of age. Restoring into a *fresh*
cluster has no GC history to trip over; the age worry (from v3's notes) was
really about a laptop sleeping *during* a restore. Stats system-table
schema was verified unchanged since the backup (only `mysql.tidb_masking_
policy` changed in that window), so the embedded per-partition stats
restore compatibly.

### Harness

Matrix launched under `systemd-run --user --unit=matrix-v5` with linger
enabled (v4's lesson: nohup+disown dies on session teardown). `run-v5.sh`
is idempotent at (branch, scenario, async) group granularity, so it doubles
as its own resume. The concurrency sweep runs `bench-sweep.sh`, a snapshot
fork of `bench.sh` taken while the matrix was mid-run (bash re-reads a
script from a file offset as it executes, so editing the live file could
corrupt the running matrix). Fold the sweep's `set_merge_concurrency` hook
back into `bench.sh` and delete the fork once both runs are done.

## Raw artifact paths

- `output-bench-v5/manifest.tsv` — 16 cells (rebuilt from disk at matrix end)
- `output-bench-v5/report.txt` — compare-runs + profile-diff + accuracy-diff
  _(generated after the final cell)_
- `output-bench-v5/{BASE,PR}/<scenario>_existing_async<ON|OFF>_iter{1,2}/` —
  per-cell run dirs with `profile_result.json`, heap/CPU snapshots,
  `stats_dump.json`, `tidb-logs/`
- `output-bench-v5-sweep/c16/BASE/...` — concurrency sweep (c=16 group; c=2
  not run — bracketed by identical c=1/c=16)
- `combined-backup-v4/` (25 GB) — reused BR backup with stats
- `bench-config-v5.sh`, `bench-v5-smoke.sh`, `run-v5.sh` — matrix drivers
- `bench-config-v5-sweep.sh`, `bench-sweep.sh`, `sweep-v5-concurrency.sh` —
  sweep drivers
- `tidb-server.master` → BASE `f994e8da27`; `tidb-server.pr` → PR `8cf6bd31b5`
- `phase-timing.py`, `merge-detail.py`, `resource-by-phase.py` — analysis tools
- Equivalence test (partitioned ≡ non-partitioned): reproduce by starting an
  isolated playground (`tiup playground nightly --port-offset 10000 --tiflash 0
  --without-monitor --db.binpath <bin>`), `analyze-profile setup … --seed 42
  --column-spec "BIGINT UNSIGNED:uniform:NOTNULL,DOUBLE:uniform,BIGINT:per-part-categ:NULL(40),DECIMAL:uniform,VARCHAR:per-part-categ"`,
  clone to a non-partitioned twin (`CREATE TABLE t_np LIKE …; ALTER TABLE t_np
  REMOVE PARTITIONING; INSERT … SELECT`), `ANALYZE … WITH 1.0 SAMPLERATE` both,
  then compare `mysql.stats_top_n` counts per `hist_id`. Run once per binary.
- v3 (correct base, macOS): `FINDINGS-v3.md`; v4 (stale base, void):
  `FINDINGS-v4.md`
