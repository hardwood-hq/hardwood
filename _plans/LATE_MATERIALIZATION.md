# Plan: late materialization on the scheduled read

Tracking issue: #500. Stage issues: none yet; each stage below needs one.

A filtered read decodes every payload column in full and drops the rows the predicate rejects
afterwards ([RECORD_FILTERING.md](../_designs/RECORD_FILTERING.md#boundaries)). This plan makes
a payload column's fills, its page tasks and, on remote files, its fetches depend on the
selection of each batch, computed from the predicate columns as their cells complete. It builds
on the scheduled pipeline of #1287 ([READ_PIPELINE.md](../_designs/READ_PIPELINE.md#scheduled-columns)).
The row-group-phased sketches on #500 were written against the per-column drain model, which
#1287 replaced; this plan supersedes them and keeps their findings on remote reads (prefetch of
the predicate columns, coalescing).

Related issues: #1475 (one scheduler per read, late materialization as a per-batch phase),
#728 and #726 (skipping masked runs inside the decoder), #1025 (sequential chunk sizing from row
ranges), #485 (drain-side gaps), #3 (a selection given by row index), #1169 / #732 (one row
reader).

## Where the read stands

| Mechanism | Unit | Decided from | Saves |
| --- | --- | --- | --- |
| Row-group pruning | row group | statistics, bloom filters, dictionaries | all I/O and decode |
| Page masks ([FETCH_PLANNING.md](../_designs/FETCH_PLANNING.md#page-masking)) | page, and row intervals within a kept page | the predicate columns' ColumnIndex | I/O and decode of dropped pages; in a kept page, the fill of rows outside its intervals |
| Null placeholders | page | inline page statistics | decompression and decode |
| Filter-only column skip | column × row group | statistics proving a full match | the filter-only column's I/O and decode |
| Drain-side fragments | batch | the predicate columns' values | nothing in decode: evaluation runs on the thread completing the cell |

Every row above except the fragments is decided from metadata, before any value is decoded, and
changes the rows the batch plan keeps. The masks therefore apply to every decoded column of a row
group or to none. Late materialization adds a source decided from values, at batch granularity,
after the plan has cut the batches.

**Today's evaluation as a dependency.** A filtered `FlatRowReader` whose predicate compiles
drain-side gives each predicate column a fragment; the cell's completion runs it into
`Batch.matches` (`ScheduledFlatColumn.testFilter`). The consumer polls one batch per column and
merges the bitmaps (`BatchMatchMerger`, aliasing mode). `ColumnReaders` runs the fragments and the
merge on the consumer (`SelectionEngine`, owning mode). The per-row matcher, and every filter on
`NestedRowReader`, runs on the consumer. Columns never wait on each other: the only cross-column
point is the consumer's poll.

## Dependency model

**Selection.** The selection `S(b)` of planned batch `b` is the merge of the predicate fragments'
bitmaps for `b`, one bit per row the plan puts in `b`. It is **resolved** when the last
predicate cell of `b` completes: that thread runs the merge (`MergePlanEvaluator`) before it
publishes its cell, so `S(b)` is resolved before the consumer can take `b`. A batch statistics
proved (`filterAlwaysMatches`) is resolved at planning as all rows.

**Rendezvous.** The cells of all columns for one batch are distinct objects today, matched only
by row. `BatchPlan` creates one shared object per planned batch (call it `PlannedBatch`), handed
to every column's `Placer.Sink.open`, so every cell of `b` references it. It holds the countdown
of predicate cells still to complete, the merged words (or an alias of a single column's
`matches` when the merge plan is one `MergePlan.Column`), the gated columns waiting on it, and a
failure. Planning is already synchronized and plans every batch once, so the object is created
exactly once, including for a batch the plan leaves open across row groups.

**Who depends on whom.**

| Read | Predicate columns | Gated payload columns |
| --- | --- | --- |
| `FlatRowReader`, drain-side eligible | the merge plan's `Column` leaves | every other decoded column |
| `ColumnReaders` / filtered `ColumnReader`, drain-side eligible | as above, once its fragments run on cells (stage 2) | flat payload columns; nested ones from stage 5 |
| Per-row matcher (ineligible shapes, `NestedRowReader`) | none | none: the read stays as today |
| Unfiltered | none | none |

A payload column that is also a predicate column is a predicate column and is never gated.
Gating is per column: an ungated column behaves exactly as today, which lets every stage ship
behind its own eligibility rule.

The countdown counts the merge plan's referenced columns. Filter-only columns are skipped only in
proven row groups, whose batches resolve at planning and have no countdown, so an undecided
batch always sees every referenced column complete. This rests on the existing rule that the
skip condition equals the poll condition ([RECORD_FILTERING.md](../_designs/RECORD_FILTERING.md#filter-only-column-skip)).

## Representation: positional selection

The selection never changes where a batch closes or how many rows a cell holds. A payload
batch keeps its planned layout; the selection marks which rows hold values, and the slots of
unselected rows are unspecified.

- `BatchPlan`, `Placer`, cell capacities and segment offsets are unchanged, so every input to
  placement is still known before decoding.
- A segment whose batch's selection is resolved when its fill runs writes the selected rows; one
  whose selection is unresolved writes every row. Both are correct, so no fill waits for a
  selection, and gating is an optimisation the scheduler can apply or withhold per page.
- Readers already read only selected rows: `FlatRowReader` walks the set bits, `ColumnScan`
  compacts payload batches before a `ColumnReader` exposes them. The public API does not change:
  a column reader still publishes only matching records. The new internal invariant is that no
  consumer reads an unselected slot of a payload batch. A test hook writes a poison value into
  unselected slots so the result-comparing suites catch a reader that does.
- Validity bits of unselected rows are unspecified, and `validity == null` means every
  selected value is present.
- A binary cell measures, and sizes its byte buffer from, the selected rows of segments measured
  after their selection resolved. Unselected rows get an empty view and entry id `-1`, so view
  building (`BinaryBatchValues.ensureViews`) and the dictionary-id check stay well defined.

The alternative is a compacted payload batch (#500's "survivor-rank stitch"): a segment's offset
is the population count of the selection before it, so every segment needs its selection before
it is filled. That brings back waiting fills, page-owned scratch for every payload page (today
only binary and nested pages own theirs) and a second row index in `FlatRowReader`. It saves the
gather `ColumnScan` runs. See the open questions.

## Scheduler changes

**Gated pages.** A payload page whose first segment's batch has an unresolved selection is gated.
`ReadScheduler` keeps gated columns in a fourth set beside `queued`, `pending` and
`unpublished`. A gated column is in neither `queued` nor `pending`, so it does not hold back
pages at higher rows. Resolving `S(b)` calls `ReadScheduler.submit(column)` for each waiting
column, which re-indexes it: its head page rejoins `queued` at its row, below every read-ahead
page, and is submitted next. Among the pages that may be submitted, the lowest row still goes
first.

Strict row order without the gated set deadlocks: the gated head is the lowest row of the read,
and the predicate pages that resolve it start at higher rows. The `pending` barrier has the same
hazard for a payload column whose fetcher waits on a gate (fetch gating, below), so that column
reports itself gated too.

**Liveness.** Today a page starting at most one batch capacity past its column's first
unpublished row is always within the depth. Add one rule: a predicate page holding rows of a
batch some column is gated on is always within the depth. Then the pages that resolve every gate
are admitted, no task waits for another task, and the remaining bound on submission, the pages in
flight, frees up as tasks finish. Gates arise only for the head pages of payload columns, so the
batches waited on are bounded by each payload column's queue (`FETCH_AHEAD` pages) and the page
its fetcher holds. A page spanning several batches is gated on its first; its later segments use
their selection if it is resolved and fill every row otherwise.

**Bounded exchanges.** The merge runs on completion, before the publish, and a completed cell
that finds no room stays in its column's publish order without blocking anything. Resolving a
selection therefore never needs exchange room. The consumer cannot take `b` before payload `b` is
published, payload `b` completes once its fills ran, and fills never wait, so the exchanges add no
cycle.

**Read-ahead depth.** A gated payload column pins the read's slowest first unpublished row, so
the predicate columns read ahead of it within today's bounds: fewer pages or batches than the
pool has threads, within `CAP_BUDGETS` batch budgets. That is the pipeline: payload `b` decodes
while predicate `b + 1 …` decode. The cost is latency per batch: payload `b` starts only once
predicate `b` has completed. Throughput holds while the predicate columns run at least one batch
ahead. Retained memory grows by the predicate cells completed and not yet
consumed, up to the gated columns' queue horizon.

**Column order.** Columns are added in decoded order, payload first, so on equal rows a payload
page is submitted before a predicate page (`scheduleId`). Under gating the predicate columns
take the lower ids. The #500 comment of 2026-09-25 measured the order mattering even without
gating (`FilterBenchmark` `selective`, +2.0 % from #1242's reorder on the drain model).

**Always-match, filter-only skip.** A proven batch resolves at planning, so its payload pages are
never gated, and the filter-only columns' boundary markers need no change.

**Failures and close.** A failure on a predicate column leaves its batches unresolved, and the
consumer polls payload columns first, so a gated payload column would never publish and the
consumer would wait on it. A failure that ends a predicate column resolves its waiting
selections as failed and fails every column of the read, as a failure while submitting does
today. Gated pages are not yet counted as tasks, so `ScheduledColumn.close` is unaffected.

**Row cap.** Unchanged in stages 1 to 4: under a filter the plan holds the cap only through proven
row groups. Selection counts are known before the payload is decoded, which would let a filtered
`head(n)` stop the payload columns once `n` matches are resolved (stage 6).

## Fetch gating on remote files

On a local file a skipped page saves decompression and decode; its fetch is a slice of the
mapping. On a remote file every page group is a request, and a page with no selected row is
bytes downloaded to be dropped.

- An indexed plan knows each page's rows from the OffsetIndex before fetching. The payload
  fetcher asks the gate before resolving a page's bytes. A page whose rows fall in resolved
  batches with no selected row becomes a `PageInfo` that carries its record count and no bytes.
  It is placed as any page is, and its task completes its segments without decoding, as a null
  placeholder is read without decompression. The record count comes from the OffsetIndex, so
  placement does not change.
- A page group is fetched as one request when first accessed. Skipping pages inside it needs the
  group re-cut at fetch time to the span of its still-needed pages (`IndexedFetchPlan`,
  `ChunkHandle`).
- Speculative fetches must not fetch what the gate drops: the one-ahead chunk prefetch, the
  next row group's first-chunk prefetch (narrowed to the predicate columns, which #500 found
  load-bearing for remote latency) and cross-column coalescing (gated payload plans become
  coalesce-unsafe).
- A payload fetch for batch `b` starts once `S(b)` resolves, so the predicate columns must run
  ahead by at least one request's latency of consumption. Today's depth is sized for decode
  threads, not round trips.
- A sequential plan finds its pages by walking headers, so a runtime selection cannot skip its
  I/O within a chunk; decode gating still applies. Skipping a whole chunk needs every batch of the
  row group resolved before the first fetch, which the depth bound does not allow for a row group
  larger than the read-ahead.

## Granularity

| Option | Unit | Placement and layout | Saves | Costs |
| --- | --- | --- | --- | --- |
| Whole batch | a planned batch with no selected row | unchanged | pages and fills wholly inside empty batches | needs clustered survivors |
| Row runs per segment | runs of set bits over a segment's rows, composed with the page mask's intervals (`FlatFill.forEachRun`) | unchanged (positional) | the whole-batch case, per-row fill, whole pages with no survivor | a bitmap walk per segment |
| Selection vector into the fill | index list, compacted output | segment offsets depend on the selection | as row runs, plus the consumer's gather | waiting fills, page-owned scratch, a survivor rank in the readers |

**Recommendation: row runs, positional**, with the whole-batch and whole-page skips as the cases
where a segment or a page has no run. A density threshold, a tuning constant, treats a dense
selection as all rows, so a non-selective filter pays no per-run overhead in the fill. It does not
remove the gating latency; see the open questions.

## Expected wins per encoding

A payload page's work falls in three parts: decompression and the level and id decode, done for
the whole page when its task reads it (`PageDecoder.decodeFlat`, `decodeFlatBinary`); value
preparation at page read for some encodings; and the fill, per kept row. A selection skips the
fill of unselected rows. A page with no selected row skips all three, and on a remote indexed
plan its fetch. Skipping the level and id decode of unselected runs inside a decoded page needs
the run cursor of #726 and the decoder plumbing of #728, which apply to a selection as they do to
a page mask.

| Encoding | Paid at page read whatever the selection | Skipping an unselected row in the fill | Expected gain |
| --- | --- | --- | --- |
| `PLAIN` fixed width | decompression, levels | O(1): offset arithmetic, nulls counted by the SIMD kernel | a load and store per value; small per row |
| `BYTE_STREAM_SPLIT` | decompression, levels | O(1) | a gather of `width` strided bytes per value; larger than `PLAIN` |
| Dictionary, fixed width | ids of the whole page, into scratch | O(1) once the ids exist | the dictionary lookup only; the id decode needs #726 (RLE runs skip by length, bit-packed groups by arithmetic) |
| Dictionary, binary | ids of the whole page | O(1) | writing an entry id; negligible |
| `RLE` booleans | ids of the whole page | O(1) | negligible |
| `DELTA_BINARY_PACKED` | stream header | sequential: passing a value decodes its miniblock | blocks past the page's last selected row are never decoded; otherwise none |
| `ALP` | every vector header | O(1) per vector | a vector with no selected row is not decoded; needs survivors clustered at vector scale |
| `BYTE_ARRAY` `PLAIN` | the length-prefix walk of the whole page (the measure) | O(1) through the byte prefix | the byte copy and the view of each unselected value, and a smaller byte buffer |
| `DELTA_LENGTH_BYTE_ARRAY` | the lengths of the whole page | O(1) | as `BYTE_ARRAY` `PLAIN` |
| `DELTA_BYTE_ARRAY` | every value reconstructed into scratch | O(1) | the copy into the batch only |
| `FIXED_LEN_BYTE_ARRAY`, `INT96` `PLAIN` | decompression, levels | O(1) | the byte copy |
| Nested (stage 5) | levels of the whole page, needed for record starts | per record, through its value range | the values of unselected records; levels are kept in the positional layout |

**Where page skips come from.** The figures below are arithmetic, not measurements. With
survivors scattered uniformly at selectivity `s`, a page of `r` rows has no survivor with
probability about `(1 − s)^r`: for `r = 20,000`, that is 13 % at `s = 0.01 %` and effectively
0 at `s = 1 %`. Scattered survivors therefore yield fill savings only; whole-page skips, and
with them the decompression and the remote I/O, need survivors that cluster (ingestion order,
tenants, time ranges) without the predicate column's page bounds catching them, or files without
a page index (#500 notes the 2022 and later NYC taxi files carry no statistics).

## Stages

| # | Stage | Depends on | Gate before the next |
| --- | --- | --- | --- |
| 0 | Measure the premise | — | the maintainer decides go or stop from the numbers |
| 1 | Batch rendezvous, merge on the completing thread | — | no regression on `FilterBenchmark` and the flat regression benchmark |
| 2 | `ColumnReaders` fragments on cells | 1 | as stage 1, column-reader filter benchmarks |
| 3 | Decode gating for flat payload columns | 1, 2 | a measured win at low selectivity; no regression unfiltered or at high selectivity, on the container and the N300 |
| 4 | Fetch gating for indexed plans on remote files | 3 | bytes and wall time on S3 against stage 3 |
| 5 | Nested payload columns | 3 | measured on a nested filtered read |
| 6 | Filtered `head(n)` from selection counts | 3 | — |

Stages 4, 5 and 6 are independent of each other.

### Stage 0: measure

Per the project's measure-first rule, nothing above is measured. On filtered reads (the
`FilterBenchmark` `selective` case, taxi with a predicate on one column and the rest projected,
1BRC with a station predicate) at selectivities of 0.01 %, 1 %, 10 % and 50 %, scattered and
clustered, measure on the N300:

- the CPU share of payload page tasks, split into decompression, level and id decode, and fill
  (a profile of today's `main` suffices; no code change);
- the fraction of payload pages with no survivor, and the bytes they hold (a script over the
  files and the predicate, independent of the reader).

The first bounds stage 3's win, the second its page skip and stage 4. A low fill share
with few empty pages means the gating latency would cost more than it saves on local files.

### Stage 1: rendezvous and merge off the consumer

`PlannedBatch` from `BatchPlan`, referenced by every cell; the countdown and the merge on the
thread completing the last predicate cell; `FlatRowReader` takes the merged words from the
batch instead of merging on its thread. No gating. Behaviour is unchanged and the consumer
loses the merge, which is useful on its own for multi-column predicates. Tests: the existing
oracles (`DrainSideOracleTest`, `PredicatePathAgreementTest`) and a test that a batch's selection
is resolved before any of its cells is published.

### Stage 2: column-reader fragments on cells

`SelectionEngine` switches from owning to aliasing mode: the predicate columns of a
`ColumnReaders` group run their fragments as their cells complete, as `FlatRowReader`'s do. The
per-row backend stays on the consumer.

### Stage 3: decode gating

The gated set and the liveness rule in `ReadScheduler`; predicate columns first in `scheduleId`;
selection-aware fills in `FlatFill` and `BinaryFill` (runs composed with the page mask, measure
of selected rows); the whole-page skip when every batch a page spans is resolved and selects none
of its rows; the density threshold; failure propagation across the read's columns. Tests: the
result-comparing suites at small batch capacities with poisoned unselected slots; scheduler tests
with `SubmissionObserver` for liveness under skewed page sizes (a payload page spanning many
batches, a predicate page spanning many batches, both); a failure on a predicate column with
gated payload columns; close while columns are gated.

### Stage 4: fetch gating

The page gate in `IndexedFetchPlan`, the "skipped by selection" page kind, page groups re-cut at
fetch time, speculative fetches made gate-aware, gated plans out of cross-column coalescing, the
fetcher reporting itself gated while it waits. Tests: `IoBudget` assertions that no byte of a
skipped page group is fetched; `S3SelectiveReadJfrIT`-style integration tests.

### Stage 5: nested payload columns

`NestedFill` skips the values of unselected records and keeps their levels, so the layer offsets
and the real view are computed as today. `NestedRowReader` has no drain-side filter, so it gains
nothing until #485.

### Stage 6: filtered `head(n)`

The merge counts matches; once `n` are resolved, the plan ends the payload columns' read at that
batch. Today the reader counts matches on the consumer and the payload is decoded to the end of
the batch in which the cap falls, and beyond by the read-ahead.

## Open questions

1. **Positional or compacted payload batches.** Positional keeps placement and fills
   wait-free; compacted removes `ColumnScan`'s gather and would match a callback consumer
   (#1475) that wants dense arrays. Recommendation: positional; revisit when stage 0 or 3 shows
   the gather.
2. **When to gate at all.** Gating trades per-batch latency for skipped work. A dense selection,
   or predicate columns more expensive than the payload, can make it a loss. Options: a per-read
   rule from metadata (predicate bytes against payload bytes), an adaptive switch that stops
   gating after several dense batches, or both. Recommendation: decide from stage 0's numbers.
3. **Depth for remote reads.** Fetch gating needs the predicate columns to run ahead by a
   round trip, which the decode-thread depth does not provide. A separate depth for predicate
   columns of remote reads, or a byte-based bound (#1475 asks the same of in-flight work).
4. **Retained memory.** A payload page of ~900K rows (1BRC, taxi) spans many batches; predicate
   cells completed for those batches stay until consumed. Gating on the first segment only bounds
   the waits, not this retention.
5. **Cascading predicates.** For `a AND b`, `b`'s fills could be gated on `a`'s selection,
   evaluating `b` only on rows `a` kept. That makes predicate columns depend on each other and
   needs an evaluation order (cheapest or most selective first).
6. **Static selections.** A selection known before the read (#3, deletion vectors) resolves
   every batch at planning and gates every column. The rendezvous of stage 1 carries it unchanged.
7. **Sequencing.** Stages 1 to 3 change `ReadScheduler`, the fills and the readers' consumption
   of the merge; #1169 stage 5 (#732) changes the readers. The reader-side change here is small
   (where the merged words come from), so either order works. The head-of-line investigation of
   `ReadScheduler.next()` (#1287 follow-up) touches the same method as the gated set and should
   land first or be folded in.
8. **Observability.** A JFR event for time payload pages spend gated, and the pages a selection
   skipped, alongside `PageDecoded`.

## Not in scope

| Topic | Why |
| --- | --- |
| Morsel-driven parallelism | #500's other half; #1475 |
| Shapes the drain-side compiler refuses | #485 |
| Batch cuts from runtime selections | the plan's inputs must be known before decoding |

## Completion

Fold the end state into `READ_PIPELINE.md` (gating, liveness), `RECORD_FILTERING.md` (the
selection, its boundary entry removed), `VALUE_DECODE.md` (selection-aware fills) and
`FETCH_PLANNING.md` (fetch gating), close #500, and delete this plan.
