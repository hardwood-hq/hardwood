# Design: always-match statistics decisions (#795)

**Status: Implemented** (row-group granularity; page-level pre-approved ranges are a
follow-up, see "Boundaries" below).

## Problem

Statistics evaluation is drop-only: `StatisticsFilterSupport.canDropLeaf`,
`RowGroupFilterEvaluator.canDropRowGroup`, and `PageDropPredicates.canDropPage` answer
only "can no rows match?". Every surviving unit is then evaluated row by row — the
compiled `RowMatcher` on the record path, per-column `ColumnBatchMatcher`s on the
drain-side path.

For a range predicate over a sorted or clustered column, the statistics of most
*surviving* row groups prove the opposite extreme: every row matches (the whole
`[min, max]` interval satisfies the predicate and the null count is zero). Per-row
evaluation over those groups reproduces a foregone conclusion at full decode-side cost.

## The decision model

`StatsDecision` (internal.predicate) is the three-valued extension of the boolean drop:

| decision         | meaning                             | today's equivalent |
|------------------|-------------------------------------|--------------------|
| `CANNOT_MATCH`   | no row matches; skip the unit       | `canDrop == true`  |
| `MIGHT_MATCH`    | undecided; evaluate rows            | `canDrop == false` |
| `ALWAYS_MATCHES` | every row matches; skip evaluation  | — (new)            |

`canDropRowGroup` is now defined as `decideRowGroup(...) == CANNOT_MATCH`; there is one
evaluation implementation, not two.

### Leaf rule

A value leaf is `ALWAYS_MATCHES` when all of:

- min and max are present and trusted (deprecated unsigned min/max fields are already
  nulled out by `MinMaxStats.of`), and
- every value in `[min, max]` satisfies the operator
  (`StatisticsFilterSupport.alwaysMatches` / `alwaysMatchesCompared`; for `IN`,
  the single-point case `min == max ∈ values`), and
- `null_count == 0` — a null row satisfies no value predicate.

`IS NOT NULL` is `ALWAYS_MATCHES` on `null_count == 0` alone. Everything else that
cannot be proven is `MIGHT_MATCH`, which preserves today's behavior exactly.

Truncated (inexact) bounds need no special handling: truncation only widens
`[min, max]`, and a predicate satisfied by the widened interval is satisfied by the
actual values.

### Deliberate non-promises

- **Floating point** (`FLOAT`, `DOUBLE`, `FLOAT16`): NaN sits outside the min/max
  ordering and `nan_count` is not consumed (#607), so a fully-satisfying interval may
  still hide non-matching NaN rows. FP value leaves never yield `ALWAYS_MATCHES`.
- **`IS NULL`**: the null count tallies leaf values, not rows, so `null_count ==
  numRows` does not prove every row's leaf is null for nested columns.
- **Bloom filters** prove absence only; they can force `CANNOT_MATCH` but never upgrade
  a decision.
- **Composition**: `AND` is `ALWAYS_MATCHES` only when every child is; `OR` when any
  child is; empty composites stay `MIGHT_MATCH`.

## Consumption (row-group granularity)

`RowGroupIterator.filterRowGroups` evaluates the tri-state per row group: `CANNOT_MATCH`
groups are dropped (unchanged), and each surviving `WorkItem` carries
`filterAlwaysMatches`.

Two consumers:

1. **Wholesale**: when every work-list row group is `ALWAYS_MATCHES`
   (`RowGroupIterator.isFilterSatisfiedByStatistics`), `FlatRowReader.create` /
   `NestedRowReader.create` treat the read as unfiltered — no matcher compilation, no
   `FilteredRowReader` wrapper, and `maxRows` caps scanned rows directly since
   scanned == matching.
2. **Per-batch, drain-side**: batches must be homogeneous for a batch-level skip, so
   the drain flushes the current batch when a page crosses a row-group boundary that
   changes the flag — the same mechanism as the existing file-boundary flush, and
   applied **uniformly across all workers of a projection** so batches stay
   row-aligned (a per-column flush would desynchronize the merge). For an
   always-matching batch the worker skips `ColumnBatchMatcher.test` and writes the
   all-ones mask a matcher would have produced; `Batch.filterAlwaysMatches` records
   the proof for downstream consumers.

The flag travels retriever → drain through a per-slot buffer written alongside
`fileNameBuffer[slot]`, under the same happens-before chain.

`RowGroupFilterEvent` gains `rowGroupsFullyMatching` so the effect is observable in JFR.

## Boundaries

- **Page-level pre-approved ranges** (skip evaluation inside surviving pages whose
  ColumnIndex entry is `ALWAYS_MATCHES`) extend `RowRanges` and compose with the
  intra-page skip-decode work (#728); follow-up to #795.
- **`ColumnReader` / `SelectionEngine`**: `computeSelection` already has an every-record
  fast path (`-1`); feeding it from `Batch.filterAlwaysMatches` is part of the same
  follow-up.
- **Record-matcher path** (`FilteredRowReader`): benefits from the wholesale case only;
  the wrapper has no row-group visibility for per-group skips.
- The row-group decision is exactly the trigger condition the rewrite capability's
  byte-copy fast path needs (#791, item 6).
