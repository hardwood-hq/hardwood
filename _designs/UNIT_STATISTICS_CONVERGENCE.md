# Design: one statistics abstraction for row group and page filtering (#1177)

**Status: In progress** (stage 1 implemented). Related: #795 (`ALWAYS_MATCH_STATISTICS.md`), #977, #1030, #1107.

## Problem

Row group filtering and page filtering ask the same question at two granularities: what a unit's
statistics prove about a leaf predicate. Three statistics answer it — min/max, the null count and
the definition level histogram — and each is carried per column chunk by `ColumnMetaData` and per
page by the `ColumnIndex`.

A statistic sourced per evaluator rather than per unit has to be written twice, and the two
granularities then come to prove different things from the same bytes. The row-group side derives
`ALWAYS_MATCHES`; `PageFilterEvaluator.computeMatchingRows` returns one `RowRanges` and has no way
to express it.

A third kind of unit sits outside the two evaluators: `SequentialFetchPlan` drops pages from the
inline `DataPageHeader.statistics` through `PageDropPredicates.canDropPage`, which routes to
`MinMaxStats.of` alone. A file with no column index reaches page pruning only here.

## The unit

Sourcing resolves *where a statistic came from* once, and each statistic then has one
implementation of *what it proves*.

```java
sealed interface UnitStats permits ChunkStats, IndexPageStats, InlinePageStats {

    /// Rows the unit covers, or UNKNOWN_ROW_COUNT when the source does not carry it.
    long rowCount();

    /// What this unit's statistics prove about one leaf predicate. The single entry point;
    /// see "What the evaluators share".
    FilterDecision decide(ResolvedPredicate leaf, LogContext logContext);

    // The statistic decide() routes to, package-private so each can be pinned on its own.

    MinMaxStats minMax(ResolvedPredicate leaf);

    NullStats nulls();

    DefinitionLevelStats definitionLevels();

    /// Narrows the position enclosing the unit to the unit itself — a column for a chunk, a page
    /// index for a page — for a discard to name.
    LogContext locate(LogContext enclosing);
}
```

| Implementation | Sourced from | `rowCount()` |
| --- | --- | --- |
| `ChunkStats` | `ColumnMetaData.statistics()`, `ColumnMetaData.sizeStatistics()` | `RowGroup.numRows()` |
| `IndexPageStats` | `ColumnIndex` and `OffsetIndex` at one page index | `pages.get(i + 1).firstRowIndex() - pages.get(i).firstRowIndex()`, and `rowGroupRowCount - firstRowIndex` for the last page |
| `InlinePageStats` | `DataPageHeader.statistics()` | `DataPageHeaderV2.numRows()`; `UNKNOWN_ROW_COUNT` for a v1 header, which carries `num_values` only |

`UNKNOWN_ROW_COUNT` is `-1`. Every proof that needs a row count yields `MIGHT_MATCH` without one,
so a v1 inline header proves no more than its min/max.

Sourcing absorbs the guards against statistics that are not there: a column ordinal the row group
does not carry, absent `ColumnMetaData`, absent `Statistics`, an absent null count or histogram. A
unit that cannot be sourced is a unit whose every statistic is unknown. `PageFilterEvaluator` keeps
its own column ordinal check and absent-`ColumnIndexBuffers` check, since it needs the parsed
indexes to know how many page units there are.

## The three statistics

Each statistic answers a `FilterDecision`.

### Min/max

`UnitStats.minMax(leaf)` is the entry point, and the static factories `MinMaxStats.of` and
`MinMaxStats.ofPage` are the sourcing step of `ChunkStats` and `IndexPageStats`. `InlinePageStats`
decodes the same `Statistics` shape as `ChunkStats`.

`MinMaxStats` keeps its own null count. `decideLeaf` composes the interval proof with a proven-zero
null count, and splitting that conjunction across two objects would return null rows to a caller
that asked for a value. The copy cannot disagree with `NullStats`, both being sourced from the same
unit in the same call.

### Null counts

`NullStats` carries the unit's null count and answers a **leaf** null predicate, one whose
`definitionLevel` equals its `leafDefinitionLevel`:

- `IS NULL` is `CANNOT_MATCH` on `nullCount == 0`.
- `IS NOT NULL` is `CANNOT_MATCH` on `nullCount == rowCount`, and `ALWAYS_MATCHES` on
  `nullCount == 0`.
- A value predicate is `CANNOT_MATCH` on `nullCount == rowCount`, since a null satisfies none of
  them, `NOT_EQ` included. It is tested before the min/max, which on a page the column index
  flags null-only are placeholders rather than bounds.
- `IS NULL` is `ALWAYS_MATCHES` on `nullCount == rowCount`.

The last rule arrives in stage 3; the others hold from stage 1. The rules reading
`nullCount == rowCount` are sound for a leaf predicate because
`FilterPredicateResolver.rejectRepeated` refuses a repeated column to a directly named leaf, value
predicates included, so the unit writes exactly one entry per row and a null count equal to the
row count accounts for all of them.

**A group predicate must not reach these rules.** `FilterPredicateResolver.resolveNullTarget`
answers a group from a leaf below it, chosen by `leafToAnswerFrom`, and that leaf is repeated
whenever the group is a `LIST` or a `MAP` with no non-repeated leaf beneath it — `rejectRepeated`
runs on the directly-named-leaf branch only. Two things then fail at once. A null count tallies
every entry below `leafDefinitionLevel`, which is the group being absent *or* the group being
present with a null below it, so `nullCount == rowCount` does not prove the group absent; that
conflation is the reason the definition level histogram was introduced. And a repeated leaf writes
one entry per element, so the row count is not the entry count and the comparison means nothing in
either direction.

On the page side `ColumnIndex.nullPages()[i]` is the same fact as `nullCount == rowCount` for that
page, and the two must agree. `IndexPageStats` sources the null count from `nullCounts()` and treats
a set `nullPages` entry as `nullCount == rowCount()`, so one rule serves both.

### Definition level histograms

`DefinitionLevelStats` carries one histogram, holding one bucket per definition level up to the
leaf's maximum. A null predicate on a node in the leaf's path splits the buckets at the node's own
level: everything below was written with the node absent, everything at or above with it present.

- `CANNOT_MATCH` when no bucket on the predicate's side of the split holds an entry.
- `ALWAYS_MATCHES` when no bucket on the other side holds an entry **and** the histogram totals
  `rowCount()` entries, which is what makes "every entry" mean "every row".
- `MIGHT_MATCH` when the file omits the histogram, or wrote one whose length is not
  `leafDefinitionLevel + 1`.

A leaf null predicate is the group case with `definitionLevel == leafDefinitionLevel`: `IS NULL`
then scans buckets `[0, leafDefinitionLevel)` and `IS NOT NULL` scans the single bucket at
`leafDefinitionLevel`. So one histogram rule serves both, and the routing is:

> A group predicate is answered from `definitionLevels()` alone, and is `MIGHT_MATCH` where the
> histogram is absent or ill-formed. A leaf predicate is answered from `definitionLevels()` where
> the histogram is present and well-formed, and falls back to `nulls()` otherwise.

The gate is `definitionLevel < leafDefinitionLevel`, which `group()` on the two null predicates
tests, because it is what makes the fallback legitimate. It is tested once, in `UnitStats.decide`,
which both evaluators call.

Preferring the histogram for a leaf predicate counts entries directly, where the null-count rules
reach the same answer by way of the non-repeated-leaf argument above.

The length guard is load-bearing once leaf predicates take this path.
`ResolvedPredicate.IsNotNullPredicate.ofLeaf` fabricates both definition levels as `0` for a caller
holding no schema, and a histogram indexed at a fabricated level would answer about the wrong node.
A histogram whose length is not `leafDefinitionLevel + 1` is therefore refused before it is read,
which sends every such predicate to `nulls()` — safe, because a fabricated pair is equal and so
names a leaf.

## What the evaluators share

Sourcing converges the statistics. It does not merge the evaluators, and only one part of them is
shared.

**The leaf decision is shared.** Which statistic answers which predicate shape is written once, in
a single method both evaluators hand every leaf to:

```java
FilterDecision decide(ResolvedPredicate leaf, LogContext logContext);
```

`decide` names every leaf predicate type, so a new one does not compile until it states which
statistic answers it — in particular whether a null can satisfy it, which is what the value rule
on `nullCount == rowCount` rests on.

`minMax()`, `nulls()` and `definitionLevels()` are the internals of it, package-private for the
tests that pin each statistic on its own. `PageDropPredicates.canDropPage` is the third caller, and
routing through `decide` is what turns its boolean drop into the same three-valued answer.

`decide` builds only what its branch reads: one statistic for a null predicate, the null count and
the min/max for a value predicate. A unit is constructed per page per leaf, so a thousand-page
column chunk builds a thousand of them.

**The recursion is not shared.** `AND` and `OR` fold to one `FilterDecision` on the row-group side
and to a pair of `RowRanges` on the page side. The two are the same fold over different algebras;
the page fold works on `RowRanges` directly, so each evaluator keeps its own.

**Absence probes are not shared.** Bloom filters and dictionaries are row-group scoped; Parquet
carries no per-page equivalent. They stay in `RowGroupFilterEvaluator`, applied on top of the
statistics decision, and consolidate within that class rather than across the two. Geospatial
statistics are the same case: `GeospatialStatistics` lives on `ColumnMetaData` alone, so
`IndexPageStats` reports it unknown and `GeospatialPredicate` stays a row-group-only decision.

Each evaluator therefore holds its recursion and its own sources of absence, and no
per-predicate-shape leaf dispatch. `PageFilterEvaluator` has one per-page loop for every leaf, and
its `switch` names no null predicate; `decide` routes them.

## Three-valued page filtering

The asymmetry the issue asks about is not inherent to page filtering. Everything downstream of the
page evaluator already carries a per-page always-match flag:

- `ColumnWorker` fills `filterAlwaysMatchesBuffer[slot]` per page, and publishes the current batch
  when a page's flag differs from the batch's, so batches are already split on the boundary.
- `SequentialFetchPlan` already threads a `RowRanges` per column and asks it `overlapsPage(first,
  last)` and `maskForPage(first, last)`.
- `PageInfo` already carries the resulting `PageRowMask`.

Only the producer is missing. `PageFilterEvaluator.computeMatchingRows` returns one `RowRanges` and
collapses every statistic to a boolean drop.

### Two range sets

`computeMatchingRows` returns `matching` and `alwaysMatching`, with `alwaysMatching ⊆ matching`.
Each leaf fills two `boolean[]` from the per-page `FilterDecision` — `keep` on anything but
`CANNOT_MATCH`, `always` on `ALWAYS_MATCHES` — and both become `RowRanges` through
`RowRanges.fromPages`.

Composition is `FilterDecision.and` and `or` lifted to sets, which `RowRanges` already implements:

| | `matching` | `alwaysMatching` |
| --- | --- | --- |
| `AND` | `intersect` | `intersect` |
| `OR` | `union` | `union` |

### Threading it down

`alwaysMatching` travels the path `matching` already takes:
`RowGroupIterator.SharedRowGroupMetadata` → `SequentialFetchPlan` → `PageInfo`.

`RowRanges` gains `containsPage(long pageFirstRow, long pageLastRow)`, the full-containment
counterpart of `overlapsPage`. A page is flagged when its whole row span is contained, which is
conservative for a masked page and needs no reasoning about which rows the mask kept.

`PageInfo` gains `alwaysMatches()`. `ColumnWorker` reads the flag from the `PageInfo` in hand rather
than from `pageSource.isCurrentFilterAlwaysMatches()`, so the value is fixed by the plan that
produced the page.

The row-group decision seeds the page decision instead of being the only source of it: an
`ALWAYS_MATCHES` row group marks every page, and a `MIGHT_MATCH` row group can still have most of
its pages marked. This is #1107's argument one level down, and it is the follow-up
`ALWAYS_MATCH_STATISTICS.md` records under "Boundaries".

## What becomes reachable

Each of these is computable from statistics the reader already parses. A cell names the stage that
brings the proof to that unit, or `prior` where the unit had it before this design. Every staged
cell is an existing rule reaching a unit it did not have, except `IS NULL` `ALWAYS_MATCHES`, which
is a new rule.

| Proof | Row group | Page |
| --- | --- | --- |
| value leaf `ALWAYS_MATCHES` from min/max and a zero null count | prior | stage 2 |
| value leaf `CANNOT_MATCH` from `nullCount == rowCount` | stage 1 | prior, as `nullPages` |
| `IS NOT NULL` `ALWAYS_MATCHES` from `nullCount == 0` | prior | stage 2 |
| `IS NULL` `CANNOT_MATCH` from `nullCount == 0` | prior | prior |
| `IS NOT NULL` `CANNOT_MATCH` from `nullCount == rowCount` | prior | prior, as `nullPages` |
| `IS NULL` `ALWAYS_MATCHES` from `nullCount == rowCount`, leaf predicates only | stage 3 | stage 3 |
| group null `CANNOT_MATCH` from the histogram | prior | prior |
| group null `ALWAYS_MATCHES` from the histogram and the row count | prior | stage 2 |

Floating-point value leaves remain a deliberate non-promise at both granularities: NaN sits outside
the min/max ordering and `nan_count` is not consumed (#898).

## Observability

`RowGroupFilterEvent.rowGroupsFullyMatching` reports the row-group half. `PageFilterEvent` gains
`pagesFullyMatching` alongside `pagesKept` and `pagesSkipped`, emitted from the same site in
`RowGroupIterator`.

## Implementation stages

1. **Sourcing.** Introduce `UnitStats`, `NullStats` and `DefinitionLevelStats` over `ChunkStats`
   and `IndexPageStats`. Both evaluators consult them, and a leaf null predicate is answered from
   `nulls()` alone. It changes the reach of one proof, the table's `stage 1` cell: the value leaf
   `CANNOT_MATCH` on `nullCount == rowCount`, which the page side applies and the shared routing
   hands to row groups with it. Every other proof holds as before, and the existing assertions
   pass unchanged; that is the acceptance criterion.
2. **Three-valued page filtering.** Two `RowRanges` out of `PageFilterEvaluator`,
   `RowRanges.containsPage`, `PageInfo.alwaysMatches()`, `PageFilterEvent.pagesFullyMatching`.
   `InlinePageStats` joins here, which is what gives a file with no column index the same proofs.
   The table's `stage 2` cells arrive with it, and none of them is a new rule:
   `MinMaxStats.decideLeaf`, the `nullCount == 0` rule and the histogram rule are all written and
   reach a page unit for the first time. The channel and what flows through it land together, so
   each is tested by the other.
3. **`IS NULL` proven in full.** `ALWAYS_MATCHES` on `nullCount == rowCount` for a leaf predicate,
   the one row of the table that is a new rule. Leaf null predicates start preferring
   `definitionLevels()` here, since a well-formed histogram proves the same thing by counting
   entries. It is independent of stage 2, being a rule `NullStats` gains rather than a unit it
   reaches.

Stage 1 carries one tripwire beyond the existing suite: a group null predicate on a `LIST`, against
a file carrying no definition level histogram, must stay `MIGHT_MATCH` at both granularities. That
is the case where the leaf is repeated and the null count answers a different question, and it is
the one a fallback written the obvious way gets wrong.

## Boundaries

- **`ColumnReader` / `SelectionEngine`.** `computeSelection` has an every-record fast path;
  feeding it from a page-level proof is the remaining half of the #795 follow-up and is not part
  of this work.
- **Bloom filters and dictionaries** prove absence only, are row-group scoped, and stay outside
  `UnitStats`. `RowGroupFilterEvaluator` keeps applying them after the statistics decision.
- **Geospatial statistics** live on `ColumnMetaData` alone, so `GeospatialPredicate` has no page
  unit to source. It stays a row-group-only decision and `IndexPageStats` reports it unknown.
- **`nan_count`** would let a floating-point leaf promise a full match. It is parsed and written
  but not consumed; consuming it is #898 and lands as a fourth statistic on `UnitStats`.
