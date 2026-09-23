# Design: one statistics abstraction for row group and page filtering (#1177)

**Status: Implemented.** Related: #795 (`ALWAYS_MATCH_STATISTICS.md`), #977, #1030, #1107.

## Problem

Row group filtering and page filtering ask the same question at two granularities: what a unit's
statistics prove about a leaf predicate. Three statistics answer it — min/max, the null count and
the definition level histogram — and each is carried per column chunk by `ColumnMetaData`, per page
by the `ColumnIndex`, and per page again by a data page header, which is all a file recording no
page index offers.

A statistic sourced per evaluator rather than per unit is written once per evaluator, and the
granularities then prove different things from the same bytes: the null count was read by two
hand-written sets of rules that did not agree, and the definition level histogram was copied into
both. A third entry point, the inline-statistics drop a file without a page index is read through,
sat outside either.

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
| `InlinePageStats` | `DataPageHeader.statistics()` | `DataPageHeaderV2.numRows()`; for a v1 header, `DataPageHeader.numValues()` on a column with no repeated node above it, and `UNKNOWN_ROW_COUNT` below one |

`UNKNOWN_ROW_COUNT` is `-1`. Every proof that needs a row count yields `MIGHT_MATCH` without one.
A v1 header counts values, and a column with no repeated node above it writes one per row, so
there the value count is the row count. Every leaf that reaches a null count rule is such a column
(see Null counts), so only a page below a repeated node goes without.

Sourcing absorbs the guards against statistics that are not there: a column ordinal the row group
does not carry, absent `ColumnMetaData`, absent `Statistics`, an absent null count or histogram. A
unit that cannot be sourced is a unit whose every statistic is unknown. `PageFilterEvaluator` keeps
its own column ordinal check and absent-`ColumnIndexBuffers` check, since it needs the parsed
indexes to know how many page units there are.

## The three statistics

Each statistic answers a `FilterDecision`.

### Min/max

`UnitStats.minMax(leaf)` is the entry point, and the static factories `MinMaxStats.of` and
`MinMaxStats.ofPage` are the sourcing step: `ChunkStats` and `InlinePageStats` decode the same
`Statistics` shape, `IndexPageStats` the column index's entry for one page.

`MinMaxStats` holds bounds alone. Proving a value predicate on every row takes a proven-zero null
count as well, since a null row satisfies none, so `decideLeaf` takes that as a parameter and
`decide` passes what `NullStats` proves. The null count is sourced once, by `NullStats`, and a
caller cannot leave the conjunction half-applied without saying so.

### Null counts

`NullStats` carries the unit's null count and answers a **leaf** null predicate, one whose
`definitionLevel` equals its `leafDefinitionLevel`:

- `IS NULL` is `CANNOT_MATCH` on `nullCount == 0`, and `ALWAYS_MATCHES` on
  `nullCount == rowCount`.
- `IS NOT NULL` is `CANNOT_MATCH` on `nullCount == rowCount`, and `ALWAYS_MATCHES` on
  `nullCount == 0`.
- A value predicate is `CANNOT_MATCH` on `nullCount == rowCount`, since a null satisfies none of
  them, `NOT_EQ` included. It is tested before the min/max, which on a page the column index
  flags null-only are placeholders rather than bounds.

The rules reading `nullCount == rowCount` are sound for a leaf predicate because
`FilterPredicateResolver.rejectRepeated` refuses a repeated column to a directly named leaf, value
predicates included, so the unit writes exactly one entry per row and a null count equal to the
row count accounts for all of them.

**A group predicate, one whose `definitionLevel` is below its `leafDefinitionLevel`, must not
reach these rules.** `FilterPredicateResolver.resolveNullTarget` answers a group from a leaf below
it, chosen by `leafToAnswerFrom`, and that leaf is repeated whenever the group is a `LIST` or a
`MAP` with no non-repeated leaf beneath it — `rejectRepeated` runs on the directly-named-leaf
branch only. Two things then fail at once. A null count tallies every entry below
`leafDefinitionLevel`, which is the group being absent *or* the group being present with a null
below it, so `nullCount == rowCount` does not prove the group absent; that conflation is the reason
the definition level histogram was introduced. And a repeated leaf writes one entry per element, so
the row count is not the entry count and the comparison means nothing in either direction.

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
which every caller goes through.

The gate follows the schema's structure, not its annotations. Every repeated node adds a
definition level, so a `LIST` or a `MAP` — in the standard encoding and in every legacy one: the
two-level lists, the repeated group named `array` or `<list>_tuple`, `MAP_KEY_VALUE` on either
group — has a repeated node between it and its leaf and passes the gate as a group. A group whose
levels are equal has nothing but required nodes down to its leaf, which is then non-repeated and
null exactly where the group is absent, so the null count answers it as it answers the leaf.

Preferring the histogram for a leaf predicate counts entries directly, where the null-count rules
reach the same answer by way of the non-repeated-leaf argument above.

The length guard is load-bearing for a leaf predicate.
`ResolvedPredicate.IsNotNullPredicate.ofLeaf` carries both definition levels as `0` for a caller
holding no schema, and a histogram indexed at that level would answer about the wrong node. A
histogram whose length is not `leafDefinitionLevel + 1` is therefore refused before it is read,
which sends every such predicate to `nulls()` — safe, because equal levels name a leaf.

## What the evaluators share

Sourcing converges the statistics. It does not merge the evaluators, and only one part of them is
shared.

**The leaf decision is shared.** Which statistic answers which predicate shape is written once, in
a single method every caller hands a leaf to:

```java
FilterDecision decide(ResolvedPredicate leaf, LogContext logContext);
```

`decide` names every leaf predicate type, so a new one does not compile until it states which
statistic answers it — in particular whether a null can satisfy it, which is what the value rule
on `nullCount == rowCount` rests on.

`minMax()`, `nulls()` and `definitionLevels()` are the internals of it, package-private for the
tests that pin each statistic on its own.

`PageDropPredicates.canDropPage` is the third caller, and reads `CANNOT_MATCH` alone: the page it
drops is replaced by a page of nulls that the per-row filter then rejects. That holds only for a
leaf a null fails, so `IS NULL` is not collected among the leaves it drops with.

`decide` builds only what its branch reads: the histogram for a null predicate, and the null count
as well where a leaf predicate falls back to it; the null count and the min/max for a value
predicate. A unit is constructed per page per leaf, so a thousand-page column chunk builds a
thousand of them, and under a null predicate each copies its page's histogram out of the
column index.

**The recursion is not shared.** `AND` and `OR` fold to one `FilterDecision` on the row-group side
and to `RowRanges` on the page side. The two are the same fold over different algebras; the page
fold works on `RowRanges` directly, so each evaluator keeps its own.

**Absence probes are not shared.** Bloom filters and dictionaries are row-group scoped; Parquet
carries no per-page equivalent. They stay in `RowGroupFilterEvaluator`, where the statistics
decision is taken first and one `absent` switch holds what each leaf probes with: the NaN guards,
the exact-bytes rule, the halves a `FLOAT16` dictionary compares. Naming every leaf type there
leaves the compiler to ask what proves a new one's literal absent. A leaf the statistics already
drop reaches no probe, so a bloom filter is read no earlier than the decision needs it. Geospatial
statistics are the same case: `GeospatialStatistics` lives on `ColumnMetaData` alone, so the page
units report it unknown and `GeospatialPredicate` stays a row-group-only decision.

Each evaluator therefore holds its recursion and its own sources of absence, and no
per-predicate-shape leaf dispatch. `PageFilterEvaluator` has one per-page loop for every leaf, and
its `switch` names no null predicate; `decide` routes them.

## What each unit proves

Every proof below is computable from statistics the reader already parses. *Read* means the
evaluator acts on it. *Not read* means the unit answers it and the caller discards the answer:
page filtering keeps a page unless it is `CANNOT_MATCH`, so the always-match half never leaves the
unit — see Boundaries.

| Proof | Row group | Page, column index | Page, inline |
| --- | --- | --- | --- |
| value leaf `CANNOT_MATCH` from min/max | read | read | read |
| value leaf `ALWAYS_MATCHES` from min/max and a zero null count | read | not read | not read |
| value leaf `CANNOT_MATCH` from `nullCount == rowCount` | read | read, also as `nullPages` | read |
| `IS NULL` `CANNOT_MATCH` from `nullCount == 0` | read | read | not collected |
| `IS NULL` `ALWAYS_MATCHES` from `nullCount == rowCount` | read | not read | not collected |
| `IS NOT NULL` `CANNOT_MATCH` from `nullCount == rowCount` | read | read, also as `nullPages` | read |
| `IS NOT NULL` `ALWAYS_MATCHES` from `nullCount == 0` | read | not read | not read |
| group null `CANNOT_MATCH` from the histogram | read | read | no histogram |
| group null `ALWAYS_MATCHES` from the histogram and the row count | read | not read | no histogram |

*Not collected* is the inline path's `IS NULL` exclusion above. *No histogram* is a page header
carrying no size statistics.

Floating-point value leaves are a deliberate non-promise at every granularity: a recorded
`nan_count` of zero rules a `NaN` row out (#1016), but a fully-satisfying interval is still not
promoted to `ALWAYS_MATCHES` (#898).

## Observability

`RowGroupFilterEvent.rowGroupsFullyMatching` reports how many row groups the statistics proved in
full.

## Boundaries

- **Page-level `ALWAYS_MATCHES` is not read.** `PageFilterEvaluator` keeps a page unless it is
  `CANNOT_MATCH`, so the always-match half of a page's decision is computed and discarded. Reading
  it would save per-row filtering inside row groups the statistics left undecided, and nothing
  else: a kept page is fetched and decoded either way. The flag it would set changes value at a
  row, so every column's worker would have to split its batches there — mid-page, since each
  column's page boundaries are its own — and `FlatRowReader` and `NestedRowReader` read the flag
  from one column's batch on the understanding that every column flushes at the same row. Set
  against that, the saving is narrow: for a range predicate on sorted data only the row group
  holding the cutoff is undecided.
- **Bloom filters and dictionaries** prove absence only, are row-group scoped, and stay outside
  `UnitStats`. `RowGroupFilterEvaluator` applies them after the statistics decision.
- **Geospatial statistics** live on `ColumnMetaData` alone, so `GeospatialPredicate` has no page
  unit to source. It stays a row-group-only decision and the page units report it unknown.
- **`nan_count` promoting a full match.** The count rules a `NaN` row out where a file records a
  zero (#1016); letting a fully-satisfying interval promise every row of a floating-point column
  is #898, and lands as a fourth statistic on `UnitStats`.
