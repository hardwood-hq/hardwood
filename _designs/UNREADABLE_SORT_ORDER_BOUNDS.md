# Bounds whose sort order the reader cannot read

**Status: Implemented** ([#1179](https://github.com/hardwood-hq/hardwood/issues/1179))

Pruning a row group or a page compares a predicate literal against the recorded `min` / `max`.
That is sound only when the reader compares in the order the bounds were written in. Two shapes
reach the reader where that order is not knowable, and in both pruning declines to act on the
bounds.

Neither is produced by Hardwood's writer. Both arrive on files written elsewhere.

## What is unreadable

A column's bounds are unreadable when either holds.

**The annotation names no order.** parquet-format defines none for `INTERVAL`, `GEOMETRY`,
`GEOGRAPHY`, `VARIANT`, `UNKNOWN`, `LIST` and `MAP`, and states for `INTERVAL` that no
`min` / `max` should be written at all. `StatisticsOrder#supportsBounds` already answers this
question on the write side, which is why Hardwood emits no bounds for such a column.

**The file names an order this build does not recognize.** `parquet.thrift` on the `ColumnOrder`
union: *"If the reader does not support the value of this union, min and max stats for this
column should be ignored."* `ColumnOrderReader` decodes an unrecognized member to
`ColumnOrder.UNKNOWN`. This applies to every physical type, not only the binary ones.

## Where the decision is made

Readability is a property of the file that wrote the bounds, so it is decided per file, when
`RowGroupIterator` plans that file. `BoundsReadability.of` reads the file's own schema and
`column_orders` and answers for each of its leaves, indexed by that file's leaf ordinals. In a
multi-file read each file is decided on its own terms: a later file may declare an order the
first does not, or place the same column at a different ordinal.

The answer is carried on `FileColumnOrdinals`, beside the filter already translated to the
file's ordinals, so the two are always read in the same ordinal space. It reaches the pruning
paths as follows:

- `RowGroupFilterEvaluator#decideRowGroup` and `PageFilterEvaluator#computeMatchingRows` take it
  as a parameter and pass it to `MinMaxStats`.
- The inline page-statistics path withholds a column's AND-necessary leaves from
  `SequentialFetchPlan` where the file's bounds for that column are unreadable, so
  `PageDropPredicates#canDropPage` never receives them.

`MinMaxStats` consults it once it has established that the file wrote a pair at all, and before
decoding the pair. Unreadable bounds yield `MinMaxStats.NullCountOnlyStats` with the
`UNKNOWN_SORT_ORDER` reason, alongside the existing `DEPRECATED_SORT_ORDER`, `NOT_A_NUMBER` and
`INVERTED`, and are reported the same way. A column that carries no bounds reports nothing,
which is what a conforming writer produces for an unordered annotation.

An ordinal outside the file's schema is a wiring error, and `BoundsReadability` throws on it
rather than answering either way.

## What stays active

Bloom filters and dictionaries answer membership by testing exact stored values, which does not
depend on how those values order. An unreadable-bounds column keeps both shortcuts, and keeps
record-level evaluation. The null count survives too, since counting nulls needs no ordering.
Only the min / max half is withheld.

`Statistics` keeps its bounds on the record either way. They are what the file holds, so
`hardwood inspect` and the other metadata surfaces continue to report them; it is pruning that
declines to act on them.

## Annotation ordering, in one place

The read side answers "does this annotation name an order" with an exhaustive switch, mirroring
`StatisticsOrder#supportsBounds` on the write side. Neither delegates to the other: the writer
asks whether to record bounds, the reader whether to trust bounds already recorded, and a column
type could in principle answer differently. Both switches are exhaustive over `LogicalType`, so
an annotation added later fails to compile until each side states its answer.
