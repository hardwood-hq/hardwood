# Design: row-reader augmented projection

**Status: Implemented.** Tracking issue: #1242. The column-reader counterpart is
`_designs/EXACT_COLUMN_READER_FILTERING.md`, whose "augmented projection" model this
extends to the row readers.

## Scope

`buildRowReader().projection(p).filter(pred)` returns the rows satisfying `pred` whether or
not `pred`'s columns are part of `p`. A predicate column outside `p` is decoded so the filter
can be evaluated, and `p` still bounds the row's fields: the column is not one of them.

This holds for the flat and the nested row reader, and for predicates the drain side
compiles as well as those the record matcher evaluates.

## The contract

For a `RowReader` built with projection `p` and filter `pred`:

- The rows returned are exactly those satisfying `pred`.
- `getFieldCount()` is the number of leaves of `p`, and `getFieldName(i)` is defined for
  `0 <= i < getFieldCount()` and names a leaf of `p`. The same holds at every depth: a
  `PqStruct` reports the children of `p`, not the children `pred` reached below it.
- The accessors resolve the leaves of `p` and nothing else. A predicate column outside `p`
  raises exactly as any other unprojected column does, by name and by index, at every depth.

## Model

`ParquetFileReader` resolves the projection twice, through the one construction both read
paths share — the column-reader path passes `completeContainers = false`, since it reads
individual leaves:

- the **payload** columns, the leaves of `p`, which the reader exposes;
- the **augmented** columns, the payload columns plus the leaves `pred` references, which
  the reader decodes.

`ReadProjection.withPredicateColumns(schema, p, predicateColumns, completeContainers)` holds
both as plain projected schemas: `payload()` and `decoded()`, the augmented columns. The
decoded schema lists the payload columns first, in file order, and the predicate-only columns
after them, also in file order, so the payload's columns and top-level fields lead it at the
indices they hold in the payload; the record's constructor rejects a pair where they do not.
A decoded index at or past `payloadColumnCount()` is predicate-only (`isFilterOnly`). A read
without a filter, or whose predicate adds no column, decodes its payload alone.

Everything that decodes — the iterator, workers, exchanges, batch arrays — spans `decoded()`.
The row readers build their accessor state from `payload()` alone: `FlatRowReader`'s
name-to-index map and value arrays span the payload columns, and `NestedRowReader`'s
`NestedBatchDataView` is built over the payload and fed the leading batches.
A predicate-only column has no entry in either, so no accessor reaches it at any depth, and no
accessor tests whether a column is predicate-only. The record matcher evaluates against a
`PredicateView` over the predicate columns instead of the reader (see
`FILTER_ONLY_COLUMN_SKIP.md`). Workers, exchanges and batch arrays span the decoded columns.

## Cost

A predicate column outside the projection has a worker and an exchange for the whole read,
since the decode set is fixed when they are built, before the first row group is planned. It
is read only in row groups statistics leave undecided: in one they prove to match in full it is
neither fetched nor decoded (`FILTER_ONLY_COLUMN_SKIP.md`). Narrowing the
decode by selectivity is late materialization, #500.

The default `ColumnProjection.all()` projection covers every predicate column by
construction, so it decodes exactly what it did before.

## Non-goals

Skipping the decode of payload columns for rows the predicate rejects is late
materialization, tracked in #500, and is not part of this design.
