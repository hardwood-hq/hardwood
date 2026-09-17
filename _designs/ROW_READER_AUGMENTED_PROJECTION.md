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
- Reading a column outside `p` is unsupported. The guarantee is `p` and nothing beyond it.

A predicate column outside `p` is decoded, so an accessor keyed by its name currently reaches
it rather than raising. That is an artifact of the decode set, not a promise: a caller must
project a column it intends to read. The artifact exists because the record-filter matcher
resolves half of `RecordFilterCompiler`'s leaf kinds, and every nested path, by name through
the reader's own accessors, and because the readers implement name-keyed access on top of the
indexed accessors. Bounding either family by `p` would require the matcher to hold an accessor
surface of its own.

## Model

`ParquetFileReader` resolves the projection twice, through the one construction both read
paths share — the column-reader path passes `completeContainers = false`, since it reads
individual leaves:

- the **payload** columns, the leaves of `p`, which the reader exposes;
- the **augmented** columns, the payload columns plus the leaves `pred` references, which
  the reader decodes.

`ProjectedSchema.createAugmented(schema, p, predicateColumns, completeContainers)` builds the augmented schema
with the payload columns first, in file order, and the predicate-only columns after them,
also in file order. It records the payload count, which `ProjectedSchema#exposedColumnCount()`
returns; every other projection reports its full column count there.

The ordering is what keeps the change local. `RecordFilterCompiler` compiles a predicate
against projected column indices and evaluates it through the reader's own indexed
accessors (`FlatRowReader.java`, where the compiler is handed
`projectedSchema::toProjectedIndex`). Exposed columns keep the indices they would have had
without augmentation, so the compiler is unchanged and its indexed path reaches a
predicate-only column at an index past the exposed range.

The row readers then bound their field surface by `exposedColumnCount()` rather than by the number
of columns they decode: `getFieldCount()` returns it, and `getFieldName(int)` rejects an index
at or past it. Below the top level the same bound is per group: `TopLevelFieldMap.FieldDesc.Struct`
records the ordinals of the children that carry an exposed leaf, and `PqStructImpl` reports
and indexes those alone, so a predicate leaf under a projected struct is decoded and
resolvable by name without becoming a field of it. Everything else — workers, exchanges,
batch arrays, the name-to-index maps, the record matcher — spans the decoded columns.

## Cost

A predicate column outside the projection is decoded for the whole read, including where
statistics settle the predicate over every row group and no record is matched individually.
The decode set is fixed when the workers and exchanges are built, which is before the first
row group is planned, and planning proceeds a file at a time (#1107), so "no work item needs
record-level evaluation" is not a question the reader can answer at that point. Narrowing the
decode by selectivity is late materialization, #500.

The default `ColumnProjection.all()` projection covers every predicate column by
construction, so it decodes exactly what it did before.

## Non-goals

Skipping the decode of payload columns for rows the predicate rejects is late
materialization, tracked in #500, and is not part of this design.
