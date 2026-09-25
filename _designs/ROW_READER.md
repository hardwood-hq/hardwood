# Row reader

How `RowReader` presents the published batches of the read pipeline as rows: the two reader implementations and how one is chosen, how an accessor addresses a value, how rows are iterated across batches and files, and how `head`, `tail` and `skip` position the read when no filter is present. The pipeline that produces the batches (workers, exchanges, batch sizing, multi-file planning, `ReaderConfig`) is in [READ_PIPELINE.md](READ_PIPELINE.md); how levels become nested batches is in [NESTED_DECODE.md](NESTED_DECODE.md); leaf decoding is in [VALUE_DECODE.md](VALUE_DECODE.md), and annotation, timestamp and Variant accessor semantics are in [LOGICAL_TYPES.md](LOGICAL_TYPES.md). A filtered row read (augmented projection, matchers, and `head`/`skip` over the matching rows) is in [RECORD_FILTERING.md](RECORD_FILTERING.md); the column-oriented API is in [COLUMN_READER.md](COLUMN_READER.md). The user-facing API is in [docs/content/how-to/row-reader.md](../docs/content/how-to/row-reader.md) and [docs/content/reference/accessors.md](../docs/content/reference/accessors.md).

## Flat and nested readers

`ParquetFileReader.createRowReader` builds one of two readers:

| Schema | Reader | Accessors served by |
|---|---|---|
| `FileSchema.isFlatSchema()`: every top-level field is a primitive and no column has a repetition level above zero | `FlatRowReader` | Typed arrays of the current batch, read directly |
| Anything else | `NestedRowReader` | `NestedBatchDataView` over a `NestedBatchIndex`, and flyweights for nested values |

The test is asked of the file schema (the first file's, in a multi-file read), not of the projection. Projecting only primitive top-level columns from a file that has a struct anywhere yields a `NestedRowReader`.

Both are `final`, implement `RowReader` through the internal `FileAwareRowReader`, and share no base class, though their iteration code is similar. `FlatRowReader` keeps every hot method in one concrete class so the JIT sees monomorphic call sites and inlines `hasNext`, `next` and the primitive accessors into the caller's loop; a base class carrying the nested path's delegation would put that indirection on the flat path. A change that pulls shared behaviour up into a superclass needs that trade-off argued again.

**Wiring.** Each reader's `create` builds one column worker and one `BatchExchange` in recycling mode per decoded column, all drawing from one `RowGroupIterator`, starts the workers and loads the first batch. The reader owns the iterator: no worker does, so the reader releases it on close, and that release is what stops the owning `ParquetFileReader` tracking it. Batch size is resolved once by `ParquetFileReader.resolveBatchSize`, the same funnel the column readers use, so a projection batches the same whichever reader reads it.

**Flat accessor state.** At construction `FlatRowReader` precomputes, per payload column, the physical type, the column schema, a `LeafKind` for `getValue` and whether `getString` may read it. Per batch it holds the column's value array and validity bitmap. A column whose batch has no nulls points its validity slot at a shared all-ones sentinel sized for the largest batch, so the per-row null test is one word load and mask with no null guard.

**Nested accessor state.** The nested workers run in `ALL_ITEMS` index mode and compute record offsets, multi-level offsets and element validity on the drain thread before publishing (see [NESTED_DECODE.md](NESTED_DECODE.md)). On a batch transition `NestedRowReader` only assembles a `NestedBatchIndex` from those fields, compacting the layer-indexed offsets to rep-level-indexed ones. `TopLevelFieldMap`, built once per reader from the projection, describes every projected top-level field as a sealed `FieldDesc`: `Primitive`, `Struct`, `ListOf`, `MapOf` or `Variant`, each carrying the projected column indices and definition-level thresholds its accessors need. `NestedBatchDataView.setRowIndex` caches each column's value index for the current record, and the flyweights (`PqStructImpl`, `PqListImpl`, `PqMapImpl`, the primitive `Pq*ListImpl`s, `PqVariantImpl`) navigate the batch arrays through the descriptors without assembling a record tree.

A filter is installed inside the reader; which path evaluates it is in [RECORD_FILTERING.md](RECORD_FILTERING.md).

Tests: `NestedSchemaTest`, `PqRowApiTest`, `DrainSideRowReaderTest`.

## Accessor addressing

Every accessor exists by name and by index (`StructAccessor`). Both resolve against the payload projection only, so a column the filter added is not reachable at any depth; the prefix invariant that makes this hold is in [RECORD_FILTERING.md](RECORD_FILTERING.md#augmented-projection).

### Index space

An index is a position among the projected children of the accessor, in request order: children are ordered by the first name in `ColumnProjection.columns` that selects a leaf under them, and children one name selects keep file schema order among themselves. `getFieldCount()` and `getFieldName(int)` report the same space.

Each schema node has one position. The row is a tree addressed by simple names one level at a time, and several names can lead into the same node (`address.zip`, `address.city`, `address`), so repeated and overlapping names merge into it rather than add positions. `ColumnReaders` addresses flat leaf columns by full path and can keep every name's positions instead (see [COLUMN_READER.md](COLUMN_READER.md#index-order)); the two agree for a projection of distinct top-level columns.

The order lives in `ProjectedSchema` and applies to exposure only. Projected column indices, which address workers, exchanges and batches, stay in file schema order, and so does the decoded projection a filter extends (see [RECORD_FILTERING.md](RECORD_FILTERING.md#augmented-projection)); a reader maps between the two once, when it is built or when a batch is installed, never per value. Views the predicate builds over its own columns resolve indices through the same `ProjectedSchema`, so they agree with the view they index.

A name resolves to exactly one schema node: a simple name to the top-level field of that name, a dotted name to the node at that path. A nested node has no other name, since a leaf's own name can recur in several structs.

| Accessor | Index counts | Name resolves |
|---|---|---|
| `FlatRowReader` | Projected leaf columns (all top-level) | Leaf name, through a `StringToIntMap` built at construction |
| `NestedRowReader` | Projected top-level fields | Top-level field name, through `TopLevelFieldMap` |
| `PqStruct` | Projected children of that struct | Child name, through the `Struct` descriptor's own map |

The index is not `ColumnSchema.columnIndex()`, the leaf's position in the file schema. The two agree only for a flat file read with `ColumnProjection.all()`; under a projection or on a nested file they differ, and a leaf index passed to an accessor reads another field or fails.

Names are single path segments. Dot notation belongs to `ColumnProjection`: `columns("address.city")` projects one leaf, and the row reaches it as `getStruct("address").getString("city")`; `getString("address.city")` on the row raises as an unprojected name. A struct reached through a narrowed projection reports only the children the projection reaches. Row assembly projects with `completeContainers = true`, so projecting any part of a `MAP` pulls in its key column and projecting any part of a `VARIANT` pulls in all its leaves.

### Values and nullness

`FlatRowReader` reads `array[rowIndex]`. In a nested batch a top-level primitive reads `values[offsets[row]]`; a struct is null when the definition level of its first projected leaf is below the struct's own maximum; a list or map is null or empty by its definition levels against the descriptor's `nullDefLevel` and element (or entry) level. Elements of a list or map of structs are position-mode `PqStructImpl`s fixed at a value index; a top-level struct is record-mode, resolving its value index from the row.

**Flyweight lifetime.** A `PqStruct`, `PqList` or `PqMap` reads the batch it was obtained from on every call and copies nothing. The reader recycles that batch to its exchange when it loads the next one, and the worker then overwrites it, so a flyweight held past `next()` can read another row's data without any error. The public JavaDoc states the flyweights are not safe to hold across `next()`. Untested.

### Failures

The user-facing contract is in [accessors.md](../docs/content/reference/accessors.md#null-handling). The design points behind it:

| Misuse | Raises | Where |
|---|---|---|
| Name outside the projection | `IllegalArgumentException` | Name map lookup |
| Index at or past `getFieldCount()` | `IndexOutOfBoundsException` (an `ArrayIndexOutOfBoundsException` on the row accessors) | Array access |
| Primitive accessor on a null value | `NullPointerException` naming the column | Validity test |
| Accessor of the wrong physical type or group kind | An unchecked exception of unspecified type | See below |
| `getString` on a column that does not hold text | `IllegalArgumentException` | `LogicalAccessorKind` |

The primitive accessors do no type check of their own: on `FlatRowReader` and at the top level of `NestedRowReader` the cast of the batch's value array (or of the field descriptor) is the check, and a mismatch surfaces as `ClassCastException`; a primitive accessor by index on a top-level group of `NestedRowReader` fails with `ArrayIndexOutOfBoundsException`. `PqStructImpl` checks the child's descriptor and raises `IllegalArgumentException`. The user docs leave the exception type unspecified so the hot path carries no branch for a programming error; giving mismatches a specified exception is a design change. The messages `FlatRowReader` and `NestedBatchDataView` compose are prefixed with the current file name through `ExceptionContext`; the name-lookup and shape failures of `TopLevelFieldMap` and the `PqStructImpl`, `PqListImpl` and `PqMapImpl` flyweights carry no file name.

Tests: `ColumnProjectionTest`, `ProjectionOrderTest`, `PqStructByIndexTest`, `PqRowApiTest`, `TextAccessorTest`, `TypedAccessorsIssue445Test`.

## Iterating batches and file boundaries

**`build()` loads the first batch.** Each reader's `initialize()` polls the first batch before `build()` returns, so a failure in the first batch is raised from `build()` and an empty relation is known at build time.

**`hasNext()` loads, `next()` commits.** Only `hasNext()` moves to a new batch. Unfiltered, `next()` advances within the current batch and throws `NoSuchElementException` at its end rather than loading the next one; on a filtered batch `hasNext()` parks the matching row and `next()` only commits it, throwing `NoSuchElementException` when nothing is parked. The protocol is therefore `hasNext()` before every `next()`. The check at the batch end exists because the value arrays are sized to the batch capacity: without it `next()` would walk into the stale tail, where the all-present sentinel lets an accessor return a value from an earlier batch.

**The flat row loop is a bound check.** `FlatRowReader.hasNext` and `next` compare the row index against `plainLimit`, which holds the batch's row count while the plain cursor serves it and `0` otherwise. Everything else (loading a batch, drain-side bit scans, per-row matching) sits in out-of-line slow-path methods, so an unfiltered loop, or a batch statistics proved to match, inlines to a counter and a compare. A per-batch decision added to `hasNext` or `next` themselves breaks this.

**Loading a batch.** Both readers do the same steps:

1. Recycle the previous step's batch of every column to its exchange.
2. Poll the payload columns, then the filter-only columns unless statistics proved the step (see [RECORD_FILTERING.md](RECORD_FILTERING.md#always-match-row-groups)).
3. Check lockstep: every column's batch has the first column's record count, or `IllegalStateException`. A column that ends while the first has produced a batch is also an `IllegalStateException`.
4. At the end of the stream, check every exchange for a worker error before reporting exhaustion, so a failure that ended a worker is raised instead of a short read.

A schema with no columns yields an empty relation: there is no column to take a record count from.

**File boundaries.** Every column worker flushes its batch in progress when the page it assembles comes from another file than the batch's, and stamps the file name on the batch. All columns see the same file sequence, so they cut at the same row and no batch spans two files. Batches do span row groups. The reader takes the file name from the first column's batch; it prefixes exception messages, attributes `RecordFilterTally` counts, and answers `FileAwareRowReader.currentFileName()`, which `AvroRowReader` (avro module) uses to name the file in its own failures. The accessor state is built once from the reference schema; how a later file's columns are mapped onto it is in [READ_PIPELINE.md](READ_PIPELINE.md).

**Close.** `close()` is idempotent. It closes the workers, recycles the held batches, drains every exchange and releases the iterator; the release runs even when teardown fails, with its own failure suppressed under the original. A reader closed early stops its workers without reading further.

Tests: `PqRowApiTest`, `NoColumnsFileReadTest`, `IteratorTrackingTest`, `MultiFileRowReaderTest`.

## Positioning: head, tail, skip

`RowReaderBuilder` validates the controls in its setters and in `build()`:

| Input | Raises |
|---|---|
| `head(n)` or `tail(n)` with `n <= 0`; `skip(n)` with `n < 0` | `IllegalArgumentException` from the setter |
| `head` + `tail`, `tail` + `skip`, `tail` + `FilterPredicate`, `tail` + `RowGroupPredicate` | `IllegalArgumentException` from `build()` |
| `tail` on a reader over several files | `UnsupportedOperationException` from `build()` |

`skip(0)` is the default and changes nothing. The semantics users rely on (the controls count over the surviving relation, in file order) are in [row-selection.md](../docs/content/concepts/row-selection.md); how `head` and `skip` count under a `FilterPredicate` is in [RECORD_FILTERING.md](RECORD_FILTERING.md#row-selection-over-the-filtered-relation). This section covers the unfiltered case, where a logical position is a physical one.

### head

`head(n)` is `maxRows`, passed to the `RowGroupIterator` and to every column worker. The iterator stops planning row groups once their rows cover the budget, and truncates each column's pages to the rows each row group must supply; each worker stops assembling at `n` rows and publishes its partial batch. The planning and fetch mechanics are in [READ_PIPELINE.md](READ_PIPELINE.md) and [FETCH_PLANNING.md](FETCH_PLANNING.md).

### skip

Without a filter, `skip(n)` starts the read at physical row `n` of the concatenated relation: the kept row groups of every input file, in file order, after the `RowGroupPredicate` (single-file only) has dropped row groups of the first file. It is a seek:

- **Whole row groups are dropped in planning.** `RowGroupIterator` plans a file at a time with a remaining-skip counter. A kept row group whose row count is at most the remainder is subtracted and never becomes a work item, so its pages are neither fetched nor decoded. The first row group the remainder falls inside becomes work item 0, and the remainder is its **residue** (`firstRowGroupSkip`).
- **Across files.** A file whose rows are all skipped has its footer read (and its schema checked against the reference) to count its rows; none of its data pages is fetched. Asking for the residue plans only as far as work item 0, so building a skipping reader does not plan the whole read.
- **The residue is decoded and discarded.** `ParquetFileReader.discardLeadingRows` calls `next()` residue times before handing the reader over, so the target row group's leading pages are fetched and decoded. Because those rows pass through the reader, the row cap is raised by the residue: the reader and workers get `head + residue`, and the iterator adds the residue to the first row group's page truncation budget. Without that term, a seek near the end of a row group would spend the `head` budget on discarded rows.
- **Past the end.** `skip(n)` at or beyond the total row count leaves no work item and yields an empty reader with no data fetched.
- **Failure while discarding** closes the partly built reader before the exception propagates, so its workers do not leak.

`dive`'s data preview relies on this seek; see [DIVE_ARCHITECTURE.md](DIVE_ARCHITECTURE.md).

### tail

`tail(n)` reads the last `n` rows of a single file. `ParquetFileReader.buildTailRowReader` keeps the trailing row groups whose row counts first cover `n`; earlier row groups are never work items. The first kept row group has a residue of `rows(kept) - n` leading rows, dropped by one of two paths:

| Condition | Path |
|---|---|
| Every kept row group can take a per-page row mask on every decoded column (`RowGroupIterator.canFastSkipAllRowGroups`) | `setTailSkip(residue)` gives work item 0 the matching range `[residue, numRows)`. Pages wholly before it are not fetched, and the first page it touches is trimmed to it, so the workers assemble exactly `n` rows |
| Any kept row group cannot (for example a nested column with v1 data pages and no OffsetIndex) | The residue is decoded and discarded through `next()`, as for `skip` |

The mask applies to every column of the row group or to none, since masking a subset would leave columns row-misaligned (the capability gate is described in [FETCH_PLANNING.md](FETCH_PLANNING.md)). The tail range must be set before any column requests its fetch plan; `setTailSkip` raises `IllegalStateException` afterwards. The probe plans the whole read, which is one footer because tail is single-file.

Tests: `ParquetReaderTest`, `BuilderCombinationTest`, `MultiFilePlanningTest`, `MisalignedPageBoundariesTest`, `NestedV1NoIndexFallbackTest`, `NestedV2NoIndexMaskingTest`, `DataPreviewIoTest` (cli).

## Boundaries

- **Page-level skip for the `skip` residue (#381).** The residue of a physical `skip` is decoded and discarded, although the per-page mask that serves `tail` could drop its leading pages the same way.
- **Page-level seek without an OffsetIndex (#378).** Locating the target page of a file without an OffsetIndex needs page locations synthesised from a header walk.
- **`tail` over several files.** Rejected at `build()` with `UnsupportedOperationException`; the tail plan and its mask probe are single-file.
- **`tail` with a filter (#542).** Rejected at `build()`; see [RECORD_FILTERING.md](RECORD_FILTERING.md#row-selection-over-the-filtered-relation).
- **Row-layer overhead (#1045).** Reading a file through `RowReader` costs measurably more CPU and allocation than reading the same columns through `ColumnReaders`.
- **Reader choice per file (#732).** The flat/nested choice is made from the file schema, so one repeated or group column puts every column of the read on the nested path, projected or not.
- **File name in nested caller-mistake messages (#1156).** `TopLevelFieldMap` and the nested flyweights raise their caller-mistake messages without the file name, which in a multi-file read leaves the file unidentified.
- **Forward-only.** A `RowReader` has no backward step, reset or reposition; a new position is a new reader.
