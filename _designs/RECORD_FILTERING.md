# Record filtering

How a filtered read tests the rows that survive statistics pruning and returns exactly the matching ones, on the row readers (`FlatRowReader`, `NestedRowReader`) and the column readers (`ColumnReader`, `ColumnReaders`). It covers the augmented projection, the per-row matcher, the drain-side batch matchers, record selection and compaction on the column path, the consumption of row groups statistics proved to match in full, and how `head`, `skip` and `tail` count over the filtered relation. The public predicate, its literal rules and its resolution to `ResolvedPredicate` are in [PREDICATE_MODEL.md](PREDICATE_MODEL.md); the CANNOT/MIGHT/ALWAYS decision for row groups and pages is in [STATISTICS_PRUNING.md](STATISTICS_PRUNING.md). Pipeline mechanics shared with unfiltered reads (page row masks, fetch plans, worker flush rules, the reorder buffer) are in [READ_PIPELINE.md](READ_PIPELINE.md) and [FETCH_PLANNING.md](FETCH_PLANNING.md). The user-facing semantics of row selection are in [docs/content/concepts/row-selection.md](../docs/content/concepts/row-selection.md).

## Contract

For a reader built with projection `p` and filter `pred`:

| Property | Statement |
|---|---|
| Exactness | The reader returns exactly the rows satisfying `pred`, in file order. No client-side residual is needed; an aggregate over the output is correct. |
| Agreement across readers | The row reader, a `ColumnReaders` group and independently built single `ColumnReader`s over the same file and predicate return the same rows and values. |
| Row alignment | Within one `ColumnReaders` group every column exposes the same matching records per batch; `getRecordCount()` is the matching count. |
| Projection bound | Accessors resolve the leaves of `p` and nothing else, at every depth. A predicate column outside `p` raises as any unprojected column does: `IllegalArgumentException` by name, `IndexOutOfBoundsException` by index. `getFieldCount()` counts the leaves of `p` on `FlatRowReader` and its top-level fields on `NestedRowReader`; a `PqStruct` reports only the children `p` reaches. |
| Null semantics | A null value satisfies no comparison or membership test; `AND`/`OR` follow SQL three-valued logic, and a row is returned only when `pred` is definitely true. |

`FilterPredicate.intersects` is the one exception to exactness: it decides whole row groups and pages by bounding box, and the record-level matcher passes every row of a surviving unit (`RecordFilterCompiler` compiles it to `row -> true`). The user documentation states this.

Every row of a row group statistics left undecided is evaluated. Rows of a row group statistics proved to match in full are returned without evaluation (see [Always-match row groups](#always-match-row-groups)), so exactness there rests on the statistics being correct.

Tests: `PredicatePathAgreementTest`, `ColumnReaderExactFilterTest`, `PredicatePushDownTest`, `FilterOnlyColumnSkipTest`, `DrainSideOracleTest`.

## Augmented projection

A filtered read decodes more than it exposes. `ReadProjection` (`internal.schema`) holds two projected schemas:

- `payload()`: the leaves of `p`, which the reader exposes.
- `decoded()`: the payload plus every leaf `pred` references that the payload does not cover. Every worker, exchange, fetch plan and batch array spans `decoded()`.

`ReadProjection.withPredicateColumns(schema, p, predicateColumns, completeContainers)` builds the pair. The row path passes `completeContainers = true`, the column path `false`, since the column readers read individual leaves. A read without a filter, a filter that adds no column, or a `ColumnProjection.all()` projection yields `ReadProjection.of(payload)`: the payload is decoded alone.

**Ordering invariant.** The payload's columns and top-level fields lead `decoded()` at the indices they hold in `payload()`; the remaining predicate columns follow in file order. A decoded index below `payloadColumnCount()` therefore names the same column in both schemas, and a decoded index at or past it is **filter-only** (`isFilterOnly`). The record constructor rejects a pair whose payload is not the prefix; the file order of the columns after it is not checked.

Everything downstream relies on this prefix property: readers take the payload batches as the prefix `[0, payloadColumnCount)` of the batch array, and poll filter-only columns only after the payload (see [Always-match row groups](#always-match-row-groups)).

**Two accessor surfaces.** A row reader builds its accessor state from `payload()` alone: `FlatRowReader`'s name-to-index map and value arrays, and `NestedRowReader`'s `NestedBatchDataView`, have no entry for a filter-only column, so no accessor reaches one and no accessor branches on whether a column is filter-only. The per-row matcher evaluates against `PredicateView` (`internal.reader`) instead, a `StructAccessor` over the predicate columns alone, whether projected or filter-only:

- Flat predicate columns are served from their typed arrays; nested ones through a `NestedBatchDataView` over a projection of the predicate paths.
- Its indexed accessors use the view's own index space: flat predicate columns first, then the top-level fields of the nested projection. `PredicateView#indexOf` maps a file leaf column into that space, or returns `-1`.
- A flat column below a struct (a leaf under a required-only path, which the column path decodes as flat) is reachable by index only, so its leaf name cannot shadow a top-level column of the same name.
- It is refreshed once per evaluated batch and positioned per record.

The column readers' `SelectionEngine` evaluates through the same view, so both reader families test `pred` through one accessor. The drain-side matchers read batches by decoded index and use neither surface.

Tests: `ReadProjectionTest`, `RequiredStructPredicateTest`.

## Per-row matcher

`RecordFilterCompiler` (`internal.predicate`) compiles a `ResolvedPredicate` once per reader into a `RowMatcher`, a single-method `test(StructAccessor)`. Field paths, leaf names, operators and literals are resolved at compile time; the per-row work is value reads and comparisons. It is the fallback for every shape the drain-side compiler refuses, and the only path on `NestedRowReader`.

**One lambda per `(type, operator)`.** Each leaf factory switches on the operator at compile time and returns a distinct lambda class per pair. Comparison semantics per type (NaN and signed-zero ordering through `Float.compare`/`Double.compare`, unsigned orders, the binary `Comparison`) are those of [PREDICATE_MODEL.md](PREDICATE_MODEL.md). A boolean leaf takes `EQ`/`NOT_EQ` only; any ordered operator reaching it raises `IllegalStateException`, since the resolver answers those as an equality or a constant.

**Fixed-arity compounds.** `And`/`Or` with two, three or four children compile to `And2Matcher`…`Or4Matcher`, final classes holding each child in its own field; larger arities use an array walker (`AndNMatcher`/`OrNMatcher`); a single child is returned as is. Because every leaf is a distinct class, each call site inside a fixed-arity matcher sees one receiver type per query, so the JIT inlines the leaf bodies into the compound. Untested. An array walk would present one megamorphic call site and keep each leaf behind a virtual call.

**Indexed leaves.** `compile(predicate, schema, leafIndex)` takes a callback mapping a file leaf column to the accessor's index. Where it returns a non-negative index the leaf reads through the indexed accessors (`isNull(int)`, `getLong(int)`, …); where it returns `-1` the leaf falls back to name-keyed access through the intermediate struct path. Both readers and `SelectionEngine` pass `PredicateView::indexOf`.

**Missing intermediate structs.** A name-keyed leaf walks the intermediate struct path; if any struct on it is null the walk yields no accessor. A value leaf then does not match, `IS NULL` matches and `IS NOT NULL` does not. A group null predicate (`IsNullPredicate.group()`) tests the first field on the path at the predicate's definition level, which separates a null group from a present group with a null child.

Tests: `RecordFilterCompilerTest`, `RecordFilterIndexedTest`, `ColumnReaderExactFilterTest`.

## Drain-side matchers

For an eligible predicate the flat row reader evaluates it on the column workers' drain threads, one column per thread, rather than one row at a time on the consumer thread. `BatchFilterCompiler.tryCompile` decides once, at reader construction, and returns either a `CompiledBatchFilter` (per-column `ColumnBatchMatcher` fragments plus a `MergePlan`) or `null`, in which case the reader falls back to the per-row matcher. There is no configuration switch.

### Eligibility

A predicate is eligible when every leaf is:

- on a top-level column (field path of length one) present in `decoded()`, and
- a supported `(type, op)`: `int`, `long`, `float`, `double`, unsigned `int`/`long`, and binary × the six comparison operators; `boolean` × `EQ`/`NOT_EQ`; the `IN` forms of those types; `IS NULL`/`IS NOT NULL`; and the constant leaves `EveryNonNullRow`/`NoRow` that negation produces,

and no column appears in two independent subtrees. `Not` never reaches the compiler, since resolution lowers it to operator inversion.

| Falls back to the per-row matcher | Reason |
|---|---|
| A leaf below a struct or list | Batch matchers read a flat typed array per column |
| `FLOAT16` comparison or `IN` | No batch matcher |
| `intersects` | Row-group/page decision only |
| A binary leaf in an instant order (`INT96`, `FIXED_LEN_BYTE_ARRAY(12)` `TIMESTAMP`) | `BinaryComparator.sliceOrder` is `NONE` |
| A column in two independent subtrees, e.g. `(a > 5 AND b > 5) OR (a < 0 AND b < 0)` | One column holds one bitmap per batch |
| Any predicate on `NestedRowReader` | That reader has no drain-side path |

`isSupported` is an exhaustive switch over the `ResolvedPredicate` hierarchy with no `default`, so a new predicate type cannot reach or silently skip the batch path without an answer there.

**Compilation.** The compiler walks the tree bottom up. A subtree living on one column collapses into one matcher slot: same-column siblings under an `And` chain through `AndBatchMatcher`, under an `Or` through `OrBatchMatcher`. A subtree spanning columns becomes a `MergePlan.And`/`Or` whose children are `MergePlan.Column(decodedIndex)` references or nested plans. Each column appears in at most one `Column` node.

Tests: `BatchFilterCompilerTest`.

### Bitmap semantics

A matcher writes bit `i` of a per-batch `long[]` when row `i` **definitely** satisfies its leaf; a null row's bit is clear. Under SQL three-valued logic a row is returned exactly when the predicate is definitely true, and "definitely true" composes word-wise: `def(A AND B) = def(A) & def(B)` and `def(A OR B) = def(A) | def(B)`. Unknown behaves as false in both, and the only case where that could matter, `NOT unknown`, never occurs because negation is lowered to leaves before compilation. No null tracking beyond the bitmaps is needed.

**Stale bits.** Bitmaps are sized to the batch capacity and reused across batches. A matcher and `MergePlanEvaluator` write only the words covering `[0, recordCount)`, and bits past `recordCount` may hold values from an earlier, longer batch. Every consumer bounds its reads by the record count: `FlatRowReader`'s bit scan and run walk stop at the batch size, `FlatRowReader.countMatches` masks the tail word, and `SelectionEngine.collectSetBits` iterates `[0, recordCount)`. A new consumer of these bitmaps must do the same. Untested.

**Binary leaves.** Byte-array matchers compare each value's slice of the batch's `BinaryBatchValues` in place. `BinaryComparator.sliceOrder` maps each `Comparison` to `UNSIGNED`, `SIGNED` or `NONE` through a switch with no `default`, and both the eligibility check and the matchers read it, so a new `Comparison` is answered in one place. Equality tests bytes only when `Comparison.byteExact()` holds; otherwise (a `BYTE_ARRAY` decimal, where one value has several spellings) it compares by order.

Tests: `DrainSideOracleTest`.

### Merge

`BatchMatchMerger` (`internal.reader`) combines the per-column bitmaps into one survivor bitmap per batch, once per batch. It has two modes, fixed at construction:

- **Aliasing** (`FlatRowReader`): each `FlatColumnWorker` ran its fragment into the batch's own `matches` array when publishing the batch, on the drain thread; the merger reads those arrays.
- **Owning** (`SelectionEngine`): there are no workers to have run the fragments, so the merger runs them itself into buffers it owns, on the consumer thread.

A plan that is a single `MergePlan.Column` yields that column's bitmap directly; any other plan goes through `MergePlanEvaluator` into a combined buffer. The merger dereferences only the columns its plan references (`referencedColumns()`), so a caller that stages the batch array need keep only those entries current.

**Oracle.** The per-row matcher and the drain-side fragments plus merge select the same rows for each supported column type under its comparison operators, the `IN` forms and the null tests, as single leaves and in cross-type `And`/`Or` compounds. A single byte-array leaf is also checked against a reference that bypasses `BinaryComparator` (decimals as `BigInteger`, byte strings through `Arrays.compareUnsigned`), since both paths compare through it. The unsigned leaves and the constant leaves are covered end to end only, where the row path runs every corpus case both drain-side and forced onto the per-row matcher.

Tests: `DrainSideOracleTest`, `PredicatePathAgreementTest`.

## Column readers

A filtered `ColumnReaders` is a grouped drain over `decoded()` plus a per-batch **selection**; a single `ColumnReader` with a filter is a one-column group (`ParquetFileReader.buildSingleColumnReader`). `ColumnScan` owns one `ColumnCursor` per decoded column over one shared `RowGroupIterator`, which keeps all columns row-aligned regardless of each column's page-skip capability.

**Selection, once per batch.** On each advance `ColumnScan` polls the cursors, checks lockstep (every column produced a batch of the same record count, or `IllegalStateException`), and asks `SelectionEngine.computeSelection` for the ascending indices of the matching records. It returns `-1` when every record matches, and compaction is skipped. The selection is computed from the pre-compaction batches and applied to every payload cursor before the next advance; a payload column that is also a predicate column is read before it is compacted.

`SelectionEngine` picks its backend once at construction:

- `BatchFilterCompiler.tryCompile` accepts the predicate: a `BatchMatchMerger` in owning mode.
- Otherwise: the `RowMatcher` compiled against a `PredicateView` over the cursors' batches, evaluated per record. Nested predicate columns arrive without element validity on this path, and the view derives it from the definition levels.

Both backends produce the same selection representation, so compaction does not depend on the backend.

**Flat compaction.** `ColumnScan` gathers a fixed-width value array in place (the selection is strictly ascending with `kept[j] >= j`, so no slot is overwritten before it is read), compacts the validity bitmap, and rebuilds a variable-length leaf through `LeafCompaction.compactBinary`, which carries a dictionary-encoded leaf's dictionary and entry indices through.

**Nested compaction.** A nested batch is compacted on its raw `(definitionLevels, repetitionLevels, values)` triplet, before the real-items view exists: each selected top-level record is the contiguous level run starting at its record offset, and compaction copies those runs. `ColumnReader` then derives layer offsets, validity and the real-items leaf from the compacted triplet, through the same code an unfiltered read uses, so no layer bookkeeping is recomputed by hand. The filtered column path therefore runs nested workers in `REAL_VIEW_KEEP_LEVELS` mode. A fixed-size-list batch has no level arrays and stays fixed-width, since selection keeps whole records.

Independently built single readers over the same file and predicate each decode the predicate columns themselves; nothing is shared between them. Each also sizes its batches from the columns it decodes, so two such readers return the same rows in the same order but not in the same batches: a reader of `id` filtered on `id` batches wider than a reader of a string column filtered on `id`. Per-batch alignment holds within one `ColumnReaders` group, or across readers given the same explicit `batchSize`. Untested for the explicit-size case.

Tests: `ColumnReaderExactFilterTest`.

## Always-match row groups

`RowGroupIterator` marks each surviving work item with `filterAlwaysMatches` when statistics decide `ALWAYS_MATCHES` for its row group (the decision is in [STATISTICS_PRUNING.md](STATISTICS_PRUNING.md)). The decision is consumed per row group and never asked of the read as a whole, which would require planning every file before the first row. The granularity is the row group.

### Homogeneous batches

When a filter is installed, every `ColumnWorker` flushes its batch in progress at a page whose `filterAlwaysMatches` differs from the batch's, and stamps the flag on the published batch (`Batch.filterAlwaysMatches`, `NestedBatch.filterAlwaysMatches`). Every worker of a read applies the same rule to the same sequence of row groups, pages and masks with the same capacity, so all columns close their batches at the same rows and a batch is either wholly proven or wholly undecided. A per-column flush would desynchronise the merge. Unfiltered reads do not flush on the flag.

A worker reads the flag of its own batch; a reader reads it from the first payload column's batch, which speaks for the step:

| Consumer | Proven batch |
|---|---|
| `FlatColumnWorker` with a fragment | Writes the all-ones mask instead of running the fragment |
| `FlatRowReader`, no cap | Clears the active matcher and merger; the batch takes the plain cursor and is tallied whole |
| `FlatRowReader` with `head(N)` | Drain-side: uses an all-ones survivor bitmap without merging. Per-row: counts every row as a match without calling the matcher |
| `NestedRowReader` | As `FlatRowReader`'s per-row path |
| `SelectionEngine` | Returns `-1` before either backend runs |

The predicate view is not refreshed for a proven batch.

Tests: `AlwaysMatchingRowGroupTest`, `FilterOnlyColumnSkipTest`.

### Filter-only column skip

A filter-only column serves only the predicate, and the predicate needs it only in undecided row groups:

| Row-group decision | Payload column | Filter-only column |
|---|---|---|
| `CANNOT_MATCH` | not read | not read |
| `MIGHT_MATCH` | read | read |
| `ALWAYS_MATCHES` | read | not fetched, decompressed or decoded |

`RowGroupIterator.skipsColumn` gives a column the `SkippedColumnFetchPlan` exactly when the work item carries `filterAlwaysMatches` and the column is filter-only. The plan holds no chunk handle, takes no part in cross-column coalescing, and yields one `PageInfo.BOUNDARY_MARKER`: a page with no bytes and no rows. The retriever hands the marker to the drain without decoding; the drain runs its per-page bookkeeping for it and assembles nothing. The marker counts as a proven page, so it closes the column's batch in progress where the payload columns close theirs and keeps the column aligned across a proven row group placed between undecided ones.

**Planning and polling must agree.** Every consumer takes a step's payload batches first and takes a batch from each filter-only column only when the step is not proven; otherwise the lockstep checks cover every column. The skip condition in `skipsColumn` is exactly this poll condition. If a filter-only column were read in a proven row group, it would publish batches no consumer takes, and the next undecided step would take them in place of its own. Nothing else, such as a page mask narrowing the payload columns in a proven row group or a row cap, may enter the skip decision, since the skipped column assembles no rows. A `ColumnScan` filter-only cursor keeps its previous batch through a proven step, and nothing reads it. At the end of the stream every exchange is checked for errors, so a failure on a filter-only column's worker surfaces. Untested.

A filter-only column keeps its worker and exchange for the whole read, since the decode set is fixed before the first row group is planned.

Tests: `FilterOnlyColumnSkipTest`.

### The row cap

With a filter, `head(N)` caps matching rows. Each `ColumnWorker` holds the cap (`activeMaxRows`) while every page it has reached belongs to a proven row group, where assembled rows and matching rows are the same rows, and drops it for the rest of the read at the first page of an undecided row group; from there the reader counts matches. A boundary marker counts as a proven page and leaves the cap in place. A payload column that reaches the cap inside a proven row group ends the stream there.

Tests: `AlwaysMatchingRowGroupTest`, `FilterOnlyColumnSkipTest`, `PredicatePushDownTest`.

## Row selection over the filtered relation

The **surviving relation** is the rows of the row groups kept by the `RowGroupPredicate`, intersected with the rows matching the `FilterPredicate`, in file order. `head`, `skip` and `tail` count over it (`LIMIT`, `OFFSET`, last `n`); there is no mode in which they count physical rows under a filter. The reader-facing explanation, including why physical positioning is `byteRange`'s job alone, is in [row-selection.md](../docs/content/concepts/row-selection.md).

- **`byteRange`** keeps a row group when its midpoint (first chunk's start plus half the row group's compressed size) falls in `[start, end)`, so disjoint ranges partition a file's row groups. `ParquetFileReader` applies it before the iterator, so statistics are asked only of the kept row groups.
- **`head(n)`** with a filter is the matching-row cap above.
- **`skip(n)`** without a filter is a physical seek to the row group the offset lands in (see [ROW_READER.md](ROW_READER.md)). With a filter, row-group statistics bound values and not match counts, so there is no seek: the reader is built over the whole kept relation with a matching-row cap of `n + head` (none without `head`) and discards the first `n` matches.
- **`byteRange` with `skip`/`head`** counts within the kept row groups.

`build()` rejects these combinations, with `IllegalArgumentException` unless noted:

| Combination | Reason |
|---|---|
| `tail` + `FilterPredicate` | The last `n` matching rows are not known from statistics; this needs a reverse scan or a window over a full forward scan |
| `tail` + `RowGroupPredicate` | `tail` plans from the total row count, which a row-group filter changes |
| `tail` + `head`, `tail` + `skip` | Mutually exclusive controls |
| `RowGroupPredicate` on a reader over several files, on the row reader and `ColumnReaders` (`UnsupportedOperationException`) | A byte range names positions in one file |

The column readers take a `FilterPredicate` and a `RowGroupPredicate` but no row-count controls.

Tests: `RowGroupFilterTest`, `PredicatePushDownTest`, `BuilderCombinationTest`.

## Boundaries

- **Nested and remaining leaf shapes on the drain side (#485).** Nested-path leaves, `FLOAT16`, `intersects` and every predicate on `NestedRowReader` are evaluated a row at a time on the consumer thread. Moving them to the drain side would let `RecordFilterCompiler` go.
- **Late materialization (#500).** Payload columns are decoded in full for every undecided row group and then compacted; decoding them only for matching rows is a different pipeline shape.
- **Dictionary-space evaluation (#859).** A predicate on a dictionary-encoded column is evaluated per row, not once per dictionary entry. A byte-equality shortcut there is sound only where `Comparison.byteExact()` holds.
- **Fused same-column range matchers (#454)** and **Vector API matchers (#456).** `id >= a AND id < b` runs two passes over the column and one word-wise AND.
- **`tail` with a filter (#542).** Rejected at `build()`, as above.
- **Page-level always-match.** A page whose index entry proves a full match is evaluated row by row like any page of an undecided row group; see [STATISTICS_PRUNING.md](STATISTICS_PRUNING.md) for why the decision stops at the row group.
