# Plan: converging the flat and nested read paths

Tracking issue: #1169. Stage issues: #1334, #1335, #475, #631, #732, #1024, #750,
#1045.

The row-read path exists twice. This document maps where the two copies are, what
each one costs, and the order in which they are merged.

## Where the split is

| Layer | Flat | Nested | Shared |
| --- | --- | --- | --- |
| Fetch, page decode | — | — | `RowGroupIterator`, `SequentialFetchPlan`, `PageDecoder`, `Page` |
| Worker | `FlatColumnWorker` | `NestedColumnWorker` | `ColumnWorker` (3 abstract methods) |
| Batch | `BatchExchange.Batch` | `NestedBatch` | `BatchExchange`, `BinaryBatchValues` |
| Reader | `FlatRowReader` | `NestedRowReader` → `NestedBatchDataView` | `StructAccessor` |
| Flyweights | — | `PqStructImpl`, `PqListImpl`, `PqMapImpl` | `NestedBatchIndex` |
| Leaf decode | inline in `FlatRowReader` | inline in four classes | `LogicalTypeConverter`, `LeafKind` |

The IO and decode layers are path-agnostic. The duplication is in the worker, the
batch and the accessors.

**Flat and repeated, not flat and nested.** The two paths are named for schema shape,
but the property that decides addressing is repetition. A column with
`maxRepetitionLevel == 0` holds one value per row, so `valueIndex == rowIndex` however
many groups the leaf sits under; only a repeated column needs offsets. A non-repeated
struct is therefore flat work wearing a nested shape, which is why #475 exists. This
document says *nested* only where it names today's classes or the path a read takes
today, and *repeated* wherever the axis is meant.

## Finding 1 — dispatch is per file, and reads the file schema

`ParquetFileReader.createRowReader` routes on `FileSchema.isFlatSchema()`. That method
asks about the file, so a projection of nothing but top-level primitives lands on
`NestedRowReader` whenever the file holds one list column anywhere — including a
column the projection never names.

Measured on a 1M-row uncompressed file holding twenty `int32` columns and one
`list<string>`, projecting the twenty scalars and reading two or all twenty of them per
row. The dispatch predicate below was applied and reverted, so the file, the projection
and the reads are identical between the two columns and only the reader differs.
Minimum of 5 runs after 3 warmups, one JVM per figure, taken in the dev container
without a pinned clock.

| Read per row | `NestedRowReader` (today) | `FlatRowReader` | ratio |
| --- | --- | --- | --- |
| 2 of 20 columns | 46.0 ms | 15.2 ms | 3.03× |
| 20 of 20 columns | 63.7 ms | 26.1 ms | 2.44× |

The fixed part of the gap — 30.8 ms at two reads per row, before per-read work dominates
— is the producer costs of finding 2 plus the per-row index refresh below. The part that
scales with reads (nested adds 17.7 ms for eighteen further reads per row against flat's
10.9 ms) is accessor indirection through `NestedBatchDataView`. Only the first is
recoverable without changing which reader serves the read.

## Finding 2 — what a non-repeated column pays on the nested path

Per batch, per column, for a column with `maxRepetitionLevel == 0`:

- `Arrays.copyOf` of the definition levels (`batchCapacity` ints), all at one value.
- `Arrays.copyOf` of the repetition levels (`batchCapacity` ints), all zero.
- `Arrays.copyOf` of `recordOffsets` (`batchCapacity` ints), exactly `[0, 1, 2, …]`.
- `computeElementValidity`, an O(valueCount) scan of the definition levels.
- `trimValues`, a full copy of the value array — finding 6, which every column pays.

`FlatColumnWorker` does none of these: it recycles one value array and one validity
bitmap for the life of the read.

`NestedBatchDataView.setRowIndex` refreshes the cached value index for every projected
column on every row, whether or not the row reads that column. The flat reader stores
one `rowIndex`.

## Finding 3 — the nested layout is already dense

`PageDecoder` allocates value arrays at `numValues` — levels included — and the typed
decoders scatter present values into the positions the definition levels give them. A
null occupies a slot on both paths.

So for a non-repeated column the nested worker's `recordOffsets` is the identity,
`valueCount == recordCount`, and the value layout is the one `FlatColumnWorker`
produces. The "layout is coupled to addressing" section of #732 describes a compaction
the row-reader path does not perform, and the addressing seam it calls for is already
written and already correct:

- `NestedBatchIndex.getValueIndex`: `recordOffsets != null ? recordOffsets[i] : i`
- `NestedBatchDataView.setRowIndex`: the same expression

Nulling the offsets for a non-repeated column is a change to the producer alone.

Per-column worker selection is also not hypothetical: the columnar path already does it.
`ColumnReader` picks its worker per column on `layers.count() > 0 ||
maxRepetitionLevel > 0`, taking `FlatColumnWorker` otherwise. #732's model is shipped —
on the `ColumnReader` API rather than the row reader. It splits on shape rather than
repetition, sending a non-repeated leaf under a group to the nested worker, so it agrees
with stage 3's interim predicate and not with the end state's.

## Finding 4 — five copies of the leaf decode

`getDate`, `getTime`, `getTimestamp`, `getLocalTimestamp`, `getDecimal`, `getUuid`,
`getInterval`, `getFloat` (FLOAT16) and the `getValue` dispatch are written out in five
places: 58 call sites into `LogicalTypeConverter` and `BinaryBatchValues` for twelve
distinct decodes.

| Class | Container | Index | Leaf schema |
| --- | --- | --- | --- |
| `FlatRowReader` | `flatValueArrays` | `rowIndex` | `ColumnSchema` |
| `NestedBatchDataView` | `batchIndex.valueArrays` | `cachedValueIndex[col]` | `SchemaNode.PrimitiveNode` |
| `PqStructImpl` | `batch.valueArrays` | `resolveValueIndex(projCol)` | `SchemaNode.PrimitiveNode` |
| `PqMapImpl` | `batch.valueArrays` | `valueIdx` | `SchemaNode.PrimitiveNode` |
| `PqListImpl` | `batch.valueArrays` | `pos` | `SchemaNode.PrimitiveNode` |

Every container is an `Object[]` holding the same element types. The four nested
classes share one container instance and one schema carrier. The decodes differ only in
which `(col, idx)` they are handed.

The copies have already drifted. `TimestampAccessorKind` is called inside the accessor
in `FlatRowReader` and by the caller in the flyweights. `requireFloatAccess` exists
twice with two separately worded messages. `ExceptionContext` enrichment reaches the two
readers and none of the three flyweights, which is #1156 and #447. #971 is one change
against a shared helper and five against the current shape. The CLI's four renderers
(#1021) decode from the same table again, and should land on the same helper.

`ColumnSchema` and `SchemaNode.PrimitiveNode` are two public records describing one
leaf. Unifying them is a public-API change and is not required: a helper taking
`(PhysicalType, LogicalType)` leaves each caller to unwrap its own carrier.

## Finding 5 — batch sizing under-counts the nested path

`BatchSizing.levelBytesPerValue` returns `0` for `maxRepetitionLevel == 0`, on the
stated grounds that a non-repeated column carries no per-value level arrays. That holds
on the flat path. On the nested path every column carries two `int[]`, so a mixed
file's batches exceed the 6 MB target by `8 × valueCount` bytes per non-repeated column
— the opposite direction from the cache budget the sizing exists to hold.

## Finding 6 — the trim copy defeats batch recycling

Unlike finding 2, this is paid by every column, repeated ones included.

`trimValues` copies the value array on every publish and overwrites the recycled batch's
`values` with the copy, so the array `BatchExchange.recycling`'s factory allocates is
used for the first batch and discarded from then on. A byte-array leaf also allocates a
fresh `BinaryBatchValues` and copies the whole byte buffer each time.

The trim is required on the `ColumnReader` paths, where the returned array's length is
the contract. On `IndexMode.ALL_ITEMS` the consumer reads `NestedBatchIndex.valueCounts`
and the length carries nothing.

## Intended end state

One row reader over a heterogeneous set of columns, each drained by the worker its shape
calls for — `FlatColumnWorker` where `maxRepetitionLevel == 0`, `NestedColumnWorker`
otherwise — addressed through one seam, `offsets == null ? rowIndex : offsets[rowIndex]`,
with one statement of the leaf decode behind the accessors.

This is #732's model, and `ColumnReader` already implements it on the columnar path
(finding 3), splitting on `layers.count() > 0 || maxRepetitionLevel > 0`. The row path
splits on repetition alone: a column with `maxRepetitionLevel == 0` is row-aligned and
needs no offsets whatever its depth, which is what makes a non-repeated struct flat work
in a nested shape.

Two constraints come with it, both named by #732 and neither designed away here:

- **The reader holds two batch types.** `BatchExchange.Batch` and `NestedBatch` are
  separate classes so `Batch` stays a leaf type for the JIT, so the reader dispatches
  addressing per column rather than over one uniform batch.
- **The fully-flat path must not regress.** Finding 1 puts part of today's gap in
  accessor indirection, so the flat case survives as a performance specialization of one
  model rather than as a second implementation of the accessor contract.

Today's class names still describe what they hold under this model, so nothing is
renamed.

## Delivery plan

Six stages. Each names the findings it closes, so no work item stands on anything but
the evidence above.

The order removes cruft first. Few stages strictly depend on their predecessors — stage
2 gates the numbers stages 3 and 5 are judged on, and stage 1 has to precede stage 4 —
but every stage after the first works in code the first has already deduplicated, and
the reader merge is the last thing that should meet five copies of a decode.

| # | Stage | Closes | Issues | Depends on |
| --- | --- | --- | --- | --- |
| 1 | State the leaf decode once | Finding 4 | #1334; unblocks #971, #1156, #447, #1021 | — |
| 2 | Benchmark projected reads | — | none (hardwood-benchmarks) | — |
| 3 | Choose the reader from the projection | Finding 1 | #1335 | 2 for numbers |
| 4 | Non-repeated structs on the flat path | — | #475, #631 | 1, 3 |
| 5 | One reader, worker chosen per column | Findings 2 and 5; remainder of 1 | #732 | 1, 4 |
| 6 | Cut the nested worker's per-batch cost | Finding 6 | #1024, #750, #1045 | — |

Stages 3 and 5 both decide which worker serves a column, at different granularity:
stage 3 chooses one reader per read from the projection, stage 5 chooses one worker per
column and needs no reader dispatch at all. Stage 5 subsumes stage 3, which is why stage
3 is worth doing anyway — a change at one call site, 3.03× measured, long before the
reader merge is ready. Stage 6 is on the other axis entirely: it cuts what
`NestedColumnWorker` spends per batch on every column it drains. The costs finding 2
lists for a *non-repeated* column are closed by stage 5 moving those columns off it, not
by optimising the worker that would no longer see them.

### Stage 1 — state the decode once

Closes finding 4. Issue: #1334.

First because every stage after it touches these classes, and this way they are
deduplicated once rather than edited five times each. It carries no performance claim
beyond not regressing, so it does not wait on stage 2.

1. A package-private helper over `(Object[] arrays, int col, int idx, PhysicalType,
   LogicalType)`, holding the logical decodes and the `TimestampAccessorKind` /
   `LogicalAccessorKind` / `requireFloatAccess` guards, landed on `PqStructImpl`,
   `PqMapImpl` and `PqListImpl`: one container, one schema carrier, no hot-loop
   exposure.
2. The same helper into `NestedBatchDataView`, then `FlatRowReader`. The gate is that
   `FlatPerformanceTest` does not move.

The failures that were five changes then become one each: #1156 and #447 (nested
accessors name the file), #971 (a type mismatch names the column), and #1021, whose
four CLI renderers decode from the same table a fifth and sixth time and consolidate on
this helper rather than beside it.

### Stage 2 — benchmark projected reads

Issues: none. The work is in the standalone `hardwood-benchmarks` repository, not this
one.

`NestedScanBenchmark` covers a full read of Overture Maps places against
`AvroParquetReader`, named and indexed, behind a checksum gate. Every benchmark method
calls `reader.rowReader()` with no projection and recurses the whole record, so nothing
in the benchmark set reads a subset of any file — the shape findings 1 and 2 are about
is unmeasured.

Two workloads close that, each with the same two contenders the full-record workload
already has — Hardwood's row reader and `AvroParquetReader` — under one projection:

- **`projectedScalars`** — the top-level primitives only.
- **`projectedScalarsAndList`** — the same projection plus one list column.

Four `@Benchmark` methods, named access only; indexed access stays on the full-record
workload rather than doubling the matrix again. The checksum gate extends to both
projections.

### Stage 3 — choose the reader from the projection, not the file

Closes finding 1. Issue: #1335.

Take the flat reader when only flat top-level columns are projected — *flat* is
non-repeated, *top-level* is not under a group. As a predicate over the projection,
replacing `schema.isFlatSchema()` at the dispatch site: every projected column has
`maxRepetitionLevel == 0`, and every projected top-level field is a primitive.
`ProjectedSchema` answers both, and no new code path is added.

Both clauses earn their place — an unannotated `repeated int32` is a top-level primitive
that repeats — but only the first is the axis. `account.id` is flat and still
unreachable, because `FlatRowReader` keys its name map on the leaf name and reports leaf
columns where the nested reader reports top-level fields. Stage 4 deletes the second
clause.

Existing coverage carries most of the guardrail: the core suite passes unmodified under
the change, with one read changing reader,
`ColumnProjectionTest.testNestedSchemaProjectTopLevelField`. Two gaps remain:

- **The motivating shape is untested.** `nested_struct_test.parquet` has no repeated
  column. Nothing projects top-level primitives from a file containing a list or map;
  `list_basic_test.parquet` (`id` plus two lists) covers it, with nulls, a logical type
  and a filtered read.
- **Nothing asserts which reader serves a read**, so the flip is invisible and can be
  undone silently. Assert the concrete reader class for a flat, a flipped and a repeated
  projection.

Judged on the `projectedScalars` workload.

### Stage 4 — non-repeated structs on the flat path

Issues: #475, #631.

#475 widens stage 3's predicate from "every projected top-level field is a primitive"
to "nothing in the projection repeats" — the axis the end state settles on. A group
name resolves to a `PqStruct` flyweight over row-aligned leaves, and an optional group
carries a group-present bitset so leaf-null and struct-null stay distinct.
`FlatColumnWorker` reduces definition levels to a leaf-present bit, so that bitset is
the capability stage 5 needs for every non-repeated struct: nothing here is written to
be deleted. Stage 1 is what makes this small: without the shared helper it adds a sixth
copy of the accessors to `FlatRowReader`.

#631 is a struct perf test in `performance-testing/end-to-end`. Its flat fixture is a
struct of primitives that must dispatch to `FlatRowReader`, which only happens once
#475 lands. Its reader-class assertion is the in-repo counterpart of the one stage 3
adds.

### Stage 5 — one reader, worker chosen per column

Reaches the end state. Closes findings 2 and 5, and the remainder of finding 1, on the
seam finding 3 shows is already in place. Issue: #732.

One reader holding columns from both workers, selecting on `maxRepetitionLevel` and
addressing each through `offsets == null ? rowIndex : offsets[rowIndex]`. A
non-repeated column then drains flat wherever it appears, which is what closes finding
2 — not by making `NestedColumnWorker` cheaper for it, but by no longer sending it
there. Finding 5 goes with it: `BatchSizing.levelBytesPerValue` returning `0` for a
non-repeated column becomes true, because such a column no longer carries level arrays.
The scalar columns of a mixed projection gain drain-side batch filtering, which exists
only on the flat worker.

The work is the heterogeneous batch set: reconciling addressing and the drain-side
intersect-matches logic across `BatchExchange.Batch` and `NestedBatch`. #732 prices this
as one hoistable per-column branch in the accessor. `ColumnReader` is the existence
proof — it has selected its worker per column since it shipped — though it splits on
shape rather than repetition; `ColumnCursor.isNested` moves to the repetition predicate
in this stage.

The fully-flat projection must come out no slower than stage 3 leaves it. Finding 1 puts
part of today's gap in accessor indirection, so the all-non-repeated case keeps a
specialization; #732 requires the same. Judged on both stage 2 workloads plus
`NestedScanBenchmark`'s full-record workload.

### Stage 6 — cut the nested worker's per-batch cost

Closes finding 6. Issues: #1024, #750, #1045.

Scoped to the columns `NestedColumnWorker` still drains once stage 5 lands — the
repeated ones. Nothing in it is written to be deleted later, and it depends on no other
stage, so it can proceed in parallel.

1. On `IndexMode.ALL_ITEMS`, swap the accumulator with the recycled batch's array
   instead of copying — the shape `publishCurrentBatch` already uses for a full
   fixed-width batch. This is finding 6, and the per-batch allocation behind #1024 and
   a share of #1045's staging garbage.
2. #750, bulk-copy present runs, so the regular path assembles a run of present values
   by `arraycopy` rather than walking per element.

Judged on `NestedScanBenchmark`'s existing full-record workload, which reads Overture
Maps places in full and so drains repeated columns already.

## Completion

When stage 5 lands, the intended end state and the constraints it carries fold into
`_designs/ROW_READER.md` (and the columnar worker choice into `_designs/COLUMN_READER.md`),
and this plan is deleted.

## Not in scope

`BatchExchange.Batch` and `NestedBatch` stay separate classes: `Batch` is a leaf type so
the JIT keeps its field accesses monomorphic on the flat hot path. Aligning the names of
the fields that mean the same thing (`Batch.validity` and `NestedBatch.elementValidity`,
same polarity) costs nothing and is worth doing.

Drain-side batch filtering (`Batch.matches`, `BatchMatchMerger`, `ColumnBatchMatcher`)
exists only on the flat path, so a mixed schema loses it for its scalar columns as well.
Stage 5 gives those columns the flat worker and with it the filter; a genuinely repeated
column still has none, which is a capability gap in the neighbourhood of #222.

The `ColumnReader` drain has its own costs under `IndexMode.REAL_VIEW`, which no stage
here touches: #756 (leaf compaction on a batch with no phantom positions) and #1023
(`NestedLevelComputer.computeRealView` recomputed per leaf, 22.6% to 37.6% of self time
in nested columnar profiles). #1023 is the largest single item measured anywhere in this
area and needs coordination across sibling leaves, which a per-column worker cannot do
alone. Both are separate work on a separate surface.
