# Column reader

What `ColumnReader` exposes and how a batch reaches it: the layer model of a column's schema chain, sentinel-suffixed offsets, the `Validity` bitmap, fixed-width and variable-length leaf values, the real-items view that turns the pipeline's raw batches into that shape, and the cursor/scan/view split that advances one or several readers over one decode pipeline. How pages become published batches (row-group iteration, `PageSource`, `ColumnWorker`, `BatchExchange`, threading, batch sizing, `ReaderConfig`) is in [READ_PIPELINE.md](READ_PIPELINE.md); how definition and repetition levels become nested batches (index modes, the drain-side real view, bulk copy, the fixed-size-list fast path) is in [NESTED_DECODE.md](NESTED_DECODE.md); value decoders and dictionaries are in [VALUE_DECODE.md](VALUE_DECODE.md); exact filtering on the column readers (selection and compaction) is in [RECORD_FILTERING.md](RECORD_FILTERING.md). The row readers are in [ROW_READER.md](ROW_READER.md). User-facing semantics are in [docs/content/how-to/column-reader.md](../docs/content/how-to/column-reader.md), [docs/content/concepts/nested-columns.md](../docs/content/concepts/nested-columns.md) and [docs/content/concepts/reader-models.md](../docs/content/concepts/reader-models.md).

## Batch model and layers

A `ColumnReader` reads one leaf column. Each batch holds a number of top-level records (`getRecordCount()`) and the leaf values those records carry (`getValueCount()`). Between the root and the leaf, the column's schema chain surfaces as a sequence of **layers**, numbered `0..getLayerCount()-1` from outermost to innermost. The leaf itself is not a layer; it has its own accessors.

The model follows Apache Arrow's nested columnar layout: one validity bitmap per nullable scope, one offsets array per repeated scope, set bit meaning present, and an offsets-plus-bytes pair for variable-length values. The shared part is the model, not the bytes: validity is a packed `long[]`, offsets are `int[]`, value bytes are `byte[]`.

### Deriving layers

`NestedLevelComputer.computeLayers` walks the chain once per reader and returns a `Layers` descriptor (`LayerKind[] kinds` plus the definition-level thresholds that drive validity and item counting). Each node contributes zero or one layer:

| Schema node on the chain | Layer |
|---|---|
| `REQUIRED` group | none |
| User-authored `OPTIONAL` group, including a list element or map value | `STRUCT` |
| `LIST`- or `MAP`-annotated group, any encoding | one `REPEATED` |
| The synthetic `repeated group` directly inside a `LIST`/`MAP` | none (folded into the annotated group's layer) |
| Unannotated `repeated` group or primitive outside a `LIST`/`MAP` scaffold | `REPEATED` |
| `repeated` primitive that is the element of a `LIST`/`MAP` scaffold (legacy 2-level list) | none |

The annotation is checked before the scaffold rule: a `LIST`-annotated group directly inside another `LIST` (the legacy 2-level list of lists) is the element, itself a list, and contributes its own `REPEATED` layer.

A user's optional group keeps its `STRUCT` layer inside a list or map, so for `list<optional group customer { optional int32 age }>` the layers are `[REPEATED, STRUCT]` and customer-null stays distinct from age-null at every depth. A map reports `REPEATED`; `LayerKind` does not distinguish map from list, and a consumer that needs the distinction reads `getColumnSchema()`. `getLayerCount()` and `getLayerKind(k)` are fixed for the reader's lifetime and answer before the first `nextBatch()`.

### Items at a layer

Every per-layer buffer is sized to the number of items at that layer:

- `count(0) == getRecordCount()`.
- For `k > 0`: `count(k) == count(k-1)` when layer `k-1` is `STRUCT`, and `getLayerOffsets(k-1)[count(k-1)]` when it is `REPEATED`.
- `getValueCount() == count(getLayerCount())`.

Only `REPEATED` layers change cardinality. A column with only `STRUCT` layers above the leaf has `getValueCount() == getRecordCount()`. The batch size caps records, never leaf values, so a repeated column's `getValueCount()` can exceed it; a record is never split across batches.

Tests: `ColumnReaderLayerModelTest`, `UnannotatedRepeatedListTest`, `LegacyTwoLevelListOfListsTest`, `MapKeyOnlyTest`.

## Offsets and validity

### Offsets

`getLayerOffsets(k)` has length `count(k) + 1`. Entry `i` is the index of item `i`'s first child at layer `k+1` (or in the leaf array for the innermost layer), and the final entry is the sentinel `count(k+1)`, so `offsets[i+1]` is valid for every item and consumer loops need no last-item special case. Offsets are per batch and `int[]`.

`getLayerOffsets(k)` throws `IllegalStateException` on a `STRUCT` layer. `getLayerKind(k)`, `getLayerValidity(k)` and `getLayerOffsets(k)` throw `IllegalStateException` for `k` outside `[0, getLayerCount())`, and the last two also before the first batch.

### Real items only

Layer offsets and the leaf array count **real items only**. A null or empty container at a `REPEATED` layer contributes no child slot; there are no phantom positions. Consequences:

- `offsets[i+1] == offsets[i]` means container `i` has no entries. Whether it is null or present-and-empty is read from `getLayerValidity(k).isNull(i)`; there is no separate empty marker.
- `getInts()[j]` is the `j`-th real leaf of the batch, in record order.

Values at a position where the leaf or an enclosing scope is null are unspecified. A consumer checks validity before reading a value.

### Validity

`Validity` (`dev.hardwood`) is an interface with two internal implementations. It is shared vocabulary: the reader returns it and the writer accepts it as a column's null mask.

| Shape | Returned when | `hasNulls()` | `words()` |
|---|---|---|---|
| `Validity.NO_NULLS` (`NoNullsValidity`, identity-stable singleton) | no item at the scope is null in the batch | `false` | `null` |
| `BackedValidity` over a packed `long[]` | at least one item is null | `true` | the backing array, not copied |

`getLeafValidity()` covers `0..getValueCount()`, `getLayerValidity(k)` covers `0..count(k)`. There is no `null` return and no nullable bitmap for a consumer to forget.

**Bit layout.** Set bit means present. Word `w` covers items `[w*64, w*64+64)`, lowest bit lowest item. Bits at or past the scope's item count are undefined and never read. The accessors are named for nullability (`isNull`, `isNotNull`, `hasNulls`, `nullCount`, `nextNull`, `nextNotNull`), matching `RowReader.isNull`, while storage keeps Arrow's present polarity.

**The sparse "no nulls" signal.** Throughout the pipeline a `null` bitmap reference means every item at that scope is present: `BatchExchange.Batch.validity` for a flat batch, `NestedBatch.elementValidity`, and each slot of `RealView.layerValidity` / `RealView.leafValidity`. Producers allocate a bitmap lazily on the first absent item, back-filling the present bits before it, so an all-present batch allocates none. `Validity.of(long[])` maps `null` to `NO_NULLS`. The `hasNulls() == true` answer of the backed shape therefore relies on every internal producer publishing `null` rather than an all-set bitmap; `BackedValidity` does not inspect its words. Untested for producers.

**Caller-owned sizing.** `BackedValidity` carries no item count and performs no bounds check, so `isNull(i)` is one word load and mask. The count-taking methods take `count` because `NO_NULLS` has no intrinsic length. Whoever hands an array to `Validity.of` guarantees at least `(count + 63) >>> 6` words for every `count` and index later used, and does not mutate the array afterwards. Internal producers trim their bitmaps to exactly that word count.

`words()` gives consumers three loop shapes over one value: per item through `isNull(i)`, an inlined word/mask test, or a word-wise scan with `Long.numberOfTrailingZeros` that skips runs of nulls. The user-facing loop guidance, including hoisting `hasNulls()` out of the loop, is in [docs/content/how-to/column-reader.md](../docs/content/how-to/column-reader.md).

Tests: `ValidityTest`, `ColumnReaderLayerModelTest`, `ColumnReadersTest`.

## Leaf values

### Fixed-width

`getInts()`, `getLongs()`, `getFloats()`, `getDoubles()` and `getBooleans()` return the typed leaf array. The accessor matching the column's physical type is the only one that succeeds; any other throws `IllegalStateException` naming the physical type. The returned array has length exactly `getValueCount()`: a worker's capacity-sized array is trimmed with a copy when the batch is short, so a capacity tail is never exposed. Physical values only; logical-type conversion is not applied.

### Variable-length

`BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` and `INT96` leaves are carried as `BinaryBatchValues` (`internal.reader`), a `(byte[] bytes, int[] offsets)` pair, instead of one `byte[]` per value:

- `getBinaryOffsets()` has length `getValueCount() + 1` and is sentinel-suffixed; value `i` occupies `[offsets[i], offsets[i+1])`. For `FIXED_LEN_BYTE_ARRAY` the offsets are `i * width`.
- `getBinaryValues()` is **capacity-sized**: only `[0, offsets[getValueCount()])` is meaningful, and bytes past it are unspecified. Consumers bound reads by the offsets, never by `bytes.length`.
- Both accessors throw `IllegalStateException` on a fixed-width column.

Layer offsets and binary offsets are orthogonal: layer offsets say which leaf values belong to a container, binary offsets say which bytes belong to a leaf value.

The total bytes of one batch are capped at `Integer.MAX_VALUE`, since offsets are `int`. The append path (`BinaryBatchValues.appendAt`) fails the read when a batch would exceed it; the remedy is a smaller batch size for that column. Where a batch has phantom positions, the drain gathers the real values into a second buffer while the raw one stays referenced by the batch, so peak binary memory for such a batch is up to twice its raw bytes.

`getBinaries()` and `getStrings()` materialise one `byte[]` or `String` per leaf, `null` at null positions, and cache the result per batch. `getStrings()` requires a text column (`BYTE_ARRAY` annotated `STRING`, `ENUM` or `JSON`, or unannotated) and throws `IllegalArgumentException` otherwise; on a dictionary-encoded `STRING`, `ENUM` or `JSON` column it returns one interned `String` per dictionary entry through the batch's dictionary indices (see [VALUE_DECODE.md](VALUE_DECODE.md)), and on an unannotated column one `String` per value. Both arrays have length `getValueCount()`, not `getRecordCount()`.

Tests: `ColumnReaderLayerModelTest`, `ColumnReadersTest`.

## Real view

Internally a nested batch (`NestedBatch`) is **raw-counting**: every position in the definition/repetition-level stream occupies a slot, including phantom positions for null or empty parents. The row readers read that shape directly. `ColumnReader` exposes the real-items shape, and `NestedLevelComputer.RealView` is the translation: per-layer sentinel-suffixed offsets, per-layer and leaf validity (all over real-item indices, `null` when all present), the real leaf count, and `realToRawLeaf`, the gather map from real leaf index to raw position, `null` when the map would be the identity (no `REPEATED` layer, or no phantom positions in the batch) and the raw values pass through.

`NestedLevelComputer.computeRealView` builds it in one pass over the raw levels, using the `Layers` item thresholds: a raw position is an item at layer `k` when its repetition level is at most the number of `REPEATED` layers before `k` and its definition level reaches layer `k`'s content; it is present when its definition level reaches layer `k`'s threshold.

Where the view is built depends on the read:

| Read | Built by | Leaf values |
|---|---|---|
| Unfiltered | the drain, before publish (`IndexMode.REAL_VIEW`) | gathered on the drain into `NestedBatch.realValues`; raw levels are dropped |
| Unfiltered, all-present batch | the drain, offsets only, no validity or gather map | pass through |
| Fixed-size-list batch | arithmetic offsets, no validity | pass through |
| Filtered | `ColumnReader`, lazily on first access, from the compacted levels (`IndexMode.REAL_VIEW_KEEP_LEVELS`) | gathered on first access |

Building on the drain keeps the per-batch level scan off the single consumer thread. The drain-side modes and the fixed-size-list fast path are described in [NESTED_DECODE.md](NESTED_DECODE.md); the compaction that forces the lazy path is in [RECORD_FILTERING.md](RECORD_FILTERING.md).

**Routing.** A column whose chain contributes any layer, or whose leaf repeats, decodes through `NestedColumnWorker`, because per-layer validity needs definition levels. A column with no layer takes `FlatColumnWorker`, publishes a `BatchExchange.Batch` with a typed value array and one leaf bitmap, and has no real view: its leaf array and validity are already real-items. `ColumnCursor.isNested` holds the rule, and `ColumnReader` applies the same rule to choose its accessor path.

Tests: `ColumnReaderLayerModelTest`, `FixedSizeListFastPathReadTest`, `ColumnReaderExactFilterTest`.

## Access: cursor and scan

Every column-reader read (one `ColumnReader`, an unfiltered `ColumnReaders` group, a filtered group, a filtered single column) runs on one model in `dev.hardwood.reader`:

- **`ColumnCursor`** (package-private) owns one decoded column's worker, its `BatchExchange` and its current raw batch. `advance()` polls the exchange, rethrows a pipeline error, and returns `false` at end of stream and on every call after it. It exposes the current batch, record count, file name and `filterAlwaysMatches`, and `close()` stops the worker. It holds no accessor or cache state.
- **`ColumnScan`** (package-private) owns the cursors of every decoded column, in `ReadProjection.decoded()` order (payload columns first, then filter-only predicate columns), the shared `RowGroupIterator`, and for a filtered read the `SelectionEngine`. One iterator feeds every cursor, which is what keeps the columns row-aligned.
- **Views.** `ColumnReader` holds its scan and the index of its payload cursor plus per-batch caches (the real view, trimmed or gathered leaf arrays, trimmed binary offsets, materialised binaries and strings). `ColumnReaders` holds the scan and one `ColumnReader` per payload column. Views own no pipeline state.

Each scan has one owner, which advances and closes it. A single `ColumnReader` owns a one-column scan (`ParquetFileReader.buildSingleColumnReader`) and is its only view; a filtered single column's scan also holds the predicate columns' cursors, which have no view. A `ColumnReaders` owns its scan, and its `ColumnReader`s are group members: views that neither advance nor close it. Single-column readers are single-file; `ColumnReaders` spans the files of an `openAll` reader.

### Index order

`ColumnReaders.getColumnReader(int)` follows the projection's names: index `i` is the `i`-th leaf column the names select, each name's leaves in file schema order (`ProjectedSchema#requestedColumn`). A column several names select sits at each of their positions, all of them holding the same view, so a repeat decodes and derives nothing twice. Leaves are addressed by full path, so a name and the result it produces carry the same qualified name; the row readers, which nest, give each node one position instead (see [ROW_READER.md](ROW_READER.md#index-space)).

Tests: `ProjectionOrderTest`.

### Advancing the scan

`ColumnScan.advance()` polls every payload cursor once, then every filter-only cursor unless the first cursor's batch is proven by statistics (see [RECORD_FILTERING.md](RECORD_FILTERING.md#filter-only-column-skip)). It checks that the polled cursors agree: every one produced a batch, all with the same record count. A cursor exhausted early or a differing record count throws `IllegalStateException`. When the first cursor reaches the end, `advance()` drains every other cursor, and one that still produces a batch throws `IllegalStateException`. With a filter, it then computes the selection and compacts each payload cursor's batch to the matching records.

A read in which pruning dropped every row group gets a scan with no cursors (`ColumnScan.empty`), whose first `advance()` returns `false` and starts no worker.

**View advance.** Only the owner advances a scan. `ColumnReaders.nextBatch()` advances it and has every member take up the new step; a single `ColumnReader`'s `nextBatch()` does the same for itself. A member's `nextBatch()` throws `IllegalStateException`. A member therefore always shows the group's current step, and no member can pair rows from a different step or drop one.

Readers built separately, each from its own `buildColumnReader`, are separate scans with their own batch sizes; their nth batches cover different rows. Row-aligned multi-column access requires one `ColumnReaders`.

**Closing.** Closing the owner closes the scan: it closes every cursor, then releases the iterator even when a cursor's teardown fails. Afterwards `nextBatch()` throws `IllegalStateException`. Closing a member has no effect. Close is idempotent.

Tests: `ColumnReadersTest`, `IteratorTrackingTest`, `PrunedToEmptyReadTest`. The lockstep checks (early exhaustion, diverging record counts, a batch after the first cursor ended) are defensive guards. Untested.

## Lifetime and ownership of batch buffers

Column-reader cursors publish through a **detaching** `BatchExchange`: every published batch is freshly allocated and never recycled, and back-pressure comes from the bounded ready queue ([READ_PIPELINE.md](READ_PIPELINE.md)). Every array and `Validity` a `ColumnReader` accessor returns belongs to the current batch and is never reused or overwritten by a later `nextBatch()`. A consumer may keep a returned array after advancing and hand it to another thread. The public contract is stated in the `ColumnReader` JavaDoc and the how-to.

Two mechanisms rely on this ownership and must keep it:

- Filtered flat compaction gathers fixed-width values in place in the published batch. This is safe because the batch is consumer-owned and no view has observed it yet when the scan compacts it.
- Nested workers reuse their accumulators across batches and publish copies (trimmed arrays, a fresh `BinaryBatchValues`). A full fixed-size-list batch hands its accumulator over and starts the next batch on a fresh one.

The reader is a single-threaded cursor: one consumer thread calls `nextBatch()` and the accessors. Per-batch caches on the view are dropped whenever the view adopts a new step. `Validity.NO_NULLS` is shared across batches, and is immutable.

Tests: `ColumnReaderBatchArrayIdentityTest`. Freshness on the filtered path is untested.

## Boundaries

- **Arrow-compatible buffers (#153).** Validity, offsets and value bytes are Java arrays, and the public accessors return them as arrays (`Validity.words()`, `getInts()`, `getBinaryValues()`, `getLayerOffsets()`). A layout Arrow consumers could take without a copy needs accessors of its own, since these signatures cannot expose off-heap buffers without copying each batch; the model (validity polarity, sentinel offsets, real items only) carries over unchanged.
- **Caller-provided buffers (#737).** Every batch allocates fresh arrays; a fill-into-caller-buffers contract for zero-allocation reads does not exist.
- **Logical types (#514).** The accessors return physical values only; logical conversion for columnar consumers is not exposed.
- **API stability (#522).** `ColumnReader`, `LayerKind` and `Validity` are `@Experimental`.
- **Per-batch `int` offsets.** Layer and binary offsets are `int[]`, which bounds one batch to `Integer.MAX_VALUE` leaf values and bytes. Offsets are per batch, so a file larger than that is not affected; a batch beyond it would need a separate API decision.
- **Compressed leaf shapes.** Leaves are materialised as typed arrays or `(bytes, offsets)`. Dictionary or run-end encoded leaves are not part of the public shape.
