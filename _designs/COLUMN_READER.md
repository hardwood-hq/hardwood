# Column reader

Describes what `ColumnReader` exposes and how a batch reaches it: the layer model of a column's schema chain, sentinel-suffixed layer offsets, the `Validity` bitmap, fixed-width and variable-length leaf values and dictionary ids, the real-items view that turns the pipeline's raw batches into that shape, and the cursor/scan/view split that advances one or several readers over one decode pipeline.

Related documents:

- [READ_PIPELINE.md](READ_PIPELINE.md): how pages become published batches (row-group iteration, `PageSource`, the scheduled columns, `BatchExchange`, threading, batch sizing, `ReaderConfig`)
- [NESTED_DECODE.md](NESTED_DECODE.md): how definition and repetition levels become nested batches (cells, index modes, the real view built before publish, the fixed-size-list fast path)
- [VALUE_DECODE.md](VALUE_DECODE.md): value decoders and dictionaries
- [RECORD_FILTERING.md](RECORD_FILTERING.md): exact filtering on the column readers (selection and compaction)
- [ROW_READER.md](ROW_READER.md): the row readers
- [docs/content/how-to/column-reader.md](../docs/content/how-to/column-reader.md), [docs/content/concepts/nested-columns.md](../docs/content/concepts/nested-columns.md) and [docs/content/concepts/reader-models.md](../docs/content/concepts/reader-models.md): user-facing semantics

## Batch model and layers

A `ColumnReader` reads one leaf column. Each batch holds a number of top-level records (`getRecordCount()`) and the leaf values those records carry (`getValueCount()`). Between the root and the leaf, the column's schema chain surfaces as a sequence of **layers**, numbered `0..getLayerCount()-1` from outermost to innermost. The leaf itself is not a layer; it has its own accessors.

The model follows Apache Arrow's nested columnar layout: one validity bitmap per nullable scope, one offsets array per repeated scope, set bit meaning present, and an offsets-plus-bytes pair for variable-length values. The shared part is the model, not the bytes: validity is a packed `long[]`, offsets are `int[]`, value bytes are `byte[]`.

### Deriving layers

`NestedLevelComputer.computeLayers` derives each leaf's `Layers` descriptor: each node on a leaf's chain contributes zero or one layer, a `STRUCT` for an optional group and a `REPEATED` for a list, a map or an unannotated repeated field. The rules per node, and the definition-level thresholds that drive validity and item counting, are in [NESTED_DECODE.md](NESTED_DECODE.md#schema-to-layers).

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

`getInts()`, `getLongs()`, `getFloats()`, `getDoubles()` and `getBooleans()` return the typed leaf array. The accessor matching the column's physical type is the only one that succeeds; any other throws `IllegalStateException` naming the physical type. The returned array has length exactly `getValueCount()`: a batch's array, sized to its capacity, is trimmed with a copy when the batch is short, so a capacity tail is never exposed. Physical values only; logical-type conversion is not applied.

### Variable-length

`BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` and `INT96` leaves are carried as `BinaryBatchValues` (`internal.reader`): a list of buffers and, per value, a buffer id and a view `[starts[i], ends[i])` into that buffer, instead of one `byte[]` per value. `getBinaryValues()` exposes them as a `BinaryValues`, read by value index:

- **A per-value window.** Value `i` is `[start(i), end(i))` of `buffer(i)`, for `i` in `[0, size())` with `size() == getValueCount()`. A null value's range is empty. A caller reads a value's bytes from its own window; the window is the public shape so that the storage behind it stays the reader's to choose. `buffer(i)` promises no sharing between values, no identity across calls and no zero copy: storage the window cannot point into is served from a copy, and an off-heap accessor can be added beside it.
- **No per-byte accessor.** A per-byte read checked against the value's length keeps its check inside the caller's loop; an unchecked one reads past the value into its neighbour. A loop over the window compiles as one over a raw array.
- **No per-value string accessor.** Taking a batch's windows builds its dictionary values' views (see **Deferred views** below); `getStrings()` resolves strings through the dictionary without them and is the string accessor.
- **A final class.** Its call sites in a caller's loop stay monomorphic and inline. A callback API would put one call site inside the library, megamorphic across callers.
- Views are not contiguous or ordered, and may overlap. A value from a dictionary-encoded page points at its entry in the dictionary's own bytes, so values naming one entry cover the same bytes; a value from a plain page is copied once, by its segment, into a chunk of the batch ([READ_PIPELINE.md](READ_PIPELINE.md#scheduled-columns)).
- `getBinaryValues()` throws `IllegalStateException` on a fixed-width column.

Views are what keep a dictionary-encoded column from costing a byte copy per value. A batch of binary values draws on at most one dictionary ([Dictionary ids](#dictionary-ids)), whose bytes it takes as one of its buffers: building its dictionary values' views records a buffer id and two `int`s per value and copies no bytes. Views must stay free to overlap and to leave value order: that freedom is what lets a dictionary value point at its entry instead of being copied.

**Bounded chunks.** A segment copies its plain values' bytes into chunks of the batch of at most 256 KB (`PlainBytes`, `BinaryBatchValues.CHUNK_BYTES`), a value never spanning two, so a batch allocates no array whose size grows with the batch. G1 allocates an array larger than half a heap region as a humongous object, in whole old-generation regions, and the smallest region is 1 MB; a humongous array per batch starts a concurrent cycle per batch. A value longer than a chunk gets a buffer of its own length, the one humongous allocation a read of such values cannot avoid. A dictionary's bytes are the column chunk's, held once for every batch drawing on it. A flat row reader's recycled holder keeps the full chunks of its last fill for its next one (`BinaryBatchValues.keepChunks`), so a read allocates its chunks once per holder rather than once per batch: its batches are read before the holder is refilled. A fill there takes a full chunk where at least half a chunk's bytes remain and an exact-size buffer otherwise, so a full chunk is at most half empty and a segment of few bytes holds no more than them. Such a batch shares no buffer with another: compacting it fails. A column reader's batches are detached, and their chunks, like their arrays, are not reused.

**Deferred views.** A dictionary value's view is built when a reader first needs it, not as the cell is filled, and the view arrays are allocated on their first write: a consumer of the dictionary ids, or of strings, which resolve through them, never pays for views. `BinaryBatchValues` keeps its bytes and views private behind accessors that build pending views first, so no reader can see a value without its view. Building them is synchronized, and the pending flag is volatile and cleared only once the views are written: a row-reader value handed to another thread reads its batch's views there while the reader thread may be building them, and either thread sees them complete. The first build of deferred views raises the column's `ViewDemand`, and the column builds views as it completes a cell from then on; only the batches in flight at that moment have theirs built on the reading thread. A flat column whose batch matcher reads every value's view (`ColumnBatchMatcher.readsEveryValueView()`) builds them as each cell completes. A dictionary-space binary matcher reads only the views of values without an entry id, so its column keeps its views deferred ([DICTIONARY_SPACE_EVALUATION.md](DICTIONARY_SPACE_EVALUATION.md)). Filtered compaction keeps the kept dictionary values deferred and shares the raw batch's buffers with the compacted one; no later fill writes them.

Layer offsets and binary views are orthogonal: layer offsets say which leaf values belong to a container, binary views say which bytes belong to a leaf value.

A view is `int`, which bounds one buffer, not the batch: a batch's bytes are spread over its buffers. Where a nested batch has phantom positions, the thread that fills it gathers the real values' views; the gathered batch shares the raw batch's buffers, which it never outlives.

`getBinaries()` and `getStrings()` materialise one `byte[]` or `String` per leaf, `null` at null positions, and cache the result per batch. `getStrings()` requires a text column (`BYTE_ARRAY` annotated `STRING`, `ENUM` or `JSON`, or unannotated) and throws `IllegalArgumentException` otherwise; on a dictionary-encoded `STRING`, `ENUM` or `JSON` column it returns one interned `String` per dictionary entry through the batch's dictionary indices (see [VALUE_DECODE.md](VALUE_DECODE.md)), and on an unannotated column one `String` per value. Both arrays have length `getValueCount()`, not `getRecordCount()`.

Tests: `ColumnReaderLayerModelTest`, `ColumnReadersTest`, `BinaryBatchValuesTest`, `ScheduledBinaryViewsTest`, `ParallelBinaryFillTest`.

### Dictionary ids

`getDictionaryIds()` gives each value's entry in the batch's dictionary, and `getBinaryDictionary()` the dictionary itself: a `BinaryDictionary` (`size()`, `getBinary(e)`, `getString(e)`) over the chunk's `ByteArrayDictionary`. `BinaryDictionary` is the one subtype so far of the sealed `ColumnDictionary`; a primitive dictionary would join it as its own subtype with a typed accessor (`getIntDictionary()` and so on), the way the leaf accessors are typed per physical storage, while `getDictionaryIds()` stays type-neutral. A consumer that works per entry (aggregating, grouping, mapping to its own keys) does that work once per dictionary instead of once per value.

- **One dictionary per batch.** A read that decodes a binary leaf ends every batch at a row-group boundary ([READ_PIPELINE.md](READ_PIPELINE.md#when-a-batch-closes)), so a batch never draws on two column chunks' dictionaries.
- **All or nothing.** The ids are returned only when every non-null value of the batch has one; a batch holding a value a writer stored plain after its dictionary filled up returns `null` from both accessors, and the consumer reads the binary views. The fill records whether every segment of the batch wrote ids (`BinaryBatchValues.idsComplete`), and a compacted batch inherits the record; `ColumnReader` checks the ids against the leaf validity only when it is unset, so a batch whose id-less segments hold only nulls still returns ids, and a filtered batch is judged by the values it kept. The id at a null is `-1`, which flat and nested cells both record, so a consumer that skips nulls by id, or forgets to, fails on index `-1` instead of counting nulls into entry 0.
- **One object per dictionary.** `ColumnReader` keeps the last `BinaryDictionary` it handed out and reuses it while the batch's `ByteArrayDictionary` is the same, so a consumer detects a new dictionary by identity. Its entries never change, and it hands out copies (`getBinary`) or the cached `String`s (`getString`), never the dictionary's own arrays, so it needs no exception to the ownership rule below. The `String` cache is filled lazily with plain writes, shared with string reuse on the row readers; values are always correct, but two threads may decode the same entry once each, so the contract promises equal strings, not one instance. `getString` refuses a column that does not hold text, which is why the public type wraps the internal dictionary together with the column rather than exposing it.
- **Binary columns only.** A primitive dictionary's values are looked up into typed arrays as the cell is filled, and the batch keeps no ids; `getDictionaryIds()` returns `null` for them, so ids for a primitive column can be added later without changing documented behaviour, while `getBinaryDictionary()` throws on them like the other binary accessors.

Tests: `ColumnReaderDictionaryIdsTest`, `DictionaryFallbackNullsTest`.

## Real view

Internally a nested batch (`NestedBatch`) is **raw-counting**: every position in the definition/repetition-level stream occupies a slot, including phantom positions for null or empty parents. The row readers read that shape directly. `ColumnReader` exposes the real-items shape, and `NestedLevelComputer.RealView` is the translation: per-layer sentinel-suffixed offsets, per-layer and leaf validity (all over real-item indices, `null` when all present), the real leaf count, and `realToRawLeaf`, the gather map from real leaf index to raw position. The map is `null` in two cases: where it would be the identity (no `REPEATED` layer, or no phantom positions in the batch), so the raw values pass through; and on a view the fill built for values it wrote at their real positions ([NESTED_DECODE.md](NESTED_DECODE.md#real-view)).

`NestedLevelComputer.computeRealView` builds it layer by layer over the raw levels, using the `Layers` item thresholds: a raw position is an item at layer `k` when its repetition level is at most the number of `REPEATED` layers before `k` and its definition level reaches layer `k`'s content; it is present when its definition level reaches layer `k`'s threshold. Each layer, and the leaf, is counted before it is filled, so its arrays are allocated at their exact size and a validity or gather map is allocated only when an item is absent or a position is a phantom. A column without repetition levels has only `STRUCT` layers: every position is an item of every layer, and each validity marks the positions whose definition level reaches the layer's threshold. `ReferenceRealView` in the test sources is an independent formulation the view is checked against on random levels (`RealViewEquivalenceTest`).

An unfiltered read builds the view on the thread that fills the batch, before publish, which keeps the per-batch level scan off the single consumer thread; a filtered read builds it lazily in `ColumnReader` from the levels record selection compacted ([NESTED_DECODE.md](NESTED_DECODE.md#index-modes), [RECORD_FILTERING.md](RECORD_FILTERING.md#column-readers)).

**Routing.** Only a column decoded through its levels (`ColumnCursor.isNested`, [NESTED_DECODE.md](NESTED_DECODE.md#which-columns-are-nested)) has a real view. A flat column publishes a `BatchExchange.Batch` with a typed value array and one leaf bitmap, which are already real-items. `ColumnReader` applies the same rule to choose its accessor path.

Tests: `ColumnReaderLayerModelTest`, `FixedSizeListFastPathReadTest`, `ColumnReaderExactFilterTest`.

## Access: cursor and scan

Every column-reader read (one `ColumnReader`, an unfiltered `ColumnReaders` group, a filtered group, a filtered single column) runs on one model in `dev.hardwood.reader`:

- **`ColumnCursor`** (package-private) owns one decoded column's `ScheduledColumn`, its `BatchExchange` and its current raw batch. `advance()` polls the exchange, rethrows a pipeline error, and returns `false` at end of stream and on every call after it. It exposes the current batch, record count, file name and `filterAlwaysMatches`, and `close()` stops the column. It holds no accessor or cache state.
- **`ColumnScan`** (package-private) owns the cursors of every decoded column, in `ReadProjection.decoded()` order (payload columns first, then filter-only predicate columns), the shared `RowGroupIterator`, and for a filtered read the `SelectionEngine`. One iterator feeds every cursor, which is what keeps the columns row-aligned.
- **Views.** `ColumnReader` holds its scan and the index of its payload cursor plus per-batch caches (the real view, trimmed or gathered leaf arrays, trimmed binary views, materialised binaries and strings). `ColumnReaders` holds the scan and one `ColumnReader` per payload column. Views own no pipeline state.

Each scan has one owner, which advances and closes it. A single `ColumnReader` owns a one-column scan (`ParquetFileReader.buildSingleColumnReader`) and is its only view; a filtered single column's scan also holds the predicate columns' cursors, which have no view. A `ColumnReaders` owns its scan, and its `ColumnReader`s are group members: views that neither advance nor close it. Single-column readers are single-file; `ColumnReaders` spans the files of an `openAll` reader.

### Index order

`ColumnReaders.getColumnReader(int)` follows the projection's names: index `i` is the `i`-th leaf column the names select, each name's leaves in file schema order (`ProjectedSchema#requestedColumn`). A column several names select sits at each of their positions, all of them holding the same view, so a repeat decodes and derives nothing twice. Leaves are addressed by full path, so a name and the result it produces carry the same qualified name; the row readers, which nest, give each node one position instead (see [ROW_READER.md](ROW_READER.md#index-space)).

Tests: `ProjectionOrderTest`.

### Advancing the scan

`ColumnScan.advance()` polls every payload cursor once, then every filter-only cursor unless the first cursor's batch is proven by statistics (see [RECORD_FILTERING.md](RECORD_FILTERING.md#filter-only-column-skip)). It checks that the polled cursors agree: every one produced a batch, all with the same record count. A cursor exhausted early or a differing record count throws `IllegalStateException`. When the first cursor reaches the end, `advance()` drains every other cursor, and one that still produces a batch throws `IllegalStateException`. With a filter, it then computes the selection and compacts each payload cursor's batch to the matching records.

A read with no row group left gets a scan with no cursors (`ColumnScan.empty`), whose first `advance()` returns `false` and starts no column. Before it builds its cursors, the read probes the bloom filters and dictionaries of its leading row groups on the calling thread (`RowGroupIterator.hasLiveWorkItem`, which the row readers ask as well), which hands each dictionary page to the context's pool to parse, up to the first one they do not drop, so a read that every probe drops, such as a point lookup of an absent key, starts no column and allocates no batch. The decisions are cached per work item, and the columns reuse them; the surviving row group's page index is still read by a column's fetcher, so a failure there carries the column it was read for.

Tests: `LiveWorkItemTest`, `EmptyOffsetIndexTest`.

**View advance.** Only the owner advances a scan. `ColumnReaders.nextBatch()` advances it and has every member take up the new step; a single `ColumnReader`'s `nextBatch()` does the same for itself. A member's `nextBatch()` throws `IllegalStateException`. A member therefore always shows the group's current step, and no member can pair rows from a different step or drop one.

Readers built separately, each from its own `buildColumnReader`, are separate scans with their own batch sizes; their nth batches cover different rows. Row-aligned multi-column access requires one `ColumnReaders`.

**Closing.** Closing the owner closes the scan: it closes every cursor, then releases the iterator even when a cursor's teardown fails. Afterwards `nextBatch()` throws `IllegalStateException`. Closing a member has no effect. Close is idempotent.

Tests: `ColumnReadersTest`, `IteratorTrackingTest`, `PrunedToEmptyReadTest`. The lockstep checks (early exhaustion, diverging record counts, a batch after the first cursor ended) are defensive guards. Untested.

## Lifetime and ownership of batch buffers

Column-reader cursors publish through a **detaching** `BatchExchange`: every published batch is freshly allocated and never recycled, and back-pressure comes from a limit on the rows the ready queue holds ([READ_PIPELINE.md](READ_PIPELINE.md#batchexchange)). Every array and `Validity` a `ColumnReader` accessor returns belongs to the current batch and is never reused or overwritten by a later `nextBatch()`, except `Validity.NO_NULLS`; the `BinaryDictionary` from `getBinaryDictionary()` is shared by the batches of one dictionary and its entries never change ([Dictionary ids](#dictionary-ids)). A consumer may keep a returned array after advancing and hand it to another thread. The public contract is stated in the `ColumnReader` JavaDoc and the how-to.

Two mechanisms rely on this ownership and must keep it:

- Filtered flat compaction gathers fixed-width values in place in the published batch. This is safe because the batch is consumer-owned and no view has observed it yet when the scan compacts it.
- A nested cell allocates its batch's arrays at their final size and writes each value into them once ([NESTED_DECODE.md](NESTED_DECODE.md#cells)), so no array is shared with another batch or written after publish.

The reader is a single-threaded cursor: one consumer thread calls `nextBatch()` and the accessors. Per-batch caches on the view are dropped whenever the view adopts a new step. `Validity.NO_NULLS` is shared across batches, and is immutable.

Tests: `ColumnReaderBatchArrayIdentityTest`. Freshness on the filtered path is untested.

## Boundaries

- **Arrow-compatible buffers (#153).** Validity, offsets and value bytes are Java arrays, and the public accessors return them as arrays (`Validity.words()`, `getInts()`, `getLayerOffsets()`) or windows into arrays (`getBinaryValues()`). A layout Arrow consumers could take without a copy needs accessors of its own, since these signatures cannot expose off-heap buffers without copying each batch; the model (validity polarity, sentinel layer offsets, real items only) carries over unchanged.
- **Caller-provided buffers (#737).** Every batch allocates fresh arrays; a fill-into-caller-buffers contract for zero-allocation reads does not exist.
- **Logical types (#514).** The accessors return physical values only; logical conversion for columnar consumers is not exposed.
- **API stability (#522).** `ColumnReader`, `LayerKind` and `Validity` are `@Experimental`.
- **Per-batch `int` offsets.** Layer offsets and binary views are `int[]`, which bounds one batch to `Integer.MAX_VALUE` leaf values, and one of its binary buffers to as many bytes. They are per batch, so a file larger than that is not affected; a batch beyond it would need a separate API decision.
- **Compressed leaf shapes.** Leaves are materialised as typed arrays or binary views, with dictionary ids for binary leaves. Primitive dictionary ids and run-end encoded leaves are not part of the public shape.
