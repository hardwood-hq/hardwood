# Nested decode

How the reader turns a nested column's repetition and definition levels into nested batches: the mapping from schema to layers, level decoding and its pooled scratch, the drain's per-batch assembly, what each `IndexMode` derives, the real-items view built on the drain, and the two fast paths that skip per-element work (the all-present page bulk copy and the fixed-size-list fast path). The worker pipeline, threading, reorder buffer and batch sizing are in [READ_PIPELINE.md](READ_PIPELINE.md); what `ColumnReader` exposes (layers, offsets, `Validity`, variable-length leaves, the cursor and scan) is in [COLUMN_READER.md](COLUMN_READER.md); how `NestedRowReader` reads the all-items batch is in [ROW_READER.md](ROW_READER.md); value decoders, dictionaries and leaf kinds are in [VALUE_DECODE.md](VALUE_DECODE.md). Record selection on a filtered column read is in [RECORD_FILTERING.md](RECORD_FILTERING.md). The user-facing layer model is in [docs/content/concepts/nested-columns.md](../docs/content/concepts/nested-columns.md).

## Which columns are nested

A column is decoded by `NestedColumnWorker` into `NestedBatch`es when:

| Reader | Condition |
|---|---|
| `ColumnReader` / `ColumnReaders` | the column has at least one layer or `maxRepetitionLevel > 0` (`ColumnCursor.isNested`). A leaf below required groups only is flat. |
| `NestedRowReader` | every projected column, flat or not, once `FileSchema.isFlatSchema()` is false. |

All other columns go through `FlatColumnWorker` (see [READ_PIPELINE.md](READ_PIPELINE.md)).

## Schema to layers

`NestedLevelComputer.computeLayers(root, columnIndex)` walks the schema from the root to the leaf and emits a `Layers` descriptor: one entry per layer, outermost first, each a `LayerKind` and a definition-level **threshold** at or above which the node of that layer is present.

| Node on the path | Contributes |
|---|---|
| `LIST`- or `MAP`-annotated group | `REPEATED`, threshold = the group's max definition level |
| Any node whose parent is a `LIST`/`MAP` group and that is not itself annotated (the synthetic `repeated group`, a legacy 2-level element, the `key_value` group) | nothing: the enclosing annotated group already accounts for its repetition |
| Unannotated `OPTIONAL` group | `STRUCT`, threshold = the group's max definition level |
| Unannotated `REPEATED` group, parent not `LIST`/`MAP` | `REPEATED`, threshold = the group's max definition level − 1 |
| `REPEATED` primitive, parent not `LIST`/`MAP` | `REPEATED`, threshold = the leaf's max definition level − 1 |
| `REQUIRED` group, non-repeated primitive | nothing |

The annotation is checked before the parent: a `LIST` group whose parent is a `LIST` is a nested list and contributes its own layer, which is how `group A (LIST) { repeated group B (LIST) { … } }` yields two layers.

**Invariant.** The number of `REPEATED` layers equals the leaf's `maxRepetitionLevel`. Offsets are computed per repetition level and remapped onto layer positions, so a descriptor that miscounts repetition misaligns every offset array. Nothing checks this at runtime.

From the thresholds `Layers.of` derives, per layer, the minimum definition level at which an *item slot* exists (`itemDefThresholds`) and the number of `REPEATED` layers above it (`itemRepThresholds`), with one extra entry for the leaf. A `STRUCT` layer passes its parent's item threshold through (a struct does not expand the item stream); a `REPEATED` layer's items exist at its threshold + 1. A chain without `REPEATED` layers has a leaf item threshold of `0`: every position is a real leaf, one per record.

### Unannotated repeated fields

The Parquet format reads a repeated field that is neither `LIST`/`MAP`-annotated nor inside such a group as a required list of required elements whose element type is the field's type ([LogicalTypes, Nested Types](https://parquet.apache.org/docs/file-format/types/logicaltypes/#nested-types)). The reader applies this on both paths:

- **Layers** (`ColumnReader`): the field contributes a `REPEATED` layer whose threshold is one below the field's own definition level, the level of the implicit required container. A repeated primitive then has its leaf items at its own max definition level; a repeated group has its children's items at the group's definition level.
- **Row assembly** (`NestedRowReader`): `TopLevelFieldMap.buildBareRepeatedListDesc` wraps the field in a synthetic required `LIST` group whose levels are one below the field's, and builds a `FieldDesc.ListOf` with `nullDefLevel = maxDef − 1` and `elementDefLevel = maxDef`. The element is the field itself, never unwrapped: a repeated primitive is a list of scalars, a repeated group a list of structs. Detection is `repetitionType() == REPEATED` at the two sites that build top-level fields and struct children; list elements and map values never reach them with their repeated wrapper intact.

These levels coincide with a legacy 2-level list, so `PqListImpl` needs no special case.

### Legacy list element rules

`SchemaNode.GroupNode.getListElement()` resolves a `LIST` group's element by the format's [backward-compatibility rules](https://parquet.apache.org/docs/file-format/types/logicaltypes/#backward-compatibility-rules), in this order: a repeated primitive is the element; a repeated group with several fields is the element; a repeated group whose single field is itself repeated is the element (a list of lists, since a synthetic 3-level wrapper never has a repeated child); a repeated group named `array` or `<list>_tuple` is the element; otherwise the repeated group's single child is. The third rule precedes the naming rule, so `mylist (LIST) { repeated group bag { repeated int32 num } }` reads as `list<struct<num: list<int>>>` and its leaf gets the same two `REPEATED` layers, and the same offsets, as the fully annotated `list<list<int>>`.

Tests: `ColumnReaderLayerModelTest`, `UnannotatedRepeatedListTest`, `LegacyTwoLevelListOfListsTest`, `ListBackwardCompatRulesTest`, `NestedSchemaTest`.

## Level decode and scratch

`PageDecoder` decodes a page's level streams (RLE / bit-packing hybrid) before its values.

- **Repetition levels** are always materialised for a column with `maxRepetitionLevel > 0`, except on a page the [fixed-size-list fast path](#fixed-size-list-fast-path) takes.
- **Definition levels** go through the all-present gate: `RleBitPackingHybridDecoder.isSingleRleRunOf(maxDef, numValues)` inspects one run header and value. When the stream is a single RLE run of `maxDef` covering every value, no array is produced and the page's definition levels are `null`. A `null` array is the reader-wide all-present convention, shared with required columns (`Page.allPresent()`); value decoders read densely from it.

**Pooled scratch.** Level arrays are borrowed from a `PageDecoder.LevelScratch`, not allocated per page. `ColumnWorker` owns one `LevelScratch` per reorder-buffer slot and passes the slot's holder to `decodePage`; the holder grows its arrays in place when a page needs more and otherwise hands back the same arrays. The two-argument `decodePage` overload used by tooling and tests passes a fresh holder. The pooling rests on three facts:

| Fact | Why it holds |
|---|---|
| One decode at a time per holder | A slot is resubmitted only once the drain has consumed its previous page (the retriever throttle; see [READ_PIPELINE.md](READ_PIPELINE.md)). `PageDecoder` holds no scratch state of its own. |
| Page level arrays die with assembly | The drain copies what it needs into its own accumulators during the synchronous assemble call; `REAL_VIEW_KEEP_LEVELS` keeps copies of the accumulators, never the page arrays. |
| No consumer reads `array.length` | A pooled array can be longer than the page. Every consumer scans `page.size()` or an explicit count; `SimdOperations.countNonNulls` takes an explicit length. |

Tests: `FixedSizeListEngagementTest` (scratch reuse across pages), `SimdOperationsTest` (stale tail ignored). The single-decode-per-holder exclusion is untested.

## Masked pages

A page row mask ([FETCH_PLANNING.md](FETCH_PLANNING.md#page-masking)) on a nested column is applied on the page's decode task, not on the drain (`NestedColumnWorker.prepareDecodedPage`); a flat column keeps its mask to the drain, which copies each kept interval. `PageTrimmer` moves the positions of the kept records to the front of the page's value, dictionary-index and level arrays, in place, and shortens `page.size()`. Assembly therefore only ever sees unmasked pages: its paths have one shape whatever masks the read carries, and a trimmed page whose leaves are all present takes the bulk copy.

Where a record starts depends on the page:

| Page | Record `r` |
|---|---|
| `fixedListK > 0` | values `[r × k, (r + 1) × k)` |
| no repetition levels (no repeated ancestor) | value `r` |
| anything else | the positions from the `r`-th repetition level `0` up to the next |

On a regular page, record `0` is the one its first value opens, so a masked page whose first repetition level is not `0` fails the read with a `ParquetReadException`.

Trimming in place relies on the page owning its arrays. Value and dictionary-index arrays are allocated per decoded page. Level arrays are the slot's scratch, which is not handed to another decode until the drain has consumed the page ([pooled scratch](#level-decode-and-scratch)). A value array pooled across pages would let one page's trim corrupt another. Untested.

Tests: `PageTrimmerTest`, `NestedV2NoIndexMaskingTest`, `MisalignedPageBoundariesTest`.

## Assembly on the drain

The drain thread assembles decoded pages into a batch through reusable accumulators: a typed value array, definition and repetition level arrays, and record offsets. `NestedColumnWorker.assemblePage` routes each page to one of three paths:

| Page | Path |
|---|---|
| `fixedListK > 0` | fixed-width assembly ([Fixed-size-list fast path](#fixed-size-list-fast-path)) |
| all-present, not byte-array | whole-page bulk copy ([All-present page bulk copy](#all-present-page-bulk-copy)) |
| anything else | per-element: each position copies its value and both levels |

Invariants every path keeps:

- **Records are never split across batches.** A record opens at each `repLevel == 0`; a full batch is published at the next record boundary. Batch boundaries therefore depend only on record counts and the flush rules every column shares, which is what keeps sibling columns aligned (see [READ_PIPELINE.md](READ_PIPELINE.md#when-a-batch-closes)).
- **Masks are applied before assembly.** A masked page arrives trimmed to its kept records ([Masked pages](#masked-pages)), so every path keeps every value it is given.
- **A published batch owns its arrays.** The accumulators are reused for the next batch, so publish copies the value, level and offset arrays out (`trimValues`, including the bytes prefix of a variable-length leaf). The one exception hands the accumulator itself to the batch and allocates a fresh one ([Fixed-size-list fast path](#fixed-size-list-fast-path)).
- **The column's first repetition level is `0`.** The first level a worker assembles is checked once per read: when it is not `0`, the column's first page of the read fails with a `ParquetReadException`. Later pages are checked only when masked, since trimming needs each masked page to open a record ([Masked pages](#masked-pages)).

`NestedBatch.allPresent` records whether every page contributing to the batch passed the all-present gate. A page that spans a publish passes its status on to the next batch.

Tests: `NestedDictBatchBoundaryTest`, `ColumnReaderBatchArrayIdentityTest`, `NestedV2NoIndexMaskingTest`, `NestedV1NoIndexFallbackTest`, `WideListBatchBoundTest`, `BadDataHandlingTest` (parquet-testing-runner).

## Index modes

Before publishing, `computeIndex` derives per-batch structures on the drain, off the serial consumer. What it derives is fixed per worker by an `IndexMode` the constructing reader chooses:

| Mode | Reader | Batch carries | Derived on the drain |
|---|---|---|---|
| `ALL_ITEMS` | `NestedRowReader` | raw definition and repetition levels | `elementValidity` and layer-indexed `multiLevelOffsets` over **all items**: a null or empty container keeps a phantom position in the next layer's index |
| `REAL_VIEW` | unfiltered `ColumnReader` (`ColumnScan.open` without a filter) | no levels; `realView`, `realValues` | the real-items view ([Real view on the drain](#real-view-on-the-drain)); the all-items index is skipped |
| `REAL_VIEW_KEEP_LEVELS` | filtered `ColumnReader` | raw levels | nothing: the batch is compacted by record selection before any view is built |

The two reader families read disjoint batch state: the all-items path reads raw levels through `NestedBatchIndex` and the row views (`PqListImpl`, `PqStructImpl`, `PqMapImpl`, `NestedBatchDataView`, `VariantShredReassembler`); the column path reads only the real-items view and values. `REAL_VIEW` drops the levels because no `ColumnReader` accessor reads them; adding one would require keeping them, or rebuilding them, on that mode.

On a fixed-width batch every mode skips the level scans: `ALL_ITEMS` gets `multiLevelOffsets` from `fixedListLayerOffsets` and `null` validity.

## Real view on the drain

The real-items view (`NestedLevelComputer.RealView`) is what `ColumnReader` exposes: per-layer offsets and validity with phantom positions removed, leaf validity, and `realToRawLeaf`, the map from each real leaf to its raw position (`null` when the map would be the identity). It is a pure function of the batch's levels, record count, max definition level and layers, so the drain can build it from its accumulators, which remain valid for `[0, valueCount)` until the counts reset after publish.

On `REAL_VIEW` the drain builds one of two views:

| Batch | View | Leaf values |
|---|---|---|
| All-present: `fixedListK > 0`, or `allPresent`, or every accumulated definition level at max | **Lean view**: layer offsets only (`computeLayerOffsets`, or `fixedListLayerOffsets` for a fixed-width batch), every validity `null`, `realToRawLeaf` `null` | `realValues` = the batch values |
| Has phantom positions | Full `computeRealView` | compacted through `realToRawLeaf` by `LeafCompaction`; passed through when the map is the identity |

An all-present batch has no phantom positions, so its real-items offsets equal the all-items offsets and every validity is trivially all-present; the lean view is the full view without the per-layer presence, leaf-validity and gather-map work. It is built unconditionally, so a structural read (`getLayerOffsets`, `getLayerValidity`) finds its offsets ready and a flat leaf read (the embedding-vector pattern) ignores them.

**Consumer.** `ColumnReader.ensureRealView()` returns the drain's `realView` when set. Otherwise the batch came from record selection (`ColumnScan.compactNestedBatch`, which slices the raw levels and values per kept record and carries no view), and the reader builds the view lazily from the sliced levels, or arithmetically for a fixed-width batch (`fixedListRealView`). Leaf values resolve in the same order: `realValues` from the drain, else the batch values when `realToRawLeaf` is `null`, else a compacted copy.

Tests: `ColumnReaderLayerModelTest`, `ColumnReaderExactFilterTest`, `FixedSizeListFastPathReadTest`, `DifferentialReadTest`.

## All-present page bulk copy

A page whose leaves are all present stores its values as one contiguous block, 1:1 with page positions; the repetition levels group them into records but do not move them. The regular path copies such a page in bulk instead of per element. The property is presence, not shape: lists of any length, maps, structs, and non-repeated columns assembled by the nested worker qualify alike.

**Gate** (`assembleRegularPage`), evaluated once per page, both required:

- `page.allPresent()`: the definition-level array is `null` (the O(1) gate above, or a column with `maxDefinitionLevel == 0`).
- The page is not a `Page.ByteArrayPage`: `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` and `INT96` leaves append into a shared byte buffer and stay per element.

A page that fails any condition takes the per-element path in full; there is no sub-page run detection.

**Assembly** (`assembleAllPresentPage`). The page is walked once over its repetition levels to open records and to publish at the batch-capacity boundary exactly as the per-element path would; the values and levels of each batch's run are then flushed in bulk (`flushRun`): values by one `System.arraycopy` per run, definition levels by a constant fill of `maxDefinitionLevel`, repetition levels by `System.arraycopy` (or a fill of `0` for a non-repeated column). The accumulator levels are filled in every index mode, since `REAL_VIEW` builds its offsets from them before dropping them at publish. The output batch is identical to what the per-element path produces for the same page; no test forces one page through both paths to compare them (untested).

Tests: `DifferentialReadTest`, `WideListBatchBoundTest`, `ColumnReaderLayerModelTest`.

## Fixed-size-list fast path

A `LIST` column in which every row is a present list of exactly `k` present elements (embedding vectors, Arrow `FixedSizeList` / `FixedShapeTensor` written as 3-level `LIST`) carries a repetition stream of `0` followed by `k − 1` ones per row and a constant definition stream. The fast path recognises that shape from the level bytes, decodes only the values, and assembles the batch with arithmetic offsets `[0, k, 2k, …]`, producing the same public output as the regular path.

### Gate

`PageDecoder` stamps a page with `fixedListK = k` (`Page.withFixedListK`) only when every condition holds:

| Condition | Rule |
|---|---|
| Enabled | The reader option `hardwood.fixed-list-fast-path` is `"true"` (case-insensitive). The option is off by default; see [docs/content/reference/reader.md](../docs/content/reference/reader.md). `ParquetFileReader` resolves it and threads it to every worker and `PageDecoder`. |
| Element type | Physical type `BOOLEAN`, `INT32`, `INT64`, `FLOAT` or `DOUBLE`. |
| Level geometry | `maxRepetitionLevel == 1`, and `maxDefinitionLevel == 2`, or `maxDefinitionLevel == 1` with a leaf whose own repetition is not `REPEATED`. |
| Page format | `DataPageV2`: both level regions non-empty. `DataPageV1`: both level encodings `RLE` and both level regions non-empty. Legacy `BIT_PACKED` levels do not qualify; the regular path misreads them (#569). |
| Definition levels | One RLE run of `maxDefinitionLevel` covering all values (`isSingleRleRunOf`). |
| Repetition levels | `k` is the first gap between `0`s, and every later gap, including the final row closed against the end of the stream, equals it. `DataPageV2` also requires `numRows × k == numValues`; `DataPageV1`, which has no row count, requires `numValues % k == 0`. |

The `maxDefinitionLevel == 1` exclusion covers a bare unannotated `repeated <primitive>` and a required legacy 2-level list (`repeated <primitive> array`), which share the geometry of a required 3-level list and are indistinguishable by levels. The level geometry is the whole test for `maxDefinitionLevel == 2`, so any single-repetition column with a primitive leaf and that geometry qualifies, including an optional legacy 2-level list and a required struct element. Every such column has a single `REPEATED` layer and at most one other layer whose items are all present, so the arithmetic view is correct for all of them.

`k` is verified, never assumed: a page whose inner lengths vary but sum to a multiple of the row count fails the per-gap check. `FixedSizeListDetector` is a pure predicate over the level bytes that never reads a vector interior. It checks small `k` (`k ≤ 8`, an all-bit-packed stream) by a word-at-a-time compare against the stream's byte period, large `k` by comparing the stream against itself shifted by one row's byte stride, and anything else (irregular run splits, unaligned rows) by a run-by-run walk that skips RLE runs by their header count.

### Representation and fallback

A fixed-width batch has `fixedListK > 0`, `null` definition and repetition levels, and implicit offsets. Its level accumulators are not grown while it stays fixed-width; only the value array grows.

Batches cut at the same rows in every column, at the flush points every column evaluates identically ([READ_PIPELINE.md](READ_PIPELINE.md#when-a-batch-closes)). A change of page shape is not one of them, so the assembler never publishes early on one. When an open fixed-width batch meets a regular page or a different `k`, `materializeFixedWidthBatchLevels` converts it to the regular representation in place (every element present, each record a `0` then `k − 1` ones) and keeps filling it; a fixed-width page arriving in an already-regular batch is written with the same synthesized levels. A published batch is thus wholly fixed-width with one `k`, or wholly regular.

A masked fixed-width page arrives trimmed to its kept records, still consecutive `k`-runs, and is copied in batch-capacity chunks with one `copyValueRun` per chunk. Record selection on a filtered read keeps whole records, so `compactNestedBatch` keeps `fixedListK` and the view is rebuilt arithmetically.

**Value hand-off.** A fresh fixed-width batch sizes its value accumulator to exactly `batchCapacity × k`. When the batch publishes full, that array becomes the batch's values and the next batch starts on a fresh array, so the values are copied once (page to batch) as on the flat path. A partial batch (file tail, row cap) or a batch that fell back is trimmed as usual.

Tests: `FixedSizeListDetectorTest`, `FixedSizeListEngagementTest`, `FixedSizeListFastPathReadTest`, `DifferentialFixedSizeListTest`.

## Boundaries

- **Partially present pages** take the per-element path in full; bulk-copying present runs inside such pages is #750.
- **Non-repeated columns** in a nested row read are assembled by `NestedColumnWorker` and pay the levels machinery (with the bulk copy when all-present); direct addressing for them is #732.
- **Fixed-size list, 2-level required lists and bare repeated primitives** (`maxDefinitionLevel == 1` with a `REPEATED` leaf) take the regular path; #808.
- **Fixed-size list with null rows** takes the regular path: the definition gate accepts a single max-value run only; #809.
- **Fixed-size list element types** are limited to fixed-width primitives; byte-array-backed leaves (`BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY`, `INT96`) and nullable elements take the regular path.
- **Legacy `BIT_PACKED` levels (#569).** `DataPageV1` level streams are decoded as the RLE / bit-packing hybrid whatever the header's level encoding says, so a page with `BIT_PACKED` levels is misread.
- **Phantom-bearing batches** on `REAL_VIEW` build the full `computeRealView` and gather the leaf values; only all-present batches get the lean view.
