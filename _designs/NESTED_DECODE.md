# Nested decode

Describes how the reader turns a nested column's repetition and definition levels into nested batches: the mapping from schema to layers, level decoding, how a decoded column's cells are measured, placed and filled, what each `NestedIndexMode` derives, the real-items view, and the fixed-size-list fast path.

Related documents:

- [READ_PIPELINE.md](READ_PIPELINE.md): the decoded columns, threading, the batch plan and batch sizing
- [COLUMN_READER.md](COLUMN_READER.md): what `ColumnReader` exposes (layers, offsets, `Validity`, variable-length leaves, the cursor and scan)
- [ROW_READER.md](ROW_READER.md): how `NestedRowReader` reads the all-items batch
- [VALUE_DECODE.md](VALUE_DECODE.md): value decoders, dictionaries and leaf kinds
- [RECORD_FILTERING.md](RECORD_FILTERING.md): record selection on a filtered column read
- [docs/content/concepts/nested-columns.md](../docs/content/concepts/nested-columns.md): the user-facing layer model

## Which columns are nested

A column is decoded through its levels into `NestedBatch`es, by a `NestedColumn`, when:

| Reader | Condition |
|---|---|
| `ColumnReader` / `ColumnReaders` | the column has at least one layer or `maxRepetitionLevel > 0` (`ColumnCursor.isNested`). A leaf below required groups only is flat. |
| `NestedRowReader` | every decoded column, flat or not, when some decoded column has `maxRepetitionLevel > 0` or some decoded top-level field is a group. A column only the predicate references counts. See [ROW_READER.md](ROW_READER.md#flat-and-nested-readers). |

All other columns are flat columns, `FlatColumn` (see [READ_PIPELINE.md](READ_PIPELINE.md#decoded-columns)).

## Schema to layers

`NestedLevelComputer.computeLayers(schema)` walks the schema once per read and emits a `Layers` descriptor for every leaf, which the read's cursors and readers share: one entry per layer, outermost first, each a `LayerKind` and a definition-level **threshold** at or above which the node of that layer is present.

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

An annotation on a repeated group outside a `LIST`/`MAP` group has no reading of its own: `LIST` and `MAP` are excluded from the format's rule, a repeated two-level `LIST` is only ever another `LIST`'s element and a `MAP` is `required` or `optional`, `MAP_KEY_VALUE` there stands for `MAP`, and no other annotation applies to a group. `BareRepeatedGroups` drops it from the footer's schema elements before `FileSchema` is built ([FILE_METADATA.md](FILE_METADATA.md#footer-read)), as an annotation a leaf's physical type cannot carry is dropped ([EXCEPTION_MODEL.md](EXCEPTION_MODEL.md#an-annotation-the-reader-cannot-use-is-dropped-not-raised)), so both paths, and every other consumer of the schema, see the unannotated group: a list of structs, or a list of maps where its only child is a repeated `MAP_KEY_VALUE` group (below). `VARIANT` is the exception: the rule covers it, making the field a list of variants, which neither path serves. The annotation stays in the schema, and the read's schema check (`SchemaCompatibility`) refuses a read that projects one of the group's columns with `UnsupportedOperationException` before any column is decoded, in the first file when the reader is built and in a later file when it is planned; reads of the file's other columns proceed. A filter never reaches that check: `FilterPredicateResolver` rejects a predicate on the group or below it with `IllegalArgumentException`, as it does any column below a repeated path.

A bare repeated group whose only child is a repeated `MAP_KEY_VALUE` group, and which carries no logical type, reads as a legacy `MAP` by `FileSchema`'s inference, and that `MAP` is the list's element: the layers are the list's `REPEATED` layer at the group's definition level minus one and the map's at the group's own, and the row path describes the element as a map. Whether a group sits inside `LIST`/`MAP` scaffolding follows the same inference in `BareRepeatedGroups` and `FileSchema`, both reading a group's annotations as `AnnotationPairings.readableGroupAnnotation` and `readableGroupConvertedType` keep them, so the `key_value` group under such a parent is never bare.

### Legacy list element rules

`SchemaNode.GroupNode.getListElement()` resolves a `LIST` group's element by the format's [backward-compatibility rules](https://parquet.apache.org/docs/file-format/types/logicaltypes/#backward-compatibility-rules), in this order: a repeated primitive is the element; a repeated group with several fields is the element; a repeated group whose single field is itself repeated is the element (a list of lists, since a synthetic 3-level wrapper never has a repeated child); a repeated group named `array` or `<list>_tuple` is the element; otherwise the repeated group's single child is. The third rule precedes the naming rule, so `mylist (LIST) { repeated group bag { repeated int32 num } }` reads as `list<struct<num: list<int>>>` and its leaf gets the same two `REPEATED` layers, and the same offsets, as the fully annotated `list<list<int>>`.

Tests: `ColumnReaderLayerModelTest`, `UnannotatedRepeatedListTest`, `LegacyTwoLevelListOfListsTest`, `ListBackwardCompatRulesTest`, `NestedSchemaTest`, `AnnotatedBareRepeatedGroupTest`.

## Level decode and scratch

`PageDecoder` decodes a page's level streams (RLE / bit-packing hybrid) before its values.

- **Repetition levels** are always materialised for a column with `maxRepetitionLevel > 0`, except on a page the [fixed-size-list fast path](#fixed-size-list-fast-path) takes.
- **Definition levels** go through the all-present gate: `RleBitPackingHybridDecoder.isSingleRleRunOf(maxDef, numValues)` inspects one run header and value. When the stream is a single RLE run of `maxDef` covering every value, no array is produced and the page's definition levels are `null`. A `null` array is the reader-wide all-present convention, shared with required columns; value readers read densely from it.

**Page-owned scratch.** A nested page is read in two passes (`PageDecoder.decodeNestedInPlace`): its task decompresses the page into a `PageDecoder.LevelScratch` the page owns (`NestedPageTask.scratch`) and decodes its levels and dictionary ids there, leaving the values in the decompressed bytes (or, for [stored values](VALUE_DECODE.md#page-decode), in the fetched page, which takes no value buffer from the scratch, only a copy of its level sections); the scratch also holds the page's record starts, the byte prefix of its plain binary values and, for a masked page, its kept levels. The page borrows the scratch from its column's pool and keeps it past its task, until every cell it is placed into is filled ([Cells](#cells)), so it cannot be the task's scratch. A scratch array can be longer than its page: no consumer reads `array.length`; every consumer scans the page's size or an explicit count, and `SimdOperations.countNonNulls` takes explicit bounds.

Tests: `SimdOperationsTest` (stale tail ignored).

## Masked pages

A page row mask ([FETCH_PLANNING.md](FETCH_PLANNING.md#page-masking)) on a nested column is applied on the page's task, before the page is measured; a flat column's fill reads each kept interval instead. `PageTrimmer.trim(InPlaceNestedPage, ...)` copies the levels of the kept records into the page's scratch and records where their values lie in the page (`KeptRuns`): runs of consecutive page positions, with adjacent kept records in one run. The trimmed page's size is its kept values, and its kept position `i` is the `i`-th position over the runs. Its values stay in the decompressed bytes, and a fill reads each kept position from there through its run. Measuring, placing and filling therefore see only the kept records, and have one shape whatever masks the read carries.

Where a record starts depends on the page:

| Page | Record `r` |
|---|---|
| `fixedListK > 0` | values `[r × k, (r + 1) × k)` |
| no repetition levels (no repeated ancestor) | value `r` |
| anything else | the positions from the `r`-th repetition level `0` up to the next |

On a regular page, record `0` is the one its first value opens, so a masked page whose first repetition level is not `0` fails the read with a `ParquetReadException`, as does a mask that selects records past the page's last.

Tests: `PageTrimmerTest`, `KeptRunsTest`, `NestedFillMaskedPageTest`, `NestedV2NoIndexMaskingTest`, `MisalignedPageBoundariesTest`.

## Cells

A nested column's batches are built from cells of the read's batch plan ([READ_PIPELINE.md](READ_PIPELINE.md#decoded-columns)). Where a page's records start, and how many values each holds, only its levels say, so a page is placed into cells once its decode job has decoded and measured it, and a cell is filled only once every page placed into it is measured. The pages of one column are decoded on several threads at once, so no fill can take its write position from the fill before it: the measure and the placement give every segment its offset, and every array of the cell its size, before any value is written, so each value is written once, into an array allocated once.

**Measure** (`PageMeasure`, on the page's decode job, after a masked page is trimmed). The page's values, its records and where each starts (as in [Masked pages](#masked-pages); on a regular page the values before the first repetition level `0` are its leading values), and, for a binary leaf, the bytes of any range of its plain values. A page whose metadata gives a record count (a column without repetition) that disagrees with its levels fails the read.

**Placement** (`Placer`, under the read's lock, in page order). As a decode's result is applied, the read places every measured page at the head of the column's unplaced pages: a page measured before the one ahead of it waits, and the result of the one ahead places both, so no decode blocks on another. The placer splits a page's records into one `NestedSegment` per batch they fall into, each a record range and the value range those records hold; a page's leading values go, as a segment of no records, into the cell holding the previous page's last record. A cell of a column without repetition is sealed as soon as its last planned record is placed; a cell of a column with repetition is sealed at the next record start, at the end of its row group where the plan closes its batch there, or at the end of the read, since its last record's values can follow on later pages.

**Fill** (`NestedFill`, outside the lock, on the decode pool: on a job the read emits for the cells a placement sealed). A placement seals cells as it places a measured page, a boundary marker, the end of the stream or the walk's failure once every page before them is placed; whichever seals a cell, the read hands the cells sealed to a job of their own ([READ_PIPELINE.md](READ_PIPELINE.md#decoded-columns)), so a cell is filled when no decode is left to fill it. The cell's arrays are allocated once at their final size: values and levels at the sum of its segments' values, record offsets at its records; a binary leaf's plain bytes go into chunks of the batch as they are copied ([COLUMN_READER.md](COLUMN_READER.md#variable-length)). Each segment's values are one contiguous range of its page's kept values. A segment writes its values straight from the page's decompressed bytes through the page's value reader ([VALUE_DECODE.md](VALUE_DECODE.md#page-decode)), through a view of its own, since two cells sharing a page can be filled at once, and run by run where the page is masked. It copies its levels by `System.arraycopy`, fills the levels its page has none of (definition levels of an all-present page, repetition levels of a column without repetition), and writes the offsets of the records it starts. A binary segment writes each plain value's bytes and view, or each dictionary value's entry id, whose view is built when a reader first needs it ([COLUMN_READER.md](COLUMN_READER.md#variable-length)). The job then computes the batch's index ([Index modes](#index-modes)) and completes the cell. A page keeps its scratch until every cell it is placed into is filled, and then returns it to the column's pool; a filled cell drops its segments, so a cell waiting for room in the exchange holds only its own arrays.

Invariants:

- **Records are never split across batches.** Batch boundaries are the plan's, which every column shares and which keeps sibling columns aligned (see [READ_PIPELINE.md](READ_PIPELINE.md#when-a-batch-closes)).
- **Masks are applied before measuring.** A masked page is trimmed to its kept records ([Masked pages](#masked-pages)), so every segment keeps every value it is given.
- **A batch's arrays are its own, at the size it holds.** The cell allocates them for its batch alone, on the row reader's recycling exchange too, so a published batch shares no array with another, and no array is grown or copied out at publish. Values the row reader returns refer to their batch's arrays and stay correct after the reader moves on ([ROW_READER.md](ROW_READER.md#values-and-nullness)), so a nested batch's arrays are never refilled.
- **Cells are filled on the decode pool only.** No walk advance and no thread applying a result fills a cell, so the pool size bounds the read's decode parallelism ([THREADING.md](THREADING.md#thread-inventory)).
- **Every column chunk begins with a record.** A chunk's first page with values whose first repetition level is not `0` fails the read with a `ParquetReadException`, placed at that page: a record never spans two row groups. Later pages may begin inside a record, unless masked, since trimming needs each masked page to open a record ([Masked pages](#masked-pages)), and unless the page before it in the chunk is skipped or drops its last record by its mask: those leading values belong to no record the column holds, so the page fails the read with a `ParquetReadException`. The placer tells a skipped page from the ordinal each page carries within its chunk.

`NestedBatch.allPresent` records whether every page with a segment in the batch passed the all-present gate.

Tests: `NestedColumnTest`, which compares each read, at several batch capacities, with the records `ReferencePageDecoder` decodes, `PlacerTest`, `ShuffledTaskOrderTest`, which measures pages ahead of the pages before them, `NestedV1MidRecordPagesTest`, `NestedDictBatchBoundaryTest`, `ColumnReaderBatchArrayIdentityTest`, `FileLogicalTypeTest`, `NestedV2NoIndexMaskingTest`, `NestedV1NoIndexFallbackTest`, `WideListBatchBoundTest`, `BadDataHandlingTest` (parquet-testing-runner).

## Index modes

Before a cell's batch is published, the fill derives per-batch structures on the thread that filled it, off the serial consumer: `NestedIndex.realView` before the values are written, `NestedIndex.compute` after. What it derives is fixed per column by a `NestedIndexMode` the constructing reader chooses:

| Mode | Reader | Batch carries | Derived before publish |
|---|---|---|---|
| `ALL_ITEMS` | `NestedRowReader` | raw definition and repetition levels | `elementValidity` and layer-indexed `multiLevelOffsets` over **all items**: a null or empty container keeps a phantom position in the next layer's index |
| `REAL_VIEW` | unfiltered `ColumnReader` (`ColumnScan.open` without a filter) | no levels, no record offsets; `realView`, and the real leaf values as both `values` and `realValues` | the real-items view ([Real view](#real-view)); the all-items index is skipped |
| `REAL_VIEW_KEEP_LEVELS` | filtered `ColumnReader` | raw levels | nothing: the batch is compacted by record selection before any view is built |

The two reader families read disjoint batch state: the all-items path reads raw levels through `NestedBatchIndex` and the row views (`PqListImpl`, `PqStructImpl`, `PqMapImpl`, `NestedBatchDataView`, `VariantShredReassembler`); the column path reads only the real-items view and values. `REAL_VIEW` drops the levels and the record offsets because no `ColumnReader` accessor reads them; adding one would require keeping them, or rebuilding them, on that mode.

On a fixed-width batch every mode skips the level scans: `ALL_ITEMS` gets `multiLevelOffsets` from `fixedListLayerOffsets` and `null` validity.

## Real view

The real-items view (`NestedLevelComputer.RealView`, defined in [COLUMN_READER.md](COLUMN_READER.md#real-view)) is a pure function of the batch's levels, record count, max definition level and layers. On `REAL_VIEW` the batch carries no levels: the fill writes the levels the view reads to the filling task's scratch instead, the repetition levels of a column with repetition and the definition levels of a batch that is not all present, and builds the view from them. Every segment writes its levels before any segment writes its values, so the view, and with it the real leaf count, is known when the values are allocated.

On `REAL_VIEW` the fill builds one of two views:

| Batch | View | Leaf values |
|---|---|---|
| All-present: `fixedListK > 0`, or `allPresent`, or every definition level at max | **Lean view**: layer offsets only (`computeLayerOffsets`, or `fixedListLayerOffsets` for a fixed-width batch), every validity `null`, `realToRawLeaf` `null` | `realValues` = the batch values |
| Has phantom positions | Full `computeRealView`, without `realToRawLeaf` | allocated at the real leaf count; each segment writes its real leaves alone, straight to their real positions |

A raw position is a real leaf when its definition level reaches the leaf's item threshold (`Layers.itemDefThresholds()` past the innermost layer), whatever its repetition level, so a segment tells a phantom position from its page's definition levels alone: the page copy (`FlatPageValues.copy`, `BinaryPageValues.copyPlain` and `copyIds`) takes a slot level, below which a row takes no value slot, and returns the slots it wrote. The batch's largest array is allocated once, at its final size, and no gather map is built.

An all-present batch has no phantom positions, so its real-items offsets equal the all-items offsets and every validity is trivially all-present; the lean view is the full view without the per-layer presence and leaf-validity work. It is built unconditionally, so a structural read (`getLayerOffsets`, `getLayerValidity`) finds its offsets ready and a flat leaf read (the embedding-vector pattern) ignores them.

**Consumer.** `ColumnReader.ensureRealView()` returns the batch's `realView` when set. Otherwise the batch came from record selection (`ColumnScan.compactNestedBatch`, which slices the raw levels and values per kept record and carries no view), and the reader builds the view lazily from the sliced levels, or arithmetically for a fixed-width batch (`fixedListRealView`). Leaf values resolve in the same order: the batch's `realValues`, else the batch values when `realToRawLeaf` is `null`, else a copy compacted through it by `LeafCompaction`.

Tests: `ColumnReaderLayerModelTest`, `ColumnReaderExactFilterTest`, `FixedSizeListFastPathReadTest`, `DifferentialReadTest`, `RealViewEquivalenceTest`.

## Fixed-size-list fast path

A `LIST` column in which every row is a present list of exactly `k` present elements (embedding vectors, Arrow `FixedSizeList` / `FixedShapeTensor` written as 3-level `LIST`) carries a repetition stream of `0` followed by `k − 1` ones per row and a constant definition stream. The fast path recognises that shape from the level bytes, decodes no levels, and builds the batch with arithmetic offsets `[0, k, 2k, …]`, producing the same public output as the regular path.

### Gate

`PageDecoder` gives a page `fixedListK = k` when it reads the page's levels, only when every condition holds:

| Condition | Rule |
|---|---|
| Enabled | The reader option `hardwood.fixed-list-fast-path` is `"true"` (case-insensitive). The option is off by default; see [docs/content/reference/reader.md](../docs/content/reference/reader.md). `ParquetFileReader` resolves it and threads it to every nested column and its `PageDecoder`s. |
| Element type | Physical type `BOOLEAN`, `INT32`, `INT64`, `FLOAT` or `DOUBLE`. |
| Level geometry | `maxRepetitionLevel == 1`, and `maxDefinitionLevel == 2`, or `maxDefinitionLevel == 1` with a leaf whose own repetition is not `REPEATED`. |
| Page format | `DataPageV2`: both level regions non-empty. `DataPageV1`: both level encodings `RLE` and both level regions non-empty. Legacy `BIT_PACKED` levels do not qualify; the regular path misreads them (#569). |
| Definition levels | One RLE run of `maxDefinitionLevel` covering all values (`isSingleRleRunOf`). |
| Repetition levels | `k` is the first gap between `0`s, and every later gap, including the final row closed against the end of the stream, equals it. `DataPageV2` also requires `numRows × k == numValues`; `DataPageV1`, which has no row count, requires `numValues % k == 0`. |

The `maxDefinitionLevel == 1` exclusion covers a bare unannotated `repeated <primitive>` and a required legacy 2-level list (`repeated <primitive> array`), which share the geometry of a required 3-level list and are indistinguishable by levels. The level geometry is the whole test for `maxDefinitionLevel == 2`, so any single-repetition column with a primitive leaf and that geometry qualifies, including an optional legacy 2-level list and a required struct element. Every such column has a single `REPEATED` layer and at most one other layer whose items are all present, so the arithmetic view is correct for all of them.

`k` is verified, never assumed: a page whose inner lengths vary but sum to a multiple of the row count fails the per-gap check. `FixedSizeListDetector` is a pure predicate over the level bytes that never reads a vector interior. It checks small `k` (`k ≤ 8`, an all-bit-packed stream) by a word-at-a-time compare against the stream's byte period, large `k` by comparing the stream against itself shifted by one row's byte stride, and anything else (irregular run splits, unaligned rows) by a run-by-run walk that skips RLE runs by their header count.

### Representation and fallback

A fixed-width batch has `fixedListK > 0`, `null` definition and repetition levels, and implicit offsets; its values are exactly its records times `k`.

Batches cut at the same rows in every column ([READ_PIPELINE.md](READ_PIPELINE.md#when-a-batch-closes)), and a change of page shape is not one of those rows. A cell is fixed-width when every page with a segment in it is a fixed-size-list page of the same `k`; in any other cell a fixed-size-list page writes the levels it omits (every element present, each record a `0` then `k − 1` ones). A published batch is thus wholly fixed-width with one `k`, or wholly regular.

A masked fixed-width page is trimmed to its kept records, still consecutive `k`-runs. Record selection on a filtered read keeps whole records, so `compactNestedBatch` keeps `fixedListK` and the view is rebuilt arithmetically.

Tests: `FixedSizeListDetectorTest`, `FixedSizeListEngagementTest`, `FixedSizeListFastPathReadTest`, `DifferentialFixedSizeListTest`.

## Boundaries

- **Non-repeated columns** in a nested row read are decoded through levels and pay the levels machinery; direct addressing for them is #732.
- **Fixed-size list, 2-level required lists and bare repeated primitives** (`maxDefinitionLevel == 1` with a `REPEATED` leaf) take the regular path; #808.
- **Fixed-size list with null rows** takes the regular path: the definition gate accepts a single max-value run only; #809.
- **Fixed-size list element types** are limited to fixed-width primitives; byte-array-backed leaves (`BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY`, `INT96`) and nullable elements take the regular path.
- **Legacy `BIT_PACKED` levels (#569).** `DataPageV1` level streams are decoded as the RLE / bit-packing hybrid whatever the header's level encoding says, so a page with `BIT_PACKED` levels is misread.
- **Bare repeated Variant groups** (#1378). A repeated `VARIANT` group outside a `LIST` or `MAP` group keeps its annotation and is refused when a read projects it, rather than read as a list of variants.
- **Real view per leaf (#1023).** Each leaf column computes its real-items view from its own levels, so sibling leaves of one repetition domain derive the same ancestor structure once each.
