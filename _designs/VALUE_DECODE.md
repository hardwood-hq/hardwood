# Value decode

How the bytes of one data page become typed Java values: the page header and decompression hand-off in `PageDecoder`, the choice of value decoder per encoding, dictionary pages and the per-chunk reuse of dictionary strings, the leaf classification (`LeafKind`) both row readers decode through, and the Vector API kernels with their scalar fallback. It does not cover where pages come from or which thread decodes them ([READ_PIPELINE.md](READ_PIPELINE.md)), how levels become nested layers, the level scratch or the fixed-size-list fast path ([NESTED_DECODE.md](NESTED_DECODE.md)), the batch layout the column reader exposes ([COLUMN_READER.md](COLUMN_READER.md)), row-reader accessor addressing ([ROW_READER.md](ROW_READER.md)), or the annotation model and per-type conversions ([LOGICAL_TYPES.md](LOGICAL_TYPES.md)). Dictionary-based row-group pruning and the location of the dictionary page are in [STATISTICS_PRUNING.md](STATISTICS_PRUNING.md); the page hierarchy itself is described for users in [docs/content/concepts/parquet-layout.md](../docs/content/concepts/parquet-layout.md).

## Page decode

`PageDecoder` is built per column chunk from the column's `ColumnSchema`, its `ColumnMetaData` (for the codec) and the context's `DecompressorFactory`. `decodePage(buffer, dictionary, scratch)` takes one page, header included, and the chunk's `Dictionary` or `null`, and returns one `Page`.

The steps, in order:

1. Parse the page header from the buffer (`PageHeaderReader`) and slice the body without copying.
2. If the header carries a CRC, check it over the stored (compressed) body (`CrcValidator`); a mismatch raises `ParquetReadException`.
3. Decompress and split the body by page type:

| Page type | Levels | Values |
|---|---|---|
| `DATA_PAGE` (v1) | inside the compressed body; the whole body is decompressed, then each level stream is read behind its 4-byte little-endian length | follow the levels in the decompressed body |
| `DATA_PAGE_V2` | stored uncompressed ahead of the values, with lengths from the header | decompressed only when the header's `is_compressed` is set |
| any other | | `ParquetReadException`; dictionary pages go through `DictionaryParser`, and index pages never reach the decoder |

4. Decode repetition levels when `maxRepetitionLevel > 0` and definition levels when `maxDefinitionLevel > 0`, both with `RleBitPackingHybridDecoder` at the bit width of the maximum level.
5. Decode the values with the decoder the page's encoding selects (next section) into a `Page`.

A `PageDecodedEvent` (JFR) is committed per decoded page with the column and both sizes.

**Decompression hand-off.** `DecompressorFactory.getDecompressor(codec)` returns a `Decompressor` per call; a codec whose library is absent, and `LZO`, raise `UnsupportedOperationException` naming the dependency, and that type is never re-typed as a read failure (see [docs/content/reference/error-handling.md](../docs/content/reference/error-handling.md)). `GZIP` goes through libdeflate over FFM when the context has a libdeflate pool (Java 22+) and through `java.util.zip.Inflater` otherwise; the user-facing switch is in [docs/content/reference/configuration.md](../docs/content/reference/configuration.md). Every decompressor, `UNCOMPRESSED` included, checks that it produced exactly the declared uncompressed size.

A decompressor returns a thread-owned, reused `byte[]` whose length may exceed the page's uncompressed size. Three rules follow. Decoders bound their reads by the page's size, never by the array's length, so a page that declares more values than its body holds fails as malformed instead of reading bytes left by an earlier page. Nothing a decoder produces may reference that array: every value decoder copies what it keeps (primitive arrays are filled, `byte[]` values are copied out), so a `Page` outlives the next decompression on the same thread. And decoding is confined to the thread that decompressed, since the next page on that thread overwrites the buffer.

**The `Page` model.** `Page` is sealed, one variant per physical storage: `BooleanPage`, `IntPage`, `LongPage`, `FloatPage`, `DoublePage`, and `ByteArrayPage` for `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` and `INT96`. Every variant carries a primitive (or `byte[][]`) value array, the two level arrays, `maxDefinitionLevel`, `size()` and `fixedListK`.

| Property | Statement |
|---|---|
| Positional values | The value array has one slot per level entry (`size()`), and a value sits at its entry's position. A slot whose definition level is below the maximum holds the type's default and is never read as a value. |
| All-present | A `null` definition-level array means every entry is at `maxDefinitionLevel`. Required columns always produce it; an optional column produces it when its definition-level stream is one RLE run of the maximum over the whole page (`RleBitPackingHybridDecoder.isSingleRleRunOf`). Consumers test presence through `allPresent()` / `isNull(i)`. |
| Level arrays are pooled | A non-null level array may be longer than `size()`; only the first `size()` entries are valid. The pooling is described in [NESTED_DECODE.md](NESTED_DECODE.md). |
| Flat columns | `repetitionLevels()` is `null` when `maxRepetitionLevel == 0`. |

Two other producers of a `Page` exist. `nullPage(n)` builds an all-null page of `n` entries without reading the body, for a page inline statistics proved non-matching ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md)); it refuses a required column. The fixed-size-list fast path decodes values with `null` level arrays and stamps `fixedListK` through `Page.withFixedListK` ([NESTED_DECODE.md](NESTED_DECODE.md)).

A decoder failure is a `RuntimeException` on a decode task; `ColumnWorker` classifies it into `ParquetReadException` and names the file, row group, column and page ([READ_PIPELINE.md](READ_PIPELINE.md)).

Tests: `CrcValidationTest`, `RleBitPackingHybridDecoderSingleRunTest`.

## Encodings and decoders

`PageDecoder.decodeTypedValues` switches on the page's encoding, then on the column's physical type. Each pair either decodes or is refused. The format defines each encoding over a fixed set of physical types, so a page declaring an encoding its column cannot carry is a malformed file and raises `ParquetReadException` naming the legal types. Decoding it would build a page of the wrong variant, wrong in its values and in the type the consumer casts to.

| Encoding | Physical types decoded | Decoder | Refused |
|---|---|---|---|
| `PLAIN` | all | `PlainDecoder` | none |
| `RLE_DICTIONARY`, `PLAIN_DICTIONARY` | all but `BOOLEAN` (see [Dictionary](#dictionary-and-string-reuse)) | index stream through `RleBitPackingHybridDecoder`, lookup in the `Dictionary` variant | no dictionary for the chunk; an index bit width above 32; a `BOOLEAN` dictionary (`UnsupportedOperationException`, raised when the dictionary page is parsed) |
| `DELTA_BINARY_PACKED` | `INT32`, `INT64` | `DeltaBinaryPackedDecoder` | every other type |
| `DELTA_LENGTH_BYTE_ARRAY` | `BYTE_ARRAY` | `DeltaLengthByteArrayDecoder` | every other type |
| `DELTA_BYTE_ARRAY` | `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` | `DeltaByteArrayDecoder` | every other type |
| `BYTE_STREAM_SPLIT` | `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `FIXED_LEN_BYTE_ARRAY` | `ByteStreamSplitDecoder` | `BOOLEAN`, `BYTE_ARRAY`, `INT96`, checked before the decoder is built because its byte width is derived from the type |
| `RLE` | `BOOLEAN`, behind a 4-byte length | `RleBitPackingHybridDecoder` at width 1 | every other type |
| `BIT_PACKED` | none | | always: it encodes levels, never values |
| `UNKNOWN` | none | | `UnsupportedOperationException` carrying the raw Thrift value. It and the `BOOLEAN` dictionary are the refusals that mark a gap in this release; every other refusal marks a malformed file |

The value decoders share one shape (`ValueDecoder`): `readX(output, definitionLevels, maxDefLevel)` fills a page-sized output array, consuming one encoded value per entry at the maximum definition level and skipping the others, or one per entry when the level array is `null`. Decoders whose encoded stream holds only the present values and must size a header or stream split first (`BYTE_STREAM_SPLIT` and both delta byte-array encodings) are told the non-null count before reading. A decoder that runs out of bytes raises, and the decode task classifies the failure as a read failure (see [Page decode](#page-decode)).

Tests: `PageDecoderEncodingTest`, `ByteArrayEncodingsTest`, `DeltaBinaryPackedDecoderHeaderTest`, `DeltaBinaryPackedDecoderWidthSweepTest`, `DeltaBinaryPackedTest`, `DeltaByteArrayTest`, `DeltaByteArrayFlbaTest`.

## Dictionary and string reuse

### Dictionary pages

A column chunk has at most one dictionary page, ahead of its first data page. `DictionaryParser` parses it once per chunk and every data page of the chunk decodes against the same `Dictionary` instance. When dictionary pushdown already read the page for pruning, the fetch plan receives that instance and does not parse the page again ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md)).

`DictionaryParser.parsePage` is the one place a dictionary page's own claims are checked, whichever entry point reaches it: the page type is `DICTIONARY_PAGE`, the body is exactly the header's compressed size, the dictionary page header is present, its value count is not negative, and the CRC matches when present. The body is decompressed and read as `num_values` `PLAIN` values. A failure while decompressing or parsing becomes `ParquetReadException` carrying type, counts, sizes and codec, except `UnsupportedOperationException` (an unavailable codec), which leaves as raised.

`Dictionary` is sealed, one variant per storage: `IntDictionary`, `LongDictionary`, `FloatDictionary`, `DoubleDictionary` over primitive arrays, and `ByteArrayDictionary` over `byte[][]` for `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` and `INT96`. A `BOOLEAN` dictionary raises `UnsupportedOperationException`. Each variant decodes a data page itself (`Dictionary.decodePage`), so the per-type lookup runs in a monomorphic call and the caller does not dispatch per value. An index outside the dictionary raises on the array access and is classified as a read failure.

A chunk may mix dictionary and `PLAIN` data pages (a writer that fell back part-way); each page is decoded by its own encoding.

### String reuse

A dictionary-encoded string column references few distinct entries from many rows. Each entry's `String` is materialised once per chunk and the same instance is returned for every value that references it. Callers receive ordinary `String`s; nothing in the API exposes the reuse.

The mechanism has three parts:

| Part | Where | Statement |
|---|---|---|
| Per-chunk cache | `ByteArrayDictionary.internedString(i)` | A lazily allocated `String[]` parallel to the entries; entry `i` is decoded as UTF-8 on first request and cached. |
| Index on the page | `Page.ByteArrayPage.dictionary()` / `dictIndices()` | A dictionary-decoded byte-array page carries its `ByteArrayDictionary` and one entry index per value (`-1` at a null). A `PLAIN` page carries `null` for both. |
| Index on the batch | `BinaryBatchValues.dictionary` / `dictIndices` | For a string column the batch records, per value, the entry index or `-1`. `stringAt(i)` returns the cached `String` when the batch has a dictionary and the index is non-negative, and decodes from the packed bytes otherwise. |

The packed bytes are written for every value whatever its encoding, so `getBinary` and raw-byte access are unaffected and the fallback in `stringAt` is always available.

**Which columns.** A column is a string column when `LeafKind.of(type, annotation) == STRING`: a `BYTE_ARRAY` annotated `STRING`, `ENUM` or `JSON`. `BatchExchange` sets `BinaryBatchValues.internStrings` from that answer when it allocates the batch; the consumer side asks the same `LeafKind` question before routing a value through `stringAt`. Both sides classify through one method, so the side that records indices and the side that reads them cannot disagree. A non-string column never records indices.

**Batch rules.** A batch starts with no dictionary. The first dictionary page that contributes switches it on: the batch adopts that page's dictionary, allocates `dictIndices` at the batch's value capacity, and backfills `-1` over the values already written (`ensureDictionary`). After that, a `PLAIN` page's values record `-1`. A column whose chunk is entirely `PLAIN` never allocates `dictIndices` and pays one `null` check per `stringAt`. The worker clears the batch's dictionary slot when it takes the batch for reuse, keeping the array.

**Chunk-straddling batches.** A batch flushes at file boundaries, not chunk boundaries, so one batch may hold values from two chunks with two dictionaries. The batch keeps the first chunk's dictionary; values from the second record `-1` and decode from the packed bytes for that batch only. The next batch adopts the second chunk's dictionary.

**Both readers.** The row readers (flat and nested) resolve every string accessor through `stringAt`: typed `getString`, generic `getValue`, and the list and map views. `ColumnReader.getStrings()` does too. Where the column path compacts a batch (filtered flat reads, repeated leaves), `LeafCompaction.compactBinary` gathers the entry indices along with the bytes and carries the dictionary over, so the compacted batch resolves against the same cache.

**Lifetime and threading.** The cache lives on the `ByteArrayDictionary`, which lives as long as the pages and batches that reference it: one column chunk's worth of reading. The returned `String`s are immutable, so handing one instance to many rows, and letting callers keep it past `next()`, is safe; the flyweight reuse contract concerns the mutable batch buffers, not immutable values. The cache is filled on the consumer thread through `stringAt`. A concurrent fill of the same entry could at worst decode it twice and never yields a wrong value, since entries are immutable. Untested.

Tests: `DictionaryParserTest`, `DictionaryCodecFailureTest`, `DictionaryTest`, `DictionaryEndToEndTest`, `NestedDictBatchBoundaryTest`, `ByteArrayDictionaryInternTest`, `DictionaryStringReuseTest`.

## Leaf kinds and nested primitive leaves

A row-reader accessor turns a stored leaf value into the Java value it returns by one of two routes, the same on the flat and nested paths.

- **Typed** accessors (`getDate`, `getDecimal`, `PqList.dates()` and their siblings) name the type they return. They read the stored primitive straight from the column array and call the `LogicalTypeConverter` entry point for that type. Nothing is boxed on the way: `getDate` reaches `intToDate(int)` from an `int[]`.
- **Generic** accessors (`getValue`, `PqList.values()`, `PqMap.Entry.getValue()`) return whatever the column holds, so they start from an `Object` and dispatch. The box is the return type of the accessor. On the nested path they go through `NestedLeafDecoder.decode`, which adds the `SchemaNode` unwrap and returns a group node untouched, since struct, list and map values are built by the flyweights.

`LeafKind` is the single statement of how a leaf decodes on the generic route:

| Kind | Leaf | Generic value |
|---|---|---|
| `STRING` | `BYTE_ARRAY` annotated `STRING`, `ENUM` or `JSON` | the per-chunk cached `String` through `stringAt` |
| `INT96_TIMESTAMP` | `INT96` with no annotation | an `Instant`, by the conventional reading of the legacy timestamp |
| `RAW` | any other unannotated leaf | the physical value as stored |
| `CONVERT` | any other annotated leaf | `LogicalTypeConverter.convert` |
| `GROUP` | a struct, list or map node | built by the flyweights; no leaf decode |

`FlatRowReader` classifies each column once at construction; the nested flyweights classify per leaf through `LeafKind.of(SchemaNode)`, and `NestedBatchIndex.decodeLeaf` sends a `STRING` leaf to `getString` before `NestedLeafDecoder.decode` is reached, because that method decodes from a raw `byte[]` and cannot use a cached `String`. `LeafKind.of(PhysicalType, LogicalType)` never answers `GROUP`, which is why a flat column's `GROUP` case is an internal error.

**Byte-array payloads.** `DECIMAL`, `UUID`, `INTERVAL`, `FLOAT16` and the 12-byte `FIXED_LEN_BYTE_ARRAY` timestamps over a byte-array column are decoded where they sit in `BinaryBatchValues.bytes` (`decimalAt`, `uuidAt`, `intervalAt`, `float16At`, `flba12InstantAt`, `flba12LocalDateTimeAt`), each backed by an offset-taking overload on `LogicalTypeConverter` (on `Flba12Timestamps` for the two timestamp forms). `byteArrayAt` materialises a copy and is reserved for accessors that hand the `byte[]` to the caller. Reading in place does not depend on escape analysis removing a discarded copy, which it does not do reliably on list elements.

The guards that reject a column an accessor does not fit, and the conversion rules per annotation, are in [LOGICAL_TYPES.md](LOGICAL_TYPES.md).

Tests: `LeafKindTest`, `NestedLeafDecoderTest`, `DictionaryStringReuseTest`.

## SIMD and scalar fallback

`SimdOperations` (`internal.encoding.simd`) is the interface for the vectorisable kernels; `ScalarOperations` implements it with plain loops and `VectorOperations` in the `java22` overlay, which depends on the incubating Vector API (`jdk.incubator.vector`). Of its kernels only `countNonNulls` uses vector instructions; its dictionary kernels are scalar loops, since the Vector API offers no simple gather for an index lookup.

**Selection.** `VectorSupport.operations()` returns the implementation, decided once per JVM when `VectorSupport` initialises. `core` is a multi-release JAR:

| Runtime | Implementation |
|---|---|
| Java 21, or classes loaded from a directory rather than the JAR | the base `VectorSupport`: always `ScalarOperations` |
| Java 22+ from the JAR, `--add-modules jdk.incubator.vector` absent | the `java22` overlay: the Vector API reference fails to link, and it falls back to `ScalarOperations` |
| Java 22+ from the JAR, module present, preferred `int` vector shorter than 4 lanes | `ScalarOperations` |
| Java 22+ from the JAR, module present, 4 or more lanes | `VectorOperations` at the preferred species width |

There is no system property or reader option; omitting the module flag is the way to run scalar. The user-facing statement is in [docs/content/reference/configuration.md](../docs/content/reference/configuration.md).

**Where it is used.** Only `RleBitPackingHybridDecoder` calls it, on the dictionary and `RLE`-boolean paths:

- `countNonNulls` sizes the index stream of a page with definition levels.
- `applyDictionaryInts` / `Longs` / `Floats` / `Doubles` perform the lookup for an all-present page of a numeric dictionary, scalar in both implementations. A page with nulls, and every `ByteArrayDictionary` page, take a scalar scatter loop.

The other decoders, the level decode and `PageDecoder`'s own non-null count are scalar.

**Contract.** Both implementations return identical results for every input, and the scalar path is always complete, so correctness never depends on which one is selected.

Tests: `SimdOperationsTest`, which loads `VectorOperations` from the multi-release overlay and compares it with `ScalarOperations`.

## Boundaries

- **Pages without CRCs (#1095).** A corrupt page body in a file written without page CRCs decodes to wrong values without error.
- **Untrusted page headers (#201).** `uncompressed_page_size` sizes the decompression buffer unchecked, and dictionary indices are not validated ahead of the lookup.
- **Materialised levels and indices (#726).** Definition levels and dictionary indices are expanded to one `int` per entry; only the single-RLE-run all-present page is recognised without expansion.
- **Vector API bit unpacking (#680).** The bit-packed index and level decode is scalar.
- **Index array on non-string byte-array dictionaries.** `ByteArrayDictionary.decodePage` builds the per-value entry-index array for every byte-array dictionary page, including `INT96`, `FIXED_LEN_BYTE_ARRAY` and unannotated `BYTE_ARRAY` columns whose batches never record it.
