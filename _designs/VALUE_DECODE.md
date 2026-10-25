# Value decode

Describes how the bytes of one data page become typed Java values: the page header and decompression hand-off in `PageDecoder`, the in-place value readers and the decoder each encoding drives, dictionary pages and the per-chunk reuse of dictionary strings, the leaf classification (`LeafKind`) both row readers decode through, and the Vector API kernel with its scalar fallback.

Related documents:

- [READ_PIPELINE.md](READ_PIPELINE.md): where pages come from and which thread decodes them
- [NESTED_DECODE.md](NESTED_DECODE.md): how levels become nested layers, the level scratch and the fixed-size-list fast path
- [COLUMN_READER.md](COLUMN_READER.md): the batch layout the column reader exposes
- [ROW_READER.md](ROW_READER.md): row-reader accessor addressing
- [LOGICAL_TYPES.md](LOGICAL_TYPES.md): the annotation model and per-type conversions
- [STATISTICS_PRUNING.md](STATISTICS_PRUNING.md): dictionary-based row-group pruning and the location of the dictionary page
- [THREADING.md](THREADING.md): which threads decode, and the context of zero threads whose reads decode on the threads driving them
- [docs/content/concepts/parquet-layout.md](../docs/content/concepts/parquet-layout.md): the page hierarchy, described for users

## Page decode

`PageDecoder` is built per column chunk from the column's `ColumnSchema`, its `ColumnMetaData` (for the codec) and the context's `DecompressorFactory`. It refuses a `FIXED_LEN_BYTE_ARRAY` column without a positive width (`FixedWidthValidator`, as the read does before it starts), so every page of the column, null placeholders included, is read at the column's width. Every data page is read in place: a decode job takes the page's header as its fetch plan parsed it, reads its levels and locates its values, and the fills write each value once, from the page's value section straight into the batch arrays, with no page-sized value array between. The value section is the decompressed page, or the fetched page itself where its values are stored uncompressed and read where they lie ([Stored values](#page-decode)).

The steps, in order (`PageDecoder.readInPlace`):

1. Take the page header the fetch plan parsed (`PageHeaderReader`) to locate the page and admit its task, handed over with the page's bytes as a `DataPage`, so it is parsed once. A header whose sizes exceed the [page limit](#page-decode) is refused as it is parsed, and a data page declaring more values than its column chunk's `num_values` before anything is sized from it: the two then disagree about the column. A page that is not a data page, or whose encoding has no in-place reader for the column's type, is refused before anything is decompressed ([Refusal](#encodings-and-readers)).
2. Slice the body without copying. If the header carries a CRC, check it over the stored (compressed) body (`CrcValidator`); a mismatch raises `ParquetReadException`.
3. Decompress and split the body by page type:

| Page type | Levels | Values |
|---|---|---|
| `DATA_PAGE` (v1) | inside the compressed body; the whole body is decompressed, then each level stream is read behind its 4-byte little-endian length | follow the levels in the decompressed body; in a chunk with codec `UNCOMPRESSED`, values read where they lie are not copied, and only the level sections are |
| `DATA_PAGE_V2` | stored uncompressed ahead of the values, with lengths from the header | decompressed only when the header's `is_compressed` is set; stored values are read where they lie, and so are values read in place in a chunk with codec `UNCOMPRESSED`, whatever the flag |

4. Decode repetition levels when `maxRepetitionLevel > 0` and definition levels when `maxDefinitionLevel > 0`, both with `RleBitPackingHybridDecoder` at the bit width of the maximum level, and the dictionary ids of a dictionary-encoded page, into scratch.
5. Build the page's value reader for its encoding (next section).

A `PageDecodedEvent` (JFR) is committed per page read with the column and both sizes; it spans the steps above and the value reader's checks, not the fills that write the values.

The entry points differ in the column they serve and in where the values live:

| Entry point | Column | Returns | Values in |
|---|---|---|---|
| `decodeFlat` | without repetition, fixed-width numeric or `BOOLEAN` | `FlatPageValues` | the fetched page for [stored values](#page-decode); otherwise the task's transient buffer, or the fetched page for stored `DATA_PAGE_V2` values in a heap buffer, for the page's task |
| `decodeFlatBinary` | without repetition, `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` or `INT96` | `BinaryPageValues` | the task's transient buffer or the fetched page, for the page's task |
| `decodeNestedInPlace` | decoded through levels | `InPlaceNestedPage`: levels, `fixedListK` and one of the two readers | the fetched page for [stored values](#page-decode); otherwise the page's own scratch, until its cells are filled ([NESTED_DECODE.md](NESTED_DECODE.md#level-decode-and-scratch)) |
| `nullsInPlace` | a null placeholder ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md)) | all-zero definition and repetition levels and no value bytes, without reading the page; refuses a required column | none |

**Decompression hand-off.** `DecompressorFactory.getDecompressor(codec)` returns a `Decompressor` per call; a codec whose library is absent, and `LZO`, raise `UnsupportedOperationException` naming the dependency, and that type is never re-typed as a read failure (see [docs/content/reference/error-handling.md](../docs/content/reference/error-handling.md)). `GZIP` goes through libdeflate over FFM when the context has a libdeflate pool (Java 22+) and through `java.util.zip.Inflater` otherwise; the user-facing switch is in [docs/content/reference/configuration.md](../docs/content/reference/configuration.md). Every decompressor, `UNCOMPRESSED` included, checks that it produced exactly the declared uncompressed size. A page header that misstates its stream fails with `ParquetReadException`, from a heap or a direct page, without the decompressor reading past the page or writing past the declared size: every native call is given the page's length and the declared size as its bounds, and Snappy, whose native call takes no output bound, has the stream's own length checked against the declared size before it writes. LZ4 and LZ4_RAW use lz4-java's safe decompressor, which takes both bounds; the fast decompressor takes only the output length, and the native implementation some lz4-java releases give it reads past the page where the header overstates it.

Tests: `DecompressorCallerBufferTest`, `MisstatedPageSizeTest`.

A decompressor writes into a buffer its caller gives it: a page's own scratch when its values outlive its task, otherwise the task's reused transient buffer (`PageDecoder.transientBytes`). Either can be longer than the page's uncompressed size. Three rules follow. Readers bound their reads by the page's size, never by the array's length, so a page that declares more values than its body holds fails as malformed instead of reading bytes left by an earlier page. The converse holds for `PLAIN`, whose values have no framing of their own: a value section, or a dictionary page body, with bytes left over after its last value fails as malformed too, since values of a width other than the one the reader assumes (such as `FIXED_LEN_BYTE_ARRAY` values written with `BYTE_ARRAY` length prefixes) would otherwise decode at the wrong boundaries. A reader over the transient buffer is used only within the task that decompressed it, since the next page decoded with the same buffers overwrites it; a page whose values outlive its task is decompressed into its own scratch.

**Page limit.** A page header's sizes are the first values of a page the reader sees, and the decode sizes buffers from them before any of the page's data is read; nothing else in the file bounds them. How much a read may hold of what they ask for is the [working-set limit](#page-decode)'s bound. The header readers hold each size to the longest array the decode allocates from it: `uncompressed_page_size` to the longest array the JVM allocates on every platform, a few elements short of `Integer.MAX_VALUE`, and a data page's `num_values` to one fewer, as a binary page's offsets take one entry per value and one more. A larger value fails with `ParquetReadException` naming the field and the bound (`PageLimit`, `ThriftCompactReader.readBoundedI32`), so no arithmetic sizing an array from it overflows. A dictionary page's `num_values` is checked against the page's bytes as it is parsed instead ([Dictionary pages](#dictionary-pages)). A `DATA_PAGE_V2`'s level byte lengths are checked against the page's bytes before they size the array the levels are copied into. Every reader of page headers, the CLI included, parses them through those readers.

Tests: `MalformedMetadataValidationTest`, `DeclaredValueCountTest`.

**Working-set limit.** What a read must hold to produce its next batch is its working set, and the file's declared sizes decide it: each decoded column holds a dictionary's entries and a data page's batches until the consumer has read them (a nested column also the page's decompressed bytes and levels, until its cells fill), and the task decoding a page holds the bytes it decompresses or copies the page into, none for values read where they lie ([Stored values](#page-decode)), an `int` per value for each level array, and the `int` arrays its encoding decodes through (dictionary or `RLE` boolean ids; a binary page's offsets, with the lengths of the two delta byte-array encodings) while it runs. A batch holds the dictionary of every chunk it has rows of, since their pages decode against them, so a batch spanning row groups holds several; a column sums the dictionaries of the chunks whose rows end after the batch holding its current chunk's first row starts, from the read's batch plan ([READ_PIPELINE.md](READ_PIPELINE.md#when-a-batch-closes)), and a dictionary that pruning parsed counts when the column's fetch plan takes it. A data page's batches are charged at their value width per value, a binary column's at its view and dictionary id, a nested column's with the levels and offsets each batch keeps, and `DELTA_BYTE_ARRAY` values, which a page of a few bytes can reconstruct to gigabytes, once their reconstructed size is known and before they are allocated. Each column counts the most dictionary entries one of its batches holds and the largest page it has met in the read, and the read the largest task buffers any page has needed; their sum is held to a limit, checked before each dictionary page is decompressed (`DictionaryParser`) and each data page is decompressed, once its header has passed the checks above (`PageDecoder`); a null placeholder standing in for a page its statistics dropped charges the levels and batches of its values before they are allocated. A read whose working set would exceed the limit fails with `ParquetReadException` naming the page, what it needs, the working set it would reach and the limit (`WorkingSet`). The task buffers count once, since a read needs one task at a time and decoding several pages at once is the reader's choice. That choice is bounded by the context's read-ahead buffer (`ReadAheadBuffer`), which every read on the context shares: a page's decode or a dictionary parse whose buffers exceed the retention size starts as the read's one task while no other such task of the read runs, beside it only while its buffers fit the buffer, which they are charged to until the task ends, and otherwise once a task of the read ends ([READ_PIPELINE.md](READ_PIPELINE.md#decoded-columns)). A refusal only defers a task, and the read's own task always starts, so nothing waits for the buffer and reads sharing it cannot block one another. The buffer also holds what a multi-file read fetches for the files it opens past the next one, under the same rule: a footer read or a span starts only while it fits, and one that does not starts on a later release ([FETCH_PLANNING.md](FETCH_PLANNING.md#opening-files-ahead)). Tasks within the retention size start at once, since a context keeps that much per thread anyway. The buffer is a quarter of the maximum heap, and the internal system property `hardwood.internal.readAheadBytes`, read when a context is created, overrides it; a value that is not a non-negative byte count fails with `IllegalArgumentException`. A working-set charge is never released, so whether a read of every page fails depends on the file and the limit alone, not on timing, the context's threads or other reads; a read that stops early has also charged the pages its columns decoded ahead of where it stopped; the limit is per read, and nothing waits for it, since working-set memory is released only as a consumer reads. The read's `RowGroupIterator` holds the working set, which its fetch plans charge with the dictionaries they parse and its columns' page decoders with their pages; code that decodes outside a read (the CLI, the dictionaries a filter parses to prune row groups) charges a working set of its own per column. The limit is the maximum heap, since the charges err high and a read whose working set exceeds the heap could not run on it, and the internal system property `hardwood.internal.maxWorkingSetBytes`, read for each read, overrides it; a value that is not a positive byte count fails the read with `IllegalArgumentException`.

Tests: `WorkingSetLimitTest`, `TaskAdmissionTest`.

**Stored values.** A page whose value section is stored uncompressed (a `V1` or `V2` page of a chunk with codec `UNCOMPRESSED`, a `V2` page with `is_compressed` unset, or a `V2` page with an empty value section) has its `PLAIN` values of a fixed-width numeric or `BOOLEAN` column read where they lie in the fetched page, with no copy (`PageDecoder.readsStoredValues`). `FlatPageValues` reads such values from a little-endian slice of the fetched page when the page is a direct buffer (a slice of a file's mapping, a buffer from an S3 `GET`), and through its array when it is a heap buffer; the buffer reads see direct buffer classes only, so they stay inlined. A read-only heap buffer, which has neither, is copied as before. A page in another encoding, or of a binary column, is read as the entry points above say. The fetched buffer stays reachable through the page's `PageInfo` until the page's last fill ([FETCH_PLANNING.md](FETCH_PLANNING.md#fetch-plans)), and its bytes do not change: an `InputFile` hands the reader buffers it owns, and Hardwood's own buffers (a slice of a file's mapping, a buffer from an S3 `GET`, a slice of the sparse temp file's mapping) are released by the garbage collector only. A reader's close waits for every job of its read, the decodes and fills among them, before its files are closed ([READ_PIPELINE.md](READ_PIPELINE.md#cancellation-and-close)).

Tests: `StoredValuesInPlaceTest`, `InPlaceReaderOracleTest` (direct-buffer forms).

**Page values.** The levels and values of a page read in place follow one model:

| Property | Statement |
|---|---|
| Positional values | A page has one position per level entry, and a value sits at its entry's position. A position whose definition level is below the maximum holds no value; a fill leaves the batch element there as it is, and a binary column's entry id there is `-1`. |
| All-present | A `null` definition-level array means every entry is at `maxDefinitionLevel`. Required columns always produce it; an optional column produces it when its definition-level stream is one RLE run of the maximum over the whole page (`RleBitPackingHybridDecoder.isSingleRleRunOf`). |
| Scratch arrays | A level, id or byte array may be longer than the page; only the page's entries are valid. The scratch is described in [READ_PIPELINE.md](READ_PIPELINE.md#decoded-columns) and [NESTED_DECODE.md](NESTED_DECODE.md). |
| Flat columns | The repetition levels are `null` when `maxRepetitionLevel == 0`. |

The fixed-size-list fast path reads a page with `null` level arrays and gives it `fixedListK` ([NESTED_DECODE.md](NESTED_DECODE.md)).

A reader failure is a `RuntimeException` on a decode job; `DecodedColumn.placeFailure` classifies it into `ParquetReadException` and names the file, row group, column and page ([READ_PIPELINE.md](READ_PIPELINE.md#error-propagation)).

Tests: `CrcValidationTest`, `RleBitPackingHybridDecoderSingleRunTest`, `DecompressorCallerBufferTest`, `InPlaceReaderOracleTest`.

### Decode buffers

A decode task reuses buffers from the tasks before it through a set of `DecodeBuffers`: each buffer a decode reuses across pages, a codec's native or direct scratch among them, is one slot (`DecodeBuffers.Slot`), declared by the code that uses it. The context's `DecodeExecutor` holds the sets: each task of the pool takes one when it starts and gives it back when it ends. At most as many tasks run at once as the pool has threads, so the executor keeps that many sets, which live as long as the context. A set's buffers grow with the pages decoded with it up to `DecodeBuffers.RETAINED_BYTES` each; the code owning a slot allocates a larger buffer for the task that needs it and does not keep it, and a level scratch drops its larger arrays when its page or cell is done (`PageDecoder.LevelScratch.trim`), so a context keeps no thread's largest page once the read that needed it is done. A larger page's native or direct buffers are freed when the page is done, not left to a collection: libdeflate allocates them in an arena of the page's own, and Snappy decompresses such a page heap to heap. A context of zero threads has no pool: the thread driving a read takes a set for each decode it runs in place, and the executor keeps one set per processor. No set belongs to a thread, so no decode state is left on one.

**Invariant: no decode runs off a context's pool, or, on a context of zero threads, off the threads driving its reads.** Every decode entry point takes a set of `DecodeBuffers` as a parameter (`Decompressor.decompress`, `PageDecoder`'s decode methods, the decode jobs, a cell's completion), and outside tests only the `DecodeExecutor` creates sets, handing one to each task it runs on the pool, or on a context of zero threads to the thread running a read's queue ([THREADING.md](THREADING.md#a-context-of-zero-threads)) (`DecodeBuffers` has no constructor outside its package). A thread off the pool that needs a page decoded, such as the CLI, hands it to the pool and waits for it (`HardwoodContextImpl.decode`); a task of the pool that needs one, such as a column's walk advance or a planning job, decodes it in place. Such a decode goes to the head of the pool's queue, ahead of the read-ahead queued there, since the thread waiting for it cannot go on without it; so does a column's walk advance ([THREADING.md](THREADING.md#invariants)), and every other task runs in arrival order. The pool's threads are the only ones decoding, so reads sharing a context share its threads: a read beside another that keeps the pool busy gets a share of them, with no decode of its own on its caller's thread. A read waits for work only the pool runs, so it may not start on a thread of the pool: on a pool whose every thread waited, none would be left to run it. The context records its pool's threads; building a read's `RowGroupIterator` on one fails with an `IllegalStateException` (`HardwoodContextImpl.requireOffPool`) instead of hanging, and a decode asked for on one runs in place. The `NoThreadLocal` Error Prone check keeps decode state out of thread-locals in main sources; the one thread-local, the I/O tag `FetchReason`, is set and restored around a block, its scope opened in a try-with-resources wherever it is set, which Error Prone's `MustBeClosedChecker` enforces.

Tests: `HardwoodContextDecodeTest`, `MetadataParseThreadTest`.

## Encodings and readers

`PageDecoder.requireReader` decides, from the page's encoding and the column's physical type, whether the page has an in-place reader, before the page is decompressed. The format defines each encoding over a fixed set of physical types, and every pair it defines has one. A page declaring an encoding its column cannot carry is a malformed file and raises `ParquetReadException` naming the legal types: reading it would take values of the wrong shape, wrong in its values and in the type the consumer casts to.

| Encoding | Physical types read | Reader | Refused |
|---|---|---|---|
| `PLAIN` | all | `FlatPageValues.plain`, `BinaryPageValues.plain`: the values in the decompressed page | bytes left over after the last value |
| `RLE_DICTIONARY`, `PLAIN_DICTIONARY` | all but `BOOLEAN` (see [Dictionary](#dictionary-and-string-reuse)) | ids decoded into scratch through `RleBitPackingHybridDecoder`; a numeric fill looks each up in the `Dictionary`, a binary fill writes it as the batch's entry id | no dictionary for the chunk; an index bit width above 32; an id outside the dictionary; a `BOOLEAN` dictionary (`UnsupportedOperationException`, raised when the dictionary page is parsed) |
| `DELTA_BINARY_PACKED` | `INT32`, `INT64` | `FlatPageValues.delta`, driving `DeltaBinaryPackedDecoder` in row order | every other type; a malformed stream header, a count past the `int` range among them, checked when the page is read; a malformed block, raised by the fill that decodes it, a last miniblock shorter than the full length the format pads it to among them |
| `DELTA_LENGTH_BYTE_ARRAY` | `BYTE_ARRAY` | `BinaryPageValues.deltaLength`: lengths decoded up front, bytes in place | every other type; a negative length; value bytes other than the sum of the lengths, checked when the page is read |
| `DELTA_BYTE_ARRAY` | `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` | `BinaryPageValues.deltaByteArray`: values reconstructed from prefixes and suffixes into the page's scratch | every other type; a negative length; a prefix longer than the previous value; suffixes that run out, or bytes left over after the last; on `FIXED_LEN_BYTE_ARRAY`, a value of another width than the column's; all checked when the page is read |
| `BYTE_STREAM_SPLIT` | `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `FIXED_LEN_BYTE_ARRAY` | `FlatPageValues.byteStreamSplit`, `BinaryPageValues.byteStreamSplit`: each value gathered from its byte streams | `BOOLEAN`, `BYTE_ARRAY`, `INT96`; a value section that does not hold exactly the page's values |
| `ALP` | `FLOAT`, `DOUBLE` | `FlatPageValues.alp`: every vector header checked when the page is read (`AlpDecoder.validate`), each vector decoded once into the reader's buffer as the fill reaches it | every other type; a header element count other than the page's non-null count; a vector that does not start where the previous one ended, or a page with bytes left over after the last vector; an exponent, factor or bit width outside the format's range; an exception position outside its vector. A compression mode or integer encoding other than 0, which the format reserves for later variants, raises `UnsupportedOperationException` |
| `RLE` | `BOOLEAN`, behind a 4-byte length | `FlatPageValues.rleBooleans`: decoded into scratch through `RleBitPackingHybridDecoder` at width 1 | every other type |
| `BIT_PACKED` | none | | always: it encodes levels, never values |
| `UNKNOWN` | none | | `UnsupportedOperationException` carrying the raw Thrift value. It, the `BOOLEAN` dictionary and an unknown ALP variant are the refusals that mark a gap in this release; every other refusal marks a malformed file |

**Run headers.** Every RLE/bit-packing hybrid stream, the levels, dictionary ids and `RLE` booleans, refuses a run whose header declares more values than an `int` holds, or whose varint is longer than 64 bits, rather than narrowing it to a negative or a smaller count (`RleBitPackingHybridDecoder.runValues`). `PageRecordCounter` refuses the same runs. `FixedSizeListDetector` declines a repetition stream holding one, leaving the page to the decode that refuses it, and refuses one in the definition levels, which it reads through the decoder.

**Refusal.** `requireReader` is one switch over the encoding, each case naming the types it is refused over, so the refusals are stated once and need no decode: an encoding over a type the format does not define it for, `RLE` over anything but `BOOLEAN`, `BIT_PACKED` always, a dictionary encoding in a chunk without a dictionary, and `UNKNOWN`. A page whose type is not a data page is refused the same way. A dictionary of the wrong kind for the column fails in the page's reader (`FlatPageValues.dictionary`, `PageDecoder.binaryValues`).

**Cursor.** A value reader is read with one forward cursor over the page's rows: a fill writes its rows in page order, and moving to a run's first row counts the non-null values the cursor passes. A fill that starts elsewhere, such as a nested cell's segment or a second cell sharing the page, reads through a `view()` with a cursor of its own, so several fills can read one page at once. An encoding decoded in order (`DELTA_BINARY_PACKED`, `ALP`) gives each view a decoder of its own; a view that starts past the page's first value decodes the values before it and passes over them.

A `DELTA_BINARY_PACKED` page's blocks are decoded by the fill that reaches them, not when the page is read, so a malformed block fails the fill: on a nested column the fill of the cell, which runs on whichever task seals the cell, and the failure names the block's own page ([READ_PIPELINE.md](READ_PIPELINE.md#error-propagation)). The blocks of the rows a page's mask drops past its last kept row are never decoded, so a malformed block there is not detected.

ALP decodes a value as `encoded * 10^factor * 10^-exponent`: two multiplications, in binary32 for `FLOAT` and binary64 for `DOUBLE`, with power-of-ten constants written as literals. The format makes that arithmetic normative so that every reader reproduces the writer's bits; a single multiplication by `10^(factor - exponent)`, a division, or computed constants decode some values one ulp off. Exception values are restored from their stored bits, NaN payloads included. A decoder that runs out of bytes raises, and the decode job classifies the failure as a read failure (see [Page decode](#page-decode)).

`ReferencePageDecoder` (core test sources) decodes a page whole into its levels and one value per entry, the reference the readers are checked against; the encoder tests round-trip through it.

Tests: `PageDecoderEncodingTest`, `PageDecoderPlainSizeTest`, `PageValuesValidationTest`, `PageBodyBoundTest`, `ByteArrayDictionaryInternTest`, `ReadFailurePositionTest`, `InPlaceReaderOracleTest`, `EncodingRoundTripTest`, `DeltaBinaryPackedDecoderHeaderTest`, `DeltaBinaryPackedDecoderWidthSweepTest`, `DeltaBinaryPackedDecoderCursorTest`, `DeltaBinaryPackedTest`, `DeltaByteArrayTest`, `DeltaByteArrayFlbaTest`, `AlpDecoderTest`, `AlpReadTest` (parquet-testing-runner).

## Dictionary and string reuse

### Dictionary pages

A column chunk has at most one dictionary page, ahead of its first data page. `DictionaryParser` parses it once per chunk, on the context's pool ([Decode buffers](#decode-buffers)), and every data page of the chunk decodes against the same `Dictionary` instance. When dictionary pushdown already read the page for pruning, the fetch plan receives that instance and does not parse the page again ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md)).

`DictionaryParser.parsePage` is the one place a dictionary page's own claims are checked, whichever entry point reaches it: the page type is `DICTIONARY_PAGE`, the body is exactly the header's compressed size, the dictionary page header is present, its value count is not negative, and the CRC matches when present. The body is decompressed and read as `num_values` `PLAIN` values, which must fill it exactly. A failure while decompressing or parsing becomes `ParquetReadException` carrying type, counts, sizes and codec, except `UnsupportedOperationException` (an unavailable codec), which leaves as raised.

`Dictionary` is sealed, one variant per storage: `IntDictionary`, `LongDictionary`, `FloatDictionary`, `DoubleDictionary` over primitive arrays, and `ByteArrayDictionary` for `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` and `INT96`, which holds its entries back to back in one `byte[]` with an `int[]` of entry offsets, the form a batch copies in and points its values at. A `BOOLEAN` dictionary raises `UnsupportedOperationException`. An id outside the dictionary raises `ArrayIndexOutOfBoundsException` in the page's reader and is classified as a read failure.

A chunk may mix dictionary and `PLAIN` data pages (a writer that fell back part-way); each page is decoded by its own encoding.

### String reuse

A dictionary-encoded string column references few distinct entries from many rows. Each entry's `String` is materialised once per chunk and the same instance is returned for every value that references it. Callers receive ordinary `String`s; nothing in the API exposes the reuse.

The mechanism has three parts:

| Part | Where | Statement |
|---|---|---|
| Per-chunk cache | `ByteArrayDictionary.internedString(i)` | A lazily allocated `String[]` parallel to the entries; entry `i` is decoded as UTF-8 on first request and cached. |
| Ids on the page | `BinaryPageValues.dictionary()` | A dictionary-encoded byte-array page's reader carries its `ByteArrayDictionary` and one id per non-null value, and these are its only representation of the values. A plain or delta page's reader has neither. |
| Index on the batch | `BinaryBatchValues.dictionary` / `dictIndices` | For a string column the batch records, per value, the entry index or `-1`. `stringAt(i)` returns the cached `String` when the batch has a dictionary and the index is non-negative, and decodes from the value's bytes otherwise. A dictionary-aware binary matcher reads the same index to reuse its cached predicate outcome. |

Every value has a byte view in the batch whatever its encoding (a dictionary value's view points into the batch's copy of the dictionary, see [COLUMN_READER.md](COLUMN_READER.md#variable-length)), so `getBinary` and raw-byte access are unaffected and the fallback in `stringAt` is always available.

**Which columns.** Every `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` and `INT96` column records entry indices: they back `ColumnReader`'s dictionary ids ([COLUMN_READER.md](COLUMN_READER.md#dictionary-ids)) as well as string reuse. Only text columns reach `stringAt`: the consumer side asks `LeafKind.of(type, annotation) == STRING` (a `BYTE_ARRAY` annotated `STRING`, `ENUM` or `JSON`, or carrying no annotation where the accessor allows it) before routing a value through it.

**Batch rules.** A batch starts with no dictionary. Segments are filled in any order, so the first segment of dictionary ids switches it on (`BinaryBatchValues.installDictionary`): the batch adopts that page's dictionary and allocates `dictIndices` at the batch's value capacity. The cell's completion writes `-1` over every segment that wrote no ids and records whether every segment wrote them (`BinaryFill.finish`; `NestedFill` for a nested cell). A column whose chunk is entirely `PLAIN` never allocates `dictIndices` and pays one `null` check per `stringAt`. A cell that takes a returned holder clears its dictionary slot, keeping the array (`BinaryBatchValues.prepareForFill`).

**One chunk per batch.** A batch of binary values holds values of one column chunk ([READ_PIPELINE.md](READ_PIPELINE.md#when-a-batch-closes)), so it draws on at most one dictionary and its strings resolve through that dictionary's shared `String`s; a batch spanning two chunks would have to decode one chunk's values from their bytes, a fresh `String` per value. A batch meeting a second dictionary fails the read (`IllegalStateException`).

**Both readers.** The row readers (flat and nested) resolve every string accessor through `stringAt`: typed `getString`, generic `getValue`, and the list and map views. `ColumnReader.getStrings()` does too. Where the column path compacts a batch (filtered flat reads, repeated leaves), `LeafCompaction.compactBinary` gathers the entry indices along with the bytes and carries the dictionary over, so the compacted batch resolves against the same cache.

**Lifetime and threading.** The cache lives on the `ByteArrayDictionary`, which lives as long as the pages and batches that reference it: one column chunk's worth of reading. The returned `String`s are immutable, so handing one instance to many rows, and letting callers keep it past `next()`, is safe. The cache is filled on the consumer thread through `stringAt`. A concurrent fill of the same entry could at worst decode it twice and never yields a wrong value, since entries are immutable. Untested.

Tests: `DictionaryParserTest`, `DictionaryCodecFailureTest`, `DictionaryTest`, `DictionaryEndToEndTest`, `NestedDictBatchBoundaryTest`, `ByteArrayDictionaryInternTest`, `DictionaryStringReuseTest`.

## Leaf kinds and nested primitive leaves

A row-reader accessor turns a stored leaf value into the Java value it returns by one of two routes, the same on the flat and nested paths.

- **Typed** accessors (`getDate`, `getDecimal`, `PqList.dates()` and their siblings) name the type they return. They read the stored primitive straight from the column array and call the `LogicalTypeConverter` entry point for that type. Nothing is boxed on the way: `getDate` reaches `intToDate(int)` from an `int[]`.

  Every typed decode is stated once, in `LeafDecoder`, as a guardless read over the column's value array, the value's index in it and, where the decode needs them, the physical type and the annotation (or the unit or scale taken from it). Each reader computes the index by its own addressing and calls these reads directly, or, for a single nested leaf, through `LeafDecoder`'s `read*` methods, which take the `NestedBatchIndex`, the projected column and the leaf's `SchemaNode` and unwrap them into the same reads. A `PqList` typed view reads the unit or scale off the annotation once and calls the reads per element. A decode fixed there is fixed for every accessor.
- **Generic** accessors (`getValue`, `PqList.values()`, `PqMap.Entry.getValue()`) return whatever the column holds, so they start from an `Object` and dispatch. The box is the return type of the accessor. Both readers dispatch by `LeafKind` (below) and reach `LeafDecoder.decode` for the kinds that start from the raw value: `FlatRowReader` with the `LeafKind` it classified at construction, the nested path through an overload that adds the `SchemaNode` unwrap and returns a group node untouched, since struct, list and map values are built by the flyweights.

`LeafKind` is the single statement of how a leaf decodes on the generic route:

| Kind | Leaf | Generic value |
|---|---|---|
| `STRING` | `BYTE_ARRAY` annotated `STRING`, `ENUM` or `JSON` | the per-chunk cached `String` through `stringAt` |
| `INT96_TIMESTAMP` | `INT96` with no annotation | an `Instant`, by the conventional reading of the legacy timestamp |
| `RAW` | any other unannotated leaf | the physical value as stored |
| `CONVERT` | any other annotated leaf | the in-place read of a typed accessor for a binary `DECIMAL`, `UUID`, `INTERVAL`, `FLOAT16` or `FIXED_LEN_BYTE_ARRAY(12)` `TIMESTAMP` (`LeafDecoder.convertAt`); `LogicalTypeConverter.convert` otherwise |
| `GROUP` | a struct, list or map node | built by the flyweights; no leaf decode |

`FlatRowReader` classifies each column once at construction; the nested flyweights classify per leaf through `LeafKind.of(SchemaNode)`, and `NestedBatchIndex.decodeLeaf` sends a `STRING` leaf to `getString` and a binary `CONVERT` leaf to `LeafDecoder.convertAt` before `LeafDecoder.decode` is reached, because that method decodes from a raw `byte[]`: it cannot use a cached `String`, and the copy it needs is one a typed accessor does not make. A generic value therefore costs what the typed accessor for its annotation costs, and a decode fixed in a typed read is fixed for the generic route too. `GenericBinaryValueTest` pins both. `LeafKind.of(PhysicalType, LogicalType)` never answers `GROUP`, which is why a flat column's `GROUP` case is an internal error.

**Byte-array payloads.** `DECIMAL`, `UUID`, `INTERVAL`, `FLOAT16` and the 12-byte `FIXED_LEN_BYTE_ARRAY` timestamps over a byte-array column are decoded where they sit in `BinaryBatchValues.bytes` (`decimalAt`, `uuidAt`, `intervalAt`, `float16At`, `flba12InstantAt`, `flba12LocalDateTimeAt`), each backed by an offset-taking overload on `LogicalTypeConverter` (on `Flba12Timestamps` for the two timestamp forms). `byteArrayAt` materialises a copy and is reserved for accessors that hand the `byte[]` to the caller. Reading in place does not depend on escape analysis removing a discarded copy, which it does not do reliably on list elements.

The guards that reject a column an accessor does not fit, and the conversion rules per annotation, are in [LOGICAL_TYPES.md](LOGICAL_TYPES.md).

Tests: `LeafKindTest`, `LeafDecoderTest`, `DictionaryStringReuseTest`.

## SIMD and scalar fallback

`SimdOperations` (`internal.encoding.simd`) is the interface for the vectorisable kernels; `ScalarOperations` implements it with plain loops and `VectorOperations` in the `java22` overlay, which depends on the incubating Vector API (`jdk.incubator.vector`). Its one kernel, `countNonNulls`, counts the entries of a definition-level range at the maximum level.

**Selection.** `VectorSupport.operations()` returns the implementation, decided once per JVM when `VectorSupport` initialises. `core` is a multi-release JAR:

| Runtime | Implementation |
|---|---|
| Java 21, or classes loaded from a directory rather than the JAR | the base `VectorSupport`: always `ScalarOperations` |
| Java 22+ from the JAR, `--add-modules jdk.incubator.vector` absent | the `java22` overlay: the Vector API reference fails to link, and it falls back to `ScalarOperations` |
| Java 22+ from the JAR, module present, preferred `int` vector shorter than 4 lanes | `ScalarOperations` |
| Java 22+ from the JAR, module present, 4 or more lanes | `VectorOperations` at the preferred species width |

There is no system property or reader option; omitting the module flag is the way to run scalar. The user-facing statement is in [docs/content/reference/configuration.md](../docs/content/reference/configuration.md).

**Where it is used.** `FlatPageValues.countNonNull` calls it for every non-null count of the read path, and `BinaryPageValues` counts through the same method:

- the non-null values of a page with definition levels, which size its dictionary ids, its `BYTE_STREAM_SPLIT` stride, its `ALP` and `PLAIN` value sections and its delta length streams;
- the non-null values a reader's cursor passes on the way to a run's first row, in both readers.

The level decode and the readers' per-value loops are scalar.

**Contract.** Both implementations return identical results for every input, and the scalar path is always complete, so correctness never depends on which one is selected.

Tests: `SimdOperationsTest`, which loads `VectorOperations` from the multi-release overlay and compares it with `ScalarOperations`.

## Boundaries

- **Pages without CRCs (#1095).** A corrupt page body in a file written without page CRCs decodes to wrong values without error.
- **Materialised levels and indices (#726).** Definition levels and dictionary indices are expanded to one `int` per entry; only the single-RLE-run all-present page is recognised without expansion.
- **Declared page values.** A data page's levels and dictionary ids are decoded whole, so a page within the limit allocates for every value it declares, however few bytes encode them, and a read that needs only its first rows cannot stop inside it.
- **Memory beyond the working set (#1644).** The working-set limit bounds what a read must hold, not what it holds by choice: the task buffers of pages decoded on several threads at once, the next row group's dictionaries and those parsed to prune row groups, and pages decoded ahead of the consumer. Concurrent reads hold up to the limit each. The charges are estimates per value: a batch's fixed overheads (its holder, a nested batch's validity words) are not counted.
- **Vector API bit unpacking (#680).** The bit-packed index and level decode is scalar.
