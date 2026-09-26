# Writer encoding

How the writer turns a buffered column chunk into bytes: the chunk and page layout, how each chunk's value encoding is chosen, the codecs, and the statistics written into the footer. The write model, the row-group lifecycle and sizing, memory and threading are in [WRITER.md](WRITER.md); schema construction, `ColumnBatch`, shredding into levels and `RowWriter` are in [WRITER_INPUT.md](WRITER_INPUT.md); the interop gate and coverage assertion are in [WRITER_VALIDATION.md](WRITER_VALIDATION.md). How a reader consumes the statistics written here is in [STATISTICS_PRUNING.md](STATISTICS_PRUNING.md), and the read-side decoders these encoders invert are in [VALUE_DECODE.md](VALUE_DECODE.md). The user-facing options and tables are in the [writer reference](../docs/content/reference/writer.md) and the [write model](../docs/content/concepts/write-model.md).

## Chunk and page layout

`ColumnChunkBuffer` owns one leaf column's chunk for the open row group. While records arrive it retains the levels a byte per entry and each present value, interned into a dictionary or copied into the `ValueEncoder`'s store; no page exists yet. At flush it encodes the whole chunk: the dictionary page where the chunk has one, then the data pages one by one, each cut, framed, compressed and written straight to the `OutputFile`. The chunk's encoding is settled before its first page is produced, so nothing is revisited once written.

The work splits along one seam. `ColumnChunkBuffer` holds the type-agnostic half: level streams, page cutting, compression, CRC, and the dictionary index stream. A per-physical-type `ValueEncoder` (`IntValueEncoder`, `LongValueEncoder`, `FloatValueEncoder`, `DoubleValueEncoder`, `BooleanValueEncoder`, `BinaryValueEncoder`) holds the typed half: the value store, the dictionary, the value section of a page under each encoding, and the statistics collector. The shredder hands the buffer source positions only, so the buffer never sees a typed value.

### Column chunk

A dictionary-encoded chunk is laid out as:

```
<dictionary page>   DICTIONARY_PAGE header, PLAIN body       ← dictionary_page_offset
<data page 0>       DATA_PAGE header, RLE_DICTIONARY values  ← data_page_offset
<data page 1>       ...
```

Any other chunk is its data pages alone, starting at `data_page_offset`, with `dictionary_page_offset` absent. The dictionary page comes first and `dictionary_page_offset` is always set when it exists; the writer does not use the format's implicit-dictionary form.

**A chunk is encoded one way throughout.** Every data page of a chunk carries the same value encoding, and a dictionary page is written only for a chunk whose data pages are `RLE_DICTIONARY`. The encoding may differ between chunks of one column: under `AUTO` each chunk is decided on what it holds.

`ColumnMetaData` records what the chunk uses:

| Field | Written as |
|---|---|
| `encodings` | `RLE` where the column has a level stream; then `PLAIN` and `RLE_DICTIONARY` for a dictionary chunk, or the chunk's one value encoding otherwise. `PLAIN` appears only where something in the chunk is plain |
| `encoding_stats` | A dictionary chunk: one `DICTIONARY_PAGE`/`PLAIN` entry with count 1 and one `DATA_PAGE`/`RLE_DICTIONARY` entry counting its data pages. Any other chunk: one `DATA_PAGE` entry under its value encoding. A flushed row group holds at least one row, so the list is never empty |
| `codec` | The file-wide codec |
| sizes | Compressed size is what was written; uncompressed size restores each body to its pre-compression length. Both include the page headers |

The complete `encoding_stats` inventory is what lets a reader prove a Hardwood-written dictionary covers every value of its chunk, making the chunk eligible for dictionary pruning ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md#dictionaries)).

### Pages

Every data page is a V1 `DATA_PAGE`. Its body is `[rep levels?][def levels?][value section]`, each level stream RLE/bit-packed through `LevelEncoder` and prefixed by its 4-byte little-endian length. A flat `REQUIRED` column has neither stream. `num_values` counts every level entry, nulls included; only present values reach the value section.

The value section of an `RLE_DICTIONARY` page is `[1-byte index bit width][RLE/bit-packed indices]`, running to the end of the page and not length-prefixed. Each page declares its own bit width, sized from its own largest index, so a page whose values sit low in the dictionary pays only for the bits they need. A single-entry dictionary gives a zero bit width. Every other page's value section is its present values under the chunk's value encoding, produced by `ValueEncoder.encodeInto`.

The dictionary page is a `DICTIONARY_PAGE` header with `num_values` equal to the dictionary size and encoding `PLAIN`, over the distinct values `PLAIN`-encoded in index order. Indices are assigned in first-seen order. `is_sorted` is not written.

Every page body, dictionary page included, is compressed with the file's codec before framing, and every page header carries a CRC-32 over the body as stored. No data page carries inline statistics.

**Each page decodes without the page before it**, since a reader may seek to any page. A page encodes its value range standalone: its own `DELTA_BINARY_PACKED` header and first value, its own `DELTA_BYTE_ARRAY` prefix baseline (the page's first value has prefix length zero), its own byte streams for `BYTE_STREAM_SPLIT`. The only chunk-level state a page depends on is the encoding choice and, for `RLE_DICTIONARY`, the dictionary page.

### Page cut

A page is cut at flush, on the bytes it encodes to. By then the chunk's encoding is settled, so each entry is charged what it will cost: a dictionary-encoded value the index width, a `PLAIN` fixed-width value its width, a `PLAIN` `BYTE_ARRAY` value its 4-byte prefix plus its length from the store's offsets. Levels are charged the width their stream encodes at, which RLE beats on any column whose levels run. The page takes entries until the next would cross `pageTargetBytes`, and at least one, so the target is a ceiling that only a single value larger than the whole target can breach.

The three delta encodings are the exception. Their width is a property of the values, which only encoding reveals, so the cut charges them the width the type would take `PLAIN`. No delta encoding exceeds that, so such pages land under the target. `AUTO` never chooses a delta encoding, so only a caller who named one reaches this.

A page cut counts level entries, not records: a page may end part-way through a repeated record, and a record larger than the target spans several pages.

Tests: `WriterLayoutTest`, `WriterDictionaryTest`, `WriterEncodingPolicyTest`, `ColumnMetaDataWriterTest`, `RowGroupDictionaryFilterSourceTest`, `WriterInteropTest` (parquet-testing-runner).

## Encoding selection

Every leaf column carries an **encoding policy**, a `ColumnEncoding`, resolved at writer creation from the most specific setting that names it:

1. the policy configured for that column's leaf path (`WriterConfig.Builder.encoding(String, ColumnEncoding)`), else
2. the file-wide default (`WriterConfig.Builder.encoding(ColumnEncoding)`), else
3. `AUTO` (`WriterConfig.DEFAULT_ENCODING`).

`AUTO` is the writer's decision between `RLE_DICTIONARY` and `PLAIN`, taken per chunk ([Dictionary decision](#dictionary-decision)). Every other policy names an encoding outright, and a column that names one builds no dictionary: its chunks carry that encoding in every row group of the file. There is no separate dictionary switch. Dictionary-or-`PLAIN` is the whole of what `AUTO` decides, so declining a dictionary is naming `PLAIN`, file-wide or for one column.

`ColumnEncoding` is a public enum of its own, separate from `dev.hardwood.metadata.Encoding`, which is the read side's vocabulary for what a file says and carries members no writer input should accept (`PLAIN_DICTIONARY`, `BIT_PACKED`). The accepted set is a compile-time fact, and `AUTO`, a policy rather than an encoding, has a member.

| Policy | Physical types |
|---|---|
| `AUTO` | every writable type |
| `PLAIN` | every writable type |
| `DELTA_BINARY_PACKED` | `INT32`, `INT64` |
| `DELTA_LENGTH_BYTE_ARRAY` | `BYTE_ARRAY` |
| `DELTA_BYTE_ARRAY` | `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` |
| `BYTE_STREAM_SPLIT` | `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `FIXED_LEN_BYTE_ARRAY` |

`EncodingSupport` is the single table, read by validation and by anything enumerating the writer's capabilities. `AUTO` and `PLAIN` are universal: no data can make either impossible. `AUTO` on a `BOOLEAN` column resolves to `PLAIN`, since one bit per value is already smaller than any dictionary page plus index stream (`EncodingSupport.dictionaryCapable`).

### Configuration, not schema

Neither the codec nor the encoding is part of a Parquet schema: `SchemaElement` carries nothing about storage, while `ColumnMetaData` carries `encodings` and `codec` for one column chunk and page headers carry an encoding per page. Both therefore live on `WriterConfig`. `FileSchema` is what `ParquetFileReader.getFileSchema()` returns, and a storage field on it would have no answer on the read side, where chunks of one column may disagree. The two also vary independently: one schema is written with different codecs for different destinations, and one configuration serves many schemas.

The coupling that remains is the per-column policy, keyed by leaf path and so meaningful only against a schema that has that column. Both are in hand at `ParquetFileWriter.create`, which is where the path is resolved and the type checked.

A column is addressed by its **dotted leaf path** as the schema spells it, synthetic `list.element` and `key_value.key` segments included, because only the full path identifies a leaf in a schema that repeats a name at several depths. The codec is file-wide only, since a page body compresses the same way whatever its type.

### Validation

Each side checks what it can see:

- `WriterConfig.Builder` rejects a null path or policy. Every `ColumnEncoding` member is writable for some type, so there is nothing else to check without a schema.
- `ParquetFileWriter.create` rejects a path matching no leaf column, with a message listing the schema's leaf paths, and a policy illegal for its column's physical type, naming the column, its type and the policy. The file-wide default is held to the same rule as an override: a file-wide `BYTE_STREAM_SPLIT` over a schema with one `BYTE_ARRAY` column fails; the writer does not quietly resolve that column to something else.

Both run before `OutputFile.create()`, after the physical-type and schema-shape checks and before the codec is resolved, so a misconfigured file is never begun.

Tests: `WriterEncodingPolicyTest`, `WriterConfigTest`, `EncodingSupportTest`.

## Dictionary decision

Under `AUTO` a chunk's encoding is chosen once, from the values the whole chunk holds, and the chunk is then encoded as a whole:

1. **While values arrive**, every present value is interned into the chunk's dictionary, which makes the chunk's cardinality exact.
2. **At flush**, the comparison below decides the chunk's encoding, and the chunk is encoded in it page by page.

### The rule

For a chunk holding `N` present values with exact cardinality `K`, compare uncompressed sizes:

- `plainBytes`: the values as `PLAIN`, `N × width` for a fixed-width type and `Σ (4 + length)` for `BYTE_ARRAY` (`ValueEncoder.plainValueBits`).
- `dictionaryBytes`: the dictionary body as `PLAIN` over `K` entries (`ValueEncoder.dictionaryPlainBytes`), plus the index stream at `bitWidth(K − 1)` bits per value. Run headers are left out, being a fraction of a percent of the stream.

The chunk is `RLE_DICTIONARY` when `dictionaryBytes < plainBytes`, and `PLAIN` otherwise (`ColumnChunkBuffer.dictionaryWins`). A chunk with no present value has an empty dictionary and is `PLAIN`, with no dictionary page. A chunk of a single distinct value is the dictionary's best case and stays dictionary-encoded, with a one-entry dictionary and zero-bit-width indices.

The comparison is on uncompressed sizes. Comparing compressed sizes would mean trial-compressing both forms, a cost the rare chunk where compression reverses the ranking does not repay.

Other writers decide sooner and on less. Arrow C++, arrow-rs and parquet-go stream, falling back to `PLAIN` part-way through a chunk once the dictionary reaches a byte limit, which leaves a mixed chunk (index pages, then plain pages, the dictionary page retained) and writes an all-distinct column with a dictionary that loses to `PLAIN` until the limit is hit. parquet-java compares sizes over the first page, then applies a byte or entry cap. DuckDB analyzes the whole row group before writing, as Hardwood does. The price of deciding on the whole chunk is interning every value to learn its exact cardinality, work a streaming writer does not do, and it falls on the columns that lose: they are hashed into a dictionary that is then resolved back into values. Early abandonment moves the same comparison to the chunk's prefix, so a losing column stops paying once the probes see it losing, and the flush-time decision keeps its result for every other column.

A chunk that loses resolves its interned indices back into stored values (`ColumnChunkBuffer.giveUpDictionary`) and writes no dictionary page. The in-memory representation, dictionary form or stored values, is a memory matter covered in [WRITER.md](WRITER.md); **it never decides the encoding**. An in-memory index costs an `int` while an encoded index costs `bitWidth(K − 1)` bits, so on a 4-byte type the dictionary form is no cheaper to hold than the values even where dictionary encoding writes less than half the bytes. The encoding is the size comparison above, and nothing else.

### Early abandonment

The same comparison also runs while values arrive, over the prefix the chunk holds, at a fixed schedule of present-value counts that doubles from a first probe of a few thousand values (`FIRST_PROBE_VALUES`). A dictionary losing **two consecutive probes** is given up on the spot, so a column whose values are all distinct stops interning at its second probe instead of hashing a row group's worth into a table the flush comparison would reject.

The probe is the flush predicate evaluated earlier against the same fields, through the same `giveUpDictionary`. Where prefix and chunk agree the produced file is byte-identical and only the work differs. The schedule counts present values because a chunk has no pages until flush.

Two consecutive losses, rather than one, protect a column whose distinct values are front-loaded: all distinct through the first probe and repeating afterwards. Such a column has stopped minting values by the second probe, its distinct ratio has halved, and the comparison has swung back to the dictionary; it keeps its dictionary and is decided at flush. The predicate is not a knife edge: for a fixed-width type of width `W` the dictionary loses only when the distinct ratio exceeds about `1 − b/(8W)` for index bit width `b`, which is where the ratio is also stable. A column that looks all-distinct through the second probe and saturates only later is the residual risk: it is written `PLAIN` where a dictionary would have paid. The file is correct either way.

A chunk abandoned early states no `distinct_count`, having thrown away what it counted with. Probe state is per chunk and resets with each row group.

### What bounds a dictionary

A dictionary has no size limit of its own beyond the structural ones: a chunk gives its dictionary up when the hash table is full (`DictionaryEncoder.MAX_SIZE`, the last size whose next resize fits an `int`; untested, the limit being out of a test's reach), and the flush comparison never chooses a dictionary whose page body would exceed a store's capacity. Otherwise it is part of what the chunk retains, and what a row group retains is what `rowGroupBufferTargetBytes` is compared against ([WRITER.md](WRITER.md)), so a large dictionary spends the row group's budget and the row group is cut sooner. The byte target bounds memory without deciding an encoding: a column whose dictionary is large but pays keeps it. The probes bound work: they stop a losing dictionary from growing. A limit that forced `PLAIN` would conflate the two, writing a column just past it `PLAIN` however well a dictionary would have paid.

The decision is internal: neither the probe schedule nor the rule is configurable.

Tests: `WriterDictionaryTest`, `WriterDictionaryProbeTest`, `RowGroupBufferStoreCapacityTest`.

## Named policies

A column under a named policy skips everything above: no interning, no index array, no probes, no `distinct_count` (except `BOOLEAN`, see [Statistics](#statistics)). Its values go straight to the value store. Statistics, level streams and page cuts are unaffected.

The delta encodings and `BYTE_STREAM_SPLIT` are never chosen by `AUTO`. Its comparison works because both candidates' sizes follow from what the chunk already holds: the exact cardinality, the dictionary's plain bytes, and each value's plain width. Neither optional encoding has that property. `DELTA_BINARY_PACKED`'s size follows from the bit width of the deltas inside each miniblock, a property of value order nothing in the buffer measures. `BYTE_STREAM_SPLIT` changes no page's size at all; it reorders bytes so the codec afterwards finds structure, which makes its payoff a property of the codec. Choosing either automatically would take a trial encode per chunk or an uncalibrated heuristic, so the writer honours an explicit choice and does not guess.

No policy demands a dictionary. A dictionary is the one encoding the writer cannot promise, since a chunk whose values repeat too little has to be written another way; a `DICTIONARY` member would either fail a write over data the writer can encode or accept a request and not honour it.

### Encoders

Each encoder in `dev.hardwood.internal.encoding` is the inverse of the decoder beside it and produces one page's value section from a range of the chunk's stored values. The byte-array encoders read `BinaryValueEncoder`'s store, which keeps a chunk's values packed end to end with their offsets, the layout a length stream and a prefix comparison both want.

- **`DELTA_BINARY_PACKED`** (`DeltaBinaryPackedEncoder`) writes 128-value blocks of four 32-value miniblocks. A trailing block writes zero as the bit width of each miniblock it does not need and no bytes for them; a miniblock holding any value is written whole, padded with the block minimum. Deltas are computed with wrap-around at the column's width, so an `INT32` column spanning the full range encodes without overflow; `DeltaBinaryPackedDecoder` accumulates in a `long` and narrows, and the narrowing recovers the value. **The block minimum is signed and the residues unsigned.** A signed minimum keeps a descending column cheap: its deltas are negative, and measured against a negative minimum they leave residues near zero. Comparing them unsigned would drive the bit width to the full type, which round-trips and costs an order of magnitude more.
- **`DELTA_LENGTH_BYTE_ARRAY`** is the lengths as `DELTA_BINARY_PACKED` followed by the concatenated bytes.
- **`DELTA_BYTE_ARRAY`** is the prefix lengths as `DELTA_BINARY_PACKED`, then the suffixes as `DELTA_LENGTH_BYTE_ARRAY`. For `FIXED_LEN_BYTE_ARRAY` the suffix lengths are still written, the values being fixed-length only by schema.
- **`BYTE_STREAM_SPLIT`** scatters `K` streams where `K` is the type's byte width (`typeLength` for `FIXED_LEN_BYTE_ARRAY`). The streams carry present values only, so a page's section is its non-null count times `K`.

`BitPacker` packs fixed-width values LSB-first for both the RLE/bit-packing hybrid's eight-value groups and the delta miniblocks. It takes widths up to 64 bits and a `long` domain, for an `INT64` column whose deltas span the type. Both callers pack a whole number of bytes at every width.

A concrete `ValueEncoder` implements the encodings its physical type may carry and throws `IllegalStateException` for the rest, a combination validation has already excluded.

Tests: `WriterEncodingPolicyTest`, `DeltaBinaryPackedEncoderTest`, `ByteArrayEncodingsTest`, `ByteStreamSplitEncoderTest`, `BitPackerTest`, `RleBitPackingHybridEncoderTest`.

## Codecs

`CompressorFactory` resolves every `CompressionCodec` member to a compressor or to a refusal with its own reason.

| Codec | Library | Form |
|---|---|---|
| `UNCOMPRESSED` | none | the body as it stands |
| `GZIP` | JDK `Deflater` | gzip wrapper |
| `SNAPPY` | snappy-java (optional) | raw block, not the framed stream |
| `ZSTD` | zstd-jni (optional) | library default level |
| `LZ4_RAW` | lz4-java (optional) | raw block |
| `BROTLI` | brotli4j (optional) | library default |

The page header records the uncompressed size, so every codec is used in its length-unaware raw form. Each optional library is checked through `CodecLibraries.require`, whose message names the Maven coordinates to add. The default codec is `ZSTD` when zstd-jni is on the classpath and `UNCOMPRESSED` otherwise, so a caller who did not ask to compress is not made to carry the dependency; an explicitly chosen codec still requires its library.

Two codecs are refused permanently:

- **`LZ4`**: the Hadoop-framed variant the format deprecated in favour of `LZ4_RAW`. The reader keeps decompressing it, since files written before the deprecation exist; the refusal points at `LZ4_RAW`.
- **`LZO`**: no maintained JVM implementation under a licence the project can depend on. The read path refuses it for the same reason.

`ParquetFileWriter.create` resolves the compressor before `OutputFile.create()`, so a refused codec or a missing library fails before the magic bytes are written. A codec that fails on a page body mid-write raises `ParquetWriteException` and fails the writer ([WRITER.md](WRITER.md)).

Every codec compresses at its library's default. There is no level setting: it would multiply the option surface by the codec list, and the codec choice already expresses the speed-versus-ratio trade-off.

Tests: `WriterCompressionTest`, `CompressorFactoryTest`, `WriterCodecFailureTest`.

## Statistics

Every column chunk carries a `Statistics`, accumulated by the `ValueEncoder`'s collector as each value arrives, independently of encoding. `StatisticsWriter` emits:

| Field | Written |
|---|---|
| `null_count` | always |
| `min_value` / `max_value` | where the chunk has a present value and the column's order is defined; never the deprecated `min` / `max` |
| `is_min_value_exact` / `is_max_value_exact` | with each bound: `true` except for a truncated `BYTE_ARRAY` bound |
| `nan_count` | every `FLOAT`, `DOUBLE` and `FLOAT16` chunk, zero included; no other type |
| `distinct_count` | where the chunk knows its cardinality exactly |

Bounds are always exact unless truncated, so a reader may treat `min_value == max_value` with both flags set as proof that a chunk holds a single value.

### Sort order

A column's order is its logical type's where it has one, and its physical type's otherwise. `ValueEncoder.forColumn` and `BinaryStatistics.forColumn` select the collector from the `ColumnSchema` once per chunk, so the order costs nothing per value.

| Order | Columns | Collector |
|---|---|---|
| Signed integer | unannotated `INT32` / `INT64`; `INT_8/16/32/64`; `DATE`; `TIME`; `TIMESTAMP` on `INT64`; `DECIMAL` on `INT32` / `INT64` | `IntStatisticsCollector`, `LongStatisticsCollector` |
| Unsigned integer | `UINT_8/16/32/64` | the same, with the sign bit flipped |
| Unsigned lexicographic | unannotated `BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY`; `STRING`; `ENUM`; `JSON`; `BSON`; `UUID` | `BinaryStatisticsCollector` |
| Signed big-endian | `DECIMAL` on `BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY` | `BinaryStatisticsCollector` |
| Signed little-endian | `TIMESTAMP` on `FIXED_LEN_BYTE_ARRAY(12)` | `BinaryStatisticsCollector` |
| Represented value | `FLOAT`, `DOUBLE`, `FLOAT16` | `FloatStatisticsCollector`, `DoubleStatisticsCollector`, `Float16StatisticsCollector` |
| Boolean (`false < true`) | `BOOLEAN` | `BooleanStatisticsCollector` |
| Undefined | `INTERVAL`, `UNKNOWN`, `VARIANT`, `GEOMETRY`, `GEOGRAPHY`, `LIST`, `MAP` | `NullCountStatistics` for binary; bounds dropped at flush otherwise |

`StatisticsOrder.supportsBounds` is the single table of which columns have defined bounds, an exhaustive switch over `LogicalType`. An undefined-order column writes `null_count` without bounds: parquet-format says so directly for `INTERVAL`, and a bound in an order the reader cannot know would prune away live rows. A binary column of undefined order does not accumulate bounds at all, so a `GEOMETRY` chunk does not copy a blob out on every bound extension only for flush to discard it. The read side mirrors this table in `BoundsReadability` ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md#bounds-readability)).

### Floating point

`NaN` is excluded from the bounds and counted in `nan_count`, so an all-`NaN` chunk carries a count and no bounds. A zero bound is sign-normalized: a zero `min` is written as `-0.0` and a zero `max` as `+0.0`, so a reader's `[min, max]` test is correct for either signed zero. `nan_count` is written even when zero, because only a recorded zero lets a reader prove a chunk holds no `NaN` and keep `gt` / `gtEq` / `notEq` pruning ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md#nan)). `FLOAT16` bounds follow the same rules over the represented half-precision value, not its bytes.

### Binary truncation

`BYTE_ARRAY` bounds longer than `statisticsTruncationLength` (default 64 bytes) are truncated so long values do not bloat the footer:

- a truncated `min` keeps the first `N` bytes, a prefix being `<=` the original;
- a truncated `max` keeps the first `N` bytes, increments the last byte that is not `0xFF` and drops the bytes after it, giving the smallest string of length `<= N` that is `>=` the original; if every kept byte is `0xFF` no such bound exists and `max` is omitted;
- a truncated bound is flagged inexact.

Truncation is order-preserving only under unsigned byte-wise comparison, so it applies only to the lexicographic order. A binary `DECIMAL` bound is never truncated: under signed big-endian comparison a shorter byte string is a different value, in either direction. `FIXED_LEN_BYTE_ARRAY` bounds are written whole and exact. Bounds are copied from the caller's arrays on update, so a caller reusing its arrays before flush cannot corrupt them.

### `distinct_count`

The count is written only where it is exact, since the format asks for the count of distinct values occurring rather than an estimate:

- an `AUTO` chunk that still holds its dictionary at flush states the dictionary size, whichever encoding the comparison then chooses;
- a `BOOLEAN` chunk states it under any policy, knowing its cardinality without a dictionary;
- a chunk abandoned by the probes or by a full dictionary table, and a chunk under a named policy, state none.

`FLOAT` and `DOUBLE` values are interned by raw bit pattern, so `-0.0` and `+0.0`, and `NaN`s of different payloads, are distinct dictionary entries and count separately.

### `column_orders`

The footer carries `column_orders` with one `TYPE_DEFINED_ORDER` per leaf column, in schema order (`ParquetFileWriter.columnOrders`), since the format leaves `min_value` / `max_value` undefined without it. It is the order every collector implements, the floating-point ones included. `IEEE754_TOTAL_ORDER` does not describe these bounds: it requires the exact smallest and largest non-`NaN` values, where the collectors normalize zero bounds, and the smallest and largest `NaN` as the bounds of an all-`NaN` chunk, where the collectors record none.

Tests: `WriterStatisticsTest`, `WriterLogicalTypeStatisticsTest`, `WriterFixedWidthTypeRoundTripTest`, `WriterVariableWidthTypeRoundTripTest`, `WriterFlba12TimestampTest`, `WriterDictionaryTest`.

## Boundaries

- **Page index and Bloom filters.** No `OffsetIndex`, `ColumnIndex` or Bloom filter is written, and data pages carry no inline statistics, so pages of a Hardwood-written file cannot be pruned. Page index writing will need pages aligned to record boundaries, which the entry-count cut does not guarantee on repeated columns (#1291).
- **`distinct_count`** is absent for a chunk that gave its dictionary up before flush (to the probes or a full table) and for a named-policy chunk other than `BOOLEAN` (#982).
- **`SizeStatistics` and `GeospatialStatistics`** are not written. Bounding-box pushdown therefore prunes nothing in a Hardwood-written `GEOMETRY` or `GEOGRAPHY` column.
- **Data page V2** is not produced; every data page is V1.
- **Per-row-group settings.** The codec and the named policies hold for the whole file; changing them at a caller-placed row-group boundary is #985. The writer does not choose a codec per chunk, which `ColumnMetaData.codec` would permit.
- **Automatic delta or byte-stream-split selection**, `RLE` for `BOOLEAN` data pages, and compression levels are not provided.
- **Sorted dictionaries** and `DictionaryPageHeader.is_sorted` are not written (#1258).
- **`GZIP` compression** uses the JDK `Deflater`; the read path decompresses through libdeflate (#1002).
