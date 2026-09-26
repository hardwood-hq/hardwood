# File metadata

How the reader turns a file's metadata bytes into the records everything else plans against: locating and reading the footer, the malformed-input policy of the Thrift compact-protocol parser, the public metadata records and what their absent fields look like, the parse and consistency checks of statistics, the page index and bloom filters, and the per-file cache that holds each parsed footer for one `ParquetFileReader`. It does not cover which byte ranges a read fetches, including the page-index and bloom-filter slices ([FETCH_PLANNING.md](FETCH_PLANNING.md)), how the `InputFile` backends serve a range ([INPUT_FILES.md](INPUT_FILES.md), [S3_STORAGE.md](S3_STORAGE.md)), what pruning concludes from the parsed statistics ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md)), or the exception types and how a failure is placed in the file ([EXCEPTION_MODEL.md](EXCEPTION_MODEL.md)). How the read pipeline plans files and prefetches the next footer is in [READ_PIPELINE.md](READ_PIPELINE.md). The user-facing pages are [metadata.md](../docs/content/how-to/metadata.md), [error-handling.md](../docs/content/reference/error-handling.md) and [parquet-layout.md](../docs/content/concepts/parquet-layout.md).

## Footer read

`ParquetMetadataReader.readFooter` reads a footer from the end of the file alone, in two range reads:

1. The last eight bytes: the little-endian footer length and the trailing magic.
2. The footer body, `footerLength` bytes ending where those eight begin.

| Check | Outcome |
|---|---|
| file shorter than 12 bytes (two magics and the length) | `ParquetReadException` "File too small to be a valid Parquet file" |
| trailing magic `PARE` (encrypted-footer mode) | `UnsupportedOperationException` |
| trailing magic not `PAR1` | `ParquetReadException` "Not a Parquet file (invalid magic number at end)" |
| footer length negative, or footer would start inside the leading magic | `ParquetReadException` "Invalid footer length: …" |
| footer carries `encryption_algorithm` (plaintext-footer mode) | `UnsupportedOperationException` |

The leading magic is never read. Every page is located through the footer, so those four bytes carry nothing a read uses, and on a remote file they would cost a request of their own at the far end of the object. A file whose leading magic is damaged reads normally. What the two reads cost on each backend is in [FETCH_PLANNING.md](FETCH_PLANNING.md#per-file).

Both encryption modes raise the same `UnsupportedOperationException` (`ParquetMetadataReader.ENCRYPTED_MESSAGE`): the file is correct and this library does not decrypt it. How the plaintext-footer case leaves the Thrift parse is in [EXCEPTION_MODEL.md](EXCEPTION_MODEL.md#the-table-above-is-the-whole-list).

The footer is read once per file per `ParquetFileReader`: at `open`/`openAll` for the first file, and through the per-file cache for the rest ([Per-file metadata](#per-file-metadata)). `FileSchema.fromSchemaElements` derives the schema from the parsed footer in the same step; a failure there is a read failure of that file, raised as `ParquetReadException`.

Tests: `ParquetMetadataReaderTest`, `EncryptedFileTest`, `ParquetFileReaderFooterFailureTest`, `ParquetFileReaderOpenFailureTest`. The "File too small" check and a footer length that reaches into the leading magic are untested.

## Thrift parse policy

`dev.hardwood.internal.thrift` decodes the footer, page headers, the page index and bloom filter headers with one `ThriftCompactReader` over a `ByteBuffer` and one static reader class per struct. It is the first code to touch a file and runs before any bound is established, so every value it produces is untrusted until it has been checked. The policy below holds for every struct reader.

### What happens to a field the reader cannot use

Thrift's forward-compatibility rule is that a reader consumes a field it does not recognise by the field's own declared type and moves on: the declared type suffices to skip the field whatever it holds, so the cost stays with that field and the struct keeps parsing. Hardwood applies that rule to every field it can do without and departs from it for the fields it cannot.

| Field | Unknown field id | Wrong wire type, or wrong element type for a collection | Absent |
|---|---|---|---|
| optional | skipped by its declared type | skipped, reported absent, logged at `WARNING` | reported absent |
| required | — | `ParquetReadException` naming the field and both types | `ParquetReadException` at the struct's STOP naming the missing fields |

An optional field has a representation for "not there" that its consumers handle, so one that cannot be decoded uses it: the file loses the field and stays readable, which is what lets a newer writer's output pass through an older reader. A required field has no such representation; reporting `RowGroup.columns` as an empty list would answer a query with zero rows instead of failing it. A required field of the wrong type is rejected where it is rather than skipped and reported missing at the end of the struct, because it did arrive and "missing" would point at the wrong fault.

An entry of an optional metadata list is itself a struct with required fields. For `encoding_stats` and `key_value_metadata`, whose content no decoding depends on (`encoding_stats` feeds only dictionary pruning), an entry that fails its struct's rules makes the whole list read as absent, logged at `WARNING` naming the field, and the file stays readable. A list whose entries do decide how data is read, such as `RowGroup.columns` or `FileMetaData.schema`, fails the parse. `ThriftCompactReader.missingFields` and a union with no or several variants are the complaints about a struct as a whole rather than a field of it.

The gate is four methods, and the typed list reads (`readStructList`, `readStringList`, `readBinaryList`, `readOptionalI64Array`) are built on the list pair; `readBoolArray` checks its element type itself, accepting either `bool` code:

| Method | On mismatch |
|---|---|
| `acceptField(header, expectedType)`, `acceptListHeader(elementType)` | skip and log |
| `requireField(header, expectedType)`, `requireListHeader(elementType)` | throw |

The expected type is always named through `ThriftCompactConstants.FieldType.Codes`, never a hex literal, because two wire codes one bit apart select different branches without any error. The log line and the exception message are built from the same words, `wrong Thrift wire type 0x1 (expected 0x5)`, and a field is either logged and dropped or thrown over, never both. A wire-type nibble that is no compact-protocol type at all raises "Unknown field type", since the bytes did not come from a Thrift writer.

A `bool` field carries its value in the header's type nibble. `readBooleanField` takes a fallback that stands for a field declared as anything else, which is then skipped without a log line. A required `bool` goes through `requireBooleanField`, which fails as `requireField` does.

### A collection is never decoded as another element type

A list header declares one element type for the whole collection. Decoding its elements as another consumes the wrong number of bytes each and desynchronises the stream: value bytes are then read as field headers, and the rest of the enclosing struct (the whole footer, for `FileMetaData.schema`) is misread. An optional list of the wrong element type is skipped element by element by the type it declares, leaving the reader on the byte after it. Elements are skipped through `skipElement`, which differs from `skipField` only for `bool`: a `bool` field carries its value in its header's type nibble, a `bool` element is one byte.

A union is the same argument at one element. Its variant carries the meaning in its field id and an empty struct as its value. `readUnionVariant`, which reads `TimeUnit` and the three bloom filter unions, requires exactly one variant and requires its value to declare `struct`; an unknown bloom filter algorithm, hash or compression raises `UnsupportedOperationException` ([Bloom filter](#bloom-filter)). `LogicalType` skips an arm it does not know by its declared type and drops the annotation with a `WARNING`, so the column reads as its physical type (recorded per leaf on `FileMetaDataReader.ReadFooter`, see [LOGICAL_TYPES.md](LOGICAL_TYPES.md)); it takes the first arm it knows, and a known arm fails the parse like any required struct field when it is not declared `struct` or its struct lacks a required field. `ColumnOrder` skips every variant by its declared type, and one with no variant or an unknown one is `UNKNOWN`.

### Sizes, counts, lengths and offsets are validated where they are read

A length or offset from the file reaches an allocation, an array index or a `ByteBuffer.slice` downstream. Checked at the point of use it produces an unchecked `IndexOutOfBoundsException` or `NegativeArraySizeException` that names neither field nor file. Checked where it is read it produces a `ParquetReadException` naming the field, which the caller attributes to the file.

| Value | Rule |
|---|---|
| the offsets, sizes and row and value counts of `FileMetaData`, `RowGroup`, `ColumnChunk`, `ColumnMetaData`, the page headers, `PageLocation` and `BloomFilterHeader` | read with `readNonNegativeI32` / `readNonNegativeI64`; `SchemaElement.type_length` is checked by `FixedWidthValidator` instead, for the columns a read touches ([EXCEPTION_MODEL.md](EXCEPTION_MODEL.md#an-annotation-the-reader-cannot-use-is-dropped-not-raised)) |
| map size, and list or set element count in the long form | at most the bytes remaining, since every element occupies at least one byte on the wire; a count past the `int` range is rejected, never truncated. A short-form list or set count (at most 14) is left to the element reads to catch |
| binary or string length | at most the bytes remaining, checked before the allocation |
| varint | at most ten bytes |
| field id (short form delta or long form) | within the Thrift `i16` range; narrowing would fold an out-of-range id onto a real one |

A count or length that exceeds the remaining bytes raises `ThriftTruncatedException`, the internal `ParquetReadException` subclass the page-header readers use to grow their peek ([EXCEPTION_MODEL.md](EXCEPTION_MODEL.md)).

Statistical counts (`null_count`, `distinct_count`, `nan_count`, `unencoded_byte_array_data_bytes`, histogram entries and the page index's per-page counts) reach no allocation or slice and are carried as the file records them.

### Enum values the reader does not know

| Enum | Unknown value |
|---|---|
| `Encoding`, `EdgeInterpolationAlgorithm` | the `UNKNOWN` constant |
| `PageType` | the `UNKNOWN` constant in `PageEncodingStats`; `ParquetReadException` in a page header |
| `ConvertedType` | `null` |
| `PhysicalType`, `RepetitionType`, `CompressionCodec` | `ParquetReadException` |
| `ColumnIndex.BoundaryOrder` | `UNORDERED` |

An enum that raises in the footer parse makes the whole file's metadata unreadable, including every column that does not carry the value.

### Naming the failure

A parse failure names the struct and field the reader stood on (`ColumnMetaData.data_page_offset — must be non-negative but was -1`). Each struct reader enters its struct with `ThriftCompactReader.pushFieldIdContext(ThriftStruct)` and restores the enclosing struct and field id on exit, so the innermost struct is the one named; a struct entered without a name (a union's empty marker, a struct `skipField` walks past) is reported without one. `ThriftStruct` lists every field id the format defines for each struct, including those no reader reads, so an id reads as its field name only where the format defines it; `ThriftStructOracleTest` checks the list against parquet-format's generated metadata. The footer is parsed once per file and `ParquetMetadataReader` prefixes its failures with the file name. The page index is parsed per column chunk inside the read pipeline, which adds the file, row group and column on the way out ([Page index](#page-index)).

Tests: `MalformedMetadataValidationTest`, `FooterRequiredFieldsTest`, `PageHeaderRequiredFieldsTest`, `MetadataRequiredFieldsTest`, `PageEncodingStatsReaderTest`, `ThriftCompactReaderTest`, `ThriftFieldNamingTest`, `CorruptFooterNamingTest`, `ThriftStructOracleTest`, `MalformedMetadataFileTest`.

## Metadata model

The parsed metadata is a tree of public records in `dev.hardwood.metadata`, reached through `ParquetFileReader.getFileMetaData()` → `FileMetaData.rowGroups()` → `RowGroup.columns()` → `ColumnChunk.metaData()`. The records mirror the Thrift structs field by field; the user-facing description is in [metadata.md](../docs/content/how-to/metadata.md).

| Record | Absent optional fields |
|---|---|
| `FileMetaData` | `keyValueMetadata` an empty map, `createdBy` `null`, `columnOrders` an empty list (also when the list is not decoded) |
| `ColumnChunk` | `metaData` and the page-index offsets and lengths `null`; `filePath` the empty string, never `null` |
| `ColumnMetaData` | `dictionaryPageOffset`, `statistics`, `geospatialStatistics`, `bloomFilterOffset`, `bloomFilterLength`, `sizeStatistics` `null`; `keyValueMetadata` an empty map; `encodingStats` an empty list |
| `Statistics` | `minValue`, `maxValue`, `nullCount`, `distinctCount`, `nanCount` `null`; the exactness flags `true` |
| `SizeStatistics` | each of its three fields `null` |
| `ColumnIndex` | `nullCounts`, both level histograms, `nanCounts` `null` |
| `OffsetIndex` | `unencodedByteArrayDataBytes` `null` |

Absent is distinct from present but empty throughout: an optional `long[]` is `null` when the writer omitted it and zero-length when the writer recorded it empty. Consumers rely on the distinction ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md#statistic-sources)).

Per-page and per-level data is held in primitive arrays (`ColumnIndex.nullPages` a `boolean[]`, counts and histograms `long[]`). Page filtering reads each entry once per page, and a primitive array makes that a plain load with no unboxing, with no `null` element whose meaning would need a rule. The arrays are the ones the file was read into and are not copied on the way in or out; the whole-chunk histogram accessors hand them out directly. `ColumnIndex.repetitionLevelHistogram(int)` and `definitionLevelHistogram(int)` return a copied slice of one page at stride `length / pageCount`, and raise `IllegalStateException` for a length that is not a whole number of entries per page.

`SizeStatistics`, `ColumnIndex` and `OffsetIndex` are public records, and there is no public reader entry point for the page index. `ColumnMetaData.sizeStatistics()` is reachable through the footer; the page index is parsed by `internal.thrift.ColumnIndexReader` and `OffsetIndexReader`, which `hardwood-cli` calls directly.

`ColumnChunk.filePath` places a chunk's data in another file, the split-file layout Hardwood does not read. Every offset in such a chunk addresses that other file, so reading this file at `data_page_offset` would decode unrelated bytes. The footer parse keeps the field, and `ColumnChunk.requireSameFile()` raises `UnsupportedOperationException` where the chunk's bytes would be read: before a row group's shared metadata is computed (for every chunk of the row group, since the page-index region is fetched as one span across all of them), before a bloom filter or dictionary is read for pruning, and in the CLI's page and dictionary readers. The check is not in the parse, so a split-file file's schema, row groups and statistics stay inspectable.

Tests: `SizeStatisticsMetadataTest`, `ColumnIndexTest`, `ColumnChunkTest`, `KeyValueMetadataTest`, `PageEncodingStatsMetadataTest`.

## Statistics parsing

`StatisticsReader` decodes the `Statistics` struct of a column chunk and of an inline data page header.

- **Preferred bounds.** `min_value` / `max_value` (fields 6 and 5) are preferred over the deprecated `min` / `max` (fields 2 and 1), which older writers filled in a signed byte order (PARQUET-1025). Each bound falls back to its deprecated field independently.
- **`isMinMaxDeprecated`.** Set when neither preferred field is present and at least one deprecated one is. A struct with no bounds at all (a null count alone) is not marked deprecated. What pruning reads from the flag is in [STATISTICS_PRUNING.md](STATISTICS_PRUNING.md).
- **Exactness.** `is_min_value_exact` / `is_max_value_exact` default to `true` when absent, as `parquet.thrift` specifies.
- **`nan_count`.** `null` when absent; only a recorded `0` proves a chunk holds no NaN.

Bounds are kept as the raw bytes of the plain encoding. Their interpretation (column order, `INT96`, unreadable sort orders) belongs to [STATISTICS_PRUNING.md](STATISTICS_PRUNING.md#bounds-readability); how the CLI renders them to [CLI_VALUE_RENDERING.md](CLI_VALUE_RENDERING.md).

`SizeStatisticsReader` decodes `SizeStatistics` into its three nullable fields, with no check of a histogram's length against the column's levels; `sizedFor` on the consuming side ignores a mis-sized one.

Tests: `StatisticsReaderTest`, `SizeStatisticsReaderTest`.

## Page index

The page index is two structs per column chunk: `ColumnIndex` with per-page statistics and `OffsetIndex` with per-page locations. The footer carries their offsets and lengths; the bytes are fetched per row group as one region ([FETCH_PLANNING.md](FETCH_PLANNING.md)) and parsed per column chunk when the chunk's pages are planned.

Consistency is checked at the parse boundary, so page filtering can walk every per-page array with one page index:

| Check | Where |
|---|---|
| `null_pages` is required and defines the page count; `min_values`, `max_values`, `null_counts` and `nan_counts` have exactly that many entries | `ColumnIndexReader` |
| each level histogram has a length that is a whole multiple of the page count (zero for zero pages) | `ColumnIndexReader` |
| `ColumnIndex` and `OffsetIndex` describe the same number of pages | `PageFilterEvaluator.readIndexPair`, which parses the two together |
| `PageLocation` offset, size and first row index are non-negative | `PageLocationReader` |
| the `OffsetIndex` of a chunk with values lists at least one page | `OffsetIndexReader` |

A failure is a `ParquetReadException`. `readIndexPair` prefixes it with the chunk's column ordinal ("Failed to parse the page index of column N: …"), and the read pipeline adds the file, row group and column on the way out. The column ordinal stays in the message because a chunk may have no path the pipeline can name.

`RowGroupIterator` also parses a column's `OffsetIndex` on its own to build the column's indexed fetch plan ([FETCH_PLANNING.md](FETCH_PLANNING.md)); the cross-check against `ColumnIndex` runs where page filtering parses the pair. The null-page placeholders in `min_values` / `max_values` are carried as read and not decoded.

Tests: `ColumnIndexReaderTest`, `OffsetIndexReaderTest`, `MalformedMetadataValidationTest`, `PageFilterEvaluatorTest`, `ColumnIndexTest`, `EmptyOffsetIndexTest`.

## Bloom filter

A chunk's bloom filter is a `BloomFilterHeader` Thrift struct followed by `numBytes` of split-block bitset. The internal records `BloomFilterHeader` and `BloomFilter` (`internal.bloomfilter`) hold it; there is no public bloom filter API. `RowGroupBloomFilterSource` fetches it lazily on the first probe ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md#bloom-filters)).

The header and body are validated where they are parsed:

- **Header.** All four fields are required. `numBytes` is read non-negative; a field of the wrong wire type raises where it is, and a field that never arrives raises at the STOP naming every missing field. The three unions each require one variant with an empty-struct value; a variant this version does not implement (an algorithm other than `BLOCK`, a hash other than `XXHASH`, a compression other than `UNCOMPRESSED`) raises `UnsupportedOperationException`, which the pruning source treats as no filter ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md#bloom-filters)).
- **Body.** `BloomFilterReader.readBitset` requires `numBytes` to be a positive multiple of 32, the block size, and requires that many bytes to remain after the header. Either failure is a `ParquetReadException`.

Tests: `BloomFilterReaderTest`, `BloomFilterMetadataTest`, `ThriftFieldNamingTest`, `BloomFilterPushDownTest`. The body checks (block multiple, truncated bitset) are untested.

## Per-file metadata

`ParquetFileReader` owns one `FileMetadataCache` over its input files, in supplied order. The cache keeps one `CompletableFuture<PreparedFile>` per file index. A `PreparedFile` holds only state derived from that file's own footer, so none of its components is ever absent: the `InputFile`, the parsed `FileMetaData`, the `FileSchema`, the footer's row groups, and the file's `BoundsReadability` ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md#bounds-readability)).

Per-reader state is not cached: the reference schema check, `FileColumnOrdinals`, the chunk path check and the row groups one reader's filter admits belong to one `RowGroupIterator` and are computed per iterator against the cached file ([READ_PIPELINE.md](READ_PIPELINE.md#incremental-planning)). `getFileMetaData(int)` can therefore succeed for a file whose schema is incompatible with a later projection; the incompatibility is reported when a read reaches that file, since only a reader knows the touched columns ([READ_PIPELINE.md](READ_PIPELINE.md#incremental-planning)).

### Public contract

| Accessor | Behaviour |
|---|---|
| `getFileCount()` | number of inputs; no I/O; usable after close |
| `getFileMetaData()` | the first file's footer, read at open; usable after close |
| `getFileMetaData(int)` | one file's footer; index `0` returns the same instance as `getFileMetaData()`; later indices load on first access or join a load a data reader started; `IllegalStateException` after close |

Each call reads at most one footer, so the checked `IOException` it declares belongs to that one file. The user-facing description is in [metadata.md](../docs/content/how-to/metadata.md#metadata-for-multiple-files).

### Lifetime and failure

- **Seeded first file.** `openAll` reads the first footer and seeds index `0` with it, so opening a reader and inspecting that index never read the same footer twice.
- **One load per file.** Indexed metadata access and every `RowGroupIterator` go through the same cache, so no direction of access reparses a footer. `computeIfAbsent` under the lifecycle lock makes all requesters of an index observe the same future.
- **Failures stay cached.** A failed load remains in the map, so the reader does not retry it implicitly. Closing and reopening the parent reader is the retry boundary after a failure and the refresh boundary for a file changed on storage; inputs must stay unchanged while the reader is open.
- **Exception shape.** A load runs in a `Supplier`, which wraps an `IOException` in `UncheckedIOException`, and `join` wraps again in `CompletionException`. `FileMetadataCache.getFile` undoes both, so synchronous metadata access and iterator planning both see the failure prefixed with the file name: an `IOException` with the original as its cause, or a `ParquetReadException` or `UnsupportedOperationException` of the original type. A non-`IOException` runtime failure while reading a later footer is restated as a read failure with the file name, as on the first file.
- **Close.** Close marks the cache closed under the lifecycle lock, so no new load is admitted and a prefetch request becomes a no-op; it then waits for every admitted load outside the lock, ignoring their failures, and clears the futures. Close is idempotent, and a second caller waits for the first to finish. The cache never closes an input; where its close falls in the reader's is in [INPUT_FILES.md](INPUT_FILES.md#ownership-and-lifecycle).

The cache's concurrency control coordinates the speculative footer prefetch with synchronous indexed access; reader cursors themselves stay single-consumer. When a file is planned, the iterator starts loading the next file's footer ([READ_PIPELINE.md](READ_PIPELINE.md)).

A `RowGroupIterator` built directly over a list of files, outside a `ParquetFileReader`, creates and owns a cache of its own and closes it together with its files.

Tests: `FileMetadataCacheTest`, `MultiFileRowReaderTest`, `MultiFilePlanningTest`, `ParquetFileReaderFooterFailureTest`, `EncryptedFileTest`.

## Boundaries

- **Parsed metadata reuse across opens** (#837). The cache lives for one `ParquetFileReader`; a caller that opens a reader per request parses the footer on every open. No mechanism lets a caller supply a parsed footer.
- **Unknown compression codec** (#967). A codec id past `LZ4_RAW` raises from the footer parse, so a file written with a codec the format adds later has unreadable metadata as well as unreadable data.
- **Parquet Modular Encryption** (#128). Both encryption modes are refused at the footer.
- **`DataPageHeaderV2.num_nulls = -1`** (#1225). parquet-java before 1.18.0 writes it for columns with statistics disabled; the non-negative rule rejects such pages.
