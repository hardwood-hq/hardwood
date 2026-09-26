# Writer

How the writer turns caller data into a Parquet file: the forward-only write model, the `OutputFile` sink and its publish/discard contract, the writer's lifecycle on failure, the components and where they live, how row groups are accumulated, sized and flushed, what bounds the writer's memory, what the footer carries, and the threading model. What the caller hands the writer (the schema builder, the `ColumnBatch` contract, shredding, annotation ranges, the `RowWriter` layer) is in [WRITER_INPUT.md](WRITER_INPUT.md); how a buffered column chunk becomes pages (page layout and cuts, the `AUTO` dictionary decision and its probes, named encoding policies, codecs, statistics) is in [WRITER_ENCODING.md](WRITER_ENCODING.md); how produced files are checked against independent readers is in [WRITER_VALIDATION.md](WRITER_VALIDATION.md). The user-facing account of the same model is [write-model.md](../docs/content/concepts/write-model.md), and the options and rejections are listed in [writer.md](../docs/content/reference/writer.md).

## Write model

### Forward-only, footer-last

The Parquet container is written front to back and never seeked backward:

```
PAR1 | <row group 0 pages> | <row group 1 pages> | ... | FileMetaData (thrift) | <footer length: 4 bytes LE> | PAR1
```

Every offset a reader needs lives in the `FileMetaData` footer, which is emitted last. The writer takes the running byte position from `OutputFile.position()`, records page and column-chunk offsets as it streams them out, and serializes the accumulated metadata at the end. No random access and no memory mapping are required on the write path, and the output size is not known until the file is finished. This is the inverse of the read side, which maps a file of known size and fans out random-access reads ([INPUT_FILES.md](INPUT_FILES.md)).

### Row-group buffering

A row group's column chunks are written contiguously, and each column chunk's metadata (compressed and uncompressed sizes, page offsets, statistics, encodings) is only known once its bytes have been encoded. The writer therefore **buffers a full row group's columns in memory, then encodes and writes them in schema order**. A file of any size is a sequence of row groups, each buffered, flushed and forgotten, so what the writer holds follows the row-group targets rather than the size of the file ([Memory](#memory)).

Three layout tiers stack here: a **page** holds a slice of one column, a **column chunk** is one column's pages for one row group, and a **row group** holds one column chunk per leaf column. All three are internal; only the page target and the two row-group targets are user-visible, as `WriterConfig` options.

Buffering the whole row group before encoding is what lets each column chunk's encoding be chosen from everything the chunk holds, and lets a page be cut on the bytes it encodes to ([WRITER_ENCODING.md](WRITER_ENCODING.md)). Both depend on the chunk being complete before its first page is produced.

### Arrival units and layout units

Data arrives as `ColumnBatch` slices through `ColumnWriter.writeBatch`, or as records through `RowWriter.writeRow`, which stages records into batches and submits them through the same path ([WRITER_INPUT.md](WRITER_INPUT.md)). A batch is only an *arrival* unit and is independent of the three layout tiers: the writer shreds a batch's records into per-column buffers, cuts the row group when a target is reached, and cuts pages when the row group is written out. A batch larger than what the open row group can take is split between row groups at a record boundary, and a row group may be filled from many batches.

Row-group boundaries are chosen by the writer from the targets; there is no explicit boundary method. A caller holding whole columns submits them as one large batch and the writer slices it into row groups; a streaming producer submits many small batches and discards each after handing it over. Both produce the same file for the same data, and the row layer produces the file the columnar layer does, byte for byte.

A file writer serves one API or the other: `columnWriter()` and `rowWriter()` latch the file to the first one used, so two independent staging states never interleave into one row group.

### Nulls and nesting

Every leaf column is written as the Dremel model describes it: a repetition-level stream (for a column under a repeated field), a definition-level stream (for a column with any optional or repeated ancestor), and the present values only. `REQUIRED` flat columns have no level stream. The caller never supplies levels: it supplies per-column `Validity` and per-layer validity and offsets, and `RecordShredder` computes the levels ([WRITER_INPUT.md](WRITER_INPUT.md)). The shredder emits `(repetition level, definition level, value index)` per leaf slot and never reads a value, so it is written once for every physical type, and a flat leaf takes the same append path as a nested one.

Levels are retained one unsigned byte per entry until flush, which bounds the writer to a maximum definition or repetition level of 255 (`LevelEncoder.MAX_STORABLE_LEVEL`); a deeper schema is rejected when the writer is created.

Tests: `WriterLayoutTest`, `RowWriterEquivalenceTest`, `WriterNestedRoundTripTest`.

## `OutputFile`

`dev.hardwood.OutputFile` is the sequential write counterpart to `InputFile`, and is public so that users can implement it for their own destinations.

| Method | Contract |
|---|---|
| `create()` | Acquires the destination's resources. Must be called before `write` or `position`; the writer calls it. |
| `write(ByteBuffer)` | Appends the buffer's remaining bytes and consumes it (position advances to limit). |
| `position()` | The number of bytes written so far, which is the offset of the next `write`. |
| `close()` | Finalizes (publishes) the file. The file is valid at the destination only after this returns. A `close()` that fails before publishing releases its resources and discards what was written, then throws. |
| `discard()` | Throws everything written away and releases resources without publishing; the destination is left as if nothing was written, and a later `close()` does nothing. |

`close()` and `discard()` are the commit and the rollback of one output. The writer calls exactly one of them per output, so an implementation can hold its output out of sight until `close()` (a temporary file, an uncompleted multipart upload) and throw it away on `discard()`.

| Backend | Where | Behaviour |
|---|---|---|
| Local file | `internal.writer.ChannelOutputFile`, from `OutputFile.of(Path)` | Streams to a temporary sibling (`<name>.hardwood-tmp`) through a coalescing buffer, and atomically renames it onto the target on `close()`; `discard()`, or a `close()` whose final flush or rename fails, deletes the sibling. A reader never observes a half-written file at the target path. |
| In memory | `internal.writer.ByteBufferOutputFile` | A growable buffer, the write-side counterpart to `ByteBufferInputFile`; its bytes are available after `close()`. Internal, used by tests and benchmarks. |

There is no object-store backend; `S3_STORAGE.md` covers read access only.

### Writer lifecycle and failure

`ParquetFileWriter` is `OPEN`, `FAILED` or `CLOSED`.

- **Creation.** Everything the schema and configuration decide is validated before the output is touched: the columns' physical types, the schema's shape (`WriterSchemaShape`), each column's encoding policy against its type, then the codec and its library. A file the writer cannot honour is therefore never begun. Only then does the writer call `create()` and write the leading magic; a failure between those two points discards the output before the exception propagates.
- **Failure.** Any exception out of `writeBatch` or `writeRow` (a rejected batch or record, an exception from the filler, a destination `IOException`, a codec `ParquetWriteException`) fails the writer: later writes are rejected with `IllegalStateException`, and `close()` calls `OutputFile.discard()` instead of writing the footer. `close()` cannot tell whether an exception is propagating out of the try-with-resources block around it, so a writer that stayed usable after a rejection would publish the rows written before it whenever the rejection is not caught.
- **Abort.** A failure raised outside the writer, such as by the caller's data source, is invisible to it; `abort()` discards the output for that case, and a later `close()` does nothing. `abort()` on a closed writer does nothing, so a published file stays published.
- **Close.** `close()` publishes a writer that has not failed: it flushes the row layer's staged records, flushes the open row group, writes the footer, and calls `OutputFile.close()`. A failure while flushing or writing the footer discards the output and is rethrown. A second `close()` does nothing.
- **Publish failure.** A failure inside `OutputFile.close()` is the backend's to clean up, by the `close()` contract above; the writer is already closed and does not also call `discard()`.
- **Discard failure.** A discard that fails is thrown (from `close()` or `abort()`), or attached as suppressed to the exception that caused it, since it leaves the temporary file behind.

`keyValueMetadata` and `createdBy` stay callable on a failed writer until `close()`; they change nothing, since no footer is written. How the exception types divide between caller error, destination and writer is in [EXCEPTION_MODEL.md](EXCEPTION_MODEL.md); the user-facing account is [write-failures.md](../docs/content/how-to/write-failures.md).

Tests: `WriterFailureTest`, `WriterCodecFailureTest`, `ChannelOutputFileTest`, `WriterSchemaShapeTest`.

## Components

Writer components live in `core`, in packages parallel to the reader, so encoders, codecs and the Thrift serializers sit beside their decode counterparts as shared substrate.

| Layer | Package | Components |
|---|---|---|
| Public API | `dev.hardwood.writer` | `ParquetFileWriter` (lifecycle, footer), `ColumnWriter` / `ColumnBatch` (columnar), `RowWriter` / `StructBuilder` / `ListBuilder` / `MapBuilder` (row layer), `WriterConfig`, `ColumnEncoding`, `PrecisionLossPolicy`, `ParquetWriteException` |
| Public API | `dev.hardwood` | `OutputFile`, `Validity` (shared with the reader) |
| Public API | `dev.hardwood.schema` | `FileSchema.Builder`, producing the reader's immutable `FileSchema` |
| Orchestration | `dev.hardwood.internal.writer` | `RowGroupBuffer` (one row group), `ColumnChunkBuffer` (one column chunk: levels, indices, page cuts, framing), `RecordShredder` (levels from validity and offsets), `ValueEncoder` and its per-type subclasses (value store, dictionary, statistics), `ColumnSource` and its per-type sources (the value-input seam), the per-type statistics collectors, `RowPlan` and its nodes (row-layer staging), `WriterSchemaShape`, `LogicalTypeValueRange`, the `OutputFile` backends |
| Value encoding | `dev.hardwood.internal.encoding` | `PlainEncoder`, `RleBitPackingHybridEncoder`, `LevelEncoder`, `DictionaryEncoder` / `LongDictionaryEncoder` / `BinaryDictionaryEncoder`, the delta and byte-stream-split encoders |
| Compression | `dev.hardwood.internal.compression` | `Compressor` / `CompressorFactory`, beside `Decompressor` |
| Metadata serialization | `dev.hardwood.internal.thrift` | `ThriftCompactWriter` and the `*Writer` struct serializers (`FileMetaDataWriter`, `SchemaElementWriter`, `LogicalTypeWriter`, `RowGroupWriter`, `ColumnChunkWriter`, `ColumnMetaDataWriter`, `StatisticsWriter`, `PageHeaderWriter`, `KeyValueMetadataWriter`, …), the inverses of the `*Reader`s |

The Thrift `*Writer` classes are pure struct serializers co-located with their readers; the orchestration types carry `*Buffer` names so they do not collide with them.

`ColumnChunkBuffer` owns the type-agnostic half of a chunk (level streams, page cuts, the dictionary index stream, compression, CRC, framing); a per-type `ValueEncoder` owns the type-specific half (reading the typed value through its `ColumnSource`, the value store, the dictionary, the value section's encoding, statistics). The shredder sees only source positions, so every type-specific line lives in one `ValueEncoder` subclass. A `ColumnSource` holds the caller's arrays by reference; every present value is copied out of it into the chunk during `writeBatch`, so the caller's arrays are free when `writeBatch` returns.

## Row-group lifecycle and sizing

### Two phases

A column chunk is accumulated while records arrive and encoded when the row group is flushed.

1. **Accumulate.** `ParquetFileWriter.writeStagedBatch` binds the shredder to the batch and hands it to `RowGroupBuffer.append`, which appends it a slice of records at a time and calls back into the writer to flush whenever a row group is cut. Per leaf slot, the chunk retains a byte for each level stream the column has, and a present value is either interned against the chunk's dictionary (an `int` index is kept) or copied into the value store, with the statistics extended either way. Nothing is encoded and no page is cut.
2. **Flush.** When a target is reached, `RowGroupBuffer.flushTo` writes each column chunk in schema order, recording the offset where it starts. Each chunk settles its encoding, writes its dictionary page if it has one, then cuts, encodes, compresses and writes its data pages one at a time straight to the `OutputFile` ([WRITER_ENCODING.md](WRITER_ENCODING.md)). Encoded pages are not buffered: the chunk's encoding is settled before its first page is produced, and all chunk metadata is carried in the footer, so nothing written has to be revisited.

One `RowGroupBuffer` serves every row group of a file. `reset()` empties it by resetting counts and keeps every store at the capacity it reached, so the writer's largest allocation is made once per file rather than once per row group. The dictionary and statistics describe one chunk and start over at each reset.

### Two targets and two structural caps

A row group is cut when either target is reached, whichever comes first:

| Control | Answers | Measure |
|---|---|---|
| `rowGroupTargetRows` | Layout: how the file is banded. A row group is self-contained, so it is the unit a file is split on across readers and the unit column-chunk statistics prune. | Records. Never exceeded; where it is the target that cuts, a row group holds exactly this many. |
| `rowGroupBufferTargetBytes` | Memory: how much the writer holds for the open row group. | Retained bytes, below. A row group passes it by at most one record. |
| `RowGroupBuffer.MAX_ROWS` | Structural: a chunk accumulates into `int`-indexed stores, so a row group cannot hold more records than those can index. | Records. Not configurable; reached only by targets set above it. A row target above it is clamped to it, which makes `Long.MAX_VALUE` the way to cut on bytes alone. |
| `RowGroupBuffer.MAX_STORE_CAPACITY` | Structural: no column chunk's `int`-indexed store may overflow. | A chunk's entries, values and nulls (which bound its level, value and index stores), and its packed binary content, counted as though every present value were stored. Not configurable. `MAX_ROWS` is one below it, so for a flat column the row ceiling and the entry cap fall on the same record; the store cap cuts earlier only for a repeated column's entries or for binary content, and a byte target larger than a chunk can hold yields more row groups. |

The row target binds for narrow records and the byte target for wide ones, so a narrow schema is banded by record count and a wide one by what it holds. The row count does not govern a single reader's parallelism, which runs over pages and columns within a row group ([READ_PIPELINE.md](READ_PIPELINE.md)).

Neither target is the size a row group takes on disk. When a row group is cut, nothing has been compressed and no chunk's encoding has been chosen, and a codec's ratio on unseen data cannot be derived. A produced-size target would require encoding and compressing each page as it is cut, which settles a chunk's encoding before the row group has been seen and gives up the flush-time decision, the uniform per-chunk encoding and the exact `distinct_count` ([WRITER_ENCODING.md](WRITER_ENCODING.md)). A caller who needs a particular on-disk size measures one file and scales the setting.

### The memory measure: retained bytes

`rowGroupBufferTargetBytes` is compared against `RowGroupBuffer.retainedBytes()`, the sum over column chunks of what each holds, read from lengths every buffer already tracks:

| Term | Value |
|---|---|
| Repetition levels | one byte per entry |
| Definition levels | one byte per entry |
| Dictionary indices | 4 bytes per interned value |
| Value store and dictionary | `ValueEncoder.retainedBytes()`: the stored values at their `PLAIN` width (a bit per `BOOLEAN`, packed content plus a 4-byte offset per `BYTE_ARRAY` value), plus the dictionary's entries and hash table where the chunk still has one |

The sum is O(columns) and is read once per appended slice, not accumulated per value. It charges content, not capacity: the stores keep their capacity across row groups, and charging capacity would report a freshly reset row group as already full.

A dictionary counts against the target like everything else the writer holds, so it needs no size limit of its own. The target bounds memory without deciding an encoding: a column whose dictionary is large but pays for itself keeps it and reaches the target sooner. Bounding the *work* of interning a column that will not win a dictionary is the job of the size probes ([WRITER_ENCODING.md](WRITER_ENCODING.md)).

### The cut: size a slice by what it could cost, cut on what it did

What an appended range retains is knowable only after appending it: a value interned against a live dictionary retains an index where it repeats and an index plus a new dictionary entry where it does not, which takes the hash to know. Appending a fixed number of records and then checking would let a batch whose records widen part way through carry a row group far past its target. So each slice is sized by what it *could* cost, and the row group is cut on what it turned out to hold:

```java
// RowGroupBuffer.append
while (pos < rows) {
    int slice = sliceThatFits(shredder, sources, pos,
            Math.min(Math.min(rows - pos, SLICE_RECORDS), targetRows - rowCount),
            targetBytes - retained);
    if (slice == 0) {            // a structural cap: not even one more record fits
        retained = cut(flush);
        continue;
    }
    appendRecords(shredder, sources, pos, slice);
    pos += slice;
    retained = retainedBytes();
    if (retained >= targetBytes || rowCount >= targetRows) {
        retained = cut(flush);
    }
}
```

`sliceThatFits` halves down from a whole slice until the slice's upper bound fits the room left, stopping at one record. The bound charges every leaf slot a present value *and* a new dictionary entry (`ValueEncoder.maxRetainedBytesPerValue()`, a constant per type except `BYTE_ARRAY`, whose value lengths are read from the batch), and every layer that can stand in for absent content an entry it may not emit. Leaf counts come from `RecordShredder.leafRange`, which composes the cumulative offsets rather than walking the records, so the bound costs O(columns × layers) plus a read of the incoming lengths for `BYTE_ARRAY`. The bound being conservative shortens the last slices of a row group, never the row group, because the cut is made on the measurement.

**Invariants.** A row group passes its byte target by at most what one record retains: the slice halves to a single record as the room runs out, and a record goes in whole because a record cannot be split across row groups. The row target is exact, the slice being clamped to what remains of it. A flush that leaves records buffered fails the append rather than looping. The structural caps sit beneath both: `sliceThatFits` also halves a slice that could take any chunk's stores past `MAX_STORE_CAPACITY`, and when not even one record fits, the row group is flushed and the record goes into the next. Only a record that exceeds a store on its own is rejected, with an `IllegalArgumentException` naming the column.

Tests: `WriterLayoutTest`, `WriterSizingMatrixTest`, `WriterLargeFileTest`, `WriterConfigTest`, `RowGroupBufferStoreCapacityTest`.

## Memory

A writer's heap is dominated by the open row group, and `retainedBytes()` is the writer's account of it; `rowGroupBufferTargetBytes` bounds that account, so peak heap follows the target rather than a multiple of it. What sits above the account, none of which grows with how much is written:

- **Growth headroom.** Stores hold more than they are charged for while they grow: value and index stores grow by half again, and level streams, packed binary content and dictionary arrays double. Capacity reached in one row group is kept for the next, so the resident set is the file's high-water capacity.
- **A dictionary given up.** A chunk that gives up its dictionary resolves its indices into the value store and empties the dictionary's entries, which stops charging for them; the dictionary's arrays and table keep the capacity they reached, for reuse by the next row group.
- **A per-column floor.** Each column's stores start at an equal share of the target (`RowGroupBuffer` divides it by the column count), clamped between a floor and a ceiling in values (`ValueEncoder.startingCapacity`). A schema with enough columns that each share falls below the floor opens at a multiple of the target before a record arrives.
- **Read windows.** Each `ValueEncoder` reads its source through a fixed typed window, filled in bulk, so reading a value is not a virtual call into `ColumnSource` per value.
- **Row-layer staging.** `RowWriter` stages records into a batch before submitting it, until a fixed record count or until the staged variable-width payload reaches `rowGroupBufferTargetBytes`. That staging is held beside the open row group.
- **The caller's batch.** A `ColumnBatch` references the caller's arrays for the duration of `writeBatch`; the writer copies what it keeps.

Where the row target cuts first, peak heap is the row count times what a record retains: a byte per level stream per entry, 4 bytes of index per present value while its chunk is interning (plus a dictionary entry if the value is new), or the value's width once the chunk has stopped interning. A column whose values are narrower than its levels (a flat `OPTIONAL` `BOOLEAN`, one bit of value against one byte of level) is the case where the level store dominates. The user-facing sizing guide is [write-model.md](../docs/content/concepts/write-model.md#what-bounds-memory).

`ParquetFileWriter.retainedBytes()` and `peakRetainedBytes()` are package-private and exist for the tests that hold the account against a heap measurement, which is the only check that the account means what it says.

Tests: `WriterRetentionTest`, `WriterSizingMatrixTest`.

## Footer

`close()` serializes one `FileMetaData` through `FileMetaDataWriter`, then its 4-byte little-endian length and the trailing `PAR1`.

| Field | Written |
|---|---|
| `version` | `1` |
| `schema` | `FileSchema.toSchemaElements()`, with the logical-type annotations `LogicalTypeWriter` serializes ([WRITER_INPUT.md](WRITER_INPUT.md)) |
| `num_rows` | the sum over row groups |
| `row_groups` | one per flushed row group, each column chunk's `ColumnMetaData` as its buffer produced it ([WRITER_ENCODING.md](WRITER_ENCODING.md)); no offset or column index, no Bloom filter |
| `key_value_metadata` | the entries set on the writer, in insertion order; omitted when there are none |
| `created_by` | `ParquetFileWriter.DEFAULT_CREATED_BY` unless replaced |
| `column_orders` | `TYPE_DEFINED_ORDER` for every leaf column, the order every statistics collector computes in; the format requires the list wherever bounds are written |

An empty row group is never written: `flushRowGroup()` does nothing when the buffer holds no records.

### File-scope fields live on the writer

Key-value metadata and `created_by` are set on `ParquetFileWriter`, not in `WriterConfig`, for two reasons. `WriterConfig` states what governs the row groups and pages that follow, while the footer states its fields once for the whole file. And key-value metadata is worth setting after data has been written, for a value the caller knows only then (a row count, a digest over what was produced), which a value object passed at `create()` cannot express. Both setters are callable until `close()`.

On the wire the field is `list<KeyValue>` with an optional `value`. The reader collapses it into a `LinkedHashMap`, last key winning and an absent value reading as `null`, so a `Map<String, String>` input reaches exactly the shape a reader observes, and a `null` value writes a `KeyValue` without field 2. The map a reader returns can therefore be passed straight back to reproduce a file's application metadata ([writer.md](../docs/content/reference/writer.md#key-value-metadata)).

### `created_by`

The default identifier follows the `<app> version <version> (build <hash>)` convention Parquet readers parse, for example `hardwood version 1.1.0 (build a093aab)`, with a `-dirty` suffix on the hash when the working tree was not clean at build time. The version and hash come from `dev.hardwood.internal.BuildInfo` ([BUILD_INFRASTRUCTURE.md](BUILD_INFRASTRUCTURE.md)); a build that cannot determine either reports `unknown` in its place, which stays parseable.

The convention is not cosmetic. A reader that cannot parse this field cannot tell which implementation wrote the file, and applies its writer-specific correctness workarounds by default: parquet-java's PARQUET-251 heuristic discards the deprecated `min` / `max` of a `BINARY` or `FIXED_LEN_BYTE_ARRAY` column written by an unidentifiable writer. Hardwood writes only the modern `min_value` / `max_value`, which that heuristic does not gate, so a parseable identifier is what keeps the outcome from depending on which statistics fields the writer happens to emit.

Tests: `WriterFooterMetadataTest`, `WriterFooterMetadataInteropTest` (parquet-testing-runner), `WriterInteropTest` (parquet-testing-runner).

## Threading

The writer is single-threaded. Every call runs on the caller's thread: shredding, interning, encoding, compression and output all happen inside `writeBatch`, `writeRow` and `close()`. The writer uses no `HardwoodContext` and starts no threads. A `ParquetFileWriter` and the views it hands out have no synchronization and are confined to one thread at a time. Untested.

The architecture leaves parallelism open without a public-API change. Within a row group the column chunks are independent after accumulation: each chunk's encoding is settled at flush, no adaptive state is shared between chunks, and chunks are concatenated in schema order only as they are written. Parallel encode would take the chunks of one row group as its work units, keeping peak memory at one row group's retention plus the encoded chunks held to preserve schema order; pipelining whole row groups would multiply retention by the number in flight.

## Boundaries

- The only public `OutputFile` factory is `OutputFile.of(Path)`; an in-memory destination exists only as the internal `ByteBufferOutputFile` (#1147).
- No object-store `OutputFile` backend, and no parallel column encoding or row-group pipelining (#1291).
- Row groups are cut only by the two targets and the structural caps; a caller cannot end a row group at a boundary of its own (#985).
- The row layer costs measurably more than the columnar one for the same file, in time and in staging allocation (#1045).
- A record rejected by `RowWriter` fails the writer; there is no way to skip it and continue (#1253).
