# Writer support (#9)

**Status: In progress.** Tracking issue: #9.

## Context

Hardwood is a read-only Parquet library: every file in the test corpus is produced
ahead of time by `tools/simple-datagen.py` (PyArrow). The 1.0 line shipped reading;
write support is the 1.1 goal.

The read pipeline memory-maps a file of known size and fans out random-access
`readRange(offset, length)` calls across columns and row groups. Writing inverts
this: the output size is unknown until the file is finished, and the Parquet
container is laid out for forward-only production. This document describes the
end-state writer architecture and the order in which it is delivered.

## Scope

The first writer milestone (1.1) targets **flat schemas written through a columnar
batch API**:

- **Flat columns only** — `REQUIRED` and `OPTIONAL` fields. No repetition, therefore
  no Dremel shredding and no repetition levels. This is the write-side counterpart of
  the reader's `FlatRowReader` fast path and covers the majority of analytics files.
- **Columnar batch input** — the user supplies typed arrays per column, mirroring
  `ColumnReader`. A row-oriented writer and integration adapters layer on top later.
- **DataPage V1** as the written page format, for maximum reader compatibility.

Logical-type annotations (STRING, DATE, TIMESTAMP, DECIMAL, UUID, …) are in scope
for the flat milestone: a column's physical bytes are written by the primitive-type
increment, and the annotation is serialized onto the schema and converted at the API
boundary.

Explicitly out of the first milestone, sequenced as later work: nested data
(structs, lists, maps), DataPage V2, the Avro write API, the optional index
structures (OffsetIndex, ColumnIndex, Bloom filters), the optional delta and
byte-stream-split encoders, and the S3 `OutputFile` backend. Sorting-column
metadata and custom record materializers are non-goals.

## Write model

### Forward-only, footer-last

The Parquet container is written front to back and never seeked backward:

```
PAR1 | <row group 0 pages> | <row group 1 pages> | ... | FileMetaData (thrift) | <footer length: 4 bytes LE> | PAR1
```

Every offset a reader needs lives in the `FileMetaData` footer, which is emitted
last. The writer maintains a running byte position, records page and column-chunk
offsets as it streams them out, and serializes the accumulated metadata at the end.
No random access and no memory mapping are required on the write path.

### Row-group buffering bounds memory

A row group's column chunks are written contiguously, and each column chunk's
metadata (compressed and uncompressed sizes, page offsets, statistics) is only known
once its bytes have been encoded. The writer therefore **encodes and buffers a full
row group's columns in memory, then flushes them in column order**. The configured
row-group size (default 128 MiB of uncompressed data) bounds peak memory — this is
the write-side inverse of the reader's whole-file mmap. Page size (default 1 MiB)
bounds the granularity within a column chunk.

### OutputFile abstraction

A sequential write counterpart to `InputFile`, far simpler than the random-access
read interface:

```java
public interface OutputFile extends Closeable {
    void create() throws IOException;        // Acquire resources
    void write(ByteBuffer data) throws IOException;
    long position();                          // Running byte offset written so far
    void close() throws IOException;          // Finalize; file is valid only after this returns
}
```

- **Local backend** (`internal.writer.ChannelOutputFile`): a buffered `FileChannel`.
  Writes to a temporary sibling path and atomically renames on `close()`, so a failed
  write never leaves a truncated file presented as valid.
- **In-memory backend** (`internal.writer.ByteBufferOutputFile`): a growable buffer,
  the write-side counterpart to `ByteBufferInputFile`, used for tests and round-trips.
- **S3 backend** (later): sequential writes buffer to the multipart part size and
  upload parts; `close()` completes the multipart upload.

A file is valid only after `close()` returns successfully. A writer abandoned before
`close()` produces no footer and therefore no readable file.

## Component architecture

Writer components live in `core`, in packages parallel to the reader, so encoders and
the thrift codec sit alongside their decode counterparts as shared substrate.

| Layer | Package | Components |
|-------|---------|------------|
| Public API | `dev.hardwood.writer` | `ParquetFileWriter`, `ColumnBatchWriter`, `WriterConfig` |
| Public API | `dev.hardwood` | `OutputFile` |
| Public API | `dev.hardwood.schema` | `FileSchema.Builder` (produces the existing immutable `FileSchema`) |
| Orchestration | `dev.hardwood.internal.writer` | `RowGroupBuffer`, `ColumnChunkBuffer`, `PageBuilder`, `StatisticsCollector`, `OutputFile` backends |
| Value encoding | `dev.hardwood.internal.encoding` | `PlainEncoder`, `RleBitPackingHybridEncoder`, `LevelEncoder`, `DictionaryEncoder` (alongside the existing decoders) |
| Compression | `dev.hardwood.internal.compression` | `Compressor` / `CompressorFactory` (alongside `Decompressor`) |
| Metadata serialization | `dev.hardwood.internal.thrift` | `ThriftCompactWriter`, `FileMetaDataWriter`, `SchemaElementWriter`, `RowGroupWriter`, `ColumnChunkWriter`, `ColumnMetaDataWriter`, `PageHeaderWriter` (the `*Writer` inverses of the existing `*Reader`s) |

The thrift `*Writer` classes are pure struct serializers, the inverse of the
`*Reader`s and co-located with them; the `internal.writer` orchestration types
carry the distinct `*Buffer` / `*Builder` names so they do not collide.

### Schema construction

The writer reuses the immutable `FileSchema` / `SchemaNode` model unchanged — files
it writes are read back through the same model. A `FileSchema.Builder` constructs
that model programmatically and computes max definition levels. The initial builder
takes a field name, physical type and repetition; a logical-type overload (and the
`FIXED_LEN_BYTE_ARRAY` type length plus `DECIMAL` scale/precision) arrives with the
logical-type increment. The builder rejects repeated fields until nested support
lands, so unsupported schemas fail at construction rather than mid-write.

Logical types are written in two parts. The **annotation** — the `LogicalType` union
and the legacy `converted_type`/`scale`/`precision` fields on `SchemaElement` — is
serialized by a `LogicalTypeWriter` (the inverse of the existing `LogicalTypeReader`),
so a written column reads back as `STRING`/`DATE`/`DECIMAL`/etc. The **value
conversion** — accepting `String`/`LocalDate`/`Instant`/`BigDecimal` and lowering them
to physical values — is the inverse of the reader's `LogicalTypeConverter` and rides
with the row-oriented API; the columnar API takes physical values directly.

### Encoding strategy and `WriterConfig`

The writer auto-selects sensible per-column encodings and exposes overrides through
`WriterConfig`; the CLI surface stays minimal.

- **Levels**: definition levels are RLE/bit-packed via `LevelEncoder` (flat schemas
  have no repetition levels).
- **Values**: `PLAIN` is the correctness baseline. `RLE_DICTIONARY` with plain
  fallback (on dictionary-size overflow) is the default for eligible columns, matching
  the reader's dictionary fast paths. The delta and byte-stream-split encodings are
  optional and deferred to after the flat milestone.
- **Compression**: `UNCOMPRESSED` first, then `SNAPPY` / `ZSTD` / `GZIP` / `LZ4` —
  the existing codec libraries are bidirectional, so the encode side reuses them.
  Default codec is `ZSTD`.

`WriterConfig` knobs: row-group size, page size, dictionary page-size limit, codec,
and the written `created_by` string.

### Statistics

`StatisticsCollector` accumulates `min` / `max` / `null_count` per column chunk
during encoding and writes them into `ColumnMetaData`, so produced files support
reader-side predicate pushdown. `min`/`max` ordering follows the column's
`ColumnOrder` (the same ordering the reader honors on read), so written statistics
are pruning-correct. Long `BYTE_ARRAY` `min`/`max` are truncated per the format's
binary min/max truncation rule, keeping statistics bounded while remaining valid for
pruning. OffsetIndex, ColumnIndex, and Bloom filters are deferred; a file is valid
without them.

## Threading model

The first implementation is **single-threaded**. Correctness is the priority for a
writer, and a half-correct file is worthless.

The architecture leaves parallelism open without a public-API change: within a row
group, columns are encoded independently into separate byte buffers and only
concatenated in column order at flush, mirroring the reader's per-column workers. A
later increment can encode columns in parallel and pipeline row-group encoding
against the flush of the previous group. This machinery is added only once the
single-threaded writer is correct.

## Validation strategy

Every increment lands with tests that prove the produced bytes are a valid Parquet
file readable by an independent engine, so no unverified write code accumulates in
`main`. The cross-engine differential is the primary check; the read-side oracle is
established in `_designs/DIFFERENTIAL_TESTING.md` and the writer runs it in reverse.

1. **DuckDB differential (primary)**: hardwood writes a file, then DuckDB reads it
   through `read_parquet('<path>')` and the values are asserted to agree. This is the
   exact inverse of [DifferentialReadTest], which has DuckDB read fixtures hardwood
   also reads; here DuckDB is the consumer of hardwood-written bytes. Because DuckDB
   shares no code with hardwood, agreement proves the bytes are spec-correct rather
   than merely self-consistent. Each row carries a synthetic index column so the
   comparison is robust to scan order (`ORDER BY` that column). Boundary values
   (signed extremes, all-null columns once nullable columns land, empty row groups)
   are written explicitly to stress the encoders.
2. **Round-trip**: write with hardwood, read back with hardwood, assert value and
   null equality. This is the fast inner-loop check and pins reader/writer agreement
   on details DuckDB does not surface (e.g. exact encodings, statistics).
3. **Property/fuzz** (later increments): random in-scope schemas and data round-tripped
   and run through the DuckDB differential, to surface edge cases beyond the
   hand-written cases (dictionary overflow, page and row-group boundaries).

DuckDB is already a `test`-scope dependency (`org.duckdb:duckdb_jdbc`); no new tooling
is required. Reading hardwood-written files with PyArrow is used for ad-hoc
verification but is not part of the automated suite.

## Delivery plan

Each increment is a shippable PR that adds user-visible capability and lands with the
validation above, so `main` never holds functionality that cannot produce a readable
file.

The sequencing is **dimension-first**: prove the thinnest slice through each
architectural dimension — paging, the columnar API contract (nulls + streaming),
dictionary pages, compression, statistics — on a single type (`INT32`) before going
wide. Type, encoding, and codec **breadth** is the same proven mechanism repeated, so
it is deferred until every dimension is settled; otherwise a late dimension would
reshape an already-multiplied surface (adding nulls or the streaming API after N typed
methods exist rewrites all N). Each increment carries a **Kind**: *Dimension* (changes
the shape of the solution), *Breadth* (more of a proven mechanism), *Layer* (additive
API), *Optimization*, *Spike* (design-only), or *Docs* (user-facing documentation).

| # | Increment | Kind | New capability | Roadmap boxes | Completed |
|---|-----------|------|----------------|---------------|-----------|
| 1 | Tracer: flat `REQUIRED INT32`, one page / one row group, `PLAIN`, uncompressed. `OutputFile` (local + in-memory), `ThriftCompactWriter`, page-header + footer serialization. | Dimension | Produces a real, readable file | 1 (ThriftCompactWriter), 3.2 (page header ser.), 4.1/4.2 (chunk/row-group ser.), 5.2 (FileMetaData ser.) | [x] |
| 2 | Page chunking within a column chunk: a large `INT32` column written as multiple size-bounded `PLAIN` pages instead of one, replacing the single-page guard. Internal — the columnar API is unchanged. | Dimension | Large columns written safely, bounded page size | 6.2 (multi-page data writing) | [ ] |
| 3 | **Columnar API contract** (`INT32` only): nullable columns (definition levels via `LevelEncoder`) **and** multi-row-group append with size-based flushing **and** `WriterConfig`, designed together. Locks how the caller supplies nulls and feeds data. | Dimension | The public write contract is settled | 2.3, 3.3, 6.1, 6.2 | [ ] |
| 4 | Dictionary encoding (`INT32`): dictionary page + `RLE_DICTIONARY` indices + plain fallback. | Dimension | Dictionary column-chunk layout proven | 2.2 | [ ] |
| 5 | Compression on the write path (`INT32`, one codec). | Dimension | Compress step + compressed/uncompressed size accounting proven | 6.2 (page compression) | [ ] |
| 6 | Column statistics (`INT32`: `min`/`max`/`null_count`, `ColumnOrder`-correct) accumulated during encode, + CRC32. | Dimension | Produced files support pushdown | 9.1 (stats), 3.2 (CRC write) | [ ] |
| 7 | Nested design spike: confirm the columnar API contract extends to repetition levels and shredding. Design only, no implementation. | Spike | De-risks the nested milestone before breadth locks the API | 6.3 (design) | [ ] |
| 8 | All primitive physical types (incl. `FIXED_LEN_BYTE_ARRAY` type length and `BYTE_ARRAY` min/max truncation), each inheriting paging, nulls, dictionary, compression and stats. | Breadth | Write any flat column type | 2.1, 9.1 (truncation) | [ ] |
| 9 | Logical-type annotations: `LogicalTypeWriter` serializes the `LogicalType` union and legacy `converted_type`/`scale`/`precision`; `FileSchema.Builder` logical-type overload. | Breadth | Columns read back with their logical type (STRING, DATE, TIMESTAMP, DECIMAL, …) | 6.4 (annotation) | [ ] |
| 10 | Remaining codecs + optional delta and byte-stream-split encoders. | Breadth | Full codec / encoding choice | 2.4, 2.5 | [ ] |
| 11 | Parallel column encoding + row-group pipelining. | Optimization | Write throughput | — | [ ] |
| 12 | Row-oriented `ParquetWriter` ergonomic layer on the columnar core, including logical-type value conversion (inverse of `LogicalTypeConverter`). | Layer | Mainstream-friendly API | 6.1 (`ParquetWriter`), 6.4 (value conversion) | [ ] |
| 13 | User-facing documentation under `docs/content/` for the writer public API (`OutputFile`, `ParquetFileWriter`, `FileSchema.Builder`): a `how-to` guide and a `reference` page, covering the settled surface including the row-oriented layer. | Docs | Documented, stable public API | — | [ ] |

Increments 1–6 prove every architectural dimension on `INT32`; 7 is a design checkpoint;
8–12 are breadth and layers that build on the settled shape; 13 documents the finished
public surface. Together they constitute the flat-writer milestone (1.1). Later milestones, each their own design and sequence: nested
shredding implementation (Dremel: structs → lists → maps), DataPage V2, the optional index
structures and Bloom filters, the Avro write adapter, a CLI write/convert command, and the
S3 `OutputFile` backend.

## User documentation

User-facing documentation under `docs/content/` is delivered as the closing increment
of the milestone (stage 13), once the full public surface — including the row-oriented
layer — is settled. Documenting the provisional `INT32`-only surface earlier would be
throwaway, so deferring is a recorded, intentional exception to the CLAUDE.md rule that
a new public API ships with a docs update — not an oversight. The public surface
(`OutputFile`, `ParquetFileWriter`, `FileSchema.Builder`) gets its `how-to`/`reference`
pages at stage 13.

## Roadmap reconciliation

`ROADMAP.md` remains the fine-grained capability inventory; this document is the plan
of record for writer architecture and sequencing. The mapping column above ties each
increment to the roadmap boxes it completes, so those boxes are ticked as increments
land. Phase 6 of `ROADMAP.md` points here.
