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

The writer milestone (#9) delivers write support for the **full schema model the reader
supports — flat and nested — through a columnar batch API**:

- **All column shapes** — `REQUIRED`, `OPTIONAL`, and `REPEATED` fields, and nested
  groups (structs, lists, maps): definition levels, repetition levels, and Dremel
  shredding, mirroring the reader's nested model. Flat columns (`REQUIRED` / `OPTIONAL`,
  no repetition) — the write-side counterpart of the reader's `FlatRowReader` fast path,
  covering the majority of analytics files — are delivered first as the thinnest slice,
  and nesting is built on that settled contract.
- **Columnar batch input** — the user fills a `ColumnBatch` the writer hands to a
  filler: an aligned slice carrying one typed array per column, addressed by index or
  name, mirroring `ColumnReader`. Nested columns carry per-layer validity and offsets,
  the write-side analog of the reader's `getLayerValidity` / `getLayerOffsets`. The
  writer re-chunks batches into pages and row groups internally. A row-oriented writer
  and integration adapters layer on top later.
- **DataPage V1** as the written page format, for maximum reader compatibility.

Logical-type annotations (STRING, DATE, TIMESTAMP, DECIMAL, UUID, …) are in scope: a
column's physical bytes are written by the primitive-type increment, and the annotation
is serialized onto the schema and converted at the API boundary.

Sequenced as later work, each its own design: DataPage V2, the Avro write API, the
optional index structures (OffsetIndex, ColumnIndex, Bloom filters), the optional delta
and byte-stream-split encoders, a CLI write/convert command, and the S3 `OutputFile`
backend. Sorting-column metadata and custom record materializers are non-goals.

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

Three nested layout tiers stack here — a **page** encodes/compresses a slice of a
column, a **column chunk** is one column's pages for one row group, and a **row group**
holds one column chunk per column. All three are internal; only the page-size and
row-group-size targets are user-visible, as `WriterConfig` knobs.

### Ingestion cadence

Data arrives as **`ColumnBatch` objects** — an aligned slice carrying one typed array
per column. `ParquetFileWriter.writeBatch` takes a filler: it creates the batch bound to
the schema, passes it to the filler to be populated, then submits it, so there is no
separate build or submit step to forget. Because the batch is schema-bound, its columns
can be addressed by index or name and every identifier is validated as values are added:
an unknown name, an out-of-range index, a non-`INT32` column, or setting the same column
twice (by either index or name) all fail eagerly rather than at write time. A batch is
atomic: every column's array must have the same length, which is the batch's row count,
and a ragged batch is rejected. The batch is only an *arrival* unit and is independent of the three
layout tiers: the writer distributes a batch's values into the per-column page buffers,
cuts pages at the page-size target, and appends encoded pages to the per-column column
chunk buffers. When the buffered row group crosses the row-group-size target it is
flushed — column chunks written in schema order, offsets recorded — and the buffers
reset. A batch larger than the row-group target is split at the boundary, so peak
memory stays bounded by the row-group size regardless of batch size.

Row-group boundaries are chosen by the writer from the size target; there is no
explicit boundary method. A caller holding whole columns submits them as one large
batch and the writer slices it into pages and row groups; a streaming producer submits
many small batches and discards each after handing it over.

### Null representation

An `OPTIONAL` column carries its nulls as a `Validity` — the same type the reader returns
from `getLeafValidity()`, promoted to the neutral `dev.hardwood` package so it is shared
read/write vocabulary rather than reader-owned. Because `Validity` is an interface, the
representation is chosen by the factory the caller uses: `NO_NULLS` (a singleton, no
allocation), `of(long[])` for a packed present-bitmap, `ofNulls(boolean[])` to bridge a
plain mask, and, in a later increment, a sparse form for the few-nulls case. The writer
consumes whichever it is given the same way — `nextNull(from, end)` walks the null
positions per page — so a new representation is a drop-in with no change to the write API.
The polarity is null-centric (`Validity.isNull`), matching the reader, so a value read
back as null is written by marking that row null.

The values array is full length — one slot per row — and the entry at a null row is
ignored. The common all-present case needs no `Validity` at all: the mask-less setter is
the all-present form for both `REQUIRED` and `OPTIONAL` columns. A `boolean[]` overload is
kept as convenience sugar over `Validity.ofNulls` (`nulls[i] == true` ⇒ null); it is the
only null form whose length is validated against the values, since a `Validity` has no
intrinsic length. A null mask is rejected on a `REQUIRED` column. `Validity` remains
`@Experimental` — its shape may still shift — so the writer overload that takes it is
experimental too until the concept is stabilized alongside the zero-copy `ColumnVector`
SPI.

The mask is lowered to definition levels at page seal: a flat `OPTIONAL` column has
`maxDefinitionLevel == 1`, so each row's level is `1` when present and `0` when null. The
levels are RLE/bit-packed (bit width 1) by `LevelEncoder` over the shared
`RleBitPackingHybridEncoder`, and only the non-null values are `PLAIN`-encoded. A DataPage
V1 body is therefore `[4-byte LE def-level length][RLE def-levels][PLAIN non-null values]`,
with the page header's `num_values` counting all rows including nulls — the exact layout
the reader parses. An all-present optional column encodes its levels as a single RLE run,
which is what lets the reader take its all-present fast path. A `REQUIRED` column has no
level stream and writes its values directly.

Internally a batch's per-column arrays sit behind a bulk **value-source seam**
(`IntColumnSource` and its per-type siblings: a `size()` plus a `copyInto` that fills a
reused page-sized primitive buffer), with an `int[]`-backed implementation. Encoders,
statistics and dictionary building consume page-sized ranges through this seam rather
than the caller's array directly, so intermediate memory stays bounded to one page. A
public `ColumnVector` / `IntColumn` SPI over the same seam — letting a caller write from
a custom columnar container without an intervening copy, plus a zero-copy fast path when
a caller's buffer already holds contiguous little-endian `PLAIN` bytes — is a later
additive layer, sequenced after nulls and dictionary settle the value and validity
facets it must expose. The primitive-array setters are sugar over the seam, so adding
the SPI never changes the `writeBatch` signature or the row-group machinery.

### Nested representation

`REPEATED` fields and nested groups shred into the same page layout as flat columns,
with a repetition-level stream ahead of the definition levels — a DataPage V1 body of
`[rep levels][def levels][values]`, both level streams RLE/bit-packed via `LevelEncoder`.
Rep and def levels are computed from the batch's per-layer validity and offsets (the
write-side inverse of the reader's Dremel assembly) by a `RecordShredder`. The detailed
shredding algorithm and the nested `ColumnBatch` input contract are settled in
`_designs/WRITER_NESTED.md` (delivery stage 5) and implemented structs → lists → maps in
stages 6–8.

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
| Public API | `dev.hardwood.writer` | `ParquetFileWriter`, `ColumnBatch`, `WriterConfig` |
| Public API | `dev.hardwood` | `OutputFile` |
| Public API | `dev.hardwood.schema` | `FileSchema.Builder` (produces the existing immutable `FileSchema`) |
| Orchestration | `dev.hardwood.internal.writer` | `RowGroupBuffer`, `ColumnChunkBuffer`, `PageBuilder`, `RecordShredder` (rep/def-level computation for nested columns), `IntColumnSource` (value-source seam), `StatisticsCollector`, `OutputFile` backends |
| Value encoding | `dev.hardwood.internal.encoding` | `PlainEncoder`, `RleBitPackingHybridEncoder`, `LevelEncoder`, `DictionaryEncoder` (alongside the existing decoders) |
| Compression | `dev.hardwood.internal.compression` | `Compressor` / `CompressorFactory` (alongside `Decompressor`) |
| Metadata serialization | `dev.hardwood.internal.thrift` | `ThriftCompactWriter`, `FileMetaDataWriter`, `SchemaElementWriter`, `RowGroupWriter`, `ColumnChunkWriter`, `ColumnMetaDataWriter`, `PageHeaderWriter` (the `*Writer` inverses of the existing `*Reader`s) |

The thrift `*Writer` classes are pure struct serializers, the inverse of the
`*Reader`s and co-located with them; the `internal.writer` orchestration types
carry the distinct `*Buffer` / `*Builder` names so they do not collide.

### Page construction

Assembling a data page — lowering definition levels, `PLAIN`- or dictionary-encoding
the values, framing the header (`num_values`, sizes, CRC) and, later, compressing the
body — is a single seam, `PageBuilder`. It is a **per-seal operation over already-filled
buffers, not a stateful object that owns them**: the value, null, and level arrays stay
in `ColumnChunkBuffer`, primitive and type-specialized, so appends remain bulk
`copyInto` copies rather than per-value calls and each physical type keeps its own
primitive buffer without a generic boxed page. This keeps two independent axes apart —
per-type *buffer management* in the chunk buffer, per-encoding *body assembly* in
`PageBuilder`.

Through stage 6 the only page shape is `[def levels?][PLAIN values]` — struct
shredding only deepens the definition levels, it adds no body variant — so
`ColumnChunkBuffer` assembles it inline (its `sealPage`). `PageBuilder` is introduced at
**stage 7**, when list shredding prepends the repetition-level stream
(`[rep levels][def levels][values]`); `RLE_DICTIONARY` bodies (stage 9) and compressed
bodies (stage 10) then add further body and framing variants that would otherwise
accrete as branches inside the chunk buffer. Extracting the seam at the first variant,
rather than earlier, lets it take its shape from the variants it must actually span
instead of from the single `PLAIN` case.

### Schema construction

The writer reuses the immutable `FileSchema` / `SchemaNode` model unchanged — files
it writes are read back through the same model. A `FileSchema.Builder` constructs
that model programmatically and computes max definition and repetition levels. It takes
both flat fields (name, physical type, repetition) and nested groups (structs, lists,
maps); a logical-type overload (and the `FIXED_LEN_BYTE_ARRAY` type length plus `DECIMAL`
scale/precision) arrives with the logical-type increment. A schema the writer cannot yet
produce is rejected at construction rather than mid-write.

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

- **Levels**: definition and repetition levels are RLE/bit-packed via `LevelEncoder`
  (flat schemas have no repetition levels; nested columns add a repetition-level stream).
- **Values**: `PLAIN` is the correctness baseline. `RLE_DICTIONARY` with plain
  fallback (on dictionary-size overflow) is the default for eligible columns, matching
  the reader's dictionary fast paths. The delta and byte-stream-split encodings are
  optional and deferred to a later breadth increment.
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
architectural dimension — paging, row-group cadence, nulls, nesting, dictionary pages,
compression, statistics — on a single type (`INT32`) before going wide. Type, encoding, and codec **breadth** is the same proven mechanism repeated, so
it is deferred until every dimension is settled; otherwise a late dimension would
reshape an already-multiplied surface (adding nulls, repetition levels, or the streaming
API after N typed methods exist rewrites all N). Each increment carries a **Kind**: *Dimension* (changes
the shape of the solution), *Breadth* (more of a proven mechanism), *Layer* (additive
API), *Optimization*, *Spike* (design-only), or *Docs* (user-facing documentation).

| # | Increment | Kind | New capability | Roadmap boxes | Completed |
|---|-----------|------|----------------|---------------|-----------|
| 1 | Tracer: flat `REQUIRED INT32`, one page / one row group, `PLAIN`, uncompressed. `OutputFile` (local + in-memory), `ThriftCompactWriter`, page-header + footer serialization. | Dimension | Produces a real, readable file | 1 (ThriftCompactWriter), 3.2 (page header ser.), 4.1/4.2 (chunk/row-group ser.), 5.2 (FileMetaData ser.) | [x] |
| 2 | Page chunking within a column chunk: a large `INT32` column written as multiple size-bounded `PLAIN` pages instead of one, replacing the single-page guard. Internal — the columnar API is unchanged. Each page carries a CRC-32 checksum over its on-disk body. | Dimension | Large columns written safely, bounded page size | 6.2 (multi-page data writing), 3.2 (CRC write) | [x] |
| 3 | **Row-group cadence** (`REQUIRED INT32` only): the `ColumnBatch` submission API, multi-row-group append, size-based auto-flush (page + row-group targets), and `WriterConfig`. Locks how the caller feeds data and how the file is banded into row groups. | Dimension | The public write cadence is settled | 6.2 (row-group size tracking, automatic flushing) | [x] |
| 4 | Nullable columns (`OPTIONAL INT32`): definition levels via `LevelEncoder`, and how nulls ride inside a `ColumnBatch`. | Dimension | The null / def-level data model is settled | 2.3, 3.3 | [x] |
| 5 | **Nested write design**: the shredding model (rep/def-level computation from struct / list / map nesting), the nested `ColumnBatch` input contract (per-layer validity + offsets, the write-side analog of `getLayerValidity` / `getLayerOffsets`), and `FileSchema.Builder` group / repeated-field support. Produces `_designs/WRITER_NESTED.md`, the reference the shredding increments implement against. | Spike | The nested write contract is settled | 6.3 (design) | [x] |
| 6 | **Struct shredding** (`INT32` leaves): `REQUIRED` / `OPTIONAL` nested groups — definition levels of depth > 1 and per-layer validity, no repetition yet. | Dimension | Nested structs written and read back | 3.3 (multi-level def), 6.3 | [x] |
| 7 | **List shredding** (`INT32` leaves): `REPEATED` fields — repetition levels via `LevelEncoder`, offset-driven nested input. | Dimension | The repetition-level data model is settled | 3.3 (rep levels), 6.3 | [x] |
| 8 | **Map shredding** (`INT32` leaves): key/value repeated group, reusing the list machinery. | Dimension | The full nested shape (structs, lists, maps) is settled | 6.3 | [ ] |
| 9 | Dictionary encoding (`INT32`): dictionary page + `RLE_DICTIONARY` indices + plain fallback, exercised on nullable and nested columns so the level + dictionary-index page layout is proven together. | Dimension | Dictionary column-chunk layout proven, incl. nulls and nesting | 2.2 | [ ] |
| 10 | Compression on the write path (`INT32`, one codec). | Dimension | Compress step + compressed/uncompressed size accounting proven | 6.2 (page compression) | [ ] |
| 11 | Column statistics (`INT32`: `min`/`max`/`null_count`, `ColumnOrder`-correct) accumulated during encode. | Dimension | Produced files support pushdown | 9.1 (stats) | [ ] |
| 12 | All primitive physical types (incl. `FIXED_LEN_BYTE_ARRAY` type length and `BYTE_ARRAY` min/max truncation), each inheriting paging, nulls, nesting, dictionary, compression and stats. Variable-width values end the constant-bytes-per-row assumption, so the row-group flush moves from the fixed rows-per-group proxy (`rowGroupTargetBytes / (columnCount × 4)`) to tracking the actual buffered uncompressed bytes. | Breadth | Write any column type, flat or nested | 2.1, 9.1 (truncation) | [ ] |
| 13 | Logical-type annotations: `LogicalTypeWriter` serializes the `LogicalType` union and legacy `converted_type`/`scale`/`precision`; `FileSchema.Builder` logical-type overload. | Breadth | Columns read back with their logical type (STRING, DATE, TIMESTAMP, DECIMAL, …) | 6.4 (annotation) | [ ] |
| 14 | Remaining codecs + optional delta and byte-stream-split encoders. | Breadth | Full codec / encoding choice | 2.4, 2.5 | [ ] |
| 15 | Parallel column encoding + row-group pipelining. | Optimization | Write throughput | — | [ ] |
| 16 | Row-oriented `ParquetWriter` ergonomic layer on the columnar core, including nested record materialization and logical-type value conversion (inverse of `LogicalTypeConverter`). | Layer | Mainstream-friendly API | 6.1 (`ParquetWriter`), 6.4 (value conversion) | [ ] |
| 17 | User-facing documentation under `docs/content/` for the writer public API (`OutputFile`, `ParquetFileWriter`, `FileSchema.Builder`): a `how-to` guide and a `reference` page, covering the settled surface including nesting and the row-oriented layer. | Docs | Documented, stable public API | — | [ ] |

Increments 1–4 settle the flat dimensions on `INT32`; 5–8 settle the nested shape —
design, then struct / list / map shredding — on `INT32`; 9–11 finish the remaining
dimensions (dictionary, compression, statistics) on the now flat-and-nested shape; 12–16
are breadth and layers that build on the settled shape; 17 documents the finished public
surface. Together they constitute the write-support milestone (#9). Sequenced as later
work, each its own design and sequence: DataPage V2, the optional index structures and
Bloom filters, the Avro write adapter, a CLI write/convert command, and the S3
`OutputFile` backend.

## User documentation

User-facing documentation under `docs/content/` is delivered as the closing increment
of the milestone (stage 17), once the full public surface — including nesting and the
row-oriented layer — is settled. Documenting the provisional `INT32`-only surface earlier
would be throwaway, so deferring is a recorded, intentional exception to the CLAUDE.md
rule that a new public API ships with a docs update — not an oversight. The public surface
(`OutputFile`, `ParquetFileWriter`, `FileSchema.Builder`) gets its `how-to`/`reference`
pages at stage 17.

## Roadmap reconciliation

`ROADMAP.md` remains the fine-grained capability inventory; this document is the plan
of record for writer architecture and sequencing. The mapping column above ties each
increment to the roadmap boxes it completes, so those boxes are ticked as increments
land. Phase 6 of `ROADMAP.md` points here.
