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

The optional index structures — OffsetIndex, ColumnIndex, and Bloom filters, with the
per-page statistics that drive page-level pruning (including the `DataPageHeader` inline
statistics) — are numbered increments 23–24 below, following the write-support milestone
on the settled surface. Sequenced as separate later milestones, each its own
design: DataPage V2, the Avro write API, and a CLI write/convert command. Sorting-column
metadata and custom record materializers are non-goals. Key-aligned layout control —
forcing a page boundary at key-value changes (one page per key) for point-query workloads
— is likewise a non-goal here; it would be a separate later increment if such demand
surfaces.

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
row-group size (default 128 MiB, counted as the buffered values would occupy `PLAIN`
and uncompressed) is what peak memory follows — this is the write-side inverse of the
reader's whole-file mmap. Peak memory is a multiple of that size rather than the size
itself, because the target counts a level as the bits it will occupy encoded where the
writer retains a byte per entry per level stream, and counts a value once where a
chunk under dictionary analysis holds it three times: a flat column peaks a little
above the target, a `list` column of empty lists at nine times it. Page size (default
1 MiB) bounds the granularity within a column chunk.

Three nested layout tiers stack here — a **page** encodes/compresses a slice of a
column, a **column chunk** is one column's pages for one row group, and a **row group**
holds one column chunk per column. All three are internal; only the page-size and
row-group-size targets are user-visible, as `WriterConfig` knobs.

### Ingestion cadence

Data arrives as **`ColumnBatch` objects** — an aligned slice carrying one typed array
per column. `ColumnWriter.writeBatch` takes a filler: it creates the batch bound to
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
- **S3 backend** (`internal.writer.S3OutputFile`, increment 22): sequential writes buffer
  to the multipart part size and upload parts; `close()` completes the multipart upload.
  In-flight bytes are bounded to the part size times a small concurrency multiple, so a
  fast producer cannot outrun the uploads; `CreateMultipartUpload` is deferred until the
  first part flushes, with a single `PutObject` for an output that never exceeds one part.
  It reuses the read-side S3 / SigV4 stack (`_designs/S3_OBJECT_STORAGE.md`,
  `_designs/S3_ZERO_SDK.md`).

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
- **Values**: `PLAIN` is the correctness baseline. `RLE_DICTIONARY` is the default for
  eligible columns, matching the reader's dictionary fast paths; a column chunk that is
  not dictionary-friendly is written `PLAIN` instead. The dictionary-vs-`PLAIN` choice is
  made per column chunk from its cardinality — incrementally with a mid-chunk `PLAIN`
  fallback on dictionary-size overflow initially (stage 9), then as a row-group-global
  decision taken once the group is buffered (stage 18), so no chunk mixes encodings. That
  automatic choice is the `AUTO` encoding policy a leaf column carries by default; the
  optional delta and byte-stream-split encodings are the other policies it may be given
  instead, per column or file-wide (stage 19).
- **Compression**: `UNCOMPRESSED` first, then `GZIP` / `SNAPPY` / `ZSTD` / `LZ4_RAW` /
  `BROTLI` — the existing codec libraries are bidirectional, so the encode side reuses
  them. `LZ4`'s deprecated Hadoop framing and `LZO` are refused rather than written
  (stage 19).
  The default codec is `ZSTD` when the zstd-jni library is on the classpath and
  `UNCOMPRESSED` otherwise, so a caller who did not ask to compress is not forced to
  carry the optional dependency; selecting a codec explicitly still requires its library.

`WriterConfig` knobs: row-group size, page size, encoding policy (file-wide and per leaf
column), codec, statistics truncation length, the row layer's precision-loss policy, and
the written `created_by` string.

### `created_by`

The default identifier follows the `<app> version <version> (build <hash>)` convention
Parquet readers parse — `hardwood version 1.1.0 (build a093aab)`, with a `-dirty` suffix
on the hash when the working tree was not clean at build time. The version and hash are
baked into `hardwood-core` at build time by `dev.hardwood.internal.BuildInfo`, which reads
a filtered resource populated by the parent POM's `capture-git-info` step; a build that
cannot determine either component reports `unknown` in its place, which stays parseable.

The convention is not cosmetic. A reader that cannot parse this field cannot tell which
implementation wrote the file, and applies its writer-specific correctness workarounds by
default: parquet-java's PARQUET-251 heuristic discards the deprecated `min` / `max` of a
`BINARY` or `FIXED_LEN_BYTE_ARRAY` column written by an unidentifiable writer. Hardwood
writes only the modern `min_value` / `max_value`, which that heuristic does not gate, so a
parseable identifier is what keeps the outcome from depending on which statistics fields
the writer happens to emit.

### Statistics

`StatisticsCollector` accumulates `min` / `max` / `null_count` per column chunk
during encoding and writes them into `ColumnMetaData`, so produced files support
reader-side predicate pushdown. `min`/`max` ordering follows the column's
`ColumnOrder` (the same ordering the reader honors on read), so written statistics
are pruning-correct. The bounds are the preferred `min_value` / `max_value` — never the
deprecated `min` / `max` — and each is flagged exact via `is_min_value_exact` /
`is_max_value_exact`, so a reader may treat `min_value == max_value` as proof that a whole
chunk holds a single value. Long `BYTE_ARRAY` `min`/`max` are truncated per the format's
binary min/max truncation rule, keeping statistics bounded while remaining valid for
pruning; a truncated bound is flagged **inexact** (`is_*_value_exact = false`), since it is
then only a bound and not the actual extreme.

A `FLOAT`, `DOUBLE` or `FLOAT16` chunk additionally carries `nan_count`, always — a chunk
with no `NaN` records a zero rather than leaving the field out. The format requires the field
of a floating-point column under the type-defined order, and the recorded zero is what a
reader needs: `NaN` sits outside the ordering the bounds are computed in, so an absent count
leaves a reader no choice but to assume `NaN` may be present and give up `gt` / `gtEq` /
`notEq` pruning. No other physical type writes the field.

These are **column-chunk** statistics, feeding row-group pruning. Per-page statistics — the
`DataPageHeader` inline statistics and the OffsetIndex / ColumnIndex structures that enable
page-level skipping — arrive with page index writing (increment 24), and Bloom filters with
increment 25. A file is valid without any of them.

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
file readable by independent implementations, so no unverified write code accumulates
in `main`. Cross-implementation reads are the primary check, graded by strictness: a
strict reader establishes conformance, a lenient one adds a second decoder's agreement
on the values. The read-side oracle is established in
`_designs/DIFFERENTIAL_TESTING.md` and the writer runs it in reverse.

1. **Strict-reader interop gate (primary)**: hardwood writes a file, parquet-java reads
   it, and the values are asserted to agree. parquet-java is the canonical
   implementation, so what it accepts is the operative definition of a conformant file.
   A permissive consumer agreeing on the values does not establish conformance: an
   encoder can emit a stream that only lenient decoders accept, and neither a
   hardwood-to-hardwood round trip nor a lenient engine can see it. The floor of the
   matrix is a single-entry dictionary per physical type — the shape where a zero-bit
   index stream carries no run header, which hardwood and DuckDB both accept and
   parquet-java rejects — and the matrix grows with the shapes each later increment
   introduces. `parquet-testing-runner` hosts the gate: it already carries
   `parquet-avro`, `parquet-hadoop` and `hadoop-common` at test scope, runs on every
   PR, and is where the reverse direction (parquet-java writes, hardwood reads)
   already lives. Settled in [WRITER_INTEROP_GATE.md](WRITER_INTEROP_GATE.md).
2. **DuckDB differential**: hardwood writes a file, then DuckDB reads it
   through `read_parquet('<path>')` and the values are asserted to agree. This is the
   exact inverse of [DifferentialReadTest], which has DuckDB read fixtures hardwood
   also reads; here DuckDB is the consumer of hardwood-written bytes. Because DuckDB
   shares no code with hardwood, agreement proves the bytes are spec-correct rather
   than merely self-consistent. Each row carries a synthetic index column so the
   comparison is robust to scan order (`ORDER BY` that column). Boundary values
   (signed extremes, all-null columns once nullable columns land, empty row groups)
   are written explicitly to stress the encoders.
3. **Round-trip**: write with hardwood, read back with hardwood, assert value and
   null equality. This is the fast inner-loop check and pins reader/writer agreement
   on details DuckDB does not surface (e.g. exact encodings, statistics).
4. **Property/fuzz** (later increments): random in-scope schemas and data round-tripped
   and run through the DuckDB differential, to surface edge cases beyond the
   hand-written cases (dictionary overflow, page and row-group boundaries).

Neither reader needs new tooling: DuckDB is already a `test`-scope dependency
(`org.duckdb:duckdb_jdbc`), and parquet-java is already on `parquet-testing-runner`'s
test classpath. Reading hardwood-written files with PyArrow is used for ad-hoc
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
API after N typed methods exist rewrites all N).

Once the shape is settled, a second principle takes over: the remainder is ranked by
whether it gates *use* of the writer, not by how much it widens it. A caller needs an
ergonomic entry point, documentation, and somewhere to write to; codec and encoding
breadth, and encode-time optimizations, extend a writer that is already usable without
them. The interop gate leads that group, because it is the only check on the shapes
every later increment introduces, and the increments it precedes multiply them.

The table is in delivery order. The `#` column is a stable identifier rather than a
position: an increment keeps the number it was filed under, so `16a` sits between 16 and 17,
`26a` / `26b` replace 26, and 32–33 and 36 are sequenced ahead of 24–25 without being
renumbered.

Each increment carries a **Kind**: *Dimension* (changes the shape of the solution),
*Breadth* (more of a proven mechanism), *Layer* (additive API), *Optimization*, *Gate*
(a validation barrier the increments after it depend on), *Spike* (design-only), *Docs*
(user-facing documentation), or *Benchmark* (a measurement the increments after it are
argued against).

| # | Increment | Kind | New capability | Roadmap boxes | Completed |
|---|-----------|------|----------------|---------------|-----------|
| 1 | Tracer: flat `REQUIRED INT32`, one page / one row group, `PLAIN`, uncompressed. `OutputFile` (local + in-memory), `ThriftCompactWriter`, page-header + footer serialization. | Dimension | Produces a real, readable file | 1 (ThriftCompactWriter), 3.2 (page header ser.), 4.1/4.2 (chunk/row-group ser.), 5.2 (FileMetaData ser.) | [x] |
| 2 | Page chunking within a column chunk: a large `INT32` column written as multiple size-bounded `PLAIN` pages instead of one, replacing the single-page guard. Internal — the columnar API is unchanged. Each page carries a CRC-32 checksum over its on-disk body. | Dimension | Large columns written safely, bounded page size | 6.2 (multi-page data writing), 3.2 (CRC write) | [x] |
| 3 | **Row-group cadence** (`REQUIRED INT32` only): the `ColumnBatch` submission API, multi-row-group append, size-based auto-flush (page + row-group targets), and `WriterConfig`. Locks how the caller feeds data and how the file is banded into row groups. | Dimension | The public write cadence is settled | 6.2 (row-group size tracking, automatic flushing) | [x] |
| 4 | Nullable columns (`OPTIONAL INT32`): definition levels via `LevelEncoder`, and how nulls ride inside a `ColumnBatch`. | Dimension | The null / def-level data model is settled | 2.3, 3.3 | [x] |
| 5 | **Nested write design**: the shredding model (rep/def-level computation from struct / list / map nesting), the nested `ColumnBatch` input contract (per-layer validity + offsets, the write-side analog of `getLayerValidity` / `getLayerOffsets`), and `FileSchema.Builder` group / repeated-field support. Produces `_designs/WRITER_NESTED.md`, the reference the shredding increments implement against. | Spike | The nested write contract is settled | 6.3 (design) | [x] |
| 6 | **Struct shredding** (`INT32` leaves): `REQUIRED` / `OPTIONAL` nested groups — definition levels of depth > 1 and per-layer validity, no repetition yet. | Dimension | Nested structs written and read back | 3.3 (multi-level def), 6.3 | [x] |
| 7 | **List shredding** (`INT32` leaves): `REPEATED` fields — repetition levels via `LevelEncoder`, offset-driven nested input. | Dimension | The repetition-level data model is settled | 3.3 (rep levels), 6.3 | [x] |
| 8 | **Map shredding** (`INT32` leaves): key/value repeated group, reusing the list machinery. | Dimension | The full nested shape (structs, lists, maps) is settled | 6.3 | [x] |
| 9 | Dictionary encoding (`INT32`): dictionary page + `RLE_DICTIONARY` indices + plain fallback, exercised on nullable and nested columns so the level + dictionary-index page layout is proven together. Settled in `_designs/WRITER_DICTIONARY.md`. | Dimension | Dictionary column-chunk layout proven, incl. nulls and nesting | 2.2 | [x] |
| 10 | Compression on the write path (`INT32`, one codec). | Dimension | Compress step + compressed/uncompressed size accounting proven | 6.2 (page compression) | [x] |
| 11 | Column statistics (`INT32`: `min`/`max`/`null_count`, `ColumnOrder`-correct) accumulated during encode. | Dimension | Produced files support pushdown | 9.1 (stats) | [x] |
| 12 | All primitive physical types (incl. `FIXED_LEN_BYTE_ARRAY` type length and `BYTE_ARRAY` min/max truncation), each inheriting paging, nulls, nesting, dictionary, compression and stats. Truncated `BYTE_ARRAY` bounds are flagged inexact (`is_min_value_exact` / `is_max_value_exact` = false), extending the exactness the fixed-width types write unconditionally as true. Variable-width values end the constant-bytes-per-row assumption, so the row-group flush moves from the fixed rows-per-group proxy (`rowGroupBufferTargetBytes / (columnCount × 4)`) to tracking the actual buffered uncompressed bytes. Settled in `_designs/WRITER_PRIMITIVE_TYPES.md`; delivered in two stacked increments (12a fixed-width `BOOLEAN`/`INT64`/`FLOAT`/`DOUBLE`, 12b variable-width `BYTE_ARRAY`/`FIXED_LEN_BYTE_ARRAY` with truncation). | Breadth | Write any column type, flat or nested | 2.1, 9.1 (truncation) | [x] |
| 13 | Logical-type annotations: `LogicalTypeWriter` serializes the `LogicalType` union and legacy `converted_type`/`scale`/`precision`; `FileSchema.Builder` logical-type overload. Both annotations are emitted together for every type with a legacy equivalent (STRING, DATE, DECIMAL, the INT/UINT widths, TIME/TIMESTAMP millis+micros, ENUM, JSON, BSON, `LIST`, `MAP`), the union taking read precedence and the `converted_type` kept for pre-union readers; only types without a legacy equivalent (UUID, FLOAT16, NANOS units, VARIANT, GEOMETRY/GEOGRAPHY) are union-only, and INTERVAL is `converted_type`-only because its union member is reserved but undefined. This makes stage 13 additive to the `converted_type`-only annotations stages 6–8 already write. An annotation also redefines the column's `min`/`max` ordering, so the statistics collectors become logical-type-aware and the footer's `column_orders` — required by the format wherever bounds are written — is emitted. Settled in `_designs/WRITER_LOGICAL_TYPES.md`; delivered in two stacked increments (13a annotations, 13b order-correct statistics and `column_orders`). | Breadth | Columns read back with their logical type (STRING, DATE, TIMESTAMP, DECIMAL, …) | 6.4 (annotation) | [x] |
| 14 | **Write-path interop gate**: `parquet-testing-runner` reads hardwood-written files with parquet-java and asserts value agreement, covering a single-entry dictionary per physical type as its floor and every shape increments 1–13 can produce. Closes the defect class where only hardwood and a lenient engine accept the bytes; the suites that precede it read back through hardwood and DuckDB, neither of which can observe it. Every increment after this one extends the matrix with the shapes it adds. Settled in `_designs/WRITER_INTEROP_GATE.md`. | Gate | Produced files are proven conformant, not merely self-consistent | — | [x] |
| 15 | **Parseable `created_by`**: `ParquetFileWriter.DEFAULT_CREATED_BY` follows the `<app> version <version> (build <hash>)` convention Parquet readers parse, rather than a bare application name they reject. `hardwood-core` gains the build-info plumbing that identifier needs (`dev.hardwood.internal.BuildInfo` over a filtered resource fed by the parent POM's `capture-git-info` step), which `hardwood-cli`'s `Version` also consumes so the plumbing exists once. Detailed above under `created_by`. Surfaced by increment 14, whose footer assertions extend with parquet-java's `VersionParser` accepting the identifier and its PARQUET-251 heuristic consequently sparing binary statistics. | Dimension | Produced files carry a version identifier consumers can parse | — | [x] |
| 16 | **Row-oriented `RowWriter`**: an ergonomic layer on the columnar core, obtained from `ParquetFileWriter.rowWriter()` and mirroring the read side's `rowReader()`. `writeRow` takes a filler over a `StructBuilder`, with `ListBuilder`/`MapBuilder` for nesting, so fields are addressed by their user-visible names rather than by leaf paths carrying the synthetic `list.element` / `key_value` segments, or by their position within the struct that declares them. Setters mirror the reader's accessors one for one — including their by-index form, so code that walks a row generically can read at a position and write at the same one — and convert logical-type values to their physical representation through `PhysicalValueConverter`, the inverse of `LogicalTypeConverter`. The layer is an adapter: it transposes staged rows into a `ColumnBatch` and submits them through `writeBatch`, so every guarantee of stages 1–15 holds unchanged, asserted by writing the same data both ways and requiring byte-identical files. A file writer serves one API or the other, latched on first use. Values whose precision the column cannot hold are rejected by default, with `WriterConfig.precisionLossPolicy(TRUNCATE)` opting into dropping the digits that do not fit; a value the column cannot represent at all is rejected under either. Settled in `_designs/WRITER_ROW_API.md`. | Layer | Mainstream-friendly API | 6.1 (`RowWriter`), 6.4 (value conversion) | [x] |
| 16a | **Annotation range checks on the columnar API**: `ColumnBatch` rejects a value outside the range its column's annotation declares, as the row layer already does. An annotation narrows what a physical type may hold — an `INT(8)` `INT32` to `[-128, 128)`, a `UINT_8` to `[0, 256)`, a `DECIMAL(p, s)` to unscaled values of at most `p` digits — and the columnar setters currently take any value the physical type can carry, so a caller can produce a file whose values fall outside the range its own annotation declares. A `uint8` consumer reads 300 back as 44, and the bounds written into the column's statistics describe values the annotation says cannot exist. This is per-value validation of the kind `ColumnBatch` already performs — `validateBinaryValues` checks every `FIXED_LEN_BYTE_ARRAY` value against the declared type length — extended to the numeric annotations. Closing it makes rejection a property of the writer rather than of which of the two APIs the caller picked. Settled in `_designs/WRITER_ANNOTATION_RANGES.md`. | Gate | Neither write API can produce a value outside its column's declared range | 6.4 (annotation) | [x] |
| 17 | **Flat write benchmark**: `FlatWriteBenchmark` in `performance-testing/micro-benchmarks` — Hardwood's columnar and row-oriented APIs against parquet-java's `ExampleParquetWriter` over a seeded, taxi-shaped six-column fixture held in memory, with every writer setting matched across contenders and the produced file sizes reported next to the times. The write-side counterpart to `FlatPerformanceTest`, and the baseline the throughput stages that follow are argued against. Settled in `_designs/FLAT_WRITE_BENCHMARK.md`. | Benchmark | A measured write baseline | — | [x] |
| 18 | **Row-group-global dictionary selection** (#975): replace the per-column-chunk optimistic build with mid-chunk `PLAIN` fallback (stage 9) with a choice made once the row group is fully buffered — each column chunk is encoded `RLE_DICTIONARY` or `PLAIN` as a whole from its true cardinality, so no chunk mixes encodings and no dictionary page is written for a chunk that ends up `PLAIN`. Trades a second encode pass and higher peak buffer occupancy for the optimal per-chunk choice; the payoff scales with the variable-width types from stage 12, where indices are far smaller than the values and the dictionary byte-limit is a poor proxy for whether encoding pays. With the whole group buffered the choice follows from the exact cardinality — and, where the byte-limit proxy is weakest, a direct comparison of the two encodings' sizes — so no predictive threshold is needed; the stage-9 streaming abort heuristic does not apply here. The public consequence is `WriterConfig.dictionaryPageLimitBytes`, which exists only as that predictive threshold and is removed with the fallback it triggers; the memory bound the analysis still needs is derived from `rowGroupBufferTargetBytes` rather than configured. Nothing has shipped that depends on the option, and the stage precedes the writer's user documentation so the option never reaches a published page. Settled in `_designs/WRITER_DICTIONARY_SELECTION.md`. | Optimization | Optimal, uniform per-chunk encoding choice | 2.2 | [x] |
| 19 | **Remaining codecs + optional delta and byte-stream-split encoders** (#976). Every codec `CompressionCodec` names is either produced or refused with a reason specific to why, and the four optional encodings become part of a single per-leaf-column encoding policy on `WriterConfig` — `AUTO` (the stage 18 size comparison), `PLAIN`, or one of the four — set file-wide or per leaf path and validated against each column's physical type at writer creation. The policy subsumes `WriterConfig.enableDictionary`, which is removed with it: dictionary-or-`PLAIN` is the whole of what `AUTO` decides, so the boolean was a two-valued encoding policy named before there were encodings to name it with, and `enableDictionary(false)` is a file-wide `PLAIN` exactly. As with stage 18's `dictionaryPageLimitBytes`, nothing has shipped that depends on it and the stage precedes the writer's user documentation, so one concept rather than two reaches a published page. The optional encodings are selected rather than inferred: the stage 18 comparison decides between a dictionary and `PLAIN` from what the buffer already retains, where a delta encoding's size is a property of value order and byte-stream-split's is a property of the codec that follows it, neither of which is decidable without a trial encode. Every codec is a new axis value on the stage 14 interop gate, and the optional encodings are ones that gate's readers must read back, so this breadth lands inside the gated series rather than after it. Settled in `_designs/WRITER_CODECS_AND_ENCODINGS.md`; delivered in two stacked increments (19a codecs, 19b encodings). | Breadth | Full codec / encoding choice | 2.4, 2.5 | [x] |
| 20 | User-facing documentation under `docs/content/` for the writer public API (`OutputFile`, `ParquetFileWriter`, `ColumnBatch`, `RowWriter`, `WriterConfig`, `FileSchema.Builder`): a `how-to` guide per entry point, a `reference` page, and a `concepts` page for the write model, covering the settled surface including nesting, the row-oriented layer, and the codec, encoding and dictionary options stages 18–19 settle. The row layer's by-index addressing belongs in that coverage, together with the one thing about it a user can get wrong: the reader's index is a position in projected schema order and the writer's is declaration order in the schema being written, so the two agree where a whole file is read into its own schema and shift under a projection. The site's reader-only framing is corrected here. Settled in `_designs/WRITER_DOCS.md`. | Docs | Documented, stable public API | — | [x] |
| 20a | **Footer key-value metadata, and `created_by` beside it** (#1029): `ParquetFileWriter.keyValueMetadata(String, String)` and `keyValueMetadata(Map<String, String>)` accumulate the footer's `key_value_metadata`, which the writer emits today as an empty map and `FileMetaDataWriter` does not serialize at all — it jumps from field 4 to field 6. The read side surfaces the field in full, so this is the same gap as 32, 33 and 36, and the one that decides whether a file the writer produces from one it read is the same file: `ARROW:schema`, `pandas`, Spark's `org.apache.spark.sql.parquet.row.metadata` and the Iceberg and Delta stamps all ride in this field, and all of them are dropped silently today. The methods sit on the writer rather than in `WriterConfig` for two reasons. `WriterConfig` states what governs the row groups that follow — 30 makes that explicit by letting `endRowGroup()` carry a new one — while the footer states its fields once for the whole file. And key-value metadata is worth setting after data has been written, for a value the caller knows only then: a row count, a digest over what was produced, an upstream schema stamped once the shape is settled. Both are reasons a value object passed at `create()` cannot express it. `created_by` relocates for the first reason alone: it is the one member of `WriterConfig` that is not per row group or narrower, and 30 has to reject a passed configuration that would change it, so with it moved the configuration `endRowGroup()` accepts supersedes wholesale and `WriterConfig` has a single lifetime throughout. On the wire the field is `list<KeyValue>` with an optional `value`, so duplicate keys and a key carrying no value are both representable; the reader collapses that into a `LinkedHashMap`, last key winning and an absent value reading as `null`, so a `Map<String, String>` input reaches exactly the shape a reader observes, and a `null` value writes a `KeyValue` whose field 2 is absent, which is what round-tripping a value-less key requires. Setters are refused after `close()`, as every other write on the writer is. Numbered after 20 and delivered directly behind it, inside `1.1.0.Beta1`: moving `createdBy` off `WriterConfig` is a breaking change to the surface 20 has just documented, so it carries the amendment to that reference page with it and the page is published once. | Layer | Application metadata survives a read-then-write | 5.2 (key_value_metadata ser.), 6.2 (footer writing) | [x] |
| 21 | **Write-path benchmark coverage** (#989): what stage 19's encoding policy costs and saves, and the profiling baseline that goes with it. `WriteEncodingBenchmark` runs four named encoding cases over the existing flat fixture against `{ZSTD, UNCOMPRESSED}` — the default codec and the control that shows an encode-side change at full size — reporting produced size beside the time. One case carries the stage: applying `PLAIN` to exactly the columns the flush-time comparison rejects anyway writes the same pages while skipping the interning that produced them, so the gap to `AUTO` is the cost of the writer's most expensive per-value decision and the upper bound on what 26 can buy — measured through public API, on the fixture 18 was argued on. Allocation per operation is reported in the run the results come from, the writer retaining a row group's values to decide their encoding. `FlatWriteBenchmark` itself is untouched, so the number 17, 18 and 19 were argued against stays comparable. The baseline is taken on bare metal, single-core-pinned, over short probe runs rather than the matrix: classify with `perfnorm` and `gc`, locate with async-profiler in cpu and alloc modes, read the code generation with `perfasm` at the one point that names, and settle each candidate with an A/B that changes the supply of what it is supposed to consume. 21b then adds the shapes that need a fixture of their own — a nested write benchmark and a schema-width axis — once the flat baseline says where the time goes. Its own design. | Benchmark | Measured cost of the encoding policy, and a located write path | — | [ ] |
| 22 | S3 `OutputFile` backend: sequential multipart upload — buffer to the part size, upload parts, complete on `close()`. In-flight bytes bounded to the part size times a small concurrency multiple; lazy `CreateMultipartUpload` deferred to the first part flush, with a single `PutObject` fallback for a sub-part output. Reuses the read-side S3 / SigV4 stack. | Layer | Write directly to object storage | — | [ ] |
| 23 | Parallel column encoding + row-group pipelining. | Optimization | Write throughput | — | [ ] |
| 32 | **`ColumnBatch.strings(…)` and a blob-plus-offsets binary form**: the writer's value-input vocabulary is narrower than the reader's value-output vocabulary, and the difference falls entirely on the variable-width types. The reader hands back `getStrings()` (decoded `String[]`) and the zero-copy pair `getBinaryValues()` / `getBinaryOffsets()` (one packed blob plus the byte spans within it); the writer accepts only `byte[][]`. A caller holding `String[]` encodes UTF-8 itself, and a caller bridging from the reader's packed form re-materializes one `byte[]` per value — an allocation per value on the API whose reason for existing is that it does not allocate per value. `strings(int|String, String[], …)` closes the first, in the same six overload shapes every other verb has. `bytes(int|String, byte[] blob, int[] offsets, …)` closes the second and is the more valuable of the two, because it is the only form that lets a columnar copy stay on the packed representation end to end; it also composes with stage 33's copier, which cannot avoid the round trip through `byte[][]` today. Neither is sugar over the other: `String[]` still needs encoding, and the packed form still needs no `byte[][]` to exist. | Layer | The columnar writer accepts every shape the columnar reader produces | 6.4 | [ ] |
| 33 | **Group-oriented nesting for columnar copying** (see `ColumnarNestedCopyTest`): a nested file can be copied from `ColumnReader` to `ColumnBatch` byte-identically today, but only through four corrections a reasonable first attempt gets wrong, three of them silently rather than loudly. Three share one root cause: the reader describes nesting per *leaf*, as layers beneath it (`getLayerCount`, `getLayerKind(k)`, `getLayerValidity(k)`, `getLayerOffsets(k)`), and the writer addresses it per *group*, by dot path (`struct`, `list`, `map`). Nothing bridges the two, so a caller reimplements the reader's own layer-derivation rule — which groups contribute a layer, and that the synthetic `list` / `key_value` segment is a path segment but not a layer — and then de-duplicates the shared ancestors a per-leaf walk reports more than once, against an "already set" rejection. A subtle error in that derivation misaligns validity and offsets against groups rather than failing. Naming the layers — `ColumnReader.getLayerPath(int)`, plus a `LayerKind` that separates `MAP` from `LIST` since the writer's verbs reject each other's groups — removes the derivation but leaves the shape mismatch and the de-duplication standing. Projecting the same buffers group-wise instead, as one accessor on `ColumnReaders` yielding each nesting group once with its path, kind, validity and offsets, removes all three and reduces the copy to "set every group, then set every leaf". The per-leaf layer view stays as it is: it is the right shape for a consumer reading one column, and the wrong one for a caller feeding a whole file to a writer. The fourth correction is the writer's and is independent of the other three: `getLeafValidity()` answers "is there a value in this slot" and is false wherever an `OPTIONAL` ancestor is absent, whereas the writer's leaf mask answers "is this column's own value null" and is refused outright on a `REQUIRED` column — so a `REQUIRED` leaf under an `OPTIONAL` struct reports nulls the writer will not accept, and for `BYTE_ARRAY` those same slots arrive as Java `null`s the all-present setter rejects and the caller must plug with a placeholder first. Both halves are the same gap: the writer validates a leaf against its own declared repetition without consulting the ancestor masks that decide whether a slot is encoded at all. `ColumnBatch` already holds the principle — "the values at rows a column's `Validity` marks null are never encoded, and so are never checked" — and extending it to ancestor-absent rows costs the property that a rejection names the setter call it came from, because ancestors may be declared after leaves: either leaf validation defers to submit, or groups must precede leaves in a batch, and that ordering rule is its own hazard. The reader-side alternative is to expose the leaf's own null bit separately from the present-in-slot mask, which is a bitmap the reader does not compute today. Acceptance is concrete rather than descriptive: the copier in `ColumnarNestedCopyTest` loses its derivation, its de-duplication set, its repetition branch and its hole-plugging, and still produces a byte-identical file. | Layer | Columnar copy is a pass-through, not a reimplementation of the reader's layer rule | 6.3 | [ ] |
| 36 | **A nullable struct enclosing a repeated field** (#1026): the shredder refuses an `OPTIONAL` struct group that directly encloses a repeated field, in both write APIs — `RecordShredder.impliedRecordCount` walks a leaf's layers from leaf to root, resets the running record count at each `REPEATED` layer to `offsets.length - 1`, and throws where a nullable `STRUCT` layer sits above one; `RowPlan` raises the same at `rowWriter()` so the row layer fails before a record is staged. The mirror shape — a nullable struct *inside* a repeated scope — round-trips today, so this is one direction of one rule rather than a hole in the nested model. It is the shape Arrow-based writers produce for a nullable struct holding a list, which makes it the most common nested shape the writer cannot express: this repository's own `nested_list_struct_test.parquet` has it, the reader reads that fixture in the suite, and a columnar copy of it therefore cannot be written back. The columnar contract already distinguishes every state the levels need — the struct's `Validity` for an absent instance, the list's for a null list, a zero-delta offset pair for an empty one, the leaf's for a null element — so the guard protects against emitting wrong levels rather than marking something the input cannot say. Lifting it means teaching the leaf-to-root walk how an absent ancestor maps onto the offsets below it, reusing the placeholder handling the shredder already applies to absent struct ancestors, and covering the shape in the interop gate so parquet-java agrees on the levels. | Dimension | The nested model is closed in both directions | — | [x] |
| 24 | **Page index writing**: per-column-chunk OffsetIndex (page locations) and ColumnIndex (per-page `min`/`max`, null counts, boundary order), written after the row group's pages and referenced from the footer, so the reader can skip individual pages. Extends the column-chunk statistics of increment 11 to page granularity; the `DataPageHeader` inline `statistics` are the pre-index fallback covered here. Truncation and `is_*_value_exact` follow the increment 11 / 12 rules. Its own design. | Layer | Page-level pruning on produced files | 9.2 | [ ] |
| 25 | **Bloom filter writing**: a split-block Bloom filter per eligible column chunk (XXHASH64), serialized with its header and referenced from the column metadata, for equality-predicate pruning where `min`/`max` do not help. Its own design. | Layer | Bloom-filter pruning on produced files | 9.3 | [ ] |
| 37 | **`encoding_stats` in the column metadata** (#1083): `ColumnMetaDataWriter` serializes fields 1–12 and stops, so field 13 — `list<PageEncodingStats>`, one `(page_type, encoding, count)` per combination — is absent from every file the writer produces. It is the field a reader needs to know whether a chunk's dictionary covers all of the chunk's values, so without it a hardwood-written file gets no dictionary-based row-group pruning, from Hardwood or from parquet-java: `RowGroupDictionaryFilterSource` treats a chunk with no `encoding_stats` as ineligible, because a dictionary that describes only a prefix cannot prove a value absent. `hardwood dive` reads the same field for its per-page `Encoding` column and its `Chunk encodings` line, both of which degrade on produced files. The coverage question the field exists to answer is one this writer answers by construction: since 18 the encoding is chosen once per chunk from the fully buffered row group, so a chunk is `RLE_DICTIONARY` or `PLAIN` throughout and a dictionary page is written only for a chunk that uses it. The entries follow from the choice already made — a `DICTIONARY_PAGE` entry where there is one, a `DATA_PAGE` entry over the pages that chunk wrote — with nothing to track and no partial coverage to represent. Delivered with the round-trip the writer and that reader have never had between them: `RowGroupDictionaryFilterSource` finding the dictionary of a hardwood-written chunk, and finding none for a `PLAIN` one. | Layer | Dictionary pruning on produced files | — | [ ] |
| 26a | **Early abandonment of a losing dictionary** (#992): run the comparison the writer already makes at flush — dictionary body plus index stream against the values `PLAIN` — on the chunk's prefix, and give the dictionary up as soon as it is losing, rather than interning a column's every value into a table that is then thrown away. Stage 21a measured that at 43% of a write and 37% of its allocation on the taxi fixture, for a file 36 bytes different: nine chunks' `distinct_count` and nothing else. Not a new heuristic but the existing predicate evaluated earlier, against fields the buffer already maintains, through the `giveUpDictionary()` the flush comparison has called since stage 18 — so where prefix and chunk agree the produced file is byte-identical and only the work differs. Probes at 8192 present values and doubling, counted in values rather than pages because `pageValues` follows the schema's widest column and a narrow schema would never reach a page boundary inside a row group. Abandons on two consecutive losing probes, which is what bounds the front-loaded column a single probe would misjudge — the risk stage 18 named when it declined to abandon on a single probe. Its own design. | Optimization | Write throughput, at no cost in produced size | — | [x] |
| 26b | **Cardinality sketch after the analysis cap** (#979): superseded. The stage existed because the per-column dictionary size cap was a memory bound that also returned an encoding verdict — a column whose dictionary was large but whose values repeated enough to pay for it was written `PLAIN` whenever the cap fired — and the sketch was how the decision would survive the cap. Stage 27 removes the cap instead: a dictionary is part of what a chunk retains, so it is bounded by `rowGroupBufferTargetBytes` like everything else the writer holds, and such a column now keeps its dictionary and reaches the byte target sooner. The size probes go on bounding the work. Nothing is left for a sketch to buy, and a bounded-error count could not have served `distinct_count` in any case, the format asking for the count of distinct values occurring rather than an estimate. Closed as superseded. | Optimization | — | — | [~] |
| 27 | **Row-group sizing** (#9, prompted by #980, which it does not close): a row target alongside a memory target, cutting a row group at whichever is reached first. How a file is banded is what governs split sizing and row-group-level pruning, and it was unaskable: what a record costs depends on its values, so the same target produces row groups of wildly different record counts for different columns. A row count needs no estimate and does not vary with the data, which is what three of the four writers surveyed use for this. The byte target bounds the writer's largest allocation, so it measures that allocation directly — level builder lengths, index count, store length and dictionary content, a sum of field reads per column — rather than what the values would occupy `PLAIN`, which is neither what is held nor what reaches the file. Because a dictionary-alive column's retained bytes depend on whether each value is new, which needs the hash, the writer appends a bounded slice and reads how full it is instead of computing in advance how much fits; a flat column's slice goes in as one bulk copy. A target on *produced* size — which is what #980 asks for — is not part of this stage: it would have to predict a codec's ratio on data it has not seen, or compress as it goes and give up the flush-time encoding decision, which is how the writers that target produced size do it. #980 therefore stays open on the successor section rather than closing here. Settled in [WRITER_ROW_GROUP_SIZING.md](WRITER_ROW_GROUP_SIZING.md). | Layer | Banding a reader can plan against, and a memory bound that means memory | — | [x] |
| 28 | **Byte-based page cuts** (#981): cut a data page when its *encoded* bytes reach `pageTargetBytes`, decided at flush where the chunk's encoding is settled and every value's size is known, rather than at an entry count computed once per file from the widest column's `PLAIN` width. That count was honoured for one column per file: a `BYTE_ARRAY` column of values wider than the nominal 16-byte assumption overshot it instead — 1 KiB values produced a 51 MiB page and 8 KiB values a 134 MiB one, the whole column chunk in a single page — while a dictionary column undershot it, cut on what its values would have cost `PLAIN` rather than on what its indices cost. Deciding at flush removes the estimate, the page plan and both per-entry checks, and takes the flat fixture's file a seventh smaller. Settled in [WRITER_ROW_GROUP_SIZING.md](WRITER_ROW_GROUP_SIZING.md). Wants a row cap alongside the byte target, whose default belongs with page index writing (24) rather than here, since page size is also pruning granularity: that number is 20,000, which is where parquet-java and arrow-rs both cap it, and it costs 8.6% in file size on the flat fixture — a cost that buys nothing until there is a page index to prune with. | Optimization | `pageTargetBytes` means what it says, for every column | — | [x] |
| 29 | **`distinct_count` for the chunks that cannot state it** (#982): the exact cardinality is written today wherever a chunk still held what it counted with when its encoding was chosen, and for `BOOLEAN` chunks, which know it without a dictionary. It is absent for a chunk that stopped interning because the size probes found its dictionary losing, and for one written with dictionary encoding disabled. The sketch of 25 cannot serve this — the format asks for the count of distinct values occurring, not an estimate — but pass two holds every value of the chunk, so an exact count without a hash structure is available from a sort over a fixed-width column's values, at a cost worth weighing against the field's value. | Breadth | Cardinality metadata on every chunk | — | [ ] |
| 30 | **Caller-controlled row-group boundaries** (#985): `ParquetFileWriter.endRowGroup()` closes the open row group and starts a new one, so a boundary can fall where the writer cannot see that it should — a sort-order break, a partition-key change, an upstream batch boundary, or a row count no byte target expresses. Row groups are cut today only by `rowGroupBufferTargetBytes` and the arrival of data, and the batch split at that bound is the only cut point that exists; nothing lets a caller say the rows it is about to write belong in a new group. The byte target stays the automatic bound and the explicit switch is an additional trigger, so a group closes at whichever comes first. A call with nothing buffered is a no-op, so calling it unconditionally at every partition boundary cannot produce an empty row group, and the row layer drains its staged rows before the cut so the byte-identical equivalence between the two write APIs holds. The switch also carries an optional `WriterConfig`, which supersedes the one in force for the row groups that follow: `ColumnMetaData` holds a codec and an encoding list per column chunk, so the format lets both change from one row group to the next, and a boundary the caller places is the only point at which naming a new one is meaningful. Everything in `WriterConfig` is per row group or narrower — page and row-group targets, statistics truncation, precision-loss policy, encoding policies, codec — 20a having moved `created_by`, the one member the footer states once, onto the writer itself, so the passed configuration supersedes wholesale and no field is rejected. The new configuration governs the group that starts; the group being closed is written with the one in force while its rows were buffered. It is validated and its compressor resolved before the open group is closed, as `create` resolves before `out.create()`, so a configuration the writer cannot honour leaves both the file and the writer intact. Amending rather than replacing wants `WriterConfig.toBuilder()`, which does not exist yet. Hardwood reads such files already — `PageDecoder` resolves a decompressor per chunk and dive reports a codec histogram per row group — and produces none, so this stage also supplies the mixed-codec fixtures the read path is written for. Its own design, and its own docs update, stage 20 having shipped by then. Arrow C++ (`NewRowGroup`) and arrow-rs (`next_row_group`) expose the boundary control; neither, nor parquet-java, lets the settings change with it. | Layer | Row groups aligned to what the caller knows, and settings that can follow the data | 6.2 | [ ] |
| 31 | **Write-path coverage assertion** (#990): the interop gate's extension contract, checked by the build rather than by whoever extends it. The space the writer can produce is derived from its own capability tables — `PhysicalType`, `ColumnEncoding`'s per-type legality, `CompressionCodec`'s produced values, and the sealed `LogicalType` hierarchy — and what each test actually produced is recorded where every write-path test already reads its file back, from the page headers and footer parquet-java parsed. A set of pairwise projections over that space is then required: physical type × page encoding, page encoding × codec, physical type × repetition shape, fixed length × page encoding, and annotation × carrier × storage form. A producible cell no test produced fails the build unless waived with a reason, and a waived cell that is observed fails too, so a waiver cannot outlive what justified it. Stage 19 is what makes this material: it multiplied codecs and encodings while the sweep stayed one-axis-at-a-time, and the annotation tables it inherited are enumerated by hand — `IntType` at four of eight width/sign combinations, `TimeType` and `TimestampType` at three of six with the unit confounded with `isAdjustedToUTC`, `DecimalType` at no carrier's precision boundary. `WriterAnnotationCoverageTest` closes that half, generating its cases from the same domain: each annotation in both storage forms, and at the ends of the range it declares rather than merely at its physical type's, which is where the statistics comparator is fragile. Its own design, [WRITE_COVERAGE_ASSERTION.md](WRITE_COVERAGE_ASSERTION.md). | Gate | The gate's coverage is asserted rather than assumed | — | [x] |
| 34 | **`StructBuilder.setValue(String\|int, Object)`**: the runtime-typed escape hatch mirroring the reader's `getValue`, and the last unmatched entry between `StructAccessor` and `StructBuilder` that is closable. It conflicts with the reserved-surface rule in `_designs/WRITER_ROW_API.md` — "no `Object`-typed setter anywhere in this API" — so that rule has to be amended rather than ignored, and the amendment is narrower than it looks. The rule's two justifications are that a boxing overload would become the default binding (`add(1)` binding where `addInt(1)` was meant) and that it would create ambiguity against the primitive forms; both are properties of an *unqualified* `set`/`add` overload competing with typed siblings on the same name, and neither holds for a distinctly named `setValue`, which no call to `setInt` can accidentally resolve to. What does hold is the original rationale for its absence: a reader must handle a column whose type it learns at runtime, whereas "a writer always knows the schema it declared". That is true of application code and false of the case that motivates this stage — a copier, a projection, or a format bridge, which knows the schema only at runtime and today must switch on `PhysicalType` by hand to pick a setter. The value is dispatched against the column's declared type and rejected if it does not fit, so the escape hatch is checked rather than coercing. | Layer | Generic row-shaped code can write a field whose type it learns at runtime | 6.4 | [ ] |
| 35 | **`StructBuilder.setVariant(String\|int, PqVariant)`**: the remaining unmatched reader accessor, and the one that is not an API question. `VARIANT` values are listed out of scope for the row layer because the writer has no Variant encoder at all — a shredded Variant group can be written field by field through the binary setters, but nothing produces the metadata dictionary and value binary a `VARIANT` column holds. So this stage is a Variant *writer*, of which `setVariant` is the surface: the encoder, its own design document, the choice of whether to write the unshredded `metadata`/`value` pair only or to shred into `typed_value` as the read path already reassembles, and interop-gate coverage against parquet-java's and Arrow's Variant readers. Sized and sequenced accordingly — it is a breadth stage in its own right, not a setter to be added alongside stage 34. | Breadth | `VARIANT` columns are writable, not only readable | 6.4 | [ ] |

Increments 1–4 settle the flat dimensions on `INT32`; 5–8 settle the nested shape —
design, then struct / list / map shredding — on `INT32`; 9–11 finish the remaining
dimensions (dictionary, compression, statistics) on the now flat-and-nested shape; 12–13
are breadth on the settled shape. Increment 14 then gates everything those thirteen
produce against a strict reader, 15 fixes the first thing that gate found, and 16 makes
the result usable by an external caller with the row-oriented layer. Between them, 16a
carries the range checks the row layer established back to the columnar API, so which
entry point a caller picks does not decide whether an out-of-range value is caught, and 17
measures what the two entry points cost against the incumbent — the baseline every
throughput claim after it is argued against, and the increment that quantified what 18 is
worth. Then 18 makes the encoding choice optimal rather than optimistic and 19 completes
the codec and encoding breadth, both inside the gate that 14 established, before 20
documents the whole of it. 20a closes the release, moving the two footer-scope fields onto
the writer and making the second of them writable at all, while `createdBy` can still move
without breaking a released surface. Those nine are the content of `1.1.0.Beta1` — a
conformance-gated, ergonomic, measured, optimally-encoding, documented writer whose files
carry the application metadata they were read with, local output only.

Increment 21 opens `1.1.0.Beta2` by widening what the write path measures to the axes the
writer grew after 17 — encoding, cardinality, nesting and schema width — because 23 and the
size refinements after it are throughput and size claims that cannot be argued without one.
22 then adds the object-store backend and 23 optimizes the encode underneath it with
parallel column encoding. Together, increments 1–23 constitute the write-support milestone
(#9). 32 and 33 lead what follows it, because together they are what stands between a file
the reader accepts and a file the writer can produce from it. They are where the round trip
costs something: a `byte[][]` re-materialization per value on the API whose reason for
existing is that it does not allocate per value, and a caller-side reimplementation of the
reader's layer rule whose errors misalign validity against groups rather than failing. A
caller bridging the two APIs pays the first on any binary column and the second on any
nested one, and `ColumnarNestedCopyTest` is the standing evidence that bridging them is a
real use case. 36 was the third of them and is settled: an `OPTIONAL` struct enclosing a
repeated field is what Arrow-based writers produce for a nullable struct holding a list and
this repository reads its own `nested_list_struct_test.parquet` fixture carrying it, so the
round trip was not available at all for that shape until the shredder levelled it. 24–25
follow, adding the optional index structures (page indexes and Bloom filters, with the
per-page statistics that make page-level pruning possible) on the settled surface:
capability that costs a caller nothing until it is asked for, which is what puts it behind
32 and 33. Increments 26–30 follow too, as refinements of what the
milestone settles rather than steps that depend on one another: 26 stops the dictionary
analysis being an encoding verdict — 26a declining a dictionary the prefix already shows is
losing, 26b rescuing one the cap cut short — 27 gives a row group a size a reader can plan
against, 28 makes the page target mean what it says for every column rather than the widest
one, 29 fills in the cardinality metadata for the chunks that cannot state it, and 30 hands
the caller a boundary the milestone leaves entirely to the byte target. None is sequenced
against 24–25, though 28's row cap is a decision better taken alongside 24. Only 30 among
them adds public API; 27 and 30 are the two ends of the same question — how large a row
group is, and where one ends — so a decision taken in either is worth carrying to the other.
Increment 31 stands apart from all of them: it depends on nothing after 19 and gates
everything, turning 14's extension contract into something the build checks. Increments
32–36 are one thread rather than five: each closes a place where the write API cannot
express something the read API can, so that code holding a file read through one can write
it through the other. 20a belongs to that thread by subject and is delivered inside
`1.1.0.Beta1` regardless, because the `created_by` relocation it carries stops being free
once that surface is released. The thread is delivered in two parts. 32, 33 and 36 are what a round
trip needs and are taken together, ahead of 24–25, for the reason given above. 36 is the one
entry in the thread that is not an API question — the shape is expressible in the input
contract and readable by the reader, and only the shredder refuses it — which makes it a
*Dimension* where the other two are *Layer*s, and the largest of the three. 34 and 35 are
the two reader accessors with no setter, and are independent of each other and of the rest:
34 is a decision about the `Object`-typed setter rule, and 35 is a Variant encoder that
happens to surface as a setter. Sequenced as separate later milestones, each its own design
and sequence: DataPage V2, the Avro write adapter, and a CLI write/convert command.

## User documentation

User-facing documentation under `docs/content/` is delivered at stage 20, the last
increment of `1.1.0.Beta1`, once every stage that shapes the writer's public surface has
landed: the row-oriented layer (16), the dictionary options (18), and the encoding policy
and codec values (19), which replace the last of those options with the policy that
subsumes it. Documenting the provisional `INT32`-only surface earlier would be
throwaway, and documenting it between 16 and 18 would mean writing the `WriterConfig`
option matrix twice, so deferring until the surface is settled is a recorded, intentional
exception to the CLAUDE.md rule that a new public API ships with a docs update — not an
oversight. Stage 15 changes only the default value of a `WriterConfig` option, which the
stage 20 reference page documents along with the rest.

Stage 20a is the one increment that reshapes what stage 20 documents rather than adding to
it, moving `created_by` from `WriterConfig` onto `ParquetFileWriter` and putting file
metadata beside it; it amends the reference page's option table and `created_by` section,
and is sequenced before the `1.1.0.Beta1` tag so the page is amended once rather than
published twice. Every other increment after stage 20 extends that surface additively, so
each carries its own docs update in the normal way: stage 22 adds an S3 `OutputFile`
factory, and stage 30 an `endRowGroup()` on `ParquetFileWriter`, which the how-to guide
covers alongside the size targets it complements. Stages 21 and 23 and the index stages
that follow are benchmarks, internal encode decisions and additive file structures with no
public API surface of their own.

## Roadmap reconciliation

`ROADMAP.md` remains the fine-grained capability inventory; this document is the plan
of record for writer architecture and sequencing. The mapping column above ties each
increment to the roadmap boxes it completes, so those boxes are ticked as increments
land. Phase 6 of `ROADMAP.md` points here.
