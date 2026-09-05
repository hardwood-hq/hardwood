# Codec and encoding breadth (#9, stage 19)

**Status: Completed** (19a codecs, 19b encodings). Tracking issue: #976. Delivery stage 19
(Breadth) of [WRITER_SUPPORT.md](WRITER_SUPPORT.md).

## Context

The writer produces two codecs and two value encodings, and the surface around it promises
more than that.

`CompressorFactory` yields `UNCOMPRESSED` and `ZSTD`; every other member of
`CompressionCodec` reaches its `default` branch. `WriterConfig.codec` therefore accepts
values it exists only to reject, and the rejection reads "not yet supported" for a codec
that will never be written as much as for one that simply has no encoder yet. The read
path meanwhile decompresses GZIP, SNAPPY, ZSTD, LZ4, LZ4_RAW and BROTLI, refusing only
LZO, so the two directions of the same library are asymmetric for no reason the code
states.

A column chunk is `RLE_DICTIONARY` or `PLAIN`, chosen once the row group is buffered by
the size comparison stage 18 settled. `internal/encoding/` holds decoders for
`DELTA_BINARY_PACKED`, `DELTA_LENGTH_BYTE_ARRAY`, `DELTA_BYTE_ARRAY` and
`BYTE_STREAM_SPLIT` and encoders for none of them: Hardwood reads four encodings it cannot
write, and a caller with a sorted timestamp column or a float column destined for
compression has no way to ask for the encoding that suits it.

This stage closes both gaps. It sits inside the series the stage 14 interop gate covers,
because a codec and an encoding are exactly the axes that gate sweeps, and it lands before
stage 20 so the writer's options are documented once in their final form.

## Scope

In scope:

- Every `CompressionCodec` member is either produced or refused with a reason specific to
  why, decided before any byte of the file is written.
- Encoders for the four optional encodings, each the inverse of the decoder beside it.
- One encoding policy per leaf column on `WriterConfig`, replacing the
  `enableDictionary` boolean, and the validation that makes a mis-addressed or
  type-illegal policy a creation-time failure rather than a silent no-op.
- The interop gate's codec and encoding axes, and the differential suite, extended with
  everything above.

Out of scope, each listed under [What is deliberately not here](#what-is-deliberately-not-here).

### Increment split

Two stacked increments, each shippable:

- **19a — codecs.** `CompressorFactory` completes, the refusals gain their reasons, and the
  gate's codec axis grows. No public API changes: `WriterConfig.codec` already accepts the
  whole enum.
- **19b — encodings.** The four encoders, the `WriterConfig` surface that selects them —
  which subsumes `enableDictionary` and removes it — and the gate's encoding axis.

## Why this is configuration and not schema

Neither a codec nor an encoding is part of a Parquet schema. `SchemaElement` carries
`type`, `type_length`, `repetition_type`, `name`, `num_children`, `converted_type`, `scale`,
`precision`, `field_id` and `logicalType`, and nothing about storage. `ColumnMetaData`
carries `encodings` and `codec`, and it describes **one column chunk** — one column of one
row group. The page headers below it carry an encoding of their own, per page. A conformant
file may compress a column with `ZSTD` in one row group and `SNAPPY` in the next.

Both therefore belong to `WriterConfig` rather than to `FileSchema`:

- `FileSchema` is bidirectional. It is what `ParquetFileReader.getFileSchema()` returns, and
  a storage field on it would have no answer on the read side, where chunks of one column may
  disagree and pages disagree underneath them. A file written with a schema would also stop
  round-tripping to an equal one.
- The two vary independently. One schema is written hot and to cold archive with different
  codecs; one configuration serves many schemas. Binding them forces a schema copy per
  storage decision.

The coupling does not disappear, it relocates: a per-column policy is keyed by leaf path, so
a configuration carrying one is meaningful only against a schema that has that column. Both
are in hand at `ParquetFileWriter.create`, which is where path resolution and type legality
are checked.

What the writer does with the format's per-chunk granularity is a separate question from who
controls it. `AUTO` already re-decides per chunk — a column may be `RLE_DICTIONARY` in one
row group and `PLAIN` in the next, each chunk measured on what it holds — while a named
policy holds for every chunk of its column, and the codec is uniform for the whole file. The
caller cannot address a single chunk, and nothing in this stage changes that: row-group
boundaries follow from `rowGroupBufferTargetBytes` and the data, so there is no stable chunk for a
configuration to name. Stage 30 (#985) is where that changes — a caller-placed boundary is
an addressable point, and the configuration it carries supersedes this one for the row groups
after it, so what is settled here becomes what a file *starts* with rather than what it is
stuck with. The other form of per-chunk variation is a decision the writer takes from what it
has buffered, which is what stage 18 is for encoding and what per-chunk codec choice would be
for compression.

## Codecs

### What is produced

| Codec | Library | Write side |
|---|---|---|
| `UNCOMPRESSED` | — | the body as it stands |
| `ZSTD` | zstd-jni (optional) | `Zstd.compressByteArray` at the library's default level |
| `GZIP` | JDK | `Deflater` in the gzip wrapper — no dependency to add |
| `SNAPPY` | snappy-java (optional) | `Snappy.rawCompress`: the raw block form Parquet specifies, not the framed stream |
| `LZ4_RAW` | lz4-java (optional) | `LZ4Factory.fastestInstance().fastCompressor()`, raw block |
| `BROTLI` | brotli4j (optional) | brotli4j's encoder, behind `Brotli4jLoader.ensureAvailability()` |

Each optional library is gated with `CodecLibraries.require(...)` and the `write` action,
the way `ZSTD` already is, so a missing library names the Maven coordinates to add. The
page header records the uncompressed size, so every codec here is used in its
length-unaware raw form.

### What is refused, and why

Two codecs are refused permanently, and their messages say so rather than implying a later
increment:

- **`LZ4`** — the Hadoop-framed variant, deprecated by the format in favour of `LZ4_RAW`.
  Producing it would put new files in a shape the format tells readers to stop expecting.
  The reader keeps decompressing it, because files written before the deprecation exist;
  the writer points the caller at `LZ4_RAW`.
- **`LZO`** — no maintained JVM implementation under a licence the project can depend on.
  The read path refuses it for the same reason, and the two directions now agree.

### Where a codec fails

Resolution stays where it is: `ParquetFileWriter.create` resolves the `Compressor` before
`out.create()`, so a refused codec or a missing library fails before the magic bytes are
written and never leaves a partial file behind.

### Compression level

Each codec compresses at its library's default, which is what `ZstdCompressor` does today
and what the reference implementations write. No level knob is added: it multiplies the
option surface by the codec list, and the codec choice already expresses the
speed-versus-ratio trade-off at the granularity callers ask for it.

## Encodings

### One policy per column

Every leaf column carries an **encoding policy**, resolved at writer creation from the most
specific setting that names it:

1. the policy configured for that column's leaf path, else
2. the file-wide default, else
3. `AUTO`.

`AUTO` is the stage 18 decision: `RLE_DICTIONARY` where the dictionary body plus its index
stream is smaller than the values `PLAIN`, `PLAIN` otherwise, taken per chunk once its row
group is buffered. Every other policy names an encoding outright, and a column that names
one builds no dictionary at all — its chunks carry that encoding in every row group of the
file.

That is the writer's entire encoding configuration: there is no separate switch for the
dictionary. Dictionary-or-`PLAIN` is the whole of what `AUTO` decides, so a boolean beside the
policy would be a second spelling of one decision, with a precedence rule between the two to
document and the per-column and file-wide scopes split across them. Declining a dictionary is
therefore naming `PLAIN` — file-wide for the whole file, or for the one column that wants it.

Delta and byte-stream-split are not candidates for `AUTO`. Its comparison works
because both of its candidates' sizes follow from what pass 1 already retains — the exact
cardinality, the dictionary's plain bytes, and every value's plain width — so it costs
arithmetic rather than a trial encode. Neither optional encoding has that property.
`DELTA_BINARY_PACKED`'s size follows from the bit width of the deltas inside each 32-value
miniblock, which is a property of value *order* that nothing in the buffer measures, and
`BYTE_STREAM_SPLIT` does not change a page's size at all — it reorders bytes so that the
codec running afterwards finds structure, which makes its payoff a property of the codec,
not of the values. Selecting either automatically means trial-encoding candidates per
chunk, or a heuristic with nothing to calibrate it against. The writer therefore honours an
explicit choice and does not guess; guessing well is a refinement on top of the numbers
this stage's benchmark produces, sequenced after the milestone with the other refinements.

### The configuration surface

```java
WriterConfig config = WriterConfig.builder()
        .encoding("ts", ColumnEncoding.DELTA_BINARY_PACKED)
        .encoding("readings.list.element", ColumnEncoding.BYTE_STREAM_SPLIT)
        .build();

// Every column PLAIN, and so no dictionary anywhere in the file:
WriterConfig plain = WriterConfig.builder()
        .encoding(ColumnEncoding.PLAIN)
        .build();
```

`ColumnEncoding` is a new public enum in `dev.hardwood.writer` holding exactly the policies
a caller may ask for — `AUTO`, `PLAIN`, `DELTA_BINARY_PACKED`, `DELTA_LENGTH_BYTE_ARRAY`,
`DELTA_BYTE_ARRAY`, `BYTE_STREAM_SPLIT` — rather than reusing `dev.hardwood.metadata.Encoding`,
which is the read side's vocabulary for what a file *says* and carries members no writer
input should accept (`PLAIN_DICTIONARY`, `BIT_PACKED`, `UNKNOWN`). The accepted set is then
a compile-time fact instead of a rejection list, and `AUTO` — which is a policy, not an
encoding, and has no `Encoding` member — has somewhere to live.

`Builder.encoding(ColumnEncoding)` sets the file-wide default and
`Builder.encoding(String columnPath, ColumnEncoding)` overrides one leaf column;
`WriterConfig.defaultEncoding()` and `WriterConfig.columnEncodings()` expose them, the
latter unmodifiable, with `DEFAULT_ENCODING` the `AUTO` that applies when neither is set. A
column is addressed by its **dotted leaf path** as the schema spells it, synthetic
`list.element` and `key_value.key` segments included, because that is what identifies a leaf
unambiguously in a schema that may repeat a name at several depths.

The two scopes are the same setting, so neither is special: a file-wide `PLAIN` says what
the removed boolean said, and a file-wide `DELTA_BINARY_PACKED` is meaningful over a schema
of nothing but integers. The codec stays file-wide only, since a page body compresses the
same way whatever its type.

### What may be asked for

| Policy | Physical types |
|---|---|
| `AUTO` | every writable type |
| `PLAIN` | every writable type |
| `DELTA_BINARY_PACKED` | `INT32`, `INT64` |
| `DELTA_LENGTH_BYTE_ARRAY` | `BYTE_ARRAY` |
| `DELTA_BYTE_ARRAY` | `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` |
| `BYTE_STREAM_SPLIT` | `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `FIXED_LEN_BYTE_ARRAY` |

`AUTO` and `PLAIN` are the two universal policies: no data can make either impossible.
`AUTO` on a `BOOLEAN` column resolves to `PLAIN`, that type being outside dictionary
encoding as it has been since stage 9.

There is no policy that demands a dictionary. A dictionary is the one encoding the writer
cannot promise — a chunk whose values repeat too little for one to pay has to be written
some other way — so a `DICTIONARY` member would mean either failing a write over data the
writer can encode perfectly well, or accepting a request and not honouring it. Dictionary
encoding stays what stage 18 made it: `AUTO`'s size-decided outcome, declined by naming
another policy.

### Validation

Split by what each side can see:

- **`WriterConfig.Builder`** rejects a null path or policy. There is nothing else it can
  check without a schema — every `ColumnEncoding` member is writable by construction.
- **`ParquetFileWriter.create`** resolves each leaf column's policy and rejects a path
  matching no leaf column of the schema, and a policy illegal for the column's physical
  type. The first message lists the schema's leaf paths, because a path that matches
  nothing is a typo whose only other effect would be to write the file in a different
  encoding than the caller asked for. The second names the column, its physical type and
  the policy, and it fires for the file-wide default exactly as it does for an override: a
  file-wide `BYTE_STREAM_SPLIT` over a schema holding one `BYTE_ARRAY` column is a
  contradiction, and the alternative — quietly resolving that column to something else —
  is the silent divergence the check exists to prevent. A mixed schema states its intent
  per column.

The check runs in the loop that already validates physical types, next to codec
resolution, so a misconfigured file fails before it exists.

### Interaction with the rest of the writer

| Aspect | A chunk of a column whose policy is not `AUTO` |
|---|---|
| Dictionary analysis | Not run. Values go straight to the value store, so the chunk pays neither the interning nor the index array |
| The size probes | Not run: they weigh a dictionary this chunk never builds |
| `Statistics.distinct_count` | Absent, as for any chunk that counted nothing — the same position stage 29 addresses |
| `min` / `max` / `null_count` | Unchanged: statistics are accumulated from values, independently of encoding |
| Level streams | Unchanged: `RLE`, length-prefixed, ahead of the value section |
| Page cuts | Unchanged: planned from retained plain widths, which stage 28 revisits for every column |
| Row-group flush | Unchanged: the target bounds buffered uncompressed bytes, which is a memory bound, not a size prediction |

A chunk under a named policy still accumulates each value's `PLAIN` width as it arrives, even
though no comparison will consult it. That running total is what `bufferedBits()` sums to
decide when the row group has reached its target and what page cuts are planned from; it is
the writer's measure of buffered data, not a term of the `AUTO` comparison alone, and
dropping it for policied columns would silently unbound a file written entirely under one.

The policy is what `RowGroupBuffer`, `ColumnChunkBuffer` and `ValueEncoder.forColumn` pass
down: each column chunk is constructed with its resolved `ColumnEncoding`, and whether it
builds a dictionary at all follows from that policy being `AUTO` and the type being
dictionary-capable.

### Page independence

A V1 data page must decode without the page before it, since a reader may seek to any page.
Each page therefore encodes its own value range standalone: its own `DELTA_BINARY_PACKED`
header and first value, its own `DELTA_BYTE_ARRAY` prefix baseline — the first value of a
page has prefix length zero — and its own K byte streams for `BYTE_STREAM_SPLIT`. The
chunk-level state is the encoding choice and nothing else.

### Where the encoders live

`internal/encoding/` gains `DeltaBinaryPackedEncoder`, `DeltaLengthByteArrayEncoder`,
`DeltaByteArrayEncoder` and `ByteStreamSplitEncoder`, each the inverse of the decoder
beside it and each producing one page's value section from a range of the chunk's stored
values. The byte-array encoders need no new value retention: `BinaryValueEncoder` already
stores a chunk's values packed end to end with their offsets, which is the layout a length
stream and a prefix comparison both want.

The one piece of shared machinery is the bit-packer. `RleBitPackingHybridEncoder.packGroup`
packs eight values at a fixed width, LSB-first, and a delta miniblock is the same layout over
32 values — so the packing moves out of that class into a helper both use, rather than being
written a second time. Two properties it must carry that the eight-value form does not need:
widths up to 64 bits, for an `INT64` column whose deltas span the type, and a `long` value
domain. A miniblock of 32 values is a whole number of bytes at every width, as a group of
eight is, so neither caller has a partial byte to handle.

`ValueEncoder.encodePlain(from, count)` — the per-type call that produces that section
today — generalizes to an encoding-carrying form. A concrete encoder implements the
encodings its physical type may carry and rejects the rest as an illegal state, a
combination configuration validation has already excluded. `ColumnChunkBuffer` keeps its
dictionary branch and delegates everything else to that call, so the type-agnostic half —
levels, page cutting, compression, CRC — is untouched.

Encoding specifics that the format pins and the tests hold:

- `DELTA_BINARY_PACKED` writes 128-value blocks of four 32-value miniblocks. A trailing
  block writes zero as the bit width of each miniblock it does not need and no bytes for
  them; a miniblock holding any value is written whole, the decoder taking its length from the
  width alone, so a partly-filled one is padded with the block minimum.

  Deltas are computed with wrap-around at the column's width, so an `INT32` column spanning the
  full range encodes without its deltas overflowing. `DeltaBinaryPackedDecoder` reconstructs
  such a column by accumulating in a `long` and narrowing with a cast, and that narrowing is
  what recovers the value: the low bits of the sum are the type's own arithmetic either way.
  The existing decoder needs no change to read this — the alternating-extremes cases in the
  round-trip suite are what hold the pair together.

  **The block minimum is signed and the residues unsigned**, and the two are not
  interchangeable. Taking the minimum as a signed value is what keeps a descending column cheap:
  its deltas are negative, and measuring them against a negative minimum leaves residues near
  zero. Comparing them unsigned instead makes every negative delta look enormous and drives the
  bit width to the full type — which round-trips perfectly and costs an order of magnitude more
  than it needs to. Encoding size is therefore asserted, not just symmetry: a uniform step must
  pack into a fraction of a byte per value.
- `DELTA_LENGTH_BYTE_ARRAY` is the lengths as `DELTA_BINARY_PACKED` followed by the
  concatenated bytes; `DELTA_BYTE_ARRAY` is the prefix lengths, then the suffixes in that
  same form. For `FIXED_LEN_BYTE_ARRAY` the suffix lengths are still written, the values
  being fixed-length only by schema.
- `BYTE_STREAM_SPLIT` scatters K streams where K is the type's byte width, `typeLength` for
  `FIXED_LEN_BYTE_ARRAY`. The streams carry present values only, so a page's stream length
  is its non-null count times K.

### Metadata

`ColumnMetaData.encodings` becomes exactly what the chunk uses: `RLE` where the column is
levelled, the chunk's value encoding, and `PLAIN` additionally only where a dictionary page
carries it. It currently lists `PLAIN` unconditionally, which was true while `PLAIN` was
either the value encoding or the dictionary body and stops being true here. Each data page
header carries the chunk's value encoding, as it does today.

## Validation

- **Round trip through Hardwood.** Every (policy, legal physical type) pair, in each
  repetition shape and inside a nested column, written and read back through the decoder
  that already exists for it. Every codec likewise: `core`'s optional codec libraries are
  on its own test classpath, so all six are exercisable there.
- **Policy resolution**, on its own: an override beating the file-wide default, the default
  beating `AUTO`, a file-wide `PLAIN` producing a file with no dictionary page anywhere, and a
  policy on one column leaving its neighbours on `AUTO` and their dictionaries intact.
- **Encoder / decoder symmetry**, per encoder, over the edges: a single value; all values
  equal; alternating `Integer.MIN_VALUE` / `MAX_VALUE` and `Long.MIN_VALUE` / `MAX_VALUE`
  to pin delta wrap-around; a range shorter than one miniblock and one spanning several
  blocks; empty and single-byte byte arrays; a fully shared prefix and no shared prefix for
  `DELTA_BYTE_ARRAY`; `NaN` and `-0.0` for byte-stream-split floats.
- **The interop gate.** Its codec axis gains `GZIP`, `SNAPPY` and `LZ4_RAW`, and its
  encoding axis gains one case per (encoding, type) pair, read back through parquet-java.
  A pair the pinned parquet-java cannot read is not a Hardwood defect, but neither is it
  coverage: it is recorded here with the reader version that gates it rather than dropped
  from the sweep silently.

  `BROTLI` is the one such case on the codec axis. parquet-java resolves a codec to a Hadoop
  codec class name, and the name it carries for `BROTLI` is
  `org.apache.hadoop.io.compress.BrotliCodec` — a class in neither parquet-java 1.17.1 nor
  Hadoop itself, whose `io.compress` package holds BZip2, Default, Deflate, Gzip, Lz4,
  Passthrough, Snappy and ZStandard and no brotli. The only artifact providing it is
  `com.github.rdblue:brotli-codec`, unmaintained and carrying native binaries for a few
  platforms, so putting it on the gate's classpath would make the gate's verdict depend on the
  architecture running it.

  This is one hole in parquet-java rather than a write-path gap: the read side has it already,
  `large_string_map.brotli.parquet` being the corpus's only `BROTLI` file and sitting in the
  comparison suite's skip list for exactly this `ClassNotFoundException`. The codec's
  independent reader is DuckDB in the differential suite, and the gate asserts the class's
  absence, so a parquet-java that gains it fails there and `BROTLI` rejoins the axis rather
  than staying out by inertia.

  Its existing "dictionary disabled" case becomes a file-wide
  `PLAIN` policy, which is the same file by a different name. The pinned parquet-java
  (1.17.1) carries byte-stream-split readers for `FLOAT`, `DOUBLE`, `INT32`, `INT64` and
  `FIXED_LEN_BYTE_ARRAY`, so every pair in the table above is coverable there.
- **The second writer-identification heuristic.** parquet-java gates `DELTA_BYTE_ARRAY` on
  who wrote the file: `CorruptDeltaByteArrays.requiresSequentialReads(ParsedVersion, Encoding)`
  forces sequential reads for that encoding when the writer is one whose PARQUET-246 defect it
  knows, or one it cannot identify. It is PARQUET-251's counterpart from stage 14, reached by a
  different encoding, and the gate asserts it the same way: for a hardwood-written
  `DELTA_BYTE_ARRAY` chunk the answer is false, which is what a parseable `created_by`
  (stage 15) buys on this path.
- **The differential suite.** `WriterDifferentialTest` gains the same pairs, so a second
  independent reader sees them. DuckDB reads all six produced codecs, which is what keeps
  `BROTLI` covered where the gate cannot reach it.

  The two readers cover byte-stream-split between them rather than each alone. DuckDB 1.4.4
  accepts the encoding for `FLOAT` and `DOUBLE` only, rejecting `INT32`, `INT64` and
  `FIXED_LEN_BYTE_ARRAY` outright — a restriction predating parquet-format 2.10, which added
  them. Those three are read back through parquet-java on the gate instead, so every pair in
  the table above has an independent reader; what varies is which one.
- **The refusals.** `LZ4` and `LZO` fail at creation with their own reasons; so do a column
  path matching no leaf, a policy illegal for its column's physical type, and a file-wide
  default illegal for any one column of the schema.

## What it buys

- Delta encodings target the columns a dictionary serves worst: sorted or near-sorted
  integers (timestamps, identifiers, counters) and byte arrays sharing prefixes (paths,
  URLs, keys). On unordered data they are larger than `PLAIN`, which is why the choice is
  the caller's.
- Byte-stream-split changes no page's size on its own. Its entire effect is on what the
  codec afterwards achieves over floating-point data, so the benchmark reports it only in
  combination with each codec, never alone.
- `FlatWriteBenchmark`'s codec dimension widens from two values to the five both writers
  produce, and it continues to report produced file size next to the time — which is the whole
  point of a codec dimension, a codec being a trade of one against the other. `BROTLI` stays
  out for the reason it is out of the gate: parquet-java cannot resolve the codec, so including
  it would report one contender against nothing. Its taxi-shaped fixture already carries the
  timestamp and string columns these encodings target, so the stage's result is measured on the
  same fixture stage 18's was.

## What is deliberately not here

Each reachable from this shape and sequenced separately:

- **Automatic selection of delta / byte-stream-split**, for the reason given above, and
  the trial-encode or order-statistic that would make it decidable.
- **`RLE` for `BOOLEAN` data pages**, the one remaining encoding a written column could
  carry.
- **Forcing a dictionary**, which would need a promise the size comparison cannot keep.
- **Per-chunk codec choice**, which `ColumnMetaData.codec` permits, for a chunk found
  incompressible — a decision the writer would take from the buffered data at flush, as it
  takes `AUTO`'s, not one the caller names.
- **Compression levels**, per codec.
- **libdeflate (FFM) for GZIP compression**, which the read path already uses for
  decompression.
- **DataPage V2**, its own milestone. Delta encodings pay more there, where level streams
  stay uncompressed beside a compressed value section.
- **`encoding_stats`** in the column metadata, which increment 37 writes.

## User documentation

Stage 20 documents the settled writer surface, this stage's `codec` and `encoding(path,
…)` among it, in one `how-to` guide and one `reference` page. Nothing here reaches a
published page before then, so the options are described once in their final form.
