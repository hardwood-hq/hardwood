# Parquet rewrite capability (#791)

**Status: Proposed.** Tracking issue: #791. Depends on writer support (#9).

## Context

Hardwood can read an existing Parquet file and, once #9 is complete, write values to a
new one. Table-maintenance operations need a layer between those APIs: select data from
one or more existing files and produce a new file while preserving encoded bytes whenever
the requested transform does not change them.

The rewrite path covers two materially different operations:

- **Decode and re-encode** is the general fallback. It supports row-level filtering,
  computed or converted values, and changes to compression or encoding.
- **Byte copy** is the fast path. A complete surviving column chunk is copied without
  decompression or decoding, then its file-relative metadata is rebased to its new
  position.

The two paths may be mixed in one output file. Planning is per row group and column
chunk rather than per rewrite: one row group may be dropped, another copied, and another
decoded because only some of its rows survive.

This is a forward-only operation. Source files are random-access [InputFile] instances;
the destination is a sequential [OutputFile]. Data and auxiliary index blocks are emitted
before a newly serialized footer, using the same footer-last layout as `ParquetFileWriter`.

## Goals

- Rewrite one or more input files to one output file.
- Drop complete row groups using a row-group predicate without reading their pages.
- Apply an exact row predicate with decode/re-encode fallback when a row group only
  partially matches.
- Project complete leaf columns without decoding retained column chunks.
- Preserve encoded column-chunk bytes when schema, codec, encoding, and values are
  unchanged.
- Preserve supported file, row-group, and column metadata deliberately rather than
  silently losing fields.
- Carry forward `ColumnIndex` and rebase `OffsetIndex` on the byte-copy path.
- Bound memory and individual reads independently of input and column-chunk size.
- Make the chosen strategies observable and directly testable.

## Non-goals

- In-place modification of an input file.
- Combining pages from different source column chunks into one destination chunk.
- Repartitioning or sorting rows.
- Schema evolution beyond column projection in the first implementation.
- Copying encrypted column chunks before Hardwood models and writes Parquet encryption
  metadata.
- Treating a metadata field Hardwood cannot serialize as if it had been preserved.

## Public API

The rewrite API is separate from `ParquetFileWriter`: the ordinary writer owns page and
row-group formation, while the rewriter must preserve source row-group boundaries and can
emit already-encoded chunks.

```java
RewriteSpec spec = RewriteSpec.builder()
        .projection(ColumnProjection.columns("id", "payload"))
        .filter(FilterPredicate.gt("id", 100))
        .compression(CompressionPolicy.preserve())
        .metadata(MetadataPolicy.preserveSupported())
        .build();

RewriteResult result = ParquetRewriter.rewrite(inputs, output, spec);
```

The initial API consists of:

```java
public final class ParquetRewriter {
    public static RewriteResult rewrite(
            List<InputFile> inputs,
            OutputFile output,
            RewriteSpec spec) throws IOException;
}

public record RewriteResult(
        long inputRows,
        long outputRows,
        int copiedRowGroups,
        int decodedRowGroups,
        int droppedRowGroups,
        long copiedBytes) {}
```

`RewriteSpec` uses the existing `ColumnProjection` and `FilterPredicate` vocabulary.
One filter has two roles: row-group statistics first classify row groups as `NONE`, `ALL`,
or `MAYBE`; exact evaluation is only needed for `MAYBE`. `ALL` is available only when
the predicate evaluator can prove that every row matches, not merely that the group may
contain a match. Without that proof, the correct classification is `MAYBE` and the group
is decoded.

Compression is explicit:

- `preserve()` retains the source codec and permits copying.
- A concrete target codec forces decode/re-encode for chunks whose source codec differs.

The API does not expose a `fastPath` switch. The planner engages copying whenever it can
prove it legal; unsupported cases take the correct fallback.

## Ownership and failure behavior

Passing inputs and output to `rewrite` transfers ownership for the duration of the call.
The rewriter opens every input and validates metadata, schemas, projection, and the plan
before creating the output. This prevents a deterministic validation failure from touching
the destination.

After `OutputFile.create()` succeeds:

- success closes and commits the output;
- any planning, read, write, or footer failure calls `OutputFile.discard()`;
- all opened inputs are closed on both success and failure;
- close/discard failures are suppressed onto the primary failure.

Inputs must not identify the destination. The local backend additionally retains its
temporary-sibling and atomic-rename behavior, so a failed rewrite cannot replace a valid
destination.

## Planning model

`RewritePlanner` produces an immutable plan after inspecting every input footer. Its
units are:

```java
sealed interface RowGroupRewrite permits DropRowGroup, CopyRowGroup, DecodeRowGroup {}

record CopyColumnChunk(
        InputFile input,
        ColumnChunk source,
        int sourceLeafIndex,
        int destinationLeafIndex) {}
```

For each input row group:

1. Resolve the filter against the source schema and column orders.
2. Evaluate row-group statistics.
3. Emit `DropRowGroup` when no row can match.
4. Test every retained leaf column against the copy predicate.
5. Emit `CopyRowGroup` when all output chunks can be copied and all rows survive.
6. Otherwise emit `DecodeRowGroup`.

The planner records a reason when copying is rejected. Reasons remain internal initially,
but are used in tests and JFR events so an accidental fast-path regression is diagnosable.

### Copy predicate

A source column chunk can be copied only when all of the following hold:

- every row in its row group survives;
- the leaf is retained by projection;
- its physical type and complete logical schema path are unchanged;
- its codec is preserved;
- no encoding, page-version, value, or statistics rewrite was requested;
- it is not encrypted;
- all metadata required by the selected metadata policy is available and writable.

Projection alone does not require decoding. Retained chunks are copied in destination
schema order; omitted chunks have no data or `ColumnChunk` entry in the destination.
Projection must retain a structurally valid schema: selecting a nested leaf retains the
necessary ancestor groups, and list/map wrappers are not flattened.

## Schema reconciliation

The first input, after projection, defines the destination schema. Later inputs must be
compatible under the same strict rules used by Hardwood's multi-file reader: matching
field structure, repetition, physical and logical types, and leaf ordering. The planner
reports the input name and first mismatching field path before the output is created.

The initial implementation requires exact schemas before projection. Schema union,
missing-column synthesis, and type promotion are later features and necessarily use the
decode path.

## Byte-copy execution

For every `CopyColumnChunk`:

1. Compute `sourceStart = source.chunkStartOffset()`.
2. Take `source.metaData().totalCompressedSize()` bytes from the source.
3. Record `destinationStart = output.position()`.
4. Copy the interval in fixed-size blocks (initially 8 MiB) through
   `InputFile.readRange(long, int)` and `OutputFile.write(ByteBuffer)`.
5. Compute `delta = destinationStart - sourceStart` with exact arithmetic.
6. Construct destination metadata with every data-relative absolute offset shifted by
   `delta`.

The bounded transfer loop avoids materializing a complete chunk and respects the `int`
range length of `InputFile.readRange`. It also avoids a single large remote range request.
The loop validates source bounds before copying and uses `Math.addExact` for offset/length
arithmetic.

Rebased column-chunk fields are:

- `data_page_offset`;
- `dictionary_page_offset`, when present;
- `index_page_offset`, once represented;
- legacy `ColumnChunk.file_offset`, once represented.

`total_compressed_size`, `total_uncompressed_size`, encodings, codec, value count,
statistics, and key/value metadata are unchanged for a copied chunk.

## Page indexes and bloom filters

Auxiliary structures are written after all row-group data and before the footer. Their
destination offsets are therefore recorded independently of the column-chunk data delta.

### Column index

`ColumnIndex` contains no file offsets. Its Thrift structure is parsed and serialized
unchanged. Re-serialization instead of opaque byte copying keeps the behavior consistent
for local and range-backed inputs and validates malformed data before publishing output.

### Offset index

Every `OffsetIndex.PageLocation.offset` is rebased by the column chunk's `delta`.
Compressed page size and first-row index are unchanged. The rebased structure is serialized
to the auxiliary region and its new offset/length are stored on the destination
`ColumnChunk`.

An index pair is carried only when both required source ranges were fetched and parsed.
If the source omitted an index, the destination omits it. `MetadataPolicy` determines
whether an unavailable optional index may be dropped or must fail the rewrite; it is never
represented by a stale source offset.

### Bloom filter

Bloom-filter bytes do not embed page offsets, so a supported filter can be copied as an
opaque bounded range and its `bloom_filter_offset` changed to the new auxiliary position.
If its length is absent and cannot be derived safely, `preserveSupported()` drops it and
`requirePreservation()` fails during planning.

## Metadata fidelity prerequisite

The current reader model and Thrift writers do not yet round-trip every footer field needed
by a general rewrite. Before byte-copy rewrite is public, metadata support must be completed
for the fields the chosen policy promises:

- `FileMetaDataWriter`: key/value metadata and column orders;
- `RowGroup`: sorting columns, file offset, total compressed size, and ordinal;
- `ColumnChunk`: file path, legacy file offset, and supported crypto metadata;
- `ColumnMetaData`: index page offset and any remaining represented Parquet fields;
- writers for `ColumnIndex` and `OffsetIndex`.

Unknown Thrift fields cannot be preserved by the typed model. `preserveSupported()` means
preserve every field represented by Hardwood and deliberately omit unsupported optional
fields; `requirePreservation()` rejects a source containing a recognized but unwritable
field. Encryption is rejected until fully modeled rather than copied with incomplete
metadata.

File-level metadata is reconciled as follows:

- format version must be compatible across inputs;
- key/value entries common with equal values are preserved;
- conflicting entries are rejected under `requirePreservation()` and omitted under
  `preserveSupported()`;
- column orders follow the projected leaf order and must agree across inputs;
- `created_by` identifies Hardwood's rewrite implementation rather than claiming that the
  newly assembled file was emitted by the source writer.

## Decode/re-encode fallback

The fallback is delivered after #9 supports every physical and nested shape accepted by
the reader. It reads only the projected payload columns plus columns required by the exact
predicate, evaluates the predicate in batches, compacts surviving values, and submits the
projected batch to `ParquetFileWriter`'s internal row-group encoder.

The rewriter must be able to preserve a planned destination row-group boundary. Therefore
the shared writer machinery gains an internal encoded-row-group sink instead of routing
through the public `writeBatch` size heuristic. Public `ParquetFileWriter` behavior remains
unchanged.

Dictionary state never crosses a source row-group boundary. A decoded group may produce
different pages and encodings, which is expected; its footer metadata is generated by the
writer rather than rebased from the source.

## Package layout

```text
dev.hardwood.rewrite/
├── ParquetRewriter          public entry point
├── RewriteSpec              public immutable specification + builder
├── RewriteResult            public execution summary
├── CompressionPolicy        public codec policy
└── MetadataPolicy           public preservation policy

dev.hardwood.internal.rewrite/
├── RewritePlanner
├── RewritePlan
├── RowGroupRewrite
├── ChunkCopier
├── MetadataRebaser
├── AuxiliaryIndexWriter
└── DecodeRewriteExecutor
```

The implementation reuses `ParquetMetadataReader`, `FileSchema`, predicate resolution and
statistics evaluation, the existing schema compatibility logic, `ThriftCompactWriter`, and
the writer's footer-last/output-discard flow. Rewrite-only policy and byte transfer do not
belong in reader or writer packages.

## Delivery stages

### Stage 1: metadata serialization foundation

- Complete typed metadata fields and their Thrift readers/writers.
- Add `ColumnIndexWriter` and `OffsetIndexWriter` with reader/writer round-trip tests.
- Add footer differential tests against parquet-java/PyArrow fixtures.

### Stage 2: single-file whole-row-group copy

- Add planner, bounded `ChunkCopier`, `MetadataRebaser`, and footer assembly.
- Support all-columns projection and whole-row-group keep/drop.
- Omit unavailable optional indexes under `preserveSupported()`.

This stage has no dependency on value-type support in #9 because it never decodes pages.

### Stage 3: projection and multi-file merge

- Build projected schema trees while retaining required ancestors.
- Copy retained chunks in destination order.
- Reuse strict multi-file schema validation.
- Reconcile file metadata and concatenate planned row groups.

### Stage 4: index and bloom-filter carry-forward

- Re-serialize `ColumnIndex`.
- Rebase and serialize `OffsetIndex`.
- Copy supported bloom filters and update their offsets.

### Stage 5: decode/re-encode fallback

- Bridge batch readers to the completed writer value-source seam.
- Apply exact row filters and requested codec/encoding changes.
- Permit mixed copy/decode output and expose strategy counts/JFR events.

## Verification

### Unit tests

- `MetadataRebaserTest`: positive, zero, and negative deltas; dictionary/no-dictionary;
  exact-arithmetic overflow.
- `ChunkCopierTest`: multi-block transfer, short final block, source bounds, injected read
  and write failures.
- `RewritePlannerTest`: every copy-predicate condition and rejection reason.
- `OffsetIndexWriterTest` / `ColumnIndexWriterTest`: reader-writer round trips and malformed
  input rejection.
- Projection tests for flat fields, structs, lists, maps, and invalid partial wrappers.

### End-to-end tests

- Copy a file and assert every destination column chunk's bytes equal its source chunk.
- Drop leading, middle, and trailing row groups and read all remaining rows.
- Project columns and verify the output schema and values with Hardwood and parquet-java.
- Merge multiple matching files and verify row-group order and total row count.
- Reject incompatible schemas before creating the output.
- Carry indexed and non-indexed inputs; verify every rebased page location addresses a
  valid page header.
- Carry or deliberately drop bloom filters according to metadata policy.
- Mix copied and decoded row groups once fallback exists.
- Inject failure after output creation and assert `discard()` leaves no published file.
- Differentially inspect footers with parquet-java and PyArrow.

### Performance tests

- JMH throughput for copy versus decode/re-encode over uncompressed, Snappy, and ZSTD
  fixtures.
- Allocation profile proving memory is bounded by the transfer block, not chunk size.
- Local and range-backed input runs to catch accidental whole-chunk reads.

## Documentation

When the API ships:

- add `docs/content/how-to/rewrite.md` for compaction, projection, and filtering tasks;
- add rewrite types to `docs/content/reference/packages.md` and JavaDoc navigation;
- extend `docs/content/concepts/parquet-layout.md` with why whole column chunks can be
  copied and which offsets must move;
- document preservation policies and unsupported encryption in reference material.

The design document remains implementation-oriented under `_designs/`; user documentation
follows the existing Diátaxis split.
