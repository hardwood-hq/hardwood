# Writer documentation (#9, stage 20)

**Status: Implemented.** Tracking issue: #9. Delivery stage 20 (Docs) of
[WRITER_SUPPORT.md](WRITER_SUPPORT.md).

## Context

Stages 1–19 deliver a writer that is conformance-gated against parquet-java, ergonomic
through both a columnar and a row-oriented entry point, and optimal in its encoding
choice. None of it appears under `docs/content/`: the site describes a reader, the
how-to section opens with "Read Parquet files with Hardwood", and `reference/packages.md`
does not list `dev.hardwood.writer`. The public writer API is reachable only through the
generated JavaDoc.

This stage documents the writer as a first-class half of the library: how to write with
each entry point, what every knob does, and the model behind the file the writer
produces. Merging to `main` refreshes the `dev` docs only — the site's `latest` alias
stays on 1.0 until the 1.1 release is published — so the reader-only framing is corrected
here rather than in the release PR.

## Surface to document

| Area | API |
|---|---|
| Destination | `dev.hardwood.OutputFile` — `of(Path)`, `create`, `write`, `position`, `discard`, `close`; the file is valid only after `close()` returns |
| Writer | `ParquetFileWriter.create(out, schema)` / `create(out, schema, config)`, `writeBatch(Consumer<ColumnBatch>)`, `rowWriter()`, `close()`; one file is written through one of the two APIs |
| Columnar input | `ColumnBatch` — `ints` / `longs` / `floats` / `doubles` / `booleans` / `bytes` / `fixed`, each by column index or name, each with a `Validity` and a `boolean[]` null overload; `struct(path, Validity)`, `list(path, offsets[, Validity])`, `map(path, offsets[, Validity])` |
| Row input | `RowWriter.writeRow(Consumer<StructBuilder>)`; `StructBuilder` typed setters by name and by field index, including the logical-type setters (`setString`, `setDate`, `setTime`, `setTimestamp`, `setLocalTimestamp`, `setDecimal`, `setUuid`, `setInterval`, `setBinary`), `setNull`, and the nesting fillers `setStruct` / `setList` / `setMap`; `ListBuilder`, `MapBuilder`; `getFieldCount()` / `getFieldName(int)` |
| Schema | `FileSchema.builder(name)` — `addColumn` overloads (type length, logical type), `struct`, `list`, `map`, and `ElementBuilder` for list elements and map values |
| Configuration | `WriterConfig` — `pageTargetBytes` (1 MiB), `rowGroupTargetBytes` (128 MiB), `codec` (`ZSTD`, `UNCOMPRESSED` when the ZSTD library is absent), `encoding` file-wide and per leaf path (`AUTO`), `statisticsTruncationLength` (64), `createdBy`, `precisionLossPolicy` (`REJECT`) |
| Enums | `ColumnEncoding`, `PrecisionLossPolicy`, the produced and refused `CompressionCodec` values, `Validity` |

Out of scope, because the feature is: the S3 `OutputFile` backend (stage 22), page-index
and Bloom-filter writing (stages 24–25), the Avro write adapter and a CLI write command
(later milestones), and `dev.hardwood.internal.writer.*`.

## Page inventory

| Page | Kind | Holds |
|---|---|---|
| `how-to/write-row-by-row.md` | How-to | Writing records through `RowWriter` |
| `how-to/write-column-by-column.md` | How-to | Writing columns through `ColumnBatch` |
| `reference/writer.md` | Reference | Every writer fact to look up: config knobs, encodings, codecs, setter map, validation |
| `concepts/write-model.md` | Concepts | Why the file is produced the way it is, and what that costs |

Navigation, in `docs/mkdocs.yml`:

```yaml
  - How-to Guides:
    …
    - Read Geospatial Columns: how-to/geospatial.md
    - Write Row by Row: how-to/write-row-by-row.md
    - Write Column by Column: how-to/write-column-by-column.md
    - Inspect File Metadata: how-to/metadata.md
  - Concepts:
    - How a Parquet File Is Laid Out: concepts/parquet-layout.md
    - The Write Model: concepts/write-model.md
    …
  - Reference:
    - Configuration: reference/configuration.md
    - Writer: reference/writer.md
    …
```

## Page contracts

### `how-to/write-row-by-row.md` — "Write Row by Row"

The goal-oriented path for a caller holding records.

1. Three-step opener: build a `FileSchema`, open an `OutputFile`, create the writer —
   one snippet, closing both in try-with-resources, with a sentence on why the file is
   valid only after `close()`.
2. `writeRow` with the typed setters by name, covering a primitive, a `String`, a
   `LocalDate`/`Instant` and a `BigDecimal` so the logical-type conversion is visible.
3. Nulls: `setNull`, and that a field left unset is null in an `OPTIONAL` column and an
   error in a `REQUIRED` one.
4. Nesting: `setStruct`, `setList`, `setMap` with the nested fillers, including a list of
   structs, and the point that fields are addressed by their user-visible names — no
   `list.element` or `key_value` path segments.
5. By-index addressing: `getFieldCount()` / `getFieldName(int)` and the index setters, for
   a caller driving the writer from its own column loop. Carries the one thing a reader
   of both APIs gets wrong, as an admonition linking to `concepts/write-model.md`: the
   reader's index is a position in *projected* schema order, the writer's is declaration
   order in the schema being written, so the two agree only for a whole-file read into
   the file's own schema.
6. Configuration pointer: one `WriterConfig` snippet (codec plus row-group target) and a
   link to `reference/writer.md` for the rest.
7. Experimental admonition, matching `how-to/column-reader.md`: `RowWriter` and its
   builders are `@Experimental`.

Excluded: rationale for the row/columnar split, memory behaviour, encoding internals.

### `how-to/write-column-by-column.md` — "Write Column by Column"

The goal-oriented path for a caller holding columns.

1. The same three-step opener, then `writeBatch(batch -> …)` with typed arrays addressed
   by name and by index.
2. Batch rules stated as rules: every column's array has the same length, that length is
   the batch's row count, a ragged batch is rejected, and each column is set once per
   batch by either index or name.
3. Nulls: the mask-less setter is the all-present form; `Validity.ofNulls(boolean[])` and
   `Validity.of(long[])`; the values array stays full length and the slot at a null row is
   ignored; a null mask on a `REQUIRED` column is rejected.
4. Variable-width values: `bytes(...)` takes `byte[][]`, and a `STRING` column is written
   as UTF-8 bytes — `ColumnBatch` has no `strings(...)` overload; `fixed(...)` values must
   match the column's declared length.
5. Nesting: `struct`, `list` and `map` with per-layer validity and offsets, worked through
   one list-of-`INT32` example with its offsets spelled out, and a note that this is the
   write-side inverse of `getLayerValidity` / `getLayerOffsets`, linking to
   `concepts/nested-columns.md`.
6. Batch sizing: batches are an arrival unit, not a layout unit — the writer re-cuts them
   into pages and row groups, and a batch larger than the row-group target is split.
7. Experimental admonition for the `Validity` and nested overloads.

Excluded: the same exclusions as above.

### `reference/writer.md` — "Writer Reference"

Look-up facts, no narration.

- **Writer options** — a `WriterConfig` table (option, default, description), matching the
  shape of the Reader Options table in `reference/configuration.md`.
- **Encodings** — `ColumnEncoding` values, what `AUTO` does (the writer weighs a dictionary
  against `PLAIN` once the row group is buffered and takes the smaller, per column chunk),
  and the legality matrix, which mirrors `EncodingSupport.supports`:

  | Encoding | Legal for |
  |---|---|
  | `AUTO`, `PLAIN` | every physical type |
  | `DELTA_BINARY_PACKED` | `INT32`, `INT64` |
  | `DELTA_LENGTH_BYTE_ARRAY` | `BYTE_ARRAY` |
  | `DELTA_BYTE_ARRAY` | `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` |
  | `BYTE_STREAM_SPLIT` | `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `FIXED_LEN_BYTE_ARRAY` |

  Plus: an illegal combination is rejected when the writer is created, not at flush; a
  `BOOLEAN` column is never dictionary-encoded.
- **Codecs** — which `CompressionCodec` values are produced, which need a library on the
  classpath, and the two refusals with their reasons (`LZ4` names the deprecated Hadoop
  framing, `LZO` has no maintained JVM implementation under a compatible licence). Both
  are refused at writer creation.
- **Typed setters** — the write-side mirror of `reference/accessors.md`: logical type →
  `StructBuilder` setter → physical representation, and the `ColumnBatch` array type for
  the same column.
- **Value ranges** — what an annotation narrows (`INT(8)`, unsigned widths, `DECIMAL(p, s)`
  digit count, `FIXED_LEN_BYTE_ARRAY` length) and that both APIs reject out-of-range
  values; `PrecisionLossPolicy` and what `TRUNCATE` permits.
- **Statistics written** — per column chunk `min`/`max`/`null_count`, `BYTE_ARRAY` bounds
  truncated at `statisticsTruncationLength` and flagged inexact, and `distinct_count`
  where the chunk's encoding decision still knew it.
- **`created_by`** — the default string's shape and that readers parse it.

### `concepts/write-model.md` — "The Write Model"

Explanation, no step-by-step.

- Forward-only, footer-last production, and what follows for the caller: no seeking, no
  file size known up front, nothing readable before `close()`, and `discard()` as the
  failure counterpart.
- Why memory is bounded by `rowGroupTargetBytes` rather than by how much is written, and
  that peak heap is a small multiple of the target — the build pins it below three times,
  and it measures well under two.
- Why the writer, not the caller, cuts row groups and pages, and what the two targets buy.
- How `AUTO` decides dictionary versus `PLAIN`, and why the decision is per column chunk.
- Index addressing on the two sides: projected order when reading, declaration order when
  writing.

## Edits to existing pages

| File | Edit |
|---|---|
| `docs/mkdocs.yml` | Nav entries above; `site_description` describes a reader and a writer |
| `docs/content/index.md` | Tagline, the "Why Hardwood" bullets, and a write Quick Example beside the read one |
| `docs/content/how-to/index.md` | Intro sentence, the guide list, and a "Choosing a Write API" table beside the reader one |
| `docs/content/reference/packages.md` | `dev.hardwood.writer` row; the `dev.hardwood` row mentions `OutputFile` |
| `docs/content/reference/error-handling.md` | A `Writing` section beside the reading one, carrying the writer's failure vocabulary: `IllegalArgumentException` (schema, path, range and batch-shape violations), `IllegalStateException` (writing after `close()`, mixing the two APIs, a builder used after its scope), `UnsupportedOperationException` (refused codec, unsupported schema), `IOException` (destination failures) |
| `docs/content/reference/configuration.md` | Cross-link from Reader Options to `reference/writer.md` |
| `docs/content/getting-started.md` | The compression-library section is read-only framed; state which codecs are written, and that the default codec falls back to `UNCOMPRESSED` without `zstd-jni` |
| `docs/content/reference/schema-element.md` | Its "build a schema for writing" note points at `how-to/metadata.md`; retarget to `how-to/write-row-by-row.md` |
| `README.md` | "Be complete: Add a Parquet file writer (after 1.0)" describes a shipped writer |
| `core/src/main/java/dev/hardwood/writer/ParquetFileWriter.java` | Class JavaDoc opens with "This increment writes …"; state the capability without the delivery framing |

## Constraints

- New pages under `docs/content/` carry the CC-BY-SA header block; the license check in
  `./mvnw verify` fails without it.
- Diátaxis purity as in [DOCS_DIATAXIS_STRUCTURE.md](DOCS_DIATAXIS_STRUCTURE.md): the
  how-to guides give no rationale, the reference page does not narrate, the concepts page
  gives no instructions.
- Pages describe the present. No "now supports", "new in 1.1", or comparisons to what the
  library used to do.
- Every snippet compiles against the API as it stands and is derived from a passing test
  under `core/src/test/java/dev/hardwood/writer/`.

## Verification

- `./mvnw verify` (license headers, and the JavaDoc edit compiles).
- `docker run --rm -v "$(pwd):/repo" hardwood-docs build -f docs/mkdocs.yml` for nav and
  intra-site links.
- Each snippet compiled once against the built `hardwood-core` jar before it is committed.

## Follow-ups

- Runnable examples in `hardwood-examples` for both write guides, and the "Try it
  yourself" admonition each other how-to guide carries. Until they exist, the two write
  guides are the only ones without it.
- A writer lesson in the Tutorial section, after 1.1 ships.
- `FORMAT_COVERAGE.md` write-side rows.
