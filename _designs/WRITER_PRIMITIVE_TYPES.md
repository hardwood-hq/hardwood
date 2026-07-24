# Primitive type write support (#9, stage 12)

**Status: In progress.** Tracking issue: #9. Delivery stage 12 (Breadth) of
[WRITER_SUPPORT.md](WRITER_SUPPORT.md). This document is the reference the primitive-type
increments implement against.

## Context

Through stage 11 the writer produces every column shape the reader supports — flat
`REQUIRED` / `OPTIONAL`, nested `struct` / `LIST` / `MAP` — with paging, nulls, dictionary
encoding, compression, and statistics all proven. But only one physical type: `INT32`. The
value that rides through the whole pipeline is an `int`, and every value-touching component
is written to `int` / `int[]`: the `ColumnBatch` setters, the column source, the shredder's
value window, the column-chunk buffer's pending array, the `PlainEncoder`, the
`DictionaryEncoder`, and the `StatisticsCollector`.

This stage removes the `INT32` restriction, so the writer can produce every primitive
physical type Parquet defines and the reader already reads:

- `BOOLEAN`, `INT32`, `INT64`, `FLOAT`, `DOUBLE` — fixed-width scalars;
- `BYTE_ARRAY` — variable-length binary (also strings, once stage 13 annotates them);
- `FIXED_LEN_BYTE_ARRAY` — binary of a schema-declared fixed length.

`INT96` is deprecated (legacy timestamps only) and is **not** produced by the writer.

Every type inherits the settled machinery unchanged — paging, null / rep / def levels,
nesting, dictionary encoding with plain fallback, compression, and column statistics. What
changes is confined to the value: how it is supplied, buffered, `PLAIN`-encoded, interned in
a dictionary, and compared for `min` / `max`.

## Scope

1. **The value seam** — the internal boundary between shredding (type-agnostic) and value
   encoding (per-type) is drawn so the nested-shredding recursion is written once, not once
   per type.
2. **`PLAIN` encoding** for every physical type, matching the byte layout the reader's
   `PlainDecoder` parses.
3. **Dictionary encoding** for every type except `BOOLEAN`, reusing the stage 9 column-chunk
   layout with a per-type interning table.
4. **Statistics** — type-defined `min` / `max` ordering per type, with `BYTE_ARRAY` bound
   **truncation** flagged inexact (`is_min_value_exact` / `is_max_value_exact` = false).
5. **`ColumnBatch` setters** for every type, and a `FileSchema.Builder` overload carrying the
   `FIXED_LEN_BYTE_ARRAY` type length.
6. **Row-group flush by buffered bytes** — variable-width values end the constant-bytes-per-row
   assumption, so the flush trigger moves from a fixed rows-per-group proxy to the actual
   buffered uncompressed byte count.

Logical-type annotations (STRING over `BYTE_ARRAY`, DECIMAL over `FIXED_LEN_BYTE_ARRAY`, …)
are stage 13; this stage writes the physical bytes only. `DELTA_*` and `BYTE_STREAM_SPLIT`
encoders are stage 14; every column here is `PLAIN` or `RLE_DICTIONARY`.

## The value seam

The nested shredder — rep/def-level computation, per-layer validity, offset-driven descent —
is entirely type-agnostic: only the leaf *value* it reads carries a type. To keep the
recursion single-sourced and every value primitive (no boxing), the seam is drawn **below the
value read**, not above it.

The `RecordShredder.LevelSink` callback drops its value parameter and carries only the source
position of a present value:

```java
public interface LevelSink {
    /// `valueIndex >= 0` marks a present leaf value at that position in the column's source;
    /// `valueIndex < 0` marks an absent slot (a null leaf, a null / empty list, or a null
    /// struct ancestor).
    void accept(int repetitionLevel, int definitionLevel, int valueIndex);
}
```

The shredder no longer reads values or holds a value window; it emits `(rep, def, valueIndex)`
and needs only each column's leaf **value count** for offset validation. Value materialization
moves into the per-type value writer the column-chunk buffer owns: on a present entry it reads
its own typed window at `valueIndex`, buffers the typed value, and feeds the typed statistics;
on an absent entry it advances the null count. Because present positions are emitted in
increasing source order, the typed window keeps its forward-only bulk-copy property.

This confines every type-specific line to one place — the value writer — behind a
type-agnostic shredder, page-sealer, compressor, and footer.

### Column sources

`ColumnSource` is the read-only bulk view the value writer pulls from. A base interface
carries `int size()` (all the shredder needs); a typed sub-interface per storage family
carries the bulk copy:

| Family | Source | Batch backing |
|---|---|---|
| `int` | `IntColumnSource` | `int[]` |
| `long` | `LongColumnSource` | `long[]` |
| `float` | `FloatColumnSource` | `float[]` |
| `double` | `DoubleColumnSource` | `double[]` |
| `boolean` | `BooleanColumnSource` | `boolean[]` |
| binary | `BinaryColumnSource` | `byte[][]` |

`BYTE_ARRAY` and `FIXED_LEN_BYTE_ARRAY` share `BinaryColumnSource` (a `byte[]` per value);
the fixed length is validated per value against the schema's type length. A public
`ColumnVector` SPI over the same shape — writing from a foreign container without an
intervening copy — remains a later additive layer, unchanged by this stage.

## `PLAIN` encoding

`PlainEncoder` gains one method per physical family, each the inverse of the matching
`PlainDecoder` reader:

| Type | Layout |
|---|---|
| `BOOLEAN` | bit-packed, 8 values per byte, LSB first |
| `INT32` | 4-byte little-endian |
| `INT64` | 8-byte little-endian |
| `FLOAT` | IEEE-754 single, little-endian |
| `DOUBLE` | IEEE-754 double, little-endian |
| `BYTE_ARRAY` | 4-byte little-endian length prefix, then the bytes |
| `FIXED_LEN_BYTE_ARRAY` | the bytes, no length prefix (length is the schema's type length) |

The page body framing is unchanged from stage 9 — `[rep levels?][def levels?][value section]`,
each level stream 4-byte-length-prefixed — with the value section now produced by the column's
value writer.

## Dictionary encoding

Dictionary encoding reuses the stage 9 column-chunk layout verbatim — a `PLAIN` dictionary
page ahead of `RLE_DICTIONARY` index pages, per-page encoding, and mid-chunk plain fallback
when the dictionary outgrows `dictionaryPageLimitBytes`. Only the interning table becomes
per-type:

- **`INT64` / `FLOAT` / `DOUBLE`** — the open-addressed value→index table keys on the widened
  value (`float` / `double` via their raw bit patterns so `-0.0` and `NaN` intern by identity);
  the dictionary body is `PLAIN`-encoded in index order through the matching encoder. Byte-size
  tracking uses the type's fixed width.
- **`BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY`** — the table keys on byte-array content (content
  hash, content equality); the dictionary body is the distinct values `PLAIN`-encoded in index
  order. Byte-size tracking sums the actual value lengths (plus the 4-byte length prefix for
  `BYTE_ARRAY`), so the fallback trigger reflects the real dictionary-page size rather than a
  count proxy — the case the byte limit was designed for.
- **`BOOLEAN`** — never dictionary-encoded: with two possible values a dictionary cannot beat
  bit-packed `PLAIN`, so a boolean chunk is always `PLAIN`.

Fixed-width dictionary keys share one open-addressed table implementation specialized per
primitive to avoid boxing; the binary table is a distinct implementation keyed on `byte[]`.
First-seen index assignment, per-page bit width, and the fallback rule are the stage 9
mechanism unchanged.

## Statistics

`StatisticsCollector` becomes per-type, each accumulating `min` / `max` over present values in
that type's **type-defined order** and the null count over absent slots, exactly as stage 11
does for `INT32`:

- **`INT32` / `INT64`** — signed numeric order; bounds encoded as 4- / 8-byte little-endian.
- **`FLOAT` / `DOUBLE`** — IEEE order with the two spec-mandated adjustments: `NaN` values are
  excluded from `min` / `max` (a chunk of only `NaN` carries a null count but no bounds), and a
  zero bound is sign-normalized so a reader's `[min, max]` test is never wrong for the other
  zero — a `min` of `+0.0` is written as `-0.0`, a `max` of `-0.0` as `+0.0`.
- **`BOOLEAN`** — `false < true`; bounds encoded as a single `0` / `1` byte.
- **`BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY`** — unsigned lexicographic order (`Arrays.compareUnsigned`),
  matching the reader's `BinaryComparator` and the type-defined order for unannotated binary.

All fixed-width bounds are exact. `BYTE_ARRAY` bounds are **truncated** to at most a configured
length so a chunk of long values does not bloat the footer:

- a truncated **`min`** keeps the value's first *N* bytes — a prefix is `<=` the original, so it
  remains a valid lower bound;
- a truncated **`max`** keeps the first *N* bytes and increments the last byte that is not
  `0xFF`, yielding the smallest length-*N* value `>=` the original; if every kept byte is `0xFF`
  no valid truncated upper bound exists and the `max` bound is omitted;
- a truncated bound is flagged **inexact** (`is_min_value_exact` / `is_max_value_exact` =
  false); an untruncated bound stays exact.

`FIXED_LEN_BYTE_ARRAY` bounds are written whole (a fixed width is already bounded), always
exact. The exactness flags become per-bound: the `Statistics` model carries
`isMinValueExact` / `isMaxValueExact` (both true for every fixed-width type and for an
untruncated binary bound), and `StatisticsWriter` emits `is_min_value_exact` /
`is_max_value_exact` from them rather than unconditionally true.

## Public API

- **`ColumnBatch`** gains a setter family per type — `longs`, `floats`, `doubles`, `booleans`,
  `bytes` (`BYTE_ARRAY`) and `fixed` (`FIXED_LEN_BYTE_ARRAY`) — each mirroring the existing
  `ints` overloads: by index and by name, all-present and nullable (`Validity` and `boolean[]`
  mask forms). The setter validates the column's physical type, so a type mismatch fails
  eagerly, exactly as the `INT32`-only check does today (now generalized to every type).
- **`FileSchema.Builder`** gains an `addColumn` / `primitive` overload carrying an `int`
  type length, required for a `FIXED_LEN_BYTE_ARRAY` column and rejected for any other type.
- **`WriterConfig`** gains `statisticsTruncationLength` (default 64 bytes) — the maximum
  `BYTE_ARRAY` `min` / `max` bound length before truncation. Library-level only; the CLI
  surface stays minimal.

The whole writer surface is `@Experimental` and is not yet in the user documentation under
`docs/content/`; it is documented as a settled API by the milestone's dedicated docs increment
(stage 18 of [WRITER_SUPPORT.md](WRITER_SUPPORT.md)), not per primitive-type increment.

## Row-group flush

The stage 3–11 writer sizes a row group by a rows-per-group proxy computed once from the
schema: `rowGroupTargetBytes` divided by the summed per-column bit cost, treating every value
as `Integer.SIZE`. This holds only while every value is four bytes. Two changes generalize it:

- the per-column bit cost uses each type's actual width — `BOOLEAN` = 1, `INT64` / `DOUBLE`
  = 64, and so on — so a fixed-width row group is sized correctly rather than at twice or a
  quarter of the target;
- for variable-width columns no constant per-row cost exists, so the flush trigger moves off
  the proxy entirely: `RowGroupBuffer` tracks the **actual buffered uncompressed bytes** across
  its column chunks (pending plus sealed page bodies) and the writer flushes once that crosses
  `rowGroupTargetBytes`. The proxy is retained only to bound the page-level entry count.

## Component architecture

| Layer | Package | Change |
|---|---|---|
| Public API | `dev.hardwood.writer` | `ColumnBatch` per-type setters; `WriterConfig.statisticsTruncationLength` |
| Schema | `dev.hardwood.schema` | `FileSchema.Builder` type-length overload for `FIXED_LEN_BYTE_ARRAY` |
| Value input | `dev.hardwood.internal.writer` | `ColumnSource` base plus one typed source / backing per family |
| Value encoding | `dev.hardwood.internal.encoding` | `PlainEncoder` per-type methods; `DictionaryEncoder` specialized per fixed-width type plus a binary variant |
| Orchestration | `dev.hardwood.internal.writer` | `RecordShredder` value seam (type-agnostic `LevelSink`); a per-type value writer behind `ColumnChunkBuffer`; per-type `StatisticsCollector`; `RowGroupBuffer` buffered-byte tracking |
| Metadata | `dev.hardwood.internal.thrift` | `StatisticsWriter` per-bound exactness; `ColumnChunkBuffer` records the column's real `PhysicalType` (no longer hard-coded `INT32`) |

## Validation strategy

The two-tier check from [WRITER_SUPPORT.md](WRITER_SUPPORT.md) applies unchanged, exercised
across the new types:

1. **DuckDB differential (primary)** — hardwood writes each physical type, flat and nested,
   plain and dictionary-encoded, nullable and required; DuckDB reads them through
   `read_parquet` and the values must match. Cases that stress the new paths: a `DOUBLE` column
   with `NaN` and both signed zeros (statistics normalization); a low-cardinality `BYTE_ARRAY`
   (dictionary the dominant real-world encoding); a `BYTE_ARRAY` column of long values (bound
   truncation, inexact flags); a `FIXED_LEN_BYTE_ARRAY` column (type length round-trips).
2. **Round-trip** — write with hardwood, read back through the `ColumnReader` /
   `ParquetFileReader` layer, asserting values, null positions, and statistics survive.
   Round-trip pins what DuckDB does not surface: the `min` / `max` bounds and their exactness
   flags, the recorded `PhysicalType`, and that a truncated `BYTE_ARRAY` bound still brackets
   every value in the chunk.

## Delivery

This stage lands in two stacked increments, each with the DuckDB differential and round-trip
checks above, so `main` never holds primitive-type write code that cannot produce a readable
file:

- **Stage 12a — fixed-width types.** The value seam (type-agnostic `LevelSink`, typed value
  writer), and `BOOLEAN` / `INT64` / `FLOAT` / `DOUBLE` alongside the refactored `INT32`. `PLAIN`
  and dictionary encoding, numeric / IEEE / boolean statistics, and the per-type-width row-group
  proxy. No variable-width values, no truncation, no exactness change.
- **Stage 12b — variable-width types.** `BYTE_ARRAY` and `FIXED_LEN_BYTE_ARRAY`: the binary
  source, binary `PLAIN` and dictionary, lexicographic statistics with `BYTE_ARRAY` truncation
  and per-bound exactness, the `FileSchema.Builder` type-length overload, and the buffered-byte
  row-group flush.

Together they complete roadmap boxes 2.1 (all physical types) and the truncation half of 9.1.
Stage 13 (logical-type annotations) then layers STRING / DATE / DECIMAL / … onto these physical
types, and stage 16 (row-group-global dictionary selection) exploits the accurate per-type
dictionary byte accounting introduced here.
