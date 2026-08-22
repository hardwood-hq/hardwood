<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Writer Reference

Facts about the write path: the configuration options, the encodings and codecs it produces, the setters each column type accepts, and what it rejects. For task-oriented instructions see [Write Row by Row](../how-to/write-row-by-row.md) and [Write Column by Column](../how-to/write-column-by-column.md).

## Writer Options

Write-time behaviour is configured with an immutable `WriterConfig`, passed to `ParquetFileWriter.create(out, schema, config)`. `WriterConfig.defaults()` returns the defaults, which the two-argument `create(out, schema)` uses.

```java
WriterConfig config = WriterConfig.builder()
        .codec(CompressionCodec.ZSTD)
        .rowGroupTargetBytes(64L << 20)
        .encoding("temperature", ColumnEncoding.BYTE_STREAM_SPLIT)
        .build();
```

| Option | Default | Description |
|--------|---------|-------------|
| `pageTargetBytes(int)` | `1 MiB` | Target size of one data page. Must be at least 4 bytes. |
| `rowGroupTargetBytes(long)` | `128 MiB` | Uncompressed bytes buffered before a row group is flushed. The writer's memory bound. Must be positive. |
| `codec(CompressionCodec)` | `ZSTD`, or `UNCOMPRESSED` when the ZSTD library is absent | Codec each page body is compressed with. |
| `encoding(ColumnEncoding)` | `AUTO` | Encoding policy for every column without an override of its own. |
| `encoding(String, ColumnEncoding)` | — | Encoding policy for one leaf column, overriding the file-wide default. |
| `statisticsTruncationLength(int)` | `64` | Longest `BYTE_ARRAY` `min` / `max` statistics bound. A longer bound is truncated and flagged inexact. Must be positive. |
| `createdBy(String)` | `hardwood version <version> (build <hash>)` | The footer's `created_by` identifier. |
| `precisionLossPolicy(PrecisionLossPolicy)` | `REJECT` | What the row-oriented layer does with a value carrying more precision than its column can hold. |

Each corresponding getter (`pageTargetBytes()`, `codec()`, …) reads the configured value back. Every setter rejects `null`, and the numeric bounds above are checked when the option is set.

## Encodings

`ColumnEncoding` sets how a column's values are encoded, file-wide or per leaf column. `AUTO` leaves the choice to the writer; every other member names an encoding outright, and a column that names one builds no dictionary.

| Policy | What it writes |
|--------|----------------|
| `AUTO` | Per column chunk, whichever of a dictionary or `PLAIN` is smaller, measured once the row group is buffered. One column may be dictionary-encoded in one row group and `PLAIN` in the next. |
| `PLAIN` | Values as they are: fixed-width types little-endian, `BYTE_ARRAY` behind a 4-byte length prefix. |
| `DELTA_BINARY_PACKED` | Differences between neighbouring values, bit-packed per block. Suits sorted or slow-moving integers. |
| `DELTA_LENGTH_BYTE_ARRAY` | Lengths delta-encoded ahead of the values, cheaper than `PLAIN`'s per-value length prefix where the lengths are similar. |
| `DELTA_BYTE_ARRAY` | Each value carrying only what it does not share with the value before it. Suits sorted values with common prefixes. |
| `BYTE_STREAM_SPLIT` | A value's bytes scattered across one stream per byte position. Changes no page's size on its own; it changes how well the codec afterwards compresses floating-point data. |

An encoding is legal only for some physical types:

| Policy | Physical types |
|---|---|
| `AUTO`, `PLAIN` | every writable type |
| `DELTA_BINARY_PACKED` | `INT32`, `INT64` |
| `DELTA_LENGTH_BYTE_ARRAY` | `BYTE_ARRAY` |
| `DELTA_BYTE_ARRAY` | `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` |
| `BYTE_STREAM_SPLIT` | `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `FIXED_LEN_BYTE_ARRAY` |

A policy naming a column whose type cannot carry it fails when the writer is created, rather than being quietly replaced — a file-wide `BYTE_STREAM_SPLIT` over a schema holding a `BYTE_ARRAY` column included. A per-column override names the column by its **dotted leaf path** as the schema spells it, synthetic segments included: `readings.list.element`, not `readings`. A path matching no leaf column is rejected the same way.

No policy demands a dictionary: dictionary encoding is an outcome `AUTO` may arrive at, not something to ask for. A `BOOLEAN` column is never dictionary-encoded, so `AUTO` resolves it to `PLAIN` whatever its values are.

## Compression Codecs

| Codec | Produced | Library needed |
|-------|----------|----------------|
| `UNCOMPRESSED` | yes | — |
| `GZIP` | yes | — (JDK) |
| `SNAPPY` | yes | `org.xerial.snappy:snappy-java` |
| `ZSTD` | yes | `com.github.luben:zstd-jni` |
| `LZ4_RAW` | yes | `at.yawk.lz4:lz4-java` |
| `BROTLI` | yes | `com.aayushatharva.brotli4j:brotli4j` |
| `LZ4` | no | — |
| `LZO` | no | — |

A missing library, and either of the two refused codecs, fails when the writer is created rather than mid-file. `LZ4` names the Hadoop framing the Parquet format deprecated in favour of `LZ4_RAW`; files already written with it are still read, so the refusal is on the write side only. `LZO` has no maintained JVM implementation under a licence this project can depend on, and is refused in both directions.

## Physical Types and Columnar Setters

Every physical type Parquet defines is written except `INT96`, which is deprecated and rejected when the writer is created. Each `ColumnBatch` setter comes in four shapes: by index or name, all-present or nullable (a `Validity` or a `boolean[]` mask).

| Physical type | `ColumnBatch` setter | Values |
|---|---|---|
| `BOOLEAN` | `booleans` | `boolean[]` |
| `INT32` | `ints` | `int[]` |
| `INT64` | `longs` | `long[]` |
| `FLOAT` | `floats` | `float[]` |
| `DOUBLE` | `doubles` | `double[]` |
| `BYTE_ARRAY` | `bytes` | `byte[][]` |
| `FIXED_LEN_BYTE_ARRAY` | `fixed` | `byte[][]`, each exactly the declared length |

The columnar API takes physical values and converts nothing: a `STRING` column is written through `bytes(...)` as UTF-8, a `DATE` column through `ints(...)` as days since the Unix epoch. Nesting is described through `struct(path, Validity)`, `list(path, offsets[, Validity])` and `map(path, offsets[, Validity])`.

## Logical Types and Row Setters

The row-oriented layer converts logical-type values to the column's physical representation, taking the same Java types the reader returns.

| Column | `StructBuilder` setter | Java type |
|---|---|---|
| `BYTE_ARRAY` annotated `STRING`, `ENUM`, `JSON`, or unannotated | `setString` | `String` (written as UTF-8) |
| `BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY` | `setBinary` | `byte[]` |
| `INT32` annotated `DATE` | `setDate` | `LocalDate` |
| `INT32` / `INT64` annotated `TIME` | `setTime` | `LocalTime` |
| `INT64` annotated `TIMESTAMP(_, UTC)` | `setTimestamp` | `Instant` |
| `INT64` annotated `TIMESTAMP(_, local)` | `setLocalTimestamp` | `LocalDateTime` |
| `INT32` / `INT64` / `BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY` annotated `DECIMAL` | `setDecimal` | `BigDecimal` |
| `FIXED_LEN_BYTE_ARRAY(16)` annotated `UUID` | `setUuid` | `UUID` |
| `FIXED_LEN_BYTE_ARRAY(12)` annotated `INTERVAL` | `setInterval` | `PqInterval` |
| any primitive | `setNull` | — |
| `struct` / `LIST` / `MAP` group | `setStruct` / `setList` / `setMap` | a filler over `StructBuilder` / `ListBuilder` / `MapBuilder` |

`ListBuilder` has the same set as `addInt`, `addString`, `addStruct`, `addNull`, … and `MapBuilder` has `addEntry`, whose entry struct declares the two fields `key` and `value`.

Timestamp columns split by `isAdjustedToUTC`, mirroring the reader's split between `getTimestamp` and `getLocalTimestamp`: using the wrong setter throws rather than silently reinterpreting the instant. Both APIs also accept every setter by field index; see [Addressing fields by index](../how-to/write-row-by-row.md#addressing-fields-by-index).

## Value Ranges

An annotation narrows what its physical type may hold, and both write APIs reject a value outside that range, naming the offending row or field:

| Annotation | Accepted values |
|---|---|
| `INT(n)` signed, `n` < physical width | `[-2^(n-1), 2^(n-1))` |
| `INT(n)` unsigned, `n` < physical width | `[0, 2^n)` |
| `INT(32)` / `INT(64)` and their unsigned forms | every value of the physical type |
| `DECIMAL(p, s)` | an unscaled value of at most `p` digits |
| `DATE`, `TIME`, `TIMESTAMP` | the range the type's unit spans |
| `FIXED_LEN_BYTE_ARRAY` | a value of exactly the declared length |
| `UNKNOWN` | no value; every row must be null |

The value at a null row is not checked. An annotation that narrows nothing skips the per-value scan entirely.

`PrecisionLossPolicy` governs **precision** only, and only on the row-oriented layer — the columnar API converts nothing, so nothing there can lose precision:

| Policy | Behaviour |
|---|---|
| `REJECT` (default) | Reject a value carrying digits the column's unit or scale cannot hold, naming the field. A value that is exact at the column's unit or scale is written normally. |
| `TRUNCATE` | Drop the digits that do not fit: a `TIME` / `TIMESTAMP` value is floored to the column's unit, a `DECIMAL` rescaled with `RoundingMode.DOWN`. |

A value the column cannot represent at all — a date beyond the `INT32` day range, an unscaled decimal wider than the declared precision — is rejected under either policy.

## Statistics Written

Each column chunk carries `min` / `max` / `null_count`, computed under the column's `ColumnOrder` during encoding, and written to the preferred `min_value` / `max_value` fields.

- `BYTE_ARRAY` bounds longer than `statisticsTruncationLength` are truncated and flagged inexact (`is_min_value_exact` / `is_max_value_exact` = `false`). Fixed-width types write those flags as `true`.
- `distinct_count` is written where the chunk still knows its cardinality — a chunk whose encoding `AUTO` chose from a dictionary, and any `BOOLEAN` chunk, which knows it without one. It is absent for a chunk whose dictionary outgrew the writer's analysis budget, and for one written under a named encoding.

Page-level index structures (OffsetIndex, ColumnIndex) and Bloom filters are not yet written.

## `created_by`

The default identifier follows the convention Parquet readers parse, naming the library, its version and the build it came from:

```
hardwood version <version> (build <commit>)
```

`createdBy(String)` replaces it. Readers that key compatibility workarounds off this field expect the `<app> version <version> (build <hash>)` shape; a bare application name is rejected by some of them.

## What the Writer Rejects

| Exception | When |
|---|---|
| `UnsupportedOperationException` | A schema column of an unsupported physical type (`INT96`); a refused codec (`LZ4`, `LZO`) or one whose library is missing; an `OPTIONAL` struct group directly enclosing a repeated field |
| `IllegalArgumentException` | An unknown column name or path; a setter that does not fit the column's type; a column set twice in one batch or record; a batch that leaves a column unset, or whose arrays disagree in length; a null mask on a `REQUIRED` column; a `boolean[]` mask whose length does not match the values; list offsets that do not start at `0`, are not non-decreasing, or disagree with the element count; a value outside the range its annotation declares; a `REQUIRED` field left unset by a record |
| `IndexOutOfBoundsException` | A field index outside `[0, getFieldCount())` |
| `IllegalStateException` | Writing after `close()`; using both write APIs on one file; using a `ColumnBatch` after it has been submitted, or a nested builder after its filler has returned |
| `IOException` | The destination cannot be created, written, or finalized |

A `ParquetFileWriter` that cannot finish a valid file discards its output rather than leaving a truncated file at the destination.
