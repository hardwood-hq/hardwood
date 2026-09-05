<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Writer Reference

Facts about the write path: how a schema is declared, the configuration options, the encodings and codecs the writer produces, the setters each column type accepts, and what it rejects. For task-oriented instructions see [Write Row by Row](../how-to/write-row-by-row.md) and [Write Column by Column](../how-to/write-column-by-column.md).

## Schema

A file's schema is declared with `FileSchema.builder(String)` and built once; the same `FileSchema` is passed to `ParquetFileWriter.create`. Every method appends one field in declaration order, which is also the order the leaf-column indices follow.

| Method | Declares |
|---|---|
| `addColumn(name, type, repetition)` | a primitive column |
| `addColumn(name, type, repetition, logicalType)` | a primitive column carrying an annotation |
| `addColumn(name, type, repetition, typeLength)` | a `FIXED_LEN_BYTE_ARRAY` column of the given byte length |
| `addColumn(name, type, repetition, typeLength, logicalType)` | both of the above |
| `struct(name, repetition, filler)` | a `struct` group whose fields `filler` declares |
| `list(name, repetition, element)` | a `LIST` group whose element `element` declares |
| `map(name, repetition, keyType[, keyTypeLength][, keyLogicalType], value)` | a `MAP` group with a `REQUIRED` key of `keyType` and a value `value` declares |
| `build()` | the `FileSchema` |

`repetition` is `REQUIRED` or `OPTIONAL`; `REPEATED` is rejected, repetition being what `list` and `map` express. `typeLength` is required and positive for `FIXED_LEN_BYTE_ARRAY` and rejected for every other type — so a `UUID`, `INTERVAL`, `FLOAT16` or fixed-width `DECIMAL` column is declared through a `typeLength` overload:

```java
FileSchema schema = FileSchema.builder("event")
        .addColumn("id", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 16,
                new LogicalType.UuidType())
        .addColumn("amount", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL, 8,
                new LogicalType.DecimalType(2, 18))   // DecimalType takes (scale, precision)
        .build();
```

Inside a `struct` filler the same methods appear on `FileSchema.StructBuilder`; a list element and a map value are declared on `FileSchema.ElementBuilder`, whose `primitive(type, repetition[, typeLength][, logicalType])`, `struct`, `list` and `map` carry no name, the surrounding layout supplying it.

## Writer Options

Write-time behaviour is configured with an immutable `WriterConfig`, passed to `ParquetFileWriter.create(out, schema, config)`. `WriterConfig.defaults()` returns the defaults, which the two-argument `create(out, schema)` uses.

```java
WriterConfig config = WriterConfig.builder()
        .codec(CompressionCodec.ZSTD)
        .rowGroupBufferTargetBytes(64L << 20)
        .encoding("temperature", ColumnEncoding.BYTE_STREAM_SPLIT)
        .build();
```

| Option | Default | Description |
|--------|---------|-------------|
| `pageTargetBytes(int)` | `1 MiB` | Encoded bytes before a data page is cut — the values as the chunk's chosen encoding writes them, so a dictionary column is measured in indices. A ceiling: the page is cut before the value that would cross it, and only a single value larger than the whole target can breach it. A named delta encoding lands under the target rather than on it, its width being a property of the values rather than of the type. Must be at least 4 bytes. |
| `rowGroupTargetRows(long)` | `1,048,576` | Records per row group. The control over how a file is banded: row groups hold exactly this many records apart from the last. Binds for records narrower than about 128 bytes; above that the buffer target below cuts first. Must be positive; a target above the structural ceiling of `Integer.MAX_VALUE - 8` records is that ceiling, so `Long.MAX_VALUE` means "cut on bytes alone". |
| `rowGroupBufferTargetBytes(long)` | `128 MiB` | Bytes the writer holds for the open row group before cutting it: level streams, dictionary indices, value stores and dictionaries. The memory control, and the number peak heap follows; what reaches the file is smaller by whatever the encoding and the codec win. A row group passes it by at most one record. See [The Write Model](../concepts/write-model.md). Must be positive. |
| `codec(CompressionCodec)` | `ZSTD`, or `UNCOMPRESSED` when the ZSTD library is absent | Codec each page body is compressed with. |
| `encoding(ColumnEncoding)` | `AUTO` | Encoding policy for every column without an override of its own. |
| `encoding(String, ColumnEncoding)` | — | Encoding policy for one leaf column, overriding the file-wide default. |
| `statisticsTruncationLength(int)` | `64` | Longest `BYTE_ARRAY` `min` / `max` statistics bound. A longer bound is truncated and flagged inexact. Must be positive. |
| `precisionLossPolicy(PrecisionLossPolicy)` | `REJECT` | What the row-oriented layer does with a value carrying more precision than its column can hold. |
| `writeFailurePolicy(WriteFailurePolicy)` | `DISCARD` | What `close()` does when a write has failed: discard the output (leave nothing behind) or commit the successfully-written prefix as a valid file. |

A row group is cut at whichever of the two row-group targets is reached first.

Each option has a getter that reads the configured value back — `pageTargetBytes()`, `rowGroupTargetRows()`, `rowGroupBufferTargetBytes()`, `codec()`, `statisticsTruncationLength()`, `precisionLossPolicy()`, `writeFailurePolicy()`, and, for the two encoding setters, `defaultEncoding()` and `columnEncodings()`. Every setter rejects `null`, and the numeric bounds above are checked when the option is set.

The footer's key-value metadata and its `created_by` identifier are set on the `ParquetFileWriter` itself; see [File Metadata](#file-metadata).

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

A policy naming a column whose type cannot carry it fails when the writer is created — a file-wide `BYTE_STREAM_SPLIT` over a schema holding a `BYTE_ARRAY` column included. A per-column override names the column by its **dotted leaf path** as the schema spells it, synthetic segments included: `readings.list.element`, not `readings`. A path matching no leaf column is rejected the same way.

No policy demands a dictionary; it is an outcome `AUTO` may arrive at. A `BOOLEAN` column is never dictionary-encoded, so `AUTO` resolves it to `PLAIN` whatever its values are.

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

The row-oriented layer takes physical values through the setter named for the type, and converts logical-type values to the column's physical representation, taking the same Java types the reader returns. The physical setters apply whatever the column's annotation is: an `INT32` column annotated `DATE` accepts `setDate` and `setInt` alike, the one taking a `LocalDate` and the other the epoch day it converts to.

| Column | `StructBuilder` setter | Java type |
|---|---|---|
| `BOOLEAN` | `setBoolean` | `boolean` |
| `INT32` | `setInt` | `int` |
| `INT64` | `setLong` | `long` |
| `FLOAT` | `setFloat` | `float` |
| `DOUBLE` | `setDouble` | `double` |
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

Timestamp columns split by `isAdjustedToUTC`, mirroring the reader's split between `getTimestamp` and `getLocalTimestamp`: using the wrong setter throws, so an instant is never silently reinterpreted. Both APIs also accept every setter by field index; see [Addressing fields by index](../how-to/write-row-by-row.md#addressing-fields-by-index).

## Value Ranges

Some annotations narrow what their physical type may hold. Where one does, both write APIs reject a stored value outside that range, naming the offending row or field:

| Annotation | Accepted values |
|---|---|
| `INT(n)` signed, `n` < physical width | `[-2^(n-1), 2^(n-1))` |
| `INT(n)` unsigned, `n` < physical width | `[0, 2^n)` |
| `TIME(unit, _)` | `[0, one day in unit)` — the annotation defines the value as elapsed time after midnight |
| `DECIMAL(p, s)` | an unscaled value of at most `p` digits |
| `UNKNOWN` | no value at all; every row must be null, so the column is set through a setter taking a null mask |

Every other annotation narrows nothing, and its column is not scanned per value. `DATE` and `TIMESTAMP` are the ones worth naming: every `INT32` is a day offset the reader materializes, and every `INT64` is a timestamp in any of the three units, so there is no value for the columnar API to reject. `INT(32)`, `INT(64)` and their unsigned forms likewise admit every value of their physical type — a large unsigned value is spelled as a negative, which is also how the reader returns it.

Two checks are not annotations and always apply: a `FIXED_LEN_BYTE_ARRAY` value must be exactly the length the column declares, and a present value of a binary column must not be `null`. The value at a row a `Validity` marks null is never encoded, so it is never checked.

An annotation over binary *content* is not a range, and the writer does not inspect it. A value passed to `bytes(...)` or `fixed(...)` is written as given, whether the column is annotated `STRING`, `ENUM`, `JSON`, `BSON`, `VARIANT`, `GEOMETRY` or `GEOGRAPHY` — encoding a `STRING` column's values as UTF-8, and producing well-formed payloads under the others, is the caller's to do. Bytes that are not valid UTF-8 are written and read back as replacement characters rather than rejected. `StructBuilder.setString` takes a `String` and encodes it, so the row-oriented layer cannot produce that.

`PrecisionLossPolicy` governs **precision** only, and only on the row-oriented layer — the columnar API converts nothing, so nothing there can lose precision:

| Policy | Behaviour |
|---|---|
| `REJECT` (default) | Reject a value carrying digits the column's unit or scale cannot hold, naming the field. A value that is exact at the column's unit or scale is written normally. |
| `TRUNCATE` | Drop the digits that do not fit: a `TIME` / `TIMESTAMP` value is floored to the column's unit, a `DECIMAL` rescaled with `RoundingMode.DOWN`. |

A value the column cannot represent at all — a `LocalDate` beyond the `INT32` day range, an unscaled decimal wider than the declared precision — is rejected under either policy. This is a conversion check on the row-oriented layer, where a Java type wider than the column is what makes it reachable; the columnar API is handed the stored value directly.

## Statistics Written

Every column chunk carries `null_count`. `min` / `max` are computed under the column's `ColumnOrder` during encoding and written to the preferred `min_value` / `max_value` fields; the deprecated `min` / `max` fields are not written.

- **Bounds are omitted where their order is undefined.** A column annotated `INTERVAL`, `UNKNOWN`, `VARIANT`, `GEOMETRY`, `GEOGRAPHY`, `LIST` or `MAP` writes its null count alone, parquet-format leaving those orderings unspecified — a bound in an order the reader cannot know would prune away live rows. An all-null chunk has no bounds to write either.
- **Truncation.** `BYTE_ARRAY` bounds longer than `statisticsTruncationLength` are truncated and flagged inexact (`is_min_value_exact` / `is_max_value_exact` = `false`). Fixed-width types write those flags as `true`.
- **`nan_count`** is written for every `FLOAT`, `DOUBLE` and `FLOAT16` chunk, including when it is zero — a recorded zero is what lets a reader prove a chunk holds no NaN. No other type writes the field.
- **`distinct_count`** is written where the chunk still knows its cardinality exactly: any chunk under `AUTO` that interned its values to the end, whichever encoding the flush-time comparison then chose, and any `BOOLEAN` chunk, which knows its cardinality without a dictionary. It is absent for a chunk written under a named encoding, and for one that stopped interning part-way, which happens when repeated size probes find the dictionary losing to `PLAIN`.

Page-level index structures (OffsetIndex, ColumnIndex), Bloom filters, and the `GeospatialStatistics` of a `GEOMETRY` or `GEOGRAPHY` column are not yet written. [Bounding-box pushdown](../how-to/geospatial.md) prunes row groups from that last field, so it prunes nothing in a file Hardwood produced.

## File Metadata

Two footer fields are set on the `ParquetFileWriter` itself, at any point until `close()` writes the footer: the file's key-value metadata and its `created_by` identifier.

| Method | Description |
|--------|-------------|
| `keyValueMetadata(String key, String value)` | Adds one key-value entry, replacing any value already held for that key. |
| `keyValueMetadata(Map<String, String> metadata)` | Adds every entry of the map, leaving entries it does not name in place. |
| `createdBy(String createdBy)` | Replaces the `created_by` identifier. |

```java
try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
    writer.keyValueMetadata("ARROW:schema", encodedArrowSchema);
    writer.columnWriter().writeBatch(batch -> batch.ints(0, values));
    writer.keyValueMetadata("row.count", String.valueOf(values.length));
}
```

### Key-value metadata

`key_value_metadata` is application-defined: Parquet does not interpret it, and the writer validates nothing beyond requiring a key. It is where `ARROW:schema`, the pandas descriptor, `org.apache.spark.sql.parquet.row.metadata` and the table-format stamps live.

Entries reach the file in the order they were added. A `null` value writes a key carrying no value, which the format allows and which is what a reader reports as a `null` — so the map `FileMetaData.keyValueMetadata()` returns can be passed straight to `keyValueMetadata(Map)` to reproduce another file's metadata exactly. A file given no entries carries no `key_value_metadata` field at all.

Because the methods are callable until `close()`, a value the caller knows only once the data is written — a row count, a digest over what was produced — can still be stamped on the file.

Reading the field back is covered in [Read File Metadata](../how-to/metadata.md).

### `created_by`

The default identifier follows the convention Parquet readers parse, naming the library, its version and the build it came from:

```
hardwood version <version> (build <commit>)
```

`ParquetFileWriter.DEFAULT_CREATED_BY` holds it. `createdBy(String)` replaces it; readers that key compatibility workarounds off this field expect the `<app> version <version> (build <hash>)` shape, and a bare application name is rejected by some of them.

## What the Writer Rejects

| Exception | When |
|---|---|
| `UnsupportedOperationException` | A schema column of an unsupported physical type (`INT96`); a refused codec (`LZ4`, `LZO`) or one whose library is missing; a [schema shape](#schema-shapes) the writer cannot produce |
| `IllegalArgumentException` | A `null` metadata key, metadata map or `created_by`; an unknown column name or path; a setter that does not fit the column's type; a `null` value array, or a `null` value at a present row of a binary column; a column set twice in one batch or record; a batch that leaves a column unset, or whose arrays disagree in length; a null mask on a `REQUIRED` column; a `boolean[]` mask whose length does not match the values; list offsets that do not start at `0`, are not non-decreasing, or disagree with the element count; a value outside the range its annotation declares; a `REQUIRED` field left unset by a record |
| `IndexOutOfBoundsException` | A leaf-column index outside `[0, leaf column count)` on a `ColumnBatch` setter, or a field index outside `[0, getFieldCount())` on a `StructBuilder` setter |
| `IllegalStateException` | Writing, or setting key-value metadata or `created_by`, after `close()`; writing after a previous write has failed (under the default `WriteFailurePolicy.DISCARD`); using both write APIs on one file; using a `ColumnBatch` after it has been submitted, or a nested builder after its filler has returned |
| `IOException` | The destination cannot be created, written, or finalized |

### Schema Shapes

Repetition is writable wherever a `LIST` or `MAP` annotation accounts for it. An annotated group is `REQUIRED` or `OPTIONAL` and holds exactly one `REPEATED` entry, which for a `MAP` is a group of `key` and `value`. That admits the canonical three-level `LIST` and two-level `MAP` layouts `FileSchema.builder` declares, and the legacy two-level lists a schema read from an existing file may carry: `LIST { repeated element }`, whose entry is the element, and `LIST { repeated group element { … } }`, whose entry is an element struct.

Every other arrangement of repetition is rejected when the writer is created, since nothing in the schema says where its entries begin and end:

- a `REPEATED` field — leaf or group — whose parent carries neither annotation;
- a `LIST` or `MAP` group that is itself `REPEATED`;
- a `LIST` or `MAP` group holding anything other than its single `REPEATED` entry;
- a `MAP` whose entry is a leaf rather than a group.

The row API reaches a list's values through an element node below the entry, which the legacy two-level lists do not have, so `rowWriter()` refuses those two shapes and `columnWriter()` writes them. `rowWriter()` also requires sibling field names to be unique, which the `ColumnBatch` indices and dotted paths do not.

A `ParquetFileWriter` that cannot finish a valid file discards its output, leaving nothing at the destination. Under `WriteFailurePolicy.COMMIT_PREFIX`, a writer that has failed publishes whatever rows were flushed before the failure instead.
