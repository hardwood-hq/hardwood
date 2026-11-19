<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Writer Reference

## Destination

The `OutputFile` a writer is created with decides where the file is written.

| Factory | Destination |
|---|---|
| `OutputFile.of(Path)` | A local file. The bytes are streamed to a temporary sibling of the target path and renamed onto it when the writer closes, so a write that fails or is abandoned leaves nothing at the path |
| `OutputFile.inMemory()` | The heap, up to `Integer.MAX_VALUE - 8` bytes. Returns an `InMemoryOutputFile`, whose `buffer()` returns the finished file |
| `S3Source.outputFile(...)` | An S3 object, uploaded as the file is written and published when the writer closes (`hardwood-s3`; see [S3 Reference](s3.md#writing)) |

An `OutputFile` writes one file. `ParquetFileWriter.create` throws `IllegalStateException` for an `OutputFile` another writer has already used.

An in-memory file can be written and read back without a filesystem: `buffer()` returns the whole file as a `ByteBuffer` positioned at its first byte, which is what `InputFile.of(ByteBuffer)` expects.

```java
InMemoryOutputFile out = OutputFile.inMemory();
try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
    RowWriter rows = writer.rowWriter();
    rows.writeRow(row -> row.setLong("id", 1L));
}

try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
    // ...
}
```

The buffer grows with the file, so no size has to be given up front; the length of a Parquet file is only known once it has been written. The whole file is held on the heap, which costs the size of the finished file in addition to the memory the writer uses for the open row group. Each call to `buffer()` returns a buffer with its own position and limit, so reading the file back leaves it intact. The buffer's contents must not be modified; whether a change shows in the buffers other calls return is unspecified.

The file is complete only after the writer is closed, and `buffer()` throws `IllegalStateException` before that, or if the destination was discarded.

To write anywhere else, extend `AbstractOutputFile` (`@Experimental`). Its public methods are final: each rejects a call the file's state does not allow with `IllegalStateException`, then calls the protected method that does the work. `position()` counts the bytes `doWrite` accepted. A subclass implements `doWrite` and overrides the other methods it needs; they do nothing by default.

| Method | Called by, while the file is open | Does |
|---|---|---|
| `doCreate()` | `create()` | Acquires the destination's resources. When it throws, the file is discarded without a call to `doDiscard()`, so it must leave nothing held |
| `doWrite(ByteBuffer)` | `write` | Appends all of the buffer's remaining bytes; `write` throws `IllegalStateException` if any are left. Required |
| `doClose()` | `close()` | Publishes the file. When it throws, `doDiscard()` runs, its failure is attached as suppressed, and the file is discarded |
| `doDiscard()` | `discard()`, or `close()` when `doClose()` throws | Releases the resources and throws away what was written. It is not called again after it throws, so it releases what it can first |

`close()` and `discard()` on a published or discarded file, and `close()` on an uncreated one, call nothing. A subclass that hands out the finished file checks for it with `requireClosed()`.

## Schema

A file's schema is declared with `FileSchema.builder(String)` and built once; the same `FileSchema` is passed to `ParquetFileWriter.create`. Every method appends one field in declaration order, which is also the order the leaf-column indices follow.

`FileSchema.builder(String, Consumer<FileSchema.StructBuilder>)` declares the root's fields on a `StructBuilder`, the type a nested `struct` is declared on, so one method can declare the fields of the root and of every struct. The returned builder takes further fields before `build()`.

| Method | Declares |
|---|---|
| `primitive(name, type, repetition)` | a primitive column |
| `primitive(name, type, repetition, column)` | a primitive column whose optional attributes `column` sets on a `FileSchema.PrimitiveBuilder` |
| `struct(name, repetition, filler)` | a `struct` group whose fields `filler` declares |
| `file(name, repetition)` | a `FILE` group with all six children, each `OPTIONAL`, in the order `uri`, `offset`, `size`, `content_type`, `checksum`, `inline` (see [FILE groups](accessors.md#file-groups)) |
| `file(name, repetition, group)` | the same `FILE` group, whose own attributes `group` sets on a `FileSchema.GroupBuilder` |
| `list(name, repetition[, group], element)` | a `LIST` group whose own attributes `group` sets, and whose element `element` declares |
| `map(name, repetition, keyType[, key], value)` | a `MAP` group with a `REQUIRED` key of `keyType`, whose optional attributes `key` sets, and a value `value` declares |
| `map(name, repetition, group, keyType, key, value)` | the same `MAP` group, whose own attributes `group` sets |
| `build()` | the `FileSchema` |

A `FileSchema.PrimitiveBuilder` sets the optional attributes of one primitive leaf. Each attribute can be set one time; a second call throws `IllegalArgumentException`.

| Method | Sets |
|---|---|
| `logicalType(logicalType)` | the annotation, which must be legal for the physical type; a leaf whose lambda does not call it is unannotated |
| `typeLength(typeLength)` | the byte length of a `FIXED_LEN_BYTE_ARRAY` leaf |
| `fieldId(fieldId)` | the field id (`field_id` in the footer) |

A `FileSchema.GroupBuilder` sets the optional attributes of a `LIST`, `MAP` or `FILE` group itself, with the same one-time rule. Its only method is `fieldId(fieldId)`.

The writer requires at least one column: `build()` rejects a schema with no fields, and `ParquetFileWriter.create` rejects one built another way, such as the childless root a file that declares no columns is read as, both with `UnsupportedOperationException`.

Each method throws `IllegalArgumentException` for a `null` argument, and for a field name that is empty or already declared among the same group's fields. `repetition` is `REQUIRED` or `OPTIONAL`; `REPEATED` is rejected, repetition being what `list` and `map` express. A message names a nested field by its dotted path, a list element as `<list>.list.element` and a map key or value as `<map>.key_value.key` or `<map>.key_value.value`. `typeLength` is rejected for every type but `FIXED_LEN_BYTE_ARRAY`, and a `FIXED_LEN_BYTE_ARRAY` column has a positive one. Declared without one, it takes the length its annotation implies:

| Annotation | Implied `typeLength` |
|---|---|
| `UUID` | 16 |
| `INTERVAL` | 12 |
| `FLOAT16` | 2 |
| `DECIMAL(precision, scale)` | the fewest bytes whose two's complement holds `precision` digits: 1 for precision 1–2, 4 for 9, 8 for 18, 16 for 38 |

Any other annotation, or none, implies no length, and the column needs `typeLength`. A `typeLength` given explicitly is kept as given; a `DECIMAL` may take more bytes than its precision needs, while a `UUID`, `INTERVAL` or `FLOAT16` of any other length is rejected.

```java
FileSchema schema = FileSchema.builder("event")
        .primitive("id", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED,
                col -> col.logicalType(LogicalType.uuid()))                         // 16 bytes
        .primitive("amount", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL,
                col -> col.logicalType(LogicalType.decimal(18, 2)))                 // 8 bytes
        .primitive("legacy_amount", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL,
                col -> col.typeLength(16).logicalType(LogicalType.decimal(18, 2)))
        .build();
```

Inside a `struct` filler the same methods appear on `FileSchema.StructBuilder`; a list element and a map value are declared on `FileSchema.ElementBuilder`, whose `primitive(type, repetition[, column])`, `struct`, `file`, `list` and `map` carry no name, the surrounding layout supplying it.

### Field ids

A field id identifies a field independently of its name. Table formats such as Iceberg, Delta Lake (column mapping mode `id`) and DuckLake map columns by it. Each declaration scope sets the field id of the field it declares:

| Field | Set with |
|---|---|
| a primitive column, list element, map key or map value | `PrimitiveBuilder.fieldId(int)` |
| a `struct` group, also as a list element or map value | `StructBuilder.fieldId(int)` in its filler |
| a `LIST`, `MAP` or `FILE` group | `GroupBuilder.fieldId(int)` in its `group` lambda |

The synthetic `list` and `key_value` groups, the root and the children of a `FILE` group carry no field id; `fieldId` on the `StructBuilder` that declares the root's fields throws `IllegalArgumentException`. The builder does not check that field ids are unique.

```java
FileSchema schema = FileSchema.builder("event")
        .primitive("id", PhysicalType.INT64, RepetitionType.REQUIRED, col -> col.fieldId(1))
        .struct("location", RepetitionType.OPTIONAL, location -> location
                .fieldId(2)
                .primitive("lat", PhysicalType.DOUBLE, RepetitionType.REQUIRED, col -> col.fieldId(3))
                .primitive("lon", PhysicalType.DOUBLE, RepetitionType.REQUIRED, col -> col.fieldId(4)))
        .list("tags", RepetitionType.OPTIONAL, group -> group.fieldId(5),
                element -> element.primitive(PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                        col -> col.logicalType(LogicalType.string()).fieldId(6)))
        .map("props", RepetitionType.OPTIONAL, group -> group.fieldId(7), PhysicalType.BYTE_ARRAY,
                key -> key.logicalType(LogicalType.string()).fieldId(8),
                value -> value.primitive(PhysicalType.INT64, RepetitionType.OPTIONAL, col -> col.fieldId(9)))
        .file("photo", RepetitionType.OPTIONAL, group -> group.fieldId(10))
        .build();
```

A schema read from a file keeps the field ids of its elements: `SchemaNode.fieldId()` and `ColumnSchema.fieldId()` return them, or `null` for an element without one. A file written with that schema carries the same field ids.

### Logical Type Factories

An annotation is built with the `LogicalType` static factory of the same name. The parameterless factories return a shared instance.

| Factory | Annotation |
|---|---|
| `LogicalType.string()` | `STRING` — UTF-8 encoded string |
| `LogicalType.enumType()` | `ENUM` — enum stored as a UTF-8 string |
| `LogicalType.uuid()` | `UUID` — 16-byte fixed-length byte array |
| `LogicalType.date()` | `DATE` — days since the Unix epoch |
| `LogicalType.json()` | `JSON` — JSON document stored as a UTF-8 string |
| `LogicalType.bson()` | `BSON` — BSON document stored as a byte array |
| `LogicalType.interval()` | `INTERVAL` — 12-byte fixed-length byte array (months, days, millis) |
| `LogicalType.float16()` | `FLOAT16` — IEEE 754 half-precision, 2-byte fixed-length byte array |
| `LogicalType.unknown()` | `UNKNOWN` — every value in the column is null |
| `LogicalType.list()` | `LIST` |
| `LogicalType.map()` | `MAP` |
| `LogicalType.intType(bitWidth, isSigned)` | `INT_8` … `UINT_64` — `bitWidth` is 8, 16, 32 or 64 |
| `LogicalType.decimal(precision, scale)` | `DECIMAL(precision, scale)` — `precision` is positive and `scale` lies in `[0, precision]`; other values throw `IllegalArgumentException` |
| `LogicalType.time(isAdjustedToUTC, unit)` | `TIME` — `unit` is `TimeUnit.MILLIS`, `MICROS` or `NANOS` |
| `LogicalType.timestamp(isAdjustedToUTC, unit)` | `TIMESTAMP` — same units, over an `INT64` or a `FIXED_LEN_BYTE_ARRAY(12)` |
| `LogicalType.variant(specVersion)` | `VARIANT` — `specVersion` is `1`; the writer rejects a column carrying it |
| `LogicalType.geometry(crs)` | `GEOMETRY` — `crs` defaults to `OGC:CRS84` when `null` |
| `LogicalType.geography(crs, edgeInterpolation)` | `GEOGRAPHY` — `edgeInterpolation` is an `EdgeInterpolationAlgorithm` |
| `LogicalType.file()` | `FILE` — annotates a group; declared with `file(name, repetition)`, and rejected on a column |

Which Java value each annotation accepts is listed under [Logical Types and Row Setters](#logical-types-and-row-setters); a pairing of annotation and physical type the format does not define is rejected when the schema is built.

## Writer Options

Write-time behaviour is configured with an immutable `WriterConfig`, passed to `ParquetFileWriter.create(out, schema, config)`. `WriterConfig.defaults()` returns the defaults, which the two-argument `create(out, schema)` uses.

```java
WriterConfig config = WriterConfig.builder()
        .compression(Compression.zstd())
        .rowGroupBufferTargetBytes(64L << 20)
        .encoding("temperature", ColumnEncoding.BYTE_STREAM_SPLIT)
        .build();
```

| Option | Default | Description |
|--------|---------|-------------|
| `pageTargetBytes(int)` | `1 MiB` | Encoded bytes before a data page is cut, counting the values as the chunk's chosen encoding writes them, so a dictionary column is measured in indices. A ceiling: the page is cut before the record that would cross it, and only a single record larger than the whole target can breach it, a page holding whole records. Must be at least 4 bytes; a target above 1,073,741,819 bytes cuts at 1,073,741,819 bytes. |
| `pageTargetRows(int)` | `20,000` | Records before a data page is cut. A page holds whole records, so on a repeated column it counts records, not values. Sets how finely the page index lets a reader skip within a column chunk; it binds on columns whose values encode small, such as dictionary-encoded ones. Must be positive. |
| `rowGroupTargetRows(long)` | `1,048,576` | Records per row group: row groups hold exactly this many records apart from the last. Must be positive; a target above the structural ceiling of `Integer.MAX_VALUE - 9` records is that ceiling, so `Long.MAX_VALUE` means "cut on bytes alone". |
| `rowGroupBufferTargetBytes(long)` | `128 MiB` | Bytes the writer holds for the open row group before cutting it: level streams, dictionary indices, value stores and dictionaries. A row group passes it by at most one record. A row group is also cut before a repeated column's chunk would reach `Integer.MAX_VALUE - 8` values and nulls, or before any column chunk would pass `Integer.MAX_VALUE - 8` bytes of `BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY` values, so a target above 2 GiB can yield smaller row groups. See [The Write Model](../concepts/write-model.md#memory-bounds). Must be positive. |
| `compression(Compression)` | `Compression.zstd()`, or `Compression.uncompressed()` when the ZSTD library is absent | Codec each page body is compressed with, and its level. See [Compression Codecs](#compression-codecs). |
| `dataPageVersion(DataPageVersion)` | `V1` | Header and layout of every data page. `V1` writes `DATA_PAGE`s, each body compressed whole. `V2` writes `DATA_PAGE_V2`s, which store the repetition and definition levels uncompressed ahead of the values, compress the values only where that makes them smaller, and state each page's row and null counts in its header. Dictionary pages are the same under either. |
| `encoding(ColumnEncoding)` | `AUTO` | Encoding policy for every column without an override of its own. |
| `encoding(String, ColumnEncoding)` | — | Encoding policy for one leaf column, overriding the file-wide default. |
| `bloomFilter(String)` | — | Writes a Bloom filter for one leaf column at a false-positive probability of `0.01`. A path naming no leaf column, or a `BOOLEAN` column, fails when the writer is created. See [Bloom filters](#bloom-filters). |
| `bloomFilter(String, double)` | — | Writes a Bloom filter for one leaf column at the given false-positive probability. Must be strictly between 0 and 1. |
| `statisticsTruncationLength(int)` | `64` | Longest `BYTE_ARRAY` `min` / `max` statistics bound. A longer bound is truncated and flagged inexact. Must be positive. |
| `precisionLossPolicy(PrecisionLossPolicy)` | `REJECT` | What the row-oriented layer does with a value carrying more precision than its column can hold. |
| `sortingColumns(SortingColumn...)`, `sortingColumns(List<SortingColumn>)` | none | The order every row group's rows are sorted in, recorded in each row group's metadata and checked as rows are written. See [Sorted Rows](#sorted-rows). |

A page is cut at whichever of the two page targets is reached first, and in any case at 2,147,483,638 values and nulls, the most a reader takes in one page; a single record holding more is a page of its own. A row group is cut at whichever of the two row-group targets is reached first.

Each option has a getter that reads the configured value back: `pageTargetBytes()`, `pageTargetRows()`, `rowGroupTargetRows()`, `rowGroupBufferTargetBytes()`, `compression()`, `dataPageVersion()`, `statisticsTruncationLength()`, `precisionLossPolicy()`, `sortingColumns()`, for the two encoding setters, `defaultEncoding()` and `columnEncodings()`, and for the Bloom filter setters, `bloomFilters()`, a map from column path to probability. Every setter rejects `null`, and the numeric bounds above are checked when the option is set.

## Sorted Rows

`sortingColumns` declares the order the rows of every row group are in: by the first column, rows equal there by the second, and so on. Each row group's metadata records the declaration as `sorting_columns`. A second call replaces the first.

```java
import static dev.hardwood.metadata.SortingColumn.NullOrder.NULLS_LAST;

WriterConfig config = WriterConfig.builder()
        .sortingColumns(
                SortingColumn.ascending("vendor", NULLS_LAST),
                SortingColumn.descending("pickup_ts", NULLS_LAST))
        .build();
```

| Factory | Order |
|---------|-------|
| `SortingColumn.ascending(String columnPath, NullOrder nullOrder)` | Least to greatest |
| `SortingColumn.descending(String columnPath, NullOrder nullOrder)` | Greatest to least |

`NullOrder` is `NULLS_FIRST` or `NULLS_LAST`. A column is named by its dotted leaf path, as for `encoding(String, ColumnEncoding)`.

Values compare in their column's order, the order its statistics use:

| Column | Order |
|--------|-------|
| `INT32`, `INT64` | Signed; unsigned under an unsigned `INT` annotation |
| `FLOAT`, `DOUBLE`, `FLOAT16` | Numeric; `NaN` after every number, `-0.0` equal to `0.0` |
| `BOOLEAN` | `false` before `true` |
| `DECIMAL` on `BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY` | By the number it holds |
| Other `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` | Unsigned, byte by byte; a prefix before the values it starts |

The writer checks the order within each row group as rows are written. The first row that sorts before the row ahead of it fails the write with an `IllegalArgumentException` naming the row, its row group and the column, and the writer accepts no more rows. Under `RowWriter`, rows are checked when their staged batch is written, which can be a later `writeRow`, `endRowGroup()` or `close()`. A new row group starts the order again.

When the writer is created, it rejects a sorting column that:

- names no leaf column of the schema;
- is repeated, or sits inside a list or map;
- has a type with no defined order: `INTERVAL`, `UNKNOWN`, `GEOMETRY` or `GEOGRAPHY`.

The builder rejects a `null` entry and a column declared twice.

## Encodings

`ColumnEncoding` sets how a column's values are encoded, file-wide or per leaf column. `AUTO` leaves the choice to the writer; every other member names an encoding outright, and a column that names one builds no dictionary.

| Policy | What it writes |
|--------|----------------|
| `AUTO` | Per column chunk, whichever of a dictionary or `PLAIN` is smaller, measured once the row group is buffered. |
| `PLAIN` | Values as they are: fixed-width types little-endian, `BYTE_ARRAY` behind a 4-byte length prefix. |
| `DELTA_BINARY_PACKED` | Differences between neighbouring values, bit-packed per block. Suits sorted or slow-moving integers. |
| `DELTA_LENGTH_BYTE_ARRAY` | Lengths delta-encoded ahead of the values, cheaper than `PLAIN`'s per-value length prefix where the lengths are similar. |
| `DELTA_BYTE_ARRAY` | Each value carrying only what it does not share with the value before it. Suits sorted values with common prefixes. |
| `BYTE_STREAM_SPLIT` | A value's bytes scattered across one stream per byte position. Changes no page's size on its own; it changes how well the codec afterwards compresses floating-point data. |
| `ALP` | Floating-point values scaled to integers by a power of ten and bit-packed per block of 1024 values. Suits values with few decimal digits, such as prices and measurements; a value with more significant digits than its block keeps is stored as it is, at its width plus 2 bytes. |

An encoding is legal only for some physical types:

| Policy | Physical types |
|---|---|
| `AUTO`, `PLAIN` | every writable type |
| `DELTA_BINARY_PACKED` | `INT32`, `INT64` |
| `DELTA_LENGTH_BYTE_ARRAY` | `BYTE_ARRAY` |
| `DELTA_BYTE_ARRAY` | `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` |
| `BYTE_STREAM_SPLIT` | `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `FIXED_LEN_BYTE_ARRAY` |
| `ALP` | `FLOAT`, `DOUBLE` |

A policy naming a column whose type cannot carry it fails when the writer is created, including a file-wide `BYTE_STREAM_SPLIT` over a schema holding a `BYTE_ARRAY` column. A per-column override names the column by its **dotted leaf path** as the schema spells it, synthetic segments included: `readings.list.element`, not `readings`. A path matching no leaf column is rejected the same way.

`AUTO` never chooses `ALP`. The format specification marks `ALP` as Preview, and a column written with it can be read only by readers that implement it. The page cut charges each `ALP` value its width plus 2 bytes, the most a value can take, so an `ALP` page holds fewer values than a `PLAIN` page and stays within `pageTargetBytes`.

No policy demands a dictionary; it is an outcome `AUTO` may arrive at. A `BOOLEAN` column is never dictionary-encoded, so `AUTO` resolves it to `PLAIN` whatever its values are.

## Compression Codecs

A `Compression` names the codec and, for the codecs that have them, a level. Each written codec has a factory:

| Factory | Codec | Library needed | Levels | Default level |
|---------|-------|----------------|--------|---------------|
| `Compression.uncompressed()` | `UNCOMPRESSED` | — | — | — |
| `Compression.gzip()` | `GZIP` | — (JDK) | 0 (stored, not compressed) to 9 | 6 |
| `Compression.snappy()` | `SNAPPY` | `org.xerial.snappy:snappy-java` | — | — |
| `Compression.zstd()` | `ZSTD` | `com.github.luben:zstd-jni` | -131072 to 22; 0 selects the default | 3 |
| `Compression.lz4Raw()` | `LZ4_RAW` | `at.yawk.lz4:lz4-java` | 1 to 17, through LZ4's high compressor (LZ4 HC) | LZ4's fast compressor |
| `Compression.brotli()` | `BROTLI` | `com.aayushatharva.brotli4j:brotli4j` | 0 to 11 | 11 |

`level(int)` on the factories of codecs with levels returns the codec at that level; a level outside the codec's range is rejected with `IllegalArgumentException`. A higher level writes a smaller file and takes longer to write; reading the file takes about as long at any level. The level is not recorded in the file.

```java
WriterConfig config = WriterConfig.builder()
        .compression(Compression.zstd().level(9))
        .build();
```

`Compression.of(CompressionCodec)` returns a codec chosen at runtime at its default level. It refuses `LZ4` and `LZO` with `UnsupportedOperationException`: `LZ4` files are read but not written, and `LZO` files are neither read nor written. A missing library fails when the writer is created rather than mid-file.

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

| Logical type | Physical type | `StructBuilder` setter | Java type |
|---|---|---|---|
| none | `BOOLEAN` | `setBoolean` | `boolean` |
| any | `INT32` | `setInt` | `int` |
| any | `INT64` | `setLong` | `long` |
| none | `FLOAT` | `setFloat` | `float` |
| none | `DOUBLE` | `setDouble` | `double` |
| `STRING`, `ENUM` or `JSON` | `BYTE_ARRAY` | `setString` | `String` (written as UTF-8) |
| any | `BYTE_ARRAY`<br>`FIXED_LEN_BYTE_ARRAY` | `setBinary` | `byte[]` |
| `DATE` | `INT32` | `setDate` | `LocalDate` |
| `TIME` | `INT32`<br>`INT64` | `setTime` | `LocalTime` |
| `TIMESTAMP(isAdjustedToUTC = true)` | `INT64`<br>`FIXED_LEN_BYTE_ARRAY(12)` | `setTimestamp` | `Instant` |
| `TIMESTAMP(isAdjustedToUTC = false)` | `INT64`<br>`FIXED_LEN_BYTE_ARRAY(12)` | `setLocalTimestamp` | `LocalDateTime` |
| `DECIMAL` | `INT32`<br>`INT64`<br>`BYTE_ARRAY`<br>`FIXED_LEN_BYTE_ARRAY` | `setDecimal` | `BigDecimal` |
| `UUID` | `FIXED_LEN_BYTE_ARRAY(16)` | `setUuid` | `UUID` |
| `INTERVAL` | `FIXED_LEN_BYTE_ARRAY(12)` | `setInterval` | `PqInterval` |
| any | any primitive | `setNull` | — |
| none / `LIST` / `MAP` | group | `setStruct` / `setList` / `setMap` | a filler over `StructBuilder` / `ListBuilder` / `MapBuilder` |
| `FILE` | group | `setFile` | `PqFile` |
| any | any | `setValue` | the form `getValue` returns, below |

`ListBuilder` has the same set as `addInt`, `addString`, `addStruct`, `addFile`, `addNull`, … and `MapBuilder` has `addEntry`, whose entry struct declares the two fields `key` and `value`.

A `FILE` group is set only through `setFile` / `addFile`; `setStruct` / `addStruct` on one throws. A group read from an existing file may declare fewer children, and a value setting a field the group has no child for is rejected. The columnar API writes a `FILE` group's children as ordinary columns, addressed as `photo.uri`, `photo.offset` and so on, and rejects a batch holding a present value that is not valid under the rules in [FILE groups](accessors.md#file-groups).

Timestamp columns split by `isAdjustedToUTC`, mirroring the reader's split between `getTimestamp` and `getLocalTimestamp`: using the wrong setter throws. Both APIs also accept every setter by field index; see [Addressing fields by index](../how-to/write-row-by-row.md#addressing-fields-by-index).

`setValue(String, Object)` and `setValue(int, Object)` take the value in the form `getValue` returns for the field's type and write it through that type's setter, with its checks. A `null` sets the field null; a value of any other Java type fails with `IllegalArgumentException`, and nothing is converted.

| Field | `setValue` takes |
|---|---|
| `BOOLEAN`, `INT32`, `INT64`, `FLOAT`, `DOUBLE` | `Boolean`, `Integer`, `Long`, `Float`, `Double` |
| `INT32` annotated `INT(8, true)` / `INT(16, true)` | `Byte` / `Short` |
| `STRING`, `ENUM`, `JSON` | `String` |
| `DATE`, `TIME`, `DECIMAL`, `UUID`, `INTERVAL` | `LocalDate`, `LocalTime`, `BigDecimal`, `UUID`, `PqInterval` |
| `TIMESTAMP(isAdjustedToUTC = true / false)` | `Instant` / `LocalDateTime` |
| `FLOAT16` | `Float`; a value without an exact half-precision representation is rejected |
| other `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` | `byte[]` |
| `FILE` group | `PqFile` |
| struct group, `LIST`, `MAP` | `PqStruct`, `PqList`, `PqMap` |

A `PqStruct` is copied by name: each field the target struct declares takes the source's field of that name, a field the source lacks stays unset, and a source field the target does not declare is not written. A `PqList`'s elements and a `PqMap`'s entries are copied in order the same way. A `PqVariant` is rejected.

A timestamp column's physical type sets the range of values it holds; its unit sets the precision on either:

| Unit | `INT64` holds | `FIXED_LEN_BYTE_ARRAY(12)` holds |
|---|---|---|
| `MILLIS` | about 292 million years either side of 1970 | every `Instant` and `LocalDateTime` |
| `MICROS` | about 292,000 years either side of 1970 | every `Instant` and `LocalDateTime` |
| `NANOS` | 1677-09-21T00:12:43.145224192 to 2262-04-11T23:47:16.854775807 | every `Instant` and `LocalDateTime` |

A value outside the range of an `INT64` column is rejected under either `PrecisionLossPolicy`.

## Value Ranges

Some annotations narrow what their physical type may hold. Where one does, both write APIs reject a stored value outside that range, naming the offending row or field:

| Annotation | Accepted values |
|---|---|
| `INT(n)` signed, `n` < physical width | `[-2^(n-1), 2^(n-1))` |
| `INT(n)` unsigned, `n` < physical width | `[0, 2^n)` |
| `TIME(unit, _)` | `[0, one day in unit)` — the annotation defines the value as elapsed time after midnight |
| `DECIMAL(p, s)` | an unscaled value of at most `p` digits |
| `UNKNOWN` | no value at all; every row must be null, so the column is set through a setter taking a null mask |

Every other annotation, `DATE`, `TIMESTAMP`, `INT(32)` and `INT(64)` included, admits every value of its physical type, and its column is not scanned per value. A large unsigned value is spelled as a negative, which is also how the reader returns it.

Two checks are not annotations and always apply: a `FIXED_LEN_BYTE_ARRAY` value must be exactly the length the column declares, and a present value of a binary column must not be `null`. The value at a row a `Validity` marks null is never encoded, so it is never checked.

An annotation over binary *content* is not a range, and the writer does not inspect it. A value passed to `bytes(...)` or `fixed(...)` is written as given, whether the column is annotated `STRING`, `ENUM`, `JSON`, `BSON`, `VARIANT`, `GEOMETRY` or `GEOGRAPHY`. Encoding a `STRING` column's values as UTF-8, and producing well-formed payloads under the others, is the caller's to do. Bytes that are not valid UTF-8 are written and read back as replacement characters rather than rejected. `StructBuilder.setString` takes a `String` and encodes it, so the row-oriented layer cannot produce that.

`PrecisionLossPolicy` governs **precision** only, and only on the row-oriented layer:

| Policy | Behaviour |
|---|---|
| `REJECT` (default) | Reject a value carrying digits the column's unit or scale cannot hold, naming the field. A value that is exact at the column's unit or scale is written normally. |
| `TRUNCATE` | Drop the digits that do not fit: a `TIME` / `TIMESTAMP` value is floored to the column's unit, a `DECIMAL` rescaled with `RoundingMode.DOWN`. |

A value the column cannot represent at all, such as a `LocalDate` beyond the `INT32` day range or an unscaled decimal wider than the declared precision, is rejected under either policy.

## Statistics Written

Every column chunk carries `null_count`. `min` / `max` are computed under the column's `ColumnOrder` during encoding and written to the preferred `min_value` / `max_value` fields; the deprecated `min` / `max` fields are not written.

- **Bounds are omitted where their order is undefined.** A column annotated `INTERVAL`, `UNKNOWN`, `VARIANT`, `GEOMETRY`, `GEOGRAPHY`, `LIST` or `MAP` writes its null count alone. An all-null chunk has no bounds to write either.
- **Truncation.** `BYTE_ARRAY` bounds longer than `statisticsTruncationLength` are truncated and flagged inexact (`is_min_value_exact` / `is_max_value_exact` = `false`). A column annotated `STRING`, `ENUM` or `JSON` is truncated at a character boundary, so its bounds are valid UTF-8. Fixed-width types write those flags as `true`.
- **`nan_count`** is written for every `FLOAT`, `DOUBLE` and `FLOAT16` chunk, including when it is zero. No other type writes the field.
- **`distinct_count`** is written where the chunk still knows its cardinality exactly: any chunk under `AUTO` that interned its values to the end, whichever encoding the flush-time comparison then chose, and any `BOOLEAN` chunk, which knows its cardinality without a dictionary. It is absent for any other chunk written under a named encoding, and for one that stopped interning part-way (see [How the Writer Picks an Encoding](../concepts/write-model.md#how-the-writer-picks-an-encoding)).

Every column chunk also carries `encoding_stats`: its page count per page type and encoding. A dictionary-encoded chunk lists one `PLAIN` dictionary page and all of its data pages as `RLE_DICTIONARY`, so a reader can prune its row group on the dictionary for an `eq` or `in` predicate (see [Query Controls](../how-to/query-controls.md)). Any other chunk lists its data pages under the one value encoding it was written in.

A column chunk carries `SizeStatistics` where it has something to count: its repetition level histogram where the column is repeated, its definition level histogram where the column's maximum definition level is 2 or more, and for a `BYTE_ARRAY` column the bytes of its values without their length prefixes.

Every file carries a page index, written between the last row group and the footer:

- **`OffsetIndex`**, for every column chunk: the location of each data page and the first record it holds, and for a `BYTE_ARRAY` column the bytes of the page's values without their length prefixes. Every page starts at a record boundary.
- **`ColumnIndex`**, for every column chunk whose bounds can be stated: each page's `min` / `max` under the same rules as the chunk statistics, its null count, whether it holds only nulls, its `NaN` count for `FLOAT`, `DOUBLE` and `FLOAT16`, its level histograms where the chunk's `SizeStatistics` carries them, and whether the pages' bounds ascend or descend. A column without an order writes none, nor does a floating-point column chunk with a page whose values are all `NaN`. Where no truncated `max` exists, a page's `max` is written whole.

The `GeospatialStatistics` of a `GEOMETRY` or `GEOGRAPHY` column are not written. [Bounding-box pushdown](../how-to/geospatial.md) prunes row groups from that last field, so it prunes nothing in a file Hardwood produced.

### Bloom filters

To let `eq` and `in` predicates on a high-cardinality column skip row groups, add a Bloom filter for the column with `bloomFilter(...)` (see [Writer Options](#writer-options)). The column then carries a split-block Bloom filter in every row group. [Bloom filters: skipping on equality](../concepts/parquet-layout.md#bloom-filters-skipping-on-equality) describes which columns benefit and what a filter costs.

- **Contents.** Every non-null value of the column chunk, hashed with XXH64 over its bytes as stored: `INT32` and `FLOAT` as 4 little-endian bytes, `INT64` and `DOUBLE` as 8, `BYTE_ARRAY` without its length prefix, `FIXED_LEN_BYTE_ARRAY` as is. `FLOAT` and `DOUBLE` values are hashed over their raw bits, so `-0.0` and `0.0` are distinct values, as are `NaN`s with different bits.
- **Size.** About 1.2 bytes per distinct value of the chunk at the default probability of `0.01`, rounded up to a power of two between 32 bytes and 128 MiB. Every chunk gets a filter whatever its encoding, an all-null chunk included.
- **Placement.** All filters of the file are written together after the last row group, before the page index, with `bloom_filter_offset` and `bloom_filter_length` set on each column chunk.

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

Entries reach the file in the order they were added. A `null` value writes a key carrying no value, which the format allows and which is what a reader reports as a `null`, so the map `FileMetaData.keyValueMetadata()` returns can be passed straight to `keyValueMetadata(Map)` to reproduce another file's metadata exactly. A file given no entries carries no `key_value_metadata` field at all.

Reading the field back is covered in [Read File Metadata](../how-to/metadata.md).

### `created_by`

The default identifier follows the convention Parquet readers parse, naming the library, its version and the build it came from:

```
hardwood version <version> (build <commit>)
```

`ParquetFileWriter.DEFAULT_CREATED_BY` holds it. `createdBy(String)` replaces it; readers that key compatibility workarounds off this field expect the `<app> version <version> (build <hash>)` shape, and a bare application name is rejected by some of them.

## Ending a Row Group

`ParquetFileWriter.endRowGroup()` writes the open row group, so the next record starts a new one. Records staged by `rowWriter()` are written into the row group being closed. When no record was written since the last row group closed, it writes nothing.

The row-group targets close row groups as well; a row group ends at a target or at a call, whichever comes first. A row group closed by a call is written exactly as one a target closes after the same records.

`endRowGroup()` and `close()` throw `IllegalStateException` when called from inside a `writeBatch` or `writeRow` filler. Any exception from `endRowGroup()` fails the writer.

## Writer Threads

Each batch's values are copied into the row group's column chunks in parallel, and at each row-group flush the column chunks are encoded and compressed in parallel, on the threads of a `HardwoodContext` and on the thread that called the write. Under a codec other than `UNCOMPRESSED` and with more than one available processor, the pages of one chunk are also compressed in parallel. The chunks are written in schema order. The file is byte-identical to one written on a single thread.

| Factory | Threads |
|---|---|
| `create(out, schema)`, `create(out, schema, config)` | The writer creates a context with one thread per available processor when it first needs one: on its first write of more than one column with more than one available processor, otherwise on its first flush of more than one column, or of one column under a codec other than `UNCOMPRESSED` with more than one available processor. It closes the context on `close()` or `abort()` |
| `create(out, schema, config, context)` | The threads of `context`, and on a context of zero threads (`HardwoodContext.create(0)`) the calling thread alone. The caller closes `context` after the writer; one context can serve several writers and readers |

A schema of one column is copied on the calling thread alone, and encoded on it alone under `UNCOMPRESSED` or with one available processor. With one available processor, values are copied on the calling thread alone. A `ParquetFileWriter` is still driven from one thread at a time.

## Finishing and Abandoning a File

| Call | Effect |
|---|---|
| `close()` | Writes the buffered row group and the footer, and publishes the file at the destination |
| `close()` after the writer has failed | Discards the output, leaving nothing at the destination |
| `abort()` | Discards the output and closes the writer |
| `close()` or `abort()` on a closed or aborted writer | Nothing |

The writer fails when `ColumnWriter.writeBatch`, `RowWriter.writeRow` or `endRowGroup` throws, whatever the exception: a batch or record rejected by the checks under [Rejected Input](#rejected-input), an exception thrown by the filler, a destination `IOException` or a codec failure. `RowWriter.tryWriteRow` fails the writer only on the exceptions described under [`RowWriteResult`](#rowwriteresult). A failed writer rejects further writes; `keyValueMetadata` and `createdBy` stay callable until `close()`.

A failure while `close()` finishes or publishes the file discards the output as well, and `close()` throws it. When the output cannot be discarded, `abort()` and `close()` on a failed writer throw the `IOException`; a failure while finishing or publishing carries it as a suppressed exception.

## `RowWriteResult`

`RowWriter.tryWriteRow` returns one of two members:

| Member | Meaning |
|---|---|
| `Staged` | The record is in the batch. A later flush can still fail the writer. |
| `Rejected(fieldPath, message)` | The record was not staged, and the staged batch is unchanged. `fieldPath` is the schema path the message names; `message` is the full text `writeRow` throws for the same rejection. |

A rejection that depends on the value handed over becomes `Rejected`, such as a value outside the range its annotation declares, a value the column cannot hold under the configured `PrecisionLossPolicy`, or a `REQUIRED` field left unset or set null.

Every other failure throws and fails the writer, as it does under `writeRow`: a defect in the calling code, such as an unknown field name or a setter that does not fit the field, an exception thrown by the filler, or a failure while a completed batch is written.

## Rejected Input

| Exception | When |
|---|---|
| `UnsupportedOperationException` | Something the format allows and the writer does not produce, such as a schema with no columns, an `INT96` column, the `LZ4` or `LZO` codec, or a [schema shape](#schema-shapes) outside what it writes. For a codec whose library is missing, the message names the dependency to add |
| `IllegalArgumentException` | The input does not fit the schema, the schema is one the format does not allow, or a configuration value lies outside what it accepts, such as a `MAP` key that is not `REQUIRED`, a setter that does not fit the column's type, a column left unset or set twice, a value outside the range its annotation declares, a `REQUIRED` field a record leaves unset, a row out of the declared sort order, or a compression level outside the codec's range |
| `IndexOutOfBoundsException` | A leaf-column index outside `[0, leaf column count)` on a `ColumnBatch` setter, or a field index outside `[0, getFieldCount())` on a `StructBuilder` setter |
| `IllegalStateException` | A call the writer's state does not allow at that point, such as a write after `close()` or after the writer has failed, using both write APIs on one file, or using a `ColumnBatch` after it has been submitted |
| `IOException` | The destination cannot be created, written, or finalized; an in-memory file would pass `Integer.MAX_VALUE - 8` bytes |
| `ParquetWriteException` | The file could not be produced for a reason that is neither the caller's input nor the destination: a compression codec that rejects a page body. Unchecked |

### Schema Shapes

Repetition is writable wherever a `LIST` or `MAP` annotation accounts for it. An annotated group is `REQUIRED` or `OPTIONAL` and holds exactly one `REPEATED` entry, which for a `MAP` is a group of `key` and `value`. That admits the canonical three-level `LIST` and two-level `MAP` layouts `FileSchema.builder` declares, and the legacy two-level lists a schema read from an existing file may carry: `LIST { repeated element }`, whose entry is the element, and `LIST { repeated group element { … } }`, whose entry is an element struct.

A `repeated` field outside a `LIST` or `MAP` group is rejected with `UnsupportedOperationException` when the writer is created. An annotated group that is itself `REPEATED`, holds other than one field, or holds an entry that is not `REPEATED` (or for a `MAP`, is not a group), a `MAP` whose entry group has no fields or whose key is not `REQUIRED`, and a `FILE` group with no children are rejected with `IllegalArgumentException`. A key-only map needs a `REQUIRED` key too, and an `OPTIONAL` key is rejected even if every key value would be present. These rules apply to schemas read from existing files as well as built ones.

The row API reaches a list's values through an element node below the entry, which the legacy two-level lists do not have, so `rowWriter()` refuses those two shapes and `columnWriter()` writes them. `rowWriter()` also requires sibling field names to be unique, which the `ColumnBatch` indices and dotted paths do not.

A column's maximum definition level and maximum repetition level must each be at most 255. A schema nested deeper is rejected with `UnsupportedOperationException` when the writer is created.
