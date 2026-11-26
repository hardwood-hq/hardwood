<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Arrow Support

The `hardwood-arrow` module converts Parquet schemas to Apache Arrow and reads Parquet files as Arrow `VectorSchemaRoot` batches through Arrow Java's `ArrowReader` contract, so Arrow's IPC writers and the C Data Interface consume them directly. Add it alongside `hardwood-core` and one of Arrow's memory backends (`arrow-memory-unsafe` or `arrow-memory-netty`):

```xml
<dependency>
    <groupId>dev.hardwood</groupId>
    <artifactId>hardwood-arrow</artifactId>
</dependency>
<dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>arrow-memory-unsafe</artifactId>
    <version>20.0.0</version>
    <scope>runtime</scope>
</dependency>
```

!!! warning "Experimental API"
    `ArrowReaders` and `ArrowBatchReader` are under active development and may change in future releases without prior deprecation.

Arrow Java requires `--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED` on JDK 16+ (and on JDK 23+, `--sun-misc-unsafe-memory-access=allow` silences warnings from `arrow-memory-unsafe`).

Convert a schema without starting column workers or passing an allocator:

```java
Schema fullSchema = ArrowReaders.schema(fileReader);
Schema projectedSchema = ArrowReaders.schema(fileReader, ColumnProjection.columns("id", "name"));
```

Read a file batch by batch:

```java
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import dev.hardwood.InputFile;
import dev.hardwood.arrow.ArrowBatchReader;
import dev.hardwood.arrow.ArrowReaders;
import dev.hardwood.reader.ParquetFileReader;

try (BufferAllocator allocator = new RootAllocator();
     ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
     ArrowBatchReader reader = ArrowReaders.batchReader(fileReader, allocator)) {

    VectorSchemaRoot root = reader.getVectorSchemaRoot();
    while (reader.loadNextBatch()) {
        BigIntVector ids = (BigIntVector) root.getVector("id");
        VarCharVector names = (VarCharVector) root.getVector("name");
        for (int i = 0; i < root.getRowCount(); i++) {
            if (!names.isNull(i)) {
                process(ids.get(i), names.getObject(i).toString());
            }
        }
    }
}
```

`ArrowReaders.buildBatchReader` supports column projection, record and row-group predicates, batch size, and dictionary-encoded output, with the semantics of [`ColumnReaders`](column-reader.md):

```java
ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, allocator)
    .projection(ColumnProjection.columns("id", "name"))
    .filter(FilterPredicate.gt("id", 1000L))
    .filter(RowGroupPredicate.byteRange(splitStart, splitEnd))
    .batchSize(65_536)
    .dictionaryEncoding(true)
    .build();
```

Multi-file `ParquetFileReader` instances read as one stream. Projection orders top-level fields by first mention and preserves enclosing structs around projected children; selecting only the key or only the value of a `MAP` fails at `build()`.

## Dictionary-encoded output

By default, dictionary-encoded Parquet columns expand into plain Arrow vectors. To pass Parquet dictionaries directly into `Int32`-indexed Arrow dictionary vectors backed by `ArrowReader`'s `DictionaryProvider`:

- `.dictionaryEncoding(true)` selects every projected `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY`, and `INT96` column (other than `UNKNOWN`-annotated columns) whose footer metadata uses Parquet dictionary encoding. On a multi-file reader this checks the first file's footer; use `.dictionaryColumns(...)` when later files introduce dictionary pages that the first lacks.
- `.dictionaryColumns("label", "address.city")` selects named leaf columns explicitly using dot-separated Parquet field paths.

Each dictionary page is loaded once per row group and reused across all batches from that row group; a new row group or plain-page fallback replaces the dictionary transparently. `ArrowStreamWriter` and `Data.exportArrayStream` handle replacement dictionaries across batches; `ArrowFileWriter` is limited to single-dictionary streams.

## Type mapping

Columns follow Arrow C++'s metadata-free Parquet mapping (no `ARROW:schema` restoration, no extension types):

| Parquet | Arrow |
|---|---|
| `BOOLEAN` | `Bool` |
| `INT32` | `Int32`; `INT(8/16/32)` as `Int8`–`UInt32`; `DATE` as `Date32`; `TIME(MILLIS)` as `Time32(ms)`; `DECIMAL(p, s)` as `Decimal128(p, s)` |
| `INT64` | `Int64`; `INT(64, unsigned)` as `UInt64`; `TIME(MICROS/NANOS)` as `Time64`; `TIMESTAMP(unit)` as `Timestamp(unit, "UTC")` when UTC-adjusted or without a zone otherwise; `DECIMAL(p, s)` as `Decimal128(p, s)` |
| `INT96` | `Timestamp(ns)` without a zone |
| `FLOAT`, `DOUBLE` | `Float32`, `Float64` |
| `BYTE_ARRAY` | `Binary`; `STRING` and `JSON` as `Utf8`; `ENUM`, `BSON`, `GEOMETRY`, `GEOGRAPHY` as `Binary`; `DECIMAL(p, s)` as `Decimal128` (`p <= 38`) or `Decimal256` (`p <= 76`) |
| `FIXED_LEN_BYTE_ARRAY(n)` | `FixedSizeBinary(n)` (including `UUID`, `INTERVAL`, `TIMESTAMP`); `FLOAT16` as `Float16`; `DECIMAL(p, s)` as for `BYTE_ARRAY` |
| any type annotated `UNKNOWN` | `Null` |
| struct / `FILE` group | `Struct` of projected children |
| `LIST` group (2- or 3-level) | `List` of its element |
| `MAP` group | `Map` with non-nullable `entries` struct holding `key` and `value` |
| bare `repeated` field | non-nullable `List` of non-nullable elements |
| `VARIANT` group | `Struct` of `metadata`, `value`, and (when shredded) `typed_value` as stored |

`OPTIONAL` fields are nullable; `REQUIRED` children inside `OPTIONAL` structs stay non-nullable with zero nulls (masked by the enclosing struct's validity bitmap). Non-negative Parquet field ids are carried as `PARQUET:field_id` field metadata, and file key-value metadata (except `ARROW:schema`) becomes schema metadata. Shredded Variant values are not reassembled into canonical bytes; use `RowReader.getVariant` for that (see [Read Variant Columns](variant.md)).

Present values are validated during conversion: out-of-range narrow integers, oversized decimals, or non-null values in `UNKNOWN`-annotated columns fail with `ParquetReadException`; out-of-range `INT96` timestamps fail with `UnsupportedOperationException`; batches whose binary bytes exceed 32-bit offsets fail with `IllegalStateException` (use a smaller batch size).

## Lifecycle

Each `loadNextBatch()` reuses the single `VectorSchemaRoot` from `getVectorSchemaRoot()` and releases the previous batch's buffers; transfer vectors out with `TransferPair` or `VectorUnloader` to keep them. All buffers come from the caller's `BufferAllocator`, which the reader does not close. Closing the reader closes its `ColumnReaders` (not the `ParquetFileReader`) and releases all Arrow memory; a failed load releases the batch's buffers, stops the read, and marks the reader unusable.

## Arrow IPC and C Data Interface

Because `ArrowBatchReader` extends `ArrowReader`, Arrow's IPC writers and C Data Interface consume it directly:

```java
try (ArrowBatchReader reader = ArrowReaders.batchReader(fileReader, allocator);
     ArrowStreamWriter writer = new ArrowStreamWriter(reader.getVectorSchemaRoot(), reader, outputStream)) {
    writer.start();
    while (reader.loadNextBatch()) {
        writer.writeBatch();
    }
    writer.end();
}
```

With `arrow-c-data` on the class path, `Data.exportArrayStream(allocator, reader, stream)` exports an `ArrowArrayStream` (for example to DuckDB via `DuckDBConnection#registerArrowStream`). The exported stream takes ownership of the reader and closes it when the consumer releases the stream.
