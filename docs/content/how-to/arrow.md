<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Arrow Support

The `hardwood-arrow` module reads the columns of a Parquet file as Arrow `VectorSchemaRoot` batches through Arrow Java's `ArrowReader` contract, so Arrow's IPC writers, the C Data Interface and anything else built on `ArrowReader` consume the batches unchanged. Add it alongside `hardwood-core` and one of Arrow's memory backends:

```xml
<dependency>
    <groupId>dev.hardwood</groupId>
    <artifactId>hardwood-arrow</artifactId>
</dependency>
<dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>arrow-memory-unsafe</artifactId>
    <version>19.0.0</version>
    <scope>runtime</scope>
</dependency>
```

!!! warning "Experimental API"
    `ArrowReaders` and `ArrowBatchReader` are under active development; the columns they serve and the Arrow types they produce may change in future releases without prior deprecation.

`arrow-memory-netty` is the other backend; the module depends on neither. Arrow Java needs this JVM option on every JDK since 16:

```text
--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
```

On JDK 23 and later, `--sun-misc-unsafe-memory-access=allow` keeps the JVM from warning about the unsafe backend; a JDK before 23 does not know the option and refuses to start on it.

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

`ArrowReaders.buildBatchReader` takes a column projection, a filter predicate and a batch size, alone or combined:

```java
ArrowBatchReader reader = ArrowReaders.buildBatchReader(fileReader, allocator)
    .projection(ColumnProjection.columns("id", "name"))
    .filter(FilterPredicate.gt("id", 1000L))
    .batchSize(65_536)
    .build();
```

The settings have the semantics of [`ColumnReaders`](column-reader.md): every batch holds the rows matching the filter, which may name a column the projection leaves out, nested ones included; the batch size caps the rows per batch, and a batch also ends at every row-group boundary of a file with a binary column; a multi-file `ParquetFileReader` reads as one stream. The root's schema is available before the first batch: one field per projected top-level field in the order the projection first names them; a name listed twice gives one field, and a projection into a group keeps the group around the children it names.

## Columns the reader serves

The reader serves primitive columns and the groups around them: structs, `LIST` and `MAP` groups in every encoding the format allows, `repeated` fields outside a `LIST` or `MAP` group, which read as non-nullable lists, and Variant groups, which read as the struct of the fields they store. A projection that selects a map's key without its value or its value without its key fails at `build()` with an `UnsupportedOperationException` naming the map, before any column worker starts. A `repeated` Variant group outside a `LIST` or `MAP` group is the one shape core's readers refuse, and a projection touching one fails at `build()` with core's own error; a projection that leaves it out reads the rest of the file.

A Variant value arrives as its `metadata` and `value` bytes, plus the `typed_value` field of a shredded Variant as the file stores it: the reader reassembles nothing, so a shredded value's canonical bytes are only available through `RowReader.getVariant` (see [Read Variant Columns](variant.md)). The struct's layout is the storage of Arrow's Variant extension type, which a consumer may register over the field; the reader does not apply it.

## Type mapping

The Arrow type of a column follows from its physical type and annotation alone, the way Arrow C++ converts a Parquet schema when the file carries no Arrow metadata and extension types are off. An `ARROW:schema` entry in the file's key-value metadata is not read, so a file PyArrow wrote reads the same as one it did not.

| Parquet | Arrow |
|---|---|
| `BOOLEAN` | `Bool` |
| `INT32` | `Int32`; `INT(8/16/32, signed or unsigned)` as the matching `Int8` to `UInt32`; `DATE` as `Date32`; `TIME(MILLIS)` as `Time32(ms)`; `DECIMAL(p, s)` as `Decimal128(p, s)` |
| `INT64` | `Int64`; `INT(64, unsigned)` as `UInt64`; `TIME(MICROS/NANOS)` as `Time64`; `TIMESTAMP(unit)` as `Timestamp(unit, "UTC")` when adjusted to UTC and `Timestamp(unit)` otherwise; `DECIMAL(p, s)` as `Decimal128(p, s)` |
| `INT96` | `Timestamp(ns)` without a zone |
| `FLOAT`, `DOUBLE` | `Float32`, `Float64` |
| `BYTE_ARRAY` | `Binary`; `STRING` and `JSON` as `Utf8`; `ENUM`, `BSON`, `GEOMETRY` and `GEOGRAPHY` as `Binary`; `DECIMAL(p, s)` as `Decimal128` for `p` up to 38 and `Decimal256` above |
| `FIXED_LEN_BYTE_ARRAY(n)` | `FixedSizeBinary(n)`, also for `UUID`, `INTERVAL` and `TIMESTAMP` over 12 bytes; `FLOAT16` as `Float16`; `DECIMAL(p, s)` as for `BYTE_ARRAY` |
| any type annotated `NULL` | `Null` |
| unannotated group, `FILE` group | `Struct` of the projected children |
| `LIST` group | `List` of its element, in the three-level and the legacy two-level encodings alike |
| `MAP` group | `Map` whose entries struct, named `entries`, holds the key and the value |
| `repeated` field outside a `LIST` or `MAP` group | non-nullable `List` of non-nullable elements |
| `VARIANT` group | `Struct` of `metadata`, `value` and, when shredded, `typed_value` as stored |

A field is nullable when its column or group is `OPTIONAL`; a list's element, a map's value and a struct's child follow their own repetition, and a map's key and entries are never nullable. A column or group with a Parquet field id of zero or more carries it as the field's `PARQUET:field_id` metadata, as Arrow C++ 19 records it; a negative id is treated as none. A map's `entries` struct and the element of a `repeated` primitive outside a `LIST` or `MAP` group carry no id. The file's key-value metadata becomes the schema's metadata, except the `ARROW:schema` entry, which is neither read nor carried.

Values are converted where the layouts differ, with every present value checked: an `INT(8)` or `INT(16)` value outside its range, a decimal payload wider than its precision allows, or a value stored in a column annotated `NULL` fails the batch with a `ParquetReadException`, and an `INT96` timestamp outside the range of a 64-bit nanosecond count with an `UnsupportedOperationException`. A `DECIMAL(p, s)` with `p` above 76, or declared over more `FIXED_LEN_BYTE_ARRAY` bytes than the Arrow decimal of its precision holds, fails at `build()`.

Dictionary-encoded Parquet columns are read into plain vectors; the reader produces no dictionary-encoded Arrow field. The values of one batch of a `Binary` or `Utf8` column must fit an Arrow buffer with 32-bit offsets; a batch whose values would exceed that fails with an `IllegalStateException` naming the column, and reading with a smaller batch size avoids it.

## Lifecycle

Every batch is loaded into the one root `getVectorSchemaRoot()` returns, and loading a batch releases the buffers of the previous one. To keep a batch beyond the next `loadNextBatch()`, transfer its vectors out with a `TransferPair` or a `VectorUnloader` and release the transferred vectors yourself.

Every buffer comes from the `BufferAllocator` passed to `ArrowReaders`. The reader creates no allocator and does not close the one it is given; after `close()` it holds no memory from it, so an allocator whose only user was the reader reports zero allocated bytes once the reader and anything transferred out of it are closed. An allocator that cannot serve a batch fails `loadNextBatch()` with Arrow's `OutOfMemoryException`.

`ArrowBatchReader` owns the column readers it wraps and closes them, but not the `ParquetFileReader` they came from, which the caller closes separately. A failure while loading a batch releases what the batch had allocated, stops the underlying read, and leaves the reader unusable: a later `loadNextBatch()` throws `IllegalStateException`. `bytesRead()` counts the bytes of the Arrow buffers loaded so far.

## Writing Arrow IPC

The reader is an `ArrowReader`, so Arrow's IPC writers take its root and, as the dictionary provider, the reader itself:

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

`ArrowFileWriter` writes the file format the same way.

## Handing the stream to a C Data consumer

With `arrow-c-data` on the class path, `Data.exportArrayStream` exports the reader as an `ArrowArrayStream` that any C Data Interface consumer reads. DuckDB registers one as a table:

```java
try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
    ArrowBatchReader reader = ArrowReaders.batchReader(fileReader, allocator);
    Data.exportArrayStream(allocator, reader, stream);
    try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
         Statement statement = connection.createStatement()) {
        ((DuckDBConnection) connection).registerArrowStream("trips", stream);
        try (ResultSet result = statement.executeQuery("SELECT count(*) FROM trips")) {
            // ...
        }
    }
}
```

The exported stream owns the reader: the consumer releases the stream when it is done, which closes the reader, and the `ArrowArrayStream` handle is closed after the consumer has released it. The `ParquetFileReader` and the allocator outlive all three.
