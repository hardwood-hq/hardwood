<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Hardwood

_A parser for the Apache Parquet file format, optimized for minimal dependencies and great performance.
Available as a Java library and a [command-line tool](cli.md)._

Goals of the project are:

* **Light-weight:** Implement the Parquet file format avoiding any 3rd party dependencies other than for compression algorithms (e.g. Snappy)
* **Correct:** Support all Parquet files which are supported by the canonical [parquet-java](https://github.com/apache/parquet-java) library
* **Fast:** Be as fast or faster as parquet-java
* **Complete:** Add a Parquet file writer (after 1.0)

## Quick Example

```java
import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
    RowReader rowReader = fileReader.createRowReader()) {

    while (rowReader.hasNext()) {
        rowReader.next();

        long id = rowReader.getLong("id");
        String name = rowReader.getString("name");
        LocalDate birthDate = rowReader.getDate("birth_date");
        Instant createdAt = rowReader.getTimestamp("created_at");
    }
}
```

See [Getting Started](getting-started.md) for installation and setup.

## Status

This is Alpha quality software, under active development.

## Package Structure

Hardwood is organized into public API packages and internal implementation packages:

| Package | Visibility | Purpose |
|---------|-----------|---------|
| `dev.hardwood` | **Public API** | Entry point for creating readers and managing shared resources (thread pool, decompressor pool). |
| `dev.hardwood.reader` | **Public API** | Single-file and multi-file readers for row-oriented and column-oriented access. |
| `dev.hardwood.metadata` | **Public API** | Parquet file metadata: row groups, column chunks, physical/logical types, and compression codecs. |
| `dev.hardwood.schema` | **Public API** | Schema representation: file schema, column schemas, and column projection. |
| `dev.hardwood.row` | **Public API** | Value types for nested data access: structs, lists, and maps. |
| `dev.hardwood.avro` | **Public API** | Avro GenericRecord support: schema conversion and row materialization (`hardwood-avro` module). |
| `dev.hardwood.s3` | **Public API** | S3 object storage support: `S3Source`, `S3InputFile`, `S3Credentials`, `S3CredentialsProvider` (`hardwood-s3` module, zero external dependencies). |
| `dev.hardwood.aws.auth` | **Public API** | Bridges the AWS SDK credential chain to Hardwood's `S3CredentialsProvider` (`hardwood-aws-auth` module, optional). |
| `dev.hardwood.jfr` | **Public API** | JFR event types emitted during file reading, decoding, and pipeline operations. |
| `dev.hardwood.internal.*` | **Internal** | Implementation details — not part of the public API and may change without notice. |
