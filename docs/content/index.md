<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Hardwood

_A lightweight Java reader for the [Apache Parquet](https://parquet.apache.org/) file format.
Available as a Java library and a [command-line tool](cli.md)._

## Why Hardwood

Hardwood is built for applications that want Parquet read support without pulling in Hadoop, Avro, or the wider [parquet-java](https://github.com/apache/parquet-java) dependency tree. Use it when you need any of: a small runtime (zero transitive dependencies in the core), fast startup (suitable for native CLIs and short-lived processes), multi-threaded decode out of the box (pages decoded in parallel across a shared thread pool), direct S3 access (without `hadoop-aws`), or drop-in replacement of `parquet-java`'s `ParquetReader<Group>` API via the [compat module](compat.md).

## Goals

* **Light-weight** — implement the Parquet format with zero transitive dependencies beyond optional compression libraries (Snappy, ZSTD, LZ4, Brotli).
* **Compatible** — read every file that `parquet-java` reads, with documented divergences where Hardwood applies stricter semantics (e.g. SQL three-valued `notEq`).
* **Fast** — match or exceed `parquet-java`'s read throughput; remain competitive in native-image builds and short-lived JVMs.
* **Concurrent** — multi-threaded at the core: pages decode in parallel on a shared thread pool, with cross-file prefetching for multi-file reads.
* **Embeddable** — usable from native CLIs, S3-only pipelines, and Avro / Spark consumers via thin shim modules.

## Quick Example

```java
import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
    RowReader rowReader = fileReader.rowReader()) {

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

This is Beta quality software, under active development.

!!! warning "2 GB single-file limit"
    Individual Parquet files must currently be at most **2 GB**. Larger datasets should be split across multiple files and read by passing a list of `InputFile`s to `Hardwood.openAll(...)` or `ParquetFileReader.openAll(...)`. Tracked as [#75](https://github.com/hardwood-hq/hardwood/issues/75).

## Roadmap

Forward-looking items tracked for post-1.0. None are committed to a specific release.

- **Writer support** — write Parquet files in addition to reading; today Hardwood is reader-only. ([#9](https://github.com/hardwood-hq/hardwood/issues/9))
- **Bloom filter predicate pushdown** — use per-chunk bloom filters for equality-predicate skipping on high-cardinality columns, where min/max statistics can't help. ([#105](https://github.com/hardwood-hq/hardwood/issues/105))
- **Parquet Modular Encryption** — read files encrypted under the Parquet [Modular Encryption spec](https://github.com/apache/parquet-format/blob/master/Encryption.md): encrypted footer, per-column keys, AES-GCM and AES-GCM-CTR. ([#128](https://github.com/hardwood-hq/hardwood/issues/128))
- **Apache Arrow interop** — `ColumnReader` output as Arrow `FieldVector` / `VectorSchemaRoot` for zero-copy handoff to DuckDB, DataFusion, Pandas-via-JNI, and other Arrow-native consumers. ([#153](https://github.com/hardwood-hq/hardwood/issues/153))

## Getting help

- **Questions, ideas, design discussion** — [GitHub Discussions](https://github.com/hardwood-hq/hardwood/discussions). The best first stop for "how do I…", "is X possible…", or "what's the right way to…".
- **Bug reports and feature requests** — the [GitHub issue tracker](https://github.com/hardwood-hq/hardwood/issues). Please check whether a similar issue already exists.

## Package Structure

Hardwood is organized into public API packages and internal implementation packages. Application code should import only from the public packages; `dev.hardwood.internal.*` and its subpackages are implementation details and may change without notice.

| Package | Visibility | Purpose |
|---------|-----------|---------|
| [`dev.hardwood`](/api/latest/dev/hardwood/package-summary.html) | **Public API** | Entry point for creating readers and managing shared resources (thread pool, decompressor pool). |
| [`dev.hardwood.reader`](/api/latest/dev/hardwood/reader/package-summary.html) | **Public API** | Single-file and multi-file readers for row-oriented and column-oriented access. |
| [`dev.hardwood.metadata`](/api/latest/dev/hardwood/metadata/package-summary.html) | **Public API** | Parquet file metadata: row groups, column chunks, physical/logical types, and compression codecs. |
| [`dev.hardwood.schema`](/api/latest/dev/hardwood/schema/package-summary.html) | **Public API** | Schema representation: file schema, column schemas, and column projection. |
| [`dev.hardwood.row`](/api/latest/dev/hardwood/row/package-summary.html) | **Public API** | Value types for nested data access: structs, lists, and maps. |
| [`dev.hardwood.avro`](/api/latest/dev/hardwood/avro/package-summary.html) | **Public API** | Avro GenericRecord support: schema conversion and row materialization (`hardwood-avro` module). |
| [`dev.hardwood.s3`](/api/latest/dev/hardwood/s3/package-summary.html) | **Public API** | S3 object storage support: `S3Source`, `S3InputFile`, `S3Credentials`, `S3CredentialsProvider` (`hardwood-s3` module, zero external dependencies). |
| [`dev.hardwood.aws.auth`](/api/latest/dev/hardwood/aws/auth/package-summary.html) | **Public API** | Bridges the AWS SDK credential chain to Hardwood's `S3CredentialsProvider` (`hardwood-aws-auth` module, optional). |
| [`dev.hardwood.jfr`](/api/latest/dev/hardwood/jfr/package-summary.html) | **Public API** | JFR event types emitted during file reading, decoding, and pipeline operations. |
| `dev.hardwood.internal.*` | **Internal** | Implementation details — not part of the public API and may change without notice. |
