<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Hardwood

_A lightweight Java reader for the [Apache Parquet](https://parquet.apache.org/) file format.
Available as a Java library and a [command-line tool](reference/cli.md)._

!!! success "Hardwood 1.0 is out!"

    Hardwood 1.0 is released — read the [announcement blog post](https://www.morling.dev/blog/hardwood-1-0-fast-lightweight-apache-parquet-reader-for-the-jvm/) for the story behind the project and what it can do.

## Why Hardwood

Hardwood gives applications Parquet read support without pulling in Hadoop, Avro, or the wider [parquet-java](https://github.com/apache/parquet-java) dependency tree:

* **Light-weight** — zero transitive dependencies beyond optional compression libraries (Snappy, ZSTD, LZ4, Brotli).
* **Fast** — matches or exceeds `parquet-java`'s read throughput; competitive in native-image builds and short-lived JVMs.
* **Concurrent** — multi-threaded at the core: pages decode in parallel on a shared thread pool, with cross-file prefetching for multi-file reads.
* **Compatible** — reads every file that `parquet-java` reads, with documented divergences where Hardwood applies stricter semantics (e.g. SQL three-valued `notEq`).
* **Embeddable** — usable from native CLIs, S3-only pipelines (without `hadoop-aws`), and Avro / Spark consumers via thin shim modules, including a [drop-in `parquet-java` replacement](how-to/compat.md).

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

Ready? [Install Hardwood](getting-started.md), then read [your first file end-to-end](tutorial/first-read.md).

Prefer to learn by running code? The [hardwood-examples](https://github.com/hardwood-hq/hardwood-examples) repository collects small, self-contained examples — one per concept — that you can clone and run with a single command.

## Status and Limitations

Hardwood 1.0 is released and ready for production use.

The Hardwood library supports reading arbitrarily large Parquet files, provided individual column chunks are not larger than 2 GB (see [Parquet file layout](concepts/parquet-layout.md)).
The interactive `dive` TUI currently caps S3 files at 2 GB.

## Roadmap

Forward-looking items tracked for post-1.0. None are committed to a specific release.

- **Finalize `ColumnReader` API** — stabilize the API for columnar access and move it out of "Experimental" state. ([#522](https://github.com/hardwood-hq/hardwood/issues/522))
- **Writer support** — write Parquet files in addition to reading; today Hardwood is reader-only. ([#9](https://github.com/hardwood-hq/hardwood/issues/9))
- **Bloom filter predicate pushdown** — use per-chunk bloom filters for equality-predicate skipping on high-cardinality columns, where min/max statistics can't help. ([#105](https://github.com/hardwood-hq/hardwood/issues/105))
- **Parquet Modular Encryption** — read files encrypted under the Parquet [Modular Encryption spec](https://github.com/apache/parquet-format/blob/master/Encryption.md): encrypted footer, per-column keys, AES-GCM and AES-GCM-CTR. ([#128](https://github.com/hardwood-hq/hardwood/issues/128))
- **Apache Arrow interop** — `ColumnReader` output as Arrow `FieldVector` / `VectorSchemaRoot` for zero-copy handoff to DuckDB, DataFusion, Pandas-via-JNI, and other Arrow-native consumers. ([#153](https://github.com/hardwood-hq/hardwood/issues/153))

## Getting help

- **Questions, ideas, design discussion** — [GitHub Discussions](https://github.com/hardwood-hq/hardwood/discussions). The best first stop for "how do I…", "is X possible…", or "what's the right way to…".
- **Bug reports and feature requests** — the [GitHub issue tracker](https://github.com/hardwood-hq/hardwood/issues). Please check whether a similar issue already exists.

## Talks & posts

- [Hardwood: A New Parser for Apache Parquet](https://www.morling.dev/blog/hardwood-new-parser-for-apache-parquet/) — project announcement.
- [Open Source Friday with Gunnar Morling](https://www.youtube.com/watch?v=teqFSSQEtCw) — GitHub Open Source Friday.
- [Chasing Efficient Java Development: From 1BRC to Developing Hardwood AI Natively](https://www.infoq.com/podcasts/chasing-efficient-java-development/) — InfoQ podcast on building Hardwood.
