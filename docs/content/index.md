<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Hardwood

_A modern lightweight Java reader and writer for the [Apache Parquet](https://parquet.apache.org/) file format.
Available as a Java library and a command-line tool._

!!! news "What's new"

    - **2026-08-31** — [Hardwood 1.1.0.Beta1](https://www.morling.dev/blog/parquet-file-write-support-bloom-filters-improved-performance-hardwood-1-1-0-beta1/): Parquet write support, Bloom filter pruning, faster reads
    - **2026-06-25** — [Hardwood 1.0](https://www.morling.dev/blog/hardwood-1-0-fast-lightweight-apache-parquet-reader-for-the-jvm/): the release announcement and the story behind the project
    - **2026-06-01** — [Confluent Developer Podcast, Ep. 31](https://www.youtube.com/watch?v=9Ov0Cn_ArHE): Gunnar Morling on building a new Parquet parser with AI

## Why Hardwood

Hardwood gives applications fast and efficient support for reading and writing Parquet, without pulling in Hadoop, Avro, or the wider [parquet-java](https://github.com/apache/parquet-java) dependency tree.
It is built to be:

* **Light-weight**: Zero transitive dependencies beyond optional compression libraries (Snappy, ZSTD, LZ4, Brotli)
* **Fast**: Hardwood aims to be the fastest Parquet reader and writer for the JVM ([1.0 read benchmarks](https://www.morling.dev/blog/hardwood-1-0-fast-lightweight-apache-parquet-reader-for-the-jvm/#_performance))
* **Complete**: Read and write support for flat and nested schemas, every logical type, every primitive type in current use, and the encodings and codecs in current use, with new format additions tracked as they land
* **Scalable**: Hardwood is multi-threaded at the core, pages are decoded in parallel, with cross-file prefetching for multi-file reads
* **Embeddable**: The Hardwood library can be used in GraalVM native binaries; WASM support coming soon ([preview](/experiments/dive-web/))
* **Agent-friendly**: Hardwood's CLI comes with a skill which lets your agents inspect and analyse Parquet files
* **Compatible**: A [drop-in shim module](how-to/compat.md) facilitates migration from `parquet-java`, with documented divergences where Hardwood applies stricter semantics (e.g. SQL three-valued `notEq`)

Besides the core library, Hardwood provides a ready-to-use CLI for inspecting and analysing Parquet files,
including an interactive TUI for exploring a file's schema, row groups, pages, and data.

## Quick Example

Here's how you read a file with the [row-based API](how-to/row-reader.md):

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

And here's how you [write a file](how-to/write-row-by-row.md):

```java
import dev.hardwood.OutputFile;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;

try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(path), schema)) {
    RowWriter rows = writer.rowWriter();

    for (Person person : people) {
        rows.writeRow(row -> row
                .setLong("id", person.id())
                .setString("name", person.name())
                .setDate("birth_date", person.birthDate()));
    }
}
```

Ready? [Set up Hardwood in your project](getting-started.md), then read [your first file end-to-end](tutorial/first-read.md).
Alternatively, [install the Hardwood CLI](reference/cli.md) for working with Parquet files on the command line.

Prefer to learn by running code? The [hardwood-examples](https://github.com/hardwood-hq/hardwood-examples) repository collects small, self-contained examples — one per concept — that you can clone and run with a single command.

## Status and Limitations

Hardwood 1.0 with read support is released and ready for production use.
Support for writing Parquet files is under active development as of Hardwood 1.1.

The Hardwood library supports reading arbitrarily large Parquet files, provided individual column chunks are not larger than 2 GB (see [Parquet file layout](concepts/parquet-layout.md)).
The interactive `dive` TUI currently caps S3 files at 2 GB.
Writing targets local files through `OutputFile.of(Path)` and the heap through `OutputFile.inMemory()`; output to object storage is coming soon.

## Roadmap

Forward-looking items tracked for post-1.0. None are committed to a specific release.

- **Finalize `ColumnReader` API** — stabilize the API for columnar access and move it out of "Experimental" state. ([#522](https://github.com/hardwood-hq/hardwood/issues/522))
- **Writer extensions** — object-store output, page-index and Bloom-filter writing, and parallel column encoding, on top of the write path described in [The Write Model](concepts/write-model.md). ([#9](https://github.com/hardwood-hq/hardwood/issues/9))
- **Bloom filter predicate pushdown** — use per-chunk bloom filters for equality-predicate skipping on high-cardinality columns, where min/max statistics can't help. ([#105](https://github.com/hardwood-hq/hardwood/issues/105))
- **Parquet Modular Encryption** — read files encrypted under the Parquet [Modular Encryption spec](https://github.com/apache/parquet-format/blob/master/Encryption.md): encrypted footer, per-column keys, AES-GCM and AES-GCM-CTR. ([#128](https://github.com/hardwood-hq/hardwood/issues/128))
- **Apache Arrow interop** — `ColumnReader` output as Arrow `FieldVector` / `VectorSchemaRoot` for zero-copy handoff to DuckDB, DataFusion, Pandas-via-JNI, and other Arrow-native consumers. ([#153](https://github.com/hardwood-hq/hardwood/issues/153))

## Getting help

- **Questions, ideas, design discussion** — [GitHub Discussions](https://github.com/hardwood-hq/hardwood/discussions). The best first stop for "how do I…", "is X possible…", or "what's the right way to…".
- **Bug reports and feature requests** — the [GitHub issue tracker](https://github.com/hardwood-hq/hardwood/issues). Please check whether a similar issue already exists.

## Articles, talks & podcasts

- [Hardwood: A New Parser for Apache Parquet](https://www.morling.dev/blog/hardwood-new-parser-for-apache-parquet/) (original project announcement)
- [Building a New Parquet Engine with AI](https://www.youtube.com/watch?v=9Ov0Cn_ArHE) (Confluent Developer podcast)
- [Hardwood Promises High-Speed JVM Apache Parquet Processing with Zero Mandatory Dependencies](https://www.infoq.com/news/2026/07/hardwood-java-parquet/) (InfoQ article)
- [Hardwood: Building a Parquet Parser From Scratch (With a Little Help From AI)](https://speakerdeck.com/gunnarmorling/hardwood-building-a-parquet-parser-from-scratch-with-a-little-help-from-ai) (conference talk)
- [GitHub Open Source Friday with Gunnar Morling](https://www.youtube.com/watch?v=teqFSSQEtCw) (GitHub podcast)
- [Chasing Efficient Java Development: From 1BRC to Developing Hardwood AI Natively](https://www.infoq.com/podcasts/chasing-efficient-java-development/) (InfoQ podcast)
