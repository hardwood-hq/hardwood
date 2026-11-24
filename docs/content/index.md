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

    - **2026-11-24** — [Hardwood 1.1.0.CR1](https://www.morling.dev/blog/s3-writes-parallel-encoding-alp-hardwood-1-1-0-cr1/): writing to S3, parallel encoding, ALP, a rewritten read engine
    - **2026-10-12** — [Hardwood 1.1.0.Beta2](https://www.morling.dev/blog/row-group-control-faster-s3-reads-footer-caching-hardwood-1-1-0-beta2/): row group control in the writer, faster S3 reads, footer caching
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
* **Embeddable**: The Hardwood library can be used in GraalVM native binaries; a WASM build of `dive` runs in the browser as a [preview](/experiments/dive-web/)
* **Agent-friendly**: Hardwood's CLI comes with a skill which lets your agents inspect and analyse Parquet files
* **Compatible**: A [drop-in shim module](how-to/compat.md) facilitates migration from `parquet-java`, with documented divergences where Hardwood applies stricter semantics (e.g. SQL three-valued `notEq`)

The [CLI](reference/cli.md) inspects and analyses Parquet files and includes an interactive TUI for exploring a file's schema, row groups, pages, and data.

## Quick Example

Reading a file with the [row-based API](how-to/row-reader.md):

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

[Writing a file](how-to/write-row-by-row.md) row by row:

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

[Set up Hardwood in your project](getting-started.md), then read [your first file end-to-end](tutorial/first-read.md).
Alternatively, [install the Hardwood CLI](getting-started.md#command-line-tool) for working with Parquet files on the command line.

## Status and Limitations

The read path is stable and suitable for production use; `ColumnReader`, with its `Validity` and `LayerKind` types, is annotated `@Experimental`, so its shape may still change.
The write path ships both APIs described under [Choosing a Writer](how-to/index.md#choosing-a-writer); the row-oriented `RowWriter` and its builders, the `ColumnBatch` setters for nested and nullable columns, and the in-memory destination `InMemoryOutputFile` are annotated `@Experimental`, so their shape may still change.

Writing targets local files through `OutputFile.of(Path)`, the heap through `OutputFile.inMemory()`, and S3 through `S3Source.outputFile(...)` ([S3 writing](reference/s3.md#writing)).
Reads have no file-size ceiling, but a column chunk read without an OffsetIndex is capped at 2 GB, and some backends carry their own limits; see [Limits](reference/reader.md#limits).

## Roadmap

1.1, in its release candidate phase:

- **Write support**: `RowWriter` and `ColumnWriter` over nested structs, lists and maps, with the types, encodings and codecs listed in [Format Support](reference/format-support.md), statistics, page indexes and Bloom filters, written to local files, memory or S3, with column chunks encoded in parallel. ([#9](https://github.com/hardwood-hq/hardwood/issues/9))
- **Substantially improved read performance**: a rebuilt read engine that decodes every column on the context's pool, structs read without nested record assembly, and dictionary-encoded batches on `ColumnReader`.
- **Remote reads with fewer requests**: a file's footer and page index fetched with the request that opens it, later files of a multi-file read opened ahead, and parsed footers reused across readers through a `MetadataSource`.
- **More predicate pushdown**: row groups skipped by Bloom filters and dictionary pages, predicates on dictionary-encoded columns evaluated once per dictionary entry, and null tests on struct, `LIST` and `MAP` groups.
- **A reworked exception model**: a corrupt file raises `ParquetReadException`, and a read failure names the file and, where it has them, the row group and column it occurred in.
- **Hardened reads of untrusted files**: a size, count or length a file's bytes cannot back fails the read before anything is allocated from it. ([#610](https://github.com/hardwood-hq/hardwood/issues/610))
- **New format features**: the `ALP` encoding, read and written, `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)`, and the `FILE` logical type.

Planned for 1.2:

- **Parquet Modular Encryption**: read files encrypted under the [Modular Encryption spec](https://github.com/apache/parquet-format/blob/master/Encryption.md). ([#128](https://github.com/hardwood-hq/hardwood/issues/128))
- **Bounded read memory**: a heap quota per `HardwoodContext`, shared by the reads using it. ([#1859](https://github.com/hardwood-hq/hardwood/issues/1859))
- **Copying and rewriting files**: copy a file through the column and row APIs without a hand-written bridge, and rewrite or merge files by copying unchanged row groups. ([#1291](https://github.com/hardwood-hq/hardwood/issues/1291))
- **Complete Variant support**: writing Variant columns, and predicate pushdown and projection on shredded Variant paths. ([#1488](https://github.com/hardwood-hq/hardwood/issues/1488))
- **Stable `ColumnReader` API**: move columnar access out of `@Experimental`. ([#522](https://github.com/hardwood-hq/hardwood/issues/522))

Planned for a later release:

- **Apache Arrow interop**: `ColumnReader` output as Arrow `FieldVector` / `VectorSchemaRoot`. ([#153](https://github.com/hardwood-hq/hardwood/issues/153))

See the [GitHub milestones](https://github.com/hardwood-hq/hardwood/milestones) for the issues of past and upcoming releases, and [Format Support](reference/format-support.md) for the parts of the Parquet format Hardwood reads and writes.

## Getting help

Ask questions on [GitHub Discussions](https://github.com/hardwood-hq/hardwood/discussions) and file bugs on the [issue tracker](https://github.com/hardwood-hq/hardwood/issues); see [Contributing](contributing.md).

## Articles, talks & podcasts

- [Hardwood: A New Parser for Apache Parquet](https://www.morling.dev/blog/hardwood-new-parser-for-apache-parquet/) (original project announcement)
- [Building a New Parquet Engine with AI](https://www.youtube.com/watch?v=9Ov0Cn_ArHE) (Confluent Developer podcast)
- [Hardwood Promises High-Speed JVM Apache Parquet Processing with Zero Mandatory Dependencies](https://www.infoq.com/news/2026/07/hardwood-java-parquet/) (InfoQ article)
- [Hardwood: Building a Parquet Parser From Scratch (With a Little Help From AI)](https://speakerdeck.com/gunnarmorling/hardwood-building-a-parquet-parser-from-scratch-with-a-little-help-from-ai) (conference talk)
- [GitHub Open Source Friday with Gunnar Morling](https://www.youtube.com/watch?v=teqFSSQEtCw) (GitHub podcast)
- [Chasing Efficient Java Development: From 1BRC to Developing Hardwood AI Natively](https://www.infoq.com/podcasts/chasing-efficient-java-development/) (InfoQ podcast)
