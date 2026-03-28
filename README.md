# Hardwood

_A parser for the Apache Parquet file format, optimized for minimal dependencies and great performance._

Goals of the project are:

* Be light-weight: Implement the Parquet file format avoiding any 3rd party dependencies other than for compression algorithms (e.g. Snappy)
* Be correct: Support all Parquet files which are supported by the canonical [parquet-java](https://github.com/apache/parquet-java) library
* Be fast: As fast or faster as parquet-java
* Be complete: Add a Parquet file writer (after 1.0)

Latest version: 1.0.0.Alpha1, 2026-02-26

## Documentation

Full documentation is available at **[hardwood-hq.github.io](https://hardwood-hq.github.io/)**.

## Quick Start

```xml
<dependency>
    <groupId>dev.hardwood</groupId>
    <artifactId>hardwood-core</artifactId>
    <version>1.0.0.Alpha1</version>
</dependency>
```

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

See the [Getting Started](https://hardwood-hq.github.io/latest/getting-started/) guide for detailed setup instructions.

---

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

---

## Status

This is Alpha quality software, under active development.

Currently, individual Parquet files must be **at most 2 GB**.
Larger datasets should be split across multiple files and read via `MultiFileParquetReader`.

Note that while this project welcomes the usage of LLMs,
vibe coding (i.e. blindly accepting AI-generated changes without understanding them) is not accepted.
While there's currently a focus on quick iteration (closing feature gaps),
the aspiration is to build a high quality code base which is maintainable, extensible, performant, and safe.

See [ROADMAP.md](ROADMAP.md) for the detailed implementation status, roadmap, and milestones.

## Performance Testing

### Flat Files

These are the results from parsing files of the NYC Yellow Taxi Trip data set (subset 2016-01 to 2025-11, ~9.2GB overall, ~650M rows),
running on a Macbook Pro M3 Max.
The test (`FlatPerformanceTest`) parses all files and adds up the values of three columns (out of 20).
The results shown are for:

* The row reader API, using indexed access (mapping field names to indexes once upfront)
* The columnar reader API, using indexed access

```
====================================================================================================
PERFORMANCE TEST RESULTS
====================================================================================================

Environment:
  CPU cores:       16
  Java version:    25
  OS:              Mac OS X aarch64

Data:
  Files processed: 119
  Total rows:      651,209,003
  Total size:      9,241.1 MB
  Runs per contender: 5

Correctness Verification:
                              passenger_count     trip_distance       fare_amount
  Hardwood (multifile indexed)       972,078,547  2,701,223,013.48  9,166,943,759.83
  Hardwood (column reader multifile)       972,078,547  2,701,223,013.48  9,166,943,759.83

Performance (all runs):
  Contender                          Time (s)     Records/sec   Records/sec/core       MB/sec
  -----------------------------------------------------------------------------------------------
  Hardwood (multifile indexed) [1]         2.75     236,975,620         14,810,976       3362.8
  Hardwood (multifile indexed) [2]         2.78     234,669,911         14,666,869       3330.1
  Hardwood (multifile indexed) [3]         2.70     240,831,732         15,051,983       3417.6
  Hardwood (multifile indexed) [4]         2.70     240,831,732         15,051,983       3417.6
  Hardwood (multifile indexed) [5]         2.68     242,897,800         15,181,113       3446.9
  Hardwood (multifile indexed) [AVG]         2.72     239,239,163         14,952,448       3395.0
                                   min: 2.68s, max: 2.78s, spread: 0.09s

  Hardwood (column reader multifile) [1]         1.30     502,476,083         31,404,755       7130.5
  Hardwood (column reader multifile) [2]         1.11     584,568,225         36,535,514       8295.4
  Hardwood (column reader multifile) [3]         1.06     614,348,116         38,396,757       8718.0
  Hardwood (column reader multifile) [4]         1.06     616,091,772         38,505,736       8742.8
  Hardwood (column reader multifile) [5]         1.08     603,530,123         37,720,633       8564.5
  Hardwood (column reader multifile) [AVG]         1.12     580,917,933         36,307,371       8243.6
                                   min: 1.06s, max: 1.30s, spread: 0.24s

====================================================================================================
```

### Nested Files

These are the results from parsing a file with points of interest from the Overture Maps data set
(~900 MB, ~9M rows), running on a Macbook Pro M3 Max.
The test (`NestedPerformanceTest`) parses all columns of the file and determines min/max values, max array lengths, etc.
As above, the results shown are for the row reader API and the columnar API with indexed access.

```
====================================================================================================
NESTED SCHEMA PERFORMANCE TEST RESULTS
====================================================================================================

Environment:
  CPU cores:       16
  Java version:    25
  OS:              Mac OS X aarch64

Data:
  Total rows:      9,152,540
  File size:       882.2 MB
  Runs per contender: 5

Correctness Verification:
                               min_ver    max_ver       rows     websites      sources  addresses
  Hardwood (indexed)                 1          9  9,152,540    3,687,576   18,305,080  9,152,540
  Hardwood (columnar)                1          9  9,152,540    3,687,576   18,305,080  9,152,540

Performance (all runs):
  Contender                          Time (s)     Records/sec   Records/sec/core       MB/sec
  -----------------------------------------------------------------------------------------------
  Hardwood (indexed) [1]                 2.22       4,120,910            257,557        397.2
  Hardwood (indexed) [2]                 1.92       4,759,511            297,469        458.8
  Hardwood (indexed) [3]                 1.89       4,855,459            303,466        468.0
  Hardwood (indexed) [4]                 1.88       4,876,153            304,760        470.0
  Hardwood (indexed) [5]                 1.88       4,858,036            303,627        468.3
  Hardwood (indexed) [AVG]               1.96       4,674,433            292,152        450.6
                                   min: 1.88s, max: 2.22s, spread: 0.34s

  Hardwood (columnar) [1]                1.34       6,830,254            426,891        658.4
  Hardwood (columnar) [2]                1.32       6,918,020            432,376        666.8
  Hardwood (columnar) [3]                1.24       7,363,266            460,204        709.8
  Hardwood (columnar) [4]                1.24       7,404,968            462,810        713.8
  Hardwood (columnar) [5]                1.22       7,477,565            467,348        720.8
  Hardwood (columnar) [AVG]              1.27       7,189,741            449,359        693.0
                                   min: 1.22s, max: 1.34s, spread: 0.12s

====================================================================================================
```

---

## Build

This project requires **Java 25 or newer for building** (to create the multi-release JAR with Java 22+ FFM support). The resulting JAR runs on **Java 21+** (libdeflate support requires Java 22+).

**Docker must be running** for the build to succeed, as the test suite uses [Testcontainers](https://www.testcontainers.org/) to spin up services (e.g. S3 integration tests). If Docker is not available, the build will fail during the test phase for these tests.

It comes with the Apache [Maven wrapper](https://github.com/takari/maven-wrapper),
i.e. a Maven distribution will be downloaded automatically, if needed.

Run the following command to build this project:

```shell
./mvnw clean verify
```

On Windows, run the following command:

```shell
mvnw.cmd clean verify
```

Pass the `-Dquick` option to skip all non-essential plug-ins and create the output artifact as quickly as possible:

```shell
./mvnw clean verify -Dquick
```

Run the following command to format the source code and organize the imports as per the project's conventions:

```shell
./mvnw process-sources
```

### Building the Native CLI

The `hardwood` CLI can be compiled to a GraalVM native binary using the `-Dnative` flag.

#### macOS — local GraalVM build

Requires GraalVM (Java 25+) installed locally. Install via [SDKMAN](https://sdkman.io/):

```shell
sdk install java 25.0.2-graalce
```

Then build:

```shell
./mvnw -Dnative package -pl cli -am
```

The resulting distribution is at `cli/target/hardwood-<version>/bin/hardwood`.

#### Linux — container build (no local GraalVM required)

Requires Docker. The build runs inside a Linux container and produces a Linux x86\_64 ELF binary:

```shell
./mvnw -Dnative -Dquarkus.native.container-build=true package -pl cli -am
```

> **Note:** The container build always produces a Linux binary. Running it on macOS will fail with `exec format error`. Use the local GraalVM build for macOS binaries.

#### How the native build works

The CLI module uses [Quarkus](https://quarkus.io/) with `quarkus-picocli` and GraalVM/Mandrel native image. Several non-obvious pieces are required to make all compression codecs work correctly in a native binary.

**Compression codec native libraries**

All compression codecs (Snappy, ZSTD, LZ4, Brotli) ship their native code as JNI libraries inside their JARs. In a standard JVM application, each library extracts itself from the JAR at runtime via `Class.getResourceAsStream()`. This extraction mechanism does not work in a GraalVM native image.

The solution differs by codec:

- **ZSTD, Snappy, LZ4** — Native libraries are unpacked from their JARs during the Maven `prepare-package` phase (`maven-dependency-plugin`) and bundled in a `lib/` directory alongside the binary. At startup, `NativeImageStartup` fires a Quarkus `StartupEvent` which calls `NativeLibraryLoader` to load each library via `System.load(absolutePath)` before any decompression occurs. For ZSTD, `zstd-jni`'s `Native.assumeLoaded()` is also called to prevent the library's own loader from attempting a duplicate load. Snappy is handled the same way — its loader may have already run at image build time (and failed), so directly calling `System.load()` at runtime bypasses its cached failure state entirely.

- **Brotli** — `brotli4j`'s loader (`Brotli4jLoader.ensureAvailability()`) is invoked explicitly at decompression time rather than in a static initializer, so it never runs at build time. Its loading strategy — extracting a classpath resource to a temp file and loading that — works in native images provided the resource is embedded in the binary. The `resource-config.json` under `cli/src/main/resources/META-INF/native-image/` instructs GraalVM to embed the brotli native libraries as image resources.

- **libdeflate (GZIP acceleration)** — libdeflate uses the Java 22+ Foreign Function & Memory (FFM) API, which relies on runtime downcall handles that cannot be created inside a native image. `LibdeflateLoader` detects the native image context via the `org.graalvm.nativeimage.imagecode` system property and returns `isAvailable() = false`, dead-code-eliminating the entire FFM path. The `--initialize-at-build-time` directive in `core`'s `native-image.properties` ensures GraalVM constant-folds this check at image build time.

**Build arguments (application.properties)**

| Argument | Reason |
|---|---|
| `-march=compatibility` | Produces a binary targeting a generic x86\_64/arm64 baseline rather than the build machine's specific CPU generation. Without this, the binary may crash with `SIGILL` on older hardware. |
| `--gc=serial` | Replaces the default G1 garbage collector with the serial GC, removing GC infrastructure code from the binary. Appropriate for a short-lived CLI process and meaningfully reduces binary size. |
| `-J--enable-native-access=ALL-UNNAMED` | Passed to the JVM _running the Mandrel build process_ (not the native image itself). Required because GraalVM's image builder uses native access internally on JDK 21+. |
| `--initialize-at-run-time=...YamlConfiguration` | Prevents log4j's YAML configuration class from initializing at image build time, where it would attempt to load SnakeYAML and fail. |

**Logging dependencies**

`netty-buffer` (an optional dependency of `brotli4j`) is declared explicitly at compile scope so that GraalVM can resolve the `ByteBufUtil` reference in `brotli4j`'s `DirectDecompress` class during image analysis.

#### Manual tests of the native CLI binary

1. Start S3Mock and set environment

```bash
docker run -d --name s3mock -p 9090:9090 adobe/s3mock

export AWS_ENDPOINT_URL=http://localhost:9090
export AWS_ACCESS_KEY_ID=foo
export AWS_SECRET_ACCESS_KEY=bar
export AWS_REGION=us-east-1
export AWS_PATH_STYLE=true
```

2. Create bucket and upload with curl

```bash
curl -X PUT http://localhost:9090/test-bucket

curl -X PUT --data-binary @performance-testing/test-data-setup/target/tlc-trip-record-data/yellow_tripdata_2025-01.parquet \
    http://localhost:9090/test-bucket/yellow_tripdata_2025-01.parquet
```

3. Run hardwood CLI

```bash
cli/target/hardwood-cli-early-access-macos-aarch64/bin/hardwood info -f s3://test-bucket/yellow_tripdata_2025-01.parquet
```

### Building the Documentation

The documentation site can be previewed locally using Docker:

```shell
# Build the image (once, or after changing requirements.txt)
docker build -t hardwood-docs docs/

# Serve locally with hot reload — preview at http://127.0.0.1:8000
docker run --rm -p 8000:8000 -v "$(pwd)/docs:/docs" hardwood-docs

# Build static site (output in docs/site/)
docker run --rm -v "$(pwd)/docs:/docs" hardwood-docs build
```

### Running Claude Code

A Docker Compose set-up is provided for running [Claude Code](https://claude.ai/claude-code) with all build dependencies (Java 25, Maven, `gh`) pre-installed.

```shell
GH_TOKEN=<your-token> docker compose run --rm claude
```

Set `GH_TOKEN` to a GitHub personal access token so that Claude Code can interact with issues and pull requests. The project directory is mounted into the container at `/workspace`, and Claude Code configuration is persisted in a named volume across sessions.

### Creating a Release

See [RELEASING.md](RELEASING.md).

### Running Performance Tests

The performance testing modules are not included in the default build. Enable them with `-Pperformance-test`.

#### End-to-End Performance Tests

There are two end-to-end performance tests: one for flat schemas (NYC Yellow Taxi Trip data) and one for nested schemas (Overture Maps POI data). Test data is downloaded automatically on the first run.

```shell
./mvnw test -Pperformance-test
```

**Flat schema test** (`FlatPerformanceTest`) — reads ~9GB of taxi trip data (2016-2025, ~650M rows) and sums three columns.

| Property | Default | Description |
|----------|---------|-------------|
| `perf.contenders` | `HARDWOOD_MULTIFILE_INDEXED` | Comma-separated list of contenders, or `all` |
| `perf.start` | `2016-01` | Start year-month for data range |
| `perf.end` | `2025-11` | End year-month for data range |
| `perf.runs` | `10` | Number of timed runs per contender |

Available contenders: `HARDWOOD_INDEXED`, `HARDWOOD_NAMED`, `HARDWOOD_PROJECTION`, `HARDWOOD_MULTIFILE_INDEXED`, `HARDWOOD_MULTIFILE_NAMED`, `HARDWOOD_COLUMN_READER`, `HARDWOOD_COLUMN_READER_MULTIFILE`, `PARQUET_JAVA_INDEXED`, `PARQUET_JAVA_NAMED`.

**Nested schema test** (`NestedPerformanceTest`) — reads ~900MB of Overture Maps POI data (~9M rows) with deeply nested columns.

| Property | Default | Description |
|----------|---------|-------------|
| `perf.contenders` | `HARDWOOD_NAMED` | Comma-separated list of contenders, or `all` |
| `perf.runs` | `5` | Number of timed runs per contender |

Available contenders: `HARDWOOD_INDEXED`, `HARDWOOD_NAMED`, `HARDWOOD_COLUMNAR`, `PARQUET_JAVA`.

**Examples:**

```shell
# Run all contenders for the flat test, limited to 2025 data
./mvnw test -Pperformance-test -Dtest=FlatPerformanceTest -Dperf.contenders=all -Dperf.start=2025-01

# Compare multifile indexed vs named access
./mvnw test -Pperformance-test -Dperf.contenders=HARDWOOD_MULTIFILE_INDEXED,HARDWOOD_MULTIFILE_NAMED

# Run nested test only
./mvnw test -Pperformance-test -Dtest=NestedPerformanceTest -Dperf.contenders=all
```

#### PyArrow Comparison Tests

Python counterparts of the Java performance tests using PyArrow, for cross-platform comparison.
These scripts require a Python environment with PyArrow installed (use the `.venv` venv).

**Flat schema** (`flat_performance_test.py`) — counterpart of `FlatPerformanceTest.java`:

```shell
cd performance-testing/end-to-end

# Run all contenders (single-threaded and multi-threaded), 5 runs each
python flat_performance_test.py

# Single-threaded only
python flat_performance_test.py -c single_threaded

# Multi-threaded, 10 runs
python flat_performance_test.py -c multi_threaded -r 10
```

**Nested schema** (`nested_performance_test.py`) — counterpart of `NestedPerformanceTest.java`:

```shell
cd performance-testing/end-to-end

# Run all contenders, 5 runs each
python nested_performance_test.py

# Single-threaded only, 3 runs
python nested_performance_test.py -c single_threaded -r 3
```

**Options:**

| Flag | Default | Description |
|------|---------|-------------|
| `-c`, `--contenders` | `all` | Contenders to run: `single_threaded`, `multi_threaded`, or `all` |
| `-r`, `--runs` | `5` | Number of timed runs per contender |

**Notes on comparability:**

- The flat test uses column projection (reads only the 3 summed columns), matching the Hardwood projection and column-reader contenders. The parquet-java contenders in `FlatPerformanceTest.java` read all columns without projection, so direct comparison against parquet-java is not apples-to-apples.
- PyArrow uses vectorized columnar operations (C++ engine) rather than row-by-row iteration.
- The `single_threaded` contender (`use_threads=False`) is most comparable to single-threaded parquet-java; `multi_threaded` is comparable to Hardwood's parallel reading.

### API Change Report

To generate an API change report comparing the current build against a previous release:

```shell
./mvnw package japicmp:cmp -pl :hardwood-core -DskipTests -Djapicmp.oldVersion=<PREVIOUS_VERSION>
```

The `package` phase is needed to build the current jar before comparing. The report is written to `core/target/japicmp/`. Internal packages (`dev.hardwood.internal`) are excluded. This is run automatically during releases.

#### JMH Micro-Benchmarks

For detailed micro-benchmarks, build the JMH benchmark JAR and run it directly:

```shell
# Build the benchmark JAR
./mvnw package -Pperformance-test -pl performance-testing/micro-benchmarks -am -DskipTests

# Run all benchmarks (with Vector API for SIMD support)
java --add-modules jdk.incubator.vector \
  -jar performance-testing/micro-benchmarks/target/benchmarks.jar \
  -p dataDir=performance-testing/test-data-setup/target/tlc-trip-record-data

# Run a specific benchmark
java --add-modules jdk.incubator.vector \
  -jar performance-testing/micro-benchmarks/target/benchmarks.jar \
  "PageHandlingBenchmark.decodePages" \
  -p dataDir=performance-testing/test-data-setup/target/tlc-trip-record-data

# Run SIMD benchmark comparing scalar vs vectorized operations
java --add-modules jdk.incubator.vector \
  -jar performance-testing/micro-benchmarks/target/benchmarks.jar SimdBenchmark \
  -p size=1024,8192,65536 -p implementation=scalar,auto

# List available benchmarks
java --add-modules jdk.incubator.vector \
  -jar performance-testing/micro-benchmarks/target/benchmarks.jar -l
```

**Available benchmarks:**

| Benchmark | Description |
|-----------|-------------|
| `MemoryMapBenchmark.memoryMapToByteArray` | Memory map a file and copy to byte array |
| `PageHandlingBenchmark.a_decompressPages` | Scan and decompress all pages |
| `PageHandlingBenchmark.b_decodePages` | Scan, decompress, and decode all pages |
| `PipelineBenchmark.a_assembleColumns` | Synchronous page decoding + column assembly |
| `PipelineBenchmark.b_consumeRows` | Full pipeline with row-oriented access |
| `SimdBenchmark.*` | SIMD operations (countNonNulls, markNulls, dictionary, bit unpacking) |

**JMH options:**

| Option | Description |
|--------|-------------|
| `-wi <n>` | Number of warmup iterations (default: 3) |
| `-i <n>` | Number of measurement iterations (default: 5) |
| `-f <n>` | Number of forks (default: 2) |
| `-p param=value` | Set benchmark parameter |
| `-l` | List available benchmarks |
| `-h` | Show help |

**Note:** The taxi data files use GZIP compression (2016-01 to 2023-01) and ZSTD compression (2023-02 onwards). The default benchmark file is `yellow_tripdata_2025-05.parquet` (ZSTD, 75MB).

---

## License

This code base is available under the Apache License, version 2.

---

## Resources

- [Parquet Format Specification](https://github.com/apache/parquet-format)
- [Thrift Compact Protocol Spec](https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md)
- [Dremel Paper](https://research.google/pubs/pub36632/)
- [parquet-java Reference](https://github.com/apache/parquet-java)
- [parquet-testing Files](https://github.com/apache/parquet-testing)
- [arrow-testing Files](https://github.com/apache/arrow-testing)
