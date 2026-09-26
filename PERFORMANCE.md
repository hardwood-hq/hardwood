# Performance

## Benchmark Results

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

## Running Performance Tests

The performance testing modules are not included in the default build. Enable them with `-Pperformance-test`.

### End-to-End Performance Tests

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

### PyArrow Comparison Tests

Python counterparts of the Java performance tests using PyArrow, for cross-platform comparison.
These scripts require a Python environment with PyArrow installed: activate the `.docker-venv` venv at the repository root (`source .docker-venv/bin/activate`).

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

### JMH Micro-Benchmarks

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
  "PageHandlingBenchmark.b_decodePages" \
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
| `SimdBenchmark.*` | SIMD operations (countNonNulls, dictionary) |
| `WideSchemaMetadataBenchmark.decodeFooter` | Thrift footer decode for 10 … 100,000 `FLOAT64` columns, bytes already in memory |
| `WideSchemaMetadataBenchmark.buildSchema` | `FileSchema` construction from decoded schema elements, same widths |
| `WideSchemaMetadataBenchmark.openFile` | Full `ParquetFileReader.open()`: mmap, footer read, decode, schema build |
| `WideSchemaMetadataParquetJavaBenchmark.*` | The same three steps through parquet-java, for comparison |

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

## Micro-Benchmark Conventions

These conventions apply to the JMH benchmarks in `performance-testing/micro-benchmarks`. They keep a number meaningful: it measures the cost the benchmark names, every contender does the same work, and a run on one branch can be compared with a run on another.

### Module Layout

The module is built only under `-Pperformance-test`. JMH 1.37 runs as an annotation processor, and the shade plugin packages the module into `target/benchmarks.jar` with `org.openjdk.jmh.Main` as its entry point. Each benchmark class carries its own run instructions in its JavaDoc.

| Package | Contents |
|---------|----------|
| `dev.hardwood.benchmarks` | Single-class benchmarks (decoders, page handling, dictionaries, SIMD, the write benchmarks) and the shared helpers: `Elem`, `NestedReads`, `BenchmarkWriter`, `BenchmarkData`, `FlatWriteFixture`, `MemoryOutputFile` |
| `dev.hardwood.benchmarks.nested` | `LIST<primitive>` and `LIST<annotated>` read benchmarks, their corpus generators and correctness gates |
| `dev.hardwood.benchmarks.mixed` | Schema-composition read benchmarks (scalars beside lists, structs, multi-layer repetition), generator and gate |
| `dev.hardwood.benchmarks.wide` | Wide-schema footer and schema-construction benchmarks, with a parquet-java counterpart |

parquet-java (`parquet-avro`, `parquet-hadoop`, with `hadoop-common` and `hadoop-mapreduce-client-core`) is on the module's classpath for two purposes only: the corpus generators write fixtures with it, and it is the contender in the benchmarks that compare against it. A Hardwood read benchmark reads through Hardwood alone.

### Harness Settings

The nested, mixed, wide and write benchmarks use these settings, and a new benchmark starts from them. Some older single-class benchmarks run one fork, or five.

| Setting | Value |
|---------|-------|
| Mode and unit | `@BenchmarkMode(Mode.AverageTime)`, `@OutputTimeUnit(TimeUnit.MILLISECONDS)` |
| State | `@State(Scope.Benchmark)`, set up and torn down per trial (`Level.Trial`) |
| Iterations | `@Warmup(iterations = 3, time = 1)`, `@Measurement(iterations = 5, time = 1)` |
| Forks | `@Fork(2)`, so that JIT and heap-layout variance between JVMs shows up in the reported error instead of being folded into one fork's iterations |
| Fork JVM arguments | explicit `-Xms` and `-Xmx`, and `--add-modules jdk.incubator.vector` where the code under test may use the Vector API, passed with `jvmArgsAppend` |
| Logging | `src/main/resources/log4j.properties` pins the root logger to `WARN` |
| Reader resources | one `HardwoodContext` per trial, closed in `@TearDown` |

A benchmark that reads a `-Dperf.*` property in its setup must pass its fork arguments with `jvmArgsAppend`. `jvmArgs` replaces the inherited command line, so the forked JVM loses every `-D` given on the `java -jar` invocation and the benchmark runs at its defaults without reporting it.

The logging setting matters for any benchmark that writes through parquet-java's record API: with Log4j 1.x at its default `DEBUG` level, parquet-java's `MessageColumnIO` formats a log message per field per record, and the benchmark measures logging instead of encoding.

### Data

A benchmark gets its data in one of three ways:

| Source | Used by | Location |
|--------|---------|----------|
| Downloaded taxi corpus | `PageHandlingBenchmark`, `MemoryMapBenchmark` | `performance-testing/test-data-setup/target/tlc-trip-record-data`, passed with `-p dataDir=...` |
| Generated corpus files | the nested, mixed and wide benchmarks; some single-class benchmarks through the `performance-testing/generate_*.py` scripts | `performance-testing/test-data-setup/target/benchmark-data`, overridable with `-Dperf.dataDir` |
| In-memory fixture | `FlatWriteBenchmark`, `WriteEncodingBenchmark` | built per trial by `FlatWriteFixture` |

Generated data is deterministic: every generator draws from a `java.util.Random` with a fixed seed, so every machine measures the same bytes and a number from one month is comparable with one from the next.

The Java corpus generators (`NestedListFileGenerator`, `NestedLogicalTypeFileGenerator`, `MixedSchemaFileGenerator`) write through `BenchmarkWriter`, which wraps parquet-java's `AvroParquetWriter` with a fixed configuration: DataPageV2 (V1 with `-Dperf.pageVersion=v1`), uncompressed, no dictionary, 3-level compliant lists. Each generator exposes idempotent `ensure...` methods, which the benchmark calls from its trial setup so a fork never measures a missing file, and a `main` that generates the corpus up front. A file that exists and is non-empty is left untouched.

| Property | Default | Meaning |
|----------|---------|---------|
| `perf.dataDir` | `performance-testing/test-data-setup/target/benchmark-data` | Corpus directory |
| `perf.totalValues` | 8,000,000 | Leaf values per list, repeated-heavy and multi-layer fixture |
| `perf.rows` | 2,000,000 for the mixed and struct fixtures; 1,000,000 for `FlatWriteFixture` | Row count |
| `perf.pageVersion` | `v2` | Data page version of generated fixtures |
| `perf.dir` | unset (write to memory) | Output directory for the write benchmarks |

A fixture's file name encodes every setting that shapes its content: `<stem>_<pageVersion>_<size>.parquet`, where the stem carries the `@Param` values (element type, null density) and the size is `perf.totalValues` or `perf.rows`, e.g. `list_int64_none_v2_8000000.parquet`. A changed setting therefore generates a new file instead of reusing one written under other settings. `perf.pageVersion` accepts only `v1` and `v2`, and a `perf.totalValues` or `perf.rows` that does not parse as a number is rejected with an `IllegalArgumentException` rather than replaced by the default. A generator run up front takes the same `-Dperf.*` values as the benchmark it prepares.

### Nested Read Benchmarks

The nested read benchmarks isolate the cost of reconstructing nested values from repetition and definition levels. Every leaf is numeric and every fixture is uncompressed and dictionary-free, so the time is level handling plus value copy, without string decoding, codec or dictionary cost. Every list, repeated-heavy and multi-layer fixture holds the same total leaf-value count, so ms/op compares directly across shapes.

Each nested fixture has a **flat twin**: a file holding the identical leaf stream (same values, same null positions) as a plain primitive column. Reading the twin through `ColumnReader` is the decode floor. The gap between a nested contender and the floor is the reconstruction cost; a change to the nested path should move the nested numbers toward the floor while the floor stays put.

| Benchmark | Contenders | Parameters |
|-----------|------------|------------|
| `NestedListReadBenchmark` | `columnNested` (column reader, compacted leaf values and leaf validity), `columnNestedStructural` (column reader, walking the per-layer offsets and validity), `rowNested` (row reader, a `PqList` per row), `flatFloor` | `elem` ∈ {`int64`, `float64`}; `nullDensity` ∈ {`none`, `sparse`, `dense`} |
| `NestedMultiListReadBenchmark` | `columnMulti`, `rowMulti`, `flatMultiFloor`: several list columns folded by one consumer thread | as above |
| `NestedLogicalTypeReadBenchmark` | `typedView` (the accessor that names its return type), `genericView` (`values()`) over `LIST<annotated>` | `elem` ∈ {`date`, `timestamp`, `decimal`}; `nullDensity` ∈ {`none`, `sparse`} |
| `MixedSchemaReadBenchmark` | `columnScalarsMixed` vs `columnScalarsFlat`, `columnStruct` vs `columnStructFlat`, `columnRepeatedHeavy`, and `column`/`row` variants of `ListOfList` and `ListOfStruct` | none; every shape is a fixed fixture |

At `none` the list and its elements are required. At `sparse` about 5% of elements and 5% of lists are null, at `dense` about 50% of elements and 10% of lists; nulls break up the contiguous runs of present values a bulk copy depends on. List lengths are drawn uniformly around a mean (never empty), so the offset scan does not see a constant stride. `MixedSchemaReadBenchmark` compares pairs of files: the same 12 scalar columns with and without two list columns beside them, a non-repeated struct against the same leaves as top-level columns, and the shapes with more than one repetition layer (`LIST<LIST<double>>`, `LIST<STRUCT<...>>`), which serve as no-regression guards.

Each family has a **correctness gate**, a standalone `main` that generates the corpus and folds every contender to a numeric checksum, then asserts that paths reading the same values agree (within a relative epsilon of 1e-6, since the paths sum in different orders):

```shell
java -cp performance-testing/micro-benchmarks/target/benchmarks.jar dev.hardwood.benchmarks.nested.NestedListGate [dataDir]
java -cp performance-testing/micro-benchmarks/target/benchmarks.jar dev.hardwood.benchmarks.nested.NestedLogicalTypeGate [dataDir]
java -cp performance-testing/micro-benchmarks/target/benchmarks.jar dev.hardwood.benchmarks.mixed.MixedSchemaGate [dataDir]
```

Run the gate before trusting the timings of a change to the read path; a change that corrupts values then fails the gate instead of posting a fast wrong number.

### Flat Write Benchmark

`FlatWriteBenchmark` is the write-side counterpart of `FlatPerformanceTest`: it encodes a flat, taxi-shaped fixture through Hardwood's write APIs and through parquet-java.

| Method | API |
|--------|-----|
| `hardwoodColumnar` | `ParquetFileWriter.columnWriter()`, `ColumnWriter.writeBatch` in 1024-row batches |
| `hardwoodRow` | `ParquetFileWriter.rowWriter()`, `RowWriter.writeRow` with named setters |
| `hardwoodRowByIndex` | the row writer with field indices instead of names |
| `hardwoodRowByIndexRaw` | the row writer with field indices, pre-encoded `BYTE_ARRAY` values and timestamps in stored units: the same inputs the columnar method receives |
| `parquetJavaGroup` | `ExampleParquetWriter` over `SimpleGroup` |

parquet-java has no columnar write API, so the comparison is between the record-shaped APIs, with Hardwood's columnar API as the ceiling. `ExampleParquetWriter` is the parquet-java contender because it is record-shaped and puts no Avro layer inside the timed region.

The fixture (`FlatWriteFixture`) is generated in memory from a fixed seed and shaped like the taxi data rather than uniform noise, because encode cost depends on dictionary behaviour and compression ratio, both of which depend on the value distribution. Its six columns span the axes that change what the writer does:

| Column | Type | Distribution | What it exercises |
|--------|------|--------------|-------------------|
| `id` | `INT64` `REQUIRED` | all distinct, ascending | a column on which dictionary encoding loses to `PLAIN` |
| `pickup_ts` | `INT64` `REQUIRED`, `TIMESTAMP(MICROS)` | ascending with jitter | logical-type conversion on the row path |
| `passenger_count` | `INT32` `OPTIONAL` | 1–6, ~5% null | definition levels over a tiny dictionary |
| `fare` | `DOUBLE` `REQUIRED` | continuous | high-cardinality fixed width |
| `payment_type` | `BYTE_ARRAY` `REQUIRED`, `STRING` | 4 distinct | the dictionary's best case |
| `vendor` | `BYTE_ARRAY` `OPTIONAL`, `STRING` | ~20 distinct, ~10% null | string encoding plus definition levels |

A column is replaced or dropped only together with the axis it covers.

The fixture is held as one primitive array per column per 1024-row batch, so the columnar method hands its arrays over without copying a slice inside the measured region. Each column is materialized in every representation the APIs take (UTF-8 `byte[]` and `String`; `long` microseconds and `Instant`), so no method pays a conversion its API does not require. A method does pay what its own API costs: `parquetJavaGroup` builds a `SimpleGroup` per record, and `hardwoodRow` converts `Instant` to the stored value.

Every writer setting a caller can set is matched across contenders: page target (1 MiB), row-group target (16 MiB, so the flush path runs several times per invocation), codec, dictionary encoding, writer version and page checksums. parquet-java's 20,000-row page cap is lifted, since Hardwood bounds a page by size alone. The codec is a parameter over the codecs both writers produce (`UNCOMPRESSED`, `LZ4_RAW`, `SNAPPY`, `ZSTD`, `GZIP`). Two differences remain because they are properties of the writers: parquet-java writes a column index and an offset index per column chunk, and the two sides measure the row-group target differently, so they produce different row-group counts.

Both sides write to memory by default (`ByteBufferOutputFile` for Hardwood, `MemoryOutputFile` for parquet-java, both appending into a `ByteArrayOutputStream`), which excludes filesystem I/O from the number. `-Dperf.dir=<dir>` writes to files instead; parquet-java is then pointed at Hadoop's `RawLocalFileSystem`, so it writes no `.crc` sidecar that Hardwood does not pay for.

The trial setup writes the fixture once through every method before timing starts. It prints each contender's file size next to the configuration, because a faster contender that writes a larger file has not won. It fails the run if the two Hardwood APIs produce files of different size, or if the row-writer variants produce bytes different from `hardwoodRow`, since either means the methods are not doing the same work. The parquet-java file is checked only by reading it back through Hardwood and counting its rows; that the two writers store the same values is established by the round-trip, equivalence and interop tests, not in the benchmark. Run it with `-prof gc`, so the allocation rate is reported alongside the time.

`WriteEncodingBenchmark` shares `FlatWriteFixture` and measures Hardwood's columnar API alone under different per-column encoding policies.

### Controlling a Benchmark

A new benchmark in this module controls for the following:

- **Correctness before timing.** A gate or a trial-setup check establishes that every contender produced the same values or the same file, or, where a contender's output cannot be compared that way, the test that establishes it is named.
- **One cost at a time.** The fixture and the output destination leave out every cost other than the one named: no codec, dictionary or strings when the subject is nested reconstruction; no filesystem when the subject is encoding.
- **A floor.** Where a cost is measured as overhead, a twin that does the same work without it (the flat twin, the flat scalar file) is measured alongside.
- **Equal work across contenders.** Settings are matched, inputs are pre-materialized in each API's native representation, and each contender's inherent costs are listed in the class JavaDoc.
- **Output size.** A write benchmark reports the size of what it produced next to the time.
- **Fixed, bounded parameters.** `@Param` values are few, chosen at the extremes of the axis they sweep, so the matrix stays small enough to run in minutes.

### Comparing Runs

A change is measured branch against `main`, on the same machine in the same session: build `benchmarks.jar` on `main`, run the benchmark, build it on the branch, run it again, and compare ms/op together with JMH's reported error. Performance changes are not gated behind `ReaderConfig` or `WriterConfig` options for the sake of an A/B comparison; the benchmark carries no on/off toggles, and the comparison is between two builds.

Numbers from a laptop or a container show relative movement during development. Numbers that are quoted (in an issue, a PR description or the tables above) come from a dedicated bare-metal machine with no other load. A quoted number states the machine, the pinned clock, the JDK, the row count and the commit it was taken at.
