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
