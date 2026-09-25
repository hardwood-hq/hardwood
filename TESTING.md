# Testing

This document describes how Hardwood's automated tests are organised and how to run them: the split between unit and integration tests, the tags that keep some tests out of an ordinary build, and the differential tests that compare Hardwood against DuckDB. It ends with manual recipes for scenarios the automated suite does not cover. The Maven profiles, Error Prone checks and CI jobs are described in [_designs/BUILD_INFRASTRUCTURE.md](_designs/BUILD_INFRASTRUCTURE.md), the native CLI build in [NATIVE_BUILD.md](NATIVE_BUILD.md), and benchmarks in [PERFORMANCE.md](PERFORMANCE.md).

## Integration tests

Tests are split across the two Maven test phases by what they need in order to run. A unit test needs nothing beyond the JVM and a source checkout, and runs in the `test` phase under Surefire. An integration test needs something the build has to provide first, and runs in the `integration-test` phase under Failsafe.

The split is carried by the standard Maven naming convention: `*Test` for Surefire, `*IT` for Failsafe. The standard flags therefore behave as Maven documents them:

| Command | Unit tests | Integration tests |
|---------|------------|-------------------|
| `./mvnw verify` | run | run, except those tagged `native` |
| `./mvnw verify -DskipITs` | run | skipped |
| `./mvnw -Dnative verify` (in `cli`) | run | run, including those tagged `native` |
| `./mvnw verify -DskipTests`, or `-Dquick` | skipped | skipped |

### What counts as an integration test

Three prerequisites put a test in the `integration-test` phase:

- **A Docker daemon.** Anything that starts a Testcontainers container, which here means anything reaching S3 through the s3proxy container built by `S3ProxyContainers` (in `test-support`).
- **A compiled native binary.** The `cli` tests that spawn the GraalVM binary as a subprocess.
- **The packaged artifact.** The `core` decompressor ITs, which run against the built JAR rather than `target/classes`, so that multi-release class selection is exercised the way a consumer sees it.

Everything else is a unit test. Reading a fixture from `src/test/resources` is not an external prerequisite.

Two sets of tests meet one of those prerequisites and are still named `*Test`, because they are separated by module rather than by phase:

- `integration-test` (`hardwood-integration-test`) tests `hardwood-core` as a dependency. It is part of the default reactor, so `./mvnw verify` runs it on the build JDK. CI runs it on its own, against `hardwood-core` installed from an earlier step, on the Java 21 baseline the JAR claims to support and across libdeflate and Vector API availability. It needs no daemon and no binary, and `-DskipITs` does not skip it.
- `performance-testing/end-to-end` reaches S3 through a container in `FlatS3PerformanceTest`. The module builds only under `-Pperformance-test`, and the `performance.yml` workflow picks the benchmarks to run with `-Dtest=` filters, which Surefire honours and Failsafe does not. Under that profile `-DskipITs` therefore leaves this container-backed test running.

### Build wiring

Failsafe's `integration-test` and `verify` goals are bound once, in the parent POM, so every module runs its ITs without repeating the binding. A module needing more than the defaults contributes only a `<configuration>`: `core` points `classesDirectory` at the built JAR, and `cli` passes the system property `native.image.path` so the native ITs can locate the binary.

The native binary is the one prerequisite an ordinary build cannot satisfy, since only the `native` profile of `cli` (activated by `-Dnative`) produces it. Those ITs carry the JUnit tag `native`, which the parent POM's `failsafe.excludedGroups` property excludes by default. The `native` profile clears that property.

The tag only ever withholds tests; it never narrows a run. `./mvnw verify` runs every IT that can run without a binary, and `./mvnw -Dnative verify` produces the binary and runs every IT there is, so adding `-Dnative` never subtracts coverage. `-DskipITs` keeps its Maven meaning, and the native ITs are tied to the condition that governs them (whether the binary was built) rather than to a property a caller might pass for an unrelated reason. A build wanting a narrower slice asks for it directly, with `-Dfailsafe.excludedGroups=native`, `-Dgroups=native` or `-Dit.test=...`.

### The native test layer

The JVM `*CommandTest` and `*S3CommandIT` classes in `cli` carry the behavioural coverage for every CLI command. The three native ITs do not repeat it. They catch what only appears once the image is built: missing reflection registrations, unreachable classpath resources, broken AWS SDK wiring, and codec native libraries that fail to load.

| Test | What it runs |
|------|--------------|
| `NativeBinarySmokeIT` | The binary against a local Parquet file, including `dive --smoke-render`; pins the reported build version against the JVM it was compiled from. |
| `NativeBinaryS3SmokeIT` | The binary against a file served by the s3proxy container. |
| `NativeCompressionCodecIT` | One fixture per supported compression codec, with `LZ4` and `LZ4_RAW` separately because they use different decompressors. |

The ITs run on the JVM and drive the binary through `ProcessBuilder`, so configuration flows one way: fixture files are resolved as classpath resources and passed as absolute paths, and settings reach the subprocess as environment variables. The JVM S3 command tests configure AWS through `System.setProperty()` instead, which works only because they run the CLI in the test's own JVM.

### The s3proxy image

Every S3 test uses `ghcr.io/hardwood-hq/s3proxy`, a mirror of `andrewgaul/s3proxy` maintained to avoid Docker Hub rate limits. The pinned tag lives in `S3ProxyContainers.IMAGE` and in the `S3PROXY_IMAGE` environment variable of the `pr-build.yml` and `main-build.yml` workflows, which pre-pull the image. These and the `docker run` command of the [manual S3 recipe](#manual-s3-testing) must name the same tag.

## Tagged and opt-in tests

Some tests stay in their Maven phase but are selected by JUnit tag or system property:

| Tag | Tests | Default | How to change it |
|-----|-------|---------|------------------|
| `native` | the native CLI ITs | excluded from Failsafe | `-Dnative` builds the binary and includes them |
| `differential` | the DuckDB [differential tests](#differential-testing) | run | `-DexcludedGroups=differential` skips them |
| `large` | `WriterLargeFileTest` (core), `LargeFileReadTest` (parquet-testing-runner): files past 2 GB | skipped | `-Dhardwood.largeFileTests=true` |

## Differential testing

The differential tests read the same Parquet file through Hardwood and through DuckDB and require the two results to agree. They catch composition defects (a reader control that is correct alone and wrong once combined with a filter) and decode defects (a value decoded wrongly for some type or encoding).

### The oracle

DuckDB, through the `duckdb_jdbc` driver (test scope in `core`, version managed in `test-bom`), runs in-process in the JUnit test; no external process or Python runs at test time. Each Hardwood query is written out a second time as SQL over `read_parquet('<file>')`. DuckDB's `WHERE`, `LIMIT` and `OFFSET` semantics match the logical model of the row reader described in [_designs/RECORD_FILTERING.md](_designs/RECORD_FILTERING.md), including three-valued logic for comparisons on nullable columns.

A disagreement is investigated as a Hardwood defect first. Where it turns out to be a semantic question rather than a bug, the answer is pinned in a test.

### The `__row__` column

`head`, `tail` and `skip` operate in file order, while SQL without `ORDER BY` is unordered and DuckDB may reorder a parallel scan. Every differential fixture therefore carries a synthetic `INT64` column `__row__` equal to the physical row position (`0..N-1`). File order is recoverable as `ORDER BY __row__`, and the reader controls map to SQL as follows:

| Hardwood | DuckDB SQL |
|----------|------------|
| `filter(p)` | `... WHERE p ORDER BY __row__` |
| `head(n)` | `... ORDER BY __row__ LIMIT n` |
| `skip(m)` | `... ORDER BY __row__ LIMIT ALL OFFSET m` |
| `skip(m).head(n)` | `... ORDER BY __row__ LIMIT n OFFSET m` |
| `tail(n)` | `SELECT __row__ FROM (... ORDER BY __row__ DESC LIMIT n) ORDER BY __row__` |
| `projection(cols)` | the query always projects `__row__` in addition to `cols` |

`filter` combines with `head` and `skip` as in SQL: the offset and limit apply to the matched rows. The combinations the builder rejects (`filter` with `tail`, `head` with `tail`, `skip` with `tail`) are pinned in `BuilderCombinationTest`, not here.

`__row__` makes these instrumented fixtures, distinct from the real-world files checked in elsewhere: those cover decoding an actual file, the differential corpus covers whether the right rows are selected and decoded.

### Comparison layers

| Layer | What is compared | Catches |
|-------|------------------|---------|
| Row identity | the ordered sequence of `__row__` values on both sides | wrong rows, wrong count or wrong order under a combination of controls |
| Values | every column, row by row, in `__row__` order | decode defects for a type, encoding, codec or page layout |

Row identity needs no per-type handling and is checked first. The value layer normalises only where the two sides use different Java types: decimals are compared by numeric value, dates as `LocalDate`, timestamps as `Instant`, binary as `byte[]` content. Floating-point values are compared exactly, because a reader must reproduce the stored bits.

### Where the harness lives

The harness is a set of JUnit classes in `core/src/test/java/dev/hardwood/`, each tagged `differential`. Each row-identity class states every case once, as a record holding both the Hardwood side (a `RowReaderBuilder` configuration or a `FilterPredicate`) and the equivalent SQL.

| Class | Layer | Fixtures | Covers |
|-------|-------|----------|--------|
| `DifferentialReadTest` | row identity | `diff_numbers_single`, `diff_numbers_multi` | projection, comparison filters on a required column, `head`, `tail`, `skip` and their legal combinations, `skip` past the end |
| `DifferentialNullTest` | row identity | `diff_nulls` | comparison filters on a nullable column drop null rows |
| `DifferentialColumnOrderTest` | row identity | `diff_order_single`, `diff_order_multi`, `diff_order_dict` | filters on columns whose sort order differs from their stored bits: `UINT_32`, `UINT_64`, `FLOAT16`, `DECIMAL` as `FIXED_LEN_BYTE_ARRAY` |
| `DifferentialValueTest` | values | `diff_types` | `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `BOOLEAN`, `STRING`, `DATE`, `TIMESTAMP`, `DECIMAL`, raw `BYTE_ARRAY` |
| `DifferentialFixedSizeListTest` | values | `diff_fixed_size_list` and its `_snappy`, `_zstd` and `_paged` variants | fixed-width `LIST` columns on the fixed-size-list fast path and its fallbacks, compared against DuckDB arrays |

A query that is known to disagree with the oracle can be marked `pending("<issue>")` in `DifferentialReadTest`, which skips it through a JUnit assumption; removing the marker once the bug is fixed turns the case into the fix's verifier.

`WriterDifferentialTest` (`core`, package `dev.hardwood.writer`) runs the harness in the opposite direction: Hardwood writes a file and DuckDB reads it back. Its files carry a synthetic index column `r` in place of `__row__`.

### How fixtures are chosen

The fixtures live in `core/src/test/resources/differential/` and are generated by `tools/simple-datagen.py` with PyArrow (see the Testing section of `CLAUDE.md` for the virtual environment and pinned versions), then checked in. They are deterministic and byte-stable; the tests vary queries over fixed files and never generate a file at test time.

Each fixture is chosen to reach one part of the read path the others cannot:

- A single-row-group file and a multi-row-group file with identical content (row groups of 50 rows), so row-group statistics and cross-row-group boundaries take part in every query.
- A dictionary-encoded variant where the dictionary matters (`diff_order_dict`).
- Columns whose values rise with `__row__`, so a threshold filter selects a contiguous, predictable run of rows and a pruning bound read in the wrong order shows up as missing or extra rows.
- Compression and page-size variants for paths a single uncompressed, single-page file cannot reach.

A new differential case extends `tools/simple-datagen.py` with a fixture carrying `__row__`, or reuses an existing one, and adds the query to the matching class.

### Other reference readers

The `parquet-testing-runner` module uses parquet-java as the reference instead of DuckDB: `ParquetComparisonTest` compares Hardwood's rows field by field against parquet-java for every file in the [apache/parquet-testing](https://github.com/apache/parquet-testing) repository (cloned on first use by `ParquetTestingRepoCloner`), and the writer interop tests (`WriterInteropTest` and its nested and logical-type siblings) read Hardwood-written files back through parquet-java.

## Manual S3 testing

The steps below run a local S3-compatible endpoint via [s3proxy](https://github.com/gaul/s3proxy) and exercise the CLI against it. Useful for verifying S3 behaviour without an AWS account, and for testing the native binary's S3 path (see also [NATIVE_BUILD.md](NATIVE_BUILD.md)).

The image is the s3proxy mirror the automated tests use; see [The s3proxy image](#the-s3proxy-image) for where its tag is pinned.

1. Start s3proxy and set environment

```bash
docker run -d --name s3proxy -p 9090:80 \
    -e S3PROXY_AUTHORIZATION=none \
    -e S3PROXY_ENDPOINT=http://0.0.0.0:80 \
    -e JCLOUDS_PROVIDER=transient \
    ghcr.io/hardwood-hq/s3proxy:sha-6597ca59cd5c5fa8ee313e13d349d507cc6090c3

export AWS_ENDPOINT_URL=http://localhost:9090
export AWS_ACCESS_KEY_ID=foo
export AWS_SECRET_ACCESS_KEY=bar
export AWS_REGION=us-east-1
export AWS_PATH_STYLE=true
```

2. Create bucket and upload with curl

```bash
curl -X PUT http://localhost:9090/test-bucket

curl -T performance-testing/test-data-setup/target/tlc-trip-record-data/yellow_tripdata_2025-01.parquet \
    http://localhost:9090/test-bucket/yellow_tripdata_2025-01.parquet

curl -T performance-testing/test-data-setup/target/overture-maps-data/overture_places.zstd.parquet \
    http://localhost:9090/test-bucket/overture_places.zstd.parquet
```

3. Run hardwood CLI

```bash
cli/target/hardwood-cli-early-access-macos-aarch64/bin/hardwood info -f s3://test-bucket/yellow_tripdata_2025-01.parquet
```


## Running the native build check locally

The `native-build-check` job of `pr-build.yml` can be run with [act](https://github.com/nektos/act) (`brew install act`), which executes workflow jobs in Docker:

```bash
act pull_request -j native-build-check \
  --container-architecture linux/amd64 \
  -P ubuntu-latest=catthehacker/ubuntu:act-latest \
  --container-options "-v $HOME/.m2:/root/.m2 -v /tmp/act-certs.pem:/tmp/act-certs.pem" \
  --env NODE_EXTRA_CA_CERTS=/tmp/act-certs.pem \
  --env TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal
```

- `catthehacker/ubuntu:act-latest` replaces `act`'s default minimal image with one that has the Node.js that JavaScript-based actions need; GitHub-hosted runners ship it pre-installed.
- `TESTCONTAINERS_HOST_OVERRIDE` makes Testcontainers reach mapped ports via `host.docker.internal` instead of the Docker bridge IP, which is unreachable from inside the `act` container on macOS.
- Mounting `~/.m2` reuses the local Maven cache.

On macOS the container also needs the system CA bundle to trust corporate or self-signed certificates, exported beforehand to the path mounted above:

```bash
security find-certificate -a -p /Library/Keychains/System.keychain > /tmp/act-certs.pem
security find-certificate -a -p ~/Library/Keychains/login.keychain-db >> /tmp/act-certs.pem
```
