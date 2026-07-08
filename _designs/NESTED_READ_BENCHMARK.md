# Nested read JMH benchmark

Status: **Implemented**

## Motivation

The general (non-fast-path) nested read path is the slowest part of the reader. For
a `LIST<float32>` scan with the fixed-size-list fast path disabled it executes on
the order of 21× the instructions of the equivalent flat read, almost all of it
per-element bookkeeping around a two-instruction value copy. Three changes target
this path:

- **#732** — push the flat/nested split down from the reader to the per-column
  worker, so non-repeated columns in a mixed file read at flat speed instead of
  being dragged onto the assembly path.
- **#750** — bulk-copy contiguous present, unmasked runs in `assembleRegularPage`
  instead of copying one element at a time.
- **#751** — compute the `RealView` on the parallel drain and drop the per-batch
  def/rep level materialization on the `ColumnReader` path.

Each of these needs before/after numbers on a workload that isolates the nested
reconstruction inner loop. No such benchmark exists:

- `NestedPerformanceTest` (end-to-end, Overture Maps places) is a whole-file,
  wall-clock, cross-engine comparison over a **string-heavy** schema. String decode
  and I/O dominate its time, drowning the level-scan and assembly signal these three
  changes move. It measures reader ergonomics, not the reconstruction inner loop.
- `FixedSizeListDecodeBenchmark` and the standalone `FixedSizeListScanBenchmark`
  measure only the fixed-width **fast path** (#740) — the path that skips
  reconstruction — not the general nested path that #732/#750/#751 improve.

This benchmark fills that gap: a JMH benchmark over **numeric** leaves that isolates
the general nested read path against a flat decode floor, across the dimensions the
three changes move.

## Goals

- Isolate the general nested assembly and level-scan cost over numeric leaves
  (int, long, float, double), with no string decode, no compression, no dictionary,
  and a warm page cache — so the measured time is level handling plus value copy.
- Measure the same file through both public read paths:
  - the `ColumnReader` **real-items** path (what #750 and #751 accelerate), and
  - the `RowReader` **all-items** path (which #751 must not regress and #732
    touches),
  against a **flat floor** — the identical leaf values as a plain primitive column.
- Expose #732's cost directly: scan the scalar columns of a **mixed** scalar+list
  schema and compare against the same scalars in a purely flat file. The delta is
  the per-scalar-column tax that today's per-file routing imposes and #732 removes.
- Cover the shapes whose acceptance criteria demand a no-regression guard:
  repeated-heavy schemas and a `>1` repetition-layer column (which #751's
  single-repeated-layer fast path deliberately does not fire on).
- Fold every contender to a numeric checksum and assert agreement before timing, so
  a future change that corrupts values fails the run rather than posting a fast wrong
  number.

## Non-goals

- Cross-engine comparison (parquet-java, Avro, Arrow). That is the job of the
  publishable standalone suite; this benchmark compares Hardwood to itself and to its
  own flat floor.
- Strings/binary leaves, compression, dictionary encoding, and cold-cache/I/O
  effects. Those are real but orthogonal; including them reintroduces the noise that
  makes `NestedPerformanceTest` unsuitable for this purpose.
- Chart generation and publication. This is an in-repo regression/measurement
  harness under `-Pperformance-test`, not a headline artifact.

## Location

`performance-testing/micro-benchmarks`, in three packages (the existing benchmarks
such as `FixedSizeListDecodeBenchmark` stay in the root `dev.hardwood.benchmarks`). It
reuses the module's JMH wiring (annotation processor, `--add-modules
jdk.incubator.vector`, shade uberjar).

The corpus generators write with parquet-java, so the module gains compile-scope
`org.apache.parquet:parquet-avro` and `parquet-hadoop` plus the `hadoop-common` and
`hadoop-mapreduce-client-core` runtime dependencies — the same coordinates the
`end-to-end` module already declares (parquet 1.17.1, hadoop 3.4.3, versions managed
in `test-bom`). Only the generators touch these; the benchmark bodies read through
Hardwood alone.

The two benchmark families live in their own packages, each self-contained with its
generator and correctness gate, over a small set of shared helpers at the root:

- `dev.hardwood.benchmarks` (shared) — `Elem` (the element-type enum), `NestedReads`
  (the column/row folds), `BenchmarkWriter` (the parquet-java writer plumbing), and
  `BenchmarkData` (corpus directory and fixture sizes).
- `dev.hardwood.benchmarks.nested` (#750, #751) — `NestedListReadBenchmark` (the
  `LIST<primitive>` path, parameterized by element type and null density),
  `NestedListFileGenerator`, and `NestedListGate`.
- `dev.hardwood.benchmarks.mixed` (#732) — `MixedSchemaReadBenchmark` (the mixed
  scalar+list schema, the struct path, and the no-regression guards: repeated-heavy
  and the `> 1` repetition-layer depth shapes), `MixedSchemaFileGenerator`, and
  `MixedSchemaGate`.

## Fixtures

Each family's generator (`NestedListFileGenerator`, `MixedSchemaFileGenerator`) writes
its fixtures through the shared `BenchmarkWriter`, which wraps parquet-java's
`AvroParquetWriter` (writer version `PARQUET_2_0` for DataPageV2,
`CompressionCodecName.UNCOMPRESSED`, dictionary disabled), mirroring the standalone
suite's `FixedSizeListFileGenerator` and `EventFileGenerator`. Each generator exposes
idempotent `ensure(dir, …)` methods that its benchmark calls from
`@Setup(Level.Trial)`, plus a `main` so the corpus can be generated up front; a file
present and non-empty is left untouched, keyed by the parameters in its name. Files
land in `performance-testing/test-data-setup/target/benchmark-data/` (the directory
the existing generators use), overridable with `-Dperf.dataDir`.

All files are DataPageV2, uncompressed, no dictionary. Every file holds the same
**total leaf-value count** (default 8,000,000, matching the fixed-size-list corpus) so
ms/op is directly comparable across shapes. Leaf values are deterministic (a
`java.util.Random` seeded per file).

### `NestedListReadBenchmark` fixtures

For each element type `t ∈ {int64, float64}` (a 4-byte and an 8-byte leaf) and each
null density `d ∈ {none, sparse, dense}`:

- `list_<t>_<d>.parquet` — `LIST<t>` with variable list length (lengths drawn around a
  mean so the offset scan sees real variation, not a constant stride the fast path
  could exploit). At `none` the list and element are required (leaf max def 1, max rep
  1); at `sparse`/`dense` both are optional, so ~5%/~50% of elements are null with the
  occasional null/empty list. Nulls and empty lists are what break the contiguous
  present runs #750 copies in one shot.
- `list_<t>_<d>_flat.parquet` — the identical leaf stream (same values, same null
  positions) as a plain `t` column (no list). The decode floor.

### `MixedSchemaReadBenchmark` fixtures

- `mixed.parquet` — 12 non-repeated scalar columns (four int, four long, four double)
  plus 2 `LIST<double>` columns. The presence of the list columns routes the whole
  file onto the nested reader today.
- `flat_scalars.parquet` — the same 12 scalar columns, with **no** list column, so the
  file is routed onto the flat reader. The floor for the scalar scan.
- `struct.parquet` — a non-repeated `STRUCT` of primitives (max rep level 0), plus its
  flat twin `struct_flat.parquet` (the same leaves as top-level scalar columns). Covers
  the flat-struct path (#475/#497) that #732 subsumes.
- `repeated_heavy.parquet` — several `LIST<double>` columns and no scalars. The
  no-regression guard for repetition-heavy schemas.
- `list_of_list.parquet` — `LIST<LIST<double>>`, leaf max rep level 2, and
  `list_of_struct.parquet` — `LIST<STRUCT<a: int64, b: float64>>`. These exercise the
  general path where #751's single-repeated-layer fast path does **not** apply and
  must be seen not to regress.

## Contenders

### `NestedListReadBenchmark`

Same file, three read paths, plus the floor:

- `columnNested` — open a `ColumnReader` on the leaf, and for each batch fold the
  compacted real-items leaf array (`getFloats()`/`getLongs()`/… over `getValueCount()`)
  with `getLeafValidity()`. A flat-leaf consumer (values, count, leaf validity — no
  offsets); the vector/embedding read pattern. This is the path #750 and #751
  accelerate.
- `columnNestedStructural` — the same `ColumnReader` path as a list-reconstructing
  consumer: read `getLayerOffsets()`/`getLayerValidity()` and walk the lists. It reads
  the per-layer view every batch, so it measures whether that view is built on the
  drain or lazily on the serial consumer.
- `rowNested` — open a `RowReader`, materialize the `PqList` per row, and fold its
  numeric elements. The ergonomic all-items path; #751 must not regress it.
- `flatFloor` — a `ColumnReader` over the flat-twin leaf. The absolute decode floor.

The gap between the `columnNested*`/`rowNested` paths and `flatFloor` is the
reconstruction cost; these methods let a change move the nested numbers toward the
floor while the floor itself stays put.

### `MixedSchemaReadBenchmark`

- `columnScalarsMixed` — scan the 12 scalar columns out of `mixed.parquet`.
- `columnScalarsFlat` — scan the same 12 scalar columns out of `flat_scalars.parquet`.
  The delta between these two is the #732 tax.
- `columnStruct` / `columnStructFlat` — the struct leaves vs their flat twin.
- `columnRepeatedHeavy` — scan every list column of `repeated_heavy.parquet` (the
  no-regression guard).
- `columnListOfList` / `rowListOfList` and `columnListOfStruct` / `rowListOfStruct` —
  the `> 1` repetition-layer depth shapes through both read paths.

## Parameters

Kept deliberately small to bound the JMH matrix:

- `NestedListReadBenchmark`: `@Param elem ∈ {int64, float64}` (a 4-byte and an 8-byte
  leaf; the copy-volume extremes) and `@Param nullDensity ∈ {none, sparse, dense}`.
- `MixedSchemaReadBenchmark`: no parameters; every shape is a fixed fixture, so the
  depth guards run as their own methods without a numeric-type sweep multiplying them.

## Measuring a change

A change is measured **branch-vs-main**, which is also #732's stated acceptance
criterion ("`-Pperformance-test` before/after numbers on fully-flat, mixed-schema,
and repeated-heavy"):

1. Land this benchmark on `main`.
2. Run it on `main` to capture the baseline.
3. Run it on the change branch (`750-...`, `751-...`, `732-...`) and compare ms/op.

The improvement changes are not gated behind `ReaderConfig` options; the benchmark
carries no on/off toggle machinery, and no config surface is added to the reader for
the sake of A/B measurement. This keeps the improvements as unconditional code paths
and the benchmark as a plain scan.

## Harness conventions

Mirror `FixedSizeListDecodeBenchmark`:

- `@BenchmarkMode(AverageTime)`, `@OutputTimeUnit(MILLISECONDS)`, `@State(Benchmark)`.
- `@Fork(2, jvmArgs = {"-Xms2g", "-Xmx2g", "--add-modules", "jdk.incubator.vector"})`,
  `@Warmup(3, 1s)`, `@Measurement(5, 1s)`.
- Data directory via `-Dperf.dataDir` (default the shared `benchmark-data` dir),
  fixture sizes via `-Dperf.totalValues` / `-Dperf.rows` (`BenchmarkData`); each
  benchmark's `@Setup` calls the generator's idempotent `ensure(...)` so a fork never
  measures a missing file.
- One shared `HardwoodContext` per trial, closed in `@TearDown`.
- A per-family gate — `NestedListGate` and `MixedSchemaGate`, each a standalone `main`
  (e.g. `java -cp benchmarks.jar dev.hardwood.benchmarks.nested.NestedListGate`) — that
  generates its corpus and folds every contender to a numeric checksum, asserting the
  paths that read the same values agree (within a rounding epsilon across paths) before
  any timing counts.
