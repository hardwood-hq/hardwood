# Flat write benchmark (#9, stage 17)

**Status: Complete.** Tracking issue: #9. Delivery stage 17 (Benchmark) of
[WRITER_SUPPORT.md](../_plans/WRITER_SUPPORT.md).

## Context

The read path has `FlatPerformanceTest`: a flat scan of the NYC taxi corpus, Hardwood's row and
column readers against parquet-java, with summed values asserted equal so a fast contender
cannot be a wrong one. The write path has no counterpart. Stages 1–16 were built against
correctness gates — round trips, byte-identical equivalence, the parquet-java interop gate — and
none of them says how long producing a file takes, or how that compares to the incumbent.

This is the first write benchmark: flat schema, three contenders, one number each. It exists to
establish a baseline that later stages are measured against — the remaining codecs and encoders
(stage 19), parallel column encoding (stage 23), row-group-global dictionary selection (stage
18) are all throughput claims, and none of them can be argued without this.

## Placement

`performance-testing/micro-benchmarks`, as a JMH benchmark alongside the existing ones, rather
than in `performance-testing/end-to-end` where `FlatPerformanceTest` lives.

Two reasons. The end-to-end suite is built around the downloaded taxi corpus under
`test-data-setup/target/tlc-trip-record-data`, and a write benchmark does not want Parquet files
— it wants source records in memory, so reading them back in first would put the read path
inside the measurement. And encode throughput is a steady-state, JIT-sensitive figure, which is
what JMH exists to measure properly.

The module already depends on `parquet-avro`, `parquet-hadoop`, `zstd-jni`, `snappy-java` and
JMH 1.37, so the benchmark adds no dependency. It follows the conventions already established
there: `@Fork(2)`, `@Warmup(3, 1s)`, `@Measurement(5, 1s)`, run instructions in the class
JavaDoc. Two forks rather than one because this is the figure every throughput stage after it
is argued against, and a single fork folds JIT and heap-layout variance into the iteration
error rather than reporting it.

The fork's JVM arguments are *appended* rather than replaced, throughout the module. Replacing
them drops the inherited command line, and with it the `-Dperf.rows`, `-Dperf.dir`,
`-Dperf.dataDir`, `-Dperf.totalValues` and `-Dperf.pageVersion` that the forked JVM's setup
reads — so an override passed on the command line is silently ignored and the benchmark runs at
its defaults.

The module carries a `log4j.properties` pinning the root logger to `WARN`, the same file the
end-to-end suite has. Without one, Log4j 1.x defaults the root logger to `DEBUG` and
parquet-java's `MessageColumnIO` record consumer formats a log message per field per record,
which costs an order of magnitude more than the write it is reporting on. Any benchmark that
writes through parquet-java's record API measures logging rather than encoding until that file
is on the classpath.

Correctness is deliberately not asserted here, unlike `FlatPerformanceTest`. The write path
already has the cover: the byte-identical equivalence tests hold the two Hardwood APIs to the
same output, and the interop gate holds that output to parquet-java's reading of it. Repeating
it inside a benchmark would buy nothing and slow the run.

## Contenders

| Contender | API |
|-----------|-----|
| `HARDWOOD_COLUMNAR` | `ParquetFileWriter.columnWriter()` → `ColumnWriter.writeBatch`, 1024-row batches |
| `HARDWOOD_ROW` | `ParquetFileWriter.rowWriter()` → `RowWriter.writeRow` |
| `PARQUET_JAVA_GROUP` | `ExampleParquetWriter` over `SimpleGroup` |

`ExampleParquetWriter` is the first parquet-java contender because it is record-shaped, so it is
the honest counterpart to `RowWriter`, and because it puts no Avro layer inside the timed
region: with `AvroParquetWriter` a share of the number is `GenericRecord` machinery, reported as
though it were parquet-java's writer. It is also already the writer this repository uses in
`LargeFileReadTest`, so it introduces no new idiom.

`AvroParquetWriter` is the mainstream API and belongs in the comparison, but as a second
contender added later and labelled for what it is — what most callers actually pay — rather than
as the baseline.

**parquet-java has no columnar write API.** Its `WriteSupport` is record-at-a-time by
construction. So `ColumnBatch` has no counterpart and the comparison is really "the two
record-shaped APIs head to head, with Hardwood's columnar API as the ceiling neither row API can
beat". That asymmetry is the point rather than a flaw in the setup, and the class JavaDoc says so.

## The data

Generated in-process from a fixed seed, not read from the taxi corpus.

A write benchmark needs records in memory. Sourcing them from the corpus would mean downloading
hundreds of megabytes through `test-data-setup` and decoding it on every run, which defeats
"fast to run" and puts the read path inside the fixture. A seeded generator is instant,
identical on every machine, and stable over time, so a number from today is comparable with one
from six months from now.

What it must not be is uniform random noise. Encode cost is dominated by dictionary behaviour
and compression ratio, both of which are distribution-sensitive; a benchmark over random data
measures a file nobody writes. The fixture is therefore shaped like the taxi data — mostly
dictionary-friendly columns, a minority nullable — across the axes that actually change what the
writer does:

| Column | Type | Distribution | What it exercises |
|--------|------|--------------|-------------------|
| `id` | `INT64` `REQUIRED` | all distinct, ascending | dictionary growth to the limit, then `PLAIN` fallback |
| `pickup_ts` | `INT64` `REQUIRED`, `TIMESTAMP(MICROS)` | ascending with jitter | logical-type conversion on the row path |
| `passenger_count` | `INT32` `OPTIONAL` | 1–6, ~5% null | definition levels over a tiny dictionary |
| `fare` | `DOUBLE` `REQUIRED` | continuous | high-cardinality fixed width |
| `payment_type` | `BYTE_ARRAY` `REQUIRED`, `STRING` | 4 distinct | the dictionary's best case |
| `vendor` | `BYTE_ARRAY` `OPTIONAL`, `STRING` | ~20 distinct, ~10% null | `setString` encoding plus levels |

Six columns, chosen to span the axes rather than to enumerate the type matrix: all-distinct
against low-cardinality, fixed against variable width, `REQUIRED` against `OPTIONAL`, annotated
against bare.

The fixture is built once per trial as **column-oriented primitive arrays** (`long[]`, `int[]`,
`double[]`, `byte[][]`, plus null masks) and shared by all three contenders. Each then pays
whatever its own API costs to get from those arrays into a file: the columnar path hands the
arrays over, `RowWriter` walks them element by element, and `ExampleParquetWriter` constructs a
`SimpleGroup` per record. That per-record object is inherent to parquet-java's design, so
including it is fair — but the class JavaDoc states plainly that it is in the measurement, so
nobody reads the gap as pure encoding speed.

The arrays are held **one per column per 1024-row batch** rather than one per column, so the
columnar contender submits a batch by handing its arrays over as they are. Slicing a batch out
of a million-element column on every call would put a copy of the whole fixture inside the
measured region and measure `Arrays.copyOfRange` alongside the encoder.

A column whose APIs take different Java types is materialized in each of them, so that no
contender pays a conversion its API does not require: the `STRING` columns as UTF-8 `byte[]`
for the columnar API and as `String` for the two record-shaped ones, and `pickup_ts` as `long`
microseconds for the two APIs that take the stored value alongside `Instant` for `RowWriter`,
which takes the annotated one. The `Instant` conversion is genuinely in `hardwoodRow`'s number
and genuinely absent from the other two, because that is what a caller holding records pays.
The `Instant` objects come from the fixture, though, so what the measured region carries is the
conversion and the pointer chase, not the allocation a caller building records would also pay.

**One million rows per invocation** (~40–50 MB uncompressed), overridable with `-Dperf.rows` in
the style of `BenchmarkData`, which reads the same property. A value that is not an `int` this
benchmark can hold is rejected rather than quietly replaced by the default, so a number is never
reported for a row count nobody asked for. At the write path's current throughput one million
rows is roughly 0.15 s per invocation, so a benchmark method takes about five seconds per fork
and the full three contenders across two codecs and two forks run in a couple of minutes.

## Comparability

Two things decide whether the numbers mean anything at all, and both are easy to get wrong.

**Every writer setting is matched across contenders**: page size, row-group / block size, codec,
dictionary enabled, the dictionary page limit, writer version, and page checksums. A codec
`@Param` over `{UNCOMPRESSED, ZSTD}` covers the two the writer produces today, and both sides
are given an explicit 16 MiB row-group target so a million rows produces a handful of row groups
and the flush path is exercised — rather than one group at the 128 MiB default, which would
measure a case real files do not hit. parquet-java's 20 000-row page cap is lifted, because
Hardwood bounds a page by size alone and the two would otherwise be cutting pages on different
rules.

Two things stay unmatched because they are properties of the writers rather than settings.
parquet-java writes a column index and an offset index per column chunk, which Hardwood does not
produce yet (delivery stage 24). And the 16 MiB row-group target means different things on the
two sides — Hardwood counts buffered *uncompressed* bytes, parquet-java its own buffered size
estimate — so the same target yields a different number of row groups, which the benchmark
prints alongside each file's size.

**Output size is reported next to the time.** A contender that is twenty percent faster and
produces a ten percent larger file has not won, and a throughput regression can otherwise hide
behind a compression change. The size each contender produces is printed once from the trial
setup, so the two numbers are always read together.

`-prof gc` is part of the normal invocation: after the page-body copy work in stage 16 the
write path's allocation rate is a number worth watching, and JMH reports it for free.

## Output destination

Both sides write to memory: Hardwood through `ByteBufferOutputFile`, parquet-java through a
small in-memory `org.apache.parquet.io.OutputFile` / `PositionOutputStream` over a
`ByteArrayOutputStream` — the shim that repository does not provide but which is a few lines to
write.

Both shims accumulate into a `ByteArrayOutputStream` and append the caller's array to it, so
neither side copies the payload twice on its way into the buffer and the harness contributes the
same allocation to both.

This deliberately excludes filesystem I/O, because the question is encode throughput and I/O
noise in a container would swamp the differences being measured. `-Dperf.dir` switches both
sides to temp files on a given directory for the case where the end-to-end cost is what is
wanted, which is a different question and a later one. parquet-java is pointed at Hadoop's
`RawLocalFileSystem` there: the default `LocalFileSystem` writes a `.crc` sidecar beside every
file, a second checksum pass and a second file that Hardwood's destination does not pay, in the
one mode whose entire purpose is measuring what the filesystem costs.

## Scope

Delivered here:

- `FlatWriteBenchmark` in `performance-testing/micro-benchmarks`, the three contenders above,
  the codec `@Param`, the seeded fixture, in-memory output, and reported output sizes.

Deliberately not here:

- **Nested shapes.** `NestedWriteBenchmark` follows once the flat one is telling us something;
  the nested write path has more surface (offsets, per-instance validity, the row layer's
  staging reset) and deserves its own fixture rather than a column bolted onto this one. It
  arrives with stage 21 (#989), along with the encoding, cardinality and schema-width axes this
  fixture holds fixed — see [WRITE_PATH_BENCHMARK_COVERAGE.md](WRITE_PATH_BENCHMARK_COVERAGE.md).
- **`AvroParquetWriter`.** Added as a second parquet-java contender once the first comparison is
  settled, so the two are not confounded on introduction.
- **Filesystem and object-store throughput.** The S3 backend arrives in stage 22; measuring it
  is its own exercise with its own dominant term.
- **Correctness assertions.** Covered by the equivalence tests and the interop gate.

## Validation

The benchmark is validated by being run, not by tests of its own. The bar for accepting it:

- All three contenders produce files of the same row count, read back through Hardwood, checked
  once from the trial setup rather than per invocation.
- The two Hardwood contenders produce files of identical size, as the byte-identical equivalence
  tests require; a divergence there is a writer defect, not a benchmark one. The trial setup
  enforces it rather than only printing the two sizes, so the run fails instead of quietly
  reporting one Hardwood API as the leaner writer.
- Run on the N300 bare-metal box for any number that gets quoted; the container's figures are
  for relative movement during development only.

**The produced files agree in size**, within about a tenth of a percent on this fixture, which
is what the benchmark exists to keep true. It was not always so: the writer once built a
dictionary optimistically per column chunk and abandoned it mid-chunk, leaving a chunk carrying
a dictionary page it had stopped using and dictionary-encoding all-distinct columns — `id`,
`fare`, `pickup_ts` here — at a dictionary page plus an index stream where the values alone were
smaller. That cost a fifth of the file, and this benchmark is what measured it and what measured
delivery stage 18 closing it.
