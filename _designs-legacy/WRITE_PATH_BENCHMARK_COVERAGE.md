# Write-path benchmark coverage (#9, stage 21)

**Status: 21a complete, 21b outstanding.** Tracking issue: #989. Delivery stage 21 (Benchmark)
of [WRITER_SUPPORT.md](../_plans/WRITER_SUPPORT.md). The encoding axis and its baseline are in
[The recorded baseline](#the-recorded-baseline); the nested and schema-width shapes are not
built yet.

## Context

`FlatWriteBenchmark` ([PERFORMANCE.md](../PERFORMANCE.md#flat-write-benchmark)) is the write path's
only benchmark. It measures one flat, taxi-shaped fixture — six columns, four physical types,
1 M rows in 1024-row batches — through three contenders, across the five codecs both writers
produce, and reports each produced file's size next to its time.

That is the whole of what the write path measures, and it is narrower than what the writer
now does. Stage 19b added four encoders behind a per-column policy, and nothing measures what
any of them costs to produce or what it does to a file. Neither does anything measure what the
writer's most expensive per-value decision — building a dictionary to find out whether the
column wanted one — costs on the columns that discard it.

Everything after stage 20 that is not a format feature is a throughput or size claim —
parallel column encoding (23), row groups sized on produced bytes (27), byte-based page cuts
(28) — and each has to be argued against a measurement that covers the shape it changes.
Stage 17 established that principle for the flat shape; this stage extends it to the encoding
policy, and pairs it with a profiling baseline so the claims start from evidence rather than
from reading the code.

## Scope

In scope:

- An encoding axis over the flat fixture, crossed with the codec that matters.
- Allocation per operation reported in the run the results come from.
- A recorded baseline on the N300 bare-metal box, and the procedure that reproduces it.

Out of scope, each listed under [What is deliberately not here](#what-is-deliberately-not-here).

### Increment split

- **21a — the encoding axis on the flat shape**, and the baseline over it. Eight
  configurations; everything runs against the fixture that already exists.
- **21b — the shapes.** A nested write benchmark and a schema-width axis, each needing a
  fixture of its own. Sequenced after 21a rather than beside it, because a nested measurement
  is only worth taking once the flat one has said where the time goes.

## Run cost is part of the design

A benchmark nobody runs measures nothing. At `@Fork(2)` with three warmup and five measurement
iterations of a second each, and a trial setup that builds the fixture and writes it once per
contender, one configuration costs roughly 25 seconds on a developer machine and about three
times that on the N300. The configuration count is therefore a budget, not a free parameter,
and every axis below is cut to the points that answer a question somebody is asking.

**The measurement matrix and the profiling probe are separate runs.** The matrix is the
recorded result: two forks, full iteration counts, every configuration. A profiler pass is a
different question — where the time goes at one point — and re-running the matrix under
`perfasm` costs hours to answer it. Probes run `-f 1 -wi 2 -i 3` over one or two
configurations, on the same fixture and the same row count so the profile describes the
workload the matrix reports. Only the row count stays fixed: at fewer rows the fixture stops
filling a row group, and a profile that never reaches a flush is a profile of half the writer.

An iteration must be several operations long, matrix or probe. An operation costs about 150 ms
on a developer machine and about a second on the N300, so the module's one-second iterations
measure a single operation there and fold each GC pause wholly into one sample. `-r 5 -w 5` is
what the recorded numbers are taken with, and what makes their error bars readable.

## What does not change

`FlatWriteBenchmark` keeps its fixture, its contenders and its codec `@Param` exactly as they
are. Its number is the one stage 17 established, stage 18 moved and stage 19 re-measured, and
comparability across those results is worth more than folding a new axis into the same class.
The encoding axis therefore lives in a benchmark of its own, sharing the fixture generator
rather than the benchmark class.

Two consequences follow, and both are deliberate:

- It carries **Hardwood contenders only**. An encoding policy is a question about this writer,
  not about the gap to parquet-java; running the incumbent identically across four encoding
  values would add run time and no information. `FlatWriteBenchmark` remains the cross-writer
  comparison.
- It carries the **columnar API alone**. Both APIs reach the same encoder through the same
  `ColumnChunkBuffer`; what separates them is the row layer's staging, which
  `FlatWriteBenchmark` already measures and which this axis does not vary.

## The encoding axis

`WriteEncodingBenchmark`, over the flat fixture, with two `@Param`s.

**Encoding** is a named case rather than a bare `ColumnEncoding`, because the legal
(policy, physical type) matrix is not rectangular and a file-wide policy is rejected at writer
creation when any one column of the schema cannot carry it. Each case names the policy and the
columns it applies to, leaving every other column on `AUTO`:

| Case | Policy | Applied to | The question |
|---|---|---|---|
| `AUTO` | — | nothing | today's behaviour, the baseline the others are read against |
| `PLAIN_ON_DISTINCT` | `PLAIN` | `id`, `pickup_ts`, `fare` | what interning a column that discards its dictionary costs |
| `DELTA_INTEGERS` | `DELTA_BINARY_PACKED` | `id`, `pickup_ts`, `passenger_count` | what delta buys on ascending integers, and what it costs to produce |
| `SPLIT_NUMERIC` | `BYTE_STREAM_SPLIT` | `fare`, `id`, `pickup_ts` | whether reordering bytes pays for itself once the codec runs |

`PLAIN_ON_DISTINCT` is not an encoding recommendation; it is a measurement, and it is the most
useful number in the stage. A column under a named policy builds no dictionary at all, so
applying `PLAIN` to exactly the columns the flush-time comparison rejects anyway writes the
same pages as `AUTO` while skipping the interning that produced them. The gap between the two
cases is the upper bound on what stage 26a (#992) can buy, measured through public API, on the
fixture stage 18 was argued on.

The two files agree, and what could separate them is one field: a chunk that still holds the
dictionary it counted with states `distinct_count`, and a chunk that has counted nothing omits
it — a few bytes per column chunk in the footer, and the gap stage 29 (#982) closes. When this
axis was first measured that field was the whole difference between the two cases, because
`AUTO` reached flush still holding the dictionaries it then rejected; since stage 26a (#992)
those chunks give the dictionary up early and state no count either, so the two cases produce
byte-identical files.

**Codec** is `{ZSTD, UNCOMPRESSED}`. `ZSTD` is `WriterConfig`'s default wherever zstd-jni is on
the classpath, so it is the configuration nearly every produced file actually uses, and it is
the only one under which `BYTE_STREAM_SPLIT` means anything — the encoding changes no page's
size by itself and reorders bytes so the codec after it finds structure. `UNCOMPRESSED` is not
a second codec choice under evaluation but the control: it is the reading where an encode-path
change shows at full size, and the ratio between the two is how much of the number such a
change can reach at all. On this fixture that ratio turns out to be favourable — the codec is
15% of the compressed number, not the large share a strong codec is assumed to take — but that
is a result the axis produced, not a premise it rests on.

`GZIP`, `SNAPPY` and `LZ4_RAW` stay out. They are points on a speed-against-ratio trade that
`FlatWriteBenchmark` already sweeps, and none of them asks a question about an encoding that
`ZSTD` does not.

Produced file size is reported per configuration, as `FlatWriteBenchmark` reports it. On the
codec axis size is one half of a trade; on the encoding axis it is most of the question.

Eight configurations, about four minutes on a developer machine.

## Allocation

`gc.alloc.rate.norm` — bytes allocated per operation — is in the run line the class's JavaDoc
documents, as it already is in `FlatWriteBenchmark`'s, and in the run every recorded result is
taken from. It is a first-class number on the write path rather than a diagnostic: the writer
retains a row group's values to decide their encoding, its peak retention is roughly three
times `rowGroupBufferTargetBytes`, and per-page allocation in the encode-compress-frame sequence is
invisible in a time measurement of a fixture whose pages hold tens of thousands of values.

Normalizing to bytes per row rather than per operation is left to whoever reads the result, the
row count being a property of the configuration.

## The profiling baseline

The benchmark says *what* costs; the baseline says *where*. It is taken on the N300 bare-metal
box, single-core-pinned, under a calibrated and pinned clock, and recorded in this document as
the stage's result.

The sequence, each step over the probe configurations rather than the matrix:

1. **Classify.** `perfnorm` and `gc` at `AUTO` under both codecs, normalized to instructions,
   cycles, cache misses, branch misses and bytes per row. `UNCOMPRESSED` is the writer's own
   cost; the ratio to `ZSTD` is how much of the number the codec owns and therefore how much
   of it any encode-side change can reach.
2. **Locate.** async-profiler in `cpu` mode for the call tree, and in `alloc` mode to attribute
   allocation to call sites. Nothing on the write path parks, so wall-clock mode buys nothing
   until stage 23 introduces parallel encoding.
3. **Read the code generation.** `perfasm` at a low `hotThreshold` on the single configuration
   step 2 names, the default 2% being enough to hide a hot loop inside a larger method. This is
   the most expensive step and it runs once, on one point.
4. **Prove by changing the supply.** A profile is evidence, not a verdict. Each candidate below
   has an A/B that changes the resource it is supposed to consume, and the A/B is what decides
   it.

The box is single-channel and E-core-only, so it answers "which instructions, how many, did it
inline" and does not answer memory-bandwidth scaling. The write path is copy-heavy, so any
bandwidth-shaped conclusion belongs on other hardware and is out of scope here.

### The questions the baseline answers

Three candidates are visible in the code as written. The stage's result is which of them are
real, how large, and in what order — not a change to any of them.

- **Interning that is discarded.** A chunk under `AUTO` interns every present value
  (`ColumnChunkBuffer.accept`), and an all-distinct column then resolves every index back into
  a stored value when the comparison rejects the dictionary. Stage 18's design records 210 ms
  against 150 ms before the stage on this fixture, and about 130 ms if such a column gave up
  early. **A/B:** `PLAIN_ON_DISTINCT` against `AUTO`, which is a matrix configuration rather
  than a special run.
- **The per-value call chain.** Each present value passes through `intern`, `stat` and
  `valueBits` on the encoder, each re-entering the read window, and travels from the caller's
  array through the window to the value store before a page ever encodes it. For a flat
  `REQUIRED` column that is a bulk-copyable case handled one value at a time. **A/B:** the
  `REQUIRED` columns of the fixture against its `OPTIONAL` ones, which differ in the level
  entry and the null branch and in nothing else.
- **Per-page allocation.** Encoding a page allocates its value section, the compressor
  allocates its output and — for the codecs that compress into a bound-sized buffer — a
  right-sized copy of it, and the level streams, page header and CRC each allocate as well.
  All of it is amortized over roughly 52 000 entries per page on this fixture, which is why the
  matrix will not show it. **A/B:** `pageTargetBytes`, which moves entries per page directly;
  the schema-width axis of 21b is the same probe from the other side.

## The recorded baseline

N300 (Intel i3-N300, 8 Gracemont E-cores), pinned at 1.50 GHz against a 3.80 GHz part,
governor `performance`, single core via `taskset -c 0`, Temurin JDK 25, 1 000 000 rows,
commit `765feb67`. Two forks, three warmup and five measurement iterations, **five seconds
each**: an operation costs about a second on this box, so the module's one-second iterations
sample a single operation and fold every GC pause wholly into one of ten measurements — the
same matrix at `-r 1` reports `AUTO` at 1250 ± 406 ms. The throttle guard reports CLEAN, at an
effective 1.50 GHz, for every run recorded here.

| Encoding | `UNCOMPRESSED` | `ZSTD` | Bytes/op (unc.) | File (unc.) | File (`ZSTD`) |
|---|---|---|---|---|---|
| `AUTO` | 926 ± 51 ms | 1086 ± 7 ms | 203 MB | 25,447,147 | 13,111,232 |
| `PLAIN_ON_DISTINCT` | 530 ± 66 ms | 696 ± 55 ms | 128 MB | 25,447,111 | 13,111,196 |
| `DELTA_INTEGERS` | 601 ± 40 ms | 695 ± 58 ms | 133 MB | 12,196,853 | 11,469,418 |
| `SPLIT_NUMERIC` | 520 ± 51 ms | 569 ± 61 ms | 152 MB | 25,447,110 | 11,335,749 |

Four results, in the order they matter:

**Interning a dictionary that is then discarded is 43% of the write.** `AUTO` against
`PLAIN_ON_DISTINCT` is 926 ms against 530 ms and 203 MB against 128 MB per operation, for two
files 36 bytes apart — nine chunks' `distinct_count`, which is the whole difference. Nothing is
bought with that time.

**The cost is memory, not arithmetic.** The two configurations differ by 14% in instructions
and 82% in cycles: 2.71 G against 2.32 G instructions, 1.36 G against 0.75 G cycles, IPC 1.99
against 3.11. LLC loads go from 0.69 M to 5.16 M per operation and LLC store misses from 0.73 M
to 1.24 M. The `cpu` profile puts `LongDictionaryEncoder.indexOf` at 40% of samples on its own,
with `insert`, `add`, `resizeTable` and `giveUpDictionary` behind it — about 46% in a dictionary
path whose result is thrown away. It is a cache-missing probe into a table that grows to a row
group's cardinality, so a cheaper hash is not the answer; not building the table is.

**The codec is a small share of the time, so encode-side work is nearly all of it.** `ZSTD`
costs 160 ms over `UNCOMPRESSED` at `AUTO` — 15% of the compressed number, not the large share
a strong codec is assumed to take. Any encode-path change therefore reaches about 85% of what a
default-configured write costs.

**`BYTE_STREAM_SPLIT` is free to produce and pays twice.** Over the same three columns as
`PLAIN_ON_DISTINCT` it is indistinguishable at `UNCOMPRESSED` (520 against 530 ms, 37 bytes
apart in output) — the encoding genuinely changes no page's size. Under `ZSTD` it is both
faster, 569 against 696 ms, and smaller, 11.34 MB against 13.11 MB: the reordered stream is
less work for the codec as well as more compressible. `DELTA_INTEGERS` halves the uncompressed
file, 12.20 MB against 25.45 MB, and costs 71 ms over `PLAIN` to produce.

The table above is the writer as stage 21a found it, at commit `765feb67`. Stage 26a (#992)
acted on the first row of it: `AUTO` now abandons a losing dictionary on the chunk's prefix and
costs 537 ms and 131 MB uncompressed, inside the error bars of `PLAIN_ON_DISTINCT`. See
[WRITER_DICTIONARY_EARLY_ABANDONMENT.md](WRITER_DICTIONARY_EARLY_ABANDONMENT.md). The
measurement is left as it was taken, because it is what the change was argued from.

### What this settles

- The candidate worth acting on is the discarded interning, and stage 26a (#992) is where it was
  scoped and closed. The number here is what it was worth, and the profile named the frame.
- The per-value call chain and the per-page allocations are not refuted, but neither shows at
  this fixture's shape. They are 21b's question, where schema width shrinks the values a page
  amortizes its fixed costs over.
- 203 MB allocated to produce a 25 MB file is eight times the output, and 128 MB of it survives
  the dictionary being removed. The `alloc` profile attributes both, below.

### Where the allocation goes

Shares of sampled allocation, by the deepest Hardwood frame on the stack. The profile spans a
whole run rather than one operation, so read the shares rather than multiplying them against
the per-operation totals.

| Site | `AUTO` | `PLAIN_ON_DISTINCT` | What it is |
|---|---|---|---|
| `ColumnChunkBuffer.accept` → `int[]` | 16.3% | 12.9% | the dictionary index array growing |
| `LongDictionaryEncoder.allocateTable` | 17.2% | — | the hash table, and every resize of it |
| `LongDictionaryEncoder.add` → `long[]` | 9.5% | — | the dictionary's values growing |
| `PlainEncoder.encodeLongs` / `encodeDoubles` | 24.1% | 36.9% | one buffer per page's value section |
| `LongValueEncoder.append` / `DoubleValueEncoder.append` | 9.8% | 15.6% | the value store growing |
| `RleBitPackingHybridEncoder` (`ensureCapacity`, `toByteArray`) | 8.2% | 11.9% | the index stream's buffer, and a copy of it |
| `ByteBufferOutputFile.write` | 4.9% | 7.4% | the benchmark's sink, not the writer |

Three things follow.

**The dictionary accounts for its own delta.** `allocateTable`, `add`, and the index array in
`accept` come to 43% under `AUTO` and to the index array alone under a named policy — where it
is `payment_type` and `vendor`, which keep their dictionaries and should.

**The largest site that is not the dictionary is the per-page value section.** `PlainEncoder`
allocates a buffer per page, which `ColumnChunkBuffer.buildBody` then copies into the page body
it already owns. It is the single biggest allocation site once the dictionary is out of the
picture, and the bytes are copied twice on their way to the output for no reason the code
requires: `encode` returns an array because that is the shape of its signature, not because a
caller needs one.

**Store growth is next, and is a sizing question rather than a copying one.** A chunk's value
store starts at one page's worth and grows by half again, so a row group's worth of values is
reached in a handful of copies whose total is on the order of the store itself.

Stage 26a (#992) removed the dictionary rows and #993 the per-page ones, the latter by having
each section produced into the page body rather than into an array to be copied in. Measured on
the same box and fixture, `AUTO` allocates 102 MB per operation uncompressed against the 203 MB
recorded above and 133 MB against 234 MB under `ZSTD` — and takes the same time either way,
within the error bars. That is the expected shape: `PlainEncoder` was 37% of the *allocation*
and barely visible on the CPU profile, so removing it buys garbage rather than cycles. What it
also buys is one fewer copy of every byte written, which is worth more where a page amortizes
its fixed costs over fewer values — the schema-width axis of 21b — and where allocation rate is
multiplied across threads, which is stage 23.

## Validation

These are benchmarks; they are validated by being run and by producing numbers that hold
still.

- The benchmark checks its produced file back through Hardwood for row count once per trial,
  as `FlatWriteBenchmark` does, and prints the produced size beside it. Correctness of the
  written bytes is the interop gate's and the differential suite's job, not a benchmark's.
- The encoding cases are read back in the trial setup and their chunk encodings asserted to be
  the ones the case names. This is what catches a case whose policy silently failed to apply —
  a benchmark reporting `AUTO`'s number under a delta case's name is worse than no number.
- `AUTO` and `PLAIN_ON_DISTINCT` must produce files of the same size on this fixture, within a
  slack that covers a `distinct_count` per column chunk — the one field the two cases can differ
  in, and which they no longer do since stage 26a. A divergence beyond that slack means the
  flush-time comparison is keeping a dictionary for a column the case declares all-distinct, and
  the case is mis-specified.
- The recorded baseline states the box, the pinned clock, the JDK, the row count and the
  commit it was taken at. A number without those is not comparable with the next one.

## What is deliberately not here

- **Acting on what the baseline finds.** This stage measures. Each candidate it confirms is
  scoped on its own evidence, and the ones already sequenced (23, 27, 28) keep their places.
- **The nested shape and schema width**, which are 21b. Both need a fixture that does not
  exist, and both are worth more once the flat baseline says where the time goes.
- **A cardinality sweep across distinct ratios.** The
  headline number — what interning a column that discards its dictionary costs — is the
  `AUTO` against `PLAIN_ON_DISTINCT` gap, and this fixture produces it. A full sweep needs a
  fixture of its own, because the cap is `max(rowGroupBufferTargetBytes / 2, 1 MiB)` and bounds one
  column's dictionary against a row group holding every column: a chunk crosses it only when
  its own dictionary is about half of everything buffered, which no column of a six-column
  fixture is. It is the evidence stage 26b (#979) needs, it belongs with the change it argues
  for, and building it before anyone proposes moving the cap measures a decision nobody is
  taking.
- **The byte-array delta encodings.** `DELTA_LENGTH_BYTE_ARRAY` and `DELTA_BYTE_ARRAY` target
  values sharing prefixes — paths, URLs, keys — and this fixture's `BYTE_ARRAY` columns are a
  4-value and a 20-value dictionary column, where a dictionary wins by a distance and delta is
  a configuration nobody would choose. Measuring them here would produce a real number about an
  unreal setup. They want a fixture with prefix-sharing values, which is a 21b question.
- **`AvroParquetWriter` as a second incumbent contender.** It is the mainstream parquet-java
  API and belongs in the comparison, but what it adds is the cost of the Avro layer, which
  arrives with the Avro write adapter rather than here.
- **Filesystem and object-store throughput.** Everything here writes to memory, so the number
  is encode throughput. The S3 backend (22) is measured with its own dominant term.
- **DataPage V2**, which is its own milestone and would double every configuration here.
- **Multi-threaded write measurement**, which has nothing to measure until stage 23.
- **A regression gate in CI.** These runs are minutes long and the container's figures are for
  relative movement during development; a gate needs stable hardware and a policy for what a
  regression is.

## User documentation

None. The benchmark added here is internal to `performance-testing/micro-benchmarks` and adds
no public API. Stage 20 documents the writer's `codec` and `encoding` surface; nothing in this
stage changes it.
