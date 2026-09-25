# Write-path coverage assertion (#9, stage 31)

**Status: Complete.** Tracking issue: #990. Delivery stage 31 (Gate) of
[WRITER_SUPPORT.md](../_plans/WRITER_SUPPORT.md). Extends the gate established in
[WRITER_INTEROP_GATE.md](WRITER_INTEROP_GATE.md).

## Context

The interop gate asserts that parquet-java reads what Hardwood writes. Its matrix varies one
axis at a time against a representative base, and its extension contract says every writer
increment extends that matrix with the shapes it introduces, in the same commit that
introduces them.

Whether an increment honoured that contract is a judgement call, and a missed extension is
invisible: from outside, a cell no test ever produced looks exactly like a covered one. The
gate itself already recognizes the problem one level down — it asserts that each case
produced the dictionary, `PLAIN` chunk or page count it exists to cover, "without this a
change to the writer's encoding choice or page sizing would silently empty an axis rather
than fail it". That reasoning stops at the case. Nothing applies it to the matrix as a whole.

Stage 19 is what made the gap material. The writer produces six codecs and resolves five named
encoding policies plus `AUTO`, each legal on a different subset of the seven writable physical
types. The space multiplied; the sweep did not, and by construction cannot: it varies one axis
at a time, so `DELTA_BYTE_ARRAY` met no codec but the base case and `BYTE_STREAM_SPLIT` met no
`FIXED_LEN_BYTE_ARRAY` of a length other than eight.

Two further gaps sat in the annotation half of the gate, where the tables are enumerated by
hand:

- `IntType` appeared at four of its eight width/sign combinations. `TimeType` and
  `TimestampType` appeared at three of six each, with the unit confounded with
  `isAdjustedToUTC` — MILLIS only ever adjusted, MICROS only ever local — so a path that drops
  the flag on one unit could not fail. `DecimalType` appeared at no carrier's precision boundary
  and never with `scale == precision`.
- The boundary values the gate wrote were physical rather than logical. `TypeFixture` carries
  `Integer.MIN_VALUE`, `NaN`, `-0.0`, the empty binary and bytes above `0x7f`; an annotation
  declares a **narrower** range, which `LogicalTypeValueRange` computes exactly, and no file
  the gate produced held a column at that range's ends. Core's `WriterAnnotationRangeTest` and
  `WriterReaderSymmetryTest` do write them, against Hardwood's own reader, over a hand-picked
  subset of the annotations. What was missing is the strict reader's opinion of those values,
  and any assurance that the subset keeps pace with the annotations the writer emits.

The ends of a range are where the statistics comparator is fragile, which is what makes them
worth reaching. An unsigned `INT(32)` maximum is stored as `-1` and must compare unsigned; a
binary `DECIMAL` bound needs sign extension; a `BYTE_ARRAY` maximum of all-`0xff` bytes past
`statisticsTruncationLength` must truncate *and* increment, or it stops bounding the column.
The gate reduces bounds with parquet-java's comparator, so a value written at the end of its
range is checked against an independent implementation of the order — but only if some test
writes one.

## Goal

Every combination the writer can produce is either produced by some test, or waived with a
stated reason. A combination that is neither fails the build.

## The mechanism

Three parts: a domain derived from the writer's own capability tables, an observation recorded
from the bytes each test produced, and a verdict that diffs them.

### Observation

Every write-path test already funnels its file through `ParquetJavaReader`, so that is where
observation belongs. Each of its entry points opens with `observe`, which walks the file once —
however many entry points one test uses — and records into `CoverageRegistry`:

- per column chunk, the physical type, the `FIXED_LEN_BYTE_ARRAY` length where it has one, the
  encodings its data pages declared, the codec its bodies are compressed with, and its
  repetition shape;
- per group node, the annotation it carries, which is where `LIST` and `MAP` live.

The repetition shape is the one thing not stated outright by the file. The three `OPTIONAL`
shapes share a descriptor and differ only in the definition levels the values produced, so they
are told apart by the chunk's null count against its value count, the latter counting nulls as
the format's `num_values` does. A chunk whose statistics state no null count leaves its shape
unrecorded rather than guessed.

No test opts in. `WriterInteropTest`, `WriterNestedInteropTest`, `WriterLogicalTypeInteropTest`,
`RowWriterLogicalTypeInteropTest` and `WriterAnnotationCoverageTest` contribute by running, and
so does every test added later.

What is recorded is what parquet-java found in the file, never what the test intended to
write. This is the same distinction the gate draws when it reads page-level value encodings
rather than the column chunk's `encodings` union: a writer that silently stopped producing an
encoding would still satisfy a registry keyed on intent.

### Domain

Hand-listing the domain would reproduce the problem it exists to solve, so every dimension is
derived:

| Dimension | Derived from |
|---|---|
| Physical types | `PhysicalType.values()`, less `INT96`, whose refusal carries its own pinned-reason test |
| Page encodings | `ColumnEncoding` × its per-type legality, resolved to the encodings a page can declare: `AUTO` reaches `PLAIN` and `RLE_DICTIONARY`, every other policy names one outright |
| Codecs | `CompressionCodec.values()`, split into the six produced and the two refused |
| Repetition shapes | `InteropCase.Nullability`, plus the nested shapes `WriterNestedInteropTest` enumerates |
| Annotation kinds | `LogicalType.class.getPermittedSubclasses()` |
| Annotation parameters | Declared per parameterized kind (below) |

The sealed-hierarchy walk is what makes the domain grow on its own: a member added to
`LogicalType` extends the writer and fails the verdict in the same commit, rather than
extending one and leaving the other to be noticed. That is a property of the code only because
`WriteCoverageVerdictTest` asserts it — `CoverageDomain.annotations()` enumerates the parameters
by hand, so nothing but that walk holds the list to the hierarchy it claims to cover. The one
member excluded from it, `VariantType`, is excluded by asking the writer rather than by saying
so: the same test probes that the writer still refuses a column carrying it, and holds the
refusal to the reason it gives. "The writer threw" would not be enough — a group annotation on a
primitive column is refused whatever the writer supports of it, so `LIST` and `MAP` are refused
there too and both are fully written. Matching the reason is what makes the probe fail on the day
the writer builds `VARIANT` groups rather than on no day at all.

The probe that decides what is producible catches only the two exceptions the writer documents
as refusals — `UnsupportedOperationException` for a capability it does not have, and
`IllegalArgumentException` for a configuration a schema cannot carry. Anything else propagates,
because a probe that swallowed an unexpected failure would drop the capability from the required
set and leave the verdict passing having asserted less.

Two capability tables the domain needs are not reachable from the test module today, and move
rather than being mirrored — a mirrored table drifts, which is the defect this design exists
to catch:

- `ColumnEncoding.supports(PhysicalType)` is package-private in `dev.hardwood.writer`. The
  legality table moves to `dev.hardwood.internal.writer`, where the enum and the test both
  read it.
- `LogicalTypeValueRange` computes `min`, `max` and the binary-`DECIMAL` `unscaledBound` and
  exposes none of them. It gains accessors, so the boundary values a test writes are the ones
  the writer itself derives.

Both targets are internal packages, which sibling modules may depend on directly.

### The required projections

The full cross product — physical types × encodings × codecs × repetition shapes ×
annotations — is neither reachable nor meaningful. What is required instead is a set of
pairwise projections, each admitted because a defect class lives in that pair and in no
smaller one:

| Projection | Cells | The defect it targets |
|---|---|---|
| physical type × page encoding | 23 | The value encoders are per type. This is the #901 class: a stream only a lenient decoder accepts. |
| page encoding × codec | 36 | Framing over an unusual page body — the axis stage 19 widened on both sides at once. |
| physical type × repetition shape | 28 | The level streams and the value stream are written together and read together. |
| `FIXED_LEN_BYTE_ARRAY` length × page encoding | 16 | `BYTE_STREAM_SPLIT` scatters by byte position and `DELTA_BYTE_ARRAY` shares prefixes; both are length-sensitive, and the flat fixture pins the length at eight. |
| annotation × carrier × {dictionary, non-dictionary} | 95 | An annotation's comparator governs the chunk's bounds in either storage form, and the dictionary path reaches those bounds through a different accumulator. |

198 required cells, each with a stated reason, against a cross product in the tens of thousands.
The projections accumulate independently, so the sweep the flat matrix already runs fills the
first three without being restructured.

The carrier is part of the annotation cell because it is part of what the annotation means: a
`DECIMAL(1, 0)` over an `INT32` is bounded by arithmetic on the precision and the same annotation
over a `BYTE_ARRAY` by the magnitude its bytes spell, so neither can stand in for the other.

There is no projection over the value boundaries an annotation declares, and deliberately so.
Those values are written and read back value by value by `WriterAnnotationCoverageTest`, which is
a stronger statement than a cell; a cell recording that the case ran would have been the one
thing here derived from intent rather than from bytes, and would have made the same list satisfy
both sides of its own question. Values *outside* the range reach no file at all, so they are not
a shape this assertion can observe — core's `WriterAnnotationRangeTest` owns them.

### Annotation parameters

The sealed walk enumerates kinds. The parameterized kinds declare their own domain, chosen so
that each parameter varies independently of the others:

| Kind | Required points |
|---|---|
| `IntType` | {8, 16, 32, 64} × {signed, unsigned} — 8 |
| `TimeType` | {MILLIS, MICROS, NANOS} × {adjusted, local} — 6 |
| `TimestampType` | {MILLIS, MICROS, NANOS} × {adjusted, local} — 6 |
| `DecimalType` | per carrier — `INT32`, `INT64`, `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` — at precision 1 and the carrier's maximum, each at scale 0 and at scale equal to the precision |

The carrier maximum comes from `LogicalTypeValidator`, which already computes it: nine digits
for `INT32`, eighteen for `INT64`, `maxFixedPrecision(typeLength)` for a fixed carrier. Scale
equal to precision is the point at which a bound derived by arithmetic on the precision
overflows, and no test writes one today.

`VariantType` is absent rather than waived: the writer refuses a column carrying it, so there is
no cell for a test to produce. `CoverageWaivers` holds no entry for it — what keeps the exclusion
honest is the hierarchy walk above, which probes that the refusal is still real and still the
one `VARIANT` gives.

## Waivers

A cell that cannot be covered is waived as a `(cell, reason, issue)` record. `BROTLI` is the
standing example: parquet-java resolves it through a Hadoop codec name that ships in neither
parquet-java nor Hadoop, so every `BROTLI` cell is waived against the DuckDB coverage in
`WriterDifferentialTest` and the pinned-reason test that already states why.

A waiver is a claim about the world, so it is checked in both directions: **a waived cell that
is nonetheless observed fails the verdict**. Without that, a waiver outlives its reason — a
parquet-java release that gains the codec would leave `BROTLI` waived by inertia, which is
precisely what the existing `parquetJavaHasNoBrotliCodec` test exists to prevent for one
codec and what this generalizes to every cell.

## Placement and mechanics

`parquet-testing-runner`, beside the gate it extends. `CoverageRegistry` and the waiver list
sit next to `ParquetJavaReader`, which is the module's single point of contact with the strict
reader.

The verdict spans test classes, so it cannot be an `@AfterAll`, and Surefire may fork per
class, so it cannot rely on a static registry surviving to the end of the run.
`WriteCoverageListener`, a `TestExecutionListener` registered through `META-INF/services`, writes
what the run recorded to `target/write-coverage/` as it ends, under a name unique to the process
so that forks do not overwrite one another. A second Surefire execution then runs
`WriteCoverageVerdictTest`, which merges those files and asserts the projections. That execution
sets `hardwood.writeCoverage=verify`, under which the listener stands down rather than adding its
own cells to what it is about to judge.

Emptying the directory of a previous run is the build's job. Every fork loads the same listener,
so one that cleared on start would delete the files its siblings had written — the fork-safety
the process-unique names exist for. The module's POM binds `maven-clean-plugin` to
`process-test-classes` against that one directory, which clears it once, before either
execution.

A failure names the empty cells, grouped under the projection each belongs to, rather than
reporting a count. Alongside them `target/write-coverage-report.txt` states how much of each
projection was reached, which is what a burndown is read off.

## What fills the projections

The first four projections are filled by the flat matrix, which two axes extend to reach the
interactions a one-axis-at-a-time sweep cannot: the optional encodings against every codec
parquet-java reads, and the `FIXED_LEN_BYTE_ARRAY` lengths the annotations fix against every
encoding the type can carry.

The annotation projection is filled by `WriterAnnotationCoverageTest`, which takes its
cases from the same `CoverageDomain.annotations()` the verdict requires. An annotation added to
the writer therefore produces a case there and a requirement here in the same commit, rather
than one without the other. For each annotation it writes a file in every storage form the
annotation has, holding the page encoding to the form the case is for so that a dictionary that
quietly resolved to `PLAIN` does not pass as having covered both.

It also writes each annotation at the ends of its declared range and at a point inside, and reads
every one of those values back through parquet-java. Those assertions are the class's own; no
projection tracks them, for the reason given above.

## Non-goals

- **Replacing the sweep.** The projections say which cells must be reached, not how. The
  matrix, the nested enumeration and the annotation tables remain what produces the files;
  this asserts that between them they leave nothing out.
- **Asserting values.** A cell is covered when a conformant file containing it was produced
  and read. What the values had to be is the gate's assertion, unchanged.
- **Covering the read path.** The read direction's corpus is the `parquet-testing` fixtures,
  whose contents no assertion of ours governs. Coverage there is a question about the
  corpus, not about a producible space.
- **Measuring code coverage.** The domain is the writer's declared capabilities, not its
  lines. A branch reached by no test and a capability produced by no test are different
  questions, and only the second one has a matrix.
